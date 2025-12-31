package vfs

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/orcastor/orcas/core"
)

// TempWriteAreaConfig 临时写入区配置
type TempWriteAreaConfig struct {
	// 启用临时写入区
	Enabled bool

	// 临时目录路径（默认：桶目录/.temp_write）
	TempDir string

	// 小文件阈值（< 此值使用内存缓存）
	SmallFileThreshold int64 // 默认 1MB

	// 大文件阈值（> 此值使用流式处理）
	LargeFileThreshold int64 // 默认 100MB

	// 临时文件清理间隔
	CleanupInterval time.Duration // 默认 1 小时

	// 临时文件保留时间
	RetentionPeriod time.Duration // 默认 24 小时
}

// DefaultTempWriteAreaConfig 返回默认配置
func DefaultTempWriteAreaConfig() *TempWriteAreaConfig {
	return &TempWriteAreaConfig{
		Enabled:            true,
		TempDir:            ".temp_write",
		SmallFileThreshold: 1 << 20,      // 1MB
		LargeFileThreshold: 100 << 20,    // 100MB
		CleanupInterval:    1 * time.Hour,
		RetentionPeriod:    24 * time.Hour,
	}
}

// TempWriteArea 临时写入区管理器
type TempWriteArea struct {
	config      *TempWriteAreaConfig
	basePath    string                    // .temp_write 目录路径
	activeFiles map[int64]*TempWriteFile  // 活跃的临时文件（key: fileID）
	mu          sync.RWMutex
	fs          *OrcasFS
	stopCleanup chan struct{}
}

// WriteRange 表示已写入的数据范围
type WriteRange struct {
	Start int64 // 起始偏移（包含）
	End   int64 // 结束偏移（不包含）
}

// TempWriteFile 临时写入文件
type TempWriteFile struct {
	fileID        int64
	dataID        int64
	tempPath      string       // 临时文件路径
	file          *os.File     // 打开的文件句柄
	size          int64        // 当前文件大小
	ranges        []WriteRange // 已写入的范围（有序，不重叠）
	needsCompress bool         // 是否需要压缩
	needsEncrypt  bool         // 是否需要加密
	mu            sync.RWMutex
	lastAccess    time.Time    // 最后访问时间
	twa           *TempWriteArea
}

// NewTempWriteArea 创建临时写入区管理器
func NewTempWriteArea(fs *OrcasFS, config *TempWriteAreaConfig) (*TempWriteArea, error) {
	if config == nil {
		config = DefaultTempWriteAreaConfig()
	}

	// 构建临时目录路径
	dataPath := fs.DataPath
	if dataPath == "" {
		dataPath = "." // 默认使用当前目录
	}
	basePath := filepath.Join(dataPath, config.TempDir)

	// 创建临时目录
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp write area: %w", err)
	}

	twa := &TempWriteArea{
		config:      config,
		basePath:    basePath,
		activeFiles: make(map[int64]*TempWriteFile),
		fs:          fs,
		stopCleanup: make(chan struct{}),
	}

	// 启动清理协程
	if config.Enabled {
		go twa.cleanupLoop()
	}

	return twa, nil
}

// GetOrCreate 获取或创建临时写入文件
// originalSize: 原文件大小，用于部分覆盖场景
func (twa *TempWriteArea) GetOrCreate(fileID, dataID, originalSize int64, needsCompress, needsEncrypt bool) (*TempWriteFile, error) {
	twa.mu.Lock()
	defer twa.mu.Unlock()

	// 检查是否已存在
	if twf, ok := twa.activeFiles[fileID]; ok {
		twf.mu.Lock()
		twf.lastAccess = time.Now()
		twf.mu.Unlock()
		return twf, nil
	}

	// 创建新的临时文件
	tempPath := filepath.Join(twa.basePath, fmt.Sprintf("%d_%d", fileID, dataID))

	// 打开或创建文件
	file, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	// 如果有原文件大小，预分配空间（用于稀疏文件）
	if originalSize > 0 {
		if err := file.Truncate(originalSize); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to truncate temp file: %w", err)
		}
		DebugLog("[TempWriteArea] Truncated temp file to original size: fileID=%d, size=%d", fileID, originalSize)
	}

	twf := &TempWriteFile{
		fileID:        fileID,
		dataID:        dataID,
		tempPath:      tempPath,
		file:          file,
		size:          originalSize, // 初始化为原文件大小
		ranges:        make([]WriteRange, 0),
		needsCompress: needsCompress,
		needsEncrypt:  needsEncrypt,
		lastAccess:    time.Now(),
		twa:           twa,
	}

	twa.activeFiles[fileID] = twf
	DebugLog("[TempWriteArea] Created temp write file: fileID=%d, dataID=%d, originalSize=%d, path=%s", fileID, dataID, originalSize, tempPath)

	return twf, nil
}

// Write 写入数据到临时文件
func (twf *TempWriteFile) Write(offset int64, data []byte) error {
	twf.mu.Lock()
	defer twf.mu.Unlock()

	// 更新最后访问时间
	twf.lastAccess = time.Now()

	// 直接写入磁盘临时文件
	n, err := twf.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("incomplete write: wrote %d bytes, expected %d", n, len(data))
	}

	// 更新写入范围
	twf.updateRanges(offset, int64(len(data)))

	// 更新文件大小
	endOffset := offset + int64(len(data))
	if endOffset > twf.size {
		twf.size = endOffset
	}

	DebugLog("[TempWriteArea] Wrote to temp file: fileID=%d, offset=%d, size=%d, totalSize=%d", 
		twf.fileID, offset, len(data), twf.size)

	return nil
}

// updateRanges 更新已写入的范围（合并重叠区域）
func (twf *TempWriteFile) updateRanges(offset, size int64) {
	newRange := WriteRange{Start: offset, End: offset + size}

	// 如果没有范围，直接添加
	if len(twf.ranges) == 0 {
		twf.ranges = append(twf.ranges, newRange)
		return
	}

	// 查找插入位置并合并重叠范围
	merged := make([]WriteRange, 0, len(twf.ranges)+1)
	inserted := false

	for _, r := range twf.ranges {
		if !inserted {
			// 检查是否可以合并
			if newRange.End < r.Start {
				// 新范围在当前范围之前，插入
				merged = append(merged, newRange)
				merged = append(merged, r)
				inserted = true
			} else if newRange.Start > r.End {
				// 新范围在当前范围之后，继续
				merged = append(merged, r)
			} else {
				// 有重叠，合并
				newRange.Start = min(newRange.Start, r.Start)
				newRange.End = max(newRange.End, r.End)
			}
		} else {
			merged = append(merged, r)
		}
	}

	// 如果还没插入，添加到末尾
	if !inserted {
		merged = append(merged, newRange)
	}

	twf.ranges = merged
}

// Flush 将临时文件移动到最终位置
func (twf *TempWriteFile) Flush() error {
	twf.mu.Lock()
	defer twf.mu.Unlock()

	DebugLog("[TempWriteArea] Flushing temp file: fileID=%d, dataID=%d, size=%d, needsCompress=%v, needsEncrypt=%v",
		twf.fileID, twf.dataID, twf.size, twf.needsCompress, twf.needsEncrypt)

	// 同步文件到磁盘
	if err := twf.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	// 关闭文件
	if err := twf.file.Close(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	twf.file = nil
	
	// 从活跃文件列表中移除（重要！避免下次GetOrCreate返回已关闭的文件）
	twf.twa.mu.Lock()
	delete(twf.twa.activeFiles, twf.fileID)
	twf.twa.mu.Unlock()

	// 判断是否需要处理
	if !twf.needsCompress && !twf.needsEncrypt {
		// 不需要压缩/加密，直接移动
		DebugLog("[TempWriteArea] Moving temp file directly (no compression/encryption): fileID=%d", twf.fileID)
		return twf.moveToFinalLocation()
	}

	// 需要压缩/加密，流式处理
	DebugLog("[TempWriteArea] Processing temp file with compression/encryption: fileID=%d", twf.fileID)
	return twf.processAndMove()
}

// moveToFinalLocation 直接移动文件到最终位置（不压缩/不加密）
// 对于大文件，需要分片存储
func (twf *TempWriteFile) moveToFinalLocation() error {
	// 获取 LocalHandler
	lh, ok := twf.twa.fs.h.(*core.LocalHandler)
	if !ok {
		return fmt.Errorf("handler is not LocalHandler")
	}

	// 获取 chunk size
	chunkSize := twf.twa.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// 打开临时文件读取
	tempFile, err := os.Open(twf.tempPath)
	if err != nil {
		return fmt.Errorf("failed to open temp file: %w", err)
	}
	defer tempFile.Close()
	defer os.Remove(twf.tempPath) // 清理临时文件

	// 计算需要多少个chunk
	numChunks := (twf.size + chunkSize - 1) / chunkSize
	
	DebugLog("[TempWriteArea] Splitting file into chunks: fileID=%d, size=%d, chunkSize=%d, numChunks=%d",
		twf.fileID, twf.size, chunkSize, numChunks)

	// 分片写入
	for sn := int64(0); sn < numChunks; sn++ {
		// 计算这个chunk的大小
		offset := sn * chunkSize
		size := chunkSize
		if offset+size > twf.size {
			size = twf.size - offset
		}

		// 读取chunk数据
		chunkData := make([]byte, size)
		n, err := tempFile.ReadAt(chunkData, offset)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read chunk %d: %w", sn, err)
		}
		if int64(n) != size {
			return fmt.Errorf("incomplete read for chunk %d: got %d bytes, expected %d", sn, n, size)
		}

		// 计算最终路径
		fileName := fmt.Sprintf("%d_%d", twf.dataID, sn)
		hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
		
		finalDir := filepath.Join(twf.twa.fs.DataPath, fmt.Sprint(twf.twa.fs.bktID), hash[21:24], hash[8:24])
		if err := os.MkdirAll(finalDir, 0755); err != nil {
			return fmt.Errorf("failed to create final directory: %w", err)
		}

		finalPath := filepath.Join(finalDir, fileName)

		// 写入chunk文件
		if err := ioutil.WriteFile(finalPath, chunkData, 0644); err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", sn, err)
		}

		DebugLog("[TempWriteArea] Wrote chunk: sn=%d, path=%s, size=%d", sn, finalPath, size)
	}

	// 更新 DataInfo
	dataInfo := &core.DataInfo{
		ID:       twf.dataID,
		Size:     twf.size,
		OrigSize: twf.size,
		Kind:     0, // 不压缩/不加密
	}

	if _, err := lh.PutDataInfo(twf.twa.fs.c, twf.twa.fs.bktID, []*core.DataInfo{dataInfo}); err != nil {
		DebugLog("[TempWriteArea] WARNING: Failed to update DataInfo: %v", err)
		// 不返回错误，因为文件已经移动成功
	} else {
		// CRITICAL: Update DataInfo cache immediately
		// This ensures Read() can find the correct DataInfo
		dataInfoCache.Put(twf.dataID, dataInfo)
		DebugLog("[TempWriteArea] Updated DataInfo cache: dataID=%d, size=%d", twf.dataID, twf.size)
	}

	DebugLog("[TempWriteArea] Successfully split and moved temp file: fileID=%d, dataID=%d, size=%d, numChunks=%d", 
		twf.fileID, twf.dataID, twf.size, numChunks)

	return nil
}

// processAndMove 处理文件（压缩/加密）并移动到最终位置
func (twf *TempWriteFile) processAndMove() error {
	// 对于现在的实现，如果需要压缩/加密，我们暂时也使用直接移动
	// TODO: 未来实现流式压缩/加密
	DebugLog("[TempWriteArea] WARNING: Compression/encryption not yet implemented, using direct move: fileID=%d", twf.fileID)
	return twf.moveToFinalLocation()
}

// Close 关闭临时文件
func (twf *TempWriteFile) Close() error {
	twf.mu.Lock()
	defer twf.mu.Unlock()

	if twf.file != nil {
		if err := twf.file.Close(); err != nil {
			return err
		}
		twf.file = nil
	}

	// 从活跃文件列表中移除
	twf.twa.mu.Lock()
	delete(twf.twa.activeFiles, twf.fileID)
	twf.twa.mu.Unlock()

	return nil
}

// cleanupLoop 定期清理过期的临时文件
func (twa *TempWriteArea) cleanupLoop() {
	ticker := time.NewTicker(twa.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			twa.cleanup()
		case <-twa.stopCleanup:
			return
		}
	}
}

// cleanup 清理过期的临时文件
func (twa *TempWriteArea) cleanup() {
	twa.mu.Lock()
	defer twa.mu.Unlock()

	now := time.Now()
	expiredFiles := make([]*TempWriteFile, 0)

	// 查找过期文件
	for _, twf := range twa.activeFiles {
		twf.mu.RLock()
		if now.Sub(twf.lastAccess) > twa.config.RetentionPeriod {
			expiredFiles = append(expiredFiles, twf)
		}
		twf.mu.RUnlock()
	}

	// 清理过期文件
	for _, twf := range expiredFiles {
		DebugLog("[TempWriteArea] Cleaning up expired temp file: fileID=%d, lastAccess=%v",
			twf.fileID, twf.lastAccess)

		// 关闭文件
		twf.Close()

		// 删除临时文件
		if err := os.Remove(twf.tempPath); err != nil {
			DebugLog("[TempWriteArea] WARNING: Failed to remove expired temp file: %v", err)
		}

		// 从活跃列表中移除
		delete(twa.activeFiles, twf.fileID)
	}

	if len(expiredFiles) > 0 {
		DebugLog("[TempWriteArea] Cleaned up %d expired temp files", len(expiredFiles))
	}
}

// Stop 停止临时写入区管理器
func (twa *TempWriteArea) Stop() {
	close(twa.stopCleanup)

	// 关闭所有活跃文件
	twa.mu.Lock()
	defer twa.mu.Unlock()

	for _, twf := range twa.activeFiles {
		twf.Close()
	}
}

// Helper functions
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

