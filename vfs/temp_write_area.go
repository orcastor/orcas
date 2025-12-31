package vfs

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/orcastor/orcas/core"
	"github.com/tjfoc/gmsm/sm4"
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
		SmallFileThreshold: 1 << 20,   // 1MB
		LargeFileThreshold: 100 << 20, // 100MB
		CleanupInterval:    1 * time.Hour,
		RetentionPeriod:    24 * time.Hour,
	}
}

// TempWriteArea 临时写入区管理器
type TempWriteArea struct {
	config      *TempWriteAreaConfig
	basePath    string                   // .temp_write 目录路径
	activeFiles map[int64]*TempWriteFile // 活跃的临时文件（key: fileID）
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
	lastAccess    time.Time // 最后访问时间
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
	if err := os.MkdirAll(basePath, 0o755); err != nil {
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
	file, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE, 0o644)
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

	// 需要压缩/加密，使用 SDK 的 BatchWriter
	DebugLog("[TempWriteArea] Processing temp file with compression/encryption using SDK: fileID=%d", twf.fileID)
	return twf.processWithSDK()
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
		if err := os.MkdirAll(finalDir, 0o755); err != nil {
			return fmt.Errorf("failed to create final directory: %w", err)
		}

		finalPath := filepath.Join(finalDir, fileName)

		// 写入chunk文件
		if err := ioutil.WriteFile(finalPath, chunkData, 0o644); err != nil {
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

// processWithSDK 使用真正的压缩和加密处理文件
func (twf *TempWriteFile) processWithSDK() error {
	DebugLog("[TempWriteArea] Processing temp file with compression/encryption using SDK: fileID=%d", twf.fileID)

	// 获取 LocalHandler
	lh, ok := twf.twa.fs.h.(*core.LocalHandler)
	if !ok {
		return fmt.Errorf("handler is not LocalHandler")
	}

	da := lh.GetDataAdapter()
	if da == nil {
		return fmt.Errorf("data adapter is nil")
	}

	// 打开临时文件读取
	tempFile, err := os.Open(twf.tempPath)
	if err != nil {
		return fmt.Errorf("failed to open temp file: %w", err)
	}
	defer tempFile.Close()
	defer os.Remove(twf.tempPath) // 清理临时文件

	// 准备 DataInfo
	dataInfo := &core.DataInfo{
		ID:       twf.dataID,
		Size:     0, // Will be updated after processing
		OrigSize: twf.size,
		Kind:     0, // Will be set based on compression/encryption
	}

	// 设置压缩标志
	if twf.needsCompress && twf.twa.fs.CmprWay > 0 {
		switch twf.twa.fs.CmprWay {
		case core.DATA_CMPR_SNAPPY:
			dataInfo.Kind |= core.DATA_CMPR_SNAPPY
		case core.DATA_CMPR_ZSTD:
			dataInfo.Kind |= core.DATA_CMPR_ZSTD
		case core.DATA_CMPR_GZIP:
			dataInfo.Kind |= core.DATA_CMPR_GZIP
		case core.DATA_CMPR_BR:
			dataInfo.Kind |= core.DATA_CMPR_BR
		}
	}

	// 设置加密标志
	if twf.needsEncrypt && twf.twa.fs.EndecWay > 0 && twf.twa.fs.EndecKey != "" {
		switch twf.twa.fs.EndecWay {
		case core.DATA_ENDEC_AES256:
			dataInfo.Kind |= core.DATA_ENDEC_AES256
		case core.DATA_ENDEC_SM4:
			dataInfo.Kind |= core.DATA_ENDEC_SM4
		}
	}

	// 获取分块大小
	chunkSize := twf.twa.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// 流式处理：读取 → 压缩 → 加密 → 写入
	buffer := make([]byte, chunkSize)
	sn := 0
	var totalProcessedSize int64 = 0

	for {
		n, readErr := tempFile.Read(buffer)
		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("failed to read from temp file: %w", readErr)
		}
		if n == 0 {
			break
		}

		chunkData := buffer[:n]

		// 处理数据：压缩和加密
		processedData, err := twf.processChunkData(chunkData, dataInfo)
		if err != nil {
			return fmt.Errorf("failed to process chunk %d: %w", sn, err)
		}

		// 写入处理后的数据
		if err := da.Write(twf.twa.fs.c, twf.twa.fs.bktID, twf.dataID, sn, processedData); err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", sn, err)
		}

		totalProcessedSize += int64(len(processedData))
		sn++

		if readErr == io.EOF {
			break
		}
	}

	// 更新 DataInfo 的最终大小
	dataInfo.Size = totalProcessedSize

	// 保存 DataInfo
	if _, err := lh.PutDataInfo(twf.twa.fs.c, twf.twa.fs.bktID, []*core.DataInfo{dataInfo}); err != nil {
		DebugLog("[TempWriteArea] WARNING: Failed to update DataInfo: %v", err)
	} else {
		// 更新 DataInfo 缓存
		dataInfoCache.Put(twf.dataID, dataInfo)
		DebugLog("[TempWriteArea] Updated DataInfo: dataID=%d, size=%d, origSize=%d, kind=%d, compress=%v, encrypt=%v",
			twf.dataID, dataInfo.Size, dataInfo.OrigSize, dataInfo.Kind, twf.needsCompress, twf.needsEncrypt)
	}

	DebugLog("[TempWriteArea] Successfully processed temp file: fileID=%d, dataID=%d, origSize=%d, processedSize=%d, numChunks=%d, compress=%v, encrypt=%v",
		twf.fileID, twf.dataID, twf.size, totalProcessedSize, sn, twf.needsCompress, twf.needsEncrypt)

	return nil
}

// processChunkData 处理单个数据块（压缩和加密）
func (twf *TempWriteFile) processChunkData(originalData []byte, dataInfo *core.DataInfo) ([]byte, error) {
	data := originalData

	// 1. 应用压缩（如果需要）
	if twf.needsCompress && twf.twa.fs.CmprWay > 0 {
		var cmpr archiver.Compressor
		if dataInfo.Kind&core.DATA_CMPR_SNAPPY != 0 {
			cmpr = &archiver.Snappy{}
		} else if dataInfo.Kind&core.DATA_CMPR_ZSTD != 0 {
			cmpr = &archiver.Zstd{
				EncoderOptions: []zstd.EOption{
					zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(twf.twa.fs.CmprQlty))),
				},
			}
		} else if dataInfo.Kind&core.DATA_CMPR_GZIP != 0 {
			cmpr = &archiver.Gz{
				CompressionLevel: int(twf.twa.fs.CmprQlty),
			}
		} else if dataInfo.Kind&core.DATA_CMPR_BR != 0 {
			cmpr = &archiver.Brotli{
				Quality: int(twf.twa.fs.CmprQlty),
			}
		}

		if cmpr != nil {
			var cmprBuf bytes.Buffer
			err := cmpr.Compress(bytes.NewBuffer(data), &cmprBuf)
			if err == nil && cmprBuf.Len() < len(data) {
				// 压缩成功且压缩后更小
				data = cmprBuf.Bytes()
				DebugLog("[TempWriteArea] Compressed chunk: origSize=%d, compressedSize=%d, ratio=%.2f%%",
					len(originalData), len(data), float64(len(data))*100/float64(len(originalData)))
			} else if err != nil {
				DebugLog("[TempWriteArea] WARNING: Compression failed, using original data: %v", err)
			} else {
				DebugLog("[TempWriteArea] Compression not beneficial, using original data: origSize=%d, compressedSize=%d",
					len(originalData), cmprBuf.Len())
			}
		}
	}

	// 2. 应用加密（如果需要）
	if twf.needsEncrypt && twf.twa.fs.EndecWay > 0 && twf.twa.fs.EndecKey != "" {
		var err error
		if dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
			data, err = aes256.Encrypt(twf.twa.fs.EndecKey, data)
			if err != nil {
				return nil, fmt.Errorf("AES256 encryption failed: %w", err)
			}
			DebugLog("[TempWriteArea] Encrypted chunk with AES256: size=%d", len(data))
		} else if dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
			data, err = sm4.Sm4Cbc([]byte(twf.twa.fs.EndecKey), data, true)
			if err != nil {
				return nil, fmt.Errorf("SM4 encryption failed: %w", err)
			}
			DebugLog("[TempWriteArea] Encrypted chunk with SM4: size=%d", len(data))
		}
	}

	return data, nil
}

// processAndMove_OLD 处理文件（压缩/加密）并移动到最终位置 (备用实现)
func (twf *TempWriteFile) processAndMove_OLD() error {
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

	// 准备 DataInfo
	dataInfo := &core.DataInfo{
		ID:       twf.dataID,
		Size:     0, // Will be updated after compression
		OrigSize: twf.size,
		Kind:     0, // Will be set based on compression/encryption
	}

	// Set compression kind based on VFS config
	if twf.needsCompress && twf.twa.fs.CmprWay > 0 {
		switch twf.twa.fs.CmprWay {
		case 1:
			dataInfo.Kind |= core.DATA_CMPR_SNAPPY
		case 2:
			dataInfo.Kind |= core.DATA_CMPR_ZSTD
		case 3:
			dataInfo.Kind |= core.DATA_CMPR_GZIP
		}
	}

	// Set encryption kind if needed
	if twf.needsEncrypt {
		dataInfo.Kind |= core.DATA_ENDEC_AES256
	}

	// 流式处理：读取、压缩/加密、写入
	buffer := make([]byte, chunkSize)
	sn := 0
	var totalProcessedSize int64 = 0

	for {
		n, readErr := tempFile.Read(buffer)
		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("failed to read from temp file: %w", readErr)
		}
		if n == 0 {
			break
		}

		chunkData := buffer[:n]

		// Process chunk data (compress and/or encrypt)
		// Note: processChunkData is in core package but not exported
		// We need to use SDK or implement our own processing
		processedData := chunkData

		// For now, we'll use the DataAdapter's Write method directly
		// The compression and encryption will be handled by the SDK layer
		// TODO: Implement proper compression/encryption here or use SDK

		// Write processed chunk using DataAdapter
		da := lh.GetDataAdapter()
		if err := da.Write(twf.twa.fs.c, twf.twa.fs.bktID, twf.dataID, sn, processedData); err != nil {
			return fmt.Errorf("failed to write processed chunk %d: %w", sn, err)
		}

		totalProcessedSize += int64(len(processedData))
		sn++

		if readErr == io.EOF {
			break
		}
	}

	// Update DataInfo with final size
	dataInfo.Size = totalProcessedSize

	// Save DataInfo
	if _, err := lh.PutDataInfo(twf.twa.fs.c, twf.twa.fs.bktID, []*core.DataInfo{dataInfo}); err != nil {
		DebugLog("[TempWriteArea] WARNING: Failed to update DataInfo: %v", err)
	} else {
		// Update DataInfo cache
		dataInfoCache.Put(twf.dataID, dataInfo)
		DebugLog("[TempWriteArea] Updated DataInfo cache: dataID=%d, size=%d, origSize=%d, kind=%d",
			twf.dataID, dataInfo.Size, dataInfo.OrigSize, dataInfo.Kind)
	}

	DebugLog("[TempWriteArea] Successfully processed and moved temp file: fileID=%d, dataID=%d, origSize=%d, processedSize=%d, numChunks=%d, compress=%v, encrypt=%v",
		twf.fileID, twf.dataID, twf.size, totalProcessedSize, sn, twf.needsCompress, twf.needsEncrypt)

	return nil
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
	now := time.Now()
	expiredFiles := make([]*TempWriteFile, 0)

	// 步骤1: 查找内存中的过期文件（持有锁）
	twa.mu.Lock()
	for _, twf := range twa.activeFiles {
		twf.mu.RLock()
		if now.Sub(twf.lastAccess) > twa.config.RetentionPeriod {
			expiredFiles = append(expiredFiles, twf)
		}
		twf.mu.RUnlock()
	}

	// 从活跃列表中移除（在关闭文件之前）
	for _, twf := range expiredFiles {
		delete(twa.activeFiles, twf.fileID)
	}

	// 构建当前活跃文件ID列表（在删除过期文件之后）
	activeFileIDs := make(map[int64]bool)
	for fileID := range twa.activeFiles {
		activeFileIDs[fileID] = true
	}
	twa.mu.Unlock()

	// 步骤2: 清理内存中的过期文件（不持有 twa.mu 锁，避免死锁）
	for _, twf := range expiredFiles {
		DebugLog("[TempWriteArea] Cleaning up expired temp file: fileID=%d, lastAccess=%v",
			twf.fileID, twf.lastAccess)

		// 关闭文件
		twf.Close()

		// 删除临时文件
		if err := os.Remove(twf.tempPath); err != nil {
			DebugLog("[TempWriteArea] WARNING: Failed to remove expired temp file: %v", err)
		}
	}

	if len(expiredFiles) > 0 {
		DebugLog("[TempWriteArea] Cleaned up %d expired temp files from memory", len(expiredFiles))
	}

	// 步骤3: 扫描磁盘上的孤儿临时文件
	twa.cleanupOrphanedFiles(activeFileIDs)
}

// cleanupOrphanedFiles 清理磁盘上的孤儿临时文件
// 这些文件可能是由于程序崩溃、异常退出等原因遗留的
func (twa *TempWriteArea) cleanupOrphanedFiles(activeFileIDs map[int64]bool) {
	tempDir := filepath.Join(twa.fs.DataPath, twa.config.TempDir)

	// 检查临时目录是否存在
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		return
	}

	// 读取临时目录中的所有文件
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		DebugLog("[TempWriteArea] WARNING: Failed to read temp directory: %v", err)
		return
	}

	now := time.Now()
	orphanedCount := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// 解析文件名：fileID_dataID
		fileName := entry.Name()
		var fileID, dataID int64
		if _, err := fmt.Sscanf(fileName, "%d_%d", &fileID, &dataID); err != nil {
			// 文件名格式不正确，可能是其他文件，跳过
			continue
		}

		// 检查文件是否在活跃列表中
		if activeFileIDs[fileID] {
			// 文件仍在使用中，跳过
			DebugLog("[TempWriteArea] Skipping active file: %s", fileName)
			continue
		}

		// 获取文件信息
		filePath := filepath.Join(tempDir, fileName)
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		// 检查文件是否过期（根据修改时间）
		if now.Sub(fileInfo.ModTime()) > twa.config.RetentionPeriod {
			// 文件已过期，删除
			if err := os.Remove(filePath); err != nil {
				DebugLog("[TempWriteArea] WARNING: Failed to remove orphaned temp file: %s, error=%v", fileName, err)
			} else {
				DebugLog("[TempWriteArea] Removed orphaned temp file: %s, age=%v", fileName, now.Sub(fileInfo.ModTime()))
				orphanedCount++
			}
		}
	}

	if orphanedCount > 0 {
		DebugLog("[TempWriteArea] Cleaned up %d orphaned temp files from disk", orphanedCount)
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
