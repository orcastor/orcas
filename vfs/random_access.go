package vfs

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/h2non/filetype"
	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/orca-zhang/ecache"
	"github.com/orcastor/orcas/core"
	"github.com/tjfoc/gmsm/sm4"
)

var (
	// 对象池：重用字节缓冲区，减少内存分配
	chunkDataPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4<<20) // 预分配4MB容量
		},
	}

	// 对象池：重用写入操作slice
	writeOpsPool = sync.Pool{
		New: func() interface{} {
			return make([]WriteOperation, 0, 32)
		},
	}

	// ecache缓存：缓存DataInfo，减少数据库查询
	// key: "<bktID>_<dataID>", value: *core.DataInfo
	dataInfoCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// ecache缓存：缓存文件对象信息，减少数据库查询
	// key: "<bktID>_<fileID>", value: *core.ObjectInfo
	fileObjCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// 批量写入缓冲区对象池：复用缓冲区，减少内存分配
	// 注意：buffer是长期使用的，只有在BatchWriteManager不再使用时才归还
	// 对象池使用默认4MB大小，如果chunkSize不同会重新分配
	batchBufferPool = sync.Pool{
		New: func() interface{} {
			// 创建默认4MB缓冲区
			return make([]byte, 4<<20)
		},
	}
)

// BatchWriteManager 批量写入管理器，支持将多个小文件的数据块打包到同一个数据块
// 使用无锁方式管理缓冲区，通过原子操作获取写入位置
type BatchWriteManager struct {
	// 文件系统（所有文件共享同一个桶）
	fs *OrcasFS

	// 共享缓冲区（无锁）
	buffer      []byte       // 共享缓冲区（大小为chunkSize）
	writeOffset atomic.Int64 // 当前写入位置（原子操作）
	bufferSize  int64        // 缓冲区大小（等于chunkSize）

	// 文件信息列表（无锁，使用原子操作管理）
	fileInfos     []*BatchFileInfo // 文件信息列表（固定大小，预分配）
	fileInfoIndex atomic.Int64     // 当前文件信息索引（原子操作）
	maxFileInfos  int64            // 最大文件信息数量

	// 刷新控制
	flushWindow    time.Duration
	maxPackageSize int64 // 最大打包数据块大小（默认4MB）
}

// BatchFileInfo 批量写入文件信息（无锁）
type BatchFileInfo struct {
	FileID   int64
	Offset   int64  // 在缓冲区中的偏移位置
	Size     int64  // 处理后的数据大小（压缩加密后）
	OrigSize int64  // 原始数据大小
	Kind     uint32 // 压缩/加密标记
}

// PackagedDataBlock 打包数据块信息
type PackagedDataBlock struct {
	PkgID     int64               // 打包数据块的ID
	Data      []byte              // 打包后的数据
	FileInfos []*PackagedFileInfo // 文件信息列表
}

// PackagedFileInfo 打包文件信息
type PackagedFileInfo struct {
	FileID    int64  // 文件ID
	DataID    int64  // 数据ID
	PkgOffset uint32 // 在打包数据块中的偏移位置
	Size      int64  // 数据大小（压缩加密后）
	OrigSize  int64  // 原始数据大小
	Kind      uint32 // 数据状态（压缩/加密等）
}

// getBatchWriteManager 获取指定桶的批量写入管理器（线程安全）
func (fs *OrcasFS) getBatchWriteManager() *BatchWriteManager {
	fs.batchWriteMgrOnce.Do(func() {
		config := core.GetWriteBufferConfig()
		// 使用chunkSize作为缓冲区大小
		chunkSize := fs.chunkSize
		if chunkSize <= 0 {
			chunkSize = 4 << 20 // 默认4MB
		}

		// 从对象池获取缓冲区（如果池中没有，会创建新的）
		buffer := batchBufferPool.Get().([]byte)
		// 确保缓冲区大小正确（如果chunkSize与对象池中的不同，重新分配）
		if int64(cap(buffer)) < chunkSize {
			buffer = make([]byte, chunkSize)
		} else {
			buffer = buffer[:chunkSize]
		}
		// 注意：不需要清零缓冲区，因为我们总是从 writeOffset 开始写入
		// flush 时会原子地重置 writeOffset，旧数据不会影响新数据

		fs.batchWriteMgr = &BatchWriteManager{
			fs:             fs,
			buffer:         buffer,
			bufferSize:     chunkSize,
			fileInfos:      make([]*BatchFileInfo, 1<<10), // 预分配1024个文件信息
			maxFileInfos:   1 << 10,
			flushWindow:    config.BufferWindow,
			maxPackageSize: chunkSize, // 使用chunkSize作为最大打包大小
		}
		// 启动定期刷新协程
		go fs.batchWriteMgr.flushLoop()
	})
	return fs.batchWriteMgr
}

// flushLoop 定期刷新循环
func (bwm *BatchWriteManager) flushLoop() {
	ticker := time.NewTicker(bwm.flushWindow)
	defer ticker.Stop()

	for range ticker.C {
		// 定期刷新（每flushWindow秒）
		bwm.flush()
	}
}

// flushAll 刷新所有待写入数据
func (bwm *BatchWriteManager) flushAll() {
	bwm.flush()
}

// flush 刷新所有待写入数据（无锁方式）
// 原子地获取当前写入位置和文件信息，然后打包并写入
func (bwm *BatchWriteManager) flush() {
	startTime := time.Now()

	// 原子地交换写入位置为0（重置并获取旧值）
	oldOffset := bwm.writeOffset.Swap(0)

	// 原子地交换文件索引为0（重置并获取旧值）
	oldIndex := bwm.fileInfoIndex.Swap(0)

	if oldOffset == 0 || oldIndex == 0 {
		return // 没有待写入数据
	}

	var copyTime, packageTime, flushPkgTime time.Duration

	// 注意：不需要清零已使用的缓冲区部分，因为：
	// 1. 新数据会从 writeOffset=0 开始写入，覆盖旧数据
	// 2. 我们只读取 fileInfo.Offset 到 fileInfo.Offset+fileInfo.Size 的数据
	// 3. 旧数据不会影响新数据的正确性

	// 复制当前的文件信息（避免在打包时被修改）
	copyStart := time.Now()
	fileInfos := make([]*BatchFileInfo, oldIndex)
	for i := int64(0); i < oldIndex; i++ {
		if bwm.fileInfos[i] != nil {
			// 浅拷贝文件信息（数据本身在buffer中，不需要深拷贝）
			fileInfos[i] = bwm.fileInfos[i]
		}
	}
	copyTime = time.Since(copyStart)

	if len(fileInfos) == 0 {
		return
	}

	// 打包数据块
	packageStart := time.Now()
	packages := bwm.packageDataBlocks(fileInfos)
	packageTime = time.Since(packageStart)

	// 批量写入打包数据块、DataInfo和ObjectInfo
	flushPkgStart := time.Now()
	for _, pkg := range packages {
		bwm.flushPackage(pkg)
	}
	flushPkgTime = time.Since(flushPkgStart)

	totalTime := time.Since(startTime)
	if totalTime > 1*time.Millisecond {
		fmt.Printf("[PERF] flush: offset=%d, index=%d, files=%d, packages=%d, total=%v, copy=%v, package=%v, flushPkg=%v\n",
			oldOffset, oldIndex, len(fileInfos), len(packages), totalTime, copyTime, packageTime, flushPkgTime)
	}
}

// packageDataBlocks 打包数据块（使用已处理的数据）
// 从共享缓冲区读取数据并打包
func (bwm *BatchWriteManager) packageDataBlocks(fileInfos []*BatchFileInfo) []*PackagedDataBlock {
	var packages []*PackagedDataBlock
	var currentPkg *PackagedDataBlock
	var currentPkgOffset uint32

	for _, fileInfo := range fileInfos {
		if fileInfo == nil || fileInfo.Size == 0 {
			continue
		}

		// 从共享缓冲区读取数据
		data := bwm.buffer[fileInfo.Offset : fileInfo.Offset+fileInfo.Size]

		// 如果当前打包块为空或数据太大，创建新的打包块
		if currentPkg == nil || int64(len(currentPkg.Data))+fileInfo.Size > bwm.maxPackageSize {
			// 如果当前打包块不为空，先保存它
			if currentPkg != nil {
				packages = append(packages, currentPkg)
			}

			// 创建新的打包块
			currentPkg = &PackagedDataBlock{
				Data:      make([]byte, 0, bwm.maxPackageSize),
				FileInfos: make([]*PackagedFileInfo, 0),
			}
			currentPkgOffset = 0
		}

		// 将处理后的数据添加到当前打包块
		pkgFileInfo := &PackagedFileInfo{
			FileID:    fileInfo.FileID,
			PkgOffset: currentPkgOffset,
			Size:      fileInfo.Size,     // 压缩加密后的大小
			OrigSize:  fileInfo.OrigSize, // 原始数据大小
			Kind:      fileInfo.Kind,     // 压缩和加密标记
		}

		// 复制数据到打包块
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		currentPkg.Data = append(currentPkg.Data, dataCopy...)
		currentPkg.FileInfos = append(currentPkg.FileInfos, pkgFileInfo)

		// 更新偏移量
		currentPkgOffset += uint32(len(data))
	}

	// 保存最后一个打包块
	if currentPkg != nil {
		packages = append(packages, currentPkg)
	}

	return packages
}

// processFileData 根据配置处理文件数据（压缩和加密）
// 返回：处理后的数据、Kind标记、原始大小
func (bwm *BatchWriteManager) processFileData(fs *OrcasFS, originalData []byte) ([]byte, uint32, int64) {
	origSize := int64(len(originalData))
	kind := uint32(0)
	cfg := fs.sdkCfg

	// 设置压缩和加密标记
	if cfg != nil {
		if cfg.WiseCmpr > 0 {
			kind |= cfg.WiseCmpr
		}
		if cfg.EndecWay > 0 {
			kind |= cfg.EndecWay
		}
	}

	if kind == 0 {
		// 无压缩无加密，直接返回原始数据
		return originalData, kind, origSize
	}

	data := originalData
	compressedData := originalData // 保存压缩后的数据（如果压缩成功）
	var err error

	// 1. 压缩（如果启用）
	hasCmpr := kind&core.DATA_CMPR_MASK != 0
	if hasCmpr && cfg != nil && cfg.WiseCmpr > 0 {
		var cmpr archiver.Compressor
		if cfg.WiseCmpr&core.DATA_CMPR_SNAPPY != 0 {
			cmpr = &archiver.Snappy{}
		} else if cfg.WiseCmpr&core.DATA_CMPR_ZSTD != 0 {
			cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(cfg.CmprQlty)))}}
		} else if cfg.WiseCmpr&core.DATA_CMPR_GZIP != 0 {
			cmpr = &archiver.Gz{CompressionLevel: int(cfg.CmprQlty)}
		} else if cfg.WiseCmpr&core.DATA_CMPR_BR != 0 {
			cmpr = &archiver.Brotli{Quality: int(cfg.CmprQlty)}
		}

		if cmpr != nil {
			var cmprBuf bytes.Buffer
			err := cmpr.Compress(bytes.NewBuffer(data), &cmprBuf)
			if err == nil && cmprBuf.Len() < len(data) {
				// 压缩成功且压缩后更小
				compressedData = cmprBuf.Bytes()
				data = compressedData
			} else {
				// 压缩失败或压缩后更大，移除压缩标记
				kind &= ^core.DATA_CMPR_MASK
				compressedData = originalData
				data = originalData
			}
		}
	}

	// 2. 加密（如果启用）
	hasEnc := kind&core.DATA_ENDEC_MASK != 0
	if hasEnc && cfg != nil {
		if kind&core.DATA_ENDEC_AES256 != 0 {
			data, err = aes256.Encrypt(cfg.EndecKey, data)
			if err != nil {
				// 加密失败，移除加密标记，使用压缩后的数据（如果有压缩）或原始数据
				kind &= ^core.DATA_ENDEC_MASK
				data = compressedData
			}
		} else if kind&core.DATA_ENDEC_SM4 != 0 {
			data, err = sm4.Sm4Cbc([]byte(cfg.EndecKey), data, true)
			if err != nil {
				// 加密失败，移除加密标记，使用压缩后的数据（如果有压缩）或原始数据
				kind &= ^core.DATA_ENDEC_MASK
				data = compressedData
			}
		}
	}

	return data, kind, origSize
}

// flushPackage 刷新单个打包数据块（批量写入数据块、DataInfo和ObjectInfo）
func (bwm *BatchWriteManager) flushPackage(pkg *PackagedDataBlock) error {
	if len(pkg.FileInfos) == 0 || len(pkg.Data) == 0 {
		return nil
	}

	startTime := time.Now()
	var newIDTime, putDataTime, putDataInfoTime, putObjTime time.Duration

	// 从BatchWriteManager获取fs和handler
	if bwm.fs == nil {
		return fmt.Errorf("invalid batch write manager: fs is nil")
	}

	fs := bwm.fs
	lh := fs.h
	bktID := fs.bktID
	ctx := fs.c

	// 1. 生成打包数据块的ID
	newIDStart := time.Now()
	pkgID := lh.NewID()
	newIDTime = time.Since(newIDStart)
	if pkgID <= 0 {
		return fmt.Errorf("failed to generate package ID")
	}
	pkg.PkgID = pkgID

	// 2. 写入打包数据块
	putDataStart := time.Now()
	_, err := lh.PutData(ctx, bktID, pkgID, 0, pkg.Data)
	putDataTime = time.Since(putDataStart)
	if err != nil {
		return fmt.Errorf("failed to write package data: %v", err)
	}

	// 3. 为每个文件创建DataInfo（使用PkgID和PkgOffset）
	var dataInfos []*core.DataInfo
	var objectInfos []*core.ObjectInfo
	var newIDLoopTime, getObjTime time.Duration

	loopStart := time.Now()
	for _, fileInfo := range pkg.FileInfos {
		// 生成DataID
		newIDLoopStart := time.Now()
		dataID := lh.NewID()
		newIDLoopTime += time.Since(newIDLoopStart)
		if dataID <= 0 {
			continue
		}
		fileInfo.DataID = dataID

		// 创建DataInfo
		dataInfo := &core.DataInfo{
			ID:        dataID,
			Size:      fileInfo.Size,
			OrigSize:  fileInfo.OrigSize,
			Kind:      fileInfo.Kind,
			PkgID:     pkgID,
			PkgOffset: fileInfo.PkgOffset,
		}
		dataInfos = append(dataInfos, dataInfo)

		// 获取文件对象，更新DataID和Size
		// 注意：文件大小应该使用原始大小（OrigSize），而不是压缩加密后的大小
		// 优化：先尝试从缓存获取，避免数据库查询
		getObjStart := time.Now()
		cacheKey := formatCacheKey(bktID, fileInfo.FileID)
		var fileObj *core.ObjectInfo
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
				fileObj = obj
			}
		}
		if fileObj == nil {
			// 缓存未命中，从数据库查询
			fileObjs, err := lh.Get(ctx, bktID, []int64{fileInfo.FileID})
			getObjTime += time.Since(getObjStart)
			if err != nil || len(fileObjs) == 0 {
				fileObj = nil
			} else {
				fileObj = fileObjs[0]
				// 更新缓存
				fileObjCache.Put(cacheKey, fileObj)
			}
		} else {
			getObjTime += time.Since(getObjStart)
		}

		if fileObj == nil {
			// 文件对象不存在，创建新的
			objectInfo := &core.ObjectInfo{
				ID:     fileInfo.FileID,
				DataID: dataID,
				Size:   fileInfo.OrigSize, // 使用原始大小
				MTime:  core.Now(),
				Type:   core.OBJ_TYPE_FILE,
			}
			objectInfos = append(objectInfos, objectInfo)
		} else {
			// 更新已有的文件对象
			objectInfo := &core.ObjectInfo{
				ID:     fileObj.ID,
				PID:    fileObj.PID,
				DataID: dataID,
				Size:   fileInfo.OrigSize, // 使用原始大小
				MTime:  core.Now(),
				Type:   fileObj.Type,
				Name:   fileObj.Name,
			}
			objectInfos = append(objectInfos, objectInfo)
		}
	}
	loopTime := time.Since(loopStart)

	// 4. 批量写入DataInfo
	putDataInfoStart := time.Now()
	if len(dataInfos) > 0 {
		_, err = lh.PutDataInfo(ctx, bktID, dataInfos)
		putDataInfoTime = time.Since(putDataInfoStart)
		if err != nil {
			return fmt.Errorf("failed to write data infos: %v", err)
		}
	} else {
		putDataInfoTime = time.Since(putDataInfoStart)
	}

	// 5. 批量写入ObjectInfo
	putObjStart := time.Now()
	if len(objectInfos) > 0 {
		_, err = lh.Put(ctx, bktID, objectInfos)
		putObjTime = time.Since(putObjStart)
		if err != nil {
			return fmt.Errorf("failed to write object infos: %v", err)
		}
	} else {
		putObjTime = time.Since(putObjStart)
	}

	totalTime := time.Since(startTime)
	if totalTime > 1*time.Millisecond {
		fmt.Printf("[PERF] flushPackage: pkgID=%d, dataSize=%d, files=%d, total=%v, newID=%v, putData=%v, loop=%v(newIDLoop=%v,getObj=%v), putDataInfo=%v, putObj=%v\n",
			pkgID, len(pkg.Data), len(pkg.FileInfos), totalTime, newIDTime, putDataTime, loopTime, newIDLoopTime, getObjTime, putDataInfoTime, putObjTime)
	}

	return nil
}

// addFile 无锁方式添加文件数据到批量写入缓冲区
// 如果空间不够，会先刷新已有数据，然后继续写入
func (bwm *BatchWriteManager) addFile(ra *RandomAccessor, data []byte) (bool, error) {
	if len(data) == 0 {
		return true, nil
	}

	// 性能分析：记录开始时间
	startTime := time.Now()
	var processTime, flushTime, copyTime, retryTime time.Duration
	var retryCount int

	// 根据配置处理压缩和加密
	processStart := time.Now()
	processedData, kind, origSize := bwm.processFileData(ra.fs, data)
	processTime = time.Since(processStart)
	if len(processedData) == 0 {
		return false, fmt.Errorf("failed to process file data")
	}

	processedSize := int64(len(processedData))

	// 尝试写入，可能需要重试（如果空间不够会先刷新）
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		retryStart := time.Now()
		retryCount++
		// 先检查当前空间（Load是原子的，但检查后可能被其他goroutine修改）
		currentOffset := bwm.writeOffset.Load()
		if currentOffset+processedSize > bwm.bufferSize {
			// 空间不够，先刷新已有数据
			if retry == 0 {
				flushStart := time.Now()
				bwm.flush()
				flushTime += time.Since(flushStart)
			} else {
				// 如果刷新后还是不够，说明数据太大，返回false让调用者fallback
				retryTime += time.Since(retryStart)
				return false, nil
			}
			// 刷新后继续重试
			retryTime += time.Since(retryStart)
			continue
		}

		// 使用Add直接占位（原子操作）
		newOffset := bwm.writeOffset.Add(processedSize)
		oldOffset := newOffset - processedSize

		// 再次检查是否超过缓冲区大小（因为占位时可能被其他goroutine修改）
		if newOffset > bwm.bufferSize {
			// 占位后发现空间不够，回退占位
			bwm.writeOffset.Add(-processedSize)
			if retry == 0 {
				flushStart := time.Now()
				bwm.flush()
				flushTime += time.Since(flushStart)
			} else {
				retryTime += time.Since(retryStart)
				return false, nil
			}
			// 刷新后继续重试
			retryTime += time.Since(retryStart)
			continue
		}

		// 写入数据到缓冲区
		// 注意：通过Add已经占位，所以oldOffset到newOffset这个位置是安全的
		copyStart := time.Now()
		copy(bwm.buffer[oldOffset:newOffset], processedData)
		copyTime += time.Since(copyStart)

		// 获取文件信息索引（原子操作）
		fileIndex := bwm.fileInfoIndex.Add(1) - 1
		if fileIndex >= bwm.maxFileInfos {
			// 文件信息列表满了，回退并刷新
			bwm.writeOffset.Add(-processedSize)
			bwm.fileInfoIndex.Add(-1)
			if retry == 0 {
				flushStart := time.Now()
				bwm.flush()
				flushTime += time.Since(flushStart)
			} else {
				retryTime += time.Since(retryStart)
				return false, nil
			}
			// 刷新后继续重试
			retryTime += time.Since(retryStart)
			continue
		}

		// 创建文件信息（无锁，因为我们已经通过Add获得了写入位置）
		bwm.fileInfos[fileIndex] = &BatchFileInfo{
			FileID:   ra.fileID,
			Offset:   oldOffset,
			Size:     processedSize,
			OrigSize: origSize,
			Kind:     kind,
		}

		retryTime += time.Since(retryStart)
		totalTime := time.Since(startTime)

		// 性能分析：只在并发场景下输出（通过goroutine ID判断）
		if totalTime > 100*time.Microsecond || retryCount > 1 {
			fmt.Printf("[PERF] addFile: fileID=%d, size=%d, total=%v, process=%v, flush=%v, copy=%v, retry=%v, retries=%d\n",
				ra.fileID, len(data), totalTime, processTime, flushTime, copyTime, retryTime, retryCount)
		}

		return true, nil
	}

	// 重试次数过多，返回错误（避免无限循环）
	return false, fmt.Errorf("failed to add file after %d retries: buffer may be too small", maxRetries)
}

// formatCacheKey 格式化缓存key（优化：直接内存拷贝，最高性能）
func formatCacheKey(bktID, id int64) string {
	// 在栈上创建固定大小的byte数组
	var buf [16]byte

	// 直接使用unsafe将8字节的int64内存拷贝到byte数组（最高性能）
	// 避免函数调用开销，直接内存操作
	*(*int64)(unsafe.Pointer(&buf[0])) = bktID
	*(*int64)(unsafe.Pointer(&buf[8])) = id
	return string(buf[:])
}

// WriteOperation 表示一次写入操作
type WriteOperation struct {
	Offset int64  // 写入偏移量
	Data   []byte // 写入的数据
}

// WriteBuffer 管理单个文件的写缓冲
type WriteBuffer struct {
	fileID     int64            // 文件对象ID
	operations []WriteOperation // 固定长度的写入操作数组（避免临时对象创建）
	writeIndex int64            // 当前写入位置（使用原子操作，从0开始）
	totalSize  int64            // 缓冲区总大小（使用原子操作优化）
}

// SequentialWriteBuffer 顺序写缓冲区（优化：从0开始的顺序写）
type SequentialWriteBuffer struct {
	fileID    int64          // 文件对象ID
	dataID    int64          // 数据对象ID（新建对象时创建）
	sn        int            // 当前数据块序号
	chunkSize int64          // chunk大小
	buffer    []byte         // 当前chunk缓冲区（最多一个chunk大小）
	offset    int64          // 当前写入位置（顺序写）
	hasData   bool           // 是否已有数据写入
	closed    bool           // 是否已关闭（变为随机写）
	dataInfo  *core.DataInfo // 数据信息
}

// RandomAccessor VFS中的随机访问对象，支持压缩和加密
type RandomAccessor struct {
	fs           *OrcasFS
	fileID       int64
	buffer       *WriteBuffer           // 随机写缓冲区
	seqBuffer    *SequentialWriteBuffer // 顺序写缓冲区（优化）
	fileObj      atomic.Value
	fileObjKey   string      // 预计算的file_obj缓存key（优化：避免重复转换）
	pendingFlush *time.Timer // 延迟刷新定时器
	pendingMu    sync.Mutex  // 保护pendingFlush
}

// NewRandomAccessor 创建随机访问对象
func NewRandomAccessor(fs *OrcasFS, fileID int64) (*RandomAccessor, error) {
	// 获取配置以初始化固定长度的operations数组（优化：避免临时对象创建）
	maxBufferWrites := int(core.GetWriteBufferConfig().MaxBufferWrites)
	if maxBufferWrites <= 0 {
		maxBufferWrites = 200 // 默认值
	}

	ra := &RandomAccessor{
		fs:         fs,
		fileID:     fileID,
		fileObjKey: formatCacheKey(fs.bktID, fileID), // 预计算并缓存key
		buffer: &WriteBuffer{
			fileID:     fileID,
			operations: make([]WriteOperation, maxBufferWrites), // 固定长度数组
			writeIndex: 0,                                       // 从0开始
		},
	}
	return ra, nil
}

// Write 添加写入操作到缓冲区
// 优化：顺序写优化 - 如果是从0开始的顺序写，直接写入数据块，避免缓存
func (ra *RandomAccessor) Write(offset int64, data []byte) error {
	// 检查是否是顺序写模式
	if ra.seqBuffer != nil && !ra.seqBuffer.closed {
		// 检查是否仍然是顺序写（从当前位置继续写）
		if offset == ra.seqBuffer.offset {
			// 顺序写，使用优化路径
			return ra.writeSequential(offset, data)
		} else if offset < ra.seqBuffer.offset {
			// 往回写，转为随机写模式
			if err := ra.flushSequentialBuffer(); err != nil {
				return err
			}
			ra.seqBuffer.closed = true
		} else {
			// 跳过了某些位置，转为随机写模式
			if err := ra.flushSequentialBuffer(); err != nil {
				return err
			}
			ra.seqBuffer.closed = true
		}
	}

	// 初始化顺序写缓冲区（如果是从0开始顺序写，且文件还没有数据）
	if ra.seqBuffer == nil && offset == 0 && len(data) > 0 {
		fileObj, err := ra.getFileObj()
		if err == nil && (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) {
			// 文件没有数据，可以初始化顺序写缓冲区
			if initErr := ra.initSequentialBuffer(); initErr == nil {
				// 初始化成功，使用顺序写
				return ra.writeSequential(offset, data)
			}
			// 初始化失败，fallback到随机写
		}
	}

	// 随机写模式：使用原有的缓冲区逻辑
	// 优化：减少数据复制，只有在必要时才复制
	// 检查是否超出容量（优化：提前检查，避免越界）
	config := core.GetWriteBufferConfig()
	if atomic.LoadInt64(&ra.buffer.writeIndex)+1 >= int64(len(ra.buffer.operations)) || atomic.LoadInt64(&ra.buffer.totalSize) >= config.MaxBufferSize {
		// 超出容量，需要强制刷新
		// 先回退writeIndex（因为已经超出了）
		atomic.AddInt64(&ra.buffer.writeIndex, -1)
		// 强制刷新当前缓冲区（同步执行，确保数据被持久化）
		_, err := ra.Flush()
		if err != nil {
			return err
		}
	}

	// 刷新后，重新获取写入位置（此时writeIndex应该已经被Flush重置为0）
	writeIndex := atomic.AddInt64(&ra.buffer.writeIndex, 1) - 1

	// 优化：使用原子操作更新totalSize，减少锁竞争
	atomic.AddInt64(&ra.buffer.totalSize, int64(len(data)))

	// 写入新的数据
	ra.buffer.operations[writeIndex].Offset = offset
	ra.buffer.operations[writeIndex].Data = make([]byte, len(data))
	copy(ra.buffer.operations[writeIndex].Data, data)

	// 优化：对于小文件写入，使用批量写入管理器
	// 如果数据量小且未达到强制刷新条件，添加到批量写入管理器
	// 注意：批量写入只适用于小文件，且需要确保数据完整性
	// 这里先使用延迟刷新，批量写入的逻辑在Flush时处理
	if int64(len(data)) < config.MaxBufferSize/10 && atomic.LoadInt64(&ra.buffer.totalSize) < config.MaxBufferSize {
		ra.scheduleDelayedFlush(config.BufferWindow)
	}

	return nil
}

// scheduleDelayedFlush 安排延迟刷新
func (ra *RandomAccessor) scheduleDelayedFlush(window time.Duration) {
	ra.pendingMu.Lock()
	defer ra.pendingMu.Unlock()

	// 取消之前的定时器
	if ra.pendingFlush != nil {
		ra.pendingFlush.Stop()
	}

	// 创建新的延迟刷新定时器
	ra.pendingFlush = time.AfterFunc(window, func() {
		ra.pendingMu.Lock()
		defer ra.pendingMu.Unlock()
		ra.pendingFlush = nil
		// 异步刷新，不阻塞
		go func() {
			_, _ = ra.Flush()
		}()
	})
}

// initSequentialBuffer 初始化顺序写缓冲区
func (ra *RandomAccessor) initSequentialBuffer() error {
	fileObj, err := ra.getFileObj()
	if err != nil {
		return err
	}

	// 如果文件已有数据，不能使用顺序写优化
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		return fmt.Errorf("file already has data")
	}

	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 4 << 20 // 默认4MB
	}

	// 创建新的数据对象
	newDataID := ra.fs.h.NewID()

	// 初始化DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: 0,
		Size:     0,
		CRC32:    0,
		Cksum:    0,
		Kind:     0,
	}

	// 设置压缩和加密标记（如果启用）
	cfg := ra.fs.sdkCfg
	if cfg != nil {
		if cfg.WiseCmpr > 0 {
			dataInfo.Kind |= cfg.WiseCmpr
		}
		if cfg.EndecWay > 0 {
			dataInfo.Kind |= cfg.EndecWay
		}
	}

	ra.seqBuffer = &SequentialWriteBuffer{
		fileID:    ra.fileID,
		dataID:    newDataID,
		sn:        0,
		chunkSize: chunkSize,
		buffer:    make([]byte, 0, chunkSize), // 预分配但长度为0
		offset:    0,
		hasData:   false,
		closed:    false,
		dataInfo:  dataInfo,
	}

	return nil
}

// writeSequential 顺序写数据（优化路径）
func (ra *RandomAccessor) writeSequential(offset int64, data []byte) error {
	if ra.seqBuffer == nil || ra.seqBuffer.closed {
		return fmt.Errorf("sequential buffer not available")
	}

	// 确保offset是顺序的
	if offset != ra.seqBuffer.offset {
		return fmt.Errorf("non-sequential write detected")
	}

	dataLeft := data
	for len(dataLeft) > 0 {
		// 计算当前chunk还剩余多少空间
		remainingInChunk := ra.seqBuffer.chunkSize - int64(len(ra.seqBuffer.buffer))
		if remainingInChunk <= 0 {
			// 当前chunk已满，写入并清空
			if err := ra.flushSequentialChunk(); err != nil {
				return err
			}
			remainingInChunk = ra.seqBuffer.chunkSize
		}

		// 计算本次能写入多少数据
		writeSize := int64(len(dataLeft))
		if writeSize > remainingInChunk {
			writeSize = remainingInChunk
		}

		// 写入到当前chunk缓冲区
		ra.seqBuffer.buffer = append(ra.seqBuffer.buffer, dataLeft[:writeSize]...)
		ra.seqBuffer.offset += writeSize
		ra.seqBuffer.hasData = true
		dataLeft = dataLeft[writeSize:]

		// 如果chunk满了，立即写入
		if int64(len(ra.seqBuffer.buffer)) >= ra.seqBuffer.chunkSize {
			if err := ra.flushSequentialChunk(); err != nil {
				return err
			}
		}
	}

	return nil
}

// flushSequentialChunk 刷新当前顺序写chunk（写入一个完整chunk）
func (ra *RandomAccessor) flushSequentialChunk() error {
	if ra.seqBuffer == nil || len(ra.seqBuffer.buffer) == 0 {
		return nil
	}

	cfg := ra.fs.sdkCfg
	chunkData := ra.seqBuffer.buffer

	// 处理第一个chunk：检查文件类型和压缩效果
	isFirstChunk := ra.seqBuffer.sn == 0
	if isFirstChunk && cfg != nil && cfg.WiseCmpr > 0 && len(chunkData) > 0 {
		kind, _ := filetype.Match(chunkData)
		if kind != filetype.Unknown {
			// 不是未知类型，不压缩
			ra.seqBuffer.dataInfo.Kind &= ^core.DATA_CMPR_MASK
		}
	}

	// 更新原始数据的CRC32和大小
	ra.seqBuffer.dataInfo.CRC32 = crc32.Update(ra.seqBuffer.dataInfo.CRC32, crc32.IEEETable, chunkData)
	ra.seqBuffer.dataInfo.OrigSize += int64(len(chunkData))

	// 压缩（如果启用）
	var processedChunk []byte
	hasCmpr := ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0
	if hasCmpr && cfg != nil && cfg.WiseCmpr > 0 {
		var cmpr archiver.Compressor
		if cfg.WiseCmpr&core.DATA_CMPR_SNAPPY != 0 {
			cmpr = &archiver.Snappy{}
		} else if cfg.WiseCmpr&core.DATA_CMPR_ZSTD != 0 {
			cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(cfg.CmprQlty)))}}
		} else if cfg.WiseCmpr&core.DATA_CMPR_GZIP != 0 {
			cmpr = &archiver.Gz{CompressionLevel: int(cfg.CmprQlty)}
		} else if cfg.WiseCmpr&core.DATA_CMPR_BR != 0 {
			cmpr = &archiver.Brotli{Quality: int(cfg.CmprQlty)}
		}

		if cmpr != nil {
			var cmprBuf bytes.Buffer
			err := cmpr.Compress(bytes.NewBuffer(chunkData), &cmprBuf)
			if err != nil {
				// 压缩失败，只在第一个chunk时移除压缩标记
				if isFirstChunk {
					ra.seqBuffer.dataInfo.Kind &= ^core.DATA_CMPR_MASK
				}
				processedChunk = chunkData
			} else {
				// 如果压缩后更大或相等，只在第一个chunk时移除压缩标记
				if isFirstChunk && cmprBuf.Len() >= len(chunkData) {
					processedChunk = chunkData
					ra.seqBuffer.dataInfo.Kind &= ^core.DATA_CMPR_MASK
				} else {
					processedChunk = cmprBuf.Bytes()
				}
			}
		} else {
			processedChunk = chunkData
		}
	} else {
		processedChunk = chunkData
	}

	// 加密（如果启用）
	var encodedChunk []byte
	var err error
	if cfg != nil && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
		encodedChunk, err = aes256.Encrypt(cfg.EndecKey, processedChunk)
	} else if cfg != nil && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
		encodedChunk, err = sm4.Sm4Cbc([]byte(cfg.EndecKey), processedChunk, true)
	} else {
		encodedChunk = processedChunk
	}
	if err != nil {
		encodedChunk = processedChunk
	}

	// 更新最终数据的CRC32
	ra.seqBuffer.dataInfo.Cksum = crc32.Update(ra.seqBuffer.dataInfo.Cksum, crc32.IEEETable, encodedChunk)

	// 更新大小（如果压缩或加密了）
	if ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0 || ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
		ra.seqBuffer.dataInfo.Size += int64(len(encodedChunk))
	}

	// 写入数据块
	if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, ra.seqBuffer.dataID, ra.seqBuffer.sn, encodedChunk); err != nil {
		return err
	}

	ra.seqBuffer.sn++
	ra.seqBuffer.buffer = ra.seqBuffer.buffer[:0] // 清空缓冲区但保留容量
	return nil
}

// flushSequentialBuffer 刷新整个顺序写缓冲区（写入最后一个chunk并完成）
func (ra *RandomAccessor) flushSequentialBuffer() error {
	if ra.seqBuffer == nil {
		return nil
	}

	// 写入最后一个chunk（如果还有数据）
	if len(ra.seqBuffer.buffer) > 0 {
		if err := ra.flushSequentialChunk(); err != nil {
			return err
		}
	}

	// 如果没有数据，直接返回
	if !ra.seqBuffer.hasData {
		return nil
	}

	// 更新DataInfo的最终大小
	if ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK == 0 && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		ra.seqBuffer.dataInfo.Size = ra.seqBuffer.dataInfo.OrigSize
		ra.seqBuffer.dataInfo.Cksum = ra.seqBuffer.dataInfo.CRC32
	}

	// 保存DataInfo
	if _, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{ra.seqBuffer.dataInfo}); err != nil {
		return err
	}

	// 更新缓存
	dataInfoCache.Put(formatCacheKey(ra.fs.bktID, ra.seqBuffer.dataID), ra.seqBuffer.dataInfo)

	// 更新文件对象
	fileObj, err := ra.getFileObj()
	if err != nil {
		return err
	}

	fileObj.DataID = ra.seqBuffer.dataID
	fileObj.Size = ra.seqBuffer.dataInfo.OrigSize

	// 更新文件对象（使用Handler的Put方法）
	if _, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{fileObj}); err != nil {
		return err
	}

	// 更新缓存
	fileObjCache.Put(ra.fileObjKey, fileObj)
	ra.fileObj.Store(fileObj)

	return nil
}

func (ra *RandomAccessor) getFileObj() (*core.ObjectInfo, error) {
	fileObjValue := ra.fileObj.Load()
	if fileObjValue != nil {
		if obj, ok := fileObjValue.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}
	// 优化：使用预计算的key（避免重复转换）
	if cached, ok := fileObjCache.Get(ra.fileObjKey); ok {
		if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}
	// 如果缓存未命中，从数据库获取
	objs, err := ra.fs.h.Get(ra.fs.c, ra.fs.bktID, []int64{ra.fileID})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("file %d not found", ra.fileID)
	}
	// 更新缓存（使用预计算的key）
	fileObjCache.Put(ra.fileObjKey, objs[0])
	ra.fileObj.Store(objs[0])
	return objs[0], nil
}

// Read 读取指定位置的数据，会合并缓冲区中的写入
// 优化：使用原子指针读取fileObj，无锁并发读取
func (ra *RandomAccessor) Read(offset int64, size int) ([]byte, error) {
	// 优化：使用原子操作读取fileObj，无锁并发读取
	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// 如果顺序写缓冲区有数据，先从它读取
	if ra.seqBuffer != nil && ra.seqBuffer.hasData {
		// 顺序写缓冲区有数据，需要先刷新才能读取
		// 但读取时不应该刷新，所以这里只处理已经在文件对象中的数据
		// 如果顺序写缓冲区未关闭，说明数据还在缓冲区中，需要先刷新
		if !ra.seqBuffer.closed {
			// 顺序写未完成，先刷新到文件对象
			if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
				return nil, flushErr
			}
			ra.seqBuffer.closed = true
			// 重新获取文件对象
			var objErr error
			fileObj, objErr = ra.getFileObj()
			if objErr != nil {
				return nil, objErr
			}
		}
	}

	// 如果没有数据ID，直接处理缓冲区写入操作
	if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
		return ra.readFromBuffer(offset, size), nil
	}

	// 获取DataInfo
	dataInfoCacheKey := formatCacheKey(ra.fs.bktID, fileObj.DataID)
	var dataInfo *core.DataInfo
	if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
		if info, ok := cached.(*core.DataInfo); ok && info != nil {
			dataInfo = info
		}
	}

	// 如果缓存未命中，从数据库获取
	if dataInfo == nil {
		var err error
		dataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
		if err != nil {
			// 如果获取DataInfo失败，尝试直接读取（可能是旧数据格式）
			data, readErr := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, fileObj.DataID, 0)
			if readErr == nil && len(data) > 0 {
				return ra.readFromDataAndBuffer(data, offset, size), nil
			}
			return ra.readFromBuffer(offset, size), nil
		}
		// 更新缓存
		dataInfoCache.Put(dataInfoCacheKey, dataInfo)
	}

	// 检查数据是否有压缩或加密
	hasCompression := dataInfo.Kind&core.DATA_CMPR_MASK != 0
	hasEncryption := dataInfo.Kind&core.DATA_ENDEC_MASK != 0

	// 创建数据读取器（抽象读取接口，统一处理未压缩和压缩加密的数据）
	var reader dataReader
	if !hasCompression && !hasEncryption {
		// 未压缩未加密：直接按chunk读取
		reader = newPlainDataReader(ra.fs.c, ra.fs.h, ra.fs.bktID, fileObj.DataID, ra.fs.chunkSize)
	} else {
		// 压缩/加密：使用decodingChunkReader（会自动解密解压）
		var endecKey string
		if ra.fs.sdkCfg != nil {
			endecKey = ra.fs.sdkCfg.EndecKey
		}
		reader = newDecodingChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, dataInfo, endecKey)
	}

	// 统一处理读取逻辑（包含写入操作的合并）
	fileData, operationsHandled := ra.readWithWrites(reader, offset, size)
	if operationsHandled {
		return fileData, nil
	}

	// 如果读取失败或未处理写入操作，从缓冲区读取
	return ra.readFromBuffer(offset, size), nil
}

// readFromDataAndBuffer 从数据和缓冲区读取并合并
func (ra *RandomAccessor) readFromDataAndBuffer(data []byte, offset int64, size int) []byte {
	// 获取缓冲区写入操作
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}

	// 合并写入操作
	mergedOps := mergeWriteOperations(operations)
	modifiedData := applyWritesToData(data, mergedOps)

	// 截取指定范围
	return ra.extractRange(modifiedData, offset, size)
}

// readFromBuffer 只从缓冲区读取（处理写入操作）
func (ra *RandomAccessor) readFromBuffer(offset int64, size int) []byte {
	// 获取缓冲区写入操作
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}

	// 合并写入操作
	mergedOps := mergeWriteOperations(operations)
	var modifiedData []byte
	if len(mergedOps) > 0 {
		// 计算需要的数据大小
		var maxSize int64
		for _, op := range mergedOps {
			opEnd := op.Offset + int64(len(op.Data))
			if opEnd > maxSize {
				maxSize = opEnd
			}
		}
		modifiedData = applyWritesToData(nil, mergedOps)
	} else {
		modifiedData = []byte{}
	}

	// 截取指定范围
	return ra.extractRange(modifiedData, offset, size)
}

// extractRange 从数据中截取指定范围
func (ra *RandomAccessor) extractRange(data []byte, offset int64, size int) []byte {
	if offset >= int64(len(data)) {
		return []byte{}
	}

	end := offset + int64(size)
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	if offset >= end {
		return []byte{}
	}

	// 如果返回的是整个slice，直接返回，否则创建新的slice
	if offset == 0 && end == int64(len(data)) {
		return data
	}

	result := make([]byte, end-offset)
	copy(result, data[offset:end])
	return result
}

// Flush 刷新缓冲区，返回新版本ID（如果没有待刷新数据返回0）
func (ra *RandomAccessor) Flush() (int64, error) {
	// 如果顺序写缓冲区有数据，先刷新它
	if ra.seqBuffer != nil && ra.seqBuffer.hasData && !ra.seqBuffer.closed {
		if err := ra.flushSequentialBuffer(); err != nil {
			return 0, err
		}
		// 顺序写完成后，关闭顺序缓冲区
		ra.seqBuffer.closed = true
		// 顺序写完成后返回新版本ID（实际上就是当前的DataID对应的版本）
		fileObj, err := ra.getFileObj()
		if err != nil {
			return 0, err
		}
		if fileObj.DataID > 0 {
			return ra.fs.h.NewID(), nil // 返回新的版本ID
		}
	}

	// 优化：使用原子操作获取并清空操作（无锁）
	// 原子地交换writeIndex并重置为0，获取实际的操作数量
	writeIndex := atomic.SwapInt64(&ra.buffer.writeIndex, 0)
	if writeIndex <= 0 {
		return 0, nil
	}

	// 检查是否是小文件，适合批量写入
	totalSize := atomic.LoadInt64(&ra.buffer.totalSize)
	fileObj, err := ra.getFileObj()
	if err == nil && totalSize > 0 && totalSize < 1<<20 { // 1MB阈值
		// 小文件，合并所有写入操作后添加到批量写入管理器
		operations := make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
		mergedOps := mergeWriteOperations(operations)

		// 计算合并后的数据总大小
		var mergedDataSize int64
		for _, op := range mergedOps {
			if op.Offset+int64(len(op.Data)) > mergedDataSize {
				mergedDataSize = op.Offset + int64(len(op.Data))
			}
		}

		// 如果数据是连续的或可以合并成连续数据，使用批量写入
		if mergedDataSize < 1<<20 && len(mergedOps) <= 10 { // 小文件且写入操作不多
			// 将所有数据合并成一个连续的数据块
			mergedData := make([]byte, mergedDataSize)
			for _, op := range mergedOps {
				copy(mergedData[op.Offset:], op.Data)
			}

			// 添加到批量写入管理器（无锁方式）
			batchMgr := ra.fs.getBatchWriteManager()
			added, err := batchMgr.addFile(ra, mergedData)
			if err != nil {
				// 处理失败，fallback到普通写入
			} else if !added {
				// 缓冲区满了，立即刷新
				batchMgr.flushAll()
				// 重试添加
				added, err = batchMgr.addFile(ra, mergedData)
				if !added {
					// 重试后还是失败，fallback到普通写入
				}
			}

			if added {
				// 成功添加到批量写入管理器，重置缓冲区
				atomic.StoreInt64(&ra.buffer.totalSize, 0)
				return 0, nil
			}
			// 如果添加失败，继续使用普通写入流程
		}
	}

	// 复制实际使用的部分（避免在flush期间被修改）
	operations := make([]WriteOperation, writeIndex)
	copy(operations, ra.buffer.operations[:writeIndex])

	// 优化：使用原子操作重置totalSize
	atomic.StoreInt64(&ra.buffer.totalSize, 0)

	// 合并重叠的写入操作
	mergedOps := mergeWriteOperations(operations)

	// 获取文件对象信息（更新缓存）
	// 优化：使用更高效的key生成（函数内部已使用对象池）
	// 注意：fileObj在上面已经获取过了，这里重新获取以确保是最新的
	fileObj, err = ra.getFileObj()
	if err != nil {
		return 0, err
	}

	// 使用SDK的listener来处理压缩和加密
	return ra.applyRandomWritesWithSDK(fileObj, mergedOps)
}

// applyRandomWritesWithSDK 使用SDK的listener处理压缩和加密，应用随机写入
// 优化为流式处理，避免大对象占用过多内存
func (ra *RandomAccessor) applyRandomWritesWithSDK(fileObj *core.ObjectInfo, writes []WriteOperation) (int64, error) {
	// 获取LocalHandler以访问ig, ma, da
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	// 创建新数据ID
	newDataID := ra.fs.h.NewID()

	// 计算新文件大小
	newSize := fileObj.Size
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > newSize {
			newSize = writeEnd
		}
	}

	// 获取chunk大小
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 4 << 20 // 默认4MB
	}
	chunkSizeInt := int(chunkSize)

	// 检查原数据是否已压缩或加密（优化：使用缓存）
	var oldDataInfo *core.DataInfo
	var hasCompression, hasEncryption bool
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// 优化：使用更高效的key生成（函数内部已使用对象池）
		dataInfoCacheKey := formatCacheKey(ra.fs.bktID, oldDataID)

		if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
			if info, ok := cached.(*core.DataInfo); ok && info != nil {
				oldDataInfo = info
			}
		}

		// 如果缓存未命中，从数据库获取
		if oldDataInfo == nil {
			var err error
			oldDataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, oldDataID)
			if err == nil && oldDataInfo != nil {
				// 更新缓存（重用已生成的key）
				dataInfoCache.Put(dataInfoCacheKey, oldDataInfo)
			}
		}

		if oldDataInfo != nil {
			hasCompression = oldDataInfo.Kind&core.DATA_CMPR_MASK != 0
			hasEncryption = oldDataInfo.Kind&core.DATA_ENDEC_MASK != 0
		}
	}

	// 创建DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: newSize,
		Kind:     core.DATA_NORMAL,
	}

	// 如果原数据已压缩或加密，必须完整读取（无法避免）
	// 但可以流式写入，避免一次性处理所有数据
	if hasCompression || hasEncryption {
		newVersionID, err := ra.applyWritesStreamingCompressed(oldDataInfo, writes, dataInfo, chunkSize, newSize)
		if err != nil {
			return 0, err
		}

		// 优化：使用时间校准器获取时间戳，减少time.Now()调用和GC压力
		// 创建新版本对象
		mTime := core.Now()
		newVersion := &core.ObjectInfo{
			ID:     newVersionID,
			PID:    ra.fileID,
			Type:   core.OBJ_TYPE_VERSION,
			DataID: newDataID,
			Size:   newSize,
			MTime:  mTime,
		}

		// 优化：批量写入元数据（将版本对象和文件对象更新一起写入）
		objectsToPut := []*core.ObjectInfo{newVersion}
		// 同时更新文件对象（如果文件对象本身需要更新）
		updateFileObj := &core.ObjectInfo{
			ID:     ra.fileID,
			DataID: newDataID,
			Size:   newSize,
		}
		objectsToPut = append(objectsToPut, updateFileObj)

		// 使用Put方法批量创建版本和更新文件对象（会自动应用版本保留策略）
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)

		// 更新缓存的文件对象信息
		if err == nil {
			// 优化：使用预计算的key（避免重复转换）
			fileObjCache.Put(ra.fileObjKey, updateFileObj)
			ra.fileObj.Store(updateFileObj)
		}

		return newVersionID, err
	}

	// 对于未压缩未加密的数据，可以按chunk流式读取和处理
	newVersionID, err := ra.applyWritesStreamingUncompressed(fileObj, oldDataInfo, writes, dataInfo, chunkSizeInt, newSize)
	if err != nil {
		return 0, err
	}

	// 优化：使用时间校准器获取时间戳，减少time.Now()调用和GC压力
	// 创建新版本对象
	mTime := core.Now()
	newVersion := &core.ObjectInfo{
		ID:     newVersionID,
		PID:    ra.fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// 优化：批量写入元数据（将版本对象和文件对象更新一起写入）
	objectsToPut := []*core.ObjectInfo{newVersion}
	// 同时更新文件对象（如果文件对象本身需要更新）
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		DataID: newDataID,
		Size:   newSize,
	}
	objectsToPut = append(objectsToPut, updateFileObj)

	// 使用Put方法批量创建版本和更新文件对象（会自动应用版本保留策略）
	_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)

	// 更新缓存的文件对象信息
	if err == nil {
		// 优化：使用预计算的key（避免重复转换）
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
	}

	return newVersionID, err
}

// applyWritesStreamingCompressed 处理已压缩或加密的数据
// 流式处理：按chunk读取原数据，应用写入操作，立即处理并写入新对象
func (ra *RandomAccessor) applyWritesStreamingCompressed(oldDataInfo *core.DataInfo, writes []WriteOperation,
	dataInfo *core.DataInfo, chunkSize int64, newSize int64,
) (int64, error) {
	// 现在每个chunk是独立压缩加密的，可以按chunk流式处理
	// 直接按chunk读取、解密、解压，不使用DataReader

	var endecKey string
	if ra.fs.sdkCfg != nil {
		endecKey = ra.fs.sdkCfg.EndecKey
	}

	// 创建一个reader来按chunk读取、解密、解压
	reader := newDecodingChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, oldDataInfo, endecKey)

	chunkSizeInt := int(chunkSize)
	if chunkSizeInt <= 0 {
		chunkSizeInt = 4 << 20 // 默认4MB
	}

	// 预先计算每个chunk相关的写入操作索引
	chunkCount := int((newSize + chunkSize - 1) / chunkSize)
	writesByChunk := make([][]int, chunkCount)
	avgWritesPerChunk := len(writes) / chunkCount
	if avgWritesPerChunk < 1 {
		avgWritesPerChunk = 1
	}
	for i := range writesByChunk {
		writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
	}

	for i, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		startChunk := int(write.Offset / chunkSize)
		endChunk := int((writeEnd + chunkSize - 1) / chunkSize)
		if endChunk >= chunkCount {
			endChunk = chunkCount - 1
		}
		if startChunk < 0 {
			startChunk = 0
		}
		for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
			writesByChunk[chunkIdx] = append(writesByChunk[chunkIdx], i)
		}
	}

	// 流式处理：按chunk读取、处理、写入
	return ra.processWritesStreaming(reader, writesByChunk, writes, dataInfo, chunkSizeInt, newSize, chunkCount)
}

// applyWritesStreamingUncompressed 处理未压缩未加密的数据
// 可以按chunk流式读取和处理，只读取受影响的数据范围
func (ra *RandomAccessor) applyWritesStreamingUncompressed(fileObj *core.ObjectInfo, oldDataInfo *core.DataInfo,
	writes []WriteOperation, dataInfo *core.DataInfo, chunkSizeInt int, newSize int64,
) (int64, error) {
	// 对于未压缩未加密的数据，可以按chunk流式处理
	// 优化：预先为每个chunk计算相关的写入操作，减少循环中的重复检查

	// 如果没有SDK配置，直接流式写入
	if ra.fs.sdkCfg == nil {
		dataInfo.Size = newSize
		dataInfo.OrigSize = newSize

		oldDataID := fileObj.DataID
		sn := 0

		// 预先计算每个chunk相关的写入操作索引（优化：减少重复检查）
		// 优化：预估容量，减少slice扩容
		chunkCount := int((newSize + int64(chunkSizeInt) - 1) / int64(chunkSizeInt))
		writesByChunk := make([][]int, chunkCount)
		// 预估每个chunk平均有多少写入操作（优化：减少slice扩容）
		avgWritesPerChunk := len(writes) / chunkCount
		if avgWritesPerChunk < 1 {
			avgWritesPerChunk = 1
		}
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}

		for i, write := range writes {
			writeEnd := write.Offset + int64(len(write.Data))
			startChunk := int(write.Offset / int64(chunkSizeInt))
			endChunk := int((writeEnd + int64(chunkSizeInt) - 1) / int64(chunkSizeInt))
			if endChunk >= chunkCount {
				endChunk = chunkCount - 1
			}
			if startChunk < 0 {
				startChunk = 0
			}
			for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
				writesByChunk[chunkIdx] = append(writesByChunk[chunkIdx], i)
			}
		}

		// 按chunk处理
		for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
			pos := int64(chunkIdx * chunkSizeInt)
			chunkEnd := pos + int64(chunkSizeInt)
			if chunkEnd > newSize {
				chunkEnd = newSize
			}
			chunkSize := int(chunkEnd - pos)

			// 读取原始chunk数据（优化：使用对象池重用缓冲区）
			chunkData := chunkDataPool.Get().([]byte)
			// 确保容量足够
			if cap(chunkData) < chunkSize {
				chunkData = make([]byte, chunkSize)
			} else {
				chunkData = chunkData[:chunkSize]
			}

			if oldDataID > 0 && oldDataID != core.EmptyDataID && pos < fileObj.Size {
				readEnd := chunkEnd
				if readEnd > fileObj.Size {
					readEnd = fileObj.Size
				}
				// 读取原始数据的这一chunk
				data, err := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, oldDataID, 0, int(pos), int(readEnd-pos))
				if err == nil && len(data) > 0 {
					copy(chunkData, data)
				}
			}

			// 应用写入操作（只处理当前chunk相关的写入，使用预计算的索引）
			for _, writeIdx := range writesByChunk[chunkIdx] {
				write := writes[writeIdx]
				writeEnd := write.Offset + int64(len(write.Data))

				// 计算重叠范围（优化：减少重复计算）
				overlapStart := int64(0)
				if write.Offset > pos {
					overlapStart = write.Offset - pos
				}
				overlapEnd := int64(chunkSize)
				if writeEnd-pos < overlapEnd {
					overlapEnd = writeEnd - pos
				}
				if overlapStart < overlapEnd {
					writeDataStart := int64(0)
					if write.Offset < pos {
						writeDataStart = pos - write.Offset
					}
					copyLen := overlapEnd - overlapStart
					// 优化：直接使用slice，避免重复计算长度
					copy(chunkData[overlapStart:overlapEnd], write.Data[writeDataStart:writeDataStart+copyLen])
				}
			}

			// 写入chunk
			// 注意：需要复制数据，因为PutData可能会异步处理，而chunkData需要归还到对象池
			chunkDataCopy := make([]byte, len(chunkData))
			copy(chunkDataCopy, chunkData)
			// 写入前归还到对象池（重置长度但保留容量）
			chunkDataPool.Put(chunkData[:0])

			if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, chunkDataCopy); err != nil {
				return 0, err
			}
			sn++
		}

		// 保存数据元数据
		_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
		if err != nil {
			return 0, err
		}

		// 优化：使用更高效的key生成（函数内部已使用对象池）
		dataInfoCache.Put(formatCacheKey(ra.fs.bktID, dataInfo.ID), dataInfo)

		newVersionID := ra.fs.h.NewID()
		return newVersionID, nil
	}

	// 有SDK配置，需要压缩和加密
	// 流式处理：按chunk读取原数据，应用写入操作，立即处理并写入新对象

	// 预先计算每个chunk相关的写入操作索引
	chunkCount := int((newSize + int64(chunkSizeInt) - 1) / int64(chunkSizeInt))
	writesByChunk := make([][]int, chunkCount)
	avgWritesPerChunk := len(writes) / chunkCount
	if avgWritesPerChunk < 1 {
		avgWritesPerChunk = 1
	}
	for i := range writesByChunk {
		writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
	}

	for i, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		startChunk := int(write.Offset / int64(chunkSizeInt))
		endChunk := int((writeEnd + int64(chunkSizeInt) - 1) / int64(chunkSizeInt))
		if endChunk >= chunkCount {
			endChunk = chunkCount - 1
		}
		if startChunk < 0 {
			startChunk = 0
		}
		for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
			writesByChunk[chunkIdx] = append(writesByChunk[chunkIdx], i)
		}
	}

	// 对于未压缩未加密的数据，可以直接按chunk读取，不需要先读取全部数据
	// 创建一个特殊的reader来支持按chunk读取
	var reader io.Reader
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// 创建plainDataReader，支持按chunk读取
		reader = newPlainDataReader(ra.fs.c, ra.fs.h, ra.fs.bktID, oldDataID, int64(chunkSizeInt))
	}

	// 流式处理：按chunk读取、处理、写入
	return ra.processWritesStreaming(reader, writesByChunk, writes, dataInfo, chunkSizeInt, newSize, chunkCount)
}

// processWritesStreaming 流式处理写入操作
// 按chunk读取原数据，应用写入操作，立即处理（压缩/加密）并写入新对象
func (ra *RandomAccessor) processWritesStreaming(
	reader io.Reader,
	writesByChunk [][]int,
	writes []WriteOperation,
	dataInfo *core.DataInfo,
	chunkSizeInt int,
	newSize int64,
	chunkCount int,
) (int64, error) {
	cfg := ra.fs.sdkCfg

	// 初始化压缩器（如果启用智能压缩）
	var cmpr archiver.Compressor
	var hasCmpr bool
	if cfg != nil && cfg.WiseCmpr > 0 {
		// 压缩器将在处理第一个chunk时确定（需要检查文件类型）
		hasCmpr = true
		if cfg.WiseCmpr&core.DATA_CMPR_SNAPPY != 0 {
			cmpr = &archiver.Snappy{}
		} else if cfg.WiseCmpr&core.DATA_CMPR_ZSTD != 0 {
			cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(cfg.CmprQlty)))}}
		} else if cfg.WiseCmpr&core.DATA_CMPR_GZIP != 0 {
			cmpr = &archiver.Gz{CompressionLevel: int(cfg.CmprQlty)}
		} else if cfg.WiseCmpr&core.DATA_CMPR_BR != 0 {
			cmpr = &archiver.Brotli{Quality: int(cfg.CmprQlty)}
		}
		if cmpr != nil {
			dataInfo.Kind |= cfg.WiseCmpr
		}
	}

	// 如果设置了加密，设置加密标记
	if cfg != nil && cfg.EndecWay > 0 {
		dataInfo.Kind |= cfg.EndecWay
	}

	// 计算CRC32（原始数据）
	var crc32Val uint32
	var dataCRC32 uint32

	sn := 0
	currentPos := int64(0)
	firstChunk := true

	// 按chunk流式处理
	for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
		pos := int64(chunkIdx * chunkSizeInt)
		chunkEnd := pos + int64(chunkSizeInt)
		if chunkEnd > newSize {
			chunkEnd = newSize
		}
		actualChunkSize := int(chunkEnd - pos)

		// 从对象池获取chunk缓冲区
		chunkData := chunkDataPool.Get().([]byte)
		if cap(chunkData) < actualChunkSize {
			chunkData = make([]byte, actualChunkSize)
		} else {
			chunkData = chunkData[:actualChunkSize]
		}

		// 1. 从reader读取原数据的这个chunk
		// 注意：reader是顺序的，需要按顺序读取到当前位置
		if reader != nil {
			// 需要读取到当前chunk的位置
			bytesToSkip := pos - currentPos
			if bytesToSkip > 0 {
				// 跳过不需要的数据（读取并丢弃）
				skipBuf := make([]byte, bytesToSkip)
				_, err := io.ReadFull(reader, skipBuf)
				if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					chunkDataPool.Put(chunkData[:0])
					return 0, fmt.Errorf("failed to skip to chunk position: %w", err)
				}
			}
			currentPos = pos

			// 读取当前chunk的数据
			n, err := reader.Read(chunkData)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				chunkDataPool.Put(chunkData[:0])
				return 0, fmt.Errorf("failed to read chunk: %w", err)
			}
			// 如果读取的数据少于chunk大小，剩余部分保持为0（新数据）
			if n < actualChunkSize {
				// 清零剩余部分
				for i := n; i < actualChunkSize; i++ {
					chunkData[i] = 0
				}
			}
			currentPos = chunkEnd
		} else {
			// 没有原数据，初始化为0
			for i := range chunkData {
				chunkData[i] = 0
			}
		}

		// 2. 应用写入操作到当前chunk
		for _, writeIdx := range writesByChunk[chunkIdx] {
			write := writes[writeIdx]
			writeEnd := write.Offset + int64(len(write.Data))

			// 计算重叠范围
			overlapStart := int64(0)
			if write.Offset > pos {
				overlapStart = write.Offset - pos
			}
			overlapEnd := int64(actualChunkSize)
			if writeEnd-pos < overlapEnd {
				overlapEnd = writeEnd - pos
			}
			if overlapStart < overlapEnd {
				writeDataStart := int64(0)
				if write.Offset < pos {
					writeDataStart = pos - write.Offset
				}
				copyLen := overlapEnd - overlapStart
				copy(chunkData[overlapStart:overlapEnd], write.Data[writeDataStart:writeDataStart+copyLen])
			}
		}

		// 3. 如果是第一个chunk，检查文件类型确定是否压缩
		if firstChunk && cfg != nil && cfg.WiseCmpr > 0 && len(chunkData) > 0 {
			kind, _ := filetype.Match(chunkData)
			if kind != filetype.Unknown {
				// 不是未知类型，不压缩
				dataInfo.Kind &= ^core.DATA_CMPR_MASK
				cmpr = nil
				hasCmpr = false
			}
			firstChunk = false
		}

		// 4. 计算原始数据的CRC32
		dataCRC32 = crc32.Update(dataCRC32, crc32.IEEETable, chunkData)

		// 5. 压缩（如果启用）
		var processedChunk []byte
		if hasCmpr && cmpr != nil {
			var cmprBuf bytes.Buffer
			err := cmpr.Compress(bytes.NewBuffer(chunkData), &cmprBuf)
			if err != nil {
				processedChunk = chunkData
				// 压缩失败，只在第一个chunk时移除压缩标记
				if firstChunk {
					dataInfo.Kind &= ^core.DATA_CMPR_MASK
					hasCmpr = false
				}
			} else {
				// 如果压缩后更大或相等，使用原始数据，移除压缩标记
				// 注意：这个逻辑只在第一个chunk时执行，因为一旦决定压缩，后续chunk都应该保持一致
				if firstChunk && cmprBuf.Len() >= len(chunkData) {
					processedChunk = chunkData
					dataInfo.Kind &= ^core.DATA_CMPR_MASK
					hasCmpr = false
				} else if !firstChunk && cmprBuf.Len() >= len(chunkData) {
					// 后续chunk如果压缩后更大，仍然使用压缩后的数据（保持一致性）
					processedChunk = cmprBuf.Bytes()
				} else {
					processedChunk = cmprBuf.Bytes()
				}
			}
		} else {
			processedChunk = chunkData
		}

		// 6. 加密（如果启用）
		var encodedChunk []byte
		var err error
		if cfg != nil && dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
			encodedChunk, err = aes256.Encrypt(cfg.EndecKey, processedChunk)
		} else if cfg != nil && dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
			encodedChunk, err = sm4.Sm4Cbc([]byte(cfg.EndecKey), processedChunk, true)
		} else {
			encodedChunk = processedChunk
		}
		if err != nil {
			encodedChunk = processedChunk
		}

		// 7. 更新最终数据的校验和
		crc32Val = crc32.Update(crc32Val, crc32.IEEETable, encodedChunk)

		// 8. 更新大小（如果压缩或加密了）
		if dataInfo.Kind&core.DATA_CMPR_MASK != 0 || dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
			dataInfo.Size += int64(len(encodedChunk))
		}

		// 9. 立即写入新对象（流式写入）
		encodedChunkCopy := make([]byte, len(encodedChunk))
		copy(encodedChunkCopy, encodedChunk)
		if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, encodedChunkCopy); err != nil {
			chunkDataPool.Put(chunkData[:0])
			return 0, err
		}
		sn++

		// 归还chunkData到对象池
		chunkDataPool.Put(chunkData[:0])
	}

	// 设置CRC32和校验和
	dataInfo.CRC32 = dataCRC32
	if dataInfo.Kind&core.DATA_CMPR_MASK == 0 && dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		dataInfo.Size = dataInfo.OrigSize
		dataInfo.Cksum = dataCRC32
	} else {
		dataInfo.Cksum = crc32Val
	}

	// 保存数据元数据
	_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return 0, err
	}

	// 更新缓存
	dataInfoCache.Put(formatCacheKey(ra.fs.bktID, dataInfo.ID), dataInfo)

	newVersionID := ra.fs.h.NewID()
	return newVersionID, nil
}

// dataReader 数据读取器接口，统一处理不同格式的数据读取
type dataReader interface {
	io.Reader
}

// readWithWrites 统一处理读取逻辑：计算读取范围、读取数据、应用写入操作、截取结果
func (ra *RandomAccessor) readWithWrites(reader dataReader, offset int64, size int) ([]byte, bool) {
	// 1. 检查缓冲区中的写入操作，确定需要读取的数据范围
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}
	mergedOps := mergeWriteOperations(operations)

	// 2. 计算实际需要读取的数据范围（考虑写入操作的影响）
	readStart := offset
	readEnd := offset + int64(size)
	if len(mergedOps) > 0 {
		for _, op := range mergedOps {
			opEnd := op.Offset + int64(len(op.Data))
			if op.Offset < readEnd && opEnd > readStart {
				if op.Offset < readStart {
					readStart = op.Offset
				}
				if opEnd > readEnd {
					readEnd = opEnd
				}
			}
		}
	}

	// 3. 只读取需要的数据范围
	readSize := readEnd - readStart
	if readSize <= 0 {
		return []byte{}, true
	}

	// 跳过readStart之前的数据
	if readStart > 0 {
		skipBuf := make([]byte, readStart)
		_, err := io.ReadFull(reader, skipBuf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, false
		}
	}

	// 读取需要的数据范围
	readData := make([]byte, readSize)
	n, err := io.ReadFull(reader, readData)
	// 如果读取失败或读取的数据少于请求的大小，只返回读取到的数据
	if err != nil {
		// io.EOF 或 io.ErrUnexpectedEOF 表示已读取完所有可用数据
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if n > 0 {
				readData = readData[:n]
			} else {
				readData = []byte{}
			}
		} else {
			// 其他错误，返回失败
			return nil, false
		}
	} else if int64(n) < readSize {
		// 读取的数据少于请求的大小（文件末尾），截取实际读取的数据
		readData = readData[:n]
	}

	// 4. 应用写入操作到读取的数据
	if len(mergedOps) > 0 {
		adjustedOps := make([]WriteOperation, 0, len(mergedOps))
		for _, op := range mergedOps {
			opEnd := op.Offset + int64(len(op.Data))
			if op.Offset < readEnd && opEnd > readStart {
				adjustedOp := WriteOperation{
					Offset: op.Offset - readStart,
					Data:   op.Data,
				}
				adjustedOps = append(adjustedOps, adjustedOp)
			}
		}
		if len(adjustedOps) > 0 {
			readData = applyWritesToData(readData, adjustedOps)
		}
	}

	// 5. 截取请求的范围（offset到offset+size，相对于readStart）
	resultOffset := offset - readStart
	resultEnd := resultOffset + int64(size)
	if resultEnd > int64(len(readData)) {
		resultEnd = int64(len(readData))
	}
	if resultOffset < 0 {
		resultOffset = 0
	}
	if resultOffset >= resultEnd {
		return []byte{}, true
	}

	return readData[resultOffset:resultEnd], true
}

// plainDataReader 支持按chunk读取未压缩未加密的数据
type plainDataReader struct {
	c          core.Ctx
	h          core.Handler
	bktID      int64
	dataID     int64
	chunkSize  int64
	currentPos int64
	sn         int
	buf        []byte
	bufPos     int
}

func newPlainDataReader(c core.Ctx, h core.Handler, bktID, dataID int64, chunkSize int64) *plainDataReader {
	if chunkSize <= 0 {
		chunkSize = 4 << 20 // 默认4MB
	}
	return &plainDataReader{
		c:         c,
		h:         h,
		bktID:     bktID,
		dataID:    dataID,
		chunkSize: chunkSize,
	}
}

func (pr *plainDataReader) Read(p []byte) (n int, err error) {
	totalRead := 0
	for len(p) > 0 {
		// 如果缓冲区为空或已读完，读取下一个chunk
		if pr.buf == nil || pr.bufPos >= len(pr.buf) {
			// 使用sn来读取chunk（未压缩未加密的数据按chunk存储）
			chunkData, err := pr.h.GetData(pr.c, pr.bktID, pr.dataID, pr.sn)
			if err != nil {
				// 如果读取失败，可能是chunk不存在（文件末尾）
				if totalRead == 0 {
					return 0, io.EOF
				}
				return totalRead, nil
			}
			if len(chunkData) == 0 {
				if totalRead == 0 {
					return 0, io.EOF
				}
				return totalRead, nil
			}
			pr.buf = chunkData
			pr.bufPos = 0
			pr.sn++
		}

		// 从缓冲区复制数据
		copyLen := len(p)
		available := len(pr.buf) - pr.bufPos
		if copyLen > available {
			copyLen = available
		}
		copy(p[:copyLen], pr.buf[pr.bufPos:pr.bufPos+copyLen])

		pr.bufPos += copyLen
		pr.currentPos += int64(copyLen)
		totalRead += copyLen
		p = p[copyLen:]
	}

	return totalRead, nil
}

// decodingChunkReader 支持按chunk读取、解密、解压的数据（流式读取，边读边处理）
type decodingChunkReader struct {
	c          core.Ctx
	h          core.Handler
	bktID      int64
	dataID     int64
	kind       uint32
	endecKey   string
	currentPos int64
	sn         int
	buf        []byte
	bufPos     int
	remain     int
}

func newDecodingChunkReader(c core.Ctx, h core.Handler, bktID int64, dataInfo *core.DataInfo, endecKey string) *decodingChunkReader {
	dr := &decodingChunkReader{
		c:        c,
		h:        h,
		bktID:    bktID,
		kind:     dataInfo.Kind,
		endecKey: endecKey,
		remain:   int(dataInfo.Size),
	}
	if dataInfo.PkgID > 0 {
		dr.dataID = dataInfo.PkgID
	} else {
		dr.dataID = dataInfo.ID
	}
	return dr
}

func (dr *decodingChunkReader) Read(p []byte) (n int, err error) {
	totalRead := 0
	for len(p) > 0 && dr.remain > 0 {
		// 如果缓冲区为空或已读完，读取下一个chunk
		if dr.buf == nil || dr.bufPos >= len(dr.buf) {
			// 读取chunk（压缩加密后的数据）
			encryptedChunk, err := dr.h.GetData(dr.c, dr.bktID, dr.dataID, dr.sn)
			if err != nil {
				// 如果读取失败，可能是chunk不存在（文件末尾）
				if totalRead == 0 {
					return 0, io.EOF
				}
				return totalRead, nil
			}
			dr.remain -= len(encryptedChunk)
			dr.sn++

			// 1. 先解密（如果启用）
			decodedChunk := encryptedChunk
			if dr.kind&core.DATA_ENDEC_AES256 != 0 {
				decodedChunk, err = aes256.Decrypt(dr.endecKey, encryptedChunk)
			} else if dr.kind&core.DATA_ENDEC_SM4 != 0 {
				decodedChunk, err = sm4.Sm4Cbc([]byte(dr.endecKey), encryptedChunk, false)
			}
			if err != nil {
				// 解密失败，使用原始数据
				decodedChunk = encryptedChunk
			}

			// 2. 再解压缩（如果启用）
			finalChunk := decodedChunk
			if dr.kind&core.DATA_CMPR_MASK != 0 {
				var decompressor archiver.Decompressor
				if dr.kind&core.DATA_CMPR_SNAPPY != 0 {
					decompressor = &archiver.Snappy{}
				} else if dr.kind&core.DATA_CMPR_ZSTD != 0 {
					decompressor = &archiver.Zstd{}
				} else if dr.kind&core.DATA_CMPR_GZIP != 0 {
					decompressor = &archiver.Gz{}
				} else if dr.kind&core.DATA_CMPR_BR != 0 {
					decompressor = &archiver.Brotli{}
				}

				if decompressor != nil {
					var decompressedBuf bytes.Buffer
					err := decompressor.Decompress(bytes.NewReader(decodedChunk), &decompressedBuf)
					if err != nil {
						// 解压缩失败，使用解密后的数据
						finalChunk = decodedChunk
					} else {
						finalChunk = decompressedBuf.Bytes()
					}
				}
			}

			dr.buf = finalChunk
			dr.bufPos = 0
		}

		// 从缓冲区复制数据
		copyLen := len(p)
		available := len(dr.buf) - dr.bufPos
		if copyLen > available {
			copyLen = available
		}
		copy(p[:copyLen], dr.buf[dr.bufPos:dr.bufPos+copyLen])

		dr.bufPos += copyLen
		dr.currentPos += int64(copyLen)
		totalRead += copyLen
		p = p[copyLen:]
	}

	if dr.remain <= 0 && totalRead == 0 {
		return 0, io.EOF
	}
	return totalRead, nil
}

// Close 关闭随机访问对象，自动刷新所有待处理的写入
func (ra *RandomAccessor) Close() error {
	// 取消延迟刷新定时器，确保同步刷新
	ra.pendingMu.Lock()
	if ra.pendingFlush != nil {
		ra.pendingFlush.Stop()
		ra.pendingFlush = nil
	}
	ra.pendingMu.Unlock()

	// 同步刷新所有待写入数据
	_, err := ra.Flush()
	if err != nil {
		return err
	}

	// 确保批量写入的数据也被刷新（flush会刷新所有数据）
	batchMgr := ra.fs.getBatchWriteManager()
	batchMgr.flushAll()

	return nil
}

// mergeWriteOperations 合并重叠的写入操作
// 优化：使用更高效的排序算法（快速排序）
func mergeWriteOperations(operations []WriteOperation) []WriteOperation {
	if len(operations) == 0 {
		return nil
	}

	// 优化：如果operations已经是排序的，可以跳过排序
	// 但为了安全，还是进行排序（可以使用更高效的算法）
	// 优化：原地排序，避免额外的内存分配
	sorted := operations
	if len(sorted) > 1 {
		// 创建新slice用于排序（避免修改原slice）
		sorted = make([]WriteOperation, len(operations))
		copy(sorted, operations)
	}
	// 使用快速排序（内置sort包）
	// 但为了不引入新依赖，使用优化的插入排序
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j].Offset > key.Offset {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}

	// 优化：预分配容量，减少扩容
	// 优化：使用对象池获取初始容量
	merged := writeOpsPool.Get().([]WriteOperation)
	merged = merged[:0] // 重置但保留容量
	if cap(merged) < len(sorted) {
		merged = make([]WriteOperation, 0, len(sorted))
	}

	for _, op := range sorted {
		if len(merged) == 0 {
			merged = append(merged, op)
			continue
		}

		last := &merged[len(merged)-1]
		lastEnd := last.Offset + int64(len(last.Data))
		opEnd := op.Offset + int64(len(op.Data))

		// 优化：如果完全重叠且新操作覆盖旧操作，直接替换（避免创建新对象）
		if op.Offset >= last.Offset && opEnd <= lastEnd {
			// 新操作完全在旧操作内，直接覆盖（避免创建新的WriteOperation）
			offsetInLast := op.Offset - last.Offset
			copy(last.Data[offsetInLast:], op.Data)
			continue
		}

		// 如果重叠，合并
		if op.Offset <= lastEnd {
			// 计算新的范围
			startOffset := last.Offset
			if op.Offset < startOffset {
				startOffset = op.Offset
			}
			endOffset := lastEnd
			if opEnd > endOffset {
				endOffset = opEnd
			}

			// 优化：使用对象池获取缓冲区
			mergedData := chunkDataPool.Get().([]byte)
			if cap(mergedData) < int(endOffset-startOffset) {
				mergedData = make([]byte, endOffset-startOffset)
			} else {
				mergedData = mergedData[:endOffset-startOffset]
			}

			// 复制旧数据
			if last.Offset >= startOffset {
				copy(mergedData[last.Offset-startOffset:], last.Data)
			}
			// 复制新数据（覆盖）
			if op.Offset >= startOffset {
				copy(mergedData[op.Offset-startOffset:], op.Data)
			}

			last.Offset = startOffset
			last.Data = mergedData
		} else {
			// 不重叠，添加新操作
			merged = append(merged, op)
		}
	}

	return merged
}

// applyWritesToData 将写入操作应用到数据
// 优化：一次性计算所需大小，避免多次扩展
func applyWritesToData(data []byte, writes []WriteOperation) []byte {
	if len(writes) == 0 {
		return data
	}

	// 计算需要的大小（优化：一次性计算，避免多次扩展）
	var maxSize int64 = int64(len(data))
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > maxSize {
			maxSize = writeEnd
		}
	}

	// 优化：一次性分配所需大小，避免多次扩展
	result := make([]byte, maxSize)
	if len(data) > 0 {
		copy(result, data)
	}

	// 应用所有写入操作（优化：移除冗余检查，maxSize已保证容量足够）
	for _, write := range writes {
		if len(write.Data) > 0 {
			writeEnd := write.Offset + int64(len(write.Data))
			copy(result[write.Offset:writeEnd], write.Data)
		}
	}

	return result
}
