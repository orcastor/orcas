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
	// Object pool: reuse byte buffers to reduce memory allocation
	chunkDataPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4<<20) // Pre-allocate 4MB capacity
		},
	}

	// Object pool: reuse write operation slices
	writeOpsPool = sync.Pool{
		New: func() interface{} {
			return make([]WriteOperation, 0, 32)
		},
	}

	// ecache cache: cache DataInfo to reduce database queries
	// key: "<bktID>_<dataID>", value: *core.DataInfo
	dataInfoCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// ecache cache: cache file object information to reduce database queries
	// key: "<bktID>_<fileID>", value: *core.ObjectInfo
	fileObjCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// Batch write buffer object pool: reuse buffers to reduce memory allocation
	// Note: buffer is used long-term, only returned when BatchWriteManager is no longer used
	// Object pool uses default 4MB size, will reallocate if chunkSize differs
	batchBufferPool = sync.Pool{
		New: func() interface{} {
			// Create default 4MB buffer
			return make([]byte, 4<<20)
		},
	}
)

// BatchWriteManager manages batch writes, supporting packaging multiple small files' data blocks into a single data block
// Uses lock-free buffer management with atomic operations to acquire write positions
type BatchWriteManager struct {
	// Filesystem (all files share the same bucket)
	fs *OrcasFS

	// Shared buffer (lock-free)
	buffer      []byte       // Shared buffer (size is chunkSize)
	writeOffset atomic.Int64 // Current write position (atomic operation)
	bufferSize  int64        // Buffer size (equals chunkSize)

	// File info list (lock-free, managed with atomic operations)
	fileInfos     []*BatchFileInfo // File info list (fixed size, pre-allocated)
	fileInfoIndex atomic.Int64     // Current file info index (atomic operation)
	maxFileInfos  int64            // Maximum number of file infos

	// Flush control
	flushWindow    time.Duration
	maxPackageSize int64 // Maximum packaged data block size (default 4MB)
}

// BatchFileInfo file info for batch writes (lock-free)
type BatchFileInfo struct {
	FileID   int64
	Offset   int64  // Offset position in buffer
	Size     int64  // Processed data size (after compression/encryption)
	OrigSize int64  // Original data size
	Kind     uint32 // Compression/encryption flags
}

// PackagedDataBlock packaged data block information
type PackagedDataBlock struct {
	PkgID     int64               // Packaged data block ID
	Data      []byte              // Packaged data
	FileInfos []*PackagedFileInfo // File info list
}

// PackagedFileInfo packaged file information
type PackagedFileInfo struct {
	FileID    int64  // File ID
	DataID    int64  // Data ID
	PkgOffset uint32 // Offset position in packaged data block
	Size      int64  // Data size (after compression/encryption)
	OrigSize  int64  // Original data size
	Kind      uint32 // Data status (compression/encryption, etc.)
}

// getBatchWriteManager gets the batch write manager for the specified bucket (thread-safe)
func (fs *OrcasFS) getBatchWriteManager() *BatchWriteManager {
	fs.batchWriteMgrOnce.Do(func() {
		config := core.GetWriteBufferConfig()
		// Use chunkSize as buffer size
		chunkSize := fs.chunkSize
		if chunkSize <= 0 {
			chunkSize = 4 << 20 // Default 4MB
		}

		// Get buffer from object pool (will create new if pool is empty)
		buffer := batchBufferPool.Get().([]byte)
		// Ensure buffer size is correct (reallocate if chunkSize differs from pool)
		if int64(cap(buffer)) < chunkSize {
			buffer = make([]byte, chunkSize)
		} else {
			buffer = buffer[:chunkSize]
		}
		// Note: No need to zero buffer, as we always write from writeOffset
		// flush will atomically reset writeOffset, old data won't affect new data

		fs.batchWriteMgr = &BatchWriteManager{
			fs:             fs,
			buffer:         buffer,
			bufferSize:     chunkSize,
			fileInfos:      make([]*BatchFileInfo, 1<<10), // Pre-allocate 1024 file infos
			maxFileInfos:   1 << 10,
			flushWindow:    config.BufferWindow,
			maxPackageSize: chunkSize, // Use chunkSize as maximum package size
		}
		// Start periodic flush goroutine
		go fs.batchWriteMgr.flushLoop()
	})
	return fs.batchWriteMgr
}

// flushLoop periodic flush loop
func (bwm *BatchWriteManager) flushLoop() {
	ticker := time.NewTicker(bwm.flushWindow)
	defer ticker.Stop()

	for range ticker.C {
		// Periodic flush (every flushWindow seconds)
		bwm.flush()
	}
}

// flushAll flushes all pending write data
func (bwm *BatchWriteManager) flushAll() {
	bwm.flush()
}

// flush flushes all pending write data (lock-free)
// Atomically gets current write position and file info, then packages and writes
func (bwm *BatchWriteManager) flush() {
	startTime := time.Now()

	// Atomically swap write position to 0 (reset and get old value)
	oldOffset := bwm.writeOffset.Swap(0)

	// Atomically swap file index to 0 (reset and get old value)
	oldIndex := bwm.fileInfoIndex.Swap(0)

	if oldOffset == 0 || oldIndex == 0 {
		return // No pending write data
	}

	var copyTime, packageTime, flushPkgTime time.Duration

	// Note: No need to zero used buffer portion because:
	// 1. New data will write from writeOffset=0, overwriting old data
	// 2. We only read data from fileInfo.Offset to fileInfo.Offset+fileInfo.Size
	// 3. Old data won't affect correctness of new data

	// Copy current file infos (avoid modification during packaging)
	copyStart := time.Now()
	fileInfos := make([]*BatchFileInfo, oldIndex)
	for i := int64(0); i < oldIndex; i++ {
		if bwm.fileInfos[i] != nil {
			// Shallow copy file info (data itself is in buffer, no need for deep copy)
			fileInfos[i] = bwm.fileInfos[i]
		}
	}
	copyTime = time.Since(copyStart)

	if len(fileInfos) == 0 {
		return
	}

	// Package data blocks
	packageStart := time.Now()
	packages := bwm.packageDataBlocks(fileInfos)
	packageTime = time.Since(packageStart)

	// Batch write packaged data blocks, DataInfo and ObjectInfo
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

// packageDataBlocks packages data blocks (using processed data)
// Reads data from shared buffer and packages it
func (bwm *BatchWriteManager) packageDataBlocks(fileInfos []*BatchFileInfo) []*PackagedDataBlock {
	var packages []*PackagedDataBlock
	var currentPkg *PackagedDataBlock
	var currentPkgOffset uint32

	for _, fileInfo := range fileInfos {
		if fileInfo == nil || fileInfo.Size == 0 {
			continue
		}

		// Read data from shared buffer
		data := bwm.buffer[fileInfo.Offset : fileInfo.Offset+fileInfo.Size]

		// If current package is empty or data is too large, create new package
		if currentPkg == nil || int64(len(currentPkg.Data))+fileInfo.Size > bwm.maxPackageSize {
			// If current package is not empty, save it first
			if currentPkg != nil {
				packages = append(packages, currentPkg)
			}

			// Create new package
			currentPkg = &PackagedDataBlock{
				Data:      make([]byte, 0, bwm.maxPackageSize),
				FileInfos: make([]*PackagedFileInfo, 0),
			}
			currentPkgOffset = 0
		}

		// Add processed data to current package
		pkgFileInfo := &PackagedFileInfo{
			FileID:    fileInfo.FileID,
			PkgOffset: currentPkgOffset,
			Size:      fileInfo.Size,     // Size after compression/encryption
			OrigSize:  fileInfo.OrigSize, // Original data size
			Kind:      fileInfo.Kind,     // Compression and encryption flags
		}

		// Copy data to package
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		currentPkg.Data = append(currentPkg.Data, dataCopy...)
		currentPkg.FileInfos = append(currentPkg.FileInfos, pkgFileInfo)

		// Update offset
		currentPkgOffset += uint32(len(data))
	}

	// Save last package
	if currentPkg != nil {
		packages = append(packages, currentPkg)
	}

	return packages
}

// processFileData processes file data according to configuration (compression and encryption)
// Returns: processed data, Kind flags, original size
func (bwm *BatchWriteManager) processFileData(fs *OrcasFS, originalData []byte) ([]byte, uint32, int64) {
	origSize := int64(len(originalData))
	kind := uint32(0)
	cfg := fs.sdkCfg

	// Set compression and encryption flags
	if cfg != nil {
		if cfg.WiseCmpr > 0 {
			kind |= cfg.WiseCmpr
		}
		if cfg.EndecWay > 0 {
			kind |= cfg.EndecWay
		}
	}

	if kind == 0 {
		// No compression or encryption, return original data directly
		return originalData, kind, origSize
	}

	data := originalData
	compressedData := originalData // Save compressed data (if compression succeeds)
	var err error

	// 1. Compression (if enabled)
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
				// Compression succeeded and compressed size is smaller
				compressedData = cmprBuf.Bytes()
				data = compressedData
			} else {
				// Compression failed or compressed size is larger, remove compression flag
				kind &= ^core.DATA_CMPR_MASK
				compressedData = originalData
				data = originalData
			}
		}
	}

	// 2. Encryption (if enabled)
	hasEnc := kind&core.DATA_ENDEC_MASK != 0
	if hasEnc && cfg != nil {
		if kind&core.DATA_ENDEC_AES256 != 0 {
			data, err = aes256.Encrypt(cfg.EndecKey, data)
			if err != nil {
				// Encryption failed, remove encryption flag, use compressed data (if compressed) or original data
				kind &= ^core.DATA_ENDEC_MASK
				data = compressedData
			}
		} else if kind&core.DATA_ENDEC_SM4 != 0 {
			data, err = sm4.Sm4Cbc([]byte(cfg.EndecKey), data, true)
			if err != nil {
				// Encryption failed, remove encryption flag, use compressed data (if compressed) or original data
				kind &= ^core.DATA_ENDEC_MASK
				data = compressedData
			}
		}
	}

	return data, kind, origSize
}

// flushPackage flushes a single packaged data block (batch writes data block, DataInfo and ObjectInfo)
func (bwm *BatchWriteManager) flushPackage(pkg *PackagedDataBlock) error {
	if len(pkg.FileInfos) == 0 || len(pkg.Data) == 0 {
		return nil
	}

	startTime := time.Now()
	var newIDTime, putDataTime, putDataInfoTime, putObjTime time.Duration

	// Get fs and handler from BatchWriteManager
	if bwm.fs == nil {
		return fmt.Errorf("invalid batch write manager: fs is nil")
	}

	fs := bwm.fs
	lh := fs.h
	bktID := fs.bktID
	ctx := fs.c

	// 1. Generate packaged data block ID
	newIDStart := time.Now()
	pkgID := lh.NewID()
	newIDTime = time.Since(newIDStart)
	if pkgID <= 0 {
		return fmt.Errorf("failed to generate package ID")
	}
	pkg.PkgID = pkgID

	// 2. Write packaged data block
	putDataStart := time.Now()
	_, err := lh.PutData(ctx, bktID, pkgID, 0, pkg.Data)
	putDataTime = time.Since(putDataStart)
	if err != nil {
		return fmt.Errorf("failed to write package data: %v", err)
	}

	// 3. Create DataInfo for each file (using PkgID and PkgOffset)
	var dataInfos []*core.DataInfo
	var objectInfos []*core.ObjectInfo
	var newIDLoopTime, getObjTime time.Duration

	loopStart := time.Now()
	for _, fileInfo := range pkg.FileInfos {
		// Generate DataID
		newIDLoopStart := time.Now()
		dataID := lh.NewID()
		newIDLoopTime += time.Since(newIDLoopStart)
		if dataID <= 0 {
			continue
		}
		fileInfo.DataID = dataID

		// Create DataInfo
		dataInfo := &core.DataInfo{
			ID:        dataID,
			Size:      fileInfo.Size,
			OrigSize:  fileInfo.OrigSize,
			Kind:      fileInfo.Kind,
			PkgID:     pkgID,
			PkgOffset: fileInfo.PkgOffset,
		}
		dataInfos = append(dataInfos, dataInfo)

		// Get file object, update DataID and Size
		// Note: File size should use original size (OrigSize), not compressed/encrypted size
		// Optimization: Try to get from cache first to avoid database query
		getObjStart := time.Now()
		cacheKey := formatCacheKey(bktID, fileInfo.FileID)
		var fileObj *core.ObjectInfo
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
				fileObj = obj
			}
		}
		if fileObj == nil {
			// Cache miss, query from database
			fileObjs, err := lh.Get(ctx, bktID, []int64{fileInfo.FileID})
			getObjTime += time.Since(getObjStart)
			if err != nil || len(fileObjs) == 0 {
				fileObj = nil
			} else {
				fileObj = fileObjs[0]
				// Update cache
				fileObjCache.Put(cacheKey, fileObj)
			}
		} else {
			getObjTime += time.Since(getObjStart)
		}

		if fileObj == nil {
			// File object doesn't exist, create new one
			objectInfo := &core.ObjectInfo{
				ID:     fileInfo.FileID,
				DataID: dataID,
				Size:   fileInfo.OrigSize, // Use original size
				MTime:  core.Now(),
				Type:   core.OBJ_TYPE_FILE,
			}
			objectInfos = append(objectInfos, objectInfo)
		} else {
			// Update existing file object
			objectInfo := &core.ObjectInfo{
				ID:     fileObj.ID,
				PID:    fileObj.PID,
				DataID: dataID,
				Size:   fileInfo.OrigSize, // Use original size
				MTime:  core.Now(),
				Type:   fileObj.Type,
				Name:   fileObj.Name,
			}
			objectInfos = append(objectInfos, objectInfo)
		}
	}
	loopTime := time.Since(loopStart)

	// 4. Batch write DataInfo
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

	// 5. Batch write ObjectInfo
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

// addFile adds file data to batch write buffer in lock-free manner
// If space is insufficient, flushes existing data first, then continues writing
func (bwm *BatchWriteManager) addFile(ra *RandomAccessor, data []byte) (bool, error) {
	if len(data) == 0 {
		return true, nil
	}

	// Performance analysis: record start time
	startTime := time.Now()
	var processTime, flushTime, copyTime, retryTime time.Duration
	var retryCount int

	// Process compression and encryption according to configuration
	processStart := time.Now()
	processedData, kind, origSize := bwm.processFileData(ra.fs, data)
	processTime = time.Since(processStart)
	if len(processedData) == 0 {
		return false, fmt.Errorf("failed to process file data")
	}

	processedSize := int64(len(processedData))

	// Try to write, may need retry (if space is insufficient, flush first)
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		retryStart := time.Now()
		retryCount++
		// Check current space first (Load is atomic, but may be modified by other goroutines after check)
		currentOffset := bwm.writeOffset.Load()
		if currentOffset+processedSize > bwm.bufferSize {
			// Space insufficient, flush existing data first
			if retry == 0 {
				flushStart := time.Now()
				bwm.flush()
				flushTime += time.Since(flushStart)
			} else {
				// If still insufficient after flush, data is too large, return false for caller to fallback
				retryTime += time.Since(retryStart)
				return false, nil
			}
			// Continue retry after flush
			retryTime += time.Since(retryStart)
			continue
		}

		// Use Add to directly reserve space (atomic operation)
		newOffset := bwm.writeOffset.Add(processedSize)
		oldOffset := newOffset - processedSize

		// Check again if exceeds buffer size (may be modified by other goroutines during reservation)
		if newOffset > bwm.bufferSize {
			// Space insufficient after reservation, rollback reservation
			bwm.writeOffset.Add(-processedSize)
			if retry == 0 {
				flushStart := time.Now()
				bwm.flush()
				flushTime += time.Since(flushStart)
			} else {
				retryTime += time.Since(retryStart)
				return false, nil
			}
			// Continue retry after flush
			retryTime += time.Since(retryStart)
			continue
		}

		// Write data to buffer
		// Note: Space is already reserved via Add, so oldOffset to newOffset is safe
		copyStart := time.Now()
		copy(bwm.buffer[oldOffset:newOffset], processedData)
		copyTime += time.Since(copyStart)

		// Get file info index (atomic operation)
		fileIndex := bwm.fileInfoIndex.Add(1) - 1
		if fileIndex >= bwm.maxFileInfos {
			// File info list is full, rollback and flush
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
			// Continue retry after flush
			retryTime += time.Since(retryStart)
			continue
		}

		// Create file info (lock-free, as we've already acquired write position via Add)
		bwm.fileInfos[fileIndex] = &BatchFileInfo{
			FileID:   ra.fileID,
			Offset:   oldOffset,
			Size:     processedSize,
			OrigSize: origSize,
			Kind:     kind,
		}

		retryTime += time.Since(retryStart)
		totalTime := time.Since(startTime)

		// Performance analysis: only output in concurrent scenarios (judged by goroutine ID)
		if totalTime > 100*time.Microsecond || retryCount > 1 {
			fmt.Printf("[PERF] addFile: fileID=%d, size=%d, total=%v, process=%v, flush=%v, copy=%v, retry=%v, retries=%d\n",
				ra.fileID, len(data), totalTime, processTime, flushTime, copyTime, retryTime, retryCount)
		}

		return true, nil
	}

	// Too many retries, return error (avoid infinite loop)
	return false, fmt.Errorf("failed to add file after %d retries: buffer may be too small", maxRetries)
}

// formatCacheKey formats cache key (optimized: direct memory copy, highest performance)
func formatCacheKey(bktID, id int64) string {
	// Create fixed-size byte array on stack
	var buf [16]byte

	// Directly use unsafe to copy 8-byte int64 memory to byte array (highest performance)
	// Avoid function call overhead, direct memory operation
	*(*int64)(unsafe.Pointer(&buf[0])) = bktID
	*(*int64)(unsafe.Pointer(&buf[8])) = id
	return string(buf[:])
}

// WriteOperation represents a single write operation
type WriteOperation struct {
	Offset int64  // Write offset
	Data   []byte // Write data
}

// WriteBuffer manages write buffer for a single file
type WriteBuffer struct {
	fileID     int64            // File object ID
	operations []WriteOperation // Fixed-length write operation array (avoid temporary object creation)
	writeIndex int64            // Current write position (using atomic operation, starts from 0)
	totalSize  int64            // Total buffer size (optimized with atomic operation)
}

// SequentialWriteBuffer sequential write buffer (optimized: sequential writes starting from 0)
type SequentialWriteBuffer struct {
	fileID    int64          // File object ID
	dataID    int64          // Data object ID (created when creating new object)
	sn        int            // Current data block sequence number
	chunkSize int64          // Chunk size
	buffer    []byte         // Current chunk buffer (at most one chunk size)
	offset    int64          // Current write position (sequential write)
	hasData   bool           // Whether data has been written
	closed    bool           // Whether closed (becomes random write)
	dataInfo  *core.DataInfo // Data information
}

// RandomAccessor random access object in VFS, supports compression and encryption
type RandomAccessor struct {
	fs           *OrcasFS
	fileID       int64
	buffer       *WriteBuffer           // Random write buffer
	seqBuffer    *SequentialWriteBuffer // Sequential write buffer (optimized)
	fileObj      atomic.Value
	fileObjKey   string      // Pre-computed file_obj cache key (optimized: avoid repeated conversion)
	pendingFlush *time.Timer // Delayed flush timer
	pendingMu    sync.Mutex  // Protects pendingFlush
}

// NewRandomAccessor creates a random access object
func NewRandomAccessor(fs *OrcasFS, fileID int64) (*RandomAccessor, error) {
	// Get configuration to initialize fixed-length operations array (optimized: avoid temporary object creation)
	maxBufferWrites := int(core.GetWriteBufferConfig().MaxBufferWrites)
	if maxBufferWrites <= 0 {
		maxBufferWrites = 200 // Default value
	}

	ra := &RandomAccessor{
		fs:         fs,
		fileID:     fileID,
		fileObjKey: formatCacheKey(fs.bktID, fileID), // Pre-compute and cache key
		buffer: &WriteBuffer{
			fileID:     fileID,
			operations: make([]WriteOperation, maxBufferWrites), // Fixed-length array
			writeIndex: 0,                                       // Start from 0
		},
	}
	return ra, nil
}

// Write adds write operation to buffer
// Optimization: sequential write optimization - if sequential write starting from 0, directly write to data block, avoid caching
func (ra *RandomAccessor) Write(offset int64, data []byte) error {
	// Check if in sequential write mode
	if ra.seqBuffer != nil && !ra.seqBuffer.closed {
		// Check if still sequential write (continue from current position)
		if offset == ra.seqBuffer.offset {
			// Sequential write, use optimized path
			return ra.writeSequential(offset, data)
		} else if offset < ra.seqBuffer.offset {
			// Write backwards, switch to random write mode
			if err := ra.flushSequentialBuffer(); err != nil {
				return err
			}
			ra.seqBuffer.closed = true
		} else {
			// Skipped some positions, switch to random write mode
			if err := ra.flushSequentialBuffer(); err != nil {
				return err
			}
			ra.seqBuffer.closed = true
		}
	}

	// Initialize sequential write buffer (if sequential write starting from 0, and file has no data)
	if ra.seqBuffer == nil && offset == 0 && len(data) > 0 {
		fileObj, err := ra.getFileObj()
		if err == nil && (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) {
			// File has no data, can initialize sequential write buffer
			if initErr := ra.initSequentialBuffer(); initErr == nil {
				// Initialization succeeded, use sequential write
				return ra.writeSequential(offset, data)
			}
			// Initialization failed, fallback to random write
		}
	}

	// Random write mode: use original buffer logic
	// Optimization: reduce data copying, only copy when necessary
	// Check if exceeds capacity (optimized: check early to avoid out of bounds)
	config := core.GetWriteBufferConfig()
	if atomic.LoadInt64(&ra.buffer.writeIndex)+1 >= int64(len(ra.buffer.operations)) || atomic.LoadInt64(&ra.buffer.totalSize) >= config.MaxBufferSize {
		// Exceeds capacity, need to force flush
		// Rollback writeIndex first (because it already exceeded)
		atomic.AddInt64(&ra.buffer.writeIndex, -1)
		// Force flush current buffer (synchronous execution, ensure data is persisted)
		_, err := ra.Flush()
		if err != nil {
			return err
		}
	}

	// After flush, reacquire write position (writeIndex should have been reset to 0 by Flush)
	writeIndex := atomic.AddInt64(&ra.buffer.writeIndex, 1) - 1

	// Optimization: use atomic operation to update totalSize, reduce lock contention
	atomic.AddInt64(&ra.buffer.totalSize, int64(len(data)))

	// Write new data
	ra.buffer.operations[writeIndex].Offset = offset
	ra.buffer.operations[writeIndex].Data = make([]byte, len(data))
	copy(ra.buffer.operations[writeIndex].Data, data)

	// Optimization: for small file writes, use batch write manager
	// If data size is small and hasn't reached force flush condition, add to batch write manager
	// Note: batch write only applies to small files, and needs to ensure data integrity
	// Here use delayed flush first, batch write logic is handled in Flush
	if int64(len(data)) < config.MaxBufferSize/10 && atomic.LoadInt64(&ra.buffer.totalSize) < config.MaxBufferSize {
		ra.scheduleDelayedFlush(config.BufferWindow)
	}

	return nil
}

// scheduleDelayedFlush schedules delayed flush
func (ra *RandomAccessor) scheduleDelayedFlush(window time.Duration) {
	ra.pendingMu.Lock()
	defer ra.pendingMu.Unlock()

	// Cancel previous timer
	if ra.pendingFlush != nil {
		ra.pendingFlush.Stop()
	}

	// Create new delayed flush timer
	ra.pendingFlush = time.AfterFunc(window, func() {
		ra.pendingMu.Lock()
		defer ra.pendingMu.Unlock()
		ra.pendingFlush = nil
		// Async flush, non-blocking
		go func() {
			_, _ = ra.Flush()
		}()
	})
}

// initSequentialBuffer initializes sequential write buffer
func (ra *RandomAccessor) initSequentialBuffer() error {
	fileObj, err := ra.getFileObj()
	if err != nil {
		return err
	}

	// If file already has data, cannot use sequential write optimization
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		return fmt.Errorf("file already has data")
	}

	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 4 << 20 // Default 4MB
	}

	// Create new data object
	newDataID := ra.fs.h.NewID()

	// Initialize DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: 0,
		Size:     0,
		CRC32:    0,
		Cksum:    0,
		Kind:     0,
	}

	// Set compression and encryption flags (if enabled)
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
		buffer:    make([]byte, 0, chunkSize), // Pre-allocate but length is 0
		offset:    0,
		hasData:   false,
		closed:    false,
		dataInfo:  dataInfo,
	}

	return nil
}

// writeSequential writes data sequentially (optimized path)
func (ra *RandomAccessor) writeSequential(offset int64, data []byte) error {
	if ra.seqBuffer == nil || ra.seqBuffer.closed {
		return fmt.Errorf("sequential buffer not available")
	}

	// Ensure offset is sequential
	if offset != ra.seqBuffer.offset {
		return fmt.Errorf("non-sequential write detected")
	}

	dataLeft := data
	for len(dataLeft) > 0 {
		// Calculate remaining space in current chunk
		remainingInChunk := ra.seqBuffer.chunkSize - int64(len(ra.seqBuffer.buffer))
		if remainingInChunk <= 0 {
			// Current chunk is full, write and clear
			if err := ra.flushSequentialChunk(); err != nil {
				return err
			}
			remainingInChunk = ra.seqBuffer.chunkSize
		}

		// Calculate how much data can be written this time
		writeSize := int64(len(dataLeft))
		if writeSize > remainingInChunk {
			writeSize = remainingInChunk
		}

		// Write to current chunk buffer
		ra.seqBuffer.buffer = append(ra.seqBuffer.buffer, dataLeft[:writeSize]...)
		ra.seqBuffer.offset += writeSize
		ra.seqBuffer.hasData = true
		dataLeft = dataLeft[writeSize:]

		// If chunk is full, write immediately
		if int64(len(ra.seqBuffer.buffer)) >= ra.seqBuffer.chunkSize {
			if err := ra.flushSequentialChunk(); err != nil {
				return err
			}
		}
	}

	return nil
}

// flushSequentialChunk flushes current sequential write chunk (writes a complete chunk)
func (ra *RandomAccessor) flushSequentialChunk() error {
	if ra.seqBuffer == nil || len(ra.seqBuffer.buffer) == 0 {
		return nil
	}

	cfg := ra.fs.sdkCfg
	chunkData := ra.seqBuffer.buffer

	// Process first chunk: check file type and compression effect
	isFirstChunk := ra.seqBuffer.sn == 0
	if isFirstChunk && cfg != nil && cfg.WiseCmpr > 0 && len(chunkData) > 0 {
		kind, _ := filetype.Match(chunkData)
		if kind != filetype.Unknown {
			// Not unknown type, don't compress
			ra.seqBuffer.dataInfo.Kind &= ^core.DATA_CMPR_MASK
		}
	}

	// Update CRC32 and size of original data
	ra.seqBuffer.dataInfo.CRC32 = crc32.Update(ra.seqBuffer.dataInfo.CRC32, crc32.IEEETable, chunkData)
	ra.seqBuffer.dataInfo.OrigSize += int64(len(chunkData))

	// Compression (if enabled)
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
				// Compression failed, only remove compression flag on first chunk
				if isFirstChunk {
					ra.seqBuffer.dataInfo.Kind &= ^core.DATA_CMPR_MASK
				}
				processedChunk = chunkData
			} else {
				// If compressed size is larger or equal, only remove compression flag on first chunk
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

	// Encryption (if enabled)
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

	// Update CRC32 of final data
	ra.seqBuffer.dataInfo.Cksum = crc32.Update(ra.seqBuffer.dataInfo.Cksum, crc32.IEEETable, encodedChunk)

	// Update size (if compressed or encrypted)
	if ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0 || ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
		ra.seqBuffer.dataInfo.Size += int64(len(encodedChunk))
	}

	// Write data block
	if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, ra.seqBuffer.dataID, ra.seqBuffer.sn, encodedChunk); err != nil {
		return err
	}

	ra.seqBuffer.sn++
	ra.seqBuffer.buffer = ra.seqBuffer.buffer[:0] // Clear buffer but keep capacity
	return nil
}

// flushSequentialBuffer flushes entire sequential write buffer (writes last chunk and completes)
func (ra *RandomAccessor) flushSequentialBuffer() error {
	if ra.seqBuffer == nil {
		return nil
	}

	// Write last chunk (if there's still data)
	if len(ra.seqBuffer.buffer) > 0 {
		if err := ra.flushSequentialChunk(); err != nil {
			return err
		}
	}

	// If no data, return directly
	if !ra.seqBuffer.hasData {
		return nil
	}

	// Update final size of DataInfo
	if ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK == 0 && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		ra.seqBuffer.dataInfo.Size = ra.seqBuffer.dataInfo.OrigSize
		ra.seqBuffer.dataInfo.Cksum = ra.seqBuffer.dataInfo.CRC32
	}

	// Save DataInfo
	if _, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{ra.seqBuffer.dataInfo}); err != nil {
		return err
	}

	// Update cache
	dataInfoCache.Put(formatCacheKey(ra.fs.bktID, ra.seqBuffer.dataID), ra.seqBuffer.dataInfo)

	// Update file object
	fileObj, err := ra.getFileObj()
	if err != nil {
		return err
	}

	fileObj.DataID = ra.seqBuffer.dataID
	fileObj.Size = ra.seqBuffer.dataInfo.OrigSize

	// Update file object (using Handler's Put method)
	if _, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{fileObj}); err != nil {
		return err
	}

	// Update cache
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
	// Optimization: use pre-computed key (avoid repeated conversion)
	if cached, ok := fileObjCache.Get(ra.fileObjKey); ok {
		if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}
	// If cache miss, get from database
	objs, err := ra.fs.h.Get(ra.fs.c, ra.fs.bktID, []int64{ra.fileID})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("file %d not found", ra.fileID)
	}
	// Update cache (using pre-computed key)
	fileObjCache.Put(ra.fileObjKey, objs[0])
	ra.fileObj.Store(objs[0])
	return objs[0], nil
}

// Read reads data at specified position, merges writes in buffer
// Optimization: use atomic pointer to read fileObj, lock-free concurrent read
func (ra *RandomAccessor) Read(offset int64, size int) ([]byte, error) {
	// Optimization: use atomic operation to read fileObj, lock-free concurrent read
	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// If sequential write buffer has data, read from it first
	if ra.seqBuffer != nil && ra.seqBuffer.hasData {
		// Sequential write buffer has data, need to flush before reading
		// But shouldn't flush when reading, so only handle data already in file object
		// If sequential write buffer is not closed, data is still in buffer, need to flush first
		if !ra.seqBuffer.closed {
			// Sequential write not completed, flush to file object first
			if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
				return nil, flushErr
			}
			ra.seqBuffer.closed = true
			// Re-acquire file object
			var objErr error
			fileObj, objErr = ra.getFileObj()
			if objErr != nil {
				return nil, objErr
			}
		}
	}

	// If no data ID, directly handle buffer write operations
	if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
		return ra.readFromBuffer(offset, size), nil
	}

	// Get DataInfo
	dataInfoCacheKey := formatCacheKey(ra.fs.bktID, fileObj.DataID)
	var dataInfo *core.DataInfo
	if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
		if info, ok := cached.(*core.DataInfo); ok && info != nil {
			dataInfo = info
		}
	}

	// If cache miss, get from database
	if dataInfo == nil {
		var err error
		dataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
		if err != nil {
			// If getting DataInfo fails, try direct read (may be old data format)
			data, readErr := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, fileObj.DataID, 0)
			if readErr == nil && len(data) > 0 {
				return ra.readFromDataAndBuffer(data, offset, size), nil
			}
			return ra.readFromBuffer(offset, size), nil
		}
		// Update cache
		dataInfoCache.Put(dataInfoCacheKey, dataInfo)
	}

	// Check if data has compression or encryption
	hasCompression := dataInfo.Kind&core.DATA_CMPR_MASK != 0
	hasEncryption := dataInfo.Kind&core.DATA_ENDEC_MASK != 0

	// Create data reader (abstract read interface, unified handling of uncompressed and compressed/encrypted data)
	var reader dataReader
	if !hasCompression && !hasEncryption {
		// Uncompressed unencrypted: directly read by chunk
		reader = newPlainDataReader(ra.fs.c, ra.fs.h, ra.fs.bktID, fileObj.DataID, ra.fs.chunkSize)
	} else {
		// Compressed/encrypted: use decodingChunkReader (will automatically decrypt and decompress)
		var endecKey string
		if ra.fs.sdkCfg != nil {
			endecKey = ra.fs.sdkCfg.EndecKey
		}
		reader = newDecodingChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, dataInfo, endecKey)
	}

	// Unified read logic (includes merging write operations)
	fileData, operationsHandled := ra.readWithWrites(reader, offset, size)
	if operationsHandled {
		return fileData, nil
	}

	// If read fails or write operations not handled, read from buffer
	return ra.readFromBuffer(offset, size), nil
}

// readFromDataAndBuffer reads from data and buffer and merges
func (ra *RandomAccessor) readFromDataAndBuffer(data []byte, offset int64, size int) []byte {
	// Get buffer write operations
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}

	// Merge write operations
	mergedOps := mergeWriteOperations(operations)
	modifiedData := applyWritesToData(data, mergedOps)

	// Extract specified range
	return ra.extractRange(modifiedData, offset, size)
}

// readFromBuffer reads only from buffer (handles write operations)
func (ra *RandomAccessor) readFromBuffer(offset int64, size int) []byte {
	// Get buffer write operations
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}

	// Merge write operations
	mergedOps := mergeWriteOperations(operations)
	var modifiedData []byte
	if len(mergedOps) > 0 {
		// Calculate required data size
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

	// Extract specified range
	return ra.extractRange(modifiedData, offset, size)
}

// extractRange extracts specified range from data
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

	// If returning the entire slice, return directly, otherwise create new slice
	if offset == 0 && end == int64(len(data)) {
		return data
	}

	result := make([]byte, end-offset)
	copy(result, data[offset:end])
	return result
}

// Flush flushes buffer, returns new version ID (returns 0 if no pending data to flush)
func (ra *RandomAccessor) Flush() (int64, error) {
	// If sequential write buffer has data, flush it first
	if ra.seqBuffer != nil && ra.seqBuffer.hasData && !ra.seqBuffer.closed {
		if err := ra.flushSequentialBuffer(); err != nil {
			return 0, err
		}
		// After sequential write completes, close sequential buffer
		ra.seqBuffer.closed = true
		// After sequential write completes, return new version ID (actually the version corresponding to current DataID)
		fileObj, err := ra.getFileObj()
		if err != nil {
			return 0, err
		}
		if fileObj.DataID > 0 {
			return ra.fs.h.NewID(), nil // Return new version ID
		}
	}

	// Optimization: use atomic operation to get and clear operations (lock-free)
	// Atomically swap writeIndex and reset to 0, get actual operation count
	writeIndex := atomic.SwapInt64(&ra.buffer.writeIndex, 0)
	if writeIndex <= 0 {
		return 0, nil
	}

	// Check if it's a small file, suitable for batch write
	totalSize := atomic.LoadInt64(&ra.buffer.totalSize)
	fileObj, err := ra.getFileObj()
	if err == nil && totalSize > 0 && totalSize < 1<<20 { // 1MB threshold
		// Small file, merge all write operations then add to batch write manager
		operations := make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
		mergedOps := mergeWriteOperations(operations)

		// Calculate total size of merged data
		var mergedDataSize int64
		for _, op := range mergedOps {
			if op.Offset+int64(len(op.Data)) > mergedDataSize {
				mergedDataSize = op.Offset + int64(len(op.Data))
			}
		}

		// If data is continuous or can be merged into continuous data, use batch write
		if mergedDataSize < 1<<20 && len(mergedOps) <= 10 { // Small file and not many write operations
			// Merge all data into a continuous data block
			mergedData := make([]byte, mergedDataSize)
			for _, op := range mergedOps {
				copy(mergedData[op.Offset:], op.Data)
			}

			// Add to batch write manager (lock-free)
			batchMgr := ra.fs.getBatchWriteManager()
			added, err := batchMgr.addFile(ra, mergedData)
			if err != nil {
				// Processing failed, fallback to normal write
			} else if !added {
				// Buffer full, flush immediately
				batchMgr.flushAll()
				// Retry add
				added, err = batchMgr.addFile(ra, mergedData)
				if !added {
					// Still failed after retry, fallback to normal write
				}
			}

			if added {
				// Successfully added to batch write manager, reset buffer
				atomic.StoreInt64(&ra.buffer.totalSize, 0)
				return 0, nil
			}
			// If add failed, continue with normal write flow
		}
	}

	// Copy actually used portion (avoid modification during flush)
	operations := make([]WriteOperation, writeIndex)
	copy(operations, ra.buffer.operations[:writeIndex])

	// Optimization: use atomic operation to reset totalSize
	atomic.StoreInt64(&ra.buffer.totalSize, 0)

	// Merge overlapping write operations
	mergedOps := mergeWriteOperations(operations)

	// Get file object information (update cache)
	// Optimization: use more efficient key generation (function internally uses object pool)
	// Note: fileObj was already obtained above, re-acquire here to ensure it's latest
	fileObj, err = ra.getFileObj()
	if err != nil {
		return 0, err
	}

	// Use SDK's listener to handle compression and encryption
	return ra.applyRandomWritesWithSDK(fileObj, mergedOps)
}

// applyRandomWritesWithSDK uses SDK's listener to handle compression and encryption, applies random writes
// Optimized for streaming processing, avoid large objects occupying too much memory
func (ra *RandomAccessor) applyRandomWritesWithSDK(fileObj *core.ObjectInfo, writes []WriteOperation) (int64, error) {
	// Get LocalHandler to access ig, ma, da
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	// Create new data ID
	newDataID := ra.fs.h.NewID()

	// Calculate new file size
	newSize := fileObj.Size
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > newSize {
			newSize = writeEnd
		}
	}

	// Get chunk size
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 4 << 20 // Default 4MB
	}
	chunkSizeInt := int(chunkSize)

	// Check if original data is compressed or encrypted (optimized: use cache)
	var oldDataInfo *core.DataInfo
	var hasCompression, hasEncryption bool
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// Optimization: use more efficient key generation (function internally uses object pool)
		dataInfoCacheKey := formatCacheKey(ra.fs.bktID, oldDataID)

		if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
			if info, ok := cached.(*core.DataInfo); ok && info != nil {
				oldDataInfo = info
			}
		}

		// If cache miss, get from database
		if oldDataInfo == nil {
			var err error
			oldDataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, oldDataID)
			if err == nil && oldDataInfo != nil {
				// Update cache (reuse generated key)
				dataInfoCache.Put(dataInfoCacheKey, oldDataInfo)
			}
		}

		if oldDataInfo != nil {
			hasCompression = oldDataInfo.Kind&core.DATA_CMPR_MASK != 0
			hasEncryption = oldDataInfo.Kind&core.DATA_ENDEC_MASK != 0
		}
	}

	// Create DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: newSize,
		Kind:     core.DATA_NORMAL,
	}

	// If original data is compressed or encrypted, must read completely (unavoidable)
	// But can stream write, avoid processing all data at once
	if hasCompression || hasEncryption {
		newVersionID, err := ra.applyWritesStreamingCompressed(oldDataInfo, writes, dataInfo, chunkSize, newSize)
		if err != nil {
			return 0, err
		}

		// Optimization: use time calibrator to get timestamp, reduce time.Now() calls and GC pressure
		// Create new version object
		mTime := core.Now()
		newVersion := &core.ObjectInfo{
			ID:     newVersionID,
			PID:    ra.fileID,
			Type:   core.OBJ_TYPE_VERSION,
			DataID: newDataID,
			Size:   newSize,
			MTime:  mTime,
		}

		// Optimization: batch write metadata (write version object and file object update together)
		objectsToPut := []*core.ObjectInfo{newVersion}
		// Also update file object (if file object itself needs update)
		updateFileObj := &core.ObjectInfo{
			ID:     ra.fileID,
			DataID: newDataID,
			Size:   newSize,
		}
		objectsToPut = append(objectsToPut, updateFileObj)

		// Use Put method to batch create version and update file object (will automatically apply version retention policy)
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)

		// Update cached file object information
		if err == nil {
			// Optimization: use pre-computed key (avoid repeated conversion)
			fileObjCache.Put(ra.fileObjKey, updateFileObj)
			ra.fileObj.Store(updateFileObj)
		}

		return newVersionID, err
	}

	// For uncompressed unencrypted data, can stream read and process by chunk
	newVersionID, err := ra.applyWritesStreamingUncompressed(fileObj, oldDataInfo, writes, dataInfo, chunkSizeInt, newSize)
	if err != nil {
		return 0, err
	}

	// Optimization: use time calibrator to get timestamp, reduce time.Now() calls and GC pressure
	// Create new version object
	mTime := core.Now()
	newVersion := &core.ObjectInfo{
		ID:     newVersionID,
		PID:    ra.fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// Optimization: batch write metadata (write version object and file object update together)
	objectsToPut := []*core.ObjectInfo{newVersion}
	// Also update file object (if file object itself needs update)
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		DataID: newDataID,
		Size:   newSize,
	}
	objectsToPut = append(objectsToPut, updateFileObj)

	// Use Put method to batch create version and update file object (will automatically apply version retention policy)
	_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)

	// Update cached file object information
	if err == nil {
		// Optimization: use pre-computed key (avoid repeated conversion)
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
	}

	return newVersionID, err
}

// applyWritesStreamingCompressed handles compressed or encrypted data
// Streaming processing: read original data by chunk, apply write operations, process and write to new object immediately
func (ra *RandomAccessor) applyWritesStreamingCompressed(oldDataInfo *core.DataInfo, writes []WriteOperation,
	dataInfo *core.DataInfo, chunkSize int64, newSize int64,
) (int64, error) {
	// Now each chunk is independently compressed and encrypted, can process by chunk in streaming
	// Directly read by chunk, decrypt, decompress, don't use DataReader

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
