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
	"github.com/orcastor/orcas/sdk"
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
)

// getBatchWriteManager gets the batch writer for the specified bucket (thread-safe)
// Returns nil if batch write is disabled
// Uses SDK's global batch writer registry
func (fs *OrcasFS) getBatchWriteManager() *sdk.BatchWriter {
	return sdk.GetBatchWriterForBucket(fs.h, fs.bktID)
}

// processFileDataForBatchWrite processes file data (compression/encryption) for batch write
// This is a helper function to process data before passing to SDK's BatchWriter
func processFileDataForBatchWrite(fs *OrcasFS, originalData []byte) ([]byte, int64) {
	origSize := int64(len(originalData))
	if origSize == 0 {
		return originalData, origSize
	}

	// Process compression and encryption according to configuration
	// Get bucket configuration
	bucket := fs.getBucketConfig()
	kind := uint32(0)
	data := originalData
	compressedData := originalData
	var err error

	// 1. Compression (if enabled)
	if bucket != nil && bucket.CmprWay > 0 {
		kind |= bucket.CmprWay
		var cmpr archiver.Compressor
		if bucket.CmprWay&core.DATA_CMPR_SNAPPY != 0 {
			cmpr = &archiver.Snappy{}
		} else if bucket.CmprWay&core.DATA_CMPR_ZSTD != 0 {
			cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(bucket.CmprQlty)))}}
		} else if bucket.CmprWay&core.DATA_CMPR_GZIP != 0 {
			cmpr = &archiver.Gz{CompressionLevel: int(bucket.CmprQlty)}
		} else if bucket.CmprWay&core.DATA_CMPR_BR != 0 {
			cmpr = &archiver.Brotli{Quality: int(bucket.CmprQlty)}
		}

		if cmpr != nil {
			var cmprBuf bytes.Buffer
			err := cmpr.Compress(bytes.NewBuffer(data), &cmprBuf)
			if err == nil && cmprBuf.Len() < len(data) {
				compressedData = cmprBuf.Bytes()
				data = compressedData
			} else {
				kind &= ^core.DATA_CMPR_MASK
				compressedData = originalData
				data = originalData
			}
		}
	}

	// 2. Encryption (if enabled)
	if bucket != nil && bucket.EndecWay > 0 {
		kind |= bucket.EndecWay
		if kind&core.DATA_ENDEC_AES256 != 0 {
			data, err = aes256.Encrypt(bucket.EndecKey, data)
			if err != nil {
				kind &= ^core.DATA_ENDEC_MASK
				data = compressedData
			}
		} else if kind&core.DATA_ENDEC_SM4 != 0 {
			data, err = sm4.Sm4Cbc([]byte(bucket.EndecKey), data, true)
			if err != nil {
				kind &= ^core.DATA_ENDEC_MASK
				data = compressedData
			}
		}
	}

	return data, origSize
}

// addFileToBatchWrite adds file data to SDK's batch write manager
// This replaces the old vfs BatchWriter.addFile method
func addFileToBatchWrite(ra *RandomAccessor, data []byte) (bool, int64, error) {
	if len(data) == 0 {
		return true, 0, nil
	}

	// Try instant upload (deduplication) before processing
	// Check configuration: bucket config > environment variable
	origSize := int64(len(data))

	// Create unified config from bucket config
	var instantUploadCfg *core.InstantUploadConfig
	bucket := ra.fs.getBucketConfig()
	instantUploadCfg = core.GetBucketInstantUploadConfig(bucket)

	if core.IsInstantUploadEnabledWithConfig(instantUploadCfg) {
		// Try instant upload (deduplication)
		instantDataID, err := tryInstantUpload(ra.fs, data, origSize, core.DATA_NORMAL)
		if err == nil && instantDataID > 0 {
			// Instant upload succeeded, use existing DataID
			// Update file object with instant upload DataID
			fileObj, err := ra.getFileObj()
			if err == nil && fileObj != nil {
				updateFileObj := &core.ObjectInfo{
					ID:     fileObj.ID,
					PID:    fileObj.PID,
					DataID: instantDataID,
					Size:   origSize,
					MTime:  core.Now(),
					Type:   fileObj.Type,
					Name:   fileObj.Name,
				}
				// Update file object in database
				_, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
				if err == nil {
					// Update cache
					fileObjCache.Put(ra.fileObjKey, updateFileObj)
					ra.fileObj.Store(updateFileObj)
					// Return success with instant upload DataID
					return true, instantDataID, nil
				}
			}
		}
		// If update failed, continue with normal write
	}

	// Process compression and encryption
	processedData, origSize := processFileDataForBatchWrite(ra.fs, data)
	if len(processedData) == 0 {
		return false, 0, fmt.Errorf("failed to process file data")
	}

	// Get file object to get PID and Name
	fileObj, err := ra.getFileObj()
	if err != nil {
		return false, 0, fmt.Errorf("failed to get file object: %v", err)
	}

	var pid int64 = 0
	var name string = ""
	if fileObj != nil {
		pid = fileObj.PID
		name = fileObj.Name
	}

	// Get batch write manager
	batchMgr := ra.fs.getBatchWriteManager()
	if batchMgr == nil {
		return false, 0, nil
	}

	// Add file to SDK's batch write manager
	added, dataID, err := batchMgr.AddFile(ra.fileID, processedData, pid, name, origSize)
	if err != nil {
		return false, 0, err
	}
	if !added {
		return false, 0, nil
	}

	// Immediately update fileObj with DataID so reads can work correctly
	// This is critical: fileObj must have DataID before flush completes
	if fileObj != nil {
		// Update fileObj with new DataID and size
		updateFileObj := &core.ObjectInfo{
			ID:     fileObj.ID,
			PID:    fileObj.PID,
			DataID: dataID,
			Size:   origSize, // Use original size
			MTime:  core.Now(),
			Type:   fileObj.Type,
			Name:   fileObj.Name,
		}
		// Update file object in database using Put (which handles updates for existing objects)
		_, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			// Update failed, but data is already in buffer, continue
		} else {
			// Update cache
			fileObjCache.Put(ra.fileObjKey, updateFileObj)
			ra.fileObj.Store(updateFileObj)
		}
	}

	return true, dataID, nil
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
	fileObjKey   string       // Pre-computed file_obj cache key (optimized: avoid repeated conversion)
	pendingFlush atomic.Value // Delayed flush timer (*time.Timer)
	sparseSize   atomic.Int64 // Sparse file size (for pre-allocated files, e.g., qBittorrent)
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

// MarkSparseFile marks file as sparse (pre-allocated) for optimization
// This is used when SetAllocationSize is called to pre-allocate space
func (ra *RandomAccessor) MarkSparseFile(size int64) {
	ra.sparseSize.Store(size)
}

// Write adds write operation to buffer
// Optimization: sequential write optimization - if sequential write starting from 0, directly write to data block, avoid caching
// Optimization: for sparse files (pre-allocated), use more aggressive delayed flush to reduce frequent flushes
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

	// For sparse files (pre-allocated), use larger buffer threshold to reduce flush frequency
	// This is critical for qBittorrent random write performance
	sparseSize := ra.sparseSize.Load()
	isSparseFile := sparseSize > 0
	maxBufferSize := config.MaxBufferSize
	if isSparseFile {
		// For sparse files, allow larger buffer (2x) to reduce flush frequency
		// This significantly improves performance for random writes
		maxBufferSize = config.MaxBufferSize * 2
	}

	if atomic.LoadInt64(&ra.buffer.writeIndex)+1 >= int64(len(ra.buffer.operations)) || atomic.LoadInt64(&ra.buffer.totalSize) >= maxBufferSize {
		// Exceeds capacity, need to force flush
		// Don't rollback writeIndex (already incremented, space is allocated)
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

	// Optimization: for sparse files, use longer delayed flush window to batch more writes
	// This reduces flush frequency and improves performance for random writes
	flushWindow := config.BufferWindow
	if isSparseFile {
		// For sparse files, use 2x flush window to batch more random writes
		flushWindow = config.BufferWindow * 2
	}

	// Optimization: for small file writes, use batch write manager
	// If data size is small and hasn't reached force flush condition, add to batch write manager
	// Note: batch write only applies to small files, and needs to ensure data integrity
	// Here use delayed flush first, batch write logic is handled in Flush
	if int64(len(data)) < maxBufferSize/10 && atomic.LoadInt64(&ra.buffer.totalSize) < maxBufferSize {
		ra.scheduleDelayedFlush(flushWindow)
	}

	return nil
}

// scheduleDelayedFlush schedules delayed flush
func (ra *RandomAccessor) scheduleDelayedFlush(window time.Duration) {
	// Cancel previous timer (atomic load and swap)
	// Use typed nil pointer (*time.Timer)(nil) instead of nil for atomic.Value
	oldTimer := ra.pendingFlush.Swap((*time.Timer)(nil))
	if oldTimer != nil {
		if timer, ok := oldTimer.(*time.Timer); ok && timer != nil {
			timer.Stop()
		}
	}

	// Create new delayed flush timer
	newTimer := time.AfterFunc(window, func() {
		// Clear timer atomically
		// Use typed nil pointer (*time.Timer)(nil) instead of nil for atomic.Value
		ra.pendingFlush.Store((*time.Timer)(nil))
		// Async flush, non-blocking
		go func() {
			_, _ = ra.Flush()
		}()
	})

	// Store new timer atomically
	ra.pendingFlush.Store(newTimer)
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
	newDataID := core.NewID()

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
	bucket := ra.fs.getBucketConfig()
	if bucket != nil {
		if bucket.CmprWay > 0 {
			dataInfo.Kind |= bucket.CmprWay
		}
		if bucket.EndecWay > 0 {
			dataInfo.Kind |= bucket.EndecWay
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

	bucket := ra.fs.getBucketConfig()
	chunkData := ra.seqBuffer.buffer

	// Process first chunk: check file type and compression effect
	isFirstChunk := ra.seqBuffer.sn == 0
	if isFirstChunk && bucket != nil && bucket.CmprWay > 0 && len(chunkData) > 0 {
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
	if hasCmpr && bucket != nil && bucket.CmprWay > 0 {
		var cmpr archiver.Compressor
		if bucket.CmprWay&core.DATA_CMPR_SNAPPY != 0 {
			cmpr = &archiver.Snappy{}
		} else if bucket.CmprWay&core.DATA_CMPR_ZSTD != 0 {
			cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(bucket.CmprQlty)))}}
		} else if bucket.CmprWay&core.DATA_CMPR_GZIP != 0 {
			cmpr = &archiver.Gz{CompressionLevel: int(bucket.CmprQlty)}
		} else if bucket.CmprWay&core.DATA_CMPR_BR != 0 {
			cmpr = &archiver.Brotli{Quality: int(bucket.CmprQlty)}
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
	if bucket != nil && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
		encodedChunk, err = aes256.Encrypt(bucket.EndecKey, processedChunk)
	} else if bucket != nil && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
		encodedChunk, err = sm4.Sm4Cbc([]byte(bucket.EndecKey), processedChunk, true)
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

	// If no data, return directly
	if !ra.seqBuffer.hasData {
		return nil
	}

	// Check if it's a small file (total size < 1MB) and no chunks have been written yet
	// If sn > 0, it means at least one chunk (typically 4MB) has been written, so it's not a small file
	totalSize := ra.seqBuffer.dataInfo.OrigSize
	if totalSize > 0 && totalSize < 1<<20 && ra.seqBuffer.sn == 0 {
		// Small file and all data is still in buffer (no chunks written yet)
		// Try instant upload first (before batch write) if enabled
		allData := ra.seqBuffer.buffer
		if len(allData) > 0 {
			// Create unified config from bucket config
			var instantUploadCfg *core.InstantUploadConfig
			bucket := ra.fs.getBucketConfig()
			instantUploadCfg = core.GetBucketInstantUploadConfig(bucket)

			if core.IsInstantUploadEnabledWithConfig(instantUploadCfg) {
				// Try instant upload (deduplication)
				instantDataID, err := tryInstantUpload(ra.fs, allData, totalSize, core.DATA_NORMAL)
				if err == nil && instantDataID > 0 {
					// Instant upload succeeded, use existing DataID
					// Update file object with instant upload DataID
					fileObj, err := ra.getFileObj()
					if err == nil && fileObj != nil {
						updateFileObj := &core.ObjectInfo{
							ID:     fileObj.ID,
							PID:    fileObj.PID,
							DataID: instantDataID,
							Size:   totalSize,
							MTime:  core.Now(),
							Type:   fileObj.Type,
							Name:   fileObj.Name,
						}
						// Update file object in database
						_, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
						if err == nil {
							// Update cache
							fileObjCache.Put(ra.fileObjKey, updateFileObj)
							ra.fileObj.Store(updateFileObj)
							// Clear sequential buffer
							ra.seqBuffer.hasData = false
							ra.seqBuffer.buffer = ra.seqBuffer.buffer[:0]
							return nil
						}
					}
					// If update failed, continue with batch write or normal write
				}
			}

			// If instant upload failed, try batch write
			batchMgr := ra.fs.getBatchWriteManager()
			if batchMgr != nil {
				added, _, err := addFileToBatchWrite(ra, allData)
				if err == nil && added {
					// Successfully added to batch write manager
					// Clear sequential buffer
					ra.seqBuffer.hasData = false
					ra.seqBuffer.buffer = ra.seqBuffer.buffer[:0]
					return nil
				}
				// If batch write failed, fallback to normal write
				// Retry once after flush
				if !added {
					batchMgr.FlushAll(ra.fs.c)
					added, _, err = addFileToBatchWrite(ra, allData)
					if err == nil && added {
						ra.seqBuffer.hasData = false
						ra.seqBuffer.buffer = ra.seqBuffer.buffer[:0]
						return nil
					}
				}
			}
		}
	}

	// Write last chunk (if there's still data)
	if len(ra.seqBuffer.buffer) > 0 {
		if err := ra.flushSequentialChunk(); err != nil {
			return err
		}
	}

	// Update final size of DataInfo
	if ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK == 0 && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		ra.seqBuffer.dataInfo.Size = ra.seqBuffer.dataInfo.OrigSize
		ra.seqBuffer.dataInfo.Cksum = ra.seqBuffer.dataInfo.CRC32
	}

	// Get file object first to prepare for combined write
	fileObj, err := ra.getFileObj()
	if err != nil {
		return err
	}

	fileObj.DataID = ra.seqBuffer.dataID
	fileObj.Size = ra.seqBuffer.dataInfo.OrigSize

	// Optimization: Use PutDataInfoAndObj to write DataInfo and ObjectInfo together in a single transaction
	// This reduces database round trips and improves performance
	err = ra.fs.h.PutDataInfoAndObj(ra.fs.c, ra.fs.bktID, []*core.DataInfo{ra.seqBuffer.dataInfo}, []*core.ObjectInfo{fileObj})
	if err != nil {
		return err
	}

	// Update caches
	dataInfoCache.Put(formatCacheKey(ra.fs.bktID, ra.seqBuffer.dataID), ra.seqBuffer.dataInfo)
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

	// Limit reading size to file size
	if offset >= fileObj.Size {
		return []byte{}, nil
	}
	if int64(size) > fileObj.Size-offset {
		size = int(fileObj.Size - offset)
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
		} else {
			// Sequential write buffer is closed, data has been flushed
			// Re-acquire file object (may have been updated by flush)
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
		bucket := ra.fs.getBucketConfig()
		if bucket != nil {
			endecKey = bucket.EndecKey
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
			return core.NewID(), nil // Return new version ID
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
	if totalSize > 0 && totalSize < 1<<20 { // 1MB threshold
		// Small file, merge all write operations then add to batch write manager
		// Get current fileObj BEFORE swapping writeIndex (to read existing data if needed)
		fileObj, err := ra.getFileObj()
		if err != nil {
			return 0, err
		}

		// If file has existing data, don't use BatchWriter (fallback to normal write)
		// BatchWriter is optimized for new files or complete overwrites
		// For incremental updates, normal write path handles merging better
		if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID && fileObj.Size > 0 {
			// File has existing data, skip BatchWriter and use normal write path
			// This ensures proper merging of existing data with new writes
			// Continue to normal write path below
		} else {
			// No existing data, can use BatchWriter
			// Now copy operations (before swapping)
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
				if batchMgr != nil {
					added, _, err := addFileToBatchWrite(ra, mergedData)
					if err != nil {
						// Processing failed, fallback to normal write
					} else if !added {
						// Buffer full, flush immediately
						batchMgr.FlushAll(ra.fs.c)
						// Retry add
						added, _, err = addFileToBatchWrite(ra, mergedData)
						if !added {
							// Still failed after retry, fallback to normal write
						}
					}

					if added {
						// Successfully added to batch write manager
						// Immediately flush to ensure data is written
						batchMgr.FlushAll(ra.fs.c)
						// Reset buffer
						atomic.StoreInt64(&ra.buffer.totalSize, 0)
						// Return new version ID
						return core.NewID(), nil
					}
				}
				// If batch write is disabled or add failed, continue with normal write flow
			}
		} // End of else block for BatchWriter
	}

	// Copy actually used portion (avoid modification during flush)
	operations := make([]WriteOperation, writeIndex)
	copy(operations, ra.buffer.operations[:writeIndex])

	// Optimization: use atomic operation to reset totalSize
	atomic.StoreInt64(&ra.buffer.totalSize, 0)

	// Merge overlapping write operations
	mergedOps := mergeWriteOperations(operations)

	// Get file object information
	// Note: For BatchWriter path, fileObj is already updated in addFile
	// For normal write path, we use getFileObj and will create new DataID to overwrite
	fileObj, err := ra.getFileObj()
	if err != nil {
		return 0, err
	}

	// Optimization for sparse files (qBittorrent scenario): use writing version to directly modify data blocks
	// This avoids creating new versions and significantly improves performance for random writes
	// Note: Sparse files can support compression and encryption, but require special handling
	sparseSize := ra.sparseSize.Load()
	isSparseFile := sparseSize > 0
	if isSparseFile && fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		// Get DataInfo to check compression/encryption status
		var oldDataInfo *core.DataInfo
		oldDataID := fileObj.DataID
		dataInfoCacheKey := formatCacheKey(ra.fs.bktID, oldDataID)
		if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
			if info, ok := cached.(*core.DataInfo); ok && info != nil {
				oldDataInfo = info
			}
		}
		if oldDataInfo == nil {
			var err error
			oldDataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, oldDataID)
			if err == nil && oldDataInfo != nil {
				dataInfoCache.Put(dataInfoCacheKey, oldDataInfo)
			}
		}

		// For sparse files, always use writing version path (supports compression/encryption)
		// This provides better performance even with compression/encryption enabled
		return ra.applyWritesWithWritingVersion(fileObj, mergedOps, oldDataInfo)
	}

	// Use SDK's listener to handle compression and encryption
	// This will create new DataID and overwrite existing data
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
	newDataID := core.NewID()

	// Calculate new file size
	// For writes starting at offset 0, if the write data is shorter than the original file,
	// the file should be truncated to the write data length (overwrite from beginning)
	// Otherwise, the file size should be the maximum of original size and write end position
	newSize := fileObj.Size
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if write.Offset == 0 && writeEnd < newSize {
			// Write starts at 0 and is shorter than original file, truncate to write length
			newSize = writeEnd
		} else if writeEnd > newSize {
			// Write extends beyond current file size, extend file
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

// applyWritesWithWritingVersion applies writes using writing version (name="0") to directly modify data blocks
// This is optimized for sparse files (qBittorrent scenario) to avoid creating new versions
// Writing versions always use uncompressed/unencrypted data for better performance
func (ra *RandomAccessor) applyWritesWithWritingVersion(fileObj *core.ObjectInfo, writes []WriteOperation, oldDataInfo *core.DataInfo) (int64, error) {
	// Get LocalHandler
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	// Get or create writing version
	writingVersion, err := lh.GetOrCreateWritingVersion(ra.fs.c, ra.fs.bktID, ra.fileID)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create writing version: %v", err)
	}

	// Use writing version's DataID (writing version always has its own DataID without compression/encryption)
	dataID := writingVersion.DataID
	if dataID == 0 || dataID == core.EmptyDataID {
		return 0, fmt.Errorf("writing version has invalid DataID")
	}

	// Get chunk size
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 4 << 20 // Default 4MB
	}

	// Writing versions always use uncompressed/unencrypted data
	// No need to handle compression/encryption
	chunkSizeInt := int64(chunkSize)

	// Group writes by chunk to batch updates
	type chunkUpdate struct {
		sn         int
		offset     int
		data       []byte
		needsWrite bool
	}
	chunkUpdates := make(map[int]*chunkUpdate)

	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		startChunk := write.Offset / chunkSizeInt
		endChunk := (writeEnd - 1) / chunkSizeInt

		// Process each chunk that this write affects
		for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
			chunkStart := chunkIdx * chunkSizeInt
			chunkEnd := chunkStart + chunkSizeInt

			// Calculate overlap between write and chunk
			overlapStart := write.Offset
			if overlapStart < chunkStart {
				overlapStart = chunkStart
			}
			overlapEnd := writeEnd
			if overlapEnd > chunkEnd {
				overlapEnd = chunkEnd
			}

			if overlapStart >= overlapEnd {
				continue
			}

			// Extract data for this chunk
			writeDataStart := overlapStart - write.Offset
			writeDataEnd := writeDataStart + (overlapEnd - overlapStart)
			chunkData := write.Data[writeDataStart:writeDataEnd]

			// Calculate offset within chunk
			chunkOffset := int(overlapStart - chunkStart)
			sn := int(chunkIdx)

			// Check if we already have an update for this chunk
			key := sn
			if existing, ok := chunkUpdates[key]; ok {
				// Merge with existing update (overwrite overlapping region)
				existingEnd := existing.offset + len(existing.data)
				newEnd := chunkOffset + len(chunkData)

				// Calculate merged range
				mergedStart := existing.offset
				if chunkOffset < mergedStart {
					mergedStart = chunkOffset
				}
				mergedEnd := existingEnd
				if newEnd > mergedEnd {
					mergedEnd = newEnd
				}

				// Create merged data
				mergedData := make([]byte, mergedEnd-mergedStart)
				// Copy existing data
				if existing.offset >= mergedStart {
					copy(mergedData[existing.offset-mergedStart:], existing.data)
				}
				// Overwrite with new data
				if chunkOffset >= mergedStart {
					copy(mergedData[chunkOffset-mergedStart:], chunkData)
				}

				existing.offset = mergedStart
				existing.data = mergedData
				existing.needsWrite = true
			} else {
				// New chunk update
				chunkUpdates[key] = &chunkUpdate{
					sn:         sn,
					offset:     chunkOffset,
					data:       chunkData,
					needsWrite: true,
				}
			}
		}
	}

	// Apply all chunk updates
	// Optimization: check if data already exists to avoid duplicate writes
	// Writing versions always use uncompressed/unencrypted data, so we can use UpdateData directly
	for _, update := range chunkUpdates {
		if !update.needsWrite {
			continue
		}

		// Try to read existing data to check if update is needed (deduplication)
		existingData, err := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, dataID, update.sn, update.offset, len(update.data))
		if err == nil && len(existingData) == len(update.data) {
			// Check if data is identical (simple byte comparison)
			identical := true
			for i := 0; i < len(update.data); i++ {
				if existingData[i] != update.data[i] {
					identical = false
					break
				}
			}
			if identical {
				// Data is identical, skip write
				continue
			}
		}

		// Use UpdateData to directly modify data block (uncompressed/unencrypted)
		// This avoids creating new versions and significantly improves performance
		updateErr := lh.UpdateData(ra.fs.c, ra.fs.bktID, dataID, update.sn, update.offset, update.data)
		if updateErr != nil {
			return 0, fmt.Errorf("failed to update data chunk %d: %v", update.sn, updateErr)
		}
	}

	// Update file object size if needed
	newSize := fileObj.Size
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > newSize {
			newSize = writeEnd
		}
	}

	// Update writing version and file object if size changed
	if newSize != fileObj.Size {
		updateFileObj := &core.ObjectInfo{
			ID:     ra.fileID,
			DataID: dataID,
			Size:   newSize,
			MTime:  core.Now(),
		}
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			return 0, fmt.Errorf("failed to update file object: %v", err)
		}

		// Update cache
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
	}

	// Return 0 to indicate no new version was created (using writing version)
	return 0, nil
}

// applyWritesStreamingCompressed handles compressed or encrypted data
// Streaming processing: read original data by chunk, apply write operations, process and write to new object immediately
func (ra *RandomAccessor) applyWritesStreamingCompressed(oldDataInfo *core.DataInfo, writes []WriteOperation,
	dataInfo *core.DataInfo, chunkSize int64, newSize int64,
) (int64, error) {
	// Now each chunk is independently compressed and encrypted, can process by chunk in streaming
	// Directly read by chunk, decrypt, decompress, don't use DataReader

	var endecKey string
	bucket := ra.fs.getBucketConfig()
	if bucket != nil {
		endecKey = bucket.EndecKey
	}

	// Create a reader to read, decrypt, and decompress by chunk
	reader := newDecodingChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, oldDataInfo, endecKey)

	chunkSizeInt := int(chunkSize)
	if chunkSizeInt <= 0 {
		chunkSizeInt = 4 << 20 // Default 4MB
	}

	// Pre-calculate write operation indices for each chunk
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

	// Stream processing: read, process, and write by chunk
	return ra.processWritesStreaming(reader, writesByChunk, writes, dataInfo, chunkSizeInt, newSize, chunkCount)
}

// applyWritesStreamingUncompressed handles uncompressed and unencrypted data
// Can stream read and process by chunk, only reading affected data ranges
func (ra *RandomAccessor) applyWritesStreamingUncompressed(fileObj *core.ObjectInfo, oldDataInfo *core.DataInfo,
	writes []WriteOperation, dataInfo *core.DataInfo, chunkSizeInt int, newSize int64,
) (int64, error) {
	// For uncompressed and unencrypted data, can stream process by chunk
	// Optimization: pre-calculate write operations for each chunk to reduce repeated checks in loops

	// If no bucket configuration, directly stream write
	bucket := ra.fs.getBucketConfig()
	if bucket == nil {
		dataInfo.Size = newSize
		dataInfo.OrigSize = newSize

		oldDataID := fileObj.DataID
		sn := 0

		// Pre-calculate write operation indices for each chunk (optimization: reduce repeated checks)
		// Optimization: estimate capacity to reduce slice expansion
		chunkCount := int((newSize + int64(chunkSizeInt) - 1) / int64(chunkSizeInt))
		writesByChunk := make([][]int, chunkCount)
		// Estimate average number of write operations per chunk (optimization: reduce slice expansion)
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

		// Process by chunk
		for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
			pos := int64(chunkIdx * chunkSizeInt)
			chunkEnd := pos + int64(chunkSizeInt)
			if chunkEnd > newSize {
				chunkEnd = newSize
			}
			chunkSize := int(chunkEnd - pos)

			// Read original chunk data (optimization: use object pool to reuse buffers)
			chunkData := chunkDataPool.Get().([]byte)
			// Ensure capacity is sufficient
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
				// Read this chunk of original data
				data, err := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, oldDataID, 0, int(pos), int(readEnd-pos))
				if err == nil && len(data) > 0 {
					copy(chunkData, data)
				}
			}

			// Apply write operations (only process writes related to current chunk, using pre-calculated indices)
			for _, writeIdx := range writesByChunk[chunkIdx] {
				write := writes[writeIdx]
				writeEnd := write.Offset + int64(len(write.Data))

				// Calculate overlap range (optimization: reduce repeated calculations)
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
					// Optimization: directly use slice to avoid repeated length calculations
					copy(chunkData[overlapStart:overlapEnd], write.Data[writeDataStart:writeDataStart+copyLen])
				}
			}

			// Write chunk
			// Note: need to copy data because PutData may process asynchronously, and chunkData needs to be returned to object pool
			chunkDataCopy := make([]byte, len(chunkData))
			copy(chunkDataCopy, chunkData)
			// Return to object pool before writing (reset length but keep capacity)
			chunkDataPool.Put(chunkData[:0])

			if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, chunkDataCopy); err != nil {
				return 0, err
			}
			sn++
		}

		// Save data metadata
		_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
		if err != nil {
			return 0, err
		}

		// Optimization: use more efficient key generation (function internally uses object pool)
		dataInfoCache.Put(formatCacheKey(ra.fs.bktID, dataInfo.ID), dataInfo)

		newVersionID := core.NewID()
		return newVersionID, nil
	}

	// Has SDK configuration, needs compression and encryption
	// Stream processing: read original data by chunk, apply write operations, process and write to new object immediately

	// Pre-calculate write operation indices for each chunk
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

	// For uncompressed and unencrypted data, can directly read by chunk without reading all data first
	// Create a special reader to support reading by chunk
	var reader io.Reader
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// Create plainDataReader to support reading by chunk
		reader = newPlainDataReader(ra.fs.c, ra.fs.h, ra.fs.bktID, oldDataID, int64(chunkSizeInt))
	}

	// Stream processing: read, process, and write by chunk
	return ra.processWritesStreaming(reader, writesByChunk, writes, dataInfo, chunkSizeInt, newSize, chunkCount)
}

// processWritesStreaming streams processing of write operations
// Read original data by chunk, apply write operations, immediately process (compress/encrypt) and write to new object
func (ra *RandomAccessor) processWritesStreaming(
	reader io.Reader,
	writesByChunk [][]int,
	writes []WriteOperation,
	dataInfo *core.DataInfo,
	chunkSizeInt int,
	newSize int64,
	chunkCount int,
) (int64, error) {
	bucket := ra.fs.getBucketConfig()

	// Initialize compressor (if smart compression is enabled)
	var cmpr archiver.Compressor
	var hasCmpr bool
	if bucket != nil && bucket.CmprWay > 0 {
		// Compressor will be determined when processing the first chunk (needs to check file type)
		hasCmpr = true
		if bucket.CmprWay&core.DATA_CMPR_SNAPPY != 0 {
			cmpr = &archiver.Snappy{}
		} else if bucket.CmprWay&core.DATA_CMPR_ZSTD != 0 {
			cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(bucket.CmprQlty)))}}
		} else if bucket.CmprWay&core.DATA_CMPR_GZIP != 0 {
			cmpr = &archiver.Gz{CompressionLevel: int(bucket.CmprQlty)}
		} else if bucket.CmprWay&core.DATA_CMPR_BR != 0 {
			cmpr = &archiver.Brotli{Quality: int(bucket.CmprQlty)}
		}
		if cmpr != nil {
			dataInfo.Kind |= bucket.CmprWay
		}
	}

	// If encryption is set, set encryption flag
	if bucket != nil && bucket.EndecWay > 0 {
		dataInfo.Kind |= bucket.EndecWay
	}

	// Calculate CRC32 (original data)
	var crc32Val uint32
	var dataCRC32 uint32

	sn := 0
	currentPos := int64(0)
	firstChunk := true

	// Stream process by chunk
	for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
		pos := int64(chunkIdx * chunkSizeInt)
		chunkEnd := pos + int64(chunkSizeInt)
		if chunkEnd > newSize {
			chunkEnd = newSize
		}
		actualChunkSize := int(chunkEnd - pos)

		// Get chunk buffer from object pool
		chunkData := chunkDataPool.Get().([]byte)
		if cap(chunkData) < actualChunkSize {
			chunkData = make([]byte, actualChunkSize)
		} else {
			chunkData = chunkData[:actualChunkSize]
		}

		// 1. Read this chunk of original data from reader
		// Note: reader is sequential, need to read sequentially to current position
		if reader != nil {
			// Need to read to current chunk position
			bytesToSkip := pos - currentPos
			if bytesToSkip > 0 {
				// Skip unnecessary data (read and discard)
				skipBuf := make([]byte, bytesToSkip)
				_, err := io.ReadFull(reader, skipBuf)
				if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					chunkDataPool.Put(chunkData[:0])
					return 0, fmt.Errorf("failed to skip to chunk position: %w", err)
				}
			}
			currentPos = pos

			// Read current chunk data
			n, err := reader.Read(chunkData)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				chunkDataPool.Put(chunkData[:0])
				return 0, fmt.Errorf("failed to read chunk: %w", err)
			}
			// If read data is less than chunk size, remaining part stays as 0 (new data)
			if n < actualChunkSize {
				// Zero remaining part
				for i := n; i < actualChunkSize; i++ {
					chunkData[i] = 0
				}
			}
			currentPos = chunkEnd
		} else {
			// No original data, initialize to 0
			for i := range chunkData {
				chunkData[i] = 0
			}
		}

		// 2. Apply write operations to current chunk
		for _, writeIdx := range writesByChunk[chunkIdx] {
			write := writes[writeIdx]
			writeEnd := write.Offset + int64(len(write.Data))

			// Calculate overlap range
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

		// 3. If it's the first chunk, check file type to determine if compression is needed
		if firstChunk && bucket != nil && bucket.CmprWay > 0 && len(chunkData) > 0 {
			kind, _ := filetype.Match(chunkData)
			if kind != filetype.Unknown {
				// Not unknown type, don't compress
				dataInfo.Kind &= ^core.DATA_CMPR_MASK
				cmpr = nil
				hasCmpr = false
			}
			firstChunk = false
		}

		// 4. Calculate CRC32 of original data
		dataCRC32 = crc32.Update(dataCRC32, crc32.IEEETable, chunkData)

		// 5. Compress (if enabled)
		var processedChunk []byte
		if hasCmpr && cmpr != nil {
			var cmprBuf bytes.Buffer
			err := cmpr.Compress(bytes.NewBuffer(chunkData), &cmprBuf)
			if err != nil {
				processedChunk = chunkData
				// Compression failed, only remove compression flag on first chunk
				if firstChunk {
					dataInfo.Kind &= ^core.DATA_CMPR_MASK
					hasCmpr = false
				}
			} else {
				// If compressed size is larger or equal, use original data and remove compression flag
				// Note: this logic only executes on first chunk, because once compression is decided, subsequent chunks should be consistent
				if firstChunk && cmprBuf.Len() >= len(chunkData) {
					processedChunk = chunkData
					dataInfo.Kind &= ^core.DATA_CMPR_MASK
					hasCmpr = false
				} else if !firstChunk && cmprBuf.Len() >= len(chunkData) {
					// For subsequent chunks, if compressed size is larger, still use compressed data (maintain consistency)
					processedChunk = cmprBuf.Bytes()
				} else {
					processedChunk = cmprBuf.Bytes()
				}
			}
		} else {
			processedChunk = chunkData
		}

		// 6. Encrypt (if enabled)
		var encodedChunk []byte
		var err error
		if bucket != nil && dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
			encodedChunk, err = aes256.Encrypt(bucket.EndecKey, processedChunk)
		} else if bucket != nil && dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
			encodedChunk, err = sm4.Sm4Cbc([]byte(bucket.EndecKey), processedChunk, true)
		} else {
			encodedChunk = processedChunk
		}
		if err != nil {
			encodedChunk = processedChunk
		}

		// 7. Update checksum of final data
		crc32Val = crc32.Update(crc32Val, crc32.IEEETable, encodedChunk)

		// 8. Update size (if compressed or encrypted)
		if dataInfo.Kind&core.DATA_CMPR_MASK != 0 || dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
			dataInfo.Size += int64(len(encodedChunk))
		}

		// 9. Immediately write to new object (stream write)
		encodedChunkCopy := make([]byte, len(encodedChunk))
		copy(encodedChunkCopy, encodedChunk)
		if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, encodedChunkCopy); err != nil {
			chunkDataPool.Put(chunkData[:0])
			return 0, err
		}
		sn++

		// Return chunkData to object pool
		chunkDataPool.Put(chunkData[:0])
	}

	// Set CRC32 and checksum
	dataInfo.CRC32 = dataCRC32
	if dataInfo.Kind&core.DATA_CMPR_MASK == 0 && dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		dataInfo.Size = dataInfo.OrigSize
		dataInfo.Cksum = dataCRC32
	} else {
		dataInfo.Cksum = crc32Val
	}

	// Save data metadata
	_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return 0, err
	}

	// Update cache
	dataInfoCache.Put(formatCacheKey(ra.fs.bktID, dataInfo.ID), dataInfo)

	newVersionID := core.NewID()
	return newVersionID, nil
}

// dataReader data reader interface, unified handling of data reading in different formats
type dataReader interface {
	io.Reader
}

// readWithWrites unified handling of read logic: calculate read range, read data, apply write operations, extract result
func (ra *RandomAccessor) readWithWrites(reader dataReader, offset int64, size int) ([]byte, bool) {
	// 1. Check write operations in buffer to determine required data range
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}
	mergedOps := mergeWriteOperations(operations)

	// 2. Calculate actual required data range (considering impact of write operations)
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

	// 3. Only read required data range
	readSize := readEnd - readStart
	if readSize <= 0 {
		return []byte{}, true
	}

	// Skip data before readStart
	if readStart > 0 {
		skipBuf := make([]byte, readStart)
		_, err := io.ReadFull(reader, skipBuf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, false
		}
	}

	// Read required data range
	readData := make([]byte, readSize)
	n, err := io.ReadFull(reader, readData)
	// If read fails or read data is less than requested size, only return read data
	if err != nil {
		// io.EOF or io.ErrUnexpectedEOF means all available data has been read
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if n > 0 {
				readData = readData[:n]
			} else {
				readData = []byte{}
			}
		} else {
			// Other errors, return failure
			return nil, false
		}
	} else if int64(n) < readSize {
		// Read data is less than requested size (end of file), extract actually read data
		readData = readData[:n]
	}

	// 4. Apply write operations to read data
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

	// 5. Extract requested range (offset to offset+size, relative to readStart)
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

// plainDataReader supports reading uncompressed and unencrypted data by chunk
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
		chunkSize = 4 << 20 // Default 4MB
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
		// If buffer is empty or fully read, read next chunk
		if pr.buf == nil || pr.bufPos >= len(pr.buf) {
			// Use sn to read chunk (uncompressed and unencrypted data is stored by chunk)
			chunkData, err := pr.h.GetData(pr.c, pr.bktID, pr.dataID, pr.sn)
			if err != nil {
				// If read fails, chunk may not exist (end of file)
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

		// Copy data from buffer
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

// decodingChunkReader supports reading, decrypting, and decompressing data by chunk (stream reading, process while reading)
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
		// If buffer is empty or fully read, read next chunk
		if dr.buf == nil || dr.bufPos >= len(dr.buf) {
			// Read chunk (compressed and encrypted data)
			encryptedChunk, err := dr.h.GetData(dr.c, dr.bktID, dr.dataID, dr.sn)
			if err != nil {
				// If read fails, chunk may not exist (end of file)
				if totalRead == 0 {
					return 0, io.EOF
				}
				return totalRead, nil
			}
			dr.remain -= len(encryptedChunk)
			dr.sn++

			// 1. Decrypt first (if enabled)
			decodedChunk := encryptedChunk
			if dr.kind&core.DATA_ENDEC_AES256 != 0 {
				decodedChunk, err = aes256.Decrypt(dr.endecKey, encryptedChunk)
			} else if dr.kind&core.DATA_ENDEC_SM4 != 0 {
				decodedChunk, err = sm4.Sm4Cbc([]byte(dr.endecKey), encryptedChunk, false)
			}
			if err != nil {
				// Decryption failed, use original data
				decodedChunk = encryptedChunk
			}

			// 2. Decompress next (if enabled)
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
						// Decompression failed, use decrypted data
						finalChunk = decodedChunk
					} else {
						finalChunk = decompressedBuf.Bytes()
					}
				}
			}

			dr.buf = finalChunk
			dr.bufPos = 0
		}

		// Copy data from buffer
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

// Truncate truncates file to specified size
// If newSize > 0, references previous data block but with new size, creates new version
// If newSize == 0, uses empty data block (EmptyDataID)
func (ra *RandomAccessor) Truncate(newSize int64) (int64, error) {
	// Flush any pending writes first
	_, err := ra.Flush()
	if err != nil {
		return 0, err
	}

	// Invalidate cache to ensure we get fresh fileObj
	// Use typed nil pointer instead of nil for atomic.Value
	ra.fileObj.Store((*core.ObjectInfo)(nil))

	// Get current file object
	fileObj, err := ra.getFileObj()
	if err != nil {
		return 0, err
	}

	oldSize := fileObj.Size
	if newSize == oldSize {
		// No change needed
		return 0, nil
	}

	// Get LocalHandler
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	// Generate new version ID
	newVersionID := core.NewID()
	if newVersionID <= 0 {
		return 0, fmt.Errorf("failed to generate version ID")
	}

	var newDataID int64
	var newDataInfo *core.DataInfo

	if newSize == 0 {
		// Use empty data block
		newDataID = core.EmptyDataID
		newDataInfo = &core.DataInfo{
			ID:        newDataID,
			Size:      0,
			OrigSize:  0,
			Kind:      core.DATA_NORMAL,
			PkgID:     0,
			PkgOffset: 0,
		}
	} else {
		// Reference previous data block but with new size
		oldDataID := fileObj.DataID
		if oldDataID > 0 && oldDataID != core.EmptyDataID {
			// Get old DataInfo
			oldDataInfo, err := ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, oldDataID)
			if err == nil && oldDataInfo != nil {
				// Generate new DataID
				newDataID = core.NewID()
				if newDataID <= 0 {
					return 0, fmt.Errorf("failed to generate DataID")
				}

				if oldDataInfo.PkgID > 0 {
					// If data is in package, we can reference the same package
					// If newSize > original size, need to read, extend with zeros, and write
					// If newSize == original size, reference same package with new OrigSize
					// If newSize < original size, we need to read, process, and write truncated data
					if newSize > int64(oldDataInfo.OrigSize) {
						// File is extended, need to read original data and fill with zeros
						// Use Read method to get decompressed/decrypted data
						readData, readErr := ra.Read(0, int(oldDataInfo.OrigSize))
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}
						// Ensure we only use OrigSize bytes
						if int64(len(readData)) > int64(oldDataInfo.OrigSize) {
							readData = readData[:oldDataInfo.OrigSize]
						}

						// Extend with zeros
						extendedData := make([]byte, newSize)
						copy(extendedData, readData)
						// Remaining bytes are already zero (Go zero-initializes slices)

						// Write extended data (will be compressed/encrypted if needed)
						if err := ra.Write(0, extendedData); err != nil {
							return 0, fmt.Errorf("failed to write extended data: %v", err)
						}
						_, flushErr := ra.Flush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush extended data: %v", flushErr)
						}

						// Get updated fileObj to get new DataID
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						// Verify size is correct
						if updatedFileObj.Size != newSize {
							return 0, fmt.Errorf("truncated file size mismatch: expected %d, got %d", newSize, updatedFileObj.Size)
						}

						// Return version ID (already created by Flush)
						versionID := core.NewID()
						if versionID <= 0 {
							return 0, fmt.Errorf("failed to generate version ID")
						}

						// Create version object for this truncate operation
						mTime := core.Now()
						newVersion := &core.ObjectInfo{
							ID:     versionID,
							PID:    ra.fileID,
							Type:   core.OBJ_TYPE_VERSION,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Update file object (if not already updated by Flush)
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Batch write version and update file object
						objectsToPut := []*core.ObjectInfo{newVersion, updateFileObj}
						_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
						if err != nil {
							return 0, fmt.Errorf("failed to update file object: %v", err)
						}

						// Update cache
						fileObjCache.Put(ra.fileObjKey, updateFileObj)
						ra.fileObj.Store(updateFileObj)

						return versionID, nil
					} else if newSize == int64(oldDataInfo.OrigSize) {
						// Same size, reference same package with new OrigSize
						newDataInfo = &core.DataInfo{
							ID:        newDataID,
							Size:      oldDataInfo.Size, // Keep compressed/encrypted size
							OrigSize:  newSize,          // New original size (same as before)
							Kind:      oldDataInfo.Kind, // Keep compression/encryption flags
							PkgID:     oldDataInfo.PkgID,
							PkgOffset: oldDataInfo.PkgOffset,
						}
					} else {
						// newSize < original size, need to read, decompress/decrypt, truncate, and re-compress/encrypt
						// Invalidate old DataInfo cache to ensure we get fresh data
						oldDataInfoCacheKey := formatCacheKey(ra.fs.bktID, oldDataID)
						dataInfoCache.Del(oldDataInfoCacheKey)

						// Use Read method to get decompressed/decrypted data (only read up to newSize)
						// Note: Read method should limit reading to requested size, but we'll ensure it
						readData, readErr := ra.Read(0, int(newSize))
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}
						// Ensure we only use newSize bytes (Read may return more due to compression/encryption)
						if int64(len(readData)) > newSize {
							readData = readData[:newSize]
						}

						// Write truncated data (will be compressed/encrypted if needed)
						// Use Write and Flush to handle compression/encryption
						if err := ra.Write(0, readData); err != nil {
							return 0, fmt.Errorf("failed to write truncated data: %v", err)
						}
						_, flushErr := ra.Flush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush truncated data: %v", flushErr)
						}

						// Flush already created new version and updated fileObj
						// Invalidate cache to ensure we get fresh fileObj
						// Use typed nil pointer instead of nil for atomic.Value
						ra.fileObj.Store((*core.ObjectInfo)(nil))

						// Get updated fileObj to verify
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						// Verify size is correct
						if updatedFileObj.Size != newSize {
							return 0, fmt.Errorf("truncated file size mismatch: expected %d, got %d", newSize, updatedFileObj.Size)
						}

						// Invalidate old DataInfo cache to ensure fresh reads
						if oldDataID > 0 && oldDataID != core.EmptyDataID && oldDataID != updatedFileObj.DataID {
							oldDataInfoCacheKey := formatCacheKey(ra.fs.bktID, oldDataID)
							dataInfoCache.Del(oldDataInfoCacheKey)
						}

						// Return version ID (Flush already created it, but we need to return a version ID)
						// Actually, we should return the version ID from the Flush operation
						// But Flush returns version ID, so we can use that
						// However, we already called Flush above, so we need to generate a new version ID
						// Or we can just return 0 to indicate success (version was created by Flush)
						// Actually, let's create a version object to track this truncate operation
						versionID := core.NewID()
						if versionID <= 0 {
							return 0, fmt.Errorf("failed to generate version ID")
						}

						// Create version object for this truncate operation
						mTime := core.Now()
						newVersion := &core.ObjectInfo{
							ID:     versionID,
							PID:    ra.fileID,
							Type:   core.OBJ_TYPE_VERSION,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Update file object (if not already updated by Flush)
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Batch write version and update file object
						objectsToPut := []*core.ObjectInfo{newVersion, updateFileObj}
						_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
						if err != nil {
							return 0, fmt.Errorf("failed to update file object: %v", err)
						}

						// Update cache
						fileObjCache.Put(ra.fileObjKey, updateFileObj)
						ra.fileObj.Store(updateFileObj)

						return versionID, nil
					}
				} else {
					// Direct data (not in package)
					if newSize > int64(oldDataInfo.OrigSize) {
						// File is extended, need to read original data and fill with zeros
						// Use Read method to get decompressed/decrypted data
						readData, readErr := ra.Read(0, int(oldDataInfo.OrigSize))
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}
						// Ensure we only use OrigSize bytes
						if int64(len(readData)) > int64(oldDataInfo.OrigSize) {
							readData = readData[:oldDataInfo.OrigSize]
						}

						// Extend with zeros
						extendedData := make([]byte, newSize)
						copy(extendedData, readData)
						// Remaining bytes are already zero (Go zero-initializes slices)

						// Write extended data (will be compressed/encrypted if needed)
						if err := ra.Write(0, extendedData); err != nil {
							return 0, fmt.Errorf("failed to write extended data: %v", err)
						}
						_, flushErr := ra.Flush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush extended data: %v", flushErr)
						}

						// Get updated fileObj to get new DataID
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						// Verify size is correct
						if updatedFileObj.Size != newSize {
							return 0, fmt.Errorf("truncated file size mismatch: expected %d, got %d", newSize, updatedFileObj.Size)
						}

						// Return version ID (already created by Flush)
						versionID := core.NewID()
						if versionID <= 0 {
							return 0, fmt.Errorf("failed to generate version ID")
						}

						// Create version object for this truncate operation
						mTime := core.Now()
						newVersion := &core.ObjectInfo{
							ID:     versionID,
							PID:    ra.fileID,
							Type:   core.OBJ_TYPE_VERSION,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Update file object (if not already updated by Flush)
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Batch write version and update file object
						objectsToPut := []*core.ObjectInfo{newVersion, updateFileObj}
						_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
						if err != nil {
							return 0, fmt.Errorf("failed to update file object: %v", err)
						}

						// Update cache
						fileObjCache.Put(ra.fileObjKey, updateFileObj)
						ra.fileObj.Store(updateFileObj)

						return versionID, nil
					} else if newSize == int64(oldDataInfo.OrigSize) {
						// Same size, reference same data block with new OrigSize
						// Read original data
						data, readErr := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, oldDataID, 0, 0, int(newSize))
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}

						// Write data to new DataID
						_, writeErr := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, newDataID, 0, data)
						if writeErr != nil {
							return 0, fmt.Errorf("failed to write data: %v", writeErr)
						}

						// Create new DataInfo
						newDataInfo = &core.DataInfo{
							ID:        newDataID,
							Size:      int64(len(data)),
							OrigSize:  int64(len(data)),
							Kind:      oldDataInfo.Kind,
							PkgID:     0,
							PkgOffset: 0,
						}
					} else {
						// newSize < original size, need to read and write truncated data
						// Read data up to newSize
						data, readErr := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, oldDataID, 0, 0, int(newSize))
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}

						// Write truncated data to new DataID
						_, writeErr := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, newDataID, 0, data)
						if writeErr != nil {
							return 0, fmt.Errorf("failed to write truncated data: %v", writeErr)
						}

						// Create new DataInfo
						newDataInfo = &core.DataInfo{
							ID:        newDataID,
							Size:      int64(len(data)),
							OrigSize:  int64(len(data)),
							Kind:      oldDataInfo.Kind,
							PkgID:     0,
							PkgOffset: 0,
						}
					}
				}
			} else {
				// No old DataInfo, create new empty data block
				newDataID = core.EmptyDataID
				newDataInfo = &core.DataInfo{
					ID:        newDataID,
					Size:      0,
					OrigSize:  0,
					Kind:      core.DATA_NORMAL,
					PkgID:     0,
					PkgOffset: 0,
				}
			}
		} else {
			// No old data, create new empty data block
			newDataID = core.EmptyDataID
			newDataInfo = &core.DataInfo{
				ID:        newDataID,
				Size:      0,
				OrigSize:  0,
				Kind:      core.DATA_NORMAL,
				PkgID:     0,
				PkgOffset: 0,
			}
		}
	}

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

	// Update file object
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// Optimization: Use PutDataInfoAndObj to write DataInfo, version object, and file object update together
	// This reduces database round trips and improves performance
	var dataInfos []*core.DataInfo
	if newDataInfo != nil && newDataID != core.EmptyDataID {
		dataInfos = []*core.DataInfo{newDataInfo}
	}
	objectsToPut := []*core.ObjectInfo{newVersion, updateFileObj}

	if len(dataInfos) > 0 {
		// Write DataInfo and ObjectInfo together
		err = lh.PutDataInfoAndObj(ra.fs.c, ra.fs.bktID, dataInfos, objectsToPut)
		if err != nil {
			return 0, fmt.Errorf("failed to save DataInfo and update objects: %v", err)
		}
		// Update cache
		dataInfoCache.Put(formatCacheKey(ra.fs.bktID, newDataID), newDataInfo)
	} else {
		// Only write ObjectInfo (no DataInfo to write)
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
		if err != nil {
			return 0, fmt.Errorf("failed to update file object: %v", err)
		}
	}

	// Update cache
	fileObjCache.Put(ra.fileObjKey, updateFileObj)
	ra.fileObj.Store(updateFileObj)

	// Invalidate old DataInfo cache if DataID changed
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID && fileObj.DataID != newDataID {
		oldDataInfoCacheKey := formatCacheKey(ra.fs.bktID, fileObj.DataID)
		dataInfoCache.Del(oldDataInfoCacheKey)
	}

	return newVersionID, nil
}

func (ra *RandomAccessor) Close() error {
	// Cancel delayed flush timer to ensure synchronous flush (atomic swap)
	// Use typed nil pointer (*time.Timer)(nil) instead of nil for atomic.Value
	oldTimer := ra.pendingFlush.Swap((*time.Timer)(nil))
	if oldTimer != nil {
		if timer, ok := oldTimer.(*time.Timer); ok && timer != nil {
			timer.Stop()
		}
	}

	// Synchronously flush all pending write data
	_, err := ra.Flush()
	if err != nil {
		return err
	}

	// Ensure batch write data is also flushed (flush will flush all data)
	batchMgr := ra.fs.getBatchWriteManager()
	if batchMgr != nil {
		batchMgr.FlushAll(ra.fs.c)
	}

	return nil
}

// mergeWriteOperations merges overlapping write operations
// Optimization: use more efficient sorting algorithm (quicksort)
func mergeWriteOperations(operations []WriteOperation) []WriteOperation {
	if len(operations) == 0 {
		return nil
	}

	// Optimization: if operations are already sorted, can skip sorting
	// But for safety, still sort (can use more efficient algorithm)
	// Optimization: in-place sort to avoid extra memory allocation
	sorted := operations
	if len(sorted) > 1 {
		// Create new slice for sorting (avoid modifying original slice)
		sorted = make([]WriteOperation, len(operations))
		copy(sorted, operations)
	}
	// Use quicksort (built-in sort package)
	// But to avoid introducing new dependencies, use optimized insertion sort
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j].Offset > key.Offset {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}

	// Optimization: pre-allocate capacity to reduce expansion
	// Optimization: use object pool to get initial capacity
	merged := writeOpsPool.Get().([]WriteOperation)
	merged = merged[:0] // Reset but keep capacity
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

		// Optimization: if completely overlapping and new operation overwrites old, directly replace (avoid creating new object)
		if op.Offset >= last.Offset && opEnd <= lastEnd {
			// New operation is completely within old operation, directly overwrite (avoid creating new WriteOperation)
			offsetInLast := op.Offset - last.Offset
			copy(last.Data[offsetInLast:], op.Data)
			continue
		}

		// If overlapping, merge
		if op.Offset <= lastEnd {
			// Calculate new range
			startOffset := last.Offset
			if op.Offset < startOffset {
				startOffset = op.Offset
			}
			endOffset := lastEnd
			if opEnd > endOffset {
				endOffset = opEnd
			}

			// Optimization: use object pool to get buffer
			mergedData := chunkDataPool.Get().([]byte)
			if cap(mergedData) < int(endOffset-startOffset) {
				mergedData = make([]byte, endOffset-startOffset)
			} else {
				mergedData = mergedData[:endOffset-startOffset]
			}

			// Copy old data
			if last.Offset >= startOffset {
				copy(mergedData[last.Offset-startOffset:], last.Data)
			}
			// Copy new data (overwrite)
			if op.Offset >= startOffset {
				copy(mergedData[op.Offset-startOffset:], op.Data)
			}

			last.Offset = startOffset
			last.Data = mergedData
		} else {
			// Not overlapping, add new operation
			merged = append(merged, op)
		}
	}

	return merged
}

// applyWritesToData applies write operations to data
// Optimization: calculate required size at once to avoid multiple expansions
func applyWritesToData(data []byte, writes []WriteOperation) []byte {
	if len(writes) == 0 {
		return data
	}

	// Calculate required size (optimization: calculate at once to avoid multiple expansions)
	var maxSize int64 = int64(len(data))
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > maxSize {
			maxSize = writeEnd
		}
	}

	// Optimization: allocate required size at once to avoid multiple expansions
	result := make([]byte, maxSize)
	if len(data) > 0 {
		copy(result, data)
	}

	// Apply all write operations (optimization: remove redundant checks, maxSize already guarantees sufficient capacity)
	for _, write := range writes {
		if len(write.Data) > 0 {
			writeEnd := write.Offset + int64(len(write.Data))
			copy(result[write.Offset:writeEnd], write.Data)
		}
	}

	return result
}
