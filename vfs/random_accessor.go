package vfs

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
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
	"golang.org/x/sync/singleflight"
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
	// key: "<dataID>", value: *core.DataInfo (dataID is globally unique)
	dataInfoCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// ecache cache: cache file object information to reduce database queries
	// key: "<fileID>", value: *core.ObjectInfo (fileID is globally unique)
	fileObjCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// ecache cache: cache directory listing to reduce database queries
	// key: "<dirID>", value: []*core.ObjectInfo (dirID is globally unique)
	dirListCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// singleflight group: prevent duplicate concurrent requests for the same directory
	// key: "<dirID>", ensures only one request per directory at a time
	dirListSingleFlight singleflight.Group
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

	// Note: Do NOT immediately update fileObj in database here
	// Batch write manager will batch update all file objects together when flushing
	// This is critical for batch write performance - all metadata updates happen in one transaction
	// The fileObj will be updated by batch write manager during flush operation
	// For reads before flush, the pendingObjects cache in batch write manager provides visibility

	// Update local cache only (not database) for immediate visibility
	// This allows reads to work correctly before batch flush completes
	if fileObj != nil {
		updateFileObj := &core.ObjectInfo{
			ID:     fileObj.ID,
			PID:    fileObj.PID,
			DataID: dataID,
			Size:   origSize, // Use original size
			MTime:  core.Now(),
			Type:   fileObj.Type,
			Name:   fileObj.Name,
		}
		// Update local cache only (not database)
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
	}

	return true, dataID, nil
}

// formatCacheKey formats cache key (optimized: direct memory copy, highest performance)
// id is globally unique, so no need for bktID
func formatCacheKey(id int64) string {
	// Create fixed-size byte array on stack
	var buf [8]byte

	// Directly use unsafe to copy 8-byte int64 memory to byte array (highest performance)
	// Avoid function call overhead, direct memory operation
	*(*int64)(unsafe.Pointer(&buf[0])) = id
	return string(buf[:])
}

// WriteOperation represents a single write operation
type WriteOperation struct {
	Offset int64  // Write offset
	Data   []byte // Write data
}

// PendingWrite represents a pending write operation waiting to be ordered
type PendingWrite struct {
	Offset int64
	Data   []byte
	Done   chan error // Channel to signal completion
}

// ConcurrentSequentialDetector detects concurrent sequential write patterns
// Used to optimize 6-thread concurrent writes into sequential writes
type ConcurrentSequentialDetector struct {
	mu               sync.Mutex
	pendingWrites    map[int64]*PendingWrite // Map offset -> pending write
	expectedOffset   int64                   // Next expected offset
	blockSize        int64                   // Detected block size (0 if not detected)
	consecutiveCount int                     // Count of consecutive blocks detected
	enabled          bool                    // Whether concurrent sequential mode is enabled
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
	// writing version support (for resumable sequential writes)
	writingVersionID int64 // Writing version object ID
}

// TempFileWriter handles efficient fragmented writes for .tmp files
// Uses in-memory buffers to accumulate data until a complete chunk is ready
// Supports optional real-time compression/encryption (can be disabled for offline processing)
// Supports concurrent writes to different chunks
type TempFileWriter struct {
	fs              *OrcasFS
	fileID          int64
	dataID          int64                // Data ID for this .tmp file
	chunkSize       int64                // Chunk size for this file (always bucket chunk size)
	sn              int                  // Current sequence number (max sn written)
	size            atomic.Int64         // Total file size (atomic for concurrent access)
	mu              sync.Mutex           // Mutex for thread-safe operations on chunks map
	chunks          map[int]*chunkBuffer // Chunk buffers for each chunk (key: sn)
	dataInfo        *core.DataInfo       // DataInfo for tracking compression/encryption
	enableRealtime  bool                 // Whether to enable real-time compression/encryption (default: false for offline processing)
	realtimeDecided bool                 // Whether realtime capability has been determined
	lh              *core.LocalHandler   // Cached LocalHandler (nil if not LocalHandler)
}

// chunkBuffer holds data for a single chunk before compression/encryption and upload
type chunkBuffer struct {
	data          []byte     // Buffer data (pre-allocated to chunkSize)
	offsetInChunk int64      // Current write position within this chunk (0 to chunkSize)
	mu            sync.Mutex // Mutex for this specific chunk buffer
}

// RandomAccessor random access object in VFS, supports compression and encryption
type RandomAccessor struct {
	fs           *OrcasFS
	fileID       int64
	buffer       *WriteBuffer           // Random write buffer
	seqBuffer    *SequentialWriteBuffer // Sequential write buffer (optimized)
	fileObj      atomic.Value
	fileObjKey   string                        // Pre-computed file_obj cache key (optimized: avoid repeated conversion)
	pendingFlush atomic.Value                  // Delayed flush timer (*time.Timer)
	sparseSize   atomic.Int64                  // Sparse file size (for pre-allocated files, e.g., qBittorrent)
	lastOffset   atomic.Int64                  // Last write offset (for sequential write detection)
	seqDetector  *ConcurrentSequentialDetector // Concurrent sequential write detector
	tempWriter   *TempFileWriter               // Simple writer for .tmp files
	tempWriterMu sync.Mutex                    // Mutex for tempWriter initialization
}

func isTempFile(obj *core.ObjectInfo) bool {
	if obj == nil {
		return false
	}
	name := strings.ToLower(obj.Name)
	return strings.HasSuffix(name, ".tmp")
}

// setChunkSizeIfDifferent sets chunk size to object only if it differs from bucket's default
func (ra *RandomAccessor) setChunkSizeIfDifferent(obj *core.ObjectInfo, chunkSize int64) {
	if obj == nil {
		return
	}

	// Get bucket's default chunk size
	bucketChunkSize := ra.fs.chunkSize
	if bucketChunkSize <= 0 {
		bucketChunkSize = 10 << 20 // Default 10MB
	}

	// Only save chunk size if it differs from bucket's default
	if chunkSize != bucketChunkSize {
		core.SetChunkSizeToObject(obj, chunkSize)
		DebugLog("[VFS setChunkSizeIfDifferent] Saved chunk size to object (differs from bucket): fileID=%d, chunkSize=%d, bucketChunkSize=%d", obj.ID, chunkSize, bucketChunkSize)
	} else {
		DebugLog("[VFS setChunkSizeIfDifferent] Skipped saving chunk size (same as bucket): fileID=%d, chunkSize=%d", obj.ID, chunkSize)
	}
}

// getOrCreateTempWriter gets or creates TempFileWriter for .tmp files
func (ra *RandomAccessor) getOrCreateTempWriter() (*TempFileWriter, error) {
	ra.tempWriterMu.Lock()
	defer ra.tempWriterMu.Unlock()

	if ra.tempWriter != nil {
		return ra.tempWriter, nil
	}

	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// Enforce fixed chunk size from bucket configuration
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Create new DataID for .tmp file
	dataID := core.NewID()
	if dataID <= 0 {
		return nil, fmt.Errorf("failed to create DataID for .tmp file")
	}

	// Initialize DataInfo with compression/encryption flags
	dataInfo := &core.DataInfo{
		ID:       dataID,
		OrigSize: 0,
		Size:     0,
		Kind:     core.DATA_NORMAL,
		CRC32:    0,
	}

	// Cache LocalHandler if available
	var lh *core.LocalHandler
	if handler, ok := ra.fs.h.(*core.LocalHandler); ok {
		lh = handler
	}

	ra.tempWriter = &TempFileWriter{
		fs:              ra.fs,
		fileID:          ra.fileID,
		dataID:          dataID,
		chunkSize:       chunkSize,
		sn:              0,
		size:            atomic.Int64{},
		chunks:          make(map[int]*chunkBuffer),
		dataInfo:        dataInfo,
		enableRealtime:  false,
		realtimeDecided: false,
		lh:              lh,
	}

	// Log creation of TempFileWriter for large file
	DebugLog("[VFS TempFileWriter Create] Created TempFileWriter for large file: fileID=%d, dataID=%d, fileName=%s, chunkSize=%d, realtimeCompressEncrypt=%v",
		ra.fileID, dataID, fileObj.Name, chunkSize, ra.tempWriter.enableRealtime)

	return ra.tempWriter, nil
}

// Write accumulates data in memory buffers and writes complete chunks
// Supports fragmented writes within a single chunk by tracking write position
// Ensures continuous writes by verifying offset matches expected position
// For .tmp files, sn is calculated as offset / chunkSize
func (tw *TempFileWriter) Write(offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// Check handler type early
	if tw.lh == nil {
		return fmt.Errorf("handler is not LocalHandler, cannot use AppendData")
	}

	writeEnd := offset + int64(len(data))
	currentSize := tw.size.Load()
	DebugLog("[VFS TempFileWriter Write] Writing data to large file: fileID=%d, dataID=%d, offset=%d, size=%d, currentFileSize=%d, writeEnd=%d",
		tw.fileID, tw.dataID, offset, len(data), currentSize, writeEnd)

	currentOffset := offset
	dataPos := 0
	var completedChunks []int // Track completed chunks to flush after releasing locks

	// Process data across chunks
	for currentOffset < writeEnd {
		// Calculate sn directly from offset: sn = offset / chunkSize
		sn := int(currentOffset / tw.chunkSize)
		chunkStart := int64(sn) * tw.chunkSize
		writeStartInChunk := currentOffset - chunkStart

		// Calculate how much data to write in this chunk
		writeEndInChunk := writeEnd - chunkStart
		if writeEndInChunk > tw.chunkSize {
			writeEndInChunk = tw.chunkSize
		}
		writeSize := writeEndInChunk - writeStartInChunk

		// Extract data for this chunk
		chunkData := data[dataPos : dataPos+int(writeSize)]

		// Get or create chunk buffer
		tw.mu.Lock()
		buf, exists := tw.chunks[sn]
		if !exists {
			buf = &chunkBuffer{
				data:          make([]byte, tw.chunkSize), // Pre-allocate full chunk size
				offsetInChunk: 0,
			}
			tw.chunks[sn] = buf
			DebugLog("[VFS TempFileWriter Write] Created new chunk buffer: fileID=%d, dataID=%d, sn=%d, chunkSize=%d", tw.fileID, tw.dataID, sn, tw.chunkSize)
		}
		tw.mu.Unlock()

		// Lock this chunk buffer to ensure sequential writes within the chunk
		buf.mu.Lock()

		if sn == 0 && !tw.realtimeDecided {
			tw.decideRealtimeProcessing(chunkData)
		}

		// Copy data into buffer
		copy(buf.data[writeStartInChunk:writeStartInChunk+int64(len(chunkData))], chunkData)
		writeEndInChunkPos := writeStartInChunk + int64(len(chunkData))
		if writeEndInChunkPos > buf.offsetInChunk {
			buf.offsetInChunk = writeEndInChunkPos
		}

		bufferProgress := buf.offsetInChunk

		// Check if chunk is full
		chunkComplete := buf.offsetInChunk >= tw.chunkSize
		buf.mu.Unlock()

		DebugLog("[VFS TempFileWriter Write] Chunk complete: fileID=%d, dataID=%d, sn=%d, chunkSize=%d, bufferProgress=%d",
			tw.fileID, tw.dataID, sn, tw.chunkSize, buf.offsetInChunk, tw.chunkSize-buf.offsetInChunk)

		if chunkComplete {
			// Chunk is full, flush it (compress/encrypt if enabled, then write)
			DebugLog("[VFS TempFileWriter Write] Chunk full, flushing: fileID=%d, dataID=%d, sn=%d, bufferSize=%d/%d",
				tw.fileID, tw.dataID, sn, bufferProgress, tw.chunkSize)
			if err := tw.flushChunk(sn); err != nil {
				DebugLog("[VFS TempFileWriter Write] ERROR: Failed to flush chunk: fileID=%d, dataID=%d, sn=%d, error=%v", tw.fileID, tw.dataID, sn, err)
				return err
			}
			completedChunks = append(completedChunks, sn)
			currentFileSize := tw.size.Load()
			DebugLog("[VFS TempFileWriter Write] Chunk flushed successfully: fileID=%d, dataID=%d, sn=%d, chunkSize=%d, currentFileSize=%d",
				tw.fileID, tw.dataID, sn, bufferProgress, currentFileSize)
		} else {
			DebugLog("[VFS TempFileWriter Write] Data buffered in chunk: fileID=%d, dataID=%d, sn=%d, bufferSize=%d/%d, remaining=%d",
				tw.fileID, tw.dataID, sn, bufferProgress, tw.chunkSize, tw.chunkSize-bufferProgress)
		}

		// Update tracking
		currentOffset += int64(len(chunkData))
		dataPos += int(len(chunkData))
		if sn >= tw.sn {
			tw.sn = sn + 1
		}
	}

	// Remove completed chunks (after releasing all chunk locks)
	if len(completedChunks) > 0 {
		tw.mu.Lock()
		for _, sn := range completedChunks {
			delete(tw.chunks, sn)
		}
		tw.mu.Unlock()
	}

	// Update size atomically
	for {
		currentSize := tw.size.Load()
		if writeEnd <= currentSize {
			break
		}
		if tw.size.CompareAndSwap(currentSize, writeEnd) {
			DebugLog("[VFS TempFileWriter Write] Updated file size: fileID=%d, dataID=%d, oldSize=%d, newSize=%d, totalChunks=%d",
				tw.fileID, tw.dataID, currentSize, writeEnd, tw.sn)
			break
		}
	}

	return nil
}

// flushChunk processes and writes a complete chunk
// If real-time compression/encryption is enabled, processes the chunk before writing
// Otherwise, writes raw data directly (for offline processing)
func (tw *TempFileWriter) flushChunk(sn int) error {
	tw.mu.Lock()
	buf, exists := tw.chunks[sn]
	tw.mu.Unlock()

	if !exists {
		return nil // Chunk already flushed
	}

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.offsetInChunk == 0 {
		return nil // No data to flush
	}

	// Extract chunk data (only up to written size)
	chunkData := buf.data[:buf.offsetInChunk]
	bucket := tw.fs.getBucketConfig()

	DebugLog("[VFS TempFileWriter flushChunk] Processing chunk: fileID=%d, dataID=%d, sn=%d, originalSize=%d, enableRealtime=%v",
		tw.fileID, tw.dataID, sn, len(chunkData), tw.enableRealtime)

	// Update DataInfo CRC32 and OrigSize
	// tw.dataInfo.CRC32 = crc32.Update(tw.dataInfo.CRC32, crc32.IEEETable, chunkData)
	tw.dataInfo.OrigSize += int64(len(chunkData))

	var finalData []byte
	var err error

	if tw.enableRealtime {
		// Real-time compression/encryption enabled
		// Process first chunk: check file type and compression effect
		isFirstChunk := sn == 0
		if isFirstChunk && bucket != nil && bucket.CmprWay > 0 && len(chunkData) > 0 {
			kind, _ := filetype.Match(chunkData)
			if kind != filetype.Unknown {
				// Not unknown type, don't compress
				tw.dataInfo.Kind &= ^core.DATA_CMPR_MASK
			}
		}

		// Compression (if enabled)
		var processedChunk []byte
		hasCmpr := tw.dataInfo.Kind&core.DATA_CMPR_MASK != 0
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
					if isFirstChunk {
						tw.dataInfo.Kind &= ^core.DATA_CMPR_MASK
					}
					processedChunk = chunkData
				} else {
					if isFirstChunk && cmprBuf.Len() >= len(chunkData) {
						processedChunk = chunkData
						tw.dataInfo.Kind &= ^core.DATA_CMPR_MASK
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
		if bucket != nil && tw.dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
			finalData, err = aes256.Encrypt(bucket.EndecKey, processedChunk)
			if err != nil {
				finalData = processedChunk
			}
		} else if bucket != nil && tw.dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
			finalData, err = sm4.Sm4Cbc([]byte(bucket.EndecKey), processedChunk, true)
			if err != nil {
				finalData = processedChunk
			}
		} else {
			finalData = processedChunk
		}

		// Update CRC32 and size of final data
		// tw.dataInfo.Cksum = crc32.Update(tw.dataInfo.Cksum, crc32.IEEETable, finalData)
		if tw.dataInfo.Kind&core.DATA_CMPR_MASK != 0 || tw.dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
			tw.dataInfo.Size += int64(len(finalData))
		}
		DebugLog("[VFS TempFileWriter flushChunk] Compression/encryption applied: fileID=%d, dataID=%d, sn=%d, originalSize=%d, finalSize=%d, ratio=%.2f%%",
			tw.fileID, tw.dataID, sn, len(chunkData), len(finalData), float64(len(finalData))*100.0/float64(len(chunkData)))
	} else {
		// Real-time compression/encryption disabled - write raw data for offline processing
		// Similar to writing versions, data will be processed offline later
		finalData = chunkData
		tw.dataInfo.Size += int64(len(finalData)) // For offline processing, Size = OrigSize initially
		DebugLog("[VFS TempFileWriter flushChunk] Writing raw data (offline processing): fileID=%d, dataID=%d, sn=%d, size=%d",
			tw.fileID, tw.dataID, sn, len(finalData))
	}

	// Write data block
	DebugLog("[VFS TempFileWriter flushChunk] Writing chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d, realtime=%v",
		tw.fileID, tw.dataID, sn, len(finalData), tw.enableRealtime)
	_, err = tw.fs.h.PutData(tw.fs.c, tw.fs.bktID, tw.dataID, sn, finalData)
	if err != nil {
		DebugLog("[VFS TempFileWriter flushChunk] ERROR: Failed to put data: fileID=%d, dataID=%d, sn=%d, error=%v", tw.fileID, tw.dataID, sn, err)
		return err
	}

	DebugLog("[VFS TempFileWriter flushChunk] Successfully wrote chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d, totalOrigSize=%d, totalSize=%d",
		tw.fileID, tw.dataID, sn, len(finalData), tw.dataInfo.OrigSize, tw.dataInfo.Size)
	return nil
}

func (tw *TempFileWriter) decideRealtimeProcessing(firstChunk []byte) {
	tw.realtimeDecided = true
	bucket := tw.fs.getBucketConfig()
	if bucket == nil {
		tw.enableRealtime = false
		tw.dataInfo.Kind = core.DATA_NORMAL
		return
	}

	enable := bucket.CmprWay > 0 || bucket.EndecWay > 0
	tw.enableRealtime = enable

	kind := core.DATA_NORMAL
	if enable {
		if bucket.CmprWay > 0 {
			kind |= bucket.CmprWay
		}
		if bucket.EndecWay&core.DATA_ENDEC_AES256 != 0 {
			kind |= core.DATA_ENDEC_AES256
		} else if bucket.EndecWay&core.DATA_ENDEC_SM4 != 0 {
			kind |= core.DATA_ENDEC_SM4
		}
	}

	// If compression is enabled, perform quick detection on first chunk to decide if we should keep it
	if kind&core.DATA_CMPR_MASK != 0 && len(firstChunk) > 0 {
		if detectedKind, _ := filetype.Match(firstChunk); detectedKind != filetype.Unknown {
			kind &= ^core.DATA_CMPR_MASK
		}
	}

	tw.dataInfo.Kind = kind
	DebugLog("[VFS TempFileWriter] realtime processing decided: fileID=%d, enableRealtime=%v, kind=0x%x", tw.fileID, tw.enableRealtime, tw.dataInfo.Kind)
}

// Flush uploads DataInfo and ObjectInfo for .tmp file
// Data chunks are already on disk via AppendData, so we only need to upload metadata
func (tw *TempFileWriter) Flush() error {
	size := tw.size.Load()
	DebugLog("[VFS TempFileWriter Flush] Starting flush for large file: fileID=%d, dataID=%d, fileSize=%d, totalChunks=%d",
		tw.fileID, tw.dataID, size, tw.sn)

	if size == 0 {
		// No data written, nothing to flush
		DebugLog("[VFS TempFileWriter Flush] No data written, skipping flush: fileID=%d, dataID=%d", tw.fileID, tw.dataID)
		return nil
	}

	// Get file object
	fileObj, err := tw.fs.h.Get(tw.fs.c, tw.fs.bktID, []int64{tw.fileID})
	if err != nil || len(fileObj) == 0 {
		return fmt.Errorf("failed to get file object: %v", err)
	}
	obj := fileObj[0]

	// Flush all remaining incomplete chunks
	tw.mu.Lock()
	remainingChunks := make([]int, 0, len(tw.chunks))
	for sn := range tw.chunks {
		remainingChunks = append(remainingChunks, sn)
	}
	tw.mu.Unlock()

	if len(remainingChunks) > 0 {
		DebugLog("[VFS TempFileWriter Flush] Flushing %d remaining incomplete chunks: fileID=%d, dataID=%d, chunks=%v",
			len(remainingChunks), tw.fileID, tw.dataID, remainingChunks)
	}

	for _, sn := range remainingChunks {
		if err := tw.flushChunk(sn); err != nil {
			DebugLog("[VFS TempFileWriter Flush] ERROR: Failed to flush remaining chunk: fileID=%d, dataID=%d, sn=%d, error=%v", tw.fileID, tw.dataID, sn, err)
			return err
		}
		tw.mu.Lock()
		delete(tw.chunks, sn)
		tw.mu.Unlock()
	}

	// Use the DataInfo we've been tracking (with compression/encryption flags and sizes)
	dataInfo := tw.dataInfo
	dataInfo.OrigSize = size
	if !tw.enableRealtime {
		// For offline processing, Size = OrigSize initially (will be updated after offline processing)
		dataInfo.Size = size
	}

	// Save chunk size to file object's Extra field only if it differs from bucket's default
	// Get bucket's default chunk size
	bucketChunkSize := tw.fs.chunkSize
	if bucketChunkSize <= 0 {
		bucketChunkSize = 10 << 20 // Default 10MB
	}
	if tw.chunkSize != bucketChunkSize {
		core.SetChunkSizeToObject(obj, tw.chunkSize)
		DebugLog("[VFS TempFileWriter Flush] Saved chunk size to object (differs from bucket): fileID=%d, chunkSize=%d, bucketChunkSize=%d", tw.fileID, tw.chunkSize, bucketChunkSize)
	} else {
		DebugLog("[VFS TempFileWriter Flush] Skipped saving chunk size (same as bucket): fileID=%d, chunkSize=%d", tw.fileID, tw.chunkSize)
	}

	// Update file object
	obj.DataID = tw.dataID
	obj.Size = size
	obj.MTime = core.Now()

	// Upload DataInfo and ObjectInfo together
	DebugLog("[VFS TempFileWriter Flush] Uploading metadata: fileID=%d, dataID=%d, fileName=%s, origSize=%d, size=%d, chunkSize=%d, totalChunks=%d, realtime=%v, hasCompression=%v, hasEncryption=%v",
		tw.fileID, tw.dataID, obj.Name, dataInfo.OrigSize, dataInfo.Size, tw.chunkSize, tw.sn, tw.enableRealtime,
		dataInfo.Kind&core.DATA_CMPR_MASK != 0, dataInfo.Kind&core.DATA_ENDEC_MASK != 0)
	err = tw.fs.h.PutDataInfoAndObj(tw.fs.c, tw.fs.bktID, []*core.DataInfo{dataInfo}, []*core.ObjectInfo{obj})
	if err != nil {
		DebugLog("[VFS TempFileWriter Flush] ERROR: Failed to upload DataInfo and ObjectInfo: fileID=%d, dataID=%d, error=%v", tw.fileID, tw.dataID, err)
		return err
	}

	// Update cache
	fileObjCache.Put(formatCacheKey(tw.fileID), obj)
	dataInfoCache.Put(formatCacheKey(tw.dataID), dataInfo)

	// Calculate compression ratio if applicable
	compressionRatio := 1.0
	if dataInfo.OrigSize > 0 && dataInfo.Size != dataInfo.OrigSize {
		compressionRatio = float64(dataInfo.Size) / float64(dataInfo.OrigSize)
	}

	DebugLog("[VFS TempFileWriter Flush] Successfully flushed large file: fileID=%d, dataID=%d, fileName=%s, origSize=%d, size=%d, chunkSize=%d, totalChunks=%d, compressionRatio=%.2f%%",
		tw.fileID, tw.dataID, obj.Name, dataInfo.OrigSize, dataInfo.Size, tw.chunkSize, tw.sn, compressionRatio*100.0)
	return nil
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
		fileObjKey: formatCacheKey(fileID), // Pre-compute and cache key
		buffer: &WriteBuffer{
			fileID:     fileID,
			operations: make([]WriteOperation, maxBufferWrites), // Fixed-length array
			writeIndex: 0,                                       // Start from 0
			totalSize:  0,
		},
		lastOffset: atomic.Int64{},
		seqDetector: &ConcurrentSequentialDetector{
			pendingWrites:    make(map[int64]*PendingWrite),
			expectedOffset:   -1,
			blockSize:        0,
			consecutiveCount: 0,
			enabled:          false,
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
// For .tmp files, uses simple TempFileWriter that directly calls PutData
func (ra *RandomAccessor) Write(offset int64, data []byte) error {
	// Check if this is a .tmp file - use simple TempFileWriter
	fileObj, err := ra.getFileObj()
	if err == nil && isTempFile(fileObj) {
		// For .tmp files, use simple TempFileWriter
		tw, err := ra.getOrCreateTempWriter()
		if err != nil {
			DebugLog("[VFS RandomAccessor Write] ERROR: Failed to get TempFileWriter: fileID=%d, error=%v", ra.fileID, err)
			return err
		}
		return tw.Write(offset, data)
	}

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

	// Initialize sequential write buffer
	if ra.seqBuffer == nil && len(data) > 0 {
		fileObj, err := ra.getFileObj()
		if err == nil {
			forceSequential := isTempFile(fileObj)

			if offset == 0 && (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) {
				// File has no data, can initialize sequential write buffer
				if initErr := ra.initSequentialBuffer(forceSequential); initErr == nil {
					// Initialization succeeded, use sequential write
					return ra.writeSequential(offset, data)
				}
				// Initialization failed, fallback to random write
			} else if forceSequential {
				// Try to resume sequential writes backed by writing version
				if initErr := ra.initSequentialBuffer(true); initErr == nil && ra.seqBuffer != nil && !ra.seqBuffer.closed {
					if offset == ra.seqBuffer.offset {
						return ra.writeSequential(offset, data)
					}
					// Offset mismatch will be handled by concurrent sequential detector
				}
			}
		}
	}

	// Random write mode: use original buffer logic
	// Optimization: reduce data copying, only copy when necessary
	// Check if exceeds capacity (optimized: check early to avoid out of bounds)
	config := core.GetWriteBufferConfig()

	// Detect sequential write pattern (even if file already has data)
	// Sequential writes are writes that continue from the last write position
	lastOffset := ra.lastOffset.Load()
	isSequentialWrite := false
	if lastOffset >= 0 {
		// Check if this write continues from the last write position
		if offset == lastOffset {
			isSequentialWrite = true
		}
	} else {
		// First write, check if it starts from file end (append mode)
		fileObj, err := ra.getFileObj()
		if err == nil && offset == fileObj.Size {
			isSequentialWrite = true
		}
	}

	// For sparse files (pre-allocated), use larger buffer threshold to reduce flush frequency
	// This is critical for qBittorrent random write performance
	sparseSize := ra.sparseSize.Load()
	isSparseFile := sparseSize > 0
	maxBufferSize := config.MaxBufferSize
	if isSparseFile {
		// For sparse files, allow larger buffer (2x) to reduce flush frequency
		// This significantly improves performance for random writes
		maxBufferSize = config.MaxBufferSize * 2
	} else if isSequentialWrite {
		// For sequential writes, use larger buffer (3x) to reduce flush frequency
		// This is critical for large file uploads and sequential writes
		maxBufferSize = config.MaxBufferSize * 3
	}

	if atomic.LoadInt64(&ra.buffer.writeIndex)+1 >= int64(len(ra.buffer.operations)) || atomic.LoadInt64(&ra.buffer.totalSize) >= maxBufferSize {
		// Exceeds capacity, need to force flush
		// Don't rollback writeIndex (already incremented, space is allocated)
		// Force flush current buffer (synchronous execution, ensure data is persisted)
		DebugLog("[VFS RandomAccessor Write] Buffer full, forcing flush for fileID=%d, writeIndex=%d, totalSize=%d, maxBufferSize=%d", ra.fileID, atomic.LoadInt64(&ra.buffer.writeIndex), atomic.LoadInt64(&ra.buffer.totalSize), maxBufferSize)
		_, err := ra.Flush()
		if err != nil {
			DebugLog("[VFS RandomAccessor Write] ERROR: Failed to flush buffer for fileID=%d: %v", ra.fileID, err)
			if err == core.ERR_QUOTA_EXCEED {
				DebugLog("[VFS RandomAccessor Write] ERROR: Quota exceeded during flush for fileID=%d", ra.fileID)
			}
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

	// Update last write offset for sequential write detection
	ra.lastOffset.Store(offset + int64(len(data)))

	// For .tmp files, reset delayed flush timer on each write (5 minutes from last write)
	// Note: This is handled by TempFileWriter for .tmp files, so we don't need to do it here
	// (The .tmp file check happens earlier in the function, so we skip this section for .tmp files)

	// Optimization: for sparse files and sequential writes, use longer delayed flush window to batch more writes
	// This reduces flush frequency and improves performance for random writes
	flushWindow := config.BufferWindow
	if isSparseFile {
		// For sparse files, use 2x flush window to batch more random writes
		flushWindow = config.BufferWindow * 2
	} else if isSequentialWrite {
		// For sequential writes, use 3x flush window to batch more writes
		// This significantly reduces flush frequency for large file uploads
		flushWindow = config.BufferWindow * 3
	}

	// Optimization: for small file writes, use batch write manager
	// If data size is small and hasn't reached force flush condition, add to batch write manager
	// Note: batch write only applies to small files, and needs to ensure data integrity
	// Here use delayed flush first, batch write logic is handled in Flush
	// For sequential writes, always use delayed flush to batch more writes
	if isSequentialWrite || (int64(len(data)) < maxBufferSize/10 && atomic.LoadInt64(&ra.buffer.totalSize) < maxBufferSize) {
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
func (ra *RandomAccessor) initSequentialBuffer(force bool) error {
	fileObj, err := ra.getFileObj()
	if err != nil {
		return err
	}

	// Try writing version-backed sequential buffer when forced or when file already has data
	if force || (fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID && fileObj.Size >= 0) {
		if err := ra.initSequentialBufferWithWritingVersion(fileObj); err == nil {
			return nil
		} else if force {
			// Forced mode should not silently fall back unless file has no data
			return err
		} else if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
			// Cannot fall back to legacy path when file already has data
			return err
		}
	}

	// Legacy path: create new data object for sequential buffer (only when file has no data)
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		return fmt.Errorf("file already has data")
	}
	return ra.initSequentialBufferWithNewData()
}

func (ra *RandomAccessor) initSequentialBufferWithNewData() error {
	// Get file object to check if it's a .tmp file and determine chunk size

	// Determine chunk size based on bucket configuration (fixed)
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
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

func (ra *RandomAccessor) initSequentialBufferWithWritingVersion(fileObj *core.ObjectInfo) error {
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		return fmt.Errorf("writing version sequential mode requires LocalHandler")
	}

	// Determine chunk size based on bucket configuration (fixed)
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Ensure writing version exists
	writingVersion, err := lh.GetOrCreateWritingVersion(ra.fs.c, ra.fs.bktID, ra.fileID)
	if err != nil {
		return err
	}

	dataID := writingVersion.DataID
	if dataID == 0 || dataID == core.EmptyDataID {
		return fmt.Errorf("writing version has invalid DataID")
	}

	// Load DataInfo for writing version
	dataInfo, err := ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, dataID)
	if err != nil || dataInfo == nil {
		dataInfo = &core.DataInfo{
			ID:       dataID,
			OrigSize: writingVersion.Size,
			Size:     writingVersion.Size,
			Kind:     core.DATA_NORMAL,
		}
	}

	currentSize := writingVersion.Size
	buffer := make([]byte, 0, chunkSize)
	sn := int(currentSize / chunkSize)
	remainder := currentSize % chunkSize

	if remainder > 0 {
		chunkData, readErr := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, dataID, sn)
		if readErr == nil && len(chunkData) >= int(remainder) {
			buffer = append(buffer, chunkData[:remainder]...)
		} else {
			// If reading existing chunk fails, reset buffer to empty to avoid corruption
			buffer = make([]byte, 0, chunkSize)
			remainder = 0
		}
	} else if currentSize > 0 {
		// Start next chunk
		buffer = make([]byte, 0, chunkSize)
	} else {
		sn = 0
	}

	ra.seqBuffer = &SequentialWriteBuffer{
		fileID:           ra.fileID,
		dataID:           dataID,
		sn:               sn,
		chunkSize:        chunkSize,
		buffer:           buffer,
		offset:           currentSize,
		hasData:          currentSize > 0,
		closed:           false,
		dataInfo:         dataInfo,
		writingVersionID: writingVersion.ID,
	}

	return nil
}

// abs returns absolute value of int64
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
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
	DebugLog("[VFS flushSequentialChunk] Original chunk: fileID=%d, chunkSize=%d, OrigSize=%d, Kind=0x%x (CMPR=%v, ENDEC=%v)",
		ra.fileID, len(chunkData), ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Kind,
		ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0,
		ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0)

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
	originalChunkSize := len(chunkData)
	processedChunkSize := len(processedChunk)
	encodedChunkSize := len(encodedChunk)

	ra.seqBuffer.dataInfo.Size += int64(encodedChunkSize)
	DebugLog("[VFS flushSequentialChunk] Chunk sizes: fileID=%d, original=%d, processed=%d, encoded=%d, Kind=0x%x",
		ra.fileID, originalChunkSize, processedChunkSize, encodedChunkSize, ra.seqBuffer.dataInfo.Kind)

	if ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0 || ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
		ra.seqBuffer.dataInfo.Size += int64(len(encodedChunk))
		DebugLog("[VFS flushSequentialChunk] Updated Size (compressed/encrypted): fileID=%d, Size=%d, OrigSize=%d",
			ra.fileID, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.dataInfo.OrigSize)
	} else {
		DebugLog("[VFS flushSequentialChunk] No compression/encryption, Size not updated: fileID=%d, Size=%d, OrigSize=%d",
			ra.fileID, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.dataInfo.OrigSize)
	}

	// Write data block
	DebugLog("[VFS flushSequentialChunk] Writing chunk: fileID=%d, dataID=%d, sn=%d, size=%d", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(encodedChunk))
	if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, ra.seqBuffer.dataID, ra.seqBuffer.sn, encodedChunk); err != nil {
		DebugLog("[VFS flushSequentialChunk] ERROR: Failed to put data for fileID=%d, dataID=%d, sn=%d, size=%d: %v", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(encodedChunk), err)
		if err == core.ERR_QUOTA_EXCEED {
			DebugLog("[VFS flushSequentialChunk] ERROR: Quota exceeded for fileID=%d", ra.fileID)
		}
		return err
	}
	DebugLog("[VFS flushSequentialChunk] Successfully wrote chunk: fileID=%d, dataID=%d, sn=%d, size=%d", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(encodedChunk))

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
					DebugLog("[VFS flushSequentialBuffer] Successfully flushed all pending writes")
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
	DebugLog("[VFS flushSequentialBuffer] Final DataInfo before update: fileID=%d, OrigSize=%d, Size=%d, Kind=0x%x (CMPR=%v, ENDEC=%v)",
		ra.fileID, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.dataInfo.Kind,
		ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0,
		ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0)

	if ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK == 0 && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		ra.seqBuffer.dataInfo.Size = ra.seqBuffer.dataInfo.OrigSize
		ra.seqBuffer.dataInfo.Cksum = ra.seqBuffer.dataInfo.CRC32
		DebugLog("[VFS flushSequentialBuffer] No compression/encryption, set Size=OrigSize: fileID=%d, Size=%d",
			ra.fileID, ra.seqBuffer.dataInfo.Size)
	} else {
		DebugLog("[VFS flushSequentialBuffer] Has compression/encryption, Size already set: fileID=%d, OrigSize=%d, Size=%d",
			ra.fileID, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size)
	}

	// Get file object first to prepare for combined write
	fileObj, err := ra.getFileObj()
	if err != nil {
		return err
	}

	fileObj.DataID = ra.seqBuffer.dataID
	fileObj.Size = ra.seqBuffer.dataInfo.OrigSize

	// Save chunk size to file object's Extra field only if it differs from bucket's default
	ra.setChunkSizeIfDifferent(fileObj, ra.seqBuffer.chunkSize)

	// Optimization: Use PutDataInfoAndObj to write DataInfo and ObjectInfo together in a single transaction
	// This reduces database round trips and improves performance
	DebugLog("[VFS flushSequentialBuffer] Writing DataInfo and ObjectInfo to disk: fileID=%d, dataID=%d, size=%d, chunkSize=%d", ra.fileID, ra.seqBuffer.dataID, fileObj.Size, ra.seqBuffer.chunkSize)
	err = ra.fs.h.PutDataInfoAndObj(ra.fs.c, ra.fs.bktID, []*core.DataInfo{ra.seqBuffer.dataInfo}, []*core.ObjectInfo{fileObj})
	if err != nil {
		DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to write DataInfo and ObjectInfo to disk: fileID=%d, dataID=%d, error=%v", ra.fileID, ra.seqBuffer.dataID, err)
		return err
	}
	DebugLog("[VFS flushSequentialBuffer] Successfully wrote DataInfo and ObjectInfo to disk: fileID=%d, dataID=%d, size=%d", ra.fileID, ra.seqBuffer.dataID, fileObj.Size)

	// Update caches
	dataInfoCache.Put(formatCacheKey(ra.seqBuffer.dataID), ra.seqBuffer.dataInfo)
	fileObjCache.Put(ra.fileObjKey, fileObj)
	ra.fileObj.Store(fileObj)

	DebugLog("[VFS flushSequentialBuffer] Successfully flushed sequential buffer: fileID=%d, dataID=%d, size=%d", ra.fileID, fileObj.DataID, fileObj.Size)
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
	dataInfoCacheKey := formatCacheKey(fileObj.DataID)
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

	// Get chunk size from file object, fallback to bucket's chunk size
	chunkSize := core.GetChunkSizeFromObject(fileObj)
	if chunkSize <= 0 {
		chunkSize = ra.fs.chunkSize
		if chunkSize <= 0 {
			chunkSize = 10 << 20 // Default 10MB
		}
	}

	// Create data reader (abstract read interface, unified handling of uncompressed and compressed/encrypted data)
	var reader dataReader
	if !hasCompression && !hasEncryption {
		// Uncompressed unencrypted: directly read by chunk
		reader = newPlainDataReader(ra.fs.c, ra.fs.h, ra.fs.bktID, fileObj.DataID, chunkSize)
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
// For .tmp files, this method does NOT flush immediately, but schedules a delayed flush after 5 minutes
// Use ForceFlush() to force immediate flush (e.g., when renaming .tmp file)
func (ra *RandomAccessor) Flush() (int64, error) {
	return ra.flushInternal(false)
}

// ForceFlush forces immediate flush, even for .tmp files
// This is used when .tmp file is renamed (removing .tmp extension)
func (ra *RandomAccessor) ForceFlush() (int64, error) {
	return ra.flushInternal(true)
}

// flushInternal is the internal flush implementation
// force: if true, flush immediately even for .tmp files; if false, schedule delayed flush for .tmp files
func (ra *RandomAccessor) flushInternal(force bool) (int64, error) {
	DebugLog("[VFS RandomAccessor Flush] Starting flush: fileID=%d, force=%v", ra.fileID, force)

	if force {
		if err := ra.flushTempFileWriter(); err != nil {
			return 0, err
		}
	}

	// Check if this is a .tmp file (only if not forcing)
	if !force {
		fileObj, err := ra.getFileObj()
		if err == nil && isTempFile(fileObj) {
			// For .tmp files, schedule delayed flush after 5 minutes instead of immediate flush
			DebugLog("[VFS RandomAccessor Flush] .tmp file detected, scheduling delayed flush after 5 minutes: fileID=%d", ra.fileID)
			ra.scheduleDelayedFlush(5 * time.Minute)
			return 0, nil // Return success but don't actually flush
		}
	}

	// If sequential write buffer has data, flush it first
	if ra.seqBuffer != nil && ra.seqBuffer.hasData && !ra.seqBuffer.closed {
		DebugLog("[VFS RandomAccessor Flush] Flushing sequential buffer: fileID=%d, dataID=%d, sn=%d, bufferSize=%d", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(ra.seqBuffer.buffer))
		if err := ra.flushSequentialBuffer(); err != nil {
			DebugLog("[VFS RandomAccessor Flush] ERROR: Failed to flush sequential buffer: fileID=%d, error=%v", ra.fileID, err)
			return 0, err
		}
		// After sequential write completes, close sequential buffer
		ra.seqBuffer.closed = true
		// After sequential write completes, return new version ID (actually the version corresponding to current DataID)
		fileObj, err := ra.getFileObj()
		if err != nil {
			DebugLog("[VFS RandomAccessor Flush] ERROR: Failed to get file object after sequential flush: fileID=%d, error=%v", ra.fileID, err)
			return 0, err
		}
		DebugLog("[VFS RandomAccessor Flush] Sequential flush completed: fileID=%d, dataID=%d, size=%d", ra.fileID, fileObj.DataID, fileObj.Size)
		if fileObj.DataID > 0 {
			return core.NewID(), nil // Return new version ID
		}
	}

	// Optimization: use atomic operation to get and clear operations (lock-free)
	// Atomically swap writeIndex and reset to 0, get actual operation count
	writeIndex := atomic.SwapInt64(&ra.buffer.writeIndex, 0)
	totalSize := atomic.SwapInt64(&ra.buffer.totalSize, 0)
	DebugLog("[VFS RandomAccessor Flush] Buffer stats: fileID=%d, writeIndex=%d, totalSize=%d", ra.fileID, writeIndex, totalSize)
	if writeIndex <= 0 {
		DebugLog("[VFS RandomAccessor Flush] No pending writes: fileID=%d", ra.fileID)
		return 0, nil
	}

	// Reset lastOffset after flush to allow detection of new sequential write pattern
	ra.lastOffset.Store(-1)

	// Check if it's a small file, suitable for batch write
	if totalSize > 0 && totalSize < 1<<20 { // 1MB threshold
		// Small file, merge all write operations then add to batch write manager
		// Get current fileObj BEFORE swapping writeIndex (to read existing data if needed)
		fileObj, err := ra.getFileObj()
		if err != nil {
			return 0, err
		}

		// Check if file has existing data in database (not just in local cache)
		// If file was added to BatchWriter before, local cache may have DataID but database doesn't
		// First check if file is in BatchWriter's pending objects (not yet flushed to database)
		batchMgr := ra.fs.getBatchWriteManager()
		isPendingInBatchWriter := false
		if batchMgr != nil {
			_, isPendingInBatchWriter = batchMgr.GetPendingObject(ra.fileID)
		}

		// If file is in BatchWriter's pending objects, it's not in database yet
		// We should still use BatchWriter for this file (it will be merged with other pending files)
		hasExistingDataInDB := false
		if !isPendingInBatchWriter && fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID && fileObj.Size > 0 {
			// File is not in BatchWriter, check if DataID exists in database
			_, err := ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
			if err == nil {
				// DataID exists in database, file has existing data
				hasExistingDataInDB = true
			}
		}

		// If file has existing data in database, don't use BatchWriter (fallback to normal write)
		// BatchWriter is optimized for new files or complete overwrites
		// For incremental updates, normal write path handles merging better
		if hasExistingDataInDB {
			// File has existing data in database, skip BatchWriter and use normal write path
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
				// Check if file is already in BatchWriter's pending objects
				// If so, don't add again, just wait for BatchWriter to auto-flush
				if isPendingInBatchWriter {
					// File is already in BatchWriter, just reset buffer and return
					// BatchWriter will automatically flush when time window expires or buffer is full
					DebugLog("[VFS RandomAccessor Flush] File already in BatchWriter pending objects, waiting for auto-flush: fileID=%d", ra.fileID)
					atomic.StoreInt64(&ra.buffer.totalSize, 0)
					return core.NewID(), nil
				}

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
						// Buffer full, need to flush before retry
						// Only flush if buffer is actually full (not just time window)
						batchMgr.FlushAll(ra.fs.c)
						DebugLog("[VFS RandomAccessor Flush] Buffer full, flushed all pending writes before retry")
						// Retry add after flush
						added, _, err = addFileToBatchWrite(ra, mergedData)
						if !added {
							// Still failed after retry, fallback to normal write
							DebugLog("[VFS RandomAccessor Flush] Batch write still failed after flush, using normal write path")
						}
					}

					if added {
						// Successfully added to batch write manager
						// Do NOT immediately flush - let batch write manager handle flushing based on its time window
						// This allows multiple files to be batched together for better performance
						// Batch write manager will automatically flush when:
						// 1. Buffer is full
						// 2. Time window expires (default 5 seconds)
						// 3. Explicit FlushAll is called elsewhere
						DebugLog("[VFS RandomAccessor Flush] Successfully added to batch write manager: fileID=%d", ra.fileID)
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

	// Optimization: use writing version to directly modify data blocks for all file types
	// This avoids creating new versions when buffer is full and data is still being written
	// This allows continuous writing on the same version, only updating size information
	// Note: Writing versions use uncompressed/unencrypted data for better performance
	// If compression/encryption is needed, it will be processed when the file is completed

	// Get DataInfo to check compression/encryption status
	var oldDataInfo *core.DataInfo
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		dataInfoCacheKey := formatCacheKey(oldDataID)
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
	}

	// For all files, always use writing version path to allow continuous writing
	// This avoids creating new versions when buffer is full, allowing data to be appended
	// to the existing version by only updating the size information
	return ra.applyWritesWithWritingVersion(fileObj, mergedOps, oldDataInfo)
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
	// Find the maximum end position of all write operations
	// If any write starts at offset 0, the file is being overwritten from the beginning,
	// so the new size should be the maximum end position (even if it's shorter than original)
	// Otherwise, the new size should be the maximum of original size and maximum end position
	newSize := fileObj.Size
	maxEnd := int64(0)
	hasWriteFromZero := false
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > maxEnd {
			maxEnd = writeEnd
		}
		if write.Offset == 0 {
			hasWriteFromZero = true
		}
	}

	if hasWriteFromZero {
		// File is being overwritten from the beginning, use maxEnd (even if shorter than original)
		newSize = maxEnd
		DebugLog("[VFS applyRandomWritesWithSDK] File overwritten from beginning: fileID=%d, oldSize=%d, newSize=%d", ra.fileID, fileObj.Size, newSize)
	} else if maxEnd > fileObj.Size {
		// File is being extended, use maxEnd
		newSize = maxEnd
		DebugLog("[VFS applyRandomWritesWithSDK] File extended: fileID=%d, oldSize=%d, newSize=%d", ra.fileID, fileObj.Size, newSize)
	} else {
		// File size doesn't change (writes are within existing file)
		DebugLog("[VFS applyRandomWritesWithSDK] File size unchanged: fileID=%d, size=%d", ra.fileID, fileObj.Size)
	}

	// Get chunk size
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}
	chunkSizeInt := int(chunkSize)

	// Check if original data is compressed or encrypted (optimized: use cache)
	var oldDataInfo *core.DataInfo
	var hasCompression, hasEncryption bool
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// Optimization: use more efficient key generation (function internally uses object pool)
		dataInfoCacheKey := formatCacheKey(oldDataID)

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
		DebugLog("[VFS applyRandomWritesWithSDK] Processing compressed/encrypted data for fileID=%d, newDataID=%d", ra.fileID, newDataID)
		newVersionID, err := ra.applyWritesStreamingCompressed(oldDataInfo, writes, dataInfo, chunkSize, newSize)
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to apply writes (compressed/encrypted) for fileID=%d: %v", ra.fileID, err)
			if err == core.ERR_QUOTA_EXCEED {
				DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Quota exceeded for fileID=%d", ra.fileID)
			}
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
			DebugLog("[VFS applyRandomWritesWithSDK] Successfully applied writes (compressed/encrypted): fileID=%d, versionID=%d, newDataID=%d, size=%d", ra.fileID, newVersionID, newDataID, newSize)
		} else {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update objects: fileID=%d, error=%v", ra.fileID, err)
		}

		return newVersionID, err
	}

	// For uncompressed unencrypted data, can stream read and process by chunk
	newVersionID, err := ra.applyWritesStreamingUncompressed(fileObj, oldDataInfo, writes, dataInfo, chunkSizeInt, newSize)
	if err != nil {
		return 0, err
	}

	// Get chunk size from file object or use current chunkSizeInt (chunkSize already declared above)
	if chunkSizeInt > 0 {
		chunkSize = int64(chunkSizeInt)
	} else {
		chunkSizeFromObj := core.GetChunkSizeFromObject(fileObj)
		if chunkSizeFromObj > 0 {
			chunkSize = chunkSizeFromObj
		} else {
			chunkSize = ra.fs.chunkSize
			if chunkSize <= 0 {
				chunkSize = 10 << 20 // Default 10MB
			}
		}
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
	// Save chunk size to version object's Extra field only if it differs from bucket's default
	ra.setChunkSizeIfDifferent(newVersion, chunkSize)

	// Optimization: batch write metadata (write version object and file object update together)
	objectsToPut := []*core.ObjectInfo{newVersion}
	// Also update file object (if file object itself needs update)
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		DataID: newDataID,
		Size:   newSize,
	}
	// Save chunk size to file object's Extra field only if it differs from bucket's default
	ra.setChunkSizeIfDifferent(updateFileObj, chunkSize)
	objectsToPut = append(objectsToPut, updateFileObj)

	// Use Put method to batch create version and update file object (will automatically apply version retention policy)
	_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)

	// Update cached file object information
	if err == nil {
		// Optimization: use pre-computed key (avoid repeated conversion)
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
		DebugLog("[VFS applyRandomWritesWithSDK] Successfully applied writes (uncompressed/unencrypted): fileID=%d, versionID=%d, newDataID=%d, size=%d", ra.fileID, newVersionID, newDataID, newSize)
	} else {
		DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update objects: fileID=%d, error=%v", ra.fileID, err)
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
		chunkSize = 10 << 20 // Default 10MB
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
		DebugLog("[VFS applyWritesWithWritingVersion] Updating data chunk on disk: fileID=%d, dataID=%d, sn=%d, offset=%d, size=%d", ra.fileID, dataID, update.sn, update.offset, len(update.data))
		updateErr := lh.UpdateData(ra.fs.c, ra.fs.bktID, dataID, update.sn, update.offset, update.data)
		if updateErr != nil {
			DebugLog("[VFS applyWritesWithWritingVersion] ERROR: Failed to update data chunk on disk: fileID=%d, dataID=%d, sn=%d, error=%v", ra.fileID, dataID, update.sn, updateErr)
			return 0, fmt.Errorf("failed to update data chunk %d: %v", update.sn, updateErr)
		}
		DebugLog("[VFS applyWritesWithWritingVersion] Successfully updated data chunk on disk: fileID=%d, dataID=%d, sn=%d, offset=%d, size=%d", ra.fileID, dataID, update.sn, update.offset, len(update.data))
	}

	// Update file object size if needed
	// Calculate the maximum end position of all write operations
	newSize := fileObj.Size
	maxEnd := int64(0)
	hasWriteFromZero := false
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > maxEnd {
			maxEnd = writeEnd
		}
		if write.Offset == 0 {
			hasWriteFromZero = true
		}
	}

	if hasWriteFromZero {
		// File is being overwritten from the beginning, use maxEnd (even if shorter than original)
		newSize = maxEnd
		DebugLog("[VFS applyWritesWithWritingVersion] File overwritten from beginning: fileID=%d, oldSize=%d, newSize=%d", ra.fileID, fileObj.Size, newSize)
	} else if maxEnd > fileObj.Size {
		// File is being extended, use maxEnd
		newSize = maxEnd
		DebugLog("[VFS applyWritesWithWritingVersion] File extended: fileID=%d, oldSize=%d, newSize=%d", ra.fileID, fileObj.Size, newSize)
	} else {
		// File size doesn't change (writes are within existing file)
		DebugLog("[VFS applyWritesWithWritingVersion] File size unchanged: fileID=%d, size=%d", ra.fileID, fileObj.Size)
	}

	// Update writing version and file object if size changed
	if newSize != fileObj.Size {
		// Get current DataInfo to update its size
		var dataInfo *core.DataInfo
		dataInfoCacheKey := formatCacheKey(dataID)
		if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
			if info, ok := cached.(*core.DataInfo); ok && info != nil {
				dataInfo = info
			}
		}
		if dataInfo == nil {
			var err error
			dataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, dataID)
			if err == nil && dataInfo != nil {
				dataInfoCache.Put(dataInfoCacheKey, dataInfo)
			}
		}

		// Update DataInfo size if it exists (for writing versions, Size should equal OrigSize)
		if dataInfo != nil {
			dataInfo.OrigSize = newSize
			dataInfo.Size = newSize // For uncompressed/unencrypted data, Size equals OrigSize
			_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
			if err != nil {
				DebugLog("[VFS applyWritesWithWritingVersion] ERROR: Failed to update DataInfo: fileID=%d, dataID=%d, error=%v", ra.fileID, dataID, err)
				// Continue with file object update even if DataInfo update fails
			} else {
				// Update cache
				dataInfoCache.Put(dataInfoCacheKey, dataInfo)
				DebugLog("[VFS applyWritesWithWritingVersion] Successfully updated DataInfo: fileID=%d, dataID=%d, size=%d", ra.fileID, dataID, newSize)
			}
		}

		// Update writing version object size
		updateVersionObj := &core.ObjectInfo{
			ID:     writingVersion.ID,
			DataID: dataID,
			Size:   newSize,
			MTime:  core.Now(),
		}
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateVersionObj})
		if err != nil {
			DebugLog("[VFS applyWritesWithWritingVersion] ERROR: Failed to update writing version: fileID=%d, versionID=%d, error=%v", ra.fileID, writingVersion.ID, err)
			// Continue with file object update
		} else {
			DebugLog("[VFS applyWritesWithWritingVersion] Successfully updated writing version: fileID=%d, versionID=%d, size=%d", ra.fileID, writingVersion.ID, newSize)
		}

		// Update file object
		updateFileObj := &core.ObjectInfo{
			ID:     ra.fileID,
			DataID: dataID,
			Size:   newSize,
			MTime:  core.Now(),
		}
		// Get chunk size from file object or use default
		chunkSize := core.GetChunkSizeFromObject(fileObj)
		if chunkSize <= 0 {
			chunkSize = ra.fs.chunkSize
			if chunkSize <= 0 {
				chunkSize = 10 << 20 // Default 10MB
			}
		}
		// Save chunk size to file object's Extra field only if it differs from bucket's default
		ra.setChunkSizeIfDifferent(updateFileObj, chunkSize)
		DebugLog("[VFS applyWritesWithWritingVersion] Writing file object to disk: fileID=%d, dataID=%d, size=%d, chunkSize=%d", ra.fileID, dataID, newSize, chunkSize)
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			DebugLog("[VFS applyWritesWithWritingVersion] ERROR: Failed to write file object to disk: fileID=%d, error=%v", ra.fileID, err)
			return 0, fmt.Errorf("failed to update file object: %v", err)
		}
		DebugLog("[VFS applyWritesWithWritingVersion] Successfully wrote file object to disk: fileID=%d, dataID=%d, size=%d", ra.fileID, dataID, newSize)

		// Update cache
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
		DebugLog("[VFS applyWritesWithWritingVersion] Successfully updated file: fileID=%d, dataID=%d, size=%d", ra.fileID, dataID, newSize)
	} else {
		DebugLog("[VFS applyWritesWithWritingVersion] No size change: fileID=%d, dataID=%d, size=%d", ra.fileID, dataID, newSize)
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
		chunkSizeInt = 10 << 20 // Default 10MB
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

			DebugLog("[VFS applyWritesStreamingUncompressed] Writing data chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d", ra.fileID, dataInfo.ID, sn, len(chunkDataCopy))
			if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, chunkDataCopy); err != nil {
				DebugLog("[VFS applyWritesStreamingUncompressed] ERROR: Failed to write data chunk to disk: fileID=%d, dataID=%d, sn=%d, error=%v", ra.fileID, dataInfo.ID, sn, err)
				return 0, err
			}
			DebugLog("[VFS applyWritesStreamingUncompressed] Successfully wrote data chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d", ra.fileID, dataInfo.ID, sn, len(chunkDataCopy))
			sn++
		}

		// Save data metadata
		DebugLog("[VFS applyWritesStreamingUncompressed] Writing DataInfo to disk: fileID=%d, dataID=%d, OrigSize=%d, Size=%d", ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size)
		_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
		if err != nil {
			DebugLog("[VFS applyWritesStreamingUncompressed] ERROR: Failed to write DataInfo to disk: fileID=%d, dataID=%d, error=%v", ra.fileID, dataInfo.ID, err)
			return 0, err
		}
		DebugLog("[VFS applyWritesStreamingUncompressed] Successfully wrote DataInfo to disk: fileID=%d, dataID=%d, OrigSize=%d, Size=%d", ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size)

		// Optimization: use more efficient key generation (function internally uses object pool)
		dataInfoCache.Put(formatCacheKey(dataInfo.ID), dataInfo)

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
		DebugLog("[VFS applyWritesStreamingCompressed] Writing encoded chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d", ra.fileID, dataInfo.ID, sn, len(encodedChunkCopy))
		if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, encodedChunkCopy); err != nil {
			DebugLog("[VFS applyWritesStreamingCompressed] ERROR: Failed to write encoded chunk to disk: fileID=%d, dataID=%d, sn=%d, error=%v", ra.fileID, dataInfo.ID, sn, err)
			chunkDataPool.Put(chunkData[:0])
			return 0, err
		}
		DebugLog("[VFS applyWritesStreamingCompressed] Successfully wrote encoded chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d", ra.fileID, dataInfo.ID, sn, len(encodedChunkCopy))
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
	DebugLog("[VFS applyWritesStreamingCompressed] Writing DataInfo to disk: fileID=%d, dataID=%d, OrigSize=%d, Size=%d", ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size)
	_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		DebugLog("[VFS applyWritesStreamingCompressed] ERROR: Failed to write DataInfo to disk: fileID=%d, dataID=%d, error=%v", ra.fileID, dataInfo.ID, err)
		return 0, err
	}
	DebugLog("[VFS applyWritesStreamingCompressed] Successfully wrote DataInfo to disk: fileID=%d, dataID=%d, OrigSize=%d, Size=%d", ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size)

	// Update cache
	dataInfoCache.Put(formatCacheKey(dataInfo.ID), dataInfo)

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
		chunkSize = 10 << 20 // Default 10MB
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
	origSize   int64 // Original data size (decompressed size)
}

func newDecodingChunkReader(c core.Ctx, h core.Handler, bktID int64, dataInfo *core.DataInfo, endecKey string) *decodingChunkReader {
	dr := &decodingChunkReader{
		c:        c,
		h:        h,
		bktID:    bktID,
		kind:     dataInfo.Kind,
		endecKey: endecKey,
		origSize: dataInfo.OrigSize, // Store original size (decompressed size)
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
	// Calculate remaining data based on current position and original size
	remain := dr.origSize - dr.currentPos
	for len(p) > 0 && remain > 0 {
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
			// Don't decrease remain here - we'll decrease it after decompression based on actual decompressed size
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

		// Update remain based on current position
		remain = dr.origSize - dr.currentPos
	}

	if remain <= 0 && totalRead == 0 {
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
						oldDataInfoCacheKey := formatCacheKey(oldDataID)
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
							oldDataInfoCacheKey := formatCacheKey(oldDataID)
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
		DebugLog("[VFS applyRandomWritesWithSDK] Writing DataInfo and ObjectInfo to disk: fileID=%d, newDataID=%d, newSize=%d", ra.fileID, newDataID, newSize)
		err = lh.PutDataInfoAndObj(ra.fs.c, ra.fs.bktID, dataInfos, objectsToPut)
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to write DataInfo and ObjectInfo to disk: fileID=%d, newDataID=%d, error=%v", ra.fileID, newDataID, err)
			return 0, fmt.Errorf("failed to save DataInfo and update objects: %v", err)
		}
		DebugLog("[VFS applyRandomWritesWithSDK] Successfully wrote DataInfo and ObjectInfo to disk: fileID=%d, newDataID=%d, newSize=%d", ra.fileID, newDataID, newSize)
		// Update cache
		dataInfoCache.Put(formatCacheKey(newDataID), newDataInfo)
	} else {
		// Only write ObjectInfo (no DataInfo to write)
		DebugLog("[VFS applyRandomWritesWithSDK] Writing ObjectInfo to disk (no DataInfo): fileID=%d, newSize=%d", ra.fileID, newSize)
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to write ObjectInfo to disk: fileID=%d, error=%v", ra.fileID, err)
			return 0, fmt.Errorf("failed to update file object: %v", err)
		}
		DebugLog("[VFS applyRandomWritesWithSDK] Successfully wrote ObjectInfo to disk: fileID=%d, newSize=%d", ra.fileID, newSize)
	}

	// Update cache
	fileObjCache.Put(ra.fileObjKey, updateFileObj)
	ra.fileObj.Store(updateFileObj)

	// Invalidate old DataInfo cache if DataID changed
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID && fileObj.DataID != newDataID {
		oldDataInfoCacheKey := formatCacheKey(fileObj.DataID)
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

	return nil
}

// flushTempFileWriter flushes the TempFileWriter when forcing a flush (e.g. before rename)
func (ra *RandomAccessor) flushTempFileWriter() error {
	ra.tempWriterMu.Lock()
	tw := ra.tempWriter
	ra.tempWriterMu.Unlock()

	if tw == nil {
		return nil
	}

	DebugLog("[VFS RandomAccessor] Forcing TempFileWriter flush: fileID=%d, dataID=%d", tw.fileID, tw.dataID)
	if err := tw.Flush(); err != nil {
		DebugLog("[VFS RandomAccessor] ERROR: TempFileWriter flush failed: fileID=%d, dataID=%d, error=%v", tw.fileID, tw.dataID, err)
		return err
	}
	return nil
}

// hasTempFileWriter reports whether this RandomAccessor currently owns a TempFileWriter
func (ra *RandomAccessor) hasTempFileWriter() bool {
	ra.tempWriterMu.Lock()
	defer ra.tempWriterMu.Unlock()
	return ra.tempWriter != nil
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

// tryInstantUpload attempts instant upload by calculating checksums and calling Ref
// Returns DataID if instant upload succeeds (> 0), 0 if it fails
func tryInstantUpload(fs *OrcasFS, data []byte, origSize int64, kind uint32) (int64, error) {
	// Calculate checksums using SDK
	hdrCRC32, crc32Val, md5Val, err := sdk.CalculateChecksums(data)
	if err != nil {
		return 0, err
	}

	// Create DataInfo for Ref
	dataInfo := &core.DataInfo{
		OrigSize: origSize,
		HdrCRC32: hdrCRC32,
		CRC32:    crc32Val,
		MD5:      md5Val,
		Kind:     kind,
	}

	// Call Ref to check if data already exists
	refIDs, err := fs.h.Ref(fs.c, fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return 0, err
	}

	if len(refIDs) > 0 && refIDs[0] != 0 {
		if refIDs[0] > 0 {
			// Instant upload succeeded, return existing DataID from database
			return refIDs[0], nil
		} else {
			// Negative ID means reference to another element in current batch
			// This should not happen in VFS (single file write)
			// But we handle it for completeness: skip instant upload, return 0
			// The negative reference will be resolved in PutDataInfo
			return 0, nil
		}
	}

	// Instant upload failed, return 0
	return 0, nil
}
