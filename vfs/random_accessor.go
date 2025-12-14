package vfs

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h2non/filetype"
	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/orca-zhang/ecache2"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
	"github.com/tjfoc/gmsm/sm4"
	"golang.org/x/sync/singleflight"
)

var (
	// Object pool: reuse byte buffers to reduce memory allocation
	// Optimization: use smaller initial capacity for small file operations
	chunkDataPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 64<<10) // Pre-allocate 64KB capacity (reduced from 4MB for small files)
		},
	}

	// Object pool: reuse write operation slices
	writeOpsPool = sync.Pool{
		New: func() interface{} {
			return make([]WriteOperation, 0, 32)
		},
	}

	// Object pool for large buffers (used when small pool buffer is insufficient)
	largeChunkDataPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4<<20) // Pre-allocate 4MB capacity for large files
		},
	}

	// Object pool for int slices (used for remainingChunks, etc.)
	intSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]int, 0, 32) // Pre-allocate capacity for chunk lists
		},
	}

	// Object pool for chunk buffers (reuse large chunk buffers to reduce allocations)
	chunkBufferPool = sync.Pool{
		New: func() interface{} {
			return &chunkBuffer{
				data:          make([]byte, 0, 10<<20), // Pre-allocate 10MB capacity
				offsetInChunk: 0,
			}
		},
	}

	// ecache cache: cache DataInfo to reduce database queries
	// key: dataID (int64), value: *core.DataInfo (dataID is globally unique)
	dataInfoCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// ecache cache: cache file object information to reduce database queries
	// key: fileID (int64), value: *core.ObjectInfo (fileID is globally unique)
	fileObjCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// ecache cache: cache directory listing to reduce database queries
	// key: dirID (int64), value: []*core.ObjectInfo (dirID is globally unique)
	dirListCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// ecache cache: cache Readdir entries (DirStream) to avoid rebuilding entries every time
	// key: dirID (int64), value: *cachedDirStream (dirID is globally unique)
	// This cache stores the final DirStream entries, avoiding data merging on every Readdir call
	readdirCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// Map to track directories that need delayed cache refresh
	// key: "<dirID>", value: true (if true, cache needs refresh on next access)
	// This allows marking cache as stale without immediately deleting it
	readdirCacheStale sync.Map // map[int64]bool

	// ecache cache: cache chunkReader by dataID (one reader per file)
	// key: dataID (int64), value: *chunkReader
	// This ensures one file uses the same reader, sharing chunk cache
	decodingReaderCache = ecache2.NewLRUCache[int64](4, 64, 5*time.Minute)

	// ecache cache: cache bucket configuration to reduce database queries
	// key: bktID (int64), value: *core.BucketInfo (bktID is globally unique)
	bucketConfigCache = ecache2.NewLRUCache[int64](4, 64, 5*time.Minute)

	// singleflight group: prevent duplicate concurrent requests for the same directory
	// key: "<dirID>", ensures only one request per directory at a time
	dirListSingleFlight singleflight.Group

	// singleflight group: prevent duplicate concurrent chunk reads
	// key: "chunk_<dataID>_<sn>", ensures only one read per chunk at a time
	chunkReadSingleFlight singleflight.Group

	// singleflight group: prevent duplicate concurrent chunk flushes
	// key: "flush_<dataID>_<sn>", ensures only one flush per chunk at a time
	chunkFlushSingleFlight singleflight.Group

	// singleflight group: prevent duplicate concurrent package file decoding
	// key: "decode_pkg_<dataID>", ensures only one decode per package file at a time
	packageDecodeSingleFlight singleflight.Group

	tempFlushMgr     *delayedFlushManager
	tempFlushMgrOnce sync.Once
)

// getBucketConfigWithCache gets bucket configuration with global cache
// This reduces database queries when multiple RandomAccessors access the same bucket
func getBucketConfigWithCache(fs *OrcasFS) *core.BucketInfo {
	if fs == nil {
		return nil
	}

	// Check cache first
	cacheKey := fs.bktID
	if cached, ok := bucketConfigCache.Get(cacheKey); ok {
		if bucket, ok := cached.(*core.BucketInfo); ok && bucket != nil {
			return bucket
		}
	}

	// Get from OrcasFS (which has its own cache)
	bucket := fs.getBucketConfig()
	if bucket != nil {
		// Cache the result
		bucketConfigCache.Put(cacheKey, bucket)
	}

	return bucket
}

const (
	tmpFileFlushInterval      = 5 * time.Minute
	delayedFlushCheckInterval = time.Second
)

type delayedFlushEntry struct {
	ra    *RandomAccessor
	force bool
}

type delayedFlushManager struct {
	mu      sync.Mutex
	entries map[int64]*delayedFlushEntry
	wakeCh  chan struct{}
}

func newDelayedFlushManager() *delayedFlushManager {
	mgr := &delayedFlushManager{
		entries: make(map[int64]*delayedFlushEntry),
		wakeCh:  make(chan struct{}, 1),
	}
	go mgr.run()
	return mgr
}

func getDelayedFlushManager() *delayedFlushManager {
	tempFlushMgrOnce.Do(func() {
		tempFlushMgr = newDelayedFlushManager()
	})
	return tempFlushMgr
}

func (m *delayedFlushManager) run() {
	ticker := time.NewTicker(delayedFlushCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.process()
		case <-m.wakeCh:
			m.process()
		}
	}
}

func (m *delayedFlushManager) process() {
	var toFlush []*delayedFlushEntry

	// For .tmp files, reset delayed flush timer on each write (5 minutes from last write)
	// Note: This is handled by TempFileWriter for .tmp files, so we don't need to do it here
	// (The .tmp file check happens earlier in the function, so we skip this section for .tmp files)

	// Optimization: for sparse files and sequential writes, use longer delayed flush window to batch more writes
	// This reduces flush frequency and improves performance for random writes
	config := core.GetWriteBufferConfig()

	m.mu.Lock()
	for fileID, entry := range m.entries {
		if entry == nil || entry.ra == nil {
			delete(m.entries, fileID)
			continue
		}
		last := atomic.LoadInt64(&entry.ra.lastActivity)
		if last == 0 {
			continue
		}
		if core.Now()-last > int64(config.BufferWindow) {
			toFlush = append(toFlush, entry)
			delete(m.entries, fileID)
		}
	}
	m.mu.Unlock()

	for _, entry := range toFlush {
		entry.ra.executeDelayedFlush(entry.force)
	}
}

func (m *delayedFlushManager) schedule(ra *RandomAccessor, force bool) {
	if ra == nil {
		return
	}
	m.mu.Lock()
	if entry, ok := m.entries[ra.fileID]; ok {
		entry.force = force
	} else {
		m.entries[ra.fileID] = &delayedFlushEntry{
			ra:    ra,
			force: force,
		}
	}
	m.mu.Unlock()

	select {
	case m.wakeCh <- struct{}{}:
	default:
	}
}

func (m *delayedFlushManager) cancel(ra *RandomAccessor) {
	if ra == nil {
		return
	}
	m.mu.Lock()
	delete(m.entries, ra.fileID)
	m.mu.Unlock()
}

// getBatchWriteManager gets the batch writer for the specified bucket (thread-safe)
// Returns nil if batch write is disabled
// Uses SDK's global batch writer registry
// Sets up callback to update dirListCache when objects are flushed
// Sets bucket configuration for package-level compression/encryption
func (fs *OrcasFS) getBatchWriteManager() *sdk.BatchWriter {
	batchMgr := sdk.GetBatchWriterForBucket(fs.h, fs.bktID)
	if batchMgr != nil {
		// Get bucket configuration and set it on BatchWriter
		// This enables package-level compression/encryption
		bucket := getBucketConfigWithCache(fs)
		if bucket != nil {
			batchMgr.SetBucketConfig(bucket.CmprWay, bucket.CmprQlty, bucket.EndecWay, bucket.EndecKey)
		}

		// Set callback to update dirListCache after flush completes
		// This ensures flushed objects are immediately added to dirListCache
		// instead of requiring getDirListWithCache to merge from pendingObjects
		batchMgr.SetOnFlushComplete(func(objectInfos []*core.ObjectInfo) {
			if len(objectInfos) == 0 {
				return
			}
			// Group objects by parent directory ID
			objectsByDir := make(map[int64][]*core.ObjectInfo)
			for _, obj := range objectInfos {
				if obj != nil && obj.PID > 0 {
					objectsByDir[obj.PID] = append(objectsByDir[obj.PID], obj)
				}
			}
			// Update dirListCache for each directory
			for dirID, objs := range objectsByDir {
				dirNode := &OrcasNode{
					fs:    fs,
					objID: dirID,
				}
				for _, obj := range objs {
					// Append to dirListCache (will check for duplicates)
					dirNode.appendChildToDirCache(dirID, obj)
					DebugLog("[VFS getBatchWriteManager] Updated dirListCache after flush: dirID=%d, fileID=%d, name=%s, DataID=%d", dirID, obj.ID, obj.Name, obj.DataID)
				}
			}
		})
	}
	return batchMgr
}

// processFileDataForBatchWrite processes file data (compression/encryption) for batch write
// This is a helper function to process data before passing to SDK's BatchWriter
// Returns: (processedData, origSize, kind)
func processFileDataForBatchWrite(fs *OrcasFS, originalData []byte) ([]byte, int64, uint32) {
	origSize := int64(len(originalData))
	if origSize == 0 {
		return originalData, origSize, 0
	}

	// Process compression and encryption according to configuration
	// Get bucket configuration (with cache)
	bucket := getBucketConfigWithCache(fs)
	kind := uint32(0)
	data := originalData
	compressedData := originalData
	var err error

	// 1. Compression (if enabled)
	// CmprWay is now smart compression by default (checks file type)
	cmprWay := uint32(0)
	if bucket != nil && bucket.CmprWay > 0 {
		cmprWay = bucket.CmprWay
	}

	if cmprWay > 0 {
		// Smart compression: check file extension first, then file type
		// This avoids unnecessary file header checks for known non-compressible file types
		needsCompression := true

		// Step 1: Check file extension first (faster than file header check)
		// Note: fileName is not available in this function, so we skip extension check here
		// Extension check is done in decideRealtimeProcessing for TempFileWriter

		// Step 2: Check file header if extension check didn't rule out compression
		if needsCompression {
			detectedKind, _ := filetype.Match(originalData)
			if detectedKind != filetype.Unknown {
				// File type is known (likely already compressed or not compressible), don't compress
				needsCompression = false
			}
		}

		if needsCompression {
			kind |= cmprWay
			var cmpr archiver.Compressor
			if cmprWay&core.DATA_CMPR_SNAPPY != 0 {
				cmpr = &archiver.Snappy{}
			} else if cmprWay&core.DATA_CMPR_ZSTD != 0 {
				cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(bucket.CmprQlty)))}}
			} else if cmprWay&core.DATA_CMPR_GZIP != 0 {
				cmpr = &archiver.Gz{CompressionLevel: int(bucket.CmprQlty)}
			} else if cmprWay&core.DATA_CMPR_BR != 0 {
				cmpr = &archiver.Brotli{Quality: int(bucket.CmprQlty)}
			}

			if cmpr != nil {
				var cmprBuf bytes.Buffer
				err := cmpr.Compress(bytes.NewBuffer(data), &cmprBuf)
				if err == nil && cmprBuf.Len() < len(data) {
					compressedData = cmprBuf.Bytes()
					data = compressedData
				} else {
					// Compression failed or compressed size is not smaller, remove compression flag
					kind &= ^core.DATA_CMPR_MASK
					compressedData = originalData
					data = originalData
				}
			}
		}
		// If needsCompression is false, don't set compression flag, just proceed to encryption
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

	return data, origSize, kind
}

// addFileToBatchWrite adds file data to SDK's batch write manager
// FlushAll is only called when buffer is full or by periodic timer
func addFileToBatchWrite(ra *RandomAccessor, data []byte) (bool, int64, error) {
	if len(data) == 0 {
		return true, 0, nil
	}

	// Try instant upload (deduplication) before processing
	// Check configuration: bucket config > environment variable
	origSize := int64(len(data))

	// Create unified config from bucket config
	var instantUploadCfg *core.InstantUploadConfig
	bucket := getBucketConfigWithCache(ra.fs)
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

	// Process individual file compression/encryption
	// Each file is compressed/encrypted separately, then packaged together
	// This allows direct access to individual files via pkgOffset
	processedData, origSize, kind := processFileDataForBatchWrite(ra.fs, data)

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

	// For .tmp files being renamed to regular files, check if there's an existing .tmp file with the same name
	// that needs to be deleted. This handles the case where a .tmp file is being renamed and added to batch writer.
	// The batch writer will handle the rename during flush, but we need to mark the old .tmp file for deletion.
	if fileObj != nil && pid > 0 {
		// Check if this is a .tmp file being renamed (name doesn't have .tmp suffix but file was originally .tmp)
		// Or if there's a .tmp file with the same name that needs to be deleted
		objNameLower := strings.ToLower(name)
		isTmpFile := strings.HasSuffix(objNameLower, ".tmp")

		if !isTmpFile {
			// File doesn't have .tmp suffix, check if there's a .tmp file with the same name
			tmpFileName := name + ".tmp"
			// Get directory listing to find existing .tmp file
			children, _, _, err := ra.fs.h.List(ra.fs.c, ra.fs.bktID, pid, core.ListOptions{
				Count: core.DefaultListPageSize,
			})
			if err == nil {
				for _, child := range children {
					// Check if there's a .tmp file with the same name (different fileID)
					if child.ID != ra.fileID && strings.ToLower(child.Name) == strings.ToLower(tmpFileName) && child.Type == core.OBJ_TYPE_FILE {
						DebugLog("[VFS addFileToBatchWrite] Found existing .tmp file with same name, will delete during batch flush: fileID=%d, tmpFileID=%d, name=%s", ra.fileID, child.ID, tmpFileName)

						// Remove from batch writer if present
						batchMgr := ra.fs.getBatchWriteManager()
						if batchMgr != nil {
							batchMgr.RemovePendingObject(child.ID)
						}

						// Flush and unregister RandomAccessor if exists
						if ra.fs != nil {
							if childRA := ra.fs.getRandomAccessorByFileID(child.ID); childRA != nil {
								// Force flush before deletion
								if _, flushErr := childRA.ForceFlush(); flushErr != nil {
									DebugLog("[VFS addFileToBatchWrite] WARNING: Failed to flush existing .tmp file before deletion: fileID=%d, error=%v", child.ID, flushErr)
								}
								// Unregister RandomAccessor
								ra.fs.unregisterRandomAccessor(child.ID, childRA)
							}
						}

						// Mark the .tmp file for deletion (will be deleted during batch flush)
						// Store the tmpFileID in a way that batch writer can access it
						// For now, we'll delete it immediately before adding to batch writer
						// This ensures the .tmp file is removed before the new file is added
						deleteErr := ra.fs.h.Delete(ra.fs.c, ra.fs.bktID, child.ID)
						if deleteErr != nil {
							DebugLog("[VFS addFileToBatchWrite] WARNING: Failed to delete existing .tmp file: fileID=%d, name=%s, error=%v", child.ID, tmpFileName, deleteErr)
						} else {
							DebugLog("[VFS addFileToBatchWrite] Successfully deleted existing .tmp file: fileID=%d, name=%s", child.ID, tmpFileName)
							// Remove from cache
							fileObjCache.Del(child.ID)
							// Remove from directory cache
							dirNode := &OrcasNode{
								fs:    ra.fs,
								objID: pid,
							}
							dirNode.removeChildFromDirCache(pid, child.ID)
						}
						break
					}
				}
			}
		}
	}

	// Get batch write manager
	batchMgr := ra.fs.getBatchWriteManager()
	if batchMgr == nil {
		return false, 0, nil
	}
	batchMgr.SetFlushContext(ra.fs.c)

	// Add file to SDK's batch write manager
	added, dataID, err := batchMgr.AddFile(ra.fileID, processedData, pid, name, origSize, kind)
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
	// IMPORTANT: Must include Type, Name, PID, MTime to avoid cache corruption (consistent with direct write path)
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
		// Update directory listing cache to ensure file is visible in Readdir (consistent with direct write path)
		if updateFileObj.PID > 0 {
			dirNode := &OrcasNode{
				fs:    ra.fs,
				objID: updateFileObj.PID,
			}
			dirNode.appendChildToDirCache(updateFileObj.PID, updateFileObj)
			DebugLog("[VFS addFileToBatchWrite] Appended file to directory listing cache: fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
		}
	}

	// Note: FlushAll is only called when buffer is full (AddFile returns false) or by periodic timer
	// Do NOT call FlushAll here to avoid unnecessary flushes

	return true, dataID, nil
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
// TempFileWriter writes data chunks synchronously using fixed 10MB chunk size
type TempFileWriter struct {
	fs              *OrcasFS
	fileID          int64
	dataID          int64                // Data ID for this .tmp file
	fileName        string               // File name (for smart compression suffix check)
	chunkSize       int64                // Chunk size for this file (always 10MB)
	size            int64                // Total file size (atomic for concurrent access)
	mu              sync.Mutex           // Mutex for thread-safe operations on chunks map
	chunks          map[int]*chunkBuffer // Chunk buffers for each chunk (key: sn)
	dataInfo        *core.DataInfo       // DataInfo for tracking compression/encryption
	enableRealtime  bool                 // Whether to enable real-time compression/encryption (default: false for offline processing)
	realtimeDecided bool                 // Whether realtime capability has been determined
	lh              *core.LocalHandler   // Cached LocalHandler (nil if not LocalHandler)
	firstChunkSN    int64                // First chunk sequence number (sn=0)
	lastChunkSN     int64                // Last chunk sequence number (updated when flushing)
}

// writeRange represents a contiguous range of written data within a chunk
type writeRange struct {
	start int64 // Start offset (inclusive)
	end   int64 // End offset (exclusive)
}

// chunkBuffer holds data for a single chunk before compression/encryption and upload
type chunkBuffer struct {
	data          []byte       // Buffer data (pre-allocated to chunkSize)
	offsetInChunk int64        // Current write position within this chunk (0 to chunkSize)
	ranges        []writeRange // Written ranges within this chunk (sorted by start, non-overlapping)
	mu            sync.Mutex   // Mutex for this specific chunk buffer
}

// addWriteRange adds a new write range and merges adjacent ranges
// Ranges are kept sorted by start offset and non-overlapping
func (buf *chunkBuffer) addWriteRange(start, end int64) {
	if start >= end {
		return // Invalid range
	}

	// Find insertion position (ranges are sorted by start)
	insertPos := len(buf.ranges)
	for i, r := range buf.ranges {
		if r.start > start {
			insertPos = i
			break
		}
	}

	// Insert new range
	newRange := writeRange{start: start, end: end}
	if insertPos == len(buf.ranges) {
		buf.ranges = append(buf.ranges, newRange)
	} else {
		// Insert at position
		buf.ranges = append(buf.ranges, writeRange{})
		copy(buf.ranges[insertPos+1:], buf.ranges[insertPos:])
		buf.ranges[insertPos] = newRange
	}

	// Merge adjacent or overlapping ranges
	merged := buf.ranges[:0]
	for _, r := range buf.ranges {
		if len(merged) == 0 {
			merged = append(merged, r)
		} else {
			last := &merged[len(merged)-1]
			// If current range connects with or overlaps last range, merge them
			if r.start <= last.end {
				if r.end > last.end {
					last.end = r.end
				}
			} else {
				merged = append(merged, r)
			}
		}
	}
	buf.ranges = merged
}

// isChunkComplete checks if chunk is complete (has exactly one range covering entire chunk)
func (buf *chunkBuffer) isChunkComplete(chunkSize int64) bool {
	return len(buf.ranges) == 1 && buf.ranges[0].start == 0 && buf.ranges[0].end == chunkSize
}

// RandomAccessor random access object in VFS, supports compression and encryption
type RandomAccessor struct {
	fs           *OrcasFS
	fileID       int64
	buffer       *WriteBuffer           // Random write buffer
	seqBuffer    *SequentialWriteBuffer // Sequential write buffer (optimized)
	fileObj      atomic.Value
	fileObjKey   int64 // Pre-computed file_obj cache key (optimized: avoid repeated conversion)
	lastActivity int64 // Last activity timestamp (atomic access)
	sparseSize   int64 // Sparse file size (for pre-allocated files, e.g., qBittorrent) (atomic access)
	lastOffset   int64 // Last write offset (for sequential write detection) (atomic access)
	seqDetector  *ConcurrentSequentialDetector // Concurrent sequential write detector
	tempWriter   atomic.Value                  // TempFileWriter for .tmp files (atomic.Value stores *TempFileWriter)
}

func isTempFile(obj *core.ObjectInfo) bool {
	if obj == nil {
		return false
	}
	name := strings.ToLower(obj.Name)
	return strings.HasSuffix(name, ".tmp")
}

// getOrCreateTempWriter gets or creates TempFileWriter for .tmp files
// Uses atomic.Value for lock-free reads, double-check pattern for creation
func (ra *RandomAccessor) getOrCreateTempWriter() (*TempFileWriter, error) {
	// Fast path: check if already exists (lock-free read)
	if val := ra.tempWriter.Load(); val != nil {
		if tw, ok := val.(*TempFileWriter); ok && tw != nil {
			return tw, nil
		}
	}

	// Slow path: need to create (use sync.Mutex for creation only)
	// Use fs.raCreateMu to ensure only one TempFileWriter is created per file
	ra.fs.raCreateMu.Lock()
	defer ra.fs.raCreateMu.Unlock()

	// Double-check after acquiring lock
	if val := ra.tempWriter.Load(); val != nil {
		if tw, ok := val.(*TempFileWriter); ok && tw != nil {
			return tw, nil
		}
	}

	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// Use fixed 10MB chunk size (always use 10MB pool buffers)
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Use existing DataID if file already has one, otherwise create new DataID
	// IMPORTANT: This ensures we reuse existing data instead of creating duplicate dataIDs
	var dataID int64
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		// File already has DataID, reuse it
		dataID = fileObj.DataID
		DebugLog("[VFS TempFileWriter Create] Reusing existing DataID: fileID=%d, dataID=%d", ra.fileID, dataID)
	} else {
		// File has no DataID, create new one
		dataID = core.NewID()
		if dataID <= 0 {
			return nil, fmt.Errorf("failed to create DataID for .tmp file")
		}
		DebugLog("[VFS TempFileWriter Create] Created new DataID: fileID=%d, dataID=%d", ra.fileID, dataID)
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

	tw := &TempFileWriter{
		fs:              ra.fs,
		fileID:          ra.fileID,
		dataID:          dataID,
		fileName:        fileObj.Name,
		chunkSize:       chunkSize, // Always use 10MB
		size:            0,
		chunks:          make(map[int]*chunkBuffer),
		dataInfo:        dataInfo,
		enableRealtime:  false,
		realtimeDecided: false,
		lh:              lh,
		firstChunkSN:    0,
		lastChunkSN:     0,
	}
	atomic.StoreInt64(&tw.firstChunkSN, 0) // First chunk is always sn=0

	// Store in atomic.Value (lock-free for subsequent reads)
	ra.tempWriter.Store(tw)

	// Log creation of TempFileWriter for large file
	DebugLog("[VFS TempFileWriter Create] Created TempFileWriter for large file: fileID=%d, dataID=%d, fileName=%s, chunkSize=%d, realtimeCompressEncrypt=%v",
		ra.fileID, dataID, fileObj.Name, chunkSize, tw.enableRealtime)

	return tw, nil
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
		return fmt.Errorf("handler is not LocalHandler, cannot use Write")
	}

	writeEnd := offset + int64(len(data))
	currentSize := atomic.LoadInt64(&tw.size)
	DebugLog("[VFS TempFileWriter Write] Writing data to large file: fileID=%d, dataID=%d, offset=%d, size=%d, currentFileSize=%d, writeEnd=%d",
		tw.fileID, tw.dataID, offset, len(data), currentSize, writeEnd)

	currentOffset := offset
	dataPos := 0

	// Process data across chunks
	for currentOffset < writeEnd {
		// Use fixed chunk size (always 10MB)
		chunkSize := tw.chunkSize

		// Calculate sn and chunk start position
		sn := int(currentOffset / chunkSize)
		chunkStart := int64(sn) * chunkSize
		writeStartInChunk := currentOffset - chunkStart

		// Calculate how much data to write in this chunk
		writeEndInChunk := writeEnd - chunkStart
		if writeEndInChunk > chunkSize {
			writeEndInChunk = chunkSize
		}
		writeSize := writeEndInChunk - writeStartInChunk

		// Extract data for this chunk
		chunkData := data[dataPos : dataPos+int(writeSize)]

		// Get or create chunk buffer
		tw.mu.Lock()
		buf, exists := tw.chunks[sn]
		if !exists {
			// Chunk buffer doesn't exist in memory, check if it exists on disk
			// First check if chunk exists on disk (might have been flushed already)
			flushKey := fmt.Sprintf("flush_%d_%d", tw.dataID, sn)
			tw.mu.Unlock()

			// Try to read from disk first (most common case: chunk already flushed)
			existingChunkData, readErr := tw.fs.h.GetData(tw.fs.c, tw.fs.bktID, tw.dataID, sn)
			if readErr == nil && len(existingChunkData) > 0 {
				// Chunk exists on disk, re-acquire lock and create buffer
				tw.mu.Lock()
				// Check again if chunk exists in memory (might have been added by another goroutine)
				buf, exists = tw.chunks[sn]
				if exists {
					// Chunk was added to memory by another goroutine, use it
					tw.mu.Unlock()
				} else {
					// Chunk doesn't exist in memory, create buffer and load from disk
					// Always use 10MB buffer from pool (capacity is 10MB, length will be set to chunk size)
					var processedChunk []byte
					dataInfoFromDB, dataInfoErr := tw.fs.h.GetDataInfo(tw.fs.c, tw.fs.bktID, tw.dataID)
					if dataInfoErr == nil && dataInfoFromDB != nil {
						processedChunk = tw.decodeChunkDataWithKind(existingChunkData, dataInfoFromDB.Kind)
					} else {
						processedChunk = tw.decodeChunkData(existingChunkData)
					}
					existingChunkSize := int64(len(processedChunk))

					// Always use 10MB buffer from pool
					pooledBuf := chunkBufferPool.Get().(*chunkBuffer)
					// Set length to chunk size, but keep capacity at 10MB
					bufferLength := chunkSize
					if existingChunkSize > bufferLength {
						bufferLength = existingChunkSize
					}
					pooledBuf.data = pooledBuf.data[:bufferLength]
					pooledBuf.offsetInChunk = 0
					pooledBuf.ranges = pooledBuf.ranges[:0]
					buf = pooledBuf

					// Load existing data into buffer
					if writeStartInChunk < existingChunkSize {
						// Overwrite operation
						if existingChunkSize >= chunkSize {
							buf.addWriteRange(0, chunkSize)
						} else {
							buf.addWriteRange(0, existingChunkSize)
						}
						copyLen := len(processedChunk)
						if copyLen > int(bufferLength) {
							copyLen = int(bufferLength)
						}
						copy(buf.data[:copyLen], processedChunk)
						buf.offsetInChunk = int64(copyLen)
					} else {
						// Append operation
						if existingChunkSize >= chunkSize {
							buf.addWriteRange(0, chunkSize)
							if buf.isChunkComplete(chunkSize) {
								DebugLog("[VFS TempFileWriter Write] Chunk already complete on disk, skipping write: fileID=%d, dataID=%d, sn=%d", tw.fileID, tw.dataID, sn)
								tw.mu.Unlock()
								currentOffset += int64(len(chunkData))
								dataPos += int(len(chunkData))
								continue
							}
						} else {
							buf.addWriteRange(0, existingChunkSize)
						}
						copyLen := len(processedChunk)
						if copyLen > int(bufferLength) {
							copyLen = int(bufferLength)
						}
						copy(buf.data[:copyLen], processedChunk)
						buf.offsetInChunk = int64(copyLen)
					}

					tw.chunks[sn] = buf
					tw.mu.Unlock()
				}
			} else {
				// Chunk doesn't exist on disk, might be flushing
				// Wait for any ongoing flush to complete (non-blocking if no flush in progress)
				_, _, _ = chunkFlushSingleFlight.Do(flushKey, func() (interface{}, error) {
					// If flush is already in progress, this will wait for it to complete
					// If flush is not in progress, this will return immediately
					return nil, nil
				})

				// After waiting, try reading from disk again (flush might have completed)
				existingChunkData2, readErr2 := tw.fs.h.GetData(tw.fs.c, tw.fs.bktID, tw.dataID, sn)

				// Re-acquire lock
				tw.mu.Lock()

				// Check again if chunk exists in memory (might have been added during flush wait)
				buf, exists = tw.chunks[sn]
				if exists {
					// Chunk was added to memory during flush wait, use it
					tw.mu.Unlock()
				} else if readErr2 == nil && len(existingChunkData2) > 0 {
					// Chunk now exists on disk after flush, create buffer and load it
					// Always use 10MB buffer from pool
					var processedChunk []byte
					dataInfoFromDB, dataInfoErr := tw.fs.h.GetDataInfo(tw.fs.c, tw.fs.bktID, tw.dataID)
					if dataInfoErr == nil && dataInfoFromDB != nil {
						processedChunk = tw.decodeChunkDataWithKind(existingChunkData2, dataInfoFromDB.Kind)
					} else {
						processedChunk = tw.decodeChunkData(existingChunkData2)
					}
					existingChunkSize := int64(len(processedChunk))

					// Always use 10MB buffer from pool
					pooledBuf := chunkBufferPool.Get().(*chunkBuffer)
					bufferLength := chunkSize
					if existingChunkSize > bufferLength {
						bufferLength = existingChunkSize
					}
					pooledBuf.data = pooledBuf.data[:bufferLength]
					pooledBuf.offsetInChunk = 0
					pooledBuf.ranges = pooledBuf.ranges[:0]
					buf = pooledBuf

					// Load existing data into buffer
					if writeStartInChunk < existingChunkSize {
						// Overwrite operation
						if existingChunkSize >= chunkSize {
							buf.addWriteRange(0, chunkSize)
						} else {
							buf.addWriteRange(0, existingChunkSize)
						}
						copyLen := len(processedChunk)
						if copyLen > int(bufferLength) {
							copyLen = int(bufferLength)
						}
						copy(buf.data[:copyLen], processedChunk)
						buf.offsetInChunk = int64(copyLen)
					} else {
						// Append operation
						if existingChunkSize >= chunkSize {
							buf.addWriteRange(0, chunkSize)
							if buf.isChunkComplete(chunkSize) {
								DebugLog("[VFS TempFileWriter Write] Chunk already complete on disk, skipping write: fileID=%d, dataID=%d, sn=%d", tw.fileID, tw.dataID, sn)
								tw.mu.Unlock()
								currentOffset += int64(len(chunkData))
								dataPos += int(len(chunkData))
								continue
							}
						} else {
							buf.addWriteRange(0, existingChunkSize)
						}
						copyLen := len(processedChunk)
						if copyLen > int(bufferLength) {
							copyLen = int(bufferLength)
						}
						copy(buf.data[:copyLen], processedChunk)
						buf.offsetInChunk = int64(copyLen)
					}

					tw.chunks[sn] = buf
					tw.mu.Unlock()
				} else {
					// Chunk still doesn't exist, create empty buffer using 10MB pool buffer
					pooledBuf := chunkBufferPool.Get().(*chunkBuffer)
					pooledBuf.data = pooledBuf.data[:chunkSize]
					pooledBuf.offsetInChunk = 0
					pooledBuf.ranges = pooledBuf.ranges[:0]
					buf = pooledBuf
					tw.chunks[sn] = buf
					tw.mu.Unlock()
				}
			}
		} else {
			// Chunk exists in memory, just unlock
			tw.mu.Unlock()
		}

		// Lock this chunk buffer to ensure sequential writes within the chunk
		buf.mu.Lock()

		if sn == 0 && !tw.realtimeDecided {
			tw.decideRealtimeProcessing(chunkData)
		}

		// Copy data into buffer
		// Buffer from pool has 10MB capacity, but length may be smaller
		// Grow buffer length if needed (capacity is already 10MB, so this is just reslicing)
		requiredBufferSize := writeStartInChunk + int64(len(chunkData))
		if requiredBufferSize > int64(len(buf.data)) {
			// Grow buffer length (capacity is already 10MB, so this is just reslicing)
			newBufferLength := requiredBufferSize
			if newBufferLength < chunkSize {
				newBufferLength = chunkSize
			}
			// Ensure we don't exceed capacity (shouldn't happen with 10MB capacity)
			if newBufferLength > int64(cap(buf.data)) {
				newBufferLength = int64(cap(buf.data))
			}
			buf.data = buf.data[:newBufferLength]
			DebugLog("[VFS TempFileWriter Write] Grew buffer length: fileID=%d, dataID=%d, sn=%d, oldLen=%d, newLen=%d, requiredSize=%d, capacity=%d",
				tw.fileID, tw.dataID, sn, len(buf.data), newBufferLength, requiredBufferSize, cap(buf.data))
		}
		copy(buf.data[writeStartInChunk:writeStartInChunk+int64(len(chunkData))], chunkData)
		writeEndInChunkPos := writeStartInChunk + int64(len(chunkData))
		if writeEndInChunkPos > buf.offsetInChunk {
			buf.offsetInChunk = writeEndInChunkPos
		}

		// Add write range and merge adjacent ranges
		buf.addWriteRange(writeStartInChunk, writeEndInChunkPos)

		bufferProgress := buf.offsetInChunk

		// Check if chunk is complete: must have exactly one range covering entire chunk
		chunkComplete := buf.isChunkComplete(chunkSize)
		buf.mu.Unlock()

		DebugLog("[VFS TempFileWriter Write] Chunk status: fileID=%d, dataID=%d, sn=%d, chunkSize=%d, bufferProgress=%d, remaining=%d, complete=%v",
			tw.fileID, tw.dataID, sn, chunkSize, buf.offsetInChunk, chunkSize-buf.offsetInChunk, chunkComplete)

		if chunkComplete {
			// Chunk is full, flush it asynchronously
			// IMPORTANT: Create a copy of the buffer data before removing from chunks
			// This ensures that the flush operation uses a snapshot of the data at the time of flush
			// and prevents data corruption if the buffer is modified during flush
			buf.mu.Lock()
			chunkDataCopy := make([]byte, buf.offsetInChunk)
			copy(chunkDataCopy, buf.data[:buf.offsetInChunk])
			bufRangesCopy := make([]writeRange, len(buf.ranges))
			copy(bufRangesCopy, buf.ranges)
			buf.mu.Unlock()

			// Remove chunk from tw.chunks IMMEDIATELY to prevent further writes
			// This ensures that subsequent writes to this chunk will read from disk instead
			tw.mu.Lock()
			delete(tw.chunks, sn) // Remove immediately to prevent concurrent writes
			tw.mu.Unlock()

			DebugLog("[VFS TempFileWriter Write] Chunk full, flushing asynchronously: fileID=%d, dataID=%d, sn=%d, bufferSize=%d/%d",
				tw.fileID, tw.dataID, sn, bufferProgress, chunkSize)

			// Use singleflight to ensure only one flush per chunk at a time
			// Key format: "flush_<dataID>_<sn>"
			flushKey := fmt.Sprintf("flush_%d_%d", tw.dataID, sn)

			// Call singleflight asynchronously
			go func() {
				// In Write(), we don't know if this is the last chunk, so use isLastChunk=false
				// The last chunk will be determined and flushed during Flush()
				_, err, _ := chunkFlushSingleFlight.Do(flushKey, func() (interface{}, error) {
					// Create a temporary buffer with the copied data for flushing
					flushBuf := &chunkBuffer{
						data:          chunkDataCopy,
						offsetInChunk: int64(len(chunkDataCopy)),
						ranges:        bufRangesCopy,
					}

					// Flush the chunk using the copied buffer
					flushErr := tw.flushChunkWithBuffer(sn, false, flushBuf)
					if flushErr != nil {
						return nil, flushErr
					}

					// Return original chunk buffer to pool after successful flush
					if cap(buf.data) <= 10<<20 {
						buf.data = buf.data[:0] // Reset length, keep capacity
						buf.offsetInChunk = 0
						buf.ranges = buf.ranges[:0] // Reset ranges
						chunkBufferPool.Put(buf)
					}

					currentFileSize := atomic.LoadInt64(&tw.size)
					DebugLog("[VFS TempFileWriter Write] Chunk flushed asynchronously: fileID=%d, dataID=%d, sn=%d, chunkSize=%d, currentFileSize=%d",
						tw.fileID, tw.dataID, sn, bufferProgress, currentFileSize)

					return nil, nil
				})

				if err != nil {
					DebugLog("[VFS TempFileWriter Write] ERROR: Failed to flush chunk asynchronously: fileID=%d, dataID=%d, sn=%d, error=%v", tw.fileID, tw.dataID, sn, err)
				}
			}()
		} else {
			// Chunk not full yet, just buffer the data
			// Will be flushed when chunk is full or during final Flush()
			DebugLog("[VFS TempFileWriter Write] Data buffered in chunk: fileID=%d, dataID=%d, sn=%d, bufferSize=%d/%d, remaining=%d",
				tw.fileID, tw.dataID, sn, bufferProgress, chunkSize, chunkSize-bufferProgress)
		}

		// Update tracking
		currentOffset += int64(len(chunkData))
		dataPos += int(len(chunkData))
	}

	// Update size atomically (always update to maximum writeEnd)
	// This ensures that concurrent writes correctly track the maximum file size
	for {
		currentSize := atomic.LoadInt64(&tw.size)
		if writeEnd <= currentSize {
			// Already updated by another concurrent write, no need to update
			break
		}
		if atomic.CompareAndSwapInt64(&tw.size, currentSize, writeEnd) {
			DebugLog("[VFS TempFileWriter Write] Updated file size: fileID=%d, dataID=%d, oldSize=%d, newSize=%d",
				tw.fileID, tw.dataID, currentSize, writeEnd)
			break
		}
		// CAS failed, retry (another concurrent write may have updated size)
	}

	return nil
}

// flushChunk processes and writes a complete chunk
// If real-time compression/encryption is enabled, processes the chunk before writing
// Otherwise, writes raw data directly (for offline processing)
// All chunks are written synchronously
func (tw *TempFileWriter) flushChunk(sn int, isLastChunk bool) error {
	tw.mu.Lock()
	buf, exists := tw.chunks[sn]
	tw.mu.Unlock()

	if !exists {
		return nil // Chunk already flushed
	}

	return tw.flushChunkWithBuffer(sn, isLastChunk, buf)
}

// flushChunkWithBuffer processes and writes a complete chunk using the provided buffer
// This is used when the chunk has already been removed from tw.chunks to prevent concurrent writes
func (tw *TempFileWriter) flushChunkWithBuffer(sn int, isLastChunk bool, buf *chunkBuffer) error {
	if buf == nil {
		return nil // No buffer to flush
	}

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.offsetInChunk == 0 {
		return nil // No data to flush
	}

	// Extract chunk data (only up to written size)
	// IMPORTANT: Create a copy of the data to avoid data race when buffer is modified concurrently
	// This is important to avoid data race when buffer is modified concurrently
	chunkData := make([]byte, buf.offsetInChunk)
	copy(chunkData, buf.data[:buf.offsetInChunk])
	bucket := getBucketConfigWithCache(tw.fs)

	DebugLog("[VFS TempFileWriter flushChunk] Processing chunk: fileID=%d, dataID=%d, sn=%d, originalSize=%d, enableRealtime=%v",
		tw.fileID, tw.dataID, sn, len(chunkData), tw.enableRealtime)

	// Update DataInfo CRC32 and OrigSize
	// tw.dataInfo.CRC32 = crc32.Update(tw.dataInfo.CRC32, crc32.IEEETable, chunkData)
	// Use atomic operation for thread-safe OrigSize update (multiple chunks may flush concurrently)
	atomic.AddInt64(&tw.dataInfo.OrigSize, int64(len(chunkData)))

	var finalData []byte
	var err error

	if tw.enableRealtime {
		// Real-time compression/encryption enabled
		// Process first chunk: check file extension first, then file type
		isFirstChunk := sn == 0
		if isFirstChunk && bucket != nil && bucket.CmprWay > 0 && len(chunkData) > 0 {
			shouldCompress := true

			// Step 1: Check file extension first (faster than file header check)
			if tw.fileName != "" {
				fileNameLower := strings.ToLower(tw.fileName)
				// Common compressed/encoded file extensions that don't benefit from compression
				nonCompressibleExts := []string{
					".zip", ".gz", ".bz2", ".xz", ".7z", ".rar", ".tar",
					".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".ico",
					".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm",
					".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a",
					".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
					".woff", ".woff2", ".ttf", ".otf", ".eot",
				}
				for _, ext := range nonCompressibleExts {
					if strings.HasSuffix(fileNameLower, ext) {
						shouldCompress = false
						DebugLog("[VFS TempFileWriter flushChunk] File extension indicates non-compressible type, skipping compression: fileID=%d, dataID=%d, sn=%d, fileName=%s, ext=%s",
							tw.fileID, tw.dataID, sn, tw.fileName, ext)
						break
					}
				}
			}

			// Step 2: If extension check passed, check file header
			if shouldCompress {
				detectedKind, _ := filetype.Match(chunkData)
				if detectedKind != filetype.Unknown {
					// Not unknown type, don't compress
					shouldCompress = false
					DebugLog("[VFS TempFileWriter flushChunk] File header indicates known type, skipping compression: fileID=%d, dataID=%d, sn=%d, fileName=%s, detectedKind=%s",
						tw.fileID, tw.dataID, sn, tw.fileName, detectedKind.Extension)
				}
			}

			if !shouldCompress {
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
				// Encryption failed, write unencrypted data and clear encryption flag
				finalData = processedChunk
				if isFirstChunk {
					tw.dataInfo.Kind &= ^core.DATA_ENDEC_MASK
					DebugLog("[VFS TempFileWriter flushChunk] Encryption failed, cleared encryption flag: fileID=%d, dataID=%d, sn=%d, Kind=0x%x", tw.fileID, tw.dataID, sn, tw.dataInfo.Kind)
				}
			}
		} else if bucket != nil && tw.dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
			finalData, err = sm4.Sm4Cbc([]byte(bucket.EndecKey), processedChunk, true)
			if err != nil {
				// Encryption failed, write unencrypted data and clear encryption flag
				finalData = processedChunk
				if isFirstChunk {
					tw.dataInfo.Kind &= ^core.DATA_ENDEC_MASK
					DebugLog("[VFS TempFileWriter flushChunk] Encryption failed, cleared encryption flag: fileID=%d, dataID=%d, sn=%d, Kind=0x%x", tw.fileID, tw.dataID, sn, tw.dataInfo.Kind)
				}
			}
		} else {
			finalData = processedChunk
		}

		// Update CRC32 and size of final data
		// tw.dataInfo.Cksum = crc32.Update(tw.dataInfo.Cksum, crc32.IEEETable, finalData)
		DebugLog("[VFS TempFileWriter flushChunk] Compression/encryption applied: fileID=%d, dataID=%d, sn=%d, originalSize=%d, finalSize=%d, ratio=%.2f%%",
			tw.fileID, tw.dataID, sn, len(chunkData), len(finalData), float64(len(finalData))*100.0/float64(len(chunkData)))
	} else {
		// Real-time compression/encryption disabled - write raw data for offline processing
		// Similar to writing versions, data will be processed offline later
		finalData = chunkData
		DebugLog("[VFS TempFileWriter flushChunk] Writing raw data (offline processing): fileID=%d, dataID=%d, sn=%d, size=%d",
			tw.fileID, tw.dataID, sn, len(finalData))
	}

	// All chunks are written synchronously
	DebugLog("[VFS TempFileWriter flushChunk] Writing chunk synchronously: fileID=%d, dataID=%d, sn=%d, size=%d, isLastChunk=%v, realtime=%v",
		tw.fileID, tw.dataID, sn, len(finalData), isLastChunk, tw.enableRealtime)

	// Write chunk to disk first, then update Size only if write was successful
	// This prevents Size from being accumulated multiple times if chunk is flushed multiple times
	err = tw.writeChunkSync(sn, finalData)
	if err != nil {
		return err
	}

	// Only update Size after successful write to prevent duplicate accumulation
	// Use atomic operation for thread-safe Size update (multiple chunks may flush concurrently)
	// For compressed/encrypted data, Size is the compressed/encrypted size
	atomic.AddInt64(&tw.dataInfo.Size, int64(len(finalData)))

	return nil
}

// writeChunkSync writes a chunk synchronously (used for last chunk)
// IMPORTANT: This function should only be called once per chunk (sn)
// If the chunk already exists on disk, we check if it's the same size and skip writing
func (tw *TempFileWriter) writeChunkSync(sn int, finalData []byte) error {
	// First, check if chunk already exists on disk
	// This prevents duplicate writes if flushChunk is called multiple times for the same chunk
	existingChunkData, readErr := tw.fs.h.GetData(tw.fs.c, tw.fs.bktID, tw.dataID, sn)
	if readErr == nil && len(existingChunkData) > 0 {
		// Chunk already exists on disk
		// If size is the same, skip writing (already written)
		// If size is different, it means chunk was modified, need to overwrite
		if len(existingChunkData) == len(finalData) {
			// Chunk already exists with the same size, skip writing
			// This handles the case where flushChunk was called multiple times (e.g., from Write() and Flush())
			DebugLog("[VFS TempFileWriter writeChunkSync] Chunk already exists with same size, skipping write: fileID=%d, dataID=%d, sn=%d, size=%d",
				tw.fileID, tw.dataID, sn, len(finalData))
			return nil
		} else {
			// Chunk exists but size is different - this should not happen for sequential writes
			// Log warning but proceed with overwrite
			DebugLog("[VFS TempFileWriter writeChunkSync] WARNING: Chunk exists with different size, will overwrite: fileID=%d, dataID=%d, sn=%d, existingSize=%d, newSize=%d",
				tw.fileID, tw.dataID, sn, len(existingChunkData), len(finalData))
		}
	}

	_, err := tw.fs.h.PutData(tw.fs.c, tw.fs.bktID, tw.dataID, sn, finalData)
	if err != nil {
		// If error is due to file already existing (ERR_OPEN_FILE), check if it's the same size
		if err == core.ERR_OPEN_FILE {
			// Check if existing file has the same size (another goroutine may have written it)
			existingChunkData, readErr := tw.fs.h.GetData(tw.fs.c, tw.fs.bktID, tw.dataID, sn)
			if readErr == nil && len(existingChunkData) == len(finalData) {
				// Chunk already exists with the same size, skip writing
				DebugLog("[VFS TempFileWriter writeChunkSync] Chunk already exists with same size (from ERR_OPEN_FILE check), skipping write: fileID=%d, dataID=%d, sn=%d, size=%d",
					tw.fileID, tw.dataID, sn, len(finalData))
				return nil
			}
			// File exists but size is different - this should not happen for sequential writes
			// Log warning and return error (don't delete, let upper layer handle it)
			DebugLog("[VFS TempFileWriter writeChunkSync] WARNING: Chunk exists with different size (ERR_OPEN_FILE): fileID=%d, dataID=%d, sn=%d, existingSize=%d, newSize=%d, error=%v",
				tw.fileID, tw.dataID, sn, len(existingChunkData), len(finalData), err)
			return err
		} else {
			DebugLog("[VFS TempFileWriter writeChunkSync] ERROR: Failed to put data: fileID=%d, dataID=%d, sn=%d, error=%v", tw.fileID, tw.dataID, sn, err)
			return err
		}
	}

	// PutData already calls f.Sync() in DefaultDataAdapter.Write, ensuring data is flushed to disk
	// Each chunk is independently flushed when written
	DebugLog("[VFS TempFileWriter writeChunkSync] Successfully wrote and flushed chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d",
		tw.fileID, tw.dataID, sn, len(finalData))
	return nil
}

// decodeChunkData decodes chunk data using TempFileWriter's compression/encryption configuration
func (tw *TempFileWriter) decodeChunkData(chunkData []byte) []byte {
	return tw.decodeChunkDataWithKind(chunkData, tw.dataInfo.Kind)
}

// decodeChunkDataWithKind decodes chunk data using specified Kind (compression/encryption configuration)
// This allows decoding chunks that were written with different Kind than current tw.dataInfo.Kind
func (tw *TempFileWriter) decodeChunkDataWithKind(chunkData []byte, kind uint32) []byte {
	if len(chunkData) == 0 {
		return chunkData
	}

	processedChunk := chunkData

	// 1. Decrypt first (if enabled)
	if kind&core.DATA_ENDEC_MASK != 0 {
		bucket := getBucketConfigWithCache(tw.fs)
		if bucket != nil {
			var err error
			if kind&core.DATA_ENDEC_AES256 != 0 {
				processedChunk, err = aes256.Decrypt(bucket.EndecKey, processedChunk)
				if err != nil {
					// Decryption failed, return original data
					DebugLog("[VFS TempFileWriter decodeChunkDataWithKind] Decryption failed, using raw data: fileID=%d, dataID=%d, error=%v", tw.fileID, tw.dataID, err)
					return chunkData
				}
			} else if kind&core.DATA_ENDEC_SM4 != 0 {
				processedChunk, err = sm4.Sm4Cbc([]byte(bucket.EndecKey), processedChunk, false)
				if err != nil {
					// Decryption failed, return original data
					DebugLog("[VFS TempFileWriter decodeChunkDataWithKind] Decryption failed, using raw data: fileID=%d, dataID=%d, error=%v", tw.fileID, tw.dataID, err)
					return chunkData
				}
			}
		}
	}

	// 2. Decompress next (if enabled)
	if kind&core.DATA_CMPR_MASK != 0 {
		var decompressor archiver.Decompressor
		if kind&core.DATA_CMPR_SNAPPY != 0 {
			decompressor = &archiver.Snappy{}
		} else if kind&core.DATA_CMPR_ZSTD != 0 {
			decompressor = &archiver.Zstd{}
		} else if kind&core.DATA_CMPR_GZIP != 0 {
			decompressor = &archiver.Gz{}
		} else if kind&core.DATA_CMPR_BR != 0 {
			decompressor = &archiver.Brotli{}
		}

		if decompressor != nil {
			var decompressedBuf bytes.Buffer
			if err := decompressor.Decompress(bytes.NewReader(processedChunk), &decompressedBuf); err == nil {
				processedChunk = decompressedBuf.Bytes()
			} else {
				// Decompression failed, return data after decryption (or original if no decryption)
				DebugLog("[VFS TempFileWriter decodeChunkDataWithKind] Decompression failed, using decrypted data: fileID=%d, dataID=%d, error=%v", tw.fileID, tw.dataID, err)
			}
		}
	}

	return processedChunk
}

func (tw *TempFileWriter) decideRealtimeProcessing(firstChunk []byte) {
	tw.realtimeDecided = true
	bucket := getBucketConfigWithCache(tw.fs)
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

	// If compression is enabled, perform smart detection:
	// 1. First check file extension to quickly filter out non-compressible files
	// 2. If extension doesn't indicate compression, then check file header
	if kind&core.DATA_CMPR_MASK != 0 && len(firstChunk) > 0 {
		shouldCompress := true

		// Step 1: Check file extension first (faster than file header check)
		if tw.fileName != "" {
			fileNameLower := strings.ToLower(tw.fileName)
			// Common compressed/encoded file extensions that don't benefit from compression
			nonCompressibleExts := []string{
				".zip", ".gz", ".bz2", ".xz", ".7z", ".rar", ".tar",
				".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".ico",
				".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm",
				".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a",
				".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
				".woff", ".woff2", ".ttf", ".otf", ".eot",
			}
			for _, ext := range nonCompressibleExts {
				if strings.HasSuffix(fileNameLower, ext) {
					shouldCompress = false
					DebugLog("[VFS TempFileWriter decideRealtimeProcessing] File extension indicates non-compressible type, skipping compression: fileID=%d, fileName=%s, ext=%s",
						tw.fileID, tw.fileName, ext)
					break
				}
			}
		}

		// Step 2: If extension check passed, check file header
		if shouldCompress {
			if detectedKind, _ := filetype.Match(firstChunk); detectedKind != filetype.Unknown {
				shouldCompress = false
				DebugLog("[VFS TempFileWriter decideRealtimeProcessing] File header indicates known type, skipping compression: fileID=%d, fileName=%s, detectedKind=%s",
					tw.fileID, tw.fileName, detectedKind.Extension)
			}
		}

		if !shouldCompress {
			kind &= ^core.DATA_CMPR_MASK
		}
	}

	tw.dataInfo.Kind = kind
	DebugLog("[VFS TempFileWriter] realtime processing decided: fileID=%d, enableRealtime=%v, kind=0x%x", tw.fileID, tw.enableRealtime, tw.dataInfo.Kind)
}

// Flush uploads DataInfo and ObjectInfo for .tmp file
// Data chunks are already on disk via AppendData, so we only need to upload metadata
func (tw *TempFileWriter) Flush() error {
	size := atomic.LoadInt64(&tw.size)
	DebugLog("[VFS TempFileWriter Flush] Starting flush for large file: fileID=%d, dataID=%d, fileSize=%d",
		tw.fileID, tw.dataID, size)

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
	// IMPORTANT: Collect all chunks that need to be processed, including:
	// 1. Chunks still in tw.chunks (incomplete chunks)
	// 2. All chunks are written synchronously, no pending writes
	// This ensures we don't miss any chunks, especially the last chunk
	// Optimization: use object pool for remainingChunks slice
	tw.mu.Lock()
	remainingChunks := intSlicePool.Get().([]int)
	remainingChunks = remainingChunks[:0] // Reset length, keep capacity

	// First, collect chunks from tw.chunks
	chunkCount := len(tw.chunks)
	if cap(remainingChunks) < chunkCount {
		remainingChunks = make([]int, 0, chunkCount)
	}
	for sn := range tw.chunks {
		remainingChunks = append(remainingChunks, sn)
	}
	tw.mu.Unlock()

	if len(remainingChunks) > 0 {
		DebugLog("[VFS TempFileWriter Flush] Flushing %d remaining incomplete chunks: fileID=%d, dataID=%d, chunks=%v",
			len(remainingChunks), tw.fileID, tw.dataID, remainingChunks)
	}

	// Sort chunks to ensure we process them in order
	// This ensures the last chunk is identified correctly
	sort.Ints(remainingChunks)

	// All chunks are written synchronously, no need to wait for async writes

	// Find the last chunk (highest sn) based on current file size
	currentSize := atomic.LoadInt64(&tw.size)
	// Calculate last chunk SN (always use fixed chunk size)
	calculatedLastChunkSN := int((currentSize+tw.chunkSize-1)/tw.chunkSize) - 1
	if calculatedLastChunkSN < 0 {
		calculatedLastChunkSN = 0
	}

	// Also check remainingChunks for the highest sn (in case there are incomplete chunks)
	// Note: remainingChunks is already sorted by sort.Ints(remainingChunks) above
	var lastChunkSN int = calculatedLastChunkSN
	if len(remainingChunks) > 0 {
		// remainingChunks is already sorted, so the last element is the highest
		highestSN := remainingChunks[len(remainingChunks)-1]
		if highestSN > lastChunkSN {
			lastChunkSN = highestSN
		}
	}
	atomic.StoreInt64(&tw.lastChunkSN, int64(lastChunkSN))
	DebugLog("[VFS TempFileWriter Flush] Determined last chunk: fileID=%d, dataID=%d, calculatedLastChunkSN=%d, lastChunkSN=%d, currentSize=%d, remainingChunks=%v",
		tw.fileID, tw.dataID, calculatedLastChunkSN, lastChunkSN, currentSize, remainingChunks)

	// Process all remaining chunks
	// IMPORTANT: Ensure we process all chunks from 0 to lastChunkSN, even if some are missing from remainingChunks
	allChunksToProcess := make(map[int]bool)
	for _, sn := range remainingChunks {
		allChunksToProcess[sn] = true
	}

	// Also ensure we process all chunks from 0 to lastChunkSN
	// This is critical for ensuring all data is written, especially the last chunk
	for sn := 0; sn <= lastChunkSN; sn++ {
		allChunksToProcess[sn] = true
	}

	// Convert to sorted slice for processing
	sortedChunksToProcess := make([]int, 0, len(allChunksToProcess))
	for sn := range allChunksToProcess {
		sortedChunksToProcess = append(sortedChunksToProcess, sn)
	}
	sort.Ints(sortedChunksToProcess)

	DebugLog("[VFS TempFileWriter Flush] Processing chunks: fileID=%d, dataID=%d, chunksToProcess=%v (0 to %d)", tw.fileID, tw.dataID, sortedChunksToProcess, lastChunkSN)

	for _, sn := range sortedChunksToProcess {
		// Check if chunk still exists in tw.chunks
		tw.mu.Lock()
		_, chunkExists := tw.chunks[sn]
		tw.mu.Unlock()

		if !chunkExists {
			// Chunk doesn't exist, it must have been written already
			DebugLog("[VFS TempFileWriter Flush] Chunk %d not in chunks, assuming already written: fileID=%d, dataID=%d", sn, tw.fileID, tw.dataID)
			continue
		}

		// Flush it now (should be last chunk or incomplete chunk)
		// Determine if this is the last chunk based on file size
		isLastChunk := sn == lastChunkSN
		if isLastChunk {
			DebugLog("[VFS TempFileWriter Flush] Flushing last chunk: fileID=%d, dataID=%d, sn=%d", tw.fileID, tw.dataID, sn)
		} else {
			DebugLog("[VFS TempFileWriter Flush] Flushing incomplete chunk: fileID=%d, dataID=%d, sn=%d", tw.fileID, tw.dataID, sn)
		}

		// Use singleflight to ensure only one flush per chunk at a time
		// Key format: "flush_<dataID>_<sn>"
		flushKey := fmt.Sprintf("flush_%d_%d", tw.dataID, sn)

		// Get buffer reference before removing from chunks
		tw.mu.Lock()
		flushBuf, exists := tw.chunks[sn]
		if !exists {
			// Chunk already flushed or doesn't exist
			tw.mu.Unlock()
			continue
		}
		// Remove chunk from tw.chunks immediately to prevent concurrent writes
		delete(tw.chunks, sn)
		tw.mu.Unlock()

		_, err, _ := chunkFlushSingleFlight.Do(flushKey, func() (interface{}, error) {
			// Flush the chunk using the saved buffer reference
			flushErr := tw.flushChunkWithBuffer(sn, isLastChunk, flushBuf)
			if flushErr != nil {
				return nil, flushErr
			}

			// Return chunk buffer to pool after successful flush
			if cap(flushBuf.data) <= 10<<20 {
				flushBuf.data = flushBuf.data[:0] // Reset length, keep capacity
				flushBuf.offsetInChunk = 0
				flushBuf.ranges = flushBuf.ranges[:0] // Reset ranges
				chunkBufferPool.Put(flushBuf)
			}

			return nil, nil
		})

		if err != nil {
			// Return remainingChunks slice to pool on error
			intSlicePool.Put(remainingChunks[:0])
			DebugLog("[VFS TempFileWriter Flush] ERROR: Failed to flush remaining chunk: fileID=%d, dataID=%d, sn=%d, error=%v", tw.fileID, tw.dataID, sn, err)
			return err
		}
	}

	// Return remainingChunks slice to pool after use
	intSlicePool.Put(remainingChunks[:0])

	// Use the DataInfo we've been tracking (with compression/encryption flags and sizes)
	dataInfo := tw.dataInfo

	// OrigSize should be the logical file size (max write offset), not the accumulated value
	// The accumulated OrigSize in flushChunk tracks actual data written, but for files with holes,
	// we need to use the logical size. However, for consistency, we should use the logical size.
	// But note: if there are holes, the accumulated OrigSize might be less than size.
	// For now, use size as OrigSize to match the file's logical size.
	dataInfo.OrigSize = size

	// For offline processing, Size should equal OrigSize (actual data size on disk)
	// For real-time processing, Size is already accumulated (compressed/encrypted size)
	if !tw.enableRealtime {
		// For offline processing, Size should equal OrigSize (both are actual data size on disk)
		// The accumulated Size in flushChunk should already equal OrigSize for offline processing
		// But to be safe, ensure they match
		if dataInfo.Size != dataInfo.OrigSize {
			DebugLog("[VFS TempFileWriter Flush] WARNING: Size mismatch for offline processing: Size=%d, OrigSize=%d, correcting", dataInfo.Size, dataInfo.OrigSize)
			dataInfo.Size = dataInfo.OrigSize
		}
		// IMPORTANT: If enableRealtime is false, we wrote raw data, so clear compression/encryption flags
		// This ensures that readers don't try to decompress/decrypt raw data
		if dataInfo.Kind&(core.DATA_CMPR_MASK|core.DATA_ENDEC_MASK) != 0 {
			DebugLog("[VFS TempFileWriter Flush] WARNING: Offline processing but Kind has compression/encryption flags, clearing: fileID=%d, dataID=%d, Kind=0x%x", tw.fileID, tw.dataID, dataInfo.Kind)
			dataInfo.Kind &= ^(core.DATA_CMPR_MASK | core.DATA_ENDEC_MASK)
		}
	} else {
		// For real-time processing, Size is already accumulated (compressed/encrypted size)
		// Don't overwrite it
		DebugLog("[VFS TempFileWriter Flush] Real-time processing: OrigSize=%d, Size=%d (compressed/encrypted)", dataInfo.OrigSize, dataInfo.Size)
	}

	// Update file object
	obj.DataID = tw.dataID
	obj.Size = size
	obj.MTime = core.Now()

	// IMPORTANT: Save original name BEFORE PutDataInfoAndObj (in case it gets updated)
	// We will rename the file ONLY after all chunks are flushed and DataInfo is uploaded
	// This ensures that the file name reflects the actual state: if TempFileWriter exists, it's still a temp file
	originalObjName := obj.Name
	objNameLower := strings.ToLower(originalObjName)
	shouldRemoveTmp := strings.HasSuffix(objNameLower, ".tmp")
	DebugLog("[VFS TempFileWriter Flush] Checking for .tmp suffix removal: fileID=%d, fileName=%s, shouldRemoveTmp=%v, objNameLower=%s", tw.fileID, originalObjName, shouldRemoveTmp, objNameLower)

	// Upload DataInfo and ObjectInfo together
	// IMPORTANT: Do this BEFORE renaming to ensure all data is persisted
	DebugLog("[VFS TempFileWriter Flush] Uploading metadata: fileID=%d, dataID=%d, fileName=%s, origSize=%d, size=%d, chunkSize=%d, realtime=%v, hasCompression=%v, hasEncryption=%v",
		tw.fileID, tw.dataID, obj.Name, dataInfo.OrigSize, dataInfo.Size, tw.chunkSize, tw.enableRealtime,
		dataInfo.Kind&core.DATA_CMPR_MASK != 0, dataInfo.Kind&core.DATA_ENDEC_MASK != 0)
	err = tw.fs.h.PutDataInfoAndObj(tw.fs.c, tw.fs.bktID, []*core.DataInfo{dataInfo}, []*core.ObjectInfo{obj})
	if err != nil {
		DebugLog("[VFS TempFileWriter Flush] ERROR: Failed to upload DataInfo and ObjectInfo: fileID=%d, dataID=%d, error=%v", tw.fileID, tw.dataID, err)
		return err
	}

	// Re-fetch file object from database to ensure we have the latest information
	// This is important for directory cache updates
	updatedFileObjs, err := tw.fs.h.Get(tw.fs.c, tw.fs.bktID, []int64{tw.fileID})
	if err == nil && len(updatedFileObjs) > 0 {
		obj = updatedFileObjs[0]
		DebugLog("[VFS TempFileWriter Flush] Re-fetched file object from database: fileID=%d, dataID=%d, size=%d, pid=%d, name=%s", tw.fileID, obj.DataID, obj.Size, obj.PID, obj.Name)
		// Use re-fetched name if it still has .tmp suffix
		if strings.HasSuffix(strings.ToLower(obj.Name), ".tmp") {
			originalObjName = obj.Name
			objNameLower = strings.ToLower(obj.Name)
			shouldRemoveTmp = true
		}
	}

	// Update cache
	fileObjCache.Put(tw.fileID, obj)
	dataInfoCache.Put(tw.dataID, dataInfo)

	// After sync flush, append file to directory listing cache
	// This ensures the file is immediately visible in Readdir
	if obj.PID > 0 {
		dirNode := &OrcasNode{
			fs:    tw.fs,
			objID: obj.PID,
		}
		dirNode.appendChildToDirCache(obj.PID, obj)
		DebugLog("[VFS TempFileWriter Flush] Appended file to directory listing cache after sync flush: fileID=%d, dirID=%d, name=%s", tw.fileID, obj.PID, obj.Name)
	}

	// Update RandomAccessor's fileObj cache if available
	// This ensures that subsequent reads use the updated file size
	if ra := tw.fs.getRandomAccessorByFileID(tw.fileID); ra != nil {
		ra.fileObj.Store(obj)
		fileObjCache.Put(ra.fileObjKey, obj)
	}

	// Calculate compression ratio if applicable
	compressionRatio := 1.0
	if dataInfo.OrigSize > 0 && dataInfo.Size != dataInfo.OrigSize {
		compressionRatio = float64(dataInfo.Size) / float64(dataInfo.OrigSize)
	}

	DebugLog("[VFS TempFileWriter Flush] Successfully flushed large file: fileID=%d, dataID=%d, fileName=%s, origSize=%d, size=%d, chunkSize=%d, compressionRatio=%.2f%%",
		tw.fileID, tw.dataID, obj.Name, dataInfo.OrigSize, dataInfo.Size, tw.chunkSize, compressionRatio*100.0)

	// Verify written data (for debugging - can be disabled in production)
	//if err := tw.VerifyWriteData(); err != nil {
	//	DebugLog("[VFS TempFileWriter Flush] WARNING: Write verification failed: fileID=%d, dataID=%d, error=%v", tw.fileID, tw.dataID, err)
	// Don't return error - verification failure doesn't mean write failed
	//}

	// IMPORTANT: Auto-remove .tmp suffix ONLY after all chunks are flushed and DataInfo is uploaded
	// This ensures that:
	// 1. All data blocks are written to disk
	// 2. DataInfo is persisted in database
	// 3. File name change happens last, so hasTempFileWriter() is the authoritative check
	// Only rename if all chunks are flushed (they should be, since we just flushed them above)
	if shouldRemoveTmp {
		// Verify all chunks are flushed: tw.chunks should be empty at this point
		tw.mu.Lock()
		remainingChunksCount := len(tw.chunks)
		tw.mu.Unlock()

		if remainingChunksCount > 0 {
			DebugLog("[VFS TempFileWriter Flush] WARNING: Cannot rename yet, %d chunks still in memory: fileID=%d, fileName=%s", remainingChunksCount, tw.fileID, originalObjName)
			// Don't rename yet - chunks are still being written
			// The rename will happen on next Flush() call when all chunks are flushed
		} else {
			// All chunks are flushed, safe to rename
			DebugLog("[VFS TempFileWriter Flush] All chunks confirmed flushed, proceeding with rename: fileID=%d, fileName=%s", tw.fileID, originalObjName)
			DebugLog("[VFS TempFileWriter Flush] File has .tmp suffix, removing it: fileID=%d, fileName=%s", tw.fileID, originalObjName)
			// Remove .tmp suffix (case-insensitive)
			objNameLen := len(originalObjName)
			tmpSuffixLen := 4 // ".tmp"
			if objNameLen >= tmpSuffixLen {
				newName := originalObjName[:objNameLen-tmpSuffixLen]
				DebugLog("[VFS TempFileWriter Flush] Calculated new name: fileID=%d, oldName=%s, newName=%s, objNameLen=%d, tmpSuffixLen=%d", tw.fileID, originalObjName, newName, objNameLen, tmpSuffixLen)
				if newName != originalObjName {
					DebugLog("[VFS TempFileWriter Flush] Auto-removing .tmp suffix after flush: fileID=%d, oldName=%s, newName=%s", tw.fileID, originalObjName, newName)

					// For small files, check if file should be moved to batch writer after rename
					// Small files (< batch write threshold) should use batch write instead of TempFileWriter
					config := core.GetWriteBufferConfig()
					batchWriteThreshold := config.MaxBatchWriteFileSize
					if batchWriteThreshold <= 0 {
						batchWriteThreshold = 64 << 10 // Default 64KB
					}
					fileSize := atomic.LoadInt64(&tw.size)
					isSmallFile := fileSize < batchWriteThreshold

					// Check if a .tmp file with the same name already exists (different fileID)
					// This can happen when uploading a new file with the same .tmp name
					// We need to delete the old .tmp file before renaming
					if obj.PID > 0 {
						// Get directory listing to find existing .tmp file
						children, _, _, err := tw.fs.h.List(tw.fs.c, tw.fs.bktID, obj.PID, core.ListOptions{
							Count: core.DefaultListPageSize,
						})
						if err == nil {
							for _, child := range children {
								// Check if there's another .tmp file with the same name (different fileID)
								if child.ID != tw.fileID && child.Name == originalObjName && child.Type == core.OBJ_TYPE_FILE {
									DebugLog("[VFS TempFileWriter Flush] Found existing .tmp file with same name, deleting it: fileID=%d, name=%s", child.ID, child.Name)

									// Remove from batch writer if present
									batchMgr := tw.fs.getBatchWriteManager()
									if batchMgr != nil {
										batchMgr.RemovePendingObject(child.ID)
									}

									// Flush and unregister RandomAccessor if exists
									if tw.fs != nil {
										if childRA := tw.fs.getRandomAccessorByFileID(child.ID); childRA != nil {
											// Force flush before deletion
											if _, flushErr := childRA.ForceFlush(); flushErr != nil {
												DebugLog("[VFS TempFileWriter Flush] WARNING: Failed to flush existing .tmp file before deletion: fileID=%d, error=%v", child.ID, flushErr)
											}
											// Unregister RandomAccessor
											tw.fs.unregisterRandomAccessor(child.ID, childRA)
										}
									}

									// Delete the old .tmp file
									deleteErr := tw.fs.h.Delete(tw.fs.c, tw.fs.bktID, child.ID)
									if deleteErr != nil {
										DebugLog("[VFS TempFileWriter Flush] WARNING: Failed to delete existing .tmp file: fileID=%d, name=%s, error=%v", child.ID, child.Name, deleteErr)
									} else {
										DebugLog("[VFS TempFileWriter Flush] Successfully deleted existing .tmp file: fileID=%d, name=%s", child.ID, child.Name)
										// Remove from cache
										fileObjCache.Del(child.ID)
										// Remove from directory cache
										dirNode := &OrcasNode{
											fs:    tw.fs,
											objID: obj.PID,
										}
										dirNode.removeChildFromDirCache(obj.PID, child.ID)
									}
									break
								}
							}
						}
					}

					// For small files, update batch writer instead of directly renaming
					// Small files should be handled by batch writer, not TempFileWriter
					if isSmallFile && config.BatchWriteEnabled {
						// Update batch writer with new name (if file is in batch writer)
						batchMgr := tw.fs.getBatchWriteManager()
						if batchMgr != nil {
							updated := batchMgr.UpdatePendingObject(tw.fileID, func(pkgInfo *sdk.PackagedFileInfo) {
								pkgInfo.Name = newName
								DebugLog("[VFS TempFileWriter Flush] Updated batch writer with new name: fileID=%d, oldName=%s, newName=%s", tw.fileID, originalObjName, newName)
							})
							if updated {
								// File is in batch writer, rename will be handled by batch writer during flush
								DebugLog("[VFS TempFileWriter Flush] Small file in batch writer, rename will be handled during batch flush: fileID=%d, oldName=%s, newName=%s", tw.fileID, originalObjName, newName)
								// Update cache with new name for immediate visibility
								updatedFileObjs, err := tw.fs.h.Get(tw.fs.c, tw.fs.bktID, []int64{tw.fileID})
								if err == nil && len(updatedFileObjs) > 0 {
									updatedObj := updatedFileObjs[0]
									// Temporarily update name in cache (batch writer will update database during flush)
									tempObj := *updatedObj
									tempObj.Name = newName
									fileObjCache.Put(tw.fileID, &tempObj)
									if ra := tw.fs.getRandomAccessorByFileID(tw.fileID); ra != nil {
										ra.fileObj.Store(&tempObj)
									}
								}
								// Don't rename in database yet - batch writer will handle it
								return nil
							}
						}
					}

					// For large files or files not in batch writer, rename directly
					// Use handler's Rename method to rename the file
					renameErr := tw.fs.h.Rename(tw.fs.c, tw.fs.bktID, tw.fileID, newName)
					if renameErr != nil {
						// Check if error is due to existing file with same name
						renameErrStr := renameErr.Error()
						if strings.Contains(renameErrStr, "object with same name already exists") || strings.Contains(renameErrStr, "name already exists") {
							// Target file already exists, merge .tmp file into existing file
							DebugLog("[VFS TempFileWriter Flush] Target file already exists, merging .tmp file into existing file: fileID=%d, oldName=%s, newName=%s", tw.fileID, originalObjName, newName)

							// Find existing file with same name in the same directory
							var existingTargetID int64
							var existingTargetObj *core.ObjectInfo
							if obj.PID > 0 {
								// List directory to find existing file
								children, _, _, listErr := tw.fs.h.List(tw.fs.c, tw.fs.bktID, obj.PID, core.ListOptions{
									Count: core.DefaultListPageSize,
								})
								if listErr == nil {
									for _, child := range children {
										if child.Name == newName && child.Type == core.OBJ_TYPE_FILE {
											existingTargetID = child.ID
											existingTargetObj = child
											break
										}
									}
								}
							}

							if existingTargetID > 0 && existingTargetObj != nil {
								// Get current .tmp file object
								sourceObj := obj

								// Create version for existing file
								versionTime := core.Now()
								newVersion := &core.ObjectInfo{
									ID:     core.NewID(),
									PID:    existingTargetObj.PID,
									Type:   core.OBJ_TYPE_FILE,
									Name:   existingTargetObj.Name,
									DataID: existingTargetObj.DataID,
									Size:   existingTargetObj.Size,
									MTime:  existingTargetObj.MTime,
								}

								// Update existing file with new DataID and Size
								updateTargetFile := &core.ObjectInfo{
									ID:     existingTargetID,
									PID:    existingTargetObj.PID,
									Type:   existingTargetObj.Type,
									Name:   existingTargetObj.Name,
									DataID: sourceObj.DataID,
									Size:   sourceObj.Size,
									MTime:  versionTime,
								}

								// Batch create version and update target file
								if lh, ok := tw.fs.h.(*core.LocalHandler); ok {
									objectsToPut := []*core.ObjectInfo{newVersion, updateTargetFile}
									_, putErr := lh.Put(tw.fs.c, tw.fs.bktID, objectsToPut)
									if putErr == nil {
										DebugLog("[VFS TempFileWriter Flush] Successfully merged .tmp file into existing file: tmpFileID=%d, targetID=%d, versionID=%d, dataID=%d, size=%d",
											tw.fileID, existingTargetID, newVersion.ID, sourceObj.DataID, sourceObj.Size)

										// Delete .tmp file
										deleteErr := tw.fs.h.Delete(tw.fs.c, tw.fs.bktID, tw.fileID)
										if deleteErr == nil {
											DebugLog("[VFS TempFileWriter Flush] Successfully deleted .tmp file after merge: fileID=%d", tw.fileID)
										} else {
											DebugLog("[VFS TempFileWriter Flush] WARNING: Failed to delete .tmp file after merge: fileID=%d, error=%v", tw.fileID, deleteErr)
										}

										// Update cache
										fileObjCache.Put(existingTargetID, updateTargetFile)
										if ra := tw.fs.getRandomAccessorByFileID(existingTargetID); ra != nil {
											ra.fileObj.Store(updateTargetFile)
										}
									} else {
										DebugLog("[VFS TempFileWriter Flush] ERROR: Failed to merge .tmp file into existing file: fileID=%d, targetID=%d, error=%v", tw.fileID, existingTargetID, putErr)
									}
								} else {
									DebugLog("[VFS TempFileWriter Flush] ERROR: Handler is not LocalHandler, cannot merge files: fileID=%d", tw.fileID)
								}
							} else {
								DebugLog("[VFS TempFileWriter Flush] WARNING: Failed to find existing target file: fileID=%d, newName=%s", tw.fileID, newName)
							}
						} else {
							DebugLog("[VFS TempFileWriter Flush] WARNING: Failed to auto-rename .tmp file: fileID=%d, oldName=%s, newName=%s, error=%v", tw.fileID, originalObjName, newName, renameErr)
						}
						// Don't return error - rename failure doesn't mean flush failed
					} else {
						DebugLog("[VFS TempFileWriter Flush] Successfully auto-renamed .tmp file: fileID=%d, oldName=%s, newName=%s", tw.fileID, originalObjName, newName)
						// Update cache with new name
						updatedFileObjs, err := tw.fs.h.Get(tw.fs.c, tw.fs.bktID, []int64{tw.fileID})
						if err == nil && len(updatedFileObjs) > 0 {
							updatedObj := updatedFileObjs[0]
							fileObjCache.Put(tw.fileID, updatedObj)

							// IMPORTANT: Update RandomAccessor's fileObj cache to reflect the new name
							// This ensures that subsequent Write() calls will see the updated name
							// and correctly check isTmpFile (which will be false after rename)
							// However, hasTempWriter check will still ensure TempFileWriter is used
							if ra := tw.fs.getRandomAccessorByFileID(tw.fileID); ra != nil {
								ra.fileObj.Store(updatedObj)
								DebugLog("[VFS TempFileWriter Flush] Updated RandomAccessor's fileObj cache after rename: fileID=%d, newName=%s", tw.fileID, updatedObj.Name)
							}

							// Update directory cache
							if updatedObj.PID > 0 {
								dirNode := &OrcasNode{
									fs:    tw.fs,
									objID: updatedObj.PID,
								}
								dirNode.updateChildInDirCache(updatedObj.PID, updatedObj)
							}
						}
					}
				}
			}
		}
	} else {
		// Chunks still exist, don't rename yet
		DebugLog("[VFS TempFileWriter Flush] Deferring rename until all chunks are flushed: fileID=%d, fileName=%s", tw.fileID, originalObjName)
	}

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
		fileObjKey: fileID, // Pre-compute and cache key
		buffer: &WriteBuffer{
			fileID:     fileID,
			operations: make([]WriteOperation, maxBufferWrites), // Fixed-length array
			writeIndex: 0,                                       // Start from 0
			totalSize:  0,
		},
		lastOffset: 0,
		seqDetector: &ConcurrentSequentialDetector{
			pendingWrites:    make(map[int64]*PendingWrite),
			expectedOffset:   -1,
			blockSize:        0,
			consecutiveCount: 0,
			enabled:          false,
		},
	}

	// Note: RandomAccessor registration is now handled by the caller (getRandomAccessor)
	// to ensure proper synchronization for .tmp files during concurrent writes
	// For non-.tmp files or when called from other places, registration should be done explicitly
	// This prevents race conditions where multiple RandomAccessors are registered for the same file

	return ra, nil
}

// MarkSparseFile marks file as sparse (pre-allocated) for optimization
// This is used when SetAllocationSize is called to pre-allocate space
func (ra *RandomAccessor) MarkSparseFile(size int64) {
	atomic.StoreInt64(&ra.sparseSize, size)
}

// Write adds write operation to buffer
// Optimization: sequential write optimization - if sequential write starting from 0, directly write to data block, avoid caching
// Optimization: for sparse files (pre-allocated), use more aggressive delayed flush to reduce frequent flushes
// For .tmp files, we don't know the final file size until rename (removing .tmp extension),
// so we can't decide whether to use batch write during Write(). Use random write mode first,
// and the decision will be made during Flush() based on final file size.
func (ra *RandomAccessor) Write(offset int64, data []byte) error {
	// Optimization: cache fileObj to avoid repeated getFileObj calls
	// Check local atomic value first (fast path)
	var fileObj *core.ObjectInfo
	fileObjValue := ra.fileObj.Load()
	if fileObjValue != nil {
		if obj, ok := fileObjValue.(*core.ObjectInfo); ok && obj != nil {
			fileObj = obj
		}
	}

	// If not in local cache, get from cache or database (only if needed)
	var err error
	if fileObj == nil {
		fileObj, err = ra.getFileObj()
		if err != nil {
			return err
		}
	}

	// Check if this is a .tmp file
	// IMPORTANT: Also check if TempFileWriter already exists, even if fileObj doesn't show .tmp
	// This handles the case where file was renamed from .tmp but RandomAccessor still has TempFileWriter
	// In this case, we should continue using TempFileWriter to avoid data corruption
	isTmpFile := isTempFile(fileObj)
	// Lock-free check using atomic.Value
	hasTempWriter := ra.tempWriter.Load() != nil

	// For small .tmp files (< batch write threshold), use batch write instead of TempFileWriter
	// This allows small files to be packaged together for better performance
	// Only use TempFileWriter for large files or if TempFileWriter already exists
	config := core.GetWriteBufferConfig()
	batchWriteThreshold := config.MaxBatchWriteFileSize
	if batchWriteThreshold <= 0 {
		batchWriteThreshold = 64 << 10 // Default 64KB
	}

	// Check if file is small enough for batch write
	// For .tmp files, we need to estimate final size from current write
	// If this is the first write and it's small, we can use batch write
	isSmallTmpFile := isTmpFile && !hasTempWriter &&
		offset == 0 && int64(len(data)) < batchWriteThreshold &&
		(fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) &&
		config.BatchWriteEnabled

	if isTmpFile || hasTempWriter {
		if isSmallTmpFile {
			// Small .tmp file, use batch write instead of TempFileWriter
			// Don't create TempFileWriter, let it fall through to random write mode
			// which will handle batch write in flushInternal
			DebugLog("[VFS RandomAccessor Write] Small .tmp file detected, will use batch write: fileID=%d, size=%d, threshold=%d", ra.fileID, len(data), batchWriteThreshold)
		} else {
			// For .tmp files, all writes should go through TempFileWriter
			// This ensures consistent data handling and avoids triggering applyRandomWritesWithSDK
			// Also use TempFileWriter if it already exists (file may have been renamed but writer still active)
			if !isTmpFile && hasTempWriter {
				// File was renamed from .tmp but TempFileWriter still exists
				// This can happen during concurrent writes or if file was renamed while writes are in progress
				DebugLog("[VFS RandomAccessor Write] WARNING: File is not .tmp but TempFileWriter exists, using TempFileWriter: fileID=%d, fileName=%s", ra.fileID, fileObj.Name)
			}
			tw, err := ra.getOrCreateTempWriter()
			if err != nil {
				return fmt.Errorf("failed to get or create TempFileWriter for .tmp file: %w", err)
			}
			return tw.Write(offset, data)
		}
	}

	// Check if in sequential write mode
	if ra.seqBuffer != nil && !ra.seqBuffer.closed {
		// Check if still sequential write (continue from current position)
		if offset == ra.seqBuffer.offset {
			// Sequential write, use optimized path
			return ra.writeSequential(offset, data)
		} else if offset < ra.seqBuffer.offset {
			// Write backwards, switch to random write mode
			if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
				return flushErr
			}
			ra.seqBuffer.closed = true
		} else {
			// Skipped some positions, switch to random write mode
			if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
				return flushErr
			}
			ra.seqBuffer.closed = true
		}
	}

	// Initialize sequential write buffer
	// For small files, check if batch write should be used instead
	// NOTE: This code should not execute for .tmp files (they should have returned above)
	// But we keep the check here for safety in case fileObj cache is stale
	if ra.seqBuffer == nil && len(data) > 0 {
		// Reuse cached fileObj (already loaded above)
		if fileObj != nil {
			// Re-check if this is a .tmp file (fileObj cache might be stale)
			// If TempFileWriter exists, we should not initialize sequential buffer
			// Lock-free check using atomic.Value
			hasTempWriter := ra.tempWriter.Load() != nil
			if hasTempWriter {
				// TempFileWriter exists, this should not happen here (should have returned above)
				// But handle it for safety - don't initialize sequential buffer
				DebugLog("[VFS RandomAccessor Write] WARNING: TempFileWriter exists but reached sequential buffer init, this should not happen: fileID=%d", ra.fileID)
			}
			forceSequential := isTmpFile && !hasTempWriter

			// Check if this is a small file that should use batch write
			// Small files (< 1MB) should use batch write for better performance
			config := core.GetWriteBufferConfig()
			isSmallFile := int64(len(data)) < 1<<20 // 1MB threshold
			shouldUseBatchWrite := isSmallFile && config.BatchWriteEnabled &&
				offset == 0 && (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID)

			if shouldUseBatchWrite {
				// For small files, use batch write instead of sequential write
				// This allows multiple small files to be packaged together
				// Don't initialize sequential buffer, let it fall through to random write mode
				// which will handle batch write in flushInternal
				// DebugLog("[VFS RandomAccessor Write] Small file detected, will use batch write: fileID=%d, size=%d", ra.fileID, len(data))
			} else if offset == 0 && (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) {
				// File has no data, can initialize sequential write buffer
				var initErr error
				if initErr = ra.initSequentialBuffer(forceSequential); initErr == nil {
					// Initialization succeeded, use sequential write
					return ra.writeSequential(offset, data)
				}
				// Initialization failed, fallback to random write
			} else if forceSequential {
				// Try to resume sequential writes backed by writing version
				var initErr error
				if initErr = ra.initSequentialBuffer(true); initErr == nil && ra.seqBuffer != nil && !ra.seqBuffer.closed {
					if offset == ra.seqBuffer.offset {
						return ra.writeSequential(offset, data)
					}
					// Offset mismatch will be handled by concurrent sequential detector
				}
			}
		}
	}

	// Random write mode: use original buffer logic
	// IMPORTANT: Double-check that TempFileWriter doesn't exist before using random write buffer
	// This prevents .tmp files from accidentally using random write when fileObj cache is stale
	// Lock-free check using atomic.Value
	hasTempWriterInRandomMode := ra.tempWriter.Load() != nil
	if hasTempWriterInRandomMode {
		// TempFileWriter exists, this should not happen (should have returned above)
		// But handle it for safety - use TempFileWriter instead of random write buffer
		DebugLog("[VFS RandomAccessor Write] WARNING: TempFileWriter exists but reached random write mode, using TempFileWriter: fileID=%d", ra.fileID)
		tw, err := ra.getOrCreateTempWriter()
		if err != nil {
			return fmt.Errorf("failed to get or create TempFileWriter: %w", err)
		}
		return tw.Write(offset, data)
	}

	// Optimization: reduce data copying, only copy when necessary
	// Check if exceeds capacity (optimized: check early to avoid out of bounds)
	// Note: config was already declared above for batch write check
	// config := core.GetWriteBufferConfig() // Already declared above

	// Detect sequential write pattern (even if file already has data)
	// Sequential writes are writes that continue from the last write position
	lastOffset := atomic.LoadInt64(&ra.lastOffset)
	isSequentialWrite := false
	if lastOffset >= 0 {
		// Check if this write continues from the last write position
		if offset == lastOffset {
			isSequentialWrite = true
		}
	} else {
		// First write, check if it starts from file end (append mode)
		// Reuse cached fileObj (already loaded above)
		if fileObj == nil {
			fileObj, err = ra.getFileObj()
			if err != nil {
				return err
			}
		}
		if offset == fileObj.Size {
			isSequentialWrite = true
		}
	}

	// Aggressive optimization: for sparse files (pre-allocated), use larger buffer threshold to reduce flush frequency
	// This is critical for qBittorrent random write performance
	// For small files, use larger buffer to allow more batching (balanced approach)
	sparseSize := atomic.LoadInt64(&ra.sparseSize)
	isSparseFile := sparseSize > 0
	maxBufferSize := config.MaxBufferSize

	// Check if this is a small file (likely to use batch write)
	// Balanced aggressive: use larger buffer for small files to allow more operations before flush
	isSmallFile := fileObj != nil && fileObj.Size > 0 && fileObj.Size < 1536<<10 // 1.5MB threshold

	if isSparseFile {
		// For sparse files, allow larger buffer (2x) to reduce flush frequency
		// This significantly improves performance for random writes
		maxBufferSize = config.MaxBufferSize * 2
	} else if isSequentialWrite {
		// Balanced aggressive: For sequential writes, use larger buffer (4x for small files, 3x for others)
		// This allows more small files to accumulate before flushing, reducing database operations
		if isSmallFile {
			maxBufferSize = config.MaxBufferSize * 4 // Aggressive for small files
		} else {
			maxBufferSize = config.MaxBufferSize * 3
		}
	} else if isSmallFile {
		// Balanced aggressive: For small files even in random write mode, use larger buffer (3x)
		// This allows more operations to accumulate before flush
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
	// Optimization: reuse existing Data slice if capacity is sufficient (reduce allocations for fragmented writes)
	ra.buffer.operations[writeIndex].Offset = offset
	if cap(ra.buffer.operations[writeIndex].Data) >= len(data) {
		// Reuse existing slice if capacity is sufficient
		ra.buffer.operations[writeIndex].Data = ra.buffer.operations[writeIndex].Data[:len(data)]
		copy(ra.buffer.operations[writeIndex].Data, data)
	} else {
		// Allocate new slice only if capacity is insufficient
		ra.buffer.operations[writeIndex].Data = make([]byte, len(data))
		copy(ra.buffer.operations[writeIndex].Data, data)
	}

	// Update last write offset for sequential write detection
	atomic.StoreInt64(&ra.lastOffset, offset+int64(len(data)))

	// Aggressive optimization: for small file writes, use batch write manager
	// If data size is small and hasn't reached force flush condition, add to batch write manager
	// Note: batch write only applies to small files, and needs to ensure data integrity
	// Here use delayed flush first, batch write logic is handled in Flush
	// Aggressive: reduce delayed flush scheduling frequency to allow more batching
	// Only schedule if buffer is getting quite full (80%+) or it's a sequential write with significant data (70%+)
	// This allows more small files to accumulate before flushing, reducing database operations
	bufferUsage := float64(atomic.LoadInt64(&ra.buffer.totalSize)) / float64(maxBufferSize)
	if isSequentialWrite && bufferUsage > 0.7 {
		// Sequential write with significant buffer usage, schedule delayed flush
		ra.requestDelayedFlush(false)
	} else if !isSequentialWrite && bufferUsage > 0.85 {
		// Fragmented write, schedule when buffer is 85% full (more aggressive than before)
		ra.requestDelayedFlush(false)
	}

	return nil
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
	bucket := getBucketConfigWithCache(ra.fs)
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

	bucket := getBucketConfigWithCache(ra.fs)
	chunkData := ra.seqBuffer.buffer

	// Process first chunk: check file type and compression effect
	isFirstChunk := ra.seqBuffer.sn == 0
	if isFirstChunk && bucket != nil && bucket.CmprWay > 0 && len(chunkData) > 0 {
		kind, _ := filetype.Match(chunkData)
		if kind != filetype.Unknown {
			// Not unknown type, don't compress
			ra.seqBuffer.dataInfo.Kind &= ^core.DATA_CMPR_MASK
			DebugLog("[VFS flushSequentialChunk] Not unknown type, don't compress: fileID=%d, dataID=%d, sn=%d, Kind=0x%x", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, ra.seqBuffer.dataInfo.Kind)
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
					DebugLog("[VFS flushSequentialChunk] Compression failed, remove compression flag: fileID=%d, dataID=%d, sn=%d, Kind=0x%x", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, ra.seqBuffer.dataInfo.Kind)
				}
				processedChunk = chunkData
			} else {
				// If compressed size is larger or equal, only remove compression flag on first chunk
				if isFirstChunk && cmprBuf.Len() >= len(chunkData) {
					processedChunk = chunkData
					ra.seqBuffer.dataInfo.Kind &= ^core.DATA_CMPR_MASK
					DebugLog("[VFS flushSequentialChunk] Compressed size is larger or equal, remove compression flag: fileID=%d, dataID=%d, sn=%d, Kind=0x%x", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, ra.seqBuffer.dataInfo.Kind)
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
	// If sn > 0, it means at least one chunk (typically 10MB) has been written, so it's not a small file
	// Use buffer length as total size if OrigSize hasn't been updated yet
	bufferSize := int64(len(ra.seqBuffer.buffer))
	totalSize := ra.seqBuffer.dataInfo.OrigSize
	if totalSize == 0 && bufferSize > 0 {
		// OrigSize not updated yet, use buffer size
		totalSize = bufferSize
	}
	if totalSize > 0 && totalSize < 1<<20 && ra.seqBuffer.sn == 0 {
		// Small file and all data is still in buffer (no chunks written yet)
		// Try instant upload first (before batch write) if enabled
		allData := ra.seqBuffer.buffer
		if len(allData) > 0 {
			// Create unified config from bucket config
			var instantUploadCfg *core.InstantUploadConfig
			bucket := getBucketConfigWithCache(ra.fs)
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
							// Update directory listing cache to ensure file is visible in Readdir (consistent with direct write path)
							if updateFileObj.PID > 0 {
								dirNode := &OrcasNode{
									fs:    ra.fs,
									objID: updateFileObj.PID,
								}
								dirNode.appendChildToDirCache(updateFileObj.PID, updateFileObj)
								DebugLog("[VFS RandomAccessor flushSequentialBuffer] Appended file to directory listing cache (instant upload): fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
							}
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
				// If batch write failed (buffer full), flush and retry
				// This is the only case where we flush: when buffer is full
				if !added {
					batchMgr.FlushAll(ra.fs.c)
					DebugLog("[VFS flushSequentialBuffer] Buffer full, flushed all pending writes")
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
	dataInfoCache.Put(ra.seqBuffer.dataID, ra.seqBuffer.dataInfo)
	fileObjCache.Put(ra.fileObjKey, fileObj)
	ra.fileObj.Store(fileObj)

	// After sync flush, append file to directory listing cache
	// This ensures the file is immediately visible in Readdir
	if fileObj.PID > 0 {
		dirNode := &OrcasNode{
			fs:    ra.fs,
			objID: fileObj.PID,
		}
		dirNode.appendChildToDirCache(fileObj.PID, fileObj)
		DebugLog("[VFS flushSequentialBuffer] Appended file to directory listing cache after sync flush: fileID=%d, dirID=%d, name=%s", ra.fileID, fileObj.PID, fileObj.Name)
	}

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

	// Check if file is in batch writer buffer (not yet flushed to database)
	// If so, use OrigSize from batch writer for size check
	var actualFileSize int64 = fileObj.Size
	var isInBatchWriter bool
	batchMgr := ra.fs.getBatchWriteManager()
	if batchMgr != nil {
		if pkgInfo, isPending := batchMgr.GetPendingObject(ra.fileID); isPending {
			// File is in batch writer, use OrigSize from batch writer
			actualFileSize = pkgInfo.OrigSize
			isInBatchWriter = true
			// DebugLog("[VFS Read] File in batch writer, using OrigSize: fileID=%d, OrigSize=%d, fileObj.Size=%d", ra.fileID, pkgInfo.OrigSize, fileObj.Size)
		}
	}

	// If file has no DataID and is not in batch writer, check buffer first
	// Buffer may have data even if fileObj.Size is 0
	if (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) && !isInBatchWriter {
		// Check if buffer has data
		writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
		if writeIndex > 0 {
			// Buffer has data, allow reading from buffer even if fileObj.Size is 0
			// Size will be calculated from buffer operations
			// DebugLog("[VFS Read] File has no DataID and buffer has data, reading from buffer: fileID=%d, writeIndex=%d", ra.fileID, writeIndex)
			return ra.readFromBuffer(offset, size), nil
		}
	}

	// Limit reading size to file size (only if we have a valid size)
	if actualFileSize > 0 {
		if offset >= actualFileSize {
			// DebugLog("[VFS Read] offset >= size: fileID=%d, offset=%d, size=%d", ra.fileID, offset, actualFileSize)
			return []byte{}, nil
		}
		if int64(size) > actualFileSize-offset {
			size = int(actualFileSize - offset)
		}
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

	// If no data ID, check batch writer buffer first (file not yet flushed to database)
	if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
		// Check if file is in batch writer buffer
		if batchMgr != nil {
			if pkgInfo, isPending := batchMgr.GetPendingObject(ra.fileID); isPending {
				// File is in batch writer buffer, read from batch writer's buffer
				// Get processed data from batch writer buffer
				// DebugLog("[VFS Read] File in batch writer buffer (no DataID): fileID=%d, OrigSize=%d, Size=%d", ra.fileID, pkgInfo.OrigSize, pkgInfo.Size)
				processedData := batchMgr.ReadPendingData(ra.fileID)
				// DebugLog("[VFS Read] ReadPendingData returned: fileID=%d, dataLen=%d", ra.fileID, len(processedData))
				if len(processedData) > 0 {
					// Decode processed data (decompress/decrypt)
					// Get kind from bucket configuration (same as used during processing)
					bucket := getBucketConfigWithCache(ra.fs)
					kind := uint32(0)
					if bucket != nil {
						if bucket.CmprWay > 0 {
							kind |= bucket.CmprWay
						}
						if bucket.EndecWay > 0 {
							kind |= bucket.EndecWay
						}
					}
					decodedData, decodeErr := ra.decodeProcessedData(processedData, kind, pkgInfo.OrigSize)
					if decodeErr == nil && len(decodedData) > 0 {
						// Merge with buffer writes and return
						return ra.readFromDataAndBuffer(decodedData, offset, size), nil
					}
				}
			}
		}
		// If no DataID and not in batch writer, read from buffer only
		return ra.readFromBuffer(offset, size), nil
	}

	// File has DataID, should read from database (not from batch writer buffer)
	// Even if file is still in batch writer cache, database has the authoritative data

	// Get DataInfo
	dataInfoCacheKey := fileObj.DataID
	var dataInfo *core.DataInfo
	if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
		if info, ok := cached.(*core.DataInfo); ok && info != nil {
			// Verify cached DataInfo matches file size (for files that may have been updated)
			// If file size changed, invalidate cache and re-fetch
			if info.OrigSize != fileObj.Size {
				// DataInfo cache is stale, invalidate it
				dataInfoCache.Del(dataInfoCacheKey)
				// Also invalidate decodingReaderCache to ensure chunkReader uses fresh DataInfo
				decodingReaderCache.Del(dataInfoCacheKey)
				dataInfo = nil
			} else {
				dataInfo = info
			}
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
			// If direct read also fails, try reading from batch writer as fallback
			// This handles the case where file has DataID but database hasn't been updated yet
			if batchMgr != nil {
				if pkgInfo, isPending := batchMgr.GetPendingObject(ra.fileID); isPending {
					// File is in batch writer, read from batch writer's buffer
					// DebugLog("[VFS Read] Database read failed, falling back to batch writer: fileID=%d, DataID=%d", ra.fileID, fileObj.DataID)
					processedData := batchMgr.ReadPendingData(ra.fileID)
					if len(processedData) > 0 {
						// Decode processed data (decompress/decrypt)
						bucket := getBucketConfigWithCache(ra.fs)
						kind := uint32(0)
						if bucket != nil {
							if bucket.CmprWay > 0 {
								kind |= bucket.CmprWay
							}
							if bucket.EndecWay > 0 {
								kind |= bucket.EndecWay
							}
						}
						decodedData, decodeErr := ra.decodeProcessedData(processedData, kind, pkgInfo.OrigSize)
						if decodeErr == nil && len(decodedData) > 0 {
							// Merge with buffer writes and return
							return ra.readFromDataAndBuffer(decodedData, offset, size), nil
						}
					}
				}
			}
			return ra.readFromBuffer(offset, size), nil
		}
		// Update cache
		dataInfoCache.Put(dataInfoCacheKey, dataInfo)
	}

	// Debug: Log DataInfo details
	// Verify DataInfo OrigSize matches file size (critical for correct reading)
	DebugLog("[VFS Read] DataInfo check: fileID=%d, DataID=%d, DataInfo.OrigSize=%d, fileObj.Size=%d",
		ra.fileID, dataInfo.ID, dataInfo.OrigSize, fileObj.Size)
	if dataInfo.OrigSize != fileObj.Size {
		// DataInfo OrigSize doesn't match file size, this is a problem
		DebugLog("[VFS Read] WARNING: DataInfo OrigSize mismatch, re-fetching: fileID=%d, DataID=%d, DataInfo.OrigSize=%d, fileObj.Size=%d",
			ra.fileID, dataInfo.ID, dataInfo.OrigSize, fileObj.Size)
		// Invalidate cache and re-fetch DataInfo
		dataInfoCache.Del(dataInfoCacheKey)
		decodingReaderCache.Del(dataInfoCacheKey)
		// Re-fetch DataInfo from database
		dataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
		if err != nil {
			DebugLog("[VFS Read] ERROR: Failed to re-fetch DataInfo: fileID=%d, DataID=%d, error=%v", ra.fileID, fileObj.DataID, err)
			return nil, fmt.Errorf("failed to re-fetch DataInfo after size mismatch: %w", err)
		}
		if dataInfo != nil {
			DebugLog("[VFS Read] Re-fetched DataInfo: fileID=%d, DataID=%d, OrigSize=%d, Size=%d",
				ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size)
			dataInfoCache.Put(dataInfoCacheKey, dataInfo)
		}
	}
	DebugLog("[VFS Read] DataInfo: fileID=%d, DataID=%d, OrigSize=%d, Size=%d, Kind=0x%x, PkgID=%d, PkgOffset=%d",
		ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size, dataInfo.Kind, dataInfo.PkgID, dataInfo.PkgOffset)

	// Always use bucket's default chunk size (force unified chunkSize)
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Use unified chunkReader for both plain and compressed/encrypted data
	var endecKey string
	bucket := getBucketConfigWithCache(ra.fs)
	if bucket != nil {
		endecKey = bucket.EndecKey
		// DebugLog("[VFS Read] Bucket config: fileID=%d, CmprWay=%d, EndecWay=%d, endecKey length=%d",
		//	ra.fileID, bucket.CmprWay, bucket.EndecWay, len(endecKey))
	}

	// Create data reader (abstract read interface, unified handling of uncompressed and compressed/encrypted data)
	// Always create a new chunkReader to ensure it uses the latest DataInfo
	// (don't reuse cached reader as it may have stale origSize)
	reader := newChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, dataInfo, endecKey, chunkSize)
	DebugLog("[VFS Read] Created chunkReader: fileID=%d, dataID=%d, origSize=%d, chunkSize=%d, kind=0x%x, endecKey length=%d",
		ra.fileID, reader.dataID, reader.origSize, reader.chunkSize, reader.kind, len(reader.endecKey))

	// Unified read logic (includes merging write operations)
	DebugLog("[VFS Read] Starting readWithWrites: fileID=%d, offset=%d, size=%d, origSize=%d",
		ra.fileID, offset, size, reader.origSize)
	fileData, operationsHandled := ra.readWithWrites(reader, offset, size)
	if operationsHandled {
		DebugLog("[VFS Read] readWithWrites completed: fileID=%d, readSize=%d, requested=%d",
			ra.fileID, len(fileData), size)
		return fileData, nil
	}
	DebugLog("[VFS Read] readWithWrites returned false, trying fallback: fileID=%d", ra.fileID)

	// If read fails or write operations not handled, try batch writer as fallback
	// This handles the case where file has DataID but database read fails
	if batchMgr != nil {
		if pkgInfo, isPending := batchMgr.GetPendingObject(ra.fileID); isPending {
			// File is in batch writer, read from batch writer's buffer
			// DebugLog("[VFS Read] Database read failed, falling back to batch writer: fileID=%d, DataID=%d", ra.fileID, fileObj.DataID)
			processedData := batchMgr.ReadPendingData(ra.fileID)
			if len(processedData) > 0 {
				// Decode processed data (decompress/decrypt)
				bucket := getBucketConfigWithCache(ra.fs)
				kind := uint32(0)
				if bucket != nil {
					if bucket.CmprWay > 0 {
						kind |= bucket.CmprWay
					}
					if bucket.EndecWay > 0 {
						kind |= bucket.EndecWay
					}
				}
				decodedData, decodeErr := ra.decodeProcessedData(processedData, kind, pkgInfo.OrigSize)
				if decodeErr == nil && len(decodedData) > 0 {
					// Merge with buffer writes and return
					return ra.readFromDataAndBuffer(decodedData, offset, size), nil
				}
			}
		}
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

// decodeProcessedData decodes processed data (decompress/decrypt) based on kind
// This is used to decode data from batch writer buffer before reading
func (ra *RandomAccessor) decodeProcessedData(processedData []byte, kind uint32, origSize int64) ([]byte, error) {
	if len(processedData) == 0 {
		return processedData, nil
	}

	// Get encryption key from bucket configuration
	bucket := getBucketConfigWithCache(ra.fs)
	endecKey := ""
	if bucket != nil {
		endecKey = bucket.EndecKey
	}

	// 1. Decrypt first (if enabled)
	// Important: decrypt first, then decompress (same order as encoding)
	decodeBuf := processedData
	var err error
	if kind&core.DATA_ENDEC_AES256 != 0 {
		// AES256 decryption
		if endecKey == "" {
			return nil, fmt.Errorf("AES256 decryption requires encryption key but key is empty")
		}
		decodeBuf, err = aes256.Decrypt(endecKey, processedData)
		if err != nil {
			return nil, fmt.Errorf("AES256 decryption failed: %v", err)
		}
	} else if kind&core.DATA_ENDEC_SM4 != 0 {
		// SM4 decryption
		if endecKey == "" {
			return nil, fmt.Errorf("SM4 decryption requires encryption key but key is empty")
		}
		decodeBuf, err = sm4.Sm4Cbc([]byte(endecKey), processedData, false) // sm4Cbc mode PKCS7 padding decryption
		if err != nil {
			return nil, fmt.Errorf("SM4 decryption failed: %v", err)
		}
	}

	// 2. Decompress next (if enabled)
	finalBuf := decodeBuf
	if kind&core.DATA_CMPR_MASK != 0 {
		var decompressor archiver.Decompressor
		if kind&core.DATA_CMPR_SNAPPY != 0 {
			decompressor = &archiver.Snappy{}
		} else if kind&core.DATA_CMPR_ZSTD != 0 {
			decompressor = &archiver.Zstd{}
		} else if kind&core.DATA_CMPR_GZIP != 0 {
			decompressor = &archiver.Gz{}
		} else if kind&core.DATA_CMPR_BR != 0 {
			decompressor = &archiver.Brotli{}
		}

		if decompressor != nil {
			var decompressedBuf bytes.Buffer
			err := decompressor.Decompress(bytes.NewReader(decodeBuf), &decompressedBuf)
			if err != nil {
				// Decompression failed, use decrypted data (compressed data may be corrupted or wrong format)
				return nil, fmt.Errorf("decompression failed: %v", err)
			}
			finalBuf = decompressedBuf.Bytes()
		}
	}

	return finalBuf, nil
}

func (ra *RandomAccessor) requestDelayedFlush(force bool) {
	if ra == nil {
		return
	}
	atomic.StoreInt64(&ra.lastActivity, core.Now())
	getDelayedFlushManager().schedule(ra, force)
}

func (ra *RandomAccessor) cancelDelayedFlush() {
	if ra == nil {
		return
	}
	getDelayedFlushManager().cancel(ra)
}

func (ra *RandomAccessor) executeDelayedFlush(force bool) {
	if ra == nil {
		return
	}
	if force {
		if _, err := ra.ForceFlush(); err != nil {
			DebugLog("[VFS RandomAccessor] ERROR: Force flush during delayed schedule failed: fileID=%d, error=%v", ra.fileID, err)
		}
		return
	}
	if _, err := ra.Flush(); err != nil {
		DebugLog("[VFS RandomAccessor] ERROR: Flush during delayed schedule failed: fileID=%d, error=%v", ra.fileID, err)
	}
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
	// Ensure any pending delayed flush entry is cleared before flushing now
	ra.cancelDelayedFlush()

	DebugLog("[VFS RandomAccessor Flush] Starting flush: fileID=%d, force=%v", ra.fileID, force)

	// Note: We do NOT flush batch writer here even if force=true
	// FlushAll is only called when buffer is full (AddFile returns false) or by periodic timer
	// If file is in batch writer, it will be flushed automatically when buffer is full or timer expires

	// For .tmp files, check final file size before flushing TempFileWriter
	// If file is small, we can use batch write instead of TempFileWriter
	fileObj, err := ra.getFileObj()
	isTmpFile := err == nil && fileObj != nil && isTempFile(fileObj)
	if isTmpFile {
		// For .tmp files, check if we should use batch write based on final file size
		// Calculate final file size from buffer operations (before swapping)
		writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
		totalSize := atomic.LoadInt64(&ra.buffer.totalSize)

		// Check if TempFileWriter has data to flush
		// This is important because large files use TempFileWriter, and data might be
		// in TempFileWriter but not in RandomAccessor's buffer (writeIndex=0, totalSize=0)
		hasTempWriterData := false
		// Lock-free check using atomic.Value
		if val := ra.tempWriter.Load(); val != nil {
			if tw, ok := val.(*TempFileWriter); ok && tw != nil {
				// Check if TempFileWriter has data (size > 0)
				twSize := atomic.LoadInt64(&tw.size)
				hasTempWriterData = twSize > 0
				DebugLog("[VFS RandomAccessor Flush] .tmp file with TempFileWriter: fileID=%d, twSize=%d, writeIndex=%d, totalSize=%d", ra.fileID, twSize, writeIndex, totalSize)
			}
		}

		// If file has no DataID and is small (< batch write threshold), we can use batch write
		// Otherwise, use TempFileWriter (for large files or files that already have DataID)
		config := core.GetWriteBufferConfig()
		batchWriteThreshold := config.MaxBatchWriteFileSize
		if batchWriteThreshold <= 0 {
			batchWriteThreshold = 64 << 10 // Default 64KB
		}
		isSmallFile := totalSize > 0 && totalSize < batchWriteThreshold
		shouldUseBatchWrite := isSmallFile && config.BatchWriteEnabled &&
			(fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) &&
			writeIndex > 0 && // Has pending writes in buffer
			!hasTempWriterData // TempFileWriter should not have data if using batch write

		if !shouldUseBatchWrite {
			// Large .tmp file or already has DataID, or TempFileWriter has data, use TempFileWriter
			if hasTempWriterData || fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
				// TempFileWriter has data or file has no DataID, flush it
				if err := ra.flushTempFileWriter(); err != nil {
					return 0, err
				}
				// After flushing TempFileWriter, re-fetch fileObj to get updated DataID and Size
				// This ensures we have the latest information after flush
				fileObj, err = ra.getFileObj()
				if err == nil && fileObj != nil {
					// Re-fetch from database to get latest DataID and Size
					objs, err := ra.fs.h.Get(ra.fs.c, ra.fs.bktID, []int64{ra.fileID})
					if err == nil && len(objs) > 0 {
						updatedFileObj := objs[0]
						ra.fileObj.Store(updatedFileObj)
						fileObjCache.Put(ra.fileObjKey, updatedFileObj)
						DebugLog("[VFS RandomAccessor Flush] Updated fileObj after TempFileWriter flush: fileID=%d, dataID=%d, size=%d", ra.fileID, updatedFileObj.DataID, updatedFileObj.Size)
						fileObj = updatedFileObj
					}
				}
			}
		} else {
			// Small .tmp file without DataID, skip TempFileWriter and use batch write
			// The batch write will be handled in the code below
			// DebugLog("[VFS RandomAccessor Flush] Small .tmp file detected, will use batch write instead of TempFileWriter: fileID=%d, size=%d", ra.fileID, totalSize)
		}
	} else {
		// Not a .tmp file, flush TempFileWriter if exists
		if err := ra.flushTempFileWriter(); err != nil {
			return 0, err
		}
		// After flushing TempFileWriter, re-fetch fileObj to get updated DataID and Size
		// This ensures we have the latest information after flush
		fileObj, err = ra.getFileObj()
		if err == nil && fileObj != nil {
			// Re-fetch from database to get latest DataID and Size
			objs, err := ra.fs.h.Get(ra.fs.c, ra.fs.bktID, []int64{ra.fileID})
			if err == nil && len(objs) > 0 {
				updatedFileObj := objs[0]
				ra.fileObj.Store(updatedFileObj)
				fileObjCache.Put(ra.fileObjKey, updatedFileObj)
				DebugLog("[VFS RandomAccessor Flush] Updated fileObj after TempFileWriter flush (non-tmp): fileID=%d, dataID=%d, size=%d", ra.fileID, updatedFileObj.DataID, updatedFileObj.Size)
				fileObj = updatedFileObj
				// Ensure directory cache is updated (TempFileWriter.Flush() should have done this, but double-check)
				if updatedFileObj.PID > 0 {
					dirNode := &OrcasNode{
						fs:    ra.fs,
						objID: updatedFileObj.PID,
					}
					dirNode.appendChildToDirCache(updatedFileObj.PID, updatedFileObj)
					DebugLog("[VFS RandomAccessor Flush] Appended file to directory listing cache after TempFileWriter flush (non-tmp): fileID=%d, dirID=%d, name=%s", ra.fileID, updatedFileObj.PID, updatedFileObj.Name)
				}
			}
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

	// For .tmp files, check if we should use batch write or TempFileWriter
	// Small .tmp files (< batch write threshold) should use batch write
	// Large .tmp files should use TempFileWriter
	if isTmpFile {
		// Check if TempFileWriter exists (lock-free check using atomic.Value)
		hasTempWriter := ra.tempWriter.Load() != nil

		if hasTempWriter {
			// TempFileWriter exists, check if there are any pending writes in buffer
			writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
			if writeIndex > 0 {
				DebugLog("[VFS RandomAccessor Flush] WARNING: .tmp file has pending writes in buffer but TempFileWriter exists: fileID=%d, writeIndex=%d. This should not happen - all writes should go through TempFileWriter. Clearing buffer.", ra.fileID, writeIndex)
				// Clear the buffer to avoid processing stale writes
				atomic.StoreInt64(&ra.buffer.writeIndex, 0)
				atomic.StoreInt64(&ra.buffer.totalSize, 0)
			}
			// For .tmp files, after TempFileWriter flush, return new version ID
			if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
				return core.NewID(), nil
			}
			return 0, nil
		}
		// TempFileWriter doesn't exist - this is a small .tmp file using batch write
		// Continue to process buffer writes below, which will add to batch write manager
		DebugLog("[VFS RandomAccessor Flush] Small .tmp file without TempFileWriter, will use batch write: fileID=%d", ra.fileID)
	}

	// Optimization: use atomic operation to get and clear operations (lock-free)
	// Atomically swap writeIndex and reset to 0, get actual operation count
	writeIndex := atomic.SwapInt64(&ra.buffer.writeIndex, 0)
	totalSize := atomic.SwapInt64(&ra.buffer.totalSize, 0)
	DebugLog("[VFS RandomAccessor Flush] Buffer stats: fileID=%d, writeIndex=%d, totalSize=%d", ra.fileID, writeIndex, totalSize)
	if writeIndex <= 0 {
		DebugLog("[VFS RandomAccessor Flush] No pending writes: fileID=%d", ra.fileID)
		// No pending writes, but check if file has DataID (from TempFileWriter or sequential buffer)
		fileObj, err := ra.getFileObj()
		if err == nil && fileObj != nil {
			if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
				// File has DataID, return new version ID
				return core.NewID(), nil
			}
		}
		return 0, nil
	}

	// Reset lastOffset after flush to allow detection of new sequential write pattern
	atomic.StoreInt64(&ra.lastOffset, -1)

	// Check if it's a small file, suitable for batch write
	// When force=true, still allow batch write but flush immediately
	// Use batch write threshold (default 64KB) instead of hardcoded 1MB
	config := core.GetWriteBufferConfig()
	batchWriteThreshold := config.MaxBatchWriteFileSize
	if batchWriteThreshold <= 0 {
		batchWriteThreshold = 64 << 10 // Default 64KB
	}
	if totalSize > 0 && totalSize < batchWriteThreshold {
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
			// Note: We do NOT flush batch writer here even if force=true
			// FlushAll is only called when buffer is full (AddFile returns false) or by periodic timer
			// No existing data, can use BatchWriter
			// Optimization: reuse operations slice from pool if possible
			// Now copy operations (before swapping)
			// Optimization: avoid copying Data slices if operations are already sequential
			// For sequential writes, we can directly use the buffer operations
			// Optimization: use object pool for operations slice to reduce allocations
			operations := writeOpsPool.Get().([]WriteOperation)
			if cap(operations) < int(writeIndex) {
				operations = make([]WriteOperation, writeIndex)
			} else {
				operations = operations[:writeIndex]
			}

			// Check if operations are sequential (common case for fragmented io.Copy)
			isSequential := true
			for i := int64(1); i < writeIndex; i++ {
				prevEnd := ra.buffer.operations[i-1].Offset + int64(len(ra.buffer.operations[i-1].Data))
				if ra.buffer.operations[i].Offset != prevEnd {
					isSequential = false
					break
				}
			}

			// Optimization: for sequential operations, avoid copying Data slices
			if isSequential && writeIndex > 0 {
				// Sequential operations, can reuse Data slices directly
				for i := int64(0); i < writeIndex; i++ {
					operations[i].Offset = ra.buffer.operations[i].Offset
					operations[i].Data = ra.buffer.operations[i].Data // Reuse slice
				}
			} else {
				// Non-sequential, need to copy
				for i := int64(0); i < writeIndex; i++ {
					operations[i].Offset = ra.buffer.operations[i].Offset
					// Reuse Data slice if capacity is sufficient (for fragmented writes)
					if cap(ra.buffer.operations[i].Data) >= len(ra.buffer.operations[i].Data) {
						operations[i].Data = ra.buffer.operations[i].Data
					} else {
						operations[i].Data = make([]byte, len(ra.buffer.operations[i].Data))
						copy(operations[i].Data, ra.buffer.operations[i].Data)
					}
				}
			}
			mergedOps := mergeWriteOperations(operations)

			// Return operations slice to pool after merging (optimization: reduce allocations)
			writeOpsPool.Put(operations[:0])

			// Calculate total size of merged data
			var mergedDataSize int64
			for _, op := range mergedOps {
				if op.Offset+int64(len(op.Data)) > mergedDataSize {
					mergedDataSize = op.Offset + int64(len(op.Data))
				}
			}

			// If data is continuous or can be merged into continuous data, use batch write
			// Aggressive optimization: for fragmented writes (many small operations), allow more operations if they're sequential
			// For small files (< 1.5MB), be more aggressive with batch write to reduce database operations
			maxOpsForBatchWrite := 15                     // Increased from 10 for small files (balanced)
			maxFileSizeForBatchWrite := int64(1536 << 10) // 1.5MB - balanced increase from 1MB

			if len(mergedOps) > 1 {
				// Check if operations are mostly sequential (common for fragmented io.Copy)
				sequentialCount := 0
				for i := 1; i < len(mergedOps); i++ {
					prevEnd := mergedOps[i-1].Offset + int64(len(mergedOps[i-1].Data))
					if mergedOps[i].Offset == prevEnd || mergedOps[i].Offset == mergedOps[i-1].Offset {
						sequentialCount++
					}
				}
				// Balanced aggressive: If most operations are sequential, allow more operations (up to 100 for very sequential writes)
				sequentialRatio := float64(sequentialCount) / float64(len(mergedOps)-1)
				if sequentialRatio > 0.9 {
					// Very sequential (>90%), allow up to 100 operations
					maxOpsForBatchWrite = 100
				} else if sequentialRatio > 0.8 {
					// Mostly sequential (>80%), allow up to 75 operations
					maxOpsForBatchWrite = 75
				} else if sequentialRatio > 0.6 {
					// Somewhat sequential (>60%), allow up to 50 operations
					maxOpsForBatchWrite = 50
				}
			}

			// Balanced: For very small files (< 64KB), allow more operations
			if mergedDataSize < 64<<10 && len(mergedOps) <= 75 {
				maxOpsForBatchWrite = 75
			}

			if mergedDataSize < maxFileSizeForBatchWrite && len(mergedOps) <= maxOpsForBatchWrite { // Small file and reasonable number of write operations
				// Check if file is already in BatchWriter's pending objects
				// If so, flush BatchWriter immediately to ensure data is persisted (especially when force=true)
				if isPendingInBatchWriter {
					if force {
						// File is already in BatchWriter and force=true, flush immediately to ensure data is persisted
						DebugLog("[VFS RandomAccessor Flush] File is in BatchWriter and force=true, flushing BatchWriter immediately: fileID=%d", ra.fileID)
						batchMgr.FlushAll(ra.fs.c)
						// Wait a bit for flush to complete
						time.Sleep(50 * time.Millisecond)
						// Re-fetch file object to get updated DataID
						fileObj, err = ra.getFileObj()
						if err == nil && fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
							// Update cache with fresh data
							cacheKey := ra.fileID
							fileObjCache.Put(cacheKey, fileObj)
							DebugLog("[VFS RandomAccessor Flush] File flushed from BatchWriter: fileID=%d, dataID=%d, size=%d", ra.fileID, fileObj.DataID, fileObj.Size)
							return core.NewID(), nil
						}
						// If DataID is still not available, fall through to normal write path
						DebugLog("[VFS RandomAccessor Flush] File still has no DataID after BatchWriter flush, falling back to normal write path: fileID=%d", ra.fileID)
					} else {
						// File is already in BatchWriter, but force=false, still flush to ensure data is persisted
						// This is needed for test cases that expect data to be available after Flush()
						DebugLog("[VFS RandomAccessor Flush] File is in BatchWriter, flushing BatchWriter: fileID=%d", ra.fileID)
						atomic.StoreInt64(&ra.buffer.totalSize, 0)
						batchMgr.FlushAll(ra.fs.c)
						// Wait a bit for flush to complete
						time.Sleep(50 * time.Millisecond)
						// Re-fetch file object to get updated DataID
						fileObj, err = ra.getFileObj()
						if err == nil && fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
							// Update cache with fresh data
							cacheKey := ra.fileID
							fileObjCache.Put(cacheKey, fileObj)
							DebugLog("[VFS RandomAccessor Flush] File flushed from BatchWriter: fileID=%d, dataID=%d, size=%d", ra.fileID, fileObj.DataID, fileObj.Size)
							return core.NewID(), nil
						}
						// If DataID is still not available, fall through to normal write path
						DebugLog("[VFS RandomAccessor Flush] File still has no DataID after BatchWriter flush, falling back to normal write path: fileID=%d", ra.fileID)
					}
				}

				// Merge all data into a continuous data block
				// Optimization: use appropriate object pool based on size
				var mergedData []byte
				useSmallPool := mergedDataSize <= 64<<10 // 64KB
				useLargePool := mergedDataSize <= 4<<20  // 4MB
				if useSmallPool {
					pooledData := chunkDataPool.Get().([]byte)
					if cap(pooledData) >= int(mergedDataSize) {
						mergedData = pooledData[:mergedDataSize]
					} else {
						mergedData = make([]byte, mergedDataSize)
						chunkDataPool.Put(pooledData[:0])
					}
				} else if useLargePool {
					pooledData := largeChunkDataPool.Get().([]byte)
					if cap(pooledData) >= int(mergedDataSize) {
						mergedData = pooledData[:mergedDataSize]
					} else {
						mergedData = make([]byte, mergedDataSize)
						largeChunkDataPool.Put(pooledData[:0])
					}
				} else {
					mergedData = make([]byte, mergedDataSize)
				}
				for _, op := range mergedOps {
					copy(mergedData[op.Offset:], op.Data)
				}

				// Add to batch write manager (lock-free)
				batchMgr := ra.fs.getBatchWriteManager()
				if batchMgr != nil {
					added, dataID, err := addFileToBatchWrite(ra, mergedData)
					if err != nil {
						// Processing failed (e.g., compression/encryption error), fallback to normal write
						DebugLog("[VFS RandomAccessor Flush] Batch write processing failed, falling back to normal write: fileID=%d, error=%v", ra.fileID, err)
					} else if !added {
						// Buffer full, flush and retry (this is the only case where we flush: when buffer is full)
						DebugLog("[VFS RandomAccessor Flush] Batch write buffer full, flushing and retrying: fileID=%d", ra.fileID)
						batchMgr.FlushAll(ra.fs.c)

						// Wait a brief moment for flush to complete (FlushAll is synchronous, but give it a moment)
						time.Sleep(10 * time.Millisecond)

						// Retry add after flush
						added, dataID, err = addFileToBatchWrite(ra, mergedData)
						if err != nil {
							DebugLog("[VFS RandomAccessor Flush] Batch write retry failed with error, falling back to normal write: fileID=%d, error=%v", ra.fileID, err)
						} else if !added {
							// Still failed after retry, fallback to normal write
							DebugLog("[VFS RandomAccessor Flush] Batch write still failed after flush, using normal write path: fileID=%d", ra.fileID)
						}
					}

					if added {
						// Successfully added to batch write manager
						// Do NOT immediately flush - let batch write manager handle flushing based on its time window
						// This allows multiple files to be batched together for better performance
						// Batch write manager will automatically flush when:
						// 1. Buffer is full (AddFile returns false, triggers FlushAll)
						// 2. Time window expires (default 1 second, set by schedulePeriodicFlush)
						// 3. Explicit FlushAll is called elsewhere
						// DebugLog("[VFS RandomAccessor Flush] Successfully added to batch write manager: fileID=%d, dataID=%d, will auto-flush when buffer full or timer expires", ra.fileID, dataID)

						// Update local cache with batch write DataID for immediate visibility
						// IMPORTANT: Must include Type, Name, PID, MTime to avoid cache corruption (consistent with direct write path)
						if fileObj, err := ra.getFileObj(); err == nil && fileObj != nil {
							updateFileObj := &core.ObjectInfo{
								ID:     fileObj.ID,
								PID:    fileObj.PID,
								DataID: dataID,
								Size:   mergedDataSize,
								MTime:  core.Now(),
								Type:   fileObj.Type,
								Name:   fileObj.Name,
							}
							fileObjCache.Put(ra.fileObjKey, updateFileObj)
							ra.fileObj.Store(updateFileObj)
							// Update directory listing cache to ensure file is visible in Readdir (consistent with direct write path)
							if updateFileObj.PID > 0 {
								dirNode := &OrcasNode{
									fs:    ra.fs,
									objID: updateFileObj.PID,
								}
								dirNode.appendChildToDirCache(updateFileObj.PID, updateFileObj)
								DebugLog("[VFS RandomAccessor Flush] Appended file to directory listing cache (batch write): fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
							}
						}

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

	// Files >= 1MB threshold: use direct write path (flush immediately to disk)
	// This ensures large files are written directly without batching
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
	fileObj, err = ra.getFileObj()
	if err != nil {
		return 0, err
	}

	// Only use writing version for sparse files (pre-allocated files with holes, e.g., qBittorrent)
	// For non-sparse files, use SDK path which handles compression/encryption properly
	sparseSize := atomic.LoadInt64(&ra.sparseSize)
	isSparseFile := sparseSize > 0

	if isSparseFile {
		// For sparse files, use writing version to directly modify data blocks
		// This avoids creating new versions and significantly improves performance for random writes
		// Note: Writing versions use uncompressed/unencrypted data for better performance
		// If compression/encryption is needed, it will be processed when the file is completed

		// Get DataInfo to check compression/encryption status
		var oldDataInfo *core.DataInfo
		oldDataID := fileObj.DataID
		if oldDataID > 0 && oldDataID != core.EmptyDataID {
			dataInfoCacheKey := oldDataID
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

		// Use writing version path for sparse files, but fall back if DB access fails
		newVersionID, err := ra.applyWritesWithWritingVersion(fileObj, mergedOps, oldDataInfo)
		if err != nil {
			if errors.Is(err, core.ERR_QUERY_DB) || errors.Is(err, core.ERR_OPEN_DB) {
				DebugLog("[VFS RandomAccessor Flush] Writing version unavailable, falling back to SDK path: fileID=%d, error=%v", ra.fileID, err)
				return ra.applyRandomWritesWithSDK(fileObj, mergedOps)
			}
			return 0, err
		}
		return newVersionID, nil
	}

	// For non-sparse files, use SDK path which handles compression/encryption properly
	DebugLog("[VFS RandomAccessor Flush] Non-sparse file, using SDK path: fileID=%d", ra.fileID)
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

	// Handle empty file case: if no writes or all writes are empty, return empty file
	// Note: We check len(writes) == 0 BEFORE filtering, because filtering happens below
	// If writes is empty, it means no write operations were passed to this function
	// This should not happen in normal flow, but we handle it for safety
	if len(writes) == 0 {
		// No writes, file remains empty
		DebugLog("[VFS applyRandomWritesWithSDK] No writes, file remains empty: fileID=%d, size=%d", ra.fileID, fileObj.Size)
		// Only update if file is actually empty (Size = 0)
		// If file has existing data (Size > 0), don't overwrite it
		if fileObj.Size == 0 {
			updateFileObj := &core.ObjectInfo{
				ID:     fileObj.ID,
				PID:    fileObj.PID,
				Type:   fileObj.Type,
				Name:   fileObj.Name,
				Size:   0,
				DataID: core.EmptyDataID,
				MTime:  core.Now(),
			}
			_, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
			if err != nil {
				DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update empty file: fileID=%d, error=%v", ra.fileID, err)
				return 0, err
			}
		}
		return 0, nil
	}

	// Filter out empty writes
	nonEmptyWrites := make([]WriteOperation, 0, len(writes))
	for _, write := range writes {
		if len(write.Data) > 0 {
			nonEmptyWrites = append(nonEmptyWrites, write)
		}
	}

	// If all writes were empty, treat as empty file
	if len(nonEmptyWrites) == 0 {
		DebugLog("[VFS applyRandomWritesWithSDK] All writes are empty, file remains empty: fileID=%d", ra.fileID)
		updateFileObj := &core.ObjectInfo{
			ID:     fileObj.ID,
			PID:    fileObj.PID,
			Type:   fileObj.Type,
			Name:   fileObj.Name,
			Size:   0,
			DataID: core.EmptyDataID,
			MTime:  core.Now(),
		}
		_, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update empty file: fileID=%d, error=%v", ra.fileID, err)
			return 0, err
		}
		return 0, nil
	}

	// Use non-empty writes for processing
	writes = nonEmptyWrites

	// For .tmp files, check if TempFileWriter is using the existing DataID
	// If TempFileWriter exists and has the same DataID, we should not create a new one
	// Otherwise, create new DataID (for non-.tmp files or when TempFileWriter doesn't exist)
	var newDataID int64
	isTmpFile := isTempFile(fileObj)
	if isTmpFile && fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		// Check if TempFileWriter is using this DataID (lock-free check using atomic.Value)
		hasTempWriter := false
		if val := ra.tempWriter.Load(); val != nil {
			if tw, ok := val.(*TempFileWriter); ok && tw != nil {
				hasTempWriter = tw.dataID == fileObj.DataID
			}
		}

		if hasTempWriter {
			// TempFileWriter is using this DataID, reuse it
			newDataID = fileObj.DataID
			DebugLog("[VFS applyRandomWritesWithSDK] Reusing existing DataID from TempFileWriter for .tmp file: fileID=%d, dataID=%d", ra.fileID, newDataID)
		} else {
			// TempFileWriter doesn't exist or uses different DataID, create new one
			// This should not happen for .tmp files in normal flow, but handle it for safety
			newDataID = core.NewID()
			if newDataID <= 0 {
				return 0, fmt.Errorf("failed to generate DataID")
			}
			DebugLog("[VFS applyRandomWritesWithSDK] Creating new DataID for .tmp file (TempFileWriter not using existing): fileID=%d, existingDataID=%d, newDataID=%d", ra.fileID, fileObj.DataID, newDataID)
		}
	} else {
		// Create new data ID (for non-.tmp files or .tmp files without DataID)
		newDataID = core.NewID()
		if newDataID <= 0 {
			return 0, fmt.Errorf("failed to generate DataID")
		}
		DebugLog("[VFS applyRandomWritesWithSDK] Created new DataID: fileID=%d, dataID=%d, isTmpFile=%v", ra.fileID, newDataID, isTmpFile)
	}

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

	// Check if original data is compressed or encrypted (optimized: use cache)
	var oldDataInfo *core.DataInfo
	var hasCompression, hasEncryption bool
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// Optimization: use more efficient key generation (function internally uses object pool)
		dataInfoCacheKey := oldDataID

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
	// Set Kind based on bucket configuration (for new files without old data)
	bucket := getBucketConfigWithCache(ra.fs)
	kind := core.DATA_NORMAL
	if bucket != nil {
		if bucket.CmprWay > 0 {
			kind |= bucket.CmprWay
		}
		if bucket.EndecWay > 0 {
			kind |= bucket.EndecWay
		}
	}
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: newSize,
		Kind:     kind,
	}

	// Check if this is a .tmp file with TempFileWriter
	// If so, TempFileWriter already handles compression/encryption, so don't use applyWritesStreamingCompressed
	// Lock-free check using atomic.Value
	hasTempWriter := ra.tempWriter.Load() != nil

	// Check if bucket has compression or encryption configuration
	// For new files (no old data), if bucket has config, should use compression/encryption path
	// BUT: If TempFileWriter exists, it already handles compression/encryption, so skip this path
	hasBucketConfig := bucket != nil && (bucket.CmprWay > 0 || bucket.EndecWay > 0)

	// If original data is compressed or encrypted, must read completely (unavoidable)
	// But can stream write, avoid processing all data at once
	// Also, for new files with bucket config, use compression/encryption path
	// Note: applyWritesStreamingCompressed can handle oldDataInfo == nil (new files)
	// BUT: If TempFileWriter exists, it already handles compression/encryption, so don't use this path
	if !hasTempWriter && (hasCompression || hasEncryption || ((oldDataID == 0 || oldDataID == core.EmptyDataID) && hasBucketConfig)) {
		DebugLog("[VFS applyRandomWritesWithSDK] Processing compressed/encrypted data for fileID=%d, newDataID=%d, hasOldData=%v, hasBucketConfig=%v", ra.fileID, newDataID, hasCompression || hasEncryption, hasBucketConfig)
		newVersionID, err := ra.applyWritesStreamingCompressed(oldDataInfo, writes, dataInfo, newSize)
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
		// IMPORTANT: Must include Type, Name, PID, MTime to avoid cache corruption
		updateFileObj := &core.ObjectInfo{
			ID:     ra.fileID,
			PID:    fileObj.PID,
			Type:   fileObj.Type,
			Name:   fileObj.Name,
			DataID: newDataID,
			Size:   newSize,
			MTime:  mTime,
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
			// Update directory listing cache to ensure file is visible in Readdir
			if updateFileObj.PID > 0 {
				dirNode := &OrcasNode{
					fs:    ra.fs,
					objID: updateFileObj.PID,
				}
				dirNode.appendChildToDirCache(updateFileObj.PID, updateFileObj)
				DebugLog("[VFS applyRandomWritesWithSDK] Appended file to directory listing cache: fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
			}
		} else {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update objects: fileID=%d, error=%v", ra.fileID, err)
		}

		return newVersionID, err
	}

	// For uncompressed unencrypted data, can stream read and process by chunk
	newVersionID, err := ra.applyWritesStreamingUncompressed(fileObj, oldDataInfo, writes, dataInfo, newSize)
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
	// IMPORTANT: Must include Type, Name, PID, MTime to avoid cache corruption
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		PID:    fileObj.PID,
		Type:   fileObj.Type,
		Name:   fileObj.Name,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}
	objectsToPut = append(objectsToPut, updateFileObj)

	// Use Put method to batch create version and update file object (will automatically apply version retention policy)
	_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)

	// Update cached file object information
	if err == nil {
		// Optimization: use pre-computed key (avoid repeated conversion)
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
		DebugLog("[VFS applyRandomWritesWithSDK] Successfully applied writes (uncompressed/unencrypted): fileID=%d, versionID=%d, newDataID=%d, size=%d", ra.fileID, newVersionID, newDataID, newSize)
		// Update directory listing cache to ensure file is visible in Readdir
		if updateFileObj.PID > 0 {
			dirNode := &OrcasNode{
				fs:    ra.fs,
				objID: updateFileObj.PID,
			}
			dirNode.appendChildToDirCache(updateFileObj.PID, updateFileObj)
			DebugLog("[VFS applyRandomWritesWithSDK] Appended file to directory listing cache: fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
		}
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
		return 0, fmt.Errorf("failed to get or create writing version: %w", err)
	}

	// Use writing version's DataID (writing version always has its own DataID without compression/encryption)
	dataID := writingVersion.DataID
	if dataID == 0 || dataID == core.EmptyDataID {
		return 0, fmt.Errorf("writing version has invalid DataID")
	}

	// Writing versions always use uncompressed/unencrypted data
	// No need to handle compression/encryption
	chunkSizeInt := int64(ra.fs.chunkSize)

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
		dataInfoCacheKey := dataID
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
		// IMPORTANT: Must include Type, Name, PID, MTime to avoid cache corruption
		mTime := core.Now()
		updateFileObj := &core.ObjectInfo{
			ID:     ra.fileID,
			PID:    fileObj.PID,
			Type:   fileObj.Type,
			Name:   fileObj.Name,
			DataID: dataID,
			Size:   newSize,
			MTime:  mTime,
		}

		DebugLog("[VFS applyWritesWithWritingVersion] Writing file object to disk: fileID=%d, dataID=%d, size=%d, chunkSize=%d", ra.fileID, dataID, newSize, ra.fs.chunkSize)
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
	dataInfo *core.DataInfo, newSize int64,
) (int64, error) {
	// Now each chunk is independently compressed and encrypted, can process by chunk in streaming
	// Directly read by chunk, decrypt, decompress, don't use DataReader

	var endecKey string
	bucket := getBucketConfigWithCache(ra.fs)
	if bucket != nil {
		endecKey = bucket.EndecKey
	}

	chunkSize := ra.fs.chunkSize

	// Create a reader to read, decrypt, and decompress by chunk
	reader := newChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, oldDataInfo, endecKey, chunkSize)

	// Pre-calculate write operation indices for each chunk
	chunkCount := int((newSize + chunkSize - 1) / chunkSize)
	if chunkCount == 0 {
		// Empty file, no chunks needed
		chunkCount = 1
	}
	writesByChunk := make([][]int, chunkCount)
	if chunkCount > 0 {
		avgWritesPerChunk := len(writes) / chunkCount
		if avgWritesPerChunk < 1 {
			avgWritesPerChunk = 1
		}
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}
	} else {
		avgWritesPerChunk := 1
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}
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
	return ra.processWritesStreaming(reader, writesByChunk, writes, dataInfo, newSize, chunkCount)
}

// applyWritesStreamingUncompressed handles uncompressed and unencrypted data
// Can stream read and process by chunk, only reading affected data ranges
func (ra *RandomAccessor) applyWritesStreamingUncompressed(fileObj *core.ObjectInfo, oldDataInfo *core.DataInfo,
	writes []WriteOperation, dataInfo *core.DataInfo, newSize int64,
) (int64, error) {
	// For uncompressed and unencrypted data, can stream process by chunk
	// Optimization: pre-calculate write operations for each chunk to reduce repeated checks in loops

	chunkSize := ra.fs.chunkSize
	// If no bucket configuration, directly stream write
	bucket := getBucketConfigWithCache(ra.fs)
	if bucket == nil {
		dataInfo.Size = newSize
		dataInfo.OrigSize = newSize

		oldDataID := fileObj.DataID
		sn := 0

		// Pre-calculate write operation indices for each chunk (optimization: reduce repeated checks)
		// Optimization: estimate capacity to reduce slice expansion
		chunkCount := int((newSize + chunkSize - 1) / chunkSize)
		if chunkCount == 0 {
			// Empty file, no chunks needed
			chunkCount = 1
		}
		writesByChunk := make([][]int, chunkCount)
		// Estimate average number of write operations per chunk (optimization: reduce slice expansion)
		avgWritesPerChunk := 1
		if chunkCount > 0 {
			avgWritesPerChunk = len(writes) / chunkCount
			if avgWritesPerChunk < 1 {
				avgWritesPerChunk = 1
			}
		}
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

		// Process by chunk
		for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
			pos := int64(chunkIdx * int(chunkSize))
			chunkEnd := pos + chunkSize
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
			// Optimization: use object pool for chunkDataCopy to reduce allocations
			chunkDataCopy := chunkDataPool.Get().([]byte)
			if cap(chunkDataCopy) < len(chunkData) {
				chunkDataCopy = make([]byte, len(chunkData))
			} else {
				chunkDataCopy = chunkDataCopy[:len(chunkData)]
			}
			copy(chunkDataCopy, chunkData)
			// Return original buffer to object pool before writing
			chunkDataPool.Put(chunkData[:0])

			DebugLog("[VFS applyWritesStreamingUncompressed] Writing data chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d", ra.fileID, dataInfo.ID, sn, len(chunkDataCopy))
			if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, chunkDataCopy); err != nil {
				// Return copy buffer to pool on error
				chunkDataPool.Put(chunkDataCopy[:0])
				DebugLog("[VFS applyWritesStreamingUncompressed] ERROR: Failed to write data chunk to disk: fileID=%d, dataID=%d, sn=%d, error=%v", ra.fileID, dataInfo.ID, sn, err)
				return 0, err
			}
			// Note: chunkDataCopy is consumed by PutData, don't return to pool here
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
		dataInfoCache.Put(dataInfo.ID, dataInfo)

		newVersionID := core.NewID()
		return newVersionID, nil
	}

	// Has SDK configuration, needs compression and encryption
	// Stream processing: read original data by chunk, apply write operations, process and write to new object immediately

	// Pre-calculate write operation indices for each chunk
	chunkCount := int((newSize + chunkSize - 1) / chunkSize)
	if chunkCount == 0 {
		// Empty file, no chunks needed
		chunkCount = 1
	}
	writesByChunk := make([][]int, chunkCount)
	if chunkCount > 0 {
		avgWritesPerChunk := len(writes) / chunkCount
		if avgWritesPerChunk < 1 {
			avgWritesPerChunk = 1
		}
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}
	} else {
		avgWritesPerChunk := 1
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}
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

	// For uncompressed and unencrypted data, can directly read by chunk without reading all data first
	// Create a special reader to support reading by chunk
	var reader dataReader
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// Create chunkReader to support reading by chunk
		reader = newChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, &core.DataInfo{ID: oldDataID, OrigSize: fileObj.Size}, "", chunkSize)
	}

	// Stream processing: read, process, and write by chunk
	return ra.processWritesStreaming(reader, writesByChunk, writes, dataInfo, newSize, chunkCount)
}

// processWritesStreaming streams processing of write operations
// Read original data by chunk, apply write operations, immediately process (compress/encrypt) and write to new object
func (ra *RandomAccessor) processWritesStreaming(
	reader dataReader,
	writesByChunk [][]int,
	writes []WriteOperation,
	dataInfo *core.DataInfo,
	newSize int64,
	chunkCount int,
) (int64, error) {
	bucket := getBucketConfigWithCache(ra.fs)

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
		DebugLog("[VFS processWritesStreaming] Set encryption flag: fileID=%d, dataID=%d, EndecWay=0x%x, Kind=0x%x", ra.fileID, dataInfo.ID, bucket.EndecWay, dataInfo.Kind)
	}

	// Calculate CRC32 (original data)
	var crc32Val uint32
	var dataCRC32 uint32

	sn := 0
	firstChunk := true
	chunkSizeInt := int(ra.fs.chunkSize)

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

		// 1. Read this chunk of original data from reader (using Read(buf, offset))
		if reader != nil {
			// Read current chunk data directly from offset
			n, err := reader.Read(chunkData, pos)
			if err != nil && err != io.EOF {
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
	dataInfoCache.Put(dataInfo.ID, dataInfo)

	newVersionID := core.NewID()
	return newVersionID, nil
}

// dataReader data reader interface, unified handling of data reading in different formats
// Read reads data starting from offset into buf, returns number of bytes read and error
type dataReader interface {
	Read(buf []byte, offset int64) (int, error)
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

	// Read required data range directly from offset
	readData := make([]byte, readSize)
	n, err := reader.Read(readData, readStart)
	// If read fails or read data is less than requested size, only return read data
	if err != nil {
		// io.EOF means all available data has been read
		if err == io.EOF {
			if n > 0 {
				readData = readData[:n]
			} else {
				readData = []byte{}
			}
		} else {
			// Other errors, return failure
			return nil, false
		}
	} else if n < len(readData) {
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

// chunkReader unified reader for both plain and compressed/encrypted data
// Uses cache and singleflight for consistent performance
// Supports asynchronous prefetching when reading reaches 80% of current chunk
type chunkReader struct {
	c                 core.Ctx
	h                 core.Handler
	bktID             int64
	dataID            int64
	kind              uint32                // Compression/encryption kind (0 for plain)
	endecKey          string                // Encryption key (empty for plain)
	origSize          int64                 // Original data size (decompressed size)
	compressedSize    int64                 // Compressed/encrypted data size (for packaged files)
	chunkSize         int64                 // Chunk size for original data
	chunkCache        *ecache2.Cache[int64] // Cache for chunks: key sn (int64), value []byte
	decodedFileCache  *ecache2.Cache[int64] // Cache for decoded file data (for packaged files): key dataID (int64), value []byte
	prefetchThreshold float64               // Threshold percentage to trigger prefetch (default: 0.8 = 80%)
	pkgOffset         uint32                // Package offset (for packaged files, 0 for non-packaged files)
}

// newChunkReader creates a unified chunk reader for both plain and compressed/encrypted data
func newChunkReader(c core.Ctx, h core.Handler, bktID int64, dataInfo *core.DataInfo, endecKey string, chunkSize int64) *chunkReader {
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	cr := &chunkReader{
		c:                 c,
		h:                 h,
		bktID:             bktID,
		chunkSize:         chunkSize,
		chunkCache:        ecache2.NewLRUCache[int64](4, 64, 5*time.Minute),
		decodedFileCache:  ecache2.NewLRUCache[int64](1, 1, 5*time.Minute), // Cache decoded file data for packaged files
		prefetchThreshold: 0.8,                                             // 80% threshold
	}

	if dataInfo != nil {
		// Set dataInfo fields
		cr.kind = dataInfo.Kind
		cr.endecKey = endecKey
		cr.origSize = dataInfo.OrigSize
		cr.compressedSize = dataInfo.Size // Compressed/encrypted size
		if dataInfo.PkgID > 0 {
			cr.dataID = dataInfo.PkgID
			cr.pkgOffset = dataInfo.PkgOffset
		} else {
			cr.dataID = dataInfo.ID
			cr.pkgOffset = 0
		}
	}

	return cr
}

// Read reads data starting from offset into buf (uses ReadAt internally)
func (cr *chunkReader) Read(buf []byte, offset int64) (int, error) {
	return cr.ReadAt(buf, offset)
}

// ReadAt implements random-access reads by directly addressing chunks
func (cr *chunkReader) ReadAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("invalid offset: %d", offset)
	}
	if offset >= cr.origSize {
		return 0, io.EOF
	}
	if len(buf) == 0 {
		return 0, nil
	}

	// Limit read size to available data
	readSize := int64(len(buf))
	if offset+readSize > cr.origSize {
		readSize = cr.origSize - offset
	}
	if readSize <= 0 {
		return 0, io.EOF
	}

	// ChunkSize must be set
	if cr.chunkSize <= 0 {
		return 0, fmt.Errorf("chunkSize not set")
	}

	// For packaged files, read entire package once and extract file data
	var fileData []byte
	if cr.pkgOffset > 0 {
		// This is a packaged file, read entire package (sn=0)
		// Check cache first for decoded file data
		if cached, ok := cr.decodedFileCache.Get(cr.dataID); ok {
			if decoded, ok := cached.([]byte); ok && len(decoded) > 0 {
				fileData = decoded
				// DebugLog("[VFS chunkReader ReadAt] Using cached decoded file data: dataID=%d, decodedLen=%d", cr.dataID, len(fileData))
			}
		}

		if fileData == nil {
			// Use singleflight to ensure only one goroutine decodes this package file at a time
			// Key format: "decode_pkg_<dataID>" to uniquely identify each package file
			sfKey := fmt.Sprintf("decode_pkg_%d", cr.dataID)

			result, err, _ := packageDecodeSingleFlight.Do(sfKey, func() (interface{}, error) {
				// Double-check cache after acquiring singleflight lock
				// Another goroutine might have already decoded it
				if cached, ok := cr.decodedFileCache.Get(cr.dataID); ok {
					if decoded, ok := cached.([]byte); ok && len(decoded) > 0 {
						return decoded, nil
					}
				}

				// DebugLog("[VFS chunkReader ReadAt] Reading packaged file: dataID=%d, pkgOffset=%d, origSize=%d, offset=%d, bufSize=%d",
				//	cr.dataID, cr.pkgOffset, cr.origSize, offset, len(buf))
				pkgData, err := cr.getChunk(0)
				if err != nil {
					// DebugLog("[VFS chunkReader ReadAt] ERROR: Failed to get package chunk: dataID=%d, error=%v", cr.dataID, err)
					return nil, err
				}
				// DebugLog("[VFS chunkReader ReadAt] Got package data: dataID=%d, pkgDataLen=%d", cr.dataID, len(pkgData))
				// Extract file data from package
				// PkgOffset is the offset of compressed/encrypted file data in the package
				// Use compressedSize (not origSize) to calculate the range
				pkgStart := int64(cr.pkgOffset)
				pkgEnd := pkgStart + cr.compressedSize
				if int64(len(pkgData)) < pkgEnd {
					// DebugLog("[VFS chunkReader ReadAt] ERROR: Package data incomplete: dataID=%d, pkgOffset=%d, compressedSize=%d, expected %d bytes, got %d",
					//	cr.dataID, cr.pkgOffset, cr.compressedSize, pkgEnd, len(pkgData))
					return nil, fmt.Errorf("package data incomplete: expected %d bytes, got %d", pkgEnd, len(pkgData))
				}
				encryptedFileData := pkgData[pkgStart:pkgEnd]
				// DebugLog("[VFS chunkReader ReadAt] Extracted encrypted file data: dataID=%d, encryptedFileDataLen=%d", cr.dataID, len(encryptedFileData))

				// Decrypt and decompress the file data
				decodedFileData := encryptedFileData

				// 1. Decrypt first (if enabled)
				if cr.kind&core.DATA_ENDEC_MASK != 0 {
					if cr.kind&core.DATA_ENDEC_AES256 != 0 {
						decodedFileData, err = aes256.Decrypt(cr.endecKey, encryptedFileData)
						if err != nil {
							// DebugLog("[VFS chunkReader ReadAt] ERROR: AES256 decryption failed: dataID=%d, error=%v", cr.dataID, err)
							return nil, fmt.Errorf("AES256 decryption failed: %v", err)
						}
					} else if cr.kind&core.DATA_ENDEC_SM4 != 0 {
						decodedFileData, err = sm4.Sm4Cbc([]byte(cr.endecKey), encryptedFileData, false)
						if err != nil {
							// DebugLog("[VFS chunkReader ReadAt] ERROR: SM4 decryption failed: dataID=%d, error=%v", cr.dataID, err)
							return nil, fmt.Errorf("SM4 decryption failed: %v", err)
						}
					}
				}

				// 2. Decompress next (if enabled)
				if cr.kind&core.DATA_CMPR_MASK != 0 {
					var decompressor archiver.Decompressor
					if cr.kind&core.DATA_CMPR_SNAPPY != 0 {
						decompressor = &archiver.Snappy{}
					} else if cr.kind&core.DATA_CMPR_ZSTD != 0 {
						decompressor = &archiver.Zstd{}
					} else if cr.kind&core.DATA_CMPR_GZIP != 0 {
						decompressor = &archiver.Gz{}
					} else if cr.kind&core.DATA_CMPR_BR != 0 {
						decompressor = &archiver.Brotli{}
					}

					if decompressor != nil {
						var decompressedBuf bytes.Buffer
						err := decompressor.Decompress(bytes.NewReader(decodedFileData), &decompressedBuf)
						if err != nil {
							// DebugLog("[VFS chunkReader ReadAt] ERROR: Decompression failed: dataID=%d, error=%v", cr.dataID, err)
							return nil, fmt.Errorf("decompression failed: %v", err)
						}
						decodedFileData = decompressedBuf.Bytes()
					}
				}

				// Cache decoded file data for future reads
				cr.decodedFileCache.Put(cr.dataID, decodedFileData)
				// DebugLog("[VFS chunkReader ReadAt] Decoded file data: dataID=%d, decodedFileDataLen=%d, origSize=%d", cr.dataID, len(decodedFileData), cr.origSize)
				return decodedFileData, nil
			})

			if err != nil {
				return 0, err
			}

			if decoded, ok := result.([]byte); ok {
				fileData = decoded
			} else {
				return 0, fmt.Errorf("invalid decoded file data type")
			}
		}

		// Now read from decoded fileData
		if offset >= int64(len(fileData)) {
			// DebugLog("[VFS chunkReader ReadAt] Offset beyond file size: dataID=%d, offset=%d, fileDataLen=%d", cr.dataID, offset, len(fileData))
			return 0, io.EOF
		}
		readSize := int64(len(buf))
		if offset+readSize > int64(len(fileData)) {
			readSize = int64(len(fileData)) - offset
		}
		// DebugLog("[VFS chunkReader ReadAt] Reading from decoded file data: dataID=%d, offset=%d, readSize=%d", cr.dataID, offset, readSize)
		copy(buf[:readSize], fileData[offset:offset+readSize])
		return int(readSize), nil
	}

	// For non-packaged files, use normal chunk-based reading
	// Note: ReadAt should be safe for concurrent use, but we add mutex to prevent
	// issues with concurrent reads that might cause confusion in logs
	// However, since ReadAt is designed to be safe for concurrent use (each call has its own offset),
	// we don't lock the entire method. Instead, we rely on singleflight for chunk reads.
	// The mutex is only used for package file decoding to prevent race conditions.
	totalRead := 0
	currentOffset := offset
	remaining := readSize

	for remaining > 0 && currentOffset < cr.origSize {
		currentSn := int(currentOffset / cr.chunkSize)
		chunkStart := int64(currentSn) * cr.chunkSize
		currentOffsetInChunk := currentOffset - chunkStart

		// Only log once per unique read request to avoid spam from concurrent reads
		// Use a simple check: only log if this is the first iteration (totalRead == 0 and currentOffset == offset)
		if totalRead == 0 && currentOffset == offset {
			DebugLog("[VFS chunkReader ReadAt] Reading chunk: dataID=%d, sn=%d, offset=%d, offsetInChunk=%d, remaining=%d, origSize=%d, totalRead=%d",
				cr.dataID, currentSn, currentOffset, currentOffsetInChunk, remaining, cr.origSize, totalRead)
		}

		// Get or load chunk (with cache and singleflight)
		chunkData, err := cr.getChunk(currentSn)
		if err != nil {
			DebugLog("[VFS chunkReader ReadAt] Error getting chunk: dataID=%d, sn=%d, error=%v, totalRead=%d, currentOffset=%d, origSize=%d",
				cr.dataID, currentSn, err, totalRead, currentOffset, cr.origSize)
			// If chunk doesn't exist, check if we've reached the end of file
			// For the last chunk, it might be smaller than chunkSize
			// Check if we've read enough data based on origSize
			if currentOffset >= cr.origSize {
				// We've reached the end of file, return what we've read
				if totalRead > 0 {
					return totalRead, nil
				}
				return 0, io.EOF
			}
			// If chunk doesn't exist but we haven't reached origSize, it's an error
			// But if we've read some data, return what we have (partial read)
			if totalRead > 0 {
				// Check if error is "not found" or EOF - might be end of file
				if err == io.EOF || (err != nil && strings.Contains(err.Error(), "not found")) {
					// If we're close to origSize, assume we've read all available data
					if currentOffset >= cr.origSize-int64(len(chunkData)) {
						return totalRead, nil
					}
				}
				// For other errors, return what we've read so far
				return totalRead, nil
			}
			// If this is the first chunk and it doesn't exist, return error
			return 0, err
		}

		// Calculate how much to read from this chunk
		// If currentOffsetInChunk is beyond the actual chunk data, we need to move to next chunk
		if currentOffsetInChunk >= int64(len(chunkData)) {
			// Current offset is beyond this chunk's data, move to next chunk
			actualChunkEnd := chunkStart + int64(len(chunkData))
			// Check if we've reached the end of file
			if actualChunkEnd >= cr.origSize {
				// End of file
				DebugLog("[VFS chunkReader ReadAt] Reached end of file: dataID=%d, actualChunkEnd=%d, origSize=%d", cr.dataID, actualChunkEnd, cr.origSize)
				break
			}
			// Move to next chunk
			currentOffset = actualChunkEnd
			continue
		}

		availableInChunk := int64(len(chunkData)) - currentOffsetInChunk
		if availableInChunk <= 0 {
			// This chunk has no more data, move to next chunk
			// Calculate the actual end of current chunk based on actual data size
			// The actual end is the start of this chunk plus the actual data length
			actualChunkEnd := chunkStart + int64(len(chunkData))
			// Check if we've reached the end of file
			if actualChunkEnd >= cr.origSize {
				// End of file
				DebugLog("[VFS chunkReader ReadAt] Reached end of file: dataID=%d, actualChunkEnd=%d, origSize=%d", cr.dataID, actualChunkEnd, cr.origSize)
				break
			}
			// Move to next chunk: directly read next chunk (sn+1) at actualChunkEnd offset
			// The key insight: chunk sn is based on configured chunkSize, but actual chunk data
			// may be smaller. When actualChunkEnd < nextChunkStart, we should still read sn+1,
			// but at offset actualChunkEnd (not nextChunkStart)
			// Update currentOffset to actualChunkEnd before reading next chunk
			currentOffset = actualChunkEnd
			nextSn := currentSn + 1
			// Try to read next chunk directly at the actual chunk end offset
			// This handles the case where actual chunk size is smaller than configured chunkSize
			nextChunkData, nextErr := cr.getChunk(nextSn)
			if nextErr != nil {
				// Next chunk doesn't exist, check if we've read enough
				if actualChunkEnd >= cr.origSize {
					DebugLog("[VFS chunkReader ReadAt] Next chunk doesn't exist, reached end: dataID=%d, actualChunkEnd=%d, origSize=%d",
						cr.dataID, actualChunkEnd, cr.origSize)
					break
				}
				// If we've read some data, return it
				if totalRead > 0 {
					DebugLog("[VFS chunkReader ReadAt] Next chunk doesn't exist, returning partial read: dataID=%d, totalRead=%d, actualChunkEnd=%d, origSize=%d",
						cr.dataID, totalRead, actualChunkEnd, cr.origSize)
					return totalRead, nil
				}
				// No data read yet, return error
				return 0, nextErr
			}
			// Next chunk exists, read from it starting at offset 0 within the chunk
			// The next chunk's data starts at actualChunkEnd in the file
			// But within the chunk, we read from offset 0
			nextChunkOffsetInChunk := int64(0)
			nextAvailableInChunk := int64(len(nextChunkData)) - nextChunkOffsetInChunk
			if nextAvailableInChunk <= 0 {
				// Next chunk is empty, move to next
				currentOffset = actualChunkEnd
				continue
			}
			// Read from next chunk
			toReadFromNext := remaining
			if toReadFromNext > nextAvailableInChunk {
				toReadFromNext = nextAvailableInChunk
			}
			copy(buf[totalRead:totalRead+int(toReadFromNext)], nextChunkData[nextChunkOffsetInChunk:nextChunkOffsetInChunk+toReadFromNext])
			DebugLog("[VFS chunkReader ReadAt] Read from next chunk: dataID=%d, currentSn=%d, nextSn=%d, toRead=%d, totalRead=%d",
				cr.dataID, currentSn, nextSn, toReadFromNext, totalRead+int(toReadFromNext))
			totalRead += int(toReadFromNext)
			// Update currentOffset correctly: next chunk starts at actualChunkEnd, and we read toReadFromNext bytes from it
			// So the new currentOffset should be actualChunkEnd + toReadFromNext
			// However, we need to ensure currentOffset is correctly aligned for the next iteration
			currentOffset = actualChunkEnd + toReadFromNext
			remaining -= toReadFromNext
			// Continue loop to read more if needed
			continue
		}

		toRead := remaining
		if toRead > availableInChunk {
			toRead = availableInChunk
		}

		// Copy data from chunk
		copy(buf[totalRead:totalRead+int(toRead)], chunkData[currentOffsetInChunk:currentOffsetInChunk+toRead])

		DebugLog("[VFS chunkReader ReadAt] Read from chunk: dataID=%d, sn=%d, chunkSize=%d, toRead=%d, totalRead=%d, remaining=%d",
			cr.dataID, currentSn, len(chunkData), toRead, totalRead+int(toRead), remaining-toRead)

		totalRead += int(toRead)
		currentOffset += toRead
		remaining -= toRead

		// Trigger prefetch if we've read past the threshold percentage of current chunk
		if len(chunkData) > 0 {
			readPercentage := float64(currentOffsetInChunk+toRead) / float64(len(chunkData))
			if readPercentage >= cr.prefetchThreshold {
				// Asynchronously prefetch next chunk
				nextSn := currentSn + 1
				go func() {
					// Check if next chunk exists and not already cached
					if currentOffset+toRead < cr.origSize {
						if _, ok := cr.chunkCache.Get(int64(nextSn)); !ok {
							// Prefetch next chunk (ignore errors)
							_, _ = cr.getChunk(nextSn)
						}
					}
				}()
			}
		}
	}

	if totalRead == 0 && currentOffset >= cr.origSize {
		DebugLog("[VFS chunkReader ReadAt] No data read, reached EOF: dataID=%d, currentOffset=%d, origSize=%d", cr.dataID, currentOffset, cr.origSize)
		return 0, io.EOF
	}

	DebugLog("[VFS chunkReader ReadAt] Completed read: dataID=%d, totalRead=%d, requested=%d, origSize=%d", cr.dataID, totalRead, readSize, cr.origSize)
	return totalRead, nil
}

// getChunk gets a chunk (plain or decompressed/decrypted), using cache and singleflight
func (cr *chunkReader) getChunk(sn int) ([]byte, error) {
	// Check cache first (fast path)
	if cached, ok := cr.chunkCache.Get(int64(sn)); ok {
		if chunkData, ok := cached.([]byte); ok && len(chunkData) > 0 {
			return chunkData, nil
		}
	}

	// Use global singleflight to ensure only one goroutine reads this chunk
	// Key format: "chunk_<dataID>_<sn>" to uniquely identify each chunk globally
	sfKey := fmt.Sprintf("chunk_%d_%d", cr.dataID, sn)

	result, err, _ := chunkReadSingleFlight.Do(sfKey, func() (interface{}, error) {
		// Double-check cache after acquiring singleflight lock
		// Another goroutine might have already loaded it
		if cached, ok := cr.chunkCache.Get(int64(sn)); ok {
			if chunkData, ok := cached.([]byte); ok && len(chunkData) > 0 {
				return chunkData, nil
			}
		}

		// Read chunk (compressed/encrypted or plain)
		rawChunk, err := cr.h.GetData(cr.c, cr.bktID, cr.dataID, sn)
		if err != nil {
			return nil, err
		}

		// For packaged files (pkgOffset > 0), package itself is not encrypted/compressed
		// Each file in the package is individually encrypted/compressed
		// So we should return raw package data without processing
		if cr.pkgOffset > 0 {
			// This is a packaged file, return raw package data
			// Decryption/decompression will be done on individual file data later
			return rawChunk, nil
		}

		// For non-packaged files, process chunk based on kind (plain, encrypted, compressed, or both)
		finalChunk := rawChunk

		// 1. Decrypt first (if enabled)
		if cr.kind&core.DATA_ENDEC_MASK != 0 {
			decodedChunk := rawChunk
			if cr.kind&core.DATA_ENDEC_AES256 != 0 {
				decodedChunk, err = aes256.Decrypt(cr.endecKey, rawChunk)
				if err != nil {
					decodedChunk = rawChunk
				}
			} else if cr.kind&core.DATA_ENDEC_SM4 != 0 {
				decodedChunk, err = sm4.Sm4Cbc([]byte(cr.endecKey), rawChunk, false)
				if err != nil {
					decodedChunk = rawChunk
				}
			}
			finalChunk = decodedChunk
		}

		// 2. Decompress next (if enabled)
		if cr.kind&core.DATA_CMPR_MASK != 0 {
			var decompressor archiver.Decompressor
			if cr.kind&core.DATA_CMPR_SNAPPY != 0 {
				decompressor = &archiver.Snappy{}
			} else if cr.kind&core.DATA_CMPR_ZSTD != 0 {
				decompressor = &archiver.Zstd{}
			} else if cr.kind&core.DATA_CMPR_GZIP != 0 {
				decompressor = &archiver.Gz{}
			} else if cr.kind&core.DATA_CMPR_BR != 0 {
				decompressor = &archiver.Brotli{}
			}

			if decompressor != nil {
				var decompressedBuf bytes.Buffer
				err := decompressor.Decompress(bytes.NewReader(finalChunk), &decompressedBuf)
				if err == nil {
					finalChunk = decompressedBuf.Bytes()
				}
			}
		}

		// Cache the processed chunk before returning
		cr.chunkCache.Put(int64(sn), finalChunk)

		return finalChunk, nil
	})

	if err != nil {
		return nil, err
	}

	chunkData, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected chunk data type")
	}

	return chunkData, nil
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

	// Note: We do NOT flush batch writer here
	// FlushAll is only called when buffer is full (AddFile returns false) or by periodic timer
	// Truncate will work with data that's already in batch writer, it will be flushed automatically

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
						_, flushErr := ra.ForceFlush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush extended data: %v", flushErr)
						}

						// Get updated fileObj to get new DataID
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						if updatedFileObj.Size != newSize {
							DebugLog("[VFS Truncate] WARNING: Truncate extend size mismatch, expected=%d actual=%d (will correct metadata)", newSize, updatedFileObj.Size)
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
						// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							PID:    updatedFileObj.PID,
							Type:   updatedFileObj.Type,
							Name:   updatedFileObj.Name,
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
						// newSize < original size: reuse existing packaged data block and adjust logical size
						newDataID = oldDataID
						newDataInfo = &core.DataInfo{
							ID:        oldDataID,
							Size:      oldDataInfo.Size,
							OrigSize:  newSize,
							Kind:      oldDataInfo.Kind,
							PkgID:     oldDataInfo.PkgID,
							PkgOffset: oldDataInfo.PkgOffset,
						}
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
						_, flushErr := ra.ForceFlush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush extended data: %v", flushErr)
						}

						// Get updated fileObj to get new DataID
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						if updatedFileObj.Size != newSize {
							DebugLog("[VFS Truncate] WARNING: Truncate shrink size mismatch, expected=%d actual=%d (will correct metadata)", newSize, updatedFileObj.Size)
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
						// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							PID:    updatedFileObj.PID,
							Type:   updatedFileObj.Type,
							Name:   updatedFileObj.Name,
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

	// Get file object to get Type, Name, PID for updateFileObj
	fileObj, err = ra.getFileObj()
	if err != nil {
		return 0, fmt.Errorf("failed to get file object: %v", err)
	}

	// Update file object
	// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		PID:    fileObj.PID,
		Type:   fileObj.Type,
		Name:   fileObj.Name,
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
		dataInfoCache.Put(newDataID, newDataInfo)
	} else {
		// Only write ObjectInfo (no DataInfo to write)
		DebugLog("[VFS applyRandomWritesWithSDK] Writing ObjectInfo to disk (no DataInfo): fileID=%d, newSize=%d", ra.fileID, newSize)
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to write ObjectInfo to disk: fileID=%d, error=%v", ra.fileID, err)
			return 0, fmt.Errorf("failed to update file object: %v", err)
		}
		DebugLog("[VFS applyRandomWritesWithSDK] Successfully wrote ObjectInfo to disk: fileID=%d, newSize=%d", ra.fileID, newSize)
		// Update cache
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
		// Update directory listing cache to ensure file is visible in Readdir
		if updateFileObj.PID > 0 {
			dirNode := &OrcasNode{
				fs:    ra.fs,
				objID: updateFileObj.PID,
			}
			dirNode.appendChildToDirCache(updateFileObj.PID, updateFileObj)
			DebugLog("[VFS applyRandomWritesWithSDK] Appended file to directory listing cache: fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
		}
	}

	// Update cache
	fileObjCache.Put(ra.fileObjKey, updateFileObj)
	ra.fileObj.Store(updateFileObj)

	// Update directory listing cache to ensure file is visible in Readdir
	if updateFileObj.PID > 0 {
		dirNode := &OrcasNode{
			fs:    ra.fs,
			objID: updateFileObj.PID,
		}
		dirNode.appendChildToDirCache(updateFileObj.PID, updateFileObj)
		DebugLog("[VFS applyRandomWritesWithSDK] Appended file to directory listing cache: fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
	}

	// Invalidate old DataInfo cache if DataID changed
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID && fileObj.DataID != newDataID {
		oldDataInfoCacheKey := fileObj.DataID
		dataInfoCache.Del(oldDataInfoCacheKey)
	}

	return newVersionID, nil
}

func (ra *RandomAccessor) Close() error {
	// Cancel any pending delayed flush so we can finish synchronously
	ra.cancelDelayedFlush()

	// Synchronously flush all pending write data
	_, err := ra.Flush()
	if err != nil {
		return err
	}

	return nil
}

// flushTempFileWriter flushes the TempFileWriter when forcing a flush (e.g. before rename)
func (ra *RandomAccessor) flushTempFileWriter() error {
	// Lock-free read using atomic.Value
	val := ra.tempWriter.Load()
	if val == nil {
		return nil
	}
	tw, ok := val.(*TempFileWriter)
	if !ok || tw == nil {
		return nil
	}

	DebugLog("[VFS RandomAccessor] Forcing TempFileWriter flush: fileID=%d, dataID=%d", tw.fileID, tw.dataID)
	if err := tw.Flush(); err != nil {
		DebugLog("[VFS RandomAccessor] ERROR: TempFileWriter flush failed: fileID=%d, dataID=%d, error=%v", tw.fileID, tw.dataID, err)
		return err
	}

	// Update RandomAccessor's fileObj cache after flush to ensure subsequent reads use updated file size
	fileObj, err := ra.getFileObj()
	if err == nil && fileObj != nil {
		// Re-fetch from database to get the latest size after flush
		objs, err := ra.fs.h.Get(ra.fs.c, ra.fs.bktID, []int64{ra.fileID})
		if err == nil && len(objs) > 0 {
			updatedFileObj := objs[0]
			ra.fileObj.Store(updatedFileObj)
			fileObjCache.Put(ra.fileObjKey, updatedFileObj)
			DebugLog("[VFS RandomAccessor] Updated fileObj cache after TempFileWriter flush: fileID=%d, size=%d", ra.fileID, updatedFileObj.Size)
		}
	}

	return nil
}

// hasTempFileWriter reports whether this RandomAccessor currently owns a TempFileWriter
// Lock-free check using atomic.Value
func (ra *RandomAccessor) hasTempFileWriter() bool {
	val := ra.tempWriter.Load()
	if val == nil {
		return false
	}
	_, ok := val.(*TempFileWriter)
	return ok
}

// mergeWriteOperations merges overlapping write operations
// Optimization: use more efficient sorting algorithm (quicksort)
// IMPORTANT: When sorting by offset, we need to preserve the original order for operations with the same offset
// This ensures that later writes (in time order) overwrite earlier writes when they overlap
func mergeWriteOperations(operations []WriteOperation) []WriteOperation {
	if len(operations) == 0 {
		return nil
	}

	// Optimization: if operations are already sorted, can skip sorting
	// Optimization: for small number of operations, use insertion sort (already sorted for sequential writes)
	// For larger numbers, use more efficient sorting
	sorted := operations
	if len(sorted) > 1 {
		// Check if already sorted (common case for sequential writes)
		isSorted := true
		for i := 1; i < len(sorted); i++ {
			if sorted[i-1].Offset > sorted[i].Offset {
				isSorted = false
				break
			}
		}

		if !isSorted {
			// Create new slice for sorting (avoid modifying original slice)
			// IMPORTANT: Use stable sort to preserve order of operations with same offset
			// This ensures that later writes (in original order) overwrite earlier writes
			sorted = make([]WriteOperation, len(operations))
			copy(sorted, operations)

			// Use stable insertion sort to preserve order for same offset
			// This ensures that when operations have the same offset, the later one (in original order) is used
			if len(sorted) < 50 {
				// Optimized stable insertion sort (in-place, preserves order for equal elements)
				for i := 1; i < len(sorted); i++ {
					key := sorted[i]
					j := i - 1
					// Sort by offset, but preserve original order for same offset (stable sort)
					for j >= 0 && sorted[j].Offset > key.Offset {
						sorted[j+1] = sorted[j]
						j--
					}
					sorted[j+1] = key
				}
			} else {
				// For larger arrays, use standard library stable sort (guaranteed stability)
				sort.SliceStable(sorted, func(i, j int) bool {
					return sorted[i].Offset < sorted[j].Offset
				})
			}
		}
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

			// Optimization: check if we can reuse last.Data slice
			mergedSize := int(endOffset - startOffset)
			var mergedData []byte

			// Try to reuse last.Data if it has sufficient capacity
			if cap(last.Data) >= mergedSize && last.Offset == startOffset {
				// Can reuse existing slice, just extend it
				mergedData = last.Data[:mergedSize]
				// IMPORTANT: Since operations are sorted by offset and we process them in order,
				// and operations maintain their original order for same offset (stable sort),
				// op comes after last in the original order, so op should overwrite last
				// Old data is already in place, just overwrite with new data
				if op.Offset >= startOffset {
					copy(mergedData[op.Offset-startOffset:], op.Data)
				}
			} else {
				// Need new buffer, use appropriate object pool based on size
				if mergedSize <= 64<<10 {
					// Small buffer, use small pool
					mergedData = chunkDataPool.Get().([]byte)
					if cap(mergedData) < mergedSize {
						mergedData = make([]byte, mergedSize)
						chunkDataPool.Put(mergedData[:0]) // Return empty pool item
					} else {
						mergedData = mergedData[:mergedSize]
					}
				} else {
					// Large buffer, use large pool
					mergedData = largeChunkDataPool.Get().([]byte)
					if cap(mergedData) < mergedSize {
						mergedData = make([]byte, mergedSize)
						largeChunkDataPool.Put(mergedData[:0]) // Return empty pool item
					} else {
						mergedData = mergedData[:mergedSize]
					}
				}

				// IMPORTANT: Copy old data first, then overwrite with new data
				// Since operations are sorted by offset and maintain original order for same offset,
				// op comes after last in the original order, so op should overwrite last
				if last.Offset >= startOffset {
					copy(mergedData[last.Offset-startOffset:], last.Data)
				}
				// Overwrite with new data (op comes later in original order, so it should overwrite)
				if op.Offset >= startOffset {
					copy(mergedData[op.Offset-startOffset:], op.Data)
				}
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
