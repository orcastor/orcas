package sdk

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/orcastor/orcas/core"
)

// PackagedFileInfo packaged file information
type PackagedFileInfo struct {
	ObjectID  int64  // Object ID
	DataID    int64  // Data ID
	Offset    int64  // Offset position in buffer
	PkgOffset uint32 // Offset position in packaged data block
	Size      int64  // Data size (processed/compressed)
	OrigSize  int64  // Original data size (before processing)
	PID       int64  // Parent directory ID
	Name      string // Object name
}

// BatchWriterBuffer encapsulates buffer, offset, and file infos for double-buffering
type BatchWriterBuffer struct {
	buffer      []byte              // Buffer data
	writeOffset int64               // Current write position (atomic operation, use atomic.LoadInt64/StoreInt64/AddInt64)
	fileInfos   []*PackagedFileInfo // File info list
	fileIndex   int64               // Current file info index (atomic operation, use atomic.LoadInt64/StoreInt64/AddInt64)
}

// BatchWriter manages batch writes for small files
// Uses double-buffering to avoid data copying and allocation
// Double-buffer: when current buffer is full, swap to bg buffer and flush current
type BatchWriter struct {
	// Current buffer pointer (atomically swapped using unsafe.Pointer) - for writing
	currentBuffer unsafe.Pointer // *BatchWriterBuffer

	// Background buffer pointer (atomically swapped using unsafe.Pointer) - for flushing
	bgBuffer unsafe.Pointer // *BatchWriterBuffer

	bufferSize   int64 // Buffer size
	maxFileInfos int64 // Maximum number of file infos

	// Flush control
	flushWindow    time.Duration
	maxPackageSize int64 // Maximum packaged data block size

	// S3-specific fields
	handler core.Handler
	bktID   int64 // Bucket ID - one BatchWriter per bucket

	// Configuration
	enabled bool

	// Pending objects cache (for visibility before flush)
	// Maps fileID -> fileInfo for objects that are in buffer but not yet flushed
	pendingObjects sync.Map // map[int64]interface{} - fileID -> fileInfo
}

var (
	// Default configuration
	// Optimized for small files (< 64KB)
	// Increased buffer size for better handling of medium-sized files (100KB)
	defaultBufferSize     = int64(16 << 20) // 16MB - increased for better batching of medium files
	defaultMaxFileInfos   = int64(2 << 10)  // 2048 - increased for more files per batch
	defaultFlushWindow    = 5 * time.Second // 5s - reduced for faster response
	defaultMaxPackageSize = int64(16 << 20) // 16MB - increased for larger packages

	// Threshold for determining if a file is too large for batch write
	// If a single file exceeds this percentage of buffer size, it should be written directly
	// This prevents large files from causing frequent flushes and performance degradation
	maxFileSizeRatio = 0.3 // 30% of buffer size

	// batchWriters stores batch writers for each bucket
	// key: bktID (int64), value: *BatchWriter
	// One writer per bucket, managed globally by SDK
	batchWriters sync.Map
)

// GetBatchWriterForBucket gets or creates batch writer for a bucket
// Returns nil if batch write is disabled
// Thread-safe: uses sync.Map for concurrent access
func GetBatchWriterForBucket(handler core.Handler, bktID int64) *BatchWriter {
	// Try to get existing writer
	if val, ok := batchWriters.Load(bktID); ok {
		if mgr, ok := val.(*BatchWriter); ok {
			return mgr
		}
	}

	// Create new writer for this bucket
	mgr := createBatchWriter(handler, bktID)
	if mgr == nil {
		return nil
	}

	// Store writer (use LoadOrStore to handle race condition)
	actual, _ := batchWriters.LoadOrStore(bktID, mgr)
	if actualMgr, ok := actual.(*BatchWriter); ok && actualMgr != mgr {
		// Another goroutine created it first, return the existing one
		return actualMgr
	}

	return mgr
}

// FlushBatchWriterForBucket flushes batch writer for a bucket
func FlushBatchWriterForBucket(ctx context.Context, bktID int64) {
	if val, ok := batchWriters.Load(bktID); ok {
		if mgr, ok := val.(*BatchWriter); ok {
			mgr.FlushAll(ctx)
		}
	}
}

// FlushAllBatchWriters flushes all batch writers for all buckets
func FlushAllBatchWriters(ctx context.Context) {
	batchWriters.Range(func(key, value interface{}) bool {
		if mgr, ok := value.(*BatchWriter); ok {
			mgr.FlushAll(ctx)
		}
		return true
	})
}

// GetBatchWriteManager creates a new batch writer for a bucket
// Deprecated: Use GetBatchWriterForBucket instead
// This function is kept for backward compatibility but should not be used directly
// The writer should be stored in a sync.Map by the caller (e.g., s3 package)
// key: bktID, value: *BatchWriter
func GetBatchWriteManager(handler core.Handler, bktID int64) *BatchWriter {
	return createBatchWriter(handler, bktID)
}

// createBatchWriter creates a new batch writer instance
// This is an internal function that creates the writer without storing it
func createBatchWriter(handler core.Handler, bktID int64) *BatchWriter {
	// Check if batch write is enabled
	config := core.GetWriteBufferConfig()
	if !config.BatchWriteEnabled {
		return nil
	}

	// Get configuration from environment or use defaults
	bufferSize := defaultBufferSize
	if config.MaxBufferSize > 0 {
		bufferSize = config.MaxBufferSize
	}

	maxFileInfos := defaultMaxFileInfos
	if config.MaxBufferWrites > 0 {
		maxFileInfos = config.MaxBufferWrites
	}

	flushWindow := defaultFlushWindow
	if config.BufferWindow > 0 {
		flushWindow = config.BufferWindow
	}

	maxPackageSize := defaultMaxPackageSize
	if bufferSize > maxPackageSize {
		maxPackageSize = bufferSize
	}

	// Create new writer with double-buffering
	// Allocate two buffers and file info lists upfront
	buf1 := &BatchWriterBuffer{
		buffer:    make([]byte, bufferSize),
		fileInfos: make([]*PackagedFileInfo, maxFileInfos),
	}
	buf2 := &BatchWriterBuffer{
		buffer:    make([]byte, bufferSize),
		fileInfos: make([]*PackagedFileInfo, maxFileInfos),
	}

	mgr := &BatchWriter{
		bufferSize:     bufferSize,
		maxFileInfos:   maxFileInfos,
		flushWindow:    flushWindow,
		maxPackageSize: maxPackageSize,
		handler:        handler,
		bktID:          bktID,
		enabled:        true,
	}

	// Start with buf1 as current, buf2 as background
	// Use atomic.StorePointer for atomic pointer assignment
	atomic.StorePointer(&mgr.currentBuffer, unsafe.Pointer(buf1))
	atomic.StorePointer(&mgr.bgBuffer, unsafe.Pointer(buf2))

	return mgr
}

// FlushAll flushes all pending write data
func (bwm *BatchWriter) FlushAll(ctx context.Context) {
	bwm.flush(ctx)
}

// flush flushes all pending write data (lock-free)
// Atomically gets current write position and file info, then packages and writes
func (bwm *BatchWriter) flush(ctx context.Context) {
	// Get current buffer (for writing) - atomic load
	currentBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.currentBuffer))

	// Get old offset and index from current buffer, atomically reset to 0
	oldOffset := atomic.SwapInt64(&currentBuf.writeOffset, 0)
	oldIndex := atomic.SwapInt64(&currentBuf.fileIndex, 0)

	if oldOffset == 0 || oldIndex == 0 {
		return // No pending write data
	}

	// Memory barrier: ensure all writes to old buffer are visible
	runtime.Gosched()

	// Atomically swap: currentBuffer <-> bgBuffer
	// Get background buffer (will become new current) - atomic load
	bgBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.bgBuffer))

	// Check if bgBuffer is ready (already flushed and reset to 0)
	// If bgBuffer still has data (writeOffset > 0), it means previous flush is still in progress
	// In this case, we cannot swap - both buffers are in use
	bgOffset := atomic.LoadInt64(&bgBuf.writeOffset)
	if bgOffset > 0 {
		// bgBuffer is still being flushed, cannot swap
		// Both buffers are in use: currentBuffer is full and bgBuffer is flushing
		// Restore currentBuffer offset/index and return (caller should use direct write)
		atomic.StoreInt64(&currentBuf.writeOffset, oldOffset)
		atomic.StoreInt64(&currentBuf.fileIndex, oldIndex)
		return
	}

	// Atomically swap currentBuffer with bgBuffer using atomic.SwapPointer
	// This is a true atomic swap operation
	oldCurrentBuf := (*BatchWriterBuffer)(atomic.SwapPointer(&bwm.currentBuffer, unsafe.Pointer(bgBuf)))
	atomic.StorePointer(&bwm.bgBuffer, unsafe.Pointer(oldCurrentBuf))

	// After swapping, next writes use the new current buffer
	// Collect file infos from the just-flushed buffer (now bgBuffer)
	fileInfos := make([]*PackagedFileInfo, 0, oldIndex)
	for i := int64(0); i < oldIndex; i++ {
		if i < int64(len(currentBuf.fileInfos)) && currentBuf.fileInfos[i] != nil {
			fileInfos = append(fileInfos, currentBuf.fileInfos[i])
		}
	}

	if len(fileInfos) == 0 {
		return
	}

	// Find last valid file info to determine buffer size
	// We need to get the size from the adapter
	var lastOffset int64
	var lastSize int64
	for _, fileInfo := range fileInfos {
		if fileInfo != nil {
			// Get offset and size from file info
			// This is adapter-specific, so we need a helper method
			offset, size := bwm.getFileInfoOffsetSize(fileInfo)
			if size > 0 {
				totalOffset := offset + size
				if totalOffset > lastOffset+lastSize {
					lastOffset = offset
					lastSize = size
				}
			}
		}
	}

	if lastSize == 0 {
		return
	}

	// Create packaged file infos for all files in buffer
	bufferSize := lastOffset + lastSize
	pkgFileInfos := make([]*PackagedFileInfo, 0, len(fileInfos))

	var currentOffset uint32 = 0
	for _, fileInfo := range fileInfos {
		if fileInfo == nil {
			continue
		}

		_, size := bwm.getFileInfoOffsetSize(fileInfo)
		if size <= 0 {
			continue
		}

		// Create packaged file info with pkgOffset
		pkgFileInfo := bwm.createPackagedFileInfo(*fileInfo, currentOffset)
		pkgFileInfos = append(pkgFileInfos, pkgFileInfo)
		currentOffset += uint32(size)
	}

	// Flush package to disk (uses buffer slice directly, no data copying)
	if err := bwm.flushPackage(ctx, pkgFileInfos, bufferSize, currentBuf.buffer); err != nil {
		fmt.Printf("Error flushing package: %v\n", err)
	}

	// Clear the old buffer and file infos for reuse (zero out metadata)
	// The actual buffer content will be overwritten on next writes
	// Also remove from pending objects cache since they're now flushed
	for i := int64(0); i < oldIndex; i++ {
		if i < int64(len(currentBuf.fileInfos)) && currentBuf.fileInfos[i] != nil {
			fileInfo := currentBuf.fileInfos[i]
			// Remove from pending objects cache
			if fileID := bwm.getFileIDFromFileInfo(fileInfo); fileID > 0 {
				bwm.pendingObjects.Delete(fileID)
			}
			currentBuf.fileInfos[i] = nil
		}
	}

	// Reset bgBuffer's offset to 0 to indicate it's ready for next swap
	// Note: currentBuf is now bgBuffer after swap
	atomic.StoreInt64(&currentBuf.writeOffset, 0)
	atomic.StoreInt64(&currentBuf.fileIndex, 0)
}

// getFileInfoOffsetSize extracts offset and size from file info
func (bwm *BatchWriter) getFileInfoOffsetSize(fileInfo *PackagedFileInfo) (int64, int64) {
	return fileInfo.Offset, fileInfo.Size
}

// flushPackage flushes a single packaged data block
// Uses buffer slice directly, no data copying
func (bwm *BatchWriter) flushPackage(ctx context.Context, fileInfos []*PackagedFileInfo, bufferSize int64, buffer []byte) error {
	if len(fileInfos) == 0 || bufferSize == 0 {
		return nil
	}

	// 1. Generate packaged data block ID
	pkgID := core.NewID()
	if pkgID <= 0 {
		return fmt.Errorf("failed to generate package ID")
	}

	// 2. Write packaged data block directly from buffer slice
	bufferData := buffer[:bufferSize]
	_, err := bwm.handler.PutData(ctx, bwm.bktID, pkgID, 0, bufferData)
	if err != nil {
		return fmt.Errorf("failed to write package data (bktID=%d, pkgID=%d, size=%d): %v", bwm.bktID, pkgID, bufferSize, err)
	}

	// 3. Create DataInfo and ObjectInfo for each file
	// Pre-allocate slices with known capacity to reduce memory allocations
	dataInfos := make([]*core.DataInfo, 0, len(fileInfos))
	objectInfos := make([]*core.ObjectInfo, 0, len(fileInfos))

	// Cache Now() call to avoid repeated system calls
	now := core.Now()

	for _, pkgInfo := range fileInfos {
		dataID := pkgInfo.DataID
		if dataID <= 0 {
			dataID = core.NewID()
			if dataID <= 0 {
				continue
			}
		}

		// Create DataInfo
		dataInfo := &core.DataInfo{
			ID:        dataID,
			Size:      pkgInfo.Size,
			OrigSize:  pkgInfo.OrigSize,
			PkgID:     pkgID,
			PkgOffset: pkgInfo.PkgOffset,
		}
		dataInfos = append(dataInfos, dataInfo)

		// Create or update ObjectInfo
		objectInfo := &core.ObjectInfo{
			ID:     pkgInfo.ObjectID,
			PID:    pkgInfo.PID,
			DataID: dataID,
			Size:   pkgInfo.Size,
			MTime:  now, // Use cached timestamp
			Type:   core.OBJ_TYPE_FILE,
			Name:   pkgInfo.Name,
		}
		objectInfos = append(objectInfos, objectInfo)
	}

	// 4. Batch write DataInfo and ObjectInfo together in a single transaction
	// Optimization: Use PutDataInfoAndObj to reduce database round trips
	if len(dataInfos) > 0 || len(objectInfos) > 0 {
		// Both DataInfo and ObjectInfo exist, use combined write
		err = bwm.handler.PutDataInfoAndObj(ctx, bwm.bktID, dataInfos, objectInfos)
		if err != nil {
			return fmt.Errorf("failed to write data and object infos: %v", err)
		}
	}

	return nil
}

// AddFile adds file data to batch write buffer
// Returns (success, dataID, error)
// dataID is returned immediately for API compatibility
func (bwm *BatchWriter) AddFile(fileID int64, data []byte, pid int64, name string, origSize int64) (bool, int64, error) {
	if len(data) == 0 {
		return true, 0, nil
	}

	kind := uint32(0)
	if origSize <= 0 {
		origSize = int64(len(data))
	}

	processedSize := int64(len(data))

	// Smart decision: if file is too large relative to buffer size, skip batch write
	// This prevents large files from causing frequent flushes and performance degradation
	// For files larger than 30% of buffer, direct write is more efficient
	maxFileSize := int64(float64(bwm.bufferSize) * maxFileSizeRatio)
	if processedSize > maxFileSize {
		// File is too large for batch write, return false to indicate direct write should be used
		return false, 0, nil
	}

	// Generate DataID upfront
	dataID := core.NewID()
	if dataID <= 0 {
		return false, 0, fmt.Errorf("failed to generate DataID")
	}

	// Try to write, may need retry (if space is insufficient, flush first)
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		// Get current buffer (atomic load with memory barrier)
		currentBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.currentBuffer))

		// Check current buffer space
		currOffset := atomic.LoadInt64(&currentBuf.writeOffset)
		if currOffset+processedSize > bwm.bufferSize {
			// Current buffer is full
			// Check if the background buffer is also full
			bgBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.bgBuffer))
			bgOff := atomic.LoadInt64(&bgBuf.writeOffset)
			if bgOff+processedSize > bwm.bufferSize {
				// Both buffers are full, cannot use batch write
				// Caller should write directly to storage, not use batch writer
				return false, 0, nil
			}
			// One buffer full, need to flush the current buffer
			// Let caller know to flush before retrying
			return false, 0, nil
		}

		// Reserve space in current buffer (atomic operation)
		oldOffset := atomic.LoadInt64(&currentBuf.writeOffset)
		newOffset := atomic.AddInt64(&currentBuf.writeOffset, processedSize)

		// Check again if exceeds buffer size (race condition check)
		if newOffset > bwm.bufferSize {
			// Space insufficient after reservation, return false
			return false, 0, nil
		}

		// Memory barrier: atomic.AddInt64 already provides memory barrier

		// Write data to current buffer
		copy(currentBuf.buffer[oldOffset:newOffset], data)

		// Get file info index
		idx := atomic.AddInt64(&currentBuf.fileIndex, 1) - 1
		if idx >= bwm.maxFileInfos {
			// File info list is full, return false
			return false, 0, nil
		}

		// Create file info
		fileInfo := bwm.createFileInfo(fileID, dataID, oldOffset, processedSize, origSize, kind, pid, name)
		currentBuf.fileInfos[idx] = fileInfo

		// Store in pending objects cache for visibility before flush
		bwm.pendingObjects.Store(fileID, fileInfo)

		return true, dataID, nil
	}

	return false, 0, nil
}

// GetPendingObject gets a pending object from the buffer (before flush)
// Returns the file info if the object is in the buffer but not yet flushed
func (bwm *BatchWriter) GetPendingObject(fileID int64) (*PackagedFileInfo, bool) {
	if fileInfo, ok := bwm.pendingObjects.Load(fileID); ok {
		if pkgInfo, ok := fileInfo.(*PackagedFileInfo); ok {
			return pkgInfo, true
		}
	}
	return nil, false
}

// GetPendingObjects gets all pending objects from pendingObjects cache
// Returns a map of fileID -> fileInfo for objects that are in buffers but not yet flushed
func (bwm *BatchWriter) GetPendingObjects() map[int64]*PackagedFileInfo {
	result := make(map[int64]*PackagedFileInfo)
	bwm.pendingObjects.Range(func(key, value interface{}) bool {
		if fileID, ok := key.(int64); ok {
			if pkgInfo, ok := value.(*PackagedFileInfo); ok {
				result[fileID] = pkgInfo
			}
		}
		return true
	})
	return result
}

// getFileIDFromFileInfo extracts fileID from file info
func (bwm *BatchWriter) getFileIDFromFileInfo(fileInfo *PackagedFileInfo) int64 {
	return fileInfo.ObjectID
}

// ReadPendingData reads data from buffer for a pending object
// Returns data if object is in buffer, nil otherwise
func (bwm *BatchWriter) ReadPendingData(fileID int64) []byte {
	// Get current buffer (atomic load with memory barrier)
	currentBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.currentBuffer))
	if fileInfo, ok := bwm.GetPendingObject(fileID); ok {
		return currentBuf.buffer[fileInfo.Offset : fileInfo.Offset+fileInfo.Size]
	}

	bgBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.bgBuffer))
	if fileInfo, ok := bwm.GetPendingObject(fileID); ok {
		return bgBuf.buffer[fileInfo.Offset : fileInfo.Offset+fileInfo.Size]
	}
	return nil
}

// createFileInfo creates a file info from the given parameters
func (bwm *BatchWriter) createFileInfo(fileID, dataID, offset, size, origSize int64, kind uint32, pid int64, name string) *PackagedFileInfo {
	return &PackagedFileInfo{
		ObjectID:  fileID,
		DataID:    dataID,
		Offset:    offset,
		PkgOffset: 0,
		Size:      size,
		OrigSize:  origSize,
		PID:       pid,
		Name:      name,
	}
}

// createPackagedFileInfo creates packaged file info from file info
func (bwm *BatchWriter) createPackagedFileInfo(fileInfo PackagedFileInfo, pkgOffset uint32) *PackagedFileInfo {
	return &PackagedFileInfo{
		ObjectID:  fileInfo.ObjectID,
		DataID:    fileInfo.DataID,
		Offset:    fileInfo.Offset,
		PkgOffset: pkgOffset,
		Size:      fileInfo.Size,
		OrigSize:  fileInfo.OrigSize,
		PID:       fileInfo.PID,
		Name:      fileInfo.Name,
	}
}
