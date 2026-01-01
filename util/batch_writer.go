package util

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
	Kind      uint32 // Compression/encryption flags (DATA_CMPR_*, DATA_ENDEC_*)
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

	// Periodic flush control
	lastFlushTime int64        // Last flush time (UnixNano, atomic access)
	flushTimer    atomic.Value // *time.Timer for periodic flush
	flushCtx      atomic.Value // Stored context for scheduled flushes
	flushUID      atomic.Value // Stored UID for scheduled flushes (int64, fallback if context doesn't have UID)

	// Callback function to update dirListCache after flush (optional, set by VFS layer)
	onFlushComplete func(objectInfos []*core.ObjectInfo)
}

var (
	// Default configuration
	// Aggressively optimized for small files (< 64KB) - large scale small file writes
	// Balanced: increased buffer size for better batching while maintaining responsiveness
	defaultBufferSize     = int64(24 << 20) // 24MB - balanced increase for better batching of many small files
	defaultMaxFileInfos   = int64(3 << 10)  // 3072 - balanced increase for more files per batch
	defaultFlushWindow    = 1 * time.Second // 1s - default flush window for batch writer
	defaultMaxPackageSize = int64(24 << 20) // 24MB - balanced for larger packages

	// Threshold for determining if a file is too large for batch write
	// If a single file exceeds this percentage of buffer size, it should be written directly
	// Balanced: slightly increased threshold to allow larger files in batch write
	maxFileSizeRatio = 0.35 // 35% of buffer size (balanced between 30% and 40%)

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

	// Initialize lastFlushTime to current time
	atomic.StoreInt64(&mgr.lastFlushTime, time.Now().UnixNano())

	// Initialize flushTimer with nil
	mgr.flushTimer.Store((*time.Timer)(nil))

	// Start with buf1 as current, buf2 as background
	// Use atomic.StorePointer for atomic pointer assignment
	atomic.StorePointer(&mgr.currentBuffer, unsafe.Pointer(buf1))
	atomic.StorePointer(&mgr.bgBuffer, unsafe.Pointer(buf2))

	return mgr
}

// FlushAll flushes all pending write data
func (bwm *BatchWriter) FlushAll(ctx context.Context) {
	if ctx != nil {
		bwm.SetFlushContext(ctx)
	}
	bwm.flush(ctx)
}

// ensureContextWithUID ensures the context has a valid UID for permission checks
// If the context doesn't have UID, uses the stored UID from SetFlushContext
func (bwm *BatchWriter) ensureContextWithUID(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	// Check if context already has UID
	if v, ok := ctx.Value("o").(map[string]interface{}); ok {
		if uid, okk := v["uid"].(int64); okk && uid > 0 {
			// Context already has valid UID
			return ctx
		}
	}

	// Context doesn't have UID, try to use stored UID from SetFlushContext
	if val := bwm.flushUID.Load(); val != nil {
		if uid, ok := val.(int64); ok && uid > 0 {
			// Create context with stored UID
			o := map[string]interface{}{
				"uid": uid,
			}
			// Preserve other context values if they exist
			if v, ok := ctx.Value("o").(map[string]interface{}); ok {
				for k, val := range v {
					if k != "uid" {
						o[k] = val
					}
				}
			}
			return context.WithValue(ctx, "o", o)
		}
	}

	// If we can't get UID, return original context
	// The permission check will fail, but at least we tried
	return ctx
}

// flush flushes all pending write data (lock-free)
// Atomically gets current write position and file info, then packages and writes
func (bwm *BatchWriter) flush(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Ensure context has UID before flushing
	ctx = bwm.ensureContextWithUID(ctx)
	// Get current buffer (for writing) - atomic load
	currentBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.currentBuffer))

	// Get old offset and index from current buffer, atomically reset to 0
	oldOffset := atomic.SwapInt64(&currentBuf.writeOffset, 0)
	oldIndex := atomic.SwapInt64(&currentBuf.fileIndex, 0)

	if oldOffset == 0 || oldIndex == 0 {
		return // No pending write data
	}

	// Cancel existing timer and reset
	bwm.cancelFlushTimer()

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
	// Optimized: pre-allocate with known capacity and combine loops
	fileInfos := make([]*PackagedFileInfo, 0, oldIndex)
	var lastOffset int64
	var lastSize int64

	// Single pass: collect file infos and find buffer size simultaneously
	for i := int64(0); i < oldIndex; i++ {
		if i < int64(len(currentBuf.fileInfos)) && currentBuf.fileInfos[i] != nil {
			fileInfo := currentBuf.fileInfos[i]
			fileInfos = append(fileInfos, fileInfo)

			// Track last offset and size for buffer size calculation
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

	if len(fileInfos) == 0 || lastSize == 0 {
		return
	}

	// Create packaged file infos for all files in buffer
	bufferSize := lastOffset + lastSize
	pkgFileInfos := make([]*PackagedFileInfo, 0, len(fileInfos))

	var currentOffset uint32 = 0
	for _, fileInfo := range fileInfos {
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
	objectInfos, err := bwm.flushPackage(ctx, pkgFileInfos, bufferSize, currentBuf.buffer)
	if err != nil {
		fmt.Printf("Error flushing package: %v\n", err)
		// If flush failed, restore the offset and index to the buffer that was swapped
		// Note: currentBuf is now bgBuffer after swap, but we need to restore to the original currentBuffer
		// Since we already swapped, we need to restore to oldCurrentBuf (which is now bgBuffer)
		// Actually, we should restore to the buffer that contains the data, which is currentBuf (old currentBuffer, now bgBuffer)
		atomic.StoreInt64(&currentBuf.writeOffset, oldOffset)
		atomic.StoreInt64(&currentBuf.fileIndex, oldIndex)
		// Reschedule periodic flush
		bwm.schedulePeriodicFlush()
		return
	}

	// Update last flush time only after successful flush
	atomic.StoreInt64(&bwm.lastFlushTime, time.Now().UnixNano())

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

	// Call callback to update dirListCache (if set by VFS layer)
	// This ensures flushed objects are immediately added to dirListCache
	// instead of requiring getDirListWithCache to merge from pendingObjects
	if bwm.onFlushComplete != nil && len(objectInfos) > 0 {
		bwm.onFlushComplete(objectInfos)
	}

	// Reset bgBuffer's offset to 0 to indicate it's ready for next swap
	// Note: currentBuf is now bgBuffer after swap
	atomic.StoreInt64(&currentBuf.writeOffset, 0)
	atomic.StoreInt64(&currentBuf.fileIndex, 0)

	// Schedule next periodic flush if there's pending data
	bwm.schedulePeriodicFlush()
}

// getFileInfoOffsetSize extracts offset and size from file info
func (bwm *BatchWriter) getFileInfoOffsetSize(fileInfo *PackagedFileInfo) (int64, int64) {
	return fileInfo.Offset, fileInfo.Size
}

// SetOnFlushComplete sets a callback function to be called after flush completes successfully
// This allows VFS layer to update dirListCache when objects are flushed
func (bwm *BatchWriter) SetOnFlushComplete(callback func(objectInfos []*core.ObjectInfo)) {
	bwm.onFlushComplete = callback
}

// flushPackage flushes a single packaged data block
// Uses buffer slice directly, no data copying
// Returns objectInfos for successfully flushed objects
func (bwm *BatchWriter) flushPackage(ctx context.Context, fileInfos []*PackagedFileInfo, bufferSize int64, buffer []byte) ([]*core.ObjectInfo, error) {
	if len(fileInfos) == 0 || bufferSize == 0 {
		return nil, nil
	}

	// 1. Generate packaged data block ID
	pkgID := core.NewID()
	if pkgID <= 0 {
		return nil, fmt.Errorf("failed to generate package ID")
	}

	// 2. Write package data block (files are already compressed/encrypted individually)
	// Each file in the package has been processed (compressed/encrypted) separately
	// Package data is the concatenation of processed file data
	packageData := buffer[:bufferSize]
	_, err := bwm.handler.PutData(ctx, bwm.bktID, pkgID, 0, packageData)
	if err != nil {
		return nil, fmt.Errorf("failed to write package data (bktID=%d, pkgID=%d, size=%d): %v", bwm.bktID, pkgID, len(packageData), err)
	}

	// 3. Create DataInfo and ObjectInfo for each file
	// Pre-allocate slices with known capacity to reduce memory allocations
	dataInfos := make([]*core.DataInfo, 0, len(fileInfos))
	objectInfos := make([]*core.ObjectInfo, 0, len(fileInfos))

	// Cache Now() call to avoid repeated system calls (optimization for many small files)
	now := core.Now()

	// Optimized loop: minimize allocations and function calls
	for i := range fileInfos {
		pkgInfo := fileInfos[i]
		if pkgInfo == nil {
			continue
		}

		dataID := pkgInfo.DataID
		if dataID <= 0 {
			dataID = core.NewID()
			if dataID <= 0 {
				continue
			}
		}

		// Create DataInfo (inline struct creation for better performance)
		// For per-file compression/encryption:
		// - Size: processed file size in package (after compression/encryption)
		// - OrigSize: original file size (before compression/encryption)
		// - PkgID: package ID (contains processed file data)
		// - PkgOffset: offset in package (points to processed file data)
		// - Kind: per-file compression/encryption flags
		// Note: When reading, use PkgOffset to directly locate file data in package,
		// then decrypt/decompress the file data individually
		processedFileSize := pkgInfo.Size // Size of processed file data in package
		dataInfos = append(dataInfos, &core.DataInfo{
			ID:        dataID,
			Size:      processedFileSize, // Processed file size in package (after compression/encryption)
			OrigSize:  pkgInfo.OrigSize,  // Original file size (before compression/encryption)
			PkgID:     pkgID,             // Package ID (contains processed file data)
			PkgOffset: pkgInfo.PkgOffset, // Offset in package (points to processed file data)
			Kind:      pkgInfo.Kind,      // Use per-file compression/encryption flags
		})

		// Create ObjectInfo (inline struct creation for better performance)
		objectInfos = append(objectInfos, &core.ObjectInfo{
			ID:     pkgInfo.ObjectID,
			PID:    pkgInfo.PID,
			DataID: dataID,
			Size:   pkgInfo.OrigSize, // Use original size, not processed size
			MTime:  now,              // Use cached timestamp
			Type:   core.OBJ_TYPE_FILE,
			Name:   pkgInfo.Name,
		})
	}

	// 4. Batch write DataInfo and ObjectInfo together in a single transaction
	// Optimization: Use PutDataInfoAndObj to reduce database round trips
	if len(dataInfos) > 0 || len(objectInfos) > 0 {
		// Both DataInfo and ObjectInfo exist, use combined write
		err = bwm.handler.PutDataInfoAndObj(ctx, bwm.bktID, dataInfos, objectInfos)
		if err != nil {
			return nil, fmt.Errorf("failed to write data and object infos: %v", err)
		}
	}

	return objectInfos, nil
}

// AddFile adds file data to batch write buffer
// Returns (success, dataID, error)
// dataID is returned immediately for API compatibility
// kind: compression/encryption flags (DATA_CMPR_*, DATA_ENDEC_*)
func (bwm *BatchWriter) AddFile(fileID int64, data []byte, pid int64, name string, origSize int64, kind uint32) (bool, int64, error) {
	if len(data) == 0 {
		return true, 0, nil
	}

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

	// Schedule periodic flush if not already scheduled (optimized: only schedule if needed)
	// Check if timer already exists first to avoid unnecessary work
	if timerVal := bwm.flushTimer.Load(); timerVal == nil {
		// Only schedule if no timer exists (optimization for high concurrency)
		bwm.schedulePeriodicFlush()
	}

	return true, dataID, nil
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

// UpdatePendingObject updates a pending object in the cache
// This is useful for updating metadata (e.g., name) before flushing
func (bwm *BatchWriter) UpdatePendingObject(fileID int64, updateFn func(*PackagedFileInfo)) bool {
	if pkgInfo, ok := bwm.GetPendingObject(fileID); ok && pkgInfo != nil {
		// Update the file info using the provided function
		updateFn(pkgInfo)
		// Store the updated file info back to cache
		bwm.pendingObjects.Store(fileID, pkgInfo)
		return true
	}
	return false
}

// RemovePendingObject removes a pending object from the cache
// This is useful when a file is deleted or renamed before flush
func (bwm *BatchWriter) RemovePendingObject(fileID int64) bool {
	if _, ok := bwm.GetPendingObject(fileID); ok {
		bwm.pendingObjects.Delete(fileID)
		return true
	}
	return false
}

// getFileIDFromFileInfo extracts fileID from file info
func (bwm *BatchWriter) getFileIDFromFileInfo(fileInfo *PackagedFileInfo) int64 {
	return fileInfo.ObjectID
}

// ReadPendingData reads data from buffer for a pending object
// Returns data if object is in buffer, nil otherwise
func (bwm *BatchWriter) ReadPendingData(fileID int64) []byte {
	// Get file info first to determine which buffer it's in
	fileInfo, ok := bwm.GetPendingObject(fileID)
	if !ok || fileInfo == nil {
		return nil
	}

	// Get current buffer (atomic load with memory barrier)
	currentBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.currentBuffer))
	// Check if file is in current buffer (check offset range)
	currentOffset := atomic.LoadInt64(&currentBuf.writeOffset)
	if fileInfo.Offset < currentOffset {
		// File is in current buffer
		if fileInfo.Offset+fileInfo.Size <= int64(len(currentBuf.buffer)) {
			return currentBuf.buffer[fileInfo.Offset : fileInfo.Offset+fileInfo.Size]
		}
	}

	// Check background buffer
	bgBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.bgBuffer))
	bgOffset := atomic.LoadInt64(&bgBuf.writeOffset)
	if fileInfo.Offset < bgOffset {
		// File is in background buffer
		if fileInfo.Offset+fileInfo.Size <= int64(len(bgBuf.buffer)) {
			return bgBuf.buffer[fileInfo.Offset : fileInfo.Offset+fileInfo.Size]
		}
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
		Kind:      kind, // Include compression/encryption flags
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
		Kind:      fileInfo.Kind, // Include compression/encryption flags
	}
}

// cancelFlushTimer cancels the existing flush timer
func (bwm *BatchWriter) cancelFlushTimer() {
	oldTimer := bwm.flushTimer.Swap((*time.Timer)(nil))
	if oldTimer != nil {
		if timer, ok := oldTimer.(*time.Timer); ok && timer != nil {
			timer.Stop()
		}
	}
}

// schedulePeriodicFlush schedules a periodic flush based on BufferWindow
// It ensures that if the last flush was recent (due to count threshold),
// the timer will wait for the remaining time until BufferWindow expires
// Optimized for high concurrency: minimize time.Now() calls and timer creation
func (bwm *BatchWriter) schedulePeriodicFlush() {
	// Check if timer already exists first (fast path for high concurrency)
	if timerVal := bwm.flushTimer.Load(); timerVal != nil {
		if timer, ok := timerVal.(*time.Timer); ok && timer != nil {
			// Timer already exists, don't create a new one
			return
		}
	}

	// Check if there's pending data to flush
	currentBuf := (*BatchWriterBuffer)(atomic.LoadPointer(&bwm.currentBuffer))
	currOffset := atomic.LoadInt64(&currentBuf.writeOffset)
	currIndex := atomic.LoadInt64(&currentBuf.fileIndex)

	// If no pending data, don't schedule
	if currOffset == 0 || currIndex == 0 {
		return
	}

	// Get last flush time and current time (minimize time.Now() calls)
	lastFlushNano := atomic.LoadInt64(&bwm.lastFlushTime)
	now := time.Now()
	lastFlushTime := time.Unix(0, lastFlushNano)

	// Calculate time elapsed since last flush
	elapsed := now.Sub(lastFlushTime)

	// Calculate remaining time until BufferWindow expires
	var delay time.Duration
	if elapsed >= bwm.flushWindow {
		// Already exceeded BufferWindow, flush immediately (with minimal delay)
		delay = 10 * time.Millisecond
	} else {
		// Wait for remaining time
		delay = bwm.flushWindow - elapsed
	}

	// Create new timer
	newTimer := time.AfterFunc(delay, func() {
		// Clear timer atomically
		bwm.flushTimer.Store((*time.Timer)(nil))

		// Check if we should flush
		// Get last flush time again to ensure we haven't flushed recently
		lastFlushNano := atomic.LoadInt64(&bwm.lastFlushTime)
		lastFlushTime := time.Unix(0, lastFlushNano)
		now := time.Now()
		elapsed := now.Sub(lastFlushTime)

		// Only flush if BufferWindow has elapsed since last flush
		if elapsed >= bwm.flushWindow {
			ctx := bwm.getFlushContext()
			// Async flush, non-blocking
			go func() {
				bwm.flush(ctx)
			}()
		} else {
			// Reschedule for remaining time
			bwm.schedulePeriodicFlush()
		}
	})

	// Store new timer atomically (use CompareAndSwap to handle race condition)
	// If another goroutine already created a timer, cancel this one
	if !bwm.flushTimer.CompareAndSwap(nil, newTimer) {
		// Another goroutine created a timer first, cancel this one
		newTimer.Stop()
	}
}

// SetFlushContext stores the context used for asynchronous flush operations
// Also extracts and stores UID from context as fallback for permission checks
func (bwm *BatchWriter) SetFlushContext(ctx context.Context) {
	if bwm == nil || ctx == nil {
		return
	}
	bwm.flushCtx.Store(ctx)

	// Extract UID from context and store it separately as fallback
	// This ensures we have UID even if context is lost or doesn't have UID
	if v, ok := ctx.Value("o").(map[string]interface{}); ok {
		if uid, okk := v["uid"].(int64); okk && uid > 0 {
			bwm.flushUID.Store(uid)
		}
	}
}

func (bwm *BatchWriter) getFlushContext() context.Context {
	if bwm == nil {
		return context.Background()
	}
	if val := bwm.flushCtx.Load(); val != nil {
		if ctx, ok := val.(context.Context); ok && ctx != nil {
			return ctx
		}
	}
	return context.Background()
}
