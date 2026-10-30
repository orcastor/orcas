package util

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/orcastor/orcas/core"
)

// PackagedFileInfo describes one independently encoded file in a package.
type PackagedFileInfo struct {
	ObjectID  int64
	DataID    int64
	Offset    int64
	PkgOffset uint32
	Size      int64
	OrigSize  int64
	PID       int64
	Name      string
	Kind      uint32
}

// A buffer is writable only while it is current. FlushAll exclusively owns the
// background buffer until both data and metadata have been persisted.
type BatchWriterBuffer struct {
	buffer    []byte
	fileInfos []*PackagedFileInfo
}

type pendingFile struct {
	info   *PackagedFileInfo
	buffer *BatchWriterBuffer
}

// BatchWriter batches small files. mu protects reservations, pending reads and
// publication; flushMu serializes disk writes without blocking the next buffer.
type BatchWriter struct {
	mu              sync.Mutex
	flushMu         sync.Mutex
	currentBuffer   *BatchWriterBuffer
	bgBuffer        *BatchWriterBuffer
	bufferSize      int64
	maxFileInfos    int64
	flushWindow     time.Duration
	handler         core.Handler
	bktID           int64
	pendingObjects  map[int64]pendingFile
	flushTimer      *time.Timer
	timerGeneration uint64
	flushCtx        context.Context
	onFlushComplete func([]*core.ObjectInfo)
}

var (
	maxFileSizeRatio = 0.35
	batchWriters     sync.Map
)

func GetBatchWriterForBucket(handler core.Handler, bktID int64) *BatchWriter {
	if val, ok := batchWriters.Load(bktID); ok {
		return val.(*BatchWriter)
	}
	mgr := createBatchWriter(handler, bktID)
	if mgr == nil {
		return nil
	}
	actual, _ := batchWriters.LoadOrStore(bktID, mgr)
	return actual.(*BatchWriter)
}

func FlushBatchWriterForBucket(ctx context.Context, bktID int64) error {
	if val, ok := batchWriters.Load(bktID); ok {
		return val.(*BatchWriter).FlushAll(ctx)
	}
	return nil
}

func FlushAllBatchWriters(ctx context.Context) error {
	var errs []error
	batchWriters.Range(func(_, value interface{}) bool {
		if err := value.(*BatchWriter).FlushAll(ctx); err != nil {
			errs = append(errs, err)
		}
		return true
	})
	return errors.Join(errs...)
}

// Deprecated: use GetBatchWriterForBucket to share a writer for a bucket.
func GetBatchWriteManager(handler core.Handler, bktID int64) *BatchWriter {
	return createBatchWriter(handler, bktID)
}

func createBatchWriter(handler core.Handler, bktID int64) *BatchWriter {
	config := core.GetWriteBufferConfig()
	if !config.BatchWriteEnabled {
		return nil
	}
	return &BatchWriter{
		bufferSize: config.MaxBufferSize, maxFileInfos: config.MaxBufferWrites,
		flushWindow: config.BufferWindow, handler: handler, bktID: bktID,
		pendingObjects: make(map[int64]pendingFile),
	}
}

// FlushAll persists every file accepted before this call acquires flushMu.
// On failure the detached buffer remains readable and is retried first. At most
// two buffers are drained so continuous producers cannot starve a flush caller.
func (bwm *BatchWriter) FlushAll(ctx context.Context) (err error) {
	if ctx != nil {
		bwm.SetFlushContext(ctx)
	} else {
		ctx = bwm.getFlushContext()
	}
	bwm.flushMu.Lock()
	var completed []*core.ObjectInfo
	defer func() {
		bwm.mu.Lock()
		bwm.schedulePeriodicFlushLocked()
		callback := bwm.onFlushComplete
		bwm.mu.Unlock()
		bwm.flushMu.Unlock()
		if callback != nil && len(completed) > 0 {
			callback(completed)
		}
	}()
	for batch := 0; batch < 2; batch++ {
		bwm.mu.Lock()
		bwm.cancelFlushTimerLocked()
		buf := bwm.bgBuffer
		if buf == nil {
			buf = bwm.currentBuffer
			if buf == nil || len(buf.fileInfos) == 0 {
				bwm.mu.Unlock()
				return nil
			}
			bwm.currentBuffer = nil
			bwm.bgBuffer = buf
		}
		infos := make([]*PackagedFileInfo, 0, len(buf.fileInfos))
		for _, info := range buf.fileInfos {
			pending, ok := bwm.pendingObjects[info.ObjectID]
			if !ok || pending.info != info || pending.buffer != buf {
				continue
			}
			copyInfo := *info
			// Deleted files leave holes; offsets refer to the original package bytes.
			copyInfo.PkgOffset = uint32(info.Offset)
			infos = append(infos, &copyInfo)
		}
		bwm.mu.Unlock()

		objects, flushErr := bwm.flushPackage(ctx, infos, int64(len(buf.buffer)), buf.buffer)
		if flushErr != nil {
			return flushErr
		}

		bwm.mu.Lock()
		for _, info := range buf.fileInfos {
			if pending, ok := bwm.pendingObjects[info.ObjectID]; ok && pending.info == info && pending.buffer == buf {
				delete(bwm.pendingObjects, info.ObjectID)
			}
		}
		bwm.bgBuffer = nil // Release idle backing arrays rather than pinning 16 MiB per bucket.
		bwm.mu.Unlock()
		completed = append(completed, objects...)
	}
	return nil
}

func (bwm *BatchWriter) SetOnFlushComplete(callback func([]*core.ObjectInfo)) {
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	bwm.onFlushComplete = callback
}

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

// AddFile reserves bytes and metadata together. A rejected write leaves both
// capacities unchanged. A successful return means buffered, not yet durable.
func (bwm *BatchWriter) AddFile(fileID int64, data []byte, pid int64, name string, origSize int64, kind uint32) (bool, int64, error) {
	if len(data) == 0 {
		return true, 0, nil
	}
	size := int64(len(data))
	if size > int64(float64(bwm.bufferSize)*maxFileSizeRatio) {
		return false, 0, nil
	}
	if origSize <= 0 {
		origSize = size
	}
	dataID := core.NewID()
	if dataID <= 0 {
		return false, 0, fmt.Errorf("failed to generate DataID")
	}
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	if _, exists := bwm.pendingObjects[fileID]; exists {
		return false, 0, fmt.Errorf("file %d already buffered", fileID)
	}
	buf := bwm.currentBuffer
	if buf != nil && (int64(len(buf.buffer))+size > bwm.bufferSize || int64(len(buf.fileInfos)) >= bwm.maxFileInfos) {
		return false, 0, nil
	}
	if bwm.maxFileInfos <= 0 || size > bwm.bufferSize || size > int64(^uint32(0)) {
		return false, 0, nil
	}
	if buf == nil {
		buf = &BatchWriterBuffer{}
		bwm.currentBuffer = buf
	}
	offset := int64(len(buf.buffer))
	info := &PackagedFileInfo{ObjectID: fileID, DataID: dataID, Offset: offset, Size: size, OrigSize: origSize, PID: pid, Name: name, Kind: kind}
	buf.buffer = append(buf.buffer, data...)
	buf.fileInfos = append(buf.fileInfos, info)
	bwm.pendingObjects[fileID] = pendingFile{info, buf}
	bwm.schedulePeriodicFlushLocked()
	return true, dataID, nil
}

// Pending metadata and data are snapshots: callers may retain them across flushes.
func (bwm *BatchWriter) GetPendingObject(fileID int64) (*PackagedFileInfo, bool) {
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	pending, ok := bwm.pendingObjects[fileID]
	if !ok {
		return nil, false
	}
	info := *pending.info
	return &info, true
}

func (bwm *BatchWriter) GetPendingObjects() map[int64]*PackagedFileInfo {
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	result := make(map[int64]*PackagedFileInfo, len(bwm.pendingObjects))
	for id, pending := range bwm.pendingObjects {
		info := *pending.info
		result[id] = &info
	}
	return result
}

// UpdatePendingObject may change metadata until its buffer is detached for I/O.
func (bwm *BatchWriter) UpdatePendingObject(fileID int64, updateFn func(*PackagedFileInfo)) bool {
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	pending, ok := bwm.pendingObjects[fileID]
	if !ok || pending.buffer == bwm.bgBuffer {
		return false
	}
	info := *pending.info
	updateFn(&info)
	// Storage identity and byte reservations are immutable.
	info.ObjectID, info.DataID, info.Offset, info.Size = pending.info.ObjectID, pending.info.DataID, pending.info.Offset, pending.info.Size
	*pending.info = info
	return true
}

func (bwm *BatchWriter) RemovePendingObject(fileID int64) bool {
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	pending, ok := bwm.pendingObjects[fileID]
	if !ok || pending.buffer == bwm.bgBuffer {
		return false
	}
	delete(bwm.pendingObjects, fileID)
	return true
}

func (bwm *BatchWriter) ReadPendingData(fileID int64) []byte {
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	pending, ok := bwm.pendingObjects[fileID]
	if !ok {
		return nil
	}
	info := pending.info
	return append([]byte(nil), pending.buffer.buffer[info.Offset:info.Offset+info.Size]...)
}

func (bwm *BatchWriter) cancelFlushTimer() {
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	bwm.cancelFlushTimerLocked()
}

func (bwm *BatchWriter) cancelFlushTimerLocked() {
	bwm.timerGeneration++
	if bwm.flushTimer != nil {
		bwm.flushTimer.Stop()
		bwm.flushTimer = nil
	}
}

func (bwm *BatchWriter) schedulePeriodicFlushLocked() {
	if bwm.flushTimer != nil || len(bwm.pendingObjects) == 0 {
		return
	}
	delay := bwm.flushWindow
	if delay <= 0 {
		delay = time.Second
	}
	generation := bwm.timerGeneration
	bwm.flushTimer = time.AfterFunc(delay, func() {
		bwm.mu.Lock()
		if generation != bwm.timerGeneration {
			bwm.mu.Unlock()
			return
		}
		bwm.flushTimer = nil
		bwm.mu.Unlock()
		if err := bwm.FlushAll(nil); err != nil {
			fmt.Printf("Error flushing package: %v\n", err)
		}
	})
}

func (bwm *BatchWriter) SetFlushContext(ctx context.Context) {
	if bwm == nil || ctx == nil {
		return
	}
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	// Scheduled persistence must survive the request that accepted the data.
	bwm.flushCtx = context.WithoutCancel(ctx)
}

func (bwm *BatchWriter) getFlushContext() context.Context {
	bwm.mu.Lock()
	defer bwm.mu.Unlock()
	if bwm.flushCtx != nil {
		return bwm.flushCtx
	}
	return context.Background()
}
