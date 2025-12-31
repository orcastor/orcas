package vfs

import (
	"fmt"
	"sync/atomic"

	"github.com/orcastor/orcas/core"
)

// shouldCompressFileByName determines if a file should be compressed based on its name and VFS config
// This checks both the VFS compression configuration and the file extension
func shouldCompressFileByName(fs *OrcasFS, fileName string) bool {
	// Check if compression is enabled in VFS config
	if fs.CmprWay == 0 {
		return false
	}
	// Use the existing shouldCompressFile function from random_accessor.go
	// Pass empty data since we're only checking by name
	return shouldCompressFile(fileName, nil)
}

// shouldUseTempWriteArea determines if temporary write area should be used for this file
func (ra *RandomAccessor) shouldUseTempWriteArea(fileObj *core.ObjectInfo) bool {
	// Check if temp write area is enabled
	if ra.fs.tempWriteArea == nil || !ra.fs.tempWriteArea.config.Enabled {
		return false
	}

	// Scenario 0: Files that need encryption or compression
	// CRITICAL: If encryption or compression is enabled, ALWAYS use temp write area
	// This ensures data is properly processed before being written to disk
	needsEncrypt := ra.fs.EndecKey != ""
	needsCompress := ra.fs.CmprWay > 0 && shouldCompressFileByName(ra.fs, fileObj.Name)
	if needsEncrypt || needsCompress {
		DebugLog("[VFS shouldUseTempWriteArea] File needs encryption/compression: fileID=%d, needsEncrypt=%v, needsCompress=%v",
			ra.fileID, needsEncrypt, needsCompress)
		return true
	}

	// Scenario 1: Sparse files (qBittorrent scenario)
	// Always use temp write area for sparse files regardless of size
	if atomic.LoadInt64(&ra.sparseSize) > 0 {
		DebugLog("[VFS shouldUseTempWriteArea] Sparse file detected: fileID=%d, sparseSize=%d",
			ra.fileID, atomic.LoadInt64(&ra.sparseSize))
		return true
	}

	// Scenario 2: Random write pattern detection
	// If non-sequential writes are detected, use temp write area
	if ra.isRandomWritePattern() {
		DebugLog("[VFS shouldUseTempWriteArea] Random write pattern detected: fileID=%d", ra.fileID)
		return true
	}

	// Scenario 3: Modifying existing file
	// If file already has DataID and size > 0, it's a modification
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID && fileObj.Size > 0 {
		DebugLog("[VFS shouldUseTempWriteArea] Modifying existing file: fileID=%d, dataID=%d, size=%d",
			ra.fileID, fileObj.DataID, fileObj.Size)
		return true
	}

	// Scenario 4: Large files (> threshold)
	// Check current file size OR buffered data size
	currentSize := fileObj.Size

	// Also consider data in write buffer
	if ra.buffer != nil {
		writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
		if writeIndex > 0 {
			// Calculate total size including buffered writes
			maxOffset := currentSize
			for i := int64(0); i < writeIndex && i < int64(len(ra.buffer.operations)); i++ {
				op := ra.buffer.operations[i]
				if len(op.Data) > 0 {
					endOffset := op.Offset + int64(len(op.Data))
					if endOffset > maxOffset {
						maxOffset = endOffset
					}
				}
			}
			if maxOffset > currentSize {
				currentSize = maxOffset
			}
		}
	}

	if currentSize >= ra.fs.tempWriteArea.config.SmallFileThreshold {
		DebugLog("[VFS shouldUseTempWriteArea] Large file: fileID=%d, size=%d, threshold=%d",
			ra.fileID, currentSize, ra.fs.tempWriteArea.config.SmallFileThreshold)
		return true
	}

	return false
}

// isRandomWritePattern detects if the write pattern is random (non-sequential)
func (ra *RandomAccessor) isRandomWritePattern() bool {
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	if writeIndex < 2 {
		return false
	}

	// Check for non-contiguous writes in buffer
	lastEnd := int64(-1)
	for i := int64(0); i < writeIndex && i < int64(len(ra.buffer.operations)); i++ {
		op := ra.buffer.operations[i]
		if len(op.Data) == 0 {
			continue
		}

		if lastEnd >= 0 && op.Offset != lastEnd {
			// Found non-contiguous write - this is random write pattern
			return true
		}
		lastEnd = op.Offset + int64(len(op.Data))
	}

	return false
}

// writeToTempWriteArea writes data to temporary write area
func (ra *RandomAccessor) writeToTempWriteArea(offset int64, data []byte) error {
	// Get or create TempWriteFile
	twf, err := ra.getOrCreateTempWriteFile()
	if err != nil {
		return err
	}

	// Write data
	return twf.Write(offset, data)
}

// getOrCreateTempWriteFile gets or creates a TempWriteFile for this RandomAccessor
func (ra *RandomAccessor) getOrCreateTempWriteFile() (*TempWriteFile, error) {
	ra.tempWriteMu.Lock()
	defer ra.tempWriteMu.Unlock()

	// Check if already exists
	if ra.tempWriteFile != nil {
		return ra.tempWriteFile, nil
	}

	// Get file object
	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// Always create new DataID for temporary write area
	// This ensures version control and data safety
	// The old DataID will be cleaned up by garbage collection
	dataID := core.NewID()
	if dataID == 0 {
		return nil, fmt.Errorf("failed to generate DataID")
	}

	// IMPORTANT: Do NOT update database here!
	// The DataID and Size will be updated in flushTempWriteFile() after all writes are complete
	// Updating database here with Size=0 would cause the "size=0" problem
	DebugLog("[VFS getOrCreateTempWriteFile] Created new DataID (will update DB on flush): fileID=%d, oldDataID=%d, newDataID=%d",
		ra.fileID, fileObj.DataID, dataID)

	// Determine if compression and encryption are needed
	needsCompress := shouldCompressFileByName(ra.fs, fileObj.Name)
	needsEncrypt := ra.fs.EndecKey != "" // Check if encryption key is configured

	// Determine original file size
	// For sparse files, use sparseSize; otherwise use fileObj.Size
	originalSize := fileObj.Size
	if sparseSize := atomic.LoadInt64(&ra.sparseSize); sparseSize > 0 {
		originalSize = sparseSize
		DebugLog("[VFS getOrCreateTempWriteFile] Using sparseSize as original size: fileID=%d, sparseSize=%d", ra.fileID, sparseSize)
	}

	// Create TempWriteFile with original file size
	// This is important for partial overwrites and sparse files
	twf, err := ra.fs.tempWriteArea.GetOrCreate(ra.fileID, dataID, originalSize, needsCompress, needsEncrypt)
	if err != nil {
		return nil, err
	}

	// Store the reference
	ra.tempWriteFile = twf

	DebugLog("[VFS getOrCreateTempWriteFile] Created TempWriteFile: fileID=%d, dataID=%d, needsCompress=%v, needsEncrypt=%v",
		ra.fileID, dataID, needsCompress, needsEncrypt)

	return twf, nil
}

// flushTempWriteFile flushes the temporary write file
func (ra *RandomAccessor) flushTempWriteFile() error {
	ra.tempWriteMu.Lock()
	twf := ra.tempWriteFile
	ra.tempWriteMu.Unlock()

	if twf == nil {
		return nil
	}

	// Get current size before flush
	twf.mu.RLock()
	finalSize := twf.size
	finalDataID := twf.dataID
	twf.mu.RUnlock()

	DebugLog("[VFS flushTempWriteFile] Flushing temp write file: fileID=%d, size=%d, dataID=%d",
		ra.fileID, finalSize, finalDataID)

	// Flush the temp write file (moves to final location)
	if err := twf.Flush(); err != nil {
		return fmt.Errorf("failed to flush temp write file: %w", err)
	}

	// Update file object in database and cache with final size and dataID
	fileObj, err := ra.getFileObj()
	if err != nil {
		return fmt.Errorf("failed to get file object: %w", err)
	}

	// Update file object
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		PID:    fileObj.PID,
		Type:   fileObj.Type,
		Name:   fileObj.Name,
		DataID: finalDataID,
		Size:   finalSize,
		MTime:  core.Now(),
	}

	// IMPORTANT: Write to database first
	// Note: Due to SQLite WAL, immediate reads may return stale data
	// We must trust the data we just wrote and update cache accordingly
	DebugLog("[VFS flushTempWriteFile] Writing to database: fileID=%d, size=%d, dataID=%d",
		updateFileObj.ID, updateFileObj.Size, updateFileObj.DataID)

	_, err = ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
	if err != nil {
		DebugLog("[VFS flushTempWriteFile] ERROR: Failed to write to database: %v", err)
		return fmt.Errorf("failed to update file object: %w", err)
	}

	// CRITICAL: Update cache with the correct data we just wrote
	// DO NOT re-read from database as it may return stale data due to WAL
	fileObjCache.Put(ra.fileObjKey, updateFileObj)
	ra.fileObj.Store(updateFileObj)

	DebugLog("[VFS flushTempWriteFile] Successfully updated file object in DB and cache: fileID=%d, size=%d, dataID=%d",
		ra.fileID, updateFileObj.Size, updateFileObj.DataID)

	// Record write operation for save pattern detection
	ra.fs.recordFileOperation(OpWrite, ra.fileID, updateFileObj.Name, updateFileObj.PID, updateFileObj.DataID, updateFileObj.Size, "", 0)

	// Register bucket for WAL checkpoint (有写入操作)
	if ra.fs.walCheckpointManager != nil {
		ra.fs.walCheckpointManager.RegisterBucket(ra.fs.bktID)
	}

	// Clear the reference
	ra.tempWriteMu.Lock()
	ra.tempWriteFile = nil
	ra.tempWriteMu.Unlock()

	return nil
}

// closeTempWriteFile closes the temporary write file
func (ra *RandomAccessor) closeTempWriteFile() error {
	ra.tempWriteMu.Lock()
	twf := ra.tempWriteFile
	ra.tempWriteMu.Unlock()

	if twf == nil {
		return nil
	}

	// Close the temp write file
	if err := twf.Close(); err != nil {
		DebugLog("[VFS closeTempWriteFile] WARNING: Failed to close temp write file: %v", err)
		return err
	}

	// Clear the reference
	ra.tempWriteMu.Lock()
	ra.tempWriteFile = nil
	ra.tempWriteMu.Unlock()

	return nil
}
