package core

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	b "github.com/orca-zhang/borm"
	"github.com/zeebo/xxh3"
)

// workLocks is used to ensure only one work is executing for the same key at a time
// key: string, value: chan struct{} (closed when processing is complete)
var workLocks sync.Map

// acquireWorkLock acquires work lock, ensures only one work is executing for the same key at a time
// key: unique identifier
// return value: acquired indicates whether lock was successfully acquired, release is the function to release the lock
// If work is already in progress, waits for it to complete before attempting to acquire
func acquireWorkLock(key string) (acquired bool, release func()) {
	lockChan := make(chan struct{})
	actual, loaded := workLocks.LoadOrStore(key, lockChan)
	if loaded {
		// Another goroutine is already processing, wait for it to complete
		existingChan := actual.(chan struct{})
		<-existingChan // Wait for existing processing to complete (channel closed)
		// After processing completes, try to acquire lock again
		lockChan = make(chan struct{})
		_, loaded = workLocks.LoadOrStore(key, lockChan)
		if loaded {
			// If still occupied, new processing has started, return failure
			return false, nil
		}
	}

	// Return success and provide a function to release the lock
	return true, func() {
		workLocks.Delete(key)
		close(lockChan)
	}
}

// ResourceController resource controller, used to limit the use of resource-intensive operations
type ResourceController struct {
	config          ResourceControlConfig
	startTime       time.Time
	processedItems  int64
	lastCheckTime   time.Time
	rateLimitTokens int64
	rateLimitMutex  sync.Mutex
}

// NewResourceController creates a new resource controller
func NewResourceController(config ResourceControlConfig) *ResourceController {
	rc := &ResourceController{
		config:          config,
		startTime:       time.Now(),
		processedItems:  0,
		lastCheckTime:   time.Now(),
		rateLimitTokens: int64(config.MaxItemsPerSecond), // Initial token count equals per-second limit
	}
	return rc
}

// ShouldStop checks if processing should stop (exceeds maximum duration)
func (rc *ResourceController) ShouldStop() bool {
	if rc.config.MaxDuration <= 0 {
		return false
	}
	return time.Since(rc.startTime) >= rc.config.MaxDuration
}

// WaitIfNeeded waits between batch processing (implements batch interval and rate limiting)
// itemsProcessed: number of items processed in this batch
func (rc *ResourceController) WaitIfNeeded(itemsProcessed int) {
	rc.processedItems += int64(itemsProcessed)

	// Rate limiting (token bucket algorithm)
	if rc.config.MaxItemsPerSecond > 0 {
		rc.rateLimitMutex.Lock()
		now := time.Now()
		elapsed := now.Sub(rc.lastCheckTime)

		// Replenish tokens per second
		if elapsed > 0 {
			tokensToAdd := int64(rc.config.MaxItemsPerSecond) * int64(elapsed) / int64(time.Second)
			rc.rateLimitTokens += tokensToAdd
			if rc.rateLimitTokens > int64(rc.config.MaxItemsPerSecond) {
				rc.rateLimitTokens = int64(rc.config.MaxItemsPerSecond)
			}
		}

		// Consume tokens
		if rc.rateLimitTokens < int64(itemsProcessed) {
			// Insufficient tokens, need to wait
			deficit := int64(itemsProcessed) - rc.rateLimitTokens
			waitTime := time.Duration(deficit) * time.Second / time.Duration(rc.config.MaxItemsPerSecond)
			if waitTime > 0 {
				time.Sleep(waitTime)
				// Replenish tokens after waiting
				rc.rateLimitTokens = int64(rc.config.MaxItemsPerSecond)
			}
		} else {
			rc.rateLimitTokens -= int64(itemsProcessed)
		}

		rc.lastCheckTime = now
		rc.rateLimitMutex.Unlock()
	}

	// Batch interval delay
	if rc.config.BatchInterval > 0 {
		delay := rc.config.BatchInterval

		// Adaptive delay: dynamically adjust delay based on number of processed items
		if rc.config.AdaptiveDelay && rc.config.AdaptiveDelayFactor > 0 {
			adaptiveFactor := 1.0 + float64(rc.processedItems)/float64(rc.config.AdaptiveDelayFactor)
			delay = time.Duration(float64(delay) * adaptiveFactor)
			// Limit maximum delay to 10x base delay to avoid excessive delay
			maxDelay := rc.config.BatchInterval * 10
			if delay > maxDelay {
				delay = maxDelay
			}
		}

		time.Sleep(delay)
	}
}

// GetProcessedItems gets the number of processed items
func (rc *ResourceController) GetProcessedItems() int64 {
	return rc.processedItems
}

// GetElapsedTime gets the elapsed time
func (rc *ResourceController) GetElapsedTime() time.Duration {
	return time.Since(rc.startTime)
}

// delayedDelete delays deletion of data files
// Wait for specified time then check if data is still unreferenced, if unreferenced then delete
// Ensure only one delete operation is executing for the same dataID at a time
func delayedDelete(c Ctx, bktID, dataID int64, ma MetadataAdapter, da DataAdapter) {
	go func() {
		// Generate unique key to ensure only one delete operation for the same dataID at a time
		key := fmt.Sprintf("delayed_delete_%d_%d", bktID, dataID)

		// Acquire work lock
		acquired, release := acquireWorkLock(key)
		if !acquired {
			// Cannot acquire lock, another goroutine is already processing deletion for the same dataID, return directly
			return
		}
		// Ensure lock is released after processing completes
		defer release()

		// Wait for specified time
		time.Sleep(time.Duration(DeleteDelaySeconds) * time.Second)

		// Check if data is still unreferenced
		refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
		if err != nil {
			return // Query failed, don't delete
		}

		if refCounts[dataID] == 0 {
			// No references, safe to delete
			// First check if it's packaged data
			dataInfo, err := ma.GetData(c, bktID, dataID)
			if err == nil && dataInfo != nil {
				if dataInfo.PkgID > 0 && dataInfo.PkgID != dataID {
					// It's packaged data (but not the package file itself), don't delete the package file itself
					// Only delete metadata
					ma.DeleteData(c, bktID, []int64{dataID})
				} else {
					// Non-packaged data or package file itself, delete data files and metadata
					// For package files, if it itself is not referenced, it's safe to delete
					// Delete metadata first, then delete file, to avoid having metadata but no file
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						// Metadata deletion failed, don't continue deleting file
						return
					}

					dataPath := getDataPathFromAdapter(ma)
					dataSize := calculateDataSize(dataPath, bktID, dataID)
					if dataSize > 0 {
						ma.DecBktRealUsed(c, bktID, dataSize)
					}
					deleteDataFiles(dataPath, bktID, dataID, ma, c)
				}
			}
		}
	}()
}

// UpdateFileLatestVersion recursively updates directory size and DataID
// Assumption: when uploading new version, file's DataID and size have been updated, so only need to update directories
// Recursively process directories until size and DataID stabilize (accumulate from leaf directories upward)
func UpdateFileLatestVersion(c Ctx, bktID int64, ma MetadataAdapter) error {
	// Recursively update directory size and DataID until no changes
	maxIterations := 100 // Prevent infinite loop
	for iteration := 0; iteration < maxIterations; iteration++ {
		hasChange := false

		// Query all directory objects with pagination
		// Use large page size to get all directories in one batch for this use case
		dirs, _, err := ma.ListObjsByType(c, bktID, OBJ_TYPE_DIR, 0, 10000)
		if err != nil {
			break
		}

		// Calculate size and DataID for each directory
		for _, dir := range dirs {
			var totalSize int64
			var maxDataID int64 = dir.DataID // Default to directory's own DataID

			// Query all child objects under this directory (not deleted) with pagination
			// Use large page size to get all children in one batch for this use case
			children, _, err := ma.ListChildren(c, bktID, dir.ID, 0, 10000)
			if err != nil {
				continue
			}

			// Accumulate child object sizes and find maximum DataID
			for _, child := range children {
				totalSize += child.Size
				// Directory's DataID should be the maximum DataID among all child objects
				if child.DataID > maxDataID {
					maxDataID = child.DataID
				}
			}

			// If there are changes, update directory
			if totalSize != dir.Size || maxDataID != dir.DataID {
				updateObj := &ObjectInfo{
					ID:     dir.ID,
					Size:   totalSize,
					DataID: maxDataID,
				}
				err = ma.SetObj(c, bktID, []string{"s", "did"}, updateObj)
				if err == nil {
					hasChange = true
				}
			}
		}

		// If no changes, exit loop
		if !hasChange {
			break
		}
	}

	return nil
}

// MarkObjectAsDeleted marks object as deleted without recursively deleting child objects
// This is used for fast deletion where child objects are deleted asynchronously
func MarkObjectAsDeleted(c Ctx, bktID, id int64, ma MetadataAdapter) error {
	// Mark current object as deleted (no recursion)
	return ma.DeleteObj(c, bktID, id)
}

// DeleteObject marks object as deleted (recursively delete child objects)
func DeleteObject(c Ctx, bktID, id int64, ma MetadataAdapter) error {
	// Get object information
	objs, err := ma.GetObj(c, bktID, []int64{id})
	if err != nil || len(objs) == 0 {
		return err
	}
	obj := objs[0]

	// If it's a directory, recursively delete all child objects
	if obj.Type == OBJ_TYPE_DIR {
		// Get all child objects (including deleted ones, using raw SQL query)
		children, err := listChildrenDirectly(c, bktID, id, ma)
		if err != nil {
			return err
		}
		// Recursively delete child objects
		for _, child := range children {
			// Only delete non-deleted objects (PID >= 0)
			if child.PID >= 0 {
				if err := DeleteObject(c, bktID, child.ID, ma); err != nil {
					return err
				}
			}
		}
	}

	// Mark current object as deleted
	return ma.DeleteObj(c, bktID, id)
}

// PermanentlyDeleteObject permanently deletes object (physically delete object and data files)
// Ensure only one delete operation is executing for the same object ID at a time
func PermanentlyDeleteObject(c Ctx, bktID, id int64, h Handler, ma MetadataAdapter, da DataAdapter) error {
	// Generate unique key to ensure only one delete operation for the same object ID at a time
	key := fmt.Sprintf("permanently_delete_object_%d_%d", bktID, id)

	// Acquire work lock
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// Cannot acquire lock, another goroutine is already processing, return error
		return fmt.Errorf("permanently delete object operation already in progress for object %d", id)
	}
	// Ensure lock is released after processing completes
	defer release()

	// Get object information.
	// Use a write-connection direct query to avoid WAL visibility issues where a pooled
	// read connection might briefly not see very recent writes, causing silent no-ops.
	obj, err := getObjDirectly(c, bktID, id, ma)
	if err != nil {
		return err
	}
	if obj == nil {
		return fmt.Errorf("object not found: bktID=%d objID=%d", bktID, id)
	}

	// If it's a directory, recursively delete all child objects.
	// NOTE: We intentionally do this sequentially. Concurrent deletes can easily hit
	// SQLite "database is locked"/busy errors and (historically) those errors were
	// swallowed, leaving orphaned Obj rows behind.
	if obj.Type == OBJ_TYPE_DIR {
		// Get all child objects (including deleted ones)
		children, err := listChildrenDirectly(c, bktID, id, ma)
		if err != nil {
			return err
		}

		// Filter non-deleted children
		nonDeletedChildren := make([]*ObjectInfo, 0, len(children))
		for _, child := range children {
			if child.PID >= 0 {
				nonDeletedChildren = append(nonDeletedChildren, child)
			}
		}

		for _, child := range nonDeletedChildren {
			if err := PermanentlyDeleteObject(c, bktID, child.ID, h, ma, da); err != nil {
				return fmt.Errorf("failed to delete child object %d: %w", child.ID, err)
			}
		}
	}

	// Decrease logical usage (object's original size)
	if obj.Type == OBJ_TYPE_FILE && obj.Size > 0 {
		if err := ma.DecBktUsed(c, bktID, obj.Size); err != nil {
			// If decreasing logical usage fails, record error but continue deletion
		}
		// Also decrease logical occupancy (object is deleted, no longer a valid object)
		if err := ma.DecBktLogicalUsed(c, bktID, obj.Size); err != nil {
			// If decreasing logical occupancy fails, record error but continue deletion
		}

		// Check if it's an instant upload object: if DataID is referenced by other objects, need to decrease instant upload savings after deletion
		if obj.DataID > 0 && obj.DataID != EmptyDataID {
			// Query DataID's reference count (excluding current object to delete, because CountDataRefs only counts objects with pid >= 0)
			// But when deleting, object hasn't been marked as deleted yet, so reference count will include current object
			// If reference count > 1, there are other objects referencing it, need to decrease instant upload savings after deletion
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{obj.DataID})
			if err == nil {
				refCount := refCounts[obj.DataID]
				// If reference count > 1, there are other objects referencing this DataID, need to decrease instant upload savings after deleting current object
				if refCount > 1 {
					// Decrease instant upload savings (because after deletion, this DataID's instant upload savings will decrease)
					if err := ma.DecBktDedupSavings(c, bktID, obj.Size); err != nil {
						// If decreasing instant upload savings fails, record error but continue deletion
					}
				}
			}
		}
	}

	// Handle current object's data files
	if obj.DataID > 0 && obj.DataID != EmptyDataID {
		// Check data's reference count (counted before deleting object, so will include current object)
		refCounts, err := ma.CountDataRefs(c, bktID, []int64{obj.DataID})
		if err != nil {
			// If query fails, skip data cleanup (avoid accidental deletion)
			refCounts = make(map[int64]int64)
		}

		// CountDataRefs only counts objects with pid >= 0, so will include current object
		// If refCount == 1, only current object is referencing this data, safe to delete
		// If refCount > 1, there are other objects referencing it, cannot delete data files
		refCount := refCounts[obj.DataID]
		if refCount == 1 {
			// Only current object references it, safe to cleanup data (with care for packaged data).
			dataPath := getDataPathFromAdapter(ma)
			dataID := obj.DataID

			// IMPORTANT: Delete DataInfo metadata as part of permanent delete.
			// Historically, PermanentlyDeleteObject deleted chunk files but left DataInfo behind,
			// which caused "data still referenced" / GC to treat the data as still alive.
			dataInfo, diErr := ma.GetData(c, bktID, dataID)
			if diErr == nil && dataInfo != nil && dataInfo.PkgID > 0 && dataInfo.PkgID != dataID {
				// Packaged region (not the package file itself): delete metadata only.
				_ = ma.DeleteData(c, bktID, []int64{dataID})
			} else {
				// Non-packaged data OR package file itself:
				// delete metadata first, then delete files to avoid leaving metadata pointing to missing files.
				if err := ma.DeleteData(c, bktID, []int64{dataID}); err == nil {
					dataSize := calculateDataSize(dataPath, bktID, dataID)
					if dataSize > 0 {
						_ = ma.DecBktRealUsed(c, bktID, dataSize)
					}
					deleteDataFiles(dataPath, bktID, dataID, ma, c)
				}
			}
		}
		// If refCount == 0, may be abnormal situation, for safety don't delete data files
	}

	// Physically delete object from database
	return deleteObjFromDB(c, bktID, id, ma)
}

// PermanentlyDeleteObjectBatch permanently deletes multiple objects in batch for better performance
// Returns a map of objID -> error for any failures (nil map means all succeeded)
// This is optimized for bulk deletion by:
// 1. Batching database queries (GetObj, CountDataRefs)
// 2. Processing deletes concurrently with controlled parallelism
// 3. Batching metadata deletions
func PermanentlyDeleteObjectBatch(c Ctx, bktID int64, ids []int64, h Handler, ma MetadataAdapter, da DataAdapter) map[int64]error {
	if len(ids) == 0 {
		return nil
	}

	// For single ID, use the original function
	if len(ids) == 1 {
		if err := PermanentlyDeleteObject(c, bktID, ids[0], h, ma, da); err != nil {
			return map[int64]error{ids[0]: err}
		}
		return nil
	}

	// Batch get all objects first to reduce database queries
	objs, err := ma.GetObj(c, bktID, ids)
	if err != nil {
		// Return error for all IDs
		result := make(map[int64]error, len(ids))
		for _, id := range ids {
			result[id] = err
		}
		return result
	}

	// Create a map for quick lookup
	objMap := make(map[int64]*ObjectInfo, len(objs))
	for _, obj := range objs {
		if obj != nil {
			objMap[obj.ID] = obj
		}
	}

	// Collect all DataIDs for batch reference counting
	dataIDs := make([]int64, 0, len(objs))
	for _, obj := range objs {
		if obj != nil && obj.DataID > 0 && obj.DataID != EmptyDataID {
			dataIDs = append(dataIDs, obj.DataID)
		}
	}

	// Batch count data references
	var refCounts map[int64]int64
	if len(dataIDs) > 0 {
		refCounts, err = ma.CountDataRefs(c, bktID, dataIDs)
		if err != nil {
			refCounts = make(map[int64]int64)
		}
	} else {
		refCounts = make(map[int64]int64)
	}

	// Process deletions with controlled concurrency
	const maxConcurrent = 10
	sem := make(chan struct{}, maxConcurrent)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make(map[int64]error)

	// Collect DataIDs and ObjIDs to delete in batch
	dataIDsToDelete := make([]int64, 0)
	objIDsToDelete := make([]int64, 0)
	var dataDeleteMu sync.Mutex

	dataPath := getDataPathFromAdapter(ma)

	for _, id := range ids {
		obj, exists := objMap[id]
		if !exists || obj == nil {
			mu.Lock()
			errors[id] = fmt.Errorf("object not found: bktID=%d objID=%d", bktID, id)
			mu.Unlock()
			continue
		}

		// Handle directories recursively (must be sequential to avoid SQLite lock issues)
		if obj.Type == OBJ_TYPE_DIR {
			wg.Add(1)
			go func(dirID int64, dirObj *ObjectInfo) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				if err := PermanentlyDeleteObject(c, bktID, dirID, h, ma, da); err != nil {
					mu.Lock()
					errors[dirID] = err
					mu.Unlock()
				}
			}(id, obj)
			continue
		}

		// Handle files
		wg.Add(1)
		go func(fileID int64, fileObj *ObjectInfo) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Decrease logical usage
			if fileObj.Size > 0 {
				_ = ma.DecBktUsed(c, bktID, fileObj.Size)
				_ = ma.DecBktLogicalUsed(c, bktID, fileObj.Size)

				// Handle dedup savings
				if fileObj.DataID > 0 && fileObj.DataID != EmptyDataID {
					refCount := refCounts[fileObj.DataID]
					if refCount > 1 {
						_ = ma.DecBktDedupSavings(c, bktID, fileObj.Size)
					}
				}
			}

			// Handle data files
			if fileObj.DataID > 0 && fileObj.DataID != EmptyDataID {
				refCount := refCounts[fileObj.DataID]
				if refCount == 1 {
					// Only this object references the data, safe to delete
					dataInfo, diErr := ma.GetData(c, bktID, fileObj.DataID)
					if diErr == nil && dataInfo != nil && dataInfo.PkgID > 0 && dataInfo.PkgID != fileObj.DataID {
						// Packaged region: delete metadata only
						dataDeleteMu.Lock()
						dataIDsToDelete = append(dataIDsToDelete, fileObj.DataID)
						dataDeleteMu.Unlock()
					} else {
						// Non-packaged data: delete metadata and files
						dataDeleteMu.Lock()
						dataIDsToDelete = append(dataIDsToDelete, fileObj.DataID)
						dataDeleteMu.Unlock()

						dataSize := calculateDataSize(dataPath, bktID, fileObj.DataID)
						if dataSize > 0 {
							_ = ma.DecBktRealUsed(c, bktID, dataSize)
						}
						deleteDataFiles(dataPath, bktID, fileObj.DataID, ma, c)
					}
				}
			}

			// Mark object for batch deletion
			dataDeleteMu.Lock()
			objIDsToDelete = append(objIDsToDelete, fileID)
			dataDeleteMu.Unlock()
		}(id, obj)
	}

	wg.Wait()

	// Batch delete DataInfo metadata
	if len(dataIDsToDelete) > 0 {
		_ = ma.DeleteData(c, bktID, dataIDsToDelete)
	}

	// Batch delete objects from database
	if len(objIDsToDelete) > 0 {
		if err := deleteObjsFromDB(c, bktID, objIDsToDelete, ma); err != nil {
			// If batch delete fails, record error for all objects
			for _, id := range objIDsToDelete {
				mu.Lock()
				if errors[id] == nil {
					errors[id] = err
				}
				mu.Unlock()
			}
		}
	}

	if len(errors) == 0 {
		return nil
	}
	return errors
}

// deleteObjsFromDB batch deletes objects from database
func deleteObjsFromDB(c Ctx, bktID int64, ids []int64, ma MetadataAdapter) error {
	if len(ids) == 0 {
		return nil
	}
	return deleteObjsFromDBBatch(c, bktID, ids, ma)
}

// CleanRecycleBin cleans up objects marked as deleted in recycle bin (physically delete unreferenced data files and metadata)
// targetID of 0 means clean all eligible objects, otherwise only clean specified object
// Ensure only one cleanup operation is executing for the same bktID at a time
func CleanRecycleBin(c Ctx, bktID int64, h Handler, ma MetadataAdapter, da DataAdapter, targetID int64) error {
	// Generate unique key to ensure only one cleanup operation for the same bktID at a time
	key := fmt.Sprintf("clean_recycle_bin_%d", bktID)

	// Acquire work lock
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// Cannot acquire lock, another goroutine is already processing, return error
		return fmt.Errorf("clean recycle bin operation already in progress for bucket %d", bktID)
	}
	// Ensure lock is released after processing completes
	defer release()

	var deletedObjs []*ObjectInfo
	var err error

	if targetID > 0 {
		// Only clean specified object
		objs, err := ma.GetObj(c, bktID, []int64{targetID})
		if err != nil {
			return err
		}
		if len(objs) == 0 {
			return nil // Object doesn't exist
		}
		// Check if already deleted (PID < 0 means deleted)
		if objs[0].PID < 0 {
			deletedObjs = objs
		} else {
			return nil // Object not deleted
		}
	} else {
		// Clean all objects deleted more than a certain time ago (leave window time, default 7 days)
		// Use pagination to avoid loading large amounts of data at once
		windowTime := Now() - 7*24*3600 // 7 days ago
		pageSize := DefaultListPageSize // Objects processed per page

		// Collect all DataIDs to check and object IDs to delete
		dataIDs := make(map[int64]bool)
		objIDsToDelete := make([]int64, 0)

		for {
			// Get deleted objects by page
			pageObjs, err := ma.ListDeletedObjs(c, bktID, windowTime, pageSize)
			if err != nil {
				return err
			}

			if len(pageObjs) == 0 {
				break // No more data
			}

			// Collect DataIDs and object IDs from current page
			for _, obj := range pageObjs {
				if obj.DataID > 0 && obj.DataID != EmptyDataID {
					dataIDs[obj.DataID] = true
				}
				objIDsToDelete = append(objIDsToDelete, obj.ID)
			}

			// If returned data is less than pageSize, it's the last page
			if len(pageObjs) < pageSize {
				break
			}
		}

		if len(dataIDs) == 0 && len(objIDsToDelete) == 0 {
			return nil
		}

		// Convert to slice
		dataIDList := make([]int64, 0, len(dataIDs))
		for dataID := range dataIDs {
			dataIDList = append(dataIDList, dataID)
		}

		// Count DataID reference counts (excluding deleted objects)
		refCounts, err := ma.CountDataRefs(c, bktID, dataIDList)
		if err != nil {
			// If query fails, skip data cleanup, only delete metadata
			refCounts = make(map[int64]int64)
		}

		// Delete unreferenced data files
		for _, dataID := range dataIDList {
			if refCounts[dataID] == 0 {
				// Check if it's packaged data
				dataInfo, err := ma.GetData(c, bktID, dataID)
				if err == nil && dataInfo != nil && dataInfo.PkgID > 0 {
					// Don't delete package file itself, don't decrease RealUsed
					// Defragmentation will clean up data blocks with same pkgID and pkgOffset
				} else {
					// Non-packaged data, calculate total data file size and decrease actual usage
					dataPath := getDataPathFromAdapter(ma)
					dataSize := calculateDataSize(dataPath, bktID, dataID)
					if dataSize > 0 {
						// Decrease bucket's actual usage
						if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
							// If decreasing usage fails, still delete file (avoid data leak)
						}
					}
					// Data is unreferenced, safe to delete file
					deleteDataFiles(dataPath, bktID, dataID, ma, c)
				}
			}
		}

		// Delete recycled object metadata from database
		// Batch delete
		for _, objID := range objIDsToDelete {
			if err := deleteObjFromDB(c, bktID, objID, ma); err != nil {
				// Record error but continue processing other objects
				continue
			}
		}

		return nil
	}

	if len(deletedObjs) == 0 {
		return nil
	}

	// Collect all DataIDs to check
	dataIDs := make(map[int64]bool)
	for _, obj := range deletedObjs {
		if obj.DataID > 0 && obj.DataID != EmptyDataID {
			dataIDs[obj.DataID] = true
		}
	}

	if len(dataIDs) == 0 {
		return nil
	}

	// Convert to slice
	dataIDList := make([]int64, 0, len(dataIDs))
	for dataID := range dataIDs {
		dataIDList = append(dataIDList, dataID)
	}

	// Count DataID reference counts (excluding deleted objects)
	refCounts, err := ma.CountDataRefs(c, bktID, dataIDList)
	if err != nil {
		// If query fails, skip data cleanup, only delete metadata
		refCounts = make(map[int64]int64)
	}

	// Delete unreferenced data files
	for _, dataID := range dataIDList {
		if refCounts[dataID] == 0 {
			// Calculate total data file size and decrease actual usage
			dataPath := getDataPathFromAdapter(ma)
			dataSize := calculateDataSize(dataPath, bktID, dataID)
			if dataSize > 0 {
				// Decrease bucket's actual usage
				if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
					// If decreasing usage fails, still delete file (avoid data leak)
				}
			}
			// Data is unreferenced, safe to delete file
			deleteDataFiles(dataPath, bktID, dataID, ma, c)
		}
	}

	// Delete recycled object metadata from database
	// Can batch delete here, but for simplicity, delete one by one for now
	for _, obj := range deletedObjs {
		if err := deleteObjFromDB(c, bktID, obj.ID, ma); err != nil {
			// Record error but continue processing other objects
			continue
		}
	}

	return nil
}

// getDataPathFromAdapter gets dataPath from MetadataAdapter if it's DefaultMetadataAdapter
func getDataPathFromAdapter(ma MetadataAdapter) string {
	if dma, ok := ma.(*DefaultMetadataAdapter); ok {
		return dma.DefaultDataMetadataAdapter.dataPath
	}
	return "."
}

// listChildrenDirectly directly queries child objects (including deleted ones)
func listChildrenDirectly(c Ctx, bktID, pid int64, ma MetadataAdapter) ([]*ObjectInfo, error) {
	// Get dataPath from adapter
	dataPath := getDataPathFromAdapter(ma)
	// Use read connection for query operations
	bktDirPath := filepath.Join(dataPath, fmt.Sprint(bktID))
	db, err := GetReadDB(bktDirPath)
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	// Note: Don't close the connection, it's from the pool

	var children []*ObjectInfo
	// Directly query all child objects, don't exclude deleted ones.
	// Select explicit columns to avoid schema drift issues.
	query := "SELECT id, pid, did, s, m, t, n, e FROM obj WHERE pid = ?"
	rows, err := db.Query(query, pid)
	if err != nil {
		return nil, ERR_QUERY_DB
	}
	defer rows.Close()

	for rows.Next() {
		var obj ObjectInfo
		err = rows.Scan(&obj.ID, &obj.PID, &obj.DataID, &obj.Size, &obj.MTime, &obj.Type, &obj.Name, &obj.Extra)
		if err != nil {
			continue
		}
		children = append(children, &obj)
	}
	return children, nil
}

// getObjDirectly fetches an object row via a write connection for consistency.
func getObjDirectly(c Ctx, bktID, id int64, ma MetadataAdapter) (*ObjectInfo, error) {
	dataPath := getDataPathFromAdapter(ma)
	bktDirPath := filepath.Join(dataPath, fmt.Sprint(bktID))

	db, err := GetWriteDB(bktDirPath)
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	// Note: Don't close the connection, it's from the pool

	var objs []*ObjectInfo
	if _, err := b.TableContext(c, db, OBJ_TBL).Select(&objs, b.Where(b.Eq("id", id))); err != nil {
		return nil, fmt.Errorf("%w: getObjDirectly failed (bktID=%d, objID=%d): %v", ERR_QUERY_DB, bktID, id, err)
	}
	if len(objs) == 0 {
		return nil, nil
	}
	return objs[0], nil
}

// deleteObjFromDB deletes object from database (physical deletion)
func deleteObjFromDB(c Ctx, bktID, id int64, ma MetadataAdapter) error {
	// Get dataPath from adapter
	dataPath := getDataPathFromAdapter(ma)
	// Use write connection for delete operation
	bktDirPath := filepath.Join(dataPath, fmt.Sprint(bktID))
	db, err := GetWriteDB(bktDirPath)
	if err != nil {
		return ERR_OPEN_DB
	}
	// Note: Don't close the connection, it's from the pool

	// Use raw SQL to delete object and its extended attributes
	// Delete extended attributes first (foreign key constraint)
	_, err = db.Exec("DELETE FROM attr WHERE id = ?", id)
	if err != nil {
		return err
	}

	// Then delete the object itself
	_, err = db.Exec("DELETE FROM obj WHERE id = ?", id)
	return err
}

// deleteObjsFromDBBatch batch deletes objects from database (physical deletion)
// This is more efficient than calling deleteObjFromDB in a loop
func deleteObjsFromDBBatch(c Ctx, bktID int64, ids []int64, ma MetadataAdapter) error {
	if len(ids) == 0 {
		return nil
	}

	// Get dataPath from adapter
	dataPath := getDataPathFromAdapter(ma)
	// Use write connection for delete operation
	bktDirPath := filepath.Join(dataPath, fmt.Sprint(bktID))
	db, err := GetWriteDB(bktDirPath)
	if err != nil {
		return ERR_OPEN_DB
	}
	// Note: Don't close the connection, it's from the pool

	// Build placeholders for IN clause
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	inClause := strings.Join(placeholders, ",")

	// Delete extended attributes first (foreign key constraint)
	_, err = db.Exec(fmt.Sprintf("DELETE FROM attr WHERE id IN (%s)", inClause), args...)
	if err != nil {
		return err
	}

	// Then delete the objects
	_, err = db.Exec(fmt.Sprintf("DELETE FROM obj WHERE id IN (%s)", inClause), args...)
	return err
}

// deleteDataFiles deletes data files
// calculateDataSize calculates total size of all chunk files for a dataID
func calculateDataSize(basePath string, bktID, dataID int64) int64 {
	chunks := scanChunks(basePath, bktID, dataID, 0, DEFAULT_CHUNK_SIZE)
	var totalSize int64
	for _, size := range chunks {
		totalSize += size
	}
	return totalSize
}

// deleteDataFiles deletes data files
// If it's packaged data (PkgID > 0), don't delete package file itself, package file regions will be handled during defragmentation
// If it's not packaged data, delete all chunk files
func deleteDataFiles(dataPath string, bktID, dataID int64, ma MetadataAdapter, c Ctx) {
	// If MetadataAdapter and Ctx are provided, query DataInfo to determine if it's packaged data
	if ma != nil && c != nil {
		dataInfo, err := ma.GetData(c, bktID, dataID)
		if err == nil && dataInfo != nil && dataInfo.PkgID > 0 {
			// It's packaged data, don't delete package file itself
			// Package file regions will be handled during defragmentation (Defragment)
			// Only delete metadata here (caller is responsible for deleting metadata)
			return
		}
	}

	// Non-packaged data: delete all chunk files (sn starts from 0, until file not found)
	sn := 0
	firstFile := true
	for {
		path := toFilePath(dataPath, bktID, dataID, sn)

		if _, err := os.Stat(path); os.IsNotExist(err) {
			break // File doesn't exist, deletion complete
		}

		// Wait for window time before deleting first file to prevent access conflicts
		if firstFile {
			time.Sleep(time.Duration(DeleteDelaySeconds) * time.Second)
			firstFile = false
		}

		// Try to delete file (ignore errors, may have been deleted already)
		os.Remove(path)
		sn++
	}
}

// ScrubData audits data integrity, checks consistency between metadata and data files
func ScrubData(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter) (*ScrubResult, error) {
	result := &ScrubResult{
		CorruptedData:      []int64{},
		OrphanedData:       []int64{},
		MismatchedChecksum: []int64{},
	}

	// Initialize resource controller
	rc := NewResourceController(GetResourceControlConfig())

	// Page size
	pageSize := DefaultListPageSize
	offset := 0

	// Map to store all data IDs (for orphaned file detection)
	dataMap := make(map[int64]*DataInfo)
	totalData := int64(0)

	// 1. Get all data in metadata by page
	for {
		// Check if should stop
		if rc.ShouldStop() {
			break
		}

		pageData, total, err := ma.ListAllData(c, bktID, offset, pageSize)
		if err != nil {
			return nil, err
		}

		// Set total count on first fetch
		if totalData == 0 {
			totalData = total
			result.TotalData = int(total)
		}

		// 2. Build mapping from data ID to DataInfo
		for _, d := range pageData {
			dataMap[d.ID] = d
		}

		// 3. Check if data files exist for each metadata entry
		for _, dataInfo := range pageData {
			if dataInfo.ID == EmptyDataID {
				continue // Skip empty data
			}

			// If it's packaged data, check package file
			if dataInfo.PkgID > 0 {
				// Check if package file exists
				dataPath := getDataPathFromAdapter(ma)
				if !dataFileExists(dataPath, bktID, dataInfo.PkgID, 0) {
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}
			} else {
				// Check chunk data files (sn starts from 0, until file not found)
				// Get bucket's chunk size configuration
				chunkSize := getChunkSize(c, bktID, ma)
				// Collect all existing chunks (pass Size and chunkSize to calculate expected max sn)
				dataPath := getDataPathFromAdapter(ma)
				chunks := scanChunks(dataPath, bktID, dataInfo.ID, dataInfo.Size, chunkSize)

				if len(chunks) == 0 {
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}

				// Check if chunks are continuous (starting from 0, continuously increasing)
				maxSN := -1
				for sn := range chunks {
					if sn > maxSN {
						maxSN = sn
					}
				}

				// Check if chunks are continuous
				isContinuous := true
				for i := 0; i <= maxSN; i++ {
					if _, exists := chunks[i]; !exists {
						isContinuous = false
						break
					}
				}

				if !isContinuous {
					// Chunks not continuous, mark as corrupted data
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}

				// Calculate actual total size
				var actualSize int64
				for _, size := range chunks {
					actualSize += size
				}

				// If actual size doesn't match size in metadata, mark as corrupted
				if actualSize != dataInfo.Size {
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}

				// If data size > 0 and has checksum, verify checksum
				if dataInfo.Size > 0 && (dataInfo.XXH3 != 0 || dataInfo.SHA256_0 != 0) {
					if !verifyChecksum(c, bktID, dataInfo, da, maxSN) {
						result.MismatchedChecksum = append(result.MismatchedChecksum, dataInfo.ID)
					}
				}
			}
		}

		// If returned data is less than pageSize, it's the last page
		if len(pageData) < pageSize {
			// Wait after processing last page
			rc.WaitIfNeeded(len(pageData))
			break
		}

		// Batch processing interval and rate limiting
		rc.WaitIfNeeded(len(pageData))
		offset += pageSize
	}

	// 4. Scan all data files in filesystem, find orphaned files without metadata references
	dataPath := getDataPathFromAdapter(ma)
	bucketDataPath := filepath.Join(dataPath, fmt.Sprint(bktID))
	if _, err := os.Stat(bucketDataPath); err == nil {
		// Use queue for level-order traversal (BFS)
		queue := []string{bucketDataPath}
		seenDataIDs := make(map[int64]bool) // For deduplication
		processedDirs := 0                  // Number of processed directories, for resource control

		for len(queue) > 0 {
			// Check if should stop
			if rc.ShouldStop() {
				break
			}

			// Pop directory from queue head
			dir := queue[0]
			queue = queue[1:]
			processedDirs++

			// Read directory contents
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue // Ignore inaccessible directories
			}

			filesInDir := 0
			for _, entry := range entries {
				fullPath := filepath.Join(dir, entry.Name())

				if entry.IsDir() {
					// If it's a directory, add to queue to continue traversal
					queue = append(queue, fullPath)
				} else {
					// If it's a file, process it
					// Parse filename: <dataID>_<sn>
					fileName := entry.Name()
					parts := strings.Split(fileName, "_")
					if len(parts) != 2 {
						continue // Wrong format, skip
					}

					dataID, err1 := strconv.ParseInt(parts[0], 10, 64)
					_, err2 := strconv.Atoi(parts[1])
					if err1 != nil || err2 != nil {
						continue // Parse failed, skip
					}

					// Check if there's metadata reference (deduplication)
					if _, exists := dataMap[dataID]; !exists {
						// Avoid adding duplicate dataID
						if !seenDataIDs[dataID] {
							seenDataIDs[dataID] = true
							result.OrphanedData = append(result.OrphanedData, dataID)
						}
					}
					filesInDir++
				}
			}

			// Apply resource control after processing certain number of directories
			if processedDirs%100 == 0 {
				rc.WaitIfNeeded(filesInDir)
			}
		}
	}

	return result, nil
}

// ScanDirtyData scans dirty data (incomplete data caused by power failure, upload failure)
// Main detection:
// 1. Incomplete chunk data (some chunks missing)
// 2. Unreadable data files (file exists but read fails)
func ScanDirtyData(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter) (*DirtyDataResult, error) {
	result := &DirtyDataResult{
		IncompleteChunks: []int64{},
		UnreadableData:   []int64{},
	}

	// Initialize resource controller
	rc := NewResourceController(GetResourceControlConfig())

	// Page size
	pageSize := DefaultListPageSize
	offset := 0

	// Get all data in metadata by page
	for {
		// Check if should stop
		if rc.ShouldStop() {
			break
		}

		pageData, _, err := ma.ListAllData(c, bktID, offset, pageSize)
		if err != nil {
			return nil, err
		}

		for _, dataInfo := range pageData {
			if dataInfo.ID == EmptyDataID {
				continue // Skip empty data
			}

			// If it's packaged data, check if package file is readable
			if dataInfo.PkgID > 0 {
				dataPath := getDataPathFromAdapter(ma)
				if !dataFileExists(dataPath, bktID, dataInfo.PkgID, 0) {
					result.UnreadableData = append(result.UnreadableData, dataInfo.ID)
					continue
				}
				// Try to read package data fragment
				pkgReader, _, err := createPkgDataReader(dataPath, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
				if err != nil {
					result.UnreadableData = append(result.UnreadableData, dataInfo.ID)
					continue
				}
				pkgReader.Close()
			} else {
				// Get bucket's chunk size configuration
				chunkSize := getChunkSize(c, bktID, ma)
				// Scan all chunks (pass Size and chunkSize to calculate expected max sn)
				dataPath := getDataPathFromAdapter(ma)
				chunks := scanChunks(dataPath, bktID, dataInfo.ID, dataInfo.Size, chunkSize)

				if len(chunks) == 0 {
					// No chunk files, skip (this should be detected by ScrubData)
					continue
				}

				// Find maximum sn
				maxSN := -1
				for sn := range chunks {
					if sn > maxSN {
						maxSN = sn
					}
				}

				// Check if chunks are continuous (from 0 to maxSN should all exist)
				isContinuous := true
				for i := 0; i <= maxSN; i++ {
					if _, exists := chunks[i]; !exists {
						isContinuous = false
						break
					}
				}

				if !isContinuous {
					// Chunks not continuous, mark as incomplete
					result.IncompleteChunks = append(result.IncompleteChunks, dataInfo.ID)
					continue
				}

				// Calculate actual total size
				var actualSize int64
				for _, size := range chunks {
					actualSize += size
				}

				// If actual size doesn't match size in metadata, mark as incomplete
				if actualSize != dataInfo.Size {
					result.IncompleteChunks = append(result.IncompleteChunks, dataInfo.ID)
					continue
				}

				// Try to read all chunks, check if readable
				for sn := 0; sn <= maxSN; sn++ {
					_, err := da.Read(c, bktID, dataInfo.ID, sn)
					if err != nil {
						result.UnreadableData = append(result.UnreadableData, dataInfo.ID)
						break
					}
				}
			}
		}

		// If returned data is less than pageSize, it's the last page
		if len(pageData) < pageSize {
			// Wait after processing last page
			rc.WaitIfNeeded(len(pageData))
			break
		}

		// Batch processing interval and rate limiting
		rc.WaitIfNeeded(len(pageData))
		offset += pageSize
	}

	return result, nil
}

// FixScrubIssues fixes issues detected by ScrubData
// Based on ScrubResult, can selectively fix different types of issues
// options:
//   - FixCorrupted: whether to fix corrupted data (delete metadata without files), default false
//   - FixOrphaned: whether to fix orphaned data (delete files without references), default false
//   - FixMismatchedChecksum: whether to fix mismatched checksum data (delete corrupted data), default false
//
// Returns fix statistics
func FixScrubIssues(c Ctx, bktID int64, result *ScrubResult, ma MetadataAdapter, da DataAdapter, options struct {
	FixCorrupted          bool
	FixOrphaned           bool
	FixMismatchedChecksum bool
},
) (*FixScrubIssuesResult, error) {
	fixResult := &FixScrubIssuesResult{
		FixedCorrupted:          0,
		FixedOrphaned:           0,
		FixedMismatchedChecksum: 0,
		FreedSize:               0,
		Errors:                  []string{},
	}

	// 1. Fix corrupted data (has metadata but no files)
	if options.FixCorrupted && len(result.CorruptedData) > 0 {
		for _, dataID := range result.CorruptedData {
			// Check if there are objects referencing this data
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
			if err != nil {
				fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to check refs for corrupted data %d: %v", dataID, err))
				continue
			}

			if refCounts[dataID] == 0 {
				// No references, safe to delete metadata
				// Check if it's packaged data
				dataInfo, err := ma.GetData(c, bktID, dataID)
				if err == nil && dataInfo != nil && dataInfo.PkgID > 0 && dataInfo.PkgID != dataID {
					// It's packaged data, only delete metadata, don't delete package file itself
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to delete corrupted data metadata %d: %v", dataID, err))
					} else {
						fixResult.FixedCorrupted++
					}
				} else {
					// Non-packaged data, delete metadata
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to delete corrupted data metadata %d: %v", dataID, err))
					} else {
						fixResult.FixedCorrupted++
					}
				}
			} else {
				// Has references, cannot delete, record warning
				fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("corrupted data %d still has %d references, cannot delete", dataID, refCounts[dataID]))
			}
		}
	}

	// 2. Fix orphaned data (has files but no metadata references)
	if options.FixOrphaned && len(result.OrphanedData) > 0 {
		for _, dataID := range result.OrphanedData {
			// Orphaned data has no metadata references, can directly delete files
			dataPath := getDataPathFromAdapter(ma)
			dataSize := calculateDataSize(dataPath, bktID, dataID)
			if dataSize > 0 {
				// Decrease actual usage
				if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
					fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to decrease real used for orphaned data %d: %v", dataID, err))
				}
				fixResult.FreedSize += dataSize
			}

			// Delete files
			deleteDataFiles(dataPath, bktID, dataID, nil, nil)
			fixResult.FixedOrphaned++
		}
	}

	// 3. Fix mismatched checksum data (data may be corrupted)
	if options.FixMismatchedChecksum && len(result.MismatchedChecksum) > 0 {
		for _, dataID := range result.MismatchedChecksum {
			// Check if there are objects referencing this data
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
			if err != nil {
				fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to check refs for mismatched checksum data %d: %v", dataID, err))
				continue
			}

			if refCounts[dataID] == 0 {
				// No references, safe to delete
				// Check if it's packaged data
				dataInfo, err := ma.GetData(c, bktID, dataID)
				if err == nil && dataInfo != nil && dataInfo.PkgID > 0 && dataInfo.PkgID != dataID {
					// It's packaged data, only delete metadata
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to delete mismatched checksum data metadata %d: %v", dataID, err))
					} else {
						fixResult.FixedMismatchedChecksum++
					}
				} else {
					// Non-packaged data, delete files and metadata
					// Delete metadata first, then delete files, to avoid having metadata but no files
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to delete mismatched checksum data metadata %d: %v", dataID, err))
						continue // Metadata deletion failed, don't continue deleting files
					}

					dataPath := getDataPathFromAdapter(ma)
					dataSize := calculateDataSize(dataPath, bktID, dataID)
					if dataSize > 0 {
						if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
							fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to decrease real used for mismatched checksum data %d: %v", dataID, err))
						}
						fixResult.FreedSize += dataSize
					}
					deleteDataFiles(dataPath, bktID, dataID, ma, c)
					fixResult.FixedMismatchedChecksum++
				}
			} else {
				// Has references, cannot delete, record warning
				fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("mismatched checksum data %d still has %d references, cannot delete", dataID, refCounts[dataID]))
			}
		}
	}

	return fixResult, nil
}

// dataFileExists checks if data file exists
func dataFileExists(basePath string, bktID, dataID int64, sn int) bool {
	path := toFilePath(basePath, bktID, dataID, sn)
	_, err := os.Stat(path)
	return err == nil
}

// Default chunk size
const DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024 // 10MB

// getChunkSize gets bucket's chunk size (from bucket configuration, use default if not set)
func getChunkSize(c Ctx, bktID int64, ma MetadataAdapter) int64 {
	buckets, err := ma.GetBkt(c, []int64{bktID})
	if err != nil || len(buckets) == 0 {
		return DEFAULT_CHUNK_SIZE
	}
	chunkSize := buckets[0].ChunkSize
	if chunkSize <= 0 {
		return DEFAULT_CHUNK_SIZE
	}
	return chunkSize
}

// scanChunks scans all chunk files for a dataID, returns mapping of sn -> size
// dataSize of 0 means unknown size, otherwise used to calculate expected max sn (prevent infinite loop)
// chunkSize is chunk size, used to calculate expected max sn
func scanChunks(basePath string, bktID, dataID int64, dataSize int64, chunkSize int64) map[int]int64 {
	chunks := make(map[int]int64)

	// Calculate expected max sn based on data size
	if dataSize > 0 && chunkSize > 0 {
		// Calculate expected chunk count: round up
		// Example: if dataSize = 10MB, chunkSize = 4MB
		//   chunk count = ceil(10/4) = 3, chunk indices are 0, 1, 2
		//   max sn = 3 - 1 = 2
		expectedChunkCount := (dataSize + chunkSize - 1) / chunkSize // Round up
		// Calculate expected maxSN: if expectedChunkCount is 1, maxSN should be 0; if 2, maxSN should be 1, etc.
		// So maxSN = expectedChunkCount - 1, but we need to scan beyond to detect missing chunks
		maxAllowedSN := int(expectedChunkCount) - 1 // Expected max SN (0-indexed)
		if maxAllowedSN < 0 {
			maxAllowedSN = 0 // At least one chunk
		}
		if maxAllowedSN > 100000 {
			maxAllowedSN = 100000 // Set an absolute upper limit to prevent abnormal situations
		}

		// Scan from 0 to find all existing chunks
		// We need to scan beyond maxAllowedSN to detect:
		// 1. Missing chunks within expected range (e.g., chunk 0 and 2 exist, but chunk 1 is missing)
		// 2. Orphaned chunks beyond expected range
		// Scan up to maxAllowedSN+10 or until we find no chunks for a while
		maxFoundSN := -1
		consecutiveMissing := 0
		scanLimit := maxAllowedSN + 10 // Scan well beyond expected range
		if scanLimit > 1000 {
			scanLimit = 1000 // Cap at reasonable limit
		}

		for sn := 0; sn <= scanLimit; sn++ {
			path := toFilePath(basePath, bktID, dataID, sn)

			info, err := os.Stat(path)
			if err != nil {
				// File doesn't exist
				consecutiveMissing++
				// If we're beyond expected range and haven't found chunks for a while, stop
				if sn > maxAllowedSN+5 && consecutiveMissing > 5 && maxFoundSN >= 0 {
					break
				}
				continue
			}

			// Found a chunk, reset consecutive missing counter
			consecutiveMissing = 0
			// Record chunk size
			chunks[sn] = info.Size()
			if sn > maxFoundSN {
				maxFoundSN = sn
			}
		}
	} else {
		// If data size is unknown, use original logic (until file not found or exceed safety limit)
		sn := 0
		for {
			path := toFilePath(basePath, bktID, dataID, sn)

			info, err := os.Stat(path)
			if err != nil {
				// File doesn't exist, stop scanning
				break
			}

			// Record chunk size
			chunks[sn] = info.Size()
			sn++

			// Prevent infinite loop (set a safety limit)
			if sn > 100000 {
				break
			}
		}
	}

	return chunks
}

// MergeDuplicateData merges duplicate instant upload data
// Find duplicate data with same checksum value but different DataIDs, merge them into one DataID
// Ensure only one merge operation is executing for the same bktID at a time
func MergeDuplicateData(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter) (*MergeDuplicateResult, error) {
	// Generate unique key to ensure only one merge operation for the same bktID at a time
	key := fmt.Sprintf("merge_duplicate_%d", bktID)

	// Acquire work lock
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// Cannot acquire lock, another goroutine is already processing, return error
		return nil, fmt.Errorf("merge duplicate data operation already in progress for bucket %d", bktID)
	}
	// Ensure lock is released after processing completes
	defer release()

	result := &MergeDuplicateResult{
		MergedGroups: 0,
		MergedData:   []map[int64]int64{},
		FreedSize:    0,
	}

	// Initialize resource controller
	rc := NewResourceController(GetResourceControlConfig())

	// Find duplicate data by page
	pageSize := 100
	offset := 0
	var totalFreedSize int64
	allMergedData := make(map[int64]int64) // oldDataID -> masterDataID

	for {
		// Check if should stop
		if rc.ShouldStop() {
			break
		}

		groups, _, err := ma.FindDuplicateData(c, bktID, offset, pageSize)
		if err != nil {
			return nil, err
		}

		if len(groups) == 0 {
			break
		}

		// Process each duplicate data group
		for _, group := range groups {
			if len(group.DataIDs) < 2 {
				continue // Not duplicate data
			}

			// Select smallest ID as master DataID (for consistency)
			masterDataID := group.DataIDs[0]
			for _, dataID := range group.DataIDs[1:] {
				if dataID < masterDataID {
					masterDataID = dataID
				}
			}

			// Check if master data exists
			masterData, err := ma.GetData(c, bktID, masterDataID)
			if err != nil || masterData == nil {
				// Master data doesn't exist, skip this group
				continue
			}

			// Check if master data file exists
			dataPath := getDataPathFromAdapter(ma)
			if !dataFileExists(dataPath, bktID, masterDataID, 0) && masterData.PkgID == 0 {
				// Master data file doesn't exist, try to find another existing one as master
				found := false
				for _, dataID := range group.DataIDs {
					if dataID == masterDataID {
						continue
					}
					data, err := ma.GetData(c, bktID, dataID)
					if err == nil && data != nil {
						if dataFileExists(dataPath, bktID, dataID, 0) || data.PkgID > 0 {
							masterDataID = dataID
							masterData = data
							found = true
							break
						}
					}
				}
				if !found {
					// No valid master data found, skip
					continue
				}
			}

			// Update all objects referencing duplicate DataIDs to point to master DataID
			duplicateDataIDs := make([]int64, 0)
			mergedMap := make(map[int64]int64)

			for _, dataID := range group.DataIDs {
				if dataID == masterDataID {
					continue // Skip master data
				}

				// Check if this data is referenced (only count non-deleted objects)
				refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
				if err != nil {
					continue
				}

				refCount := refCounts[dataID]
				if refCount > 0 {
					// Objects reference this data, need to update references
					if err := ma.UpdateObjDataID(c, bktID, dataID, masterDataID); err != nil {
						continue // Update failed, skip this data
					}
					mergedMap[dataID] = masterDataID
					allMergedData[dataID] = masterDataID
				}

				// Calculate data size (for calculating freed space)
				dataPath := getDataPathFromAdapter(ma)
				dataSize := calculateDataSize(dataPath, bktID, dataID)
				if dataSize > 0 {
					totalFreedSize += dataSize
				}

				// If this data is not referenced or update completed, can delete
				// But only delete data files after confirming all references are updated
				// Collect data IDs to delete here
				if refCount == 0 || len(mergedMap) > 0 {
					duplicateDataIDs = append(duplicateDataIDs, dataID)
				}
			}

			// Delete duplicate data files (only after no references or references updated)
			// Wait for window time before deletion to prevent access conflicts
			for _, dataID := range duplicateDataIDs {
				// Check reference count again (ensure nothing missed)
				refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
				if err != nil {
					continue
				}

				refCount := refCounts[dataID]
				if refCount == 0 {
					// No references, use delayed delete
					// delayedDelete will check reference count after window time, delete if still 0
					delayedDelete(c, bktID, dataID, ma, da)
				} else {
					// Still has references, UpdateObjDataID may have failed, don't delete
					// But metadata may need to be kept (because references were updated)
				}
			}

			if len(mergedMap) > 0 {
				result.MergedGroups++
				result.MergedData = append(result.MergedData, mergedMap)
			}
		}

		// If returned groups less than pageSize, this is the last page
		if len(groups) < pageSize {
			// Wait after processing last page
			rc.WaitIfNeeded(len(groups))
			break
		}

		// Batch processing interval and rate limiting
		rc.WaitIfNeeded(len(groups))
		offset += pageSize
	}

	result.FreedSize = totalFreedSize
	return result, nil
}

// verifyChecksum verifies data checksum (using streaming calculation to avoid loading all into memory)
func verifyChecksum(c Ctx, bktID int64, dataInfo *DataInfo, da DataAdapter, maxSN int) bool {
	var reader io.Reader
	var err error

	// If packaged data, read specified segment from package file
	if dataInfo.PkgID > 0 {
		// Packaged data stored in PkgID file (sn=0), read Size bytes from PkgOffset position
		var pkgReader *pkgReader
		// Get dataPath from DataAdapter
		dataPath := ""
		if dda, ok := da.(*DefaultDataAdapter); ok {
			dataPath = dda.dataPath
		}
		pkgReader, _, err = createPkgDataReader(dataPath, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
		if err != nil {
			return false
		}
		defer pkgReader.Close()
		reader = pkgReader
	} else {
		// Create chunk data reader (streaming read all chunks)
		reader, _, err = createChunkDataReader(c, da, bktID, dataInfo.ID, maxSN)
		if err != nil {
			return false
		}
	}

	// Initialize hash calculators
	var xxh3Hash *xxh3.Hasher
	var sha256Hash hash.Hash
	needXXH3 := (dataInfo.Kind&DATA_ENDEC_MASK == 0 && dataInfo.Kind&DATA_CMPR_MASK == 0) && dataInfo.XXH3 != 0
	needSHA256 := (dataInfo.Kind&DATA_ENDEC_MASK == 0 && dataInfo.Kind&DATA_CMPR_MASK == 0) && dataInfo.SHA256_0 != 0

	if needXXH3 {
		xxh3Hash = xxh3.New()
	}
	if needSHA256 {
		sha256Hash = sha256.New()
	}

	// Stream read and update hash, calculate total size
	const bufferSize = 64 * 1024 // 64KB buffer
	buf := make([]byte, bufferSize)
	var actualSize int64
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			actualSize += int64(n)
			if xxh3Hash != nil {
				xxh3Hash.Write(buf[:n])
			}
			if sha256Hash != nil {
				sha256Hash.Write(buf[:n])
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return false
		}
	}

	// Verify data size
	if actualSize != dataInfo.Size {
		return false
	}

	// If data is unencrypted and uncompressed, can verify XXH3 and SHA-256
	if needXXH3 {
		calculated := xxh3Hash.Sum64()
		if int64(calculated) != dataInfo.XXH3 {
			return false
		}
	}
	if needSHA256 {
		hashSum := sha256Hash.Sum(nil)
		// SHA-256 stored as 4 int64 values (32 bytes = 4 * 8 bytes)
		sha256_0 := int64(binary.BigEndian.Uint64(hashSum[0:8]))
		sha256_1 := int64(binary.BigEndian.Uint64(hashSum[8:16]))
		sha256_2 := int64(binary.BigEndian.Uint64(hashSum[16:24]))
		sha256_3 := int64(binary.BigEndian.Uint64(hashSum[24:32]))
		if sha256_0 != dataInfo.SHA256_0 || sha256_1 != dataInfo.SHA256_1 || sha256_2 != dataInfo.SHA256_2 || sha256_3 != dataInfo.SHA256_3 {
			return false
		}
	}

	return true
}

// createPkgDataReader creates streaming reader for packaged data
func createPkgDataReader(basePath string, bktID, pkgID int64, offset, size int) (*pkgReader, int64, error) {
	path := toFilePath(basePath, bktID, pkgID, 0)

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}

	// Seek to offset position
	if offset > 0 {
		if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
			f.Close()
			return nil, 0, err
		}
	}

	// Create LimitedReader to limit read size and ensure file is properly closed
	limitedReader := &pkgReader{
		Reader: io.LimitReader(f, int64(size)),
		file:   f,
	}
	return limitedReader, int64(size), nil
}

// pkgReader wraps LimitedReader and ensures file is closed
type pkgReader struct {
	io.Reader
	file *os.File
}

func (pr *pkgReader) Close() error {
	if pr.file != nil {
		return pr.file.Close()
	}
	return nil
}

// createChunkDataReader creates streaming reader for chunk data
func createChunkDataReader(c Ctx, da DataAdapter, bktID, dataID int64, maxSN int) (io.Reader, int64, error) {
	return &chunkReader{
		c:      c,
		da:     da,
		bktID:  bktID,
		dataID: dataID,
		maxSN:  maxSN,
		sn:     0,
	}, 0, nil
}

type chunkReader struct {
	c      Ctx
	da     DataAdapter
	bktID  int64
	dataID int64
	maxSN  int
	sn     int
	buf    []byte
	bufIdx int
}

func (cr *chunkReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// If current buffer is exhausted, read next chunk
	if cr.bufIdx >= len(cr.buf) {
		if cr.sn > cr.maxSN {
			return 0, io.EOF
		}
		var err error
		cr.buf, err = cr.da.Read(cr.c, cr.bktID, cr.dataID, cr.sn)
		if err != nil {
			return 0, err
		}
		cr.bufIdx = 0
		cr.sn++
	}

	// Copy data from buffer to output
	n = copy(p, cr.buf[cr.bufIdx:])
	cr.bufIdx += n
	return n, nil
}

// holeInfo represents a hole (gap) in a package file where data can be filled
type holeInfo struct {
	pkgID    int64  // Package file ID
	start    int64  // Start offset of the hole
	size     int64  // Size of the hole
	filePath string // Path to the package file
}

// holeMatch represents the best match for filling a hole
type holeMatch struct {
	singleFile    *DataInfo   // Best single file match
	combinedFiles []*DataInfo // Combined files for large holes
}

// findBestMatchForHole finds the best match (single file or combination) for a hole
func findBestMatchForHole(hole holeInfo, unfilledFiles []*DataInfo, filledFiles map[int64]bool, avgFileSize int64) holeMatch {
	match := holeMatch{}

	// Strategy 1: Find single best-fit file (minimize waste)
	bestUtilization := float64(0)
	bestWaste := int64(1 << 62)

	for _, dataInfo := range unfilledFiles {
		if filledFiles[dataInfo.ID] || dataInfo.Size > hole.size {
			continue
		}
		waste := hole.size - dataInfo.Size
		utilization := float64(dataInfo.Size) / float64(hole.size)
		if utilization > bestUtilization || (utilization == bestUtilization && waste < bestWaste) {
			bestWaste = waste
			bestUtilization = utilization
			match.singleFile = dataInfo
		}
	}

	// Strategy 2: For large holes, try combining multiple small files
	if hole.size > 2*avgFileSize && avgFileSize > 0 && len(unfilledFiles) > 1 {
		combinedSize := int64(0)
		combined := make([]*DataInfo, 0)
		for _, dataInfo := range unfilledFiles {
			if filledFiles[dataInfo.ID] {
				continue
			}
			if combinedSize+dataInfo.Size <= hole.size {
				combinedSize += dataInfo.Size
				combined = append(combined, dataInfo)
			}
		}
		if len(combined) > 1 {
			combinedUtilization := float64(combinedSize) / float64(hole.size)
			// Use combination if it uses at least 80% of hole or is better than single file
			if combinedUtilization > 0.8 || (match.singleFile != nil && combinedUtilization > bestUtilization) {
				match.combinedFiles = combined
				match.singleFile = nil // Prefer combination
			}
		}
	}

	return match
}

// removeFromSlice removes a DataInfo with given ID from slice
func removeFromSlice(slice []*DataInfo, id int64) []*DataInfo {
	for i, di := range slice {
		if di.ID == id {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// fillHoleWithFile fills a hole with a file data, returns true if successful
func fillHoleWithFile(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter, hole holeInfo, pkgID int64, dataInfo *DataInfo, chunkSize int64, filledFiles *map[int64]bool, result *DefragmentResult) bool {
	(*filledFiles)[dataInfo.ID] = true

	// Read file data
	var dataBytes []byte
	var err error
	if dataInfo.PkgID > 0 {
		// If already packaged, read from package file
		dataPath := getDataPathFromAdapter(ma)
		pkgReader, _, err := createPkgDataReader(dataPath, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
		if err != nil {
			(*filledFiles)[dataInfo.ID] = false
			return false
		}
		dataBytes = make([]byte, dataInfo.Size)
		_, err = io.ReadFull(pkgReader, dataBytes)
		pkgReader.Close()
		if err != nil {
			(*filledFiles)[dataInfo.ID] = false
			return false
		}
	} else {
		// Read chunk data
		maxSN := int((dataInfo.Size + chunkSize - 1) / chunkSize)
		reader, _, err := createChunkDataReader(c, da, bktID, dataInfo.ID, maxSN)
		if err != nil {
			(*filledFiles)[dataInfo.ID] = false
			return false
		}
		dataBytes = make([]byte, dataInfo.Size)
		_, err = io.ReadFull(reader, dataBytes)
		if err != nil {
			(*filledFiles)[dataInfo.ID] = false
			return false
		}
	}

	// Write to hole in package file
	pkgFile, err := os.OpenFile(hole.filePath, os.O_RDWR, 0o666)
	if err != nil {
		(*filledFiles)[dataInfo.ID] = false
		return false
	}

	_, err = pkgFile.WriteAt(dataBytes, hole.start)
	pkgFile.Close()
	if err != nil {
		(*filledFiles)[dataInfo.ID] = false
		return false
	}

	// Update DataInfo to point to this package file
	newDataInfo := *dataInfo
	newDataInfo.PkgID = pkgID
	newDataInfo.PkgOffset = uint32(hole.start)
	err = ma.PutData(c, bktID, []*DataInfo{&newDataInfo})
	if err != nil {
		(*filledFiles)[dataInfo.ID] = false
		return false
	}

	// Delete old data files if not packaged
	if dataInfo.PkgID == 0 {
		dataPath := getDataPathFromAdapter(ma)
		oldSize := calculateDataSize(dataPath, bktID, dataInfo.ID)
		if oldSize > 0 {
			deleteDataFiles(dataPath, bktID, dataInfo.ID, ma, c)
			ma.DecBktRealUsed(c, bktID, oldSize-dataInfo.Size)
			result.FreedSize += oldSize - dataInfo.Size
		}
	}

	return true
}

// Defragment performs defragmentation: offline merge and pack small files, move remaining data blocks forward in packages
// Strategy:
// 1. First, find holes (gaps from deleted data blocks) in existing package files and fill them with small files
// 2. Then, pack remaining small files into new package files
// Benefits of hole-filling approach:
// - Reuses space in existing package files, reducing new package file creation
// - Reduces fragmentation by consolidating data
// - More efficient than repacking everything: only processes files that fit in holes
// Evaluation: Hole-filling is beneficial when:
// - Package files have significant fragmentation (many deleted blocks)
// - Small files can efficiently fill existing holes
// - Reduces total I/O compared to full repacking
// maxSize: Maximum file size (files smaller than this will be packed)
// accessWindow: Access window time (seconds), reserved parameter. Currently only uses reference count to determine if data is in use.
// If data has references (refCount > 0), skip. If access time field is added in future, can use this parameter.
// Ensures only one defragmentation operation is executing for the same bktID at a time
func Defragment(c Ctx, bktID int64, a Admin, ma MetadataAdapter, da DataAdapter) (*DefragmentResult, error) {
	var maxSize int64 = 4 * 1024 * 1024 // 4MB
	var accessWindow int64 = 3600       // 1 hour

	cfg := GetCronJobConfig()
	if cfg.DefragmentMaxSize > 0 {
		maxSize = cfg.DefragmentMaxSize
	}
	if cfg.DefragmentAccessWindow > 0 {
		accessWindow = cfg.DefragmentAccessWindow
	}

	// Generate unique key to ensure only one defragmentation operation per bktID at a time
	key := fmt.Sprintf("defragment_%d", bktID)

	// Acquire work lock
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// Cannot acquire lock, another goroutine is processing, return error
		return nil, fmt.Errorf("defragment operation already in progress for bucket %d", bktID)
	}
	// Ensure lock is released after processing completes
	defer release()

	result := &DefragmentResult{
		PackedGroups:  0,
		PackedFiles:   0,
		CompactedPkgs: 0,
		FreedSize:     0,
		SkippedInUse:  0,
	}

	// Initialize resource controller
	rc := NewResourceController(GetResourceControlConfig())

	// Get bucket configuration
	buckets, err := ma.GetBkt(c, []int64{bktID})
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, ERR_QUERY_DB
	}
	chunkSize := getChunkSize(c, bktID, ma)
	if chunkSize <= 0 {
		chunkSize = DEFAULT_CHUNK_SIZE
	}

	// Step 1: Find holes in existing package files and collect small files for packing
	// Note: System doesn't have access time (ATime) field, only MTime (modification time),
	// and MTime is different from access time, so we only use reference count to determine if data is in use
	pageSize := 500
	offset := 0
	pendingPacking := make([]*DataInfo, 0)
	batchDataIDs := make([]int64, 0, pageSize) // Batch collect DataIDs for reference count query

	// First, collect all existing package files and identify holes
	pkgDataMap := make(map[int64][]*DataInfo) // pkgID -> []DataInfo
	pkgHoles := make(map[int64][]holeInfo)    // pkgID -> []holeInfo (holes in package files)

	// Collect all package files and their data
	offset = 0
	for {
		if rc.ShouldStop() {
			break
		}

		dataList, _, err := ma.ListAllData(c, bktID, offset, pageSize)
		if err != nil {
			break
		}

		for _, dataInfo := range dataList {
			if dataInfo.PkgID > 0 {
				pkgDataMap[dataInfo.PkgID] = append(pkgDataMap[dataInfo.PkgID], dataInfo)
			}
		}

		if len(dataList) < pageSize {
			rc.WaitIfNeeded(len(dataList))
			break
		}

		rc.WaitIfNeeded(len(dataList))
		offset += pageSize
	}

	// Identify holes in existing package files
	for pkgID, dataList := range pkgDataMap {
		// Read package file to find holes
		dataPath := getDataPathFromAdapter(ma)
		pkgPath := toFilePath(dataPath, bktID, pkgID, 0)
		pkgFile, err := os.Open(pkgPath)
		if err != nil {
			continue
		}

		fileInfo, err := pkgFile.Stat()
		if err != nil {
			pkgFile.Close()
			continue
		}
		fileSize := fileInfo.Size()
		pkgFile.Close()

		// Check which data blocks are still valid (referenced)
		validData := make([]*DataInfo, 0)
		dataIDs := make([]int64, 0, len(dataList))
		for _, di := range dataList {
			dataIDs = append(dataIDs, di.ID)
		}

		refCounts, err := ma.CountDataRefs(c, bktID, dataIDs)
		if err != nil {
			refCounts = make(map[int64]int64)
		}

		for _, di := range dataList {
			if refCounts[di.ID] > 0 {
				validData = append(validData, di)
			}
		}

		// Calculate holes: gaps between valid data blocks
		// Sort valid data by offset using efficient sort
		sortedData := make([]*DataInfo, len(validData))
		copy(sortedData, validData)
		sort.Slice(sortedData, func(i, j int) bool {
			return sortedData[i].PkgOffset < sortedData[j].PkgOffset
		})

		// Find holes
		holes := make([]holeInfo, 0)
		currentPos := int64(0)
		for _, di := range sortedData {
			holeStart := currentPos
			holeEnd := int64(di.PkgOffset)
			if holeEnd > holeStart {
				holes = append(holes, holeInfo{
					pkgID:    pkgID,
					start:    holeStart,
					size:     holeEnd - holeStart,
					filePath: pkgPath,
				})
			}
			currentPos = int64(di.PkgOffset) + di.Size
		}
		// Check hole at end of file
		if currentPos < fileSize {
			holes = append(holes, holeInfo{
				pkgID:    pkgID,
				start:    currentPos,
				size:     fileSize - currentPos,
				filePath: pkgPath,
			})
		}

		if len(holes) > 0 {
			pkgHoles[pkgID] = holes
		}
	}

	// Step 2: Find small files that can be packed (paginated)
	offset = 0
	for {
		// Check if should stop
		if rc.ShouldStop() {
			break
		}

		dataList, _, err := ma.FindSmallPackageData(c, bktID, maxSize, offset, pageSize)
		if err != nil {
			return nil, err
		}

		if len(dataList) == 0 {
			break
		}

		// Collect current page's DataIDs
		batchDataIDs = batchDataIDs[:0]
		for _, dataInfo := range dataList {
			batchDataIDs = append(batchDataIDs, dataInfo.ID)
		}

		// Batch query reference counts
		refCounts, err := ma.CountDataRefs(c, bktID, batchDataIDs)
		if err != nil {
			refCounts = make(map[int64]int64)
		}

		for _, dataInfo := range dataList {
			refCount := refCounts[dataInfo.ID]

			if refCount == 0 {
				// Not referenced, this is orphan data, should delete instead of pack
				// Use delayed delete, wait for window time before deletion
				delayedDelete(c, bktID, dataInfo.ID, ma, da)
			} else {
				// Referenced, data is in use, can consider packing
				// But need to check access window (currently using reference count as judgment)
				// If access time field is added in future, can use accessWindow parameter
				pendingPacking = append(pendingPacking, dataInfo)
			}
		}

		if len(dataList) < pageSize {
			// Wait after processing last page
			rc.WaitIfNeeded(len(dataList))
			break
		}

		// Batch processing interval and rate limiting
		rc.WaitIfNeeded(len(dataList))
		offset += pageSize
	}

	// Step 3: Fill holes in existing package files with small files
	// Optimized strategy:
	// 1. Sort files by size (smallest first) using efficient O(n log n) sort
	// 2. Sort holes by size (smallest first) to prioritize filling small holes (better utilization)
	// 3. For each hole, try to find best fit (minimize waste)
	// 4. For large holes, try to combine multiple small files (greedy combination)
	sortedPending := make([]*DataInfo, len(pendingPacking))
	copy(sortedPending, pendingPacking)
	sort.Slice(sortedPending, func(i, j int) bool {
		return sortedPending[i].Size < sortedPending[j].Size
	})

	filledFiles := make(map[int64]bool) // Track which files have been filled into holes

	// Collect all holes from all packages and sort by size (smallest first)
	// This prioritizes filling small holes first, which improves space utilization
	allHoles := make([]struct {
		hole  holeInfo
		pkgID int64
		index int
	}, 0)
	for pkgID, holes := range pkgHoles {
		for i, hole := range holes {
			allHoles = append(allHoles, struct {
				hole  holeInfo
				pkgID int64
				index int
			}{hole, pkgID, i})
		}
	}
	// Sort holes by size (smallest first) - filling small holes first improves utilization
	sort.Slice(allHoles, func(i, j int) bool {
		return allHoles[i].hole.size < allHoles[j].hole.size
	})

	// Pre-calculate average file size once (optimization: avoid recalculating in loop)
	unfilledFiles := make([]*DataInfo, 0, len(sortedPending))
	for _, di := range sortedPending {
		if !filledFiles[di.ID] {
			unfilledFiles = append(unfilledFiles, di)
		}
	}
	avgFileSize := int64(0)
	if len(unfilledFiles) > 0 {
		totalSize := int64(0)
		for _, di := range unfilledFiles {
			totalSize += di.Size
		}
		avgFileSize = totalSize / int64(len(unfilledFiles))
	}

	// Fill holes with optimized matching
	for _, holeEntry := range allHoles {
		hole := holeEntry.hole
		pkgID := holeEntry.pkgID

		// Find best match for this hole
		bestMatch := findBestMatchForHole(hole, unfilledFiles, filledFiles, avgFileSize)

		// Fill the hole with best match
		if bestMatch.singleFile != nil {
			// Fill with single best-fit file
			if fillHoleWithFile(c, bktID, ma, da, hole, pkgID, bestMatch.singleFile, chunkSize, &filledFiles, result) {
				result.PackedFiles++
				// Update unfilledFiles list (remove filled file)
				unfilledFiles = removeFromSlice(unfilledFiles, bestMatch.singleFile.ID)
			}
		} else if len(bestMatch.combinedFiles) > 0 {
			// Fill with multiple combined files
			currentOffset := hole.start
			for _, dataInfo := range bestMatch.combinedFiles {
				if filledFiles[dataInfo.ID] {
					continue
				}
				// Create a sub-hole for this file
				subHole := holeInfo{
					pkgID:    hole.pkgID,
					start:    currentOffset,
					size:     dataInfo.Size,
					filePath: hole.filePath,
				}
				if fillHoleWithFile(c, bktID, ma, da, subHole, pkgID, dataInfo, chunkSize, &filledFiles, result) {
					currentOffset += dataInfo.Size
					result.PackedFiles++
					// Update unfilledFiles list
					unfilledFiles = removeFromSlice(unfilledFiles, dataInfo.ID)
				}
			}
		}
	}

	// Step 4: Pack remaining small files (those not filled into holes)
	// Use unfilledFiles which is already maintained during hole filling
	remainingFiles := unfilledFiles

	// Pack remaining small files (grouped by chunkSize)
	if len(remainingFiles) > 0 {
		// Sort by size to fill each package efficiently
		// Simple greedy algorithm: sort from small to large, try to fill chunkSize
		pkgGroups := make([][]*DataInfo, 0)
		currentGroup := make([]*DataInfo, 0)
		currentGroupSize := int64(0)

		for _, dataInfo := range remainingFiles {
			// Check if exceeds chunkSize
			if currentGroupSize+dataInfo.Size > chunkSize && len(currentGroup) > 0 {
				// Current group is full, create new group
				pkgGroups = append(pkgGroups, currentGroup)
				currentGroup = make([]*DataInfo, 0)
				currentGroupSize = 0
			}
			currentGroup = append(currentGroup, dataInfo)
			currentGroupSize += dataInfo.Size
		}

		if len(currentGroup) > 0 {
			pkgGroups = append(pkgGroups, currentGroup)
		}

		// Execute packing: read each data, write to new package file
		for _, group := range pkgGroups {
			if len(group) == 0 {
				continue
			}

			// Create new package data ID
			pkgID := NewID()
			if pkgID <= 0 {
				continue
			}

			// Read all data and merge
			pkgBuffer := make([]byte, 0, chunkSize)
			dataInfos := make([]*DataInfo, 0, len(group))

			for _, dataInfo := range group {
				// Read data
				var dataBytes []byte
				if dataInfo.PkgID > 0 {
					// If already packaged, read from package file
					dataPath := getDataPathFromAdapter(ma)
					pkgReader, _, err := createPkgDataReader(dataPath, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
					if err != nil {
						continue // Read failed, skip
					}
					dataBytes = make([]byte, dataInfo.Size)
					_, err = io.ReadFull(pkgReader, dataBytes)
					pkgReader.Close()
					if err != nil {
						continue
					}
				} else {
					// Read chunk data
					maxSN := int((dataInfo.Size + chunkSize - 1) / chunkSize)
					reader, _, err := createChunkDataReader(c, da, bktID, dataInfo.ID, maxSN)
					if err != nil {
						continue
					}
					dataBytes = make([]byte, dataInfo.Size)
					_, err = io.ReadFull(reader, dataBytes)
					if err != nil {
						continue
					}
				}

				// Record offset position
				offset := len(pkgBuffer)
				pkgBuffer = append(pkgBuffer, dataBytes...)

				// Update data information
				newDataInfo := *dataInfo
				newDataInfo.PkgID = pkgID
				newDataInfo.PkgOffset = uint32(offset)
				dataInfos = append(dataInfos, &newDataInfo)
			}

			if len(dataInfos) == 0 || len(pkgBuffer) == 0 {
				continue
			}

			// Write to new package file
			err := da.Write(c, bktID, pkgID, 0, pkgBuffer)
			if err != nil {
				continue // Write failed, skip
			}

			// Update metadata
			err = ma.PutData(c, bktID, dataInfos)
			if err != nil {
				continue
			}

			// Calculate freed space (old data file size)
			var freedSize int64
			for _, dataInfo := range group {
				if dataInfo.PkgID > 0 {
					// If originally packaged, need to check if other data references same package file
					// Simplified: only count non-packaged data size
				} else {
					// Calculate total size of chunk data
					dataPath := getDataPathFromAdapter(ma)
					oldSize := calculateDataSize(dataPath, bktID, dataInfo.ID)
					freedSize += oldSize - dataInfo.Size // Subtract new packaged data size
					// Delete old data files
					deleteDataFiles(dataPath, bktID, dataInfo.ID, ma, c)
				}
			}

			// Update actual usage
			if freedSize > 0 {
				ma.DecBktRealUsed(c, bktID, freedSize)
				result.FreedSize += freedSize
			}

			result.PackedGroups++
			result.PackedFiles += int64(len(dataInfos))
		}
	}

	// Step 5: Compact existing package files: remove deleted data blocks, move remaining data forward
	// Use pkgDataMap already collected in Step 1
	// Compact each package file
	for pkgID, dataList := range pkgDataMap {
		// Read entire package file
		// Use same path calculation as in core/data.go
		dataPath := getDataPathFromAdapter(ma)
		pkgPath := toFilePath(dataPath, bktID, pkgID, 0)
		pkgFile, err := os.Open(pkgPath)
		if err != nil {
			continue
		}

		// Read file size
		fileInfo, err := pkgFile.Stat()
		if err != nil {
			pkgFile.Close()
			continue
		}
		fileSize := fileInfo.Size()
		pkgData := make([]byte, fileSize)
		_, err = io.ReadFull(pkgFile, pkgData)
		pkgFile.Close()
		if err != nil {
			continue
		}

		// Check if each data is still referenced
		validData := make([]*DataInfo, 0)
		validOffsets := make([]int, 0) // List of valid offset positions

		for _, dataInfo := range dataList {
			// Check if DataInfo ID is negative (marked as deleted)
			if dataInfo.ID < 0 {
				// DataInfo ID is negative, marked as deleted
				// Query objects referencing this DataInfo to get deletion time (MTime)
				objs, err := ma.GetObjByDataID(c, bktID, dataInfo.ID) // Use negative ID to find objects
				if err == nil && len(objs) > 0 {
					// Use first object's MTime as deletion time reference
					deleteTime := objs[0].MTime
					if deleteTime > 0 {
						// Check if deletion time exceeds window time
						now := Now()
						if now-deleteTime >= accessWindow {
							// Exceeds window time, can delete this data block
							// Don't add to validData, will be cleaned up later
							continue
						} else {
							// Not exceeded window time, temporarily keep (may be recovered)
							validData = append(validData, dataInfo)
							validOffsets = append(validOffsets, int(dataInfo.PkgOffset))
							continue
						}
					}
				}
				// If cannot get deletion time, don't keep by default
				continue
			}

			// DataInfo ID is positive, check if still referenced
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataInfo.ID})
			if err != nil {
				continue
			}

			if refCounts[dataInfo.ID] > 0 {
				// Still normally referenced, keep
				validData = append(validData, dataInfo)
				validOffsets = append(validOffsets, int(dataInfo.PkgOffset))
			}
			// If no reference and ID is positive, may be orphan data, don't add to validData
		}

		if len(validData) == 0 {
			// No valid data in package file, can delete entire package file
			// Delete all related data metadata first, then delete file, avoid having metadata but no file
			dataIDs := make([]int64, 0, len(dataList))
			for _, di := range dataList {
				dataIDs = append(dataIDs, di.ID)
			}
			if len(dataIDs) > 0 {
				if err := ma.DeleteData(c, bktID, dataIDs); err != nil {
					// Metadata deletion failed, don't continue deleting file
					continue
				}
			}
			// Pass nil to indicate not packaged data, but package file itself, need to delete entire file
			// Note: c may be nil here, use current directory as fallback
			dataPath := getDataPathFromAdapter(ma)
			deleteDataFiles(dataPath, bktID, pkgID, nil, nil)
			result.CompactedPkgs++
			continue
		}

		// Repack valid data (move forward)
		newPkgBuffer := make([]byte, 0, chunkSize)
		newDataInfos := make([]*DataInfo, 0, len(validData))

		// Pre-calculate offset of each data in new package file
		newOffsets := make([]int, len(validData))
		currentOffset := 0
		for i, dataInfo := range validData {
			newOffsets[i] = currentOffset
			currentOffset += int(dataInfo.Size)
		}

		for i, dataInfo := range validData {
			oldOffset := validOffsets[i]
			dataBytes := pkgData[oldOffset : oldOffset+int(dataInfo.Size)]

			newOffset := newOffsets[i]
			if len(newPkgBuffer) < newOffset+int(dataInfo.Size) {
				// Ensure buffer is large enough
				if cap(newPkgBuffer) < newOffset+int(dataInfo.Size) {
					oldBuf := newPkgBuffer
					newPkgBuffer = make([]byte, newOffset+int(dataInfo.Size))
					copy(newPkgBuffer, oldBuf)
				} else {
					newPkgBuffer = newPkgBuffer[:newOffset+int(dataInfo.Size)]
				}
			}
			copy(newPkgBuffer[newOffset:], dataBytes)

			newDataInfo := *dataInfo
			newDataInfo.PkgOffset = uint32(newOffset)
			newDataInfos = append(newDataInfos, &newDataInfo)
		}

		// If new package file is smaller, update
		if int64(len(newPkgBuffer)) < fileSize {
			// Use new pkgID scheme: create new package file, update all references, put old data in wait queue
			// 1. Generate new pkgID (as new DataID)
			newPkgID := NewID()
			if newPkgID <= 0 {
				continue
			}

			// 2. Create new package file
			err := da.Write(c, bktID, newPkgID, 0, newPkgBuffer)
			if err != nil {
				continue // Write failed, skip
			}

			// 3. Collect all objects that need updating (objects referencing old DataInfo)
			oldDataIDs := make([]int64, 0, len(validData))

			for i, dataInfo := range validData {
				oldDataID := dataInfo.ID
				// Create new DataInfo, use new pkgID
				newDataID := NewID()
				if newDataID <= 0 {
					continue
				}

				// Use pre-calculated offset
				newOffset := newOffsets[i]

				newDataInfo := *dataInfo
				newDataInfo.ID = newDataID
				newDataInfo.PkgID = newPkgID
				newDataInfo.PkgOffset = uint32(newOffset)

				// Record old DataID
				oldDataIDs = append(oldDataIDs, oldDataID)

				// Update new data metadata
				ma.PutData(c, bktID, []*DataInfo{&newDataInfo})

				// Update all objects referencing old DataID to point to new DataID
				ma.UpdateObjDataID(c, bktID, oldDataID, newDataID)
			}

			// 4. Use delayed delete for old pkgID and old DataIDs
			// Put old pkgID in delayed delete
			delayedDelete(c, bktID, pkgID, ma, da)

			// Put old DataIDs in delayed delete
			for _, oldDataID := range oldDataIDs {
				delayedDelete(c, bktID, oldDataID, ma, da)
			}

			// 5. Calculate freed space (don't reduce yet, wait for delay time before reducing)
			// Because old file still exists, just no longer referenced
			freedSize := fileSize - int64(len(newPkgBuffer))
			if freedSize > 0 {
				result.FreedSize += freedSize
			}
			result.CompactedPkgs++
		}
	}

	return result, nil
}

// ScanOrphanedChunks scans orphaned chunks (chunks without metadata or without object references)
// Scans all chunk files in the data directory, checks if DataInfo exists and if it's referenced by any ObjectInfo
// Every 1000 chunks, checks metadata and reference counts. If DataInfo doesn't exist or has no object references,
// delays and re-checks. If still orphaned after delay, deletes the chunk
// delaySeconds: delay time in seconds before re-checking and deleting orphaned chunks
func ScanOrphanedChunks(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter, delaySeconds int) (*ScanOrphanedChunksResult, error) {
	result := &ScanOrphanedChunksResult{
		OrphanedChunks: []int64{},
		Errors:         []string{},
	}

	// Get data path
	dataPath := getDataPathFromAdapter(ma)
	// Check if dataPath already contains bktID as the last component
	// If the dataPath ends with bktID, use it directly; otherwise join with bktID
	bktDataPath := dataPath
	if filepath.Base(dataPath) != fmt.Sprint(bktID) {
		bktDataPath = filepath.Join(dataPath, fmt.Sprint(bktID))
	}

	// Check if bucket data directory exists
	if _, err := os.Stat(bktDataPath); os.IsNotExist(err) {
		return result, nil // No data directory, nothing to scan
	}

	// Initialize resource controller
	rc := NewResourceController(GetResourceControlConfig())

	// Map to track all chunks by dataID: dataID -> []chunkInfo
	type chunkInfo struct {
		path string
		size int64
	}
	chunksByDataID := make(map[int64][]chunkInfo)
	// Map to track which dataIDs we've already checked
	checkedDataIDs := make(map[int64]bool)
	// Map to track orphaned dataIDs (metadata doesn't exist)
	orphanedDataIDs := make(map[int64]bool)

	// Scan all chunk files in the bucket directory
	// Chunk files are stored in: dataPath/bktID/hash[21:24]/hash[8:24]/dataID_sn
	scanCount := 0
	batchSize := 1000

	// Recursive function to scan directory using os.ReadDir
	var scanDir func(dirPath string) error
	scanDir = func(dirPath string) error {
		// Check if should stop
		if rc.ShouldStop() {
			return fmt.Errorf("resource controller requested stop")
		}

		entries, err := os.ReadDir(dirPath)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("error reading directory %s: %v", dirPath, err))
			return nil // Continue with other directories
		}

		for _, entry := range entries {
			// Check if should stop
			if rc.ShouldStop() {
				return fmt.Errorf("resource controller requested stop")
			}

			fullPath := filepath.Join(dirPath, entry.Name())

			// If it's a directory, recurse into it
			if entry.IsDir() {
				if err := scanDir(fullPath); err != nil {
					return err
				}
				continue
			}

			// It's a file, process it
			info, err := entry.Info()
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("error getting file info for %s: %v", fullPath, err))
				continue
			}

			// Extract dataID and sn from filename
			// Filename format: dataID_sn
			fileName := info.Name()
			// Skip database files and other non-chunk files
			if fileName == ".db" || fileName == ".db-shm" || fileName == ".db-wal" {
				continue
			}
			if !strings.Contains(fileName, "_") {
				continue // Not a chunk file, skip
			}

			parts := strings.Split(fileName, "_")
			if len(parts) != 2 {
				continue // Invalid format, skip
			}

			dataID, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				continue // Invalid dataID, skip
			}

			_, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				continue // Invalid sn, skip
			}

			// Verify this is actually a chunk file by checking path structure
			// Path should be: dataPath/bktID/hash[21:24]/hash[8:24]/dataID_sn
			expectedHash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
			if len(expectedHash) < 24 {
				continue // Invalid hash length, skip
			}
			expectedSubDir1 := expectedHash[21:24]
			expectedSubDir2 := expectedHash[8:24]
			dir := filepath.Dir(fullPath)
			// Check if path contains expected hash subdirectories
			// The path structure is: .../hash[21:24]/hash[8:24]/filename
			// So we check if the directory path contains both subdirectories
			// Use a more lenient check: verify the directory path contains both hash parts
			dirStr := dir
			if !strings.Contains(dirStr, expectedSubDir1) || !strings.Contains(dirStr, expectedSubDir2) {
				continue // Path doesn't match expected structure, skip
			}

			scanCount++
			result.TotalScanned++

			// Add chunk to dataID mapping
			if chunksByDataID[dataID] == nil {
				chunksByDataID[dataID] = []chunkInfo{}
			}
			chunksByDataID[dataID] = append(chunksByDataID[dataID], chunkInfo{
				path: fullPath,
				size: info.Size(),
			})

			// Every batchSize chunks, check metadata for collected dataIDs
			if scanCount%batchSize == 0 {
				// Get unique dataIDs that haven't been checked yet
				batchDataIDs := make([]int64, 0)
				for dataID := range chunksByDataID {
					if !checkedDataIDs[dataID] {
						batchDataIDs = append(batchDataIDs, dataID)
						checkedDataIDs[dataID] = true
					}
				}

				// Check metadata and references for all dataIDs in this batch
				if len(batchDataIDs) > 0 {
					// First, check which DataInfo exist
					existingDataIDs := make([]int64, 0)
					for _, checkDataID := range batchDataIDs {
						_, err := ma.GetData(c, bktID, checkDataID)
						if err != nil {
							// DataInfo doesn't exist, mark as orphaned
							orphanedDataIDs[checkDataID] = true
						} else {
							// DataInfo exists, need to check if it's referenced by any ObjectInfo
							existingDataIDs = append(existingDataIDs, checkDataID)
						}
					}

					// Check reference counts for existing DataInfo
					if len(existingDataIDs) > 0 {
						refCounts, err := ma.CountDataRefs(c, bktID, existingDataIDs)
						if err != nil {
							// If query fails, mark all as potentially orphaned (safer to re-check later)
							for _, dataID := range existingDataIDs {
								orphanedDataIDs[dataID] = true
							}
						} else {
							// Mark dataIDs with refCount == 0 as orphaned
							for _, dataID := range existingDataIDs {
								if refCounts[dataID] == 0 {
									orphanedDataIDs[dataID] = true
								}
							}
						}
					}
				}

				// Batch processing interval
				rc.WaitIfNeeded(batchSize)
			}
		}

		return nil
	}

	// Start scanning from bucket data directory
	err := scanDir(bktDataPath)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("error scanning directory: %v", err))
	}

	// Check remaining dataIDs that weren't checked in batches
	remainingDataIDs := make([]int64, 0)
	for dataID := range chunksByDataID {
		if !checkedDataIDs[dataID] {
			remainingDataIDs = append(remainingDataIDs, dataID)
			checkedDataIDs[dataID] = true
		}
	}

	// Check remaining dataIDs
	if len(remainingDataIDs) > 0 {
		// First, check which DataInfo exist
		existingDataIDs := make([]int64, 0)
		for _, dataID := range remainingDataIDs {
			_, err := ma.GetData(c, bktID, dataID)
			if err != nil {
				// DataInfo doesn't exist, mark as orphaned
				orphanedDataIDs[dataID] = true
			} else {
				// DataInfo exists, need to check if it's referenced by any ObjectInfo
				existingDataIDs = append(existingDataIDs, dataID)
			}
		}

		// Check reference counts for existing DataInfo
		if len(existingDataIDs) > 0 {
			refCounts, err := ma.CountDataRefs(c, bktID, existingDataIDs)
			if err != nil {
				// If query fails, mark all as potentially orphaned (safer to re-check later)
				for _, dataID := range existingDataIDs {
					orphanedDataIDs[dataID] = true
				}
			} else {
				// Mark dataIDs with refCount == 0 as orphaned
				for _, dataID := range existingDataIDs {
					if refCounts[dataID] == 0 {
						orphanedDataIDs[dataID] = true
					}
				}
			}
		}
	}

	// Collect all orphaned chunks
	orphanedChunkPaths := make(map[int64][]chunkInfo)
	for dataID := range orphanedDataIDs {
		if chunks, ok := chunksByDataID[dataID]; ok {
			orphanedChunkPaths[dataID] = chunks
		}
		result.OrphanedChunks = append(result.OrphanedChunks, dataID)
	}

	// After scanning, check delayed chunks if delay is configured
	if len(orphanedChunkPaths) > 0 && delaySeconds > 0 {
		result.DelayedChunks = len(orphanedChunkPaths)
		fmt.Printf("Found %d orphaned dataIDs, waiting %d seconds before re-checking...\n", len(orphanedChunkPaths), delaySeconds)
		time.Sleep(time.Duration(delaySeconds) * time.Second)

		// Re-check metadata and references for delayed chunks
		recheckDataIDs := make([]int64, 0, len(orphanedChunkPaths))
		for checkDataID := range orphanedChunkPaths {
			recheckDataIDs = append(recheckDataIDs, checkDataID)
		}

		// Check which DataInfo exist
		existingDataIDs := make([]int64, 0)
		missingDataIDs := make([]int64, 0)
		for _, checkDataID := range recheckDataIDs {
			_, err := ma.GetData(c, bktID, checkDataID)
			if err != nil {
				// DataInfo still doesn't exist, mark for deletion
				missingDataIDs = append(missingDataIDs, checkDataID)
			} else {
				// DataInfo exists, need to check references
				existingDataIDs = append(existingDataIDs, checkDataID)
			}
		}

		// Check reference counts for existing DataInfo
		if len(existingDataIDs) > 0 {
			refCounts, err := ma.CountDataRefs(c, bktID, existingDataIDs)
			if err != nil {
				// If query fails, mark all as potentially orphaned (safer to re-check later)
				missingDataIDs = append(missingDataIDs, existingDataIDs...)
			} else {
				// Mark dataIDs with refCount == 0 as orphaned
				for _, dataID := range existingDataIDs {
					if refCounts[dataID] == 0 {
						missingDataIDs = append(missingDataIDs, dataID)
					}
				}
			}
		}

		// Delete chunks for dataIDs that are still orphaned
		for _, checkDataID := range missingDataIDs {
			chunks, ok := orphanedChunkPaths[checkDataID]
			if !ok {
				continue
			}
			// Still orphaned, delete all chunks for this dataID
			result.StillOrphaned++
			for _, chunk := range chunks {
				result.FreedSize += chunk.size
				if err := os.Remove(chunk.path); err != nil {
					result.Errors = append(result.Errors, fmt.Sprintf("failed to delete chunk %s: %v", chunk.path, err))
				} else {
					result.DeletedChunks++
				}
			}
		}
	} else if len(orphanedChunkPaths) > 0 {
		// No delay configured, delete immediately
		for _, chunks := range orphanedChunkPaths {
			for _, chunk := range chunks {
				result.FreedSize += chunk.size
				if err := os.Remove(chunk.path); err != nil {
					result.Errors = append(result.Errors, fmt.Sprintf("failed to delete chunk %s: %v", chunk.path, err))
				} else {
					result.DeletedChunks++
				}
			}
		}
	}

	// Scan temporary write area for orphaned files
	scanTempWriteArea(dataPath, delaySeconds, result)

	return result, nil
}

// scanTempWriteArea scans the temporary write area for orphaned files
func scanTempWriteArea(dataPath string, delaySeconds int, result *ScanOrphanedChunksResult) {
	tempDir := filepath.Join(dataPath, ".temp_write")

	// Check if temp directory exists
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		return // No temp directory, nothing to scan
	}

	// Read all files in temp directory
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("error reading temp directory %s: %v", tempDir, err))
		return
	}

	now := time.Now()
	retentionPeriod := time.Duration(delaySeconds) * time.Second
	if retentionPeriod < 24*time.Hour {
		retentionPeriod = 24 * time.Hour // Minimum 24 hours for temp files
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse filename: fileID_dataID
		fileName := entry.Name()
		var fileID, dataID int64
		if _, err := fmt.Sscanf(fileName, "%d_%d", &fileID, &dataID); err != nil {
			// Invalid format, skip
			continue
		}

		// Get file info
		filePath := filepath.Join(tempDir, fileName)
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		// Check if file is old enough to be considered orphaned
		if now.Sub(fileInfo.ModTime()) > retentionPeriod {
			// File is orphaned, delete it
			if err := os.Remove(filePath); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("error removing orphaned temp file %s: %v", fileName, err))
			} else {
				result.DeletedChunks++
				result.OrphanedChunks = append(result.OrphanedChunks, dataID)
			}
		}
	}
}

// DeduplicationConfig configuration for deduplication jobs
type DeduplicationConfig struct {
	Enabled           bool          `json:"enabled"`              // Enable deduplication job
	Schedule          string        `json:"schedule"`             // Cron expression, default "0 2 * * *" (2:00 AM daily)
	PageSize          int           `json:"page_size"`            // Page size for processing, default 100
	BatchInterval     time.Duration `json:"batch_interval"`       // Interval between batches, default 100ms
	MaxDuration       time.Duration `json:"max_duration"`         // Maximum execution time, default 2 hours
	MaxItemsPerSecond int           `json:"max_items_per_second"` // Max items per second, default 100
	ConcurrentBuckets int           `json:"concurrent_buckets"`   // Number of buckets to process concurrently, default 1
}

// DefaultDeduplicationConfig returns default configuration
func DefaultDeduplicationConfig() DeduplicationConfig {
	return DeduplicationConfig{
		Enabled:           true,
		Schedule:          "0 2 * * *", // 2:00 AM daily
		PageSize:          100,
		BatchInterval:     100 * time.Millisecond,
		MaxDuration:       2 * time.Hour,
		MaxItemsPerSecond: 100,
		ConcurrentBuckets: 1,
	}
}

// DeduplicationJobResult result of deduplication job
type DeduplicationJobResult struct {
	StartTime       time.Time                       `json:"start_time"`        // Start time
	EndTime         time.Time                       `json:"end_time"`          // End time
	Duration        time.Duration                   `json:"duration"`          // Execution duration
	BucketsScanned  int                             `json:"buckets_scanned"`   // Number of buckets scanned
	BucketResults   map[int64]*MergeDuplicateResult `json:"bucket_results"`    // Results for each bucket
	TotalFreedSize  int64                           `json:"total_freed_size"`  // Total freed space (bytes)
	TotalMergedData int                             `json:"total_merged_data"` // Total merged data count
	Errors          []string                        `json:"errors"`            // Error list
}

// Global deduplication job lock
var (
	dedupJobLock    sync.Mutex
	dedupJobRunning bool
	lastDedupResult *DeduplicationJobResult
	lastDedupTime   time.Time
)

// RunDeduplicationJob runs deduplication job for all buckets
// This is the main entry point for scheduled deduplication jobs
// It iterates through all buckets and calls MergeDuplicateData for each
func RunDeduplicationJob(ctx context.Context, ma MetadataAdapter, da DataAdapter) (*DeduplicationJobResult, error) {
	// Acquire global lock to ensure only one deduplication job runs at a time
	dedupJobLock.Lock()
	if dedupJobRunning {
		dedupJobLock.Unlock()
		return nil, fmt.Errorf("deduplication job already running")
	}
	dedupJobRunning = true
	dedupJobLock.Unlock()

	defer func() {
		dedupJobLock.Lock()
		dedupJobRunning = false
		dedupJobLock.Unlock()
	}()

	result := &DeduplicationJobResult{
		StartTime:     time.Now(),
		BucketResults: make(map[int64]*MergeDuplicateResult),
		Errors:        make([]string, 0),
	}

	log.Printf("[Deduplication Job] Started: time=%v", result.StartTime)

	// Get all buckets
	buckets, err := ma.ListAllBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	log.Printf("[Deduplication Job] Found %d buckets to process", len(buckets))

	// Get configuration
	config := GetDeduplicationConfig()

	// Process buckets
	if config.ConcurrentBuckets > 1 {
		// Concurrent processing
		result = processBucketsConcurrent(ctx, buckets, ma, da, config)
	} else {
		// Sequential processing
		result = processBucketsSequential(ctx, buckets, ma, da, config)
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	// Calculate totals
	for _, bucketResult := range result.BucketResults {
		result.TotalFreedSize += bucketResult.FreedSize
		result.TotalMergedData += len(bucketResult.MergedData)
	}

	log.Printf("[Deduplication Job] Completed: duration=%v, bucketsScanned=%d, totalFreedSize=%d bytes, totalMergedData=%d",
		result.Duration, result.BucketsScanned, result.TotalFreedSize, result.TotalMergedData)

	// Save result for status queries
	dedupJobLock.Lock()
	lastDedupResult = result
	lastDedupTime = time.Now()
	dedupJobLock.Unlock()

	return result, nil
}

// processBucketsSequential processes buckets sequentially
func processBucketsSequential(ctx context.Context, buckets []*BucketInfo, ma MetadataAdapter, da DataAdapter, config DeduplicationConfig) *DeduplicationJobResult {
	result := &DeduplicationJobResult{
		StartTime:     time.Now(),
		BucketResults: make(map[int64]*MergeDuplicateResult),
		Errors:        make([]string, 0),
	}

	for _, bucket := range buckets {
		// Check context cancellation
		select {
		case <-ctx.Done():
			result.Errors = append(result.Errors, "job cancelled by context")
			return result
		default:
		}

		// Check max duration
		if config.MaxDuration > 0 && time.Since(result.StartTime) >= config.MaxDuration {
			result.Errors = append(result.Errors, fmt.Sprintf("max duration reached: %v", config.MaxDuration))
			break
		}

		log.Printf("[Deduplication Job] Processing bucket: bktID=%d, name=%s", bucket.ID, bucket.Name)

		// Run deduplication for this bucket
		bucketResult, err := RunDeduplicationForBucket(ctx, bucket.ID, ma, da)
		if err != nil {
			errMsg := fmt.Sprintf("bucket %d failed: %v", bucket.ID, err)
			result.Errors = append(result.Errors, errMsg)
			log.Printf("[Deduplication Job] ERROR: %s", errMsg)
			continue
		}

		result.BucketResults[bucket.ID] = bucketResult
		result.BucketsScanned++

		log.Printf("[Deduplication Job] Bucket completed: bktID=%d, mergedGroups=%d, freedSize=%d bytes",
			bucket.ID, bucketResult.MergedGroups, bucketResult.FreedSize)
	}

	return result
}

// processBucketsConcurrent processes buckets concurrently
func processBucketsConcurrent(ctx context.Context, buckets []*BucketInfo, ma MetadataAdapter, da DataAdapter, config DeduplicationConfig) *DeduplicationJobResult {
	result := &DeduplicationJobResult{
		StartTime:     time.Now(),
		BucketResults: make(map[int64]*MergeDuplicateResult),
		Errors:        make([]string, 0),
	}

	// Create a semaphore to limit concurrency
	sem := make(chan struct{}, config.ConcurrentBuckets)
	var wg sync.WaitGroup
	var mu sync.Mutex // Protect shared result

	for _, bucket := range buckets {
		// Check context cancellation
		select {
		case <-ctx.Done():
			mu.Lock()
			result.Errors = append(result.Errors, "job cancelled by context")
			mu.Unlock()
			return result
		default:
		}

		// Check max duration
		if config.MaxDuration > 0 && time.Since(result.StartTime) >= config.MaxDuration {
			mu.Lock()
			result.Errors = append(result.Errors, fmt.Sprintf("max duration reached: %v", config.MaxDuration))
			mu.Unlock()
			return result
		}

		wg.Add(1)
		go func(bkt *BucketInfo) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			log.Printf("[Deduplication Job] Processing bucket: bktID=%d, name=%s", bkt.ID, bkt.Name)

			// Run deduplication for this bucket
			bucketResult, err := RunDeduplicationForBucket(ctx, bkt.ID, ma, da)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errMsg := fmt.Sprintf("bucket %d failed: %v", bkt.ID, err)
				result.Errors = append(result.Errors, errMsg)
				log.Printf("[Deduplication Job] ERROR: %s", errMsg)
				return
			}

			result.BucketResults[bkt.ID] = bucketResult
			result.BucketsScanned++

			log.Printf("[Deduplication Job] Bucket completed: bktID=%d, mergedGroups=%d, freedSize=%d bytes",
				bkt.ID, bucketResult.MergedGroups, bucketResult.FreedSize)
		}(bucket)
	}

	wg.Wait()
	return result
}

// RunDeduplicationForBucket runs deduplication for a single bucket
// This wraps the existing MergeDuplicateData function with proper resource control
func RunDeduplicationForBucket(ctx context.Context, bktID int64, ma MetadataAdapter, da DataAdapter) (*MergeDuplicateResult, error) {
	// Call the existing MergeDuplicateData function
	// This function already has:
	// - Work lock to prevent concurrent execution for same bucket
	// - Resource controller for rate limiting
	// - Pagination support
	// - Error handling
	return MergeDuplicateData(ctx, bktID, ma, da)
}

// GetDeduplicationJobStatus returns the status of the last deduplication job
func GetDeduplicationJobStatus() (running bool, lastResult *DeduplicationJobResult, lastTime time.Time) {
	dedupJobLock.Lock()
	defer dedupJobLock.Unlock()

	return dedupJobRunning, lastDedupResult, lastDedupTime
}

// GetDeduplicationConfig returns the current deduplication configuration
// TODO: Load from actual configuration file/database
func GetDeduplicationConfig() DeduplicationConfig {
	// For now, return default config
	// In production, this should load from config file or database
	config := DefaultDeduplicationConfig()

	// Try to load from GetCronJobConfig if available
	cronConfig := GetCronJobConfig()
	if cronConfig.DeduplicationSchedule != "" {
		config.Schedule = cronConfig.DeduplicationSchedule
	}

	return config
}

// AddDeduplicationToCron adds deduplication job to the given CronScheduler
// This should be called during CronScheduler initialization
func AddDeduplicationToCron(scheduler *CronScheduler) {
	config := GetDeduplicationConfig()

	if !config.Enabled {
		log.Printf("[Deduplication Job] Disabled by configuration")
		return
	}

	log.Printf("[Deduplication Job] Enabled: schedule=%s", config.Schedule)

	// The actual scheduling will be handled by CronScheduler
	// based on config.Schedule (which comes from CronJobConfig)
}
