package core

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
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

					dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
					if dataSize > 0 {
						ma.DecBktRealUsed(c, bktID, dataSize)
					}
					deleteDataFiles(ORCAS_DATA, bktID, dataID, ma, c)
				}
			}
		}
	}()
}

// asyncApplyVersionRetention asynchronously applies version retention policy (post-processing)
// After version creation succeeds, uniformly process:
// 1. Version merging within time window (delete old versions within time window, keep only latest version)
// 2. Version count limit (delete oldest versions exceeding the limit)
// Ensure only one version retention processing is executing for the same parent object at a time
func asyncApplyVersionRetention(c Ctx, bktID, fileID int64, newVersionTime int64, ma MetadataAdapter, da DataAdapter) {
	go func() {
		// Acquire work lock
		acquired, release := acquireWorkLock(fmt.Sprintf("version_retention_%d_%d", bktID, fileID))
		if !acquired {
			// Cannot acquire lock, return directly
			return
		}
		// Ensure lock is released after processing completes
		defer release()
		// Wait a short time to ensure version has been created successfully
		time.Sleep(100 * time.Millisecond)

		// Query all versions of the file (including the newly created version)
		versions, err := ma.ListVersions(c, bktID, fileID)
		if err != nil {
			// Query failed, don't process
			return
		}

		if len(versions) == 0 {
			return
		}

		var versionsToDelete []*ObjectInfo

		config := GetVersionRetentionConfig()
		// 1. Process version merging within time window: delete old versions within time window
		if config.MinVersionInterval > 0 && len(versions) > 1 {
			// Latest version is the newly created version (versions[0])
			// Start checking from the second version, if time interval is less than minimum interval, mark for deletion
			for i := 1; i < len(versions); i++ {
				version := versions[i]
				timeDiff := newVersionTime - version.MTime

				// If time interval is less than minimum interval, need to delete this version (merge)
				if timeDiff < config.MinVersionInterval {
					versionsToDelete = append(versionsToDelete, version)
				} else {
					// Since versions are sorted by time descending, if this version is not in time window, later versions won't be either
					break
				}
			}
		}

		// 2. Process version count limit: if total version count (minus versions to delete) still exceeds limit, delete oldest versions
		remainingVersions := len(versions) - len(versionsToDelete)
		if config.MaxVersions > 0 && remainingVersions > int(config.MaxVersions) {
			// Number of additional versions to delete
			additionalToDelete := remainingVersions - int(config.MaxVersions)

			// Delete oldest versions from back to front (ListVersions returns sorted by MTime descending, so last is oldest)
			// Skip versions already marked for deletion
			alreadyMarked := make(map[int64]bool)
			for _, v := range versionsToDelete {
				alreadyMarked[v.ID] = true
			}

			for i := len(versions) - 1; i >= 0 && additionalToDelete > 0; i-- {
				version := versions[i]
				// If already in deletion list, skip
				if alreadyMarked[version.ID] {
					continue
				}

				versionsToDelete = append(versionsToDelete, version)
				additionalToDelete--
			}
		}

		// 3. Execute deletion operations
		for _, versionToDelete := range versionsToDelete {
			// Delete version object (mark as deleted)
			if err := ma.DeleteObj(c, bktID, versionToDelete.ID); err != nil {
				// Deletion failed, record error but continue processing other versions
				continue
			}

			// If version has DataID, use delayed delete to handle data files
			if versionToDelete.DataID > 0 && versionToDelete.DataID != EmptyDataID {
				delayedDelete(c, bktID, versionToDelete.DataID, ma, da)
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

		// Query all directory objects
		dirs, err := ma.ListObjsByType(c, bktID, OBJ_TYPE_DIR)
		if err != nil {
			break
		}

		// Calculate size and DataID for each directory
		for _, dir := range dirs {
			var totalSize int64
			var maxDataID int64 = dir.DataID // Default to directory's own DataID

			// Query all child objects under this directory (not deleted)
			children, err := ma.ListChildren(c, bktID, dir.ID)
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
				err = ma.SetObj(c, bktID, []string{"size", "did"}, updateObj)
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

	// Get object information
	objs, err := ma.GetObj(c, bktID, []int64{id})
	if err != nil || len(objs) == 0 {
		return err
	}
	obj := objs[0]

	// If it's a directory, recursively delete all child objects
	if obj.Type == OBJ_TYPE_DIR {
		// Get all child objects (including deleted ones)
		children, err := listChildrenDirectly(c, bktID, id, ma)
		if err != nil {
			return err
		}
		// Recursively delete child objects (only delete non-deleted objects)
		for _, child := range children {
			if child.PID >= 0 {
				if err := PermanentlyDeleteObject(c, bktID, child.ID, h, ma, da); err != nil {
					return err
				}
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
			// Only current object references it, check if it's packaged data
			dataInfo, err := ma.GetData(c, bktID, obj.DataID)
			if err == nil && dataInfo != nil && dataInfo.PkgID > 0 {
				// It's packaged data, use idgen to generate new negative dataID, update DataInfo's ID to mark deletion
				newDataID := NewID()
				if newDataID > 0 {
					negativeDataID := -newDataID
					// Only update current DataInfo's ID to negative (mark deletion)
					dataInfo.ID = negativeDataID
					ma.PutData(c, bktID, []*DataInfo{dataInfo})
				}
				// Don't delete package file itself, don't decrease RealUsed
				// Defragmentation will clean up data blocks with same pkgID and pkgOffset
			} else {
				// Non-packaged data, calculate total data file size and decrease actual usage
				dataSize := calculateDataSize(ORCAS_DATA, bktID, obj.DataID)
				if dataSize > 0 {
					// Decrease bucket's actual usage
					if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
						// If decreasing usage fails, still delete file (avoid data leak)
						// But record error
					}
				}
				// Delete data files
				deleteDataFiles(ORCAS_DATA, bktID, obj.DataID, ma, c)
			}
		}
		// If refCount == 0, may be abnormal situation, for safety don't delete data files
	}

	// Physically delete object from database
	return deleteObjFromDB(c, bktID, id)
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
					dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
					if dataSize > 0 {
						// Decrease bucket's actual usage
						if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
							// If decreasing usage fails, still delete file (avoid data leak)
						}
					}
					// Data is unreferenced, safe to delete file
					deleteDataFiles(ORCAS_DATA, bktID, dataID, ma, c)
				}
			}
		}

		// Delete recycled object metadata from database
		// Batch delete
		for _, objID := range objIDsToDelete {
			if err := deleteObjFromDB(c, bktID, objID); err != nil {
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
			dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
			if dataSize > 0 {
				// Decrease bucket's actual usage
				if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
					// If decreasing usage fails, still delete file (avoid data leak)
				}
			}
			// Data is unreferenced, safe to delete file
			deleteDataFiles(ORCAS_DATA, bktID, dataID, ma, c)
		}
	}

	// Delete recycled object metadata from database
	// Can batch delete here, but for simplicity, delete one by one for now
	for _, obj := range deletedObjs {
		if err := deleteObjFromDB(c, bktID, obj.ID); err != nil {
			// Record error but continue processing other objects
			continue
		}
	}

	return nil
}

// listChildrenDirectly directly queries child objects (including deleted ones)
func listChildrenDirectly(c Ctx, bktID, pid int64, ma MetadataAdapter) ([]*ObjectInfo, error) {
	db, err := GetDB(c, bktID)
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	defer db.Close()

	var children []*ObjectInfo
	// Directly query all child objects, don't exclude deleted ones
	query := "SELECT * FROM obj WHERE pid = ?"
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

// deleteObjFromDB deletes object from database (physical deletion)
func deleteObjFromDB(c Ctx, bktID, id int64) error {
	db, err := GetDB(c, bktID)
	if err != nil {
		return ERR_OPEN_DB
	}
	defer db.Close()

	// Use raw SQL to delete
	_, err = db.Exec("DELETE FROM obj WHERE id = ?", id)
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
func deleteDataFiles(basePath string, bktID, dataID int64, ma MetadataAdapter, c Ctx) {
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
		fileName := fmt.Sprintf("%d_%d", dataID, sn)
		hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
		path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)

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
				if !dataFileExists(ORCAS_DATA, bktID, dataInfo.PkgID, 0) {
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}
			} else {
				// Check chunk data files (sn starts from 0, until file not found)
				// Get bucket's chunk size configuration
				chunkSize := getChunkSize(c, bktID, ma)
				// Collect all existing chunks (pass Size and chunkSize to calculate expected max sn)
				chunks := scanChunks(ORCAS_DATA, bktID, dataInfo.ID, dataInfo.Size, chunkSize)

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
				if dataInfo.Size > 0 && (dataInfo.Cksum > 0 || dataInfo.CRC32 > 0) {
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
	bucketDataPath := filepath.Join(ORCAS_DATA, fmt.Sprint(bktID))
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
				if !dataFileExists(ORCAS_DATA, bktID, dataInfo.PkgID, 0) {
					result.UnreadableData = append(result.UnreadableData, dataInfo.ID)
					continue
				}
				// Try to read package data fragment
				pkgReader, _, err := createPkgDataReader(ORCAS_DATA, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
				if err != nil {
					result.UnreadableData = append(result.UnreadableData, dataInfo.ID)
					continue
				}
				pkgReader.Close()
			} else {
				// Get bucket's chunk size configuration
				chunkSize := getChunkSize(c, bktID, ma)
				// Scan all chunks (pass Size and chunkSize to calculate expected max sn)
				chunks := scanChunks(ORCAS_DATA, bktID, dataInfo.ID, dataInfo.Size, chunkSize)

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
			dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
			if dataSize > 0 {
				// Decrease actual usage
				if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
					fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to decrease real used for orphaned data %d: %v", dataID, err))
				}
				fixResult.FreedSize += dataSize
			}

			// Delete files
			deleteDataFiles(ORCAS_DATA, bktID, dataID, nil, nil)
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

					dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
					if dataSize > 0 {
						if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
							fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to decrease real used for mismatched checksum data %d: %v", dataID, err))
						}
						fixResult.FreedSize += dataSize
					}
					deleteDataFiles(ORCAS_DATA, bktID, dataID, ma, c)
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
	fileName := fmt.Sprintf("%d_%d", dataID, sn)
	hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
	path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)
	_, err := os.Stat(path)
	return err == nil
}

// Default chunk size
const DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024 // 4MB

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
		maxAllowedSN := int(expectedChunkCount) + 1                  // Allow some tolerance to prevent calculation errors
		if maxAllowedSN > 100000 {
			maxAllowedSN = 100000 // Set an absolute upper limit to prevent abnormal situations
		}

		// Scan from 0 until file not found or exceed expected max sn
		sn := 0
		for {
			fileName := fmt.Sprintf("%d_%d", dataID, sn)
			hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
			path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)

			info, err := os.Stat(path)
			if err != nil {
				// File doesn't exist, stop scanning
				break
			}

			// Record chunk size
			chunks[sn] = info.Size()
			sn++

			// If data size is known, stop when exceed expected max sn (allow some tolerance)
			if sn > maxAllowedSN {
				break
			}
		}
	} else {
		// If data size is unknown, use original logic (until file not found or exceed safety limit)
		sn := 0
		for {
			fileName := fmt.Sprintf("%d_%d", dataID, sn)
			hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
			path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)

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

		// 处理每一组重复数据
		for _, group := range groups {
			if len(group.DataIDs) < 2 {
				continue // 不是重复数据
			}

			// 选择最小的ID作为主DataID（保持一致性）
			masterDataID := group.DataIDs[0]
			for _, dataID := range group.DataIDs[1:] {
				if dataID < masterDataID {
					masterDataID = dataID
				}
			}

			// 检查主数据是否存在
			masterData, err := ma.GetData(c, bktID, masterDataID)
			if err != nil || masterData == nil {
				// 主数据不存在，跳过这组
				continue
			}

			// 检查主数据的文件是否存在
			if !dataFileExists(ORCAS_DATA, bktID, masterDataID, 0) && masterData.PkgID == 0 {
				// 主数据文件不存在，尝试找另一个存在的作为主数据
				found := false
				for _, dataID := range group.DataIDs {
					if dataID == masterDataID {
						continue
					}
					data, err := ma.GetData(c, bktID, dataID)
					if err == nil && data != nil {
						if dataFileExists(ORCAS_DATA, bktID, dataID, 0) || data.PkgID > 0 {
							masterDataID = dataID
							masterData = data
							found = true
							break
						}
					}
				}
				if !found {
					// 没有找到有效的主数据，跳过
					continue
				}
			}

			// 更新所有引用重复DataID的对象，将它们指向主DataID
			duplicateDataIDs := make([]int64, 0)
			mergedMap := make(map[int64]int64)

			for _, dataID := range group.DataIDs {
				if dataID == masterDataID {
					continue // 跳过主数据
				}

				// 检查这个数据是否被引用（只统计未删除的对象）
				refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
				if err != nil {
					continue
				}

				refCount := refCounts[dataID]
				if refCount > 0 {
					// 有对象引用这个数据，需要更新引用
					if err := ma.UpdateObjDataID(c, bktID, dataID, masterDataID); err != nil {
						continue // 更新失败，跳过这个数据
					}
					mergedMap[dataID] = masterDataID
					allMergedData[dataID] = masterDataID
				}

				// 计算数据大小（用于计算释放的空间）
				dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
				if dataSize > 0 {
					totalFreedSize += dataSize
				}

				// 如果这个数据没有被引用或已经更新完成，可以删除
				// 但是只有确认所有引用都已更新后才能删除数据文件
				// 这里先收集要删除的数据ID
				if refCount == 0 || len(mergedMap) > 0 {
					duplicateDataIDs = append(duplicateDataIDs, dataID)
				}
			}

			// 删除重复的数据文件（只有在没有引用或引用已更新后）
			// 在删除前等待窗口时间，防止有人在访问
			for _, dataID := range duplicateDataIDs {
				// 再次检查引用计数（确保没有遗漏）
				refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
				if err != nil {
					continue
				}

				refCount := refCounts[dataID]
				if refCount == 0 {
					// 没有引用，使用延迟删除处理
					// delayedDelete 会在等待窗口时间后检查引用计数，如果仍为0则删除
					delayedDelete(c, bktID, dataID, ma, da)
				} else {
					// 还有引用，说明UpdateObjDataID可能失败了，不删除
					// 但是元数据可能需要保留（因为引用已更新）
				}
			}

			if len(mergedMap) > 0 {
				result.MergedGroups++
				result.MergedData = append(result.MergedData, mergedMap)
			}
		}

		// 如果返回的组数少于pageSize，说明已经是最后一页
		if len(groups) < pageSize {
			// 最后一页处理完后等待
			rc.WaitIfNeeded(len(groups))
			break
		}

		// 批次处理间隔和速率限制
		rc.WaitIfNeeded(len(groups))
		offset += pageSize
	}

	result.FreedSize = totalFreedSize
	return result, nil
}

// verifyChecksum 验证数据的校验和（使用流式计算，避免全部加载到内存）
func verifyChecksum(c Ctx, bktID int64, dataInfo *DataInfo, da DataAdapter, maxSN int) bool {
	var reader io.Reader
	var err error

	// 如果是打包数据，从打包文件中读取指定片段
	if dataInfo.PkgID > 0 {
		// 打包数据存储在 PkgID 文件中（sn=0），从 PkgOffset 位置读取 Size 大小的数据
		var pkgReader *pkgReader
		pkgReader, _, err = createPkgDataReader(ORCAS_DATA, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
		if err != nil {
			return false
		}
		defer pkgReader.Close()
		reader = pkgReader
	} else {
		// 创建分片数据读取器（流式读取所有分片）
		reader, _, err = createChunkDataReader(c, da, bktID, dataInfo.ID, maxSN)
		if err != nil {
			return false
		}
	}

	// 初始化哈希计算器
	var crc32Hash hash.Hash32
	var md5Hash hash.Hash
	needCksum := dataInfo.Cksum > 0
	needCRC32 := (dataInfo.Kind&DATA_ENDEC_MASK == 0 && dataInfo.Kind&DATA_CMPR_MASK == 0) && dataInfo.CRC32 > 0
	needMD5 := (dataInfo.Kind&DATA_ENDEC_MASK == 0 && dataInfo.Kind&DATA_CMPR_MASK == 0) && dataInfo.MD5 != 0

	if needCksum || needCRC32 {
		crc32Hash = crc32.NewIEEE()
	}
	if needMD5 {
		md5Hash = md5.New()
	}

	// 流式读取并更新哈希，同时计算总大小
	const bufferSize = 64 * 1024 // 64KB 缓冲区
	buf := make([]byte, bufferSize)
	var actualSize int64
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			actualSize += int64(n)
			if crc32Hash != nil {
				crc32Hash.Write(buf[:n])
			}
			if md5Hash != nil {
				md5Hash.Write(buf[:n])
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return false
		}
	}

	// 验证数据大小
	if actualSize != dataInfo.Size {
		return false
	}

	// 验证Cksum（最终数据的CRC32）
	if needCksum {
		calculated := crc32Hash.Sum32()
		if calculated != dataInfo.Cksum {
			return false
		}
	}

	// 如果数据是未加密未压缩的，可以验证CRC32和MD5
	if needCRC32 {
		calculated := crc32Hash.Sum32()
		if calculated != dataInfo.CRC32 {
			return false
		}
	}
	if needMD5 {
		hashSum := md5Hash.Sum(nil)
		// MD5存储为int64，取第4-12字节（索引4到11），使用大端序转换
		// 与 sdk/data.go 中的计算方式保持一致
		md5Int64 := int64(binary.BigEndian.Uint64(hashSum[4:12]))
		if md5Int64 != dataInfo.MD5 {
			return false
		}
	}

	return true
}

// createPkgDataReader 创建打包数据的流式读取器
func createPkgDataReader(basePath string, bktID, pkgID int64, offset, size int) (*pkgReader, int64, error) {
	fileName := fmt.Sprintf("%d_%d", pkgID, 0)
	hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
	path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}

	// 定位到偏移位置
	if offset > 0 {
		if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
			f.Close()
			return nil, 0, err
		}
	}

	// 创建 LimitedReader 限制读取大小，并确保文件正确关闭
	limitedReader := &pkgReader{
		Reader: io.LimitReader(f, int64(size)),
		file:   f,
	}
	return limitedReader, int64(size), nil
}

// pkgReader 包装 LimitedReader 并确保文件关闭
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

// createChunkDataReader 创建分片数据的流式读取器
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

	// 如果当前缓冲区已读完，读取下一个分片
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

	// 从缓冲区复制数据到输出
	n = copy(p, cr.buf[cr.bufIdx:])
	cr.bufIdx += n
	return n, nil
}

// Defragment 碎片整理：小文件离线归并打包，打包中被删除的数据块前移
// maxSize: 最大文件大小（小于此大小的文件会被打包）
// accessWindow: 访问窗口时间（秒），预留参数。目前只基于引用计数判断数据是否正在使用，
// 如果数据有引用（refCount > 0），则跳过。将来如果添加了访问时间字段，可以使用此参数。
// 确保同一个bktID同一时间只有一个碎片整理操作在执行
func Defragment(c Ctx, bktID int64, a Admin, ma MetadataAdapter, da DataAdapter) (*DefragmentResult, error) {
	var maxSize int64 = 4 * 1024 * 1024 // 4MB
	var accessWindow int64 = 3600       // 1小时

	cfg := GetCronJobConfig()
	if cfg.DefragmentMaxSize > 0 {
		maxSize = cfg.DefragmentMaxSize
	}
	if cfg.DefragmentAccessWindow > 0 {
		accessWindow = cfg.DefragmentAccessWindow
	}

	// 生成唯一key，确保同一个bktID同一时间只有一个碎片整理操作
	key := fmt.Sprintf("defragment_%d", bktID)

	// 获取工作锁
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// 无法获取锁，说明已有其他goroutine在处理，返回错误
		return nil, fmt.Errorf("defragment operation already in progress for bucket %d", bktID)
	}
	// 确保在处理完成后释放锁
	defer release()

	result := &DefragmentResult{
		PackedGroups:  0,
		PackedFiles:   0,
		CompactedPkgs: 0,
		FreedSize:     0,
		SkippedInUse:  0,
	}

	// 初始化资源控制器
	rc := NewResourceController(GetResourceControlConfig())

	// 获取桶配置
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

	// 1. 查找可以打包的小文件数据（分页处理）
	// 注意：由于系统中没有访问时间（ATime）字段，只有MTime（修改时间），
	// 而MTime和访问时间不是同一个概念，所以这里只基于引用计数来判断数据是否正在使用
	pageSize := 500
	offset := 0
	pendingPacking := make([]*DataInfo, 0)
	batchDataIDs := make([]int64, 0, pageSize) // 批量收集DataID用于查询引用计数

	for {
		// 检查是否应该停止
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

		// 收集当前页的DataID
		batchDataIDs = batchDataIDs[:0]
		for _, dataInfo := range dataList {
			batchDataIDs = append(batchDataIDs, dataInfo.ID)
		}

		// 批量查询引用计数
		refCounts, err := ma.CountDataRefs(c, bktID, batchDataIDs)
		if err != nil {
			refCounts = make(map[int64]int64)
		}

		for _, dataInfo := range dataList {
			refCount := refCounts[dataInfo.ID]

			if refCount == 0 {
				// 未被引用，这是孤儿数据，应该删除而不是打包
				// 使用延迟删除处理，等待窗口时间后再删除
				delayedDelete(c, bktID, dataInfo.ID, ma, da)
			} else {
				// 被引用，说明数据正在使用中，可以考虑打包
				// 但需要检查访问窗口（目前使用引用计数作为判断）
				// 如果将来添加了访问时间字段，可以使用accessWindow参数
				pendingPacking = append(pendingPacking, dataInfo)
			}
		}

		if len(dataList) < pageSize {
			// 最后一页处理完后等待
			rc.WaitIfNeeded(len(dataList))
			break
		}

		// 批次处理间隔和速率限制
		rc.WaitIfNeeded(len(dataList))
		offset += pageSize
	}

	// 2. 打包小文件（按chunkSize分组）
	if len(pendingPacking) > 0 {
		// 按大小排序，尽量填满每个包
		// 简单的贪心算法：从小到大排序，尽量填满chunkSize
		pkgGroups := make([][]*DataInfo, 0)
		currentGroup := make([]*DataInfo, 0)
		currentGroupSize := int64(0)

		for _, dataInfo := range pendingPacking {
			// 检查是否超过chunkSize
			if currentGroupSize+dataInfo.Size > chunkSize && len(currentGroup) > 0 {
				// 当前组已满，创建新组
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

		// 3. 执行打包：读取每个数据，写入新打包文件
		for _, group := range pkgGroups {
			if len(group) == 0 {
				continue
			}

			// 创建新的打包数据ID
			pkgID := NewID()
			if pkgID <= 0 {
				continue
			}

			// 读取所有数据并合并
			pkgBuffer := make([]byte, 0, chunkSize)
			dataInfos := make([]*DataInfo, 0, len(group))

			for _, dataInfo := range group {
				// 读取数据
				var dataBytes []byte
				if dataInfo.PkgID > 0 {
					// 如果已经是打包数据，从打包文件中读取
					pkgReader, _, err := createPkgDataReader(ORCAS_DATA, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
					if err != nil {
						continue // 读取失败，跳过
					}
					dataBytes = make([]byte, dataInfo.Size)
					_, err = io.ReadFull(pkgReader, dataBytes)
					pkgReader.Close()
					if err != nil {
						continue
					}
				} else {
					// 读取分片数据
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

				// 记录偏移位置
				offset := len(pkgBuffer)
				pkgBuffer = append(pkgBuffer, dataBytes...)

				// 更新数据信息
				newDataInfo := *dataInfo
				newDataInfo.PkgID = pkgID
				newDataInfo.PkgOffset = uint32(offset)
				dataInfos = append(dataInfos, &newDataInfo)
			}

			if len(dataInfos) == 0 || len(pkgBuffer) == 0 {
				continue
			}

			// 写入新的打包文件
			err := da.Write(c, bktID, pkgID, 0, pkgBuffer)
			if err != nil {
				continue // 写入失败，跳过
			}

			// 更新元数据
			err = ma.PutData(c, bktID, dataInfos)
			if err != nil {
				continue
			}

			// 计算释放的空间（旧数据文件大小）
			var freedSize int64
			for _, dataInfo := range group {
				if dataInfo.PkgID > 0 {
					// 如果原来是打包数据，需要检查是否还有其他数据引用同一个打包文件
					// 简化处理：只统计非打包数据的大小
				} else {
					// 计算分片数据的总大小
					oldSize := calculateDataSize(ORCAS_DATA, bktID, dataInfo.ID)
					freedSize += oldSize - dataInfo.Size // 减去新打包数据的大小
					// 删除旧数据文件
					deleteDataFiles(ORCAS_DATA, bktID, dataInfo.ID, ma, c)
				}
			}

			// 更新实际使用量
			if freedSize > 0 {
				ma.DecBktRealUsed(c, bktID, freedSize)
				result.FreedSize += freedSize
			}

			result.PackedGroups++
			result.PackedFiles += int64(len(dataInfos))
		}
	}

	// 4. 整理已有打包文件：删除打包中被删除的数据块，前移剩余数据
	// 查找所有打包文件（PkgID > 0）
	// 这里需要遍历所有数据，找出打包文件ID列表
	pkgDataMap := make(map[int64][]*DataInfo) // pkgID -> []DataInfo

	offset = 0
	for {
		// 检查是否应该停止
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
			// 最后一页处理完后等待
			rc.WaitIfNeeded(len(dataList))
			break
		}

		// 批次处理间隔和速率限制
		rc.WaitIfNeeded(len(dataList))
		offset += pageSize
	}

	// 整理每个打包文件
	for pkgID, dataList := range pkgDataMap {
		// 读取整个打包文件
		// 使用与core/data.go中相同的路径计算方式
		fileName := fmt.Sprintf("%d_%d", pkgID, 0)
		hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
		pkgPath := filepath.Join(ORCAS_DATA, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)
		pkgFile, err := os.Open(pkgPath)
		if err != nil {
			continue
		}

		// 读取文件大小
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

		// 检查每个数据是否仍被引用
		validData := make([]*DataInfo, 0)
		validOffsets := make([]int, 0) // 有效的偏移位置列表

		for _, dataInfo := range dataList {
			// 检查DataInfo的ID是否为负数（标记为已删除）
			if dataInfo.ID < 0 {
				// DataInfo的ID为负数，说明已标记删除
				// 查询引用这个DataInfo的对象，获取删除时间（MTime）
				objs, err := ma.GetObjByDataID(c, bktID, dataInfo.ID) // 使用负数ID查找对象
				if err == nil && len(objs) > 0 {
					// 使用第一个对象的MTime作为删除时间的参考
					deleteTime := objs[0].MTime
					if deleteTime > 0 {
						// 检查删除时间是否超过窗口时间
						now := Now()
						if now-deleteTime >= accessWindow {
							// 超过窗口时间，可以删除这个数据块
							// 不添加到validData中，后续会被清理
							continue
						} else {
							// 未超过窗口时间，暂时保留（可能被恢复）
							validData = append(validData, dataInfo)
							validOffsets = append(validOffsets, int(dataInfo.PkgOffset))
							continue
						}
					}
				}
				// 如果无法获取删除时间，默认不保留
				continue
			}

			// DataInfo的ID为正数，检查是否仍被引用
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataInfo.ID})
			if err != nil {
				continue
			}

			if refCounts[dataInfo.ID] > 0 {
				// 仍被正常引用，保留
				validData = append(validData, dataInfo)
				validOffsets = append(validOffsets, int(dataInfo.PkgOffset))
			}
			// 如果没有引用且ID为正数，可能是孤儿数据，不添加到validData
		}

		if len(validData) == 0 {
			// 打包文件中没有有效数据，可以删除整个打包文件
			// 先删除所有相关数据的元数据，再删除文件，避免出现有元数据但没有文件的情况
			dataIDs := make([]int64, 0, len(dataList))
			for _, di := range dataList {
				dataIDs = append(dataIDs, di.ID)
			}
			if len(dataIDs) > 0 {
				if err := ma.DeleteData(c, bktID, dataIDs); err != nil {
					// 元数据删除失败，不继续删除文件
					continue
				}
			}
			// 这里传入 nil 表示不是打包数据，而是打包文件本身，需要删除整个文件
			deleteDataFiles(ORCAS_DATA, bktID, pkgID, nil, nil)
			result.CompactedPkgs++
			continue
		}

		// 重新打包有效数据（前移）
		newPkgBuffer := make([]byte, 0, chunkSize)
		newDataInfos := make([]*DataInfo, 0, len(validData))

		// 预先计算每个数据在新打包文件中的偏移量
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
				// 确保缓冲区足够大
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

		// 如果新打包文件更小，更新
		if int64(len(newPkgBuffer)) < fileSize {
			// 使用新pkgID方案：创建新的打包文件，更新所有引用，旧数据放入等待队列
			// 1. 生成新的pkgID（作为新的DataID）
			newPkgID := NewID()
			if newPkgID <= 0 {
				continue
			}

			// 2. 创建新的打包文件
			err := da.Write(c, bktID, newPkgID, 0, newPkgBuffer)
			if err != nil {
				continue // 写入失败，跳过
			}

			// 3. 收集所有需要更新的对象（引用旧DataInfo的对象）
			oldDataIDs := make([]int64, 0, len(validData))

			for i, dataInfo := range validData {
				oldDataID := dataInfo.ID
				// 创建新的DataInfo，使用新的pkgID
				newDataID := NewID()
				if newDataID <= 0 {
					continue
				}

				// 使用预先计算的偏移量
				newOffset := newOffsets[i]

				newDataInfo := *dataInfo
				newDataInfo.ID = newDataID
				newDataInfo.PkgID = newPkgID
				newDataInfo.PkgOffset = uint32(newOffset)

				// 记录旧的DataID
				oldDataIDs = append(oldDataIDs, oldDataID)

				// 更新新数据的元数据
				ma.PutData(c, bktID, []*DataInfo{&newDataInfo})

				// 更新所有引用旧DataID的对象的DataID为新DataID
				ma.UpdateObjDataID(c, bktID, oldDataID, newDataID)
			}

			// 4. 使用延迟删除处理旧pkgID和旧DataID
			// 将旧的pkgID放入延迟删除
			delayedDelete(c, bktID, pkgID, ma, da)

			// 将旧的DataID放入延迟删除
			for _, oldDataID := range oldDataIDs {
				delayedDelete(c, bktID, oldDataID, ma, da)
			}

			// 5. 计算释放的空间（暂时不减少，等待延迟时间后再减少）
			// 因为旧文件还在，只是不再被引用
			freedSize := fileSize - int64(len(newPkgBuffer))
			if freedSize > 0 {
				result.FreedSize += freedSize
			}
			result.CompactedPkgs++
		}
	}

	return result, nil
}
