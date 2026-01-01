package vfs

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orcastor/orcas/core"
)

// VersionRetentionPolicy defines version retention rules
type VersionRetentionPolicy struct {
	// Enable retention policy
	Enabled bool

	// Time window: within this window, only keep the last full version
	// 时间窗口：窗口内只保留最后一个完整版本 (秒)
	// Default: 5 minutes (300 seconds)
	TimeWindowSeconds int64

	// Maximum number of versions to keep (full + journal)
	// 最大保留版本数（完整版本+增量版本总数）
	// 0 = unlimited
	// Default: 10
	MaxVersions int

	// Minimum number of full versions to keep
	// 最少保留的完整版本数（无论时间和数量限制）
	// Default: 3
	MinFullVersions int

	// Maximum number of journals per full version
	// 每个完整版本最多保留的journal数量
	// 0 = unlimited
	// Default: 10
	MaxJournalsPerBase int

	// Cleanup interval (seconds)
	// 清理间隔（秒）
	// Default: 5 minutes (300 seconds)
	CleanupIntervalSeconds int64
}

// DefaultVersionRetentionPolicy returns default retention policy
func DefaultVersionRetentionPolicy() VersionRetentionPolicy {
	return VersionRetentionPolicy{
		Enabled:                true,
		TimeWindowSeconds:      5 * 60, // 5 minutes
		MaxVersions:            10,     // 10 versions total
		MinFullVersions:        3,      // Keep at least 3 full versions
		MaxJournalsPerBase:     10,     // 10 journals per base
		CleanupIntervalSeconds: 5 * 60, // Cleanup every 5 minutes
	}
}

// VersionRetentionManager manages version retention policy
type VersionRetentionManager struct {
	fs     *OrcasFS
	policy VersionRetentionPolicy
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Statistics
	totalCleanups int64 // Total cleanup runs (atomic)
	totalDeleted  int64 // Total versions deleted (atomic)
	lastCleanup   int64 // Last cleanup timestamp (atomic)
}

// NewVersionRetentionManager creates a new version retention manager
func NewVersionRetentionManager(fs *OrcasFS, policy VersionRetentionPolicy) *VersionRetentionManager {
	vrm := &VersionRetentionManager{
		fs:     fs,
		policy: policy,
		stopCh: make(chan struct{}),
	}

	if policy.Enabled {
		vrm.wg.Add(1)
		go vrm.worker()
		DebugLog("[VersionRetention] Started with policy: timeWindow=%ds, maxVersions=%d, minFull=%d, maxJournals=%d",
			policy.TimeWindowSeconds, policy.MaxVersions, policy.MinFullVersions, policy.MaxJournalsPerBase)
	} else {
		DebugLog("[VersionRetention] Disabled by policy")
	}

	return vrm
}

// worker runs periodic cleanup
func (vrm *VersionRetentionManager) worker() {
	defer vrm.wg.Done()

	interval := time.Duration(vrm.policy.CleanupIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	DebugLog("[VersionRetention] Worker started with interval=%v (for statistics only, actual cleanup is triggered per-file)", interval)

	for {
		select {
		case <-ticker.C:
			// Periodic logging of statistics only
			// Actual cleanup is triggered per-file after version creation
			vrm.logStats()
		case <-vrm.stopCh:
			DebugLog("[VersionRetention] Worker stopped")
			return
		}
	}
}

// logStats logs current statistics
func (vrm *VersionRetentionManager) logStats() {
	stats := vrm.GetRetentionStats()
	DebugLog("[VersionRetention] Stats: cleanups=%d, deleted=%d, lastCleanup=%d",
		stats["totalCleanups"], stats["totalDeleted"], stats["lastCleanup"])
}

// runCleanup performs cleanup for all files with versions (DEPRECATED - not used)
// Cleanup is now triggered per-file after version creation
func (vrm *VersionRetentionManager) runCleanup() {
	// This method is no longer used
	// Cleanup is triggered per-file via CleanupFileVersions()
	DebugLog("[VersionRetention] runCleanup called but deprecated - use CleanupFileVersions per file")
}

// CleanupFileVersions cleans up versions for a specific file (public method)
// This should be called after creating a new version for the file
func (vrm *VersionRetentionManager) CleanupFileVersions(fileID int64) int {
	if !vrm.policy.Enabled {
		return 0
	}

	atomic.AddInt64(&vrm.totalCleanups, 1)
	atomic.StoreInt64(&vrm.lastCleanup, time.Now().Unix())

	deleted := vrm.cleanupFileVersions(fileID)

	if deleted > 0 {
		atomic.AddInt64(&vrm.totalDeleted, int64(deleted))
	}

	return deleted
}

// cleanupFileVersions cleans up versions for a specific file
func (vrm *VersionRetentionManager) cleanupFileVersions(fileID int64) int {
	// 1. Get all versions
	versions, err := vrm.getFileVersions(fileID)
	if err != nil {
		DebugLog("[VersionRetention] Failed to get versions for fileID=%d: %v", fileID, err)
		return 0
	}

	if len(versions) == 0 {
		return 0
	}

	DebugLog("[VersionRetention] Processing fileID=%d with %d versions", fileID, len(versions))

	// 2. Separate full versions and journals
	fullVersions, journals := vrm.separateVersions(versions)

	DebugLog("[VersionRetention] FileID=%d: full=%d, journals=%d", fileID, len(fullVersions), len(journals))

	// 3. Build dependency map
	dependencies := vrm.buildDependencyMap(journals)

	// 4. Apply retention rules
	toDelete := vrm.selectVersionsToDelete(fullVersions, journals, dependencies)

	// 5. Delete selected versions
	if len(toDelete) > 0 {
		vrm.deleteVersions(toDelete)
		DebugLog("[VersionRetention] Cleaned fileID=%d: deleted=%d versions", fileID, len(toDelete))
		return len(toDelete)
	}

	return 0
}

// getFileVersions gets all versions for a file
func (vrm *VersionRetentionManager) getFileVersions(fileID int64) ([]*core.ObjectInfo, error) {
	objs, _, _, err := vrm.fs.h.List(vrm.fs.c, vrm.fs.bktID, fileID, core.ListOptions{
		Count: 0, // Get all
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}

	versions := make([]*core.ObjectInfo, 0, len(objs))
	for _, obj := range objs {
		if obj.Type == core.OBJ_TYPE_VERSION || obj.Type == core.OBJ_TYPE_JOURNAL {
			versions = append(versions, obj)
		}
	}

	return versions, nil
}

// separateVersions separates full versions and journals
func (vrm *VersionRetentionManager) separateVersions(versions []*core.ObjectInfo) (
	fullVersions, journals []*core.ObjectInfo) {

	for _, v := range versions {
		if v.Type == core.OBJ_TYPE_VERSION {
			fullVersions = append(fullVersions, v)
		} else if v.Type == core.OBJ_TYPE_JOURNAL {
			journals = append(journals, v)
		}
	}

	// Sort by MTime (newest first)
	sort.Slice(fullVersions, func(i, j int) bool {
		return fullVersions[i].MTime > fullVersions[j].MTime
	})
	sort.Slice(journals, func(i, j int) bool {
		return journals[i].MTime > journals[j].MTime
	})

	return
}

// buildDependencyMap builds a map of base version dependencies
func (vrm *VersionRetentionManager) buildDependencyMap(journals []*core.ObjectInfo) map[int64][]int64 {
	dependencies := make(map[int64][]int64) // baseVersionID -> []journalIDs

	for _, journal := range journals {
		baseVersionID := parseBaseVersionID(journal.Extra)
		if baseVersionID > 0 {
			dependencies[baseVersionID] = append(dependencies[baseVersionID], journal.ID)
		}
	}

	return dependencies
}

// parseBaseVersionID parses baseVersionID from Extra JSON field
func parseBaseVersionID(extra string) int64 {
	if extra == "" {
		return 0
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(extra), &data); err != nil {
		return 0
	}

	if baseID, ok := data["baseVersionID"].(float64); ok {
		return int64(baseID)
	}

	return 0
}

// selectVersionsToDelete applies all retention rules and selects versions to delete
func (vrm *VersionRetentionManager) selectVersionsToDelete(
	fullVersions, journals []*core.ObjectInfo,
	dependencies map[int64][]int64) []int64 {

	toDelete := make([]int64, 0)
	now := time.Now().Unix()

	// Rule 1: Time window merge (only for full versions)
	deleted1 := vrm.applyTimeWindowRule(fullVersions, dependencies, now)
	toDelete = append(toDelete, deleted1...)
	DebugLog("[VersionRetention] Rule 1 (time window): selected %d versions", len(deleted1))

	// Rule 2: Max versions limit
	deleted2 := vrm.applyMaxVersionsRule(fullVersions, journals, dependencies, toDelete)
	toDelete = append(toDelete, deleted2...)
	DebugLog("[VersionRetention] Rule 2 (max versions): selected %d versions", len(deleted2))

	// Rule 3: Max journals per base
	deleted3 := vrm.applyMaxJournalsRule(journals, dependencies, toDelete)
	toDelete = append(toDelete, deleted3...)
	DebugLog("[VersionRetention] Rule 3 (max journals): selected %d versions", len(deleted3))

	// Deduplicate
	return deduplicateIDs(toDelete)
}

// applyTimeWindowRule: Within time window, only keep last full version
// Time windows are absolute: [0:00-0:05), [0:05-0:10), [0:10-0:15), etc.
func (vrm *VersionRetentionManager) applyTimeWindowRule(
	fullVersions []*core.ObjectInfo,
	dependencies map[int64][]int64,
	now int64) []int64 {

	if vrm.policy.TimeWindowSeconds == 0 {
		return nil // No time window enforcement
	}

	toDelete := make([]int64, 0)
	windowSize := vrm.policy.TimeWindowSeconds

	// Group versions by time window
	// Window ID = MTime / WindowSize
	// Example: WindowSize=300 (5 minutes)
	//   MTime=100  → Window 0 (0-299)
	//   MTime=350  → Window 1 (300-599)
	//   MTime=650  → Window 2 (600-899)
	windowGroups := make(map[int64][]*core.ObjectInfo)

	for _, v := range fullVersions {
		windowID := v.MTime / windowSize
		windowGroups[windowID] = append(windowGroups[windowID], v)
	}

	// For each window, keep only the newest version
	for windowID, versions := range windowGroups {
		if len(versions) <= 1 {
			continue // Only one version in this window, keep it
		}

		// Sort by MTime (newest first)
		sort.Slice(versions, func(i, j int) bool {
			return versions[i].MTime > versions[j].MTime
		})

		// Keep the first (newest), delete the rest if no dependencies
		for i := 1; i < len(versions); i++ {
			v := versions[i]
			if len(dependencies[v.ID]) == 0 {
				toDelete = append(toDelete, v.ID)
				DebugLog("[VersionRetention] Time window %d: deleting version %d (MTime=%d, keeping newer %d)",
					windowID, v.ID, v.MTime, versions[0].ID)
			} else {
				DebugLog("[VersionRetention] Time window %d: keeping version %d (MTime=%d) - has %d journal dependencies",
					windowID, v.ID, v.MTime, len(dependencies[v.ID]))
			}
		}

		if len(versions) > 1 {
			DebugLog("[VersionRetention] Window %d [%d-%d): kept version %d (MTime=%d), marked %d for deletion",
				windowID, windowID*windowSize, (windowID+1)*windowSize, versions[0].ID, versions[0].MTime, len(versions)-1)
		}
	}

	return toDelete
}

// applyMaxVersionsRule: Keep only MaxVersions versions
func (vrm *VersionRetentionManager) applyMaxVersionsRule(
	fullVersions, journals []*core.ObjectInfo,
	dependencies map[int64][]int64,
	alreadyDeleted []int64) []int64 {

	if vrm.policy.MaxVersions == 0 {
		return nil // No limit
	}

	// Count versions not already marked for deletion
	totalVersions := countVersionsNotInDeleteList(fullVersions, journals, alreadyDeleted)
	if totalVersions <= vrm.policy.MaxVersions {
		return nil // Under limit
	}

	toDelete := make([]int64, 0)

	// Combine and sort all versions by MTime (newest first)
	allVersions := append([]*core.ObjectInfo{}, fullVersions...)
	allVersions = append(allVersions, journals...)
	sort.Slice(allVersions, func(i, j int) bool {
		return allVersions[i].MTime > allVersions[j].MTime
	})

	kept := 0
	fullKept := 0

	for _, v := range allVersions {
		// Skip if already marked for deletion
		if contains(alreadyDeleted, v.ID) {
			continue
		}

		if kept < vrm.policy.MaxVersions {
			// Keep this version
			kept++
			if v.Type == core.OBJ_TYPE_VERSION {
				fullKept++
			}
		} else {
			// Delete this version if allowed
			canDelete := true

			// Don't delete if it's a base version with dependencies
			if v.Type == core.OBJ_TYPE_VERSION && len(dependencies[v.ID]) > 0 {
				canDelete = false
			}

			// Don't delete if we'd go below MinFullVersions
			if v.Type == core.OBJ_TYPE_VERSION && fullKept <= vrm.policy.MinFullVersions {
				canDelete = false
			}

			if canDelete {
				toDelete = append(toDelete, v.ID)
				DebugLog("[VersionRetention] Max versions: deleting version %d (MTime=%d)",
					v.ID, v.MTime)
			} else {
				kept++ // Keep it
				if v.Type == core.OBJ_TYPE_VERSION {
					fullKept++
				}
			}
		}
	}

	return toDelete
}

// applyMaxJournalsRule: Keep only MaxJournalsPerBase journals per base
// When journal count exceeds limit, merge oldest journals with base to create new base
func (vrm *VersionRetentionManager) applyMaxJournalsRule(
	journals []*core.ObjectInfo,
	dependencies map[int64][]int64,
	alreadyDeleted []int64) []int64 {

	if vrm.policy.MaxJournalsPerBase == 0 {
		return nil // No limit
	}

	DebugLog("[VersionRetention] Applying MaxJournalsPerBase rule: limit=%d", vrm.policy.MaxJournalsPerBase)

	// Group journals by base version
	journalsByBase := make(map[int64][]*core.ObjectInfo)
	for _, j := range journals {
		if contains(alreadyDeleted, j.ID) {
			continue
		}
		baseID := parseBaseVersionID(j.Extra)
		if baseID > 0 {
			journalsByBase[baseID] = append(journalsByBase[baseID], j)
		}
	}

	toDelete := make([]int64, 0)

	// Process each base version
	for baseID, baseJournals := range journalsByBase {
		if len(baseJournals) <= vrm.policy.MaxJournalsPerBase {
			continue // Under limit
		}

		DebugLog("[VersionRetention] Base %d has %d journals (limit=%d), merging oldest",
			baseID, len(baseJournals), vrm.policy.MaxJournalsPerBase)

		// Sort journals by MTime (oldest first)
		sort.Slice(baseJournals, func(i, j int) bool {
			return baseJournals[i].MTime < baseJournals[j].MTime
		})

		// Calculate how many journals to merge
		excessCount := len(baseJournals) - vrm.policy.MaxJournalsPerBase
		// Merge at least excessCount, but can merge more for efficiency
		mergeCount := excessCount
		if mergeCount < 2 {
			mergeCount = 2 // Merge at least 2 journals to be effective
		}
		if mergeCount > len(baseJournals) {
			mergeCount = len(baseJournals)
		}

		// Get base version object
		baseVersionObjs, err := vrm.fs.h.Get(vrm.fs.c, vrm.fs.bktID, []int64{baseID})
		if err != nil || len(baseVersionObjs) == 0 {
			DebugLog("[VersionRetention] WARNING: Failed to get base version %d: %v", baseID, err)
			continue
		}
		baseVersion := baseVersionObjs[0]

		// Split journals into merge and remaining
		journalsToMerge := baseJournals[:mergeCount]
		remainingJournals := baseJournals[mergeCount:]

		// Perform merge (asynchronous to avoid blocking)
		go func(base *core.ObjectInfo, toMerge, remaining []*core.ObjectInfo) {
			if err := vrm.mergeBaseAndJournals(base, toMerge, remaining); err != nil {
				DebugLog("[VersionRetention] ERROR: Failed to merge journals for base %d: %v", base.ID, err)
			} else {
				DebugLog("[VersionRetention] Successfully merged %d journals for base %d", len(toMerge), base.ID)
			}
		}(baseVersion, journalsToMerge, remainingJournals)

		// Note: We don't add journals to toDelete here because they will be deleted
		// by mergeBaseAndJournals after the merge is complete (with delayed delete)
	}

	return toDelete
}

// MergeConfig defines configuration for journal merge operations
type MergeConfig struct {
	// Maximum memory to use for merge buffer (default: 100MB)
	MaxMemoryBuffer int64

	// Chunk size for streaming merge (default: 10MB)
	StreamChunkSize int64

	// Use streaming merge for files larger than this (default: 50MB)
	StreamingThreshold int64

	// Maximum number of journals to merge at once (default: 5)
	MaxJournalsPerMerge int
}

// DefaultMergeConfig returns default merge configuration
func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		MaxMemoryBuffer:     100 << 20, // 100MB
		StreamChunkSize:     10 << 20,  // 10MB
		StreamingThreshold:  50 << 20,  // 50MB
		MaxJournalsPerMerge: 5,
	}
}

// mergeBaseAndJournals merges a base version with its oldest journals to create a new base
// Supports both in-memory and streaming merge strategies based on file size
func (vrm *VersionRetentionManager) mergeBaseAndJournals(
	baseVersion *core.ObjectInfo,
	journalsToMerge []*core.ObjectInfo,
	remainingJournals []*core.ObjectInfo) error {

	if len(journalsToMerge) == 0 {
		return nil // Nothing to merge
	}

	DebugLog("[VersionRetention MergeJournals] Starting merge: baseVersion=%d, journalsToMerge=%d, remaining=%d",
		baseVersion.ID, len(journalsToMerge), len(remainingJournals))

	// Get merge configuration
	mergeConfig := DefaultMergeConfig()

	// Decide merge strategy based on file size
	if baseVersion.Size > mergeConfig.StreamingThreshold {
		DebugLog("[VersionRetention MergeJournals] Using STREAMING merge for large file: size=%d", baseVersion.Size)
		return vrm.mergeBaseAndJournalsStreaming(baseVersion, journalsToMerge, remainingJournals, mergeConfig)
	}

	// Limit number of journals to merge at once
	if len(journalsToMerge) > mergeConfig.MaxJournalsPerMerge {
		DebugLog("[VersionRetention MergeJournals] Too many journals (%d), splitting into batches", len(journalsToMerge))
		return vrm.mergeBaseAndJournalsInBatches(baseVersion, journalsToMerge, remainingJournals, mergeConfig)
	}

	DebugLog("[VersionRetention MergeJournals] Using IN-MEMORY merge for small file: size=%d", baseVersion.Size)
	return vrm.mergeBaseAndJournalsInMemory(baseVersion, journalsToMerge, remainingJournals)
}

// mergeBaseAndJournalsInMemory performs in-memory merge (for small files)
func (vrm *VersionRetentionManager) mergeBaseAndJournalsInMemory(
	baseVersion *core.ObjectInfo,
	journalsToMerge []*core.ObjectInfo,
	remainingJournals []*core.ObjectInfo) error {

	lh, ok := vrm.fs.h.(*core.LocalHandler)
	if !ok {
		return fmt.Errorf("handler is not LocalHandler")
	}

	// Step 1: Sort journals by MTime (oldest first)
	sort.Slice(journalsToMerge, func(i, j int) bool {
		return journalsToMerge[i].MTime < journalsToMerge[j].MTime
	})

	baseSize := baseVersion.Size
	baseData := make([]byte, baseSize)

	if baseVersion.DataID > 0 && baseVersion.DataID != core.EmptyDataID {
		// Read base data in chunks
		chunkSize := vrm.fs.chunkSize
		if chunkSize <= 0 {
			chunkSize = 10 << 20 // Default 10MB
		}

		offset := int64(0)
		for offset < baseSize {
			chunkIdx := int(offset / chunkSize)
			chunkData, err := vrm.fs.h.GetData(vrm.fs.c, vrm.fs.bktID, baseVersion.DataID, chunkIdx)
			if err != nil {
				return fmt.Errorf("failed to read base chunk %d: %w", chunkIdx, err)
			}

			copyLen := int64(len(chunkData))
			if offset+copyLen > baseSize {
				copyLen = baseSize - offset
			}
			copy(baseData[offset:offset+copyLen], chunkData[:copyLen])
			offset += copyLen
		}
	}

	DebugLog("[VersionRetention MergeJournals] Read base data: size=%d", len(baseData))

	// Step 3: Apply each journal to base data
	currentSize := baseSize
	for _, jVersion := range journalsToMerge {
		// Parse journal extra data to get journalDataID
		var extraData map[string]interface{}
		if err := json.Unmarshal([]byte(jVersion.Extra), &extraData); err != nil {
			return fmt.Errorf("failed to parse journal extra: %w", err)
		}

		journalDataID, ok := extraData["journalDataID"].(float64)
		if !ok {
			return fmt.Errorf("journalDataID not found in journal %d", jVersion.ID)
		}

		// Read journal data
		journalBytes, err := vrm.fs.h.GetData(vrm.fs.c, vrm.fs.bktID, int64(journalDataID), 0)
		if err != nil {
			return fmt.Errorf("failed to read journal data %d: %w", int64(journalDataID), err)
		}

		// Create a temporary journal object to deserialize
		tempJournal := &Journal{
			fileID:   baseVersion.PID,
			dataID:   baseVersion.DataID,
			baseSize: currentSize,
			entries:  make([]JournalEntry, 0),
		}

		// Deserialize journal entries
		if err := tempJournal.DeserializeJournal(journalBytes); err != nil {
			return fmt.Errorf("failed to deserialize journal %d: %w", jVersion.ID, err)
		}

		DebugLog("[VersionRetention MergeJournals] Applying journal %d: entries=%d", jVersion.ID, len(tempJournal.entries))

		// Apply journal entries to base data
		for i := range tempJournal.entries {
			entry := &tempJournal.entries[i]
			entryEnd := entry.Offset + entry.Length

			// Extend base data if needed
			if entryEnd > int64(len(baseData)) {
				newSize := entryEnd
				extended := make([]byte, newSize)
				copy(extended, baseData)
				baseData = extended
			}

			// Copy entry data
			copy(baseData[entry.Offset:entry.Offset+entry.Length], entry.Data)

			// Update current size
			if entryEnd > currentSize {
				currentSize = entryEnd
			}
		}

		// Update current size from journal
		journalSize := atomic.LoadInt64(&tempJournal.currentSize)
		if journalSize > currentSize {
			currentSize = journalSize
		}
	}

	// Adjust baseData to final size
	if currentSize < int64(len(baseData)) {
		baseData = baseData[:currentSize]
	} else if currentSize > int64(len(baseData)) {
		extended := make([]byte, currentSize)
		copy(extended, baseData)
		baseData = extended
	}

	DebugLog("[VersionRetention MergeJournals] Merged data: finalSize=%d", len(baseData))

	// Step 4: Create new base version with merged data
	newDataID := core.NewID()
	da := lh.GetDataAdapter()

	// Write merged data in chunks
	chunkSize := vrm.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20
	}

	sn := 0
	offset := int64(0)
	for offset < currentSize {
		endOffset := offset + chunkSize
		if endOffset > currentSize {
			endOffset = currentSize
		}

		if err := da.Write(vrm.fs.c, vrm.fs.bktID, newDataID, sn, baseData[offset:endOffset]); err != nil {
			return fmt.Errorf("failed to write merged chunk %d: %w", sn, err)
		}

		offset = endOffset
		sn++
	}

	// Calculate checksums
	hdrXXH3, xxh3Val, sha256_0, sha256_1, sha256_2, sha256_3 := core.CalculateChecksums(baseData)

	// Create DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: currentSize,
		Kind:     core.DATA_NORMAL,
		HdrXXH3:  hdrXXH3,
		XXH3:     xxh3Val,
		SHA256_0: sha256_0,
		SHA256_1: sha256_1,
		SHA256_2: sha256_2,
		SHA256_3: sha256_3,
	}

	_, err := lh.PutDataInfo(vrm.fs.c, vrm.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return fmt.Errorf("failed to write merged DataInfo: %w", err)
	}

	// Create new version object
	newVersionID := core.NewID()
	mTime := core.Now()
	newVersionObj := &core.ObjectInfo{
		ID:     newVersionID,
		PID:    baseVersion.PID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   currentSize,
		MTime:  mTime,
		Name:   fmt.Sprintf("%d", mTime),
		Extra:  `{"versionType":1,"merged":true}`,
	}

	_, err = lh.Put(vrm.fs.c, vrm.fs.bktID, []*core.ObjectInfo{newVersionObj})
	if err != nil {
		return fmt.Errorf("failed to create merged version: %w", err)
	}

	DebugLog("[VersionRetention MergeJournals] Created new merged version: versionID=%d, dataID=%d, size=%d",
		newVersionID, newDataID, currentSize)

	// Step 5: Update remaining journals to point to new base version
	if len(remainingJournals) > 0 {
		updatedJournals := make([]*core.ObjectInfo, 0, len(remainingJournals))
		for _, jVersion := range remainingJournals {
			// Parse and update extra data
			var extraData map[string]interface{}
			if err := json.Unmarshal([]byte(jVersion.Extra), &extraData); err != nil {
				DebugLog("[VersionRetention MergeJournals] WARNING: Failed to parse journal extra for %d: %v", jVersion.ID, err)
				continue
			}

			// Update baseVersionID to point to new merged version
			extraData["baseVersionID"] = float64(newVersionID)
			updatedExtra, _ := json.Marshal(extraData)

			updatedJournal := &core.ObjectInfo{
				ID:     jVersion.ID,
				PID:    jVersion.PID,
				Type:   jVersion.Type,
				DataID: newDataID, // Point to new base DataID
				Size:   jVersion.Size,
				MTime:  jVersion.MTime,
				Name:   jVersion.Name,
				Extra:  string(updatedExtra),
			}
			updatedJournals = append(updatedJournals, updatedJournal)
		}

		if len(updatedJournals) > 0 {
			_, err = lh.Put(vrm.fs.c, vrm.fs.bktID, updatedJournals)
			if err != nil {
				DebugLog("[VersionRetention MergeJournals] WARNING: Failed to update remaining journals: %v", err)
				// Don't fail the whole operation
			} else {
				DebugLog("[VersionRetention MergeJournals] Updated %d remaining journals to point to new base", len(updatedJournals))
			}
		}
	}

	// Step 6: Schedule delayed delete for old versions (concurrent access protection)
	// Wait 5 seconds before deleting to allow concurrent readers to finish
	oldVersionIDs := []int64{baseVersion.ID}
	for _, j := range journalsToMerge {
		oldVersionIDs = append(oldVersionIDs, j.ID)
	}

	go func() {
		time.Sleep(5 * time.Second) // Delayed delete for concurrent access protection

		DebugLog("[VersionRetention MergeJournals] Delaying delete of %d old versions (5s protection)", len(oldVersionIDs))

		for _, versionID := range oldVersionIDs {
			if err := vrm.fs.h.Delete(vrm.fs.c, vrm.fs.bktID, versionID); err != nil {
				DebugLog("[VersionRetention MergeJournals] WARNING: Failed to delete old version %d: %v", versionID, err)
			}
		}

		DebugLog("[VersionRetention MergeJournals] Deleted %d old versions after delay", len(oldVersionIDs))
	}()

	DebugLog("[VersionRetention MergeJournals] Merge completed successfully")
	return nil
}

// mergeBaseAndJournalsStreaming performs streaming merge for large files
// This avoids loading the entire file into memory
func (vrm *VersionRetentionManager) mergeBaseAndJournalsStreaming(
	baseVersion *core.ObjectInfo,
	journalsToMerge []*core.ObjectInfo,
	remainingJournals []*core.ObjectInfo,
	config MergeConfig) error {

	DebugLog("[VersionRetention MergeStreaming] Starting streaming merge: baseSize=%d, journals=%d",
		baseVersion.Size, len(journalsToMerge))

	lh, ok := vrm.fs.h.(*core.LocalHandler)
	if !ok {
		return fmt.Errorf("handler is not LocalHandler")
	}

	// Sort journals by MTime
	sort.Slice(journalsToMerge, func(i, j int) bool {
		return journalsToMerge[i].MTime < journalsToMerge[j].MTime
	})

	// Load all journal entries into memory (should be small even for large files)
	type journalEntryWithID struct {
		offset int64
		length int64
		data   []byte
	}
	allEntries := make([]journalEntryWithID, 0)

	for _, jVersion := range journalsToMerge {
		var extraData map[string]interface{}
		if err := json.Unmarshal([]byte(jVersion.Extra), &extraData); err != nil {
			return fmt.Errorf("failed to parse journal extra: %w", err)
		}

		journalDataID, ok := extraData["journalDataID"].(float64)
		if !ok {
			return fmt.Errorf("journalDataID not found in journal %d", jVersion.ID)
		}

		journalBytes, err := vrm.fs.h.GetData(vrm.fs.c, vrm.fs.bktID, int64(journalDataID), 0)
		if err != nil {
			return fmt.Errorf("failed to read journal data: %w", err)
		}

		tempJournal := &Journal{
			fileID:   baseVersion.PID,
			dataID:   baseVersion.DataID,
			baseSize: baseVersion.Size,
			entries:  make([]JournalEntry, 0),
		}

		if err := tempJournal.DeserializeJournal(journalBytes); err != nil {
			return fmt.Errorf("failed to deserialize journal: %w", err)
		}

		for i := range tempJournal.entries {
			entry := &tempJournal.entries[i]
			allEntries = append(allEntries, journalEntryWithID{
				offset: entry.Offset,
				length: entry.Length,
				data:   entry.Data,
			})
		}
	}

	DebugLog("[VersionRetention MergeStreaming] Loaded %d journal entries", len(allEntries))

	// Sort entries by offset for efficient streaming
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].offset < allEntries[j].offset
	})

	// Calculate final size
	currentSize := baseVersion.Size
	for i := range allEntries {
		entryEnd := allEntries[i].offset + allEntries[i].length
		if entryEnd > currentSize {
			currentSize = entryEnd
		}
	}

	// Create new DataID and write data in chunks
	newDataID := core.NewID()
	da := lh.GetDataAdapter()
	chunkSize := config.StreamChunkSize

	entryIdx := 0
	for chunkStart := int64(0); chunkStart < currentSize; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > currentSize {
			chunkEnd = currentSize
		}
		chunkLen := chunkEnd - chunkStart

		// Read base chunk if within base size
		chunkData := make([]byte, chunkLen)
		if chunkStart < baseVersion.Size {
			readLen := chunkLen
			if chunkStart+readLen > baseVersion.Size {
				readLen = baseVersion.Size - chunkStart
			}

			if baseVersion.DataID > 0 && baseVersion.DataID != core.EmptyDataID {
				baseChunkIdx := int(chunkStart / vrm.fs.chunkSize)
				baseChunkData, err := vrm.fs.h.GetData(vrm.fs.c, vrm.fs.bktID, baseVersion.DataID, baseChunkIdx)
				if err == nil {
					baseChunkOffset := chunkStart % vrm.fs.chunkSize
					copyLen := readLen
					if baseChunkOffset+copyLen > int64(len(baseChunkData)) {
						copyLen = int64(len(baseChunkData)) - baseChunkOffset
					}
					if copyLen > 0 {
						copy(chunkData, baseChunkData[baseChunkOffset:baseChunkOffset+copyLen])
					}
				}
			}
		}

		// Apply journal entries that overlap with this chunk
		for entryIdx < len(allEntries) {
			entry := &allEntries[entryIdx]
			entryEnd := entry.offset + entry.length

			if entry.offset >= chunkEnd {
				break // This and following entries are beyond current chunk
			}

			if entryEnd <= chunkStart {
				entryIdx++ // This entry is before current chunk
				continue
			}

			// Calculate overlap
			overlapStart := entry.offset
			if overlapStart < chunkStart {
				overlapStart = chunkStart
			}

			overlapEnd := entryEnd
			if overlapEnd > chunkEnd {
				overlapEnd = chunkEnd
			}

			// Copy overlapping data
			srcOffset := overlapStart - entry.offset
			dstOffset := overlapStart - chunkStart
			copyLength := overlapEnd - overlapStart

			copy(chunkData[dstOffset:dstOffset+copyLength], entry.data[srcOffset:srcOffset+copyLength])

			if entryEnd <= chunkEnd {
				entryIdx++ // Done with this entry
			} else {
				break // This entry continues into next chunk
			}
		}

		// Write chunk
		chunkIdx := int(chunkStart / chunkSize)
		if err := da.Write(vrm.fs.c, vrm.fs.bktID, newDataID, chunkIdx, chunkData); err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", chunkIdx, err)
		}

		DebugLog("[VersionRetention MergeStreaming] Wrote chunk %d: %d bytes", chunkIdx, len(chunkData))
	}

	// Create DataInfo and new version (same as in-memory merge)
	return vrm.finalizeMerge(lh, newDataID, currentSize, baseVersion, journalsToMerge, remainingJournals)
}

// mergeBaseAndJournalsInBatches splits large number of journals into smaller batches
func (vrm *VersionRetentionManager) mergeBaseAndJournalsInBatches(
	baseVersion *core.ObjectInfo,
	journalsToMerge []*core.ObjectInfo,
	remainingJournals []*core.ObjectInfo,
	config MergeConfig) error {

	DebugLog("[VersionRetention MergeBatches] Processing %d journals in batches of %d",
		len(journalsToMerge), config.MaxJournalsPerMerge)

	// Sort journals by MTime
	sort.Slice(journalsToMerge, func(i, j int) bool {
		return journalsToMerge[i].MTime < journalsToMerge[j].MTime
	})

	currentBase := baseVersion
	processed := 0

	for processed < len(journalsToMerge) {
		// Take next batch
		batchEnd := processed + config.MaxJournalsPerMerge
		if batchEnd > len(journalsToMerge) {
			batchEnd = len(journalsToMerge)
		}

		batchJournals := journalsToMerge[processed:batchEnd]

		// Determine which journals remain after this batch
		var batchRemaining []*core.ObjectInfo
		if batchEnd < len(journalsToMerge) {
			// More journals to process after this batch
			batchRemaining = journalsToMerge[batchEnd:]
		} else {
			// This is the last batch, use original remaining journals
			batchRemaining = remainingJournals
		}

		DebugLog("[VersionRetention MergeBatches] Processing batch %d-%d of %d journals",
			processed+1, batchEnd, len(journalsToMerge))

		// Merge this batch
		if err := vrm.mergeBaseAndJournalsInMemory(currentBase, batchJournals, batchRemaining); err != nil {
			return fmt.Errorf("failed to merge batch %d-%d: %w", processed+1, batchEnd, err)
		}

		// Update currentBase to the newly created version for next iteration
		// Note: In a real implementation, we'd need to retrieve the new version ID
		// For now, we'll just process one batch and return
		// TODO: Implement proper batch chaining
		break
	}

	DebugLog("[VersionRetention MergeBatches] Batch merge completed")
	return nil
}

// finalizeMerge creates DataInfo and version object after data is written
// For large files, skip recalculating checksums to save time/memory
func (vrm *VersionRetentionManager) finalizeMerge(
	lh *core.LocalHandler,
	newDataID int64,
	currentSize int64,
	baseVersion *core.ObjectInfo,
	journalsToMerge []*core.ObjectInfo,
	remainingJournals []*core.ObjectInfo) error {

	// For small files (<10MB), calculate accurate checksums
	// For large files, use placeholder checksums to avoid memory pressure
	var hdrXXH3, xxh3Val, sha256_0, sha256_1, sha256_2, sha256_3 int64

	if currentSize < 10<<20 {
		// Small file: calculate accurate checksums
		finalData := make([]byte, currentSize)
		chunkSize := vrm.fs.chunkSize
		if chunkSize <= 0 {
			chunkSize = 10 << 20
		}

		offset := int64(0)
		for offset < currentSize {
			chunkIdx := int(offset / chunkSize)
			chunkData, err := vrm.fs.h.GetData(vrm.fs.c, vrm.fs.bktID, newDataID, chunkIdx)
			if err != nil {
				return fmt.Errorf("failed to read back chunk for checksum: %w", err)
			}

			copyLen := int64(len(chunkData))
			if offset+copyLen > currentSize {
				copyLen = currentSize - offset
			}
			copy(finalData[offset:offset+copyLen], chunkData[:copyLen])
			offset += copyLen
		}

		hdrXXH3, xxh3Val, sha256_0, sha256_1, sha256_2, sha256_3 = core.CalculateChecksums(finalData)
	} else {
		// Large file: use placeholder checksums (will be calculated on-demand if needed)
		hdrXXH3 = 0
		xxh3Val = 0
		sha256_0 = 0
		sha256_1 = 0
		sha256_2 = 0
		sha256_3 = 0
		DebugLog("[VersionRetention Finalize] Skipping checksum calculation for large file: size=%d", currentSize)
	}

	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: currentSize,
		Kind:     core.DATA_NORMAL,
		HdrXXH3:  hdrXXH3,
		XXH3:     xxh3Val,
		SHA256_0: sha256_0,
		SHA256_1: sha256_1,
		SHA256_2: sha256_2,
		SHA256_3: sha256_3,
	}

	_, err := lh.PutDataInfo(vrm.fs.c, vrm.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return fmt.Errorf("failed to write DataInfo: %w", err)
	}

	// Create new version object
	newVersionID := core.NewID()
	mTime := core.Now()
	newVersionObj := &core.ObjectInfo{
		ID:     newVersionID,
		PID:    baseVersion.PID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   currentSize,
		MTime:  mTime,
		Name:   fmt.Sprintf("%d", mTime),
		Extra:  `{"versionType":1,"merged":true}`,
	}

	_, err = lh.Put(vrm.fs.c, vrm.fs.bktID, []*core.ObjectInfo{newVersionObj})
	if err != nil {
		return fmt.Errorf("failed to create merged version: %w", err)
	}

	DebugLog("[VersionRetention Finalize] Created merged version: versionID=%d, dataID=%d, size=%d",
		newVersionID, newDataID, currentSize)

	// Update remaining journals
	if len(remainingJournals) > 0 {
		updatedJournals := make([]*core.ObjectInfo, 0, len(remainingJournals))
		for _, jVersion := range remainingJournals {
			var extraData map[string]interface{}
			if err := json.Unmarshal([]byte(jVersion.Extra), &extraData); err != nil {
				DebugLog("[VersionRetention Finalize] WARNING: Failed to parse journal extra for %d: %v", jVersion.ID, err)
				continue
			}

			extraData["baseVersionID"] = float64(newVersionID)
			updatedExtra, _ := json.Marshal(extraData)

			updatedJournal := &core.ObjectInfo{
				ID:     jVersion.ID,
				PID:    jVersion.PID,
				Type:   jVersion.Type,
				DataID: newDataID,
				Size:   jVersion.Size,
				MTime:  jVersion.MTime,
				Name:   jVersion.Name,
				Extra:  string(updatedExtra),
			}
			updatedJournals = append(updatedJournals, updatedJournal)
		}

		if len(updatedJournals) > 0 {
			_, err = lh.Put(vrm.fs.c, vrm.fs.bktID, updatedJournals)
			if err != nil {
				DebugLog("[VersionRetention Finalize] WARNING: Failed to update remaining journals: %v", err)
			} else {
				DebugLog("[VersionRetention Finalize] Updated %d remaining journals", len(updatedJournals))
			}
		}
	}

	// Delayed delete
	oldVersionIDs := []int64{baseVersion.ID}
	for _, j := range journalsToMerge {
		oldVersionIDs = append(oldVersionIDs, j.ID)
	}

	go func() {
		time.Sleep(5 * time.Second)
		for _, versionID := range oldVersionIDs {
			vrm.fs.h.Delete(vrm.fs.c, vrm.fs.bktID, versionID)
		}
		DebugLog("[VersionRetention Finalize] Deleted %d old versions after delay", len(oldVersionIDs))
	}()

	return nil
}

// TODO List for Version Retention:
//
// 1. ✅ COMPLETED: Fix MaxJournalsPerBase logic (disabled broken implementation)
//
// 2. 🔧 IN PROGRESS: Implement journal merge functionality
//    Status: Design complete, implementation pending
//    Dependencies:
//      - Understand journal data format (vfs/journal.go)
//      - Implement data merge algorithm
//      - Add transaction support
//    Priority: HIGH (required to enable MaxJournalsPerBase)
//
// 3. 🔧 TODO: Add reference counting for version objects
//    Purpose: Track active readers of each version
//    Required for: Safe delayed deletion during merge
//    Implementation:
//      - Add reference counter in ObjectInfo or separate tracking
//      - Increment on version open, decrement on close
//      - Check ref count before delayed delete
//    Priority: HIGH (required for safe merge)
//
// 4. 🔧 TODO: Implement merge transaction support
//    Purpose: Ensure atomicity of merge operation
//    Steps:
//      a. Begin transaction
//      b. Create new base version
//      c. Update remaining journals' baseVersionID
//      d. Schedule delayed delete for old versions
//      e. Commit transaction
//    Rollback on any failure
//    Priority: HIGH (required for data integrity)
//
// 5. 🔧 TODO: Add merge performance optimization
//    Purpose: Avoid blocking during large file merge
//    Implementation:
//      - Background merge task queue
//      - Streaming merge for large files
//      - Progress tracking and cancellation
//    Priority: MEDIUM (performance optimization)
//
// 6. 🔧 TODO: Add merge metrics and monitoring
//    Purpose: Track merge operations and performance
//    Metrics:
//      - Total merges performed
//      - Average merge time
//      - Merge failures and reasons
//      - Space saved by merges
//    Priority: LOW (observability)
//
// 7. 🔧 TODO: Implement smart merge strategy
//    Purpose: Decide when to merge based on cost/benefit
//    Considerations:
//      - Journal size vs merge cost
//      - Access frequency of versions
//      - Available system resources
//    Priority: LOW (optimization)

// deleteVersions deletes the specified versions
func (vrm *VersionRetentionManager) deleteVersions(versionIDs []int64) error {
	if len(versionIDs) == 0 {
		return nil
	}

	DebugLog("[VersionRetention] Deleting %d versions", len(versionIDs))

	for _, versionID := range versionIDs {
		if err := vrm.fs.h.Delete(vrm.fs.c, vrm.fs.bktID, versionID); err != nil {
			DebugLog("[VersionRetention] WARNING: Failed to delete version %d: %v", versionID, err)
			// Continue with other deletions
		}
	}

	return nil
}

// GetRetentionStats returns retention statistics
func (vrm *VersionRetentionManager) GetRetentionStats() map[string]interface{} {
	return map[string]interface{}{
		"enabled":       vrm.policy.Enabled,
		"totalCleanups": atomic.LoadInt64(&vrm.totalCleanups),
		"totalDeleted":  atomic.LoadInt64(&vrm.totalDeleted),
		"lastCleanup":   atomic.LoadInt64(&vrm.lastCleanup),
		"policy": map[string]interface{}{
			"timeWindowSeconds":      vrm.policy.TimeWindowSeconds,
			"maxVersions":            vrm.policy.MaxVersions,
			"minFullVersions":        vrm.policy.MinFullVersions,
			"maxJournalsPerBase":     vrm.policy.MaxJournalsPerBase,
			"cleanupIntervalSeconds": vrm.policy.CleanupIntervalSeconds,
		},
	}
}

// Stop stops the retention manager
func (vrm *VersionRetentionManager) Stop() {
	close(vrm.stopCh)
	vrm.wg.Wait()
	DebugLog("[VersionRetention] Stopped")
}

// Helper functions

func deduplicateIDs(ids []int64) []int64 {
	seen := make(map[int64]bool)
	result := make([]int64, 0, len(ids))

	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}

	return result
}

func contains(slice []int64, item int64) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func countVersionsNotInDeleteList(fullVersions, journals []*core.ObjectInfo, deleteList []int64) int {
	count := 0

	for _, v := range fullVersions {
		if !contains(deleteList, v.ID) {
			count++
		}
	}

	for _, j := range journals {
		if !contains(deleteList, j.ID) {
			count++
		}
	}

	return count
}
