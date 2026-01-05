package vfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orcastor/orcas/core"
	"github.com/zeebo/xxh3"
)

// JournalEntry represents a single random write operation in the journal
// Entries are stored in a way that allows merging and efficient reads
type JournalEntry struct {
	Offset int64  // Write offset
	Length int64  // Write length
	Data   []byte // Write data (nil for sparse writes/holes)
}

// Journal represents a collection of random writes that can be merged and applied
// It supports both small and large file strategies
type Journal struct {
	fileID      int64          // File ID this journal belongs to
	dataID      int64          // Base DataID this journal is based on
	baseSize    int64          // Base file size before any journal writes
	virtualSize int64          // Virtual size for sparse files (0 if not sparse)
	entries     []JournalEntry // List of write entries
	entriesMu   sync.RWMutex   // Mutex for entries
	currentSize int64          // Current size including journal writes (atomic)
	memoryUsage int64          // Current memory usage in bytes (atomic)
	isLargeFile bool           // Whether this is a large file (different strategy)
	isSparse    bool           // Whether this is a sparse file
	threshold   int64          // Threshold for large/small file distinction
	mergeCount  int32          // Number of times entries have been merged (atomic)
	lastMerge   int64          // Last merge timestamp (atomic)
	isDirty     int32          // Whether journal has uncommitted changes (atomic)
	fs          *OrcasFS       // Reference to VFS
	created     int64          // Creation timestamp
	modified    int64          // Last modification timestamp (atomic)
	// Performance stats
	flushCount    int32 // Number of times flushed (atomic)
	lastFlush     int64 // Last flush timestamp (atomic)
	flushDuration int64 // Total flush duration in milliseconds (atomic)
	snapshotCount int32 // Number of snapshots created (atomic)
	lastSnapshot  int64 // Last snapshot timestamp (atomic)
	baseVersionID int64 // Base version ID this journal is based on
	// WAL support
	wal *JournalWAL // Write-Ahead Log for crash recovery
}

// JournalManager manages all active journals
type JournalManager struct {
	journals    map[int64]*Journal // map[fileID]*Journal
	mu          sync.RWMutex       // Mutex for journals map
	config      JournalConfig      // Configuration
	fs          *OrcasFS           // Reference to VFS
	totalMemory int64              // Total memory used by all journals (atomic)
}

// JournalConfig defines the configuration for journal management
type JournalConfig struct {
	Enabled             bool          // Whether journal is enabled
	SmallFileThreshold  int64         // Threshold for small/large file (default: 10MB)
	MergeInterval       time.Duration // Auto-merge interval for large files (default: 30s)
	MaxEntriesSmall     int           // Max entries before forced merge for small files (default: 100)
	MaxEntriesLarge     int           // Max entries before forced merge for large files (default: 1000)
	EnableAutoMerge     bool          // Enable automatic merging for large files
	MaxMemoryPerJournal int64         // Max memory per journal before forced flush (default: 50MB)
	MaxTotalMemory      int64         // Max total memory for all journals (default: 200MB)
	EnableMemoryLimit   bool          // Enable memory limit enforcement (default: true)

	// Snapshot configuration
	SnapshotEntryCount   int           // Create snapshot after N entries (default: 100)
	SnapshotMemorySize   int64         // Create snapshot after N bytes (default: 10MB)
	SnapshotTimeInterval time.Duration // Create snapshot after N time (default: 5min)

	// Full flush configuration
	FullFlushJournalCount int // Full flush after N journal snapshots (default: 10)
	FullFlushTotalEntries int // Full flush after N total entries (default: 1000)
}

// DefaultJournalConfig returns default configuration
func DefaultJournalConfig() JournalConfig {
	return JournalConfig{
		Enabled:             true,
		SmallFileThreshold:  10 << 20, // 10MB
		MergeInterval:       30 * time.Second,
		MaxEntriesSmall:     100,
		MaxEntriesLarge:     1000,
		EnableAutoMerge:     true,
		MaxMemoryPerJournal: 50 << 20,  // 50MB per journal
		MaxTotalMemory:      200 << 20, // 200MB total
		EnableMemoryLimit:   true,
	}
}

// NewJournalManager creates a new journal manager
func NewJournalManager(fs *OrcasFS, config JournalConfig) *JournalManager {
	jm := &JournalManager{
		journals: make(map[int64]*Journal),
		config:   config,
		fs:       fs,
	}

	// Start background merge worker for large files
	if config.EnableAutoMerge && config.MergeInterval > 0 {
		go jm.autoMergeWorker()
	}

	// Recover journals from WAL snapshots
	if fs.GetDataPath() != "" {
		go func() {
			if err := jm.RecoverFromWAL(); err != nil {
				DebugLog("[JournalManager] ERROR: Failed to recover from WAL: %v", err)
			}
		}()
	}

	return jm
}

// GetOrCreate gets an existing journal or creates a new one
func (jm *JournalManager) GetOrCreate(fileID, dataID, baseSize int64) *Journal {
	jm.mu.RLock()
	j, exists := jm.journals[fileID]
	jm.mu.RUnlock()

	if exists {
		return j
	}

	jm.mu.Lock()
	defer jm.mu.Unlock()

	// Double-check after acquiring write lock
	j, exists = jm.journals[fileID]
	if exists {
		return j
	}

	// Create new journal
	now := time.Now().Unix()
	j = &Journal{
		fileID:      fileID,
		dataID:      dataID,
		baseSize:    baseSize,
		entries:     make([]JournalEntry, 0, 32),
		isLargeFile: baseSize >= jm.config.SmallFileThreshold,
		threshold:   jm.config.SmallFileThreshold,
		fs:          jm.fs,
		created:     now,
		modified:    now,
	}
	atomic.StoreInt64(&j.currentSize, baseSize)
	atomic.StoreInt32(&j.isDirty, 0)

	// Initialize JournalWAL for crash recovery
	dataPath := jm.fs.GetDataPath()
	if dataPath != "" {
		walConfig := DefaultJournalWALConfig()
		walConfig.Enabled = true
		walConfig.SyncMode = "FULL" // Use FULL mode for maximum safety
		wal, err := NewJournalWAL(fileID, dataPath, walConfig)
		if err != nil {
			DebugLog("[Journal GetOrCreate] WARNING: Failed to create JournalWAL: fileID=%d, error=%v", fileID, err)
		} else {
			j.wal = wal
			DebugLog("[Journal GetOrCreate] JournalWAL initialized: fileID=%d", fileID)
		}
	}

	jm.journals[fileID] = j

	DebugLog("[Journal GetOrCreate] Created new journal: fileID=%d, dataID=%d, baseSize=%d, isLarge=%v, wal=%v",
		fileID, dataID, baseSize, j.isLargeFile, j.wal != nil)

	return j
}

// Get retrieves an existing journal
func (jm *JournalManager) Get(fileID int64) (*Journal, bool) {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	j, exists := jm.journals[fileID]
	return j, exists
}

// Remove removes a journal from the manager
func (jm *JournalManager) Remove(fileID int64) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	// Subtract memory usage before removing
	if j, exists := jm.journals[fileID]; exists {
		memUsage := atomic.LoadInt64(&j.memoryUsage)
		atomic.AddInt64(&jm.totalMemory, -memUsage)

		// Close WAL if exists
		if j.wal != nil {
			j.wal.Close()
		}

		DebugLog("[Journal Remove] Removed journal: fileID=%d, freedMemory=%d", fileID, memUsage)
	}

	delete(jm.journals, fileID)
}

// RecoverFromWAL recovers journals from WAL snapshots on startup
func (jm *JournalManager) RecoverFromWAL() error {
	dataPath := jm.fs.GetDataPath()
	if dataPath == "" {
		return nil
	}

	journalDir := filepath.Join(dataPath, "journals")

	// Check if journal directory exists
	if _, err := os.Stat(journalDir); os.IsNotExist(err) {
		DebugLog("[JournalManager RecoverFromWAL] No journal directory found, skipping recovery")
		return nil
	}

	// Scan for snapshot files
	files, err := os.ReadDir(journalDir)
	if err != nil {
		return fmt.Errorf("failed to read journal directory: %w", err)
	}

	recoveredCount := 0
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".jwal.snap") {
			continue
		}

		// Parse fileID from filename (format: <fileID>.jwal.snap)
		name := file.Name()
		fileIDStr := strings.TrimSuffix(name, ".jwal.snap")
		fileID, err := strconv.ParseInt(fileIDStr, 10, 64)
		if err != nil {
			DebugLog("[JournalManager RecoverFromWAL] WARNING: Invalid snapshot filename: %s", name)
			continue
		}

		// Create JournalWAL instance
		walConfig := DefaultJournalWALConfig()
		walConfig.Enabled = true
		jwal, err := NewJournalWAL(fileID, dataPath, walConfig)
		if err != nil {
			DebugLog("[JournalManager RecoverFromWAL] WARNING: Failed to create WAL for fileID=%d: %v", fileID, err)
			continue
		}

		// Recover journal from snapshot
		recoveredJournal, err := jwal.Recover()
		if err != nil {
			DebugLog("[JournalManager RecoverFromWAL] WARNING: Failed to recover journal for fileID=%d: %v", fileID, err)
			jwal.Close()
			continue
		}

		if recoveredJournal == nil {
			jwal.Close()
			continue
		}

		// Set the fs reference and wal
		recoveredJournal.fs = jm.fs
		recoveredJournal.wal = jwal
		recoveredJournal.threshold = jm.config.SmallFileThreshold
		recoveredJournal.isLargeFile = recoveredJournal.baseSize >= jm.config.SmallFileThreshold

		// Register the recovered journal
		jm.mu.Lock()
		jm.journals[fileID] = recoveredJournal
		jm.mu.Unlock()

		// Update memory usage
		for i := range recoveredJournal.entries {
			entryMem := int64(len(recoveredJournal.entries[i].Data)) + 16
			atomic.AddInt64(&recoveredJournal.memoryUsage, entryMem)
			atomic.AddInt64(&jm.totalMemory, entryMem)
		}

		recoveredCount++
		DebugLog("[JournalManager RecoverFromWAL] ✅ Recovered journal: fileID=%d, entries=%d, baseSize=%d",
			fileID, len(recoveredJournal.entries), recoveredJournal.baseSize)
	}

	if recoveredCount > 0 {
		DebugLog("[JournalManager RecoverFromWAL] Successfully recovered %d journals from WAL", recoveredCount)
	} else {
		DebugLog("[JournalManager RecoverFromWAL] No journals to recover")
	}

	return nil
}

// tryFreeMemory attempts to free memory by flushing journals
func (jm *JournalManager) tryFreeMemory(needed int64) {
	DebugLog("[JournalManager tryFreeMemory] Attempting to free memory: needed=%d, total=%d",
		needed, atomic.LoadInt64(&jm.totalMemory))

	jm.mu.RLock()
	defer jm.mu.RUnlock()

	// Build list of journals sorted by memory usage (largest first)
	type journalMem struct {
		journal *Journal
		memory  int64
	}

	journals := make([]journalMem, 0, len(jm.journals))
	for _, j := range jm.journals {
		mem := atomic.LoadInt64(&j.memoryUsage)
		if mem > 0 {
			journals = append(journals, journalMem{journal: j, memory: mem})
		}
	}

	// Sort by memory usage (descending)
	sort.Slice(journals, func(i, k int) bool {
		return journals[i].memory > journals[k].memory
	})

	// Flush journals until we free enough memory
	freed := int64(0)
	for _, jm := range journals {
		if freed >= needed {
			break
		}

		// Try to flush this journal
		DebugLog("[JournalManager tryFreeMemory] Flushing journal: fileID=%d, memory=%d",
			jm.journal.fileID, jm.memory)

		_, _, err := jm.journal.Flush()
		if err != nil {
			DebugLog("[JournalManager tryFreeMemory] ERROR: Failed to flush journal %d: %v",
				jm.journal.fileID, err)
			continue
		}

		freed += jm.memory
	}

	DebugLog("[JournalManager tryFreeMemory] Freed %d bytes from %d journals",
		freed, len(journals))
}

// shouldCreateSnapshot checks if a journal snapshot should be created
func (j *Journal) shouldCreateSnapshot() bool {
	cfg := j.fs.journalMgr.config

	// Don't snapshot if not dirty
	if atomic.LoadInt32(&j.isDirty) == 0 {
		return false
	}

	// Condition 1: Entry count threshold
	if len(j.entries) >= cfg.SnapshotEntryCount {
		return true
	}

	// Condition 2: Memory usage threshold
	if atomic.LoadInt64(&j.memoryUsage) >= cfg.SnapshotMemorySize {
		return true
	}

	// Condition 3: Time interval
	lastSnapshot := atomic.LoadInt64(&j.lastSnapshot)
	if lastSnapshot > 0 { // Has previous snapshot
		elapsed := time.Now().Unix() - lastSnapshot
		if elapsed >= int64(cfg.SnapshotTimeInterval.Seconds()) {
			return true
		}
	}

	return false
}

// shouldFullFlush checks if a full flush (instead of snapshot) should be performed
func (j *Journal) shouldFullFlush() bool {
	cfg := j.fs.journalMgr.config

	// Condition 1: Too many entries
	if len(j.entries) >= cfg.FullFlushTotalEntries {
		DebugLog("[Journal shouldFullFlush] Triggering full flush: entries=%d >= %d",
			len(j.entries), cfg.FullFlushTotalEntries)
		return true
	}

	// Condition 2: Too many snapshots
	snapshotCount := atomic.LoadInt32(&j.snapshotCount)
	if snapshotCount >= int32(cfg.FullFlushJournalCount) {
		DebugLog("[Journal shouldFullFlush] Triggering full flush: snapshots=%d >= %d",
			snapshotCount, cfg.FullFlushJournalCount)
		return true
	}

	return false
}

// CreateJournalSnapshot creates a lightweight journal snapshot version
func (j *Journal) CreateJournalSnapshot() (versionID int64, err error) {
	j.entriesMu.Lock()
	defer j.entriesMu.Unlock()

	if len(j.entries) == 0 {
		return 0, fmt.Errorf("no entries to snapshot")
	}

	DebugLog("[Journal CreateSnapshot] Creating snapshot: fileID=%d, entries=%d",
		j.fileID, len(j.entries))

	// Serialize and write journal data (use locked version since we already hold the lock)
	journalData, err := j.serializeJournalLocked()
	if err != nil {
		return 0, fmt.Errorf("failed to serialize journal: %w", err)
	}

	lh, ok := j.fs.h.(*core.LocalHandler)
	if !ok {
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	journalDataID := core.NewID()
	da := lh.GetDataAdapter()
	if err := da.Write(j.fs.c, j.fs.bktID, journalDataID, 0, journalData); err != nil {
		return 0, fmt.Errorf("failed to write journal data: %w", err)
	}

	// Create version object
	currentSize := atomic.LoadInt64(&j.currentSize)
	mTime := core.Now()
	// Use nanosecond timestamp for name to ensure uniqueness even if multiple snapshots
	// are created in the same second
	nameTimestamp := time.Now().UnixNano()

	// Journal should be under version object if baseVersionID exists, otherwise under file
	// This allows journal to be sorted under version objects
	journalPID := j.fileID
	if j.baseVersionID > 0 {
		journalPID = j.baseVersionID
		DebugLog("[Journal CreateSnapshot] Journal under version object: fileID=%d, baseVersionID=%d, journalPID=%d",
			j.fileID, j.baseVersionID, journalPID)
	} else {
		DebugLog("[Journal CreateSnapshot] Journal under file object: fileID=%d, baseVersionID=%d, journalPID=%d",
			j.fileID, j.baseVersionID, journalPID)
	}

	versionObj := &core.ObjectInfo{
		ID:     0,                     // Let Put() generate the ID
		PID:    journalPID,            // Journal under version object if baseVersionID exists
		Type:   core.OBJ_TYPE_JOURNAL, // Use JOURNAL type for snapshots
		DataID: j.dataID,              // References base DataID
		Size:   currentSize,
		MTime:  mTime,
		Name:   strconv.FormatInt(nameTimestamp, 10), // Use nanosecond timestamp for uniqueness
		Extra: fmt.Sprintf(`{"versionType":2,"journalDataID":%d,"baseVersionID":%d,"entryCount":%d}`,
			journalDataID, j.baseVersionID, len(j.entries)),
	}

	DebugLog("[Journal CreateSnapshot] Calling Put: PID=%d, Type=%d, DataID=%d, Size=%d, Name=%s",
		versionObj.PID, versionObj.Type, versionObj.DataID, versionObj.Size, versionObj.Name)
	ids, err := lh.Put(j.fs.c, j.fs.bktID, []*core.ObjectInfo{versionObj})
	if err != nil {
		return 0, fmt.Errorf("failed to create version: %w", err)
	}
	DebugLog("[Journal CreateSnapshot] Put returned: ids=%v, len=%d", ids, len(ids))
	if len(ids) == 0 || ids[0] == 0 {
		return 0, fmt.Errorf("failed to create version: no ID returned (ids=%v)", ids)
	}
	versionID = ids[0]
	DebugLog("[Journal CreateSnapshot] Created version object: versionID=%d", versionID)

	// Update snapshot statistics (DON'T clear entries!)
	atomic.StoreInt64(&j.lastSnapshot, time.Now().Unix())
	atomic.AddInt32(&j.snapshotCount, 1)

	// Create WAL snapshot for crash recovery
	if j.wal != nil {
		if err := j.wal.CreateSnapshot(j); err != nil {
			DebugLog("[Journal CreateSnapshot] WARNING: Failed to create WAL snapshot: fileID=%d, error=%v", j.fileID, err)
			// Don't fail the whole operation if WAL snapshot fails
		} else {
			DebugLog("[Journal CreateSnapshot] WAL snapshot created: fileID=%d", j.fileID)
		}
	}

	DebugLog("[Journal CreateSnapshot] Snapshot created: versionID=%d, journalDataID=%d, entries=%d",
		versionID, journalDataID, len(j.entries))

	// Trigger version retention cleanup for this file
	if j.fs.retentionMgr != nil {
		go func() {
			deleted := j.fs.retentionMgr.CleanupFileVersions(j.fileID)
			if deleted > 0 {
				DebugLog("[Journal CreateSnapshot] Cleaned up %d old versions for fileID=%d", deleted, j.fileID)
			}
		}()
	}

	return versionID, nil
}

// SmartFlush performs intelligent flushing based on journal state
func (j *Journal) SmartFlush() (versionID int64, err error) {
	if j.shouldFullFlush() {
		DebugLog("[Journal SmartFlush] Performing full flush for fileID=%d", j.fileID)

		// Full flush
		newDataID, newSize, err := j.Flush()
		if err != nil {
			return 0, fmt.Errorf("full flush failed: %w", err)
		}

		lh, ok := j.fs.h.(*core.LocalHandler)
		if !ok {
			return 0, fmt.Errorf("handler is not LocalHandler")
		}

		versionID = core.NewID()
		mTime := core.Now()

		// Create version object
		versionObj := &core.ObjectInfo{
			ID:     versionID,
			PID:    j.fileID,
			Type:   core.OBJ_TYPE_VERSION,
			DataID: newDataID,
			Size:   newSize,
			MTime:  mTime,
			Name:   strconv.FormatInt(mTime, 10),
			Extra:  `{"versionType":1}`,
		}

		// Get file object to update
		fileObjs, err := j.fs.h.Get(j.fs.c, j.fs.bktID, []int64{j.fileID})
		if err != nil || len(fileObjs) == 0 {
			return 0, fmt.Errorf("failed to get file object: %w", err)
		}
		fileObj := fileObjs[0]

		// Update file object with new dataID and size
		updateFileObj := &core.ObjectInfo{
			ID:     j.fileID,
			PID:    fileObj.PID,
			Type:   fileObj.Type,
			Name:   fileObj.Name,
			DataID: newDataID,
			Size:   newSize,
			MTime:  mTime,
			Mode:   fileObj.Mode,
			Extra:  fileObj.Extra,
		}

		// Batch write: version object + update file object
		objectsToPut := []*core.ObjectInfo{versionObj, updateFileObj}
		_, err = lh.Put(j.fs.c, j.fs.bktID, objectsToPut)
		if err != nil {
			return 0, fmt.Errorf("failed to create version and update file: %w", err)
		}

		// Update cache
		fileObjCache.Put(j.fileID, updateFileObj)

		atomic.StoreInt32(&j.snapshotCount, 0)
		j.baseVersionID = versionID

		DebugLog("[Journal SmartFlush] Full flush completed: versionID=%d, newDataID=%d, newSize=%d, updated file object",
			versionID, newDataID, newSize)

		// Trigger version retention cleanup for this file
		if j.fs.retentionMgr != nil {
			go func() {
				deleted := j.fs.retentionMgr.CleanupFileVersions(j.fileID)
				if deleted > 0 {
					DebugLog("[Journal SmartFlush] Cleaned up %d old versions for fileID=%d", deleted, j.fileID)
				}
			}()
		}

		return versionID, nil
	}

	if j.shouldCreateSnapshot() {
		DebugLog("[Journal SmartFlush] Creating journal snapshot for fileID=%d", j.fileID)
		return j.CreateJournalSnapshot()
	}

	return 0, nil
}

// GetMemoryStats returns memory usage statistics
func (jm *JournalManager) GetMemoryStats() (totalMemory int64, journalCount int, perJournalMem map[int64]int64) {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	totalMemory = atomic.LoadInt64(&jm.totalMemory)
	journalCount = len(jm.journals)
	perJournalMem = make(map[int64]int64, journalCount)

	for fileID, j := range jm.journals {
		perJournalMem[fileID] = atomic.LoadInt64(&j.memoryUsage)
	}

	return
}

// GetAllStats returns comprehensive statistics for all journals
func (jm *JournalManager) GetAllStats() map[string]interface{} {
	jm.mu.RLock()
	defer jm.mu.RUnlock()

	totalMemory := atomic.LoadInt64(&jm.totalMemory)
	journalCount := len(jm.journals)

	totalEntries := 0
	totalFlushes := int32(0)
	totalFlushDuration := int64(0)
	largeFileCount := 0
	sparseFileCount := 0
	dirtyCount := 0

	journalStats := make([]map[string]interface{}, 0, journalCount)

	for _, j := range jm.journals {
		stats := j.GetStats()
		journalStats = append(journalStats, stats)

		totalEntries += stats["entryCount"].(int)
		totalFlushes += stats["flushCount"].(int32)

		if j.isLargeFile {
			largeFileCount++
		}
		if j.isSparse {
			sparseFileCount++
		}
		if atomic.LoadInt32(&j.isDirty) != 0 {
			dirtyCount++
		}

		flushCount := atomic.LoadInt32(&j.flushCount)
		if flushCount > 0 {
			totalFlushDuration += atomic.LoadInt64(&j.flushDuration)
		}
	}

	avgFlushDuration := int64(0)
	if totalFlushes > 0 {
		avgFlushDuration = totalFlushDuration / int64(totalFlushes)
	}

	return map[string]interface{}{
		"totalMemory":      totalMemory,
		"journalCount":     journalCount,
		"totalEntries":     totalEntries,
		"largeFileCount":   largeFileCount,
		"sparseFileCount":  sparseFileCount,
		"dirtyCount":       dirtyCount,
		"totalFlushes":     totalFlushes,
		"avgFlushDuration": time.Duration(avgFlushDuration) * time.Millisecond,
		"journals":         journalStats,
		"config": map[string]interface{}{
			"enabled":             jm.config.Enabled,
			"smallFileThreshold":  jm.config.SmallFileThreshold,
			"maxEntriesSmall":     jm.config.MaxEntriesSmall,
			"maxEntriesLarge":     jm.config.MaxEntriesLarge,
			"enableMemoryLimit":   jm.config.EnableMemoryLimit,
			"maxMemoryPerJournal": jm.config.MaxMemoryPerJournal,
			"maxTotalMemory":      jm.config.MaxTotalMemory,
		},
	}
}

// Write adds a write entry to the journal
func (j *Journal) Write(offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	j.entriesMu.Lock()
	defer j.entriesMu.Unlock()

	// Check memory limit BEFORE adding entry
	if j.fs.journalMgr.config.EnableMemoryLimit {
		currentMem := atomic.LoadInt64(&j.memoryUsage)
		newEntrySize := int64(len(data))

		// Check per-journal memory limit
		if currentMem+newEntrySize > j.fs.journalMgr.config.MaxMemoryPerJournal {
			// Force flush before adding this entry
			DebugLog("[Journal Write] Memory limit exceeded, forcing flush: fileID=%d, current=%d, adding=%d, limit=%d",
				j.fileID, currentMem, newEntrySize, j.fs.journalMgr.config.MaxMemoryPerJournal)

			// Unlock before flush (flush needs to lock)
			j.entriesMu.Unlock()
			_, _, err := j.Flush()
			j.entriesMu.Lock()

			if err != nil {
				DebugLog("[Journal Write] ERROR: Forced flush failed: %v", err)
				return fmt.Errorf("forced flush failed due to memory limit: %w", err)
			}

			// After flush, memory should be cleared
			DebugLog("[Journal Write] Forced flush completed, continuing with write")
		}

		// Check global memory limit
		totalMem := atomic.LoadInt64(&j.fs.journalMgr.totalMemory)
		if totalMem+newEntrySize > j.fs.journalMgr.config.MaxTotalMemory {
			// Try to flush other journals first
			DebugLog("[Journal Write] Global memory limit exceeded: total=%d, adding=%d, limit=%d",
				totalMem, newEntrySize, j.fs.journalMgr.config.MaxTotalMemory)

			// Unlock and try to free memory from other journals
			j.entriesMu.Unlock()
			j.fs.journalMgr.tryFreeMemory(newEntrySize)
			j.entriesMu.Lock()
		}
	}

	// Create new entry
	entryCopy := make([]byte, len(data))
	copy(entryCopy, data)

	// Write to WAL FIRST for crash recovery
	if j.wal != nil {
		if err := j.wal.WriteEntry(offset, int64(len(data)), entryCopy); err != nil {
			DebugLog("[Journal Write] ERROR: Failed to write WAL entry: fileID=%d, error=%v", j.fileID, err)
			return fmt.Errorf("failed to write WAL entry: %w", err)
		}
	}

	entry := JournalEntry{
		Offset: offset,
		Length: int64(len(data)),
		Data:   entryCopy,
	}

	// Add to entries
	j.entries = append(j.entries, entry)

	// Update memory usage
	entryMemory := int64(len(data)) + 16 // data + overhead (offset + length)
	atomic.AddInt64(&j.memoryUsage, entryMemory)
	atomic.AddInt64(&j.fs.journalMgr.totalMemory, entryMemory)

	// Update current size if write extends the file
	endOffset := offset + int64(len(data))
	currentSize := atomic.LoadInt64(&j.currentSize)
	// For sparse files, currentSize should remain at baseSize (sparseSize)
	// Don't update currentSize for sparse files based on write operations
	if !j.isSparse && endOffset > currentSize {
		atomic.StoreInt64(&j.currentSize, endOffset)
	}

	// Mark as dirty
	atomic.StoreInt32(&j.isDirty, 1)
	atomic.StoreInt64(&j.modified, time.Now().Unix())

	// Check if snapshot is needed (asynchronous)
	if j.shouldCreateSnapshot() {
		// Create snapshot in background to avoid blocking writes
		go func() {
			if _, err := j.CreateJournalSnapshot(); err != nil {
				DebugLog("[Journal Write] WARNING: Failed to create snapshot: %v", err)
			}
		}()
	}

	// Check if merge is needed
	entryCount := len(j.entries)
	maxEntries := j.fs.journalMgr.config.MaxEntriesSmall
	if j.isLargeFile {
		maxEntries = j.fs.journalMgr.config.MaxEntriesLarge
	}

	// For small files or when entry count exceeds threshold, trigger merge
	if !j.isLargeFile || entryCount >= maxEntries {
		if entryCount >= maxEntries/2 { // Merge at 50% threshold
			j.mergeEntriesLocked()
		}
	}

	DebugLog("[Journal Write] Added entry: fileID=%d, offset=%d, length=%d, totalEntries=%d, memoryUsage=%d, currentSize=%d",
		j.fileID, offset, len(data), len(j.entries), atomic.LoadInt64(&j.memoryUsage), atomic.LoadInt64(&j.currentSize))

	return nil
}

// mergeEntriesLocked merges overlapping or adjacent entries
// Must be called with entriesMu locked
func (j *Journal) mergeEntriesLocked() {
	if len(j.entries) <= 1 {
		return
	}

	// Calculate old memory usage
	oldMemory := int64(0)
	for i := range j.entries {
		oldMemory += int64(len(j.entries[i].Data)) + 16
	}

	// Sort entries by offset
	sort.Slice(j.entries, func(i, k int) bool {
		return j.entries[i].Offset < j.entries[k].Offset
	})

	merged := make([]JournalEntry, 0, len(j.entries))
	current := j.entries[0]

	for i := 1; i < len(j.entries); i++ {
		next := j.entries[i]
		currentEnd := current.Offset + current.Length
		nextEnd := next.Offset + next.Length

		// Check if entries overlap or are adjacent
		if next.Offset <= currentEnd {
			// Merge entries
			mergeStart := current.Offset
			mergeEnd := nextEnd
			if currentEnd > nextEnd {
				mergeEnd = currentEnd
			}
			mergeLength := mergeEnd - mergeStart

			// Create merged data buffer
			mergedData := make([]byte, mergeLength)

			// Copy current data
			copy(mergedData[0:current.Length], current.Data)

			// Overlay next data
			nextOffsetInMerged := next.Offset - mergeStart
			copy(mergedData[nextOffsetInMerged:nextOffsetInMerged+next.Length], next.Data)

			current = JournalEntry{
				Offset: mergeStart,
				Length: mergeLength,
				Data:   mergedData,
			}
		} else {
			// No overlap, save current and move to next
			merged = append(merged, current)
			current = next
		}
	}

	// Add last entry
	merged = append(merged, current)

	// Calculate new memory usage
	newMemory := int64(0)
	for i := range merged {
		newMemory += int64(len(merged[i].Data)) + 16
	}

	// Update memory counters
	memoryDiff := newMemory - oldMemory
	atomic.AddInt64(&j.memoryUsage, memoryDiff)
	atomic.AddInt64(&j.fs.journalMgr.totalMemory, memoryDiff)

	// Update entries
	oldCount := len(j.entries)
	j.entries = merged
	atomic.AddInt32(&j.mergeCount, 1)
	atomic.StoreInt64(&j.lastMerge, time.Now().Unix())

	DebugLog("[Journal Merge] Merged entries: fileID=%d, before=%d, after=%d, mergeCount=%d, oldMem=%d, newMem=%d, saved=%d",
		j.fileID, oldCount, len(merged), atomic.LoadInt32(&j.mergeCount), oldMemory, newMemory, oldMemory-newMemory)
}

// MergeEntries merges overlapping or adjacent entries (public API)
func (j *Journal) MergeEntries() {
	j.entriesMu.Lock()
	defer j.entriesMu.Unlock()
	j.mergeEntriesLocked()
}

// Read reads data from the journal, applying journal entries on top of base data
func (j *Journal) Read(offset, length int64, baseReader func(offset, length int64) ([]byte, error)) ([]byte, error) {
	j.entriesMu.RLock()
	defer j.entriesMu.RUnlock()

	currentSize := atomic.LoadInt64(&j.currentSize)
	if offset >= currentSize {
		DebugLog("[Journal Read] Offset beyond current size: fileID=%d, offset=%d, currentSize=%d, isSparse=%v", 
			j.fileID, offset, currentSize, j.isSparse)
		return nil, io.EOF
	}

	// Adjust length if reading beyond current size
	if offset+length > currentSize {
		length = currentSize - offset
		DebugLog("[Journal Read] Adjusted length: fileID=%d, offset=%d, requestedLength=%d, adjustedLength=%d, currentSize=%d, isSparse=%v",
			j.fileID, offset, length, length, currentSize, j.isSparse)
	}

	// Read base data first
	baseData, err := baseReader(offset, length)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// If no base data, create zero buffer
	if baseData == nil || len(baseData) == 0 {
		baseData = make([]byte, length)
	} else if int64(len(baseData)) < length {
		// Extend base data with zeros if needed
		extended := make([]byte, length)
		copy(extended, baseData)
		baseData = extended
	}

	// Apply journal entries on top of base data
	readEnd := offset + length
	for i := range j.entries {
		entry := &j.entries[i]
		entryEnd := entry.Offset + entry.Length

		// Skip entries that don't overlap with read range
		if entry.Offset >= readEnd || entryEnd <= offset {
			continue
		}

		// Calculate overlap region
		overlapStart := entry.Offset
		if overlapStart < offset {
			overlapStart = offset
		}

		overlapEnd := entryEnd
		if overlapEnd > readEnd {
			overlapEnd = readEnd
		}

		// Copy overlapping data from entry to result
		srcOffset := overlapStart - entry.Offset
		dstOffset := overlapStart - offset
		copyLength := overlapEnd - overlapStart

		copy(baseData[dstOffset:dstOffset+copyLength], entry.Data[srcOffset:srcOffset+copyLength])
	}

	return baseData, nil
}

// Truncate truncates the journal to the specified size
func (j *Journal) Truncate(size int64) error {
	j.entriesMu.Lock()
	defer j.entriesMu.Unlock()

	oldSize := atomic.LoadInt64(&j.currentSize)
	atomic.StoreInt64(&j.currentSize, size)

	// Remove or truncate entries beyond new size
	validEntries := make([]JournalEntry, 0, len(j.entries))
	for i := range j.entries {
		entry := j.entries[i]
		entryEnd := entry.Offset + entry.Length

		if entry.Offset >= size {
			// Entry is completely beyond new size, skip it
			continue
		} else if entryEnd > size {
			// Entry needs truncation
			newLength := size - entry.Offset
			truncatedData := make([]byte, newLength)
			copy(truncatedData, entry.Data[:newLength])
			entry.Length = newLength
			entry.Data = truncatedData
		}

		validEntries = append(validEntries, entry)
	}

	j.entries = validEntries
	atomic.StoreInt32(&j.isDirty, 1)
	atomic.StoreInt64(&j.modified, time.Now().Unix())

	DebugLog("[Journal Truncate] Truncated journal: fileID=%d, oldSize=%d, newSize=%d, entries=%d",
		j.fileID, oldSize, size, len(j.entries))

	return nil
}

// GetSize returns the current size including journal modifications
func (j *Journal) GetSize() int64 {
	return atomic.LoadInt64(&j.currentSize)
}

// IsDirty returns whether the journal has uncommitted changes
func (j *Journal) IsDirty() bool {
	return atomic.LoadInt32(&j.isDirty) != 0
}

// GetEntryCount returns the number of entries in the journal
func (j *Journal) GetEntryCount() int {
	j.entriesMu.RLock()
	defer j.entriesMu.RUnlock()
	return len(j.entries)
}

// Flush flushes the journal to create a new version
// This merges all journal entries with the base data and creates a new DataID
func (j *Journal) Flush() (newDataID int64, newSize int64, err error) {
	startTime := time.Now()
	defer func() {
		if err == nil {
			duration := time.Since(startTime)
			atomic.AddInt32(&j.flushCount, 1)
			atomic.StoreInt64(&j.lastFlush, time.Now().Unix())
			atomic.AddInt64(&j.flushDuration, duration.Milliseconds())

			flushCount := atomic.LoadInt32(&j.flushCount)
			avgDuration := time.Duration(atomic.LoadInt64(&j.flushDuration)/int64(flushCount)) * time.Millisecond

			DebugLog("[Journal Flush] Performance: fileID=%d, duration=%v, avgDuration=%v, totalFlushes=%d",
				j.fileID, duration, avgDuration, flushCount)
		} else {
			// Log error on failure
			duration := time.Since(startTime)
			DebugLog("[Journal Flush] ERROR: Failed after %v: fileID=%d, error=%v", duration, j.fileID, err)
		}
	}()

	j.entriesMu.Lock()
	defer j.entriesMu.Unlock()

	if !j.IsDirty() {
		// Nothing to flush
		return j.dataID, j.baseSize, nil
	}

	// Get entry count before starting (safe since we hold the lock)
	entryCount := len(j.entries)
	DebugLog("[Journal Flush] Starting flush: fileID=%d, entries=%d, baseDataID=%d, baseSize=%d",
		j.fileID, entryCount, j.dataID, j.baseSize)

	// Merge entries first to optimize
	j.mergeEntriesLocked()

	// Calculate new size
	newSize = atomic.LoadInt64(&j.currentSize)

	// If no entries and size matches base, nothing to do
	if len(j.entries) == 0 && newSize == j.baseSize {
		atomic.StoreInt32(&j.isDirty, 0)
		return j.dataID, j.baseSize, nil
	}

	// Strategy 1: For small files, create complete new data block
	if !j.isLargeFile {
		return j.flushSmallFile(newSize)
	}

	// Strategy 2: For large files, use chunked approach with journal entries
	return j.flushLargeFile(newSize)
}

// flushSmallFile creates a complete new data block for small files
func (j *Journal) flushSmallFile(newSize int64) (int64, int64, error) {
	// Create new buffer with final data
	finalData := make([]byte, newSize)

	// Read base data if exists
	if j.dataID > 0 && j.dataID != core.EmptyDataID && j.baseSize > 0 {
		baseData, err := j.readBaseData(0, j.baseSize)
		if err != nil {
			DebugLog("[Journal flushSmallFile] ERROR: Failed to read base data: %v", err)
			return 0, 0, fmt.Errorf("failed to read base data: %w", err)
		}
		copyLen := j.baseSize
		if copyLen > newSize {
			copyLen = newSize
		}
		copy(finalData[:copyLen], baseData)
	}

	// Apply all journal entries
	for i := range j.entries {
		entry := &j.entries[i]
		if entry.Offset >= newSize {
			continue
		}

		copyLen := entry.Length
		if entry.Offset+copyLen > newSize {
			copyLen = newSize - entry.Offset
		}

		copy(finalData[entry.Offset:entry.Offset+copyLen], entry.Data[:copyLen])
	}

	// Write final data using SDK
	newDataID := core.NewID()
	if newDataID == 0 {
		return 0, 0, fmt.Errorf("failed to generate DataID")
	}

	// Get file object for naming
	fileObj, err := j.fs.h.Get(j.fs.c, j.fs.bktID, []int64{j.fileID})
	if err != nil || len(fileObj) == 0 {
		return 0, 0, fmt.Errorf("failed to get file object: %w", err)
	}

	// Write data
	lh, ok := j.fs.h.(*core.LocalHandler)
	if !ok {
		return 0, 0, fmt.Errorf("handler is not LocalHandler")
	}

	da := lh.GetDataAdapter()
	if da == nil {
		return 0, 0, fmt.Errorf("failed to get DataAdapter")
	}

	// Prepare DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: newSize,
		Kind:     core.DATA_NORMAL,
	}

	// Mark as sparse if this is a sparse file
	if j.isSparse {
		dataInfo.Kind |= core.DATA_SPARSE
		DebugLog("[Journal flushSmallFile] Marking as sparse file: fileID=%d, virtualSize=%d, actualSize=%d",
			j.fileID, j.virtualSize, newSize)
	}

	// Determine if compression/encryption is needed
	needsCompress := j.fs.CmprWay > 0 && core.ShouldCompressFileByName(fileObj[0].Name)
	needsEncrypt := j.fs.EndecWay > 0 && j.fs.EndecKey != ""

	// Set compression kind based on VFS config
	if needsCompress && j.fs.CmprWay > 0 {
		dataInfo.Kind |= j.fs.CmprWay
	}

	// Set encryption kind if needed
	if needsEncrypt && j.fs.EndecWay > 0 {
		dataInfo.Kind |= j.fs.EndecWay
	}

	// Write data in chunks
	chunkSize := j.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	sn := 0
	offset := int64(0)
	totalProcessedSize := int64(0) // Track total size after compression/encryption
	for offset < newSize {
		endOffset := offset + chunkSize
		if endOffset > newSize {
			endOffset = newSize
		}

		chunkData := finalData[offset:endOffset]
		isFirstChunk := sn == 0

		// Process chunk data (compression + encryption) using core.ProcessData
		processedChunk, err := core.ProcessData(chunkData, &dataInfo.Kind, getCmprQltyForFS(j.fs), getEndecKeyForFS(j.fs), isFirstChunk)
		if err != nil {
			DebugLog("[Journal flushSmallFile] ERROR: Failed to process chunk: fileID=%d, sn=%d, error=%v", j.fileID, sn, err)
			return 0, 0, fmt.Errorf("failed to process chunk %d: %w", sn, err)
		}

		// Write processed chunk
		if err := da.Write(j.fs.c, j.fs.bktID, newDataID, sn, processedChunk); err != nil {
			return 0, 0, fmt.Errorf("failed to write chunk %d: %w", sn, err)
		}

		// Accumulate processed size
		totalProcessedSize += int64(len(processedChunk))

		offset = endOffset
		sn++
	}

	// Set Size to actual stored size (after compression/encryption)
	// If no compression/encryption, Size should equal OrigSize
	if dataInfo.Kind&core.DATA_CMPR_MASK != 0 || dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
		dataInfo.Size = totalProcessedSize
		DebugLog("[Journal flushSmallFile] Set Size to processed size: fileID=%d, OrigSize=%d, Size=%d, Kind=0x%x",
			j.fileID, dataInfo.OrigSize, dataInfo.Size, dataInfo.Kind)
	} else {
		dataInfo.Size = dataInfo.OrigSize
		DebugLog("[Journal flushSmallFile] No compression/encryption, Size=OrigSize: fileID=%d, Size=%d",
			j.fileID, dataInfo.Size)
	}

	// Calculate and set hash values (including HdrXXH3)
	var hdrXXH3 int64
	if len(finalData) > core.DefaultHdrSize {
		hdrXXH3 = int64(xxh3.Hash(finalData[0:core.DefaultHdrSize]))
	} else {
		hdrXXH3 = int64(xxh3.Hash(finalData))
	}
	xxh3Val := int64(xxh3.Hash(finalData))
	sha256Sum := sha256.Sum256(finalData)
	dataInfo.HdrXXH3 = hdrXXH3
	dataInfo.XXH3 = xxh3Val
	dataInfo.SHA256_0 = int64(binary.BigEndian.Uint64(sha256Sum[0:8]))
	dataInfo.SHA256_1 = int64(binary.BigEndian.Uint64(sha256Sum[8:16]))
	dataInfo.SHA256_2 = int64(binary.BigEndian.Uint64(sha256Sum[16:24]))
	dataInfo.SHA256_3 = int64(binary.BigEndian.Uint64(sha256Sum[24:32]))

	// Write DataInfo
	_, err = lh.PutDataInfo(j.fs.c, j.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write DataInfo: %w", err)
	}

	// Clear memory usage before clearing entries
	oldMemory := atomic.LoadInt64(&j.memoryUsage)
	if oldMemory > 0 {
		atomic.AddInt64(&j.fs.journalMgr.totalMemory, -oldMemory)
		atomic.StoreInt64(&j.memoryUsage, 0)
		DebugLog("[Journal flushSmallFile] Cleared memory: fileID=%d, freed=%d", j.fileID, oldMemory)
	}

	// Clear journal entries and mark as clean
	j.entries = j.entries[:0]
	j.dataID = newDataID
	j.baseSize = newSize
	atomic.StoreInt32(&j.isDirty, 0)

	// Delete WAL snapshot after successful flush
	if j.wal != nil {
		if err := j.wal.DeleteSnapshot(); err != nil {
			DebugLog("[Journal flushSmallFile] WARNING: Failed to delete WAL snapshot: fileID=%d, error=%v", j.fileID, err)
		} else {
			DebugLog("[Journal flushSmallFile] WAL snapshot deleted: fileID=%d", j.fileID)
		}
	}

	DebugLog("[Journal flushSmallFile] Flushed small file: fileID=%d, newDataID=%d, newSize=%d",
		j.fileID, newDataID, newSize)

	return newDataID, newSize, nil
}

// flushLargeFile creates a new data block for large files using chunked approach
func (j *Journal) flushLargeFile(newSize int64) (int64, int64, error) {
	// Calculate modification ratio to decide strategy
	modifiedBytes := j.calculateModifiedBytes()
	modificationRatio := float64(modifiedBytes) / float64(j.baseSize)

	DebugLog("[Journal flushLargeFile] fileID=%d, baseSize=%d, modifiedBytes=%d, ratio=%.2f%%",
		j.fileID, j.baseSize, modifiedBytes, modificationRatio*100)

	// Strategy decision:
	// 1. If modification ratio < 10%: Use chunked COW flush (only modified chunks)
	// 2. If modification ratio > 50%: Use full flush (most of file changed)
	// 3. Otherwise (10-50%): Use chunked COW flush

	if modificationRatio > 0.5 {
		// More than 50% modified, full flush is more efficient
		DebugLog("[Journal flushLargeFile] Using full flush strategy (high modification ratio)")
		return j.flushSmallFile(newSize)
	}

	// Use chunked COW flush for moderate to low modifications
	DebugLog("[Journal flushLargeFile] Using chunked COW flush strategy")
	return j.flushLargeFileChunked(newSize)
}

// calculateModifiedBytes calculates total bytes modified by journal entries
func (j *Journal) calculateModifiedBytes() int64 {
	if len(j.entries) == 0 {
		return 0
	}

	// Sort entries by offset
	sortedEntries := make([]JournalEntry, len(j.entries))
	copy(sortedEntries, j.entries)
	sort.Slice(sortedEntries, func(i, k int) bool {
		return sortedEntries[i].Offset < sortedEntries[k].Offset
	})

	// Calculate total modified bytes, considering overlaps
	totalModified := int64(0)
	lastEnd := int64(-1)

	for i := range sortedEntries {
		entry := &sortedEntries[i]
		start := entry.Offset
		end := entry.Offset + entry.Length

		if start > lastEnd {
			// No overlap with previous
			totalModified += entry.Length
			lastEnd = end
		} else if end > lastEnd {
			// Partial overlap
			totalModified += end - lastEnd
			lastEnd = end
		}
		// else: completely overlapped, no additional bytes
	}

	return totalModified
}

// flushLargeFileChunked flushes large file using Copy-on-Write at chunk level
// Only modified chunks are written, unmodified chunks reference the original DataID
func (j *Journal) flushLargeFileChunked(newSize int64) (int64, int64, error) {
	startTime := time.Now()

	chunkSize := j.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// 1. Identify which chunks are modified
	modifiedChunks := j.identifyModifiedChunks(chunkSize, newSize)
	modifiedCount := len(modifiedChunks)
	totalChunks := int((newSize + chunkSize - 1) / chunkSize)

	DebugLog("[Journal flushLargeFileChunked] fileID=%d, totalChunks=%d, modifiedChunks=%d (%.1f%%)",
		j.fileID, totalChunks, modifiedCount, float64(modifiedCount)*100/float64(totalChunks))

	// 2. Generate new DataID
	newDataID := core.NewID()
	if newDataID == 0 {
		return 0, 0, fmt.Errorf("failed to generate DataID")
	}

	// Get file object for naming
	fileObj, err := j.fs.h.Get(j.fs.c, j.fs.bktID, []int64{j.fileID})
	if err != nil || len(fileObj) == 0 {
		return 0, 0, fmt.Errorf("failed to get file object: %w", err)
	}

	// Get LocalHandler
	lh, ok := j.fs.h.(*core.LocalHandler)
	if !ok {
		return 0, 0, fmt.Errorf("handler is not LocalHandler")
	}

	da := lh.GetDataAdapter()
	if da == nil {
		return 0, 0, fmt.Errorf("failed to get DataAdapter")
	}

	// 3. Process each modified chunk
	for chunkIdx := range modifiedChunks {
		chunkOffset := int64(chunkIdx) * chunkSize
		chunkLength := chunkSize
		if chunkOffset+chunkLength > newSize {
			chunkLength = newSize - chunkOffset
		}

		// Generate chunk data (read base + apply journal entries)
		chunkData, err := j.generateChunkData(chunkOffset, chunkLength)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to generate chunk %d data: %w", chunkIdx, err)
		}

		// Write chunk to new DataID
		if err := da.Write(j.fs.c, j.fs.bktID, newDataID, chunkIdx, chunkData); err != nil {
			return 0, 0, fmt.Errorf("failed to write chunk %d: %w", chunkIdx, err)
		}

		DebugLog("[Journal flushLargeFileChunked] Wrote chunk %d: offset=%d, length=%d",
			chunkIdx, chunkOffset, len(chunkData))
	}

	// 4. For unmodified chunks, copy reference from original DataID
	// This implements Copy-on-Write at chunk level
	if j.dataID > 0 && j.dataID != core.EmptyDataID {
		for chunkIdx := 0; chunkIdx < totalChunks; chunkIdx++ {
			if modifiedChunks[chunkIdx] {
				continue // Already written
			}

			chunkOffset := int64(chunkIdx) * chunkSize
			chunkLength := chunkSize
			if chunkOffset+chunkLength > j.baseSize {
				chunkLength = j.baseSize - chunkOffset
			}
			if chunkLength <= 0 {
				continue // Beyond original file size
			}

			// Read chunk from original DataID
			originalChunkData, err := j.fs.h.GetData(j.fs.c, j.fs.bktID, j.dataID, chunkIdx)
			if err != nil {
				// If can't read (sparse or missing), write zeros
				originalChunkData = make([]byte, chunkLength)
			}

			// Write to new DataID (Copy-on-Write)
			if err := da.Write(j.fs.c, j.fs.bktID, newDataID, chunkIdx, originalChunkData); err != nil {
				return 0, 0, fmt.Errorf("failed to copy chunk %d: %w", chunkIdx, err)
			}
		}
	}

	// 5. Prepare DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: newSize,
		Kind:     core.DATA_NORMAL,
	}

	// Mark as sparse if this is a sparse file
	if j.isSparse {
		dataInfo.Kind |= core.DATA_SPARSE
		DebugLog("[Journal flushLargeFileChunked] Marking as sparse file: virtualSize=%d, actualSize=%d",
			j.virtualSize, newSize)
	}

	// Determine if compression/encryption is needed
	needsCompress := j.fs.CmprWay > 0 && core.ShouldCompressFileByName(fileObj[0].Name)
	needsEncrypt := j.fs.EndecWay > 0 && j.fs.EndecKey != ""

	// Set compression kind based on VFS config
	if needsCompress && j.fs.CmprWay > 0 {
		dataInfo.Kind |= j.fs.CmprWay
	}

	// Set encryption kind if needed
	if needsEncrypt && j.fs.EndecWay > 0 {
		dataInfo.Kind |= j.fs.EndecWay
	}

	// Calculate hash for the entire file
	// Note: For chunked flush, we need to read all chunks to calculate hash
	// This is a tradeoff: COW saves write time but hash calculation still needs full read
	finalData := make([]byte, newSize)
	for chunkIdx := 0; chunkIdx < totalChunks; chunkIdx++ {
		chunkOffset := int64(chunkIdx) * chunkSize
		chunkData, err := j.fs.h.GetData(j.fs.c, j.fs.bktID, newDataID, chunkIdx)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read back chunk %d for hashing: %w", chunkIdx, err)
		}
		copy(finalData[chunkOffset:], chunkData)
	}

	dataInfo.HdrXXH3, dataInfo.XXH3, dataInfo.SHA256_0, dataInfo.SHA256_1, dataInfo.SHA256_2, dataInfo.SHA256_3 = core.CalculateChecksums(finalData)

	// Write DataInfo
	_, err = lh.PutDataInfo(j.fs.c, j.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write DataInfo: %w", err)
	}

	// Clear memory usage before clearing entries
	oldMemory := atomic.LoadInt64(&j.memoryUsage)
	if oldMemory > 0 {
		atomic.AddInt64(&j.fs.journalMgr.totalMemory, -oldMemory)
		atomic.StoreInt64(&j.memoryUsage, 0)
		DebugLog("[Journal flushLargeFileChunked] Cleared memory: fileID=%d, freed=%d", j.fileID, oldMemory)
	}

	// Clear journal entries and mark as clean
	j.entries = j.entries[:0]
	j.dataID = newDataID
	j.baseSize = newSize
	atomic.StoreInt32(&j.isDirty, 0)

	// Delete WAL snapshot after successful flush
	if j.wal != nil {
		if err := j.wal.DeleteSnapshot(); err != nil {
			DebugLog("[Journal flushLargeFileChunked] WARNING: Failed to delete WAL snapshot: fileID=%d, error=%v", j.fileID, err)
		} else {
			DebugLog("[Journal flushLargeFileChunked] WAL snapshot deleted: fileID=%d", j.fileID)
		}
	}

	duration := time.Since(startTime)
	DebugLog("[Journal flushLargeFileChunked] Completed: fileID=%d, newDataID=%d, newSize=%d, modifiedChunks=%d/%d, duration=%v",
		j.fileID, newDataID, newSize, modifiedCount, totalChunks, duration)

	return newDataID, newSize, nil
}

// identifyModifiedChunks returns a map of chunk indices that have been modified
func (j *Journal) identifyModifiedChunks(chunkSize, fileSize int64) map[int]bool {
	modifiedChunks := make(map[int]bool)

	for i := range j.entries {
		entry := &j.entries[i]

		// Calculate which chunks this entry affects
		startChunk := int(entry.Offset / chunkSize)
		endChunk := int((entry.Offset + entry.Length - 1) / chunkSize)

		// Handle edge case where entry extends beyond current file size
		maxChunk := int((fileSize - 1) / chunkSize)
		if endChunk > maxChunk {
			endChunk = maxChunk
		}

		// Mark all affected chunks as modified
		for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
			modifiedChunks[chunkIdx] = true
		}
	}

	return modifiedChunks
}

// generateChunkData generates data for a specific chunk by reading base and applying journal entries
func (j *Journal) generateChunkData(offset, length int64) ([]byte, error) {
	// 1. Initialize chunk with base data or zeros
	chunkData := make([]byte, length)

	// 2. Read base data if it exists and covers this range
	if j.dataID > 0 && j.dataID != core.EmptyDataID && offset < j.baseSize {
		readLength := length
		if offset+readLength > j.baseSize {
			readLength = j.baseSize - offset
		}

		if readLength > 0 {
			baseData, err := j.readBaseData(offset, readLength)
			if err != nil {
				DebugLog("[Journal generateChunkData] Warning: Failed to read base data at offset %d: %v", offset, err)
				// Continue with zeros
			} else {
				copy(chunkData, baseData)
			}
		}
	}

	// 3. Apply all journal entries that overlap this chunk
	chunkEnd := offset + length
	for i := range j.entries {
		entry := &j.entries[i]
		entryEnd := entry.Offset + entry.Length

		// Check if entry overlaps with this chunk
		if entry.Offset >= chunkEnd || entryEnd <= offset {
			continue // No overlap
		}

		// Calculate overlap region
		overlapStart := entry.Offset
		if overlapStart < offset {
			overlapStart = offset
		}

		overlapEnd := entryEnd
		if overlapEnd > chunkEnd {
			overlapEnd = chunkEnd
		}

		// Copy data from entry to chunk
		chunkCopyStart := overlapStart - offset
		chunkCopyEnd := overlapEnd - offset

		entryCopyStart := overlapStart - entry.Offset
		entryCopyEnd := overlapEnd - entry.Offset

		copy(chunkData[chunkCopyStart:chunkCopyEnd],
			entry.Data[entryCopyStart:entryCopyEnd])
	}

	return chunkData, nil
}

// readBaseData reads data from the base DataID
func (j *Journal) readBaseData(offset, length int64) ([]byte, error) {
	if j.dataID == 0 || j.dataID == core.EmptyDataID {
		// No base data, return zeros
		return make([]byte, length), nil
	}

	// Get DataInfo to determine chunk size and other properties
	lh, ok := j.fs.h.(*core.LocalHandler)
	if !ok {
		return nil, fmt.Errorf("handler is not LocalHandler")
	}

	dataInfo, err := lh.GetDataInfo(j.fs.c, j.fs.bktID, j.dataID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data info: %w", err)
	}

	// Calculate chunk size
	chunkSize := j.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Calculate which chunks we need to read
	startChunk := int(offset / chunkSize)
	endChunk := int((offset + length - 1) / chunkSize)

	// Read chunks and assemble result
	result := make([]byte, 0, length)
	for sn := startChunk; sn <= endChunk; sn++ {
		chunkData, err := j.fs.h.GetData(j.fs.c, j.fs.bktID, j.dataID, sn)
		if err != nil {
			// If chunk doesn't exist for sparse file, fill with zeros
			if dataInfo.Kind&core.DATA_SPARSE != 0 {
				chunkData = make([]byte, chunkSize)
			} else {
				return nil, fmt.Errorf("failed to read chunk %d: %w", sn, err)
			}
		}

		// Calculate the range within this chunk that we need
		chunkStart := int64(sn) * chunkSize
		chunkEnd := chunkStart + int64(len(chunkData))

		// Calculate the overlap with our requested range
		readStart := offset
		if readStart < chunkStart {
			readStart = chunkStart
		}

		readEnd := offset + length
		if readEnd > chunkEnd {
			readEnd = chunkEnd
		}

		if readStart < readEnd {
			// Extract the relevant portion from this chunk
			startInChunk := readStart - chunkStart
			endInChunk := readEnd - chunkStart
			result = append(result, chunkData[startInChunk:endInChunk]...)
		}
	}

	return result, nil
}

// GetStats returns statistics about the journal
func (j *Journal) GetStats() map[string]interface{} {
	j.entriesMu.RLock()
	defer j.entriesMu.RUnlock()

	flushCount := atomic.LoadInt32(&j.flushCount)
	avgDuration := int64(0)
	if flushCount > 0 {
		avgDuration = atomic.LoadInt64(&j.flushDuration) / int64(flushCount)
	}

	modifiedBytes := j.calculateModifiedBytes()
	modificationRatio := float64(0)
	if j.baseSize > 0 {
		modificationRatio = float64(modifiedBytes) / float64(j.baseSize)
	}

	return map[string]interface{}{
		"fileID":            j.fileID,
		"baseSize":          j.baseSize,
		"currentSize":       atomic.LoadInt64(&j.currentSize),
		"virtualSize":       j.virtualSize,
		"entryCount":        len(j.entries),
		"memoryUsage":       atomic.LoadInt64(&j.memoryUsage),
		"modifiedBytes":     modifiedBytes,
		"modificationRatio": modificationRatio,
		"isLargeFile":       j.isLargeFile,
		"isSparse":          j.isSparse,
		"isDirty":           atomic.LoadInt32(&j.isDirty) != 0,
		"mergeCount":        atomic.LoadInt32(&j.mergeCount),
		"flushCount":        flushCount,
		"avgFlushDuration":  time.Duration(avgDuration) * time.Millisecond,
		"created":           time.Unix(j.created, 0),
		"modified":          time.Unix(atomic.LoadInt64(&j.modified), 0),
	}
}

// Clear clears all journal entries
func (j *Journal) Clear() {
	j.entriesMu.Lock()
	defer j.entriesMu.Unlock()

	j.entries = j.entries[:0]
	atomic.StoreInt32(&j.isDirty, 0)
	atomic.StoreInt64(&j.currentSize, j.baseSize)

	DebugLog("[Journal Clear] Cleared journal: fileID=%d", j.fileID)
}

// autoMergeWorker periodically merges entries for large files
func (jm *JournalManager) autoMergeWorker() {
	ticker := time.NewTicker(jm.config.MergeInterval)
	defer ticker.Stop()

	for range ticker.C {
		jm.mu.RLock()
		journals := make([]*Journal, 0, len(jm.journals))
		for _, j := range jm.journals {
			journals = append(journals, j)
		}
		jm.mu.RUnlock()

		// Merge entries for large files
		for _, j := range journals {
			if j.isLargeFile && j.GetEntryCount() > 10 {
				j.MergeEntries()
			}
		}
	}
}

// SerializeJournal serializes journal entries to bytes for persistence
func (j *Journal) SerializeJournal() ([]byte, error) {
	j.entriesMu.RLock()
	defer j.entriesMu.RUnlock()
	return j.serializeJournalLocked()
}

// serializeJournalLocked is the internal version that doesn't acquire locks
// Caller must hold entriesMu (either RLock or Lock)
func (j *Journal) serializeJournalLocked() ([]byte, error) {
	// Calculate buffer size
	// Format: [entryCount:8][currentSize:8][entry1][entry2]...
	// Each entry: [offset:8][length:8][data]
	bufSize := 16 // Header: entryCount + currentSize
	for i := range j.entries {
		bufSize += 16 + len(j.entries[i].Data)
	}

	buf := bytes.NewBuffer(make([]byte, 0, bufSize))

	// Write header
	binary.Write(buf, binary.LittleEndian, int64(len(j.entries)))
	binary.Write(buf, binary.LittleEndian, atomic.LoadInt64(&j.currentSize))

	// Write entries
	for i := range j.entries {
		entry := &j.entries[i]
		binary.Write(buf, binary.LittleEndian, entry.Offset)
		binary.Write(buf, binary.LittleEndian, entry.Length)
		buf.Write(entry.Data)
	}

	return buf.Bytes(), nil
}

// DeserializeJournal deserializes journal entries from bytes
func (j *Journal) DeserializeJournal(data []byte) error {
	j.entriesMu.Lock()
	defer j.entriesMu.Unlock()

	buf := bytes.NewReader(data)

	// Read header
	var entryCount int64
	var currentSize int64
	if err := binary.Read(buf, binary.LittleEndian, &entryCount); err != nil {
		return fmt.Errorf("failed to read entry count: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &currentSize); err != nil {
		return fmt.Errorf("failed to read current size: %w", err)
	}

	atomic.StoreInt64(&j.currentSize, currentSize)

	// Read entries
	j.entries = make([]JournalEntry, 0, entryCount)
	for i := int64(0); i < entryCount; i++ {
		var offset, length int64
		if err := binary.Read(buf, binary.LittleEndian, &offset); err != nil {
			return fmt.Errorf("failed to read entry offset: %w", err)
		}
		if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
			return fmt.Errorf("failed to read entry length: %w", err)
		}

		entryData := make([]byte, length)
		if _, err := io.ReadFull(buf, entryData); err != nil {
			return fmt.Errorf("failed to read entry data: %w", err)
		}

		j.entries = append(j.entries, JournalEntry{
			Offset: offset,
			Length: length,
			Data:   entryData,
		})
	}

	DebugLog("[Journal Deserialize] Loaded journal: fileID=%d, entries=%d, size=%d",
		j.fileID, len(j.entries), currentSize)

	return nil
}
