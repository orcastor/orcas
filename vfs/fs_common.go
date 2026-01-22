// Package vfs provides ORCAS filesystem implementation, supporting FUSE mounting and random access API
package vfs

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/orcastor/orcas/core"
)

// debugEnabled is a global flag to control debug logging
// 0 = disabled, 1 = enabled
var debugEnabled int32

// init initializes debug mode from environment variable (for backward compatibility)
func init() {
	// Check ORCAS_DEBUG environment variable at startup
	if os.Getenv("ORCAS_DEBUG") != "" && os.Getenv("ORCAS_DEBUG") != "0" {
		atomic.StoreInt32(&debugEnabled, 1)
	}
}

// SetDebugEnabled sets the debug mode (can be called from cmd/main.go)
func SetDebugEnabled(enabled bool) {
	if enabled {
		atomic.StoreInt32(&debugEnabled, 1)
	} else {
		atomic.StoreInt32(&debugEnabled, 0)
	}
}

// IsDebugEnabled returns whether debug mode is enabled
func IsDebugEnabled() bool {
	return atomic.LoadInt32(&debugEnabled) == 1
}

// DebugLog logs debug messages with timestamp
// Debug mode can be controlled by:
// 1. SetDebugEnabled() function (from cmd/main.go -debug parameter)
// 2. ORCAS_DEBUG environment variable (initialized in init() for backward compatibility)
func DebugLog(format string, args ...interface{}) {
	// Check if debug is enabled (environment variable is checked once in init())
	if !IsDebugEnabled() {
		return
	}

	// Get current timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Format message with timestamp
	message := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] [VFS DEBUG] %s\n", timestamp, message)
}

// OrcasFS implements ORCAS filesystem, mapping ORCAS object storage to filesystem
// This struct is available on all platforms
type OrcasFS struct {
	h           core.Handler
	c           core.Ctx
	bktID       int64
	root        *OrcasNode
	core.Config // Embedded Config: compression, encryption, instant upload settings
	chunkSize   int64
	requireKey  bool // If true, return EPERM error when EndecKey is not provided
	// Business layer configuration (from MountOptions.Config, not from bucket config)
	// These fields are NOT stored in database, handled at business layer
	raRegistry sync.Map // map[fileID]*RandomAccessor
	// Mutex to protect RandomAccessor creation for .tmp files during concurrent writes
	raCreateMu sync.Mutex // Protects creation of RandomAccessor for same fileID
	// WAL checkpoint manager for periodic WAL flushing
	walCheckpointManager *WALCheckpointManager
	// Journal manager for tracking random writes
	journalMgr *JournalManager
	// Atomic replace manager for atomic replace adaptation
	atomicReplaceMgr *AtomicReplaceMgr
	// Version retention manager for version cleanup
	retentionMgr *VersionRetentionManager
	// OnRootDeleted is called when the root node is deleted (entire bucket is deleted)
	// This callback can be used to perform cleanup operations
	// Note: If you need to unmount in the callback, use fs.Server.Unmount()
	// The server field is set automatically when Mount() is called
	OnRootDeleted func(fs *OrcasFS)
	// Server is the FUSE server instance (set automatically by Mount method)
	// Can be used in OnRootDeleted callback to unmount the filesystem
	Server interface {
		Unmount() error
		Wait()
		Serve()
	}
	// GetFallbackFiles: Callback function to get fallback files.
	// When RequireKey is true and no key is provided, these files will be shown as a fallback.
	// Returns a map where key is the file name and value is the file content.
	GetFallbackFiles func() map[string]string
	// KeyFileNameFilter is called when Create/Open wants to proceed while key check failed
	// (RequireKey enabled but no key provided). Return 0 to allow; non-zero errno to reject.
	KeyFileNameFilter func(fileName string) syscall.Errno
	// OnKeyFileContent is called when Write happens while key check failed
	// (RequireKey enabled but no key provided). Return 0 to allow; non-zero errno to reject.
	OnKeyFileContent func(fileName, key string) syscall.Errno

	keyContent string

	// noKeyTemp: in-memory temp files created when key check failed. Never persisted to DB.
	noKeyTempMu     sync.RWMutex
	noKeyTempByID   map[int64]*noKeyTempFile
	noKeyTempByName map[string]int64

	// MEMORY LEAK FIX: Track raRegistry cleanup state
	raCleanupStopped atomic.Bool // Whether raRegistry cleanup worker has stopped
}

type noKeyTempFile struct {
	id    int64
	name  string
	data  []byte
	mtime int64
}

// NewOrcasFS creates a new ORCAS filesystem
// This function is available on all platforms
// requireKey: if true, return EPERM error when EndecKey is not provided in Config
//
// IMPORTANT: VFS requires LocalHandler for full functionality:
// - TempFileWriter (for .tmp files and large files) requires LocalHandler
// - Random writes with temp write area require LocalHandler
// - Direct data updates require LocalHandler
// If a non-LocalHandler is provided, some features may not work (e.g., .tmp file writes)
func NewOrcasFS(h core.Handler, c core.Ctx, bktID int64, requireKey ...bool) *OrcasFS {
	return NewOrcasFSWithConfig(h, c, bktID, nil, requireKey...)
}

// NewOrcasFSWithConfig creates a new ORCAS filesystem with full configuration
// This function is available on all platforms
// cfg: Configuration from core.Config (paths, compression, encryption, instant upload settings)
//
//	If nil, uses default configuration (no compression, no encryption, instant upload OFF)
//
// requireKey: if true, return EPERM error when EndecKey is not provided in Config
func NewOrcasFSWithConfig(h core.Handler, c core.Ctx, bktID int64, cfg *core.Config, requireKey ...bool) *OrcasFS {
	// Get bucket configuration (includes chunkSize, compression, encryption settings)
	var chunkSize int64
	bucket, err := h.GetBktInfo(c, bktID)
	if err == nil && bucket != nil {
		if bucket.ChunkSize > 0 {
			chunkSize = bucket.ChunkSize
		}
	} else {
		// If GetBktInfo fails, create a default bucket info
		bucket = &core.BucketInfo{
			ID:        bktID,
			ChunkSize: chunkSize,
		}
	}

	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Check if requireKey option is provided
	reqKey := false
	if len(requireKey) > 0 {
		reqKey = requireKey[0]
	}

	// Initialize Config (embed in OrcasFS)
	var config core.Config
	if cfg != nil {
		config = *cfg
	}

	// Verify handler type and log warning if not LocalHandler
	// VFS requires LocalHandler for full functionality (TempFileWriter, temp write area, etc.)
	if _, ok := h.(*core.LocalHandler); !ok {
		handlerType := fmt.Sprintf("%T", h)
		DebugLog("[VFS NewOrcasFSWithConfig] WARNING: handler is not LocalHandler (type: %s), some features may not work: bktID=%d", handlerType, bktID)
		DebugLog("[VFS NewOrcasFSWithConfig] NOTE: TempFileWriter and temp write area require LocalHandler")
	}

	ofs := &OrcasFS{
		h:          h,
		c:          c,
		bktID:      bktID,
		chunkSize:  chunkSize,
		requireKey: reqKey,
		Config:     config,
		// init noKeyTemp with very negative IDs to avoid collisions
		noKeyTempByID:   make(map[int64]*noKeyTempFile),
		noKeyTempByName: make(map[string]int64),
	}

	// Initialize journal manager
	journalConfig := DefaultJournalConfig()
	// Sync threshold with temp write area
	if journalConfig.SmallFileThreshold == 0 {
		journalConfig.SmallFileThreshold = 10 << 20 // 10MB default
	}
	ofs.journalMgr = NewJournalManager(ofs, journalConfig)
	DebugLog("[VFS NewOrcasFSWithConfig] Journal manager initialized: threshold=%d", journalConfig.SmallFileThreshold)

	// Initialize deletion manager for atomic replace adaptation
	ofs.atomicReplaceMgr = NewAtomicReplaceMgr(ofs, 5*time.Second)
	DebugLog("[VFS NewOrcasFSWithConfig] Deletion manager initialized: delay=5s")

	// Initialize version retention manager
	retentionPolicy := DefaultVersionRetentionPolicy()
	ofs.retentionMgr = NewVersionRetentionManager(ofs, retentionPolicy)
	DebugLog("[VFS NewOrcasFSWithConfig] Version retention manager initialized: enabled=%v",
		retentionPolicy.Enabled)

	// MEMORY LEAK FIX: Start RandomAccessor cleanup worker
	// This prevents memory leak from inactive RandomAccessors accumulating in raRegistry
	go ofs.raRegistryCleanupWorker()

	// Initialize WAL checkpoint manager
	// This periodically flushes WAL to main database to reduce dirty read issues
	if config.DataPath != "" {
		walConfig := DefaultWALCheckpointConfig()
		walConfig.CheckpointInterval = 300 * time.Second // 每300秒刷新一次
		walCheckpointManager := NewWALCheckpointManager(config.DataPath, walConfig)
		if err := walCheckpointManager.Start(); err != nil {
			DebugLog("[VFS NewOrcasFSWithConfig] WARNING: Failed to start WAL checkpoint manager: %v", err)
		} else {
			ofs.walCheckpointManager = walCheckpointManager
			DebugLog("[VFS NewOrcasFSWithConfig] WAL checkpoint manager started: interval=%v, dataPath=%s",
				walConfig.CheckpointInterval, config.DataPath)
		}
	} else {
		DebugLog("[VFS NewOrcasFSWithConfig] Skipping WAL checkpoint manager: DataPath is empty")
	}

	// Root node initialization
	// Windows platform needs to initialize root node immediately, as it doesn't rely on FUSE (implemented in fs_win.go)
	// Other platforms' root nodes are initialized during Mount through FUSE's Inode system (implemented in fs.go)
	// initRootNode method is implemented in platform-specific files (fs_win.go or fs.go)
	ofs.initRootNode()

	// TODO: If custom paths are specified, create custom adapters that use these paths
	// and replace the adapters in LocalHandler using SetAdapter method
	// This requires creating custom DataAdapter and MetadataAdapter implementations
	// that use ofs.Config.BasePath and ofs.Config.DataPath instead of global ORCAS_BASE and ORCAS_DATA

	return ofs
}

func (fs *OrcasFS) Root() *OrcasNode {
	if fs == nil {
		return nil
	}
	return fs.root
}

// Close stops all background managers and cleans up resources
// This should be called when the filesystem is no longer needed
func (fs *OrcasFS) Close() error {
	if fs == nil {
		return nil
	}

	// MEMORY LEAK FIX: Stop raRegistry cleanup worker
	fs.raCleanupStopped.Store(true)

	// Stop WAL checkpoint manager
	if fs.walCheckpointManager != nil {
		fs.walCheckpointManager.Stop()
		fs.walCheckpointManager = nil
	}

	// Close all RandomAccessors
	fs.raRegistry.Range(func(key, value interface{}) bool {
		if ra, ok := value.(*RandomAccessor); ok {
			ra.Close()
		}
		return true
	})

	return nil
}

// GetBasePath returns the base path for metadata (database storage location)
// If empty, the global ORCAS_BASE environment variable should be used
func (fs *OrcasFS) GetBasePath() string {
	if fs == nil {
		return ""
	}
	return fs.Config.BasePath
}

// GetDataPath returns the data path for file data storage location
// If empty, the global ORCAS_DATA environment variable should be used
func (fs *OrcasFS) GetDataPath() string {
	if fs == nil {
		return ""
	}
	return fs.Config.DataPath
}

// GetEndecKey returns the encryption key for data encryption/decryption
// If empty, encryption key is not used
func (fs *OrcasFS) GetEndecKey() string {
	if fs == nil {
		return ""
	}
	return fs.Config.EndecKey
}

// SetEndecKey dynamically sets the encryption key for data encryption/decryption
// This allows changing the encryption key at runtime without recreating the filesystem
// Note: This method is not thread-safe. If concurrent access is needed, external synchronization is required.
func (fs *OrcasFS) SetEndecKey(key string) {
	if fs == nil {
		return
	}
	fs.Config.EndecKey = key
	// When key is set, clear all no-key temporary in-memory files.
	if key != "" {
		fs.noKeyTempMu.Lock()
		fs.noKeyTempByID = make(map[int64]*noKeyTempFile)
		fs.noKeyTempByName = make(map[string]int64)
		fs.noKeyTempMu.Unlock()
		fs.keyContent = ""
	}
	fs.root.invalidateDirListCache(fs.bktID)
}

// getEndecKeyForFS returns the encryption key for a given OrcasFS
// Returns OrcasFS.Config.EndecKey if set, otherwise empty string
// Key should be provided via MountOptions.Config, not from bucket config
func getEndecKeyForFS(fs *OrcasFS) string {
	if fs == nil {
		return ""
	}
	return fs.Config.EndecKey
}

// getCmprWayForFS returns the compression method for a given OrcasFS
// Returns OrcasFS.Config.CmprWay if set, otherwise 0
// Should be provided via MountOptions.Config, not from bucket config
func getCmprWayForFS(fs *OrcasFS) uint32 {
	if fs == nil {
		return 0
	}
	return fs.Config.CmprWay
}

// getCmprQltyForFS returns the compression quality for a given OrcasFS
// Returns OrcasFS.Config.CmprQlty if set, otherwise 0
// Should be provided via MountOptions.Config, not from bucket config
func getCmprQltyForFS(fs *OrcasFS) uint32 {
	if fs == nil {
		return 0
	}
	return fs.Config.CmprQlty
}

// getEndecWayForFS returns the encryption method for a given OrcasFS
// Returns OrcasFS.Config.EndecWay if set, otherwise 0
// Should be provided via MountOptions.Config, not from bucket config
func getEndecWayForFS(fs *OrcasFS) uint32 {
	if fs == nil {
		return 0
	}
	return fs.Config.EndecWay
}

// getRefLevelForFS returns the instant upload level for a given OrcasFS
// Returns OrcasFS.Config.RefLevel if set, otherwise 0
// Should be provided via MountOptions.Config, not from bucket config
func getRefLevelForFS(fs *OrcasFS) uint32 {
	if fs == nil {
		return 0
	}
	return fs.Config.RefLevel
}

// getBucketConfig gets bucket configuration, refreshing from database if needed
func (fs *OrcasFS) getBucketConfig() *core.BucketInfo {
	// Always refresh from database to ensure we have the latest config
	// This is important for tests where bucket config may be updated after OrcasFS creation
	bucket, err := fs.h.GetBktInfo(fs.c, fs.bktID)
	if err == nil && bucket != nil {
		return bucket
	}
	// Return default config if bucket not found
	return &core.BucketInfo{
		ID:        fs.bktID,
		ChunkSize: fs.chunkSize,
	}
}

// checkKey checks if KEY is required and present in OrcasFS.Config.EndecKey
// Returns EPERM if requireKey is true but KEY is not provided (and no fallback files)
// Returns 0 if key is present or if fallback files are available
// Note: This should not happen if mount-time validation is working correctly,
// but we keep this check as a safety measure
func (fs *OrcasFS) checkKey(dontUseFallback ...bool) syscall.Errno {
	if !fs.requireKey {
		return 0 // Key not required
	}
	// Check if EndecKey exists in OrcasFS.Config
	if fs.Config.EndecKey != "" {
		return 0 // Key is present
	}
	// Key is required but not provided
	// If fallback files are configured, allow read-only access to them
	if fs.GetFallbackFiles != nil {
		if len(dontUseFallback) > 0 && dontUseFallback[0] {
			return syscall.EPERM
		}
		files := fs.GetFallbackFiles()
		if len(files) > 0 {
			// DebugLog("[VFS checkKey] RequireKey is enabled but EndecKey is not provided, using fallback files")
			return 0 // Allow access to fallback files
		}
	}
	// Key is required but not provided and no fallback files
	// This should have been caught at mount time, but return EPERM as fallback
	DebugLog("[VFS checkKey] ERROR: RequireKey is enabled but EndecKey is not provided")
	return syscall.EPERM
}

func (fs *OrcasFS) noKeyTempGetByID(id int64) (*noKeyTempFile, bool) {
	fs.noKeyTempMu.RLock()
	defer fs.noKeyTempMu.RUnlock()
	f, ok := fs.noKeyTempByID[id]
	return f, ok
}

func (fs *OrcasFS) noKeyTempGetIDByName(name string) (int64, bool) {
	fs.noKeyTempMu.RLock()
	defer fs.noKeyTempMu.RUnlock()
	id, ok := fs.noKeyTempByName[name]
	return id, ok
}

func (fs *OrcasFS) noKeyTempList() map[string]int64 {
	fs.noKeyTempMu.RLock()
	defer fs.noKeyTempMu.RUnlock()
	out := make(map[string]int64, len(fs.noKeyTempByName))
	for k, v := range fs.noKeyTempByName {
		out[k] = v
	}
	return out
}

func (fs *OrcasFS) noKeyTempCreate(name string) *noKeyTempFile {
	fs.noKeyTempMu.Lock()
	defer fs.noKeyTempMu.Unlock()
	if existingID, ok := fs.noKeyTempByName[name]; ok {
		if f, ok2 := fs.noKeyTempByID[existingID]; ok2 && f != nil {
			return f
		}
	}
	// Base ID derived from filename, consistent with fallback IDs
	id := core.NewID()
	f := &noKeyTempFile{
		id:    id,
		name:  name,
		data:  nil,
		mtime: core.Now(),
	}
	fs.noKeyTempByID[id] = f
	fs.noKeyTempByName[name] = id
	return f
}

// noKeyTempDelete deletes a noKeyTemp file by name
func (fs *OrcasFS) noKeyTempDelete(name string) {
	fs.noKeyTempMu.Lock()
	defer fs.noKeyTempMu.Unlock()
	if id, ok := fs.noKeyTempByName[name]; ok {
		delete(fs.noKeyTempByID, id)
		delete(fs.noKeyTempByName, name)
	}
}

// shouldUseFallbackFiles checks if we should use fallback files
// Returns true if RequireKey is true, no key is provided, and fallback files are configured
func (fs *OrcasFS) shouldUseFallbackFiles() bool {
	if !fs.requireKey {
		return false
	}
	if fs.Config.EndecKey != "" {
		return false
	}
	if fs.GetFallbackFiles == nil {
		return false
	}
	files := fs.GetFallbackFiles()
	return len(files) > 0
}

// registerRandomAccessor tracks active RandomAccessor instances for a file
// IMPORTANT: For .tmp files, we must ensure only one RandomAccessor with TempFileWriter is registered
// to avoid creating multiple TempFileWriter instances with different dataIDs during concurrent writes
// This function should be called while holding raCreateMu lock for .tmp files
func (fs *OrcasFS) registerRandomAccessor(fileID int64, ra *RandomAccessor) {
	if fs == nil || ra == nil {
		return
	}

	// Check if there's already a registered RandomAccessor
	if existing, ok := fs.raRegistry.Load(fileID); ok {
		if current, ok := existing.(*RandomAccessor); ok && current != nil {
			// Preserve existing writer accessor so .tmp flushes keep working
			if current == ra {
				return
			}
			// If existing RandomAccessor has TempFileWriter, don't replace it with a new one
			// This ensures all concurrent writers use the same TempFileWriter
			if current.hasTempFileWriter() {
				if !ra.hasTempFileWriter() {
					DebugLog("[VFS RA Registry] Skipping register of non-writer RA: fileID=%d, ra=%p, existing=%p (has TempFileWriter)", fileID, ra, current)
					return
				} else {
					// Both have TempFileWriter, keep the existing one (first one wins)
					DebugLog("[VFS RA Registry] Skipping register of RA with TempFileWriter (existing one already registered): fileID=%d, ra=%p, existing=%p", fileID, ra, current)
					return
				}
			}
		}
	}

	// Register the RandomAccessor
	fs.raRegistry.Store(fileID, ra)
	DebugLog("[VFS RA Registry] Registered RandomAccessor: fileID=%d, ra=%p", fileID, ra)
}

// unregisterRandomAccessor removes RandomAccessor tracking if it matches current entry
func (fs *OrcasFS) unregisterRandomAccessor(fileID int64, ra *RandomAccessor) {
	if fs == nil || ra == nil {
		return
	}
	if current, ok := fs.raRegistry.Load(fileID); ok {
		if current == ra {
			fs.raRegistry.Delete(fileID)
			DebugLog("[VFS RA Registry] Unregistered RandomAccessor: fileID=%d, ra=%p", fileID, ra)
		}
	}
}

// getRandomAccessorByFileID returns tracked RandomAccessor for given file ID (if any)
func (fs *OrcasFS) getRandomAccessorByFileID(fileID int64) *RandomAccessor {
	if fs == nil {
		return nil
	}
	if val, ok := fs.raRegistry.Load(fileID); ok {
		if ra, ok2 := val.(*RandomAccessor); ok2 && ra != nil {
			return ra
		}
	}
	return nil
}

// MEMORY LEAK FIX: raRegistryCleanupWorker periodically cleans up inactive RandomAccessors
// This prevents memory leak from RandomAccessors that are never properly released
func (fs *OrcasFS) raRegistryCleanupWorker() {
	// Run cleanup every 5 minutes
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	// Initial cleanup after 1 minute
	time.Sleep(1 * time.Minute)

	for {
		select {
		case <-ticker.C:
			fs.cleanupInactiveRandomAccessors()
		default:
			// Check if we should stop
			if fs.raCleanupStopped.Load() {
				DebugLog("[VFS RA Registry] Cleanup worker stopped")
				return
			}
			time.Sleep(10 * time.Second)
		}
	}
}

// cleanupInactiveRandomAccessors removes RandomAccessors that have been inactive for too long
// MEMORY LEAK FIX: Prevents accumulation of orphaned RandomAccessors in raRegistry
func (fs *OrcasFS) cleanupInactiveRandomAccessors() {
	if fs == nil {
		return
	}

	const inactivityThreshold = 10 * time.Minute // Remove RandomAccessors inactive for 10+ minutes
	now := time.Now().Unix()

	cleanedCount := 0
	totalBefore := 0

	// Collect inactive RandomAccessors
	var inactiveFileIDs []int64
	fs.raRegistry.Range(func(key, value interface{}) bool {
		totalBefore++
		fileID, ok := key.(int64)
		if !ok {
			return true
		}
		ra, ok := value.(*RandomAccessor)
		if !ok || ra == nil {
			// Invalid entry, remove it
			inactiveFileIDs = append(inactiveFileIDs, fileID)
			return true
		}

		// Check last activity time (atomic load from RandomAccessor.lastActivity)
		lastActivity := atomic.LoadInt64(&ra.lastActivity)
		inactiveDuration := time.Duration(now-lastActivity) * time.Second

		if inactiveDuration >= inactivityThreshold {
			DebugLog("[VFS RA Registry] Found inactive RandomAccessor: fileID=%d, inactiveDuration=%v, lastActivity=%d",
				fileID, inactiveDuration, lastActivity)
			inactiveFileIDs = append(inactiveFileIDs, fileID)
		}
		return true
	})

	// Remove inactive RandomAccessors
	for _, fileID := range inactiveFileIDs {
		if val, ok := fs.raRegistry.Load(fileID); ok {
			if ra, ok := val.(*RandomAccessor); ok && ra != nil {
				// Try to close the RandomAccessor to release resources
				if err := ra.Close(); err != nil {
					DebugLog("[VFS RA Registry] WARNING: Failed to close inactive RandomAccessor: fileID=%d, error=%v",
						fileID, err)
				}
			}
		}
		fs.raRegistry.Delete(fileID)
		cleanedCount++
	}

	if cleanedCount > 0 {
		DebugLog("[VFS RA Registry] Cleanup completed: total=%d, cleaned=%d, remaining=%d",
			totalBefore, cleanedCount, totalBefore-cleanedCount)

		// MEMORY LEAK FIX: Force GC after cleaning up multiple RandomAccessors
		// This helps reclaim memory from closed RandomAccessors and their chunks
		if cleanedCount >= 5 {
			DebugLog("[VFS RA Registry] Forcing GC after cleanup of %d RandomAccessors", cleanedCount)
			runtime.GC()
		}
	}
}

// listAllObjects paginates through Handler.List until the requested count is reached or all pages are fetched.
//
// Count semantics:
// - opt.Count > 0:  target total count to fetch
//   - If Count < DefaultListPageSize: use Count as page size (single page)
//   - If Count >= DefaultListPageSize: use DefaultListPageSize as page size (multiple pages)
//
// - opt.Count == -1: fetch ALL pages (use DefaultListPageSize as page size, no limit on total count)
// - opt.Count == 0:  treated as "unspecified", fetch ALL pages (use DefaultListPageSize as page size)
func listAllObjects(ctx core.Ctx, h core.Handler, bktID, pid int64, opt core.ListOptions) ([]*core.ObjectInfo, error) {
	targetCount := opt.Count

	// Normalize target count
	if targetCount <= 0 {
		// Fetch all pages: mark as "fetch all"
		targetCount = -1
	}

	pageOpt := opt
	pageOpt.Delim = "" // Start from first page

	var all []*core.ObjectInfo

	for {
		// Determine page size for this request
		if targetCount > 0 {
			// Calculate remaining count needed
			remaining := targetCount - len(all)
			if remaining <= 0 {
				// Already got enough, trim to exact target count if exceeded
				if len(all) > targetCount {
					all = all[:targetCount]
				}
				break
			}
			// Use remaining count if less than DefaultListPageSize, otherwise use DefaultListPageSize
			if remaining < core.DefaultListPageSize {
				pageOpt.Count = remaining
			} else {
				pageOpt.Count = core.DefaultListPageSize
			}
		} else {
			// Fetch all pages: always use DefaultListPageSize
			pageOpt.Count = core.DefaultListPageSize
		}

		objs, _, delim, err := h.List(ctx, bktID, pid, pageOpt)
		if err != nil {
			return nil, err
		}

		all = append(all, objs...)

		// Check if we've reached the target count
		if targetCount > 0 && len(all) >= targetCount {
			// Trim to exact target count if we exceeded it
			if len(all) > targetCount {
				all = all[:targetCount]
			}
			break
		}

		// No more pages
		if delim == "" || len(objs) == 0 {
			break
		}

		// Continue with next page using returned delimiter
		pageOpt.Delim = delim
	}

	return all, nil
}

// listAllObjectsForFS is a convenience wrapper bound to OrcasFS fields.
func (fs *OrcasFS) listAllObjects(pid int64, opt core.ListOptions) ([]*core.ObjectInfo, error) {
	return listAllObjects(fs.c, fs.h, fs.bktID, pid, opt)
}
