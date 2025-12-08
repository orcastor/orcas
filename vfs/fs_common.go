// Package vfs provides ORCAS filesystem implementation, supporting FUSE mounting and random access API
package vfs

import (
	"fmt"
	"os"
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
	h          core.Handler
	c          core.Ctx
	bktID      int64
	root       *OrcasNode
	bucket     *core.BucketInfo // Bucket configuration (includes compression/encryption settings)
	chunkSize  int64
	requireKey bool // If true, return EPERM error when KEY is not provided
	// Batch writer is now managed globally by SDK, accessed via GetBatchWriterForBucket
	raRegistry sync.Map // map[fileID]*RandomAccessor
	// Mutex to protect RandomAccessor creation for .tmp files during concurrent writes
	raCreateMu sync.Mutex // Protects creation of RandomAccessor for same fileID
}

// NewOrcasFS creates a new ORCAS filesystem
// This function is available on all platforms
// requireKey: if true, return EPERM error when KEY is not provided in context
func NewOrcasFS(h core.Handler, c core.Ctx, bktID int64, requireKey ...bool) *OrcasFS {
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

	// Register bucket config to Handler for ConvertWritingVersions job
	// This allows the scheduled job to access compression/encryption settings
	if bucket != nil {
		if lh, ok := h.(*core.LocalHandler); ok {
			lh.SetBucketConfig(bktID, bucket)
		}
	}

	// Check if requireKey option is provided
	reqKey := false
	if len(requireKey) > 0 {
		reqKey = requireKey[0]
	}

	ofs := &OrcasFS{
		h:          h,
		c:          c,
		bktID:      bktID,
		bucket:     bucket,
		chunkSize:  chunkSize,
		requireKey: reqKey,
	}

	// Root node initialization
	// Windows platform needs to initialize root node immediately, as it doesn't rely on FUSE (implemented in fs_win.go)
	// Other platforms' root nodes are initialized during Mount through FUSE's Inode system (implemented in fs.go)
	// initRootNode method is implemented in platform-specific files (fs_win.go or fs.go)
	ofs.initRootNode()

	return ofs
}

// getBucketConfig gets bucket configuration, refreshing from database if needed
func (fs *OrcasFS) getBucketConfig() *core.BucketInfo {
	if fs.bucket != nil {
		return fs.bucket
	}
	// Refresh bucket config from database
	bucket, err := fs.h.GetBktInfo(fs.c, fs.bktID)
	if err == nil && bucket != nil {
		fs.bucket = bucket
		return bucket
	}
	// Return default config if bucket not found
	return &core.BucketInfo{
		ID:        fs.bktID,
		ChunkSize: fs.chunkSize,
	}
}

// checkKey checks if KEY is required and present in context
// Returns EPERM if requireKey is true but KEY is not provided
func (fs *OrcasFS) checkKey() syscall.Errno {
	if !fs.requireKey {
		return 0 // Key not required
	}
	// Check if KEY exists in context
	// Use core.getKey function to extract key from context
	if v, ok := fs.c.Value("o").(map[string]interface{}); ok {
		if key, okk := v["key"].(string); okk && key != "" {
			return 0 // Key is present
		}
	}
	// Key is required but not provided
	return syscall.EPERM
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
