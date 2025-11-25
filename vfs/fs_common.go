// Package vfs provides ORCAS filesystem implementation, supporting FUSE mounting and random access API
package vfs

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

// debugEnabled is a global flag to control debug logging
// 0 = disabled, 1 = enabled
var debugEnabled int32

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
// 2. ORCAS_DEBUG environment variable (for backward compatibility)
func DebugLog(format string, args ...interface{}) {
	// Check if debug is enabled via SetDebugEnabled() or environment variable
	enabled := IsDebugEnabled() || os.Getenv("ORCAS_DEBUG") != "0"
	if !enabled {
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
	h         core.Handler
	c         core.Ctx
	bktID     int64
	root      *OrcasNode
	bucket    *core.BucketInfo // Bucket configuration (includes compression/encryption settings)
	chunkSize int64
	// Batch writer is now managed globally by SDK, accessed via GetBatchWriterForBucket
}

// NewOrcasFS creates a new ORCAS filesystem
// This function is available on all platforms
// sdkCfg parameter is deprecated, configuration is now read from bucket
func NewOrcasFS(h core.Handler, c core.Ctx, bktID int64, sdkCfg *sdk.Config) *OrcasFS {
	// Get bucket configuration (includes chunkSize, compression, encryption settings)
	var chunkSize int64 = 10 << 20 // Default 10MB
	bucket, err := h.GetBktInfo(c, bktID)
	if err == nil && bucket != nil {
		if bucket.ChunkSize > 0 {
			chunkSize = bucket.ChunkSize
		}
		// If bucket config is empty but sdkCfg is provided, migrate config to bucket
		// This is for backward compatibility during migration
		if sdkCfg != nil && bucket.CmprWay == 0 && bucket.EndecWay == 0 {
			// Note: This migration should be done at bucket creation time, not here
			// But we keep it for backward compatibility
		}
	} else {
		// If GetBktInfo fails, create a default bucket info
		bucket = &core.BucketInfo{
			ID:        bktID,
			ChunkSize: chunkSize,
		}
	}

	// Register bucket config to Handler for ConvertWritingVersions job
	// This allows the scheduled job to access compression/encryption settings
	if bucket != nil {
		if lh, ok := h.(*core.LocalHandler); ok {
			lh.SetBucketConfig(bktID, bucket)
		}
	}

	ofs := &OrcasFS{
		h:         h,
		c:         c,
		bktID:     bktID,
		bucket:    bucket,
		chunkSize: chunkSize,
	}

	DebugLog("NewOrcasFS: bucketID=%d, chunkSize=%d", bktID, chunkSize)

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
