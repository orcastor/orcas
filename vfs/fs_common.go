// Package vfs provides ORCAS filesystem implementation, supporting FUSE mounting and random access API
package vfs

import (
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

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
	var chunkSize int64 = 4 << 20 // Default 4MB
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
