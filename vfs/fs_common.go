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
	sdkCfg    *sdk.Config
	chunkSize int64
	// Batch writer is now managed globally by SDK, accessed via GetBatchWriterForBucket
}

// NewOrcasFS creates a new ORCAS filesystem
// This function is available on all platforms
func NewOrcasFS(h core.Handler, c core.Ctx, bktID int64, sdkCfg *sdk.Config) *OrcasFS {
	// Get bucket's chunkSize
	var chunkSize int64 = 4 << 20 // Default 4MB
	bucket, err := h.GetBktInfo(c, bktID)
	if err == nil && bucket != nil && bucket.ChunkSize > 0 {
		chunkSize = bucket.ChunkSize
	}

	// Register SDK config to Handler for ConvertWritingVersions job
	// This allows the scheduled job to access compression/encryption settings
	if sdkCfg != nil {
		if lh, ok := h.(*core.LocalHandler); ok {
			lh.SetSDKConfig(bktID, sdkCfg.WiseCmpr, sdkCfg.CmprQlty, sdkCfg.EndecWay, sdkCfg.EndecKey)
		}
	}

	ofs := &OrcasFS{
		h:         h,
		c:         c,
		bktID:     bktID,
		sdkCfg:    sdkCfg,
		chunkSize: chunkSize,
	}

	// Root node initialization
	// Windows platform needs to initialize root node immediately, as it doesn't rely on FUSE (implemented in fs_win.go)
	// Other platforms' root nodes are initialized during Mount through FUSE's Inode system (implemented in fs.go)
	// initRootNode method is implemented in platform-specific files (fs_win.go or fs.go)
	ofs.initRootNode()

	return ofs
}
