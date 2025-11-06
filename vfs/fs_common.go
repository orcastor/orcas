// Package vfs provides ORCAS filesystem implementation, supporting FUSE mounting and random access API
package vfs

import (
	"sync"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

// OrcasFS implements ORCAS filesystem, mapping ORCAS object storage to filesystem
// This struct is available on all platforms
type OrcasFS struct {
	h                 core.Handler
	c                 core.Ctx
	bktID             int64
	root              *OrcasNode
	sdk               sdk.OrcasSDK
	sdkCfg            *sdk.Config
	chunkSize         int64
	batchWriteMgr     *BatchWriteManager // One batch write manager per bucket
	batchWriteMgrOnce sync.Once          // Protects batch write manager initialization
}

// NewOrcasFS creates a new ORCAS filesystem
// This function is available on all platforms
func NewOrcasFS(h core.Handler, c core.Ctx, bktID int64, sdkCfg *sdk.Config) *OrcasFS {
	// Create SDK instance
	sdkInstance := sdk.New(h)

	// Get bucket's chunkSize
	var chunkSize int64 = 4 << 20 // Default 4MB
	buckets, err := h.GetBkt(c, []int64{bktID})
	if err == nil && len(buckets) > 0 && buckets[0].ChunkSize > 0 {
		chunkSize = buckets[0].ChunkSize
	}

	ofs := &OrcasFS{
		h:         h,
		c:         c,
		bktID:     bktID,
		sdk:       sdkInstance,
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
