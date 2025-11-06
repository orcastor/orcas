//go:build windows
// +build windows

package vfs

import (
	"github.com/orcastor/orcas/core"
)

// OrcasNode minimal implementation on Windows (currently only for testing and RandomAccessor API)
// TODO: Can use Dokany in the future to implement complete filesystem mounting functionality
// Dokany is a FUSE alternative on Windows, similar to Linux's FUSE
// Reference: https://github.com/dokan-dev/dokany
type OrcasNode struct {
	fs     *OrcasFS
	objID  int64
	obj    *core.ObjectInfo
	isRoot bool
	ra     *RandomAccessor
}

// initRootNode initializes root node (Windows platform implementation)
// Windows platform needs to initialize root node immediately, as it currently doesn't rely on filesystem mounting
// Note: If Dokany support is implemented in the future, can initialize during Mount (similar to FUSE)
func (ofs *OrcasFS) initRootNode() {
	if ofs.root == nil {
		ofs.root = &OrcasNode{
			fs:     ofs,
			objID:  core.ROOT_OID,
			obj:    nil,
			isRoot: true,
		}
	}
}

// TODO: Implement Windows platform Mount functionality (using Dokany)
// func (ofs *OrcasFS) Mount(mountPoint string, opts *DokanyMountOptions) (*DokanyInstance, error) {
// 	// Use Dokany to implement filesystem mounting
// 	// Requires:
// 	// 1. Install Dokany driver
// 	// 2. Go bindings library (e.g., github.com/dokan-dev/dokany-go)
// 	// 3. Implement Dokany filesystem interface
// }
