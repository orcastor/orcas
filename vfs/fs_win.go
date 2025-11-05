//go:build windows
// +build windows

package vfs

import (
	"sync"

	"github.com/orcastor/orcas/core"
)

// OrcasNode Windows上的最小化实现（当前仅用于测试和RandomAccessor API）
// TODO: 未来可以使用 Dokany 实现完整的文件系统挂载功能
// Dokany 是 Windows 上的 FUSE 替代方案，类似于 Linux 的 FUSE
// 参考：https://github.com/dokan-dev/dokany
type OrcasNode struct {
	fs     *OrcasFS
	objID  int64
	obj    *core.ObjectInfo
	objMu  sync.RWMutex
	isRoot bool
	ra     *RandomAccessor
}

// initRootNode 初始化root节点（Windows平台实现）
// Windows平台需要立即初始化root节点，因为当前不依赖文件系统挂载
// 注意：如果未来实现 Dokany 支持，可以在 Mount 时初始化（类似 FUSE）
func (ofs *OrcasFS) initRootNode() {
	if ofs.root == nil {
		ofs.root = &OrcasNode{
			fs:     ofs,
			objID:  core.ROOT_OID,
			obj:    nil,
			objMu:  sync.RWMutex{},
			isRoot: true,
		}
	}
}

// TODO: 实现 Windows 平台的 Mount 功能（使用 Dokany）
// func (ofs *OrcasFS) Mount(mountPoint string, opts *DokanyMountOptions) (*DokanyInstance, error) {
// 	// 使用 Dokany 实现文件系统挂载
// 	// 需要：
// 	// 1. 安装 Dokany 驱动
// 	// 2. Go 绑定库（如 github.com/dokan-dev/dokany-go）
// 	// 3. 实现 Dokany 的文件系统接口
// }
