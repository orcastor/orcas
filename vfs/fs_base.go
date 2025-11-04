//go:build windows
// +build windows

package vfs

import (
	"sync"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

// OrcasNode Windows上的最小化实现（仅用于测试，不支持FUSE挂载）
type OrcasNode struct {
	fs     *OrcasFS
	objID  int64
	obj    *core.ObjectInfo
	objMu  sync.RWMutex
	isRoot bool
	ra     *RandomAccessor
}

// OrcasFS 实现ORCAS文件系统，将ORCAS对象存储映射为文件系统
// 这个结构体不依赖FUSE，可以在Windows上使用
type OrcasFS struct {
	h         core.Handler
	c         core.Ctx
	bktID     int64
	root      *OrcasNode
	sdk       sdk.OrcasSDK
	sdkCfg    *sdk.Config
	chunkSize int64
}

// NewOrcasFS 创建新的ORCAS文件系统
func NewOrcasFS(h core.Handler, c core.Ctx, bktID int64, sdkCfg *sdk.Config) *OrcasFS {
	// 创建SDK实例
	sdkInstance := sdk.New(h)

	// 获取桶的chunkSize
	var chunkSize int64 = 4 * 1024 * 1024 // 默认4MB
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
	ofs.root = &OrcasNode{
		fs:     ofs,
		objID:  core.ROOT_OID,
		obj:    nil,
		objMu:  sync.RWMutex{},
		isRoot: true,
	}
	return ofs
}
