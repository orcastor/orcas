// Package vfs 提供ORCAS文件系统实现，支持FUSE挂载和随机访问API
package vfs

import (
	"sync"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

// OrcasFS 实现ORCAS文件系统，将ORCAS对象存储映射为文件系统
// 这个结构体在所有平台上都可用
type OrcasFS struct {
	h                 core.Handler
	c                 core.Ctx
	bktID             int64
	root              *OrcasNode
	sdk               sdk.OrcasSDK
	sdkCfg            *sdk.Config
	chunkSize         int64
	batchWriteMgr     *BatchWriteManager // 每个桶一个批量写入管理器
	batchWriteMgrOnce sync.Once          // 保护批量写入管理器的初始化
}

// NewOrcasFS 创建新的ORCAS文件系统
// 这个函数在所有平台上都可用
func NewOrcasFS(h core.Handler, c core.Ctx, bktID int64, sdkCfg *sdk.Config) *OrcasFS {
	// 创建SDK实例
	sdkInstance := sdk.New(h)

	// 获取桶的chunkSize
	var chunkSize int64 = 4 << 20 // 默认4MB
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

	// root节点初始化
	// Windows平台需要立即初始化root节点，因为不依赖FUSE（在fs_win.go中实现）
	// 其他平台的root节点会在Mount时通过FUSE的Inode系统初始化（在fs.go中实现）
	// initRootNode 方法在平台特定的文件中实现（fs_win.go 或 fs.go）
	ofs.initRootNode()

	return ofs
}
