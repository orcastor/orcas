//go:build !windows
// +build !windows

package vfs

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

// MountOptions 挂载选项
type MountOptions struct {
	// 挂载点路径
	MountPoint string
	// FUSE挂载选项
	FuseOptions []string
	// 是否前台运行（false表示后台运行）
	Foreground bool
	// 是否允许其他用户访问
	AllowOther bool
	// 是否允许root访问
	AllowRoot bool
	// 默认权限
	DefaultPermissions bool
	// SDK配置（用于加密、压缩、秒传等特性）
	SDKConfig *sdk.Config
}

// Mount 挂载ORCAS文件系统
func Mount(h core.Handler, c core.Ctx, bktID int64, opts *MountOptions) (*fuse.Server, error) {
	if opts == nil {
		return nil, fmt.Errorf("mount options cannot be nil")
	}

	// 检查挂载点
	mountPoint, err := filepath.Abs(opts.MountPoint)
	if err != nil {
		return nil, fmt.Errorf("invalid mount point: %w", err)
	}

	// 检查挂载点是否存在
	info, err := os.Stat(mountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			// 创建挂载点目录
			if err := os.MkdirAll(mountPoint, 0755); err != nil {
				return nil, fmt.Errorf("failed to create mount point: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat mount point: %w", err)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("mount point is not a directory: %s", mountPoint)
	}

	// 创建文件系统，传入SDK配置
	ofs := NewOrcasFS(h, c, bktID, opts.SDKConfig)

	// 构建FUSE挂载选项
	fuseOpts := &fuse.MountOptions{
		Options: []string{
			"default_permissions",
		},
	}

	if opts.AllowOther {
		fuseOpts.Options = append(fuseOpts.Options, "allow_other")
	}
	if opts.AllowRoot {
		fuseOpts.Options = append(fuseOpts.Options, "allow_root")
	}
	if opts.DefaultPermissions {
		fuseOpts.Options = append(fuseOpts.Options, "default_permissions")
	}

	// 添加自定义选项
	if len(opts.FuseOptions) > 0 {
		fuseOpts.Options = append(fuseOpts.Options, opts.FuseOptions...)
	}

	// 挂载文件系统
	server, err := ofs.Mount(mountPoint, fuseOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to mount: %w", err)
	}

	return server, nil
}

// Serve 运行文件系统服务（阻塞直到卸载）
func Serve(server *fuse.Server, foreground bool) error {
	if foreground {
		// 前台运行，等待信号
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// 启动服务
		go func() {
			server.Serve()
		}()

		// 等待信号
		<-sigChan

		// 卸载
		return server.Unmount()
	} else {
		// 后台运行
		server.Serve()
		return nil
	}
}

// Unmount 卸载文件系统
// 注意：此函数需要传入已挂载的server，或使用系统命令卸载
// 如果使用server.Unmount()，请直接调用server的方法
func Unmount(mountPoint string) error {
	// 注意：在Unix系统上，可以使用系统命令卸载
	// 例如：fusermount -u /mnt/point 或 umount /mnt/point
	// 这里返回错误，提示用户使用server.Unmount()或系统命令
	return fmt.Errorf("please use server.Unmount() or system command 'fusermount -u %s' to unmount", mountPoint)
}
