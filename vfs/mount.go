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

// MountOptions mount options
type MountOptions struct {
	// Mount point path
	MountPoint string
	// FUSE mount options
	FuseOptions []string
	// Run in foreground (false means background)
	Foreground bool
	// Allow other users to access
	AllowOther bool
	// Allow root access
	AllowRoot bool
	// Default permissions
	DefaultPermissions bool
	// SDK configuration (for encryption, compression, instant upload, etc.)
	SDKConfig *sdk.Config
}

// Mount mounts ORCAS filesystem
func Mount(h core.Handler, c core.Ctx, bktID int64, opts *MountOptions) (*fuse.Server, error) {
	if opts == nil {
		return nil, fmt.Errorf("mount options cannot be nil")
	}

	// Check mount point
	mountPoint, err := filepath.Abs(opts.MountPoint)
	if err != nil {
		return nil, fmt.Errorf("invalid mount point: %w", err)
	}

	// Check if mount point exists
	info, err := os.Stat(mountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			// Create mount point directory
			if err := os.MkdirAll(mountPoint, 0o755); err != nil {
				return nil, fmt.Errorf("failed to create mount point: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat mount point: %w", err)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("mount point is not a directory: %s", mountPoint)
	}

	// Create filesystem, pass SDK configuration
	ofs := NewOrcasFS(h, c, bktID, opts.SDKConfig)

	// Build FUSE mount options
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

	// Add custom options
	if len(opts.FuseOptions) > 0 {
		fuseOpts.Options = append(fuseOpts.Options, opts.FuseOptions...)
	}

	// Mount filesystem
	server, err := ofs.Mount(mountPoint, fuseOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to mount: %w", err)
	}

	return server, nil
}

// Serve runs filesystem service (blocks until unmount)
func Serve(server *fuse.Server, foreground bool) error {
	if foreground {
		// Run in foreground, wait for signal
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// Start service
		go func() {
			server.Serve()
		}()

		// Wait for signal
		<-sigChan

		// Unmount
		return server.Unmount()
	} else {
		// Run in background
		server.Serve()
		return nil
	}
}

// Unmount unmounts filesystem
// Note: This function requires a mounted server, or use system command to unmount
// If using server.Unmount(), please call server's method directly
func Unmount(mountPoint string) error {
	// Note: On Unix systems, can use system command to unmount
	// For example: fusermount -u /mnt/point or umount /mnt/point
	// Here returns error, prompting user to use server.Unmount() or system command
	return fmt.Errorf("please use server.Unmount() or system command 'fusermount -u %s' to unmount", mountPoint)
}
