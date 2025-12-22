//go:build !windows
// +build !windows

package vfs

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/orcastor/orcas/core"
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
	// Default permissions
	DefaultPermissions bool
	// Configuration (for encryption, compression, instant upload, etc.)
	Config *core.Config
	// Enable debug mode (verbose output with timestamps)
	Debug bool
	// RequireKey: if true, return EPERM error when KEY is not provided in context
	RequireKey bool
	// BasePath: Base path for metadata (database storage location)
	// If empty, uses global ORCAS_BASE environment variable
	BasePath string
	// DataPath: Data path for file data storage location
	// If empty, uses global ORCAS_DATA environment variable
	DataPath string
	// EndecKey: Encryption key for data encryption/decryption
	// If empty, encryption key will not be used (data will not be encrypted/decrypted)
	// This overrides bucket config EndecKey
	EndecKey string
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

	// Set debug mode if specified (needed early for logging)
	if opts.Debug {
		SetDebugEnabled(true)
	}

	// Create filesystem with configuration from Config
	// Use Config if provided, otherwise create empty config
	var cfg *core.Config
	if opts.Config != nil {
		cfg = opts.Config
		// Override with explicit paths and EndecKey if provided (for backward compatibility)
		if opts.BasePath != "" || opts.DataPath != "" || opts.EndecKey != "" {
			newCfg := *cfg
			if opts.BasePath != "" {
				newCfg.BasePath = opts.BasePath
			}
			if opts.DataPath != "" {
				newCfg.DataPath = opts.DataPath
			}
			if opts.EndecKey != "" {
				newCfg.EndecKey = opts.EndecKey
			}
			cfg = &newCfg
		}
	} else if opts.BasePath != "" || opts.DataPath != "" || opts.EndecKey != "" {
		// For backward compatibility: if only paths or EndecKey is provided, create config
		cfg = &core.Config{
			BasePath: opts.BasePath,
			DataPath: opts.DataPath,
			EndecKey: opts.EndecKey,
		}
	}

	// Get ORCAS_BASE and ORCAS_DATA paths
	basePath := core.ORCAS_BASE
	dataPath := core.ORCAS_DATA
	if cfg != nil {
		if cfg.BasePath != "" {
			basePath = cfg.BasePath
		}
		if cfg.DataPath != "" {
			dataPath = cfg.DataPath
		}
	}
	if opts.BasePath != "" {
		basePath = opts.BasePath
	}
	if opts.DataPath != "" {
		dataPath = opts.DataPath
	}
	// If dataPath is empty, use basePath as fallback
	if dataPath == "" {
		dataPath = basePath
	}

	// Check if mount point conflicts with ORCAS_BASE or ORCAS_DATA
	// If mount point is in the same directory as ORCAS_BASE or ORCAS_DATA,
	// we need to bind mount to a temporary location first to avoid conflicts
	var actualMountPoint string
	var tmpBindMount string
	var needUnbind bool

	// Get absolute paths for comparison
	if basePath != "" {
		absBasePath, err := filepath.Abs(basePath)
		if err == nil {
			// Check if mount point is within ORCAS_BASE directory
			if strings.HasPrefix(mountPoint, absBasePath+string(filepath.Separator)) || mountPoint == absBasePath {
				// Create temporary bind mount point
				tmpBindMount = filepath.Join(os.TempDir(), fmt.Sprintf("orcas_mount_%d", os.Getpid()))
				if err := os.MkdirAll(tmpBindMount, 0o755); err != nil {
					return nil, fmt.Errorf("failed to create temporary bind mount point: %w", err)
				}
				needUnbind = true
				actualMountPoint = tmpBindMount
				fmt.Printf("Warning: Mount point %s conflicts with ORCAS_BASE %s, using temporary bind mount at %s\n", mountPoint, absBasePath, tmpBindMount)
			}
		}
	}

	if dataPath != "" && actualMountPoint == "" {
		absDataPath, err := filepath.Abs(dataPath)
		if err == nil {
			// Check if mount point is within ORCAS_DATA directory
			if strings.HasPrefix(mountPoint, absDataPath+string(filepath.Separator)) || mountPoint == absDataPath {
				// Create temporary bind mount point
				tmpBindMount = filepath.Join(os.TempDir(), fmt.Sprintf("orcas_mount_%d", os.Getpid()))
				if err := os.MkdirAll(tmpBindMount, 0o755); err != nil {
					return nil, fmt.Errorf("failed to create temporary bind mount point: %w", err)
				}
				needUnbind = true
				actualMountPoint = tmpBindMount
				fmt.Printf("Warning: Mount point %s conflicts with ORCAS_DATA %s, using temporary bind mount at %s\n", mountPoint, absDataPath, tmpBindMount)
			}
		}
	}

	// Use original mount point if no conflict
	if actualMountPoint == "" {
		actualMountPoint = mountPoint
	}

	// Check if mount point exists
	info, err := os.Stat(actualMountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			// Create mount point directory
			if err := os.MkdirAll(actualMountPoint, 0o755); err != nil {
				return nil, fmt.Errorf("failed to create mount point: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat mount point: %w", err)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("mount point is not a directory: %s", actualMountPoint)
	}

	// If we need to bind mount, do it now
	if needUnbind && tmpBindMount != "" {
		// Bind mount the original mount point to temporary location
		cmd := exec.Command("mount", "--bind", mountPoint, tmpBindMount)
		if err := cmd.Run(); err != nil {
			// Clean up temporary directory
			os.RemoveAll(tmpBindMount)
			return nil, fmt.Errorf("failed to bind mount %s to %s: %w", mountPoint, tmpBindMount, err)
		}
		fmt.Printf("Bind mounted %s to %s\n", mountPoint, tmpBindMount)
	}

	// Create filesystem with full configuration
	var ofs *OrcasFS
	if cfg != nil {
		ofs = NewOrcasFSWithConfig(h, c, bktID, cfg, opts.RequireKey)
	} else {
		ofs = NewOrcasFS(h, c, bktID, opts.RequireKey)
	}

	// Build FUSE mount options
	fuseOpts := &fuse.MountOptions{
		Options: []string{
			"default_permissions",
		},
	}

	if opts.AllowOther {
		fuseOpts.Options = append(fuseOpts.Options, "allow_other")
	}
	// Note: allow_root is not a standard FUSE option and is not supported by fusermount3
	// If root access is needed, use allow_other instead (requires user_allow_other in /etc/fuse.conf)
	// if opts.AllowRoot {
	// 	fuseOpts.Options = append(fuseOpts.Options, "allow_root")
	// }
	if opts.DefaultPermissions {
		fuseOpts.Options = append(fuseOpts.Options, "default_permissions")
	}

	// Add custom options
	if len(opts.FuseOptions) > 0 {
		fuseOpts.Options = append(fuseOpts.Options, opts.FuseOptions...)
	}

	// Mount filesystem to actual mount point (may be temporary bind mount)
	server, err := ofs.Mount(actualMountPoint, fuseOpts)
	if err != nil {
		// Clean up bind mount if it was created
		if needUnbind && tmpBindMount != "" {
			exec.Command("umount", tmpBindMount).Run()
			os.RemoveAll(tmpBindMount)
		}
		return nil, fmt.Errorf("failed to mount: %w", err)
	}

	// If we used a temporary bind mount, we need to bind mount the FUSE mount back to original location
	if needUnbind && tmpBindMount != "" {
		// Unmount the temporary bind mount first
		if err := exec.Command("umount", tmpBindMount).Run(); err != nil {
			DebugLog("[VFS Mount] WARNING: Failed to unmount temporary bind mount %s: %v", tmpBindMount, err)
		}
		// Bind mount the FUSE mount to the original mount point
		cmd := exec.Command("mount", "--bind", actualMountPoint, mountPoint)
		if err := cmd.Run(); err != nil {
			// Try to unmount the FUSE mount
			server.Unmount()
			os.RemoveAll(tmpBindMount)
			return nil, fmt.Errorf("failed to bind mount FUSE mount to original location %s: %w", mountPoint, err)
		}
		fmt.Printf("Bind mounted FUSE filesystem from %s to %s\n", actualMountPoint, mountPoint)
		// Clean up temporary directory
		os.RemoveAll(tmpBindMount)
	}

	// Note: fs.Mount() returns a server that needs to be started with server.Serve()
	// Do not call Serve() here, let the caller handle it
	return server, nil
}

// Serve runs filesystem service (blocks until unmount)
// Note: fs.Mount() already starts server.Serve() in a goroutine,
// so we should NOT call server.Serve() again. We just need to wait
// for the signal to unmount.
func Serve(server *fuse.Server, foreground bool) error {
	if foreground {
		// Run in foreground, wait for signal
		// Note: fs.Mount() already started server.Serve() in a goroutine,
		// so we just need to wait for the signal to unmount
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// Wait for signal
		sig := <-sigChan
		fmt.Printf("\nReceived signal: %v, unmounting...\n", sig)

		// Unmount (this will cause the server to stop)
		err := server.Unmount()
		if err != nil {
			return fmt.Errorf("failed to unmount: %w", err)
		}

		return nil
	} else {
		// Run in background - fs.Mount() already started the server in a goroutine,
		// so we just wait for the server to finish (which happens when unmounted)
		server.Wait()
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
