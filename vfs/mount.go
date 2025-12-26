//go:build !windows
// +build !windows

package vfs

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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
	// RequireKey: if true, return EPERM error when EndecKey is not provided in Config
	RequireKey bool
	// EndecKey: Encryption key for data encryption/decryption
	// If empty, encryption key will not be used (data will not be encrypted/decrypted)
	// This overrides bucket config EndecKey
	EndecKey string
	// BaseDBKey: Encryption key for main database (BASE path, SQLCipher)
	// If empty, database will not be encrypted
	// This can be set at runtime before mounting
	BaseDBKey string
	// DataDBKey: Encryption key for bucket databases (DATA path, SQLCipher)
	// If empty, bucket databases will not be encrypted
	// This can be set at runtime before mounting
	DataDBKey string
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
		// Override with explicit EndecKey if provided (for backward compatibility)
		if opts.EndecKey != "" {
			newCfg := *cfg
			newCfg.EndecKey = opts.EndecKey
			cfg = &newCfg
		}
	} else if opts.EndecKey != "" {
		// For backward compatibility: if only EndecKey is provided, create config
		cfg = &core.Config{
			EndecKey: opts.EndecKey,
		}
	}

	// If config has paths, set them in Handler
	// Paths are now managed via Handler, not context
	if cfg != nil && (cfg.BasePath != "" || cfg.DataPath != "") {
		h.MetadataAdapter().SetBasePath(cfg.BasePath)
		h.MetadataAdapter().SetDataPath(cfg.DataPath)
		h.DataAdapter().SetDataPath(cfg.DataPath)
	}

	// Set database encryption keys if provided
	if opts.BaseDBKey != "" {
		h.MetadataAdapter().SetBaseKey(opts.BaseDBKey)
	}
	if opts.DataDBKey != "" {
		h.MetadataAdapter().SetDataKey(opts.DataDBKey)
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

	// Check if RequireKey is set but EndecKey is not provided
	if opts.RequireKey {
		var endecKey string
		if cfg != nil {
			endecKey = cfg.EndecKey
		}
		if opts.EndecKey != "" {
			endecKey = opts.EndecKey
		}
		if endecKey == "" {
			return nil, fmt.Errorf("RequireKey is enabled but EndecKey is not provided. Please provide encryption key via Config.EndecKey or MountOptions.EndecKey")
		}
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

	// Mount filesystem to mount point
	server, err := ofs.Mount(mountPoint, fuseOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to mount: %w", err)
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

// Unmount unmounts filesystem using system command
// This function attempts to unmount the filesystem at the given mount point
// using fusermount -u (preferred) or umount as fallback
func Unmount(mountPoint string) error {
	// Resolve absolute path
	absMountPoint, err := filepath.Abs(mountPoint)
	if err != nil {
		return fmt.Errorf("invalid mount point: %w", err)
	}

	// Check if mount point exists
	info, err := os.Stat(absMountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("mount point does not exist: %s", absMountPoint)
		}
		return fmt.Errorf("failed to stat mount point: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("mount point is not a directory: %s", absMountPoint)
	}

	// Try fusermount -u first (preferred for FUSE filesystems)
	// fusermount3 is the newer version, but fusermount should work too
	cmd := exec.Command("fusermount", "-u", absMountPoint)
	fusermountErr := cmd.Run()
	if fusermountErr == nil {
		DebugLog("[VFS Unmount] Successfully unmounted using fusermount: %s", absMountPoint)
		return nil
	}

	// If fusermount fails, try fusermount3 (newer version)
	cmd = exec.Command("fusermount3", "-u", absMountPoint)
	fusermount3Err := cmd.Run()
	if fusermount3Err == nil {
		DebugLog("[VFS Unmount] Successfully unmounted using fusermount3: %s", absMountPoint)
		return nil
	}

	// As fallback, try umount (may require root privileges)
	cmd = exec.Command("umount", absMountPoint)
	umountErr := cmd.Run()
	if umountErr == nil {
		DebugLog("[VFS Unmount] Successfully unmounted using umount: %s", absMountPoint)
		return nil
	}

	// All methods failed
	return fmt.Errorf("failed to unmount %s: tried fusermount (error: %v), fusermount3 (error: %v), and umount (error: %v), all failed. You may need to use server.Unmount() instead", absMountPoint, fusermountErr, fusermount3Err, umountErr)
}
