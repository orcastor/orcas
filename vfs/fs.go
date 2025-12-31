//go:build !windows
// +build !windows

package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	b "github.com/orca-zhang/borm"
	"github.com/orcastor/orcas/core"
)

// O_LARGEFILE flag for large file support (files > 2GB)
// On 64-bit systems, this is typically 0 or not needed, but we support it for compatibility
// On 32-bit Linux systems, this is typically 0x8000
// Note: Using ALL_CAPS for system constant is acceptable
//
//nolint:revive // O_LARGEFILE is a system constant, ALL_CAPS is appropriate
const O_LARGEFILE = 0x8000

// cachedDirStream wraps DirStream entries for caching
type cachedDirStream struct {
	entries []fuse.DirEntry
	objID   int64 // Parent directory ID
	pid     int64 // Parent's parent ID
}

// initRootNode initializes root node (non-Windows platform implementation)
// On non-Windows platforms, root node is initialized during Mount, not during NewOrcasFS
func (ofs *OrcasFS) initRootNode() {
	// Non-Windows platform: root node is initialized during Mount, not here
	// This allows proper initialization through FUSE's Inode system during Mount
}

// Mount mounts filesystem to specified path (Linux/Unix only)
func (ofs *OrcasFS) Mount(mountPoint string, opts *fuse.MountOptions) (*fuse.Server, error) {
	// Initialize root node (if not already initialized)
	// Root node's objID is bucketID, not ROOT_OID
	if ofs.root == nil {
		ofs.root = &OrcasNode{
			fs:     ofs,
			objID:  ofs.bktID,
			isRoot: true,
		}
	}
	// Set root node's fs reference
	ofs.root.fs = ofs

	// Default mount options
	if opts == nil {
		opts = &fuse.MountOptions{
			Options: []string{
				"default_permissions",
			},
		}
	}

	// Mount filesystem, directly use root node as root Inode
	// Note: fs.Mount() does NOT automatically start the server
	// The caller must call server.Serve() to start the service
	server, err := fs.Mount(mountPoint, ofs.root, &fs.Options{
		MountOptions: *opts,
		// Explicitly set to not auto-start the server
		// The server will be started by the caller via server.Serve()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to mount: %w", err)
	}

	// Store server reference in OrcasFS for use in OnRootDeleted callback
	ofs.Server = server

	return server, nil
}

// OrcasNode represents a node in ORCAS filesystem (file or directory)
type OrcasNode struct {
	fs.Inode
	fs     *OrcasFS
	objID  int64
	obj    atomic.Value // *core.ObjectInfo
	isRoot bool
	ra     atomic.Value // *RandomAccessor (nil means not initialized, releasedMarker means released)
}

// releasedMarker is a special marker to indicate that RandomAccessor has been released
// atomic.Value cannot store nil, so we use this marker instead
var releasedMarker = &RandomAccessor{}

var (
	_ fs.InodeEmbedder     = (*OrcasNode)(nil)
	_ fs.NodeStatfser      = (*OrcasNode)(nil)
	_ fs.NodeAccesser      = (*OrcasNode)(nil)
	_ fs.NodeGetattrer     = (*OrcasNode)(nil)
	_ fs.NodeSetattrer     = (*OrcasNode)(nil)
	_ fs.NodeOnAdder       = (*OrcasNode)(nil)
	_ fs.NodeGetxattrer    = (*OrcasNode)(nil)
	_ fs.NodeSetxattrer    = (*OrcasNode)(nil)
	_ fs.NodeRemovexattrer = (*OrcasNode)(nil)
	_ fs.NodeListxattrer   = (*OrcasNode)(nil)
	_ fs.NodeReadlinker    = (*OrcasNode)(nil)
	_ fs.NodeOpener        = (*OrcasNode)(nil)
	// Note: NodeReader and NodeWriter are automatically forwarded to FileHandle by go-fuse
	// Since OrcasNode implements FileReader and FileWriter, NodeReader/NodeWriter will work automatically
	_ fs.NodeFsyncer        = (*OrcasNode)(nil)
	_ fs.NodeFlusher        = (*OrcasNode)(nil)
	_ fs.NodeReleaser       = (*OrcasNode)(nil)
	_ fs.NodeAllocater      = (*OrcasNode)(nil)
	_ fs.NodeCopyFileRanger = (*OrcasNode)(nil)
	_ fs.NodeStatxer        = (*OrcasNode)(nil)
	_ fs.NodeLseeker        = (*OrcasNode)(nil)
	_ fs.NodeGetlker        = (*OrcasNode)(nil)
	_ fs.NodeSetlker        = (*OrcasNode)(nil)
	_ fs.NodeSetlkwer       = (*OrcasNode)(nil)
	_ fs.NodeIoctler        = (*OrcasNode)(nil)
	_ fs.NodeOnForgetter    = (*OrcasNode)(nil)
	_ fs.NodeLookuper       = (*OrcasNode)(nil)
	_ fs.NodeWrapChilder    = (*OrcasNode)(nil)
	_ fs.NodeOpendirer      = (*OrcasNode)(nil)
	_ fs.NodeReaddirer      = (*OrcasNode)(nil)
	_ fs.NodeMkdirer        = (*OrcasNode)(nil)
	_ fs.NodeMknoder        = (*OrcasNode)(nil)
	_ fs.NodeLinker         = (*OrcasNode)(nil)
	_ fs.NodeSymlinker      = (*OrcasNode)(nil)
	_ fs.NodeCreater        = (*OrcasNode)(nil)
	_ fs.NodeUnlinker       = (*OrcasNode)(nil)
	_ fs.NodeRmdirer        = (*OrcasNode)(nil)
	_ fs.NodeRenamer        = (*OrcasNode)(nil)
	_ fs.FileReader         = (*OrcasNode)(nil)
	_ fs.FileWriter         = (*OrcasNode)(nil)
	// Note: FileReleaser.Release has different signature than NodeReleaser.Release
	// We implement NodeReleaser.Release which forwards to FileReleaser if needed
	// FileReleaser interface is not explicitly declared here due to signature mismatch
)

// getObj gets object information (with cache)
// Optimization: use atomic operations, completely lock-free
// For file objects, also checks global fileObjCache to get latest size
func (n *OrcasNode) getObj() (*core.ObjectInfo, error) {
	// First check: atomic read
	if val := n.obj.Load(); val != nil {
		if obj, ok := val.(*core.ObjectInfo); ok && obj != nil {
			// DebugLog("[VFS getObj] Found in local cache: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d",
			//	obj.ID, obj.Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, obj.Name, obj.PID)
			// For file objects, also check global fileObjCache to get latest size
			// This ensures we get the most up-to-date file size after writes
			if obj.Type == core.OBJ_TYPE_FILE {
				cacheKey := obj.ID
				if cached, ok := fileObjCache.Get(cacheKey); ok {
					if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
						// DebugLog("[VFS getObj] Found in global cache (for file): objID=%d, type=%d, name=%s, size=%d",
						//	cachedObj.ID, cachedObj.Type, cachedObj.Name, cachedObj.Size)
						// Use cached object (has latest size from RandomAccessor updates)
						// Also update local cache for consistency
						n.obj.Store(cachedObj)
						return cachedObj, nil
					}
				}
			}
			return obj, nil
		}
	}

	// If root node, return virtual object with bucketID as ID
	if n.isRoot {
		return &core.ObjectInfo{
			ID:    n.fs.bktID,
			PID:   0,
			Type:  core.OBJ_TYPE_DIR,
			Name:  "/",
			MTime: core.Now(),
		}, nil
	}

	// Check global fileObjCache first (before database query) for both files and directories
	// This ensures we get cached information from Readdir/Lookup operations
	if !n.isRoot {
		cacheKey := n.objID
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				// Verify that cached object ID matches expected ID
				// This prevents issues where cache might have incorrect information
				if cachedObj.ID == n.objID {
					// Update local cache for consistency
					n.obj.Store(cachedObj)
					DebugLog("[VFS getObj] Found in global cache: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d, size=%d",
						cachedObj.ID, cachedObj.Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, cachedObj.Name, cachedObj.PID, cachedObj.Size)
					return cachedObj, nil
				} else {
					// Cache has incorrect ID, invalidate it and fetch from database
					DebugLog("[VFS getObj] WARNING: Cached object ID mismatch (expected %d, got %d), invalidating cache", n.objID, cachedObj.ID)
					fileObjCache.Del(cacheKey)
				}
			}
		}
	}

	// Query object (executed outside lock)
	DebugLog("[VFS getObj] Querying database: objID=%d", n.objID)
	objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{n.objID})
	if err != nil {
		DebugLog("[VFS getObj] ERROR: Failed to query database: objID=%d, error=%v", n.objID, err)
		return nil, err
	}
	if len(objs) == 0 {
		DebugLog("[VFS getObj] ERROR: Object not found in database: objID=%d", n.objID)
		return nil, syscall.ENOENT
	}

	DebugLog("[VFS getObj] Got from database: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d, size=%d, DataID=%d",
		objs[0].ID, objs[0].Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, objs[0].Name, objs[0].PID, objs[0].Size, objs[0].DataID)

	// Double check: check cache again (may have been updated by other goroutine)
	if val := n.obj.Load(); val != nil {
		if obj, ok := val.(*core.ObjectInfo); ok && obj != nil {
			// Verify that cached object type matches database type
			// This prevents issues where cache might have incorrect type information
			if obj.Type == objs[0].Type && obj.ID == objs[0].ID {
				DebugLog("[VFS getObj] Found in local cache (after DB query): objID=%d, type=%d, size=%d",
					obj.ID, obj.Type, obj.Size)
				return obj, nil
			} else {
				// Cached object type doesn't match database, clear it
				DebugLog("[VFS getObj] WARNING: Cached object type mismatch (cached type=%d, DB type=%d), clearing cache: objID=%d", obj.Type, objs[0].Type, obj.ID)
				n.obj.Store((*core.ObjectInfo)(nil))
			}
		}
	}

	// Verify that cached object in global cache matches database type
	cacheKey := n.objID
	if cached, ok := fileObjCache.Get(cacheKey); ok {
		if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
			// If cached object type doesn't match database, clear it
			if cachedObj.Type != objs[0].Type || cachedObj.ID != objs[0].ID {
				DebugLog("[VFS getObj] WARNING: Global cache object type mismatch (cached type=%d, DB type=%d), clearing cache: objID=%d", cachedObj.Type, objs[0].Type, cachedObj.ID)
				fileObjCache.Del(cacheKey)
			}
		}
	}

	// Update both local and global cache (atomic operation)
	n.obj.Store(objs[0])
	// Update global fileObjCache for both files and directories
	// This allows GetAttr to use cached information from Readdir/Lookup
	fileObjCache.Put(cacheKey, objs[0])
	DebugLog("[VFS getObj] Updated caches: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d, size=%d",
		objs[0].ID, objs[0].Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, objs[0].Name, objs[0].PID, objs[0].Size)

	return objs[0], nil
}

// invalidateObj invalidates object cache
func (n *OrcasNode) invalidateObj() {
	n.obj.Store((*core.ObjectInfo)(nil))
}

// getDirListCacheMutex gets or creates a mutex for a specific directory cache
// This ensures thread-safe operations on directory listing cache
func getDirListCacheMutex(dirID int64) *sync.RWMutex {
	if mu, ok := dirListCacheMu.Load(dirID); ok {
		return mu.(*sync.RWMutex)
	}
	// Create new mutex if not exists
	mu := &sync.RWMutex{}
	if actual, loaded := dirListCacheMu.LoadOrStore(dirID, mu); loaded {
		return actual.(*sync.RWMutex)
	}
	return mu
}

// invalidateDirListCache invalidates directory listing cache for a directory
// This should be called whenever directory contents change (Create, Mkdir, Unlink, Rmdir, Rename)
// Cache will be rebuilt on next readdir/list operation
func (n *OrcasNode) invalidateDirListCache(dirID int64) {
	cacheKey := n.getDirListCacheKey(dirID)
	dirListCache.Del(cacheKey)
	readdirCache.Del(dirID)
	readdirCacheStale.Delete(dirID)
	DebugLog("[VFS invalidateDirListCache] Invalidated directory cache: dirID=%d", dirID)
}

// Getattr gets file/directory attributes
func (n *OrcasNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if !n.isRoot {
		if errno := n.fs.checkKey(); errno != 0 {
			DebugLog("[VFS Getattr] ERROR: checkKey failed: objID=%d, errno=%d", n.objID, errno)
			return errno
		}
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Getattr] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return syscall.ENOENT
	}

	// For root directory, use 777 permissions
	if n.isRoot && obj.Type == core.OBJ_TYPE_DIR {
		out.Mode = syscall.S_IFDIR | 0o777
	} else {
		out.Mode = getModeFromObj(obj)
	}
	out.Size = uint64(obj.Size)
	out.Mtime = uint64(obj.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime
	out.Nlink = 1
	return 0
}

// getModeFromObj extracts file mode from ObjectInfo.Mode field
// If Mode is 0, returns default mode based on object type
func getModeFromObj(obj *core.ObjectInfo) uint32 {
	if obj == nil {
		return syscall.S_IFREG | 0o644
	}

	// If Mode is set, use it (preserving file type bits)
	if obj.Mode != 0 {
		var fileTypeBits uint32 = syscall.S_IFREG
		if obj.Type == core.OBJ_TYPE_DIR {
			fileTypeBits = syscall.S_IFDIR
		}
		// Preserve file type bits and use stored permission bits
		return fileTypeBits | (obj.Mode & 0o7777)
	}

	// Default mode based on object type
	return getMode(obj.Type)
}

// setModeInObj sets file mode in ObjectInfo.Mode field
func setModeInObj(obj *core.ObjectInfo, mode uint32) {
	if obj == nil {
		return
	}

	// Extract permission bits (remove file type bits)
	obj.Mode = mode & 0o7777
}

// getMode returns default file mode based on object type
func getMode(objType int) uint32 {
	switch objType {
	case core.OBJ_TYPE_DIR:
		return syscall.S_IFDIR | 0o755
	case core.OBJ_TYPE_FILE:
		return syscall.S_IFREG | 0o644
	default:
		return syscall.S_IFREG | 0o644
	}
}

// Lookup looks up child node
func (n *OrcasNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Lookup] ERROR: checkKey failed: parentID=%d, name=%s, errno=%d", n.objID, name, errno)
		return nil, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Lookup] ERROR: Failed to get parent object: parentID=%d, name=%s, error=%v", n.objID, name, err)
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Lookup] ERROR: Parent is not a directory: parentID=%d, name=%s, type=%d", n.objID, name, obj.Type)
		return nil, syscall.ENOTDIR
	}

	// Get directory listing with cache and singleflight
	children, errno := n.getDirListWithCache(obj.ID)
	if errno != 0 {
		DebugLog("[VFS Lookup] ERROR: Failed to get directory listing: parentID=%d, name=%s, errno=%d", n.objID, name, errno)
		return nil, errno
	}

	// Find matching child object
	// If multiple objects with the same name exist (shouldn't happen, but handle it),
	// prioritize file over directory
	var matchedChild *core.ObjectInfo
	var matchedFile *core.ObjectInfo
	for _, child := range children {
		if child.Name == name {
			if child.Type == core.OBJ_TYPE_FILE {
				// Found a file with matching name, prioritize it
				matchedFile = child
				break
			} else if child.Type == core.OBJ_TYPE_DIR && matchedChild == nil {
				// Found a directory with matching name, but prefer file if available
				matchedChild = child
			}
		}
	}

	// Use file if found, otherwise use directory
	if matchedFile != nil {
		matchedChild = matchedFile
	}

	if matchedChild == nil {
		return nil, syscall.ENOENT
	}

	// For file objects, always check global fileObjCache first to get latest size
	// Directory listing cache may have stale size information
	cacheKey := matchedChild.ID
	if matchedChild.Type == core.OBJ_TYPE_FILE {
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				// Verify that cached object ID matches
				if cachedObj.ID == matchedChild.ID {
					// Use cached object (has latest size from RandomAccessor/Release updates)
					matchedChild = cachedObj
					// DebugLog("[VFS Lookup] Using file object from global cache (latest size): objID=%d, size=%d", matchedChild.ID, matchedChild.Size)
				} else {
					// Cache has incorrect ID, invalidate it
					DebugLog("[VFS Lookup] WARNING: Cached object ID mismatch (expected %d, got %d), invalidating cache", matchedChild.ID, cachedObj.ID)
					fileObjCache.Del(cacheKey)
				}
			}
		} else {
			// Global cache miss, fetch from database to get latest size
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{matchedChild.ID})
			if err == nil && len(objs) > 0 {
				matchedChild = objs[0]
				// Update cache with latest information
				fileObjCache.Put(cacheKey, matchedChild)
				// DebugLog("[VFS Lookup] Fetched file object from database (latest size): objID=%d, size=%d", matchedChild.ID, matchedChild.Size)
			}
		}
	} else {
		// For directories, verify type matches
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				// If cached object type doesn't match, invalidate cache and fetch from database
				if cachedObj.Type != matchedChild.Type || cachedObj.ID != matchedChild.ID {
					DebugLog("[VFS Lookup] WARNING: Cached object type mismatch (cached type=%d, list type=%d), invalidating cache: objID=%d", cachedObj.Type, matchedChild.Type, matchedChild.ID)
					fileObjCache.Del(cacheKey)
					// Fetch from database to get correct type
					objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{matchedChild.ID})
					if err == nil && len(objs) > 0 {
						matchedChild = objs[0]
						// Update cache with correct type
						fileObjCache.Put(cacheKey, matchedChild)
						// DebugLog("[VFS Lookup] Fetched from database and updated cache: objID=%d, type=%d", matchedChild.ID, matchedChild.Type)
					}
				}
			}
		}
	}

	// Create child node
	childNode := &OrcasNode{
		fs:    n.fs,
		objID: matchedChild.ID,
	}
	childNode.obj.Store(matchedChild)

	// Update global fileObjCache to ensure consistency
	// This prevents issues where cache might have incorrect type information
	fileObjCache.Put(cacheKey, matchedChild)

	// Create Inode based on type
	var stableAttr fs.StableAttr
	if matchedChild.Type == core.OBJ_TYPE_DIR {
		stableAttr = fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Ino:  uint64(matchedChild.ID),
		}
	} else {
		stableAttr = fs.StableAttr{
			Mode: syscall.S_IFREG,
			Ino:  uint64(matchedChild.ID),
		}
	}

	childInode := n.NewInode(ctx, childNode, stableAttr)

	// Fill EntryOut
	out.Mode = getMode(matchedChild.Type)
	out.Size = uint64(matchedChild.Size)
	out.Mtime = uint64(matchedChild.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime
	out.Ino = uint64(matchedChild.ID)

	return childInode, 0
}

// Readdir reads directory contents
// Optimized: uses interface-level cache to avoid rebuilding entries every time
// Implements delayed cache refresh: marks cache as stale instead of immediately deleting
func (n *OrcasNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if errno := n.fs.checkKey(); errno != 0 {
		if !n.isRoot {
			DebugLog("[VFS Readdir] ERROR: checkKey failed: objID=%d, errno=%d", n.objID, errno)
			return nil, errno
		}
		DebugLog("[VFS Readdir] Root node, returning empty directory stream (key check failed)")
		return fs.NewListDirStream([]fuse.DirEntry{}), 0
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Readdir] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Readdir] ERROR: Object is not a directory: objID=%d, type=%d", n.objID, obj.Type)
		return nil, syscall.ENOTDIR
	}

	// Check if cache is marked as stale (delayed refresh)
	cacheKey := obj.ID
	if _, isStale := readdirCacheStale.Load(obj.ID); isStale {
		// Cache is stale, delete it and clear stale marker
		readdirCache.Del(cacheKey)
		readdirCacheStale.Delete(obj.ID)
	}

	// Check Readdir cache first (interface-level cache)
	if cached, ok := readdirCache.Get(cacheKey); ok {
		if cachedStream, ok := cached.(*cachedDirStream); ok && cachedStream != nil {
			// Cache hit, return cached entries directly (no data merging needed)
			// Asynchronously preload child directories in background
			go func() {
				// Get children from dirListCache for preloading
				dirListCacheKey := n.getDirListCacheKey(obj.ID)
				if childrenCached, ok := dirListCache.Get(dirListCacheKey); ok {
					if children, ok := childrenCached.([]*core.ObjectInfo); ok && children != nil {
						n.preloadChildDirs(children)
					}
				}
			}()
			return fs.NewListDirStream(cachedStream.entries), 0
		}
	}

	// Cache miss, get directory listing and build entries
	children, errno := n.getDirListWithCache(obj.ID)
	if errno != 0 {
		DebugLog("[VFS Readdir] ERROR: Failed to get directory listing: objID=%d, errno=%d", n.objID, errno)
		return nil, errno
	}

	// Build directory stream
	entries := make([]fuse.DirEntry, 0, len(children)+2)
	// Add . and ..
	// For root directory, both . and .. point to itself (bucketID)
	var dotIno, dotDotIno uint64
	if n.isRoot {
		dotIno = uint64(n.fs.bktID)
		dotDotIno = uint64(n.fs.bktID) // Root's .. points to itself
	} else {
		dotIno = uint64(obj.ID)
		if obj.PID == n.fs.bktID {
			// Parent is root (bucketID), use bucketID
			dotDotIno = uint64(n.fs.bktID)
		} else {
			dotDotIno = uint64(obj.PID)
		}
	}
	entries = append(entries, fuse.DirEntry{
		Name: ".",
		Mode: syscall.S_IFDIR,
		Ino:  dotIno,
	})
	entries = append(entries, fuse.DirEntry{
		Name: "..",
		Mode: syscall.S_IFDIR,
		Ino:  dotDotIno,
	})

	// Add child objects
	for _, child := range children {
		mode := getMode(child.Type)
		entries = append(entries, fuse.DirEntry{
			Name: child.Name,
			Mode: mode,
			Ino:  uint64(child.ID),
		})
	}

	// Cache the DirStream entries for future Readdir calls
	cachedStream := &cachedDirStream{
		entries: entries,
		objID:   obj.ID,
		pid:     obj.PID,
	}
	readdirCache.Put(cacheKey, cachedStream)

	// Asynchronously preload child directories
	go n.preloadChildDirs(children)

	return fs.NewListDirStream(entries), 0
}

// getDirListCacheKey generates cache key for directory listing
// Root directory's dirID is now bucketID, so no special encoding needed
func (n *OrcasNode) getDirListCacheKey(dirID int64) int64 {
	// dirID is globally unique (including root which is bucketID), use it directly
	return dirID
}

// getDirListWithCache gets directory listing with cache and singleflight
// Uses singleflight to prevent duplicate concurrent requests for the same directory
// Thread-safe: uses per-directory mutex to prevent race conditions with cache updates
func (n *OrcasNode) getDirListWithCache(dirID int64) ([]*core.ObjectInfo, syscall.Errno) {
	// Get pending objects first (these are always fresh and should be included)
	pendingChildren := n.getPendingObjectsForDir(dirID)

	// Check cache first (with lock to prevent race conditions with cache updates)
	cacheKey := n.getDirListCacheKey(dirID)
	mu := getDirListCacheMutex(cacheKey)
	mu.RLock()
	cached, cacheOk := dirListCache.Get(cacheKey)
	mu.RUnlock()

	if cacheOk {
		if children, ok := cached.([]*core.ObjectInfo); ok && children != nil {
			// Merge pending objects with cached children
			// Strategy: Cached entries (from database) are authoritative
			// Pending entries are only added if they don't exist in cache (by ID or by name)
			// If same file exists in both, prefer cached entry (with DataID) over pending entry

			// Create maps to track existing children by ID and by name
			childrenMapByID := make(map[int64]*core.ObjectInfo)
			childrenMapByName := make(map[string]*core.ObjectInfo)

			// First, add all cached children (these are from database, authoritative)
			for _, child := range children {
				childrenMapByID[child.ID] = child
				// For name-based deduplication, prefer cached entry (with DataID) over pending
				if existing, exists := childrenMapByName[child.Name]; !exists {
					childrenMapByName[child.Name] = child
				} else {
					// If same name exists, prefer the one with DataID (from database) over pending (DataID=0)
					if child.DataID > 0 && existing.DataID == 0 {
						childrenMapByName[child.Name] = child
					}
				}
			}

			// Then, add pending objects that are not already in cache
			// IMPORTANT: Even if pending object has DataID (already flushed), if it's not in cache,
			// it might be a race condition where file was flushed but cache wasn't updated yet
			for _, pending := range pendingChildren {
				// Check by ID first - if same ID exists in cache, prefer cache entry if it has DataID
				if existing, exists := childrenMapByID[pending.ID]; exists {
					// Same ID exists - prefer cache entry if it has DataID, otherwise use pending
					if existing.DataID > 0 {
						// Cached entry has DataID (from database), skip pending
						// DebugLog("[VFS getDirListWithCache] Skipping pending object in cache (same ID in cache with DataID): dirID=%d, fileID=%d, name=%s, cacheDataID=%d", dirID, pending.ID, pending.Name, existing.DataID)
						continue
					}
					// Cache entry has no DataID but pending has DataID, update cache entry with pending data
					if pending.DataID > 0 {
						existing.DataID = pending.DataID
						existing.Size = pending.Size
						DebugLog("[VFS getDirListWithCache] Updated cache entry with pending DataID: dirID=%d, fileID=%d, name=%s, DataID=%d", dirID, pending.ID, pending.Name, pending.DataID)
						continue
					}
					// Both have no DataID, prefer cache entry
					// DebugLog("[VFS getDirListWithCache] Skipping pending object in cache (same ID, both no DataID, prefer cache): dirID=%d, fileID=%d, name=%s", dirID, pending.ID, pending.Name)
					continue
				}

				// Check by name - if same name exists in cache, prefer cache entry if it has DataID
				if existing, exists := childrenMapByName[pending.Name]; exists {
					if existing.DataID > 0 {
						// Cached entry has DataID (from database), skip pending entry
						// DebugLog("[VFS getDirListWithCache] Skipping pending object in cache (same name in cache with DataID): dirID=%d, fileID=%d, name=%s, existingFileID=%d, existingDataID=%d", dirID, pending.ID, pending.Name, existing.ID, existing.DataID)
						continue
					}
					// Both are pending (no DataID), but existing is from cache
					// If pending has DataID, update cache entry
					if pending.DataID > 0 {
						existing.DataID = pending.DataID
						existing.Size = pending.Size
						DebugLog("[VFS getDirListWithCache] Updated cache entry with pending DataID (by name): dirID=%d, fileID=%d, name=%s, DataID=%d", dirID, pending.ID, pending.Name, pending.DataID)
						continue
					}
					// Both have no DataID, prefer cache entry
					if existing.ID != pending.ID {
						// DebugLog("[VFS getDirListWithCache] Skipping pending object in cache (same name, different ID, both no DataID, prefer cache entry): dirID=%d, fileID=%d, name=%s, existingFileID=%d", dirID, pending.ID, pending.Name, existing.ID)
						continue
					}
				}

				// New pending object not in cache, add it
				childrenMapByID[pending.ID] = pending
				childrenMapByName[pending.Name] = pending
				children = append(children, pending)
				DebugLog("[VFS getDirListWithCache] Added pending object to cached directory listing: dirID=%d, fileID=%d, name=%s, DataID=%d", dirID, pending.ID, pending.Name, pending.DataID)
			}

			// DebugLog("[VFS getDirListWithCache] Found in cache: dirID=%d, count=%d (with %d pending)", dirID, len(children), len(pendingChildren))
			return children, 0
		}
	}

	// Use singleflight to prevent duplicate concurrent requests
	// Convert dirID to string for singleflight key
	key := fmt.Sprintf("%d", dirID)
	result, err, _ := dirListSingleFlight.Do(key, func() (interface{}, error) {
		// Double-check cache after acquiring singleflight lock
		if cached, ok := dirListCache.Get(cacheKey); ok {
			if children, ok := cached.([]*core.ObjectInfo); ok && children != nil {
				// DebugLog("[VFS getDirListWithCache] Found in cache (after singleflight): dirID=%d, count=%d", dirID, len(children))
				return children, nil
			}
		}

		// List directory contents from database
		// DebugLog("[VFS getDirListWithCache] Querying database: dirID=%d", dirID)
		children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, dirID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		if err != nil {
			DebugLog("[VFS getDirListWithCache] ERROR: Failed to list directory: dirID=%d, error=%v", dirID, err)
			return nil, err
		}

		// CRITICAL: Merge with fileObjCache and RandomAccessor to avoid SQLite WAL dirty read
		// After Flush(), fileObjCache is immediately updated, but database may have WAL delay
		// We must merge fileObjCache data into directory listing to ensure consistency
		// ALSO: Check RandomAccessor registry for files that are still in memory (even if cache expired)
		for i, child := range children {
			var latestFileObj *core.ObjectInfo
			
			// Priority 1: Check fileObjCache (most recent, includes flushed data)
			if cached, ok := fileObjCache.Get(child.ID); ok {
				if fileObj, ok := cached.(*core.ObjectInfo); ok && fileObj != nil && fileObj.DataID > 0 {
					latestFileObj = fileObj
					DebugLog("[VFS getDirListWithCache] Found in fileObjCache: dirID=%d, fileID=%d, name=%s, size=%d, dataID=%d",
						dirID, child.ID, child.Name, fileObj.Size, fileObj.DataID)
				}
			}
			
			// Priority 2: Check RandomAccessor registry (for files still in memory, even if cache expired)
			// This handles the case where fileObjCache expired (30s TTL) but file is still being accessed
			if latestFileObj == nil {
				if ra := n.fs.getRandomAccessorByFileID(child.ID); ra != nil {
					if fileObj, err := ra.getFileObj(); err == nil && fileObj != nil && fileObj.DataID > 0 {
						latestFileObj = fileObj
						DebugLog("[VFS getDirListWithCache] Found in RandomAccessor: dirID=%d, fileID=%d, name=%s, size=%d, dataID=%d",
							dirID, child.ID, child.Name, fileObj.Size, fileObj.DataID)
					}
				}
			}
			
			// Update with latest data if found
			if latestFileObj != nil {
				// Update all relevant fields
				children[i].DataID = latestFileObj.DataID
				children[i].Size = latestFileObj.Size
				children[i].MTime = latestFileObj.MTime
				DebugLog("[VFS getDirListWithCache] Merged latest data into dir list: dirID=%d, fileID=%d, name=%s, size=%d, mtime=%d",
					dirID, child.ID, child.Name, latestFileObj.Size, latestFileObj.MTime)
			}
		}

		// Get pending objects from RandomAccessor registry
		// These are objects that are being written but not yet flushed to database
		pendingChildren := n.getPendingObjectsForDir(dirID)

		// Merge pending objects with database children
		// Strategy: Database entries are authoritative (already flushed, have complete metadata)
		// Pending entries are only added if they don't exist in database (by ID or by name)
		// If same file exists in both, prefer database entry (with DataID) over pending entry

		// Create maps to track existing children by ID and by name
		childrenMapByID := make(map[int64]*core.ObjectInfo)
		childrenMapByName := make(map[string]*core.ObjectInfo)

		// First, add all database children (these are authoritative)
		for _, child := range children {
			childrenMapByID[child.ID] = child
			// For name-based deduplication, prefer database entry (with DataID) over pending
			if existing, exists := childrenMapByName[child.Name]; !exists {
				childrenMapByName[child.Name] = child
			} else {
				// If same name exists, prefer the one with DataID (from database) over pending (DataID=0)
				if child.DataID > 0 && existing.DataID == 0 {
					childrenMapByName[child.Name] = child
				}
			}
		}

		// Then, add pending objects that are not already in database
		for _, pending := range pendingChildren {
			// Check by ID first - if same ID exists in database
			if existing, exists := childrenMapByID[pending.ID]; exists {
				// Same ID exists - but pending might have newer data (DataID, Size, MTime)
				// Update database entry with pending data if pending has DataID
				if pending.DataID > 0 && existing.DataID == 0 {
					existing.DataID = pending.DataID
					existing.Size = pending.Size
					existing.MTime = pending.MTime
					DebugLog("[VFS getDirListWithCache] Updated database entry with pending DataID: dirID=%d, fileID=%d, name=%s, DataID=%d, size=%d", dirID, pending.ID, pending.Name, pending.DataID, pending.Size)
					continue
				}
				// Database entry is authoritative (has DataID or both have no DataID), skip pending
				DebugLog("[VFS getDirListWithCache] Skipping pending object (same ID in database): dirID=%d, fileID=%d, name=%s, dbDataID=%d, pendingDataID=%d", dirID, pending.ID, pending.Name, existing.DataID, pending.DataID)
				continue
			}

			// Check by name - if same name exists in database
			if existing, exists := childrenMapByName[pending.Name]; exists {
				// If database entry has DataID, it's authoritative
				if existing.DataID > 0 {
					// Database entry has DataID (already flushed), skip pending entry
					DebugLog("[VFS getDirListWithCache] Skipping pending object (same name in database with DataID): dirID=%d, fileID=%d, name=%s, existingFileID=%d, existingDataID=%d", dirID, pending.ID, pending.Name, existing.ID, existing.DataID)
					continue
				}
				// Database entry has no DataID, but pending has DataID - update database entry
				if pending.DataID > 0 {
					existing.DataID = pending.DataID
					existing.Size = pending.Size
					existing.MTime = pending.MTime
					DebugLog("[VFS getDirListWithCache] Updated database entry with pending DataID (by name): dirID=%d, fileID=%d, name=%s, DataID=%d, size=%d", dirID, pending.ID, pending.Name, pending.DataID, pending.Size)
					continue
				}
				// Both have no DataID, prefer the one from database
				if existing.ID != pending.ID {
					DebugLog("[VFS getDirListWithCache] Skipping pending object (same name, different ID, prefer database entry): dirID=%d, fileID=%d, name=%s, existingFileID=%d", dirID, pending.ID, pending.Name, existing.ID)
					continue
				}
			}

			// New pending object not in database, add it
			childrenMapByID[pending.ID] = pending
			childrenMapByName[pending.Name] = pending
			children = append(children, pending)
			DebugLog("[VFS getDirListWithCache] Added pending object to directory listing: dirID=%d, fileID=%d, name=%s", dirID, pending.ID, pending.Name)
		}

		// Cache the result (including pending objects) with write lock
		mu := getDirListCacheMutex(cacheKey)
		mu.Lock()
		dirListCache.Put(cacheKey, children)
		mu.Unlock()
		// DebugLog("[VFS getDirListWithCache] Cached directory listing: dirID=%d, count=%d", dirID, len(children))

		return children, nil
	})

	if err != nil {
		return nil, syscall.EIO
	}

	children, ok := result.([]*core.ObjectInfo)
	if !ok {
		return nil, syscall.EIO
	}

	return children, 0
}

// getPendingObjectsForDir gets pending objects from RandomAccessor registry for a directory
// These are objects that are being written but not yet flushed to database
// Optimized: uses map for O(1) deduplication instead of O(n²) linear search
func (n *OrcasNode) getPendingObjectsForDir(dirID int64) []*core.ObjectInfo {
	// Use map for O(1) deduplication by fileID
	// Key: fileID, Value: *core.ObjectInfo
	pendingMap := make(map[int64]*core.ObjectInfo)

	// Get pending objects from RandomAccessor registry
	// These are files that are open for writing
	// IMPORTANT: Even if file has DataID (already flushed), if it's still in registry,
	// it might not be in database List yet (cache/race condition), so include it
	if n.fs != nil {
		n.fs.raRegistry.Range(func(key, value interface{}) bool {
			if fileID, ok := key.(int64); ok {
				if ra, ok := value.(*RandomAccessor); ok && ra != nil {
					// Get file object from RandomAccessor
					fileObj, err := ra.getFileObj()
					if err == nil && fileObj != nil && fileObj.PID == dirID {
						// Check if file has been marked for deletion (PID < 0 indicates deleted)
						// Skip deleted files from pending objects
						if fileObj.PID < 0 {
							DebugLog("[VFS getPendingObjectsForDir] Skipping deleted file in RandomAccessor registry: fileID=%d, name=%s, pid=%d", fileID, fileObj.Name, fileObj.PID)
							return true // Continue to next iteration
						}
						// Only add if not already in map
						if _, exists := pendingMap[fileID]; !exists {
							// Create a copy to avoid modifying the original
							// Note: Even if file has DataID, include it because:
							// 1. Database List might not have it yet (cache/race condition)
							// 2. RandomAccessor registry is authoritative for files being written
							pendingMap[fileID] = &core.ObjectInfo{
								ID:     fileObj.ID,
								PID:    fileObj.PID,
								Type:   core.OBJ_TYPE_FILE,
								Name:   fileObj.Name,
								Size:   fileObj.Size,
								DataID: fileObj.DataID, // Keep DataID if file is already flushed
								MTime:  fileObj.MTime,
							}
						}
					}
				}
			}
			return true
		})
	}

	// Convert map to slice
	// Pre-allocate slice with known capacity for better performance
	pendingObjects := make([]*core.ObjectInfo, 0, len(pendingMap))
	for _, obj := range pendingMap {
		pendingObjects = append(pendingObjects, obj)
	}

	return pendingObjects
}

// preloadChildDirs asynchronously preloads child directories
// This improves performance when navigating through directory trees
func (n *OrcasNode) preloadChildDirs(children []*core.ObjectInfo) {
	for _, child := range children {
		// Only preload directories, not files
		if child.Type != core.OBJ_TYPE_DIR {
			continue
		}

		// Check if already cached
		cacheKey := child.ID
		fileObjCache.Put(cacheKey, child)

		dirListCacheKey := n.getDirListCacheKey(child.ID)
		if _, ok := dirListCache.Get(dirListCacheKey); ok {
			// Already cached, skip
			continue
		}

		// Preload directory listing asynchronously
		// Use singleflight to prevent duplicate requests
		key := fmt.Sprintf("%d", child.ID)
		go func(dirID int64, dirListCacheKey int64, key string) {
			_, err, _ := dirListSingleFlight.Do(key, func() (interface{}, error) {
				// Double-check cache
				if _, ok := dirListCache.Get(dirListCacheKey); ok {
					return nil, nil
				}

				// List directory contents
				// DebugLog("[VFS preloadChildDirs] Preloading directory: dirID=%d", dirID)
				children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, dirID, core.ListOptions{
					Count: core.DefaultListPageSize,
				})
				if err != nil {
					DebugLog("[VFS preloadChildDirs] ERROR: Failed to preload directory: dirID=%d, error=%v", dirID, err)
					return nil, err
				}

				// Cache the result
				dirListCache.Put(dirListCacheKey, children)
				// DebugLog("[VFS preloadChildDirs] Preloaded directory: dirID=%d, count=%d", dirID, len(children))

				// Cache child objects for GetAttr optimization
				for _, grandchild := range children {
					grandchildCacheKey := grandchild.ID
					fileObjCache.Put(grandchildCacheKey, grandchild)
				}

				return nil, nil
			})
			if err != nil {
				DebugLog("[VFS preloadChildDirs] ERROR: Singleflight error: dirID=%d, error=%v", dirID, err)
			}
		}(child.ID, dirListCacheKey, key)
	}
}

// Create creates a file
func (n *OrcasNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	DebugLog("[VFS Create] Entry: name=%s, parentID=%d, flags=0x%x, mode=0%o", name, n.objID, flags, mode)
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Create] ERROR: checkKey failed: name=%s, parentID=%d, errno=%d", name, n.objID, errno)
		return nil, nil, 0, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Create] ERROR: Failed to get parent directory object: parentID=%d, name=%s, error=%v", n.objID, name, err)
		return nil, nil, 0, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Create] ERROR: Parent is not a directory: parentID=%d, name=%s, type=%d", n.objID, name, obj.Type)
		return nil, nil, 0, syscall.ENOTDIR
	}

	// Check for O_LARGEFILE flag (support for files > 2GB)
	hasLargeFileFlag := (flags & O_LARGEFILE) != 0
	if hasLargeFileFlag {
		DebugLog("[VFS Create] O_LARGEFILE flag set: name=%s, parentID=%d, flags=0x%x", name, obj.ID, flags)
	}

	DebugLog("[VFS Create] Creating file: name=%s, parentID=%d, flags=0x%x (O_LARGEFILE=0x%x), mode=0%o",
		name, obj.ID, flags, O_LARGEFILE, mode)

	// Check if file already exists
	// First check cache for directory listing to see if there's a directory with the same name
	parentCacheKey := n.getDirListCacheKey(obj.ID)
	var children []*core.ObjectInfo
	if cachedChildren, ok := dirListCache.Get(parentCacheKey); ok {
		if cachedList, ok := cachedChildren.([]*core.ObjectInfo); ok && cachedList != nil {
			// Check cache first for directory with same name
			for _, child := range cachedList {
				if child.Name == name && child.Type == core.OBJ_TYPE_DIR {
					// Found a directory with the same name in cache, verify from database
					DebugLog("[VFS Create] Found directory with same name in cache, verifying from database: name=%s, dirID=%d", name, child.ID)
					// Clear cache and query database to ensure accuracy
					dirListCache.Del(parentCacheKey)
					break
				}
			}
		}
	}

	// Query database to get accurate list
	children, _, _, err = n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		DebugLog("[VFS Create] ERROR: Failed to list directory children for parentID=%d: %v", obj.ID, err)
		return nil, nil, 0, syscall.EIO
	}

	var existingFileID int64
	var existingFileObj *core.ObjectInfo
	for _, child := range children {
		if child.Name == name {
			if child.Type == core.OBJ_TYPE_FILE {
				existingFileID = child.ID
				existingFileObj = child
				break
			} else if child.Type == core.OBJ_TYPE_DIR {
				// A directory with the same name exists, cannot create a file
				DebugLog("[VFS Create] ERROR: A directory with the same name already exists: name=%s, dirID=%d", name, child.ID)
				// Clear any cached directory object to prevent confusion
				dirCacheKey := child.ID
				fileObjCache.Del(dirCacheKey)
				return nil, nil, 0, syscall.EISDIR
			}
		}
	}

	// If file exists, check flags
	if existingFileID > 0 {
		// Check if O_EXCL is set (exclusive create, fail if exists)
		if flags&syscall.O_EXCL != 0 {
			return nil, nil, 0, syscall.EEXIST
		}

		// File exists and O_EXCL is not set, open existing file
		// Try to get from cache first
		cacheKey := existingFileID
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				// Verify that cached object is actually a file, not a directory
				// This prevents issues where cache might have incorrect type information
				if cachedObj.Type == core.OBJ_TYPE_FILE && cachedObj.ID == existingFileID {
					existingFileObj = cachedObj
				} else {
					// Cache has incorrect type information, invalidate it and fetch from database
					DebugLog("[VFS Create] WARNING: Cached object has incorrect type (expected FILE, got type=%d) or ID mismatch, invalidating cache: fileID=%d, cachedID=%d", cachedObj.Type, existingFileID, cachedObj.ID)
					fileObjCache.Del(cacheKey)
					existingFileObj = nil
				}
			}
		}

		// If cache miss or cache had incorrect type, get from database
		if existingFileObj == nil {
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{existingFileID})
			if err != nil {
				DebugLog("[VFS Create] ERROR: Failed to get existing file object: fileID=%d, error=%v", existingFileID, err)
				return nil, nil, 0, syscall.EIO
			}
			if len(objs) == 0 {
				DebugLog("[VFS Create] ERROR: Get returned empty result for existing file: fileID=%d", existingFileID)
				return nil, nil, 0, syscall.EIO
			}
			existingFileObj = objs[0]
			// Verify that the object from database is actually a file
			if existingFileObj.Type != core.OBJ_TYPE_FILE {
				DebugLog("[VFS Create] ERROR: Object from database is not a file (type=%d): fileID=%d", existingFileObj.Type, existingFileID)
				return nil, nil, 0, syscall.EISDIR
			}
		}

		// If O_TRUNC is set, handle file overwrite
		if flags&syscall.O_TRUNC != 0 {
			// Check if existing file is a .tmp file
			isExistingTmpFile := isTempFile(existingFileObj)

			if isExistingTmpFile {
				// Existing file is a .tmp file, delete it
				DebugLog("[VFS Create] Existing file is .tmp file, deleting it before overwrite: fileID=%d, name=%s", existingFileID, existingFileObj.Name)

				// Force flush before deletion (if RandomAccessor exists)
				if n.fs != nil {
					if targetRA := n.fs.getRandomAccessorByFileID(existingFileID); targetRA != nil {
						// Force flush before deletion
						if _, err := targetRA.ForceFlush(); err != nil {
							DebugLog("[VFS Create] WARNING: Failed to flush .tmp file before deletion: fileID=%d, error=%v", existingFileID, err)
						}
						// Unregister RandomAccessor
						n.fs.unregisterRandomAccessor(existingFileID, targetRA)
					}
				}

				// Delete the .tmp file
				err := n.fs.h.Delete(n.fs.c, n.fs.bktID, existingFileID)
				if err != nil {
					DebugLog("[VFS Create] ERROR: Failed to delete .tmp file: fileID=%d, error=%v", existingFileID, err)
					return nil, nil, 0, syscall.EIO
				}

				// Remove from caches
				fileObjCache.Del(existingFileID)
				n.invalidateDirListCache(obj.ID)

				// File was deleted, continue to create new file below
				existingFileID = 0
				existingFileObj = nil
			} else {
				// Existing file is not a .tmp file, but check if there are .tmp files with the same name in cache
				// This handles the case where .tmp files exist in the directory listing cache but weren't found in database query
				parentCacheKey := n.getDirListCacheKey(obj.ID)
				if cachedChildren, ok := dirListCache.Get(parentCacheKey); ok {
					if cachedList, ok := cachedChildren.([]*core.ObjectInfo); ok && cachedList != nil {
						// Look for .tmp files with the same name (without .tmp suffix matching the target name)
						tmpFileName := name + ".tmp"
						updatedChildren := make([]*core.ObjectInfo, 0, len(cachedList))
						for _, child := range cachedList {
							if isTempFile(child) && (child.Name == tmpFileName || child.Name == name) {
								// Found a .tmp file with matching name, delete it
								DebugLog("[VFS Create] Found .tmp file in cache with same name, deleting it: fileID=%d, name=%s, targetName=%s", child.ID, child.Name, name)

								// Force flush before deletion (if RandomAccessor exists)
								if n.fs != nil {
									if targetRA := n.fs.getRandomAccessorByFileID(child.ID); targetRA != nil {
										if _, err := targetRA.ForceFlush(); err != nil {
											DebugLog("[VFS Create] WARNING: Failed to flush cached .tmp file before deletion: fileID=%d, error=%v", child.ID, err)
										}
										n.fs.unregisterRandomAccessor(child.ID, targetRA)
									}
								}

								// Delete the .tmp file from database
								if err := n.fs.h.Delete(n.fs.c, n.fs.bktID, child.ID); err != nil {
									DebugLog("[VFS Create] WARNING: Failed to delete cached .tmp file: fileID=%d, error=%v", child.ID, err)
								} else {
									DebugLog("[VFS Create] Successfully deleted cached .tmp file: fileID=%d, name=%s", child.ID, child.Name)
								}

								// Remove from file object cache
								fileObjCache.Del(child.ID)
								// Skip adding this .tmp file to updated children list
								continue
							}
							updatedChildren = append(updatedChildren, child)
						}

						// Update cache with filtered children (without .tmp files)
						if len(updatedChildren) != len(cachedList) {
							dirListCache.Put(parentCacheKey, updatedChildren)
							DebugLog("[VFS Create] Updated directory cache, removed .tmp files with same name: removedCount=%d", len(cachedList)-len(updatedChildren))
						}
					}
				}

				// Create version before truncating
				// This preserves file history for non-.tmp files
				if lh, ok := n.fs.h.(*core.LocalHandler); ok {
					err := lh.CreateVersionFromFile(n.fs.c, n.fs.bktID, existingFileID)
					if err != nil {
						// Log error but continue with truncate (don't fail the operation)
						DebugLog("[VFS Create] WARNING: Failed to create version from existing file: fileID=%d, error=%v", existingFileID, err)
					} else {
						DebugLog("[VFS Create] Created version from existing file before truncate: fileID=%d", existingFileID)
					}
				}

				// Truncate file to size 0
				existingFileObj.Size = 0
				existingFileObj.DataID = 0
				existingFileObj.MTime = core.Now()
				_, err := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{existingFileObj})
				if err != nil {
					DebugLog("[VFS Create] ERROR: Failed to truncate existing file: fileID=%d, error=%v", existingFileID, err)
					return nil, nil, 0, syscall.EIO
				}
			}
		}

		// If existingFileID is still > 0, create file node for existing file
		if existingFileID == 0 {
			// File was deleted (was a .tmp file), continue to create new file below
			existingFileObj = nil
		} else {
			// Create file node for existing file
			fileNode := &OrcasNode{
				fs:    n.fs,
				objID: existingFileObj.ID,
			}
			fileNode.obj.Store(existingFileObj)

			stableAttr := fs.StableAttr{
				Mode: syscall.S_IFREG,
				Ino:  uint64(existingFileObj.ID),
			}

			fileInode := n.NewInode(ctx, fileNode, stableAttr)

			// Fill EntryOut
			out.Mode = getModeFromObj(existingFileObj)
			out.Size = uint64(existingFileObj.Size)
			out.Mtime = uint64(existingFileObj.MTime)
			out.Ctime = out.Mtime
			out.Atime = out.Mtime
			out.Ino = uint64(existingFileObj.ID)

			return fileInode, fileNode, 0, 0
		}
	}

	// File doesn't exist, check if O_CREAT is set
	// O_CREAT is required to create a new file
	if flags&syscall.O_CREAT == 0 {
		// O_CREAT not set, cannot create file
		return nil, nil, 0, syscall.ENOENT
	}

	// Before creating new file, check if cache has a directory with the same name
	// If so, clear it to ensure we create a file, not a directory
	// This prevents issues where cache might have incorrect type information
	// parentCacheKey is already declared above (using getDirListCacheKey)
	if cachedChildren, ok := dirListCache.Get(parentCacheKey); ok {
		if children, ok := cachedChildren.([]*core.ObjectInfo); ok && children != nil {
			for _, child := range children {
				if child.Name == name && child.Type == core.OBJ_TYPE_DIR {
					// Found a directory with the same name in cache, clear directory listing cache
					DebugLog("[VFS Create] WARNING: Found directory with same name in cache, clearing cache to ensure file creation: name=%s, dirID=%d", name, child.ID)
					dirListCache.Del(parentCacheKey)
					// Also clear the directory object cache if it exists
					dirCacheKey := child.ID
					fileObjCache.Del(dirCacheKey)
					// Also invalidate the directory node's local cache if it exists
					// This ensures that if the directory node is already created, it will be refreshed
					dirNode := &OrcasNode{
						fs:    n.fs,
						objID: child.ID,
					}
					dirNode.invalidateObj()
					break
				}
			}
		}
	}

	// Also check if any cached object with the same name exists (from fileObjCache)
	// We need to iterate through all cached objects, but that's expensive
	// Instead, we'll rely on the List query above and directory listing cache check
	// If a directory with the same name exists in database, List will return it and we'll handle it above

	// File doesn't exist and O_CREAT is set, create new file
	fileObj := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   obj.ID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  name,
		Size:  0,
		MTime: core.Now(),
	}
	// Save mode (permissions)
	setModeInObj(fileObj, mode)

	ids, err := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		// Check if it's a duplicate key error (concurrent create with same name)
		if err == core.ERR_DUP_KEY {
			DebugLog("[VFS Create] Duplicate key error (concurrent create), re-querying database: name=%s, parentID=%d", name, obj.ID)
			// Re-query database to get the existing file
			children, _, _, listErr := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
				Count: core.DefaultListPageSize,
			})
			if listErr == nil {
				for _, child := range children {
					if child.Name == name && child.Type == core.OBJ_TYPE_FILE {
						// Found existing file, use it
						existingFileID = child.ID
						existingFileObj = child
						DebugLog("[VFS Create] Found existing file from re-query: name=%s, fileID=%d", name, existingFileID)
						break
					}
				}
			}
			// If found existing file, continue with existing file logic below
			if existingFileID > 0 {
				// Fall through to existing file handling
			} else {
				DebugLog("[VFS Create] ERROR: Duplicate key error but file not found in re-query: name=%s, parentID=%d", name, obj.ID)
				return nil, nil, 0, syscall.EIO
			}
		} else {
			DebugLog("[VFS Create] ERROR: Failed to put file object to database: name=%s, parentID=%d, error=%v", name, obj.ID, err)
			return nil, nil, 0, syscall.EIO
		}
	} else if len(ids) == 0 || ids[0] == 0 {
		// Put succeeded but returned zero ID - likely duplicate key conflict
		DebugLog("[VFS Create] WARNING: Put returned empty or zero ID (likely duplicate key), re-querying database and pending objects: name=%s, parentID=%d, ids=%v", name, obj.ID, ids)

		// First, check pending objects from RandomAccessor registry
		// File might be created by another goroutine but not yet flushed to database
		pendingChildren := n.getPendingObjectsForDir(obj.ID)
		for _, pending := range pendingChildren {
			if pending.Name == name && pending.Type == core.OBJ_TYPE_FILE {
				// Found existing file in pending objects, use it
				existingFileID = pending.ID
				existingFileObj = pending
				DebugLog("[VFS Create] Found existing file in pending objects: name=%s, fileID=%d", name, existingFileID)
				break
			}
		}

		// If not found in pending objects, query database (including deleted files)
		// Note: List only returns non-deleted files (PID >= 0), but unique constraint
		// includes deleted files (PID < 0), so we need to query directly
		if existingFileID == 0 {
			// First try List (non-deleted files)
			children, _, _, listErr := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
				Count: core.DefaultListPageSize,
			})
			if listErr == nil {
				for _, child := range children {
					if child.Name == name && child.Type == core.OBJ_TYPE_FILE {
						// Found existing file, use it
						existingFileID = child.ID
						existingFileObj = child
						DebugLog("[VFS Create] Found existing file from database re-query: name=%s, fileID=%d", name, existingFileID)
						break
					}
				}
			}

			// If still not found, query directly from database (including deleted files)
			// This handles the case where file was deleted (PID < 0) but still conflicts
			if existingFileID == 0 {
				existingFileID, existingFileObj = n.queryFileByNameDirectly(obj.ID, name)
				if existingFileID > 0 {
					DebugLog("[VFS Create] Found existing file from direct database query (including deleted): name=%s, fileID=%d, PID=%d", name, existingFileID, existingFileObj.PID)
				}
			}
		}

		// If still not found, try with retry and delay (file might be in transaction)
		if existingFileID == 0 {
			maxRetries := 3
			for retry := 0; retry < maxRetries; retry++ {
				// Wait a bit for transaction to commit
				time.Sleep(time.Duration(retry+1) * 50 * time.Millisecond)

				// Check pending objects again
				pendingChildren = n.getPendingObjectsForDir(obj.ID)
				for _, pending := range pendingChildren {
					if pending.Name == name && pending.Type == core.OBJ_TYPE_FILE {
						existingFileID = pending.ID
						existingFileObj = pending
						DebugLog("[VFS Create] Found existing file in pending objects after retry %d: name=%s, fileID=%d", retry+1, name, existingFileID)
						break
					}
				}
				if existingFileID > 0 {
					break
				}

				// Query database again (including deleted files)
				children, _, _, listErr := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
					Count: core.DefaultListPageSize,
				})
				if listErr == nil {
					for _, child := range children {
						if child.Name == name && child.Type == core.OBJ_TYPE_FILE {
							existingFileID = child.ID
							existingFileObj = child
							DebugLog("[VFS Create] Found existing file from database after retry %d: name=%s, fileID=%d", retry+1, name, existingFileID)
							break
						}
					}
				}

				// If still not found, query directly from database (including deleted files)
				if existingFileID == 0 {
					existingFileID, existingFileObj = n.queryFileByNameDirectly(obj.ID, name)
					if existingFileID > 0 {
						DebugLog("[VFS Create] Found existing file from direct database query after retry %d (including deleted): name=%s, fileID=%d, PID=%d", retry+1, name, existingFileID, existingFileObj.PID)
					}
				}
				if existingFileID > 0 {
					break
				}
			}
		}

		// If still not found after retries, return error
		if existingFileID == 0 {
			DebugLog("[VFS Create] ERROR: Put returned zero ID and file not found after retries: name=%s, parentID=%d", name, obj.ID)
			return nil, nil, 0, syscall.EIO
		}
	} else {
		// Success - use the returned ID
		fileObj.ID = ids[0]
		DebugLog("[VFS Create] Successfully created file: name=%s, fileID=%d, parentID=%d", name, fileObj.ID, obj.ID)
		// Record file creation operation for save pattern detection
		n.fs.recordFileOperation(OpCreate, fileObj.ID, name, obj.ID, 0, 0, "", 0)
		// Continue with new file creation logic below (existingFileID is already 0)
	}

	// If we found an existing file (from duplicate key or zero ID), handle it
	if existingFileID > 0 {
		// Get file object if not already set
		if existingFileObj == nil {
			cacheKey := existingFileID
			if cached, ok := fileObjCache.Get(cacheKey); ok {
				if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil && cachedObj.Type == core.OBJ_TYPE_FILE {
					existingFileObj = cachedObj
				}
			}
		}
		if existingFileObj == nil {
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{existingFileID})
			if err == nil && len(objs) > 0 {
				existingFileObj = objs[0]
			}
		}
		if existingFileObj == nil {
			DebugLog("[VFS Create] ERROR: Found existing file ID but failed to get file object: fileID=%d", existingFileID)
			return nil, nil, 0, syscall.EIO
		}

		// Check if O_EXCL is set (exclusive create, fail if exists)
		if flags&syscall.O_EXCL != 0 {
			return nil, nil, 0, syscall.EEXIST
		}

		// If O_TRUNC is set, handle file overwrite
		if flags&syscall.O_TRUNC != 0 {
			// Check if existing file is a .tmp file
			isExistingTmpFile := isTempFile(existingFileObj)

			if isExistingTmpFile {
				// Existing file is a .tmp file, delete it
				DebugLog("[VFS Create] Existing file is .tmp file, deleting it before overwrite: fileID=%d, name=%s", existingFileID, existingFileObj.Name)

				// Force flush before deletion (if RandomAccessor exists)
				if n.fs != nil {
					if targetRA := n.fs.getRandomAccessorByFileID(existingFileID); targetRA != nil {
						// Force flush before deletion
						if _, err := targetRA.ForceFlush(); err != nil {
							DebugLog("[VFS Create] WARNING: Failed to flush .tmp file before deletion: fileID=%d, error=%v", existingFileID, err)
						}
						// Unregister RandomAccessor
						n.fs.unregisterRandomAccessor(existingFileID, targetRA)
					}
				}

				// Delete the .tmp file
				err := n.fs.h.Delete(n.fs.c, n.fs.bktID, existingFileID)
				if err != nil {
					DebugLog("[VFS Create] ERROR: Failed to delete .tmp file: fileID=%d, error=%v", existingFileID, err)
					return nil, nil, 0, syscall.EIO
				}

				// Remove from caches
				fileObjCache.Del(existingFileID)
				n.invalidateDirListCache(obj.ID)

				// File was deleted, continue to create new file below
				existingFileID = 0
				existingFileObj = nil
			} else {
				// Existing file is not a .tmp file, but check if there are .tmp files with the same name in cache
				// This handles the case where .tmp files exist in the directory listing cache but weren't found in database query
				parentCacheKey := n.getDirListCacheKey(obj.ID)
				if cachedChildren, ok := dirListCache.Get(parentCacheKey); ok {
					if cachedList, ok := cachedChildren.([]*core.ObjectInfo); ok && cachedList != nil {
						// Look for .tmp files with the same name (without .tmp suffix matching the target name)
						tmpFileName := name + ".tmp"
						updatedChildren := make([]*core.ObjectInfo, 0, len(cachedList))
						for _, child := range cachedList {
							if isTempFile(child) && (child.Name == tmpFileName || child.Name == name) {
								// Found a .tmp file with matching name, delete it
								DebugLog("[VFS Create] Found .tmp file in cache with same name, deleting it: fileID=%d, name=%s, targetName=%s", child.ID, child.Name, name)

								// Force flush before deletion (if RandomAccessor exists)
								if n.fs != nil {
									if targetRA := n.fs.getRandomAccessorByFileID(child.ID); targetRA != nil {
										if _, err := targetRA.ForceFlush(); err != nil {
											DebugLog("[VFS Create] WARNING: Failed to flush cached .tmp file before deletion: fileID=%d, error=%v", child.ID, err)
										}
										n.fs.unregisterRandomAccessor(child.ID, targetRA)
									}
								}

								// Delete the .tmp file from database
								if err := n.fs.h.Delete(n.fs.c, n.fs.bktID, child.ID); err != nil {
									DebugLog("[VFS Create] WARNING: Failed to delete cached .tmp file: fileID=%d, error=%v", child.ID, err)
								} else {
									DebugLog("[VFS Create] Successfully deleted cached .tmp file: fileID=%d, name=%s", child.ID, child.Name)
								}

								// Remove from file object cache
								fileObjCache.Del(child.ID)
								// Skip adding this .tmp file to updated children list
								continue
							}
							updatedChildren = append(updatedChildren, child)
						}

						// Update cache with filtered children (without .tmp files)
						if len(updatedChildren) != len(cachedList) {
							dirListCache.Put(parentCacheKey, updatedChildren)
							DebugLog("[VFS Create] Updated directory cache, removed .tmp files with same name: removedCount=%d", len(cachedList)-len(updatedChildren))
						}
					}
				}

				// Create version before truncating
				// This preserves file history for non-.tmp files
				if lh, ok := n.fs.h.(*core.LocalHandler); ok {
					err := lh.CreateVersionFromFile(n.fs.c, n.fs.bktID, existingFileID)
					if err != nil {
						// Log error but continue with truncate (don't fail the operation)
						DebugLog("[VFS Create] WARNING: Failed to create version from existing file: fileID=%d, error=%v", existingFileID, err)
					} else {
						DebugLog("[VFS Create] Created version from existing file before truncate: fileID=%d", existingFileID)
					}
				}

				// Truncate file to size 0
				existingFileObj.Size = 0
				existingFileObj.DataID = 0
				existingFileObj.MTime = core.Now()
				_, err := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{existingFileObj})
				if err != nil {
					DebugLog("[VFS Create] ERROR: Failed to truncate existing file: fileID=%d, error=%v", existingFileID, err)
					return nil, nil, 0, syscall.EIO
				}
			}
		}

		// If existingFileID is still > 0, create file node for existing file
		if existingFileID == 0 {
			// File was deleted (was a .tmp file), continue to create new file below
			existingFileObj = nil
		} else {
			// Create file node for existing file
			fileNode := &OrcasNode{
				fs:    n.fs,
				objID: existingFileObj.ID,
			}
			fileNode.obj.Store(existingFileObj)

			stableAttr := fs.StableAttr{
				Mode: syscall.S_IFREG,
				Ino:  uint64(existingFileObj.ID),
			}

			fileInode := n.NewInode(ctx, fileNode, stableAttr)

			// Fill EntryOut
			out.Mode = getModeFromObj(existingFileObj)
			out.Size = uint64(existingFileObj.Size)
			out.Mtime = uint64(existingFileObj.MTime)
			out.Ctime = out.Mtime
			out.Atime = out.Mtime
			out.Ino = uint64(existingFileObj.ID)

			return fileInode, fileNode, 0, 0
		}
	}

	// New file was created successfully, continue with file node creation

	// Before caching new file, ensure any directory with the same name is removed from cache
	// This is critical to prevent the file from being incorrectly identified as a directory
	// Check directory listing cache and remove any directory with the same name
	// parentCacheKey is already set above using getDirListCacheKey
	if cachedChildren, ok := dirListCache.Get(parentCacheKey); ok {
		if children, ok := cachedChildren.([]*core.ObjectInfo); ok && children != nil {
			updatedChildren := make([]*core.ObjectInfo, 0, len(children))
			for _, child := range children {
				if child.Name == name && child.Type == core.OBJ_TYPE_DIR {
					// Remove directory with same name from cache
					DebugLog("[VFS Create] Removing directory with same name from cache: name=%s, dirID=%d", name, child.ID)
					dirCacheKey := child.ID
					fileObjCache.Del(dirCacheKey)
					// Invalidate the directory node's local cache
					dirNode := &OrcasNode{
						fs:    n.fs,
						objID: child.ID,
					}
					dirNode.invalidateObj()
					// Don't add this directory to updatedChildren
					continue
				}
				updatedChildren = append(updatedChildren, child)
			}
			// Update directory listing cache with filtered children (directory removed)
			if len(updatedChildren) != len(children) {
				dirListCache.Put(parentCacheKey, updatedChildren)
				DebugLog("[VFS Create] Updated directory listing cache, removed directory with same name: name=%s", name)
			}
		}
	}

	// Cache new file object for GetAttr optimization
	cacheKey := fileObj.ID
	fileObjCache.Put(cacheKey, fileObj)
	n.invalidateDirListCache(obj.ID)

	// Create file node
	fileNode := &OrcasNode{
		fs:    n.fs,
		objID: fileObj.ID,
	}
	fileNode.obj.Store(fileObj)

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
		Ino:  uint64(fileObj.ID),
	}

	fileInode := n.NewInode(ctx, fileNode, stableAttr)

	// Fill EntryOut
	out.Mode = getModeFromObj(fileObj)
	out.Size = 0
	out.Mtime = uint64(fileObj.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime
	out.Ino = uint64(fileObj.ID)

	// Invalidate parent directory cache (object metadata may change later)
	n.invalidateObj()

	return fileInode, fileNode, 0, 0
}

// Open opens a file
func (n *OrcasNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	DebugLog("[VFS Open] Entry: objID=%d, flags=0x%x", n.objID, flags)
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Open] ERROR: checkKey failed: objID=%d, flags=0x%x, errno=%d", n.objID, flags, errno)
		return nil, 0, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Open] ERROR: Failed to get object: objID=%d, error=%v, flags=0x%x", n.objID, err, flags)
		// If O_CREAT is set and file doesn't exist, return ENOENT
		// The caller should use Create() instead, but some applications may try Open with O_CREAT
		// In FUSE, Open() is called after Create() or Lookup(), so if we get here with ENOENT,
		// it means the file doesn't exist and Create() should have been called first
		return nil, 0, syscall.ENOENT
	}

	DebugLog("[VFS Open] Object info: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d, flags=0x%x",
		obj.ID, obj.Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, obj.Name, obj.PID, flags)

	if obj.Type != core.OBJ_TYPE_FILE {
		DebugLog("[VFS Open] ERROR: Object is not a file (type=%d, expected FILE=%d): objID=%d, name=%s, PID=%d",
			obj.Type, core.OBJ_TYPE_FILE, obj.ID, obj.Name, obj.PID)
		// Check cache to see what's stored
		cacheKey := obj.ID
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				DebugLog("[VFS Open] Cached object info: objID=%d, type=%d, name=%s, PID=%d",
					cachedObj.ID, cachedObj.Type, cachedObj.Name, cachedObj.PID)
			}
		}
		// Check local cache
		if val := n.obj.Load(); val != nil {
			if localObj, ok := val.(*core.ObjectInfo); ok && localObj != nil {
				DebugLog("[VFS Open] Local cached object info: objID=%d, type=%d, name=%s, PID=%d",
					localObj.ID, localObj.Type, localObj.Name, localObj.PID)
			}
		}
		// Query database directly to verify
		objs, dbErr := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{obj.ID})
		if dbErr == nil && len(objs) > 0 {
			dbObj := objs[0]
			DebugLog("[VFS Open] Database object info: objID=%d, type=%d, name=%s, PID=%d",
				dbObj.ID, dbObj.Type, dbObj.Name, dbObj.PID)
		}
		return nil, 0, syscall.EISDIR
	}

	// Check if O_TRUNC is set - if so, truncate file to size 0
	// Note: FUSE will also call Setattr after Open, but we handle it here proactively
	// to ensure the file is ready for writing immediately
	if flags&syscall.O_TRUNC != 0 {
		DebugLog("[VFS Open] O_TRUNC flag set, truncating file: fileID=%d", obj.ID)
		if errno := n.truncateFile(0); errno != 0 {
			DebugLog("[VFS Open] ERROR: Failed to truncate file: fileID=%d, errno=%d", obj.ID, errno)
			return nil, 0, errno
		}
		// Update object size in both local and global cache
		obj.Size = 0
		n.obj.Store(obj)
		// Also update global cache to ensure getObj() returns correct size
		cacheKey := obj.ID
		fileObjCache.Put(cacheKey, obj)
		// Invalidate cache
		n.invalidateObj()
	}

	// RandomAccessor is now created lazily on the first write.
	// Even when the file is opened RDWR, we defer creation so that pure reads
	// still go directly to the underlying data without touching RA state.

	// Check for O_LARGEFILE flag (support for files > 2GB)
	hasLargeFileFlag := (flags & O_LARGEFILE) != 0
	if hasLargeFileFlag {
		DebugLog("[VFS Open] O_LARGEFILE flag set: fileID=%d, flags=0x%x", obj.ID, flags)
	}

	// Increment reference count when file is opened
	DebugLog("[VFS Open] Opened file: fileID=%d, flags=0x%x (O_WRONLY=0x%x, O_RDWR=0x%x, O_RDONLY=0x%x, O_LARGEFILE=0x%x)",
		obj.ID, flags, syscall.O_WRONLY, syscall.O_RDWR, syscall.O_RDONLY, O_LARGEFILE)

	// Check if file is opened for writing
	// Note: macOS/SMB may use different flag combinations
	// O_WRONLY = 0x1, O_RDWR = 0x2, but some systems may use other flags
	// Also check if O_CREAT is set (0x200) which indicates write intent
	// O_TRUNC (0x400) also indicates write intent
	// O_EXCL (0x800) is often used with O_CREAT for exclusive creation, but in SMB context
	// it may be used alone to indicate write intent after file creation
	isWriteMode := (flags&syscall.O_WRONLY != 0) || (flags&syscall.O_RDWR != 0) ||
		(flags&syscall.O_CREAT != 0) || (flags&syscall.O_TRUNC != 0) || (flags&syscall.O_EXCL != 0)
	DebugLog("[VFS Open] File open mode: fileID=%d, isWriteMode=%v, hasLargeFileFlag=%v, flags=0x%x (O_WRONLY=0x%x, O_RDWR=0x%x, O_CREAT=0x%x, O_TRUNC=0x%x, O_EXCL=0x%x)",
		obj.ID, isWriteMode, hasLargeFileFlag, flags, syscall.O_WRONLY, syscall.O_RDWR, syscall.O_CREAT, syscall.O_TRUNC, syscall.O_EXCL)

	// Note: O_LARGEFILE is supported - we already support large files (>2GB) by default
	// This flag is mainly for compatibility with 32-bit applications

	// Return the node itself as FileHandle
	// This allows Write/Read operations to work on the file
	// Note: fuseFlags=0 means default behavior (no special flags)
	// FUSE will call Write/Read methods on the FileHandle if they are implemented
	// IMPORTANT: For macOS/SMB, we need to ensure Write is always available
	// even if flags don't explicitly indicate write mode, because SMB may use
	// different flag combinations that don't match standard POSIX flags

	// Set fuseFlags to indicate write capability if file was opened with write intent
	// Note: fuseFlags is already declared in function signature, so we just set it
	if isWriteMode {
		// Indicate that this FileHandle supports writing
		// This helps FUSE know that Write operations are available
		DebugLog("[VFS Open] File opened with write intent, Write operations will be available: fileID=%d", obj.ID)
	}

	// Print file metadata after Open completes
	DebugLog("[VFS Open] File metadata after Open: fileID=%d, name=%s, size=%d, dataID=%d, mtime=%d, mode=0%o, type=%d, pid=%d",
		obj.ID, obj.Name, obj.Size, obj.DataID, obj.MTime, getModeFromObj(obj), obj.Type, obj.PID)

	DebugLog("[VFS Open] Returning FileHandle: fileID=%d, fuseFlags=0x%x, FileHandle type=%T, implements FileWriter=%v",
		obj.ID, 0, n, true)
	return n, 0, 0
}

// Mkdir creates a directory
func (n *OrcasNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	DebugLog("[VFS Mkdir] Entry: name=%s, parentID=%d, mode=0%o", name, n.objID, mode)
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Mkdir] ERROR: checkKey failed: name=%s, parentID=%d, errno=%d", name, n.objID, errno)
		return nil, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Mkdir] ERROR: Failed to get parent object: name=%s, parentID=%d, error=%v", name, n.objID, err)
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Mkdir] ERROR: Parent is not a directory: name=%s, parentID=%d, type=%d", name, n.objID, obj.Type)
		return nil, syscall.ENOTDIR
	}

	// Check if a file or directory with the same name already exists
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		DebugLog("[VFS Mkdir] ERROR: Failed to list directory children for parentID=%d: %v", obj.ID, err)
		return nil, syscall.EIO
	}

	for _, child := range children {
		if child.Name == name {
			if child.Type == core.OBJ_TYPE_DIR {
				// Directory already exists
				DebugLog("[VFS Mkdir] ERROR: Directory already exists: name=%s, dirID=%d", name, child.ID)
				return nil, syscall.EEXIST
			} else if child.Type == core.OBJ_TYPE_FILE {
				// A file with the same name exists, cannot create a directory
				DebugLog("[VFS Mkdir] ERROR: A file with the same name already exists: name=%s, fileID=%d", name, child.ID)
				return nil, syscall.ENOTDIR
			}
		}
	}

	// Create directory object
	dirObj := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   obj.ID,
		Type:  core.OBJ_TYPE_DIR,
		Name:  name,
		Size:  0,
		MTime: core.Now(),
	}
	// Save mode (permissions)
	setModeInObj(dirObj, mode)

	ids, err := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{dirObj})
	if err != nil || len(ids) == 0 || ids[0] == 0 {
		return nil, syscall.EIO
	}

	dirObj.ID = ids[0]

	// Cache new directory object for GetAttr optimization
	cacheKey := dirObj.ID
	fileObjCache.Put(cacheKey, dirObj)

	// Invalidate parent directory listing cache
	n.invalidateDirListCache(obj.ID)

	// IMPORTANT: Clear readdirCache and mark as stale to ensure Readdir sees the new directory
	// This is critical because Readdir checks readdirCache first, and if it exists,
	// it returns cached entries without checking dirListCache
	parentCacheKey := obj.ID
	readdirCache.Del(parentCacheKey)
	readdirCacheStale.Store(parentCacheKey, true)
	DebugLog("[VFS Mkdir] Cleared readdirCache and marked as stale for parent directory: parentID=%d, newDirID=%d, name=%s", obj.ID, dirObj.ID, name)

	// Create directory node
	dirNode := &OrcasNode{
		fs:    n.fs,
		objID: dirObj.ID,
	}
	dirNode.obj.Store(dirObj)

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
		Ino:  uint64(dirObj.ID),
	}

	dirInode := n.NewInode(ctx, dirNode, stableAttr)

	// Fill EntryOut
	out.Mode = getModeFromObj(dirObj)
	out.Size = 0
	out.Mtime = uint64(dirObj.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime
	out.Ino = uint64(dirObj.ID)

	// Invalidate parent directory cache (metadata may be updated later)
	n.invalidateObj()

	return dirInode, 0
}

// Unlink deletes a file
func (n *OrcasNode) Unlink(ctx context.Context, name string) syscall.Errno {
	DebugLog("[VFS Unlink] Entry: name=%s, parentID=%d", name, n.objID)
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Unlink] ERROR: checkKey failed: name=%s, parentID=%d, errno=%d", name, n.objID, errno)
		return errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Unlink] ERROR: Failed to get parent object: name=%s, parentID=%d, error=%v", name, n.objID, err)
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Unlink] ERROR: Parent is not a directory: name=%s, parentID=%d, type=%d", name, n.objID, obj.Type)
		return syscall.ENOTDIR
	}

	// Find child object
	// IMPORTANT: Also check RandomAccessor registry for files that are being written
	// (especially .tmp files) that may not be in List results yet
	var targetID int64
	var targetObj *core.ObjectInfo

	// First, try to find from RandomAccessor registry (for files being written, especially .tmp files)
	if n.fs != nil {
		n.fs.raRegistry.Range(func(key, value interface{}) bool {
			if fileID, ok := key.(int64); ok {
				if ra, ok := value.(*RandomAccessor); ok && ra != nil {
					fileObj, err := ra.getFileObj()
					if err == nil && fileObj != nil && fileObj.PID == obj.ID && fileObj.Name == name && fileObj.Type == core.OBJ_TYPE_FILE {
						targetID = fileID
						targetObj = fileObj
						DebugLog("[VFS Unlink] Found target file from RandomAccessor registry: fileID=%d, name=%s", targetID, name)
						return false // Stop iteration
					}
				}
			}
			return true // Continue iteration
		})
	}

	// If not found in RandomAccessor registry, try to find from List
	if targetID == 0 {
		children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		if err != nil {
			DebugLog("[VFS Unlink] ERROR: Failed to list directory children: name=%s, parentID=%d, error=%v", name, obj.ID, err)
			return syscall.EIO
		}

		for _, child := range children {
			if child.Name == name && child.Type == core.OBJ_TYPE_FILE {
				targetID = child.ID
				targetObj = child
				DebugLog("[VFS Unlink] Found target file from List: fileID=%d, name=%s", targetID, name)
				break
			}
		}
	}

	if targetID == 0 {
		DebugLog("[VFS Unlink] ERROR: Target file not found: name=%s, parentID=%d", name, obj.ID)
		return syscall.ENOENT
	}

	// Step 0: Check if this delete should be intercepted for save pattern merge
	if n.fs.savePatternDetector != nil {
		if shouldIntercept, match := n.fs.savePatternDetector.ShouldInterceptDelete(targetID, name, obj.ID); shouldIntercept {
			// Intercept delete operation, mark for pending merge
			if err := n.fs.savePatternDetector.HandleDeleteWithMerge(match); err != nil {
				DebugLog("[VFS Unlink] Failed to handle delete with merge: %v", err)
				return syscall.EIO
			}
			// Successfully intercepted, return success without actually deleting
			DebugLog("[VFS Unlink] Intercepted delete for save pattern merge: name=%s, fileID=%d", name, targetID)
			return 0
		}
	}

	// Step 1: Remove from RandomAccessor registry if present
	// This ensures the file is removed from pending objects before deletion

	// Remove from RandomAccessor registry if present
	if n.fs != nil {
		if targetRA := n.fs.getRandomAccessorByFileID(targetID); targetRA != nil {
			// Force flush before deletion to ensure data is saved
			if _, err := targetRA.ForceFlush(); err != nil {
				DebugLog("[VFS Unlink] WARNING: Failed to flush file before deletion: fileID=%d, error=%v", targetID, err)
			}
			// Unregister RandomAccessor
			n.fs.unregisterRandomAccessor(targetID, targetRA)
			DebugLog("[VFS Unlink] Removed file from RandomAccessor registry: fileID=%d", targetID)
		}
	}

	// Step 1: Remove from parent directory first (mark as deleted)
	// This makes the file disappear from parent's listing immediately
	err = n.fs.h.Recycle(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		DebugLog("[VFS Unlink] ERROR: Failed to recycle file: fileID=%d, name=%s, parentID=%d, error=%v", targetID, name, obj.ID, err)
		return syscall.EIO
	}

	// Record delete operation for save pattern detection
	n.fs.recordFileOperation(OpDelete, targetID, name, obj.ID, 0, 0, "", 0)

	// Step 2: Update cache immediately
	// CRITICAL: Clear all caches for the deleted file to prevent data corruption
	// If a file with the same name is recreated, it should get a new DataID and DataInfo
	// Clearing caches ensures we don't reuse old DataID or DataInfo from deleted file
	n.invalidateDirListCache(obj.ID)

	// Get file object to get DataID before clearing cache
	// Use targetObj from above if available, otherwise fetch from database
	if targetObj == nil {
		targetObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{targetID})
		if err == nil && len(targetObjs) > 0 {
			targetObj = targetObjs[0]
		}
	}
	if targetObj != nil && targetObj.DataID > 0 && targetObj.DataID != core.EmptyDataID {
		// Clear DataInfo cache for the deleted file's DataID
		dataInfoCacheKey := targetObj.DataID
		dataInfoCache.Del(dataInfoCacheKey)
		decodingReaderCache.Del(dataInfoCacheKey)
		DebugLog("[VFS Unlink] Cleared DataInfo cache for deleted file: fileID=%d, dataID=%d", targetID, dataInfoCacheKey)
	}

	// Clear file object cache
	fileObjCache.Del(targetID)
	DebugLog("[VFS Unlink] Cleared file object cache for deleted file: fileID=%d", targetID)

	// Invalidate parent directory cache
	n.invalidateObj()

	// Step 3: Asynchronously delete and clean up (permanent deletion)
	// This includes physical deletion of data files and metadata
	go func() {
		// Use the original context to preserve authentication information
		// Context is read-only and safe to use in goroutines
		err := n.fs.h.Delete(n.fs.c, n.fs.bktID, targetID)
		if err != nil {
			DebugLog("[VFS Unlink] ERROR: Failed to permanently delete file: fileID=%d, error=%v", targetID, err)
		} else {
			DebugLog("[VFS Unlink] Successfully permanently deleted file: fileID=%d", targetID)
		}
	}()

	return 0
}

// Rmdir deletes a directory
func (n *OrcasNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	DebugLog("[VFS Rmdir] Entry: name=%s, parentID=%d", name, n.objID)
	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Rmdir] ERROR: Failed to get parent object: name=%s, parentID=%d, error=%v", name, n.objID, err)
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Rmdir] ERROR: Parent is not a directory: name=%s, parentID=%d, type=%d", name, n.objID, obj.Type)
		return syscall.ENOTDIR
	}

	// Check if KEY is required (only for non-root nodes)
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Rmdir] ERROR: checkKey failed: name=%s, parentID=%d, errno=%d", name, n.objID, errno)
		return errno
	}

	// Find child directory
	// If List fails (I/O error), try to find from cache or assume directory doesn't exist
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})

	var targetID int64
	if err != nil {
		// I/O error occurred, try to find directory from cache
		// If not found in cache, assume it doesn't exist and just clean up cache
		DebugLog("[VFS Rmdir] WARNING: Failed to list parent directory (I/O error), trying cache: dirID=%d, error=%v", obj.ID, err)
		// Try to find from directory cache
		cachedChildren, cacheErrno := n.getDirListWithCache(obj.ID)
		if cacheErrno == 0 && cachedChildren != nil {
			for _, child := range cachedChildren {
				if child.Name == name && child.Type == core.OBJ_TYPE_DIR {
					targetID = child.ID
					DebugLog("[VFS Rmdir] Found directory in cache: dirID=%d, name=%s", targetID, name)
					break
				}
			}
		}
		// If still not found, assume directory doesn't exist, just clean up cache and return success
		if targetID == 0 {
			DebugLog("[VFS Rmdir] Directory not found in cache, assuming already deleted: name=%s", name)
			// Just clean up cache and return success (directory may have been already deleted)
			n.invalidateObj()
			return 0
		}
	} else {
		// Successfully listed, find target directory
		for _, child := range children {
			if child.Name == name && child.Type == core.OBJ_TYPE_DIR {
				targetID = child.ID
				break
			}
		}

		if targetID == 0 {
			DebugLog("[VFS Rmdir] ERROR: Target directory not found: name=%s, parentID=%d", name, obj.ID)
			return syscall.ENOENT
		}
	}

	// Check if trying to remove root node (the target directory is the root node)
	// Root node's ID is bucketID
	if targetID == n.fs.bktID {
		DebugLog("[VFS Rmdir] Attempted to remove root node: targetID=%d (bucketID), name=%s, parentID=%d", targetID, name, obj.ID)

		// Call OnRootDeleted callback if set (root node deletion means entire bucket is deleted)
		if n.fs.OnRootDeleted != nil {
			DebugLog("[VFS Rmdir] Calling OnRootDeleted callback due to root node deletion")
			n.fs.OnRootDeleted(n.fs)
		}
		// Allow root node removal, return success
		return 0
	}

	// Check if directory is empty
	// If List fails (I/O error), assume directory is empty or already deleted
	dirChildren, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, targetID, core.ListOptions{
		Count: 1,
	})
	if err != nil {
		// I/O error occurred, assume directory is empty or already deleted
		// Log warning but continue with deletion
		DebugLog("[VFS Rmdir] WARNING: Failed to check if directory is empty (I/O error), assuming empty: dirID=%d, error=%v", targetID, err)
	} else if len(dirChildren) > 0 {
		DebugLog("[VFS Rmdir] ERROR: Directory is not empty: name=%s, dirID=%d, parentID=%d, childCount=%d", name, targetID, obj.ID, len(dirChildren))
		return syscall.ENOTEMPTY
	}

	// Step 1: Remove from parent directory first (mark as deleted)
	// This makes the directory disappear from parent's listing immediately
	// If Recycle fails, log warning but continue (directory may have been already deleted)
	err = n.fs.h.Recycle(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		// If Recycle fails, assume directory is already deleted or inaccessible
		// Just clean up cache and continue with async deletion
		DebugLog("[VFS Rmdir] WARNING: Failed to mark directory as deleted (may already be deleted): dirID=%d, error=%v", targetID, err)
	}

	// Step 2: Invalidate directory listing cache
	n.invalidateDirListCache(obj.ID)

	// Invalidate parent directory cache
	n.invalidateObj()

	// Step 3: Asynchronously delete and clean up (permanent deletion)
	// This includes recursively deleting all child objects and physical deletion of data files and metadata
	go func() {
		// Use the original context to preserve authentication information
		// Context is read-only and safe to use in goroutines
		// Delete will recursively delete all child objects in the background
		err := n.fs.h.Delete(n.fs.c, n.fs.bktID, targetID)
		if err != nil {
			DebugLog("[VFS Rmdir] ERROR: Failed to permanently delete directory: dirID=%d, error=%v", targetID, err)
		} else {
			DebugLog("[VFS Rmdir] Successfully permanently deleted directory: dirID=%d", targetID)
		}
	}()

	return 0
}

// Rename renames a file/directory
func (n *OrcasNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	DebugLog("[VFS Rename] Entry: name=%s, newName=%s, parentID=%d, flags=0x%x", name, newName, n.objID, flags)
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Rename] ERROR: checkKey failed: name=%s, newName=%s, parentID=%d, errno=%d", name, newName, n.objID, errno)
		return errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Rename] ERROR: Failed to get source parent object: name=%s, newName=%s, parentID=%d, error=%v", name, newName, n.objID, err)
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Rename] ERROR: Source parent is not a directory: name=%s, newName=%s, parentID=%d, type=%d", name, newName, n.objID, obj.Type)
		return syscall.ENOTDIR
	}

	// First, try to find source object from cache (RandomAccessor or fileObjCache)
	// This avoids unnecessary database queries if the file is already in cache
	var sourceID int64

	// Try to find source file from RandomAccessor cache
	// Check if this node has a RandomAccessor that matches the source file name
	// IMPORTANT: Only match exact name here (not name without .tmp suffix)
	// This ensures we find the correct file, especially for .tmp files
	val := n.ra.Load()
	if val != nil && val != releasedMarker {
		if ra, ok := val.(*RandomAccessor); ok && ra != nil {
			fileObj, err := ra.getFileObj()
			if err == nil && fileObj != nil && fileObj.Name == name && fileObj.PID == obj.ID {
				sourceID = fileObj.ID
				DebugLog("[VFS Rename] Found source file from node RandomAccessor: sourceID=%d, name=%s", sourceID, name)
			}
		}
	}

	// If not found in RandomAccessor, try to find from fileObjCache via List
	// We call List but prioritize cache entries for each child
	var sourceObj *core.ObjectInfo
	if sourceID == 0 {
		// Try to get from List result, but also check cache for each child
		children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		if err != nil {
			DebugLog("[VFS Unlink] ERROR: Failed to list directory children: name=%s, parentID=%d, error=%v", name, obj.ID, err)
			return syscall.EIO
		}

		// First, try to find from cache for each child
		// Also check if name matches with or without .tmp suffix (for files that may have been auto-renamed)
		nameLower := strings.ToLower(name)
		hasTmpSuffix := strings.HasSuffix(nameLower, ".tmp")
		var nameWithoutTmp string
		if hasTmpSuffix {
			nameWithoutTmp = name[:len(name)-4] // Remove ".tmp" suffix
		}

		// IMPORTANT: When searching for source file, prioritize exact match over name without .tmp suffix
		// This ensures we find the .tmp file itself, not the target file with the same name (without .tmp)
		// First pass: look for exact match (especially important for .tmp files)
		for _, child := range children {
			// Prioritize exact name match first
			if child.Name == name {
				// Found exact match, try to get from cache first
				cacheKey := child.ID
				if cached, ok := fileObjCache.Get(cacheKey); ok {
					if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
						// Use cached object (may have more up-to-date information)
						sourceID = cachedObj.ID
						sourceObj = cachedObj
						DebugLog("[VFS Rename] Found source file from List (exact match, cached): sourceID=%d, name=%s", sourceID, name)
						break
					}
				}
				// If cache miss, use child from List
				if sourceID == 0 {
					sourceID = child.ID
					sourceObj = child
					DebugLog("[VFS Rename] Found source file from List (exact match, from DB): sourceID=%d, name=%s", sourceID, name)
					break
				}
			}
		}

		// Second pass: if exact match not found and source name has .tmp suffix,
		// also check for name without .tmp suffix (for files that may have been auto-renamed)
		// But only if we haven't found an exact match
		if sourceID == 0 && hasTmpSuffix {
			for _, child := range children {
				if child.Name == nameWithoutTmp {
					// Found match without .tmp suffix, try to get from cache first
					cacheKey := child.ID
					if cached, ok := fileObjCache.Get(cacheKey); ok {
						if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
							// Use cached object (may have more up-to-date information)
							sourceID = cachedObj.ID
							sourceObj = cachedObj
							break
						}
					}
					// If cache miss, use child from List
					if sourceID == 0 {
						sourceID = child.ID
						sourceObj = child
						break
					}
				}
			}
		}

		// If still not found, try to find from RandomAccessor registry
		// This handles cases where file is being written and may not be in List yet
		// IMPORTANT: Prioritize exact match over name without .tmp suffix
		if sourceID == 0 {
			// First pass: look for exact match in RandomAccessor registry
			n.fs.raRegistry.Range(func(key, value interface{}) bool {
				if ra, ok := value.(*RandomAccessor); ok && ra != nil {
					fileObj, err := ra.getFileObj()
					if err == nil && fileObj != nil && fileObj.PID == obj.ID {
						// Prioritize exact name match first
						if fileObj.Name == name {
							sourceID = fileObj.ID
							sourceObj = fileObj
							DebugLog("[VFS Rename] Found source file from RandomAccessor registry (exact match): sourceID=%d, name=%s", sourceID, name)
							return false // Stop iteration
						}
					}
				}
				return true // Continue iteration
			})

			// Second pass: if exact match not found and source name has .tmp suffix,
			// also check for name without .tmp suffix (for files that may have been auto-renamed)
			if sourceID == 0 && hasTmpSuffix {
				n.fs.raRegistry.Range(func(key, value interface{}) bool {
					if ra, ok := value.(*RandomAccessor); ok && ra != nil {
						fileObj, err := ra.getFileObj()
						if err == nil && fileObj != nil && fileObj.PID == obj.ID {
							// Match name without .tmp suffix
							if fileObj.Name == nameWithoutTmp {
								sourceID = fileObj.ID
								sourceObj = fileObj
								return false // Stop iteration
							}
						}
					}
					return true // Continue iteration
				})
			}
		}
	} else {
		// If found from RandomAccessor, get source object info
		// Try cache first
		cacheKey := sourceID
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				sourceObj = cachedObj
			}
		}
		// If cache miss, get from database
		if sourceObj == nil {
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{sourceID})
			if err == nil && len(objs) > 0 {
				sourceObj = objs[0]
			}
		}
	}

	if sourceID == 0 || sourceObj == nil {
		DebugLog("[VFS Rename] ERROR: Source file not found: name=%s, parentID=%d", name, obj.ID)
		return syscall.ENOENT
	}

	DebugLog("[VFS Rename] Found source file: sourceID=%d, name=%s, type=%d, PID=%d", sourceID, sourceObj.Name, sourceObj.Type, sourceObj.PID)

	// Get new parent object first (needed for interception check)
	newParentNode, ok := newParent.(*OrcasNode)
	if !ok {
		DebugLog("[VFS Rename] ERROR: newParent is not OrcasNode: name=%s, newName=%s", name, newName)
		return syscall.EIO
	}
	
	newParentObj, err := newParentNode.getObj()
	if err != nil {
		DebugLog("[VFS Rename] ERROR: Failed to get new parent object: name=%s, newName=%s, error=%v", name, newName, err)
		return syscall.ENOENT
	}
	
	if newParentObj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Rename] ERROR: New parent is not a directory: name=%s, newName=%s, newParentID=%d, type=%d", name, newName, newParentObj.ID, newParentObj.Type)
		return syscall.ENOTDIR
	}

	// Check if this rename should be intercepted for save pattern merge
	if n.fs.savePatternDetector != nil && sourceObj.Type == core.OBJ_TYPE_FILE {
		if shouldIntercept, match := n.fs.savePatternDetector.ShouldInterceptRename(
			sourceID, sourceObj.Name, newName, obj.ID, newParentObj.ID); shouldIntercept {
			// Intercept rename operation, execute merge
			if err := n.fs.savePatternDetector.HandleRenameWithMerge(match); err != nil {
				DebugLog("[VFS Rename] Failed to handle rename with merge: %v", err)
				return syscall.EIO
			}
			// Successfully merged, return success without actually renaming
			DebugLog("[VFS Rename] Intercepted rename for save pattern merge: %s -> %s, fileID=%d", 
				sourceObj.Name, newName, sourceID)
			return 0
		}
	}

	// Determine if source is .tmp file that will lose its .tmp suffix
	isTmpFile := false
	isRemovingTmp := false
	if sourceObj.Type == core.OBJ_TYPE_FILE {
		oldNameLower := strings.ToLower(sourceObj.Name)
		newNameLower := strings.ToLower(newName)
		isTmpFile = strings.HasSuffix(oldNameLower, ".tmp")
		isRemovingTmp = isTmpFile && !strings.HasSuffix(newNameLower, ".tmp")

		if isRemovingTmp {
			n.forceFlushTempFileBeforeRename(sourceID, sourceObj.Name, newName)
			// Re-fetch source object after flush to ensure we have latest DataID
			// Invalidate cache for source file (not current node)
			cacheKey := sourceID
			fileObjCache.Del(cacheKey)
			// Re-fetch source object from database
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{sourceID})
			if err != nil || len(objs) == 0 {
				DebugLog("[VFS Rename] ERROR: Failed to re-fetch source object after flush: fileID=%d, error=%v", sourceID, err)
				return syscall.ENOENT
			}
			sourceObj = objs[0]
			// Update cache with fresh data
			fileObjCache.Put(cacheKey, sourceObj)
			DebugLog("[VFS Rename] Re-fetched source object after flush: fileID=%d, dataID=%d, size=%d, name=%s", sourceID, sourceObj.DataID, sourceObj.Size, sourceObj.Name)
			// For empty files (Size = 0), EmptyDataID is valid and should be allowed
			// Only verify DataID for non-empty files
			if sourceObj.Size > 0 && (sourceObj.DataID == 0 || sourceObj.DataID == core.EmptyDataID) {
				DebugLog("[VFS Rename] WARNING: Source file has data but no DataID after flush: fileID=%d, name=%s, size=%d", sourceID, sourceObj.Name, sourceObj.Size)
				// Re-fetch again with retries (error case)
				maxRetries := 10
				for retry := 0; retry < maxRetries; retry++ {
					fileObjCache.Del(cacheKey)
					objs, err = n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{sourceID})
					if err == nil && len(objs) > 0 {
						sourceObj = objs[0]
						// Check if file is empty - if so, EmptyDataID is valid
						if sourceObj.Size == 0 {
							fileObjCache.Put(cacheKey, sourceObj)
							DebugLog("[VFS Rename] Source file is empty (EmptyDataID is valid): fileID=%d, dataID=%d, size=%d", sourceID, sourceObj.DataID, sourceObj.Size)
							break
						}
						if sourceObj.DataID > 0 && sourceObj.DataID != core.EmptyDataID {
							fileObjCache.Put(cacheKey, sourceObj)
							DebugLog("[VFS Rename] Successfully re-fetched source object after retry (retry %d/%d): fileID=%d, dataID=%d, size=%d, name=%s", retry+1, maxRetries, sourceID, sourceObj.DataID, sourceObj.Size, sourceObj.Name)
							break
						}
					}
					if retry < maxRetries-1 {
						time.Sleep(50 * time.Millisecond) // Only wait on error retry
					}
				}
				// Final check: only require DataID for non-empty files
				if sourceObj.Size > 0 && (sourceObj.DataID == 0 || sourceObj.DataID == core.EmptyDataID) {
					DebugLog("[VFS Rename] ERROR: Source file still has no DataID after wait: fileID=%d, name=%s, size=%d", sourceID, sourceObj.Name, sourceObj.Size)
					return syscall.EIO
				}
			} else if sourceObj.Size == 0 {
				// Empty file, EmptyDataID is valid
				DebugLog("[VFS Rename] Source file is empty (EmptyDataID is valid): fileID=%d, dataID=%d, size=%d", sourceID, sourceObj.DataID, sourceObj.Size)
			}
		}
	}

	// Get target parent directory
	// Check if target file already exists in the new parent directory
	// First try to find from cache (RandomAccessor or fileObjCache) before calling List
	var existingTargetID int64
	var existingTargetObj *core.ObjectInfo

	// Try to find target file from newParentNode's RandomAccessor cache
	newParentVal := newParentNode.ra.Load()
	if newParentVal != nil && newParentVal != releasedMarker {
		if ra, ok := newParentVal.(*RandomAccessor); ok && ra != nil {
			fileObj, err := ra.getFileObj()
			if err == nil && fileObj != nil && fileObj.Name == newName && fileObj.PID == newParentObj.ID {
				existingTargetID = fileObj.ID
				existingTargetObj = fileObj
			}
		}
	}

	// If not found in RandomAccessor, call List and check cache for each child
	// Also check for files that may have had .tmp suffix removed by TempFileWriter.Flush()
	if existingTargetID == 0 {
		targetChildren, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, newParentObj.ID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		if err != nil {
			DebugLog("[VFS Unlink] ERROR: Failed to list directory children: name=%s, parentID=%d, error=%v", name, obj.ID, err)
			return syscall.EIO
		}

		// Check if newName has .tmp suffix - if so, also check for name without .tmp
		// (in case old file had .tmp removed by TempFileWriter.Flush())
		newNameLower := strings.ToLower(newName)
		hasTmpSuffix := strings.HasSuffix(newNameLower, ".tmp")
		var nameWithoutTmp string
		if hasTmpSuffix {
			nameWithoutTmp = newName[:len(newName)-4] // Remove ".tmp" suffix
		}

		// First, try to find from cache for each child
		// IMPORTANT: Exclude source file itself (sourceID) when searching for existing target
		// This ensures we find the actual target file, not the source file being renamed
		for _, child := range targetChildren {
			// Skip source file itself - we're looking for a different file with the same name
			if child.ID == sourceID {
				continue
			}
			// Match exact name or name without .tmp suffix (for files that may have been auto-renamed)
			if child.Name == newName || (hasTmpSuffix && child.Name == nameWithoutTmp) {
				// Found potential match, try to get from cache first
				cacheKey := child.ID
				if cached, ok := fileObjCache.Get(cacheKey); ok {
					if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
						// Use cached object (may have more up-to-date information)
						// Double-check it's not the source file
						if cachedObj.ID != sourceID {
							existingTargetID = cachedObj.ID
							existingTargetObj = cachedObj
							break
						}
					}
				}
				// If cache miss, use child from List
				// Double-check it's not the source file
				if existingTargetID == 0 && child.ID != sourceID {
					existingTargetID = child.ID
					existingTargetObj = child
					break
				}
			}
		}
	}

	// First, try to update cache from RandomAccessor for source file
	// Check if source file has an open RandomAccessor and update its cache
	n.updateFileObjCache(sourceID, newName, newParentObj.ID)

	// Track if we need to delete target file (if it's a .tmp file)
	var targetTmpFileID int64 = 0

	// If target exists, check its type and handle accordingly
	if existingTargetID > 0 {
		// Get the existing target object to check its type (try cache first)
		var existingObj *core.ObjectInfo
		if existingTargetObj != nil {
			existingObj = existingTargetObj
		} else {
			// Try to get from cache first
			targetCacheKey := existingTargetID
			if cached, ok := fileObjCache.Get(targetCacheKey); ok {
				if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
					existingObj = obj
				}
			}
			// If cache miss, get from database
			if existingObj == nil {
				existingObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{existingTargetID})
				if err == nil && len(existingObjs) > 0 {
					existingObj = existingObjs[0]
				}
			}
		}
		// Check if target is a directory - cannot rename a file to a directory
		if existingObj != nil && existingObj.Type == core.OBJ_TYPE_DIR {
			DebugLog("[VFS Rename] ERROR: Target is a directory, cannot rename file to directory: targetID=%d, name=%s", existingTargetID, existingObj.Name)
			return syscall.EISDIR
		}
		// If target is a file, handle it (create version, delete if .tmp, etc.)
		if existingObj != nil && existingObj.Type == core.OBJ_TYPE_FILE {
			// Check if existing target file is a .tmp file
			// Also check if target name is .tmp and existing file name matches without .tmp suffix
			// (in case old .tmp file had suffix removed by TempFileWriter.Flush())
			existingNameLower := strings.ToLower(existingObj.Name)
			newNameLower := strings.ToLower(newName)
			isExistingTmpFile := strings.HasSuffix(existingNameLower, ".tmp")
			isTargetTmpFile := strings.HasSuffix(newNameLower, ".tmp")

			// Check if existing file name matches target name without .tmp suffix
			// This handles case where old .tmp file had suffix removed by Flush()
			var nameWithoutTmp string
			if isTargetTmpFile {
				nameWithoutTmp = newName[:len(newName)-4] // Remove ".tmp" suffix
			}
			isOldTmpFileWithoutSuffix := isTargetTmpFile && existingObj.Name == nameWithoutTmp

			if isExistingTmpFile || isOldTmpFileWithoutSuffix {
				// If target file is a .tmp file (or old .tmp file without suffix), we'll delete it after rename
				targetTmpFileID = existingTargetID
				DebugLog("[VFS Rename] Target file is .tmp file (or old .tmp without suffix), will delete after rename: fileID=%d, name=%s, targetName=%s", existingTargetID, existingObj.Name, newName)

				if n.fs != nil {
					if targetRA := n.fs.getRandomAccessorByFileID(existingTargetID); targetRA != nil {
						// Force flush before deletion
						if _, err := targetRA.ForceFlush(); err != nil {
							DebugLog("[VFS Rename] WARNING: Failed to flush target .tmp file before deletion: fileID=%d, error=%v", existingTargetID, err)
						}
						// Unregister RandomAccessor
						n.fs.unregisterRandomAccessor(existingTargetID, targetRA)
					}
				}

				// Remove from file object cache immediately (before database delete)
				targetCacheKey := existingTargetID
				fileObjCache.Del(targetCacheKey)
			} else {
				// Target file is not a .tmp file
				// If source is a .tmp file being renamed to this target, special handling is done above
				// (merge version and delete .tmp file, so we skip the normal rename path)
				if !isRemovingTmp {
					// Source is not a .tmp file, target is not a .tmp file
					// Create version from existing file (non-.tmp files should preserve versions)
					// Note: We need to check if handler supports CreateVersionFromFile
					if lh, ok := n.fs.h.(*core.LocalHandler); ok {
						err = lh.CreateVersionFromFile(n.fs.c, n.fs.bktID, existingTargetID)
						if err != nil {
							// Log error but continue with rename (don't fail the operation)
							// The existing file will be overwritten
							DebugLog("[VFS Rename] WARNING: Failed to create version from target file: targetID=%d, error=%v", existingTargetID, err)
						}
					}
				}
			}
		}
	}

	// Special handling: if source is .tmp file and target exists, merge version and delete .tmp file
	// BUT: if sourceID == existingTargetID, it's the same file (just renaming), don't merge/delete
	DebugLog("[VFS Rename] Checking merge condition: isRemovingTmp=%v, existingTargetID=%d, sourceID=%d, sourceName=%s, targetName=%s",
		isRemovingTmp, existingTargetID, sourceID, sourceObj.Name, newName)
	if isRemovingTmp && existingTargetID > 0 && sourceID != existingTargetID {
		// Source is .tmp file, target file exists (non-.tmp) and is different file
		// Instead of renaming, we should:
		// 1. Create a version from source .tmp file and attach it to target file
		// 2. Update target file with source .tmp file's data
		// 3. Delete source .tmp file
		DebugLog("[VFS Rename] Merging .tmp file into existing target file: sourceID=%d, targetID=%d, targetName=%s", sourceID, existingTargetID, newName)

		// For empty files (Size = 0), EmptyDataID is valid and should be handled specially
		// Empty files don't need DataID, so we can skip the merge logic and just delete the source .tmp file
		if sourceObj.Size == 0 {
			DebugLog("[VFS Rename] Source .tmp file is empty (Size=0), skipping merge and just deleting source file: sourceID=%d, targetID=%d", sourceID, existingTargetID)
			// Delete source .tmp file (it's empty, no need to merge)
			err := n.fs.h.Delete(n.fs.c, n.fs.bktID, sourceID)
			if err != nil {
				DebugLog("[VFS Rename] ERROR: Failed to delete empty source .tmp file: sourceID=%d, error=%v", sourceID, err)
				return syscall.EIO
			}
			// Invalidate cache
			cacheKey := sourceID
			fileObjCache.Del(cacheKey)
			// Remove from parent directory cache using sourceObj.ID (the actual .tmp file ID)
			// Invalidate directory cache
			if sourceObj.PID > 0 {
				n.invalidateDirListCache(sourceObj.PID)
			}
			if obj.ID != sourceObj.PID && obj.ID > 0 {
				n.invalidateDirListCache(obj.ID)
			}
			DebugLog("[VFS Rename] Successfully deleted empty source .tmp file: sourceID=%d", sourceID)
			return 0
		}

		// Verify source file has DataID before merging (only for non-empty files)
		if sourceObj.DataID == 0 || sourceObj.DataID == core.EmptyDataID {
			DebugLog("[VFS Rename] WARNING: Source .tmp file has no DataID before merge, retrying: sourceID=%d, targetID=%d, targetName=%s, size=%d", sourceID, existingTargetID, newName, sourceObj.Size)
			// Try multiple times to get DataID (error retry case)
			cacheKey := sourceID
			maxRetries := 10
			for retry := 0; retry < maxRetries; retry++ {
				fileObjCache.Del(cacheKey)
				objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{sourceID})
				if err == nil && len(objs) > 0 && objs[0].DataID > 0 && objs[0].DataID != core.EmptyDataID {
					sourceObj = objs[0]
					fileObjCache.Put(cacheKey, sourceObj)
					DebugLog("[VFS Rename] Successfully got DataID after retry (retry %d/%d): sourceID=%d, dataID=%d, size=%d",
						retry+1, maxRetries, sourceID, sourceObj.DataID, sourceObj.Size)
					break
				} else {
					DebugLog("[VFS Rename] Still no DataID after retry %d/%d: sourceID=%d, error=%v", retry+1, maxRetries, sourceID, err)
				}
				if retry < maxRetries-1 {
					time.Sleep(50 * time.Millisecond) // Only wait on error retry
				}
			}
			// Final check
			if sourceObj.DataID == 0 || sourceObj.DataID == core.EmptyDataID {
				DebugLog("[VFS Rename] ERROR: Source .tmp file still has no DataID after %d retries: sourceID=%d, size=%d", maxRetries, sourceID, sourceObj.Size)
				return syscall.EIO
			}
		}

		// Get LocalHandler to access version creation methods
		lh, ok := n.fs.h.(*core.LocalHandler)
		if !ok {
			DebugLog("[VFS Rename] ERROR: Handler is not LocalHandler, cannot merge versions: sourceID=%d, targetID=%d", sourceID, existingTargetID)
			return syscall.EIO
		}

		// 1. Create a version from source .tmp file and attach it to target file
		// The version will have source .tmp file's DataID and Size
		versionTime := core.Now()
		newVersion := &core.ObjectInfo{
			ID:     core.NewID(),
			PID:    existingTargetID, // Parent is the target file
			Type:   core.OBJ_TYPE_VERSION,
			Name:   strconv.FormatInt(versionTime, 10), // Use timestamp as version name
			DataID: sourceObj.DataID,
			Size:   sourceObj.Size,
			MTime:  versionTime,
		}

		// 2. Update target file with source .tmp file's data
		// Get existing target file object to preserve Type, Name, PID
		var existingTargetFileObj *core.ObjectInfo
		if existingTargetObj != nil {
			existingTargetFileObj = existingTargetObj
		} else {
			// Get from database
			targetObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{existingTargetID})
			if err == nil && len(targetObjs) > 0 {
				existingTargetFileObj = targetObjs[0]
			}
		}
		if existingTargetFileObj == nil {
			DebugLog("[VFS Rename] ERROR: Failed to get existing target file object: targetID=%d", existingTargetID)
			return syscall.EIO
		}

		// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
		updateTargetFile := &core.ObjectInfo{
			ID:     existingTargetID,
			PID:    existingTargetFileObj.PID,
			Type:   existingTargetFileObj.Type,
			Name:   existingTargetFileObj.Name,
			DataID: sourceObj.DataID,
			Size:   sourceObj.Size,
			MTime:  versionTime,
		}

		// Batch create version and update target file
		DebugLog("[VFS Rename] Preparing to merge: sourceID=%d, sourceDataID=%d, sourceSize=%d, targetID=%d, targetName=%s, versionID=%d",
			sourceID, sourceObj.DataID, sourceObj.Size, existingTargetID, newName, newVersion.ID)
		objectsToPut := []*core.ObjectInfo{newVersion, updateTargetFile}
		_, err = lh.Put(n.fs.c, n.fs.bktID, objectsToPut)
		if err != nil {
			DebugLog("[VFS Rename] ERROR: Failed to merge .tmp file into target file: sourceID=%d, sourceDataID=%d, sourceSize=%d, targetID=%d, targetName=%s, versionID=%d, error=%v",
				sourceID, sourceObj.DataID, sourceObj.Size, existingTargetID, newName, newVersion.ID, err)
			// Log details about objects being put
			for i, obj := range objectsToPut {
				DebugLog("[VFS Rename] Object %d: ID=%d, Type=%d, Name=%s, DataID=%d, Size=%d, PID=%d",
					i, obj.ID, obj.Type, obj.Name, obj.DataID, obj.Size, obj.PID)
			}
			return syscall.EIO
		}
		DebugLog("[VFS Rename] Successfully merged .tmp file into target file: sourceID=%d, targetID=%d, versionID=%d, sourceDataID=%d, sourceSize=%d",
			sourceID, existingTargetID, newVersion.ID, sourceObj.DataID, sourceObj.Size)

		// 3. Delete source .tmp file
		// First, flush and unregister RandomAccessor if exists (similar to target .tmp file deletion)
		if n.fs != nil {
			if sourceRA := n.fs.getRandomAccessorByFileID(sourceID); sourceRA != nil {
				// Force flush before deletion
				if _, flushErr := sourceRA.ForceFlush(); flushErr != nil {
					DebugLog("[VFS Rename] WARNING: Failed to flush source .tmp file before deletion: sourceID=%d, error=%v", sourceID, flushErr)
				}
				// Unregister RandomAccessor
				n.fs.unregisterRandomAccessor(sourceID, sourceRA)
			}
		}

		// Remove from file object cache immediately (before database delete)
		cacheKey := sourceID
		fileObjCache.Del(cacheKey)

		DebugLog("[VFS Rename] Deleting source .tmp file after merge: sourceID=%d", sourceID)
		err = n.fs.h.Delete(n.fs.c, n.fs.bktID, sourceID)
		if err != nil {
			DebugLog("[VFS Rename] ERROR: Failed to delete source .tmp file after merge: sourceID=%d, error=%v", sourceID, err)
			// Log error but don't fail the operation (merge already succeeded)
		} else {
			DebugLog("[VFS Rename] Successfully deleted source .tmp file after merge: sourceID=%d", sourceID)
		}

		// Invalidate directory listing cache for both directories
		if sourceObj.PID > 0 {
			n.invalidateDirListCache(sourceObj.PID)
		}
		if obj.ID != sourceObj.PID && obj.ID > 0 {
			n.invalidateDirListCache(obj.ID)
		}
		// Update target file in new parent directory listing (if target exists)
		// Note: Target file is updated with new data, so we need to update it in cache
		targetObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{existingTargetID})
		if err == nil && len(targetObjs) > 0 {
			// Invalidate directory listing cache
			n.invalidateDirListCache(newParentObj.ID)
			// Also update fileObjCache with latest data
			targetCacheKey := existingTargetID
			fileObjCache.Put(targetCacheKey, targetObjs[0])
		}

		return 0
	}

	// If target is a .tmp file, delete it BEFORE renaming to avoid unique constraint violation
	// This ensures the target name is available when we rename the source file
	if targetTmpFileID > 0 {
		DebugLog("[VFS Rename] Deleting target .tmp file before rename to avoid unique constraint: fileID=%d", targetTmpFileID)
		err := n.fs.h.Delete(n.fs.c, n.fs.bktID, targetTmpFileID)
		if err != nil {
			DebugLog("[VFS Rename] ERROR: Failed to delete target .tmp file before rename: fileID=%d, error=%v", targetTmpFileID, err)
			// Continue with rename anyway - if it fails due to unique constraint, we'll handle it
		} else {
			DebugLog("[VFS Rename] Successfully deleted target .tmp file before rename: fileID=%d", targetTmpFileID)
		}
		// Cache already removed above, no need to remove again
	}

	DebugLog("[VFS Rename] Rename source file to target name: sourceID=%d, targetID=%d, targetName=%s", sourceID, existingTargetID, newName)
	// Rename source file to target name
	err = n.fs.h.Rename(n.fs.c, n.fs.bktID, sourceID, newName)
	if err != nil {
		// Check if error is due to unique constraint violation
		// This can happen if target file still exists (race condition or delete failed)
		// Note: err is guaranteed to be non-nil here, so err.Error() is safe
		if err == core.ERR_DUP_KEY || strings.Contains(err.Error(), "UNIQUE constraint failed") {
			DebugLog("[VFS Rename] Unique constraint violation detected, attempting to resolve: sourceID=%d, targetName=%s, error=%v", sourceID, newName, err)

			// Directly query database to find the conflicting file (including deleted ones)
			// We need to check all files with the same name in the parent directory, even if marked as deleted
			// because unique constraint is based on (pid, name) and deleted files (PID < 0) can still conflict
			var conflictFileID int64 = 0

			// First, try to get from List (non-deleted files)
			conflictChildren, _, _, listErr := n.fs.h.List(n.fs.c, n.fs.bktID, newParentObj.ID, core.ListOptions{
				Count: core.DefaultListPageSize,
			})
			if listErr == nil {
				for _, child := range conflictChildren {
					if child.Name == newName && child.Type == core.OBJ_TYPE_FILE && child.ID != sourceID {
						conflictFileID = child.ID
						DebugLog("[VFS Rename] Found conflicting file in List: fileID=%d, name=%s", conflictFileID, newName)
						break
					}
				}
			}

			// If not found in List, check the original existingTargetID
			// The target file might have been marked as deleted (PID < 0) but still exists in DB
			// and still violates unique constraint
			if conflictFileID == 0 && existingTargetID > 0 && existingTargetID != sourceID {
				// Check if the original target file still exists (might be marked as deleted but still in DB)
				targetObjs, getErr := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{existingTargetID})
				if getErr == nil && len(targetObjs) > 0 {
					targetObj := targetObjs[0]
					// Check if target file has the same name (regardless of PID, as unique constraint is on (pid, name))
					// But we need to check if it's in the same parent directory
					// If PID is negative, it means it's marked as deleted, but we still need to handle it
					if targetObj.Name == newName {
						// Check if it's in the same parent (original PID matches new parent, or it's marked as deleted)
						originalPID := targetObj.PID
						if originalPID < 0 {
							originalPID = -originalPID // Get original PID from deleted file
						}
						if originalPID == newParentObj.ID || targetObj.PID == newParentObj.ID {
							conflictFileID = existingTargetID
							DebugLog("[VFS Rename] Found conflicting file from existingTargetID: fileID=%d, name=%s, pid=%d (deleted=%v)", conflictFileID, newName, targetObj.PID, targetObj.PID < 0)
						}
					}
				}
			}

			// If we found a conflicting file, delete it and retry
			if conflictFileID > 0 {
				// Check if it's a .tmp file - if so, flush and unregister RandomAccessor first
				conflictNameLower := strings.ToLower(newName)
				isConflictTmpFile := strings.HasSuffix(conflictNameLower, ".tmp")

				if isConflictTmpFile {
					// Flush and unregister RandomAccessor if exists
					if n.fs != nil {
						if conflictRA := n.fs.getRandomAccessorByFileID(conflictFileID); conflictRA != nil {
							if _, flushErr := conflictRA.ForceFlush(); flushErr != nil {
								DebugLog("[VFS Rename] WARNING: Failed to flush conflicting .tmp file: fileID=%d, error=%v", conflictFileID, flushErr)
							}
							n.fs.unregisterRandomAccessor(conflictFileID, conflictRA)
						}
					}

					// Remove from cache
					conflictCacheKey := conflictFileID
					fileObjCache.Del(conflictCacheKey)
				}

				// Delete the conflicting file
				// Use Recycle first to mark as deleted (faster), then permanently delete if needed
				DebugLog("[VFS Rename] Deleting conflicting file to resolve unique constraint: fileID=%d, name=%s", conflictFileID, newName)

				// First, try to permanently delete the conflicting file
				// This will physically remove it from database, resolving the unique constraint
				deleteErr := n.fs.h.Delete(n.fs.c, n.fs.bktID, conflictFileID)
				if deleteErr != nil {
					// Delete failed, try Recycle as fallback (mark as deleted)
					DebugLog("[VFS Rename] WARNING: Permanent delete failed, trying Recycle: fileID=%d, error=%v", conflictFileID, deleteErr)
					recycleErr := n.fs.h.Recycle(n.fs.c, n.fs.bktID, conflictFileID)
					if recycleErr != nil {
						DebugLog("[VFS Rename] ERROR: Both Delete and Recycle failed, cannot proceed with rename: fileID=%d, deleteErr=%v, recycleErr=%v", conflictFileID, deleteErr, recycleErr)
						return syscall.EIO
					}
					// Recycle succeeded, but file is still in DB (just marked as deleted)
					// We need to wait longer and retry, or use a different approach
					DebugLog("[VFS Rename] File marked as deleted via Recycle, waiting before retry: fileID=%d", conflictFileID)
					time.Sleep(200 * time.Millisecond) // Wait longer for Recycle to take effect
				} else {
					DebugLog("[VFS Rename] Successfully permanently deleted conflicting file: fileID=%d", conflictFileID)
					// Wait a brief moment for delete to complete
					time.Sleep(100 * time.Millisecond)
				}

				// Retry rename with retry loop
				maxRetries := 3
				for retry := 0; retry < maxRetries; retry++ {
					err = n.fs.h.Rename(n.fs.c, n.fs.bktID, sourceID, newName)
					if err == nil {
						DebugLog("[VFS Rename] Successfully renamed after deleting conflicting file (retry %d/%d): sourceID=%d, targetName=%s", retry+1, maxRetries, sourceID, newName)
						break
					}

					// Check if still unique constraint error
					if err != nil && (err == core.ERR_DUP_KEY || strings.Contains(err.Error(), "UNIQUE constraint failed")) {
						if retry < maxRetries-1 {
							DebugLog("[VFS Rename] Still unique constraint error after delete, retrying (retry %d/%d): sourceID=%d, targetName=%s", retry+1, maxRetries, sourceID, newName)
							time.Sleep(100 * time.Millisecond * time.Duration(retry+1)) // Exponential backoff
							continue
						} else {
							DebugLog("[VFS Rename] ERROR: Still unique constraint error after %d retries: sourceID=%d, targetName=%s, error=%v", maxRetries, sourceID, newName, err)
							return syscall.EIO
						}
					} else {
						// Other error
						DebugLog("[VFS Rename] ERROR: Failed to rename after deleting conflicting file: sourceID=%d, targetName=%s, error=%v", sourceID, newName, err)
						return syscall.EIO
					}
				}

				if err != nil {
					DebugLog("[VFS Rename] ERROR: Failed to rename after all retries: sourceID=%d, targetName=%s, error=%v", sourceID, newName, err)
					return syscall.EIO
				}
				// Continue with normal flow
			} else {
				// No conflicting file found, but still got unique constraint error
				// This might be a race condition - wait and retry once
				DebugLog("[VFS Rename] Unique constraint error but no conflicting file found, retrying after brief wait: sourceID=%d, targetName=%s", sourceID, newName)
				time.Sleep(100 * time.Millisecond)
				err = n.fs.h.Rename(n.fs.c, n.fs.bktID, sourceID, newName)
				if err != nil {
					DebugLog("[VFS Rename] ERROR: Failed to rename after retry: sourceID=%d, targetName=%s, error=%v", sourceID, newName, err)
					return syscall.EIO
				}
				DebugLog("[VFS Rename] Successfully renamed after retry: sourceID=%d, targetName=%s", sourceID, newName)
				// Continue with normal flow
			}
			// Note: listErr check is handled above, if listErr != nil, we skip the conflict resolution
			if listErr != nil {
				// Failed to query database, return error
				DebugLog("[VFS Rename] ERROR: Failed to query database for conflicting file: error=%v", listErr)
				return syscall.EIO
			}
		} else {
			// Other error, return it
			DebugLog("[VFS Rename] ERROR: Failed to rename source file to target name: sourceID=%d, targetID=%d, targetName=%s, error=%v", sourceID, existingTargetID, newName, err)
			return syscall.EIO
		}
	}

	DebugLog("[VFS Rename] Successfully renamed source file to target name: sourceID=%d, targetID=%d, targetName=%s", sourceID, existingTargetID, newName)

	// Record rename operation for save pattern detection
	n.fs.recordFileOperation(OpRename, sourceID, name, obj.ID, 0, 0, newName, newParentObj.ID)

	// If moved to different directory, need to move
	if newParentObj.ID != obj.ID {
		err = n.fs.h.MoveTo(n.fs.c, n.fs.bktID, sourceID, newParentObj.ID)
		if err != nil {
			DebugLog("[VFS Rename] ERROR: Failed to move source file to target directory: sourceID=%d, targetID=%d, targetName=%s, error=%v", sourceID, existingTargetID, newName, err)
			return syscall.EIO
		}
	}

	// Note: If source is .tmp file and target file exists, we already handled it above
	// by merging the version and deleting the .tmp file, so no need to delete target file here

	// If source file is a .tmp file being renamed away from .tmp
	// Case 1: target doesn't exist (existingTargetID == 0) - just rename
	// Case 2: target is the same file (existingTargetID == sourceID) - just rename (already handled above by skipping merge)
	// Only process this if target file didn't exist (we already handled the merge case above for different files)
	if isRemovingTmp && (existingTargetID == 0 || existingTargetID == sourceID) {
		DebugLog("[VFS Rename] Source .tmp file renamed to new name (target didn't exist): fileID=%d, oldName=%s, newName=%s", sourceID, sourceObj.Name, newName)

		// Note: We don't delete the source file here because it has been renamed to the target name
		// The source file ID now represents the renamed file, so we should not delete it
		// The .tmp extension has been removed, so it's no longer a temporary file
		DebugLog("[VFS Rename] .tmp extension removed (flush already performed): fileID=%d, oldName=%s, newName=%s", sourceID, sourceObj.Name, newName)
	}

	// Update cache after database operations
	// Update source file cache with new name and parent
	n.updateFileObjCache(sourceID, newName, newParentObj.ID)

	// Invalidate DataInfo cache for source file to ensure fresh data after flush
	// This is important for .tmp files that were just flushed
	if sourceObj.Type == core.OBJ_TYPE_FILE && sourceObj.DataID > 0 {
		dataInfoCacheKey := sourceObj.DataID
		dataInfoCache.Del(dataInfoCacheKey)
		DebugLog("[VFS Rename] Invalidated DataInfo cache: fileID=%d, dataID=%d", sourceID, sourceObj.DataID)
	}

	// Update cached source object name for future logic
	if sourceObj.Type == core.OBJ_TYPE_FILE {
		sourceObj.Name = newName
	}

	// Invalidate directory listing cache for both old and new parent directories
	if obj.ID != newParentObj.ID {
		n.invalidateDirListCache(obj.ID)
	}
	n.invalidateDirListCache(newParentObj.ID)

	// Invalidate both directories' cache (for GetAttr)
	n.invalidateObj()
	newParentNode.invalidateObj()

	return 0
}

// Read reads file content
// Read implements FileReader interface
func (n *OrcasNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return n.readImpl(ctx, dest, off)
}

// readImpl is the actual read implementation
func (n *OrcasNode) readImpl(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	DebugLog("[VFS Read] Entry: objID=%d, offset=%d, size=%d", n.objID, off, len(dest))
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Read] ERROR: checkKey failed: objID=%d, offset=%d, size=%d, errno=%d", n.objID, off, len(dest), errno)
		return nil, errno
	}

	// Force refresh object cache to get latest DataID (important after writes)
	// This ensures we get the latest DataID from database or fileObjCache
	n.invalidateObj()

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Read] ERROR: Failed to get object: objID=%d, offset=%d, size=%d, error=%v", n.objID, off, len(dest), err)
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		DebugLog("[VFS Read] ERROR: Object is not a file: objID=%d, type=%d, offset=%d, size=%d", n.objID, obj.Type, off, len(dest))
		return nil, syscall.EISDIR
	}

	// DebugLog("[VFS Read] Reading file: objID=%d, DataID=%d, Size=%d, offset=%d, size=%d", obj.ID, obj.DataID, obj.Size, off, len(dest))

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		// Empty file
		// DebugLog("[VFS Read] Empty file (no DataID): objID=%d, DataID=%d (EmptyDataID=%d)", obj.ID, obj.DataID, core.EmptyDataID)
		return fuse.ReadResultData(nil), 0
	}

	// Get dataReader (cached by dataID, one per file)
	// DebugLog("[VFS Read] Getting DataReader: objID=%d, DataID=%d, offset=%d", obj.ID, obj.DataID, off)
	reader, errno := n.getDataReader(off)
	if errno != 0 {
		// DebugLog("[VFS Read] ERROR: Failed to get DataReader: objID=%d, DataID=%d, offset=%d, errno=%d (%s)", obj.ID, obj.DataID, off, errno, errno.Error())
		return nil, errno
	}
	if reader == nil {
		// DebugLog("[VFS Read] ERROR: DataReader is nil: objID=%d, DataID=%d, offset=%d", obj.ID, obj.DataID, off)
		return nil, syscall.EIO
	}
	// DebugLog("[VFS Read] Got DataReader: objID=%d, DataID=%d, offset=%d, reader=%p", obj.ID, obj.DataID, off, reader)

	// Use dataReader interface (Read(buf, offset))
	// DebugLog("[VFS Read] Calling reader.Read: objID=%d, DataID=%d, offset=%d, size=%d", obj.ID, obj.DataID, off, len(dest))
	nRead, err := reader.Read(dest, off)
	if err != nil && err != io.EOF {
		// DebugLog("[VFS Read] ERROR: Read failed: objID=%d, DataID=%d, offset=%d, size=%d, error=%v, errorType=%T", obj.ID, obj.DataID, off, len(dest), err, err)
		return nil, syscall.EIO
	}
	if err == io.EOF {
		// DebugLog("[VFS Read] Read reached EOF: objID=%d, DataID=%d, offset=%d, requested=%d, read=%d", obj.ID, obj.DataID, off, len(dest), nRead)
	} else {
		// DebugLog("[VFS Read] Successfully read data: objID=%d, DataID=%d, offset=%d, requested=%d, read=%d", obj.ID, obj.DataID, off, len(dest), nRead)
	}

	// Verify read data integrity (for debugging - can be disabled in production)
	// Only verify on first read (offset=0) to avoid performance impact
	if off == 0 && nRead > 0 {
		// Note: Full verification is expensive, so we only log a warning if size seems wrong
		// Full verification can be triggered manually via VerifyFileData()
		if obj.Size > 0 && int64(nRead) > obj.Size {
			// DebugLog("[VFS Read] WARNING: Read more than file size: objID=%d, read=%d, fileSize=%d", obj.ID, nRead, obj.Size)
		}
	}

	// CRITICAL: In go-fuse v2:
	// - Read(ctx, buf, off) 的 buf 一定会复用
	// - buf 只在 Read 调用栈内有效
	// - 禁止将 buf 放入 ReadResultData
	// - 需要跨栈、跨协程、跨请求 → 必须拷贝
	// We must create a copy to ensure data integrity because:
	// 1. dest buffer is reused by FUSE library (guaranteed in go-fuse v2)
	// 2. dest is only valid within Read call stack
	// 3. ReadResultData may be used across stack, goroutine, or request boundaries
	// 4. chunkData from cache may be shared across goroutines
	resultData := make([]byte, nRead)
	copy(resultData, dest[:nRead])
	return fuse.ReadResultData(resultData), 0
}

// getDataReader gets or creates DataReader (with cache)
// offset: starting offset for streaming readers (compressed/encrypted)
// For plain readers, offset is ignored as they support ReadAt
// For compressed/encrypted files, uses dataID as cache key to ensure one file uses the same reader
func (n *OrcasNode) getDataReader(offset int64) (dataReader, syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		// DebugLog("[VFS getDataReader] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return nil, syscall.ENOENT
	}

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		// DebugLog("[VFS getDataReader] ERROR: Empty DataID: objID=%d, DataID=%d", obj.ID, obj.DataID)
		return nil, syscall.EIO
	}

	// Get DataInfo
	// DebugLog("[VFS getDataReader] Getting DataInfo: objID=%d, DataID=%d, bktID=%d", obj.ID, obj.DataID, n.fs.bktID)
	dataInfo, err := n.fs.h.GetDataInfo(n.fs.c, n.fs.bktID, obj.DataID)
	if err != nil {
		// DebugLog("[VFS getDataReader] ERROR: Failed to get DataInfo: objID=%d, DataID=%d, bktID=%d, error=%v, errorType=%T", obj.ID, obj.DataID, n.fs.bktID, err, err)
		// Try to check if DataID exists in database
		// DebugLog("[VFS getDataReader] Attempting to verify DataID existence: objID=%d, DataID=%d", obj.ID, obj.DataID)
		return nil, syscall.EIO
	}
	if dataInfo == nil {
		// DebugLog("[VFS getDataReader] ERROR: GetDataInfo returned nil: objID=%d, DataID=%d", obj.ID, obj.DataID)
		return nil, syscall.EIO
	}
	// DebugLog("[VFS getDataReader] Got DataInfo: objID=%d, DataID=%d, OrigSize=%d, Size=%d, Kind=0x%x, PkgID=%d, PkgOffset=%d",
	//	obj.ID, obj.DataID, dataInfo.OrigSize, dataInfo.Size, dataInfo.Kind, dataInfo.PkgID, dataInfo.PkgOffset)

	hasCompression := dataInfo.Kind&core.DATA_CMPR_MASK != 0
	hasEncryption := dataInfo.Kind&core.DATA_ENDEC_MASK != 0

	// DebugLog("[VFS getDataReader] Has compression: %v, Has encryption: %v", hasCompression, hasEncryption)

	// Always use bucket's default chunk size (force unified chunkSize)
	chunkSize := n.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Use unified chunkReader for both plain and compressed/encrypted data
	// Use dataID as cache key to ensure one file uses the same reader
	// This allows sharing chunk cache across all reads of the same file
	cacheKey := obj.DataID

	// Try to get cached reader
	if cached, ok := decodingReaderCache.Get(cacheKey); ok {
		if reader, ok := cached.(*chunkReader); ok && reader != nil {
			// DebugLog("[VFS getDataReader] Reusing cached reader: objID=%d, DataID=%d", obj.ID, obj.DataID)
			return reader, 0
		}
	}

	// Create new reader
	// Get encryption key from OrcasFS (not from bucket config)
	endecKey := getEndecKeyForFS(n.fs)

	// Create chunkReader (dataInfo is always available here)
	var reader *chunkReader
	if !hasCompression && !hasEncryption {
		// Plain data: create chunkReader with plain DataInfo
		// DebugLog("[VFS getDataReader] Creating plain reader: objID=%d, DataID=%d, OrigSize=%d, Size=%d, chunkSize=%d",
		//	obj.ID, obj.DataID, dataInfo.OrigSize, dataInfo.Size, chunkSize)
		plainDataInfo := &core.DataInfo{
			ID:       obj.DataID,
			OrigSize: dataInfo.OrigSize,
			Size:     dataInfo.Size,
			Kind:     0, // Plain data
		}
		reader = newChunkReader(n.fs.c, n.fs.h, n.fs.bktID, plainDataInfo, "", chunkSize)
	} else {
		// Compressed/encrypted: use chunkReader with processing
		// DebugLog("[VFS getDataReader] Creating compressed/encrypted reader: objID=%d, DataID=%d, OrigSize=%d, Size=%d, chunkSize=%d, hasCompression=%v, hasEncryption=%v",
		//	obj.ID, obj.DataID, dataInfo.OrigSize, dataInfo.Size, chunkSize, hasCompression, hasEncryption)
		reader = newChunkReader(n.fs.c, n.fs.h, n.fs.bktID, dataInfo, endecKey, chunkSize)
	}

	if reader == nil {
		// DebugLog("[VFS getDataReader] ERROR: Failed to create chunkReader: objID=%d, DataID=%d", obj.ID, obj.DataID)
		return nil, syscall.EIO
	}

	// Cache the reader (one per dataID)
	decodingReaderCache.Put(cacheKey, reader)
	// DebugLog("[VFS getDataReader] Created and cached new reader: objID=%d, DataID=%d, reader=%p", obj.ID, obj.DataID, reader)

	return reader, 0
}

// Write writes file content
// Optimization: reduce lock hold time, ra.Write itself is thread-safe
// Write implements FileWriter interface
func (n *OrcasNode) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	return n.writeImpl(ctx, data, off)
}

// writeImpl is the actual write implementation
func (n *OrcasNode) writeImpl(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	DebugLog("[VFS Write] Write called: objID=%d, offset=%d, size=%d", n.objID, off, len(data))

	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Write] ERROR: checkKey failed: objID=%d, errno=%d (EPERM=%d)", n.objID, errno, syscall.EPERM)
		return 0, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Write] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return 0, syscall.ENOENT
	}

	DebugLog("[VFS Write] Object info: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d",
		obj.ID, obj.Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, obj.Name, obj.PID)

	if obj.Type != core.OBJ_TYPE_FILE {
		DebugLog("[VFS Write] ERROR: Object is not a file (type=%d, expected FILE=%d): objID=%d, name=%s, PID=%d",
			obj.Type, core.OBJ_TYPE_FILE, obj.ID, obj.Name, obj.PID)
		// Check cache to see what's stored
		cacheKey := obj.ID
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				DebugLog("[VFS Write] Cached object info: objID=%d, type=%d, name=%s, PID=%d",
					cachedObj.ID, cachedObj.Type, cachedObj.Name, cachedObj.PID)
			}
		}
		// Check local cache
		if val := n.obj.Load(); val != nil {
			if localObj, ok := val.(*core.ObjectInfo); ok && localObj != nil {
				DebugLog("[VFS Write] Local cached object info: objID=%d, type=%d, name=%s, PID=%d",
					localObj.ID, localObj.Type, localObj.Name, localObj.PID)
			}
		}
		// Query database directly to verify
		objs, dbErr := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{obj.ID})
		if dbErr == nil && len(objs) > 0 {
			dbObj := objs[0]
			DebugLog("[VFS Write] Database object info: objID=%d, type=%d, name=%s, PID=%d",
				dbObj.ID, dbObj.Type, dbObj.Name, dbObj.PID)
		}
		return 0, syscall.EISDIR
	}

	// Get or create RandomAccessor (has internal lock, but releases quickly)
	ra, err := n.getRandomAccessor()
	if err != nil {
		DebugLog("[VFS Write] ERROR: Failed to get RandomAccessor for file objID=%d: %v", obj.ID, err)
		return 0, syscall.EIO
	}

	// Write data (don't flush immediately)
	// ra.Write itself is thread-safe, don't need to hold raMu lock
	DebugLog("[VFS Write] Writing data: fileID=%d, offset=%d, size=%d", obj.ID, off, len(data))
	err = ra.Write(off, data)
	if err != nil {
		DebugLog("[VFS Write] ERROR: Failed to write data: fileID=%d, offset=%d, size=%d, error=%v", obj.ID, off, len(data), err)
		return 0, syscall.EIO
	}

	// Invalidate object cache (will get latest size on next read)
	// This operation is fast, but can be optimized to async
	n.invalidateObj()

	DebugLog("[VFS Write] Successfully wrote data: fileID=%d, offset=%d, size=%d, written=%d", obj.ID, off, len(data), len(data))
	return uint32(len(data)), 0
}

// Flush implements NodeFlusher interface
// Optimization: use atomic operations, completely lock-free
func (n *OrcasNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	DebugLog("[VFS Flush] Entry: objID=%d, FileHandle=%v", n.objID, f)
	// Forward to FileHandle if it implements FileFlusher
	if f != nil {
		if fileFlusher, ok := f.(fs.FileFlusher); ok {
			errno := fileFlusher.Flush(ctx)
			DebugLog("[VFS Flush] Forwarded to FileHandle: objID=%d, errno=%d", n.objID, errno)
			return errno
		}
	}
	// Otherwise use our own implementation
	// Call flushImpl which doesn't need FileHandle
	return n.flushImpl(ctx)
}

// flushImpl is the actual flush implementation
func (n *OrcasNode) flushImpl(ctx context.Context) syscall.Errno {
	DebugLog("[VFS flushImpl] Entry: objID=%d", n.objID)
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Flush] ERROR: checkKey failed: objID=%d, errno=%d", n.objID, errno)
		return errno
	}

	// Atomically read ra
	val := n.ra.Load()
	if val == nil || val == releasedMarker {
		return 0
	}

	ra, ok := val.(*RandomAccessor)
	if !ok || ra == nil {
		return 0
	}

	// Execute Flush (ra.Flush is thread-safe)
	obj, err := n.getObj()
	if err == nil && obj != nil {
		DebugLog("[VFS Flush] Flushing file: fileID=%d, currentSize=%d", obj.ID, obj.Size)
	}
	versionID, err := ra.Flush()
	if err != nil {
		// Use n.objID instead of obj.ID to avoid nil pointer dereference
		fileID := n.objID
		if obj != nil {
			fileID = obj.ID
		}
		DebugLog("[VFS Flush] ERROR: Failed to flush file: fileID=%d, error=%v", fileID, err)
		return syscall.EIO
	}

	// IMPORTANT: After flush, get updated object from RandomAccessor's cache, NOT from database
	// ra.Flush() has already updated the cache with the correct size
	// Reading from database may return stale data due to SQLite WAL not being checkpointed yet
	// This fixes the bug where file size was incorrectly reverted to old value after flush
	fileID := n.objID
	if obj != nil {
		fileID = obj.ID
	}

	// Try to get updated object from RandomAccessor's cache first
	var updatedObj *core.ObjectInfo
	if cachedObj := ra.fileObj.Load(); cachedObj != nil {
		if loadedObj, ok := cachedObj.(*core.ObjectInfo); ok && loadedObj != nil {
			updatedObj = loadedObj
			DebugLog("[VFS Flush] Got updated object from RandomAccessor cache: fileID=%d, size=%d, dataID=%d, mtime=%d",
				updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
		}
	}

	// If not in RandomAccessor cache, try global cache
	if updatedObj == nil {
		if cached, ok := fileObjCache.Get(fileID); ok {
			if loadedObj, ok := cached.(*core.ObjectInfo); ok && loadedObj != nil {
				updatedObj = loadedObj
				DebugLog("[VFS Flush] Got updated object from global cache: fileID=%d, size=%d, dataID=%d, mtime=%d",
					updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
			}
		}
	}

	// Only if cache is empty, read from database (this should rarely happen)
	if updatedObj == nil {
		DebugLog("[VFS Flush] WARNING: Object not in cache after flush, reading from database: fileID=%d", fileID)
		updatedObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
		if err == nil && len(updatedObjs) > 0 {
			updatedObj = updatedObjs[0]
			DebugLog("[VFS Flush] Got updated object from database: fileID=%d, size=%d, dataID=%d, mtime=%d",
				updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
		}
	}

	if updatedObj != nil {
		// Update global file object cache with latest metadata
		cacheKey := updatedObj.ID
		fileObjCache.Put(cacheKey, updatedObj)
		DebugLog("[VFS Flush] Updated file object cache: fileID=%d, size=%d, dataID=%d, mtime=%d",
			updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)

		// Update local cache
		n.obj.Store(updatedObj)

		// IMPORTANT: Update directory listing cache after flush to ensure file is visible
		// This is critical for macOS/SMB where files may disappear if not in directory cache
		// macOS/SMB relies heavily on directory listing cache, and if file is not in cache,
		// it may return error -43 (file not found) even though file exists in database
		if updatedObj.PID > 0 {
			dirNode := &OrcasNode{
				fs:    n.fs,
				objID: updatedObj.PID,
			}
			dirNode.invalidateDirListCache(updatedObj.PID)
			DebugLog("[VFS Flush] Updated directory listing cache after flush: fileID=%d, dirID=%d, name=%s, size=%d",
				updatedObj.ID, updatedObj.PID, updatedObj.Name, updatedObj.Size)
		}

		DebugLog("[VFS Flush] Successfully flushed file: fileID=%d, versionID=%d, finalSize=%d, dataID=%d, mtime=%d",
			updatedObj.ID, versionID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
	} else {
		// If we can't get updated object, at least invalidate cache
		n.invalidateObj()
		DebugLog("[VFS Flush] Successfully flushed file: fileID=%d, versionID=%d (failed to get final obj)", fileID, versionID)
	}

	return 0
}

// Fsync implements NodeFsyncer interface
func (n *OrcasNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	DebugLog("[VFS Fsync] Entry: objID=%d, FileHandle=%v, flags=0x%x", n.objID, f, flags)
	// Forward to FileHandle if it implements FileFsyncer
	if f != nil {
		if fileFsyncer, ok := f.(fs.FileFsyncer); ok {
			errno := fileFsyncer.Fsync(ctx, flags)
			DebugLog("[VFS Fsync] Forwarded to FileHandle: objID=%d, errno=%d", n.objID, errno)
			return errno
		}
	}
	// Otherwise use our own implementation
	return n.fsyncImpl(ctx, flags)
}

// fsyncImpl is the actual fsync implementation
func (n *OrcasNode) fsyncImpl(ctx context.Context, flags uint32) syscall.Errno {
	DebugLog("[VFS fsyncImpl] Entry: objID=%d, flags=0x%x", n.objID, flags)
	// Flush RandomAccessor first
	if errno := n.Flush(ctx, nil); errno != 0 {
		DebugLog("[VFS Fsync] ERROR: Flush failed: objID=%d, flags=0x%x, errno=%d", n.objID, flags, errno)
		return errno
	}

	// Flush object cache
	n.invalidateObj()
	return 0
}

// Setattr sets file attributes (including truncate operation)
func (n *OrcasNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	DebugLog("[VFS Setattr] Entry: objID=%d, valid=0x%x, FileHandle=%v", n.objID, in.Valid, f)
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Setattr] ERROR: checkKey failed: objID=%d, valid=0x%x, errno=%d", n.objID, in.Valid, errno)
		return errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Setattr] ERROR: Failed to get object: objID=%d, valid=0x%x, error=%v", n.objID, in.Valid, err)
		return syscall.ENOENT
	}

	// Handle truncate operation
	if in.Valid&fuse.FATTR_SIZE != 0 {
		newSize := int64(in.Size)
		oldSize := obj.Size

		// If size changed, execute truncate
		if newSize != oldSize {
			if errno := n.truncateFile(newSize); errno != 0 {
				return errno
			}
			obj.Size = newSize
		}
	}

	// Handle mode (permissions) change
	needUpdate := false
	oldMode := obj.Mode
	if in.Valid&fuse.FATTR_MODE != 0 {
		oldModeValue := getModeFromObj(obj)
		DebugLog("[VFS Setattr] Setting file mode: objID=%d, oldMode=0%o, newMode=0%o, oldModeField=%d", n.objID, oldModeValue, in.Mode, oldMode)
		setModeInObj(obj, in.Mode)
		needUpdate = true
		DebugLog("[VFS Setattr] Mode updated: objID=%d, newModeField=%d", n.objID, obj.Mode)
	}

	// Update modification time
	if in.Valid&fuse.FATTR_MTIME != 0 {
		oldMTime := obj.MTime
		obj.MTime = int64(in.Mtime)
		DebugLog("[VFS Setattr] Setting mtime: objID=%d, oldMTime=%d, newMTime=%d", n.objID, oldMTime, obj.MTime)
		needUpdate = true
	}

	// Update access time
	if in.Valid&fuse.FATTR_ATIME != 0 {
		// Note: We don't store atime separately, but we can update it if needed
		// For now, we'll just use mtime for atime
		DebugLog("[VFS Setattr] Setting atime: objID=%d, atime=%d", n.objID, in.Atime)
	}

	// Update change time (ctime is typically updated automatically on any change)
	// We'll set it to current time if any attribute changed
	if needUpdate {
		// CTime is typically set to current time when any attribute changes
		// But we'll use the provided ctime if available, otherwise use mtime
		if in.Valid&fuse.FATTR_CTIME != 0 {
			DebugLog("[VFS Setattr] Setting ctime: objID=%d, ctime=%d", n.objID, in.Ctime)
		}
	}

	// Update object information to database if needed
	if needUpdate {
		// If mode or mtime changed, update through Handler
		_, err := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{obj})
		if err != nil {
			DebugLog("[VFS Setattr] ERROR: Failed to update object: objID=%d, error=%v", n.objID, err)
			return syscall.EIO
		}
		DebugLog("[VFS Setattr] Successfully updated object in database: objID=%d, mode=%d, mtime=%d", n.objID, obj.Mode, obj.MTime)

		// Update global file object cache to ensure consistency
		cacheKey := obj.ID
		fileObjCache.Put(cacheKey, obj)
		DebugLog("[VFS Setattr] Updated file object cache: objID=%d", n.objID)

		// Update local cache to ensure immediate consistency
		n.obj.Store(obj)
		DebugLog("[VFS Setattr] Updated local object cache: objID=%d", n.objID)

		// If this is a child object, also update parent directory listing cache
		// This ensures that directory listings show updated permissions
		if obj.PID > 0 {
			parentCacheKey := n.getDirListCacheKey(obj.PID)
			dirListCache.Del(parentCacheKey)
			DebugLog("[VFS Setattr] Invalidated parent directory listing cache: objID=%d, parentID=%d", n.objID, obj.PID)
		}
	}

	// Note: File size update has been completed in truncateFile through Flush
	// Here only need to update mtime and mode (if set)

	// Invalidate cache (this will force re-fetch on next access)
	// But we've already updated caches above, so this is just for safety
	n.invalidateObj()

	// Fill output with updated values
	out.Mode = getModeFromObj(obj)
	out.Size = uint64(obj.Size)
	out.Mtime = uint64(obj.MTime)

	// Set ctime and atime
	if in.Valid&fuse.FATTR_CTIME != 0 {
		out.Ctime = in.Ctime
	} else {
		// Use mtime as ctime if ctime not explicitly set
		out.Ctime = out.Mtime
	}

	if in.Valid&fuse.FATTR_ATIME != 0 {
		out.Atime = in.Atime
	} else {
		// Use mtime as atime if atime not explicitly set
		out.Atime = out.Mtime
	}

	DebugLog("[VFS Setattr] Successfully completed: objID=%d, mode=0%o, size=%d, mtime=%d", n.objID, out.Mode, out.Size, out.Mtime)
	return 0
}

// truncateFile truncates file to specified size
func (n *OrcasNode) truncateFile(newSize int64) syscall.Errno {
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS truncateFile] ERROR: checkKey failed: objID=%d, errno=%d (EPERM=%d)", n.objID, errno, syscall.EPERM)
		return errno
	}

	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return syscall.EISDIR
	}

	oldSize := obj.Size

	// If new size equals old size, no operation needed
	if newSize == oldSize {
		return 0
	}

	// If new size is less than old size, need to truncate (delete excess part)
	if newSize < oldSize {
		// If have RandomAccessor, need to clean write operations in buffer that exceed new size
		val := n.ra.Load()
		var ra *RandomAccessor
		if val != nil {
			if r, ok := val.(*RandomAccessor); ok && r != nil {
				// Validate RandomAccessor is properly initialized
				if r.fs != nil && r.fileID > 0 && r.fileID == obj.ID {
					ra = r
				} else {
					DebugLog("[VFS truncateFile] WARNING: Invalid RandomAccessor detected: fileID=%d, ra.fileID=%d, ra.fs=%v, objID=%d", n.objID, r.fileID, r.fs != nil, obj.ID)
				}
			}
		}
		if ra != nil {
			// Use RandomAccessor's Truncate method
			// This will reference previous data block but with new size, and create new version
			_, err := ra.Truncate(newSize)
			if err != nil {
				DebugLog("[VFS truncateFile] ERROR: Failed to truncate using RandomAccessor: objID=%d, newSize=%d, error=%v", n.objID, newSize, err)
				return syscall.EIO
			}
		} else {
			// No RandomAccessor or invalid RandomAccessor, create one and use Truncate method
			DebugLog("[VFS truncateFile] Creating new RandomAccessor for truncate: objID=%d, newSize=%d", obj.ID, newSize)

			// Remove old RandomAccessor from global registry if exists
			// We need to get the old RA first to properly unregister it
			var oldRA *RandomAccessor
			if n.fs != nil {
				oldRA = n.fs.getRandomAccessorByFileID(obj.ID)
				if oldRA != nil {
					n.fs.unregisterRandomAccessor(obj.ID, oldRA)
					DebugLog("[VFS truncateFile] Unregistered old RandomAccessor: objID=%d, oldRA=%p", obj.ID, oldRA)
				}
			}

			ra, err := NewRandomAccessor(n.fs, obj.ID)
			if err != nil {
				DebugLog("[VFS truncateFile] ERROR: Failed to create RandomAccessor: objID=%d, error=%v", obj.ID, err)
				return syscall.EIO
			}

			// Store the new RandomAccessor so subsequent operations can use it
			n.ra.Store(ra)

			// Register the new RandomAccessor in global registry
			if n.fs != nil {
				n.fs.registerRandomAccessor(obj.ID, ra)
				DebugLog("[VFS truncateFile] Registered new RandomAccessor: objID=%d, ra=%p", obj.ID, ra)
			}

			_, err = ra.Truncate(newSize)
			if err != nil {
				DebugLog("[VFS truncateFile] ERROR: Failed to truncate with new RandomAccessor: objID=%d, newSize=%d, error=%v", obj.ID, newSize, err)
				return syscall.EIO
			}
		}
	} else {
		// New size is greater than old size, extend file
		// Optimization: For fallocate-like operations (pre-allocation), we don't need to write zeros
		// Just update the file size metadata (sparse file support)
		// This is much faster for large file pre-allocation (e.g., qBittorrent)
		ra, err := n.getRandomAccessor()
		if err != nil {
			// If can't get RandomAccessor, just update size metadata
			updateFileObj := &core.ObjectInfo{
				ID:     obj.ID,
				PID:    obj.PID,
				DataID: obj.DataID,
				Size:   newSize,
				MTime:  core.Now(),
				Type:   obj.Type,
				Name:   obj.Name,
			}
			_, err := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{updateFileObj})
			if err != nil {
				return syscall.EIO
			}
			return 0
		}

		// Mark as sparse file for optimization
		ra.MarkSparseFile(newSize)

		// Update file object size directly (sparse file - no data allocation)
		updateFileObj := &core.ObjectInfo{
			ID:     obj.ID,
			PID:    obj.PID,
			DataID: obj.DataID,
			Size:   newSize, // Update size without allocating data
			MTime:  core.Now(),
			Type:   obj.Type,
			Name:   obj.Name,
		}

		// Update file object in database
		_, err = n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			return syscall.EIO
		}
	}

	return 0
}

// getRandomAccessor gets or creates RandomAccessor (lazy loading)
// Optimization: use atomic operations, completely lock-free
func (n *OrcasNode) getRandomAccessor() (*RandomAccessor, error) {
	// First check: atomic read (fast path)
	if val := n.ra.Load(); val != nil {
		// Check if it's the released marker
		if val == releasedMarker {
			// Was released, need to create new one
		} else if ra, ok := val.(*RandomAccessor); ok && ra != nil {
			return ra, nil
		}
	}

	obj, err := n.getObj()
	if err != nil {
		return nil, err
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return nil, fmt.Errorf("object is not a file")
	}

	// IMPORTANT: Check for existing RandomAccessor with TempFileWriter for ALL files
	// Use hasTempFileWriter() instead of isTempFile() because:
	// 1. File may have been renamed (removed .tmp suffix) but TempFileWriter still exists
	// 2. TempFileWriter existence is the authoritative indicator, not the file name
	// 3. We must reuse the existing RandomAccessor to avoid creating a new one without TempFileWriter
	//    which would lead to random write mode and new dataID creation
	if n.fs != nil {
		// First check without lock (fast path)
		if existing := n.fs.getRandomAccessorByFileID(obj.ID); existing != nil && existing.hasTempFileWriter() {
			n.ra.Store(existing)
			DebugLog("[VFS getRandomAccessor] Reusing existing RandomAccessor with TempFileWriter (fast path): fileID=%d, ra=%p", obj.ID, existing)
			return existing, nil
		}

		// Check if there's an existing RandomAccessor with TempFileWriter
		// If so, acquire lock to prevent race conditions during creation
		existingRA := n.fs.getRandomAccessorByFileID(obj.ID)
		hasExistingTempWriter := existingRA != nil && existingRA.hasTempFileWriter()
		// Also check for .tmp files (new files that haven't been renamed yet)
		isNewTmpFile := isTempFile(obj)

		if isNewTmpFile || hasExistingTempWriter {
			n.fs.raCreateMu.Lock()
			defer n.fs.raCreateMu.Unlock()

			// Double-check after acquiring lock
			if existing := n.fs.getRandomAccessorByFileID(obj.ID); existing != nil && existing.hasTempFileWriter() {
				n.ra.Store(existing)
				DebugLog("[VFS getRandomAccessor] Reusing existing RandomAccessor with TempFileWriter (after lock): fileID=%d, ra=%p", obj.ID, existing)
				return existing, nil
			}
		}
	}

	// Create RandomAccessor (no auto-registration in NewRandomAccessor anymore)
	newRA, err := NewRandomAccessor(n.fs, obj.ID)
	if err != nil {
		return nil, err
	}

	// For .tmp files or files with existing TempFileWriter, we may be holding the lock
	// Check again before registering to ensure we don't create duplicate RandomAccessor
	existingRAForRegister := n.fs.getRandomAccessorByFileID(obj.ID)
	hasExistingTempWriterForRegister := existingRAForRegister != nil && existingRAForRegister.hasTempFileWriter()
	isNewTmpFileForRegister := isTempFile(obj)

	if n.fs != nil && (isNewTmpFileForRegister || hasExistingTempWriterForRegister) {
		// Check if another goroutine registered a RandomAccessor with TempFileWriter while we were creating newRA
		if existing := n.fs.getRandomAccessorByFileID(obj.ID); existing != nil && existing.hasTempFileWriter() {
			// Another goroutine registered a RandomAccessor with TempFileWriter
			// Close what we created and use the existing one
			newRA.Close()
			n.ra.Store(existing)
			DebugLog("[VFS getRandomAccessor] Using existing RandomAccessor with TempFileWriter: fileID=%d, ra=%p", obj.ID, existing)
			return existing, nil
		}
		// Only register if it's a new .tmp file (for renamed files with TempFileWriter, registration happens below)
		if isNewTmpFileForRegister {
			// Register the new RandomAccessor (we're holding the lock, so this is safe)
			n.fs.registerRandomAccessor(obj.ID, newRA)
		}
	}

	// Try to atomically set ra (if already set by other goroutine, use existing)
	// Try swapping from nil or releasedMarker
	oldVal := n.ra.Load()
	if !n.ra.CompareAndSwap(oldVal, newRA) {
		// Other goroutine already created, close what we created, use existing
		newRA.Close()
		if n.fs != nil {
			DebugLog("[VFS getRandomAccessor] Unregistering RandomAccessor: fileID=%d", obj.ID)
			n.fs.unregisterRandomAccessor(obj.ID, newRA)
		}
		if val := n.ra.Load(); val != nil && val != releasedMarker {
			if ra, ok := val.(*RandomAccessor); ok && ra != nil {
				return ra, nil
			}
		}
		// If we get here, something unexpected happened, try again
		return n.getRandomAccessor()
	}

	// Register RandomAccessor globally for cross-node lookup (e.g., rename flush)
	// For new .tmp files, registration was already done above while holding the lock
	// For non-.tmp files (including renamed files), check if there's already a RandomAccessor with TempFileWriter before registering
	if n.fs != nil && !isTempFile(obj) {
		// IMPORTANT: Before registering, check if there's already a RandomAccessor with TempFileWriter
		// This handles the case where file was renamed from .tmp but still has active writes
		// Use hasTempFileWriter() instead of isTempFile() as the authoritative check
		if existing := n.fs.getRandomAccessorByFileID(obj.ID); existing != nil && existing.hasTempFileWriter() {
			// There's already a RandomAccessor with TempFileWriter, use it instead
			newRA.Close()
			n.ra.Store(existing)
			DebugLog("[VFS getRandomAccessor] Found existing RandomAccessor with TempFileWriter for renamed file, reusing: fileID=%d, ra=%p", obj.ID, existing)
			return existing, nil
		}
		DebugLog("[VFS getRandomAccessor] Registering RandomAccessor: fileID=%d", obj.ID)
		n.fs.registerRandomAccessor(obj.ID, newRA)
	}

	return newRA, nil
}

// updateFileObjCache updates file object cache from RandomAccessor if it exists
// This is used to update cache before database operations to ensure consistency
func (n *OrcasNode) updateFileObjCache(fileID int64, newName string, newPID int64) {
	// Try to get RandomAccessor from cache (if file is open)
	// First check if this node has a RandomAccessor for the source file
	val := n.ra.Load()
	if val != nil && val != releasedMarker {
		if ra, ok := val.(*RandomAccessor); ok && ra != nil && ra.fileID == fileID {
			n.updateFileObjCacheFromAccessor(ra, newName, newPID)
			return
		}
	}

	// Try registry for cross-node RandomAccessor
	if n.fs != nil {
		if ra := n.fs.getRandomAccessorByFileID(fileID); ra != nil {
			n.updateFileObjCacheFromAccessor(ra, newName, newPID)
			return
		}
	}

	// If this node doesn't have the RandomAccessor, try to update global cache
	// by getting the file object and updating it
	cacheKey := fileID
	if cached, ok := fileObjCache.Get(cacheKey); ok {
		if fileObj, ok := cached.(*core.ObjectInfo); ok && fileObj != nil {
			// Update cached object with new name and parent
			updatedFileObj := &core.ObjectInfo{
				ID:     fileObj.ID,
				PID:    newPID,
				DataID: fileObj.DataID,
				Size:   fileObj.Size,
				MTime:  fileObj.MTime,
				Type:   fileObj.Type,
				Name:   newName,
			}
			fileObjCache.Put(cacheKey, updatedFileObj)
		}
	}
}

func (n *OrcasNode) updateFileObjCacheFromAccessor(ra *RandomAccessor, newName string, newPID int64) {
	if ra == nil {
		return
	}
	fileObj, err := ra.getFileObj()
	if err == nil && fileObj != nil {
		updatedFileObj := &core.ObjectInfo{
			ID:     fileObj.ID,
			PID:    newPID,
			DataID: fileObj.DataID,
			Size:   fileObj.Size,
			MTime:  fileObj.MTime,
			Type:   fileObj.Type,
			Name:   newName,
		}
		ra.fileObj.Store(updatedFileObj)
		fileObjCache.Put(ra.fileObjKey, updatedFileObj)
	}
}

// forceFlushTempFileBeforeRename ensures .tmp files flush pending data before renaming away from .tmp
// - If TempFileWriter: directly sync flush, no waiting
// - Cache: strong consistency (always read from database and update cache)
func (n *OrcasNode) forceFlushTempFileBeforeRename(fileID int64, oldName, newName string) {
	DebugLog("[VFS Rename] .tmp file being renamed, forcing flush: fileID=%d, oldName=%s, newName=%s", fileID, oldName, newName)

	// Ensure file has a DataID - if not, pre-allocate one
	// This ensures the file always has a DataID even before flush completes
	{
		// Ensure file has a DataID - if not, pre-allocate one
		// This ensures the file always has a DataID even before flush completes
		cacheKey := fileID
		fileObjCache.Del(cacheKey) // Invalidate cache to get fresh data
		objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
		if err == nil && len(objs) > 0 {
			fileObj := objs[0]
			// For empty files (Size = 0), EmptyDataID is valid and should be preserved
			// Only pre-allocate DataID if file has data (Size > 0) but no DataID
			if (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) && fileObj.Size > 0 {
				// File has data but no DataID, pre-allocate one
				newDataID := core.NewID()
				if newDataID > 0 {
					DebugLog("[VFS Rename] Pre-allocating DataID for .tmp file: fileID=%d, dataID=%d, size=%d", fileID, newDataID, fileObj.Size)
					// Update file object with pre-allocated DataID
					updateFileObj := &core.ObjectInfo{
						ID:     fileObj.ID,
						PID:    fileObj.PID,
						Type:   fileObj.Type,
						Name:   fileObj.Name,
						DataID: newDataID,
						Size:   fileObj.Size, // Preserve existing size
						MTime:  core.Now(),
					}
					_, putErr := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{updateFileObj})
					if putErr == nil {
						// Update cache
						fileObjCache.Put(cacheKey, updateFileObj)
						DebugLog("[VFS Rename] Successfully pre-allocated DataID: fileID=%d, dataID=%d", fileID, newDataID)
					} else {
						DebugLog("[VFS Rename] WARNING: Failed to pre-allocate DataID: fileID=%d, error=%v", fileID, putErr)
					}
				} else {
					DebugLog("[VFS Rename] WARNING: Failed to generate DataID for pre-allocation: fileID=%d", fileID)
				}
			} else if fileObj.Size == 0 {
				// Empty file, EmptyDataID is valid, no need to pre-allocate
				DebugLog("[VFS Rename] Empty .tmp file (Size=0), EmptyDataID is valid, no pre-allocation needed: fileID=%d, dataID=%d", fileID, fileObj.DataID)
			}
		}
	}

	var raToFlush *RandomAccessor
	if val := n.ra.Load(); val != nil && val != releasedMarker {
		if ra, ok := val.(*RandomAccessor); ok && ra != nil && ra.fileID == fileID {
			raToFlush = ra
		}
	}
	if raToFlush == nil && n.fs != nil {
		raToFlush = n.fs.getRandomAccessorByFileID(fileID)
	}

	// Get file object to check size
	cacheKey := fileID
	fileObjCache.Del(cacheKey) // Invalidate cache to get fresh data
	objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
	var fileObj *core.ObjectInfo
	if err == nil && len(objs) > 0 {
		fileObj = objs[0]
	}

	// Check if file is large (uses TempFileWriter) or empty (Size=0)
	isLargeFile := false
	isEmptyFile := false
	if fileObj != nil {
		isEmptyFile = fileObj.Size == 0
		// Large file threshold: 1MB (files larger than this use TempFileWriter)
		isLargeFile = fileObj.Size > 1<<20
	}

	// For large files or empty files, force flush to disk
	shouldForceFlush := isLargeFile || isEmptyFile

	// Check if file has TempFileWriter - if so, directly sync flush (no waiting)
	if raToFlush != nil && raToFlush.hasTempFileWriter() {
		DebugLog("[VFS Rename] File has TempFileWriter, directly syncing flush: fileID=%d, size=%d, isLargeFile=%v, isEmptyFile=%v", fileID, fileObj.Size, isLargeFile, isEmptyFile)
		if err := raToFlush.flushTempFileWriter(); err != nil {
			DebugLog("[VFS Rename] ERROR: Failed to flush TempFileWriter: fileID=%d, error=%v", fileID, err)
		} else {
			DebugLog("[VFS Rename] Successfully flushed TempFileWriter: fileID=%d", fileID)
			// Strong consistency: invalidate cache and re-fetch from database
			cacheKey := fileID
			fileObjCache.Del(cacheKey)
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
			if err == nil && len(objs) > 0 {
				fileObj = objs[0]
				// For empty files, ensure EmptyDataID is set
				if fileObj.Size == 0 && fileObj.DataID != core.EmptyDataID {
					DebugLog("[VFS Rename] Empty file detected, setting EmptyDataID: fileID=%d, currentDataID=%d", fileID, fileObj.DataID)
					updateFileObj := &core.ObjectInfo{
						ID:     fileObj.ID,
						PID:    fileObj.PID,
						Type:   fileObj.Type,
						Name:   fileObj.Name,
						DataID: core.EmptyDataID,
						Size:   0,
						MTime:  core.Now(),
					}
					_, putErr := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{updateFileObj})
					if putErr == nil {
						fileObj = updateFileObj
						DebugLog("[VFS Rename] Successfully set EmptyDataID for empty file: fileID=%d", fileID)
					} else {
						DebugLog("[VFS Rename] WARNING: Failed to set EmptyDataID for empty file: fileID=%d, error=%v", fileID, putErr)
					}
				}
				fileObjCache.Put(cacheKey, fileObj)
				DebugLog("[VFS Rename] Strong consistency: re-fetched from database after TempFileWriter flush: fileID=%d, dataID=%d, size=%d", fileID, fileObj.DataID, fileObj.Size)
				// After sync flush, append file to directory listing cache
				if fileObj.PID > 0 {
					dirNode := &OrcasNode{
						fs:    n.fs,
						objID: fileObj.PID,
					}
					dirNode.invalidateDirListCache(fileObj.PID)
					DebugLog("[VFS Rename] Appended file to directory listing cache after sync flush: fileID=%d, dirID=%d, name=%s", fileID, fileObj.PID, fileObj.Name)
				}
			}
		}
		// IMPORTANT: Do NOT unregister RandomAccessor immediately after flush
		// There may be concurrent writes still in progress that need to use the same TempFileWriter
		// Unregistering too early can cause subsequent writes to create a new RandomAccessor
		// without TempFileWriter, leading to random write mode and new dataID creation
		// The RandomAccessor will be unregistered when the file is closed or when no longer needed
		// DebugLog("[VFS Rename] Keeping RandomAccessor registered after TempFileWriter flush to allow concurrent writes: fileID=%d", fileID)
		return
	}

	// For large files or empty files without TempFileWriter, force flush
	if shouldForceFlush && raToFlush != nil {
		DebugLog("[VFS Rename] Large file or empty file detected, forcing flush: fileID=%d, size=%d, isLargeFile=%v, isEmptyFile=%v", fileID, fileObj.Size, isLargeFile, isEmptyFile)
		if _, err := raToFlush.ForceFlush(); err != nil {
			DebugLog("[VFS Rename] ERROR: Failed to force flush: fileID=%d, error=%v", fileID, err)
		} else {
			DebugLog("[VFS Rename] Successfully force flushed: fileID=%d", fileID)
			// Get updated object from cache first (ForceFlush already updated it)
			// Only read from database if cache is empty (to avoid WAL stale read)
			cacheKey := fileID
			var fileObj *core.ObjectInfo
			if cached, ok := fileObjCache.Get(cacheKey); ok {
				if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
					fileObj = obj
					DebugLog("[VFS Rename] Got file object from cache after flush: fileID=%d, size=%d, dataID=%d", fileID, fileObj.Size, fileObj.DataID)
				}
			}
			if fileObj == nil {
				DebugLog("[VFS Rename] WARNING: File object not in cache after flush, reading from database: fileID=%d", fileID)
				objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
				if err == nil && len(objs) > 0 {
					fileObj = objs[0]
				}
			}
			if fileObj != nil {
				// For empty files, ensure EmptyDataID is set
				if fileObj.Size == 0 && fileObj.DataID != core.EmptyDataID {
					DebugLog("[VFS Rename] Empty file detected after flush, setting EmptyDataID: fileID=%d, currentDataID=%d", fileID, fileObj.DataID)
					updateFileObj := &core.ObjectInfo{
						ID:     fileObj.ID,
						PID:    fileObj.PID,
						Type:   fileObj.Type,
						Name:   fileObj.Name,
						DataID: core.EmptyDataID,
						Size:   0,
						MTime:  core.Now(),
					}
					_, putErr := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{updateFileObj})
					if putErr == nil {
						fileObj = updateFileObj
						DebugLog("[VFS Rename] Successfully set EmptyDataID for empty file: fileID=%d", fileID)
					} else {
						DebugLog("[VFS Rename] WARNING: Failed to set EmptyDataID for empty file: fileID=%d, error=%v", fileID, putErr)
					}
				}
				fileObjCache.Put(cacheKey, fileObj)
				DebugLog("[VFS Rename] Re-fetched after force flush: fileID=%d, dataID=%d, size=%d", fileID, fileObj.DataID, fileObj.Size)
			}
		}
	}

	// If RandomAccessor exists but no TempFileWriter, flush its buffer first
	// This handles the case where data is in RandomAccessor's buffer but not yet flushed
	if raToFlush != nil && !raToFlush.hasTempFileWriter() {
		// Check if RandomAccessor has pending writes
		writeIndex := atomic.LoadInt64(&raToFlush.buffer.writeIndex)
		totalSize := atomic.LoadInt64(&raToFlush.buffer.totalSize)
		if writeIndex > 0 || totalSize > 0 {
			DebugLog("[VFS Rename] RandomAccessor has pending writes, flushing: fileID=%d, writeIndex=%d, totalSize=%d", fileID, writeIndex, totalSize)
			if _, err := raToFlush.ForceFlush(); err != nil {
				DebugLog("[VFS Rename] WARNING: Failed to flush RandomAccessor buffer: fileID=%d, error=%v", fileID, err)
			} else {
				DebugLog("[VFS Rename] Successfully flushed RandomAccessor buffer: fileID=%d", fileID)
			}
		}
	}

	// If RandomAccessor exists but no TempFileWriter, flush its buffer first
	// This handles the case where data is in RandomAccessor's buffer but not yet flushed
	if raToFlush != nil && !raToFlush.hasTempFileWriter() {
		// Check if RandomAccessor has pending writes
		writeIndex := atomic.LoadInt64(&raToFlush.buffer.writeIndex)
		totalSize := atomic.LoadInt64(&raToFlush.buffer.totalSize)
		if writeIndex > 0 || totalSize > 0 {
			DebugLog("[VFS Rename] RandomAccessor has pending writes, flushing: fileID=%d, writeIndex=%d, totalSize=%d", fileID, writeIndex, totalSize)
			if _, err := raToFlush.ForceFlush(); err != nil {
				DebugLog("[VFS Rename] WARNING: Failed to flush RandomAccessor buffer: fileID=%d, error=%v", fileID, err)
			} else {
				DebugLog("[VFS Rename] Successfully flushed RandomAccessor buffer: fileID=%d", fileID)
			}
		}
	}

	// If RandomAccessor exists, flush it
	if raToFlush != nil {
		DebugLog("[VFS Rename] Flushing RandomAccessor: fileID=%d", fileID)
		if _, err := raToFlush.ForceFlush(); err != nil {
			DebugLog("[VFS Rename] WARNING: Failed to flush RandomAccessor: fileID=%d, error=%v", fileID, err)
		}

		// Get updated object from cache first (ForceFlush already updated it)
		// Only read from database if cache is empty (to avoid WAL stale read)
		cacheKey := fileID
		var fileObj *core.ObjectInfo
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
				fileObj = obj
				DebugLog("[VFS Rename] Got file object from cache after flush: fileID=%d, size=%d, dataID=%d", fileID, fileObj.Size, fileObj.DataID)
			}
		}
		if fileObj == nil {
			DebugLog("[VFS Rename] WARNING: File object not in cache after flush, reading from database: fileID=%d", fileID)
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
			if err == nil && len(objs) > 0 {
				fileObj = objs[0]
			}
		}
		if fileObj != nil {
			// For empty files (Size = 0), EmptyDataID is valid
			if fileObj.Size == 0 {
				fileObjCache.Put(cacheKey, fileObj)
				DebugLog("[VFS Rename] Empty file after batch flush (EmptyDataID is valid): fileID=%d, dataID=%d, size=%d", fileID, fileObj.DataID, fileObj.Size)
			} else {
				// Retry only if DataID is missing (error case) and file is not empty
				DebugLog("[VFS Rename] WARNING: No DataID after batch flush, retrying: fileID=%d, size=%d", fileID, fileObj.Size)
				maxRetries := 10
				for retry := 0; retry < maxRetries; retry++ {
					fileObjCache.Del(cacheKey)
					objs, err = n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
					if err == nil && len(objs) > 0 {
						fileObj = objs[0]
						// Check if file is empty - if so, EmptyDataID is valid
						if fileObj.Size == 0 {
							fileObjCache.Put(cacheKey, fileObj)
							DebugLog("[VFS Rename] Empty file after retry (EmptyDataID is valid): fileID=%d, dataID=%d, size=%d", fileID, fileObj.DataID, fileObj.Size)
							break
						}
						if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
							fileObjCache.Put(cacheKey, fileObj)
							DebugLog("[VFS Rename] Successfully got DataID after retry (retry %d/%d): fileID=%d, dataID=%d, size=%d", retry+1, maxRetries, fileID, fileObj.DataID, fileObj.Size)
							break
						}
					}
					if retry < maxRetries-1 {
						time.Sleep(50 * time.Millisecond) // Only wait on error retry
					}
				}
			}
		}

		// For empty files (Size = 0), EmptyDataID is valid
		if fileObj != nil {
			if fileObj.Size == 0 || (fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID) {
				// Update file object with new name (will be updated by Rename operation later)
				// For now, just ensure it's in cache with current name
				// The Rename operation will update the name in database and cache
				fileObjCache.Put(cacheKey, fileObj)
				DebugLog("[VFS Rename] File flushed and cached (name will be updated by Rename): fileID=%d, dataID=%d, size=%d, currentName=%s, newName=%s", fileID, fileObj.DataID, fileObj.Size, fileObj.Name, newName)
				// Note: Directory listing cache will be updated by Rename operation after database update
			} else {
				DebugLog("[VFS Rename] WARNING: File still has no DataID after batch flush and retries: fileID=%d, size=%d", fileID, fileObj.Size)
			}
		}

		// IMPORTANT: Do NOT unregister RandomAccessor immediately after batch flush
		// There may be concurrent writes still in progress that need to use the same RandomAccessor
		// Unregistering too early can cause subsequent writes to create a new RandomAccessor
		// without TempFileWriter, leading to random write mode and new dataID creation
		// The RandomAccessor will be unregistered when the file is closed or when no longer needed
		// DebugLog("[VFS Rename] Keeping RandomAccessor registered after batch flush to allow concurrent writes: fileID=%d", fileID)
		return
	}

	// If no RandomAccessor found, try to create a temporary RandomAccessor to flush any pending data
	if raToFlush == nil {
		DebugLog("[VFS Rename] No RandomAccessor found, trying to create temporary RandomAccessor to flush: fileID=%d", fileID)
		tempRA, err := NewRandomAccessor(n.fs, fileID)
		if err == nil && tempRA != nil {
			// Check if it has TempFileWriter
			if tempRA.hasTempFileWriter() {
				// Directly sync flush TempFileWriter
				if err := tempRA.flushTempFileWriter(); err != nil {
					DebugLog("[VFS Rename] WARNING: Failed to flush temporary TempFileWriter: fileID=%d, error=%v", fileID, err)
				} else {
					DebugLog("[VFS Rename] Successfully flushed temporary TempFileWriter: fileID=%d", fileID)
				}
			} else {
				// Try to flush any pending data
				if _, flushErr := tempRA.ForceFlush(); flushErr != nil {
					DebugLog("[VFS Rename] WARNING: Failed to flush temporary RandomAccessor: fileID=%d, error=%v", fileID, flushErr)
				} else {
					DebugLog("[VFS Rename] Successfully flushed temporary RandomAccessor: fileID=%d", fileID)
				}
			}
			// Strong consistency: invalidate cache and re-fetch from database
			cacheKey := fileID
			fileObjCache.Del(cacheKey)
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
			if err == nil && len(objs) > 0 {
				fileObj := objs[0]
				// For empty files, ensure EmptyDataID is set
				if fileObj.Size == 0 && fileObj.DataID != core.EmptyDataID {
					DebugLog("[VFS Rename] Empty file detected after temporary flush, setting EmptyDataID: fileID=%d, currentDataID=%d", fileID, fileObj.DataID)
					updateFileObj := &core.ObjectInfo{
						ID:     fileObj.ID,
						PID:    fileObj.PID,
						Type:   fileObj.Type,
						Name:   fileObj.Name,
						DataID: core.EmptyDataID,
						Size:   0,
						MTime:  core.Now(),
					}
					_, putErr := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{updateFileObj})
					if putErr == nil {
						fileObj = updateFileObj
						DebugLog("[VFS Rename] Successfully set EmptyDataID for empty file: fileID=%d", fileID)
					} else {
						DebugLog("[VFS Rename] WARNING: Failed to set EmptyDataID for empty file: fileID=%d, error=%v", fileID, putErr)
					}
				}
				fileObjCache.Put(cacheKey, fileObj)
				DebugLog("[VFS Rename] Strong consistency: re-fetched from database after temporary flush: fileID=%d, dataID=%d, size=%d", fileID, fileObj.DataID, fileObj.Size)
				// Update directory listing cache
				if fileObj.PID > 0 {
					dirNode := &OrcasNode{
						fs:    n.fs,
						objID: fileObj.PID,
					}
					dirNode.invalidateDirListCache(fileObj.PID)
					DebugLog("[VFS Rename] Updated directory listing cache after temporary flush: fileID=%d, dirID=%d, name=%s", fileID, fileObj.PID, fileObj.Name)
				}
			}
		} else {
			DebugLog("[VFS Rename] WARNING: Unable to create temporary RandomAccessor for .tmp file flush: fileID=%d, error=%v", fileID, err)
		}
		return
	}

	// RandomAccessor exists but no TempFileWriter
	// Flush RandomAccessor normally (if not already flushed above)
	// Note: We already flushed above if there were pending writes, but we still need to ensure
	// RandomAccessor is fully flushed
	// Note: raToFlush is guaranteed to be non-nil here because if it was nil, we would have returned above
	// Also ensure RandomAccessor is fully flushed (in case there's any remaining data)
	writeIndex := atomic.LoadInt64(&raToFlush.buffer.writeIndex)
	totalSize := atomic.LoadInt64(&raToFlush.buffer.totalSize)
	if writeIndex > 0 || totalSize > 0 {
		DebugLog("[VFS Rename] RandomAccessor still has pending writes after initial flush, flushing again: fileID=%d, writeIndex=%d, totalSize=%d", fileID, writeIndex, totalSize)
		if _, err := raToFlush.ForceFlush(); err != nil {
			DebugLog("[VFS Rename] ERROR: Failed to force flush .tmp file: fileID=%d, error=%v", fileID, err)
		} else {
			DebugLog("[VFS Rename] Successfully forced flush .tmp file: fileID=%d", fileID)
		}
	} else {
		// No pending writes, but still try to flush to ensure any cached data is written
		DebugLog("[VFS Rename] RandomAccessor has no pending writes, but flushing to ensure data is written: fileID=%d", fileID)
		if _, err := raToFlush.ForceFlush(); err != nil {
			DebugLog("[VFS Rename] WARNING: Failed to force flush .tmp file (may be empty): fileID=%d, error=%v", fileID, err)
		} else {
			DebugLog("[VFS Rename] Successfully forced flush .tmp file: fileID=%d", fileID)
		}
	}

	// Get updated object from cache first (ForceFlush already updated it)
	// Only read from database if cache is empty (to avoid WAL stale read)
	cacheKey = fileID
	fileObj = nil // Reset fileObj to get updated version
	if cached, ok := fileObjCache.Get(cacheKey); ok {
		if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
			fileObj = obj
			DebugLog("[VFS Rename] Got file object from cache after flush: fileID=%d, size=%d, dataID=%d", fileID, fileObj.Size, fileObj.DataID)
		}
	}
	if fileObj == nil {
		DebugLog("[VFS Rename] WARNING: File object not in cache after flush, reading from database: fileID=%d", fileID)
		objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
		if err == nil && len(objs) > 0 {
			fileObj = objs[0]
		}
	}
	if fileObj != nil {
		// For empty files, ensure EmptyDataID is set
		if fileObj.Size == 0 && fileObj.DataID != core.EmptyDataID {
			DebugLog("[VFS Rename] Empty file detected after flush, setting EmptyDataID: fileID=%d, currentDataID=%d", fileID, fileObj.DataID)
			updateFileObj := &core.ObjectInfo{
				ID:     fileObj.ID,
				PID:    fileObj.PID,
				Type:   fileObj.Type,
				Name:   fileObj.Name,
				DataID: core.EmptyDataID,
				Size:   0,
				MTime:  core.Now(),
			}
			_, putErr := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{updateFileObj})
			if putErr == nil {
				fileObj = updateFileObj
				DebugLog("[VFS Rename] Successfully set EmptyDataID for empty file: fileID=%d", fileID)
			} else {
				DebugLog("[VFS Rename] WARNING: Failed to set EmptyDataID for empty file: fileID=%d, error=%v", fileID, putErr)
			}
		}
		fileObjCache.Put(cacheKey, fileObj)
		DebugLog("[VFS Rename] Got file object from cache after flush: fileID=%d, dataID=%d, size=%d, name=%s", fileID, fileObj.DataID, fileObj.Size, fileObj.Name)
		if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID || fileObj.Size == 0 {
			DebugLog("[VFS Rename] WARNING: File has no data after flush: fileID=%d, dataID=%d, size=%d", fileID, fileObj.DataID, fileObj.Size)
		}
		// Update directory listing cache
		if fileObj.PID > 0 {
			dirNode := &OrcasNode{
				fs:    n.fs,
				objID: fileObj.PID,
			}
			dirNode.invalidateDirListCache(fileObj.PID)
			DebugLog("[VFS Rename] Updated directory listing cache after flush: fileID=%d, dirID=%d, name=%s", fileID, fileObj.PID, fileObj.Name)
		}
	} else {
		DebugLog("[VFS Rename] WARNING: Failed to get file object after flush: fileID=%d", fileID)
	}

	// IMPORTANT: Do NOT unregister RandomAccessor immediately after force flush
	// There may be concurrent writes still in progress that need to use the same RandomAccessor
	// Unregistering too early can cause subsequent writes to create a new RandomAccessor
	// without TempFileWriter, leading to random write mode and new dataID creation
	// The RandomAccessor will be unregistered when the file is closed or when no longer needed
	// DebugLog("[VFS Rename] Keeping RandomAccessor registered after force flush to allow concurrent writes: fileID=%d", fileID)
}

// Release releases file handle (closes file)
// Optimization: use atomic operations, completely lock-free
// Release implements NodeReleaser interface
func (n *OrcasNode) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	DebugLog("[VFS Release] Entry: objID=%d, FileHandle=%v", n.objID, f)
	// Forward to FileHandle if it implements FileReleaser
	if f != nil {
		if fileReleaser, ok := f.(fs.FileReleaser); ok {
			errno := fileReleaser.Release(ctx)
			DebugLog("[VFS Release] Forwarded to FileHandle: objID=%d, errno=%d", n.objID, errno)
			return errno
		}
	}
	// Otherwise use our own implementation
	return n.releaseImpl(ctx)
}

// releaseImpl is the actual release implementation
func (n *OrcasNode) releaseImpl(ctx context.Context) syscall.Errno {
	DebugLog("[VFS releaseImpl] Entry: objID=%d", n.objID)
	if !n.isRoot {
		if errno := n.fs.checkKey(); errno != 0 {
			return errno
		}
	}

	// Atomically get and swap with released marker
	// atomic.Value cannot store nil, so we use releasedMarker instead
	val := n.ra.Load()
	if val == nil {
		// Already released or never initialized
		return 0
	}

	ra, ok := val.(*RandomAccessor)
	if !ok || ra == nil {
		return 0
	}

	// Check if already released (compare with marker)
	if ra == releasedMarker {
		return 0
	}

	// Try to atomically swap with released marker
	// If swap fails, another goroutine already released it
	if !n.ra.CompareAndSwap(ra, releasedMarker) {
		// Another goroutine already released it, just return
		return 0
	}

	// CRITICAL: For .tmp files, do NOT flush on Release
	// Flush should only occur in these scenarios:
	// 1. chunk写满（range只有一个，而且是从0-10MB的范围写满）
	// 2. tmp的后缀被重命名掉
	// 3. 写入以后超时了，没有任何操作，也没有去除tmp后缀
	// Release should NOT trigger flush for .tmp files
	obj, err := n.getObj()
	fileID := ra.fileID // Use ra.fileID as fallback if obj is nil
	if err == nil && obj != nil {
		fileID = obj.ID
		isTmpFile := isTempFile(obj)
		DebugLog("[VFS Release] Releasing file: fileID=%d, currentSize=%d, isTmpFile=%v", fileID, obj.Size, isTmpFile)

		// For .tmp files, skip flush and close
		// Flush will happen on timeout or rename
		if isTmpFile {
			DebugLog("[VFS Release] Skipping flush for .tmp file (will flush on timeout or rename): fileID=%d", fileID)
			// Just invalidate cache, don't flush or close
			n.invalidateObj()
			return 0
		}
	} else {
		DebugLog("[VFS Release] Releasing file: fileID=%d (failed to get obj: %v)", fileID, err)
	}

	// For non-.tmp files, execute ForceFlush to ensure immediate flush (no delayed flush)
	// Release should always flush immediately for non-.tmp files, not schedule delayed flush
	versionID, err := ra.ForceFlush()
	if err != nil {
		DebugLog("[VFS Release] ERROR: Failed to flush during release: fileID=%d, error=%v", fileID, err)
		// Record error but don't prevent close
	} else {
		DebugLog("[VFS Release] Flushed during release: fileID=%d, versionID=%d", fileID, versionID)
	}

	// IMPORTANT: Get updated object from RandomAccessor's cache BEFORE closing
	// ra.ForceFlush() has already updated the cache with the correct size
	// We must read from cache before Close() because Close() might clear some state
	var updatedObj *core.ObjectInfo
	if cachedObj := ra.fileObj.Load(); cachedObj != nil {
		if loadedObj, ok := cachedObj.(*core.ObjectInfo); ok && loadedObj != nil {
			updatedObj = loadedObj
			DebugLog("[VFS Release] Got updated object from RandomAccessor cache before close: fileID=%d, size=%d, dataID=%d, mtime=%d",
				updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
		}
	}

	// Close RandomAccessor (this will flush any remaining data)
	err = ra.Close()
	if err != nil {
		DebugLog("[VFS Release] ERROR: Failed to close RandomAccessor: fileID=%d, error=%v", fileID, err)
	}

	// If not in RandomAccessor cache, try global cache
	if updatedObj == nil {
		if cached, ok := fileObjCache.Get(fileID); ok {
			if loadedObj, ok := cached.(*core.ObjectInfo); ok && loadedObj != nil {
				updatedObj = loadedObj
				DebugLog("[VFS Release] Got updated object from global cache: fileID=%d, size=%d, dataID=%d, mtime=%d",
					updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
			}
		}
	}

	// Only if cache is empty, read from database (this should rarely happen)
	// Reading from database may return stale data due to SQLite WAL not being checkpointed yet
	if updatedObj == nil {
		DebugLog("[VFS Release] WARNING: Object not in cache after flush, reading from database: fileID=%d", fileID)
		updatedObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
		if err == nil && len(updatedObjs) > 0 {
			updatedObj = updatedObjs[0]
			DebugLog("[VFS Release] Got updated object from database: fileID=%d, size=%d, dataID=%d, mtime=%d",
				updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
		}
	}

	if updatedObj != nil {
		// Update global file object cache with latest metadata
		cacheKey := updatedObj.ID
		fileObjCache.Put(cacheKey, updatedObj)
		DebugLog("[VFS Release] Updated file object cache: fileID=%d, size=%d, dataID=%d, mtime=%d",
			updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)

		// Update local cache
		n.obj.Store(updatedObj)

		// IMPORTANT: Update directory listing cache after release to ensure file is visible
		// This is critical for macOS/SMB where files may disappear if not in directory cache
		// macOS/SMB relies heavily on directory listing cache, and if file is not in cache,
		// it may return error -43 (file not found) even though file exists in database
		if updatedObj.PID > 0 {
			dirNode := &OrcasNode{
				fs:    n.fs,
				objID: updatedObj.PID,
			}
			dirNode.invalidateDirListCache(updatedObj.PID)
			DebugLog("[VFS Release] Updated directory listing cache after release: fileID=%d, dirID=%d, name=%s, size=%d",
				updatedObj.ID, updatedObj.PID, updatedObj.Name, updatedObj.Size)
		}

		DebugLog("[VFS Release] Successfully released file: fileID=%d, finalSize=%d, dataID=%d, mtime=%d",
			updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
	} else {
		// If we can't get updated object, at least invalidate cache
		n.invalidateObj()
		DebugLog("[VFS Release] Successfully released file: fileID=%d (failed to get final obj)", fileID)
	}

	return 0
}

// queryFileByNameDirectly queries database directly for a file by name and parent ID
// This includes deleted files (PID < 0) which may still cause unique constraint conflicts
// Returns (fileID, fileObj) or (0, nil) if not found
func (n *OrcasNode) queryFileByNameDirectly(parentID int64, fileName string) (int64, *core.ObjectInfo) {
	// Query database directly using handler's metadata adapter
	// We need to access the metadata adapter to query directly
	// Since handler doesn't expose direct SQL query, we'll use GetReadDB from core
	// Bucket database: use dataPath/<bktID>/
	bktDirPath := filepath.Join(n.fs.DataPath, fmt.Sprint(n.fs.bktID))
	db, err := core.GetReadDB(bktDirPath, "")
	if err != nil {
		DebugLog("[VFS queryFileByNameDirectly] ERROR: Failed to get database connection: %v", err)
		return 0, nil
	}
	// Note: Don't close the connection, it's from the pool

	// Query for file with matching name and parent ID (including deleted files)
	// Unique constraint is on (pid, name), so we need to check:
	// 1. pid = parentID (non-deleted file)
	// 2. pid = -parentID (deleted file, if parentID > 0)
	// 3. pid = -bktID (deleted file from root, if parentID == bktID)
	var objs []core.ObjectInfo
	var whereConds []interface{}
	if parentID == n.fs.bktID {
		// Root directory: check pid = bktID and pid = -bktID
		whereConds = []interface{}{
			b.Eq("name", fileName),
			b.Eq("type", core.OBJ_TYPE_FILE),
			b.Or(b.Eq("pid", n.fs.bktID), b.Eq("pid", -n.fs.bktID)),
		}
	} else {
		// Non-root: check pid = parentID and pid = -parentID
		whereConds = []interface{}{
			b.Eq("name", fileName),
			b.Eq("type", core.OBJ_TYPE_FILE),
			b.Or(b.Eq("pid", parentID), b.Eq("pid", -parentID)),
		}
	}
	_, err = b.TableContext(n.fs.c, db, core.OBJ_TBL).Select(&objs, b.Where(whereConds...), b.Limit(1))
	if err != nil {
		DebugLog("[VFS queryFileByNameDirectly] ERROR: Failed to query database: %v", err)
		return 0, nil
	}

	if len(objs) > 0 {
		// Found file, return it (even if deleted)
		return objs[0].ID, &objs[0]
	}

	return 0, nil
}

// ============================================================================
// Missing FUSE interface implementations (with logging)
// ============================================================================

// Statfs implements NodeStatfser interface
func (n *OrcasNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	DebugLog("[VFS Statfs] Entry: objID=%d, bktID=%d", n.objID, n.fs.bktID)

	// Check cache first (cache will auto-refresh TTL on access)
	cacheKey := n.fs.bktID
	if cached, ok := statfsCache.Get(cacheKey); ok {
		if cachedStatfs, ok := cached.(*fuse.StatfsOut); ok && cachedStatfs != nil {
			// Cache hit: copy cached result and return (TTL is auto-refreshed by LRU cache)
			*out = *cachedStatfs
			DebugLog("[VFS Statfs] Cache hit: bktID=%d, returning cached statfs", n.fs.bktID)
			return 0
		}
	}

	// Cache miss: compute statfs result
	DebugLog("[VFS Statfs] Cache miss: bktID=%d, computing statfs", n.fs.bktID)

	// Get bucket information to determine quota and used space
	bucket, err := n.fs.h.GetBktInfo(n.fs.c, n.fs.bktID)
	if err != nil {
		DebugLog("[VFS Statfs] ERROR: Failed to get bucket info: bktID=%d, error=%v, using defaults", n.fs.bktID, err)
		// If we can't get bucket info, use default values
		*out = fuse.StatfsOut{}
		DebugLog("[VFS Statfs] Returning default (zeroed) statfs: objID=%d, bktID=%d, error=%v", n.objID, n.fs.bktID, err)
		return 0
	}

	// Calculate filesystem statistics based on bucket quota and usage
	// Block size: use 4KB (4096 bytes) as standard block size
	blockSize := uint64(4096)

	// Total blocks: use quota if set, otherwise use a large default value
	// If quota is negative, it means unlimited, use a very large value
	var totalBlocks uint64
	if bucket.Quota > 0 {
		// Quota is set, convert to blocks
		totalBlocks = uint64(bucket.Quota) / blockSize
		if totalBlocks == 0 {
			totalBlocks = 1 // At least 1 block
		}
	} else {
		// Unlimited quota, get actual disk size from DataPath
		// Use syscall.Statfs to get filesystem statistics
		var stat syscall.Statfs_t
		dataPath := n.fs.GetDataPath()
		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			// Path doesn't exist, use parent directory
			dataPath = filepath.Dir(dataPath)
			// If parent also doesn't exist, use the path as-is (Statfs may still work)
		}
		if err := syscall.Statfs(dataPath, &stat); err == nil {
			// Get total blocks from filesystem
			// stat.Blocks is total data blocks in filesystem
			// stat.Bsize is filesystem block size
			fsBlockSize := uint64(stat.Bsize)
			if fsBlockSize == 0 {
				fsBlockSize = blockSize // Fallback to 4KB if bsize is 0
			}
			// Calculate total size: stat.Blocks * stat.Bsize
			totalSize := uint64(stat.Blocks) * fsBlockSize
			// Convert to our block size (4KB)
			totalBlocks = totalSize / blockSize
			DebugLog("[VFS Statfs] Got disk size from DataPath: path=%s, fsBlocks=%d, fsBlockSize=%d, totalSize=%d, totalBlocks=%d",
				dataPath, stat.Blocks, fsBlockSize, totalSize, totalBlocks)
		} else {
			// Failed to get disk size, use a large default value (1TB in blocks)
			totalBlocks = (1 << 40) / blockSize // 1TB / 4KB = 268435456 blocks
			DebugLog("[VFS Statfs] WARNING: Failed to get disk size from DataPath: path=%s, error=%v, using default 1TB", dataPath, err)
		}
	}

	// Used blocks: convert RealUsed (actual physical usage) to blocks
	usedBlocks := uint64(bucket.RealUsed) / blockSize
	if usedBlocks > totalBlocks {
		usedBlocks = totalBlocks // Cap at total
	}

	// Free blocks: total - used
	freeBlocks := totalBlocks - usedBlocks

	// Available blocks: same as free blocks (no reserved space for now)
	availBlocks := freeBlocks

	// File count: use LogicalUsed as a proxy for file count (rough estimate)
	// This is not exact, but provides a reasonable estimate
	// Assume average file size of 1MB for estimation
	estimatedFiles := uint64(bucket.LogicalUsed) / (1 << 20) // 1MB
	if estimatedFiles == 0 {
		estimatedFiles = 1 // At least 1 file
	}
	freeFiles := estimatedFiles // Assume we can create as many files as we have

	// Fill StatfsOut structure
	out.Blocks = totalBlocks       // Total data blocks in filesystem
	out.Bfree = freeBlocks         // Free blocks in filesystem
	out.Bavail = availBlocks       // Free blocks available to unprivileged user
	out.Files = estimatedFiles     // Total file nodes in filesystem
	out.Ffree = freeFiles          // Free file nodes in filesystem
	out.Bsize = uint32(blockSize)  // Block size
	out.Frsize = uint32(blockSize) // Fragment size (same as block size)

	DebugLog("[VFS Statfs] Bucket stats: bktID=%d, quota=%d, used=%d, realUsed=%d, totalBlocks=%d, freeBlocks=%d, availBlocks=%d",
		n.fs.bktID, bucket.Quota, bucket.Used, bucket.RealUsed, totalBlocks, freeBlocks, availBlocks)

	// Store result in cache (create a copy to avoid issues with pointer reuse)
	cachedResult := &fuse.StatfsOut{
		Blocks: out.Blocks,
		Bfree:  out.Bfree,
		Bavail: out.Bavail,
		Files:  out.Files,
		Ffree:  out.Ffree,
		Bsize:  out.Bsize,
		Frsize: out.Frsize,
	}
	statfsCache.Put(cacheKey, cachedResult)
	DebugLog("[VFS Statfs] Cached result: bktID=%d", n.fs.bktID)

	return 0
}

// Access implements NodeAccesser interface
func (n *OrcasNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	DebugLog("[VFS Access] Entry: objID=%d, mask=0x%x", n.objID, mask)

	// Check if requireKey is set and key is missing
	// Root node is exempt from key check (can be accessed without key)
	if !n.isRoot {
		if errno := n.fs.checkKey(); errno != 0 {
			DebugLog("[VFS Access] ERROR: checkKey failed (requireKey set but no key): objID=%d, mask=0x%x, errno=%d", n.objID, mask, errno)
			return errno // Returns EPERM if requireKey is set but EndecKey is empty
		}
	}

	// For precise permission checking, could implement based on Getattr result
	// For now, allow access if key check passes
	DebugLog("[VFS Access] Allowing access: objID=%d, mask=0x%x", n.objID, mask)
	return 0
}

// OnAdd implements NodeOnAdder interface
func (n *OrcasNode) OnAdd(ctx context.Context) {
}

// Getxattr implements NodeGetxattrer interface
func (n *OrcasNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	if errno := n.fs.checkKey(); errno != 0 {
		return 0, errno
	}

	// For system-specific attributes (like security.selinux, system.*, trusted.*),
	// return ENOTSUP directly to avoid "No data available" errors in ls
	// These attributes are system-specific and not supported by our filesystem
	// Note: user.* attributes are user-defined and should be supported, so we allow them
	if strings.HasPrefix(attr, "security.") || strings.HasPrefix(attr, "system.") ||
		strings.HasPrefix(attr, "trusted.") {
		// These are system-specific attributes that we don't support
		return 0, syscall.ENOTSUP
	}

	// Get from database
	if lh, ok := n.fs.h.(*core.LocalHandler); ok {
		ma := lh.MetadataAdapter()
		if ma != nil {
			// First, verify that the object exists
			obj, err := n.getObj()
			if err != nil {
				// Object doesn't exist, return ENOENT
				return 0, syscall.ENOENT
			}
			if obj == nil {
				// Object is nil, return ENOENT
				return 0, syscall.ENOENT
			}

			value, err := ma.GetAttr(n.fs.c, n.fs.bktID, n.objID, attr)
			if err != nil {
				// Check if this is a "not found" error (attribute doesn't exist)
				if strings.Contains(err.Error(), "attribute not found") {
					// Don't cache sentinel values - just return ENODATA
					return 0, syscall.ENODATA
				}
				// For other errors (database errors, etc.), return EIO
				return 0, syscall.EIO
			}
			if len(value) > len(dest) {
				return uint32(len(value)), syscall.ERANGE
			}
			copy(dest, value)

			return uint32(len(value)), 0
		}
	} else {
	}
	// If MetadataAdapter is not available, return ENOTSUP (operation not supported)
	// This indicates that extended attributes are not supported at all,
	// rather than that a specific attribute doesn't exist (ENODATA)
	// This prevents "No data available" errors in ls and other tools
	return 0, syscall.ENOTSUP
}

// Setxattr implements NodeSetxattrer interface
func (n *OrcasNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	if errno := n.fs.checkKey(); errno != 0 {
		return errno
	}

	// Get MetadataAdapter from handler
	if lh, ok := n.fs.h.(*core.LocalHandler); ok {
		ma := lh.MetadataAdapter()
		if ma != nil {
			err := ma.SetAttr(n.fs.c, n.fs.bktID, n.objID, attr, data)
			if err != nil {
				return syscall.EIO
			}

			return 0
		}
	} else {
	}
	// If MetadataAdapter is not available, return ENOTSUP (operation not supported)
	// This tells macOS that extended attributes are not supported, which is better than ENODATA
	// ENODATA would suggest the attribute doesn't exist, but ENOTSUP indicates the feature isn't available
	return syscall.ENOTSUP
}

// Removexattr implements NodeRemovexattrer interface
func (n *OrcasNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	if errno := n.fs.checkKey(); errno != 0 {
		return errno
	}

	// Get MetadataAdapter from handler
	if lh, ok := n.fs.h.(*core.LocalHandler); ok {
		ma := lh.MetadataAdapter()
		if ma != nil {
			// Check if attribute exists before removing
			_, err := ma.GetAttr(n.fs.c, n.fs.bktID, n.objID, attr)
			if err != nil {
				// Check if this is a "not found" error (attribute doesn't exist)
				if strings.Contains(err.Error(), "attribute not found") || strings.Contains(err.Error(), "not found") {
					return syscall.ENODATA
				}
				// For other errors (database errors, etc.), return EIO
				return syscall.EIO
			}

			// Attribute exists, remove it
			err = ma.RemoveAttr(n.fs.c, n.fs.bktID, n.objID, attr)
			if err != nil {
				return syscall.EIO
			}

			return 0
		}
	} else {
	}
	// If MetadataAdapter is not available, return ENOTSUP (operation not supported)
	return syscall.ENOTSUP
}

// Listxattr implements NodeListxattrer interface
func (n *OrcasNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	if errno := n.fs.checkKey(); errno != 0 {
		return 0, errno
	}

	var keys []string

	// Get from database
	if lh, ok := n.fs.h.(*core.LocalHandler); ok {
		ma := lh.MetadataAdapter()
		if ma != nil {
			var err error
			keys, err = ma.ListAttrs(n.fs.c, n.fs.bktID, n.objID)
			if err != nil {
				return 0, syscall.EIO
			}
		} else {
		}
	} else {
	}

	// If no attributes found, return 0 (success with empty list)
	if len(keys) == 0 {
		return 0, 0
	}

	// Format keys as null-terminated strings: "key1\0key2\0key3\0"
	totalLen := 0
	for _, key := range keys {
		totalLen += len(key) + 1 // +1 for null terminator
	}
	if totalLen > len(dest) {
		return uint32(totalLen), syscall.ERANGE
	}
	pos := 0
	for _, key := range keys {
		copy(dest[pos:], key)
		pos += len(key)
		dest[pos] = 0 // null terminator
		pos++
	}
	return uint32(totalLen), 0
}

// Readlink implements NodeReadlinker interface
func (n *OrcasNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	DebugLog("[VFS Readlink] Entry: objID=%d", n.objID)
	// Default implementation: return EINVAL (not a symlink)
	DebugLog("[VFS Readlink] ERROR: Not a symlink: objID=%d", n.objID)
	return nil, syscall.EINVAL
}

// Note: NodeReader and NodeWriter are automatically forwarded to FileHandle by go-fuse
// Since OrcasNode implements FileReader and FileWriter, NodeReader/NodeWriter will work automatically

// Allocate implements NodeAllocater interface
func (n *OrcasNode) Allocate(ctx context.Context, f fs.FileHandle, off uint64, size uint64, mode uint32) syscall.Errno {
	DebugLog("[VFS Allocate] Entry: objID=%d, FileHandle=%v, offset=%d, size=%d, mode=0x%x", n.objID, f, off, size, mode)
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS Allocate] ERROR: Not supported: objID=%d", n.objID)
	return syscall.ENOTSUP
}

// CopyFileRange implements NodeCopyFileRanger interface
func (n *OrcasNode) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64, len uint64, flags uint64) (uint32, syscall.Errno) {
	DebugLog("[VFS CopyFileRange] Entry: objID=%d, fhIn=%v, offIn=%d, outInode=%v, fhOut=%v, offOut=%d, len=%d, flags=0x%x",
		n.objID, fhIn, offIn, out, fhOut, offOut, len, flags)
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS CopyFileRange] ERROR: Not supported: objID=%d", n.objID)
	return 0, syscall.ENOTSUP
}

// Statx implements NodeStatxer interface
func (n *OrcasNode) Statx(ctx context.Context, f fs.FileHandle, flags uint32, mask uint32, out *fuse.StatxOut) syscall.Errno {
	DebugLog("[VFS Statx] Entry: objID=%d, FileHandle=%v, flags=0x%x, mask=0x%x", n.objID, f, flags, mask)
	// Default implementation: return ENOTSUP (not supported)
	// Statx is a Linux-specific extension
	DebugLog("[VFS Statx] ERROR: Not supported: objID=%d", n.objID)
	return syscall.ENOTSUP
}

// Lseek implements NodeLseeker interface
func (n *OrcasNode) Lseek(ctx context.Context, f fs.FileHandle, off uint64, whence uint32) (uint64, syscall.Errno) {
	DebugLog("[VFS Lseek] Entry: objID=%d, FileHandle=%v, offset=%d, whence=%d", n.objID, f, off, whence)
	// Default implementation: return ENOTSUP (not supported)
	// Lseek is used for SEEK_DATA and SEEK_HOLE
	DebugLog("[VFS Lseek] ERROR: Not supported: objID=%d", n.objID)
	return 0, syscall.ENOTSUP
}

// Getlk implements NodeGetlker interface
func (n *OrcasNode) Getlk(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	DebugLog("[VFS Getlk] Entry: objID=%d, FileHandle=%v, owner=%d, flags=0x%x", n.objID, f, owner, flags)
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS Getlk] ERROR: Not supported: objID=%d", n.objID)
	return syscall.ENOTSUP
}

// Setlk implements NodeSetlker interface
func (n *OrcasNode) Setlk(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	DebugLog("[VFS Setlk] Entry: objID=%d, FileHandle=%v, owner=%d, flags=0x%x", n.objID, f, owner, flags)
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS Setlk] ERROR: Not supported: objID=%d", n.objID)
	return syscall.ENOTSUP
}

// Setlkw implements NodeSetlkwer interface
func (n *OrcasNode) Setlkw(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	DebugLog("[VFS Setlkw] Entry: objID=%d, FileHandle=%v, owner=%d, flags=0x%x", n.objID, f, owner, flags)
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS Setlkw] ERROR: Not supported: objID=%d", n.objID)
	return syscall.ENOTSUP
}

// Ioctl implements NodeIoctler interface
func (n *OrcasNode) Ioctl(ctx context.Context, f fs.FileHandle, cmd uint32, arg uint64, input []byte, output []byte) (int32, syscall.Errno) {
	DebugLog("[VFS Ioctl] Entry: objID=%d, FileHandle=%v, cmd=0x%x, arg=%d, inputLen=%d, outputLen=%d",
		n.objID, f, cmd, arg, len(input), len(output))
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS Ioctl] ERROR: Not supported: objID=%d", n.objID)
	return 0, syscall.ENOTSUP
}

// OnForget implements NodeOnForgetter interface
func (n *OrcasNode) OnForget() {
}

// WrapChild implements NodeWrapChilder interface
func (n *OrcasNode) WrapChild(ctx context.Context, ops fs.InodeEmbedder) fs.InodeEmbedder {
	return ops
}

// Opendir implements NodeOpendirer interface
func (n *OrcasNode) Opendir(ctx context.Context) syscall.Errno {
	DebugLog("[VFS Opendir] Entry: objID=%d", n.objID)
	// Default implementation: return success
	// This is just for sanity/permission checks
	DebugLog("[VFS Opendir] Allowing directory open: objID=%d", n.objID)
	return 0
}

// Mknod implements NodeMknoder interface
func (n *OrcasNode) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	DebugLog("[VFS Mknod] Entry: name=%s, parentID=%d, mode=0%o, dev=%d", name, n.objID, mode, dev)
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS Mknod] ERROR: Not supported: name=%s, parentID=%d", name, n.objID)
	return nil, syscall.ENOTSUP
}

// Link implements NodeLinker interface
func (n *OrcasNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	DebugLog("[VFS Link] Entry: name=%s, parentID=%d, target=%v", name, n.objID, target)
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS Link] ERROR: Not supported: name=%s, parentID=%d", name, n.objID)
	return nil, syscall.ENOTSUP
}

// Symlink implements NodeSymlinker interface
func (n *OrcasNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	DebugLog("[VFS Symlink] Entry: target=%s, name=%s, parentID=%d", target, name, n.objID)
	// Default implementation: return ENOTSUP (not supported)
	DebugLog("[VFS Symlink] ERROR: Not supported: target=%s, name=%s, parentID=%d", target, name, n.objID)
	return nil, syscall.ENOTSUP
}
