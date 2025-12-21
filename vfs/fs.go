//go:build !windows
// +build !windows

package vfs

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/orca-zhang/ecache2"
	"github.com/orcastor/orcas/core"
)

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
	if ofs.root == nil {
		ofs.root = &OrcasNode{
			fs:     ofs,
			objID:  core.ROOT_OID,
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
	_ fs.InodeEmbedder = (*OrcasNode)(nil)
	_                  = fs.NodeLookuper(&OrcasNode{})
	_                  = fs.NodeReaddirer(&OrcasNode{})
	_                  = fs.NodeCreater(&OrcasNode{})
	_                  = fs.NodeMkdirer(&OrcasNode{})
	_                  = fs.NodeUnlinker(&OrcasNode{})
	_                  = fs.NodeRmdirer(&OrcasNode{})
	_                  = fs.NodeRenamer(&OrcasNode{})
	_                  = fs.NodeGetattrer(&OrcasNode{})
	_                  = fs.NodeOpener(&OrcasNode{})
	_                  = fs.FileReader(&OrcasNode{})
	_                  = fs.FileWriter(&OrcasNode{})
	_                  = fs.FileReleaser(&OrcasNode{})
)

var streamingReaderCache = ecache2.NewLRUCache[int64](4, 256, 15*time.Second)

type cachedReader struct {
	reader     io.Reader
	nextOffset int64
	dataID     int64
}

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

	// If root node, return virtual object
	if n.isRoot {
		return &core.ObjectInfo{
			ID:   core.ROOT_OID,
			PID:  0,
			Type: core.OBJ_TYPE_DIR,
			Name: "/",
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

// invalidateDirListCache invalidates directory listing cache
// Uses delayed refresh: marks cache as stale instead of immediately deleting
func (n *OrcasNode) invalidateDirListCache(dirID int64) {
	cacheKey := dirID
	// Mark Readdir cache as stale (delayed refresh)
	readdirCacheStale.Store(dirID, true)
	// Also invalidate dirListCache for consistency
	dirListCache.Del(cacheKey)
	// DebugLog("[VFS invalidateDirListCache] Marked directory cache as stale (delayed refresh): dirID=%d", dirID)
}

// appendChildToDirCache appends a newly created child object into the
// cached directory listing instead of invalidating the entire cache.
// If cache doesn't exist, creates a new cache entry with the child.
func (n *OrcasNode) appendChildToDirCache(dirID int64, child *core.ObjectInfo) {
	if child == nil {
		return
	}
	cacheKey := dirID
	if cached, ok := dirListCache.Get(cacheKey); ok {
		if children, ok := cached.([]*core.ObjectInfo); ok && children != nil {
			// Check if child already exists
			for _, existing := range children {
				if existing != nil && existing.ID == child.ID {
					return
				}
			}
			// Append child to existing cache
			newChildren := make([]*core.ObjectInfo, len(children)+1)
			copy(newChildren, children)
			newChildren[len(children)] = child
			dirListCache.Put(cacheKey, newChildren)
			DebugLog("[VFS appendChildToDirCache] Appended child to existing cache: dirID=%d, childID=%d", dirID, child.ID)
			return
		}
	}
	// Cache doesn't exist, create new cache entry with the child
	newChildren := []*core.ObjectInfo{child}
	dirListCache.Put(cacheKey, newChildren)
	DebugLog("[VFS appendChildToDirCache] Created new cache entry with child: dirID=%d, childID=%d", dirID, child.ID)
}

// removeChildFromDirCache removes a child object from the cached directory listing
// instead of invalidating the entire cache. This preserves other cached children.
func (n *OrcasNode) removeChildFromDirCache(dirID int64, childID int64) {
	cacheKey := dirID
	if cached, ok := dirListCache.Get(cacheKey); ok {
		if children, ok := cached.([]*core.ObjectInfo); ok && children != nil {
			updatedChildren := make([]*core.ObjectInfo, 0, len(children))
			found := false
			for _, child := range children {
				if child != nil && child.ID == childID {
					found = true
					continue // Skip this child
				}
				updatedChildren = append(updatedChildren, child)
			}
			if found {
				dirListCache.Put(cacheKey, updatedChildren)
				DebugLog("[VFS removeChildFromDirCache] Removed child from cache: dirID=%d, childID=%d", dirID, childID)
			}
		}
	}
}

// updateChildInDirCache updates a child object in the cached directory listing
// instead of invalidating the entire cache. This preserves other cached children.
// If the child is not found in cache, it will append it instead (for newly created files).
// Also marks Readdir cache as stale for delayed refresh.
func (n *OrcasNode) updateChildInDirCache(dirID int64, updatedChild *core.ObjectInfo) {
	if updatedChild == nil {
		return
	}
	cacheKey := dirID
	if cached, ok := dirListCache.Get(cacheKey); ok {
		if children, ok := cached.([]*core.ObjectInfo); ok && children != nil {
			updated := false
			for i, child := range children {
				if child != nil && child.ID == updatedChild.ID {
					// Update the child in place
					children[i] = updatedChild
					updated = true
					break
				}
			}
			if updated {
				dirListCache.Put(cacheKey, children)
				// Mark Readdir cache as stale (delayed refresh)
				readdirCacheStale.Store(dirID, true)
				// DebugLog("[VFS updateChildInDirCache] Updated child in cache: dirID=%d, childID=%d, newName=%s", dirID, updatedChild.ID, updatedChild.Name)
				return
			}
		}
	}
	// If child not found in cache (e.g., newly created file), append it instead
	// DebugLog("[VFS updateChildInDirCache] Child not found in cache, appending instead: dirID=%d, childID=%d, newName=%s", dirID, updatedChild.ID, updatedChild.Name)
	n.appendChildToDirCache(dirID, updatedChild)
}

// Getattr gets file/directory attributes
func (n *OrcasNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Getattr] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return syscall.ENOENT
	}

	out.Mode = getMode(obj.Type)
	out.Size = uint64(obj.Size)
	out.Mtime = uint64(obj.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime
	out.Nlink = 1
	return 0
}

// getMode returns file mode based on object type
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
	// DebugLog("[VFS Lookup] Looking up child node: name=%s, parentID=%d", name, n.objID)
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return nil, syscall.ENOTDIR
	}

	// Get directory listing with cache and singleflight
	children, errno := n.getDirListWithCache(obj.ID)
	if errno != 0 {
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

	// Verify that the matched object type is correct by querying database
	// This prevents issues where cache might have incorrect type information
	cacheKey := matchedChild.ID
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
					DebugLog("[VFS Lookup] Fetched from database and updated cache: objID=%d, type=%d", matchedChild.ID, matchedChild.Type)
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
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
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
				if childrenCached, ok := dirListCache.Get(cacheKey); ok {
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
		return nil, errno
	}

	// Build directory stream
	entries := make([]fuse.DirEntry, 0, len(children)+2)
	// Add . and ..
	entries = append(entries, fuse.DirEntry{
		Name: ".",
		Mode: syscall.S_IFDIR,
		Ino:  uint64(obj.ID),
	})
	entries = append(entries, fuse.DirEntry{
		Name: "..",
		Mode: syscall.S_IFDIR,
		Ino:  uint64(obj.PID),
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

// getDirListWithCache gets directory listing with cache and singleflight
// Uses singleflight to prevent duplicate concurrent requests for the same directory
func (n *OrcasNode) getDirListWithCache(dirID int64) ([]*core.ObjectInfo, syscall.Errno) {
	// Get pending objects first (these are always fresh and should be included)
	pendingChildren := n.getPendingObjectsForDir(dirID)

	// Check cache first
	cacheKey := dirID
	if cached, ok := dirListCache.Get(cacheKey); ok {
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
			// Check by ID first - if same ID exists in database, skip (database is authoritative)
			if existing, exists := childrenMapByID[pending.ID]; exists {
				// Same ID exists - database entry is authoritative, skip pending
				DebugLog("[VFS getDirListWithCache] Skipping pending object (same ID in database): dirID=%d, fileID=%d, name=%s, dbDataID=%d", dirID, pending.ID, pending.Name, existing.DataID)
				continue
			}

			// Check by name - if same name exists in database (with DataID), skip pending
			if existing, exists := childrenMapByName[pending.Name]; exists {
				if existing.DataID > 0 {
					// Database entry has DataID (already flushed), skip pending entry
					DebugLog("[VFS getDirListWithCache] Skipping pending object (same name in database with DataID): dirID=%d, fileID=%d, name=%s, existingFileID=%d, existingDataID=%d", dirID, pending.ID, pending.Name, existing.ID, existing.DataID)
					continue
				}
				// Both are pending (no DataID), but existing is from database query result
				// This shouldn't happen normally, but if it does, prefer the one from database
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

		// Cache the result (including pending objects)
		dirListCache.Put(cacheKey, children)
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

		if _, ok := dirListCache.Get(cacheKey); ok {
			// Already cached, skip
			continue
		}

		// Preload directory listing asynchronously
		// Use singleflight to prevent duplicate requests
		key := fmt.Sprintf("%d", child.ID)
		go func(dirID int64, cacheKey int64, key string) {
			_, err, _ := dirListSingleFlight.Do(key, func() (interface{}, error) {
				// Double-check cache
				if _, ok := dirListCache.Get(cacheKey); ok {
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
				dirListCache.Put(cacheKey, children)
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
		}(child.ID, cacheKey, key)
	}
}

// Create creates a file
func (n *OrcasNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		return nil, nil, 0, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Create] ERROR: Failed to get parent directory object: %v", err)
		return nil, nil, 0, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Create] ERROR: Parent is not a directory (type=%d)", obj.Type)
		return nil, nil, 0, syscall.ENOTDIR
	}

	DebugLog("[VFS Create] Creating file: name=%s, parentID=%d, flags=0x%x, mode=0%o", name, obj.ID, flags, mode)

	// Check if file already exists
	// First check cache for directory listing to see if there's a directory with the same name
	parentCacheKey := obj.ID
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
				n.removeChildFromDirCache(obj.ID, existingFileID)

				// File was deleted, continue to create new file below
				existingFileID = 0
				existingFileObj = nil
			} else {
				// Existing file is not a .tmp file, create version before truncating
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
			out.Mode = syscall.S_IFREG | 0o644
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
	// parentCacheKey is already declared above
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
				n.removeChildFromDirCache(obj.ID, existingFileID)

				// File was deleted, continue to create new file below
				existingFileID = 0
				existingFileObj = nil
			} else {
				// Existing file is not a .tmp file, create version before truncating
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
			out.Mode = syscall.S_IFREG | 0o644
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
	n.appendChildToDirCache(obj.ID, fileObj)

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
	out.Mode = syscall.S_IFREG | 0o644
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
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		return nil, 0, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Open] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return nil, 0, syscall.ENOENT
	}

	DebugLog("[VFS Open] Object info: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d",
		obj.ID, obj.Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, obj.Name, obj.PID)

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

	// RandomAccessor is now created lazily on the first write.
	// Even when the file is opened RDWR, we defer creation so that pure reads
	// still go directly to the underlying data without touching RA state.

	// Increment reference count when file is opened
	DebugLog("[VFS Open] Opened file: fileID=%d", obj.ID)

	// Return the node itself as FileHandle
	// This allows Write/Read operations to work on the file
	return n, 0, 0
}

// Mkdir creates a directory
func (n *OrcasNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		return nil, errno
	}
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
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

	ids, err := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{dirObj})
	if err != nil || len(ids) == 0 || ids[0] == 0 {
		return nil, syscall.EIO
	}

	dirObj.ID = ids[0]

	// Cache new directory object for GetAttr optimization
	cacheKey := dirObj.ID
	fileObjCache.Put(cacheKey, dirObj)
	n.appendChildToDirCache(obj.ID, dirObj)

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
	out.Mode = syscall.S_IFDIR | 0o755
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
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		return errno
	}
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
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

	// Step 0: Remove from RandomAccessor registry if present
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
		return syscall.EIO
	}

	// Step 2: Update cache immediately
	// CRITICAL: Clear all caches for the deleted file to prevent data corruption
	// If a file with the same name is recreated, it should get a new DataID and DataInfo
	// Clearing caches ensures we don't reuse old DataID or DataInfo from deleted file
	n.removeChildFromDirCache(obj.ID, targetID)

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
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		return errno
	}
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return syscall.ENOTDIR
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
			return syscall.ENOENT
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
			return syscall.ENOTEMPTY
		}
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

	// Step 2: Update cache immediately
	// Only update directory listing cache, don't delete fileObjCache
	// This preserves the directory object cache for potential future use
	n.removeChildFromDirCache(obj.ID, targetID)

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
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		return errno
	}
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
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
	// Note: InodeEmbedder interface needs to be converted to specific node type
	// Here assume newParent is OrcasNode type
	var newParentNode *OrcasNode
	if node, ok := newParent.(*OrcasNode); ok {
		newParentNode = node
	} else {
		return syscall.EIO
	}

	newParentObj, err := newParentNode.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if newParentObj.Type != core.OBJ_TYPE_DIR {
		return syscall.ENOTDIR
	}

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
			// Use delayed double delete to prevent dirty read
			if sourceObj.PID > 0 {
				// First delete: immediate removal
				n.removeChildFromDirCache(sourceObj.PID, sourceObj.ID)
				// Delayed second delete: remove again after delay to prevent dirty read
				go func(dirID int64, fileID int64) {
					time.Sleep(200 * time.Millisecond)
					if n.fs != nil {
						tempNode := &OrcasNode{fs: n.fs, objID: dirID}
						tempNode.removeChildFromDirCache(dirID, fileID)
						DebugLog("[VFS Rename] Delayed second delete: Removed empty .tmp file from parent directory cache again: tmpFileID=%d, parentDirID=%d", fileID, dirID)
					}
				}(sourceObj.PID, sourceObj.ID)
			}
			// Also remove from obj.ID if different
			if obj.ID != sourceObj.PID && obj.ID > 0 {
				// First delete: immediate removal
				n.removeChildFromDirCache(obj.ID, sourceObj.ID)
				// Delayed second delete: remove again after delay to prevent dirty read
				go func(dirID int64, fileID int64) {
					time.Sleep(200 * time.Millisecond)
					if n.fs != nil {
						tempNode := &OrcasNode{fs: n.fs, objID: dirID}
						tempNode.removeChildFromDirCache(dirID, fileID)
						DebugLog("[VFS Rename] Delayed second delete: Removed empty .tmp file from obj.ID cache again: tmpFileID=%d, objDirID=%d", fileID, dirID)
					}
				}(obj.ID, sourceObj.ID)
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

		// Update directory listing cache for both directories
		// Use delayed double delete to prevent dirty read:
		// 1. First delete: Remove immediately from cache
		// 2. Delayed delete: Remove again after a delay to catch any cache repopulation from stale database reads
		// This prevents race condition where another thread reads from database (before delete completes)
		// and repopulates the cache with the .tmp file
		if sourceObj.PID > 0 {
			// First delete: immediate removal
			n.removeChildFromDirCache(sourceObj.PID, sourceObj.ID)
			DebugLog("[VFS Rename] First delete: Removed source .tmp file from parent directory cache: tmpFileID=%d, parentDirID=%d", sourceObj.ID, sourceObj.PID)

			// Delayed second delete: remove again after delay to prevent dirty read
			go func(dirID int64, fileID int64) {
				time.Sleep(200 * time.Millisecond) // Wait for potential cache repopulation
				// Get fresh node reference (n may be invalid after delay)
				if n.fs != nil {
					// Create a temporary node to access cache removal method
					tempNode := &OrcasNode{fs: n.fs, objID: dirID}
					tempNode.removeChildFromDirCache(dirID, fileID)
					DebugLog("[VFS Rename] Delayed second delete: Removed source .tmp file from parent directory cache again: tmpFileID=%d, parentDirID=%d", fileID, dirID)
				}
			}(sourceObj.PID, sourceObj.ID)
		}
		// Also remove from obj.ID if different (in case obj is not the direct parent)
		if obj.ID != sourceObj.PID && obj.ID > 0 {
			// First delete: immediate removal
			n.removeChildFromDirCache(obj.ID, sourceObj.ID)
			DebugLog("[VFS Rename] First delete: Also removed source .tmp file from obj.ID cache: tmpFileID=%d, objDirID=%d", sourceObj.ID, obj.ID)

			// Delayed second delete: remove again after delay to prevent dirty read
			go func(dirID int64, fileID int64) {
				time.Sleep(200 * time.Millisecond) // Wait for potential cache repopulation
				// Get fresh node reference (n may be invalid after delay)
				if n.fs != nil {
					// Create a temporary node to access cache removal method
					tempNode := &OrcasNode{fs: n.fs, objID: dirID}
					tempNode.removeChildFromDirCache(dirID, fileID)
					DebugLog("[VFS Rename] Delayed second delete: Removed source .tmp file from obj.ID cache again: tmpFileID=%d, objDirID=%d", fileID, dirID)
				}
			}(obj.ID, sourceObj.ID)
		}
		// Update target file in new parent directory listing (if target exists)
		// Note: Target file is updated with new data, so we need to update it in cache
		targetObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{existingTargetID})
		if err == nil && len(targetObjs) > 0 {
			// Update target file in directory listing cache
			n.updateChildInDirCache(newParentObj.ID, targetObjs[0])
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

	// Update directory listing cache instead of invalidating
	// Remove source from old parent directory listing (only if moving to different parent)
	if obj.ID != newParentObj.ID {
		n.removeChildFromDirCache(obj.ID, sourceID)
	}

	// Add/update source in new parent directory listing
	// Re-fetch source object from database to get latest data (including updated name and PID)
	cacheKey := sourceID
	fileObjCache.Del(cacheKey)
	objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{sourceID})
	if err == nil && len(objs) > 0 {
		updatedSourceObj := objs[0]
		fileObjCache.Put(cacheKey, updatedSourceObj)
		// Check if source already exists in new parent's listing (for move within same directory with name change)
		// If moving to different parent, just add it
		if obj.ID == newParentObj.ID {
			// Same parent, just update the name in listing
			// updateChildInDirCache will append if not found (for newly created files)
			n.updateChildInDirCache(newParentObj.ID, updatedSourceObj)
			DebugLog("[VFS Rename] Updated child in same directory cache: fileID=%d, dirID=%d, newName=%s", sourceID, newParentObj.ID, newName)
		} else {
			// Different parent, remove from old and add to new
			// Note: appendChildToDirCache will check for duplicates
			n.appendChildToDirCache(newParentObj.ID, updatedSourceObj)
			DebugLog("[VFS Rename] Added child to new directory cache: fileID=%d, oldDirID=%d, newDirID=%d, newName=%s", sourceID, obj.ID, newParentObj.ID, newName)
		}
	} else {
		// Fallback: use cached sourceObj with updated name
		updatedSourceObj := sourceObj
		updatedSourceObj.Name = newName
		updatedSourceObj.PID = newParentObj.ID
		if obj.ID == newParentObj.ID {
			// updateChildInDirCache will append if not found (for newly created files)
			n.updateChildInDirCache(newParentObj.ID, updatedSourceObj)
		} else {
			n.appendChildToDirCache(newParentObj.ID, updatedSourceObj)
		}
		DebugLog("[VFS Rename] WARNING: Failed to re-fetch source object after rename, using cached data: fileID=%d, error=%v", sourceID, err)
	}

	// Invalidate both directories' cache (for GetAttr)
	n.invalidateObj()
	newParentNode.invalidateObj()

	return 0
}

// Read reads file content
func (n *OrcasNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		return nil, errno
	}

	// Force refresh object cache to get latest DataID (important after writes)
	// This ensures we get the latest DataID from database or fileObjCache
	n.invalidateObj()

	obj, err := n.getObj()
	if err != nil {
		// DebugLog("[VFS Read] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
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
func (n *OrcasNode) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
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

// Flush flushes file
// Optimization: use atomic operations, completely lock-free
func (n *OrcasNode) Flush(ctx context.Context) syscall.Errno {
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
	// After flush, invalidate object cache
	n.invalidateObj()

	// Get updated object to log final size
	obj, err = n.getObj()
	if err == nil && obj != nil {
		DebugLog("[VFS Flush] Successfully flushed file: fileID=%d, versionID=%d, finalSize=%d", obj.ID, versionID, obj.Size)
	}

	return 0
}

// Fsync syncs file
func (n *OrcasNode) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	// Flush RandomAccessor first
	if errno := n.Flush(ctx); errno != 0 {
		return errno
	}
	// Flush object cache
	n.invalidateObj()
	return 0
}

// Setattr sets file attributes (including truncate operation)
func (n *OrcasNode) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
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

	// Update modification time
	if in.Valid&fuse.FATTR_MTIME != 0 {
		obj.MTime = int64(in.Mtime)
	}

	// Update object information to database
	// Note: File size update has been completed in truncateFile through Flush
	// Here only need to update mtime (if set)
	if in.Valid&fuse.FATTR_MTIME != 0 && in.Valid&fuse.FATTR_SIZE == 0 {
		// If only updating mtime, need to update through Handler
		// Since RandomAccessor's Flush automatically updates file size, truncate operation's size update is already done
		// Here only need to handle separate mtime update
		// Simplified handling: mtime update can be automatically updated on next write or Flush
	}

	// Invalidate cache
	n.invalidateObj()

	// Fill output
	out.Mode = getMode(obj.Type)
	out.Size = uint64(obj.Size)
	out.Mtime = uint64(obj.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime

	return 0
}

// truncateFile truncates file to specified size
func (n *OrcasNode) truncateFile(newSize int64) syscall.Errno {
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
				ra = r
			}
		}
		if ra != nil {
			// Use RandomAccessor's Truncate method
			// This will reference previous data block but with new size, and create new version
			_, err := ra.Truncate(newSize)
			if err != nil {
				return syscall.EIO
			}
		} else {
			// No RandomAccessor, create one and use Truncate method
			ra, err := NewRandomAccessor(n.fs, obj.ID)
			if err != nil {
				return syscall.EIO
			}
			defer ra.Close()

			_, err = ra.Truncate(newSize)
			if err != nil {
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
				fileObj := objs[0]
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
					dirNode.appendChildToDirCache(fileObj.PID, fileObj)
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
			// Re-fetch to check if empty file needs EmptyDataID
			cacheKey := fileID
			fileObjCache.Del(cacheKey)
			objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
			if err == nil && len(objs) > 0 {
				fileObj := objs[0]
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

		// Strong consistency: re-fetch from database
		cacheKey := fileID
		fileObjCache.Del(cacheKey)

		// Fetch from database (should have DataID immediately after flush)
		objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
		var fileObj *core.ObjectInfo
		if err == nil && len(objs) > 0 {
			fileObj = objs[0]
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
					dirNode.appendChildToDirCache(fileObj.PID, fileObj)
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

	// Strong consistency: always re-fetch from database and update cache
	cacheKey = fileID
	fileObjCache.Del(cacheKey)
	objs, err = n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{fileID})
	if err == nil && len(objs) > 0 {
		fileObj := objs[0]
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
		DebugLog("[VFS Rename] Strong consistency: re-fetched from database after flush: fileID=%d, dataID=%d, size=%d, name=%s", fileID, fileObj.DataID, fileObj.Size, fileObj.Name)
		if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID || fileObj.Size == 0 {
			DebugLog("[VFS Rename] WARNING: File has no data after flush: fileID=%d, dataID=%d, size=%d", fileID, fileObj.DataID, fileObj.Size)
		}
		// Update directory listing cache
		if fileObj.PID > 0 {
			dirNode := &OrcasNode{
				fs:    n.fs,
				objID: fileObj.PID,
			}
			dirNode.appendChildToDirCache(fileObj.PID, fileObj)
			DebugLog("[VFS Rename] Updated directory listing cache after flush: fileID=%d, dirID=%d, name=%s", fileID, fileObj.PID, fileObj.Name)
		}
	} else {
		DebugLog("[VFS Rename] WARNING: Failed to re-fetch file object after flush: fileID=%d, error=%v", fileID, err)
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
func (n *OrcasNode) Release(ctx context.Context) syscall.Errno {
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

	// For non-.tmp files, execute Flush and Close
	versionID, err := ra.Flush()
	if err != nil {
		DebugLog("[VFS Release] ERROR: Failed to flush during release: fileID=%d, error=%v", fileID, err)
		// Record error but don't prevent close
	} else {
		DebugLog("[VFS Release] Flushed during release: fileID=%d, versionID=%d", fileID, versionID)
	}

	// Close RandomAccessor (this will flush any remaining data)
	err = ra.Close()
	if err != nil {
		DebugLog("[VFS Release] ERROR: Failed to close RandomAccessor: fileID=%d, error=%v", fileID, err)
	}

	// After flush, invalidate object cache
	n.invalidateObj()

	// Get updated object to log final size
	obj, err = n.getObj()
	if err == nil && obj != nil {
		DebugLog("[VFS Release] Successfully released file: fileID=%d, finalSize=%d", obj.ID, obj.Size)
	} else {
		DebugLog("[VFS Release] Successfully released file: fileID=%d (failed to get final obj: %v)", fileID, err)
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
	db, err := core.GetReadDB(n.fs.c, n.fs.bktID)
	if err != nil {
		DebugLog("[VFS queryFileByNameDirectly] ERROR: Failed to get database connection: %v", err)
		return 0, nil
	}
	// Note: Don't close the connection, it's from the pool

	var obj core.ObjectInfo
	// Query for file with matching name and parent ID (including deleted files)
	// Unique constraint is on (pid, name), so we need to check:
	// 1. pid = parentID (non-deleted file)
	// 2. pid = -parentID (deleted file, if parentID > 0)
	// 3. pid = -1 (deleted file from root, if parentID == 0)
	var rows *sql.Rows
	if parentID == 0 {
		// Root directory: check pid = 0 and pid = -1
		query := "SELECT id, pid, did, size, mtime, type, name, extra FROM obj WHERE name = ? AND type = ? AND (pid = ? OR pid = -1) LIMIT 1"
		rows, err = db.Query(query, fileName, core.OBJ_TYPE_FILE, parentID)
	} else {
		// Non-root: check pid = parentID and pid = -parentID
		query := "SELECT id, pid, did, size, mtime, type, name, extra FROM obj WHERE name = ? AND type = ? AND (pid = ? OR pid = ?) LIMIT 1"
		rows, err = db.Query(query, fileName, core.OBJ_TYPE_FILE, parentID, -parentID)
	}
	if err != nil {
		DebugLog("[VFS queryFileByNameDirectly] ERROR: Failed to query database: %v", err)
		return 0, nil
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&obj.ID, &obj.PID, &obj.DataID, &obj.Size, &obj.MTime, &obj.Type, &obj.Name, &obj.Extra)
		if err == nil {
			// Found file, return it (even if deleted)
			return obj.ID, &obj
		}
		DebugLog("[VFS queryFileByNameDirectly] ERROR: Failed to scan row: %v", err)
	}

	return 0, nil
}
