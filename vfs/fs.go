//go:build !windows
// +build !windows

package vfs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/orca-zhang/ecache"
	"github.com/orcastor/orcas/core"
)

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

var streamingReaderCache = ecache.NewLRUCache(4, 256, 15*time.Second)

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
			// For file objects, also check global fileObjCache to get latest size
			// This ensures we get the most up-to-date file size after writes
			if obj.Type == core.OBJ_TYPE_FILE {
				cacheKey := formatCacheKey(obj.ID)
				if cached, ok := fileObjCache.Get(cacheKey); ok {
					if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
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
		cacheKey := formatCacheKey(n.objID)
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				// Update local cache for consistency
				n.obj.Store(cachedObj)
				DebugLog("[VFS getObj] Found in global cache: objID=%d, type=%d, size=%d", cachedObj.ID, cachedObj.Type, cachedObj.Size)
				return cachedObj, nil
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

	DebugLog("[VFS getObj] Got from database: objID=%d, type=%d, size=%d, DataID=%d",
		objs[0].ID, objs[0].Type, objs[0].Size, objs[0].DataID)

	// Double check: check cache again (may have been updated by other goroutine)
	if val := n.obj.Load(); val != nil {
		if obj, ok := val.(*core.ObjectInfo); ok && obj != nil {
			DebugLog("[VFS getObj] Found in local cache (after DB query): objID=%d, type=%d, size=%d",
				obj.ID, obj.Type, obj.Size)
			return obj, nil
		}
	}

	// Update both local and global cache (atomic operation)
	n.obj.Store(objs[0])
	// Update global fileObjCache for both files and directories
	// This allows GetAttr to use cached information from Readdir/Lookup
	cacheKey := formatCacheKey(n.objID)
	fileObjCache.Put(cacheKey, objs[0])
	DebugLog("[VFS getObj] Updated caches: objID=%d, type=%d, size=%d",
		objs[0].ID, objs[0].Type, objs[0].Size)

	return objs[0], nil
}

// invalidateObj invalidates object cache
func (n *OrcasNode) invalidateObj() {
	n.obj.Store((*core.ObjectInfo)(nil))
}

// invalidateDirListCache invalidates directory listing cache
func (n *OrcasNode) invalidateDirListCache(dirID int64) {
	cacheKey := formatCacheKey(dirID)
	dirListCache.Del(cacheKey)
	DebugLog("[VFS invalidateDirListCache] Invalidated directory listing cache: dirID=%d", dirID)
}

// appendChildToDirCache appends a newly created child object into the
// cached directory listing instead of invalidating the entire cache.
func (n *OrcasNode) appendChildToDirCache(dirID int64, child *core.ObjectInfo) {
	if child == nil {
		return
	}
	cacheKey := formatCacheKey(dirID)
	if cached, ok := dirListCache.Get(cacheKey); ok {
		if children, ok := cached.([]*core.ObjectInfo); ok && children != nil {
			for _, existing := range children {
				if existing != nil && existing.ID == child.ID {
					return
				}
			}
			newChildren := make([]*core.ObjectInfo, len(children)+1)
			copy(newChildren, children)
			newChildren[len(children)] = child
			dirListCache.Put(cacheKey, newChildren)
			DebugLog("[VFS appendChildToDirCache] Appended child to cache: dirID=%d, childID=%d", dirID, child.ID)
		}
	}
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
	for _, child := range children {
		if child.Name == name {
			// Create child node
			childNode := &OrcasNode{
				fs:    n.fs,
				objID: child.ID,
			}
			childNode.obj.Store(child)

			// Create Inode based on type
			var stableAttr fs.StableAttr
			if child.Type == core.OBJ_TYPE_DIR {
				stableAttr = fs.StableAttr{
					Mode: syscall.S_IFDIR,
					Ino:  uint64(child.ID),
				}
			} else {
				stableAttr = fs.StableAttr{
					Mode: syscall.S_IFREG,
					Ino:  uint64(child.ID),
				}
			}

			childInode := n.NewInode(ctx, childNode, stableAttr)

			// Fill EntryOut
			out.Mode = getMode(child.Type)
			out.Size = uint64(child.Size)
			out.Mtime = uint64(child.MTime)
			out.Ctime = out.Mtime
			out.Atime = out.Mtime
			out.Ino = uint64(child.ID)

			return childInode, 0
		}
	}

	return nil, syscall.ENOENT
}

// Readdir reads directory contents
func (n *OrcasNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// DebugLog("[VFS Readdir] Reading directory: objID=%d", n.objID)
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

	// Build directory stream
	entries := make([]fuse.DirEntry, 0, len(children)+1)
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

	// Add child objects and cache their information for GetAttr optimization
	for _, child := range children {
		mode := getMode(child.Type)
		entries = append(entries, fuse.DirEntry{
			Name: child.Name,
			Mode: mode,
			Ino:  uint64(child.ID),
		})
	}

	// Asynchronously preload child directories
	go n.preloadChildDirs(children)

	return fs.NewListDirStream(entries), 0
}

// getDirListWithCache gets directory listing with cache and singleflight
// Uses singleflight to prevent duplicate concurrent requests for the same directory
func (n *OrcasNode) getDirListWithCache(dirID int64) ([]*core.ObjectInfo, syscall.Errno) {
	// Check cache first
	cacheKey := formatCacheKey(dirID)
	if cached, ok := dirListCache.Get(cacheKey); ok {
		if children, ok := cached.([]*core.ObjectInfo); ok && children != nil {
			// DebugLog("[VFS getDirListWithCache] Found in cache: dirID=%d, count=%d", dirID, len(children))
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

		// Cache the result
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

// preloadChildDirs asynchronously preloads child directories
// This improves performance when navigating through directory trees
func (n *OrcasNode) preloadChildDirs(children []*core.ObjectInfo) {
	for _, child := range children {
		// Only preload directories, not files
		if child.Type != core.OBJ_TYPE_DIR {
			continue
		}

		// Check if already cached
		cacheKey := formatCacheKey(child.ID)
		fileObjCache.Put(cacheKey, child)

		if _, ok := dirListCache.Get(cacheKey); ok {
			// Already cached, skip
			continue
		}

		// Preload directory listing asynchronously
		// Use singleflight to prevent duplicate requests
		key := fmt.Sprintf("%d", child.ID)
		go func(dirID int64, cacheKey string, key string) {
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
					grandchildCacheKey := formatCacheKey(grandchild.ID)
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
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
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
		cacheKey := formatCacheKey(existingFileID)
		if cached, ok := fileObjCache.Get(cacheKey); ok {
			if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
				existingFileObj = cachedObj
			}
		}

		// If cache miss, get from database
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
		}

		// If O_TRUNC is set, truncate the file
		if flags&syscall.O_TRUNC != 0 {
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

	// File doesn't exist, check if O_CREAT is set
	// O_CREAT is required to create a new file
	if flags&syscall.O_CREAT == 0 {
		// O_CREAT not set, cannot create file
		return nil, nil, 0, syscall.ENOENT
	}

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
		DebugLog("[VFS Create] ERROR: Failed to put file object to database: name=%s, parentID=%d, error=%v", name, obj.ID, err)
		return nil, nil, 0, syscall.EIO
	}
	if len(ids) == 0 || ids[0] == 0 {
		DebugLog("[VFS Create] ERROR: Put returned empty or zero ID: name=%s, parentID=%d, ids=%v", name, obj.ID, ids)
		return nil, nil, 0, syscall.EIO
	}

	fileObj.ID = ids[0]
	DebugLog("[VFS Create] Successfully created file: name=%s, fileID=%d, parentID=%d", name, fileObj.ID, obj.ID)

	// Cache new file object for GetAttr optimization
	cacheKey := formatCacheKey(fileObj.ID)
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
	obj, err := n.getObj()
	if err != nil {
		return nil, 0, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
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
	cacheKey := formatCacheKey(dirObj.ID)
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
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return syscall.ENOTDIR
	}

	// Find child object
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		return syscall.EIO
	}

	var targetID int64
	for _, child := range children {
		if child.Name == name && child.Type == core.OBJ_TYPE_FILE {
			targetID = child.ID
			break
		}
	}

	if targetID == 0 {
		return syscall.ENOENT
	}

	// Step 1: Remove from parent directory first (mark as deleted)
	// This makes the file disappear from parent's listing immediately
	err = n.fs.h.Recycle(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		return syscall.EIO
	}

	// Step 2: Refresh cache immediately
	cacheKey := formatCacheKey(targetID)
	fileObjCache.Del(cacheKey)

	// Invalidate parent directory cache
	n.invalidateObj()
	// Invalidate parent directory listing cache
	n.invalidateDirListCache(obj.ID)

	// Step 3: Asynchronously delete and clean up (permanent deletion)
	// This includes physical deletion of data files and metadata
	go func() {
		// Use background context for async operation
		bgCtx := context.Background()
		err := n.fs.h.Delete(bgCtx, n.fs.bktID, targetID)
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
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return syscall.ENOTDIR
	}

	// Find child directory
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		return syscall.EIO
	}

	var targetID int64
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
	dirChildren, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, targetID, core.ListOptions{
		Count: 1,
	})
	if err != nil {
		return syscall.EIO
	}
	if len(dirChildren) > 0 {
		return syscall.ENOTEMPTY
	}

	// Step 1: Remove from parent directory first (mark as deleted)
	// This makes the directory disappear from parent's listing immediately
	err = n.fs.h.Recycle(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		return syscall.EIO
	}

	// Step 2: Refresh cache immediately
	cacheKey := formatCacheKey(targetID)
	fileObjCache.Del(cacheKey)
	// Also remove directory listing cache for the deleted directory
	dirListCache.Del(cacheKey)

	// Invalidate parent directory cache
	n.invalidateObj()
	// Invalidate parent directory listing cache
	n.invalidateDirListCache(obj.ID)

	// Step 3: Asynchronously delete and clean up (permanent deletion)
	// This includes physical deletion of data files and metadata
	go func() {
		// Use background context for async operation
		bgCtx := context.Background()
		err := n.fs.h.Delete(bgCtx, n.fs.bktID, targetID)
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
	val := n.ra.Load()
	if val != nil && val != releasedMarker {
		if ra, ok := val.(*RandomAccessor); ok && ra != nil {
			fileObj, err := ra.getFileObj()
			if err == nil && fileObj != nil && fileObj.Name == name && fileObj.PID == obj.ID {
				sourceID = fileObj.ID
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
		for _, child := range children {
			if child.Name == name {
				// Found potential match, try to get from cache first
				cacheKey := formatCacheKey(child.ID)
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
	} else {
		// If found from RandomAccessor, get source object info
		// Try cache first
		cacheKey := formatCacheKey(sourceID)
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
		return syscall.ENOENT
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
	if existingTargetID == 0 {
		targetChildren, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, newParentObj.ID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		if err != nil {
			return syscall.EIO
		}

		// First, try to find from cache for each child
		for _, child := range targetChildren {
			if child.Name == newName {
				// Found potential match, try to get from cache first
				cacheKey := formatCacheKey(child.ID)
				if cached, ok := fileObjCache.Get(cacheKey); ok {
					if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
						// Use cached object (may have more up-to-date information)
						existingTargetID = cachedObj.ID
						existingTargetObj = cachedObj
						break
					}
				}
				// If cache miss, use child from List
				if existingTargetID == 0 {
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

	// If target file exists and is a file (not directory), create a version from it
	if existingTargetID > 0 {
		// Get the existing target object to check its type (try cache first)
		var existingObj *core.ObjectInfo
		if existingTargetObj != nil && existingTargetObj.Type == core.OBJ_TYPE_FILE {
			existingObj = existingTargetObj
		} else {
			// Try to get from cache first
			targetCacheKey := formatCacheKey(existingTargetID)
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

		if existingObj != nil && existingObj.Type == core.OBJ_TYPE_FILE {
			// Check if existing target file is a .tmp file
			existingNameLower := strings.ToLower(existingObj.Name)
			isExistingTmpFile := strings.HasSuffix(existingNameLower, ".tmp")

			if isExistingTmpFile {
				// If target file is a .tmp file, delete it instead of creating a version
				// .tmp files are temporary and should be removed when overwritten
				DebugLog("[VFS Rename] Target file is .tmp file, deleting it: fileID=%d, name=%s", existingTargetID, existingObj.Name)

				// First, try to flush and close any open RandomAccessor for the target .tmp file
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

				// Delete the .tmp file (asynchronously to avoid blocking rename)
				go func() {
					bgCtx := context.Background()
					err := n.fs.h.Delete(bgCtx, n.fs.bktID, existingTargetID)
					if err != nil {
						DebugLog("[VFS Rename] ERROR: Failed to delete target .tmp file: fileID=%d, error=%v", existingTargetID, err)
					} else {
						DebugLog("[VFS Rename] Successfully deleted target .tmp file: fileID=%d", existingTargetID)
					}
				}()

				// Remove from cache immediately
				targetCacheKey := formatCacheKey(existingTargetID)
				fileObjCache.Del(targetCacheKey)
			} else {
				// Create version from existing file (non-.tmp files should preserve versions)
				// Note: We need to check if handler supports CreateVersionFromFile
				if lh, ok := n.fs.h.(*core.LocalHandler); ok {
					err = lh.CreateVersionFromFile(n.fs.c, n.fs.bktID, existingTargetID)
					if err != nil {
						// Log error but continue with rename (don't fail the operation)
						// The existing file will be overwritten
					}
				}
			}
		}
	}

	// Rename source file to target name
	err = n.fs.h.Rename(n.fs.c, n.fs.bktID, sourceID, newName)
	if err != nil {
		return syscall.EIO
	}

	// If moved to different directory, need to move
	if newParentObj.ID != obj.ID {
		err = n.fs.h.MoveTo(n.fs.c, n.fs.bktID, sourceID, newParentObj.ID)
		if err != nil {
			return syscall.EIO
		}
	}

	// Update cache after database operations
	// Update source file cache with new name and parent
	n.updateFileObjCache(sourceID, newName, newParentObj.ID)

	// Invalidate DataInfo cache for source file to ensure fresh data after flush
	// This is important for .tmp files that were just flushed
	if sourceObj.Type == core.OBJ_TYPE_FILE && sourceObj.DataID > 0 {
		dataInfoCacheKey := formatCacheKey(sourceObj.DataID)
		dataInfoCache.Del(dataInfoCacheKey)
		DebugLog("[VFS Rename] Invalidated DataInfo cache: fileID=%d, dataID=%d", sourceID, sourceObj.DataID)
	}

	// Note: After creating the version from the existing target file,
	// the existing target file (existingTargetID) now has a version.
	// When we rename the source file to the target name, the source file
	// will replace the existing target file (same name, same parent).
	// The old target file's data is preserved as a version.

	if sourceObj.Type == core.OBJ_TYPE_FILE {
		if isRemovingTmp {
			DebugLog("[VFS Rename] .tmp extension removed (flush already performed): fileID=%d, oldName=%s, newName=%s", sourceID, sourceObj.Name, newName)
		}

		// Writing versions are no longer maintained for .tmp rename flow
		// Update cached source object name for future logic
		sourceObj.Name = newName
	}

	// Invalidate both directories' cache
	n.invalidateObj()
	newParentNode.invalidateObj()
	// Invalidate both directories' listing cache
	n.invalidateDirListCache(obj.ID)
	newParentNode.invalidateDirListCache(newParentObj.ID)

	return 0
}

// Read reads file content
func (n *OrcasNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Force refresh object cache to get latest DataID (important after writes)
	// This ensures we get the latest DataID from database or fileObjCache
	n.invalidateObj()

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Read] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return nil, syscall.EISDIR
	}

	DebugLog("[VFS Read] Reading file: objID=%d, DataID=%d, Size=%d, offset=%d, size=%d", obj.ID, obj.DataID, obj.Size, off, len(dest))

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		// Empty file
		DebugLog("[VFS Read] Empty file (no DataID): objID=%d", obj.ID)
		return fuse.ReadResultData(nil), 0
	}

	// Get dataReader (cached by dataID, one per file)
	reader, errno := n.getDataReader(off)
	if errno != 0 {
		DebugLog("[VFS Read] ERROR: Failed to get DataReader: objID=%d, DataID=%d, error=%v", obj.ID, obj.DataID, errno)
		return nil, errno
	}

	// Use dataReader interface (Read(buf, offset))
	nRead, err := reader.Read(dest, off)
	if err != nil && err != io.EOF {
		DebugLog("[VFS Read] ERROR: Read failed: objID=%d, DataID=%d, offset=%d, size=%d, error=%v", obj.ID, obj.DataID, off, len(dest), err)
		return nil, syscall.EIO
	}
	DebugLog("[VFS Read] Successfully read data: objID=%d, DataID=%d, offset=%d, requested=%d, read=%d", obj.ID, obj.DataID, off, len(dest), nRead)

	// Verify read data integrity (for debugging - can be disabled in production)
	// Only verify on first read (offset=0) to avoid performance impact
	if off == 0 && nRead > 0 {
		// Note: Full verification is expensive, so we only log a warning if size seems wrong
		// Full verification can be triggered manually via VerifyFileData()
		if obj.Size > 0 && int64(nRead) > obj.Size {
			DebugLog("[VFS Read] WARNING: Read more than file size: objID=%d, read=%d, fileSize=%d", obj.ID, nRead, obj.Size)
		}
	}

	return fuse.ReadResultData(dest[:nRead]), 0
}

// getDataReader gets or creates DataReader (with cache)
// offset: starting offset for streaming readers (compressed/encrypted)
// For plain readers, offset is ignored as they support ReadAt
// For compressed/encrypted files, uses dataID as cache key to ensure one file uses the same reader
func (n *OrcasNode) getDataReader(offset int64) (dataReader, syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS getDataReader] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return nil, syscall.ENOENT
	}

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		DebugLog("[VFS getDataReader] ERROR: Empty DataID: objID=%d, DataID=%d", obj.ID, obj.DataID)
		return nil, syscall.EIO
	}

	// Get DataInfo
	DebugLog("[VFS getDataReader] Getting DataInfo: objID=%d, DataID=%d", obj.ID, obj.DataID)
	dataInfo, err := n.fs.h.GetDataInfo(n.fs.c, n.fs.bktID, obj.DataID)
	if err != nil {
		DebugLog("[VFS getDataReader] ERROR: Failed to get DataInfo: objID=%d, DataID=%d, error=%v", obj.ID, obj.DataID, err)
		return nil, syscall.EIO
	}
	DebugLog("[VFS getDataReader] Got DataInfo: objID=%d, DataID=%d, OrigSize=%d, Size=%d", obj.ID, obj.DataID, dataInfo.OrigSize, dataInfo.Size)

	hasCompression := dataInfo.Kind&core.DATA_CMPR_MASK != 0
	hasEncryption := dataInfo.Kind&core.DATA_ENDEC_MASK != 0

	DebugLog("[VFS getDataReader] Has compression: %v, Has encryption: %v", hasCompression, hasEncryption)

	// Always use bucket's default chunk size (force unified chunkSize)
	chunkSize := n.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Use unified chunkReader for both plain and compressed/encrypted data
	// Use dataID as cache key to ensure one file uses the same reader
	// This allows sharing chunk cache across all reads of the same file
	cacheKey := formatCacheKey(obj.DataID)

	// Try to get cached reader
	if cached, ok := decodingReaderCache.Get(cacheKey); ok {
		if reader, ok := cached.(*chunkReader); ok && reader != nil {
			DebugLog("[VFS getDataReader] Reusing cached reader: objID=%d, DataID=%d", obj.ID, obj.DataID)
			return reader, 0
		}
	}

	// Create new reader
	var endecKey string
	bucket := n.fs.getBucketConfig()
	if bucket != nil {
		endecKey = bucket.EndecKey
	}

	// Create chunkReader (dataInfo is always available here)
	var reader *chunkReader
	if !hasCompression && !hasEncryption {
		// Plain data: create chunkReader with plain DataInfo
		plainDataInfo := &core.DataInfo{
			ID:       obj.DataID,
			OrigSize: dataInfo.OrigSize,
			Size:     dataInfo.Size,
			Kind:     0, // Plain data
		}
		reader = newChunkReader(n.fs.c, n.fs.h, n.fs.bktID, plainDataInfo, "", chunkSize)
	} else {
		// Compressed/encrypted: use chunkReader with processing
		reader = newChunkReader(n.fs.c, n.fs.h, n.fs.bktID, dataInfo, endecKey, chunkSize)
	}

	// Cache the reader (one per dataID)
	decodingReaderCache.Put(cacheKey, reader)
	DebugLog("[VFS getDataReader] Created and cached new reader: objID=%d, DataID=%d", obj.ID, obj.DataID)

	return reader, 0
}

// Write writes file content
// Optimization: reduce lock hold time, ra.Write itself is thread-safe
func (n *OrcasNode) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return 0, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
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
	if err == nil {
		DebugLog("[VFS Flush] Flushing file: fileID=%d, currentSize=%d", obj.ID, obj.Size)
	}
	versionID, err := ra.Flush()
	if err != nil {
		DebugLog("[VFS Flush] ERROR: Failed to flush file: fileID=%d, error=%v", obj.ID, err)
		return syscall.EIO
	}
	// After flush, invalidate object cache
	n.invalidateObj()

	// Get updated object to log final size
	obj, err = n.getObj()
	if err == nil {
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

	// Reuse existing writer RandomAccessor for .tmp files to preserve buffered data
	if n.fs != nil && isTempFile(obj) {
		if existing := n.fs.getRandomAccessorByFileID(obj.ID); existing != nil && existing.hasTempFileWriter() {
			n.ra.Store(existing)
			return existing, nil
		}
	}

	newRA, err := NewRandomAccessor(n.fs, obj.ID)
	if err != nil {
		return nil, err
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
	if n.fs != nil {
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
	cacheKey := formatCacheKey(fileID)
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
func (n *OrcasNode) forceFlushTempFileBeforeRename(fileID int64, oldName, newName string) {
	DebugLog("[VFS Rename] .tmp file being renamed, forcing flush: fileID=%d, oldName=%s, newName=%s", fileID, oldName, newName)

	var raToFlush *RandomAccessor
	if val := n.ra.Load(); val != nil && val != releasedMarker {
		if ra, ok := val.(*RandomAccessor); ok && ra != nil && ra.fileID == fileID {
			raToFlush = ra
		}
	}
	if raToFlush == nil && n.fs != nil {
		raToFlush = n.fs.getRandomAccessorByFileID(fileID)
	}

	if raToFlush == nil {
		DebugLog("[VFS Rename] WARNING: Unable to find RandomAccessor for .tmp file flush: fileID=%d", fileID)
		return
	}

	if _, err := raToFlush.ForceFlush(); err != nil {
		DebugLog("[VFS Rename] ERROR: Failed to force flush .tmp file: fileID=%d, error=%v", fileID, err)
	} else {
		DebugLog("[VFS Rename] Successfully forced flush .tmp file: fileID=%d", fileID)

		// Verify flushed data (for debugging)
		// Get the file node to verify
		fileNode := &OrcasNode{
			fs:    n.fs,
			objID: fileID,
		}
		if err := fileNode.VerifyChunkData(); err != nil {
			DebugLog("[VFS Rename] WARNING: Chunk verification failed after flush: fileID=%d, error=%v", fileID, err)
		} else {
			DebugLog("[VFS Rename] Chunk verification passed after flush: fileID=%d", fileID)
		}
	}

	// After forcing flush, unregister RandomAccessor to avoid stale references
	if n.fs != nil {
		DebugLog("[VFS Rename] Unregistering RandomAccessor: fileID=%d", fileID)
		n.fs.unregisterRandomAccessor(fileID, raToFlush)
	}
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

	// Execute Flush and Close (these operations may take time)
	// Flush buffer
	obj, err := n.getObj()
	fileID := ra.fileID // Use ra.fileID as fallback if obj is nil
	if err == nil && obj != nil {
		fileID = obj.ID
		DebugLog("[VFS Release] Releasing file: fileID=%d, currentSize=%d", fileID, obj.Size)
	} else {
		DebugLog("[VFS Release] Releasing file: fileID=%d (failed to get obj: %v)", fileID, err)
	}
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

func streamingReaderCacheKey(dataID int64, offset int64) string {
	return fmt.Sprintf("stream_reader:%d:%d", dataID, offset)
}

func acquireStreamingReader(dataID int64, offset int64) *cachedReader {
	// For compressed/encrypted files, readers are now cached by dataID in decodingReaderCache
	// This function is kept for backward compatibility but may not be used for decodingChunkReader
	// Try to find any cached reader for this dataID (not just matching offset)
	key := streamingReaderCacheKey(dataID, offset)
	if val, ok := streamingReaderCache.Get(key); ok {
		if entry, ok := val.(*cachedReader); ok && entry != nil && entry.reader != nil {
			// Check if reader supports ReadAt (for random access)
			if _, supportsReadAt := entry.reader.(io.ReaderAt); supportsReadAt {
				// Reader supports ReadAt, can be reused for any offset
				streamingReaderCache.Del(key)
				return entry
			}
			// For streaming readers, only reuse if offset matches
			if entry.nextOffset == offset {
				streamingReaderCache.Del(key)
				return entry
			}
		}
		streamingReaderCache.Del(key)
	}

	return nil
}

func storeStreamingReader(entry *cachedReader) {
	if entry == nil || entry.reader == nil {
		return
	}
	streamingReaderCache.Put(streamingReaderCacheKey(entry.dataID, entry.nextOffset), entry)
}

func releaseStreamingReader(dataID int64, offset int64) {
	streamingReaderCache.Del(streamingReaderCacheKey(dataID, offset))
}
