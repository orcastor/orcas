//go:build !windows
// +build !windows

package vfs

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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
	server, err := fs.Mount(mountPoint, ofs.root, &fs.Options{
		MountOptions: *opts,
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
	ra     atomic.Value // *RandomAccessor
}

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
	_                  = fs.FileReader(&OrcasNode{})
	_                  = fs.FileWriter(&OrcasNode{})
	_                  = fs.FileReleaser(&OrcasNode{})
)

// getObj gets object information (with cache)
// Optimization: use atomic operations, completely lock-free
func (n *OrcasNode) getObj() (*core.ObjectInfo, error) {
	// First check: atomic read
	if val := n.obj.Load(); val != nil {
		if obj, ok := val.(*core.ObjectInfo); ok && obj != nil {
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

	// Query object (executed outside lock)
	objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{n.objID})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, syscall.ENOENT
	}

	// Double check: check cache again (may have been updated by other goroutine)
	if val := n.obj.Load(); val != nil {
		if obj, ok := val.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}

	// Update cache (atomic operation)
	n.obj.Store(objs[0])

	return objs[0], nil
}

// invalidateObj invalidates object cache
func (n *OrcasNode) invalidateObj() {
	n.obj.Store((*core.ObjectInfo)(nil))
}

// Getattr gets file/directory attributes
func (n *OrcasNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
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
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return nil, syscall.ENOTDIR
	}

	// List directory contents
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		return nil, syscall.EIO
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
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return nil, syscall.ENOTDIR
	}

	// List directory contents
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		return nil, syscall.EIO
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

	// Add child objects
	for _, child := range children {
		mode := getMode(child.Type)
		entries = append(entries, fuse.DirEntry{
			Name: child.Name,
			Mode: mode,
			Ino:  uint64(child.ID),
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Create creates a file
func (n *OrcasNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return nil, nil, 0, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return nil, nil, 0, syscall.ENOTDIR
	}

	// Create file object
	fileObj := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   obj.ID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  name,
		Size:  0,
		MTime: core.Now(),
	}

	ids, err := n.fs.h.Put(n.fs.c, n.fs.bktID, []*core.ObjectInfo{fileObj})
	if err != nil || len(ids) == 0 || ids[0] == 0 {
		return nil, nil, 0, syscall.EIO
	}

	fileObj.ID = ids[0]

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

	// Invalidate parent directory cache
	n.invalidateObj()

	return fileInode, fileNode, fuse.FOPEN_DIRECT_IO, 0
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

	// Invalidate parent directory cache
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

	// Delete object
	err = n.fs.h.Delete(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		return syscall.EIO
	}

	// Invalidate parent directory cache
	n.invalidateObj()

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

	// Delete directory
	err = n.fs.h.Delete(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		return syscall.EIO
	}

	// Invalidate parent directory cache
	n.invalidateObj()

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

	// Find source object
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		return syscall.EIO
	}

	var sourceID int64
	for _, child := range children {
		if child.Name == name {
			sourceID = child.ID
			break
		}
	}

	if sourceID == 0 {
		return syscall.ENOENT
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

	// Rename
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

	// Invalidate both directories' cache
	n.invalidateObj()
	newParentNode.invalidateObj()

	return 0
}

// Read reads file content
func (n *OrcasNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return nil, syscall.EISDIR
	}

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		// Empty file
		return fuse.ReadResultData(nil), 0
	}

	// Get or create DataReader (supports caching for performance)
	reader, errno := n.getDataReader()
	if errno != 0 {
		return nil, errno
	}

	// Use cached reader position to optimize seeking
	// Note: Since DataReader is streaming, we need to read from beginning each time
	// But can avoid repeated creation by caching reader

	// Skip to specified offset
	if off > 0 {
		// Since DataReader is streaming, each read needs to start from beginning
		// Use io.CopyN to skip
		_, err := io.CopyN(io.Discard, reader, off)
		if err != nil && err != io.EOF {
			return nil, syscall.EIO
		}
	}

	// Read requested data
	result := make([]byte, len(dest))
	nRead, err := reader.Read(result)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(result[:nRead]), 0
}

// getDataReader gets or creates DataReader (with cache)
func (n *OrcasNode) getDataReader() (io.Reader, syscall.Errno) {
	// Note: Since FUSE reads may be random access, caching reader may not be very effective
	// But we can try to reuse reader for performance

	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		return nil, syscall.EIO
	}

	// Get DataInfo
	dataInfo, err := n.fs.h.GetDataInfo(n.fs.c, n.fs.bktID, obj.DataID)
	if err != nil {
		return nil, syscall.EIO
	}

	// Get encryption key
	var endecKey string
	if n.fs.sdkCfg != nil {
		endecKey = n.fs.sdkCfg.EndecKey
	}

	// Create new decodingChunkReader (create new one for each read, since FUSE reads are random access)
	// Directly read by chunk, decrypt, decompress, don't use DataReader
	reader := newDecodingChunkReader(n.fs.c, n.fs.h, n.fs.bktID, dataInfo, endecKey)

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
		return 0, syscall.EIO
	}

	// Write data (don't flush immediately)
	// ra.Write itself is thread-safe, don't need to hold raMu lock
	err = ra.Write(off, data)
	if err != nil {
		return 0, syscall.EIO
	}

	// Invalidate object cache (will get latest size on next read)
	// This operation is fast, but can be optimized to async
	n.invalidateObj()

	return uint32(len(data)), 0
}

// Flush flushes file
// Optimization: use atomic operations, completely lock-free
func (n *OrcasNode) Flush(ctx context.Context) syscall.Errno {
	// Atomically read ra
	val := n.ra.Load()
	if val == nil {
		return 0
	}

	ra, ok := val.(*RandomAccessor)
	if !ok || ra == nil {
		return 0
	}

	// Execute Flush (ra.Flush is thread-safe)
	_, err := ra.Flush()
	if err != nil {
		return syscall.EIO
	}
	// After flush, invalidate object cache
	n.invalidateObj()

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
		if ra, ok := val.(*RandomAccessor); ok && ra != nil {
			return ra, nil
		}
	}

	// Need to create, use CompareAndSwap to ensure only one goroutine creates
	// Create new RandomAccessor
	obj, err := n.getObj()
	if err != nil {
		return nil, err
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return nil, fmt.Errorf("object is not a file")
	}

	newRA, err := NewRandomAccessor(n.fs, obj.ID)
	if err != nil {
		return nil, err
	}

	// Try to atomically set ra (if already set by other goroutine, use existing)
	if !n.ra.CompareAndSwap(nil, newRA) {
		// Other goroutine already created, close what we created, use existing
		newRA.Close()
		if val := n.ra.Load(); val != nil {
			if ra, ok := val.(*RandomAccessor); ok && ra != nil {
				return ra, nil
			}
		}
	}

	return newRA, nil
}

// Release releases file handle (closes file)
// Optimization: use atomic operations, completely lock-free
func (n *OrcasNode) Release(ctx context.Context) syscall.Errno {
	// Atomically get and clear ra
	val := n.ra.Swap(nil)
	if val == nil {
		return 0
	}

	ra, ok := val.(*RandomAccessor)
	if !ok || ra == nil {
		return 0
	}

	// Execute Flush and Close (these operations may take time)
	// Flush buffer
	_, err := ra.Flush()
	if err != nil {
		// Record error but don't prevent close
	}

	// Close RandomAccessor
	ra.Close()

	// After flush, invalidate object cache
	n.invalidateObj()

	return 0
}
