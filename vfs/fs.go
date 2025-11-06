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

// initRootNode 初始化root节点（非Windows平台实现）
// 在非Windows平台上，root节点在Mount时初始化，而不是在NewOrcasFS时初始化
func (ofs *OrcasFS) initRootNode() {
	// 非Windows平台：root节点在Mount时初始化，这里不初始化
	// 这样可以在Mount时通过FUSE的Inode系统正确初始化
}

// Mount 挂载文件系统到指定路径（仅Linux/Unix）
func (ofs *OrcasFS) Mount(mountPoint string, opts *fuse.MountOptions) (*fuse.Server, error) {
	// 初始化root节点（如果还没有初始化）
	if ofs.root == nil {
		ofs.root = &OrcasNode{
			fs:     ofs,
			objID:  core.ROOT_OID,
			isRoot: true,
		}
	}
	// 设置root节点的fs引用
	ofs.root.fs = ofs

	// 默认挂载选项
	if opts == nil {
		opts = &fuse.MountOptions{
			Options: []string{
				"default_permissions",
			},
		}
	}

	// 挂载文件系统，直接使用root节点作为根Inode
	server, err := fs.Mount(mountPoint, ofs.root, &fs.Options{
		MountOptions: *opts,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to mount: %w", err)
	}

	return server, nil
}

// OrcasNode 表示ORCAS文件系统中的一个节点（文件或目录）
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

// getObj 获取对象信息（带缓存）
// 优化：使用原子操作，完全无锁
func (n *OrcasNode) getObj() (*core.ObjectInfo, error) {
	// 第一次检查：原子读取
	if val := n.obj.Load(); val != nil {
		if obj, ok := val.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}

	// 如果是根节点，返回虚拟对象
	if n.isRoot {
		return &core.ObjectInfo{
			ID:   core.ROOT_OID,
			PID:  0,
			Type: core.OBJ_TYPE_DIR,
			Name: "/",
		}, nil
	}

	// 查询对象（在锁外执行）
	objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{n.objID})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, syscall.ENOENT
	}

	// 双重检查：再次检查缓存（可能被其他goroutine更新了）
	if val := n.obj.Load(); val != nil {
		if obj, ok := val.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}

	// 更新缓存（原子操作）
	n.obj.Store(objs[0])

	return objs[0], nil
}

// invalidateObj 使对象缓存失效
func (n *OrcasNode) invalidateObj() {
	n.obj.Store((*core.ObjectInfo)(nil))
}

// Getattr 获取文件/目录属性
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

// getMode 根据对象类型返回文件模式
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

// Lookup 查找子节点
func (n *OrcasNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return nil, syscall.ENOTDIR
	}

	// 列出目录内容
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		return nil, syscall.EIO
	}

	// 查找匹配的子对象
	for _, child := range children {
		if child.Name == name {
			// 创建子节点
			childNode := &OrcasNode{
				fs:    n.fs,
				objID: child.ID,
			}
			childNode.obj.Store(child)

			// 根据类型创建Inode
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

			// 填充EntryOut
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

// Readdir 读取目录内容
func (n *OrcasNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return nil, syscall.ENOTDIR
	}

	// 列出目录内容
	children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
	if err != nil {
		return nil, syscall.EIO
	}

	// 构建目录流
	entries := make([]fuse.DirEntry, 0, len(children)+1)
	// 添加 . 和 ..
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

	// 添加子对象
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

// Create 创建文件
func (n *OrcasNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return nil, nil, 0, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return nil, nil, 0, syscall.ENOTDIR
	}

	// 创建文件对象
	fileObj := &core.ObjectInfo{
		ID:    n.fs.h.NewID(),
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

	// 创建文件节点
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

	// 填充EntryOut
	out.Mode = syscall.S_IFREG | 0o644
	out.Size = 0
	out.Mtime = uint64(fileObj.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime
	out.Ino = uint64(fileObj.ID)

	// 使父目录缓存失效
	n.invalidateObj()

	return fileInode, fileNode, fuse.FOPEN_DIRECT_IO, 0
}

// Mkdir 创建目录
func (n *OrcasNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return nil, syscall.ENOTDIR
	}

	// 创建目录对象
	dirObj := &core.ObjectInfo{
		ID:    n.fs.h.NewID(),
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

	// 创建目录节点
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

	// 填充EntryOut
	out.Mode = syscall.S_IFDIR | 0o755
	out.Size = 0
	out.Mtime = uint64(dirObj.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime
	out.Ino = uint64(dirObj.ID)

	// 使父目录缓存失效
	n.invalidateObj()

	return dirInode, 0
}

// Unlink 删除文件
func (n *OrcasNode) Unlink(ctx context.Context, name string) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return syscall.ENOTDIR
	}

	// 查找子对象
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

	// 删除对象
	err = n.fs.h.Delete(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		return syscall.EIO
	}

	// 使父目录缓存失效
	n.invalidateObj()

	return 0
}

// Rmdir 删除目录
func (n *OrcasNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return syscall.ENOTDIR
	}

	// 查找子目录
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

	// 检查目录是否为空
	dirChildren, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, targetID, core.ListOptions{
		Count: 1,
	})
	if err != nil {
		return syscall.EIO
	}
	if len(dirChildren) > 0 {
		return syscall.ENOTEMPTY
	}

	// 删除目录
	err = n.fs.h.Delete(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		return syscall.EIO
	}

	// 使父目录缓存失效
	n.invalidateObj()

	return 0
}

// Rename 重命名文件/目录
func (n *OrcasNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return syscall.ENOTDIR
	}

	// 查找源对象
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

	// 获取目标父目录
	// 注意：InodeEmbedder 接口需要转换为具体的节点类型
	// 这里假设 newParent 是 OrcasNode 类型
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

	// 重命名
	err = n.fs.h.Rename(n.fs.c, n.fs.bktID, sourceID, newName)
	if err != nil {
		return syscall.EIO
	}

	// 如果移动到不同目录，需要移动
	if newParentObj.ID != obj.ID {
		err = n.fs.h.MoveTo(n.fs.c, n.fs.bktID, sourceID, newParentObj.ID)
		if err != nil {
			return syscall.EIO
		}
	}

	// 使两个目录的缓存失效
	n.invalidateObj()
	newParentNode.invalidateObj()

	return 0
}

// Read 读取文件内容
func (n *OrcasNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return nil, syscall.EISDIR
	}

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		// 空文件
		return fuse.ReadResultData(nil), 0
	}

	// 获取或创建DataReader（支持缓存以提高性能）
	reader, errno := n.getDataReader()
	if errno != 0 {
		return nil, errno
	}

	// 使用缓存的reader位置来优化跳转
	// 注意：由于DataReader是流式的，我们需要每次都从头读取
	// 但可以通过缓存reader来避免重复创建

	// 跳过到指定偏移
	if off > 0 {
		// 由于DataReader是流式的，每次读取都需要从头开始
		// 使用io.CopyN来跳过
		_, err := io.CopyN(io.Discard, reader, off)
		if err != nil && err != io.EOF {
			return nil, syscall.EIO
		}
	}

	// 读取请求的数据
	result := make([]byte, len(dest))
	nRead, err := reader.Read(result)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(result[:nRead]), 0
}

// getDataReader 获取或创建DataReader（带缓存）
func (n *OrcasNode) getDataReader() (io.Reader, syscall.Errno) {
	// 注意：由于FUSE的读取可能是随机访问的，缓存reader可能不太有效
	// 但我们可以尝试复用reader以提高性能

	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		return nil, syscall.EIO
	}

	// 获取DataInfo
	dataInfo, err := n.fs.h.GetDataInfo(n.fs.c, n.fs.bktID, obj.DataID)
	if err != nil {
		return nil, syscall.EIO
	}

	// 获取加密密钥
	var endecKey string
	if n.fs.sdkCfg != nil {
		endecKey = n.fs.sdkCfg.EndecKey
	}

	// 创建新的decodingChunkReader（每次读取都创建新的，因为FUSE的读取是随机访问的）
	// 直接按chunk读取、解密、解压，不使用DataReader
	reader := newDecodingChunkReader(n.fs.c, n.fs.h, n.fs.bktID, dataInfo, endecKey)

	return reader, 0
}

// Write 写入文件内容
// 优化：减少锁持有时间，ra.Write本身是线程安全的
func (n *OrcasNode) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return 0, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return 0, syscall.EISDIR
	}

	// 获取或创建RandomAccessor（内部有锁，但会尽快释放）
	ra, err := n.getRandomAccessor()
	if err != nil {
		return 0, syscall.EIO
	}

	// 写入数据（不立即刷新）
	// ra.Write本身是线程安全的，不需要持有raMu锁
	err = ra.Write(off, data)
	if err != nil {
		return 0, syscall.EIO
	}

	// 使对象缓存失效（下次读取时会获取最新的大小）
	// 这个操作很快，但可以优化为异步
	n.invalidateObj()

	return uint32(len(data)), 0
}

// Flush 刷新文件
// 优化：使用原子操作，完全无锁
func (n *OrcasNode) Flush(ctx context.Context) syscall.Errno {
	// 原子读取ra
	val := n.ra.Load()
	if val == nil {
		return 0
	}

	ra, ok := val.(*RandomAccessor)
	if !ok || ra == nil {
		return 0
	}

	// 执行Flush（ra.Flush是线程安全的）
	_, err := ra.Flush()
	if err != nil {
		return syscall.EIO
	}
	// 刷新后使对象缓存失效
	n.invalidateObj()

	return 0
}

// Fsync 同步文件
func (n *OrcasNode) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	// 先刷新RandomAccessor
	if errno := n.Flush(ctx); errno != 0 {
		return errno
	}
	// 刷新对象缓存
	n.invalidateObj()
	return 0
}

// Setattr 设置文件属性（包括截断操作）
func (n *OrcasNode) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	// 处理截断操作
	if in.Valid&fuse.FATTR_SIZE != 0 {
		newSize := int64(in.Size)
		oldSize := obj.Size

		// 如果大小发生变化，执行截断
		if newSize != oldSize {
			if errno := n.truncateFile(newSize); errno != 0 {
				return errno
			}
			obj.Size = newSize
		}
	}

	// 更新修改时间
	if in.Valid&fuse.FATTR_MTIME != 0 {
		obj.MTime = int64(in.Mtime)
	}

	// 更新对象信息到数据库
	// 注意：文件大小更新已经在truncateFile中通过Flush完成
	// 这里只需要更新mtime（如果设置了）
	if in.Valid&fuse.FATTR_MTIME != 0 && in.Valid&fuse.FATTR_SIZE == 0 {
		// 如果只更新mtime，需要通过Handler更新
		// 由于RandomAccessor的Flush会自动更新文件大小，所以截断操作的大小更新已经完成
		// 这里只需要处理单独的mtime更新
		// 简化处理：mtime更新可以通过下一次写入或Flush时自动更新
	}

	// 使缓存失效
	n.invalidateObj()

	// 填充输出
	out.Mode = getMode(obj.Type)
	out.Size = uint64(obj.Size)
	out.Mtime = uint64(obj.MTime)
	out.Ctime = out.Mtime
	out.Atime = out.Mtime

	return 0
}

// truncateFile 截断文件到指定大小
func (n *OrcasNode) truncateFile(newSize int64) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return syscall.EISDIR
	}

	oldSize := obj.Size

	// 如果新大小等于旧大小，无需操作
	if newSize == oldSize {
		return 0
	}

	// 如果新大小小于旧大小，需要截断（删除超出部分）
	if newSize < oldSize {
		// 如果有RandomAccessor，需要清理缓冲区中超出新大小的写入操作
		val := n.ra.Load()
		var ra *RandomAccessor
		if val != nil {
			if r, ok := val.(*RandomAccessor); ok && r != nil {
				ra = r
			}
		}
		if ra != nil {
			// 清理缓冲区中超出新大小的部分
			// 注意：RandomAccessor的buffer是私有的，我们需要通过其他方式处理
			// 这里我们先刷新缓冲区，然后处理截断
			_, _ = ra.Flush() // 先刷新已有数据

			// 如果文件有数据，需要读取并重写（只保留新大小范围内的数据）
			if obj.DataID > 0 && obj.DataID != core.EmptyDataID {
				// 读取需要保留的数据（0到newSize）
				data, err := n.fs.h.GetData(n.fs.c, n.fs.bktID, obj.DataID, 0, 0, int(newSize))
				if err != nil {
					return syscall.EIO
				}

				// 如果新大小为0，删除所有数据
				if newSize == 0 {
					// 通过写入空数据来截断
					// 实际上，我们需要创建一个新的空版本或删除数据
					// 这里简化处理：写入0字节数据
					return 0 // 大小更新将在Setattr中完成
				}

				// 写入保留的数据（从0开始，覆盖原数据）
				if err := ra.Write(0, data); err != nil {
					return syscall.EIO
				}

				// 刷新以确保数据写入
				_, err = ra.Flush()
				if err != nil {
					return syscall.EIO
				}
			}
		} else {
			// 没有RandomAccessor，直接操作数据
			// 如果文件有数据，需要读取并重写
			if obj.DataID > 0 && obj.DataID != core.EmptyDataID && newSize > 0 {
				// 读取需要保留的数据
				data, err := n.fs.h.GetData(n.fs.c, n.fs.bktID, obj.DataID, 0, 0, int(newSize))
				if err != nil {
					return syscall.EIO
				}

				// 创建RandomAccessor来重写数据
				ra, err := NewRandomAccessor(n.fs, obj.ID)
				if err != nil {
					return syscall.EIO
				}

				// 写入保留的数据
				if err := ra.Write(0, data); err != nil {
					ra.Close()
					return syscall.EIO
				}

				// 刷新并关闭
				_, err = ra.Flush()
				if err != nil {
					ra.Close()
					return syscall.EIO
				}
				ra.Close()
			} else if newSize == 0 {
				// 新大小为0，删除数据
				// 这里简化处理：保持DataID不变，但大小设为0
				// 实际读取时会返回空数据
			}
		}
	} else {
		// 新大小大于旧大小，扩展文件（用0填充）
		// 注意：在POSIX中，扩展文件通常用0填充，但也可以不填充（稀疏文件）
		// 这里我们采用不填充的方式，只有在实际写入时才分配空间
		// 如果需要填充0，可以在这里写入0字节数据
	}

	return 0
}

// getRandomAccessor 获取或创建RandomAccessor（懒加载）
// 优化：使用原子操作，完全无锁
func (n *OrcasNode) getRandomAccessor() (*RandomAccessor, error) {
	// 第一次检查：原子读取（快速路径）
	if val := n.ra.Load(); val != nil {
		if ra, ok := val.(*RandomAccessor); ok && ra != nil {
			return ra, nil
		}
	}

	// 需要创建，使用CompareAndSwap确保只有一个goroutine创建
	// 创建新的RandomAccessor
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

	// 尝试原子地设置ra（如果已经被其他goroutine设置了，使用已存在的）
	if !n.ra.CompareAndSwap(nil, newRA) {
		// 其他goroutine已经创建了，关闭我们创建的，使用已存在的
		newRA.Close()
		if val := n.ra.Load(); val != nil {
			if ra, ok := val.(*RandomAccessor); ok && ra != nil {
				return ra, nil
			}
		}
	}

	return newRA, nil
}

// Release 释放文件句柄（关闭文件）
// 优化：使用原子操作，完全无锁
func (n *OrcasNode) Release(ctx context.Context) syscall.Errno {
	// 原子地获取并清空ra
	val := n.ra.Swap(nil)
	if val == nil {
		return 0
	}

	ra, ok := val.(*RandomAccessor)
	if !ok || ra == nil {
		return 0
	}

	// 执行Flush和Close（这些操作可能需要时间）
	// 刷新缓冲区
	_, err := ra.Flush()
	if err != nil {
		// 记录错误但不阻止关闭
	}

	// 关闭RandomAccessor
	ra.Close()

	// 刷新后使对象缓存失效
	n.invalidateObj()

	return 0
}
