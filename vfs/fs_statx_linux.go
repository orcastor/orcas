//go:build linux
// +build linux

package vfs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

var _ fs.NodeStatxer = (*OrcasNode)(nil)

// Statx reports the same metadata as Getattr through Linux's statx interface.
func (n *OrcasNode) Statx(ctx context.Context, f fs.FileHandle, flags uint32, mask uint32, out *fuse.StatxOut) syscall.Errno {
	DebugLog("[VFS Statx] Entry: objID=%d, FileHandle=%v, flags=0x%x, mask=0x%x", n.objID, f, flags, mask)

	var attr fuse.AttrOut
	if errno := n.Getattr(ctx, f, &attr); errno != 0 {
		DebugLog("[VFS Statx] ERROR: Getattr failed: objID=%d, errno=%d", n.objID, errno)
		return errno
	}

	out.Mask = unix.STATX_BASIC_STATS
	out.Blksize = attr.Blksize
	if out.Blksize == 0 {
		out.Blksize = 4096
	}
	out.Nlink = attr.Nlink
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Mode = uint16(attr.Mode)
	out.Ino = attr.Ino
	if out.Ino == 0 {
		out.Ino = uint64(n.objID)
	}
	out.Size = attr.Size
	out.Blocks = attr.Blocks
	if out.Blocks == 0 && out.Size > 0 {
		out.Blocks = ((out.Size + 4095) / 4096) * 8
	}
	out.Atime = fuse.SxTime{Sec: attr.Atime, Nsec: attr.Atimensec}
	out.Mtime = fuse.SxTime{Sec: attr.Mtime, Nsec: attr.Mtimensec}
	out.Ctime = fuse.SxTime{Sec: attr.Ctime, Nsec: attr.Ctimensec}
	out.SetTimeout(attr.Timeout())

	return 0
}
