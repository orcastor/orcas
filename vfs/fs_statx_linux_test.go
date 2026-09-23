//go:build linux
// +build linux

package vfs

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/orcastor/orcas/core"
	"golang.org/x/sys/unix"
)

func TestStatxReportsObjectType(t *testing.T) {
	tests := []struct {
		name     string
		obj      *core.ObjectInfo
		wantType uint16
	}{
		{
			name:     "directory",
			obj:      &core.ObjectInfo{ID: 910001, Type: core.OBJ_TYPE_DIR, MTime: 123},
			wantType: syscall.S_IFDIR,
		},
		{
			name:     "file",
			obj:      &core.ObjectInfo{ID: 910002, Type: core.OBJ_TYPE_FILE, Size: 17, MTime: 456},
			wantType: syscall.S_IFREG,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &OrcasNode{fs: &OrcasFS{}, objID: tt.obj.ID}
			node.obj.Store(tt.obj)

			var out fuse.StatxOut
			if errno := node.Statx(context.Background(), nil, 0, unix.STATX_BASIC_STATS, &out); errno != 0 {
				t.Fatalf("Statx() errno = %v, want success", errno)
			}
			if out.Mask&unix.STATX_TYPE == 0 || out.Mask&unix.STATX_MODE == 0 {
				t.Fatalf("Statx() mask = %#x, want type and mode", out.Mask)
			}
			if got := out.Mode & syscall.S_IFMT; got != tt.wantType {
				t.Fatalf("Statx() type = %#o, want %#o", got, tt.wantType)
			}
			if out.Ino != uint64(tt.obj.ID) {
				t.Fatalf("Statx() inode = %d, want %d", out.Ino, tt.obj.ID)
			}
			if out.Size != uint64(tt.obj.Size) {
				t.Fatalf("Statx() size = %d, want %d", out.Size, tt.obj.Size)
			}
			if out.Mtime.Sec != uint64(tt.obj.MTime) {
				t.Fatalf("Statx() mtime = %d, want %d", out.Mtime.Sec, tt.obj.MTime)
			}
		})
	}
}
