//go:build !windows
// +build !windows

package vfs

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/orcastor/orcas/core"
)

type metadataTestHandler struct {
	core.Handler
	objects []*core.ObjectInfo
	err     error
}

func (h *metadataTestHandler) Get(core.Ctx, int64, []int64) ([]*core.ObjectInfo, error) {
	return h.objects, h.err
}

func newMetadataTestNode(id int64, h core.Handler) *OrcasNode {
	fileObjCache.Del(id)
	return &OrcasNode{
		fs: &OrcasFS{
			h:     h,
			c:     core.Ctx(context.Background()),
			bktID: 1,
		},
		objID: id,
	}
}

func TestGetattrDoesNotMaskMissingObjectWhenKeyCallbackConfigured(t *testing.T) {
	node := newMetadataTestNode(900001, &metadataTestHandler{})
	node.fs.OnKeyFileContent = func(string, string) syscall.Errno { return 0 }

	if errno := node.Getattr(context.Background(), nil, &fuse.AttrOut{}); errno != syscall.ENOENT {
		t.Fatalf("Getattr() errno = %v, want ENOENT", errno)
	}
}

func TestGetattrReturnsEIOForMetadataFailure(t *testing.T) {
	node := newMetadataTestNode(900002, &metadataTestHandler{err: errors.New("database unavailable")})
	node.fs.OnKeyFileContent = func(string, string) syscall.Errno { return 0 }

	if errno := node.Getattr(context.Background(), nil, &fuse.AttrOut{}); errno != syscall.EIO {
		t.Fatalf("Getattr() errno = %v, want EIO", errno)
	}
}

func TestGetattrReportsDirectoryType(t *testing.T) {
	const objectID = int64(900003)
	node := newMetadataTestNode(objectID, &metadataTestHandler{
		objects: []*core.ObjectInfo{{
			ID:    objectID,
			Type:  core.OBJ_TYPE_DIR,
			MTime: 123,
		}},
	})
	defer fileObjCache.Del(objectID)

	var out fuse.AttrOut
	if errno := node.Getattr(context.Background(), nil, &out); errno != 0 {
		t.Fatalf("Getattr() errno = %v, want success", errno)
	}
	if got := out.Mode & syscall.S_IFMT; got != syscall.S_IFDIR {
		t.Fatalf("Getattr() type = %#o, want S_IFDIR", got)
	}
}
