//go:build !windows
// +build !windows

package vfs

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/orcastor/orcas/core"
)

type metadataTestHandler struct {
	core.Handler
	objects  []*core.ObjectInfo
	children []*core.ObjectInfo
	err      error
}

func (h *metadataTestHandler) Get(_ core.Ctx, _ int64, ids []int64) ([]*core.ObjectInfo, error) {
	if h.err != nil {
		return nil, h.err
	}

	objects := make([]*core.ObjectInfo, 0, len(ids))
	for _, id := range ids {
		for _, object := range h.objects {
			if object.ID == id {
				objects = append(objects, object)
				break
			}
		}
	}
	return objects, nil
}

func (h *metadataTestHandler) List(core.Ctx, int64, int64, core.ListOptions) ([]*core.ObjectInfo, int64, string, error) {
	if h.err != nil {
		return nil, 0, "", h.err
	}
	return h.children, int64(len(h.children)), "", nil
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

func TestLookupReturnsValidLinkCount(t *testing.T) {
	tests := []struct {
		name       string
		objectType int
		wantMode   uint32
	}{
		{name: "directory", objectType: core.OBJ_TYPE_DIR, wantMode: syscall.S_IFDIR},
		{name: "file", objectType: core.OBJ_TYPE_FILE, wantMode: syscall.S_IFREG},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentID := int64(910000 + i*10)
			childID := parentID + 1
			child := &core.ObjectInfo{
				ID:    childID,
				PID:   parentID,
				Name:  tt.name,
				Type:  tt.objectType,
				MTime: 123,
			}
			handler := &metadataTestHandler{
				objects:  []*core.ObjectInfo{child},
				children: []*core.ObjectInfo{child},
			}
			parent := newMetadataTestNode(parentID, handler)
			parent.obj.Store(&core.ObjectInfo{ID: parentID, Type: core.OBJ_TYPE_DIR})
			fs.NewNodeFS(parent, &fs.Options{})

			defer fileObjCache.Del(childID)
			defer parent.invalidateDirListCache(parentID)

			var out fuse.EntryOut
			if _, errno := parent.Lookup(context.Background(), tt.name, &out); errno != 0 {
				t.Fatalf("Lookup() errno = %v, want success", errno)
			}
			if got := out.Mode & syscall.S_IFMT; got != tt.wantMode {
				t.Fatalf("Lookup() type = %#o, want %#o", got, tt.wantMode)
			}
			if out.Nlink == 0 {
				t.Fatal("Lookup() Nlink = 0, want a valid link count")
			}
		})
	}
}
