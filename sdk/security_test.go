package sdk

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orcastor/orcas/core"
)

type downloadFixture struct {
	core.Handler
	object   *core.ObjectInfo
	children []*core.ObjectInfo
}

func (f *downloadFixture) Get(core.Ctx, int64, []int64) ([]*core.ObjectInfo, error) {
	return []*core.ObjectInfo{f.object}, nil
}
func (f *downloadFixture) List(_ core.Ctx, _ int64, _ int64, opts core.ListOptions) ([]*core.ObjectInfo, int64, string, error) {
	if opts.Delim != "" {
		return nil, 0, "", nil
	}
	return f.children, int64(len(f.children)), "next", nil
}

func TestSecurityDownloadTraversal(t *testing.T) {
	for _, name := range []string{"..", ".", "../outside", "nested/../../outside", "/absolute", `..\outside`, `C:\outside`, "file:stream", "bad\x00name", ""} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			h := &downloadFixture{object: &core.ObjectInfo{ID: 1, Name: "download", Type: core.OBJ_TYPE_DIR},
				children: []*core.ObjectInfo{{ID: 2, Name: name, Type: core.OBJ_TYPE_FILE, DataID: core.EmptyDataID}}}
			sdk := &OrcasSDKImpl{h: h}
			if err := sdk.Download(context.Background(), 1, 1, root); err == nil {
				t.Fatal("unsafe child object name accepted")
			}
			h.object.Name = name
			h.children = nil
			if err := sdk.Download(context.Background(), 1, 1, root); err == nil {
				t.Fatal("unsafe top-level directory name accepted")
			}
		})
	}
}

func TestSecurityDownloadSymlinks(t *testing.T) {
	for _, directory := range []bool{false, true} {
		root := t.TempDir()
		outside := t.TempDir()
		victim := filepath.Join(outside, "victim")
		if err := os.WriteFile(victim, []byte("preserve me"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(root, "download"), 0o700); err != nil {
			t.Fatal(err)
		}
		target := victim
		link := filepath.Join(root, "download", "file")
		if directory {
			target = outside
			link = filepath.Join(root, "download")
			if err := os.Remove(link); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		h := &downloadFixture{object: &core.ObjectInfo{ID: 1, Name: "download", Type: core.OBJ_TYPE_DIR},
			children: []*core.ObjectInfo{{ID: 2, Name: "file", Type: core.OBJ_TYPE_FILE, DataID: core.EmptyDataID}}}
		sdk := &OrcasSDKImpl{h: h}
		if err := sdk.Download(context.Background(), 1, 1, root); err == nil {
			t.Fatal("download followed symlink outside destination")
		}
		data, err := os.ReadFile(victim)
		if err != nil || string(data) != "preserve me" {
			t.Fatal("download overwrote a file outside destination")
		}
	}
}

func TestSecurityDownloadSafeFiles(t *testing.T) {
	root := t.TempDir()
	h := &downloadFixture{object: &core.ObjectInfo{ID: 1, Name: "安全目录", Type: core.OBJ_TYPE_DIR},
		children: []*core.ObjectInfo{{ID: 2, Name: "file.txt", Type: core.OBJ_TYPE_FILE, DataID: core.EmptyDataID}}}
	sdk := &OrcasSDKImpl{h: h}
	if err := sdk.Download(context.Background(), 1, 1, root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "安全目录", "file.txt")); err != nil {
		t.Fatal(err)
	}
	// The explicitly selected single-file destination is also confined.
	h.object = h.children[0]
	outside := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(outside, []byte("preserve me"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if err := sdk.Download(context.Background(), 1, 2, link); err == nil {
		t.Fatal("single-file download followed an escaping symlink")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "preserve me" {
		t.Fatal("single-file download overwrote external target")
	}
}
