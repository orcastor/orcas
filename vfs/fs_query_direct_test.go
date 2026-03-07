package vfs

import (
	"testing"

	"github.com/orcastor/orcas/core"
)

func TestQueryFileByNameDirectlyFindsActiveFile(t *testing.T) {
	testDir := testTmpDir("orcas_query_file_direct_active")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileID, err := createTestFile(t, fs, bktID, "direct_active.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	gotID, gotObj := fs.root.queryFileByNameDirectly(bktID, "direct_active.txt")
	if gotID != fileID {
		t.Fatalf("unexpected fileID: got=%d want=%d", gotID, fileID)
	}
	if gotObj == nil {
		t.Fatalf("expected object, got nil")
	}
	if gotObj.Type != core.OBJ_TYPE_FILE {
		t.Fatalf("unexpected object type: got=%d", gotObj.Type)
	}
	if gotObj.PID != bktID {
		t.Fatalf("expected active pid=%d, got=%d", bktID, gotObj.PID)
	}
}

func TestQueryFileByNameDirectlyFindsDeletedFile(t *testing.T) {
	testDir := testTmpDir("orcas_query_file_direct_deleted")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileID, err := createTestFile(t, fs, bktID, "direct_deleted.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := fs.h.Recycle(fs.c, bktID, fileID); err != nil {
		t.Fatalf("failed to recycle file: %v", err)
	}

	gotID, gotObj := fs.root.queryFileByNameDirectly(bktID, "direct_deleted.txt")
	if gotID != fileID {
		t.Fatalf("unexpected fileID for deleted object: got=%d want=%d", gotID, fileID)
	}
	if gotObj == nil {
		t.Fatalf("expected deleted object, got nil")
	}
	if gotObj.PID >= 0 {
		t.Fatalf("expected deleted pid<0, got=%d", gotObj.PID)
	}
}

func TestQueryFileByNameDirectlyPrefersActiveOverDeleted(t *testing.T) {
	testDir := testTmpDir("orcas_query_file_direct_active_preferred")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	const fileName = "direct_prefer_active.txt"

	oldID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("failed to create old file: %v", err)
	}
	if err := fs.h.Recycle(fs.c, bktID, oldID); err != nil {
		t.Fatalf("failed to recycle old file: %v", err)
	}

	newID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("failed to create new active file: %v", err)
	}

	gotID, gotObj := fs.root.queryFileByNameDirectly(bktID, fileName)
	if gotID != newID {
		t.Fatalf("expected active file ID=%d, got=%d (oldID=%d)", newID, gotID, oldID)
	}
	if gotObj == nil {
		t.Fatalf("expected active object, got nil")
	}
	if gotObj.PID != bktID {
		t.Fatalf("expected active pid=%d, got=%d", bktID, gotObj.PID)
	}
}

func TestQueryActiveObjectByNameDirectlyFindsDirectory(t *testing.T) {
	testDir := testTmpDir("orcas_query_active_object_dir")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	dirObj := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   bktID,
		Type:  core.OBJ_TYPE_DIR,
		Name:  "active_dir",
		MTime: core.Now(),
	}
	if _, err := fs.h.Put(fs.c, bktID, []*core.ObjectInfo{dirObj}); err != nil {
		t.Fatalf("failed to create directory object: %v", err)
	}

	gotID, gotObj := fs.root.queryActiveObjectByNameDirectly(bktID, "active_dir")
	if gotID != dirObj.ID {
		t.Fatalf("unexpected directory ID: got=%d want=%d", gotID, dirObj.ID)
	}
	if gotObj == nil {
		t.Fatalf("expected directory object, got nil")
	}
	if gotObj.Type != core.OBJ_TYPE_DIR {
		t.Fatalf("expected directory type=%d, got=%d", core.OBJ_TYPE_DIR, gotObj.Type)
	}
}

func TestQueryActiveObjectByNameDirectlyIgnoresDeleted(t *testing.T) {
	testDir := testTmpDir("orcas_query_active_object_ignore_deleted")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileID, err := createTestFile(t, fs, bktID, "deleted_only.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := fs.h.Recycle(fs.c, bktID, fileID); err != nil {
		t.Fatalf("failed to recycle file: %v", err)
	}

	gotID, gotObj := fs.root.queryActiveObjectByNameDirectly(bktID, "deleted_only.txt")
	if gotID != 0 || gotObj != nil {
		t.Fatalf("expected no active object, got id=%d obj=%v", gotID, gotObj)
	}
}

func TestQueryActiveObjectsByNamesDirectly(t *testing.T) {
	testDir := testTmpDir("orcas_query_active_objects_by_names")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileID, err := createTestFile(t, fs, bktID, "batch_a.txt")
	if err != nil {
		t.Fatalf("failed to create file A: %v", err)
	}

	dirObj := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   bktID,
		Type:  core.OBJ_TYPE_DIR,
		Name:  "batch_dir",
		MTime: core.Now(),
	}
	if _, err := fs.h.Put(fs.c, bktID, []*core.ObjectInfo{dirObj}); err != nil {
		t.Fatalf("failed to create directory object: %v", err)
	}

	m := fs.root.queryActiveObjectsByNamesDirectly(bktID, []string{"batch_a.txt", "batch_dir", "batch_a.txt", "missing.txt"})
	if len(m) != 2 {
		t.Fatalf("unexpected result size: got=%d want=2", len(m))
	}
	if got := m["batch_a.txt"]; got == nil || got.ID != fileID {
		t.Fatalf("unexpected file A result: %#v", got)
	}
	if got := m["batch_dir"]; got == nil || got.ID != dirObj.ID || got.Type != core.OBJ_TYPE_DIR {
		t.Fatalf("unexpected dir result: %#v", got)
	}
	if got := m["missing.txt"]; got != nil {
		t.Fatalf("expected missing name to be absent, got: %#v", got)
	}
}
