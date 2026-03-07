package vfs

import "testing"

func TestCleanupDeletedTargetTombstoneBeforeRenameDeletesDeleted(t *testing.T) {
	testDir := testTmpDir("orcas_cleanup_tombstone_deleted")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	tombID, err := createTestFile(t, fs, bktID, "cleanup_target.txt")
	if err != nil {
		t.Fatalf("failed to create tombstone source file: %v", err)
	}
	if err := fs.h.Recycle(fs.c, bktID, tombID); err != nil {
		t.Fatalf("failed to recycle tombstone source file: %v", err)
	}

	fs.root.cleanupDeletedTargetTombstoneBeforeRename(bktID, 0, "cleanup_target.txt")

	objs, err := fs.h.Get(fs.c, bktID, []int64{tombID})
	if err == nil && len(objs) > 0 {
		t.Fatalf("expected tombstone object to be permanently deleted, got len=%d", len(objs))
	}

	gotID, _ := fs.root.queryFileByNameDirectly(bktID, "cleanup_target.txt")
	if gotID != 0 {
		t.Fatalf("expected no object with target name after cleanup, got id=%d", gotID)
	}
}

func TestCleanupDeletedTargetTombstoneBeforeRenameKeepsActive(t *testing.T) {
	testDir := testTmpDir("orcas_cleanup_tombstone_active")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	activeID, err := createTestFile(t, fs, bktID, "keep_active.txt")
	if err != nil {
		t.Fatalf("failed to create active file: %v", err)
	}

	fs.root.cleanupDeletedTargetTombstoneBeforeRename(bktID, 0, "keep_active.txt")

	objs, err := fs.h.Get(fs.c, bktID, []int64{activeID})
	if err != nil || len(objs) == 0 {
		t.Fatalf("expected active object to remain, err=%v len=%d", err, len(objs))
	}
}

func TestCleanupDeletedTargetTombstoneBeforeRenameSkipsSourceID(t *testing.T) {
	testDir := testTmpDir("orcas_cleanup_tombstone_skip_source")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	tombID, err := createTestFile(t, fs, bktID, "same_as_source.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := fs.h.Recycle(fs.c, bktID, tombID); err != nil {
		t.Fatalf("failed to recycle file: %v", err)
	}

	fs.root.cleanupDeletedTargetTombstoneBeforeRename(bktID, tombID, "same_as_source.txt")

	objs, err := fs.h.Get(fs.c, bktID, []int64{tombID})
	if err != nil || len(objs) == 0 {
		t.Fatalf("expected object to remain when conflictID==sourceID, err=%v len=%d", err, len(objs))
	}
}
