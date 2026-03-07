package vfs

import (
	"context"
	"fmt"
	"syscall"
	"testing"

	"github.com/orcastor/orcas/core"
)

func TestIsUniqueConstraintError(t *testing.T) {
	if !isUniqueConstraintError(core.ERR_DUP_KEY) {
		t.Fatalf("expected ERR_DUP_KEY to be recognized")
	}
	if !isUniqueConstraintError(fmt.Errorf("wrapped: %w", core.ERR_DUP_KEY)) {
		t.Fatalf("expected wrapped ERR_DUP_KEY to be recognized")
	}
	if !isUniqueConstraintError(fmt.Errorf("sqlite: UNIQUE constraint failed: obj.n")) {
		t.Fatalf("expected sqlite unique text to be recognized")
	}
	if isUniqueConstraintError(fmt.Errorf("io timeout")) {
		t.Fatalf("did not expect non-unique error to be recognized")
	}
}

func TestWaitForRenameTargetSlotFreeDeletesTombstone(t *testing.T) {
	testDir := testTmpDir("orcas_wait_slot_delete_tombstone")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	const name = "slot_tombstone.txt"
	tombID, err := createTestFile(t, fs, bktID, name)
	if err != nil {
		t.Fatalf("failed to create tombstone file: %v", err)
	}
	if err := fs.h.Recycle(fs.c, bktID, tombID); err != nil {
		t.Fatalf("failed to recycle tombstone file: %v", err)
	}

	ok := fs.root.waitForRenameTargetSlotFree(bktID, 0, name, 1)
	if !ok {
		t.Fatalf("expected slot to become free after tombstone cleanup")
	}

	if gotID, _ := fs.root.queryFileByNameDirectly(bktID, name); gotID != 0 {
		t.Fatalf("expected no conflicting object after cleanup, got=%d", gotID)
	}
	if objs, getErr := fs.h.Get(fs.c, bktID, []int64{tombID}); getErr == nil && len(objs) > 0 {
		t.Fatalf("expected tombstone object to be deleted, got len=%d", len(objs))
	}
}

func TestWaitForRenameTargetSlotFreeReturnsFalseForActiveConflict(t *testing.T) {
	testDir := testTmpDir("orcas_wait_slot_active_conflict")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	const name = "slot_active.txt"
	conflictID, err := createTestFile(t, fs, bktID, name)
	if err != nil {
		t.Fatalf("failed to create conflicting file: %v", err)
	}

	ok := fs.root.waitForRenameTargetSlotFree(bktID, 0, name, 2)
	if ok {
		t.Fatalf("expected slot wait to fail while active conflict exists (conflictID=%d)", conflictID)
	}
}

func TestWaitForRenameTargetSlotFreeSkipsSourceObject(t *testing.T) {
	testDir := testTmpDir("orcas_wait_slot_skip_source")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	const name = "slot_source.txt"
	sourceID, err := createTestFile(t, fs, bktID, name)
	if err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	ok := fs.root.waitForRenameTargetSlotFree(bktID, sourceID, name, 1)
	if !ok {
		t.Fatalf("expected source object to be ignored as conflict")
	}
}

func TestCreateVersionFromTargetWithRetry(t *testing.T) {
	testDir := testTmpDir("orcas_create_version_retry")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	targetID, err := createTestFile(t, fs, bktID, "version_target.txt")
	if err != nil {
		t.Fatalf("failed to create target file: %v", err)
	}

	if err := fs.root.createVersionFromTargetWithRetry(targetID); err != nil {
		t.Fatalf("expected version creation success, got: %v", err)
	}

	children, _, _, err := fs.h.List(fs.c, bktID, targetID, core.ListOptions{Count: 32})
	if err != nil {
		t.Fatalf("failed to list target versions: %v", err)
	}
	foundVersion := false
	for _, child := range children {
		if child.Type == core.OBJ_TYPE_VERSION {
			foundVersion = true
			break
		}
	}
	if !foundVersion {
		t.Fatalf("expected at least one version under target file after helper call")
	}
}

func TestCreateVersionFromTargetWithRetryReturnsErrorForMissingFile(t *testing.T) {
	testDir := testTmpDir("orcas_create_version_retry_missing")
	defer cleanupTestDir(t, testDir)

	fs, _ := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	if err := fs.root.createVersionFromTargetWithRetry(core.NewID()); err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestBuildRenameCandidateNames(t *testing.T) {
	cases := []struct {
		name string
		want []string
	}{
		{name: "doc.tmp", want: []string{"doc.tmp", "doc"}},
		{name: "doc.TMP", want: []string{"doc.TMP", "doc"}},
		{name: "plain.txt", want: []string{"plain.txt"}},
		{name: "", want: []string{""}},
	}

	for _, tc := range cases {
		got := buildRenameCandidateNames(tc.name)
		if len(got) != len(tc.want) {
			t.Fatalf("name=%q candidate length mismatch: got=%v want=%v", tc.name, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("name=%q candidate[%d] mismatch: got=%q want=%q", tc.name, i, got[i], tc.want[i])
			}
		}
	}
}

func TestFindSourceInRARegistryForRename(t *testing.T) {
	const parentID = int64(42)

	fs := &OrcasFS{}
	n := &OrcasNode{fs: fs}

	exactObj := &core.ObjectInfo{ID: 1001, PID: parentID, Name: "file.tmp"}
	exactRA := &RandomAccessor{fileID: exactObj.ID, fileObjKey: exactObj.ID}
	exactRA.fileObj.Store(exactObj)
	fs.raRegistry.Store(exactObj.ID, exactRA)

	fallbackObj := &core.ObjectInfo{ID: 1002, PID: parentID, Name: "file"}
	fallbackRA := &RandomAccessor{fileID: fallbackObj.ID, fileObjKey: fallbackObj.ID}
	fallbackRA.fileObj.Store(fallbackObj)
	fs.raRegistry.Store(fallbackObj.ID, fallbackRA)

	otherObj := &core.ObjectInfo{ID: 1003, PID: parentID + 1, Name: "file.tmp"}
	otherRA := &RandomAccessor{fileID: otherObj.ID, fileObjKey: otherObj.ID}
	otherRA.fileObj.Store(otherObj)
	fs.raRegistry.Store(otherObj.ID, otherRA)

	gotID, gotObj := n.findSourceInRARegistryForRename(parentID, "file.tmp", "file")
	if gotID != exactObj.ID || gotObj == nil || gotObj.ID != exactObj.ID {
		t.Fatalf("expected exact match id=%d, got id=%d obj=%v", exactObj.ID, gotID, gotObj)
	}

	gotID, gotObj = n.findSourceInRARegistryForRename(parentID, "missing.tmp", "file")
	if gotID != fallbackObj.ID || gotObj == nil || gotObj.ID != fallbackObj.ID {
		t.Fatalf("expected fallback match id=%d, got id=%d obj=%v", fallbackObj.ID, gotID, gotObj)
	}
}

func TestFindSourceInRARegistryForRenameUsesFileObjCacheWithoutDBFetch(t *testing.T) {
	const parentID = int64(77)

	fs := &OrcasFS{}
	n := &OrcasNode{fs: fs}

	obj := &core.ObjectInfo{ID: 2001, PID: parentID, Name: "cache-only.tmp"}
	ra := &RandomAccessor{fileID: obj.ID, fileObjKey: obj.ID}
	// Deliberately do not seed ra.fileObj atomic cache.
	fileObjCache.Put(obj.ID, obj)
	fs.raRegistry.Store(obj.ID, ra)

	gotID, gotObj := n.findSourceInRARegistryForRename(parentID, "cache-only.tmp", "")
	if gotID != obj.ID || gotObj == nil || gotObj.ID != obj.ID {
		t.Fatalf("expected fileObjCache-backed match id=%d, got id=%d obj=%v", obj.ID, gotID, gotObj)
	}
}

func TestFindActiveObjectByNamesFromDirCache(t *testing.T) {
	testDir := testTmpDir("orcas_find_from_dir_cache")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	exactID, err := createTestFile(t, fs, bktID, "cache_a.tmp")
	if err != nil {
		t.Fatalf("failed to create exact file: %v", err)
	}
	fallbackID, err := createTestFile(t, fs, bktID, "cache_a")
	if err != nil {
		t.Fatalf("failed to create fallback file: %v", err)
	}

	children, listErr := fs.listAllObjects(bktID, core.ListOptions{})
	if listErr != nil {
		t.Fatalf("failed to list children: %v", listErr)
	}
	cacheKey := fs.root.getDirListCacheKey(bktID)
	dirListCache.Put(cacheKey, children)

	cachedFallback := &core.ObjectInfo{ID: fallbackID, PID: bktID, Type: core.OBJ_TYPE_FILE, Name: "cache_a", Size: 1234}
	fileObjCache.Put(fallbackID, cachedFallback)

	gotID, gotObj := fs.root.findActiveObjectByNamesFromDirCache(bktID, []string{"cache_a.tmp", "cache_a"})
	if gotID != exactID || gotObj == nil || gotObj.ID != exactID {
		t.Fatalf("expected exact cached lookup id=%d, got id=%d obj=%v", exactID, gotID, gotObj)
	}

	gotID, gotObj = fs.root.findActiveObjectByNamesFromDirCache(bktID, []string{"missing", "cache_a"})
	if gotID != fallbackID || gotObj == nil || gotObj.ID != fallbackID {
		t.Fatalf("expected fallback cached lookup id=%d, got id=%d obj=%v", fallbackID, gotID, gotObj)
	}
	if gotObj.Size != 1234 {
		t.Fatalf("expected fileObjCache override size=1234, got=%d", gotObj.Size)
	}

	gotID, gotObj = fs.root.findActiveObjectByNamesFromDirCache(bktID, []string{"missing_a", "missing_b"})
	if gotID != 0 || gotObj != nil {
		t.Fatalf("expected cache miss, got id=%d obj=%v", gotID, gotObj)
	}
}

func TestRenameFailureDoesNotPreMutateSourceCache(t *testing.T) {
	testDir := testTmpDir("orcas_rename_cache_no_pre_mutate")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	sourceID, err := createTestFile(t, fs, bktID, "source-name.txt")
	if err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Create target directory with the same name used for rename destination.
	targetDir := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   bktID,
		Type:  core.OBJ_TYPE_DIR,
		Name:  "target-dir",
		MTime: core.Now(),
	}
	if _, err := fs.h.Put(fs.c, bktID, []*core.ObjectInfo{targetDir}); err != nil {
		t.Fatalf("failed to create target directory: %v", err)
	}

	// Seed cache with source object so rename failure can be checked for accidental pre-mutation.
	sourceObjs, getErr := fs.h.Get(fs.c, bktID, []int64{sourceID})
	if getErr != nil || len(sourceObjs) == 0 {
		t.Fatalf("failed to get source object for cache seed: err=%v len=%d", getErr, len(sourceObjs))
	}
	sourceObj := sourceObjs[0]
	fileObjCache.Put(sourceID, sourceObj)

	errno := fs.root.Rename(context.Background(), "source-name.txt", fs.root, "target-dir", 0)
	if errno != syscall.EISDIR {
		t.Fatalf("expected EISDIR, got errno=%d", errno)
	}

	cached, ok := fileObjCache.Get(sourceID)
	if !ok {
		t.Fatalf("expected source cache entry to remain")
	}
	cachedObj, ok := cached.(*core.ObjectInfo)
	if !ok || cachedObj == nil {
		t.Fatalf("expected cached object type, got=%T", cached)
	}
	if cachedObj.Name != "source-name.txt" {
		t.Fatalf("source cache name mutated on failed rename: got=%s", cachedObj.Name)
	}
	if cachedObj.PID != bktID {
		t.Fatalf("source cache pid mutated on failed rename: got=%d want=%d", cachedObj.PID, bktID)
	}
}
