package vfs

import (
	"bytes"
	"testing"
	"time"

	"github.com/orcastor/orcas/core"
)

func TestQuiesceWritesFlushesPendingRandomAccessor(t *testing.T) {
	testDir := testTmpDir("orcas_quiesce_writes_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileID, err := createTestFile(t, fs, bktID, "quiesce_test.bin")
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("failed to open random accessor: %v", err)
	}
	fs.registerRandomAccessor(fileID, ra)
	defer fs.unregisterRandomAccessor(fileID, ra)
	defer ra.Close()

	testData := bytes.Repeat([]byte("quiesce-write-"), 4096) // ~52KB
	if err := ra.Write(0, testData); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if !hasPendingWriteState(ra) {
		t.Fatalf("expected pending write state before quiesce")
	}

	if err := fs.QuiesceWrites(5 * time.Second); err != nil {
		t.Fatalf("quiesce failed: %v", err)
	}
	if hasPendingWriteState(ra) {
		t.Fatalf("pending write state still exists after quiesce")
	}

	lh, ok := fs.h.(*core.LocalHandler)
	if !ok {
		t.Fatalf("handler is not LocalHandler")
	}
	objs, err := lh.Get(fs.c, bktID, []int64{fileID})
	if err != nil {
		t.Fatalf("failed to read object metadata: %v", err)
	}
	if len(objs) == 0 || objs[0].DataID == 0 || objs[0].DataID == core.EmptyDataID {
		t.Fatalf("expected data to be flushed with valid DataID")
	}

	ra2, err := NewRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("failed to create verifier accessor: %v", err)
	}
	defer ra2.Close()

	readData, err := ra2.Read(0, len(testData))
	if err != nil {
		t.Fatalf("read back failed: %v", err)
	}
	if !bytes.Equal(readData, testData) {
		t.Fatalf("read back mismatch: got=%d bytes want=%d bytes", len(readData), len(testData))
	}
}

func TestSetEndecKeySafeDrainsWritesAndSwitchesKey(t *testing.T) {
	testDir := testTmpDir("orcas_set_key_safe_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileID, err := createTestFile(t, fs, bktID, "set_key_safe.bin")
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("failed to open random accessor: %v", err)
	}
	fs.registerRandomAccessor(fileID, ra)
	defer fs.unregisterRandomAccessor(fileID, ra)
	defer ra.Close()

	testData := bytes.Repeat([]byte("switch-key-"), 4096) // ~40KB
	if err := ra.Write(0, testData); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	const nextKey = "test-key-for-safe-switch"
	if err := fs.SetEndecKeySafe(nextKey, 5*time.Second); err != nil {
		t.Fatalf("SetEndecKeySafe failed: %v", err)
	}
	if got := fs.GetEndecKey(); got != nextKey {
		t.Fatalf("unexpected key after safe switch: got=%q want=%q", got, nextKey)
	}
	if hasPendingWriteState(ra) {
		t.Fatalf("pending write state still exists after safe key switch")
	}

	ra2, err := NewRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("failed to create verifier accessor: %v", err)
	}
	defer ra2.Close()

	readData, err := ra2.Read(0, len(testData))
	if err != nil {
		t.Fatalf("read back failed: %v", err)
	}
	if !bytes.Equal(readData, testData) {
		t.Fatalf("read back mismatch after safe key switch")
	}
}

func TestHasPendingWriteStateIgnoresCleanJournal(t *testing.T) {
	ra := &RandomAccessor{}
	if hasPendingWriteState(ra) {
		t.Fatalf("unexpected pending state for empty accessor")
	}

	ra.journalMu.Lock()
	ra.journal = &Journal{} // non-nil but clean
	ra.journalMu.Unlock()
	if hasPendingWriteState(ra) {
		t.Fatalf("clean journal should not be treated as pending write state")
	}

	ra.journalMu.Lock()
	ra.journal.isDirty = 1
	ra.journalMu.Unlock()
	if !hasPendingWriteState(ra) {
		t.Fatalf("dirty journal should be treated as pending write state")
	}
}

func TestQuiesceWritesSkipsIdleAccessorFlushErrors(t *testing.T) {
	fs := &OrcasFS{}

	// fs=nil inside accessor would make ForceFlush fail if QuiesceWrites incorrectly flushes idle accessors.
	idleRA := &RandomAccessor{fileID: 1001}
	fs.raRegistry.Store(idleRA.fileID, idleRA)
	defer fs.raRegistry.Delete(idleRA.fileID)

	if err := fs.QuiesceWrites(200 * time.Millisecond); err != nil {
		t.Fatalf("quiesce should skip idle accessor flush and succeed, got: %v", err)
	}
}

func TestGetEndecKeyForFSReflectsDirectConfigAssignment(t *testing.T) {
	testDir := testTmpDir("orcas_endec_key_compat_test")
	defer cleanupTestDir(t, testDir)

	fs, _ := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fs.SetEndecKey("key-from-setter")
	if got := getEndecKeyForFS(fs); got != "key-from-setter" {
		t.Fatalf("unexpected key from setter path: got=%q", got)
	}

	// Keep compatibility with direct embedded config field assignment.
	fs.EndecKey = "key-from-direct-field"
	if got := getEndecKeyForFS(fs); got != "key-from-direct-field" {
		t.Fatalf("unexpected key from direct field assignment: got=%q", got)
	}
}
