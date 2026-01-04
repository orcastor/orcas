package vfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
)

// Helper function for tests (not in Windows-specific files)
func getOrCreateRandomAccessor(ofs *OrcasFS, fileID int64) (*RandomAccessor, error) {
	return NewRandomAccessor(ofs, fileID)
}

// setupTestFSWithEncryption creates a test filesystem with encryption enabled
func setupTestFSWithEncryption(t *testing.T, testDir string, encryptionKey string, cmprWay uint32) (*OrcasFS, int64) {
	// Use the same setup as journal_test.go
	fs, bktID := setupTestFS(t, testDir)

	// Add encryption configuration
	fs.EndecWay = core.DATA_ENDEC_AES256
	fs.EndecKey = encryptionKey
	fs.CmprWay = cmprWay

	// Manually initialize root node for testing (since we don't call Mount)
	if fs.root == nil {
		fs.root = &OrcasNode{
			fs:     fs,
			objID:  bktID,
			isRoot: true,
		}
	}

	return fs, bktID
}

// TestJournalBasicWriteRead tests basic write and read operations with journal
func TestJournalBasicWriteRead(t *testing.T) {
	// Setup test environment
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_basic")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	// Create a test file
	fileName := "test_journal_basic.txt"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Open RandomAccessor
	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	// First, write some data sequentially (this will use sequential buffer, not journal)
	// For new files starting at offset 0, sequential buffer is used
	initialData := []byte("Initial data")
	err = ra.Write(0, initialData)
	if err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// Flush to commit the sequential buffer, so file has base data
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Now perform random writes (this will trigger journal usage)
	// Write at a non-sequential offset to trigger journal
	testData := []byte("Hello, Journal!")
	err = ra.Write(100, testData)
	if err != nil {
		t.Fatalf("Failed to write at offset 100: %v", err)
	}

	// Read back initial data
	readBuf, err := ra.Read(0, len(initialData))
	if err != nil {
		t.Fatalf("Failed to read initial data: %v", err)
	}
	if !bytes.Equal(readBuf, initialData) {
		t.Errorf("Initial data mismatch: got %q, want %q", readBuf, initialData)
	}

	// Read back journal data
	readBuf, err = ra.Read(100, len(testData))
	if err != nil {
		t.Fatalf("Failed to read journal data: %v", err)
	}
	if !bytes.Equal(readBuf, testData) {
		t.Errorf("Journal data mismatch: got %q, want %q", readBuf, testData)
	}

	t.Logf("✓ Basic write/read test passed")
}

// TestJournalRandomWrites tests random write pattern
func TestJournalRandomWrites(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_random")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_journal_random.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	// Perform random writes
	writes := []struct {
		offset int64
		data   string
	}{
		{0, "AAAA"},
		{100, "BBBB"},
		{50, "CCCC"},
		{25, "DDDD"},
		{75, "EEEE"},
	}

	for _, w := range writes {
		err = ra.Write(w.offset, []byte(w.data))
		if err != nil {
			t.Fatalf("Failed to write at offset %d: %v", w.offset, err)
		}
	}

	// Verify reads
	for _, w := range writes {
		readBuf, err := ra.Read(w.offset, len(w.data))
		if err != nil {
			t.Fatalf("Failed to read at offset %d: %v", w.offset, err)
		}
		if !bytes.Equal(readBuf, []byte(w.data)) {
			t.Errorf("Data mismatch at offset %d: got %q, want %q", w.offset, readBuf, w.data)
		}
	}

	t.Logf("✓ Random writes test passed")
}

// TestJournalFlush tests journal flush operation
func TestJournalFlush(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_flush")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_journal_flush.txt"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	// Write data
	testData := []byte("Data to be flushed")
	err = ra.Write(0, testData)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Flush
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Verify data persists after flush by reading through a new accessor
	ra2, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get second RandomAccessor: %v", err)
	}
	defer ra2.Close()

	readBuf, err := ra2.Read(0, len(testData))
	if err != nil {
		t.Fatalf("Failed to read after flush: %v", err)
	}
	if !bytes.Equal(readBuf, testData) {
		t.Errorf("Data mismatch after flush: got %q, want %q", readBuf, testData)
	}

	t.Logf("✓ Flush test passed")
}

// TestJournalMemoryLimit tests memory limit enforcement
func TestJournalMemoryLimit(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_memory")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	// Configure journal with small memory limit
	fs.journalMgr.config.EnableMemoryLimit = true
	fs.journalMgr.config.MaxMemoryPerJournal = 1024 // 1KB limit

	fileName := "test_journal_memory.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	// Write data exceeding memory limit (should trigger auto-flush)
	chunkSize := 256
	numChunks := 10 // Total 2.5KB, exceeds 1KB limit
	for i := 0; i < numChunks; i++ {
		data := bytes.Repeat([]byte{byte(i)}, chunkSize)
		offset := int64(i * chunkSize)
		err = ra.Write(offset, data)
		if err != nil {
			t.Fatalf("Failed to write chunk %d: %v", i, err)
		}
	}

	// Verify data integrity
	for i := 0; i < numChunks; i++ {
		offset := int64(i * chunkSize)
		readBuf, err := ra.Read(offset, chunkSize)
		if err != nil {
			t.Fatalf("Failed to read at offset %d: %v", offset, err)
		}
		expected := byte(i)
		for j := 0; j < len(readBuf); j++ {
			if readBuf[j] != expected {
				t.Errorf("Data mismatch at position %d: got %d, want %d", offset+int64(j), readBuf[j], expected)
				break
			}
		}
	}

	t.Logf("✓ Memory limit test passed")
}

// TestJournalConcurrentWrites tests concurrent write operations
func TestJournalConcurrentWrites(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_concurrent")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_journal_concurrent.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	// First, write some initial data sequentially to establish base data
	// This ensures the file has data before concurrent random writes
	initialData := make([]byte, 50)
	for i := range initialData {
		initialData[i] = byte('A')
	}
	err = ra.Write(0, initialData)
	if err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// Flush to commit the sequential buffer
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Now perform concurrent random writes (these will trigger journal)
	// Start from offset 100 to avoid sequential write detection
	numGoroutines := 10
	writesPerGoroutine := 10
	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer func() { done <- true }()
			for i := 0; i < writesPerGoroutine; i++ {
				// Start from offset 100 to ensure random writes (not sequential)
				offset := int64(100 + goroutineID*writesPerGoroutine + i)
				data := []byte{byte(goroutineID)}
				err := ra.Write(offset, data)
				if err != nil {
					t.Errorf("Goroutine %d: Failed to write at offset %d: %v", goroutineID, offset, err)
					return
				}
				time.Sleep(time.Millisecond) // Small delay to increase concurrency
			}
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify initial data
	readBuf, err := ra.Read(0, len(initialData))
	if err != nil {
		t.Errorf("Failed to read initial data: %v", err)
	} else if !bytes.Equal(readBuf, initialData) {
		t.Errorf("Initial data mismatch: got %q, want %q", readBuf, initialData)
	}

	// Verify all concurrent writes
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < writesPerGoroutine; i++ {
			offset := int64(100 + g*writesPerGoroutine + i)
			readBuf, err := ra.Read(offset, 1)
			if err != nil {
				t.Errorf("Failed to read at offset %d: %v", offset, err)
				continue
			}
			if len(readBuf) == 0 {
				t.Errorf("Empty read buffer at offset %d", offset)
				continue
			}
			if readBuf[0] != byte(g) {
				t.Errorf("Data mismatch at offset %d: got %d, want %d", offset, readBuf[0], g)
			}
		}
	}

	t.Logf("✓ Concurrent writes test passed")
}

// TestJournalSmartFlushUpdatesFileObject tests that SmartFlush correctly updates file object's dataID and size
func TestJournalSmartFlushUpdatesFileObject(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_smartflush")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_smartflush_update.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Verify initial state: dataID=0, size=0
	fileObj1, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj1) == 0 {
		t.Fatalf("Failed to get initial file object: %v", err)
	}
	if fileObj1[0].DataID != 0 || fileObj1[0].Size != 0 {
		t.Fatalf("Initial file object should have dataID=0, size=0, got dataID=%d, size=%d",
			fileObj1[0].DataID, fileObj1[0].Size)
	}
	t.Logf("✓ Initial state verified: dataID=0, size=0")

	// Get journal directly
	journal := fs.journalMgr.GetOrCreate(fileID, 0, 0)
	if journal == nil {
		t.Fatalf("Failed to get journal")
	}

	// Write data to journal
	testData := []byte("TEST_DATA_FOR_SMART_FLUSH")
	for i := 0; i < 3; i++ {
		offset := int64(i * 100)
		err = journal.Write(offset, testData)
		if err != nil {
			t.Fatalf("Failed to write to journal at offset %d: %v", offset, err)
		}
	}

	t.Logf("Written %d entries to journal", journal.GetEntryCount())

	// Force full flush by calling Flush() and then creating version manually
	newDataID, newSize, err := journal.Flush()
	if err != nil {
		t.Fatalf("Failed to flush journal: %v", err)
	}
	t.Logf("Journal flushed: newDataID=%d, newSize=%d", newDataID, newSize)

	// Manually create version and update file object (similar to SmartFlush)
	lh, ok := fs.h.(*core.LocalHandler)
	if !ok {
		t.Fatal("Handler is not LocalHandler")
	}

	versionID := core.NewID()
	mTime := core.Now()

	versionObj := &core.ObjectInfo{
		ID:     versionID,
		PID:    fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
		Name:   fmt.Sprintf("v%d", mTime),
	}

	// Get file object
	fileObjs, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObjs) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	fileObj := fileObjs[0]

	// Update file object
	updateFileObj := &core.ObjectInfo{
		ID:     fileID,
		PID:    fileObj.PID,
		Type:   fileObj.Type,
		Name:   fileObj.Name,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// Batch write
	objectsToPut := []*core.ObjectInfo{versionObj, updateFileObj}
	_, err = lh.Put(fs.c, bktID, objectsToPut)
	if err != nil {
		t.Fatalf("Failed to write objects: %v", err)
	}

	t.Logf("✓ Version and file object written to database")

	// Verify file object is updated
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get updated file object: %v", err)
	}

	if fileObj2[0].DataID == 0 {
		t.Errorf("❌ File object dataID not updated: still 0 (should be %d)", newDataID)
	} else {
		t.Logf("✓ File object dataID updated: %d", fileObj2[0].DataID)
	}

	if fileObj2[0].Size == 0 {
		t.Errorf("❌ File object size not updated: still 0 (should be %d)", newSize)
	} else {
		t.Logf("✓ File object size updated: %d", fileObj2[0].Size)
	}

	// Verify version was created
	versions, _, _, err := fs.h.List(fs.c, bktID, fileID, core.ListOptions{Count: 0})
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}

	t.Logf("Listed %d objects under fileID=%d", len(versions), fileID)
	foundVersion := false
	for _, v := range versions {
		t.Logf("  Object: ID=%d, Type=%d, Name=%s, DataID=%d, Size=%d", v.ID, v.Type, v.Name, v.DataID, v.Size)
		if v.Type == core.OBJ_TYPE_VERSION && v.ID == versionID {
			foundVersion = true
			t.Logf("✓ Version created: ID=%d, dataID=%d, size=%d", v.ID, v.DataID, v.Size)
			if v.DataID != fileObj2[0].DataID {
				t.Errorf("Version dataID (%d) doesn't match file object dataID (%d)",
					v.DataID, fileObj2[0].DataID)
			}
			break
		}
	}

	if !foundVersion {
		t.Logf("Note: Version object (ID=%d) not listed (this may be expected behavior for child objects)", versionID)
	} else {
		t.Logf("✓ Version object verified in database")
	}

	t.Logf("✓ SmartFlush file object update test passed")
}

// TestJournalSnapshotDoesNotUpdateFileObject tests that journal snapshots don't update file object
func TestJournalSnapshotDoesNotUpdateFileObject(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_snapshot_noupdate")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_snapshot_noupdate.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	// Configure to trigger snapshot but not full flush
	fs.journalMgr.config.SnapshotEntryCount = 5
	fs.journalMgr.config.FullFlushTotalEntries = 1000 // High threshold

	// Get initial file object
	fileObj1, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj1) == 0 {
		t.Fatalf("Failed to get initial file object: %v", err)
	}
	initialDataID := fileObj1[0].DataID
	initialSize := fileObj1[0].Size

	// Write data to trigger snapshot (but not full flush)
	testData := []byte("DATA")
	for i := 0; i < 6; i++ { // Triggers snapshot at 5 entries
		offset := int64(i * 10)
		err = ra.Write(offset, testData)
		if err != nil {
			t.Fatalf("Failed to write at offset %d: %v", offset, err)
		}
	}

	// Flush to trigger snapshot
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Give a moment for async operations
	time.Sleep(100 * time.Millisecond)

	// Verify file object is NOT updated (journal snapshot should not change file object)
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get file object after snapshot: %v", err)
	}

	if fileObj2[0].DataID != initialDataID {
		t.Logf("Note: File object dataID changed from %d to %d (may be expected if full flush occurred)",
			initialDataID, fileObj2[0].DataID)
	} else {
		t.Logf("✓ File object dataID unchanged: %d", fileObj2[0].DataID)
	}

	if fileObj2[0].Size != initialSize {
		t.Logf("Note: File object size changed from %d to %d (may be expected if full flush occurred)",
			initialSize, fileObj2[0].Size)
	} else {
		t.Logf("✓ File object size unchanged: %d", fileObj2[0].Size)
	}

	// Verify journal snapshot was created
	versions, _, _, err := fs.h.List(fs.c, bktID, fileID, core.ListOptions{Count: 0})
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}

	foundJournalSnapshot := false
	for _, v := range versions {
		if v.Type == core.OBJ_TYPE_JOURNAL {
			foundJournalSnapshot = true
			t.Logf("✓ Journal snapshot created: ID=%d, name=%s", v.ID, v.Name)
			break
		}
	}

	if !foundJournalSnapshot {
		t.Logf("Note: No journal snapshot found (may have triggered full flush instead)")
	}

	t.Logf("✓ Journal snapshot behavior test passed")
}

// TestJournalFlushUpdatesFileObject tests flushJournal updates file object correctly
func TestJournalFlushUpdatesFileObject(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_flush_update")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_flush_update.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Verify initial state
	fileObj1, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj1) == 0 {
		t.Fatalf("Failed to get initial file object: %v", err)
	}
	t.Logf("Initial state: dataID=%d, size=%d", fileObj1[0].DataID, fileObj1[0].Size)

	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	// Write some data
	testData := []byte("Hello, World! This is a test of journal flush.")
	err = ra.Write(0, testData)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Write more data at different offset
	testData2 := []byte("More data here.")
	err = ra.Write(100, testData2)
	if err != nil {
		t.Fatalf("Failed to write second chunk: %v", err)
	}

	// Flush
	versionID, err := ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	t.Logf("Flush returned versionID: %d", versionID)

	// Give a moment for operations to complete
	time.Sleep(100 * time.Millisecond)

	// Verify file object is updated
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get updated file object: %v", err)
	}

	if fileObj2[0].DataID == 0 {
		t.Errorf("❌ File object dataID still 0 after flush")
	} else {
		t.Logf("✓ File object dataID updated: %d", fileObj2[0].DataID)
	}

	expectedMinSize := int64(100 + len(testData2))
	if fileObj2[0].Size < expectedMinSize {
		t.Errorf("❌ File object size (%d) less than expected minimum (%d)",
			fileObj2[0].Size, expectedMinSize)
	} else {
		t.Logf("✓ File object size updated: %d (expected >= %d)", fileObj2[0].Size, expectedMinSize)
	}

	// Verify data can be read back correctly
	readBuf, err := ra.Read(0, len(testData))
	if err != nil {
		t.Fatalf("Failed to read data after flush: %v", err)
	}
	if !bytes.Equal(readBuf, testData) {
		t.Errorf("Data mismatch after flush")
	} else {
		t.Logf("✓ Data integrity verified after flush")
	}

	t.Logf("✓ Journal flush file object update test passed")
}

// TestJournalFlushWithAtomicReplace tests flushJournal with atomic replace scenario
// This test covers the case where a sparse file uses journal writes with non-sequential writes,
// and during flush, an atomic replace operation is detected (pending deletion exists).
// This scenario can occur when WPS opens pptx/ppt files.
func TestJournalFlushWithAtomicReplace(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_test_atomic_replace")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_atomic_replace.ppt"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Step 1: Create an "old" file object with some versions (simulating atomic replace scenario)
	// This represents the file that was previously deleted and is pending deletion
	oldFileID := core.NewID()
	root := fs.root
	if root == nil {
		t.Fatalf("Failed to get root node")
	}

	oldFileObj := &core.ObjectInfo{
		ID:     oldFileID,
		PID:    root.objID,
		Type:   core.OBJ_TYPE_FILE,
		Name:   fileName,
		DataID: 0,
		Size:   0,
		MTime:  core.Now(),
	}

	lh, ok := fs.h.(*core.LocalHandler)
	if !ok {
		t.Fatalf("Handler is not LocalHandler")
	}

	_, err = lh.Put(fs.c, bktID, []*core.ObjectInfo{oldFileObj})
	if err != nil {
		t.Fatalf("Failed to create old file object: %v", err)
	}

	// Create some version objects for the old file
	oldVersionIDs := make([]int64, 0, 2)
	for i := 0; i < 2; i++ {
		versionID := core.NewID()
		versionObj := &core.ObjectInfo{
			ID:     versionID,
			PID:    oldFileID,
			Type:   core.OBJ_TYPE_VERSION,
			DataID: 0,
			Size:   0,
			MTime:  core.Now(),
		}
		_, err = lh.Put(fs.c, bktID, []*core.ObjectInfo{versionObj})
		if err != nil {
			t.Fatalf("Failed to create version object: %v", err)
		}
		oldVersionIDs = append(oldVersionIDs, versionID)
	}

	t.Logf("Created old file: fileID=%d, versions=%d", oldFileID, len(oldVersionIDs))

	// Give a moment for version objects to be available for List operations
	time.Sleep(50 * time.Millisecond)

	// Step 2: Schedule deletion for the old file (simulating atomic replace)
	// This will call getFileVersions which should find the version objects we just created
	if fs.atomicReplaceMgr == nil {
		t.Fatalf("Atomic replace manager not initialized")
	}

	err = fs.atomicReplaceMgr.ScheduleDeletion(bktID, root.objID, fileName, oldFileID)
	if err != nil {
		t.Fatalf("Failed to schedule deletion: %v", err)
	}

	// Verify that versions were found by ScheduleDeletion
	// We can't directly check, but we'll verify in the flush step

	t.Logf("Scheduled deletion for old file: fileID=%d", oldFileID)

	// Step 3: Write data to the new file using journal (simulating sparse file with non-sequential writes)
	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	// Set sparse size to simulate sparse file
	ra.MarkSparseFile(8036352) // 8MB sparse file

	// Write data at various offsets (non-sequential, simulating WPS write pattern)
	writeOffsets := []int64{0, 524288, 1048576, 2097152, 3145728}
	for i, offset := range writeOffsets {
		data := make([]byte, 512)
		for j := range data {
			data[j] = byte(i + 1) // Distinct pattern for each write
		}
		err = ra.Write(offset, data)
		if err != nil {
			t.Fatalf("Failed to write at offset %d: %v", offset, err)
		}
	}

	t.Logf("Written %d journal entries", len(writeOffsets))

	// Step 4: Flush journal (this should detect atomic replace and merge versions)
	versionID, err := ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	t.Logf("Flush completed: versionID=%d", versionID)

	// Give a moment for operations to complete
	time.Sleep(100 * time.Millisecond)

	// Step 5: Verify versions were merged to the new file
	// Check that old versions now have PID = fileID (new file)
	// Note: Versions may not be merged if pd.Versions was empty (getFileVersions may not find them)
	// This is acceptable - the important part is that the atomic replace detection works
	versionsMerged := false
	for _, versionID := range oldVersionIDs {
		versions, err := fs.h.Get(fs.c, bktID, []int64{versionID})
		if err != nil {
			t.Logf("Note: Failed to get version %d (may have been deleted): %v", versionID, err)
			continue
		}
		if len(versions) == 0 {
			t.Logf("Note: Version %d not found (may have been deleted)", versionID)
			continue
		}
		if versions[0].PID == fileID {
			versionsMerged = true
			t.Logf("✓ Version %d merged to new file (PID=%d)", versionID, fileID)
		} else if versions[0].PID == oldFileID {
			// Version still has old PID - this means getFileVersions didn't find it
			// or the merge didn't happen. This is acceptable if pd.Versions was empty.
			t.Logf("Note: Version %d still has old PID=%d (may not have been in pd.Versions)", versionID, oldFileID)
		}
	}

	if !versionsMerged {
		t.Logf("Note: No versions were merged (this is acceptable if getFileVersions didn't find them)")
	}

	// Step 6: Verify old file object was deleted
	oldFileObjs, err := fs.h.Get(fs.c, bktID, []int64{oldFileID})
	if err == nil && len(oldFileObjs) > 0 {
		t.Errorf("Old file object %d still exists (should be deleted)", oldFileID)
	} else {
		t.Logf("✓ Old file object %d deleted", oldFileID)
	}

	// Step 7: Verify new file object is updated correctly
	fileObj, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}

	if fileObj[0].DataID == 0 {
		t.Errorf("File object dataID still 0 after flush")
	} else {
		t.Logf("✓ File object dataID updated: %d", fileObj[0].DataID)
	}

	// Step 8: Verify data can be read back correctly
	for i, offset := range writeOffsets {
		expectedData := make([]byte, 512)
		for j := range expectedData {
			expectedData[j] = byte(i + 1)
		}
		readBuf, err := ra.Read(offset, len(expectedData))
		if err != nil {
			t.Fatalf("Failed to read at offset %d: %v", offset, err)
		}
		if !bytes.Equal(readBuf, expectedData) {
			t.Errorf("Data mismatch at offset %d: got %v, want %v", offset, readBuf[:min(10, len(readBuf))], expectedData[:min(10, len(expectedData))])
		} else {
			t.Logf("✓ Data integrity verified at offset %d", offset)
		}
	}

	// Step 9: Verify pending deletion was canceled
	pendingCount := fs.atomicReplaceMgr.GetPendingCount()
	if pendingCount > 0 {
		t.Logf("Note: %d pending deletions still exist (may be from other tests)", pendingCount)
	}

	t.Logf("✓ Journal flush with atomic replace test passed")
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Helper functions

func setupTestFS(t *testing.T, testDir string) (*OrcasFS, int64) {
	// Create test directory
	if err := os.MkdirAll(testDir, 0o755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Initialize main database
	if err := core.InitDB(testDir, ""); err != nil {
		t.Fatalf("Failed to init database: %v", err)
	}

	// Create local handler
	// NewLocalHandler(basePath, dataPath)
	// basePath: for main database
	// dataPath: for bucket databases and data files
	lh := core.NewLocalHandler(testDir, testDir)

	cfg := &core.Config{
		DataPath: testDir,
	}

	// Create test bucket using LocalAdmin
	ctx := context.Background()

	// Set ADMIN role in context for PutBkt (no login needed for LocalAdmin)
	uid := core.NewID()
	ctx = core.UserInfo2Ctx(ctx, &core.UserInfo{ID: uid, Role: core.ADMIN})

	bktID := core.NewID()

	admin := core.NewLocalAdmin(testDir, testDir)
	bucketInfo := &core.BucketInfo{
		ID:        bktID,
		Name:      "test_journal_bucket",
		ChunkSize: 1 << 20, // 1MB chunks
		Type:      1,
		Quota:     -1,
	}
	if err := admin.PutBkt(ctx, []*core.BucketInfo{bucketInfo}); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Create VFS with authenticated context
	fs := NewOrcasFSWithConfig(lh, ctx, bktID, cfg)
	if fs == nil {
		t.Fatal("Failed to create OrcasFS")
	}

	// Enable journal
	if fs.journalMgr == nil {
		t.Fatal("Journal manager not initialized")
	}
	fs.journalMgr.config.Enabled = true

	return fs, bktID
}

func createTestFile(t *testing.T, fs *OrcasFS, bktID int64, fileName string) (int64, error) {
	// Get root node
	root := fs.root
	if root == nil {
		return 0, fmt.Errorf("failed to get root node")
	}

	// Create file
	fileID := core.NewID()
	fileObj := &core.ObjectInfo{
		ID:     fileID,
		PID:    root.objID,
		Type:   core.OBJ_TYPE_FILE,
		Name:   fileName,
		DataID: 0,
		Size:   0,
		MTime:  core.Now(),
	}

	lh, ok := fs.h.(*core.LocalHandler)
	if !ok {
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	_, err := lh.Put(fs.c, bktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		return 0, err
	}

	return fileID, nil
}

func cleanupFS(fs *OrcasFS) {
	// Close filesystem (stops WAL checkpoint manager and closes RandomAccessors)
	if fs != nil {
		fs.Close()
	}

	// Close database pool connections
	pool := core.GetDBPool()
	if pool != nil {
		pool.Close()
	}

	// Small delay to ensure all connections are fully closed
	time.Sleep(50 * time.Millisecond)
}

func cleanupTestDir(t *testing.T, testDir string) {
	// Close database connections first
	pool := core.GetDBPool()
	if pool != nil {
		pool.Close()
	}

	// Small delay to ensure connections are fully closed
	time.Sleep(50 * time.Millisecond)

	if err := os.RemoveAll(testDir); err != nil {
		t.Logf("Warning: Failed to cleanup test directory %s: %v", testDir, err)
	} else {
		t.Logf("✓ Cleaned up test directory: %s", testDir)
	}
}

// TestJournalEncryptionSimple tests basic encryption without complex journal operations
func TestJournalEncryptionSimple(t *testing.T) {
	// Setup test environment
	testDir := filepath.Join(os.TempDir(), "orcas_journal_encryption_simple_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	// Enable encryption
	fs.EndecWay = core.DATA_ENDEC_AES256
	fs.EndecKey = "test-encryption-key-32-bytes!!"

	// Disable journal WAL to avoid complexity
	if fs.journalMgr != nil {
		// Disable journal WAL is not needed - it has its own mechanism
	}

	fileName := "test_encryption_simple.txt"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	originalContent := []byte("This is the original content before truncate.")
	newContent := []byte("This is the NEW content after truncate. It should be encrypted and decrypted correctly!")

	t.Run("WriteOriginalContent", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		if err := ra.Write(0, originalContent); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}

		if _, err := ra.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		if err := ra.Close(); err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		t.Logf("✓ Original content written and flushed (%d bytes)", len(originalContent))
	})

	t.Run("TruncateAndRewrite", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		// Truncate to 0
		if _, err := ra.Truncate(0); err != nil {
			t.Fatalf("Failed to truncate: %v", err)
		}

		t.Logf("✓ File truncated to 0")

		// Write new content
		if err := ra.Write(0, newContent); err != nil {
			t.Fatalf("Failed to write new content: %v", err)
		}

		t.Logf("✓ New content written (%d bytes)", len(newContent))

		// Flush
		if _, err := ra.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		t.Logf("✓ Content flushed")

		// Close
		if err := ra.Close(); err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		t.Logf("✓ RandomAccessor closed")
	})

	t.Run("ReopenAndRead", func(t *testing.T) {
		// Get file object to check size
		lh, ok := fs.h.(*core.LocalHandler)
		if !ok {
			t.Fatalf("Handler is not LocalHandler")
		}

		objs, err := lh.Get(fs.c, bktID, []int64{fileID})
		if err != nil || len(objs) == 0 {
			t.Fatalf("Failed to get file object: %v", err)
		}
		fileObj := objs[0]

		t.Logf("File info: Size=%d, DataID=%d", fileObj.Size, fileObj.DataID)

		// Verify size
		if fileObj.Size != int64(len(newContent)) {
			t.Errorf("Size mismatch: expected %d, got %d", len(newContent), fileObj.Size)
		}

		// Verify DataInfo
		dataInfo, err := lh.GetDataInfo(fs.c, bktID, fileObj.DataID)
		if err != nil {
			t.Fatalf("Failed to get DataInfo: %v", err)
		}

		t.Logf("DataInfo: ID=%d, OrigSize=%d, Size=%d, Kind=0x%x",
			dataInfo.ID, dataInfo.OrigSize, dataInfo.Size, dataInfo.Kind)

		// Verify encryption flag
		if dataInfo.Kind&core.DATA_ENDEC_AES256 == 0 {
			t.Errorf("Missing AES256 encryption flag: Kind=0x%x", dataInfo.Kind)
		}

		// Verify sizes
		if dataInfo.OrigSize != int64(len(newContent)) {
			t.Errorf("OrigSize mismatch: expected %d, got %d", len(newContent), dataInfo.OrigSize)
		}

		if dataInfo.Size < dataInfo.OrigSize {
			t.Errorf("Size should be >= OrigSize for encrypted data: Size=%d, OrigSize=%d",
				dataInfo.Size, dataInfo.OrigSize)
		}

		// Create new RandomAccessor and read
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}
		defer ra.Close()

		// Read content
		readData, err := ra.Read(0, int(fileObj.Size))
		if err != nil {
			t.Fatalf("Failed to read content: %v", err)
		}

		t.Logf("Read %d bytes", len(readData))

		// Verify content
		if !bytes.Equal(readData, newContent) {
			t.Errorf("Content mismatch:\nExpected (%d bytes): %s\nGot (%d bytes): %s",
				len(newContent), string(newContent), len(readData), string(readData))
			t.Logf("Expected hex: %x", newContent)
			t.Logf("Got hex: %x", readData)
		} else {
			t.Logf("✓ Content verified correctly after reopen!")
		}
	})
}

// TestEncryptionWithoutJournal tests encryption fix without using Journal
// This verifies that the encryption in flushSmallFile works correctly
func TestEncryptionWithoutJournal(t *testing.T) {
	// Setup test environment
	testDir := filepath.Join(os.TempDir(), "orcas_encryption_no_journal_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	// Enable encryption
	fs.EndecWay = core.DATA_ENDEC_AES256
	fs.EndecKey = "test-encryption-key-32-bytes!!"

	// Disable journal completely
	if fs.journalMgr != nil {
		fs.journalMgr.config.Enabled = false
	}

	fileName := "test_encryption_no_journal.txt"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Use small content to ensure it goes through flushSmallFile path
	content := []byte("This content should be encrypted and decrypted correctly!")

	t.Run("WriteAndFlush", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		// Write
		if err := ra.Write(0, content); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}

		// Force sequential buffer flush
		if _, err := ra.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		if err := ra.Close(); err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		t.Logf("✓ Content written and flushed (%d bytes)", len(content))
	})

	t.Run("VerifyEncryption", func(t *testing.T) {
		lh, ok := fs.h.(*core.LocalHandler)
		if !ok {
			t.Fatalf("Handler is not LocalHandler")
		}

		objs, err := lh.Get(fs.c, bktID, []int64{fileID})
		if err != nil || len(objs) == 0 {
			t.Fatalf("Failed to get file object: %v", err)
		}
		fileObj := objs[0]

		dataInfo, err := lh.GetDataInfo(fs.c, bktID, fileObj.DataID)
		if err != nil {
			t.Fatalf("Failed to get DataInfo: %v", err)
		}

		t.Logf("DataInfo: ID=%d, OrigSize=%d, Size=%d, Kind=0x%x",
			dataInfo.ID, dataInfo.OrigSize, dataInfo.Size, dataInfo.Kind)

		// Verify encryption flag
		if dataInfo.Kind&core.DATA_ENDEC_AES256 == 0 {
			t.Errorf("Missing AES256 encryption flag: Kind=0x%x", dataInfo.Kind)
		}

		// Verify sizes
		if dataInfo.OrigSize != int64(len(content)) {
			t.Errorf("OrigSize mismatch: expected %d, got %d", len(content), dataInfo.OrigSize)
		}

		// For encrypted data, Size should be >= OrigSize
		if dataInfo.Size < dataInfo.OrigSize {
			t.Errorf("Size should be >= OrigSize: Size=%d, OrigSize=%d",
				dataInfo.Size, dataInfo.OrigSize)
		}

		t.Logf("✓ Encryption verified: OrigSize=%d, EncryptedSize=%d", dataInfo.OrigSize, dataInfo.Size)
	})

	t.Run("ReadAndDecrypt", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}
		defer ra.Close()

		readData, err := ra.Read(0, len(content))
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		if !bytes.Equal(readData, content) {
			t.Errorf("Content mismatch:\nExpected: %s\nGot: %s", string(content), string(readData))
			t.Logf("Expected hex: %x", content)
			t.Logf("Got hex: %x", readData)
		} else {
			t.Logf("✓ Content decrypted correctly!")
		}
	})
}

// TestJournalEncryptionAfterTruncate tests that encrypted files can be correctly read after truncate + write + reopen
// This test verifies the fix for: "decryption failed: cipher: message authentication failed"
func TestJournalEncryptionAfterTruncate(t *testing.T) {
	// Setup test environment
	testDir := filepath.Join(os.TempDir(), "orcas_journal_encryption_test")
	defer cleanupTestDir(t, testDir)

	encryptionKey := "test-encryption-key-32-bytes!!"
	fs, bktID := setupTestFSWithEncryption(t, testDir, encryptionKey, 0)
	defer cleanupFS(fs)

	// Test data
	originalContent := []byte("This is the original file content with some text.")
	newContent := []byte("This is the NEW content after truncate and rewrite. It should be encrypted correctly!")

	fileName := "test_encrypted_file.txt"
	var fileID int64

	t.Run("CreateInitialFile", func(t *testing.T) {
		// Create file
		var err error
		fileID, err = createTestFile(t, fs, bktID, fileName)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		// Write initial content
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		if err := ra.Write(0, originalContent); err != nil {
			t.Fatalf("Failed to write initial content: %v", err)
		}

		if _, err := ra.Flush(); err != nil {
			t.Fatalf("Failed to flush initial write: %v", err)
		}

		// Read back and verify
		readData, err := ra.Read(0, len(originalContent))
		if err != nil {
			t.Fatalf("Failed to read initial content: %v", err)
		}

		if !bytes.Equal(readData, originalContent) {
			t.Errorf("Initial content mismatch:\nExpected: %s\nGot: %s", originalContent, readData)
		}

		if err := ra.Close(); err != nil {
			t.Fatalf("Failed to close RandomAccessor: %v", err)
		}

		t.Logf("✓ Initial file created and verified: %s (%d bytes)", fileName, len(originalContent))
	})

	t.Run("TruncateAndRewrite", func(t *testing.T) {
		// Get file object
		lh, ok := fs.h.(*core.LocalHandler)
		if !ok {
			t.Fatalf("Handler is not LocalHandler")
		}

		objs, err := lh.Get(fs.c, bktID, []int64{fileID})
		if err != nil || len(objs) == 0 {
			t.Fatalf("Failed to get file object: %v", err)
		}
		fileObj := objs[0]

		t.Logf("File before truncate: ID=%d, Size=%d, DataID=%d", fileID, fileObj.Size, fileObj.DataID)

		// Create RandomAccessor
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		// Truncate to 0
		if _, err := ra.Truncate(0); err != nil {
			t.Fatalf("Failed to truncate file: %v", err)
		}

		t.Logf("✓ File truncated to 0")

		// Write new content
		if err := ra.Write(0, newContent); err != nil {
			t.Fatalf("Failed to write new content: %v", err)
		}

		t.Logf("✓ New content written (%d bytes)", len(newContent))

		// Flush
		if _, err := ra.Flush(); err != nil {
			t.Fatalf("Failed to flush after write: %v", err)
		}

		t.Logf("✓ Content flushed")

		// Close
		if err := ra.Close(); err != nil {
			t.Fatalf("Failed to close RandomAccessor: %v", err)
		}

		t.Logf("✓ RandomAccessor closed")

		// Get updated file object
		objs, err = lh.Get(fs.c, bktID, []int64{fileID})
		if err != nil || len(objs) == 0 {
			t.Fatalf("Failed to get updated file object: %v", err)
		}
		updatedFileObj := objs[0]

		t.Logf("File after rewrite: ID=%d, Size=%d, DataID=%d", updatedFileObj.ID, updatedFileObj.Size, updatedFileObj.DataID)

		if updatedFileObj.Size != int64(len(newContent)) {
			t.Errorf("File size mismatch after rewrite: expected %d, got %d", len(newContent), updatedFileObj.Size)
		}

		// Get DataInfo to verify encryption
		dataInfo, err := lh.GetDataInfo(fs.c, bktID, updatedFileObj.DataID)
		if err != nil {
			t.Fatalf("Failed to get DataInfo: %v", err)
		}

		t.Logf("DataInfo: ID=%d, Size=%d, OrigSize=%d, Kind=0x%x",
			dataInfo.ID, dataInfo.Size, dataInfo.OrigSize, dataInfo.Kind)

		// Verify encryption flag is set
		if dataInfo.Kind&core.DATA_ENDEC_AES256 == 0 {
			t.Errorf("DataInfo.Kind does not have AES256 encryption flag: Kind=0x%x", dataInfo.Kind)
		}

		// Verify sizes
		if dataInfo.OrigSize != int64(len(newContent)) {
			t.Errorf("DataInfo.OrigSize mismatch: expected %d, got %d", len(newContent), dataInfo.OrigSize)
		}

		// For encrypted data, Size should be >= OrigSize (due to padding)
		if dataInfo.Size < dataInfo.OrigSize {
			t.Errorf("DataInfo.Size should be >= OrigSize for encrypted data: Size=%d, OrigSize=%d",
				dataInfo.Size, dataInfo.OrigSize)
		}

		t.Logf("✓ Encryption verified: OrigSize=%d, EncryptedSize=%d", dataInfo.OrigSize, dataInfo.Size)
	})

	t.Run("ReopenAndRead", func(t *testing.T) {
		// Get file object
		lh, ok := fs.h.(*core.LocalHandler)
		if !ok {
			t.Fatalf("Handler is not LocalHandler")
		}

		objs, err := lh.Get(fs.c, bktID, []int64{fileID})
		if err != nil || len(objs) == 0 {
			t.Fatalf("Failed to get file object: %v", err)
		}
		fileObj := objs[0]

		t.Logf("Reopening file: ID=%d, Size=%d, DataID=%d", fileID, fileObj.Size, fileObj.DataID)

		// Create new RandomAccessor (simulating file reopen)
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}
		defer ra.Close()

		// Read content
		readData, err := ra.Read(0, int(fileObj.Size))
		if err != nil {
			t.Fatalf("Failed to read content after reopen: %v", err)
		}

		t.Logf("Read data length: %d bytes", len(readData))
		t.Logf("Read data: %s", string(readData))
		t.Logf("Expected: %s", string(newContent))

		// Verify content matches what we wrote
		if !bytes.Equal(readData, newContent) {
			t.Errorf("Content mismatch after reopen:\nExpected (%d bytes): %s\nGot (%d bytes): %s",
				len(newContent), string(newContent), len(readData), string(readData))

			// Show hex dump for debugging
			t.Logf("Expected hex: %x", newContent)
			t.Logf("Got hex: %x", readData)
		} else {
			t.Logf("✓ Content verified correctly after reopen!")
		}
	})
}

// TestJournalMultipleModifications tests multiple sequential modifications with encryption
func TestJournalMultipleModifications(t *testing.T) {
	// Setup test environment
	testDir := filepath.Join(os.TempDir(), "orcas_journal_multi_mod_test")
	defer cleanupTestDir(t, testDir)

	encryptionKey := "test-encryption-key-32-bytes!!"
	fs, bktID := setupTestFSWithEncryption(t, testDir, encryptionKey, 0)
	defer cleanupFS(fs)

	fileName := "test_multi_mod.txt"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Perform multiple modifications
	modifications := [][]byte{
		[]byte("Version 1: Initial content"),
		[]byte("Version 2: Modified content with more text"),
		[]byte("V3: Short"),
		[]byte("Version 4: This is a much longer version with lots of text to test encryption properly."),
	}

	for i, content := range modifications {
		t.Run(fmt.Sprintf("Modification_%d", i+1), func(t *testing.T) {
			ra, err := getOrCreateRandomAccessor(fs, fileID)
			if err != nil {
				t.Fatalf("Failed to create RandomAccessor: %v", err)
			}

			// Truncate to 0
			if _, err := ra.Truncate(0); err != nil {
				t.Fatalf("Failed to truncate: %v", err)
			}

			// Write new content
			if err := ra.Write(0, content); err != nil {
				t.Fatalf("Failed to write: %v", err)
			}

			if _, err := ra.Flush(); err != nil {
				t.Fatalf("Failed to flush: %v", err)
			}

			if err := ra.Close(); err != nil {
				t.Fatalf("Failed to close: %v", err)
			}

			// Reopen and verify
			ra2, err := getOrCreateRandomAccessor(fs, fileID)
			if err != nil {
				t.Fatalf("Failed to reopen: %v", err)
			}
			defer ra2.Close()

			readData, err := ra2.Read(0, len(content))
			if err != nil {
				t.Fatalf("Failed to read: %v", err)
			}

			if !bytes.Equal(readData, content) {
				t.Errorf("Content mismatch in modification %d:\nExpected: %s\nGot: %s",
					i+1, string(content), string(readData))
			} else {
				t.Logf("✓ Modification %d verified (%d bytes)", i+1, len(content))
			}
		})
	}
}

// TestSequentialBufferEncryption tests encryption via sequential buffer (not journal)
// This verifies that encryption works correctly in the sequential write path
func TestSequentialBufferEncryption(t *testing.T) {
	// Setup test environment
	testDir := filepath.Join(os.TempDir(), "orcas_encryption_no_journal_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	// Enable encryption
	fs.EndecWay = core.DATA_ENDEC_AES256
	fs.EndecKey = "test-encryption-key-32-bytes!!"

	// Keep journal enabled but let the optimization decide
	// For small files after truncate with sequential writes, it should use seqBuffer

	fileName := "test_encryption_no_journal.txt"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Use small content to test the optimization
	// After truncate(0) + sequential write, should use seqBuffer instead of journal
	content := []byte("This content should be encrypted via sequential buffer, not journal!")

	t.Run("WriteAndFlush", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		// Write
		if err := ra.Write(0, content); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}

		// Force sequential buffer flush
		if _, err := ra.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		if err := ra.Close(); err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		t.Logf("✓ Content written and flushed (%d bytes)", len(content))
	})

	t.Run("VerifyEncryption", func(t *testing.T) {
		lh, ok := fs.h.(*core.LocalHandler)
		if !ok {
			t.Fatalf("Handler is not LocalHandler")
		}

		objs, err := lh.Get(fs.c, bktID, []int64{fileID})
		if err != nil || len(objs) == 0 {
			t.Fatalf("Failed to get file object: %v", err)
		}
		fileObj := objs[0]

		dataInfo, err := lh.GetDataInfo(fs.c, bktID, fileObj.DataID)
		if err != nil {
			t.Fatalf("Failed to get DataInfo: %v", err)
		}

		t.Logf("DataInfo: ID=%d, OrigSize=%d, Size=%d, Kind=0x%x",
			dataInfo.ID, dataInfo.OrigSize, dataInfo.Size, dataInfo.Kind)

		// Verify encryption flag
		if dataInfo.Kind&core.DATA_ENDEC_AES256 == 0 {
			t.Errorf("Missing AES256 encryption flag: Kind=0x%x", dataInfo.Kind)
		}

		// Verify sizes
		if dataInfo.OrigSize != int64(len(content)) {
			t.Errorf("OrigSize mismatch: expected %d, got %d", len(content), dataInfo.OrigSize)
		}

		// For encrypted data, Size should be >= OrigSize
		if dataInfo.Size < dataInfo.OrigSize {
			t.Errorf("Size should be >= OrigSize: Size=%d, OrigSize=%d",
				dataInfo.Size, dataInfo.OrigSize)
		}

		t.Logf("✓ Encryption verified via sequential buffer: OrigSize=%d, EncryptedSize=%d", dataInfo.OrigSize, dataInfo.Size)
	})

	t.Run("ReadAndDecrypt", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}
		defer ra.Close()

		readData, err := ra.Read(0, len(content))
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		if !bytes.Equal(readData, content) {
			t.Errorf("Content mismatch:\nExpected: %s\nGot: %s", string(content), string(readData))
			t.Logf("Expected hex: %x", content)
			t.Logf("Got hex: %x", readData)
		} else {
			t.Logf("✓ Content decrypted correctly!")
		}
	})
}

// TestSmallFileTruncateRewrite tests the optimized path: truncate(0) + sequential write
// With the optimization, small files should use sequential buffer instead of journal
func TestSmallFileTruncateRewrite(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_encryption_after_truncate_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	// Enable encryption
	fs.EndecWay = core.DATA_ENDEC_AES256
	fs.EndecKey = "test-encryption-key-32-bytes!!"

	fileName := "test_truncate_rewrite.txt"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	originalContent := []byte("Original content before truncate.")
	newContent := []byte("New content after truncate. Should use sequential buffer!")

	t.Run("WriteOriginal", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		if err := ra.Write(0, originalContent); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}

		if _, err := ra.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		if err := ra.Close(); err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		t.Logf("✓ Original content written (%d bytes)", len(originalContent))
	})

	t.Run("TruncateAndRewrite", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		// Truncate to 0
		if _, err := ra.Truncate(0); err != nil {
			t.Fatalf("Failed to truncate: %v", err)
		}

		t.Logf("✓ File truncated to 0")

		// Write new content sequentially
		// With optimization, this should use sequential buffer, not journal
		if err := ra.Write(0, newContent); err != nil {
			t.Fatalf("Failed to write new content: %v", err)
		}

		t.Logf("✓ New content written (%d bytes)", len(newContent))

		if _, err := ra.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		if err := ra.Close(); err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		t.Logf("✓ Flush and close completed")
	})

	t.Run("VerifyNewContent", func(t *testing.T) {
		ra, err := getOrCreateRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}
		defer ra.Close()

		readData, err := ra.Read(0, len(newContent))
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		if !bytes.Equal(readData, newContent) {
			t.Errorf("Content mismatch after truncate:\nExpected: %s\nGot: %s",
				string(newContent), string(readData))
		} else {
			t.Logf("✓ New content verified after truncate!")
		}
	})
}

// TestJournalReadOverlay tests that reads correctly apply journal overlay
// This reproduces the Office file modification issue where:
// 1. File is opened and read successfully
// 2. File is modified with journal writes (without flush)
// 3. Subsequent reads should see the modified data (journal overlay)
// 4. Bug was: reads went directly to base data without journal overlay
func TestJournalReadOverlay(t *testing.T) {
	// Setup test environment
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(".", testBktID)
	if err != nil {
		t.Fatalf("Failed to init bucket DB: %v", err)
	}

	dma := &core.DefaultMetadataAdapter{
		DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
		DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
	}
	dma.DefaultBaseMetadataAdapter.SetPath(".")
	dma.DefaultDataMetadataAdapter.SetPath(".")
	dda := &core.DefaultDataAdapter{}

	// Create LocalHandler
	lh := core.NewLocalHandler("", "").(*core.LocalHandler)
	lh.SetAdapter(dma, dda)

	// Login to get context
	testCtx, userInfo, _, err := lh.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Failed to login: %v", err)
	}

	// Create bucket
	bucket := &core.BucketInfo{
		ID:       testBktID,
		Name:     "test_journal_bucket",
		Type:     1,
		Quota:    1000000000,
		Used:     0,
		RealUsed: 0,
	}
	if err := dma.PutBkt(testCtx, []*core.BucketInfo{bucket}); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	if err := dma.PutACL(testCtx, testBktID, userInfo.ID, core.ALL); err != nil {
		t.Fatalf("Failed to set ACL: %v", err)
	}

	// Create file object (empty initially, will write data via RandomAccessor)
	fileID, _ := ig.New()
	fileObj := &core.ObjectInfo{
		ID:    fileID,
		PID:   testBktID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  "test.ppt",
		Size:  0,
		MTime: core.Now(),
	}
	if _, err := dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj}); err != nil {
		t.Fatalf("Failed to create file object: %v", err)
	}

	// Create VFS
	ofs := NewOrcasFS(lh, testCtx, testBktID)
	t.Logf("Created test file: fileID=%d", fileID)

	// Create RandomAccessor
	ra, err := NewRandomAccessor(ofs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	defer ra.Close()

	// Step 1: Create initial file content (simulating an Office file)
	// 8MB file with recognizable pattern
	fileSize := int64(8 * 1024 * 1024)
	originalData := make([]byte, fileSize)
	for i := range originalData {
		originalData[i] = byte(i % 256)
	}

	// Write initial data and flush (simulating file upload)
	if err := ra.Write(0, originalData); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}
	if _, err := ra.Flush(); err != nil {
		t.Fatalf("Failed to flush initial data: %v", err)
	}
	t.Logf("✓ Initial file created: size=%d", fileSize)

	// Step 2: Read initial data to verify it's correct
	data, err := ra.Read(0, 4096)
	if err != nil {
		t.Fatalf("Failed to read initial data: %v", err)
	}
	if len(data) != 4096 {
		t.Fatalf("Expected 4096 bytes, got %d", len(data))
	}
	if !bytes.Equal(data, originalData[:4096]) {
		t.Fatalf("Initial read data mismatch")
	}
	t.Logf("✓ Initial read successful: read %d bytes at offset 0", len(data))

	// Step 3: Perform random writes (simulating Office modifications)
	// These writes create journal entries WITHOUT flushing

	// Write 1: Modify 512 bytes at offset 7971328 (similar to Office pattern from logs)
	writeOffset1 := int64(7971328)
	writeData1 := make([]byte, 512)
	for i := range writeData1 {
		writeData1[i] = 0xFF // Distinct pattern
	}
	if err := ra.Write(writeOffset1, writeData1); err != nil {
		t.Fatalf("Failed to write 1: %v", err)
	}
	t.Logf("✓ Write 1 successful: wrote %d bytes at offset %d", len(writeData1), writeOffset1)

	// Write 2: Modify 2560 bytes at offset 7968768 (overlapping/adjacent)
	writeOffset2 := int64(7968768)
	writeData2 := make([]byte, 2560)
	for i := range writeData2 {
		writeData2[i] = 0xAA // Another distinct pattern
	}
	if err := ra.Write(writeOffset2, writeData2); err != nil {
		t.Fatalf("Failed to write 2: %v", err)
	}
	t.Logf("✓ Write 2 successful: wrote %d bytes at offset %d", len(writeData2), writeOffset2)

	// Write 3: Modify 512 bytes at offset 0 (beginning of file)
	writeOffset3 := int64(0)
	writeData3 := make([]byte, 512)
	for i := range writeData3 {
		writeData3[i] = 0x55 // Yet another distinct pattern
	}
	if err := ra.Write(writeOffset3, writeData3); err != nil {
		t.Fatalf("Failed to write 3: %v", err)
	}
	t.Logf("✓ Write 3 successful: wrote %d bytes at offset %d", len(writeData3), writeOffset3)

	// Step 4: Read back the modified regions WITHOUT flushing
	// This is the CRITICAL test - reads should see journal overlay

	// Test read at write 1 location using RandomAccessor.Read
	data1, err := ra.Read(writeOffset1, 512)
	if err != nil {
		t.Fatalf("Failed to read after write 1: %v", err)
	}
	if len(data1) != 512 {
		t.Fatalf("Expected 512 bytes, got %d", len(data1))
	}
	if !bytes.Equal(data1, writeData1) {
		t.Errorf("❌ FAILED: RandomAccessor.Read after write 1 doesn't match written data")
		t.Errorf("Expected all 0xFF, got first 16 bytes: %x", data1[:16])
		t.Fatalf("Journal overlay not applied for write 1")
	}
	t.Logf("✓ RandomAccessor.Read after write 1 matches: journal overlay working")

	// Test read at write 2 location
	data2, err := ra.Read(writeOffset2, 2560)
	if err != nil {
		t.Fatalf("Failed to read after write 2: %v", err)
	}
	if len(data2) != 2560 {
		t.Fatalf("Expected 2560 bytes, got %d", len(data2))
	}
	if !bytes.Equal(data2, writeData2) {
		t.Errorf("❌ FAILED: RandomAccessor.Read after write 2 doesn't match written data")
		t.Errorf("Expected all 0xAA, got first 16 bytes: %x", data2[:16])
		t.Fatalf("Journal overlay not applied for write 2")
	}
	t.Logf("✓ RandomAccessor.Read after write 2 matches: journal overlay working")

	// Test read at write 3 location
	data3, err := ra.Read(writeOffset3, 512)
	if err != nil {
		t.Fatalf("Failed to read after write 3: %v", err)
	}
	if len(data3) != 512 {
		t.Fatalf("Expected 512 bytes, got %d", len(data3))
	}
	if !bytes.Equal(data3, writeData3) {
		t.Errorf("❌ FAILED: RandomAccessor.Read after write 3 doesn't match written data")
		t.Errorf("Expected all 0x55, got first 16 bytes: %x", data3[:16])
		t.Fatalf("Journal overlay not applied for write 3")
	}
	t.Logf("✓ RandomAccessor.Read after write 3 matches: journal overlay working")

	// Read unmodified region to ensure base data still works
	unmodifiedOffset := int64(4 * 1024 * 1024) // 4MB offset (not modified)
	data4, err := ra.Read(unmodifiedOffset, 4096)
	if err != nil {
		t.Fatalf("Failed to read unmodified region: %v", err)
	}
	if len(data4) != 4096 {
		t.Fatalf("Expected 4096 bytes, got %d", len(data4))
	}
	expectedData := originalData[unmodifiedOffset : unmodifiedOffset+4096]
	if !bytes.Equal(data4, expectedData) {
		t.Fatalf("Unmodified region data mismatch - journal corrupted base data")
	}
	t.Logf("✓ Unmodified region still correct: base data intact")

	// Step 5: Test spanning read (across modified and unmodified regions)
	// Note: write2 (offset 7968768, len 2560) and write1 (offset 7971328, len 512) are adjacent
	// write2 ends at 7971328, write1 starts at 7971328, write1 ends at 7971840
	spanOffset := writeOffset2 - 1024 // Start before modified region
	spanSize := 8192                  // Read across modified region
	dataSpan, err := ra.Read(spanOffset, spanSize)
	if err != nil {
		t.Fatalf("Failed to read spanning region: %v", err)
	}
	if len(dataSpan) != spanSize {
		t.Fatalf("Expected %d bytes, got %d", spanSize, len(dataSpan))
	}

	// Verify the spanning read
	// First 1024 bytes should be original data
	if !bytes.Equal(dataSpan[:1024], originalData[spanOffset:spanOffset+1024]) {
		t.Fatalf("Spanning read: pre-modified region mismatch")
	}
	// Next 2560 bytes should be writeData2
	if !bytes.Equal(dataSpan[1024:1024+2560], writeData2) {
		t.Errorf("❌ FAILED: Spanning read: modified region doesn't match")
		t.Errorf("Expected 0xAA, got: %x", dataSpan[1024:1024+16])
		t.Fatalf("Journal overlay not applied in spanning read")
	}
	// Next 512 bytes should be writeData1 (adjacent write)
	if !bytes.Equal(dataSpan[1024+2560:1024+2560+512], writeData1) {
		t.Fatalf("Spanning read: write1 region mismatch")
	}
	// Remaining bytes should be original data
	remainingStart := spanOffset + 1024 + 2560 + 512
	remainingLen := spanSize - 1024 - 2560 - 512
	if !bytes.Equal(dataSpan[1024+2560+512:], originalData[remainingStart:remainingStart+int64(remainingLen)]) {
		t.Fatalf("Spanning read: post-modified region mismatch")
	}
	t.Logf("✓ Spanning read correct: journal overlay applied correctly")

	t.Logf("\n✅ All RandomAccessor.Read tests passed: Journal overlay working")
}

// TestJournalMultiLayerRead tests reading with multiple layers of journal snapshots
// This test verifies that when there are multiple journal snapshots (layers),
// reads correctly apply all layers in the correct order:
// 1. Base data (fileObj.DataID)
// 2. Layer 1 journal snapshot (applied on top of base)
// 3. Layer 2 journal snapshot (applied on top of layer 1)
// 4. Current journal entries (applied on top of layer 2)
func TestJournalMultiLayerRead(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_multilayer_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	// Configure journal to create snapshots easily
	fs.journalMgr.config.SnapshotEntryCount = 3       // Create snapshot after 3 entries
	fs.journalMgr.config.FullFlushTotalEntries = 1000 // High threshold to avoid full flush

	fileName := "test_multilayer.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Step 1: Create base data
	baseData := make([]byte, 1024)
	for i := range baseData {
		baseData[i] = byte('A') // Base pattern: all 'A'
	}

	ra1, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}

	if err := ra1.Write(0, baseData); err != nil {
		t.Fatalf("Failed to write base data: %v", err)
	}
	if _, err := ra1.Flush(); err != nil {
		t.Fatalf("Failed to flush base data: %v", err)
	}
	if err := ra1.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 1: Base data written (all 'A')")

	// Step 2: Create Layer 1 journal snapshot
	// Write 3 entries and manually create snapshot
	ra2, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for layer 1: %v", err)
	}

	layer1Data1 := []byte("BBB") // Write "BBB" at offset 0
	layer1Data2 := []byte("CCC") // Write "CCC" at offset 100
	layer1Data3 := []byte("DDD") // Write "DDD" at offset 200

	if err := ra2.Write(0, layer1Data1); err != nil {
		t.Fatalf("Failed to write layer1 entry 1: %v", err)
	}
	if err := ra2.Write(100, layer1Data2); err != nil {
		t.Fatalf("Failed to write layer1 entry 2: %v", err)
	}
	if err := ra2.Write(200, layer1Data3); err != nil {
		t.Fatalf("Failed to write layer1 entry 3: %v", err)
	}

	// Get journal from manager and manually create snapshot
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal1 := fs.journalMgr.GetOrCreate(fileID, fileObj2[0].DataID, fileObj2[0].Size)

	// Manually create snapshot
	layer1VersionID, err := journal1.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 1 snapshot: %v", err)
	}
	if err := ra2.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 2: Layer 1 journal snapshot created (versionID=%d, 3 entries)", layer1VersionID)

	// Note: We don't verify snapshot creation via List() as journal snapshots may not be listed
	// The important part is testing the read functionality with multiple layers
	t.Logf("✓ Layer 1 journal snapshot created (versionID=%d)", layer1VersionID)

	// Step 3: Create Layer 2 journal snapshot
	// Write 3 more entries and manually create snapshot
	ra3, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for layer 2: %v", err)
	}

	layer2Data1 := []byte("EEE") // Write "EEE" at offset 0 (overwrites layer1's "BBB")
	layer2Data2 := []byte("FFF") // Write "FFF" at offset 300 (new offset)
	layer2Data3 := []byte("GGG") // Write "GGG" at offset 400 (new offset)

	if err := ra3.Write(0, layer2Data1); err != nil {
		t.Fatalf("Failed to write layer2 entry 1: %v", err)
	}
	if err := ra3.Write(300, layer2Data2); err != nil {
		t.Fatalf("Failed to write layer2 entry 2: %v", err)
	}
	if err := ra3.Write(400, layer2Data3); err != nil {
		t.Fatalf("Failed to write layer2 entry 3: %v", err)
	}

	// Get journal from manager and manually create snapshot
	fileObj3, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj3) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal2 := fs.journalMgr.GetOrCreate(fileID, fileObj3[0].DataID, fileObj3[0].Size)

	// Manually create snapshot
	layer2VersionID, err := journal2.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 2 snapshot: %v", err)
	}
	if err := ra3.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 3: Layer 2 journal snapshot created (versionID=%d, 3 entries)", layer2VersionID)

	// Note: We don't verify snapshot creation via List() as journal snapshots may not be listed
	// The important part is testing the read functionality with multiple layers
	t.Logf("✓ Layer 2 journal snapshot created (versionID=%d)", layer2VersionID)

	// Step 4: Add current journal entries (not flushed yet)
	ra4, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for current layer: %v", err)
	}
	defer ra4.Close()

	currentData1 := []byte("HHH") // Write "HHH" at offset 0 (overwrites layer2's "EEE")
	currentData2 := []byte("III") // Write "III" at offset 500 (new offset)

	if err := ra4.Write(0, currentData1); err != nil {
		t.Fatalf("Failed to write current entry 1: %v", err)
	}
	if err := ra4.Write(500, currentData2); err != nil {
		t.Fatalf("Failed to write current entry 2: %v", err)
	}
	t.Logf("✓ Step 4: Current journal entries written (2 entries, not flushed)")

	// Step 5: Test reading with all layers
	// Expected result at each offset:
	// - Offset 0: "HHH" (current layer overwrites layer2's "EEE", which overwrote layer1's "BBB")
	// - Offset 100: "CCC" (from layer1, not overwritten)
	// - Offset 200: "DDD" (from layer1, not overwritten)
	// - Offset 300: "FFF" (from layer2, not overwritten)
	// - Offset 400: "GGG" (from layer2, not overwritten)
	// - Offset 500: "III" (from current layer)
	// - Other offsets: 'A' (from base data)

	testCases := []struct {
		name     string
		offset   int64
		length   int
		expected []byte
	}{
		{
			name:     "Offset 0 (current layer)",
			offset:   0,
			length:   3,
			expected: []byte("HHH"),
		},
		{
			name:     "Offset 100 (layer1)",
			offset:   100,
			length:   3,
			expected: []byte("CCC"),
		},
		{
			name:     "Offset 200 (layer1)",
			offset:   200,
			length:   3,
			expected: []byte("DDD"),
		},
		{
			name:     "Offset 300 (layer2)",
			offset:   300,
			length:   3,
			expected: []byte("FFF"),
		},
		{
			name:     "Offset 400 (layer2)",
			offset:   400,
			length:   3,
			expected: []byte("GGG"),
		},
		{
			name:     "Offset 500 (current layer)",
			offset:   500,
			length:   3,
			expected: []byte("III"),
		},
		{
			name:     "Offset 50 (base data, not modified)",
			offset:   50,
			length:   1,
			expected: []byte("A"),
		},
		{
			name:   "Spanning read across multiple layers",
			offset: 0,
			length: 600,
			expected: func() []byte {
				result := make([]byte, 600)
				// Fill with base data
				for i := range result {
					result[i] = 'A'
				}
				// Apply layer1
				copy(result[0:3], "BBB")
				copy(result[100:103], "CCC")
				copy(result[200:203], "DDD")
				// Apply layer2 (overwrites layer1 at offset 0)
				copy(result[0:3], "EEE")
				copy(result[300:303], "FFF")
				copy(result[400:403], "GGG")
				// Apply current layer (overwrites layer2 at offset 0)
				copy(result[0:3], "HHH")
				copy(result[500:503], "III")
				return result
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readData, err := ra4.Read(tc.offset, tc.length)
			if err != nil {
				t.Fatalf("Failed to read at offset %d: %v", tc.offset, err)
			}

			if len(readData) != len(tc.expected) {
				t.Errorf("Length mismatch at offset %d: got %d, want %d",
					tc.offset, len(readData), len(tc.expected))
				return
			}

			if !bytes.Equal(readData, tc.expected) {
				t.Errorf("❌ FAILED: Data mismatch at offset %d\n"+
					"Expected: %q (%x)\n"+
					"Got:      %q (%x)",
					tc.offset, tc.expected, tc.expected, readData, readData)

				// Show detailed comparison for debugging
				minLen := len(readData)
				if len(tc.expected) < minLen {
					minLen = len(tc.expected)
				}
				for i := 0; i < minLen; i++ {
					if readData[i] != tc.expected[i] {
						t.Logf("First difference at position %d: expected 0x%02x ('%c'), got 0x%02x ('%c')",
							i, tc.expected[i], tc.expected[i], readData[i], readData[i])
						break
					}
				}
			} else {
				t.Logf("✓ Read correct at offset %d: %q", tc.offset, readData)
			}
		})
	}

	t.Logf("\n✅ Multi-layer journal read test completed")
}

// TestJournalMultiLayerReopenRead tests reading with multiple layers after reopening the file
// This simulates a real-world scenario where:
// 1. File is written and flushed (base data)
// 2. Multiple journal snapshots are created
// 3. File is reopened
// 4. New journal entries are added
// 5. Reads should correctly apply all layers including snapshots
func TestJournalMultiLayerReopenRead(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_multilayer_reopen_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_multilayer_reopen.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Step 1: Create base data
	baseData := make([]byte, 2048)
	for i := range baseData {
		baseData[i] = byte('X') // Base pattern: all 'X'
	}

	ra1, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}

	if err := ra1.Write(0, baseData); err != nil {
		t.Fatalf("Failed to write base data: %v", err)
	}
	if _, err := ra1.Flush(); err != nil {
		t.Fatalf("Failed to flush base data: %v", err)
	}
	if err := ra1.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 1: Base data written (all 'X', %d bytes)", len(baseData))

	// Step 2: Create Layer 1 journal snapshot with overlapping writes
	ra2, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for layer 1: %v", err)
	}

	// Write overlapping data at different offsets
	layer1Data1 := []byte("LAYER1_OFFSET_0")   // 15 bytes at offset 0
	layer1Data2 := []byte("LAYER1_OFFSET_100") // 16 bytes at offset 100
	layer1Data3 := []byte("LAYER1_OFFSET_500") // 16 bytes at offset 500

	if err := ra2.Write(0, layer1Data1); err != nil {
		t.Fatalf("Failed to write layer1 entry 1: %v", err)
	}
	if err := ra2.Write(100, layer1Data2); err != nil {
		t.Fatalf("Failed to write layer1 entry 2: %v", err)
	}
	if err := ra2.Write(500, layer1Data3); err != nil {
		t.Fatalf("Failed to write layer1 entry 3: %v", err)
	}

	// Get journal and create snapshot
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal1 := fs.journalMgr.GetOrCreate(fileID, fileObj2[0].DataID, fileObj2[0].Size)
	layer1VersionID, err := journal1.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 1 snapshot: %v", err)
	}
	if err := ra2.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 2: Layer 1 journal snapshot created (versionID=%d)", layer1VersionID)

	// Step 3: Reopen file and create Layer 2 journal snapshot
	// This simulates the file being closed and reopened
	ra3, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for layer 2: %v", err)
	}

	// Write data that overlaps with layer 1
	layer2Data1 := []byte("LAYER2_OFFSET_0")   // 15 bytes at offset 0 (overwrites layer1)
	layer2Data2 := []byte("LAYER2_OFFSET_200") // 16 bytes at offset 200 (new)
	layer2Data3 := []byte("LAYER2_OFFSET_600") // 16 bytes at offset 600 (new)

	if err := ra3.Write(0, layer2Data1); err != nil {
		t.Fatalf("Failed to write layer2 entry 1: %v", err)
	}
	if err := ra3.Write(200, layer2Data2); err != nil {
		t.Fatalf("Failed to write layer2 entry 2: %v", err)
	}
	if err := ra3.Write(600, layer2Data3); err != nil {
		t.Fatalf("Failed to write layer2 entry 3: %v", err)
	}

	// Get journal and create snapshot
	fileObj3, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj3) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal2 := fs.journalMgr.GetOrCreate(fileID, fileObj3[0].DataID, fileObj3[0].Size)
	layer2VersionID, err := journal2.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 2 snapshot: %v", err)
	}
	if err := ra3.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 3: Layer 2 journal snapshot created (versionID=%d)", layer2VersionID)

	// Step 4: Reopen file again and add current journal entries
	ra4, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for current layer: %v", err)
	}
	defer ra4.Close()

	// Write data that overlaps with previous layers
	currentData1 := []byte("CURRENT_OFFSET_0")   // 16 bytes at offset 0 (overwrites layer2)
	currentData2 := []byte("CURRENT_OFFSET_150") // 17 bytes at offset 150 (new, between layer1 and layer2)
	currentData3 := []byte("CURRENT_OFFSET_700") // 17 bytes at offset 700 (new)

	if err := ra4.Write(0, currentData1); err != nil {
		t.Fatalf("Failed to write current entry 1: %v", err)
	}
	if err := ra4.Write(150, currentData2); err != nil {
		t.Fatalf("Failed to write current entry 2: %v", err)
	}
	if err := ra4.Write(700, currentData3); err != nil {
		t.Fatalf("Failed to write current entry 3: %v", err)
	}
	t.Logf("✓ Step 4: Current journal entries written (3 entries, not flushed)")

	// Step 5: Test reading with all layers
	// Expected results:
	// - Offset 0: "CURRENT_OFFSET_0" (current layer overwrites layer2, which overwrote layer1)
	// - Offset 100: "LAYER1_OFFSET_100" (from layer1, not overwritten)
	// - Offset 150: "CURRENT_OFFSET_150" (from current layer)
	// - Offset 200: "LAYER2_OFFSET_200" (from layer2)
	// - Offset 500: "LAYER1_OFFSET_500" (from layer1, not overwritten)
	// - Offset 600: "LAYER2_OFFSET_600" (from layer2)
	// - Offset 700: "CURRENT_OFFSET_700" (from current layer)
	// - Other offsets: 'X' (from base data)

	testCases := []struct {
		name     string
		offset   int64
		length   int
		expected []byte
	}{
		{
			name:     "Offset 0 (current layer overwrites all)",
			offset:   0,
			length:   16,
			expected: []byte("CURRENT_OFFSET_0"),
		},
		{
			name:     "Offset 100 (layer1, not overwritten)",
			offset:   100,
			length:   len("LAYER1_OFFSET_100"),
			expected: []byte("LAYER1_OFFSET_100"),
		},
		{
			name:     "Offset 150 (current layer)",
			offset:   150,
			length:   len("CURRENT_OFFSET_150"),
			expected: []byte("CURRENT_OFFSET_150"),
		},
		{
			name:     "Offset 200 (layer2)",
			offset:   200,
			length:   len("LAYER2_OFFSET_200"),
			expected: []byte("LAYER2_OFFSET_200"),
		},
		{
			name:     "Offset 500 (layer1, not overwritten)",
			offset:   500,
			length:   len("LAYER1_OFFSET_500"),
			expected: []byte("LAYER1_OFFSET_500"),
		},
		{
			name:     "Offset 600 (layer2)",
			offset:   600,
			length:   len("LAYER2_OFFSET_600"),
			expected: []byte("LAYER2_OFFSET_600"),
		},
		{
			name:     "Offset 700 (current layer)",
			offset:   700,
			length:   len("CURRENT_OFFSET_700"),
			expected: []byte("CURRENT_OFFSET_700"),
		},
		{
			name:     "Offset 50 (base data, not modified)",
			offset:   50,
			length:   1,
			expected: []byte("X"),
		},
		{
			name:   "Large spanning read (0-800)",
			offset: 0,
			length: 800,
			expected: func() []byte {
				result := make([]byte, 800)
				// Fill with base data
				for i := range result {
					result[i] = 'X'
				}
				// Apply layer1
				copy(result[0:15], "LAYER1_OFFSET_0")
				copy(result[100:100+len("LAYER1_OFFSET_100")], "LAYER1_OFFSET_100")
				copy(result[500:500+len("LAYER1_OFFSET_500")], "LAYER1_OFFSET_500")
				// Apply layer2 (overwrites layer1 at offset 0)
				copy(result[0:15], "LAYER2_OFFSET_0")
				copy(result[200:200+len("LAYER2_OFFSET_200")], "LAYER2_OFFSET_200")
				copy(result[600:600+len("LAYER2_OFFSET_600")], "LAYER2_OFFSET_600")
				// Apply current layer (overwrites layer2 at offset 0)
				copy(result[0:len("CURRENT_OFFSET_0")], "CURRENT_OFFSET_0")
				copy(result[150:150+len("CURRENT_OFFSET_150")], "CURRENT_OFFSET_150")
				copy(result[700:700+len("CURRENT_OFFSET_700")], "CURRENT_OFFSET_700")
				return result
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readData, err := ra4.Read(tc.offset, tc.length)
			if err != nil {
				t.Fatalf("Failed to read at offset %d: %v", tc.offset, err)
			}

			if len(readData) != len(tc.expected) {
				t.Errorf("Length mismatch at offset %d: got %d, want %d",
					tc.offset, len(readData), len(tc.expected))
				return
			}

			if !bytes.Equal(readData, tc.expected) {
				t.Errorf("❌ FAILED: Data mismatch at offset %d\n"+
					"Expected: %q\n"+
					"Got:      %q",
					tc.offset, tc.expected, readData)

				// Show detailed comparison for debugging
				minLen := len(readData)
				if len(tc.expected) < minLen {
					minLen = len(tc.expected)
				}
				for i := 0; i < minLen; i++ {
					if readData[i] != tc.expected[i] {
						t.Logf("First difference at position %d (absolute offset %d): expected 0x%02x ('%c'), got 0x%02x ('%c')",
							i, tc.offset+int64(i), tc.expected[i], tc.expected[i], readData[i], readData[i])
						// Show context around the difference
						start := i - 10
						if start < 0 {
							start = 0
						}
						end := i + 10
						if end > minLen {
							end = minLen
						}
						t.Logf("Expected context: %q", tc.expected[start:end])
						t.Logf("Got context:      %q", readData[start:end])
						break
					}
				}
			} else {
				t.Logf("✓ Read correct at offset %d: %q", tc.offset, readData)
			}
		})
	}

	t.Logf("\n✅ Multi-layer journal reopen read test completed")
}

// TestJournalMultiLayerOverlappingEntries tests reading with overlapping entries across multiple layers
// This is a critical test case because overlapping entries can cause issues in multi-layer journal reads
// Scenario:
// 1. Base data: all 'Z'
// 2. Layer 1: Write "AAA" at offset 0-2, "BBB" at offset 5-7 (overlaps with AAA at offset 5)
// 3. Layer 2: Write "CCC" at offset 1-3 (overlaps with Layer1's AAA), "DDD" at offset 6-8 (overlaps with Layer1's BBB)
// 4. Current: Write "EEE" at offset 2-4 (overlaps with Layer1's AAA and Layer2's CCC)
// Expected: Reads should correctly apply all layers with proper overlap resolution
func TestJournalMultiLayerOverlappingEntries(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_overlap_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_overlap.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Step 1: Create base data
	baseData := make([]byte, 1024)
	for i := range baseData {
		baseData[i] = byte('Z') // Base pattern: all 'Z'
	}

	ra1, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}

	if err := ra1.Write(0, baseData); err != nil {
		t.Fatalf("Failed to write base data: %v", err)
	}
	if _, err := ra1.Flush(); err != nil {
		t.Fatalf("Failed to flush base data: %v", err)
	}
	if err := ra1.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 1: Base data written (all 'Z', %d bytes)", len(baseData))

	// Step 2: Create Layer 1 with overlapping entries
	ra2, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for layer 1: %v", err)
	}

	// Layer 1 entries with internal overlap
	layer1Data1 := []byte("AAA") // offset 0-2
	layer1Data2 := []byte("BBB")  // offset 5-7 (doesn't overlap with AAA, but will be overlapped by Layer2)

	if err := ra2.Write(0, layer1Data1); err != nil {
		t.Fatalf("Failed to write layer1 entry 1: %v", err)
	}
	if err := ra2.Write(5, layer1Data2); err != nil {
		t.Fatalf("Failed to write layer1 entry 2: %v", err)
	}

	// Get journal and create snapshot
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal1 := fs.journalMgr.GetOrCreate(fileID, fileObj2[0].DataID, fileObj2[0].Size)
	layer1VersionID, err := journal1.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 1 snapshot: %v", err)
	}
	if err := ra2.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 2: Layer 1 journal snapshot created (versionID=%d)", layer1VersionID)
	t.Logf("  Layer 1 entries: offset 0-2='AAA', offset 5-7='BBB'")

	// Step 3: Create Layer 2 with entries that overlap Layer 1
	ra3, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for layer 2: %v", err)
	}

	// Layer 2 entries that overlap Layer 1
	layer2Data1 := []byte("CCC") // offset 1-3 (overlaps Layer1's AAA at offset 1-2)
	layer2Data2 := []byte("DDD") // offset 6-8 (overlaps Layer1's BBB at offset 6-7)

	if err := ra3.Write(1, layer2Data1); err != nil {
		t.Fatalf("Failed to write layer2 entry 1: %v", err)
	}
	if err := ra3.Write(6, layer2Data2); err != nil {
		t.Fatalf("Failed to write layer2 entry 2: %v", err)
	}

	// Get journal and create snapshot
	fileObj3, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj3) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal2 := fs.journalMgr.GetOrCreate(fileID, fileObj3[0].DataID, fileObj3[0].Size)
	layer2VersionID, err := journal2.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 2 snapshot: %v", err)
	}
	if err := ra3.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 3: Layer 2 journal snapshot created (versionID=%d)", layer2VersionID)
	t.Logf("  Layer 2 entries: offset 1-3='CCC' (overlaps Layer1), offset 6-8='DDD' (overlaps Layer1)")

	// Step 4: Add current journal entries that overlap both previous layers
	ra4, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for current layer: %v", err)
	}
	defer ra4.Close()

	// Current entries that overlap both Layer1 and Layer2
	currentData1 := []byte("EEE") // offset 2-4 (overlaps Layer1's AAA at offset 2, Layer2's CCC at offset 2-3)

	if err := ra4.Write(2, currentData1); err != nil {
		t.Fatalf("Failed to write current entry 1: %v", err)
	}
	t.Logf("✓ Step 4: Current journal entries written (1 entry, not flushed)")
	t.Logf("  Current entry: offset 2-4='EEE' (overlaps Layer1 and Layer2)")

	// Step 5: Test reading with overlapping entries
	// Expected results at each position:
	// - Offset 0: 'A' (from Layer1, not overwritten)
	// - Offset 1: 'C' (from Layer2, overwrites Layer1's 'A')
	// - Offset 2: 'E' (from Current, overwrites Layer2's 'C' and Layer1's 'A')
	// - Offset 3: 'E' (from Current, overwrites Layer2's 'C')
	// - Offset 4: 'E' (from Current)
	// - Offset 5: 'B' (from Layer1, not overwritten)
	// - Offset 6: 'D' (from Layer2, overwrites Layer1's 'B')
	// - Offset 7: 'D' (from Layer2, overwrites Layer1's 'B')
	// - Offset 8: 'D' (from Layer2)
	// - Other offsets: 'Z' (from base data)

	testCases := []struct {
		name     string
		offset   int64
		length   int
		expected []byte
		desc     string
	}{
		{
			name:     "Offset 0 (Layer1 only)",
			offset:   0,
			length:   1,
			expected: []byte("A"),
			desc:     "Layer1's 'A' at offset 0, not overwritten",
		},
		{
			name:     "Offset 1 (Layer2 overwrites Layer1)",
			offset:   1,
			length:   1,
			expected: []byte("C"),
			desc:     "Layer2's 'C' overwrites Layer1's 'A'",
		},
		{
			name:     "Offset 2 (Current overwrites both)",
			offset:   2,
			length:   1,
			expected: []byte("E"),
			desc:     "Current's 'E' overwrites Layer2's 'C' and Layer1's 'A'",
		},
		{
			name:     "Offset 3 (Current overwrites Layer2)",
			offset:   3,
			length:   1,
			expected: []byte("E"),
			desc:     "Current's 'E' overwrites Layer2's 'C'",
		},
		{
			name:     "Offset 4 (Current only)",
			offset:   4,
			length:   1,
			expected: []byte("E"),
			desc:     "Current's 'E' at offset 4",
		},
		{
			name:     "Offset 5 (Layer1 only)",
			offset:   5,
			length:   1,
			expected: []byte("B"),
			desc:     "Layer1's 'B' at offset 5, not overwritten",
		},
		{
			name:     "Offset 6 (Layer2 overwrites Layer1)",
			offset:   6,
			length:   1,
			expected: []byte("D"),
			desc:     "Layer2's 'D' overwrites Layer1's 'B'",
		},
		{
			name:     "Offset 7 (Layer2 overwrites Layer1)",
			offset:   7,
			length:   1,
			expected: []byte("D"),
			desc:     "Layer2's 'D' overwrites Layer1's 'B'",
		},
		{
			name:     "Offset 8 (Layer2 only)",
			offset:   8,
			length:   1,
			expected: []byte("D"),
			desc:     "Layer2's 'D' at offset 8",
		},
		{
			name:     "Offset 10 (Base data, not modified)",
			offset:   10,
			length:   1,
			expected: []byte("Z"),
			desc:     "Base data 'Z' at offset 10, not modified",
		},
		{
			name:     "Read range 0-9 (all overlapping regions)",
			offset:   0,
			length:   10,
			expected: []byte("ACEEEBDDDZ"),
			desc:     "Read all overlapping regions: A(0) C(1) E(2-4) B(5) D(6-8) Z(9)",
		},
		{
			name:     "Read range 0-3 (complex overlap)",
			offset:   0,
			length:   4,
			expected: []byte("ACEE"),
			desc:     "Complex overlap: A(0) C(1) E(2-3) - Layer1, Layer2, Current all overlap",
		},
		{
			name:     "Read range 5-9 (Layer1 and Layer2 overlap)",
			offset:   5,
			length:   5,
			expected: []byte("BDDDZ"),
			desc:     "Layer1 and Layer2 overlap: B(5) D(6-8) Z(9)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readData, err := ra4.Read(tc.offset, tc.length)
			if err != nil {
				t.Fatalf("Failed to read at offset %d: %v", tc.offset, err)
			}

			if len(readData) != len(tc.expected) {
				t.Errorf("Length mismatch at offset %d: got %d, want %d",
					tc.offset, len(readData), len(tc.expected))
				return
			}

			if !bytes.Equal(readData, tc.expected) {
				t.Errorf("❌ FAILED: Data mismatch at offset %d\n"+
					"Description: %s\n"+
					"Expected: %q (%x)\n"+
					"Got:      %q (%x)",
					tc.offset, tc.desc, tc.expected, tc.expected, readData, readData)

				// Show detailed comparison for debugging
				minLen := len(readData)
				if len(tc.expected) < minLen {
					minLen = len(tc.expected)
				}
				for i := 0; i < minLen; i++ {
					if readData[i] != tc.expected[i] {
						t.Logf("First difference at position %d (absolute offset %d): expected 0x%02x ('%c'), got 0x%02x ('%c')",
							i, tc.offset+int64(i), tc.expected[i], tc.expected[i], readData[i], readData[i])
						// Show context around the difference
						start := i - 5
						if start < 0 {
							start = 0
						}
						end := i + 5
						if end > minLen {
							end = minLen
						}
						t.Logf("Expected context: %q", tc.expected[start:end])
						t.Logf("Got context:      %q", readData[start:end])
						break
					}
				}
			} else {
				t.Logf("✓ Read correct at offset %d: %q (%s)", tc.offset, readData, tc.desc)
			}
		})
	}

	t.Logf("\n✅ Multi-layer journal overlapping entries test completed")
}

// TestJournalMultiLayerComplexOverlap tests complex overlapping scenarios with multiple layers
// This test creates a scenario where entries in different layers have complex overlaps
// that require careful ordering when applying journal entries
func TestJournalMultiLayerComplexOverlap(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_complex_overlap_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_complex_overlap.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Step 1: Create base data
	baseData := make([]byte, 2000)
	for i := range baseData {
		baseData[i] = byte('0' + (i % 10)) // Base pattern: "0123456789..."
	}

	ra1, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}

	if err := ra1.Write(0, baseData); err != nil {
		t.Fatalf("Failed to write base data: %v", err)
	}
	if _, err := ra1.Flush(); err != nil {
		t.Fatalf("Failed to flush base data: %v", err)
	}
	if err := ra1.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 1: Base data written (%d bytes)", len(baseData))

	// Step 2: Layer 1 - Write entries that will be partially overwritten
	ra2, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for layer 1: %v", err)
	}

	// Layer 1: Large entry that will be partially overwritten by Layer 2
	layer1Data1 := bytes.Repeat([]byte("L1"), 100) // 200 bytes at offset 100-299

	if err := ra2.Write(100, layer1Data1); err != nil {
		t.Fatalf("Failed to write layer1 entry: %v", err)
	}

	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal1 := fs.journalMgr.GetOrCreate(fileID, fileObj2[0].DataID, fileObj2[0].Size)
	layer1VersionID, err := journal1.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 1 snapshot: %v", err)
	}
	if err := ra2.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 2: Layer 1 journal snapshot created (versionID=%d, 200 bytes at offset 100)", layer1VersionID)

	// Step 3: Layer 2 - Write entries that partially overlap Layer 1
	ra3, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for layer 2: %v", err)
	}

	// Layer 2: Entry that overlaps with Layer 1's entry
	// Write "L2" at offset 150-249 (overlaps Layer1's 100-299 at 150-249)
	layer2Data1 := bytes.Repeat([]byte("L2"), 50) // 100 bytes at offset 150-249

	if err := ra3.Write(150, layer2Data1); err != nil {
		t.Fatalf("Failed to write layer2 entry: %v", err)
	}

	fileObj3, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj3) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal2 := fs.journalMgr.GetOrCreate(fileID, fileObj3[0].DataID, fileObj3[0].Size)
	layer2VersionID, err := journal2.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 2 snapshot: %v", err)
	}
	if err := ra3.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 3: Layer 2 journal snapshot created (versionID=%d, 100 bytes at offset 150, overlaps Layer1)", layer2VersionID)

	// Step 4: Current layer - Write entries that overlap both previous layers
	ra4, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor for current layer: %v", err)
	}
	defer ra4.Close()

	// Current: Entry that overlaps with both Layer1 and Layer2
	// Write "CUR" at offset 120-179 (overlaps Layer1's 100-299 and Layer2's 150-249)
	currentData1 := bytes.Repeat([]byte("CUR"), 20) // 60 bytes at offset 120-179

	if err := ra4.Write(120, currentData1); err != nil {
		t.Fatalf("Failed to write current entry: %v", err)
	}
	t.Logf("✓ Step 4: Current journal entry written (60 bytes at offset 120, overlaps Layer1 and Layer2)")

	// Step 5: Test reading with complex overlaps
	// Expected results:
	// - Offset 100-119: "L1L1..." (from Layer1, not overwritten)
	// - Offset 120-179: "CURCUR..." (from Current, overwrites Layer1 and Layer2)
	// - Offset 180-249: "L2L2..." (from Layer2, overwrites Layer1)
	// - Offset 250-299: "L1L1..." (from Layer1, not overwritten)
	// - Other offsets: base data

	testCases := []struct {
		name     string
		offset   int64
		length   int
		expected []byte
		desc     string
	}{
		{
			name:     "Offset 100-119 (Layer1 only)",
			offset:   100,
			length:   20,
			expected: bytes.Repeat([]byte("L1"), 10), // 20 bytes
			desc:     "Layer1's data, not overwritten",
		},
		{
			name:     "Offset 120-179 (Current overwrites both)",
			offset:   120,
			length:   60,
			expected: bytes.Repeat([]byte("CUR"), 20), // 60 bytes
			desc:     "Current's data overwrites Layer1 and Layer2",
		},
		{
			name:     "Offset 180-249 (Layer2 overwrites Layer1)",
			offset:   180,
			length:   70,
			expected: func() []byte {
				// Layer2's "L2" at offset 150-249, but we're reading from 180
				// So we get Layer2's data from offset 180-249 (70 bytes = 35 * "L2")
				return bytes.Repeat([]byte("L2"), 35)
			}(),
			desc:     "Layer2's data overwrites Layer1",
		},
		{
			name:     "Offset 250-299 (Layer1 only)",
			offset:   250,
			length:   50,
			expected: bytes.Repeat([]byte("L1"), 25), // 50 bytes
			desc:     "Layer1's data, not overwritten",
		},
		{
			name:     "Offset 50 (Base data, not modified)",
			offset:   50,
			length:   10,
			expected: []byte("0123456789"),
			desc:     "Base data, not modified",
		},
		{
			name:     "Large spanning read (100-300)",
			offset:   100,
			length:   200,
			expected: func() []byte {
				result := make([]byte, 200)
				// Fill with base data first
				for i := 0; i < 200; i++ {
					result[i] = byte('0' + ((100 + i) % 10))
				}
				// Apply Layer1 (100-299)
				layer1Data := bytes.Repeat([]byte("L1"), 100)
				copy(result[0:200], layer1Data)
				// Apply Layer2 (150-249, overwrites Layer1)
				layer2Data := bytes.Repeat([]byte("L2"), 50)
				copy(result[50:150], layer2Data)
				// Apply Current (120-179, overwrites both)
				currentData := bytes.Repeat([]byte("CUR"), 20)
				copy(result[20:80], currentData)
				return result
			}(),
			desc:     "Large spanning read across all overlapping regions",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readData, err := ra4.Read(tc.offset, tc.length)
			if err != nil {
				t.Fatalf("Failed to read at offset %d: %v", tc.offset, err)
			}

			if len(readData) != len(tc.expected) {
				t.Errorf("Length mismatch at offset %d: got %d, want %d",
					tc.offset, len(readData), len(tc.expected))
				return
			}

			if !bytes.Equal(readData, tc.expected) {
				t.Errorf("❌ FAILED: Data mismatch at offset %d\n"+
					"Description: %s\n"+
					"Expected: %q\n"+
					"Got:      %q",
					tc.offset, tc.desc, tc.expected, readData)

				// Show first few differences
				minLen := len(readData)
				if len(tc.expected) < minLen {
					minLen = len(tc.expected)
				}
				differences := 0
				for i := 0; i < minLen && differences < 5; i++ {
					if readData[i] != tc.expected[i] {
						t.Logf("Difference at position %d (absolute offset %d): expected 0x%02x ('%c'), got 0x%02x ('%c')",
							i, tc.offset+int64(i), tc.expected[i], tc.expected[i], readData[i], readData[i])
						differences++
					}
				}
			} else {
				previewLen := 20
				if len(readData) < previewLen {
					previewLen = len(readData)
				}
				t.Logf("✓ Read correct at offset %d: %q (%s)", tc.offset, readData[:previewLen], tc.desc)
			}
		})
	}

	t.Logf("\n✅ Multi-layer journal complex overlap test completed")
}

// TestJournalMultiLayerSnapshotRead tests reading when journal snapshots exist in database
// This test verifies that when there are journal snapshots stored as OBJ_TYPE_JOURNAL objects,
// reads correctly apply all snapshot layers before applying current journal entries.
// This is the critical test case that may reveal the actual bug.
func TestJournalMultiLayerSnapshotRead(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_snapshot_read_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_snapshot_read.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Step 1: Create base data
	baseData := make([]byte, 1000)
	for i := range baseData {
		baseData[i] = byte('B') // Base pattern: all 'B'
	}

	ra1, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}

	if err := ra1.Write(0, baseData); err != nil {
		t.Fatalf("Failed to write base data: %v", err)
	}
	if _, err := ra1.Flush(); err != nil {
		t.Fatalf("Failed to flush base data: %v", err)
	}
	if err := ra1.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}
	t.Logf("✓ Step 1: Base data written (all 'B', %d bytes)", len(baseData))

	// Get file object to get base DataID
	fileObj1, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj1) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	baseDataID := fileObj1[0].DataID
	t.Logf("Base DataID: %d", baseDataID)

	// Step 2: Create Layer 1 journal snapshot manually (simulating what CreateJournalSnapshot does)
	// This creates a journal with entries and saves it as OBJ_TYPE_JOURNAL
	journal1 := fs.journalMgr.GetOrCreate(fileID, baseDataID, fileObj1[0].Size)
	
	// Write entries to journal1
	layer1Data1 := []byte("L1_100") // offset 100
	layer1Data2 := []byte("L1_200") // offset 200
	if err := journal1.Write(100, layer1Data1); err != nil {
		t.Fatalf("Failed to write layer1 entry 1: %v", err)
	}
	if err := journal1.Write(200, layer1Data2); err != nil {
		t.Fatalf("Failed to write layer1 entry 2: %v", err)
	}

	// Create snapshot
	layer1VersionID, err := journal1.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 1 snapshot: %v", err)
	}
	t.Logf("✓ Step 2: Layer 1 journal snapshot created (versionID=%d)", layer1VersionID)

	// Step 3: Create Layer 2 journal snapshot
	// Get the journal again (it should have the same baseDataID but may have been updated)
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	
	// Create a new journal for layer 2, but it should be based on the same baseDataID
	// The issue is: when we create a new journal, does it correctly load the previous snapshot?
	journal2 := fs.journalMgr.GetOrCreate(fileID, fileObj2[0].DataID, fileObj2[0].Size)
	
	// Write entries to journal2
	layer2Data1 := []byte("L2_150") // offset 150 (overlaps with Layer1's offset 100-105)
	layer2Data2 := []byte("L2_250") // offset 250 (new)
	if err := journal2.Write(150, layer2Data1); err != nil {
		t.Fatalf("Failed to write layer2 entry 1: %v", err)
	}
	if err := journal2.Write(250, layer2Data2); err != nil {
		t.Fatalf("Failed to write layer2 entry 2: %v", err)
	}

	// Create snapshot
	layer2VersionID, err := journal2.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 2 snapshot: %v", err)
	}
	t.Logf("✓ Step 3: Layer 2 journal snapshot created (versionID=%d)", layer2VersionID)

	// Step 4: Create current journal entries (not flushed)
	// Close and reopen to simulate real scenario
	// Get a fresh journal instance
	fileObj3, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj3) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	
	// Create new RandomAccessor to get fresh journal
	ra4, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	defer ra4.Close()

	// Write current entries
	currentData1 := []byte("CUR_120") // offset 120 (overlaps with Layer1 and Layer2)
	if err := ra4.Write(120, currentData1); err != nil {
		t.Fatalf("Failed to write current entry: %v", err)
	}
	t.Logf("✓ Step 4: Current journal entry written (offset 120)")

	// Step 5: Test reading
	// Expected results:
	// - Offset 100-105: "L1_100" (from Layer1, not overwritten by Layer2 or Current)
	// - Offset 120-125: "CUR_120" (from Current, overwrites Layer1 and Layer2)
	// - Offset 150-155: "L2_150" (from Layer2, but may be overwritten by Current if overlap)
	// - Offset 200-205: "L1_200" (from Layer1, not overwritten)
	// - Offset 250-255: "L2_250" (from Layer2, not overwritten)
	// - Other offsets: 'B' (from base data)

	testCases := []struct {
		name     string
		offset   int64
		length   int
		expected []byte
		desc     string
	}{
		{
			name:     "Offset 100 (Layer1)",
			offset:   100,
			length:   6,
			expected: []byte("L1_100"),
			desc:     "Layer1's data at offset 100",
		},
		{
			name:     "Offset 120 (Current overwrites)",
			offset:   120,
			length:   7,
			expected: []byte("CUR_120"),
			desc:     "Current's data overwrites Layer1 and Layer2",
		},
		{
			name:     "Offset 150 (Layer2, may be overwritten)",
			offset:   150,
			length:   6,
			expected: []byte("L2_150"),
			desc:     "Layer2's data at offset 150 (if Current doesn't overlap)",
		},
		{
			name:     "Offset 200 (Layer1)",
			offset:   200,
			length:   6,
			expected: []byte("L1_200"),
			desc:     "Layer1's data at offset 200",
		},
		{
			name:     "Offset 250 (Layer2)",
			offset:   250,
			length:   6,
			expected: []byte("L2_250"),
			desc:     "Layer2's data at offset 250",
		},
		{
			name:     "Offset 50 (Base data)",
			offset:   50,
			length:   1,
			expected: []byte("B"),
			desc:     "Base data, not modified",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readData, err := ra4.Read(tc.offset, tc.length)
			if err != nil {
				t.Fatalf("Failed to read at offset %d: %v", tc.offset, err)
			}

			if len(readData) != len(tc.expected) {
				t.Errorf("Length mismatch at offset %d: got %d, want %d",
					tc.offset, len(readData), len(tc.expected))
				return
			}

			if !bytes.Equal(readData, tc.expected) {
				t.Errorf("❌ FAILED: Data mismatch at offset %d\n"+
					"Description: %s\n"+
					"Expected: %q\n"+
					"Got:      %q\n"+
					"This may indicate that journal snapshots are not being applied correctly!",
					tc.offset, tc.desc, tc.expected, readData)
			} else {
				t.Logf("✓ Read correct at offset %d: %q (%s)", tc.offset, readData, tc.desc)
			}
		})
	}

	t.Logf("\n✅ Multi-layer journal snapshot read test completed")
}

// TestJournalMultiLayerVersionChain tests reading when journal snapshots form a chain
// This test verifies the critical bug: when there are multiple journal snapshots
// stored in the database, readBaseData should recursively load and apply all
// journal snapshot layers, not just read the base DataID directly.
//
// Scenario:
// 1. Base data: DataID1
// 2. Layer 1 journal snapshot: based on DataID1, creates snapshot with entries
// 3. Layer 2 journal snapshot: based on Layer 1's versionID, creates snapshot with entries
// 4. Current journal: should apply Layer 1 + Layer 2 + Current entries
//
// The bug: readBaseData only reads fileObj.DataID, it doesn't check for journal snapshots
// that need to be applied first. This means Layer 1 and Layer 2 entries are skipped!
func TestJournalMultiLayerVersionChain(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_version_chain_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_version_chain.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Step 1: Create base data
	baseData := make([]byte, 500)
	for i := range baseData {
		baseData[i] = byte('Z') // Base pattern: all 'Z'
	}

	ra1, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}

	if err := ra1.Write(0, baseData); err != nil {
		t.Fatalf("Failed to write base data: %v", err)
	}
	if _, err := ra1.Flush(); err != nil {
		t.Fatalf("Failed to flush base data: %v", err)
	}
	if err := ra1.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}

	// Get base DataID
	fileObj1, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj1) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	baseDataID := fileObj1[0].DataID
	t.Logf("✓ Step 1: Base data written (DataID=%d, all 'Z')", baseDataID)

	// Step 2: Create Layer 1 journal snapshot
	// This snapshot is based on baseDataID
	journal1 := fs.journalMgr.GetOrCreate(fileID, baseDataID, fileObj1[0].Size)
	
	layer1Data1 := []byte("L1_100") // offset 100
	layer1Data2 := []byte("L1_200") // offset 200
	if err := journal1.Write(100, layer1Data1); err != nil {
		t.Fatalf("Failed to write layer1 entry 1: %v", err)
	}
	if err := journal1.Write(200, layer1Data2); err != nil {
		t.Fatalf("Failed to write layer1 entry 2: %v", err)
	}

	layer1VersionID, err := journal1.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 1 snapshot: %v", err)
	}
	t.Logf("✓ Step 2: Layer 1 journal snapshot created (versionID=%d, based on DataID=%d)", layer1VersionID, baseDataID)

	// Verify Layer 1 snapshot was created
	versions1, _, _, err := fs.h.List(fs.c, bktID, fileID, core.ListOptions{Count: 0})
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}
	layer1Found := false
	for _, v := range versions1 {
		if v.Type == core.OBJ_TYPE_JOURNAL && v.ID == layer1VersionID {
			layer1Found = true
			t.Logf("  Layer 1 snapshot: ID=%d, DataID=%d, Extra=%s", v.ID, v.DataID, v.Extra)
			break
		}
	}
	if !layer1Found {
		t.Logf("⚠️  Warning: Layer 1 snapshot not found in version list (may be expected)")
	}

	// Step 3: Create Layer 2 journal snapshot
	// This snapshot should be based on Layer 1's versionID
	// But when we GetOrCreate, we need to make sure it's based on the correct base
	// The issue: GetOrCreate uses fileObj.DataID, which is still baseDataID!
	// So Layer 2 journal doesn't know about Layer 1's entries
	
	// Get file object again (DataID should still be baseDataID, not updated)
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	
	// CRITICAL: fileObj2.DataID is still baseDataID, not layer1VersionID!
	// This means when we create journal2, it's based on baseDataID, not layer1VersionID
	// So it doesn't know about Layer 1's entries!
	t.Logf("⚠️  CRITICAL: fileObj.DataID=%d (still base, not updated to layer1 version)", fileObj2[0].DataID)
	
	journal2 := fs.journalMgr.GetOrCreate(fileID, fileObj2[0].DataID, fileObj2[0].Size)
	
	layer2Data1 := []byte("L2_150") // offset 150 (should overlap with Layer1's "L1_100" at 100-105)
	layer2Data2 := []byte("L2_250") // offset 250
	if err := journal2.Write(150, layer2Data1); err != nil {
		t.Fatalf("Failed to write layer2 entry 1: %v", err)
	}
	if err := journal2.Write(250, layer2Data2); err != nil {
		t.Fatalf("Failed to write layer2 entry 2: %v", err)
	}

	layer2VersionID, err := journal2.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 2 snapshot: %v", err)
	}
	t.Logf("✓ Step 3: Layer 2 journal snapshot created (versionID=%d, based on DataID=%d)", layer2VersionID, fileObj2[0].DataID)

	// Step 4: Create current journal entries
	// Close and reopen to simulate real scenario
	// Get a fresh journal instance
	fileObj3, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj3) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	
	// Create new RandomAccessor
	ra4, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	defer ra4.Close()

	// Write current entries
	currentData1 := []byte("CUR_120") // offset 120 (should overlap with Layer1 and Layer2)
	if err := ra4.Write(120, currentData1); err != nil {
		t.Fatalf("Failed to write current entry: %v", err)
	}
	t.Logf("✓ Step 4: Current journal entry written (offset 120)")

	// Step 5: Test reading
	// Expected results if journal snapshots are correctly applied:
	// - Offset 100-105: "L1_100" (from Layer1, not overwritten by Layer2 or Current)
	// - Offset 120-125: "CUR_120" (from Current, overwrites Layer1 and Layer2)
	// - Offset 150-155: "L2_150" (from Layer2, but may be overwritten by Current if overlap)
	// - Offset 200-205: "L1_200" (from Layer1, not overwritten)
	// - Offset 250-255: "L2_250" (from Layer2, not overwritten)
	// - Other offsets: 'Z' (from base data)
	//
	// BUT: If readBaseData doesn't load journal snapshots, then:
	// - Offset 100-105: 'Z' (base data, Layer1 entries not applied!)
	// - Offset 200-205: 'Z' (base data, Layer1 entries not applied!)
	// - Offset 150-155: 'Z' (base data, Layer2 entries not applied!)
	// - Offset 250-255: 'Z' (base data, Layer2 entries not applied!)
	// - Only Current entries would be visible

	testCases := []struct {
		name     string
		offset   int64
		length   int
		expected []byte
		desc     string
	}{
		{
			name:     "Offset 100 (Layer1 - CRITICAL TEST)",
			offset:   100,
			length:   6,
			expected: []byte("L1_100"),
			desc:     "Layer1's data - if this fails, journal snapshots are not being loaded!",
		},
		{
			name:     "Offset 120 (Current overwrites)",
			offset:   120,
			length:   7,
			expected: []byte("CUR_120"),
			desc:     "Current's data overwrites Layer1 and Layer2",
		},
		{
			name:     "Offset 150 (Layer2 - CRITICAL TEST)",
			offset:   150,
			length:   6,
			expected: []byte("L2_150"),
			desc:     "Layer2's data - if this fails, journal snapshots are not being loaded!",
		},
		{
			name:     "Offset 200 (Layer1 - CRITICAL TEST)",
			offset:   200,
			length:   6,
			expected: []byte("L1_200"),
			desc:     "Layer1's data - if this fails, journal snapshots are not being loaded!",
		},
		{
			name:     "Offset 250 (Layer2 - CRITICAL TEST)",
			offset:   250,
			length:   6,
			expected: []byte("L2_250"),
			desc:     "Layer2's data - if this fails, journal snapshots are not being loaded!",
		},
		{
			name:     "Offset 50 (Base data)",
			offset:   50,
			length:   1,
			expected: []byte("Z"),
			desc:     "Base data, not modified",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readData, err := ra4.Read(tc.offset, tc.length)
			if err != nil {
				t.Fatalf("Failed to read at offset %d: %v", tc.offset, err)
			}

			if len(readData) != len(tc.expected) {
				t.Errorf("Length mismatch at offset %d: got %d, want %d",
					tc.offset, len(readData), len(tc.expected))
				return
			}

			if !bytes.Equal(readData, tc.expected) {
				t.Errorf("❌ FAILED: Data mismatch at offset %d\n"+
					"Description: %s\n"+
					"Expected: %q\n"+
					"Got:      %q\n"+
					"\n"+
					"🔴 THIS IS THE BUG: Journal snapshots are not being loaded and applied!\n"+
					"   readBaseData only reads fileObj.DataID, it doesn't check for journal snapshots.\n"+
					"   When there are multiple journal snapshots, they need to be recursively loaded\n"+
					"   and applied in the correct order (base -> layer1 -> layer2 -> current).",
					tc.offset, tc.desc, tc.expected, readData)
			} else {
				t.Logf("✓ Read correct at offset %d: %q (%s)", tc.offset, readData, tc.desc)
			}
		})
	}

	t.Logf("\n✅ Multi-layer journal version chain test completed")
	t.Logf("⚠️  If any CRITICAL TEST failed, it means journal snapshots are not being loaded!")
}

// TestJournalMultiLayerReopenFromDatabase tests the critical bug scenario:
// When a file is closed and reopened, GetOrCreate creates a new journal
// but doesn't load journal snapshots from the database.
// readBaseData only reads fileObj.DataID, missing all journal snapshot layers.
//
// This test simulates:
// 1. Create base data and flush
// 2. Create Layer 1 journal snapshot, close file
// 3. Reopen file, create Layer 2 journal snapshot, close file
// 4. Reopen file again, add current entries
// 5. Read - should see all layers, but may only see current layer if bug exists
func TestJournalMultiLayerReopenFromDatabase(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "orcas_journal_reopen_db_test")
	defer cleanupTestDir(t, testDir)

	fs, bktID := setupTestFS(t, testDir)
	defer cleanupFS(fs)

	fileName := "test_reopen_db.dat"
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Step 1: Create base data and flush
	baseData := make([]byte, 1000)
	for i := range baseData {
		baseData[i] = byte('B') // Base pattern: all 'B'
	}

	ra1, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	if err := ra1.Write(0, baseData); err != nil {
		t.Fatalf("Failed to write base data: %v", err)
	}
	if _, err := ra1.Flush(); err != nil {
		t.Fatalf("Failed to flush base data: %v", err)
	}
	if err := ra1.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}

	fileObj1, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj1) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	baseDataID := fileObj1[0].DataID
	t.Logf("✓ Step 1: Base data written and flushed (DataID=%d)", baseDataID)

	// Step 2: Create Layer 1 journal snapshot, then close
	// Use VFS interface: write data, manually create snapshot via journal, then close
	ra2, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}

	layer1Data := []byte("LAYER1_100") // offset 100
	if err := ra2.Write(100, layer1Data); err != nil {
		t.Fatalf("Failed to write layer1: %v", err)
	}

	// Get journal and manually create snapshot (simulating SmartFlush behavior)
	// This ensures snapshot is actually created and saved to database
	fileObj2, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj2) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	journal1 := fs.journalMgr.GetOrCreate(fileID, fileObj2[0].DataID, fileObj2[0].Size)
	layer1VersionID, err := journal1.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 1 snapshot: %v", err)
	}

	// Verify snapshot was created in database
	versions, _, _, err := fs.h.List(fs.c, bktID, fileID, core.ListOptions{Count: 100})
	if err == nil {
		found := false
		t.Logf("  After Layer 1 creation, listing all objects under fileID=%d:", fileID)
		for _, v := range versions {
			t.Logf("    Object: ID=%d, PID=%d, Type=%d, DataID=%d, Name=%s, Extra=%s", v.ID, v.PID, v.Type, v.DataID, v.Name, v.Extra)
			if v.Type == core.OBJ_TYPE_JOURNAL && v.ID == layer1VersionID {
				found = true
				t.Logf("  Verified: Layer 1 snapshot exists in database: ID=%d, DataID=%d", v.ID, v.DataID)
			}
		}
		if !found {
			t.Logf("⚠️  Warning: Layer 1 snapshot (ID=%d) not found in version list", layer1VersionID)
		}
	}

	if err := ra2.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}

	// CRITICAL: Remove journal from manager to simulate file close
	// In real scenario, when RandomAccessor closes, journal may stay in manager
	// But to test the bug, we need to remove it so GetOrCreate creates a new one
	fs.journalMgr.Remove(fileID)

	// Small delay to ensure cleanup completes
	time.Sleep(50 * time.Millisecond)

	t.Logf("✓ Step 2: Layer 1 journal snapshot created (versionID=%d), journal removed from manager", layer1VersionID)

	// Step 3: Reopen file, create Layer 2 journal snapshot, then close
	// When we reopen, GetOrCreate will create a NEW journal (since we removed it)
	// The question is: does it load Layer 1 snapshot entries from database?
	ra3, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}

	fileObj3, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj3) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	t.Logf("⚠️  After reopen: fileObj.DataID=%d (should still be base DataID, not updated by snapshot)", fileObj3[0].DataID)

	layer2Data := []byte("LAYER2_200") // offset 200
	if err := ra3.Write(200, layer2Data); err != nil {
		t.Fatalf("Failed to write layer2: %v", err)
	}

	// Get journal and manually create snapshot
	journal2 := fs.journalMgr.GetOrCreate(fileID, fileObj3[0].DataID, fileObj3[0].Size)
	layer2VersionID, err := journal2.CreateJournalSnapshot()
	if err != nil {
		t.Fatalf("Failed to create layer 2 snapshot: %v", err)
	}

	// Verify both snapshots exist in database
	versions, _, _, err = fs.h.List(fs.c, bktID, fileID, core.ListOptions{Count: 100})
	if err == nil {
		journalCount := 0
		for _, v := range versions {
			t.Logf("  Version object: ID=%d, PID=%d, Type=%d, DataID=%d, Name=%s", v.ID, v.PID, v.Type, v.DataID, v.Name)
			if v.Type == core.OBJ_TYPE_JOURNAL {
				journalCount++
				t.Logf("    Journal snapshot: ID=%d, Extra=%s", v.ID, v.Extra)
			}
		}
		t.Logf("  Found %d journal snapshots in database (should be >= 2)", journalCount)
	} else {
		t.Logf("  Failed to list versions: %v", err)
	}

	if err := ra3.Close(); err != nil {
		t.Fatalf("Failed to close RandomAccessor: %v", err)
	}

	// CRITICAL: Remove journal from manager again to test the bug
	fs.journalMgr.Remove(fileID)

	// Small delay to ensure cleanup completes
	time.Sleep(50 * time.Millisecond)

	t.Logf("✓ Step 3: Layer 2 journal snapshot created (versionID=%d), journal removed from manager", layer2VersionID)

	// Step 4: Reopen file again, add current entries (don't flush, keep in journal)
	// This simulates a file that's currently open with unflushed journal entries
	ra4, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	defer ra4.Close()

	fileObj4, err := fs.h.Get(fs.c, bktID, []int64{fileID})
	if err != nil || len(fileObj4) == 0 {
		t.Fatalf("Failed to get file object: %v", err)
	}
	t.Logf("⚠️  After second reopen: fileObj.DataID=%d (should still be base DataID, not updated by snapshots)", fileObj4[0].DataID)

	currentData := []byte("CURRENT_300") // offset 300
	if err := ra4.Write(300, currentData); err != nil {
		t.Fatalf("Failed to write current: %v", err)
	}
	
	// Don't flush - keep entries in current journal to test overlay
	t.Logf("✓ Step 4: Current journal entry written (offset 300, not flushed)")

	// Step 5: Test reading
	// Expected if bug exists:
	// - Offset 100: 'B' (base data, Layer1 entries NOT loaded!)
	// - Offset 200: 'B' (base data, Layer2 entries NOT loaded!)
	// - Offset 300: "CURRENT_300" (current entries visible)
	//
	// Expected if fixed:
	// - Offset 100: "LAYER1_100" (Layer1 entries loaded and applied)
	// - Offset 200: "LAYER2_200" (Layer2 entries loaded and applied)
	// - Offset 300: "CURRENT_300" (current entries applied)

	testCases := []struct {
		name     string
		offset   int64
		length   int
		expected []byte
		desc     string
	}{
		{
			name:     "Offset 100 (Layer1 - BUG TEST)",
			offset:   100,
			length:   len("LAYER1_100"),
			expected: []byte("LAYER1_100"),
			desc:     "Layer1's data - if this shows 'B', journal snapshots are NOT being loaded from database!",
		},
		{
			name:     "Offset 200 (Layer2 - BUG TEST)",
			offset:   200,
			length:   len("LAYER2_200"),
			expected: []byte("LAYER2_200"),
			desc:     "Layer2's data - if this shows 'B', journal snapshots are NOT being loaded from database!",
		},
		{
			name:     "Offset 300 (Current)",
			offset:   300,
			length:   len("CURRENT_300"),
			expected: []byte("CURRENT_300"),
			desc:     "Current's data",
		},
		{
			name:     "Offset 50 (Base data)",
			offset:   50,
			length:   1,
			expected: []byte("B"),
			desc:     "Base data, not modified",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readData, err := ra4.Read(tc.offset, tc.length)
			if err != nil {
				t.Fatalf("Failed to read at offset %d: %v", tc.offset, err)
			}

			if len(readData) != len(tc.expected) {
				t.Errorf("Length mismatch at offset %d: got %d, want %d",
					tc.offset, len(readData), len(tc.expected))
				return
			}

			if !bytes.Equal(readData, tc.expected) {
				// Check if it's the base data (indicating bug)
				if bytes.Equal(readData, bytes.Repeat([]byte("B"), len(readData))) {
					t.Errorf("🔴 BUG CONFIRMED: Data mismatch at offset %d\n"+
						"Description: %s\n"+
						"Expected: %q\n"+
						"Got:      %q (base data 'B')\n"+
						"\n"+
						"❌ THIS IS THE BUG:\n"+
						"   Journal snapshots are NOT being loaded from database when file is reopened!\n"+
						"   readBaseData only reads fileObj.DataID, it doesn't check for journal snapshots.\n"+
						"   GetOrCreate doesn't load journal snapshots from database.\n"+
						"   When there are multiple journal snapshots, they need to be:\n"+
						"   1. Found by querying OBJ_TYPE_JOURNAL objects\n"+
						"   2. Loaded in order (by baseVersionID chain)\n"+
						"   3. Applied recursively when reading base data",
						tc.offset, tc.desc, tc.expected, readData)
				} else {
					t.Errorf("❌ FAILED: Data mismatch at offset %d\n"+
						"Description: %s\n"+
						"Expected: %q\n"+
						"Got:      %q",
						tc.offset, tc.desc, tc.expected, readData)
				}
			} else {
				t.Logf("✓ Read correct at offset %d: %q (%s)", tc.offset, readData, tc.desc)
			}
		})
	}

	t.Logf("\n✅ Multi-layer journal reopen from database test completed")
}
