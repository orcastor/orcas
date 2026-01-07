package vfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestDecryptErrorOnChunk1 reproduces the decryption error when reading chunk 1
// This test case simulates the actual scenario:
// - Original file is small (only chunk 0, e.g., 512 bytes)
// - Random writes through journal extend file size beyond one chunk (e.g., 11575808 bytes)
// - Reading beyond chunk 0 should read from journal, not try to read non-existent chunk 1
func TestDecryptErrorOnChunk1(t *testing.T) {
	// Setup test environment
	testDir := filepath.Join(os.TempDir(), "orcas_decrypt_error_test")
	defer cleanupTestDir(t, testDir)

	// Create filesystem with encryption enabled
	encryptionKey := "test-encryption-key-12345678901234567890123456789012" // 32 bytes for AES-256
	fs, bktID := setupTestFSWithEncryption(t, testDir, encryptionKey, 0) // No compression
	defer cleanupFS(fs)

	fileName := "test_decrypt_error.ppt"
	chunkSize := int64(10485760) // 10MB chunk size

	// Step 1: Create file with small initial data (only chunk 0, less than chunkSize)
	fileID, err := createTestFile(t, fs, bktID, fileName)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	t.Logf("Created file: fileID=%d, fileName=%s", fileID, fileName)

	// Step 2: Write small initial data (only chunk 0, 512 bytes)
	ra, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor: %v", err)
	}
	defer ra.Close()

	initialData := make([]byte, 512)
	for i := range initialData {
		initialData[i] = byte(i % 256)
	}
	err = ra.Write(0, initialData)
	if err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}
	t.Logf("Written initial data: offset=0, size=%d (only chunk 0)", len(initialData))

	// Step 3: Flush initial data to create baseVersion (only chunk 0)
	versionID, err := ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush initial data: %v", err)
	}
	t.Logf("Flushed initial data: versionID=%d (baseVersion has only chunk 0)", versionID)

	// Step 4: Write data beyond chunk 0 through journal (this extends file size beyond one chunk)
	// Write at offset 10485760 (start of chunk 1) - this extends file size
	extendedData := make([]byte, 1090048) // 11575808 - 10485760 = 1090048 bytes
	for i := range extendedData {
		extendedData[i] = byte((i + 200) % 256) // Different pattern
	}
	err = ra.Write(chunkSize, extendedData)
	if err != nil {
		t.Fatalf("Failed to write extended data: %v", err)
	}
	t.Logf("Written extended data through journal: offset=%d, size=%d (extends beyond chunk 0)", chunkSize, len(extendedData))

	// Step 5: Verify journal has the extended data
	ra.journalMu.RLock()
	hasJournal := ra.journal != nil
	journalIsDirty := false
	if hasJournal {
		journalIsDirty = ra.journal.IsDirty()
	}
	ra.journalMu.RUnlock()
	t.Logf("Journal state: exists=%v, isDirty=%v", hasJournal, journalIsDirty)

	// Step 6: Read from chunk 0 (should succeed, reads from baseVersion)
	readBuf0 := make([]byte, 512)
	readData0, err := ra.Read(0, len(readBuf0))
	if err != nil {
		t.Fatalf("Failed to read from chunk 0: %v", err)
	}
	t.Logf("✓ Successfully read from chunk 0: offset=0, size=%d", len(readData0))

	// Step 7: Read from chunk 1 (should read from journal, not try to read non-existent chunk 1)
	// This is where the error occurred in the log - should now work correctly
	readBuf1 := make([]byte, 131072)
	readData1, err := ra.Read(chunkSize, len(readBuf1))
	if err != nil {
		t.Logf("❌ ERROR reading from chunk 1: offset=%d, error=%v", chunkSize, err)
		if err != nil && (errorContains(err.Error(), "decryption failed") || errorContains(err.Error(), "message authentication failed")) {
			t.Errorf("❌ Still getting decryption error - fix didn't work: %v", err)
		} else {
			t.Fatalf("Unexpected error reading from chunk 1: %v", err)
		}
	} else {
		t.Logf("✓ Successfully read from chunk 1 (from journal): offset=%d, size=%d", chunkSize, len(readData1))
		// Verify data integrity - should match extendedData
		expectedSize := min(len(readData1), len(extendedData))
		if !bytes.Equal(readData1[:expectedSize], extendedData[:expectedSize]) {
			t.Errorf("Data mismatch: got %v, want %v", readData1[:min(10, len(readData1))], extendedData[:min(10, len(extendedData))])
		} else {
			t.Logf("✓ Data integrity verified at offset %d", chunkSize)
		}
	}

	// Step 8: Try reading at offset 11513856 (near the end, where error occurred in log)
	readBuf2 := make([]byte, 4096)
	readData2, err := ra.Read(11513856, len(readBuf2))
	if err != nil {
		t.Logf("❌ ERROR reading at offset 11513856: error=%v", err)
		if err != nil && (errorContains(err.Error(), "decryption failed") || errorContains(err.Error(), "message authentication failed")) {
			t.Errorf("❌ Still getting decryption error at offset 11513856 - fix didn't work: %v", err)
		}
	} else {
		t.Logf("✓ Successfully read at offset 11513856: size=%d", len(readData2))
	}

	// Step 9: Flush journal to persist extended data
	versionID2, err := ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush journal: %v", err)
	}
	t.Logf("Flushed journal: versionID=%d", versionID2)

	// Step 10: Close and reopen to read from persisted data
	ra.Close()
	ra2, err := getOrCreateRandomAccessor(fs, fileID)
	if err != nil {
		t.Fatalf("Failed to get RandomAccessor for read: %v", err)
	}
	defer ra2.Close()

	// Step 11: Read from chunk 1 after flush (should read from persisted data)
	readBuf3 := make([]byte, 131072)
	readData3, err := ra2.Read(chunkSize, len(readBuf3))
	if err != nil {
		t.Fatalf("Failed to read from chunk 1 after flush: %v", err)
	}
	t.Logf("✓ Successfully read from chunk 1 after flush: offset=%d, size=%d", chunkSize, len(readData3))
}

// Helper function to check if error message contains a substring
func errorContains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || 
		(len(s) > len(substr) && 
			(s[:len(substr)] == substr || 
			 s[len(s)-len(substr):] == substr || 
			 findSubstringInError(s, substr))))
}

func findSubstringInError(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

