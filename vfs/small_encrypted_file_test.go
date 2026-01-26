package vfs

import (
	"bytes"
	"testing"

	"github.com/orcastor/orcas/core"
)

// TestSmallEncryptedFileRandomAccessor tests writing and reading a small encrypted file (241 bytes)
// using RandomAccessor directly, without FUSE layer
func TestSmallEncryptedFileRandomAccessor(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/.db"

	// Initialize database
	c, err := core.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer c.Close()

	// Create bucket with encryption enabled
	bktID := core.NewID()
	encryptionKey := "test-encryption-key-32-bytes!!"
	bkt := &core.BucketInfo{
		ID:   bktID,
		Name: "test-bucket",
		Extra: map[string]interface{}{
			"endec_way": int64(core.DATA_ENDEC_AES256GCM),
			"endec_key": encryptionKey,
		},
	}

	lh := core.NewLocalHandler(tmpDir)
	_, err = lh.PutBucket(c, bkt)
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Create VFS
	fs, err := NewOrcasFS(c, lh, bktID, tmpDir)
	if err != nil {
		t.Fatalf("Failed to create VFS: %v", err)
	}
	defer fs.Close()

	// Set encryption key
	fs.SetKey(encryptionKey)

	// Create test file object
	fileName := "test-small-encrypted.txt"
	testData := make([]byte, 241) // Exactly 241 bytes
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Create file object in database
	fileID := core.NewID()
	fileObj := &core.ObjectInfo{
		ID:     fileID,
		PID:    1, // Root directory
		Type:   core.OBJ_TYPE_FILE,
		Name:   fileName,
		DataID: 0,
		Size:   0,
		MTime:  core.Now(),
	}

	_, err = lh.Put(c, bktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("Failed to create file object: %v", err)
	}

	// Create RandomAccessor
	ra := NewRandomAccessor(fs, fileID, false)

	// Write data
	t.Logf("Writing %d bytes to file", len(testData))
	err = ra.Write(0, testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Flush
	t.Logf("Flushing file")
	versionID, err := ra.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	t.Logf("Flush returned versionID=%d", versionID)

	// Close (simulates Release)
	t.Logf("Closing RandomAccessor")
	err = ra.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Create new RandomAccessor for reading
	t.Logf("Creating new RandomAccessor for reading")
	ra2 := NewRandomAccessor(fs, fileID, false)
	defer ra2.Close()

	// Read data
	t.Logf("Reading %d bytes from file", len(testData))
	readData, err := ra2.Read(0, len(testData))
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	t.Logf("Read %d bytes from file", len(readData))

	// Verify data
	if len(readData) != len(testData) {
		t.Fatalf("Read wrong size: expected %d, got %d", len(testData), len(readData))
	}

	if !bytes.Equal(readData, testData) {
		// Print first 64 bytes of each for debugging
		t.Logf("Expected (first 64 bytes): %x", testData[:min(64, len(testData))])
		t.Logf("Got (first 64 bytes): %x", readData[:min(64, len(readData))])
		t.Fatalf("Read data does not match written data")
	}

	t.Logf("Test passed: successfully wrote and read %d bytes", len(testData))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
