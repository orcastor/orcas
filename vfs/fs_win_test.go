//go:build windows
// +build windows

package vfs

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

// TestDokanyInitialization tests Dokany DLL loading
func TestDokanyInitialization(t *testing.T) {
	// This test will fail if Dokany driver is not installed
	// Skip if DLL is not available
	err := initDokany()
	if err != nil {
		t.Skipf("Dokany driver not installed: %v", err)
	}

	// Verify function pointers are loaded
	if dokanMainProc == nil {
		t.Error("DokanMain function pointer not loaded")
	}
	if dokanUnmountProc == nil {
		t.Error("DokanUnmount function pointer not loaded")
	}
}

// TestDokanyMountOptions tests Dokany mount options
func TestDokanyMountOptions(t *testing.T) {
	// Setup test environment
	ensureTestUser(t)
	handler := core.NewLocalHandler()
	ctx := context.Background()
	ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err = core.InitBucketDB(ctx, testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// Get user info for bucket creation
	_, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Create bucket
	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:        testBktID,
		Name:      "test-bucket",
		UID:       userInfo.ID,
		Type:      1,
		Quota:     -1,
		ChunkSize: 4 * 1024 * 1024,
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create filesystem
	sdkCfg := &sdk.Config{}
	ofs := NewOrcasFS(handler, ctx, testBktID, sdkCfg)

	// Test mount options
	mountPoint := filepath.Join(os.TempDir(), "orcas_test_mount")
	defer os.RemoveAll(mountPoint)

	opts := &DokanyOptions{
		MountPoint:  mountPoint,
		ThreadCount: 5,
		Options:     0,
	}

	// Try to mount (will fail if Dokany not installed, but should not crash)
	instance, err := ofs.MountDokany(mountPoint, opts)
	if err != nil {
		// Expected if Dokany not installed
		t.Logf("Mount failed (expected if Dokany not installed): %v", err)
		return
	}

	if instance == nil {
		t.Error("Mount returned nil instance")
	}

	// Cleanup
	if instance != nil {
		instance.Unmount()
	}
}

// TestDokanyOperations tests Dokany file operations
func TestDokanyOperations(t *testing.T) {
	// Setup test environment
	ensureTestUser(t)
	handler := core.NewLocalHandler()
	ctx := context.Background()
	ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err = core.InitBucketDB(ctx, testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// Get user info for bucket creation
	_, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Create bucket
	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:        testBktID,
		Name:      "test-bucket",
		UID:       userInfo.ID,
		Type:      1,
		Quota:     -1,
		ChunkSize: 4 * 1024 * 1024,
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create filesystem
	sdkCfg := &sdk.Config{}
	ofs := NewOrcasFS(handler, ctx, testBktID, sdkCfg)

	// Test create file
	testData := []byte("Hello, Dokany!")
	fileName := "/test-file.txt"

	// Create file object
	fileObj := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   core.ROOT_OID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  "test-file.txt",
		Size:  int64(len(testData)),
		MTime: core.Now(),
	}

	// Upload data
	dataID, err := handler.PutData(ctx, testBktID, 0, 0, testData)
	if err != nil {
		t.Fatalf("PutData failed: %v", err)
	}
	fileObj.DataID = dataID

	// Create object
	_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test findObjectByPath
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil {
		t.Fatalf("findObjectByPath failed: %v", err)
	}

	if obj.Name != "test-file.txt" {
		t.Errorf("Expected name 'test-file.txt', got '%s'", obj.Name)
	}

	if obj.Size != int64(len(testData)) {
		t.Errorf("Expected size %d, got %d", len(testData), obj.Size)
	}

	// Test dokanyGetFileInformation
	fileInfo := &FileInfo{}
	status := dokanyGetFileInformation(ofs, fileName, fileInfo, 0)
	if status != DOKAN_SUCCESS {
		t.Errorf("dokanyGetFileInformation failed with status %d", status)
	}

	if fileInfo.Length != int64(len(testData)) {
		t.Errorf("Expected file length %d, got %d", len(testData), fileInfo.Length)
	}

	if fileInfo.Attributes != FILE_ATTRIBUTE_NORMAL {
		t.Errorf("Expected FILE_ATTRIBUTE_NORMAL, got %d", fileInfo.Attributes)
	}

	// Test dokanyReadFile
	buffer := make([]byte, len(testData))
	bytesRead, status := dokanyReadFile(ofs, fileName, buffer, 0, 0)
	if status != DOKAN_SUCCESS {
		t.Errorf("dokanyReadFile failed with status %d", status)
	}

	if bytesRead != len(testData) {
		t.Errorf("Expected to read %d bytes, got %d", len(testData), bytesRead)
	}

	if string(buffer) != string(testData) {
		t.Errorf("Read data mismatch: expected '%s', got '%s'", string(testData), string(buffer))
	}
}

// TestDokanyDirectoryOperations tests Dokany directory operations
func TestDokanyDirectoryOperations(t *testing.T) {
	// Setup test environment
	ensureTestUser(t)
	handler := core.NewLocalHandler()
	ctx := context.Background()
	ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err = core.InitBucketDB(ctx, testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// Get user info for bucket creation
	_, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Create bucket
	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:        testBktID,
		Name:      "test-bucket",
		UID:       userInfo.ID,
		Type:      1,
		Quota:     -1,
		ChunkSize: 4 * 1024 * 1024,
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create filesystem
	sdkCfg := &sdk.Config{}
	ofs := NewOrcasFS(handler, ctx, testBktID, sdkCfg)

	// Create test directory
	dirObj := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   core.ROOT_OID,
		Type:  core.OBJ_TYPE_DIR,
		Name:  "test-dir",
		Size:  0,
		MTime: core.Now(),
	}

	_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{dirObj})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test findObjectByPath for directory
	obj, err := findObjectByPath(ofs, "/test-dir")
	if err != nil {
		t.Fatalf("findObjectByPath failed: %v", err)
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		t.Errorf("Expected directory type, got %d", obj.Type)
	}

	// Test dokanyGetFileInformation for directory
	fileInfo := &FileInfo{}
	status := dokanyGetFileInformation(ofs, "/test-dir", fileInfo, 0)
	if status != DOKAN_SUCCESS {
		t.Errorf("dokanyGetFileInformation failed with status %d", status)
	}

	if fileInfo.Attributes != FILE_ATTRIBUTE_DIRECTORY {
		t.Errorf("Expected FILE_ATTRIBUTE_DIRECTORY, got %d", fileInfo.Attributes)
	}

	// Test dokanyFindFiles
	foundFiles := []string{}
	fillFindData := func(fileName string, fileInfo *FileInfo) bool {
		foundFiles = append(foundFiles, fileName)
		return true
	}

	status = dokanyFindFiles(ofs, "/test-dir", fillFindData)
	if status != DOKAN_SUCCESS {
		t.Errorf("dokanyFindFiles failed with status %d", status)
	}

	// Should find the directory itself (if it has children, they would be listed)
	// For empty directory, should return success with no files
}

// TestDokanyPathNormalization tests path normalization
func TestDokanyPathNormalization(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/", "/"},
		{"/file.txt", "/file.txt"},
		{"file.txt", "/file.txt"},
		{"/dir/file.txt", "/dir/file.txt"},
		{"dir/file.txt", "/dir/file.txt"},
		{".", "/"},
		{"./file.txt", "/file.txt"},
		{"\\", "/"}, // Windows backslash
		{"\\file.txt", "/file.txt"},
		{"dir\\file.txt", "/dir/file.txt"},
		{"C:\\path\\file.txt", "/C:/path/file.txt"}, // Full Windows path
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizePath(tt.input)
			if result != tt.expected {
				t.Errorf("normalizePath(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestDokanyTimeConversion tests time conversion to FileTime
func TestDokanyTimeConversion(t *testing.T) {
	// Test with current time
	now := core.Now()
	fileTime := timeToFileTime(now)

	// Verify FileTime is not zero
	if fileTime.LowDateTime == 0 && fileTime.HighDateTime == 0 {
		t.Error("FileTime should not be zero")
	}

	// Test with zero time
	zeroTime := timeToFileTime(0)
	// Zero Unix time should convert to a specific FileTime (epoch difference)
	if zeroTime.LowDateTime == 0 && zeroTime.HighDateTime == 0 {
		t.Error("Zero Unix time should convert to non-zero FileTime")
	}
}

// TestDokanyCreateFile tests file creation
func TestDokanyCreateFile(t *testing.T) {
	// Setup test environment
	ensureTestUser(t)
	handler := core.NewLocalHandler()
	ctx := context.Background()
	ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err = core.InitBucketDB(ctx, testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// Get user info for bucket creation
	_, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Create bucket
	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:        testBktID,
		Name:      "test-bucket",
		UID:       userInfo.ID,
		Type:      1,
		Quota:     -1,
		ChunkSize: 4 * 1024 * 1024,
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create filesystem
	sdkCfg := &sdk.Config{}
	ofs := NewOrcasFS(handler, ctx, testBktID, sdkCfg)

	// Test creating a new file
	fileName := "/new-file.txt"
	handle, status := dokanyCreateFile(ofs, fileName, FILE_WRITE_DATA, 0, FILE_CREATE, FILE_ATTRIBUTE_NORMAL)
	if status != DOKAN_SUCCESS {
		t.Errorf("dokanyCreateFile failed with status %d", status)
	}

	if handle == 0 {
		t.Error("dokanyCreateFile returned zero handle")
	}

	// Verify file was created
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil {
		t.Fatalf("File not found after creation: %v", err)
	}

	if obj.Name != "new-file.txt" {
		t.Errorf("Expected name 'new-file.txt', got '%s'", obj.Name)
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		t.Errorf("Expected file type, got %d", obj.Type)
	}
}

// TestDokanyWriteAndRead tests write and read operations
func TestDokanyWriteAndRead(t *testing.T) {
	// Setup test environment
	ensureTestUser(t)
	handler := core.NewLocalHandler()
	ctx := context.Background()
	ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err = core.InitBucketDB(ctx, testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// Get user info for bucket creation
	_, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Create bucket
	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:        testBktID,
		Name:      "test-bucket",
		UID:       userInfo.ID,
		Type:      1,
		Quota:     -1,
		ChunkSize: 4 * 1024 * 1024,
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create filesystem
	sdkCfg := &sdk.Config{}
	ofs := NewOrcasFS(handler, ctx, testBktID, sdkCfg)

	// Create file with data directly (simpler test)
	testData := []byte("Hello, World!")
	dataID, err := handler.PutData(ctx, testBktID, 0, 0, testData)
	if err != nil {
		t.Fatalf("PutData failed: %v", err)
	}

	fileName := "/write-test.txt"
	fileObj := &core.ObjectInfo{
		ID:     core.NewID(),
		PID:    core.ROOT_OID,
		Type:   core.OBJ_TYPE_FILE,
		Name:   "write-test.txt",
		Size:   int64(len(testData)),
		DataID: dataID,
		MTime:  core.Now(),
	}
	_, err = handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Read data back
	buffer := make([]byte, len(testData))
	bytesRead, status := dokanyReadFile(ofs, fileName, buffer, 0, uintptr(fileObj.ID))
	if status != DOKAN_SUCCESS {
		t.Fatalf("dokanyReadFile failed with status %d", status)
	}

	if bytesRead != len(testData) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testData), bytesRead)
	}

	if bytesRead > 0 && string(buffer[:bytesRead]) != string(testData) {
		t.Fatalf("Read data mismatch: expected '%s', got '%s'", string(testData), string(buffer[:bytesRead]))
	}

	// Test write operation
	writeData := []byte("Updated!")

	// Get RandomAccessor before write to check state
	raBefore, err := getRandomAccessor(ofs, fileObj.ID)
	if err != nil {
		t.Fatalf("getRandomAccessor failed: %v", err)
	}

	// Check if file has data (should have DataID from initial write)
	fileObjBefore, err := raBefore.getFileObj()
	if err != nil {
		t.Fatalf("getFileObj failed: %v", err)
	}
	t.Logf("Before write: fileObj DataID=%d, Size=%d", fileObjBefore.DataID, fileObjBefore.Size)

	// Use the same RandomAccessor instance for write and flush
	// This ensures we're checking the same instance that was used for writing
	ra, err := getOrCreateRandomAccessor(ofs, fileObj.ID)
	if err != nil {
		t.Fatalf("getOrCreateRandomAccessor failed: %v", err)
	}

	// Write data directly using RandomAccessor (same instance)
	err = ra.Write(0, writeData)
	if err != nil {
		t.Fatalf("ra.Write failed: %v", err)
	}

	// Check buffer state after write
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	totalSize := atomic.LoadInt64(&ra.buffer.totalSize)
	t.Logf("After write, before flush: writeIndex=%d, totalSize=%d", writeIndex, totalSize)

	// Check if sequential buffer is being used
	if ra.seqBuffer != nil {
		t.Logf("Sequential buffer: hasData=%v, closed=%v, offset=%d",
			ra.seqBuffer.hasData, ra.seqBuffer.closed, ra.seqBuffer.offset)
	}

	// Verify data was added to buffer
	if writeIndex == 0 && totalSize == 0 && (ra.seqBuffer == nil || !ra.seqBuffer.hasData) {
		t.Fatalf("Write failed: no data in buffer or sequential buffer (writeIndex=%d, totalSize=%d)", writeIndex, totalSize)
	}

	// Flush to ensure data is written
	versionID, err := ra.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Logf("Flush completed: versionID=%d", versionID)

	// Check buffer state after flush
	writeIndexAfter := atomic.LoadInt64(&ra.buffer.writeIndex)
	totalSizeAfter := atomic.LoadInt64(&ra.buffer.totalSize)
	t.Logf("After flush: writeIndex=%d, totalSize=%d", writeIndexAfter, totalSizeAfter)

	// Close RandomAccessor
	err = ra.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Flush batch write manager if it exists
	batchMgr := ofs.getBatchWriteManager()
	if batchMgr != nil {
		batchMgr.FlushAll(ctx)
	}

	// Wait a bit for batch write to complete (if used)
	// This ensures all async operations are finished
	time.Sleep(100 * time.Millisecond)

	// Refresh object info to get updated DataID and size
	// Try multiple times in case batch write is still processing
	var updatedObj *core.ObjectInfo
	originalDataID := fileObj.DataID
	originalSize := fileObj.Size
	t.Logf("Original object: DataID=%d, Size=%d", originalDataID, originalSize)

	for retry := 0; retry < 10; retry++ {
		objs, err := handler.Get(ctx, testBktID, []int64{fileObj.ID})
		if err == nil && len(objs) > 0 {
			updatedObj = objs[0]
			t.Logf("Retry %d: DataID=%d, Size=%d", retry, updatedObj.DataID, updatedObj.Size)
			// Check if DataID has changed (indicating new data was written)
			if updatedObj.DataID != 0 && updatedObj.DataID != core.EmptyDataID && updatedObj.DataID != originalDataID {
				t.Logf("DataID changed from %d to %d", originalDataID, updatedObj.DataID)
				break
			}
			// If size changed, data was written (even if DataID is same)
			if updatedObj.Size == int64(len(writeData)) {
				t.Logf("Size matches written data: %d", updatedObj.Size)
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if updatedObj == nil {
		objs, err := handler.Get(ctx, testBktID, []int64{fileObj.ID})
		if err != nil || len(objs) == 0 {
			t.Fatalf("Failed to refresh object info: %v", err)
		}
		updatedObj = objs[0]
	}

	t.Logf("Final object: DataID=%d, Size=%d (expected size=%d)", updatedObj.DataID, updatedObj.Size, len(writeData))

	// Verify object has DataID
	if updatedObj.DataID == 0 || updatedObj.DataID == core.EmptyDataID {
		t.Fatalf("File object has no DataID after write and flush")
	}

	// Check if DataID changed or size changed
	dataIDChanged := updatedObj.DataID != originalDataID
	sizeChanged := updatedObj.Size != originalSize
	t.Logf("DataID changed: %v, Size changed: %v", dataIDChanged, sizeChanged)

	// If neither changed, the write may not have been flushed
	if !dataIDChanged && !sizeChanged {
		t.Fatalf("Write appears to have failed: DataID and Size unchanged (DataID=%d->%d, Size=%d->%d)",
			originalDataID, updatedObj.DataID, originalSize, updatedObj.Size)
	}

	// Verify size matches written data
	if updatedObj.Size != int64(len(writeData)) {
		t.Logf("Warning: Size mismatch: expected %d, got %d (dataID=%d). This may indicate partial write or buffer not fully flushed.",
			len(writeData), updatedObj.Size, updatedObj.DataID)
		// Don't fail yet, check the actual data
	}

	// Read updated data directly using handler to verify data was written correctly
	readData, err := handler.GetData(ctx, testBktID, updatedObj.DataID, 0)
	if err != nil {
		// Try reading all chunks
		var allChunks []byte
		for chunkIdx := 0; ; chunkIdx++ {
			chunkData, chunkErr := handler.GetData(ctx, testBktID, updatedObj.DataID, chunkIdx)
			if chunkErr != nil {
				break
			}
			allChunks = append(allChunks, chunkData...)
		}
		if len(allChunks) > 0 {
			readData = allChunks
		} else {
			t.Fatalf("Failed to read data from DataID %d: %v", updatedObj.DataID, err)
		}
	}

	// Verify data matches
	if len(readData) != len(writeData) {
		t.Fatalf("Data size mismatch: expected %d, got %d", len(writeData), len(readData))
	}

	if string(readData) != string(writeData) {
		t.Fatalf("Direct read data mismatch: expected '%s', got '%s'", string(writeData), string(readData))
	}

	// Now test dokanyReadFile (should also work since data is correct)
	readBuffer := make([]byte, len(writeData))
	bytesRead2, status := dokanyReadFile(ofs, fileName, readBuffer, 0, uintptr(updatedObj.ID))
	if status != DOKAN_SUCCESS {
		t.Fatalf("dokanyReadFile failed with status %d", status)
	}

	if bytesRead2 != len(writeData) {
		t.Fatalf("Expected to read %d bytes, got %d (size=%d, dataID=%d)", len(writeData), bytesRead2, updatedObj.Size, updatedObj.DataID)
	}

	if string(readBuffer[:bytesRead2]) != string(writeData) {
		t.Fatalf("Read data mismatch: expected '%s', got '%s' (size=%d, dataID=%d)", string(writeData), string(readBuffer[:bytesRead2]), updatedObj.Size, updatedObj.DataID)
	}
}
