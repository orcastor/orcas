package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
)

// TestInstantUploadBasic tests basic instant upload functionality
func TestInstantUploadBasic(t *testing.T) {
	_, router := setupTestEnvironmentForInstantUpload(t)
	bucketName := "test-bucket"

	// Test data that will be uploaded twice
	testData := []byte("This is test data for instant upload. It should be deduplicated on second upload.")
	key1 := "file1.txt"
	key2 := "file2.txt"

	// 1. Upload first file
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key1), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("First PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// Wait for first upload to complete and ensure it's written to database
	time.Sleep(500 * time.Millisecond)

	// 2. Upload second file with same data (should trigger instant upload)
	startTime := time.Now()
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key2), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	uploadDuration := time.Since(startTime)

	if w.Code != http.StatusOK {
		t.Fatalf("Second PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// Instant upload should be much faster than normal upload
	// For small files, instant upload should complete in < 50ms
	if uploadDuration > 200*time.Millisecond {
		t.Logf("Warning: Second upload took %v, might not have used instant upload", uploadDuration)
	}

	// 3. Verify both files exist and have same content
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key1), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject for file1 failed: status=%d", w.Code)
	}
	if !bytes.Equal(w.Body.Bytes(), testData) {
		t.Fatalf("File1 content mismatch")
	}

	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key2), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject for file2 failed: status=%d", w.Code)
	}
	if !bytes.Equal(w.Body.Bytes(), testData) {
		t.Fatalf("File2 content mismatch")
	}

	// Wait for second upload to complete
	time.Sleep(500 * time.Millisecond)

	// 4. Verify both files share the same DataID (instant upload deduplication)
	handler := core.NewLocalHandler()
	ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	bktID, err := getBucketIDByNameForTest(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("Failed to get bucket ID: %v", err)
	}

	// List all objects in root to find our files
	userCtx := core.UserInfo2Ctx(ctx, &core.UserInfo{ID: 1})
	allObjs, _, _, err := handler.List(userCtx, bktID, 0, core.ListOptions{Count: 1000})
	if err != nil {
		t.Fatalf("Failed to list objects: %v", err)
	}

	var obj1, obj2 *core.ObjectInfo
	for _, obj := range allObjs {
		if obj.Name == key1 && obj.PID == 0 {
			obj1 = obj
		}
		if obj.Name == key2 && obj.PID == 0 {
			obj2 = obj
		}
	}

	if obj1 == nil {
		t.Fatalf("File1 not found in object list")
	}
	if obj2 == nil {
		t.Fatalf("File2 not found in object list")
	}

	if obj1.DataID != obj2.DataID {
		t.Fatalf("Files should share same DataID (instant upload), but got DataID1=%d, DataID2=%d", obj1.DataID, obj2.DataID)
	}

	t.Logf("✓ Instant upload test passed: Both files share DataID %d", obj1.DataID)
}

// TestInstantUploadDifferentData tests that different data does not trigger instant upload
func TestInstantUploadDifferentData(t *testing.T) {
	_, router := setupTestEnvironmentForInstantUpload(t)
	bucketName := "test-bucket"

	// Different test data
	testData1 := []byte("This is test data for file 1.")
	testData2 := []byte("This is different test data for file 2.")
	key1 := "file1.txt"
	key2 := "file2.txt"

	// 1. Upload first file
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key1), bytes.NewReader(testData1))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("First PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// Wait for first upload to complete
	time.Sleep(500 * time.Millisecond)

	// 2. Upload second file with different data (should NOT trigger instant upload)
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key2), bytes.NewReader(testData2))
	req.Header.Set("Content-Type", "application/octet-stream")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Second PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// Wait for second upload to complete
	time.Sleep(500 * time.Millisecond)

	// 3. Verify files have different DataIDs
	handler := core.NewLocalHandler()
	ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	bktID, err := getBucketIDByNameForTest(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("Failed to get bucket ID: %v", err)
	}
	// List all objects in root to find our files
	userCtx := core.UserInfo2Ctx(ctx, &core.UserInfo{ID: 1})
	allObjs, _, _, err := handler.List(userCtx, bktID, 0, core.ListOptions{Count: 1000})
	if err != nil {
		t.Fatalf("Failed to list objects: %v", err)
	}

	var obj1, obj2 *core.ObjectInfo
	for _, obj := range allObjs {
		if obj.Name == key1 && obj.PID == 0 {
			obj1 = obj
		}
		if obj.Name == key2 && obj.PID == 0 {
			obj2 = obj
		}
	}

	if obj1 == nil {
		t.Fatalf("File1 not found in object list")
	}
	if obj2 == nil {
		t.Fatalf("File2 not found in object list")
	}

	if obj1.DataID == obj2.DataID {
		t.Fatalf("Files with different data should have different DataIDs, but both have DataID=%d", obj1.DataID)
	}

	t.Logf("✓ Different data test passed: File1 DataID=%d, File2 DataID=%d", obj1.DataID, obj2.DataID)
}

// TestInstantUploadCopyObject tests instant upload in copy operation
func TestInstantUploadCopyObject(t *testing.T) {
	_, router := setupTestEnvironmentForInstantUpload(t)
	bucketName := "test-bucket"

	// Test data
	testData := []byte("This is test data for copy operation with instant upload.")
	sourceKey := "source-file.txt"
	destKey := "dest-file.txt"

	// 1. Upload source file
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, sourceKey), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// Wait for upload to complete
	time.Sleep(500 * time.Millisecond)

	// 2. Copy file (should trigger instant upload in destination bucket)
	startTime := time.Now()
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
	req.Header.Set("x-amz-copy-source", fmt.Sprintf("/%s/%s", bucketName, sourceKey))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	copyDuration := time.Since(startTime)

	if w.Code != http.StatusOK {
		t.Fatalf("CopyObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// Wait for copy to complete
	time.Sleep(500 * time.Millisecond)

	// 3. Verify both files share the same DataID
	handler := core.NewLocalHandler()
	ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	bktID, err := getBucketIDByNameForTest(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("Failed to get bucket ID: %v", err)
	}
	// List all objects in root to find our files
	userCtx := core.UserInfo2Ctx(ctx, &core.UserInfo{ID: 1})
	allObjs, _, _, err := handler.List(userCtx, bktID, 0, core.ListOptions{Count: 1000})
	if err != nil {
		t.Fatalf("Failed to list objects: %v", err)
	}

	var sourceObj, destObj *core.ObjectInfo
	for _, obj := range allObjs {
		if obj.Name == sourceKey && obj.PID == 0 {
			sourceObj = obj
		}
		if obj.Name == destKey && obj.PID == 0 {
			destObj = obj
		}
	}

	if sourceObj == nil {
		t.Fatalf("Source file not found in object list")
	}
	if destObj == nil {
		t.Fatalf("Dest file not found in object list")
	}

	if sourceObj.DataID != destObj.DataID {
		t.Fatalf("Source and dest files should share same DataID (instant upload), but got Source DataID=%d, Dest DataID=%d", sourceObj.DataID, destObj.DataID)
	}

	t.Logf("✓ Copy with instant upload test passed: Both files share DataID %d, copy took %v", sourceObj.DataID, copyDuration)
}

// setupTestEnvironmentForInstantUpload sets up test environment for instant upload tests
func setupTestEnvironmentForInstantUpload(t *testing.T) (int64, *gin.Engine) {
	// Create independent temporary directories for each test
	testID := time.Now().UnixNano() % 1000000000
	baseDir := filepath.Join(os.TempDir(), fmt.Sprintf("o_s3_iu_%d", testID))
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("o_s3d_iu_%d", testID))

	os.MkdirAll(baseDir, 0o755)
	os.MkdirAll(dataDir, 0o755)

	// Set environment variables
	os.Setenv("ORCAS_BASE", baseDir)
	os.Setenv("ORCAS_DATA", dataDir)
	// Disable batch write to avoid permission issues in tests
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "false")
	core.ORCAS_BASE = baseDir
	core.ORCAS_DATA = dataDir

	// Initialize database
	core.InitDB()
	time.Sleep(50 * time.Millisecond)

	ensureTestUserForInstantUpload(t)

	// Create test bucket
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// Login and create bucket
	handler := core.NewLocalHandler()
	ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:        testBktID,
		Name:      "test-bucket",
		UID:       1,
		Type:      1,
		Quota:     -1,              // Unlimited quota
		ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Wait for bucket info to be written
	time.Sleep(200 * time.Millisecond)

	// Setup router
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		// Mock JWT authentication, set UID
		c.Set("uid", int64(1))
		c.Request = c.Request.WithContext(core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{
			ID: 1,
		}))
		c.Next()
	})

	// Register routes
	router.GET("/", listBuckets)
	router.PUT("/:bucket", createBucket)
	router.DELETE("/:bucket", deleteBucket)
	router.GET("/:bucket", listObjects)
	router.GET("/:bucket/*key", getObject)
	router.PUT("/:bucket/*key", putObject)
	router.DELETE("/:bucket/*key", deleteObject)
	router.HEAD("/:bucket/*key", headObject)

	return testBktID, router
}

// findObjectByPathForTest finds object by path (for testing)
func findObjectByPathForTest(ctx context.Context, bktID int64, path string) (*core.ObjectInfo, error) {
	handler := core.NewLocalHandler()
	// Ensure context has user info for permission check
	userCtx := core.UserInfo2Ctx(ctx, &core.UserInfo{ID: 1})
	
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty path")
	}

	// For simple paths (single filename), just list root directory
	if len(parts) == 1 {
		objs, _, _, err := handler.List(userCtx, bktID, 0, core.ListOptions{
			Count: 1000, // Get all objects
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range objs {
			if obj.Name == parts[0] && obj.PID == 0 {
				return obj, nil
			}
		}
		return nil, fmt.Errorf("object not found: %s", parts[0])
	}

	// For paths with multiple parts, traverse directory structure
	var currentPID int64 = 0
	for i, part := range parts {
		isLast := i == len(parts)-1
		// List all objects in current directory and find by exact name match
		objs, _, _, err := handler.List(userCtx, bktID, currentPID, core.ListOptions{
			Count: 1000, // Get all objects
		})
		if err != nil {
			return nil, err
		}

		var found *core.ObjectInfo
		for _, obj := range objs {
			if obj.Name == part && obj.PID == currentPID {
				found = obj
				break
			}
		}

		if found == nil {
			return nil, fmt.Errorf("object not found: %s", part)
		}

		if isLast {
			return found, nil
		}

		if found.Type != core.OBJ_TYPE_DIR {
			return nil, fmt.Errorf("not a directory: %s", part)
		}

		currentPID = found.ID
	}

	return nil, fmt.Errorf("object not found")
}

// getBucketIDByNameForTest gets bucket ID by name (for testing)
func getBucketIDByNameForTest(ctx context.Context, name string) (int64, error) {
	// Ensure context has user info
	userCtx := core.UserInfo2Ctx(ctx, &core.UserInfo{ID: 1})
	
	ma := &core.DefaultMetadataAdapter{}
	buckets, err := ma.ListBkt(userCtx, 1) // UID = 1 for test user
	if err != nil {
		return 0, err
	}

	for _, bkt := range buckets {
		if bkt.Name == name {
			return bkt.ID, nil
		}
	}

	return 0, fmt.Errorf("bucket not found: %s", name)
}

// splitPath splits path into parts
func splitPath(path string) []string {
	if path == "" || path == "/" {
		return []string{}
	}

	// Remove leading slash
	if path[0] == '/' {
		path = path[1:]
	}

	parts := []string{}
	current := ""
	for _, char := range path {
		if char == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}

	return parts
}

// ensureTestUserForInstantUpload ensures test user exists
func ensureTestUserForInstantUpload(t *testing.T) {
	handler := core.NewLocalHandler()
	ctx := context.Background()

	// Try to login first
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err == nil {
		return // User already exists
	}

	// Use the same approach as multipart test
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetDB()
	if err != nil {
		t.Logf("Warning: Failed to get DB: %v", err)
		return
	}
	defer db.Close()

	_, err = db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar, key) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
	if err != nil {
		t.Logf("Warning: Failed to create user: %v", err)
	}
}

