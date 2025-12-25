package s3_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/s3"
)

// ensureTestUserForListObjects ensures test user exists for list objects tests
func ensureTestUserForListObjects(t *testing.T) {
	handler := core.NewLocalHandler("", "")
	ctx := context.Background()
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err == nil {
		// User already exists
		return
	}
	// Create user if doesn't exist using direct DB insert
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetMainDBWithKey(".", "")
	if err != nil {
		t.Fatalf("Failed to get DB: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar, key) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
	if err != nil {
		t.Fatalf("Failed to create test user: %v", err)
	}
}

// setupTestEnvironmentForListObjects sets up test environment
func setupTestEnvironmentForListObjects(t *testing.T) (int64, *gin.Engine) {
	// Enable batch write
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")

	// Initialize database (paths now managed via Handler)
	core.InitDB(".", "")
	ensureTestUserForListObjects(t)

	// Create test bucket
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(".", testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// Login and create bucket
	handler := core.NewLocalHandler(".", ".")
	ctx, userInfo, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Ensure userInfo is in context for PutBkt (which needs uid for ACL creation)
	if userInfo != nil {
		ctx = core.UserInfo2Ctx(ctx, userInfo)
	}

	// Create admin with correct paths
	dma := &core.DefaultMetadataAdapter{
		DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
		DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
	}
	dma.DefaultBaseMetadataAdapter.SetPath(".")
	dma.DefaultDataMetadataAdapter.SetPath(".")
	acm := &core.DefaultAccessCtrlMgr{}
	acm.SetAdapter(dma)
	admin := core.NewAdminWithAdapters(dma, &core.DefaultDataAdapter{}, acm)
	bkt := &core.BucketInfo{
		ID:    testBktID,
		Name:  "test-bucket",
		Type:  1,
		Quota: -1, // Unlimited quota
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Setup router
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		// Mock JWT authentication, set UID
		c.Set("uid", int64(1))
		c.Request = c.Request.WithContext(core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{
			ID:   1,
			Role: core.ADMIN,
		}))
		c.Next()
	})

	// Register routes
	router.GET("/", s3.ListBuckets)
	router.PUT("/:bucket", s3.CreateBucket)
	router.DELETE("/:bucket", s3.DeleteBucket)
	router.GET("/:bucket", s3.ListObjects)
	router.GET("/:bucket/*key", s3.GetObject)
	router.PUT("/:bucket/*key", s3.PutObject)
	router.DELETE("/:bucket/*key", s3.DeleteObject)
	router.HEAD("/:bucket/*key", s3.HeadObject)

	return testBktID, router
}

// TestListObjectsWithSlashes tests ListObjects with keys containing slashes
func TestListObjectsWithSlashes(t *testing.T) {
	_, router := setupTestEnvironmentForListObjects(t)

	// Create objects with different path structures
	testObjects := []struct {
		key  string
		data string
	}{
		{"file1.txt", "content1"},
		{"dir1/file2.txt", "content2"},
		{"dir1/subdir/file3.txt", "content3"},
		{"dir2/file4.txt", "content4"},
		{"dir2/subdir2/file5.txt", "content5"},
		{"rootfile.txt", "content6"},
	}

	// Upload objects
	for _, obj := range testObjects {
		req := httptest.NewRequest("PUT", fmt.Sprintf("/test-bucket/%s", obj.key), bytes.NewReader([]byte(obj.data)))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
			t.Logf("Failed to upload %s: status=%d, body=%s", obj.key, w.Code, w.Body.String())
			// Continue anyway to see what happens
		} else {
			t.Logf("Successfully uploaded %s", obj.key)
		}
	}

	// Test 1: List all objects
	t.Run("ListAllObjects", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("ListObjects failed: status=%d, body=%s", w.Code, w.Body.String())
		}

		var result s3.ListBucketResult
		if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to parse XML: %v", err)
		}

		// Verify all objects are listed with correct keys (including slashes)
		expectedKeys := make(map[string]bool)
		for _, obj := range testObjects {
			expectedKeys[obj.key] = true
		}

		// Debug: print all returned keys
		t.Logf("Returned keys: %v", func() []string {
			keys := make([]string, len(result.Contents))
			for i, content := range result.Contents {
				keys[i] = content.Key
			}
			return keys
		}())

		foundKeys := make(map[string]bool)
		for _, content := range result.Contents {
			foundKeys[content.Key] = true
			if !expectedKeys[content.Key] {
				t.Errorf("Unexpected key in result: %s", content.Key)
			}
		}

		for key := range expectedKeys {
			if !foundKeys[key] {
				t.Errorf("Missing key in result: %s", key)
			}
		}

		if len(result.Contents) != len(testObjects) {
			t.Errorf("Expected %d objects, got %d", len(testObjects), len(result.Contents))
		}
	})

	// Test 2: List objects with prefix
	t.Run("ListObjectsWithPrefix", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket?prefix=dir1/", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("ListObjects with prefix failed: status=%d, body=%s", w.Code, w.Body.String())
		}

		var result s3.ListBucketResult
		if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to parse XML: %v", err)
		}

		// Verify all keys start with "dir1/"
		for _, content := range result.Contents {
			if !strings.HasPrefix(content.Key, "dir1/") {
				t.Errorf("Key %s does not start with prefix dir1/", content.Key)
			}
		}

		// Should find dir1/file2.txt and dir1/subdir/file3.txt
		expectedCount := 2
		if len(result.Contents) != expectedCount {
			t.Errorf("Expected %d objects with prefix dir1/, got %d", expectedCount, len(result.Contents))
		}
	})

	// Test 3: List objects with delimiter
	t.Run("ListObjectsWithDelimiter", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket?delimiter=/", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("ListObjects with delimiter failed: status=%d, body=%s", w.Code, w.Body.String())
		}

		var result s3.ListBucketResult
		if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to parse XML: %v", err)
		}

		// With delimiter, should get common prefixes and root-level files
		// Root-level files: file1.txt, rootfile.txt
		// Common prefixes: dir1/, dir2/
		rootFiles := 0
		for _, content := range result.Contents {
			if !strings.Contains(content.Key, "/") {
				rootFiles++
			} else {
				t.Errorf("With delimiter, should not have keys with slashes: %s", content.Key)
			}
		}

		if rootFiles != 2 {
			t.Errorf("Expected 2 root-level files, got %d", rootFiles)
		}

		// Should have common prefixes
		if len(result.CommonPrefixes) < 2 {
			t.Errorf("Expected at least 2 common prefixes, got %d", len(result.CommonPrefixes))
		}
	})

	// Test 4: List objects with prefix and delimiter
	t.Run("ListObjectsWithPrefixAndDelimiter", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket?prefix=dir2/&delimiter=/", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("ListObjects with prefix and delimiter failed: status=%d, body=%s", w.Code, w.Body.String())
		}

		var result s3.ListBucketResult
		if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to parse XML: %v", err)
		}

		// Should find dir2/file4.txt and common prefix dir2/subdir2/
		for _, content := range result.Contents {
			if !strings.HasPrefix(content.Key, "dir2/") {
				t.Errorf("Key %s does not start with prefix dir2/", content.Key)
			}
			if strings.Contains(strings.TrimPrefix(content.Key, "dir2/"), "/") {
				t.Errorf("With delimiter, should not have nested paths: %s", content.Key)
			}
		}
	})

	// Test 5: Verify keys contain slashes for nested objects
	t.Run("VerifyKeysContainSlashes", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("ListObjects failed: status=%d", w.Code)
		}

		var result s3.ListBucketResult
		if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to parse XML: %v", err)
		}

		// Check that nested objects have slashes in their keys
		keyMap := make(map[string]bool)
		for _, content := range result.Contents {
			keyMap[content.Key] = true
		}

		// Verify specific nested keys exist
		if !keyMap["dir1/subdir/file3.txt"] {
			t.Error("Missing key: dir1/subdir/file3.txt")
		}
		if !keyMap["dir2/subdir2/file5.txt"] {
			t.Error("Missing key: dir2/subdir2/file5.txt")
		}
		if !keyMap["dir1/file2.txt"] {
			t.Error("Missing key: dir1/file2.txt")
		}
	})

	// Test 6: List objects with prefix ending with a letter (not slash)
	// This test verifies that prefix filtering works correctly when prefix ends with a letter
	// rather than a slash, ensuring only keys that start with the exact prefix are returned
	t.Run("ListObjectsWithPrefixEndingWithLetter", func(t *testing.T) {
		// Test prefix="dir1/f" - should match dir1/file2.txt but not dir1/subdir/file3.txt
		// Note: This tests that prefix ending with a letter (not slash) correctly filters objects
		req := httptest.NewRequest("GET", "/test-bucket?prefix=dir1/f", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("ListObjects with prefix dir1/f failed: status=%d, body=%s", w.Code, w.Body.String())
		}

		var result s3.ListBucketResult
		if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to parse XML: %v", err)
		}

		// Verify all returned keys start with prefix "dir1/f"
		// This ensures prefix filtering works correctly when prefix ends with a letter
		for _, content := range result.Contents {
			if !strings.HasPrefix(content.Key, "dir1/f") {
				t.Errorf("Key %s does not start with prefix dir1/f", content.Key)
			}
		}

		// Verify that keys starting with "dir1/subdir/" are NOT included
		// (because "dir1/f" doesn't match "dir1/subdir/")
		for _, content := range result.Contents {
			if strings.HasPrefix(content.Key, "dir1/subdir/") {
				t.Errorf("Should not find key starting with dir1/subdir/ with prefix dir1/f, got: %s", content.Key)
			}
		}

		// Test prefix="file" - should match file1.txt, file2.txt, file4.txt but not rootfile.txt
		// This tests prefix filtering when prefix is a single word (not a path)
		req2 := httptest.NewRequest("GET", "/test-bucket?prefix=file", nil)
		w2 := httptest.NewRecorder()
		router.ServeHTTP(w2, req2)

		if w2.Code != http.StatusOK {
			t.Fatalf("ListObjects with prefix file failed: status=%d", w2.Code)
		}

		var result2 s3.ListBucketResult
		if err := xml.Unmarshal(w2.Body.Bytes(), &result2); err != nil {
			t.Fatalf("Failed to parse XML: %v", err)
		}

		// Verify all returned keys start with prefix "file"
		for _, content := range result2.Contents {
			if !strings.HasPrefix(content.Key, "file") {
				t.Errorf("Key %s does not start with prefix file", content.Key)
			}
		}

		// Should NOT find rootfile.txt (because "file" doesn't match "rootfile")
		// This verifies that prefix matching is exact from the start
		for _, content := range result2.Contents {
			if content.Key == "rootfile.txt" {
				t.Error("Should not find rootfile.txt with prefix file")
			}
		}

		// Test prefix="dir2/subdir2/f" - should match dir2/subdir2/file5.txt but not dir2/file4.txt
		// This tests prefix filtering with nested paths ending with a letter
		req3 := httptest.NewRequest("GET", "/test-bucket?prefix=dir2/subdir2/f", nil)
		w3 := httptest.NewRecorder()
		router.ServeHTTP(w3, req3)

		if w3.Code != http.StatusOK {
			t.Fatalf("ListObjects with prefix dir2/subdir2/f failed: status=%d", w3.Code)
		}

		var result3 s3.ListBucketResult
		if err := xml.Unmarshal(w3.Body.Bytes(), &result3); err != nil {
			t.Fatalf("Failed to parse XML: %v", err)
		}

		// Verify all returned keys start with prefix "dir2/subdir2/f"
		for _, content := range result3.Contents {
			if !strings.HasPrefix(content.Key, "dir2/subdir2/f") {
				t.Errorf("Key %s does not start with prefix dir2/subdir2/f", content.Key)
			}
		}

		// Should NOT find dir2/file4.txt (because "dir2/subdir2/f" doesn't match "dir2/file4")
		// This verifies that prefix matching correctly excludes keys that don't match
		for _, content := range result3.Contents {
			if content.Key == "dir2/file4.txt" || strings.HasPrefix(content.Key, "dir2/file") {
				t.Errorf("Should not find key starting with dir2/file with prefix dir2/subdir2/f, got: %s", content.Key)
			}
		}
	})
}

// TestListObjectsEmptyBucket tests ListObjects on an empty bucket
func TestListObjectsEmptyBucket(t *testing.T) {
	_, router := setupTestEnvironmentForListObjects(t)

	req := httptest.NewRequest("GET", "/test-bucket", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("ListObjects failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	var result s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse XML: %v", err)
	}

	if len(result.Contents) != 0 {
		t.Errorf("Expected 0 objects in empty bucket, got %d", len(result.Contents))
	}
}

// TestListObjectsMaxKeys tests ListObjects with max-keys parameter
func TestListObjectsMaxKeys(t *testing.T) {
	_, router := setupTestEnvironmentForListObjects(t)

	// Create multiple objects
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file%d.txt", i)
		req := httptest.NewRequest("PUT", fmt.Sprintf("/test-bucket/%s", key), bytes.NewReader([]byte("content")))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
			t.Errorf("Failed to upload %s: status=%d", key, w.Code)
		}
	}

	// Test with max-keys=5
	req := httptest.NewRequest("GET", "/test-bucket?max-keys=5", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("ListObjects failed: status=%d", w.Code)
	}

	var result s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse XML: %v", err)
	}

	if len(result.Contents) > 5 {
		t.Errorf("Expected at most 5 objects, got %d", len(result.Contents))
	}

	if len(result.Contents) == 5 && !result.IsTruncated {
		t.Error("Expected IsTruncated=true when max-keys limit is reached")
	}
}
