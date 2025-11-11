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

// BenchmarkInstantUploadVsNormalUpload benchmarks instant upload vs normal upload
func BenchmarkInstantUploadVsNormalUpload(b *testing.B) {
	_, router := setupTestEnvironmentForInstantUploadPerf(b)
	bucketName := "test-bucket"

	// Prepare test data: same data for instant upload, different data for normal upload
	testDataForInstant := make([]byte, 1024*1024) // 1MB
	for i := range testDataForInstant {
		testDataForInstant[i] = byte(i % 256)
	}

	testDataForNormal := make([]byte, 1024*1024) // 1MB, different content
	for i := range testDataForNormal {
		testDataForNormal[i] = byte((i + 1000) % 256)
	}

	// First upload (baseline)
	key1 := "baseline.txt"
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key1), bytes.NewReader(testDataForInstant))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		b.Fatalf("Baseline PutObject failed: status=%d", w.Code)
	}
	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()

	// Benchmark instant upload (same data)
	b.Run("InstantUpload", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("instant_%d.txt", i)
			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testDataForInstant))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				b.Fatalf("Instant upload failed: status=%d", w.Code)
			}
		}
	})

	// Benchmark normal upload (different data)
	b.Run("NormalUpload", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Create unique data for each upload
			uniqueData := make([]byte, len(testDataForNormal))
			copy(uniqueData, testDataForNormal)
			uniqueData[0] = byte(i % 256) // Make it unique

			key := fmt.Sprintf("normal_%d.txt", i)
			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(uniqueData))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				b.Fatalf("Normal upload failed: status=%d", w.Code)
			}
		}
	})
}

// TestInstantUploadPerformanceComparison tests performance comparison between instant upload and normal upload
func TestInstantUploadPerformanceComparison(t *testing.T) {
	_, router := setupTestEnvironmentForInstantUploadPerf(t)
	bucketName := "test-bucket"

	// Prepare test data sets
	// Data set 1: Can be instant uploaded (duplicate data)
	duplicateData := make([]byte, 512*1024) // 512KB
	for i := range duplicateData {
		duplicateData[i] = byte(i % 256)
	}

	// Data set 2: Cannot be instant uploaded (unique data)
	uniqueData := make([]byte, 512*1024) // 512KB
	for i := range uniqueData {
		uniqueData[i] = byte((i + 10000) % 256)
	}

	// Upload baseline file for instant upload
	baselineKey := "baseline.txt"
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, baselineKey), bytes.NewReader(duplicateData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Baseline PutObject failed: status=%d", w.Code)
	}
	time.Sleep(200 * time.Millisecond)

	// Test instant upload performance (upload same data multiple times)
	numUploads := 10
	instantUploadTimes := make([]time.Duration, numUploads)
	for i := 0; i < numUploads; i++ {
		key := fmt.Sprintf("instant_%d.txt", i)
		startTime := time.Now()
		req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(duplicateData))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		instantUploadTimes[i] = time.Since(startTime)
		if w.Code != http.StatusOK {
			t.Fatalf("Instant upload failed: status=%d", w.Code)
		}
	}

	// Test normal upload performance (upload different data)
	normalUploadTimes := make([]time.Duration, numUploads)
	for i := 0; i < numUploads; i++ {
		// Create unique data for each upload
		uniqueDataCopy := make([]byte, len(uniqueData))
		copy(uniqueDataCopy, uniqueData)
		uniqueDataCopy[0] = byte(i) // Make it unique

		key := fmt.Sprintf("normal_%d.txt", i)
		startTime := time.Now()
		req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(uniqueDataCopy))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		normalUploadTimes[i] = time.Since(startTime)
		if w.Code != http.StatusOK {
			t.Fatalf("Normal upload failed: status=%d", w.Code)
		}
	}

	// Calculate statistics
	var instantTotal, normalTotal time.Duration
	for i := 0; i < numUploads; i++ {
		instantTotal += instantUploadTimes[i]
		normalTotal += normalUploadTimes[i]
	}
	instantAvg := instantTotal / time.Duration(numUploads)
	normalAvg := normalTotal / time.Duration(numUploads)

	// Calculate improvement
	improvement := float64(normalAvg) / float64(instantAvg)
	timeSaved := normalAvg - instantAvg

	t.Logf("\n=== Instant Upload Performance Comparison ===")
	t.Logf("Test data size: %d KB", len(duplicateData)/1024)
	t.Logf("Number of uploads: %d", numUploads)
	t.Logf("\nInstant Upload (duplicate data):")
	t.Logf("  Average time: %v", instantAvg)
	t.Logf("  Total time: %v", instantTotal)
	t.Logf("\nNormal Upload (unique data):")
	t.Logf("  Average time: %v", normalAvg)
	t.Logf("  Total time: %v", normalTotal)
	t.Logf("\nPerformance Improvement:")
	t.Logf("  Speedup: %.2fx faster", improvement)
	t.Logf("  Time saved per upload: %v", timeSaved)
	t.Logf("  Total time saved: %v", normalTotal-instantTotal)

	// Verify instant upload actually happened (files should share DataID)
	handler := core.NewLocalHandler()
	ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Get bucket ID
	bktID, err := getBucketIDByNameForTestPerf(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("Failed to get bucket ID: %v", err)
	}

	baselineObj, err := findObjectByPathForTestPerf(ctx, bktID, baselineKey)
	if err != nil {
		t.Fatalf("Failed to find baseline file: %v", err)
	}

	instantUploadCount := 0
	for i := 0; i < numUploads; i++ {
		key := fmt.Sprintf("instant_%d.txt", i)
		obj, err := findObjectByPathForTestPerf(ctx, bktID, key)
		if err == nil && obj.DataID == baselineObj.DataID {
			instantUploadCount++
		}
	}

	t.Logf("\nInstant Upload Verification:")
	t.Logf("  Files sharing DataID with baseline: %d/%d", instantUploadCount, numUploads)

	if instantUploadCount != numUploads {
		t.Errorf("Expected all instant upload files to share DataID, but only %d/%d do", instantUploadCount, numUploads)
	}

	// Instant upload should be significantly faster
	if improvement < 1.5 {
		t.Logf("Warning: Instant upload improvement (%.2fx) is less than expected (1.5x)", improvement)
	}
}

// TestInstantUploadWithDifferentSizes tests instant upload with different file sizes
func TestInstantUploadWithDifferentSizes(t *testing.T) {
	_, router := setupTestEnvironmentForInstantUploadPerf(t)
	bucketName := "test-bucket"

	// Test different file sizes
	sizes := []int{1024, 10 * 1024, 100 * 1024, 1024 * 1024} // 1KB, 10KB, 100KB, 1MB

	for _, size := range sizes {
		// Create test data
		testData := make([]byte, size)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Upload baseline
		baselineKey := fmt.Sprintf("baseline_%d.txt", size)
		req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, baselineKey), bytes.NewReader(testData))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("Baseline PutObject failed for size %d: status=%d", size, w.Code)
		}
		time.Sleep(200 * time.Millisecond)

		// Upload duplicate (should trigger instant upload)
		startTime := time.Now()
		duplicateKey := fmt.Sprintf("duplicate_%d.txt", size)
		req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, duplicateKey), bytes.NewReader(testData))
		req.Header.Set("Content-Type", "application/octet-stream")
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		uploadDuration := time.Since(startTime)

		if w.Code != http.StatusOK {
			t.Fatalf("Duplicate PutObject failed for size %d: status=%d", size, w.Code)
		}

		// Verify DataID sharing
		handler := core.NewLocalHandler()
		ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
		if err != nil {
			t.Fatalf("Login failed: %v", err)
		}

		// Get bucket ID
		bktID, err := getBucketIDByNameForTestPerf(ctx, "test-bucket")
		if err != nil {
			t.Fatalf("Failed to get bucket ID: %v", err)
		}

		baselineObj, err := findObjectByPathForTestPerf(ctx, bktID, baselineKey)
		if err != nil {
			t.Fatalf("Failed to find baseline file: %v", err)
		}
		duplicateObj, err := findObjectByPathForTestPerf(ctx, bktID, duplicateKey)
		if err != nil {
			t.Fatalf("Failed to find duplicate file: %v", err)
		}

		if baselineObj.DataID != duplicateObj.DataID {
			t.Errorf("Files with size %d should share DataID, but got Baseline DataID=%d, Duplicate DataID=%d", size, baselineObj.DataID, duplicateObj.DataID)
		} else {
			t.Logf("✓ Size %d: Instant upload successful, DataID=%d, upload time=%v", size, baselineObj.DataID, uploadDuration)
		}
	}
}

// setupTestEnvironmentForInstantUploadPerf sets up test environment for instant upload performance tests
func setupTestEnvironmentForInstantUploadPerf(t testing.TB) (int64, *gin.Engine) {
	// Create independent temporary directories for each test
	testID := time.Now().UnixNano() % 1000000000
	baseDir := filepath.Join(os.TempDir(), fmt.Sprintf("o_s3_iup_%d", testID))
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("o_s3d_iup_%d", testID))

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

	ensureTestUserForInstantUploadPerf(t)

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

// findObjectByPathForTestPerf finds object by path (for performance testing)
func findObjectByPathForTestPerf(ctx context.Context, bktID int64, path string) (*core.ObjectInfo, error) {
	handler := core.NewLocalHandler()
	// Ensure context has user info for permission check
	userCtx := core.UserInfo2Ctx(ctx, &core.UserInfo{ID: 1})
	
	parts := splitPathPerf(path)
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

// getBucketIDByNameForTestPerf gets bucket ID by name (for performance testing)
func getBucketIDByNameForTestPerf(ctx context.Context, name string) (int64, error) {
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

// splitPathPerf splits path into parts
func splitPathPerf(path string) []string {
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

// ensureTestUserForInstantUploadPerf ensures test user exists
func ensureTestUserForInstantUploadPerf(t testing.TB) {
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

