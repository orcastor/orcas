package s3_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/s3"
)

// setupTestEnvironment 设置测试环境
func setupTestEnvironmentForCache(t *testing.T) (int64, *gin.Engine) {
	// 启用批量写入以测试批量写入和缓存的协同工作
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")

	// 初始化环境变量
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_s3_cache_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_s3_cache_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}

	core.InitDB("")
	ensureTestUserForCache(t)

	// 创建测试bucket
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// 登录并创建bucket
	handler := core.NewLocalHandler()
	ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:    testBktID,
		Name:  "test-bucket",
		Type:  1,
		Quota: -1, // 无限制配额
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// 设置gin为测试模式
	gin.SetMode(gin.TestMode)

	// 创建测试用的gin engine
	router := gin.New()
	router.Use(func(c *gin.Context) {
		// 模拟JWT认证，设置UID
		c.Set("uid", int64(1))
		c.Request = c.Request.WithContext(core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{
			ID: 1,
		}))
		c.Next()
	})

	// 注册路由
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

// ensureTestUserForCache 确保测试用户存在
func ensureTestUserForCache(t *testing.T) {
	handler := core.NewLocalHandler()
	ctx := context.Background()
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err == nil {
		return
	}

	// 如果登录失败，尝试创建用户（使用与performance_test.go相同的方式）
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetDB()
	if err != nil {
		t.Logf("Warning: Failed to get DB: %v", err)
		return
	}
	defer db.Close()

	// 直接插入用户（简化版本）
	_, err = db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar, key) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
	if err != nil {
		t.Logf("Warning: Failed to create user: %v", err)
	}
}

// TestCachePutObjectListObjects 测试putObject后listObjects能正确看到新对象
func TestCachePutObjectListObjects(t *testing.T) {
	_, router := setupTestEnvironmentForCache(t)
	bucketName := "test-bucket"
	key := "test-object-1"
	testData := []byte("test data")

	// 0. Create bucket via S3 API to ensure it's in the cache
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s", bucketName), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// Bucket may already exist (created in setup), so 200 or 409 is OK
	// But if it returns 500, we should fail the test as bucket won't be available
	if w.Code != http.StatusOK && w.Code != http.StatusConflict {
		t.Fatalf("CreateBucket failed: status=%d, body=%s (bucket must exist for test)", w.Code, w.Body.String())
	}

	// 1. PutObject
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 2. ListObjects - 应该能看到新对象
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ListObjects failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	var result s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal ListBucketResult: %v", err)
	}

	// 验证对象存在
	found := false
	for _, content := range result.Contents {
		if content.Key == key {
			found = true
			if content.Size != int64(len(testData)) {
				t.Errorf("Object size mismatch: expected %d, got %d", len(testData), content.Size)
			}
			break
		}
	}
	if !found {
		t.Errorf("Object %s not found in ListObjects result", key)
	}
}

// TestCacheConcurrentPutObjectListObjects 测试并发putObject和listObjects
func TestCacheConcurrentPutObjectListObjects(t *testing.T) {
	_, router := setupTestEnvironmentForCache(t)
	bucketName := "test-bucket"
	numObjects := 50

	var wg sync.WaitGroup
	errors := make(chan error, numObjects*2)

	// 并发PutObject
	for i := 0; i < numObjects; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("test-object-%d", id)
			testData := []byte(fmt.Sprintf("test data %d", id))

			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("PutObject %d failed: status=%d", id, w.Code)
			}
		}(i)
	}

	// 并发ListObjects
	listConcurrency := 10
	for i := 0; i < listConcurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req := httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("ListObjects %d failed: status=%d", id, w.Code)
				return
			}

			var result s3.ListBucketResult
			if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
				errors <- fmt.Errorf("ListObjects %d unmarshal failed: %v", id, err)
				return
			}

			// 验证至少有一些对象
			if len(result.Contents) == 0 {
				errors <- fmt.Errorf("ListObjects %d returned empty result", id)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查错误
	errorCount := 0
	for err := range errors {
		t.Errorf("Error in concurrent test: %v", err)
		errorCount++
	}

	// 等待一下确保所有写入完成
	time.Sleep(100 * time.Millisecond)

	// 最终验证：ListObjects应该包含所有对象
	req := httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Final ListObjects failed: status=%d", w.Code)
	}

	var result s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal final ListBucketResult: %v", err)
	}

	// 验证所有对象都存在
	objectMap := make(map[string]bool)
	for _, content := range result.Contents {
		objectMap[content.Key] = true
	}

	missingCount := 0
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("test-object-%d", i)
		if !objectMap[key] {
			t.Errorf("Object %s missing from final ListObjects", key)
			missingCount++
		}
	}

	if missingCount > 0 {
		t.Errorf("Missing %d objects in final ListObjects (found %d/%d)", missingCount, len(result.Contents), numObjects)
	}
}

// TestCacheConcurrentPutDeleteList 测试并发put、delete和list操作
func TestCacheConcurrentPutDeleteList(t *testing.T) {
	_, router := setupTestEnvironmentForCache(t)
	bucketName := "test-bucket"
	numOperations := 100

	var wg sync.WaitGroup
	errors := make(chan error, numOperations*3)
	objectCounts := make(map[int]int)
	var mu sync.Mutex

	// 并发操作：put、delete、list
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			key := fmt.Sprintf("test-object-%d", id)
			testData := []byte(fmt.Sprintf("test data %d", id))

			// PutObject
			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("PutObject %d failed: status=%d", id, w.Code)
				return
			}

			// ListObjects
			req = httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
			w = httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("ListObjects after put %d failed: status=%d", id, w.Code)
				return
			}

			var result s3.ListBucketResult
			if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
				errors <- fmt.Errorf("ListObjects after put %d unmarshal failed: %v", id, err)
				return
			}

			mu.Lock()
			objectCounts[id] = len(result.Contents)
			mu.Unlock()

			// 验证对象存在
			found := false
			for _, content := range result.Contents {
				if content.Key == key {
					found = true
					break
				}
			}
			if !found {
				errors <- fmt.Errorf("Object %s not found in ListObjects after put", key)
			}

			// DeleteObject (只删除偶数ID的对象)
			if id%2 == 0 {
				req = httptest.NewRequest("DELETE", fmt.Sprintf("/%s/%s", bucketName, key), nil)
				w = httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if w.Code != http.StatusNoContent {
					errors <- fmt.Errorf("DeleteObject %d failed: status=%d", id, w.Code)
					return
				}

				// ListObjects after delete
				req = httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
				w = httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if w.Code != http.StatusOK {
					errors <- fmt.Errorf("ListObjects after delete %d failed: status=%d", id, w.Code)
					return
				}

				var result2 s3.ListBucketResult
				if err := xml.Unmarshal(w.Body.Bytes(), &result2); err != nil {
					errors <- fmt.Errorf("ListObjects after delete %d unmarshal failed: %v", id, err)
					return
				}

				// 验证对象不存在
				for _, content := range result2.Contents {
					if content.Key == key {
						errors <- fmt.Errorf("Object %s still found in ListObjects after delete", key)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查错误
	errorCount := 0
	for err := range errors {
		t.Errorf("Error in concurrent put/delete/list test: %v", err)
		errorCount++
	}

	// 等待一下确保所有操作完成
	time.Sleep(200 * time.Millisecond)

	// 最终验证
	req := httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Final ListObjects failed: status=%d", w.Code)
	}

	var result s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal final ListBucketResult: %v", err)
	}

	// 验证：只有奇数ID的对象应该存在
	objectMap := make(map[string]bool)
	for _, content := range result.Contents {
		objectMap[content.Key] = true
	}

	expectedCount := numOperations / 2 // 只有奇数ID的对象
	if len(result.Contents) != expectedCount {
		t.Errorf("Expected %d objects in final ListObjects, got %d", expectedCount, len(result.Contents))
	}

	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("test-object-%d", i)
		shouldExist := i%2 != 0
		exists := objectMap[key]
		if shouldExist && !exists {
			t.Errorf("Object %s should exist but is missing", key)
		} else if !shouldExist && exists {
			t.Errorf("Object %s should not exist but is present", key)
		}
	}
}

// TestCacheUpdateObject 测试更新对象后listObjects能看到更新
func TestCacheUpdateObject(t *testing.T) {
	_, router := setupTestEnvironmentForCache(t)
	bucketName := "test-bucket"
	key := "test-object-update"
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2 updated")

	// 1. PutObject
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData1))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d", w.Code)
	}

	// 2. ListObjects - 验证初始大小
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ListObjects failed: status=%d", w.Code)
	}

	var result1 s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result1); err != nil {
		t.Fatalf("Failed to unmarshal ListBucketResult: %v", err)
	}

	var initialSize int64
	for _, content := range result1.Contents {
		if content.Key == key {
			initialSize = content.Size
			break
		}
	}

	if initialSize != int64(len(testData1)) {
		t.Errorf("Initial size mismatch: expected %d, got %d", len(testData1), initialSize)
	}

	// 3. UpdateObject (PutObject with same key)
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData2))
	req.Header.Set("Content-Type", "application/octet-stream")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("UpdateObject failed: status=%d", w.Code)
	}

	// 4. ListObjects - 验证更新后的大小
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ListObjects after update failed: status=%d", w.Code)
	}

	var result2 s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result2); err != nil {
		t.Fatalf("Failed to unmarshal ListBucketResult after update: %v", err)
	}

	var updatedSize int64
	for _, content := range result2.Contents {
		if content.Key == key {
			updatedSize = content.Size
			break
		}
	}

	if updatedSize != int64(len(testData2)) {
		t.Errorf("Updated size mismatch: expected %d, got %d", len(testData2), updatedSize)
	}

	if updatedSize == initialSize {
		t.Errorf("Size should have changed after update")
	}
}

// TestCacheNestedPaths 测试嵌套路径的缓存一致性
func TestCacheNestedPaths(t *testing.T) {
	_, router := setupTestEnvironmentForCache(t)
	bucketName := "test-bucket"

	// 创建嵌套路径的对象
	paths := []string{
		"dir1/file1.txt",
		"dir1/file2.txt",
		"dir1/dir2/file3.txt",
		"dir1/dir2/file4.txt",
	}

	for _, path := range paths {
		testData := []byte(fmt.Sprintf("data for %s", path))
		req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, path), bytes.NewReader(testData))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("PutObject %s failed: status=%d", path, w.Code)
		}
	}

	// ListObjects with prefix
	req := httptest.NewRequest("GET", fmt.Sprintf("/%s?prefix=dir1/", bucketName), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ListObjects with prefix failed: status=%d", w.Code)
	}

	var result s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal ListBucketResult: %v", err)
	}

	// 验证所有对象都存在
	objectMap := make(map[string]bool)
	for _, content := range result.Contents {
		objectMap[content.Key] = true
	}

	for _, path := range paths {
		if !objectMap[path] {
			t.Errorf("Object %s not found in ListObjects with prefix", path)
		}
	}
}

// TestCacheStressTest 压力测试：大量并发操作
func TestCacheStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	_, router := setupTestEnvironmentForCache(t)
	bucketName := "test-bucket"
	numObjects := 200
	concurrency := 50

	var wg sync.WaitGroup
	errors := make(chan error, numObjects*2)

	// 并发PutObject
	for i := 0; i < numObjects; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("stress-test-object-%d", id)
			testData := []byte(fmt.Sprintf("stress test data %d", id))

			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("PutObject %d failed: status=%d", id, w.Code)
			}
		}(i)
	}

	// 并发ListObjects
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req := httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("ListObjects %d failed: status=%d", id, w.Code)
				return
			}

			var result s3.ListBucketResult
			if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
				errors <- fmt.Errorf("ListObjects %d unmarshal failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查错误
	errorCount := 0
	for err := range errors {
		t.Errorf("Error in stress test: %v", err)
		errorCount++
	}

	// 等待确保所有写入完成
	time.Sleep(500 * time.Millisecond)

	// 最终验证
	req := httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Final ListObjects failed: status=%d", w.Code)
	}

	var result s3.ListBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal final ListBucketResult: %v", err)
	}

	// 验证所有对象都存在
	objectMap := make(map[string]bool)
	for _, content := range result.Contents {
		if len(content.Key) > 0 {
			objectMap[content.Key] = true
		}
	}

	missingCount := 0
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("stress-test-object-%d", i)
		if !objectMap[key] {
			missingCount++
		}
	}

	if missingCount > 0 {
		t.Errorf("Missing %d objects in final ListObjects (found %d/%d)", missingCount, len(result.Contents), numObjects)
	}
}
