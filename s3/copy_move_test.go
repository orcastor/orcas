package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// ==================== Copy 和 Move 操作测试用例 ====================

// TestCopyObjectSameBucket 测试同一bucket内的复制操作
func TestCopyObjectSameBucket(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	sourceKey := "source-file.txt"
	destKey := "dest-file.txt"

	// 1. 创建源文件
	testData := []byte("Hello, World! This is test data for copy operation.")
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, sourceKey), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 等待文件写入完成
	time.Sleep(200 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, sourceKey, 10) {
		t.Fatalf("Source file not found after upload")
	}

	// 2. 复制文件
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
	req.Header.Set("x-amz-copy-source", fmt.Sprintf("/%s/%s", bucketName, sourceKey))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CopyObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 3. 验证目标文件存在且内容正确
	time.Sleep(200 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, destKey, 10) {
		t.Fatalf("Destination file not found after copy")
	}

	// 4. 读取目标文件验证内容
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	if !bytes.Equal(w.Body.Bytes(), testData) {
		t.Errorf("Copied data mismatch: expected %d bytes, got %d bytes", len(testData), len(w.Body.Bytes()))
	}

	// 5. 验证源文件仍然存在
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, sourceKey), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Source file should still exist: status=%d", w.Code)
	}
}

// TestCopyObjectLargeFile 测试复制大文件（跨多个chunk）
func TestCopyObjectLargeFile(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	sourceKey := "large-source-file"
	destKey := "large-dest-file"

	// 创建大文件（12MB，跨越3个chunk）
	chunkSize := int64(4 * 1024 * 1024) // 4MB
	fileSize := chunkSize * 3           // 12MB
	testData := generateLargeTestData(fileSize)

	// 1. 上传源文件
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, sourceKey), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 等待文件写入完成
	time.Sleep(500 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, sourceKey, 10) {
		t.Fatalf("Source file not found after upload")
	}

	// 2. 复制大文件
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
	req.Header.Set("x-amz-copy-source", fmt.Sprintf("/%s/%s", bucketName, sourceKey))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CopyObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 3. 验证目标文件
	time.Sleep(500 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, destKey, 10) {
		t.Fatalf("Destination file not found after copy")
	}

	// 4. 读取目标文件验证内容（读取部分数据）
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
	req.Header.Set("Range", "bytes=0-1048575") // 读取前1MB
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("Range read failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	expectedData := testData[0:1048576]
	if !bytes.Equal(w.Body.Bytes(), expectedData) {
		t.Errorf("Copied data mismatch in first 1MB")
	}
}

// TestMoveObjectSameBucket 测试同一bucket内的移动操作
func TestMoveObjectSameBucket(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	sourceKey := "old-name.txt"
	destKey := "new-name.txt"

	// 1. 创建源文件
	testData := []byte("Hello, World! This is test data for move operation.")
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, sourceKey), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 等待文件写入完成
	time.Sleep(200 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, sourceKey, 10) {
		t.Fatalf("Source file not found after upload")
	}

	// 2. 移动文件
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
	req.Header.Set("x-amz-move-source", fmt.Sprintf("/%s/%s", bucketName, sourceKey))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("MoveObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 3. 验证目标文件存在且内容正确
	time.Sleep(200 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, destKey, 10) {
		t.Fatalf("Destination file not found after move")
	}

	// 4. 读取目标文件验证内容
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	if !bytes.Equal(w.Body.Bytes(), testData) {
		t.Errorf("Moved data mismatch: expected %d bytes, got %d bytes", len(testData), len(w.Body.Bytes()))
	}

	// 5. 验证源文件已被删除
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, sourceKey), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("Source file should be deleted: status=%d", w.Code)
	}
}

// TestMoveObjectToSubdirectory 测试移动到子目录
func TestMoveObjectToSubdirectory(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	sourceKey := "file.txt"
	destKey := "subdir/moved-file.txt"

	// 1. 创建源文件
	testData := []byte("Test data for move to subdirectory")
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, sourceKey), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	time.Sleep(200 * time.Millisecond)

	// 2. 移动到子目录
	req = httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
	req.Header.Set("x-amz-move-source", fmt.Sprintf("/%s/%s", bucketName, sourceKey))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("MoveObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	// 3. 验证目标文件存在
	time.Sleep(200 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, destKey, 10) {
		t.Fatalf("Destination file not found after move")
	}

	// 4. 验证源文件已被删除
	req = httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, sourceKey), nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("Source file should be deleted: status=%d", w.Code)
	}
}

// ==================== 并发测试用例 ====================

// TestConcurrentPutObjectCopyMove 测试并发上传（用于copy/move测试）
func TestConcurrentPutObjectCopyMove(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	numConcurrent := 10

	var wg sync.WaitGroup
	errors := make(chan error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-file-%d.txt", idx)
			testData := []byte(fmt.Sprintf("Concurrent test data %d", idx))
			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("PutObject %d failed: status=%d, body=%s", idx, w.Code, w.Body.String())
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// 验证所有文件都存在
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < numConcurrent; i++ {
		key := fmt.Sprintf("concurrent-file-%d.txt", i)
		if !waitForObjectExists(t, router, bucketName, key, 5) {
			t.Errorf("File %s not found after concurrent upload", key)
		}
	}
}

// TestConcurrentGetObjectCopyMove 测试并发读取（用于copy/move测试）
func TestConcurrentGetObjectCopyMove(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	key := "concurrent-read-file.txt"

	// 1. 先上传文件
	testData := generateLargeTestData(1024 * 1024) // 1MB
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	time.Sleep(200 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, key, 10) {
		t.Fatalf("File not found after upload")
	}

	// 2. 并发读取
	numConcurrent := 10
	var wg sync.WaitGroup
	errors := make(chan error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("GetObject %d failed: status=%d", idx, w.Code)
				return
			}
			if !bytes.Equal(w.Body.Bytes(), testData) {
				errors <- fmt.Errorf("GetObject %d data mismatch", idx)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestConcurrentCopyObject 测试并发复制
func TestConcurrentCopyObject(t *testing.T) {
	_, router := setupTestEnvironmentForMultipart(t)
	bucketName := "test-bucket"
	sourceKey := "source-concurrent.txt"

	// 1. 创建源文件
	testData := []byte("Source data for concurrent copy test")
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, sourceKey), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
	}

	time.Sleep(200 * time.Millisecond)
	if !waitForObjectExists(t, router, bucketName, sourceKey, 10) {
		t.Fatalf("Source file not found after upload")
	}

	// 2. 并发复制
	numConcurrent := 5
	var wg sync.WaitGroup
	errors := make(chan error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			destKey := fmt.Sprintf("dest-concurrent-%d.txt", idx)
			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, destKey), nil)
			req.Header.Set("x-amz-copy-source", fmt.Sprintf("/%s/%s", bucketName, sourceKey))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errors <- fmt.Errorf("CopyObject %d failed: status=%d, body=%s", idx, w.Code, w.Body.String())
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// 验证所有复制的文件都存在
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < numConcurrent; i++ {
		destKey := fmt.Sprintf("dest-concurrent-%d.txt", i)
		if !waitForObjectExists(t, router, bucketName, destKey, 5) {
			t.Errorf("Copied file %s not found", destKey)
		}
	}
}
