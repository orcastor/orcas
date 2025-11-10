package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
)

// PerformanceMetrics 性能指标
type PerformanceMetrics struct {
	TestName       string
	Operation      string
	DataSize       int64
	Concurrency    int
	Operations     int
	Duration       time.Duration
	ThroughputMBps float64
	OpsPerSecond   float64
	AvgLatency     time.Duration
	MaxMemoryMB    float64
	TotalAllocMB   float64
	NumGC          uint32
}

// ensureTestUser 确保测试用户存在
func ensureTestUser(t testHelper) {
	handler := core.NewLocalHandler()
	ctx := context.Background()
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err == nil {
		return
	}

	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetDB()
	if err != nil {
		t.Logf("Warning: Failed to get DB: %v", err)
		return
	}
	defer db.Close()

	_, err = db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar, key) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
	if err != nil {
		t.Logf("Warning: Failed to create test user: %v", err)
	}
}

// testHelper 测试辅助接口
type testHelper interface {
	Fatalf(format string, args ...interface{})
	Logf(format string, args ...interface{})
}

// setupTestEnvironment 设置测试环境
func setupTestEnvironment(t testHelper) (int64, *gin.Engine) {
	// 初始化环境变量
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_s3_perf_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_s3_perf_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}

	core.InitDB()
	ensureTestUser(t)

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
		UID:   1,
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

	// 注册S3路由
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

// generateTestData 生成测试数据
func generateTestData(size int64) []byte {
	data := make([]byte, size)
	for i := int64(0); i < size; i++ {
		data[i] = byte(i % 256)
	}
	return data
}

// BenchmarkPutObject 测试PutObject性能
func BenchmarkPutObject(b *testing.B) {
	_, router := setupTestEnvironment(b)
	bucketName := "test-bucket"
	dataSize := int64(1024 * 1024) // 1MB
	testData := generateTestData(dataSize)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("test-object-%d", i)
		req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			b.Fatalf("PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
		}
	}
}

// BenchmarkGetObject 测试GetObject性能
func BenchmarkGetObject(b *testing.B) {
	_, router := setupTestEnvironment(b)
	bucketName := "test-bucket"
	dataSize := int64(1024 * 1024) // 1MB
	testData := generateTestData(dataSize)

	// 先上传一个对象
	key := "test-object"
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		b.Fatalf("Setup PutObject failed: status=%d", w.Code)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", fmt.Sprintf("/%s/%s", bucketName, key), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			b.Fatalf("GetObject failed: status=%d", w.Code)
		}

		// 验证数据
		if int64(len(w.Body.Bytes())) != dataSize {
			b.Fatalf("Data size mismatch: expected=%d, got=%d", dataSize, len(w.Body.Bytes()))
		}
	}
}

// BenchmarkListObjects 测试ListObjects性能
func BenchmarkListObjects(b *testing.B) {
	_, router := setupTestEnvironment(b)
	bucketName := "test-bucket"
	dataSize := int64(1024) // 1KB
	testData := generateTestData(dataSize)

	// 先上传一些对象
	numObjects := 100
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("test-object-%d", i)
		req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("Setup PutObject failed: status=%d", w.Code)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", fmt.Sprintf("/%s", bucketName), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			b.Fatalf("ListObjects failed: status=%d", w.Code)
		}
	}
}

// BenchmarkHeadObject 测试HeadObject性能
func BenchmarkHeadObject(b *testing.B) {
	_, router := setupTestEnvironment(b)
	bucketName := "test-bucket"
	dataSize := int64(1024 * 1024) // 1MB
	testData := generateTestData(dataSize)

	// 先上传一个对象
	key := "test-object"
	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		b.Fatalf("Setup PutObject failed: status=%d", w.Code)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("HEAD", fmt.Sprintf("/%s/%s", bucketName, key), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			b.Fatalf("HeadObject failed: status=%d", w.Code)
		}
	}
}

// runConcurrentTest 运行并发测试
func runConcurrentTest(t *testing.T, name, operation string, dataSize int64, concurrency, operations int) PerformanceMetrics {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	bktID, router := setupTestEnvironment(t)
	bucketName := "test-bucket"
	testData := generateTestData(dataSize)

	// 对于GET操作，需要先上传对象
	if operation == "GET" || operation == "HEAD" {
		// 上传10个对象供GET测试使用
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("test-object-%d", i)
			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("Setup PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
			}
		}
		// 等待数据写入完成
		time.Sleep(100 * time.Millisecond)
		// 清空缓存以确保测试真实性能
		_ = bktID // 使用bktID避免未使用变量警告
	}

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(concurrency)

	opsPerGoroutine := operations / concurrency
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				var req *http.Request
				var path string

				switch operation {
				case "PUT":
					key := fmt.Sprintf("test-object-%d-%d", id, j)
					path = fmt.Sprintf("/%s/%s", bucketName, key)
					req = httptest.NewRequest("PUT", path, bytes.NewReader(testData))
					req.Header.Set("Content-Type", "application/octet-stream")
				case "GET":
					key := fmt.Sprintf("test-object-%d", j%10) // 循环使用前10个对象
					path = fmt.Sprintf("/%s/%s", bucketName, key)
					req = httptest.NewRequest("GET", path, nil)
				case "LIST":
					path = fmt.Sprintf("/%s", bucketName)
					req = httptest.NewRequest("GET", path, nil)
				case "HEAD":
					key := fmt.Sprintf("test-object-%d", j%10)
					path = fmt.Sprintf("/%s/%s", bucketName, key)
					req = httptest.NewRequest("HEAD", path, nil)
				default:
					errors <- fmt.Errorf("unknown operation: %s", operation)
					return
				}

				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)

				if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
					errors <- fmt.Errorf("operation failed: status=%d, body=%s", w.Code, w.Body.String())
					return
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// 检查错误
	close(errors)
	for err := range errors {
		t.Errorf("Error in concurrent test: %v", err)
	}

	runtime.ReadMemStats(&m2)

	// 计算吞吐量
	totalData := int64(operations) * dataSize
	throughputMBps := float64(totalData) / duration.Seconds() / (1024 * 1024)
	opsPerSecond := float64(operations) / duration.Seconds()
	avgLatency := duration / time.Duration(operations)

	return PerformanceMetrics{
		TestName:       name,
		Operation:      operation,
		DataSize:       dataSize,
		Concurrency:    concurrency,
		Operations:     operations,
		Duration:       duration,
		ThroughputMBps: throughputMBps,
		OpsPerSecond:   opsPerSecond,
		AvgLatency:     avgLatency,
		MaxMemoryMB:    float64(m2.HeapInuse-m1.HeapInuse) / (1024 * 1024),
		TotalAllocMB:   float64(m2.TotalAlloc-m1.TotalAlloc) / (1024 * 1024),
		NumGC:          m2.NumGC - m1.NumGC,
	}
}

// TestConcurrentPutObject 测试并发PutObject
func TestConcurrentPutObject(t *testing.T) {
	metrics := runConcurrentTest(t, "ConcurrentPutObject", "PUT", 1024*1024, 10, 100)
	t.Logf("ConcurrentPutObject: %+v", metrics)
}

// TestConcurrentGetObject 测试并发GetObject
func TestConcurrentGetObject(t *testing.T) {
	// 先上传一些对象
	_, router := setupTestEnvironment(t)
	bucketName := "test-bucket"
	dataSize := int64(1024 * 1024)
	testData := generateTestData(dataSize)

	// 上传对象，确保上传完成
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("test-object-%d", i)
		req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("Setup PutObject failed: status=%d, body=%s", w.Code, w.Body.String())
		}
	}

	// 等待一下确保数据写入完成
	time.Sleep(100 * time.Millisecond)

	metrics := runConcurrentTest(t, "ConcurrentGetObject", "GET", dataSize, 10, 100)
	t.Logf("ConcurrentGetObject: %+v", metrics)
}

// TestConcurrentListObjects 测试并发ListObjects
func TestConcurrentListObjects(t *testing.T) {
	metrics := runConcurrentTest(t, "ConcurrentListObjects", "LIST", 0, 10, 100)
	t.Logf("ConcurrentListObjects: %+v", metrics)
}

// TestPerformanceReport 生成性能测试报告
func TestPerformanceReport(t *testing.T) {
	report := &strings.Builder{}
	report.WriteString("# S3 Performance Test Report\n\n")
	report.WriteString(fmt.Sprintf("Generated at: %s\n\n", time.Now().Format(time.RFC3339)))

	// 测试不同大小的对象
	sizes := []int64{1024, 1024 * 1024, 10 * 1024 * 1024} // 1KB, 1MB, 10MB
	concurrencies := []int{1, 10, 50, 100}

	report.WriteString("## Single Operation Performance\n\n")
	report.WriteString("| Operation | Data Size | Ops/sec | Avg Latency | Throughput (MB/s) |\n")
	report.WriteString("|-----------|-----------|---------|-------------|-------------------|\n")

	// PutObject测试
	for _, size := range sizes {
		_, router := setupTestEnvironment(t)
		bucketName := "test-bucket"
		testData := generateTestData(size)

		start := time.Now()
		ops := 100
		for i := 0; i < ops; i++ {
			key := fmt.Sprintf("test-object-%d", i)
			req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("PutObject failed: status=%d", w.Code)
			}
		}
		duration := time.Since(start)
		opsPerSec := float64(ops) / duration.Seconds()
		avgLatency := duration / time.Duration(ops)
		throughput := float64(size*int64(ops)) / duration.Seconds() / (1024 * 1024)

		report.WriteString(fmt.Sprintf("| PutObject | %s | %.2f | %v | %.2f |\n",
			formatSize(size), opsPerSec, avgLatency, throughput))
	}

	report.WriteString("\n## Concurrent Performance\n\n")
	report.WriteString("| Operation | Concurrency | Data Size | Ops/sec | Avg Latency | Throughput (MB/s) |\n")
	report.WriteString("|-----------|-------------|-----------|---------|-------------|-------------------|\n")

	// 并发测试
	for _, concurrency := range concurrencies {
		for _, size := range []int64{1024 * 1024} { // 只测试1MB
			metrics := runConcurrentTest(t, fmt.Sprintf("ConcurrentPut-%d", concurrency), "PUT", size, concurrency, 100)
			report.WriteString(fmt.Sprintf("| PutObject | %d | %s | %.2f | %v | %.2f |\n",
				concurrency, formatSize(size), metrics.OpsPerSecond, metrics.AvgLatency, metrics.ThroughputMBps))
		}
	}

	// 写入报告文件
	reportFile := "PERFORMANCE_TEST_REPORT.md"
	err := os.WriteFile(reportFile, []byte(report.String()), 0644)
	if err != nil {
		t.Fatalf("Failed to write report: %v", err)
	}
	t.Logf("Performance report written to %s", reportFile)
}

// formatSize 格式化大小
func formatSize(size int64) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	} else if size < 1024*1024 {
		return fmt.Sprintf("%dKB", size/1024)
	} else {
		return fmt.Sprintf("%dMB", size/(1024*1024))
	}
}
