package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/s3/util"
)

// BenchmarkHTTPHeaderFormatting benchmarks the optimized header formatting functions
func BenchmarkHTTPHeaderFormatting(b *testing.B) {
	testDataID := int64(1234567890123456789)
	testSize := int64(1024 * 1024)
	testMTime := int64(1609459200) // 2021-01-01 00:00:00 UTC

	b.Run("FormatETag", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = util.FormatETag(testDataID)
		}
	})

	b.Run("FormatContentLength", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = util.FormatContentLength(testSize)
		}
	})

	b.Run("FormatLastModified", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = util.FormatLastModified(testMTime)
		}
	})

	b.Run("FormatContentRangeHeader", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = util.FormatContentRangeHeader(0, testSize-1, testSize)
		}
	})
}

// BenchmarkGetObjectHeaders benchmarks GetObject header setting performance
func BenchmarkGetObjectHeaders(b *testing.B) {
	_, router := setupTestEnvironmentForHTTPPerf(b)
	bucketName := "test-bucket"
	key := "test-object"

	// Upload a test object first
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		b.Fatalf("Failed to upload test object: status=%d", w.Code)
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
	}
}

// BenchmarkPutObjectHeaders benchmarks PutObject header setting performance
func BenchmarkPutObjectHeaders(b *testing.B) {
	_, router := setupTestEnvironmentForHTTPPerf(b)
	bucketName := "test-bucket"

	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("test-object-%d", i)
		req := httptest.NewRequest("PUT", fmt.Sprintf("/%s/%s", bucketName, key), bytes.NewReader(testData))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("PutObject failed: status=%d", w.Code)
		}
	}
}

// setupTestEnvironmentForHTTPPerf sets up test environment for HTTP performance tests
func setupTestEnvironmentForHTTPPerf(t testing.TB) (int64, *gin.Engine) {
	// Ensure test user exists
	ensureTestUser(t)

	handler := core.NewLocalHandler()
	ctx := context.Background()
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Create test bucket
	admin := core.NewLocalAdmin()
	testBktID := core.NewID()
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
	bktID := testBktID

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

	return bktID, router
}
