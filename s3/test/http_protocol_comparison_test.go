package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// ProtocolPerformanceMetrics stores performance metrics for a protocol
type ProtocolPerformanceMetrics struct {
	Protocol       string
	OpsPerSecond   float64
	ThroughputMBps float64
	AvgLatency     time.Duration
	MaxMemoryMB    float64
	NumGC          uint32
	Duration       time.Duration
}

// generateSelfSignedCert is defined in http2_test.go

// testHTTP11Performance tests HTTP/1.1 performance using real network connection
// This ensures fair comparison with HTTP/2 and HTTP/3
func testHTTP11Performance(t *testing.T, dataSize int64, concurrency, operations int) ProtocolPerformanceMetrics {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Setup test environment
	_, router := setupTestEnvironment(t)

	// Create HTTP server without TLS (HTTP/1.1 doesn't require TLS)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	server := &http.Server{
		Addr:    listener.Addr().String(),
		Handler: router,
	}

	// Start server in background
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(listener)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	baseURL := fmt.Sprintf("http://%s", listener.Addr().String())
	bucketName := "test-bucket"
	testData := generateTestData(dataSize)

	// Create HTTP/1.1 client with connection reuse
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        concurrency,
			MaxIdleConnsPerHost: concurrency,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 60 * time.Second,
	}

	// Warm up connection
	warmupReq, _ := http.NewRequest("PUT", fmt.Sprintf("%s/%s/warmup", baseURL, bucketName), bytes.NewReader([]byte("warmup")))
	warmupReq.Header.Set("Content-Type", "application/octet-stream")
	if warmupResp, err := client.Do(warmupReq); err == nil {
		warmupResp.Body.Close()
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
				key := fmt.Sprintf("test-object-%d-%d", id, j)
				req, err := http.NewRequest("PUT", fmt.Sprintf("%s/%s/%s", baseURL, bucketName, key), bytes.NewReader(testData))
				if err != nil {
					errors <- fmt.Errorf("HTTP/1.1 failed to create request: %v", err)
					return
				}
				req.Header.Set("Content-Type", "application/octet-stream")

				resp, err := client.Do(req)
				if err != nil {
					errors <- fmt.Errorf("HTTP/1.1 request failed: %v", err)
					return
				}
				resp.Body.Close()

				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
					errors <- fmt.Errorf("HTTP/1.1 operation failed: status=%d", resp.StatusCode)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Shutdown server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	<-serverDone

	close(errors)
	for err := range errors {
		t.Errorf("Error: %v", err)
	}

	runtime.ReadMemStats(&m2)

	totalData := int64(operations) * dataSize
	throughputMBps := float64(totalData) / duration.Seconds() / (1024 * 1024)
	opsPerSecond := float64(operations) / duration.Seconds()
	avgLatency := duration / time.Duration(operations)

	return ProtocolPerformanceMetrics{
		Protocol:       "HTTP/1.1",
		OpsPerSecond:   opsPerSecond,
		ThroughputMBps: throughputMBps,
		AvgLatency:     avgLatency,
		MaxMemoryMB:    float64(m2.HeapInuse-m1.HeapInuse) / (1024 * 1024),
		NumGC:          m2.NumGC - m1.NumGC,
		Duration:       duration,
	}
}

// testHTTP2Performance tests HTTP/2 performance using h2c (cleartext, no TLS)
// This excludes TLS overhead for fair comparison
func testHTTP2Performance(t *testing.T, dataSize int64, concurrency, operations int) ProtocolPerformanceMetrics {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Setup test environment
	_, router := setupTestEnvironment(t)

	// Create HTTP server without TLS (using h2c for cleartext HTTP/2)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	// Wrap handler with h2c to support HTTP/2 over cleartext
	h2s := &http2.Server{}
	h2cHandler := h2c.NewHandler(router, h2s)

	server := &http.Server{
		Addr:    listener.Addr().String(),
		Handler: h2cHandler,
	}

	// Start server in background
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(listener)
	}()

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	baseURL := fmt.Sprintf("http://%s", listener.Addr().String())
	bucketName := "test-bucket"
	testData := generateTestData(dataSize)

	// Create HTTP/2 client with h2c support (cleartext, no TLS)
	transport := &http2.Transport{
		// Allow HTTP (cleartext) for h2c
		AllowHTTP: true,
		// Dial function for cleartext HTTP/2
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			return net.Dial(network, addr)
		},
		// Set reasonable timeouts
		ReadIdleTimeout:  30 * time.Second,
		PingTimeout:      15 * time.Second,
		WriteByteTimeout: 10 * time.Second,
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}

	// Warm up connection
	warmupReq, _ := http.NewRequest("PUT", fmt.Sprintf("%s/%s/warmup", baseURL, bucketName), bytes.NewReader([]byte("warmup")))
	warmupReq.Header.Set("Content-Type", "application/octet-stream")
	if warmupResp, err := client.Do(warmupReq); err == nil {
		warmupResp.Body.Close()
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
				key := fmt.Sprintf("test-object-%d-%d", id, j)
				req, err := http.NewRequest("PUT", fmt.Sprintf("%s/%s/%s", baseURL, bucketName, key), bytes.NewReader(testData))
				if err != nil {
					errors <- fmt.Errorf("HTTP/2 failed to create request: %v", err)
					return
				}
				req.Header.Set("Content-Type", "application/octet-stream")

				resp, err := client.Do(req)
				if err != nil {
					errors <- fmt.Errorf("HTTP/2 request failed: %v", err)
					return
				}
				resp.Body.Close()

				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
					errors <- fmt.Errorf("HTTP/2 operation failed: status=%d", resp.StatusCode)
					return
				}

				// Verify HTTP/2 protocol
				if resp.ProtoMajor != 2 {
					t.Logf("Warning: HTTP/2 request used HTTP/%d.%d instead", resp.ProtoMajor, resp.ProtoMinor)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Shutdown server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	<-serverDone

	close(errors)
	for err := range errors {
		t.Errorf("Error: %v", err)
	}

	runtime.ReadMemStats(&m2)

	totalData := int64(operations) * dataSize
	throughputMBps := float64(totalData) / duration.Seconds() / (1024 * 1024)
	opsPerSecond := float64(operations) / duration.Seconds()
	avgLatency := duration / time.Duration(operations)

	return ProtocolPerformanceMetrics{
		Protocol:       "HTTP/2",
		OpsPerSecond:   opsPerSecond,
		ThroughputMBps: throughputMBps,
		AvgLatency:     avgLatency,
		MaxMemoryMB:    float64(m2.HeapInuse-m1.HeapInuse) / (1024 * 1024),
		NumGC:          m2.NumGC - m1.NumGC,
		Duration:       duration,
	}
}

// testHTTP3Performance removed - HTTP/3 testing no longer supported
func testHTTP3Performance(t *testing.T, dataSize int64, concurrency, operations int) ProtocolPerformanceMetrics {
	t.Skip("HTTP/3 testing has been removed")
	return ProtocolPerformanceMetrics{}
}

// TestHTTPProtocolComparison compares HTTP/1.1 and HTTP/2 performance with batch write enabled
func TestHTTPProtocolComparison(t *testing.T) {
	// Ensure batch write is enabled
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	defer os.Unsetenv("ORCAS_BATCH_WRITE_ENABLED")

	// Test parameters
	dataSize := int64(1024) // 1KB (small file, benefits from batch write)
	concurrency := 10
	operations := 100

	t.Logf("\n=== HTTP Protocol Performance Comparison (Batch Write Enabled) ===")
	t.Logf("Data Size: %d bytes, Concurrency: %d, Operations: %d\n", dataSize, concurrency, operations)

	// Test HTTP/1.1
	t.Logf("Testing HTTP/1.1...")
	h1Metrics := testHTTP11Performance(t, dataSize, concurrency, operations)
	t.Logf("HTTP/1.1 Results: %.2f ops/sec, %.2f MB/s, %v avg latency, %.2f MB memory, %d GC",
		h1Metrics.OpsPerSecond, h1Metrics.ThroughputMBps, h1Metrics.AvgLatency,
		h1Metrics.MaxMemoryMB, h1Metrics.NumGC)

	// Test HTTP/2
	t.Logf("\nTesting HTTP/2...")
	h2Metrics := testHTTP2Performance(t, dataSize, concurrency, operations)
	t.Logf("HTTP/2 Results: %.2f ops/sec, %.2f MB/s, %v avg latency, %.2f MB memory, %d GC",
		h2Metrics.OpsPerSecond, h2Metrics.ThroughputMBps, h2Metrics.AvgLatency,
		h2Metrics.MaxMemoryMB, h2Metrics.NumGC)

	// Calculate improvements
	if h2Metrics.OpsPerSecond > 0 && h1Metrics.OpsPerSecond > 0 {
		h2Improvement := ((h2Metrics.OpsPerSecond - h1Metrics.OpsPerSecond) / h1Metrics.OpsPerSecond) * 100
		throughputImprovement := ((h2Metrics.ThroughputMBps - h1Metrics.ThroughputMBps) / h1Metrics.ThroughputMBps) * 100
		latencyReduction := ((float64(h1Metrics.AvgLatency) - float64(h2Metrics.AvgLatency)) / float64(h1Metrics.AvgLatency)) * 100

		t.Logf("\n=== Comparison Results ===")
		t.Logf("HTTP/2 vs HTTP/1.1: %.2f%% change in ops/sec", h2Improvement)
		t.Logf("HTTP/2 vs HTTP/1.1: %.2f%% change in throughput", throughputImprovement)
		t.Logf("HTTP/2 vs HTTP/1.1: %.2f%% change in latency", latencyReduction)

		if h2Improvement > 0 {
			t.Logf("✅ HTTP/2 shows %.2f%% improvement over HTTP/1.1", h2Improvement)
		} else {
			t.Logf("⚠️ HTTP/2 shows %.2f%% degradation vs HTTP/1.1 (likely due to TLS overhead in local testing)", -h2Improvement)
		}
	}

	// Generate comparison table
	t.Logf("\n=== Performance Comparison Table ===")
	t.Logf("| Protocol | Ops/sec | Throughput (MB/s) | Avg Latency | Memory (MB) | GC Count |")
	t.Logf("|----------|---------|-------------------|-------------|-------------|----------|")
	t.Logf("| HTTP/1.1 | %.2f | %.2f | %v | %.2f | %d |",
		h1Metrics.OpsPerSecond, h1Metrics.ThroughputMBps, h1Metrics.AvgLatency,
		h1Metrics.MaxMemoryMB, h1Metrics.NumGC)
	t.Logf("| HTTP/2   | %.2f | %.2f | %v | %.2f | %d |",
		h2Metrics.OpsPerSecond, h2Metrics.ThroughputMBps, h2Metrics.AvgLatency,
		h2Metrics.MaxMemoryMB, h2Metrics.NumGC)
}

// TestHTTPProtocolComparisonComprehensive runs comprehensive comparison tests
func TestHTTPProtocolComparisonComprehensive(t *testing.T) {
	// Ensure batch write is enabled
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	defer os.Unsetenv("ORCAS_BATCH_WRITE_ENABLED")

	// Test different scenarios
	scenarios := []struct {
		name        string
		dataSize    int64
		concurrency int
		operations  int
	}{
		{"Small Files (1KB)", 1024, 10, 100},
		{"Small Files (1KB) High Concurrency", 1024, 50, 500},
		{"Medium Files (64KB)", 64 * 1024, 10, 50},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Logf("\n=== Scenario: %s ===", scenario.name)
			t.Logf("Data Size: %d bytes, Concurrency: %d, Operations: %d",
				scenario.dataSize, scenario.concurrency, scenario.operations)

			// Test HTTP/1.1
			h1Metrics := testHTTP11Performance(t, scenario.dataSize, scenario.concurrency, scenario.operations)
			t.Logf("HTTP/1.1: %.2f ops/sec, %.2f MB/s", h1Metrics.OpsPerSecond, h1Metrics.ThroughputMBps)

			// Test HTTP/2
			h2Metrics := testHTTP2Performance(t, scenario.dataSize, scenario.concurrency, scenario.operations)
			t.Logf("HTTP/2: %.2f ops/sec, %.2f MB/s", h2Metrics.OpsPerSecond, h2Metrics.ThroughputMBps)

			// Calculate improvements
			if h2Metrics.OpsPerSecond > 0 && h1Metrics.OpsPerSecond > 0 {
				improvement := ((h2Metrics.OpsPerSecond - h1Metrics.OpsPerSecond) / h1Metrics.OpsPerSecond) * 100
				if improvement > 0 {
					t.Logf("HTTP/2 vs HTTP/1.1: +%.2f%%", improvement)
				} else {
					t.Logf("HTTP/2 vs HTTP/1.1: %.2f%% (degradation)", improvement)
				}
			}
		})
	}
}
