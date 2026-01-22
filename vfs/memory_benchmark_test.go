package vfs

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
)

// TestMemoryBenchmark_ChunkedFileWriter benchmarks memory usage of ChunkedFileWriter
func TestMemoryBenchmark_ChunkedFileWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory benchmark in short mode")
	}

	// Setup
	h, c, bktID, cleanup := setupBenchmarkEnv(t)
	defer cleanup()

	// Test configurations
	testCases := []struct {
		name      string
		fileSize  int64
		chunkSize int64
	}{
		{"10MB file (10MB chunks)", 10 * 1024 * 1024, 10 * 1024 * 1024},
		{"50MB file (10MB chunks)", 50 * 1024 * 1024, 10 * 1024 * 1024},
		{"100MB file (10MB chunks)", 100 * 1024 * 1024, 10 * 1024 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runChunkedFileWriterBenchmark(t, h, c, bktID, tc.fileSize, tc.chunkSize)
		})
	}
}

// TestMemoryLeak_RandomAccessorRegistry tests RandomAccessor cleanup
func TestMemoryLeak_RandomAccessorRegistry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	h, c, bktID, cleanup := setupBenchmarkEnv(t)
	defer cleanup()

	ofs := NewOrcasFSWithConfig(h, c, bktID, &core.Config{})
	defer ofs.Close()

	stats := make([]MemoryStats, 0, 20)

	// Baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	stats = append(stats, collectMemoryStats("Baseline"))
	printMemoryStats(stats[0])
	chunk := make([]byte, 1024*1024) // 1MB
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	// Create and write to 20 files
	const numFiles = 20
	const fileSize = 10 * 1024 * 1024 // 10MB each

	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("test_file_%d.dat", i)

		// Create RandomAccessor
		ra, err := NewRandomAccessor(ofs, int64(i+1000))
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}
		ra.fileObj.Store(&core.ObjectInfo{
			ID:   int64(i + 1000),
			Name: filename,
			Size: 0,
		})
		ofs.registerRandomAccessor(int64(i+1000), ra)

		// Write 10MB
		for offset := int64(0); offset < fileSize; offset += 1024 * 1024 {
			err := ra.Write(offset, chunk)
			if err != nil {
				t.Fatalf("Failed to write: %v", err)
			}
		}

		// Don't close yet - accumulate in registry
		if (i+1)%5 == 0 {
			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			stats = append(stats, collectMemoryStats(fmt.Sprintf("After %d files (not closed)", i+1)))
			printMemoryStats(stats[len(stats)-1])
		}
	}

	// Now close all RandomAccessors
	for i := 0; i < numFiles; i++ {
		ra := ofs.getRandomAccessorByFileID(int64(i + 1000))
		if ra != nil {
			ra.Close()
			ofs.unregisterRandomAccessor(int64(i+1000), ra)
		}
	}

	// Force cleanup of inactive RAs
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After closing all files"))
	printMemoryStats(stats[len(stats)-1])

	// Trigger cleanup worker manually
	ofs.cleanupInactiveRandomAccessors()

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After cleanup worker"))
	printMemoryStats(stats[len(stats)-1])

	// Analysis
	analyzeMemoryStats(t, stats)
}

// TestMemoryLeak_JournalAccumulation tests journal memory management
func TestMemoryLeak_JournalAccumulation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	h, c, bktID, cleanup := setupBenchmarkEnv(t)
	defer cleanup()

	ofs := NewOrcasFSWithConfig(h, c, bktID, &core.Config{})
	defer ofs.Close()

	stats := make([]MemoryStats, 0, 15)

	// Baseline
	runtime.GC()
	stats = append(stats, collectMemoryStats("Baseline"))
	printMemoryStats(stats[0])
	chunk := make([]byte, 1024*1024) // 1MB
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	const numFiles = 15
	const fileSize = 5 * 1024 * 1024 // 5MB each

	// Create files with journals
	for i := 0; i < numFiles; i++ {
		ra, err := NewRandomAccessor(ofs, int64(i+2000))
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}
		ra.fileObj.Store(&core.ObjectInfo{
			ID:     int64(i + 2000),
			Name:   fmt.Sprintf("journal_test_%d.dat", i),
			Size:   0,
			DataID: 0,
		})

		// Write with random pattern to trigger journal
		for offset := int64(0); offset < fileSize; offset += 1024 * 1024 {
			ra.Write(offset, chunk)
		}

		ofs.registerRandomAccessor(int64(i+2000), ra)

		if (i+1)%5 == 0 {
			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			stats = append(stats, collectMemoryStats(fmt.Sprintf("After %d files with journals", i+1)))
			printMemoryStats(stats[len(stats)-1])
		}
	}

	// Flush all journals
	for i := 0; i < numFiles; i++ {
		ra := ofs.getRandomAccessorByFileID(int64(i + 2000))
		if ra != nil {
			ra.Flush()
		}
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After flushing all journals"))
	printMemoryStats(stats[len(stats)-1])

	// Trigger journal cleanup
	if ofs.journalMgr != nil {
		ofs.journalMgr.cleanupInactiveJournals()
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After journal cleanup"))
	printMemoryStats(stats[len(stats)-1])

	analyzeMemoryStats(t, stats)
}

// TestMemoryStress_ConcurrentLargeUploads simulates heavy concurrent upload load
func TestMemoryStress_ConcurrentLargeUploads(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	h, c, bktID, cleanup := setupBenchmarkEnv(t)
	defer cleanup()

	ofs := NewOrcasFSWithConfig(h, c, bktID, &core.Config{})
	defer ofs.Close()

	stats := make([]MemoryStats, 0, 10)
	var totalBytesWritten atomic.Int64

	// Baseline
	runtime.GC()
	stats = append(stats, collectMemoryStats("Baseline"))
	printMemoryStats(stats[0])

	// Configuration
	const (
		numConcurrent = 20
		fileSize      = 50 * 1024 * 1024 // 50MB per file
	)
	chunk := make([]byte, 1024*1024) // 1MB chunks
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	// Start monitoring
	stopMonitor := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				runtime.GC()
				writtenMB := totalBytesWritten.Load() / 1024 / 1024
				stats = append(stats, collectMemoryStats(
					fmt.Sprintf("Monitoring: %d MB written", writtenMB),
				))
				printMemoryStats(stats[len(stats)-1])
			case <-stopMonitor:
				return
			}
		}
	}()

	// Launch concurrent uploads
	startTime := time.Now()
	var errors atomic.Int32

	for i := 0; i < numConcurrent; i++ {
		go func(fileNum int) {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Goroutine %d panicked: %v", fileNum, r)
					errors.Add(1)
				}
			}()

			ra, err := NewRandomAccessor(ofs, int64(fileNum+3000))
			if err != nil {
				t.Errorf("Failed to create RandomAccessor: %v", err)
				errors.Add(1)
				return
			}
			ra.fileObj.Store(&core.ObjectInfo{
				ID:     int64(fileNum + 3000),
				Name:   fmt.Sprintf("stress_file_%d.dat", fileNum),
				Size:   0,
				DataID: 0,
			})

			// Write in 1MB chunks
			for offset := int64(0); offset < fileSize; offset += 1024 * 1024 {
				err := ra.Write(offset, chunk)
				if err != nil {
					t.Errorf("File %d write failed at offset %d: %v", fileNum, offset, err)
					errors.Add(1)
					return
				}
				totalBytesWritten.Add(int64(len(chunk)))
			}

			// Flush and close
			ra.Flush()
			ra.Close()
		}(i)

		// Stagger starts
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for completion
	expectedBytes := int64(numConcurrent * fileSize)
	for totalBytesWritten.Load() < expectedBytes {
		time.Sleep(500 * time.Millisecond)
		if time.Since(startTime) > 10*time.Minute {
			t.Fatal("Timeout waiting for uploads")
		}
	}

	close(stopMonitor)

	// Final measurement
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After all uploads complete"))
	printMemoryStats(stats[len(stats)-1])

	// Trigger cleanup workers
	ofs.cleanupInactiveRandomAccessors()
	if ofs.journalMgr != nil {
		ofs.journalMgr.cleanupInactiveJournals()
	}

	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After cleanup"))
	printMemoryStats(stats[len(stats)-1])

	// Analysis
	fmt.Println("\n=== Stress Test Results ===")
	fmt.Printf("Total written: %d MB\n", totalBytesWritten.Load()/1024/1024)
	fmt.Printf("Duration: %.2f seconds\n", time.Since(startTime).Seconds())
	fmt.Printf("Errors: %d\n", errors.Load())
	fmt.Println()

	analyzeMemoryStats(t, stats)

	if errors.Load() > 0 {
		t.Errorf("Test completed with %d errors", errors.Load())
	}
}

// runChunkedFileWriterBenchmark runs a single benchmark
func runChunkedFileWriterBenchmark(t *testing.T, h core.Handler, c core.Ctx, bktID int64, fileSize, chunkSize int64) {
	fmt.Printf("\n=== Benchmark: %d MB file, %d MB chunks ===\n", fileSize/1024/1024, chunkSize/1024/1024)

	ofs := NewOrcasFSWithConfig(h, c, bktID, &core.Config{})
	defer ofs.Close()

	stats := make([]MemoryStats, 0, 20)

	// Baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	stats = append(stats, collectMemoryStats("Baseline"))
	printMemoryStats(stats[0])

	// Create RandomAccessor
	ra, err := NewRandomAccessor(ofs, 1)
	if err != nil {
		t.Fatalf("Failed to create RandomAccessor: %v", err)
	}
	ra.fileObj.Store(&core.ObjectInfo{
		ID:     1,
		Name:   "benchmark_file.dat",
		Size:   0,
		DataID: 0,
	})

	// Write data in chunks
	chunk := make([]byte, 1024*1024) // 1MB
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	bytesWritten := int64(0)
	mbWritten := int64(0)

	for bytesWritten < fileSize {
		writeSize := int64(len(chunk))
		if bytesWritten+writeSize > fileSize {
			writeSize = fileSize - bytesWritten
		}

		err := ra.Write(bytesWritten, chunk[:writeSize])
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		bytesWritten += writeSize
		newMBWritten := bytesWritten / 1024 / 1024

		// Collect stats every 10MB
		if newMBWritten > mbWritten && newMBWritten%10 == 0 {
			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			stats = append(stats, collectMemoryStats(fmt.Sprintf("After %d MB written", newMBWritten)))
			printMemoryStats(stats[len(stats)-1])
			mbWritten = newMBWritten
		}
	}

	// Flush
	ra.Flush()

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After flush"))
	printMemoryStats(stats[len(stats)-1])

	// Close
	ra.Close()

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After close"))
	printMemoryStats(stats[len(stats)-1])

	// Analysis
	analyzeMemoryStats(t, stats)
}

// setupBenchmarkEnv sets up benchmark environment
func setupBenchmarkEnv(t *testing.T) (core.Handler, core.Ctx, int64, func()) {
	tempDir, err := os.MkdirTemp("", "orcas_bench_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	h := core.NewLocalHandler(tempDir, tempDir)
	ctx := context.Background()
	ctx, _, _, err = h.Login(ctx, "orcas", "orcas")
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to login: %v", err)
	}

	ig := idgen.NewIDGen(nil, 0)
	bktID, _ := ig.New()
	err = core.InitBucketDB(tempDir, bktID)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to init bucket DB: %v", err)
	}

	// Create bucket using admin
	admin := core.NewLocalAdmin(tempDir, tempDir)
	bkt := &core.BucketInfo{
		ID:        bktID,
		Name:      "benchmark_bucket",
		Type:      1,
		Quota:     -1,
		ChunkSize: 4 * 1024 * 1024, // 4MB chunk size
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create bucket: %v", err)
	}

	c := ctx

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return h, c, bktID, cleanup
}
