package vfs

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orcastor/orcas/core"
	b "github.com/orca-zhang/borm"
)

// MemoryStats records memory usage at different points
type MemoryStats struct {
	Timestamp         time.Time
	HeapAlloc        uint64
	HeapInUse        uint64
	HeapObjects      uint64
	RARegistrySize   int
	JournalCount     int
	TotalMemory      int64
	Description      string
}

// collectMemoryStats gathers current memory statistics
func collectMemoryStats(description string) MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return MemoryStats{
		Timestamp:    time.Now(),
		HeapAlloc:   m.HeapAlloc,
		HeapInUse:   m.HeapInuse,
		HeapObjects: m.HeapObjects,
		Description: description,
	}
}

// printMemoryStats prints memory statistics in a readable format
func printMemoryStats(stats MemoryStats) {
	fmt.Printf("[%s] %s\n", stats.Timestamp.Format("15:04:05.000"), stats.Description)
	fmt.Printf("  Heap Alloc:    %10d MB (%10d bytes)\n", stats.HeapAlloc/1024/1024, stats.HeapAlloc)
	fmt.Printf("  Heap In Use:   %10d MB (%10d bytes)\n", stats.HeapInUse/1024/1024, stats.HeapInUse)
	fmt.Printf("  Heap Objects:  %10d\n", stats.HeapObjects)
}

// formatMB converts bytes to megabytes
func formatMB(bytes uint64) string {
	return fmt.Sprintf("%.2f MB", float64(bytes)/1024/1024)
}

// TestMemoryLeak_SingleLargeFile tests memory usage when uploading a single large file
func TestMemoryLeak_SingleLargeFile(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	// Setup test environment
	h, c, bktID, cleanup := setupTestEnv(t)
	defer cleanup()

	stats := make([]MemoryStats, 0, 10)

	// Baseline memory
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	stats = append(stats, collectMemoryStats("Baseline"))
	printMemoryStats(stats[len(stats)-1])

	// Create a 100MB file
	const fileSize = 100 * 1024 * 1024 // 100MB
	chunkSize := int64(10 * 1024 * 1024) // 10MB chunks

	// Create root node
	ofs := NewOrcasFSWithConfig(h, c, bktID, &core.Config{ChunkSize: chunkSize})
	ofs.Root().fs = ofs

	// Create file node
	fileNode := &OrcasNode{
		fs:    ofs,
		objID: 0, // Will be set after creation
	}

	ctx := context.Background()

	// Create file
	fileObj, errno := fileNode.Create(ctx, "test_large_file.dat", 0o644, 0)
	if errno != 0 {
		t.Fatalf("Failed to create file: %v", errno)
	}

	// Write file in chunks
	chunk := make([]byte, 1024*1024) // 1MB chunks
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	bytesWritten := int64(0)
	for bytesWritten < fileSize {
		writeSize := int64(len(chunk))
		if bytesWritten+writeSize > fileSize {
			writeSize = fileSize - bytesWritten
		}

		n, errno := fileObj.Write(ctx, chunk[:writeSize], int64(bytesWritten))
		if errno != 0 {
			t.Fatalf("Failed to write file: %v", errno)
		}
		if int64(n) != writeSize {
			t.Fatalf("Write size mismatch: expected %d, got %d", writeSize, n)
		}

		bytesWritten += int64(n)

		// Collect stats every 10MB
		if bytesWritten%(10*1024*1024) == 0 {
			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			desc := fmt.Sprintf("After writing %d MB", bytesWritten/1024/1024)
			stats = append(stats, collectMemoryStats(desc))
			printMemoryStats(stats[len(stats)-1])
		}
	}

	// Flush and close
	fileObj.Release(ctx)

	// Final memory after close
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After file close"))
	printMemoryStats(stats[len(stats)-1])

	// Close filesystem
	ofs.Close()

	// Final cleanup
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("After FS close"))
	printMemoryStats(stats[len(stats)-1])

	// Analysis
	fmt.Println("\n=== Memory Analysis ===")
	analyzeMemoryStats(t, stats)
}

// TestMemoryLeak_ConcurrentUploads tests memory usage with concurrent file uploads
func TestMemoryLeak_ConcurrentUploads(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	h, c, bktID, cleanup := setupTestEnv(t)
	defer cleanup()

	stats := make([]MemoryStats, 0, 15)

	// Baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	stats = append(stats, collectMemoryStats("Baseline"))
	printMemoryStats(stats[len(stats)-1])

	const (
		numFiles    = 10
		fileSize    = 50 * 1024 * 1024 // 50MB per file
		chunkSize   = 10 * 1024 * 1024  // 10MB chunks
	)

	ofs := NewOrcasFSWithConfig(h, c, bktID, &core.Config{ChunkSize: chunkSize})
	defer ofs.Close()

	ctx := context.Background()

	// Concurrent uploads
	var bytesWritten atomic.Int64
	var uploadErrors atomic.Int32

	chunk := make([]byte, 1024*1024) // 1MB
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	startTime := time.Now()

	// Upload files concurrently
	for i := 0; i < numFiles; i++ {
		go func(fileNum int) {
			// Create file
			fileNode := &OrcasNode{
				fs:    ofs,
				objID: 0,
			}

			fileObj, errno := fileNode.Create(ctx, fmt.Sprintf("concurrent_file_%d.dat", fileNum), 0o644, 0)
			if errno != 0 {
				uploadErrors.Add(1)
				t.Errorf("Failed to create file %d: %v", fileNum, errno)
				return
			}
			defer fileObj.Release(ctx)

			// Write file
			written := int64(0)
			for written < fileSize {
				writeSize := int64(len(chunk))
				if written+writeSize > fileSize {
					writeSize = fileSize - written
				}

				n, errno := fileObj.Write(ctx, chunk[:writeSize], int64(written))
				if errno != 0 {
					uploadErrors.Add(1)
					t.Errorf("Failed to write file %d: %v", fileNum, errno)
					return
				}

				written += int64(n)
				bytesWritten.Add(int64(n))
			}
		}(i)

		// Stagger starts slightly
		time.Sleep(50 * time.Millisecond)
	}

	// Monitor memory during uploads
	monitorDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				runtime.GC()
				stats = append(stats, collectMemoryStats(
					fmt.Sprintf("During uploads: %d MB written", bytesWritten.Load()/1024/1024),
				))
				printMemoryStats(stats[len(stats)-1])
			case <-monitorDone:
				return
			}
		}
	}()

	// Wait for all uploads to complete
	for bytesWritten.Load() < int64(numFiles*fileSize) {
		time.Sleep(500 * time.Millisecond)
		if time.Since(startTime) > 5*time.Minute {
			t.Fatal("Timeout waiting for uploads to complete")
		}
	}
	close(monitorDone)

	// Give time for cleanup
	time.Sleep(2 * time.Second)
	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	stats = append(stats, collectMemoryStats("After all uploads complete"))
	printMemoryStats(stats[len(stats)-1])

	// Close filesystem
	ofs.Close()
	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	stats = append(stats, collectMemoryStats("After FS close"))
	printMemoryStats(stats[len(stats)-1])

	// Analysis
	fmt.Println("\n=== Concurrent Uploads Memory Analysis ===")
	analyzeMemoryStats(t, stats)

	if uploadErrors.Load() > 0 {
		t.Errorf("Upload completed with %d errors", uploadErrors.Load())
	}
}

// TestMemoryLeak_LongRunning tests memory stability over a long running session
func TestMemoryLeak_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	h, c, bktID, cleanup := setupTestEnv(t)
	defer cleanup()

	const (
		smallFileSize = 10 * 1024 * 1024 // 10MB
		numIterations  = 50               // Create/delete 50 files
		chunkSize     = 10 * 1024 * 1024
	)

	ofs := NewOrcasFSWithConfig(h, c, bktID, &core.Config{ChunkSize: chunkSize})
	defer ofs.Close()

	ctx := context.Background()
	stats := make([]MemoryStats, 0, numIterations/5+5)

	// Baseline
	runtime.GC()
	stats = append(stats, collectMemoryStats("Baseline"))
	printMemoryStats(stats[0])

	chunk := make([]byte, 1024*1024) // 1MB
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}

	// Create, write, and delete files repeatedly
	for iter := 0; iter < numIterations; iter++ {
		filename := fmt.Sprintf("temp_file_%d.dat", iter)

		// Create file
		fileNode := &OrcasNode{
			fs:    ofs,
			objID: 0,
		}

		fileObj, errno := fileNode.Create(ctx, filename, 0o644, 0)
		if errno != 0 {
			t.Fatalf("Failed to create file %d: %v", iter, errno)
		}

		// Write 10MB
		written := int64(0)
		for written < smallFileSize {
			n, errno := fileObj.Write(ctx, chunk, int64(written))
			if errno != 0 {
				t.Fatalf("Failed to write file %d: %v", iter, errno)
			}
			written += int64(n)
		}

		fileObj.Release(ctx)

		// Delete file
		fileNode.Unlink(ctx, filename)

		// Collect stats every 5 iterations
		if (iter+1)%5 == 0 {
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
			desc := fmt.Sprintf("After %d files created/deleted", iter+1)
			stats = append(stats, collectMemoryStats(desc))
			printMemoryStats(stats[len(stats)-1])
		}
	}

	// Final stats
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	stats = append(stats, collectMemoryStats("Final"))
	printMemoryStats(stats[len(stats)-1])

	// Analysis
	fmt.Println("\n=== Long Running Memory Analysis ===")
	analyzeLongRunningMemoryStats(t, stats)
}

// analyzeMemoryStats analyzes memory statistics for a single test run
func analyzeMemoryStats(t *testing.T, stats []MemoryStats) {
	if len(stats) < 2 {
		t.Fatal("Not enough stats to analyze")
	}

	baseline := stats[0]
	peakAlloc := baseline.HeapAlloc
	peakInUse := baseline.HeapInUse
	peakIdx := 0

	for i, s := range stats {
		if s.HeapAlloc > peakAlloc {
			peakAlloc = s.HeapAlloc
			peakIdx = i
		}
		if s.HeapInUse > peakInUse {
			peakInUse = s.HeapInUse
		}
	}

	final := stats[len(stats)-1]

	fmt.Printf("Baseline:     %s\n", formatMB(baseline.HeapAlloc))
	fmt.Printf("Peak Alloc:   %s (at '%s')\n", formatMB(peakAlloc), stats[peakIdx].Description)
	fmt.Printf("Peak In Use:  %s\n", formatMB(peakInUse))
	fmt.Printf("Final:        %s\n", formatMB(final.HeapAlloc))
	fmt.Printf("Growth:       %s\n", formatMB(final.HeapAlloc-baseline.HeapAlloc))

	// Check for memory leak (growth should be < 50MB for most tests)
	growth := int64(final.HeapAlloc - baseline.HeapAlloc)
	const maxAcceptableGrowth = 50 * 1024 * 1024 // 50MB

	if growth > maxAcceptableGrowth {
		t.Errorf("Possible memory leak detected: growth = %s, max acceptable = %s",
			formatMB(uint64(growth)), formatMB(maxAcceptableGrowth))
	} else {
		t.Logf("Memory growth acceptable: %s", formatMB(uint64(growth)))
	}

	// Check if final memory is significantly lower than peak
	releaseRatio := float64(final.HeapAlloc) / float64(peakAlloc)
	if releaseRatio > 0.8 {
		t.Logf("WARNING: Memory release ratio is %.2f%% (may indicate slow release)", releaseRatio*100)
	}
}

// analyzeLongRunningMemoryStats analyzes memory for long-running tests
func analyzeLongRunningMemoryStats(t *testing.T, stats []MemoryStats) {
	if len(stats) < 3 {
		t.Fatal("Not enough stats for long running analysis")
	}

	fmt.Println("Memory Trend Analysis:")

	baseline := stats[0]
	midPoint := stats[len(stats)/2]
	final := stats[len(stats)-1]

	fmt.Printf("  Baseline:    %s\n", formatMB(baseline.HeapAlloc))
	fmt.Printf("  Midpoint:    %s (%d iterations)\n", formatMB(midPoint.HeapAlloc), len(stats)/2*5)
	fmt.Printf("  Final:       %s (%d iterations)\n", formatMB(final.HeapAlloc), len(stats)*5)

	growth1 := int64(midPoint.HeapAlloc - baseline.HeapAlloc)
	growth2 := int64(final.HeapAlloc - midPoint.HeapAlloc)
	totalGrowth := int64(final.HeapAlloc - baseline.HeapAlloc)

	fmt.Printf("  Growth (first half):  %s\n", formatMB(uint64(growth1)))
	fmt.Printf("  Growth (second half): %s\n", formatMB(uint64(growth2)))
	fmt.Printf("  Total Growth:         %s\n", formatMB(uint64(totalGrowth)))

	// Check if growth is accelerating (worse than linear)
	if growth2 > growth1*2 {
		t.Error("Memory growth is accelerating - possible leak!")
	} else if totalGrowth > 100*1024*1024 {
		t.Errorf("Excessive memory growth: %s", formatMB(uint64(totalGrowth)))
	} else {
		t.Logf("Memory growth is acceptable: %s", formatMB(uint64(totalGrowth)))
	}

	// Calculate trend (simple linear regression slope)
	n := float64(len(stats))
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumX2 := 0.0

	for i, s := range stats {
		x := float64(i)
		y := float64(s.HeapAlloc)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)
	slopeMB := slope / 1024 / 1024

	fmt.Printf("  Trend slope: %.2f MB/iteration\n", slopeMB)

	if slopeMB > 1.0 {
		t.Errorf("Positive trend detected: %.2f MB/iteration", slopeMB)
	}
}

// setupTestEnv sets up the test environment
func setupTestEnv(t *testing.T) (core.Handler, core.Ctx, int64, func()) {
	// Create temp directory for test data
	tempDir, err := os.MkdirTemp("", "orcas_vfs_mem_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Initialize database
	dbPath := "file:" + tempDir + "/orcas.db?cache=shared&_journal_mode=WAL"
	db, err := b.Open(dbPath)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create handler
	h := core.NewLocalHandler(db)
	c := core.NewCtx(context.Background(), h, nil, nil)

	// Create test bucket
	bktID, err := h.CreateBkt(c, "test_bucket", "")
	if err != nil {
		db.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	return h, c, bktID, cleanup
}
