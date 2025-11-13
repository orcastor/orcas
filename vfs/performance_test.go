package vfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

// PerformanceMetrics performance metrics
type PerformanceMetrics struct {
	TestName       string
	DataSize       int64
	ChunkSize      int64
	WriteOps       int
	Concurrency    int
	Duration       time.Duration
	ThroughputMBps float64
	OpsPerSecond   float64
	MaxMemoryMB    float64
	TotalAllocMB   float64
	NumGC          uint32
	HasCompression bool
	HasEncryption  bool
}

// ensureTestUser ensures test user exists, creates it if not
func ensureTestUser(t *testing.T) {
	// Try to login first, if successful, user already exists
	handler := core.NewLocalHandler()
	ctx := context.Background()
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err == nil {
		// User already exists, return directly
		return
	}

	// User doesn't exist, need to create. Since creating user requires admin permission, we create directly via database
	// Use the same password hash as the original default user
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetDB()
	if err != nil {
		t.Logf("Warning: Failed to get DB: %v", err)
		return
	}
	defer db.Close()

	// Use INSERT OR IGNORE to avoid duplicate creation
	_, err = db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar, key) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
	if err != nil {
		t.Logf("Warning: Failed to create test user: %v", err)
	}
}

// runPerformanceTest runs performance test and returns metrics
func runPerformanceTest(t *testing.T, name string, dataSize, chunkSize int64, writeOps, concurrency int, sdkCfg *sdk.Config) PerformanceMetrics {
	// Initialize
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}
	core.InitDB("")

	// Ensure test user exists
	ensureTestUser(t)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{}
	dda.SetOptions(core.Options{Sync: true})

	lh := core.NewLocalHandler().(*core.LocalHandler)
	lh.SetAdapter(dma, dda)

	testCtx, userInfo, _, err := lh.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	bucket := &core.BucketInfo{
		ID:       testBktID,
		Name:     "perf_bucket",
		UID:      userInfo.ID,
		Type:     1,
		Quota:    10000000000, // 10GB
		Used:     0,
		RealUsed: 0,
	}
	if err := dma.PutBkt(testCtx, []*core.BucketInfo{bucket}); err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create file object
	fileID, _ := ig.New()
	fileObj := &core.ObjectInfo{
		ID:    fileID,
		PID:   core.ROOT_OID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  fmt.Sprintf("perf_%s.txt", name),
		Size:  0,
		MTime: core.Now(),
	}
	_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("PutObj failed: %v", err)
	}

	ofs := NewOrcasFS(lh, testCtx, testBktID, sdkCfg)

	// Prepare test data
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// Record start state
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// Execute write operations
	if concurrency == 1 {
		// Single thread
		ra, err := NewRandomAccessor(ofs, fileID)
		if err != nil {
			t.Fatalf("NewRandomAccessor failed: %v", err)
		}

		// Optimization: Use delayed flush mechanism, small file writes go to memory first, flush periodically or before close
		// Batch write optimization: Reduce Flush frequency, let buffer accumulate more operations
		for i := 0; i < writeOps; i++ {
			offset := int64(i) * dataSize
			err := ra.Write(offset, testData)
			if err != nil {
				t.Fatalf("Write failed: %v", err)
			}
			// For small files, use delayed flush (automatically handled by Write method)
			// Don't actively Flush, let batch write manager handle it
		}
		// Automatically flush all pending writes when closing
		err = ra.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	} else {
		// Concurrency optimization:
		// 1. Each goroutine uses independent RandomAccessor (avoid internal lock contention)
		// 2. Optimize write offset calculation to ensure non-overlapping and continuous
		// 3. Reduce Flush calls, let batch write manager handle uniformly (small file optimization)
		// 4. Use Close to automatically flush, avoid explicit Flush contention
		var wg sync.WaitGroup
		wg.Add(concurrency)

		// Calculate write range for each goroutine to ensure no overlap
		opsPerGoroutine := writeOps / concurrency
		remainingOps := writeOps % concurrency

		for g := 0; g < concurrency; g++ {
			go func(goroutineID int) {
				defer wg.Done()

				// Each goroutine creates independent RandomAccessor
				ra, err := NewRandomAccessor(ofs, fileID)
				if err != nil {
					t.Errorf("NewRandomAccessor failed in goroutine %d: %v", goroutineID, err)
					return
				}
				defer ra.Close() // Close will automatically flush, no need for explicit Flush

				// Calculate write operations for current goroutine (handle remainder allocation)
				myOps := opsPerGoroutine
				if goroutineID < remainingOps {
					myOps++
				}

				// Calculate start offset: total data written by all previous goroutines
				startOffset := int64(0)
				for i := 0; i < goroutineID; i++ {
					prevOps := opsPerGoroutine
					if i < remainingOps {
						prevOps++
					}
					startOffset += int64(prevOps) * dataSize
				}

				// Execute write operations (continuous writes, no overlap)
				for i := 0; i < myOps; i++ {
					offset := startOffset + int64(i)*dataSize
					err := ra.Write(offset, testData)
					if err != nil {
						t.Errorf("Write failed in goroutine %d, offset %d: %v", goroutineID, offset, err)
						return
					}
				}

				// For small files, use batch write manager, no need for explicit Flush
				// Close will automatically flush all pending writes
			}(g)
		}
		wg.Wait()

		// After all goroutines complete, ensure batch write manager flushes all data
		// Note: Each RandomAccessor's Close will already trigger flush, this is a safeguard
		batchMgr := ofs.getBatchWriteManager()
		if batchMgr != nil {
			batchMgr.FlushAll(testCtx)
		}
	}

	// Record end state
	endTime := time.Now()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	duration := endTime.Sub(startTime)
	totalData := float64(dataSize) * float64(writeOps)
	throughputMBps := (totalData / 1024 / 1024) / duration.Seconds()
	opsPerSecond := float64(writeOps) / duration.Seconds()

	return PerformanceMetrics{
		TestName:       name,
		DataSize:       dataSize,
		ChunkSize:      chunkSize,
		WriteOps:       writeOps,
		Concurrency:    concurrency,
		Duration:       duration,
		ThroughputMBps: throughputMBps,
		OpsPerSecond:   opsPerSecond,
		MaxMemoryMB:    float64(endMem.Alloc-startMem.Alloc) / 1024 / 1024,
		TotalAllocMB:   float64(endMem.TotalAlloc-startMem.TotalAlloc) / 1024 / 1024,
		NumGC:          endMem.NumGC - startMem.NumGC,
		HasCompression: hasCompression,
		HasEncryption:  hasEncryption,
	}
}

// TestPerformanceComprehensive comprehensive performance test
func TestPerformanceComprehensive(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comprehensive performance test in short mode")
	}

	var results []PerformanceMetrics

	// Test scenario 1: Small data blocks, single thread (optimization: use batch write, increase data volume)
	t.Run("SmallData_SingleThread", func(t *testing.T) {
		result := runPerformanceTest(t, "small_single", 4*1024, 4*1024*1024, 200, 1, nil) // Increased from 100 to 200
		results = append(results, result)
	})

	// Test scenario 2: Medium data blocks, single thread (optimization: use batch write, increase data volume)
	t.Run("MediumData_SingleThread", func(t *testing.T) {
		result := runPerformanceTest(t, "medium_single", 256*1024, 4*1024*1024, 100, 1, nil) // Increased from 50 to 100
		results = append(results, result)
	})

	// Test scenario 3: Small data blocks, concurrent (3 goroutines, optimized test)
	// Optimization: Increase write operations to better test batch write manager performance
	// Increased to 60 writes = 240KB, can better test batch packaging and flush mechanism
	t.Run("SmallData_Concurrent3", func(t *testing.T) {
		result := runPerformanceTest(t, "small_concurrent3", 4*1024, 4*1024*1024, 60, 3, nil) // Increased from 30 to 60
		results = append(results, result)
	})

	// Test scenario 4: Encryption, single thread (increase data volume)
	t.Run("Encrypted_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runPerformanceTest(t, "encrypted_single", 256*1024, 4*1024*1024, 20, 1, sdkCfg) // Increased from 5 to 20
		results = append(results, result)
	})

	// Test scenario 5: Compression, single thread (increase data volume)
	t.Run("Compressed_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
		}
		result := runPerformanceTest(t, "compressed_single", 256*1024, 4*1024*1024, 20, 1, sdkCfg) // Increased from 5 to 20
		results = append(results, result)
	})

	// Test scenario 6: Compression+Encryption, single thread (increase data volume)
	t.Run("CompressedEncrypted_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runPerformanceTest(t, "compressed_encrypted_single", 256*1024, 4*1024*1024, 20, 1, sdkCfg) // Increased from 5 to 20
		results = append(results, result)
	})

	// Test scenario 7: Large file, single thread (100MB)
	t.Run("LargeFile_SingleThread", func(t *testing.T) {
		result := runPerformanceTest(t, "large_single", 100*1024*1024, 4*1024*1024, 1, 1, nil)
		results = append(results, result)
	})

	// Test scenario 8: Large file+Compression+Encryption, single thread (100MB)
	t.Run("LargeFile_CompressedEncrypted_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runPerformanceTest(t, "large_compressed_encrypted_single", 100*1024*1024, 4*1024*1024, 1, 1, sdkCfg)
		results = append(results, result)
	})

	// Test scenario 9: Sequential write optimization (sequential write from 0, trigger sequential write optimization)
	t.Run("SequentialWrite_Optimized", func(t *testing.T) {
		result := runSequentialWriteTest(t, "sequential_write_optimized", 10*1024*1024, 4*1024*1024, nil)
		results = append(results, result)
	})

	// Test scenario 10: Sequential write+Compression+Encryption optimization
	t.Run("SequentialWrite_CompressedEncrypted", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runSequentialWriteTest(t, "sequential_write_compressed_encrypted", 10*1024*1024, 4*1024*1024, sdkCfg)
		results = append(results, result)
	})

	// Test scenario 11: Random write (different offsets, non-contiguous)
	t.Run("RandomWrite_NonSequential", func(t *testing.T) {
		result := runRandomWriteTest(t, "random_write_nonsequential", 10*1024*1024, 4*1024*1024, nil)
		results = append(results, result)
	})

	// Test scenario 12: Random write+Compression+Encryption
	t.Run("RandomWrite_CompressedEncrypted", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runRandomWriteTest(t, "random_write_compressed_encrypted", 10*1024*1024, 4*1024*1024, sdkCfg)
		results = append(results, result)
	})

	// Test scenario 13: Random write (overlapping writes)
	t.Run("RandomWrite_Overlapping", func(t *testing.T) {
		result := runRandomWriteOverlappingTest(t, "random_write_overlapping", 10*1024*1024, 4*1024*1024, nil)
		results = append(results, result)
	})

	// Test scenario 14: Random write (small data chunks, multiple writes)
	t.Run("RandomWrite_SmallChunks", func(t *testing.T) {
		result := runRandomWriteSmallChunksTest(t, "random_write_small_chunks", 10*1024*1024, 4*1024*1024, nil)
		results = append(results, result)
	})

	// Test scenario 15: Instant upload (deduplication) performance
	t.Run("InstantUpload_Performance", func(t *testing.T) {
		runInstantUploadPerformanceTest(t)
	})

	// Print performance report
	printPerformanceReport(results)

	// Print batch write optimization comparison
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("Batch Write Optimization Comparison")
	fmt.Println(strings.Repeat("=", 120))
	fmt.Printf("%-50s %12s %12s %10s %15s\n", "Test Name", "Throughput", "Ops/sec", "Memory", "Total Data")
	fmt.Println(strings.Repeat("-", 120))

	// Print all test results
	for _, r := range results {
		totalDataMB := float64(r.DataSize) * float64(r.WriteOps) / 1024 / 1024
		fmt.Printf("%-50s %10.2f MB/s %10.2f ops/s %8.2f MB %12.2f MB\n",
			r.TestName, r.ThroughputMBps, r.OpsPerSecond, r.MaxMemoryMB, totalDataMB)
	}

	fmt.Println("\n" + strings.Repeat("-", 120))
	fmt.Println("Key Benefits of Batch Write Optimization:")
	fmt.Println("  - Delayed Flush: Small files are buffered in memory and flushed periodically")
	fmt.Println("  - Batch Metadata: Multiple metadata objects are written together")
	fmt.Println("  - Batch Data Blocks: Data blocks can be grouped for efficient writes")
	fmt.Println("  - Configurable Window: Flush window time can be configured via ORCAS_WRITE_BUFFER_WINDOW_SEC")
	fmt.Println("  - Reduced I/O: Fewer database and disk operations")
	fmt.Println("  - Better Throughput: Especially for small file writes")
	fmt.Println(strings.Repeat("=", 120))
}

// runSequentialWriteTest runs sequential write performance test (test sequential write optimization)
func runSequentialWriteTest(t *testing.T, name string, totalSize, chunkSize int64, sdkCfg *sdk.Config) PerformanceMetrics {
	// Initialize
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}
	// Performance test enables batch write optimization by default
	if os.Getenv("ORCAS_BATCH_WRITE_ENABLED") == "" {
		os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	}
	core.InitDB("")

	// Ensure test user exists
	ensureTestUser(t)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{}
	dda.SetOptions(core.Options{Sync: true})

	lh := core.NewLocalHandler().(*core.LocalHandler)
	lh.SetAdapter(dma, dda)

	testCtx, userInfo, _, err := lh.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	bucket := &core.BucketInfo{
		ID:       testBktID,
		Name:     "perf_bucket",
		UID:      userInfo.ID,
		Type:     1,
		Quota:    10000000000, // 10GB
		Used:     0,
		RealUsed: 0,
	}
	if err := dma.PutBkt(testCtx, []*core.BucketInfo{bucket}); err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create file object
	fileID, _ := ig.New()
	fileObj := &core.ObjectInfo{
		ID:    fileID,
		PID:   core.ROOT_OID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  fmt.Sprintf("perf_%s.txt", name),
		Size:  0,
		MTime: core.Now(),
	}
	_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("PutObj failed: %v", err)
	}

	ofs := NewOrcasFS(lh, testCtx, testBktID, sdkCfg)

	// Prepare test data
	writeChunkSize := int64(1024 * 1024) // 1MB per write
	writeCount := int(totalSize / writeChunkSize)
	if writeCount == 0 {
		writeCount = 1
	}

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// Record start state
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// Execute sequential writes (starting from 0, continuous writes)
	ra, err := NewRandomAccessor(ofs, fileID)
	if err != nil {
		t.Fatalf("NewRandomAccessor failed: %v", err)
	}

	currentOffset := int64(0)
	for i := 0; i < writeCount; i++ {
		writeSize := writeChunkSize
		if currentOffset+writeSize > totalSize {
			writeSize = totalSize - currentOffset
		}
		if writeSize <= 0 {
			break
		}

		testData := make([]byte, writeSize)
		for j := range testData {
			testData[j] = byte((currentOffset + int64(j)) % 256)
		}

		err := ra.Write(currentOffset, testData)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		currentOffset += int64(writeSize)
	}

	// Flush
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	ra.Close()

	// Record end state
	endTime := time.Now()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	duration := endTime.Sub(startTime)
	throughputMBps := (float64(totalSize) / 1024 / 1024) / duration.Seconds()
	opsPerSecond := float64(writeCount) / duration.Seconds()

	return PerformanceMetrics{
		TestName:       name,
		DataSize:       totalSize,
		ChunkSize:      chunkSize,
		WriteOps:       writeCount,
		Concurrency:    1,
		Duration:       duration,
		ThroughputMBps: throughputMBps,
		OpsPerSecond:   opsPerSecond,
		MaxMemoryMB:    float64(endMem.Alloc-startMem.Alloc) / 1024 / 1024,
		TotalAllocMB:   float64(endMem.TotalAlloc-startMem.TotalAlloc) / 1024 / 1024,
		NumGC:          endMem.NumGC - startMem.NumGC,
		HasCompression: hasCompression,
		HasEncryption:  hasEncryption,
	}
}

// runRandomWriteTest runs random write performance test (different offsets, non-contiguous)
func runRandomWriteTest(t *testing.T, name string, totalSize, chunkSize int64, sdkCfg *sdk.Config) PerformanceMetrics {
	// Initialize (same as runSequentialWriteTest)
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}
	// Performance test enables batch write optimization by default
	if os.Getenv("ORCAS_BATCH_WRITE_ENABLED") == "" {
		os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	}
	core.InitDB("")

	// Ensure test user exists
	ensureTestUser(t)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{}
	dda.SetOptions(core.Options{Sync: true})

	lh := core.NewLocalHandler().(*core.LocalHandler)
	lh.SetAdapter(dma, dda)

	testCtx, userInfo, _, err := lh.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	bucket := &core.BucketInfo{
		ID:       testBktID,
		Name:     "perf_bucket",
		UID:      userInfo.ID,
		Type:     1,
		Quota:    10000000000, // 10GB
		Used:     0,
		RealUsed: 0,
	}
	if err := dma.PutBkt(testCtx, []*core.BucketInfo{bucket}); err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create file object
	fileID, _ := ig.New()
	fileObj := &core.ObjectInfo{
		ID:    fileID,
		PID:   core.ROOT_OID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  fmt.Sprintf("perf_%s.txt", name),
		Size:  0,
		MTime: core.Now(),
	}
	_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("PutObj failed: %v", err)
	}

	ofs := NewOrcasFS(lh, testCtx, testBktID, sdkCfg)

	// Prepare test data: random offsets, non-contiguous writes
	writeChunkSize := int64(512 * 1024) // 512KB per write
	writeCount := 20                    // Write 20 times
	writeSize := writeChunkSize

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// Record start state
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// Execute random writes (different offsets, non-contiguous)
	ra, err := NewRandomAccessor(ofs, fileID)
	if err != nil {
		t.Fatalf("NewRandomAccessor failed: %v", err)
	}

	// Generate random offset list (ensure non-contiguous)
	offsets := make([]int64, writeCount)
	for i := 0; i < writeCount; i++ {
		// Generate random offset, ensure non-contiguous
		offsets[i] = int64(i*2) * writeChunkSize // Write with intervals, skip some positions
	}

	// Shuffle order (simulate real random writes)
	for i := writeCount - 1; i > 0; i-- {
		j := i % (i + 1)
		offsets[i], offsets[j] = offsets[j], offsets[i]
	}

	totalWritten := int64(0)
	for i := 0; i < writeCount; i++ {
		offset := offsets[i]
		testData := make([]byte, writeSize)
		for j := range testData {
			testData[j] = byte((offset + int64(j)) % 256)
		}

		err := ra.Write(offset, testData)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		totalWritten += writeSize
	}

	// Flush
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	ra.Close()

	// Record end state
	endTime := time.Now()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	duration := endTime.Sub(startTime)
	throughputMBps := (float64(totalWritten) / 1024 / 1024) / duration.Seconds()
	opsPerSecond := float64(writeCount) / duration.Seconds()

	return PerformanceMetrics{
		TestName:       name,
		DataSize:       totalWritten,
		ChunkSize:      chunkSize,
		WriteOps:       writeCount,
		Concurrency:    1,
		Duration:       duration,
		ThroughputMBps: throughputMBps,
		OpsPerSecond:   opsPerSecond,
		MaxMemoryMB:    float64(endMem.Alloc-startMem.Alloc) / 1024 / 1024,
		TotalAllocMB:   float64(endMem.TotalAlloc-startMem.TotalAlloc) / 1024 / 1024,
		NumGC:          endMem.NumGC - startMem.NumGC,
		HasCompression: hasCompression,
		HasEncryption:  hasEncryption,
	}
}

// runRandomWriteOverlappingTest runs random write performance test (overlapping writes)
func runRandomWriteOverlappingTest(t *testing.T, name string, totalSize, chunkSize int64, sdkCfg *sdk.Config) PerformanceMetrics {
	// Initialize (same as runSequentialWriteTest)
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}
	core.InitDB("")

	// Ensure test user exists
	ensureTestUser(t)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{}
	dda.SetOptions(core.Options{Sync: true})

	lh := core.NewLocalHandler().(*core.LocalHandler)
	lh.SetAdapter(dma, dda)

	testCtx, userInfo, _, err := lh.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	bucket := &core.BucketInfo{
		ID:       testBktID,
		Name:     "perf_bucket",
		UID:      userInfo.ID,
		Type:     1,
		Quota:    10000000000, // 10GB
		Used:     0,
		RealUsed: 0,
	}
	if err := dma.PutBkt(testCtx, []*core.BucketInfo{bucket}); err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create file object
	fileID, _ := ig.New()
	fileObj := &core.ObjectInfo{
		ID:    fileID,
		PID:   core.ROOT_OID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  fmt.Sprintf("perf_%s.txt", name),
		Size:  0,
		MTime: core.Now(),
	}
	_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("PutObj failed: %v", err)
	}

	ofs := NewOrcasFS(lh, testCtx, testBktID, sdkCfg)

	// Prepare test data: overlapping writes
	writeChunkSize := int64(1024 * 1024) // 1MB per write
	writeCount := 15                     // Write 15 times
	overlapSize := int64(256 * 1024)     // Overlap 256KB each time

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// Record start state
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// Execute overlapping writes
	ra, err := NewRandomAccessor(ofs, fileID)
	if err != nil {
		t.Fatalf("NewRandomAccessor failed: %v", err)
	}

	totalWritten := int64(0)
	currentOffset := int64(0)
	for i := 0; i < writeCount; i++ {
		testData := make([]byte, writeChunkSize)
		for j := range testData {
			testData[j] = byte((currentOffset + int64(j)) % 256)
		}

		err := ra.Write(currentOffset, testData)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		totalWritten += writeChunkSize

		// Next write overlaps with current one
		currentOffset += writeChunkSize - overlapSize
	}

	// Flush
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	ra.Close()

	// Record end state
	endTime := time.Now()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	duration := endTime.Sub(startTime)
	throughputMBps := (float64(totalWritten) / 1024 / 1024) / duration.Seconds()
	opsPerSecond := float64(writeCount) / duration.Seconds()

	return PerformanceMetrics{
		TestName:       name,
		DataSize:       totalWritten,
		ChunkSize:      chunkSize,
		WriteOps:       writeCount,
		Concurrency:    1,
		Duration:       duration,
		ThroughputMBps: throughputMBps,
		OpsPerSecond:   opsPerSecond,
		MaxMemoryMB:    float64(endMem.Alloc-startMem.Alloc) / 1024 / 1024,
		TotalAllocMB:   float64(endMem.TotalAlloc-startMem.TotalAlloc) / 1024 / 1024,
		NumGC:          endMem.NumGC - startMem.NumGC,
		HasCompression: hasCompression,
		HasEncryption:  hasEncryption,
	}
}

// runRandomWriteSmallChunksTest runs random write performance test (small data chunks, multiple writes)
func runRandomWriteSmallChunksTest(t *testing.T, name string, totalSize, chunkSize int64, sdkCfg *sdk.Config) PerformanceMetrics {
	// Initialize (same as runSequentialWriteTest)
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}
	core.InitDB("")

	// Ensure test user exists
	ensureTestUser(t)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{}
	dda.SetOptions(core.Options{Sync: true})

	lh := core.NewLocalHandler().(*core.LocalHandler)
	lh.SetAdapter(dma, dda)

	testCtx, userInfo, _, err := lh.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	bucket := &core.BucketInfo{
		ID:       testBktID,
		Name:     "perf_bucket",
		UID:      userInfo.ID,
		Type:     1,
		Quota:    10000000000, // 10GB
		Used:     0,
		RealUsed: 0,
	}
	if err := dma.PutBkt(testCtx, []*core.BucketInfo{bucket}); err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Create file object
	fileID, _ := ig.New()
	fileObj := &core.ObjectInfo{
		ID:    fileID,
		PID:   core.ROOT_OID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  fmt.Sprintf("perf_%s.txt", name),
		Size:  0,
		MTime: core.Now(),
	}
	_, err = dma.PutObj(testCtx, testBktID, []*core.ObjectInfo{fileObj})
	if err != nil {
		t.Fatalf("PutObj failed: %v", err)
	}

	ofs := NewOrcasFS(lh, testCtx, testBktID, sdkCfg)

	// Prepare test data: small data chunks, multiple writes
	writeChunkSize := int64(64 * 1024) // 64KB per write
	writeCount := 100                  // Write 100 times

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// Record start state
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// Execute small chunk random writes
	ra, err := NewRandomAccessor(ofs, fileID)
	if err != nil {
		t.Fatalf("NewRandomAccessor failed: %v", err)
	}

	// Generate random offset list
	offsets := make([]int64, writeCount)
	for i := 0; i < writeCount; i++ {
		// Random offset, but ensure within totalSize range
		maxOffset := totalSize - writeChunkSize
		if maxOffset < 0 {
			maxOffset = 0
		}
		offsets[i] = int64(i%10) * (maxOffset / 10) // Distribute across different positions
	}

	totalWritten := int64(0)
	for i := 0; i < writeCount; i++ {
		offset := offsets[i]
		testData := make([]byte, writeChunkSize)
		for j := range testData {
			testData[j] = byte((offset + int64(j)) % 256)
		}

		err := ra.Write(offset, testData)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		totalWritten += writeChunkSize
	}

	// Flush
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	ra.Close()

	// Record end state
	endTime := time.Now()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	duration := endTime.Sub(startTime)
	throughputMBps := (float64(totalWritten) / 1024 / 1024) / duration.Seconds()
	opsPerSecond := float64(writeCount) / duration.Seconds()

	return PerformanceMetrics{
		TestName:       name,
		DataSize:       totalWritten,
		ChunkSize:      chunkSize,
		WriteOps:       writeCount,
		Concurrency:    1,
		Duration:       duration,
		ThroughputMBps: throughputMBps,
		OpsPerSecond:   opsPerSecond,
		MaxMemoryMB:    float64(endMem.Alloc-startMem.Alloc) / 1024 / 1024,
		TotalAllocMB:   float64(endMem.TotalAlloc-startMem.TotalAlloc) / 1024 / 1024,
		NumGC:          endMem.NumGC - startMem.NumGC,
		HasCompression: hasCompression,
		HasEncryption:  hasEncryption,
	}
}

// printPerformanceReport prints performance report
func printPerformanceReport(results []PerformanceMetrics) {
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("Comprehensive Performance Test Report")
	fmt.Println(strings.Repeat("=", 120))
	fmt.Printf("%-30s %10s %8s %8s %12s %12s %10s %10s %8s %8s\n",
		"Test Name", "Data Size", "Ops", "Conc", "Duration", "Throughput", "Ops/sec", "Max Mem", "Total", "GC")
	fmt.Println(strings.Repeat("-", 120))

	for _, r := range results {
		compressFlag := ""
		if r.HasCompression {
			compressFlag += "C"
		}
		if r.HasEncryption {
			compressFlag += "E"
		}
		if compressFlag == "" {
			compressFlag = "-"
		}

		fmt.Printf("%-30s %10s %8d %8d %12s %12.2f %10.2f %10.2f %10.2f %8d\n",
			r.TestName,
			formatBytes(r.DataSize),
			r.WriteOps,
			r.Concurrency,
			r.Duration.Round(time.Millisecond).String(),
			r.ThroughputMBps,
			r.OpsPerSecond,
			r.MaxMemoryMB,
			r.TotalAllocMB,
			r.NumGC,
		)
	}

	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("Performance Analysis:")
	fmt.Println(strings.Repeat("-", 120))

	// Analyze single thread performance
	fmt.Println("\nSingle Thread Performance:")
	singleThread := filterResults(results, func(r PerformanceMetrics) bool {
		return r.Concurrency == 1
	})
	printAnalysis(singleThread)

	// Analyze concurrent performance
	fmt.Println("\nConcurrent Performance:")
	concurrent := filterResults(results, func(r PerformanceMetrics) bool {
		return r.Concurrency > 1
	})
	printAnalysis(concurrent)

	// Analyze encryption/compression performance
	fmt.Println("\nEncryption/Compression Performance:")
	encrypted := filterResults(results, func(r PerformanceMetrics) bool {
		return r.HasEncryption || r.HasCompression
	})
	printAnalysis(encrypted)

	fmt.Println("\n" + strings.Repeat("=", 120))
}

// formatBytes formats byte count
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func filterResults(results []PerformanceMetrics, filter func(PerformanceMetrics) bool) []PerformanceMetrics {
	var filtered []PerformanceMetrics
	for _, r := range results {
		if filter(r) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func printAnalysis(results []PerformanceMetrics) {
	if len(results) == 0 {
		fmt.Println("  No data")
		return
	}

	var totalThroughput, totalOps, totalMemory, totalGC float64
	for _, r := range results {
		totalThroughput += r.ThroughputMBps
		totalOps += r.OpsPerSecond
		totalMemory += r.MaxMemoryMB
		totalGC += float64(r.NumGC)
	}

	count := float64(len(results))
	fmt.Printf("  Average Throughput: %.2f MB/s\n", totalThroughput/count)
	fmt.Printf("  Average Ops/sec: %.2f\n", totalOps/count)
	fmt.Printf("  Average Memory: %.2f MB\n", totalMemory/count)
	fmt.Printf("  Average GC: %.1f\n", totalGC/count)
}

// runInstantUploadPerformanceTest runs instant upload performance test
func runInstantUploadPerformanceTest(t *testing.T) {
	// Initialize
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_instant_upload_perf_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_instant_upload_perf_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}
	core.InitDB("")

	// Ensure test user exists
	ensureTestUser(t)

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err := core.InitBucketDB(context.Background(), testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	dma := &core.DefaultMetadataAdapter{}
	dda := &core.DefaultDataAdapter{}
	dda.SetOptions(core.Options{Sync: true})

	lh := core.NewLocalHandler().(*core.LocalHandler)
	lh.SetAdapter(dma, dda)

	testCtx, userInfo, _, err := lh.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	// Create bucket
	bucket := &core.BucketInfo{
		ID:        testBktID,
		Name:      "test-bucket",
		UID:       userInfo.ID,
		Type:      1,
		Quota:     10 << 30, // 10GB
		ChunkSize: 4 * 1024 * 1024,
	}
	dma.PutBkt(testCtx, []*core.BucketInfo{bucket})

	// Test different file sizes
	testSizes := []struct {
		size     int64
		numTests int
		name     string
	}{
		{1 * 1024, 50, "1KB"},
		{10 * 1024, 50, "10KB"},
		{100 * 1024, 30, "100KB"},
		{1024 * 1024, 20, "1MB"},
		{10 * 1024 * 1024, 10, "10MB"},
	}

	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("Instant Upload (Deduplication) Performance Test")
	fmt.Println(strings.Repeat("=", 120))

	for _, testCase := range testSizes {
		t.Run(testCase.name, func(t *testing.T) {
			testInstantUploadForSize(t, lh, testCtx, testBktID, testCase.size, testCase.numTests, testCase.name)
		})
	}
}

// testInstantUploadForSize tests instant upload performance for a specific file size
func testInstantUploadForSize(t *testing.T, lh *core.LocalHandler, ctx context.Context, bktID int64, dataSize int64, numTests int, sizeName string) {
	// Prepare test data (same data for instant upload, different data for normal upload)
	duplicateData := make([]byte, dataSize)
	for i := range duplicateData {
		duplicateData[i] = byte(i % 256)
	}

	uniqueData := make([]byte, dataSize)
	for i := range uniqueData {
		uniqueData[i] = byte((i + 10000) % 256)
	}

	// Create file system with instant upload enabled
	fs := &OrcasFS{
		h:         lh,
		bktID:     bktID,
		c:         ctx,
		chunkSize: 4 * 1024 * 1024,
	}

	// Upload baseline file for instant upload
	ig := idgen.NewIDGen(nil, 0)
	baselineFileID, _ := ig.New()
	baselineObj := &core.ObjectInfo{
		ID:     baselineFileID,
		PID:    0,
		Type:   core.OBJ_TYPE_FILE,
		Name:   fmt.Sprintf("baseline_%s.bin", sizeName),
		DataID: core.EmptyDataID,
		Size:   0,
		MTime:  core.Now(),
	}
	_, err := lh.Put(ctx, bktID, []*core.ObjectInfo{baselineObj})
	if err != nil {
		t.Fatalf("Failed to create baseline file: %v", err)
	}

	baselineRA, err := NewRandomAccessor(fs, baselineFileID)
	if err != nil {
		t.Fatalf("Failed to create baseline RandomAccessor: %v", err)
	}
	err = baselineRA.Write(0, duplicateData)
	if err != nil {
		t.Fatalf("Failed to write baseline data: %v", err)
	}
	_, err = baselineRA.Flush()
	if err != nil {
		t.Fatalf("Failed to flush baseline data: %v", err)
	}
	baselineRA.Close()

	// Wait a bit for data to be written
	time.Sleep(200 * time.Millisecond)

	// Test instant upload performance (upload same data multiple times)
	instantUploadTimes := make([]time.Duration, numTests)
	var instantUploadDataIDs []int64
	for i := 0; i < numTests; i++ {
		fileID, _ := ig.New()
		obj := &core.ObjectInfo{
			ID:     fileID,
			PID:    0,
			Type:   core.OBJ_TYPE_FILE,
			Name:   fmt.Sprintf("instant_%s_%d.bin", sizeName, i),
			DataID: core.EmptyDataID,
			Size:   0,
			MTime:  core.Now(),
		}
		_, err := lh.Put(ctx, bktID, []*core.ObjectInfo{obj})
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		ra, err := NewRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		startTime := time.Now()
		err = ra.Write(0, duplicateData)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
		_, err = ra.Flush()
		if err != nil {
			t.Fatalf("Failed to flush data: %v", err)
		}
		instantUploadTimes[i] = time.Since(startTime)

		// Get file object to check DataID
		objs, err := lh.Get(ctx, bktID, []int64{fileID})
		if err == nil && len(objs) > 0 {
			instantUploadDataIDs = append(instantUploadDataIDs, objs[0].DataID)
		}

		ra.Close()
	}

	// Test normal upload performance (upload different data)
	normalUploadTimes := make([]time.Duration, numTests)
	for i := 0; i < numTests; i++ {
		// Create unique data for each upload
		uniqueDataCopy := make([]byte, len(uniqueData))
		copy(uniqueDataCopy, uniqueData)
		uniqueDataCopy[0] = byte(i) // Make it unique

		fileID, _ := ig.New()
		obj := &core.ObjectInfo{
			ID:     fileID,
			PID:    0,
			Type:   core.OBJ_TYPE_FILE,
			Name:   fmt.Sprintf("normal_%s_%d.bin", sizeName, i),
			DataID: core.EmptyDataID,
			Size:   0,
			MTime:  core.Now(),
		}
		_, err := lh.Put(ctx, bktID, []*core.ObjectInfo{obj})
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		ra, err := NewRandomAccessor(fs, fileID)
		if err != nil {
			t.Fatalf("Failed to create RandomAccessor: %v", err)
		}

		startTime := time.Now()
		err = ra.Write(0, uniqueDataCopy)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
		_, err = ra.Flush()
		if err != nil {
			t.Fatalf("Failed to flush data: %v", err)
		}
		normalUploadTimes[i] = time.Since(startTime)

		ra.Close()
	}

	// Calculate statistics
	var instantTotal, normalTotal time.Duration
	for i := 0; i < numTests; i++ {
		instantTotal += instantUploadTimes[i]
		normalTotal += normalUploadTimes[i]
	}
	instantAvg := instantTotal / time.Duration(numTests)
	normalAvg := normalTotal / time.Duration(numTests)

	// Calculate improvement
	improvement := float64(normalAvg) / float64(instantAvg)
	timeSaved := normalAvg - instantAvg

	// Check how many files actually used instant upload (same DataID as baseline)
	baselineObjs, _ := lh.Get(ctx, bktID, []int64{baselineFileID})
	baselineDataID := int64(0)
	if len(baselineObjs) > 0 {
		baselineDataID = baselineObjs[0].DataID
	}
	instantUploadCount := 0
	for _, dataID := range instantUploadDataIDs {
		if dataID == baselineDataID && dataID > 0 {
			instantUploadCount++
		}
	}

	// Print results
	fmt.Printf("\n=== Size: %s (%d tests) ===\n", sizeName, numTests)
	fmt.Printf("Instant Upload (duplicate data):\n")
	fmt.Printf("  Average time: %v\n", instantAvg)
	fmt.Printf("  Total time: %v\n", instantTotal)
	fmt.Printf("  Files using instant upload: %d/%d\n", instantUploadCount, numTests)
	fmt.Printf("\nNormal Upload (unique data):\n")
	fmt.Printf("  Average time: %v\n", normalAvg)
	fmt.Printf("  Total time: %v\n", normalTotal)
	fmt.Printf("\nPerformance Improvement:\n")
	fmt.Printf("  Speedup: %.2fx faster\n", improvement)
	fmt.Printf("  Time saved per upload: %v\n", timeSaved)
	fmt.Printf("  Total time saved: %v\n", normalTotal-instantTotal)
}
