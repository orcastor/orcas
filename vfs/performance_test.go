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

// PerformanceMetrics 性能指标
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

// ensureTestUser 确保测试用户存在，如果不存在则创建
func ensureTestUser(t *testing.T) {
	// 先尝试登录，如果成功说明用户已存在
	handler := core.NewLocalHandler()
	ctx := context.Background()
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err == nil {
		// 用户已存在，直接返回
		return
	}

	// 用户不存在，需要创建。由于创建用户需要管理员权限，我们直接通过数据库创建
	// 使用与原来默认用户相同的密码哈希
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetDB()
	if err != nil {
		t.Logf("Warning: Failed to get DB: %v", err)
		return
	}
	defer db.Close()

	// 使用 INSERT OR IGNORE 避免重复创建
	_, err = db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar, key) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
	if err != nil {
		t.Logf("Warning: Failed to create test user: %v", err)
	}
}

// runPerformanceTest 运行性能测试并返回指标
func runPerformanceTest(t *testing.T, name string, dataSize, chunkSize int64, writeOps, concurrency int, sdkCfg *sdk.Config) PerformanceMetrics {
	// 初始化
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
	core.InitDB()

	// 确保测试用户存在
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

	// 创建文件对象
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

	// 准备测试数据
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// 记录开始状态
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// 执行写入操作
	if concurrency == 1 {
		// 单线程
		ra, err := NewRandomAccessor(ofs, fileID)
		if err != nil {
			t.Fatalf("NewRandomAccessor failed: %v", err)
		}

		// 优化：使用延迟刷新机制，小文件写入先写到内存，定期或关闭前刷新
		// 批量写入优化：减少Flush频率，让缓冲区积累更多操作
		for i := 0; i < writeOps; i++ {
			offset := int64(i) * dataSize
			err := ra.Write(offset, testData)
			if err != nil {
				t.Fatalf("Write failed: %v", err)
			}
			// 对于小文件，使用延迟刷新（由Write方法自动处理）
			// 不主动Flush，让批量写入管理器处理
		}
		// 关闭时自动刷新所有待写入数据
		err = ra.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	} else {
		// 并发优化：
		// 1. 每个goroutine使用独立的RandomAccessor（避免内部锁竞争）
		// 2. 优化写入偏移量计算，确保不重叠且连续
		// 3. 减少Flush调用，让批量写入管理器统一处理（小文件优化）
		// 4. 使用Close自动刷新，避免显式Flush竞争
		var wg sync.WaitGroup
		wg.Add(concurrency)

		// 计算每个goroutine的写入范围，确保不重叠
		opsPerGoroutine := writeOps / concurrency
		remainingOps := writeOps % concurrency

		for g := 0; g < concurrency; g++ {
			go func(goroutineID int) {
				defer wg.Done()

				// 每个goroutine创建独立的RandomAccessor
				ra, err := NewRandomAccessor(ofs, fileID)
				if err != nil {
					t.Errorf("NewRandomAccessor failed in goroutine %d: %v", goroutineID, err)
					return
				}
				defer ra.Close() // Close会自动刷新，不需要显式Flush

				// 计算当前goroutine的写入操作数（处理余数分配）
				myOps := opsPerGoroutine
				if goroutineID < remainingOps {
					myOps++
				}

				// 计算起始偏移量：前面所有goroutine写入的总数据量
				startOffset := int64(0)
				for i := 0; i < goroutineID; i++ {
					prevOps := opsPerGoroutine
					if i < remainingOps {
						prevOps++
					}
					startOffset += int64(prevOps) * dataSize
				}

				// 执行写入操作（连续写入，不重叠）
				for i := 0; i < myOps; i++ {
					offset := startOffset + int64(i)*dataSize
					err := ra.Write(offset, testData)
					if err != nil {
						t.Errorf("Write failed in goroutine %d, offset %d: %v", goroutineID, offset, err)
						return
					}
				}

				// 对于小文件，使用批量写入管理器，不需要显式Flush
				// Close时会自动刷新所有待写入数据
			}(g)
		}
		wg.Wait()

		// 所有goroutine完成后，确保批量写入管理器刷新所有数据
		// 注意：每个RandomAccessor的Close已经会触发刷新，这里作为保险
		batchMgr := ofs.getBatchWriteManager()
		if batchMgr != nil {
			batchMgr.flushAll()
		}
	}

	// 记录结束状态
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

// TestPerformanceComprehensive 综合性能测试
func TestPerformanceComprehensive(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comprehensive performance test in short mode")
	}

	var results []PerformanceMetrics

	// 测试场景1: 小数据块，单线程（优化：使用批量写入，增加数据量）
	t.Run("SmallData_SingleThread", func(t *testing.T) {
		result := runPerformanceTest(t, "small_single", 4*1024, 4*1024*1024, 200, 1, nil) // 从100增加到200
		results = append(results, result)
	})

	// 测试场景2: 中等数据块，单线程（优化：使用批量写入，增加数据量）
	t.Run("MediumData_SingleThread", func(t *testing.T) {
		result := runPerformanceTest(t, "medium_single", 256*1024, 4*1024*1024, 100, 1, nil) // 从50增加到100
		results = append(results, result)
	})

	// 测试场景3: 小数据块，并发（3个goroutine，优化测试）
	// 优化：增加写入操作数，更好地测试批量写入管理器的性能
	// 增加到60次写入 = 240KB，可以更好地测试批量打包和刷新机制
	t.Run("SmallData_Concurrent3", func(t *testing.T) {
		result := runPerformanceTest(t, "small_concurrent3", 4*1024, 4*1024*1024, 60, 3, nil) // 从30增加到60
		results = append(results, result)
	})

	// 测试场景4: 加密，单线程（增加数据量）
	t.Run("Encrypted_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runPerformanceTest(t, "encrypted_single", 256*1024, 4*1024*1024, 20, 1, sdkCfg) // 从5增加到20
		results = append(results, result)
	})

	// 测试场景5: 压缩，单线程（增加数据量）
	t.Run("Compressed_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
		}
		result := runPerformanceTest(t, "compressed_single", 256*1024, 4*1024*1024, 20, 1, sdkCfg) // 从5增加到20
		results = append(results, result)
	})

	// 测试场景6: 压缩+加密，单线程（增加数据量）
	t.Run("CompressedEncrypted_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runPerformanceTest(t, "compressed_encrypted_single", 256*1024, 4*1024*1024, 20, 1, sdkCfg) // 从5增加到20
		results = append(results, result)
	})

	// 测试场景7: 大文件，单线程（100MB）
	t.Run("LargeFile_SingleThread", func(t *testing.T) {
		result := runPerformanceTest(t, "large_single", 100*1024*1024, 4*1024*1024, 1, 1, nil)
		results = append(results, result)
	})

	// 测试场景8: 大文件+压缩+加密，单线程（100MB）
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

	// 测试场景9: 顺序写优化（从0开始顺序写，触发顺序写优化）
	t.Run("SequentialWrite_Optimized", func(t *testing.T) {
		result := runSequentialWriteTest(t, "sequential_write_optimized", 10*1024*1024, 4*1024*1024, nil)
		results = append(results, result)
	})

	// 测试场景10: 顺序写+压缩+加密优化
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

	// 测试场景11: 随机写（不同offset，不连续）
	t.Run("RandomWrite_NonSequential", func(t *testing.T) {
		result := runRandomWriteTest(t, "random_write_nonsequential", 10*1024*1024, 4*1024*1024, nil)
		results = append(results, result)
	})

	// 测试场景12: 随机写+压缩+加密
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

	// 测试场景13: 随机写（重叠写入）
	t.Run("RandomWrite_Overlapping", func(t *testing.T) {
		result := runRandomWriteOverlappingTest(t, "random_write_overlapping", 10*1024*1024, 4*1024*1024, nil)
		results = append(results, result)
	})

	// 测试场景14: 随机写（小数据块，多次写入）
	t.Run("RandomWrite_SmallChunks", func(t *testing.T) {
		result := runRandomWriteSmallChunksTest(t, "random_write_small_chunks", 10*1024*1024, 4*1024*1024, nil)
		results = append(results, result)
	})

	// 打印性能报告
	printPerformanceReport(results)

	// 打印批量写入优化对比
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("Batch Write Optimization Comparison")
	fmt.Println(strings.Repeat("=", 120))
	fmt.Printf("%-50s %12s %12s %10s %15s\n", "Test Name", "Throughput", "Ops/sec", "Memory", "Total Data")
	fmt.Println(strings.Repeat("-", 120))

	// 打印所有测试结果
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

// runSequentialWriteTest 运行顺序写性能测试（测试顺序写优化）
func runSequentialWriteTest(t *testing.T, name string, totalSize, chunkSize int64, sdkCfg *sdk.Config) PerformanceMetrics {
	// 初始化
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
	// 性能测试默认开启批量写入优化
	if os.Getenv("ORCAS_BATCH_WRITE_ENABLED") == "" {
		os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	}
	core.InitDB()

	// 确保测试用户存在
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

	// 创建文件对象
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

	// 准备测试数据
	writeChunkSize := int64(1024 * 1024) // 1MB per write
	writeCount := int(totalSize / writeChunkSize)
	if writeCount == 0 {
		writeCount = 1
	}

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// 记录开始状态
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// 执行顺序写入（从0开始，连续写入）
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

	// 记录结束状态
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

// runRandomWriteTest 运行随机写性能测试（不同offset，不连续）
func runRandomWriteTest(t *testing.T, name string, totalSize, chunkSize int64, sdkCfg *sdk.Config) PerformanceMetrics {
	// 初始化（与runSequentialWriteTest相同）
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
	// 性能测试默认开启批量写入优化
	if os.Getenv("ORCAS_BATCH_WRITE_ENABLED") == "" {
		os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	}
	core.InitDB()

	// 确保测试用户存在
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

	// 创建文件对象
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

	// 准备测试数据：随机offset，不连续写入
	writeChunkSize := int64(512 * 1024) // 512KB per write
	writeCount := 20                    // 写入20次
	writeSize := writeChunkSize

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// 记录开始状态
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// 执行随机写入（不同offset，不连续）
	ra, err := NewRandomAccessor(ofs, fileID)
	if err != nil {
		t.Fatalf("NewRandomAccessor failed: %v", err)
	}

	// 生成随机offset列表（确保不连续）
	offsets := make([]int64, writeCount)
	for i := 0; i < writeCount; i++ {
		// 生成随机offset，确保不连续
		offsets[i] = int64(i*2) * writeChunkSize // 间隔写入，跳过一些位置
	}

	// 打乱顺序（模拟真实随机写）
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

	// 记录结束状态
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

// runRandomWriteOverlappingTest 运行随机写性能测试（重叠写入）
func runRandomWriteOverlappingTest(t *testing.T, name string, totalSize, chunkSize int64, sdkCfg *sdk.Config) PerformanceMetrics {
	// 初始化（与runSequentialWriteTest相同）
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
	core.InitDB()

	// 确保测试用户存在
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

	// 创建文件对象
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

	// 准备测试数据：重叠写入
	writeChunkSize := int64(1024 * 1024) // 1MB per write
	writeCount := 15                     // 写入15次
	overlapSize := int64(256 * 1024)     // 每次重叠256KB

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// 记录开始状态
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// 执行重叠写入
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

		// 下次写入与本次重叠
		currentOffset += writeChunkSize - overlapSize
	}

	// Flush
	_, err = ra.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	ra.Close()

	// 记录结束状态
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

// runRandomWriteSmallChunksTest 运行随机写性能测试（小数据块，多次写入）
func runRandomWriteSmallChunksTest(t *testing.T, name string, totalSize, chunkSize int64, sdkCfg *sdk.Config) PerformanceMetrics {
	// 初始化（与runSequentialWriteTest相同）
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
	core.InitDB()

	// 确保测试用户存在
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

	// 创建文件对象
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

	// 准备测试数据：小数据块，多次写入
	writeChunkSize := int64(64 * 1024) // 64KB per write
	writeCount := 100                  // 写入100次

	hasCompression := sdkCfg != nil && sdkCfg.WiseCmpr > 0
	hasEncryption := sdkCfg != nil && sdkCfg.EndecWay > 0

	// 记录开始状态
	var startMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&startMem)
	startTime := time.Now()

	// 执行小数据块随机写入
	ra, err := NewRandomAccessor(ofs, fileID)
	if err != nil {
		t.Fatalf("NewRandomAccessor failed: %v", err)
	}

	// 生成随机offset列表
	offsets := make([]int64, writeCount)
	for i := 0; i < writeCount; i++ {
		// 随机offset，但确保在totalSize范围内
		maxOffset := totalSize - writeChunkSize
		if maxOffset < 0 {
			maxOffset = 0
		}
		offsets[i] = int64(i%10) * (maxOffset / 10) // 分散在不同位置
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

	// 记录结束状态
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

// printPerformanceReport 打印性能报告
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

	// 分析单线程性能
	fmt.Println("\nSingle Thread Performance:")
	singleThread := filterResults(results, func(r PerformanceMetrics) bool {
		return r.Concurrency == 1
	})
	printAnalysis(singleThread)

	// 分析并发性能
	fmt.Println("\nConcurrent Performance:")
	concurrent := filterResults(results, func(r PerformanceMetrics) bool {
		return r.Concurrency > 1
	})
	printAnalysis(concurrent)

	// 分析加密压缩性能
	fmt.Println("\nEncryption/Compression Performance:")
	encrypted := filterResults(results, func(r PerformanceMetrics) bool {
		return r.HasEncryption || r.HasCompression
	})
	printAnalysis(encrypted)

	fmt.Println("\n" + strings.Repeat("=", 120))
}

// formatBytes 格式化字节数
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
