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

// runPerformanceTest 运行性能测试并返回指标
func runPerformanceTest(t *testing.T, name string, dataSize, chunkSize int64, writeOps, concurrency int, sdkCfg *sdk.Config) PerformanceMetrics {
	// 初始化
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test")
		os.MkdirAll(tmpDir, 0755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_perf_test_data")
		os.MkdirAll(tmpDir, 0755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}
	core.InitDB()

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
		MTime: time.Now().Unix(),
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

		// 优化：减少Flush频率，让缓冲区积累更多操作
		// 只在最后Flush一次，让Write操作快速返回
		for i := 0; i < writeOps; i++ {
			offset := int64(i) * dataSize
			err := ra.Write(offset, testData)
			if err != nil {
				t.Fatalf("Write failed: %v", err)
			}
			// 移除频繁的Flush，让缓冲区积累更多操作
		}
		// 只在最后Flush一次
		_, err = ra.Flush()
		if err != nil {
			t.Fatalf("Final Flush failed: %v", err)
		}
		ra.Close()
	} else {
		// 并发
		var wg sync.WaitGroup
		wg.Add(concurrency)
		for g := 0; g < concurrency; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				ra, err := NewRandomAccessor(ofs, fileID)
				if err != nil {
					t.Errorf("NewRandomAccessor failed in goroutine %d: %v", goroutineID, err)
					return
				}
				defer ra.Close()

				opsPerGoroutine := writeOps / concurrency
				for i := 0; i < opsPerGoroutine; i++ {
					offset := int64(goroutineID*opsPerGoroutine*int(dataSize) + i*int(dataSize))
					err := ra.Write(offset, testData)
					if err != nil {
						t.Errorf("Write failed in goroutine %d: %v", goroutineID, err)
						return
					}
				}
				_, err = ra.Flush()
				if err != nil {
					t.Errorf("Flush failed in goroutine %d: %v", goroutineID, err)
				}
			}(g)
		}
		wg.Wait()
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

	// 测试场景1: 小数据块，单线程（快速测试）
	t.Run("SmallData_SingleThread", func(t *testing.T) {
		result := runPerformanceTest(t, "small_single", 4*1024, 4*1024*1024, 10, 1, nil)
		results = append(results, result)
	})

	// 测试场景2: 中等数据块，单线程（快速测试）
	t.Run("MediumData_SingleThread", func(t *testing.T) {
		result := runPerformanceTest(t, "medium_single", 256*1024, 4*1024*1024, 5, 1, nil)
		results = append(results, result)
	})

	// 测试场景3: 小数据块，并发（3个goroutine，快速测试）
	t.Run("SmallData_Concurrent3", func(t *testing.T) {
		result := runPerformanceTest(t, "small_concurrent3", 4*1024, 4*1024*1024, 15, 3, nil)
		results = append(results, result)
	})

	// 测试场景4: 加密，单线程（快速测试）
	t.Run("Encrypted_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runPerformanceTest(t, "encrypted_single", 256*1024, 4*1024*1024, 5, 1, sdkCfg)
		results = append(results, result)
	})

	// 测试场景5: 压缩，单线程（快速测试）
	t.Run("Compressed_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
		}
		result := runPerformanceTest(t, "compressed_single", 256*1024, 4*1024*1024, 5, 1, sdkCfg)
		results = append(results, result)
	})

	// 测试场景6: 压缩+加密，单线程（快速测试）
	t.Run("CompressedEncrypted_SingleThread", func(t *testing.T) {
		sdkCfg := &sdk.Config{
			WiseCmpr: core.DATA_CMPR_SNAPPY,
			CmprQlty: 1,
			EndecWay: core.DATA_ENDEC_AES256,
			EndecKey: "this is a test encryption key that is long enough for AES256",
		}
		result := runPerformanceTest(t, "compressed_encrypted_single", 256*1024, 4*1024*1024, 5, 1, sdkCfg)
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

	// 打印性能报告
	printPerformanceReport(results)
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
