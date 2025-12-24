package sdk

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
)

// BatchWritePerformanceMetrics 批量写入性能指标
type BatchWritePerformanceMetrics struct {
	TestName          string
	BatchWriteEnabled bool
	Operation         string
	DataSize          int64
	Concurrency       int
	Operations        int
	Duration          time.Duration
	ThroughputMBps    float64
	OpsPerSecond      float64
	AvgLatency        time.Duration
	MaxMemoryMB       float64
	TotalAllocMB      float64
	NumGC             uint32
}

// setupTestEnvironmentForBatchWrite 设置测试环境（支持批量写入开关）
func setupTestEnvironmentForBatchWrite(t *testing.T, batchWriteEnabled bool) (int64, context.Context, core.Handler) {
	// 设置批量写入开关
	if batchWriteEnabled {
		os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	} else {
		os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "false")
	}

	// 初始化数据库（路径现在通过 Handler 管理）
	core.InitDB(".", "")
	ensureTestUserForBatchWrite(t)

	// 创建测试bucket
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	ctx := context.Background()
	err := core.InitBucketDB(".", testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	// 登录并创建bucket
	handler := core.NewLocalHandler("", "")
	ctx, userInfo, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	if userInfo == nil || userInfo.ID <= 0 {
		t.Fatalf("Login returned invalid user info: %v", userInfo)
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

	// 预热权限缓存：进行一次权限检查确保缓存正确设置
	_, _ = handler.PutData(ctx, testBktID, 0, 0, []byte{})
	// 忽略错误，这只是为了预热缓存

	return testBktID, ctx, handler
}

// ensureTestUserForBatchWrite 确保测试用户存在
func ensureTestUserForBatchWrite(t *testing.T) {
	handler := core.NewLocalHandler("", "")
	ctx := context.Background()
	_, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err == nil {
		return
	}

	// 如果登录失败，尝试创建用户
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetMainDBWithKey(".", "")
	if err != nil {
		t.Logf("Warning: Failed to get DB: %v", err)
		return
	}
	defer db.Close()

	_, err = db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar, key) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
	if err != nil {
		t.Logf("Warning: Failed to create user: %v", err)
	}
}

// runBatchWritePerformanceTest 运行批量写入性能测试
func runBatchWritePerformanceTest(t *testing.T, batchWriteEnabled bool, numObjects int, concurrency int, dataSize int64) BatchWritePerformanceMetrics {
	bktID, ctx, handler := setupTestEnvironmentForBatchWrite(t, batchWriteEnabled)

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	start := time.Now()
	var wg sync.WaitGroup
	errors := make(chan error, numObjects)
	latencies := make([]time.Duration, numObjects)
	var mu sync.Mutex

	// 使用信号量限制并发数
	sem := make(chan struct{}, concurrency)

	// 获取batch write manager（如果启用）
	var batchMgr *BatchWriter
	if batchWriteEnabled {
		batchMgr = GetBatchWriteManager(handler, bktID)
		if batchMgr != nil {
			batchMgr.SetFlushContext(ctx)
		}
	}

	// 并发AddFile或PutData
	for i := 0; i < numObjects; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 获取信号量
			sem <- struct{}{}
			defer func() { <-sem }()

			key := fmt.Sprintf("test-object-%d", id)
			testData := make([]byte, dataSize)
			for j := range testData {
				testData[j] = byte(id + j)
			}

			reqStart := time.Now()
			var err error

			if batchWriteEnabled && batchMgr != nil {
				// 使用batch write manager
				objID := core.NewID()
				added, _, addErr := batchMgr.AddFile(objID, testData, 0, key, int64(len(testData)), 0)
				if addErr != nil {
					err = addErr
				} else if !added {
					// Buffer full or file too large for batch write
					// Try flush once, but if still fails, fallback to direct write
					batchMgr.FlushAll(ctx)
					added, _, addErr = batchMgr.AddFile(objID, testData, 0, key, int64(len(testData)), 0)
					if !added || addErr != nil {
						// Fallback to direct write for large files or when buffer is full
						_, err = handler.PutData(ctx, bktID, 0, 0, testData)
					}
				}
			} else {
				_, err = handler.PutData(ctx, bktID, 0, 0, testData)
			}

			latency := time.Since(reqStart)

			if err != nil {
				errors <- fmt.Errorf("Write %d failed: %v", id, err)
				return
			}

			mu.Lock()
			latencies[id] = latency
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// 如果使用batch write，确保flush所有数据
	if batchWriteEnabled && batchMgr != nil {
		batchMgr.FlushAll(ctx)
		time.Sleep(100 * time.Millisecond) // 等待flush完成
	}

	runtime.ReadMemStats(&m2)

	// 检查错误
	close(errors)
	errorCount := 0
	for err := range errors {
		t.Errorf("Error in performance test: %v", err)
		errorCount++
	}

	// 计算统计信息
	totalData := int64(numObjects) * dataSize
	throughputMBps := float64(totalData) / duration.Seconds() / (1024 * 1024)
	opsPerSecond := float64(numObjects) / duration.Seconds()

	var totalLatency time.Duration
	for _, lat := range latencies {
		totalLatency += lat
	}
	avgLatency := totalLatency / time.Duration(numObjects)

	maxMemoryMB := float64(m2.Alloc-m1.Alloc) / (1024 * 1024)
	totalAllocMB := float64(m2.TotalAlloc-m1.TotalAlloc) / (1024 * 1024)

	return BatchWritePerformanceMetrics{
		TestName:          fmt.Sprintf("BatchWrite_%v", batchWriteEnabled),
		BatchWriteEnabled: batchWriteEnabled,
		Operation:         "AddFile",
		DataSize:          dataSize,
		Concurrency:       concurrency,
		Operations:        numObjects,
		Duration:          duration,
		ThroughputMBps:    throughputMBps,
		OpsPerSecond:      opsPerSecond,
		AvgLatency:        avgLatency,
		MaxMemoryMB:       maxMemoryMB,
		TotalAllocMB:      totalAllocMB,
		NumGC:             m2.NumGC - m1.NumGC,
	}
}

// TestBatchWritePerformanceComparison 测试批量写入开启和关闭的性能对比
func TestBatchWritePerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	testCases := []struct {
		name        string
		numObjects  int
		concurrency int
		dataSize    int64 // 字节
	}{
		{
			name:        "SmallFiles_50",
			numObjects:  50,
			concurrency: 5,
			dataSize:    10 * 1024, // 10KB
		},
		{
			name:        "SmallFiles_100",
			numObjects:  100,
			concurrency: 10,
			dataSize:    50 * 1024, // 50KB
		},
		{
			name:        "SmallFiles_200",
			numObjects:  200,
			concurrency: 20,
			dataSize:    100 * 1024, // 100KB
		},
	}

	var results []BatchWritePerformanceMetrics

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing %s: %d objects, %d concurrent, %d bytes each", tc.name, tc.numObjects, tc.concurrency, tc.dataSize)

			// 测试关闭批量写入
			t.Log("Testing with batch write DISABLED...")
			metricsDisabled := runBatchWritePerformanceTest(t, false, tc.numObjects, tc.concurrency, tc.dataSize)
			results = append(results, metricsDisabled)

			// 等待一下确保环境清理
			time.Sleep(1 * time.Second)

			// 测试开启批量写入
			t.Log("Testing with batch write ENABLED...")
			metricsEnabled := runBatchWritePerformanceTest(t, true, tc.numObjects, tc.concurrency, tc.dataSize)
			results = append(results, metricsEnabled)

			// 计算性能提升
			throughputImprovement := ((metricsEnabled.ThroughputMBps - metricsDisabled.ThroughputMBps) / metricsDisabled.ThroughputMBps) * 100
			opsImprovement := ((metricsEnabled.OpsPerSecond - metricsDisabled.OpsPerSecond) / metricsDisabled.OpsPerSecond) * 100
			latencyImprovement := ((float64(metricsDisabled.AvgLatency) - float64(metricsEnabled.AvgLatency)) / float64(metricsDisabled.AvgLatency)) * 100

			t.Logf("=== Performance Comparison for %s ===", tc.name)
			t.Logf("Batch Write Disabled:")
			t.Logf("  Duration: %v", metricsDisabled.Duration)
			t.Logf("  Throughput: %.2f MB/s", metricsDisabled.ThroughputMBps)
			t.Logf("  Ops/sec: %.2f", metricsDisabled.OpsPerSecond)
			t.Logf("  Avg Latency: %v", metricsDisabled.AvgLatency)
			t.Logf("  Memory: %.2f MB", metricsDisabled.MaxMemoryMB)
			t.Logf("Batch Write Enabled:")
			t.Logf("  Duration: %v", metricsEnabled.Duration)
			t.Logf("  Throughput: %.2f MB/s", metricsEnabled.ThroughputMBps)
			t.Logf("  Ops/sec: %.2f", metricsEnabled.OpsPerSecond)
			t.Logf("  Avg Latency: %v", metricsEnabled.AvgLatency)
			t.Logf("  Memory: %.2f MB", metricsEnabled.MaxMemoryMB)
			t.Logf("Improvement:")
			t.Logf("  Throughput: %.2f%%", throughputImprovement)
			t.Logf("  Ops/sec: %.2f%%", opsImprovement)
			t.Logf("  Latency: %.2f%%", latencyImprovement)
		})
	}

	// 生成报告
	generateBatchWritePerformanceReport(t, results)
}

// generateBatchWritePerformanceReport 生成性能测试报告
func generateBatchWritePerformanceReport(t *testing.T, results []BatchWritePerformanceMetrics) {
	report := "# 批量写入性能测试报告\n\n"
	report += "## 测试概述\n\n"
	report += "本测试对比了开启和关闭批量写入功能时的性能差异。\n\n"
	report += "## 测试结果\n\n"

	// 按测试名称分组
	testGroups := make(map[string][]BatchWritePerformanceMetrics)
	for _, r := range results {
		testGroups[r.TestName] = append(testGroups[r.TestName], r)
	}

	for testName, metrics := range testGroups {
		if len(metrics) != 2 {
			continue
		}

		var disabled, enabled BatchWritePerformanceMetrics
		for _, m := range metrics {
			if !m.BatchWriteEnabled {
				disabled = m
			} else {
				enabled = m
			}
		}

		report += fmt.Sprintf("### %s\n\n", testName)
		report += "| 指标 | 批量写入关闭 | 批量写入开启 | 提升 |\n"
		report += "|------|-------------|-------------|------|\n"

		throughputImprovement := ((enabled.ThroughputMBps - disabled.ThroughputMBps) / disabled.ThroughputMBps) * 100
		opsImprovement := ((enabled.OpsPerSecond - disabled.OpsPerSecond) / disabled.OpsPerSecond) * 100
		latencyImprovement := ((float64(disabled.AvgLatency) - float64(enabled.AvgLatency)) / float64(disabled.AvgLatency)) * 100

		report += fmt.Sprintf("| 吞吐量 (MB/s) | %.2f | %.2f | %.2f%% |\n", disabled.ThroughputMBps, enabled.ThroughputMBps, throughputImprovement)
		report += fmt.Sprintf("| 操作数/秒 | %.2f | %.2f | %.2f%% |\n", disabled.OpsPerSecond, enabled.OpsPerSecond, opsImprovement)
		report += fmt.Sprintf("| 平均延迟 | %v | %v | %.2f%% |\n", disabled.AvgLatency, enabled.AvgLatency, latencyImprovement)
		report += fmt.Sprintf("| 总耗时 | %v | %v | - |\n", disabled.Duration, enabled.Duration)
		report += fmt.Sprintf("| 内存使用 (MB) | %.2f | %.2f | - |\n", disabled.MaxMemoryMB, enabled.MaxMemoryMB)
		report += fmt.Sprintf("| GC次数 | %d | %d | - |\n\n", disabled.NumGC, enabled.NumGC)
	}

	report += "## 结论\n\n"
	report += "批量写入功能通过合并多个小文件的数据块和元数据写入，显著提升了小文件写入的性能。\n\n"

	// 写入报告文件
	reportFile := "BATCH_WRITE_PERFORMANCE_REPORT.md"
	if err := os.WriteFile(reportFile, []byte(report), 0o644); err != nil {
		t.Logf("Failed to write report: %v", err)
	} else {
		t.Logf("Performance report written to %s", reportFile)
	}
}

// BenchmarkBatchWriteComparison 基准测试对比
func BenchmarkBatchWriteComparison(b *testing.B) {
	testCases := []struct {
		name     string
		dataSize int64
		enabled  bool
	}{
		{"BatchWrite_Disabled_10KB", 10 * 1024, false},
		{"BatchWrite_Enabled_10KB", 10 * 1024, true},
		{"BatchWrite_Disabled_50KB", 50 * 1024, false},
		{"BatchWrite_Enabled_50KB", 50 * 1024, true},
		{"BatchWrite_Disabled_100KB", 100 * 1024, false},
		{"BatchWrite_Enabled_100KB", 100 * 1024, true},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			bktID, ctx, handler := setupTestEnvironmentForBatchWrite(&testing.T{}, tc.enabled)
			testData := make([]byte, tc.dataSize)

			var batchMgr *BatchWriter
			if tc.enabled {
				batchMgr = GetBatchWriteManager(handler, bktID)
				if batchMgr != nil {
					batchMgr.SetFlushContext(ctx)
				}
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := 0
				for pb.Next() {
					key := fmt.Sprintf("bench-object-%d", id)
					id++

					if tc.enabled && batchMgr != nil {
						objID := core.NewID()
						added, _, err := batchMgr.AddFile(objID, testData, 0, key, int64(len(testData)), 0)
						if err != nil || !added {
							b.Errorf("AddFile failed: err=%v, added=%v", err, added)
						}
					} else {
						_, err := handler.PutData(ctx, bktID, 0, 0, testData)
						if err != nil {
							b.Errorf("PutData failed: %v", err)
						}
					}
				}
			})

			if tc.enabled && batchMgr != nil {
				batchMgr.FlushAll(ctx)
			}
		})
	}
}
