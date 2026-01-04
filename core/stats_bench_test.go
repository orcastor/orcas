package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	borm "github.com/orca-zhang/borm"
)

// BenchmarkBucketStatsDelta_Add benchmarks the Add operation
func BenchmarkBucketStatsDelta_Add(b *testing.B) {
	delta := &BucketStatsDelta{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			delta.Add(1, 1, 1, 1)
		}
	})
}

// BenchmarkBucketStatsDelta_Get benchmarks the Get operation
func BenchmarkBucketStatsDelta_Get(b *testing.B) {
	delta := &BucketStatsDelta{
		Used:         100,
		RealUsed:     200,
		LogicalUsed: 300,
		DedupSavings: 400,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		used, realUsed, logicalUsed, dedupSavings := delta.Get()
		// Restore values for next iteration
		delta.Add(used, realUsed, logicalUsed, dedupSavings)
	}
}

// BenchmarkBucketStatsDelta_GetBucketInfoFromCache benchmarks cache hit performance
func BenchmarkBucketStatsDelta_GetBucketInfoFromCache(b *testing.B) {
	// Setup: create a bucket in cache
	bktID := int64(12345)
	bucketInfo := &BucketInfo{
		ID:           bktID,
		Name:         "test_bucket",
		Type:         1,
		Quota:        1000000,
		Used:         100,
		RealUsed:     200,
		LogicalUsed: 300,
		DedupSavings: 400,
		ChunkSize:    10 * 1024 * 1024,
	}
	updateBucketInfoInCache(bktID, ".", bucketInfo)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = getBucketInfoFromCache(bktID)
	}
}

// BenchmarkBucketStatsDelta_UpdateBucketStatsCache benchmarks update performance
func BenchmarkBucketStatsDelta_UpdateBucketStatsCache(b *testing.B) {
	bktID := int64(12345)
	dataPath := "."
	
	// Pre-create delta in cache
	getOrCreateBucketStatsDelta(bktID, dataPath)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			updateBucketStatsCache(bktID, dataPath, 1, 1, 1, 1)
		}
	})
}

// BenchmarkGetBkt_CacheHit benchmarks GetBkt with cache hit
func BenchmarkGetBkt_CacheHit(b *testing.B) {
	baseDir, dataDir, cleanup := setupBenchDirs("bench_getbkt_cache")
	defer cleanup()

	InitDB(baseDir, "")
	ig := idgen.NewIDGen(nil, 0)
	bktID, _ := ig.New()
	InitBucketDB(dataDir, bktID)

	dma := &DefaultMetadataAdapter{
		DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
		DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
	}
	dma.DefaultBaseMetadataAdapter.SetPath(baseDir)
	dma.DefaultDataMetadataAdapter.SetPath(dataDir)

	// Create bucket and cache it
	bucket := &BucketInfo{
		ID:    bktID,
		Name:  "test",
		Type:  1,
		Quota: 1000000,
	}
	ctx := context.Background()
	if err := dma.PutBkt(ctx, []*BucketInfo{bucket}); err != nil {
		b.Fatalf("PutBkt failed: %v", err)
	}

	// Warm up cache
	_, _ = dma.GetBkt(ctx, []int64{bktID})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dma.GetBkt(ctx, []int64{bktID})
	}
}

// BenchmarkGetBkt_CacheMiss benchmarks GetBkt with cache miss
func BenchmarkGetBkt_CacheMiss(b *testing.B) {
	baseDir, dataDir, cleanup := setupBenchDirs("bench_getbkt_db")
	defer cleanup()

	InitDB(baseDir, "")
	ig := idgen.NewIDGen(nil, 0)
	bktID, _ := ig.New()
	InitBucketDB(dataDir, bktID)

	dma := &DefaultMetadataAdapter{
		DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
		DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
	}
	dma.DefaultBaseMetadataAdapter.SetPath(baseDir)
	dma.DefaultDataMetadataAdapter.SetPath(dataDir)

	// Create bucket
	bucket := &BucketInfo{
		ID:    bktID,
		Name:  "test",
		Type:  1,
		Quota: 1000000,
	}
	ctx := context.Background()
	if err := dma.PutBkt(ctx, []*BucketInfo{bucket}); err != nil {
		b.Fatalf("PutBkt failed: %v", err)
	}

	// Invalidate cache before each iteration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		invalidateBucketInfoCache(bktID)
		_, _ = dma.GetBkt(ctx, []int64{bktID})
	}
}

// BenchmarkGetBkt_Concurrent benchmarks concurrent GetBkt calls
func BenchmarkGetBkt_Concurrent(b *testing.B) {
	baseDir, dataDir, cleanup := setupBenchDirs("bench_getbkt_concurrent")
	defer cleanup()

	InitDB(baseDir, "")
	ig := idgen.NewIDGen(nil, 0)
	bktID, _ := ig.New()
	InitBucketDB(dataDir, bktID)

	dma := &DefaultMetadataAdapter{
		DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
		DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
	}
	dma.DefaultBaseMetadataAdapter.SetPath(baseDir)
	dma.DefaultDataMetadataAdapter.SetPath(dataDir)

	// Create bucket and cache it
	bucket := &BucketInfo{
		ID:    bktID,
		Name:  "test",
		Type:  1,
		Quota: 1000000,
	}
	ctx := context.Background()
	if err := dma.PutBkt(ctx, []*BucketInfo{bucket}); err != nil {
		b.Fatalf("PutBkt failed: %v", err)
	}

	// Warm up cache
	_, _ = dma.GetBkt(ctx, []int64{bktID})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = dma.GetBkt(ctx, []int64{bktID})
		}
	})
}

// BenchmarkGetBkt_DirectDB benchmarks direct database query (for comparison)
func BenchmarkGetBkt_DirectDB(b *testing.B) {
	baseDir, dataDir, cleanup := setupBenchDirs("bench_getbkt_direct")
	defer cleanup()

	InitDB(baseDir, "")
	ig := idgen.NewIDGen(nil, 0)
	bktID, _ := ig.New()
	InitBucketDB(dataDir, bktID)

	// Create bucket directly in database
	bktDirPath := filepath.Join(dataDir, fmt.Sprint(bktID))
	db, err := GetWriteDB(bktDirPath)
	if err != nil {
		b.Fatalf("GetWriteDB failed: %v", err)
	}

	bucket := &BucketInfo{
		ID:    bktID,
		Name:  "test",
		Type:  1,
		Quota: 1000000,
	}
	bktSlice := []*BucketInfo{bucket}
	ctx := context.Background()
	if _, err = borm.TableContext(ctx, db, BKT_TBL).ReplaceInto(&bktSlice); err != nil {
		b.Fatalf("ReplaceInto failed: %v", err)
	}

	// Switch to read connection
	readDB, err := GetReadDB(bktDirPath)
	if err != nil {
		b.Fatalf("GetReadDB failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var bucketInfo []*BucketInfo
		_, _ = borm.TableContext(ctx, readDB, BKT_TBL).Select(&bucketInfo, borm.Where(borm.Eq("id", bktID)))
	}
}

// BenchmarkUpdateBucketStatsCache_Concurrent benchmarks concurrent updates
func BenchmarkUpdateBucketStatsCache_Concurrent(b *testing.B) {
	ig := idgen.NewIDGen(nil, 0)
	bktID, _ := ig.New()
	dataPath := "."

	// Pre-create delta in cache
	getOrCreateBucketStatsDelta(bktID, dataPath)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			updateBucketStatsCache(bktID, dataPath, 1, 1, 1, 1)
		}
	})
}

// BenchmarkGetBkt_MultipleBuckets benchmarks GetBkt with multiple buckets
func BenchmarkGetBkt_MultipleBuckets(b *testing.B) {
	baseDir, dataDir, cleanup := setupBenchDirs("bench_getbkt_multi")
	defer cleanup()

	InitDB(baseDir, "")
	ig := idgen.NewIDGen(nil, 0)
	
	dma := &DefaultMetadataAdapter{
		DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
		DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
	}
	dma.DefaultBaseMetadataAdapter.SetPath(baseDir)
	dma.DefaultDataMetadataAdapter.SetPath(dataDir)

	// Create 10 buckets
	bktIDs := make([]int64, 10)
	buckets := make([]*BucketInfo, 10)
	ctx := context.Background()
	
	for i := 0; i < 10; i++ {
		bktID, _ := ig.New()
		bktIDs[i] = bktID
		InitBucketDB(dataDir, bktID)
		buckets[i] = &BucketInfo{
			ID:    bktID,
			Name:  fmt.Sprintf("bucket_%d", i),
			Type:  1,
			Quota: 1000000,
		}
	}
	
	if err := dma.PutBkt(ctx, buckets); err != nil {
		b.Fatalf("PutBkt failed: %v", err)
	}

	// Warm up cache
	_, _ = dma.GetBkt(ctx, bktIDs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dma.GetBkt(ctx, bktIDs)
	}
}

// setupBenchDirs creates temporary directories for benchmarking
func setupBenchDirs(prefix string) (baseDir, dataDir string, cleanup func()) {
	tmpBaseDir, _ := os.MkdirTemp("", prefix+"_base_")
	tmpDataDir, _ := os.MkdirTemp("", prefix+"_data_")
	
	cleanup = func() {
		os.RemoveAll(tmpBaseDir)
		os.RemoveAll(tmpDataDir)
	}
	
	return tmpBaseDir, tmpDataDir, cleanup
}

// TestBucketStatsDelta_PerformanceComparison runs a performance comparison test
func TestBucketStatsDelta_PerformanceComparison(t *testing.T) {
	baseDir, dataDir, cleanup := setupBenchDirs("perf_compare")
	defer cleanup()

	InitDB(baseDir, "")
	ig := idgen.NewIDGen(nil, 0)
	bktID, _ := ig.New()
	InitBucketDB(dataDir, bktID)

	dma := &DefaultMetadataAdapter{
		DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
		DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
	}
	dma.DefaultBaseMetadataAdapter.SetPath(baseDir)
	dma.DefaultDataMetadataAdapter.SetPath(dataDir)

	// Create bucket
	bucket := &BucketInfo{
		ID:    bktID,
		Name:  "test",
		Type:  1,
		Quota: 1000000,
	}
	ctx := context.Background()
	if err := dma.PutBkt(ctx, []*BucketInfo{bucket}); err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	// Warm up cache
	_, _ = dma.GetBkt(ctx, []int64{bktID})

	// Test cache hit performance
	iterations := 10000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_, _ = dma.GetBkt(ctx, []int64{bktID})
	}
	cacheHitDuration := time.Since(start)

	// Test cache miss performance
	start = time.Now()
	for i := 0; i < iterations; i++ {
		invalidateBucketInfoCache(bktID)
		_, _ = dma.GetBkt(ctx, []int64{bktID})
	}
	cacheMissDuration := time.Since(start)

	t.Logf("Cache Hit:  %d iterations in %v (%.2f ops/sec)", 
		iterations, cacheHitDuration, float64(iterations)/cacheHitDuration.Seconds())
	t.Logf("Cache Miss: %d iterations in %v (%.2f ops/sec)", 
		iterations, cacheMissDuration, float64(iterations)/cacheMissDuration.Seconds())
	t.Logf("Speedup: %.2fx", cacheMissDuration.Seconds()/cacheHitDuration.Seconds())
}

// TestBucketStatsDelta_ConcurrentSafety tests concurrent safety
func TestBucketStatsDelta_ConcurrentSafety(t *testing.T) {
	bktID := int64(12345)
	dataPath := "."
	
	// Pre-create delta in cache
	getOrCreateBucketStatsDelta(bktID, dataPath)

	var wg sync.WaitGroup
	goroutines := 100
	opsPerGoroutine := 1000

	// Concurrent updates
	start := time.Now()
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				updateBucketStatsCache(bktID, dataPath, 1, 1, 1, 1)
			}
		}()
	}
	wg.Wait()
	duration := time.Since(start)

	totalOps := goroutines * opsPerGoroutine
	t.Logf("Concurrent updates: %d ops in %v (%.2f ops/sec)", 
		totalOps, duration, float64(totalOps)/duration.Seconds())
	
	// Verify final state
	if v, ok := bucketStatsCache.Get(bktID); ok {
		if delta, ok := v.(*BucketStatsDelta); ok && delta != nil {
			used := atomic.LoadInt64(&delta.Used)
			realUsed := atomic.LoadInt64(&delta.RealUsed)
			logicalUsed := atomic.LoadInt64(&delta.LogicalUsed)
			dedupSavings := atomic.LoadInt64(&delta.DedupSavings)
			
			expected := int64(totalOps)
			if used != expected || realUsed != expected || 
			   logicalUsed != expected || dedupSavings != expected {
				t.Errorf("Expected all values to be %d, got used=%d, realUsed=%d, logicalUsed=%d, dedupSavings=%d",
					expected, used, realUsed, logicalUsed, dedupSavings)
			} else {
				t.Logf("Concurrent safety verified: all values = %d", expected)
			}
		}
	}
}

