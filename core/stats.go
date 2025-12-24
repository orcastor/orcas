package core

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	b "github.com/orca-zhang/borm"
	"github.com/orca-zhang/ecache2"
)

// BucketStatsDelta incremental update for bucket space statistics
type BucketStatsDelta struct {
	Used         int64  // Incremental change in Used (atomic)
	RealUsed     int64  // Incremental change in RealUsed (atomic)
	LogicalUsed  int64  // Incremental change in LogicalUsed (atomic)
	DedupSavings int64  // Incremental change in DedupSavings (instant upload space savings) (atomic)
	DataPath     string // Data path for this bucket
}

// Add adds incremental changes using atomic operations
func (bsd *BucketStatsDelta) Add(used, realUsed, logicalUsed, dedupSavings int64) {
	atomic.AddInt64(&bsd.Used, used)
	atomic.AddInt64(&bsd.RealUsed, realUsed)
	atomic.AddInt64(&bsd.LogicalUsed, logicalUsed)
	atomic.AddInt64(&bsd.DedupSavings, dedupSavings)
}

// Get retrieves current incremental values (for flushing) and resets them atomically
func (bsd *BucketStatsDelta) Get() (used, realUsed, logicalUsed, dedupSavings int64) {
	used = atomic.SwapInt64(&bsd.Used, 0)
	realUsed = atomic.SwapInt64(&bsd.RealUsed, 0)
	logicalUsed = atomic.SwapInt64(&bsd.LogicalUsed, 0)
	dedupSavings = atomic.SwapInt64(&bsd.DedupSavings, 0)
	return used, realUsed, logicalUsed, dedupSavings
}

// bucketStatsCache Async cache for bucket space statistics
// key: "bkt_stats_<bktID>", value: *BucketStatsDelta
var bucketStatsCache = ecache2.NewLRUCache[int64](16, 1024, 2*time.Second)

func init() {
	// When cache item is updated or expired, asynchronously flush to database
	bucketStatsCache.Inspect(func(action int, key int64, iface *interface{}, bytes []byte, status int) {
		// action: PUT means update/add, DEL means delete/expire
		// status: 0 means new item, 1 means update, 2 means delete
		if action == ecache2.DEL && status == 1 {
			// Cache item expired, need to flush to database
			if iface != nil && *iface != nil {
				if delta, ok := (*iface).(*BucketStatsDelta); ok {
					// Asynchronously flush to database
					go flushBucketStats(key, delta)
				}
			}
		}
	})

	// Start periodic flush goroutine, periodically flush all cache items
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// Traverse all cache items, trigger flush
			bucketStatsCache.Walk(func(key int64, iface *interface{}, bytes []byte, expireAt int64) bool {
				if iface != nil && *iface != nil {
					if delta, ok := (*iface).(*BucketStatsDelta); ok {
						// Asynchronously flush to database
						go flushBucketStats(key, delta)
					}
				}
				return true
			})
		}
	}()
}

// flushBucketStats Flush bucket space statistics to database
func flushBucketStats(bktID int64, delta *BucketStatsDelta) {
	if bktID <= 0 {
		return
	}

	// Get delta values and reset (Get() resets the values)
	used, realUsed, logicalUsed, dedupSavings := delta.Get()
	if used == 0 && realUsed == 0 && logicalUsed == 0 && dedupSavings == 0 {
		return
	}

	// Build update map: borm.Update will add field name, so value should only contain expression
	v := b.V{}
	if used != 0 {
		v["u"] = b.U(fmt.Sprintf("MAX(0, u + (%d))", used))
	}
	if realUsed != 0 {
		v["ru"] = b.U(fmt.Sprintf("MAX(0, ru + (%d))", realUsed))
	}
	if logicalUsed != 0 {
		v["lu"] = b.U(fmt.Sprintf("MAX(0, lu + (%d))", logicalUsed))
	}
	if dedupSavings != 0 {
		v["ds"] = b.U(fmt.Sprintf("MAX(0, ds + (%d))", dedupSavings))
	}
	if len(v) == 0 {
		return
	}

	// Execute merged UPDATE using borm
	if len(v) > 0 {
		// Batch update database
		// Use write connection for bucket database (bucket info is now stored in bucket database)
		// Get dataPath from delta (set when BucketStatsDelta is created)
		dataPath := delta.DataPath
		if dataPath == "" {
			dataPath = "."
		}
		bktDirPath := filepath.Join(dataPath, fmt.Sprint(bktID))
		db, err := GetWriteDB(bktDirPath, "")
		if err != nil {
			return
		}
		// Batch update database using borm with context
		c := context.Background()
		_, err = b.TableContext(c, db, BKT_TBL).Debug().Update(v, b.Where(b.Eq("id", bktID)))
		if err != nil {
			// Log error but don't return (async operation, error handling is best effort)
			return
		}
	}
}

// updateBucketStatsCache Update bucket space statistics cache (async)
func updateBucketStatsCache(bktID int64, dataPath string, used, realUsed, logicalUsed, dedupSavings int64) {
	// Get or create BucketStatsDelta
	var delta *BucketStatsDelta
	if v, ok := bucketStatsCache.Get(bktID); ok {
		if d, ok := v.(*BucketStatsDelta); ok {
			delta = d
		}
	}

	if delta == nil {
		if dataPath == "" {
			dataPath = "."
		}
		delta = &BucketStatsDelta{
			DataPath:     dataPath,
			Used:         used,
			RealUsed:     realUsed,
			LogicalUsed:  logicalUsed,
			DedupSavings: dedupSavings,
		}
		bucketStatsCache.Put(bktID, delta)
		return
	}
	// Update dataPath if it's not set (was ".") and we have a valid dataPath
	if delta.DataPath == "" || delta.DataPath == "." {
		if dataPath != "" {
			delta.DataPath = dataPath
		} else {
			delta.DataPath = "."
		}
	}

	// Add delta
	delta.Add(used, realUsed, logicalUsed, dedupSavings)
	return
}
