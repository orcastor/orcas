package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/orca-zhang/ecache"
)

// BucketStatsDelta incremental update for bucket space statistics
type BucketStatsDelta struct {
	Used         int64 // Incremental change in Used
	RealUsed     int64 // Incremental change in RealUsed
	LogicalUsed  int64 // Incremental change in LogicalUsed
	DedupSavings int64 // Incremental change in DedupSavings (instant upload space savings)
	mu           sync.Mutex
}

// Add adds incremental changes
func (bsd *BucketStatsDelta) Add(used, realUsed, logicalUsed, dedupSavings int64) {
	bsd.mu.Lock()
	defer bsd.mu.Unlock()
	bsd.Used += used
	bsd.RealUsed += realUsed
	bsd.LogicalUsed += logicalUsed
	bsd.DedupSavings += dedupSavings
}

// Get retrieves current incremental values (for flushing)
func (bsd *BucketStatsDelta) Get() (used, realUsed, logicalUsed, dedupSavings int64) {
	bsd.mu.Lock()
	defer bsd.mu.Unlock()
	return bsd.Used, bsd.RealUsed, bsd.LogicalUsed, bsd.DedupSavings
}

// Reset resets incremental values
func (bsd *BucketStatsDelta) Reset() {
	bsd.mu.Lock()
	defer bsd.mu.Unlock()
	bsd.Used = 0
	bsd.RealUsed = 0
	bsd.LogicalUsed = 0
	bsd.DedupSavings = 0
}

// bucketStatsCache Async cache for bucket space statistics
// key: "bkt_stats_<bktID>", value: *BucketStatsDelta
var bucketStatsCache = ecache.NewLRUCache(16, 1024, 2*time.Second)

func init() {
	// When cache item is updated or expired, asynchronously flush to database
	bucketStatsCache.Inspect(func(action int, key string, iface *interface{}, bytes []byte, status int) {
		// action: PUT means update/add, DEL means delete/expire
		// status: 0 means new item, 1 means update, 2 means delete
		if action == ecache.DEL && status == 1 {
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
			bucketStatsCache.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
				if iface != nil && *iface != nil {
					if delta, ok := (*iface).(*BucketStatsDelta); ok {
						// Check if there is data to flush
						used, realUsed, logicalUsed, dedupSavings := delta.Get()
						if used != 0 || realUsed != 0 || logicalUsed != 0 || dedupSavings != 0 {
							// Asynchronously flush to database
							go flushBucketStats(key, delta)
						}
					}
				}
				return true
			})
		}
	}()
}

// flushBucketStats Flush bucket space statistics to database
func flushBucketStats(key string, delta *BucketStatsDelta) {
	// Parse bucket ID
	// key format: "bkt_stats_<bktID>"
	var bktID int64
	_, err := fmt.Sscanf(key, "bkt_stats_%d", &bktID)
	if err != nil || bktID <= 0 {
		return
	}

	// Get delta values and reset
	used, realUsed, logicalUsed, dedupSavings := delta.Get()
	if used == 0 && realUsed == 0 && logicalUsed == 0 && dedupSavings == 0 {
		return // No data to flush
	}
	delta.Reset()

	// Batch update database
	db, err := GetDB()
	if err != nil {
		return
	}
	defer db.Close()

	// Batch execute updates (merge updates for multiple fields)
	if used != 0 {
		if used > 0 {
			_, _ = db.Exec("UPDATE bkt SET used = used + ? WHERE id = ?", used, bktID)
		} else {
			_, _ = db.Exec("UPDATE bkt SET used = MAX(0, used - ?) WHERE id = ?", -used, bktID)
		}
	}
	if realUsed != 0 {
		if realUsed > 0 {
			_, _ = db.Exec("UPDATE bkt SET real_used = real_used + ? WHERE id = ?", realUsed, bktID)
		} else {
			_, _ = db.Exec("UPDATE bkt SET real_used = MAX(0, real_used - ?) WHERE id = ?", -realUsed, bktID)
		}
	}
	if logicalUsed != 0 {
		if logicalUsed > 0 {
			_, _ = db.Exec("UPDATE bkt SET logical_used = logical_used + ? WHERE id = ?", logicalUsed, bktID)
		} else {
			_, _ = db.Exec("UPDATE bkt SET logical_used = MAX(0, logical_used - ?) WHERE id = ?", -logicalUsed, bktID)
		}
	}
	if dedupSavings != 0 {
		if dedupSavings > 0 {
			_, _ = db.Exec("UPDATE bkt SET dedup_savings = dedup_savings + ? WHERE id = ?", dedupSavings, bktID)
		} else {
			_, _ = db.Exec("UPDATE bkt SET dedup_savings = MAX(0, dedup_savings - ?) WHERE id = ?", -dedupSavings, bktID)
		}
	}
}

// updateBucketStatsCache Update bucket space statistics cache (async)
func updateBucketStatsCache(bktID int64, used, realUsed, logicalUsed, dedupSavings int64) {
	key := fmt.Sprintf("bkt_stats_%d", bktID)

	// Get or create BucketStatsDelta
	var delta *BucketStatsDelta
	if v, ok := bucketStatsCache.Get(key); ok {
		if d, ok := v.(*BucketStatsDelta); ok {
			delta = d
		}
	}

	if delta == nil {
		delta = &BucketStatsDelta{}
		bucketStatsCache.Put(key, delta)
	}

	// Add delta
	delta.Add(used, realUsed, logicalUsed, dedupSavings)

	// Trigger update (let ecache know there are changes, may trigger flush)
	bucketStatsCache.Put(key, delta)
}
