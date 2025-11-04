package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/orca-zhang/ecache"
)

// BucketStatsDelta 桶空间统计的增量更新
type BucketStatsDelta struct {
	Used         int64 // Used的增量
	RealUsed     int64 // RealUsed的增量
	LogicalUsed  int64 // LogicalUsed的增量
	DedupSavings int64 // DedupSavings的增量（秒传节省空间）
	mu           sync.Mutex
}

// Add 添加增量
func (bsd *BucketStatsDelta) Add(used, realUsed, logicalUsed, dedupSavings int64) {
	bsd.mu.Lock()
	defer bsd.mu.Unlock()
	bsd.Used += used
	bsd.RealUsed += realUsed
	bsd.LogicalUsed += logicalUsed
	bsd.DedupSavings += dedupSavings
}

// Get 获取当前的增量值（用于刷新）
func (bsd *BucketStatsDelta) Get() (used, realUsed, logicalUsed, dedupSavings int64) {
	bsd.mu.Lock()
	defer bsd.mu.Unlock()
	return bsd.Used, bsd.RealUsed, bsd.LogicalUsed, bsd.DedupSavings
}

// Reset 重置增量值
func (bsd *BucketStatsDelta) Reset() {
	bsd.mu.Lock()
	defer bsd.mu.Unlock()
	bsd.Used = 0
	bsd.RealUsed = 0
	bsd.LogicalUsed = 0
	bsd.DedupSavings = 0
}

// bucketStatsCache 桶空间统计的异步缓存
// key: "bkt_stats_<bktID>", value: *BucketStatsDelta
var bucketStatsCache = ecache.NewLRUCache(16, 1024, 2*time.Second)

func init() {
	// 当缓存项被更新或过期时，异步刷新到数据库
	bucketStatsCache.Inspect(func(action int, key string, iface *interface{}, bytes []byte, status int) {
		// action: PUT表示更新/新增, DEL表示删除/过期
		// status: 0表示新项, 1表示更新, 2表示删除
		if action == ecache.DEL && status == 1 {
			// 缓存项过期，需要刷新到数据库
			if iface != nil && *iface != nil {
				if delta, ok := (*iface).(*BucketStatsDelta); ok {
					// 异步刷新到数据库
					go flushBucketStats(key, delta)
				}
			}
		}
	})

	// 启动定期刷新goroutine，定期刷新所有缓存项
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// 遍历所有缓存项，触发刷新
			bucketStatsCache.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
				if iface != nil && *iface != nil {
					if delta, ok := (*iface).(*BucketStatsDelta); ok {
						// 检查是否有待刷新的数据
						used, realUsed, logicalUsed, dedupSavings := delta.Get()
						if used != 0 || realUsed != 0 || logicalUsed != 0 || dedupSavings != 0 {
							// 异步刷新到数据库
							go flushBucketStats(key, delta)
						}
					}
				}
				return true
			})
		}
	}()
}

// flushBucketStats 将桶空间统计刷新到数据库
func flushBucketStats(key string, delta *BucketStatsDelta) {
	// 解析bucket ID
	// key格式: "bkt_stats_<bktID>"
	var bktID int64
	_, err := fmt.Sscanf(key, "bkt_stats_%d", &bktID)
	if err != nil || bktID <= 0 {
		return
	}

	// 获取增量值并重置
	used, realUsed, logicalUsed, dedupSavings := delta.Get()
	if used == 0 && realUsed == 0 && logicalUsed == 0 && dedupSavings == 0 {
		return // 没有需要刷新的数据
	}
	delta.Reset()

	// 批量更新数据库
	db, err := GetDB()
	if err != nil {
		return
	}
	defer db.Close()

	// 批量执行更新（合并多个字段的更新）
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

// updateBucketStatsCache 更新桶空间统计缓存（异步）
func updateBucketStatsCache(bktID int64, used, realUsed, logicalUsed, dedupSavings int64) {
	key := fmt.Sprintf("bkt_stats_%d", bktID)

	// 获取或创建BucketStatsDelta
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

	// 添加增量
	delta.Add(used, realUsed, logicalUsed, dedupSavings)

	// 触发更新（让ecache知道有变化，可能会触发刷新）
	bucketStatsCache.Put(key, delta)
}
