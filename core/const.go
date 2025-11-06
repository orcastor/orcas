package core

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

/*
环境变量配置清单

以下是 ORCAS 系统支持的所有环境变量配置项：

1. 基础路径配置
   - ORCAS_BASE: 基础路径，用于存储元数据等基础文件（字符串）
   - ORCAS_DATA: 数据路径，用于存储数据文件（字符串）

2. 删除延迟配置
   - ORCAS_DELETE_DELAY: 删除延迟时间（秒），等待指定时间后删除数据文件
     默认值: 5
     示例: export ORCAS_DELETE_DELAY=10

3. 资源控制配置
   - ORCAS_BATCH_INTERVAL_MS: 批次间隔时间（毫秒），每批处理之间的延迟
     默认值: 100
     示例: export ORCAS_BATCH_INTERVAL_MS=200

   - ORCAS_MAX_DURATION_SEC: 最大运行时长（秒），超过此时间后停止处理
     默认值: 0（不限制）
     示例: export ORCAS_MAX_DURATION_SEC=3600

   - ORCAS_MAX_ITEMS_PER_SEC: 每秒最大处理项数，用于速率限制
     默认值: 0（不限制）
     示例: export ORCAS_MAX_ITEMS_PER_SEC=1000

   - ORCAS_ADAPTIVE_DELAY: 是否启用自适应延迟（true/false/1/0）
     默认值: true
     示例: export ORCAS_ADAPTIVE_DELAY=true

   - ORCAS_ADAPTIVE_DELAY_FACTOR: 自适应延迟因子，延迟时间 = BatchInterval * (1 + 已处理项数 / 因子)
     默认值: 1000
     示例: export ORCAS_ADAPTIVE_DELAY_FACTOR=2000

4. 版本保留策略配置
   - ORCAS_MIN_VERSION_INTERVAL_SEC: 最小版本间隔时间（秒），在指定时间内只允许创建一个版本
     默认值: 0（不限制）
     示例: export ORCAS_MIN_VERSION_INTERVAL_SEC=300

   - ORCAS_MAX_VERSIONS: 最大保留版本数，超过此数量的旧版本会被删除
     默认值: 0（不限制）
     示例: export ORCAS_MAX_VERSIONS=10

5. 随机写缓冲配置
   - ORCAS_WRITE_BUFFER_WINDOW_SEC: 缓冲窗口时间（秒），在指定时间内的多次写入会被合并
     默认值: 10
     示例: export ORCAS_WRITE_BUFFER_WINDOW_SEC=10

   - ORCAS_MAX_WRITE_BUFFER_SIZE: 最大缓冲区大小（字节），超过此大小会立即触发写入
     默认值: 8388608 (8MB)
     示例: export ORCAS_MAX_WRITE_BUFFER_SIZE=16777216

   - ORCAS_MAX_WRITE_BUFFER_COUNT: 最大缓冲写入次数，超过此次数会立即触发写入
     默认值: 200
     示例: export ORCAS_MAX_WRITE_BUFFER_COUNT=300

6. 定时任务配置（Crontab）
   - ORCAS_CRON_SCRUB_ENABLED: 是否启用ScrubData定时任务（true/false/1/0）
     默认值: false
     示例: export ORCAS_CRON_SCRUB_ENABLED=true

   - ORCAS_CRON_SCRUB_SCHEDULE: ScrubData的cron表达式（格式：分钟 小时 天 月 星期）
     默认值: "0 2 * * *" (每天凌晨2点)
     示例: export ORCAS_CRON_SCRUB_SCHEDULE="0 2 * * *"

   - ORCAS_CRON_MERGE_ENABLED: 是否启用MergeDuplicateData定时任务（true/false/1/0）
     默认值: false
     示例: export ORCAS_CRON_MERGE_ENABLED=true

   - ORCAS_CRON_MERGE_SCHEDULE: MergeDuplicateData的cron表达式
     默认值: "0 3 * * *" (每天凌晨3点)
     示例: export ORCAS_CRON_MERGE_SCHEDULE="0 3 * * *"

   - ORCAS_CRON_DEFRAGMENT_ENABLED: 是否启用Defragment定时任务（true/false/1/0）
     默认值: false
     示例: export ORCAS_CRON_DEFRAGMENT_ENABLED=true

   - ORCAS_CRON_DEFRAGMENT_SCHEDULE: Defragment的cron表达式
     默认值: "0 4 * * 0" (每周日凌晨4点)
     示例: export ORCAS_CRON_DEFRAGMENT_SCHEDULE="0 4 * * 0"

   - ORCAS_CRON_DEFRAGMENT_MAX_SIZE: Defragment的最大文件大小（小于此大小的文件会被打包）
     默认值: 10485760 (10MB)
     示例: export ORCAS_CRON_DEFRAGMENT_MAX_SIZE=20971520

   - ORCAS_CRON_DEFRAGMENT_ACCESS_WINDOW: Defragment的访问窗口时间（秒）
     默认值: 0（不限制）
     示例: export ORCAS_CRON_DEFRAGMENT_ACCESS_WINDOW=3600

   - ORCAS_CRON_DEFRAGMENT_THRESHOLD: Defragment的空间占用阈值（百分比，0-100）
     只有当碎片率（(RealUsed - LogicalUsed) / RealUsed * 100）达到此阈值时才执行碎片整理
     LogicalUsed统计所有有效对象（未删除的）的逻辑占用大小
     设置为0表示不检查阈值，总是执行
     默认值: 10（即碎片率>=10%时才执行）
     示例: export ORCAS_CRON_DEFRAGMENT_THRESHOLD=20

使用示例：
   export ORCAS_BASE=/var/orcas/base
   export ORCAS_DATA=/var/orcas/data
   export ORCAS_DELETE_DELAY=10
   export ORCAS_BATCH_INTERVAL_MS=200
   export ORCAS_MIN_VERSION_INTERVAL_SEC=300
   export ORCAS_MAX_VERSIONS=10
*/

const (
	ROOT_OID    int64 = 0
	DELETED_PID int64 = -1 // 标记已删除的对象的父ID

	// DefaultListPageSize 默认列表分页大小
	// 用于 List 操作和分页处理，避免一次性加载大量数据
	DefaultListPageSize = 1000
)

// DeleteDelaySeconds 删除延迟时间（秒），等待指定时间后删除数据文件
// 可以通过环境变量 ORCAS_DELETE_DELAY 覆盖，默认5秒
var DeleteDelaySeconds = func() int64 {
	if delay := os.Getenv("ORCAS_DELETE_DELAY"); delay != "" {
		if d, err := strconv.ParseInt(delay, 10, 64); err == nil && d > 0 {
			return d
		}
	}
	return 5 // 默认5秒
}()

// ResourceControlConfig 资源控制配置
type ResourceControlConfig struct {
	// BatchInterval 批次间隔时间（毫秒），每批处理之间的延迟
	// 可通过环境变量 ORCAS_BATCH_INTERVAL_MS 配置，默认100ms
	BatchInterval time.Duration

	// MaxDuration 最大运行时长（秒），超过此时间后停止处理
	// 可通过环境变量 ORCAS_MAX_DURATION_SEC 配置，默认0表示不限制
	MaxDuration time.Duration

	// MaxItemsPerSecond 每秒最大处理项数，用于速率限制
	// 可通过环境变量 ORCAS_MAX_ITEMS_PER_SEC 配置，默认0表示不限制
	MaxItemsPerSecond int

	// AdaptiveDelay 是否启用自适应延迟，根据处理的数据量动态调整延迟
	// 可通过环境变量 ORCAS_ADAPTIVE_DELAY 配置（true/false），默认true
	AdaptiveDelay bool

	// AdaptiveDelayFactor 自适应延迟因子，延迟时间 = BatchInterval * (1 + 已处理项数 / 因子)
	// 可通过环境变量 ORCAS_ADAPTIVE_DELAY_FACTOR 配置，默认1000
	AdaptiveDelayFactor int64
}

// GetResourceControlConfig 获取资源控制配置
func GetResourceControlConfig() ResourceControlConfig {
	config := ResourceControlConfig{
		BatchInterval:       100 * time.Millisecond, // 默认100ms
		MaxDuration:         0,                      // 默认不限制
		MaxItemsPerSecond:   0,                      // 默认不限制
		AdaptiveDelay:       true,                   // 默认启用
		AdaptiveDelayFactor: 1000,                   // 默认因子1000
	}

	// 从环境变量读取配置
	if interval := os.Getenv("ORCAS_BATCH_INTERVAL_MS"); interval != "" {
		if d, err := strconv.ParseInt(interval, 10, 64); err == nil && d > 0 {
			config.BatchInterval = time.Duration(d) * time.Millisecond
		}
	}

	if maxDur := os.Getenv("ORCAS_MAX_DURATION_SEC"); maxDur != "" {
		if d, err := strconv.ParseInt(maxDur, 10, 64); err == nil && d > 0 {
			config.MaxDuration = time.Duration(d) * time.Second
		}
	}

	if maxItems := os.Getenv("ORCAS_MAX_ITEMS_PER_SEC"); maxItems != "" {
		if d, err := strconv.ParseInt(maxItems, 10, 64); err == nil && d > 0 {
			config.MaxItemsPerSecond = int(d)
		}
	}

	if adaptive := os.Getenv("ORCAS_ADAPTIVE_DELAY"); adaptive != "" {
		config.AdaptiveDelay = adaptive == "true" || adaptive == "1"
	}

	if factor := os.Getenv("ORCAS_ADAPTIVE_DELAY_FACTOR"); factor != "" {
		if d, err := strconv.ParseInt(factor, 10, 64); err == nil && d > 0 {
			config.AdaptiveDelayFactor = d
		}
	}

	return config
}

// VersionRetentionConfig 版本保留策略配置
type VersionRetentionConfig struct {
	// MinVersionInterval 最小版本间隔时间（秒），在指定时间内只允许创建一个版本
	// 可通过环境变量 ORCAS_MIN_VERSION_INTERVAL_SEC 配置，默认0表示不限制
	MinVersionInterval int64

	// MaxVersions 最大保留版本数，超过此数量的旧版本会被删除
	// 可通过环境变量 ORCAS_MAX_VERSIONS 配置，默认0表示不限制
	MaxVersions int64
}

// GetVersionRetentionConfig 获取版本保留策略配置
func GetVersionRetentionConfig() VersionRetentionConfig {
	config := VersionRetentionConfig{
		MinVersionInterval: 0, // 默认不限制
		MaxVersions:        0, // 默认不限制
	}

	// 从环境变量读取配置
	if interval := os.Getenv("ORCAS_MIN_VERSION_INTERVAL_SEC"); interval != "" {
		if d, err := strconv.ParseInt(interval, 10, 64); err == nil && d >= 0 {
			config.MinVersionInterval = d
		}
	}

	if maxVers := os.Getenv("ORCAS_MAX_VERSIONS"); maxVers != "" {
		if d, err := strconv.ParseInt(maxVers, 10, 64); err == nil && d >= 0 {
			config.MaxVersions = d
		}
	}

	return config
}

// CronJobConfig 定时任务配置
type CronJobConfig struct {
	// ScrubEnabled 是否启用ScrubData定时任务
	ScrubEnabled bool
	// ScrubSchedule ScrubData的cron表达式（格式：分钟 小时 天 月 星期）
	ScrubSchedule string

	// MergeEnabled 是否启用MergeDuplicateData定时任务
	MergeEnabled bool
	// MergeSchedule MergeDuplicateData的cron表达式
	MergeSchedule string

	// DefragmentEnabled 是否启用Defragment定时任务
	DefragmentEnabled bool
	// DefragmentSchedule Defragment的cron表达式
	DefragmentSchedule string
	// DefragmentMaxSize Defragment的最大文件大小（小于此大小的文件会被打包）
	DefragmentMaxSize int64
	// DefragmentAccessWindow Defragment的访问窗口时间（秒）
	DefragmentAccessWindow int64
	// DefragmentThreshold Defragment的空间占用阈值（百分比，0-100）
	// 只有当碎片率（(Used - RealUsed) / Used * 100）达到此阈值时才执行碎片整理
	DefragmentThreshold int64
}

// GetCronJobConfig 获取定时任务配置
func GetCronJobConfig() CronJobConfig {
	config := CronJobConfig{
		ScrubEnabled:           false,
		ScrubSchedule:          "0 2 * * *", // 每天凌晨2点
		MergeEnabled:           false,
		MergeSchedule:          "0 3 * * *", // 每天凌晨3点
		DefragmentEnabled:      false,
		DefragmentSchedule:     "0 4 * * 0",      // 每周日凌晨4点
		DefragmentMaxSize:      10 * 1024 * 1024, // 默认10MB
		DefragmentAccessWindow: 0,                // 默认不限制
		DefragmentThreshold:    10,               // 默认10%（碎片率>=10%时才执行）
	}

	// 从环境变量读取配置
	if scrub := os.Getenv("ORCAS_CRON_SCRUB_ENABLED"); scrub != "" {
		config.ScrubEnabled = scrub == "true" || scrub == "1"
	}
	if schedule := os.Getenv("ORCAS_CRON_SCRUB_SCHEDULE"); schedule != "" {
		config.ScrubSchedule = schedule
	}

	if merge := os.Getenv("ORCAS_CRON_MERGE_ENABLED"); merge != "" {
		config.MergeEnabled = merge == "true" || merge == "1"
	}
	if schedule := os.Getenv("ORCAS_CRON_MERGE_SCHEDULE"); schedule != "" {
		config.MergeSchedule = schedule
	}

	if defrag := os.Getenv("ORCAS_CRON_DEFRAGMENT_ENABLED"); defrag != "" {
		config.DefragmentEnabled = defrag == "true" || defrag == "1"
	}
	if schedule := os.Getenv("ORCAS_CRON_DEFRAGMENT_SCHEDULE"); schedule != "" {
		config.DefragmentSchedule = schedule
	}
	if maxSize := os.Getenv("ORCAS_CRON_DEFRAGMENT_MAX_SIZE"); maxSize != "" {
		if d, err := strconv.ParseInt(maxSize, 10, 64); err == nil && d > 0 {
			config.DefragmentMaxSize = d
		}
	}
	if window := os.Getenv("ORCAS_CRON_DEFRAGMENT_ACCESS_WINDOW"); window != "" {
		if d, err := strconv.ParseInt(window, 10, 64); err == nil && d >= 0 {
			config.DefragmentAccessWindow = d
		}
	}
	if threshold := os.Getenv("ORCAS_CRON_DEFRAGMENT_THRESHOLD"); threshold != "" {
		if d, err := strconv.ParseInt(threshold, 10, 64); err == nil && d >= 0 && d <= 100 {
			config.DefragmentThreshold = d
		}
	}

	return config
}

// WriteBufferConfig 随机写缓冲配置
type WriteBufferConfig struct {
	// MaxBufferSize 最大缓冲区大小（字节），超过此大小会立即触发写入
	// 可通过环境变量 ORCAS_MAX_WRITE_BUFFER_SIZE 配置，默认8MB
	MaxBufferSize int64

	// MaxBufferWrites 最大缓冲写入次数，超过此次数会立即触发写入
	// 可通过环境变量 ORCAS_MAX_WRITE_BUFFER_COUNT 配置，默认200
	MaxBufferWrites int64

	// BufferWindow 缓冲窗口时间（秒），在指定时间内的多次写入会被合并
	// 可通过环境变量 ORCAS_WRITE_BUFFER_WINDOW_SEC 配置，默认10秒
	BufferWindow time.Duration
}

// GetWriteBufferConfig 获取随机写缓冲配置
func GetWriteBufferConfig() WriteBufferConfig {
	config := WriteBufferConfig{
		MaxBufferSize:   8 * 1024 * 1024,  // 默认8MB
		MaxBufferWrites: 200,              // 默认200个操作
		BufferWindow:    10 * time.Second, // 默认10秒
	}

	// 从环境变量读取配置
	if maxSize := os.Getenv("ORCAS_MAX_WRITE_BUFFER_SIZE"); maxSize != "" {
		if d, err := strconv.ParseInt(maxSize, 10, 64); err == nil && d > 0 {
			config.MaxBufferSize = d
		}
	}

	if maxCount := os.Getenv("ORCAS_MAX_WRITE_BUFFER_COUNT"); maxCount != "" {
		if d, err := strconv.ParseInt(maxCount, 10, 64); err == nil && d > 0 {
			config.MaxBufferWrites = d
		}
	}

	if window := os.Getenv("ORCAS_WRITE_BUFFER_WINDOW_SEC"); window != "" {
		if d, err := strconv.ParseInt(window, 10, 64); err == nil && d > 0 {
			config.BufferWindow = time.Duration(d) * time.Second
		}
	}

	return config
}

var (
	ORCAS_BASE = os.Getenv("ORCAS_BASE")
	ORCAS_DATA = os.Getenv("ORCAS_DATA")
)

type Ctx context.Context

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ERR_AUTH_FAILED   = Error("auth failed")
	ERR_NEED_LOGIN    = Error("need login")
	ERR_INCORRECT_PWD = Error("incorrect username or password")

	ERR_NO_PERM = Error("no permission")
	ERR_NO_ROLE = Error("role mismatch")

	ERR_OPEN_FILE = Error("open file failed")
	ERR_READ_FILE = Error("read file failed")

	ERR_OPEN_DB      = Error("open db failed")
	ERR_QUERY_DB     = Error("query db failed")
	ERR_EXEC_DB      = Error("exec db failed")
	ERR_DUP_KEY      = Error("object with same name already exists")
	ERR_QUOTA_EXCEED = Error("quota exceeded")
)

var (
	// 时间校准器：使用自己的时间戳，减少time.Now()调用和GC压力
	// 参考ecache的实现方式，定期更新时间戳
	currentTimeStamp   atomic.Int64 // 当前Unix时间戳（秒）
	timeCalibratorOnce sync.Once    // 确保只初始化一次
)

// initTimeCalibrator 初始化时间校准器
// 定期更新时间戳（默认每毫秒更新一次），减少time.Now()调用
func initTimeCalibrator() {
	timeCalibratorOnce.Do(func() {
		// 初始化当前时间戳
		currentTimeStamp.Store(time.Now().Unix())

		// 启动时间校准协程，每毫秒更新一次时间戳
		// 这个精度对于大多数场景已经足够，同时不会造成太大开销
		go func() {
			ticker := time.NewTicker(1 * time.Millisecond)
			defer ticker.Stop()
			for {
				currentTimeStamp.Store(time.Now().Unix())
				<-ticker.C
			}
		}()
	})
}

// Now 获取当前Unix时间戳（秒）
// 使用时间校准器的时间戳，避免每次调用time.Now()创建临时对象
// 这个函数可以在整个项目中复用，减少GC压力
// 命名类似于time.Now()，但返回Unix时间戳（秒）而不是time.Time对象
func Now() int64 {
	// 确保时间校准器已初始化
	initTimeCalibrator()
	return currentTimeStamp.Load()
}
