package core

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

/*
Environment Variable Configuration List

The following are all environment variable configuration items supported by the ORCAS system:

1. Basic Path Configuration
   - BasePath: Base path for storing metadata and other basic files (string, defaults to current directory ".")
   - DataPath: Data path for storing data files (string, defaults to current directory ".")

2. Delete Delay Configuration
   - ORCAS_DELETE_DELAY: Delete delay time (seconds), wait for the specified time before deleting data files
     Default: 5
     Example: export ORCAS_DELETE_DELAY=10

3. Resource Control Configuration
   - ORCAS_BATCH_INTERVAL_MS: Batch interval time (milliseconds), delay between each batch processing
     Default: 100
     Example: export ORCAS_BATCH_INTERVAL_MS=200

   - ORCAS_MAX_DURATION_SEC: Maximum running duration (seconds), stop processing after this time
     Default: 0 (no limit)
     Example: export ORCAS_MAX_DURATION_SEC=3600

   - ORCAS_MAX_ITEMS_PER_SEC: Maximum items processed per second, for rate limiting
     Default: 0 (no limit)
     Example: export ORCAS_MAX_ITEMS_PER_SEC=1000

   - ORCAS_ADAPTIVE_DELAY: Whether to enable adaptive delay (true/false/1/0)
     Default: true
     Example: export ORCAS_ADAPTIVE_DELAY=true

   - ORCAS_ADAPTIVE_DELAY_FACTOR: Adaptive delay factor, delay time = BatchInterval * (1 + processed items / factor)
     Default: 1000
     Example: export ORCAS_ADAPTIVE_DELAY_FACTOR=2000

4. Version Retention Policy Configuration
   - ORCAS_MIN_VERSION_INTERVAL_SEC: Minimum version interval time (seconds), only allow creating one version within the specified time
     Default: 0 (no limit)
     Example: export ORCAS_MIN_VERSION_INTERVAL_SEC=300

   - ORCAS_MAX_VERSIONS: Maximum retained versions, old versions exceeding this number will be deleted
     Default: 0 (no limit)
     Example: export ORCAS_MAX_VERSIONS=10

5. Random Write Buffer Configuration
   - ORCAS_WRITE_BUFFER_WINDOW_SEC: Buffer window time (seconds), multiple writes within the specified time will be merged
     Default: 10
     Example: export ORCAS_WRITE_BUFFER_WINDOW_SEC=10

   - ORCAS_MAX_WRITE_BUFFER_SIZE: Maximum buffer size (bytes), exceeding this size will trigger immediate write
     Default: 8388608 (8MB)
     Example: export ORCAS_MAX_WRITE_BUFFER_SIZE=16777216

   - ORCAS_MAX_WRITE_BUFFER_COUNT: Maximum buffered write count, exceeding this count will trigger immediate write
     Default: 200
     Example: export ORCAS_MAX_WRITE_BUFFER_COUNT=300

   - ORCAS_BATCH_WRITE_ENABLED: Whether to enable batch write optimization (true/false/1/0)
     When disabled, files are written as individual objects directly
     Default: true
     Example: export ORCAS_BATCH_WRITE_ENABLED=false
   - ORCAS_MAX_BATCH_WRITE_FILE_SIZE: Maximum file size for batch write optimization (bytes)
     Files larger than this size will use direct write path
     Based on performance tests: optimal threshold is 64KB (default)
     Default: 65536 (64KB)
     Example: export ORCAS_MAX_BATCH_WRITE_FILE_SIZE=131072

6. Scheduled Task Configuration (Crontab)
   - ORCAS_CRON_SCRUB_ENABLED: Whether to enable ScrubData scheduled task (true/false/1/0)
     Default: false
     Example: export ORCAS_CRON_SCRUB_ENABLED=true

   - ORCAS_CRON_SCRUB_SCHEDULE: Cron expression for ScrubData (format: minute hour day month weekday)
     Default: "0 2 * * *" (daily at 2 AM)
     Example: export ORCAS_CRON_SCRUB_SCHEDULE="0 2 * * *"

   - ORCAS_CRON_MERGE_ENABLED: Whether to enable MergeDuplicateData scheduled task (true/false/1/0)
     Default: false
     Example: export ORCAS_CRON_MERGE_ENABLED=true

   - ORCAS_CRON_MERGE_SCHEDULE: Cron expression for MergeDuplicateData
     Default: "0 3 * * *" (daily at 3 AM)
     Example: export ORCAS_CRON_MERGE_SCHEDULE="0 3 * * *"

   - ORCAS_CRON_DEFRAGMENT_ENABLED: Whether to enable Defragment scheduled task (true/false/1/0)
     Default: false
     Example: export ORCAS_CRON_DEFRAGMENT_ENABLED=true

   - ORCAS_CRON_DEFRAGMENT_SCHEDULE: Cron expression for Defragment
     Default: "0 4 * * 0" (Sundays at 4 AM)
     Example: export ORCAS_CRON_DEFRAGMENT_SCHEDULE="0 4 * * 0"

   - ORCAS_CRON_DEFRAGMENT_MAX_SIZE: Maximum file size for Defragment (files smaller than this will be packed)
     Default: 10485760 (10MB)
     Example: export ORCAS_CRON_DEFRAGMENT_MAX_SIZE=20971520

   - ORCAS_CRON_DEFRAGMENT_ACCESS_WINDOW: Access window time for Defragment (seconds)
     Default: 0 (no limit)
     Example: export ORCAS_CRON_DEFRAGMENT_ACCESS_WINDOW=3600

   - ORCAS_CRON_DEFRAGMENT_THRESHOLD: Space usage threshold for Defragment (percentage, 0-100)
     Defragmentation will only be performed when fragmentation rate ((RealUsed - LogicalUsed) / RealUsed * 100) reaches this threshold
     LogicalUsed counts logical usage of all valid objects (not deleted)
     Set to 0 means no threshold check, always execute
     Default: 10 (execute when fragmentation rate >= 10%)
     Example: export ORCAS_CRON_DEFRAGMENT_THRESHOLD=20

Usage Example:
   // Paths default to current directory "." if not set
   // They can be set via Handler's SetPaths method or when creating Handler
   export ORCAS_DELETE_DELAY=10
   export ORCAS_BATCH_INTERVAL_MS=200
   export ORCAS_MIN_VERSION_INTERVAL_SEC=300
   export ORCAS_MAX_VERSIONS=10
*/

const (
	ROOT_OID    int64 = 0
	DELETED_PID int64 = -1 // Marks parent ID of deleted objects

	// DefaultListPageSize Default list page size
	// Used for List operations and pagination, to avoid loading large amounts of data at once
	DefaultListPageSize = 1000
)

// DeleteDelaySeconds Delete delay time (seconds), wait for the specified time before deleting data files
// Can be overridden via environment variable ORCAS_DELETE_DELAY, default is 5 seconds
var DeleteDelaySeconds = func() int64 {
	if delay := os.Getenv("ORCAS_DELETE_DELAY"); delay != "" {
		if d, err := strconv.ParseInt(delay, 10, 64); err == nil && d > 0 {
			return d
		}
	}
	return 5 // Default 5 seconds
}()

// ResourceControlConfig Resource control configuration
type ResourceControlConfig struct {
	// BatchInterval Batch interval time (milliseconds), delay between each batch processing
	// Configurable via environment variable ORCAS_BATCH_INTERVAL_MS, default 100ms
	BatchInterval time.Duration

	// MaxDuration Maximum running duration (seconds), stop processing after this time
	// Configurable via environment variable ORCAS_MAX_DURATION_SEC, default 0 means no limit
	MaxDuration time.Duration

	// MaxItemsPerSecond Maximum items processed per second, for rate limiting
	// Configurable via environment variable ORCAS_MAX_ITEMS_PER_SEC, default 0 means no limit
	MaxItemsPerSecond int

	// AdaptiveDelay Whether to enable adaptive delay, dynamically adjust delay based on processed data volume
	// Configurable via environment variable ORCAS_ADAPTIVE_DELAY (true/false), default true
	AdaptiveDelay bool

	// AdaptiveDelayFactor Adaptive delay factor, delay time = BatchInterval * (1 + processed items / factor)
	// Configurable via environment variable ORCAS_ADAPTIVE_DELAY_FACTOR, default 1000
	AdaptiveDelayFactor int64
}

// GetResourceControlConfig Get resource control configuration
func GetResourceControlConfig() ResourceControlConfig {
	config := ResourceControlConfig{
		BatchInterval:       100 * time.Millisecond, // Default 100ms
		MaxDuration:         0,                      // Default no limit
		MaxItemsPerSecond:   0,                      // Default no limit
		AdaptiveDelay:       true,                   // Default enabled
		AdaptiveDelayFactor: 1000,                   // Default factor 1000
	}

	// Read configuration from environment variables
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

// VersionRetentionConfig Version retention policy configuration
type VersionRetentionConfig struct {
	// MinVersionInterval Minimum version interval time (seconds), only allow creating one version within the specified time
	// Configurable via environment variable ORCAS_MIN_VERSION_INTERVAL_SEC, default 0 means no limit
	MinVersionInterval int64

	// MaxVersions Maximum retained versions, old versions exceeding this number will be deleted
	// Configurable via environment variable ORCAS_MAX_VERSIONS, default 0 means no limit
	MaxVersions int64
}

// GetVersionRetentionConfig Get version retention policy configuration
func GetVersionRetentionConfig() VersionRetentionConfig {
	config := VersionRetentionConfig{
		MinVersionInterval: 0, // Default no limit
		MaxVersions:        0, // Default no limit
	}

	// Read configuration from environment variables
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

// CronJobConfig Scheduled task configuration
type CronJobConfig struct {
	// ScrubEnabled Whether to enable ScrubData scheduled task
	ScrubEnabled bool
	// ScrubSchedule Cron expression for ScrubData (format: minute hour day month weekday)
	ScrubSchedule string

	// MergeEnabled Whether to enable MergeDuplicateData scheduled task
	MergeEnabled bool
	// MergeSchedule Cron expression for MergeDuplicateData
	MergeSchedule string

	// DefragmentEnabled Whether to enable Defragment scheduled task
	DefragmentEnabled bool
	// DefragmentSchedule Cron expression for Defragment
	DefragmentSchedule string
	// DefragmentMaxSize Maximum file size for Defragment (files smaller than this will be packed)
	DefragmentMaxSize int64
	// DefragmentAccessWindow Access window time for Defragment (seconds)
	DefragmentAccessWindow int64
	// DefragmentThreshold Space usage threshold for Defragment (percentage, 0-100)
	// Defragmentation will only be performed when fragmentation rate ((Used - RealUsed) / Used * 100) reaches this threshold
	DefragmentThreshold int64

	// ConvertWritingVersionsEnabled Whether to enable ConvertWritingVersions scheduled task
	ConvertWritingVersionsEnabled bool
	// ConvertWritingVersionsSchedule Cron expression for ConvertWritingVersions
	ConvertWritingVersionsSchedule string
}

// GetCronJobConfig Get scheduled task configuration
func GetCronJobConfig() CronJobConfig {
	config := CronJobConfig{
		ScrubEnabled:                   false,
		ScrubSchedule:                  "0 2 * * *", // Daily at 2 AM
		MergeEnabled:                   false,
		MergeSchedule:                  "0 3 * * *", // Daily at 3 AM
		DefragmentEnabled:              false,
		DefragmentSchedule:             "0 4 * * 0",      // Sundays at 4 AM
		DefragmentMaxSize:              10 * 1024 * 1024, // Default 10MB
		DefragmentAccessWindow:         0,                // Default no limit
		DefragmentThreshold:            10,               // Default 10% (execute when fragmentation rate >= 10%)
		ConvertWritingVersionsEnabled:  false,
		ConvertWritingVersionsSchedule: "0 1 * * *", // Daily at 1 AM
	}

	// Read configuration from environment variables
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

	if convert := os.Getenv("ORCAS_CRON_CONVERT_WRITING_VERSIONS_ENABLED"); convert != "" {
		config.ConvertWritingVersionsEnabled = convert == "true" || convert == "1"
	}
	if schedule := os.Getenv("ORCAS_CRON_CONVERT_WRITING_VERSIONS_SCHEDULE"); schedule != "" {
		config.ConvertWritingVersionsSchedule = schedule
	}

	return config
}

// WriteBufferConfig Random write buffer configuration
type WriteBufferConfig struct {
	// MaxBufferSize Maximum buffer size (bytes), exceeding this size will trigger immediate write
	// Configurable via environment variable ORCAS_MAX_WRITE_BUFFER_SIZE, default 8MB
	MaxBufferSize int64

	// MaxBufferWrites Maximum buffered write count, exceeding this count will trigger immediate write
	// Configurable via environment variable ORCAS_MAX_WRITE_BUFFER_COUNT, default 2048
	MaxBufferWrites int64

	// BufferWindow Buffer window time (seconds), multiple writes within the specified time will be merged
	// Configurable via environment variable ORCAS_WRITE_BUFFER_WINDOW_SEC, default 10 seconds
	BufferWindow time.Duration

	// BatchWriteEnabled Whether to enable batch write optimization
	// When disabled, files are written as individual objects directly
	// Configurable via environment variable ORCAS_BATCH_WRITE_ENABLED, default true
	BatchWriteEnabled bool

	// MaxBatchWriteFileSize Maximum file size for batch write optimization (bytes)
	// Files larger than this size will use direct write path
	// Based on performance tests: 1KB files benefit 155%, 1MB files degrade 9.6-65.5%
	// Optimal threshold: 64KB-128KB for best performance
	// Configurable via environment variable ORCAS_MAX_BATCH_WRITE_FILE_SIZE, default 64KB
	MaxBatchWriteFileSize int64
}

// GetWriteBufferConfig Get random write buffer configuration
func GetWriteBufferConfig() WriteBufferConfig {
	config := WriteBufferConfig{
		MaxBufferSize:         8 * 1024 * 1024,  // Default 8MB
		MaxBufferWrites:       2048,             // Default 2048 files
		BufferWindow:          10 * time.Second, // Default 10 seconds
		BatchWriteEnabled:     true,             // Default enabled
		MaxBatchWriteFileSize: 64 * 1024,        // Default 64KB (optimal based on performance tests)
	}

	// Read configuration from environment variables
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

	if enabled := os.Getenv("ORCAS_BATCH_WRITE_ENABLED"); enabled != "" {
		config.BatchWriteEnabled = enabled == "true" || enabled == "1"
	}

	if maxFileSize := os.Getenv("ORCAS_MAX_BATCH_WRITE_FILE_SIZE"); maxFileSize != "" {
		if d, err := strconv.ParseInt(maxFileSize, 10, 64); err == nil && d > 0 {
			config.MaxBatchWriteFileSize = d
		}
	}

	return config
}

// Instant upload level setting
const (
	REF_LEVEL_OFF  = iota // OFF
	REF_LEVEL_FULL        // Read entire file
	REF_LEVEL_FAST        // Read entire file after header check succeeds
)

// Conflict resolution for same name
const (
	CONFLICT_COVER  = iota // Merge or overwrite
	CONFLICT_RENAME        // Rename
	CONFLICT_THROW         // Throw error
	CONFLICT_SKIP          // Skip
)

// Config represents business layer configuration (not stored in database)
// These fields are handled at business layer (cmd/vfs), not in bucket config
type Config struct {
	UserName string // Username
	Password string // Password
	NoAuth   bool   // If true, bypass authentication and permission checks (no user required)
	BasePath string // Base path for metadata (database storage location), if empty uses current directory "."
	DataPath string // Data path for file data storage location, if empty uses current directory "."
	RefLevel uint32 // Instant upload level setting: REF_LEVEL_OFF (default) / REF_LEVEL_FULL: Ref / REF_LEVEL_FAST: TryRef+Ref
	PkgThres uint32 // Package count limit, default 1000 if not set
	CmprWay  uint32 // Compression method (smart compression by default, decides whether to compress based on file type), see DATA_CMPR_MASK
	CmprQlty uint32 // Compression level, br:[0,11], gzip:[-3,9], zstd:[0,10]
	EndecWay uint32 // Encryption method, see DATA_ENDEC_MASK
	EndecKey string // Encryption KEY, SM4 requires exactly 16 characters, AES256 requires more than 16 characters
	DontSync string // Filename wildcards to exclude from sync (https://pkg.go.dev/path/filepath#Match), separated by semicolons
	Conflict uint32 // Conflict resolution for same name, CONFLICT_COVER: merge or overwrite / CONFLICT_RENAME: rename / CONFLICT_THROW: throw error / CONFLICT_SKIP: skip
	NameTmpl string // Rename suffix, "%s的副本", should contain "%s"
	WorkersN uint32 // Concurrent pool size, not less than 16
	// ChkPtDir string // Checkpoint directory for resume, not enabled if path not set
}

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

// Time calibrator: uses custom timestamp to reduce time.Now() calls and GC pressure
// Inspired by ecache's implementation, periodically update timestamp
var (
	clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)
)

func now() int64 { return atomic.LoadInt64(&clock) }
func init() {
	go func() { // internal counter that reduce GC caused by `time.Now()`
		for {
			atomic.StoreInt64(&clock, time.Now().UnixNano()) // calibration every second
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock, int64(100*time.Millisecond))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// Now Get current Unix timestamp (seconds)
// Uses time calibrator's timestamp to avoid creating temporary objects with each time.Now() call
// This function can be reused throughout the project to reduce GC pressure
// Named similar to time.Now(), but returns Unix timestamp (seconds) instead of time.Time object
func Now() int64 {
	return atomic.LoadInt64(&clock) / 1e9 // Convert nanoseconds to seconds
}

// InstantUploadConfig stores instant upload (deduplication) configuration
type InstantUploadConfig struct {
	RefLevel uint32 // Instant upload level: 0=OFF, 1=FULL, 2=FAST
}

// GetBucketInstantUploadConfig extracts instant upload config from bucket info
// Returns nil if bucket is nil or config is not set
// Note: RefLevel is no longer stored in bucket config, should be provided via core.Config in business layer
func GetBucketInstantUploadConfig(bucket *BucketInfo) *InstantUploadConfig {
	if bucket == nil {
		return nil
	}
	// Note: RefLevel is no longer stored in bucket config
	// Always fallback to environment variable
	return &InstantUploadConfig{
		RefLevel: getInstantUploadRefLevel(),
	}
}

// IsInstantUploadEnabled checks if instant upload is enabled via environment variable
func IsInstantUploadEnabled() bool {
	enabled := os.Getenv("ORCAS_INSTANT_UPLOAD_ENABLED")
	return enabled == "true" || enabled == "1"
}

// IsInstantUploadEnabledWithConfig checks if instant upload is enabled with given config
func IsInstantUploadEnabledWithConfig(cfg *InstantUploadConfig) bool {
	if cfg == nil {
		return IsInstantUploadEnabled()
	}
	return cfg.RefLevel > 0
}

// getInstantUploadRefLevel gets instant upload ref level from environment variable
// Returns 0 (OFF), 1 (FULL), or 2 (FAST)
func getInstantUploadRefLevel() uint32 {
	refLevel := os.Getenv("ORCAS_INSTANT_UPLOAD_REF_LEVEL")
	if refLevel == "" {
		if IsInstantUploadEnabled() {
			return 1 // FULL by default if enabled
		}
		return 0 // OFF by default
	}
	level, err := strconv.ParseUint(refLevel, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(level)
}
