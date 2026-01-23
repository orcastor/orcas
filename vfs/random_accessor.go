package vfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/h2non/filetype"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/orca-zhang/ecache2"
	"github.com/orcastor/orcas/core"
	"github.com/tjfoc/gmsm/sm4"
	"github.com/zeebo/xxh3"
	"golang.org/x/sync/singleflight"
)

const (
	// File size thresholds
	SmallFileThreshold = 10 << 20  // 10MB - threshold for small vs large file strategies
	DefaultChunkSize   = 10 << 20  // 10MB - default chunk size for streaming operations
	LargeFileThreshold = 100 << 20 // 100MB - threshold for very large file optimization

	// Buffer sizes
	SmallChunkPoolCap   = 64 << 10 // 64KB - capacity for small file chunk pool
	LargeChunkPoolCap   = 4 << 20  // 4MB - capacity for large file chunk pool
	DefaultChunkPoolCap = 10 << 20 // 10MB - default chunk buffer pool capacity
	ZeroSliceSize       = 64 << 10 // 64KB - zero slice size for efficient clearing
	// Sequential buffer tiered pool configuration
	// Three tiers: 128KB (small), 1MB (medium), 10MB (large)
	// Larger buffers are more expensive, so we keep fewer of them
	SequentialBufferTier1Cap = 128 << 10 // 128KB - tier 1: small files, keep more buffers
	SequentialBufferTier2Cap = 1 << 20   // 1MB - tier 2: medium files, keep moderate buffers
	SequentialBufferTier3Cap = 10 << 20  // 10MB - tier 3: large files, keep fewer buffers

	// Maximum number of buffers to keep in each pool tier
	// Larger buffers consume more memory, so we limit them more strictly
	SequentialBufferTier1Max = 100 // 128KB buffers: ~12.8MB total
	SequentialBufferTier2Max = 20  // 1MB buffers: ~20MB total
	SequentialBufferTier3Max = 5   // 10MB buffers: ~50MB total

	// Buffer operation limits
	DefaultMaxBufferOps  = 10000     // Maximum number of write operations in buffer
	DefaultMaxBufferSize = 100 << 20 // 100MB - maximum total buffer size

	// Write pattern detection
	SequentialWriteWindow = 4 << 10 // 4KB - window for detecting sequential writes

	// Sparse file local sequential write detection
	// If writes are within N chunks, treat as sequential even if out of order
	LocalSequentialChunkCount = 2 // 1-2 chunks: treat as sequential write
)

// WriterType represents the type of chunked file writer
type WriterType int

const (
	WRITER_TYPE_TMP    WriterType = iota // .tmp file: completes on rename (removing .tmp suffix)
	WRITER_TYPE_SPARSE                   // Sparse file: completes on file close
	WRITER_TYPE_SEQ                      // New sequential upload (non-.tmp), flushes on normal Flush/Close
)

var (
	// Performance tuning
	ChunkedCOWThreshold = 0.1 // 10% - modification ratio threshold for chunked COW

	// Pool array sizes
	DefaultWriteOpsPoolCap = 32 // Capacity for write operations slice pool
	DefaultIntSlicePoolCap = 32 // Capacity for int slice pool

	// Cache configuration
	FileCacheSize        = 512              // Number of file objects to cache
	DataCacheSize        = 512              // Number of data info objects to cache
	DirCacheSize         = 512              // Number of directory listings to cache
	ReaderCacheSize      = 64               // Number of decoding readers to cache
	DecodedFileCacheSize = 16               // Number of decoded packaged file blobs to cache (memory-heavy)
	StatfsCacheSize      = 16               // Number of statfs results to cache
	CacheShardCount      = 16               // Number of cache shards for concurrency
	CacheTTL             = 30 * time.Second // Cache time-to-live for file/data cache
	ReaderCacheTTL       = 30 * time.Second // Cache TTL for decoding readers
	StatfsCacheTTL       = 5 * time.Second  // Cache TTL for statfs results
	ChunkCacheTTL        = 10 * time.Second // Cache TTL for chunks
	ChunkCacheSize       = 8                // Number of chunks to cache per reader (reduced from hardcoded 64 to save memory)
	GlobalChunkCacheSize = 32               // Total number of chunks to cache globally (approx 320MB at 10MB/chunk)
	// Increased from 16 to 32 to reduce repeated AES decryption and temporary buffer allocations
)

var (
	// Object pool: reuse write operation slices
	writeOpsPool = sync.Pool{
		New: func() interface{} {
			return make([]WriteOperation, 0, DefaultWriteOpsPoolCap)
		},
	}

	// Object pool for int slices (used for remainingChunks, etc.)
	intSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]int, 0, DefaultIntSlicePoolCap)
		},
	}

	// Tiered buffer pool with capacity limits
	// Three tiers: 128KB, 1MB, 10MB with decreasing pool sizes
	bufferTier1Pool = &limitedPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, SequentialBufferTier1Cap)
			},
		},
		maxCount: SequentialBufferTier1Max,
		count:    0,
		mu:       sync.Mutex{},
	}

	bufferTier2Pool = &limitedPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, SequentialBufferTier2Cap)
			},
		},
		maxCount: SequentialBufferTier2Max,
		count:    0,
		mu:       sync.Mutex{},
	}

	bufferTier3Pool = &limitedPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, SequentialBufferTier3Cap)
			},
		},
		maxCount: SequentialBufferTier3Max,
		count:    0,
		mu:       sync.Mutex{},
	}

	// Object pool for SequentialWriteBuffer (without buffer, buffer comes from tiered pools)
	sequentialBufferPool = sync.Pool{
		New: func() interface{} {
			return &SequentialWriteBuffer{}
		},
	}

	// Zero slice for efficient zero-filling (reused to avoid allocations)
	zeroSlice = make([]byte, ZeroSliceSize) // 64KB zero slice

	// clearedChunkedWriterMarker is a special marker to indicate that ChunkedFileWriter has been cleared
	// atomic.Value cannot store nil, so we use this marker instead
	clearedChunkedWriterMarker = &ChunkedFileWriter{}
)

// limitedPool is a sync.Pool wrapper with capacity limit
type limitedPool struct {
	pool     sync.Pool
	maxCount int
	count    int
	mu       sync.Mutex
}

// Get gets an object from the pool if available and under capacity limit
func (p *limitedPool) Get() interface{} {
	p.mu.Lock()
	if p.count > 0 {
		p.count--
		p.mu.Unlock()
		return p.pool.Get()
	}
	p.mu.Unlock()
	return nil
}

// Put puts an object back to the pool if under capacity limit
func (p *limitedPool) Put(x interface{}) {
	if x == nil {
		return
	}
	p.mu.Lock()
	if p.count < p.maxCount {
		p.count++
		p.mu.Unlock()
		p.pool.Put(x)
	} else {
		p.mu.Unlock()
		// Pool is full, discard the object (let GC handle it)
	}
}

func allocChunkData(size int) []byte {
	if size <= 0 {
		return nil
	}
	return make([]byte, size)
}

func allocChunkBuffer(size int64) *chunkBuffer {
	if size <= 0 {
		return &chunkBuffer{data: nil}
	}
	return &chunkBuffer{
		data:          make([]byte, size),
		offsetInChunk: 0,
		ranges:        nil,
	}
}

var (
	// ecache cache: cache DataInfo to reduce database queries
	// key: dataID (int64), value: *core.DataInfo (dataID is globally unique)
	dataInfoCache = ecache2.NewLRUCache[int64](uint16(CacheShardCount), uint16(DataCacheSize), CacheTTL)

	// ecache cache: cache file object information to reduce database queries
	// key: fileID (int64), value: *core.ObjectInfo (fileID is globally unique)
	fileObjCache = ecache2.NewLRUCache[int64](uint16(CacheShardCount), uint16(FileCacheSize), CacheTTL)

	// ecache cache: cache directory listing to reduce database queries
	// key: dirID (int64), value: []*core.ObjectInfo (dirID is globally unique)
	dirListCache = ecache2.NewLRUCache[int64](uint16(CacheShardCount), uint16(DirCacheSize), CacheTTL)

	// ecache cache: cache Readdir entries (DirStream) to avoid rebuilding entries every time
	// key: dirID (int64), value: *cachedDirStream (dirID is globally unique)
	// This cache stores the final DirStream entries, avoiding data merging on every Readdir call
	readdirCache = ecache2.NewLRUCache[int64](uint16(CacheShardCount), uint16(DirCacheSize), CacheTTL)

	// Per-directory mutexes for thread-safe directory listing cache operations
	// key: dirID (int64), value: *sync.RWMutex
	// Used to synchronize cache reads and writes for directory listings
	dirListCacheMu sync.Map

	// Map to track directories that need delayed cache refresh
	// key: "<dirID>", value: true (if true, cache needs refresh on next access)
	// This allows marking cache as stale without immediately deleting it
	readdirCacheStale sync.Map // map[int64]bool

	// ecache cache: cache chunkReader by dataID (one reader per file)
	// key: dataID (int64), value: *chunkReader
	// This ensures one file uses the same reader, sharing chunk cache
	decodingReaderCache = ecache2.NewLRUCache[int64](uint16(CacheShardCount/4), uint16(ReaderCacheSize), ReaderCacheTTL)

	// ecache cache: cache Statfs results to reduce filesystem queries
	// key: bktID (int64), value: *fuse.StatfsOut
	// TTL is short (5 seconds) but will be refreshed on access (LRU behavior)
	statfsCache = ecache2.NewLRUCache[int64](uint16(CacheShardCount/4), uint16(StatfsCacheSize), StatfsCacheTTL)

	// ecache cache: global cache for chunks (plain or decompressed/decrypted)
	// key: [2]int64{dataID, sn}, value: []byte
	// Global limit controls total memory usage for cached chunks
	globalChunkCache = ecache2.NewLRUCache[[2]int64](uint16(CacheShardCount), uint16(GlobalChunkCacheSize), ChunkCacheTTL)

	// ecache cache: global cache for decoded file data (for packaged files)
	// key: dataID (int64), value: []byte
	globalDecodedFileCache = ecache2.NewLRUCache[int64](uint16(CacheShardCount/4), uint16(DecodedFileCacheSize), ChunkCacheTTL)

	// singleflight group: prevent duplicate concurrent operations globally.
	//
	// Key prefixes MUST remain unique per operation type to avoid cross-talk:
	// - dir list: "<dirID>"
	// - chunk read: "chunk_<dataID>_<sn>"
	// - chunk flush: "flush_<dataID>_<sn>"
	// - package decode: "decode_pkg_<dataID>"
	// - seq datainfo update: "update_datainfo_<dataID>"
	globalSingleFlight singleflight.Group

	tempFlushMgr     *delayedFlushManager
	tempFlushMgrOnce sync.Once

	// Periodic sync manager: periodically sync filesystem to ensure data persistence
	periodicSyncMgr     *periodicSyncManager
	periodicSyncMgrOnce sync.Once
)

var (
	// ORCAS_CACHE_EVICT_TO_POOL=0 disables returning evicted cached []byte to tier pools.
	// Default is ON.
	cacheEvictToPool atomic.Bool
)

func init() {
	// Default ON; allow emergency disable.
	cacheEvictToPool.Store(true)
	if os.Getenv("ORCAS_CACHE_EVICT_TO_POOL") == "0" {
		cacheEvictToPool.Store(false)
	}

	// Attach low-overhead probes to memory-heavy caches.
	// NOTE: ecache2 signals eviction by calling inspector with action=PUT, status=-1,
	// passing the evicted item's key/value.
	globalChunkCache.Inspect(func(action int, _ [2]int64, iface *interface{}, _ []byte, status int) {
		if !cacheEvictToPool.Load() {
			return
		}
		if action != ecache2.PUT || status != -1 || iface == nil || *iface == nil {
			return
		}
		if b, ok := (*iface).([]byte); ok && len(b) > 0 {
			putBufferToTier(b)
		}
	})
	globalDecodedFileCache.Inspect(func(action int, _ int64, iface *interface{}, _ []byte, status int) {
		if !cacheEvictToPool.Load() {
			return
		}
		if action != ecache2.PUT || status != -1 || iface == nil || *iface == nil {
			return
		}
		if b, ok := (*iface).([]byte); ok && len(b) > 0 {
			putBufferToTier(b)
		}
	})
}

// isRandomWritePattern detects if the write pattern is random (non-sequential)
func (ra *RandomAccessor) isRandomWritePattern() bool {
	if ra.buffer == nil {
		return false
	}
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	if writeIndex < 2 {
		return false
	}

	// Sample first few writes to detect pattern
	var lastEnd int64 = -1
	sampleCount := int64(5)
	if writeIndex < sampleCount {
		sampleCount = writeIndex
	}

	for i := int64(0); i < sampleCount; i++ {
		op := ra.buffer.operations[i]
		if len(op.Data) > 0 {
			if lastEnd >= 0 && op.Offset != lastEnd {
				// Non-contiguous writes detected
				return true
			}
			lastEnd = op.Offset + int64(len(op.Data))
		}
	}

	return false
}

const (
	tmpFileFlushInterval      = 5 * time.Minute
	delayedFlushCheckInterval = time.Second
	// PeriodicSyncInterval is the interval for periodic filesystem sync
	// This ensures data is persisted to disk even if Fsync is not called explicitly
	// Set to 1 second to ensure data is persisted quickly, especially for small files
	PeriodicSyncInterval = 1 * time.Second
)

type delayedFlushEntry struct {
	ra    *RandomAccessor
	force bool
}

type delayedFlushManager struct {
	mu      sync.Mutex
	entries map[int64]*delayedFlushEntry
	wakeCh  chan struct{}
}

func newDelayedFlushManager() *delayedFlushManager {
	mgr := &delayedFlushManager{
		entries: make(map[int64]*delayedFlushEntry),
		wakeCh:  make(chan struct{}, 1),
	}
	go mgr.run()
	return mgr
}

func getDelayedFlushManager() *delayedFlushManager {
	tempFlushMgrOnce.Do(func() {
		tempFlushMgr = newDelayedFlushManager()
	})
	return tempFlushMgr
}

func (m *delayedFlushManager) run() {
	ticker := time.NewTicker(delayedFlushCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.process()
		case <-m.wakeCh:
			m.process()
		}
	}
}

func (m *delayedFlushManager) process() {
	var toFlush []*delayedFlushEntry

	// For .tmp files, reset delayed flush timer on each write (5 minutes from last write)
	// Note: This is handled by ChunkedFileWriter for .tmp files, so we don't need to do it here
	// (The .tmp file check happens earlier in the function, so we skip this section for .tmp files)

	// Optimization: for sparse files and sequential writes, use longer delayed flush window to batch more writes
	// This reduces flush frequency and improves performance for random writes
	config := core.GetWriteBufferConfig()

	m.mu.Lock()
	for fileID, entry := range m.entries {
		if entry == nil || entry.ra == nil {
			delete(m.entries, fileID)
			continue
		}
		last := atomic.LoadInt64(&entry.ra.lastActivity)
		if last == 0 {
			continue
		}
		if core.Now()-last > int64(config.BufferWindow) {
			toFlush = append(toFlush, entry)
			delete(m.entries, fileID)
		}
	}
	m.mu.Unlock()

	for _, entry := range toFlush {
		entry.ra.executeDelayedFlush(entry.force)
	}
}

func (m *delayedFlushManager) schedule(ra *RandomAccessor, force bool) {
	if ra == nil {
		return
	}
	m.mu.Lock()
	if entry, ok := m.entries[ra.fileID]; ok {
		entry.force = force
	} else {
		m.entries[ra.fileID] = &delayedFlushEntry{
			ra:    ra,
			force: force,
		}
	}
	m.mu.Unlock()

	select {
	case m.wakeCh <- struct{}{}:
	default:
	}
}

func (m *delayedFlushManager) cancel(ra *RandomAccessor) {
	if ra == nil {
		return
	}
	m.mu.Lock()
	delete(m.entries, ra.fileID)
	m.mu.Unlock()
}

// periodicSyncManager periodically syncs filesystem to ensure data persistence
// This is critical for small files that may not have explicit Fsync calls
type periodicSyncManager struct {
	mu       sync.Mutex
	lastSync int64 // Unix timestamp of last sync
	stopCh   chan struct{}
}

func newPeriodicSyncManager() *periodicSyncManager {
	mgr := &periodicSyncManager{
		lastSync: 0,
		stopCh:   make(chan struct{}),
	}
	go mgr.run()
	return mgr
}

func getPeriodicSyncManager() *periodicSyncManager {
	periodicSyncMgrOnce.Do(func() {
		periodicSyncMgr = newPeriodicSyncManager()
	})
	return periodicSyncMgr
}

func (m *periodicSyncManager) run() {
	ticker := time.NewTicker(PeriodicSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.sync()
		case <-m.stopCh:
			return
		}
	}
}

func (m *periodicSyncManager) sync() {
	m.mu.Lock()
	now := core.Now()
	// Only sync if enough time has passed since last sync
	if now-m.lastSync < int64(PeriodicSyncInterval.Seconds()) {
		m.mu.Unlock()
		return
	}
	m.lastSync = now
	m.mu.Unlock()

	// Call system-level sync to ensure all data is persisted to disk
	// This is critical for small files that may not have explicit Fsync calls
	// Note: syscall.Sync() syncs the entire filesystem, which may be slow
	// But it's necessary to ensure data persistence, especially for removable media
	// We do this periodically (every 1 second) to ensure data is persisted quickly
	// Note: syscall.Sync() has no return value, so we can't check for errors
	syscall.Sync()
	DebugLog("[VFS PeriodicSync] Synced filesystem")
}

func (m *periodicSyncManager) stop() {
	close(m.stopCh)
}

// WriteOperation represents a single write operation
type WriteOperation struct {
	Offset int64  // Write offset
	Data   []byte // Write data
}

// PendingWrite represents a pending write operation waiting to be ordered
type PendingWrite struct {
	Offset int64
	Data   []byte
	Done   chan error // Channel to signal completion
}

// ConcurrentSequentialDetector detects concurrent sequential write patterns
// Used to optimize 6-thread concurrent writes into sequential writes
type ConcurrentSequentialDetector struct {
	mu               sync.Mutex
	pendingWrites    map[int64]*PendingWrite // Map offset -> pending write
	expectedOffset   int64                   // Next expected offset
	blockSize        int64                   // Detected block size (0 if not detected)
	consecutiveCount int                     // Count of consecutive blocks detected
	enabled          bool                    // Whether concurrent sequential mode is enabled
}

// WriteBuffer manages write buffer for a single file
type WriteBuffer struct {
	fileID     int64            // File object ID
	operations []WriteOperation // Fixed-length write operation array (avoid temporary object creation)
	writeIndex int64            // Current write position (using atomic operation, starts from 0)
	totalSize  int64            // Total buffer size (optimized with atomic operation)
}

// SequentialWriteBuffer sequential write buffer (optimized: sequential writes starting from 0)
type SequentialWriteBuffer struct {
	mu         sync.Mutex // Mutex for thread-safe writes and offset modifications
	fileID     int64      // File object ID
	dataID     int64      // Data object ID (created when creating new object)
	sn         int        // Current data block sequence number
	chunkSize  int64      // Chunk size
	buffer     []byte     // Current chunk buffer (at most one chunk size)
	offset     int64      // Current write position (sequential write)
	hasData    bool       // Whether data has been written
	closed     bool       // Whether closed (becomes random write)
	dataInfo   *core.DataInfo
	xxh3Hash   *xxh3.Hasher // XXH3 hasher for original data
	sha256Hash hash.Hash    // SHA-256 hasher for original data
}

// ChunkedFileWriter handles efficient fragmented writes for files that need chunked buffering
// Supports both .tmp files and sparse files
// Uses in-memory buffers to accumulate data until a complete chunk is ready
// Supports optional real-time compression/encryption (can be disabled for offline processing)
// Supports concurrent writes to different chunks
// ChunkedFileWriter writes data chunks synchronously using fixed 10MB chunk size
// Completion timing:
//   - WRITER_TYPE_TMP: Completes when file is renamed (removing .tmp suffix)
//   - WRITER_TYPE_SPARSE: Completes when file is closed
type ChunkedFileWriter struct {
	fs              *OrcasFS
	fileID          int64
	dataID          int64                // Data ID for this file
	fileName        string               // File name (for smart compression suffix check)
	chunkSize       int64                // Chunk size for this file (always 10MB)
	size            int64                // Total file size (atomic for concurrent access)
	mu              sync.Mutex           // Mutex for thread-safe operations on chunks map
	chunks          map[int]*chunkBuffer // Chunk buffers for each chunk (key: sn)
	dataInfo        *core.DataInfo       // DataInfo for tracking compression/encryption
	enableRealtime  bool                 // Whether to enable real-time compression/encryption (default: false for offline processing)
	realtimeDecided bool                 // Whether realtime capability has been determined
	lh              *core.LocalHandler   // Cached LocalHandler (nil if not LocalHandler)
	firstChunkSN    int64                // First chunk sequence number (sn=0)
	flushing        int32                // Atomic flag: 1 if currently flushing, 0 otherwise (prevents writes during flush)
	writerType      WriterType           // Type of writer: .tmp file or sparse file
	ra              *RandomAccessor      // Reference to parent RandomAccessor for cache updates
}

// writeRange represents a contiguous range of written data within a chunk
type writeRange struct {
	start int64 // Start offset (inclusive)
	end   int64 // End offset (exclusive)
}

// chunkBuffer holds data for a single chunk before compression/encryption and upload
type chunkBuffer struct {
	data          []byte       // Buffer data (pre-allocated to chunkSize)
	offsetInChunk int64        // Current write position within this chunk (0 to chunkSize)
	ranges        []writeRange // Written ranges within this chunk (sorted by start, non-overlapping)
	mu            sync.Mutex   // Mutex for this specific chunk buffer
}

// addWriteRange adds a new write range and merges adjacent ranges
// Ranges are kept sorted by start offset and non-overlapping
func (buf *chunkBuffer) addWriteRange(start, end int64) {
	if start >= end {
		return // Invalid range
	}

	// Find insertion position (ranges are sorted by start)
	insertPos := len(buf.ranges)
	for i, r := range buf.ranges {
		if r.start > start {
			insertPos = i
			break
		}
	}

	// Insert new range
	newRange := writeRange{start: start, end: end}
	if insertPos == len(buf.ranges) {
		buf.ranges = append(buf.ranges, newRange)
	} else {
		// Insert at position
		buf.ranges = append(buf.ranges, writeRange{})
		copy(buf.ranges[insertPos+1:], buf.ranges[insertPos:])
		buf.ranges[insertPos] = newRange
	}

	// Merge adjacent or overlapping ranges
	merged := buf.ranges[:0]
	for _, r := range buf.ranges {
		if len(merged) == 0 {
			merged = append(merged, r)
		} else {
			last := &merged[len(merged)-1]
			// If current range connects with or overlaps last range, merge them
			if r.start <= last.end {
				if r.end > last.end {
					last.end = r.end
				}
			} else {
				merged = append(merged, r)
			}
		}
	}
	buf.ranges = merged
}

// isChunkComplete checks if chunk is complete (has exactly one range covering entire chunk)
// IMPORTANT: Also check offsetInChunk to ensure all data has been written
// This prevents premature flushing when ranges show chunk is complete but offsetInChunk hasn't reached chunkSize
func (buf *chunkBuffer) isChunkComplete(chunkSize int64) bool {
	// Must have exactly one range covering entire chunk
	if len(buf.ranges) != 1 || buf.ranges[0].start != 0 || buf.ranges[0].end != chunkSize {
		return false
	}
	// IMPORTANT: Also check offsetInChunk to ensure all data has been written
	// This is critical for preventing data loss when ranges are merged but data hasn't been fully written
	// offsetInChunk should be >= chunkSize for a complete chunk
	if buf.offsetInChunk < chunkSize {
		return false
	}
	return true
}

// RandomAccessor random access object in VFS, supports compression and encryption
type RandomAccessor struct {
	fs            *OrcasFS
	fileID        int64
	buffer        *WriteBuffer           // Random write buffer
	seqBuffer     *SequentialWriteBuffer // Sequential write buffer (optimized)
	fileObj       atomic.Value
	fileObjKey    int64        // Pre-computed file_obj cache key (optimized: avoid repeated conversion)
	lastActivity  int64        // Last activity timestamp (atomic access)
	sparseSize    int64        // Sparse file size (for pre-allocated files, e.g., qBittorrent) (atomic access)
	lastOffset    int64        // Last write offset (for sequential write detection) (atomic access)
	chunkedWriter atomic.Value // ChunkedFileWriter for .tmp files and sparse files (atomic.Value stores *ChunkedFileWriter)
	tempWriteMu   sync.Mutex   // Mutex for temp operations
	isTmpFile     bool         // Whether this file is a .tmp file (determined at creation time, immutable)
	dataInfo      atomic.Value // Cached DataInfo (atomic.Value stores *core.DataInfo)
	journal       *Journal     // Journal for tracking random writes (protected by JournalManager)
	journalMu     sync.RWMutex // Mutex for journal access
	flushMu       sync.Mutex   // Mutex for flush operations (prevents concurrent flushes)
	// Local sequential write tracking for sparse files
	writeRangeStart int64 // Start of current write range (atomic access)
	writeRangeEnd   int64 // End of current write range (atomic access)
}

func isTempFile(obj *core.ObjectInfo) bool {
	if obj == nil {
		return false
	}
	name := strings.ToLower(obj.Name)
	return strings.HasSuffix(name, ".tmp")
}

// getOrCreateChunkedWriter gets or creates ChunkedFileWriter for .tmp files or sparse files
// Uses atomic.Value for lock-free reads, double-check pattern for creation
// writerType specifies whether this is for .tmp file or sparse file
func (ra *RandomAccessor) getOrCreateChunkedWriter(writerType WriterType) (*ChunkedFileWriter, error) {
	// Fast path: check if already exists (lock-free read)
	if val := ra.chunkedWriter.Load(); val != nil {
		// Check if it's the cleared marker
		if val == clearedChunkedWriterMarker {
			// ChunkedFileWriter was cleared, need to create new one
			// Continue to slow path below
		} else if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil {
			// Verify ChunkedFileWriter is valid (has LocalHandler) and matches writer type
			if cw.lh != nil && cw.fileID > 0 && cw.dataID > 0 && cw.writerType == writerType {
				return cw, nil
			}
			// ChunkedFileWriter exists but is invalid or wrong type, clear it and recreate
			DebugLog("[VFS RandomAccessor getOrCreateChunkedWriter] WARNING: Existing ChunkedFileWriter is invalid or wrong type (lh=%v, fileID=%d, dataID=%d, type=%d, expected=%d), will recreate: fileID=%d",
				cw.lh != nil, cw.fileID, cw.dataID, cw.writerType, writerType, ra.fileID)
			ra.chunkedWriter.Store(clearedChunkedWriterMarker) // Clear invalid ChunkedFileWriter (use marker instead of nil)
		}
	}

	// Slow path: need to create (use sync.Mutex for creation only)
	// Use fs.raCreateMu to ensure only one ChunkedFileWriter is created per file
	ra.fs.raCreateMu.Lock()
	defer ra.fs.raCreateMu.Unlock()

	// Double-check after acquiring lock
	if val := ra.chunkedWriter.Load(); val != nil {
		// Check if it's the cleared marker
		if val == clearedChunkedWriterMarker {
			// ChunkedFileWriter was cleared, need to create new one
			// Continue to creation below
		} else if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil {
			// Verify ChunkedFileWriter is valid (has LocalHandler) and matches writer type
			if cw.lh != nil && cw.fileID > 0 && cw.dataID > 0 && cw.writerType == writerType {
				return cw, nil
			}
			// ChunkedFileWriter exists but is invalid or wrong type, clear it and recreate
			DebugLog("[VFS RandomAccessor getOrCreateChunkedWriter] WARNING: Existing ChunkedFileWriter is invalid or wrong type after lock (lh=%v, fileID=%d, dataID=%d, type=%d, expected=%d), will recreate: fileID=%d",
				cw.lh != nil, cw.fileID, cw.dataID, cw.writerType, writerType, ra.fileID)
			ra.chunkedWriter.Store(clearedChunkedWriterMarker) // Clear invalid ChunkedFileWriter (use marker instead of nil)
		}
	}

	fileObj, err := ra.getFileObj()
	if err != nil {
		DebugLog("[VFS RandomAccessor getOrCreateChunkedWriter] ERROR: Failed to get file object: fileID=%d, error=%v", ra.fileID, err)
		return nil, err
	}

	// Use fixed 10MB chunk size (always use 10MB pool buffers)
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize // 10MB default
	}

	// Use existing DataID if file already has one, otherwise create new DataID
	// IMPORTANT: This ensures we reuse existing data instead of creating duplicate dataIDs
	var dataID int64
	var initialSize int64
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		// File already has DataID, reuse it
		dataID = fileObj.DataID
		// CRITICAL: Initialize size from existing file size to prevent size loss
		// This is important when ChunkedFileWriter is recreated for a file that already has data
		// Without this, the size would be reset to 0, causing data loss on flush
		initialSize = fileObj.Size
		DebugLog("[VFS ChunkedFileWriter Create] Reusing existing DataID: fileID=%d, dataID=%d, initialSize=%d, writerType=%d", ra.fileID, dataID, initialSize, writerType)
	} else {
		// File has no DataID, create new one
		dataID = core.NewID()
		if dataID <= 0 {
			DebugLog("[VFS RandomAccessor getOrCreateChunkedWriter] ERROR: Failed to create DataID: fileID=%d, writerType=%d", ra.fileID, writerType)
			return nil, fmt.Errorf("failed to create DataID")
		}
		initialSize = 0
		DebugLog("[VFS ChunkedFileWriter Create] Created new DataID: fileID=%d, dataID=%d, writerType=%d", ra.fileID, dataID, writerType)
	}

	// Initialize DataInfo with compression/encryption flags
	dataInfo := &core.DataInfo{
		ID:       dataID,
		OrigSize: 0,
		Size:     0,
		Kind:     core.DATA_NORMAL,
		XXH3:     0,
		SHA256_0: 0,
		SHA256_1: 0,
		SHA256_2: 0,
		SHA256_3: 0,
	}

	// For WRITER_TYPE_SEQ, set encryption flag from OrcasFS config
	// Compression will be decided on first chunk based on file type detection
	// This ensures ChunkedFileWriter processes data with correct flags from the start
	var enableRealtimeForSeq bool
	var realtimeDecidedForSeq bool
	if writerType == WRITER_TYPE_SEQ {
		endecWay := getEndecWayForFS(ra.fs)
		if endecWay > 0 {
			dataInfo.Kind |= endecWay
			DebugLog("[VFS ChunkedFileWriter Create] Set encryption flag for SEQ writer: fileID=%d, EndecWay=0x%x, Kind=0x%x", ra.fileID, endecWay, dataInfo.Kind)
			// Enable realtime processing for encryption (compression will be decided on first chunk)
			enableRealtimeForSeq = true
			// Don't mark as fully decided yet - compression detection happens on first chunk
			realtimeDecidedForSeq = false
		} else {
			// No encryption or compression configured, no realtime processing needed
			enableRealtimeForSeq = false
			realtimeDecidedForSeq = true
		}
		DebugLog("[VFS ChunkedFileWriter Create] Initial DataInfo Kind for SEQ writer: fileID=%d, Kind=0x%x, enableRealtime=%v", ra.fileID, dataInfo.Kind, enableRealtimeForSeq)
	}

	// Cache LocalHandler if available
	// IMPORTANT: ChunkedFileWriter requires LocalHandler because it needs:
	// 1. PutDataInfoAndObj() - for atomic metadata writes (in Handler interface)
	// 2. Temp write area - for large files and random writes (handled by TempWriteArea)
	// 3. GetDataAdapter() - for accessing data adapter (LocalHandler only)
	// If handler is not LocalHandler (e.g., RPC handler, wrapper handler), these methods won't be available
	var lh *core.LocalHandler
	if handler, ok := ra.fs.h.(*core.LocalHandler); ok {
		lh = handler
	} else {
		// Log handler type for debugging
		handlerType := fmt.Sprintf("%T", ra.fs.h)
		DebugLog("[VFS RandomAccessor getOrCreateChunkedWriter] ERROR: handler is not LocalHandler, cannot create ChunkedFileWriter: fileID=%d, handlerType=%s, writerType=%d", ra.fileID, handlerType, writerType)
		DebugLog("[VFS RandomAccessor getOrCreateChunkedWriter] NOTE: ChunkedFileWriter requires LocalHandler for direct data access. If using RPC/wrapper handler, consider using a different write path.")
		return nil, fmt.Errorf("handler is not LocalHandler (type: %s), cannot create ChunkedFileWriter", handlerType)
	}

	cw := &ChunkedFileWriter{
		fs:              ra.fs,
		fileID:          ra.fileID,
		dataID:          dataID,
		fileName:        fileObj.Name,
		chunkSize:       chunkSize,   // Always use 10MB
		size:            initialSize, // Initialize from existing file size if reusing DataID
		chunks:          make(map[int]*chunkBuffer),
		dataInfo:        dataInfo,
		enableRealtime:  enableRealtimeForSeq,  // For SEQ writer, enable if encryption configured
		realtimeDecided: realtimeDecidedForSeq, // For SEQ writer, let first chunk decide compression
		lh:              lh,
		firstChunkSN:    0,
		writerType:      writerType, // Set writer type
		ra:              ra,         // Store reference to parent RandomAccessor
	}
	atomic.StoreInt64(&cw.firstChunkSN, 0) // First chunk is always sn=0

	// Store in atomic.Value (lock-free for subsequent reads)
	ra.chunkedWriter.Store(cw)

	// Log creation of ChunkedFileWriter
	writerTypeName := "tmp"
	if writerType == WRITER_TYPE_SPARSE {
		writerTypeName = "sparse"
	} else if writerType == WRITER_TYPE_SEQ {
		writerTypeName = "seq"
	}
	DebugLog("[VFS ChunkedFileWriter Create] Created ChunkedFileWriter for %s file: fileID=%d, dataID=%d, fileName=%s, chunkSize=%d, realtimeCompressEncrypt=%v",
		writerTypeName, ra.fileID, dataID, fileObj.Name, chunkSize, cw.enableRealtime)

	// For new files (WRITER_TYPE_SEQ), update fileObj.DataID immediately to make it visible
	// This allows Read to find the DataID before final Flush
	if writerType == WRITER_TYPE_SEQ && initialSize == 0 && (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) {
		updateFileObj := &core.ObjectInfo{
			ID:     fileObj.ID,
			PID:    fileObj.PID,
			Type:   fileObj.Type,
			Name:   fileObj.Name,
			DataID: dataID,
			Size:   fileObj.Size, // Keep existing size (will be updated periodically)
			MTime:  core.Now(),
		}
		if _, putErr := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj}); putErr != nil {
			DebugLog("[VFS ChunkedFileWriter Create] WARNING: Failed to update file DataID for SEQ writer: %v", putErr)
		} else {
			// Update cache immediately
			fileObjCache.Put(ra.fileObjKey, updateFileObj)
			ra.fileObj.Store(updateFileObj)
			DebugLog("[VFS ChunkedFileWriter Create] Updated file DataID for SEQ writer: fileID=%d, oldDataID=%d, newDataID=%d",
				ra.fileID, fileObj.DataID, dataID)
		}
	}

	return cw, nil
}

// getOrCreateTempWriter gets or creates ChunkedFileWriter for .tmp files (backward compatibility)
func (ra *RandomAccessor) getOrCreateTempWriter() (*ChunkedFileWriter, error) {
	return ra.getOrCreateChunkedWriter(WRITER_TYPE_TMP)
}

// Write accumulates data in memory buffers and writes complete chunks
// Supports fragmented writes within a single chunk by tracking write position
// Ensures continuous writes by verifying offset matches expected position
// For chunked files, sn is calculated as offset / chunkSize
func (cw *ChunkedFileWriter) Write(offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// IMPORTANT: Allow writes during flush
	// Flush will only process chunks that existed at flush start time
	// New writes during flush will create/update chunks that will be flushed in next flush call
	// This prevents data loss from rejecting legitimate writes during flush
	// The flushing flag is still used to prevent concurrent flush operations

	// Check handler type early
	if cw.lh == nil {
		DebugLog("[VFS ChunkedFileWriter Write] ERROR: handler is not LocalHandler, cannot use Write: fileID=%d, dataID=%d, offset=%d, size=%d", cw.fileID, cw.dataID, offset, len(data))
		return fmt.Errorf("handler is not LocalHandler, cannot use Write")
	}

	writeEnd := offset + int64(len(data))

	currentOffset := offset
	dataPos := 0

	// Process data across chunks
	for currentOffset < writeEnd {
		// Use fixed chunk size (always 10MB)
		chunkSize := cw.chunkSize

		// Calculate sn and chunk start position
		sn := int(currentOffset / chunkSize)
		chunkStart := int64(sn) * chunkSize
		writeStartInChunk := currentOffset - chunkStart

		// Calculate how much data to write in this chunk
		writeEndInChunk := writeEnd - chunkStart
		if writeEndInChunk > chunkSize {
			writeEndInChunk = chunkSize
		}
		writeSize := writeEndInChunk - writeStartInChunk

		// Extract data for this chunk
		chunkData := data[dataPos : dataPos+int(writeSize)]

		// Get or create chunk buffer
		cw.mu.Lock()
		buf, exists := cw.chunks[sn]
		if !exists {
			// Chunk buffer doesn't exist in memory
			// For .tmp files with pure append writes, chunks are written sequentially:
			// - If chunk is not in memory, it means it hasn't been created yet (for new chunks)
			// - OR it has been flushed (for completed chunks, which shouldn't receive more writes)
			// For pure append writes, we can skip reading from disk and create a new buffer directly
			// This avoids unnecessary disk reads for sequential writes

			// Check if chunk might have been flushed (wait for flush to complete if in progress)
			flushKey := fmt.Sprintf("flush_%d_%d", cw.dataID, sn)
			cw.mu.Unlock()

			// Wait for any ongoing flush to complete (non-blocking if no flush in progress)
			// This ensures we don't create a buffer for a chunk that's being flushed
			_, _, _ = globalSingleFlight.Do(flushKey, func() (interface{}, error) {
				return nil, nil
			})

			// For pure append writes, check if chunk exists on disk only if writeStartInChunk > 0
			// (meaning we're writing to middle/end of chunk, not the beginning)
			// If writing from beginning (writeStartInChunk == 0), chunk shouldn't exist yet
			var existingChunkData []byte
			var readErr error
			if writeStartInChunk > 0 {
				// Writing to middle/end of chunk, might need to read existing data
				DebugLog("[VFS ChunkedFileWriter Write] Reading chunk from disk: fileID=%d, dataID=%d, sn=%d, offsetInChunk=%d", cw.fileID, cw.dataID, sn, writeStartInChunk)
				existingChunkData, readErr = cw.fs.h.GetData(cw.fs.c, cw.fs.bktID, cw.dataID, sn)
				if readErr == nil && len(existingChunkData) > 0 {
					DebugLog("[VFS ChunkedFileWriter Write] READ from disk successful: fileID=%d, dataID=%d, sn=%d, size=%d",
						cw.fileID, cw.dataID, sn, len(existingChunkData))
				} else {
					DebugLog("[VFS ChunkedFileWriter Write] READ from disk failed or empty: fileID=%d, dataID=%d, sn=%d, error=%v, size=%d",
						cw.fileID, cw.dataID, sn, readErr, len(existingChunkData))
				}
			} else {
				// Writing from beginning, chunk shouldn't exist (pure append)
				// Skip read to avoid unnecessary disk I/O
				readErr = fmt.Errorf("chunk not found (expected for new chunk)")
				DebugLog("[VFS ChunkedFileWriter Write] Skipping disk read for new chunk: fileID=%d, dataID=%d, sn=%d", cw.fileID, cw.dataID, sn)
			}
			if readErr == nil && len(existingChunkData) > 0 {
				// Chunk exists on disk, re-acquire lock and create buffer
				cw.mu.Lock()
				// Check again if chunk exists in memory (might have been added by another goroutine)
				buf, exists = cw.chunks[sn]
				if exists {
					// Chunk was added to memory by another goroutine, use it
					DebugLog("[VFS ChunkedFileWriter Write] Chunk was added to memory by another goroutine: fileID=%d, dataID=%d, sn=%d", cw.fileID, cw.dataID, sn)
					cw.mu.Unlock()
				} else {
					// Chunk doesn't exist in memory, create buffer and load from disk
					// Always use 10MB buffer from pool (capacity is 10MB, length will be set to chunk size)
					DebugLog("[VFS ChunkedFileWriter Write] Getting DataInfo for chunk decode: fileID=%d, dataID=%d, sn=%d", cw.fileID, cw.dataID, sn)
					var processedChunk []byte
					dataInfoFromDB, dataInfoErr := cw.fs.h.GetDataInfo(cw.fs.c, cw.fs.bktID, cw.dataID)
					if dataInfoErr == nil && dataInfoFromDB != nil {
						DebugLog("[VFS ChunkedFileWriter Write] Got DataInfo, decoding chunk with Kind: fileID=%d, dataID=%d, sn=%d, kind=0x%x", cw.fileID, cw.dataID, sn, dataInfoFromDB.Kind)
						processedChunk = cw.decodeChunkData(existingChunkData, dataInfoFromDB.Kind)
					} else {
						DebugLog("[VFS ChunkedFileWriter Write] Failed to get DataInfo or nil, using default decode: fileID=%d, dataID=%d, sn=%d, error=%v", cw.fileID, cw.dataID, sn, dataInfoErr)
						processedChunk = cw.decodeChunkData(existingChunkData, cw.dataInfo.Kind)
					}
					existingChunkSize := int64(len(processedChunk))

					// Allocate a fresh buffer (no object pool) sized to hold the chunk
					bufferLength := chunkSize
					if existingChunkSize > bufferLength {
						bufferLength = existingChunkSize
					}
					buf = allocChunkBuffer(bufferLength)

					// Load existing data into buffer
					// IMPORTANT: Always load existing data into buffer, even if chunk is complete
					// This ensures that subsequent writes can modify the chunk without reading from disk
					// The chunk should only be flushed in the 3 specified scenarios:
					// 1. chunk写满（range只有一个，而且是从0-10MB的范围写满）
					// 2. tmp的后缀被重命名掉
					// 3. 写入以后超时了，没有任何操作，也没有去除tmp后缀
					if writeStartInChunk < existingChunkSize {
						// Overwrite operation
						if existingChunkSize >= chunkSize {
							buf.addWriteRange(0, chunkSize)
						} else {
							buf.addWriteRange(0, existingChunkSize)
						}
						copyLen := len(processedChunk)
						if copyLen > int(bufferLength) {
							copyLen = int(bufferLength)
						}
						copy(buf.data[:copyLen], processedChunk)
						buf.offsetInChunk = int64(copyLen)
					} else {
						// Append operation
						// IMPORTANT: For append operations, we should load existing data into buffer
						// even if chunk is complete, because we might need to overwrite part of it
						// Only skip if writeStartInChunk >= chunkSize (writing beyond chunk boundary)
						if writeStartInChunk >= chunkSize {
							// Writing beyond chunk boundary, this shouldn't happen for append
							// But handle it gracefully by skipping
							DebugLog("[VFS ChunkedFileWriter Write] WARNING: Append write beyond chunk boundary, skipping: fileID=%d, dataID=%d, sn=%d, writeStartInChunk=%d, chunkSize=%d",
								cw.fileID, cw.dataID, sn, writeStartInChunk, chunkSize)
							cw.mu.Unlock()
							currentOffset += int64(len(chunkData))
							dataPos += int(len(chunkData))
							continue
						}
						// Load existing data into buffer
						if existingChunkSize >= chunkSize {
							buf.addWriteRange(0, chunkSize)
						} else {
							buf.addWriteRange(0, existingChunkSize)
						}
						copyLen := len(processedChunk)
						if copyLen > int(bufferLength) {
							copyLen = int(bufferLength)
						}
						copy(buf.data[:copyLen], processedChunk)
						buf.offsetInChunk = int64(copyLen)
					}

					cw.chunks[sn] = buf
					cw.mu.Unlock()
				}
			} else {
				// Chunk doesn't exist on disk, might be flushing
				// Wait for any ongoing flush to complete (non-blocking if no flush in progress)
				_, _, _ = globalSingleFlight.Do(flushKey, func() (interface{}, error) {
					// If flush is already in progress, this will wait for it to complete
					// If flush is not in progress, this will return immediately
					return nil, nil
				})

				// After waiting, try reading from disk again (flush might have completed)
				// Only read if writeStartInChunk > 0 (writing to middle/end of chunk)
				var existingChunkData2 []byte
				var readErr2 error
				if writeStartInChunk > 0 {
					DebugLog("[VFS ChunkedFileWriter Write] READING from disk after flush wait (non-zero offset): fileID=%d, dataID=%d, sn=%d, writeStartInChunk=%d",
						cw.fileID, cw.dataID, sn, writeStartInChunk)
					existingChunkData2, readErr2 = cw.fs.h.GetData(cw.fs.c, cw.fs.bktID, cw.dataID, sn)
					if readErr2 == nil && len(existingChunkData2) > 0 {
						DebugLog("[VFS ChunkedFileWriter Write] READ from disk after flush wait successful: fileID=%d, dataID=%d, sn=%d, size=%d",
							cw.fileID, cw.dataID, sn, len(existingChunkData2))
					} else {
						DebugLog("[VFS ChunkedFileWriter Write] READ from disk after flush wait failed/empty: fileID=%d, dataID=%d, sn=%d, error=%v",
							cw.fileID, cw.dataID, sn, readErr2)
					}
				} else {
					// Writing from beginning, skip read
					DebugLog("[VFS ChunkedFileWriter Write] SKIPPING read after flush wait (pure append from offset 0): fileID=%d, dataID=%d, sn=%d",
						cw.fileID, cw.dataID, sn)
					readErr2 = fmt.Errorf("chunk not found (expected for new chunk)")
				}

				// Re-acquire lock
				cw.mu.Lock()

				// Check again if chunk exists in memory (might have been added during flush wait)
				buf, exists = cw.chunks[sn]
				if exists {
					// Chunk was added to memory during flush wait, use it
					cw.mu.Unlock()
				} else if readErr2 == nil && len(existingChunkData2) > 0 {
					// Chunk now exists on disk after flush, create buffer and load it
					// Always use 10MB buffer from pool
					var processedChunk []byte
					dataInfoFromDB, dataInfoErr := cw.fs.h.GetDataInfo(cw.fs.c, cw.fs.bktID, cw.dataID)
					if dataInfoErr == nil && dataInfoFromDB != nil {
						processedChunk = cw.decodeChunkData(existingChunkData2, dataInfoFromDB.Kind)
					} else {
						processedChunk = cw.decodeChunkData(existingChunkData2, cw.dataInfo.Kind)
					}
					existingChunkSize := int64(len(processedChunk))

					// Allocate a fresh buffer (no object pool) sized to hold the chunk
					bufferLength := chunkSize
					if existingChunkSize > bufferLength {
						bufferLength = existingChunkSize
					}
					buf = allocChunkBuffer(bufferLength)

					// Load existing data into buffer
					// IMPORTANT: Always load existing data into buffer, even if chunk is complete
					// This ensures that subsequent writes can modify the chunk without reading from disk
					// The chunk should only be flushed in the 3 specified scenarios:
					// 1. chunk写满（range只有一个，而且是从0-10MB的范围写满）
					// 2. tmp的后缀被重命名掉
					// 3. 写入以后超时了，没有任何操作，也没有去除tmp后缀
					if writeStartInChunk < existingChunkSize {
						// Overwrite operation
						if existingChunkSize >= chunkSize {
							buf.addWriteRange(0, chunkSize)
						} else {
							buf.addWriteRange(0, existingChunkSize)
						}
						copyLen := len(processedChunk)
						if copyLen > int(bufferLength) {
							copyLen = int(bufferLength)
						}
						copy(buf.data[:copyLen], processedChunk)
						buf.offsetInChunk = int64(copyLen)
					} else {
						// Append operation
						// IMPORTANT: For append operations, we should load existing data into buffer
						// even if chunk is complete, because we might need to overwrite part of it
						// Only skip if writeStartInChunk >= chunkSize (writing beyond chunk boundary)
						if writeStartInChunk >= chunkSize {
							// Writing beyond chunk boundary, this shouldn't happen for append
							// But handle it gracefully by skipping
							DebugLog("[VFS ChunkedFileWriter Write] WARNING: Append write beyond chunk boundary, skipping: fileID=%d, dataID=%d, sn=%d, writeStartInChunk=%d, chunkSize=%d",
								cw.fileID, cw.dataID, sn, writeStartInChunk, chunkSize)
							cw.mu.Unlock()
							currentOffset += int64(len(chunkData))
							dataPos += int(len(chunkData))
							continue
						}
						// Load existing data into buffer
						if existingChunkSize >= chunkSize {
							buf.addWriteRange(0, chunkSize)
						} else {
							buf.addWriteRange(0, existingChunkSize)
						}
						copyLen := len(processedChunk)
						if copyLen > int(bufferLength) {
							copyLen = int(bufferLength)
						}
						copy(buf.data[:copyLen], processedChunk)
						buf.offsetInChunk = int64(copyLen)
					}

					cw.chunks[sn] = buf
					cw.mu.Unlock()
				} else {
					// Chunk still doesn't exist, create empty buffer
					// No read operation needed - pure append write from beginning
					buf = allocChunkBuffer(chunkSize)
					cw.chunks[sn] = buf
					cw.mu.Unlock()
				}
			}
		} else {
			// Chunk exists in memory, just unlock
			// No read operation needed - chunk already in memory
			cw.mu.Unlock()
		}

		// Lock this chunk buffer to ensure sequential writes within the chunk
		buf.mu.Lock()

		if sn == 0 && !cw.realtimeDecided {
			cw.decideRealtimeProcessing(chunkData)
		}

		// Copy data into buffer
		// Buffer from pool has 10MB capacity, but length may be smaller
		// Optimize: minimize buffer resizing by checking capacity first
		requiredBufferSize := writeStartInChunk + int64(len(chunkData))
		oldBufferLen := int64(len(buf.data))
		if requiredBufferSize > oldBufferLen {
			// Need to grow buffer length (capacity is already 10MB, so this is just reslicing)
			newBufferLength := requiredBufferSize
			if newBufferLength < chunkSize {
				newBufferLength = chunkSize
			}
			// Ensure we don't exceed capacity (shouldn't happen with 10MB capacity)
			if newBufferLength > int64(cap(buf.data)) {
				newBufferLength = int64(cap(buf.data))
			}
			// Only resize if actually needed (avoid unnecessary reslicing)
			if newBufferLength > oldBufferLen {
				oldLen := len(buf.data)
				buf.data = buf.data[:newBufferLength]
				// CRITICAL: Zero-fill newly extended portion to prevent data corruption
				// Buffer from pool may contain old data, so we must clear the extended portion
				// Use efficient batch clearing with copy
				extendedLen := len(buf.data) - oldLen
				for cleared := 0; cleared < extendedLen; {
					chunk := extendedLen - cleared
					if chunk > len(zeroSlice) {
						chunk = len(zeroSlice)
					}
					copy(buf.data[oldLen+cleared:oldLen+cleared+chunk], zeroSlice[:chunk])
					cleared += chunk
				}
			}
		}
		// CRITICAL: Ensure buffer is large enough before copying
		// Double-check to prevent panic or data corruption
		writeEndInChunkPos := writeStartInChunk + int64(len(chunkData))
		currentBufferLen := int64(len(buf.data))
		if writeEndInChunkPos > currentBufferLen {
			// Buffer needs to be extended - this should have been done above, but double-check
			DebugLog("[VFS ChunkedFileWriter Write] WARNING: Buffer too small, extending: fileID=%d, dataID=%d, sn=%d, writeStartInChunk=%d, writeEndInChunkPos=%d, currentBufferLen=%d, chunkSize=%d",
				cw.fileID, cw.dataID, sn, writeStartInChunk, writeEndInChunkPos, currentBufferLen, chunkSize)
			// Extend buffer to at least writeEndInChunkPos
			newBufferLength := writeEndInChunkPos
			if newBufferLength < chunkSize {
				newBufferLength = chunkSize
			}
			if newBufferLength > int64(cap(buf.data)) {
				newBufferLength = int64(cap(buf.data))
			}
			if newBufferLength > currentBufferLen {
				oldLen := len(buf.data)
				buf.data = buf.data[:newBufferLength]
				// CRITICAL: Zero-fill newly extended portion to prevent data corruption
				// Buffer from pool may contain old data, so we must clear the extended portion
				// Use efficient batch clearing with copy
				extendedLen := len(buf.data) - oldLen
				for cleared := 0; cleared < extendedLen; {
					chunk := extendedLen - cleared
					if chunk > len(zeroSlice) {
						chunk = len(zeroSlice)
					}
					copy(buf.data[oldLen+cleared:oldLen+cleared+chunk], zeroSlice[:chunk])
					cleared += chunk
				}
			}
		}
		// Copy data into buffer
		copyEnd := writeStartInChunk + int64(len(chunkData))
		if copyEnd > int64(len(buf.data)) {
			copyEnd = int64(len(buf.data))
		}
		actualCopyLen := copyEnd - writeStartInChunk
		if actualCopyLen > 0 {
			copy(buf.data[writeStartInChunk:copyEnd], chunkData[:actualCopyLen])
		}
		if actualCopyLen < int64(len(chunkData)) {
			DebugLog("[VFS ChunkedFileWriter Write] ERROR: Data truncated during copy: fileID=%d, dataID=%d, sn=%d, writeStartInChunk=%d, requestedLen=%d, actualCopyLen=%d, bufferLen=%d",
				cw.fileID, cw.dataID, sn, writeStartInChunk, len(chunkData), actualCopyLen, len(buf.data))
		}

		// Update offsetInChunk and add write range based on actual data written
		actualWriteEnd := writeStartInChunk + actualCopyLen
		if actualWriteEnd > buf.offsetInChunk {
			buf.offsetInChunk = actualWriteEnd
		}
		buf.addWriteRange(writeStartInChunk, actualWriteEnd)

		bufferProgress := buf.offsetInChunk

		// Check if chunk is complete: must have exactly one range covering entire chunk
		chunkComplete := buf.isChunkComplete(chunkSize)
		buf.mu.Unlock()

		if chunkComplete {
			// Chunk is full, flush it synchronously for memory efficiency
			// For sequential writes, synchronous flushing prevents memory accumulation
			// by freeing the buffer immediately after flush

			// CRITICAL: Extract chunk buffer before deleting from map to ensure we can clean it up
			cw.mu.Lock()
			bufToDelete := cw.chunks[sn]
			delete(cw.chunks, sn) // Remove immediately to prevent concurrent writes
			cw.mu.Unlock()

			// Use singleflight to ensure only one flush per chunk at a time
			// Key format: "flush_<dataID>_<sn>"
			flushKey := fmt.Sprintf("flush_%d_%d", cw.dataID, sn)

			// Flush synchronously (using singleflight to prevent duplicate flushes)
			_, err, _ := globalSingleFlight.Do(flushKey, func() (interface{}, error) {
				// Flush the chunk using the buffer (no copy needed for synchronous flush)
				// IMPORTANT: Must flush BEFORE clearing buffer data
				flushErr := cw.flushChunkWithBuffer(sn, buf)
				if flushErr != nil {
					return nil, flushErr
				}

				// MEMORY LEAK FIX: Explicitly clear chunk buffer data to help GC
				// This prevents 10MB buffers from accumulating in memory during continuous uploads
				// IMPORTANT: Clear buffer AFTER flush completes to ensure data is written
				if bufToDelete != nil {
					bufToDelete.mu.Lock()
					if cap(bufToDelete.data) > 0 {
						// Clear the entire underlying array to allow GC to reclaim memory
						// Set to nil instead of just clearing to ensure immediate release
						bufToDelete.data = nil
					}
					bufToDelete.ranges = nil
					bufToDelete.mu.Unlock()
				}

				// CRITICAL: Only return chunk buffer to pool if chunk is completely written
				// In Write(), we only flush when chunkComplete is true, which means:
				// - offsetInChunk >= chunkSize
				// - ranges cover entire chunk (0 to chunkSize)
				// So it's safe to return to pool after successful flush
				// Double-check to be extra safe
				buf.mu.Lock()
				isComplete := buf.isChunkComplete(chunkSize)
				buf.mu.Unlock()
				if isComplete {
					// Chunk is complete, safe to return to pool
					if cap(buf.data) <= 10<<20 {
						// CRITICAL: Clear entire buffer capacity before returning to pool
						// This prevents data corruption when buffer is reused
						// Clear the entire capacity, not just the length
						clearLen := cap(buf.data)
						if clearLen > 0 {
							// Extend to full capacity for clearing
							buf.data = buf.data[:clearLen]
							for cleared := 0; cleared < clearLen; {
								chunk := clearLen - cleared
								if chunk > len(zeroSlice) {
									chunk = len(zeroSlice)
								}
								copy(buf.data[cleared:cleared+chunk], zeroSlice[:chunk])
								cleared += chunk
							}
						}
						buf.data = buf.data[:0] // Reset length, keep capacity
						buf.offsetInChunk = 0
						buf.ranges = buf.ranges[:0] // Reset ranges
						// no chunk buffer pool: allow GC to reclaim
					}
				} else {
					// This should not happen if chunkComplete check is correct
					DebugLog("[VFS ChunkedFileWriter Write] WARNING: Not returning incomplete chunk to pool: fileID=%d, dataID=%d, sn=%d, offsetInChunk=%d, chunkSize=%d",
						cw.fileID, cw.dataID, sn, buf.offsetInChunk, chunkSize)
				}

				currentFileSize := atomic.LoadInt64(&cw.size)
				DebugLog("[VFS ChunkedFileWriter Write] Chunk flushed synchronously: fileID=%d, dataID=%d, sn=%d, chunkSize=%d, currentFileSize=%d",
					cw.fileID, cw.dataID, sn, bufferProgress, currentFileSize)

				return nil, nil
			})

			if err != nil {
				DebugLog("[VFS ChunkedFileWriter Write] ERROR: Failed to flush chunk synchronously: fileID=%d, dataID=%d, sn=%d, error=%v", cw.fileID, cw.dataID, sn, err)
				return err
			}
		} else {
			// Chunk not full yet, just buffer the data
			// Will be flushed when chunk is full or during final Flush()
			DebugLog("[VFS ChunkedFileWriter Write] Data buffered in chunk: fileID=%d, dataID=%d, sn=%d, bufferSize=%d/%d, remaining=%d",
				cw.fileID, cw.dataID, sn, bufferProgress, chunkSize, chunkSize-bufferProgress)
		}

		// Update tracking based on actual data written
		currentOffset += actualCopyLen
		dataPos += int(actualCopyLen)
	}

	// Update size atomically (always update to maximum writeEnd)
	// This ensures that concurrent writes correctly track the maximum file size
	for {
		currentSize := atomic.LoadInt64(&cw.size)
		if writeEnd <= currentSize {
			// Already updated by another concurrent write, no need to update
			break
		}
		if atomic.CompareAndSwapInt64(&cw.size, currentSize, writeEnd) {
			DebugLog("[VFS ChunkedFileWriter Write] Updated file size: fileID=%d, dataID=%d, oldSize=%d, newSize=%d",
				cw.fileID, cw.dataID, currentSize, writeEnd)
			break
		}
		// CAS failed, retry (another concurrent write may have updated size)
	}

	return nil
}

// flushChunk processes and writes a complete chunk
// If real-time compression/encryption is enabled, processes the chunk before writing
// Otherwise, writes raw data directly (for offline processing)
// All chunks are written synchronously
func (cw *ChunkedFileWriter) flushChunk(sn int) error {
	cw.mu.Lock()
	buf, exists := cw.chunks[sn]
	cw.mu.Unlock()

	if !exists {
		return nil // Chunk already flushed
	}

	return cw.flushChunkWithBuffer(sn, buf)
}

// flushChunkWithBuffer processes and writes a complete chunk using the provided buffer
// This is used when the chunk has already been removed from cw.chunks to prevent concurrent writes
func (cw *ChunkedFileWriter) flushChunkWithBuffer(sn int, buf *chunkBuffer) error {
	if buf == nil {
		return nil // No buffer to flush
	}

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.offsetInChunk == 0 {
		return nil // No data to flush
	}

	// Extract chunk data (only up to written size)
	// IMPORTANT: Create a copy of the data to avoid data race when buffer is modified concurrently
	// CRITICAL: Ensure we copy all data up to offsetInChunk
	// buf.data should always have length >= offsetInChunk, but check to be safe
	// First, ensure buf.data is extended to at least offsetInChunk before copying
	// IMPORTANT: If buf.data length < offsetInChunk, it means data hasn't been fully written yet
	// This can happen if flush is triggered before write completes (e.g., async writes)
	// In this case, we should only flush up to len(buf.data), not offsetInChunk
	if int64(len(buf.data)) < buf.offsetInChunk {
		DebugLog("[VFS ChunkedFileWriter flushChunk] WARNING: buf.data length (%d) < offsetInChunk (%d), data not fully written: fileID=%d, dataID=%d, sn=%d",
			len(buf.data), buf.offsetInChunk, cw.fileID, cw.dataID, sn)
		// Use actual data length instead of offsetInChunk to avoid flushing unwritten data
		// This prevents data corruption from premature flush
		// Update offsetInChunk to match actual data length
		buf.offsetInChunk = int64(len(buf.data))
		DebugLog("[VFS ChunkedFileWriter flushChunk] Adjusted offsetInChunk to match actual data length: fileID=%d, dataID=%d, sn=%d, newOffsetInChunk=%d",
			cw.fileID, cw.dataID, sn, buf.offsetInChunk)
	}

	// Copy all data up to offsetInChunk
	// IMPORTANT: After adjusting offsetInChunk above, it should match len(buf.data)
	// So we only copy actual data, no zero-filling needed
	chunkDataSize := int(buf.offsetInChunk)
	chunkData := allocChunkData(chunkDataSize)

	copyLen := int64(len(buf.data))
	if copyLen > buf.offsetInChunk {
		copyLen = buf.offsetInChunk
	}
	if copyLen > 0 {
		copy(chunkData[:copyLen], buf.data[:copyLen])
	}

	// IMPORTANT: After adjusting offsetInChunk, copyLen should equal offsetInChunk
	// If not, it means there's still a mismatch (should not happen)
	if copyLen < buf.offsetInChunk {
		DebugLog("[VFS ChunkedFileWriter flushChunk] ERROR: copyLen (%d) < offsetInChunk (%d) after adjustment, this should not happen: fileID=%d, dataID=%d, sn=%d, bufDataLen=%d",
			copyLen, buf.offsetInChunk, cw.fileID, cw.dataID, sn, len(buf.data))
		// Adjust chunkData size to match actual data length
		chunkData = chunkData[:copyLen]
		buf.offsetInChunk = copyLen
	}

	// Log buffer state for debugging
	DebugLog("[VFS ChunkedFileWriter flushChunk] Buffer state: fileID=%d, dataID=%d, sn=%d, offsetInChunk=%d, bufDataLen=%d, chunkDataLen=%d, numRanges=%d",
		cw.fileID, cw.dataID, sn, buf.offsetInChunk, len(buf.data), len(chunkData), len(buf.ranges))

	DebugLog("[VFS ChunkedFileWriter flushChunk] Processing chunk: fileID=%d, dataID=%d, sn=%d, originalSize=%d, enableRealtime=%v",
		cw.fileID, cw.dataID, sn, len(chunkData), cw.enableRealtime)

	// Update DataInfo XXH3/SHA256 and OrigSize
	// Note: XXH3 and SHA256 are updated in the actual processing code
	// Use atomic operation for thread-safe OrigSize update (multiple chunks may flush concurrently)
	atomic.AddInt64(&cw.dataInfo.OrigSize, int64(len(chunkData)))

	var finalData []byte
	var err error

	if cw.enableRealtime {
		// Real-time compression/encryption enabled
		// Process first chunk: check file extension first, then file type
		isFirstChunk := sn == 0
		cmprWay := getCmprWayForFS(cw.fs)
		if isFirstChunk && cmprWay > 0 && len(chunkData) > 0 {
			if !core.ShouldCompressFile(cw.fileName, chunkData) {
				cw.dataInfo.Kind &= ^core.DATA_CMPR_MASK
				DebugLog("[VFS ChunkedFileWriter flushChunk] File should not be compressed, skipping compression: fileID=%d, dataID=%d, sn=%d, fileName=%s",
					cw.fileID, cw.dataID, sn, cw.fileName)
			}
		}

		finalData, err = core.ProcessData(chunkData, &cw.dataInfo.Kind, getCmprQltyForFS(cw.fs), getEndecKeyForFS(cw.fs), isFirstChunk)
		if err != nil {
			DebugLog("[VFS ChunkedFileWriter flushChunk] ERROR: Failed to process chunk data: fileID=%d, dataID=%d, sn=%d, error=%v", cw.fileID, cw.dataID, sn, err)
			return err
		}

		// Update size of final data
		DebugLog("[VFS ChunkedFileWriter flushChunk] Compression/encryption applied: fileID=%d, dataID=%d, sn=%d, originalSize=%d, finalSize=%d, ratio=%.2f%%",
			cw.fileID, cw.dataID, sn, len(chunkData), len(finalData), float64(len(finalData))*100.0/float64(len(chunkData)))
	} else {
		// Real-time compression/encryption disabled - write raw data for offline processing
		// Data will be processed offline later (compression/encryption if needed)
		finalData = chunkData
		DebugLog("[VFS ChunkedFileWriter flushChunk] Writing raw data (offline processing): fileID=%d, dataID=%d, sn=%d, size=%d",
			cw.fileID, cw.dataID, sn, len(finalData))
	}

	// All chunks are written synchronously
	DebugLog("[VFS ChunkedFileWriter flushChunk] Writing chunk synchronously: fileID=%d, dataID=%d, sn=%d, size=%d, realtime=%v",
		cw.fileID, cw.dataID, sn, len(finalData), cw.enableRealtime)

	// Write chunk to disk first, then update Size only if write was successful
	// This prevents Size from being accumulated multiple times if chunk is flushed multiple times
	err = cw.writeChunkSync(sn, finalData)
	if err != nil {
		DebugLog("[VFS ChunkedFileWriter Write] ERROR: Failed to write chunk synchronously: fileID=%d, dataID=%d, sn=%d, size=%d, error=%v", cw.fileID, cw.dataID, sn, len(finalData), err)
		return err
	}

	// Only update Size after successful write to prevent duplicate accumulation
	// Use atomic operation for thread-safe Size update (multiple chunks may flush concurrently)
	// For compressed/encrypted data, Size is the compressed/encrypted size
	atomic.AddInt64(&cw.dataInfo.Size, int64(len(finalData)))

	// For WRITER_TYPE_SEQ (new sequential uploads), update fileObj.Size after each chunk flush
	// This ensures data is immediately visible for Read operations
	// We update synchronously for Size (critical for Read), and asynchronously for DataInfo (optimization)
	if cw.writerType == WRITER_TYPE_SEQ {
		if lh, ok := cw.fs.h.(*core.LocalHandler); ok {
			currentOrigSize := atomic.LoadInt64(&cw.dataInfo.OrigSize)
			currentSize := atomic.LoadInt64(&cw.dataInfo.Size)

			// Synchronously update fileObj.Size to make progress immediately visible for Read
			// Get fileObj first
			fileObjs, getErr := lh.Get(cw.fs.c, cw.fs.bktID, []int64{cw.fileID})
			if getErr != nil || len(fileObjs) == 0 {
				DebugLog("[VFS ChunkedFileWriter flushChunk] ERROR: Failed to get fileObj for size update: fileID=%d, sn=%d, error=%v", cw.fileID, sn, getErr)
			} else {
				currentFileObj := fileObjs[0]
				updateFileObj := &core.ObjectInfo{
					ID:     currentFileObj.ID,
					PID:    currentFileObj.PID,
					DataID: cw.dataID,
					Size:   currentOrigSize, // Use OrigSize as file size
					MTime:  core.Now(),
					Type:   currentFileObj.Type,
					Name:   currentFileObj.Name,
					Mode:   currentFileObj.Mode,
					Extra:  currentFileObj.Extra,
				}

				// Synchronously update file size for immediate Read visibility
				updateErr := lh.MetadataAdapter().SetObj(cw.fs.c, cw.fs.bktID, []string{"did", "s", "m"}, updateFileObj)
				if updateErr != nil {
					DebugLog("[VFS ChunkedFileWriter flushChunk] ERROR: SetObj failed for size update: fileID=%d, sn=%d, error=%v", cw.fileID, sn, updateErr)
				} else {
					// Update cache immediately
					fileObjCache.Put(cw.fileID, updateFileObj)

					// Also update RandomAccessor's cache using direct reference
					if cw.ra != nil {
						cw.ra.fileObj.Store(updateFileObj)
						fileObjCache.Put(cw.ra.fileObjKey, updateFileObj)
						DebugLog("[VFS ChunkedFileWriter flushChunk] SUCCESS: Updated fileObj.Size via RA: fileID=%d, sn=%d, newSize=%d", cw.fileID, sn, updateFileObj.Size)
					} else {
						DebugLog("[VFS ChunkedFileWriter flushChunk] WARNING: RandomAccessor reference is nil: fileID=%d, sn=%d, newSize=%d", cw.fileID, sn, updateFileObj.Size)
					}
				}
			}

			// Asynchronously update DataInfo every 10 chunks to reduce database load
			if sn == 0 || (sn > 0 && sn%10 == 0) {
				dataInfoSnapshot := core.DataInfo{
					ID:       cw.dataInfo.ID,
					OrigSize: currentOrigSize,
					Size:     currentSize,
					Kind:     cw.dataInfo.Kind,
					HdrXXH3:  cw.dataInfo.HdrXXH3,
					XXH3:     cw.dataInfo.XXH3,
					SHA256_0: cw.dataInfo.SHA256_0,
					SHA256_1: cw.dataInfo.SHA256_1,
					SHA256_2: cw.dataInfo.SHA256_2,
					SHA256_3: cw.dataInfo.SHA256_3,
				}
				dataID := cw.dataID
				fileID := cw.fileID
				currentSN := sn
				bktID := cw.fs.bktID
				ctx := cw.fs.c

				// Async DataInfo update
				go func() {
					updateKey := fmt.Sprintf("update_datainfo_%d", dataID)
					_, err, _ := globalSingleFlight.Do(updateKey, func() (interface{}, error) {
						_, updateErr := lh.PutDataInfo(ctx, bktID, []*core.DataInfo{&dataInfoSnapshot})
						if updateErr != nil {
							DebugLog("[VFS ChunkedFileWriter flushChunk] WARNING: Failed to update DataInfo: fileID=%d, dataID=%d, sn=%d, error=%v", fileID, dataID, currentSN, updateErr)
							return nil, updateErr
						}

						dataInfoCache.Put(dataID, &dataInfoSnapshot)
						decodingReaderCache.Del(dataID)
						DebugLog("[VFS ChunkedFileWriter flushChunk] Updated DataInfo: fileID=%d, dataID=%d, sn=%d, OrigSize=%d, Size=%d",
							fileID, dataID, currentSN, dataInfoSnapshot.OrigSize, dataInfoSnapshot.Size)

						return nil, nil
					})

					if err != nil {
						DebugLog("[VFS ChunkedFileWriter flushChunk] WARNING: DataInfo update failed: fileID=%d, dataID=%d, sn=%d, error=%v", fileID, dataID, currentSN, err)
					}
				}()
			}
		}
	}

	// chunkData is no longer needed after writeChunkSync completes (no pool; allow GC)

	return nil
}

// writeChunkSync writes a chunk synchronously (used for last chunk)
// IMPORTANT: This function should only be called once per chunk (sn)
// For .tmp files with pure append writes, chunks are written only once when full
// Singleflight ensures only one flush per chunk, so we can skip pre-write checks
// Chunks are written immediately when full, no batching
func (cw *ChunkedFileWriter) writeChunkSync(sn int, finalData []byte) error {
	// For pure append writes (.tmp files), chunks are written sequentially:
	// - Each chunk is written exactly once when it becomes full
	// - Singleflight ensures only one flush per chunk
	// - No pre-write read check (optimization: avoid unnecessary disk read)
	DebugLog("[VFS ChunkedFileWriter writeChunkSync] WRITING chunk (NO PRE-READ): fileID=%d, dataID=%d, sn=%d, size=%d",
		cw.fileID, cw.dataID, sn, len(finalData))

	_, err := cw.fs.h.PutData(cw.fs.c, cw.fs.bktID, cw.dataID, sn, finalData)
	if err == nil {
		DebugLog("[VFS ChunkedFileWriter writeChunkSync] Successfully wrote and flushed chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d",
			cw.fileID, cw.dataID, sn, len(finalData))
		return nil
	}

	// Handle ERR_OPEN_FILE: chunk may already exist
	if err != core.ERR_OPEN_FILE {
		DebugLog("[VFS ChunkedFileWriter writeChunkSync] ERROR: Failed to put data: fileID=%d, dataID=%d, sn=%d, error=%v", cw.fileID, cw.dataID, sn, err)
		return err
	}

	// Check if chunk exists on disk
	existingData, readErr := cw.fs.h.GetData(cw.fs.c, cw.fs.bktID, cw.dataID, sn)
	if readErr != nil || len(existingData) == 0 {
		// ERR_OPEN_FILE but chunk doesn't exist - real error (permission, disk space, etc.)
		DebugLog("[VFS ChunkedFileWriter writeChunkSync] ERROR: ERR_OPEN_FILE but chunk doesn't exist (likely permission/disk issue): fileID=%d, dataID=%d, sn=%d, size=%d, readErr=%v",
			cw.fileID, cw.dataID, sn, len(finalData), readErr)
		return fmt.Errorf("ERR_OPEN_FILE but chunk doesn't exist (likely permission/disk issue): %w", err)
	}

	// Chunk exists, check if size matches
	if len(existingData) == len(finalData) {
		// Size matches, assume chunk is correct and skip write
		DebugLog("[VFS ChunkedFileWriter writeChunkSync] Chunk already exists with matching size (ERR_OPEN_FILE), skipping write: fileID=%d, dataID=%d, sn=%d, size=%d",
			cw.fileID, cw.dataID, sn, len(finalData))
		return nil
	}

	// Chunk exists but size doesn't match - delete and rewrite
	// This can happen if: previous write was interrupted/partial, concurrent writes, or external modification
	DebugLog("[VFS ChunkedFileWriter writeChunkSync] WARNING: Chunk exists but size mismatch (ERR_OPEN_FILE), will delete and rewrite: fileID=%d, dataID=%d, sn=%d, writtenSize=%d, existingSize=%d",
		cw.fileID, cw.dataID, sn, len(finalData), len(existingData))

	// Try to delete existing chunk (ignore deletion errors - will retry write anyway)
	if cw.lh != nil {
		if da := cw.lh.GetDataAdapter(); da != nil {
			if deleteErr := da.Delete(cw.fs.c, cw.fs.bktID, cw.dataID, sn); deleteErr == nil {
				DebugLog("[VFS ChunkedFileWriter writeChunkSync] Deleted existing chunk before rewrite (size mismatch): fileID=%d, dataID=%d, sn=%d",
					cw.fileID, cw.dataID, sn)
			} else {
				DebugLog("[VFS ChunkedFileWriter writeChunkSync] WARNING: Failed to delete existing chunk (ignoring): fileID=%d, dataID=%d, sn=%d, error=%v",
					cw.fileID, cw.dataID, sn, deleteErr)
			}
		}
	}

	// Retry PutData after deletion attempt
	_, retryErr := cw.fs.h.PutData(cw.fs.c, cw.fs.bktID, cw.dataID, sn, finalData)
	if retryErr == nil {
		DebugLog("[VFS ChunkedFileWriter writeChunkSync] Successfully rewrote chunk after deletion (size mismatch): fileID=%d, dataID=%d, sn=%d, size=%d",
			cw.fileID, cw.dataID, sn, len(finalData))
		return nil
	}

	// Retry failed
	DebugLog("[VFS ChunkedFileWriter writeChunkSync] ERROR: Failed to rewrite chunk after deletion (size mismatch): fileID=%d, dataID=%d, sn=%d, error=%v",
		cw.fileID, cw.dataID, sn, retryErr)
	return fmt.Errorf("failed to rewrite chunk after deletion (size mismatch): %w", retryErr)
}

// decodeChunkData decodes chunk data using specified Kind (compression/encryption configuration)
// This allows decoding chunks that were written with different Kind than current cw.dataInfo.Kind
func (cw *ChunkedFileWriter) decodeChunkData(chunkData []byte, kind uint32) []byte {
	if len(chunkData) == 0 {
		return chunkData
	}

	// Use util.UnprocessData for unified decryption + decompression
	processedChunk, err := core.UnprocessData(chunkData, kind, getEndecKeyForFS(cw.fs))
	if err != nil {
		// Processing failed, return original data
		DebugLog("[VFS ChunkedFileWriter decodeChunkData] Unprocess failed, using raw data: fileID=%d, dataID=%d, error=%v", cw.fileID, cw.dataID, err)
		return chunkData
	}

	return processedChunk
}

func (cw *ChunkedFileWriter) decideRealtimeProcessing(firstChunk []byte) {
	cw.realtimeDecided = true

	// Get compression and encryption settings from OrcasFS, not from bucket config
	cmprWay := getCmprWayForFS(cw.fs)
	endecWay := getEndecWayForFS(cw.fs)

	cw.enableRealtime = cmprWay > 0 || endecWay > 0

	kind := core.DATA_NORMAL
	if cmprWay > 0 {
		kind |= cmprWay
	}
	if endecWay > 0 {
		kind |= endecWay
	}

	// If compression is enabled, perform smart detection:
	// 1. First check file extension to quickly filter out non-compressible files
	// 2. If extension doesn't indicate compression, then check file header
	if kind&core.DATA_CMPR_MASK != 0 && len(firstChunk) > 0 {
		if !core.ShouldCompressFile(cw.fileName, firstChunk) {
			kind &= ^core.DATA_CMPR_MASK
			DebugLog("[VFS ChunkedFileWriter decideRealtimeProcessing] File should not be compressed, skipping compression: fileID=%d, fileName=%s",
				cw.fileID, cw.fileName)
		}
	}

	cw.dataInfo.Kind = kind
	DebugLog("[VFS ChunkedFileWriter] realtime processing decided: fileID=%d, enableRealtime=%v, kind=0x%x", cw.fileID, cw.enableRealtime, cw.dataInfo.Kind)
}

// Flush uploads DataInfo and ObjectInfo for .tmp file
// Data chunks are already on disk via AppendData, so we only need to upload metadata
// force: if true, flush incomplete last chunk; if false, only flush complete chunks
func (cw *ChunkedFileWriter) Flush(force bool) error {
	// Validate ChunkedFileWriter before flushing
	if cw.fileID <= 0 || cw.dataID <= 0 {
		DebugLog("[VFS ChunkedFileWriter Flush] ERROR: ChunkedFileWriter is invalid (fileID=%d, dataID=%d), cannot flush", cw.fileID, cw.dataID)
		return fmt.Errorf("ChunkedFileWriter is invalid (fileID=%d, dataID=%d)", cw.fileID, cw.dataID)
	}

	// CRITICAL: Set flushing flag to prevent writes during flush
	// This ensures data consistency and prevents chunk corruption
	if !atomic.CompareAndSwapInt32(&cw.flushing, 0, 1) {
		// Already flushing, wait for it to complete or return error
		DebugLog("[VFS ChunkedFileWriter Flush] Already flushing, skipping duplicate flush: fileID=%d, dataID=%d", cw.fileID, cw.dataID)
		return fmt.Errorf("flush already in progress")
	}
	defer atomic.StoreInt32(&cw.flushing, 0)

	size := atomic.LoadInt64(&cw.size)
	origSize := atomic.LoadInt64(&cw.dataInfo.OrigSize)
	DebugLog("[VFS ChunkedFileWriter Flush] Starting flush for large file: fileID=%d, dataID=%d, cw.size=%d, dataInfo.OrigSize=%d, writerType=%d",
		cw.fileID, cw.dataID, size, origSize, cw.writerType)

	if size == 0 {
		// No data written, nothing to flush
		DebugLog("[VFS ChunkedFileWriter Flush] No data written, skipping flush: fileID=%d, dataID=%d", cw.fileID, cw.dataID)
		return nil
	}

	// Get file object
	fileObj, err := cw.fs.h.Get(cw.fs.c, cw.fs.bktID, []int64{cw.fileID})
	if err != nil || len(fileObj) == 0 {
		DebugLog("[VFS ChunkedFileWriter Flush] ERROR: Failed to get file object: fileID=%d, dataID=%d, error=%v, len=%d", cw.fileID, cw.dataID, err, len(fileObj))
		return fmt.Errorf("failed to get file object: %v", err)
	}
	obj := fileObj[0]

	// Flush all remaining incomplete chunks
	// IMPORTANT: Collect a snapshot of all chunks that need to be processed at flush start time
	// This ensures we only flush chunks that existed when flush started
	// New writes during flush will create/update chunks that will be flushed in next flush call
	// This prevents data corruption from concurrent writes during flush
	// Optimization: use object pool for remainingChunks slice
	cw.mu.Lock()
	remainingChunks := intSlicePool.Get().([]int)
	remainingChunks = remainingChunks[:0] // Reset length, keep capacity

	// Collect a snapshot of chunks from cw.chunks at flush start time
	// IMPORTANT: This is a snapshot - new writes during flush will not affect this list
	chunkCount := len(cw.chunks)
	if cap(remainingChunks) < chunkCount {
		remainingChunks = make([]int, 0, chunkCount)
	}
	for sn := range cw.chunks {
		remainingChunks = append(remainingChunks, sn)
	}
	cw.mu.Unlock()

	// IMPORTANT: After collecting snapshot, allow writes during flush
	// The flushing flag prevents concurrent flush operations, but allows writes
	// New writes during flush will create/update chunks that will be flushed in next flush call

	// Process remaining chunks if any
	var firstErr error
	if len(remainingChunks) > 0 {
		DebugLog("[VFS ChunkedFileWriter Flush] Flushing %d remaining incomplete chunks concurrently: fileID=%d, dataID=%d, chunks=%v, currentOrigSize=%d",
			len(remainingChunks), cw.fileID, cw.dataID, remainingChunks, atomic.LoadInt64(&cw.dataInfo.OrigSize))

		// Process all remaining chunks concurrently
		// IMPORTANT: Only process chunks that actually exist in cw.chunks
		// Chunks that were already flushed in Write() have been removed from cw.chunks
		// Use singleflight to prevent duplicate flushes
		// No need to sort or check disk - singleflight handles duplicate prevention
		var wg sync.WaitGroup
		var firstErrMu sync.Mutex

		for _, sn := range remainingChunks {
			// Check if chunk still exists in cw.chunks (might have been flushed by concurrent write)
			cw.mu.Lock()
			flushBuf, exists := cw.chunks[sn]
			if !exists {
				// Chunk already flushed or doesn't exist, skip
				cw.mu.Unlock()
				continue
			}

			// CRITICAL: Check if chunk is complete before flushing
			// This prevents premature flushing of incomplete chunks during active writes
			// Only flush chunks that are fully written (offsetInChunk >= chunkSize)
			// EXCEPTION: If force=true, also flush incomplete chunks (for final flush on close/release)
			chunkSize := cw.chunkSize

			flushBuf.mu.Lock()
			isComplete := flushBuf.isChunkComplete(chunkSize)
			offsetInChunk := flushBuf.offsetInChunk
			flushBuf.mu.Unlock()

			// Allow flushing if:
			// 1. Chunk is complete (offsetInChunk >= chunkSize), OR
			// 2. Force flush is enabled (for final flush on close/release)
			if !isComplete && !force {
				// Chunk is not complete and not forcing flush, skip it and leave it in cw.chunks
				// It will be flushed when it becomes complete or in the next flush
				DebugLog("[VFS ChunkedFileWriter Flush] Skipping incomplete chunk (not forcing): fileID=%d, dataID=%d, sn=%d, offsetInChunk=%d, chunkSize=%d, force=%v",
					cw.fileID, cw.dataID, sn, offsetInChunk, chunkSize, force)
				cw.mu.Unlock()
				continue
			}

			if !isComplete && force {
				DebugLog("[VFS ChunkedFileWriter Flush] Force flushing incomplete chunk: fileID=%d, dataID=%d, sn=%d, offsetInChunk=%d, chunkSize=%d",
					cw.fileID, cw.dataID, sn, offsetInChunk, chunkSize)
			}

			// Chunk is complete, safe to remove and flush
			// Remove chunk from cw.chunks immediately to prevent concurrent writes
			delete(cw.chunks, sn)
			cw.mu.Unlock()

			// Flush chunk concurrently
			wg.Add(1)
			go func(chunkSN int, buf *chunkBuffer) {
				// MEMORY LEAK FIX: Clean up buffer after flush completes
				defer func() {
					if buf != nil {
						buf.mu.Lock()
						if cap(buf.data) > 0 {
							buf.data = nil // Explicitly release 10MB buffer
						}
						buf.ranges = nil
						buf.mu.Unlock()
					}
					wg.Done()
				}()

				// Use singleflight to ensure only one flush per chunk at a time
				// Key format: "flush_<dataID>_<sn>"
				flushKey := fmt.Sprintf("flush_%d_%d", cw.dataID, chunkSN)

				_, err, _ := globalSingleFlight.Do(flushKey, func() (interface{}, error) {
					// Process chunk (compression/encryption) - reuse flushChunkWithBuffer logic
					flushErr := cw.flushChunkWithBuffer(chunkSN, buf)
					if flushErr != nil {
						return nil, flushErr
					}

					// CRITICAL: Only return chunk buffer to pool if chunk is completely written
					chunkSize := cw.chunkSize
					buf.mu.Lock()
					isComplete := buf.isChunkComplete(chunkSize)
					buf.mu.Unlock()

					// Return chunk buffer to pool only if chunk is completely written
					if isComplete && cap(buf.data) <= 10<<20 {
						// CRITICAL: Clear entire buffer capacity before returning to pool
						// This prevents data corruption when buffer is reused
						clearLen := cap(buf.data)
						if clearLen > 0 {
							// Extend to full capacity for clearing
							buf.data = buf.data[:clearLen]
							for cleared := 0; cleared < clearLen; {
								chunk := clearLen - cleared
								if chunk > len(zeroSlice) {
									chunk = len(zeroSlice)
								}
								copy(buf.data[cleared:cleared+chunk], zeroSlice[:chunk])
								cleared += chunk
							}
						}
						buf.data = buf.data[:0] // Reset length, keep capacity
						buf.offsetInChunk = 0
						buf.ranges = buf.ranges[:0] // Reset ranges
						// no chunk buffer pool: allow GC to reclaim
					}

					return nil, nil
				})

				if err != nil {
					firstErrMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					firstErrMu.Unlock()
					DebugLog("[VFS ChunkedFileWriter Flush] ERROR: Failed to flush chunk: fileID=%d, dataID=%d, sn=%d, error=%v", cw.fileID, cw.dataID, chunkSN, err)
				}
			}(sn, flushBuf)
		}

		// Wait for all concurrent flushes to complete
		wg.Wait()

		if firstErr != nil {
			// Return remainingChunks slice to pool before returning error
			intSlicePool.Put(remainingChunks[:0])
			return firstErr
		}
	} else {
		// No remaining chunks, all were flushed during Write()
		DebugLog("[VFS ChunkedFileWriter Flush] No remaining chunks to flush (all flushed during Write): fileID=%d, dataID=%d, currentOrigSize=%d",
			cw.fileID, cw.dataID, atomic.LoadInt64(&cw.dataInfo.OrigSize))
	}

	// Return remainingChunks slice to pool
	intSlicePool.Put(remainingChunks[:0])

	// Use the DataInfo we've been tracking (with compression/encryption flags and sizes)
	dataInfo := cw.dataInfo

	// OrigSize should be the logical file size (max write offset)
	// For WRITER_TYPE_SEQ (sequential uploads), OrigSize is accumulated correctly in flushChunk
	// and should not be overwritten with cw.size. Only use cw.size if OrigSize is 0 or less.
	// For sparse files or files with holes, cw.size may be more accurate.
	currentOrigSize := atomic.LoadInt64(&dataInfo.OrigSize)
	if currentOrigSize == 0 || size > currentOrigSize {
		// Use cw.size as OrigSize (for sparse files or when OrigSize wasn't accumulated)
		dataInfo.OrigSize = size
	}

	// For offline processing, Size should equal OrigSize (actual data size on disk)
	// For real-time processing, Size is already accumulated (compressed/encrypted size)
	if !cw.enableRealtime {
		// For offline processing, Size should equal OrigSize (both are actual data size on disk)
		// The accumulated Size in flushChunk should already equal OrigSize for offline processing
		// But to be safe, ensure they match
		if dataInfo.Size != dataInfo.OrigSize {
			DebugLog("[VFS ChunkedFileWriter Flush] WARNING: Size mismatch for offline processing: Size=%d, OrigSize=%d, correcting", dataInfo.Size, dataInfo.OrigSize)
			dataInfo.Size = dataInfo.OrigSize
		}
		// IMPORTANT: If enableRealtime is false, we wrote raw data, so clear compression/encryption flags
		// This ensures that readers don't try to decompress/decrypt raw data
		if dataInfo.Kind&(core.DATA_CMPR_MASK|core.DATA_ENDEC_MASK) != 0 {
			DebugLog("[VFS ChunkedFileWriter Flush] WARNING: Offline processing but Kind has compression/encryption flags, clearing: fileID=%d, dataID=%d, Kind=0x%x", cw.fileID, cw.dataID, dataInfo.Kind)
			dataInfo.Kind &= ^(core.DATA_CMPR_MASK | core.DATA_ENDEC_MASK)
		}
	} else {
		// For real-time processing, Size is already accumulated (compressed/encrypted size)
		// Don't overwrite it
		DebugLog("[VFS ChunkedFileWriter Flush] Real-time processing: OrigSize=%d, Size=%d (compressed/encrypted)", dataInfo.OrigSize, dataInfo.Size)
	}

	// Update file object
	obj.DataID = cw.dataID
	obj.Size = size
	obj.MTime = core.Now()

	// IMPORTANT: Do NOT auto-remove .tmp suffix during flush
	// Renaming should be controlled by the upper layer (e.g., when file upload is complete)
	// Auto-renaming during flush can cause issues:
	// 1. Data might not be fully written yet
	// 2. Multiple flush calls might cause race conditions
	// 3. Upper layer should control when to rename based on business logic
	// The .tmp suffix removal should happen explicitly via Rename() call from upper layer

	// Upload DataInfo and ObjectInfo together
	// IMPORTANT: Do this BEFORE renaming to ensure all data is persisted
	DebugLog("[VFS ChunkedFileWriter Flush] Uploading metadata: fileID=%d, dataID=%d, fileName=%s, origSize=%d, size=%d, chunkSize=%d, realtime=%v, hasCompression=%v, hasEncryption=%v",
		cw.fileID, cw.dataID, obj.Name, dataInfo.OrigSize, dataInfo.Size, cw.chunkSize, cw.enableRealtime,
		dataInfo.Kind&core.DATA_CMPR_MASK != 0, dataInfo.Kind&core.DATA_ENDEC_MASK != 0)
	err = cw.fs.h.PutDataInfoAndObj(cw.fs.c, cw.fs.bktID, []*core.DataInfo{dataInfo}, []*core.ObjectInfo{obj})
	if err != nil {
		DebugLog("[VFS ChunkedFileWriter Flush] ERROR: Failed to upload DataInfo and ObjectInfo: fileID=%d, dataID=%d, error=%v", cw.fileID, cw.dataID, err)
		return err
	}

	// Re-fetch file object from database to ensure we have the latest information
	// This is important for directory cache updates
	updatedFileObjs, err := cw.fs.h.Get(cw.fs.c, cw.fs.bktID, []int64{cw.fileID})
	if err == nil && len(updatedFileObjs) > 0 {
		obj = updatedFileObjs[0]
		DebugLog("[VFS ChunkedFileWriter Flush] Re-fetched file object from database: fileID=%d, dataID=%d, size=%d, pid=%d, name=%s", cw.fileID, obj.DataID, obj.Size, obj.PID, obj.Name)
	}

	// Update cache
	fileObjCache.Put(cw.fileID, obj)
	dataInfoCache.Put(cw.dataID, dataInfo)

	// After sync flush, append file to directory listing cache
	// This ensures the file is immediately visible in Readdir
	if obj.PID > 0 {
		dirNode := &OrcasNode{
			fs:    cw.fs,
			objID: obj.PID,
		}
		dirNode.invalidateDirListCache(obj.PID)
		DebugLog("[VFS ChunkedFileWriter Flush] Appended file to directory listing cache after sync flush: fileID=%d, dirID=%d, name=%s", cw.fileID, obj.PID, obj.Name)
	}

	// Update RandomAccessor's fileObj cache if available
	// This ensures that subsequent reads use the updated file size
	if ra := cw.fs.getRandomAccessorByFileID(cw.fileID); ra != nil {
		ra.fileObj.Store(obj)
		fileObjCache.Put(ra.fileObjKey, obj)
	}

	// Calculate compression ratio if applicable
	compressionRatio := 1.0
	if dataInfo.OrigSize > 0 && dataInfo.Size != dataInfo.OrigSize {
		compressionRatio = float64(dataInfo.Size) / float64(dataInfo.OrigSize)
	}

	DebugLog("[VFS ChunkedFileWriter Flush] Successfully flushed large file: fileID=%d, dataID=%d, fileName=%s, origSize=%d, size=%d, chunkSize=%d, compressionRatio=%.2f%%",
		cw.fileID, cw.dataID, obj.Name, dataInfo.OrigSize, dataInfo.Size, cw.chunkSize, compressionRatio*100.0)

	// Verify written data (for debugging - can be disabled in production)
	//if err := cw.VerifyWriteData(); err != nil {
	//	DebugLog("[VFS ChunkedFileWriter Flush] WARNING: Write verification failed: fileID=%d, dataID=%d, error=%v", cw.fileID, cw.dataID, err)
	// Don't return error - verification failure doesn't mean write failed
	//}

	// IMPORTANT: Do NOT auto-remove .tmp suffix during flush
	// Renaming should be controlled by the upper layer explicitly via Rename() call
	// Auto-renaming during flush can cause issues:
	// 1. Data might not be fully written yet
	// 2. Multiple flush calls might cause race conditions
	// 3. Upper layer should control when to rename based on business logic
	// Removed all auto-rename logic - upper layer must call Rename() explicitly when ready

	return nil
}

// NewRandomAccessor creates a random access object
func NewRandomAccessor(fs *OrcasFS, fileID int64) (*RandomAccessor, error) {
	// Get configuration to initialize fixed-length operations array (optimized: avoid temporary object creation)
	maxBufferWrites := int(core.GetWriteBufferConfig().MaxBufferWrites)
	if maxBufferWrites <= 0 {
		maxBufferWrites = 200 // Default value
	}

	ra := &RandomAccessor{
		fs:         fs,
		fileID:     fileID,
		fileObjKey: fileID, // Pre-compute and cache key
		buffer: &WriteBuffer{
			fileID:     fileID,
			operations: make([]WriteOperation, maxBufferWrites), // Fixed-length array
			writeIndex: 0,                                       // Start from 0
			totalSize:  0,
		},
		lastOffset: 0,
	}

	// Initialize isTmpFile flag by checking file name at creation time
	// This avoids repeated file name checks on every write operation
	// IMPORTANT: This flag is immutable after creation - if file is renamed,
	// the RandomAccessor MUST be closed and recreated
	// If getFileObj fails (e.g., permission issue), default to false (not a .tmp file)
	// This allows RandomAccessor to be created even if file object is not immediately accessible
	fileObj, err := ra.getFileObj()
	if err != nil {
		// Log warning but don't fail - isTmpFile will default to false
		// This allows RandomAccessor to be created even if file object is not immediately accessible
		// The flag will be checked again when needed (e.g., in Write method)
		DebugLog("[VFS NewRandomAccessor] WARNING: Failed to get file object for isTmpFile check: fileID=%d, error=%v, defaulting to isTmpFile=false", fileID, err)
		ra.isTmpFile = false
	} else {
		ra.isTmpFile = isTempFile(fileObj)
		DebugLog("[VFS NewRandomAccessor] Created RandomAccessor: fileID=%d, fileName=%s, isTmpFile=%v", fileID, fileObj.Name, ra.isTmpFile)
	}

	// Note: RandomAccessor registration is now handled by the caller (getRandomAccessor)
	// to ensure proper synchronization for .tmp files during concurrent writes
	// For non-.tmp files or when called from other places, registration should be done explicitly
	// This prevents race conditions where multiple RandomAccessors are registered for the same file

	return ra, nil
}

// MarkSparseFile marks file as sparse (pre-allocated) for optimization
// This is used when SetAllocationSize is called to pre-allocate space
func (ra *RandomAccessor) MarkSparseFile(size int64) {
	atomic.StoreInt64(&ra.sparseSize, size)
}

// Write adds write operation to buffer
// Optimization: sequential write optimization - if sequential write starting from 0, directly write to data block, avoid caching
// Optimization: for sparse files (pre-allocated), use more aggressive delayed flush to reduce frequent flushes
// For .tmp files, we don't know the final file size until rename (removing .tmp extension),
// so we use random write mode first, and the decision will be made during Flush() based on final file size.
func (ra *RandomAccessor) Write(offset int64, data []byte) error {
	// Optimization: cache fileObj to avoid repeated getFileObj calls
	// Check local atomic value first (fast path)
	var fileObj *core.ObjectInfo
	fileObjValue := ra.fileObj.Load()
	if fileObjValue != nil {
		if obj, ok := fileObjValue.(*core.ObjectInfo); ok && obj != nil {
			fileObj = obj
		}
	}

	// If not in local cache, get from cache or database (only if needed)
	var err error
	if fileObj == nil {
		fileObj, err = ra.getFileObj()
		if err != nil {
			DebugLog("[VFS RandomAccessor Write] ERROR: Failed to get file object: fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, offset, len(data), err)
			return err
		}
	}

	// CRITICAL: Detect file rename by comparing isTmpFile flag with actual file name
	// 原来这里一旦检测到 .tmp 被重命名，就直接返回错误，要求上层重建 RandomAccessor，
	// 这会导致已经 flush 到后端的文件在后续写入时被拒绝（见 3.log 中
	//  "File was renamed from .tmp, RandomAccessor must be recreated" 的错误）。
	// 为了兼容 Office 等在重命名后继续使用同一 FD 写入的场景，这里改为：
	//   - 如果原 RA 认为自己是 .tmp，但实际文件名已经不是 .tmp（说明发生了重命名），
	//     则记录日志、清理掉旧的 TMP ChunkedFileWriter，保持 isTmpFile=false，
	//     然后继续执行后续的随机写路径，让新的写入走正常非 tmp 的写路径（使用 journal/顺写）。
	//   - 不再返回 "RandomAccessor must be recreated" 错误。
	actualIsTmpFile := isTempFile(fileObj)
	if ra.isTmpFile != actualIsTmpFile {
		// 文件名与 RA 创建时的 isTmpFile 状态不一致，说明发生了重命名
		if ra.isTmpFile && !actualIsTmpFile {
			// 从 .tmp 重命名为普通文件：
			// 清理掉仍然挂在 RA 上的 TMP 类型 ChunkedFileWriter，避免后续误用
			// IMPORTANT: 同时清除 sparseSize，因为重命名后的文件不应该再被当作sparse file
			// 否则后续写入会被误判为sparse file，创建SPARSE类型的ChunkedFileWriter，导致大小不匹配错误
			DebugLog("[VFS RandomAccessor Write] Detected .tmp rename for fileID=%d (oldName=%s), clearing TMP ChunkedFileWriter and sparseSize, switching to non-tmp mode", ra.fileID, fileObj.Name)
			ra.chunkedWriter.Store(clearedChunkedWriterMarker)
			atomic.StoreInt64(&ra.sparseSize, 0) // 清除sparseSize，避免误判为sparse file
			ra.isTmpFile = false
			// 继续往下走，使用普通文件的顺写/随机写路径，不再直接拒绝
		} else if !ra.isTmpFile && actualIsTmpFile {
			// 从普通文件重命名为 .tmp（极少见），当前 RA 没有 TMP 语义，安全起见仍然要求重建
			DebugLog("[VFS RandomAccessor Write] File was renamed to .tmp, RandomAccessor must be recreated: fileID=%d, fileName=%s", ra.fileID, fileObj.Name)
			ra.Close()
			return fmt.Errorf("file was renamed to .tmp, RandomAccessor must be recreated: fileID=%d, fileName=%s", ra.fileID, fileObj.Name)
		}
	}

	// Update write range for sparse files (after rename detection, so we use updated sparseSize)
	// This allows shouldUseJournal to check if writes are within local sequential range
	sparseSize := ra.getSparseSize() // Re-read sparseSize after rename detection (may have been cleared)
	if sparseSize > 0 {
		ra.updateWriteRange(offset, int64(len(data)))
	}

	// PRIORITY 0: Check if we can continue using sequential buffer
	// This must be checked BEFORE shouldUseJournal to ensure sequential writes don't use journal
	if ra.seqBuffer != nil {
		ra.seqBuffer.mu.Lock()
		seqOffset := ra.seqBuffer.offset
		seqClosed := ra.seqBuffer.closed
		hasSeqData := ra.seqBuffer.hasData
		ra.seqBuffer.mu.Unlock()

		if !seqClosed && hasSeqData {
			// Check if still sequential write (continue from current position)
			if offset == seqOffset {
				// Sequential write, use optimized path (this takes priority over journal)
				return ra.writeSequential(offset, data)
			} else if offset < seqOffset {
				// Write backwards, switch to random write mode
				// OPTIMIZATION: Only flush if buffer is full, otherwise defer to Release
				ra.seqBuffer.mu.Lock()
				bufferSize := int64(len(ra.seqBuffer.buffer))
				chunkSize := ra.seqBuffer.chunkSize
				ra.seqBuffer.mu.Unlock()

				if bufferSize >= chunkSize {
					// Buffer is full, flush it now
					if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
						DebugLog("[VFS RandomAccessor Write] ERROR: Failed to flush sequential buffer (chunk full, backwards write): fileID=%d, offset=%d, seqOffset=%d, size=%d, error=%v", ra.fileID, offset, seqOffset, len(data), flushErr)
						return flushErr
					}
				} else {
					// Buffer is not full, defer flush to Release
					DebugLog("[VFS RandomAccessor Write] Deferring sequential buffer flush (backwards write, buffer not full): fileID=%d, offset=%d, seqOffset=%d, bufferSize=%d, chunkSize=%d",
						ra.fileID, offset, seqOffset, bufferSize, chunkSize)
				}
				ra.seqBuffer.mu.Lock()
				ra.seqBuffer.closed = true
				ra.seqBuffer.mu.Unlock()
			} else {
				// offset > seqOffset: skipped some positions, switch to random write mode
				// OPTIMIZATION: Only flush if buffer is full, otherwise defer to Release
				ra.seqBuffer.mu.Lock()
				bufferSize := int64(len(ra.seqBuffer.buffer))
				chunkSize := ra.seqBuffer.chunkSize
				ra.seqBuffer.mu.Unlock()

				if bufferSize >= chunkSize {
					// Buffer is full, flush it now
					if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
						DebugLog("[VFS RandomAccessor Write] ERROR: Failed to flush sequential buffer (chunk full, skipped positions): fileID=%d, offset=%d, seqOffset=%d, size=%d, error=%v", ra.fileID, offset, seqOffset, len(data), flushErr)
						return flushErr
					}
				} else {
					// Buffer is not full, defer flush to Release
					DebugLog("[VFS RandomAccessor Write] Deferring sequential buffer flush (skipped positions, buffer not full): fileID=%d, offset=%d, seqOffset=%d, bufferSize=%d, chunkSize=%d",
						ra.fileID, offset, seqOffset, bufferSize, chunkSize)
				}
				ra.seqBuffer.mu.Lock()
				ra.seqBuffer.closed = true
				ra.seqBuffer.mu.Unlock()
			}
		}
	}

	// PRIORITY 1: Check if should use journal
	// Journal provides version control for random writes on existing files
	// Pass offset and length for sparse file local sequential range detection
	if ra.shouldUseJournal(fileObj, offset, int64(len(data))) {
		DebugLog("[VFS RandomAccessor Write] Using journal: fileID=%d, offset=%d, size=%d", ra.fileID, offset, len(data))

		// Migrate existing WriteBuffer entries to Journal (if any)
		if ra.buffer.writeIndex > 0 {
			if err := ra.migrateWriteBufferToJournal(); err != nil {
				DebugLog("[VFS RandomAccessor Write] WARNING: Failed to migrate WriteBuffer to Journal: %v", err)
				// Continue anyway, write to journal
			}
		}

		// Migrate sequential buffer data to Journal (if any)
		if ra.seqBuffer != nil {
			if err := ra.migrateSeqBufferToJournal(); err != nil {
				DebugLog("[VFS RandomAccessor Write] WARNING: Failed to migrate SeqBuffer to Journal: %v", err)
			}
		}

		return ra.writeToJournal(offset, data)
	}

	// CRITICAL: For non-.tmp files, do NOT use ChunkedFileWriter (TMP type)
	// ChunkedFileWriter should only be used for .tmp files and sparse files
	// Once a file is renamed (removed .tmp suffix), writes should go through normal write path
	// Use the immutable isTmpFile flag (set at creation time) instead of checking file name
	// This is more efficient and avoids cache inconsistency issues
	// IMPORTANT: If file is renamed, RandomAccessor MUST be closed and recreated

	// IMPORTANT: Check ChunkedFileWriter only for .tmp files
	// For non-.tmp files, ChunkedFileWriter should have been cleared after rename (for .tmp files)
	// BUT sparse files and new sequential uploads can also use ChunkedFileWriter
	// (WRITER_TYPE_SPARSE, WRITER_TYPE_SEQ), so we need to allow those cases.
	if !ra.isTmpFile {
		// File is not .tmp, check if ChunkedFileWriter still exists
		// Only reject/clear if it's a WRITER_TYPE_TMP (which shouldn't exist for non-.tmp files)
		// Allow WRITER_TYPE_SPARSE / WRITER_TYPE_SEQ for sparse/sequential files
		hasChunkedWriter := ra.chunkedWriter.Load() != nil
		if hasChunkedWriter {
			chunkedWriterVal := ra.chunkedWriter.Load()
			if chunkedWriterVal != clearedChunkedWriterMarker {
				cw, ok := chunkedWriterVal.(*ChunkedFileWriter)
				if ok && cw != nil {
					if cw.writerType == WRITER_TYPE_TMP {
						// File is not .tmp but still has a TMP ChunkedFileWriter attached.
						// This can happen if a previous .tmp RandomAccessor was reused or not cleaned up correctly.
						// Instead of rejecting writes, clear the stale TMP writer and continue with normal path.
						DebugLog("[VFS RandomAccessor Write] WARNING: Non-.tmp file has stale TMP ChunkedFileWriter, clearing and continuing: fileID=%d, fileName=%s", ra.fileID, fileObj.Name)
						ra.chunkedWriter.Store(clearedChunkedWriterMarker)
					} else if cw.writerType == WRITER_TYPE_SPARSE || cw.writerType == WRITER_TYPE_SEQ {
						// Sparse or sequential ChunkedFileWriter exists, use it directly
						atomic.StoreInt64(&ra.lastActivity, core.Now())
						getDelayedFlushManager().schedule(ra, false)
						DebugLog("[VFS RandomAccessor Write] Using existing ChunkedFileWriter (type=%d): fileID=%d, offset=%d, size=%d", cw.writerType, ra.fileID, offset, len(data))
						return cw.Write(offset, data)
					}
				}
			} else {
				// ChunkedFileWriter was cleared (clearedChunkedWriterMarker)
				// This could mean:
				// 1. File was renamed from .tmp (should allow writes via normal path, not SPARSE ChunkedFileWriter)
				// 2. Sparse file ChunkedFileWriter was cleared after flush (should allow writes)
				// IMPORTANT: If rename detection just happened (ra.isTmpFile was just set to false),
				// we should allow writes via normal path (journal/sequential buffer), not reject them.
				// The rename detection logic above already cleared sparseSize, so we won't create SPARSE ChunkedFileWriter.
				sparseSize := ra.getSparseSize()
				if sparseSize > 0 {
					// Sparse file - allow writes (ChunkedFileWriter will be recreated if needed)
					DebugLog("[VFS RandomAccessor Write] Sparse file with cleared ChunkedFileWriter, allowing writes (will recreate if needed): fileID=%d", ra.fileID)
					// Continue to normal write path below
				} else {
					// Non-sparse, non-.tmp file with cleared ChunkedFileWriter
					// This could mean:
					// 1. File was renamed from .tmp (rename detection just cleared sparseSize)
					// 2. ChunkedFileWriter was cleared for other reasons
					// In both cases, we should allow writes via normal path (journal/sequential buffer)
					// The rename detection logic above already handled the cleanup, so we can proceed
					DebugLog("[VFS RandomAccessor Write] Non-sparse file with cleared ChunkedFileWriter, allowing writes via normal path: fileID=%d, fileName=%s", ra.fileID, fileObj.Name)
					// Continue to normal write path below (journal/sequential buffer)
				}
			}
		}
		// For non-.tmp files, continue to normal write path below
	} else {
		// File is .tmp, check ChunkedFileWriter first
		hasChunkedWriter := ra.chunkedWriter.Load() != nil
		if hasChunkedWriter {
			chunkedWriterVal := ra.chunkedWriter.Load()
			if chunkedWriterVal != clearedChunkedWriterMarker {
				// ChunkedFileWriter exists, use it (priority over sequential write)
				// IMPORTANT: Update lastActivity before writing to enable timeout flush
				// Timeout flush should start counting from the last write operation
				atomic.StoreInt64(&ra.lastActivity, core.Now())
				// Schedule delayed flush (will be cancelled if new writes come in)
				getDelayedFlushManager().schedule(ra, false)

				cw, ok := chunkedWriterVal.(*ChunkedFileWriter)
				if ok && cw != nil && cw.writerType == WRITER_TYPE_TMP {
					return cw.Write(offset, data)
				}
			}
		}
	}

	// OPTIMIZATION: For sparse files, check if we should use ChunkedFileWriter
	// ChunkedFileWriter can handle out-of-order writes within local sequential range efficiently
	if sparseSize > 0 && !ra.isTmpFile {
		// Check if writes are within local sequential range (a few chunks)
		chunkSize := ra.fs.chunkSize
		if chunkSize <= 0 {
			chunkSize = DefaultChunkSize
		}
		localRange := int64(LocalSequentialChunkCount) * chunkSize

		writeRangeStart := atomic.LoadInt64(&ra.writeRangeStart)
		writeRangeEnd := atomic.LoadInt64(&ra.writeRangeEnd)
		currentWriteEnd := offset + int64(len(data))

		// Check if current write is within local sequential range
		if writeRangeStart > 0 && writeRangeEnd > 0 {
			newRangeStart := writeRangeStart
			newRangeEnd := writeRangeEnd
			if offset < writeRangeStart {
				newRangeStart = offset
			}
			if currentWriteEnd > writeRangeEnd {
				newRangeEnd = currentWriteEnd
			}
			newRangeSize := newRangeEnd - newRangeStart
			if newRangeSize <= localRange {
				// Within local sequential range, use ChunkedFileWriter
				cw, err := ra.getOrCreateChunkedWriter(WRITER_TYPE_SPARSE)
				if err == nil && cw != nil {
					DebugLog("[VFS RandomAccessor Write] Using ChunkedFileWriter for sparse file: fileID=%d, offset=%d, size=%d", ra.fileID, offset, len(data))
					return cw.Write(offset, data)
				}
			}
		} else if int64(len(data)) <= localRange {
			// First write in range, check if it's small enough
			cw, err := ra.getOrCreateChunkedWriter(WRITER_TYPE_SPARSE)
			if err == nil && cw != nil {
				DebugLog("[VFS RandomAccessor Write] Using ChunkedFileWriter for sparse file (first write): fileID=%d, offset=%d, size=%d", ra.fileID, offset, len(data))
				return cw.Write(offset, data)
			}
		}
	}

	// Check if this is a .tmp file (and ChunkedFileWriter doesn't exist yet)
	// Use the immutable isTmpFile flag (set at creation time)
	// Note: ra.isTmpFile was already checked above, but we need to check again here
	// because the code path above might have returned early
	if ra.isTmpFile {
		// For .tmp files, all writes should go through ChunkedFileWriter
		// This ensures consistent data handling and avoids triggering applyRandomWritesWithSDK
		// IMPORTANT: Update lastActivity before writing to enable timeout flush
		// Timeout flush should start counting from the last write operation
		atomic.StoreInt64(&ra.lastActivity, core.Now())
		// Schedule delayed flush (will be cancelled if new writes come in)
		getDelayedFlushManager().schedule(ra, false)

		cw, err := ra.getOrCreateChunkedWriter(WRITER_TYPE_TMP)
		if err != nil {
			DebugLog("[VFS RandomAccessor Write] ERROR: Failed to get or create ChunkedFileWriter for .tmp file: fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, offset, len(data), err)
			return fmt.Errorf("failed to get or create ChunkedFileWriter for .tmp file: %w", err)
		}
		return cw.Write(offset, data)
	}

	// New file large sequential uploads (non-.tmp): prefer ChunkedFileWriter to avoid journal/seqBuffer path
	// 条件：
	// - 非 .tmp 文件
	// - 当前 fileObj 没有 DataID（新文件）
	// - 从 offset=0 开始写
	// - 首次写入的数据量足够大（>= chunkSize），明确属于「大文件顺序上传」
	// 其它场景仍然走原有 seqBuffer / buffer / journal 路径，尽量减小行为影响范围，避免影响
	// 小文件以及依赖 journal 行为的测试（如 TestJournalBasicWriteRead / TestJournalRandomWrites 等）。
	if !ra.isTmpFile && (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID) && fileObj.Size == 0 && offset == 0 {
		chunkSize := ra.fs.chunkSize
		if chunkSize <= 0 {
			chunkSize = DefaultChunkSize
		}
		// 如果当前写入的数据量小于一个 chunk，则认为不是「大文件顺序上传」，保持原有路径
		if int64(len(data)) >= chunkSize {
			atomic.StoreInt64(&ra.lastActivity, core.Now())
			getDelayedFlushManager().schedule(ra, false)

			cw, err := ra.getOrCreateChunkedWriter(WRITER_TYPE_SEQ)
			if err == nil && cw != nil {
				DebugLog("[VFS RandomAccessor Write] Using WRITER_TYPE_SEQ ChunkedFileWriter for new sequential upload: fileID=%d, offset=%d, size=%d", ra.fileID, offset, len(data))
				return cw.Write(offset, data)
			}
			DebugLog("[VFS RandomAccessor Write] WARNING: Failed to create WRITER_TYPE_SEQ ChunkedFileWriter, fallback to seqBuffer/random: fileID=%d, error=%v", ra.fileID, err)
		}
	}

	// Check if in sequential write mode (only for non-.tmp files)
	if ra.seqBuffer != nil {
		ra.seqBuffer.mu.Lock()
		seqOffset := ra.seqBuffer.offset
		seqClosed := ra.seqBuffer.closed
		ra.seqBuffer.mu.Unlock()

		if !seqClosed {
			// Check if still sequential write (continue from current position)
			if offset == seqOffset {
				// Sequential write, use optimized path
				return ra.writeSequential(offset, data)
			} else if offset < seqOffset {
				// Write backwards, switch to random write mode
				// OPTIMIZATION: Only flush if buffer is full, otherwise defer to Release
				ra.seqBuffer.mu.Lock()
				bufferSize := int64(len(ra.seqBuffer.buffer))
				chunkSize := ra.seqBuffer.chunkSize
				ra.seqBuffer.mu.Unlock()

				if bufferSize >= chunkSize {
					// Buffer is full, flush it now
					if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
						DebugLog("[VFS RandomAccessor Write] ERROR: Failed to flush sequential buffer (chunk full, backwards write): fileID=%d, offset=%d, seqOffset=%d, size=%d, error=%v", ra.fileID, offset, seqOffset, len(data), flushErr)
						return flushErr
					}
				} else {
					// Buffer is not full, defer flush to Release
					DebugLog("[VFS RandomAccessor Write] Deferring sequential buffer flush (backwards write, buffer not full): fileID=%d, offset=%d, seqOffset=%d, bufferSize=%d, chunkSize=%d",
						ra.fileID, offset, seqOffset, bufferSize, chunkSize)
				}
				ra.seqBuffer.mu.Lock()
				ra.seqBuffer.closed = true
				ra.seqBuffer.mu.Unlock()
			} else {
				// Skipped some positions, switch to random write mode
				// OPTIMIZATION: Only flush if buffer is full, otherwise defer to Release
				ra.seqBuffer.mu.Lock()
				bufferSize := int64(len(ra.seqBuffer.buffer))
				chunkSize := ra.seqBuffer.chunkSize
				ra.seqBuffer.mu.Unlock()

				if bufferSize >= chunkSize {
					// Buffer is full, flush it now
					if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
						DebugLog("[VFS RandomAccessor Write] ERROR: Failed to flush sequential buffer (chunk full, skipped positions): fileID=%d, offset=%d, seqOffset=%d, size=%d, error=%v", ra.fileID, offset, seqOffset, len(data), flushErr)
						return flushErr
					}
				} else {
					// Buffer is not full, defer flush to Release
					DebugLog("[VFS RandomAccessor Write] Deferring sequential buffer flush (skipped positions, buffer not full): fileID=%d, offset=%d, seqOffset=%d, bufferSize=%d, chunkSize=%d",
						ra.fileID, offset, seqOffset, bufferSize, chunkSize)
				}
				ra.seqBuffer.mu.Lock()
				ra.seqBuffer.closed = true
				ra.seqBuffer.mu.Unlock()
			}
		}
	}

	// Initialize sequential write buffer
	// NOTE: This code should not execute for .tmp files or when ChunkedFileWriter exists (they should have returned above)
	// But we keep the check here for safety in case fileObj cache is stale
	if len(data) > 0 {
		// Reuse cached fileObj (already loaded above)
		if fileObj != nil {
			// Re-check if ChunkedFileWriter exists (should have returned above, but check for safety)
			// Lock-free check using atomic.Value
			chunkedWriterVal := ra.chunkedWriter.Load()
			hasChunkedWriter := chunkedWriterVal != nil && chunkedWriterVal != clearedChunkedWriterMarker
			if hasChunkedWriter {
				// ChunkedFileWriter exists, this should not happen here (should have returned above)
				// But handle it for safety - don't initialize sequential buffer
				DebugLog("[VFS RandomAccessor Write] WARNING: ChunkedFileWriter exists but reached sequential buffer init, this should not happen: fileID=%d", ra.fileID)
				// Fall through to random write mode
			} else {
				// No ChunkedFileWriter, can proceed with sequential buffer initialization
				// OPTIMIZATION: Support sequential append after flush
				// If offset == fileObj.Size, this is sequential append, should continue using sequential buffer
				isSequentialAppend := offset == fileObj.Size && fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID
				isNewFile := offset == 0 && (fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID)

				var initErr error
				if isNewFile {
					// File has no data, can initialize sequential write buffer
					if initErr = ra.initSequentialBuffer(false); initErr == nil {
						// Initialization succeeded, use sequential write
						return ra.writeSequential(offset, data)
					}
					// Initialization failed, fallback to random write
					DebugLog("[VFS RandomAccessor Write] WARNING: Failed to initialize sequential buffer, falling back to random write: fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, offset, len(data), initErr)
				} else if isSequentialAppend {
					// Sequential append after flush - reset or reinitialize sequential buffer
					if ra.seqBuffer != nil {
						ra.seqBuffer.mu.Lock()
						if ra.seqBuffer.closed {
							// Sequential buffer was closed after flush, reset it for continued sequential writes
							DebugLog("[VFS RandomAccessor Write] Resetting sequential buffer for continued sequential append: fileID=%d, offset=%d, fileSize=%d", ra.fileID, offset, fileObj.Size)
							ra.seqBuffer.closed = false
							ra.seqBuffer.offset = fileObj.Size
							ra.clearSeqBuffer() // Clear buffer and shrink capacity if needed
							ra.seqBuffer.hasData = false
							// Update dataID to current file's DataID for continued writes
							ra.seqBuffer.dataID = fileObj.DataID
							// Reset DataInfo for new chunk sequence
							ra.seqBuffer.dataInfo.OrigSize = 0
							ra.seqBuffer.dataInfo.Size = 0
							ra.seqBuffer.sn = 0
							ra.seqBuffer.mu.Unlock()
							return ra.writeSequential(offset, data)
						} else {
							// Sequential buffer is still active, use it directly
							ra.seqBuffer.mu.Unlock()
							return ra.writeSequential(offset, data)
						}
					} else {
						// No sequential buffer exists, but this is sequential append
						// For files with existing data, we should use journal for safety
						// But user wants sequential buffer, so we'll initialize it with existing DataID
						DebugLog("[VFS RandomAccessor Write] Sequential append but no sequential buffer, initializing with existing DataID: fileID=%d, offset=%d, dataID=%d", ra.fileID, offset, fileObj.DataID)
						initErr = ra.initSequentialBufferForAppend(fileObj)
						if initErr == nil {
							return ra.writeSequential(offset, data)
						}
						// Initialization failed, fallback to journal
						DebugLog("[VFS RandomAccessor Write] WARNING: Failed to initialize sequential buffer for append, falling back to journal: fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, offset, len(data), initErr)
					}
				}
			}
		}
	}

	// Random write mode: use original buffer logic
	// IMPORTANT: Double-check that ChunkedFileWriter doesn't exist before using random write buffer
	// This prevents .tmp files and sparse files from accidentally using random write when fileObj cache is stale
	// Lock-free check using atomic.Value
	chunkedWriterValInRandomMode := ra.chunkedWriter.Load()
	hasChunkedWriterInRandomMode := chunkedWriterValInRandomMode != nil && chunkedWriterValInRandomMode != clearedChunkedWriterMarker
	if hasChunkedWriterInRandomMode {
		// ChunkedFileWriter exists, this should not happen (should have returned above)
		// But handle it for safety - use ChunkedFileWriter instead of random write buffer
		// Check the type of existing ChunkedFileWriter
		if cw, ok := chunkedWriterValInRandomMode.(*ChunkedFileWriter); ok && cw != nil {
			// Use existing ChunkedFileWriter if type matches
			if (ra.isTmpFile && cw.writerType == WRITER_TYPE_TMP) ||
				(!ra.isTmpFile && (cw.writerType == WRITER_TYPE_SPARSE || cw.writerType == WRITER_TYPE_SEQ)) {
				DebugLog("[VFS RandomAccessor Write] WARNING: ChunkedFileWriter exists but reached random write mode, using existing ChunkedFileWriter: fileID=%d, writerType=%d", ra.fileID, cw.writerType)
				// IMPORTANT: Update lastActivity before writing to enable timeout flush
				atomic.StoreInt64(&ra.lastActivity, core.Now())
				getDelayedFlushManager().schedule(ra, false)
				return cw.Write(offset, data)
			}
			// Type mismatch - clear and recreate with correct type
			DebugLog("[VFS RandomAccessor Write] WARNING: ChunkedFileWriter type mismatch, clearing and recreating: fileID=%d, existingType=%d, isTmpFile=%v", ra.fileID, cw.writerType, ra.isTmpFile)
			ra.chunkedWriter.Store(clearedChunkedWriterMarker)
		}

		// Create ChunkedFileWriter with correct type
		var writerType WriterType
		if ra.isTmpFile {
			writerType = WRITER_TYPE_TMP
		} else {
			// Check if sparse file
			sparseSize := ra.getSparseSize()
			if sparseSize > 0 {
				writerType = WRITER_TYPE_SPARSE
			} else {
				// Non-sparse, non-.tmp: treat as sequential large upload, use WRITER_TYPE_SEQ
				writerType = WRITER_TYPE_SEQ
			}
		}

		if writerType == WRITER_TYPE_TMP || writerType == WRITER_TYPE_SPARSE || writerType == WRITER_TYPE_SEQ {
			DebugLog("[VFS RandomAccessor Write] WARNING: ChunkedFileWriter exists but reached random write mode, recreating with correct type: fileID=%d, writerType=%d", ra.fileID, writerType)
			// IMPORTANT: Update lastActivity before writing to enable timeout flush
			atomic.StoreInt64(&ra.lastActivity, core.Now())
			getDelayedFlushManager().schedule(ra, false)

			cw, err := ra.getOrCreateChunkedWriter(writerType)
			if err != nil {
				DebugLog("[VFS RandomAccessor Write] ERROR: Failed to get or create ChunkedFileWriter (random mode fallback): fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, offset, len(data), err)
				return fmt.Errorf("failed to get or create ChunkedFileWriter: %w", err)
			}
			return cw.Write(offset, data)
		}
	}

	// Optimization: reduce data copying, only copy when necessary
	// Check if exceeds capacity (optimized: check early to avoid out of bounds)
	config := core.GetWriteBufferConfig()

	// Detect sequential write pattern (even if file already has data)
	// Sequential writes are writes that continue from the last write position
	lastOffset := atomic.LoadInt64(&ra.lastOffset)
	isSequentialWrite := false
	if lastOffset >= 0 {
		// Check if this write continues from the last write position
		if offset == lastOffset {
			isSequentialWrite = true
		}
	} else {
		// First write, check if it starts from file end (append mode)
		// Reuse cached fileObj (already loaded above)
		if fileObj == nil {
			fileObj, err = ra.getFileObj()
			if err != nil {
				DebugLog("[VFS RandomAccessor Write] ERROR: Failed to get file object (for sequential detection): fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, offset, len(data), err)
				return err
			}
		}
		if offset == fileObj.Size {
			isSequentialWrite = true
		}
	}

	// Aggressive optimization: for sparse files (pre-allocated), use larger buffer threshold to reduce flush frequency
	// This is critical for qBittorrent random write performance
	// For small files, use larger buffer to allow more batching (balanced approach)
	sparseSizeVal := atomic.LoadInt64(&ra.sparseSize)
	isSparseFile := sparseSizeVal > 0
	maxBufferSize := config.MaxBufferSize

	// Check if this is a small file (likely to use batch write)
	// Balanced aggressive: use larger buffer for small files to allow more operations before flush
	isSmallFile := fileObj != nil && fileObj.Size > 0 && fileObj.Size < 1536<<10 // 1.5MB threshold

	if isSparseFile {
		// For sparse files, allow larger buffer (2x) to reduce flush frequency
		// This significantly improves performance for random writes
		maxBufferSize = config.MaxBufferSize * 2
	} else if isSequentialWrite {
		// Balanced aggressive: For sequential writes, use larger buffer (4x for small files, 3x for others)
		// This allows more small files to accumulate before flushing, reducing database operations
		if isSmallFile {
			maxBufferSize = config.MaxBufferSize * 4 // Aggressive for small files
		} else {
			maxBufferSize = config.MaxBufferSize * 3
		}
	} else if isSmallFile {
		// Balanced aggressive: For small files even in random write mode, use larger buffer (3x)
		// This allows more operations to accumulate before flush
		maxBufferSize = config.MaxBufferSize * 3
	} else {
		// For random writes on large files, also use larger buffer (3x) to reduce flush frequency
		// This ensures all writes can be accumulated before flush, preventing partial data writes
		maxBufferSize = config.MaxBufferSize * 3
	}

	// Reserve space for this write operation first
	writeIndex := atomic.AddInt64(&ra.buffer.writeIndex, 1) - 1

	// Check if we exceeded capacity AFTER reserving space
	// This ensures we don't flush prematurely before adding the current write
	if writeIndex >= int64(len(ra.buffer.operations)) {
		// Operations array is full, need to flush before we can write
		// Rollback writeIndex since we can't write to this position
		atomic.AddInt64(&ra.buffer.writeIndex, -1)
		DebugLog("[VFS RandomAccessor Write] Operations array full, forcing flush for fileID=%d, writeIndex=%d, operationsLen=%d", ra.fileID, writeIndex, len(ra.buffer.operations))
		_, err := ra.Flush()
		if err != nil {
			DebugLog("[VFS RandomAccessor Write] ERROR: Failed to flush buffer for fileID=%d: %v", ra.fileID, err)
			if err == core.ERR_QUOTA_EXCEED {
				DebugLog("[VFS RandomAccessor Write] ERROR: Quota exceeded during flush for fileID=%d", ra.fileID)
			}
			return err
		}
		// After flush, reacquire write position (writeIndex should have been reset to 0 by Flush)
		writeIndex = atomic.AddInt64(&ra.buffer.writeIndex, 1) - 1
	}

	// Check if totalSize would exceed limit AFTER adding this write
	// This ensures we flush all accumulated writes together, not just partial data
	// IMPORTANT: Only flush if adding this write would exceed the limit, not if it's already at the limit
	// This prevents premature flushing when buffer is exactly at the limit
	currentTotalSize := atomic.LoadInt64(&ra.buffer.totalSize)
	newTotalSize := currentTotalSize + int64(len(data))
	if newTotalSize > maxBufferSize {
		// Total size would exceed limit, flush current buffer first
		// But we've already reserved writeIndex, so we need to flush what we have
		// Rollback writeIndex since we'll flush first
		atomic.AddInt64(&ra.buffer.writeIndex, -1)
		DebugLog("[VFS RandomAccessor Write] Total size would exceed limit, forcing flush for fileID=%d, currentTotalSize=%d, newDataSize=%d, newTotalSize=%d, maxBufferSize=%d", ra.fileID, currentTotalSize, len(data), newTotalSize, maxBufferSize)
		_, err := ra.Flush()
		if err != nil {
			DebugLog("[VFS RandomAccessor Write] ERROR: Failed to flush buffer for fileID=%d: %v", ra.fileID, err)
			if err == core.ERR_QUOTA_EXCEED {
				DebugLog("[VFS RandomAccessor Write] ERROR: Quota exceeded during flush for fileID=%d", ra.fileID)
			}
			return err
		}
		// After flush, reacquire write position (writeIndex should have been reset to 0 by Flush)
		writeIndex = atomic.AddInt64(&ra.buffer.writeIndex, 1) - 1
	}

	// Optimization: use atomic operation to update totalSize, reduce lock contention
	atomic.AddInt64(&ra.buffer.totalSize, int64(len(data)))

	// Write new data
	// Optimization: reuse existing Data slice if capacity is sufficient (reduce allocations for fragmented writes)
	ra.buffer.operations[writeIndex].Offset = offset
	if cap(ra.buffer.operations[writeIndex].Data) >= len(data) {
		// Reuse existing slice if capacity is sufficient
		ra.buffer.operations[writeIndex].Data = ra.buffer.operations[writeIndex].Data[:len(data)]
		copy(ra.buffer.operations[writeIndex].Data, data)
	} else {
		// Allocate new slice only if capacity is insufficient
		ra.buffer.operations[writeIndex].Data = make([]byte, len(data))
		copy(ra.buffer.operations[writeIndex].Data, data)
	}

	// Update last write offset for sequential write detection
	atomic.StoreInt64(&ra.lastOffset, offset+int64(len(data)))

	// Aggressive: reduce delayed flush scheduling frequency
	// Only schedule if buffer is getting quite full (80%+) or it's a sequential write with significant data (70%+)
	// This allows more small files to accumulate before flushing, reducing database operations
	bufferUsage := float64(atomic.LoadInt64(&ra.buffer.totalSize)) / float64(maxBufferSize)
	if isSequentialWrite && bufferUsage > 0.7 {
		// Sequential write with significant buffer usage, schedule delayed flush
		ra.requestDelayedFlush(false)
	} else if !isSequentialWrite && bufferUsage > 0.85 {
		// Fragmented write, schedule when buffer is 85% full (more aggressive than before)
		ra.requestDelayedFlush(false)
	}

	return nil
}

// initSequentialBuffer initializes sequential write buffer
func (ra *RandomAccessor) initSequentialBuffer(force bool) error {
	fileObj, err := ra.getFileObj()
	if err != nil {
		DebugLog("[VFS RandomAccessor initSequentialBuffer] ERROR: Failed to get file object: fileID=%d, error=%v", ra.fileID, err)
		return err
	}

	// Create new data object for sequential buffer (only when file has no data)
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		DebugLog("[VFS RandomAccessor initSequentialBuffer] ERROR: File already has data: fileID=%d, dataID=%d", ra.fileID, fileObj.DataID)
		return fmt.Errorf("file already has data")
	}
	return ra.initSequentialBufferWithNewData()
}

func (ra *RandomAccessor) initSequentialBufferWithNewData() error {
	// Get file object to check if it's a .tmp file and determine chunk size

	// Determine chunk size based on bucket configuration (fixed)
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize // 10MB default
	}

	// Create new data object
	newDataID := core.NewID()

	// Initialize DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: 0,
		Size:     0,
		XXH3:     0,
		SHA256_0: 0,
		SHA256_1: 0,
		SHA256_2: 0,
		SHA256_3: 0,
		Kind:     0,
	}

	// Set compression and encryption flags (if enabled)
	// Get from OrcasFS configuration, not from bucket config
	cmprWay := getCmprWayForFS(ra.fs)
	endecWay := getEndecWayForFS(ra.fs)
	DebugLog("[VFS initSequentialBufferWithNewData] FS config: fileID=%d, bktID=%d, CmprWay=0x%x, EndecWay=0x%x", ra.fileID, ra.fs.bktID, cmprWay, endecWay)
	if cmprWay > 0 {
		dataInfo.Kind |= cmprWay
	}
	if endecWay > 0 {
		dataInfo.Kind |= endecWay
		DebugLog("[VFS initSequentialBufferWithNewData] Set encryption flag: fileID=%d, EndecWay=0x%x, Kind=0x%x", ra.fileID, endecWay, dataInfo.Kind)
	}
	DebugLog("[VFS initSequentialBufferWithNewData] Final DataInfo Kind: fileID=%d, Kind=0x%x", ra.fileID, dataInfo.Kind)

	// Get SequentialWriteBuffer from pool to reduce allocations
	// Buffer starts from tier 1 (128KB), will upgrade automatically as needed
	ra.seqBuffer = getSequentialBuffer()
	ra.seqBuffer.fileID = ra.fileID
	ra.seqBuffer.dataID = newDataID
	ra.seqBuffer.sn = 0
	ra.seqBuffer.chunkSize = chunkSize
	ra.seqBuffer.offset = 0
	ra.seqBuffer.hasData = false
	ra.seqBuffer.closed = false
	ra.seqBuffer.dataInfo = dataInfo

	// Update file object with new DataID immediately (but keep Size unchanged)
	// This allows the DataID to be visible before Flush, while Size will be updated on Flush
	fileObj, err := ra.getFileObj()
	if err == nil && fileObj != nil {
		updateFileObj := &core.ObjectInfo{
			ID:     fileObj.ID,
			PID:    fileObj.PID,
			Type:   fileObj.Type,
			Name:   fileObj.Name,
			DataID: newDataID,
			Size:   fileObj.Size, // Keep existing size (will be updated on Flush)
			MTime:  core.Now(),
		}
		if _, putErr := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj}); putErr != nil {
			DebugLog("[VFS initSequentialBufferWithNewData] WARNING: Failed to update file DataID: %v", putErr)
		} else {
			// Update cache immediately
			fileObjCache.Put(ra.fileObjKey, updateFileObj)
			ra.fileObj.Store(updateFileObj)
			DebugLog("[VFS initSequentialBufferWithNewData] Created new DataID and updated DB: fileID=%d, oldDataID=%d, newDataID=%d, size=%d",
				ra.fileID, fileObj.DataID, newDataID, fileObj.Size)
		}
	}

	return nil
}

// initSequentialBufferForAppend initializes sequential buffer for appending to existing file
// This is used when flush happened but file is still open and we want to continue sequential writes
func (ra *RandomAccessor) initSequentialBufferForAppend(fileObj *core.ObjectInfo) error {
	if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
		return fmt.Errorf("file has no data, use initSequentialBuffer instead")
	}

	// Determine chunk size based on bucket configuration
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize // 10MB default
	}

	// Create new DataID for the appended chunks (they will be merged later)
	newDataID := core.NewID()

	// Initialize DataInfo for new chunks
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: 0,
		Size:     0,
		XXH3:     0,
		SHA256_0: 0,
		SHA256_1: 0,
		SHA256_2: 0,
		SHA256_3: 0,
		Kind:     0,
	}

	// Set compression and encryption flags (if enabled)
	cmprWay := getCmprWayForFS(ra.fs)
	endecWay := getEndecWayForFS(ra.fs)
	if cmprWay > 0 {
		dataInfo.Kind |= cmprWay
	}
	if endecWay > 0 {
		dataInfo.Kind |= endecWay
	}

	// Get SequentialWriteBuffer from pool to reduce allocations
	// Buffer starts from tier 1 (128KB), will upgrade automatically as needed
	ra.seqBuffer = getSequentialBuffer()
	ra.seqBuffer.fileID = ra.fileID
	ra.seqBuffer.dataID = newDataID // New DataID for appended chunks
	ra.seqBuffer.sn = 0             // Start from chunk 0 for new chunks
	ra.seqBuffer.chunkSize = chunkSize
	ra.seqBuffer.offset = fileObj.Size // Start from current file size
	ra.seqBuffer.hasData = false
	ra.seqBuffer.closed = false
	ra.seqBuffer.dataInfo = dataInfo

	DebugLog("[VFS initSequentialBufferForAppend] Initialized sequential buffer for append: fileID=%d, offset=%d, newDataID=%d, baseDataID=%d", ra.fileID, fileObj.Size, newDataID, fileObj.DataID)
	return nil
}

// writeSequential writes data sequentially (optimized path)
func (ra *RandomAccessor) writeSequential(offset int64, data []byte) error {
	if ra.seqBuffer == nil {
		return fmt.Errorf("sequential buffer not available")
	}

	ra.seqBuffer.mu.Lock()
	defer ra.seqBuffer.mu.Unlock()

	if ra.seqBuffer.closed {
		return fmt.Errorf("sequential buffer not available")
	}

	// Ensure offset is sequential
	if offset != ra.seqBuffer.offset {
		return fmt.Errorf("non-sequential write detected")
	}

	dataLeft := data
	for len(dataLeft) > 0 {
		// Calculate remaining space in current chunk
		remainingInChunk := ra.seqBuffer.chunkSize - int64(len(ra.seqBuffer.buffer))
		if remainingInChunk <= 0 {
			// Current chunk is full, write and clear
			// Need to unlock before calling flushSequentialChunk (which will lock again)
			ra.seqBuffer.mu.Unlock()
			if err := ra.flushSequentialChunk(); err != nil {
				DebugLog("[VFS RandomAccessor writeSequential] ERROR: Failed to flush sequential chunk (chunk full): fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, offset, len(data), err)
				ra.seqBuffer.mu.Lock()
				return err
			}
			ra.seqBuffer.mu.Lock()
			remainingInChunk = ra.seqBuffer.chunkSize
		}

		// Calculate how much data can be written this time
		writeSize := int64(len(dataLeft))
		if writeSize > remainingInChunk {
			writeSize = remainingInChunk
		}

		// Check if buffer needs upgrade before writing
		currentTier := getBufferTier(cap(ra.seqBuffer.buffer))
		neededCap := int64(len(ra.seqBuffer.buffer)) + writeSize

		// Upgrade buffer if needed capacity exceeds current tier
		if neededCap > int64(cap(ra.seqBuffer.buffer)) {
			var targetTier int
			if neededCap > SequentialBufferTier2Cap {
				targetTier = 3 // Need tier 3 (10MB)
			} else if neededCap > SequentialBufferTier1Cap {
				targetTier = 2 // Need tier 2 (1MB)
			} else {
				targetTier = 1 // Stay at tier 1 (128KB)
			}

			if targetTier > currentTier {
				// Upgrade buffer to larger tier
				ra.seqBuffer.buffer = upgradeBuffer(ra.seqBuffer.buffer, targetTier)
				DebugLog("[VFS writeSequential] Upgraded buffer: fileID=%d, dataID=%d, fromTier=%d, toTier=%d, neededCap=%d",
					ra.fileID, ra.seqBuffer.dataID, currentTier, targetTier, neededCap)
			}
		}

		// Write to current chunk buffer
		ra.seqBuffer.buffer = append(ra.seqBuffer.buffer, dataLeft[:writeSize]...)
		ra.seqBuffer.offset += writeSize
		ra.seqBuffer.hasData = true
		dataLeft = dataLeft[writeSize:]

		// If chunk is full, write immediately
		if int64(len(ra.seqBuffer.buffer)) >= ra.seqBuffer.chunkSize {
			// Need to unlock before calling flushSequentialChunk (which will lock again)
			ra.seqBuffer.mu.Unlock()
			if err := ra.flushSequentialChunk(); err != nil {
				DebugLog("[VFS RandomAccessor writeSequential] ERROR: Failed to flush sequential chunk (immediate flush): fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, offset, len(data), err)
				ra.seqBuffer.mu.Lock()
				return err
			}
			ra.seqBuffer.mu.Lock()
		}
	}

	return nil
}

// getBufferFromTier gets a buffer from the appropriate tier pool
// Returns buffer from tier 1 (128KB) by default
func getBufferFromTier(tier int) []byte {
	var pool *limitedPool
	switch tier {
	case 1:
		pool = bufferTier1Pool
	case 2:
		pool = bufferTier2Pool
	case 3:
		pool = bufferTier3Pool
	default:
		pool = bufferTier1Pool
	}

	if buf := pool.Get(); buf != nil {
		return buf.([]byte)[:0] // Reset length but keep capacity
	}

	// Pool is empty or at capacity, create new buffer
	switch tier {
	case 1:
		return make([]byte, 0, SequentialBufferTier1Cap)
	case 2:
		return make([]byte, 0, SequentialBufferTier2Cap)
	case 3:
		return make([]byte, 0, SequentialBufferTier3Cap)
	default:
		return make([]byte, 0, SequentialBufferTier1Cap)
	}
}

// putBufferToTier returns a buffer to the appropriate tier pool
func putBufferToTier(buf []byte) {
	if buf == nil {
		return
	}

	cap := cap(buf)
	buf = buf[:0] // Reset length but keep capacity

	var pool *limitedPool
	switch {
	case cap <= SequentialBufferTier1Cap:
		pool = bufferTier1Pool
	case cap <= SequentialBufferTier2Cap:
		pool = bufferTier2Pool
	case cap <= SequentialBufferTier3Cap:
		pool = bufferTier3Pool
	default:
		// Buffer is larger than tier 3, don't pool it (let GC handle it)
		return
	}

	pool.Put(buf)
}

// upgradeBuffer upgrades buffer to a larger tier, copying data and returning old buffer
func upgradeBuffer(oldBuf []byte, targetTier int) []byte {
	if oldBuf == nil {
		return getBufferFromTier(targetTier)
	}

	// Get new buffer from target tier
	newBuf := getBufferFromTier(targetTier)

	// Copy data if there's any
	if len(oldBuf) > 0 {
		// Ensure new buffer has enough capacity
		if cap(newBuf) < len(oldBuf) {
			// New buffer from pool is too small, create larger one
			putBufferToTier(newBuf) // Return the small one
			switch targetTier {
			case 2:
				newBuf = make([]byte, len(oldBuf), SequentialBufferTier2Cap)
			case 3:
				newBuf = make([]byte, len(oldBuf), SequentialBufferTier3Cap)
			default:
				newBuf = make([]byte, len(oldBuf), SequentialBufferTier1Cap)
			}
		}
		copy(newBuf, oldBuf)
		newBuf = newBuf[:len(oldBuf)]
	}

	// Return old buffer to its tier pool
	putBufferToTier(oldBuf)

	return newBuf
}

// downgradeBuffer downgrades buffer to a smaller tier, copying data and returning old buffer
func downgradeBuffer(oldBuf []byte, targetTier int) []byte {
	if oldBuf == nil {
		return getBufferFromTier(targetTier)
	}

	// Get new buffer from target tier
	newBuf := getBufferFromTier(targetTier)

	// Copy data if it fits in the smaller buffer
	if len(oldBuf) > 0 {
		if len(oldBuf) <= cap(newBuf) {
			copy(newBuf, oldBuf)
			newBuf = newBuf[:len(oldBuf)]
		} else {
			// Data doesn't fit, keep old buffer (shouldn't happen in practice)
			putBufferToTier(newBuf) // Return the small one
			return oldBuf
		}
	}

	// Return old buffer to its tier pool
	putBufferToTier(oldBuf)

	return newBuf
}

// getBufferTier determines which tier a buffer capacity belongs to
func getBufferTier(cap int) int {
	switch {
	case cap <= SequentialBufferTier1Cap:
		return 1
	case cap <= SequentialBufferTier2Cap:
		return 2
	case cap <= SequentialBufferTier3Cap:
		return 3
	default:
		return 3 // Larger than tier 3, treat as tier 3
	}
}

// clearSeqBuffer clears the sequential buffer and releases large buffers back to pool
// This reduces memory usage when many files are open simultaneously
func (ra *RandomAccessor) clearSeqBuffer() {
	if ra.seqBuffer == nil {
		return
	}

	oldCap := cap(ra.seqBuffer.buffer)
	currentTier := getBufferTier(oldCap)

	// If buffer is tier 3 (10MB), return it to pool and reset to tier 1 (128KB).
	// This avoids an extra copy (tier3->tier2) and keeps memory low when many files are open.
	if currentTier == 3 {
		putBufferToTier(ra.seqBuffer.buffer)
		ra.seqBuffer.buffer = getBufferFromTier(1)
		DebugLog("[VFS clearSeqBuffer] Released tier 3 buffer and reset to tier 1: fileID=%d, dataID=%d, oldCap=%d, newCap=%d",
			ra.fileID, ra.seqBuffer.dataID, oldCap, cap(ra.seqBuffer.buffer))
		return
	}

	// For tier 1/2, just clear and keep capacity for better throughput on continued sequential writes.
	ra.seqBuffer.buffer = ra.seqBuffer.buffer[:0]
}

// getSequentialBuffer gets a SequentialWriteBuffer from pool or creates a new one
// Buffer comes from tier 1 pool (128KB) initially
func getSequentialBuffer() *SequentialWriteBuffer {
	if pooled := sequentialBufferPool.Get(); pooled != nil {
		sb := pooled.(*SequentialWriteBuffer)
		// Reset fields
		sb.fileID = 0
		sb.dataID = 0
		sb.sn = 0
		sb.chunkSize = 0
		sb.offset = 0
		sb.hasData = false
		sb.closed = false
		sb.dataInfo = nil
		sb.xxh3Hash = nil
		sb.sha256Hash = nil

		// Return old buffer to appropriate tier pool and get tier 1 buffer
		if sb.buffer != nil {
			putBufferToTier(sb.buffer) // Return old buffer to pool
		}
		sb.buffer = getBufferFromTier(1) // Get tier 1 buffer (128KB)
		return sb
	}

	// Create new SequentialWriteBuffer with tier 1 buffer
	return &SequentialWriteBuffer{
		buffer: getBufferFromTier(1),
	}
}

// putSequentialBuffer returns a SequentialWriteBuffer to pool for reuse
// Buffer is returned to appropriate tier pool based on its capacity
func putSequentialBuffer(sb *SequentialWriteBuffer) {
	if sb == nil {
		return
	}

	// Return buffer to appropriate tier pool
	putBufferToTier(sb.buffer)
	sb.buffer = nil // Clear reference

	// Return SequentialWriteBuffer object to pool (without buffer)
	sequentialBufferPool.Put(sb)
}

// flushSequentialChunk flushes current sequential write chunk (writes a complete chunk)
func (ra *RandomAccessor) flushSequentialChunk() error {
	if ra.seqBuffer == nil {
		return nil
	}

	ra.seqBuffer.mu.Lock()
	defer ra.seqBuffer.mu.Unlock()

	return ra.flushSequentialChunkLocked()
}

// flushSequentialChunkLocked flushes current sequential write chunk (assumes lock is already held)
func (ra *RandomAccessor) flushSequentialChunkLocked() error {
	if ra.seqBuffer == nil || len(ra.seqBuffer.buffer) == 0 {
		return nil
	}

	chunkData := ra.seqBuffer.buffer
	isFirstChunk := ra.seqBuffer.sn == 0

	// Process first chunk: check file type and compression effect using ShouldCompressFile
	if isFirstChunk {
		cmprWay := getCmprWayForFS(ra.fs)
		if cmprWay > 0 && len(chunkData) > 0 {
			// Use core.ShouldCompressFile to detect if file should be compressed
			// Pass empty filename since we're checking by file header only
			objInfo, ok := ra.fileObj.Load().(*core.ObjectInfo)
			if ok && !core.ShouldCompressFile(objInfo.Name, chunkData) {
				// Not a compressible file, remove compression flag
				ra.seqBuffer.dataInfo.Kind &= ^core.DATA_CMPR_MASK
				DebugLog("[VFS flushSequentialChunk] Not compressible file, remove compression flag: fileID=%d, dataID=%d, sn=%d, Kind=0x%x",
					ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, ra.seqBuffer.dataInfo.Kind)
			}
		}
	}

	// Update hash values incrementally
	if ra.seqBuffer.xxh3Hash == nil {
		ra.seqBuffer.xxh3Hash = xxh3.New()
		ra.seqBuffer.sha256Hash = sha256.New()
	}

	// Calculate HdrXXH3 for first chunk (first 100KB or full if smaller)
	if isFirstChunk && ra.seqBuffer.dataInfo.HdrXXH3 == 0 {
		if len(chunkData) > core.DefaultHdrSize {
			ra.seqBuffer.dataInfo.HdrXXH3 = int64(xxh3.Hash(chunkData[0:core.DefaultHdrSize]))
		} else {
			ra.seqBuffer.dataInfo.HdrXXH3 = int64(xxh3.Hash(chunkData))
		}
	}

	ra.seqBuffer.xxh3Hash.Write(chunkData)
	ra.seqBuffer.sha256Hash.Write(chunkData)
	ra.seqBuffer.dataInfo.XXH3 = int64(ra.seqBuffer.xxh3Hash.Sum64())
	sha256Sum := ra.seqBuffer.sha256Hash.Sum(nil)
	ra.seqBuffer.dataInfo.SHA256_0 = int64(binary.BigEndian.Uint64(sha256Sum[0:8]))
	ra.seqBuffer.dataInfo.SHA256_1 = int64(binary.BigEndian.Uint64(sha256Sum[8:16]))
	ra.seqBuffer.dataInfo.SHA256_2 = int64(binary.BigEndian.Uint64(sha256Sum[16:24]))
	ra.seqBuffer.dataInfo.SHA256_3 = int64(binary.BigEndian.Uint64(sha256Sum[24:32]))
	ra.seqBuffer.dataInfo.OrigSize += int64(len(chunkData))
	DebugLog("[VFS flushSequentialChunk] Original chunk: fileID=%d, chunk=%d, OrigSize=%d, Kind=0x%x (CMPR=%v, ENDEC=%v)",
		ra.fileID, len(chunkData), ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Kind,
		ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0,
		ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0)

	// Process chunk data using core.ProcessData (compression + encryption)
	encodedChunk, err := core.ProcessData(chunkData, &ra.seqBuffer.dataInfo.Kind, getCmprQltyForFS(ra.fs), getEndecKeyForFS(ra.fs), isFirstChunk)
	if err != nil {
		// Processing failed, use original chunk
		DebugLog("[VFS flushSequentialChunk] ProcessData failed: fileID=%d, dataID=%d, sn=%d, error=%v", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, err)
		encodedChunk = chunkData
	}
	if len(encodedChunk) == 0 && len(chunkData) > 0 {
		DebugLog("[VFS flushSequentialChunk] ERROR: ProcessData returned empty chunk but original has data: fileID=%d, dataID=%d, sn=%d, originalSize=%d",
			ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(chunkData))
		return fmt.Errorf("ProcessData returned empty chunk for fileID=%d, dataID=%d, sn=%d", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn)
	}

	// Update size (if compressed or encrypted)
	originalChunkSize := len(chunkData)
	encodedChunkSize := len(encodedChunk)

	// IMPORTANT: Only accumulate Size once (not twice as before)
	// Size represents the actual stored size (after compression/encryption)
	ra.seqBuffer.dataInfo.Size += int64(encodedChunkSize)
	DebugLog("[VFS flushSequentialChunk] Chunk sizes: fileID=%d, original=%d, encoded=%d, Kind=0x%x",
		ra.fileID, originalChunkSize, encodedChunkSize, ra.seqBuffer.dataInfo.Kind)
	DebugLog("[VFS flushSequentialChunk] Updated Size: fileID=%d, Size=%d, OrigSize=%d (CMPR=%v, ENDEC=%v)",
		ra.fileID, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.dataInfo.OrigSize,
		ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0,
		ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0)

	// Write data block
	DebugLog("[VFS flushSequentialChunk] Writing chunk: fileID=%d, dataID=%d, sn=%d, originalSize=%d, encodedSize=%d",
		ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(chunkData), len(encodedChunk))
	if len(encodedChunk) == 0 {
		DebugLog("[VFS flushSequentialChunk] ERROR: encodedChunk is empty: fileID=%d, dataID=%d, sn=%d, originalSize=%d",
			ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(chunkData))
		return fmt.Errorf("encodedChunk is empty for fileID=%d, dataID=%d, sn=%d", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn)
	}
	if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, ra.seqBuffer.dataID, ra.seqBuffer.sn, encodedChunk); err != nil {
		DebugLog("[VFS flushSequentialChunk] ERROR: Failed to put data for fileID=%d, dataID=%d, sn=%d, size=%d: %v", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(encodedChunk), err)
		if err == core.ERR_QUOTA_EXCEED {
			DebugLog("[VFS flushSequentialChunk] ERROR: Quota exceeded for fileID=%d", ra.fileID)
		}
		return err
	}
	DebugLog("[VFS flushSequentialChunk] Successfully wrote chunk: fileID=%d, dataID=%d, sn=%d, encodedSize=%d, OrigSize=%d, Size=%d",
		ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(encodedChunk), ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size)

	ra.seqBuffer.sn++
	ra.clearSeqBuffer()         // Clear buffer and shrink capacity if needed
	ra.seqBuffer.hasData = true // Mark that data has been written (OrigSize > 0)

	DebugLog("[VFS flushSequentialChunk] After flush: fileID=%d, dataID=%d, sn=%d, bufferLen=%d, hasData=%v, OrigSize=%d, Size=%d",
		ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.sn, len(ra.seqBuffer.buffer), ra.seqBuffer.hasData,
		ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size)

	// For continuous sequential writes (large files), periodically update DataInfo and file object
	// This ensures progress is visible even when file is not closed yet
	// Update every 10 chunks (approximately every 100MB) to balance visibility and performance
	// Only update if this is not the first chunk (sn > 0) and we've written multiple chunks
	if ra.seqBuffer.sn > 0 && ra.seqBuffer.sn%10 == 0 {
		// Update DataInfo and file object periodically for continuous writes
		// This allows progress tracking for large files that are continuously written
		if lh, ok := ra.fs.h.(*core.LocalHandler); ok {
			// CRITICAL: Ensure DataInfo is complete and consistent before updating
			// This is especially important for encrypted files to ensure accurate reading
			// Verify that:
			// 1. DataInfo ID matches seqBuffer.dataID
			// 2. OrigSize and Size are consistent (Size >= OrigSize if compressed/encrypted)
			// 3. Kind flags are valid (encryption flags should not change during write)
			if ra.seqBuffer.dataInfo.ID != ra.seqBuffer.dataID {
				DebugLog("[VFS flushSequentialChunk] ERROR: DataInfo ID mismatch, skipping periodic update: fileID=%d, dataID=%d, DataInfo.ID=%d", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.dataInfo.ID)
			} else if ra.seqBuffer.dataInfo.OrigSize <= 0 {
				DebugLog("[VFS flushSequentialChunk] WARNING: DataInfo OrigSize is invalid, skipping periodic update: fileID=%d, dataID=%d, OrigSize=%d", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.dataInfo.OrigSize)
			} else {
				// Validate Size consistency
				// Note: Compression may make Size < OrigSize, but encryption always makes Size >= OrigSize
				// For encrypted-only (no compression), Size should be >= OrigSize
				// For compressed-only (no encryption), Size may be < OrigSize
				// For both compressed and encrypted, Size depends on compression ratio
				hasCompression := ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0
				hasEncryption := ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0
				if hasEncryption && !hasCompression && ra.seqBuffer.dataInfo.Size < ra.seqBuffer.dataInfo.OrigSize {
					// Encrypted-only data should always have Size >= OrigSize (encryption adds overhead)
					DebugLog("[VFS flushSequentialChunk] WARNING: DataInfo Size < OrigSize for encrypted-only data, this may indicate inconsistency: fileID=%d, dataID=%d, Size=%d, OrigSize=%d, Kind=0x%x", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Kind)
				}
				if ra.seqBuffer.dataInfo.Size <= 0 {
					DebugLog("[VFS flushSequentialChunk] WARNING: DataInfo Size is invalid, this may cause read issues: fileID=%d, dataID=%d, Size=%d, OrigSize=%d", ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.dataInfo.OrigSize)
				}

				// Use async update with singleflight to prevent blocking writes and duplicate updates
				// This avoids transmission curve dips by not blocking the write path
				// Singleflight ensures only one update happens at a time per DataID
				updateKey := fmt.Sprintf("update_datainfo_%d", ra.seqBuffer.dataID)

				// Create snapshot of data for async update (to avoid race conditions)
				// This ensures we update with consistent data even if writes continue
				dataInfoSnapshot := *ra.seqBuffer.dataInfo
				fileID := ra.fileID
				dataID := ra.seqBuffer.dataID
				sn := ra.seqBuffer.sn
				fileObjKey := ra.fileObjKey
				bktID := ra.fs.bktID
				ctx := ra.fs.c

				// Launch async update in goroutine (non-blocking)
				// Singleflight will deduplicate if multiple updates are triggered simultaneously
				go func() {
					// Use singleflight to ensure only one update per DataID at a time
					// This prevents duplicate updates and ensures consistency
					_, err, _ := globalSingleFlight.Do(updateKey, func() (interface{}, error) {
						// Create a copy of DataInfo snapshot for update
						dataInfoCopy := dataInfoSnapshot

						// Update DataInfo asynchronously (non-blocking)
						_, updateErr := lh.PutDataInfo(ctx, bktID, []*core.DataInfo{&dataInfoCopy})
						if updateErr != nil {
							DebugLog("[VFS flushSequentialChunk] WARNING: Failed to update DataInfo periodically (async, non-critical): fileID=%d, dataID=%d, sn=%d, error=%v", fileID, dataID, sn, updateErr)
							return nil, updateErr
						}

						// Update cache with latest DataInfo (use copy to avoid race conditions)
						dataInfoCache.Put(dataID, &dataInfoCopy)
						// Also invalidate decodingReaderCache to ensure chunkReader uses fresh DataInfo
						decodingReaderCache.Del(dataID)

						// Also update file object size to make progress visible
						// Get file object (may need to reload if cache is stale)
						fileObjVal := ra.fileObj.Load()
						var fileObj *core.ObjectInfo
						if fileObjVal != nil {
							if obj, ok := fileObjVal.(*core.ObjectInfo); ok {
								fileObj = obj
							}
						}
						if fileObj == nil {
							// Try cache
							if cached, ok := fileObjCache.Get(fileObjKey); ok {
								if obj, ok := cached.(*core.ObjectInfo); ok {
									fileObj = obj
								}
							}
						}

						if fileObj != nil {
							// Update file object size to reflect current progress
							// CRITICAL: Ensure Size matches DataInfo.OrigSize for consistency
							updateFileObj := &core.ObjectInfo{
								ID:     fileObj.ID,
								PID:    fileObj.PID,
								DataID: dataID,
								Size:   dataInfoCopy.OrigSize, // Use OrigSize as file size (must match DataInfo.OrigSize)
								MTime:  core.Now(),
								Type:   fileObj.Type,
								Name:   fileObj.Name,
							}

							// Use SetObj to update file size without closing the file
							updateErr := lh.MetadataAdapter().SetObj(ctx, bktID, []string{"did", "s", "m"}, updateFileObj)
							if updateErr != nil {
								DebugLog("[VFS flushSequentialChunk] WARNING: Failed to update file object size periodically (async, non-critical): fileID=%d, error=%v", fileID, updateErr)
							} else {
								// Update cache with latest file object
								fileObjCache.Put(fileObjKey, updateFileObj)
								ra.fileObj.Store(updateFileObj)
								DebugLog("[VFS flushSequentialChunk] Periodically updated DataInfo and file object (async) for continuous write: fileID=%d, dataID=%d, sn=%d, OrigSize=%d, Size=%d, Kind=0x%x (CMPR=%v, ENDEC=%v)", fileID, dataID, sn, dataInfoCopy.OrigSize, dataInfoCopy.Size, dataInfoCopy.Kind, hasCompression, hasEncryption)
							}
						}

						return nil, nil
					})

					// Log error if update failed (but don't block)
					if err != nil {
						DebugLog("[VFS flushSequentialChunk] Async DataInfo update completed with error: fileID=%d, dataID=%d, error=%v", fileID, dataID, err)
					}
				}()
			}
		}
	}

	return nil
}

// flushSequentialBuffer flushes entire sequential write buffer (writes last chunk and completes)
func (ra *RandomAccessor) flushSequentialBuffer() error {
	if ra.seqBuffer == nil {
		return nil
	}

	ra.seqBuffer.mu.Lock()
	defer ra.seqBuffer.mu.Unlock()

	DebugLog("[VFS flushSequentialBuffer] Entry: fileID=%d, hasData=%v, bufferLen=%d, sn=%d, OrigSize=%d, Size=%d, dataID=%d",
		ra.fileID, ra.seqBuffer.hasData, len(ra.seqBuffer.buffer), ra.seqBuffer.sn,
		ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.dataID)

	// If no data, return directly
	if !ra.seqBuffer.hasData {
		DebugLog("[VFS flushSequentialBuffer] Early return: hasData=false, fileID=%d", ra.fileID)
		return nil
	}

	// Check if it's a small file (total size < 1MB) and no chunks have been written yet
	// If sn > 0, it means at least one chunk (typically 10MB) has been written, so it's not a small file
	// IMPORTANT: Calculate total size correctly by considering both OrigSize and current buffer
	// OrigSize accumulates size of flushed chunks, buffer contains unflushed data
	bufferSize := int64(len(ra.seqBuffer.buffer))
	totalSize := ra.seqBuffer.dataInfo.OrigSize + bufferSize
	DebugLog("[VFS flushSequentialBuffer] Checking instant upload eligibility: fileID=%d, totalSize=%d (OrigSize=%d + bufferSize=%d), sn=%d",
		ra.fileID, totalSize, ra.seqBuffer.dataInfo.OrigSize, bufferSize, ra.seqBuffer.sn)
	if totalSize > 0 && totalSize < 1<<20 && ra.seqBuffer.sn == 0 {
		// Small file and all data is still in buffer (no chunks written yet)
		// Try instant upload first (before batch write) if enabled
		allData := ra.seqBuffer.buffer
		if len(allData) > 0 {
			if core.IsInstantUploadEnabledWithConfig(core.GetBucketInstantUploadConfig(ra.fs.getBucketConfig())) {
				// Try instant upload (deduplication)
				instantDataID, err := tryInstantUpload(ra.fs, allData, totalSize, core.DATA_NORMAL)
				if err == nil && instantDataID > 0 {
					// Instant upload succeeded, use existing DataID
					// Update file object with instant upload DataID
					fileObj, err := ra.getFileObj()
					if err == nil && fileObj != nil {
						updateFileObj := &core.ObjectInfo{
							ID:     fileObj.ID,
							PID:    fileObj.PID,
							DataID: instantDataID,
							Size:   totalSize,
							MTime:  core.Now(),
							Type:   fileObj.Type,
							Name:   fileObj.Name,
						}
						// Update file object in database
						_, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
						if err == nil {
							// Update cache
							fileObjCache.Put(ra.fileObjKey, updateFileObj)
							ra.fileObj.Store(updateFileObj)
							// Update directory listing cache to ensure file is visible in Readdir (consistent with direct write path)
							if updateFileObj.PID > 0 {
								dirNode := &OrcasNode{
									fs:    ra.fs,
									objID: updateFileObj.PID,
								}
								dirNode.invalidateDirListCache(updateFileObj.PID)
								DebugLog("[VFS RandomAccessor flushSequentialBuffer] Appended file to directory listing cache (instant upload): fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
							}
							// Clear sequential buffer
							ra.seqBuffer.hasData = false
							ra.clearSeqBuffer() // Clear buffer and shrink capacity if needed
							return nil
						}
					}
					// If update failed, continue with batch write or normal write
				}
			}
		}
	}

	// Write last chunk (if there's still data)
	if len(ra.seqBuffer.buffer) > 0 {
		DebugLog("[VFS flushSequentialBuffer] Flushing remaining buffer: fileID=%d, bufferSize=%d, sn=%d", ra.fileID, len(ra.seqBuffer.buffer), ra.seqBuffer.sn)
		if err := ra.flushSequentialChunkLocked(); err != nil {
			DebugLog("[VFS RandomAccessor flushSequentialBuffer] ERROR: Failed to flush last sequential chunk: fileID=%d, bufferSize=%d, error=%v", ra.fileID, len(ra.seqBuffer.buffer), err)
			return err
		}
		DebugLog("[VFS flushSequentialBuffer] After flushing remaining buffer: fileID=%d, sn=%d, OrigSize=%d, Size=%d",
			ra.fileID, ra.seqBuffer.sn, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size)
	} else {
		DebugLog("[VFS flushSequentialBuffer] No remaining buffer to flush: fileID=%d, sn=%d, OrigSize=%d, Size=%d",
			ra.fileID, ra.seqBuffer.sn, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size)
	}

	// Update final size of DataInfo
	DebugLog("[VFS flushSequentialBuffer] Final DataInfo before update: fileID=%d, OrigSize=%d, Size=%d, Kind=0x%x (CMPR=%v, ENDEC=%v)",
		ra.fileID, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.dataInfo.Kind,
		ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK != 0,
		ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK != 0)

	if ra.seqBuffer.dataInfo.Kind&core.DATA_CMPR_MASK == 0 && ra.seqBuffer.dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		ra.seqBuffer.dataInfo.Size = ra.seqBuffer.dataInfo.OrigSize
		DebugLog("[VFS flushSequentialBuffer] No compression/encryption, set Size=OrigSize: fileID=%d, Size=%d",
			ra.fileID, ra.seqBuffer.dataInfo.Size)
	} else {
		DebugLog("[VFS flushSequentialBuffer] Has compression/encryption, Size already set: fileID=%d, OrigSize=%d, Size=%d",
			ra.fileID, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size)
	}

	// Get file object first to prepare for combined write
	fileObj, err := ra.getFileObj()
	if err != nil {
		DebugLog("[VFS RandomAccessor flushSequentialBuffer] ERROR: Failed to get file object: fileID=%d, error=%v", ra.fileID, err)
		return err
	}

	// IMPORTANT: If file already had data, we need to read and merge existing data before flushing
	// This prevents data loss when sequential write is incorrectly used for existing files
	oldSize := fileObj.Size
	oldDataID := fileObj.DataID
	newSize := ra.seqBuffer.dataInfo.OrigSize

	if oldDataID > 0 && oldDataID != core.EmptyDataID && oldSize > 0 {
		// File already had data, need to merge new writes with existing data
		// Use streaming processing to avoid loading entire file into memory
		DebugLog("[VFS flushSequentialBuffer] File already had data (oldDataID=%d, oldSize=%d), merging with new writes (newSize=%d) using streaming: fileID=%d", oldDataID, oldSize, newSize, ra.fileID)

		// Get new write data and offset from sequential buffer
		// Optimization: avoid unnecessary memory copy by directly using buffer
		// NOTE: seqBuffer.mu is already locked by caller (flushSequentialBuffer at line 2491)
		writeData := ra.seqBuffer.buffer // Direct reference to buffer
		writeOffset := ra.seqBuffer.offset - int64(len(ra.seqBuffer.buffer))
		ra.seqBuffer.buffer = nil  // Clear reference to prevent modification
		ra.seqBuffer.closed = true // Mark as closed

		// Optimization: If this is a complete overwrite from offset 0 (writeOffset == 0 and new data covers entire old file),
		// we can directly replace instead of merging, which avoids reading and decrypting old data
		// This is important for encrypted files to avoid decryption errors and improve performance
		isCompleteOverwrite := writeOffset == 0 && int64(len(writeData)) >= oldSize
		if isCompleteOverwrite {
			DebugLog("[VFS flushSequentialBuffer] Complete overwrite detected (offset=0, newSize=%d >= oldSize=%d), directly replacing without merging: fileID=%d", len(writeData), oldSize, ra.fileID)
			// Directly use sequential buffer data, no need to merge with old data
			// This creates a new version (new DataID) and avoids reading/decrypting old encrypted data
			fileObj.DataID = ra.seqBuffer.dataID
			fileObj.Size = newSize

			// Create version object for complete overwrite
			lh, ok := ra.fs.h.(*core.LocalHandler)
			if !ok {
				DebugLog("[VFS flushSequentialBuffer] ERROR: Handler is not LocalHandler")
				return fmt.Errorf("handler is not LocalHandler")
			}

			versionID := core.NewID()
			if versionID == 0 {
				DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to generate version ID")
				return fmt.Errorf("failed to generate version ID")
			}

			mTime := core.Now()
			newVersion := &core.ObjectInfo{
				ID:     versionID,
				PID:    ra.fileID,
				Type:   core.OBJ_TYPE_VERSION,
				DataID: ra.seqBuffer.dataID,
				Size:   newSize,
				MTime:  mTime,
			}

			// For sparse files, use sparseSize instead of newSize for file object size
			sparseSize := ra.getSparseSize()
			fileSize := newSize
			if sparseSize > 0 && sparseSize > newSize {
				fileSize = sparseSize
				DebugLog("[VFS flushSequentialBuffer] Using sparseSize for sparse file (complete overwrite): fileID=%d, sparseSize=%d, newSize=%d", ra.fileID, sparseSize, newSize)
			}

			// Update file object MTime and Size
			fileObj.MTime = mTime
			fileObj.Size = fileSize
			fileObj.DataID = ra.seqBuffer.dataID

			// Write DataInfo and version object first
			_, err := lh.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{ra.seqBuffer.dataInfo})
			if err != nil {
				DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to write DataInfo (complete overwrite): fileID=%d, dataID=%d, error=%v", ra.fileID, ra.seqBuffer.dataID, err)
				return err
			}

			// Create version object
			_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{newVersion})
			if err != nil {
				DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to create version (complete overwrite): fileID=%d, error=%v", ra.fileID, err)
				return err
			}

			// Update file object using SetObj to ensure Size field is updated
			// Put uses InsertIgnore which won't update existing objects
			err = lh.MetadataAdapter().SetObj(ra.fs.c, ra.fs.bktID, []string{"did", "s", "m"}, fileObj)
			if err != nil {
				DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to update file object (complete overwrite): fileID=%d, error=%v", ra.fileID, err)
				return err
			}
			DebugLog("[VFS flushSequentialBuffer] Updated file object using SetObj (complete overwrite): fileID=%d, DataID=%d, Size=%d", ra.fileID, fileObj.DataID, fileObj.Size)

			// Update caches
			dataInfoCache.Put(ra.seqBuffer.dataID, ra.seqBuffer.dataInfo)
			fileObjCache.Put(ra.fileObjKey, fileObj)
			ra.fileObj.Store(fileObj)

			// Invalidate directory listing cache
			if fileObj.PID > 0 {
				dirNode := &OrcasNode{
					fs:    ra.fs,
					objID: fileObj.PID,
				}
				dirNode.invalidateDirListCache(fileObj.PID)
			}

			// Trigger version retention cleanup
			if ra.fs.retentionMgr != nil {
				go func() {
					deleted := ra.fs.retentionMgr.CleanupFileVersions(ra.fileID)
					if deleted > 0 {
						DebugLog("[VFS flushSequentialBuffer] Cleaned up %d old versions for fileID=%d", deleted, ra.fileID)
					}
				}()
			}

			DebugLog("[VFS flushSequentialBuffer] Successfully completed full overwrite: fileID=%d, oldDataID=%d, newDataID=%d, oldSize=%d, newSize=%d, versionID=%d", ra.fileID, oldDataID, ra.seqBuffer.dataID, oldSize, newSize, versionID)
			return nil
		}

		// CRITICAL OPTIMIZATION: Check if this is pure append (no overlap with existing data)
		// Pure append means: writeOffset >= oldSize (new data starts at or after old file end)
		// For pure append, we can use SDK's AppendData to avoid reprocessing existing chunks
		isPureAppend := writeOffset >= oldSize
		if isPureAppend {
			DebugLog("[VFS flushSequentialBuffer] Pure append detected (writeOffset=%d >= oldSize=%d), using append mode to avoid reprocessing existing chunks: fileID=%d", writeOffset, oldSize, ra.fileID)

			// Use SDK's streaming append to add new chunks without reprocessing old ones
			lh, ok := ra.fs.h.(*core.LocalHandler)
			if ok {
				// Call optimized append path
				versionID, appendErr := ra.applyAppendWithSDK(lh, fileObj, writeOffset, writeData)
				if appendErr == nil {
					DebugLog("[VFS flushSequentialBuffer] Successfully completed append: fileID=%d, oldDataID=%d, newSize=%d, versionID=%d", ra.fileID, oldDataID, writeOffset+int64(len(writeData)), versionID)
					return nil
				}
				// If append failed, fall through to merge path
				DebugLog("[VFS flushSequentialBuffer] Append failed, falling back to merge path: fileID=%d, error=%v", ra.fileID, appendErr)
			}
		}

		// Restore old DataID and size so applyRandomWritesWithSDK can read existing data
		// applyRandomWritesWithSDK will use streaming processing to read existing data chunk by chunk
		// and merge with new writes, avoiding loading entire file into memory
		fileObj.DataID = oldDataID
		fileObj.Size = oldSize

		// Update cache to ensure applyRandomWritesWithSDK uses correct old DataID
		fileObjCache.Put(ra.fileObjKey, fileObj)
		ra.fileObj.Store(fileObj)

		// Convert sequential buffer data to write operations
		writeOps := []WriteOperation{
			{
				Offset: writeOffset,
				Data:   writeData, // Use direct reference (no copy)
			},
		}

		// Use applyRandomWritesWithSDK to handle the merge properly
		// This function uses streaming processing to read existing data chunk by chunk
		// and merge with new writes, avoiding large memory usage
		versionID, err := ra.applyRandomWritesWithSDK(fileObj, writeOps)
		if err != nil {
			DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to apply random writes after merging: fileID=%d, error=%v", ra.fileID, err)
			return err
		}

		// Calculate final size: preserve original size if new writes don't extend beyond it
		// This ensures that writing 4KB to an 84KB file doesn't truncate it to 4KB
		finalSize := oldSize
		if writeOffset+int64(len(writeData)) > oldSize {
			finalSize = writeOffset + int64(len(writeData))
		}

		// IMPORTANT: applyRandomWritesWithSDK already updated the file object and cache
		// We should verify the cache has the correct size, not re-read from database (to avoid WAL dirty read)
		// Get from cache to verify
		if cachedObj := ra.fileObj.Load(); cachedObj != nil {
			if obj, ok := cachedObj.(*core.ObjectInfo); ok && obj != nil {
				DebugLog("[VFS flushSequentialBuffer] Checking file size after merge: fileID=%d, cachedSize=%d, finalSize=%d, oldSize=%d", ra.fileID, obj.Size, finalSize, oldSize)
				if obj.Size < finalSize {
					// File size was incorrectly truncated, fix it
					DebugLog("[VFS flushSequentialBuffer] File size was incorrectly truncated (%d < %d), fixing: fileID=%d", obj.Size, finalSize, ra.fileID)
					updateFileObj := &core.ObjectInfo{
						ID:     obj.ID,
						PID:    obj.PID,
						Type:   obj.Type,
						Name:   obj.Name,
						Size:   finalSize,
						DataID: obj.DataID,
						MTime:  core.Now(),
					}
					// Use SetObj to ensure Size field is updated
					// Put uses InsertIgnore which won't update existing objects
					lh, ok := ra.fs.h.(*core.LocalHandler)
					if !ok {
						DebugLog("[VFS flushSequentialBuffer] ERROR: Handler is not LocalHandler")
						return fmt.Errorf("handler is not LocalHandler")
					}
					err := lh.MetadataAdapter().SetObj(ra.fs.c, ra.fs.bktID, []string{"s", "m"}, updateFileObj)
					if err != nil {
						DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to fix file size: fileID=%d, error=%v", ra.fileID, err)
						return err
					}
					// Update cache
					fileObjCache.Put(ra.fileObjKey, updateFileObj)
					ra.fileObj.Store(updateFileObj)
					DebugLog("[VFS flushSequentialBuffer] Fixed file size: fileID=%d, newSize=%d", ra.fileID, finalSize)
				} else {
					DebugLog("[VFS flushSequentialBuffer] File size is correct: fileID=%d, size=%d", ra.fileID, obj.Size)
				}
			}
		}

		DebugLog("[VFS flushSequentialBuffer] Successfully merged existing data with new writes using streaming: fileID=%d, oldSize=%d, finalSize=%d, versionID=%d", ra.fileID, oldSize, finalSize, versionID)
		return nil
	}

	// File had no data, proceed with normal sequential flush
	fileObj.DataID = ra.seqBuffer.dataID
	// IMPORTANT: Use OrigSize as file size, which is the total size of data written so far
	// This includes all chunks that have been written (sn=0, sn=1, etc.)
	// The offset might not reflect the actual file size if sequential write was interrupted
	fileObj.Size = newSize

	DebugLog("[VFS flushSequentialBuffer] New file sequential flush: fileID=%d, dataID=%d, newSize=%d, OrigSize=%d, Size=%d, sn=%d",
		ra.fileID, fileObj.DataID, newSize, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size, ra.seqBuffer.sn)

	// Create version object for new file sequential write
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		DebugLog("[VFS flushSequentialBuffer] ERROR: Handler is not LocalHandler")
		return fmt.Errorf("handler is not LocalHandler")
	}

	versionID := core.NewID()
	if versionID == 0 {
		DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to generate version ID")
		return fmt.Errorf("failed to generate version ID")
	}

	mTime := core.Now()
	newVersion := &core.ObjectInfo{
		ID:     versionID,
		PID:    ra.fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: ra.seqBuffer.dataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// Update file object MTime
	fileObj.MTime = mTime

	// CRITICAL: Ensure DataInfo.ID is set to dataID before writing
	// This is important because DataInfo.ID must match the dataID used in PutData calls
	if ra.seqBuffer.dataInfo.ID != ra.seqBuffer.dataID {
		DebugLog("[VFS flushSequentialBuffer] WARNING: DataInfo.ID mismatch, fixing: fileID=%d, DataInfo.ID=%d, dataID=%d",
			ra.fileID, ra.seqBuffer.dataInfo.ID, ra.seqBuffer.dataID)
		ra.seqBuffer.dataInfo.ID = ra.seqBuffer.dataID
	}

	// Optimization: Use PutDataInfoAndObj to write DataInfo, version object, and file object update together
	// This reduces database round trips and improves performance
	objectsToPut := []*core.ObjectInfo{newVersion, fileObj}
	DebugLog("[VFS flushSequentialBuffer] Writing DataInfo, version object, and file object to disk: fileID=%d, dataID=%d, DataInfo.ID=%d, size=%d, versionID=%d, chunkSize=%d, OrigSize=%d, DataInfo.Size=%d",
		ra.fileID, ra.seqBuffer.dataID, ra.seqBuffer.dataInfo.ID, fileObj.Size, versionID, ra.seqBuffer.chunkSize,
		ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size)
	err = lh.PutDataInfoAndObj(ra.fs.c, ra.fs.bktID, []*core.DataInfo{ra.seqBuffer.dataInfo}, objectsToPut)
	if err != nil {
		DebugLog("[VFS flushSequentialBuffer] ERROR: Failed to write DataInfo and ObjectInfo to disk: fileID=%d, dataID=%d, error=%v", ra.fileID, ra.seqBuffer.dataID, err)
		return err
	}
	DebugLog("[VFS flushSequentialBuffer] Successfully wrote DataInfo, version object, and file object to disk: fileID=%d, dataID=%d, size=%d, versionID=%d, OrigSize=%d, DataInfo.Size=%d",
		ra.fileID, ra.seqBuffer.dataID, fileObj.Size, versionID, ra.seqBuffer.dataInfo.OrigSize, ra.seqBuffer.dataInfo.Size)

	// Update caches
	dataInfoCache.Put(ra.seqBuffer.dataID, ra.seqBuffer.dataInfo)
	fileObjCache.Put(ra.fileObjKey, fileObj)
	ra.fileObj.Store(fileObj)

	// After sync flush, append file to directory listing cache
	// This ensures the file is immediately visible in Readdir
	if fileObj.PID > 0 {
		dirNode := &OrcasNode{
			fs:    ra.fs,
			objID: fileObj.PID,
		}
		dirNode.invalidateDirListCache(fileObj.PID)
		DebugLog("[VFS flushSequentialBuffer] Appended file to directory listing cache after sync flush: fileID=%d, dirID=%d, name=%s", ra.fileID, fileObj.PID, fileObj.Name)
	}

	// Trigger version retention cleanup
	if ra.fs.retentionMgr != nil {
		go func() {
			deleted := ra.fs.retentionMgr.CleanupFileVersions(ra.fileID)
			if deleted > 0 {
				DebugLog("[VFS flushSequentialBuffer] Cleaned up %d old versions for fileID=%d", deleted, ra.fileID)
			}
		}()
	}

	DebugLog("[VFS flushSequentialBuffer] Successfully flushed sequential buffer: fileID=%d, dataID=%d, size=%d, versionID=%d", ra.fileID, fileObj.DataID, fileObj.Size, versionID)
	return nil
}

func (ra *RandomAccessor) getFileObj() (*core.ObjectInfo, error) {
	// Fast path: atomic value (最快)
	if fileObjValue := ra.fileObj.Load(); fileObjValue != nil {
		if obj, ok := fileObjValue.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}

	// Medium path: cache (快)
	if cached, ok := fileObjCache.Get(ra.fileObjKey); ok {
		if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
			// 回填到 atomic.Value 以加速后续访问
			ra.fileObj.Store(obj)
			return obj, nil
		}
	}

	// Slow path: database (慢)
	return ra.fetchFileObjFromDB()
}

// fetchFileObjFromDB fetches file object from database and updates caches
func (ra *RandomAccessor) fetchFileObjFromDB() (*core.ObjectInfo, error) {
	objs, err := ra.fs.h.Get(ra.fs.c, ra.fs.bktID, []int64{ra.fileID})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("file %d not found", ra.fileID)
	}

	obj := objs[0]
	// 更新两层缓存
	fileObjCache.Put(ra.fileObjKey, obj)
	ra.fileObj.Store(obj)
	return obj, nil
}

// updateFileObject updates file object with new dataID and size, and updates all caches
// This is a common operation used in multiple flush paths
func (ra *RandomAccessor) updateFileObject(dataID, size int64) error {
	fileObj, err := ra.getFileObj()
	if err != nil {
		return fmt.Errorf("failed to get file object: %w", err)
	}

	updateFileObj := &core.ObjectInfo{
		ID:     fileObj.ID,
		PID:    fileObj.PID,
		Type:   fileObj.Type,
		Name:   fileObj.Name,
		DataID: dataID,
		Size:   size,
		MTime:  core.Now(),
		Mode:   fileObj.Mode,
		Extra:  fileObj.Extra,
	}

	_, err = ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
	if err != nil {
		return fmt.Errorf("failed to update file object in database: %w", err)
	}

	// Update caches
	fileObjCache.Put(ra.fileObjKey, updateFileObj)
	ra.fileObj.Store(updateFileObj)

	DebugLog("[VFS updateFileObject] Updated file object: fileID=%d, dataID=%d, size=%d",
		ra.fileID, dataID, size)

	return nil
}

// Read reads data at specified position, merges writes in buffer
// Optimization: use atomic pointer to read fileObj, lock-free concurrent read
func (ra *RandomAccessor) Read(offset int64, size int) ([]byte, error) {
	// PRIORITY 1: Check if journal is active
	// If journal exists, use it for reading (applies journal entries on top of base data)
	ra.journalMu.RLock()
	hasJournal := ra.journal != nil
	journalIsSparse := false
	if hasJournal {
		journalIsSparse = ra.journal.isSparse
	}
	ra.journalMu.RUnlock()

	if hasJournal {
		// Use journal size if available
		journalSize := ra.getJournalSize()
		if journalSize >= 0 {
			// For sparse files, use virtual size
			if journalIsSparse && ra.journal.virtualSize > 0 {
				journalSize = ra.journal.virtualSize
				DebugLog("[VFS Read] Using sparse journal virtual size: fileID=%d, virtualSize=%d", ra.fileID, journalSize)
			}

			// Limit read to journal size
			if offset >= journalSize {
				return []byte{}, nil
			}
			if int64(size) > journalSize-offset {
				size = int(journalSize - offset)
			}
		}

		DebugLog("[VFS Read] Reading from journal: fileID=%d, offset=%d, size=%d, isSparse=%v", ra.fileID, offset, size, journalIsSparse)
		data, err := ra.readFromJournal(offset, int64(size))
		if err != nil {
			DebugLog("[VFS Read] ERROR: Failed to read from journal: %v", err)
			// Fall back to regular read
		} else {
			return data, nil
		}
	}

	// Optimization: use atomic operation to read fileObj, lock-free concurrent read
	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// Use fileObj.Size as the authoritative source for file size
	var actualFileSize int64 = fileObj.Size

	// Check for ChunkedFileWriter: if it exists and has data, use its size
	// This is important when writes are in progress and fileObj.Size hasn't been updated yet
	if val := ra.chunkedWriter.Load(); val != nil && val != clearedChunkedWriterMarker {
		if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil {
			cwSize := atomic.LoadInt64(&cw.size)
			if cwSize > actualFileSize {
				actualFileSize = cwSize
				DebugLog("[VFS Read] ChunkedFileWriter detected, using cw.size: fileID=%d, cwSize=%d, fileObj.Size=%d", ra.fileID, cwSize, fileObj.Size)
			}
		}
	}

	// Check for sparse file: sparse files may have sparseSize > fileObj.Size
	sparseSize := atomic.LoadInt64(&ra.sparseSize)
	if sparseSize > 0 && sparseSize > actualFileSize {
		// Sparse file: use sparseSize as the actual file size for reading
		actualFileSize = sparseSize
		DebugLog("[VFS Read] Sparse file detected, using sparseSize: fileID=%d, sparseSize=%d, fileObj.Size=%d", ra.fileID, sparseSize, fileObj.Size)
	}

	// If file has no DataID, check buffer first
	// Buffer may have data even if fileObj.Size is 0
	// IMPORTANT: This check must happen BEFORE checking actualFileSize == 0,
	// because buffer-only writes may have fileObj.Size = 0 but buffer has data
	// Also check sequential buffer if it exists and has data
	if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
		// First check sequential buffer if it exists and has data
		if ra.seqBuffer != nil {
			ra.seqBuffer.mu.Lock()
			seqHasData := ra.seqBuffer.hasData
			seqClosed := ra.seqBuffer.closed
			seqOffset := ra.seqBuffer.offset
			seqChunkSize := ra.seqBuffer.chunkSize
			seqBufferLen := len(ra.seqBuffer.buffer)
			seqBuffer := make([]byte, seqBufferLen)
			copy(seqBuffer, ra.seqBuffer.buffer)
			ra.seqBuffer.mu.Unlock()

			if seqHasData && !seqClosed {
				// Sequential buffer has data, read from it
				// Sequential buffer contains data from offset 0, so we can read directly
				// The buffer contains data up to seqOffset
				seqDataLen := seqOffset
				if offset < seqDataLen {
					readEnd := offset + int64(size)
					if readEnd > seqDataLen {
						readEnd = seqDataLen
					}
					if offset < readEnd {
						// Read from sequential buffer's buffer field
						// The buffer contains data from offset 0 to seqDataLen
						// But we need to read from the actual data, which may be in chunks
						// For now, if offset is within the current chunk buffer, read from it
						bufferStart := (seqOffset / seqChunkSize) * seqChunkSize
						bufferEnd := bufferStart + int64(seqBufferLen)
						if offset >= bufferStart && offset < bufferEnd {
							bufferOffset := offset - bufferStart
							readEndInBuffer := readEnd - bufferStart
							if readEndInBuffer > int64(seqBufferLen) {
								readEndInBuffer = int64(seqBufferLen)
							}
							if bufferOffset < readEndInBuffer {
								result := make([]byte, readEndInBuffer-bufferOffset)
								copy(result, seqBuffer[bufferOffset:readEndInBuffer])
								DebugLog("[VFS Read] Reading from sequential buffer: fileID=%d, offset=%d, size=%d, resultLen=%d", ra.fileID, offset, size, len(result))
								return result, nil
							}
						}
						// If offset is not in current buffer, data may have been flushed
						// Fall through to check random buffer
					}
				}
			}
		}

		// Check if random buffer has data
		writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
		if writeIndex > 0 {
			// Buffer has data, allow reading from buffer even if fileObj.Size is 0
			// Size will be calculated from buffer operations
			DebugLog("[VFS Read] File has no DataID and buffer has data, reading from buffer: fileID=%d, writeIndex=%d, offset=%d, size=%d", ra.fileID, writeIndex, offset, size)
			result := ra.readFromBuffer(offset, size)
			DebugLog("[VFS Read] readFromBuffer returned: fileID=%d, resultLen=%d", ra.fileID, len(result))
			return result, nil
		}
	}

	// Limit reading size to file size (only if we have a valid size)
	DebugLog("[VFS Read] Before size limit: fileID=%d, offset=%d, requestedSize=%d, actualFileSize=%d", ra.fileID, offset, size, actualFileSize)
	// IMPORTANT: If actualFileSize is 0 and file has DataID, try to read from DataInfo
	// This handles the case where chunk was auto-flushed but fileObj.Size hasn't been updated yet
	// For files with DataID, DataInfo.OrigSize may contain the actual data size
	if actualFileSize == 0 {
		// If file has DataID, don't return empty immediately - try to read from DataInfo
		// This is important for auto-flushed chunks where fileObj.Size may not be updated yet
		if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
			DebugLog("[VFS Read] File is empty (actualFileSize=0, no DataID), returning empty: fileID=%d", ra.fileID)
			return []byte{}, nil
		}
		// File has DataID but Size is 0, continue to read from DataInfo
		// DataInfo.OrigSize will be used to determine actual file size
		DebugLog("[VFS Read] File Size is 0 but has DataID, will read from DataInfo: fileID=%d, DataID=%d", ra.fileID, fileObj.DataID)
	}
	if actualFileSize > 0 {
		if offset >= actualFileSize {
			DebugLog("[VFS Read] offset >= size, returning empty: fileID=%d, offset=%d, size=%d", ra.fileID, offset, actualFileSize)
			return []byte{}, nil
		}
		if int64(size) > actualFileSize-offset {
			oldSize := size
			size = int(actualFileSize - offset)
			DebugLog("[VFS Read] Limited read size: fileID=%d, oldSize=%d, newSize=%d, actualFileSize=%d, offset=%d", ra.fileID, oldSize, size, actualFileSize, offset)
		}
	}
	DebugLog("[VFS Read] After size limit: fileID=%d, offset=%d, size=%d, actualFileSize=%d", ra.fileID, offset, size, actualFileSize)

	// If sequential write buffer has data, read from it first
	if ra.seqBuffer != nil {
		ra.seqBuffer.mu.Lock()
		seqHasData := ra.seqBuffer.hasData
		seqClosed := ra.seqBuffer.closed
		ra.seqBuffer.mu.Unlock()

		if seqHasData {
			// Sequential write buffer has data, need to flush before reading
			// But shouldn't flush when reading, so only handle data already in file object
			// If sequential write buffer is not closed, data is still in buffer, need to flush first
			if !seqClosed {
				// Sequential write not completed, flush to file object first
				if flushErr := ra.flushSequentialBuffer(); flushErr != nil {
					return nil, flushErr
				}
				ra.seqBuffer.mu.Lock()
				ra.seqBuffer.closed = true
				ra.seqBuffer.mu.Unlock()
				// Re-acquire file object
				var objErr error
				fileObj, objErr = ra.getFileObj()
				if objErr != nil {
					return nil, objErr
				}
			} else {
				// Sequential write buffer is closed, data has been flushed
				// Re-acquire file object (may have been updated by flush)
				var objErr error
				fileObj, objErr = ra.getFileObj()
				if objErr != nil {
					return nil, objErr
				}
			}
		}
	}

	// If no data ID, read from buffer only
	if fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
		DebugLog("[VFS Read] No DataID, reading from buffer only: fileID=%d, offset=%d, size=%d", ra.fileID, offset, size)
		return ra.readFromBuffer(offset, size), nil
	}

	// File has DataID, read from database
	// Get DataInfo
	dataInfoCacheKey := fileObj.DataID
	DebugLog("[VFS Read] File has DataID, getting DataInfo: fileID=%d, DataID=%d, fileObj.Size=%d, offset=%d, size=%d",
		ra.fileID, fileObj.DataID, fileObj.Size, offset, size)
	var dataInfo *core.DataInfo
	if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
		if info, ok := cached.(*core.DataInfo); ok && info != nil {
			// Verify cached DataInfo matches file size (for files that may have been updated)
			// If file size changed, invalidate cache and re-fetch
			if info.OrigSize != fileObj.Size {
				// DataInfo cache is stale, invalidate it
				DebugLog("[VFS Read] DataInfo cache mismatch, invalidating: fileID=%d, DataID=%d, cached.OrigSize=%d, fileObj.Size=%d",
					ra.fileID, fileObj.DataID, info.OrigSize, fileObj.Size)
				dataInfoCache.Del(dataInfoCacheKey)
				// Also invalidate decodingReaderCache to ensure chunkReader uses fresh DataInfo
				decodingReaderCache.Del(dataInfoCacheKey)
				dataInfo = nil
			} else {
				dataInfo = info
				DebugLog("[VFS Read] Using cached DataInfo: fileID=%d, DataID=%d, OrigSize=%d, Size=%d",
					ra.fileID, fileObj.DataID, dataInfo.OrigSize, dataInfo.Size)
			}
		}
	}

	// If cache miss, get from database
	if dataInfo == nil {
		DebugLog("[VFS Read] DataInfo cache miss, fetching from database: fileID=%d, DataID=%d", ra.fileID, fileObj.DataID)
		var err error
		dataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
		if err != nil {
			DebugLog("[VFS Read] ERROR: Failed to get DataInfo from database: fileID=%d, DataID=%d, error=%v", ra.fileID, fileObj.DataID, err)
			// If getting DataInfo fails, try direct read (may be old data format)
			data, readErr := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, fileObj.DataID, 0)
			if readErr == nil && len(data) > 0 {
				DebugLog("[VFS Read] Fallback to direct GetData: fileID=%d, DataID=%d, dataLen=%d", ra.fileID, fileObj.DataID, len(data))
				return ra.readFromDataAndBuffer(data, offset, size), nil
			}
			DebugLog("[VFS Read] Fallback to readFromBuffer: fileID=%d, DataID=%d", ra.fileID, fileObj.DataID)
			return ra.readFromBuffer(offset, size), nil
		}
		// Update cache
		dataInfoCache.Put(dataInfoCacheKey, dataInfo)
		DebugLog("[VFS Read] Fetched DataInfo from database: fileID=%d, DataID=%d, OrigSize=%d, Size=%d",
			ra.fileID, fileObj.DataID, dataInfo.OrigSize, dataInfo.Size)
	}

	// Debug: Log DataInfo details
	// Verify DataInfo OrigSize matches file size (critical for correct reading)
	DebugLog("[VFS Read] DataInfo check: fileID=%d, DataID=%d, DataInfo.OrigSize=%d, DataInfo.Size=%d, fileObj.Size=%d, offset=%d, requestedSize=%d",
		ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size, fileObj.Size, offset, size)
	if dataInfo.OrigSize != fileObj.Size {
		// DataInfo OrigSize doesn't match file size, this is a problem
		DebugLog("[VFS Read] WARNING: DataInfo OrigSize mismatch, re-fetching: fileID=%d, DataID=%d, DataInfo.OrigSize=%d, fileObj.Size=%d",
			ra.fileID, dataInfo.ID, dataInfo.OrigSize, fileObj.Size)
		// Invalidate cache and re-fetch DataInfo
		dataInfoCache.Del(dataInfoCacheKey)
		decodingReaderCache.Del(dataInfoCacheKey)
		// Re-fetch DataInfo from database
		dataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
		if err != nil {
			DebugLog("[VFS Read] ERROR: Failed to re-fetch DataInfo: fileID=%d, DataID=%d, error=%v", ra.fileID, fileObj.DataID, err)
			return nil, fmt.Errorf("failed to re-fetch DataInfo after size mismatch: %w", err)
		}
		if dataInfo != nil {
			DebugLog("[VFS Read] Re-fetched DataInfo: fileID=%d, DataID=%d, OrigSize=%d, Size=%d",
				ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size)
			// If re-fetched DataInfo still doesn't match fileObj.Size, use fileObj.Size
			// This can happen if DataInfo hasn't been updated yet after truncate
			if dataInfo.OrigSize != fileObj.Size {
				DebugLog("[VFS Read] WARNING: Re-fetched DataInfo OrigSize still doesn't match fileObj.Size, will use fileObj.Size: fileID=%d, DataID=%d, DataInfo.OrigSize=%d, fileObj.Size=%d",
					ra.fileID, dataInfo.ID, dataInfo.OrigSize, fileObj.Size)
			}
			dataInfoCache.Put(dataInfoCacheKey, dataInfo)
		}
	}
	DebugLog("[VFS Read] DataInfo: fileID=%d, DataID=%d, OrigSize=%d, Size=%d, Kind=0x%x, PkgID=%d, PkgOffset=%d",
		ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size, dataInfo.Kind, dataInfo.PkgID, dataInfo.PkgOffset)

	// Always use bucket's default chunk size (force unified chunkSize)
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize // 10MB default
	}

	// Use unified chunkReader for both plain and compressed/encrypted data
	// Get encryption key from OrcasFS (not from bucket config)
	endecKey := getEndecKeyForFS(ra.fs)
	// IMPORTANT: Log encryption key info to help debug key mismatch issues
	if dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
		if endecKey == "" {
			DebugLog("[VFS Read] WARNING: Data is encrypted but encryption key is empty: fileID=%d, DataID=%d, Kind=0x%x",
				ra.fileID, dataInfo.ID, dataInfo.Kind)
		} else {
			DebugLog("[VFS Read] Encryption key info: fileID=%d, DataID=%d, Kind=0x%x, endecKey length=%d",
				ra.fileID, dataInfo.ID, dataInfo.Kind, len(endecKey))
		}
	}

	// Create data reader (abstract read interface, unified handling of uncompressed and compressed/encrypted data)
	// Always create a new chunkReader to ensure it uses the latest DataInfo
	// (don't reuse cached reader as it may have stale origSize)
	reader := newChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, dataInfo, endecKey, chunkSize)
	// Ensure reader.origSize matches fileObj.Size (critical for truncated files)
	// If DataInfo.OrigSize doesn't match fileObj.Size, use fileObj.Size to limit reads
	// This is important because after truncate, DataInfo.OrigSize may not be updated immediately
	// EXCEPTION: If fileObj.Size is 0 but DataInfo.OrigSize > 0, use DataInfo.OrigSize
	// This handles the case where chunk was auto-flushed but fileObj.Size hasn't been updated yet
	if actualFileSize == 0 && reader.origSize > 0 {
		// File size is 0 but DataInfo has data, use DataInfo.OrigSize
		// This is important for auto-flushed chunks where fileObj.Size may not be updated yet
		DebugLog("[VFS Read] File Size is 0 but DataInfo has data, using DataInfo.OrigSize: fileID=%d, DataInfo.OrigSize=%d",
			ra.fileID, reader.origSize)
		actualFileSize = reader.origSize
	} else if reader.origSize != actualFileSize && actualFileSize > 0 {
		// Always use fileObj.Size if it doesn't match DataInfo.OrigSize (and fileObj.Size > 0)
		// This ensures truncated files are read correctly
		DebugLog("[VFS Read] WARNING: chunkReader origSize mismatch, using fileObj.Size: fileID=%d, DataInfo.OrigSize=%d, fileObj.Size=%d",
			ra.fileID, reader.origSize, actualFileSize)
		reader.origSize = actualFileSize
	}
	DebugLog("[VFS Read] Created chunkReader: fileID=%d, dataID=%d, origSize=%d, chunkSize=%d, kind=0x%x, endecKey length=%d",
		ra.fileID, reader.dataID, reader.origSize, reader.chunkSize, reader.kind, len(reader.endecKey))

	// Unified read logic (includes merging write operations)
	// IMPORTANT: Ensure read size is limited to actualFileSize to prevent reading beyond truncated size
	// This is critical for truncated files where DataInfo.OrigSize may not match fileObj.Size
	if int64(size) > actualFileSize-offset {
		size = int(actualFileSize - offset)
		if size < 0 {
			size = 0
		}
	}
	DebugLog("[VFS Read] Starting readWithWrites: fileID=%d, offset=%d, size=%d, origSize=%d, actualFileSize=%d",
		ra.fileID, offset, size, reader.origSize, actualFileSize)
	fileData, operationsHandled := ra.readWithWrites(reader, offset, size, actualFileSize)
	if operationsHandled {
		DebugLog("[VFS Read] readWithWrites completed: fileID=%d, readSize=%d, requested=%d, actualFileSize=%d, reader.origSize=%d",
			ra.fileID, len(fileData), size, actualFileSize, reader.origSize)
		// IMPORTANT: Ensure we don't return more data than actualFileSize
		if int64(len(fileData)) > actualFileSize-offset {
			oldLen := len(fileData)
			fileData = fileData[:actualFileSize-offset]
			DebugLog("[VFS Read] WARNING: Truncated read data: fileID=%d, oldLen=%d, newLen=%d, actualFileSize=%d, offset=%d", ra.fileID, oldLen, len(fileData), actualFileSize, offset)
		}
		return fileData, nil
	}
	DebugLog("[VFS Read] readWithWrites returned false, trying fallback: fileID=%d", ra.fileID)

	// If read fails or write operations not handled, read from buffer
	return ra.readFromBuffer(offset, size), nil
}

// readFromDataAndBuffer reads from data and buffer and merges
func (ra *RandomAccessor) readFromDataAndBuffer(data []byte, offset int64, size int) []byte {
	// Get buffer write operations
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}

	// Merge write operations
	mergedOps := mergeWriteOperations(operations)
	modifiedData := applyWritesToData(data, mergedOps)

	// Extract specified range
	return ra.extractRange(modifiedData, offset, size)
}

// readFromBuffer reads only from buffer (handles write operations)
func (ra *RandomAccessor) readFromBuffer(offset int64, size int) []byte {
	// Get buffer write operations
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}

	// Merge write operations
	mergedOps := mergeWriteOperations(operations)
	var modifiedData []byte
	if len(mergedOps) > 0 {
		// Calculate required data size
		var maxSize int64
		for _, op := range mergedOps {
			opEnd := op.Offset + int64(len(op.Data))
			if opEnd > maxSize {
				maxSize = opEnd
			}
		}
		modifiedData = applyWritesToData(nil, mergedOps)
	} else {
		modifiedData = []byte{}
	}

	// Extract specified range
	return ra.extractRange(modifiedData, offset, size)
}

// extractRange extracts specified range from data
func (ra *RandomAccessor) extractRange(data []byte, offset int64, size int) []byte {
	if offset >= int64(len(data)) {
		return []byte{}
	}

	end := offset + int64(size)
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	if offset >= end {
		return []byte{}
	}

	// If returning the entire slice, return directly, otherwise create new slice
	if offset == 0 && end == int64(len(data)) {
		return data
	}

	result := make([]byte, end-offset)
	copy(result, data[offset:end])
	return result
}

func (ra *RandomAccessor) requestDelayedFlush(force bool) {
	if ra == nil {
		return
	}
	atomic.StoreInt64(&ra.lastActivity, core.Now())
	getDelayedFlushManager().schedule(ra, force)
}

func (ra *RandomAccessor) cancelDelayedFlush() {
	if ra == nil {
		return
	}
	getDelayedFlushManager().cancel(ra)
}

func (ra *RandomAccessor) executeDelayedFlush(force bool) {
	if ra == nil {
		return
	}

	// CRITICAL: For .tmp files, skip delayed flush
	// .tmp files should only be flushed on rename (force=true)
	// Delayed flush (timeout) should not trigger flush for .tmp files
	// Use the immutable isTmpFile flag (set at creation time)
	if ra.isTmpFile {
		DebugLog("[VFS RandomAccessor] Skipping delayed flush for .tmp file (only flush on rename): fileID=%d, force=%v", ra.fileID, force)
		return
	}

	if force {
		if _, err := ra.ForceFlush(); err != nil {
			DebugLog("[VFS RandomAccessor] ERROR: Force flush during delayed schedule failed: fileID=%d, error=%v", ra.fileID, err)
		}
		return
	}
	if _, err := ra.Flush(); err != nil {
		DebugLog("[VFS RandomAccessor] ERROR: Flush during delayed schedule failed: fileID=%d, error=%v", ra.fileID, err)
	}
}

// Flush flushes buffer, returns new version ID (returns 0 if no pending data to flush)
// For .tmp files, this method does NOT flush immediately, but schedules a delayed flush after 5 minutes
// Use ForceFlush() to force immediate flush (e.g., when renaming .tmp file)
func (ra *RandomAccessor) Flush() (int64, error) {
	return ra.flushInternal(false)
}

// ForceFlush forces immediate flush, even for .tmp files
// This is used when .tmp file is renamed (removing .tmp extension)
func (ra *RandomAccessor) ForceFlush() (int64, error) {
	return ra.flushInternal(true)
}

// flushInternal is the internal flush implementation
// force: if true, flush immediately even for .tmp files; if false, schedule delayed flush for .tmp files
func (ra *RandomAccessor) flushInternal(force bool) (int64, error) {
	// Serialize flush operations to prevent concurrent access issues
	ra.flushMu.Lock()
	defer ra.flushMu.Unlock()

	// Ensure any pending delayed flush entry is cleared before flushing now
	ra.cancelDelayedFlush()

	DebugLog("[VFS RandomAccessor Flush] Starting flush: fileID=%d, force=%v", ra.fileID, force)

	// Validate RandomAccessor is properly initialized
	if ra.fs == nil {
		DebugLog("[VFS RandomAccessor Flush] ERROR: RandomAccessor.fs is nil: fileID=%d", ra.fileID)
		return 0, fmt.Errorf("RandomAccessor.fs is nil")
	}
	if ra.fileID <= 0 {
		DebugLog("[VFS RandomAccessor Flush] ERROR: RandomAccessor.fileID is invalid: fileID=%d", ra.fileID)
		return 0, fmt.Errorf("RandomAccessor.fileID is invalid: %d", ra.fileID)
	}

	// PRIORITY 1: Flush journal if exists (replaces TempWriteFile)
	DebugLog("[VFS RandomAccessor Flush] 🔍 Checking journal flush path: fileID=%d, force=%v", ra.fileID, force)
	versionID, journalFlushed := ra.flushJournal()
	if journalFlushed {
		DebugLog("[VFS RandomAccessor Flush] ✅ Successfully flushed journal: fileID=%d, versionID=%d", ra.fileID, versionID)
		return versionID, nil
	} else {
		DebugLog("[VFS RandomAccessor Flush] ⚠️ Journal not flushed: fileID=%d, versionID=%d, will try other paths", ra.fileID, versionID)
	}

	// For .tmp files, check final file size before flushing ChunkedFileWriter
	// Use the immutable isTmpFile flag (set at creation time)
	if ra.isTmpFile {
		fileObj, err := ra.getFileObj()
		if err != nil {
			return 0, fmt.Errorf("failed to get file object for .tmp file flush: %w", err)
		}
		// CRITICAL: For .tmp files, only flush when force=true (i.e., during rename)
		// For .tmp files, flush should only occur in these scenarios:
		// 1. chunk写满（range只有一个，而且是从0-10MB的范围写满）- handled in ChunkedFileWriter.Write
		// 2. tmp的后缀被重命名掉 - handled by ForceFlush (force=true)
		// 3. 写入以后超时了，没有任何操作，也没有去除tmp后缀 - handled by delayed flush (but should skip for .tmp files)
		// Regular flush (force=false) should NOT trigger flush for .tmp files
		if !force {
			DebugLog("[VFS RandomAccessor Flush] Skipping flush for .tmp file (only flush on rename): fileID=%d, force=%v", ra.fileID, force)
			return 0, nil
		}

		// force=true: This is a rename operation, flush ChunkedFileWriter
		// For .tmp files, check if we should use batch write based on final file size
		// Calculate final file size from buffer operations (before swapping)
		writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
		totalSize := atomic.LoadInt64(&ra.buffer.totalSize)

		// Check if ChunkedFileWriter has data to flush
		// This is important because large files use ChunkedFileWriter, and data might be
		// in ChunkedFileWriter but not in RandomAccessor's buffer (writeIndex=0, totalSize=0)
		hasChunkedWriterData := false
		// Lock-free check using atomic.Value
		if val := ra.chunkedWriter.Load(); val != nil && val != clearedChunkedWriterMarker {
			if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil && cw.writerType == WRITER_TYPE_TMP {
				// Validate ChunkedFileWriter before checking size
				if cw.fileID > 0 && cw.dataID > 0 {
					// Check if ChunkedFileWriter has data (size > 0)
					cwSize := atomic.LoadInt64(&cw.size)
					hasChunkedWriterData = cwSize > 0
					DebugLog("[VFS RandomAccessor Flush] .tmp file with ChunkedFileWriter (force flush on rename): fileID=%d, cwSize=%d, writeIndex=%d, totalSize=%d", ra.fileID, cwSize, writeIndex, totalSize)
				} else {
					// ChunkedFileWriter is invalid, clear it
					DebugLog("[VFS RandomAccessor Flush] WARNING: ChunkedFileWriter is invalid (fileID=%d, dataID=%d), clearing: ra.fileID=%d", cw.fileID, cw.dataID, ra.fileID)
					ra.chunkedWriter.Store(clearedChunkedWriterMarker)
				}
			}
		}

		// Use ChunkedFileWriter for .tmp files
		if hasChunkedWriterData || fileObj.DataID == 0 || fileObj.DataID == core.EmptyDataID {
			// ChunkedFileWriter has data or file has no DataID, flush it
			if err := ra.flushTempFileWriter(); err != nil {
				return 0, err
			}
			// IMPORTANT: flushTempFileWriter already updated the cache, don't re-read from database
			// Just get the updated object from cache to avoid WAL dirty read
			fileObj, err = ra.getFileObj()
			if err == nil && fileObj != nil {
				DebugLog("[VFS RandomAccessor Flush] Got fileObj from cache after ChunkedFileWriter flush: fileID=%d, dataID=%d, size=%d", ra.fileID, fileObj.DataID, fileObj.Size)
			}
		}
	}

	// Get fileObj for non-.tmp files or after .tmp file flush
	fileObj, err := ra.getFileObj()
	if err != nil {
		return 0, fmt.Errorf("failed to get file object: %w", err)
	}

	// For non-.tmp files, flush ChunkedFileWriter if exists
	// IMPORTANT:
	// - Sparse files: WRITER_TYPE_SPARSE 通常在 Close() 完成 flush，这里只是兜底
	// - 顺序上传的新文件：WRITER_TYPE_SEQ 需要在 Flush()/Close() 时确保完全落盘
	if !ra.isTmpFile {
		// Check if sparse file and has ChunkedFileWriter
		sparseSize := ra.getSparseSize()
		if sparseSize > 0 {
			// For sparse files, ChunkedFileWriter should be flushed in Close()
			// But if it still exists here, flush it (shouldn't happen normally)
			val := ra.chunkedWriter.Load()
			if val != nil && val != clearedChunkedWriterMarker {
				if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil && cw.writerType == WRITER_TYPE_SPARSE {
					DebugLog("[VFS RandomAccessor Flush] WARNING: Sparse file ChunkedFileWriter still exists in Flush(), flushing: fileID=%d", ra.fileID)
					if err := ra.flushChunkedWriter(WRITER_TYPE_SPARSE); err != nil {
						return 0, err
					}
				}
			}
		} else {
			// For non-sparse, non-.tmp files, flush ChunkedFileWriter if exists (TMP or SEQ type)
			if err := ra.flushTempFileWriter(); err != nil {
				return 0, err
			}
			if err := ra.flushChunkedWriter(WRITER_TYPE_SEQ); err != nil {
				return 0, err
			}
		}
		// IMPORTANT: flushChunkedWriter/flushTempFileWriter already updated the cache, don't re-read from database
		// Just get the updated object from cache to avoid WAL dirty read
		fileObj, err = ra.getFileObj()
		if err == nil && fileObj != nil {
			DebugLog("[VFS RandomAccessor Flush] Got fileObj from cache after ChunkedFileWriter flush (non-tmp): fileID=%d, dataID=%d, size=%d", ra.fileID, fileObj.DataID, fileObj.Size)
		}
	}

	// OPTIMIZATION: For regular files, only flush sequential buffer if:
	// 1. force=true (ForceFlush/Release) - always flush
	// 2. buffer is full (>= chunkSize) - flush to avoid memory bloat
	// 3. For regular Flush (force=false), skip if buffer is not full
	// This reduces disk I/O and improves performance by batching writes
	if ra.seqBuffer != nil {
		ra.seqBuffer.mu.Lock()
		seqHasData := ra.seqBuffer.hasData
		seqClosed := ra.seqBuffer.closed
		seqDataID := ra.seqBuffer.dataID
		seqSn := ra.seqBuffer.sn
		seqBufferSize := len(ra.seqBuffer.buffer)
		seqChunkSize := ra.seqBuffer.chunkSize
		ra.seqBuffer.mu.Unlock()

		if seqHasData && !seqClosed {
			// Check if we should flush:
			// - force=true: always flush (Release/ForceFlush)
			// - buffer is full: flush to avoid memory bloat
			shouldFlush := force || int64(seqBufferSize) >= seqChunkSize

			DebugLog("[VFS RandomAccessor Flush] Sequential buffer state: fileID=%d, hasData=%v, closed=%v, bufferSize=%d, chunkSize=%d, sn=%d, force=%v, shouldFlush=%v",
				ra.fileID, seqHasData, seqClosed, seqBufferSize, seqChunkSize, seqSn, force, shouldFlush)

			if shouldFlush {
				DebugLog("[VFS RandomAccessor Flush] Flushing sequential buffer: fileID=%d, dataID=%d, sn=%d, bufferSize=%d, chunkSize=%d, force=%v",
					ra.fileID, seqDataID, seqSn, seqBufferSize, seqChunkSize, force)
				if err := ra.flushSequentialBuffer(); err != nil {
					DebugLog("[VFS RandomAccessor Flush] ERROR: Failed to flush sequential buffer: fileID=%d, error=%v", ra.fileID, err)
					return 0, err
				}
				DebugLog("[VFS RandomAccessor Flush] Sequential buffer flushed successfully: fileID=%d", ra.fileID)
				// After sequential write completes, close sequential buffer only if force=true
				// For regular flush with full chunk, keep buffer open for continued writes
				if force {
					ra.seqBuffer.mu.Lock()
					ra.seqBuffer.closed = true
					ra.seqBuffer.mu.Unlock()
				}
				// After sequential write completes, return new version ID (actually the version corresponding to current DataID)
				fileObj, err := ra.getFileObj()
				if err != nil {
					DebugLog("[VFS RandomAccessor Flush] ERROR: Failed to get file object after sequential flush: fileID=%d, error=%v", ra.fileID, err)
					return 0, err
				}
				DebugLog("[VFS RandomAccessor Flush] Sequential flush completed: fileID=%d, dataID=%d, size=%d", ra.fileID, fileObj.DataID, fileObj.Size)

				// IMPORTANT: flushSequentialBuffer already updated the cache, don't re-read from database
				// Just verify the cache is correct to avoid WAL dirty read
				if cachedObj := ra.fileObj.Load(); cachedObj != nil {
					if obj, ok := cachedObj.(*core.ObjectInfo); ok && obj != nil {
						DebugLog("[VFS RandomAccessor Flush] Verified file object cache after sequential flush: fileID=%d, size=%d, dataID=%d, mtime=%d",
							obj.ID, obj.Size, obj.DataID, obj.MTime)
						fileObj = obj
					}
				}

				if fileObj.DataID > 0 {
					return core.NewID(), nil // Return new version ID
				}
			} else {
				// Buffer is not full and force=false, skip flush for better performance
				// Data will be flushed when:
				// 1. Buffer becomes full (>= chunkSize) - automatic flush in writeSequential
				// 2. Release is called (force=true) - forced flush
				DebugLog("[VFS RandomAccessor Flush] Skipping sequential buffer flush (buffer not full): fileID=%d, bufferSize=%d, chunkSize=%d, force=%v",
					ra.fileID, seqBufferSize, seqChunkSize, force)
			}
		}
	}

	// For .tmp files, use ChunkedFileWriter
	// Large .tmp files should use ChunkedFileWriter
	if ra.isTmpFile {
		// Check if ChunkedFileWriter exists (lock-free check using atomic.Value)
		hasChunkedWriter := ra.chunkedWriter.Load() != nil && ra.chunkedWriter.Load() != clearedChunkedWriterMarker

		if hasChunkedWriter {
			// ChunkedFileWriter exists, check if there are any pending writes in buffer
			writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
			if writeIndex > 0 {
				DebugLog("[VFS RandomAccessor Flush] .tmp file has pending writes in buffer but ChunkedFileWriter exists: fileID=%d, writeIndex=%d. Writing buffer data to ChunkedFileWriter.", ra.fileID, writeIndex)
				// Get ChunkedFileWriter
				if val := ra.chunkedWriter.Load(); val != nil && val != clearedChunkedWriterMarker {
					if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil && cw.writerType == WRITER_TYPE_TMP {
						// Validate ChunkedFileWriter before using it
						if cw.fileID <= 0 || cw.dataID <= 0 {
							DebugLog("[VFS RandomAccessor Flush] ERROR: ChunkedFileWriter is invalid (fileID=%d, dataID=%d), cannot write buffer operations: ra.fileID=%d", cw.fileID, cw.dataID, ra.fileID)
							ra.chunkedWriter.Store(clearedChunkedWriterMarker)
							return 0, fmt.Errorf("ChunkedFileWriter is invalid (fileID=%d, dataID=%d)", cw.fileID, cw.dataID)
						}
						// Write all pending operations from buffer to ChunkedFileWriter
						// Note: We need to read operations before clearing writeIndex
						// But we can't use atomic.Swap here because we need to read the operations
						// So we'll read them first, then clear
						opsCount := int(writeIndex)
						for i := 0; i < opsCount; i++ {
							op := ra.buffer.operations[i]
							if len(op.Data) > 0 {
								if err := cw.Write(op.Offset, op.Data); err != nil {
									DebugLog("[VFS RandomAccessor Flush] ERROR: Failed to write buffer operation to ChunkedFileWriter: fileID=%d, offset=%d, size=%d, error=%v", ra.fileID, op.Offset, len(op.Data), err)
									return 0, fmt.Errorf("failed to write buffer operation to ChunkedFileWriter: %w", err)
								}
								DebugLog("[VFS RandomAccessor Flush] Wrote buffer operation to ChunkedFileWriter: fileID=%d, offset=%d, size=%d", ra.fileID, op.Offset, len(op.Data))
							}
						}
						// Clear the buffer after writing to ChunkedFileWriter
						atomic.StoreInt64(&ra.buffer.writeIndex, 0)
						atomic.StoreInt64(&ra.buffer.totalSize, 0)
						DebugLog("[VFS RandomAccessor Flush] Successfully wrote %d buffer operations to ChunkedFileWriter: fileID=%d", opsCount, ra.fileID)
					}
				}
			}
			// For .tmp files, after ChunkedFileWriter flush, verify cache is updated
			// IMPORTANT: Don't re-read from database to avoid WAL dirty read
			if cachedObj := ra.fileObj.Load(); cachedObj != nil {
				if obj, ok := cachedObj.(*core.ObjectInfo); ok && obj != nil {
					DebugLog("[VFS RandomAccessor Flush] Verified file object cache after .tmp file flush: fileID=%d, size=%d, dataID=%d, mtime=%d",
						obj.ID, obj.Size, obj.DataID, obj.MTime)
				}
			}

			if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
				return core.NewID(), nil
			}
			return 0, nil
		}

		// ChunkedFileWriter doesn't exist - this should not happen for .tmp files
		// But handle it for safety - continue to process buffer writes below
		DebugLog("[VFS RandomAccessor Flush] Small .tmp file without ChunkedFileWriter, will use normal write path: fileID=%d", ra.fileID)
	}

	// Optimization: use atomic operation to get and clear operations (lock-free)
	// Atomically swap writeIndex and reset to 0, get actual operation count
	writeIndex := atomic.SwapInt64(&ra.buffer.writeIndex, 0)
	totalSize := atomic.SwapInt64(&ra.buffer.totalSize, 0)
	DebugLog("[VFS RandomAccessor Flush] Buffer stats: fileID=%d, writeIndex=%d, totalSize=%d", ra.fileID, writeIndex, totalSize)
	// IMPORTANT: Check both writeIndex and totalSize to determine if there are pending writes
	// writeIndex > 0 means there are operations, but totalSize should also be > 0 for valid writes
	// If writeIndex > 0 but totalSize == 0, it's likely a stale state, ignore it
	if writeIndex <= 0 || totalSize <= 0 {
		DebugLog("[VFS RandomAccessor Flush] No pending writes: fileID=%d, writeIndex=%d, totalSize=%d", ra.fileID, writeIndex, totalSize)
		// No pending writes, return 0 (no new version created)
		return 0, nil
	}

	// Reset lastOffset after flush to allow detection of new sequential write pattern
	atomic.StoreInt64(&ra.lastOffset, -1)

	// Files >= threshold: use direct write path (flush immediately to disk)
	// This ensures files are written directly without batching

	// Files >= 1MB threshold: use direct write path (flush immediately to disk)
	// This ensures large files are written directly without batching
	// Copy actually used portion (avoid modification during flush)
	operations := make([]WriteOperation, writeIndex)
	copy(operations, ra.buffer.operations[:writeIndex])

	// Optimization: use atomic operation to reset totalSize
	atomic.StoreInt64(&ra.buffer.totalSize, 0)

	// Merge overlapping write operations
	mergedOps := mergeWriteOperations(operations)

	// Get file object information
	// For normal write path, we use getFileObj and will create new DataID to overwrite
	fileObj, err = ra.getFileObj()
	if err != nil {
		return 0, err
	}
	DebugLog("[VFS RandomAccessor Flush] Got fileObj: fileID=%d, dataID=%d, size=%d, writeIndex=%d, totalSize=%d", ra.fileID, fileObj.DataID, fileObj.Size, writeIndex, totalSize)

	// Use SDK path which handles compression/encryption properly
	// Note: Sparse files and random writes are now handled by temp write area
	sparseSize := ra.getSparseSize()
	DebugLog("[VFS RandomAccessor Flush] 🔍 Using SDK path (buffer flush): fileID=%d, fileObj.DataID=%d, fileObj.Size=%d, mergedOps count=%d, sparseSize=%d", ra.fileID, fileObj.DataID, fileObj.Size, len(mergedOps), sparseSize)
	for i, op := range mergedOps {
		DebugLog("[VFS RandomAccessor Flush] MergedOp[%d]: offset=%d, size=%d", i, op.Offset, len(op.Data))
	}
	versionID, err = ra.applyRandomWritesWithSDK(fileObj, mergedOps)
	if err != nil {
		return 0, err
	}

	// IMPORTANT: applyRandomWritesWithSDK already updated the cache with correct values
	// Don't re-read from database to avoid WAL dirty read
	// Just verify the cache was updated correctly
	if cachedObj := ra.fileObj.Load(); cachedObj != nil {
		if obj, ok := cachedObj.(*core.ObjectInfo); ok && obj != nil {
			DebugLog("[VFS RandomAccessor Flush] Verified file object cache after flush: fileID=%d, size=%d, dataID=%d, mtime=%d",
				obj.ID, obj.Size, obj.DataID, obj.MTime)
		}
	}

	return versionID, nil
}

// applyRandomWritesWithSDK uses SDK's listener to handle compression and encryption, applies random writes
// Optimized for streaming processing, avoid large objects occupying too much memory
func (ra *RandomAccessor) applyRandomWritesWithSDK(fileObj *core.ObjectInfo, writes []WriteOperation) (int64, error) {
	// Get LocalHandler to access ig, ma, da
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		DebugLog("[VFS applyRandomWritesWithSDK] ERROR: handler is not LocalHandler, operation not supported: fileID=%d, writes=%d", ra.fileID, len(writes))
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	// Handle empty file case: if no writes or all writes are empty, return empty file
	// Note: We check len(writes) == 0 BEFORE filtering, because filtering happens below
	// If writes is empty, it means no write operations were passed to this function
	// This should not happen in normal flow, but we handle it for safety
	if len(writes) == 0 {
		// No writes, file remains empty
		DebugLog("[VFS applyRandomWritesWithSDK] No writes, file remains empty: fileID=%d, size=%d", ra.fileID, fileObj.Size)
		// Only update if file is actually empty (Size = 0)
		// If file has existing data (Size > 0), don't overwrite it
		if fileObj.Size == 0 {
			updateFileObj := &core.ObjectInfo{
				ID:     fileObj.ID,
				PID:    fileObj.PID,
				Type:   fileObj.Type,
				Name:   fileObj.Name,
				Size:   0,
				DataID: core.EmptyDataID,
				MTime:  core.Now(),
			}
			_, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
			if err != nil {
				DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update empty file: fileID=%d, error=%v", ra.fileID, err)
				return 0, err
			}
		}
		return 0, nil
	}

	// Filter out empty writes
	nonEmptyWrites := make([]WriteOperation, 0, len(writes))
	for _, write := range writes {
		if len(write.Data) > 0 {
			nonEmptyWrites = append(nonEmptyWrites, write)
		}
	}

	// If all writes were empty, treat as empty file
	if len(nonEmptyWrites) == 0 {
		DebugLog("[VFS applyRandomWritesWithSDK] All writes are empty, file remains empty: fileID=%d", ra.fileID)
		updateFileObj := &core.ObjectInfo{
			ID:     fileObj.ID,
			PID:    fileObj.PID,
			Type:   fileObj.Type,
			Name:   fileObj.Name,
			Size:   0,
			DataID: core.EmptyDataID,
			MTime:  core.Now(),
		}
		_, err := ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update empty file: fileID=%d, error=%v", ra.fileID, err)
			return 0, err
		}
		return 0, nil
	}

	// Use non-empty writes for processing
	writes = nonEmptyWrites

	// For .tmp files, check if ChunkedFileWriter is using the existing DataID
	// If ChunkedFileWriter exists and has the same DataID, we should not create a new one
	// Otherwise, create new DataID (for non-.tmp files or when ChunkedFileWriter doesn't exist)
	// Use the immutable isTmpFile flag (set at creation time)
	var newDataID int64
	if ra.isTmpFile && fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		// Check if ChunkedFileWriter is using this DataID (lock-free check using atomic.Value)
		hasChunkedWriter := false
		if val := ra.chunkedWriter.Load(); val != nil {
			if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil {
				hasChunkedWriter = cw.dataID == fileObj.DataID
			}
		}

		if hasChunkedWriter {
			// ChunkedFileWriter is using this DataID, reuse it
			newDataID = fileObj.DataID
			DebugLog("[VFS applyRandomWritesWithSDK] Reusing existing DataID from ChunkedFileWriter for .tmp file: fileID=%d, dataID=%d", ra.fileID, newDataID)
		} else {
			// ChunkedFileWriter doesn't exist or uses different DataID, create new one
			// This should not happen for .tmp files in normal flow, but handle it for safety
			newDataID = core.NewID()
			if newDataID <= 0 {
				return 0, fmt.Errorf("failed to generate DataID")
			}
			DebugLog("[VFS applyRandomWritesWithSDK] Creating new DataID for .tmp file (ChunkedFileWriter not using existing): fileID=%d, existingDataID=%d, newDataID=%d", ra.fileID, fileObj.DataID, newDataID)
		}
	} else {
		// Create new data ID (for non-.tmp files or .tmp files without DataID)
		newDataID = core.NewID()
		if newDataID <= 0 {
			return 0, fmt.Errorf("failed to generate DataID")
		}
		DebugLog("[VFS applyRandomWritesWithSDK] Created new DataID: fileID=%d, dataID=%d, isTmpFile=%v", ra.fileID, newDataID, ra.isTmpFile)
	}

	// Calculate new file size
	// Strategy: Use the maximum of current file size and the end position of all writes
	// This ensures:
	// 1. File extends if writes go beyond current size
	// 2. File preserves existing data beyond write regions
	// 3. Partial overwrites don't truncate the file
	oldSize := fileObj.Size
	maxEnd := int64(0)
	minOffset := int64(-1)

	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > maxEnd {
			maxEnd = writeEnd
		}
		if minOffset < 0 || write.Offset < minOffset {
			minOffset = write.Offset
		}
	}

	// Determine new file size based on write pattern
	var newSize int64
	if maxEnd > fileObj.Size {
		// Writes extend beyond current file size, use maxEnd
		newSize = maxEnd
		DebugLog("[VFS applyRandomWritesWithSDK] File size extended: fileID=%d, oldSize=%d, newSize=%d, maxEnd=%d", ra.fileID, oldSize, newSize, maxEnd)
	} else {
		// Writes are within current file bounds
		// Preserve existing file size to avoid truncation
		newSize = fileObj.Size
		DebugLog("[VFS applyRandomWritesWithSDK] File size preserved (writes within bounds): fileID=%d, size=%d, maxEnd=%d", ra.fileID, newSize, maxEnd)
	}

	// For sparse files, use sparseSize instead of calculated newSize for file object size
	// newSize is the actual data size, but file object size should be sparseSize
	sparseSize := ra.getSparseSize()
	DebugLog("[VFS applyRandomWritesWithSDK] 🔍 Checking sparse file: fileID=%d, sparseSize=%d, newSize=%d, maxEnd=%d", ra.fileID, sparseSize, newSize, maxEnd)
	if sparseSize > 0 {
		// For sparse files, always use sparseSize as file object size, regardless of newSize
		// This ensures sparse files maintain their virtual size even if only partial data is written
		// IMPORTANT: This must be done BEFORE creating updateFileObj, so updateFileObj.Size uses sparseSize
		fileSize := sparseSize
		DebugLog("[VFS applyRandomWritesWithSDK] ✅ Using sparseSize for sparse file: fileID=%d, sparseSize=%d, newSize=%d, setting fileSize=%d", ra.fileID, sparseSize, newSize, fileSize)
		// Update newSize to sparseSize so that updateFileObj.Size will be set correctly
		newSize = fileSize
	} else {
		DebugLog("[VFS applyRandomWritesWithSDK] ⚠️ Not a sparse file or sparseSize=0: fileID=%d, sparseSize=%d, newSize=%d", ra.fileID, sparseSize, newSize)
	}

	// Check if original data is compressed or encrypted (optimized: use cache)
	var oldDataInfo *core.DataInfo
	var hasCompression, hasEncryption bool
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// Optimization: use more efficient key generation (function internally uses object pool)
		dataInfoCacheKey := oldDataID

		if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
			if info, ok := cached.(*core.DataInfo); ok && info != nil {
				oldDataInfo = info
			}
		}

		// If cache miss, get from database
		if oldDataInfo == nil {
			var err error
			oldDataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, oldDataID)
			if err == nil && oldDataInfo != nil {
				// Update cache (reuse generated key)
				dataInfoCache.Put(dataInfoCacheKey, oldDataInfo)
			}
		}

		if oldDataInfo != nil {
			hasCompression = oldDataInfo.Kind&core.DATA_CMPR_MASK != 0
			hasEncryption = oldDataInfo.Kind&core.DATA_ENDEC_MASK != 0
		}
	}

	// Create DataInfo
	// Set Kind based on OrcasFS configuration (for new files without old data)
	cmprWay := getCmprWayForFS(ra.fs)
	endecWay := getEndecWayForFS(ra.fs)
	kind := core.DATA_NORMAL
	if cmprWay > 0 {
		kind |= cmprWay
	}
	if endecWay > 0 {
		kind |= endecWay
	}
	// IMPORTANT: DataInfo.OrigSize should match the actual file size (newSize)
	// newSize already considers whether to extend or preserve based on write pattern
	// So we should use newSize directly, not try to preserve old size again
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: newSize, // Use newSize which already handles size logic correctly
		Kind:     kind,
	}
	DebugLog("[VFS applyRandomWritesWithSDK] DataInfo.OrigSize set to: fileID=%d, OrigSize=%d, oldSize=%d", ra.fileID, newSize, oldSize)

	// Check if this is a .tmp file with ChunkedFileWriter
	// If so, ChunkedFileWriter already handles compression/encryption, so don't use applyWritesStreamingCompressed
	// Lock-free check using atomic.Value
	hasChunkedWriter := ra.chunkedWriter.Load() != nil

	// Check if OrcasFS has compression or encryption configuration
	// For new files (no old data), if OrcasFS has config, should use compression/encryption path
	// BUT: If ChunkedFileWriter exists, it already handles compression/encryption, so skip this path
	hasBucketConfig := cmprWay > 0 || endecWay > 0

	// If original data is compressed or encrypted, must read completely (unavoidable)
	// But can stream write, avoid processing all data at once
	// Also, for new files with bucket config, use compression/encryption path
	// Note: applyWritesStreamingCompressed can handle oldDataInfo == nil (new files)
	// BUT: If ChunkedFileWriter exists, it already handles compression/encryption, so don't use this path
	if !hasChunkedWriter && (hasCompression || hasEncryption || ((oldDataID == 0 || oldDataID == core.EmptyDataID) && hasBucketConfig)) {
		DebugLog("[VFS applyRandomWritesWithSDK] Processing compressed/encrypted data for fileID=%d, newDataID=%d, hasOldData=%v, hasBucketConfig=%v", ra.fileID, newDataID, hasCompression || hasEncryption, hasBucketConfig)
		newVersionID, err := ra.applyWritesStreamingCompressed(oldDataInfo, writes, dataInfo, newSize)
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to apply writes (compressed/encrypted) for fileID=%d: %v", ra.fileID, err)
			if err == core.ERR_QUOTA_EXCEED {
				DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Quota exceeded for fileID=%d", ra.fileID)
			}
			return 0, err
		}

		// Optimization: use time calibrator to get timestamp, reduce time.Now() calls and GC pressure
		// Create new version object
		mTime := core.Now()
		newVersion := &core.ObjectInfo{
			ID:     newVersionID,
			PID:    ra.fileID,
			Type:   core.OBJ_TYPE_VERSION,
			DataID: newDataID,
			Size:   newSize,
			MTime:  mTime,
		}

		// Create version object first
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{newVersion})
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to create version: %v", err)
			return 0, err
		}

		// Update file object using SetObj to ensure Size field is updated
		// Put uses InsertIgnore which won't update existing objects
		updateFileObj := &core.ObjectInfo{
			ID:     ra.fileID,
			PID:    fileObj.PID,
			Type:   fileObj.Type,
			Name:   fileObj.Name,
			DataID: newDataID,
			Size:   newSize,
			MTime:  mTime,
		}
		DebugLog("[VFS applyRandomWritesWithSDK] 🔍 Calling SetObj to update file object (no DataInfo): fileID=%d, DataID=%d, Size=%d, MTime=%d", ra.fileID, updateFileObj.DataID, updateFileObj.Size, updateFileObj.MTime)
		err = lh.MetadataAdapter().SetObj(ra.fs.c, ra.fs.bktID, []string{"did", "s", "m"}, updateFileObj)

		// Update cached file object information
		if err == nil {
			// Optimization: use pre-computed key (avoid repeated conversion)
			fileObjCache.Put(ra.fileObjKey, updateFileObj)
			ra.fileObj.Store(updateFileObj)
			DebugLog("[VFS applyRandomWritesWithSDK] Successfully applied writes (compressed/encrypted): fileID=%d, versionID=%d, newDataID=%d, size=%d", ra.fileID, newVersionID, newDataID, newSize)
			// Update directory listing cache to ensure file is visible in Readdir
			if updateFileObj.PID > 0 {
				dirNode := &OrcasNode{
					fs:    ra.fs,
					objID: updateFileObj.PID,
				}
				dirNode.invalidateDirListCache(updateFileObj.PID)
				DebugLog("[VFS applyRandomWritesWithSDK] Appended file to directory listing cache: fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
			}
		} else {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update objects: fileID=%d, error=%v", ra.fileID, err)
		}

		return newVersionID, err
	}

	// For uncompressed unencrypted data, can stream read and process by chunk
	newVersionID, err := ra.applyWritesStreamingUncompressed(fileObj, oldDataInfo, writes, dataInfo, newSize)
	if err != nil {
		return 0, err
	}

	// Optimization: use time calibrator to get timestamp, reduce time.Now() calls and GC pressure
	// Create new version object
	mTime := core.Now()
	newVersion := &core.ObjectInfo{
		ID:     newVersionID,
		PID:    ra.fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// Optimization: batch write metadata (write version object and file object update together)
	objectsToPut := []*core.ObjectInfo{newVersion}
	// Also update file object (if file object itself needs update)
	// IMPORTANT: Must include Type, Name, PID, MTime to avoid cache corruption
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		PID:    fileObj.PID,
		Type:   fileObj.Type,
		Name:   fileObj.Name,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}
	objectsToPut = append(objectsToPut, updateFileObj)

	// Use Put method to batch create version and update file object (will automatically apply version retention policy)
	_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)

	// Update cached file object information
	if err == nil {
		// Optimization: use pre-computed key (avoid repeated conversion)
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
		DebugLog("[VFS applyRandomWritesWithSDK] Successfully applied writes (uncompressed/unencrypted): fileID=%d, versionID=%d, newDataID=%d, size=%d", ra.fileID, newVersionID, newDataID, newSize)
		// Update directory listing cache to ensure file is visible in Readdir
		if updateFileObj.PID > 0 {
			dirNode := &OrcasNode{
				fs:    ra.fs,
				objID: updateFileObj.PID,
			}
			dirNode.invalidateDirListCache(updateFileObj.PID)
			DebugLog("[VFS applyRandomWritesWithSDK] Appended file to directory listing cache: fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
		}
	} else {
		DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to update objects: fileID=%d, error=%v", ra.fileID, err)
	}

	return newVersionID, err
}

// applyAppendWithSDK handles pure append operations without reprocessing existing chunks
// This is a critical optimization for sequential uploads: when appending data to existing file,
// only new chunks need to be processed, old chunks can be referenced directly
func (ra *RandomAccessor) applyAppendWithSDK(lh *core.LocalHandler, fileObj *core.ObjectInfo, appendOffset int64, appendData []byte) (int64, error) {
	DebugLog("[VFS applyAppendWithSDK] Starting append: fileID=%d, oldSize=%d, appendOffset=%d, appendSize=%d",
		ra.fileID, fileObj.Size, appendOffset, len(appendData))

	// Get old DataInfo
	oldDataInfo, err := lh.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
	if err != nil {
		DebugLog("[VFS applyAppendWithSDK] Failed to get old DataInfo: fileID=%d, error=%v", ra.fileID, err)
		return 0, err
	}

	// Create new DataID for appended data
	newDataID := core.NewID()
	if newDataID <= 0 {
		return 0, fmt.Errorf("failed to generate DataID")
	}

	da := lh.GetDataAdapter()
	if da == nil {
		return 0, fmt.Errorf("failed to get DataAdapter")
	}

	chunkSize := ra.fs.chunkSize
	oldSize := fileObj.Size
	newSize := appendOffset + int64(len(appendData))

	// Calculate chunk ranges
	oldChunkCount := int((oldSize + chunkSize - 1) / chunkSize)
	newChunkCount := int((newSize + chunkSize - 1) / chunkSize)

	DebugLog("[VFS applyAppendWithSDK] Chunk analysis: oldChunkCount=%d, newChunkCount=%d, chunkSize=%d",
		oldChunkCount, newChunkCount, chunkSize)

	// 1. Copy existing chunks directly (encrypted data, no reprocessing!)
	for chunkIdx := 0; chunkIdx < oldChunkCount; chunkIdx++ {
		encryptedChunk, err := da.Read(ra.fs.c, ra.fs.bktID, fileObj.DataID, chunkIdx)
		if err != nil {
			DebugLog("[VFS applyAppendWithSDK] Failed to read old chunk %d: %v", chunkIdx, err)
			return 0, fmt.Errorf("failed to read old chunk %d: %w", chunkIdx, err)
		}

		err = da.Write(ra.fs.c, ra.fs.bktID, newDataID, chunkIdx, encryptedChunk)
		if err != nil {
			DebugLog("[VFS applyAppendWithSDK] Failed to write copied chunk %d: %v", chunkIdx, err)
			return 0, fmt.Errorf("failed to write copied chunk %d: %w", chunkIdx, err)
		}

		DebugLog("[VFS applyAppendWithSDK] Copied chunk %d directly (size=%d, no decrypt/encrypt)",
			chunkIdx, len(encryptedChunk))
	}

	// 2. Process and write new chunks
	// Use ProcessData to handle compression/encryption consistently
	for chunkIdx := oldChunkCount; chunkIdx < newChunkCount; chunkIdx++ {
		chunkOffset := int64(chunkIdx) * chunkSize
		dataOffset := chunkOffset - appendOffset
		chunkEnd := chunkOffset + chunkSize
		if chunkEnd > newSize {
			chunkEnd = newSize
		}
		chunkLength := chunkEnd - chunkOffset

		var chunkData []byte
		if dataOffset >= 0 && dataOffset < int64(len(appendData)) {
			dataEnd := dataOffset + chunkLength
			if dataEnd > int64(len(appendData)) {
				dataEnd = int64(len(appendData))
			}
			chunkData = appendData[dataOffset:dataEnd]
		} else {
			chunkData = make([]byte, chunkLength) // Zero-filled
		}

		// Process chunk (compress + encrypt)
		processKind := oldDataInfo.Kind // Inherit kind from old data
		processedChunk, err := core.ProcessData(chunkData, &processKind, ra.fs.CmprQlty, ra.fs.EndecKey, false)
		if err != nil {
			DebugLog("[VFS applyAppendWithSDK] Failed to process new chunk %d: %v", chunkIdx, err)
			return 0, fmt.Errorf("failed to process new chunk %d: %w", chunkIdx, err)
		}

		err = da.Write(ra.fs.c, ra.fs.bktID, newDataID, chunkIdx, processedChunk)
		if err != nil {
			DebugLog("[VFS applyAppendWithSDK] Failed to write new chunk %d: %v", chunkIdx, err)
			return 0, fmt.Errorf("failed to write new chunk %d: %w", chunkIdx, err)
		}

		DebugLog("[VFS applyAppendWithSDK] Wrote new chunk %d (offset=%d, size=%d, processed=%d)",
			chunkIdx, chunkOffset, len(chunkData), len(processedChunk))
	}

	// 3. Calculate hashes for new file
	// Note: For append, we need to read all chunks to calculate hash
	// But at least we avoid re-encrypting old chunks
	finalData := make([]byte, newSize)
	for chunkIdx := 0; chunkIdx < newChunkCount; chunkIdx++ {
		chunkOffset := int64(chunkIdx) * chunkSize
		chunkEnd := chunkOffset + chunkSize
		if chunkEnd > newSize {
			chunkEnd = newSize
		}
		actualChunkSize := int(chunkEnd - chunkOffset)

		// Read decrypted chunk data for hash calculation
		chunkData, err := lh.GetData(ra.fs.c, ra.fs.bktID, newDataID, chunkIdx)
		if err != nil {
			DebugLog("[VFS applyAppendWithSDK] Failed to read chunk %d for hashing: %v", chunkIdx, err)
			return 0, fmt.Errorf("failed to read chunk %d for hashing: %w", chunkIdx, err)
		}

		if len(chunkData) > actualChunkSize {
			chunkData = chunkData[:actualChunkSize]
		}
		copy(finalData[chunkOffset:], chunkData)
	}

	// Calculate hashes
	hdrSize := int(core.DefaultHdrSize)
	if hdrSize > len(finalData) {
		hdrSize = len(finalData)
	}
	hdrXXH3 := int64(xxh3.Hash(finalData[:hdrSize]))
	dataXXH3 := int64(xxh3.Hash(finalData))
	dataSHA256 := sha256.Sum256(finalData)

	// 4. Create DataInfo
	newDataInfo := &core.DataInfo{
		ID:       newDataID,
		Kind:     oldDataInfo.Kind, // Inherit kind
		OrigSize: newSize,
		HdrXXH3:  hdrXXH3,
		XXH3:     dataXXH3,
		SHA256_0: int64(binary.BigEndian.Uint64(dataSHA256[0:8])),
		SHA256_1: int64(binary.BigEndian.Uint64(dataSHA256[8:16])),
		SHA256_2: int64(binary.BigEndian.Uint64(dataSHA256[16:24])),
		SHA256_3: int64(binary.BigEndian.Uint64(dataSHA256[24:32])),
	}

	_, err = lh.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{newDataInfo})
	if err != nil {
		DebugLog("[VFS applyAppendWithSDK] Failed to write DataInfo: fileID=%d, error=%v", ra.fileID, err)
		return 0, err
	}

	// 5. Create version and update file object
	versionID := core.NewID()
	if versionID == 0 {
		return 0, fmt.Errorf("failed to generate version ID")
	}

	mTime := core.Now()
	newVersion := &core.ObjectInfo{
		ID:     versionID,
		PID:    ra.fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{newVersion})
	if err != nil {
		DebugLog("[VFS applyAppendWithSDK] Failed to create version: fileID=%d, error=%v", ra.fileID, err)
		return 0, err
	}

	// Update file object
	fileObj.DataID = newDataID
	fileObj.Size = newSize
	fileObj.MTime = mTime

	// For sparse files, preserve sparseSize
	sparseSize := ra.getSparseSize()
	if sparseSize > 0 && sparseSize > newSize {
		fileObj.Size = sparseSize
		DebugLog("[VFS applyAppendWithSDK] Using sparseSize for file: fileID=%d, sparseSize=%d, newSize=%d",
			ra.fileID, sparseSize, newSize)
	}

	err = lh.MetadataAdapter().SetObj(ra.fs.c, ra.fs.bktID, []string{"did", "s", "m"}, fileObj)
	if err != nil {
		DebugLog("[VFS applyAppendWithSDK] Failed to update file object: fileID=%d, error=%v", ra.fileID, err)
		return 0, err
	}

	// Update caches
	dataInfoCache.Put(newDataID, newDataInfo)
	fileObjCache.Put(ra.fileObjKey, fileObj)
	ra.fileObj.Store(fileObj)

	// Invalidate directory listing cache
	if fileObj.PID > 0 {
		dirNode := &OrcasNode{
			fs:    ra.fs,
			objID: fileObj.PID,
		}
		dirNode.invalidateDirListCache(fileObj.PID)
	}

	DebugLog("[VFS applyAppendWithSDK] Successfully completed append: fileID=%d, oldSize=%d, newSize=%d, oldChunks=%d (copied), newChunks=%d (processed)",
		ra.fileID, oldSize, newSize, oldChunkCount, newChunkCount-oldChunkCount)

	return versionID, nil
}

// applyWritesStreamingCompressed handles compressed or encrypted data
// Streaming processing: read original data by chunk, apply write operations, process and write to new object immediately
func (ra *RandomAccessor) applyWritesStreamingCompressed(oldDataInfo *core.DataInfo, writes []WriteOperation,
	dataInfo *core.DataInfo, newSize int64,
) (int64, error) {
	// Now each chunk is independently compressed and encrypted, can process by chunk in streaming
	// Directly read by chunk, decrypt, decompress, don't use DataReader

	// Get encryption key from OrcasFS (not from bucket config)
	endecKey := getEndecKeyForFS(ra.fs)

	chunkSize := ra.fs.chunkSize

	// Create a reader to read, decrypt, and decompress by chunk
	// IMPORTANT: For compressed/encrypted data, we need to read from the old file
	// Use the larger of oldDataInfo.OrigSize and newSize to ensure we can read all necessary data
	var dataInfoForReader *core.DataInfo
	var processingSize int64 = newSize
	if oldDataInfo != nil {
		// Use original file size for reading if it's larger than newSize
		// This ensures we can preserve data beyond the write regions
		readerSize := newSize
		if oldDataInfo.OrigSize > newSize {
			readerSize = oldDataInfo.OrigSize
			processingSize = oldDataInfo.OrigSize
			DebugLog("[VFS applyWritesStreamingCompressed] Using old file size for reading: fileID=%d, oldOrigSize=%d, newSize=%d", ra.fileID, oldDataInfo.OrigSize, newSize)
		}
		dataInfoForReader = &core.DataInfo{
			ID:        oldDataInfo.ID,
			OrigSize:  readerSize, // Use larger size to read all necessary data
			Size:      oldDataInfo.Size,
			Kind:      oldDataInfo.Kind,
			PkgID:     oldDataInfo.PkgID,
			PkgOffset: oldDataInfo.PkgOffset,
		}
	}
	reader := newChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, dataInfoForReader, endecKey, chunkSize)

	// Pre-calculate write operation indices for each chunk
	// Use processingSize for chunk count to ensure we process all necessary chunks
	chunkCount := int((processingSize + chunkSize - 1) / chunkSize)
	if chunkCount == 0 {
		// Empty file, no chunks needed
		chunkCount = 1
	}
	writesByChunk := make([][]int, chunkCount)
	if chunkCount > 0 {
		avgWritesPerChunk := len(writes) / chunkCount
		if avgWritesPerChunk < 1 {
			avgWritesPerChunk = 1
		}
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}
	} else {
		avgWritesPerChunk := 1
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}
	}

	for i, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		startChunk := int(write.Offset / chunkSize)
		endChunk := int((writeEnd + chunkSize - 1) / chunkSize)
		if endChunk >= chunkCount {
			endChunk = chunkCount - 1
		}
		if startChunk < 0 {
			startChunk = 0
		}
		for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
			writesByChunk[chunkIdx] = append(writesByChunk[chunkIdx], i)
		}
	}

	// Stream processing: read, process, and write by chunk
	// Pass processingSize for chunk processing, but dataInfo.OrigSize will be set to newSize in the caller
	return ra.processWritesStreaming(reader, writesByChunk, writes, dataInfo, processingSize, chunkCount)
}

// applyWritesStreamingUncompressed handles uncompressed and unencrypted data
// Can stream read and process by chunk, only reading affected data ranges
func (ra *RandomAccessor) applyWritesStreamingUncompressed(fileObj *core.ObjectInfo, oldDataInfo *core.DataInfo,
	writes []WriteOperation, dataInfo *core.DataInfo, newSize int64,
) (int64, error) {
	// For uncompressed and unencrypted data, can stream process by chunk
	// Optimization: pre-calculate write operations for each chunk to reduce repeated checks in loops

	chunkSize := ra.fs.chunkSize
	// Stream processing: read original data by chunk, apply write operations, process and write to new object immediately
	// Pre-calculate write operation indices for each chunk
	// IMPORTANT: newSize already reflects the correct final file size from applyRandomWritesWithSDK
	// We should use newSize directly for the final file size
	// However, for chunk processing, we need to read from old file if it's larger (to preserve data)
	oldFileSize := fileObj.Size
	if oldDataInfo != nil && oldDataInfo.OrigSize > 0 {
		oldFileSize = oldDataInfo.OrigSize
	}
	// For chunk count calculation, use the larger size to ensure we process all necessary chunks
	// But the final file size will be newSize (set later)
	totalSize := newSize
	if oldFileSize > newSize {
		totalSize = oldFileSize
		DebugLog("[VFS applyWritesStreamingUncompressed] Need to read from old file (larger than new): fileID=%d, oldSize=%d, newSize=%d, processingSize=%d", ra.fileID, oldFileSize, newSize, totalSize)
	}
	chunkCount := int((totalSize + chunkSize - 1) / chunkSize)
	if chunkCount == 0 {
		// Empty file, no chunks needed
		chunkCount = 1
	}
	DebugLog("[VFS applyWritesStreamingCompressed] Calculated chunkCount: fileID=%d, dataID=%d, newSize=%d, chunkSize=%d, chunkCount=%d, writes count=%d", ra.fileID, dataInfo.ID, newSize, chunkSize, chunkCount, len(writes))
	writesByChunk := make([][]int, chunkCount)
	if chunkCount > 0 {
		avgWritesPerChunk := len(writes) / chunkCount
		if avgWritesPerChunk < 1 {
			avgWritesPerChunk = 1
		}
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}
	} else {
		avgWritesPerChunk := 1
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}
	}

	for i, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		startChunk := int(write.Offset / chunkSize)
		endChunk := int((writeEnd + chunkSize - 1) / chunkSize)
		if endChunk >= chunkCount {
			endChunk = chunkCount - 1
		}
		if startChunk < 0 {
			startChunk = 0
		}
		DebugLog("[VFS applyWritesStreamingCompressed] Write[%d]: offset=%d, size=%d, startChunk=%d, endChunk=%d", i, write.Offset, len(write.Data), startChunk, endChunk)
		for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
			writesByChunk[chunkIdx] = append(writesByChunk[chunkIdx], i)
		}
	}
	// Log writesByChunk distribution
	for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
		if len(writesByChunk[chunkIdx]) > 0 {
			DebugLog("[VFS applyWritesStreamingCompressed] writesByChunk[%d]: %d writes", chunkIdx, len(writesByChunk[chunkIdx]))
		}
	}

	// For uncompressed and unencrypted data, can directly read by chunk without reading all data first
	// Create a special reader to support reading by chunk
	var reader dataReader
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// Use oldDataInfo if available (has full metadata), otherwise create minimal one
		// IMPORTANT: Use original file size (oldDataInfo.OrigSize or fileObj.Size) to read all data
		// This ensures that when writing 4KB to an 84KB file, we can copy the remaining 80KB
		// Only limit to newSize if file was actually truncated (newSize < oldSize)
		var dataInfoForReader *core.DataInfo
		oldFileSize := fileObj.Size
		if oldDataInfo != nil && oldDataInfo.OrigSize > 0 {
			oldFileSize = oldDataInfo.OrigSize
		}
		// Use the larger of oldFileSize and newSize to ensure we can read all remaining data
		readerSize := oldFileSize
		if newSize > oldFileSize {
			readerSize = newSize
		}

		if oldDataInfo != nil {
			dataInfoForReader = &core.DataInfo{
				ID:        oldDataInfo.ID,
				OrigSize:  readerSize, // Use original size to read all remaining data
				Size:      oldDataInfo.Size,
				Kind:      oldDataInfo.Kind,
				PkgID:     oldDataInfo.PkgID,
				PkgOffset: oldDataInfo.PkgOffset,
			}
			if readerSize > newSize {
				DebugLog("[VFS applyWritesStreamingUncompressed] Using original file size for reader to preserve remaining data: fileID=%d, oldOrigSize=%d, newSize=%d, readerSize=%d", ra.fileID, oldDataInfo.OrigSize, newSize, readerSize)
			}
		} else {
			// Fallback: create minimal DataInfo (should not happen for existing files, but handle for safety)
			dataInfoForReader = &core.DataInfo{ID: oldDataID, OrigSize: readerSize}
		}
		// Create chunkReader to support reading by chunk
		reader = newChunkReader(ra.fs.c, ra.fs.h, ra.fs.bktID, dataInfoForReader, "", chunkSize)
	}

	// Stream processing: read, process, and write by chunk
	// IMPORTANT: Pass totalSize for chunk processing (to read all necessary data from old file)
	// But the final file size will be newSize (set below)
	DebugLog("[VFS applyWritesStreamingUncompressed] Calling processWritesStreaming: fileID=%d, dataID=%d, newSize=%d, processingSize=%d, chunkCount=%d, writes count=%d", ra.fileID, dataInfo.ID, newSize, totalSize, chunkCount, len(writes))
	newVersionID, err := ra.processWritesStreaming(reader, writesByChunk, writes, dataInfo, totalSize, chunkCount)
	if err != nil {
		DebugLog("[VFS applyWritesStreamingUncompressed] ERROR: processWritesStreaming failed: fileID=%d, error=%v", ra.fileID, err)
		return 0, err
	}

	// Update fileObj with new DataID and size
	// This ensures subsequent operations use the correct DataID
	// IMPORTANT: Use newSize (not totalSize) as the final file size
	// newSize already reflects the correct size based on write operations
	updateFileObj, err := ra.getFileObj()
	if err == nil && updateFileObj != nil {
		oldDataID := updateFileObj.DataID
		oldSize := updateFileObj.Size
		updateFileObj.DataID = dataInfo.ID
		// Use newSize as the final file size (already calculated correctly in applyRandomWritesWithSDK)
		updateFileObj.Size = newSize
		// Update dataInfo.OrigSize to match the actual file size
		dataInfo.OrigSize = newSize
		updateFileObj.MTime = core.Now()
		DebugLog("[VFS applyWritesStreamingUncompressed] Updating fileObj: fileID=%d, oldDataID=%d, newDataID=%d, oldSize=%d, newSize=%d", ra.fileID, oldDataID, dataInfo.ID, oldSize, newSize)
		_, err = ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			DebugLog("[VFS applyWritesStreamingUncompressed] ERROR: Failed to update fileObj: fileID=%d, dataID=%d, error=%v", ra.fileID, dataInfo.ID, err)
			return 0, err
		}
		// Update cache
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
		DebugLog("[VFS applyWritesStreamingUncompressed] Successfully updated fileObj: fileID=%d, dataID=%d, size=%d", ra.fileID, dataInfo.ID, dataInfo.OrigSize)
	} else {
		DebugLog("[VFS applyWritesStreamingUncompressed] WARNING: Failed to get fileObj for update: fileID=%d, error=%v", ra.fileID, err)
	}

	return newVersionID, nil
}

// processWritesStreaming streams processing of write operations
// Read original data by chunk, apply write operations, immediately process (compress/encrypt) and write to new object
func (ra *RandomAccessor) processWritesStreaming(
	reader dataReader,
	writesByChunk [][]int,
	writes []WriteOperation,
	dataInfo *core.DataInfo,
	newSize int64,
	chunkCount int,
) (int64, error) {
	// Get compression and encryption settings from OrcasFS
	cmprWay := getCmprWayForFS(ra.fs)
	cmprQlty := getCmprQltyForFS(ra.fs)
	endecWay := getEndecWayForFS(ra.fs)

	// Initialize compressor (if smart compression is enabled)
	var cmpr archiver.Compressor
	var hasCmpr bool
	if cmprWay > 0 {
		// Compressor will be determined when processing the first chunk (needs to check file type)
		hasCmpr = true
		cmpr = core.CreateCompressor(cmprWay, cmprQlty)
		if cmpr != nil {
			dataInfo.Kind |= cmprWay
		}
	}

	// If encryption is set, set encryption flag
	if endecWay > 0 {
		dataInfo.Kind |= endecWay
		DebugLog("[VFS processWritesStreaming] Set encryption flag: fileID=%d, dataID=%d, EndecWay=0x%x, Kind=0x%x", ra.fileID, dataInfo.ID, endecWay, dataInfo.Kind)
	}

	// Calculate XXH3 and SHA-256 (original data)
	var xxh3Hash *xxh3.Hasher
	var sha256Hash hash.Hash
	var dataXXH3 uint64
	var hdrXXH3Calculated bool // Track if HdrXXH3 is calculated

	sn := 0
	firstChunk := true
	chunkSizeInt := int(ra.fs.chunkSize)

	// Get LocalHandler and DataAdapter for direct chunk access (optimization)
	lh, _ := ra.fs.h.(*core.LocalHandler)
	var da core.DataAdapter
	if lh != nil {
		da = lh.GetDataAdapter()
	}
	var oldDataID int64
	var hasOldData bool
	if reader != nil {
		if cr, ok := reader.(*chunkReader); ok {
			oldDataID = cr.dataID
			hasOldData = oldDataID > 0
		}
	}

	// Stream process by chunk
	DebugLog("[VFS processWritesStreaming] Starting chunk processing: fileID=%d, dataID=%d, chunkCount=%d, newSize=%d, chunkSize=%d, hasOldData=%v, canOptimize=%v", ra.fileID, dataInfo.ID, chunkCount, newSize, chunkSizeInt, hasOldData, da != nil)
	for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
		pos := int64(chunkIdx * chunkSizeInt)
		chunkEnd := pos + int64(chunkSizeInt)
		// IMPORTANT: newSize parameter now represents totalSize (max of oldSize and newSize)
		// This ensures we process all chunks, including those beyond the original newSize
		// Only limit chunkEnd if it exceeds totalSize (which should not happen if chunkCount is correct)
		if chunkEnd > newSize {
			chunkEnd = newSize
		}
		actualChunkSize := int(chunkEnd - pos)
		if actualChunkSize == 0 {
			// Skip empty chunks
			continue
		}

		hasWrites := len(writesByChunk[chunkIdx]) > 0

		// OPTIMIZATION: For unmodified chunks with existing encrypted data, copy directly
		// This avoids expensive read→decrypt→re-encrypt→write cycle
		// Still need to read original data for hash calculation, but can skip re-encryption
		if !hasWrites && hasOldData && da != nil {
			// Read encrypted chunk directly from old DataID
			encryptedChunk, err := da.Read(ra.fs.c, ra.fs.bktID, oldDataID, chunkIdx)
			if err == nil && len(encryptedChunk) > 0 {
				// Successfully read encrypted chunk, copy it directly to new DataID
				err = da.Write(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, encryptedChunk)
				if err == nil {
					// Success! Skip decryption and re-encryption
					DebugLog("[VFS processWritesStreaming] Copied encrypted chunk directly: fileID=%d, chunkIdx=%d, sn=%d, size=%d (saved decrypt+encrypt)",
						ra.fileID, chunkIdx, sn, len(encryptedChunk))

					// Still need to read original data for hash calculation
					chunkData := allocChunkData(actualChunkSize)

					n, readErr := reader.Read(chunkData, pos)
					if readErr == nil || readErr == io.EOF {
						// Update hash with original data
						if xxh3Hash == nil {
							xxh3Hash = xxh3.New()
							sha256Hash = sha256.New()
						}

						// Handle HdrXXH3 calculation
						if !hdrXXH3Calculated {
							hdrEnd := dataInfo.OrigSize + int64(n)
							if hdrEnd <= core.DefaultHdrSize {
								xxh3Hash.Write(chunkData[:n])
							} else if dataInfo.OrigSize < core.DefaultHdrSize {
								remainingHdrSize := core.DefaultHdrSize - dataInfo.OrigSize
								if remainingHdrSize > 0 && remainingHdrSize < int64(n) {
									xxh3Hash.Write(chunkData[:remainingHdrSize])
								} else {
									xxh3Hash.Write(chunkData[:n])
								}
								dataInfo.HdrXXH3 = int64(xxh3Hash.Sum64())
								hdrXXH3Calculated = true
								xxh3Hash = xxh3.New()
								xxh3Hash.Write(chunkData[:n])
							} else {
								hdrSize := int(core.DefaultHdrSize)
								if hdrSize > n {
									hdrSize = n
								}
								dataInfo.HdrXXH3 = int64(xxh3.Hash(chunkData[:hdrSize]))
								hdrXXH3Calculated = true
								xxh3Hash.Write(chunkData[:n])
							}
						} else {
							xxh3Hash.Write(chunkData[:n])
						}

						sha256Hash.Write(chunkData[:n])
						dataXXH3 = xxh3Hash.Sum64()
					}

					sn++
					continue // Skip normal processing
				}
			}
			// If direct copy failed, fall through to normal processing
		}

		// IMPORTANT: For chunks beyond the original newSize, we need to read the full chunk from original file
		// actualChunkSize is already correct based on totalSize
		DebugLog("[VFS processWritesStreaming] Processing chunk: fileID=%d, dataID=%d, chunkIdx=%d, sn=%d, pos=%d, chunkEnd=%d, actualChunkSize=%d, writesCount=%d", ra.fileID, dataInfo.ID, chunkIdx, sn, pos, chunkEnd, actualChunkSize, len(writesByChunk[chunkIdx]))

		// Allocate chunk buffer on demand (no object pool)
		chunkData := allocChunkData(actualChunkSize)

		// 1. Read this chunk of original data from reader (using Read(buf, offset))
		if reader != nil {
			// Read current chunk data directly from offset
			// IMPORTANT: Read the full actualChunkSize to ensure we get all data from original file
			n, err := reader.Read(chunkData, pos)
			if err != nil && err != io.EOF {
				// If chunk doesn't exist (e.g., file was extended), treat as zero-filled
				// This is normal when writing to a new part of the file
				errStr := err.Error()
				if strings.Contains(errStr, "not found") || strings.Contains(errStr, "cannot find") ||
					strings.Contains(errStr, "no such file") || strings.Contains(errStr, "does not exist") {
					DebugLog("[VFS processWritesStreaming] Chunk not found (expected for new data), zero-filling: fileID=%d, dataID=%d, chunkIdx=%d, sn=%d, pos=%d, error=%v", ra.fileID, dataInfo.ID, chunkIdx, sn, pos, err)
					// Zero-fill the entire chunk (it's new data)
					for i := range chunkData {
						chunkData[i] = 0
					}
					n = 0 // Treat as no data read
				} else {
					// For other errors, return error
					return 0, fmt.Errorf("failed to read chunk: %w", err)
				}
			}
			// If read data is less than chunk size, remaining part stays as 0 (new data)
			// IMPORTANT: For chunks beyond original write size, we should have read all data
			// If n < actualChunkSize, it means we're at the end of the file or there's an issue
			if n < actualChunkSize {
				// Zero remaining part only if we're beyond the original file size
				// Otherwise, this might indicate a problem
				DebugLog("[VFS processWritesStreaming] Read less data than expected: fileID=%d, dataID=%d, chunkIdx=%d, pos=%d, n=%d, actualChunkSize=%d", ra.fileID, dataInfo.ID, chunkIdx, pos, n, actualChunkSize)
				for i := n; i < actualChunkSize; i++ {
					chunkData[i] = 0
				}
			}
		} else {
			// No original data, initialize to 0
			for i := range chunkData {
				chunkData[i] = 0
			}
		}

		// 2. Apply write operations to current chunk
		for _, writeIdx := range writesByChunk[chunkIdx] {
			write := writes[writeIdx]
			writeEnd := write.Offset + int64(len(write.Data))

			// Calculate overlap range
			overlapStart := int64(0)
			if write.Offset > pos {
				overlapStart = write.Offset - pos
			}
			overlapEnd := int64(actualChunkSize)
			if writeEnd-pos < overlapEnd {
				overlapEnd = writeEnd - pos
			}
			if overlapStart < overlapEnd {
				writeDataStart := int64(0)
				if write.Offset < pos {
					writeDataStart = pos - write.Offset
				}
				copyLen := overlapEnd - overlapStart
				copy(chunkData[overlapStart:overlapEnd], write.Data[writeDataStart:writeDataStart+copyLen])
			}
		}

		// 3. If it's the first chunk, check file type to determine if compression is needed
		if firstChunk && cmprWay > 0 && len(chunkData) > 0 {
			kind, _ := filetype.Match(chunkData)
			if kind != filetype.Unknown {
				// Not unknown type, don't compress
				dataInfo.Kind &= ^core.DATA_CMPR_MASK
				cmpr = nil
				hasCmpr = false
			}
			firstChunk = false
		}

		// 4. Calculate XXH3 and SHA-256 of original data
		if xxh3Hash == nil {
			xxh3Hash = xxh3.New()
			sha256Hash = sha256.New()
		}

		// Calculate HdrXXH3 for first chunk (first 100KB or full if smaller)
		if !hdrXXH3Calculated {
			hdrEnd := dataInfo.OrigSize + int64(len(chunkData))
			if hdrEnd <= core.DefaultHdrSize {
				// Still within header size, write to XXH3 (will be HdrXXH3)
				xxh3Hash.Write(chunkData)
			} else if dataInfo.OrigSize < core.DefaultHdrSize {
				// Cross header boundary, calculate HdrXXH3
				remainingHdrSize := core.DefaultHdrSize - dataInfo.OrigSize
				if remainingHdrSize > 0 && remainingHdrSize < int64(len(chunkData)) {
					// Write only the remaining part to reach DefaultHdrSize
					xxh3Hash.Write(chunkData[:remainingHdrSize])
				} else {
					xxh3Hash.Write(chunkData)
				}
				dataInfo.HdrXXH3 = int64(xxh3Hash.Sum64())
				hdrXXH3Calculated = true
				// Reset for full XXH3 calculation
				xxh3Hash = xxh3.New()
				xxh3Hash.Write(chunkData)
			} else {
				// Already beyond header size, but HdrXXH3 not calculated yet
				// Calculate from first part of first chunk
				hdrSize := int(core.DefaultHdrSize)
				if hdrSize > len(chunkData) {
					hdrSize = len(chunkData)
				}
				dataInfo.HdrXXH3 = int64(xxh3.Hash(chunkData[:hdrSize]))
				hdrXXH3Calculated = true
				xxh3Hash.Write(chunkData)
			}
		} else {
			xxh3Hash.Write(chunkData)
		}

		sha256Hash.Write(chunkData)
		dataXXH3 = xxh3Hash.Sum64()

		// Note: dataInfo.OrigSize should NOT be accumulated here
		// It's already set to the correct final file size (newSize) when dataInfo was created
		// Accumulating it here would double the size, causing bugs

		// 5. Compress (if enabled)
		var processedChunk []byte
		if hasCmpr && cmpr != nil {
			var cmprBuf bytes.Buffer
			err := cmpr.Compress(bytes.NewBuffer(chunkData), &cmprBuf)
			if err != nil {
				processedChunk = chunkData
				// Compression failed, only remove compression flag on first chunk
				if firstChunk {
					dataInfo.Kind &= ^core.DATA_CMPR_MASK
					hasCmpr = false
				}
			} else {
				// If compressed size is larger or equal, use original data and remove compression flag
				// Note: this logic only executes on first chunk, because once compression is decided, subsequent chunks should be consistent
				if firstChunk && cmprBuf.Len() >= len(chunkData) {
					processedChunk = chunkData
					dataInfo.Kind &= ^core.DATA_CMPR_MASK
					hasCmpr = false
				} else if !firstChunk && cmprBuf.Len() >= len(chunkData) {
					// For subsequent chunks, if compressed size is larger, still use compressed data (maintain consistency)
					// CRITICAL: bytes.Buffer.Bytes() returns a reference to the underlying buffer
					// We must create a copy to ensure data integrity
					compressedData := cmprBuf.Bytes()
					processedChunk = make([]byte, len(compressedData))
					copy(processedChunk, compressedData)
				} else {
					// CRITICAL: bytes.Buffer.Bytes() returns a reference to the underlying buffer
					// We must create a copy to ensure data integrity
					compressedData := cmprBuf.Bytes()
					processedChunk = make([]byte, len(compressedData))
					copy(processedChunk, compressedData)
				}
			}
		} else {
			processedChunk = chunkData
		}

		// 6. Encrypt (if enabled)
		endecKey := getEndecKeyForFS(ra.fs)
		var encodedChunk []byte
		var err error
		if endecKey != "" && dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
			encodedChunk, err = aes256.Encrypt(endecKey, processedChunk)
		} else if endecKey != "" && dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
			encodedChunk, err = sm4.Sm4Cbc([]byte(endecKey), processedChunk, true)
		} else {
			encodedChunk = processedChunk
		}
		if err != nil {
			encodedChunk = processedChunk
		}

		// 7. Update size (if compressed or encrypted)
		if dataInfo.Kind&core.DATA_CMPR_MASK != 0 || dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
			dataInfo.Size += int64(len(encodedChunk))
		}

		// 8. Immediately write to new object (stream write)
		encodedChunkCopy := make([]byte, len(encodedChunk))
		copy(encodedChunkCopy, encodedChunk)
		DebugLog("[VFS applyWritesStreamingCompressed] Writing encoded chunk to disk: fileID=%d, dataID=%d, chunkIdx=%d, sn=%d, pos=%d, size=%d, writesCount=%d", ra.fileID, dataInfo.ID, chunkIdx, sn, pos, len(encodedChunkCopy), len(writesByChunk[chunkIdx]))
		if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, encodedChunkCopy); err != nil {
			DebugLog("[VFS applyWritesStreamingCompressed] ERROR: Failed to write encoded chunk to disk: fileID=%d, dataID=%d, sn=%d, error=%v", ra.fileID, dataInfo.ID, sn, err)
			return 0, err
		}
		DebugLog("[VFS applyWritesStreamingCompressed] Successfully wrote encoded chunk to disk: fileID=%d, dataID=%d, sn=%d, size=%d", ra.fileID, dataInfo.ID, sn, len(encodedChunkCopy))
		sn++

		// chunkData no longer needed (no object pool; allow GC)
	}

	// Set XXH3, SHA-256 and checksum
	dataInfo.XXH3 = int64(dataXXH3)
	sha256Sum := sha256Hash.Sum(nil)
	dataInfo.SHA256_0 = int64(binary.BigEndian.Uint64(sha256Sum[0:8]))
	dataInfo.SHA256_1 = int64(binary.BigEndian.Uint64(sha256Sum[8:16]))
	dataInfo.SHA256_2 = int64(binary.BigEndian.Uint64(sha256Sum[16:24]))
	dataInfo.SHA256_3 = int64(binary.BigEndian.Uint64(sha256Sum[24:32]))
	if dataInfo.Kind&core.DATA_CMPR_MASK == 0 && dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		dataInfo.Size = dataInfo.OrigSize
	}

	// Save data metadata
	DebugLog("[VFS applyWritesStreamingCompressed] Writing DataInfo to disk: fileID=%d, dataID=%d, OrigSize=%d, Size=%d", ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size)
	_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		DebugLog("[VFS applyWritesStreamingCompressed] ERROR: Failed to write DataInfo to disk: fileID=%d, dataID=%d, error=%v", ra.fileID, dataInfo.ID, err)
		return 0, err
	}
	DebugLog("[VFS applyWritesStreamingCompressed] Successfully wrote DataInfo to disk: fileID=%d, dataID=%d, OrigSize=%d, Size=%d", ra.fileID, dataInfo.ID, dataInfo.OrigSize, dataInfo.Size)

	// Update cache
	dataInfoCache.Put(dataInfo.ID, dataInfo)

	// Update fileObj with new DataID and size
	// This ensures subsequent operations use the correct DataID
	updateFileObj, err := ra.getFileObj()
	if err == nil && updateFileObj != nil {
		oldDataID := updateFileObj.DataID
		oldSize := updateFileObj.Size
		updateFileObj.DataID = dataInfo.ID
		updateFileObj.Size = dataInfo.OrigSize
		updateFileObj.MTime = core.Now()
		DebugLog("[VFS applyWritesStreamingCompressed] Updating fileObj: fileID=%d, oldDataID=%d, newDataID=%d, oldSize=%d, newSize=%d", ra.fileID, oldDataID, dataInfo.ID, oldSize, dataInfo.OrigSize)
		_, err = ra.fs.h.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			DebugLog("[VFS applyWritesStreamingCompressed] ERROR: Failed to update fileObj: fileID=%d, dataID=%d, error=%v", ra.fileID, dataInfo.ID, err)
			return 0, err
		}
		// Update cache
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
		DebugLog("[VFS applyWritesStreamingCompressed] Successfully updated fileObj: fileID=%d, dataID=%d, size=%d", ra.fileID, dataInfo.ID, dataInfo.OrigSize)
	} else {
		DebugLog("[VFS applyWritesStreamingCompressed] WARNING: Failed to get fileObj for update: fileID=%d, error=%v", ra.fileID, err)
	}

	newVersionID := core.NewID()
	return newVersionID, nil
}

// dataReader data reader interface, unified handling of data reading in different formats
// Read reads data starting from offset into buf, returns number of bytes read and error
type dataReader interface {
	Read(buf []byte, offset int64) (int, error)
}

// readWithWrites unified handling of read logic: calculate read range, read data, apply write operations, extract result
func (ra *RandomAccessor) readWithWrites(reader dataReader, offset int64, size int, actualFileSize int64) ([]byte, bool) {
	// 1. Check write operations in buffer to determine required data range
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}
	mergedOps := mergeWriteOperations(operations)

	// 2. Calculate actual required data range (considering impact of write operations)
	readStart := offset
	readEnd := offset + int64(size)
	if len(mergedOps) > 0 {
		for _, op := range mergedOps {
			opEnd := op.Offset + int64(len(op.Data))
			if op.Offset < readEnd && opEnd > readStart {
				if op.Offset < readStart {
					readStart = op.Offset
				}
				if opEnd > readEnd {
					readEnd = opEnd
				}
			}
		}
	}

	// IMPORTANT: Limit readEnd to actualFileSize to prevent reading beyond truncated size
	// This is critical for truncated files where DataInfo.OrigSize may not match fileObj.Size
	// Use actualFileSize (from fileObj.Size) as the authoritative source for file size
	if actualFileSize > 0 {
		if readEnd > actualFileSize {
			readEnd = actualFileSize
		}
		if readStart > actualFileSize {
			readStart = actualFileSize
		}
	} else if chunkReader, ok := reader.(*chunkReader); ok {
		// Fallback to chunkReader.origSize if actualFileSize is not available
		if readEnd > chunkReader.origSize {
			readEnd = chunkReader.origSize
		}
		if readStart > chunkReader.origSize {
			readStart = chunkReader.origSize
		}
	}

	// 3. Only read required data range
	readSize := readEnd - readStart
	if readSize <= 0 {
		return []byte{}, true
	}

	// Read required data range directly from offset
	readData := make([]byte, readSize)
	n, err := reader.Read(readData, readStart)
	// If read fails or read data is less than requested size, only return read data
	if err != nil {
		// io.EOF means all available data has been read
		if err == io.EOF {
			if n > 0 {
				readData = readData[:n]
			} else {
				readData = []byte{}
			}
		} else {
			// Other errors, return failure
			DebugLog("[VFS readWithWrites] ERROR: reader.Read failed: fileID=%d, offset=%d, size=%d, readStart=%d, readSize=%d, error=%v",
				ra.fileID, offset, size, readStart, readSize, err)
			return nil, false
		}
	} else if n < len(readData) {
		// Read data is less than requested size (end of file), extract actually read data
		readData = readData[:n]
	} else if n > 0 {
		// CRITICAL: Limit readData size to actualFileSize to prevent reading beyond truncated size
		// This is essential for truncated files where DataInfo.OrigSize may not match fileObj.Size
		if actualFileSize > 0 && readStart+int64(n) > actualFileSize {
			oldN := n
			maxReadSize := actualFileSize - readStart
			if maxReadSize < 0 {
				maxReadSize = 0
			}
			n = int(maxReadSize)
			readData = readData[:n]
			DebugLog("[VFS readWithWrites] Truncated readData after reader.Read: fileID=%d, readStart=%d, oldN=%d, newN=%d, actualFileSize=%d",
				ra.fileID, readStart, oldN, n, actualFileSize)
		}
	} else if n == 0 && readSize > 0 {
		// IMPORTANT: If reader.Read returns 0 bytes without error when readSize > 0, this is suspicious
		// This could indicate that the data hasn't been flushed yet or there's a bug
		// However, we should still return the data (which will be all zeros) to avoid breaking legitimate cases
		// But log a warning to help debug the issue
		DebugLog("[VFS readWithWrites] WARNING: reader.Read returned 0 bytes without error: fileID=%d, offset=%d, size=%d, readStart=%d, readSize=%d",
			ra.fileID, offset, size, readStart, readSize)
		// Continue with empty data - this might be legitimate (file is empty or data not yet flushed)
		readData = []byte{}
	}

	// IMPORTANT: If we read data but got 0 bytes when we expected data, log a warning
	// This helps catch bugs where data wasn't properly read but don't fail the read operation
	// as the file might legitimately contain zeros or data might not be flushed yet
	if n == 0 && readSize > 0 && len(readData) == 0 {
		DebugLog("[VFS readWithWrites] INFO: No data read (may be empty file or data not flushed): fileID=%d, offset=%d, size=%d, readStart=%d, readSize=%d",
			ra.fileID, offset, size, readStart, readSize)
	}

	// 4. Apply write operations to read data
	if len(mergedOps) > 0 {
		adjustedOps := make([]WriteOperation, 0, len(mergedOps))
		for _, op := range mergedOps {
			opEnd := op.Offset + int64(len(op.Data))
			if op.Offset < readEnd && opEnd > readStart {
				adjustedOp := WriteOperation{
					Offset: op.Offset - readStart,
					Data:   op.Data,
				}
				adjustedOps = append(adjustedOps, adjustedOp)
			}
		}
		if len(adjustedOps) > 0 {
			readData = applyWritesToData(readData, adjustedOps)
			// CRITICAL: Limit readData size to actualFileSize after applying writes
			// This prevents reading beyond truncated size when writes extend the data
			if actualFileSize > 0 && int64(len(readData)) > actualFileSize-readStart {
				oldLen := len(readData)
				readData = readData[:actualFileSize-readStart]
				DebugLog("[VFS readWithWrites] Truncated readData after applyWritesToData: fileID=%d, readStart=%d, oldLen=%d, newLen=%d, actualFileSize=%d",
					ra.fileID, readStart, oldLen, len(readData), actualFileSize)
			}
		}
	}

	// 5. Extract requested range (offset to offset+size, relative to readStart)
	resultOffset := offset - readStart
	resultEnd := resultOffset + int64(size)
	// CRITICAL: Limit resultEnd to actualFileSize to prevent reading beyond truncated size
	if actualFileSize > 0 && offset+int64(size) > actualFileSize {
		resultEnd = resultOffset + (actualFileSize - offset)
		if resultEnd < resultOffset {
			resultEnd = resultOffset
		}
	}
	if resultEnd > int64(len(readData)) {
		resultEnd = int64(len(readData))
	}
	if resultOffset < 0 {
		resultOffset = 0
	}
	if resultOffset >= resultEnd {
		return []byte{}, true
	}

	result := readData[resultOffset:resultEnd]

	// CRITICAL: Limit result size to actualFileSize to prevent reading beyond truncated size
	// This is essential for truncated files where DataInfo.OrigSize may not match fileObj.Size
	if actualFileSize > 0 && int64(len(result)) > actualFileSize-offset {
		oldLen := len(result)
		result = result[:actualFileSize-offset]
		DebugLog("[VFS readWithWrites] Truncated result to actualFileSize: fileID=%d, offset=%d, oldLen=%d, newLen=%d, actualFileSize=%d",
			ra.fileID, offset, oldLen, len(result), actualFileSize)
	}

	DebugLog("[VFS readWithWrites] Returning result: fileID=%d, offset=%d, size=%d, resultLen=%d, readDataLen=%d, resultOffset=%d, resultEnd=%d, actualFileSize=%d",
		ra.fileID, offset, size, len(result), len(readData), resultOffset, resultEnd, actualFileSize)
	return result, true
}

// chunkReader unified reader for both plain and compressed/encrypted data
// Uses cache and singleflight for consistent performance
// Supports asynchronous prefetching when reading reaches 80% of current chunk
type chunkReader struct {
	c                 core.Ctx
	h                 core.Handler
	bktID             int64
	dataID            int64
	kind              uint32  // Compression/encryption kind (0 for plain)
	endecKey          string  // Encryption key (empty for plain)
	origSize          int64   // Original data size (decompressed size)
	compressedSize    int64   // Compressed/encrypted data size (for packaged files)
	chunkSize         int64   // Chunk size for original data
	prefetchThreshold float64 // Threshold percentage to trigger prefetch (default: 0.8 = 80%)
	pkgOffset         uint32  // Package offset (for packaged files, 0 for non-packaged files)
}

// newChunkReader creates a unified chunk reader for both plain and compressed/encrypted data
func newChunkReader(c core.Ctx, h core.Handler, bktID int64, dataInfo *core.DataInfo, endecKey string, chunkSize int64) *chunkReader {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize // 10MB default
	}

	cr := &chunkReader{
		c:                 c,
		h:                 h,
		bktID:             bktID,
		chunkSize:         chunkSize,
		prefetchThreshold: 0.8, // 80% threshold
	}

	if dataInfo != nil {
		// Set dataInfo fields
		cr.kind = dataInfo.Kind
		cr.endecKey = endecKey
		cr.origSize = dataInfo.OrigSize
		cr.compressedSize = dataInfo.Size // Compressed/encrypted size
		if dataInfo.PkgID > 0 {
			cr.dataID = dataInfo.PkgID
			cr.pkgOffset = dataInfo.PkgOffset
		} else {
			cr.dataID = dataInfo.ID
			cr.pkgOffset = 0
		}
	}

	return cr
}

// Read reads data starting from offset into buf (uses ReadAt internally)
func (cr *chunkReader) Read(buf []byte, offset int64) (int, error) {
	return cr.ReadAt(buf, offset)
}

// ReadAt implements random-access reads by directly addressing chunks
func (cr *chunkReader) ReadAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("invalid offset: %d", offset)
	}
	if offset >= cr.origSize {
		return 0, io.EOF
	}
	if len(buf) == 0 {
		return 0, nil
	}

	// Limit read size to available data
	readSize := int64(len(buf))
	if offset+readSize > cr.origSize {
		readSize = cr.origSize - offset
	}
	if readSize <= 0 {
		return 0, io.EOF
	}

	// ChunkSize must be set
	if cr.chunkSize <= 0 {
		return 0, fmt.Errorf("chunkSize not set")
	}

	// For packaged files, read entire package once and extract file data
	var fileData []byte
	if cr.pkgOffset > 0 {
		// This is a packaged file, read entire package (sn=0)
		// Check cache first for decoded file data
		if cached, ok := globalDecodedFileCache.Get(cr.dataID); ok {
			if decoded, ok := cached.([]byte); ok && len(decoded) > 0 {
				// Truncate cached data to origSize (important for truncated files)
				// Cached data may be from before truncate operation
				if int64(len(decoded)) > cr.origSize {
					decoded = decoded[:cr.origSize]
					// Update cache with truncated data
					globalDecodedFileCache.Put(cr.dataID, decoded)
					DebugLog("[VFS chunkReader ReadAt] Truncated cached decoded file data to origSize: dataID=%d, cachedLen=%d, origSize=%d", cr.dataID, len(decoded), cr.origSize)
				}
				fileData = decoded
				// DebugLog("[VFS chunkReader ReadAt] Using cached decoded file data: dataID=%d, decodedLen=%d", cr.dataID, len(fileData))
			}
		}

		if fileData == nil {
			// Use singleflight to ensure only one goroutine decodes this package file at a time
			// Key format: "decode_pkg_<dataID>" to uniquely identify each package file
			sfKey := fmt.Sprintf("decode_pkg_%d", cr.dataID)

			result, err, _ := globalSingleFlight.Do(sfKey, func() (interface{}, error) {
				// Double-check cache after acquiring singleflight lock
				// Another goroutine might have already decoded it
				if cached, ok := globalDecodedFileCache.Get(cr.dataID); ok {
					if decoded, ok := cached.([]byte); ok && len(decoded) > 0 {
						return decoded, nil
					}
				}

				// DebugLog("[VFS chunkReader ReadAt] Reading packaged file: dataID=%d, pkgOffset=%d, origSize=%d, offset=%d, bufSize=%d",
				//	cr.dataID, cr.pkgOffset, cr.origSize, offset, len(buf))
				pkgData, err := cr.getChunk(0)
				if err != nil {
					// DebugLog("[VFS chunkReader ReadAt] ERROR: Failed to get package chunk: dataID=%d, error=%v", cr.dataID, err)
					return nil, err
				}
				// DebugLog("[VFS chunkReader ReadAt] Got package data: dataID=%d, pkgDataLen=%d", cr.dataID, len(pkgData))
				// Extract file data from package
				// PkgOffset is the offset of compressed/encrypted file data in the package
				// Use compressedSize (not origSize) to calculate the range
				pkgStart := int64(cr.pkgOffset)
				pkgEnd := pkgStart + cr.compressedSize
				if int64(len(pkgData)) < pkgEnd {
					// DebugLog("[VFS chunkReader ReadAt] ERROR: Package data incomplete: dataID=%d, pkgOffset=%d, compressedSize=%d, expected %d bytes, got %d",
					//	cr.dataID, cr.pkgOffset, cr.compressedSize, pkgEnd, len(pkgData))
					return nil, fmt.Errorf("package data incomplete: expected %d bytes, got %d", pkgEnd, len(pkgData))
				}
				encryptedFileData := pkgData[pkgStart:pkgEnd]
				// DebugLog("[VFS chunkReader ReadAt] Extracted encrypted file data: dataID=%d, encryptedFileDataLen=%d", cr.dataID, len(encryptedFileData))

				// Decrypt and decompress the file data using util.UnprocessData
				decodedFileData, err := core.UnprocessData(encryptedFileData, cr.kind, cr.endecKey)
				if err != nil {
					return nil, fmt.Errorf("failed to unprocess file data: %v", err)
				}

				// Truncate decoded file data to origSize (important for truncated files)
				// After truncate operation, origSize may be smaller than decoded data
				if int64(len(decodedFileData)) > cr.origSize {
					decodedFileData = decodedFileData[:cr.origSize]
					DebugLog("[VFS chunkReader ReadAt] Truncated decoded file data to origSize: dataID=%d, decodedLen=%d, origSize=%d", cr.dataID, len(decodedFileData), cr.origSize)
				}

				// Cache decoded file data for future reads
				globalDecodedFileCache.Put(cr.dataID, decodedFileData)
				// DebugLog("[VFS chunkReader ReadAt] Decoded file data: dataID=%d, decodedFileDataLen=%d, origSize=%d", cr.dataID, len(decodedFileData), cr.origSize)
				return decodedFileData, nil
			})

			if err != nil {
				return 0, err
			}

			if decoded, ok := result.([]byte); ok {
				fileData = decoded
			} else {
				return 0, fmt.Errorf("invalid decoded file data type")
			}
		}

		// Now read from decoded fileData
		// Use origSize to limit fileData size (important for truncated files)
		// fileData may be longer than origSize if it was cached before truncate
		effectiveFileSize := int64(len(fileData))
		if effectiveFileSize > cr.origSize {
			effectiveFileSize = cr.origSize
		}
		if offset >= effectiveFileSize {
			// DebugLog("[VFS chunkReader ReadAt] Offset beyond file size: dataID=%d, offset=%d, effectiveFileSize=%d, fileDataLen=%d", cr.dataID, offset, effectiveFileSize, len(fileData))
			return 0, io.EOF
		}
		readSize := int64(len(buf))
		if offset+readSize > effectiveFileSize {
			readSize = effectiveFileSize - offset
		}
		// DebugLog("[VFS chunkReader ReadAt] Reading from decoded file data: dataID=%d, offset=%d, readSize=%d, effectiveFileSize=%d", cr.dataID, offset, readSize, effectiveFileSize)
		copy(buf[:readSize], fileData[offset:offset+readSize])
		return int(readSize), nil
	}

	// For non-packaged files, use normal chunk-based reading
	// Note: ReadAt should be safe for concurrent use, but we add mutex to prevent
	// issues with concurrent reads that might cause confusion in logs
	// However, since ReadAt is designed to be safe for concurrent use (each call has its own offset),
	// we don't lock the entire method. Instead, we rely on singleflight for chunk reads.
	// The mutex is only used for package file decoding to prevent race conditions.
	totalRead := 0
	currentOffset := offset
	remaining := readSize
	// Track the last chunk we read from to handle non-aligned chunk boundaries
	// This is critical for compressed files where actual chunk size may be smaller than chunkSize
	var lastChunkSn int = -1
	var lastChunkStart int64 = -1
	var lastChunkEnd int64 = -1

	for remaining > 0 && currentOffset < cr.origSize {
		// First, check if currentOffset is within the last chunk we read from
		// This handles the case where actualChunkEnd is not aligned to chunkSize boundary
		if lastChunkSn >= 0 && currentOffset >= lastChunkStart && currentOffset < lastChunkEnd {
			// We're still in the last chunk, use it directly
			currentSn := lastChunkSn
			chunkStart := lastChunkStart
			currentOffsetInChunk := currentOffset - chunkStart

			// Get the chunk data (should be cached)
			chunkData, err := cr.getChunk(currentSn)
			if err != nil {
				// Chunk not found, fall through to normal calculation
				lastChunkSn = -1
				lastChunkStart = -1
				lastChunkEnd = -1
				// Fall through to normal calculation below
			} else {
				// We have the chunk, check if we're still within its bounds
				// IMPORTANT: Use actual data size to check bounds, not chunkSize
				actualChunkDataSize := int64(len(chunkData))
				actualChunkEndForData := chunkStart + actualChunkDataSize
				// Recalculate currentOffsetInChunk to ensure it's within bounds
				if currentOffset < actualChunkEndForData {
					currentOffsetInChunk = currentOffset - chunkStart
					// We're within the chunk, proceed with reading
					availableInChunk := actualChunkDataSize - currentOffsetInChunk
					if availableInChunk > 0 {
						toRead := remaining
						if toRead > availableInChunk {
							toRead = availableInChunk
						}

						// Copy data from chunk
						copy(buf[totalRead:totalRead+int(toRead)], chunkData[currentOffsetInChunk:currentOffsetInChunk+toRead])
						DebugLog("[VFS chunkReader ReadAt] Read from chunk (using last chunk): dataID=%d, sn=%d, chunkSize=%d, toRead=%d, totalRead=%d, remaining=%d",
							cr.dataID, currentSn, len(chunkData), toRead, totalRead+int(toRead), remaining-toRead)
						totalRead += int(toRead)
						currentOffset += toRead
						remaining -= toRead
						// Update last chunk info: use chunkSize for non-last chunks, actual data size for last chunk
						nextChunkStartForLastChunkEnd := chunkStart + cr.chunkSize
						if nextChunkStartForLastChunkEnd >= cr.origSize {
							// This is the last chunk, use actual data size
							lastChunkEnd = actualChunkEndForData
						} else {
							// This is not the last chunk, use chunkSize
							lastChunkEnd = nextChunkStartForLastChunkEnd
						}
						continue
					}
				}
				// We've moved beyond this chunk, reset tracking
				lastChunkSn = -1
				lastChunkStart = -1
				lastChunkEnd = -1
			}
		}

		// Normal calculation: use chunkSize to determine which chunk we're in
		currentSn := int(currentOffset / cr.chunkSize)
		chunkStart := int64(currentSn) * cr.chunkSize
		currentOffsetInChunk := currentOffset - chunkStart

		// Only log once per unique read request to avoid spam from concurrent reads
		// Use a simple check: only log if this is the first iteration (totalRead == 0 and currentOffset == offset)
		if totalRead == 0 && currentOffset == offset {
			DebugLog("[VFS chunkReader ReadAt] Reading chunk: dataID=%d, sn=%d, offset=%d, offsetInChunk=%d, remaining=%d, origSize=%d, totalRead=%d",
				cr.dataID, currentSn, currentOffset, currentOffsetInChunk, remaining, cr.origSize, totalRead)
		}

		// Get or load chunk (with cache and singleflight)
		chunkData, err := cr.getChunk(currentSn)
		if err != nil {
			DebugLog("[VFS chunkReader ReadAt] Error getting chunk: dataID=%d, sn=%d, error=%v, totalRead=%d, currentOffset=%d, origSize=%d",
				cr.dataID, currentSn, err, totalRead, currentOffset, cr.origSize)
			// If chunk doesn't exist, check if we've reached the end of file
			// For the last chunk, it might be smaller than chunkSize
			// Check if we've read enough data based on origSize
			if currentOffset >= cr.origSize {
				// We've reached the end of file, return what we've read
				if totalRead > 0 {
					return totalRead, nil
				}
				return 0, io.EOF
			}
			// If chunk doesn't exist but we haven't reached origSize, it's an error
			// But if we've read some data, return what we have (partial read)
			if totalRead > 0 {
				// Check if error is "not found" or EOF - might be end of file
				if err == io.EOF || strings.Contains(err.Error(), "not found") {
					// If we're close to origSize, assume we've read all available data
					if currentOffset >= cr.origSize-int64(len(chunkData)) {
						return totalRead, nil
					}
				}
				// For other errors, return what we've read so far
				return totalRead, nil
			}
			// If this is the first chunk and it doesn't exist, return error
			return 0, err
		}

		// Calculate how much to read from this chunk
		// If currentOffsetInChunk is beyond the actual chunk data, we need to move to next chunk
		// IMPORTANT: Always use len(chunkData) to determine if we've reached the end of this chunk's data
		// The actual chunk end in file coordinates: for non-last chunks, use chunkSize; for last chunk, use actual data size
		actualChunkDataSize := int64(len(chunkData))
		// Calculate actual chunk end in file coordinates: for non-last chunks, use chunkSize; for last chunk, use actual data size
		// This is used to determine where the next chunk starts in the file coordinate system
		nextChunkStart := chunkStart + cr.chunkSize
		var actualChunkEnd int64
		if nextChunkStart >= cr.origSize {
			// This is the last chunk, use actual data size
			actualChunkEnd = chunkStart + actualChunkDataSize
		} else {
			// This is not the last chunk, use chunkSize for file coordinate system
			// Even if len(chunkData) < chunkSize (compressed), the next chunk still starts at chunkStart + chunkSize
			actualChunkEnd = nextChunkStart
		}

		// Check if we've read all data from this chunk (use actual data size, not chunkSize)
		if currentOffsetInChunk >= actualChunkDataSize {
			// Current offset is beyond this chunk's data, move to next chunk
			// CRITICAL: Check if we've reached the end of file FIRST before trying next chunk
			// This prevents attempting to read non-existent chunks (e.g., sn=1 when file only has sn=0)
			if actualChunkEnd >= cr.origSize || currentOffset >= cr.origSize {
				// End of file - we've read all available data
				DebugLog("[VFS chunkReader ReadAt] Reached end of file: dataID=%d, actualChunkEnd=%d, origSize=%d, currentOffset=%d", cr.dataID, actualChunkEnd, cr.origSize, currentOffset)
				break
			}
			// Move to next chunk
			// If currentOffset is already at or beyond actualChunkEnd, we're already past this chunk
			// This can happen when offset is beyond the chunk size (e.g., offset=49152 but chunk only has 32768 bytes)
			// IMPORTANT: For compressed files, chunk actual size may be smaller than chunkSize
			// So the next chunk starts at actualChunkEnd, not at nextChunkStart (which is based on chunkSize)
			// For uncompressed files, actualChunkEnd should equal chunkStart + chunkSize (except last chunk)
			if currentOffset >= actualChunkEnd {
				// We're already past this chunk, try to read next chunk
				// CRITICAL: Double-check if we've reached the end of file before trying next chunk
				// This is important for files where chunk data size is smaller than expected
				if actualChunkEnd >= cr.origSize || currentOffset >= cr.origSize {
					DebugLog("[VFS chunkReader ReadAt] Reached end of file before next chunk: dataID=%d, actualChunkEnd=%d, origSize=%d, currentOffset=%d", cr.dataID, actualChunkEnd, cr.origSize, currentOffset)
					break
				}
				// CRITICAL: Before attempting to read next chunk, verify it's within file bounds
				// This prevents decryption errors when trying to read non-existent chunks
				// Calculate next chunk start to verify it's within file bounds
				nextSn := currentSn + 1
				nextChunkStart := int64(nextSn) * cr.chunkSize
				if nextChunkStart >= cr.origSize {
					// Next chunk would be beyond file size, we've reached the end
					DebugLog("[VFS chunkReader ReadAt] Next chunk beyond file size, reached end: dataID=%d, nextChunkStart=%d, origSize=%d, currentOffset=%d", cr.dataID, nextChunkStart, cr.origSize, currentOffset)
					break
				}
				// Try to read next chunk (sn+1) to see if it exists

				nextChunkData, nextErr := cr.getChunk(nextSn)
				if nextErr != nil {
					// Next chunk doesn't exist or failed to read, check if we've reached the end of file
					if actualChunkEnd >= cr.origSize || currentOffset >= cr.origSize {
						DebugLog("[VFS chunkReader ReadAt] Next chunk doesn't exist, reached end: dataID=%d, actualChunkEnd=%d, origSize=%d, currentOffset=%d", cr.dataID, actualChunkEnd, cr.origSize, currentOffset)
						break
					}
					// If we've read some data, return it
					if totalRead > 0 {
						DebugLog("[VFS chunkReader ReadAt] Next chunk doesn't exist, returning partial read: dataID=%d, totalRead=%d, actualChunkEnd=%d, origSize=%d", cr.dataID, totalRead, actualChunkEnd, cr.origSize)
						return totalRead, nil
					}
					// No data read yet, but check if error is decryption failure for non-existent chunk
					// If it's a decryption error and we're at or beyond origSize, treat as EOF
					if strings.Contains(nextErr.Error(), "decryption") || strings.Contains(nextErr.Error(), "authentication") {
						if currentOffset >= cr.origSize || actualChunkEnd >= cr.origSize {
							DebugLog("[VFS chunkReader ReadAt] Decryption error for non-existent chunk, reached end: dataID=%d, currentOffset=%d, origSize=%d, error=%v", cr.dataID, currentOffset, cr.origSize, nextErr)
							break
						}
					}
					// No data read yet, return error
					return 0, nextErr
				}
				// Next chunk exists, read from it directly to avoid infinite loop
				// The next chunk starts at actualChunkEnd in file coordinates
				// But within the chunk, we read from offset 0
				nextChunkOffsetInChunk := int64(0)
				nextAvailableInChunk := int64(len(nextChunkData)) - nextChunkOffsetInChunk
				if nextAvailableInChunk <= 0 {
					// Next chunk is empty, move to next chunk
					nextChunkStart := int64(nextSn) * cr.chunkSize
					nextActualChunkEnd := nextChunkStart + int64(len(nextChunkData))
					// Check if we've reached the end of file
					if nextActualChunkEnd >= cr.origSize {
						DebugLog("[VFS chunkReader ReadAt] Reached end of file after empty chunk: dataID=%d, actualChunkEnd=%d, origSize=%d", cr.dataID, nextActualChunkEnd, cr.origSize)
						break
					}
					// Move to next chunk
					currentOffset = nextActualChunkEnd
					currentSn = nextSn + 1
					// Reset last chunk tracking since we're moving to a new chunk
					lastChunkSn = -1
					lastChunkStart = -1
					lastChunkEnd = -1
					continue
				}
				// Read from next chunk
				toReadFromNext := remaining
				if toReadFromNext > nextAvailableInChunk {
					toReadFromNext = nextAvailableInChunk
				}
				copy(buf[totalRead:totalRead+int(toReadFromNext)], nextChunkData[nextChunkOffsetInChunk:nextChunkOffsetInChunk+toReadFromNext])
				DebugLog("[VFS chunkReader ReadAt] Read from next chunk: dataID=%d, currentSn=%d, nextSn=%d, toRead=%d, totalRead=%d",
					cr.dataID, currentSn, nextSn, toReadFromNext, totalRead+int(toReadFromNext))
				totalRead += int(toReadFromNext)
				// Update currentOffset: next chunk starts at actualChunkEnd in file coordinates
				// After reading toReadFromNext bytes from nextSn chunk, the new file offset is actualChunkEnd + toReadFromNext
				currentOffset = actualChunkEnd + toReadFromNext
				remaining -= toReadFromNext
				// Update last chunk tracking: we're now in nextSn chunk
				// This is critical for handling non-aligned chunk boundaries
				lastChunkSn = nextSn
				lastChunkStart = actualChunkEnd
				// Calculate lastChunkEnd: for non-last chunks, use chunkSize; for last chunk, use actual data size
				nextNextChunkStart := actualChunkEnd + cr.chunkSize
				if nextNextChunkStart >= cr.origSize {
					// nextSn is the last chunk, use actual data size
					lastChunkEnd = actualChunkEnd + int64(len(nextChunkData))
				} else {
					// nextSn is not the last chunk, use chunkSize
					lastChunkEnd = nextNextChunkStart
				}
				// Continue loop to read more if needed
				continue
			}
			// Normal case: move to end of current chunk
			currentOffset = actualChunkEnd
			// Reset last chunk tracking since we're moving to chunk boundary
			lastChunkSn = -1
			lastChunkStart = -1
			lastChunkEnd = -1
			continue
		}

		availableInChunk := int64(len(chunkData)) - currentOffsetInChunk
		if availableInChunk <= 0 {
			// This chunk has no more data, move to next chunk
			// Calculate the actual end of current chunk
			// IMPORTANT: For uncompressed files, each chunk (except last) should have chunkSize bytes
			// The actual chunk end should be chunkStart + chunkSize for non-last chunks
			// For the last chunk, it should be chunkStart + len(chunkData) (which may be < chunkSize)
			nextChunkStart := chunkStart + cr.chunkSize
			var actualChunkEnd int64
			if nextChunkStart >= cr.origSize {
				// This is the last chunk, use actual data size
				actualChunkEnd = chunkStart + int64(len(chunkData))
			} else {
				// This is not the last chunk, use chunkSize
				actualChunkEnd = nextChunkStart
			}
			// Check if we've reached the end of file
			if actualChunkEnd >= cr.origSize {
				// End of file
				DebugLog("[VFS chunkReader ReadAt] Reached end of file: dataID=%d, actualChunkEnd=%d, origSize=%d", cr.dataID, actualChunkEnd, cr.origSize)
				break
			}
			// Move to next chunk: directly read next chunk (sn+1) at actualChunkEnd offset
			// The key insight: chunk sn is based on configured chunkSize, but actual chunk data
			// may be smaller. When actualChunkEnd < nextChunkStart, we should still read sn+1,
			// but at offset actualChunkEnd (not nextChunkStart)
			// Update currentOffset to actualChunkEnd before reading next chunk
			currentOffset = actualChunkEnd
			nextSn := currentSn + 1
			// Try to read next chunk directly at the actual chunk end offset
			// This handles the case where actual chunk size is smaller than configured chunkSize
			nextChunkData, nextErr := cr.getChunk(nextSn)
			if nextErr != nil {
				// Next chunk doesn't exist, check if we've read enough
				if actualChunkEnd >= cr.origSize {
					DebugLog("[VFS chunkReader ReadAt] Next chunk doesn't exist, reached end: dataID=%d, actualChunkEnd=%d, origSize=%d",
						cr.dataID, actualChunkEnd, cr.origSize)
					break
				}
				// If we've read some data, return it
				if totalRead > 0 {
					DebugLog("[VFS chunkReader ReadAt] Next chunk doesn't exist, returning partial read: dataID=%d, totalRead=%d, actualChunkEnd=%d, origSize=%d",
						cr.dataID, totalRead, actualChunkEnd, cr.origSize)
					return totalRead, nil
				}
				// No data read yet, return error
				return 0, nextErr
			}
			// Next chunk exists, read from it starting at offset 0 within the chunk
			// The next chunk's data starts at actualChunkEnd in the file
			// But within the chunk, we read from offset 0
			nextChunkOffsetInChunk := int64(0)
			nextAvailableInChunk := int64(len(nextChunkData)) - nextChunkOffsetInChunk
			if nextAvailableInChunk <= 0 {
				// Next chunk is empty, move to next chunk
				// Update actualChunkEnd to the end of this empty chunk
				nextChunkStart := int64(nextSn) * cr.chunkSize
				actualChunkEnd = nextChunkStart + int64(len(nextChunkData))
				// Check if we've reached the end of file
				if actualChunkEnd >= cr.origSize {
					DebugLog("[VFS chunkReader ReadAt] Reached end of file after empty chunk: dataID=%d, actualChunkEnd=%d, origSize=%d", cr.dataID, actualChunkEnd, cr.origSize)
					break
				}
				// Move to next chunk
				currentOffset = actualChunkEnd
				// Continue to try next chunk
				continue
			}
			// Read from next chunk
			// IMPORTANT: The next chunk starts at actualChunkEnd in the file coordinate system
			// But we read from offset 0 within the next chunk
			toReadFromNext := remaining
			if toReadFromNext > nextAvailableInChunk {
				toReadFromNext = nextAvailableInChunk
			}
			copy(buf[totalRead:totalRead+int(toReadFromNext)], nextChunkData[nextChunkOffsetInChunk:nextChunkOffsetInChunk+toReadFromNext])
			DebugLog("[VFS chunkReader ReadAt] Read from next chunk: dataID=%d, currentSn=%d, nextSn=%d, toRead=%d, totalRead=%d",
				cr.dataID, currentSn, nextSn, toReadFromNext, totalRead+int(toReadFromNext))
			totalRead += int(toReadFromNext)
			// Update currentOffset correctly: next chunk starts at actualChunkEnd in file coordinates
			// We read toReadFromNext bytes from the next chunk, so new offset is actualChunkEnd + toReadFromNext
			// IMPORTANT: actualChunkEnd is the file offset where the next chunk (nextSn) starts
			// After reading toReadFromNext bytes from nextSn chunk, the new file offset is actualChunkEnd + toReadFromNext
			currentOffset = actualChunkEnd + toReadFromNext
			remaining -= toReadFromNext
			// Update last chunk tracking: we're now in nextSn chunk
			// This is critical for handling non-aligned chunk boundaries
			lastChunkSn = nextSn
			lastChunkStart = actualChunkEnd
			// Calculate lastChunkEnd: for non-last chunks, use chunkSize; for last chunk, use actual data size
			nextNextChunkStart := actualChunkEnd + cr.chunkSize
			if nextNextChunkStart >= cr.origSize {
				// nextSn is the last chunk, use actual data size
				lastChunkEnd = actualChunkEnd + int64(len(nextChunkData))
			} else {
				// nextSn is not the last chunk, use chunkSize
				lastChunkEnd = nextNextChunkStart
			}
			// Continue loop to read more if needed
			continue
		}

		toRead := remaining
		if toRead > availableInChunk {
			toRead = availableInChunk
		}

		// Copy data from chunk
		copy(buf[totalRead:totalRead+int(toRead)], chunkData[currentOffsetInChunk:currentOffsetInChunk+toRead])

		DebugLog("[VFS chunkReader ReadAt] Read from chunk: dataID=%d, sn=%d, chunkSize=%d, toRead=%d, totalRead=%d, remaining=%d",
			cr.dataID, currentSn, len(chunkData), toRead, totalRead+int(toRead), remaining-toRead)

		totalRead += int(toRead)
		currentOffset += toRead
		remaining -= toRead
		// Update last chunk tracking: we're still in currentSn chunk
		// This helps handle non-aligned chunk boundaries in next iteration
		lastChunkSn = currentSn
		lastChunkStart = chunkStart
		// Calculate lastChunkEnd: for non-last chunks, use chunkSize; for last chunk, use actual data size
		nextChunkStartForLastChunkEnd := chunkStart + cr.chunkSize
		if nextChunkStartForLastChunkEnd >= cr.origSize {
			// currentSn is the last chunk, use actual data size
			lastChunkEnd = chunkStart + int64(len(chunkData))
		} else {
			// currentSn is not the last chunk, use chunkSize
			lastChunkEnd = nextChunkStartForLastChunkEnd
		}

		// Trigger prefetch if we've read past the threshold percentage of current chunk
		if len(chunkData) > 0 {
			readPercentage := float64(currentOffsetInChunk+toRead) / float64(len(chunkData))
			if readPercentage >= cr.prefetchThreshold {
				// Asynchronously prefetch next chunk
				nextSn := currentSn + 1
				// CRITICAL: Check if next chunk ACTUALLY EXISTS based on file size and chunk size
				// The old check (currentOffset+toRead < cr.origSize) only verified there's more data,
				// but didn't verify the data is in a NEW chunk. For files that fit in one chunk,
				// this would incorrectly try to prefetch sn=1 which doesn't exist.
				nextChunkStart := int64(nextSn) * cr.chunkSize
				if nextChunkStart < cr.origSize {
					// Capture variables for goroutine to avoid closure issues
					capturedNextSn := nextSn
					go func() {
						// Check if not already cached before prefetching
						// Check global cache
						cacheKey := [2]int64{cr.dataID, int64(capturedNextSn)}
						if _, ok := globalChunkCache.Get(cacheKey); !ok {
							// Prefetch next chunk (ignore errors)
							_, _ = cr.getChunk(capturedNextSn)
						}
					}()
				}
			}
		}
	}

	if totalRead == 0 && currentOffset >= cr.origSize {
		DebugLog("[VFS chunkReader ReadAt] No data read, reached EOF: dataID=%d, currentOffset=%d, origSize=%d", cr.dataID, currentOffset, cr.origSize)
		return 0, io.EOF
	}

	DebugLog("[VFS chunkReader ReadAt] Completed read: dataID=%d, totalRead=%d, requested=%d, origSize=%d", cr.dataID, totalRead, readSize, cr.origSize)
	return totalRead, nil
}

// getChunk gets a chunk (plain or decompressed/decrypted), using cache and singleflight
func (cr *chunkReader) getChunk(sn int) ([]byte, error) {
	// CRITICAL: Validate chunk number is within valid range based on file size
	// This prevents reading garbage/stale data from old file versions when file was larger
	// For example, if a file shrinks from 20MB (2 chunks) to 10MB (1 chunk), the old sn=1
	// data may still exist in storage but should not be read
	if cr.origSize > 0 && cr.chunkSize > 0 {
		maxChunks := int((cr.origSize + cr.chunkSize - 1) / cr.chunkSize)
		if sn >= maxChunks {
			DebugLog("[VFS chunkReader getChunk] Chunk sn=%d is out of range: dataID=%d, maxChunks=%d, origSize=%d, chunkSize=%d",
				sn, cr.dataID, maxChunks, cr.origSize, cr.chunkSize)
			return nil, fmt.Errorf("chunk sn=%d is out of range (maxChunks=%d, origSize=%d, chunkSize=%d)",
				sn, maxChunks, cr.origSize, cr.chunkSize)
		}
	}

	// Check cache first (fast path)
	cacheKey := [2]int64{cr.dataID, int64(sn)}
	if cached, ok := globalChunkCache.Get(cacheKey); ok {
		if chunkData, ok := cached.([]byte); ok && len(chunkData) > 0 {
			return chunkData, nil
		}
	}

	// Use global singleflight to ensure only one goroutine reads this chunk
	// Key format: "chunk_<dataID>_<sn>" to uniquely identify each chunk globally
	sfKey := fmt.Sprintf("chunk_%d_%d", cr.dataID, sn)

	result, err, _ := globalSingleFlight.Do(sfKey, func() (interface{}, error) {
		// Double-check cache after acquiring singleflight lock
		// Another goroutine might have already loaded it
		if cached, ok := globalChunkCache.Get(cacheKey); ok {
			if chunkData, ok := cached.([]byte); ok && len(chunkData) > 0 {
				return chunkData, nil
			}
		}

		// Read chunk (compressed/encrypted or plain)
		rawChunk, err := cr.h.GetData(cr.c, cr.bktID, cr.dataID, sn)
		if err != nil {
			return nil, err
		}

		// For packaged files (pkgOffset > 0), package itself is not encrypted/compressed
		// Each file in the package is individually encrypted/compressed
		// So we should return raw package data without processing
		if cr.pkgOffset > 0 {
			// This is a packaged file, return raw package data
			// Decryption/decompression will be done on individual file data later
			return rawChunk, nil
		}

		// For non-packaged files, process chunk based on kind (plain, encrypted, compressed, or both)
		// Use util.UnprocessData for unified decryption + decompression
		finalChunk, err := core.UnprocessData(rawChunk, cr.kind, cr.endecKey)
		if err != nil {
			DebugLog("[VFS chunkReader getChunk] ERROR: Failed to unprocess chunk: dataID=%d, sn=%d, error=%v", cr.dataID, sn, err)
			// If decryption/decompression fails, return error (don't return garbage data)
			return nil, fmt.Errorf("failed to unprocess chunk (decryption/decompression failed): %v", err)
		}

		// Cache the processed chunk before returning
		globalChunkCache.Put(cacheKey, finalChunk)

		return finalChunk, nil
	})

	if err != nil {
		return nil, err
	}

	chunkData, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected chunk data type")
	}

	return chunkData, nil
}

// Truncate truncates file to specified size
// If newSize > 0, references previous data block but with new size, creates new version
// If newSize == 0, uses empty data block (EmptyDataID)
func (ra *RandomAccessor) Truncate(newSize int64) (int64, error) {
	// Flush any pending writes first
	_, err := ra.Flush()
	if err != nil {
		return 0, err
	}

	// Get file object (may be from cache or database)
	fileObj, err := ra.getFileObj()
	if err != nil {
		return 0, fmt.Errorf("failed to get file object: %v", err)
	}

	oldSize := fileObj.Size

	// Also check if buffer has data
	if oldSize == 0 {
		writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
		if writeIndex > 0 {
			totalSize := atomic.LoadInt64(&ra.buffer.totalSize)
			if totalSize > 0 {
				oldSize = totalSize
				DebugLog("[VFS Truncate] File has data in buffer, using totalSize: fileID=%d, oldSize=%d (was 0)", ra.fileID, oldSize)
				// Update fileObj cache with correct size
				if fileObj != nil {
					fileObj.Size = oldSize
					fileObjCache.Put(ra.fileObjKey, fileObj)
					ra.fileObj.Store(fileObj)
				}
			}
		}
	}

	DebugLog("[VFS Truncate] Starting truncate: fileID=%d, oldSize=%d, newSize=%d, fileObj.Size=%d", ra.fileID, oldSize, newSize, fileObj.Size)
	if newSize == oldSize {
		// No change needed
		DebugLog("[VFS Truncate] Same size, returning 0: fileID=%d, size=%d", ra.fileID, newSize)
		return 0, nil
	}

	// Get LocalHandler
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		DebugLog("[VFS Truncate] ERROR: handler is not LocalHandler, operation not supported: fileID=%d, oldSize=%d, newSize=%d", ra.fileID, oldSize, newSize)
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	// Generate new version ID
	newVersionID := core.NewID()
	if newVersionID <= 0 {
		return 0, fmt.Errorf("failed to generate version ID")
	}

	var newDataID int64
	var newDataInfo *core.DataInfo

	if newSize == 0 {
		// Use empty data block
		// IMPORTANT: Always create a new version when truncating to 0, even if oldSize is 0
		// This ensures consistency and allows tracking of truncate operations
		DebugLog("[VFS Truncate] Truncating to 0: fileID=%d, oldSize=%d, fileObj.Size=%d", ra.fileID, oldSize, fileObj.Size)
		newDataID = core.EmptyDataID
		newDataInfo = &core.DataInfo{
			ID:        newDataID,
			Size:      0,
			OrigSize:  0,
			Kind:      core.DATA_NORMAL,
			PkgID:     0,
			PkgOffset: 0,
		}
	} else {
		// Reference previous data block but with new size
		oldDataID := fileObj.DataID
		if oldDataID > 0 && oldDataID != core.EmptyDataID {
			// Get old DataInfo
			oldDataInfo, err := ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, oldDataID)
			if err == nil && oldDataInfo != nil {
				// Generate new DataID
				newDataID = core.NewID()
				if newDataID <= 0 {
					return 0, fmt.Errorf("failed to generate DataID")
				}

				if oldDataInfo.PkgID > 0 {
					// If data is in package, we can reference the same package
					// If newSize > original size, need to read, extend with zeros, and write
					// If newSize == original size, reference same package with new OrigSize
					// If newSize < original size, we need to read, process, and write truncated data
					if newSize > int64(oldDataInfo.OrigSize) {
						// File is extended, need to read original data and fill with zeros
						// Use Read method to get decompressed/decrypted data
						readData, readErr := ra.Read(0, int(oldDataInfo.OrigSize))
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}
						// Ensure we only use OrigSize bytes
						if int64(len(readData)) > int64(oldDataInfo.OrigSize) {
							readData = readData[:oldDataInfo.OrigSize]
						}

						// Extend with zeros
						extendedData := make([]byte, newSize)
						copy(extendedData, readData)
						// Remaining bytes are already zero (Go zero-initializes slices)

						// Write extended data (will be compressed/encrypted if needed)
						if err := ra.Write(0, extendedData); err != nil {
							return 0, fmt.Errorf("failed to write extended data: %v", err)
						}
						_, flushErr := ra.ForceFlush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush extended data: %v", flushErr)
						}

						// Get updated fileObj to get new DataID
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						if updatedFileObj.Size != newSize {
							DebugLog("[VFS Truncate] WARNING: Truncate extend size mismatch, expected=%d actual=%d (will correct metadata)", newSize, updatedFileObj.Size)
						}

						// Return version ID (already created by Flush)
						versionID := core.NewID()
						if versionID <= 0 {
							return 0, fmt.Errorf("failed to generate version ID")
						}

						// Create version object for this truncate operation
						mTime := core.Now()
						newVersion := &core.ObjectInfo{
							ID:     versionID,
							PID:    ra.fileID,
							Type:   core.OBJ_TYPE_VERSION,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Update file object (if not already updated by Flush)
						// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							PID:    updatedFileObj.PID,
							Type:   updatedFileObj.Type,
							Name:   updatedFileObj.Name,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Batch write version and update file object
						objectsToPut := []*core.ObjectInfo{newVersion, updateFileObj}
						_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
						if err != nil {
							return 0, fmt.Errorf("failed to update file object: %v", err)
						}

						// Update cache
						fileObjCache.Put(ra.fileObjKey, updateFileObj)
						ra.fileObj.Store(updateFileObj)

						return versionID, nil
					} else if newSize == int64(oldDataInfo.OrigSize) {
						// Same size, reference same package with new OrigSize
						// Check if fileObj.Size also matches (should be the case for uncompressed files)
						if oldSize == newSize {
							// No change needed, return 0
							return 0, nil
						}
						// If fileObj.Size doesn't match but OrigSize matches, this shouldn't happen for uncompressed files
						// But for compressed files, fileObj.Size might be different from OrigSize
						// In this case, we should still return 0 since the actual data size hasn't changed
						// Only the metadata might be inconsistent, but that's not a truncate operation
						return 0, nil
					} else {
						// newSize < original size: need to read, truncate, and write data
						// Cannot just change OrigSize in package - need to actually truncate the data
						// Use Read method to get decompressed/decrypted data, then truncate
						// IMPORTANT: Temporarily save old fileObj.Size to ensure we can read full data
						// before truncating, since Read method limits reading size to fileObj.Size
						oldFileObjSize := fileObj.Size
						// Temporarily set fileObj.Size to oldDataInfo.OrigSize to allow reading full data
						// This ensures we can read the complete original data before truncating
						fileObj.Size = oldDataInfo.OrigSize
						// Update cache temporarily to allow reading full data
						ra.fileObj.Store(fileObj)
						readData, readErr := ra.Read(0, int(oldDataInfo.OrigSize))
						// Restore original fileObj.Size
						fileObj.Size = oldFileObjSize
						ra.fileObj.Store(fileObj)
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}
						// Truncate to newSize
						if int64(len(readData)) > newSize {
							readData = readData[:newSize]
						}

						// Clear buffer before writing truncated data to ensure clean state
						atomic.StoreInt64(&ra.buffer.writeIndex, 0)
						atomic.StoreInt64(&ra.buffer.totalSize, 0)
						atomic.StoreInt64(&ra.lastOffset, -1)
						if ra.seqBuffer != nil {
							putSequentialBuffer(ra.seqBuffer) // Return to pool for reuse
							ra.seqBuffer = nil
						}

						// CRITICAL: Remove existing journal before writing truncated data
						// This ensures journal.baseSize doesn't carry over the old (larger) size
						// which would cause flushSmallFile to use the wrong size
						ra.fs.journalMgr.Remove(ra.fileID)
						DebugLog("[VFS Truncate] Removed existing journal before writing truncated data: fileID=%d", ra.fileID)

						// CRITICAL: Update fileObj.Size to newSize BEFORE writing truncated data
						// This ensures that getOrCreateJournal uses the new (smaller) size as baseSize
						// Without this, the new journal would use the old (larger) size
						fileObj.Size = newSize
						fileObjCache.Put(ra.fileObjKey, fileObj)
						ra.fileObj.Store(fileObj)
						DebugLog("[VFS Truncate] Updated fileObj.Size to %d before writing truncated data: fileID=%d", newSize, ra.fileID)

						// Write truncated data (will be compressed/encrypted if needed)
						if err := ra.Write(0, readData); err != nil {
							return 0, fmt.Errorf("failed to write truncated data: %v", err)
						}
						_, flushErr := ra.ForceFlush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush truncated data: %v", flushErr)
						}

						// CRITICAL: Clear fileObj cache to force re-fetch from database
						// This ensures we get the latest fileObj with correct size after flush
						fileObjCache.Del(ra.fileObjKey)
						ra.fileObj.Store((*core.ObjectInfo)(nil))

						// Get updated fileObj to get new DataID
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						if updatedFileObj.Size != newSize {
							DebugLog("[VFS Truncate] WARNING: Truncate shrink size mismatch, expected=%d actual=%d (will correct metadata)", newSize, updatedFileObj.Size)
							// Force update size to newSize
							updatedFileObj.Size = newSize
						}

						// Return version ID (already created by Flush)
						versionID := core.NewID()
						if versionID <= 0 {
							return 0, fmt.Errorf("failed to generate version ID")
						}

						// Create version object for this truncate operation
						mTime := core.Now()
						newVersion := &core.ObjectInfo{
							ID:     versionID,
							PID:    ra.fileID,
							Type:   core.OBJ_TYPE_VERSION,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Update file object (if not already updated by Flush)
						// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							PID:    updatedFileObj.PID,
							Type:   updatedFileObj.Type,
							Name:   updatedFileObj.Name,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Batch write version and update file object
						objectsToPut := []*core.ObjectInfo{newVersion, updateFileObj}
						_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
						if err != nil {
							return 0, fmt.Errorf("failed to update file object: %v", err)
						}

						// Update cache
						fileObjCache.Put(ra.fileObjKey, updateFileObj)
						ra.fileObj.Store(updateFileObj)

						// CRITICAL: Ensure DataInfo.OrigSize matches newSize
						// This is essential for correct reading after truncate
						if updateFileObj.DataID > 0 && updateFileObj.DataID != core.EmptyDataID {
							dataInfo, err := ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, updateFileObj.DataID)
							if err == nil && dataInfo != nil {
								if dataInfo.OrigSize != newSize {
									DebugLog("[VFS Truncate] Fixing DataInfo.OrigSize mismatch: fileID=%d, dataID=%d, oldOrigSize=%d, newSize=%d", ra.fileID, updateFileObj.DataID, dataInfo.OrigSize, newSize)
									dataInfo.OrigSize = newSize
									// Update DataInfo in database
									_, err = ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
									if err != nil {
										DebugLog("[VFS Truncate] WARNING: Failed to update DataInfo.OrigSize: %v", err)
									} else {
										// Update cache with corrected DataInfo
										dataInfoCache.Put(updateFileObj.DataID, dataInfo)
									}
								}
							}
						}

						// Invalidate DataInfo cache to ensure fresh reads after truncate
						// This is critical for compressed files where DataInfo.OrigSize must match fileObj.Size
						if oldDataID > 0 && oldDataID != core.EmptyDataID {
							dataInfoCache.Del(oldDataID)
							decodingReaderCache.Del(oldDataID)
						}
						if updateFileObj.DataID > 0 && updateFileObj.DataID != core.EmptyDataID {
							// Clear decodingReaderCache to force recreation with updated DataInfo
							decodingReaderCache.Del(updateFileObj.DataID)
						}
						// CRITICAL: Force clear all caches to ensure fresh reads after truncate
						// This ensures that subsequent reads will get the updated fileObj.Size and DataInfo.OrigSize from database
						fileObjCache.Del(ra.fileObjKey)
						ra.fileObj.Store((*core.ObjectInfo)(nil))
						if updateFileObj.DataID > 0 && updateFileObj.DataID != core.EmptyDataID {
							dataInfoCache.Del(updateFileObj.DataID)
							decodingReaderCache.Del(updateFileObj.DataID)
							DebugLog("[VFS Truncate] Cleared all caches after truncate: fileID=%d, dataID=%d, newSize=%d", ra.fileID, updateFileObj.DataID, newSize)
						}

						return versionID, nil
					}
				} else {
					// Direct data (not in package)
					if newSize == int64(oldDataInfo.OrigSize) {
						// Same size as original, no change needed
						// Check if fileObj.Size also matches (should be the case for uncompressed files)
						if oldSize == newSize {
							// No change needed, return 0
							DebugLog("[VFS Truncate] Direct data same size, returning 0: fileID=%d, size=%d", ra.fileID, newSize)
							return 0, nil
						}
						// If fileObj.Size doesn't match but OrigSize matches, this shouldn't happen for uncompressed files
						// But for compressed files, fileObj.Size might be different from OrigSize
						// In this case, we should still return 0 since the actual data size hasn't changed
						// Only the metadata might be inconsistent, but that's not a truncate operation
						DebugLog("[VFS Truncate] Direct data same OrigSize but different fileObj.Size, returning 0: fileID=%d, oldSize=%d, newSize=%d", ra.fileID, oldSize, newSize)
						return 0, nil
					} else if newSize > int64(oldDataInfo.OrigSize) {
						// File is extended, need to read original data and fill with zeros
						// Use Read method to get decompressed/decrypted data
						readData, readErr := ra.Read(0, int(oldDataInfo.OrigSize))
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}
						// Ensure we only use OrigSize bytes
						if int64(len(readData)) > int64(oldDataInfo.OrigSize) {
							readData = readData[:oldDataInfo.OrigSize]
						}

						// Extend with zeros
						extendedData := make([]byte, newSize)
						copy(extendedData, readData)
						// Remaining bytes are already zero (Go zero-initializes slices)

						// Write extended data (will be compressed/encrypted if needed)
						if err := ra.Write(0, extendedData); err != nil {
							return 0, fmt.Errorf("failed to write extended data: %v", err)
						}
						_, flushErr := ra.ForceFlush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush extended data: %v", flushErr)
						}

						// Get updated fileObj to get new DataID
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						if updatedFileObj.Size != newSize {
							DebugLog("[VFS Truncate] WARNING: Truncate shrink size mismatch, expected=%d actual=%d (will correct metadata)", newSize, updatedFileObj.Size)
						}

						// Return version ID (already created by Flush)
						versionID := core.NewID()
						if versionID <= 0 {
							return 0, fmt.Errorf("failed to generate version ID")
						}

						// Create version object for this truncate operation
						mTime := core.Now()
						newVersion := &core.ObjectInfo{
							ID:     versionID,
							PID:    ra.fileID,
							Type:   core.OBJ_TYPE_VERSION,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Update file object (if not already updated by Flush)
						// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							PID:    updatedFileObj.PID,
							Type:   updatedFileObj.Type,
							Name:   updatedFileObj.Name,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Batch write version and update file object
						objectsToPut := []*core.ObjectInfo{newVersion, updateFileObj}
						_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
						if err != nil {
							return 0, fmt.Errorf("failed to update file object: %v", err)
						}

						// Update cache
						fileObjCache.Put(ra.fileObjKey, updateFileObj)
						ra.fileObj.Store(updateFileObj)

						// Invalidate DataInfo cache to ensure fresh reads after truncate/extend
						// This is critical for compressed files where DataInfo.OrigSize must match fileObj.Size
						if oldDataID > 0 && oldDataID != core.EmptyDataID {
							dataInfoCache.Del(oldDataID)
							decodingReaderCache.Del(oldDataID)
						}
						if updatedFileObj.DataID > 0 && updatedFileObj.DataID != core.EmptyDataID {
							dataInfoCache.Del(updatedFileObj.DataID)
							decodingReaderCache.Del(updatedFileObj.DataID)
						}

						return versionID, nil
					} else if newSize == int64(oldDataInfo.OrigSize) {
						// Same size as original, no change needed
						// Check if fileObj.Size also matches (should be the case for uncompressed files)
						if oldSize == newSize {
							// No change needed, return 0
							return 0, nil
						}
						// If fileObj.Size doesn't match but OrigSize matches, this shouldn't happen for uncompressed files
						// But for compressed files, fileObj.Size might be different from OrigSize
						// In this case, we should still return 0 since the actual data size hasn't changed
						// Only the metadata might be inconsistent, but that's not a truncate operation
						return 0, nil
					} else {
						// newSize < original size, need to read and write truncated data
						// Use Read method to get decompressed/decrypted data, then truncate
						// IMPORTANT: Temporarily save old fileObj.Size to ensure we can read full data
						// before truncating, since Read method limits reading size to fileObj.Size
						oldFileObjSize := fileObj.Size
						// Temporarily set fileObj.Size to oldDataInfo.OrigSize to allow reading full data
						// This ensures we can read the complete original data before truncating
						fileObj.Size = oldDataInfo.OrigSize
						// Update cache temporarily to allow reading full data
						ra.fileObj.Store(fileObj)
						readData, readErr := ra.Read(0, int(oldDataInfo.OrigSize))
						// Restore original fileObj.Size
						fileObj.Size = oldFileObjSize
						ra.fileObj.Store(fileObj)
						if readErr != nil {
							return 0, fmt.Errorf("failed to read data for truncate: %v", readErr)
						}
						// Truncate to newSize
						if int64(len(readData)) > newSize {
							readData = readData[:newSize]
						}

						// Clear buffer before writing truncated data to ensure clean state
						atomic.StoreInt64(&ra.buffer.writeIndex, 0)
						atomic.StoreInt64(&ra.buffer.totalSize, 0)
						atomic.StoreInt64(&ra.lastOffset, -1)
						if ra.seqBuffer != nil {
							putSequentialBuffer(ra.seqBuffer) // Return to pool for reuse
							ra.seqBuffer = nil
						}

						// CRITICAL: Remove existing journal before writing truncated data
						// This ensures journal.baseSize doesn't carry over the old (larger) size
						// which would cause flushSmallFile to use the wrong size
						ra.fs.journalMgr.Remove(ra.fileID)
						DebugLog("[VFS Truncate] Removed existing journal before writing truncated data: fileID=%d", ra.fileID)

						// CRITICAL: Update fileObj.Size to newSize BEFORE writing truncated data
						// This ensures that getOrCreateJournal uses the new (smaller) size as baseSize
						// Without this, the new journal would use the old (larger) size
						fileObj.Size = newSize
						fileObjCache.Put(ra.fileObjKey, fileObj)
						ra.fileObj.Store(fileObj)
						DebugLog("[VFS Truncate] Updated fileObj.Size to %d before writing truncated data: fileID=%d", newSize, ra.fileID)

						// Write truncated data (will be compressed/encrypted if needed)
						if err := ra.Write(0, readData); err != nil {
							return 0, fmt.Errorf("failed to write truncated data: %v", err)
						}
						_, flushErr := ra.ForceFlush()
						if flushErr != nil {
							return 0, fmt.Errorf("failed to flush truncated data: %v", flushErr)
						}

						// CRITICAL: Clear fileObj cache to force re-fetch from database
						// This ensures we get the latest fileObj with correct size after flush
						fileObjCache.Del(ra.fileObjKey)
						ra.fileObj.Store((*core.ObjectInfo)(nil))

						// Get updated fileObj to get new DataID
						updatedFileObj, err := ra.getFileObj()
						if err != nil {
							return 0, fmt.Errorf("failed to get updated file object: %v", err)
						}

						if updatedFileObj.Size != newSize {
							DebugLog("[VFS Truncate] WARNING: Truncate shrink size mismatch, expected=%d actual=%d (will correct metadata)", newSize, updatedFileObj.Size)
							// Force update size to newSize
							updatedFileObj.Size = newSize
						}

						// Return version ID (already created by Flush)
						versionID := core.NewID()
						if versionID <= 0 {
							return 0, fmt.Errorf("failed to generate version ID")
						}

						// Create version object for this truncate operation
						mTime := core.Now()
						newVersion := &core.ObjectInfo{
							ID:     versionID,
							PID:    ra.fileID,
							Type:   core.OBJ_TYPE_VERSION,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Update file object (if not already updated by Flush)
						// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
						updateFileObj := &core.ObjectInfo{
							ID:     ra.fileID,
							PID:    updatedFileObj.PID,
							Type:   updatedFileObj.Type,
							Name:   updatedFileObj.Name,
							DataID: updatedFileObj.DataID,
							Size:   newSize,
							MTime:  mTime,
						}

						// Batch write version and update file object
						objectsToPut := []*core.ObjectInfo{newVersion, updateFileObj}
						_, err = lh.Put(ra.fs.c, ra.fs.bktID, objectsToPut)
						if err != nil {
							return 0, fmt.Errorf("failed to update file object: %v", err)
						}

						// Update cache
						fileObjCache.Put(ra.fileObjKey, updateFileObj)
						ra.fileObj.Store(updateFileObj)

						// CRITICAL: Ensure DataInfo.OrigSize matches newSize
						// This is essential for correct reading after truncate
						if updateFileObj.DataID > 0 && updateFileObj.DataID != core.EmptyDataID {
							dataInfo, err := ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, updateFileObj.DataID)
							if err == nil && dataInfo != nil {
								if dataInfo.OrigSize != newSize {
									DebugLog("[VFS Truncate] Fixing DataInfo.OrigSize mismatch: fileID=%d, dataID=%d, oldOrigSize=%d, newSize=%d", ra.fileID, updateFileObj.DataID, dataInfo.OrigSize, newSize)
									dataInfo.OrigSize = newSize
									// Update DataInfo in database
									_, err = ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
									if err != nil {
										DebugLog("[VFS Truncate] WARNING: Failed to update DataInfo.OrigSize: %v", err)
									}
									// Always clear cache after updating DataInfo to ensure fresh reads
									// Don't update cache here - let subsequent reads fetch from database
									dataInfoCache.Del(updateFileObj.DataID)
									decodingReaderCache.Del(updateFileObj.DataID)
								} else {
									// DataInfo.OrigSize already matches, but clear cache to ensure fresh reads
									dataInfoCache.Del(updateFileObj.DataID)
									decodingReaderCache.Del(updateFileObj.DataID)
								}
							}
						}

						// Invalidate DataInfo cache to ensure fresh reads after truncate
						// This is critical for compressed files where DataInfo.OrigSize must match fileObj.Size
						if oldDataID > 0 && oldDataID != core.EmptyDataID {
							dataInfoCache.Del(oldDataID)
							decodingReaderCache.Del(oldDataID)
						}
						if updateFileObj.DataID > 0 && updateFileObj.DataID != core.EmptyDataID {
							// Clear decodingReaderCache to force recreation with updated DataInfo
							decodingReaderCache.Del(updateFileObj.DataID)
							// Also clear DataInfo cache to force re-fetch from database
							dataInfoCache.Del(updateFileObj.DataID)
						}
						// CRITICAL: Force clear all caches to ensure fresh reads after truncate
						// This ensures that subsequent reads will get the updated fileObj.Size and DataInfo.OrigSize from database
						fileObjCache.Del(ra.fileObjKey)
						ra.fileObj.Store((*core.ObjectInfo)(nil))
						if updateFileObj.DataID > 0 && updateFileObj.DataID != core.EmptyDataID {
							dataInfoCache.Del(updateFileObj.DataID)
							decodingReaderCache.Del(updateFileObj.DataID)
							DebugLog("[VFS Truncate] Cleared all caches after truncate: fileID=%d, dataID=%d, newSize=%d", ra.fileID, updateFileObj.DataID, newSize)
						}

						return versionID, nil
					}
				}
			} else {
				// No old DataInfo, create new empty data block
				newDataID = core.EmptyDataID
				newDataInfo = &core.DataInfo{
					ID:        newDataID,
					Size:      0,
					OrigSize:  0,
					Kind:      core.DATA_NORMAL,
					PkgID:     0,
					PkgOffset: 0,
				}
			}
		} else {
			// No old data, create new empty data block
			newDataID = core.EmptyDataID
			newDataInfo = &core.DataInfo{
				ID:        newDataID,
				Size:      0,
				OrigSize:  0,
				Kind:      core.DATA_NORMAL,
				PkgID:     0,
				PkgOffset: 0,
			}
		}
	}

	// Create new version object
	mTime := core.Now()
	newVersion := &core.ObjectInfo{
		ID:     newVersionID,
		PID:    ra.fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// Get file object to get Type, Name, PID for updateFileObj
	DebugLog("[VFS Truncate] Before getFileObj: fileID=%d, newVersionID=%d, newDataID=%d, newSize=%d", ra.fileID, newVersionID, newDataID, newSize)
	fileObj, err = ra.getFileObj()
	if err != nil {
		DebugLog("[VFS Truncate] ERROR: Failed to get file object: fileID=%d, error=%v", ra.fileID, err)
		return 0, fmt.Errorf("failed to get file object: %v", err)
	}
	DebugLog("[VFS Truncate] Got fileObj: fileID=%d, fileObj.Size=%d, fileObj.DataID=%d", ra.fileID, fileObj.Size, fileObj.DataID)

	// Update file object
	// IMPORTANT: Must include Type, Name, PID to avoid cache corruption
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		PID:    fileObj.PID,
		Type:   fileObj.Type,
		Name:   fileObj.Name,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// Optimization: Use PutDataInfoAndObj to write DataInfo, version object, and file object update together
	// This reduces database round trips and improves performance
	var dataInfos []*core.DataInfo
	if newDataInfo != nil && newDataID != core.EmptyDataID {
		dataInfos = []*core.DataInfo{newDataInfo}
		DebugLog("[VFS Truncate] Will write DataInfo: fileID=%d, newDataID=%d, newSize=%d", ra.fileID, newDataID, newSize)
	} else {
		DebugLog("[VFS Truncate] No DataInfo to write (newDataID=%d, newSize=%d): fileID=%d", newDataID, newSize, ra.fileID)
	}
	// Create version object first
	_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{newVersion})
	if err != nil {
		DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to create version: %v", err)
		return 0, err
	}

	if len(dataInfos) > 0 {
		// Write DataInfo
		DebugLog("[VFS applyRandomWritesWithSDK] Writing DataInfo to disk: fileID=%d, newDataID=%d, newSize=%d", ra.fileID, newDataID, newSize)
		_, err = lh.PutDataInfo(ra.fs.c, ra.fs.bktID, dataInfos)
		if err != nil {
			DebugLog("[VFS applyRandomWritesWithSDK] ERROR: Failed to write DataInfo to disk: fileID=%d, newDataID=%d, error=%v", ra.fileID, newDataID, err)
			return 0, fmt.Errorf("failed to save DataInfo: %v", err)
		}
		DebugLog("[VFS applyRandomWritesWithSDK] Successfully wrote DataInfo to disk: fileID=%d, newDataID=%d, newSize=%d", ra.fileID, newDataID, newSize)
		// Update cache
		dataInfoCache.Put(newDataID, newDataInfo)
		// Clear decoded file cache and reader cache if DataID changed or size changed (important for truncate)
		// This ensures that truncated files don't use stale cached data
		// For packaged data with same DataID but different OrigSize, we need to clear cache
		if newSize != oldSize {
			// Clear both old and new DataID caches to ensure fresh reads
			if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
				decodingReaderCache.Del(fileObj.DataID)
			}
			if newDataID > 0 && newDataID != core.EmptyDataID {
				decodingReaderCache.Del(newDataID)
			}
			DebugLog("[VFS Truncate] Cleared decoding reader cache: fileID=%d, oldDataID=%d, newDataID=%d, oldSize=%d, newSize=%d", ra.fileID, fileObj.DataID, newDataID, oldSize, newSize)
		}
	}

	// Update file object using SetObj to ensure Size field is updated
	// Put uses InsertIgnore which won't update existing objects
	DebugLog("[VFS applyRandomWritesWithSDK] 🔍 Calling SetObj to update file object (with DataInfo): fileID=%d, newSize=%d, newDataID=%d, updateFileObj.Size=%d", ra.fileID, newSize, newDataID, updateFileObj.Size)
	err = lh.MetadataAdapter().SetObj(ra.fs.c, ra.fs.bktID, []string{"did", "s", "m"}, updateFileObj)
	if err != nil {
		DebugLog("[VFS applyRandomWritesWithSDK] ❌ ERROR: Failed to update file object: %v", err)
		return 0, fmt.Errorf("failed to update file object: %v", err)
	}
	DebugLog("[VFS applyRandomWritesWithSDK] ✅ Successfully updated file object using SetObj (with DataInfo): fileID=%d, newSize=%d, newDataID=%d, updateFileObj.Size=%d", ra.fileID, newSize, newDataID, updateFileObj.Size)

	// Clear decoding reader cache if size changed (important for truncate)
	// This ensures that truncated files don't use stale cached data
	if newSize != oldSize && fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		decodingReaderCache.Del(fileObj.DataID)
		DebugLog("[VFS Truncate] Cleared decoding reader cache (size changed): fileID=%d, DataID=%d, oldSize=%d, newSize=%d", ra.fileID, fileObj.DataID, oldSize, newSize)
	}

	// IMPORTANT: Update cache with updateFileObj which has the correct newSize
	// This ensures subsequent getFileObj() calls return the updated fileObj with correct Size
	fileObjCache.Put(ra.fileObjKey, updateFileObj)
	ra.fileObj.Store(updateFileObj)
	DebugLog("[VFS applyRandomWritesWithSDK] Updated fileObj cache: fileID=%d, newSize=%d, newDataID=%d", ra.fileID, updateFileObj.Size, updateFileObj.DataID)

	// Update directory listing cache to ensure file is visible in Readdir
	if updateFileObj.PID > 0 {
		dirNode := &OrcasNode{
			fs:    ra.fs,
			objID: updateFileObj.PID,
		}
		dirNode.invalidateDirListCache(updateFileObj.PID)
		DebugLog("[VFS applyRandomWritesWithSDK] Appended file to directory listing cache: fileID=%d, dirID=%d, name=%s", ra.fileID, updateFileObj.PID, updateFileObj.Name)
	}

	// Invalidate old DataInfo cache if DataID changed
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID && fileObj.DataID != newDataID {
		oldDataInfoCacheKey := fileObj.DataID
		dataInfoCache.Del(oldDataInfoCacheKey)
	}

	// Reset buffer state after truncate to ensure subsequent writes work correctly
	// This is important because truncate changes the file size, and we need to reset
	// the lastOffset and ensure buffer is ready for new writes
	atomic.StoreInt64(&ra.lastOffset, -1)
	atomic.StoreInt64(&ra.buffer.writeIndex, 0)
	atomic.StoreInt64(&ra.buffer.totalSize, 0)
	// Clear sequential buffer if it exists
	if ra.seqBuffer != nil {
		putSequentialBuffer(ra.seqBuffer) // Return to pool for reuse
		ra.seqBuffer = nil
	}

	// Keep updated fileObj in cache (don't clear it) to ensure subsequent reads use correct size
	// FileObj has been updated with newSize, so we should keep it in cache
	// Also invalidate DataInfo cache to ensure fresh reads use correct OrigSize
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		dataInfoCache.Del(fileObj.DataID)
		decodingReaderCache.Del(fileObj.DataID)
	}
	if newDataID > 0 && newDataID != core.EmptyDataID {
		dataInfoCache.Del(newDataID)
		decodingReaderCache.Del(newDataID)
	}

	// CRITICAL: Set sparseSize for sparse file optimization
	// When truncating to a larger size (especially from 0), mark as sparse
	// This allows subsequent writes to use temporary write area efficiently
	if newSize > oldSize {
		atomic.StoreInt64(&ra.sparseSize, newSize)
		DebugLog("[VFS Truncate] Marked as sparse file: fileID=%d, sparseSize=%d", ra.fileID, newSize)
	}

	DebugLog("[VFS Truncate] Returning versionID: fileID=%d, versionID=%d, newSize=%d, newDataID=%d", ra.fileID, newVersionID, newSize, newDataID)
	return newVersionID, nil
}

func (ra *RandomAccessor) Close() error {
	DebugLog("[VFS RandomAccessor Close] Starting Close: fileID=%d", ra.fileID)
	// Cancel any pending delayed flush so we can finish synchronously
	ra.cancelDelayedFlush()

	// PRIORITY 1: Flush ChunkedFileWriter for sparse files (completes on close)
	// For .tmp files, ChunkedFileWriter is flushed on rename (removing .tmp suffix)
	// IMPORTANT: For sparse files, we must complete the flush on close to ensure all data is persisted
	// This is different from .tmp files which flush on rename
	sparseSize := ra.getSparseSize()
	if sparseSize > 0 {
		// Use flushChunkedWriter to ensure complete flush with proper fileObj update
		if err := ra.flushChunkedWriter(WRITER_TYPE_SPARSE); err != nil {
			DebugLog("[VFS RandomAccessor Close] WARNING: Failed to flush ChunkedFileWriter for sparse file: %v", err)
			// Don't return error immediately, continue with other cleanup
			// But we should still try to clear the ChunkedFileWriter to prevent issues
		}
		// Clear ChunkedFileWriter after flush for sparse files (flushChunkedWriter doesn't clear it)
		ra.chunkedWriter.Store(clearedChunkedWriterMarker)

		// Check if file is fully written and clear sparse file flag if so
		// This is done after flush to ensure we have the latest file size
		// Re-read fileObj from cache after flush to get the latest size
		// Note: flushChunkedWriter already checks and clears sparse flag, but we check again here
		// to ensure it's cleared even if the check in flushChunkedWriter didn't work
		fileObjAfterFlush, err := ra.getFileObj()
		if err == nil && fileObjAfterFlush != nil {
			// Use ChunkedFileWriter's size if available, otherwise use fileObj.Size
			// ChunkedFileWriter's size is the actual written size
			val := ra.chunkedWriter.Load()
			actualSize := fileObjAfterFlush.Size
			if val != nil && val != clearedChunkedWriterMarker {
				if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil {
					cwSize := atomic.LoadInt64(&cw.size)
					if cwSize > actualSize {
						actualSize = cwSize
					}
				}
			}
			if actualSize >= sparseSize {
				DebugLog("[VFS RandomAccessor Close] Sparse file fully written (size=%d >= sparseSize=%d), clearing sparse file flag: fileID=%d", actualSize, sparseSize, ra.fileID)
				atomic.StoreInt64(&ra.sparseSize, 0)
			} else {
				DebugLog("[VFS RandomAccessor Close] Sparse file not fully written (size=%d < sparseSize=%d), keeping sparse file flag: fileID=%d", actualSize, sparseSize, ra.fileID)
			}
		}

		DebugLog("[VFS RandomAccessor Close] Completed ChunkedFileWriter flush for sparse file: fileID=%d", ra.fileID)
	}

	// PRIORITY 2: Close journal if exists (flushes and creates new version)
	if err := ra.closeJournal(); err != nil {
		DebugLog("[VFS RandomAccessor Close] WARNING: Failed to close journal: %v", err)
		// Don't return error, continue with other cleanup
	}

	// PRIORITY 3: Synchronously flush all pending write data (buffer, etc.)
	// This handles any remaining buffered writes that weren't handled by ChunkedFileWriter
	// IMPORTANT: For sparse files, ChunkedFileWriter was already flushed above,
	// so we need to ensure Flush() doesn't overwrite the dataID
	// Save the dataID before Flush() to restore it if needed
	// CRITICAL: Use ForceFlush() instead of Flush() to ensure all data is flushed on Close
	// This is important for deferred flush optimization - buffer may not be full but should still be flushed
	// MEMORY OPTIMIZATION: Check if there's any data to flush before calling ForceFlush
	// This avoids unnecessary decryption operations when cleaning up inactive RandomAccessors
	hasDataToFlush := false

	// Check if there's data in buffer
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	totalSize := atomic.LoadInt64(&ra.buffer.totalSize)
	if writeIndex > 0 || totalSize > 0 {
		hasDataToFlush = true
	}

	// Check if there's data in sequential buffer
	if !hasDataToFlush && ra.seqBuffer != nil {
		ra.seqBuffer.mu.Lock()
		hasSeqData := ra.seqBuffer.hasData || len(ra.seqBuffer.buffer) > 0
		ra.seqBuffer.mu.Unlock()
		if hasSeqData {
			hasDataToFlush = true
		}
	}

	// Check if there's a journal with data
	if !hasDataToFlush {
		ra.journalMu.RLock()
		hasJournal := ra.journal != nil
		ra.journalMu.RUnlock()
		if hasJournal {
			hasDataToFlush = true
		}
	}

	var savedDataID int64 = 0
	if sparseSize > 0 {
		if cachedObj := ra.fileObj.Load(); cachedObj != nil {
			if obj, ok := cachedObj.(*core.ObjectInfo); ok && obj != nil {
				savedDataID = obj.DataID
				DebugLog("[VFS RandomAccessor Close] Saved dataID before ForceFlush(): fileID=%d, dataID=%d", ra.fileID, savedDataID)
			}
		}
	}

	if hasDataToFlush {
		DebugLog("[VFS RandomAccessor Close] Calling ForceFlush: fileID=%d", ra.fileID)
		_, err := ra.ForceFlush()
		if err != nil {
			DebugLog("[VFS RandomAccessor Close] ERROR: ForceFlush failed: fileID=%d, error=%v", ra.fileID, err)
			return err
		}
		DebugLog("[VFS RandomAccessor Close] ForceFlush completed: fileID=%d", ra.fileID)
	} else {
		DebugLog("[VFS RandomAccessor Close] Skipping ForceFlush (no data to flush): fileID=%d", ra.fileID)
	}

	// Log final file object state after flush
	fileObjAfterFlush, err := ra.getFileObj()
	if err == nil && fileObjAfterFlush != nil {
		DebugLog("[VFS RandomAccessor Close] Final file object after flush: fileID=%d, DataID=%d, Size=%d",
			ra.fileID, fileObjAfterFlush.DataID, fileObjAfterFlush.Size)
	}

	// IMPORTANT: After Flush(), verify that fileObj still has correct dataID
	// If Flush() overwrote it with 0 (e.g., from stale database read), restore it
	if sparseSize > 0 && savedDataID > 0 {
		if cachedObj := ra.fileObj.Load(); cachedObj != nil {
			if obj, ok := cachedObj.(*core.ObjectInfo); ok && obj != nil {
				if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
					DebugLog("[VFS RandomAccessor Close] WARNING: fileObj dataID was reset to 0 after ForceFlush(), restoring from saved value: fileID=%d, savedDataID=%d", ra.fileID, savedDataID)
					// Restore the saved dataID
					obj.DataID = savedDataID
					ra.fileObj.Store(obj)
					fileObjCache.Put(ra.fileObjKey, obj)
					DebugLog("[VFS RandomAccessor Close] Restored fileObj dataID: fileID=%d, dataID=%d", ra.fileID, savedDataID)
				}
			}
		}
	}

	// Clean up decoding reader cache to free memory
	// This honors the request to release cache on file close
	if fileObj, _ := ra.getFileObj(); fileObj != nil {
		if fileObj.DataID > 0 {
			decodingReaderCache.Del(fileObj.DataID)
			DebugLog("[VFS RandomAccessor Close] Removed from decodingReaderCache: fileID=%d, dataID=%d", ra.fileID, fileObj.DataID)
		}
	}

	return nil
}

// flushChunkedWriter flushes the ChunkedFileWriter when forcing a flush
// For .tmp files: called before rename (removing .tmp suffix)
// For sparse files: called on file close
func (ra *RandomAccessor) flushChunkedWriter(writerType WriterType) error {
	// Lock-free read using atomic.Value
	val := ra.chunkedWriter.Load()
	if val == nil || val == clearedChunkedWriterMarker {
		return nil
	}
	cw, ok := val.(*ChunkedFileWriter)
	if !ok || cw == nil {
		return nil
	}

	// Check if writer type matches
	if cw.writerType != writerType {
		return nil // Not the right type, skip
	}

	// Validate ChunkedFileWriter before flushing
	// If fileID or dataID is 0, ChunkedFileWriter is invalid and should not be flushed
	if cw.fileID <= 0 || cw.dataID <= 0 {
		DebugLog("[VFS RandomAccessor flushChunkedWriter] WARNING: ChunkedFileWriter is invalid (fileID=%d, dataID=%d), skipping flush: ra.fileID=%d", cw.fileID, cw.dataID, ra.fileID)
		// Clear invalid ChunkedFileWriter
		ra.chunkedWriter.Store(clearedChunkedWriterMarker)
		return nil
	}

	writerTypeName := "tmp"
	if writerType == WRITER_TYPE_SPARSE {
		writerTypeName = "sparse"
	} else if writerType == WRITER_TYPE_SEQ {
		writerTypeName = "seq"
	}
	DebugLog("[VFS RandomAccessor flushChunkedWriter] Forcing ChunkedFileWriter flush for %s file: fileID=%d, dataID=%d", writerTypeName, cw.fileID, cw.dataID)
	// Force flush to ensure incomplete last chunk is also flushed (on close/release)
	if err := cw.Flush(true); err != nil {
		DebugLog("[VFS RandomAccessor flushChunkedWriter] ERROR: ChunkedFileWriter flush failed: fileID=%d, dataID=%d, error=%v", cw.fileID, cw.dataID, err)
		return err
	}

	// IMPORTANT: ChunkedFileWriter.Flush() already updated the cache, don't re-read from database
	// Just verify the cache was updated to avoid WAL dirty read
	if cachedObj := ra.fileObj.Load(); cachedObj != nil {
		if obj, ok := cachedObj.(*core.ObjectInfo); ok && obj != nil {
			DebugLog("[VFS RandomAccessor flushChunkedWriter] Verified fileObj cache after ChunkedFileWriter flush: fileID=%d, size=%d, dataID=%d", ra.fileID, obj.Size, obj.DataID)

			// IMPORTANT: For sparse files, if file is fully written (size >= sparseSize), clear sparse file flag
			// This ensures that fully written files are no longer treated as sparse files
			if writerType == WRITER_TYPE_SPARSE {
				sparseSize := ra.getSparseSize()
				if sparseSize > 0 && obj.Size >= sparseSize {
					DebugLog("[VFS RandomAccessor flushChunkedWriter] Sparse file fully written (size=%d >= sparseSize=%d), clearing sparse file flag: fileID=%d", obj.Size, sparseSize, ra.fileID)
					atomic.StoreInt64(&ra.sparseSize, 0)
				}
			}
		}
	}

	return nil
}

// flushTempFileWriter flushes the ChunkedFileWriter for .tmp files (backward compatibility)
func (ra *RandomAccessor) flushTempFileWriter() error {
	return ra.flushChunkedWriter(WRITER_TYPE_TMP)
}

// hasChunkedWriter reports whether this RandomAccessor currently owns a ChunkedFileWriter
// Lock-free check using atomic.Value
func (ra *RandomAccessor) hasChunkedWriter(writerType WriterType) bool {
	val := ra.chunkedWriter.Load()
	if val == nil || val == clearedChunkedWriterMarker {
		return false
	}
	cw, ok := val.(*ChunkedFileWriter)
	if !ok || cw == nil {
		return false
	}
	return cw.writerType == writerType
}

// hasTempFileWriter reports whether this RandomAccessor currently owns a ChunkedFileWriter for .tmp files (backward compatibility)
func (ra *RandomAccessor) hasTempFileWriter() bool {
	return ra.hasChunkedWriter(WRITER_TYPE_TMP)
}

// mergeWriteOperations merges overlapping write operations
// Optimization: use more efficient sorting algorithm (quicksort)
// IMPORTANT: When sorting by offset, we need to preserve the original order for operations with the same offset
// This ensures that later writes (in time order) overwrite earlier writes when they overlap
func mergeWriteOperations(operations []WriteOperation) []WriteOperation {
	if len(operations) == 0 {
		return nil
	}

	// Optimization: if operations are already sorted, can skip sorting
	// Optimization: for small number of operations, use insertion sort (already sorted for sequential writes)
	// For larger numbers, use more efficient sorting
	sorted := operations
	if len(sorted) > 1 {
		// Check if already sorted (common case for sequential writes)
		isSorted := true
		for i := 1; i < len(sorted); i++ {
			if sorted[i-1].Offset > sorted[i].Offset {
				isSorted = false
				break
			}
		}

		if !isSorted {
			// Create new slice for sorting (avoid modifying original slice)
			// IMPORTANT: Use stable sort to preserve order of operations with same offset
			// This ensures that later writes (in original order) overwrite earlier writes
			sorted = make([]WriteOperation, len(operations))
			copy(sorted, operations)

			// Use stable insertion sort to preserve order for same offset
			// This ensures that when operations have the same offset, the later one (in original order) is used
			if len(sorted) < 50 {
				// Optimized stable insertion sort (in-place, preserves order for equal elements)
				for i := 1; i < len(sorted); i++ {
					key := sorted[i]
					j := i - 1
					// Sort by offset, but preserve original order for same offset (stable sort)
					for j >= 0 && sorted[j].Offset > key.Offset {
						sorted[j+1] = sorted[j]
						j--
					}
					sorted[j+1] = key
				}
			} else {
				// For larger arrays, use standard library stable sort (guaranteed stability)
				sort.SliceStable(sorted, func(i, j int) bool {
					return sorted[i].Offset < sorted[j].Offset
				})
			}
		}
	}

	// Optimization: pre-allocate capacity to reduce expansion
	// Optimization: use object pool to get initial capacity
	merged := writeOpsPool.Get().([]WriteOperation)
	merged = merged[:0] // Reset but keep capacity
	if cap(merged) < len(sorted) {
		merged = make([]WriteOperation, 0, len(sorted))
	}

	for _, op := range sorted {
		if len(merged) == 0 {
			merged = append(merged, op)
			continue
		}

		last := &merged[len(merged)-1]
		lastEnd := last.Offset + int64(len(last.Data))
		opEnd := op.Offset + int64(len(op.Data))

		// Optimization: if completely overlapping and new operation overwrites old, directly replace (avoid creating new object)
		if op.Offset >= last.Offset && opEnd <= lastEnd {
			// New operation is completely within old operation, directly overwrite (avoid creating new WriteOperation)
			offsetInLast := op.Offset - last.Offset
			copy(last.Data[offsetInLast:], op.Data)
			continue
		}

		// If overlapping, merge
		if op.Offset <= lastEnd {
			// Calculate new range
			startOffset := last.Offset
			if op.Offset < startOffset {
				startOffset = op.Offset
			}
			endOffset := lastEnd
			if opEnd > endOffset {
				endOffset = opEnd
			}

			// Optimization: check if we can reuse last.Data slice
			mergedSize := int(endOffset - startOffset)
			var mergedData []byte

			// Try to reuse last.Data if it has sufficient capacity
			if cap(last.Data) >= mergedSize && last.Offset == startOffset {
				// Can reuse existing slice, just extend it
				mergedData = last.Data[:mergedSize]
				// IMPORTANT: Since operations are sorted by offset and we process them in order,
				// and operations maintain their original order for same offset (stable sort),
				// op comes after last in the original order, so op should overwrite last
				// Old data is already in place, just overwrite with new data
				if op.Offset >= startOffset {
					copy(mergedData[op.Offset-startOffset:], op.Data)
				}
			} else {
				// Need new buffer (no object pool)
				mergedData = make([]byte, mergedSize)

				// IMPORTANT: Copy old data first, then overwrite with new data
				// Since operations are sorted by offset and maintain original order for same offset,
				// op comes after last in the original order, so op should overwrite last
				if last.Offset >= startOffset {
					copy(mergedData[last.Offset-startOffset:], last.Data)
				}
				// Overwrite with new data (op comes later in original order, so it should overwrite)
				if op.Offset >= startOffset {
					copy(mergedData[op.Offset-startOffset:], op.Data)
				}
			}

			last.Offset = startOffset
			last.Data = mergedData
		} else {
			// Not overlapping, add new operation
			merged = append(merged, op)
		}
	}

	return merged
}

// applyWritesToData applies write operations to data
// Optimization: calculate required size at once to avoid multiple expansions
func applyWritesToData(data []byte, writes []WriteOperation) []byte {
	if len(writes) == 0 {
		return data
	}

	// Calculate required size (optimization: calculate at once to avoid multiple expansions)
	var maxSize int64 = int64(len(data))
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > maxSize {
			maxSize = writeEnd
		}
	}

	// Optimization: allocate required size at once to avoid multiple expansions
	result := make([]byte, maxSize)
	if len(data) > 0 {
		copy(result, data)
	}

	// Apply all write operations (optimization: remove redundant checks, maxSize already guarantees sufficient capacity)
	for _, write := range writes {
		if len(write.Data) > 0 {
			writeEnd := write.Offset + int64(len(write.Data))
			copy(result[write.Offset:writeEnd], write.Data)
		}
	}

	return result
}

// tryInstantUpload attempts instant upload by calculating checksums and calling Ref
// Returns DataID if instant upload succeeds (> 0), 0 if it fails
func tryInstantUpload(fs *OrcasFS, data []byte, origSize int64, kind uint32) (int64, error) {
	// Calculate checksums using SDK
	hdrXXH3, xxh3Val, sha256_0, sha256_1, sha256_2, sha256_3 := core.CalculateChecksums(data)

	// Create DataInfo for Ref
	dataInfo := &core.DataInfo{
		OrigSize: origSize,
		HdrXXH3:  hdrXXH3,
		XXH3:     xxh3Val,
		SHA256_0: sha256_0,
		SHA256_1: sha256_1,
		SHA256_2: sha256_2,
		SHA256_3: sha256_3,
		Kind:     kind,
	}

	// Call Ref to check if data already exists
	refIDs, err := fs.h.Ref(fs.c, fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return 0, err
	}

	if len(refIDs) > 0 && refIDs[0] != 0 {
		if refIDs[0] > 0 {
			// Instant upload succeeded, return existing DataID from database
			return refIDs[0], nil
		} else {
			// Negative ID means reference to another element in current batch
			// This should not happen in VFS (single file write)
			// But we handle it for completeness: skip instant upload, return 0
			// The negative reference will be resolved in PutDataInfo
			return 0, nil
		}
	}

	// Instant upload failed, return 0
	return 0, nil
}

// shouldUseJournal determines if journal should be used for this file
// offset and length are used for sparse file local sequential range detection
func (ra *RandomAccessor) shouldUseJournal(fileObj *core.ObjectInfo, offset, length int64) bool {
	// Check if journal is enabled
	if ra.fs.journalMgr == nil || !ra.fs.journalMgr.config.Enabled {
		return false
	}

	// Don't use journal for .tmp files (they use ChunkedFileWriter)
	if ra.isTmpFile {
		return false
	}

	// Don't use journal if ChunkedFileWriter is active (WRITER_TYPE_SEQ or WRITER_TYPE_SPARSE)
	// ChunkedFileWriter handles its own versioning and progress tracking
	if val := ra.chunkedWriter.Load(); val != nil && val != clearedChunkedWriterMarker {
		if cw, ok := val.(*ChunkedFileWriter); ok && cw != nil {
			if cw.writerType == WRITER_TYPE_SEQ || cw.writerType == WRITER_TYPE_SPARSE {
				DebugLog("[VFS shouldUseJournal] ChunkedFileWriter active (type=%d), not using journal: fileID=%d", cw.writerType, ra.fileID)
				return false
			}
		}
	}

	// OPTIMIZATION: Check if this is a sequential append (offset == current file size)
	// If flush happened but file is still open, we should continue using sequential buffer
	// for sequential writes from the end of the file
	if offset == fileObj.Size {
		// Sequential append from end of file - check if we can reuse sequential buffer
		if ra.seqBuffer != nil {
			ra.seqBuffer.mu.Lock()
			seqClosed := ra.seqBuffer.closed
			ra.seqBuffer.mu.Unlock()
			if !seqClosed {
				// Sequential buffer is still active, continue using it
				DebugLog("[VFS shouldUseJournal] Sequential append from file end (offset=%d == size=%d), continuing with sequential buffer: fileID=%d", offset, fileObj.Size, ra.fileID)
				return false
			} else {
				// Sequential buffer was closed after flush, but this is still sequential append
				// We should reinitialize sequential buffer instead of using journal
				DebugLog("[VFS shouldUseJournal] Sequential append after flush (offset=%d == size=%d), should reinit sequential buffer: fileID=%d", offset, fileObj.Size, ra.fileID)
				return false
			}
		} else {
			// No sequential buffer, but this is sequential append - should initialize sequential buffer
			DebugLog("[VFS shouldUseJournal] Sequential append from file end (offset=%d == size=%d), should use sequential buffer: fileID=%d", offset, fileObj.Size, ra.fileID)
			return false
		}
	}

	// PRIORITY CHECK: If we are already using sequential buffer, continue using it
	// This allows sequential writes (even non-zero offset) to bypass journal
	// and continue using the efficient sequential buffer path
	if ra.seqBuffer != nil {
		ra.seqBuffer.mu.Lock()
		seqOffset := ra.seqBuffer.offset
		seqClosed := ra.seqBuffer.closed
		ra.seqBuffer.mu.Unlock()

		if !seqClosed && offset == seqOffset {
			DebugLog("[VFS shouldUseJournal] Sequential write matching buffer offset, ensuring sequential buffer usage: fileID=%d, offset=%d", ra.fileID, offset)
			return false
		}
	}

	// Fast-path: any non-zero offset write is a random write candidate.
	// To ensure correctness (especially for files with existing or future data),
	// prefer journaling so reads can see these modifications immediately.
	if offset != 0 {
		DebugLog("[VFS shouldUseJournal] Non-zero offset write, using journal: fileID=%d, offset=%d, length=%d", ra.fileID, offset, length)
		return true
	}

	// OPTIMIZATION: Don't use journal for small files after truncate(0) with sequential writes
	// This is a common pattern: truncate(0) + write new content sequentially
	// Sequential buffer is more efficient and avoids journal complexity
	if fileObj.DataID == core.EmptyDataID || fileObj.Size == 0 {
		// Check if we have sequential write buffer initialized
		if ra.seqBuffer != nil {
			ra.seqBuffer.mu.Lock()
			hasSeqData := ra.seqBuffer.hasData && !ra.seqBuffer.closed
			ra.seqBuffer.mu.Unlock()
			if hasSeqData {
				// Already using sequential buffer, don't switch to journal
				DebugLog("[VFS shouldUseJournal] Skipping journal for sequential write after truncate: fileID=%d", ra.fileID)
				return false
			}
		}

		// Check if current write is sequential (offset == 0 or continuing from last write)
		lastOffset := atomic.LoadInt64(&ra.lastOffset)
		if lastOffset == 0 {
			// First write after truncate, prefer sequential buffer for small files
			// Journal is better for large files or random writes
			DebugLog("[VFS shouldUseJournal] First write after truncate, preferring sequential buffer: fileID=%d", ra.fileID)
			return false
		}
	}

	// OPTIMIZATION: Sparse files write path selection
	// Key issue: Buffer path (applyRandomWritesWithSDK) will rewrite entire file on each flush!
	// For 1GB sparse file, even writing 16MB triggers rewriting all 106 chunks
	sparseSize := ra.getSparseSize()
	if sparseSize > 0 {
		// Check if writes are sequential from start (fresh upload scenario)
		lastOffset := atomic.LoadInt64(&ra.lastOffset)

		// Sequential pattern detection:
		// 1. First write (lastOffset == 0 or -1) starting from offset 0
		// 2. Or sequential buffer already active with data
		if ra.seqBuffer != nil {
			ra.seqBuffer.mu.Lock()
			hasSeqData := ra.seqBuffer.hasData && !ra.seqBuffer.closed
			seqOffset := ra.seqBuffer.offset
			ra.seqBuffer.mu.Unlock()
			if hasSeqData && seqOffset > 0 {
				// Already have sequential buffer with data, continue using it
				DebugLog("[VFS shouldUseJournal] Sparse file with sequential buffer active, not using journal: fileID=%d, sparseSize=%d, seqOffset=%d",
					ra.fileID, sparseSize, seqOffset)
				return false
			}
		}

		// If lastOffset is small (near start of file), this looks like initial sequential upload
		// Allow sequential buffer for fresh uploads (not using journal)
		if lastOffset <= 0 {
			// First write or no writes yet - defer decision, don't force journal
			DebugLog("[VFS shouldUseJournal] Sparse file first write, deferring journal decision: fileID=%d, sparseSize=%d, lastOffset=%d",
				ra.fileID, sparseSize, lastOffset)
			return false
		}

		// OPTIMIZATION: For sparse files, check if writes are within a few chunks (local sequential)
		// If writes are within 1-2 chunks, treat as sequential even if out of order
		// This avoids using journal for small local writes, improving performance
		chunkSize := ra.fs.chunkSize
		if chunkSize <= 0 {
			chunkSize = DefaultChunkSize
		}
		localSeqRange := int64(LocalSequentialChunkCount) * chunkSize

		writeRangeStart := atomic.LoadInt64(&ra.writeRangeStart)
		writeRangeEnd := atomic.LoadInt64(&ra.writeRangeEnd)
		currentWriteEnd := offset + length

		// Check if current write is within local sequential range
		if writeRangeStart > 0 && writeRangeEnd > 0 {
			// We have an existing write range, check if current write extends it within limit
			newRangeStart := writeRangeStart
			newRangeEnd := writeRangeEnd
			if offset < writeRangeStart {
				newRangeStart = offset
			}
			if currentWriteEnd > writeRangeEnd {
				newRangeEnd = currentWriteEnd
			}
			newRangeSize := newRangeEnd - newRangeStart
			if newRangeSize <= localSeqRange {
				// Still within local sequential range, don't use journal
				DebugLog("[VFS shouldUseJournal] Sparse file writes within local sequential range (%d chunks), not using journal: fileID=%d, sparseSize=%d, rangeSize=%d, chunkSize=%d",
					LocalSequentialChunkCount, ra.fileID, sparseSize, newRangeSize, chunkSize)
				return false
			}
		} else {
			// First write in range, check if it's small enough to be local sequential
			if length <= localSeqRange {
				DebugLog("[VFS shouldUseJournal] Sparse file first write in local range, not using journal: fileID=%d, sparseSize=%d, offset=%d, length=%d",
					ra.fileID, sparseSize, offset, length)
				return false
			}
		}

		// CRITICAL: For sparse files with EXISTING data (fileObj.DataID > 0), check if within local range
		// If writes are within a few chunks, use buffer path instead of journal
		if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
			// Check if writes are within local sequential range
			if writeRangeStart > 0 && writeRangeEnd > 0 {
				newRangeStart := writeRangeStart
				newRangeEnd := writeRangeEnd
				if offset < writeRangeStart {
					newRangeStart = offset
				}
				if currentWriteEnd > writeRangeEnd {
					newRangeEnd = currentWriteEnd
				}
				newRangeSize := newRangeEnd - newRangeStart
				if newRangeSize <= localSeqRange {
					// Writes are within local range, use buffer path (more efficient for small ranges)
					DebugLog("[VFS shouldUseJournal] Sparse file with existing data but within local sequential range (%d chunks), using buffer: fileID=%d, sparseSize=%d, rangeSize=%d, dataID=%d",
						LocalSequentialChunkCount, ra.fileID, sparseSize, newRangeSize, fileObj.DataID)
					return false
				}
			}

			// Writes span too many chunks, use journal to avoid rewriting entire file
			DebugLog("[VFS shouldUseJournal] Using journal for sparse file with existing data (avoid full file rewrite): fileID=%d, sparseSize=%d, dataID=%d",
				ra.fileID, sparseSize, fileObj.DataID)
			return true
		}

		// Check if this is random write pattern on sparse file
		// For true random writes (like qBittorrent), use journal
		if ra.isRandomWritePattern() {
			DebugLog("[VFS shouldUseJournal] Using journal for sparse file with random writes: fileID=%d, sparseSize=%d",
				ra.fileID, sparseSize)
			return true
		}

		// Sequential pattern on NEW sparse file (no existing data) - use sequential buffer
		DebugLog("[VFS shouldUseJournal] Sparse file appears sequential (new file), not using journal: fileID=%d, sparseSize=%d, lastOffset=%d, dataID=%d",
			ra.fileID, sparseSize, lastOffset, fileObj.DataID)
		return false
	}

	// PRIORITY CHECK 2: Files that need encryption or compression
	// But only for existing files with data (not after truncate(0))
	// For new files or after truncate, sequential buffer handles encryption/compression efficiently
	needsEncrypt := ra.fs.EndecWay > 0 && ra.fs.EndecKey != ""
	needsCompress := ra.fs.CmprWay > 0 && core.ShouldCompressFileByName(fileObj.Name)
	if (needsEncrypt || needsCompress) && fileObj.DataID != 0 && fileObj.DataID != core.EmptyDataID && fileObj.Size > 0 {
		DebugLog("[VFS shouldUseJournal] Using journal for encryption/compression on existing file: fileID=%d, needsEncrypt=%v, needsCompress=%v",
			ra.fileID, needsEncrypt, needsCompress)
		return true
	}

	// Check if sequential write buffer is active
	// Don't use journal for sequential writes (they have their own optimization)
	if ra.seqBuffer != nil {
		ra.seqBuffer.mu.Lock()
		hasSeqData := ra.seqBuffer.hasData && !ra.seqBuffer.closed
		ra.seqBuffer.mu.Unlock()
		if hasSeqData {
			return false
		}
	}

	// STRATEGY 1: Use journal for files with existing data (modifications)
	// This is the primary use case for journal.
	// NOTE: Only checking DataID here is enough to identify "existing file with data".
	// Cached fileObj.Size may be stale (e.g. after Flush), so don't require Size > 0.
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		DebugLog("[VFS shouldUseJournal] Using journal for existing file modification: fileID=%d, size=%d, dataID=%d",
			ra.fileID, fileObj.Size, fileObj.DataID)
		return true
	}

	// STRATEGY 2: Use journal for random write pattern
	// If we detect multiple non-sequential writes, switch to journal
	if ra.isRandomWritePattern() {
		DebugLog("[VFS shouldUseJournal] Using journal for random write pattern: fileID=%d", ra.fileID)
		return true
	}

	// STRATEGY 3: Use journal for large files (above threshold)
	// Check current file size OR buffered data size (replaces TempWriteArea)
	currentSize := fileObj.Size

	// Also consider data in write buffer
	if ra.buffer != nil {
		writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
		if writeIndex > 0 {
			// Calculate total size including buffered writes
			maxOffset := currentSize
			for i := int64(0); i < writeIndex && i < int64(len(ra.buffer.operations)); i++ {
				op := ra.buffer.operations[i]
				if len(op.Data) > 0 {
					endOffset := op.Offset + int64(len(op.Data))
					if endOffset > maxOffset {
						maxOffset = endOffset
					}
				}
			}
			if maxOffset > currentSize {
				currentSize = maxOffset
			}
		}
	}

	// Use journal for large files (default threshold: 10MB from journal config)
	if currentSize >= ra.fs.journalMgr.config.SmallFileThreshold {
		DebugLog("[VFS shouldUseJournal] Using journal for large file: fileID=%d, size=%d, threshold=%d",
			ra.fileID, currentSize, ra.fs.journalMgr.config.SmallFileThreshold)
		return true
	}

	return false
}

// getSparseSize returns the sparse size if set (helper function)
func (ra *RandomAccessor) getSparseSize() int64 {
	return atomic.LoadInt64(&ra.sparseSize)
}

// migrateWriteBufferToJournal migrates existing WriteBuffer entries to Journal
// This is called when switching from WriteBuffer to Journal mode
func (ra *RandomAccessor) migrateWriteBufferToJournal() error {
	writeIndex := ra.buffer.writeIndex
	if writeIndex <= 0 {
		return nil // Nothing to migrate
	}

	DebugLog("[VFS migrateWriteBufferToJournal] Migrating %d WriteBuffer entries to Journal: fileID=%d", writeIndex, ra.fileID)

	journal, err := ra.getOrCreateJournal()
	if err != nil {
		return fmt.Errorf("failed to get or create journal: %w", err)
	}

	// Copy all WriteBuffer entries to Journal
	for i := int64(0); i < writeIndex; i++ {
		op := ra.buffer.operations[i]
		if len(op.Data) > 0 {
			if err := journal.Write(op.Offset, op.Data); err != nil {
				return fmt.Errorf("failed to migrate write entry %d: %w", i, err)
			}
		}
	}

	// Clear WriteBuffer after successful migration
	ra.buffer.writeIndex = 0
	ra.buffer.totalSize = 0

	DebugLog("[VFS migrateWriteBufferToJournal] Successfully migrated %d entries to Journal: fileID=%d", writeIndex, ra.fileID)

	return nil
}

// getOrCreateJournal gets or creates a journal for this RandomAccessor
func (ra *RandomAccessor) getOrCreateJournal() (*Journal, error) {
	ra.journalMu.RLock()
	if ra.journal != nil {
		// Update sparse properties if sparseSize was set after journal creation
		sparseSize := ra.getSparseSize()
		if sparseSize > 0 && (!ra.journal.isSparse || ra.journal.virtualSize != sparseSize) {
			ra.journalMu.RUnlock()
			ra.journalMu.Lock()
			// Double-check after acquiring write lock
			if ra.journal != nil {
				ra.journal.virtualSize = sparseSize
				ra.journal.isSparse = true
				DebugLog("[VFS getOrCreateJournal] Updated existing journal sparse properties: fileID=%d, virtualSize=%d", ra.fileID, sparseSize)
			}
			ra.journalMu.Unlock()
			ra.journalMu.RLock()
		}
		journal := ra.journal
		ra.journalMu.RUnlock()
		if journal != nil {
			return journal, nil
		}
		// Journal was cleared, continue to create new one
	} else {
		ra.journalMu.RUnlock()
	}

	ra.journalMu.Lock()
	defer ra.journalMu.Unlock()

	// Double-check after acquiring write lock
	if ra.journal != nil {
		return ra.journal, nil
	}

	// Get file object
	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// Check if this is a sparse file
	sparseSize := ra.getSparseSize()
	baseSize := fileObj.Size
	if sparseSize > 0 {
		// Use sparse size as the base size
		baseSize = sparseSize
		DebugLog("[VFS getOrCreateJournal] Sparse file detected: fileID=%d, sparseSize=%d, actualSize=%d",
			ra.fileID, sparseSize, fileObj.Size)
	}

	// Get or create journal from manager
	journal := ra.fs.journalMgr.GetOrCreate(ra.fileID, fileObj.DataID, baseSize)

	// Set sparse properties if applicable
	if sparseSize > 0 {
		journal.virtualSize = sparseSize
		journal.isSparse = true
	}

	ra.journal = journal

	DebugLog("[VFS getOrCreateJournal] Created/retrieved journal: fileID=%d, dataID=%d, baseSize=%d, isSparse=%v",
		ra.fileID, fileObj.DataID, baseSize, journal.isSparse)

	return journal, nil
}

// writeToJournal writes data to the journal
func (ra *RandomAccessor) writeToJournal(offset int64, data []byte) error {
	journal, err := ra.getOrCreateJournal()
	if err != nil {
		return fmt.Errorf("failed to get or create journal: %w", err)
	}

	return journal.Write(offset, data)
}

// readFromJournal reads data from the journal, applying journal entries on top of base data
func (ra *RandomAccessor) readFromJournal(offset, length int64) ([]byte, error) {
	ra.journalMu.RLock()
	journal := ra.journal
	ra.journalMu.RUnlock()

	if journal == nil {
		// No journal, read from base data only
		return ra.readBaseData(offset, length)
	}

	// Create base reader function
	baseReader := func(off, len int64) ([]byte, error) {
		return ra.readBaseData(off, len)
	}

	// Read with journal overlay
	return journal.Read(offset, length, baseReader)
}

// readBaseData reads data from the base DataID, applying all journal snapshots recursively
// This function handles the case where there are multiple journal snapshots that need to be
// loaded and applied in the correct order (base -> layer1 -> layer2 -> ...)
// CRITICAL: When reading during Journal flush, use Journal's baseDataID instead of fileObj.DataID
// to avoid reading from the new dataID that hasn't been fully written yet
func (ra *RandomAccessor) readBaseData(offset, length int64) ([]byte, error) {
	// CRITICAL FIX: Check if Journal exists and use its baseDataID
	// This prevents reading from a new dataID during flush that hasn't been fully written
	ra.journalMu.RLock()
	journal := ra.journal
	var baseDataID int64 = 0
	var baseSize int64 = 0
	if journal != nil {
		// Use Journal's baseDataID instead of fileObj.DataID
		// This ensures we read from the correct base data during flush
		baseDataID = journal.dataID
		baseSize = journal.baseSize
	}
	ra.journalMu.RUnlock()

	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// Use Journal's baseDataID if available, otherwise use fileObj.DataID
	dataIDToRead := baseDataID
	if dataIDToRead == 0 || dataIDToRead == core.EmptyDataID {
		dataIDToRead = fileObj.DataID
	}

	if dataIDToRead == 0 || dataIDToRead == core.EmptyDataID {
		// No data, return zeros
		// CRITICAL: For sparse files, Journal.Read has already adjusted the length
		// based on currentSize (which is set to sparseSize). So we should return
		// the requested length (zero-filled), and let Journal.Read handle size limits.
		// This ensures that sparse files correctly return zeros for unwritten regions.
		if length <= 0 {
			return []byte{}, nil
		}
		// Return zero-filled buffer of requested length
		// Journal.Read will handle the actual size adjustment based on currentSize
		return make([]byte, length), nil
	}

	// Get DataInfo
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		return nil, fmt.Errorf("handler is not LocalHandler")
	}

	dataInfo, err := lh.GetDataInfo(ra.fs.c, ra.fs.bktID, dataIDToRead)
	if err != nil {
		// If reading from baseDataID fails and we have a journal, log a warning
		// but don't fail - this might be a transient issue during flush
		if journal != nil {
			DebugLog("[VFS readBaseData] WARNING: Failed to get DataInfo for baseDataID=%d, trying fileObj.DataID=%d: %v", baseDataID, fileObj.DataID, err)
			// Fallback to fileObj.DataID if baseDataID read fails
			if fileObj.DataID != 0 && fileObj.DataID != core.EmptyDataID && fileObj.DataID != baseDataID {
				dataInfo, err = lh.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
				if err == nil {
					dataIDToRead = fileObj.DataID
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get data info: %w", err)
		}
	}

	// Calculate chunk size
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Adjust length if needed
	// CRITICAL: For sparse files, don't limit length based on fileObj.Size
	// Journal.Read will handle size limits based on currentSize (which is sparseSize)
	// This ensures sparse files correctly return zeros for unwritten regions
	sparseSize := atomic.LoadInt64(&ra.sparseSize)
	if sparseSize > 0 {
		// For sparse files, use sparseSize as the effective size limit
		// But don't limit here - let Journal.Read handle it
		// This ensures we return zeros for unwritten regions
	} else {
		// For non-sparse files, limit based on baseSize or fileObj.Size
		// Use baseSize if available (from Journal), otherwise use fileObj.Size
		sizeLimit := fileObj.Size
		if baseSize > 0 {
			sizeLimit = baseSize
		}
		if offset+length > sizeLimit {
			length = sizeLimit - offset
		}
		if length <= 0 {
			return []byte{}, nil
		}
	}

	// Calculate which chunks we need to read
	startChunk := int(offset / chunkSize)
	endChunk := int((offset + length - 1) / chunkSize)

	// Read chunks and assemble result
	// CRITICAL: GetData returns encrypted/compressed raw data, need to decrypt/decompress
	// CRITICAL: Use dataIDToRead instead of fileObj.DataID to read from correct base data
	// CRITICAL: If baseVersion doesn't have all chunks (e.g., only chunk 0), but journal has writes
	// beyond base data range, we should return zeros for missing chunks instead of error
	// This allows journal to fill in the data for those regions
	result := make([]byte, 0, length)
	for sn := startChunk; sn <= endChunk; sn++ {
		rawChunk, err := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, dataIDToRead, sn)
		if err != nil {
			// If chunk doesn't exist, check if it's beyond base data size
			// For sparse files, always fill with zeros
			// For non-sparse files, if chunk is beyond baseSize, fill with zeros (journal will provide data)
			chunkStart := int64(sn) * chunkSize
			isBeyondBaseSize := baseSize > 0 && chunkStart >= baseSize
			if dataInfo.Kind&core.DATA_SPARSE != 0 || isBeyondBaseSize {
				// For sparse files or chunks beyond base size, return zero-filled plain data (not encrypted)
				chunkData := make([]byte, chunkSize)
				readStart := offset
				if readStart < chunkStart {
					readStart = chunkStart
				}
				readEnd := offset + length
				chunkEnd := chunkStart + chunkSize
				if readEnd > chunkEnd {
					readEnd = chunkEnd
				}
				if readStart < readEnd {
					startInChunk := readStart - chunkStart
					endInChunk := readEnd - chunkStart
					result = append(result, chunkData[startInChunk:endInChunk]...)
				}
				DebugLog("[VFS readBaseData] Chunk %d doesn't exist (sparse=%v, beyondBaseSize=%v), filling with zeros: fileID=%d, chunkStart=%d, baseSize=%d",
					sn, dataInfo.Kind&core.DATA_SPARSE != 0, isBeyondBaseSize, ra.fileID, chunkStart, baseSize)
				continue
			} else {
				// For non-sparse files and chunk is within base size, this is an error
				return nil, fmt.Errorf("failed to read chunk %d: %w", sn, err)
			}
		}

		// CRITICAL: Decrypt and decompress the raw chunk data
		// Use UnprocessData to handle encryption/compression (same as chunkReader.getChunk)
		chunkData, err := core.UnprocessData(rawChunk, dataInfo.Kind, ra.fs.EndecKey)
		if err != nil {
			return nil, fmt.Errorf("failed to unprocess chunk %d: %w", sn, err)
		}

		// Calculate the range within this chunk that we need
		chunkStart := int64(sn) * chunkSize
		chunkEnd := chunkStart + int64(len(chunkData))

		// Calculate the overlap with our requested range
		readStart := offset
		if readStart < chunkStart {
			readStart = chunkStart
		}

		readEnd := offset + length
		if readEnd > chunkEnd {
			readEnd = chunkEnd
		}

		if readStart < readEnd {
			// Extract the relevant portion from this chunk
			startInChunk := readStart - chunkStart
			endInChunk := readEnd - chunkStart
			result = append(result, chunkData[startInChunk:endInChunk]...)
		}
	}

	// CRITICAL FIX: Load and apply all journal snapshots recursively
	// Journal snapshots are stored as OBJ_TYPE_JOURNAL objects with baseVersionID chain
	// We need to find all snapshots and apply them in order
	appliedData, err := ra.applyJournalSnapshots(result, fileObj.DataID, offset, length)
	if err != nil {
		DebugLog("[VFS readBaseData] WARNING: Failed to apply journal snapshots: %v, using base data only", err)
		return result, nil // Return base data if snapshot loading fails
	}

	return appliedData, nil
}

// applyJournalSnapshots loads and applies all journal snapshots recursively
// baseData: the base data read from fileObj.DataID
// baseDataID: the DataID that journal snapshots are based on
// offset, length: the read range
func (ra *RandomAccessor) applyJournalSnapshots(baseData []byte, baseDataID int64, offset, length int64) ([]byte, error) {
	// List all child objects (versions and journal snapshots) under this file
	// Journal snapshots may be stored as OBJ_TYPE_JOURNAL with PID = fileID or PID = versionID
	// We need to recursively find all journal snapshots
	// Use Count: 1000 to actually retrieve objects (Count: 0 only returns count, not objects)
	objs, _, _, err := ra.fs.h.List(ra.fs.c, ra.fs.bktID, ra.fileID, core.ListOptions{Count: 1000})
	if err != nil {
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}

	DebugLog("[VFS applyJournalSnapshots] Listed %d objects under fileID=%d, baseDataID=%d", len(objs), ra.fileID, baseDataID)

	// Find all journal snapshots (OBJ_TYPE_JOURNAL) recursively
	// Journal snapshots may be:
	// - Directly under file (PID = fileID) - for journals based on base data
	// - Under version objects (PID = versionID) - for journals based on versions
	// - Type = OBJ_TYPE_JOURNAL
	// - DataID = base DataID they're based on
	// - Extra contains journalDataID and baseVersionID
	allJournals := make(map[int64]*core.ObjectInfo) // journal versionID -> journal snapshot
	versionIDs := make(map[int64]bool)              // Track version IDs to recursively search

	// First pass: find journals directly under file and collect version IDs
	for _, obj := range objs {
		DebugLog("[VFS applyJournalSnapshots] Object: ID=%d, PID=%d, Type=%d, DataID=%d, Name=%s",
			obj.ID, obj.PID, obj.Type, obj.DataID, obj.Name)
		if obj.Type == core.OBJ_TYPE_JOURNAL {
			allJournals[obj.ID] = obj
			DebugLog("[VFS applyJournalSnapshots] Found journal snapshot: ID=%d, PID=%d, DataID=%d, baseDataID=%d, Extra=%s",
				obj.ID, obj.PID, obj.DataID, baseDataID, obj.Extra)
		} else if obj.Type == core.OBJ_TYPE_VERSION {
			versionIDs[obj.ID] = true
		}
	}

	// Second pass: recursively search for journals under version objects
	for versionID := range versionIDs {
		versionObjs, _, _, err := ra.fs.h.List(ra.fs.c, ra.fs.bktID, versionID, core.ListOptions{Count: 1000})
		if err != nil {
			DebugLog("[VFS applyJournalSnapshots] WARNING: Failed to list objects under version %d: %v", versionID, err)
			continue
		}
		for _, obj := range versionObjs {
			if obj.Type == core.OBJ_TYPE_JOURNAL {
				allJournals[obj.ID] = obj
				DebugLog("[VFS applyJournalSnapshots] Found journal snapshot under version: ID=%d, PID=%d, DataID=%d, Extra=%s",
					obj.ID, obj.PID, obj.DataID, obj.Extra)
			}
		}
	}

	if len(allJournals) == 0 {
		// No journal snapshots, return base data as-is
		DebugLog("[VFS applyJournalSnapshots] No journal snapshots found for fileID=%d, baseDataID=%d", ra.fileID, baseDataID)
		return baseData, nil
	}

	// Build journal snapshot chain: find all journals that form a chain from baseDataID
	// Chain logic:
	// 1. Journals with DataID == baseDataID are directly based on base data
	// 2. Journals with baseVersionID pointing to another journal form a chain
	// 3. We need to apply them in order: base -> layer1 -> layer2 -> ...

	journalSnapshots := make([]*core.ObjectInfo, 0)
	processed := make(map[int64]bool)

	// Step 1: Find journals directly based on baseDataID
	for _, journal := range allJournals {
		if journal.DataID == baseDataID {
			baseVersionID := ra.parseBaseVersionID(journal.Extra)
			// If baseVersionID is 0 or matches baseDataID, this is a direct child of base
			if baseVersionID == 0 || baseVersionID == baseDataID {
				journalSnapshots = append(journalSnapshots, journal)
				processed[journal.ID] = true
			}
		}
	}

	// Step 2: Build chain by following baseVersionID references
	// Continue until we've processed all journals in the chain
	for {
		foundNew := false
		for _, journal := range allJournals {
			if processed[journal.ID] {
				continue
			}

			baseVersionID := ra.parseBaseVersionID(journal.Extra)
			if baseVersionID == 0 {
				continue
			}

			// Check if this journal is based on an already processed journal
			if processed[baseVersionID] {
				journalSnapshots = append(journalSnapshots, journal)
				processed[journal.ID] = true
				foundNew = true
			}
		}

		if !foundNew {
			break
		}
	}

	if len(journalSnapshots) == 0 {
		// No valid journal snapshots in chain, return base data as-is
		DebugLog("[VFS applyJournalSnapshots] No journal snapshots in chain for fileID=%d, baseDataID=%d", ra.fileID, baseDataID)
		return baseData, nil
	}

	// Sort snapshots by MTime (oldest first) to apply in correct order
	// Older snapshots should be applied first
	sort.Slice(journalSnapshots, func(i, j int) bool {
		return journalSnapshots[i].MTime < journalSnapshots[j].MTime
	})

	DebugLog("[VFS applyJournalSnapshots] Found %d journal snapshots for fileID=%d, baseDataID=%d",
		len(journalSnapshots), ra.fileID, baseDataID)

	// Start with base data
	result := make([]byte, len(baseData))
	copy(result, baseData)

	// Apply each journal snapshot in order
	for _, journalSnapshot := range journalSnapshots {
		// Parse journal snapshot extra data to get journalDataID (supports both JSON and binary formats)
		journalDataID, err := GetJournalDataID(journalSnapshot.Extra)
		if err != nil {
			DebugLog("[VFS applyJournalSnapshots] WARNING: Failed to parse journal extra for snapshot %d: %v", journalSnapshot.ID, err)
			continue
		}
		if journalDataID == 0 {
			DebugLog("[VFS applyJournalSnapshots] WARNING: journalDataID not found in snapshot %d", journalSnapshot.ID)
			continue
		}

		// Read journal data
		journalBytes, err := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, journalDataID, 0)
		if err != nil {
			DebugLog("[VFS applyJournalSnapshots] WARNING: Failed to read journal data %d for snapshot %d: %v",
				int64(journalDataID), journalSnapshot.ID, err)
			continue
		}

		// Create a temporary journal to deserialize
		tempJournal := &Journal{
			fileID:   ra.fileID,
			dataID:   baseDataID,
			baseSize: journalSnapshot.Size,
			entries:  make([]JournalEntry, 0),
		}

		// Deserialize journal entries
		if err := tempJournal.DeserializeJournal(journalBytes); err != nil {
			DebugLog("[VFS applyJournalSnapshots] WARNING: Failed to deserialize journal for snapshot %d: %v", journalSnapshot.ID, err)
			continue
		}

		DebugLog("[VFS applyJournalSnapshots] Applying journal snapshot %d: entries=%d, size=%d",
			journalSnapshot.ID, len(tempJournal.entries), journalSnapshot.Size)

		// Apply journal entries to result data
		readEnd := offset + length
		for i := range tempJournal.entries {
			entry := &tempJournal.entries[i]
			entryEnd := entry.Offset + entry.Length

			// Skip entries that don't overlap with read range
			if entry.Offset >= readEnd || entryEnd <= offset {
				continue
			}

			// Calculate overlap region (in absolute file coordinates)
			overlapStart := entry.Offset
			if overlapStart < offset {
				overlapStart = offset
			}

			overlapEnd := entryEnd
			if overlapEnd > readEnd {
				overlapEnd = readEnd
			}

			// Calculate positions in the result buffer (relative to offset)
			srcOffset := overlapStart - entry.Offset // Position in entry.Data
			dstOffset := overlapStart - offset       // Position in result buffer
			copyLength := overlapEnd - overlapStart

			// Ensure we don't write beyond the result buffer
			if dstOffset+copyLength > int64(len(result)) {
				DebugLog("[VFS applyJournalSnapshots] WARNING: Overlap extends beyond result buffer, truncating")
				copyLength = int64(len(result)) - dstOffset
			}

			if copyLength > 0 && dstOffset >= 0 && dstOffset < int64(len(result)) {
				copy(result[dstOffset:dstOffset+copyLength], entry.Data[srcOffset:srcOffset+copyLength])
			}
		}
	}

	return result, nil
}

// parseBaseVersionID parses baseVersionID from Extra JSON field
func (ra *RandomAccessor) parseBaseVersionID(extra string) int64 {
	if extra == "" {
		return 0
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(extra), &data); err != nil {
		return 0
	}

	if baseID, ok := data["baseVersionID"].(float64); ok {
		return int64(baseID)
	}

	return 0
}

// flushJournal flushes the journal to create a new version
// Returns (versionID, true) if journal was flushed, (0, false) if no journal or not dirty
func (ra *RandomAccessor) flushJournal() (int64, bool) {
	ra.journalMu.RLock()
	journal := ra.journal
	ra.journalMu.RUnlock()

	if journal == nil {
		DebugLog("[VFS flushJournal] No journal: fileID=%d", ra.fileID)
		return 0, false
	}
	if !journal.IsDirty() {
		DebugLog("[VFS flushJournal] Journal not dirty: fileID=%d", ra.fileID)
		return 0, false
	}
	DebugLog("[VFS flushJournal] Journal is dirty, proceeding to flush: fileID=%d, isSparse=%v, virtualSize=%d", ra.fileID, journal.isSparse, journal.virtualSize)

	// Note: Don't call journal.GetEntryCount() here as it acquires entriesMu.RLock()
	// which could block if another thread is in Flush() holding entriesMu.Lock()
	DebugLog("[VFS flushJournal] Flushing journal: fileID=%d", ra.fileID)

	// Get virtualSize before Flush (for sparse files, journal may be cleared after Flush)
	virtualSize := int64(0)
	isSparse := false
	DebugLog("[VFS flushJournal] Checking journal sparse properties: fileID=%d, journal.isSparse=%v, journal.virtualSize=%d", ra.fileID, journal.isSparse, journal.virtualSize)
	if journal.isSparse && journal.virtualSize > 0 {
		virtualSize = journal.virtualSize
		isSparse = true
		DebugLog("[VFS flushJournal] Sparse file detected: fileID=%d, virtualSize=%d", ra.fileID, virtualSize)
	} else {
		// Also check sparseSize from RandomAccessor as fallback
		sparseSize := ra.getSparseSize()
		if sparseSize > 0 {
			virtualSize = sparseSize
			isSparse = true
			DebugLog("[VFS flushJournal] Sparse file detected from RandomAccessor: fileID=%d, sparseSize=%d (journal.isSparse=%v, journal.virtualSize=%d)", ra.fileID, virtualSize, journal.isSparse, journal.virtualSize)
		}
	}

	// Flush journal to create new data block
	newDataID, newSize, err := journal.Flush()
	if err != nil {
		DebugLog("[VFS flushJournal] ERROR: Failed to flush journal: %v", err)
		return 0, false
	}

	// Get file object
	fileObj, err := ra.getFileObj()
	if err != nil {
		DebugLog("[VFS flushJournal] ERROR: Failed to get file object: %v", err)
		return 0, false
	}

	// Check for atomic replace scenario: if there's a pending deletion for this file,
	// it means there's an atomic replace operation in progress. In this case, we should
	// cancel the deletion and merge versions to ensure file integrity.
	// This is important for sparse files with journal writes that may have non-sequential writes.
	if ra.fs.atomicReplaceMgr != nil {
		if pd, canceled := ra.fs.atomicReplaceMgr.CheckAndCancelDeletion(ra.fs.bktID, fileObj.PID, fileObj.Name); canceled {
			DebugLog("[VFS flushJournal] Detected atomic replace during flush: fileID=%d, oldFileID=%d, versions=%d",
				ra.fileID, pd.FileID, len(pd.Versions))

			// Merge versions from old file to current file if needed
			if len(pd.Versions) > 0 {
				// Get all version objects
				versions, err := ra.fs.h.Get(ra.fs.c, ra.fs.bktID, pd.Versions)
				if err != nil {
					DebugLog("[VFS flushJournal] WARNING: Failed to get versions during atomic replace: %v", err)
				} else {
					// Change PID from oldFileID to newFileID
					for _, v := range versions {
						v.PID = ra.fileID
					}

					// Update versions
					_, err = ra.fs.h.Put(ra.fs.c, ra.fs.bktID, versions)
					if err != nil {
						DebugLog("[VFS flushJournal] WARNING: Failed to merge versions during atomic replace: %v", err)
						// Continue anyway - flush should proceed
					} else {
						DebugLog("[VFS flushJournal] Merged %d versions from oldFileID=%d to fileID=%d",
							len(versions), pd.FileID, ra.fileID)
					}
				}
			}

			// Delete old file object if it's different from current file
			if pd.FileID != ra.fileID {
				if err := ra.fs.h.Delete(ra.fs.c, ra.fs.bktID, pd.FileID); err != nil {
					DebugLog("[VFS flushJournal] WARNING: Failed to delete old file object: oldFileID=%d, error=%v", pd.FileID, err)
					// Non-fatal - old file object can be cleaned up later
				}
			}
		}
	}

	// Create new version
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		DebugLog("[VFS flushJournal] ERROR: Handler is not LocalHandler")
		return 0, false
	}

	versionID := core.NewID()
	if versionID == 0 {
		DebugLog("[VFS flushJournal] ERROR: Failed to generate version ID")
		return 0, false
	}

	mTime := core.Now()

	// For sparse files, use virtualSize instead of newSize for file object size
	// newSize is the actual data size, but file object size should be virtualSize
	fileSize := newSize
	if isSparse && virtualSize > 0 {
		fileSize = virtualSize
		DebugLog("[VFS flushJournal] Using virtualSize for sparse file: fileID=%d, virtualSize=%d, newSize=%d", ra.fileID, fileSize, newSize)
	}

	newVersion := &core.ObjectInfo{
		ID:     versionID,
		PID:    ra.fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// Update file object
	updateFileObj := &core.ObjectInfo{
		ID:     ra.fileID,
		PID:    fileObj.PID,
		Type:   fileObj.Type,
		Name:   fileObj.Name,
		DataID: newDataID,
		Size:   fileSize,
		MTime:  mTime,
	}
	DebugLog("[VFS flushJournal] 🔍 Prepared updateFileObj: fileID=%d, DataID=%d, Size=%d, isSparse=%v, virtualSize=%d, newSize=%d", ra.fileID, updateFileObj.DataID, updateFileObj.Size, isSparse, virtualSize, newSize)

	// Create version object first
	_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{newVersion})
	if err != nil {
		DebugLog("[VFS flushJournal] ❌ ERROR: Failed to create version: %v", err)
		return 0, false
	}

	// Update file object using SetObj to ensure Size field is updated
	// Put uses InsertIgnore which won't update existing objects
	DebugLog("[VFS flushJournal] 🔍 Calling SetObj to update file object: fileID=%d, DataID=%d, Size=%d, MTime=%d, isSparse=%v, virtualSize=%d", ra.fileID, updateFileObj.DataID, updateFileObj.Size, updateFileObj.MTime, isSparse, virtualSize)
	err = lh.MetadataAdapter().SetObj(ra.fs.c, ra.fs.bktID, []string{"did", "s", "m"}, updateFileObj)
	if err != nil {
		DebugLog("[VFS flushJournal] ERROR: Failed to update file object: %v", err)
		return 0, false
	}
	DebugLog("[VFS flushJournal] Updated file object using SetObj: fileID=%d, DataID=%d, Size=%d", ra.fileID, updateFileObj.DataID, updateFileObj.Size)

	// Update cache
	fileObjCache.Put(ra.fileObjKey, updateFileObj)
	ra.fileObj.Store(updateFileObj)

	// Register bucket for WAL checkpoint
	if ra.fs.walCheckpointManager != nil {
		ra.fs.walCheckpointManager.RegisterBucket(ra.fs.bktID)
	}

	DebugLog("[VFS flushJournal] Successfully flushed journal: fileID=%d, newDataID=%d, newSize=%d, versionID=%d",
		ra.fileID, newDataID, newSize, versionID)

	return versionID, true
}

// closeJournal closes the journal and removes it from the manager
func (ra *RandomAccessor) closeJournal() error {
	ra.journalMu.Lock()
	journal := ra.journal
	ra.journal = nil
	ra.journalMu.Unlock()

	if journal == nil {
		return nil
	}

	// If journal has uncommitted changes, flush them
	if journal.IsDirty() {
		DebugLog("[VFS closeJournal] Journal has uncommitted changes, flushing: fileID=%d", ra.fileID)
		versionID, flushed := ra.flushJournal()
		if !flushed {
			DebugLog("[VFS closeJournal] WARNING: Failed to flush journal")
			return fmt.Errorf("failed to flush journal")
		}
		DebugLog("[VFS closeJournal] Successfully flushed journal before close: fileID=%d, versionID=%d", ra.fileID, versionID)
	}

	// Remove journal from manager
	ra.fs.journalMgr.Remove(ra.fileID)

	DebugLog("[VFS closeJournal] Closed journal: fileID=%d", ra.fileID)

	return nil
}

// truncateJournal truncates the journal to the specified size
func (ra *RandomAccessor) truncateJournal(size int64) error {
	ra.journalMu.RLock()
	journal := ra.journal
	ra.journalMu.RUnlock()

	if journal == nil {
		return nil
	}

	return journal.Truncate(size)
}

// updateWriteRange updates the write range for sparse files
// This tracks the range of writes to detect if they're within a few chunks (local sequential)
func (ra *RandomAccessor) updateWriteRange(offset, length int64) {
	writeEnd := offset + length
	writeRangeStart := atomic.LoadInt64(&ra.writeRangeStart)
	writeRangeEnd := atomic.LoadInt64(&ra.writeRangeEnd)

	if writeRangeStart == 0 || writeRangeEnd == 0 {
		// First write, initialize range
		atomic.StoreInt64(&ra.writeRangeStart, offset)
		atomic.StoreInt64(&ra.writeRangeEnd, writeEnd)
		DebugLog("[VFS updateWriteRange] Initialized write range: fileID=%d, start=%d, end=%d", ra.fileID, offset, writeEnd)
	} else {
		// Update range to include new write
		newStart := writeRangeStart
		newEnd := writeRangeEnd
		if offset < writeRangeStart {
			newStart = offset
		}
		if writeEnd > writeRangeEnd {
			newEnd = writeEnd
		}
		atomic.StoreInt64(&ra.writeRangeStart, newStart)
		atomic.StoreInt64(&ra.writeRangeEnd, newEnd)
		DebugLog("[VFS updateWriteRange] Updated write range: fileID=%d, start=%d, end=%d, rangeSize=%d", ra.fileID, newStart, newEnd, newEnd-newStart)
	}
}

// getJournalSize returns the current size including journal modifications
func (ra *RandomAccessor) getJournalSize() int64 {
	ra.journalMu.RLock()
	journal := ra.journal
	ra.journalMu.RUnlock()

	if journal == nil {
		return -1 // No journal
	}

	return journal.GetSize()
}

// migrateSeqBufferToJournal migrates pending seqBuffer data to Journal
func (ra *RandomAccessor) migrateSeqBufferToJournal() error {
	ra.seqBuffer.mu.Lock()
	defer ra.seqBuffer.mu.Unlock()

	// Always mark as closed since we are moving data to journal
	ra.seqBuffer.closed = true

	if !ra.seqBuffer.hasData || len(ra.seqBuffer.buffer) == 0 {
		return nil
	}

	// Calculate data range
	// seqBuffer contains chunks. sn determines start offset.
	chunkSize := ra.seqBuffer.chunkSize
	startOffset := int64(ra.seqBuffer.sn) * chunkSize
	bufferData := ra.seqBuffer.buffer

	DebugLog("[VFS migrateSeqBufferToJournal] Migrating seqBuffer to Journal: fileID=%d, offset=%d, size=%d", ra.fileID, startOffset, len(bufferData))

	journal, err := ra.getOrCreateJournal()
	if err != nil {
		return fmt.Errorf("failed to get or create journal: %w", err)
	}

	if err := journal.Write(startOffset, bufferData); err != nil {
		return fmt.Errorf("failed to write seqBuffer data to journal: %w", err)
	}

	// Clear seqBuffer
	ra.seqBuffer.hasData = false
	ra.clearSeqBuffer() // Clear buffer and shrink capacity if needed

	return nil
}
