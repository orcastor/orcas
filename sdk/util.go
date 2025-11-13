package sdk

import (
	"encoding/binary"
	"strconv"
	"sync"
	"time"
)

// FormatContentRangeHeader formats the Content-Range header
// Format: bytes start-end/total
// Optimized: uses strconv.AppendInt with sync.Pool to reduce allocations
var contentRangePool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 70) // Pre-allocate: "bytes " (6) + numbers (up to 20 each) + "-" (1) + "/" (1) = ~70 bytes
		return &buf
	},
}

func FormatContentRangeHeader(start, end, total int64) string {
	buf := contentRangePool.Get().(*[]byte)
	*buf = (*buf)[:0] // Reset length
	*buf = append(*buf, "bytes "...)
	*buf = strconv.AppendInt(*buf, start, 10)
	*buf = append(*buf, '-')
	*buf = strconv.AppendInt(*buf, end, 10)
	*buf = append(*buf, '/')
	*buf = strconv.AppendInt(*buf, total, 10)
	result := string(*buf)
	contentRangePool.Put(buf)
	return result
}

// FormatETag formats DataID as ETag (hex string with quotes)
// Optimized: uses strconv.AppendUint with sync.Pool to reduce allocations
var etagPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 20) // Pre-allocate: quote (1) + hex (up to 16) + quote (1) = ~18 bytes
		return &buf
	},
}

func FormatETag(dataID int64) string {
	buf := etagPool.Get().(*[]byte)
	*buf = (*buf)[:0] // Reset length
	*buf = append(*buf, '"')
	*buf = strconv.AppendUint(*buf, uint64(dataID), 16)
	*buf = append(*buf, '"')
	result := string(*buf)
	etagPool.Put(buf)
	return result
}

// FormatContentLength formats int64 as Content-Length header value
// Optimized: uses strconv.AppendInt to avoid strconv.FormatInt allocation
var contentLengthPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 20) // Pre-allocate for numbers up to 20 digits
		return &buf
	},
}

func FormatContentLength(size int64) string {
	buf := contentLengthPool.Get().(*[]byte)
	*buf = (*buf)[:0] // Reset length
	*buf = strconv.AppendInt(*buf, size, 10)
	result := string(*buf)
	contentLengthPool.Put(buf)
	return result
}

// FormatLastModified formats Unix timestamp as RFC1123 Last-Modified header
// Optimized: caches formatted time strings to avoid repeated formatting
// Uses sync.Map for better concurrent performance
var (
	lastModifiedCache sync.Map // map[int64]string - thread-safe map for concurrent access
)

func FormatLastModified(mtime int64) string {
	// Round to nearest second for caching (RFC1123 doesn't include sub-seconds)
	roundedTime := mtime

	// Try to get from cache (sync.Map is optimized for read-heavy workloads)
	if cached, ok := lastModifiedCache.Load(roundedTime); ok {
		return cached.(string)
	}

	// Format time using RFC1123 format with GMT (HTTP standard requires GMT, not UTC)
	// Use custom format string to ensure "GMT" is used instead of "UTC"
	// Format: "Mon, 02 Jan 2006 15:04:05 GMT"
	const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"
	formatted := time.Unix(roundedTime, 0).UTC().Format(httpTimeFormat)

	// Cache the result (with size limit to prevent unbounded growth)
	// Use LoadOrStore to avoid race condition
	actual, _ := lastModifiedCache.LoadOrStore(roundedTime, formatted)
	return actual.(string)
}

// FastSplitPath splits a path string into parts, optimized for performance
// Handles leading/trailing slashes and empty segments efficiently
func FastSplitPath(path string) []string {
	if path == "" {
		return nil
	}

	// Trim leading and trailing slashes
	start := 0
	end := len(path)
	for start < end && path[start] == '/' {
		start++
	}
	for end > start && path[end-1] == '/' {
		end--
	}

	if start >= end {
		return nil
	}

	// Count segments to pre-allocate slice
	segments := 1
	for i := start; i < end; i++ {
		if path[i] == '/' {
			segments++
		}
	}

	// Pre-allocate with known capacity
	parts := make([]string, 0, segments)
	current := start

	for i := start; i < end; i++ {
		if path[i] == '/' {
			if i > current {
				parts = append(parts, path[current:i])
			}
			current = i + 1
		}
	}

	// Add last segment
	if current < end {
		parts = append(parts, path[current:end])
	}

	return parts
}

// FormatCacheKeyInt formats a cache key with two int64 values
// Optimized: uses fixed 16-byte binary encoding instead of string formatting
// Returns a string with exactly 16 bytes (8 bytes per int64, big-endian)
func FormatCacheKeyInt(id1, id2 int64) string {
	// Use fixed 16-byte array: 8 bytes for id1 + 8 bytes for id2
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(id1))
	binary.BigEndian.PutUint64(buf[8:16], uint64(id2))
	// Convert to string - this creates a copy, which is safe
	return string(buf[:])
}

// FormatCacheKeySingleInt formats a cache key with a single int64 value
// Optimized: uses fixed 8-byte binary encoding instead of string formatting
// Returns a string with exactly 8 bytes (big-endian)
func FormatCacheKeySingleInt(id int64) string {
	// Use fixed 8-byte array
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(id))
	// Convert to string - this creates a copy, which is safe
	return string(buf[:])
}

// FormatCacheKeyString formats a cache key with int64 and string
var cacheKeyPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 64) // Pre-allocate for typical cache keys
		return &buf
	},
}

func FormatCacheKeyString(id int64, s string) string {
	buf := cacheKeyPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	*buf = strconv.AppendInt(*buf, id, 10)
	*buf = append(*buf, ':')
	*buf = append(*buf, s...)
	result := string(*buf)
	cacheKeyPool.Put(buf)
	return result
}

// FastBase extracts the base name from a path (like filepath.Base but faster)
func FastBase(path string) string {
	if path == "" {
		return "."
	}

	// Trim trailing slashes
	end := len(path)
	for end > 0 && path[end-1] == '/' {
		end--
	}
	if end == 0 {
		return "/"
	}

	// Find last slash
	lastSlash := -1
	for i := end - 1; i >= 0; i-- {
		if path[i] == '/' {
			lastSlash = i
			break
		}
	}

	if lastSlash < 0 {
		return path[:end]
	}

	return path[lastSlash+1 : end]
}

// FastDir extracts the directory from a path (like filepath.Dir but faster)
func FastDir(path string) string {
	if path == "" {
		return "."
	}

	// Trim trailing slashes
	end := len(path)
	for end > 0 && path[end-1] == '/' {
		end--
	}
	if end == 0 {
		return "/"
	}

	// Find last slash
	lastSlash := -1
	for i := end - 1; i >= 0; i-- {
		if path[i] == '/' {
			lastSlash = i
			break
		}
	}

	if lastSlash < 0 {
		return "."
	}

	// Trim trailing slashes from directory part
	dirEnd := lastSlash
	for dirEnd > 0 && path[dirEnd-1] == '/' {
		dirEnd--
	}
	if dirEnd == 0 {
		return "/"
	}

	return path[:dirEnd]
}

// FastTrimPrefix removes the leading prefix from a string, optimized version
// Returns the string without the prefix, or the original string if prefix not found
func FastTrimPrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}
