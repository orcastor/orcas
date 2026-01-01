// Package util provides S3-specific utility functions for S3 API handlers
package util

import (
	"encoding/xml"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/util"
)

// S3Error represents an S3 error response
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

// S3ErrorResponse sends an S3-compatible error response
func S3ErrorResponse(c *gin.Context, statusCode int, code, message string) {
	errResp := S3Error{
		Code:    code,
		Message: message,
	}
	c.Header("Content-Type", "application/xml")
	c.XML(statusCode, errResp)
}

// RangeSpec represents a parsed HTTP Range request
type RangeSpec struct {
	Start int64 // Start byte position (inclusive)
	End   int64 // End byte position (inclusive)
	Valid bool  // Whether the range is valid
}

// ParseRangeHeader parses the HTTP Range header
// Supports formats:
//   - bytes=start-end
//   - bytes=start- (from start to end of file)
//   - bytes=-suffix (last suffix bytes)
//
// Returns nil if Range header is not present or invalid
func ParseRangeHeader(rangeHeader string, fileSize int64) *RangeSpec {
	if rangeHeader == "" {
		return nil
	}

	// Range header format: "bytes=start-end" or "bytes=start-" or "bytes=-suffix"
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return nil
	}

	var start, end int64

	if parts[0] == "" {
		// Suffix range: bytes=-suffix
		if parts[1] == "" {
			return nil
		}
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return nil
		}
		start = fileSize - suffix
		if start < 0 {
			start = 0
		}
		end = fileSize - 1
	} else if parts[1] == "" {
		// Start to end: bytes=start-
		var err error
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil || start < 0 {
			return nil
		}
		if start >= fileSize {
			return nil // Range not satisfiable
		}
		end = fileSize - 1
	} else {
		// Start to end: bytes=start-end
		var err error
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil || start < 0 {
			return nil
		}
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < 0 {
			return nil
		}
		if start > end {
			return nil
		}
	}

	// Validate range against file size
	if start >= fileSize {
		return nil // Range not satisfiable
	}
	if end >= fileSize {
		end = fileSize - 1
	}

	return &RangeSpec{
		Start: start,
		End:   end,
		Valid: true,
	}
}

// FormatContentRangeHeader formats the Content-Range header
// Delegates to sdk.FormatContentRangeHeader for consistency
func FormatContentRangeHeader(start, end, total int64) string {
	return util.FormatContentRangeHeader(start, end, total)
}

// FormatETag formats DataID as ETag (hex string with quotes)
// Delegates to util.FormatETag for consistency
func FormatETag(dataID int64) string {
	return util.FormatETag(dataID)
}

// FormatContentLength formats int64 as Content-Length header value
// Delegates to util.FormatContentLength for consistency
func FormatContentLength(size int64) string {
	return util.FormatContentLength(size)
}

// FormatLastModified formats Unix timestamp as RFC1123 Last-Modified header
// Delegates to util.FormatLastModified for consistency
func FormatLastModified(mtime int64) string {
	return util.FormatLastModified(mtime)
}

// SetObjectHeaders sets common object response headers in batch
// Optimized: reduces multiple c.Header() calls overhead
func SetObjectHeaders(c *gin.Context, contentLength int64, etag int64, lastModified int64, acceptRanges string) {
	// Set headers in optimal order (most frequently used first)
	c.Header("Content-Length", util.FormatContentLength(contentLength))
	c.Header("ETag", util.FormatETag(etag))
	c.Header("Last-Modified", util.FormatLastModified(lastModified))
	if acceptRanges != "" {
		c.Header("Accept-Ranges", acceptRanges)
	}
}

// SetObjectHeadersWithContentType sets object headers including Content-Type
// Optimized: batch header setting for GetObject/HeadObject responses
func SetObjectHeadersWithContentType(c *gin.Context, contentType string, contentLength int64, etag int64, lastModified int64, acceptRanges string) {
	c.Header("Content-Type", contentType)
	c.Header("Content-Length", util.FormatContentLength(contentLength))
	c.Header("ETag", util.FormatETag(etag))
	c.Header("Last-Modified", util.FormatLastModified(lastModified))
	if acceptRanges != "" {
		c.Header("Accept-Ranges", acceptRanges)
	}
}

// FastSplitPath splits a path string into parts, optimized for performance
// Delegates to sdk.FastSplitPath for consistency
func FastSplitPath(path string) []string {
	return util.FastSplitPath(path)
}

// FastBase extracts the base name from a path (like filepath.Base but faster)
// Delegates to util.FastBase for consistency
func FastBase(path string) string {
	return util.FastBase(path)
}

// FastDir extracts the directory from a path (like filepath.Dir but faster)
// Delegates to util.FastDir for consistency
func FastDir(path string) string {
	return util.FastDir(path)
}

// FastTrimPrefix removes the leading prefix from a string, optimized version
// Delegates to util.FastTrimPrefix for consistency
func FastTrimPrefix(s, prefix string) string {
	return util.FastTrimPrefix(s, prefix)
}

func GetBatchWriterForBucket(handler core.Handler, bktID int64) *util.BatchWriter {
	return util.GetBatchWriterForBucket(handler, bktID)
}

func CalculateChecksums(data []byte) (int64, int64, int64, int64, int64, int64) {
	return core.CalculateChecksums(data)
}
