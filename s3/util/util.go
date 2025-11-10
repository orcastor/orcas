package util

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
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
	error := S3Error{
		Code:    code,
		Message: message,
	}
	c.Header("Content-Type", "application/xml")
	c.XML(statusCode, error)
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
	var err error

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
// Format: bytes start-end/total
func FormatContentRangeHeader(start, end, total int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", start, end, total)
}
