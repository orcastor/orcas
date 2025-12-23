package sdk

import (
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/zeebo/xxh3"
)

const (
	// HdrSize is the size of header data used for HdrXXH3 calculation (first 100KB)
	HdrSize = 102400
)

// CalculateChecksums calculates HdrXXH3, XXH3, and SHA-256 checksums from data
// Returns HdrXXH3, XXH3, SHA256_0, SHA256_1, SHA256_2, SHA256_3, and error
// This function is used for instant upload (deduplication) feature
func CalculateChecksums(data []byte) (int64, int64, int64, int64, int64, int64, error) {
	if len(data) == 0 {
		return 0, 0, 0, 0, 0, 0, nil
	}

	// Calculate HdrXXH3 (first 100KB or entire file if smaller)
	var hdrXXH3 int64
	if len(data) > HdrSize {
		hdrXXH3 = int64(xxh3.Hash(data[0:HdrSize]))
	} else {
		hdrXXH3 = int64(xxh3.Hash(data))
	}

	// Calculate XXH3 for entire file
	fullXXH3 := int64(xxh3.Hash(data))

	// Calculate SHA-256 for entire file
	sha256Hash := sha256.Sum256(data)
	sha256_0 := int64(binary.BigEndian.Uint64(sha256Hash[0:8]))
	sha256_1 := int64(binary.BigEndian.Uint64(sha256Hash[8:16]))
	sha256_2 := int64(binary.BigEndian.Uint64(sha256Hash[16:24]))
	sha256_3 := int64(binary.BigEndian.Uint64(sha256Hash[24:32]))

	return hdrXXH3, fullXXH3, sha256_0, sha256_1, sha256_2, sha256_3, nil
}

// CalculateChecksumsFromReader calculates checksums by reading from an io.Reader
// This is more memory-efficient for large files
// Returns HdrXXH3, XXH3, SHA256_0, SHA256_1, SHA256_2, SHA256_3, and error
func CalculateChecksumsFromReader(reader io.Reader, size int64) (uint64, uint64, int64, int64, int64, int64, error) {
	if size == 0 {
		return 0, 0, 0, 0, 0, 0, nil
	}

	// Read header for HdrXXH3
	headerBuf := make([]byte, HdrSize)
	headerRead := 0
	if size < HdrSize {
		headerBuf = make([]byte, size)
	}
	n, err := io.ReadFull(reader, headerBuf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, 0, 0, 0, 0, 0, err
	}
	headerRead = n

	// Calculate HdrXXH3
	var hdrXXH3 uint64
	if headerRead > 0 {
		hdrXXH3 = xxh3.Hash(headerBuf[:headerRead])
	}

	// Create XXH3 and SHA-256 hashers
	xxh3Hash := xxh3.New()
	sha256Hash := sha256.New()

	// Write header to hashers
	if headerRead > 0 {
		xxh3Hash.Write(headerBuf[:headerRead])
		sha256Hash.Write(headerBuf[:headerRead])
	}

	// Read remaining data and update hashers
	remaining := size - int64(headerRead)
	if remaining > 0 {
		buf := make([]byte, 64*1024) // 64KB buffer
		for remaining > 0 {
			toRead := int64(len(buf))
			if toRead > remaining {
				toRead = remaining
			}
			n, err := reader.Read(buf[:toRead])
			if n > 0 {
				xxh3Hash.Write(buf[:n])
				sha256Hash.Write(buf[:n])
				remaining -= int64(n)
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				return 0, 0, 0, 0, 0, 0, err
			}
		}
	}

	// Get final checksums
	fullXXH3 := xxh3Hash.Sum64()
	sha256Sum := sha256Hash.Sum(nil)
	sha256_0 := int64(binary.BigEndian.Uint64(sha256Sum[0:8]))
	sha256_1 := int64(binary.BigEndian.Uint64(sha256Sum[8:16]))
	sha256_2 := int64(binary.BigEndian.Uint64(sha256Sum[16:24]))
	sha256_3 := int64(binary.BigEndian.Uint64(sha256Sum[24:32]))

	return hdrXXH3, fullXXH3, sha256_0, sha256_1, sha256_2, sha256_3, nil
}
