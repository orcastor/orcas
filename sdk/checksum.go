package sdk

import (
	"crypto/md5"
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	// HdrSize is the size of header data used for HdrCRC32 calculation (first 100KB)
	HdrSize = 102400
)

// CalculateChecksums calculates HdrCRC32, CRC32, and MD5 checksums from data
// Returns HdrCRC32, CRC32, MD5 (as int64), and error
// This function is used for instant upload (deduplication) feature
func CalculateChecksums(data []byte) (uint32, uint32, int64, error) {
	if len(data) == 0 {
		return 0, 0, 0, nil
	}

	// Calculate HdrCRC32 (first 100KB or entire file if smaller)
	var hdrCRC32 uint32
	if len(data) > HdrSize {
		hdrCRC32 = crc32.ChecksumIEEE(data[0:HdrSize])
	} else {
		hdrCRC32 = crc32.ChecksumIEEE(data)
	}

	// Calculate CRC32 for entire file
	crc32Hash := crc32.NewIEEE()
	if _, err := crc32Hash.Write(data); err != nil {
		return 0, 0, 0, err
	}
	fullCRC32 := crc32Hash.Sum32()

	// Calculate MD5 for entire file
	md5Hash := md5.Sum(data)
	// Extract middle 8 bytes and convert to int64 (same as SDK)
	md5Int64 := int64(binary.BigEndian.Uint64(md5Hash[4:12]))

	return hdrCRC32, fullCRC32, md5Int64, nil
}

// CalculateChecksumsFromReader calculates checksums by reading from an io.Reader
// This is more memory-efficient for large files
// Returns HdrCRC32, CRC32, MD5 (as int64), and error
func CalculateChecksumsFromReader(reader io.Reader, size int64) (uint32, uint32, int64, error) {
	if size == 0 {
		return 0, 0, 0, nil
	}

	// Read header for HdrCRC32
	headerBuf := make([]byte, HdrSize)
	headerRead := 0
	if size < HdrSize {
		headerBuf = make([]byte, size)
	}
	n, err := io.ReadFull(reader, headerBuf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, 0, 0, err
	}
	headerRead = n

	// Calculate HdrCRC32
	var hdrCRC32 uint32
	if headerRead > 0 {
		hdrCRC32 = crc32.ChecksumIEEE(headerBuf[:headerRead])
	}

	// Create CRC32 and MD5 hashers
	crc32Hash := crc32.NewIEEE()
	md5Hash := md5.New()

	// Write header to hashers
	if headerRead > 0 {
		crc32Hash.Write(headerBuf[:headerRead])
		md5Hash.Write(headerBuf[:headerRead])
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
				crc32Hash.Write(buf[:n])
				md5Hash.Write(buf[:n])
				remaining -= int64(n)
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				return 0, 0, 0, err
			}
		}
	}

	// Get final checksums
	fullCRC32 := crc32Hash.Sum32()
	md5Sum := md5Hash.Sum(nil)
	md5Int64 := int64(binary.BigEndian.Uint64(md5Sum[4:12]))

	return hdrCRC32, fullCRC32, md5Int64, nil
}
