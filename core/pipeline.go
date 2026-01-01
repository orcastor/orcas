package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/h2non/filetype"
	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/tjfoc/gmsm/sm4"
	"github.com/zeebo/xxh3"
)

// nonCompressibleExts is a map of file extensions that don't benefit from compression
// Common compressed/encoded file extensions that are already compressed or encoded
var nonCompressibleExts map[string]struct{} = map[string]struct{}{
	// Archive/Compression formats
	".zip": {}, ".gz": {}, ".bz2": {}, ".xz": {}, ".7z": {}, ".rar": {}, ".tar": {},
	".cab": {}, ".deb": {}, ".dmg": {}, ".iso": {}, ".lz": {}, ".lzma": {}, ".lzo": {},
	".pak": {}, ".rpm": {}, ".sit": {}, ".sitx": {}, ".tgz": {}, ".z": {}, ".zst": {},
	".apk": {}, ".ipa": {}, ".jar": {}, ".war": {}, ".ear": {}, ".zipx": {}, ".ace": {},
	".arc": {}, ".arj": {}, ".cpio": {}, ".lha": {}, ".lzh": {}, ".zoo": {},

	// Image formats
	".jpg": {}, ".jpeg": {}, ".png": {}, ".gif": {}, ".webp": {}, ".bmp": {}, ".ico": {},
	".svg": {}, ".tiff": {}, ".tif": {}, ".psd": {}, ".ai": {}, ".eps": {}, ".raw": {},
	".cr2": {}, ".nef": {}, ".orf": {}, ".sr2": {}, ".arw": {}, ".dng": {}, ".heic": {},
	".heif": {}, ".avif": {}, ".jp2": {}, ".j2k": {}, ".jpx": {}, ".jpf": {}, ".jpm": {},
	".jpc": {}, ".jxr": {}, ".wdp": {}, ".hdp": {}, ".exr": {}, ".hdr": {}, ".xcf": {},
	".sketch": {}, ".fig": {}, ".xd": {},

	// Video formats
	".mp4": {}, ".avi": {}, ".mkv": {}, ".mov": {}, ".wmv": {}, ".flv": {}, ".webm": {},
	".m4v": {}, ".mpg": {}, ".mpeg": {}, ".mts": {}, ".m2ts": {}, ".ogv": {}, ".qt": {},
	".rm": {}, ".rmvb": {}, ".swf": {}, ".ts": {}, ".vob": {}, ".f4v": {}, ".f4p": {},
	".f4a": {}, ".f4b": {}, ".mxf": {}, ".divx": {}, ".xvid": {}, ".h264": {}, ".h265": {},
	".hevc": {}, ".vp8": {}, ".vp9": {}, ".av1": {}, ".3gp": {}, ".3g2": {}, ".asf": {},
	".avchd": {}, ".m2v": {}, ".mpv": {}, ".nsv": {}, ".ogm": {},

	// Audio formats
	".mp3": {}, ".wav": {}, ".flac": {}, ".aac": {}, ".ogg": {}, ".m4a": {}, ".wma": {},
	".m4p": {}, ".ape": {}, ".opus": {}, ".ra": {}, ".amr": {}, ".aa": {},
	".aax": {}, ".act": {}, ".aiff": {}, ".au": {}, ".awb": {}, ".dct": {}, ".dss": {},
	".dvf": {}, ".gsm": {}, ".iklax": {}, ".ivs": {}, ".m4b": {}, ".mmf": {}, ".mpc": {},
	".msv": {}, ".nmf": {}, ".nsf": {}, ".oga": {}, ".mogg": {}, ".rf64": {},
	".sln": {}, ".tta": {}, ".voc": {}, ".vox": {}, ".wv": {}, ".wvx": {}, ".3ga": {},

	// CAD formats
	".dwg": {}, ".dxf": {}, ".dgn": {}, ".dwf": {}, ".ifc": {}, ".step": {}, ".stp": {},
	".iges": {}, ".igs": {}, ".3dm": {}, ".3ds": {}, ".max": {}, ".obj": {}, ".fbx": {},
	".dae": {}, ".blend": {}, ".skp": {}, ".c4d": {}, ".ma": {}, ".mb": {},
	".lwo": {}, ".lws": {}, ".x3d": {}, ".x3dv": {}, ".x3db": {}, ".ply": {}, ".stl": {},
	".off": {}, ".3mf": {}, ".amf": {}, ".collada": {}, ".x": {}, ".b3d": {},
	".bvh": {}, ".cob": {}, ".csm": {}, ".enff": {}, ".gltf": {},
	".glb": {}, ".iqm": {}, ".irrmesh": {}, ".irr": {}, ".lxo": {},
	".md2": {}, ".md3": {}, ".md5anim": {}, ".md5camera": {}, ".md5mesh": {},
	".mdc": {}, ".mdl": {}, ".mesh": {}, ".mesh.xml": {}, ".mot": {}, ".ms3d": {},
	".ndo": {}, ".nff": {}, ".ogex": {}, ".pk3": {},
	".pmx": {}, ".prj": {}, ".q3o": {}, ".q3s": {}, ".scn": {}, ".sib": {},
	".smd": {}, ".ter": {}, ".uc": {}, ".vta": {},
	".xgl": {}, ".zgl": {},

	// Document formats (already compressed)
	".pdf": {}, ".doc": {}, ".docx": {}, ".xls": {}, ".xlsx": {}, ".ppt": {}, ".pptx": {},
	".odt": {}, ".ods": {}, ".odp": {}, ".rtf": {}, ".pages": {}, ".numbers": {}, ".key": {},
	".epub": {}, ".mobi": {}, ".azw": {}, ".azw3": {}, ".fb2": {}, ".ibooks": {},

	// Font formats
	".woff": {}, ".woff2": {}, ".ttf": {}, ".otf": {}, ".eot": {}, ".fon": {}, ".fnt": {},
	".ttc": {}, ".afm": {}, ".pfb": {}, ".pfm": {},

	// Other compressed/binary formats
	".db": {}, ".sqlite": {}, ".sqlite3": {}, ".mdb": {}, ".accdb": {}, ".fdb": {},
	".gdb": {}, ".pst": {}, ".ost": {}, ".msg": {}, ".eml": {},
}

// ShouldCompressFile checks if a file should be compressed based on file extension and file header
// Returns true if the file should be compressed, false otherwise
//
// Algorithm:
//  1. Check file extension first (faster than file header check)
//  2. If extension check passed, check file header using filetype.Match
//
// Empty chunks are allowed to be compressed
func ShouldCompressFile(fileName string, firstChunk []byte) bool {
	if len(firstChunk) == 0 {
		return false // Empty chunk, don't compress
	}

	shouldCompress := true

	// Step 1: Check file extension first (faster than file header check)
	if fileName != "" {
		ext := strings.ToLower(filepath.Ext(fileName))
		// Check if file extension is in non-compressible extensions map
		if _, ok := nonCompressibleExts[ext]; ok {
			shouldCompress = false
		}
	}

	// Step 2: If extension check passed, check file header
	if shouldCompress {
		detectedKind, _ := filetype.Match(firstChunk)
		if detectedKind != filetype.Unknown {
			// Detected as multimedia/archive/application type, don't compress
			shouldCompress = false
		}
	}

	return shouldCompress
}

// ShouldCompressFileByName determines if a file should be compressed based on its name only
// This checks the file extension only (no file header check)
// This is faster but less accurate than ShouldCompressFile
func ShouldCompressFileByName(fileName string) bool {
	// Check extension
	if fileName != "" {
		ext := strings.ToLower(filepath.Ext(fileName))
		// Check if file extension is in non-compressible extensions map
		if _, ok := nonCompressibleExts[ext]; ok {
			return false
		}
	}
	return true
}

// CreateCompressor creates a compressor based on compression type and quality
// Returns nil if compression is disabled (cmprWay == 0)
func CreateCompressor(cmprWay uint32, cmprQlty uint32) archiver.Compressor {
	if cmprWay == 0 {
		return nil
	}
	if cmprWay&DATA_CMPR_SNAPPY != 0 {
		return &archiver.Snappy{}
	} else if cmprWay&DATA_CMPR_ZSTD != 0 {
		return &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(cmprQlty)))}}
	} else if cmprWay&DATA_CMPR_GZIP != 0 {
		return &archiver.Gz{CompressionLevel: int(cmprQlty)}
	} else if cmprWay&DATA_CMPR_BR != 0 {
		return &archiver.Brotli{Quality: int(cmprQlty)}
	}
	return nil
}

// CreateDecompressor creates a decompressor based on compression flags in kind
// Returns nil if no compression is present
func CreateDecompressor(kind uint32) archiver.Decompressor {
	if kind&DATA_CMPR_MASK == 0 {
		return nil
	}
	if kind&DATA_CMPR_SNAPPY != 0 {
		return &archiver.Snappy{}
	} else if kind&DATA_CMPR_ZSTD != 0 {
		return &archiver.Zstd{}
	} else if kind&DATA_CMPR_GZIP != 0 {
		return &archiver.Gz{}
	} else if kind&DATA_CMPR_BR != 0 {
		return &archiver.Brotli{}
	}
	return nil
}

// CompressData compresses data using the specified compressor
// Returns compressed data if compression succeeds and reduces size, otherwise returns original data
// The kind parameter will be modified to remove compression flag if compression fails or doesn't reduce size
func CompressData(data []byte, cmpr archiver.Compressor, kind *uint32) ([]byte, error) {
	if cmpr == nil || len(data) == 0 {
		return data, nil
	}

	var cmprBuf bytes.Buffer
	err := cmpr.Compress(bytes.NewBuffer(data), &cmprBuf)
	if err != nil {
		return data, err
	}

	return cmprBuf.Bytes(), nil
}

// DecompressData decompresses data using the specified decompressor
func DecompressData(data []byte, decompr archiver.Decompressor) ([]byte, error) {
	if decompr == nil || len(data) == 0 {
		return data, nil
	}

	var dcmprBuf bytes.Buffer
	err := decompr.Decompress(bytes.NewBuffer(data), &dcmprBuf)
	if err != nil {
		return nil, err
	}

	return dcmprBuf.Bytes(), nil
}

// CompressReader compresses data from reader to writer
func CompressReader(reader io.Reader, writer io.Writer, cmpr archiver.Compressor) error {
	if cmpr == nil {
		// No compression, copy directly
		_, err := io.Copy(writer, reader)
		return err
	}
	return cmpr.Compress(reader, writer)
}

// DecompressReader decompresses data from reader to writer
func DecompressReader(reader io.Reader, writer io.Writer, decompr archiver.Decompressor) error {
	if decompr == nil {
		// No decompression, copy directly
		_, err := io.Copy(writer, reader)
		return err
	}
	return decompr.Decompress(reader, writer)
}

// HasCompression checks if data has compression flag
func HasCompression(kind uint32) bool {
	return kind&DATA_CMPR_MASK != 0
}

// GetCompressionType returns the compression type from kind
func GetCompressionType(kind uint32) uint32 {
	return kind & DATA_CMPR_MASK
}

// GetCompressionName returns human-readable compression algorithm name
func GetCompressionName(cmprWay uint32) string {
	switch {
	case cmprWay&DATA_CMPR_SNAPPY != 0:
		return "Snappy"
	case cmprWay&DATA_CMPR_ZSTD != 0:
		return "Zstd"
	case cmprWay&DATA_CMPR_GZIP != 0:
		return "Gzip"
	case cmprWay&DATA_CMPR_BR != 0:
		return "Brotli"
	default:
		return "None"
	}
}

// EncryptData encrypts data using the specified encryption method
// Returns encrypted data if encryption succeeds, otherwise returns error
func EncryptData(data []byte, kind uint32, key string) ([]byte, error) {
	if len(data) == 0 || key == "" {
		return data, nil
	}

	if kind&DATA_ENDEC_AES256 != 0 {
		return aes256.Encrypt(key, data)
	} else if kind&DATA_ENDEC_SM4 != 0 {
		return sm4.Sm4Cbc([]byte(key), data, true) // SM4 CBC mode with PKCS7 padding
	}

	return data, nil
}

// DecryptData decrypts data using the specified encryption method
// Returns decrypted data if decryption succeeds, otherwise returns error
func DecryptData(data []byte, kind uint32, key string) ([]byte, error) {
	if len(data) == 0 || key == "" {
		return data, nil
	}

	if kind&DATA_ENDEC_AES256 != 0 {
		return aes256.Decrypt(key, data)
	} else if kind&DATA_ENDEC_SM4 != 0 {
		return sm4.Sm4Cbc([]byte(key), data, false) // SM4 CBC mode decrypt
	}

	return data, nil
}

// HasEncryption checks if data has encryption flag
func HasEncryption(kind uint32) bool {
	return kind&DATA_ENDEC_MASK != 0
}

// GetEncryptionType returns the encryption type from kind
func GetEncryptionType(kind uint32) uint32 {
	return kind & DATA_ENDEC_MASK
}

// GetEncryptionName returns human-readable encryption algorithm name
func GetEncryptionName(endecWay uint32) string {
	switch {
	case endecWay&DATA_ENDEC_AES256 != 0:
		return "AES256"
	case endecWay&DATA_ENDEC_SM4 != 0:
		return "SM4"
	default:
		return "None"
	}
}

// ValidateEncryptionKey validates if the encryption key is valid for the specified encryption method
func ValidateEncryptionKey(endecWay uint32, key string) error {
	if endecWay == 0 {
		return nil
	}

	if key == "" {
		return fmt.Errorf("encryption key is required")
	}

	if endecWay&DATA_ENDEC_AES256 != 0 {
		if len(key) <= 16 {
			return fmt.Errorf("AES256 encryption key must be longer than 16 characters")
		}
	} else if endecWay&DATA_ENDEC_SM4 != 0 {
		if len(key) != 16 {
			return fmt.Errorf("SM4 encryption key must be exactly 16 characters")
		}
	}

	return nil
}

// ProcessData applies compression and encryption to data
// Processing order: Compression -> Encryption
// Returns processed data and updates kind flags if compression/encryption fails
//
// Parameters:
//   - data: original data to process
//   - kind: pointer to kind flags (will be modified if processing fails)
//   - cmprQlty: compression quality
//   - encryptionKey: encryption key (can be empty if needsEncryption is false)
//   - isFirstChunk: whether the current chunk is the first chunk
func ProcessData(data []byte, kind *uint32, cmprQlty uint32, encryptionKey string, isFirstChunk bool) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	processedData := data

	// Step 1: Apply compression (if needed)
	if kind != nil && *kind&DATA_CMPR_MASK > 0 {
		// Create compressor
		cmpr := CreateCompressor(*kind&DATA_CMPR_MASK, cmprQlty)

		var err error
		processedData, err = CompressData(processedData, cmpr, kind)
		if err != nil {
			if isFirstChunk {
				*kind &= ^DATA_CMPR_MASK
			} else {
				return nil, fmt.Errorf("compression failed: %v", err)
			}
		} else {
			if isFirstChunk && len(processedData) >= len(data) {
				*kind &= ^DATA_CMPR_MASK
				processedData = data
			}
		}
	}

	// Step 2: Apply encryption (if needed)
	if encryptionKey != "" && kind != nil && *kind&DATA_ENDEC_MASK != 0 {
		var err error
		processedData, err = EncryptData(processedData, *kind, encryptionKey)
		if err != nil {
			if isFirstChunk {
				*kind &= ^DATA_ENDEC_MASK
			} else {
				return nil, fmt.Errorf("encryption failed: %v", err)
			}
		}
	}

	return processedData, nil
}

// UnprocessData applies decryption and decompression to data
// Processing order: Decryption -> Decompression (reverse of ProcessData)
//
// Parameters:
//   - data: processed data to unprocess
//   - kind: kind flags indicating what processing was applied
//   - decryptionKey: decryption key (can be empty if no encryption)
func UnprocessData(data []byte, kind uint32, decryptionKey string) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	processedData := data

	// Step 1: Apply decryption (if needed)
	if kind&DATA_ENDEC_MASK != 0 && decryptionKey != "" {
		var err error
		processedData, err = DecryptData(processedData, kind, decryptionKey)
		if err != nil {
			return nil, fmt.Errorf("decryption failed: %v", err)
		}
	}

	// Step 2: Apply decompression (if needed)
	if kind&DATA_CMPR_MASK != 0 {
		decompr := CreateDecompressor(kind)
		var err error
		processedData, err = DecompressData(processedData, decompr)
		if err != nil {
			return nil, fmt.Errorf("decompression failed: %v", err)
		}
	}

	return processedData, nil
}

// SetCompressionKind sets compression kind flags based on compression method
func SetCompressionKind(kind *uint32, cmprWay uint32) {
	if kind == nil || cmprWay == 0 {
		return
	}
	*kind |= (cmprWay & DATA_CMPR_MASK)
}

// SetEncryptionKind sets encryption kind flags based on encryption method
func SetEncryptionKind(kind *uint32, endecWay uint32) {
	if kind == nil || endecWay == 0 {
		return
	}
	*kind |= (endecWay & DATA_ENDEC_MASK)
}

// ClearCompressionKind removes compression kind flags
func ClearCompressionKind(kind *uint32) {
	if kind == nil {
		return
	}
	*kind &= ^DATA_CMPR_MASK
}

// ClearEncryptionKind removes encryption kind flags
func ClearEncryptionKind(kind *uint32) {
	if kind == nil {
		return
	}
	*kind &= ^DATA_ENDEC_MASK
}

// CalculateChecksums calculates HdrXXH3, XXH3, and SHA-256 checksums from data
// Returns HdrXXH3, XXH3, SHA256_0, SHA256_1, SHA256_2, SHA256_3, and error
// This function is used for instant upload (deduplication) feature
func CalculateChecksums(data []byte) (hdrXXH3 int64, fullXXH3 int64, sha256_0 int64, sha256_1 int64, sha256_2 int64, sha256_3 int64) {
	if len(data) == 0 {
		hdrXXH3 = 3244421341483603138
		fullXXH3 = 3244421341483603138
		sha256_0 = -2039914840885289964
		sha256_1 = -7278955230309402332
		sha256_2 = 2859295262623109964
		sha256_3 = -6587190536697628587
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		// Calculate HdrXXH3 (first 100KB or entire file if smaller)
		if len(data) > DefaultHdrSize {
			fullXXH3 = int64(xxh3.Hash(data))
			hdrXXH3 = int64(xxh3.Hash(data[0:DefaultHdrSize]))
		} else {
			fullXXH3 = int64(xxh3.Hash(data))
			hdrXXH3 = fullXXH3
		}
	}()

	go func() {
		defer wg.Done()
		// Calculate SHA-256 for entire file
		sha256Hash := sha256.Sum256(data)
		sha256_0 = int64(binary.BigEndian.Uint64(sha256Hash[0:8]))
		sha256_1 = int64(binary.BigEndian.Uint64(sha256Hash[8:16]))
		sha256_2 = int64(binary.BigEndian.Uint64(sha256Hash[16:24]))
		sha256_3 = int64(binary.BigEndian.Uint64(sha256Hash[24:32]))
	}()
	wg.Wait()

	return
}

// CalculateChecksumsFromReader calculates checksums by reading from an io.Reader
// This is more memory-efficient for large files
// Returns HdrXXH3, XXH3, SHA256_0, SHA256_1, SHA256_2, SHA256_3, and error
func CalculateChecksumsFromReader(reader io.Reader, size int64) (uint64, uint64, int64, int64, int64, int64, error) {
	if size == 0 {
		return 0, 0, 0, 0, 0, 0, nil
	}

	// Read header for HdrXXH3
	headerBuf := make([]byte, DefaultHdrSize)
	headerRead := 0
	if size < DefaultHdrSize {
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
