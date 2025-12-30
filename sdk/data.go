package sdk

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/h2non/filetype"
	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/orcastor/orcas/core"
	"github.com/tjfoc/gmsm/sm4"
	"github.com/zeebo/xxh3"
)

const (
	UPLOAD_DATA = 1 << iota
	XXH3_SHA256
	HDR_XXH3
)

const (
	PKG_ALIGN     = 4096
	PKG_SIZE      = 10 * 1024 * 1024 // Default chunk size, used if configuration is not set
	DEFAULT_CHUNK = 10 * 1024 * 1024 // Default 10MB
)

type listener struct {
	bktID      int64
	d          *core.DataInfo
	cfg        core.Config
	action     uint32
	sha256Hash hash.Hash
	xxh3Hash   *xxh3.Hasher
	once       bool
	sn, cnt    int
	cmprBuf    bytes.Buffer
	cmpr       archiver.Compressor
	chunkSize  int64 // Chunk size obtained from bucket configuration
}

// getChunkSize gets chunk size (obtained from bucket configuration, uses default value if not set)
func (l *listener) getChunkSize() int64 {
	if l.chunkSize > 0 {
		return l.chunkSize
	}
	return DEFAULT_CHUNK
}

func newListener(bktID int64, d *core.DataInfo, cfg core.Config, action uint32, chunkSize int64) *listener {
	l := &listener{bktID: bktID, d: d, cfg: cfg, action: action, chunkSize: chunkSize}
	if action&XXH3_SHA256 != 0 {
		l.sha256Hash = sha256.New()
		l.xxh3Hash = xxh3.New()
	}
	return l
}

func (l *listener) Once() *listener {
	l.once = true
	return l
}

func (l *listener) encode(src []byte) (dst []byte, err error) {
	// Encryption
	if l.d.Kind&core.DATA_ENDEC_AES256 != 0 {
		// AES256 encryption
		dst, err = aes256.Encrypt(l.cfg.EndecKey, src)
	} else if l.d.Kind&core.DATA_ENDEC_SM4 != 0 {
		// SM4 encryption
		dst, err = sm4.Sm4Cbc([]byte(l.cfg.EndecKey), src, true) // sm4Cbc mode PKCS7 padding encryption
	} else {
		dst = src
	}
	if err != nil {
		dst = src
	}
	return
}

func (l *listener) OnData(c core.Ctx, h core.Handler, dp *dataPkger, buf []byte) (once bool, err error) {
	if l.cnt == 0 {
		if l.action&HDR_XXH3 != 0 {
			if len(buf) > HdrSize {
				l.d.HdrXXH3 = int64(xxh3.Hash(buf[0:HdrSize]))
			} else {
				l.d.HdrXXH3 = int64(xxh3.Hash(buf))
			}
		}
		// If smart compression is enabled, check file type to determine whether to compress
		if l.cfg.CmprWay > 0 {
			kind, _ := filetype.Match(buf)
			if kind == filetype.Unknown { // Multimedia, archive, application
				l.d.Kind |= l.cfg.CmprWay
				if l.cfg.CmprWay&core.DATA_CMPR_SNAPPY != 0 {
					l.cmpr = &archiver.Snappy{}
				} else if l.cfg.CmprWay&core.DATA_CMPR_ZSTD != 0 {
					l.cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(l.cfg.CmprQlty)))}}
				} else if l.cfg.CmprWay&core.DATA_CMPR_GZIP != 0 {
					l.cmpr = &archiver.Gz{CompressionLevel: int(l.cfg.CmprQlty)}
				} else if l.cfg.CmprWay&core.DATA_CMPR_BR != 0 {
					l.cmpr = &archiver.Brotli{Quality: int(l.cfg.CmprQlty)}
				}
			}
			// If it's a blacklisted type, don't compress
			// fmt.Println(kind.MIME.Value)
		}
		if l.cfg.EndecWay > 0 {
			l.d.Kind |= l.cfg.EndecWay
		}
	}
	isFirstChunk := l.cnt == 0
	l.cnt++

	if l.action&XXH3_SHA256 != 0 {
		l.xxh3Hash.Write(buf)
		l.sha256Hash.Write(buf)
	}

	// Upload data
	// Important: chunk first (buf is already chunk-sized data), then independently compress and encrypt each chunk
	if l.action&UPLOAD_DATA != 0 {
		// Process current chunk
		var processedChunk []byte

		// 1. Compress first (if enabled)
		if l.d.Kind&core.DATA_CMPR_MASK != 0 && l.cmpr != nil {
			l.cmprBuf.Reset()
			err := l.cmpr.Compress(bytes.NewBuffer(buf), &l.cmprBuf)
			if err != nil {
				// Compression failed, use original data, remove compression flag
				// Note: This logic only executes on first chunk, because once compression is decided, subsequent chunks should remain consistent
				if isFirstChunk {
					l.d.Kind &= ^core.DATA_CMPR_MASK
				}
				processedChunk = buf
			} else {
				// If compressed size is larger or equal, use original data, remove compression flag
				// Note: This logic only executes on first chunk, because once compression is decided, subsequent chunks should remain consistent
				if isFirstChunk && l.cmprBuf.Len() >= len(buf) {
					l.d.Kind &= ^core.DATA_CMPR_MASK
					processedChunk = buf
				} else {
					processedChunk = l.cmprBuf.Bytes()
				}
			}
		} else {
			processedChunk = buf
		}

		// 2. Encrypt next (if enabled)
		encodedBuf, encodeErr := l.encode(processedChunk)
		if encodeErr != nil {
			encodedBuf = processedChunk // Encryption failed, use unencrypted data
		}

		// 3. Update checksum and size
		if l.d.Kind&core.DATA_CMPR_MASK != 0 || l.d.Kind&core.DATA_ENDEC_MASK != 0 {
			l.d.Size += int64(len(encodedBuf))
		}

		// 4. Upload data block
		chunkSize := l.getChunkSize()
		if l.d.OrigSize < chunkSize {
			// Small file, check if should package
			var pushOk bool
			if pushOk, err = dp.Push(c, h, l.bktID, encodedBuf, l.d); err == nil {
				if pushOk {
					encodedBuf = nil // Packaging successful, no longer upload
				}
			} else {
				return l.once, err
			}
		}

		if encodedBuf != nil {
			if l.d.ID, err = h.PutData(c, l.bktID, l.d.ID, l.sn, encodedBuf); err != nil {
				return l.once, err
			}
			l.sn++
		}
	}
	return l.once, nil
}

func (l *listener) OnFinish(c core.Ctx, h core.Handler) error {
	if l.action&XXH3_SHA256 != 0 {
		// Calculate XXH3 and SHA-256
		l.d.XXH3 = int64(l.xxh3Hash.Sum64())
		sha256Sum := l.sha256Hash.Sum(nil)
		l.d.SHA256_0 = int64(binary.BigEndian.Uint64(sha256Sum[0:8]))
		l.d.SHA256_1 = int64(binary.BigEndian.Uint64(sha256Sum[8:16]))
		l.d.SHA256_2 = int64(binary.BigEndian.Uint64(sha256Sum[16:24]))
		l.d.SHA256_3 = int64(binary.BigEndian.Uint64(sha256Sum[24:32]))
	}
	// Note: Now each chunk is immediately processed and uploaded in OnData, no need to handle residual data here
	// If compression produced residual data (theoretically shouldn't, because each chunk is independently compressed), handle here
	if l.cmprBuf.Len() > 0 {
		// Encrypt residual data (if enabled)
		encodedBuf, err := l.encode(l.cmprBuf.Bytes())
		if err != nil {
			return err
		}
		if l.d.ID, err = h.PutData(c, l.bktID, l.d.ID, l.sn, encodedBuf); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		l.d.Size += int64(len(encodedBuf))
	}
	// If neither compressed nor encrypted, use original data's XXH3 and size
	if l.d.Kind&core.DATA_CMPR_MASK == 0 && l.d.Kind&core.DATA_ENDEC_MASK == 0 {
		l.d.Size = l.d.OrigSize
	}
	return nil
}

func (osi *OrcasSDKImpl) readFile(c core.Ctx, path string, dp *dataPkger, l *listener) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var n int
	var once bool
	chunkSize := l.getChunkSize()
	buf := make([]byte, chunkSize)
	for {
		if n, err = f.Read(buf); n <= 0 || err != nil {
			break
		}
		if once, err = l.OnData(c, osi.h, dp, buf[0:n]); once || err != nil {
			break
		}
	}
	if err != io.EOF {
		return err
	}
	err = l.OnFinish(c, osi.h)
	return err
}

func (osi *OrcasSDKImpl) putOne(c core.Ctx, bktID int64, o *core.ObjectInfo) (int64, error) {
	ids, err := osi.h.Put(c, bktID, []*core.ObjectInfo{o})
	if err != nil {
		return 0, err
	}
	if len(ids) > 0 && ids[0] > 0 {
		return ids[0], nil
	}
	return 0, fmt.Errorf("remote object exists, pid:%d, name:%s", o.PID, o.Name)
}

func (osi *OrcasSDKImpl) getRename(o *core.ObjectInfo, i int) *core.ObjectInfo {
	x := *o
	if i <= 0 {
		x.Name = fmt.Sprintf(osi.cfg.NameTmpl, x.Name)
	} else {
		x.Name = fmt.Sprintf(osi.cfg.NameTmpl+"%d", x.Name, i+1)
	}
	return &x
}

func (osi *OrcasSDKImpl) putObjects(c core.Ctx, bktID int64, o []*core.ObjectInfo) ([]int64, error) {
	if len(o) <= 0 {
		return nil, nil
	}

	ids, err := osi.h.Put(c, bktID, o)
	if err != nil {
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return nil, err
	}

	switch osi.cfg.Conflict {
	case COVER: // Merge or overwrite
		var vers []*core.ObjectInfo
		for i := range ids {
			if ids[i] <= 0 {
				var pathErr error
				ids[i], pathErr = osi.Path2ID(c, bktID, o[i].PID, o[i].Name)
				if pathErr != nil {
					err = pathErr
				}
				// If it's a file, need to create a new version, version update logic needs jobs to complete
				if o[i].Type == core.OBJ_TYPE_FILE {
					vers = append(vers, &core.ObjectInfo{
						PID:   ids[i],
						MTime: o[i].MTime,
						Type:  core.OBJ_TYPE_VERSION,
						Size:  o[i].Size,
					})
				}
			}
		}
		if len(vers) > 0 {
			_, err = osi.h.Put(c, bktID, vers)
			if err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
				return nil, err
			}
			// Need scheduled task to update first version's DataID
		}
	case RENAME: // Rename
		m := make(map[int]int)
		var rename []*core.ObjectInfo
		for i := range ids {
			// Need to rename and recreate
			if ids[i] <= 0 {
				// First directly create with NameTmpl, covers most scenarios
				m[len(rename)] = i
				rename = append(rename, osi.getRename(o[i], 0))
			}
		}
		if len(rename) > 0 {
			ids2, err2 := osi.h.Put(c, bktID, rename)
			if err2 != nil {
				return ids, err2
			}
			for i := range ids2 {
				if ids2[i] > 0 {
					ids[m[i]] = ids2[i]
					continue
				}
				// Still failed, use NameTmpl to find how many directories, then try one by one
				_, cnt, _, err3 := osi.h.List(c, bktID, rename[i].PID, core.ListOptions{
					Word: rename[i].Name + "*",
				})
				if err3 != nil {
					return ids, err3
				}
				// Assume there are test, test的副本, test的副本2, cnt is 2
				for j := 0; j <= int(cnt/2)+1; j++ {
					// First try the number after count, normal sequential search, branch with highest probability of hit
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], int(cnt)+j)); err3 == nil {
						break
					}
					// Search from front to back
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], j)); err3 == nil {
						break
					}
					// Search from cnt backwards
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], int(cnt)-1-j)); err3 == nil {
						break
					}
				}
			}
		}
	case THROW: // Throw error
		for i := range ids {
			if ids[i] <= 0 {
				return ids, fmt.Errorf("remote object exists, pid:%d, name:%s", o[i].PID, o[i].Name)
			}
		}
	case SKIP: // Skip
		break
	}
	return ids, nil
}

type sizeSort []uploadInfo

func (u sizeSort) Len() int           { return len(u) }
func (u sizeSort) Less(i, j int) bool { return u[i].o.Size < u[j].o.Size || u[i].o.Name < u[j].o.Name }
func (u sizeSort) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }

func (osi *OrcasSDKImpl) uploadFiles(c core.Ctx, bktID int64, u []uploadInfo,
	d []*core.DataInfo, dp *dataPkger, level, doneAction uint32,
) error {
	if len(u) <= 0 {
		return nil
	}

	if len(d) <= 0 {
		// Sort by file size first, try to make them packageable
		sort.Sort(sizeSort(u))
		d = make([]*core.DataInfo, len(u))
		for i := range u {
			d[i] = &core.DataInfo{
				Kind:     core.DATA_NORMAL,
				OrigSize: u[i].o.Size,
			}
		}
	}

	// Get chunkSize from bucket configuration
	var chunkSize int64 = DEFAULT_CHUNK
	bucket, err := osi.h.GetBktInfo(c, bktID)
	if err == nil && bucket != nil && bucket.ChunkSize > 0 {
		chunkSize = bucket.ChunkSize
	}
	fmt.Println("chunkSize", chunkSize)
	fmt.Println("osi.cfg.PkgThres", osi.cfg.PkgThres)
	if dp == nil {
		dp = newDataPkger(osi.cfg.PkgThres, chunkSize)
	}

	var u1, u2 []uploadInfo
	var d1, d2 []*core.DataInfo

	// If it's a file, first check if instant upload is needed
	switch level {
	case FAST:
		// If pre-instant upload is needed, read hdrXXH3 first, queue for check
		// Use chunkSize obtained from bucket configuration
		for i, fi := range u {
			if fi.o.Size > chunkSize {
				u1 = append(u1, fi)
				d1 = append(d1, d[i])
			} else {
				u2 = append(u2, fi)
				d2 = append(d2, d[i])
			}
		}
		if len(u1) > 0 {
			u, d, u1, d1 = u1, d1, nil, nil
			for i, fi := range u {
				if err := osi.readFile(c, filepath.Join(fi.path, fi.o.Name), dp,
					newListener(bktID, d[i], osi.cfg, HDR_XXH3&^doneAction, chunkSize).Once()); err != nil {
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
				}
			}
			ids, err := osi.h.Ref(c, bktID, d)
			if err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
			}
			for i, id := range ids {
				if id > 0 {
					u2 = append(u2, u[i])
					d2 = append(d2, d[i])
				} else {
					u1 = append(u1, u[i])
					d1 = append(d1, d[i])
				}
			}
			if err := osi.uploadFiles(c, bktID, u1, d1, dp, OFF, doneAction|HDR_XXH3); err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
				return err
			}
		}
		if err := osi.uploadFiles(c, bktID, u2, d2, dp, FULL, doneAction); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case FULL:
		// If pre-instant upload is not needed or pre-instant upload failed, read entire xxh3 and sha256 then try instant upload
		for i, fi := range u {
			if err := osi.readFile(c, filepath.Join(fi.path, fi.o.Name), dp,
				newListener(bktID, d[i], osi.cfg, (HDR_XXH3|XXH3_SHA256)&^doneAction, chunkSize)); err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
			}
		}
		ids, err := osi.h.Ref(c, bktID, d)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
		}
		var f []*core.ObjectInfo
		var gap int64
		for i, id := range ids {
			// Set DataID, if it exists, instant upload succeeded, no longer need to upload data
			if id > 0 {
				u[i].o.DataID = id
				f = append(f, u[i].o)
				gap++
			} else {
				if id < 0 { // Less than 0 means referenced data
					d[i].ID = id + gap // Someone left, need to fill the gap
				}
				u1 = append(u1, u[i])
				d1 = append(d1, d[i])
			}
		}
		if _, err := osi.putObjects(c, bktID, f); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		// Instant upload failed (including exceeding size or pre-instant upload failed), normal upload
		// Note: Here recursively calls uploadFiles, chunkSize has been obtained at upper level and passed to dp
		if err := osi.uploadFiles(c, bktID, u1, d1, dp, OFF, doneAction|HDR_XXH3|XXH3_SHA256); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case OFF:
		// Objects uploaded directly
		for i, fi := range u {
			// If referencing others, i.e., <0, no longer need to upload
			if d[i].ID >= 0 {
				if err := osi.readFile(c, filepath.Join(fi.path, fi.o.Name), dp,
					newListener(bktID, d[i], osi.cfg, (UPLOAD_DATA|HDR_XXH3|XXH3_SHA256)&^doneAction, chunkSize)); err != nil {
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
				}
			}
		}
		// Flush packaged data
		if err := dp.Flush(c, osi.h, bktID); err != nil {
			return err
		}
		ids, err := osi.h.PutDataInfo(c, bktID, d)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		var f []*core.ObjectInfo
		// Handle packaged uploaded objects
		for i, id := range ids {
			if u[i].o.DataID != id { // Including less than 0
				u[i].o.DataID = id
			}
			f = append(f, u[i].o)
		}
		if _, err := osi.putObjects(c, bktID, f); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	}
	return nil
}

type DummyArchiver struct{}

func (DummyArchiver) CheckExt(string) error { return nil }
func (DummyArchiver) Compress(in io.Reader, out io.Writer) error {
	_, err := io.Copy(out, in)
	return err
}

func (DummyArchiver) Decompress(in io.Reader, out io.Writer) error {
	_, err := io.Copy(out, in)
	return err
}

type dataReader struct {
	c               core.Ctx
	h               core.Handler
	buf             bytes.Buffer
	bktID           int64
	dataID          int64
	size, sn        int
	getSize, remain int
	offset, kind    uint32
	endecKey        string
}

func NewDataReader(c core.Ctx, h core.Handler, bktID int64, d *core.DataInfo, endecKey string) *dataReader {
	dr := &dataReader{
		c:        c,
		h:        h,
		bktID:    bktID,
		remain:   int(d.OrigSize),
		kind:     d.Kind,
		endecKey: endecKey,
	}
	if d.PkgID > 0 {
		dr.dataID = d.PkgID
		dr.offset = d.PkgOffset
		dr.getSize = int(d.Size)
	} else {
		dr.dataID = d.ID
		dr.getSize = -1
	}

	return dr
}

func (dr *dataReader) Read(p []byte) (n int, err error) {
	if dr.remain <= 0 {
		return 0, io.EOF
	}

	if len(p) <= dr.buf.Len() {
		dr.remain -= len(p)
		return dr.buf.Read(p)
	}

	// Continue reading chunks until buffer has enough data or we hit EOF
	for dr.buf.Len() < len(p) {
		fmt.Println("dr.sn", dr.sn)
		fmt.Println("dr.remain", dr.remain)
		fmt.Println("len(p)", len(p))

		// GetData's offsetOrSize parameter: one parameter means sn, two parameters mean sn+offset, three parameters mean sn+offset+size
		buf, err := dr.h.GetData(dr.c, dr.bktID, dr.dataID, dr.sn, int(dr.offset), dr.getSize)
		if err != nil {
			// Otherwise, return the error (EOF or other error)
			return 0, err
		}

		// If GetData returns empty data, treat as EOF
		if len(buf) == 0 {
			if dr.buf.Len() > 0 {
				break
			}
			return 0, io.EOF
		}

		fmt.Println("buf", len(buf))

		dr.sn++

		// Important: decrypt first, then decompress (because each chunk is independently compressed and encrypted)
		// 1. Decrypt first (if enabled)
		decodeBuf := buf
		if dr.kind&core.DATA_ENDEC_AES256 != 0 {
			// AES256 decryption
			if dr.endecKey == "" {
				return 0, fmt.Errorf("AES256 decryption requires encryption key but key is empty (chunk sn=%d)", dr.sn-1)
			}
			decodeBuf, err = aes256.Decrypt(dr.endecKey, buf)
			if err != nil {
				// Decryption failed - this usually means wrong key or corrupted data
				return 0, fmt.Errorf("AES256 decryption failed for chunk sn=%d (key length=%d, data length=%d): %v. This usually means the encryption key is incorrect or the data is corrupted", dr.sn-1, len(dr.endecKey), len(buf), err)
			}
		} else if dr.kind&core.DATA_ENDEC_SM4 != 0 {
			// SM4 decryption
			if dr.endecKey == "" {
				return 0, fmt.Errorf("SM4 decryption requires encryption key but key is empty (chunk sn=%d)", dr.sn-1)
			}
			decodeBuf, err = sm4.Sm4Cbc([]byte(dr.endecKey), buf, false) // sm4Cbc mode PKCS7 padding decryption
			if err != nil {
				// Decryption failed - this usually means wrong key or corrupted data
				return 0, fmt.Errorf("SM4 decryption failed for chunk sn=%d (key length=%d, data length=%d): %v. This usually means the encryption key is incorrect or the data is corrupted", dr.sn-1, len(dr.endecKey), len(buf), err)
			}
		}

		// 2. Decompress next (if enabled)
		// Note: Each chunk is independently compressed, so need to create independent decompressor for each chunk
		finalBuf := decodeBuf
		if dr.kind&core.DATA_CMPR_MASK != 0 {
			var decompressor archiver.Decompressor
			if dr.kind&core.DATA_CMPR_SNAPPY != 0 {
				decompressor = &archiver.Snappy{}
			} else if dr.kind&core.DATA_CMPR_ZSTD != 0 {
				decompressor = &archiver.Zstd{}
			} else if dr.kind&core.DATA_CMPR_GZIP != 0 {
				decompressor = &archiver.Gz{}
			} else if dr.kind&core.DATA_CMPR_BR != 0 {
				decompressor = &archiver.Brotli{}
			}

			if decompressor != nil {
				var decompressedBuf bytes.Buffer
				err := decompressor.Decompress(bytes.NewReader(decodeBuf), &decompressedBuf)
				if err != nil {
					// Decompression failed, use decrypted data (compressed data may be corrupted or wrong format)
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
					finalBuf = decodeBuf
				} else {
					finalBuf = decompressedBuf.Bytes()
				}
			}
		}

		dr.buf.Write(finalBuf)
	}

	dr.remain -= len(p)
	return dr.buf.Read(p)
}

func (osi *OrcasSDKImpl) downloadFile(c core.Ctx, bktID int64, o *core.ObjectInfo, lpath string) (err error) {
	if o.Type != core.OBJ_TYPE_FILE {
		return nil
	}

	dataID := o.DataID
	// If not the first version
	if dataID == 0 {
		os, _, _, listErr := osi.h.List(c, bktID, o.ID, core.ListOptions{
			Type:  core.OBJ_TYPE_VERSION,
			Count: 1,
			Order: "-id",
		})
		if listErr != nil || len(os) < 1 {
			fmt.Println(runtime.Caller(0))
			fmt.Println(listErr)
			return listErr
		}
		dataID = os[0].DataID
	}

	var d *core.DataInfo
	if dataID == core.EmptyDataID {
		d = core.EmptyDataInfo()
	} else {
		d, err = osi.h.GetDataInfo(c, bktID, dataID)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	}

	os.MkdirAll(filepath.Dir(lpath), 0o766)

	f, err := os.OpenFile(lpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()
	b := bufio.NewWriter(f)
	defer b.Flush()

	// Get encryption key from SDK config
	// EndecKey is no longer stored in bucket config, should be provided via SDK config
	endecKey := osi.cfg.EndecKey

	// Check if encryption is required but key is missing
	if d.Kind&core.DATA_ENDEC_MASK != 0 && endecKey == "" {
		return fmt.Errorf("encryption key is required for encrypted data (DataID: %d, Kind: 0x%x) but not provided in SDK config or bucket config", dataID, d.Kind)
	}

	_, err = io.CopyN(b, NewDataReader(c, osi.h, bktID, d, endecKey), d.OrigSize)
	if err != nil {
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return err
	}
	return nil
}

type dataPkger struct {
	buf       *bytes.Buffer
	infos     []*core.DataInfo
	thres     uint32
	chunkSize int64 // Chunk size
}

func newDataPkger(thres uint32, chunkSize int64) *dataPkger {
	if chunkSize <= 0 {
		chunkSize = DEFAULT_CHUNK
	}
	return &dataPkger{
		buf:       bytes.NewBuffer(make([]byte, 0, chunkSize)),
		thres:     thres,
		chunkSize: chunkSize,
	}
}

func (dp *dataPkger) SetThres(thres uint32) {
	dp.thres = thres
}

func (dp *dataPkger) Push(c core.Ctx, h core.Handler, bktID int64, b []byte, d *core.DataInfo) (bool, error) {
	offset := dp.buf.Len()
	// Use configured chunkSize instead of PKG_SIZE constant
	chunkSize := dp.chunkSize
	if chunkSize <= 0 {
		chunkSize = DEFAULT_CHUNK
	}
	if offset+ /*offset%PKG_ALIGN+*/ len(b) > int(chunkSize) || len(dp.infos) >= int(dp.thres) || len(b) >= int(chunkSize) {
		return false, dp.Flush(c, h, bktID)
	}
	// Handle alignment before writing, last chunk doesn't need padding, PS: need to test how much read performance differs
	/*if offset%PKG_ALIGN > 0 {
		if padding := PKG_ALIGN - offset%PKG_ALIGN; padding > 0 {
			dp.buf = append(dp.buf, make([]byte, padding)...)
			offset = len(dp.buf)
		}
	}*/
	// Fill content
	dp.buf.Write(b)
	// Record offset
	d.PkgOffset = uint32(offset)
	// Record data info to set packaged data
	dp.infos = append(dp.infos, d)
	return true, nil
}

func (dp *dataPkger) Flush(c core.Ctx, h core.Handler, bktID int64) error {
	if dp.buf.Len() <= 0 {
		return nil
	}
	// Upload packaged data packet
	pkgID, err := h.PutData(c, bktID, 0, 0, dp.buf.Bytes())
	if err != nil {
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return err
	}
	if len(dp.infos) == 1 {
		dp.infos[0].ID = pkgID
	} else {
		for i := range dp.infos {
			dp.infos[i].PkgID = pkgID
		}
	}
	dp.buf.Reset()
	dp.infos = nil
	return nil
}
