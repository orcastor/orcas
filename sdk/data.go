package sdk

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/h2non/filetype"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/orcastor/orcas/core"
	"github.com/tjfoc/gmsm/sm4"
)

var CmprBlacklist = map[string]int{
	"image/png":                   1,
	"image/jpeg":                  1,
	"application/epub+zip":        1,
	"application/zip":             1,
	"application/x-tar":           1,
	"application/vnd.rar":         1,
	"application/gzip":            1,
	"application/x-bzip2":         1,
	"application/x-7z-compressed": 1,
	"application/x-xz":            1,
	"application/zstd":            1,
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

const (
	UPLOAD_DATA = 1 << iota
	CRC32_MD5
	HDR_CRC32
)

const (
	PKG_ALIGN = 8192
	PKG_SIZE  = 4194304
	HDR_SIZE  = 102400
)

type listener struct {
	d       *core.DataInfo
	cfg     Config
	action  uint32
	md5Hash hash.Hash
	once    bool
	sn, cnt int
	cmprBuf bytes.Buffer
	cmpr    archiver.Compressor
}

func newListener(d *core.DataInfo, cfg Config, action uint32) *listener {
	l := &listener{d: d, cfg: cfg, action: action, cmpr: &DummyArchiver{}}
	if action&CRC32_MD5 != 0 {
		l.md5Hash = md5.New()
	}
	return l
}

func (l *listener) Once() *listener {
	l.once = true
	return l
}

func (l *listener) encode(src []byte) (dst []byte, err error) {
	// 加密
	if l.d.Kind&core.DATA_ENDEC_AES256 != 0 {
		// AES256加密
		dst, err = aes256.Encrypt(l.cfg.EndecKey, src)
	} else if l.d.Kind&core.DATA_ENDEC_SM4 != 0 {
		// SM4加密
		dst, err = sm4.Sm4Cbc([]byte(l.cfg.EndecKey), src, true) //sm4Cbc模式pksc7填充加密
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
		if l.action&HDR_CRC32 != 0 {
			if len(buf) > HDR_SIZE {
				l.d.HdrCRC32 = crc32.ChecksumIEEE(buf[0:HDR_SIZE])
			} else {
				l.d.HdrCRC32 = crc32.ChecksumIEEE(buf)
			}
		}
		// 6. 如果开启智能压缩的，检查文件类型确定是否要压缩
		if l.cfg.WiseCmpr > 0 {
			kind, _ := filetype.Match(buf)
			if CmprBlacklist[kind.MIME.Value] == 0 {
				l.d.Kind |= l.cfg.WiseCmpr
				if l.cfg.WiseCmpr&core.DATA_CMPR_SNAPPY != 0 {
					l.cmpr = &archiver.Snappy{}
				} else if l.cfg.WiseCmpr&core.DATA_CMPR_ZSTD != 0 {
					l.cmpr = &archiver.Zstd{}
				} else if l.cfg.WiseCmpr&core.DATA_CMPR_GZIP != 0 {
					l.cmpr = &archiver.Gz{}
				}
				// 如果是黑名单类型，记得要恢复不压缩
			}
			// fmt.Println(kind.MIME.Value)
		}
		if l.cfg.EndecWay > 0 {
			l.d.Kind |= l.cfg.EndecWay
		}
	}
	l.cnt++

	if l.action&CRC32_MD5 != 0 {
		l.d.CRC32 = crc32.Update(l.d.CRC32, crc32.IEEETable, buf)
		l.md5Hash.Write(buf)
	}

	// 上传数据
	if l.action&UPLOAD_DATA != 0 {
		var cmprBuf []byte
		if l.d.Kind&core.DATA_CMPR_MASK == 0 {
			cmprBuf = buf
		} else {
			l.cmpr.Compress(bytes.NewBuffer(buf), &l.cmprBuf)
			// 如果压缩后更大了，恢复原始的
			if l.d.OrigSize < PKG_SIZE {
				if l.cmprBuf.Len() >= len(buf) {
					l.d.Kind &= ^core.DATA_CMPR_MASK
					cmprBuf = buf
				} else {
					cmprBuf = l.cmprBuf.Bytes()
				}
				l.cmprBuf.Reset()
			} else {
				if l.cmprBuf.Len() >= PKG_SIZE {
					cmprBuf = l.cmprBuf.Next(PKG_SIZE)
				}
			}
		}

		if cmprBuf != nil {
			// 加密
			encodedBuf, err := l.encode(cmprBuf)
			if err != nil {
				return l.once, err
			}

			if l.d.Kind&core.DATA_CMPR_MASK != 0 || l.d.Kind&core.DATA_ENDEC_MASK != 0 {
				l.d.Checksum = crc32.Update(l.d.Checksum, crc32.IEEETable, encodedBuf)
				l.d.Size += int64(len(encodedBuf))
			}

			// 需要上传
			if l.d.OrigSize < PKG_SIZE {
				// 7. 检查是否要打包
				if ok, err := dp.Push(c, h, encodedBuf, l.d); err == nil {
					if ok {
						encodedBuf = nil // 打包成功，不再上传
					}
				} else {
					return l.once, err
				}
			}

			if encodedBuf != nil {
				if l.d.ID, err = h.PutData(c, l.d.ID, l.sn, encodedBuf); err != nil {
					return l.once, err
				}
				l.sn++
			}
		}
	}
	return l.once, nil
}

func (l *listener) OnFinish(c core.Ctx, h core.Handler) error {
	if l.action&CRC32_MD5 != 0 {
		l.d.MD5 = fmt.Sprintf("%X", l.md5Hash.Sum(nil)[4:12])
	}
	if l.cmprBuf.Len() > 0 {
		encodedBuf, err := l.encode(l.cmprBuf.Bytes())
		if err != nil {
			return err
		}
		if l.d.ID, err = h.PutData(c, l.d.ID, l.sn, encodedBuf); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		l.d.Checksum = crc32.Update(l.d.Checksum, crc32.IEEETable, encodedBuf)
		l.d.Size += int64(l.cmprBuf.Len())
	}
	if l.d.Kind&core.DATA_CMPR_MASK == 0 && l.d.Kind&core.DATA_ENDEC_MASK == 0 {
		l.d.Checksum = l.d.CRC32
		l.d.Size = l.d.OrigSize
	}
	return nil
}

func (osi *OrcasSDKImpl) readFile(c core.Ctx, path string, l *listener) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var n int
	var once bool
	buf := make([]byte, PKG_SIZE)
	for {
		if n, err = f.Read(buf); n <= 0 || err != nil {
			break
		}
		if once, err = l.OnData(c, osi.h, osi.dp, buf[0:n]); once || err != nil {
			break
		}
	}
	if err != io.EOF {
		return err
	}
	err = l.OnFinish(c, osi.h)
	return err
}

type SortBySize []*core.ObjectInfo

func (p SortBySize) Len() int           { return len(p) }
func (p SortBySize) Less(i, j int) bool { return p[i].Size < p[j].Size || p[i].Name < p[j].Name }
func (p SortBySize) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (osi *OrcasSDKImpl) uploadFiles(c core.Ctx, path string, f []*core.ObjectInfo, d []*core.DataInfo, level, action uint32) error {
	if len(f) <= 0 {
		return nil
	}

	if len(d) <= 0 {
		// 先按文件大小排序一下，尽量让它们可以打包
		sort.Sort(SortBySize(f))
		d = make([]*core.DataInfo, len(f))
		for i := range f {
			d[i] = &core.DataInfo{
				Kind:     core.DATA_NORMAL,
				OrigSize: f[i].Size,
			}
		}
	}

	var f1, f2 []*core.ObjectInfo
	var d1, d2 []*core.DataInfo

	// 2. 如果是文件，先看是否要秒传
	switch level {
	case FAST:
		// 3. 如果要预先秒传的，先读取hdrCrc32，排队检查
		for i, fi := range f {
			if err := osi.readFile(c, filepath.Join(path, fi.Name),
				newListener(d[i], osi.cfg, HDR_CRC32&^action).Once()); err != nil {
				// TODO: 处理错误情况
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)

			}
		}
		ids, err := osi.h.Ref(c, d)
		if err != nil {
			// TODO: 处理错误情况
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
		}
		for i, id := range ids {
			if id > 0 {
				f1 = append(f1, f[i])
				d1 = append(d1, d[i])
			} else {
				f2 = append(f2, f[i])
				d2 = append(d2, d[i])
			}
		}
		if err := osi.uploadFiles(c, path, f1, d1, FULL, action|HDR_CRC32); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		if err := osi.uploadFiles(c, path, f2, d2, OFF, action|HDR_CRC32); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case FULL:
		// 4. 如果不需要预先秒传或者预先秒传失败的，整个读取crc32和md5以后尝试秒传
		for i, fi := range f {
			if err := osi.readFile(c, filepath.Join(path, fi.Name),
				newListener(d[i], osi.cfg, (HDR_CRC32|CRC32_MD5)&^action)); err != nil {
				// TODO: 处理错误情况
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)

			}
		}
		ids, err := osi.h.Ref(c, d)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			// TODO: 处理错误情况
		}
		// 设置DataID，如果是有的，说明秒传成功，不再需要上传数据了
		for i, id := range ids {
			if id > 0 {
				f[i].DataID = id
			} else {
				f2 = append(f2, f[i])
				d2 = append(d2, d[i])
			}
		}
		if _, err := osi.h.Put(c, f); err != nil {
			// TODO: 重名冲突
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		// 5. 秒传失败的（包括超过大小或者预先秒传失败），丢到待上传对列
		if err := osi.uploadFiles(c, path, f2, d2, OFF, action|HDR_CRC32|CRC32_MD5); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case OFF:
		// 直接上传的对象
		for i, fi := range f {
			if err := osi.readFile(c, filepath.Join(path, fi.Name),
				newListener(d[i], osi.cfg, (UPLOAD_DATA|HDR_CRC32|CRC32_MD5)&^action)); err != nil {
				// TODO: 处理错误情况
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
			}
		}
		// 刷新一下打包数据
		if err := osi.dp.Flush(c, osi.h); err != nil {
			// TODO: 处理错误情况
			return err
		}
		ids, err := osi.h.PutDataInfo(c, d)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		// 处理打包上传的对象
		for i, id := range ids {
			if f[i].DataID != id {
				f[i].DataID = id
			}
			f = append(f, f[i])
		}
		if _, err := osi.h.Put(c, f); err != nil {
			// TODO: 重名冲突
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	}
	return nil
}

type dataReader struct {
	c                core.Ctx
	h                core.Handler
	buf              bytes.Buffer
	dataID           int64
	offset, size, sn int
	getSize, remain  int
	kind             uint32
	endecKey         string
}

func newDataReader(c core.Ctx, h core.Handler, d *core.DataInfo, endecKey string) *dataReader {
	dr := &dataReader{c: c, h: h, remain: int(d.Size), kind: d.Kind, endecKey: endecKey}
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
	if len(p) <= dr.buf.Len() {
		return dr.buf.Read(p)
	}
	if dr.remain > 0 {
		buf, err := dr.h.GetData(dr.c, dr.dataID, dr.sn, dr.offset, dr.getSize)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return 0, err
		}
		dr.remain -= len(buf)
		dr.sn++

		decodeBuf := buf
		if dr.kind&core.DATA_ENDEC_AES256 != 0 {
			// AES256解密
			decodeBuf, err = aes256.Decrypt(dr.endecKey, buf)
		} else if dr.kind&core.DATA_ENDEC_SM4 != 0 {
			// SM4解密
			decodeBuf, err = sm4.Sm4Cbc([]byte(dr.endecKey), buf, false) //sm4Cbc模式pksc7填充加密
		}
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			decodeBuf = buf
		}
		dr.buf.Write(decodeBuf)
	}
	return dr.buf.Read(p)
}

func (osi *OrcasSDKImpl) downloadFile(c core.Ctx, o *core.ObjectInfo, lpath string) (err error) {
	if o.Type != core.OBJ_TYPE_FILE {
		return nil
	}

	dataID := o.DataID
	// 如果不是首版本
	if dataID == 0 {
		os, _, _, err := osi.h.List(c, o.ID, core.ListOptions{
			Type:  core.OBJ_TYPE_VERSION,
			Count: 1,
			Order: "-mtime",
		})
		if err != nil || len(os) < 1 {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		dataID = os[0].DataID
	}

	var d *core.DataInfo
	if dataID == core.EmptyDataID {
		d = core.EmptyDataInfo()
	} else {
		d, err = osi.h.GetDataInfo(c, dataID)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	}

	os.MkdirAll(filepath.Dir(lpath), 0766)

	f, err := os.OpenFile(lpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	b := bufio.NewWriter(f)
	defer b.Flush()

	var decmpr archiver.Decompressor
	if d.Kind&core.DATA_CMPR_MASK != 0 {
		if d.Kind&core.DATA_CMPR_SNAPPY != 0 {
			decmpr = &archiver.Snappy{}
		} else if d.Kind&core.DATA_CMPR_ZSTD != 0 {
			decmpr = &archiver.Zstd{}
		} else if d.Kind&core.DATA_CMPR_GZIP != 0 {
			decmpr = &archiver.Gz{}
		}
	} else {
		decmpr = &DummyArchiver{}
	}

	if err = decmpr.Decompress(newDataReader(c, osi.h, d, osi.cfg.EndecKey), b); err != nil {
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return err
	}
	return nil
}

type dataPkger struct {
	buf   *bytes.Buffer
	infos []*core.DataInfo
	thres uint32
}

func newDataPkger(thres uint32) *dataPkger {
	return &dataPkger{
		buf:   bytes.NewBuffer(make([]byte, 0, PKG_SIZE)),
		thres: thres,
	}
}

func (dp *dataPkger) SetThres(thres uint32) {
	dp.thres = thres
}

func (dp *dataPkger) Push(c core.Ctx, h core.Handler, b []byte, d *core.DataInfo) (bool, error) {
	offset := dp.buf.Len()
	if offset+ /*offset%PKG_ALIGN+*/ len(b) > PKG_SIZE || len(dp.infos) >= int(dp.thres) || len(b) >= PKG_SIZE {
		return false, dp.Flush(c, h)
	}
	// 写入前再处理对齐，最后一块就不用补齐了，PS：需要测试一下读性能差多少
	/*if offset%PKG_ALIGN > 0 {
		if padding := PKG_ALIGN - offset%PKG_ALIGN; padding > 0 {
			dp.buf = append(dp.buf, make([]byte, padding)...)
			offset = len(dp.buf)
		}
	}*/
	// 填充内容
	dp.buf.Write(b)
	// 记录偏移
	d.PkgOffset = offset
	// 记录下来要设置打包数据的数据信息
	dp.infos = append(dp.infos, d)
	return true, nil
}

func (dp *dataPkger) Flush(c core.Ctx, h core.Handler) error {
	if dp.buf.Len() <= 0 {
		return nil
	}
	// 上传打包的数据包
	pkgID, err := h.PutData(c, 0, 0, dp.buf.Bytes())
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
