package sdk

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
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

const (
	UPLOAD_DATA = 1 << iota
	CRC32_MD5
	HDR_CRC32
)

const (
	PKG_ALIGN = 4096
	PKG_SIZE  = 4194304
	HDR_SIZE  = 102400
)

type listener struct {
	bktID   int64
	d       *core.DataInfo
	cfg     Config
	action  uint32
	md5Hash hash.Hash
	once    bool
	sn, cnt int
	cmprBuf bytes.Buffer
	cmpr    archiver.Compressor
}

func newListener(bktID int64, d *core.DataInfo, cfg Config, action uint32) *listener {
	l := &listener{bktID: bktID, d: d, cfg: cfg, action: action}
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
		dst, err = sm4.Sm4Cbc([]byte(l.cfg.EndecKey), src, true) //sm4Cbc模式PKSC7填充加密
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
		// 如果开启智能压缩的，检查文件类型确定是否要压缩
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
			}
			// 如果是黑名单类型，不压缩
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
				l.d.Cksum = crc32.Update(l.d.Cksum, crc32.IEEETable, encodedBuf)
				l.d.Size += int64(len(encodedBuf))
			}

			// 需要上传
			if l.d.OrigSize < PKG_SIZE {
				// 7. 检查是否要打包
				if ok, err := dp.Push(c, h, l.bktID, encodedBuf, l.d); err == nil {
					if ok {
						encodedBuf = nil // 打包成功，不再上传
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
	}
	return l.once, nil
}

func (l *listener) OnFinish(c core.Ctx, h core.Handler) error {
	if l.action&CRC32_MD5 != 0 {
		l.d.MD5 = int64(binary.BigEndian.Uint64(l.md5Hash.Sum(nil)[4:12]))
	}
	if l.cmprBuf.Len() > 0 {
		encodedBuf, err := l.encode(l.cmprBuf.Bytes())
		if err != nil {
			return err
		}
		if l.d.ID, err = h.PutData(c, l.bktID, l.d.ID, l.sn, encodedBuf); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		l.d.Cksum = crc32.Update(l.d.Cksum, crc32.IEEETable, encodedBuf)
		l.d.Size += int64(l.cmprBuf.Len())
	}
	if l.d.Kind&core.DATA_CMPR_MASK == 0 && l.d.Kind&core.DATA_ENDEC_MASK == 0 {
		l.d.Cksum = l.d.CRC32
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
	buf := make([]byte, PKG_SIZE)
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
	case COVER: // 合并或覆盖
		var vers []*core.ObjectInfo
		for i := range ids {
			if ids[i] <= 0 {
				ids[i], err = osi.Path2ID(c, bktID, o[i].PID, o[i].Name)
				// 如果是文件，需要新建一个版本，版本更新的逻辑需要jobs来完成
				if o[i].Type == core.OBJ_TYPE_FILE {
					vers = append(vers, &core.ObjectInfo{
						PID:    ids[i],
						MTime:  o[i].MTime,
						Type:   core.OBJ_TYPE_VERSION,
						Status: core.OBJ_NORMAL,
						Size:   o[i].Size,
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
		}
	case RENAME: // 重命名
		m := make(map[int]int)
		var rename []*core.ObjectInfo
		for i := range ids {
			// 需要重命名重新创建
			if ids[i] <= 0 {
				// 先直接用NameTmpl创建，覆盖大部分场景
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
				// 还是失败，用NameTmpl找有多少个目录，然后往后一个一个尝试
				_, cnt, _, err3 := osi.h.List(c, bktID, rename[i].PID, core.ListOptions{
					Word: rename[i].Name + "*",
				})
				if err3 != nil {
					return ids, err3
				}
				// 假设有 test、test的副本、test的副本2，cnt为2
				for j := 0; j <= int(cnt/2)+1; j++ {
					// 先试试个数后面一个，正常顺序查找，最大概率命中的分支
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], int(cnt)+j)); err3 == nil {
						break
					}
					// 从最前面往后找
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], j)); err3 == nil {
						break
					}
					// 从cnt个开始往前找
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], int(cnt)-1-j)); err3 == nil {
						break
					}
				}
			}
		}
	case THROW: // 报错
		for i := range ids {
			if ids[i] <= 0 {
				return ids, fmt.Errorf("remote object exists, pid:%d, name:%s", o[i].PID, o[i].Name)
			}
		}
	case SKIP: // 跳过
		break
	}
	return ids, nil
}

type sizeSort []uploadInfo

func (u sizeSort) Len() int           { return len(u) }
func (u sizeSort) Less(i, j int) bool { return u[i].o.Size < u[j].o.Size || u[i].o.Name < u[j].o.Name }
func (u sizeSort) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }

func (osi *OrcasSDKImpl) uploadFiles(c core.Ctx, bktID int64, u []uploadInfo,
	d []*core.DataInfo, dp *dataPkger, level, doneAction uint32) error {
	if len(u) <= 0 {
		return nil
	}

	if len(d) <= 0 {
		// 先按文件大小排序一下，尽量让它们可以打包
		sort.Sort(sizeSort(u))
		d = make([]*core.DataInfo, len(u))
		for i := range u {
			d[i] = &core.DataInfo{
				Kind:     core.DATA_NORMAL,
				OrigSize: u[i].o.Size,
			}
		}
	}

	if dp == nil {
		dp = newDataPkger(osi.cfg.PkgThres)
	}

	var u1, u2 []uploadInfo
	var d1, d2 []*core.DataInfo

	// 如果是文件，先看是否要秒传
	switch level {
	case FAST:
		// 如果要预先秒传的，先读取hdrCRC32，排队检查
		for i, fi := range u {
			if fi.o.Size > PKG_SIZE {
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
					newListener(bktID, d[i], osi.cfg, HDR_CRC32&^doneAction).Once()); err != nil {
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
			if err := osi.uploadFiles(c, bktID, u1, d1, dp, OFF, doneAction|HDR_CRC32); err != nil {
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
		// 如果不需要预先秒传或者预先秒传失败的，整个读取crc32和md5以后尝试秒传
		for i, fi := range u {
			if err := osi.readFile(c, filepath.Join(fi.path, fi.o.Name), dp,
				newListener(bktID, d[i], osi.cfg, (HDR_CRC32|CRC32_MD5)&^doneAction)); err != nil {
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
			// 设置DataID，如果是有的，说明秒传成功，不再需要上传数据了
			if id > 0 {
				u[i].o.DataID = id
				f = append(f, u[i].o)
				gap++
			} else {
				if id < 0 { // 小于0说明是引用的数据
					d[i].ID = id + gap // 有人走了，那需要补充空隙
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
		// 秒传失败的（包括超过大小或者预先秒传失败），普通上传
		if err := osi.uploadFiles(c, bktID, u1, d1, dp, OFF, doneAction|HDR_CRC32|CRC32_MD5); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case OFF:
		// 直接上传的对象
		for i, fi := range u {
			// 如果是引用别人的，也就是<0的，不用再传了就
			if d[i].ID >= 0 {
				if err := osi.readFile(c, filepath.Join(fi.path, fi.o.Name), dp,
					newListener(bktID, d[i], osi.cfg, (UPLOAD_DATA|HDR_CRC32|CRC32_MD5)&^doneAction)); err != nil {
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
				}
			}
		}
		// 刷新一下打包数据
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
		// 处理打包上传的对象
		for i, id := range ids {
			if u[i].o.DataID != id { // 包括小于0的
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
	c                core.Ctx
	h                core.Handler
	buf              bytes.Buffer
	bktID            int64
	dataID           int64
	offset, size, sn int
	getSize, remain  int
	kind             uint32
	endecKey         string
}

func newDataReader(c core.Ctx, h core.Handler, bktID int64, d *core.DataInfo, endecKey string) *dataReader {
	dr := &dataReader{c: c, h: h, bktID: bktID, remain: int(d.Size), kind: d.Kind, endecKey: endecKey}
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
		buf, err := dr.h.GetData(dr.c, dr.bktID, dr.dataID, dr.sn, dr.offset, dr.getSize)
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

func (osi *OrcasSDKImpl) downloadFile(c core.Ctx, bktID int64, o *core.ObjectInfo, lpath string) (err error) {
	if o.Type != core.OBJ_TYPE_FILE {
		return nil
	}

	dataID := o.DataID
	// 如果不是首版本
	if dataID == 0 {
		os, _, _, err := osi.h.List(c, bktID, o.ID, core.ListOptions{
			Type:  core.OBJ_TYPE_VERSION,
			Count: 1,
			Order: "-id",
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
		d, err = osi.h.GetDataInfo(c, bktID, dataID)
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

	if err = decmpr.Decompress(newDataReader(c, osi.h, bktID, d, osi.cfg.EndecKey), b); err != nil {
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

func (dp *dataPkger) Push(c core.Ctx, h core.Handler, bktID int64, b []byte, d *core.DataInfo) (bool, error) {
	offset := dp.buf.Len()
	if offset+ /*offset%PKG_ALIGN+*/ len(b) > PKG_SIZE || len(dp.infos) >= int(dp.thres) || len(b) >= PKG_SIZE {
		return false, dp.Flush(c, h, bktID)
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

func (dp *dataPkger) Flush(c core.Ctx, h core.Handler, bktID int64) error {
	if dp.buf.Len() <= 0 {
		return nil
	}
	// 上传打包的数据包
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
