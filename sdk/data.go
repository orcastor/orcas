package sdk

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/DataDog/zstd"
	"github.com/golang/snappy"
	"github.com/h2non/filetype"
	"github.com/orcastor/orcas/core"
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
	PKG_ALIGN = 8192
	PKG_SIZE  = 4194304
)

type listener struct {
	d            *core.DataInfo
	action, cmpr uint32
	md5Hash      hash.Hash
	once         bool
	sn, cnt      int
	cmprBuf      []byte
}

func newListener(d *core.DataInfo, action, cmpr uint32) *listener {
	hl := &listener{d: d, action: action, cmpr: cmpr}
	if action&CRC32_MD5 != 0 {
		hl.md5Hash = md5.New()
	}
	return hl
}

func (hl *listener) Once() *listener {
	hl.once = true
	return hl
}

func (hl *listener) OnData(c core.Ctx, h core.Hanlder, dp *dataPkg, buf []byte) (once bool, err error) {
	if hl.action&HDR_CRC32 != 0 && hl.cnt == 0 {
		if len(buf) > HdrSize {
			hl.d.HdrCRC32 = crc32.ChecksumIEEE(buf[0:HdrSize])
		} else {
			hl.d.HdrCRC32 = crc32.ChecksumIEEE(buf)
		}
		// 6. 如果开启智能压缩的，检查文件类型确定是否要压缩
		kind, _ := filetype.Match(buf)
		if CmprBlacklist[kind.MIME.Value] == 0 {
			hl.d.Kind |= hl.cmpr
		}
		// fmt.Println(kind.MIME.Value)
	}
	if hl.action&CRC32_MD5 != 0 {
		hl.d.CRC32 = crc32.Update(hl.d.CRC32, crc32.IEEETable, buf)
		hl.md5Hash.Write(buf)
	}
	// 上传数据
	if hl.action&UPLOAD_DATA != 0 {
		cmprBuf := buf
		if hl.d.Kind&core.DATA_CMPR_MASK != 0 {
			if hl.d.Kind&core.DATA_CMPR_SNAPPY != 0 {
				cmprBuf = snappy.Encode(nil, buf)
			} else if hl.d.Kind&core.DATA_CMPR_ZSTD != 0 {
				cmprBuf, err = zstd.Compress(nil, buf)
			} else if hl.d.Kind&core.DATA_CMPR_GZIP != 0 {
				var b bytes.Buffer
				w := gzip.NewWriter(&b)
				w.Write(buf)
				w.Close()
				if n, err1 := b.Read(buf); err1 == nil {
					cmprBuf = buf[0:n]
				}
			}
			if err != nil {
				// FIXME：压缩失败就用原始的？
			}
			// 如果压缩后更大了，恢复原始的
			if hl.cnt == 0 && len(buf) < PKG_SIZE && len(cmprBuf) >= len(buf) {
				hl.d.Kind &= ^core.DATA_CMPR_MASK
				cmprBuf = buf
			}
		}

		toUpload := cmprBuf
		if hl.cnt == 0 && len(buf) < PKG_SIZE {
			// 7. 检查是否要打包，不要打包的直接上传，打包默认一次不超过50个，大小不超过4MB
			if dp.Push(toUpload, hl.d) {
				toUpload = nil
			} else {
				if err = dp.Flush(c, h); err != nil {
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
					return hl.once, err
				}
			}
		} else {
			if hl.d.Kind&core.DATA_CMPR_MASK != 0 {
				hl.cmprBuf = append(hl.cmprBuf, cmprBuf...)
				if len(hl.cmprBuf) >= PKG_SIZE {
					toUpload, hl.cmprBuf = hl.cmprBuf[0:PKG_SIZE], hl.cmprBuf[PKG_SIZE:]
				} else {
					toUpload = nil
				}
			}
		}

		if len(toUpload) > 0 {
			// 需要上传
			if hl.d.ID, err = h.PutData(c, hl.d.ID, hl.sn, toUpload); err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
				return hl.once, err
			}
			hl.sn++
		}

		if hl.d.Kind&core.DATA_CMPR_MASK != 0 {
			hl.d.Checksum = crc32.Update(hl.d.Checksum, crc32.IEEETable, cmprBuf)
			hl.d.Size += int64(len(cmprBuf))
		}
	}
	hl.cnt++
	return hl.once, nil
}

func (hl *listener) OnFinish(c core.Ctx, h core.Hanlder) (err error) {
	if hl.action&CRC32_MD5 != 0 {
		hl.d.MD5 = fmt.Sprintf("%x", hl.md5Hash.Sum(nil)[4:12])
	}
	if len(hl.cmprBuf) > 0 {
		if hl.d.ID, err = h.PutData(c, hl.d.ID, hl.sn, hl.cmprBuf); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		hl.d.Checksum = crc32.Update(hl.d.Checksum, crc32.IEEETable, hl.cmprBuf)
		hl.d.Size += int64(len(hl.cmprBuf))
	}
	if hl.d.Kind&core.DATA_CMPR_MASK == 0 {
		hl.d.Checksum = hl.d.CRC32
		hl.d.Size = hl.d.OrigSize
	}
	return nil
}

func (osi *OrcasSDKImpl) readFile(c core.Ctx, path string, l *listener) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, PKG_SIZE)
	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		once, err1 := l.OnData(c, osi.h, osi.dp, buf[0:n])
		if err1 != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err1
		}
		if once {
			break
		}
		if err == io.EOF {
			break
		}
	}
	err = l.OnFinish(c, osi.h)
	return err
}

func (osi *OrcasSDKImpl) uploadFiles(c core.Ctx, pid int64, path string, files []*core.ObjectInfo, di []*core.DataInfo, level, action uint32) error {
	if len(files) <= 0 {
		return nil
	}

	var files1, files2 []*core.ObjectInfo
	var di1, di2 []*core.DataInfo

	// 2. 如果是文件，先看是否要秒传
	switch level {
	case TRY_REF:
		// 3. 如果要预先秒传的，先读取hdrCrc32，排队检查
		for i, fi := range files {
			if err := osi.readFile(c, filepath.Join(path, fi.Name),
				newListener(di[i], HDR_CRC32&^action, osi.cfg.WiseCmpr).Once()); err != nil {
				// TODO: 处理错误情况
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)

			}
		}
		ids, err := osi.h.Ref(c, di)
		if err != nil {
			// TODO: 处理错误情况
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
		}
		for i, id := range ids {
			if id > 0 {
				files1 = append(files1, files[i])
				di1 = append(di1, di[i])
			} else {
				files2 = append(files2, files[i])
				di2 = append(di2, di[i])
			}
		}
		if err := osi.uploadFiles(c, pid, path, files1, di1, REF, action|HDR_CRC32); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		if err := osi.uploadFiles(c, pid, path, files2, di2, NO_REF, action|HDR_CRC32); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case REF:
		// 4. 如果不需要预先秒传或者预先秒传失败的，整个读取crc32和md5以后尝试秒传
		for i, fi := range files {
			if err := osi.readFile(c, filepath.Join(path, fi.Name),
				newListener(di[i], (HDR_CRC32|CRC32_MD5)&^action, osi.cfg.WiseCmpr)); err != nil {
				// TODO: 处理错误情况
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)

			}
		}
		ids, err := osi.h.Ref(c, di)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			// TODO: 处理错误情况
		}
		// 设置DataID，如果是有的，说明秒传成功，不再需要上传数据了
		for i, id := range ids {
			if id > 0 {
				files[i].DataID = id
				files1 = append(files1, files[i])
			} else {
				files2 = append(files2, files[i])
				di2 = append(di2, di[i])
			}
		}
		if _, err := osi.h.Put(c, files1); err != nil {
			// TODO: 重名冲突
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		// 5. 秒传失败的（包括超过大小或者预先秒传失败），丢到待上传对列
		if err := osi.uploadFiles(c, pid, path, files2, di2, NO_REF, action|HDR_CRC32|CRC32_MD5); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case NO_REF:
		// 直接上传的对象
		for i, fi := range files {
			if err := osi.readFile(c, filepath.Join(path, fi.Name),
				newListener(di[i], (UPLOAD_DATA|HDR_CRC32|CRC32_MD5)&^action, osi.cfg.WiseCmpr)); err != nil {
				// TODO: 处理错误情况
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
			}
		}
		// 刷新一下打包数据
		if err := osi.dp.Flush(c, osi.h); err != nil {
			// TODO: 处理错误情况
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		ids, err := osi.h.PutDataInfo(c, di)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		// 处理打包上传的对象
		for i, id := range ids {
			if files[i].DataID != id {
				files[i].DataID = id
			}
		}
		if _, err := osi.h.Put(c, files); err != nil {
			// TODO: 重名冲突
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	}
	return nil
}

type dataPkg struct {
	buf   []byte
	infos []*core.DataInfo
	thres uint32
}

func newDataPkg(thres uint32) *dataPkg {
	return &dataPkg{
		buf:   make([]byte, 0, PKG_SIZE),
		thres: thres,
	}
}

func (dp *dataPkg) SetThres(thres uint32) {
	dp.thres = thres
}

func (dp *dataPkg) Push(b []byte, d *core.DataInfo) bool {
	offset := len(dp.buf)
	if offset+ /*offset%PKG_ALIGN+*/ len(b) > PKG_SIZE || len(dp.infos) >= int(dp.thres) || len(b) == PKG_SIZE {
		return false
	}
	// 写入前再处理对齐，最后一块就不用补齐了，PS：需要测试一下读性能差多少
	/*if offset%PKG_ALIGN > 0 {
		if padding := PKG_ALIGN - offset%PKG_ALIGN; padding > 0 {
			dp.buf = append(dp.buf, make([]byte, padding)...)
			offset = len(dp.buf)
		}
	}*/
	// 填充内容
	dp.buf = append(dp.buf, b...)
	// 记录偏移
	d.PkgOffset = offset
	// 记录下来要设置打包数据的数据信息
	dp.infos = append(dp.infos, d)
	return true
}

func (dp *dataPkg) Flush(c core.Ctx, h core.Hanlder) error {
	if len(dp.buf) <= 0 {
		return nil
	}
	// 上传打包的数据包
	pkgID, err := h.PutData(c, 0, 0, dp.buf)
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
	dp.buf, dp.infos = make([]byte, 0, PKG_SIZE), []*core.DataInfo{}
	return nil
}
