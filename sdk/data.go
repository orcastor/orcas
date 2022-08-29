package sdk

import (
	"crypto/md5"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/orcastor/orcas/core"
)

const (
	UPLOAD_DATA = iota >> 1
	CRC32_MD5
	HDR_CRC32
)

const (
	PKG_ALIGN = 8192
	PKG_SIZE  = 4194304
)

type listener struct {
	d       *core.DataInfo
	action  int
	md5Hash hash.Hash
	once    bool
}

func newListener(d *core.DataInfo, action int) *listener {
	hl := &listener{d: d, action: action}
	if action&CRC32_MD5 != 0 {
		hl.md5Hash = md5.New()
	}
	return hl
}

func (hl *listener) Once() *listener {
	hl.once = true
	return hl
}

func (hl *listener) OnData(c core.Ctx, h core.Hanlder, dp *dataPkg, sn int, buf []byte) (once bool, err error) {
	if hl.action&HDR_CRC32 != 0 && sn == 0 {
		if len(buf) > HdrSize {
			hl.d.HdrCRC32 = crc32.ChecksumIEEE(buf[0:HdrSize])
		} else {
			hl.d.HdrCRC32 = crc32.ChecksumIEEE(buf)
		}
	}
	if hl.action&CRC32_MD5 != 0 {
		hl.d.CRC32 = crc32.Update(hl.d.CRC32, crc32.IEEETable, buf)
		hl.md5Hash.Write(buf)
	}
	// 上传数据
	if hl.action&UPLOAD_DATA != 0 {
		// 6. 如果开启智能压缩的，检查文件类型确定是否要压缩
		if sn != 0 || !dp.Push(buf, hl.d) {
			// 7. 检查是否要打包，不要打包的直接上传，打包默认一次不超过50个，大小不超过4MB
			if err = dp.Flush(c, h); err != nil {
				return
			}
			if hl.d.ID, err = h.PutData(c, hl.d.ID, sn, buf); err != nil {
				return
			}
		}
	}
	return hl.once, nil
}

func (hl *listener) Finish(c core.Ctx, h core.Hanlder) error {
	if hl.action&CRC32_MD5 != 0 {
		hl.d.MD5 = binary.LittleEndian.Uint64(hl.md5Hash.Sum(nil)[4:12])
	}
	return nil
}

func (osi *OrcasSDKImpl) readFile(c core.Ctx, path string, oi *core.ObjectInfo, l *listener) error {
	f, err := os.Open(filepath.Join(path, oi.Name))
	if err != nil {
		return err
	}
	defer f.Close()

	sn := 0
	buf := make([]byte, PKG_SIZE)
	for {
		_, err := f.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		once, err := l.OnData(c, osi.h, osi.dp, sn, buf)
		if err != nil {
			return err
		}
		if once {
			break
		}
		sn++
	}
	err = l.Finish(c, osi.h)
	return err
}

type dataPkg struct {
	buf   []byte
	infos []*core.DataInfo
	thres int
}

func newDataPkg(thres int) *dataPkg {
	return &dataPkg{
		buf:   make([]byte, 0, PKG_SIZE),
		thres: thres,
	}
}

func (dp *dataPkg) SetThres(thres int) {
	dp.thres = thres
}

func (dp *dataPkg) Push(b []byte, d *core.DataInfo) bool {
	offset := len(dp.buf)
	if offset+len(b) > PKG_SIZE || len(dp.infos) >= dp.thres {
		return false
	}
	// 填充内容
	dp.buf = append(dp.buf, b...)
	// 记录偏移
	d.PkgOffset = offset
	// 记录下来要设置打包数据的数据信息
	dp.infos = append(dp.infos, d)
	// 处理对齐
	if padding := PKG_ALIGN - len(b)%PKG_ALIGN; padding > 0 {
		dp.buf = append(dp.buf, make([]byte, padding)...)
	}
	return true
}

func (dp *dataPkg) Flush(c core.Ctx, h core.Hanlder) error {
	if len(dp.buf) <= 0 {
		return nil
	}
	// 上传打包的数据包
	pkgID, err := h.PutData(c, 0, 0, dp.buf)
	if err != nil {
		return err
	}
	for i := range dp.infos {
		dp.infos[i].PkgID = pkgID
	}
	dp.buf, dp.infos = make([]byte, 0, PKG_SIZE), []*core.DataInfo{}
	return nil
}
