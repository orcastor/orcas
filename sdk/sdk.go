package sdk

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/orcastor/orcas/core"
)

const PathSeparator = "/"
const HdrSize = 102400

type Config struct {
	DataSync bool   // 断电保护策略(Power-off Protection Policy)，强制每次写入数据后刷到磁盘
	RefLevel int    // 0: OFF / 1: Ref / 2: TryRef+Ref
	RefThres int    // 秒传大小限制，不限制默认为0
	PkgThres int    // 打包个数限制，不设置默认50个
	Conflict int    // 同名冲突后，0: Throw / 1: Merge / 2: Rename
	NameTail string // 重命名尾巴，"-副本" / "{\d}"
	WiseCmpr bool   // 智能压缩，根据文件类型决定是否压缩
	ChkPtDir string // 断点续传记录目录，不设置路径默认不开启
	EncKey   string // 加密KEY
	// BEDecmpr bool   // 后端解压，PS：必须是非加密数据
}

type OrcasSDK interface {
	SetConfig(cfg Config)

	GetObjectIDByPath(c core.Ctx, pid int64, path string) (id int64, err error)

	UploadDirByPID(c core.Ctx, pid int64, path string) error
	UploadFileByPID(c core.Ctx, pid int64, path string) error
}

type OrcasSDKImpl struct {
	h   core.Hanlder
	cfg Config
}

func New(h core.Hanlder) OrcasSDK {
	return &OrcasSDKImpl{h: h}
}

func (osi *OrcasSDKImpl) SetConfig(cfg Config) {
	osi.cfg = cfg
}

func (osi *OrcasSDKImpl) GetObjectIDByPath(c core.Ctx, pid int64, path string) (int64, error) {
	for _, child := range strings.Split(path, PathSeparator) {
		if child == "" {
			continue
		}
		os, _, _, err := osi.h.List(c, pid, core.ListOptions{
			Word:  child,
			Count: 1,
			Brief: 2,
		})
		if err != nil {
			return 0, fmt.Errorf("open remote path error(%s): %+v", path, err)
		}
		if len(os) <= 0 {
			return 0, errors.New("open remote path error: " + path)
		}
		pid = os[0].ID
	}
	return pid, nil
}

type Uploader struct {
	tryRefFiles []fs.FileInfo
	refFiles    []fs.FileInfo
	files       []fs.FileInfo
}

func GetHdrCRC32(path string) uint32 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()

	buf := make([]byte, HdrSize)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return 0
	}
	if n == HdrSize {
		return crc32.ChecksumIEEE(buf)
	}
	return crc32.ChecksumIEEE(buf[0:n])
}

func GetHash(path string) (c uint32, m uint64) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0
	}
	defer f.Close()

	x := md5.New()
	buf := make([]byte, 4096)
	for {
		_, err := f.Read(buf)
		c = crc32.Update(c, crc32.IEEETable, buf)
		x.Write(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0
		}
	}
	m = binary.LittleEndian.Uint64(x.Sum(nil)[4:12])
	return
}

func (osi *OrcasSDKImpl) UploadDirByPID(c core.Ctx, pid int64, path string) error {
	type Elem struct {
		pid  chan int64
		path string
	}

	e := Elem{pid: make(chan int64), path: path}
	e.pid <- pid

	// 层序遍历
	q := []Elem{e}
	// 遍历本地目录
	for len(q) > 0 {
		f, err := ioutil.ReadDir(q[0].path)
		if err != nil {
			// TODO
			return err
		}

		defer close(q[0].pid)

		if len(f) <= 0 {
			// 目录为空，直接弹出
			q = q[1:]
			continue
		}

		var dirs []*core.ObjectInfo
		var files []*core.ObjectInfo
		for _, file := range f {
			if file.IsDir() {
				dirs = append(dirs, &core.ObjectInfo{
					MTime:  file.ModTime().Unix(),
					Type:   core.OBJ_TYPE_DIR,
					Status: core.OBJ_NORMAL,
					Name:   file.Name(),
				})
				continue
			} else {
				files = append(files, &core.ObjectInfo{
					MTime:  file.ModTime().Unix(),
					Type:   core.OBJ_TYPE_FILE,
					Status: core.OBJ_NORMAL,
					Name:   file.Name(),
					Size:   file.Size(),
				})
			}
		}

		// 异步获取上一级目录的id
		pid := <-q[0].pid
		dirElems := make([]Elem, len(dirs))
		if len(dirs) > 0 {
			// 1. 如果是目录， 直接上传
			for i := range dirs {
				dirs[i].PID = pid
				dirElems[i].path = filepath.Join(path, dirs[i].Name)
			}

			go func() {
				// 上传目录
				ids, err := osi.h.Put(c, dirs)
				if err != nil {
					// TODO: 怎么处理
					return
				}
				for i, id := range ids {
					if id > 0 {
						dirElems[i].pid <- id
					} else {
						// TODO: 重名冲突
					}
				}
			}()
		}

		if len(files) > 0 {
			osi.uploadFiles(c, pid, q[0].path, files)
		}

		q = append(q[1:], dirElems...)
	}
	return nil
}

func (osi *OrcasSDKImpl) uploadFiles(c core.Ctx, pid int64, path string, files []*core.ObjectInfo) error {
	var o, s []*core.ObjectInfo
	var d []*core.DataInfo
	// 2. 如果是文件，先看是否要秒传
	switch osi.cfg.RefLevel {
	case 2:
		// 3. 如果要预先秒传的，先读取hdrCrc32，排队检查
		var pd []*core.DataInfo
		for _, fi := range files {
			pd = append(pd, &core.DataInfo{
				OrigSize: fi.Size,
				HdrCRC32: GetHdrCRC32(filepath.Join(path, fi.Name)),
			})
		}
		ids, err := osi.h.Ref(c, pd)
		if err != nil {
			// TODO: 处理错误情况
		}
		for i, id := range ids {
			if id > 0 {
				// 继续尝试秒传
				pd[i].CRC32, pd[i].MD5 = GetHash(filepath.Join(path, files[i].Name))
				d = append(d, pd[i])
			} else {
				// 扔到待上传队列
				o = append(o, files[i])
			}
		}
		files = []*core.ObjectInfo{} // 清空，防止下去以后重复处理
		fallthrough
	case 1:
		// 4. 如果不需要预先秒传或者预先秒传失败的，整个读取crc32和md5以后尝试秒传
		for _, fi := range files {
			fpath := filepath.Join(path, fi.Name)
			c, m := GetHash(fpath)
			d = append(d, &core.DataInfo{
				OrigSize: fi.Size,
				HdrCRC32: GetHdrCRC32(fpath),
				CRC32:    c,
				MD5:      m,
			})
		}
		ids, err := osi.h.Ref(c, d)
		if err != nil {
			// TODO: 处理错误情况
		}
		// 设置DataID，如果是有的，说明秒传成功，不再需要上传了
		for i, id := range ids {
			if id > 0 {
				files[i].DataID = id
				s = append(s, files[i])
			} else {
				o = append(o, files[i])
			}
		}
		// 5. 秒传失败的（包括超过大小或者预先秒传失败），丢到待上传对列
		files = []*core.ObjectInfo{} // 清空，防止下去以后重复处理
		fallthrough
	case 0:
		// 丢到待上传
		o, files = append(o, files...), []*core.ObjectInfo{} // 清空，防止下去以后重复处理
	}

	// 6. 如果开启智能压缩的，检查文件类型确定是否要压缩
	// 7. 检查是否要打包，不要打包的直接上传，打包默认一次不超过50个，大小不超过4MB
	return nil
}

func (osi *OrcasSDKImpl) UploadFileByPID(c core.Ctx, pid int64, path string) error {
	return nil
}

const (
	PKG_ALIGN = 8192
	PKG_SIZE  = 4194304
)

type DataPkg struct {
	buf     []byte
	offsets []int
	thres   int
}

func NewDataPkg(thres int) *DataPkg {
	return &DataPkg{
		buf:   make([]byte, 0, PKG_SIZE),
		thres: thres,
	}
}

func (dp *DataPkg) Push(b []byte) bool {
	offset := len(dp.buf)
	if offset+len(b) > PKG_SIZE || len(dp.offsets) >= dp.thres {
		return false
	}
	// 填充内容
	dp.buf = append(dp.buf, b...)
	// 记录偏移
	dp.offsets = append(dp.offsets, offset)
	// 处理对齐
	padding := PKG_ALIGN - len(b)%PKG_ALIGN
	if padding > 0 {
		dp.buf = append(dp.buf, make([]byte, padding)...)
	}
	return true
}

func (dp *DataPkg) GetOffsets() []int {
	return dp.offsets
}
