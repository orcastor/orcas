package sdk

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/orcastor/orcas/core"
)

const PathSeparator = "/"
const HdrSize = 102400

const (
	NO_REF = iota
	REF
	TRY_REF
)

type Config struct {
	DataSync bool   // 断电保护策略(Power-off Protection Policy)，强制每次写入数据后刷到磁盘
	RefLevel int    // 0: OFF / 1: Ref / 2: TryRef+Ref
	RefThres int    // 秒传大小限制，不限制默认为0
	PkgThres int    // 打包个数限制，不设置默认50个
	Conflict int    // 同名冲突后，0: Merge / 1: Throw / 2: Rename
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
	dp  *dataPkg
}

func New(h core.Hanlder) OrcasSDK {
	return &OrcasSDKImpl{h: h, dp: newDataPkg(50)}
}

func (osi *OrcasSDKImpl) SetConfig(cfg Config) {
	osi.cfg = cfg
	if cfg.PkgThres > 0 {
		osi.dp.SetThres(cfg.PkgThres)
	}
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

func (osi *OrcasSDKImpl) UploadDirByPID(c core.Ctx, pid int64, path string) error {
	type Elem struct {
		chPID chan int64
		path  string
	}

	e := Elem{chPID: make(chan int64), path: path}
	e.chPID <- pid

	// 层序遍历
	q := []Elem{e}
	// 遍历本地目录
	for len(q) > 0 {
		f, err := ioutil.ReadDir(q[0].path)
		if err != nil {
			// TODO
			return err
		}

		defer close(q[0].chPID)

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
		pid := <-q[0].chPID
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
						dirElems[i].chPID <- id
					} else {
						// TODO: 重名冲突
					}
				}
			}()
		}

		if len(files) > 0 {
			di := make([]*core.DataInfo, len(files))
			for i := range files {
				di[i].OrigSize = files[i].Size
			}
			osi.uploadFiles(c, pid, q[0].path, files, di, osi.cfg.RefLevel, 0)
		}

		q = append(q[1:], dirElems...)
	}
	return nil
}

func (osi *OrcasSDKImpl) uploadFiles(c core.Ctx, pid int64, path string, files []*core.ObjectInfo, di []*core.DataInfo, level, action int) error {
	var files1, files2 []*core.ObjectInfo
	var di1, di2 []*core.DataInfo

	// 2. 如果是文件，先看是否要秒传
	switch level {
	case TRY_REF:
		// 3. 如果要预先秒传的，先读取hdrCrc32，排队检查
		for i, fi := range files {
			if err := osi.readFile(c, path, fi,
				newListener(di[i], HDR_CRC32&^action).Once()); err != nil {
				// TODO: 处理错误情况
			}
		}
		ids, err := osi.h.Ref(c, di)
		if err != nil {
			// TODO: 处理错误情况
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
			return err
		}
		if err := osi.uploadFiles(c, pid, path, files2, di2, NO_REF, action|HDR_CRC32); err != nil {
			return err
		}
	case REF:
		// 4. 如果不需要预先秒传或者预先秒传失败的，整个读取crc32和md5以后尝试秒传
		for i, fi := range files {
			if err := osi.readFile(c, path, fi,
				newListener(di[i], (HDR_CRC32|CRC32_MD5)&^action)); err != nil {
				// TODO: 处理错误情况
			}
		}
		ids, err := osi.h.Ref(c, di)
		if err != nil {
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
			return err
		}
		// 5. 秒传失败的（包括超过大小或者预先秒传失败），丢到待上传对列
		if err := osi.uploadFiles(c, pid, path, files2, di2, NO_REF, action|HDR_CRC32|CRC32_MD5); err != nil {
			return err
		}
	case NO_REF:
		// 直接上传的对象
		for i, fi := range files {
			if err := osi.readFile(c, path, fi,
				newListener(di[i], (UPLOAD_DATA|HDR_CRC32|CRC32_MD5)&^action)); err != nil {
				// TODO: 处理错误情况
			}
		}
		ids, err := osi.h.PutDataInfo(c, di)
		if err != nil {
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
			return err
		}
	}
	return nil
}

func (osi *OrcasSDKImpl) UploadFileByPID(c core.Ctx, pid int64, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	return osi.uploadFiles(c, pid, filepath.Dir(path), []*core.ObjectInfo{&core.ObjectInfo{
		MTime:  fi.ModTime().Unix(),
		Type:   core.OBJ_TYPE_FILE,
		Status: core.OBJ_NORMAL,
		Name:   fi.Name(),
		Size:   fi.Size(),
	}}, []*core.DataInfo{&core.DataInfo{
		OrigSize: fi.Size(),
	}}, osi.cfg.RefLevel, 0)
}
