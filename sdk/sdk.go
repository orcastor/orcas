package sdk

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/orcastor/orcas/core"
)

const PathSeparator = "/"

const (
	OFF  = iota // OFF
	FULL        // 整个文件读取
	FAST        // 头部检查成功再整个文件读取
)

type Config struct {
	DataSync bool   // 断电保护策略(Power-off Protection Policy)，强制每次写入数据后刷到磁盘
	RefLevel uint32 // 0: OFF（默认） / 1: Ref / 2: TryRef+Ref
	PkgThres uint32 // 打包个数限制，不设置默认50个
	WiseCmpr uint32 // 智能压缩，根据文件类型决定是否压缩，选择压缩算法， 取值见core.DATA_CMPR_MASK
	EndecWay uint32 // 加密方式，取值见core.DATA_ENDEC_MASK
	EndecKey string // 加密KEY
	DontSync string // 不同步的文件名通配符（https://pkg.go.dev/path/filepath#Match），用分号分隔
	// Conflict uint32 // 同名冲突后，0: Merge or Cover（默认） / 1: Throw / 2: Rename / 3: Skip
	// NameTail string // 重命名尾巴，"-副本" / "{\d}"
	// ChkPtDir string // 断点续传记录目录，不设置路径默认不开启
	// BEDecmpr bool   // 后端解压，PS：必须是非加密数据
}

type OrcasSDK interface {
	SetConfig(cfg Config)

	Path2ID(c core.Ctx, pid int64, rpath string) (id int64, err error)
	ID2Path(c core.Ctx, id int64) (rpath string, err error)

	Upload(c core.Ctx, pid int64, lpath string) error
	Download(c core.Ctx, pid int64, lpath string) error
}

type OrcasSDKImpl struct {
	h   core.Handler
	cfg Config
	dp  *dataPkger
	bl  []string
}

func New(h core.Handler) OrcasSDK {
	return &OrcasSDKImpl{h: h, dp: newDataPkger(50)}
}

func (osi *OrcasSDKImpl) SetConfig(cfg Config) {
	osi.cfg = cfg
	if cfg.PkgThres > 0 {
		osi.dp.SetThres(cfg.PkgThres)
	}
	if cfg.DataSync {
		osi.h.SetOptions(core.Options{Sync: true})
	}
	if cfg.DontSync != "" {
		osi.bl = strings.Split(cfg.DontSync, ";")
	}
}

func (osi *OrcasSDKImpl) skip(name string) bool {
	for _, v := range osi.bl {
		if ok, _ := filepath.Match(v, name); ok {
			return true
		}
	}
	return false
}

func (osi *OrcasSDKImpl) Path2ID(c core.Ctx, pid int64, rpath string) (int64, error) {
	for _, child := range strings.Split(rpath, PathSeparator) {
		if child == "" {
			continue
		}
		os, _, _, err := osi.h.List(c, pid, core.ListOptions{
			Word:  child,
			Count: 1,
			Brief: 2,
		})
		if err != nil {
			return 0, fmt.Errorf("open remote path error(path:%s): %+v", rpath, err)
		}
		if len(os) <= 0 {
			return 0, errors.New("remote path does not exist, path:" + rpath)
		}
		pid = os[0].ID
	}
	return pid, nil
}

func (osi *OrcasSDKImpl) ID2Path(c core.Ctx, id int64) (rpath string, err error) {
	for id != core.ROOT_OID {
		os, err := osi.h.Get(c, []int64{id})
		if err != nil {
			return "", fmt.Errorf("open remote object error(id:%d): %+v", id, err)
		}
		if len(os) <= 0 {
			return "", fmt.Errorf("remote object does not exist, id:%d", id)
		}
		rpath = filepath.Join(os[0].Name, rpath)
		id = os[0].PID
	}
	return rpath, nil
}

// 层序遍历
type elem struct {
	id   int64
	path string
}

func (osi *OrcasSDKImpl) Upload(c core.Ctx, pid int64, lpath string) error {
	f, err := os.Open(lpath)
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	f.Close()

	o := &core.ObjectInfo{
		PID:    pid,
		MTime:  fi.ModTime().Unix(),
		Type:   core.OBJ_TYPE_FILE,
		Status: core.OBJ_NORMAL,
		Name:   fi.Name(),
		Size:   fi.Size(),
	}
	if !fi.IsDir() {
		if osi.skip(o.Name) {
			return nil
		}
		if o.Size > 0 {
			return osi.uploadFiles(c,
				filepath.Dir(lpath),
				[]*core.ObjectInfo{o},
				[]*core.DataInfo{&core.DataInfo{
					OrigSize: fi.Size(),
				}}, osi.cfg.RefLevel, 0)
		}

		o.DataID = core.EmptyDataID
		if _, err = osi.h.Put(c, []*core.ObjectInfo{o}); err != nil {
			// TODO: 重名冲突
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
		}
		return err
	}

	// 上传目录
	o.Type = core.OBJ_TYPE_DIR
	dirIDs, err := osi.h.Put(c, []*core.ObjectInfo{o})
	if err != nil || len(dirIDs) <= 0 || dirIDs[0] <= 0 {
		// TODO: 怎么处理
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return err
	}

	q := []elem{elem{id: dirIDs[0], path: lpath}}
	var emptyFiles []*core.ObjectInfo

	// 遍历本地目录
	for len(q) > 0 {
		rawFiles, err := ioutil.ReadDir(q[0].path)
		if err != nil {
			// TODO
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}

		if len(rawFiles) <= 0 {
			// 目录为空，直接弹出
			q = q[1:]
			continue
		}

		var dirs, files []*core.ObjectInfo
		for _, fi := range rawFiles {
			if osi.skip(fi.Name()) {
				continue
			}
			if fi.IsDir() {
				dirs = append(dirs, &core.ObjectInfo{
					PID:    q[0].id,
					MTime:  fi.ModTime().Unix(),
					Type:   core.OBJ_TYPE_DIR,
					Status: core.OBJ_NORMAL,
					Name:   fi.Name(),
				})
				continue
			} else {
				file := &core.ObjectInfo{
					PID:    q[0].id,
					MTime:  fi.ModTime().Unix(),
					Type:   core.OBJ_TYPE_FILE,
					Status: core.OBJ_NORMAL,
					Name:   fi.Name(),
					Size:   fi.Size(),
				}
				if file.Size > 0 {
					files = append(files, file)
				} else {
					file.DataID = core.EmptyDataID
					emptyFiles = append(emptyFiles, file)
				}
			}
		}

		// 异步获取上一级目录的id
		wg := &sync.WaitGroup{}
		dirElems := make([]elem, len(dirs))
		if len(dirs) > 0 {
			// 1. 如果是目录， 直接上传
			wg.Add(1)
			go func() {
				defer wg.Done()
				// 上传目录
				ids, err := osi.h.Put(c, dirs)
				if err != nil {
					// TODO: 怎么处理
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
					return
				}
				for i, id := range ids {
					if id > 0 {
						dirElems[i].id = id
					} else {
						// TODO: 重名冲突
					}
					dirElems[i].path = filepath.Join(q[0].path, dirs[i].Name)
				}
			}()
		}

		if len(files) > 0 {
			osi.uploadFiles(c, q[0].path, files, nil, osi.cfg.RefLevel, 0)
		}

		wg.Wait()
		q = append(q[1:], dirElems...)
	}

	if _, err := osi.h.Put(c, emptyFiles); err != nil {
		// TODO: 重名冲突
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return err
	}
	return nil
}

func (osi *OrcasSDKImpl) Download(c core.Ctx, id int64, lpath string) error {
	o, err := osi.h.Get(c, []int64{id})
	if err != nil {
		return fmt.Errorf("open remote object error(id:%d): %+v", id, err)
	}
	if len(o) <= 0 {
		return fmt.Errorf("remote object does not exist, id:%d", id)
	}

	if o[0].Type == core.OBJ_TYPE_FILE {
		return osi.downloadFile(c, o[0], lpath)
	} else if o[0].Type != core.OBJ_TYPE_DIR {
		return fmt.Errorf("remote object type error, id:%d type:%d", id, o[0].Type)
	}

	// 遍历远端目录
	q := []elem{elem{id: id, path: filepath.Join(lpath, o[0].Name)}}
	var delim string
	wg := &sync.WaitGroup{}
	for len(q) > 0 {
		os.MkdirAll(q[0].path, 0766)
		o, _, d, err := osi.h.List(c, q[0].id, core.ListOptions{
			Delim: delim,
			Count: 100,
			Order: "type",
		})
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}

		for _, o := range o {
			if osi.skip(o.Name) {
				continue
			}
			path := filepath.Join(q[0].path, o.Name)
			switch o.Type {
			case core.OBJ_TYPE_DIR:
				q = append(q, elem{id: o.ID, path: path})
			case core.OBJ_TYPE_FILE:
				n := o
				wg.Add(1)
				go func() {
					defer wg.Done()
					osi.downloadFile(c, n, path)
				}()
			}
		}

		if len(o) <= 0 {
			q = q[1:]
			delim = ""
		} else {
			delim = d
		}
	}
	wg.Wait()
	return nil
}
