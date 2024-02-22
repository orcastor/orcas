package sdk

import (
	"context"
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

// 秒传级别设置 对应 Config.RefLevel
const (
	OFF  = iota // OFF
	FULL        // 整个文件读取
	FAST        // 头部检查成功再整个文件读取
)

// 同名冲突解决方式
const (
	COVER  = iota // 合并或覆盖
	RENAME        // 重命名
	THROW         // 报错
	SKIP          // 跳过
)

type Config struct {
	UserName string // 用户名
	Password string // 密码
	DataSync bool   // 断电保护策略(Power-off Protection Policy)，强制每次写入数据后刷到磁盘
	RefLevel uint32 // 秒传级别设置：OFF（默认） / FULL: Ref / FAST: TryRef+Ref
	PkgThres uint32 // 打包个数限制，不设置默认1000个
	WiseCmpr uint32 // 智能压缩，根据文件类型决定是否压缩，取值见core.DATA_CMPR_MASK
	CmprQlty uint32 // 压缩级别，br:[0,11]，gzip:[-3,9]，zstd:[0,10]
	EndecWay uint32 // 加密方式，取值见core.DATA_ENDEC_MASK
	EndecKey string // 加密KEY，SM4需要固定为16个字符，AES256需要大于16个字符
	DontSync string // 不同步的文件名通配符（https://pkg.go.dev/path/filepath#Match），用分号分隔
	Conflict uint32 // 同名冲突解决方式，COVER：合并或覆盖 / RENAME：重命名 / THROW：报错 / SKIP：跳过
	NameTmpl string // 重命名尾巴，"%s的副本"
	WorkersN uint32 // 并发池大小，不小于16
	// ChkPtDir string // 断点续传记录目录，不设置路径默认不开启
}

type OrcasSDK interface {
	H() core.Handler

	Close()

	Login(cfg Config) (core.Ctx, *core.UserInfo, []*core.BucketInfo, error)

	Path2ID(c core.Ctx, bktID, pid int64, rpath string) (id int64, err error)
	ID2Path(c core.Ctx, bktID, id int64) (rpath string, err error)

	Upload(c core.Ctx, bktID, pid int64, lpath string) error
	Download(c core.Ctx, bktID, pid int64, lpath string) error
}

type OrcasSDKImpl struct {
	h   core.Handler
	cfg Config
	bl  []string
	f   *Fanout
}

func New(h core.Handler) OrcasSDK {
	return &OrcasSDKImpl{h: h, f: NewFanout()}
}

func (osi *OrcasSDKImpl) Close() {
	osi.h.Close()
}

func (osi *OrcasSDKImpl) H() core.Handler {
	return osi.h
}

func (osi *OrcasSDKImpl) Login(cfg Config) (core.Ctx, *core.UserInfo, []*core.BucketInfo, error) {
	if cfg.UserName == "" {
		return nil, nil, nil, errors.New(`UserName is empty.`)
	}
	if cfg.Password == "" {
		return nil, nil, nil, errors.New(`Password is empty.`)
	}
	if cfg.PkgThres <= 0 {
		cfg.PkgThres = 1000
	}
	osi.h.SetOptions(core.Options{Sync: cfg.DataSync})
	osi.bl = strings.Split(cfg.DontSync, ";")
	if cfg.WorkersN < 16 {
		cfg.WorkersN = 16
	}
	if cfg.WorkersN > 0 {
		osi.f.TuneWorker(int(cfg.WorkersN))
	}
	if cfg.Conflict == RENAME {
		if !strings.Contains(cfg.NameTmpl, "%s") {
			return nil, nil, nil, errors.New(`cfg.NameTmp should contains "%s".`)
		}
	}
	switch cfg.EndecWay {
	case core.DATA_ENDEC_SM4:
		if len(cfg.EndecKey) != 16 {
			return nil, nil, nil, errors.New(`The length of EndecKey for DATA_ENDEC_SM4 should be 16.`)
		}
	case core.DATA_ENDEC_AES256:
		if len(cfg.EndecKey) <= 16 {
			return nil, nil, nil, errors.New(`The length of EndecKey for DATA_ENDEC_AES256 should be greater than 16.`)
		}
	}
	osi.cfg = cfg
	return osi.h.Login(context.TODO(), cfg.UserName, cfg.Password)
}

func (osi *OrcasSDKImpl) skip(name string) bool {
	for _, v := range osi.bl {
		if ok, _ := filepath.Match(v, name); ok {
			return true
		}
	}
	return false
}

const PathSeparator = "/"

func (osi *OrcasSDKImpl) Path2ID(c core.Ctx, bktID, pid int64, rpath string) (int64, error) {
	for _, child := range strings.Split(rpath, PathSeparator) {
		if child == "" {
			continue
		}
		os, _, _, err := osi.h.List(c, bktID, pid, core.ListOptions{
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

func (osi *OrcasSDKImpl) ID2Path(c core.Ctx, bktID, id int64) (rpath string, err error) {
	for id != core.ROOT_OID {
		os, err := osi.h.Get(c, bktID, []int64{id})
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

type uploadInfo struct {
	path string
	o    *core.ObjectInfo
}

func (osi *OrcasSDKImpl) Upload(c core.Ctx, bktID, pid int64, lpath string) error {
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
		ID:    osi.h.NewID(),
		PID:   pid,
		MTime: fi.ModTime().Unix(),
		Type:  core.OBJ_TYPE_FILE,
		Name:  fi.Name(),
		Size:  fi.Size(),
	}
	if !fi.IsDir() {
		if osi.skip(o.Name) {
			return nil
		}
		if o.Size > 0 {
			return osi.uploadFiles(c,
				bktID,
				[]uploadInfo{{path: filepath.Dir(lpath), o: o}},
				nil, nil, osi.cfg.RefLevel, 0)
		}

		o.DataID = core.EmptyDataID
		if _, err = osi.putObjects(c, bktID, []*core.ObjectInfo{o}); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
		}
		return err
	}

	// 上传目录
	o.Type = core.OBJ_TYPE_DIR
	dirIDs, err := osi.putObjects(c, bktID, []*core.ObjectInfo{o})
	if err != nil || len(dirIDs) <= 0 || dirIDs[0] <= 0 {
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return err
	}

	q := []elem{{id: dirIDs[0], path: lpath}}
	var emptyFiles []*core.ObjectInfo
	var u []uploadInfo

	// 遍历本地目录
	for len(q) > 0 {
		if q[0].path == "" {
			// 路径为空，直接弹出
			q = q[1:]
			continue
		}

		rawFiles, err := ioutil.ReadDir(q[0].path)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}

		if len(rawFiles) <= 0 {
			// 目录为空，直接弹出
			q = q[1:]
			continue
		}

		var dirs []*core.ObjectInfo
		for _, fi := range rawFiles {
			if osi.skip(fi.Name()) {
				continue
			}
			if fi.IsDir() {
				dirs = append(dirs, &core.ObjectInfo{
					ID:    osi.h.NewID(),
					PID:   q[0].id,
					MTime: fi.ModTime().Unix(),
					Type:  core.OBJ_TYPE_DIR,
					Name:  fi.Name(),
				})
				continue
			} else {
				file := &core.ObjectInfo{
					ID:    osi.h.NewID(),
					PID:   q[0].id,
					MTime: fi.ModTime().Unix(),
					Type:  core.OBJ_TYPE_FILE,
					Name:  fi.Name(),
					Size:  fi.Size(),
				}
				if file.Size > 0 {
					u = append(u, uploadInfo{
						path: q[0].path,
						o:    file,
					})
					if len(u) >= int(osi.cfg.PkgThres) {
						var tmpu []uploadInfo
						tmpu, u = u, nil
						osi.f.MustDo(c, func(c core.Ctx) {
							osi.uploadFiles(c, bktID, tmpu, nil, nil, osi.cfg.RefLevel, 0)
						})
					}
				} else {
					file.DataID = core.EmptyDataID
					emptyFiles = append(emptyFiles, file)
				}
			}
		}

		// 异步获取上一级目录的id
		wg := &sync.WaitGroup{}
		dirElems := make([]elem, len(dirs))
		if len(dirs) > 0 { // FIXME：处理目录过多问题
			// 1. 如果是目录， 直接上传
			wg.Add(1)
			go func() {
				defer wg.Done()
				// 上传目录
				ids, err := osi.putObjects(c, bktID, dirs)
				if err != nil {
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
					return
				}
				for i, id := range ids {
					if id > 0 {
						dirElems[i].id = id
						dirElems[i].path = filepath.Join(q[0].path, dirs[i].Name)
					}
				}
			}()
		}

		wg.Wait()
		q = append(q[1:], dirElems...)
	}

	if len(u) > 0 {
		var tmpu []uploadInfo
		tmpu, u = u, nil
		osi.f.MustDo(c, func(c core.Ctx) {
			osi.uploadFiles(c, bktID, tmpu, nil, nil, osi.cfg.RefLevel, 0)
		})
	}

	if len(emptyFiles) > 0 {
		osi.f.MustDo(c, func(c core.Ctx) {
			if _, err := osi.putObjects(c, bktID, emptyFiles); err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
			}
		})
	}
	osi.f.Wait()
	return nil
}

func (osi *OrcasSDKImpl) Download(c core.Ctx, bktID, id int64, lpath string) error {
	o, err := osi.h.Get(c, bktID, []int64{id})
	if err != nil {
		return fmt.Errorf("open remote object error(id:%d): %+v", id, err)
	}
	if len(o) <= 0 {
		return fmt.Errorf("remote object does not exist, id:%d", id)
	}

	if o[0].Type == core.OBJ_TYPE_FILE {
		return osi.downloadFile(c, bktID, o[0], lpath)
	} else if o[0].Type != core.OBJ_TYPE_DIR {
		return fmt.Errorf("remote object type error, id:%d type:%d", id, o[0].Type)
	}

	// 遍历远端目录
	q := []elem{{id: id, path: filepath.Join(lpath, o[0].Name)}}
	var delim string
	for len(q) > 0 {
		os.MkdirAll(q[0].path, 0766)
		o, _, d, err := osi.h.List(c, bktID, q[0].id, core.ListOptions{
			Delim: delim,
			Count: 1000,
			Order: "type",
		})
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}

		for _, x := range o {
			if osi.skip(x.Name) {
				continue
			}
			path := filepath.Join(q[0].path, x.Name)
			switch x.Type {
			case core.OBJ_TYPE_DIR:
				q = append(q, elem{id: x.ID, path: path})
			case core.OBJ_TYPE_FILE:
				f := x
				osi.f.MustDo(c, func(c core.Ctx) {
					osi.downloadFile(c, bktID, f, path)
				})
			}
		}

		if len(o) <= 0 {
			q = q[1:]
			delim = ""
		} else {
			delim = d
		}
	}
	osi.f.Wait()
	return nil
}
