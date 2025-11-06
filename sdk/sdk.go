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

// Instant upload level setting, corresponds to Config.RefLevel
const (
	OFF  = iota // OFF
	FULL        // Read entire file
	FAST        // Read entire file after header check succeeds
)

// Conflict resolution for same name
const (
	COVER  = iota // Merge or overwrite
	RENAME        // Rename
	THROW         // Throw error
	SKIP          // Skip
)

type Config struct {
	UserName string // Username
	Password string // Password
	DataSync bool   // Power failure protection policy, force flush to disk after each data write
	RefLevel uint32 // Instant upload level setting: OFF (default) / FULL: Ref / FAST: TryRef+Ref
	PkgThres uint32 // Package count limit, default 1000 if not set
	WiseCmpr uint32 // Smart compression, decide whether to compress based on file type, see core.DATA_CMPR_MASK
	CmprQlty uint32 // Compression level, br:[0,11], gzip:[-3,9], zstd:[0,10]
	EndecWay uint32 // Encryption method, see core.DATA_ENDEC_MASK
	EndecKey string // Encryption KEY, SM4 requires exactly 16 characters, AES256 requires more than 16 characters
	DontSync string // Filename wildcards to exclude from sync (https://pkg.go.dev/path/filepath#Match), separated by semicolons
	Conflict uint32 // Conflict resolution for same name, COVER: merge or overwrite / RENAME: rename / THROW: throw error / SKIP: skip
	NameTmpl string // Rename suffix, "%s的副本"
	WorkersN uint32 // Concurrent pool size, not less than 16
	// ChkPtDir string // Checkpoint directory for resume, not enabled if path not set
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

// Level-order traversal
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

	// Upload directory
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

	// Traverse local directory
	for len(q) > 0 {
		if q[0].path == "" {
			// Path is empty, pop directly
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
			// Directory is empty, pop directly
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

		// Asynchronously get parent directory id
		wg := &sync.WaitGroup{}
		dirElems := make([]elem, len(dirs))
		if len(dirs) > 0 { // FIXME: handle too many directories issue
			// 1. If it's a directory, upload directly
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Upload directory
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

	// Traverse remote directory
	q := []elem{{id: id, path: filepath.Join(lpath, o[0].Name)}}
	var delim string
	for len(q) > 0 {
		os.MkdirAll(q[0].path, 0o766)
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
