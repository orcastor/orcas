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
	"golang.org/x/sync/errgroup"
)

// Re-export constants and types from core package for backward compatibility
// These are now defined in core package, use aliases here
const (
	OFF  = core.REF_LEVEL_OFF  // OFF
	FULL = core.REF_LEVEL_FULL // Read entire file
	FAST = core.REF_LEVEL_FAST // Read entire file after header check succeeds
)

const (
	COVER  = core.CONFLICT_COVER  // Merge or overwrite
	RENAME = core.CONFLICT_RENAME // Rename
	THROW  = core.CONFLICT_THROW  // Throw error
	SKIP   = core.CONFLICT_SKIP   // Skip
)

type OrcasSDK interface {
	H() core.Handler

	Close()

	Login(cfg core.Config) (core.Ctx, *core.UserInfo, []*core.BucketInfo, error)

	Path2ID(c core.Ctx, bktID, pid int64, rpath string) (id int64, err error)
	ID2Path(c core.Ctx, bktID, id int64) (rpath string, err error)

	Upload(c core.Ctx, bktID, pid int64, lpath string) error
	Download(c core.Ctx, bktID, pid int64, lpath string) error
}

type OrcasSDKImpl struct {
	h   core.Handler
	cfg core.Config
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

func (osi *OrcasSDKImpl) Login(cfg core.Config) (core.Ctx, *core.UserInfo, []*core.BucketInfo, error) {
	if cfg.UserName == "" {
		return nil, nil, nil, errors.New(`UserName is empty.`)
	}
	if cfg.Password == "" {
		return nil, nil, nil, errors.New(`Password is empty.`)
	}
	if cfg.PkgThres <= 0 {
		cfg.PkgThres = 1000
	}
	osi.bl = strings.Split(cfg.DontSync, ";")
	if cfg.WorkersN < 16 {
		cfg.WorkersN = 16
	}
	if cfg.WorkersN > 0 {
		osi.f.TuneWorker(int(cfg.WorkersN))
	}
	if cfg.Conflict == core.CONFLICT_RENAME {
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
	// Root is now bucketID, stop when we reach root
	for id != bktID {
		os, err := osi.h.Get(c, bktID, []int64{id})
		if err != nil {
			return "", fmt.Errorf("open remote object error(id:%d): %+v", id, err)
		}
		if len(os) <= 0 {
			return "", fmt.Errorf("remote object does not exist, id:%d", id)
		}
		rpath = filepath.Join(os[0].Name, rpath)
		id = os[0].PID
		// If PID is 0 (legacy data), we've reached root
		if id == 0 {
			break
		}
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
		ID:    core.NewID(),
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
					ID:    core.NewID(),
					PID:   q[0].id,
					MTime: fi.ModTime().Unix(),
					Type:  core.OBJ_TYPE_DIR,
					Name:  fi.Name(),
				})
				continue
			} else {
				file := &core.ObjectInfo{
					ID:    core.NewID(),
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

func (osi *OrcasSDKImpl) Download(c core.Ctx, bktID, id int64, lpath string) (err error) {
	objects, err := osi.h.Get(c, bktID, []int64{id})
	if err != nil {
		return fmt.Errorf("open remote object error(id:%d): %w", id, err)
	}
	if len(objects) == 0 {
		return fmt.Errorf("remote object does not exist, id:%d", id)
	}
	object := objects[0]
	if object.Type == core.OBJ_TYPE_FILE {
		return osi.downloadFile(c, bktID, object, lpath)
	}
	if object.Type != core.OBJ_TYPE_DIR {
		return fmt.Errorf("remote object type error, id:%d type:%d", id, object.Type)
	}
	if err := validateDownloadName(object.Name); err != nil {
		return err
	}
	if err := os.MkdirAll(lpath, 0o700); err != nil {
		return err
	}
	root, err := os.OpenRoot(lpath)
	if err != nil {
		return err
	}
	defer root.Close()
	var downloads errgroup.Group
	downloads.SetLimit(16)
	defer func() {
		if downloadErr := downloads.Wait(); err == nil {
			err = downloadErr
		}
	}()

	q := []elem{{id: id, path: object.Name}}
	var delim string
	for len(q) > 0 {
		if err := root.MkdirAll(q[0].path, 0o700); err != nil {
			return err
		}
		children, _, next, err := osi.h.List(c, bktID, q[0].id, core.ListOptions{Delim: delim, Count: 1000, Order: "type"})
		if err != nil {
			return err
		}
		for _, child := range children {
			if err := validateDownloadName(child.Name); err != nil {
				return err
			}
			if osi.skip(child.Name) {
				continue
			}
			path := filepath.Join(q[0].path, child.Name)
			switch child.Type {
			case core.OBJ_TYPE_DIR:
				q = append(q, elem{id: child.ID, path: path})
			case core.OBJ_TYPE_FILE:
				downloads.Go(func() error { return osi.downloadFileInRoot(c, bktID, child, root, path) })
			}
		}
		if len(children) == 0 {
			q = q[1:]
			delim = ""
		} else {
			delim = next
		}
	}
	return nil
}

// Remote object names are single filesystem components, never local paths.
func validateDownloadName(name string) error {
	if name == "." || !filepath.IsLocal(name) || strings.ContainsAny(name, "/\\:\x00") {
		return fmt.Errorf("unsafe remote object name: %q", name)
	}
	return nil
}
