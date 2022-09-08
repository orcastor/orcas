package core

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/orca-zhang/ecache"
)

type Options struct {
	Sync bool
}

type DataAdapter interface {
	SetOptions(opt Options)
	Close()

	Write(c Ctx, bktID, dataID int64, sn int, buf []byte) error
	Flush(c Ctx, bktID, dataID int64) error

	Read(c Ctx, bktID, dataID int64, sn int) ([]byte, error)
	ReadBytes(c Ctx, bktID, dataID int64, sn, offset, size int) ([]byte, error)

	FileSize(c Ctx, bktID, dataID int64, sn int) (int64, error)
}

const interval = time.Second

var Q = ecache.NewLRUCache(16, 256, interval)

func init() {
	Q.Inspect(func(action int, key string, iface *interface{}, bytes []byte, status int) {
		// evicted / updated / deleted
		if (action == ecache.PUT && status <= 0) || (action == ecache.DEL && status == 1) {
			(*iface).(*AsyncHandle).Close()
		}
	})

	go func() {
		// manually evict expired items
		for {
			now := time.Now().UnixNano()
			keys := []string{}
			Q.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
				if expireAt < now {
					keys = append(keys, key)
				}
				return true
			})
			for _, k := range keys {
				Q.Del(k)
			}
			time.Sleep(interval)
		}
	}()
}

func HasInflight() (b bool) {
	Q.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
		b = true
		return false
	})
	return
}

type AsyncHandle struct {
	F *os.File
	B *bufio.Writer
}

func (ah AsyncHandle) Close() {
	ah.B.Flush()
	ah.F.Close()
}

type DefaultDataAdapter struct {
	acm AccessCtrlMgr
	opt Options
}

func NewDefaultDataAdapter(acm AccessCtrlMgr) DataAdapter {
	return &DefaultDataAdapter{
		acm: acm,
	}
}

func (ddo *DefaultDataAdapter) SetOptions(opt Options) {
	ddo.opt = opt
}

func (ddo *DefaultDataAdapter) Close() {
	for HasInflight() {
		time.Sleep(100 * time.Millisecond)
	}
}

// path/<文件名hash的最后三个字节>/hash/<dataID>_<sn>
func toFilePath(path string, bcktID, dataID int64, sn int) string {
	fileName := fmt.Sprintf("%d_%d", dataID, sn)
	hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
	return filepath.Join(path, fmt.Sprint(bcktID), hash[21:24], hash[8:24], fileName)
}

func (ddo *DefaultDataAdapter) Write(c Ctx, bktID, dataID int64, sn int, buf []byte) error {
	if err := ddo.acm.CheckPermission(c, W, bktID); err != nil {
		return err
	}

	path := toFilePath(Conf().Path, bktID, dataID, sn)
	// 不用判断是否存在，以及是否创建成功，如果失败，下面写入文件之前会报错
	os.MkdirAll(filepath.Dir(path), 0766)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}

	ah := &AsyncHandle{F: f, B: bufio.NewWriter(f)}
	_, err = ah.B.Write(buf)
	if ddo.opt.Sync {
		ah.Close()
	} else {
		Q.Put(strconv.FormatInt(dataID, 10), ah)
	}
	return err
}

func (ddo *DefaultDataAdapter) Flush(c Ctx, bktID, dataID int64) error {
	Q.Del(strconv.FormatInt(dataID, 10))
	return nil
}

func (ddo *DefaultDataAdapter) Read(c Ctx, bktID, dataID int64, sn int) ([]byte, error) {
	if err := ddo.acm.CheckPermission(c, R, bktID); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(toFilePath(Conf().Path, bktID, dataID, sn))
}

func (ddo *DefaultDataAdapter) ReadBytes(c Ctx, bktID, dataID int64, sn, offset, size int) ([]byte, error) {
	if offset == 0 && size == -1 {
		return ddo.Read(c, bktID, dataID, sn)
	}

	if err := ddo.acm.CheckPermission(c, R, bktID); err != nil {
		return nil, err
	}

	f, err := os.Open(toFilePath(Conf().Path, bktID, dataID, sn))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if offset > 0 {
		f.Seek(int64(offset), io.SeekStart)
	}

	var buf []byte
	if size == -1 {
		fi, err := f.Stat()
		if err != nil || fi.Size() < int64(offset) {
			return nil, err
		}
		buf = make([]byte, fi.Size()-int64(offset))
	} else {
		buf = make([]byte, size)
	}

	n, err := bufio.NewReaderSize(f, cap(buf)).Read(buf)
	if err != nil {
		return nil, err
	}
	if size > 0 && n < int(size) {
		return buf[:n], nil
	}
	return buf, nil
}

func (ddo *DefaultDataAdapter) FileSize(c Ctx, bktID, dataID int64, sn int) (int64, error) {
	if err := ddo.acm.CheckPermission(c, R, bktID); err != nil {
		return 0, err
	}

	f, err := os.Open(toFilePath(Conf().Path, bktID, dataID, sn))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
