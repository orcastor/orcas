package core

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/orca-zhang/ecache"
)

type Option struct {
	Sync bool
}

type DataOperator interface {
	SetOption(opt Option)
	Write(c Ctx, dataID int64, sn int, buf []byte) error
	Read(c Ctx, dataID int64, sn int) ([]byte, error)
	ReadBytes(c Ctx, dataID int64, sn int, offset, size int64) ([]byte, error)
}

const interval = time.Second

var Q = ecache.NewLRUCache(16, 256, interval)

func init() {
	Q.Inspect(func(action int, key string, iface *interface{}, bytes []byte, status int) {
		if action == ecache.PUT && status <= 0 { // evicted / updated
			(*iface).(*AsyncHandle).Close()
		}
	})

	go func() {
		// manually evict expired items
		for {
			now := time.Now().UnixNano()
			m := make(map[string]*AsyncHandle, 0)
			Q.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
				if expireAt < now {
					m[key] = (*iface).(*AsyncHandle)
				}
				return true
			})
			for fn, ah := range m {
				ah.Close()
				Q.Del(fn)
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

type DefaultDataOperator struct {
	Options Option
}

func (ddo *DefaultDataOperator) SetOption(opt Option) {
	ddo.Options = opt
}

func toHash(name string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(name)))[8:24]
}

func toFileName(dataID int64, sn int) string {
	return fmt.Sprintf("%d_%d", dataID, sn)
}

func (ddo *DefaultDataOperator) Write(c Ctx, dataID int64, sn int, buf []byte) error {
	fn := toFileName(dataID, sn)
	hash := toHash(fn)
	// path/<文件名hash的最后三个字节>/hash
	dirPath := filepath.Join(Conf().Path, DATA_DIR, hash[len(hash)-3:], hash)
	// 不用判断是否存在，以及是否创建成功，如果失败，下面写入文件之前会报错
	os.MkdirAll(dirPath, 0766)

	f, err := os.OpenFile(filepath.Join(dirPath, fn), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	ah := &AsyncHandle{F: f, B: bufio.NewWriter(f)}
	if ddo.Options.Sync {
		defer ah.Close()
	} else {
		defer Q.Put(fn, ah)
	}

	_, err = ah.B.Write(buf)
	return err
}

func (ddo *DefaultDataOperator) Read(c Ctx, dataID int64, sn int) ([]byte, error) {
	fn := toFileName(dataID, sn)
	hash := toHash(fn)
	// path/<文件名hash的最后三个字节>/hash
	return ioutil.ReadFile(filepath.Join(Conf().Path, DATA_DIR, hash[len(hash)-3:], hash, fn))
}

func (ddo *DefaultDataOperator) ReadBytes(c Ctx, dataID int64, sn int, offset, size int64) ([]byte, error) {
	fn := toFileName(dataID, sn)
	hash := toHash(fn)
	// path/<文件名hash的最后三个字节>/hash
	f, err := os.Open(filepath.Join(Conf().Path, DATA_DIR, hash[len(hash)-3:], hash, fn))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if offset > 0 {
		f.Seek(offset, io.SeekStart)
	}

	var buf []byte
	if size == -1 {
		fi, err := f.Stat()
		if err != nil || fi.Size() < offset {
			return nil, err
		}
		buf = make([]byte, fi.Size()-offset)
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
