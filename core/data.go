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

type DataAdapter interface {
	SetOptions(opt Options)
	Close()

	Write(c Ctx, bktID, dataID int64, sn int, buf []byte) error

	Read(c Ctx, bktID, dataID int64, sn int) ([]byte, error)
	ReadBytes(c Ctx, bktID, dataID int64, sn, offset, size int) ([]byte, error)
}

const interval = time.Second

var queue = ecache.NewLRUCache(16, 1024, interval)

func init() {
	queue.Inspect(func(action int, key string, iface *interface{}, bytes []byte, status int) {
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
			queue.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
				if expireAt < now {
					keys = append(keys, key)
				}
				return true
			})
			for _, k := range keys {
				queue.Del(k)
			}
			time.Sleep(interval)
		}
	}()
}

func HasInflight() (b bool) {
	queue.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
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
	opt Options
}

func (dda *DefaultDataAdapter) SetOptions(opt Options) {
	dda.opt = opt
}

func (dda *DefaultDataAdapter) Close() {
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

func (dda *DefaultDataAdapter) Write(c Ctx, bktID, dataID int64, sn int, buf []byte) error {
	path := toFilePath(ORCAS_DATA, bktID, dataID, sn)
	// 不用判断是否存在，以及是否创建成功，如果失败，下面写入文件之前会报错
	os.MkdirAll(filepath.Dir(path), 0766)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return ERR_OPEN_FILE
	}

	ah := &AsyncHandle{F: f, B: bufio.NewWriter(f)}
	_, err = ah.B.Write(buf)
	if dda.opt.Sync {
		ah.Close()
	} else {
		go ah.B.Flush()
		queue.Put(path, ah)
	}
	return err
}

func (dda *DefaultDataAdapter) Read(c Ctx, bktID, dataID int64, sn int) ([]byte, error) {
	return ioutil.ReadFile(toFilePath(ORCAS_DATA, bktID, dataID, sn))
}

func (dda *DefaultDataAdapter) ReadBytes(c Ctx, bktID, dataID int64, sn, offset, size int) ([]byte, error) {
	if offset == 0 && size == -1 {
		return dda.Read(c, bktID, dataID, sn)
	}

	f, err := os.Open(toFilePath(ORCAS_DATA, bktID, dataID, sn))
	if err != nil {
		return nil, ERR_OPEN_FILE
	}
	defer f.Close()

	if offset > 0 {
		f.Seek(int64(offset), io.SeekStart)
	}

	var buf []byte
	if size == -1 {
		fi, err := f.Stat()
		if err != nil || fi.Size() < int64(offset) {
			return nil, ERR_READ_FILE
		}
		buf = make([]byte, fi.Size()-int64(offset))
	} else {
		buf = make([]byte, size)
	}

	n, err := bufio.NewReaderSize(f, cap(buf)).Read(buf)
	if err != nil {
		return nil, ERR_READ_FILE
	}
	if size > 0 && n < int(size) {
		return buf[:n], nil
	}
	return buf, nil
}
