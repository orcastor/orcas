package core

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
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
	Write(c context.Context, dataID int64, sn int, buf []byte) error
	Read(c context.Context, dataID int64, sn int) ([]byte, error)
	ReadBytes(c context.Context, dataID int64, sn int, offset, size int64) ([]byte, error)
}

const interval = time.Second

var buffer = ecache.NewLRUCache(16, 256, interval)

func init() {
	buffer.Inspect(func(action int, key string, iface *interface{}, bytes []byte, status int) {
		// evicted / updated / deleted
		if (action == ecache.PUT && status <= 0) || (action == ecache.DEL && status == 1) {
			w := (*iface).(*AsyncHandle)
			w.B.Flush()
			w.F.Close()
		}
	})

	go func() {
		// manually evict expired items
		for {
			now := time.Now().UnixNano()
			keys := []string{}
			buffer.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
				if expireAt < now {
					keys = append(keys, key)
				}
				return true
			})
			for _, key := range keys {
				buffer.Del(key)
			}
			time.Sleep(interval)
		}
	}()
}

func HasInflight() (b bool) {
	buffer.Walk(func(key string, iface *interface{}, bytes []byte, expireAt int64) bool {
		b = true
		return false
	})
	return b
}

type AsyncHandle struct {
	F *os.File
	B *bufio.Writer
}

type DefaultDataOperator struct {
	Options Option
}

func (ddo *DefaultDataOperator) SetOption(opt Option) {
	ddo.Options = opt
}

func toHash(dataID int64) string {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], uint64(dataID))
	return fmt.Sprintf("%x", md5.Sum(data[:]))[8:24]
}

func (ddo *DefaultDataOperator) Write(c context.Context, dataID int64, sn int, buf []byte) error {
	hash := toHash(dataID)

	// path/<文件名hash的最后三个字节>/hash
	dirPath := filepath.Join(Conf().Path, DATA_DIR, hash[len(hash)-3:], hash)
	// 不用判断是否存在，以及是否创建成功，如果失败，下面写入文件之前会报错
	os.MkdirAll(dirPath, 0766)

	path := filepath.Join(dirPath, fmt.Sprintf("%d_%d", dataID, sn))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	if ddo.Options.Sync {
		defer f.Close()
	}

	b := bufio.NewWriter(f)
	_, err = b.Write(buf)
	if err != nil {
		return err
	}

	if ddo.Options.Sync {
		err = b.Flush()
	} else {
		go b.Flush()
		buffer.Put(path, &AsyncHandle{F: f, B: b})
	}
	return err
}

func (ddo *DefaultDataOperator) Read(c context.Context, dataID int64, sn int) ([]byte, error) {
	hash := toHash(dataID)
	// path/<文件名hash的最后三个字节>/hash
	return os.ReadFile(filepath.Join(Conf().Path, DATA_DIR, hash[len(hash)-3:], hash, fmt.Sprintf("%d_%d", dataID, sn)))
}

func (ddo *DefaultDataOperator) ReadBytes(c context.Context, dataID int64, sn int, offset, size int64) ([]byte, error) {
	hash := toHash(dataID)
	// path/<文件名hash的最后三个字节>/hash
	path := filepath.Join(Conf().Path, DATA_DIR, hash[len(hash)-3:], hash, fmt.Sprintf("%d_%d", dataID, sn))
	f, err := os.Open(path)
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
		if err != nil {
			return nil, err
		}
		if fi.Size() < offset {
			return nil, nil
		}

		buf = make([]byte, fi.Size()-offset)
	} else {
		buf = make([]byte, size)
	}

	n, err := bufio.NewReader(f).Read(buf)
	if err != nil {
		return nil, err
	}
	if size > 0 && n < int(size) {
		return buf[:n], nil
	}
	return buf, nil
}
