package core

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type DataAdapter interface {
	Close()

	SetDataPath(dataPath string)

	Write(c Ctx, bktID, dataID int64, sn int, buf []byte) error

	Read(c Ctx, bktID, dataID int64, sn int) ([]byte, error)
	ReadBytes(c Ctx, bktID, dataID int64, sn, offset, size int) ([]byte, error)
	// Delete deletes a specific data chunk (DataID + sn)
	// If the chunk doesn't exist, it returns nil (no error)
	Delete(c Ctx, bktID, dataID int64, sn int) error
}

type DefaultDataAdapter struct {
	dataPath string // Path for data file storage
}

// SetDataPath sets the data path for the adapter
func (dda *DefaultDataAdapter) SetDataPath(dataPath string) {
	dda.dataPath = dataPath
}

func (dda *DefaultDataAdapter) Close() {
}

// path/<last 3 bytes of filename hash>/hash/<dataID>_<sn>
func toFilePath(path string, bcktID, dataID int64, sn int) string {
	fileName := fmt.Sprintf("%d_%d", dataID, sn)
	hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
	return filepath.Join(path, fmt.Sprint(bcktID), hash[21:24], hash[8:24], fileName)
}

func (dda *DefaultDataAdapter) Write(c Ctx, bktID, dataID int64, sn int, buf []byte) error {
	path := toFilePath(dda.dataPath, bktID, dataID, sn)
	// No need to check if it exists or if creation succeeds; if it fails, an error will be reported before writing the file below
	os.MkdirAll(filepath.Dir(path), 0o766)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return ERR_OPEN_FILE
	}
	defer f.Close()

	_, err = f.Write(buf)
	if err != nil {
		return err
	}

	// Immediately flush data to disk to ensure data is persisted
	// This is critical for large file writes where chunks must be flushed when full
	err = f.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (dda *DefaultDataAdapter) Read(c Ctx, bktID, dataID int64, sn int) ([]byte, error) {
	path := toFilePath(dda.dataPath, bktID, dataID, sn)
	data, err := ioutil.ReadFile(path)
	// Only return empty data for sparse files (handled by caller based on DataInfo)
	// For non-sparse files, return error if file doesn't exist
	if err != nil && os.IsNotExist(err) {
		return nil, err
	}
	return data, err
}

func (dda *DefaultDataAdapter) ReadBytes(c Ctx, bktID, dataID int64, sn, offset, size int) ([]byte, error) {
	if offset == 0 && size == -1 {
		return dda.Read(c, bktID, dataID, sn)
	}

	path := toFilePath(dda.dataPath, bktID, dataID, sn)
	f, err := os.Open(path)
	// Only handle missing files for sparse files (handled by caller based on DataInfo)
	// For non-sparse files, return error if file doesn't exist
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, ERR_READ_FILE
	}
	defer f.Close()

	if offset > 0 {
		f.Seek(int64(offset), io.SeekStart)
	}

	var buf []byte
	if size == -1 {
		fi, err := f.Stat()
		if err != nil {
			return nil, ERR_READ_FILE
		}
		if fi.Size() < int64(offset) {
			// Requested offset is beyond file size, return error (caller will handle sparse case)
			return nil, ERR_READ_FILE
		}
		buf = make([]byte, fi.Size()-int64(offset))
	} else {
		buf = make([]byte, size)
	}

	n, err := bufio.NewReaderSize(f, cap(buf)).Read(buf)
	if err != nil && err != io.EOF {
		return nil, ERR_READ_FILE
	}

	// If read less than requested, return what we have (caller will handle sparse case)
	if size > 0 && n < size {
		return buf[:n], nil
	}

	if n < len(buf) {
		return buf[:n], nil
	}
	return buf, nil
}

// Delete deletes a specific data chunk (DataID + sn)
// If the chunk doesn't exist, it returns nil (no error)
func (dda *DefaultDataAdapter) Delete(c Ctx, bktID, dataID int64, sn int) error {
	path := toFilePath(dda.dataPath, bktID, dataID, sn)

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// File doesn't exist, return nil (no error)
		return nil
	}

	// Delete the file
	err := os.Remove(path)
	if err != nil {
		return err
	}
	return nil
}
