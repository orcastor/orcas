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
	// Update updates part of existing data chunk (for writing versions with name="0")
	// offset: offset within the chunk, size: size to update, buf: data to write
	// If offset+len(buf) exceeds chunk size, the chunk will be extended
	// This allows direct modification of data blocks without creating new versions
	Update(c Ctx, bktID, dataID int64, sn int, offset int, buf []byte) error

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
	result := filepath.Join(path, fmt.Sprint(bcktID), hash[21:24], hash[8:24], fileName)
	fmt.Printf("[DEBUG toFilePath] path=%s, bcktID=%d, dataID=%d, sn=%d -> %s\n", path, bcktID, dataID, sn, result)
	return result
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

// Update updates part of existing data chunk (for writing versions with name="0")
// This allows direct modification of data blocks without creating new versions
// offset: offset within the chunk, buf: data to write at that offset
// If the chunk doesn't exist, it will be created and padded with zeros if needed
// If offset+len(buf) exceeds current chunk size, the chunk will be extended
func (dda *DefaultDataAdapter) Update(c Ctx, bktID, dataID int64, sn int, offset int, buf []byte) error {
	if len(buf) == 0 {
		return nil // Nothing to update
	}

	path := toFilePath(dda.dataPath, bktID, dataID, sn)
	// Ensure directory exists
	os.MkdirAll(filepath.Dir(path), 0o766)

	// Open file for read-write (create if not exists)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return ERR_OPEN_FILE
	}
	defer f.Close()

	// Get current file size
	fi, err := f.Stat()
	if err != nil {
		return ERR_READ_FILE
	}
	currentSize := fi.Size()

	// Calculate required size
	requiredSize := int64(offset + len(buf))
	if requiredSize > currentSize {
		// Extend file with zeros if needed
		if err := f.Truncate(requiredSize); err != nil {
			return ERR_OPEN_FILE
		}
		// Seek to end and write zeros if there's a gap
		if int64(offset) > currentSize {
			// There's a gap between current size and offset, fill with zeros
			if _, err := f.Seek(currentSize, io.SeekStart); err != nil {
				return ERR_OPEN_FILE
			}
			zeroPadding := make([]byte, int64(offset)-currentSize)
			if _, err := f.Write(zeroPadding); err != nil {
				return ERR_OPEN_FILE
			}
		}
	}

	// Seek to offset and write data
	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return ERR_OPEN_FILE
	}

	// Use buffered writer for better performance
	_, err = f.Write(buf)
	if err != nil {
		return err
	}

	f.Sync()
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
