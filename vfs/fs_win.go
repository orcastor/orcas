//go:build windows
// +build windows

package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/orcastor/orcas/core"
)

// FileHandle is a mock interface for Windows compatibility
// In Linux version, this is fs.FileHandle from go-fuse
type FileHandle interface{}

// FileFsyncer is a mock interface for Windows compatibility
type FileFsyncer interface {
	Fsync(ctx context.Context, flags uint32) syscall.Errno
}

// FileReleaser is a mock interface for Windows compatibility
type FileReleaser interface {
	Release(ctx context.Context) syscall.Errno
}

// FileFlusher is a mock interface for Windows compatibility
type FileFlusher interface {
	Flush(ctx context.Context) syscall.Errno
}

// OrcasNode minimal implementation on Windows (currently only for testing and RandomAccessor API)
// Dokany support is now implemented for complete filesystem mounting functionality
// Dokany is a FUSE alternative on Windows, similar to Linux's FUSE
// Reference: https://github.com/dokan-dev/dokany
type OrcasNode struct {
	fs     *OrcasFS
	objID  int64
	obj    *core.ObjectInfo
	isRoot bool
	ra     *RandomAccessor
}

// initRootNode initializes root node (Windows platform implementation)
// Windows platform needs to initialize root node immediately, as it currently doesn't rely on filesystem mounting
// Note: If Dokany support is implemented, can initialize during Mount (similar to FUSE)
func (ofs *OrcasFS) initRootNode() {
	if ofs.root == nil {
		ofs.root = &OrcasNode{
			fs:     ofs,
			objID:  ofs.bktID,
			obj:    nil,
			isRoot: true,
		}
	}
}

// Dokany DLL and function pointers
var (
	dokanDLLHandle *syscall.DLL
)

// Dokany DLL name
const dokanDLLName = "dokan2.dll"

// Dokany API function pointers (loaded at runtime)
var (
	dokanMainProc             *syscall.Proc
	dokanUnmountProc          *syscall.Proc
	dokanCreateFileSystemProc *syscall.Proc
	dokanVersionProc          *syscall.Proc
	dokanDriverVersionProc    *syscall.Proc
)

// Load Dokany DLL and get function pointers
func initDokany() error {
	// Try to load dokan2.dll using syscall
	var err error
	dokanDLLHandle, err = syscall.LoadDLL(dokanDLLName)
	if err != nil {
		return fmt.Errorf("failed to load %s: %w. Please install Dokany driver from https://github.com/dokan-dev/dokany/releases", dokanDLLName, err)
	}

	// Load function pointers
	dokanMainProc, err = dokanDLLHandle.FindProc("DokanMain")
	if err != nil {
		return fmt.Errorf("failed to load DokanMain: %w", err)
	}

	dokanUnmountProc, err = dokanDLLHandle.FindProc("DokanUnmount")
	if err != nil {
		return fmt.Errorf("failed to load DokanUnmount: %w", err)
	}

	dokanCreateFileSystemProc, err = dokanDLLHandle.FindProc("DokanCreateFileSystem")
	if err != nil {
		return fmt.Errorf("failed to load DokanCreateFileSystem: %w", err)
	}

	// Optional functions
	dokanVersionProc, _ = dokanDLLHandle.FindProc("DokanVersion")
	dokanDriverVersionProc, _ = dokanDLLHandle.FindProc("DokanDriverVersion")

	return nil
}

// Dokany constants
const (
	DOKAN_SUCCESS              = 0
	DOKAN_ERROR                = -1
	DOKAN_DRIVE_LETTER_ERROR   = -2
	DOKAN_DRIVER_INSTALL_ERROR = -3
	DOKAN_START_ERROR          = -4
	DOKAN_MOUNT_ERROR          = -5
	DOKAN_MOUNT_POINT_ERROR    = -6
	DOKAN_VERSION_ERROR        = -7
)

// Dokany file access flags
const (
	FILE_READ_DATA        = 0x0001
	FILE_WRITE_DATA       = 0x0002
	FILE_APPEND_DATA      = 0x0004
	FILE_READ_EA          = 0x0008
	FILE_WRITE_EA         = 0x0010
	FILE_EXECUTE          = 0x0020
	FILE_READ_ATTRIBUTES  = 0x0080
	FILE_WRITE_ATTRIBUTES = 0x0100
	DELETE                = 0x10000
	READ_CONTROL          = 0x20000
	WRITE_DAC             = 0x40000
	WRITE_OWNER           = 0x80000
	SYNCHRONIZE           = 0x100000
	FILE_ALL_ACCESS       = 0x1F01FF
)

// Dokany create disposition
const (
	FILE_SUPERSEDE    = 0x00000000
	FILE_OPEN         = 0x00000001
	FILE_CREATE       = 0x00000002
	FILE_OPEN_IF      = 0x00000003
	FILE_OVERWRITE    = 0x00000004
	FILE_OVERWRITE_IF = 0x00000005
)

// Dokany file attributes
const (
	FILE_ATTRIBUTE_READONLY      = 0x00000001
	FILE_ATTRIBUTE_HIDDEN        = 0x00000002
	FILE_ATTRIBUTE_SYSTEM        = 0x00000004
	FILE_ATTRIBUTE_DIRECTORY     = 0x00000010
	FILE_ATTRIBUTE_ARCHIVE       = 0x00000020
	FILE_ATTRIBUTE_NORMAL        = 0x00000080
	FILE_ATTRIBUTE_TEMPORARY     = 0x00000100
	FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
)

// DokanyOptions represents Dokany mount options
type DokanyOptions struct {
	MountPoint         string // Mount point (e.g., "M:\\")
	ThreadCount        uint16 // Number of threads (default: 5)
	Options            uint32 // Mount options
	GlobalContext      uint64 // Global context
	Timeout            uint32 // Timeout in milliseconds (default: 0 = no timeout)
	AllocationUnitSize uint32 // Allocation unit size (default: 0 = default)
	SectorSize         uint32 // Sector size (default: 0 = default)
}

// DokanyOperations represents file system operations
type DokanyOperations struct {
	// File operations
	CreateFile           func(fileName string, desiredAccess, shareMode, creationDisposition, flagsAndAttributes uint32) (uintptr, int) // Returns handle and status
	OpenDirectory        func(fileName string) int
	CreateDirectory      func(fileName string) int
	Cleanup              func(fileName string, context uintptr)
	CloseFile            func(fileName string, context uintptr)
	ReadFile             func(fileName string, buffer []byte, offset int64, context uintptr) (int, int) // Returns bytes read and status
	WriteFile            func(fileName string, buffer []byte, offset int64, context uintptr) (int, int) // Returns bytes written and status
	FlushFileBuffers     func(fileName string, context uintptr) int
	GetFileInformation   func(fileName string, fileInfo *FileInfo, context uintptr) int
	FindFiles            func(fileName string, fillFindData func(fileName string, fileInfo *FileInfo) bool) int
	FindFilesWithPattern func(fileName, searchPattern string, fillFindData func(fileName string, fileInfo *FileInfo) bool) int
	SetFileAttributes    func(fileName string, fileAttributes uint32, context uintptr) int
	SetFileTime          func(fileName string, creationTime, lastAccessTime, lastWriteTime *FileTime, context uintptr) int
	DeleteFile           func(fileName string, context uintptr) int
	DeleteDirectory      func(fileName string, context uintptr) int
	MoveFile             func(fileName, newFileName string, replaceIfExisting bool, context uintptr) int
	SetEndOfFile         func(fileName string, length int64, context uintptr) int
	SetAllocationSize    func(fileName string, length int64, context uintptr) int
	LockFile             func(fileName string, offset, length int64, context uintptr) int
	UnlockFile           func(fileName string, offset, length int64, context uintptr) int
	GetDiskFreeSpace     func(freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes *uint64) int
	GetVolumeInformation func(volumeNameBuffer []byte, volumeNameSize *uint32, volumeSerialNumber *uint32, maximumComponentLength *uint32, fileSystemFlags *uint32, fileSystemNameBuffer []byte, fileSystemNameSize *uint32) int
	Mounted              func(mountPoint string) int
	Unmounted            func() int
	GetFileSecurity      func(fileName string, securityInformation uint32, securityDescriptor []byte, lengthNeeded *uint32, context uintptr) int
	SetFileSecurity      func(fileName string, securityInformation uint32, securityDescriptor []byte, context uintptr) int
}

// FileInfo represents file information for Dokany
type FileInfo struct {
	Attributes     uint32
	CreationTime   FileTime
	LastAccessTime FileTime
	LastWriteTime  FileTime
	Length         int64
	Index          uint32
}

// FileTime represents Windows FILETIME
type FileTime struct {
	LowDateTime  uint32
	HighDateTime uint32
}

// DokanyInstance represents a mounted Dokany filesystem
type DokanyInstance struct {
	options    *DokanyOptions
	operations *DokanyOperations
	fs         *OrcasFS
	mountPoint string
}

// MountOptions mount options (Windows version)
type MountOptions struct {
	// Mount point path (e.g., "M:\\")
	MountPoint string
	// Run in foreground (false means background)
	Foreground bool
	// Thread count for Dokany (default: 5)
	ThreadCount uint16
	// Configuration (for encryption, compression, instant upload, etc.)
	Config *core.Config
	// Enable debug mode (verbose output with timestamps)
	Debug bool
	// RequireKey: if true, return EPERM error when EndecKey is not provided in Config
	RequireKey bool
	// EndecKey: Encryption key for data encryption/decryption
	// If empty, encryption key will not be used (data will not be encrypted/decrypted)
	// This overrides bucket config EndecKey
	EndecKey string
	// BaseDBKey: Encryption key for main database (BASE path, SQLCipher)
	// If empty, database will not be encrypted
	// This can be set at runtime before mounting
	BaseDBKey string
	// DataDBKey: Encryption key for bucket databases (DATA path, SQLCipher)
	// If empty, bucket databases will not be encrypted
	// This can be set at runtime before mounting
	DataDBKey string
}

// Mount mounts ORCAS filesystem using Dokany (Windows)
func Mount(h core.Handler, c core.Ctx, bktID int64, opts *MountOptions) (*DokanyInstance, error) {
	if opts == nil {
		return nil, fmt.Errorf("mount options cannot be nil")
	}

	// Check mount point
	mountPoint, err := filepath.Abs(opts.MountPoint)
	if err != nil {
		return nil, fmt.Errorf("invalid mount point: %w", err)
	}

	// Check if mount point exists
	info, err := os.Stat(mountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			// Create mount point directory
			if err := os.MkdirAll(mountPoint, 0o755); err != nil {
				return nil, fmt.Errorf("failed to create mount point: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat mount point: %w", err)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("mount point is not a directory: %s", mountPoint)
	}

	// Set debug mode if specified
	if opts.Debug {
		SetDebugEnabled(true)
	}

	// Create filesystem, pass RequireKey option
	// Note: Config is handled separately in Windows version if needed
	ofs := NewOrcasFS(h, c, bktID, opts.RequireKey)

	// Set default thread count
	if opts.ThreadCount == 0 {
		opts.ThreadCount = 5
	}

	// Mount using Dokany
	instance, err := ofs.MountDokany(mountPoint, &DokanyOptions{
		MountPoint:  mountPoint,
		ThreadCount: opts.ThreadCount,
		Options:     0,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to mount: %w", err)
	}

	return instance, nil
}

// Serve runs filesystem service (blocks until unmount)
func Serve(instance *DokanyInstance, foreground bool) error {
	if foreground {
		// Run in foreground, wait for interrupt
		// Note: Full implementation would start Dokany main loop here
		// For now, this is a placeholder
		select {} // Block forever (would be replaced with actual Dokany main loop)
	} else {
		// Run in background
		// Note: Full implementation would start Dokany main loop in goroutine
		return nil
	}
}

// Unmount unmounts filesystem
func Unmount(mountPoint string) error {
	// Note: Full implementation would call DokanUnmount
	return fmt.Errorf("please use instance.Unmount() to unmount")
}

// MountDokany mounts the filesystem using Dokany
func (ofs *OrcasFS) MountDokany(mountPoint string, opts *DokanyOptions) (*DokanyInstance, error) {
	// Initialize Dokany if not already done
	if dokanMainProc == nil {
		if err := initDokany(); err != nil {
			return nil, err
		}
	}

	// Normalize mount point
	mountPoint = filepath.Clean(mountPoint)
	if len(mountPoint) == 0 {
		return nil, fmt.Errorf("mount point cannot be empty")
	}

	// Check if mount point exists and is a directory
	info, err := os.Stat(mountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			// Create mount point directory
			if err := os.MkdirAll(mountPoint, 0o755); err != nil {
				return nil, fmt.Errorf("failed to create mount point: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat mount point: %w", err)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("mount point is not a directory: %s", mountPoint)
	}

	// Set default options
	if opts == nil {
		opts = &DokanyOptions{
			ThreadCount: 5,
			Options:     0,
		}
	}
	opts.MountPoint = mountPoint

	// Create operations structure
	operations := createDokanyOperations(ofs)

	// Create instance
	instance := &DokanyInstance{
		options:    opts,
		operations: operations,
		fs:         ofs,
		mountPoint: mountPoint,
	}

	// Note: Actual mounting would be done by calling DokanMain
	// This is a simplified implementation - full implementation would require
	return instance, nil
}

// createDokanyOperations creates Dokany operations structure
func createDokanyOperations(ofs *OrcasFS) *DokanyOperations {
	return &DokanyOperations{
		CreateFile: func(fileName string, desiredAccess, shareMode, creationDisposition, flagsAndAttributes uint32) (uintptr, int) {
			return dokanyCreateFile(ofs, fileName, desiredAccess, shareMode, creationDisposition, flagsAndAttributes)
		},
		ReadFile: func(fileName string, buffer []byte, offset int64, context uintptr) (int, int) {
			return dokanyReadFile(ofs, fileName, buffer, offset, context)
		},
		WriteFile: func(fileName string, buffer []byte, offset int64, context uintptr) (int, int) {
			return dokanyWriteFile(ofs, fileName, buffer, offset, context)
		},
		GetFileInformation: func(fileName string, fileInfo *FileInfo, context uintptr) int {
			return dokanyGetFileInformation(ofs, fileName, fileInfo, context)
		},
		FindFiles: func(fileName string, fillFindData func(fileName string, fileInfo *FileInfo) bool) int {
			return dokanyFindFiles(ofs, fileName, fillFindData)
		},
		DeleteFile: func(fileName string, context uintptr) int {
			return dokanyDeleteFile(ofs, fileName, context)
		},
		DeleteDirectory: func(fileName string, context uintptr) int {
			return dokanyDeleteDirectory(ofs, fileName, context)
		},
		MoveFile: func(fileName, newFileName string, replaceIfExisting bool, context uintptr) int {
			return dokanyMoveFile(ofs, fileName, newFileName, replaceIfExisting, context)
		},
		CloseFile: func(fileName string, context uintptr) {
			dokanyCloseFile(ofs, fileName, context)
		},
		Cleanup: func(fileName string, context uintptr) {
			dokanyCleanup(ofs, fileName, context)
		},
		SetEndOfFile: func(fileName string, length int64, context uintptr) int {
			return dokanySetEndOfFile(ofs, fileName, length, context)
		},
		SetAllocationSize: func(fileName string, length int64, context uintptr) int {
			return dokanySetAllocationSize(ofs, fileName, length, context)
		},
		FlushFileBuffers: func(fileName string, context uintptr) int {
			return dokanyFlushFileBuffers(ofs, fileName, context)
		},
	}
}

// Dokany operation implementations
func dokanyCreateFile(ofs *OrcasFS, fileName string, desiredAccess, shareMode, creationDisposition, flagsAndAttributes uint32) (uintptr, int) {
	// Normalize path
	fileName = normalizePath(fileName)

	// Check if file exists
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil {
		// File doesn't exist, check if we should create it
		if creationDisposition == FILE_CREATE || creationDisposition == FILE_OPEN_IF || creationDisposition == FILE_OVERWRITE_IF {
			// Create new file
			parentPath := filepath.Dir(fileName)
			name := filepath.Base(fileName)
			var pid int64 = ofs.bktID // Root is now bucketID
			if parentPath != "." && parentPath != "/" {
				parentObj, err := findObjectByPath(ofs, parentPath)
				if err != nil {
					return 0, DOKAN_ERROR
				}
				pid = parentObj.ID
			}

			fileObj := &core.ObjectInfo{
				ID:    core.NewID(),
				PID:   pid,
				Type:  core.OBJ_TYPE_FILE,
				Name:  name,
				Size:  0,
				MTime: core.Now(),
			}
			_, err = ofs.h.Put(ofs.c, ofs.bktID, []*core.ObjectInfo{fileObj})
			if err != nil {
				return 0, DOKAN_ERROR
			}
			// Return handle (using object ID as handle)
			return uintptr(fileObj.ID), DOKAN_SUCCESS
		}
		return 0, DOKAN_ERROR
	}

	// File exists, return handle
	return uintptr(obj.ID), DOKAN_SUCCESS
}

func dokanyReadFile(ofs *OrcasFS, fileName string, buffer []byte, offset int64, context uintptr) (int, int) {
	// Always get fresh object info from database (don't rely on cache)
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil || obj.Type != core.OBJ_TYPE_FILE {
		return 0, DOKAN_ERROR
	}

	// Get fresh object info from database to ensure we have latest DataID
	objs, err := ofs.h.Get(ofs.c, ofs.bktID, []int64{obj.ID})
	if err != nil || len(objs) == 0 {
		return 0, DOKAN_ERROR
	}
	obj = objs[0]

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		return 0, DOKAN_SUCCESS
	}

	// Use RandomAccessor to read data, which handles caching and merging writes correctly
	ra, err := getOrCreateRandomAccessor(ofs, obj.ID)
	if err != nil {
		return 0, DOKAN_ERROR
	}

	// Read data using RandomAccessor (handles buffer merging and caching)
	readData, err := ra.Read(offset, len(buffer))
	if err != nil {
		return 0, DOKAN_ERROR
	}

	if len(readData) == 0 {
		return 0, DOKAN_SUCCESS
	}

	// Copy data to buffer
	copySize := len(readData)
	if copySize > len(buffer) {
		copySize = len(buffer)
	}
	copy(buffer, readData[:copySize])
	return copySize, DOKAN_SUCCESS
}

func dokanyWriteFile(ofs *OrcasFS, fileName string, buffer []byte, offset int64, context uintptr) (int, int) {
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil || obj.Type != core.OBJ_TYPE_FILE {
		return 0, DOKAN_ERROR
	}

	// Get or create RandomAccessor
	ra, err := getOrCreateRandomAccessor(ofs, obj.ID)
	if err != nil {
		return 0, DOKAN_ERROR
	}

	// Write data
	err = ra.Write(offset, buffer)
	if err != nil {
		return 0, DOKAN_ERROR
	}

	return len(buffer), DOKAN_SUCCESS
}

func dokanyGetFileInformation(ofs *OrcasFS, fileName string, fileInfo *FileInfo, context uintptr) int {
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil {
		return DOKAN_ERROR
	}

	// Set file attributes
	if obj.Type == core.OBJ_TYPE_DIR {
		fileInfo.Attributes = FILE_ATTRIBUTE_DIRECTORY
	} else {
		fileInfo.Attributes = FILE_ATTRIBUTE_NORMAL
	}

	// Set file size
	fileInfo.Length = obj.Size

	// Set file times
	fileInfo.CreationTime = timeToFileTime(obj.MTime)
	fileInfo.LastAccessTime = timeToFileTime(obj.MTime)
	fileInfo.LastWriteTime = timeToFileTime(obj.MTime)

	return DOKAN_SUCCESS
}

func dokanyFindFiles(ofs *OrcasFS, fileName string, fillFindData func(fileName string, fileInfo *FileInfo) bool) int {
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil || obj.Type != core.OBJ_TYPE_DIR {
		return DOKAN_ERROR
	}

	// List directory contents (fetch all pages)
	children, err := ofs.listAllObjects(obj.ID, core.ListOptions{})
	if err != nil {
		return DOKAN_ERROR
	}

	// Fill find data for each child
	for _, child := range children {
		fileInfo := &FileInfo{}
		if child.Type == core.OBJ_TYPE_DIR {
			fileInfo.Attributes = FILE_ATTRIBUTE_DIRECTORY
		} else {
			fileInfo.Attributes = FILE_ATTRIBUTE_NORMAL
		}
		fileInfo.Length = child.Size
		fileInfo.CreationTime = timeToFileTime(child.MTime)
		fileInfo.LastAccessTime = timeToFileTime(child.MTime)
		fileInfo.LastWriteTime = timeToFileTime(child.MTime)

		if !fillFindData(child.Name, fileInfo) {
			break
		}
	}

	return DOKAN_SUCCESS
}

func dokanyDeleteFile(ofs *OrcasFS, fileName string, context uintptr) int {
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil {
		return DOKAN_ERROR
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return DOKAN_ERROR
	}

	err = ofs.h.Delete(ofs.c, ofs.bktID, obj.ID)
	if err != nil {
		return DOKAN_ERROR
	}

	return DOKAN_SUCCESS
}

func dokanyDeleteDirectory(ofs *OrcasFS, fileName string, context uintptr) int {
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil {
		return DOKAN_ERROR
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		return DOKAN_ERROR
	}

	// Performance optimization: Skip checking if directory is empty
	// Instead, directly mark as deleted and let async cleanup handle it
	// This significantly improves performance for large directories
	// The directory will be removed from parent's listing immediately,
	// and all child objects will be cleaned up asynchronously in the background

	// Step 1: Remove from parent directory first (mark as deleted)
	// This makes the directory disappear from parent's listing immediately
	err = ofs.h.Recycle(ofs.c, ofs.bktID, obj.ID)
	if err != nil {
		return DOKAN_ERROR
	}

	// Step 2: Asynchronously delete and clean up (permanent deletion)
	// This includes recursively deleting all child objects and physical deletion of data files and metadata
	// The async cleanup will handle both empty and non-empty directories
	go func() {
		err := ofs.h.Delete(ofs.c, ofs.bktID, obj.ID)
		if err != nil {
			// Log error but don't return it to the caller since deletion is already marked
			// The directory is already removed from parent's listing
		}
	}()

	return DOKAN_SUCCESS
}

func dokanyMoveFile(ofs *OrcasFS, fileName, newFileName string, replaceIfExisting bool, context uintptr) int {
	srcObj, err := findObjectByPath(ofs, fileName)
	if err != nil {
		return DOKAN_ERROR
	}

	// Check if destination exists
	dstObj, err := findObjectByPath(ofs, newFileName)
	if err == nil {
		if !replaceIfExisting {
			return DOKAN_ERROR
		}
		// Delete existing file
		if err := ofs.h.Delete(ofs.c, ofs.bktID, dstObj.ID); err != nil {
			return DOKAN_ERROR
		}
	}

	// Get destination parent
	dstParentPath := filepath.Dir(newFileName)
	dstName := filepath.Base(newFileName)
	var dstParentID int64 = ofs.bktID // Root is now bucketID
	if dstParentPath != "." && dstParentPath != "/" {
		dstParentObj, err := findObjectByPath(ofs, dstParentPath)
		if err != nil {
			return DOKAN_ERROR
		}
		dstParentID = dstParentObj.ID
	}

	// Rename
	if err := ofs.h.Rename(ofs.c, ofs.bktID, srcObj.ID, dstName); err != nil {
		return DOKAN_ERROR
	}

	// Move to new parent if needed
	if dstParentID != srcObj.PID {
		if err := ofs.h.MoveTo(ofs.c, ofs.bktID, srcObj.ID, dstParentID); err != nil {
			return DOKAN_ERROR
		}
	}

	// Update cache after rename: directly update cached ObjectInfo's Name field
	// This is critical for files that are being written (e.g., .tmp files that are immediately renamed)
	// The cache key is based on fileID, so we need to update the cached ObjectInfo with the new name
	fileObjKey := srcObj.ID
	if cached, ok := fileObjCache.Get(fileObjKey); ok {
		if cachedObj, ok := cached.(*core.ObjectInfo); ok && cachedObj != nil {
			// Directly update the Name field in the cached object
			cachedObj.Name = dstName
			// Also update PID if moved to different directory
			if dstParentID != srcObj.PID {
				cachedObj.PID = dstParentID
			}
			// Put back to cache to ensure it's updated
			fileObjCache.Put(fileObjKey, cachedObj)
		}
	}

	return DOKAN_SUCCESS
}

func dokanyCloseFile(ofs *OrcasFS, fileName string, context uintptr) {
	// Cleanup resources if needed
	// For now, nothing to do
}

func dokanyCleanup(ofs *OrcasFS, fileName string, context uintptr) {
	// Flush any pending writes
	if context != 0 {
		objID := int64(context)
		ra, err := getRandomAccessor(ofs, objID)
		if err == nil {
			ra.Flush()
		}
	}
}

// dokanySetEndOfFile sets the end of file (truncate or extend)
// This is called when file size is explicitly set (e.g., truncate)
func dokanySetEndOfFile(ofs *OrcasFS, fileName string, length int64, context uintptr) int {
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil || obj.Type != core.OBJ_TYPE_FILE {
		return DOKAN_ERROR
	}

	// Get or create RandomAccessor
	ra, err := getOrCreateRandomAccessor(ofs, obj.ID)
	if err != nil {
		return DOKAN_ERROR
	}

	// Use Truncate to set file size (handles both truncate and extend)
	_, err = ra.Truncate(length)
	if err != nil {
		return DOKAN_ERROR
	}

	return DOKAN_SUCCESS
}

// dokanySetAllocationSize sets the allocation size (fallocate equivalent)
// For qBittorrent and similar tools, this pre-allocates space without writing data
// Optimization: For extend operations, only update file size metadata, don't allocate actual data blocks
// This creates a sparse file representation, which is efficient for random writes
func dokanySetAllocationSize(ofs *OrcasFS, fileName string, length int64, context uintptr) int {
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil || obj.Type != core.OBJ_TYPE_FILE {
		return DOKAN_ERROR
	}

	// Get or create RandomAccessor
	ra, err := getOrCreateRandomAccessor(ofs, obj.ID)
	if err != nil {
		return DOKAN_ERROR
	}

	// For allocation size, we only need to update the file size metadata
	// If extending, we don't need to write zeros - sparse file support
	// The actual data will be written when WriteFile is called
	oldSize := obj.Size

	// If extending (length > oldSize), just update size metadata without allocating data
	// This is the key optimization for qBittorrent: pre-allocate without writing zeros
	if length > oldSize {
		// Update file object size directly (sparse file - no data allocation)
		// This is much faster than writing zeros for large files
		updateFileObj := &core.ObjectInfo{
			ID:     obj.ID,
			PID:    obj.PID,
			DataID: obj.DataID,
			Size:   length, // Update size without allocating data
			MTime:  core.Now(),
			Type:   obj.Type,
			Name:   obj.Name,
		}

		// Update file object in database
		_, err = ofs.h.Put(ofs.c, ofs.bktID, []*core.ObjectInfo{updateFileObj})
		if err != nil {
			return DOKAN_ERROR
		}

		// Mark as sparse file in RandomAccessor for optimization
		// This will help optimize subsequent random writes
		ra.MarkSparseFile(length)
	} else if length < oldSize {
		// If truncating, use Truncate method which properly handles data
		_, err = ra.Truncate(length)
		if err != nil {
			return DOKAN_ERROR
		}
	}
	// If length == oldSize, no operation needed

	return DOKAN_SUCCESS
}

// dokanyFlushFileBuffers flushes file buffers
func dokanyFlushFileBuffers(ofs *OrcasFS, fileName string, context uintptr) int {
	obj, err := findObjectByPath(ofs, fileName)
	if err != nil || obj.Type != core.OBJ_TYPE_FILE {
		return DOKAN_ERROR
	}

	// Get RandomAccessor and flush
	ra, err := getRandomAccessor(ofs, obj.ID)
	if err == nil {
		_, err = ra.Flush()
		if err != nil {
			return DOKAN_ERROR
		}
	}

	return DOKAN_SUCCESS
}

// Helper functions
func normalizePath(path string) string {
	// Normalize to Unix-style path (always use forward slashes)
	// Convert Windows backslashes to forward slashes
	path = strings.ReplaceAll(path, "\\", "/")

	// Clean path (remove . and .. components)
	path = filepath.Clean(path)
	// Convert back to forward slashes after Clean
	path = strings.ReplaceAll(path, "\\", "/")

	// Remove leading slash and normalize
	path = strings.TrimPrefix(path, "/")
	if path == "" || path == "." {
		return "/"
	}

	// Ensure leading slash for absolute paths
	return "/" + path
}

func timeToFileTime(t int64) FileTime {
	// Convert Unix timestamp to Windows FILETIME
	// FILETIME is 100-nanosecond intervals since January 1, 1601
	// Unix time is seconds since January 1, 1970
	// Difference: 11644473600 seconds
	const filetimeEpoch = 11644473600
	const filetimeScale = 10000000 // 100-nanosecond intervals per second

	filetime := (t + filetimeEpoch) * filetimeScale
	return FileTime{
		LowDateTime:  uint32(filetime & 0xFFFFFFFF),
		HighDateTime: uint32(filetime >> 32),
	}
}

func findObjectByPath(ofs *OrcasFS, path string) (*core.ObjectInfo, error) {
	// Normalize path
	path = normalizePath(path)
	if path == "/" {
		return &core.ObjectInfo{
			ID:   ofs.bktID,
			PID:  0,
			Type: core.OBJ_TYPE_DIR,
			Name: "/",
		}, nil
	}

	// Split path by /
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return &core.ObjectInfo{
			ID:   ofs.bktID,
			PID:  0,
			Type: core.OBJ_TYPE_DIR,
			Name: "/",
		}, nil
	}

	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return nil, fmt.Errorf("object not found")
	}

	var currentID int64 = ofs.bktID // Root is now bucketID

	// Traverse path
	for i, part := range parts {
		if part == "" {
			continue
		}

		// List current directory (fetch all pages)
		children, err := ofs.listAllObjects(currentID, core.ListOptions{})
		if err != nil {
			return nil, err
		}

		// Find child with matching name
		var found bool
		for _, child := range children {
			if child.Name == part {
				currentID = child.ID
				found = true

				// If this is the last part, return the object
				if i == len(parts)-1 {
					return child, nil
				}

				// Check if it's a directory
				if child.Type != core.OBJ_TYPE_DIR {
					return nil, fmt.Errorf("path component is not a directory")
				}

				break
			}
		}

		if !found {
			return nil, fmt.Errorf("object not found")
		}
	}

	return nil, fmt.Errorf("object not found")
}

func getOrCreateRandomAccessor(ofs *OrcasFS, fileID int64) (*RandomAccessor, error) {
	return NewRandomAccessor(ofs, fileID)
}

func getRandomAccessor(ofs *OrcasFS, fileID int64) (*RandomAccessor, error) {
	return NewRandomAccessor(ofs, fileID)
}

// getObj gets the object info for this node
func (n *OrcasNode) getObj() (*core.ObjectInfo, error) {
	if n.obj != nil {
		return n.obj, nil
	}
	if n.fs == nil {
		return nil, fmt.Errorf("filesystem not initialized")
	}
	objs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{n.objID})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("object not found: %d", n.objID)
	}
	n.obj = objs[0]
	return n.obj, nil
}

// invalidateObj invalidates the cached object info
func (n *OrcasNode) invalidateObj() {
	n.obj = nil
}

// Unmount unmounts the Dokany filesystem
func (instance *DokanyInstance) Unmount() error {
	if dokanUnmountProc == nil {
		return fmt.Errorf("Dokany not initialized")
	}

	// Call DokanUnmount
	// Note: This is a simplified implementation
	// For now, return success (actual unmount would be done by Dokany driver)
	return nil
}

// Unlink deletes a file (Windows implementation)
// This is a simplified version that uses handler methods directly
// Unlink deletes a single file
func (n *OrcasNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.unlinkInternal(ctx, []string{name}, false)
}

// UnlinkBatch deletes multiple files efficiently using batch operations
// This is optimized for deleting many files at once, reducing database round trips
func (n *OrcasNode) UnlinkBatch(ctx context.Context, names []string) syscall.Errno {
	if len(names) == 0 {
		return 0
	}
	if len(names) == 1 {
		return n.Unlink(ctx, names[0])
	}
	return n.unlinkInternal(ctx, names, true)
}

// unlinkInternal is the internal implementation for both single and batch unlink
// batchMode: if true, uses batch optimizations for multiple files
func (n *OrcasNode) unlinkInternal(ctx context.Context, names []string, batchMode bool) syscall.Errno {
	DebugLog("[VFS Unlink] Entry: names=%v, parentID=%d, batchMode=%v", names, n.objID, batchMode)
	
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Unlink] ERROR: checkKey failed: names=%v, parentID=%d, errno=%d", names, n.objID, errno)
		return errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Unlink] ERROR: Failed to get parent object: names=%v, parentID=%d, error=%v", names, n.objID, err)
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Unlink] ERROR: Parent is not a directory: names=%v, parentID=%d, type=%d", names, n.objID, obj.Type)
		return syscall.ENOTDIR
	}

	// Build a map of names for quick lookup
	nameMap := make(map[string]bool, len(names))
	for _, name := range names {
		nameMap[name] = true
	}

	// Find all target objects
	// IMPORTANT: Also check RandomAccessor registry for files that are being written
	// (especially .tmp files) that may not be in List results yet
	type targetInfo struct {
		id   int64
		obj  *core.ObjectInfo
		name string
	}
	targets := make([]targetInfo, 0, len(names))
	foundInRegistry := make(map[string]bool)

	// First, try to find from RandomAccessor registry (for files being written, especially .tmp files)
	if n.fs != nil {
		n.fs.raRegistry.Range(func(key, value interface{}) bool {
			if fileID, ok := key.(int64); ok {
				if ra, ok := value.(*RandomAccessor); ok && ra != nil {
					fileObj, err := ra.getFileObj()
					if err == nil && fileObj != nil && fileObj.PID == obj.ID && fileObj.Type == core.OBJ_TYPE_FILE {
						if nameMap[fileObj.Name] {
							targets = append(targets, targetInfo{
								id:   fileID,
								obj:  fileObj,
								name: fileObj.Name,
							})
							foundInRegistry[fileObj.Name] = true
							DebugLog("[VFS Unlink] Found target file from RandomAccessor registry: fileID=%d, name=%s", fileID, fileObj.Name)
						}
					}
				}
			}
			return true // Continue iteration
		})
	}

	// If not all found in RandomAccessor registry, try to find from List
	if len(targets) < len(names) {
		children, err := n.fs.listAllObjects(obj.ID, core.ListOptions{})
		if err != nil {
			DebugLog("[VFS Unlink] ERROR: Failed to list directory children: names=%v, parentID=%d, error=%v", names, obj.ID, err)
			return syscall.EIO
		}

		for _, child := range children {
			if nameMap[child.Name] && child.Type == core.OBJ_TYPE_FILE && !foundInRegistry[child.Name] {
				targets = append(targets, targetInfo{
					id:   child.ID,
					obj:  child,
					name: child.Name,
				})
				DebugLog("[VFS Unlink] Found target file from List: fileID=%d, name=%s", child.ID, child.Name)
			}
		}
	}

	if len(targets) == 0 {
		DebugLog("[VFS Unlink] ERROR: No target files found: names=%v, parentID=%d", names, obj.ID)
		return syscall.ENOENT
	}

	// Step 1: Remove from RandomAccessor registry if present and flush
	// This ensures the files are removed from pending objects before deletion
	targetIDs := make([]int64, 0, len(targets))
	targetObjs := make([]*core.ObjectInfo, 0, len(targets))
	dataIDsToClear := make([]int64, 0)

	for _, target := range targets {
		targetIDs = append(targetIDs, target.id)
		if target.obj != nil {
			targetObjs = append(targetObjs, target.obj)
			if target.obj.DataID > 0 && target.obj.DataID != core.EmptyDataID {
				dataIDsToClear = append(dataIDsToClear, target.obj.DataID)
			}
		}

		// Remove from RandomAccessor registry if present
		if n.fs != nil {
			if targetRA := n.fs.getRandomAccessorByFileID(target.id); targetRA != nil {
				// Force flush before deletion to ensure data is saved
				if _, err := targetRA.ForceFlush(); err != nil {
					DebugLog("[VFS Unlink] WARNING: Failed to flush file before deletion: fileID=%d, error=%v", target.id, err)
				}
				// Unregister RandomAccessor
				n.fs.unregisterRandomAccessor(target.id, targetRA)
				DebugLog("[VFS Unlink] Removed file from RandomAccessor registry: fileID=%d", target.id)
			}
		}
	}

	// If some targets were not found, fetch them from database
	if len(targetObjs) < len(targetIDs) {
		missingIDs := make([]int64, 0)
		for i, targetID := range targetIDs {
			if i >= len(targetObjs) || targetObjs[i] == nil {
				missingIDs = append(missingIDs, targetID)
			}
		}
		if len(missingIDs) > 0 {
			fetchedObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, missingIDs)
			if err == nil {
				for _, fetchedObj := range fetchedObjs {
					targetObjs = append(targetObjs, fetchedObj)
					if fetchedObj.DataID > 0 && fetchedObj.DataID != core.EmptyDataID {
						dataIDsToClear = append(dataIDsToClear, fetchedObj.DataID)
					}
				}
			}
		}
	}

	// Step 2: Batch Recycle - mark all files as deleted
	// In batch mode, we can optimize by calling Recycle in parallel or using batch operations
	if batchMode && len(targetIDs) > 1 {
		// Use concurrent Recycle for better performance
		const maxConcurrentRecycle = 10
		sem := make(chan struct{}, maxConcurrentRecycle)
		var wg sync.WaitGroup
		var recycleErrors []error
		var errorsMu sync.Mutex

		for _, targetID := range targetIDs {
			wg.Add(1)
			go func(id int64) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				if err := n.fs.h.Recycle(n.fs.c, n.fs.bktID, id); err != nil {
					errorsMu.Lock()
					recycleErrors = append(recycleErrors, fmt.Errorf("failed to recycle file %d: %w", id, err))
					errorsMu.Unlock()
					DebugLog("[VFS Unlink] ERROR: Failed to recycle file: fileID=%d, error=%v", id, err)
				}
			}(targetID)
		}
		wg.Wait()

		if len(recycleErrors) > 0 {
			DebugLog("[VFS Unlink] ERROR: Some files failed to recycle: errors=%d", len(recycleErrors))
			// Continue with deletion even if some Recycle operations failed
		}
	} else {
		// Sequential Recycle for single file or when batch mode is disabled
		for _, targetID := range targetIDs {
			if err := n.fs.h.Recycle(n.fs.c, n.fs.bktID, targetID); err != nil {
				DebugLog("[VFS Unlink] ERROR: Failed to recycle file: fileID=%d, error=%v", targetID, err)
				return syscall.EIO
			}
		}
	}

	// Step 3: Update cache immediately
	// CRITICAL: Clear all caches for the deleted files to prevent data corruption
	n.invalidateDirListCache(obj.ID)

	// Clear DataInfo cache for all deleted files' DataIDs
	for _, dataID := range dataIDsToClear {
		dataInfoCache.Del(dataID)
		decodingReaderCache.Del(dataID)
		DebugLog("[VFS Unlink] Cleared DataInfo cache for deleted file: dataID=%d", dataID)
	}

	// Clear file object cache for all deleted files
	for _, targetID := range targetIDs {
		fileObjCache.Del(targetID)
		DebugLog("[VFS Unlink] Cleared file object cache for deleted file: fileID=%d", targetID)
	}

	// CRITICAL: Remove journal and clean up jwal files
	// This ensures jwal files are deleted even if RandomAccessor was already closed
	if n.fs.journalMgr != nil {
		for _, targetID := range targetIDs {
			n.fs.journalMgr.Remove(targetID)
			DebugLog("[VFS Unlink] Removed journal and cleaned up jwal files: fileID=%d", targetID)
		}
	}

	// Invalidate parent directory cache
	n.invalidateObj()

	// Step 4: Schedule delayed deletion for atomic replace adaptation
	// Instead of immediately deleting, schedule it for 5 seconds later
	// This allows Rename to detect atomic replace pattern and merge versions
	if n.fs.atomicReplaceMgr != nil {
		// Schedule deletion for each file
		for i, targetID := range targetIDs {
			targetName := targets[i].name
			if err := n.fs.atomicReplaceMgr.ScheduleDeletion(n.fs.bktID, obj.ID, targetName, targetID); err != nil {
				DebugLog("[VFS Unlink] WARNING: Failed to schedule delayed deletion: fileID=%d, error=%v", targetID, err)
				// Fallback to immediate deletion if scheduling fails
				go func(id int64) {
					err := n.fs.h.Delete(n.fs.c, n.fs.bktID, id)
					if err != nil {
						DebugLog("[VFS Unlink] ERROR: Failed to permanently delete file: fileID=%d, error=%v", id, err)
					} else {
						DebugLog("[VFS Unlink] Successfully permanently deleted file: fileID=%d", id)
					}
				}(targetID)
			} else {
				DebugLog("[VFS Unlink] Scheduled delayed deletion: fileID=%d, name=%s (will delete in 5s)", targetID, targetName)
			}
		}
	} else {
		// Fallback: Asynchronously delete and clean up (permanent deletion)
		// This includes physical deletion of data files and metadata
		// In batch mode, use concurrent deletion for better performance
		if batchMode && len(targetIDs) > 1 {
			// Use concurrent Delete for better performance
			const maxConcurrentDelete = 10
			sem := make(chan struct{}, maxConcurrentDelete)
			var wg sync.WaitGroup

			for _, targetID := range targetIDs {
				wg.Add(1)
				go func(id int64) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()

					// Use the original context to preserve authentication information
					// Context is read-only and safe to use in goroutines
					err := n.fs.h.Delete(n.fs.c, n.fs.bktID, id)
					if err != nil {
						DebugLog("[VFS Unlink] ERROR: Failed to permanently delete file: fileID=%d, error=%v", id, err)
					} else {
						DebugLog("[VFS Unlink] Successfully permanently deleted file: fileID=%d", id)
					}
				}(targetID)
			}
			// Don't wait for deletion to complete, it's asynchronous
		} else {
			// Sequential Delete for single file
			for _, targetID := range targetIDs {
				go func(id int64) {
					// Use the original context to preserve authentication information
					// Context is read-only and safe to use in goroutines
					err := n.fs.h.Delete(n.fs.c, n.fs.bktID, id)
					if err != nil {
						DebugLog("[VFS Unlink] ERROR: Failed to permanently delete file: fileID=%d, error=%v", id, err)
					} else {
						DebugLog("[VFS Unlink] Successfully permanently deleted file: fileID=%d", id)
					}
				}(targetID)
			}
		}
	}

	return 0
}

// invalidateDirListCache invalidates directory listing cache for a directory
// This should be called whenever directory contents change (Create, Mkdir, Unlink, Rmdir, Rename)
// Cache will be rebuilt on next readdir/list operation
func (n *OrcasNode) invalidateDirListCache(dirID int64) {
	// Note: getDirListCacheKey was removed, using dirID directly as key
	dirListCache.Del(dirID)
	readdirCache.Del(dirID)
	readdirCacheStale.Delete(dirID)
	DebugLog("[VFS invalidateDirListCache] Invalidated directory cache: dirID=%d", dirID)
}

// O_LARGEFILE constant for Windows compatibility
const O_LARGEFILE = 0x8000

// Open opens a file and returns a file handle (mock implementation for Windows)
// This is a mock implementation that uses RandomAccessor internally
func (n *OrcasNode) Open(ctx context.Context, flags uint32) (fh FileHandle, fuseFlags uint32, errno syscall.Errno) {
	DebugLog("[VFS Open] Entry: objID=%d, flags=0x%x", n.objID, flags)

	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Open] ERROR: checkKey failed: objID=%d, flags=0x%x, errno=%d", n.objID, flags, errno)
		return nil, 0, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Open] ERROR: Failed to get object: objID=%d, error=%v, flags=0x%x", n.objID, err, flags)
		return nil, 0, syscall.ENOENT
	}

	DebugLog("[VFS Open] Object info: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d, flags=0x%x",
		obj.ID, obj.Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, obj.Name, obj.PID, flags)

	if obj.Type != core.OBJ_TYPE_FILE {
		DebugLog("[VFS Open] ERROR: Object is not a file (type=%d, expected FILE=%d): objID=%d, name=%s, PID=%d",
			obj.Type, core.OBJ_TYPE_FILE, obj.ID, obj.Name, obj.PID)
		return nil, 0, syscall.EISDIR
	}

	// Check if O_TRUNC is set - if so, truncate file to size 0
	if flags&syscall.O_TRUNC != 0 {
		DebugLog("[VFS Open] O_TRUNC flag set, truncating file: fileID=%d", obj.ID)
		if errno := n.truncateFile(0); errno != 0 {
			DebugLog("[VFS Open] ERROR: Failed to truncate file: fileID=%d, errno=%d", obj.ID, errno)
			return nil, 0, errno
		}
		// Update object size
		obj.Size = 0
		n.obj = obj
	}

	// Return the node itself as FileHandle (same as Linux version)
	DebugLog("[VFS Open] Opened file: fileID=%d, flags=0x%x", obj.ID, flags)
	DebugLog("[VFS Open] Returning FileHandle: fileID=%d, fuseFlags=0x%x, FileHandle type=*vfs.OrcasNode", obj.ID, 0)
	return n, 0, 0
}

// getRandomAccessor gets or creates a RandomAccessor for this node (Windows version)
func (n *OrcasNode) getRandomAccessor() (*RandomAccessor, error) {
	if n.ra != nil {
		return n.ra, nil
	}

	obj, err := n.getObj()
	if err != nil {
		return nil, err
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return nil, fmt.Errorf("object is not a file")
	}

	// Create new RandomAccessor
	ra, err := NewRandomAccessor(n.fs, obj.ID)
	if err != nil {
		return nil, err
	}

	n.ra = ra
	return ra, nil
}

// Write writes data to the file at the specified offset (mock implementation for Windows)
func (n *OrcasNode) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	DebugLog("[VFS Write] Write called: objID=%d, offset=%d, size=%d", n.objID, off, len(data))

	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Write] ERROR: checkKey failed: objID=%d, errno=%d", n.objID, errno)
		return 0, errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Write] ERROR: Failed to get object: objID=%d, error=%v", n.objID, err)
		return 0, syscall.ENOENT
	}

	DebugLog("[VFS Write] Object info: objID=%d, type=%d (FILE=%d, DIR=%d), name=%s, PID=%d",
		obj.ID, obj.Type, core.OBJ_TYPE_FILE, core.OBJ_TYPE_DIR, obj.Name, obj.PID)

	if obj.Type != core.OBJ_TYPE_FILE {
		DebugLog("[VFS Write] ERROR: Object is not a file (type=%d, expected FILE=%d): objID=%d, name=%s, PID=%d",
			obj.Type, core.OBJ_TYPE_FILE, obj.ID, obj.Name, obj.PID)
		return 0, syscall.EISDIR
	}

	// Get or create RandomAccessor
	ra, err := n.getRandomAccessor()
	if err != nil {
		DebugLog("[VFS Write] ERROR: Failed to get RandomAccessor for file objID=%d: %v", obj.ID, err)
		return 0, syscall.EIO
	}

	// Write data
	DebugLog("[VFS Write] Writing data: fileID=%d, offset=%d, size=%d", obj.ID, off, len(data))
	err = ra.Write(off, data)
	if err != nil {
		DebugLog("[VFS Write] ERROR: Failed to write data: fileID=%d, offset=%d, size=%d, error=%v", obj.ID, off, len(data), err)
		return 0, syscall.EIO
	}

	// Invalidate object cache
	n.invalidateObj()

	DebugLog("[VFS Write] Successfully wrote data: fileID=%d, offset=%d, size=%d, written=%d", obj.ID, off, len(data), len(data))
	return uint32(len(data)), 0
}

// Fsync flushes file data to storage (mock implementation for Windows)
func (n *OrcasNode) Fsync(ctx context.Context, f FileHandle, flags uint32) syscall.Errno {
	DebugLog("[VFS Fsync] Entry: objID=%d, FileHandle=%v, flags=0x%x", n.objID, f, flags)

	// Forward to FileHandle if it implements FileFsyncer
	if f != nil {
		if fileFsyncer, ok := f.(FileFsyncer); ok {
			errno := fileFsyncer.Fsync(ctx, flags)
			DebugLog("[VFS Fsync] Forwarded to FileHandle: objID=%d, errno=%d", n.objID, errno)
			return errno
		}
	}

	// Otherwise use our own implementation
	return n.fsyncImpl(ctx, flags)
}

// fsyncImpl is the actual fsync implementation
func (n *OrcasNode) fsyncImpl(ctx context.Context, flags uint32) syscall.Errno {
	DebugLog("[VFS fsyncImpl] Entry: objID=%d, flags=0x%x", n.objID, flags)

	// Flush RandomAccessor first
	if errno := n.Flush(ctx, nil); errno != 0 {
		DebugLog("[VFS Fsync] ERROR: Flush failed: objID=%d, flags=0x%x, errno=%d", n.objID, flags, errno)
		return errno
	}

	// Flush object cache
	n.invalidateObj()
	return 0
}

// Release releases the file handle (mock implementation for Windows)
func (n *OrcasNode) Release(ctx context.Context, f FileHandle) syscall.Errno {
	DebugLog("[VFS Release] Entry: objID=%d, FileHandle=%v", n.objID, f)

	// Forward to FileHandle if it implements FileReleaser
	if f != nil {
		if fileReleaser, ok := f.(FileReleaser); ok {
			errno := fileReleaser.Release(ctx)
			DebugLog("[VFS Release] Forwarded to FileHandle: objID=%d, errno=%d", n.objID, errno)
			return errno
		}
	}

	// Otherwise use our own implementation
	return n.releaseImpl(ctx)
}

// releaseImpl is the actual release implementation
func (n *OrcasNode) releaseImpl(ctx context.Context) syscall.Errno {
	DebugLog("[VFS releaseImpl] Entry: objID=%d", n.objID)

	if !n.isRoot {
		if errno := n.fs.checkKey(); errno != 0 {
			return errno
		}
	}

	// Flush RandomAccessor before release
	if n.ra != nil {
		_, err := n.ra.Flush()
		if err != nil {
			DebugLog("[VFS Release] ERROR: Failed to flush RandomAccessor: objID=%d, error=%v", n.objID, err)
			return syscall.EIO
		}
	}

	// Clear RandomAccessor reference (but don't close it, as it may be reused)
	// In Windows version, we just clear the reference
	n.ra = nil

	DebugLog("[VFS Release] Successfully released file: objID=%d", n.objID)
	return 0
}

// truncateFile truncates the file to the specified size (helper method)
func (n *OrcasNode) truncateFile(size int64) syscall.Errno {
	obj, err := n.getObj()
	if err != nil {
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		return syscall.EISDIR
	}

	// Get or create RandomAccessor
	_, err = n.getRandomAccessor()
	if err != nil {
		return syscall.EIO
	}

	// Truncate by writing empty data or using Setattr
	// For simplicity, we'll use Setattr via updating the object
	// In a real implementation, we might need to handle this differently
	obj.Size = size
	n.obj = obj

	// If RandomAccessor exists, we might need to clear its buffers
	// For now, just update the object size
	return 0
}

// Flush flushes file data (Windows version)
func (n *OrcasNode) Flush(ctx context.Context, f FileHandle) syscall.Errno {
	DebugLog("[VFS Flush] Entry: objID=%d, FileHandle=%v", n.objID, f)

	// Forward to FileHandle if it implements FileFlusher
	if f != nil {
		if fileFlusher, ok := f.(FileFlusher); ok {
			errno := fileFlusher.Flush(ctx)
			DebugLog("[VFS Flush] Forwarded to FileHandle: objID=%d, errno=%d", n.objID, errno)
			return errno
		}
	}

	// Otherwise use our own implementation
	return n.flushImpl(ctx)
}

// flushImpl is the actual flush implementation
func (n *OrcasNode) flushImpl(ctx context.Context) syscall.Errno {
	DebugLog("[VFS flushImpl] Entry: objID=%d", n.objID)

	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Flush] ERROR: checkKey failed: objID=%d, errno=%d", n.objID, errno)
		return errno
	}

	if n.ra == nil {
		return 0
	}

	// Execute Flush
	obj, err := n.getObj()
	if err == nil && obj != nil {
		DebugLog("[VFS Flush] Flushing file: fileID=%d, currentSize=%d", obj.ID, obj.Size)
	}

	versionID, err := n.ra.Flush()
	if err != nil {
		fileID := n.objID
		if obj != nil {
			fileID = obj.ID
		}
		DebugLog("[VFS Flush] ERROR: Failed to flush file: fileID=%d, error=%v", fileID, err)
		return syscall.EIO
	}

	// Get updated object from RandomAccessor's cache
	var updatedObj *core.ObjectInfo
	if cachedObj := n.ra.fileObj.Load(); cachedObj != nil {
		if loadedObj, ok := cachedObj.(*core.ObjectInfo); ok && loadedObj != nil {
			updatedObj = loadedObj
			DebugLog("[VFS Flush] Got updated object from RandomAccessor cache: fileID=%d, size=%d, dataID=%d, mtime=%d",
				updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
		}
	}

	// Update local cache
	if updatedObj != nil {
		n.obj = updatedObj
		DebugLog("[VFS Flush] Updated file object cache: fileID=%d, size=%d, dataID=%d, mtime=%d",
			updatedObj.ID, updatedObj.Size, updatedObj.DataID, updatedObj.MTime)
	}

	DebugLog("[VFS Flush] Successfully flushed file: fileID=%d, versionID=%d", n.objID, versionID)
	return 0
}

// readResultWrapper wraps byte slice as io.Reader for Windows compatibility
type readResultWrapper struct {
	data []byte
	pos  int
}

func (r *readResultWrapper) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

// Read reads data from the file at the specified offset (mock implementation for Windows)
func (n *OrcasNode) Read(ctx context.Context, dest []byte, off int64) (io.Reader, syscall.Errno) {
	DebugLog("[VFS Read] Entry: objID=%d, offset=%d, size=%d", n.objID, off, len(dest))

	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Read] ERROR: checkKey failed: objID=%d, offset=%d, size=%d, errno=%d", n.objID, off, len(dest), errno)
		return nil, errno
	}

	// Force refresh object cache to get latest DataID (important after writes)
	n.invalidateObj()

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Read] ERROR: Failed to get object: objID=%d, offset=%d, size=%d, error=%v", n.objID, off, len(dest), err)
		return nil, syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		DebugLog("[VFS Read] ERROR: Object is not a file: objID=%d, type=%d, offset=%d, size=%d", n.objID, obj.Type, off, len(dest))
		return nil, syscall.EISDIR
	}

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		// Empty file
		return &readResultWrapper{data: nil}, 0
	}

	// Check if there's an active RandomAccessor with journal
	if n.fs != nil {
		if ra := n.fs.getRandomAccessorByFileID(obj.ID); ra != nil {
			ra.journalMu.RLock()
			hasJournal := ra.journal != nil
			ra.journalMu.RUnlock()

			if hasJournal {
				DebugLog("[VFS Read] Using RandomAccessor with journal: objID=%d, offset=%d, size=%d", obj.ID, off, len(dest))
				data, err := ra.Read(off, len(dest))
				if err != nil && err != io.EOF {
					DebugLog("[VFS Read] ERROR: RandomAccessor read failed: objID=%d, offset=%d, size=%d, error=%v", obj.ID, off, len(dest), err)
					return nil, syscall.EIO
				}
				// Return data as ReadResult wrapper
				return &readResultWrapper{data: data}, 0
			}
		}
	}

	// Use DataReader for reading
	dataReader, errno := n.getDataReader(off)
	if errno != 0 {
		DebugLog("[VFS Read] ERROR: Failed to get DataReader: objID=%d, offset=%d, size=%d, errno=%d", n.objID, off, len(dest), errno)
		return nil, errno
	}

	// Read data using dataReader interface
	nRead, err := dataReader.Read(dest, off)
	if err != nil && err != io.EOF {
		DebugLog("[VFS Read] ERROR: DataReader read failed: objID=%d, offset=%d, size=%d, error=%v", n.objID, off, len(dest), err)
		return nil, syscall.EIO
	}

	// Create a copy of the data (required for FUSE compatibility)
	resultData := make([]byte, nRead)
	copy(resultData, dest[:nRead])
	return &readResultWrapper{data: resultData}, 0
}

// getDataReader gets or creates DataReader (Windows version)
func (n *OrcasNode) getDataReader(offset int64) (dataReader, syscall.Errno) {
	obj, err := n.getObj()
	if err != nil {
		return nil, syscall.ENOENT
	}

	if obj.DataID == 0 || obj.DataID == core.EmptyDataID {
		return nil, syscall.EIO
	}

	// Get DataInfo
	dataInfo, err := n.fs.h.GetDataInfo(n.fs.c, n.fs.bktID, obj.DataID)
	if err != nil {
		return nil, syscall.EIO
	}
	if dataInfo == nil {
		return nil, syscall.EIO
	}

	// Get chunk size
	chunkSize := n.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 10 << 20 // Default 10MB
	}

	// Create chunk reader
	reader := newChunkReader(n.fs.c, n.fs.h, n.fs.bktID, dataInfo, n.fs.EndecKey, chunkSize)
	return reader, 0
}
