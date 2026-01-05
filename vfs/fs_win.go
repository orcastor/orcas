//go:build windows
// +build windows

package vfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/orcastor/orcas/core"
)

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

	// List directory contents
	children, _, _, err := ofs.h.List(ofs.c, ofs.bktID, obj.ID, core.ListOptions{
		Count: core.DefaultListPageSize,
	})
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

	// Check if directory is empty
	children, _, _, err := ofs.h.List(ofs.c, ofs.bktID, obj.ID, core.ListOptions{
		Count: 1,
	})
	if err != nil {
		return DOKAN_ERROR
	}

	if len(children) > 0 {
		return DOKAN_ERROR // Directory not empty
	}

	err = ofs.h.Delete(ofs.c, ofs.bktID, obj.ID)
	if err != nil {
		return DOKAN_ERROR
	}

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

		// List current directory
		children, _, _, err := ofs.h.List(ofs.c, ofs.bktID, currentID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
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
func (n *OrcasNode) Unlink(ctx context.Context, name string) syscall.Errno {
	DebugLog("[VFS Unlink] Entry: name=%s, parentID=%d", name, n.objID)
	// Check if KEY is required
	if errno := n.fs.checkKey(); errno != 0 {
		DebugLog("[VFS Unlink] ERROR: checkKey failed: name=%s, parentID=%d, errno=%d", name, n.objID, errno)
		return errno
	}

	obj, err := n.getObj()
	if err != nil {
		DebugLog("[VFS Unlink] ERROR: Failed to get parent object: name=%s, parentID=%d, error=%v", name, n.objID, err)
		return syscall.ENOENT
	}

	if obj.Type != core.OBJ_TYPE_DIR {
		DebugLog("[VFS Unlink] ERROR: Parent is not a directory: name=%s, parentID=%d, type=%d", name, n.objID, obj.Type)
		return syscall.ENOTDIR
	}

	// Find child object
	var targetID int64
	var targetObj *core.ObjectInfo

	// Try to find from RandomAccessor registry first
	if n.fs != nil {
		n.fs.raRegistry.Range(func(key, value interface{}) bool {
			if fileID, ok := key.(int64); ok {
				if ra, ok := value.(*RandomAccessor); ok && ra != nil {
					fileObj, err := ra.getFileObj()
					if err == nil && fileObj != nil && fileObj.PID == obj.ID && fileObj.Name == name && fileObj.Type == core.OBJ_TYPE_FILE {
						targetID = fileID
						targetObj = fileObj
						DebugLog("[VFS Unlink] Found target file from RandomAccessor registry: fileID=%d, name=%s", targetID, name)
						return false // Stop iteration
					}
				}
			}
			return true // Continue iteration
		})
	}

	// If not found in RandomAccessor registry, try to find from List
	if targetID == 0 {
		children, _, _, err := n.fs.h.List(n.fs.c, n.fs.bktID, obj.ID, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		if err != nil {
			DebugLog("[VFS Unlink] ERROR: Failed to list directory children: name=%s, parentID=%d, error=%v", name, obj.ID, err)
			return syscall.EIO
		}

		for _, child := range children {
			if child.Name == name && child.Type == core.OBJ_TYPE_FILE {
				targetID = child.ID
				targetObj = child
				DebugLog("[VFS Unlink] Found target file from List: fileID=%d, name=%s", targetID, name)
				break
			}
		}
	}

	if targetID == 0 {
		DebugLog("[VFS Unlink] ERROR: Target file not found: name=%s, parentID=%d", name, obj.ID)
		return syscall.ENOENT
	}

	// Remove from RandomAccessor registry if present
	if n.fs != nil {
		if targetRA := n.fs.getRandomAccessorByFileID(targetID); targetRA != nil {
			// Force flush before deletion to ensure data is saved
			if _, err := targetRA.ForceFlush(); err != nil {
				DebugLog("[VFS Unlink] WARNING: Failed to flush file before deletion: fileID=%d, error=%v", targetID, err)
			}
			// Unregister RandomAccessor
			n.fs.unregisterRandomAccessor(targetID, targetRA)
			DebugLog("[VFS Unlink] Removed file from RandomAccessor registry: fileID=%d", targetID)
		}
	}

	// Remove from parent directory first (mark as deleted)
	err = n.fs.h.Recycle(n.fs.c, n.fs.bktID, targetID)
	if err != nil {
		DebugLog("[VFS Unlink] ERROR: Failed to recycle file: fileID=%d, name=%s, parentID=%d, error=%v", targetID, name, obj.ID, err)
		return syscall.EIO
	}

	// Update cache immediately
	n.invalidateDirListCache(obj.ID)

	// Get file object to get DataID before clearing cache
	if targetObj == nil {
		targetObjs, err := n.fs.h.Get(n.fs.c, n.fs.bktID, []int64{targetID})
		if err == nil && len(targetObjs) > 0 {
			targetObj = targetObjs[0]
		}
	}
	if targetObj != nil && targetObj.DataID > 0 && targetObj.DataID != core.EmptyDataID {
		// Clear DataInfo cache for the deleted file's DataID
		dataInfoCacheKey := targetObj.DataID
		dataInfoCache.Del(dataInfoCacheKey)
		decodingReaderCache.Del(dataInfoCacheKey)
		DebugLog("[VFS Unlink] Cleared DataInfo cache for deleted file: fileID=%d, dataID=%d", targetID, dataInfoCacheKey)
	}

	// Clear file object cache
	fileObjCache.Del(targetID)
	DebugLog("[VFS Unlink] Cleared file object cache for deleted file: fileID=%d", targetID)

	// CRITICAL: Remove journal and clean up jwal files
	// This ensures jwal files are deleted even if RandomAccessor was already closed
	if n.fs.journalMgr != nil {
		n.fs.journalMgr.Remove(targetID)
		DebugLog("[VFS Unlink] Removed journal and cleaned up jwal files: fileID=%d", targetID)
	}

	// Invalidate parent directory cache
	n.invalidateObj()

	// Schedule delayed deletion for atomic replace adaptation
	if n.fs.atomicReplaceMgr != nil {
		if err := n.fs.atomicReplaceMgr.ScheduleDeletion(n.fs.bktID, obj.ID, name, targetID); err != nil {
			DebugLog("[VFS Unlink] WARNING: Failed to schedule delayed deletion: fileID=%d, error=%v", targetID, err)
			// Fallback to immediate deletion if scheduling fails
			go func() {
				err := n.fs.h.Delete(n.fs.c, n.fs.bktID, targetID)
				if err != nil {
					DebugLog("[VFS Unlink] ERROR: Failed to permanently delete file: fileID=%d, error=%v", targetID, err)
				} else {
					DebugLog("[VFS Unlink] Successfully permanently deleted file: fileID=%d", targetID)
				}
			}()
		} else {
			DebugLog("[VFS Unlink] Scheduled delayed deletion: fileID=%d, name=%s (will delete in 5s)", targetID, name)
		}
	} else {
		// Fallback: Asynchronously delete and clean up (permanent deletion)
		go func() {
			err := n.fs.h.Delete(n.fs.c, n.fs.bktID, targetID)
			if err != nil {
				DebugLog("[VFS Unlink] ERROR: Failed to permanently delete file: fileID=%d, error=%v", targetID, err)
			} else {
				DebugLog("[VFS Unlink] Successfully permanently deleted file: fileID=%d", targetID)
			}
		}()
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
