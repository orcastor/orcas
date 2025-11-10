package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/orca-zhang/ecache"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
	"github.com/orcastor/orcas/s3/util"
	"github.com/orcastor/orcas/sdk"
)

var (
	handler = core.NewLocalHandler()

	// pathCache caches path to object mapping
	// key: "<bktID>:<path>", value: *core.ObjectInfo
	pathCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// dirListCache caches directory listing
	// key: "<bktID>:<pid>", value: []*core.ObjectInfo
	dirListCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// objectCache caches object by ID
	// key: "<bktID>:<objectID>", value: *core.ObjectInfo
	objectCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// bucketListCache caches bucket list by user ID
	// key: "<uid>", value: []*core.BucketInfo
	bucketListCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// bucketCache caches bucket by name and user ID
	// key: "<uid>:<bucketName>", value: *core.BucketInfo
	bucketCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// Batch write size thresholds (exported for use in handler.go)
	maxBatchWriteFileSize = core.GetWriteBufferConfig().MaxBufferSize
	minBatchWriteFileSize = int64(0)

	// multipartUploads stores ongoing multipart uploads
	// key: "<bktID>:<uploadId>", value: *MultipartUpload
	multipartUploads sync.Map
)

// MultipartUpload represents an ongoing multipart upload
type MultipartUpload struct {
	UploadID    string            // Upload ID
	BucketID    int64             // Bucket ID
	Key         string            // Object key
	Initiated   time.Time         // Initiation time
	Parts       []*Part           // Uploaded parts
	ContentType string            // Content type
	Metadata    map[string]string // User metadata
	mu          sync.RWMutex      // Mutex for thread-safe access
}

// Part represents a single part of a multipart upload
type Part struct {
	PartNumber int       // Part number (1-based)
	ETag       string    // ETag of the part
	DataID     int64     // Data ID in storage
	Size       int64     // Part size
	UploadTime time.Time // Upload time
}

// formatPathCacheKey formats cache key for path lookup
func formatPathCacheKey(bktID int64, path string) string {
	return fmt.Sprintf("%d:%s", bktID, path)
}

// formatDirListCacheKey formats cache key for directory listing
func formatDirListCacheKey(bktID, pid int64) string {
	return fmt.Sprintf("%d:%d", bktID, pid)
}

// formatObjectCacheKey formats cache key for object by ID
func formatObjectCacheKey(bktID, objectID int64) string {
	return fmt.Sprintf("%d:%d", bktID, objectID)
}

// formatBucketListCacheKey formats cache key for bucket list
func formatBucketListCacheKey(uid int64) string {
	return fmt.Sprintf("%d", uid)
}

// formatBucketCacheKey formats cache key for bucket by name
func formatBucketCacheKey(uid int64, bucketName string) string {
	return fmt.Sprintf("%d:%s", uid, bucketName)
}

// invalidatePathCache invalidates cache for a path and its parent directories
func invalidatePathCache(bktID int64, path string) {
	// Invalidate the path itself
	pathCache.Del(formatPathCacheKey(bktID, path))

	// Invalidate all parent directories
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := len(parts); i > 0; i-- {
		parentPath := strings.Join(parts[:i], "/")
		pathCache.Del(formatPathCacheKey(bktID, parentPath))
	}

	// Invalidate directory listing for all parent directories
	var pid int64 = 0
	for i := 0; i < len(parts); i++ {
		dirListCache.Del(formatDirListCacheKey(bktID, pid))
		// Note: We can't easily get parent directory IDs here without querying
		// The directory listing cache will be naturally invalidated on next access
	}
}

// invalidateObjectCache invalidates object cache by ID
func invalidateObjectCache(bktID, objectID int64) {
	objectCache.Del(formatObjectCacheKey(bktID, objectID))
}

// invalidateBucketListCache invalidates bucket list cache for a user
func invalidateBucketListCache(uid int64) {
	bucketListCache.Del(formatBucketListCacheKey(uid))
}

// invalidateBucketCache invalidates bucket cache by name
func invalidateBucketCache(uid int64, bucketName string) {
	bucketCache.Del(formatBucketCacheKey(uid, bucketName))
}

// invalidateDirListCache invalidates directory listing cache
func invalidateDirListCache(bktID, pid int64) {
	dirListCache.Del(formatDirListCacheKey(bktID, pid))
}

// getBucketByName gets bucket by name (with cache)
func getBucketByName(c *gin.Context, name string) (*core.BucketInfo, error) {
	uid := middleware.GetUID(c)
	if uid == 0 {
		return nil, fmt.Errorf("unauthorized")
	}

	// Try cache first
	cacheKey := formatBucketCacheKey(uid, name)
	if cached, ok := bucketCache.Get(cacheKey); ok {
		if bkt, ok := cached.(*core.BucketInfo); ok && bkt != nil {
			// Verify the cached bucket still exists in current database
			// This is important for tests where each test has its own database
			ctx := c.Request.Context()
			ma := &core.DefaultMetadataAdapter{}
			buckets, err := ma.GetBkt(ctx, []int64{bkt.ID})
			if err == nil && len(buckets) > 0 && buckets[0].ID == bkt.ID {
				// Bucket exists in current database, use cached value
				return bkt, nil
			}
			// Bucket doesn't exist in current database, clear cache and query again
			bucketCache.Del(cacheKey)
			// Also clear bucket list cache to force fresh query
			bucketListCache.Del(formatBucketListCacheKey(uid))
		}
	}

	// Get all buckets for the user (with cache)
	allBuckets, err := getBucketsByUser(c, uid)
	if err != nil {
		return nil, err
	}

	// Find bucket by name
	for _, bkt := range allBuckets {
		if bkt.Name == name {
			// Cache the bucket
			bucketCache.Put(cacheKey, bkt)
			return bkt, nil
		}
	}

	return nil, fmt.Errorf("bucket not found")
}

// getBucketsByUser gets buckets for a user (with cache)
func getBucketsByUser(c *gin.Context, uid int64) ([]*core.BucketInfo, error) {
	ctx := c.Request.Context()

	// Try cache first
	cacheKey := formatBucketListCacheKey(uid)
	if cached, ok := bucketListCache.Get(cacheKey); ok {
		if buckets, ok := cached.([]*core.BucketInfo); ok {
			return buckets, nil
		}
	}

	// Query from database
	ma := &core.DefaultMetadataAdapter{}
	buckets, err := ma.ListBkt(ctx, uid)
	if err != nil {
		return nil, err
	}

	// Update cache
	bucketListCache.Put(cacheKey, buckets)

	return buckets, nil
}

// getBucketIDByName gets bucket ID by name
func getBucketIDByName(c *gin.Context, name string) (int64, error) {
	bkt, err := getBucketByName(c, name)
	if err != nil {
		return 0, err
	}
	return bkt.ID, nil
}

// findObjectByPath finds object by path in bucket (with cache)
func findObjectByPath(c *gin.Context, bktID int64, key string) (*core.ObjectInfo, error) {
	ctx := c.Request.Context()
	parts := strings.Split(strings.Trim(key, "/"), "/")
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		// Root directory
		return &core.ObjectInfo{
			ID:   0,
			PID:  0,
			Type: core.OBJ_TYPE_DIR,
			Name: "",
		}, nil
	}

	// Try cache first
	cacheKey := formatPathCacheKey(bktID, key)
	if cached, ok := pathCache.Get(cacheKey); ok {
		if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}

	var pid int64 = 0
	var found *core.ObjectInfo
	for i, part := range parts {
		if part == "" {
			continue
		}

		// Try directory listing cache
		dirCacheKey := formatDirListCacheKey(bktID, pid)
		var objs []*core.ObjectInfo
		if cached, ok := dirListCache.Get(dirCacheKey); ok {
			if cachedObjs, ok := cached.([]*core.ObjectInfo); ok {
				objs = cachedObjs
			}
		}

		// If cache miss, query from database
		if objs == nil {
			opt := core.ListOptions{
				Count: core.DefaultListPageSize,
			}
			var err error
			objs, _, _, err = handler.List(ctx, bktID, pid, opt)
			if err != nil {
				return nil, err
			}
			// Update cache
			dirListCache.Put(dirCacheKey, objs)
		}

		// Find matching object by exact name
		found = nil
		for _, obj := range objs {
			if obj.Name == part && obj.PID == pid {
				found = obj
				break
			}
		}

		if found == nil {
			if i == len(parts)-1 {
				// Last part not found, return nil
				return nil, fmt.Errorf("object not found")
			}
			return nil, fmt.Errorf("path not found: %s", strings.Join(parts[:i+1], "/"))
		}

		if i == len(parts)-1 {
			// Last part, cache and return the object
			pathCache.Put(cacheKey, found)
			// Also cache by object ID
			objectCache.Put(formatObjectCacheKey(bktID, found.ID), found)
			return found, nil
		}

		// Check if it's a directory
		if found.Type != core.OBJ_TYPE_DIR {
			return nil, fmt.Errorf("not a directory: %s", strings.Join(parts[:i+1], "/"))
		}

		pid = found.ID
	}

	return nil, fmt.Errorf("object not found")
}

// ensurePath ensures the path exists, creating directories as needed (with cache)
func ensurePath(c *gin.Context, bktID int64, key string) (int64, error) {
	ctx := c.Request.Context()
	parts := strings.Split(strings.Trim(key, "/"), "/")
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		return 0, nil
	}

	var pid int64 = 0
	for i, part := range parts {
		if part == "" {
			continue
		}

		// Try directory listing cache
		dirCacheKey := formatDirListCacheKey(bktID, pid)
		var objs []*core.ObjectInfo
		if cached, ok := dirListCache.Get(dirCacheKey); ok {
			if cachedObjs, ok := cached.([]*core.ObjectInfo); ok {
				objs = cachedObjs
			}
		}

		// If cache miss, query from database
		if objs == nil {
			opt := core.ListOptions{
				Count: core.DefaultListPageSize,
			}
			var err error
			objs, _, _, err = handler.List(ctx, bktID, pid, opt)
			if err != nil {
				return 0, err
			}
			// Update cache
			dirListCache.Put(dirCacheKey, objs)
		}

		// Find matching object by exact name
		var found *core.ObjectInfo
		for _, obj := range objs {
			if obj.Name == part && obj.PID == pid {
				found = obj
				break
			}
		}

		if found == nil {
			// Create directory
			if i < len(parts)-1 {
				// Not the last part, create directory
				dirObj := &core.ObjectInfo{
					ID:    core.NewID(),
					PID:   pid,
					Type:  core.OBJ_TYPE_DIR,
					Name:  part,
					MTime: core.Now(),
				}
				ids, err := handler.Put(ctx, bktID, []*core.ObjectInfo{dirObj})
				if err != nil {
					return 0, err
				}
				pid = ids[0]
				// Invalidate cache after creating directory
				invalidateDirListCache(bktID, pid)
				parentPath := strings.Join(parts[:i+1], "/")
				invalidatePathCache(bktID, parentPath)
			} else {
				// Last part, will be created by caller
				return pid, nil
			}
		} else {
			if i < len(parts)-1 {
				if found.Type != core.OBJ_TYPE_DIR {
					return 0, fmt.Errorf("not a directory: %s", strings.Join(parts[:i+1], "/"))
				}
				pid = found.ID
			} else {
				// Last part exists
				return pid, nil
			}
		}
	}

	return pid, nil
}

// S3 XML Response structures
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Buckets Buckets  `xml:"Buckets"`
	Owner   Owner    `xml:"Owner"`
}

type Buckets struct {
	Bucket []Bucket `xml:"Bucket"`
}

type Bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type ListBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix,omitempty"`
	Marker         string         `xml:"Marker,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []Content      `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type Content struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// listBuckets handles GET / - ListBuckets (with cache)
func listBuckets(c *gin.Context) {
	uid := middleware.GetUID(c)
	if uid == 0 {
		util.S3ErrorResponse(c, http.StatusForbidden, "AccessDenied", "Access Denied")
		return
	}

	// Get buckets from cache or database
	buckets, err := getBucketsByUser(c, uid)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	result := ListAllMyBucketsResult{
		Buckets: Buckets{
			Bucket: make([]Bucket, 0, len(buckets)),
		},
		Owner: Owner{
			ID:          strconv.FormatInt(uid, 10),
			DisplayName: strconv.FormatInt(uid, 10),
		},
	}

	for _, bkt := range buckets {
		// Get creation time from bucket (we'll use current time as placeholder)
		result.Buckets.Bucket = append(result.Buckets.Bucket, Bucket{
			Name:         bkt.Name,
			CreationDate: time.Now().UTC().Format(time.RFC3339),
		})
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}

// createBucket handles PUT /{bucket} - CreateBucket
func createBucket(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidBucketName", "Bucket name is required")
		return
	}

	ctx := c.Request.Context()
	uid := middleware.GetUID(c)
	if uid == 0 {
		util.S3ErrorResponse(c, http.StatusForbidden, "AccessDenied", "Access Denied")
		return
	}

	// Check if bucket already exists
	_, err := getBucketByName(c, bucketName)
	if err == nil {
		util.S3ErrorResponse(c, http.StatusConflict, "BucketAlreadyExists", "The requested bucket name is not available")
		return
	}

	// Create new bucket
	bkt := &core.BucketInfo{
		ID:   core.NewID(),
		Name: bucketName,
		UID:  uid,
		Type: 1,
	}

	admin := core.NewLocalAdmin()
	if err := admin.PutBkt(ctx, []*core.BucketInfo{bkt}); err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Invalidate cache after creating bucket
	invalidateBucketListCache(uid)
	invalidateBucketCache(uid, bucketName)
	// Also cache the new bucket
	bucketCache.Put(formatBucketCacheKey(uid, bucketName), bkt)

	c.Status(http.StatusOK)
}

// deleteBucket handles DELETE /{bucket} - DeleteBucket
func deleteBucket(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidBucketName", "Bucket name is required")
		return
	}

	ctx := c.Request.Context()
	uid := middleware.GetUID(c)
	if uid == 0 {
		util.S3ErrorResponse(c, http.StatusForbidden, "AccessDenied", "Access Denied")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	admin := core.NewLocalAdmin()
	if err := admin.DeleteBkt(ctx, bktID); err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Invalidate cache after deleting bucket
	invalidateBucketListCache(uid)
	invalidateBucketCache(uid, bucketName)

	c.Status(http.StatusNoContent)
}

// listObjects handles GET /{bucket} - ListObjects
func listObjects(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidBucketName", "Bucket name is required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	prefix := c.Query("prefix")
	marker := c.Query("marker")
	maxKeys := core.DefaultListPageSize
	if mk := c.Query("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n > 0 {
			maxKeys = n
		}
	}
	delimiter := c.Query("delimiter")

	ctx := c.Request.Context()
	pid := int64(0)
	if prefix != "" {
		// Find parent directory for prefix
		obj, err := findObjectByPath(c, bktID, prefix)
		if err == nil && obj != nil && obj.Type == core.OBJ_TYPE_DIR {
			pid = obj.ID
		}
	}

	// Merge pending objects from batch writer FIRST (before querying database)
	// This ensures pending objects are always visible, even if cache is empty
	batchMgr := sdk.GetBatchWriterForBucket(handler, bktID)
	pendingObjsMap := make(map[int64]*sdk.PackagedFileInfo)
	if batchMgr != nil {
		pendingObjsMap = batchMgr.GetPendingObjects()
	}

	// Try directory listing cache first
	dirCacheKey := formatDirListCacheKey(bktID, pid)
	var objs []*core.ObjectInfo
	var cnt int64
	if cached, ok := dirListCache.Get(dirCacheKey); ok {
		if cachedObjs, ok := cached.([]*core.ObjectInfo); ok {
			objs = cachedObjs
			cnt = int64(len(objs))
		}
	}

	// If cache miss or need to filter by prefix/delimiter, query from database
	if objs == nil {
		opt := core.ListOptions{
			Word:  prefix,
			Delim: delimiter,
			Count: maxKeys,
			Order: "+name",
		}

		var err error
		objs, cnt, _, err = handler.List(ctx, bktID, pid, opt)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		// Update cache
		dirListCache.Put(dirCacheKey, objs)
	} else {
		// Filter cached objects by prefix and delimiter if needed
		if prefix != "" || delimiter != "" {
			filteredObjs := make([]*core.ObjectInfo, 0)
			for _, obj := range objs {
				// Filter by prefix
				if prefix != "" && !strings.HasPrefix(obj.Name, prefix) {
					continue
				}
				// Filter by delimiter (simplified, full implementation would handle CommonPrefixes)
				if delimiter != "" {
					// Check if object name contains delimiter after prefix
					if prefix != "" {
						relativeName := strings.TrimPrefix(obj.Name, prefix)
						if strings.Contains(relativeName, delimiter) {
							// This would be a CommonPrefix, skip for now
							continue
						}
					} else if strings.Contains(obj.Name, delimiter) {
						// This would be a CommonPrefix, skip for now
						continue
					}
				}
				filteredObjs = append(filteredObjs, obj)
			}
			objs = filteredObjs
			cnt = int64(len(objs))
		}
	}

	// Merge pending objects from batch write manager (before flush)
	// Only add pending objects that are not already in the database results
	// Create a set of existing object IDs for fast lookup
	existingIDs := make(map[int64]bool)
	for _, obj := range objs {
		existingIDs[obj.ID] = true
	}

	for fileID, fileInfo := range pendingObjsMap {
		// Skip if object already exists in database results
		if existingIDs[fileID] {
			continue
		}

		// Check if object belongs to this directory (PID matches)
		if fileInfo.PID == pid {
			// Check if object matches prefix and delimiter filters
			matches := true
			if prefix != "" && !strings.HasPrefix(fileInfo.Name, prefix) {
				matches = false
			}
			if matches && delimiter != "" {
				if prefix != "" {
					relativeName := strings.TrimPrefix(fileInfo.Name, prefix)
					if strings.Contains(relativeName, delimiter) {
						matches = false
					}
				} else if strings.Contains(fileInfo.Name, delimiter) {
					matches = false
				}
			}
			if matches {
				// Create ObjectInfo from PackagedFileInfo
				objInfo := &core.ObjectInfo{
					ID:     fileInfo.ObjectID,
					PID:    fileInfo.PID,
					Name:   fileInfo.Name,
					Type:   core.OBJ_TYPE_FILE,
					DataID: fileInfo.DataID,
					Size:   fileInfo.Size,
					MTime:  core.Now(),
				}
				objs = append(objs, objInfo)
				cnt++
				// Add to existing IDs to avoid duplicates
				existingIDs[fileID] = true
			}
		}
	}

	result := ListBucketResult{
		Name:        bucketName,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     maxKeys,
		IsTruncated: cnt > int64(maxKeys),
		Contents:    make([]Content, 0),
	}

	for _, obj := range objs {
		if obj.Type == core.OBJ_TYPE_FILE {
			result.Contents = append(result.Contents, Content{
				Key:          obj.Name,
				LastModified: time.Unix(obj.MTime, 0).UTC().Format(time.RFC1123),
				ETag:         fmt.Sprintf(`"%x"`, obj.DataID),
				Size:         obj.Size,
				StorageClass: "STANDARD",
			})
		}
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}

// getObject handles GET /{bucket}/{key} - GetObject
func getObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	ctx := c.Request.Context()
	obj, err := findObjectByPath(c, bktID, key)
	if err != nil {
		// Object not found in database, check batch writer for pending objects
		batchMgr := sdk.GetBatchWriterForBucket(handler, bktID)
		if batchMgr != nil {
			// Try to find object in pending objects by name
			fileName := filepath.Base(key)
			parentPath := filepath.Dir(key)
			var pid int64 = 0
			if parentPath != "." && parentPath != "/" {
				parentObj, err := findObjectByPath(c, bktID, parentPath)
				if err == nil && parentObj != nil && parentObj.Type == core.OBJ_TYPE_DIR {
					pid = parentObj.ID
				}
			}

			// Search pending objects
			pendingObjs := batchMgr.GetPendingObjects()
			for _, pkgInfo := range pendingObjs {
				if pkgInfo != nil && pkgInfo.Name == fileName && pkgInfo.PID == pid {
					// Found in pending objects, create ObjectInfo
					obj = &core.ObjectInfo{
						ID:     pkgInfo.ObjectID,
						PID:    pkgInfo.PID,
						Name:   pkgInfo.Name,
						Type:   core.OBJ_TYPE_FILE,
						DataID: pkgInfo.DataID,
						Size:   pkgInfo.Size,
						MTime:  core.Now(),
					}
					err = nil
					break
				}
			}
		}

		if err != nil {
			util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchKey", "The specified key does not exist")
			return
		}
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "The specified key is not a file")
		return
	}

	// Cache the object for future lookups by ID
	objectCache.Put(formatObjectCacheKey(bktID, obj.ID), obj)

	// Get object size
	objectSize := obj.Size
	if objectSize <= 0 {
		// If size is not available, we need to get it from DataInfo
		dataInfo, err := handler.GetDataInfo(ctx, bktID, obj.DataID)
		if err == nil && dataInfo != nil {
			objectSize = dataInfo.Size
		}
		// If still not available, we'll try to get it later when reading data
	}

	// Parse Range header if present
	rangeHeader := c.GetHeader("Range")
	var rangeSpec *util.RangeSpec
	if rangeHeader != "" {
		rangeSpec = util.ParseRangeHeader(rangeHeader, objectSize)
		if rangeSpec != nil && !rangeSpec.Valid {
			// Range not satisfiable
			c.Header("Content-Range", util.FormatContentRangeHeader(0, objectSize-1, objectSize))
			util.S3ErrorResponse(c, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range cannot be satisfied")
			return
		}
	}

	// Read object data
	// First check if object is in batch writer (pending flush)
	var data []byte
	var startOffset, readSize int64
	batchMgr := sdk.GetBatchWriterForBucket(handler, bktID)
	if batchMgr != nil {
		// Try to read from pending buffer
		data = batchMgr.ReadPendingData(obj.ID)
		if data != nil {
			// If objectSize is not available, use buffer size
			if objectSize <= 0 {
				objectSize = int64(len(data))
			}
			// Data read from buffer, apply range if specified
			if rangeSpec != nil && rangeSpec.Valid {
				startOffset = rangeSpec.Start
				readSize = rangeSpec.End - rangeSpec.Start + 1
				if startOffset >= int64(len(data)) {
					c.Header("Content-Range", util.FormatContentRangeHeader(0, objectSize-1, objectSize))
					util.S3ErrorResponse(c, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range cannot be satisfied")
					return
				}
				if startOffset+readSize > int64(len(data)) {
					readSize = int64(len(data)) - startOffset
				}
				data = data[startOffset : startOffset+readSize]
			} else {
				readSize = int64(len(data))
			}

			// Set response headers
			c.Header("Content-Type", "application/octet-stream")
			c.Header("Content-Length", strconv.FormatInt(readSize, 10))
			c.Header("ETag", fmt.Sprintf(`"%x"`, obj.DataID))
			c.Header("Last-Modified", time.Unix(obj.MTime, 0).UTC().Format(time.RFC1123))
			c.Header("Accept-Ranges", "bytes")

			if rangeSpec != nil && rangeSpec.Valid {
				// Return 206 Partial Content
				// Use original object size for Content-Range, not buffer size
				c.Header("Content-Range", util.FormatContentRangeHeader(startOffset, startOffset+readSize-1, objectSize))
				c.Data(http.StatusPartialContent, "application/octet-stream", data)
			} else {
				c.Data(http.StatusOK, "application/octet-stream", data)
			}
			return
		}
	}

	// Object not in buffer or not found, read from storage
	// First get DataInfo to check if it's packaged data
	dataInfo, err := handler.GetDataInfo(ctx, bktID, obj.DataID)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Determine actual object size from DataInfo if not available
	if objectSize <= 0 {
		objectSize = dataInfo.Size
	}

	// Check if data is packaged (PkgID > 0)
	if dataInfo.PkgID > 0 {
		// Read from packaged data block
		if rangeSpec != nil && rangeSpec.Valid {
			// Calculate offset within the package
			pkgStartOffset := int(dataInfo.PkgOffset) + int(rangeSpec.Start)
			pkgReadSize := int(rangeSpec.End - rangeSpec.Start + 1)
			// Ensure we don't read beyond the package data
			if pkgStartOffset+pkgReadSize > int(dataInfo.PkgOffset)+int(dataInfo.Size) {
				pkgReadSize = int(dataInfo.PkgOffset) + int(dataInfo.Size) - pkgStartOffset
			}
			data, err = handler.GetData(ctx, bktID, dataInfo.PkgID, 0, pkgStartOffset, pkgReadSize)
			startOffset = rangeSpec.Start
			readSize = int64(pkgReadSize)
		} else {
			// Read entire packaged data
			data, err = handler.GetData(ctx, bktID, dataInfo.PkgID, 0, int(dataInfo.PkgOffset), int(dataInfo.Size))
			readSize = int64(len(data))
		}
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	} else {
		// Read normal data
		if rangeSpec != nil && rangeSpec.Valid {
			// Read specific range - need to handle multiple chunks
			// Get chunk size from bucket configuration
			bktInfo, err := handler.GetBktInfo(ctx, bktID)
			chunkSize := int64(4 * 1024 * 1024) // Default 4MB
			if err == nil && bktInfo != nil && bktInfo.ChunkSize > 0 {
				chunkSize = bktInfo.ChunkSize
			}

			offset := rangeSpec.Start
			size := rangeSpec.End - rangeSpec.Start + 1
			startOffset = offset
			readSize = size

			// Calculate which chunks we need to read
			startChunk := int(offset / chunkSize)
			endChunk := int((offset + size - 1) / chunkSize)

			// Read data from multiple chunks and concatenate
			var allData []byte
			for chunkNum := startChunk; chunkNum <= endChunk; chunkNum++ {
				// Calculate offset within this chunk
				chunkStartOffset := offset - int64(chunkNum)*chunkSize
				var chunkReadSize int64

				if chunkNum == startChunk && chunkNum == endChunk {
					// Single chunk case
					chunkReadSize = size
				} else if chunkNum == startChunk {
					// First chunk - read from offset to end of chunk
					chunkReadSize = chunkSize - chunkStartOffset
				} else if chunkNum == endChunk {
					// Last chunk - read from start to end offset
					chunkEndOffset := (offset + size - 1) - int64(chunkNum)*chunkSize
					chunkReadSize = chunkEndOffset + 1
				} else {
					// Middle chunk - read entire chunk
					chunkReadSize = chunkSize
					chunkStartOffset = 0
				}

				// Read from this chunk
				chunkData, err := handler.GetData(ctx, bktID, obj.DataID, chunkNum, int(chunkStartOffset), int(chunkReadSize))
				if err != nil {
					// If chunk doesn't exist, it might be the last chunk which could be smaller
					// Try to read from start of chunk to end (read entire chunk)
					if chunkNum == endChunk {
						chunkData, err = handler.GetData(ctx, bktID, obj.DataID, chunkNum)
						if err == nil && int64(len(chunkData)) > chunkStartOffset {
							// Adjust read size if chunk is smaller than expected
							actualChunkReadSize := int64(len(chunkData)) - chunkStartOffset
							if actualChunkReadSize < chunkReadSize {
								chunkReadSize = actualChunkReadSize
							}
							chunkData = chunkData[chunkStartOffset : chunkStartOffset+chunkReadSize]
						} else {
							util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to read chunk %d: %v", chunkNum, err))
							return
						}
					} else {
						util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to read chunk %d: %v", chunkNum, err))
						return
					}
				}

				allData = append(allData, chunkData...)
			}

			data = allData
		} else {
			// Read entire data - need to read all chunks
			// Get chunk size from bucket configuration
			// If GetBktInfo fails, use default chunkSize (4MB)
			chunkSize := int64(4 * 1024 * 1024) // Default 4MB
			bktInfo, bktErr := handler.GetBktInfo(ctx, bktID)
			if bktErr == nil && bktInfo != nil && bktInfo.ChunkSize > 0 {
				chunkSize = bktInfo.ChunkSize
			}
			// If GetBktInfo fails, continue with default chunkSize

			// Read all chunks and concatenate
			var allChunks []byte
			chunkIdx := 0
			for {
				chunkData, chunkErr := handler.GetData(ctx, bktID, obj.DataID, chunkIdx)
				if chunkErr != nil {
					// No more chunks, break
					break
				}
				if len(chunkData) == 0 {
					// Empty chunk, break
					break
				}
				allChunks = append(allChunks, chunkData...)
				chunkIdx++
				// If chunk is smaller than chunkSize, it's the last chunk
				if int64(len(chunkData)) < chunkSize {
					break
				}
			}
			data = allChunks
			readSize = int64(len(data))
		}
	}

	// Set response headers
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Length", strconv.FormatInt(readSize, 10))
	c.Header("ETag", fmt.Sprintf(`"%x"`, obj.DataID))
	c.Header("Last-Modified", time.Unix(obj.MTime, 0).UTC().Format(time.RFC1123))
	c.Header("Accept-Ranges", "bytes")

	if rangeSpec != nil && rangeSpec.Valid {
		// Return 206 Partial Content
		c.Header("Content-Range", util.FormatContentRangeHeader(startOffset, startOffset+readSize-1, objectSize))
		c.Data(http.StatusPartialContent, "application/octet-stream", data)
	} else {
		c.Data(http.StatusOK, "application/octet-stream", data)
	}
}

// putObject handles PUT /{bucket}/{key} - PutObject
func putObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	ctx := c.Request.Context()

	// Read request body
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}

	// Ensure parent directory exists
	parentPath := filepath.Dir(key)
	var pid int64 = 0
	if parentPath != "." && parentPath != "/" {
		pid, err = ensurePath(c, bktID, parentPath)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Check if object exists
	fileName := filepath.Base(key)
	obj, err := findObjectByPath(c, bktID, key)
	var objID int64
	if err == nil && obj != nil {
		// Object exists, update it
		objID = obj.ID
		// Invalidate object cache as it will be updated
		invalidateObjectCache(bktID, objID)
	} else {
		// Create new object
		objID = core.NewID()
	}

	dataSize := int64(len(data))
	var dataID int64
	var objInfo *core.ObjectInfo

	// Check if batch write should be used for small files
	// Only use batch write for files within optimal size range (1KB - 64KB)
	// Files too small (< 1KB) don't benefit much from batching
	// Files too large (> 64KB) have better performance with direct write
	config := core.GetWriteBufferConfig()
	useBatchWrite := config.BatchWriteEnabled &&
		dataSize >= minBatchWriteFileSize &&
		dataSize <= maxBatchWriteFileSize &&
		obj == nil // Only for new files

	if useBatchWrite {
		// Try to use batch writer
		batchMgr := sdk.GetBatchWriterForBucket(handler, bktID)
		if batchMgr != nil {
			added, batchDataID, err := batchMgr.AddFile(objID, data, pid, fileName, dataSize)
			if err == nil && added {
				// Successfully added to batch write manager
				// For S3 API, we flush immediately to ensure data is available
				// The batch write still provides benefits through:
				// 1. Batch metadata writes (DataInfo and ObjectInfo)
				// 2. Reduced database transactions
				// 3. Better I/O patterns for small files
				batchMgr.FlushAll(ctx)
				dataID = batchDataID
				// Get object info after batch write
				// Wait a bit for database to be updated
				time.Sleep(50 * time.Millisecond)
				objs, err := handler.Get(ctx, bktID, []int64{objID})
				if err == nil && len(objs) > 0 {
					objInfo = objs[0]
				} else {
					// If object not found after flush, create ObjectInfo from pending objects
					// This handles the case where flush is still in progress
					if fileInfo, ok := batchMgr.GetPendingObject(objID); ok {
						objInfo = &core.ObjectInfo{
							ID:     fileInfo.ObjectID,
							PID:    fileInfo.PID,
							Name:   fileInfo.Name,
							Type:   core.OBJ_TYPE_FILE,
							DataID: fileInfo.DataID,
							Size:   fileInfo.Size,
							MTime:  core.Now(),
						}
					}
					// If still no objInfo, we'll create it from metadata below
					// Don't fallback to normal write here, as data is already in batch buffer
				}

				// If objInfo is still nil, create it from metadata
				if objInfo == nil {
					objInfo = &core.ObjectInfo{
						ID:     objID,
						PID:    pid,
						Name:   fileName,
						Type:   core.OBJ_TYPE_FILE,
						DataID: dataID,
						Size:   dataSize,
						MTime:  core.Now(),
					}
				}
			} else if err != nil {
				// Error occurred, fallback to normal write
				useBatchWrite = false
			} else {
				// Buffer full or other issue, flush existing data and retry once
				batchMgr.FlushAll(ctx)
				added, batchDataID, err = batchMgr.AddFile(objID, data, pid, fileName, dataSize)
				if err == nil && added {
					// Successfully added after flush, flush again to ensure data is written
					batchMgr.FlushAll(ctx)
					dataID = batchDataID
					// Wait a bit for database to be updated
					time.Sleep(50 * time.Millisecond)
					objs, err := handler.Get(ctx, bktID, []int64{objID})
					if err == nil && len(objs) > 0 {
						objInfo = objs[0]
					} else {
						// If object not found after flush, create ObjectInfo from pending objects
						if fileInfo, ok := batchMgr.GetPendingObject(objID); ok {
							objInfo = &core.ObjectInfo{
								ID:     fileInfo.ObjectID,
								PID:    fileInfo.PID,
								Name:   fileInfo.Name,
								Type:   core.OBJ_TYPE_FILE,
								DataID: fileInfo.DataID,
								Size:   fileInfo.Size,
								MTime:  core.Now(),
							}
						}
						// If still no objInfo, create it from metadata
						if objInfo == nil {
							objInfo = &core.ObjectInfo{
								ID:     objID,
								PID:    pid,
								Name:   fileName,
								Type:   core.OBJ_TYPE_FILE,
								DataID: dataID,
								Size:   dataSize,
								MTime:  core.Now(),
							}
						}
					}
				} else {
					// Still failed after flush, fallback to normal write
					useBatchWrite = false
				}
			}
		} else {
			useBatchWrite = false
		}
	}

	if !useBatchWrite {
		// Normal write path
		// Get chunk size from bucket configuration
		// If GetBktInfo fails, use default chunkSize (4MB)
		chunkSize := int64(4 * 1024 * 1024) // Default 4MB
		bktInfo, err := handler.GetBktInfo(ctx, bktID)
		if err == nil && bktInfo != nil && bktInfo.ChunkSize > 0 {
			chunkSize = bktInfo.ChunkSize
		}
		// If GetBktInfo fails, continue with default chunkSize

		// Upload data according to chunkSize
		// If data size is larger than chunkSize, split into multiple chunks
		if dataSize <= chunkSize {
			// Data fits in one chunk, upload directly with sn=0
			dataID, err = handler.PutData(ctx, bktID, 0, 0, data)
		} else {
			// Data is larger than chunkSize, split into multiple chunks
			dataID = core.NewID()
			chunkCount := int((dataSize + chunkSize - 1) / chunkSize) // Ceiling division

			for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
				chunkStart := int64(chunkIdx) * chunkSize
				chunkEnd := chunkStart + chunkSize
				if chunkEnd > dataSize {
					chunkEnd = dataSize
				}
				chunkData := data[chunkStart:chunkEnd]

				// Upload chunk with sn=chunkIdx (starting from 0)
				_, err = handler.PutData(ctx, bktID, dataID, chunkIdx, chunkData)
				if err != nil {
					util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to upload chunk %d: %v", chunkIdx, err))
					return
				}
			}
		}
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		// Create or update object metadata
		objInfo = &core.ObjectInfo{
			ID:     objID,
			PID:    pid,
			Name:   fileName,
			Type:   core.OBJ_TYPE_FILE,
			DataID: dataID,
			Size:   dataSize,
			MTime:  core.Now(),
		}

		_, err = handler.Put(ctx, bktID, []*core.ObjectInfo{objInfo})
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		// Update object cache
		objectCache.Put(formatObjectCacheKey(bktID, objID), objInfo)
	}

	// Update directory listing cache with new/updated object
	if objInfo != nil {
		// Update object cache
		objectCache.Put(formatObjectCacheKey(bktID, objID), objInfo)

		// Update directory listing cache
		// For updates, we need to refresh the cache to get the latest data
		// For new objects, we can add to cache, but to avoid ecache slice reuse issues,
		// we invalidate and let next query refresh from database
		invalidateDirListCache(bktID, pid)

		// Also invalidate path cache to ensure consistency
		invalidatePathCache(bktID, key)
	}

	// Invalidate path cache
	invalidatePathCache(bktID, key)
	// Also invalidate parent path cache
	parentPath = filepath.Dir(key)
	if parentPath != "." && parentPath != "/" {
		invalidatePathCache(bktID, parentPath)
	}

	c.Header("ETag", fmt.Sprintf(`"%x"`, dataID))
	c.Status(http.StatusOK)
}

// deleteObject handles DELETE /{bucket}/{key} - DeleteObject
func deleteObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	ctx := c.Request.Context()
	obj, err := findObjectByPath(c, bktID, key)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchKey", "The specified key does not exist")
		return
	}

	if err := handler.Delete(ctx, bktID, obj.ID); err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Update directory listing cache by removing deleted object
	parentPath := filepath.Dir(key)
	if parentPath != "." && parentPath != "/" {
		parentObj, err := findObjectByPath(c, bktID, parentPath)
		if err == nil && parentObj != nil {
			dirCacheKey := formatDirListCacheKey(bktID, parentObj.ID)
			if cached, ok := dirListCache.Get(dirCacheKey); ok {
				if cachedObjs, ok := cached.([]*core.ObjectInfo); ok {
					// Remove deleted object from cache
					filteredObjs := make([]*core.ObjectInfo, 0, len(cachedObjs))
					for _, cachedObj := range cachedObjs {
						if cachedObj.ID != obj.ID {
							filteredObjs = append(filteredObjs, cachedObj)
						}
					}
					dirListCache.Put(dirCacheKey, filteredObjs)
				}
			} else {
				// Cache miss, invalidate to force refresh
				invalidateDirListCache(bktID, parentObj.ID)
			}
		}
	} else {
		// Root directory
		dirCacheKey := formatDirListCacheKey(bktID, 0)
		if cached, ok := dirListCache.Get(dirCacheKey); ok {
			if cachedObjs, ok := cached.([]*core.ObjectInfo); ok {
				// Remove deleted object from cache
				filteredObjs := make([]*core.ObjectInfo, 0, len(cachedObjs))
				for _, cachedObj := range cachedObjs {
					if cachedObj.ID != obj.ID {
						filteredObjs = append(filteredObjs, cachedObj)
					}
				}
				dirListCache.Put(dirCacheKey, filteredObjs)
			}
		} else {
			invalidateDirListCache(bktID, 0)
		}
	}

	// Invalidate cache after deleting object
	invalidatePathCache(bktID, key)
	invalidateObjectCache(bktID, obj.ID)
	if parentPath != "." && parentPath != "/" {
		invalidatePathCache(bktID, parentPath)
	}

	c.Status(http.StatusNoContent)
}

// headObject handles HEAD /{bucket}/{key} - HeadObject
func headObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		c.Status(http.StatusBadRequest)
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	obj, err := findObjectByPath(c, bktID, key)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	if obj.Type != core.OBJ_TYPE_FILE {
		c.Status(http.StatusBadRequest)
		return
	}

	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Length", strconv.FormatInt(obj.Size, 10))
	c.Header("ETag", fmt.Sprintf(`"%x"`, obj.DataID))
	c.Header("Last-Modified", time.Unix(obj.MTime, 0).UTC().Format(time.RFC1123))
	c.Header("Accept-Ranges", "bytes")
	c.Status(http.StatusOK)
}

// formatMultipartUploadKey formats cache key for multipart upload
func formatMultipartUploadKey(bktID int64, uploadID string) string {
	return fmt.Sprintf("%d:%s", bktID, uploadID)
}

// initiateMultipartUpload handles POST /{bucket}/{key}?uploads - InitiateMultipartUpload
func initiateMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	// Generate upload ID
	uploadID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), core.NewID())

	// Extract metadata from headers
	metadata := make(map[string]string)
	for k, v := range c.Request.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			metaKey := strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")
			if len(v) > 0 {
				metadata[metaKey] = v[0]
			}
		}
	}

	contentType := c.GetHeader("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Create multipart upload
	upload := &MultipartUpload{
		UploadID:    uploadID,
		BucketID:    bktID,
		Key:         key,
		Initiated:   time.Now(),
		Parts:       make([]*Part, 0),
		ContentType: contentType,
		Metadata:    metadata,
	}

	// Store multipart upload
	keyStr := formatMultipartUploadKey(bktID, uploadID)
	multipartUploads.Store(keyStr, upload)

	// Return XML response
	type InitiateMultipartUploadResult struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}

	result := InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      key,
		UploadID: uploadID,
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}

// uploadPart handles PUT /{bucket}/{key}?partNumber={partNumber}&uploadId={uploadId} - UploadPart
func uploadPart(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	// Get query parameters
	partNumberStr := c.Query("partNumber")
	uploadID := c.Query("uploadId")

	if partNumberStr == "" || uploadID == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "partNumber and uploadId are required")
		return
	}

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidArgument", "Invalid part number")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	// Get multipart upload
	keyStr := formatMultipartUploadKey(bktID, uploadID)
	uploadVal, ok := multipartUploads.Load(keyStr)
	if !ok {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
		return
	}

	upload, ok := uploadVal.(*MultipartUpload)
	if !ok || upload.Key != key {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
		return
	}

	// Read part data
	partData, err := io.ReadAll(c.Request.Body)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}

	if len(partData) == 0 {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Part data cannot be empty")
		return
	}

	ctx := c.Request.Context()

	// Get chunk size from bucket configuration
	// If GetBktInfo fails, use default chunkSize (4MB)
	chunkSize := int64(4 * 1024 * 1024) // Default 4MB
	bktInfo, err := handler.GetBktInfo(ctx, bktID)
	if err == nil && bktInfo != nil && bktInfo.ChunkSize > 0 {
		chunkSize = bktInfo.ChunkSize
	}
	// If GetBktInfo fails, continue with default chunkSize

	partSize := int64(len(partData))
	var dataID int64

	// Upload part data according to chunkSize
	// If part size is larger than chunkSize, split into multiple chunks
	if partSize <= chunkSize {
		// Part fits in one chunk, upload directly with sn=0
		dataID, err = handler.PutData(ctx, bktID, 0, 0, partData)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	} else {
		// Part is larger than chunkSize, split into multiple chunks
		// First chunk uses the part's DataID, subsequent chunks use the same DataID with sn starting from 0
		dataID = core.NewID()
		chunkCount := int((partSize + chunkSize - 1) / chunkSize) // Ceiling division

		for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
			chunkStart := int64(chunkIdx) * chunkSize
			chunkEnd := chunkStart + chunkSize
			if chunkEnd > partSize {
				chunkEnd = partSize
			}
			chunkData := partData[chunkStart:chunkEnd]

			// Upload chunk with sn=chunkIdx (starting from 0)
			_, err = handler.PutData(ctx, bktID, dataID, chunkIdx, chunkData)
			if err != nil {
				util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to upload chunk %d: %v", chunkIdx, err))
				return
			}
		}
	}

	// Create part
	part := &Part{
		PartNumber: partNumber,
		ETag:       fmt.Sprintf("%x", dataID),
		DataID:     dataID,
		Size:       partSize,
		UploadTime: time.Now(),
	}

	// Add part to upload (thread-safe)
	upload.mu.Lock()
	// Check if part number already exists, replace it
	found := false
	for i, p := range upload.Parts {
		if p.PartNumber == partNumber {
			upload.Parts[i] = part
			found = true
			break
		}
	}
	if !found {
		upload.Parts = append(upload.Parts, part)
	}
	upload.mu.Unlock()

	// Return ETag
	c.Header("ETag", part.ETag)
	c.Status(http.StatusOK)
}

// completeMultipartUpload handles POST /{bucket}/{key}?uploadId={uploadId} - CompleteMultipartUpload
func completeMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	uploadID := c.Query("uploadId")
	if uploadID == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "uploadId is required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	// Get multipart upload
	keyStr := formatMultipartUploadKey(bktID, uploadID)
	uploadVal, ok := multipartUploads.Load(keyStr)
	if !ok {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
		return
	}

	upload, ok := uploadVal.(*MultipartUpload)
	if !ok || upload.Key != key {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
		return
	}

	// Parse complete multipart upload request
	type CompleteMultipartUpload struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}

	var completeReq CompleteMultipartUpload
	if err := c.ShouldBindXML(&completeReq); err != nil && err != io.EOF {
		util.S3ErrorResponse(c, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed")
		return
	}

	ctx := c.Request.Context()

	// Get parts from upload
	upload.mu.RLock()
	parts := make([]*Part, len(upload.Parts))
	copy(parts, upload.Parts)
	upload.mu.RUnlock()

	if len(parts) == 0 {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "At least one part is required")
		return
	}

	// Sort parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Validate parts from request match uploaded parts
	if len(completeReq.Parts) > 0 {
		// Create map of requested parts
		requestedParts := make(map[int]string)
		for _, reqPart := range completeReq.Parts {
			requestedParts[reqPart.PartNumber] = reqPart.ETag
		}

		// Validate all uploaded parts are in request
		for _, part := range parts {
			if expectedETag, ok := requestedParts[part.PartNumber]; !ok || expectedETag != part.ETag {
				util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidPart", "One or more of the specified parts could not be found")
				return
			}
		}
	}

	// Read all parts and concatenate
	// Get chunk size from bucket configuration
	// If GetBktInfo fails, use default chunkSize (4MB)
	chunkSize := int64(4 * 1024 * 1024) // Default 4MB
	bktInfo, err := handler.GetBktInfo(ctx, bktID)
	if err == nil && bktInfo != nil && bktInfo.ChunkSize > 0 {
		chunkSize = bktInfo.ChunkSize
	}
	// If GetBktInfo fails, continue with default chunkSize

	var allData []byte
	var totalSize int64
	for _, part := range parts {
		// Read part data - check if part spans multiple chunks
		var partData []byte
		if part.Size <= chunkSize {
			// Part fits in one chunk, read with sn=0
			partData, err = handler.GetData(ctx, bktID, part.DataID, 0)
		} else {
			// Part spans multiple chunks, read all chunks and concatenate
			chunkCount := int((part.Size + chunkSize - 1) / chunkSize) // Ceiling division
			partData = make([]byte, 0, part.Size)
			for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
				chunkData, chunkErr := handler.GetData(ctx, bktID, part.DataID, chunkIdx)
				if chunkErr != nil {
					util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to read part %d chunk %d: %v", part.PartNumber, chunkIdx, chunkErr))
					return
				}
				partData = append(partData, chunkData...)
			}
			// Trim to actual part size (last chunk might be smaller)
			if int64(len(partData)) > part.Size {
				partData = partData[:part.Size]
			}
		}

		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to read part %d: %v", part.PartNumber, err))
			return
		}
		allData = append(allData, partData...)
		totalSize += part.Size
	}

	// Ensure parent directory exists
	parentPath := filepath.Dir(key)
	var pid int64 = 0
	if parentPath != "." && parentPath != "/" {
		pid, err = ensurePath(c, bktID, parentPath)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Create final object
	fileName := filepath.Base(key)
	objID := core.NewID()

	// Upload concatenated data according to chunkSize
	// If concatenated data size is larger than chunkSize, split into multiple chunks
	concatenatedSize := int64(len(allData))
	var dataID int64
	if concatenatedSize <= chunkSize {
		// Data fits in one chunk, upload directly with sn=0
		dataID, err = handler.PutData(ctx, bktID, 0, 0, allData)
	} else {
		// Data is larger than chunkSize, split into multiple chunks
		dataID = core.NewID()
		chunkCount := int((concatenatedSize + chunkSize - 1) / chunkSize) // Ceiling division

		for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
			chunkStart := int64(chunkIdx) * chunkSize
			chunkEnd := chunkStart + chunkSize
			if chunkEnd > concatenatedSize {
				chunkEnd = concatenatedSize
			}
			chunkData := allData[chunkStart:chunkEnd]

			// Upload chunk with sn=chunkIdx (starting from 0)
			_, err = handler.PutData(ctx, bktID, dataID, chunkIdx, chunkData)
			if err != nil {
				util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to upload chunk %d: %v", chunkIdx, err))
				return
			}
		}
	}
	if err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Create object metadata
	objInfo := &core.ObjectInfo{
		ID:     objID,
		PID:    pid,
		Name:   fileName,
		Type:   core.OBJ_TYPE_FILE,
		DataID: dataID,
		Size:   totalSize,
		MTime:  core.Now(),
	}

	_, err = handler.Put(ctx, bktID, []*core.ObjectInfo{objInfo})
	if err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Clean up parts (delete part data)
	// Note: In a production system, you might want to keep parts for a grace period
	// or implement a garbage collection mechanism. Parts will be cleaned up by the
	// garbage collection mechanism when they are no longer referenced.
	for _, part := range parts {
		// Parts are temporary data that will be cleaned up by GC
		// We don't delete them immediately to allow for potential retries
		_ = part
	}

	// Remove multipart upload
	multipartUploads.Delete(keyStr)

	// Invalidate caches
	invalidatePathCache(bktID, key)
	invalidateObjectCache(bktID, objID)
	if parentPath != "." && parentPath != "/" {
		invalidatePathCache(bktID, parentPath)
		invalidateDirListCache(bktID, pid)
	}

	// Return XML response
	type CompleteMultipartUploadResult struct {
		XMLName     xml.Name `xml:"CompleteMultipartUploadResult"`
		Location    string   `xml:"Location"`
		Bucket      string   `xml:"Bucket"`
		Key         string   `xml:"Key"`
		ETag        string   `xml:"ETag"`
		ContentType string   `xml:"ContentType,omitempty"`
	}

	result := CompleteMultipartUploadResult{
		Location:    fmt.Sprintf("/%s/%s", bucketName, key),
		Bucket:      bucketName,
		Key:         key,
		ETag:        fmt.Sprintf(`"%x-%d"`, dataID, len(parts)), // S3-style ETag for multipart
		ContentType: upload.ContentType,
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}

// abortMultipartUpload handles DELETE /{bucket}/{key}?uploadId={uploadId} - AbortMultipartUpload
func abortMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	uploadID := c.Query("uploadId")
	if uploadID == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "uploadId is required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	// Get multipart upload
	keyStr := formatMultipartUploadKey(bktID, uploadID)
	uploadVal, ok := multipartUploads.Load(keyStr)
	if !ok {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
		return
	}

	upload, ok := uploadVal.(*MultipartUpload)
	if !ok || upload.Key != key {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
		return
	}

	// Delete all parts
	upload.mu.RLock()
	parts := make([]*Part, len(upload.Parts))
	copy(parts, upload.Parts)
	upload.mu.RUnlock()

	// Delete all parts
	// Note: Parts will be cleaned up by the garbage collection mechanism
	// when they are no longer referenced
	for _, part := range parts {
		// Parts are temporary data that will be cleaned up by GC
		_ = part
	}

	// Remove multipart upload
	multipartUploads.Delete(keyStr)

	c.Status(http.StatusNoContent)
}

// listMultipartUploads handles GET /{bucket}?uploads - ListMultipartUploads
func listMultipartUploads(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name is required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	// Get query parameters
	prefix := c.Query("prefix")
	delimiter := c.Query("delimiter")
	maxUploadsStr := c.Query("max-uploads")
	keyMarker := c.Query("key-marker")
	_ = c.Query("upload-id-marker") // Not used in current implementation

	maxUploads := 1000 // Default
	if maxUploadsStr != "" {
		if val, err := strconv.Atoi(maxUploadsStr); err == nil && val > 0 && val <= 10000 {
			maxUploads = val
		}
	}

	// Collect uploads for this bucket
	var uploads []*MultipartUpload
	multipartUploads.Range(func(key, value interface{}) bool {
		if upload, ok := value.(*MultipartUpload); ok && upload.BucketID == bktID {
			// Apply prefix filter
			if prefix != "" && !strings.HasPrefix(upload.Key, prefix) {
				return true
			}
			// Apply key marker filter
			if keyMarker != "" && upload.Key <= keyMarker {
				return true
			}
			uploads = append(uploads, upload)
		}
		return true
	})

	// Sort by key and initiated time
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})

	// Apply max-uploads limit
	if len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
	}

	// Build response
	type Upload struct {
		Key          string    `xml:"Key"`
		UploadID     string    `xml:"UploadId"`
		Initiated    time.Time `xml:"Initiated"`
		StorageClass string    `xml:"StorageClass"`
	}

	type ListMultipartUploadsResult struct {
		XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
		Bucket             string   `xml:"Bucket"`
		KeyMarker          string   `xml:"KeyMarker,omitempty"`
		UploadIDMarker     string   `xml:"UploadIdMarker,omitempty"`
		NextKeyMarker      string   `xml:"NextKeyMarker,omitempty"`
		NextUploadIDMarker string   `xml:"NextUploadIdMarker,omitempty"`
		MaxUploads         int      `xml:"MaxUploads"`
		IsTruncated        bool     `xml:"IsTruncated"`
		Prefix             string   `xml:"Prefix,omitempty"`
		Delimiter          string   `xml:"Delimiter,omitempty"`
		Uploads            []Upload `xml:"Upload"`
	}

	result := ListMultipartUploadsResult{
		Bucket:      bucketName,
		KeyMarker:   keyMarker,
		MaxUploads:  maxUploads,
		IsTruncated: false, // Simplified, could be true if more uploads exist
		Prefix:      prefix,
		Delimiter:   delimiter,
		Uploads:     make([]Upload, 0, len(uploads)),
	}

	for _, upload := range uploads {
		result.Uploads = append(result.Uploads, Upload{
			Key:          upload.Key,
			UploadID:     upload.UploadID,
			Initiated:    upload.Initiated,
			StorageClass: "STANDARD",
		})
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}

// listParts handles GET /{bucket}/{key}?uploadId={uploadId} - ListParts
func listParts(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	if bucketName == "" || key == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	uploadID := c.Query("uploadId")
	if uploadID == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "uploadId is required")
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	// Get multipart upload
	keyStr := formatMultipartUploadKey(bktID, uploadID)
	uploadVal, ok := multipartUploads.Load(keyStr)
	if !ok {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
		return
	}

	upload, ok := uploadVal.(*MultipartUpload)
	if !ok || upload.Key != key {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
		return
	}

	// Get query parameters
	maxPartsStr := c.Query("max-parts")
	partNumberMarkerStr := c.Query("part-number-marker")

	maxParts := 1000 // Default
	if maxPartsStr != "" {
		if val, err := strconv.Atoi(maxPartsStr); err == nil && val > 0 && val <= 10000 {
			maxParts = val
		}
	}

	partNumberMarker := 0
	if partNumberMarkerStr != "" {
		if val, err := strconv.Atoi(partNumberMarkerStr); err == nil {
			partNumberMarker = val
		}
	}

	// Get parts
	upload.mu.RLock()
	parts := make([]*Part, 0, len(upload.Parts))
	for _, part := range upload.Parts {
		if part.PartNumber > partNumberMarker {
			parts = append(parts, part)
		}
	}
	upload.mu.RUnlock()

	// Sort by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Apply max-parts limit
	isTruncated := false
	if len(parts) > maxParts {
		parts = parts[:maxParts]
		isTruncated = true
	}

	// Build response
	type PartInfo struct {
		PartNumber   int       `xml:"PartNumber"`
		ETag         string    `xml:"ETag"`
		Size         int64     `xml:"Size"`
		LastModified time.Time `xml:"LastModified"`
	}

	type ListPartsResult struct {
		XMLName              xml.Name   `xml:"ListPartsResult"`
		Bucket               string     `xml:"Bucket"`
		Key                  string     `xml:"Key"`
		UploadID             string     `xml:"UploadId"`
		PartNumberMarker     int        `xml:"PartNumberMarker,omitempty"`
		NextPartNumberMarker int        `xml:"NextPartNumberMarker,omitempty"`
		MaxParts             int        `xml:"MaxParts"`
		IsTruncated          bool       `xml:"IsTruncated"`
		Parts                []PartInfo `xml:"Part"`
	}

	result := ListPartsResult{
		Bucket:      bucketName,
		Key:         key,
		UploadID:    uploadID,
		MaxParts:    maxParts,
		IsTruncated: isTruncated,
		Parts:       make([]PartInfo, 0, len(parts)),
	}

	for _, part := range parts {
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:   part.PartNumber,
			ETag:         part.ETag,
			Size:         part.Size,
			LastModified: part.UploadTime,
		})
	}

	if len(parts) > 0 {
		result.NextPartNumberMarker = parts[len(parts)-1].PartNumber
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}
