package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/orca-zhang/ecache2"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
	"github.com/orcastor/orcas/s3/util"
)

var (
	handler = core.NewLocalHandler(".", ".")

	// pathCache caches path to object mapping
	// key: "<bktID>:<path>", value: *core.ObjectInfo
	pathCache = ecache2.NewLRUCache[string](16, 512, 30*time.Second)

	// dirListCache caches directory listing
	// key: pid (int64), value: []*core.ObjectInfo
	dirListCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// objectCache caches object by ID
	// key: objectID (int64), value: *core.ObjectInfo
	objectCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// bucketListCache caches bucket list by user ID
	// key: uid (int64), value: []*core.BucketInfo
	bucketListCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// bucketCache caches bucket by ID
	// key: bktID (int64), value: *core.BucketInfo
	bucketCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// bucketInfoCache caches bucket info by bucket ID to avoid repeated GetBktInfo calls
	// key: bktID (int64), value: *core.BucketInfo
	bucketInfoCache = ecache2.NewLRUCache[int64](16, 512, 30*time.Second)

	// multipartUploads stores ongoing multipart uploads
	// key: "<bktID>:<uploadId>", value: *MultipartUpload
	multipartUploads sync.Map
)

// getStorageClass returns StorageClass based on bucket's instant upload (deduplication) configuration
// REDUCED_REDUNDANCY: bucket supports instant upload (RefLevel > 0), data may be deduplicated (reduces redundancy)
// STANDARD: bucket does not support instant upload (RefLevel = 0), data is unique (standard redundancy)
func getStorageClass(ctx context.Context, bktID int64) string {
	// Get bucket info from cache or database
	var bucket *core.BucketInfo
	if cached, ok := bucketInfoCache.Get(bktID); ok {
		if bktInfo, ok := cached.(*core.BucketInfo); ok && bktInfo != nil {
			bucket = bktInfo
		}
	}

	// If cache miss, query database
	if bucket == nil {
		bktInfo, err := handler.GetBktInfo(ctx, bktID)
		if err == nil && bktInfo != nil {
			bucket = bktInfo
			// Update cache
			bucketInfoCache.Put(bktID, bucket)
		}
	}

	// Get instant upload config from bucket
	instantUploadCfg := core.GetBucketInstantUploadConfig(bucket)
	if core.IsInstantUploadEnabledWithConfig(instantUploadCfg) {
		// Bucket supports instant upload (deduplication), data may be deduplicated
		// Deduplication reduces redundancy, return REDUCED_REDUNDANCY
		return "REDUCED_REDUNDANCY"
	}
	// Bucket does not support instant upload, data is unique (no deduplication)
	// Unique data has standard redundancy, return STANDARD
	return "STANDARD"
}

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

// FormatCacheKeyString formats a cache key with int64 and string
var cacheKeyPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 64) // Pre-allocate for typical cache keys
		return &buf
	},
}

// formatPathCacheKey formats cache key for path lookup
func formatPathCacheKey(bktID int64, path string) string {
	buf := cacheKeyPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	*buf = strconv.AppendInt(*buf, bktID, 10)
	*buf = append(*buf, ':')
	*buf = append(*buf, path...)
	result := string(*buf)
	cacheKeyPool.Put(buf)
	return result
}

// getDirListCacheKey generates cache key for directory listing
// When pid is 0 (root), use bucketID to differentiate between buckets
// Encoding: if pid == 0, use -(bktID + 1); otherwise use pid
func getDirListCacheKey(bktID int64, pid int64) int64 {
	if pid == 0 {
		// Root directory: use negative value to encode bucketID
		// Use -(bktID + 1) to ensure it's negative and avoid conflict with pid 0
		return -(bktID + 1)
	}
	// Non-root directory: pid is globally unique, use it directly
	return pid
}

// invalidatePathCache invalidates cache for a path and its parent directories
func invalidatePathCache(bktID int64, path string) {
	// Invalidate the path itself
	pathCache.Del(formatPathCacheKey(bktID, path))

	// Invalidate all parent directories
	parts := util.FastSplitPath(path)
	for i := len(parts); i > 0; i-- {
		parentPath := strings.Join(parts[:i], "/")
		pathCache.Del(formatPathCacheKey(bktID, parentPath))
	}

	// Invalidate directory listing for all parent directories
	// Note: We can't easily get parent directory IDs here without querying
	// The directory listing cache will be naturally invalidated on next access
}

// invalidateObjectCache invalidates object cache by ID
func invalidateObjectCache(objectID int64) {
	objectCache.Del(objectID)
}

// invalidateBucketListCache invalidates bucket list cache for a user
func invalidateBucketListCache(uid int64) {
	bucketListCache.Del(uid)
}

// invalidateBucketCache invalidates bucket cache by ID
func invalidateBucketCache(bktID int64) {
	bucketCache.Del(bktID)
}

// invalidateDirListCache invalidates directory listing cache
// When pid is 0 (root), bktID is required to differentiate between buckets
func invalidateDirListCache(bktID int64, pid int64) {
	cacheKey := getDirListCacheKey(bktID, pid)
	dirListCache.Del(cacheKey)
}

// getBucketByName gets bucket by name (with cache)
func getBucketByName(c *gin.Context, name string) (*core.BucketInfo, error) {
	uid := middleware.GetUID(c)
	if uid == 0 {
		return nil, fmt.Errorf("unauthorized")
	}

	// Get all buckets for the user (with cache)
	allBuckets, err := getBucketsByUser(c, uid)
	if err != nil {
		return nil, err
	}

	// Find bucket by name
	for _, bkt := range allBuckets {
		if bkt.Name == name {
			// Try cache first by bktID
			if cached, ok := bucketCache.Get(bkt.ID); ok {
				if cachedBkt, ok := cached.(*core.BucketInfo); ok && cachedBkt != nil {
					// Verify the cached bucket still exists in current database
					// This is important for tests where each test has its own database
					ctx := c.Request.Context()
					ma := &core.DefaultMetadataAdapter{
						DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
						DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
					}
					ma.DefaultBaseMetadataAdapter.SetPath(".")
					ma.DefaultDataMetadataAdapter.SetPath(".")
					buckets, err := ma.GetBkt(ctx, []int64{cachedBkt.ID})
					if err == nil && len(buckets) > 0 && buckets[0].ID == cachedBkt.ID {
						// Bucket exists in current database, use cached value
						return cachedBkt, nil
					}
					// Bucket doesn't exist in current database, clear cache
					bucketCache.Del(bkt.ID)
				}
			}
			// Cache the bucket by bktID
			bucketCache.Put(bkt.ID, bkt)
			return bkt, nil
		}
	}

	return nil, fmt.Errorf("bucket not found")
}

// getBucketsByUser gets buckets for a user (with cache)
func getBucketsByUser(c *gin.Context, uid int64) ([]*core.BucketInfo, error) {
	ctx := c.Request.Context()

	// Try cache first
	if cached, ok := bucketListCache.Get(uid); ok {
		if buckets, ok := cached.([]*core.BucketInfo); ok {
			return buckets, nil
		}
	}

	// Query from database using ACL + GetBkt combination
	// Create adapter with default paths (handler's paths are set via SetHandlerPaths)
	ma := &core.DefaultMetadataAdapter{
		DefaultBaseMetadataAdapter: &core.DefaultBaseMetadataAdapter{},
		DefaultDataMetadataAdapter: &core.DefaultDataMetadataAdapter{},
	}
	ma.DefaultBaseMetadataAdapter.SetPath(".")
	ma.DefaultDataMetadataAdapter.SetPath(".")

	// Get ACLs for the user
	acls, err := ma.ListACLByUser(ctx, uid)
	if err != nil {
		return nil, err
	}
	if len(acls) == 0 {
		return []*core.BucketInfo{}, nil
	}

	// Get bucket IDs from ACLs
	bktIDs := make([]int64, 0, len(acls))
	for _, acl := range acls {
		bktIDs = append(bktIDs, acl.BktID)
	}

	// Get bucket info for these buckets
	buckets, err := ma.GetBkt(ctx, bktIDs)
	if err != nil {
		return nil, err
	}

	// Update cache
	bucketListCache.Put(uid, buckets)

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
	// Optimized: use fast path splitting
	parts := util.FastSplitPath(key)
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
		var objs []*core.ObjectInfo
		dirListCacheKey := getDirListCacheKey(bktID, pid)
		if cached, ok := dirListCache.Get(dirListCacheKey); ok {
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
			dirListCache.Put(dirListCacheKey, objs)
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
			objectCache.Put(found.ID, found)
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

// listAllObjectsRecursively lists all objects in a bucket using iterative approach (not recursive)
func listAllObjectsRecursively(c *gin.Context, bktID int64, prefix string, delimiter string, maxKeys int) ([]Content, []CommonPrefix, error) {
	ctx := c.Request.Context()
	var contents []Content
	var commonPrefixes []CommonPrefix
	commonPrefixSet := make(map[string]bool)

	// Use a queue to store directories to process: (pid, currentPath)
	type dirEntry struct {
		pid         int64
		currentPath string
	}
	queue := []dirEntry{{pid: 0, currentPath: ""}}

	// Process directories iteratively
	for len(queue) > 0 && len(contents) < maxKeys {
		// Dequeue first directory
		entry := queue[0]
		queue = queue[1:]

		// List objects in current directory with pagination
		processedIDs := make(map[int64]bool) // Track processed object IDs to avoid duplicates
		// Note: processedIDs is per directory, reset for each new directory
		for {
			if len(contents) >= maxKeys {
				break
			}

			opt := core.ListOptions{
				Count: core.DefaultListPageSize,
				Order: "+name",
			}

			objs, _, _, err := handler.List(ctx, bktID, entry.pid, opt)
			if err != nil {
				return nil, nil, err
			}

			if len(objs) == 0 {
				break // No more objects in this directory
			}

			// Filter out already processed objects
			newObjs := make([]*core.ObjectInfo, 0)
			for _, obj := range objs {
				if !processedIDs[obj.ID] {
					newObjs = append(newObjs, obj)
					processedIDs[obj.ID] = true
				}
			}

			if len(newObjs) == 0 {
				// All objects were already processed, but we got results
				// This means we've seen all objects, break
				break
			}

			for _, obj := range newObjs {
				if len(contents) >= maxKeys {
					break
				}

				// Build full path (key)
				// Note: obj.Name is just the filename/dirname, not the full path
				// We need to build the full path from currentPath + obj.Name
				var key string
				if entry.currentPath == "" {
					key = obj.Name
				} else {
					key = entry.currentPath + "/" + obj.Name
				}

				// Filter by prefix
				if prefix != "" && !strings.HasPrefix(key, prefix) {
					continue
				}

				if obj.Type == core.OBJ_TYPE_FILE {
					// It's a file, add to contents
					// Get StorageClass based on bucket's instant upload configuration
					storageClass := getStorageClass(ctx, bktID)
					contents = append(contents, Content{
						Key:          key,
						LastModified: util.FormatLastModified(obj.MTime),
						ETag:         util.FormatETag(obj.DataID),
						Size:         obj.Size,
						StorageClass: storageClass,
					})
				} else if obj.Type == core.OBJ_TYPE_DIR {
					// It's a directory
					if delimiter != "" {
						// With delimiter, add to common prefixes
						prefixKey := key + delimiter
						if !commonPrefixSet[prefixKey] {
							commonPrefixes = append(commonPrefixes, CommonPrefix{
								Prefix: prefixKey,
							})
							commonPrefixSet[prefixKey] = true
						}
					} else {
						// Without delimiter, add directory to queue for processing
						queue = append(queue, dirEntry{
							pid:         obj.ID,
							currentPath: key,
						})
					}
				}
			}

			// Check if there are more objects to fetch
			// If we got fewer objects than requested, we've reached the end
			if int64(len(objs)) < core.DefaultListPageSize {
				break // No more objects in this directory
			}

			// If all returned objects were already processed, we've seen everything
			if len(newObjs) == 0 {
				break
			}
		}
	}

	// Sort contents by key
	sort.Slice(contents, func(i, j int) bool {
		return contents[i].Key < contents[j].Key
	})

	// Sort common prefixes
	sort.Slice(commonPrefixes, func(i, j int) bool {
		return commonPrefixes[i].Prefix < commonPrefixes[j].Prefix
	})

	return contents, commonPrefixes, nil
}

// ensurePath ensures the path exists, creating directories as needed (with cache)
// Optimization: Batch create missing directories to reduce database round trips
func ensurePath(c *gin.Context, bktID int64, key string) (int64, error) {
	ctx := c.Request.Context()
	// Optimized: use fast path splitting
	parts := util.FastSplitPath(key)
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		return 0, nil
	}

	var pid int64 = 0
	var dirsToCreate []*core.ObjectInfo
	var createPIDs []int64

	// First pass: check existing directories and collect missing ones
	for i, part := range parts {
		if part == "" {
			continue
		}

		// Try directory listing cache
		var objs []*core.ObjectInfo
		cacheKey := getDirListCacheKey(bktID, pid)
		if cached, ok := dirListCache.Get(cacheKey); ok {
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
			dirListCache.Put(cacheKey, objs)
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
			// Directory doesn't exist, prepare to create it
			if i < len(parts)-1 {
				// Not the last part, will create directory
				newPID := core.NewID()
				dirObj := &core.ObjectInfo{
					ID:    newPID,
					PID:   pid,
					Type:  core.OBJ_TYPE_DIR,
					Name:  part,
					MTime: core.Now(),
				}
				dirsToCreate = append(dirsToCreate, dirObj)
				createPIDs = append(createPIDs, newPID)
				pid = newPID
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

	// Batch create all missing directories in one database operation
	if len(dirsToCreate) > 0 {
		ids, err := handler.Put(ctx, bktID, dirsToCreate)
		if err != nil {
			return 0, err
		}

		// Invalidate caches for all created directories
		for i, dirObj := range dirsToCreate {
			if i < len(ids) && ids[i] > 0 {
				invalidateDirListCache(bktID, dirObj.PID)
				// Build parent path for cache invalidation
				pathParts := parts[:i+1]
				parentPath := strings.Join(pathParts, "/")
				invalidatePathCache(bktID, parentPath)
			}
		}

		// Return the last created directory's PID
		if len(ids) > 0 {
			return ids[len(ids)-1], nil
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

// ListBuckets handles GET / - ListBuckets (with cache)
// Exported for testing purposes
func ListBuckets(c *gin.Context) {
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

// CreateBucket handles PUT /{bucket} - CreateBucket
// Exported for testing purposes
func CreateBucket(c *gin.Context) {
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
		Type: 1,
	}

	admin := core.NewLocalAdmin(".", ".")
	if err := admin.PutBkt(ctx, []*core.BucketInfo{bkt}); err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Create ACL entry for bucket owner (PutBkt already creates ACL with ALL permission)
	// The ACL is created automatically by PutBkt, so we don't need to create it again

	// Invalidate cache after creating bucket
	invalidateBucketListCache(uid)
	// Cache the new bucket by bktID
	bucketCache.Put(bkt.ID, bkt)

	c.Status(http.StatusOK)
}

// DeleteBucket handles DELETE /{bucket} - DeleteBucket
// Exported for testing purposes
func DeleteBucket(c *gin.Context) {
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

	admin := core.NewLocalAdmin(".", ".")
	if err := admin.DeleteBkt(ctx, bktID); err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Invalidate cache after deleting bucket
	invalidateBucketListCache(uid)
	invalidateBucketCache(bktID)

	c.Status(http.StatusNoContent)
}

// ListObjects handles GET /{bucket} - ListObjects
// Exported for testing purposes
func ListObjects(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidBucketName", "Bucket name is required")
		return
	}

	// Check if this is a GetBucketLocation request
	// location parameter can be empty string or present
	// Check both GetQuery (for empty values) and raw query string
	if _, hasLocation := c.GetQuery("location"); hasLocation || strings.Contains(c.Request.URL.RawQuery, "location") {
		// Handle GetBucketLocation API
		bktID, err := getBucketIDByName(c, bucketName)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
			return
		}
		_ = bktID // bucket exists, proceed
		// Return default location (us-east-1)
		locationResult := struct {
			XMLName  xml.Name `xml:"LocationConstraint"`
			Location string   `xml:",chardata"`
		}{
			Location: "us-east-1",
		}
		c.Header("Content-Type", "application/xml")
		c.XML(http.StatusOK, locationResult)
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

	// List all objects recursively in the bucket
	contents, commonPrefixes, err := listAllObjectsRecursively(c, bktID, prefix, delimiter, maxKeys+1) // +1 to check if truncated
	if err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Handle marker (pagination)
	startIndex := 0
	if marker != "" {
		for i, content := range contents {
			if content.Key > marker {
				startIndex = i
				break
			}
		}
	}

	// Apply maxKeys limit
	isTruncated := len(contents) > startIndex+maxKeys
	if startIndex+maxKeys < len(contents) {
		contents = contents[startIndex : startIndex+maxKeys]
	} else {
		contents = contents[startIndex:]
	}

	// Merge pending objects from batch writer
	batchMgr := util.GetBatchWriterForBucket(handler, bktID)
	if batchMgr != nil {
		pendingObjsMap := batchMgr.GetPendingObjects()
		existingKeys := make(map[string]bool)
		for _, content := range contents {
			existingKeys[content.Key] = true
		}

		// Build paths for pending objects and add them if they match prefix
		for _, fileInfo := range pendingObjsMap {
			// Build full path for pending object
			// We need to find the path by traversing from fileInfo.PID to root
			ctx := c.Request.Context()
			key, err := buildPathFromPID(ctx, bktID, fileInfo.PID, fileInfo.Name)
			if err != nil {
				continue // Skip if we can't build path
			}

			// Check if already exists
			if existingKeys[key] {
				continue
			}

			// Filter by prefix
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}

			// Filter by delimiter
			if delimiter != "" {
				relativeKey := key
				if prefix != "" {
					relativeKey = strings.TrimPrefix(key, prefix)
				}
				if strings.Contains(relativeKey, delimiter) {
					// This would be a CommonPrefix, skip
					continue
				}
			}

			// Add to contents
			// Get StorageClass based on bucket's instant upload configuration
			storageClass := getStorageClass(ctx, bktID)
			contents = append(contents, Content{
				Key:          key,
				LastModified: util.FormatLastModified(core.Now()),
				ETag:         util.FormatETag(fileInfo.DataID),
				Size:         fileInfo.Size,
				StorageClass: storageClass,
			})
		}

		// Re-sort after adding pending objects
		sort.Slice(contents, func(i, j int) bool {
			return contents[i].Key < contents[j].Key
		})

		// Re-apply maxKeys limit
		if len(contents) > maxKeys {
			contents = contents[:maxKeys]
			isTruncated = true
		}
	}

	result := ListBucketResult{
		Name:           bucketName,
		Prefix:         prefix,
		Marker:         marker,
		MaxKeys:        maxKeys,
		IsTruncated:    isTruncated,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}

// buildPathFromPID builds the full path from a parent ID and object name
func buildPathFromPID(ctx context.Context, bktID int64, pid int64, name string) (string, error) {
	if pid == 0 {
		return name, nil
	}

	parts := []string{name}
	currentPID := pid

	for currentPID != 0 {
		// Try object cache
		if cached, ok := objectCache.Get(currentPID); ok {
			if parent, ok := cached.(*core.ObjectInfo); ok && parent != nil {
				parts = append([]string{parent.Name}, parts...)
				currentPID = parent.PID
				continue
			}
		}

		// Query to find parent object
		opt := core.ListOptions{
			Count: 10000,
		}
		allObjs, _, _, err := handler.List(ctx, bktID, 0, opt)
		if err != nil {
			return "", err
		}

		var parent *core.ObjectInfo
		for _, o := range allObjs {
			if o.ID == currentPID {
				parent = o
				break
			}
		}

		if parent == nil {
			// Can't find parent, return partial path
			return strings.Join(parts, "/"), nil
		}

		// Cache parent
		objectCache.Put(currentPID, parent)

		parts = append([]string{parent.Name}, parts...)
		currentPID = parent.PID
	}

	return strings.Join(parts, "/"), nil
}

// GetObject handles GET /{bucket}/{key} - GetObject
// Exported for testing purposes
func GetObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

	// Check if key is empty and handle special bucket-level operations
	if key == "" {
		// Check if this is a GetBucketLocation request
		if _, hasLocation := c.GetQuery("location"); hasLocation || strings.Contains(c.Request.URL.RawQuery, "location") {
			bktID, err := getBucketIDByName(c, bucketName)
			if err != nil {
				util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
				return
			}
			_ = bktID // bucket exists, proceed
			// Return default location (us-east-1)
			locationResult := struct {
				XMLName  xml.Name `xml:"LocationConstraint"`
				Location string   `xml:",chardata"`
			}{
				Location: "us-east-1",
			}
			c.Header("Content-Type", "application/xml")
			c.XML(http.StatusOK, locationResult)
			return
		}

		// Check if this is an object-lock configuration request
		if _, hasObjectLock := c.GetQuery("object-lock"); hasObjectLock || strings.Contains(c.Request.URL.RawQuery, "object-lock") {
			bktID, err := getBucketIDByName(c, bucketName)
			if err != nil {
				util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
				return
			}
			_ = bktID // bucket exists, proceed
			// Object lock is not configured for this bucket
			util.S3ErrorResponse(c, http.StatusNotFound, "ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket")
			return
		}

		// Check if this is a ListObjectsV2 request (list-type=2)
		if listType := c.Query("list-type"); listType == "2" || c.Query("prefix") != "" || c.Query("delimiter") != "" {
			// Delegate to ListObjects function
			ListObjects(c)
			return
		}

		// If none of the above, return error
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and key are required")
		return
	}

	if bucketName == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name is required")
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
		batchMgr := util.GetBatchWriterForBucket(handler, bktID)
		if batchMgr != nil {
			// Try to find object in pending objects by name
			fileName := util.FastBase(key)
			parentPath := util.FastDir(key)
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
	objectCache.Put(obj.ID, obj)

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
	batchMgr := util.GetBatchWriterForBucket(handler, bktID)
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

			// Set response headers (optimized: batch header setting)
			util.SetObjectHeadersWithContentType(c, "application/octet-stream", readSize, obj.DataID, obj.MTime, "bytes")

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
			chunkSize := int64(10 * 1024 * 1024) // Default 10MB
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
			chunkSize := int64(10 * 1024 * 1024) // Default 10MB
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

	// Set response headers (optimized: batch header setting)
	util.SetObjectHeadersWithContentType(c, "application/octet-stream", readSize, obj.DataID, obj.MTime, "bytes")

	// Set StorageClass header based on bucket's instant upload configuration
	storageClass := getStorageClass(ctx, bktID)
	c.Header("x-amz-storage-class", storageClass)

	if rangeSpec != nil && rangeSpec.Valid {
		// Return 206 Partial Content
		c.Header("Content-Range", util.FormatContentRangeHeader(startOffset, startOffset+readSize-1, objectSize))
		c.Data(http.StatusPartialContent, "application/octet-stream", data)
	} else {
		c.Data(http.StatusOK, "application/octet-stream", data)
	}
}

// PutObject handles PUT /{bucket}/{key} - PutObject
// Exported for testing purposes
func PutObject(c *gin.Context) {
	// Check for copy or move operations
	if c.GetHeader("x-amz-copy-source") != "" {
		copyObject(c)
		return
	}
	if c.GetHeader("x-amz-move-source") != "" {
		moveObject(c)
		return
	}

	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

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

	// Extract file name early for reuse
	fileName := util.FastBase(key)

	// Ensure parent directory exists (only if not root)
	// Optimization: Skip ensurePath for root directory files to reduce database queries
	var pid int64 = 0
	parentPath := util.FastDir(key)
	if parentPath != "." && parentPath != "/" && parentPath != "" {
		pid, err = ensurePath(c, bktID, parentPath)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Optimization: Skip object existence check for new uploads (most common case)
	// Only check if conditional headers are present (If-Match, If-None-Match, etc.)
	// This avoids unnecessary database queries for new file uploads
	var objID int64
	var isUpdate bool
	hasConditionalHeader := c.GetHeader("If-Match") != "" || c.GetHeader("If-None-Match") != "" ||
		c.GetHeader("If-Modified-Since") != "" || c.GetHeader("If-Unmodified-Since") != ""

	if hasConditionalHeader {
		// Only check object existence when conditional headers are present
		obj, err := findObjectByPath(c, bktID, key)
		if err == nil && obj != nil {
			// Object exists, this is an update
			objID = obj.ID
			isUpdate = true
			// Invalidate object cache as it will be updated
			invalidateObjectCache(objID)
		} else {
			// New object, create ID early
			objID = core.NewID()
			isUpdate = false
		}
	} else {
		// No conditional headers, assume new upload (most common case)
		// Create new ID, let database handle conflicts with INSERT OR REPLACE semantics
		objID = core.NewID()
		isUpdate = false
	}

	dataSize := int64(len(data))
	var dataID int64
	var objInfo *core.ObjectInfo

	// Pre-calculate checksums once for both instant upload and normal upload paths
	// This avoids redundant calculation if instant upload fails
	// Only calculate checksums if instant upload is enabled or if we need them for DataInfo
	var hdrXXH3 int64
	var xxh3Val int64
	var sha256_0, sha256_1, sha256_2, sha256_3 int64
	var checksumsCalculated bool
	instantUploadEnabled := core.IsInstantUploadEnabled()

	if dataSize > 0 {
		hdrXXH3, xxh3Val, sha256_0, sha256_1, sha256_2, sha256_3 = core.CalculateChecksums(data)
		checksumsCalculated = true

		// Try instant upload (deduplication) before uploading data
		// Only if instant upload is enabled via configuration
		if instantUploadEnabled {
			// Create DataInfo for Ref
			dataInfo := &core.DataInfo{
				OrigSize: dataSize,
				HdrXXH3:  hdrXXH3,
				XXH3:     xxh3Val,
				SHA256_0: sha256_0,
				SHA256_1: sha256_1,
				SHA256_2: sha256_2,
				SHA256_3: sha256_3,
				Kind:     core.DATA_NORMAL, // Default: no compression/encryption
			}

			// Try instant upload
			refIDs, err := handler.Ref(ctx, bktID, []*core.DataInfo{dataInfo})
			if err == nil && len(refIDs) > 0 && refIDs[0] != 0 {
				if refIDs[0] > 0 {
					// Instant upload succeeded, use existing DataID from database
					dataID = refIDs[0]
				} else {
					// Negative ID means reference to another element in current batch
					// This should not happen in S3 PutObject (single file upload)
					// But we handle it for completeness: skip instant upload, use normal upload
					// The negative reference will be resolved in PutDataInfo
				}
			}
		}
	}

	// Check if batch write should be used for small files
	// Only use batch write for files within optimal size range (1KB - 64KB)
	// Performance tests show:
	// - Small files (1KB): 155.3% improvement with batch write
	// - Medium files (1MB): 9.6-65.5% degradation with batch write
	// - Large files (10MB): 34.7% improvement with batch write
	// Optimal threshold: 64KB for best overall performance
	config := core.GetWriteBufferConfig()
	// Use MaxBatchWriteFileSize from config (default 64KB) instead of MaxBufferSize (8MB)
	// Only use batch write for new files (not updates) to avoid complexity
	useBatchWrite := config.BatchWriteEnabled &&
		dataSize <= config.MaxBatchWriteFileSize &&
		!isUpdate // Only for new files, not updates

	if useBatchWrite {
		// Try to use batch writer
		batchMgr := util.GetBatchWriterForBucket(handler, bktID)
		if batchMgr != nil {
			batchMgr.SetFlushContext(ctx)
			added, batchDataID, err := batchMgr.AddFile(objID, data, pid, fileName, dataSize, 0) // kind=0: S3 API data is raw, no compression/encryption
			if err == nil && added {
				// Successfully added to batch write manager
				// For S3 API, we flush immediately to ensure data is available
				// The batch write still provides benefits through:
				// 1. Batch metadata writes (DataInfo and ObjectInfo)
				// 2. Reduced database transactions
				// 3. Better I/O patterns for small files
				dataID = batchDataID
				// First check memory cache (pending objects) for better performance
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
				} else {
					// If not in memory cache, query database
					objs, err := handler.Get(ctx, bktID, []int64{objID})
					if err == nil && len(objs) > 0 {
						objInfo = objs[0]
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
				added, batchDataID, err = batchMgr.AddFile(objID, data, pid, fileName, dataSize, 0) // kind=0: S3 API data is raw, no compression/encryption
				if err == nil && added {
					dataID = batchDataID
					// First check memory cache (pending objects) for better performance
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
					} else {
						// If not in memory cache, query database
						objs, err := handler.Get(ctx, bktID, []int64{objID})
						if err == nil && len(objs) > 0 {
							objInfo = objs[0]
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
		// Only upload if instant upload failed (dataID == 0)
		if dataID == 0 {
			// Get chunk size from bucket configuration
			// Use cache to avoid repeated GetBktInfo queries
			chunkSize := int64(10 * 1024 * 1024) // Default 10MB
			if cached, ok := bucketInfoCache.Get(bktID); ok {
				if bktInfo, ok := cached.(*core.BucketInfo); ok && bktInfo != nil && bktInfo.ChunkSize > 0 {
					chunkSize = bktInfo.ChunkSize
				}
			} else {
				// Cache miss, query database
				bktInfo, err := handler.GetBktInfo(ctx, bktID)
				if err == nil && bktInfo != nil {
					// Cache the result
					bucketInfoCache.Put(bktID, bktInfo)
					if bktInfo.ChunkSize > 0 {
						chunkSize = bktInfo.ChunkSize
					}
				}
				// If GetBktInfo fails, continue with default chunkSize
			}

			// Upload data according to chunkSize
			// If data size is larger than chunkSize, split into multiple chunks
			if dataSize <= chunkSize {
				// Data fits in one chunk, upload directly with sn=0
				dataID, err = handler.PutData(ctx, bktID, 0, 0, data)
				if err != nil {
					util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
					return
				}
			} else {
				// Data is larger than chunkSize, split into multiple chunks
				dataID = core.NewID()
				chunkCount := int((dataSize + chunkSize - 1) / chunkSize) // Ceiling division

				// Pre-calculate chunk boundaries to avoid repeated calculations in loop
				for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
					chunkStart := int64(chunkIdx) * chunkSize
					chunkEnd := chunkStart + chunkSize
					if chunkEnd > dataSize {
						chunkEnd = dataSize
					}
					// Use slice directly without copying (data is already in memory)
					chunkData := data[chunkStart:chunkEnd]

					// Upload chunk with sn=chunkIdx (starting from 0)
					_, err = handler.PutData(ctx, bktID, dataID, chunkIdx, chunkData)
					if err != nil {
						util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to upload chunk %d: %v", chunkIdx, err))
						return
					}
				}
			}

			// Optimization: Combine DataInfo and ObjectInfo writes into a single transaction
			// This reduces database round trips and improves performance
			if dataID > 0 && dataID != core.EmptyDataID {
				// Prepare ObjectInfo
				objInfo = &core.ObjectInfo{
					ID:     objID,
					PID:    pid,
					Name:   fileName,
					Type:   core.OBJ_TYPE_FILE,
					DataID: dataID,
					Size:   dataSize,
					MTime:  core.Now(),
				}

				// Prepare DataInfo if we have checksums
				var dataInfo *core.DataInfo
				if checksumsCalculated {
					// Use pre-calculated checksums - most common path
					dataInfo = &core.DataInfo{
						ID:       dataID,
						Size:     dataSize,
						OrigSize: dataSize,
						HdrXXH3:  hdrXXH3,
						XXH3:     xxh3Val,
						SHA256_0: sha256_0,
						SHA256_1: sha256_1,
						SHA256_2: sha256_2,
						SHA256_3: sha256_3,
						Kind:     core.DATA_NORMAL, // Default: no compression/encryption
					}
				} else if dataSize > 0 {
					// Fallback: calculate checksums if not already calculated
					calcHdrXXH3, calcXXH3Val, calcSHA256_0, calcSHA256_1, calcSHA256_2, calcSHA256_3 := core.CalculateChecksums(data)
					dataInfo = &core.DataInfo{
						ID:       dataID,
						Size:     dataSize,
						OrigSize: dataSize,
						HdrXXH3:  calcHdrXXH3,
						XXH3:     calcXXH3Val,
						SHA256_0: calcSHA256_0,
						SHA256_1: calcSHA256_1,
						SHA256_2: calcSHA256_2,
						SHA256_3: calcSHA256_3,
						Kind:     core.DATA_NORMAL, // Default: no compression/encryption
					}
				}

				// Use combined write method if we have DataInfo, otherwise fallback to separate writes
				if dataInfo != nil {
					// Combined write: DataInfo and ObjectInfo in one transaction
					err = handler.PutDataInfoAndObj(ctx, bktID, []*core.DataInfo{dataInfo}, []*core.ObjectInfo{objInfo})
					if err != nil {
						util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
						return
					}
				} else {
					// Fallback: write ObjectInfo only (DataInfo creation failed, but object creation should still work)
					_, err = handler.Put(ctx, bktID, []*core.ObjectInfo{objInfo})
					if err != nil {
						util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
						return
					}
				}
			} else {
				// No dataID (should not happen in normal flow), create ObjectInfo only
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
			}
		}
	}

	// Update caches with new/updated object
	if objInfo != nil {
		// Update object cache (only once, avoid duplicate)
		objectCache.Put(objID, objInfo)

		// Update directory listing cache
		// For updates, we need to refresh the cache to get the latest data
		// For new objects, we can add to cache, but to avoid ecache slice reuse issues,
		// we invalidate and let next query refresh from database
		invalidateDirListCache(bktID, pid)

		// Invalidate path caches to ensure consistency
		// Invalidate the object's path cache
		invalidatePathCache(bktID, key)
		// Also invalidate parent path cache if not root
		if parentPath != "." && parentPath != "/" && parentPath != "" {
			invalidatePathCache(bktID, parentPath)
		}
	}

	c.Header("ETag", util.FormatETag(dataID))
	c.Status(http.StatusOK)
}

// DeleteObject handles DELETE /{bucket}/{key} - DeleteObject
// Exported for testing purposes
func DeleteObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

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
		// In concurrent scenarios, object might not be found due to cache inconsistency
		// Try invalidating cache and retry once
		invalidatePathCache(bktID, key)
		parentPath := util.FastDir(key)
		if parentPath != "." && parentPath != "/" && parentPath != "" {
			invalidatePathCache(bktID, parentPath)
		}
		// Try to get parent PID and invalidate dir list cache
		if parentPath != "." && parentPath != "/" && parentPath != "" {
			if parentObj, err2 := findObjectByPath(c, bktID, parentPath); err2 == nil && parentObj != nil {
				invalidateDirListCache(bktID, parentObj.ID)
			}
		} else {
			invalidateDirListCache(bktID, 0)
		}
		// Retry once after cache invalidation
		obj, err = findObjectByPath(c, bktID, key)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchKey", "The specified key does not exist")
			return
		}
	}

	if err := handler.Delete(ctx, bktID, obj.ID); err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Update directory listing cache by removing deleted object
	parentPath := util.FastDir(key)
	if parentPath != "." && parentPath != "/" {
		parentObj, err := findObjectByPath(c, bktID, parentPath)
		if err == nil && parentObj != nil {
			cacheKey := getDirListCacheKey(bktID, parentObj.ID)
			if cached, ok := dirListCache.Get(cacheKey); ok {
				if cachedObjs, ok := cached.([]*core.ObjectInfo); ok {
					// Remove deleted object from cache
					filteredObjs := make([]*core.ObjectInfo, 0, len(cachedObjs))
					for _, cachedObj := range cachedObjs {
						if cachedObj.ID != obj.ID {
							filteredObjs = append(filteredObjs, cachedObj)
						}
					}
					dirListCache.Put(cacheKey, filteredObjs)
				}
			} else {
				// Cache miss, invalidate to force refresh
				invalidateDirListCache(bktID, parentObj.ID)
			}
		}
	} else {
		// Root directory
		cacheKey := getDirListCacheKey(bktID, 0)
		if cached, ok := dirListCache.Get(cacheKey); ok {
			if cachedObjs, ok := cached.([]*core.ObjectInfo); ok {
				// Remove deleted object from cache
				filteredObjs := make([]*core.ObjectInfo, 0, len(cachedObjs))
				for _, cachedObj := range cachedObjs {
					if cachedObj.ID != obj.ID {
						filteredObjs = append(filteredObjs, cachedObj)
					}
				}
				dirListCache.Put(cacheKey, filteredObjs)
			}
		} else {
			invalidateDirListCache(bktID, 0)
		}
	}

	// Invalidate cache after deleting object
	invalidatePathCache(bktID, key)
	invalidateObjectCache(obj.ID)
	if parentPath != "." && parentPath != "/" {
		invalidatePathCache(bktID, parentPath)
	}

	c.Status(http.StatusNoContent)
}

// copyObject handles PUT /{bucket}/{key} with x-amz-copy-source header - CopyObject
func copyObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	destKey := c.Param("key")
	destKey = util.FastTrimPrefix(destKey, "/")

	if bucketName == "" || destKey == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and destination key are required")
		return
	}

	// Get source from x-amz-copy-source header
	source := c.GetHeader("x-amz-copy-source")
	if source == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "x-amz-copy-source header is required")
		return
	}

	// Parse source: /bucket/key or bucket/key
	source = util.FastTrimPrefix(source, "/")
	parts := strings.SplitN(source, "/", 2)
	if len(parts) != 2 {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Invalid x-amz-copy-source format")
		return
	}
	sourceBucket := parts[0]
	sourceKey := parts[1]

	// Get destination bucket ID
	destBktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified destination bucket does not exist")
		return
	}

	// Get source bucket ID (can be same or different bucket)
	sourceBktID, err := getBucketIDByName(c, sourceBucket)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified source bucket does not exist")
		return
	}

	ctx := c.Request.Context()

	// Find source object
	sourceObj, err := findObjectByPath(c, sourceBktID, sourceKey)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchKey", "The specified source key does not exist")
		return
	}

	if sourceObj.Type != core.OBJ_TYPE_FILE {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Source key is not a file")
		return
	}

	// Read source data
	sourceData, err := handler.GetData(ctx, sourceBktID, sourceObj.DataID, 0)
	if err != nil {
		// Try reading all chunks if single chunk read fails
		var allChunks []byte
		for chunkIdx := 0; ; chunkIdx++ {
			chunkData, chunkErr := handler.GetData(ctx, sourceBktID, sourceObj.DataID, chunkIdx)
			if chunkErr != nil {
				break
			}
			allChunks = append(allChunks, chunkData...)
		}
		if len(allChunks) > 0 {
			sourceData = allChunks
			err = nil
		}
	}
	if err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to read source data: %v", err))
		return
	}

	// Ensure parent directory exists for destination
	parentPath := util.FastDir(destKey)
	var pid int64 = 0
	if parentPath != "." && parentPath != "/" {
		pid, err = ensurePath(c, destBktID, parentPath)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Check if destination object exists
	fileName := util.FastBase(destKey)
	destObj, err := findObjectByPath(c, destBktID, destKey)
	var destObjID int64
	if err == nil && destObj != nil {
		// Object exists, update it
		destObjID = destObj.ID
		invalidateObjectCache(destObjID)
	} else {
		// Create new object
		destObjID = core.NewID()
	}

	// Get chunk size for destination bucket
	chunkSize := int64(10 * 1024 * 1024) // Default 10MB
	bktInfo, err := handler.GetBktInfo(ctx, destBktID)
	if err == nil && bktInfo != nil && bktInfo.ChunkSize > 0 {
		chunkSize = bktInfo.ChunkSize
	}

	dataSize := int64(len(sourceData))
	var dataID int64

	// Try instant upload (deduplication) before uploading data
	// Calculate checksums and check if data already exists in destination bucket
	// Only if instant upload is enabled via configuration
	if dataSize > 0 && core.IsInstantUploadEnabled() {
		// Calculate checksums for instant upload
		hdrXXH3, xxh3Val, sha256_0, sha256_1, sha256_2, sha256_3 := core.CalculateChecksums(sourceData)
		// Create DataInfo for Ref
		dataInfo := &core.DataInfo{
			OrigSize: dataSize,
			HdrXXH3:  hdrXXH3,
			XXH3:     xxh3Val,
			SHA256_0: sha256_0,
			SHA256_1: sha256_1,
			SHA256_2: sha256_2,
			SHA256_3: sha256_3,
			Kind:     core.DATA_NORMAL, // Default: no compression/encryption
		}

		// Try instant upload
		refIDs, err := handler.Ref(ctx, destBktID, []*core.DataInfo{dataInfo})
		if err == nil && len(refIDs) > 0 && refIDs[0] != 0 {
			if refIDs[0] > 0 {
				// Instant upload succeeded, use existing DataID from database
				dataID = refIDs[0]
			} else {
				// Negative ID means reference to another element in current batch
				// This should not happen in S3 CopyObject (single file copy)
				// But we handle it for completeness: skip instant upload, use normal upload
				// The negative reference will be resolved in PutDataInfo
			}
		}
	}

	// Only upload if instant upload failed (dataID == 0)
	if dataID == 0 {
		// Upload data according to chunkSize
		if dataSize <= chunkSize {
			// Data fits in one chunk, upload directly with sn=0
			dataID, err = handler.PutData(ctx, destBktID, 0, 0, sourceData)
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
				chunkData := sourceData[chunkStart:chunkEnd]

				// Upload chunk with sn=chunkIdx (starting from 0)
				_, err = handler.PutData(ctx, destBktID, dataID, chunkIdx, chunkData)
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
	}

	// Create or update object metadata
	objInfo := &core.ObjectInfo{
		ID:     destObjID,
		PID:    pid,
		Name:   fileName,
		Type:   core.OBJ_TYPE_FILE,
		DataID: dataID,
		Size:   dataSize,
		MTime:  core.Now(),
	}

	_, err = handler.Put(ctx, destBktID, []*core.ObjectInfo{objInfo})
	if err != nil {
		util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Update caches
	objectCache.Put(destObjID, objInfo)
	invalidateDirListCache(destBktID, pid)
	invalidatePathCache(destBktID, destKey)
	if parentPath != "." && parentPath != "/" {
		invalidatePathCache(destBktID, parentPath)
	}

	// Return copy result in S3 XML format
	type CopyObjectResult struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
	}

	result := CopyObjectResult{
		ETag:         util.FormatETag(dataID),
		LastModified: time.Now().UTC().Format(time.RFC3339),
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}

// moveObject handles PUT /{bucket}/{key} with x-amz-move-source header - MoveObject (non-standard)
func moveObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	destKey := c.Param("key")
	destKey = util.FastTrimPrefix(destKey, "/")

	if bucketName == "" || destKey == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Bucket name and destination key are required")
		return
	}

	// Get source from x-amz-move-source header (non-standard extension)
	source := c.GetHeader("x-amz-move-source")
	if source == "" {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "x-amz-move-source header is required")
		return
	}

	// Parse source: /bucket/key or bucket/key
	source = util.FastTrimPrefix(source, "/")
	parts := strings.SplitN(source, "/", 2)
	if len(parts) != 2 {
		util.S3ErrorResponse(c, http.StatusBadRequest, "InvalidRequest", "Invalid x-amz-move-source format")
		return
	}
	sourceBucket := parts[0]
	sourceKey := parts[1]

	// Get destination bucket ID
	destBktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified destination bucket does not exist")
		return
	}

	// Get source bucket ID (can be same or different bucket)
	sourceBktID, err := getBucketIDByName(c, sourceBucket)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchBucket", "The specified source bucket does not exist")
		return
	}

	ctx := c.Request.Context()

	// Find source object
	sourceObj, err := findObjectByPath(c, sourceBktID, sourceKey)
	if err != nil {
		util.S3ErrorResponse(c, http.StatusNotFound, "NoSuchKey", "The specified source key does not exist")
		return
	}

	// Ensure parent directory exists for destination
	parentPath := util.FastDir(destKey)
	var pid int64 = 0
	if parentPath != "." && parentPath != "/" {
		pid, err = ensurePath(c, destBktID, parentPath)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Check if destination object exists
	fileName := util.FastBase(destKey)
	destObj, err := findObjectByPath(c, destBktID, destKey)
	var destObjID int64
	if err == nil && destObj != nil {
		// Object exists, delete it first
		if err := handler.Delete(ctx, destBktID, destObj.ID); err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to delete existing destination object: %v", err))
			return
		}
		invalidateObjectCache(destObj.ID)
		destObjID = destObj.ID
	} else {
		// Create new object ID
		destObjID = core.NewID()
	}

	// Move object: update PID and name
	if sourceBktID == destBktID {
		// Same bucket: just move (update PID and name)
		if err := handler.MoveTo(ctx, sourceBktID, sourceObj.ID, pid); err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to move object: %v", err))
			return
		}
		if fileName != sourceObj.Name {
			if err := handler.Rename(ctx, sourceBktID, sourceObj.ID, fileName); err != nil {
				util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to rename object: %v", err))
				return
			}
		}
		destObjID = sourceObj.ID
	} else {
		// Different buckets: copy and delete
		// Read source data
		sourceData, err := handler.GetData(ctx, sourceBktID, sourceObj.DataID, 0)
		if err != nil {
			// Try reading all chunks if single chunk read fails
			var allChunks []byte
			for chunkIdx := 0; ; chunkIdx++ {
				chunkData, chunkErr := handler.GetData(ctx, sourceBktID, sourceObj.DataID, chunkIdx)
				if chunkErr != nil {
					break
				}
				allChunks = append(allChunks, chunkData...)
			}
			if len(allChunks) > 0 {
				sourceData = allChunks
				err = nil
			}
		}
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to read source data: %v", err))
			return
		}

		// Get chunk size for destination bucket
		chunkSize := int64(10 * 1024 * 1024) // Default 10MB
		bktInfo, err := handler.GetBktInfo(ctx, destBktID)
		if err == nil && bktInfo != nil && bktInfo.ChunkSize > 0 {
			chunkSize = bktInfo.ChunkSize
		}

		dataSize := int64(len(sourceData))
		var dataID int64

		// Try instant upload (deduplication) before uploading data
		// Calculate checksums and check if data already exists in destination bucket
		// Only if instant upload is enabled via configuration
		if dataSize > 0 && core.IsInstantUploadEnabled() {
			// Calculate checksums for instant upload
			hdrXXH3, xxh3Val, sha256_0, sha256_1, sha256_2, sha256_3 := core.CalculateChecksums(sourceData)

			// Create DataInfo for Ref
			dataInfo := &core.DataInfo{
				OrigSize: dataSize,
				HdrXXH3:  hdrXXH3,
				XXH3:     xxh3Val,
				SHA256_0: sha256_0,
				SHA256_1: sha256_1,
				SHA256_2: sha256_2,
				SHA256_3: sha256_3,
				Kind:     core.DATA_NORMAL, // Default: no compression/encryption
			}

			// Try instant upload
			refIDs, err := handler.Ref(ctx, destBktID, []*core.DataInfo{dataInfo})
			if err == nil && len(refIDs) > 0 && refIDs[0] != 0 {
				if refIDs[0] > 0 {
					// Instant upload succeeded, use existing DataID from database
					dataID = refIDs[0]
				} else {
					// Negative ID means reference to another element in current batch
					// This should not happen in S3 MoveObject (single file move)
					// But we handle it for completeness: skip instant upload, use normal upload
					// The negative reference will be resolved in PutDataInfo
				}
			}
		}

		// Only upload if instant upload failed (dataID == 0)
		if dataID == 0 {
			// Upload data according to chunkSize
			if dataSize <= chunkSize {
				dataID, err = handler.PutData(ctx, destBktID, 0, 0, sourceData)
			} else {
				dataID = core.NewID()
				chunkCount := int((dataSize + chunkSize - 1) / chunkSize)
				for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
					chunkStart := int64(chunkIdx) * chunkSize
					chunkEnd := chunkStart + chunkSize
					if chunkEnd > dataSize {
						chunkEnd = dataSize
					}
					chunkData := sourceData[chunkStart:chunkEnd]
					_, err = handler.PutData(ctx, destBktID, dataID, chunkIdx, chunkData)
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
		}

		// Create destination object
		objInfo := &core.ObjectInfo{
			ID:     destObjID,
			PID:    pid,
			Name:   fileName,
			Type:   sourceObj.Type,
			DataID: dataID,
			Size:   sourceObj.Size,
			MTime:  core.Now(),
		}
		_, err = handler.Put(ctx, destBktID, []*core.ObjectInfo{objInfo})
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}

		// Delete source object
		if err := handler.Delete(ctx, sourceBktID, sourceObj.ID); err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", fmt.Sprintf("Failed to delete source object: %v", err))
			return
		}

		// Update caches
		objectCache.Put(destObjID, objInfo)
		invalidateObjectCache(sourceObj.ID)
		invalidatePathCache(sourceBktID, sourceKey)
	}

	// Update caches
	invalidateDirListCache(destBktID, pid)
	invalidatePathCache(destBktID, destKey)
	if parentPath != "." && parentPath != "/" {
		invalidatePathCache(destBktID, parentPath)
	}
	// Invalidate source directory cache
	sourceParentPath := util.FastDir(sourceKey)
	if sourceParentPath != "." && sourceParentPath != "/" {
		sourceParentObj, err := findObjectByPath(c, sourceBktID, sourceParentPath)
		if err == nil && sourceParentObj != nil {
			invalidateDirListCache(sourceBktID, sourceParentObj.ID)
		}
	} else {
		invalidateDirListCache(sourceBktID, 0)
	}

	c.Header("ETag", util.FormatETag(destObjID))
	c.Status(http.StatusOK)
}

// headBucket handles HEAD /{bucket}/ - HeadBucket (bucket existence check)
func headBucket(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		c.Status(http.StatusBadRequest)
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	// Bucket exists, return 200 OK
	_ = bktID // bucket exists, proceed
	c.Status(http.StatusOK)
}

// HeadObject handles HEAD /{bucket}/{key} - HeadObject
// Exported for testing purposes
func HeadObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

	if bucketName == "" {
		c.Status(http.StatusBadRequest)
		return
	}

	bktID, err := getBucketIDByName(c, bucketName)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	// If key is empty (e.g., HEAD /bucket/), treat as bucket check
	// Return 200 OK to indicate bucket exists
	if key == "" {
		c.Status(http.StatusOK)
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

	// Set response headers (optimized: batch header setting)
	util.SetObjectHeadersWithContentType(c, "application/octet-stream", obj.Size, obj.DataID, obj.MTime, "bytes")

	// Set StorageClass header based on bucket's instant upload configuration
	ctx := c.Request.Context()
	storageClass := getStorageClass(ctx, bktID)
	c.Header("x-amz-storage-class", storageClass)

	c.Status(http.StatusOK)
}

// formatMultipartUploadKey formats cache key for multipart upload
func formatMultipartUploadKey(bktID int64, uploadID string) string {
	return fmt.Sprintf("%d:%s", bktID, uploadID)
}

// InitiateMultipartUpload handles POST /{bucket}/{key}?uploads - InitiateMultipartUpload
func InitiateMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

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

// UploadPart handles PUT /{bucket}/{key}?partNumber={partNumber}&uploadId={uploadId} - UploadPart
func UploadPart(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

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
	chunkSize := int64(10 * 1024 * 1024) // Default 10MB
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

// CompleteMultipartUpload handles POST /{bucket}/{key}?uploadId={uploadId} - CompleteMultipartUpload
func CompleteMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

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
	chunkSize := int64(10 * 1024 * 1024) // Default 10MB
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
	parentPath := util.FastDir(key)
	var pid int64 = 0
	if parentPath != "." && parentPath != "/" {
		pid, err = ensurePath(c, bktID, parentPath)
		if err != nil {
			util.S3ErrorResponse(c, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
	}

	// Create final object
	fileName := util.FastBase(key)
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
	invalidateObjectCache(objID)
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

// AbortMultipartUpload handles DELETE /{bucket}/{key}?uploadId={uploadId} - AbortMultipartUpload
func AbortMultipartUpload(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

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

// ListMultipartUploads handles GET /{bucket}?uploads - ListMultipartUploads
func ListMultipartUploads(c *gin.Context) {
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

	ctx := c.Request.Context()
	for _, upload := range uploads {
		// Get StorageClass based on bucket's instant upload configuration
		storageClass := getStorageClass(ctx, upload.BucketID)
		result.Uploads = append(result.Uploads, Upload{
			Key:          upload.Key,
			UploadID:     upload.UploadID,
			Initiated:    upload.Initiated,
			StorageClass: storageClass,
		})
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, result)
}

// ListParts handles GET /{bucket}/{key}?uploadId={uploadId} - ListParts
func ListParts(c *gin.Context) {
	bucketName := c.Param("bucket")
	key := c.Param("key")
	key = util.FastTrimPrefix(key, "/")

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
