package core

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/orca-zhang/idgen"
	"golang.org/x/crypto/pbkdf2"
)

// Global ID generator instance
var (
	globalIDGen     *idgen.IDGen
	globalIDGenOnce sync.Once
)

// InitIDGen initializes the global ID generator
// If not initialized, a default ID generator will be created on first use
func InitIDGen(ig *idgen.IDGen) {
	globalIDGenOnce.Do(func() {
		if ig != nil {
			globalIDGen = ig
		} else {
			globalIDGen = idgen.NewIDGen(nil, 0)
		}
	})
}

// getIDGen returns the global ID generator, initializing it if necessary
func getIDGen() *idgen.IDGen {
	globalIDGenOnce.Do(func() {
		if globalIDGen == nil {
			globalIDGen = idgen.NewIDGen(nil, 0)
		}
	})
	return globalIDGen
}

// NewID generates a new ID using the global ID generator
func NewID() int64 {
	id, _ := getIDGen().New()
	return id
}

type ListOptions struct {
	Word  string `json:"w,omitempty"` // 过滤词，支持通配符*和?
	Delim string `json:"d,omitempty"` // 分隔符，每次请求后返回，原样回传即可
	Type  int    `json:"t,omitempty"` // 对象类型，-1: malformed, 0: 不过滤(default), 1: dir, 2: file, 3: version, 4: preview(thumb/m3u8/pdf)
	Count int    `json:"c,omitempty"` // 查询个数
	Order string `json:"o,omitempty"` // 排序方式，id/mtime/name/size/type 前缀 +: 升序（默认） -: 降序
	Brief int    `json:"e,omitempty"` // 显示更少内容(只在网络传输层，节省流量时有效)，0: FULL(default), 1: without EXT, 2:only ID
	// 可以考虑改用FieldMask实现 https://mp.weixin.qq.com/s/L7He7M4JWi84z1emuokjbQ
}

type ScrubResult struct {
	TotalData          int     `json:"total_data"`          // 总数据数
	CorruptedData      []int64 `json:"corrupted_data"`      // 损坏数据：有元数据但没有数据文件
	OrphanedData       []int64 `json:"orphaned_data"`       // 孤立数据：有数据文件但没有元数据引用
	MismatchedChecksum []int64 `json:"mismatched_checksum"` // 校验和不匹配的数据
}

type DirtyDataResult struct {
	IncompleteChunks []int64 `json:"incomplete_chunks"` // 分片不完整的数据（上传过程中断电）
	UnreadableData   []int64 `json:"unreadable_data"`   // 无法读取的数据文件
}

type MergeDuplicateResult struct {
	MergedGroups int64             `json:"merged_groups"` // 合并的重复数据组数
	MergedData   []map[int64]int64 `json:"merged_data"`   // 合并的数据映射：重复DataID -> 主DataID
	FreedSize    int64             `json:"freed_size"`    // 释放的空间大小（字节）
}

type DefragmentResult struct {
	PackedGroups  int64 `json:"packed_groups"`  // 打包的组数
	PackedFiles   int64 `json:"packed_files"`   // 打包的文件数
	CompactedPkgs int64 `json:"compacted_pkgs"` // 整理的打包文件数
	FreedSize     int64 `json:"freed_size"`     // 释放的空间大小（字节）
	SkippedInUse  int64 `json:"skipped_in_use"` // 跳过正在使用的数据数
}

type FixScrubIssuesResult struct {
	FixedCorrupted          int      `json:"fixed_corrupted"`           // 修复的损坏数据数
	FixedOrphaned           int      `json:"fixed_orphaned"`            // 修复的孤立数据数
	FixedMismatchedChecksum int      `json:"fixed_mismatched_checksum"` // 修复的校验和不匹配数据数
	FreedSize               int64    `json:"freed_size"`                // 释放的空间大小（字节）
	Errors                  []string `json:"errors"`                    // 修复过程中的错误信息
}

type Options struct{}

type Handler interface {
	// 传入underlying，返回当前的，构成链式调用
	New(h Handler) Handler
	Close()

	SetOptions(opt Options)
	// 设置自定义的存储适配器
	SetAdapter(ma MetadataAdapter, da DataAdapter)

	// 登录用户
	Login(c Ctx, usr, pwd string) (Ctx, *UserInfo, []*BucketInfo, error)

	// 只有文件长度、HdrCRC32是预Ref，如果成功返回新DataID，失败返回0
	// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
	// 如果非预Ref DataID传0，说明跳过了预Ref
	Ref(c Ctx, bktID int64, d []*DataInfo) ([]int64, error)
	// PutData uploads data chunk, sn starts from 0, if dataID is 0 a new one will be created
	PutData(c Ctx, bktID, dataID int64, sn int, buf []byte) (int64, error)
	// GetData reads data: one param means sn, two params mean sn+offset, three params mean sn+offset+size
	GetData(c Ctx, bktID, id int64, sn int, offsetOrSize ...int) ([]byte, error)
	// 上传元数据
	PutDataInfo(c Ctx, bktID int64, d []*DataInfo) ([]int64, error)
	// PutDataInfoAndObj writes both DataInfo and ObjectInfo in a single transaction
	// This optimization reduces database round trips for better performance
	PutDataInfoAndObj(c Ctx, bktID int64, d []*DataInfo, o []*ObjectInfo) error
	// 获取数据信息
	GetDataInfo(c Ctx, bktID, id int64) (*DataInfo, error)

	// Name不传默认用ID字符串化后的值作为Name
	Put(c Ctx, bktID int64, o []*ObjectInfo) ([]int64, error)
	Get(c Ctx, bktID int64, ids []int64) ([]*ObjectInfo, error)
	List(c Ctx, bktID, pid int64, opt ListOptions) (o []*ObjectInfo, cnt int64, delim string, err error)

	Rename(c Ctx, bktID, id int64, name string) error
	MoveTo(c Ctx, bktID, id, pid int64) error

	// Recycle marks object as deleted to recycle bin
	// During garbage collection: data without metadata reference is dirty data (need window time)
	// Metadata without data is corrupted data
	Recycle(c Ctx, bktID, id int64) error
	// Delete permanently deletes object
	Delete(c Ctx, bktID, id int64) error

	// CleanRecycleBin physically deletes objects marked as deleted in recycle bin
	CleanRecycleBin(c Ctx, bktID int64, targetID int64) error
	// ListRecycleBin lists objects in recycle bin
	ListRecycleBin(c Ctx, bktID int64, opt ListOptions) (o []*ObjectInfo, cnt int64, delim string, err error)

	// UpdateFileLatestVersion updates all files' latest version DataID, update time, size and directory size
	UpdateFileLatestVersion(c Ctx, bktID int64) error

	// GetBkt Get bucket information (for getting bucket configuration)
	GetBktInfo(c Ctx, bktID int64) (*BucketInfo, error)
}

type Admin interface {
	// 传入underlying，返回当前的，构成链式调用
	New(a Admin) Admin
	Close()

	PutBkt(c Ctx, o []*BucketInfo) error
	DeleteBkt(c Ctx, bktID int64) error
	// SetQuota Set bucket quota
	SetQuota(c Ctx, bktID int64, quota int64) error
	// 审计数据完整性：检查元数据和数据文件的一致性
	Scrub(c Ctx, bktID int64) (*ScrubResult, error)
	// 扫描脏数据：检查上传失败、机器断电导致的不完整数据
	ScanDirtyData(c Ctx, bktID int64) (*DirtyDataResult, error)
	// 合并秒传重复数据：查找并合并具有相同校验值但不同DataID的重复数据
	MergeDuplicateData(c Ctx, bktID int64) (*MergeDuplicateResult, error)
	// 碎片整理：小文件离线归并打包，打包中被删除的数据块前移
	Defragment(c Ctx, bktID int64) (*DefragmentResult, error)

	// 用户管理
	CreateUser(c Ctx, username, password, name string, role uint32) (*UserInfo, error)
	UpdateUser(c Ctx, userID int64, username, password, name string, role *uint32) error
	DeleteUser(c Ctx, userID int64) error
	ListUsers(c Ctx) ([]*UserInfo, error)
}

type LocalHandler struct {
	ma  MetadataAdapter
	da  DataAdapter
	acm AccessCtrlMgr
	opt Options
	// SDK config registry: map[bktID] -> SDK config
	// This allows ConvertWritingVersions to access SDK config for compression/encryption
	sdkConfigs sync.Map // map[int64]*SDKConfigInfo
	// Bucket config registry: map[bktID] -> BucketInfo
	// This allows access to bucket configuration for compression/encryption
	bucketConfigs sync.Map // map[int64]*BucketInfo
}

// SDKConfigInfo stores SDK configuration for a bucket
// This is a simplified version of sdk.Config, containing only compression/encryption settings
type SDKConfigInfo struct {
	CmprWay  uint32 // Compression method (core.DATA_CMPR_MASK), smart compression by default
	CmprQlty uint32 // Compression quality
	EndecWay uint32 // Encryption method (core.DATA_ENDEC_MASK)
	EndecKey string // Encryption key
}

func NewLocalHandler() Handler {
	dma := &DefaultMetadataAdapter{}
	return &LocalHandler{
		ma:  dma,
		da:  &DefaultDataAdapter{},
		acm: &DefaultAccessCtrlMgr{ma: dma},
	}
}

// NewNoAuthHandler creates a Handler that bypasses all authentication and permission checks
// This is useful for testing, internal operations, or when authentication is handled externally
// The handler uses NoAuthAccessCtrlMgr which always allows all operations
func NewNoAuthHandler() Handler {
	dma := &DefaultMetadataAdapter{}
	return &LocalHandler{
		ma:  dma,
		da:  &DefaultDataAdapter{},
		acm: &NoAuthAccessCtrlMgr{},
	}
}

// NewHandlerWithAccessCtrl creates a Handler with a custom AccessCtrlMgr
// This allows injecting a custom access control manager for testing or special use cases
func NewHandlerWithAccessCtrl(acm AccessCtrlMgr) Handler {
	dma := &DefaultMetadataAdapter{}
	if acm == nil {
		acm = &DefaultAccessCtrlMgr{ma: dma}
	}
	acm.SetAdapter(dma)
	return &LocalHandler{
		ma:  dma,
		da:  &DefaultDataAdapter{},
		acm: acm,
	}
}

// NewHandlerWithAdapters creates a Handler with custom MetadataAdapter, DataAdapter, and AccessCtrlMgr
// This provides maximum flexibility for testing or custom implementations
func NewHandlerWithAdapters(ma MetadataAdapter, da DataAdapter, acm AccessCtrlMgr) Handler {
	if ma == nil {
		ma = &DefaultMetadataAdapter{}
	}
	if da == nil {
		da = &DefaultDataAdapter{}
	}
	if acm == nil {
		acm = &DefaultAccessCtrlMgr{ma: ma}
	}
	acm.SetAdapter(ma)
	return &LocalHandler{
		ma:  ma,
		da:  da,
		acm: acm,
	}
}

// New returns current handler, forming a chain with underlying handler
func (lh *LocalHandler) New(Handler) Handler {
	// Ignore underlying handler
	return lh
}

func (lh *LocalHandler) Close() {
	lh.da.Close()
	lh.ma.Close()
}

func (lh *LocalHandler) SetOptions(opt Options) {
	lh.da.SetOptions(opt)
	lh.opt = opt
}

func (lh *LocalHandler) SetAdapter(ma MetadataAdapter, da DataAdapter) {
	lh.ma = ma
	lh.da = da
	lh.acm.SetAdapter(ma)
}

// GetDataAdapter returns the DataAdapter instance
// This allows external packages to access DataAdapter for operations like Delete
func (lh *LocalHandler) GetDataAdapter() DataAdapter {
	return lh.da
}

// SetSDKConfig sets SDK configuration for a bucket
// This allows ConvertWritingVersions to access compression/encryption settings
// cmprWay is smart compression by default (checks file type)
func (lh *LocalHandler) SetSDKConfig(bktID int64, cmprWay, cmprQlty, endecWay uint32, endecKey string) {
	lh.sdkConfigs.Store(bktID, &SDKConfigInfo{
		CmprWay:  cmprWay, // Smart compression by default (checks file type)
		CmprQlty: cmprQlty,
		EndecWay: endecWay,
		EndecKey: endecKey,
	})
}

// GetSDKConfig gets SDK configuration for a bucket
func (lh *LocalHandler) GetSDKConfig(bktID int64) *SDKConfigInfo {
	if cfg, ok := lh.sdkConfigs.Load(bktID); ok {
		if sdkCfg, ok := cfg.(*SDKConfigInfo); ok {
			return sdkCfg
		}
	}
	return nil
}

// SetBucketConfig sets bucket configuration for a bucket
// This allows ConvertWritingVersions and other operations to access bucket config
func (lh *LocalHandler) SetBucketConfig(bktID int64, bucket *BucketInfo) {
	if bucket != nil {
		lh.bucketConfigs.Store(bktID, bucket)
		// Also sync to SDKConfig
		// CmprWay is now smart compression by default
		lh.SetSDKConfig(bktID, bucket.CmprWay, bucket.CmprQlty, bucket.EndecWay, bucket.EndecKey)
	}
}

func (lh *LocalHandler) Login(c Ctx, usr, pwd string) (Ctx, *UserInfo, []*BucketInfo, error) {
	// pwd from db or cache
	u, err := lh.ma.GetUsr2(c, usr)
	if err != nil {
		return c, nil, nil, err
	}

	if u.ID <= 0 {
		return c, nil, nil, ERR_INCORRECT_PWD
	}

	// pbkdf2 check
	vs := strings.Split(u.Pwd, ":")
	if len(vs) < 3 {
		return c, nil, nil, ERR_AUTH_FAILED
	}

	// iter/salt/hash
	iter, _ := strconv.Atoi(vs[0])
	salt, _ := base64.URLEncoding.DecodeString(vs[1])
	dk := pbkdf2.Key([]byte(pwd), salt, iter, 16, sha1.New)
	d := base64.URLEncoding.EncodeToString(dk)
	if d != vs[2] {
		return c, nil, nil, ERR_INCORRECT_PWD
	}

	// list buckets
	b, _ := lh.ma.ListBkt(c, u.ID)

	// 同步每个bucket的配置到SDKConfig
	for _, bucket := range b {
		if bucket != nil {
			// CmprWay is now smart compression by default
			lh.SetSDKConfig(bucket.ID, bucket.CmprWay, bucket.CmprQlty, bucket.EndecWay, bucket.EndecKey)
		}
	}

	// set uid to ctx
	c, u.Pwd = UserInfo2Ctx(c, u), ""
	return c, u, b, nil
}

// HashPassword encrypts password using PBKDF2
// Returns format: iter:salt:hash
// iter is randomly generated iteration count (between 1000-10000), increases password hash diversity
func HashPassword(password string) (string, error) {
	// Generate random iteration count (between 1000-10000)
	iterBytes := make([]byte, 2)
	if _, err := rand.Read(iterBytes); err != nil {
		return "", err
	}
	// Convert 2 bytes to uint16, then map to 1000-10000 range
	iterRand := binary.BigEndian.Uint16(iterBytes)
	iter := 1000 + int(iterRand)%9001 // 1000 + (0-9000) = 1000-10000

	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	dk := pbkdf2.Key([]byte(password), salt, iter, 16, sha1.New)
	saltStr := base64.URLEncoding.EncodeToString(salt)
	hashStr := base64.URLEncoding.EncodeToString(dk)
	return fmt.Sprintf("%d:%s:%s", iter, saltStr, hashStr), nil
}

// Ref performs pre-ref: only file length and HdrCRC32 are pre-ref, returns new DataID on success, 0 on failure
// With file length, CRC32, MD5, returns referenced DataID on success, 0 on failure
// Client detects DataID change means no need to upload data
// If non-pre-ref DataID is 0, pre-ref is skipped
func (lh *LocalHandler) Ref(c Ctx, bktID int64, d []*DataInfo) ([]int64, error) {
	if err := lh.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return nil, err
	}
	return lh.ma.RefData(c, bktID, d)
}

// PutData uploads data chunk
// For large files, sn starts from 0. For small files or packaged uploads, use sn=0
// If dataID is 0, a new dataID will be created automatically
func (lh *LocalHandler) PutData(c Ctx, bktID, dataID int64, sn int, buf []byte) (int64, error) {
	if err := lh.acm.CheckPermission(c, DW, bktID); err != nil {
		return 0, err
	}

	// Check quota and update usage (each data block write needs to check)
	dataSize := int64(len(buf))
	if dataSize > 0 {
		// Optimization: Use bucket cache to avoid database query on every write
		var bucket *BucketInfo
		if cached, ok := lh.bucketConfigs.Load(bktID); ok {
			if bktInfo, ok := cached.(*BucketInfo); ok && bktInfo != nil {
				bucket = bktInfo
			}
		}

		// If cache miss, query database and update cache
		if bucket == nil {
			buckets, err := lh.ma.GetBkt(c, []int64{bktID})
			if err != nil {
				return 0, err
			}
			if len(buckets) == 0 {
				return 0, ERR_QUERY_DB
			}
			bucket = buckets[0]
			// Update cache for future use
			lh.SetBucketConfig(bktID, bucket)
		}

		// If quota >= 0, check if it exceeds quota
		if bucket.Quota >= 0 {
			if bucket.RealUsed+dataSize > bucket.Quota {
				return 0, ERR_QUOTA_EXCEED
			}
		}

		// Increase actual usage (atomic operation, guaranteed consistency at database level)
		if err := lh.ma.IncBktRealUsed(c, bktID, dataSize); err != nil {
			return 0, err
		}
	}

	if dataID == 0 {
		if len(buf) <= 0 {
			dataID = EmptyDataID
		} else {
			dataID = NewID()
		}
	}
	log.Printf("[Core PutData] Writing data to disk: bktID=%d, dataID=%d, sn=%d, size=%d", bktID, dataID, sn, len(buf))
	err := lh.da.Write(c, bktID, dataID, sn, buf)
	if err != nil {
		log.Printf("[Core PutData] ERROR: Failed to write data to disk: bktID=%d, dataID=%d, sn=%d, size=%d, error=%v", bktID, dataID, sn, len(buf), err)
		return dataID, err
	}
	log.Printf("[Core PutData] Successfully wrote data to disk: bktID=%d, dataID=%d, sn=%d, size=%d", bktID, dataID, sn, len(buf))
	return dataID, nil
}

// PutDataFromReader uploads data chunk from io.Reader, sn starts from 0
// If dataID is 0, a new dataID will be created automatically
// This is more efficient for streaming data as it avoids loading all data into memory
func (lh *LocalHandler) PutDataFromReader(c Ctx, bktID, dataID int64, sn int, r io.Reader, size int64) (int64, error) {
	if err := lh.acm.CheckPermission(c, DW, bktID); err != nil {
		return 0, err
	}

	// Check quota and update usage (each data block write needs to check)
	if size > 0 {
		// Optimization: Use bucket cache to avoid database query on every write
		var bucket *BucketInfo
		if cached, ok := lh.bucketConfigs.Load(bktID); ok {
			if bktInfo, ok := cached.(*BucketInfo); ok && bktInfo != nil {
				bucket = bktInfo
			}
		}

		// If cache miss, query database and update cache
		if bucket == nil {
			buckets, err := lh.ma.GetBkt(c, []int64{bktID})
			if err != nil {
				return 0, err
			}
			if len(buckets) == 0 {
				return 0, ERR_QUERY_DB
			}
			bucket = buckets[0]
			// Update cache for future use
			lh.SetBucketConfig(bktID, bucket)
		}

		// If quota >= 0, check if it exceeds quota
		if bucket.Quota >= 0 {
			if bucket.RealUsed+size > bucket.Quota {
				return 0, ERR_QUOTA_EXCEED
			}
		}

		// Increase actual usage (atomic operation, guaranteed consistency at database level)
		if err := lh.ma.IncBktRealUsed(c, bktID, size); err != nil {
			return 0, err
		}
	}

	if dataID == 0 {
		if size <= 0 {
			dataID = EmptyDataID
		} else {
			dataID = NewID()
		}
	}

	// Read all data from reader into buffer
	buf := make([]byte, size)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		log.Printf("[Core PutDataFromReader] ERROR: Failed to read from reader: bktID=%d, dataID=%d, sn=%d, size=%d, error=%v", bktID, dataID, sn, size, err)
		return dataID, err
	}
	if int64(n) < size {
		buf = buf[:n]
	}

	log.Printf("[Core PutDataFromReader] Writing data to disk: bktID=%d, dataID=%d, sn=%d, size=%d", bktID, dataID, sn, len(buf))
	err = lh.da.Write(c, bktID, dataID, sn, buf)
	if err != nil {
		log.Printf("[Core PutDataFromReader] ERROR: Failed to write data to disk: bktID=%d, dataID=%d, sn=%d, size=%d, error=%v", bktID, dataID, sn, len(buf), err)
		return dataID, err
	}
	log.Printf("[Core PutDataFromReader] Successfully wrote data to disk: bktID=%d, dataID=%d, sn=%d, size=%d", bktID, dataID, sn, len(buf))
	return dataID, nil
}

// PutDataInfo creates metadata after data is uploaded
func (lh *LocalHandler) PutDataInfo(c Ctx, bktID int64, d []*DataInfo) (ids []int64, err error) {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return nil, err
	}

	var n []*DataInfo
	for _, x := range d {
		if x.ID == 0 {
			x.ID = NewID()
		}
		ids = append(ids, x.ID)
		if x.ID > 0 {
			n = append(n, x)
		}
	}
	// Set to two's complement to reference data from other elements
	for i := range ids {
		if ids[i] < 0 && ^ids[i] < int64(len(ids)) {
			ids[i] = ids[^ids[i]]
		}
	}
	return ids, lh.ma.PutData(c, bktID, n)
}

// PutDataInfoAndObj writes both DataInfo and ObjectInfo in a single transaction
// This optimization reduces database round trips for better performance
func (lh *LocalHandler) PutDataInfoAndObj(c Ctx, bktID int64, d []*DataInfo, o []*ObjectInfo) error {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return err
	}

	// Prepare DataInfo
	for _, x := range d {
		if x.ID == 0 {
			x.ID = NewID()
		}
	}

	// Prepare ObjectInfo (same logic as Put method)
	for _, x := range o {
		if x.ID == 0 {
			x.ID = NewID()
		}
		// Auto-generate name if empty (writing versions with name="0" keep their name)
		if x.Name == "" {
			x.Name = strconv.FormatInt(x.ID, 10)
		}
	}
	for _, x := range o {
		if x.PID < 0 && int(^x.PID) <= len(o) {
			x.PID = o[^x.PID].ID
		}
	}

	// Use combined write method
	err := lh.ma.PutDataAndObj(c, bktID, d, o)
	if err != nil {
		return err
	}
	return nil
}

func (lh *LocalHandler) GetDataInfo(c Ctx, bktID, id int64) (*DataInfo, error) {
	if err := lh.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, err
	}
	return lh.ma.GetData(c, bktID, id)
}

// GetData reads data chunk
// One param means sn, two params mean sn+offset, three params mean sn+offset+size
// For sparse files (DATA_SPARSE flag), missing chunks are filled with zeros
func (lh *LocalHandler) GetData(c Ctx, bktID, id int64, sn int, offsetOrSize ...int) ([]byte, error) {
	if err := lh.acm.CheckPermission(c, DR, bktID); err != nil {
		return nil, err
	}

	// Get DataInfo to check if it's a sparse file
	dataInfo, err := lh.ma.GetData(c, bktID, id)
	if err != nil {
		return nil, err
	}
	isSparse := IsSparseFile(dataInfo)

	var data []byte
	switch len(offsetOrSize) {
	case 0:
		data, err = lh.da.Read(c, bktID, id, sn)
	case 1:
		data, err = lh.da.ReadBytes(c, bktID, id, sn, offsetOrSize[0], -1)
	default:
		data, err = lh.da.ReadBytes(c, bktID, id, sn, offsetOrSize[0], offsetOrSize[1])
	}

	// For sparse files, if file doesn't exist, fill with zeros
	if err != nil && isSparse {
		if os.IsNotExist(err) {
			// Calculate expected size
			var expectedSize int
			switch len(offsetOrSize) {
			case 0:
				// Read entire chunk - use chunk size from bucket configuration
				chunkSize := getChunkSize(c, bktID, lh.ma)
				// For sparse files, if chunk doesn't exist, return chunkSize zeros
				// This allows readers to continue reading subsequent chunks
				return make([]byte, chunkSize), nil
			case 1:
				// Read from offset to end - we don't know end size, return empty
				return []byte{}, nil
			default:
				// Read specific size
				expectedSize = offsetOrSize[1]
				if expectedSize > 0 {
					return make([]byte, expectedSize), nil
				}
				return []byte{}, nil
			}
		}
		// For other errors, still return error
		return nil, err
	}

	// For sparse files, if read less than requested, fill remaining with zeros
	if err == nil && isSparse && len(offsetOrSize) >= 2 {
		requestedSize := offsetOrSize[1]
		if requestedSize > 0 && len(data) < requestedSize {
			// Fill remaining bytes with zeros
			padded := make([]byte, requestedSize)
			copy(padded, data)
			return padded, nil
		}
	}

	return data, err
}

// GetOrCreateWritingVersion gets or creates a "writing version" (name="0") for a file
// Writing versions allow direct modification of data blocks without creating new versions
// This is optimized for scenarios like qBittorrent random writes
// The version ID is set to creation time (timestamp), and name will be set to completion time when finished
func (lh *LocalHandler) GetOrCreateWritingVersion(c Ctx, bktID, fileID int64) (*ObjectInfo, error) {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return nil, err
	}

	// Query for existing writing version (name="0"), include writing versions
	versions, err := lh.ma.ListVersions(c, bktID, fileID, false)
	if err != nil {
		return nil, err
	}

	// Find writing version (name="0")
	for _, v := range versions {
		if v.Name == WritingVersionName {
			return v, nil
		}
	}

	// No writing version exists, create one
	// Get file object to get current DataID and Size
	fileObjs, err := lh.ma.GetObj(c, bktID, []int64{fileID})
	if err != nil {
		return nil, err
	}
	if len(fileObjs) == 0 {
		return nil, fmt.Errorf("file %d not found", fileID)
	}
	fileObj := fileObjs[0]

	// For writing version, we need a separate DataID without compression/encryption
	// Check if current DataID has compression/encryption
	var writingDataID int64 = fileObj.DataID
	if fileObj.DataID > 0 && fileObj.DataID != EmptyDataID {
		dataInfo, err := lh.ma.GetData(c, bktID, fileObj.DataID)
		if err == nil && dataInfo != nil {
			hasCompression := dataInfo.Kind&DATA_CMPR_MASK != 0
			hasEncryption := dataInfo.Kind&DATA_ENDEC_MASK != 0
			// If original data has compression/encryption, create new DataID for writing version
			if hasCompression || hasEncryption {
				// Create new DataID for writing version (uncompressed/unencrypted)
				writingDataID = NewID()
				if writingDataID <= 0 {
					return nil, fmt.Errorf("failed to create DataID for writing version")
				}
				// Create DataInfo for writing version (no compression/encryption)
				writingDataInfo := &DataInfo{
					ID:       writingDataID,
					Size:     0,
					OrigSize: fileObj.Size,
					Kind:     DATA_NORMAL, // No compression, no encryption
				}
				if dataInfo.Kind&DATA_SPARSE != 0 {
					writingDataInfo.Kind |= DATA_SPARSE // Preserve sparse flag
				}
				_, err = lh.PutDataInfo(c, bktID, []*DataInfo{writingDataInfo})
				if err != nil {
					return nil, fmt.Errorf("failed to create DataInfo for writing version: %v", err)
				}
			}
		}
	} else if fileObj.DataID == 0 || fileObj.DataID == EmptyDataID {
		// File has no data yet, create new DataID for writing version
		writingDataID = NewID()
		if writingDataID <= 0 {
			return nil, fmt.Errorf("failed to create DataID for writing version")
		}
		// Create DataInfo for writing version (no compression/encryption)
		writingDataInfo := &DataInfo{
			ID:       writingDataID,
			Size:     0,
			OrigSize: fileObj.Size,
			Kind:     DATA_NORMAL | DATA_SPARSE, // No compression, no encryption, sparse
		}
		_, err = lh.PutDataInfo(c, bktID, []*DataInfo{writingDataInfo})
		if err != nil {
			return nil, fmt.Errorf("failed to create DataInfo for writing version: %v", err)
		}
	}

	// Create writing version with name="0"
	// ID is set to creation time (timestamp in seconds)
	creationTime := Now()
	writingVersion := &ObjectInfo{
		ID:     creationTime, // Use creation time as ID
		PID:    fileID,
		Type:   OBJ_TYPE_VERSION,
		Name:   WritingVersionName, // Will be changed to completion time when finished
		DataID: writingDataID,      // Use DataID without compression/encryption
		Size:   fileObj.Size,       // Use current file's Size
		MTime:  creationTime,
	}

	// Put the writing version (will not trigger version retention policy if name="0")
	ids, err := lh.Put(c, bktID, []*ObjectInfo{writingVersion})
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 || ids[0] == 0 {
		return nil, fmt.Errorf("failed to create writing version")
	}

	writingVersion.ID = ids[0]
	return writingVersion, nil
}

// UpdateData updates part of existing data chunk (for writing versions with name="0")
// This allows direct modification of data blocks without creating new versions
// offset: offset within the chunk, buf: data to write at that offset
// Unlike PutData which creates new data blocks, UpdateData modifies existing ones
func (lh *LocalHandler) UpdateData(c Ctx, bktID, dataID int64, sn int, offset int, buf []byte) error {
	if err := lh.acm.CheckPermission(c, DW, bktID); err != nil {
		return err
	}

	if len(buf) == 0 {
		return nil // Nothing to update
	}

	// For UpdateData, we need to handle quota differently:
	// If the data block already exists, we need to calculate the size difference
	// Get old data size if exists
	oldData, err := lh.da.Read(c, bktID, dataID, sn)
	oldSize := int64(0)
	if err == nil {
		oldSize = int64(len(oldData))
	}

	// Calculate new size after update
	newSize := int64(offset + len(buf))
	if newSize < oldSize {
		newSize = oldSize // Update doesn't shrink, only extends or modifies
	}

	sizeDiff := newSize - oldSize

	// Only update quota if size changed (extended)
	if sizeDiff > 0 {
		// Get bucket info and check quota
		buckets, err := lh.ma.GetBkt(c, []int64{bktID})
		if err != nil {
			return err
		}
		if len(buckets) == 0 {
			return ERR_QUERY_DB
		}
		bucket := buckets[0]

		// If quota >= 0, check if it exceeds quota
		if bucket.Quota >= 0 {
			if bucket.RealUsed+sizeDiff > bucket.Quota {
				return ERR_QUOTA_EXCEED
			}
		}

		// Increase usage (only when extending)
		if err := lh.ma.IncBktRealUsed(c, bktID, sizeDiff); err != nil {
			return err
		}
	}

	// Update data block using DataAdapter.Update (supports partial update)
	return lh.da.Update(c, bktID, dataID, sn, offset, buf)
}

// Put creates objects
// During garbage collection: data without metadata reference is dirty data (need window time)
// Metadata without data is corrupted data
// PID supports using two's complement to directly reference object ID that hasn't been uploaded yet
func (lh *LocalHandler) Put(c Ctx, bktID int64, o []*ObjectInfo) ([]int64, error) {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return nil, err
	}

	// 设置版本对象的时间戳（用于后续的版本保留策略处理）
	// Note: Writing versions (name="0") do not trigger version retention policy
	for _, obj := range o {
		if obj.Type == OBJ_TYPE_VERSION && obj.PID > 0 {
			// 设置MTime（如果未设置）
			if obj.MTime == 0 {
				obj.MTime = Now()
			}
		}
	}

	for _, x := range o {
		if x.ID == 0 {
			x.ID = NewID()
		}
		// Auto-generate name if empty (writing versions with name="0" keep their name)
		if x.Name == "" {
			x.Name = strconv.FormatInt(x.ID, 10)
		}
	}
	for _, x := range o {
		if x.PID < 0 && int(^x.PID) <= len(o) {
			x.PID = o[^x.PID].ID
		}
	}

	ids, err := lh.ma.PutObj(c, bktID, o)
	if err != nil {
		return ids, err
	}

	// 如果是新版本对象（OBJ_TYPE_VERSION），异步处理版本保留策略
	// 统一处理时间窗口内的版本合并和版本数量限制
	// Note: Writing versions (name="0") do not trigger version retention policy
	for _, obj := range o {
		if obj.Type == OBJ_TYPE_VERSION && obj.PID > 0 && obj.Name != WritingVersionName {
			// 异步应用版本保留策略（后置处理：时间窗口合并 + 版本数量限制）
			// 跳过正在写入的版本（name="0"），它们可以随时修改，不需要版本管理
			asyncApplyVersionRetention(c, bktID, obj.PID, obj.MTime, lh.ma, lh.da)
		}
	}

	// 如果是新版本对象（OBJ_TYPE_VERSION），更新父文件对象的DataID和大小
	for _, obj := range o {
		if obj.Type == OBJ_TYPE_VERSION && obj.PID > 0 && obj.DataID > 0 && obj.DataID != EmptyDataID {
			// 查找父文件对象
			parentObjs, err := lh.ma.GetObj(c, bktID, []int64{obj.PID})
			if err == nil && len(parentObjs) > 0 {
				parent := parentObjs[0]
				// 如果新版本的DataID更大，更新父文件的DataID和大小
				if parent.Type == OBJ_TYPE_FILE && obj.DataID > parent.DataID {
					// 从DataInfo获取大小
					dataInfo, err := lh.ma.GetData(c, bktID, obj.DataID)
					if err == nil && dataInfo != nil {
						updateObj := &ObjectInfo{
							ID:     parent.ID,
							DataID: obj.DataID,
							Size:   dataInfo.OrigSize,
							MTime:  obj.MTime,
						}
						lh.ma.SetObj(c, bktID, []string{"did", "size", "mtime"}, updateObj)
					} else {
						updateObj := &ObjectInfo{
							ID:     parent.ID,
							DataID: obj.DataID,
							MTime:  obj.MTime,
						}
						lh.ma.SetObj(c, bktID, []string{"did", "mtime"}, updateObj)
					}
				}
			}
		}
	}

	// Increase logical usage (calculate space even for instant upload)
	var totalSize int64
	var dedupSavings int64 // Space saved by instant upload
	for _, obj := range o {
		// Only file objects calculate size, directories don't
		if obj.Type == OBJ_TYPE_FILE && obj.Size > 0 {
			totalSize += obj.Size

			// 检查是否是秒传：如果DataID已存在且被其他对象引用，说明是秒传
			if obj.DataID > 0 && obj.DataID != EmptyDataID {
				// 查询DataID的引用计数（不包括当前刚创建的对象）
				// 注意：这里查询时当前对象已经创建，所以引用计数会包含当前对象
				// 如果引用计数 > 1，说明之前就有其他对象引用，这次是秒传
				refCounts, err := lh.ma.CountDataRefs(c, bktID, []int64{obj.DataID})
				if err == nil {
					refCount := refCounts[obj.DataID]
					// 如果引用计数 > 1，说明之前就有对象引用这个DataID，这次是秒传
					// 节省的空间 = 对象大小（因为数据已经存在，不需要再存储）
					if refCount > 1 {
						dedupSavings += obj.Size
					}
				}
			}
		}
	}
	if totalSize > 0 {
		if err := lh.ma.IncBktUsed(c, bktID, totalSize); err != nil {
			// 如果增加逻辑使用量失败，记录错误但不影响对象创建
			// 因为对象已经创建成功了
		}
		// 同时增加逻辑占用（只统计有效对象）
		if err := lh.ma.IncBktLogicalUsed(c, bktID, totalSize); err != nil {
			// 如果增加逻辑占用失败，记录错误但不影响对象创建
		}
	}
	// 增加秒传节省空间统计
	if dedupSavings > 0 {
		if err := lh.ma.IncBktDedupSavings(c, bktID, dedupSavings); err != nil {
			// 如果增加秒传节省空间失败，记录错误但不影响对象创建
		}
	}

	return ids, nil
}

func (lh *LocalHandler) Get(c Ctx, bktID int64, ids []int64) ([]*ObjectInfo, error) {
	if err := lh.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, err
	}
	return lh.ma.GetObj(c, bktID, ids)
}

func (lh *LocalHandler) List(c Ctx, bktID, pid int64, opt ListOptions) ([]*ObjectInfo, int64, string, error) {
	if err := lh.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, 0, "", err
	}
	return lh.ma.ListObj(c, bktID, pid, opt.Word, opt.Delim, opt.Order, opt.Count)
}

func (lh *LocalHandler) Rename(c Ctx, bktID, id int64, name string) error {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return err
	}
	return lh.ma.SetObj(c, bktID, []string{"name"}, &ObjectInfo{ID: id, Name: name})
}

// CreateVersionFromFile creates a new version from an existing file
// This is used when renaming a file to a name that already exists
// The existing file becomes a version, and the version's parent is the existing file itself
// After creating the version, the existing file can be replaced by the renamed file
func (lh *LocalHandler) CreateVersionFromFile(c Ctx, bktID, existingFileID int64) error {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return err
	}

	// Get the existing file object
	fileObjs, err := lh.ma.GetObj(c, bktID, []int64{existingFileID})
	if err != nil {
		return fmt.Errorf("failed to get existing file: %w", err)
	}
	if len(fileObjs) == 0 {
		return fmt.Errorf("file %d not found", existingFileID)
	}
	existingFile := fileObjs[0]

	// Only create version for files, not directories
	if existingFile.Type != OBJ_TYPE_FILE {
		return fmt.Errorf("object %d is not a file", existingFileID)
	}

	// Create a new version object from the existing file
	// The version will have the same DataID and Size as the existing file
	// The version's parent (PID) is the existing file itself
	versionTime := Now()
	newVersion := &ObjectInfo{
		ID:     NewID(),
		PID:    existingFileID, // Parent is the existing file (the file itself)
		Type:   OBJ_TYPE_VERSION,
		Name:   strconv.FormatInt(versionTime, 10), // Use timestamp as version name
		DataID: existingFile.DataID,
		Size:   existingFile.Size,
		MTime:  versionTime,
	}

	// Put the version (this will trigger version retention policy)
	ids, err := lh.Put(c, bktID, []*ObjectInfo{newVersion})
	if err != nil {
		return fmt.Errorf("failed to create version: %w", err)
	}
	if len(ids) == 0 || ids[0] == 0 {
		return fmt.Errorf("failed to create version: no ID returned")
	}

	return nil
}

func (lh *LocalHandler) MoveTo(c Ctx, bktID, id, pid int64) error {
	if err := lh.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return err
	}
	return lh.ma.SetObj(c, bktID, []string{"pid"}, &ObjectInfo{ID: id, PID: pid})
}

func (lh *LocalHandler) Recycle(c Ctx, bktID, id int64) error {
	if err := lh.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return err
	}
	// Recycle only marks as deleted without recursion for fast deletion
	// Child objects will be deleted asynchronously in the background
	return MarkObjectAsDeleted(c, bktID, id, lh.ma)
}

func (lh *LocalHandler) Delete(c Ctx, bktID, id int64) error {
	if err := lh.acm.CheckPermission(c, MDD, bktID); err != nil {
		return err
	}
	// Delete is permanent deletion, directly physically deletes object and data files
	return PermanentlyDeleteObject(c, bktID, id, lh, lh.ma, lh.da)
}

func (lh *LocalHandler) CleanRecycleBin(c Ctx, bktID int64, targetID int64) error {
	if err := lh.acm.CheckPermission(c, ALL, bktID); err != nil {
		return err
	}
	return CleanRecycleBin(c, bktID, lh, lh.ma, lh.da, targetID)
}

func (lh *LocalHandler) ListRecycleBin(c Ctx, bktID int64, opt ListOptions) ([]*ObjectInfo, int64, string, error) {
	if err := lh.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, 0, "", err
	}
	if opt.Count == 0 {
		opt.Count = DefaultListPageSize
	}
	return lh.ma.ListRecycleBin(c, bktID, opt)
}

func (lh *LocalHandler) GetBktInfo(c Ctx, bktID int64) (*BucketInfo, error) {
	if err := lh.acm.CheckOwn(c, bktID); err != nil {
		return nil, err
	}
	buckets, err := lh.ma.GetBkt(c, []int64{bktID})
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, ERR_QUERY_DB
	}
	bucket := buckets[0]
	// 同步bucket配置到SDKConfig
	if bucket != nil {
		// CmprWay is now smart compression by default
		lh.SetSDKConfig(bktID, bucket.CmprWay, bucket.CmprQlty, bucket.EndecWay, bucket.EndecKey)
	}
	return bucket, nil
}

func (lh *LocalHandler) UpdateFileLatestVersion(c Ctx, bktID int64) error {
	if err := lh.acm.CheckPermission(c, ALL, bktID); err != nil {
		return err
	}
	return UpdateFileLatestVersion(c, bktID, lh.ma)
}

type LocalAdmin struct {
	ma  MetadataAdapter
	da  DataAdapter
	acm AccessCtrlMgr
}

func NewLocalAdmin() Admin {
	dma := &DefaultMetadataAdapter{}
	return &LocalAdmin{
		ma:  dma,
		da:  &DefaultDataAdapter{},
		acm: &DefaultAccessCtrlMgr{ma: dma},
	}
}

// NewNoAuthAdmin creates an Admin that bypasses all authentication and permission checks
// This is useful for testing, internal operations, or when authentication is handled externally
// The admin uses NoAuthAccessCtrlMgr which always allows all operations
func NewNoAuthAdmin() Admin {
	dma := &DefaultMetadataAdapter{}
	return &LocalAdmin{
		ma:  dma,
		da:  &DefaultDataAdapter{},
		acm: &NoAuthAccessCtrlMgr{},
	}
}

// NewAdminWithAccessCtrl creates an Admin with a custom AccessCtrlMgr
// This allows injecting a custom access control manager for testing or special use cases
func NewAdminWithAccessCtrl(acm AccessCtrlMgr) Admin {
	dma := &DefaultMetadataAdapter{}
	if acm == nil {
		acm = &DefaultAccessCtrlMgr{ma: dma}
	}
	acm.SetAdapter(dma)
	return &LocalAdmin{
		ma:  dma,
		da:  &DefaultDataAdapter{},
		acm: acm,
	}
}

// NewAdminWithAdapters creates an Admin with custom MetadataAdapter, DataAdapter, and AccessCtrlMgr
// This provides maximum flexibility for testing or custom implementations
func NewAdminWithAdapters(ma MetadataAdapter, da DataAdapter, acm AccessCtrlMgr) Admin {
	if ma == nil {
		ma = &DefaultMetadataAdapter{}
	}
	if da == nil {
		da = &DefaultDataAdapter{}
	}
	if acm == nil {
		acm = &DefaultAccessCtrlMgr{ma: ma}
	}
	acm.SetAdapter(ma)
	return &LocalAdmin{
		ma:  ma,
		da:  da,
		acm: acm,
	}
}

// New returns current admin, forming a chain with underlying admin
func (la *LocalAdmin) New(Admin) Admin {
	// Ignore underlying admin
	return la
}

func (la *LocalAdmin) Close() {
}

func (la *LocalAdmin) PutBkt(c Ctx, o []*BucketInfo) error {
	if err := la.acm.CheckRole(c, ADMIN); err != nil {
		return err
	}
	return la.ma.PutBkt(c, o)
}

func (la *LocalAdmin) DeleteBkt(c Ctx, bktID int64) error {
	if err := la.acm.CheckRole(c, ADMIN); err != nil {
		return err
	}
	return la.ma.DeleteBkt(c, bktID)
}

func (la *LocalAdmin) SetQuota(c Ctx, bktID int64, quota int64) error {
	if err := la.acm.CheckRole(c, ADMIN); err != nil {
		return err
	}
	return la.ma.UpdateBktQuota(c, bktID, quota)
}

func (la *LocalAdmin) Scrub(c Ctx, bktID int64) (*ScrubResult, error) {
	if err := la.acm.CheckPermission(c, READ, bktID); err != nil {
		return nil, err
	}
	return ScrubData(c, bktID, la.ma, la.da)
}

func (la *LocalAdmin) ScanDirtyData(c Ctx, bktID int64) (*DirtyDataResult, error) {
	if err := la.acm.CheckPermission(c, ALL, bktID); err != nil {
		return nil, err
	}
	return ScanDirtyData(c, bktID, la.ma, la.da)
}

func (la *LocalAdmin) MergeDuplicateData(c Ctx, bktID int64) (*MergeDuplicateResult, error) {
	if err := la.acm.CheckPermission(c, ALL, bktID); err != nil {
		return nil, err
	}
	return MergeDuplicateData(c, bktID, la.ma, la.da)
}

func (la *LocalAdmin) Defragment(c Ctx, bktID int64) (*DefragmentResult, error) {
	if err := la.acm.CheckPermission(c, ALL, bktID); err != nil {
		return nil, err
	}
	return Defragment(c, bktID, la, la.ma, la.da)
}

func (la *LocalAdmin) CreateUser(c Ctx, username, password, name string, role uint32) (*UserInfo, error) {
	if err := la.acm.CheckRole(c, ADMIN); err != nil {
		return nil, err
	}

	// Check if username already exists
	existingUser, err := la.ma.GetUsr2(c, username)
	if err == nil && existingUser != nil && existingUser.ID > 0 {
		return nil, fmt.Errorf("user %s already exists", username)
	}

	// Encrypt password
	hashedPwd, err := HashPassword(password)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %v", err)
	}

	// Create user
	user := &UserInfo{
		ID:     NewID(),
		Usr:    username,
		Pwd:    hashedPwd,
		Name:   name,
		Role:   role,
		Avatar: "",
	}

	if err := la.ma.PutUsr(c, user); err != nil {
		return nil, err
	}

	// Clear sensitive information before returning
	user.Pwd = ""
	return user, nil
}

func (la *LocalAdmin) UpdateUser(c Ctx, userID int64, username, password, name string, role *uint32) error {
	if err := la.acm.CheckRole(c, ADMIN); err != nil {
		return err
	}

	// Get existing user
	users, err := la.ma.GetUsr(c, []int64{userID})
	if err != nil {
		return err
	}
	if len(users) == 0 {
		return fmt.Errorf("user with ID %d not found", userID)
	}
	user := users[0]

	// Update fields
	fields := []string{}
	if username != "" {
		// Check if new username is already used by another user
		existingUser, err := la.ma.GetUsr2(c, username)
		if err == nil && existingUser != nil && existingUser.ID > 0 && existingUser.ID != userID {
			return fmt.Errorf("username %s already exists", username)
		}
		user.Usr = username
		fields = append(fields, "usr")
	}
	if password != "" {
		hashedPwd, err := HashPassword(password)
		if err != nil {
			return fmt.Errorf("failed to hash password: %v", err)
		}
		user.Pwd = hashedPwd
		fields = append(fields, "pwd")
	}
	if name != "" {
		user.Name = name
		fields = append(fields, "name")
	}
	if role != nil {
		user.Role = *role
		fields = append(fields, "role")
	}

	if len(fields) == 0 {
		return nil // No fields to update
	}

	return la.ma.SetUsr(c, fields, user)
}

func (la *LocalAdmin) DeleteUser(c Ctx, userID int64) error {
	if err := la.acm.CheckRole(c, ADMIN); err != nil {
		return err
	}
	return la.ma.DeleteUser(c, userID)
}

func (la *LocalAdmin) ListUsers(c Ctx) ([]*UserInfo, error) {
	if err := la.acm.CheckRole(c, ADMIN); err != nil {
		return nil, err
	}
	return la.ma.ListUsers(c)
}
