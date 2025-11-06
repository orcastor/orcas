package core

import (
	"crypto/sha1"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/orca-zhang/idgen"
	"golang.org/x/crypto/pbkdf2"
)

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

type Options struct {
	Sync bool
}

type Handler interface {
	// 传入underlying，返回当前的，构成链式调用
	New(h Handler) Handler
	Close()

	SetOptions(opt Options)
	// 设置自定义的存储适配器
	SetAdapter(ma MetadataAdapter, da DataAdapter)

	// 登录用户
	Login(c Ctx, usr, pwd string) (Ctx, *UserInfo, []*BucketInfo, error)

	NewID() int64
	// 只有文件长度、HdrCRC32是预Ref，如果成功返回新DataID，失败返回0
	// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
	// 如果非预Ref DataID传0，说明跳过了预Ref
	Ref(c Ctx, bktID int64, d []*DataInfo) ([]int64, error)
	// sn从0开始，DataID不传默认创建一个新的
	PutData(c Ctx, bktID, dataID int64, sn int, buf []byte) (int64, error)
	// 只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
	GetData(c Ctx, bktID, id int64, sn int, offsetOrSize ...int) ([]byte, error)
	// 上传元数据
	PutDataInfo(c Ctx, bktID int64, d []*DataInfo) ([]int64, error)
	// 获取数据信息
	GetDataInfo(c Ctx, bktID, id int64) (*DataInfo, error)

	// Name不传默认用ID字符串化后的值作为Name
	Put(c Ctx, bktID int64, o []*ObjectInfo) ([]int64, error)
	Get(c Ctx, bktID int64, ids []int64) ([]*ObjectInfo, error)
	List(c Ctx, bktID, pid int64, opt ListOptions) (o []*ObjectInfo, cnt int64, delim string, err error)

	Rename(c Ctx, bktID, id int64, name string) error
	MoveTo(c Ctx, bktID, id, pid int64) error

	// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
	Recycle(c Ctx, bktID, id int64) error // 标记删除到回收站
	Delete(c Ctx, bktID, id int64) error

	// 清理回收站中已标记删除的对象（物理删除）
	CleanRecycleBin(c Ctx, bktID int64, targetID int64) error
	// 列举回收站
	ListRecycleBin(c Ctx, bktID int64, opt ListOptions) (o []*ObjectInfo, cnt int64, delim string, err error)

	// 审计数据完整性：检查元数据和数据文件的一致性
	Scrub(c Ctx, bktID int64) (*ScrubResult, error)
	// 扫描脏数据：检查上传失败、机器断电导致的不完整数据
	ScanDirtyData(c Ctx, bktID int64) (*DirtyDataResult, error)
	// 合并秒传重复数据：查找并合并具有相同校验值但不同DataID的重复数据
	MergeDuplicateData(c Ctx, bktID int64) (*MergeDuplicateResult, error)
	// 碎片整理：小文件离线归并打包，打包中被删除的数据块前移
	Defragment(c Ctx, bktID int64, maxSize int64, accessWindow int64) (*DefragmentResult, error)
	// 循环更新所有文件的最新版DataID、更新时间、大小和目录大小
	UpdateFileLatestVersion(c Ctx, bktID int64) error

	// 获取桶信息（用于获取桶配置）
	GetBkt(c Ctx, ids []int64) ([]*BucketInfo, error)
	// 设置桶配额
	SetQuota(c Ctx, bktID int64, quota int64) error
}

type Admin interface {
	// 传入underlying，返回当前的，构成链式调用
	New(a Admin) Admin
	Close()

	PutBkt(c Ctx, o []*BucketInfo) error
}

type LocalHandler struct {
	ma  MetadataAdapter
	da  DataAdapter
	acm AccessCtrlMgr
	ig  *idgen.IDGen
	opt Options
}

func NewLocalHandler() Handler {
	dma := &DefaultMetadataAdapter{}
	return &LocalHandler{
		ma:  dma,
		da:  &DefaultDataAdapter{},
		acm: &DefaultAccessCtrlMgr{ma: dma},
		ig:  idgen.NewIDGen(nil, 0), // 需要改成配置
	}
}

// 传入underlying，返回当前的，构成链式调用
func (lh *LocalHandler) New(Handler) Handler {
	// 忽略下层handler
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

	// set uid & key to ctx
	c, u.Pwd, u.Key = UserInfo2Ctx(c, u), "", ""
	return c, u, b, nil
}

func (lh *LocalHandler) NewID() int64 {
	id, _ := lh.ig.New()
	return id
}

// 只有文件长度、HdrCRC32是预Ref，如果成功返回新DataID，失败返回0
// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
// 如果非预Ref DataID传0，说明跳过了预Ref
func (lh *LocalHandler) Ref(c Ctx, bktID int64, d []*DataInfo) ([]int64, error) {
	if err := lh.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return nil, err
	}
	return lh.ma.RefData(c, bktID, d)
}

// 打包上传或者小文件，sn传-1，大文件sn从0开始，DataID不传默认创建一个新的
func (lh *LocalHandler) PutData(c Ctx, bktID, dataID int64, sn int, buf []byte) (int64, error) {
	if err := lh.acm.CheckPermission(c, DW, bktID); err != nil {
		return 0, err
	}

	// 检查配额并更新使用量（每个写入的数据块都需要检查）
	dataSize := int64(len(buf))
	if dataSize > 0 {
		// 获取桶信息并检查配额
		buckets, err := lh.ma.GetBkt(c, []int64{bktID})
		if err != nil {
			return 0, err
		}
		if len(buckets) == 0 {
			return 0, ERR_QUERY_DB
		}
		bucket := buckets[0]

		// 如果配额 >= 0，需要检查是否超过配额
		if bucket.Quota >= 0 {
			if bucket.RealUsed+dataSize > bucket.Quota {
				return 0, ERR_QUOTA_EXCEED
			}
		}

		// 增加实际使用量（原子操作，在数据库层面保证一致性）
		if err := lh.ma.IncBktRealUsed(c, bktID, dataSize); err != nil {
			return 0, err
		}
	}

	if dataID == 0 {
		if len(buf) <= 0 {
			dataID = EmptyDataID
		} else {
			dataID, _ = lh.ig.New()
		}
	}
	return dataID, lh.da.Write(c, bktID, dataID, sn, buf)
}

// 上传完数据以后，再创建元数据
func (lh *LocalHandler) PutDataInfo(c Ctx, bktID int64, d []*DataInfo) (ids []int64, err error) {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return nil, err
	}

	var n []*DataInfo
	for _, x := range d {
		if x.ID == 0 {
			x.ID, _ = lh.ig.New()
		}
		ids = append(ids, x.ID)
		if x.ID > 0 {
			n = append(n, x)
		}
	}
	// 设置为反码引用别的元素的数据
	for i := range ids {
		if ids[i] < 0 && ^ids[i] < int64(len(ids)) {
			ids[i] = ids[^ids[i]]
		}
	}
	return ids, lh.ma.PutData(c, bktID, n)
}

func (lh *LocalHandler) GetDataInfo(c Ctx, bktID, id int64) (*DataInfo, error) {
	if err := lh.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, err
	}
	return lh.ma.GetData(c, bktID, id)
}

// 只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
func (lh *LocalHandler) GetData(c Ctx, bktID, id int64, sn int, offsetOrSize ...int) ([]byte, error) {
	if err := lh.acm.CheckPermission(c, DR, bktID); err != nil {
		return nil, err
	}

	switch len(offsetOrSize) {
	case 0:
		return lh.da.Read(c, bktID, id, sn)
	case 1:
		return lh.da.ReadBytes(c, bktID, id, sn, offsetOrSize[0], -1)
	}
	return lh.da.ReadBytes(c, bktID, id, sn, offsetOrSize[0], offsetOrSize[1])
}

// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
// PID支持用补码来直接引用当次还未上传的对象的ID
func (lh *LocalHandler) Put(c Ctx, bktID int64, o []*ObjectInfo) ([]int64, error) {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return nil, err
	}

	// 设置版本对象的时间戳（用于后续的版本保留策略处理）
	for _, obj := range o {
		if obj.Type == OBJ_TYPE_VERSION && obj.PID > 0 {
			// 设置MTime（如果未设置）
			if obj.MTime == 0 {
				obj.MTime = time.Now().Unix()
			}
		}
	}

	for _, x := range o {
		if x.ID == 0 {
			x.ID, _ = lh.ig.New()
		}
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
	for _, obj := range o {
		if obj.Type == OBJ_TYPE_VERSION && obj.PID > 0 {
			// 异步应用版本保留策略（后置处理：时间窗口合并 + 版本数量限制）
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

	// 增加逻辑使用量（即使秒传了也要计算空间）
	var totalSize int64
	var dedupSavings int64 // 秒传节省的空间
	for _, obj := range o {
		// 只有文件对象才计算大小，目录不计算
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
	// Recycle 只是标记删除，真正的清理由 CleanRecycleBin 完成
	return DeleteObject(c, bktID, id, lh.ma)
}

func (lh *LocalHandler) Delete(c Ctx, bktID, id int64) error {
	if err := lh.acm.CheckPermission(c, MDD, bktID); err != nil {
		return err
	}
	// Delete 是彻底删除，直接物理删除对象和数据文件
	return PermanentlyDeleteObject(c, bktID, id, lh, lh.ma, lh.da)
}

func (lh *LocalHandler) CleanRecycleBin(c Ctx, bktID int64, targetID int64) error {
	if err := lh.acm.CheckPermission(c, MDRW, bktID); err != nil {
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

func (lh *LocalHandler) Scrub(c Ctx, bktID int64) (*ScrubResult, error) {
	if err := lh.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, err
	}
	return ScrubData(c, bktID, lh.ma, lh.da)
}

func (lh *LocalHandler) ScanDirtyData(c Ctx, bktID int64) (*DirtyDataResult, error) {
	if err := lh.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, err
	}
	return ScanDirtyData(c, bktID, lh.ma, lh.da)
}

func (lh *LocalHandler) GetBkt(c Ctx, ids []int64) ([]*BucketInfo, error) {
	return lh.ma.GetBkt(c, ids)
}

func (lh *LocalHandler) SetQuota(c Ctx, bktID int64, quota int64) error {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return err
	}
	return lh.ma.UpdateBktQuota(c, bktID, quota)
}

func (lh *LocalHandler) MergeDuplicateData(c Ctx, bktID int64) (*MergeDuplicateResult, error) {
	if err := lh.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return nil, err
	}
	return MergeDuplicateData(c, bktID, lh.ma, lh.da)
}

func (lh *LocalHandler) Defragment(c Ctx, bktID int64, maxSize int64, accessWindow int64) (*DefragmentResult, error) {
	if err := lh.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return nil, err
	}
	return Defragment(c, bktID, lh, lh.ma, lh.da, maxSize, accessWindow)
}

func (lh *LocalHandler) UpdateFileLatestVersion(c Ctx, bktID int64) error {
	if err := lh.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return err
	}
	return UpdateFileLatestVersion(c, bktID, lh.ma)
}

type LocalAdmin struct {
	ma  MetadataAdapter
	acm AccessCtrlMgr
	ig  *idgen.IDGen
}

func NewLocalAdmin() Admin {
	dma := &DefaultMetadataAdapter{}
	return &LocalAdmin{
		ma:  dma,
		acm: &DefaultAccessCtrlMgr{ma: dma},
		ig:  idgen.NewIDGen(nil, 0), // 需要改成配置
	}
}

// 传入underlying，返回当前的，构成链式调用
func (la *LocalAdmin) New(Admin) Admin {
	// 忽略下层handler
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
