package core

import (
	"strconv"

	"github.com/orca-zhang/idgen"
)

type ListOptions struct {
	Word  string // 过滤词，支持通配符*和?
	Delim string // 分隔符，每次请求后返回，原样回传即可
	Type  int    // 对象类型，-1: malformed, 0: 不过滤(default), 1: dir, 2: file, 3: version, 4: preview(thumb/m3u8/pdf)
	Count int    // 查询个数
	Order string // 排序方式，id/mtime/name/size/type 前缀 +: 升序（默认） -: 降序
	Brief int    // 显示更少内容(只在网络传输层，节省流量时有效)，0: FULL(default), 1: without EXT, 2:only ID
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
	Login(c Ctx, usr, pwd string) error

	// 只有文件长度、HdrCRC32是预Ref，如果成功返回新DataID，失败返回0
	// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
	// 如果非预Ref DataID传0，说明跳过了预Ref
	Ref(c Ctx, bktID int64, d []*DataInfo) ([]int64, error)
	// sn从0开始，DataID不传默认创建一个新的
	PutData(c Ctx, bktID, dataID int64, sn int, buf []byte) (int64, error)
	// 只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
	GetData(c Ctx, bktID, id int64, sn int, offset ...int) ([]byte, error)
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
	Recycle(c Ctx, bktID, id int64) error
	Delete(c Ctx, bktID, id int64) error
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
}

func NewLocalHandler() Handler {
	return &LocalHandler{
		ma:  &DefaultMetadataAdapter{},
		da:  &DefaultDataAdapter{},
		acm: &DefaultAccessCtrlMgr{},
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
}

func (lh *LocalHandler) SetAdapter(ma MetadataAdapter, da DataAdapter) {
	lh.ma = ma
	lh.da = da
}

func (lh *LocalHandler) Login(c Ctx, usr, pwd string) error {
	// pwd from db or cache
	// pbkdf2 check
	// set uid & key to ctx
	return ERR_AUTH_FAILED
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
func (lh *LocalHandler) GetData(c Ctx, bktID, id int64, sn int, offset ...int) ([]byte, error) {
	if err := lh.acm.CheckPermission(c, DR, bktID); err != nil {
		return nil, err
	}

	switch len(offset) {
	case 0:
		return lh.da.Read(c, bktID, id, sn)
	case 1:
		return lh.da.ReadBytes(c, bktID, id, sn, offset[0], -1)
	}
	return lh.da.ReadBytes(c, bktID, id, sn, offset[0], offset[1])
}

// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
// PID支持用补码来直接引用当次还未上传的对象的ID
func (lh *LocalHandler) Put(c Ctx, bktID int64, o []*ObjectInfo) ([]int64, error) {
	if err := lh.acm.CheckPermission(c, MDW, bktID); err != nil {
		return nil, err
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
	return lh.ma.PutObj(c, bktID, o)
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
	// TODO
	return nil
}

func (lh *LocalHandler) Delete(c Ctx, bktID, id int64) error {
	if err := lh.acm.CheckPermission(c, MDD, bktID); err != nil {
		return err
	}
	// TODO
	return nil
}

type LocalAdmin struct {
	aa  AdminAdapter
	acm AccessCtrlMgr
	ig  *idgen.IDGen
}

func NewLocalAdmin() Admin {
	return &LocalAdmin{
		aa:  &DefaultAdminAdapter{},
		acm: &DefaultAccessCtrlMgr{},
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
	return la.aa.PutBkt(c, o)
}
