package core

import (
	"strconv"

	"github.com/orca-zhang/idgen"
)

type ListOptions struct {
	Word  string // 过滤词，支持通配符*和?
	Delim string // 分隔符，每次请求后返回，原样回传即可
	Type  int    // 对象类型，0: 不过滤(default), 1: dir, 2: file, 3: version, 4: preview(thumb/m3u8/pdf)
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

	PutBkt(c Ctx, o []*BucketInfo) error

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

type RWHandler struct {
	ma  MetadataAdapter
	da  DataAdapter
	acm AccessCtrlMgr
	ig  *idgen.IDGen
}

func NewRWHandler() Handler {
	return &RWHandler{
		ma:  &DefaultMetadataAdapter{},
		da:  &DefaultDataAdapter{},
		acm: &DefaultAccessCtrlMgr{},
		ig:  idgen.NewIDGen(nil, 0), // 需要改成配置
	}
}

// 传入underlying，返回当前的，构成链式调用
func (ch *RWHandler) New(Handler) Handler {
	// 忽略下层handler
	return ch
}

func (ch *RWHandler) Close() {
	ch.da.Close()
	ch.ma.Close()
}

func (ch *RWHandler) SetOptions(opt Options) {
	ch.da.SetOptions(opt)
}

func (ch *RWHandler) SetAdapter(ma MetadataAdapter, da DataAdapter) {
	ch.ma = ma
	ch.da = da
}

func (ch *RWHandler) PutBkt(c Ctx, o []*BucketInfo) error {
	if err := ch.acm.CheckRole(c, ADMIN); err != nil {
		return err
	}
	return ch.ma.PutBkt(c, o)
}

// 只有文件长度、HdrCRC32是预Ref，如果成功返回新DataID，失败返回0
// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
// 如果非预Ref DataID传0，说明跳过了预Ref
func (ch *RWHandler) Ref(c Ctx, bktID int64, d []*DataInfo) ([]int64, error) {
	if err := ch.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return nil, err
	}
	return ch.ma.RefData(c, bktID, d)
}

// 打包上传或者小文件，sn传-1，大文件sn从0开始，DataID不传默认创建一个新的
func (ch *RWHandler) PutData(c Ctx, bktID, dataID int64, sn int, buf []byte) (int64, error) {
	if err := ch.acm.CheckPermission(c, DW, bktID); err != nil {
		return 0, err
	}

	if dataID == 0 {
		if len(buf) <= 0 {
			dataID = EmptyDataID
		} else {
			dataID, _ = ch.ig.New()
		}
	}
	return dataID, ch.da.Write(c, bktID, dataID, sn, buf)
}

// 上传完数据以后，再创建元数据
func (ch *RWHandler) PutDataInfo(c Ctx, bktID int64, d []*DataInfo) (ids []int64, err error) {
	if err := ch.acm.CheckPermission(c, MDW, bktID); err != nil {
		return nil, err
	}

	var n []*DataInfo
	for _, x := range d {
		if x.ID == 0 {
			x.ID, _ = ch.ig.New()
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
	return ids, ch.ma.PutData(c, bktID, n)
}

func (ch *RWHandler) GetDataInfo(c Ctx, bktID, id int64) (*DataInfo, error) {
	if err := ch.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, err
	}
	return ch.ma.GetData(c, bktID, id)
}

// 只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
func (ch *RWHandler) GetData(c Ctx, bktID, id int64, sn int, offset ...int) ([]byte, error) {
	if err := ch.acm.CheckPermission(c, DR, bktID); err != nil {
		return nil, err
	}

	switch len(offset) {
	case 0:
		return ch.da.Read(c, bktID, id, sn)
	case 1:
		return ch.da.ReadBytes(c, bktID, id, sn, offset[0], -1)
	}
	return ch.da.ReadBytes(c, bktID, id, sn, offset[0], offset[1])
}

// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
// PID支持用补码来直接引用当次还未上传的对象的ID
func (ch *RWHandler) Put(c Ctx, bktID int64, o []*ObjectInfo) ([]int64, error) {
	if err := ch.acm.CheckPermission(c, MDW, bktID); err != nil {
		return nil, err
	}

	for _, x := range o {
		if x.ID == 0 {
			x.ID, _ = ch.ig.New()
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
	return ch.ma.PutObj(c, bktID, o)
}

func (ch *RWHandler) Get(c Ctx, bktID int64, ids []int64) ([]*ObjectInfo, error) {
	if err := ch.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, err
	}
	return ch.ma.GetObj(c, bktID, ids)
}

func (ch *RWHandler) List(c Ctx, bktID, pid int64, opt ListOptions) ([]*ObjectInfo, int64, string, error) {
	if err := ch.acm.CheckPermission(c, MDR, bktID); err != nil {
		return nil, 0, "", err
	}
	return ch.ma.ListObj(c, bktID, pid, opt.Word, opt.Delim, opt.Order, opt.Count)
}

// 如果存在同名文件，会报错：Error: stepping, UNIQUE constraint failed: obj.name (19)
func (ch *RWHandler) Rename(c Ctx, bktID, id int64, name string) error {
	if err := ch.acm.CheckPermission(c, MDW, bktID); err != nil {
		return err
	}
	return ch.ma.SetObj(c, bktID, []string{"name"}, &ObjectInfo{ID: id, Name: name})
}

func (ch *RWHandler) MoveTo(c Ctx, bktID, id, pid int64) error {
	if err := ch.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return err
	}
	return ch.ma.SetObj(c, bktID, []string{"pid"}, &ObjectInfo{ID: id, PID: pid})
}

func (ch *RWHandler) Recycle(c Ctx, bktID, id int64) error {
	if err := ch.acm.CheckPermission(c, MDRW, bktID); err != nil {
		return err
	}
	// TODO
	return nil
}

func (ch *RWHandler) Delete(c Ctx, bktID, id int64) error {
	if err := ch.acm.CheckPermission(c, MDD, bktID); err != nil {
		return err
	}
	// TODO
	return nil
}
