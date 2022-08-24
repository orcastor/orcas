package core

import (
	"github.com/orca-zhang/idgen"
)

type ListOptions struct {
	Word  string // 过滤词
	Delim string // 分隔符
	Type  int    // 对象类型，0: none, 1: dir, 2: file, 3: version, 4: thumb, 5. HLS(m3u8)
	Size  int    // 查询个数
	Order string // 排序方式，id/mtime/name/size/type 前缀 +: 升序（默认） -: 降序
}

type Hanlder interface {
	// 传入underlying，返回当前的，构成链式调用
	New(h Hanlder) Hanlder

	// 只有文件长度、HdrCRC32是预Ref，如果成功返回新DataID，失败返回0
	// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
	// 如果非预Ref DataID传0，说明跳过了预Ref
	Ref(c Ctx, d []*DataInfo) ([]int64, error)
	// 打包上传或者小文件，sn传-1，大文件sn从0开始，DataID不传默认创建一个新的
	PutData(c Ctx, dataID int64, sn int, buf []byte) (int64, error)
	// 上传完数据以后，再创建元数据
	PutDataInfo(c Ctx, d []*DataInfo) error
	// 只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
	GetData(c Ctx, o *ObjectInfo, sn int, offset ...int64) ([]byte, error)
	// 用于非文件内容的扫描，只看文件是否存在，大小是否合适
	FileSize(c Ctx, dataID int64, sn int) (int64, error)

	// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
	Put(c Ctx, o []*ObjectInfo) ([]int64, error)
	List(c Ctx, pid int64, opt ListOptions) (o []*ObjectInfo, cnt int64, delim string, err error)

	Rename(c Ctx, id int64, name string) error
	MoveTo(c Ctx, id, pid int64) error

	Recycle(c Ctx, id int64) error
	Delete(c Ctx, id int64) error
}

type RWHanlder struct {
	mo    MetaOperator
	do    DataOperator
	ig    *idgen.IDGen
	bktID int64
}

func New(bktID int64) Hanlder {
	acm := &DefaultAccessCtrlMgr{}
	return &RWHanlder{
		mo:    NewDefaultMetaOperator(acm),
		do:    NewDefaultDataOperator(acm),
		ig:    idgen.NewIDGen(nil, 0), // 需要改成配置
		bktID: bktID,
	}
}

// 传入underlying，返回当前的，构成链式调用
func (ch *RWHanlder) New(Hanlder) Hanlder {
	// 忽略下层handler
	return ch
}

// 只有文件长度、HdrCRC32是预Ref，如果成功返回新DataID，失败返回0
// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
// 如果非预Ref DataID传0，说明跳过了预Ref
func (ch *RWHanlder) Ref(c Ctx, d []*DataInfo) ([]int64, error) {
	return ch.mo.RefData(c, ch.bktID, d)
}

// 打包上传或者小文件，sn传-1，大文件sn从0开始，DataID不传默认创建一个新的
func (ch *RWHanlder) PutData(c Ctx, dataID int64, sn int, buf []byte) (int64, error) {
	if dataID == 0 {
		dataID, _ = ch.ig.New()
	}
	return dataID, ch.do.Write(c, ch.bktID, dataID, sn, buf)
}

// 上传完数据以后，再创建元数据
func (ch *RWHanlder) PutDataInfo(c Ctx, d []*DataInfo) error {
	return ch.mo.PutData(c, ch.bktID, d)
}

// 只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
func (ch *RWHanlder) GetData(c Ctx, o *ObjectInfo, sn int, offset ...int64) ([]byte, error) {
	switch len(offset) {
	case 0:
		return ch.do.Read(c, ch.bktID, o.DataID, sn)
	case 1:
		return ch.do.ReadBytes(c, ch.bktID, o.DataID, sn, offset[0], -1)
	}
	return ch.do.ReadBytes(c, ch.bktID, o.DataID, sn, offset[0], offset[1])
}

// 用于非文件内容的扫描，只看文件是否存在，大小是否合适
func (ch *RWHanlder) FileSize(c Ctx, dataID int64, sn int) (int64, error) {
	return ch.do.FileSize(c, ch.bktID, dataID, sn)
}

// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
func (ch *RWHanlder) Put(c Ctx, o []*ObjectInfo) ([]int64, error) {
	return ch.mo.PutObj(c, ch.bktID, o)
}

func (ch *RWHanlder) List(c Ctx, pid int64, opt ListOptions) ([]*ObjectInfo, int64, string, error) {
	return ch.mo.ListObj(c, ch.bktID, pid, opt.Word, opt.Delim, opt.Order, opt.Size, 0)
}

func (ch *RWHanlder) Rename(c Ctx, id int64, name string) error {
	return ch.mo.SetObj(c, ch.bktID, []string{"name"}, &ObjectInfo{ID: id, Name: name})
	// TODO: 处理有冲突的情况
}

func (ch *RWHanlder) MoveTo(c Ctx, id, pid int64) error {
	return ch.mo.SetObj(c, ch.bktID, []string{"pid"}, &ObjectInfo{ID: id, PID: pid})
	// TODO: 处理有冲突的情况
}

func (ch *RWHanlder) Recycle(c Ctx, id int64) error {
	return ch.mo.SetObj(c, ch.bktID, []string{"status"}, &ObjectInfo{ID: id, Status: OBJ_RECYCLED})
}

func (ch *RWHanlder) Delete(c Ctx, id int64) error {
	return ch.mo.SetObj(c, ch.bktID, []string{"status"}, &ObjectInfo{ID: id, Status: OBJ_DELETED})
}
