package core

import (
	"context"

	"github.com/orca-zhang/idgen"
)

type Hanlder interface {
	// 传入underlying，返回当前的，构成链式调用
	New(h Hanlder) Hanlder

	// 只有文件长度、HdrCRC32是预Ref，如果成功返回新DataID，失败返回0
	// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
	// 如果非预Ref DataID传0，说明跳过了预Ref
	Ref(c context.Context, d []*DataInfo) ([]int64, error)
	// 打包上传或者小文件，sn传-1，大文件sn从0开始，DataID不传默认创建一个新的
	PutData(c context.Context, dataID int64, sn int, buf []byte) (int64, error)
	// 上传完数据以后，再创建元数据
	PutDataInfo(c context.Context, d []*DataInfo) error
	// 只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
	GetData(c context.Context, o *ObjectInfo, sn int, offset ...int64) ([]byte, error)

	// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
	Create(c context.Context, o []*ObjectInfo) ([]int64, error)
	List(c context.Context, o *ObjectInfo, opt ListOptions) ([]*ObjectInfo, error)

	Rename(c context.Context, o *ObjectInfo, newName string) error
	MoveTo(c context.Context, o, newParent *ObjectInfo) error

	Recycle(c context.Context, o *ObjectInfo) error
	Delete(c context.Context, o *ObjectInfo) error
}

type RWHanlder struct {
	mo MetaOperator
	do DataOperator
	ig *idgen.IDGen
}

func New() Hanlder {
	return &RWHanlder{
		mo: &DefaultMetaOperator{},
		do: &DefaultDataOperator{},
		ig: idgen.NewIDGen(nil, 0), // 需要改成配置
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
func (ch *RWHanlder) Ref(c context.Context, d []*DataInfo) ([]int64, error) {
	return ch.mo.Ref(c, d)
}

// 打包上传或者小文件，sn传-1，大文件sn从0开始，DataID不传默认创建一个新的
func (ch *RWHanlder) PutData(c context.Context, dataID int64, sn int, buf []byte) (int64, error) {
	if dataID == 0 {
		dataID, _ = ch.ig.New()
	}
	return dataID, ch.do.Write(c, dataID, sn, buf)
}

// 上传完数据以后，再创建元数据
func (ch *RWHanlder) PutDataInfo(c context.Context, d []*DataInfo) error {
	return ch.mo.PutDataInfo(c, d)
}

// 只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
func (ch *RWHanlder) GetData(c context.Context, o *ObjectInfo, sn int, offset ...int64) ([]byte, error) {
	switch len(offset) {
	case 0:
		return ch.do.Read(c, o.DataID, sn)
	case 1:
		return ch.do.ReadBytes(c, o.DataID, sn, offset[0], -1)
	}
	return ch.do.ReadBytes(c, o.DataID, sn, offset[0], offset[1])
}

// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
func (ch *RWHanlder) Create(c context.Context, o []*ObjectInfo) ([]int64, error) {
	return nil, nil
}

func (ch *RWHanlder) List(c context.Context, o *ObjectInfo, opt ListOptions) ([]*ObjectInfo, error) {
	return nil, nil
}

func (ch *RWHanlder) Rename(c context.Context, o *ObjectInfo, newName string) error {
	return nil
}

func (ch *RWHanlder) MoveTo(c context.Context, o, newParent *ObjectInfo) error {
	return nil
}

func (ch *RWHanlder) Recycle(c context.Context, o *ObjectInfo) error {
	return nil
}

func (ch *RWHanlder) Delete(c context.Context, o *ObjectInfo) error {
	return nil
}
