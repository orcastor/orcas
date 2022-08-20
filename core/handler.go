package core

import "context"

type Hanlder interface {
	// 传入underlying，返回当前的，构成链式调用
	New(h Hanlder) Hanlder

	// 只有文件长度、HdrCRC32是预校验，如果成功返回新DataID，失败返回0
	// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
	// 如果非预校验DataID传0，说明跳过了预校验
	Ref(c context.Context, d []*DataInfo) ([]int64, error)
	// 打包上传或者小文件，sn传-1，大文件sn从0开始，DataID不传默认创建一个新的
	PutData(c context.Context, dataID int64, sn int, buf []byte) (int64, error)
	// 上传完数据以后，再创建元数据
	PutDataInfo(c context.Context, d []*DataInfo) error
	// 只传一个参数说明是sn，传两个参数说明是offset+size
	GetData(c context.Context, o *ObjectInfo, sn ...int64) ([]int64, error)

	// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
	Create(c context.Context, o []*ObjectInfo) ([]int64, error)
	List(c context.Context, o *ObjectInfo, opt ListOptions) ([]*ObjectInfo, error)

	Rename(c context.Context, o *ObjectInfo, newName string) error
	MoveTo(c context.Context, o, newParent *ObjectInfo) error

	Recycle(c context.Context, o *ObjectInfo) error
	Delete(c context.Context, o *ObjectInfo) error
}
