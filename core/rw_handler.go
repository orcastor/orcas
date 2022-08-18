package core

type RWHanlder struct {
}

// 传入underlying，返回当前的，构成链式调用
func (ch *RWHanlder) New(Hanlder) Hanlder {
	// 忽略下层handler
	return ch
}

// 只有文件长度、HdrCRC32是预校验，如果成功返回DataID，失败返回0
// 有文件长度、CRC32、MD5，成功返回引用的DataID，失败返回0，客户端发现DataID有变化，说明不需要上传数据
// 如果非预校验DataID传0，说明跳过了预校验
func (ch *RWHanlder) Ref(d []*DataInfo) ([]uint64, error) {
	return nil, nil
}

// 打包上传或者小文件，sn传-1，大文件sn从0开始，DataID不传默认创建一个新的
func (ch *RWHanlder) PutData(dataID uint64, sn int, buf []byte) (uint64, error) {
	return 0, nil
}

// 上传完数据以后，再创建元数据
func (ch *RWHanlder) PutDataInfo(d []*DataInfo) error {
	return nil
}

// 垃圾回收时有数据没有元数据引用的为脏数据（需要留出窗口时间），有元数据没有数据的为损坏数据
func (ch *RWHanlder) Create(o []*ObjectInfo) ([]uint64, error) {
	return nil, nil
}

// 只传一个参数说明是sn，传两个参数说明是offset+size
func (ch *RWHanlder) Get(o *ObjectInfo, sn ...uint64) ([]uint64, error) {
	return nil, nil
}

func (ch *RWHanlder) List(o *ObjectInfo, opt ListOptions) ([]*ObjectInfo, error) {
	return nil, nil
}

func (ch *RWHanlder) Rename(o *ObjectInfo, newName string) error {
	return nil
}

func (ch *RWHanlder) MoveTo(o, newParent *ObjectInfo) error {
	return nil
}

func (ch *RWHanlder) Recycle(o *ObjectInfo) error {
	return nil
}

func (ch *RWHanlder) Delete(o *ObjectInfo) error {
	return nil
}
