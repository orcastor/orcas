package sdk

type Config struct {
	DataSync int    // Power-off Protection Method，强制每次写入数据后刷到磁盘
	RefLevel int    // None / Ref / TryRef+Ref
	Conflict int    // Throw / Merge / Rename，同名冲突后
	NameTail string // 重命名尾巴，"-副本" / "{\d}"
	BEDecmpr int    // 后端解压，PS：必须是非加密数据
	WiseCmpr int    // 智能压缩，根据文件类型决定是否压缩
}
