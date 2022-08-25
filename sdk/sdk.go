package sdk

type Config struct {
	DataSync bool   // Power-off Protection Method，强制每次写入数据后刷到磁盘
	RefLevel int    // None / Ref / TryRef+Ref
	RefThres int    // 秒传大小限制
	Conflict int    // Throw / Merge / Rename，同名冲突后
	NameTail string // 重命名尾巴，"-副本" / "{\d}"
	BEDecmpr bool   // 后端解压，PS：必须是非加密数据
	WiseCmpr bool   // 智能压缩，根据文件类型决定是否压缩
}
