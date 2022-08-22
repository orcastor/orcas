package sdk

type Config struct {
	DataSync     int    // Power-off Protection Method，强制每次写入数据后刷到磁盘
	TryRef       int    // None / Ref / TryRef+Ref
	Conflict     int    // Throw / Merge / Rename，同名冲突后
	ConflictTail string // "-副本" / "{\d}"
	SSideDecompr int    // 服务端解压，PS：必须是非加密数据
}
