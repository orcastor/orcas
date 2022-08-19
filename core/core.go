package core

/*
这里定义文件的接口：

批量写入文件（单次文件数据量不超过4MB）同时可以带部分文件的秒传/秒传预筛选
读取文件
列举目录：无限加载模式，支持按文件名、文件类型过滤，支持文件名称、大小、时间排序
随机读取文件的一部分（在线播放和在线预览等）

id生成器，默认单机的，如果是分布式的，可能需要改成分布式的

【文件的属性包括】
父级的id
文件名称
文件大小
创建时间
修改时间（如果没有，那就是创建时间）
访问时间（如果没有，那就是修改时间>创建时间）
创建时的冲突解决方式（合并、重命名）
文件的类型
是否秒传
存储块id、存储块偏移位置
快照版本
幂等操作id
【附加属性】
是否压缩
原始MD5值
原始CRC32值
前32KB crc32值

【存储块】
MD5值
8KB对齐，最大尽量在4MB以内

端到端的实现：
在客户端做加解密和解压缩
本地的实现，数据直接写入存储；远端的实现，数据通过rpc传输，调用方无法感觉到差别

如何识别出服务端发生过异常或者断电？
-> 有写数据的bucket记录并刷盘，定期清理不再继续写入的bucket
如何识别由此导致的数据不完整或者丢失？
-> 重新启动以后，对上次还在写入数据的bucket最新写入的文件进行扫描，不完整的标记畸形
或者说断电一致性保障方案？

元数据和数据的时序以及是否实时刷盘？
写入是先数据后元数据，删除是先元数据后数据，只有在刷盘以后才做后一步

数据一致性通过auditor来定期扫描实现
auditor扫描低频

单独的job处理文件数量、文件大小、删除等

上传文件的过程：
hdrCRC32和长度来预检查是否可能秒传（可以用阈值来优化小对象直接跳过预检查）
没有可能的直接上传
有可能的，计算整个文件的MD5和CRC32后尝试秒传
秒传失败，转普通上传
*/

type ObjectInfo struct {
	ID        uint64 // 对象ID（idgen随机生成的id）
	ParentID  uint64 // 父对象ID
	UpdatedAt int64  // 更新时间
	DataID    uint64 // 数据ID，如果为0，说明没有数据（新创建的文件，DataID就是对象ID，作为对象的首版本数据）
	// BcktID uint64 // 桶ID，如果支持引用别的桶的数据，为0说明是本桶
	Type   int    // 对象类型，0: none, 1: dir, 2: file, 3: version, 4: thumb, 5. HLS(m3u8)
	Status int    // 对象状态，0: none, 1: deleted, 2: recycle(to be deleted), 3: malformed
	Name   string // 对象名称
	Size   uint64 // 对象的大小，目录的大小是子对象数，文件的大小是最新版本的字节数
}

type DataInfo struct {
	ID       uint64 // 数据ID（对象ID/版本ID，idgen随机生成的id）
	Size     uint64 // 数据的大小
	HdrCRC32 uint64 // 头部100KB的CRC32校验值
	CRC32    uint64 // 整个对象的CRC32校验值（最原始数据）
	MD5      uint64 // 整个对象的MD5值（最原始数据）

	// MIME       string // 数据的多媒体类型
	Kind   int // 数据类型（用于预览等），0: etc, 1: image, 2: video, 3: docs
	Status int // 数据状态，正常、压缩、加密、损坏
	// Normal      0x01, 正常
	// Malformed   0x02, 是否损坏
	// Compressed  0x04, 是否压缩，0: none, 1: snappy, 2: zip 3: zstd
	// Encrypted   0x08, 是否加密

	// PkgID不为0说明是打包数据
	PkgID     uint64 // 打包数据的ID（也是idgen生成的id）
	PkgOffset uint64 // 打包数据的偏移位置

	// SnapshotID uint64 // 快照版本ID
}

// 数据存储，<=4194304B的对象，用<ID/PkgID>为名称，否则用<ID/PkgID>-<SN>为名称，<SN>为数据块的序号，从0开始递增

type ListOptions struct {
	Word  string // 过滤词
	Delim string // 分隔符
	Type  int    // 对象类型，0: none, 1: dir, 2: file, 3: version, 4: thumb, 5. HLS(m3u8)
	Size  uint64 // 查询个数
	Order string // 排序方式，time/name/size 前缀 +: 升序（默认） -: 降序，多个字段用逗号分割
}

type BucketInfo struct {
	ID   uint64      // 桶ID
	Name string      // 桶名称
	Root *ObjectInfo // 桶的根对象
	// SnapshotID uint64 // 最新快照版本ID
}

type ObjectOpConfig struct {
	PwroffProtection int    // Power-off Protection Method，强制每次写入数据后刷到磁盘
	TryRef           int    // None / Ref / TryRef+Ref
	SameNameConflict int    // Throw / Merge / Rename
	ConflictTail     string // "-副本" / "{\d}"
	ServSideDecompr  int    // 服务端解压，PS：必须是非加密数据
}

type Bucket interface {
	Info() (*BucketInfo, error)
}

type Object interface {
	Info() (*ObjectInfo, error)

	GetData(sn ...uint64) ([]byte, error)

	List(opt ListOptions) ([]*Object, error)

	Put([]*ObjectInfo, []byte) error
	Delete() error

	Rename(newName string) error
	MoveTo(parent *ObjectInfo) error
}
