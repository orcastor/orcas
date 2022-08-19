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

type ListOptions struct {
	Word  string // 过滤词
	Delim string // 分隔符
	Type  int    // 对象类型，0: none, 1: dir, 2: file, 3: version, 4: thumb, 5. HLS(m3u8)
	Size  uint64 // 查询个数
	Order string // 排序方式，time/name/size 前缀 +: 升序（默认） -: 降序，多个字段用逗号分割
}

type ObjectOpConfig struct {
	DataSync     int    // Power-off Protection Method，强制每次写入数据后刷到磁盘
	TryRef       int    // None / Ref / TryRef+Ref
	Conflict     int    // Throw / Merge / Rename，同名冲突后
	ConflictTail string // "-副本" / "{\d}"
	SSideDecompr int    // 服务端解压，PS：必须是非加密数据
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
