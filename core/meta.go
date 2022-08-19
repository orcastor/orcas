package core

import (
	"context"
	"database/sql"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	b "github.com/orca-zhang/borm"
)

type BucketInfo struct {
	ID   int64       `borm:"id"`      // 桶ID
	Name string      `borm:"name"`    // 桶名称
	Root *ObjectInfo `borm:"root_id"` // 桶的根对象
	// SnapshotID int64 // 最新快照版本ID
}

type ObjectInfo struct {
	ID        int64 `borm:"id"`         // 对象ID（idgen随机生成的id）
	ParentID  int64 `borm:"pid"`        // 父对象ID
	UpdatedAt int64 `borm:"updated_at"` // 更新时间
	DataID    int64 `borm:"data_id"`    // 数据ID，如果为0，说明没有数据（新创建的文件，DataID就是对象ID，作为对象的首版本数据）
	// BktID int64 // 桶ID，如果支持引用别的桶的数据，为0说明是本桶
	Type   int    `borm:"type"` // 对象类型，0: none, 1: dir, 2: file, 3: version, 4: thumb, 5. HLS(m3u8)
	Status int    `borm:"stat"` // 对象状态，0: none, 1: deleted, 2: recycle(to be deleted), 3: malformed
	Name   string `borm:"name"` // 对象名称
	Size   int64  `borm:"size"` // 对象的大小，目录的大小是子对象数，文件的大小是最新版本的字节数
	Ext    string `borm:"ext"`
}

type DataInfo struct {
	ID       int64  `borm:"id"`        // 数据ID（对象ID/版本ID，idgen随机生成的id）
	Size     int64  `borm:"size"`      // 数据的大小
	Checksum uint64 `borm:"checksum"`  // 整个对象的MD5值（最终数据，用于一致性审计）
	HdrCRC32 uint64 `borm:"hdr_crc32"` // 头部100KB的CRC32校验值
	CRC32    uint64 `borm:"crc32"`     // 整个对象的CRC32校验值（最原始数据）
	MD5      uint64 `borm:"md5"`       // 整个对象的MD5值（最原始数据）

	// MIME       string // 数据的多媒体类型
	Kind   int `borm:"kind"`   // 数据类型（用于预览等），0: etc, 1: image, 2: video, 3: docs
	Status int `borm:"status"` // 数据状态，正常、压缩、加密、损坏
	// Normal      0x01, 正常
	// Malformed   0x02, 是否损坏
	// Compressed  0x04, 是否压缩，0: none, 1: snappy, 2: zip 3: zstd
	// Encrypted   0x08, 是否加密

	// PkgID不为0说明是打包数据
	PkgID     int64 `borm:"pkg_id"`  // 打包数据的ID（也是idgen生成的id）
	PkgOffset int64 `borm:"pkg_off"` // 打包数据的偏移位置

	// SnapshotID int64 // 快照版本ID
}

// 数据存储，<=4194304B的对象，用<ID/PkgID>为名称，否则用<ID/PkgID>-<SN>为名称，<SN>为数据块的序号，从0开始递增

type MetaOperator interface {
	Ref(c context.Context, d []*DataInfo) ([]uint64, error)
	PutDataInfo(c context.Context, d []*DataInfo) error
	GetDataInfo(c context.Context, id int64) (*DataInfo, error)

	Create(c context.Context, o []*ObjectInfo) ([]uint64, error)
	Get(c context.Context, id int64) (*ObjectInfo, error)
}

func GetDB(dbName string) (*sql.DB, error) {
	dirPath := filepath.Join(Conf().Path, dbName)
	return sql.Open("sqlite3", dirPath+"?_journal=WAL")
}

func InitBucket(bktName string) error {
	db, err := GetDB(bktName)
	if err != nil {
		return err
	}
	defer db.Close()

	db.Exec(`create table data (id BIGINT PRIMARY KEY NOT NULL,
		size BIGINT NOT NULL,
		checksum UNSIGNED BIG INT NOT NULL,
		hdr_crc32 UNSIGNED BIG INT NOT NULL,
		crc32 UNSIGNED BIG INT NOT NULL,
		md5 UNSIGNED BIG INT NOT NULL,
		kind INT NOT NULL,
		status INT NOT NULL,
		pkg_id BIGINT NOT NULL,
		pkg_off BIGINT NOT NULL
	)`)
	return nil
}

type DefaultMetaOperator struct {
}

func (dmo *DefaultMetaOperator) Ref(c context.Context, d []*DataInfo) ([]uint64, error) {
	return nil, nil
}

func (dmo *DefaultMetaOperator) PutDataInfo(c context.Context, d []*DataInfo) error {
	db, err := GetDB("meta.db")
	defer db.Close()

	t := b.Table(db, DATA_TBL, c)
	_, err = t.ReplaceInto(&d)
	return err
}

func (dmo *DefaultMetaOperator) GetDataInfo(c context.Context, id int64) (d *DataInfo, err error) {
	db, err := GetDB("meta.db")
	defer db.Close()

	t := b.Table(db, DATA_TBL, c)
	d = &DataInfo{}
	_, err = t.Select(d, b.Where(b.Eq("id", id)))
	return
}

func (dmo *DefaultMetaOperator) Create(c context.Context, o []*ObjectInfo) ([]uint64, error) {
	db, err := GetDB("meta.db")
	defer db.Close()

	t := b.Table(db, OBJ_TBL, c)
	_, err = t.ReplaceInto(&o)
	return nil, err
}

func (dmo *DefaultMetaOperator) Get(c context.Context, id int64) (o *ObjectInfo, err error) {
	db, err := GetDB("meta.db")
	defer db.Close()

	t := b.Table(db, OBJ_TBL, c)
	o = &ObjectInfo{}
	_, err = t.Select(o, b.Where(b.Eq("id", id)))
	return
}
