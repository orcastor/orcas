package core

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	b "github.com/orca-zhang/borm"
)

type BucketInfo struct {
	ID   int64  `borm:"id"`       // 桶ID
	Name string `borm:"name"`     // 桶名称
	UID  int64  `borm:"uid"`      // 拥有者
	Type int    `borm:"type"`     // 桶类型，0: none, 1: normal ...
	OID  int64  `borm:"root_oid"` // 桶的根对象ID
	// SnapshotID int64 // 最新快照版本ID
}

// 对象状态
const (
	OBJ_NONE = iota
	OBJ_NORMAL
	OBJ_DELETED
	OBJ_RECYCLED
	OBJ_MALFORMED
)

// 对象类型
const (
	OBJ_TYPE_NA = iota
	OBJ_TYPE_DIR
	OBJ_TYPE_FILE
	OBJ_TYPE_VERSION
	OBJ_TYPE_PREVIEW
)

type ObjectInfo struct {
	ID     int64 `borm:"id"`    // 对象ID（idgen随机生成的id）
	PID    int64 `borm:"pid"`   // 父对象ID
	MTime  int64 `borm:"mtime"` // 更新时间，秒级时间戳
	DataID int64 `borm:"did"`   // 数据ID，如果为0，说明没有数据（新创建的文件，DataID就是对象ID，作为对象的首版本数据）
	// BktID int64 // 桶ID，如果支持引用别的桶的数据，为0说明是本桶
	Type   int    `borm:"type"`   // 对象类型，0: none, 1: dir, 2: file, 3: version, 4: preview(thumb/m3u8/pdf)
	Status int    `borm:"status"` // 对象状态，0: none, 1: normal, 1: deleted, 2: recycle(to be deleted), 3: malformed
	Name   string `borm:"name"`   // 对象名称
	Size   int64  `borm:"size"`   // 对象的大小，目录的大小是子对象数，文件的大小是最新版本的字节数
	Ext    string `borm:"ext"`
}

// 数据状态
const (
	DATA_NORMAL    = 1 >> iota // 正常
	DATA_MALFORMED             // 是否损坏
	DATA_CMPRED                // 是否压缩 // 0: none, 1: snappy, 2: zip 3: zstd
	DATA_ENCRYPTED             // 是否加密
)

// 数据类型
const (
	DATA_KIND_ETC = iota
	DATA_KIND_IMG
	DATA_KIND_VIDEO
	DATA_KIND_DOCS
)

type DataInfo struct {
	ID       int64  `borm:"id"`        // 数据ID（对象ID/版本ID，idgen随机生成的id）
	Size     int64  `borm:"size"`      // 数据的大小
	HdrCRC32 uint64 `borm:"hdr_crc32"` // 头部100KB的CRC32校验值
	CRC32    uint64 `borm:"crc32"`     // 整个对象的CRC32校验值（最原始数据）
	MD5      uint64 `borm:"md5"`       // 整个对象的MD5值（最原始数据）

	Checksum uint64 `borm:"checksum"` // 整个对象的MD5值（最终数据，用于一致性审计）
	// MIME       string // 数据的多媒体类型
	Kind   int `borm:"kind"`   // 数据类型（用于预览等），0: etc, 1: image, 2: video, 3: docs
	Status int `borm:"status"` // 数据状态，正常、压缩、加密、损坏

	// PkgID不为0说明是打包数据
	PkgID     int64 `borm:"pkg_id"`  // 打包数据的ID（也是idgen生成的id）
	PkgOffset int64 `borm:"pkg_off"` // 打包数据的偏移位置

	// SnapshotID int64 // 快照版本ID
}

// 数据存储，<=4194304B的对象，用<ID/PkgID>为名称，否则用<ID/PkgID>-<SN>为名称，<SN>为数据块的序号，从0开始递增

type BucketMetaOperator interface {
	PutBkt(c Ctx, o []*BucketInfo) error
	GetBkt(c Ctx, ids []int64) ([]*BucketInfo, error)
	ListBkt(c Ctx, uid int64) ([]*BucketInfo, error)
}

type DataMetaOperator interface {
	RefData(c Ctx, bktID int64, d []*DataInfo) ([]int64, error)
	PutData(c Ctx, bktID int64, d []*DataInfo) error
	GetData(c Ctx, bktID, id int64) (*DataInfo, error)
}

type ObjectMetaOperator interface {
	PutObj(c Ctx, bktID int64, o []*ObjectInfo) ([]int64, error)
	GetObj(c Ctx, bktID int64, ids []int64) ([]*ObjectInfo, error)
	SetObj(c Ctx, bktID int64, fields []string, o *ObjectInfo) error
	ListObj(c Ctx, bktID, pid int64, wd, delim, order string, size, status int) ([]*ObjectInfo, int64, string, error)
}

type MetaOperator interface {
	BucketMetaOperator
	DataMetaOperator
	ObjectMetaOperator
}

func GetDB(bktID ...interface{}) (*sql.DB, error) {
	dirPath := filepath.Join(Conf().Path, fmt.Sprint(bktID...))
	os.MkdirAll(dirPath, 0766)
	return sql.Open("sqlite3", filepath.Join(dirPath, "meta.db")+"?_journal=WAL")
}

func InitDB() error {
	db, err := GetDB()
	if err != nil {
		return err
	}
	defer db.Close()

	db.Exec(`CREATE TABLE bkt (id BIGINT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL,
		uid BIGINT NOT NULL,
		type INT NOT NULL,
		root_oid BIGINT NOT NULL
	)`)

	db.Exec(`CREATE INDEX ix_uid on bkt (uid)`)
	return nil
}

func InitBucketDB(bktID int64) error {
	db, err := GetDB(bktID)
	if err != nil {
		return err
	}
	defer db.Close()

	db.Exec(`CREATE TABLE obj (id BIGINT PRIMARY KEY NOT NULL,
		pid BIGINT NOT NULL,
		mtime BIGINT NOT NULL,
		did BIGINT NOT NULL,
		type INT NOT NULL,
		status INT NOT NULL,
		name TEXT NOT NULL,
		size BIGINT NOT NULL,
		ext TEXT NOT NULL
	)`)

	db.Exec(`CREATE UNIQUE INDEX uk_pid_name on obj (pid, name)`)

	db.Exec(`CREATE TABLE data (id BIGINT PRIMARY KEY NOT NULL,
		size BIGINT NOT NULL,
		hdr_crc32 UNSIGNED BIG INT NOT NULL,
		crc32 UNSIGNED BIG INT NOT NULL,
		md5 UNSIGNED BIG INT NOT NULL,
		checksum UNSIGNED BIG INT NOT NULL,
		kind INT NOT NULL,
		status INT NOT NULL,
		pkg_id BIGINT NOT NULL,
		pkg_off BIGINT NOT NULL
	)`)

	db.Exec(`CREATE INDEX ix_ref ON data (size, hdr_crc32, crc32, md5)`)
	db.Exec(`PRAGMA temp_store = MEMORY`)
	return nil
}

type DefaultMetaOperator struct {
	acm AccessCtrlMgr
}

func NewDefaultMetaOperator(acm AccessCtrlMgr) MetaOperator {
	return &DefaultMetaOperator{
		acm: acm,
	}
}

func (dmo *DefaultMetaOperator) PutBkt(c Ctx, o []*BucketInfo) error {
	if err := dmo.acm.CheckRole(c, ADMIN); err != nil {
		return err
	}

	db, err := GetDB()
	defer db.Close()

	_, err = b.Table(db, BKT_TBL, c).ReplaceInto(&o)
	for _, x := range o {
		InitBucketDB(x.ID)
	}
	return err
}

func (dmo *DefaultMetaOperator) GetBkt(c Ctx, ids []int64) (o []*BucketInfo, err error) {
	if err := dmo.acm.CheckRole(c, ADMIN); err != nil {
		return nil, err
	}

	db, err := GetDB()
	defer db.Close()

	_, err = b.Table(db, BKT_TBL, c).Select(&o, b.Where(b.In("id", ids)))
	return
}

func (dmo *DefaultMetaOperator) ListBkt(c Ctx, uid int64) (o []*BucketInfo, err error) {
	if err := dmo.acm.CheckPermission(c, R, -1); err != nil {
		return nil, err
	}

	db, err := GetDB()
	defer db.Close()

	_, err = b.Table(db, BKT_TBL, c).Select(&o, b.Where(b.Eq("uid", uid)))
	return
}

func (dmo *DefaultMetaOperator) RefData(c Ctx, bktID int64, d []*DataInfo) ([]int64, error) {
	if err := dmo.acm.CheckPermission(c, RW, bktID); err != nil {
		return nil, err
	}

	db, err := GetDB(bktID)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tbl := fmt.Sprintf("tmp_%x", time.Now().UnixNano())
	// 创建临时表
	db.Exec(`CREATE TEMPORARY TABLE ` + tbl + ` (size BIGINT NOT NULL,
		hdr_crc32 UNSIGNED BIG INT NOT NULL,
		crc32 UNSIGNED BIG INT NOT NULL,
		md5 UNSIGNED BIG INT NOT NULL
	)`)
	// 把待查询数据放到临时表
	_, err = b.Table(db, tbl, c).Insert(&d, b.Fields("size", "hdr_crc32", "crc32", "md5"))
	var refs []struct {
		ID       int64  `borm:"max(a.id)"`
		Size     int64  `borm:"b.size"`
		HdrCRC32 uint64 `borm:"b.hdr_crc32"`
		CRC32    uint64 `borm:"b.crc32"`
		MD5      uint64 `borm:"b.md5"`
	}
	// 联表查询
	_, err = b.Table(db, `data a, `+tbl+` b on a.size=b.size 
	    and a.hdr_crc32=b.hdr_crc32 and (b.crc32=0 or b.md5=0 or 
		(a.crc32=b.crc32 and a.md5=b.md5))`, c).Select(&refs,
		b.GroupBy("b.size", "b.hdr_crc32", "b.crc32", "b.md5"))
	// 删除临时表
	db.Exec(`DROP TABLE ` + tbl)

	// 构造辅助查询map
	aux := make(map[string]int64, 0)
	for _, ref := range refs {
		aux[fmt.Sprintf("%d:%d:%d:%d", ref.Size, ref.HdrCRC32, ref.CRC32, ref.MD5)] = ref.ID
	}

	res := make([]int64, len(d))
	for i, x := range d {
		// 如果最基础的数据不完整，直接跳过
		if x.Size == 0 || x.HdrCRC32 == 0 {
			continue
		}

		if id, ok := aux[fmt.Sprintf("%d:%d:%d:%d", x.Size, x.HdrCRC32, x.CRC32, x.MD5)]; ok {
			// 全文件的数据没有，说明是预Ref
			if x.CRC32 == 0 || x.MD5 == 0 {
				res[i] = 1 // 非0代表预Ref成功
			} else {
				res[i] = id
			}
		}
	}
	return res, err
}

func (dmo *DefaultMetaOperator) PutData(c Ctx, bktID int64, d []*DataInfo) error {
	db, err := GetDB(bktID)
	defer db.Close()

	_, err = b.Table(db, DATA_TBL, c).ReplaceInto(&d)
	return err
}

func (dmo *DefaultMetaOperator) GetData(c Ctx, bktID, id int64) (d *DataInfo, err error) {
	if err := dmo.acm.CheckPermission(c, R, bktID); err != nil {
		return nil, err
	}

	db, err := GetDB(bktID)
	defer db.Close()

	d = &DataInfo{}
	_, err = b.Table(db, DATA_TBL, c).Select(d, b.Where(b.Eq("id", id)))
	return
}

func (dmo *DefaultMetaOperator) PutObj(c Ctx, bktID int64, o []*ObjectInfo) (ids []int64, err error) {
	if err := dmo.acm.CheckPermission(c, W, bktID); err != nil {
		return nil, err
	}

	db, err := GetDB(bktID)
	defer db.Close()

	n, err := b.Table(db, OBJ_TBL, c).Insert(&o)
	if n == len(o) {
		for _, x := range o {
			ids = append(ids, x.ID)
		}
	} else {
		// TODO: 处理有冲突的情况
	}
	return ids, err
}

func (dmo *DefaultMetaOperator) GetObj(c Ctx, bktID int64, ids []int64) (o []*ObjectInfo, err error) {
	if err := dmo.acm.CheckPermission(c, R, bktID); err != nil {
		return nil, err
	}

	db, err := GetDB(bktID)
	defer db.Close()

	_, err = b.Table(db, OBJ_TBL, c).Select(&o, b.Where(b.In("id", ids)))
	return
}

func (dmo *DefaultMetaOperator) SetObj(c Ctx, bktID int64, fields []string, o *ObjectInfo) error {
	if err := dmo.acm.CheckPermission(c, W, bktID); err != nil {
		return err
	}

	db, err := GetDB(bktID)
	defer db.Close()

	_, err = b.Table(db, OBJ_TBL, c).Update(o, b.Fields(fields...), b.Where(b.Eq("id", o.ID)))
	return err
}

func toDelim(field string, o *ObjectInfo) string {
	var d interface{}
	switch field {
	case "id":
		return fmt.Sprint(o.ID)
	case "name":
		return fmt.Sprint(o.Name)
	case "mtime":
		d = o.MTime
	case "size":
		d = o.Size
	case "type":
		d = o.Type
	}
	return fmt.Sprintf("%v:%d", d, o.ID)
}

func (dmo *DefaultMetaOperator) ListObj(c Ctx, bktID, pid int64,
	wd, delim, order string, size, status int) (o []*ObjectInfo,
	cnt int64, d string, err error) {
	if err := dmo.acm.CheckPermission(c, R, bktID); err != nil {
		return nil, 0, "", err
	}

	conds := []interface{}{b.Eq("pid", pid)}
	if wd != "" {
		if strings.Index(wd, "%") >= 0 {
			conds = append(conds, b.Like("name", wd))
		} else {
			conds = append(conds, b.Eq("name", wd))
		}
	}

	if status != 0 {
		conds = append(conds, b.Eq("status", status))
	}

	db, err := GetDB(bktID)
	defer db.Close()

	if _, err = b.Table(db, OBJ_TBL, c).Select(&cnt,
		b.Fields("count(1)"),
		b.Where(conds...)); err != nil {
		return nil, 0, "", err
	}

	// 处理order
	if order == "" {
		order = "id"
	}
	fn := b.Gt
	orderBy := order
	switch order[0] {
	case '-':
		fn = b.Lt
		order = order[1:]
		orderBy = order + " desc"
	case '+':
		order = order[1:]
		orderBy = order
	}
	if order != "id" && order != "name" {
		orderBy = orderBy + ", id"
	}

	// 处理边界条件
	ds := strings.Split(delim, ":")
	if len(ds) > 0 && ds[0] != "" {
		if order == "id" || order == "name" {
			conds = append(conds, fn(order, ds[0]))
		} else if len(ds) == 2 {
			conds = append(conds, b.Or(fn(order, ds[0]),
				b.And(b.Eq(order, ds[0]), b.Gt("id", ds[1]))))
		}
	}

	_, err = b.Table(db, OBJ_TBL, c).Select(&o,
		b.Where(conds...),
		b.OrderBy(orderBy),
		b.Limit(size))

	if len(o) > 0 {
		d = toDelim(order, o[len(o)-1])
	}
	return
}
