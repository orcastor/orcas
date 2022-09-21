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

const ROOT_OID int64 = 0

type BucketInfo struct {
	ID    int64  `borm:"id"`    // 桶ID
	Name  string `borm:"name"`  // 桶名称
	UID   int64  `borm:"uid"`   // 拥有者
	Type  int    `borm:"type"`  // 桶类型，0: none, 1: normal ...
	Quota int64  `borm:"quota"` // 配额
	Usage int64  `borm:"usage"` // 使用量，统计所有版本的原始大小
	// SnapshotID int64 // 最新快照版本ID
}

// 对象类型
const (
	OBJ_TYPE_MALFORMED = iota - 1
	OBJ_TYPE_NONE
	OBJ_TYPE_DIR
	OBJ_TYPE_FILE
	OBJ_TYPE_VERSION
	OBJ_TYPE_PREVIEW
)

type ObjectInfo struct {
	ID     int64  `borm:"id"`    // 对象ID（idgen随机生成的id）
	PID    int64  `borm:"pid"`   // 父对象ID
	MTime  int64  `borm:"mtime"` // 更新时间，秒级时间戳
	DataID int64  `borm:"did"`   // 数据ID，如果为0，说明没有数据（新创建的文件，DataID就是对象ID，作为对象的首版本数据）
	Type   int    `borm:"type"`  // 对象类型，-1: malformed, 0: none, 1: dir, 2: file, 3: version, 4: preview(thumb/m3u8/pdf)
	Name   string `borm:"name"`  // 对象名称
	Size   int64  `borm:"size"`  // 对象的大小，目录的大小是子对象数，文件的大小是最新版本的字节数
	Ext    string `borm:"ext"`   // 对象的扩展信息
	// BktID int64 // 桶ID，如果支持引用别的桶的数据，为0说明是本桶
}

// 数据状态
const (
	DATA_NORMAL         = uint32(1 << iota) // 正常
	DATA_ENDEC_AES256                       // 是否AES加密
	DATA_ENDEC_SM4                          // 是否SM4加密
	DATA_ENDEC_RESERVED                     // 是否保留的加密
	DATA_CMPR_SNAPPY                        // 是否snappy压缩
	DATA_CMPR_ZSTD                          // 是否zstd压缩
	DATA_CMPR_GZIP                          // 是否gzip压缩
	DATA_CMPR_RESERVED                      // 是否保留的压缩
	DATA_KIND_IMG                           // 图片类型
	DATA_KIND_VIDEO                         // 视频类型
	DATA_KIND_AUDIO                         // 音频类型
	DATA_KIND_ARCHIVE                       // 归档类型
	DATA_KIND_DOCS                          // 文档类型
	DATA_KIND_FONT                          // 字体类型
	DATA_KIND_APP                           // 应用类型
	DATA_KIND_RESERVED                      // 未知类型

	DATA_MALFORMED  = 0 // 是否损坏
	DATA_ENDEC_MASK = DATA_ENDEC_AES256 | DATA_ENDEC_SM4 | DATA_ENDEC_RESERVED
	DATA_CMPR_MASK  = DATA_CMPR_SNAPPY | DATA_CMPR_ZSTD | DATA_CMPR_GZIP | DATA_CMPR_RESERVED
	DATA_KIND_MASK  = DATA_KIND_IMG | DATA_KIND_VIDEO | DATA_KIND_AUDIO | DATA_KIND_ARCHIVE | DATA_KIND_DOCS | DATA_KIND_FONT | DATA_KIND_APP | DATA_KIND_RESERVED
)

type DataInfo struct {
	ID        int64  `borm:"id"`      // 数据ID（idgen随机生成的id）
	Size      int64  `borm:"size"`    // 数据的大小
	OrigSize  int64  `borm:"o_size"`  // 数据的原始大小
	HdrCRC32  uint32 `borm:"h_crc32"` // 头部100KB的CRC32校验值
	CRC32     uint32 `borm:"crc32"`   // 整个数据的CRC32校验值（最原始数据）
	MD5       int64  `borm:"md5"`     // 整个数据的MD5值（最原始数据）
	Cksum     uint32 `borm:"cksum"`   // 整个数据的CRC32校验值（最终数据，用于一致性审计）
	Kind      uint32 `borm:"kind"`    // 数据状态，正常、损坏、加密、压缩、类型（用于预览等）
	PkgID     int64  `borm:"pkg_id"`  // 打包数据的ID（也是idgen生成的id）
	PkgOffset uint32 `borm:"pkg_off"` // 打包数据的偏移位置
	// PkgID不为0说明是打包数据
	// SnapshotID int64 // 快照版本ID
}

const EmptyDataID = 4708888888888

func EmptyDataInfo() *DataInfo {
	return &DataInfo{
		ID:   EmptyDataID,
		MD5:  -1081059644736014743,
		Kind: DATA_NORMAL,
	}
}

type UserInfo struct {
	ID     int64  `borm:"id"`
	Usr    string `borm:"usr"`
	Pwd    string `borm:"pwd"` // PBKDF2-HMAC-SHA256
	Key    string `borm:"key"`
	Role   uint32 `borm:"role"`
	Name   string `borm:"name"`
	Avatar string `borm:"avatar"`
}

const (
	BKT_TBL = "bkt"
	USR_TBL = "usr"
	ACL_TBL = "acl"

	OBJ_TBL  = "obj"
	DATA_TBL = "data"
)

type BucketMetadataAdapter interface {
	PutBkt(c Ctx, o []*BucketInfo) error
	GetBkt(c Ctx, ids []int64) ([]*BucketInfo, error)
	ListBkt(c Ctx, uid int64) ([]*BucketInfo, error)
}

type DataMetadataAdapter interface {
	RefData(c Ctx, bktID int64, d []*DataInfo) ([]int64, error)
	PutData(c Ctx, bktID int64, d []*DataInfo) error
	GetData(c Ctx, bktID, id int64) (*DataInfo, error)
}

type ObjectMetadataAdapter interface {
	PutObj(c Ctx, bktID int64, o []*ObjectInfo) ([]int64, error)
	GetObj(c Ctx, bktID int64, ids []int64) ([]*ObjectInfo, error)
	SetObj(c Ctx, bktID int64, fields []string, o *ObjectInfo) error
	ListObj(c Ctx, bktID, pid int64, wd, delim, order string, count int) ([]*ObjectInfo, int64, string, error)
}

type MetadataAdapter interface {
	Close()

	BucketMetadataAdapter
	DataMetadataAdapter
	ObjectMetadataAdapter
}

func GetDB(bktID ...interface{}) (*sql.DB, error) {
	dirPath := filepath.Join(Conf().Path, fmt.Sprint(bktID...))
	os.MkdirAll(dirPath, 0766)
	return sql.Open("sqlite3", filepath.Join(dirPath, "meta.db")+"?_journal=WAL&cache=shared&mode=rwc&nolock=1")
}

func InitDB() error {
	db, err := GetDB()
	if err != nil {
		return ERR_OPEN_DB
	}
	defer db.Close()

	db.Exec(`CREATE TABLE bkt (id BIGINT PRIMARY KEY NOT NULL,
		uid BIGINT NOT NULL,
		quota BIGINT NOT NULL,
		usage BIGINT NOT NULL,
		type TINYINT NOT NULL,
		name TEXT NOT NULL
	)`)

	db.Exec(`CREATE INDEX ix_uid on bkt (uid)`)
	db.Exec(`CREATE UNIQUE INDEX uk_name on bkt (name)`)
	return nil
}

func InitBucketDB(bktID int64) error {
	db, err := GetDB(bktID)
	if err != nil {
		return ERR_OPEN_DB
	}
	defer db.Close()

	db.Exec(`CREATE TABLE obj (id BIGINT PRIMARY KEY NOT NULL,
		pid BIGINT NOT NULL,
		did BIGINT NOT NULL,
		size BIGINT NOT NULL,
		mtime BIGINT NOT NULL,
		type TINYINT NOT NULL,
		name TEXT NOT NULL,
		ext TEXT NOT NULL
	)`)

	db.Exec(`CREATE TABLE data (id BIGINT PRIMARY KEY NOT NULL,
		size BIGINT NOT NULL,
		o_size BIGINT NOT NULL,
		md5 BIGINT NOT NULL,
		pkg_id BIGINT NOT NULL,
		pkg_off UNSIGNED INT NOT NULL,
		h_crc32 UNSIGNED INT NOT NULL,
		crc32 UNSIGNED INT NOT NULL,
		cksum UNSIGNED INT NOT NULL,
		kind SMALLINT NOT NULL
	)`)

	db.Exec(`CREATE UNIQUE INDEX uk_pid_name on obj (pid, name)`)
	db.Exec(`CREATE INDEX ix_ref ON data (o_size, h_crc32, crc32, md5)`)
	db.Exec(`PRAGMA temp_store = MEMORY`)
	return nil
}

type DefaultMetadataAdapter struct {
}

func (dma *DefaultMetadataAdapter) Close() {
}

func (dma *DefaultMetadataAdapter) PutBkt(c Ctx, o []*BucketInfo) error {
	db, err := GetDB()
	if err != nil {
		return ERR_OPEN_DB
	}
	defer db.Close()

	if _, err = b.Table(db, BKT_TBL, c).ReplaceInto(&o); err != nil {
		return ERR_EXEC_DB
	}
	for _, x := range o {
		InitBucketDB(x.ID)
	}
	return nil
}

func (dma *DefaultMetadataAdapter) GetBkt(c Ctx, ids []int64) (o []*BucketInfo, err error) {
	db, err := GetDB()
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	defer db.Close()

	if _, err = b.Table(db, BKT_TBL, c).Select(&o, b.Where(b.In("id", ids))); err != nil {
		return nil, ERR_QUERY_DB
	}
	return
}

func (dma *DefaultMetadataAdapter) ListBkt(c Ctx, uid int64) (o []*BucketInfo, err error) {
	db, err := GetDB()
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	defer db.Close()

	if _, err = b.Table(db, BKT_TBL, c).Select(&o, b.Where(b.Eq("uid", uid))); err != nil {
		return nil, ERR_QUERY_DB
	}
	return
}

func (dma *DefaultMetadataAdapter) RefData(c Ctx, bktID int64, d []*DataInfo) ([]int64, error) {
	db, err := GetDB(bktID)
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	defer db.Close()

	tbl := fmt.Sprintf("tmp_%x", time.Now().UnixNano())
	// 创建临时表
	db.Exec(`CREATE TEMPORARY TABLE ` + tbl + ` (o_size BIGINT NOT NULL,
		h_crc32 UNSIGNED INT NOT NULL,
		crc32 UNSIGNED INT NOT NULL,
		md5 BIGINT NOT NULL
	)`)
	// 把待查询数据放到临时表
	if _, err = b.Table(db, tbl, c).Insert(&d,
		b.Fields("o_size", "h_crc32", "crc32", "md5")); err != nil {
		return nil, ERR_EXEC_DB
	}
	var refs []struct {
		ID       int64  `borm:"max(a.id)"`
		OrigSize int64  `borm:"b.o_size"`
		HdrCRC32 uint32 `borm:"b.h_crc32"`
		CRC32    uint32 `borm:"b.crc32"`
		MD5      int64  `borm:"b.md5"`
	}
	// 联表查询
	if _, err = b.Table(db, `data a, `+tbl+` b`, c).Select(&refs,
		b.Join(`on a.o_size=b.o_size and a.h_crc32=b.h_crc32 and 
			(b.crc32=0 or b.md5=0 or (a.crc32=b.crc32 and a.md5=b.md5))`),
		b.GroupBy("b.o_size", "b.h_crc32", "b.crc32", "b.md5")); err != nil {
		return nil, ERR_QUERY_DB
	}
	// 删除临时表
	db.Exec(`DROP TABLE ` + tbl)

	// 构造辅助查询map
	aux := make(map[string]int64, 0)
	for _, ref := range refs {
		aux[fmt.Sprintf("%d:%d:%d:%d", ref.OrigSize, ref.HdrCRC32, ref.CRC32, ref.MD5)] = ref.ID
	}

	res := make([]int64, len(d))
	for i, x := range d {
		// 如果最基础的数据不完整，直接跳过
		if x.OrigSize == 0 || x.HdrCRC32 == 0 {
			continue
		}

		key := fmt.Sprintf("%d:%d:%d:%d", x.OrigSize, x.HdrCRC32, x.CRC32, x.MD5)
		if id, ok := aux[key]; ok {
			// 全文件的数据没有，说明是预Ref
			if x.CRC32 == 0 || x.MD5 == 0 {
				if id > 0 {
					res[i] = 1 // 非0代表预Ref成功，预Ref只看数据库
				}
			} else {
				res[i] = id
			}
		} else {
			// 没有秒传成功，但是当前批次可能有一样的数据
			aux[key] = int64(^i)
		}
	}
	return res, nil
}

func (dma *DefaultMetadataAdapter) PutData(c Ctx, bktID int64, d []*DataInfo) error {
	db, err := GetDB(bktID)
	if err != nil {
		return ERR_OPEN_DB
	}
	defer db.Close()

	if _, err = b.Table(db, DATA_TBL, c).ReplaceInto(&d); err != nil {
		return ERR_EXEC_DB
	}
	return nil
}

func (dma *DefaultMetadataAdapter) GetData(c Ctx, bktID, id int64) (d *DataInfo, err error) {
	db, err := GetDB(bktID)
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	defer db.Close()

	d = &DataInfo{}
	if _, err = b.Table(db, DATA_TBL, c).Select(d, b.Where(b.Eq("id", id))); err != nil {
		return nil, ERR_QUERY_DB
	}
	return
}

func (dma *DefaultMetadataAdapter) PutObj(c Ctx, bktID int64, o []*ObjectInfo) (ids []int64, err error) {
	db, err := GetDB(bktID)
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	defer db.Close()

	for _, x := range o {
		ids = append(ids, x.ID)
	}

	var n int
	if n, err = b.Table(db, OBJ_TBL, c).InsertIgnore(&o); err != nil {
		return nil, ERR_EXEC_DB
	}
	if n != len(o) {
		var inserted []int64
		if _, err = b.Table(db, OBJ_TBL, c).Select(&inserted, b.Fields("id"), b.Where(b.In("id", ids))); err != nil {
			return nil, ERR_QUERY_DB
		}
		// 处理有冲突的情况
		m := make(map[int64]struct{}, 0)
		for _, v := range inserted {
			m[v] = struct{}{}
		}
		// 擦除没有插入成功的id
		for i, id := range ids {
			if _, ok := m[id]; !ok {
				ids[i] = 0
			}
		}
	}
	return
}

func (dma *DefaultMetadataAdapter) GetObj(c Ctx, bktID int64, ids []int64) (o []*ObjectInfo, err error) {
	db, err := GetDB(bktID)
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	defer db.Close()

	if _, err = b.Table(db, OBJ_TBL, c).Select(&o, b.Where(b.In("id", ids))); err != nil {
		return nil, ERR_QUERY_DB
	}
	return
}

func (dma *DefaultMetadataAdapter) SetObj(c Ctx, bktID int64, fields []string, o *ObjectInfo) error {
	db, err := GetDB(bktID)
	if err != nil {
		return ERR_OPEN_DB
	}
	defer db.Close()

	if _, err = b.Table(db, OBJ_TBL, c).Update(o, b.Fields(fields...), b.Where(b.Eq("id", o.ID))); err != nil {
		// 如果存在同名文件，会报错：Error: stepping, UNIQUE constraint failed: obj.name (19)
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ERR_DUP_KEY
		}
		return ERR_EXEC_DB
	}
	return nil
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

func doOrder(delim, order string, conds *[]interface{}) (string, string) {
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
			*conds = append(*conds, fn(order, ds[0]))
		} else if len(ds) == 2 {
			*conds = append(*conds, b.Or(fn(order, ds[0]),
				b.And(b.Eq(order, ds[0]), b.Gt("id", ds[1]))))
		}
	}
	return orderBy, order
}

func (dma *DefaultMetadataAdapter) ListObj(c Ctx, bktID, pid int64,
	wd, delim, order string, count int) (o []*ObjectInfo,
	cnt int64, d string, err error) {
	conds := []interface{}{b.Eq("pid", pid)}
	if wd != "" {
		if strings.ContainsAny(wd, "*?") {
			conds = append(conds, b.GLOB("name", wd))
		} else {
			conds = append(conds, b.Eq("name", wd))
		}
	}

	db, err := GetDB(bktID)
	if err != nil {
		return nil, 0, "", ERR_OPEN_DB
	}
	defer db.Close()

	if _, err = b.Table(db, OBJ_TBL, c).Select(&cnt,
		b.Fields("count(1)"),
		b.Where(conds...)); err != nil {
		return nil, 0, "", ERR_QUERY_DB
	}

	if count > 0 {
		var orderBy string
		orderBy, order = doOrder(delim, order, &conds)
		if _, err = b.Table(db, OBJ_TBL, c).Select(&o,
			b.Where(conds...),
			b.OrderBy(orderBy),
			b.Limit(count)); err != nil {
			return nil, 0, "", ERR_QUERY_DB
		}

		if len(o) > 0 {
			d = toDelim(order, o[len(o)-1])
		}
	}
	return
}
