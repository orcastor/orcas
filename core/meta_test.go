package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

var bktID = int64(0)
var c = context.TODO()

func init() {
	// Initialize test environment with in-memory filesystem
	InitTestEnv()
	bktID, _ = idgen.NewIDGen(nil, 0).New()
}

func TestListBkt(t *testing.T) {
	Convey("normal", t, func() {
		Convey("put bkt", func() {
			InitDB()
			dma := &DefaultMetadataAdapter{}
			id1, _ := idgen.NewIDGen(nil, 0).New()
			id2, _ := idgen.NewIDGen(nil, 0).New()
			uid, _ := idgen.NewIDGen(nil, 0).New()
			b1 := &BucketInfo{
				ID:   id1,
				Name: "zhangwei",
				UID:  uid,
				Type: 1,
			}
			b2 := &BucketInfo{
				ID:   id2,
				Name: "zhangwei2",
				UID:  uid,
				Type: 1,
			}
			So(dma.PutBkt(c, []*BucketInfo{b1, b2}), ShouldBeNil)

			bs, err := dma.ListBkt(c, uid)
			So(err, ShouldBeNil)
			So(len(bs), ShouldEqual, 2)
			So(bs[0], ShouldResemble, b1)
			So(bs[1], ShouldResemble, b2)
		})
	})
}

func TestRefData(t *testing.T) {
	Convey("normal", t, func() {
		dma := &DefaultMetadataAdapter{}
		InitBucketDB(context.TODO(), bktID)

		id, _ := idgen.NewIDGen(nil, 0).New()
		So(dma.PutData(c, bktID, []*DataInfo{{
			ID:       id,
			OrigSize: 1,
			HdrXXH3:  222,
			XXH3:     333,
			SHA256_0: -2039914840885289964,
			SHA256_1: -7278955230309402332,
			SHA256_2: 2859295262623109964,
			SHA256_3: -6587190536697628587,
			Kind:     DATA_NORMAL,
		}}), ShouldBeNil)

		Convey("single try ref", func() {
			ids, err := dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, 0)

			ids, err = dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 0,
				HdrXXH3:  222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)

			ids, err = dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  0,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)

			ids, err = dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  0,
				XXH3:     333,
				SHA256_0: -2039914840885289964,
				SHA256_1: -7278955230309402332,
				SHA256_2: 2859295262623109964,
				SHA256_3: -6587190536697628587,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)
		})
		Convey("multiple try ref", func() {
			ids, err := dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  222,
			}, {
				OrigSize: 1,
				HdrXXH3:  222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldNotEqual, 0)
		})
		Convey("multiple try ref diff", func() {
			ids, err := dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  222,
			}, {
				OrigSize: 1,
				HdrXXH3:  111,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldEqual, 0)
		})

		Convey("single ref", func() {
			ids, err := dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  222,
				XXH3:     333,
				SHA256_0: -2039914840885289964,
				SHA256_1: -7278955230309402332,
				SHA256_2: 2859295262623109964,
				SHA256_3: -6587190536697628587,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, id)

			ids, err = dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  222,
				XXH3:     0,
				SHA256_0: -2039914840885289964,
				SHA256_1: -7278955230309402332,
				SHA256_2: 2859295262623109964,
				SHA256_3: -6587190536697628587,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, id)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[0], ShouldEqual, 1)

			ids, err = dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  222,
				XXH3:     333,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, id)
			So(ids[0], ShouldEqual, 1)
		})

		Convey("multiple ref", func() {
			ids, err := dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  222,
				XXH3:     333,
				SHA256_0: -2039914840885289964,
				SHA256_1: -7278955230309402332,
				SHA256_2: 2859295262623109964,
				SHA256_3: -6587190536697628587,
			}, {
				OrigSize: 1,
				HdrXXH3:  222,
				XXH3:     333,
				SHA256_0: -2039914840885289964,
				SHA256_1: -7278955230309402332,
				SHA256_2: 2859295262623109964,
				SHA256_3: -6587190536697628587,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldNotEqual, 0)
		})
		Convey("multiple ref diff", func() {
			ids, err := dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrXXH3:  222,
				XXH3:     333,
				SHA256_0: -2039914840885289964,
				SHA256_1: -7278955230309402332,
				SHA256_2: 2859295262623109964,
				SHA256_3: -6587190536697628587,
			}, {
				OrigSize: 1,
				HdrXXH3:  111,
				XXH3:     333,
				SHA256_0: -2039914840885289964,
				SHA256_1: -7278955230309402332,
				SHA256_2: 2859295262623109964,
				SHA256_3: -6587190536697628587,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldEqual, id)
			So(ids[1], ShouldEqual, 0)
		})

		Convey("multiple ref same but do not exist", func() {
			// Use different data that doesn't exist in database
			ids, err := dma.RefData(c, bktID, []*DataInfo{{
				OrigSize: 999,
				HdrXXH3: 888,
				XXH3:     777,
				SHA256_0: 11111,
				SHA256_1: 22222,
				SHA256_2: 33333,
				SHA256_3: 44444,
			}, {
				OrigSize: 999,
				HdrXXH3: 888,
				XXH3:     777,
				SHA256_0: 11111,
				SHA256_1: 22222,
				SHA256_2: 33333,
				SHA256_3: 44444,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldEqual, 0)
			So(ids[1], ShouldEqual, ^0)
		})
	})
}

func TestGetData(t *testing.T) {
	Convey("normal", t, func() {
		Convey("get data info", func() {
			InitBucketDB(context.TODO(), bktID)
			dma := &DefaultMetadataAdapter{}
			id, _ := idgen.NewIDGen(nil, 0).New()
			d := &DataInfo{
				ID:   id,
				Size: 1,
				Kind: DATA_NORMAL,
			}
			So(dma.PutData(c, bktID, []*DataInfo{d}), ShouldBeNil)

			d1, err := dma.GetData(c, bktID, id)
			So(err, ShouldBeNil)
			So(d1, ShouldResemble, d)
		})
	})
}

func TestPutObj(t *testing.T) {
	Convey("normal", t, func() {
		Convey("put same name obj", func() {
			InitBucketDB(context.TODO(), bktID)

			dma := &DefaultMetadataAdapter{}
			ig := idgen.NewIDGen(nil, 0)
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()
			d := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_DIR,
				Name:   "test",
				Size:   1,
				Extra:  "{}",
			}
			ids, err := dma.PutObj(c, bktID, []*ObjectInfo{d})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, id)

			id1, _ := ig.New()
			d1 := &ObjectInfo{
				ID:     id1,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_DIR,
				Name:   "test",
				Size:   1,
				Extra:  "{}",
			}
			// push same name diff id obj
			ids, err = dma.PutObj(c, bktID, []*ObjectInfo{d1, d})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldEqual, 0)
			// same name same id, like idempotent
			So(ids[1], ShouldEqual, id)
		})
	})
}

func TestGetObj(t *testing.T) {
	Convey("normal", t, func() {
		Convey("get obj info", func() {
			InitBucketDB(context.TODO(), bktID)

			dma := &DefaultMetadataAdapter{}
			ig := idgen.NewIDGen(nil, 0)
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()
			d := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_DIR,
				Name:   "test",
				Size:   1,
				Extra:  "{}",
			}
			ids, err := dma.PutObj(c, bktID, []*ObjectInfo{d})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, 0)

			d1, err := dma.GetObj(c, bktID, ids)
			So(err, ShouldBeNil)
			So(len(d1), ShouldEqual, 1)
			So(d1[0], ShouldResemble, d)
		})
	})
}

func TestSetObj(t *testing.T) {
	Convey("normal", t, func() {
		InitBucketDB(context.TODO(), bktID)

		dma := &DefaultMetadataAdapter{}
		ig := idgen.NewIDGen(nil, 0)
		id, _ := ig.New()
		pid, _ := ig.New()
		did, _ := ig.New()
		d := &ObjectInfo{
			ID:     id,
			PID:    pid,
			MTime:  Now(),
			DataID: did,
			Type:   OBJ_TYPE_DIR,
			Name:   "test",
			Size:   1,
			Extra:  "{}",
		}
		id1, _ := ig.New()
		d1 := &ObjectInfo{
			ID:     id1,
			PID:    pid,
			MTime:  Now(),
			DataID: did,
			Type:   OBJ_TYPE_DIR,
			Name:   "test2",
			Size:   1,
			Extra:  "{}",
		}
		ids, err := dma.PutObj(c, bktID, []*ObjectInfo{d, d1})
		So(err, ShouldBeNil)
		So(len(ids), ShouldEqual, 2)
		So(ids[0], ShouldNotEqual, 0)

		o, err := dma.GetObj(c, bktID, ids)
		So(err, ShouldBeNil)
		So(len(o), ShouldEqual, 2)
		So(o[0], ShouldResemble, d)
		So(o[1], ShouldResemble, d1)
		Convey("set obj name", func() {
			d.Name = "test1"
			dma.SetObj(c, bktID, []string{"name"}, &ObjectInfo{ID: id, Name: d.Name})
			o, err = dma.GetObj(c, bktID, ids)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(o[0], ShouldResemble, d)
			So(o[1], ShouldResemble, d1)
		})

		Convey("same obj name", func() {
			d.Name = "test2"
			err := dma.SetObj(c, bktID, []string{"name"}, &ObjectInfo{ID: id, Name: d.Name})
			So(err, ShouldEqual, ERR_DUP_KEY)
		})
	})
}

func TestListObj(t *testing.T) {
	Convey("normal", t, func() {
		// Use unique bktID for this test
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		// Clean up before test
		CleanTestDB(testBktID)
		CleanTestBucketData(testBktID)
		InitDB() // Initialize main database first
		InitBucketDB(c, testBktID)

		dma := &DefaultMetadataAdapter{}
		pid, _ := ig.New()

		id1, _ := ig.New()
		id2, _ := ig.New()
		id3, _ := ig.New()
		id4, _ := ig.New()
		id5, _ := ig.New()

		did1, _ := ig.New()
		did2, _ := ig.New()
		did3, _ := ig.New()
		did4, _ := ig.New()
		did5, _ := ig.New()

		now := time.Now().Unix()
		d1 := &ObjectInfo{
			ID:     id1,
			PID:    pid,
			MTime:  now + 1,
			DataID: did1,
			Type:   OBJ_TYPE_DIR,
			Name:   "test1",
			Size:   0,
			Extra:  "{}",
		}
		d2 := &ObjectInfo{
			ID:     id2,
			PID:    pid,
			MTime:  now + 2,
			DataID: did2,
			Type:   OBJ_TYPE_FILE,
			Name:   "test2",
			Size:   2,
			Extra:  "{}",
		}
		d3 := &ObjectInfo{
			ID:     id3,
			PID:    pid,
			MTime:  now + 3,
			DataID: did3,
			Type:   OBJ_TYPE_VERSION,
			Name:   "test3",
			Size:   3,
			Extra:  "{}",
		}
		d4 := &ObjectInfo{
			ID:     id4,
			PID:    pid,
			MTime:  now + 3,
			DataID: did4,
			Type:   OBJ_TYPE_PREVIEW,
			Name:   "test4",
			Size:   3,
			Extra:  "{}",
		}
		d5 := &ObjectInfo{
			ID:     id5,
			PID:    pid,
			MTime:  now + 4,
			DataID: did5,
			Type:   OBJ_TYPE_PREVIEW,
			Name:   "test5",
			Size:   4,
			Extra:  "{}",
		}
		ids, err := dma.PutObj(c, testBktID, []*ObjectInfo{d1, d2, d3, d4, d5})
		So(err, ShouldBeNil)
		So(len(ids), ShouldEqual, 5)

		Convey("list obj pagination", func() {
			o, cnt, d, err := dma.ListObj(c, testBktID, pid, "", "", "", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id2))

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", d, "", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id4))

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", d, "", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id5))

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", d, "", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 0)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, "")
		})

		Convey("word", func() {
			o, cnt, d, err := dma.ListObj(c, testBktID, pid, "xxx", "", "", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 0)
			So(cnt, ShouldEqual, 0)
			So(d, ShouldEqual, "")

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "test1", "", "", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 1)
			So(d, ShouldEqual, fmt.Sprint(id1))

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "?es*", "", "", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id2))
		})

		Convey("order", func() {
			o, cnt, d, err := dma.ListObj(c, testBktID, pid, "", "", "+id", 5)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 5)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id5))
			So(o, ShouldResemble, []*ObjectInfo{d1, d2, d3, d4, d5})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", "", "-id", 5)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 5)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id1))
			So(o, ShouldResemble, []*ObjectInfo{d5, d4, d3, d2, d1})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", "", "-name", 5)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 5)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, "test1")
			So(o, ShouldResemble, []*ObjectInfo{d5, d4, d3, d2, d1})

			// 比较非id或者name的时候，相同值的排序是否稳定
			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", "", "+mtime", 3)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 3)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", now+3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d1, d2, d3})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", "", "-mtime", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", now+3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d5, d3})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", d, "-mtime", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", now+2, id2))
			So(o, ShouldResemble, []*ObjectInfo{d4, d2})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", "", "+size", 3)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 3)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", 3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d1, d2, d3})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", "", "-size", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", 3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d5, d3})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", d, "-size", 2)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", 2, id2))
			So(o, ShouldResemble, []*ObjectInfo{d4, d2})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", "", "+type", 3)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 3)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", 3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d1, d2, d3})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", "", "-type", 1)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", OBJ_TYPE_PREVIEW, id4))
			So(o, ShouldResemble, []*ObjectInfo{d4})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", d, "-type", 1)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", OBJ_TYPE_PREVIEW, id5))
			So(o, ShouldResemble, []*ObjectInfo{d5})
		})

		Convey("list obj with 0 count", func() {
			o, cnt, d, err := dma.ListObj(c, testBktID, pid, "", "", "", 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 0)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldBeEmpty)
		})
	})
}

// TestListObjsByType tests ListObjsByType with pagination
func TestListObjsByType(t *testing.T) {
	Convey("ListObjsByType with pagination", t, func() {
		// Use unique bktID for this test
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		// Clean up before test
		CleanTestDB(testBktID)
		CleanTestBucketData(testBktID)
		InitDB() // Initialize main database first
		InitBucketDB(context.TODO(), testBktID)
		dma := &DefaultMetadataAdapter{}
		pid, _ := ig.New()

		// Create multiple files
		var fileIDs []int64
		for i := 0; i < 15; i++ {
			id, _ := ig.New()
			did, _ := ig.New()
			fileIDs = append(fileIDs, id)
			d := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   fmt.Sprintf("file%d", i),
				Size:   int64(i),
				Extra:  "{}",
			}
			_, err := dma.PutObj(c, testBktID, []*ObjectInfo{d})
			So(err, ShouldBeNil)
		}

		Convey("list all files without pagination", func() {
			objs, total, err := dma.ListObjsByType(c, testBktID, OBJ_TYPE_FILE, 0, 0)
			So(err, ShouldBeNil)
			So(total, ShouldEqual, 15)
			So(len(objs), ShouldEqual, 0) // limit=0 returns empty
		})

		Convey("list files with pagination", func() {
			// First page
			objs, total, err := dma.ListObjsByType(c, testBktID, OBJ_TYPE_FILE, 0, 5)
			So(err, ShouldBeNil)
			So(total, ShouldEqual, 15)
			So(len(objs), ShouldEqual, 5)

			// Second page
			objs2, total2, err := dma.ListObjsByType(c, testBktID, OBJ_TYPE_FILE, 5, 5)
			So(err, ShouldBeNil)
			So(total2, ShouldEqual, 15)
			So(len(objs2), ShouldEqual, 5)
			So(objs2[0].ID, ShouldNotEqual, objs[0].ID) // Different pages

			// Third page
			objs3, total3, err := dma.ListObjsByType(c, testBktID, OBJ_TYPE_FILE, 10, 5)
			So(err, ShouldBeNil)
			So(total3, ShouldEqual, 15)
			So(len(objs3), ShouldEqual, 5)

			// Last page (partial)
			objs4, total4, err := dma.ListObjsByType(c, testBktID, OBJ_TYPE_FILE, 15, 5)
			So(err, ShouldBeNil)
			So(total4, ShouldEqual, 15)
			So(len(objs4), ShouldEqual, 0) // No more items
		})

		Convey("list directories", func() {
			// Create some directories
			dirID1, _ := ig.New()
			dirID2, _ := ig.New()
			d1 := &ObjectInfo{
				ID:     dirID1,
				PID:    pid,
				MTime:  Now(),
				DataID: 0,
				Type:   OBJ_TYPE_DIR,
				Name:   "dir1",
				Size:   0,
				Extra:  "{}",
			}
			d2 := &ObjectInfo{
				ID:     dirID2,
				PID:    pid,
				MTime:  Now(),
				DataID: 0,
				Type:   OBJ_TYPE_DIR,
				Name:   "dir2",
				Size:   0,
				Extra:  "{}",
			}
			_, err := dma.PutObj(c, testBktID, []*ObjectInfo{d1, d2})
			So(err, ShouldBeNil)

			objs, total, err := dma.ListObjsByType(c, testBktID, OBJ_TYPE_DIR, 0, 10)
			So(err, ShouldBeNil)
			So(total, ShouldEqual, 2)
			So(len(objs), ShouldEqual, 2)
		})
	})
}

// TestListChildren tests ListChildren with pagination
func TestListChildren(t *testing.T) {
	Convey("ListChildren with pagination", t, func() {
		// Use unique bktID for this test
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		// Clean up before test
		CleanTestDB(testBktID)
		CleanTestBucketData(testBktID)
		InitDB() // Initialize main database first
		InitBucketDB(context.TODO(), testBktID)
		dma := &DefaultMetadataAdapter{}
		parentID, _ := ig.New()

		// Create parent directory
		parent := &ObjectInfo{
			ID:     parentID,
			PID:    0,
			MTime:  Now(),
			DataID: 0,
			Type:   OBJ_TYPE_DIR,
			Name:   "parent",
			Size:   0,
			Extra:  "{}",
		}
		_, err := dma.PutObj(c, testBktID, []*ObjectInfo{parent})
		So(err, ShouldBeNil)

		// Create multiple children
		var childIDs []int64
		for i := 0; i < 12; i++ {
			id, _ := ig.New()
			did, _ := ig.New()
			childIDs = append(childIDs, id)
			child := &ObjectInfo{
				ID:     id,
				PID:    parentID,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   fmt.Sprintf("child%d", i),
				Size:   int64(i * 100),
				Extra:  "{}",
			}
			_, err := dma.PutObj(c, testBktID, []*ObjectInfo{child})
			So(err, ShouldBeNil)
		}

		Convey("list all children without pagination", func() {
			children, total, err := dma.ListChildren(c, testBktID, parentID, 0, 0)
			So(err, ShouldBeNil)
			So(total, ShouldEqual, 12)
			So(len(children), ShouldEqual, 0) // limit=0 returns empty
		})

		Convey("list children with pagination", func() {
			// First page
			children1, total1, err := dma.ListChildren(c, testBktID, parentID, 0, 5)
			So(err, ShouldBeNil)
			So(total1, ShouldEqual, 12)
			So(len(children1), ShouldEqual, 5)

			// Second page
			children2, total2, err := dma.ListChildren(c, testBktID, parentID, 5, 5)
			So(err, ShouldBeNil)
			So(total2, ShouldEqual, 12)
			So(len(children2), ShouldEqual, 5)
			So(children2[0].ID, ShouldNotEqual, children1[0].ID) // Different pages

			// Third page (partial)
			children3, total3, err := dma.ListChildren(c, testBktID, parentID, 10, 5)
			So(err, ShouldBeNil)
			So(total3, ShouldEqual, 12)
			So(len(children3), ShouldEqual, 2) // Only 2 remaining

			// Beyond end
			children4, total4, err := dma.ListChildren(c, testBktID, parentID, 15, 5)
			So(err, ShouldBeNil)
			So(total4, ShouldEqual, 12)
			So(len(children4), ShouldEqual, 0) // No more items
		})

		Convey("list children of non-existent parent", func() {
			nonExistentID, _ := ig.New()
			children, total, err := dma.ListChildren(c, testBktID, nonExistentID, 0, 10)
			So(err, ShouldBeNil)
			So(total, ShouldEqual, 0)
			So(len(children), ShouldEqual, 0)
		})
	})
}

// TestListVersions tests ListVersions with excludeWriting parameter
func TestListVersions(t *testing.T) {
	Convey("ListVersions with excludeWriting", t, func() {
		InitBucketDB(context.TODO(), bktID)
		dma := &DefaultMetadataAdapter{}
		ig := idgen.NewIDGen(nil, 0)
		fileID, _ := ig.New()

		// Create file
		file := &ObjectInfo{
			ID:     fileID,
			PID:    0,
			MTime:  Now(),
			DataID: 0,
			Type:   OBJ_TYPE_FILE,
			Name:   "testfile",
			Size:   100,
			Extra:  "{}",
		}
		_, err := dma.PutObj(c, bktID, []*ObjectInfo{file})
		So(err, ShouldBeNil)

		// Create writing version
		writingVersionID, _ := ig.New()
		writingDid, _ := ig.New()
		writingVersion := &ObjectInfo{
			ID:     writingVersionID,
			PID:    fileID,
			MTime:  Now(),
			DataID: writingDid,
			Type:   OBJ_TYPE_VERSION,
			Name:   WritingVersionName,
			Size:   100,
			Extra:  "{}",
		}
		_, err = dma.PutObj(c, bktID, []*ObjectInfo{writingVersion})
		So(err, ShouldBeNil)

		// Create regular versions
		var versionIDs []int64
		for i := 0; i < 3; i++ {
			id, _ := ig.New()
			did, _ := ig.New()
			versionIDs = append(versionIDs, id)
			version := &ObjectInfo{
				ID:     id,
				PID:    fileID,
				MTime:  Now() + int64(i),
				DataID: did,
				Type:   OBJ_TYPE_VERSION,
				Name:   fmt.Sprintf("%d", Now()+int64(i)),
				Size:   100,
				Extra:  "{}",
			}
			_, err = dma.PutObj(c, bktID, []*ObjectInfo{version})
			So(err, ShouldBeNil)
		}

		Convey("list versions including writing version", func() {
			versions, err := dma.ListVersions(c, bktID, fileID, false)
			So(err, ShouldBeNil)
			So(len(versions), ShouldEqual, 4) // 1 writing + 3 regular
		})

		Convey("list versions excluding writing version", func() {
			versions, err := dma.ListVersions(c, bktID, fileID, true)
			So(err, ShouldBeNil)
			So(len(versions), ShouldEqual, 3) // Only regular versions
			for _, v := range versions {
				So(v.Name, ShouldNotEqual, WritingVersionName)
			}
		})
	})
}

// TestGetObjByDataID tests GetObjByDataID
func TestGetObjByDataID(t *testing.T) {
	Convey("GetObjByDataID", t, func() {
		InitBucketDB(context.TODO(), bktID)
		dma := &DefaultMetadataAdapter{}
		ig := idgen.NewIDGen(nil, 0)
		dataID, _ := ig.New()

		// Create multiple objects with same DataID
		var objIDs []int64
		for i := 0; i < 3; i++ {
			id, _ := ig.New()
			objIDs = append(objIDs, id)
			obj := &ObjectInfo{
				ID:     id,
				PID:    0,
				MTime:  Now(),
				DataID: dataID,
				Type:   OBJ_TYPE_FILE,
				Name:   fmt.Sprintf("file%d", i),
				Size:   100,
				Extra:  "{}",
			}
			_, err := dma.PutObj(c, bktID, []*ObjectInfo{obj})
			So(err, ShouldBeNil)
		}

		Convey("get objects by DataID", func() {
			objs, err := dma.GetObjByDataID(c, bktID, dataID)
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 3)
			for _, obj := range objs {
				So(obj.DataID, ShouldEqual, dataID)
			}
		})

		Convey("get objects by non-existent DataID", func() {
			nonExistentDataID, _ := ig.New()
			objs, err := dma.GetObjByDataID(c, bktID, nonExistentDataID)
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 0)
		})
	})
}

// TestDeleteObj tests DeleteObj
func TestDeleteObj(t *testing.T) {
	Convey("DeleteObj", t, func() {
		InitBucketDB(context.TODO(), bktID)
		dma := &DefaultMetadataAdapter{}
		ig := idgen.NewIDGen(nil, 0)
		pid, _ := ig.New()

		// Create object
		objID, _ := ig.New()
		did, _ := ig.New()
		obj := &ObjectInfo{
			ID:     objID,
			PID:    pid,
			MTime:  Now(),
			DataID: did,
			Type:   OBJ_TYPE_FILE,
			Name:   "testfile",
			Size:   100,
			Extra:  "{}",
		}
		_, err := dma.PutObj(c, bktID, []*ObjectInfo{obj})
		So(err, ShouldBeNil)

		Convey("delete object", func() {
			err := dma.DeleteObj(c, bktID, objID)
			So(err, ShouldBeNil)

			// Verify object is marked as deleted (PID flipped to negative)
			objs, err := dma.GetObj(c, bktID, []int64{objID})
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 1)
			So(objs[0].PID, ShouldBeLessThan, 0)
		})

		Convey("delete object with root PID", func() {
			rootObjID, _ := ig.New()
			rootObj := &ObjectInfo{
				ID:     rootObjID,
				PID:    0, // Root
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   "rootfile",
				Size:   100,
				Extra:  "{}",
			}
			_, err := dma.PutObj(c, bktID, []*ObjectInfo{rootObj})
			So(err, ShouldBeNil)

			err = dma.DeleteObj(c, bktID, rootObjID)
			So(err, ShouldBeNil)

			// Verify root object uses -1 as special marker
			objs, err := dma.GetObj(c, bktID, []int64{rootObjID})
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 1)
			So(objs[0].PID, ShouldEqual, -1)
		})
	})
}
