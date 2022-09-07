package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

var bktID = int64(0)

func init() {
	Init(&CoreConfig{
		Path: "/tmp/test/",
	})
	bktID, _ = idgen.NewIDGen(nil, 0).New()
}

func TestListBkt(t *testing.T) {
	Convey("normal", t, func() {
		Convey("put bkt", func() {
			InitDB()
			dmo := NewDefaultMetadataAdapter(&DefaultAccessCtrlMgr{})
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
			So(dmo.PutBkt(c, []*BucketInfo{b1, b2}), ShouldBeNil)

			bs, err := dmo.ListBkt(c, uid)
			So(err, ShouldBeNil)
			So(len(bs), ShouldEqual, 2)
			So(bs[0], ShouldResemble, b1)
			So(bs[1], ShouldResemble, b2)
		})
	})
}

func TestRefData(t *testing.T) {
	Convey("normal", t, func() {
		dmo := NewDefaultMetadataAdapter(&DefaultAccessCtrlMgr{})
		InitBucketDB(bktID)
		id, _ := idgen.NewIDGen(nil, 0).New()
		So(dmo.PutData(c, bktID, []*DataInfo{{
			ID:       id,
			OrigSize: 1,
			HdrCRC32: 222,
			CRC32:    333,
			MD5:      "444",
			Kind:     DATA_NORMAL,
		}}), ShouldBeNil)

		Convey("single try ref", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, 0)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 0,
				HdrCRC32: 222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 0,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 0,
				CRC32:    333,
				MD5:      "444",
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)
		})
		Convey("multiple try ref", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
			}, {
				OrigSize: 1,
				HdrCRC32: 222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldNotEqual, 0)
		})
		Convey("multiple try ref diff", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
			}, {
				OrigSize: 1,
				HdrCRC32: 111,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldEqual, 0)
		})

		Convey("single ref", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      "444",
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, id)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
				CRC32:    0,
				MD5:      "444",
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, id)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[0], ShouldEqual, 1)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
				CRC32:    333,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, id)
			So(ids[0], ShouldEqual, 0)
		})

		Convey("multiple ref", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      "444",
			}, {
				OrigSize: 1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      "444",
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldNotEqual, 0)
		})
		Convey("multiple ref diff", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      "444",
			}, {
				OrigSize: 1,
				HdrCRC32: 111,
				CRC32:    333,
				MD5:      "444",
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldEqual, id)
			So(ids[1], ShouldEqual, 0)
		})

		Convey("multiple ref same but do not exist", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{{
				OrigSize: 1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      "445",
			}, {
				OrigSize: 1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      "445",
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
			dmo := NewDefaultMetadataAdapter(&DefaultAccessCtrlMgr{})
			InitBucketDB(bktID)
			id, _ := idgen.NewIDGen(nil, 0).New()
			d := &DataInfo{
				ID:   id,
				Size: 1,
				Kind: DATA_NORMAL,
			}
			So(dmo.PutData(c, bktID, []*DataInfo{d}), ShouldBeNil)

			d1, err := dmo.GetData(c, bktID, id)
			So(err, ShouldBeNil)
			So(d1, ShouldResemble, d)
		})
	})
}

func TestPutObj(t *testing.T) {
	Convey("normal", t, func() {
		Convey("put same name obj", func() {
			dmo := NewDefaultMetadataAdapter(&DefaultAccessCtrlMgr{})
			InitBucketDB(bktID)
			ig := idgen.NewIDGen(nil, 0)
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()
			d := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  time.Now().Unix(),
				DataID: did,
				Type:   OBJ_TYPE_DIR,
				Status: OBJ_NORMAL,
				Name:   "test",
				Size:   1,
				Ext:    "{}",
			}
			ids, err := dmo.PutObj(c, bktID, []*ObjectInfo{d})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, id)

			id1, _ := ig.New()
			d1 := &ObjectInfo{
				ID:     id1,
				PID:    pid,
				MTime:  time.Now().Unix(),
				DataID: did,
				Type:   OBJ_TYPE_DIR,
				Status: OBJ_NORMAL,
				Name:   "test",
				Size:   1,
				Ext:    "{}",
			}
			// push same name diff id obj
			ids, err = dmo.PutObj(c, bktID, []*ObjectInfo{d1, d})
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
			dmo := NewDefaultMetadataAdapter(&DefaultAccessCtrlMgr{})
			InitBucketDB(bktID)
			ig := idgen.NewIDGen(nil, 0)
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()
			d := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  time.Now().Unix(),
				DataID: did,
				Type:   OBJ_TYPE_DIR,
				Status: OBJ_NORMAL,
				Name:   "test",
				Size:   1,
				Ext:    "{}",
			}
			ids, err := dmo.PutObj(c, bktID, []*ObjectInfo{d})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, d)

			d1, err := dmo.GetObj(c, bktID, ids)
			So(err, ShouldBeNil)
			So(len(d1), ShouldEqual, 1)
			So(d1[0], ShouldResemble, d)
		})
	})
}

func TestSetObj(t *testing.T) {
	Convey("normal", t, func() {

		dmo := NewDefaultMetadataAdapter(&DefaultAccessCtrlMgr{})
		InitBucketDB(bktID)
		ig := idgen.NewIDGen(nil, 0)
		id, _ := ig.New()
		pid, _ := ig.New()
		did, _ := ig.New()
		d := &ObjectInfo{
			ID:     id,
			PID:    pid,
			MTime:  time.Now().Unix(),
			DataID: did,
			Type:   OBJ_TYPE_DIR,
			Status: OBJ_NORMAL,
			Name:   "test",
			Size:   1,
			Ext:    "{}",
		}
		ids, err := dmo.PutObj(c, bktID, []*ObjectInfo{d})
		So(err, ShouldBeNil)
		So(len(ids), ShouldEqual, 1)
		So(ids[0], ShouldNotEqual, d)

		d1, err := dmo.GetObj(c, bktID, ids)
		So(err, ShouldBeNil)
		So(len(d1), ShouldEqual, 1)
		So(d1[0], ShouldResemble, d)
		Convey("set obj name", func() {
			d.Name = "test1"
			dmo.SetObj(c, bktID, []string{"name"}, &ObjectInfo{ID: id, Name: d.Name})
			d1, err = dmo.GetObj(c, bktID, ids)
			So(err, ShouldBeNil)
			So(len(d1), ShouldEqual, 1)
			So(d1[0], ShouldResemble, d)
		})
		Convey("set obj status", func() {
			d.Status = OBJ_MALFORMED
			dmo.SetObj(c, bktID, []string{"status"}, &ObjectInfo{ID: id, Status: d.Status})
			d1, err = dmo.GetObj(c, bktID, ids)
			So(err, ShouldBeNil)
			So(len(d1), ShouldEqual, 1)
			So(d1[0], ShouldResemble, d)
		})
	})
}

func TestListObj(t *testing.T) {
	Convey("normal", t, func() {

		dmo := NewDefaultMetadataAdapter(&DefaultAccessCtrlMgr{})
		InitBucketDB(bktID)
		ig := idgen.NewIDGen(nil, 0)
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
			Status: OBJ_NORMAL,
			Name:   "test1",
			Size:   0,
			Ext:    "{}",
		}
		d2 := &ObjectInfo{
			ID:     id2,
			PID:    pid,
			MTime:  now + 2,
			DataID: did2,
			Type:   OBJ_TYPE_FILE,
			Status: OBJ_NORMAL,
			Name:   "test2",
			Size:   2,
			Ext:    "{}",
		}
		d3 := &ObjectInfo{
			ID:     id3,
			PID:    pid,
			MTime:  now + 3,
			DataID: did3,
			Type:   OBJ_TYPE_VERSION,
			Status: OBJ_NORMAL,
			Name:   "test3",
			Size:   3,
			Ext:    "{}",
		}
		d4 := &ObjectInfo{
			ID:     id4,
			PID:    pid,
			MTime:  now + 3,
			DataID: did4,
			Type:   OBJ_TYPE_PREVIEW,
			Status: OBJ_NORMAL,
			Name:   "test4",
			Size:   3,
			Ext:    "{}",
		}
		d5 := &ObjectInfo{
			ID:     id5,
			PID:    pid,
			MTime:  now + 4,
			DataID: did5,
			Type:   OBJ_TYPE_PREVIEW,
			Status: OBJ_DELETED,
			Name:   "test5",
			Size:   4,
			Ext:    "{}",
		}
		ids, err := dmo.PutObj(c, bktID, []*ObjectInfo{d1, d2, d3, d4, d5})
		So(err, ShouldBeNil)
		So(len(ids), ShouldEqual, 5)

		Convey("list obj pagination", func() {
			o, cnt, d, err := dmo.ListObj(c, bktID, pid, "", "", "", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id2))

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", d, "", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id4))

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", d, "", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id5))

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", d, "", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 0)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, "")
		})

		Convey("word", func() {
			o, cnt, d, err := dmo.ListObj(c, bktID, pid, "xxx", "", "", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 0)
			So(cnt, ShouldEqual, 0)
			So(d, ShouldEqual, "")

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "test1", "", "", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 1)
			So(d, ShouldEqual, fmt.Sprint(id1))

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "?es*", "", "", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id2))
		})

		Convey("order", func() {
			o, cnt, d, err := dmo.ListObj(c, bktID, pid, "", "", "+id", 5, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 5)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id5))
			So(o, ShouldResemble, []*ObjectInfo{d1, d2, d3, d4, d5})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", "", "-id", 5, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 5)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprint(id1))
			So(o, ShouldResemble, []*ObjectInfo{d5, d4, d3, d2, d1})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", "", "-name", 5, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 5)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, "test1")
			So(o, ShouldResemble, []*ObjectInfo{d5, d4, d3, d2, d1})

			// 比较非id或者name的时候，相同值的排序是否稳定
			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", "", "+mtime", 3, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 3)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", now+3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d1, d2, d3})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", "", "-mtime", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", now+3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d5, d3})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", d, "-mtime", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", now+2, id2))
			So(o, ShouldResemble, []*ObjectInfo{d4, d2})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", "", "+size", 3, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 3)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", 3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d1, d2, d3})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", "", "-size", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", 3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d5, d3})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", d, "-size", 2, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", 2, id2))
			So(o, ShouldResemble, []*ObjectInfo{d4, d2})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", "", "+type", 3, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 3)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", 3, id3))
			So(o, ShouldResemble, []*ObjectInfo{d1, d2, d3})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", "", "-type", 1, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", OBJ_TYPE_PREVIEW, id4))
			So(o, ShouldResemble, []*ObjectInfo{d4})

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", d, "-type", 1, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", OBJ_TYPE_PREVIEW, id5))
			So(o, ShouldResemble, []*ObjectInfo{d5})
		})

		Convey("status filter", func() {
			o, cnt, d, err := dmo.ListObj(c, bktID, pid, "", "", "", 2, OBJ_NORMAL)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(cnt, ShouldEqual, 4)
			So(d, ShouldEqual, fmt.Sprint(id2))

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", d, "", 2, OBJ_DELETED)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 1)
			So(d, ShouldEqual, fmt.Sprint(id5))

			o, cnt, d, err = dmo.ListObj(c, bktID, pid, "", d, "", 2, OBJ_RECYCLED)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 0)
			So(cnt, ShouldEqual, 0)
			So(d, ShouldEqual, "")
		})

		Convey("list obj with 0 count", func() {
			o, cnt, d, err := dmo.ListObj(c, bktID, pid, "", "", "", 0, 0)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 0)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldBeEmpty)
		})
	})
}
