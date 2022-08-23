package core

import (
	"testing"

	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	Init(&CoreConfig{
		Path: "/tmp/test/",
	})
}

var bktID = int64(888)

func TestListBkt(t *testing.T) {
	Convey("normal", t, func() {
		Convey("put bkt", func() {
			InitDB()
			dmo := &DefaultMetaOperator{}
			id1, _ := idgen.NewIDGen(nil, 0).New()
			id2, _ := idgen.NewIDGen(nil, 0).New()
			uid, _ := idgen.NewIDGen(nil, 0).New()
			oid, _ := idgen.NewIDGen(nil, 0).New()
			b1 := &BucketInfo{
				ID:   id1,
				Name: "zhangwei",
				UID:  uid,
				Type: 1,
				OID:  oid,
			}
			b2 := &BucketInfo{
				ID:   id2,
				Name: "zhangwei2",
				UID:  uid,
				Type: 1,
				OID:  oid,
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
		dmo := &DefaultMetaOperator{}
		InitBucketDB(bktID)
		id, _ := idgen.NewIDGen(nil, 0).New()
		So(dmo.PutData(c, bktID, []*DataInfo{&DataInfo{
			ID:       id,
			Size:     1,
			HdrCRC32: 222,
			CRC32:    333,
			MD5:      444,
			Status:   1,
		}}), ShouldBeNil)

		Convey("single try ref", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, 0)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     0,
				HdrCRC32: 222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 0,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 0,
				CRC32:    333,
				MD5:      444,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)
		})
		Convey("multiple try ref", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
			}, &DataInfo{
				Size:     1,
				HdrCRC32: 222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldNotEqual, 0)
		})
		Convey("multiple try ref diff", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
			}, &DataInfo{
				Size:     1,
				HdrCRC32: 111,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldEqual, 0)
		})

		Convey("single ref", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      444,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, id)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
				CRC32:    0,
				MD5:      444,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, id)
			So(ids[0], ShouldNotEqual, 0)

			ids, err = dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      0,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, id)
			So(ids[0], ShouldNotEqual, 0)
		})

		Convey("multiple ref", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      444,
			}, &DataInfo{
				Size:     1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      444,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldNotEqual, 0)
			So(ids[1], ShouldNotEqual, 0)
		})
		Convey("multiple ref diff", func() {
			ids, err := dmo.RefData(c, bktID, []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      444,
			}, &DataInfo{
				Size:     1,
				HdrCRC32: 111,
				CRC32:    333,
				MD5:      444,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldEqual, id)
			So(ids[1], ShouldEqual, 0)
		})
	})
}

func TestPutData(t *testing.T) {
	Convey("normal", t, func() {
		Convey("put data info", func() {
			dmo := &DefaultMetaOperator{}
			InitBucketDB(bktID)
			id, _ := idgen.NewIDGen(nil, 0).New()
			So(dmo.PutData(c, bktID, []*DataInfo{&DataInfo{
				ID:     id,
				Size:   1,
				Status: 1,
			}}), ShouldBeNil)
		})
	})
}

func TestGetData(t *testing.T) {
	Convey("normal", t, func() {
		Convey("get data info", func() {
			dmo := &DefaultMetaOperator{}
			InitBucketDB(bktID)
			id, _ := idgen.NewIDGen(nil, 0).New()
			d := &DataInfo{
				ID:     id,
				Size:   1,
				Status: 1,
			}
			So(dmo.PutData(c, bktID, []*DataInfo{d}), ShouldBeNil)

			d1, err := dmo.GetData(c, bktID, id)
			So(err, ShouldBeNil)
			So(d1, ShouldResemble, d)
		})
	})
}
