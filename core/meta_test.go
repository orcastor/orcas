package core

import (
	"context"
	"testing"

	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	Init(&CoreConfig{
		Path: "/tmp/test/",
	})
}

func TestRef(t *testing.T) {
	Convey("normal", t, func() {
		dmo := &DefaultMetaOperator{}
		InitBucket(DATA_DIR)
		id, _ := idgen.NewIDGen(nil, 0).New()
		So(dmo.PutDataInfo(context.TODO(), []*DataInfo{&DataInfo{
			ID:       id,
			Size:     1,
			HdrCRC32: 222,
			CRC32:    333,
			MD5:      444,
			Status:   1,
		}}), ShouldBeNil)

		Convey("single try ref", func() {
			ids, err := dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, 0)

			ids, err = dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
				Size:     0,
				HdrCRC32: 222,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)

			ids, err = dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 0,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, 0)

			ids, err = dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
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
			ids, err := dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
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
			ids, err := dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
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
			ids, err := dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
				CRC32:    333,
				MD5:      444,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, id)

			ids, err = dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
				Size:     1,
				HdrCRC32: 222,
				CRC32:    0,
				MD5:      444,
			}})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldNotEqual, id)
			So(ids[0], ShouldNotEqual, 0)

			ids, err = dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
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
			ids, err := dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
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
			ids, err := dmo.Ref(context.TODO(), []*DataInfo{&DataInfo{
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

func TestPutDataInfo(t *testing.T) {
	Convey("normal", t, func() {
		Convey("put data info", func() {
			dmo := &DefaultMetaOperator{}
			InitBucket(DATA_DIR)
			id, _ := idgen.NewIDGen(nil, 0).New()
			So(dmo.PutDataInfo(context.TODO(), []*DataInfo{&DataInfo{
				ID:     id,
				Size:   1,
				Status: 1,
			}}), ShouldBeNil)
		})
	})
}

func TestGetDataInfo(t *testing.T) {
	Convey("normal", t, func() {
		Convey("get data info", func() {
			dmo := &DefaultMetaOperator{}
			InitBucket(DATA_DIR)
			id, _ := idgen.NewIDGen(nil, 0).New()
			d := &DataInfo{
				ID:     id,
				Size:   1,
				Status: 1,
			}
			So(dmo.PutDataInfo(context.TODO(), []*DataInfo{d}), ShouldBeNil)

			d1, err := dmo.GetDataInfo(context.TODO(), id)
			So(err, ShouldBeNil)
			So(d1, ShouldResemble, d)
		})
	})
}
