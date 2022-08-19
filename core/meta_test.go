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

func TestPutDataInfo(t *testing.T) {
	Convey("normal", t, func() {
		Convey("sync write one file", func() {
			dmo := &DefaultMetaOperator{}
			InitBucket("meta.db")
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
		Convey("sync write one file", func() {
			dmo := &DefaultMetaOperator{}
			InitBucket("meta.db")
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
