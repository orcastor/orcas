package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	Init(&CoreConfig{
		Path: "/tmp/test/",
	})
	bktID, _ = idgen.NewIDGen(nil, 0).New()
}

var c = context.TODO()

func TestWrite(t *testing.T) {
	Convey("normal", t, func() {
		Convey("sync write one file", func() {
			ddo := NewDefaultDataOperator(&DefaultAccessCtrlMgr{})
			ddo.SetOptions(Options{
				Sync: true,
			})
			So(ddo.Write(c, bktID, 4701534814223, 0, []byte("xxxxx")), ShouldBeNil)
		})
		Convey("async write one file", func() {
			ddo := NewDefaultDataOperator(&DefaultAccessCtrlMgr{})
			So(ddo.Write(c, bktID, 4701535862800, 0, []byte("yyyyy")), ShouldBeNil)
			for HasInflight() {
				time.Sleep(time.Second)
			}
		})
	})
	Convey("empty file", t, func() {
		Convey("write one empty file", func() {
			ddo := NewDefaultDataOperator(&DefaultAccessCtrlMgr{})
			ddo.SetOptions(Options{
				Sync: true,
			})
			So(ddo.Write(c, bktID, 4701534814288, 0, nil), ShouldBeNil)
		})
	})
}

func TestRead(t *testing.T) {
	Convey("normal", t, func() {
		Convey("read one file", func() {
			ddo := NewDefaultDataOperator(&DefaultAccessCtrlMgr{})
			ddo.SetOptions(Options{
				Sync: true,
			})
			key, _ := idgen.NewIDGen(nil, 0).New()
			value := []byte("test_read")
			So(ddo.Write(c, bktID, key, 0, []byte(value)), ShouldBeNil)

			bs, err := ddo.Read(c, bktID, key, 0)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value)
		})
	})
}

func TestReadBytes(t *testing.T) {
	Convey("normal", t, func() {
		ddo := NewDefaultDataOperator(&DefaultAccessCtrlMgr{})
		ddo.SetOptions(Options{
			Sync: true,
		})
		key, _ := idgen.NewIDGen(nil, 0).New()
		value := []byte("test_read")
		So(ddo.Write(c, bktID, key, 0, []byte(value)), ShouldBeNil)

		Convey("offset - 0", func() {
			bs, err := ddo.ReadBytes(c, bktID, key, 0, 0, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value)
		})

		Convey("offset - valid x with all", func() {
			bs, err := ddo.ReadBytes(c, bktID, key, 0, 2, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})

		Convey("offset - invalid x with all", func() {
			bs, err := ddo.ReadBytes(c, bktID, key, 0, 12, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldBeNil)
		})

		Convey("size - valid x with valid size", func() {
			bs, err := ddo.ReadBytes(c, bktID, key, 0, 2, len(value)-2)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})

		Convey("size - valid x with bigger size", func() {
			bs, err := ddo.ReadBytes(c, bktID, key, 0, 2, len(value)*2)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})
	})
}

func TestWriteSyncConcurrent(t *testing.T) {
	Convey("normal", t, func() {
		Convey("sync write files", func() {
			ig := idgen.NewIDGen(nil, 0)
			ddo := NewDefaultDataOperator(&DefaultAccessCtrlMgr{})
			ddo.SetOptions(Options{
				Sync: true,
			})
			bid, _ := ig.New()
			for i := 0; i < 20000; i++ {
				id, _ := ig.New()
				ddo.Write(c, bid, id, 0, []byte(fmt.Sprint(i)))
			}
		})
	})
}

func TestWriteAsyncConcurrent(t *testing.T) {
	Convey("normal", t, func() {
		Convey("async write files", func() {
			ig := idgen.NewIDGen(nil, 0)
			ddo := NewDefaultDataOperator(&DefaultAccessCtrlMgr{})
			bid, _ := ig.New()
			for i := 0; i < 20000; i++ {
				id, _ := ig.New()
				ddo.Write(c, bid, id, 0, []byte(fmt.Sprint(i)))
			}
			for HasInflight() {
				time.Sleep(time.Second)
			}
		})
	})
}
