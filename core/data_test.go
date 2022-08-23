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
}

var c = context.TODO()

func TestWrite(t *testing.T) {
	Convey("normal", t, func() {
		Convey("sync write one file", func() {
			ddo := &DefaultDataOperator{
				Options: Option{
					Sync: true,
				}}
			So(ddo.Write(c, 4701534814223, 0, []byte("xxxxx")), ShouldBeNil)
		})
		Convey("async write one file", func() {
			ddo := &DefaultDataOperator{}
			So(ddo.Write(c, 4701535862800, 0, []byte("yyyyy")), ShouldBeNil)
			for HasInflight() {
				time.Sleep(time.Second)
			}
		})
	})
}

func TestRead(t *testing.T) {
	Convey("normal", t, func() {
		Convey("read one file", func() {
			ddo := &DefaultDataOperator{
				Options: Option{
					Sync: true,
				}}
			key := int64(4701529571344)
			value := []byte("test_read")
			So(ddo.Write(c, key, 0, []byte(value)), ShouldBeNil)
			bs, err := ddo.Read(c, key, 0)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value)
		})
	})
}

func TestReadBytes(t *testing.T) {
	Convey("normal", t, func() {
		ddo := &DefaultDataOperator{
			Options: Option{
				Sync: true,
			}}
		key := int64(4701530619920)
		value := []byte("test_read")
		So(ddo.Write(c, key, 0, []byte(value)), ShouldBeNil)

		Convey("offset - 0", func() {
			bs, err := ddo.ReadBytes(c, key, 0, 0, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value)
		})

		Convey("offset - valid x with all", func() {
			bs, err := ddo.ReadBytes(c, key, 0, 2, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})

		Convey("offset - invalid x with all", func() {
			bs, err := ddo.ReadBytes(c, key, 0, 12, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldBeNil)
		})

		Convey("size - valid x with valid size", func() {
			bs, err := ddo.ReadBytes(c, key, 0, 2, int64(len(value)-2))
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})

		Convey("size - valid x with bigger size", func() {
			bs, err := ddo.ReadBytes(c, key, 0, 2, int64(len(value)*2))
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})
	})
}

func TestWriteSyncConcurrent(t *testing.T) {
	Convey("normal", t, func() {
		Convey("sync write files", func() {
			ig := idgen.NewIDGen(nil, 0)
			ddo := &DefaultDataOperator{
				Options: Option{
					Sync: true,
				}}
			for i := 0; i < 30001; i++ {
				id, _ := ig.New()
				So(ddo.Write(c, id, 0, []byte(fmt.Sprint(i))), ShouldBeNil)
			}
		})
	})
}

func TestWriteAsyncConcurrent(t *testing.T) {
	Convey("normal", t, func() {
		Convey("async write files", func() {
			ig := idgen.NewIDGen(nil, 0)
			ddo := &DefaultDataOperator{}
			for i := 0; i < 30001; i++ {
				id, _ := ig.New()
				So(ddo.Write(c, id, 0, []byte(fmt.Sprint(i))), ShouldBeNil)
			}
			for HasInflight() {
				time.Sleep(time.Second)
			}
		})
	})
}
