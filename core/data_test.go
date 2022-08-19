package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	Init(&CoreConfig{
		Path: "/tmp/test/",
	})
}

func TestWrite(t *testing.T) {
	Convey("normal", t, func() {
		Convey("sync write one file", func() {
			ddo := &DefaultDataOperator{
				Options: Option{
					Sync: true,
				}}
			So(ddo.Write(context.TODO(), "11111", []byte("xxxxx")), ShouldBeNil)
		})
		Convey("async write one file", func() {
			ddo := &DefaultDataOperator{}
			So(ddo.Write(context.TODO(), "22222", []byte("yyyyy")), ShouldBeNil)
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
			key := "test_read"
			value := []byte("test_read")
			So(ddo.Write(context.TODO(), key, []byte(value)), ShouldBeNil)
			bs, err := ddo.Read(context.TODO(), key)
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
		key := "test_read"
		value := []byte("test_read")
		So(ddo.Write(context.TODO(), key, []byte(value)), ShouldBeNil)

		Convey("offset - 0", func() {
			bs, err := ddo.ReadBytes(context.TODO(), key, 0, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value)
		})

		Convey("offset - valid x with all", func() {
			bs, err := ddo.ReadBytes(context.TODO(), key, 2, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})

		Convey("offset - invalid x with all", func() {
			bs, err := ddo.ReadBytes(context.TODO(), key, 12, -1)
			So(err, ShouldBeNil)
			So(bs, ShouldBeNil)
		})

		Convey("size - valid x with valid size", func() {
			bs, err := ddo.ReadBytes(context.TODO(), key, 2, int64(len(value)-2))
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})

		Convey("size - valid x with bigger size", func() {
			bs, err := ddo.ReadBytes(context.TODO(), key, 2, int64(len(value)*2))
			So(err, ShouldBeNil)
			So(bs, ShouldResemble, value[2:])
		})
	})
}

func TestWriteSyncConcurrent(t *testing.T) {
	Convey("normal", t, func() {
		Convey("sync write files", func() {
			ddo := &DefaultDataOperator{
				Options: Option{
					Sync: true,
				}}
			for i := 0; i < 10000; i++ {
				So(ddo.Write(context.TODO(), fmt.Sprint(i), []byte(fmt.Sprint(i))), ShouldBeNil)
			}
		})
	})
}

func TestWriteAsyncConcurrent(t *testing.T) {
	Convey("normal", t, func() {
		Convey("async write files", func() {
			ddo := &DefaultDataOperator{}
			for i := 0; i < 10000; i++ {
				So(ddo.Write(context.TODO(), fmt.Sprint(i), []byte(fmt.Sprint(i))), ShouldBeNil)
			}
			for HasInflight() {
				time.Sleep(time.Second)
			}
		})
	})
}
