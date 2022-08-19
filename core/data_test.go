package core

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	Init(&CoreConfig{
		Path: "./test/",
	})
}

func TestWrite(t *testing.T) {
	Convey("normal", t, func() {
		Convey("sync write one file", func() {
			ddo := &DefaultDataOperator{}
			So(ddo.Write(context.TODO(), "11111", []byte("xxxxx")), ShouldBeNil)
		})
		Convey("async write one file", func() {
			ddo := &DefaultDataOperator{
				Options: Option{
					Async: true,
				}}
			So(ddo.Write(context.TODO(), "22222", []byte("yyyyy")), ShouldBeNil)
			time.Sleep(10 * time.Second)
		})
	})
}
