package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	b "github.com/orca-zhang/borm"
	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	bktID = int64(0)
	c     = context.TODO()
)

func init() {
	// Initialize test environment with in-memory filesystem
	InitTestEnv()
	bktID, _ = idgen.NewIDGen(nil, 0).New()
}

func TestRefData(t *testing.T) {
	Convey("normal", t, func() {
		baseDir, dataDir, cleanup := SetupTestDirs("test_ref_data")
		defer cleanup()

		InitDB(baseDir, "")
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultBaseMetadataAdapter.SetPath(baseDir)
		dma.DefaultDataMetadataAdapter.SetPath(dataDir)
		InitBucketDB(dataDir, bktID)

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
				HdrXXH3:  888,
				XXH3:     777,
				SHA256_0: 11111,
				SHA256_1: 22222,
				SHA256_2: 33333,
				SHA256_3: 44444,
			}, {
				OrigSize: 999,
				HdrXXH3:  888,
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
			InitBucketDB(".", bktID)
			dma := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma.DefaultDataMetadataAdapter.SetPath(".")
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
			InitBucketDB(".", bktID)

			dma := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma.DefaultDataMetadataAdapter.SetPath(".")
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

// TestPutObjDuplicateName tests the behavior when creating files with the same name in the same directory
// This test verifies that the behavior is consistent after changing from InsertIgnore to ReplaceInto
func TestPutObjDuplicateName(t *testing.T) {
	Convey("PutObj with duplicate name in same directory", t, func() {
		InitDB(".", "")
		testBktID, _ := idgen.NewIDGen(nil, 0).New()
		InitBucketDB(".", testBktID)

		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetDataPath(".")

		ig := idgen.NewIDGen(nil, 0)
		pid, _ := ig.New() // Parent directory ID

		// Create first file
		file1ID, _ := ig.New()
		file1 := &ObjectInfo{
			ID:     file1ID,
			PID:    pid,
			Type:   OBJ_TYPE_FILE,
			Name:   "test.txt",
			DataID: 0,
			Size:   100,
			MTime:  Now(),
		}

		ids, err := dma.PutObj(c, testBktID, []*ObjectInfo{file1})
		So(err, ShouldBeNil)
		So(len(ids), ShouldEqual, 1)
		So(ids[0], ShouldEqual, file1ID)

		// Verify file1 exists
		objs, err := dma.GetObj(c, testBktID, []int64{file1ID})
		So(err, ShouldBeNil)
		So(len(objs), ShouldEqual, 1)
		So(objs[0].ID, ShouldEqual, file1ID)
		So(objs[0].Name, ShouldEqual, "test.txt")

		// Try to create second file with same name and different ID
		// This should conflict with (pid, n) unique constraint
		file2ID, _ := ig.New()
		file2 := &ObjectInfo{
			ID:     file2ID,
			PID:    pid,
			Type:   OBJ_TYPE_FILE,
			Name:   "test.txt", // Same name, same parent
			DataID: 0,
			Size:   200,
			MTime:  Now(),
		}

		ids, err = dma.PutObj(c, testBktID, []*ObjectInfo{file2})
		So(err, ShouldBeNil)
		So(len(ids), ShouldEqual, 1)
		// With ReplaceInto, file2 should replace file1
		// So file2ID should exist, but file1ID should be replaced
		So(ids[0], ShouldEqual, file2ID)

		// Verify file1 no longer exists (replaced by file2)
		objs, err = dma.GetObj(c, testBktID, []int64{file1ID})
		So(err, ShouldBeNil)
		So(len(objs), ShouldEqual, 0) // file1 was replaced

		// Verify file2 exists
		objs, err = dma.GetObj(c, testBktID, []int64{file2ID})
		So(err, ShouldBeNil)
		So(len(objs), ShouldEqual, 1)
		So(objs[0].ID, ShouldEqual, file2ID)
		So(objs[0].Name, ShouldEqual, "test.txt")
		So(objs[0].Size, ShouldEqual, 200) // file2's size

		// Verify only one file with this name exists
		children, _, _, err := dma.ListObj(c, testBktID, pid, "", "", "", 100)
		So(err, ShouldBeNil)
		count := 0
		for _, child := range children {
			if child.Name == "test.txt" {
				count++
				So(child.ID, ShouldEqual, file2ID) // Should be file2
			}
		}
		So(count, ShouldEqual, 1) // Only one file with this name
	})
}

func TestGetObj(t *testing.T) {
	Convey("normal", t, func() {
		Convey("get obj info", func() {
			InitBucketDB(".", bktID)

			dma := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma.DefaultDataMetadataAdapter.SetPath(".")
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
		InitBucketDB(".", bktID)

		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetPath(".")
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
			dma.SetObj(c, bktID, []string{"n"}, &ObjectInfo{ID: id, Name: d.Name})
			o, err = dma.GetObj(c, bktID, ids)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 2)
			So(o[0], ShouldResemble, d)
			So(o[1], ShouldResemble, d1)
		})

		Convey("same obj name", func() {
			d.Name = "test2"
			err := dma.SetObj(c, bktID, []string{"n"}, &ObjectInfo{ID: id, Name: d.Name})
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
		InitDB(".", "") // Initialize main database first
		InitBucketDB(".", testBktID)

		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetPath(".")
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
			Type:   OBJ_TYPE_JOURNAL,
			Name:   "test4",
			Size:   3,
			Extra:  "{}",
		}
		d5 := &ObjectInfo{
			ID:     id5,
			PID:    pid,
			MTime:  now + 4,
			DataID: did5,
			Type:   OBJ_TYPE_JOURNAL,
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
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", OBJ_TYPE_JOURNAL, id4))
			So(o, ShouldResemble, []*ObjectInfo{d4})

			o, cnt, d, err = dma.ListObj(c, testBktID, pid, "", d, "-type", 1)
			So(err, ShouldBeNil)
			So(len(o), ShouldEqual, 1)
			So(cnt, ShouldEqual, 5)
			So(d, ShouldEqual, fmt.Sprintf("%d:%d", OBJ_TYPE_JOURNAL, id5))
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
		InitDB(".", "") // Initialize main database first
		InitBucketDB(".", testBktID)
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetPath(".")
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
		InitDB(".", "") // Initialize main database first
		InitBucketDB(".", testBktID)
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetPath(".")
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
		InitBucketDB(".", bktID)
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetPath(".")
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

		Convey("list versions", func() {
			versions, err := dma.ListVersions(c, bktID, fileID)
			So(err, ShouldBeNil)
			So(len(versions), ShouldEqual, 3)
		})
	})
}

// TestGetObjByDataID tests GetObjByDataID
func TestGetObjByDataID(t *testing.T) {
	Convey("GetObjByDataID", t, func() {
		InitBucketDB(".", bktID)
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetPath(".")
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
		InitBucketDB(".", bktID)
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetPath(".")
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

// TestAttrOperations tests SetAttr, GetAttr, ListAttrs, RemoveAttr.
// SetAttr uses INSERT ON CONFLICT DO UPDATE (upsert) instead of REPLACE INTO,
// so updates modify the row in place without delete+insert.
func TestAttrOperations(t *testing.T) {
	Convey("AttrOperations", t, func() {
		baseDir, dataDir, cleanup := SetupTestDirs("test_attr")
		defer cleanup()
		InitDB(baseDir, "")
		ig := idgen.NewIDGen(nil, 0)
		tbktID, _ := ig.New()
		InitBucketDB(dataDir, tbktID)
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter:   &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter:   &DefaultDataMetadataAdapter{},
		}
		dma.DefaultBaseMetadataAdapter.SetPath(baseDir)
		dma.DefaultDataMetadataAdapter.SetPath(dataDir)
		ctx := context.Background()

		Convey("SetAttr insert then GetAttr", func() {
			objID, _ := ig.New()
			err := dma.SetAttr(ctx, tbktID, objID, "user.k1", []byte("v1"))
			So(err, ShouldBeNil)
			val, err := dma.GetAttr(ctx, tbktID, objID, "user.k1")
			So(err, ShouldBeNil)
			So(val, ShouldResemble, []byte("v1"))
		})

		Convey("SetAttr update same key uses upsert not replace", func() {
			objID, _ := ig.New()
			err := dma.SetAttr(ctx, tbktID, objID, "user.k1", []byte("v1"))
			So(err, ShouldBeNil)
			bktDir := filepath.Join(dataDir, fmt.Sprint(tbktID))
			db, err := GetWriteDB(bktDir)
			So(err, ShouldBeNil)
			var rowid1 int64
			err = db.QueryRow("SELECT rowid FROM attr WHERE id = ? AND k = ?", objID, "user.k1").Scan(&rowid1)
			So(err, ShouldBeNil)

			err = dma.SetAttr(ctx, tbktID, objID, "user.k1", []byte("v2"))
			So(err, ShouldBeNil)
			val, err := dma.GetAttr(ctx, tbktID, objID, "user.k1")
			So(err, ShouldBeNil)
			So(val, ShouldResemble, []byte("v2"))

			var rowid2 int64
			err = db.QueryRow("SELECT rowid FROM attr WHERE id = ? AND k = ?", objID, "user.k1").Scan(&rowid2)
			So(err, ShouldBeNil)
			So(rowid2, ShouldEqual, rowid1)
		})

		Convey("ListAttrs and RemoveAttr", func() {
			objID, _ := ig.New()
			So(dma.SetAttr(ctx, tbktID, objID, "user.a", []byte("1")), ShouldBeNil)
			So(dma.SetAttr(ctx, tbktID, objID, "user.b", []byte("2")), ShouldBeNil)
			keys, err := dma.ListAttrs(ctx, tbktID, objID)
			So(err, ShouldBeNil)
			sort.Strings(keys)
			So(keys, ShouldResemble, []string{"user.a", "user.b"})

			err = dma.RemoveAttr(ctx, tbktID, objID, "user.a")
			So(err, ShouldBeNil)
			_, err = dma.GetAttr(ctx, tbktID, objID, "user.a")
			So(err, ShouldNotBeNil)
			keys, err = dma.ListAttrs(ctx, tbktID, objID)
			So(err, ShouldBeNil)
			So(keys, ShouldResemble, []string{"user.b"})
		})
	})
}

// TestPutBktWithCustomPath tests that PutBkt creates bucket database in custom path set via Handler
func TestPutBktWithCustomPath(t *testing.T) {
	Convey("PutBkt with custom path via Handler", t, func() {
		// Create temporary directories for custom paths
		tmpBaseDir, err := os.MkdirTemp("", "orcas_test_custom_base_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpBaseDir)

		tmpDataDir, err := os.MkdirTemp("", "orcas_test_custom_data_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDataDir)

		// Remove any existing database files to ensure clean state
		os.Remove(filepath.Join(tmpBaseDir, ".db"))
		os.Remove(filepath.Join(tmpBaseDir, ".db-wal"))
		os.Remove(filepath.Join(tmpBaseDir, ".db-shm"))

		// Close connection pool to ensure clean state
		pool := GetDBPool()
		pool.Close()

		// Create context with custom paths
		ctx := context.Background()
		ctx = context.Background()

		// Initialize main database
		err = InitDB(".", "")
		So(err, ShouldBeNil)

		// Create bucket using custom path context
		ig := idgen.NewIDGen(nil, 0)
		bktID, _ := ig.New()
		uid, _ := ig.New()

		// Create admin with custom paths
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultBaseMetadataAdapter.SetPath(".")
		dma.DefaultDataMetadataAdapter.SetPath(tmpDataDir)
		acm := &DefaultAccessCtrlMgr{}
		acm.SetAdapter(dma)
		admin := NewAdminWithAdapters(dma, &DefaultDataAdapter{}, acm)

		bkt := &BucketInfo{
			ID:   bktID,
			Name: "test-bucket-custom-path",
			Type: 1,
		}

		// Set UID and ADMIN role in context for PutBkt
		ctx = UserInfo2Ctx(ctx, &UserInfo{ID: uid, Role: ADMIN})

		// PutBkt should create database in custom data path
		err = admin.PutBkt(ctx, []*BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Verify bucket database was created in custom data path
		expectedDBPath := filepath.Join(tmpDataDir, fmt.Sprint(bktID), ".db")
		_, err = os.Stat(expectedDBPath)
		So(err, ShouldBeNil)

		// Verify bucket database is NOT in current directory (if different)
		if tmpDataDir != "." {
			currentDirDBPath := filepath.Join(".", fmt.Sprint(bktID), ".db")
			_, err = os.Stat(currentDirDBPath)
			So(err, ShouldNotBeNil) // Should not exist
		}

		// Verify we can read bucket info from the custom path database
		// Create a new adapter with the same paths to read bucket info
		readDma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		readDma.DefaultBaseMetadataAdapter.SetPath(".")
		readDma.DefaultDataMetadataAdapter.SetPath(tmpDataDir)
		bkts, err := readDma.GetBkt(ctx, []int64{bktID})
		So(err, ShouldBeNil)
		So(len(bkts), ShouldEqual, 1)
		So(bkts[0].ID, ShouldEqual, bktID)
		So(bkts[0].Name, ShouldEqual, "test-bucket-custom-path")
	})
}

// TestPutBktWithConfig2Ctx tests that PutBkt creates bucket database in custom path set via Handler
func TestPutBktWithConfig2Ctx(t *testing.T) {
	Convey("PutBkt with custom path via Handler", t, func() {
		// Create temporary directories for custom paths
		tmpBaseDir, err := os.MkdirTemp("", "orcas_test_config_base_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpBaseDir)

		tmpDataDir, err := os.MkdirTemp("", "orcas_test_config_data_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDataDir)

		// Remove any existing database files to ensure clean state
		os.Remove(filepath.Join(tmpBaseDir, ".db"))
		os.Remove(filepath.Join(tmpBaseDir, ".db-wal"))
		os.Remove(filepath.Join(tmpBaseDir, ".db-shm"))

		// Create context (paths now managed via Handler)
		ctx := context.Background()

		// Initialize main database
		err = InitDB(".", "")
		So(err, ShouldBeNil)

		// Create bucket using config context
		ig := idgen.NewIDGen(nil, 0)
		bktID, _ := ig.New()
		uid, _ := ig.New()

		// Create admin with custom paths
		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultBaseMetadataAdapter.SetPath(".")
		dma.DefaultDataMetadataAdapter.SetPath(tmpDataDir)
		admin := NewAdminWithAdapters(dma, &DefaultDataAdapter{}, &DefaultAccessCtrlMgr{ma: dma})

		bkt := &BucketInfo{
			ID:   bktID,
			Name: "test-bucket-config-path",
			Type: 1,
		}

		// Set UID and ADMIN role in context for PutBkt
		ctx = UserInfo2Ctx(ctx, &UserInfo{ID: uid, Role: ADMIN})

		// PutBkt should create database in custom data path
		err = admin.PutBkt(ctx, []*BucketInfo{bkt})
		So(err, ShouldBeNil)

		// Verify bucket database was created in custom data path
		expectedDBPath := filepath.Join(tmpDataDir, fmt.Sprint(bktID), ".db")
		_, err = os.Stat(expectedDBPath)
		So(err, ShouldBeNil)

		// Verify bucket database is NOT in current directory (if different)
		if tmpDataDir != "." {
			currentDirDBPath := filepath.Join(".", fmt.Sprint(bktID), ".db")
			_, err = os.Stat(currentDirDBPath)
			So(err, ShouldNotBeNil) // Should not exist
		}

		// Verify we can read bucket info from the custom path database
		// Create a new adapter with the same paths to read bucket info
		readDma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		readDma.DefaultBaseMetadataAdapter.SetPath(".")
		readDma.DefaultDataMetadataAdapter.SetPath(tmpDataDir)
		bkts, err := readDma.GetBkt(ctx, []int64{bktID})
		So(err, ShouldBeNil)
		So(len(bkts), ShouldEqual, 1)
		So(bkts[0].ID, ShouldEqual, bktID)
		So(bkts[0].Name, ShouldEqual, "test-bucket-config-path")
	})
}

// TestPutBktMultiplePaths tests that multiple buckets can use different paths in same process
func TestPutBktMultiplePaths(t *testing.T) {
	Convey("PutBkt with multiple different paths in same process", t, func() {
		// Create two sets of temporary directories
		tmpBaseDir1, err := os.MkdirTemp("", "orcas_test_multi1_base_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpBaseDir1)

		tmpDataDir1, err := os.MkdirTemp("", "orcas_test_multi1_data_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDataDir1)

		tmpBaseDir2, err := os.MkdirTemp("", "orcas_test_multi2_base_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpBaseDir2)

		tmpDataDir2, err := os.MkdirTemp("", "orcas_test_multi2_data_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDataDir2)

		ig := idgen.NewIDGen(nil, 0)
		uid, _ := ig.New()

		// Create first admin with first path set
		dma1 := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma1.DefaultBaseMetadataAdapter.SetPath(".")
		dma1.DefaultDataMetadataAdapter.SetPath(tmpDataDir1)
		admin1 := NewAdminWithAdapters(dma1, &DefaultDataAdapter{}, &DefaultAccessCtrlMgr{ma: dma1})

		// Create first bucket with first path set
		ctx1 := context.Background()
		err = InitDB(".", "")
		So(err, ShouldBeNil)

		bktID1, _ := ig.New()
		ctx1 = UserInfo2Ctx(ctx1, &UserInfo{ID: uid, Role: ADMIN})

		bkt1 := &BucketInfo{
			ID:   bktID1,
			Name: "bucket-path1",
			Type: 1,
		}
		err = admin1.PutBkt(ctx1, []*BucketInfo{bkt1})
		So(err, ShouldBeNil)

		// Create second admin with second path set
		dma2 := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma2.DefaultBaseMetadataAdapter.SetPath(".")
		dma2.DefaultDataMetadataAdapter.SetPath(tmpDataDir2)
		admin2 := NewAdminWithAdapters(dma2, &DefaultDataAdapter{}, &DefaultAccessCtrlMgr{ma: dma2})

		// Create second bucket with second path set
		ctx2 := context.Background()
		err = InitDB(".", "")
		So(err, ShouldBeNil)

		bktID2, _ := ig.New()
		ctx2 = context.Background()
		ctx2 = UserInfo2Ctx(ctx2, &UserInfo{ID: uid, Role: ADMIN})

		bkt2 := &BucketInfo{
			ID:   bktID2,
			Name: "bucket-path2",
			Type: 1,
		}
		err = admin2.PutBkt(ctx2, []*BucketInfo{bkt2})
		So(err, ShouldBeNil)

		// Verify both buckets exist in their respective paths
		dbPath1 := filepath.Join(tmpDataDir1, fmt.Sprint(bktID1), ".db")
		_, err = os.Stat(dbPath1)
		So(err, ShouldBeNil)

		dbPath2 := filepath.Join(tmpDataDir2, fmt.Sprint(bktID2), ".db")
		_, err = os.Stat(dbPath2)
		So(err, ShouldBeNil)

		// Verify buckets are in correct locations (not swapped)
		// Use admin1's adapter to read bucket1
		bkts1, err := admin1.(*LocalAdmin).ma.GetBkt(ctx1, []int64{bktID1})
		So(err, ShouldBeNil)
		So(len(bkts1), ShouldEqual, 1)
		So(bkts1[0].Name, ShouldEqual, "bucket-path1")

		// Use admin2's adapter to read bucket2
		bkts2, err := admin2.(*LocalAdmin).ma.GetBkt(ctx2, []int64{bktID2})
		So(err, ShouldBeNil)
		So(len(bkts2), ShouldEqual, 1)
		So(bkts2[0].Name, ShouldEqual, "bucket-path2")

		// Verify bucket1 is NOT in path2 and bucket2 is NOT in path1
		wrongPath1 := filepath.Join(tmpDataDir2, fmt.Sprint(bktID1), ".db")
		_, err = os.Stat(wrongPath1)
		So(err, ShouldNotBeNil)

		wrongPath2 := filepath.Join(tmpDataDir1, fmt.Sprint(bktID2), ".db")
		_, err = os.Stat(wrongPath2)
		So(err, ShouldNotBeNil)
	})
}

// TestHandlerWithoutMainDB tests that handler can work without main database (usr, acl tables)
// This verifies that bucket operations can work independently without requiring main database
func TestHandlerWithoutMainDB(t *testing.T) {
	Convey("Handler without main database", t, func() {
		// Create temporary directories
		tmpBaseDir, err := os.MkdirTemp("", "orcas_test_no_main_db_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpBaseDir)

		tmpDataDir, err := os.MkdirTemp("", "orcas_test_no_main_db_data_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDataDir)

		// Verify main database does not exist
		mainDBPath := filepath.Join(tmpBaseDir, ".db")
		_, err = os.Stat(mainDBPath)
		So(err, ShouldNotBeNil) // Should not exist

		// Create NoAuthHandler without calling InitDB
		// NoAuthHandler bypasses main database authentication and ACL checks
		handler := NewNoAuthHandler(tmpDataDir)
		defer handler.Close()

		ctx := context.Background()

		// Create bucket ID
		ig := idgen.NewIDGen(nil, 0)
		bktID, _ := ig.New()

		// Initialize bucket database first (this should work without main database)
		err = InitBucketDB(tmpDataDir, bktID)
		So(err, ShouldBeNil)

		// Create bucket info directly in bucket database (without using PutBkt to avoid PutACL which needs main DB)
		bkt := &BucketInfo{
			ID:        bktID,
			Name:      "test-bucket-no-main-db",
			Type:      1,
			Quota:     -1, // Unlimited
			Used:      0,
			RealUsed:  0,
			ChunkSize: DEFAULT_CHUNK_SIZE,
		}
		// Write bucket info directly to bucket database
		bktDirPath := filepath.Join(tmpDataDir, fmt.Sprint(bktID))
		db, err := GetWriteDB(bktDirPath)
		So(err, ShouldBeNil)
		bktSlice := []*BucketInfo{bkt}
		_, err = b.TableContext(ctx, db, BKT_TBL).ReplaceInto(&bktSlice)
		So(err, ShouldBeNil)

		// Verify bucket database exists
		bktDBPath := filepath.Join(tmpDataDir, fmt.Sprint(bktID), ".db")
		_, err = os.Stat(bktDBPath)
		So(err, ShouldBeNil) // Should exist

		// Verify main database still does not exist
		_, err = os.Stat(mainDBPath)
		So(err, ShouldNotBeNil) // Should still not exist

		// Test PutData - should work without main database
		testData := []byte("test data without main db")
		dataID, err := handler.PutData(ctx, bktID, 0, 0, testData)
		So(err, ShouldBeNil)
		So(dataID, ShouldBeGreaterThan, 0)

		// Test GetData - should work without main database
		retrievedData, err := handler.GetData(ctx, bktID, dataID, 0)
		So(err, ShouldBeNil)
		So(retrievedData, ShouldResemble, testData)

		// Test PutObj - should work without main database
		fileID, _ := ig.New()
		fileObj := &ObjectInfo{
			ID:     fileID,
			PID:    bktID,
			Type:   OBJ_TYPE_FILE,
			Name:   "test-file.txt",
			Size:   int64(len(testData)),
			MTime:  Now(),
			DataID: dataID,
		}
		objIDs, err := handler.Put(ctx, bktID, []*ObjectInfo{fileObj})
		So(err, ShouldBeNil)
		So(len(objIDs), ShouldEqual, 1)
		So(objIDs[0], ShouldEqual, fileID)

		// Test GetObj - should work without main database
		objs, err := handler.Get(ctx, bktID, []int64{fileID})
		So(err, ShouldBeNil)
		So(len(objs), ShouldEqual, 1)
		So(objs[0].ID, ShouldEqual, fileID)
		So(objs[0].Name, ShouldEqual, "test-file.txt")
		So(objs[0].DataID, ShouldEqual, dataID)

		// Verify main database still does not exist after operations
		_, err = os.Stat(mainDBPath)
		So(err, ShouldNotBeNil) // Should still not exist

		// Verify bucket database still exists
		_, err = os.Stat(bktDBPath)
		So(err, ShouldBeNil) // Should still exist
	})
}

// TestMetadataNameEncryption tests file name encryption feature
func TestMetadataNameEncryption(t *testing.T) {
	Convey("Metadata Name Encryption", t, func() {
		// Create temporary directory
		tmpDir, err := os.MkdirTemp("", "orcas_name_encryption_test_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		testBktID := int64(999)
		encryptionKey := "test-metadata-encryption-key"
		c := context.Background()

		Convey("Encrypt and decrypt file names", func() {
			// Initialize bucket with encryption
			err := InitBucketDB(tmpDir, testBktID)
			So(err, ShouldBeNil)

			// Create adapter with encryption key
			dma := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
			dma.DefaultDataMetadataAdapter.SetDataKey(encryptionKey) // Enable encryption

			// Create test object
			ig := idgen.NewIDGen(nil, 0)
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()

			plainName := "secret-document.txt"
			obj := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   plainName, // Plain name
				Size:   1024,
				Mode:   0o644,
				Extra:  "{}",
			}

			// Put object (should encrypt name automatically during storage)
			ids, err := dma.PutObj(c, testBktID, []*ObjectInfo{obj})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(ids[0], ShouldEqual, id)

			// After Put returns, object should have decrypted name (Put returns decrypted names)
			So(obj.Name, ShouldEqual, plainName)                     // Should be decrypted after Put returns
			So(obj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0)) // Flag should be cleared after Put returns

			// Get object back (should decrypt name automatically)
			objs, err := dma.GetObj(c, testBktID, []int64{id})
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 1)
			So(objs[0].Name, ShouldEqual, plainName)                     // Should be decrypted
			So(objs[0].Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0)) // Flag should be cleared after decryption

			// List objects (should decrypt names)
			listObjs, cnt, _, err := dma.ListObj(c, testBktID, pid, "", "", "", 10)
			So(err, ShouldBeNil)
			So(cnt, ShouldEqual, 1)
			So(len(listObjs), ShouldEqual, 1)
			So(listObjs[0].Name, ShouldEqual, plainName) // Should be decrypted
		})

		Convey("No encryption when no key is set", func() {
			// Initialize bucket without encryption
			err := InitBucketDB(tmpDir, testBktID+1)
			So(err, ShouldBeNil)

			// Create adapter without encryption key
			dma := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
			// No SetDataKey call - encryption disabled

			ig := idgen.NewIDGen(nil, 0)
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()

			plainName := "public-document.txt"
			obj := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   plainName,
				Size:   512,
				Mode:   0o644,
				Extra:  "{}",
			}

			// Put object (should NOT encrypt)
			ids, err := dma.PutObj(c, testBktID+1, []*ObjectInfo{obj})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)

			// Verify name was NOT encrypted (Mode flag should NOT be set)
			So(obj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0))

			// Get object back
			objs, err := dma.GetObj(c, testBktID+1, []int64{id})
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 1)
			So(objs[0].Name, ShouldEqual, plainName) // Should still be plain
		})

		Convey("Special characters in filenames", func() {
			err := InitBucketDB(tmpDir, testBktID+2)
			So(err, ShouldBeNil)

			dma := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
			dma.DefaultDataMetadataAdapter.SetDataKey(encryptionKey)

			ig := idgen.NewIDGen(nil, 0)

			testCases := []string{
				"文件名中文.txt",
				"file with spaces.pdf",
				"file-with-dashes.doc",
				"file_with_underscores.xls",
				"file.multiple.dots.zip",
			}

			objIDs := make([]int64, len(testCases))
			pid, _ := ig.New()

			// Put all test objects
			for i, name := range testCases {
				id, _ := ig.New()
				did, _ := ig.New()
				objIDs[i] = id

				obj := &ObjectInfo{
					ID:     id,
					PID:    pid,
					MTime:  Now(),
					DataID: did,
					Type:   OBJ_TYPE_FILE,
					Name:   name,
					Size:   100,
					Mode:   0o644,
					Extra:  "{}",
				}

				ids, err := dma.PutObj(c, testBktID+2, []*ObjectInfo{obj})
				So(err, ShouldBeNil)
				So(len(ids), ShouldEqual, 1)
			}

			// Get all objects back and verify
			objs, err := dma.GetObj(c, testBktID+2, objIDs)
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, len(testCases))

			// Verify all names are correctly decrypted
			for i, obj := range objs {
				So(obj.Name, ShouldEqual, testCases[i])
			}
		})
	})
}

// TestDataKeyBasedEncryption verifies encryption uses the data DB key
func TestDataKeyBasedEncryption(t *testing.T) {
	Convey("Data Key Based Encryption", t, func() {
		tmpDir, err := os.MkdirTemp("", "orcas_datakey_test_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		testBktID := int64(888)
		key1 := "key-one"
		key2 := "key-two"

		Convey("Different keys produce different ciphertexts", func() {
			err := InitBucketDB(tmpDir, testBktID)
			So(err, ShouldBeNil)

			// Adapter 1 with key1
			dma1 := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma1.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
			dma1.DefaultDataMetadataAdapter.SetDataKey(key1)

			// Adapter 2 with key2
			dma2 := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma2.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
			dma2.DefaultDataMetadataAdapter.SetDataKey(key2)

			plainName := "test-file.txt"

			// Encrypt with key1
			encrypted1, err := dma1.DefaultDataMetadataAdapter.encryptName(plainName)
			So(err, ShouldBeNil)

			// Encrypt with key2
			encrypted2, err := dma2.DefaultDataMetadataAdapter.encryptName(plainName)
			So(err, ShouldBeNil)

			// Should be different (due to different keys and random nonces)
			So(encrypted1, ShouldNotEqual, encrypted2)
			So(encrypted1, ShouldNotEqual, plainName)
			So(encrypted2, ShouldNotEqual, plainName)

			// Decrypt with correct keys
			decrypted1, err := dma1.DefaultDataMetadataAdapter.decryptName(encrypted1)
			So(err, ShouldBeNil)
			So(decrypted1, ShouldEqual, plainName)

			decrypted2, err := dma2.DefaultDataMetadataAdapter.decryptName(encrypted2)
			So(err, ShouldBeNil)
			So(decrypted2, ShouldEqual, plainName)

			// Decrypt with wrong key (should fail gracefully and return as-is)
			wrongDecrypt, err := dma1.DefaultDataMetadataAdapter.decryptName(encrypted2)
			So(err, ShouldBeNil)
			// Should return ciphertext as-is when decryption fails
			So(wrongDecrypt, ShouldNotEqual, plainName)
		})

		Convey("Error when decrypting without key", func() {
			// Initialize bucket with encryption
			err := InitBucketDB(tmpDir, testBktID+2)
			So(err, ShouldBeNil)

			// Create adapter with encryption key and encrypt a file name
			testEncKey := "test-encryption-key-for-missing-key-test"
			dma1 := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma1.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
			dma1.DefaultDataMetadataAdapter.SetDataKey(testEncKey)

			// Create and store an object with encrypted name
			ig := idgen.NewIDGen(nil, 0)
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()

			plainName := "encrypted-file.txt"
			obj := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   plainName,
				Size:   1024,
				Mode:   0o644,
				Extra:  "{}",
			}

			// Put object (should encrypt name)
			ids, err := dma1.PutObj(c, testBktID+2, []*ObjectInfo{obj})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)
			So(obj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, MODE_NAME_ENCRYPTED)

			// Now try to list objects WITHOUT providing decryption key
			dma2 := &DefaultMetadataAdapter{
				DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
				DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
			}
			dma2.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
			// No SetDataKey call - no decryption key available

			// Try to list objects (should fail with error about missing key)
			_, _, _, err = dma2.ListObj(c, testBktID+2, pid, "", "", "", 10)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "decryption key not available")

			// Try to get object (should also fail)
			_, err = dma2.GetObj(c, testBktID+2, []int64{id})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "decryption key not available")
		})
	})
}

// TestPutObjReturnsDecryptedName tests that PutObj returns objects with decrypted names
func TestPutObjReturnsDecryptedName(t *testing.T) {
	Convey("PutObj Returns Decrypted Name", t, func() {
		tmpDir, err := os.MkdirTemp("", "orcas_put_decrypt_test_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		testBktID := int64(777)
		encryptionKey := "test-put-decrypt-key"
		testCtx := context.Background()

		err = InitBucketDB(tmpDir, testBktID)
		So(err, ShouldBeNil)

		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
		dma.DefaultDataMetadataAdapter.SetDataKey(encryptionKey)

		ig := idgen.NewIDGen(nil, 0)
		plainName := "test-file-after-put.txt"

		Convey("PutObj should return decrypted name", func() {
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()

			obj := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   plainName,
				Size:   1024,
				Mode:   0o644,
			}

			// Put object - name should be encrypted during storage
			ids, err := dma.PutObj(testCtx, testBktID, []*ObjectInfo{obj})
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 1)

			// After Put returns, object should have decrypted name
			So(obj.Name, ShouldEqual, plainName)
			So(obj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0))
		})

		Convey("PutDataAndObj should return decrypted name", func() {
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()

			obj := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   plainName,
				Size:   2048,
				Mode:   0o644,
			}

			dataInfo := &DataInfo{
				ID:       did,
				Size:     2048,
				OrigSize: 2048,
			}

			// PutDataAndObj - name should be encrypted during storage
			err := dma.PutDataAndObj(testCtx, testBktID, []*DataInfo{dataInfo}, []*ObjectInfo{obj})
			So(err, ShouldBeNil)

			// After PutDataAndObj returns, object should have decrypted name
			So(obj.Name, ShouldEqual, plainName)
			So(obj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0))
		})

		Convey("PutDataAndObj should use upsert not replace on conflict", func() {
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()

			// First insert
			obj1 := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  1000,
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   "test-upsert.txt",
				Size:   1024,
				Mode:   0o644,
			}

			dataInfo1 := &DataInfo{
				ID:       did,
				Size:     1024,
				OrigSize: 1024,
				XXH3:     0x1111111111111111,
			}

			err := dma.PutDataAndObj(testCtx, testBktID, []*DataInfo{dataInfo1}, []*ObjectInfo{obj1})
			So(err, ShouldBeNil)

			// Get the ROWID after first insert
			bktDirPath := filepath.Join(dma.DefaultDataMetadataAdapter.dataPath, fmt.Sprint(testBktID))
			db, err := GetReadDB(bktDirPath)
			So(err, ShouldBeNil)

			var originalRowID int64
			err = db.QueryRow("SELECT rowid FROM obj WHERE id = ?", id).Scan(&originalRowID)
			So(err, ShouldBeNil)

			var originalDataRowID int64
			err = db.QueryRow("SELECT rowid FROM data WHERE id = ?", did).Scan(&originalDataRowID)
			So(err, ShouldBeNil)

			// Second insert with same ID (conflict) - should update not replace
			obj2 := &ObjectInfo{
				ID:     id, // Same ID - will conflict
				PID:    pid,
				MTime:  2000, // Different mtime
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   "test-upsert-updated.txt", // Different name
				Size:   2048,                      // Different size
				Mode:   0o755,
			}

			dataInfo2 := &DataInfo{
				ID:       did, // Same ID - will conflict
				Size:     2048,
				OrigSize: 2048,
				XXH3:     0x2222222222222222, // Different hash
			}

			err = dma.PutDataAndObj(testCtx, testBktID, []*DataInfo{dataInfo2}, []*ObjectInfo{obj2})
			So(err, ShouldBeNil)

			// Get the ROWID after second insert
			var newRowID int64
			err = db.QueryRow("SELECT rowid FROM obj WHERE id = ?", id).Scan(&newRowID)
			So(err, ShouldBeNil)

			var newDataRowID int64
			err = db.QueryRow("SELECT rowid FROM data WHERE id = ?", did).Scan(&newDataRowID)
			So(err, ShouldBeNil)

			// With INSERT ON CONFLICT DO UPDATE, ROWID should remain the same
			So(newRowID, ShouldEqual, originalRowID)
			So(newDataRowID, ShouldEqual, originalDataRowID)

			// Verify data was updated
			objs, err := dma.GetObj(testCtx, testBktID, []int64{id})
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 1)
			So(objs[0].Name, ShouldEqual, "test-upsert-updated.txt")
			So(objs[0].Size, ShouldEqual, int64(2048))
			So(objs[0].MTime, ShouldEqual, int64(2000))

			// Verify DataInfo was updated
			dataInfo, err := dma.GetData(testCtx, testBktID, did)
			So(err, ShouldBeNil)
			So(dataInfo, ShouldNotBeNil)
			So(dataInfo.Size, ShouldEqual, int64(2048))
			So(dataInfo.XXH3, ShouldEqual, uint64(0x2222222222222222))
		})


		Convey("SetObj should return decrypted name when updating name", func() {
			// First create an object
			id, _ := ig.New()
			pid, _ := ig.New()
			did, _ := ig.New()

			obj := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   "original-name.txt",
				Size:   1024,
				Mode:   0o644,
			}

			_, err := dma.PutObj(testCtx, testBktID, []*ObjectInfo{obj})
			So(err, ShouldBeNil)

			// Now rename it
			newName := "renamed-file.txt"
			renameObj := &ObjectInfo{
				ID:   id,
				Name: newName,
			}

			err = dma.SetObj(testCtx, testBktID, []string{"n"}, renameObj)
			So(err, ShouldBeNil)

			// After SetObj returns, object should have decrypted name
			So(renameObj.Name, ShouldEqual, newName)
			So(renameObj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0))
		})
	})
}

// TestListOperationsReturnDecryptedNames tests that all List operations return decrypted names
func TestListOperationsReturnDecryptedNames(t *testing.T) {
	Convey("List Operations Return Decrypted Names", t, func() {
		tmpDir, err := os.MkdirTemp("", "orcas_list_decrypt_test_*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(tmpDir)

		testBktID := int64(666)
		encryptionKey := "test-list-decrypt-key"
		testCtx := context.Background()

		err = InitBucketDB(tmpDir, testBktID)
		So(err, ShouldBeNil)

		dma := &DefaultMetadataAdapter{
			DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{},
			DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{},
		}
		dma.DefaultDataMetadataAdapter.SetDataPath(tmpDir)
		dma.DefaultDataMetadataAdapter.SetDataKey(encryptionKey)

		ig := idgen.NewIDGen(nil, 0)
		pid, _ := ig.New()

		// Create multiple test files
		testFiles := []string{
			"file1.txt",
			"file2.txt",
			"file3.txt",
		}

		fileIDs := make([]int64, len(testFiles))
		for i, name := range testFiles {
			id, _ := ig.New()
			did, _ := ig.New()
			fileIDs[i] = id

			obj := &ObjectInfo{
				ID:     id,
				PID:    pid,
				MTime:  Now(),
				DataID: did,
				Type:   OBJ_TYPE_FILE,
				Name:   name,
				Size:   100,
				Mode:   0o644,
			}

			_, err := dma.PutObj(testCtx, testBktID, []*ObjectInfo{obj})
			So(err, ShouldBeNil)
		}

		Convey("ListObj should return decrypted names", func() {
			objs, cnt, _, err := dma.ListObj(testCtx, testBktID, pid, "", "", "", 10)
			So(err, ShouldBeNil)
			So(cnt, ShouldEqual, int64(len(testFiles)))

			for _, obj := range objs {
				So(obj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0))
				// Verify name is in the original list
				found := false
				for _, originalName := range testFiles {
					if obj.Name == originalName {
						found = true
						break
					}
				}
				So(found, ShouldBeTrue)
			}
		})

		Convey("ListChildren should return decrypted names", func() {
			children, cnt, err := dma.ListChildren(testCtx, testBktID, pid, 0, 10)
			So(err, ShouldBeNil)
			So(cnt, ShouldEqual, int64(len(testFiles)))

			for _, child := range children {
				So(child.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0))
				found := false
				for _, originalName := range testFiles {
					if child.Name == originalName {
						found = true
						break
					}
				}
				So(found, ShouldBeTrue)
			}
		})

		Convey("ListObjsByType should return decrypted names", func() {
			objs, cnt, err := dma.ListObjsByType(testCtx, testBktID, OBJ_TYPE_FILE, 0, 10)
			So(err, ShouldBeNil)
			So(cnt, ShouldBeGreaterThanOrEqualTo, int64(len(testFiles)))

			for _, obj := range objs {
				So(obj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0))
			}
		})

		Convey("GetObjByDataID should return decrypted names", func() {
			// Get one file's dataID
			objs, err := dma.GetObj(testCtx, testBktID, []int64{fileIDs[0]})
			So(err, ShouldBeNil)
			So(len(objs), ShouldEqual, 1)
			dataID := objs[0].DataID

			// Query by dataID
			objsByDataID, err := dma.GetObjByDataID(testCtx, testBktID, dataID)
			So(err, ShouldBeNil)
			So(len(objsByDataID), ShouldBeGreaterThan, 0)

			for _, obj := range objsByDataID {
				So(obj.Mode&MODE_NAME_ENCRYPTED, ShouldEqual, uint32(0))
			}
		})
	})
}

// TestPutACLUpsert tests PutACL uses upsert (not replace) behavior
func TestPutACLUpsert(t *testing.T) {
	Convey("PutACL Upsert", t, func() {
		baseDir, _, cleanup := SetupTestDirs("test_put_acl")
		defer cleanup()
		InitDB(baseDir, "")
		dba := &DefaultBaseMetadataAdapter{}
		dba.SetPath(baseDir)
		ctx := context.Background()
		ig := idgen.NewIDGen(nil, 0)
		bktID, _ := ig.New()
		uid, _ := ig.New()

		Convey("PutACL insert then update", func() {
			err := dba.PutACL(ctx, bktID, uid, 1)
			So(err, ShouldBeNil)
			acls, err := dba.ListACL(ctx, bktID)
			So(err, ShouldBeNil)
			So(len(acls), ShouldEqual, 1)
			So(acls[0].Perm, ShouldEqual, 1)

			// Update same ACL entry
			err = dba.PutACL(ctx, bktID, uid, 2)
			So(err, ShouldBeNil)
			acls, err = dba.ListACL(ctx, bktID)
			So(err, ShouldBeNil)
			So(len(acls), ShouldEqual, 1)
			So(acls[0].Perm, ShouldEqual, 2)
		})
	})
}

// TestPutUsrUpsert tests PutUsr uses upsert (not replace) behavior
func TestPutUsrUpsert(t *testing.T) {
	Convey("PutUsr Upsert", t, func() {
		baseDir, _, cleanup := SetupTestDirs("test_put_usr")
		defer cleanup()
		InitDB(baseDir, "")
		dba := &DefaultBaseMetadataAdapter{}
		dba.SetPath(baseDir)
		ctx := context.Background()
		ig := idgen.NewIDGen(nil, 0)
		userID, _ := ig.New()

		Convey("PutUsr insert then update", func() {
			usr := &UserInfo{
				ID:     userID,
				Usr:    "testuser",
				Pwd:    "password1",
				Role:   0,
				Name:   "Test User",
				Avatar: "",
			}
			err := dba.PutUsr(ctx, usr)
			So(err, ShouldBeNil)

			users, err := dba.GetUsr(ctx, []int64{userID})
			So(err, ShouldBeNil)
			So(len(users), ShouldEqual, 1)
			So(users[0].Name, ShouldEqual, "Test User")
			So(users[0].Pwd, ShouldEqual, "password1")

			// Update same user
			usr.Name = "Updated User"
			usr.Pwd = "password2"
			err = dba.PutUsr(ctx, usr)
			So(err, ShouldBeNil)

			users, err = dba.GetUsr(ctx, []int64{userID})
			So(err, ShouldBeNil)
			So(len(users), ShouldEqual, 1)
			So(users[0].Name, ShouldEqual, "Updated User")
			So(users[0].Pwd, ShouldEqual, "password2")
		})
	})
}
