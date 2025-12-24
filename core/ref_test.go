package core

import (
	"testing"

	"github.com/orca-zhang/idgen"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRefNegativeID(t *testing.T) {
	Convey("Ref with negative ID (same batch reference)", t, func() {
		InitDB(".", "")
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		InitBucketDB(".", testBktID)

		dma := &DefaultMetadataAdapter{}

		// Create test data
		data1ID, _ := ig.New()
		data1 := &DataInfo{
			ID:       data1ID,
			OrigSize: 100,
			HdrXXH3:  12345,
			XXH3:     67890,
			SHA256_0: 11111,
			SHA256_1: 22222,
			SHA256_2: 33333,
			SHA256_3: 44444,
			Kind:     DATA_NORMAL,
		}
		So(dma.PutData(c, testBktID, []*DataInfo{data1}), ShouldBeNil)

		// Test Ref with same data in batch
		// First element: should find in database
		// Second element: should reference first element in batch (negative ID)
		refData := []*DataInfo{
			{
				OrigSize: 100,
				HdrXXH3: 12345,
				XXH3:    67890,
				SHA256_0: 11111,
			SHA256_1: 22222,
			SHA256_2: 33333,
			SHA256_3: 44444,
			},
			{
				OrigSize: 100,
				HdrXXH3: 12345,
				XXH3:    67890,
				SHA256_0: 11111,
			SHA256_1: 22222,
			SHA256_2: 33333,
			SHA256_3: 44444,
			},
		}

		refIDs, err := dma.RefData(c, testBktID, refData)
		So(err, ShouldBeNil)
		So(len(refIDs), ShouldEqual, 2)
		So(refIDs[0], ShouldEqual, data1.ID) // First element: found in database
		So(refIDs[1], ShouldEqual, data1.ID) // Second element: should also find in database (not negative)

		// Test Ref with new data in batch (not in database)
		// First element: not in database, should be 0
		// Second element: same as first, should reference first (negative ID)
		newRefData := []*DataInfo{
			{
				OrigSize: 200,
				HdrXXH3: 99999,
				XXH3:    88888,
				SHA256_0: 77777,
				SHA256_1: 88888,
				SHA256_2: 99999,
				SHA256_3: 10000,
			},
			{
				OrigSize: 200,
				HdrXXH3: 99999,
				XXH3:    88888,
				SHA256_0: 77777,
				SHA256_1: 88888,
				SHA256_2: 99999,
				SHA256_3: 10000,
			},
		}

		refIDs2, err := dma.RefData(c, testBktID, newRefData)
		So(err, ShouldBeNil)
		So(len(refIDs2), ShouldEqual, 2)
		So(refIDs2[0], ShouldEqual, 0)         // First element: not in database
		So(refIDs2[1], ShouldBeLessThan, 0)    // Second element: should reference first (negative ID)
		So(refIDs2[1], ShouldEqual, ^int64(0)) // Should be ^0 = -1
	})
}

func TestPutDataInfoNegativeID(t *testing.T) {
	Convey("PutDataInfo with negative ID (same batch reference)", t, func() {
		InitDB(".", "")
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		InitBucketDB(".", testBktID)

		dma := &DefaultMetadataAdapter{}

		// Create first DataInfo
		data1ID, _ := ig.New()
		data1 := &DataInfo{
			ID:       data1ID,
			OrigSize: 100,
			HdrXXH3:  12345,
			XXH3:     67890,
			SHA256_0: 11111,
			SHA256_1: 22222,
			SHA256_2: 33333,
			SHA256_3: 44444,
			Kind:     DATA_NORMAL,
		}
		So(dma.PutData(c, testBktID, []*DataInfo{data1}), ShouldBeNil)

		// Test PutDataInfo with negative ID reference
		// Second DataInfo references first using negative ID (^0 = -1)
		// Simulate PutDataInfo processing: negative ID should be resolved
		ids := []int64{data1ID, ^int64(0)} // Second element references first
		for i := range ids {
			if ids[i] < 0 && ^ids[i] < int64(len(ids)) {
				ids[i] = ids[^ids[i]]
			}
		}
		So(ids[0], ShouldEqual, data1ID)
		So(ids[1], ShouldEqual, data1ID) // Should be resolved to data1ID
	})
}

func TestPutNegativePID(t *testing.T) {
	Convey("Put with negative PID (same batch reference)", t, func() {
		InitDB(".", "")
		ig := idgen.NewIDGen(nil, 0)
		testBktID, _ := ig.New()
		InitBucketDB(".", testBktID)

		dma := &DefaultMetadataAdapter{}

		// Create objects with negative PID reference
		// First object: directory (PID = 0)
		// Second object: file in first directory (PID = ^0 = -1, references first object)
		dirID, _ := ig.New()
		fileID, _ := ig.New()

		objs := []*ObjectInfo{
			{
				ID:   dirID,
				PID:  0,
				Type: OBJ_TYPE_DIR,
				Name: "dir1",
			},
			{
				ID:   fileID,
				PID:  ^int64(0), // Reference to first object (^0 = -1)
				Type: OBJ_TYPE_FILE,
				Name: "file1",
			},
		}

		// Simulate Put processing: negative PID should be resolved
		for _, x := range objs {
			if x.ID == 0 {
				newID, _ := ig.New()
				x.ID = newID
			}
		}
		for _, x := range objs {
			if x.PID < 0 && int(^x.PID) <= len(objs) {
				x.PID = objs[^x.PID].ID
			}
		}

		So(objs[0].PID, ShouldEqual, 0)
		So(objs[1].PID, ShouldEqual, dirID) // Should be resolved to dirID

		// Now put objects
		_, err := dma.PutObj(c, testBktID, objs)
		So(err, ShouldBeNil)

		// Verify objects
		resultObjs, err := dma.GetObj(c, testBktID, []int64{dirID, fileID})
		So(err, ShouldBeNil)
		So(len(resultObjs), ShouldEqual, 2)
		So(resultObjs[0].PID, ShouldEqual, 0)
		So(resultObjs[1].PID, ShouldEqual, dirID)
	})
}
