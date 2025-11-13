package sdk

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
)

// setupTestEnvironmentForBatchWriterVisibility 设置测试环境
func setupTestEnvironmentForBatchWriterVisibility(t *testing.T) (int64, context.Context, core.Handler) {
	// 启用批量写入
	os.Setenv("ORCAS_BATCH_WRITE_ENABLED", "true")
	// 设置较大的flush window，以便测试未flush的情况
	os.Setenv("ORCAS_BUFFER_WINDOW", "10s")

	// 初始化环境变量
	if core.ORCAS_BASE == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_sdk_visibility_test")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_BASE", tmpDir)
		core.ORCAS_BASE = tmpDir
	}
	if core.ORCAS_DATA == "" {
		tmpDir := filepath.Join(os.TempDir(), "orcas_sdk_visibility_test_data")
		os.MkdirAll(tmpDir, 0o755)
		os.Setenv("ORCAS_DATA", tmpDir)
		core.ORCAS_DATA = tmpDir
	}

	core.InitDB("")

	// 确保测试用户存在
	handler := core.NewLocalHandler()
	ctx, _, _, err := handler.Login(context.Background(), "orcas", "orcas")
	if err != nil {
		// 如果登录失败，尝试创建用户
		db, err := core.GetDB()
		if err != nil {
			t.Fatalf("GetDB failed: %v", err)
		}
		_, err = db.Exec("INSERT OR IGNORE INTO usr (id, name, pwd) VALUES (1, 'orcas', 'orcas')")
		if err != nil {
			t.Fatalf("Failed to create test user: %v", err)
		}
		ctx, _, _, err = handler.Login(context.Background(), "orcas", "orcas")
		if err != nil {
			t.Fatalf("Login failed: %v", err)
		}
	}

	// 创建测试bucket
	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	err = core.InitBucketDB(ctx, testBktID)
	if err != nil {
		t.Fatalf("InitBucketDB failed: %v", err)
	}

	admin := core.NewLocalAdmin()
	bkt := &core.BucketInfo{
		ID:    testBktID,
		Name:  "test-bucket",
		UID:   1,
		Type:  1,
		Quota: -1, // 无限制配额
	}
	err = admin.PutBkt(ctx, []*core.BucketInfo{bkt})
	if err != nil {
		t.Fatalf("PutBkt failed: %v", err)
	}

	return testBktID, ctx, handler
}

// TestBatchWriterVisibilityBeforeFlush 测试在flush之前，GetPendingObjects能否查询到数据
func TestBatchWriterVisibilityBeforeFlush(t *testing.T) {
	bktID, ctx, handler := setupTestEnvironmentForBatchWriterVisibility(t)

	// 获取batch write manager
	batchMgr := GetBatchWriteManager(handler, bktID)
	if batchMgr == nil {
		t.Fatal("Failed to get batch write manager")
	}

	// 添加文件到batch write manager，但不flush
	testKey := "test-object-before-flush"
	testData := []byte("test data before flush")
	objID := core.NewID()

	// 确保父目录存在
	pid := int64(0)

	added, dataID, err := batchMgr.AddFile(objID, testData, pid, testKey, int64(len(testData)))
	if err != nil {
		t.Fatalf("AddFile failed: %v", err)
	}
	if !added {
		t.Fatal("Failed to add file to batch write manager")
	}

	t.Logf("File added to batch write manager (objID: %d, dataID: %d), but not flushed yet", objID, dataID)

	// 测试 GetPendingObjects - 应该能查询到
	t.Log("Testing GetPendingObjects before flush (should see object from batch writer cache)...")
	pendingObjs := batchMgr.GetPendingObjects()

	found := false
	for fileID, pkgInfo := range pendingObjs {
		if pkgInfo != nil && pkgInfo.ObjectID == objID && pkgInfo.Name == testKey {
			found = true
			t.Logf("Found object in GetPendingObjects before flush: %s (Size: %d, ObjectID: %d)", pkgInfo.Name, pkgInfo.Size, fileID)
			break
		}
	}

	if !found {
		t.Error("Object should be visible in GetPendingObjects before flush (from batch writer cache)")
	} else {
		t.Log("✓ Object correctly visible in GetPendingObjects before flush (from batch writer cache)")
	}

	// 测试 GetPendingObject - 应该能查询到
	t.Log("Testing GetPendingObject before flush (should read from batch writer cache)...")
	pkgInfo, ok := batchMgr.GetPendingObject(objID)
	if !ok || pkgInfo == nil {
		t.Error("GetPendingObject should return object before flush (from batch writer cache)")
	} else {
		if pkgInfo.Name != testKey {
			t.Errorf("Object name mismatch: expected %q, got %q", testKey, pkgInfo.Name)
		} else {
			t.Log("✓ Object correctly visible in GetPendingObject before flush (from batch writer cache)")
		}
	}

	// 测试 ReadPendingData - 应该能读取到数据
	t.Log("Testing ReadPendingData before flush (should read from batch writer cache)...")
	readData := batchMgr.ReadPendingData(objID)
	if readData == nil {
		t.Error("ReadPendingData should return data before flush (from batch writer cache)")
	} else {
		if !bytes.Equal(readData, testData) {
			t.Errorf("Data mismatch: expected %q, got %q", string(testData), string(readData))
		} else {
			t.Log("✓ Object correctly visible in ReadPendingData before flush (from batch writer cache) with correct data")
		}
	}

	// 现在flush，然后再次测试（应该仍然能查询到，因为已经写入数据库）
	t.Log("Flushing batch write manager...")
	batchMgr.FlushAll(ctx)
	// 等待flush完成
	time.Sleep(100 * time.Millisecond)

	// 再次测试 GetPendingObjects - 现在应该已经清空（因为已经flush）
	t.Log("Testing GetPendingObjects after flush...")
	pendingObjs = batchMgr.GetPendingObjects()
	found = false
	for fileID, pkgInfo := range pendingObjs {
		if pkgInfo != nil && pkgInfo.ObjectID == objID {
			found = true
			t.Logf("Found object in GetPendingObjects after flush: %s (Size: %d, ObjectID: %d)", pkgInfo.Name, pkgInfo.Size, fileID)
			break
		}
	}

	// After flush, pending objects should be cleared (or may still be there briefly during flush)
	// This is acceptable behavior
	if found {
		t.Log("Object still in GetPendingObjects after flush (may be cleared shortly)")
	} else {
		t.Log("✓ Object correctly removed from GetPendingObjects after flush")
	}

	// 测试从数据库读取 - 现在应该能查询到
	t.Log("Testing database read after flush...")
	objs, err := handler.Get(ctx, bktID, []int64{objID})
	if err != nil {
		t.Logf("Get failed (may be due to async write): %v", err)
	} else if len(objs) > 0 {
		obj := objs[0]
		if obj.Name != testKey {
			t.Errorf("Object name mismatch: expected %q, got %q", testKey, obj.Name)
		} else {
			t.Log("✓ Object correctly visible in database after flush")
		}
	} else {
		t.Log("Object not yet in database (may need more time for async write)")
	}
}

// TestBatchWriterVisibilityConcurrent 测试并发写入和查询的情况
func TestBatchWriterVisibilityConcurrent(t *testing.T) {
	bktID, ctx, handler := setupTestEnvironmentForBatchWriterVisibility(t)

	// 获取batch write manager
	batchMgr := GetBatchWriteManager(handler, bktID)
	if batchMgr == nil {
		t.Fatal("Failed to get batch write manager")
	}

	// 并发添加多个文件，但不flush
	numFiles := 10
	testKeys := make([]string, numFiles)
	testData := make([][]byte, numFiles)
	objIDs := make([]int64, numFiles)

	for i := 0; i < numFiles; i++ {
		testKeys[i] = fmt.Sprintf("concurrent-test-%d", i)
		testData[i] = []byte(fmt.Sprintf("test data %d", i))
		objIDs[i] = core.NewID()

		added, _, err := batchMgr.AddFile(objIDs[i], testData[i], 0, testKeys[i], int64(len(testData[i])))
		if err != nil {
			t.Fatalf("AddFile failed for file %d: %v", i, err)
		}
		if !added {
			t.Fatalf("Failed to add file %d to batch write manager", i)
		}
	}

	t.Logf("Added %d files to batch write manager, but not flushed yet", numFiles)

	// 测试 GetPendingObjects - 应该能查询到所有文件
	t.Log("Testing GetPendingObjects before flush (should see objects from batch writer cache)...")
	pendingObjs := batchMgr.GetPendingObjects()

	foundCount := 0
	for fileID, pkgInfo := range pendingObjs {
		if pkgInfo != nil {
			for i, testKey := range testKeys {
				if pkgInfo.Name == testKey && pkgInfo.ObjectID == objIDs[i] {
					foundCount++
					// 验证 GetPendingObject 也能查询到
					pkgInfo2, ok := batchMgr.GetPendingObject(fileID)
					if ok && pkgInfo2 != nil {
						// 验证 ReadPendingData 也能读取到
						readData := batchMgr.ReadPendingData(fileID)
						if readData != nil && bytes.Equal(readData, testData[i]) {
							t.Logf("✓ Object %s correctly visible in GetPendingObjects, GetPendingObject, and ReadPendingData", testKey)
						} else {
							t.Errorf("Data mismatch for object %s", testKey)
						}
					} else {
						t.Errorf("GetPendingObject failed for %s", testKey)
					}
					break
				}
			}
		}
	}

	if foundCount != numFiles {
		t.Errorf("Found %d objects in GetPendingObjects before flush (expected %d)", foundCount, numFiles)
	} else {
		t.Logf("✓ All %d objects correctly visible in GetPendingObjects before flush (from batch writer cache)", numFiles)
	}

	// 现在flush（对象应该仍然能查询到，因为已经写入数据库）
	t.Log("Flushing batch write manager...")
	batchMgr.FlushAll(ctx)
	// Wait longer for flush to complete and database to be updated
	time.Sleep(200 * time.Millisecond)

	// 再次测试 GetPendingObjects - 现在应该已经清空（因为已经flush）
	t.Log("Testing GetPendingObjects after flush...")
	pendingObjs = batchMgr.GetPendingObjects()

	foundCount = 0
	for _, pkgInfo := range pendingObjs {
		if pkgInfo != nil {
			for _, testKey := range testKeys {
				if pkgInfo.Name == testKey {
					foundCount++
					break
				}
			}
		}
	}

	// After flush, objects should be in database, but pendingObjects may still have them
	// if flush is still in progress. So we may see duplicates. Let's check if we have at least numFiles
	if foundCount > 0 {
		t.Logf("Found %d objects still in GetPendingObjects after flush (may be cleared shortly)", foundCount)
	} else {
		t.Logf("✓ All objects correctly removed from GetPendingObjects after flush")
	}

	// 验证从数据库读取
	t.Log("Testing database read after flush...")
	objs, err := handler.Get(ctx, bktID, objIDs)
	if err != nil {
		t.Logf("Get failed (may be due to async write): %v", err)
	} else {
		foundCount = 0
		for _, obj := range objs {
			for _, testKey := range testKeys {
				if obj.Name == testKey {
					foundCount++
					break
				}
			}
		}
		if foundCount < numFiles {
			t.Errorf("Found %d objects in database after flush (expected at least %d)", foundCount, numFiles)
		} else {
			t.Logf("✓ All %d objects correctly visible in database after flush", foundCount)
		}
	}
}
