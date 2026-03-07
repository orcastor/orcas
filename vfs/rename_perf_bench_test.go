package vfs

import (
	"context"
	"fmt"
	"testing"

	"github.com/orca-zhang/idgen"
	"github.com/orcastor/orcas/core"
)

func setupTmpRenameBenchFS(tb testing.TB) (*OrcasFS, core.Handler, core.Ctx, int64, *OrcasNode) {
	tb.Helper()

	ensureTestUserForBenchmark(tb)
	handler := core.NewLocalHandler("", "")
	ctx := context.Background()
	ctx, _, _, err := handler.Login(ctx, "orcas", "orcas")
	if err != nil {
		tb.Fatalf("Login failed: %v", err)
	}

	ig := idgen.NewIDGen(nil, 0)
	testBktID, _ := ig.New()
	if err := core.InitBucketDB(".", testBktID); err != nil {
		tb.Fatalf("InitBucketDB failed: %v", err)
	}

	admin := core.NewLocalAdmin(".", ".")
	bkt := &core.BucketInfo{
		ID:        testBktID,
		Name:      "bench-tmp-rename-bucket",
		Type:      1,
		Quota:     -1,
		ChunkSize: 4 * 1024 * 1024,
	}
	if err := admin.PutBkt(ctx, []*core.BucketInfo{bkt}); err != nil {
		tb.Fatalf("PutBkt failed: %v", err)
	}

	ofs := NewOrcasFS(handler, ctx, testBktID)
	if ofs.root == nil {
		ofs.root = &OrcasNode{fs: ofs, objID: testBktID, isRoot: true}
	}
	return ofs, handler, ctx, testBktID, ofs.root
}

func createBenchFile(tb testing.TB, handler core.Handler, ctx core.Ctx, bktID, parentID int64, name string) int64 {
	tb.Helper()
	fileObj := &core.ObjectInfo{
		ID:    core.NewID(),
		PID:   parentID,
		Type:  core.OBJ_TYPE_FILE,
		Name:  name,
		Size:  0,
		MTime: core.Now(),
	}
	if _, err := handler.Put(ctx, bktID, []*core.ObjectInfo{fileObj}); err != nil {
		tb.Fatalf("Put failed for %s: %v", name, err)
	}
	return fileObj.ID
}

// BenchmarkTmpRenameNoConflict measures .tmp -> final rename without existing target conflict.
// This path should benefit from skipping target directory scans in the optimized implementation.
func BenchmarkTmpRenameNoConflict(b *testing.B) {
	ofs, handler, ctx, testBktID, root := setupTmpRenameBenchFS(b)
	payload := []byte("bench-payload-for-tmp-rename")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmpName := fmt.Sprintf("bench-%d.tmp", i)
		finalName := fmt.Sprintf("bench-%d.bin", i)
		fileObj := &core.ObjectInfo{
			ID:    core.NewID(),
			PID:   testBktID,
			Type:  core.OBJ_TYPE_FILE,
			Name:  tmpName,
			Size:  0,
			MTime: core.Now(),
		}
		if _, err := handler.Put(ctx, testBktID, []*core.ObjectInfo{fileObj}); err != nil {
			b.Fatalf("Put failed at iter=%d: %v", i, err)
		}

		ra, err := NewRandomAccessor(ofs, fileObj.ID)
		if err != nil {
			b.Fatalf("NewRandomAccessor failed at iter=%d: %v", i, err)
		}
		ofs.registerRandomAccessor(fileObj.ID, ra)

		if err := ra.Write(0, payload); err != nil {
			b.Fatalf("Write failed at iter=%d: %v", i, err)
		}

		errno := root.Rename(context.Background(), tmpName, root, finalName, 0)
		if errno != 0 {
			b.Fatalf("Rename failed at iter=%d: errno=%d", i, errno)
		}

		ofs.unregisterRandomAccessor(fileObj.ID, ra)
		if err := ra.Close(); err != nil {
			b.Fatalf("Close failed at iter=%d: %v", i, err)
		}
	}
}

// BenchmarkRenameLargeDirNoConflict measures same-directory rename when parent directory
// has many unrelated children. This highlights source/target lookup metadata overhead.
func BenchmarkRenameLargeDirNoConflict(b *testing.B) {
	_, handler, ctx, testBktID, root := setupTmpRenameBenchFS(b)
	const fillerCount = 2000

	for i := 0; i < fillerCount; i++ {
		name := fmt.Sprintf("filler-%05d.dat", i)
		createBenchFile(b, handler, ctx, testBktID, testBktID, name)
	}

	createBenchFile(b, handler, ctx, testBktID, testBktID, "hot-a.dat")
	currentName := "hot-a.dat"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nextName := "hot-b.dat"
		if currentName == "hot-b.dat" {
			nextName = "hot-a.dat"
		}
		errno := root.Rename(context.Background(), currentName, root, nextName, 0)
		if errno != 0 {
			b.Fatalf("Rename failed at iter=%d: %s -> %s errno=%d", i, currentName, nextName, errno)
		}
		currentName = nextName
	}
}

// BenchmarkTmpRenameWithConflict measures .tmp -> final rename when target already exists
// and merge/conflict handling is required.
func BenchmarkTmpRenameWithConflict(b *testing.B) {
	ofs, handler, ctx, testBktID, root := setupTmpRenameBenchFS(b)
	targetData := []byte("target-old-data")
	tmpData := []byte("tmp-new-data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		finalName := fmt.Sprintf("bench-conflict-%d.bin", i)
		tmpName := finalName + ".tmp"

		targetObj := &core.ObjectInfo{ID: core.NewID(), PID: testBktID, Type: core.OBJ_TYPE_FILE, Name: finalName, MTime: core.Now()}
		if _, err := handler.Put(ctx, testBktID, []*core.ObjectInfo{targetObj}); err != nil {
			b.Fatalf("Put target failed at iter=%d: %v", i, err)
		}
		targetRA, err := NewRandomAccessor(ofs, targetObj.ID)
		if err != nil {
			b.Fatalf("NewRandomAccessor target failed at iter=%d: %v", i, err)
		}
		ofs.registerRandomAccessor(targetObj.ID, targetRA)
		if err := targetRA.Write(0, targetData); err != nil {
			b.Fatalf("Write target failed at iter=%d: %v", i, err)
		}
		if _, err := targetRA.ForceFlush(); err != nil {
			b.Fatalf("ForceFlush target failed at iter=%d: %v", i, err)
		}
		ofs.unregisterRandomAccessor(targetObj.ID, targetRA)
		if err := targetRA.Close(); err != nil {
			b.Fatalf("Close target failed at iter=%d: %v", i, err)
		}

		tmpObj := &core.ObjectInfo{ID: core.NewID(), PID: testBktID, Type: core.OBJ_TYPE_FILE, Name: tmpName, MTime: core.Now()}
		if _, err := handler.Put(ctx, testBktID, []*core.ObjectInfo{tmpObj}); err != nil {
			b.Fatalf("Put tmp failed at iter=%d: %v", i, err)
		}
		tmpRA, err := NewRandomAccessor(ofs, tmpObj.ID)
		if err != nil {
			b.Fatalf("NewRandomAccessor tmp failed at iter=%d: %v", i, err)
		}
		ofs.registerRandomAccessor(tmpObj.ID, tmpRA)
		if err := tmpRA.Write(0, tmpData); err != nil {
			b.Fatalf("Write tmp failed at iter=%d: %v", i, err)
		}
		if _, err := tmpRA.ForceFlush(); err != nil {
			b.Fatalf("ForceFlush tmp failed at iter=%d: %v", i, err)
		}

		errno := root.Rename(context.Background(), tmpName, root, finalName, 0)
		if errno != 0 {
			b.Fatalf("Rename conflict failed at iter=%d: errno=%d", i, errno)
		}

		ofs.unregisterRandomAccessor(tmpObj.ID, tmpRA)
		if err := tmpRA.Close(); err != nil {
			b.Fatalf("Close tmp failed at iter=%d: %v", i, err)
		}
	}
}
