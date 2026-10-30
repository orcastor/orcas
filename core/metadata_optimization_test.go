package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func optimizationMetadata(t *testing.T) (*DefaultMetadataAdapter, *sql.DB, int64) {
	t.Helper()
	path := t.TempDir()
	const bucket = int64(17)
	if err := InitBucketDB(path, bucket); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { GetDBPool().ForceReleaseDB(path) })
	dma := NewDefaultMetadataAdapter()
	dma.SetDataPath(path)
	db, err := GetWriteDB(filepath.Join(path, fmt.Sprint(bucket)))
	if err != nil {
		t.Fatal(err)
	}
	return dma, db, bucket
}

func TestMetadataOptimizationIndexesAndPagination(t *testing.T) {
	dma, db, bucket := optimizationMetadata(t)
	ctx := context.Background()
	// Reinitialization upgrades existing databases without losing their data.
	objects := []*ObjectInfo{{ID: 1, PID: 50, DataID: 9, Name: "a"}, {ID: 2, PID: 50, DataID: 9, Name: "b"}, {ID: 3, PID: 50, Name: "c"}}
	if _, err := dma.PutObj(ctx, bucket, objects); err != nil {
		t.Fatal(err)
	}
	if err := InitBucketDB(dma.dataPath, bucket); err != nil {
		t.Fatal(err)
	}
	for _, plan := range []struct{ query, index string }{
		{`SELECT * FROM obj WHERE pid=50 AND id>1 ORDER BY id LIMIT 2`, "ix_obj_pid_id"},
		{`SELECT did, count(1) FROM obj WHERE did IN (9,10) AND pid>=0 GROUP BY did`, "ix_obj_did_pid"},
	} {
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+plan.query)
		if err != nil {
			t.Fatal(err)
		}
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			details = append(details, detail)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(details, "\n")
		if !strings.Contains(joined, plan.index) || strings.Contains(joined, "TEMP B-TREE") {
			t.Fatalf("unexpected query plan: %s", joined)
		}
	}
	h := &LocalHandler{ma: dma, acm: &NoAuthAccessCtrlMgr{}}
	first, total, delim, err := h.List(ctx, bucket, 50, ListOptions{Count: 2})
	if err != nil || len(first) != 2 || total != 3 || delim == "" {
		t.Fatalf("public first page: %v %d %q %v", first, total, delim, err)
	}
	second, total, _, err := h.List(ctx, bucket, 50, ListOptions{Count: 2, Delim: delim, SkipCount: true})
	if err != nil || len(second) != 1 || second[0].ID != 3 || total != 0 {
		t.Fatalf("internal second page: %v %d %v", second, total, err)
	}
	_, total, _, err = h.List(ctx, bucket, 50, ListOptions{Count: 0})
	if err != nil || total != 3 {
		t.Fatalf("count-only public listing: %d %v", total, err)
	}
	if _, _, _, err := h.List(ctx, bucket, 50, ListOptions{Count: 2, Order: "invalid", SkipCount: true}); err == nil {
		t.Fatal("internal pagination bypassed order validation")
	}
	h.acm = &optimizationDeniedACL{}
	if _, _, _, err := h.List(ctx, bucket, 50, ListOptions{Count: 2, SkipCount: true}); !errors.Is(err, errOptimizationDenied) {
		t.Fatalf("internal pagination bypassed permissions: %v", err)
	}
}

func TestMetadataOptimizationExistingBucketUpgrade(t *testing.T) {
	dma, db, bucket := optimizationMetadata(t)
	if _, err := db.Exec(`DROP INDEX ix_obj_pid_id`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP INDEX ix_obj_did_pid`); err != nil {
		t.Fatal(err)
	}
	GetDBPool().ForceReleaseDB(dma.dataPath)
	db, err := GetReadDB(filepath.Join(dma.dataPath, fmt.Sprint(bucket)))
	if err != nil {
		t.Fatal(err)
	}
	var indexes int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('ix_obj_pid_id', 'ix_obj_did_pid')`).Scan(&indexes); err != nil || indexes != 2 {
		t.Fatalf("existing bucket upgrade: %d indexes, %v", indexes, err)
	}
}

var errOptimizationDenied = errors.New("denied")

type optimizationDeniedACL struct{ AccessCtrlMgr }

func (*optimizationDeniedACL) CheckPermission(Ctx, int, int64) error { return errOptimizationDenied }

func TestMetadataOptimizationRefDataPooledConnection(t *testing.T) {
	dma, db, bucket := optimizationMetadata(t)
	ctx := context.Background()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	existing := &DataInfo{ID: 101, OrigSize: 100, HdrXXH3: -123, XXH3: -456, SHA256_0: -11, SHA256_1: 22, SHA256_2: -33, SHA256_3: 44}
	if err := dma.PutData(ctx, bucket, []*DataInfo{existing}); err != nil {
		t.Fatal(err)
	}
	full := *existing
	partial := full
	partial.XXH3, partial.SHA256_0 = 0, 0
	missing := full
	missing.HdrXXH3 = 999
	collision := full
	collision.SHA256_3++
	inputs := []*DataInfo{&full, &full, &partial, &missing, &missing, &collision, {OrigSize: 0}, {OrigSize: 100}}
	want := []int64{101, 101, 1, 0, ^int64(3), 0, 0, 0}
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for repeat := 0; repeat < 20; repeat++ {
				got, err := dma.RefData(ctx, bucket, inputs)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Errorf("RefData=%v, err=%v; want %v", got, err, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	// Hold every pooled connection to inspect all private temp schemas.
	var conns []*sql.Conn
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()
	for i := 0; i < 8; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		var remaining int
		if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_temp_master WHERE name LIKE 'tmp_%'`).Scan(&remaining); err != nil || remaining != 0 {
			t.Fatalf("temporary tables leaked: %d, %v", remaining, err)
		}
	}
}
