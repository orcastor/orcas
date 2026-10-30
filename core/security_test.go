package core

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/pbkdf2"
)

func TestSecurityPasswordHashes(t *testing.T) {
	password := "a long test password"
	encoded, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	if encoded == second || !strings.HasPrefix(encoded, "pbkdf2-sha256:600000:") {
		t.Fatal("password hashes must use strong parameters and independent salts")
	}
	if ok, legacy := verifyPassword(password, encoded); !ok || legacy {
		t.Fatal("new password rejected")
	}
	if ok, _ := verifyPassword("wrong", encoded); ok {
		t.Fatal("wrong password accepted")
	}
	if _, err := HashPassword(""); err == nil {
		t.Fatal("empty password accepted")
	}
	parts := strings.Split(encoded, ":")
	for _, bad := range []string{"", "1::", "-1:" + parts[2] + ":" + parts[3],
		"pbkdf2-sha256:999999999999:" + parts[2] + ":" + parts[3],
		"pbkdf2-sha256:600000:invalid:" + parts[3],
		"pbkdf2-sha256:600000:" + parts[2] + ":invalid", encoded + ":extra"} {
		if ok, _ := verifyPassword(password, bad); ok {
			t.Fatalf("malformed hash accepted: %q", bad)
		}
	}
}

func TestSecurityBootstrapAndLegacyLogin(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ORCAS_ADMIN_PASSWORD", "")
	if err := InitDB(dir, ""); err == nil {
		t.Fatal("new DB silently created a default administrator")
	}
	t.Setenv("ORCAS_ADMIN_PASSWORD", "explicit bootstrap password")
	if err := InitDB(dir, ""); err != nil {
		t.Fatal(err)
	}
	handler := NewLocalHandler(dir, dir)
	defer handler.Close()
	if _, _, _, err := handler.Login(context.Background(), "orcas", "orcas"); err == nil {
		t.Fatal("well-known password still works")
	}
	if _, u, _, err := handler.Login(context.Background(), "orcas", "explicit bootstrap password"); err != nil || u.Pwd != "" {
		t.Fatalf("bootstrap login failed or exposed password: %v", err)
	}
	t.Setenv("ORCAS_ADMIN_PASSWORD", "")
	if err := InitDB(dir, ""); err != nil {
		t.Fatalf("existing DB unexpectedly requires bootstrap: %v", err)
	}

	salt := []byte("0123456789abcdef")
	legacy := fmt.Sprintf("1000:%s:%s", base64.URLEncoding.EncodeToString(salt),
		base64.URLEncoding.EncodeToString(pbkdf2.Key([]byte("legacy password"), salt, 1000, 16, sha1.New)))
	ma := handler.MetadataAdapter()
	if err := ma.PutUsr(context.Background(), &UserInfo{ID: 7, Usr: "legacy", Pwd: legacy}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := handler.Login(context.Background(), "legacy", "wrong"); err == nil {
		t.Fatal("bad legacy password accepted")
	}
	if _, u, _, err := handler.Login(context.Background(), "legacy", "legacy password"); err != nil || u.Pwd != "" {
		t.Fatalf("legacy login failed or exposed password: %v", err)
	}
	u, err := ma.GetUsr2(context.Background(), "legacy")
	if err != nil || !strings.HasPrefix(u.Pwd, "pbkdf2-sha256:") {
		t.Fatalf("legacy hash not upgraded: %v", err)
	}
	currentHash := u.Pwd
	adapter := ma.(*DefaultMetadataAdapter)
	if err := adapter.UpgradePassword(context.Background(), u.ID, legacy, "stale migration"); err != nil {
		t.Fatal(err)
	}
	after, err := ma.GetUsr2(context.Background(), "legacy")
	if err != nil || after.Pwd != currentHash {
		t.Fatal("stale migration overwrote a newer password")
	}

	if _, _, _, err := handler.Login(context.Background(), "legacy", "legacy password"); err != nil {
		t.Fatal(err)
	}
}

func TestSecurityAuthorizationRevocation(t *testing.T) {
	t.Setenv("ORCAS_ADMIN_PASSWORD", "test bootstrap password")
	dir := t.TempDir()
	if err := InitDB(dir, ""); err != nil {
		t.Fatal(err)
	}
	handler := NewLocalHandler(dir, dir)
	defer handler.Close()
	ma := handler.MetadataAdapter()
	user := &UserInfo{ID: 42, Usr: "regular", Role: USER, Pwd: "unused"}
	ctx := context.Background()
	if err := ma.PutUsr(ctx, user); err != nil {
		t.Fatal(err)
	}
	acm := &DefaultAccessCtrlMgr{ma: ma}
	forged := UserInfo2Ctx(ctx, &UserInfo{ID: 42, Role: ADMIN})
	if err := acm.CheckRole(forged, ADMIN); err == nil {
		t.Fatal("context role bypassed the user database")
	}
	user.Role = ADMIN
	if err := ma.SetUsr(ctx, []string{"role"}, user); err != nil {
		t.Fatal(err)
	}
	if err := acm.CheckRole(forged, ADMIN); err != nil {
		t.Fatal(err)
	}
	user.Role = USER
	if err := ma.SetUsr(ctx, []string{"role"}, user); err != nil {
		t.Fatal(err)
	}
	if err := acm.CheckRole(forged, ADMIN); err == nil {
		t.Fatal("demoted administrator retained access")
	}
	if err := ma.PutACL(ctx, 99, 42, ALL); err != nil {
		t.Fatal(err)
	}
	if err := acm.CheckPermission(forged, READ, 99); err != nil {
		t.Fatal(err)
	}
	if err := ma.DeleteUser(ctx, 42); err != nil {
		t.Fatal(err)
	}
	if err := acm.CheckPermission(forged, READ, 99); err == nil {
		t.Fatal("deleted user retained bucket access")
	}
	acls, err := ma.ListACLByUser(ctx, 42)
	if err != nil || len(acls) != 0 {
		t.Fatalf("deleted user's ACLs remain: %v", err)
	}
	// Even an orphaned ACL must not grant access to a deleted identity.
	if err := ma.PutACL(ctx, 99, 42, ALL); err != nil {
		t.Fatal(err)
	}
	if err := acm.CheckPermission(forged, READ, 99); err == nil {
		t.Fatal("orphaned ACL granted access")
	}
}

func TestSecurityPrivateStorageFiles(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "private", ".db")
	if err := prepareDatabaseFile(dbPath); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dbPath)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("new database is accessible to other users: %v", err)
	}
	adapter := &DefaultDataAdapter{dataPath: dir}
	if err := adapter.Write(context.Background(), 1, 2, 0, []byte("private content")); err != nil {
		t.Fatal(err)
	}
	path := toFilePath(dir, 1, 2, 0)
	info, err = os.Stat(path)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("new data file is accessible to other users: %v", err)
	}
	info, err = os.Stat(filepath.Dir(path))
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("new data directory is accessible to other users: %v", err)
	}
}

func TestSecuritySortInjection(t *testing.T) {
	dir := t.TempDir()
	if err := InitBucketDB(dir, 100); err != nil {
		t.Fatal(err)
	}
	adapter := &DefaultMetadataAdapter{DefaultBaseMetadataAdapter: &DefaultBaseMetadataAdapter{}, DefaultDataMetadataAdapter: &DefaultDataMetadataAdapter{}}
	adapter.DefaultDataMetadataAdapter.SetPath(dir)
	db, err := GetWriteDB(filepath.Join(dir, "100"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO obj (id, pid, m, did, t, n, s, md, e) VALUES (1, 0, 0, 0, 2, 'safe', 0, 0, '')`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, order := range []string{"id desc", "id, random()", "(SELECT 1)", "id; DELETE FROM obj;--", "`id`", "-", "+-id"} {
		if _, _, _, err := adapter.ListObj(ctx, 100, 0, "", "", order, 10); err == nil {
			t.Fatalf("unsafe sort order accepted: %q", order)
		}
		if _, _, _, err := adapter.ListRecycleBin(ctx, 100, ListOptions{Order: order, Count: 10}); err == nil {
			t.Fatalf("unsafe recycle sort accepted: %q", order)
		}
	}
	objects, _, _, err := adapter.ListObj(ctx, 100, 0, "", "", "-name", 10)
	if err != nil || len(objects) != 1 || objects[0].Name != "safe" {
		t.Fatalf("safe sort failed or objects modified: %v", err)
	}
}
