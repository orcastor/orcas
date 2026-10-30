package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
	"github.com/orcastor/orcas/core"
	rpcmiddleware "github.com/orcastor/orcas/rpc/middleware"
)

var testCredential = AWSCredential{AccessKeyID: "test-key", SecretAccessKey: "test-secret", UserID: 42}

// Sign independently of the production canonicalization and signing helpers.
func signedRequest(t *testing.T, body, timestamp string) *http.Request {
	t.Helper()
	r := httptest.NewRequest("PUT", "http://example.com/bucket/object~name", strings.NewReader(body))
	sum := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(sum[:])
	r.Header.Set("X-Amz-Content-Sha256", hash)
	r.Header.Set("X-Amz-Date", timestamp)
	headers := "host;x-amz-content-sha256;x-amz-date"
	canonical := "PUT\n/bucket/object~name\n\nhost:example.com\nx-amz-content-sha256:" + hash + "\nx-amz-date:" + timestamp + "\n\n" + headers + "\n" + hash
	date := timestamp[:8]
	scope := date + "/us-east-1/s3/aws4_request"
	canonicalHash := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + hex.EncodeToString(canonicalHash[:])
	mac := func(key []byte, value string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(value))
		return h.Sum(nil)
	}
	key := mac([]byte("AWS4"+testCredential.SecretAccessKey), date)
	key = mac(key, "us-east-1")
	key = mac(key, "s3")
	key = mac(key, "aws4_request")
	signature := hex.EncodeToString(mac(key, toSign))
	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", testCredential.AccessKeyID, scope, headers, signature))
	return r
}

func installTestStore(t *testing.T) {
	t.Helper()
	credentialStoreMu.RLock()
	old := credentialStore
	credentialStoreMu.RUnlock()
	store := NewInMemoryCredentialStore()
	if err := store.PutCredential(&testCredential); err != nil {
		t.Fatal(err)
	}
	SetCredentialStore(store)
	t.Cleanup(func() { SetCredentialStore(old) })
}

func TestSecurityAWSV4(t *testing.T) {
	installTestStore(t)
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	r := signedRequest(t, "payload", timestamp)
	if _, err := AuthenticateAWSV4(r); err != nil {
		t.Fatalf("independently signed request rejected: %v", err)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil || string(body) != "payload" {
		t.Fatalf("authentication consumed or corrupted body: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*http.Request)
	}{
		{"body", func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader("tampered")) }},
		{"host", func(r *http.Request) { r.Host = "other.example" }},
		{"query", func(r *http.Request) { r.URL.RawQuery = "uploadId=other" }},
		{"method", func(r *http.Request) { r.Method = "DELETE" }},
		{"unsigned-copy", func(r *http.Request) { r.Header.Set("X-Amz-Copy-Source", "/private/secret") }},
		{"unsigned-metadata", func(r *http.Request) { r.Header.Set("X-Amz-Meta-Owner", "other") }},
		{"missing-date", func(r *http.Request) { r.Header.Del("X-Amz-Date") }},
		{"missing-host-signature", func(r *http.Request) {
			r.Header.Set("Authorization", strings.ReplaceAll(r.Header.Get("Authorization"), "SignedHeaders=host;", "SignedHeaders="))
		}},
		{"scope", func(r *http.Request) {
			r.Header.Set("Authorization", strings.ReplaceAll(r.Header.Get("Authorization"), "aws4_request", "wrong"))
		}},
		{"duplicate", func(r *http.Request) {
			r.Header.Add("Unused", "x")
			r.Header.Set("Authorization", r.Header.Get("Authorization")+", Signature="+strings.Repeat("0", 64))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := signedRequest(t, "payload", timestamp)
			tc.change(r)
			if _, err := AuthenticateAWSV4(r); err == nil {
				t.Fatal("tampered request accepted")
			}
		})
	}
	for _, timestamp := range []string{time.Now().Add(-time.Hour).UTC().Format("20060102T150405Z"), time.Now().Add(time.Hour).UTC().Format("20060102T150405Z"), "20261030T250000Z"} {
		r := signedRequest(t, "payload", timestamp)
		if _, err := AuthenticateAWSV4(r); err == nil {
			t.Fatalf("stale, future or invalid timestamp accepted: %q", timestamp)
		}
	}
}

func TestSecurityCanonicalAWSExample(t *testing.T) {
	r := httptest.NewRequest("GET", "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	r.Header.Set("Range", "bytes=0-9")
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	emptyHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	r.Header.Set("X-Amz-Content-Sha256", emptyHash)
	headers := "host;range;x-amz-content-sha256;x-amz-date"
	want := "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\nx-amz-content-sha256:" + emptyHash + "\nx-amz-date:20130524T000000Z\n\n" + headers + "\n" + emptyHash
	got, err := buildCanonicalRequest(r, headers)
	if err != nil || got != want {
		t.Fatalf("AWS canonical example mismatch: %v\n%q", err, got)
	}
	r = httptest.NewRequest("GET", "http://example.com:80/a~b?z=~&a=2&a=1&%C3%A9=0", nil)
	got, err = buildCanonicalRequest(r, "host")
	if err != nil || !strings.Contains(got, "\n/a~b\n%C3%A9=0&a=1&a=2&z=~\nhost:example.com:80\n") {
		t.Fatalf("incorrect path/query/host encoding: %v\n%s", err, got)
	}
}

func TestSecurityS3Authentication(t *testing.T) {
	installTestStore(t)
	t.Setenv("ORCAS_SKIP_SIG_VERIFY", "1")
	oldSecret := rpcmiddleware.ORCAS_SECRET
	rpcmiddleware.ORCAS_SECRET = strings.Repeat("test", 8)
	t.Cleanup(func() { rpcmiddleware.ORCAS_SECRET = oldSecret })
	router := gin.New()
	router.Use(S3Auth())
	router.PUT("/bucket/object~name", func(c *gin.Context) {
		values, _ := c.Request.Context().Value("o").(map[string]interface{})
		if values["role"] == uint32(core.ADMIN) {
			t.Error("authentication invented administrator privilege")
		}
		if rpcmiddleware.GetUID(c) != 42 {
			t.Error("wrong authenticated user")
		}
		c.Status(204)
	})
	token, _, err := rpcmiddleware.GenerateToken("regular", 42, core.USER)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		auth   string
		status int
	}{
		{"", 403}, {"invalid", 403}, {"AWS4-HMAC-SHA256 Credential=unknown", 403}, {"Bearer " + token, 204},
	} {
		r := httptest.NewRequest("PUT", "/bucket/object~name", nil)
		r.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("auth %q returned %d", tc.auth, w.Code)
		}
		if strings.Contains(w.Body.String(), "canonical") || strings.Contains(w.Body.String(), "expected") {
			t.Fatal("internal signature details exposed")
		}
	}
	r := signedRequest(t, "payload", time.Now().UTC().Format("20060102T150405Z"))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatalf("AWS auth rejected: %d %s", w.Code, w.Body.String())
	}
	// Recheck expiry on every request, including tokens previously accepted.
	oldClock := jwt.TimeFunc
	t.Cleanup(func() { jwt.TimeFunc = oldClock })
	jwt.TimeFunc = func() time.Time { return time.Now().Add(48 * time.Hour) }
	r = httptest.NewRequest("PUT", "/bucket/object~name", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("cached token survived expiration")
	}
}

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("body read failure") }
func (failingBody) Close() error             { return nil }

func TestSecurityAWSBodyLimits(t *testing.T) {
	installTestStore(t)
	t.Setenv("ORCAS_MAX_REQUEST_BYTES", "8")
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	for _, unknown := range []bool{false, true} {
		r := signedRequest(t, "123456789", timestamp)
		if unknown {
			r.ContentLength = -1
		}
		if _, err := AuthenticateAWSV4(r); err == nil {
			t.Fatal("oversized request accepted")
		}
		r = signedRequest(t, "123456789", timestamp)
		if unknown {
			r.ContentLength = -1
		}
		router := gin.New()
		router.Use(S3Auth())
		router.PUT("/bucket/object~name", func(c *gin.Context) { t.Error("oversized request reached storage") })
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != 413 {
			t.Fatalf("oversized body status: %d %s", w.Code, w.Body.String())
		}
	}
	r := signedRequest(t, "payload", timestamp)
	r.Body = failingBody{}
	if _, err := AuthenticateAWSV4(r); err == nil {
		t.Fatal("body read failure ignored")
	}
}

func TestSecurityCredentialStore(t *testing.T) {
	store := NewInMemoryCredentialStore()
	credential := testCredential
	if err := store.PutCredential(&credential); err != nil {
		t.Fatal(err)
	}
	credential.SecretAccessKey = "modified"
	got, err := store.GetCredential(testCredential.AccessKeyID)
	if err != nil || got.SecretAccessKey != testCredential.SecretAccessKey {
		t.Fatal("store retained caller-owned pointer")
	}
	got.SecretAccessKey = "modified again"
	got, _ = store.GetCredential(testCredential.AccessKeyID)
	if got.SecretAccessKey != testCredential.SecretAccessKey {
		t.Fatal("store exposed mutable credential")
	}
	for _, bad := range []*AWSCredential{nil, {}, {AccessKeyID: "id", UserID: 1}} {
		if err := store.PutCredential(bad); err == nil {
			t.Fatal("invalid credential accepted")
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 50; n++ {
				_ = store.PutCredential(&testCredential)
				_, _ = store.GetCredential(testCredential.AccessKeyID)
			}
		}()
	}
	wg.Wait()
}
