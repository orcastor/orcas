package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
)

func testSecret(t *testing.T) {
	old := ORCAS_SECRET
	ORCAS_SECRET = strings.Repeat("test", 8)
	t.Cleanup(func() { ORCAS_SECRET = old })
}

func TestSecurityJWT(t *testing.T) {
	testSecret(t)
	token, _, err := GenerateToken("user", 42, 0)
	if err != nil {
		t.Fatal(err)
	}
	if claims, err := ParseToken(token); err != nil || claims.Audience != "42" {
		t.Fatalf("valid token rejected: %v", err)
	}
	for _, secret := range []string{"", "short"} {
		ORCAS_SECRET = secret
		if _, _, err := GenerateToken("user", 42, 0); err == nil {
			t.Fatal("weak signing key accepted")
		}
		if _, err := ParseToken(token); err == nil {
			t.Fatal("token accepted with weak signing key")
		}
	}
	ORCAS_SECRET = strings.Repeat("test", 8)
	good := Claims{StandardClaims: jwt.StandardClaims{Audience: "42", Issuer: MOD_NAME, ExpiresAt: time.Now().Add(time.Hour).Unix()}}
	for _, tc := range []struct {
		name   string
		change func(*Claims)
		method jwt.SigningMethod
	}{
		{"algorithm", func(c *Claims) {}, jwt.SigningMethodHS512},
		{"issuer", func(c *Claims) { c.Issuer = "other" }, jwt.SigningMethodHS256},
		{"no-expiry", func(c *Claims) { c.ExpiresAt = 0 }, jwt.SigningMethodHS256},
		{"expired", func(c *Claims) { c.ExpiresAt = time.Now().Add(-time.Hour).Unix() }, jwt.SigningMethodHS256},
		{"bad-user", func(c *Claims) { c.Audience = "not-a-user" }, jwt.SigningMethodHS256},
		{"zero-user", func(c *Claims) { c.Audience = "0" }, jwt.SigningMethodHS256},
		{"negative-user", func(c *Claims) { c.Audience = "-1" }, jwt.SigningMethodHS256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claims := good
			tc.change(&claims)
			signed, err := jwt.NewWithClaims(tc.method, claims).SignedString([]byte(ORCAS_SECRET))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ParseToken(signed); err == nil {
				t.Fatal("invalid claims accepted")
			}
		})
	}
	router := gin.New()
	router.Use(JWT())
	router.GET("/", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	router.POST("/api/login", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	for _, tc := range []struct {
		method, path, auth string
		allowed            bool
	}{
		{"GET", "/", "", false}, {"GET", "/?token=" + token, "", false},
		{"GET", "/", "Bearer " + token, true}, {"GET", "/", token, true},
		{"POST", "/api/login", "", true}, {"GET", "/unknown", "", false},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if (w.Code == http.StatusNoContent) != tc.allowed {
			t.Fatalf("unexpected auth result for %s %s: %d", tc.method, tc.path, w.Code)
		}
	}
}

func TestSecurityCORS(t *testing.T) {
	t.Setenv("ORCAS_CORS_ORIGINS", "https://trusted.example,*,null")
	router := gin.New()
	router.Use(CORS())
	router.POST("/", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	for _, tc := range []struct {
		origin string
		status int
		allow  string
	}{
		{"https://trusted.example", 204, "https://trusted.example"}, {"https://evil.example", 403, ""},
		{"https://trusted.example.evil.example", 403, ""}, {"null", 403, ""}, {"", 204, ""},
		{"http://example.com", 204, "http://example.com"},
	} {
		for _, method := range []string{"POST", "OPTIONS"} {
			r := httptest.NewRequest(method, "http://example.com/", nil)
			r.Header.Set("Origin", tc.origin)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != tc.status || w.Header().Get("Access-Control-Allow-Origin") != tc.allow || w.Header().Get("Access-Control-Allow-Credentials") != "" {
				t.Fatalf("CORS %s %q: %d %v", method, tc.origin, w.Code, w.Header())
			}
		}
	}
}

func TestSecurityRequestLimit(t *testing.T) {
	router := gin.New()
	router.Use(RequestLimit(8))
	router.POST("/", func(c *gin.Context) {
		if _, err := io.ReadAll(c.Request.Body); err != nil {
			c.Status(413)
			return
		}
		c.Status(204)
	})
	for _, unknown := range []bool{false, true} {
		r := httptest.NewRequest("POST", "/", strings.NewReader("123456789"))
		if unknown {
			r.ContentLength = -1
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != 413 {
			t.Fatalf("oversized request accepted (unknown=%v): %d", unknown, w.Code)
		}
	}
}
