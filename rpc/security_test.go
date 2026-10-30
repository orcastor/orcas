package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/rpc/middleware"
)

func TestSecurityMalformedRequests(t *testing.T) {
	for _, handler := range []gin.HandlerFunc{login, list, get} {
		router := gin.New()
		router.Use(middleware.RequestLimit(32))
		router.POST("/", handler)
		for _, tc := range []struct {
			body   string
			status int
		}{{"{", 400}, {strings.Repeat(" ", 33), 413}} {
			r := httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
			r.ContentLength = -1
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("malformed request returned %d: %s", w.Code, w.Body.String())
			}
		}
	}
}

func TestSecurityListLimit(t *testing.T) {
	router := gin.New()
	router.POST("/", list)
	for _, body := range []string{`{"c": -1}`, `{"c": 1001}`, `{"c": 2147483647}`} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("unsafe list limit accepted: %s", body)
		}
	}
}
