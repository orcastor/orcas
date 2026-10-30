package middleware

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

func CORS() gin.HandlerFunc {
	return CORSPolicy("POST, GET, OPTIONS", "Content-Type, Authorization")
}

// CORSPolicy allows only exact origins configured at startup. An empty list
// disables cross-origin access; wildcard and null origins are not accepted.
func CORSPolicy(methods, headers string) gin.HandlerFunc {
	allowed := make(map[string]bool)
	for _, origin := range strings.Split(os.Getenv("ORCAS_CORS_ORIGINS"), ",") {
		origin = strings.TrimSpace(origin)
		if origin != "" && origin != "*" && origin != "null" {
			allowed[origin] = true
		}
	}
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		c.Writer.Header().Add("Vary", "Origin")
		if origin != "" {
			scheme := "http"
			if c.Request.TLS != nil {
				scheme = "https"
			}
			if !allowed[origin] && origin != scheme+"://"+c.Request.Host {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Headers", headers)
			c.Header("Access-Control-Allow-Methods", methods)
			c.Header("Access-Control-Expose-Headers", "Content-Length, Content-Type, ETag, Last-Modified")
		}

		// Allow all OPTIONS methods
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
