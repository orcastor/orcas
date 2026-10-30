package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RequestLimit bounds both declared and streamed JSON request bodies.
func RequestLimit(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > limit {
			c.AbortWithStatus(http.StatusRequestEntityTooLarge)
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		c.Next()
	}
}
