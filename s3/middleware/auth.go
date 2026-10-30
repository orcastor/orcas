package middleware

import (
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	rpcmiddleware "github.com/orcastor/orcas/rpc/middleware"
	s3util "github.com/orcastor/orcas/s3/util"
)

const defaultMaxRequestBytes int64 = 64 << 20

func maxRequestBytes() int64 {
	if n, err := strconv.ParseInt(os.Getenv("ORCAS_MAX_REQUEST_BYTES"), 10, 64); err == nil && n > 0 && n < 1<<63-1 {
		return n
	}
	return defaultMaxRequestBytes
}

// S3Auth requires a verified AWS V4 signature or a valid JWT on every request.
// Roles are resolved by the core authorization layer from the user database.
func S3Auth() gin.HandlerFunc {
	limit := maxRequestBytes()
	return func(c *gin.Context) {
		if c.Request.ContentLength > limit {
			s3util.S3ErrorResponse(c, http.StatusRequestEntityTooLarge, "EntityTooLarge", "Request body exceeds the configured limit")
			c.Abort()
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)

		var uid int64
		if strings.HasPrefix(c.GetHeader("Authorization"), "AWS4-HMAC-SHA256 ") {
			credential, err := AuthenticateAWSV4(c.Request)
			if err != nil || credential == nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					s3util.S3ErrorResponse(c, http.StatusRequestEntityTooLarge, "EntityTooLarge", "Request body exceeds the configured limit")
					c.Abort()
					return
				}
				s3util.S3ErrorResponse(c, http.StatusForbidden, "SignatureDoesNotMatch", "The request signature could not be verified")
				c.Abort()
				return
			}
			uid = credential.UserID
		} else {
			claims, err := rpcmiddleware.ParseToken(rpcmiddleware.GetToken(c))
			if err != nil {
				s3util.S3ErrorResponse(c, http.StatusForbidden, "AccessDenied", "A valid authentication token is required")
				c.Abort()
				return
			}
			uid, _ = strconv.ParseInt(claims.Audience, 10, 64)
		}
		if uid <= 0 {
			s3util.S3ErrorResponse(c, http.StatusForbidden, "AccessDenied", "Invalid user identity")
			c.Abort()
			return
		}
		c.Set("uid", uid)
		c.Request = c.Request.WithContext(core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{ID: uid}))
		c.Next()
	}
}
