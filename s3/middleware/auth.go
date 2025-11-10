package middleware

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
	s3util "github.com/orcastor/orcas/s3/util"
)

// S3Auth handles S3 authentication
// Supports both AWS Signature V4 and JWT token authentication
func S3Auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Try AWS Signature V4 authentication first
		authHeader := c.GetHeader("Authorization")
		if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
			// AWS Signature V4 authentication
			credential, err := AuthenticateAWSV4(c.Request)
			if err == nil && credential != nil {
				// Authentication successful, set user ID
				c.Set("uid", credential.UserID)
				c.Request = c.Request.WithContext(core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{
					ID: credential.UserID,
				}))
				c.Next()
				return
			}
			// If AWS auth fails, fall through to JWT authentication
		}

		// Fall back to JWT authentication
		token := c.GetHeader("Authorization")
		if token == "" {
			token = c.Query("token")
		}
		if token == "" {
			token = c.PostForm("token")
		}

		if token != "" {
			// Remove "Bearer " prefix if present
			token = strings.TrimPrefix(token, "Bearer ")
			claims, err := middleware.ParseToken(token)
			if err != nil {
				s3util.S3ErrorResponse(c, http.StatusForbidden, "AccessDenied", "Invalid token")
				c.Abort()
				return
			}

			uid := middleware.GetUID(c)
			if uid == 0 {
				// Set UID from token if not already set
				uidStr := claims.StandardClaims.Audience
				if uidStr != "" {
					// Parse UID from token
					if parsedUID, err := strconv.ParseInt(uidStr, 10, 64); err == nil {
						c.Set("uid", parsedUID)
						c.Request = c.Request.WithContext(core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{
							ID: parsedUID,
						}))
					}
				}
			}
		}

		c.Next()
	}
}
