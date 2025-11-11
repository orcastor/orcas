package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/orca-zhang/ecache"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
	s3util "github.com/orcastor/orcas/s3/util"
)

var (
	// tokenCache caches JWT token parsing results
	// key: token string, value: *middleware.Claims
	tokenCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// tokenUIDCache caches token to UID mapping for fast lookup
	// key: token string, value: int64 (UID)
	tokenUIDCache = ecache.NewLRUCache(16, 512, 30*time.Second)
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

			// Try to get UID from cache first (fast path)
			if cachedUID, ok := tokenUIDCache.GetInt64(token); ok {
				uid := middleware.GetUID(c)
				if uid == 0 {
					c.Set("uid", cachedUID)
					c.Request = c.Request.WithContext(core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{
						ID: cachedUID,
					}))
				}
				c.Next()
				return
			}

			// Try to get parsed claims from cache
			var claims *middleware.Claims
			var err error
			if cachedClaims, ok := tokenCache.Get(token); ok {
				if cl, ok := cachedClaims.(*middleware.Claims); ok {
					claims = cl
				}
			}

			// If not in cache, parse token
			if claims == nil {
				claims, err = middleware.ParseToken(token)
				if err != nil {
					s3util.S3ErrorResponse(c, http.StatusForbidden, "AccessDenied", "Invalid token")
					c.Abort()
					return
				}
				// Cache parsed claims
				tokenCache.Put(token, claims)
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
						// Cache token to UID mapping for fast lookup
						tokenUIDCache.PutInt64(token, parsedUID)
					}
				}
			}
		}

		c.Next()
	}
}
