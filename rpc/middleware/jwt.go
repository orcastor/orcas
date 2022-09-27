package middleware

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v7"
	"github.com/gotomicro/ego/core/elog"
)

var noAuthPath = map[string]bool{
	"/login": true,
}

const (
	TokenExpiredCode int32 = 401
	TokenErrorCode   int32 = 402
)

var (
	ErrTokenExpired   = errors.New("Token is expired")
	ErrTokenMalformed = errors.New("That's not even a token")
	ErrTokenInvalid   = errors.New("Token invalid")
)

func RedisCli() redis.Cmdable {
	// TODO
	return nil
}

type Claims struct {
	Username  string `json:"username"`
	Privilege int64  `json:"privilege"`
	jwt.StandardClaims
}

func GetJWTSecret() []byte {
	encodeString := "JhbGciOiJIUzI"
	decodeBytes, _ := base64.StdEncoding.DecodeString(encodeString)
	return decodeBytes
}

func GenerateToken(username string, uid, privilege int64) (string, int64, error) {
	expireTime := time.Now().Add(24 * time.Hour).Unix()

	claims := Claims{
		username,
		privilege,
		jwt.StandardClaims{
			Audience:  fmt.Sprintf("id:%d", uid),
			ExpiresAt: expireTime,
			Issuer:    "orcas",
		},
	}

	tokenClaims := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := tokenClaims.SignedString(GetJWTSecret())
	return token, expireTime, err
}

func ParseToken(token string) (*Claims, error) {
	tokenClaims, err := jwt.ParseWithClaims(token, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return GetJWTSecret(), nil
	})

	if err != nil {
		if ve, ok := err.(*jwt.ValidationError); ok {
			if ve.Errors&jwt.ValidationErrorMalformed != 0 {
				return nil, ErrTokenMalformed
			} else if ve.Errors&jwt.ValidationErrorExpired != 0 {
				return nil, ErrTokenExpired
			}
			return nil, ErrTokenInvalid
		}
	}

	if tokenClaims != nil {
		if claims, ok := tokenClaims.Claims.(*Claims); ok && tokenClaims.Valid {
			return claims, nil
		}
	}

	return nil, err
}

func CacheKeyJWTToken(uid int64) string {
	return fmt.Sprintf("orcas:token:%d", uid)
}

func JWT() gin.HandlerFunc {
	return func(c *gin.Context) {
		if IsAuthPath(c) {
			token := GetToken(c)
			claims, err := ParseToken(token)
			if err != nil {
				elog.Errorf("%+v", err)
				if err == ErrTokenExpired {
					JWTAbortResponse(c, int(TokenExpiredCode), "token expired")
				} else {
					JWTAbortResponse(c, int(TokenErrorCode), "token error")
				}
				return
			} else {
				elog.Infof("login_info: %+v", claims)
				var uid int64
				if n, err := fmt.Sscanf(claims.StandardClaims.Audience, "id:%d", &uid); n < 1 || err != nil {
					JWTAbortResponse(c, int(TokenErrorCode), "token error")
					return
				}
				if val, err := RedisCli().Get(CacheKeyJWTToken(uid)).Result(); val != token {
					elog.Errorf("CheckTokenCache error: %+v,%s,%s", err, val, token)
					JWTAbortResponse(c, int(TokenExpiredCode), "token expired")
					return
				}
				c.Set("uid", uid) //设置已登录用户Id
			}
		}
		c.Next()
	}
}

func JWTAbortResponse(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(200, map[string]interface{}{
		"code": code,
		"msg":  msg,
		"ts":   time.Now().Unix(),
	})
}

func IsAuthPath(c *gin.Context) bool {
	path := c.FullPath()
	if b := noAuthPath[path]; b {
		return false
	}
	return true
}

func GetToken(c *gin.Context) string {
	//header
	if token := c.GetHeader("Authorization"); token != "" {
		return token
	}
	//get
	if token := c.Query("token"); token != "" {
		return token
	}
	//postform
	if token := c.PostForm("token"); token != "" {
		return token
	}
	return ""
}

func GetUid(c *gin.Context) int64 {
	if uid, ok := c.Get("uid"); ok {
		return uid.(int64)
	}
	return 0
}

func GetUidInt(c *gin.Context) int {
	return int(GetUid(c))
}
