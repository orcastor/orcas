package middleware

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v7"
	"github.com/golang-jwt/jwt"
	"github.com/gotomicro/ego/core/elog"
)

var noAuthPath = map[string]bool{
	"/login": true,
}

const (
	TokenExpiredCode int32 = 401
	TokenErrorCode   int32 = 402

	MOD_NAME   = "orcas"
	JWT_SECRET = "xxxxxxxx"
)

var (
	ErrTokenExpired   = errors.New("token expired")
	ErrTokenMalformed = errors.New("not a token")
	ErrTokenInvalid   = errors.New("token invalid")
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

func GenerateToken(username string, uid, privilege int64) (string, int64, error) {
	expireTime := time.Now().Add(24 * time.Hour).Unix()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		username,
		privilege,
		jwt.StandardClaims{
			Audience:  strconv.FormatInt(uid, 10),
			ExpiresAt: expireTime,
			Issuer:    MOD_NAME,
		},
	}).SignedString([]byte(JWT_SECRET))
	return token, expireTime, err
}

func ParseToken(token string) (*Claims, error) {
	tokenClaims, err := jwt.ParseWithClaims(token, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(JWT_SECRET), nil
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
		return nil, err
	}

	return tokenClaims.Claims.(*Claims), nil
}

func CacheKeyJWTToken(uid int64) string {
	return fmt.Sprintf("%s:token:%d", MOD_NAME, uid)
}

func JWT() gin.HandlerFunc {
	return func(c *gin.Context) {
		if noAuthPath[c.FullPath()] {
			c.Next()
			return
		}

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
			uid, _ := strconv.ParseInt(claims.StandardClaims.Audience, 10, 64)
			if val, err := RedisCli().Get(CacheKeyJWTToken(uid)).Result(); val != token {
				elog.Errorf("CheckTokenCache error: %+v,%s,%s", err, val, token)
				JWTAbortResponse(c, int(TokenExpiredCode), "token expired")
				return
			}
			c.Set("uid", uid)
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

func GetToken(c *gin.Context) (token string) {
	token = c.GetHeader("Authorization")
	if token != "" {
		return
	}
	// get
	token = c.Query("token")
	if token != "" {
		return
	}
	// postform
	token = c.PostForm("token")
	return
}

func GetUid(c *gin.Context) int64 {
	if uid, ok := c.Get("uid"); ok {
		return uid.(int64)
	}
	return 0
}
