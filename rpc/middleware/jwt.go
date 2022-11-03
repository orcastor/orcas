package middleware

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"
	"github.com/gotomicro/ego/core/elog"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/util"
)

var noAuthPath = map[string]bool{
	"":           true,
	"/":          true,
	"/api/login": true,
}

var ORCAS_SECRET = os.Getenv("ORCAS_SECRET")

const (
	TokenExpiredCode int32 = 599

	MOD_NAME = "orcas"
)

var (
	ErrTokenExpired   = errors.New("token expired")
	ErrTokenMalformed = errors.New("not a token")
	ErrTokenInvalid   = errors.New("token invalid")
)

type Claims struct {
	User string `json:"u"`
	Role uint32 `json:"r"`
	jwt.StandardClaims
}

func GenerateToken(user string, uid int64, role uint32) (string, int64, error) {
	expireTime := time.Now().Add(24 * time.Hour).Unix()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		user,
		role,
		jwt.StandardClaims{
			Audience:  strconv.FormatInt(uid, 10),
			ExpiresAt: expireTime,
			Issuer:    MOD_NAME,
		},
	}).SignedString([]byte(ORCAS_SECRET))
	return token, expireTime, err
}

func ParseToken(token string) (*Claims, error) {
	tokenClaims, err := jwt.ParseWithClaims(token, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(ORCAS_SECRET), nil
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
				util.AbortResponse(c, int(TokenExpiredCode), "token expired")
			} else {
				util.AbortResponse(c, int(TokenExpiredCode), "token error")
			}
			return
		} else {
			elog.Infof("login_info: %+v", claims)
			uid, _ := strconv.ParseInt(claims.StandardClaims.Audience, 10, 64)
			c.Request = c.Request.WithContext((core.UserInfo2Ctx(c.Request.Context(), &core.UserInfo{
				ID: uid,
			})))
			c.Set("uid", uid)
		}
		c.Next()
	}
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

func GetUID(c *gin.Context) int64 {
	if uid, ok := c.Get("uid"); ok {
		return uid.(int64)
	}
	return 0
}
