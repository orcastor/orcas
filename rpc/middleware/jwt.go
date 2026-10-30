package middleware

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
	"github.com/gotomicro/ego/core/elog"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/util"
)

var noAuthPath = map[string]bool{
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
	ErrSecretInvalid  = errors.New("ORCAS_SECRET must contain at least 32 bytes")
)

// ValidateSecret rejects missing or trivially short JWT signing keys.
func ValidateSecret() error {
	if len(ORCAS_SECRET) < 32 {
		return ErrSecretInvalid
	}
	return nil
}

type Claims struct {
	User string `json:"u"`
	Role uint32 `json:"r"`
	jwt.StandardClaims
}

func GenerateToken(user string, uid int64, role uint32) (string, int64, error) {
	if err := ValidateSecret(); err != nil {
		return "", 0, err
	}
	if uid <= 0 {
		return "", 0, ErrTokenInvalid
	}
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
	if err := ValidateSecret(); err != nil {
		return nil, err
	}
	if len(token) > 8192 {
		return nil, ErrTokenMalformed
	}
	tokenClaims, err := jwt.ParseWithClaims(token, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(ORCAS_SECRET), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))

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

	if tokenClaims == nil || !tokenClaims.Valid {
		return nil, ErrTokenInvalid
	}
	claims, ok := tokenClaims.Claims.(*Claims)
	if !ok || claims.Issuer != MOD_NAME || claims.ExpiresAt <= time.Now().Unix() {
		return nil, ErrTokenInvalid
	}
	if uid, err := strconv.ParseInt(claims.Audience, 10, 64); err != nil || uid <= 0 {
		return nil, ErrTokenInvalid
	}
	return claims, nil
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
			elog.Debugf("login_info: %+v", claims)
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
	return strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
}

func GetUID(c *gin.Context) int64 {
	if uid, ok := c.Get("uid"); ok {
		return uid.(int64)
	}
	return 0
}
