package main

import (
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
	"github.com/orcastor/orcas/rpc/util"

	"github.com/gin-gonic/gin"
	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egin"
)

var mntPath = "/tmp/test/"

// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid go run main.go --config=config.toml
func main() {
	core.Init(&core.CoreConfig{
		Path: mntPath,
	})
	hanlder := core.NewLocalHandler()
	if err := ego.New().Serve(func() *egin.Component {
		server := egin.Load("server.http").Build()

		server.Use(middleware.Metrics())
		server.Use(middleware.CORS())
		server.Use(middleware.JWT())

		server.POST("/login", func(ctx *gin.Context) {
			var req struct {
				UserName string `json:"username"`
				Password string `json:"password"`
			}
			ctx.BindJSON(&req)
			_, u, b, err := hanlder.Login(ctx.Request.Context(), req.UserName, req.Password)
			if err != nil {
				util.AbortResponse(ctx, 100, err.Error())
				return
			}
			token, _, err := middleware.GenerateToken(u.Usr, u.ID, u.Role)
			util.Response(ctx, gin.H{
				"user":         u,
				"bkts":         b,
				"access_token": token,
			})
			return
		})
		return server
	}()).Run(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
}
