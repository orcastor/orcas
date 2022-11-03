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
	core.InitDB()
	hanlder := core.NewLocalHandler()
	if err := ego.New().Serve(func() *egin.Component {
		server := egin.Load("server.http").Build()

		server.Use(middleware.Metrics())
		server.Use(middleware.CORS())
		server.Use(middleware.JWT())

		api := server.Group("/api")
		api.POST("/login", func(ctx *gin.Context) {
			var req struct {
				UserName string `json:"u"`
				Password string `json:"p"`
			}
			ctx.BindJSON(&req)
			_, u, b, err := hanlder.Login(ctx.Request.Context(), req.UserName, req.Password)
			if err != nil {
				util.AbortResponse(ctx, 100, err.Error())
				return
			}
			token, _, err := middleware.GenerateToken(u.Usr, u.ID, u.Role)
			util.Response(ctx, gin.H{
				"u":            u,
				"b":            b,
				"access_token": token,
			})
		})
		api.POST("/list", func(ctx *gin.Context) {
			var req struct {
				BktID int64            `json:"b,omitempty"`
				PID   int64            `json:"p,omitempty"`
				Opt   core.ListOptions `json:"o"`
			}
			ctx.BindJSON(&req)
			o, c, d, err := hanlder.List(ctx.Request.Context(), req.BktID, req.PID, req.Opt)
			if err != nil {
				util.AbortResponse(ctx, 100, err.Error())
				return
			}
			switch req.Opt.Brief {
			case 1:
				for i := range o {
					o[i].Extra = ""
				}
			case 2:
				for i := range o {
					o[i] = &core.ObjectInfo{ID: o[i].ID}
				}
			}
			util.Response(ctx, gin.H{
				"o": o,
				"c": c,
				"d": d,
			})
		})
		api.POST("/get", func(ctx *gin.Context) {
			var req struct {
				BktID int64 `json:"b,omitempty"`
				ID    int64 `json:"i,omitempty"`
			}
			ctx.BindJSON(&req)
			o, err := hanlder.Get(ctx.Request.Context(), req.BktID, []int64{req.ID})
			if err != nil {
				util.AbortResponse(ctx, 100, err.Error())
				return
			}
			if len(o) > 0 {
				util.Response(ctx, gin.H{
					"o": o[0],
				})
				return
			}
			util.Response(ctx, gin.H{})
		})
		api.POST("/token", func(ctx *gin.Context) {
			util.Response(ctx, gin.H{
				"uid": middleware.GetUID(ctx),
			})
		})
		return server
	}()).Run(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
}
