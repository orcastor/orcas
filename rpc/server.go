package main

import (
	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egin"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
)

var mntPath = "/tmp/test/"

// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid go run main.go --config=config.toml
func main() {
	core.Init(&core.CoreConfig{
		Path: mntPath,
	})
	core.InitDB()
	if err := ego.New().Serve(func() *egin.Component {
		server := egin.Load("server.http").Build()

		server.Use(middleware.Metrics())
		server.Use(middleware.CORS())
		server.Use(middleware.JWT())

		api := server.Group("/api")
		api.POST("/login", login)
		api.POST("/list", list)
		api.POST("/get", get)
		api.POST("/token", token)
		return server
	}()).Run(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
}
