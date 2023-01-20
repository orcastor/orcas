package main

import (
	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egin"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
)

// bash <(curl -L https://raw.githubusercontent.com/gotomicro/egoctl/main/getlatest.sh)

// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid ORCAS_BASE=/tmp/test ORCAS_DATA=/tmp/test ORCAS_SECRET=xxxxxxxx egoctl run --runargs --config=config.toml
// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid ORCAS_BASE=/tmp/test ORCAS_DATA=/tmp/test ORCAS_SECRET=xxxxxxxx go run server.go --config=config.toml
func main() {
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
