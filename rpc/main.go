package main

import (
	"github.com/orcastor/orcas/rpc/middleware"

	"github.com/gin-gonic/gin"
	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egin"
)

// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid go run main.go --config=config.toml
func main() {
	if err := ego.New().Serve(func() *egin.Component {
		server := egin.Load("server.http").Build()

		server.Use(middleware.Metrics())
		server.Use(middleware.CORS())
		server.Use(middleware.JWT())

		server.GET("/hello", func(ctx *gin.Context) {
			ctx.JSON(200, "Hello Ego")
			return
		})
		return server
	}()).Run(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
}
