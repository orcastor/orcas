package main

import (
	"os"
	"time"

	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/econf"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egin"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
)

func main() {
	if err := middleware.ValidateSecret(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
	// Initialize database with encryption key from environment variable
	// ORCAS_BASE_DB_KEY: main database encryption key (BASE path, empty means unencrypted)
	// ORCAS_DATA_DB_KEY: bucket database encryption key (DATA path, empty means unencrypted)
	baseDBKey := os.Getenv("ORCAS_BASE_DB_KEY")
	if err := core.InitDB(os.Getenv("ORCAS_BASE"), baseDBKey); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
	hanlder = core.NewLocalHandler(os.Getenv("ORCAS_BASE"), os.Getenv("ORCAS_DATA"))

	// Set database keys in handler if provided
	dataDBKey := os.Getenv("ORCAS_DATA_DB_KEY")
	if baseDBKey != "" || dataDBKey != "" {
		hanlder.MetadataAdapter().SetBaseKey(baseDBKey)
		hanlder.MetadataAdapter().SetDataKey(dataDBKey)
	}
	if err := ego.New().Serve(func() *egin.Component {
		headerTimeout := econf.GetDuration("server.http.serverReadHeaderTimeout")
		if headerTimeout <= 0 {
			headerTimeout = 10 * time.Second
		}
		readTimeout := econf.GetDuration("server.http.serverReadTimeout")
		if readTimeout <= 0 {
			readTimeout = 2 * time.Minute
		}
		server := egin.Load("server.http").Build(egin.WithServerReadHeaderTimeout(headerTimeout), egin.WithServerReadTimeout(readTimeout))

		server.Use(middleware.Metrics())
		server.Use(middleware.CORS())
		server.Use(middleware.RequestLimit(1 << 20))
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
