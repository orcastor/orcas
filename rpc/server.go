package main

import (
	"os"

	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egin"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
)

// bash <(curl -L https://raw.githubusercontent.com/gotomicro/egoctl/main/getlatest.sh)

// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid ORCAS_BASE=/opt/orcas ORCAS_DATA=/opt/orcas_disk ORCAS_DB_KEY=your-db-encryption-key ORCAS_SECRET=xxxxxxxx egoctl run --runargs --config=config.toml
// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid ORCAS_BASE=/opt/orcas ORCAS_DATA=/opt/orcas_disk ORCAS_DB_KEY=your-db-encryption-key ORCAS_SECRET=xxxxxxxx go run ./... --config=config.toml
func main() {
	// Initialize database with encryption key from environment variable
	// ORCAS_BASE_DB_KEY: main database encryption key (BASE path, empty means unencrypted)
	// ORCAS_DATA_DB_KEY: bucket database encryption key (DATA path, empty means unencrypted)
	baseDBKey := os.Getenv("ORCAS_BASE_DB_KEY")
	core.InitDB(os.Getenv("ORCAS_BASE"), baseDBKey)

	// Set database keys in handler if provided
	dataDBKey := os.Getenv("ORCAS_DATA_DB_KEY")
	if baseDBKey != "" || dataDBKey != "" {
		hanlder.MetadataAdapter().SetBaseKey(baseDBKey)
		hanlder.MetadataAdapter().SetDataKey(dataDBKey)
	}
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
