package main

import (
	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egin"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/s3/middleware"
)

// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid ORCAS_BASE=/opt/orcas ORCAS_DATA=/opt/orcas_disk ORCAS_SECRET=xxxxxxxx egoctl run --runargs --config=config.toml
// EGO_DEBUG=true EGO_LOG_EXTRA_KEYS=uid ORCAS_BASE=/opt/orcas ORCAS_DATA=/opt/orcas_disk ORCAS_SECRET=xxxxxxxx go run ./... --config=config.toml
func main() {
	core.InitDB()
	if err := ego.New().Serve(func() *egin.Component {
		server := egin.Load("server.http").Build()

		server.Use(middleware.Metrics())
		server.Use(middleware.CORS())
		server.Use(middleware.S3Auth())

		// S3 API endpoints
		// GET / - ListBuckets
		// PUT /{bucket} - CreateBucket
		// DELETE /{bucket} - DeleteBucket
		// GET /{bucket} - ListObjects
		// GET /{bucket}/{key} - GetObject
		// PUT /{bucket}/{key} - PutObject
		// DELETE /{bucket}/{key} - DeleteObject
		// HEAD /{bucket}/{key} - HeadObject
		server.GET("/", listBuckets)
		server.PUT("/:bucket", createBucket)
		server.DELETE("/:bucket", deleteBucket)
		server.GET("/:bucket", listObjects)
		server.GET("/:bucket/*key", getObject)
		server.PUT("/:bucket/*key", putObject)
		server.DELETE("/:bucket/*key", deleteObject)
		server.HEAD("/:bucket/*key", headObject)

		return server
	}()).Run(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
}
