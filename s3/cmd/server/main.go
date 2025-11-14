package main

import (
	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egin"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/s3"
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
		// PUT /{bucket}/{key} with x-amz-copy-source - CopyObject
		// PUT /{bucket}/{key} with x-amz-move-source - MoveObject (non-standard)
		// DELETE /{bucket}/{key} - DeleteObject
		// HEAD /{bucket}/{key} - HeadObject
		// POST /{bucket}/{key}?uploads - InitiateMultipartUpload
		// PUT /{bucket}/{key}?partNumber={n}&uploadId={id} - UploadPart
		// POST /{bucket}/{key}?uploadId={id} - CompleteMultipartUpload
		// DELETE /{bucket}/{key}?uploadId={id} - AbortMultipartUpload
		// GET /{bucket}?uploads - ListMultipartUploads
		// GET /{bucket}/{key}?uploadId={id} - ListParts
		server.GET("/", s3.ListBuckets)
		server.PUT("/:bucket", s3.CreateBucket)
		server.DELETE("/:bucket", s3.DeleteBucket)
		server.GET("/:bucket", s3.ListObjects)
		server.GET("/:bucket/*key", s3.GetObject)
		server.PUT("/:bucket/*key", s3.PutObject)
		server.DELETE("/:bucket/*key", s3.DeleteObject)
		server.HEAD("/:bucket/*key", s3.HeadObject)

		return server
	}()).Run(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
}
