package middleware

import (
	"github.com/gin-gonic/gin"
	rpcmiddleware "github.com/orcastor/orcas/rpc/middleware"
)

func CORS() gin.HandlerFunc {
	return rpcmiddleware.CORSPolicy("GET, POST, PUT, DELETE, HEAD, OPTIONS",
		"Content-Type, Authorization, X-Amz-Date, X-Amz-Content-Sha256, X-Amz-Copy-Source, X-Amz-Move-Source")
}
