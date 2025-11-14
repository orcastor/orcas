package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/s3"
	"github.com/orcastor/orcas/s3/middleware"
)

func main() {
	// Get port from environment or use default
	port := os.Getenv("ORCAS_PORT")
	if port == "" {
		port = "9000"
	}

	// Setup directories
	baseDir := os.Getenv("ORCAS_BASE")
	if baseDir == "" {
		baseDir = filepath.Join(os.TempDir(), "orcas_benchmark")
		os.MkdirAll(baseDir, 0o755)
		os.Setenv("ORCAS_BASE", baseDir)
		core.ORCAS_BASE = baseDir
	}

	dataDir := os.Getenv("ORCAS_DATA")
	if dataDir == "" {
		dataDir = filepath.Join(os.TempDir(), "orcas_benchmark_data")
		os.MkdirAll(dataDir, 0o755)
		os.Setenv("ORCAS_DATA", dataDir)
		core.ORCAS_DATA = dataDir
	}

	// Initialize database
	core.InitDB("")

	// Ensure test user exists
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	db, err := core.GetDB()
	if err == nil {
		db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar) VALUES (1, 1, 'orcas', ?, 'orcas', '', '')`, hashedPwd)
		db.Close()
	}

	// Setup gin router
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(middleware.CORS())
	router.Use(middleware.S3Auth())

	// S3 API endpoints
	router.GET("/", s3.ListBuckets)
	router.PUT("/:bucket", s3.CreateBucket)
	router.DELETE("/:bucket", s3.DeleteBucket)
	router.GET("/:bucket", s3.ListObjects)
	router.GET("/:bucket/*key", s3.GetObject)
	router.PUT("/:bucket/*key", s3.PutObject)
	router.DELETE("/:bucket/*key", s3.DeleteObject)
	router.HEAD("/:bucket/*key", s3.HeadObject)

	// Start server
	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	// Handle graceful shutdown
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
		<-sigint

		fmt.Println("\nShutting down server...")
		server.Shutdown(context.Background())
		os.Exit(0)
	}()

	fmt.Printf("orcas S3 benchmark server starting on port %s...\n", port)
	fmt.Printf("ORCAS_BASE: %s\n", baseDir)
	fmt.Printf("ORCAS_DATA: %s\n", dataDir)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
