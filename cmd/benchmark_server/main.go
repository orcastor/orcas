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

// Global handler for bucket operations
var admin *core.LocalAdmin

// Simple S3 server for benchmarking
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

	// Ensure test user exists with ADMIN role (role=1 is ADMIN)
	hashedPwd := "1000:Zd54dfEjoftaY8NiAINGag==:q1yB510yT5tGIGNewItVSg=="
	var testUserID int64 = 1
	db, err := core.GetDB()
	if err == nil {
		// role=1 is ADMIN, which is required for PutBkt
		db.Exec(`INSERT OR IGNORE INTO usr (id, role, usr, pwd, name, avatar) VALUES (1, 1, 'orcas', ?, 'orcas', '')`, hashedPwd)
		// Update role to ensure it's ADMIN if user already exists
		db.Exec(`UPDATE usr SET role = 1 WHERE id = 1`)
		db.Close()
	}

	// Register AWS credentials for benchmark testing
	// Map minioadmin/minioadmin to test user
	store := middleware.NewInMemoryCredentialStore()

	// Put credential with proper error handling
	cred := &middleware.AWSCredential{
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UserID:          testUserID,
	}
	if err := store.PutCredential(cred); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to register AWS credential: %v\n", err)
		os.Exit(1)
	}

	middleware.SetCredentialStore(store)

	// Verify credential was registered
	fmt.Fprintf(os.Stderr, "[SETUP] Verifying credential registration...\n")
	if retrieved, err := store.GetCredential("minioadmin"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to verify credential registration: %v\n", err)
		os.Exit(1)
	} else if retrieved == nil {
		fmt.Fprintf(os.Stderr, "Credential was stored but retrieval returned nil\n")
		os.Exit(1)
	} else {
		fmt.Fprintf(os.Stderr, "[SETUP] AWS credential registered: AccessKeyID=%s, UserID=%d\n", retrieved.AccessKeyID, retrieved.UserID)
	}

	// Also auto-create test bucket for benchmark
	fmt.Fprintf(os.Stderr, "[SETUP] Creating test bucket...\n")
	testBucket := &core.BucketInfo{
		ID:   core.NewID(),
		Name: "test-bucket",
		Type: 1,
	}
	admin := core.NewLocalAdmin()

	// Create context with user info for bucket creation
	ctx := context.Background()
	userInfo := &core.UserInfo{
		ID:   testUserID,
		Name: "orcas",
		Role: 1, // ADMIN role
	}
	ctx = core.UserInfo2Ctx(ctx, userInfo)

	if err := admin.PutBkt(ctx, []*core.BucketInfo{testBucket}); err != nil {
		fmt.Fprintf(os.Stderr, "[SETUP] Warning: Failed to create test bucket: %v\n", err)
		fmt.Fprintf(os.Stderr, "[SETUP] Bucket creation may have failed - test will attempt to create it\n")
	} else {
		fmt.Fprintf(os.Stderr, "[SETUP] Test bucket 'test-bucket' created successfully for user %d\n", testUserID)
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

	fmt.Fprintf(os.Stderr, "orcas S3 benchmark server starting on port %s...\n", port)
	fmt.Fprintf(os.Stderr, "ORCAS_BASE: %s\n", baseDir)
	fmt.Fprintf(os.Stderr, "ORCAS_DATA: %s\n", dataDir)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
