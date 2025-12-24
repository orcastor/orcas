package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
	"github.com/orcastor/orcas/vfs"
)

var (
	configFile  = flag.String("config", "", "Configuration file path (JSON format)")
	action      = flag.String("action", "", "Operation type: upload, download, mount, add-user, update-user, delete-user, list-users, create-bucket, delete-bucket, list-buckets, update-bucket")
	localPath   = flag.String("local", "", "Local file or directory path")
	remotePath  = flag.String("remote", "", "Remote path (relative to bucket root directory)")
	mountPoint  = flag.String("mountpoint", "", "Mount point path (for mount action)")
	bucketID    = flag.Int64("bucket", 0, "Bucket ID (if not specified, use first bucket)")
	bucketName  = flag.String("bucketname", "", "Bucket name (for create-bucket)")
	bucketQuota = flag.Int64("quota", -1, "Bucket quota in bytes (-1 for unlimited, for create-bucket)")
	bucketOwner = flag.String("owner", "", "Bucket owner username or user ID (for create-bucket, default is current user)")

	// Database key parameters
	dbKey = flag.String("dbkey", "", "Main database encryption key (for InitDB)")

	// Path parameters
	basePath = flag.String("basepath", "", "Base path for metadata (database storage location)")
	dataPath = flag.String("datapath", "", "Data path for file data storage location")

	//  Configuration parameters (can be set via command line arguments or configuration file)
	userName = flag.String("user", "", "Username")
	password = flag.String("pass", "", "Password")
	refLevel = flag.String("reflevel", "", "Instant upload level: OFF, FULL, FAST")
	cmprWay  = flag.String("cmprway", "", "Compression method (smart compression by default): SNAPPY, ZSTD, GZIP, BR")
	cmprQlty = flag.Int("cmprqlty", 0, "Compression level")
	endecWay = flag.String("endecway", "", "Encryption method: AES256, SM4")
	endecKey = flag.String("endeckey", "", "Encryption key")
	dontSync = flag.String("dontsync", "", "Filename wildcards to exclude from sync (separated by semicolons)")
	conflict = flag.String("conflict", "", "Conflict resolution for same name: COVER, RENAME, THROW, SKIP")
	nameTmpl = flag.String("nametmpl", "", "Rename template (contains %s)")
	workersN = flag.Int("workers", 0, "Concurrent pool size (not less than 16)")

	// User management parameters
	userID    = flag.Int64("userid", 0, "User ID (for update-user and delete-user)")
	newUser   = flag.String("newuser", "", "New username (for add-user and update-user)")
	newPass   = flag.String("newpass", "", "New password (for add-user and update-user)")
	userName_ = flag.String("username", "", "User display name (for add-user and update-user)")
	userRole  = flag.String("role", "", "User role: USER or ADMIN (for add-user and update-user)")

	// Debug parameter
	debug = flag.Bool("debug", false, "Enable debug mode (verbose output)")

	// Mount options
	requireKey = flag.Bool("requirekey", false, "Require KEY in context, return EPERM error if not provided (for mount action)")
	noAuth     = flag.Bool("noauth", false, "Bypass authentication and permission checks (no user required, for mount action)")
)

func main() {
	flag.Parse()

	// Determine if main database is needed
	// Main database is required for:
	// 1. User management operations
	// 2. Bucket management operations (need ACL)
	// 3. Login operations (unless NoAuth is used)
	needsMainDB := *action == "add-user" || *action == "update-user" ||
		*action == "delete-user" || *action == "list-users" ||
		*action == "create-bucket" || *action == "delete-bucket" ||
		*action == "list-buckets" || *action == "update-bucket" ||
		(*action != "mount" && *action != "" && !*noAuth)

	// Load configuration for upload/download operations
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Validate required parameters
	if cfg.UserName == "" {
		fmt.Fprintf(os.Stderr, "Error: Username cannot be empty (use -user parameter or configuration file)\n")
		os.Exit(1)
	}
	if cfg.Password == "" {
		fmt.Fprintf(os.Stderr, "Error: Password cannot be empty (use -pass parameter or configuration file)\n")
		os.Exit(1)
	}
	if *action == "" {
		fmt.Fprintf(os.Stderr, "Error: Must specify operation type (use -action upload, download, or mount)\n")
		flag.Usage()
		os.Exit(1)
	}
	if *action != "mount" && *localPath == "" {
		fmt.Fprintf(os.Stderr, "Error: Must specify local path (use -local parameter)\n")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize database only if needed
	if needsMainDB {
		var dbKeyValue string
		if *dbKey != "" {
			dbKeyValue = *dbKey
		}
		core.InitDB(cfg.BasePath, dbKeyValue)
	}

	// Handle user management and bucket management commands
	if *action == "add-user" || *action == "update-user" || *action == "delete-user" || *action == "list-users" {
		handleUserManagement()
		return
	}
	if *action == "create-bucket" || *action == "delete-bucket" || *action == "list-buckets" || *action == "update-bucket" {
		handleBucketManagement()
		return
	}

	// Handle mount operation
	if *action == "mount" {
		handleMount()
		return
	}

	// Create SDK instance
	handler := core.NewLocalHandler(cfg.BasePath, cfg.DataPath)
	sdkInstance := sdk.New(handler)
	defer sdkInstance.Close()

	// Login
	ctx, userInfo, buckets, err := sdkInstance.Login(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Login failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Login successful: User %s (ID: %d)\n", userInfo.Name, userInfo.ID)
	if len(buckets) == 0 {
		fmt.Fprintf(os.Stderr, "Error: No available bucket\n")
		os.Exit(1)
	}

	// Select bucket
	var selectedBktID int64
	if *bucketID > 0 {
		selectedBktID = *bucketID
		// Verify bucket exists
		found := false
		for _, b := range buckets {
			if b.ID == selectedBktID {
				found = true
				break
			}
		}
		if !found {
			fmt.Fprintf(os.Stderr, "Error: Bucket ID %d does not exist\n", *bucketID)
			os.Exit(1)
		}
	} else {
		selectedBktID = buckets[0].ID
		fmt.Printf("Using Bucket: %s (ID: %d)\n", buckets[0].Name, buckets[0].ID)
	}

	// Execute operation
	var pid int64 = core.ROOT_OID
	if *action == "upload" {
		// If remote path is specified, need to find parent directory ID
		remotePathStr := *remotePath
		if *remotePath != "" && *remotePath != "/" {
			pid, err = sdkInstance.Path2ID(ctx, selectedBktID, core.ROOT_OID, *remotePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: Cannot find remote path %s: %v\n", *remotePath, err)
				os.Exit(1)
			}
		} else {
			remotePathStr = "/"
		}

		fmt.Printf("Starting upload: %s -> %s\n", *localPath, remotePathStr)
		err = sdkInstance.Upload(ctx, selectedBktID, pid, *localPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Upload failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Upload completed!")
	} else if *action == "download" {
		// Download needs to find ID through path first
		if *remotePath == "" {
			fmt.Fprintf(os.Stderr, "Error: Download operation must specify remote path (use -remote parameter)\n")
			os.Exit(1)
		}

		pid, err = sdkInstance.Path2ID(ctx, selectedBktID, core.ROOT_OID, *remotePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Cannot find remote path %s: %v\n", *remotePath, err)
			os.Exit(1)
		}

		fmt.Printf("Starting download: /%s -> %s\n", *remotePath, *localPath)
		err = sdkInstance.Download(ctx, selectedBktID, pid, *localPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Download failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Download completed!")
	} else {
		fmt.Fprintf(os.Stderr, "Error: Unknown operation type %s (supported: upload, download, mount)\n", *action)
		os.Exit(1)
	}
}

func handleMount() {
	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Check if NoAuth is enabled (from command line or config)
	if *noAuth {
		cfg.NoAuth = true
	}

	// Validate required parameters
	if !cfg.NoAuth {
		if cfg.UserName == "" {
			fmt.Fprintf(os.Stderr, "Error: Username cannot be empty (use -user parameter or configuration file, or use -noauth to bypass authentication)\n")
			os.Exit(1)
		}
		if cfg.Password == "" {
			fmt.Fprintf(os.Stderr, "Error: Password cannot be empty (use -pass parameter or configuration file, or use -noauth to bypass authentication)\n")
			os.Exit(1)
		}
	}
	if *mountPoint == "" {
		fmt.Fprintf(os.Stderr, "Error: Mount point cannot be empty (use -mountpoint parameter)\n")
		os.Exit(1)
	}

	// Validate encryption key length
	if cfg.EndecWay == core.DATA_ENDEC_AES256 && len(cfg.EndecKey) <= 16 {
		fmt.Fprintf(os.Stderr, "Error: AES256 encryption key must be longer than 16 characters (current length: %d)\n", len(cfg.EndecKey))
		os.Exit(1)
	}
	if cfg.EndecWay == core.DATA_ENDEC_SM4 && len(cfg.EndecKey) != 16 {
		fmt.Fprintf(os.Stderr, "Error: SM4 encryption key must be exactly 16 characters (current length: %d)\n", len(cfg.EndecKey))
		os.Exit(1)
	}

	// Create handler and context
	var handler core.Handler
	var ctx core.Ctx
	var selectedBktID int64

	if cfg.NoAuth {
		// NoAuth mode: use NoAuthHandler and skip login
		handler = core.NewNoAuthHandler("", "")
		defer handler.Close()
		// Set paths if configured
		if cfg.BasePath != "" || cfg.DataPath != "" {
			handler.SetPaths(cfg.BasePath, cfg.DataPath)
		}
		ctx = context.Background()

		// In NoAuth mode, bucket ID must be specified
		if *bucketID <= 0 {
			fmt.Fprintf(os.Stderr, "Error: Bucket ID must be specified when using -noauth (use -bucket parameter)\n")
			os.Exit(1)
		}
		selectedBktID = *bucketID
		fmt.Printf("NoAuth mode: Bypassing authentication\n")
		fmt.Printf("Using Bucket ID: %d\n", selectedBktID)
	} else {
		// Normal mode: use LocalHandler and login
		handler = core.NewLocalHandler("", "")
		defer handler.Close()
		// Set paths if configured
		if cfg.BasePath != "" || cfg.DataPath != "" {
			handler.SetPaths(cfg.BasePath, cfg.DataPath)
		}

		// Login
		var userInfo *core.UserInfo
		var buckets []*core.BucketInfo
		ctx, userInfo, buckets, err = handler.Login(context.Background(), cfg.UserName, cfg.Password)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Login failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Login successful: User %s (ID: %d)\n", userInfo.Name, userInfo.ID)
		if len(buckets) == 0 {
			fmt.Fprintf(os.Stderr, "Error: No available bucket\n")
			os.Exit(1)
		}

		// Select bucket
		if *bucketID > 0 {
			selectedBktID = *bucketID
			// Verify bucket exists
			found := false
			for _, b := range buckets {
				if b.ID == selectedBktID {
					found = true
					break
				}
			}
			if !found {
				fmt.Fprintf(os.Stderr, "Error: Bucket ID %d does not exist\n", *bucketID)
				os.Exit(1)
			}
		} else {
			selectedBktID = buckets[0].ID
			fmt.Printf("Using Bucket: %s (ID: %d)\n", buckets[0].Name, buckets[0].ID)
		}
	}

	// Display configuration
	fmt.Printf("\nMount Configuration:\n")
	fmt.Printf("  Mount Point: %s\n", *mountPoint)
	if cfg.EndecWay == core.DATA_ENDEC_AES256 {
		fmt.Printf("  Encryption: AES256\n")
	} else if cfg.EndecWay == core.DATA_ENDEC_SM4 {
		fmt.Printf("  Encryption: SM4\n")
	}
	if cfg.CmprWay == core.DATA_CMPR_SNAPPY {
		fmt.Printf("  Compression: SNAPPY (level: %d)\n", cfg.CmprQlty)
	} else if cfg.CmprWay == core.DATA_CMPR_ZSTD {
		fmt.Printf("  Compression: ZSTD (level: %d)\n", cfg.CmprQlty)
	} else if cfg.CmprWay == core.DATA_CMPR_GZIP {
		fmt.Printf("  Compression: GZIP (level: %d)\n", cfg.CmprQlty)
	} else if cfg.CmprWay == core.DATA_CMPR_BR {
		fmt.Printf("  Compression: BROTLI (level: %d)\n", cfg.CmprQlty)
	}
	if *requireKey {
		fmt.Printf("  Require Key: Enabled (EPERM if KEY not provided)\n")
	}
	fmt.Printf("\n")

	// Mount filesystem
	// Note: AllowRoot is not supported by fusermount3, use AllowOther instead if needed
	// (requires user_allow_other in /etc/fuse.conf for non-root users)
	// Create mount options with common fields
	// Platform-specific fields (AllowOther, DefaultPermissions, EndecKey) are handled differently:
	// - On Linux/Unix: set via MountOptions fields
	// - On Windows: these fields don't exist in MountOptions, but EndecKey is passed via Config
	mountOpts := &vfs.MountOptions{
		MountPoint: *mountPoint,
		Foreground: true,
		Config:     &cfg,
		Debug:      *debug,
		RequireKey: *requireKey,
	}
	server, err := vfs.Mount(handler, ctx, selectedBktID, mountOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to mount filesystem: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Filesystem mounted successfully at %s\n", *mountPoint)
	fmt.Printf("Press Ctrl+C to unmount\n\n")

	// Run service (blocks until unmount)
	err = vfs.Serve(server, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Mount service error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Filesystem unmounted successfully")
}

func loadConfig() (core.Config, error) {
	cfg := core.Config{}

	// If configuration file is specified, load it first
	if *configFile != "" {
		data, err := os.ReadFile(*configFile)
		if err != nil {
			return core.Config{}, fmt.Errorf("failed to load configuration file: %v", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return core.Config{}, fmt.Errorf("failed to load configuration file: %v", err)
		}
		return cfg, nil
	}

	// Command line arguments override values in configuration file
	if *userName != "" {
		cfg.UserName = *userName
	}
	if *password != "" {
		cfg.Password = *password
	} else if cfg.Password == "" && !*noAuth {
		// If password is not provided via command line or config file, try to read from stdin
		// This supports both interactive input (hidden) and pipe input
		// Skip if NoAuth is enabled
		pwd, err := readPassword("Password: ")
		if err == nil && pwd != "" {
			cfg.Password = pwd
		}
	}
	if *noAuth {
		cfg.NoAuth = true
	}
	if *refLevel != "" {
		cfg.RefLevel = core.REF_LEVEL_FULL
	}
	if *cmprWay != "" {
		cfg.CmprWay = core.DATA_CMPR_GZIP
	}
	// cmprqlty default value is 0, if command line sets a value greater than 0, use it
	if *cmprQlty > 0 {
		cfg.CmprQlty = uint32(*cmprQlty)
	}
	if *endecWay != "" {
		cfg.EndecWay = core.DATA_ENDEC_AES256
	}
	if *endecKey != "" {
		cfg.EndecKey = *endecKey
	}
	if *dontSync != "" {
		cfg.DontSync = *dontSync
	}
	if *conflict != "" {
		cfg.Conflict = core.CONFLICT_COVER
	}
	if *nameTmpl != "" {
		cfg.NameTmpl = *nameTmpl
	}
	// workers default value is 0, if command line sets a value greater than 0, use it
	if *workersN > 0 {
		cfg.WorkersN = uint32(*workersN)
	}

	// BasePath and DataPath priority: command line > config file > environment variables
	// Command line arguments have highest priority
	if *basePath != "" {
		cfg.BasePath = *basePath
	} else if cfg.BasePath == "" {
		// Fallback to environment variable if not set in config file
		if envBasePath := os.Getenv("ORCAS_BASE"); envBasePath != "" {
			cfg.BasePath = envBasePath
		}
	}
	if *dataPath != "" {
		cfg.DataPath = *dataPath
	} else if cfg.DataPath == "" {
		// Fallback to environment variable if not set in config file
		if envDataPath := os.Getenv("ORCAS_DATA"); envDataPath != "" {
			cfg.DataPath = envDataPath
		}
	}

	return cfg, nil
}

// readPassword reads password from stdin with hidden input
func readPassword(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt)

	// Use platform-specific method to read password without echoing
	var password string
	var err error

	if runtime.GOOS == "windows" {
		// Check if stdin is a terminal (interactive) or pipe
		// For pipe input, read directly; for terminal, use PowerShell
		stat, _ := os.Stdin.Stat()
		isPipe := (stat.Mode() & os.ModeCharDevice) == 0

		if isPipe {
			// Pipe mode: read directly from stdin
			reader := bufio.NewReader(os.Stdin)
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return "", err
			}
			password = strings.TrimRight(line, "\r\n")
		} else {
			// Interactive mode: use PowerShell to read password without echoing
			cmd := exec.Command("powershell", "-Command", "$pwd = Read-Host -AsSecureString; $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($pwd); [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)")
			cmd.Stdin = os.Stdin
			output, err := cmd.Output()
			if err != nil {
				return "", err
			}
			password = strings.TrimSpace(string(output))
		}
	} else {
		// Unix/Linux: check if stdin is a terminal or pipe
		stat, _ := os.Stdin.Stat()
		isPipe := (stat.Mode() & os.ModeCharDevice) == 0

		if isPipe {
			// Pipe mode: read directly from stdin
			reader := bufio.NewReader(os.Stdin)
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return "", err
			}
			password = strings.TrimRight(line, "\r\n")
		} else {
			// Interactive mode: use stty to disable echo, then read password
			cmd := exec.Command("stty", "-echo")
			cmd.Stdin = os.Stdin
			cmd.Stderr = os.Stderr
			_ = cmd.Run()

			// Read password
			var b []byte = make([]byte, 1)
			var pwd strings.Builder
			for {
				n, err := os.Stdin.Read(b)
				if err != nil || n == 0 {
					break
				}
				if b[0] == '\n' || b[0] == '\r' {
					break
				}
				pwd.WriteByte(b[0])
			}
			password = pwd.String()

			// Restore terminal settings
			cmd = exec.Command("stty", "echo")
			cmd.Stdin = os.Stdin
			cmd.Stderr = os.Stderr
			_ = cmd.Run()
		}
	}

	fmt.Fprintln(os.Stderr) // Print newline after password input
	return password, err
}

func handleUserManagement() {
	// Load configuration for admin login
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Validate admin credentials
	if cfg.UserName == "" {
		fmt.Fprintf(os.Stderr, "Error: Admin username cannot be empty (use -user parameter or configuration file)\n")
		os.Exit(1)
	}
	if cfg.Password == "" {
		fmt.Fprintf(os.Stderr, "Error: Admin password cannot be empty (use -pass parameter or configuration file)\n")
		os.Exit(1)
	}

	// Create handler and login as admin
	handler := core.NewLocalHandler(cfg.BasePath, cfg.DataPath)
	ctx, userInfo, _, err := handler.Login(context.Background(), cfg.UserName, cfg.Password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Login failed: %v\n", err)
		os.Exit(1)
	}

	// Check if user is admin
	if userInfo.Role != core.ADMIN {
		fmt.Fprintf(os.Stderr, "Error: User %s is not an administrator\n", cfg.UserName)
		os.Exit(1)
	}

	// Create admin instance
	admin := core.NewLocalAdmin()
	defer admin.Close()

	// Execute user management command
	switch *action {
	case "add-user":
		if *newUser == "" {
			fmt.Fprintf(os.Stderr, "Error: Must specify username with -newuser parameter\n")
			os.Exit(1)
		}
		if *newPass == "" {
			// Try to read password from stdin
			pwd, err := readPassword("New user password: ")
			if err != nil || pwd == "" {
				fmt.Fprintf(os.Stderr, "Error: Must specify password with -newpass parameter or provide via stdin\n")
				os.Exit(1)
			}
			*newPass = pwd
		}
		role := uint32(core.USER)
		if *userRole == "ADMIN" {
			role = uint32(core.ADMIN)
		}
		name := *userName_
		if name == "" {
			name = *newUser
		}
		user, err := admin.CreateUser(ctx, *newUser, *newPass, name, role)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create user: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("User created successfully:\n")
		fmt.Printf("  ID: %d\n", user.ID)
		fmt.Printf("  Username: %s\n", user.Usr)
		fmt.Printf("  Name: %s\n", user.Name)
		if user.Role == core.ADMIN {
			fmt.Printf("  Role: ADMIN\n")
		} else {
			fmt.Printf("  Role: USER\n")
		}

	case "update-user":
		if *userID == 0 {
			fmt.Fprintf(os.Stderr, "Error: Must specify user ID with -userid parameter\n")
			os.Exit(1)
		}
		// If newPass is specified but empty, try to read from stdin
		// Note: We only read if -newpass was explicitly provided but empty, or if we need to update password
		// For now, we only read if -newpass was set to empty string (user wants to update password)
		var role *uint32
		if *userRole != "" {
			r := uint32(core.USER)
			if *userRole == "ADMIN" {
				r = uint32(core.ADMIN)
			}
			role = &r
		}
		// If newPass is needed but not provided, try to read from stdin
		// Note: UpdateUser accepts empty string for newPass if password shouldn't be updated
		// So we only prompt if user explicitly wants to update password
		updatePass := *newPass
		// If password is not provided and user wants to update password, try to read from stdin
		// For safety, we don't auto-prompt. User must explicitly provide -newpass or pipe
		err := admin.UpdateUser(ctx, *userID, *newUser, updatePass, *userName_, role)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to update user: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("User %d updated successfully\n", *userID)

	case "delete-user":
		if *userID == 0 {
			fmt.Fprintf(os.Stderr, "Error: Must specify user ID with -userid parameter\n")
			os.Exit(1)
		}
		err := admin.DeleteUser(ctx, *userID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to delete user: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("User %d deleted successfully\n", *userID)

	case "list-users":
		users, err := admin.ListUsers(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to list users: %v\n", err)
			os.Exit(1)
		}
		if len(users) == 0 {
			fmt.Println("No users found")
			return
		}
		fmt.Printf("Total users: %d\n\n", len(users))
		for _, u := range users {
			roleStr := "USER"
			if u.Role == core.ADMIN {
				roleStr = "ADMIN"
			}
			fmt.Printf("ID: %d\n", u.ID)
			fmt.Printf("  Username: %s\n", u.Usr)
			fmt.Printf("  Name: %s\n", u.Name)
			fmt.Printf("  Role: %s\n", roleStr)
			fmt.Println()
		}

	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown user management action: %s\n", *action)
		os.Exit(1)
	}
}

func handleBucketManagement() {
	// Load configuration for admin login
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Validate admin credentials
	if cfg.UserName == "" {
		fmt.Fprintf(os.Stderr, "Error: Admin username cannot be empty (use -user parameter or configuration file)\n")
		os.Exit(1)
	}
	if cfg.Password == "" {
		fmt.Fprintf(os.Stderr, "Error: Admin password cannot be empty (use -pass parameter or configuration file)\n")
		os.Exit(1)
	}

	// Create handler and login as admin
	handler := core.NewLocalHandler(cfg.BasePath, cfg.DataPath)
	ctx, userInfo, _, err := handler.Login(context.Background(), cfg.UserName, cfg.Password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Login failed: %v\n", err)
		os.Exit(1)
	}

	// Check if user is admin
	if userInfo.Role != core.ADMIN {
		fmt.Fprintf(os.Stderr, "Error: User %s is not an administrator\n", cfg.UserName)
		os.Exit(1)
	}

	// Create admin instance
	admin := core.NewLocalAdmin()
	defer admin.Close()

	// Execute bucket management command
	switch *action {
	case "create-bucket":
		if *bucketName == "" {
			fmt.Fprintf(os.Stderr, "Error: Must specify bucket name with -bucketname parameter\n")
			os.Exit(1)
		}

		// Determine bucket owner
		var ownerID int64
		var ownerName string
		if *bucketOwner != "" {
			// Get all users to find the owner
			allUsers, err := admin.ListUsers(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: Failed to list users: %v\n", err)
				os.Exit(1)
			}

			// Try to parse as user ID first
			if parsedID, err := strconv.ParseInt(*bucketOwner, 10, 64); err == nil && parsedID > 0 {
				// It's a user ID
				found := false
				for _, u := range allUsers {
					if u.ID == parsedID {
						ownerID = u.ID
						ownerName = u.Name
						found = true
						break
					}
				}
				if !found {
					fmt.Fprintf(os.Stderr, "Error: User with ID %d not found\n", parsedID)
					os.Exit(1)
				}
			} else {
				// It's a username
				found := false
				for _, u := range allUsers {
					if u.Usr == *bucketOwner {
						ownerID = u.ID
						ownerName = u.Name
						found = true
						break
					}
				}
				if !found {
					fmt.Fprintf(os.Stderr, "Error: User with username %s not found\n", *bucketOwner)
					os.Exit(1)
				}
			}
		} else {
			// Use current user as owner
			ownerID = userInfo.ID
			ownerName = userInfo.Name
		}

		// Generate new bucket ID
		bktID := core.NewID()
		if bktID <= 0 {
			fmt.Fprintf(os.Stderr, "Error: Failed to generate bucket ID\n")
			os.Exit(1)
		}

		// Create bucket with configuration from command line
		bucket := &core.BucketInfo{
			ID:           bktID,
			Name:         *bucketName,
			Type:         1, // Normal bucket
			Quota:        *bucketQuota,
			Used:         0,
			RealUsed:     0,
			LogicalUsed:  0,
			DedupSavings: 0,
			ChunkSize:    0, // Use default chunk size
		}

		// Note: Compression, encryption, and instant upload configuration are no longer stored in bucket
		// These should be provided via core.Config when mounting filesystem or using SDK

		err := admin.PutBkt(ctx, []*core.BucketInfo{bucket})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create bucket: %v\n", err)
			os.Exit(1)
		}

		// Create ACL entry for bucket owner (if ownerID is available)
		// PutBkt already creates ACL with ALL permission, so this is just for verification
		if ownerID > 0 {
			// ACL is automatically created by PutBkt with ALL permission
			fmt.Printf("Bucket owner ACL created with ALL permission\n")
		}

		fmt.Printf("Bucket created successfully:\n")
		fmt.Printf("  ID: %d\n", bucket.ID)
		fmt.Printf("  Name: %s\n", bucket.Name)
		if bucket.Quota < 0 {
			fmt.Printf("  Quota: Unlimited\n")
		} else {
			fmt.Printf("  Quota: %d bytes (%.2f GB)\n", bucket.Quota, float64(bucket.Quota)/(1024*1024*1024))
		}
		fmt.Printf("  Owner: %s (ID: %d)\n", ownerName, ownerID)
		// Note: Compression, encryption, and instant upload configuration are no longer stored in bucket
		// These should be provided via core.Config when mounting filesystem or using SDK

	case "delete-bucket":
		if *bucketID == 0 {
			fmt.Fprintf(os.Stderr, "Error: Must specify bucket ID with -bucket parameter\n")
			os.Exit(1)
		}

		// Verify bucket exists
		buckets, err := handler.GetBktInfo(ctx, *bucketID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Bucket %d not found: %v\n", *bucketID, err)
			os.Exit(1)
		}

		// Delete bucket
		err = admin.DeleteBkt(ctx, *bucketID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to delete bucket: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Bucket %d (%s) deleted successfully\n", *bucketID, buckets.Name)

	case "list-buckets":
		// Get all buckets for the user
		_, _, bucketList, err := handler.Login(context.Background(), cfg.UserName, cfg.Password)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get buckets: %v\n", err)
			os.Exit(1)
		}

		if len(bucketList) == 0 {
			fmt.Println("No buckets found")
			return
		}

		fmt.Printf("Total buckets: %d\n\n", len(bucketList))
		for _, bkt := range bucketList {
			fmt.Printf("ID: %d\n", bkt.ID)
			fmt.Printf("  Name: %s\n", bkt.Name)
			if bkt.Quota < 0 {
				fmt.Printf("  Quota: Unlimited\n")
			} else {
				fmt.Printf("  Quota: %d bytes (%.2f GB)\n", bkt.Quota, float64(bkt.Quota)/(1024*1024*1024))
			}
			fmt.Printf("  Used: %d bytes (%.2f GB)\n", bkt.Used, float64(bkt.Used)/(1024*1024*1024))
			fmt.Printf("  Real Used: %d bytes (%.2f GB)\n", bkt.RealUsed, float64(bkt.RealUsed)/(1024*1024*1024))
			// Note: Compression, encryption, and instant upload configuration are no longer stored in bucket
			// These should be provided via core.Config when mounting filesystem or using SDK
			fmt.Println()
		}

	case "update-bucket":
		if *bucketID == 0 {
			fmt.Fprintf(os.Stderr, "Error: Must specify bucket ID with -bucket parameter\n")
			os.Exit(1)
		}

		// Get current bucket info
		bucket, err := handler.GetBktInfo(ctx, *bucketID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Bucket %d not found: %v\n", *bucketID, err)
			os.Exit(1)
		}

		// Update compression configuration if provided
		// Note: Compression, encryption, and instant upload configuration are no longer stored in bucket
		// These should be provided via core.Config when mounting filesystem or using SDK
		// If these flags are provided, warn user that they are ignored
		if *cmprWay != "" || *cmprQlty > 0 || *endecWay != "" || *endecKey != "" || *refLevel != "" {
			fmt.Fprintf(os.Stderr, "Warning: Compression, encryption, and instant upload settings are no longer stored in bucket.\n")
			fmt.Fprintf(os.Stderr, "These should be provided via core.Config when mounting filesystem or using SDK.\n")
		}

		// For now, we still need to update bucket for other fields (if any)
		// But compression/encryption/refLevel are no longer supported
		updated := false
		// TODO: Add other bucket fields that can be updated here

		if !updated {
			fmt.Fprintf(os.Stderr, "Error: No configuration changes specified. Bucket-level compression/encryption/instant-upload settings are no longer supported.\n")
			fmt.Fprintf(os.Stderr, "These should be provided via core.Config when mounting filesystem or using SDK.\n")
			os.Exit(1)
		}

		// Update bucket
		err = admin.PutBkt(ctx, []*core.BucketInfo{bucket})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to update bucket: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Bucket %d updated successfully:\n", *bucketID)
		// Note: Compression, encryption, and instant upload configuration are no longer stored in bucket
		// These should be provided via core.Config when mounting filesystem or using SDK

	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown bucket management action: %s\n", *action)
		os.Exit(1)
	}
}

func getCompressionString(cmprWay uint32) string {
	switch {
	case cmprWay&core.DATA_CMPR_SNAPPY != 0:
		return "SNAPPY"
	case cmprWay&core.DATA_CMPR_ZSTD != 0:
		return "ZSTD"
	case cmprWay&core.DATA_CMPR_GZIP != 0:
		return "GZIP"
	case cmprWay&core.DATA_CMPR_BR != 0:
		return "BR"
	default:
		return "NONE"
	}
}

func getEncryptionString(endecWay uint32) string {
	switch {
	case endecWay&core.DATA_ENDEC_AES256 != 0:
		return "AES256"
	case endecWay&core.DATA_ENDEC_SM4 != 0:
		return "SM4"
	default:
		return "NONE"
	}
}

func getRefLevelString(refLevel uint32) string {
	switch refLevel {
	case 1:
		return "FULL"
	case 2:
		return "FAST"
	default:
		return "OFF"
	}
}

func maskKey(key string) string {
	if key == "" {
		return "(empty)"
	}
	if len(key) <= 8 {
		return "***"
	}
	return key[:4] + "..." + key[len(key)-4:]
}
