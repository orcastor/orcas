package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

var (
	configFile = flag.String("config", "", "Configuration file path (JSON format)")
	action     = flag.String("action", "", "Operation type: upload, download, add-user, update-user, delete-user, list-users")
	localPath  = flag.String("local", "", "Local file or directory path")
	remotePath = flag.String("remote", "", "Remote path (relative to bucket root directory)")
	bucketID   = flag.Int64("bucket", 0, "Bucket ID (if not specified, use first bucket)")

	// Configuration parameters (can be set via command line arguments or configuration file)
	userName = flag.String("user", "", "Username")
	password = flag.String("pass", "", "Password")
	dataSync = flag.String("datasync", "", "Data sync (power failure protection): true or false")
	refLevel = flag.String("reflevel", "", "Instant upload level: OFF, FULL, FAST")
	wiseCmpr = flag.String("wisecmpr", "", "Smart compression: SNAPPY, ZSTD, GZIP, BR")
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
)

type Config struct {
	UserName string `json:"user_name"`
	Password string `json:"password"`
	DataSync bool   `json:"data_sync"`
	RefLevel string `json:"ref_level"`
	WiseCmpr string `json:"wise_cmpr"`
	CmprQlty int    `json:"cmpr_qlty"`
	EndecWay string `json:"endec_way"`
	EndecKey string `json:"endec_key"`
	DontSync string `json:"dont_sync"`
	Conflict string `json:"conflict"`
	NameTmpl string `json:"name_tmpl"`
	WorkersN int    `json:"workers_n"`
}

func main() {
	flag.Parse()

	// Initialize database
	core.InitDB()

	// Handle user management commands
	if *action == "add-user" || *action == "update-user" || *action == "delete-user" || *action == "list-users" {
		handleUserManagement()
		return
	}

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
		fmt.Fprintf(os.Stderr, "Error: Must specify operation type (use -action upload or -action download)\n")
		flag.Usage()
		os.Exit(1)
	}
	if *localPath == "" {
		fmt.Fprintf(os.Stderr, "Error: Must specify local path (use -local parameter)\n")
		flag.Usage()
		os.Exit(1)
	}

	// Convert configuration to SDK Config
	sdkCfg := convertToSDKConfig(cfg)

	// Create SDK instance
	handler := core.NewLocalHandler()
	sdkInstance := sdk.New(handler)
	defer sdkInstance.Close()

	// Login
	ctx, userInfo, buckets, err := sdkInstance.Login(sdkCfg)
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
		fmt.Fprintf(os.Stderr, "Error: Unknown operation type %s (supported: upload, download)\n", *action)
		os.Exit(1)
	}
}

func loadConfig() (*Config, error) {
	cfg := &Config{}

	// If configuration file is specified, load it first
	if *configFile != "" {
		if err := loadJSONConfig(*configFile, cfg); err != nil {
			return nil, fmt.Errorf("failed to load configuration file: %v", err)
		}
	}

	// Command line arguments override values in configuration file
	if *userName != "" {
		cfg.UserName = *userName
	}
	if *password != "" {
		cfg.Password = *password
	}
	// datasync is a boolean value, if command line provides a value, parse and override
	if *dataSync != "" {
		if *dataSync == "true" || *dataSync == "1" {
			cfg.DataSync = true
		} else {
			cfg.DataSync = false
		}
	}
	if *refLevel != "" {
		cfg.RefLevel = *refLevel
	}
	if *wiseCmpr != "" {
		cfg.WiseCmpr = *wiseCmpr
	}
	// cmprqlty default value is 0, if command line sets a value greater than 0, use it
	if *cmprQlty > 0 {
		cfg.CmprQlty = *cmprQlty
	}
	if *endecWay != "" {
		cfg.EndecWay = *endecWay
	}
	if *endecKey != "" {
		cfg.EndecKey = *endecKey
	}
	if *dontSync != "" {
		cfg.DontSync = *dontSync
	}
	if *conflict != "" {
		cfg.Conflict = *conflict
	}
	if *nameTmpl != "" {
		cfg.NameTmpl = *nameTmpl
	}
	// workers default value is 0, if command line sets a value greater than 0, use it
	if *workersN > 0 {
		cfg.WorkersN = *workersN
	}

	return cfg, nil
}

func loadJSONConfig(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, cfg)
}

func convertToSDKConfig(cfg *Config) sdk.Config {
	sdkCfg := sdk.Config{
		UserName: cfg.UserName,
		Password: cfg.Password,
		DataSync: cfg.DataSync,
		DontSync: cfg.DontSync,
		NameTmpl: cfg.NameTmpl,
	}

	// Convert RefLevel
	switch cfg.RefLevel {
	case "FULL":
		sdkCfg.RefLevel = sdk.FULL
	case "FAST":
		sdkCfg.RefLevel = sdk.FAST
	default:
		sdkCfg.RefLevel = sdk.OFF
	}

	// Convert WiseCmpr
	switch cfg.WiseCmpr {
	case "SNAPPY":
		sdkCfg.WiseCmpr = core.DATA_CMPR_SNAPPY
	case "ZSTD":
		sdkCfg.WiseCmpr = core.DATA_CMPR_ZSTD
	case "GZIP":
		sdkCfg.WiseCmpr = core.DATA_CMPR_GZIP
	case "BR":
		sdkCfg.WiseCmpr = core.DATA_CMPR_BR
	}

	if cfg.CmprQlty > 0 {
		sdkCfg.CmprQlty = uint32(cfg.CmprQlty)
	}

	// Convert EndecWay
	switch cfg.EndecWay {
	case "AES256":
		sdkCfg.EndecWay = core.DATA_ENDEC_AES256
		sdkCfg.EndecKey = cfg.EndecKey
	case "SM4":
		sdkCfg.EndecWay = core.DATA_ENDEC_SM4
		sdkCfg.EndecKey = cfg.EndecKey
	}

	// Convert Conflict
	switch cfg.Conflict {
	case "RENAME":
		sdkCfg.Conflict = sdk.RENAME
	case "THROW":
		sdkCfg.Conflict = sdk.THROW
	case "SKIP":
		sdkCfg.Conflict = sdk.SKIP
	default:
		sdkCfg.Conflict = sdk.COVER
	}

	if cfg.WorkersN > 0 {
		sdkCfg.WorkersN = uint32(cfg.WorkersN)
	}

	return sdkCfg
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
	handler := core.NewLocalHandler()
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
			fmt.Fprintf(os.Stderr, "Error: Must specify password with -newpass parameter\n")
			os.Exit(1)
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
		var role *uint32
		if *userRole != "" {
			r := uint32(core.USER)
			if *userRole == "ADMIN" {
				r = uint32(core.ADMIN)
			}
			role = &r
		}
		err := admin.UpdateUser(ctx, *userID, *newUser, *newPass, *userName_, role)
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
