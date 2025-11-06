package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
)

var (
	configFile = flag.String("config", "", "Configuration file path (JSON format)")
	action     = flag.String("action", "", "Operation type: upload or download")
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

	// Load configuration
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
