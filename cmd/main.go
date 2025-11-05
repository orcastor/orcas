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
	configFile = flag.String("config", "", "配置文件路径 (JSON 格式)")
	action     = flag.String("action", "", "操作类型: upload 或 download")
	localPath  = flag.String("local", "", "本地文件或目录路径")
	remotePath = flag.String("remote", "", "远程路径 (相对于 bucket 根目录)")
	bucketID   = flag.Int64("bucket", 0, "Bucket ID (如果不指定，使用第一个 bucket)")

	// 配置参数 (可以通过命令行参数或配置文件设置)
	userName = flag.String("user", "", "用户名")
	password = flag.String("pass", "", "密码")
	dataSync = flag.String("datasync", "", "数据同步 (断电保护): true 或 false")
	refLevel = flag.String("reflevel", "", "秒传级别: OFF, FULL, FAST")
	wiseCmpr = flag.String("wisecmpr", "", "智能压缩: SNAPPY, ZSTD, GZIP, BR")
	cmprQlty = flag.Int("cmprqlty", 0, "压缩级别")
	endecWay = flag.String("endecway", "", "加密方式: AES256, SM4")
	endecKey = flag.String("endeckey", "", "加密密钥")
	dontSync = flag.String("dontsync", "", "不同步的文件名通配符 (用分号分隔)")
	conflict = flag.String("conflict", "", "同名冲突解决方式: COVER, RENAME, THROW, SKIP")
	nameTmpl = flag.String("nametmpl", "", "重命名模板 (包含 %s)")
	workersN = flag.Int("workers", 0, "并发池大小 (不小于16)")
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

	// 初始化数据库
	core.InitDB()

	// 加载配置
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}

	// 验证必需参数
	if cfg.UserName == "" {
		fmt.Fprintf(os.Stderr, "错误: 用户名不能为空 (使用 -user 参数或配置文件设置)\n")
		os.Exit(1)
	}
	if cfg.Password == "" {
		fmt.Fprintf(os.Stderr, "错误: 密码不能为空 (使用 -pass 参数或配置文件设置)\n")
		os.Exit(1)
	}
	if *action == "" {
		fmt.Fprintf(os.Stderr, "错误: 必须指定操作类型 (使用 -action upload 或 -action download)\n")
		flag.Usage()
		os.Exit(1)
	}
	if *localPath == "" {
		fmt.Fprintf(os.Stderr, "错误: 必须指定本地路径 (使用 -local 参数)\n")
		flag.Usage()
		os.Exit(1)
	}

	// 转换配置为 SDK Config
	sdkCfg := convertToSDKConfig(cfg)

	// 创建 SDK 实例
	handler := core.NewLocalHandler()
	sdkInstance := sdk.New(handler)
	defer sdkInstance.Close()

	// 登录
	ctx, userInfo, buckets, err := sdkInstance.Login(sdkCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "登录失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("登录成功: 用户 %s (ID: %d)\n", userInfo.Name, userInfo.ID)
	if len(buckets) == 0 {
		fmt.Fprintf(os.Stderr, "错误: 没有可用的 bucket\n")
		os.Exit(1)
	}

	// 选择 bucket
	var selectedBktID int64
	if *bucketID > 0 {
		selectedBktID = *bucketID
		// 验证 bucket 是否存在
		found := false
		for _, b := range buckets {
			if b.ID == selectedBktID {
				found = true
				break
			}
		}
		if !found {
			fmt.Fprintf(os.Stderr, "错误: Bucket ID %d 不存在\n", *bucketID)
			os.Exit(1)
		}
	} else {
		selectedBktID = buckets[0].ID
		fmt.Printf("使用 Bucket: %s (ID: %d)\n", buckets[0].Name, buckets[0].ID)
	}

	// 执行操作
	var pid int64 = core.ROOT_OID
	if *action == "upload" {
		// 如果指定了远程路径，需要找到父目录 ID
		remotePathStr := *remotePath
		if *remotePath != "" && *remotePath != "/" {
			pid, err = sdkInstance.Path2ID(ctx, selectedBktID, core.ROOT_OID, *remotePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "错误: 无法找到远程路径 %s: %v\n", *remotePath, err)
				os.Exit(1)
			}
		} else {
			remotePathStr = "/"
		}

		fmt.Printf("开始上传: %s -> %s\n", *localPath, remotePathStr)
		err = sdkInstance.Upload(ctx, selectedBktID, pid, *localPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "上传失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("上传完成!")
	} else if *action == "download" {
		// 下载需要先通过路径找到 ID
		if *remotePath == "" {
			fmt.Fprintf(os.Stderr, "错误: 下载操作必须指定远程路径 (使用 -remote 参数)\n")
			os.Exit(1)
		}

		pid, err = sdkInstance.Path2ID(ctx, selectedBktID, core.ROOT_OID, *remotePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "错误: 无法找到远程路径 %s: %v\n", *remotePath, err)
			os.Exit(1)
		}

		fmt.Printf("开始下载: /%s -> %s\n", *remotePath, *localPath)
		err = sdkInstance.Download(ctx, selectedBktID, pid, *localPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "下载失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("下载完成!")
	} else {
		fmt.Fprintf(os.Stderr, "错误: 未知的操作类型 %s (支持: upload, download)\n", *action)
		os.Exit(1)
	}
}

func loadConfig() (*Config, error) {
	cfg := &Config{}

	// 如果指定了配置文件，先加载配置文件
	if *configFile != "" {
		if err := loadJSONConfig(*configFile, cfg); err != nil {
			return nil, fmt.Errorf("加载配置文件失败: %v", err)
		}
	}

	// 命令行参数覆盖配置文件中的值
	if *userName != "" {
		cfg.UserName = *userName
	}
	if *password != "" {
		cfg.Password = *password
	}
	// datasync 是布尔值，如果命令行提供了值，则解析并覆盖
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
	// cmprqlty 默认值为 0，如果命令行设置了值且大于 0，则使用
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
	// workers 默认值为 0，如果命令行设置了值且大于 0，则使用
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

	// 转换 RefLevel
	switch cfg.RefLevel {
	case "FULL":
		sdkCfg.RefLevel = sdk.FULL
	case "FAST":
		sdkCfg.RefLevel = sdk.FAST
	default:
		sdkCfg.RefLevel = sdk.OFF
	}

	// 转换 WiseCmpr
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

	// 转换 EndecWay
	switch cfg.EndecWay {
	case "AES256":
		sdkCfg.EndecWay = core.DATA_ENDEC_AES256
		sdkCfg.EndecKey = cfg.EndecKey
	case "SM4":
		sdkCfg.EndecWay = core.DATA_ENDEC_SM4
		sdkCfg.EndecKey = cfg.EndecKey
	}

	// 转换 Conflict
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
