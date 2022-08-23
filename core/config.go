package core

import "context"

type Ctx context.Context

const (
	DATA_DIR = "data"

	BKT_TBL = "bkt"
	USR_TBL = "usr"
	ACL_TBL = "acl"

	OBJ_TBL  = "obj"
	DATA_TBL = "data"
)

var config *CoreConfig

type CoreConfig struct {
	Path string `yaml:"path"`
}

func Init(c *CoreConfig) {
	config = c
}

func Conf() *CoreConfig {
	return config
}
