package core

import "context"

type Ctx context.Context

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
