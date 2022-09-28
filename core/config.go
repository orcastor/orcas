package core

var config *CoreConfig

type CoreConfig struct {
	Path string `yaml:"path" json:"path,omitempty"`
}

func Init(c *CoreConfig) {
	config = c
}

func Conf() *CoreConfig {
	return config
}
