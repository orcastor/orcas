package core

const (
	DATA_DIR = "data"

	BKT_TBL  = "bkt"
	OBJ_TBL  = "obj"
	DATA_TBL = "data"
)

var config *CoreConfig

type CoreConfig struct {
	Path  string   `yaml:"path"`
	Chain []string `yaml:"chain"` // cli serv rw
}

func Init(c *CoreConfig) {
	config = c
}

func Conf() *CoreConfig {
	return config
}
