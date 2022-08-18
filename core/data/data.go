package data

type Option struct {
}

type DataOperator interface {
	SetOption(opt Option)
	Write(file string, buf []byte) error
	Read(file string) ([]byte, error)
	ReadBytes(file string, offset int, size int) ([]byte, error)
}
