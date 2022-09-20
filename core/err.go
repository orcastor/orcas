package core

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ERR_AUTH_FAILED = Error("auth failed")
	ERR_NEED_LOGIN  = Error("need login")

	ERR_OPEN_FILE = Error("open file failed")
	ERR_READ_FILE = Error("read file failed")

	ERR_OPEN_DB  = Error("open db failed")
	ERR_QUERY_DB = Error("query db failed")
	ERR_EXEC_DB  = Error("exec db failed")
)
