package core

import (
	"context"
	"os"
)

const ROOT_OID int64 = 0

var ORCAS_BASE = os.Getenv("ORCAS_BASE")
var ORCAS_DATA = os.Getenv("ORCAS_DATA")

type Ctx context.Context

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ERR_AUTH_FAILED   = Error("auth failed")
	ERR_NEED_LOGIN    = Error("need login")
	ERR_INCORRECT_PWD = Error("incorrect username or password")

	ERR_NO_PERM = Error("no permission")
	ERR_NO_ROLE = Error("role mismatch")

	ERR_OPEN_FILE = Error("open file failed")
	ERR_READ_FILE = Error("read file failed")

	ERR_OPEN_DB  = Error("open db failed")
	ERR_QUERY_DB = Error("query db failed")
	ERR_EXEC_DB  = Error("exec db failed")
	ERR_DUP_KEY  = Error("object with same name already exists")
)
