package core

const (
	NA = iota
	R
	W
	RW
)

const (
	USER = iota
	ADMIN
)

type AccessCtrlMgr interface {
	CheckPermission(c Ctx, action int, bktID int64) error
	CheckRole(c Ctx, role int) error
}

type Users interface {
	Auth(c Ctx, usr, pwd string) (int64, error)
	ChPwd(c Ctx, uid int64, pwd, newPwd string) error

	AddUsr(c Ctx, usr, pwd string) error
	DelUsr(c Ctx, usr, pwd string) error

	NewTOTP(c Ctx) (string, string)
	CheckTOTP(c Ctx, key, token string) (bool, error)
}

type DefaultAccessCtrlMgr struct {
}

func (dac *DefaultAccessCtrlMgr) CheckPermission(c Ctx, action int, bktID int64) error {
	return nil
}

func (dac *DefaultAccessCtrlMgr) CheckRole(c Ctx, role int) error {
	return nil
}
