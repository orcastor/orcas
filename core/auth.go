package core

const (
	NA = 1 << iota
	DR
	DW
	DD
	MDR
	MDW
	MDD

	DRW  = DR | DW
	MDRW = MDR | MDW
	ALL  = DRW | DD | MDRW | MDD
)

const (
	USER = iota
	ADMIN
)

type AccessCtrlMgr interface {
	CheckPermission(c Ctx, action int, bktID int64) error
	CheckRole(c Ctx, role int) error
}

type Admin interface {
	ChPwd(c Ctx, uid int64, pwd, newPwd string) error
	ChRole(c Ctx, uid int64, role int) error

	AddUsr(c Ctx, usr, pwd string) error
	DelUsr(c Ctx, usr, pwd string) error

	NewOTP(c Ctx) (string, string)
}

type DefaultAccessCtrlMgr struct {
}

func (dac *DefaultAccessCtrlMgr) CheckPermission(c Ctx, action int, bktID int64) error {
	return nil
}

func (dac *DefaultAccessCtrlMgr) CheckRole(c Ctx, role int) error {
	return nil
}

func Auth(c Ctx, usr, pwd, otp string) error {
	// pwd -> rsa decode
	// pwd from db or cache
	// pbkdf2 check
	// set uid & key to ctx
	return ERR_AUTH_FAILED
}
