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

type DefaultAccessCtrlMgr struct {
}

func (dacm *DefaultAccessCtrlMgr) CheckPermission(c Ctx, action int, bktID int64) error {
	// check owner of bucket
	return nil
}

func (dacm *DefaultAccessCtrlMgr) CheckRole(c Ctx, role int) error {
	return nil
}
