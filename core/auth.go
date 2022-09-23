package core

import "context"

const (
	NA  = 1 << iota
	DR  // 数据读取
	DW  // 数据写入
	DD  // 数据删除
	MDR // 元数据读取
	MDW // 元数据写入
	MDD // 元数据删除

	DRW  = DR | DW               // 数据读写
	MDRW = MDR | MDW             // 元数据读写
	ALL  = DRW | DD | MDRW | MDD // 数据、元数据读写删
)

const (
	USER  = iota // 普通用户
	ADMIN        // 管理员
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

func userInfo2Ctx(c Ctx, u *UserInfo) Ctx {
	return context.WithValue(c, "o", map[string]interface{}{
		"uid": u.ID,
		"key": u.Key,
	})
}

func getUID(c Ctx) int64 {
	if v, ok := c.Value("o").(map[string]interface{}); ok {
		if uid, okk := v["id"]; okk {
			if u, okkk := uid.(int64); okkk {
				return u
			}
		}
	}
	return 0
}

func getKey(c Ctx) string {
	if v, ok := c.Value("o").(map[string]interface{}); ok {
		if key, okk := v["key"]; okk {
			if k, okkk := key.(string); okkk {
				return k
			}
		}
	}
	return ""
}
