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
	SetAdapter(ma MetadataAdapter)

	CheckPermission(c Ctx, action int, bktID int64) error
	CheckRole(c Ctx, role uint32) error
}

type DefaultAccessCtrlMgr struct {
	ma MetadataAdapter
}

func (dacm *DefaultAccessCtrlMgr) SetAdapter(ma MetadataAdapter) {
	dacm.ma = ma
}

func (dacm *DefaultAccessCtrlMgr) CheckPermission(c Ctx, action int, bktID int64) error {
	uid := getUID(c)
	if uid <= 0 {
		return ERR_NEED_LOGIN
	}
	b, err := dacm.ma.GetBkt(c, []int64{bktID})
	if err != nil {
		return err
	}
	// check is owner of the bucket
	if len(b) > 0 && b[0].UID == uid {
		return nil
	}
	return ERR_NO_PERM
}

func (dacm *DefaultAccessCtrlMgr) CheckRole(c Ctx, role uint32) error {
	uid := getUID(c)
	if uid <= 0 {
		return ERR_NEED_LOGIN
	}
	u, err := dacm.ma.GetUsr(c, []int64{uid})
	if err != nil {
		return err
	}
	if len(u) > 0 && u[0].Role == role {
		return nil
	}
	return ERR_NO_ROLE
}

func userInfo2Ctx(c Ctx, u *UserInfo) Ctx {
	return context.WithValue(c, "o", map[string]interface{}{
		"uid": u.ID,
		"key": u.Key,
	})
}

func getUID(c Ctx) int64 {
	if v, ok := c.Value("o").(map[string]interface{}); ok {
		if uid, okk := v["uid"]; okk {
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
