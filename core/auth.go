package core

import (
	"context"
	"fmt"
	"time"

	"github.com/orca-zhang/ecache"
)

const (
	NA  = 1 << iota
	DR  // Data Read
	DW  // Data Write
	DD  // Data Delete
	MDR // Metadata Read
	MDW // Metadata Write
	MDD // Metadata Delete

	DRW    = DR | DW               // Data Read/Write
	MDRW   = MDR | MDW             // Metadata Read/Write
	READ   = DR | MDR              // Data and Metadata Read
	WRITE  = DW | MDW              // Data and Metadata Write
	DELETE = DD | MDD              // Data and Metadata Delete
	ALL    = READ | WRITE | DELETE // Data and Metadata Read/Write/Delete
)

const (
	USER  = iota // Regular User
	ADMIN        // Administrator
)

type AccessCtrlMgr interface {
	SetAdapter(ma MetadataAdapter)

	CheckPermission(c Ctx, action int, bktID int64) error
	CheckRole(c Ctx, role uint32) error
	CheckOwn(c Ctx, bktID int64) error
}

var cache = ecache.NewLRUCache(16, 256, 30*time.Second)

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
	bk := fmt.Sprintf("bkt:%d", bktID)
	if u, ok := cache.GetInt64(bk); ok {
		if u == uid {
			return nil
		} else {
			return ERR_NO_PERM
		}
	}
	b, err := dacm.ma.GetBkt(c, []int64{bktID})
	if err != nil {
		return err
	}
	// check is owner of the bucket
	if len(b) > 0 {
		cache.PutInt64(bk, b[0].UID)
		if b[0].UID == uid {
			return nil
		}
	}
	return ERR_NO_PERM
}

func (dacm *DefaultAccessCtrlMgr) CheckRole(c Ctx, role uint32) error {
	uid := getUID(c)
	if uid <= 0 {
		return ERR_NEED_LOGIN
	}

	// First check if role is already in context (for benchmark/testing)
	if v, ok := c.Value("o").(map[string]interface{}); ok {
		if ctxRole, okk := v["role"].(uint32); okk {
			if ctxRole == role {
				return nil
			}
		}
	}

	rk := fmt.Sprintf("role:%d", uid)
	if r, ok := cache.GetInt64(rk); ok {
		if uint32(r) == role {
			return nil
		} else {
			return ERR_NO_ROLE
		}
	}
	u, err := dacm.ma.GetUsr(c, []int64{uid})
	if err != nil {
		return err
	}
	if len(u) > 0 {
		cache.PutInt64(rk, int64(u[0].Role))
		if u[0].Role == role {
			return nil
		}
	}
	return ERR_NO_ROLE
}

// CheckOwn Check if the user is the owner of the bucket
func (dacm *DefaultAccessCtrlMgr) CheckOwn(c Ctx, bktID int64) error {
	uid := getUID(c)
	if uid <= 0 {
		return ERR_NEED_LOGIN
	}
	bk := fmt.Sprintf("bkt:%d", bktID)
	if u, ok := cache.GetInt64(bk); ok {
		if u == uid {
			return nil
		} else {
			return ERR_NO_PERM
		}
	}
	b, err := dacm.ma.GetBkt(c, []int64{bktID})
	if err != nil {
		return err
	}
	// Check if user is the owner of the bucket
	if len(b) > 0 {
		cache.PutInt64(bk, b[0].UID)
		if b[0].UID == uid {
			return nil
		}
	}
	return ERR_NO_PERM
}

func UserInfo2Ctx(c Ctx, u *UserInfo) Ctx {
	o := map[string]interface{}{
		"uid": u.ID,
	}
	// Also store role if provided (for benchmark/testing)
	if u != nil && u.Role > 0 {
		o["role"] = u.Role
	}
	return context.WithValue(c, "o", o)
}

// BucketInfo2Ctx sets bucket information to context, including bucket key
// This allows GetDB to use bucket's key for database encryption
func BucketInfo2Ctx(c Ctx, bkt *BucketInfo) Ctx {
	if bkt == nil {
		return c
	}
	// Get existing context values
	o := make(map[string]interface{})
	if v, ok := c.Value("o").(map[string]interface{}); ok {
		for k, val := range v {
			o[k] = val
		}
	}
	// Set bucket key (bucket key takes precedence over user key for bucket database)
	if bkt.Key != "" {
		o["key"] = bkt.Key
	}
	// Set bucket ID for reference
	o["bktID"] = bkt.ID
	return context.WithValue(c, "o", o)
}

func getUID(c Ctx) int64 {
	if v, ok := c.Value("o").(map[string]interface{}); ok {
		if uid, okk := v["uid"].(int64); okk {
			return uid
		}
	}
	return 0
}

func getKey(c Ctx) string {
	if v, ok := c.Value("o").(map[string]interface{}); ok {
		// Get key from context (could be user key or bucket key set via BucketInfo2Ctx)
		if key, okk := v["key"].(string); okk {
			return key
		}
	}
	return ""
}
