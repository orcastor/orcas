package core

import (
	"context"
	"time"

	"github.com/orca-zhang/ecache2"
)

const (
	NA  = 0
	DR  = 1 << (iota - 1) // Data Read
	DW                    // Data Write
	DD                    // Data Delete
	MDR                   // Metadata Read
	MDW                   // Metadata Write
	MDD                   // Metadata Delete

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

// cache for bucket ownership: key [2]int64{1, bktID}, value: ownerUID (int64)
// cache for user role: key [2]int64{2, uid}, value: role (int64)
var cache = ecache2.NewLRUCache[[2]int64](16, 256, 30*time.Second)

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
	// Check if user has the required permission via ACL
	hasPermission, err := dacm.ma.CheckPermission(c, bktID, uid, action)
	if err != nil {
		return err
	}
	if hasPermission {
		// Cache the permission check result
		bk := [2]int64{1, bktID} // Type 1 = bucket access
		cache.Put(bk, uid)
		return nil
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

	rk := [2]int64{2, uid} // Type 2 = user role
	if v, ok := cache.Get(rk); ok {
		if r, ok := v.(int64); ok && uint32(r) == role {
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
		cache.Put(rk, int64(u[0].Role))
		if u[0].Role == role {
			return nil
		}
	}
	return ERR_NO_ROLE
}

// CheckOwn Check if the user has access to the bucket (via ACL)
// This is equivalent to checking if user has any permission (ALL)
func (dacm *DefaultAccessCtrlMgr) CheckOwn(c Ctx, bktID int64) error {
	uid := getUID(c)
	if uid <= 0 {
		return ERR_NEED_LOGIN
	}
	// Check if user has any permission (we use READ as minimum permission check)
	hasPermission, err := dacm.ma.CheckPermission(c, bktID, uid, READ)
	if err != nil {
		return err
	}
	if hasPermission {
		// Cache the permission check result
		bk := [2]int64{1, bktID} // Type 1 = bucket access
		cache.Put(bk, uid)
		return nil
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
	// Key is no longer stored in bucket config, should be provided via context or other means
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

// NoAuthAccessCtrlMgr is an AccessCtrlMgr that bypasses all permission checks
// This bypasses authentication and authorization checks that require the main database
// (user table and ACL table), but bucket database operations (data and object metadata)
// are still performed normally.
// This is useful for testing, internal operations, or when authentication is handled externally
type NoAuthAccessCtrlMgr struct{}

func (nacm *NoAuthAccessCtrlMgr) SetAdapter(ma MetadataAdapter) {
	// No-op: NoAuthAccessCtrlMgr doesn't need MetadataAdapter
	// It bypasses permission checks, so it doesn't need to query the main database
}

func (nacm *NoAuthAccessCtrlMgr) CheckPermission(c Ctx, action int, bktID int64) error {
	// Always allow: bypass permission check (no main database ACL query)
	// Bucket database operations (data/object metadata) will still be performed
	return nil
}

func (nacm *NoAuthAccessCtrlMgr) CheckRole(c Ctx, role uint32) error {
	// Always allow: bypass role check (no main database user query)
	return nil
}

func (nacm *NoAuthAccessCtrlMgr) CheckOwn(c Ctx, bktID int64) error {
	// Always allow: bypass ownership check (no main database ACL query)
	// Bucket database operations will still be performed normally
	return nil
}
