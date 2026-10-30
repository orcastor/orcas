package main

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
	"github.com/orcastor/orcas/rpc/util"
)

// Create handler (DBKey will be set via API or environment variable)
var hanlder = core.NewLocalHandler("", "")

func login(ctx *gin.Context) {
	var req struct {
		UserName string `json:"u"`
		Password string `json:"p"`
	}
	if !bindRequest(ctx, &req) {
		return
	}
	_, u, b, err := hanlder.Login(ctx.Request.Context(), req.UserName, req.Password)
	if err != nil {
		util.AbortResponse(ctx, 100, "incorrect username or password")
		return
	}
	token, _, err := middleware.GenerateToken(u.Usr, u.ID, u.Role)
	if err != nil {
		util.AbortResponse(ctx, 100, err.Error())
		return
	}
	util.Response(ctx, gin.H{
		"u":            u,
		"b":            b,
		"access_token": token,
	})
}

func list(ctx *gin.Context) {
	var req struct {
		BktID int64 `json:"b,omitempty"`
		PID   int64 `json:"p,omitempty"`
		core.ListOptions
	}
	if !bindRequest(ctx, &req) {
		return
	}
	if req.Count == 0 {
		req.Count = 1000
	}
	if req.Count < 0 || req.Count > 1000 {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"code": 100, "msg": "Count must be between 1 and 1000"})
		return
	}
	o, cnt, delimiter, err := hanlder.List(ctx.Request.Context(), req.BktID, req.PID, req.ListOptions)
	if err != nil {
		util.AbortResponse(ctx, 100, err.Error())
		return
	}
	switch req.Brief {
	case 1:
		for i := range o {
			o[i].Extra = ""
			o[i].PID = 0
			o[i].DataID = 0
		}
	case 2:
		for i := range o {
			o[i] = &core.ObjectInfo{ID: o[i].ID}
		}
	}
	util.Response(ctx, gin.H{
		"o": o,
		"c": cnt,
		"d": delimiter,
	})
}

func get(ctx *gin.Context) {
	var req struct {
		BktID int64 `json:"b,omitempty"`
		ID    int64 `json:"i,omitempty"`
	}
	if !bindRequest(ctx, &req) {
		return
	}
	o, err := hanlder.Get(ctx.Request.Context(), req.BktID, []int64{req.ID})
	if err != nil {
		util.AbortResponse(ctx, 100, err.Error())
		return
	}
	if len(o) > 0 {
		util.Response(ctx, gin.H{
			"o": o[0],
		})
		return
	}
	util.Response(ctx, gin.H{})
}

func token(ctx *gin.Context) {
	util.Response(ctx, gin.H{
		"u": middleware.GetUID(ctx),
	})
}

// bindRequest stops processing malformed JSON before any storage operation.
func bindRequest(c *gin.Context, request interface{}) bool {
	if err := c.ShouldBindJSON(request); err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		c.AbortWithStatusJSON(status, gin.H{"code": 100, "msg": "Invalid request body"})
		return false
	}
	return true
}
