package main

import (
	"github.com/gin-gonic/gin"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
	"github.com/orcastor/orcas/rpc/util"
)

var hanlder = core.NewLocalHandler()

func login(ctx *gin.Context) {
	var req struct {
		UserName string `json:"u"`
		Password string `json:"p"`
	}
	ctx.BindJSON(&req)
	_, u, b, err := hanlder.Login(ctx.Request.Context(), req.UserName, req.Password)
	if err != nil {
		util.AbortResponse(ctx, 100, err.Error())
		return
	}
	token, _, err := middleware.GenerateToken(u.Usr, u.ID, u.Role)
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
	ctx.BindJSON(&req)
	if req.Count == 0 {
		req.Count = 1000
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
	ctx.BindJSON(&req)
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
