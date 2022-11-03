package main

import (
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/rpc/middleware"
	"github.com/orcastor/orcas/rpc/util"

	"github.com/gin-gonic/gin"
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
		BktID int64            `json:"b,omitempty"`
		PID   int64            `json:"p,omitempty"`
		Opt   core.ListOptions `json:"o"`
	}
	ctx.BindJSON(&req)
	o, c, d, err := hanlder.List(ctx.Request.Context(), req.BktID, req.PID, req.Opt)
	if err != nil {
		util.AbortResponse(ctx, 100, err.Error())
		return
	}
	switch req.Opt.Brief {
	case 1:
		for i := range o {
			o[i].Extra = ""
		}
	case 2:
		for i := range o {
			o[i] = &core.ObjectInfo{ID: o[i].ID}
		}
	}
	util.Response(ctx, gin.H{
		"o": o,
		"c": c,
		"d": d,
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
