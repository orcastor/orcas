package util

import "github.com/gin-gonic/gin"

func AbortResponse(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(200, gin.H{
		"code": code,
		"msg":  msg,
	})
}

func Response(c *gin.Context, data gin.H) {
	c.AbortWithStatusJSON(200, gin.H{
		"code": 0,
		"data": data,
	})
}
