package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		var code int = -1
		resp := c.Request.Response
		if resp != nil {
			code = resp.StatusCode
		}

		RequestTime(c.Request.Method, c.Request.URL.Path, time.Since(start).Seconds())
		RequestCount(c.Request.Method, c.Request.URL.Path, code)
	}
}

var (
	requestTime  *kitprometheus.Histogram
	requestCount *kitprometheus.Counter
)

func init() {
	requestTime = kitprometheus.NewHistogramFrom(prometheus.HistogramOpts{
		Namespace: "orcas",
		Subsystem: "s3",
		Name:      "request_time",
		Help:      "S3 request time cost.",
	}, []string{"method", "path"})

	requestCount = kitprometheus.NewCounterFrom(prometheus.CounterOpts{
		Namespace: "orcas",
		Subsystem: "s3",
		Name:      "request_count",
		Help:      "S3 request count.",
	}, []string{"method", "path", "code"})
}

func RequestTime(method, path string, tm float64) {
	requestTime.With([]string{
		"method", method,
		"path", path,
	}...).Observe(tm)
}

func RequestCount(method, path string, code int) {
	requestCount.With([]string{
		"method", method,
		"path", path,
		"code", strconv.FormatInt(int64(code), 10),
	}...).Add(1)
}
