// Package metrics 汇总服务端要暴露给 Prometheus 的指标。
package metrics

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// OnlineUsers 是当前在线连接数（Gauge：可增可减）。
	OnlineUsers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "im_online_users",
		Help: "当前在线连接数",
	})

	// MessagesTotal 按处理结果分类计数（Counter：只增不减）。
	// QPS = rate(im_messages_total[1m])，不需要单独做 gauge。
	MessagesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "im_messages_total",
		Help: "聊天消息处理结果计数",
	}, []string{"result"})

	// PersistDuration 是消息落库耗时，用来观察 MySQL 是否成为瓶颈。
	PersistDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "im_persist_duration_seconds",
		Help:    "单条消息落库耗时",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})

	RateLimitedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_rate_limited_total",
		Help: "被限流拒绝的消息数",
	})

	DuplicateTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_duplicate_total",
		Help: "被幂等去重拦截的消息数",
	})

	AckTimeoutTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_ack_timeout_total",
		Help: "客户端重试次数（未及时收到 ACK 的消息）",
	})

	PersistedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_persisted_total",
		Help: "成功落库的消息数",
	})
)

func init() {
	prometheus.MustRegister(
		OnlineUsers,
		MessagesTotal,
		PersistDuration,
		RateLimitedTotal,
		DuplicateTotal,
		AckTimeoutTotal,
		PersistedTotal,
	)
}

// Serve 在独立端口暴露 /metrics。
// 用独立端口而不是塞进 8888：主服务是裸 TCP、没有 HTTP 路由，
// 混在一起要么改造主服务，要么给指标开个新端口 —— 后者更干净。
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("指标服务启动失败: %v", err)
		}
	}()
}
