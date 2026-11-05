package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"imsystem/internal/config"
	"imsystem/internal/dedupe"
	"imsystem/internal/delayed"
	"imsystem/internal/imserver"
	"imsystem/internal/limiter"
	"imsystem/internal/metrics"
	"imsystem/internal/queue"
	"imsystem/internal/storage"
)

func main() {
	// Windows 控制台 UTF-8 输出
	k := syscall.NewLazyDLL("kernel32.dll")
	k.NewProc("SetConsoleOutputCP").Call(65001)

	cfg := config.Load()

	// main 是唯一的装配处：所有具体实现只在这里被 new 一次，
	// 业务包之间只依赖接口。将来把内存队列换成 Redis 队列，也只改这一行。
	repo, err := storage.NewMySQL(cfg.MySQLDSN)
	if err != nil {
		// 存储连不上就拒绝启动：带着"能收发但落不了库"的假象上线更危险
		log.Fatalf("初始化 MySQL 失败: %v", err)
	}
	defer repo.Close()

	// Redis 客户端内部自带连接池，这里只给地址和密码
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPass,
	})
	defer rdb.Close()

	// 和 MySQL 一样，启动就探活：Redis 不通时服务端跑起来只会把消息全丢在 Push 失败里
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPing()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("连接 Redis 失败: %v", err)
	}

	q := queue.NewRedisQueue(rdb, queue.MessageQueueKey)
	delayedQ := delayed.New(rdb, delayed.Key)
	rateLimiter := limiter.New(rdb, cfg.RateLimitPerSec, time.Second)
	dedupeGate := dedupe.New(rdb)

	// 指标独立端口：Prometheus 抓 :9091/metrics
	metrics.Serve(cfg.MetricsAddr)

	// ctx 是整个进程的生命周期：收到 Ctrl+C / SIGTERM 时取消，
	// Worker 也随之退出，避免 main 返回后还有协程在写库。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Worker 跑在独立协程里：网络读循环只负责入队，落库慢/失败都不影响收发
	go queue.NewWorker(repo).Run(ctx, q)

	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		host, portStr = "0.0.0.0", "8888"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 8888
	}

	server := imserver.NewServer(host, port)
	server.HeartbeatTimeout = cfg.HeartbeatTimeout
	server.Sink = q
	server.Delayed = delayedQ
	server.Limiter = rateLimiter
	server.Dedupe = dedupeGate

	// Start 会阻塞在 Accept，所以放协程里跑；主协程只等退出信号
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		if err := server.Start(); err != nil {
			log.Printf("服务端异常退出: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Println("收到退出信号，开始优雅关闭")

	// 先关监听和所有在线连接：不再进新消息，各读协程自然退出
	if err := server.Close(); err != nil {
		log.Printf("关闭监听失败: %v", err)
	}
	<-serverDone

	// 给 Worker 一点时间把已取出但还没落库的消息写完，再取消 ctx
	time.Sleep(200 * time.Millisecond)
	stop()

	// 退出前关掉外部资源（defer 里的 repo.Close/rdb.Close 现在才真正执行到）
	log.Println("服务已停止")
}
