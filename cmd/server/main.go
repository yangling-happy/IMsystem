package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"syscall"

	"imsystem/internal/config"
	"imsystem/internal/imserver"
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

	q := queue.NewMemoryQueue(cfg.SendQueueSize)

	// Worker 跑在独立协程里：网络读循环只负责入队，落库慢/失败都不影响收发
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	server.Start()
}
