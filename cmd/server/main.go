package main

import (
	"net"
	"strconv"
	"syscall"

	"imsystem/internal/config"
	"imsystem/internal/imserver"
)

func main() {
	// Windows 控制台 UTF-8 输出
	k := syscall.NewLazyDLL("kernel32.dll")
	k.NewProc("SetConsoleOutputCP").Call(65001)

	cfg := config.Load()
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
	server.Start()
}
