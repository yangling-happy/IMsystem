package main

import (
	"syscall"

	"imsystem/internal/imserver"
)

func main() {
	// Windows 控制台 UTF-8 输出
	k := syscall.NewLazyDLL("kernel32.dll")
	k.NewProc("SetConsoleOutputCP").Call(65001)

	server := imserver.NewServer("127.0.0.1", 8888)
	server.Start()
}
