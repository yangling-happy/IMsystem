package main

import "syscall"

func main() {
	k := syscall.NewLazyDLL("kernel32.dll")
	k.NewProc("SetConsoleOutputCP").Call(65001)
	server := NewServer("127.0.0.1", 8888)
	server.Start()
}
