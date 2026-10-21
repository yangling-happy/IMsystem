package main

import (
	"fmt"
	"net"
)

type Client struct {
	ServerIP   string
	ServerPort int
	conn       net.Conn
}

// NewClient 建立与服务器的 TCP 连接
func NewClient(serverIP string, serverPort int) (*Client, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", serverIP, serverPort))
	if err != nil {
		return nil, err
	}
	return &Client{
		ServerIP:   serverIP,
		ServerPort: serverPort,
		conn:       conn,
	}, nil
}

func main() {
	client, err := NewClient("127.0.0.1", 8888)
	if err != nil {
		fmt.Println("连接服务器失败:", err)
		return
	}
	defer client.conn.Close()
	fmt.Println("连接服务器成功!")
}
