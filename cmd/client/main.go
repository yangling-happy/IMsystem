package main

import (
	"flag"
	"fmt"
	"net"
)

type Client struct {
	ServerIP   string
	ServerPort int
	conn       net.Conn
	choice     int
}

// NewClient 建立与服务器的 TCP 连接
func NewClient(serverIP string, serverPort int) (*Client, error) {
	conn, err := net.Dial("tcp", net.JoinHostPort(serverIP, fmt.Sprintf("%d", serverPort)))
	if err != nil {
		return nil, err
	}
	return &Client{
		ServerIP:   serverIP,
		ServerPort: serverPort,
		conn:       conn,
		choice:     99,
	}, nil
}
func (client *Client) Menu() bool {
	var choice int
	fmt.Println("请选择功能:")
	fmt.Println("1.公聊 ")
	fmt.Println("2.私聊")
	fmt.Println("3.更新用户名")

	fmt.Scanln(&choice)

	if choice >= 0 && choice <= 3 {
		client.choice = choice
		return true
	} else {
		fmt.Println("输入错误")
		return false
	}
}
func (client *Client) Run() {
	for client.choice != 0 {
		for !client.Menu() {
		}
		switch client.choice {
		case 1:
			fmt.Println("公聊")
			break
		case 2:
			fmt.Println("私聊")
			break
		case 3:
			fmt.Println("更新用户名")
			break
		}
	}
}
func main() {
	flag.Parse()
	client, err := NewClient("127.0.0.1", 8888)
	if err != nil {
		fmt.Println("连接服务器失败:", err)
		return
	}
	defer client.conn.Close()
	fmt.Println("连接服务器成功!")
	client.Run()
}
