package main

import (
	"flag"
	"fmt"
	"net"
)

type Client struct {
	ServerIP   string
	ServerPort int
	Name       string
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
func (client *Client) SelectUser() {
	sendMsg := "who\n"
	_, err := client.conn.Write([]byte(sendMsg))
	if err != nil {
		fmt.Println("发送消息错误:", err)
		return
	}
}
func (client *Client) PrivateChat() {
	var remoteName string
	var chatMsg string
	client.SelectUser()
	fmt.Println("请选择用户....(输入exit退出)")
	fmt.Scanln(&remoteName)

	for remoteName != "exit" {
		fmt.Println("请输入聊天内容,输入exit退出")
		fmt.Scanln(&chatMsg)

		for chatMsg != "exit" {
			if len(chatMsg) != 0 {
				sendMsg := "to|" + remoteName + "|" + chatMsg + "\n"
				_, err := client.conn.Write([]byte(sendMsg))
				if err != nil {
					fmt.Println("发送消息错误:", err)
					break
				}
			}
			chatMsg = ""
			fmt.Println("请输入聊天内容,输入exit退出")
			fmt.Scanln(&chatMsg)
		}
		client.SelectUser()
		fmt.Println("请选择用户....(输入exit退出)")
		fmt.Scanln(&remoteName)
	}
}
func (client *Client) PublicChat() {
	var chatMsg string
	fmt.Println("请输入聊天内容,输入exit退出")
	fmt.Scanln(&chatMsg)

	for chatMsg != "exit" {
		if len(chatMsg) != 0 {
			sendMsg := chatMsg + "\n"
			_, err := client.conn.Write([]byte(sendMsg))
			if err != nil {
				fmt.Println("发送消息错误:", err)
				break
			}
		}
		chatMsg = ""
		fmt.Println("请输入聊天内容,输入exit退出")
		fmt.Scanln(&chatMsg)
	}
}
func (client *Client) Update() bool {
	fmt.Println("请输入用户名:")
	fmt.Scanln(&client.Name)

	sendMsg := "rename|" + client.Name + "\n"
	_, err := client.conn.Write([]byte(sendMsg))
	if err != nil {
		fmt.Println("发送消息错误:", err)
		return false
	}
	return true
}

// ReadServer 读取服务器发回的消息
func (client *Client) ReadServer() {
	buf := make([]byte, 4096)
	for {
		n, err := client.conn.Read(buf)
		if err != nil {
			fmt.Println("与服务器断开连接:", err)
			return
		}
		fmt.Print(string(buf[:n]))
	}
}

func (client *Client) Run() {
	for client.choice != 0 {
		for !client.Menu() {
		}
		switch client.choice {
		case 1:
			fmt.Println("公聊")
			client.PublicChat()
			break
		case 2:
			fmt.Println("私聊")
			client.PrivateChat()
			break
		case 3:
			fmt.Println("更新用户名")
			client.Update()
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
	go client.ReadServer()
	client.Run()
}
