package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"imsystem/internal/model"
	"imsystem/internal/protocol"
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

// send 编码成一行 JSON 后写出。所有发送路径都必须经过这里，
// 避免出现"某处还在用旧字符串协议"的半吊子状态。
func (client *Client) send(env protocol.Envelope) error {
	b, err := protocol.Encode(env)
	if err != nil {
		return err
	}
	_, err = client.conn.Write(b)
	return err
}

// newMsgID 生成消息唯一标识，P3 的幂等去重和 ACK 都靠它。
func newMsgID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
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
	if err := client.send(protocol.Envelope{Type: protocol.TypeWho}); err != nil {
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
				env := protocol.Envelope{
					Type:     protocol.TypeChat,
					MsgID:    newMsgID(),
					To:       remoteName,
					ChatType: string(model.ChatSingle),
					Body:     chatMsg,
				}
				if err := client.send(env); err != nil {
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
			env := protocol.Envelope{
				Type:     protocol.TypeChat,
				MsgID:    newMsgID(),
				ChatType: string(model.ChatGroup),
				Body:     chatMsg,
			}
			if err := client.send(env); err != nil {
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

	env := protocol.Envelope{Type: protocol.TypeRename, Body: client.Name}
	if err := client.send(env); err != nil {
		fmt.Println("发送消息错误:", err)
		return false
	}
	return true
}

// ReadServer 逐行读取服务器报文并渲染
func (client *Client) ReadServer() {
	scanner := bufio.NewScanner(client.conn)
	scanner.Buffer(make([]byte, 0, 1024), 64*1024)

	for scanner.Scan() {
		env, err := protocol.Decode(scanner.Bytes())
		if err != nil {
			continue
		}
		switch env.Type {
		case protocol.TypeChat:
			if env.ChatType == string(model.ChatGroup) {
				fmt.Printf("[公聊] %s: %s\n", env.From, env.Body)
			} else {
				fmt.Printf("[私聊] %s -> %s: %s\n", env.From, env.To, env.Body)
			}
		default:
			fmt.Println(env.Body)
		}
	}
	fmt.Println("与服务器断开连接:", scanner.Err())
}

func (client *Client) Run() {
	for client.choice != 0 {
		for !client.Menu() {
		}
		switch client.choice {
		case 1:
			fmt.Println("公聊")
			client.PublicChat()
		case 2:
			fmt.Println("私聊")
			client.PrivateChat()
		case 3:
			fmt.Println("更新用户名")
			client.Update()
		}
	}
}

func main() {
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
