package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"imsystem/internal/model"
	"imsystem/internal/protocol"
)

const (
	// ackTimeout 是等 ACK 的时长：超过就重发。
	ackTimeout = 2 * time.Second
	// maxSendRetry 是最多重发次数，用完就放弃并提示用户。
	maxSendRetry = 3
	// clientHeartbeatEvery 是客户端心跳间隔。服务端超时默认 90 秒，
	// 30 秒发一次意味着允许连续丢 2 次心跳仍不误杀。
	clientHeartbeatEvery = 30 * time.Second
)

type pendingMsg struct {
	env     protocol.Envelope
	attempt int
	last    time.Time
}

type retryItem struct {
	env     protocol.Envelope
	attempt int
}

type Client struct {
	ServerIP   string
	ServerPort int
	Name       string
	conn       net.Conn
	choice     int

	// mu 保护 pending 与 seen：读协程和重试协程都会访问
	mu      sync.Mutex
	pending map[string]*pendingMsg
	seen    map[string]bool
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
		pending:    make(map[string]*pendingMsg),
		seen:       make(map[string]bool),
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

// sendChat 发送聊天消息，并登记为"等 ACK"。
// msg_id 只在这里生成一次；重发必须复用，否则服务端会当成一条新消息。
func (client *Client) sendChat(env protocol.Envelope) error {
	if env.MsgID == "" {
		env.MsgID = newMsgID()
	}

	client.mu.Lock()
	client.pending[env.MsgID] = &pendingMsg{env: env, last: time.Now()}
	client.mu.Unlock()

	return client.send(env)
}

// AckLoop 扫描迟迟没收到 ACK 的消息并重发。
func (client *Client) AckLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		var retries []retryItem
		client.mu.Lock()
		for id, p := range client.pending {
			if time.Since(p.last) < ackTimeout {
				continue
			}
			if p.attempt >= maxSendRetry {
				delete(client.pending, id)
				fmt.Printf("[超时] 消息 %s 未收到 ACK，已重试 %d 次，放弃\n", id, p.attempt)
				continue
			}
			p.attempt++
			p.last = time.Now()
			retries = append(retries, retryItem{env: p.env, attempt: p.attempt})
		}
		client.mu.Unlock()

		for _, r := range retries {
			fmt.Printf("[重试] msg_id=%s 第 %d 次重发\n", r.env.MsgID, r.attempt)
			if err := client.send(r.env); err != nil {
				fmt.Println("重发失败:", err)
			}
		}
	}
}

// markAcked 收到服务端 ACK 后，把消息从待确认表里删掉。
func (client *Client) markAcked(msgID string) {
	if msgID == "" {
		return
	}
	client.mu.Lock()
	delete(client.pending, msgID)
	client.mu.Unlock()
}

// markSeen 做接收端去重：同一条消息被重复投递时只展示一次。
func (client *Client) markSeen(msgID string) bool {
	if msgID == "" {
		return true
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.seen[msgID] {
		return false
	}
	client.seen[msgID] = true
	return true
}

// newMsgID 生成消息唯一标识，幂等去重和 ACK 都靠它。
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
					To:       remoteName,
					ChatType: string(model.ChatSingle),
					Body:     chatMsg,
				}
				if err := client.sendChat(env); err != nil {
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
				ChatType: string(model.ChatGroup),
				Body:     chatMsg,
			}
			if err := client.sendChat(env); err != nil {
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

// clientHeartbeatEvery 是客户端心跳间隔。服务端超时默认 90 秒，
// 30 秒发一次意味着允许连续丢 2 次心跳仍不误杀。
func (client *Client) HeartbeatLoop() {
	ticker := time.NewTicker(clientHeartbeatEvery)
	defer ticker.Stop()
	for range ticker.C {
		if err := client.send(protocol.Envelope{Type: protocol.TypeHeartbeat}); err != nil {
			return
		}
	}
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
		case protocol.TypeHeartbeat:
			// 服务端目前不回心跳；留这个分支是为了协议对称
		case protocol.TypeAck:
			client.markAcked(env.MsgID)
		case protocol.TypeChat:
			// 重复投递只显示一次（服务端重试可能让同一条到达两次）
			if !client.markSeen(env.MsgID) {
				continue
			}
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
	go client.HeartbeatLoop()
	go client.AckLoop()
	go client.ReadServer()
	client.Run()
}
