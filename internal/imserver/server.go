package imserver

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"imsystem/internal/model"
	"imsystem/internal/protocol"
)

const (
	// maxLineBytes 是单条报文的上限（含 JSON 本身）。超过时 Scanner 返回
	// bufio.ErrTooLong，连接按异常断开处理。
	maxLineBytes = 64 * 1024

	// idleTimeout 是"多久没有收到任何报文就断开"。P2 会用心跳机制替换它。
	idleTimeout = 10 * time.Second
)

// Server 是用户管理与消息分发的中枢
type Server struct {
	Ip   string
	Port int

	users   map[string]*User
	usersMu sync.RWMutex
}

func NewServer(ip string, port int) *Server {
	return &Server{
		Ip:    ip,
		Port:  port,
		users: make(map[string]*User),
	}
}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", net.JoinHostPort(s.Ip, fmt.Sprintf("%d", s.Port)))
	if err != nil {
		fmt.Println("启动服务器失败:", err)
		return
	}
	defer listener.Close()
	fmt.Printf("服务器监听 %s:%d\n", s.Ip, s.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("接受连接失败:", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// handleConn 处理一条连接的完整生命周期。
// 读取用 bufio.Scanner：它保证每次拿到的是"完整的一行"，
// 彻底消除原来 conn.Read 把半条/两条消息当一条的问题。
func (s *Server) handleConn(conn net.Conn) {
	user := NewUser(conn)
	s.addUser(user)
	defer s.removeUser(user)

	go user.WriteLoop()
	defer close(user.send)

	s.broadcastNotice(user.Name + " 已上线")

	scanner := bufio.NewScanner(conn)
	// 默认上限只有 bufio.MaxScanTokenSize（64KB），这里显式扩容并复用初始缓冲。
	scanner.Buffer(make([]byte, 0, 1024), maxLineBytes)

	for {
		// 每次等待下一行前重置读超时：空闲过久就断开（P2 改为心跳驱动）
		_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))
		if !scanner.Scan() {
			if errors.Is(scanner.Err(), os.ErrDeadlineExceeded) {
				user.Send(protocol.Envelope{
					Type: protocol.TypeNotice,
					Body: "你已因长时间无操作被断开",
				})
			}
			_ = conn.Close()
			return
		}

		env, err := protocol.Decode(scanner.Bytes())
		if err != nil {
			// 空行不是错误，跳过即可；坏报文只回提示，不因此踢人
			if !errors.Is(err, protocol.ErrEmptyLine) {
				user.Send(protocol.Envelope{
					Type: protocol.TypeNotice,
					Body: "报文格式错误: " + err.Error(),
				})
			}
			continue
		}

		s.handleEnvelope(user, env)
	}
}

// ---- 用户表操作 ----

func (s *Server) addUser(u *User) {
	s.usersMu.Lock()
	s.users[u.Name] = u
	s.usersMu.Unlock()
}

func (s *Server) removeUser(u *User) {
	s.usersMu.Lock()
	delete(s.users, u.Name)
	s.usersMu.Unlock()
	s.broadcastNotice(u.Name + " 已下线")
}

// ---- 消息分发 ----

// handleEnvelope 按报文类型分发。这里是"协议解释"的唯一入口。
func (s *Server) handleEnvelope(user *User, env protocol.Envelope) {
	switch env.Type {
	case protocol.TypeWho:
		s.listUsers(user)
	case protocol.TypeRename:
		s.rename(user, env.Body)
	case protocol.TypeChat:
		if env.To == "" {
			s.broadcastChat(user, env)
			return
		}
		s.sendTo(user, env)
	default:
		user.Send(protocol.Envelope{
			Type: protocol.TypeNotice,
			Body: "未知报文类型: " + env.Type,
		})
	}
}

// broadcastNotice 向所有在线用户下发系统提示（上下线、错误等）
func (s *Server) broadcastNotice(msg string) {
	s.usersMu.RLock()
	for _, u := range s.users {
		u.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: msg})
	}
	s.usersMu.RUnlock()
}

// broadcastChat 公聊：把发送者的消息原样广播给所有人（含自己）
func (s *Server) broadcastChat(sender *User, env protocol.Envelope) {
	out := protocol.Envelope{
		Type:     protocol.TypeChat,
		MsgID:    env.MsgID,
		From:     sender.Name,
		ChatType: string(model.ChatGroup),
		Body:     env.Body,
	}
	s.usersMu.RLock()
	for _, u := range s.users {
		u.Send(out)
	}
	s.usersMu.RUnlock()
}

func (s *Server) listUsers(requester *User) {
	s.usersMu.RLock()
	for _, u := range s.users {
		requester.Send(protocol.Envelope{
			Type: protocol.TypeNotice,
			Body: fmt.Sprintf("%s 在线", u.Name),
		})
	}
	s.usersMu.RUnlock()
}

func (s *Server) rename(user *User, newName string) {
	if newName == "" {
		user.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "用户名不能为空"})
		return
	}
	s.usersMu.Lock()
	if newName == user.Name {
		s.usersMu.Unlock()
		user.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "新用户名与当前用户名相同"})
		return
	}
	if _, exists := s.users[newName]; exists {
		s.usersMu.Unlock()
		user.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "当前用户名被使用"})
		return
	}
	delete(s.users, user.Name)
	user.Name = newName
	s.users[newName] = user
	s.usersMu.Unlock()
	user.Send(protocol.Envelope{
		Type: protocol.TypeNotice,
		Body: fmt.Sprintf("改名成功，当前用户名: %s", newName),
	})
}

func (s *Server) sendTo(sender *User, env protocol.Envelope) {
	if env.To == "" {
		sender.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "缺少接收人"})
		return
	}
	if env.Body == "" {
		sender.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "无消息内容，请重发"})
		return
	}
	s.usersMu.RLock()
	target, ok := s.users[env.To]
	s.usersMu.RUnlock()
	if !ok {
		sender.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "当前用户不存在"})
		return
	}
	target.Send(protocol.Envelope{
		Type:     protocol.TypeChat,
		MsgID:    env.MsgID,
		From:     sender.Name,
		To:       env.To,
		ChatType: string(model.ChatSingle),
		Body:     env.Body,
	})
}
