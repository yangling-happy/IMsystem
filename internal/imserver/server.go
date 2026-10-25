package imserver

import (
	"fmt"
	"net"
	"sync"
	"time"
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

// handleConn 处理一条连接的完整生命周期
func (s *Server) handleConn(conn net.Conn) {
	user := NewUser(conn)
	s.addUser(user)
	defer s.removeUser(user)

	go user.WriteLoop()
	defer close(user.send)

	s.broadcast(user, "已上线")

	isLive := make(chan struct{})
	disconnected := make(chan struct{})

	// 读循环：从连接读消息交给 processMessage
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				close(disconnected)
				return
			}
			s.processMessage(user, string(buf[:n]))
			isLive <- struct{}{}
		}
	}()

	// 超时检测：10 秒无消息则断开
	for {
		select {
		case <-isLive:
		case <-disconnected:
			return
		case <-time.After(10 * time.Second):
			user.Send("你已因长时间无操作被断开\n")
			conn.Close()
			return
		}
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
	s.broadcast(u, "已下线")
}

// ---- 消息分发 ----

func (s *Server) broadcast(sender *User, msg string) {
	sendMsg := fmt.Sprintf("[%s]%s:%s\n", sender.Addr, sender.Name, msg)
	s.usersMu.RLock()
	for _, u := range s.users {
		u.Send(sendMsg)
	}
	s.usersMu.RUnlock()
}

// processMessage 把原始消息解析成命令后分发
func (s *Server) processMessage(user *User, raw string) {
	cmd := ParseCommand(raw)
	switch cmd.Type {
	case CmdWho:
		s.listUsers(user)
	case CmdRename:
		s.rename(user, cmd.Content)
	case CmdTo:
		s.sendTo(user, cmd.Target, cmd.Content)
	case CmdBroadcast:
		s.broadcast(user, cmd.Content)
	}
}

func (s *Server) listUsers(requester *User) {
	s.usersMu.RLock()
	for _, u := range s.users {
		requester.Send(fmt.Sprintf("[%s]%s:已上线\n", u.Addr, u.Name))
	}
	s.usersMu.RUnlock()
}

func (s *Server) rename(user *User, newName string) {
	if newName == "" {
		user.Send("用户名不能为空\n")
		return
	}
	s.usersMu.Lock()
	if newName == user.Name {
		s.usersMu.Unlock()
		user.Send("新用户名与当前用户名相同\n")
		return
	}
	if _, exists := s.users[newName]; exists {
		s.usersMu.Unlock()
		user.Send("当前用户名被使用\n")
		return
	}
	delete(s.users, user.Name)
	user.Name = newName
	s.users[newName] = user
	s.usersMu.Unlock()
	user.Send(fmt.Sprintf("改名成功，当前用户名: %s\n", newName))
}

func (s *Server) sendTo(sender *User, targetName, content string) {
	if targetName == "" {
		sender.Send("消息格式不正确，请使用\"to|用户名|消息\"格式\n")
		return
	}
	if content == "" {
		sender.Send("无消息内容，请重发\n")
		return
	}
	s.usersMu.RLock()
	target, ok := s.users[targetName]
	s.usersMu.RUnlock()
	if !ok {
		sender.Send("当前用户不存在\n")
		return
	}
	target.Send(fmt.Sprintf("%s对你说：%s\n", sender.Name, content))
}
