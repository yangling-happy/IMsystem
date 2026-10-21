package imserver

import "net"

// User 只负责一条连接的读写，不关心 Server 的存在
type User struct {
	Name string
	Addr string
	conn net.Conn
	send chan string
}

func NewUser(conn net.Conn) *User {
	addr := conn.RemoteAddr().String()
	return &User{
		Name: addr,
		Addr: addr,
		conn: conn,
		send: make(chan string, 16),
	}
}

// Send 把消息塞进发送队列
func (u *User) Send(msg string) {
	select {
	case u.send <- msg:
	default:
		// 队列满则丢弃，避免阻塞广播
	}
}

func (u *User) Close() {
	u.conn.Close()
}

// WriteLoop 把 send 队列里的消息持续写给客户端，直到 send 被关闭
func (u *User) WriteLoop() {
	for msg := range u.send {
		u.conn.Write([]byte(msg))
	}
}
