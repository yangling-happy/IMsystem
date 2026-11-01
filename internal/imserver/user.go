package imserver

import (
	"net"

	"imsystem/internal/protocol"
)

// User 只负责一条连接的读写，不关心 Server 的存在
type User struct {
	Name string
	Addr string
	conn net.Conn
	send chan protocol.Envelope
}

func NewUser(conn net.Conn) *User {
	addr := conn.RemoteAddr().String()
	return &User{
		Name: addr,
		Addr: addr,
		conn: conn,
		send: make(chan protocol.Envelope, 16),
	}
}

// Send 把报文塞进发送队列
func (u *User) Send(env protocol.Envelope) {
	select {
	case u.send <- env:
	default:
		// 队列满则丢弃，避免阻塞广播（P3 用缓冲队列 + 指标解决）
	}
}

func (u *User) Close() {
	u.conn.Close()
}

// WriteLoop 把 send 队列里的报文编码后持续写给客户端，直到 send 被关闭
func (u *User) WriteLoop() {
	for env := range u.send {
		b, err := protocol.Encode(env)
		if err != nil {
			continue
		}
		if _, err := u.conn.Write(b); err != nil {
			return
		}
	}
}
