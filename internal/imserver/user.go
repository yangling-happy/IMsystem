package imserver

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"imsystem/internal/protocol"
)

// User 只负责一条连接的读写，不关心 Server 的存在
type User struct {
	Name string
	Addr string
	conn net.Conn
	send chan protocol.Envelope

	// lastActive 记录最后一次收到报文的时间（Unix 纳秒），
	// 心跳巡检协程与读协程并发读写，必须用原子操作。
	lastActive atomic.Int64

	// mu 保护 send 的关闭状态：Send 可能由任意协程调用（广播、巡检踢人），
	// 而 CloseSend 由读循环退出时调用，两者必须互斥，否则会 panic。
	mu     sync.RWMutex
	closed bool
}

func NewUser(conn net.Conn) *User {
	addr := conn.RemoteAddr().String()
	u := &User{
		Name: addr,
		Addr: addr,
		conn: conn,
		send: make(chan protocol.Envelope, 16),
	}
	u.Touch()
	return u
}

// Send 把报文塞进发送队列。若队列已关闭则静默丢弃（用户已离线）。
func (u *User) Send(env protocol.Envelope) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.closed {
		return
	}
	select {
	case u.send <- env:
	default:
		// 队列满则丢弃，避免阻塞广播（P3 用缓冲队列 + 指标解决）
	}
}

// CloseSend 关闭发送队列，由读循环退出时调用。重复调用是安全的。
func (u *User) CloseSend() {
	u.mu.Lock()
	if !u.closed {
		u.closed = true
		close(u.send)
	}
	u.mu.Unlock()
}

// Touch 刷新活跃时间，每收到一条报文调用一次
func (u *User) Touch() {
	u.lastActive.Store(time.Now().UnixNano())
}

// LastActive 返回最后一次活跃时间
func (u *User) LastActive() time.Time {
	return time.Unix(0, u.lastActive.Load())
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
