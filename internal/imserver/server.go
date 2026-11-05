package imserver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"imsystem/internal/delayed"
	"imsystem/internal/metrics"
	"imsystem/internal/model"
	"imsystem/internal/protocol"
	"imsystem/internal/queue"
)

const (
	// maxLineBytes 是单条报文的上限（含 JSON 本身）。超过时 Scanner 返回
	// bufio.ErrTooLong，连接按异常断开处理。
	maxLineBytes = 64 * 1024

	// heartbeatCheckInterval 是巡检间隔：每 10 秒扫一遍在线用户。
	// 掉线检测延迟上限 = HeartbeatTimeout + heartbeatCheckInterval。
	heartbeatCheckInterval = 10 * time.Second

	// closeDelay 让写协程有机会把"被断开"的提示刷出去，再真正关闭连接。
	closeDelay = 100 * time.Millisecond

	// dispatchInterval 是离线消息补投的轮询间隔。
	dispatchInterval = 500 * time.Millisecond
	// dispatchBatch 是单轮最多取多少条，避免一轮做太久拖慢下一轮。
	dispatchBatch = 20
	// retryBaseDelay 是首次退避等待，之后按 2 倍递增。
	retryBaseDelay = time.Second
	// maxBackoffShift 限制退避的翻倍次数。
	maxBackoffShift = 3
	// retryMaxDelay 是退避上限。
	// 不宜设大：轮询本来就每 500ms 扫一次 ZSET，退避不省 Redis 开销，
	// 却会把"用户刚上线"的补投延迟拖到这个值。5 秒是延迟与压力的折中。
	retryMaxDelay = 5 * time.Second
	// delayedOpTimeout 是单次 Redis 操作的上限。
	delayedOpTimeout = 200 * time.Millisecond
	// limiterOpTimeout 更短：限流在请求路径上，不能把用户发送拖慢。
	limiterOpTimeout = 100 * time.Millisecond
	// enqueueTimeout 是入队上限。不能太小：突发流量时 Redis 连接池要扩容，
	// 冷连接一次握手就可能超过 50ms，超时即丢消息（实测踩到过）。
	enqueueTimeout = 300 * time.Millisecond
	// maxBodyBytes 是聊天正文上限。整行上限是 maxLineBytes（64KB），
	// 但聊天正文没必要那么大：给 msg_id/from/to 等字段留足空间后取 4KB。
	maxBodyBytes = 4 * 1024
)

// DelayedQueue 是离线消息延迟队列的抽象，实现是 delayed.Queue。
type DelayedQueue interface {
	Push(ctx context.Context, t delayed.Task, readyAt time.Time) error
	PopDue(ctx context.Context, now time.Time, limit int) ([]delayed.Task, error)
}

// Server 是用户管理与消息分发的中枢
type Server struct {
	Ip   string
	Port int

	// HeartbeatTimeout 超过这个时长没收到任何报文，即判定掉线并断开。
	HeartbeatTimeout time.Duration

	// Sink 是消息入队口，nil 表示不持久化（P0 的无存储模式）。
	// 由 cmd/server 注入，imserver 不关心背后是内存队列还是 Redis。
	Sink queue.Sink

	// Delayed 是离线消息延迟队列，nil 表示不支持离线补投。
	Delayed DelayedQueue

	// Limiter 是发送限流器，nil 表示不限流。
	Limiter RateLimiter

	// Dedupe 是消息幂等检查器，nil 表示不做去重。
	Dedupe MessageDedupe

	users   map[string]*User
	usersMu sync.RWMutex

	// ln 是监听句柄，Close() 通过关闭它让 Start() 的 Accept 返回 net.ErrClosed。
	ln net.Listener
}

// RateLimiter 是限流抽象，实现是 limiter.Limiter。
type RateLimiter interface {
	Allow(ctx context.Context, key string) (bool, error)
}

// MessageDedupe 是消息幂等抽象，实现是 dedupe.Dedupe。
type MessageDedupe interface {
	Seen(ctx context.Context, msgID string) (bool, error)
}

func NewServer(ip string, port int) *Server {
	return &Server{
		Ip:               ip,
		Port:             port,
		HeartbeatTimeout: 90 * time.Second,
		users:            make(map[string]*User),
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", net.JoinHostPort(s.Ip, fmt.Sprintf("%d", s.Port)))
	if err != nil {
		return fmt.Errorf("启动服务器失败: %w", err)
	}
	s.ln = listener
	fmt.Printf("服务器监听 %s:%d\n", s.Ip, s.Port)

	// 心跳巡检：独立协程，不阻塞网络读写
	go s.kickIdle()

	// 离线消息补投：独立协程，每隔 dispatchInterval 看一次有没有到期任务
	if s.Delayed != nil {
		go s.dispatchLoop()
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Close() 主动关闭监听时 Accept 会返回 net.ErrClosed，这是正常退出路径
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("接受连接失败: %w", err)
		}
		go s.handleConn(conn)
	}
}

// Close 停止接收新连接，并关掉所有在线连接，让各协程的读循环退出。
func (s *Server) Close() error {
	if s.ln == nil {
		return nil
	}
	err := s.ln.Close()

	s.usersMu.Lock()
	users := make([]*User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	s.usersMu.Unlock()
	for _, u := range users {
		u.Close()
	}
	return err
}

// handleConn 处理一条连接的完整生命周期。
// 读取用 bufio.Scanner：它保证每次拿到的是"完整的一行"，
// 彻底消除原来 conn.Read 把半条/两条消息当一条的问题。
func (s *Server) handleConn(conn net.Conn) {
	user := NewUser(conn)
	s.addUser(user)
	metrics.OnlineUsers.Inc()
	defer func() {
		metrics.OnlineUsers.Dec()
		s.removeUser(user)
	}()

	go user.WriteLoop()
	defer user.CloseSend()

	s.broadcastNotice(user.Name + " 已上线")

	scanner := bufio.NewScanner(conn)
	// 默认上限只有 bufio.MaxScanTokenSize（64KB），这里显式扩容并复用初始缓冲。
	scanner.Buffer(make([]byte, 0, 1024), maxLineBytes)

	for scanner.Scan() {
		// 任何报文都算"我还活着"：聊天、查询、心跳都刷新活跃时间
		user.Touch()

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

	// 连接已由对方关闭、被心跳巡检踢掉，或读出错，统一走清理
	_ = conn.Close()
}

// kickIdle 周期性检查在线用户，超时未活跃的断开。
// 这是"网线被拔/进程被杀"这类没有 FIN 的病态断连的唯一检测手段。
func (s *Server) kickIdle() {
	ticker := time.NewTicker(heartbeatCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()

		s.usersMu.RLock()
		var idle []*User
		for _, u := range s.users {
			if now.Sub(u.LastActive()) > s.HeartbeatTimeout {
				idle = append(idle, u)
			}
		}
		s.usersMu.RUnlock()

		for _, u := range idle {
			u.Send(protocol.Envelope{
				Type: protocol.TypeNotice,
				Body: "心跳超时，连接已断开",
			})
			time.AfterFunc(closeDelay, u.Close)
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
	name := u.Name
	delete(s.users, name)
	s.usersMu.Unlock()
	s.broadcastNotice(name + " 已下线")
}

// ---- 消息分发 ----

// handleEnvelope 按报文类型分发。这里是"协议解释"的唯一入口。
func (s *Server) handleEnvelope(user *User, env protocol.Envelope) {
	switch env.Type {
	case protocol.TypeHeartbeat:
		// 心跳只需要刷新活跃时间（已在读循环里做过），不做业务分发
	case protocol.TypeAck:
		// ACK 的方向是服务端 -> 客户端；客户端发来的 ACK 不需要处理
	case protocol.TypeWho:
		s.listUsers(user)
	case protocol.TypeRename:
		s.rename(user, env.Body)
	case protocol.TypeChat:
		// 正文超长直接拒绝并回提示，但不断连：断连会让用户丢掉
		// 正在输入的上下文，而且这条消息本身也没有值得罚断线的恶意。
		if len(env.Body) > maxBodyBytes {
			user.Send(protocol.Envelope{
				Type:  protocol.TypeNotice,
				MsgID: env.MsgID,
				Body:  fmt.Sprintf("消息内容过长（上限 %d 字节）", maxBodyBytes),
			})
			return
		}
		// 限流只作用于聊天：心跳、改名、查询不能被限流拖死
		if !s.allowSend(user) {
			metrics.RateLimitedTotal.Inc()
			user.Send(protocol.Envelope{
				Type: protocol.TypeNotice,
				Body: "发送太快了，请稍后再试",
			})
			return
		}

		// 幂等前置拦截：客户端重试会带着同一个 msg_id 再来一次，
		// 这里挡掉就不会重复投递、重复入库。
		if s.isDuplicate(user, env) {
			// 重复消息也必须回 ACK，否则客户端收不到确认会一直重发
			s.sendAck(user, env)
			return
		}

		if env.To == "" {
			s.broadcastChat(user, env)
		} else {
			s.sendTo(user, env)
		}
		// 先投递再入队：投递是同步的（用户等着看消息），落库是异步的（不阻塞读循环）
		if err := s.enqueue(user, env); err != nil {
			user.Send(protocol.Envelope{
				Type: protocol.TypeNotice,
				Body: "服务端繁忙，消息未入队，请稍后重试",
			})
			return
		}

		metrics.MessagesTotal.WithLabelValues("ok").Inc()
		// 受理成功才回 ACK：客户端收到才算发成功，没收到就重发同一个 msg_id
		s.sendAck(user, env)
	default:
		user.Send(protocol.Envelope{
			Type: protocol.TypeNotice,
			Body: "未知报文类型: " + env.Type,
		})
	}
}

// enqueue 把聊天消息转成 model.Message 并塞进缓冲队列。
// 这里绝不能阻塞：读循环一旦被存储拖慢，整个连接的消息处理都会卡住。
func (s *Server) enqueue(user *User, env protocol.Envelope) error {
	if s.Sink == nil {
		return nil
	}
	chatType := model.ChatSingle
	if env.To == "" {
		chatType = model.ChatGroup
	}

	s.usersMu.RLock()
	from := user.Name
	s.usersMu.RUnlock()

	m := model.Message{
		MsgID:    env.MsgID,
		From:     from,
		To:       env.To,
		ChatType: chatType,
		Body:     env.Body,
		SentAt:   time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), enqueueTimeout)
	defer cancel()
	if err := s.Sink.Push(ctx, m); err != nil {
		log.Printf("消息入队失败 msg_id=%s err=%v", m.MsgID, err)
		return err
	}
	return nil
}

// isDuplicate 查消息是否重复。Redis 故障时放行（fail-open）：
// 还有 MySQL 唯一索引兜底，宁可放过去也不能把正常消息全挡死。
func (s *Server) isDuplicate(user *User, env protocol.Envelope) bool {
	if s.Dedupe == nil || env.MsgID == "" {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), limiterOpTimeout)
	defer cancel()
	dup, err := s.Dedupe.Seen(ctx, env.MsgID)
	if err != nil {
		log.Printf("幂等检查失败，按未重复处理 msg_id=%s err=%v", env.MsgID, err)
		return false
	}
	if !dup {
		return false
	}

	metrics.DuplicateTotal.Inc()
	user.Send(protocol.Envelope{
		Type:  protocol.TypeNotice,
		MsgID: env.MsgID,
		Body:  "重复消息已忽略",
	})
	return true
}

// sendAck 告诉发送方"服务端已受理"。
// 没有 msg_id 的消息无法被重试追踪，也就不需要 ACK。
func (s *Server) sendAck(user *User, env protocol.Envelope) {
	if env.MsgID == "" {
		return
	}
	user.Send(protocol.Envelope{Type: protocol.TypeAck, MsgID: env.MsgID})
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
	s.usersMu.RLock()
	// 在锁内读 sender.Name：改名会取写锁，这里拿读锁避免读到改名前后的撕裂值
	from := sender.Name
	out := protocol.Envelope{
		Type:     protocol.TypeChat,
		MsgID:    env.MsgID,
		From:     from,
		ChatType: string(model.ChatGroup),
		Body:     env.Body,
	}
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
	from := sender.Name
	s.usersMu.RUnlock()
	if !ok {
		// 对方不在线：不丢消息，转成离线任务等对方上线再补投
		s.delayOffline(sender, env, from)
		return
	}
	target.Send(protocol.Envelope{
		Type:     protocol.TypeChat,
		MsgID:    env.MsgID,
		From:     from,
		To:       env.To,
		ChatType: string(model.ChatSingle),
		Body:     env.Body,
	})
}

// allowSend 查限流。Redis 故障时放行（fail-open）：
// 限流是用来防刷的，不属于消息正确性，不能因为它把正常用户挡在门外。
func (s *Server) allowSend(user *User) bool {
	if s.Limiter == nil {
		return true
	}

	s.usersMu.RLock()
	key := user.Name
	s.usersMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), limiterOpTimeout)
	defer cancel()
	ok, err := s.Limiter.Allow(ctx, key)
	if err != nil {
		log.Printf("限流器异常，按放行处理: %v", err)
		return true
	}
	return ok
}

// ---- 离线消息 ----

// delayOffline 对方不在线时，把消息写进延迟队列等对方上线补投。
// 这条消息同时也已经由 enqueue 落了库，所以即使服务重启也不会丢。
func (s *Server) delayOffline(sender *User, env protocol.Envelope, from string) {
	if s.Delayed == nil {
		sender.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "当前用户不存在"})
		return
	}

	m := model.Message{
		MsgID:    env.MsgID,
		From:     from,
		To:       env.To,
		ChatType: model.ChatSingle,
		Body:     env.Body,
		SentAt:   time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), delayedOpTimeout)
	defer cancel()
	if err := s.Delayed.Push(ctx, delayed.Task{Message: m}, time.Now()); err != nil {
		log.Printf("离线消息入队失败 msg_id=%s err=%v", m.MsgID, err)
		sender.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "离线消息入队失败，请重发"})
		return
	}
	sender.Send(protocol.Envelope{Type: protocol.TypeNotice, Body: "对方不在线，消息已存入离线队列"})
}

// dispatchLoop 周期性取出到期任务并补投。
func (s *Server) dispatchLoop() {
	ticker := time.NewTicker(dispatchInterval)
	defer ticker.Stop()

	ctx := context.Background()
	for range ticker.C {
		tasks, err := s.Delayed.PopDue(ctx, time.Now(), dispatchBatch)
		if err != nil {
			log.Printf("取离线消息失败: %v", err)
			continue
		}
		for _, t := range tasks {
			// 投递成功就不管了；对方仍不在线则重新排队等下次
			if s.deliverOffline(t.Message) {
				continue
			}
			s.retryLater(ctx, t)
		}
	}
}

// deliverOffline 尝试把一条离线消息发给在线的接收人，返回是否成功。
func (s *Server) deliverOffline(m model.Message) bool {
	s.usersMu.RLock()
	target, ok := s.users[m.To]
	s.usersMu.RUnlock()
	if !ok {
		return false
	}
	target.Send(protocol.Envelope{
		Type:     protocol.TypeChat,
		MsgID:    m.MsgID,
		From:     m.From,
		To:       m.To,
		ChatType: string(m.ChatType),
		Body:     m.Body,
	})
	return true
}

// retryLater 把任务以更晚的 readyAt 重新入队。
// 不设"放弃次数"：离线消息只有对方上线才能送达，提前放弃等于永久丢消息。
// 只用退避上限控制轮询压力，长期不上线的用户最多每 30 秒试一次。
func (s *Server) retryLater(ctx context.Context, t delayed.Task) {
	next := t.Attempt + 1
	shift := next - 1
	if shift > maxBackoffShift {
		shift = maxBackoffShift
	}
	delay := retryBaseDelay << shift
	if delay > retryMaxDelay {
		delay = retryMaxDelay
	}

	opCtx, cancel := context.WithTimeout(ctx, delayedOpTimeout)
	defer cancel()
	if err := s.Delayed.Push(opCtx, delayed.Task{Message: t.Message, Attempt: next}, time.Now().Add(delay)); err != nil {
		log.Printf("离线消息重排失败 msg_id=%s err=%v", t.Message.MsgID, err)
	}
}
