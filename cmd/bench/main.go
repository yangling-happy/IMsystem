// Package main 是并发压测客户端：
// 建立 N 条长连接，每条按固定速率发 M 条公聊消息，
// 统计吞吐与 send->ACK 延迟分位（p50/p95/p99），用于验证恢复能力与评估瓶颈。
package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"imsystem/internal/model"
	"imsystem/internal/protocol"
)

// maxLineBytes 与服务端一致，压测客户端也要能读完 64KB 的整行。
const maxLineBytes = 64 * 1024

type stats struct {
	mu        sync.Mutex
	latencies []time.Duration
	acked     int
	limited   int
}

func (s *stats) addLatency(d time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
}

func (s *stats) addAcked() {
	s.mu.Lock()
	s.acked++
	s.mu.Unlock()
}

func (s *stats) addLimited() {
	s.mu.Lock()
	s.limited++
	s.mu.Unlock()
}

func (s *stats) snapshot() ([]time.Duration, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lat := make([]time.Duration, len(s.latencies))
	copy(lat, s.latencies)
	return lat, s.acked, s.limited
}

type benchConn struct {
	conn    net.Conn
	pending map[string]time.Time
	mu      sync.Mutex
	all     *stats
}

func (b *benchConn) readLoop(done chan<- struct{}) {
	defer close(done)
	scanner := bufio.NewScanner(b.conn)
	scanner.Buffer(make([]byte, 0, 1024), maxLineBytes)

	for scanner.Scan() {
		env, err := protocol.Decode(scanner.Bytes())
		if err != nil {
			continue
		}
		switch env.Type {
		case protocol.TypeAck:
			b.mu.Lock()
			start, ok := b.pending[env.MsgID]
			if ok {
				delete(b.pending, env.MsgID)
			}
			b.mu.Unlock()
			if ok {
				b.all.addAcked()
				b.all.addLatency(time.Since(start))
			}
		case protocol.TypeNotice:
			// 只统计限流提示：上下线通知也走 TypeNotice，不能混在一起
			if strings.Contains(env.Body, "发送太快") {
				b.all.addLimited()
			}
		}
	}
}

// pendingCount 返回还没收到 ACK 的消息数
func (b *benchConn) pendingCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

func (b *benchConn) send(env protocol.Envelope) error {
	buf, err := protocol.Encode(env)
	if err != nil {
		return err
	}
	_, err = b.conn.Write(buf)
	return err
}

func runConn(id int, addr string, msgs, rate int, wait time.Duration, all *stats) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("[连接 %d] 建立失败: %v\n", id, err)
		return
	}
	defer conn.Close()

	b := &benchConn{conn: conn, pending: make(map[string]time.Time), all: all}
	done := make(chan struct{})
	go b.readLoop(done)

	// rate 是每条连接每秒的发送条数。服务端限流默认 20/s，
	// 压测要观察吞吐而不是看限流拦截，所以默认压在 10/s。
	var ticker *time.Ticker
	if rate > 0 {
		interval := time.Second / time.Duration(rate)
		if interval <= 0 {
			interval = time.Millisecond
		}
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	for i := 0; i < msgs; i++ {
		if ticker != nil {
			<-ticker.C
		}
		env := protocol.Envelope{
			Type:     protocol.TypeChat,
			MsgID:    newMsgID(),
			ChatType: string(model.ChatGroup),
			Body:     fmt.Sprintf("bench-%d-%d", id, i),
		}
		b.mu.Lock()
		b.pending[env.MsgID] = time.Now()
		b.mu.Unlock()
		if err := b.send(env); err != nil {
			fmt.Printf("[连接 %d] 发送失败: %v\n", id, err)
			return
		}
	}

	// 等剩余 ACK 到齐，最多等到 wait
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if b.pendingCount() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// newMsgID 与服务端约定一致：16 位 hex，重试时复用同一个值
func newMsgID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8888", "服务端地址")
	conns := flag.Int("conns", 20, "并发连接数")
	msgs := flag.Int("msgs", 20, "每条连接发送的消息数")
	rate := flag.Int("rate", 10, "每条连接每秒发送条数（0 表示不限速）")
	wait := flag.Duration("ack-wait", 5*time.Second, "发完后等待剩余 ACK 的最长时间")
	flag.Parse()

	start := time.Now()
	all := &stats{}
	var wg sync.WaitGroup
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runConn(id, *addr, *msgs, *rate, *wait, all)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	lat, acked, limited := all.snapshot()
	total := *conns * *msgs
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	fmt.Printf("连接数=%d 每连接消息数=%d 总发送=%d\n", *conns, *msgs, total)
	fmt.Printf("ACK=%d 未确认=%d 限流拦截=%d\n", acked, total-acked, limited)
	fmt.Printf("耗时=%.2fs 吞吐=%.0f msg/s\n", elapsed.Seconds(), float64(total)/elapsed.Seconds())
	if len(lat) > 0 {
		fmt.Printf("延迟 p50=%s p95=%s p99=%s max=%s\n",
			percentile(lat, 0.50), percentile(lat, 0.95), percentile(lat, 0.99), lat[len(lat)-1])
	}
}
