package queue

import (
	"context"
	"errors"
	"log"
	"time"

	"imsystem/internal/metrics"
	"imsystem/internal/model"
	"imsystem/internal/storage"
)

const (
	// defaultMaxRetry 是首次失败后额外重试的次数（总尝试次数 = 1 + maxRetry）。
	defaultMaxRetry = 3
	// defaultBackoff 是第一次重试前的等待，之后按 2 倍递增。
	defaultBackoff = 100 * time.Millisecond
	// defaultPopWait 是单次取消息的最长阻塞时间。
	defaultPopWait = time.Second
)

// Source 是消息来源抽象。真实实现是 Redis 队列，测试实现是内存队列。
type Source interface {
	// Pop 阻塞最多 wait 取一条消息；没有消息时返回 ok=false 且 err=nil。
	Pop(ctx context.Context, wait time.Duration) (m model.Message, ok bool, err error)
}

// Sink 是入队抽象，供服务端在收到聊天消息时调用。
type Sink interface {
	Push(ctx context.Context, m model.Message) error
}

// Worker 是消息异步持久化的消费方：取消息、落库、失败重试。
// 它只依赖 Source 与 storage.Repo 接口，所以可以完全脱离 Redis/MySQL 做单测。
type Worker struct {
	repo     storage.Repo
	maxRetry int
	backoff  time.Duration
	popWait  time.Duration
}

func NewWorker(repo storage.Repo) *Worker {
	return &Worker{
		repo:     repo,
		maxRetry: defaultMaxRetry,
		backoff:  defaultBackoff,
		popWait:  defaultPopWait,
	}
}

// Run 持续消费消息，直到 ctx 取消。
// 由调用方负责启动协程，Worker 自己不创建协程。
func (w *Worker) Run(ctx context.Context, src Source) {
	for {
		if ctx.Err() != nil {
			return
		}
		m, ok, err := src.Pop(ctx, w.popWait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// 取消息出错（例如 Redis 连接断开）：退避后重试，不退出进程
			log.Printf("取消息失败: %v", err)
			if !sleep(ctx, w.backoff) {
				return
			}
			continue
		}
		if !ok {
			// 等待期内没有消息，属于正常情况
			continue
		}
		w.persist(ctx, m)
	}
}

// persist 落库一条消息，带指数退避重试。
func (w *Worker) persist(ctx context.Context, m model.Message) {
	start := time.Now()
	for attempt := 0; attempt <= w.maxRetry; attempt++ {
		err := w.repo.SaveMessage(ctx, m)
		if err == nil {
			metrics.PersistDuration.Observe(time.Since(start).Seconds())
			metrics.PersistedTotal.Inc()
			return
		}
		// 唯一索引冲突说明这条消息已经落过库，属于幂等命中，不算失败
		if errors.Is(err, storage.ErrDuplicate) {
			return
		}
		if attempt == w.maxRetry {
			// 重试耗尽：丢弃并留下证据，后续接告警/死信队列
			log.Printf("消息落库失败，已丢弃 msg_id=%s err=%v", m.MsgID, err)
			return
		}
		if !sleep(ctx, w.backoff*(1<<attempt)) {
			return
		}
	}
}

// sleep 等待 d，若 ctx 提前取消则返回 false。
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
