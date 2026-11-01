package queue

import (
	"context"
	"errors"
	"time"

	"imsystem/internal/model"
)

// ErrQueueFull 表示内存队列已满。生产环境用 Redis 队列，不会出现这个错误。
var ErrQueueFull = errors.New("queue: full")

// MemoryQueue 是内存队列，用于单测和本地联调。
// 它同时实现 Sink 与 Source，方便把 Worker 完整跑起来而不依赖 Redis。
type MemoryQueue struct {
	ch chan model.Message
}

func NewMemoryQueue(size int) *MemoryQueue {
	return &MemoryQueue{ch: make(chan model.Message, size)}
}

// Push 非阻塞入队：队列满立刻返回 ErrQueueFull，绝不阻塞调用方（读循环/分发路径）。
func (q *MemoryQueue) Push(ctx context.Context, m model.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case q.ch <- m:
		return nil
	default:
		return ErrQueueFull
	}
}

// Pop 阻塞最多 wait 取一条消息。
func (q *MemoryQueue) Pop(ctx context.Context, wait time.Duration) (model.Message, bool, error) {
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return model.Message{}, false, ctx.Err()
	case <-timer.C:
		return model.Message{}, false, nil
	case m := <-q.ch:
		return m, true, nil
	}
}
