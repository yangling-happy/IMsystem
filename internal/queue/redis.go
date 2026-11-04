package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"imsystem/internal/model"
)

// MessageQueueKey 是待持久化消息的 Redis List 键名。
// 用固定键名而不是每用户一个队列：先保证"消息一定进得来、出得去"，
// 分片是吞吐量到瓶颈之后才做的事。
const MessageQueueKey = "im:queue:messages"

// RedisQueue 是生产用的缓冲队列，同时实现 Sink（Push）与 Source（Pop）。
type RedisQueue struct {
	rdb *redis.Client
	key string
}

func NewRedisQueue(rdb *redis.Client, key string) *RedisQueue {
	return &RedisQueue{rdb: rdb, key: key}
}

// Push 用 RPUSH 追加到队尾。
// Redis 的写命令不阻塞：队列积压只体现为 List 长度，可监控，但不会卡住发送方。
func (q *RedisQueue) Push(ctx context.Context, m model.Message) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	if err := q.rdb.RPush(ctx, q.key, payload).Err(); err != nil {
		return fmt.Errorf("rpush: %w", err)
	}
	return nil
}

// Pop 用 BLPOP 从队头取，队尾写、队头读 => 先进先出。
// 没消息时 Redis 会挂住这条连接直到 wait 到期，所以 Worker 不会空转打满 CPU。
func (q *RedisQueue) Pop(ctx context.Context, wait time.Duration) (model.Message, bool, error) {
	res, err := q.rdb.BLPop(ctx, wait, q.key).Result()
	if err != nil {
		// redis.Nil 是"等到超时也没有消息"，属于正常情况，不是故障
		if errors.Is(err, redis.Nil) {
			return model.Message{}, false, nil
		}
		return model.Message{}, false, fmt.Errorf("blpop: %w", err)
	}

	// BLPOP 返回 [key, value]，value 才是消息体
	var m model.Message
	if err := json.Unmarshal([]byte(res[1]), &m); err != nil {
		// 坏数据已经出队，无法重试，只能丢弃并留下证据
		return model.Message{}, false, fmt.Errorf("unmarshal message: %w", err)
	}
	return m, true, nil
}
