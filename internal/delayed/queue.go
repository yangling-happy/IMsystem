package delayed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"imsystem/internal/model"
)

// Key 是离线消息延迟队列的 ZSET 键名。
const Key = "im:delayed:messages"

// Task 是一条待补投的离线消息。
// Attempt 记录已经重试过几次，用来做退避和放弃判断。
type Task struct {
	Message model.Message `json:"message"`
	Attempt int           `json:"attempt"`
}

// Queue 用 ZSET 实现延迟队列：member 是任务，score 是"可以投递的时间戳"。
type Queue struct {
	rdb *redis.Client
	key string
}

func New(rdb *redis.Client, key string) *Queue {
	return &Queue{rdb: rdb, key: key}
}

// Push 把任务排到 readyAt 之后才可被取出。
// 用毫秒时间戳做 score：ZSET 按 score 有序，取到期任务就是一次范围查询。
func (q *Queue) Push(ctx context.Context, t Task, readyAt time.Time) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	if err := q.rdb.ZAdd(ctx, q.key, redis.Z{
		Score:  float64(readyAt.UnixMilli()),
		Member: payload,
	}).Err(); err != nil {
		return fmt.Errorf("zadd: %w", err)
	}
	return nil
}

// popDueScript 原子地"查到期 + 删除"。
// 为什么必须原子：如果有两个投递协程，非原子的话同一条任务会被取到两次，
// 接收方就会收到重复消息。Lua 在 Redis 里单线程执行，天然互斥。
var popDueScript = redis.NewScript(`
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #items == 0 then
  return {}
end
redis.call('ZREM', KEYS[1], unpack(items))
return items
`)

// PopDue 取出 score <= now 的最多 limit 条任务，并同时从 ZSET 移除。
func (q *Queue) PopDue(ctx context.Context, now time.Time, limit int) ([]Task, error) {
	raw, err := popDueScript.Run(ctx, q.rdb, []string{q.key}, now.UnixMilli(), limit).StringSlice()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("pop due: %w", err)
	}

	tasks := make([]Task, 0, len(raw))
	for _, item := range raw {
		var t Task
		if err := json.Unmarshal([]byte(item), &t); err != nil {
			// 坏数据已经出队，无法重试，丢弃并跳过
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}
