package dedupe

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	KeyPrefix = "im:dedupe:"
	// ttl 决定 msg_id 的记忆时长：太短会让迟到的重试穿透，太长会白占内存。
	ttl = 24 * time.Hour
)

// Dedupe 用 SETNX 做消息幂等的前置拦截：同一个 msg_id 只放行一次。
type Dedupe struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Dedupe {
	return &Dedupe{rdb: rdb}
}

// Seen 返回 true 表示这个 msg_id 之前已经出现过，应当忽略。
// 空 msg_id 不做去重（调用方没生成标识，无法判断）。
func (d *Dedupe) Seen(ctx context.Context, msgID string) (bool, error) {
	if msgID == "" {
		return false, nil
	}
	ok, err := d.rdb.SetNX(ctx, KeyPrefix+msgID, 1, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("setnx dedupe: %w", err)
	}
	// SetNX 返回 true 表示"我是第一个写入的"，即首次出现
	return !ok, nil
}
