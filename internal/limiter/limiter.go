package limiter

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const KeyPrefix = "im:rate:"

// Limiter 用 ZSET 记录每个 key 最近 window 内的请求时间点。
type Limiter struct {
	rdb    *redis.Client
	limit  int64
	window time.Duration
}

func New(rdb *redis.Client, limitPerWindow int, window time.Duration) *Limiter {
	return &Limiter{rdb: rdb, limit: int64(limitPerWindow), window: window}
}

// seq 保证同一毫秒内的两次请求有不同 member。
// 如果 member 相同，ZADD 会覆盖而不是新增，计数就会漏。
var seq uint64

// allowScript 先清掉窗口外的旧记录，再看当前计数。
// 整个过程必须原子：并发请求都在做"查数-判断-写入"，
// 拆成多条命令的话，阈值以内的并发会集体通过。
var allowScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count >= limit then
  return 0
end
redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)
return 1
`)

// Allow 判断该 key 在当前时间窗口内是否还能再发一条。
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	now := time.Now()
	n := atomic.AddUint64(&seq, 1)
	member := strconv.FormatInt(now.UnixNano(), 10) + "-" + strconv.FormatUint(n, 10)

	res, err := allowScript.Run(ctx, l.rdb,
		[]string{KeyPrefix + key},
		now.UnixMilli(),
		l.window.Milliseconds(),
		l.limit,
		member,
	).Int()
	if err != nil {
		return false, fmt.Errorf("rate limit: %w", err)
	}
	return res == 1, nil
}
