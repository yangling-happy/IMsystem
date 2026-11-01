// Package config 集中管理运行参数，全部支持环境变量覆盖。
package config

import (
	"os"
	"strconv"
	"time"
)

// Config 是服务端的全部运行参数。
type Config struct {
	Addr       string
	MySQLDSN   string
	RedisAddr  string
	RedisPass  string

	HeartbeatEvery   time.Duration
	HeartbeatTimeout time.Duration
	RateLimitPerSec  int
	SendQueueSize    int
}

// Load 读取环境变量，缺省值对应 deploy/docker-compose.yml 里的服务。
func Load() Config {
	return Config{
		Addr:             envStr("IM_ADDR", "0.0.0.0:8888"),
		MySQLDSN:         envStr("IM_MYSQL_DSN", "root:root@tcp(127.0.0.1:3306)/imsystem?parseTime=true&charset=utf8mb4"),
		RedisAddr:        envStr("IM_REDIS_ADDR", "127.0.0.1:6379"),
		RedisPass:        envStr("IM_REDIS_PASS", ""),

		HeartbeatEvery:   envDur("IM_HEARTBEAT_EVERY", 30*time.Second),
		HeartbeatTimeout: envDur("IM_HEARTBEAT_TIMEOUT", 90*time.Second),
		RateLimitPerSec:  envInt("IM_RATE_LIMIT", 20),
		SendQueueSize:    envInt("IM_SEND_QUEUE", 64),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
