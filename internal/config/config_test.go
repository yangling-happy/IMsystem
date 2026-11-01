package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg := Load()

	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"心跳间隔", cfg.HeartbeatEvery, 30 * time.Second},
		{"心跳超时", cfg.HeartbeatTimeout, 90 * time.Second},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("got %v, want %v", c.got, c.want)
			}
		})
	}
}

func TestLoadEnvOverride(t *testing.T) {
	t.Setenv("IM_HEARTBEAT_TIMEOUT", "45s")
	t.Setenv("IM_RATE_LIMIT", "100")

	cfg := Load()

	if cfg.HeartbeatTimeout != 45*time.Second {
		t.Errorf("HeartbeatTimeout got %v, want 45s", cfg.HeartbeatTimeout)
	}
	if cfg.RateLimitPerSec != 100 {
		t.Errorf("RateLimitPerSec got %d, want 100", cfg.RateLimitPerSec)
	}
}
