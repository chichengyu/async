package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

type scopeLogger struct{ messages []string }

func (l *scopeLogger) Log(_ context.Context, _ core.LogLevel, msg string, _ ...core.LogField) {
	l.messages = append(l.messages, msg)
}
func (l *scopeLogger) With(...core.LogField) core.Logger               { return l }
func (l *scopeLogger) WithContext(ctx context.Context) context.Context { return ctx }

// Rebinding trace context must preserve the logger selected at Build, including
// after the builder is reset or reused to build another limiter.
func TestRateLimiterBuiltLoggerIsolation(t *testing.T) {
	old := core.GetLogger()
	defer core.SetLogger(old)
	global, local := &scopeLogger{}, &scopeLogger{}
	core.SetLogger(global)
	b := NewRatelimitBuilder(context.Background()).Logger(local).RateLimiter().Rate(1).Per(time.Hour)
	first := b.Build()
	defer first.Close()
	second := b.DefaultLogger().Build()
	defer second.Close()
	_, a := first.WithTraceID(context.Background())
	_, z := second.WithTraceID(context.Background())
	core.LogCtxInfo(a, "local")
	core.LogCtxInfo(z, "global")
	if len(local.messages) != 1 || local.messages[0] != "local" ||
		len(global.messages) != 1 || global.messages[0] != "global" {
		t.Fatalf("misrouted limiter logs: local=%v global=%v", local.messages, global.messages)
	}
}

func TestShardedRateLimiterBuiltLoggerIsolation(t *testing.T) {
	old := core.GetLogger()
	defer core.SetLogger(old)
	global, local := &scopeLogger{}, &scopeLogger{}
	core.SetLogger(global)
	b := NewRatelimitBuilder(context.Background()).Logger(local).Sharded().RateLimiter().Shards(2).Rate(2).Per(time.Hour)
	first := b.Build()
	defer first.Close()
	second := b.DefaultLogger().Build()
	defer second.Close()
	for i := 0; i < 2; i++ {
		_, a := first.GetShard(i).WithTraceID(context.Background())
		_, z := second.GetShard(i).WithTraceID(context.Background())
		core.LogCtxInfo(a, "local")
		core.LogCtxInfo(z, "global")
	}
	if len(local.messages) != 2 || len(global.messages) != 2 {
		t.Fatalf("misrouted shard logs: local=%v global=%v", local.messages, global.messages)
	}
}
