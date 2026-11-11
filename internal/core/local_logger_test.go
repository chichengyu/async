package core

import (
	"context"
	"testing"
)

type localLogRecorder struct{ messages []string }

func (l *localLogRecorder) Log(_ context.Context, _ LogLevel, msg string, _ ...LogField) {
	l.messages = append(l.messages, msg)
}
func (l *localLogRecorder) With(...LogField) Logger                         { return l }
func (l *localLogRecorder) WithContext(ctx context.Context) context.Context { return ctx }

func TestLocalLoggerRoutingAndDefault(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	global, first, second := &localLogRecorder{}, &localLogRecorder{}, &localLogRecorder{}
	SetLogger(global)
	a := WithLogger(context.Background(), first)
	b := WithLogger(context.Background(), second)
	LogCtxInfo(a, "a")
	LogCtxInfo(b, "b")
	LogCtxInfo(WithLogger(a, nil), "default")
	LogCtxInfo(CopyLogger(a, context.Background()), "copied")
	if len(first.messages) != 2 || len(second.messages) != 1 || len(global.messages) != 1 {
		t.Fatalf("misrouted logs: first=%v second=%v global=%v", first.messages, second.messages, global.messages)
	}
	if GetLogger() != global {
		t.Fatal("local settings mutated global logger")
	}
}
