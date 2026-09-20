package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ==================== LogField 构造函数测试 ====================

func TestLogField_Str(t *testing.T) {
	f := Str("key", "value")
	if f.Key != "key" || f.Value != "value" {
		t.Fatalf("unexpected Str: %v", f)
	}
}

func TestLogField_Err(t *testing.T) {
	f := Err(errors.New("test error"))
	if f.Key != "error" {
		t.Fatalf("expected key='error', got %s", f.Key)
	}
}

func TestLogField_Dur(t *testing.T) {
	d := 5 * time.Second
	f := Dur("elapsed", d)
	if f.Key != "elapsed" || f.Value != d {
		t.Fatalf("unexpected Dur: %v", f)
	}
}

func TestLogField_Any(t *testing.T) {
	f := Any("data", map[string]int{"a": 1})
	if f.Key != "data" {
		t.Fatalf("unexpected Any key: %s", f.Key)
	}
}

func TestLogField_Bytes(t *testing.T) {
	f := Bytes("raw", []byte("hello"))
	if f.Key != "raw" {
		t.Fatalf("unexpected Bytes key: %s", f.Key)
	}
}

func TestLogField_Int64(t *testing.T) {
	f := Int64("count", 100)
	if f.Key != "count" || f.Value != int64(100) {
		t.Fatalf("unexpected Int64: %v", f)
	}
}

// ==================== Logger 接口测试 ====================

func TestNopLogger_ImplementsLogger(t *testing.T) {
	var _ Logger = &nopLogger{}
}

func TestNopLogger_AllMethods(t *testing.T) {
	n := &nopLogger{}
	ctx := context.Background()

	n.Log(ctx, LevelError, "error msg", Str("key", "val"))
	n.Log(ctx, LevelWarn, "warn msg")
	n.Log(ctx, LevelInfo, "info msg")
	n.Log(ctx, LevelDebug, "debug msg")

	logger := n.With(Str("a", "b"))
	logger.Log(ctx, LevelError, "test")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	ctx2 := n.WithContext(ctx)
	if ctx2 != ctx {
		t.Fatal("expected same context from nop logger")
	}
}

func TestSetLogger_Nil(t *testing.T) {
	orig := GetLogger()
	SetLogger(nil)
	if GetLogger() == nil {
		t.Fatal("expected nop logger when nil passed")
	}
	SetLogger(orig)
}

func TestSetLogger_Custom(t *testing.T) {
	orig := GetLogger()
	defer SetLogger(orig)

	SetLogger(&nopLogger{})
	if GetLogger() == nil {
		t.Fatal("expected non-nil custom logger")
	}
}

// ==================== 日志级别测试 ====================

func TestLogLevels(t *testing.T) {
	if LevelError != 0 {
		t.Fatal("expected LevelError=0")
	}
	if LevelWarn != 1 {
		t.Fatal("expected LevelWarn=1")
	}
}

// ==================== logCtx helpers 测试 ====================

func TestLogCtx_NoPanic(t *testing.T) {
	ctx := context.Background()
	logCtx(ctx, LevelError, "test message", Str("key", "value"))
}

func TestLogCtx_NilCtx(t *testing.T) {
	logCtx(nil, LevelError, "test nil ctx")
}

func TestLogGlobal(t *testing.T) {
	logGlobal(LevelInfo, "test global log")
}

// ==================== 高并发日志字段创建 ====================

func TestLogField_HighConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	n := 100000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			switch i % 5 {
			case 0:
				f := Str("key", "value")
				_ = f
			case 1:
				f := Err(errors.New("test"))
				_ = f
			case 2:
				f := Dur("d", time.Second)
				_ = f
			case 3:
				f := Any("a", i)
				_ = f
			case 4:
				f := Int64("c", int64(i))
				_ = f
			}
		}(i)
	}
	wg.Wait()
	t.Logf("100K concurrent LogField creations completed")
}

// TestLogCtx_HighConcurrency 高并发日志调用
func TestLogCtx_HighConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	n := 50000
	ctx := context.Background()
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			logCtx(ctx, LevelDebug, "concurrent log", Str("test", "val"))
		}()
	}
	wg.Wait()
}
