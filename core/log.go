package core

import (
	"context"
	"os"
	"time"
)

type LogLevel int

const (
	LevelError LogLevel = iota
	LevelWarn
	LevelInfo
	LevelDebug
)

type LogField struct {
	Key   string
	Value any
}

func Str(key, val string) LogField             { return LogField{Key: key, Value: val} }
func Err(err error) LogField                   { return LogField{Key: "error", Value: err} }
func Dur(key string, d time.Duration) LogField { return LogField{Key: key, Value: d} }
func Any(key string, val any) LogField         { return LogField{Key: key, Value: val} }
func Bytes(key string, val []byte) LogField    { return LogField{Key: key, Value: val} }
func Int64(key string, val int64) LogField     { return LogField{Key: key, Value: val} }

type Logger interface {
	Log(ctx context.Context, level LogLevel, msg string, fields ...LogField)
	With(fields ...LogField) Logger
	WithContext(ctx context.Context) context.Context
}

type nopLogger struct{}

func (n nopLogger) Log(_ context.Context, _ LogLevel, _ string, _ ...LogField) {}
func (n nopLogger) With(_ ...LogField) Logger                                  { return n }
func (n nopLogger) WithContext(ctx context.Context) context.Context            { return ctx }

var defaultLogger Logger = &nopLogger{}

func SetLogger(l Logger) {
	if l == nil {
		defaultLogger = &nopLogger{}
	} else {
		defaultLogger = l
	}
}

func GetLogger() Logger {
	return defaultLogger
}

func logCtx(ctx context.Context, level LogLevel, msg string, fields ...LogField) {
	all := make([]LogField, 0, len(fields)+1)
	if ctx != nil {
		if tid := getTraceIDFromCtx(ctx); tid != "" {
			all = append(all, Str("trace_id", tid))
		}
	}
	all = append(all, fields...)
	defaultLogger.Log(ctx, level, msg, all...)
}

func logGlobal(level LogLevel, msg string, fields ...LogField) {
	defaultLogger.Log(context.Background(), level, msg, fields...)
}

func logFatal(msg string, fields ...LogField) {
	defaultLogger.Log(context.Background(), LevelError, msg, fields...)
	os.Exit(1)
}

func getTraceIDFromCtx(ctx context.Context) string {
	if v := ctx.Value(TraceIDKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func LogCtxError(ctx context.Context, msg string, fields ...LogField) {
	logCtx(ctx, LevelError, msg, fields...)
}

func LogCtxWarn(ctx context.Context, msg string, fields ...LogField) {
	logCtx(ctx, LevelWarn, msg, fields...)
}

func LogCtxInfo(ctx context.Context, msg string, fields ...LogField) {
	logCtx(ctx, LevelInfo, msg, fields...)
}

func LogCtxDebug(ctx context.Context, msg string, fields ...LogField) {
	logCtx(ctx, LevelDebug, msg, fields...)
}

func LogError(msg string, fields ...LogField) {
	logGlobal(LevelError, msg, fields...)
}

func LogWarn(msg string, fields ...LogField) {
	logGlobal(LevelWarn, msg, fields...)
}

func LogInfo(msg string, fields ...LogField) {
	logGlobal(LevelInfo, msg, fields...)
}

func LogDebug(msg string, fields ...LogField) {
	logGlobal(LevelDebug, msg, fields...)
}

func LogFatal(msg string, fields ...LogField) {
	logFatal(msg, fields...)
}

func LogTaskFailCtx(ctx context.Context, msg string, fields ...LogField) {
	level := GetTaskFailLogLevel()
	switch level {
	case LogLevelError:
		logCtx(ctx, LevelError, msg, fields...)
	case LogLevelWarn:
		logCtx(ctx, LevelWarn, msg, fields...)
	case LogLevelInfo:
		logCtx(ctx, LevelInfo, msg, fields...)
	case LogLevelDebug:
		logCtx(ctx, LevelDebug, msg, fields...)
	case LogLevelSilent:
	default:
	}
}
