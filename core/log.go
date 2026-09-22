// Package core 的日志子系统，提供可注入的日志接口和结构化日志字段。
//
// 日志接口：
//   - Logger：可注入的日志接口，默认使用空实现（静默运行）
//   - SetLogger(l)：注入自定义日志实现
//   - GetLogger()：获取当前日志器
//
// 日志字段构造函数：
//   - Str(key, val)：字符串字段
//   - Err(err)：错误字段
//   - Dur(key, d)：时间段字段
//   - Any(key, val)：任意类型字段
//   - Bytes(key, val)：字节切片字段
//   - Int64(key, val)：int64 字段
//
// 便捷日志函数（自动从 ctx 中提取 trace_id）：
//   - LogCtxError/LogCtxWarn/LogCtxInfo/LogCtxDebug：带 ctx 的日志
//   - LogError/LogWarn/LogInfo/LogDebug：全局日志（无 ctx）
//   - LogFatal：致命错误后 os.Exit(1)
//
// 使用示例：
//
//	// 注入 zerolog 实现
//	type zLogger struct{}
//	func (z zLogger) Log(ctx context.Context, level core.LogLevel, msg string, fields ...core.LogField) {
//	    // 实现日志输出
//	}
//	func (z zLogger) With(fields ...core.LogField) core.Logger { return z }
//	func (z zLogger) WithContext(ctx context.Context) context.Context { return ctx }
//	core.SetLogger(zLogger{})
package core

import (
	"context"
	"os"
	"time"
)

// LogLevel 日志级别，与外部注入的 Logger 使用的级别一致。
type LogLevel int

const (
	LevelError LogLevel = iota // 错误级别
	LevelWarn                  // 警告级别
	LevelInfo                  // 信息级别
	LevelDebug                 // 调试级别
)

// LogField 结构化日志字段，包含 key 和 value。
type LogField struct {
	Key   string // 字段名
	Value any    // 字段值
}

// Str 创建字符串类型的日志字段。
//
// 参数：
//   - key：字段名
//   - val：字段值
//
// 使用示例：
//
//	core.LogCtxInfo(ctx, "请求完成", core.Str("method", "GET"), core.Str("path", "/api/users"))
func Str(key, val string) LogField { return LogField{Key: key, Value: val} }

// Err 创建错误类型的日志字段，key 固定为 "error"。
//
// 参数：
//   - err：错误对象
//
// 使用示例：
//
//	core.LogCtxError(ctx, "任务失败", core.Err(err))
func Err(err error) LogField { return LogField{Key: "error", Value: err} }

// Dur 创建时间段类型的日志字段。
//
// 参数：
//   - key：字段名
//   - d：时间间隔
//
// 使用示例：
//
//	core.LogCtxInfo(ctx, "处理完成", core.Dur("elapsed", time.Since(start)))
func Dur(key string, d time.Duration) LogField { return LogField{Key: key, Value: d} }

// Any 创建任意类型值的日志字段。
//
// 参数：
//   - key：字段名
//   - val：任意类型的字段值
//
// 使用示例：
//
//	core.LogCtxDebug(ctx, "请求详情", core.Any("headers", reqHeaders), core.Any("body", reqBody))
func Any(key string, val any) LogField { return LogField{Key: key, Value: val} }

// Bytes 创建字节切片类型的日志字段。
//
// 参数：
//   - key：字段名
//   - val：字节切片值
//
// 使用示例：
//
//	core.LogCtxError(ctx, "panic 调用栈", core.Bytes("stack", debug.Stack()))
func Bytes(key string, val []byte) LogField { return LogField{Key: key, Value: val} }

// Int64 创建 int64 类型的日志字段。
//
// 参数：
//   - key：字段名
//   - val：int64 值
//
// 使用示例：
//
//	core.LogCtxInfo(ctx, "统计", core.Int64("count", 100), core.Int64("concurrency", 8))
func Int64(key string, val int64) LogField { return LogField{Key: key, Value: val} }

// Logger 可注入的日志接口。
// 实现此接口即可将 async 库的内部日志接入任意日志框架（如 zerolog、zap、logrus）。
//
// 使用示例：
//
//	// 基于 zerolog 的适配器
//	type ZerologAdapter struct{}
//	func (a ZerologAdapter) Log(ctx context.Context, level core.LogLevel, msg string, fields ...core.LogField) {
//	    // 将 fields 转换为 zerolog 格式并输出
//	}
//	func (a ZerologAdapter) With(fields ...core.LogField) core.Logger {
//	    return a // 返回带字段的新 logger
//	}
//	func (a ZerologAdapter) WithContext(ctx context.Context) context.Context {
//	    return ctx // 将 logger 注入到 context
//	}
type Logger interface {
	Log(ctx context.Context, level LogLevel, msg string, fields ...LogField)
	With(fields ...LogField) Logger
	WithContext(ctx context.Context) context.Context
}

// nopLogger 空日志实现，默认使用，不输出任何日志。
type nopLogger struct{}

func (n nopLogger) Log(_ context.Context, _ LogLevel, _ string, _ ...LogField) {}
func (n nopLogger) With(_ ...LogField) Logger                                  { return n }
func (n nopLogger) WithContext(ctx context.Context) context.Context            { return ctx }

var defaultLogger Logger = &nopLogger{}

// SetLogger 注入自定义日志实现。传入 nil 则恢复为默认空日志实现。
//
// 使用示例：
//
//	// 注入自定义日志
//	core.SetLogger(myZerologAdapter)
//
//	// 恢复静默模式
//	core.SetLogger(nil)
func SetLogger(l Logger) {
	if l == nil {
		defaultLogger = &nopLogger{}
	} else {
		defaultLogger = l
	}
}

// GetLogger 返回当前日志器。
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
	if v := ctx.Value(getTraceIDKey()); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// LogCtxError 输出 Error 级别日志，自动附带 ctx 中的 trace_id。
//
// 参数：
//   - ctx：上下文（用于获取 trace_id）
//   - msg：日志消息
//   - fields：可选的日志字段
//
// 使用示例：
//
//	core.LogCtxError(ctx, "数据库查询失败", core.Err(err), core.Str("sql", query))
func LogCtxError(ctx context.Context, msg string, fields ...LogField) {
	logCtx(ctx, LevelError, msg, fields...)
}

// LogCtxWarn 输出 Warn 级别日志，自动附带 ctx 中的 trace_id。
//
// 参数：
//   - ctx：上下文
//   - msg：日志消息
//   - fields：可选的日志字段
//
// 使用示例：
//
//	core.LogCtxWarn(ctx, "响应时间较长", core.Dur("elapsed", elapsed))
func LogCtxWarn(ctx context.Context, msg string, fields ...LogField) {
	logCtx(ctx, LevelWarn, msg, fields...)
}

// LogCtxInfo 输出 Info 级别日志，自动附带 ctx 中的 trace_id。
//
// 参数：
//   - ctx：上下文
//   - msg：日志消息
//   - fields：可选的日志字段
//
// 使用示例：
//
//	core.LogCtxInfo(ctx, "任务完成", core.Int64("total", 100))
func LogCtxInfo(ctx context.Context, msg string, fields ...LogField) {
	logCtx(ctx, LevelInfo, msg, fields...)
}

// LogCtxDebug 输出 Debug 级别日志，自动附带 ctx 中的 trace_id。
//
// 参数：
//   - ctx：上下文
//   - msg：日志消息
//   - fields：可选的日志字段
//
// 使用示例：
//
//	core.LogCtxDebug(ctx, "请求参数", core.Any("params", params))
func LogCtxDebug(ctx context.Context, msg string, fields ...LogField) {
	logCtx(ctx, LevelDebug, msg, fields...)
}

// LogError 输出全局 Error 级别日志（不使用 ctx）。
//
// 参数：
//   - msg：日志消息
//   - fields：可选的日志字段
//
// 使用示例：
//
//	core.LogError("服务启动失败", core.Err(err))
func LogError(msg string, fields ...LogField) {
	logGlobal(LevelError, msg, fields...)
}

// LogWarn 输出全局 Warn 级别日志（不使用 ctx）。
//
// 参数：
//   - msg：日志消息
//   - fields：可选的日志字段
func LogWarn(msg string, fields ...LogField) {
	logGlobal(LevelWarn, msg, fields...)
}

// LogInfo 输出全局 Info 级别日志（不使用 ctx）。
//
// 参数：
//   - msg：日志消息
//   - fields：可选的日志字段
func LogInfo(msg string, fields ...LogField) {
	logGlobal(LevelInfo, msg, fields...)
}

// LogDebug 输出全局 Debug 级别日志（不使用 ctx）。
//
// 参数：
//   - msg：日志消息
//   - fields：可选的日志字段
func LogDebug(msg string, fields ...LogField) {
	logGlobal(LevelDebug, msg, fields...)
}

// LogFatal 输出 Error 日志后调用 os.Exit(1) 终止进程。
// 仅在无法恢复的致命错误时使用。
//
// 参数：
//   - msg：日志消息
//   - fields：可选的日志字段
//
// 使用示例：
//
//	core.LogFatal("无法连接数据库", core.Err(err))
func LogFatal(msg string, fields ...LogField) {
	logFatal(msg, fields...)
}

// LogTaskFailCtx 根据当前 TaskFailLogLevel 配置输出任务失败日志。
// 如果级别设置为 LogLevelSilent，则不输出任何日志。
//
// 参数：
//   - ctx：上下文
//   - msg：日志消息
//   - fields：可选的日志字段
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
