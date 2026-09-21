# 全局配置

`async` 库提供了丰富的全局配置项，建议在程序启动时一次性设置。所有配置都是线程安全的，可以在运行时动态修改。

## 目录

- [默认超时](#默认超时)
- [提交超时](#提交超时)
- [清理超时](#清理超时)
- [日志配置](#日志配置)
- [并发度控制](#并发度控制)
- [TraceID 管理](#traceid-管理)
- [安全调用](#安全调用)
- [通用工具](#通用工具)

---

## 默认超时

设置 Group 和 Pool 的全局默认超时时间，当未通过 `WithTimeout` 单独设置时生效。

> **不设置时默认值为 30 秒**，即每个任务最多执行 30 秒后超时取消。

```go
// 获取当前默认超时（初始值 30s）
d := async.GetDefaultTimeout() // → 30s

// 自定义全局默认超时
async.SetDefaultTimeout(10 * time.Second)

// 设为 0 则关闭默认超时（任务无超时限制）
async.SetDefaultTimeout(0)
```

---

## 提交超时

当 Pool/Group 的所有 worker 都在忙且任务队列满时，`Submit` 和 `Go` 方法会阻塞等待，超过此时间会返回 `ErrSubmitTimeout`。

```go
// 设置提交超时为 3 秒（默认 5 秒）
async.SetSubmitTimeout(3 * time.Second)

// 获取当前提交超时
d := async.GetSubmitTimeout()
```

> 也可以为单个 Pool/Group 单独设置：`pool.WithSubmitTimeout(2 * time.Second)`

---

## 清理超时

`WaitTimeout` 或 `WaitContext` 超时后，后台清理 goroutine 的最大存活时间。超过此时间后日志级别从 Warn 升级为 Error。

> **不设置时默认值为 30 分钟**。设为 0 表示无限等待（清理 goroutine 永不强制退出）。

```go
// 获取当前值（初始值 30 分钟）
d := async.GetMaxCleanupDuration()

// 超时后清理 goroutine 最多再等 5 分钟
async.SetMaxCleanupDuration(5 * time.Minute)

// 设为 0 表示无限等待
async.SetMaxCleanupDuration(0)
```

---

## 日志配置

`async` 库不依赖任何第三方日志框架，采用接口注入方式。默认静默运行。

### 设置失败日志级别

> **不设置时默认值为 `LogLevelError`**，失败任务以 Error 级别输出日志。

```go
// 获取当前级别（初始值 LogLevelError）
lvl := async.GetTaskFailLogLevel()

// 失败只打印 Warn
async.SetTaskFailLogLevel(async.LogLevelWarn)

// 关闭所有失败日志
async.SetTaskFailLogLevel(async.LogLevelSilent)
```

可用的日志级别：

| 常量 | 说明 |
|------|------|
| `async.LogLevelError` | 错误级别（默认） |
| `async.LogLevelWarn` | 警告级别 |
| `async.LogLevelInfo` | 信息级别 |
| `async.LogLevelDebug` | 调试级别 |
| `async.LogLevelSilent` | 静默（不输出日志） |

### Trace 日志开关

```go
// 关闭 Trace 日志（默认开启）
async.SetTraceLogEnabled(false)

// 查询是否开启
enabled := async.GetTraceLogEnabled()
```

### 注入自定义 Logger

实现 `async.Logger` 接口后注入：

```go
type Logger interface {
    Log(ctx context.Context, level LogLevel, msg string, fields ...LogField)
    With(fields ...LogField) Logger
    WithContext(ctx context.Context) context.Context
}
```

**示例：注入 zerolog 实现**

```go
import "github.com/rs/zerolog"

type ZerologAdapter struct{}

func (z ZerologAdapter) Log(ctx context.Context, level async.LogLevel, msg string, fields ...async.LogField) {
    // 将 async 日志转发到 zerolog
    event := log.WithLevel(convertLevel(level))
    for _, f := range fields {
        event = event.Interface(f.Key, f.Value)
    }
    event.Msg(msg)
}

func (z ZerologAdapter) With(fields ...async.LogField) async.Logger {
    // 返回带字段的新 logger
    return z
}

func (z ZerologAdapter) WithContext(ctx context.Context) context.Context {
    return ctx
}

// 注入
async.SetLogger(ZerologAdapter{})
```

---

## 并发度控制

库内置了三种并发度计算方式：

```go
// CPU 密集型：等于 CPU 核心数
cpuConcurrency := async.CPU() // 8 核机器返回 8

// IO 密集型：等于 CPU 核心数 * 2（推荐默认值）
ioConcurrency := async.IO() // 8 核机器返回 16

// 自定义倍数：等于 CPU 核心数 * n
customConcurrency := async.IOMulti(4) // 8 核机器返回 32

// WithConfig：n>0 返回 n，否则返回 IO()
concurrency := async.WithConfig(10) // 返回 10
concurrency = async.WithConfig(0)   // 返回 IO()
```

---

## TraceID 管理

所有异步调用会自动注入 `trace_id`，方便日志追踪。

```go
// 确保 ctx 中有 trace_id（没有则自动生成 32 位随机串）
ctx := async.EnsureTraceID(context.Background())

// 从 ctx 中提取 trace_id
traceID := async.GetTraceID(ctx)

// 设置指定的 trace_id（便捷方法，等价于 context.WithValue）
ctx = async.WithTraceID(ctx, "my-custom-id")

// 生成新的随机 trace_id
newID := async.NewTraceID() // 32 位十六进制字符串
```

> 建议在所有异步调用入口处使用 `EnsureTraceID`，所有子任务会自动继承。

---

## 安全调用

`SafeCall` 和 `SafeCallVoid` 自动捕获 panic 并包装为 `PanicError`，避免单个任务崩溃导致整个程序退出。

```go
// 带返回值的安全调用
result, err := async.SafeCall(ctx, input, func(ctx context.Context, item MyType) (string, error) {
    return item.Process(ctx)
})
if err != nil {
    if async.IsPanicError(err) {
        log.Printf("任务 panic: %v", err)
    }
}

// 无返回值的安全调用
err := async.SafeCallVoid(ctx, input, func(ctx context.Context, item MyType) error {
    return item.DoSomething(ctx)
})
```

### 检查 PanicError

```go
result, err := async.SafeCall(ctx, input, riskyFn)
if err != nil && async.IsPanicError(err) {
    // 区分 panic 错误和普通业务错误
    var pe *async.PanicError
    if errors.As(err, &pe) {
        log.Printf("panic: %v at %s", pe.Cause, pe.Stack)
    }
}
```
}

## 通用工具

### Must - err!=nil 则 panic

适用于初始化阶段或测试代码中确保操作必然成功：

```go
// 初始化时确保配置加载成功
cfg := async.Must(loadConfig("config.yaml"))

// 测试代码中直接提取值
val := async.Must(someFn(ctx, input))
// 等价于：
// val, err := someFn(ctx, input)
// if err != nil { panic(err) }
```

---

## 方法速查表

### 超时配置

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `SetDefaultTimeout` | `func SetDefaultTimeout(d time.Duration)` | 设置全局 Pool/Group 任务默认超时 |
| `GetDefaultTimeout` | `func GetDefaultTimeout() time.Duration` | 获取全局默认超时 |
| `SetSubmitTimeout` | `func SetSubmitTimeout(d time.Duration)` | 设置全局提交等待空闲 worker 的超时 |
| `GetSubmitTimeout` | `func GetSubmitTimeout() time.Duration` | 获取全局提交超时 |
| `SetMaxCleanupDuration` | `func SetMaxCleanupDuration(d time.Duration)` | 设置超时后清理 goroutine 的最大等待时长 |
| `GetMaxCleanupDuration` | `func GetMaxCleanupDuration() time.Duration` | 获取最大清理时长 |

### 日志配置

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `SetLogger` | `func SetLogger(logger Logger)` | 注入自定义日志实现（支持 zerolog、zap、slog 等） |
| `GetLogger` | `func GetLogger() Logger` | 获取当前日志器 |
| `SetTaskFailLogLevel` | `func SetTaskFailLogLevel(level TaskLogLevel)` | 设置任务失败时的日志级别 |
| `GetTaskFailLogLevel` | `func GetTaskFailLogLevel() TaskLogLevel` | 获取失败日志级别 |
| `SetTraceLogEnabled` | `func SetTraceLogEnabled(enabled bool)` | 开关 Trace 日志 |
| `GetTraceLogEnabled` | `func GetTraceLogEnabled() bool` | 获取 Trace 日志状态 |

### MergeCancel

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `MergeCancel` | `func MergeCancel(oldCancel, newCancel context.CancelFunc) context.CancelFunc` | 合并两个 CancelFunc，调用返回的函数时依次执行 newCancel 和 oldCancel |

### 日志级别

| 常量 | 值 | 说明 |
|------|---|------|
| `LogLevelError` | `"error"` | 错误（默认） |
| `LogLevelWarn` | `"warn"` | 警告 |
| `LogLevelInfo` | `"info"` | 信息 |
| `LogLevelDebug` | `"debug"` | 调试 |
| `LogLevelSilent` | `"silent"` | 静默（完全不输出） |

### 配置常量

| 常量 | 默认值 | 说明 |
|------|--------|------|
| `DefaultSubmitTimeout` | 5 秒 | Submit 等待空闲 worker 的最大时长 |
| `WaitContextCleanupWarn` | 5 分钟 | 超时清理 goroutine 仍在运行时发出警告的阈值 |
| `WaitContextCleanupError` | 30 分钟 | 超时清理 goroutine 仍在运行时发出错误日志的阈值 |
| `SlotAcquireWarnTimeout` | 30 秒 | Group 等待并发槽位时发出警告的阈值 |

### 哨兵错误

| 错误常量 | 触发场景 |
|---------|---------|
| `ErrSubmitTimeout` | 提交任务时超过提交超时时间 |
| `ErrPoolClosed` | 向已关闭的 Pool 提交任务 |
| `ErrPoolWaited` | Pool 已调用 `Wait` 后继续 `Submit` |
| `ErrGroupWaited` | Group 已调用 `Wait` 后继续 `Go` |
| `ErrSkipped` | FailFast 模式下因已有任务失败而跳过 |
| `ErrRateLimiterStopped` | 限流器已停止时尝试获取令牌 |
| `ErrTimeout` | 通用超时（如 `WaitTimeout` / `WithTimeout` 返回） |

### 并发度

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `CPU` | `func CPU() int` | CPU 密集型并发度（`runtime.NumCPU()`） |
| `IO` | `func IO() int` | IO 密集型并发度（`runtime.NumCPU() × 2`） |
| `IOMulti` | `func IOMulti(n int) int` | 自定义倍数（`runtime.NumCPU() × n`） |
| `WithConfig` | `func WithConfig(n int) int` | n > 0 返回 n，否则返回 IO() |

### TraceID

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `EnsureTraceID` | `func EnsureTraceID(ctx context.Context) context.Context` | 确保 ctx 有 trace_id（不存在则新建） |
| `GetTraceID` | `func GetTraceID(ctx context.Context) string` | 从 ctx 提取 trace_id |
| `WithTraceID` | `func WithTraceID(ctx context.Context, traceID string) context.Context` | 设置指定 trace_id 到 ctx |
| `NewTraceID` | `func NewTraceID() string` | 生成随机的 trace_id |

### 安全调用

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `IsPanicError` | `func IsPanicError(err error) bool` | 检查 err 是否为 PanicError |
| `NewPanicError` | `func NewPanicError(r interface{}) *PanicError` | 从 recover() 值创建 PanicError |

### 通用工具

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `Must[T]` | `func Must[T any](val T, err error) T` | 提取值，err != nil 时 panic |
| `SafeCall[T, R]` | `func SafeCall[T any, R any](ctx context.Context, item T, fn func(ctx context.Context, item T) (R, error)) (R, error)` | 安全调用，捕获 fn 中 panic 转为 error |
| `SafeCallVoid[T]` | `func SafeCallVoid[T any](ctx context.Context, item T, fn func(ctx context.Context, item T) error) error` | 无返回值安全调用，捕获 panic |

### 类型定义

| 类型 | 定义 | 说明 |
|------|------|------|
| `Logger` | `type Logger = core.Logger` | 日志器接口 |
| `LogField` | `type LogField = core.LogField` | 日志字段（key-value 对） |
| `LogLevel` | `type LogLevel = core.LogLevel` | 日志级别字符串别名 |
| `TaskLogLevel` | `type TaskLogLevel = core.TaskLogLevel` | 任务日志级别 |
| `TraceIDKeyType` | `type TraceIDKeyType = core.TraceIDKeyType` | TraceID 的 context key 类型 |
| `PanicError` | `type PanicError = core.PanicError` | 包含 recover 值与堆栈的 panic 错误 |