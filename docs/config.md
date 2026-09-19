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

---

## 默认超时

设置 Group 和 Pool 的全局默认超时时间，当未通过 `WithTimeout` 单独设置时生效。

```go
// 设置全局默认超时为 30 秒
async.SetDefaultTimeout(30 * time.Second)

// 获取当前全局默认超时
d := async.GetDefaultTimeout()

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

```go
// 超时后清理 goroutine 最多再等 5 分钟
async.SetMaxCleanupDuration(5 * time.Minute)

// 设为 0 表示无限等待
async.SetMaxCleanupDuration(0)

// 获取当前值
d := async.GetMaxCleanupDuration()
```

---

## 日志配置

`async` 库不依赖任何第三方日志框架，采用接口注入方式。默认静默运行。

### 设置失败日志级别

```go
// 任务失败只打印 Warn（默认 Error）
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

---

## 方法速查表

### 超时配置

| 方法 | 说明 |
|------|------|
| `SetDefaultTimeout(d)` | 设置全局默认超时 |
| `GetDefaultTimeout()` | 获取全局默认超时 |
| `SetSubmitTimeout(d)` | 设置全局提交超时 |
| `GetSubmitTimeout()` | 获取全局提交超时 |
| `SetMaxCleanupDuration(d)` | 设置清理最大时长 |
| `GetMaxCleanupDuration()` | 获取清理最大时长 |

### 日志配置

| 方法 | 说明 |
|------|------|
| `SetTaskFailLogLevel(level)` | 设置失败日志级别 |
| `GetTaskFailLogLevel()` | 获取失败日志级别 |
| `SetTraceLogEnabled(bool)` | 开关 Trace 日志 |
| `GetTraceLogEnabled()` | 获取 Trace 日志状态 |
| `SetLogger(logger)` | 注入自定义 Logger |

### 日志级别

| 常量 | 说明 |
|------|------|
| `LogLevelError` | 错误（默认） |
| `LogLevelWarn` | 警告 |
| `LogLevelInfo` | 信息 |
| `LogLevelDebug` | 调试 |
| `LogLevelSilent` | 静默 |

### 并发度

| 函数 | 说明 |
|------|------|
| `CPU()` | CPU 密集型并发度（核心数） |
| `IO()` | IO 密集型并发度（核心数×2） |
| `IOMulti(n)` | 自定义倍数（核心数×n） |
| `WithConfig(n)` | n>0 返回 n，否则返回 IO() |

### TraceID

| 函数 | 说明 |
|------|------|
| `EnsureTraceID(ctx)` | 确保 ctx 有 trace_id |
| `GetTraceID(ctx)` | 提取 trace_id |
| `WithTraceID(ctx, id)` | 设置指定 trace_id |
| `NewTraceID()` | 生成随机 trace_id |

### 安全调用

| 函数 | 说明 |
|------|------|
| `SafeCall[T, R](ctx, input, fn)` | 安全调用（有返回值，捕获 panic） |
| `SafeCallVoid[T](ctx, input, fn)` | 安全调用（无返回值，捕获 panic） |
| `IsPanicError(err)` | 检查是否为 PanicError |
| `NewPanicError(r)` | 创建 PanicError