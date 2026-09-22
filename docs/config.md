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

**`SetTraceLogEnabled`** — 开关 Trace 级别日志

| 参数 | 类型 | 说明 |
|------|------|------|
| `enabled` | `bool` | `true` 开启 Trace 日志，`false` 关闭 |

```go
// 关闭 Trace 日志（生产环境建议关闭降低 IO 占用）
async.SetTraceLogEnabled(false)

// 开启 Trace 日志（开发调试用）
async.SetTraceLogEnabled(true)
```

**`GetTraceLogEnabled`** — 查询 Trace 日志是否开启

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `bool` | `bool` | 当前 Trace 日志开关状态 |

```go
enabled := async.GetTraceLogEnabled()
```

### 注入自定义 Logger

实现 `async.Logger` 接口后通过 **`SetLogger`** 注入：

**`SetLogger`** — 注入自定义日志实现

| 参数 | 类型 | 说明 |
|------|------|------|
| `logger` | `Logger` | 实现了 `Logger` 接口的自定义日志器（传 nil 恢复默认静默日志） |

```go
async.SetLogger(myLogger)
```

**`GetLogger`** — 获取当前日志器

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `Logger` | `Logger` | 当前注册的日志器（未注册时返回默认的静默实现） |

```go
logger := async.GetLogger()
```

**Logger 接口定义：**

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

### SafeCall — 带返回值的安全调用

**`SafeCall`** — 执行 fn，捕获 panic 并包装为错误返回

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `item` | `T` | 输入参数 |
| `fn` | `func(context.Context, T) (R, error)` | 要安全执行的函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `R` | `R` | 正常执行时的返回值（panic 时为零值） |
| `error` | `error` | 正常错误或 `(*PanicError)`（panic 时） |

```go
val, err := async.SafeCall(ctx, input, func(ctx context.Context, item MyType) (string, error) {
    return item.Process(ctx)
})
if err != nil {
    var pe *async.PanicError
    if errors.As(err, &pe) {
        log.Printf("任务 panic: %v", pe)
    }
}
```

### SafeCallVoid — 无返回值的安全调用

**`SafeCallVoid`** — 执行无返回值 fn，捕获 panic 并包装为错误

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `item` | `T` | 输入参数 |
| `fn` | `func(context.Context, T) error` | 要安全执行的函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 正常错误或 `(*PanicError)`（panic 时） |

```go
err := async.SafeCallVoid(ctx, input, func(ctx context.Context, item MyType) error {
    return item.DoSomething(ctx)
})
if err != nil {
    var pe *async.PanicError
    if errors.As(err, &pe) {
        log.Printf("任务 panic: %v", pe)
    }
}
```

### 检查 PanicError

使用 `errors.As` 判断是否为 panic 错误：

```go
result, err := async.SafeCall(ctx, input, riskyFn)
if err != nil {
    var pe *async.PanicError
    if errors.As(err, &pe) {
        log.Printf("panic 值: %v\n调用栈: %s", pe.Cause, pe.Stack)
    } else {
        log.Printf("业务错误: %v", err)
    }
}
```

## 通用工具

### Must - err!=nil 则 panic

适用于初始化阶段或测试代码中确保操作必然成功：

**`Must`** — 提取值，err != nil 时 panic

| 参数 | 类型 | 说明 |
|------|------|------|
| `val` | `T` | 函数返回值 |
| `err` | `error` | 函数返回的错误 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `T` | `T` | err == nil 时返回 val，否则 panic(err) |

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

**`MergeCancel`** — 合并两个 CancelFunc，使得调用返回的函数时依次执行 newCancel 和 oldCancel

| 参数 | 类型 | 说明 |
|------|------|------|
| `oldCancel` | `context.CancelFunc` | 旧的取消函数 |
| `newCancel` | `context.CancelFunc` | 新的取消函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `context.CancelFunc` | `context.CancelFunc` | 合并后的取消函数（先调 newCancel 再调 oldCancel） |

```go
oldCancel := context.WithCancel(ctx)

// 包装旧的 cancel，确保新逻辑执行后再调用旧的
newCancel := async.MergeCancel(oldCancel, func() {
    log.Println("清理新资源")
})

// 调用 newCancel 时会先清理新资源，再触发 oldCancel
defer newCancel()
```

> 主要用于 Pool/Group 内部，在创建新 context 的同时能保留上层取消链。

### Log 字段构造器

在实现自定义 Logger 或直接调用 Log 快捷函数时，使用以下构造器创建键值字段：

所有字段构造器均返回 `LogField` 类型，包含 `Key` 和 `Value` 两个字段。

**`Str`** — 创建字符串字段

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 字段名 |
| `val` | `string` | 字段值 |

**`Err`** — 创建错误字段（key 固定为 `"error"`）

| 参数 | 类型 | 说明 |
|------|------|------|
| `err` | `error` | 错误对象 |

**`Dur`** — 创建 Duration 字段

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 字段名 |
| `d` | `time.Duration` | 时间间隔 |

**`Any`** — 创建任意类型字段

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 字段名 |
| `val` | `any` | 任意类型的值 |

**`Bytes`** — 创建字节数组字段

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 字段名 |
| `val` | `[]byte` | 字节数组 |

**`Int64`** — 创建 int64 字段

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 字段名 |
| `val` | `int64` | 整数 |

```go
import "github.com/chichengyu/async"

// 示例：手动构建日志
logger := async.GetLogger()
logger.Log(ctx, async.LogLevelInfo, "请求完成",
    async.Str("method", "POST"),
    async.Dur("latency", 150*time.Millisecond),
    async.Int64("status", 200),
    async.Err(nil),
)
```

### Log 快捷函数

库提供了无需注入 Logger 即可直接输出日志的快捷函数，底层使用已注册的 Logger（未注册时使用默认静默实现）。

#### Context 感知版本（推荐）

这些函数从 context 中提取 trace_id 等元信息，推荐在请求处理链路中使用：

| 函数 | 签名 | 说明 |
|------|------|------|
| `LogCtxError` | `func LogCtxError(ctx context.Context, msg string, fields ...LogField)` | 输出 Error 级别日志 |
| `LogCtxWarn` | `func LogCtxWarn(ctx context.Context, msg string, fields ...LogField)` | 输出 Warn 级别日志 |
| `LogCtxInfo` | `func LogCtxInfo(ctx context.Context, msg string, fields ...LogField)` | 输出 Info 级别日志 |
| `LogCtxDebug` | `func LogCtxDebug(ctx context.Context, msg string, fields ...LogField)` | 输出 Debug 级别日志 |
| `LogTaskFailCtx` | `func LogTaskFailCtx(ctx context.Context, msg string, fields ...LogField)` | 以任务失败日志级别输出（受 `SetTaskFailLogLevel` 控制） |

```go
async.LogCtxInfo(ctx, "用户登录成功",
    async.Str("user_id", "u123"),
    async.Dur("elapsed", 45*time.Millisecond),
)

async.LogCtxWarn(ctx, "临近限流阈值",
    async.Int64("current_qps", 9500),
    async.Int64("limit", 10000),
)
```

#### 无 Context 版本

不需要 trace_id 时使用以下简写：

| 函数 | 签名 | 说明 |
|------|------|------|
| `LogError` | `func LogError(msg string, fields ...LogField)` | Error 级别 |
| `LogWarn` | `func LogWarn(msg string, fields ...LogField)` | Warn 级别 |
| `LogInfo` | `func LogInfo(msg string, fields ...LogField)` | Info 级别 |
| `LogDebug` | `func LogDebug(msg string, fields ...LogField)` | Debug 级别 |
| `LogFatal` | `func LogFatal(msg string, fields ...LogField)` | Fatal 级别（不退出进程，使用最高日志级别输出） |

```go
async.LogError("数据库连接失败",
    async.Err(dbErr),
    async.Str("dsn", maskedDSN),
)
```

#### 任务失败日志

**`LogTaskFail`** — 内部任务失败时自动调用的日志函数

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `err` | `error` | 任务返回的错误 |
| `msg` | `string` | 附加信息 |

输出级别受 `SetTaskFailLogLevel` 控制，默认 Error 级别。可通过设为 `LogLevelSilent` 完全关闭。

```go
// 由 Pool/Group 内部任务失败时自动调用，一般无需手动使用
async.LogTaskFail(ctx, err, "map_task_failed")
```

### 日志级别

| 常量 | 值 | 说明 |
|------|---|------|
| `LogLevelError` | `"error"` | 错误（默认） |
| `LogLevelWarn` | `"warn"` | 警告 |
| `LogLevelInfo` | `"info"` | 信息 |
| `LogLevelDebug` | `"debug"` | 调试 |
| `LogLevelSilent` | `"silent"` | 静默（完全不输出） |

### 背压控制类型

#### OverflowStrategy

**`OverflowStrategy`** — 环形缓冲和背压队列溢出时的处理策略

| 常量 | 说明 |
|------|------|
| `OverflowBlock` | 阻塞等待消费者消费空间（默认，背压队列满时 Submit 阻塞） |
| `OverflowDrop` | 覆盖/丢弃最旧的数据（环形缓冲覆盖最旧结果，队列满时静默丢弃新任务） |
| `OverflowError` | 返回 `ErrQueueOverflow` 错误（调用方自行降级处理） |

```go
p := async.NewPool[string](8).
    WithMaxPending(1000).
    WithOverflow(async.OverflowError) // 满时返回错误
defer p.Close()

err := p.Submit(ctx, fn)
if errors.Is(err, async.ErrQueueOverflow) {
    // 降级处理
    fallbackProcess()
}
```

**`QueueDepth`** — 队列深度查询函数类型

```go
type QueueDepth = func() int
```

由 Pool 内部生成，通过 `p.QueueDepth()` 返回，调用此函数可实时查询当前排队中的任务数。`ShardedPool` 的 `TotalPending()` 即通过遍历所有分片的 QueueDepth 实现。

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
| `ErrQueueOverflow` | **新增** — 背压队列满，任务被拒绝（配合 `WithMaxPending` + `OverflowError`） |

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
| `NewPanicError` | `func NewPanicError(r interface{}) *PanicError` | 从 recover() 值创建 PanicError（通常由 SafeCall 内部使用） |

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
| `Result[T]` | `type Result[T any] struct{ Value T; Err error; Occupied bool }` | 任务结果：Ok() 检查是否成功，IsPanic() 检查是否为 panic |
| `AutoScaleConfig` | `type AutoScaleConfig struct{ ... }` | 自动扩缩容配置（详见下方接口定义） |

### AutoScale 配置

| 函数 / 方法 | 完整签名 | 说明 |
|------------|---------|------|
| `DefaultAutoScaleConfig` | `func DefaultAutoScaleConfig() *AutoScaleConfig` | 返回推荐默认配置（Min=1, Max=NumCPU×4, ScaleUp=1, ScaleDown=2 等） |
| `(*AutoScaleConfig).Normalize` | `func (c *AutoScaleConfig) Normalize()` | 规范化配置值（Min≥1, Max≥Min, ScaleUp≥1, ScaleDown≥1） |

### Logger 接口定义

```go
type Logger interface {
    Log(ctx context.Context, level LogLevel, msg string, fields ...LogField)
    With(fields ...LogField) Logger
    WithContext(ctx context.Context) context.Context
}
```

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `Log` | `Log(ctx context.Context, level LogLevel, msg string, fields ...LogField)` | 输出一条日志（ctx 可携带 trace_id 等上下文信息） |
| `With` | `With(fields ...LogField) Logger` | 创建携带预设字段的新 Logger（用于链式追加固定字段如模块名） |
| `WithContext` | `WithContext(ctx context.Context) context.Context` | 将 Logger 注入 context 中 |