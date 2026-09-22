# 全局配置文档

## 概述

async 提供了一套全局配置体系，在程序启动时一次性设置，影响后续所有 Pool、Group、Task 等操作。

```go
import "github.com/chichengyu/async"

func init() {
    async.SetDefaultTimeout(30 * time.Second)
    async.SetSubmitTimeout(5 * time.Second)
    async.SetTaskFailLogLevel(async.LogLevelWarn)
}
```

## 目录

- [配置项](#配置项)
  - [SetDefaultTimeout](#setdefaulttimeout)
  - [SetSubmitTimeout](#setsubmittimeout)
  - [SetMaxCleanupDuration](#setmaxcleanupduration)
  - [SetTaskFailLogLevel](#settaskfailloglevel)
  - [SetTraceLogEnabled](#settracelogenabled)
  - [SetLogger](#setlogger)
  - [GetDefaultTimeout](#getdefaulttimeout)
  - [GetSubmitTimeout](#getsubmittimeout)
  - [GetMaxCleanupDuration](#getmaxcleanupduration)
  - [GetTaskFailLogLevel](#gettaskfailloglevel)
  - [GetTraceLogEnabled](#gettracelogenabled)
  - [GetLogger](#getlogger)
  - [MergeCancel](#mergecancel)
  - [WithConfig](#withconfig)
  - [NewPanicError](#newpanicerror)
- [超时常量](#超时常量)
- [日志级别](#日志级别)
- [Logger 接口](#logger-接口)
- [哨兵错误](#哨兵错误)
- [溢出策略](#溢出策略)
- [RingBuffer 环形缓冲](#ringbuffer-环形缓冲)
- [通用工具](#通用工具)
  - [Split / Must / Partition](#split)
- [TraceID 管理](#traceid-管理)
  - [NewTraceID](#newtraceid)
  - [EnsureTraceID](#ensuretraceid)
  - [GetTraceID](#gettraceid)
  - [WithTraceID](#withtraceid)
- [Context 感知日志](#context-感知日志)
  - [LogCtxDebug](#logctxdebug)
  - [LogCtxInfo](#logctxinfo)
  - [LogCtxWarn](#logctxwarn)
  - [LogCtxError](#logctxerror)
  - [LogTaskFail](#logtaskfail)
  - [LogDebug / LogInfo / LogWarn / LogError](#logdebug--loginfo--logwarn--logerror)
- [日志字段辅助函数](#日志字段辅助函数)
- [并发度选择指南](#并发度选择指南)
  - [CPU](#cpu)
  - [IO](#io)
  - [IOMulti](#iomulti)
- [AutoScaleConfig 配置](#autoscaleconfig-配置)
  - [DefaultAutoScaleConfig](#defaultautoscaleconfig)
  - [AutoScaleConfig 字段](#autoscaleconfig-字段)

## 配置项

以下函数在程序启动时（如 `init()`）调用一次，全局生效。

### SetDefaultTimeout

设置所有 Pool/Group/Task 在没有显式指定超时时的默认超时时间。

```go
// 语法
func SetDefaultTimeout(d time.Duration)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 30s | 默认任务超时时间 |

```go
func init() {
    async.SetDefaultTimeout(60 * time.Second) // 改为 60 秒
}
```

---

### SetSubmitTimeout

设置 `Submit` 操作阻塞等待空闲 worker 的最长时间。超时返回 `ErrSubmitTimeout`。

```go
// 语法
func SetSubmitTimeout(d time.Duration)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 5s | 提交超时时间 |

```go
func init() {
    async.SetSubmitTimeout(3 * time.Second) // 提交超过 3 秒即失败
}
```

---

### SetMaxCleanupDuration

设置 Pool 清理失败 goroutine 时的最大等待时间。

```go
// 语法
func SetMaxCleanupDuration(d time.Duration)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 30min | 清理最大等待时间 |

```go
func init() {
    async.SetMaxCleanupDuration(5 * time.Minute)
}
```

---

### SetTaskFailLogLevel

设置任务失败时的日志输出级别。例如设为 `LogLevelWarn` 时，任务失败仅输出 Warning 级别日志。

```go
// 语法
func SetTaskFailLogLevel(lvl LogLevel)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `lvl` | `LogLevel` | `LogLevelError` | 失败日志级别 |

```go
func init() {
    async.SetTaskFailLogLevel(async.LogLevelWarn)
    // 或完全静默
    async.SetTaskFailLogLevel(async.LogLevelNone)
}
```

---

### SetTraceLogEnabled

控制是否启用 Trace 级别的日志输出。Trace 日志包含详细的并发状态、worker 调度信息等。

```go
// 语法
func SetTraceLogEnabled(enabled bool)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | `bool` | true | 是否启用 Trace 日志 |

```go
func init() {
    // 生产环境关闭 Trace 日志减少输出
    async.SetTraceLogEnabled(false)
}
```

---

### SetLogger

注入自定义 Logger 实现，支持 zerolog / zap / logrus 等外部日志库。Logger 接口见下节。

```go
// 语法
func SetLogger(logger Logger)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `logger` | `Logger` | 静默 Logger | 自定义日志实现 |

```go
func init() {
    async.SetLogger(MyZeroLogger{})
}
```

---

### GetDefaultTimeout

获取当前全局默认超时时间。

```go
// 语法
func GetDefaultTimeout() time.Duration
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `time.Duration` | `time.Duration` | 当前默认超时（未设置时返回 30s） |

```go
d := async.GetDefaultTimeout()
log.Printf("当前默认超时: %v", d)
```

---

### GetSubmitTimeout

获取当前提交超时时间。

```go
// 语法
func GetSubmitTimeout() time.Duration
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `time.Duration` | `time.Duration` | 当前提交超时（未设置时返回 5s） |

```go
d := async.GetSubmitTimeout()
log.Printf("当前提交超时: %v", d)
```

---

### GetMaxCleanupDuration

获取当前清理 goroutine 的最大存活时间。

```go
// 语法
func GetMaxCleanupDuration() time.Duration
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `time.Duration` | `time.Duration` | 当前清理超时（未设置时返回 30min） |

---

### GetTaskFailLogLevel

获取当前任务失败日志级别。

```go
// 语法
func GetTaskFailLogLevel() LogLevel
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `LogLevel` | `LogLevel` | 当前失败日志级别（未设置时返回 LogLevelError） |

```go
lvl := async.GetTaskFailLogLevel()
if lvl >= async.LogLevelWarn {
    log.Println("任务失败仅输出 Warning 级别")
}
```

---

### GetTraceLogEnabled

获取 Trace 日志是否已启用。

```go
// 语法
func GetTraceLogEnabled() bool
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `bool` | `bool` | Trace 日志是否启用（默认 true） |

```go
if async.GetTraceLogEnabled() {
    log.Println("Trace 日志已启用")
}
```

---

### GetLogger

获取当前注入的 Logger 实例。

```go
// 语法
func GetLogger() Logger
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `Logger` | `Logger` | 当前 Logger 实例（未注入时返回静默 Logger） |

---

### MergeCancel

合并两个 CancelFunc，调用时依次执行新旧 cancel。用于在已有 cancel 的上下文中安全地叠加新的取消逻辑。

```go
// 语法
func MergeCancel(old, new context.CancelFunc) context.CancelFunc
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `old` | `context.CancelFunc` | 旧的 CancelFunc |
| `new` | `context.CancelFunc` | 新的 CancelFunc |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `context.CancelFunc` | `context.CancelFunc` | 合并后的 CancelFunc |

```go
// 在已有 cancel 的基础上叠加新的 cancel
ctx, cancel1 := context.WithCancel(parent)
ctx, cancel2 := context.WithTimeout(ctx, 5*time.Second)
merged := async.MergeCancel(cancel1, cancel2)
defer merged() // 同时调用 cancel1 和 cancel2
```

---

### WithConfig

如果传入值 n > 0 则返回 n，否则返回 IO()。常用于让用户可传 0 使用默认并发度的场景。

```go
// 语法
func WithConfig(n int) int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `n` | `int` | 用户指定的并发度（0 表示使用默认） |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | n 或 IO() |

```go
concurrency := async.WithConfig(userConcurrency)
p := async.NewPool[int](concurrency)
```

---

### NewPanicError

创建 PanicError，封装 panic 的原始值和当前调用栈。

```go
// 语法
func NewPanicError(v interface{}, stack []byte) *PanicError
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `v` | `interface{}` | panic 的原始值 |
| `stack` | `[]byte` | 调用栈信息 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*PanicError` | `*PanicError` | 封装后的 PanicError |

---

## 超时常量

以下常量定义了各操作的临界超时阈值：

| 常量 | 值 | 说明 |
|------|-----|------|
| `DefaultSubmitTimeout` | 5s | Submit 等待空闲 worker 的默认超时 |
| `WaitContextCleanupWarn` | 5min | 超时清理 goroutine 发出警告的间隔 |
| `WaitContextCleanupError` | 30min | 超时清理 goroutine 发出错误的阈值 |
| `SlotAcquireWarnTimeout` | 30s | 等待并发槽位时发出警告的阈值 |

---

## 日志级别

```go
const (
    LogLevelDebug  = 1
    LogLevelInfo   = 2
    LogLevelWarn   = 3
    LogLevelError  = 4
    LogLevelSilent = 99 // 关闭所有日志
)
```

| 常量 | 说明 |
|------|------|
| `async.LogLevelDebug` | 调试信息，包含详细的并发状态 |
| `async.LogLevelInfo` | 一般信息 |
| `async.LogLevelWarn` | 警告（如任务失败、重试） |
| `async.LogLevelError` | 错误（默认值） |
| `async.LogLevelSilent` | 关闭所有日志输出 |

## Logger 接口

```go
type Logger interface {
    Debug(format string, args ...interface{})
    Info(format string, args ...interface{})
    Warn(format string, args ...interface{})
    Error(format string, args ...interface{})
}
```

### 集成 zerolog

```go
import "github.com/rs/zerolog/log"

type ZeroLogger struct{}

func (l ZeroLogger) Debug(format string, args ...interface{}) {
    log.Debug().Msgf(format, args...)
}
func (l ZeroLogger) Info(format string, args ...interface{}) {
    log.Info().Msgf(format, args...)
}
func (l ZeroLogger) Warn(format string, args ...interface{}) {
    log.Warn().Msgf(format, args...)
}
func (l ZeroLogger) Error(format string, args ...interface{}) {
    log.Error().Msgf(format, args...)
}

func init() {
    async.SetLogger(ZeroLogger{})
}
```

### 集成 zap

```go
import "go.uber.org/zap"

type ZapLogger struct {
    logger *zap.Logger
}

func (l ZapLogger) Debug(format string, args ...interface{}) {
    l.logger.Sugar().Debugf(format, args...)
}
// ... 实现其余方法
```

---

## 哨兵错误

所有模块使用的统一错误变量：

| 错误 | 说明 |
|------|------|
| `ErrPoolClosed` | 池已关闭，任务被丢弃 |
| `ErrPoolWaited` | 在 Wait 之后调用 Submit，任务被丢弃 |
| `ErrPoolWaiting` | 在 Wait 执行期间调用 Submit |
| `ErrSubmitTimeout` | 提交超时，等待空闲 worker 超过 SubmitTimeout |
| `ErrGroupWaited` | 在 Wait 之后调用 Go，任务被丢弃 |
| `ErrGroupWaiting` | 在 Wait 执行期间调用 Go |
| `ErrSkipped` | FailFast 模式下，由于前序失败此任务被跳过 |
| `ErrRateLimiterStopped` | 限流器已停止 |
| `ErrTimeout` | 操作超时 |
| `ErrQueueOverflow` | 队列溢出，任务被拒绝 |

| Context Key | 说明 |
|------|------|
| `TraceIDKey` | context 中 trace_id 的 key |

错误变量类型均为包级别 `var`，可使用 `errors.Is(err, async.ErrSubmitTimeout)` 进行错误判断。

```go
err := p.Submit(ctx, fn)
if errors.Is(err, async.ErrSubmitTimeout) {
    log.Println("提交超时，可考虑增大并发度或 SubmitTimeout")
}
if errors.Is(err, async.ErrPoolClosed) {
    log.Println("池已关闭，无法提交新任务")
}
```

---

## 溢出策略

当队列满时控制行为的策略枚举。配合 `WithMaxPending` + `WithOverflow` 使用。

```go
type OverflowStrategy int

const (
    OverflowBlock = 0 // 阻塞等待（默认）：Submit 会阻塞直到有空位
    OverflowDrop  = 1 // 丢弃：任务被静默丢弃，结果中标记为丢弃
    OverflowError = 2 // 拒绝：Submit 立即返回 ErrQueueOverflow
)
```

| 策略 | 值 | Submit 行为 |
|------|-----|-------------|
| `OverflowBlock` | 0 | 阻塞直到有空位（默认） |
| `OverflowDrop` | 1 | 静默丢弃任务 |
| `OverflowError` | 2 | 立即返回 ErrQueueOverflow |

```go
p := async.NewPool[string](4)
p.WithMaxPending(100)
p.WithOverflow(async.OverflowError) // 超过 100 个等待时立即返回错误

// 超 100 后会出错
err := p.Submit(ctx, fn)
if errors.Is(err, async.ErrQueueOverflow) {
    log.Println("队列已满")
}
```

---

## RingBuffer 环形缓冲

固定容量环形缓冲区，适合海量任务场景下限制内存使用。

```go
type RingBuffer[T any] struct {
    // 内部实现
}
```

配合 `WithRingBuffer` 使用：

```go
// 容量 10000，溢出时丢弃旧数据
p := async.NewPool[string](8)
p.WithRingBuffer(10000, async.OverflowDrop)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `capacity` | `int` | 环形缓冲最大容量 |
| `overflow` | `OverflowStrategy` | 溢出策略 |

---

## 通用工具

以下为 `async` 包根级别的通用便捷函数，无需导入子模块即可使用。

### Split

按谓词将切片拆分为两个：满足条件的放入 `matched`，不满足的放入 `unmatched`。内部调用 `mapreduce.Partition`。

```go
// 语法
func Split[T any](items []T, pred func(T) bool) (matched []T, unmatched []T)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `items` | `[]T` | 原始切片 |
| `pred` | `func(T) bool` | 判断函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `matched` | `[]T` | 满足条件的元素集合 |
| `unmatched` | `[]T` | 不满足条件的元素集合 |

```go
// 分离奇偶数
nums := []int{1, 2, 3, 4, 5, 6}
evens, odds := async.Split(nums, func(n int) bool { return n%2 == 0 })
// evens: [2, 4, 6], odds: [1, 3, 5]

// 分离有效/无效记录
valid, invalid := async.Split(records, func(r *Record) bool { return r.Status == "ok" })
```

### Must

提取值，如果 `err != nil` 则 panic。适用于初始化阶段或测试代码中确保操作必然成功。

```go
// 语法
func Must[T any](val T, err error) T
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `val` | `T` | 结果值 |
| `err` | `error` | 可能的错误 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `T` | `T` | val（err 为 nil 时直接返回） |

```go
// 初始化阶段
cfg := async.Must(loadConfig("config.yaml"))

// 测试代码
db := async.Must(sql.Open("postgres", dsn))

// 一行断言
assert.Equal(t, expected, async.Must(getResult(ctx, input)))
```

> **注意**：Must 会在 err 非 nil 时直接 panic。仅适用于初始化或测试，不要在运行时业务逻辑中使用。

### Partition

按 Result 切片分离成功值和失败值。

```go
// 语法
func Partition[T any](results []Result[T]) (values []T, errors []error)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]Result[T]` | Result 切片 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `values` | `[]T` | 所有成功的值 |
| `errors` | `[]error` | 所有失败的 error |

```go
results := async.Map(ctx, items, 8, fn)
successes, failures := async.Partition(results)
fmt.Printf("成功: %d, 失败: %d\n", len(successes), len(failures))
```

---

## TraceID 管理

### NewTraceID

生成一个新的 TraceID（32 位随机十六进制字符串）。适用于在无 context 时创建新的追踪 ID。

```go
// 语法
func NewTraceID() string
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `string` | `string` | 32 位随机十六进制字符串 |

```go
id := async.NewTraceID()
fmt.Println(id) // "a1b2c3d4e5f6789012345678abcdef01"

// 配合 WithTraceID 注入到 context
ctx = async.WithTraceID(context.Background(), id)
```

---

### EnsureTraceID

确保 context 中包含 TraceID。如果已存在则原样返回，否则自动生成并注入新的 TraceID。

```go
// 语法
func EnsureTraceID(ctx context.Context) context.Context
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 原始上下文 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `context.Context` | `context.Context` | 包含 TraceID 的新 context |

```go
// HTTP 中间件中确保每个请求都有 trace_id
func TraceMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        ctx := async.EnsureTraceID(r.Context())
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

---

### GetTraceID

从 context 中提取 TraceID。如果不存在则返回空字符串。

```go
// 语法
func GetTraceID(ctx context.Context) string
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `string` | `string` | TraceID 值，不存在时返回 "" |

```go
id := async.GetTraceID(ctx)
if id == "" {
    log.Println("警告: context 中无 TraceID")
} else {
    log.Printf("TraceID: %s", id)
}
```

---

### WithTraceID

设置自定义 TraceID 到 context 中。常用于将上游请求的追踪 ID 传递到下游。

```go
// 语法
func WithTraceID(ctx context.Context, id string) context.Context
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 原始上下文 |
| `id` | `string` | 自定义 TraceID |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `context.Context` | `context.Context` | 注入 TraceID 后的新 context |

```go
// HTTP 服务中传递上游 trace_id
func HandleRequest(w http.ResponseWriter, r *http.Request) {
    traceID := r.Header.Get("X-Trace-Id")
    ctx := async.WithTraceID(r.Context(), traceID)

    // 后续所有操作都使用 ctx，日志中自动携带 trace_id
    results, err := async.DefaultMap(ctx, items, func(ctx context.Context, item Item) (Result, error) {
        return process(ctx, item)
    })
}
```

---

## Context 感知日志

context 感知日志函数会自动从 ctx 提取 TraceID 并附加到日志中，支持可变数量的 `LogField` 参数。

### LogCtxDebug

输出 DEBUG 级别日志，自动携带 TraceID。

```go
// 语法
func LogCtxDebug(ctx context.Context, msg string, fields ...LogField)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文（自动提取 TraceID） |
| `msg` | `string` | 日志消息 |
| `fields` | `...LogField` | 可变数量的日志字段 |

```go
async.LogCtxDebug(ctx, "开始处理任务",
    async.Str("task_id", taskID),
    async.Int("batch_size", 100))
// 输出: [trace_id=xxx] 开始处理任务 task_id=xxx batch_size=100
```

---

### LogCtxInfo

输出 INFO 级别日志，自动携带 TraceID。

```go
// 语法
func LogCtxInfo(ctx context.Context, msg string, fields ...LogField)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `msg` | `string` | 日志消息 |
| `fields` | `...LogField` | 可变日志字段 |

```go
async.LogCtxInfo(ctx, "任务完成",
    async.Str("task", "import"),
    async.Dur("cost", elapsed),
    async.Int64("records", count))
```

---

### LogCtxWarn

输出 WARN 级别日志，自动携带 TraceID。

```go
// 语法
func LogCtxWarn(ctx context.Context, msg string, fields ...LogField)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `msg` | `string` | 日志消息 |
| `fields` | `...LogField` | 可变日志字段 |

```go
async.LogCtxWarn(ctx, "任务执行缓慢",
    async.Dur("elapsed", elapsed),
    async.Int64("threshold_ms", 5000))
```

---

### LogCtxError

输出 ERROR 级别日志，自动携带 TraceID。

```go
// 语法
func LogCtxError(ctx context.Context, msg string, fields ...LogField)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `msg` | `string` | 日志消息 |
| `fields` | `...LogField` | 可变日志字段 |

```go
async.LogCtxError(ctx, "请求失败",
    async.Err(err),
    async.Int64("retry", 3),
    async.Str("endpoint", "/api/v1/users"))
```

---

### LogTaskFail

输出任务失败日志。日志级别受 `SetTaskFailLogLevel` 全局配置控制。内部会自动判断错误是否为 `PanicError` 并区分 panic 和普通错误。

```go
// 语法
func LogTaskFail(ctx context.Context, err error, taskLabel string)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `err` | `error` | 任务错误（panic 时自动识别并记录 stack） |
| `taskLabel` | `string` | 任务标签（如 "pool_task"、"map_chunk" 等） |

```go
async.LogTaskFail(ctx, err, "process_order")
// 普通错误: [ERROR] [trace_id=xxx] 任务失败: process_order err=xxx
// Panic:     [ERROR] [trace_id=xxx] 任务 panic: process_order panic=xxx stack=...
```

---

### LogDebug / LogInfo / LogWarn / LogError

无 context 版本的日志函数，不携带 TraceID。

```go
// 语法
func LogDebug(msg string, fields ...LogField)
func LogInfo(msg string, fields ...LogField)
func LogWarn(msg string, fields ...LogField)
func LogError(msg string, fields ...LogField)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `msg` | `string` | 日志消息 |
| `fields` | `...LogField` | 可变日志字段 |

```go
// 程序启动日志（此时可能还没有 context）
async.LogInfo("服务启动", async.Str("version", "v2.0.0"), async.Int("port", 8080))
async.LogWarn("内存使用率较高", async.Int64("percent", 85))
async.LogError("配置加载失败", async.Err(err))
```

---

## 日志字段辅助函数

以下函数用于构造 `LogField`，可传入任意 Context 日志函数的 `fields` 参数中。

| 函数 | 语法 | 类型 | 示例 |
|------|------|------|------|
| `Str` | `func Str(key, val string) LogField` | string | `async.Str("name", "alice")` |
| `Int` | `func Int(key string, val int) LogField` | int | `async.Int("count", 42)` |
| `Int64` | `func Int64(key string, val int64) LogField` | int64 | `async.Int64("id", 12345)` |
| `Float64` | `func Float64(key string, val float64) LogField` | float64 | `async.Float64("ratio", 0.95)` |
| `Bool` | `func Bool(key string, val bool) LogField` | bool | `async.Bool("ok", true)` |
| `Dur` | `func Dur(key string, val time.Duration) LogField` | time.Duration | `async.Dur("cost", d)` |
| `Err` | `func Err(err error) LogField` | error | `async.Err(err)` |

```go
// 组合使用
async.LogCtxInfo(ctx, "批量处理完成",
    async.Str("task", "import_users"),
    async.Int64("total", 10000),
    async.Int64("success", 9850),
    async.Int64("failed", 150),
    async.Dur("cost", 3*time.Second),
    async.Float64("rate", 0.985),
    async.Bool("complete", true))
```

---

## 并发度选择指南

### CPU

返回 CPU 核心数，用于 CPU 密集型任务的并发度。

```go
// 语法
func CPU() int
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | `runtime.NumCPU()` |

```go
p := async.NewPool[int](async.CPU()) // CPU 密集型任务
```

---

### IO

返回 CPU 核心数 × 2，推荐作为 IO 密集型任务的默认并发度。

```go
// 语法
func IO() int
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | `runtime.NumCPU() * 2` |

```go
p := async.NewPool[string](async.IO()) // 网络/数据库任务推荐
```

---

### IOMulti

返回 CPU 核心数 × n，用于高延迟外部服务场景。

```go
// 语法
func IOMulti(n int) int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `n` | `int` | 自定义倍数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | `runtime.NumCPU() * n` |

```go
p := async.NewPool[Response](async.IOMulti(4)) // 高延迟外部 API，4 倍 CPU
```

---

| 场景 | 推荐并发度 | 说明 |
|------|-----------|------|
| 纯计算（加解密、编码、排序） | `CPU()` | CPU 核心数即可，超额订阅无益 |
| 网络请求、数据库查询 | `IO()` | IO 等待多，可超额订阅 2 倍 |
| 文件读写 | `IO()` | 磁盘 IO 也是等待型操作 |
| 批量调用外部 API | `IOMulti(4)` | 高延迟外部服务，可 4 倍 CPU |
| gRPC 长连接调用 | `IO()` | 连接池模式下 IO 即可 |
| Redis/本地缓存读写 | `CPU()` 或 `IOMulti(2)` | 低延迟操作 |

---

## AutoScaleConfig 配置

### DefaultAutoScaleConfig

返回生产级默认自动扩缩容配置。所有零值字段会自动填充为合理默认值。

```go
// 语法
func DefaultAutoScaleConfig() *AutoScaleConfig
```

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*AutoScaleConfig` | `*AutoScaleConfig` | 生产级默认配置 |

| 默认值 | 说明 |
|--------|------|
| `MinWorkers = CPU()` | 最少保留 CPU() 个 worker |
| `MaxWorkers = CPU() * 100` | 最多扩展至 100 倍 CPU |
| `CheckInterval = 5s` | 每 5 秒检测一次负载 |
| `ScaleUpThreshold = 0.7` | 70% 忙碌时触发扩容 |
| `ScaleDownThreshold = 0.2` | 20% 忙碌时触发缩容 |
| `ScaleUpChecks = 3` | 连续 3 次高负载才扩容 |
| `ScaleDownChecks = 5` | 连续 5 次低负载才缩容 |
| `ScaleUpFactor = 1.5` | 每次扩容增加 50% worker |
| `ScaleDownFactor = 0.75` | 每次缩容保留 75% worker |

```go
// 使用默认配置
p := async.NewAutoScalePool[int](4, nil) // nil = DefaultAutoScaleConfig()

// 自定义配置
p := async.NewAutoScalePool[int](4, &async.AutoScaleConfig{
    MinWorkers:     2,
    MaxWorkers:     200,
    CheckInterval:  10 * time.Second,
    ScaleUpThreshold:   0.8,
    ScaleDownThreshold: 0.1,
    ScaleUpChecks:  2,
    ScaleDownChecks: 3,
    ScaleUpFactor:  2.0,
    ScaleDownFactor: 0.5,
})
```

### AutoScaleConfig 字段

```go
type AutoScaleConfig struct {
    MinWorkers         int           // 最小 worker 数，<=0 时使用 CPU()
    MaxWorkers         int           // 最大 worker 数，<=0 时使用 CPU()*100
    CheckInterval      time.Duration // 检测间隔，<=0 时默认 5s
    ScaleUpThreshold   float64       // 扩容阈值（busy/size），<=0 时默认 0.7
    ScaleDownThreshold float64       // 缩容阈值（busy/size），<=0 时默认 0.2
    ScaleUpChecks      int           // 连续满足条件后扩容的检测次数，<=0 时默认 3
    ScaleDownChecks    int           // 连续满足条件后缩容的检测次数，<=0 时默认 5
    ScaleUpFactor      float64       // 扩容倍数，<=0 时默认 1.5
    ScaleDownFactor    float64       // 缩容倍数，<=0 时默认 0.75
}
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `MinWorkers` | `int` | `CPU()` | 最小 worker 数量，防止过度缩容 |
| `MaxWorkers` | `int` | `CPU()*100` | 最大 worker 数量，防止无限扩容 |
| `CheckInterval` | `time.Duration` | 5s | 负载检测周期 |
| `ScaleUpThreshold` | `float64` | 0.7 | busy/concurrency > 此值触发扩容 |
| `ScaleDownThreshold` | `float64` | 0.2 | busy/concurrency < 此值触发缩容 |
| `ScaleUpChecks` | `int` | 3 | 连续 N 次满足扩容条件后才扩容 |
| `ScaleDownChecks` | `int` | 5 | 连续 N 次满足缩容条件后才缩容 |
| `ScaleUpFactor` | `float64` | 1.5 | 扩容因子（新并发度 = 当前 × factor） |
| `ScaleDownFactor` | `float64` | 0.75 | 缩容因子（新并发度 = 当前 × factor） |