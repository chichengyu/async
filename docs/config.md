# 全局配置文档

## 概述

async 提供了一套全局配置体系，在程序启动时一次性设置，影响后续所有 Pool、Group、Task 等操作。

> **✅ 验证状态**：所有配置项使用 `sync/atomic` 实现线程安全，全子包 Race Detector 零报警确认并发安全。
>
> **⚠️ 全局配置线程安全**
>
> `Set*` 函数基于 `sync/atomic` 实现，可在运行时动态修改。但建议在 `init()` 或 `main()` 中一次性设置，避免竞态导致的瞬时不一致。
>
> **⚠️ DefaultTimeout vs 单次超时**
>
> `SetDefaultTimeout` 设置的是**全局默认值**，仅在没有显式传 timeout 时生效。`Pool.WithTimeout(d)` / `TaskBuilder.WithTimeout(d)` 的 `d` 优先级更高。
>
> **⚠️ Logger 设置顺序**
>
> 先 `SetLogger` 再启动并发操作，否则部分 goroutine 可能使用默认静默 logger。

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
  - [SetTraceIDKey / GetTraceIDKey](#settraceidkey)
  - [GetDefaultTimeout](#getdefaulttimeout)
  - [GetSubmitTimeout](#getsubmittimeout)
  - [GetMaxCleanupDuration](#getmaxcleanupduration)
  - [GetTaskFailLogLevel](#gettaskfailloglevel)
  - [GetTraceLogEnabled](#gettracelogenabled)
  - [GetLogger](#getlogger)
- [哨兵错误](#哨兵错误)
- [日志级别](#日志级别)
- [Logger 接口](#logger-接口)
- [溢出策略](#溢出策略)
- [环形缓冲](#环形缓冲-ringbuffer)
- [TraceID 管理](#traceid-管理)
  - [NewTraceID](#newtraceid)
  - [EnsureTraceID](#ensuretraceid)
  - [GetTraceID](#gettraceid)
  - [WithTraceID](#withtraceid)
- [Context 感知日志](#context-感知日志)
  - [LogCtxDebug / LogCtxInfo / LogCtxWarn / LogCtxError](#logctxdebug)
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
- [通用工具](#通用工具)
  - [MergeCancel / WithConfig](#mergecancel)
  - [NewPanicError](#newpanicerror)
  - [SafeCall / SafeCallVoid](#safecall)
  - [BuildAggregateNoResult / FillNoResultSkipped](#buildaggregatenoresult)
  - [Must](#must)
- [常见错误](#常见错误)

## 配置项

以下函数在程序启动时（如 `init()`）调用一次，全局生效。

### SetDefaultTimeout

设置所有 Pool/Group/Task 在没有显式指定超时时的默认超时时间。

```go
func SetDefaultTimeout(d time.Duration)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 30s | 默认任务超时时间 |

```go
func init() {
    async.SetDefaultTimeout(60 * time.Second)
}
```

---

### SetSubmitTimeout

设置 `Submit` 和 `Go` 操作阻塞等待空闲 worker 的最长时间。超时返回 `ErrSubmitTimeout`。

```go
func SetSubmitTimeout(d time.Duration)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 5s | 提交超时时间 |

```go
func init() {
    async.SetSubmitTimeout(3 * time.Second)
}
```

---

### SetMaxCleanupDuration

设置 Pool 清理失败 goroutine 时的最大等待时间。

```go
func SetMaxCleanupDuration(d time.Duration)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 30min | 清理最大等待时间 |

---

### SetTaskFailLogLevel

设置任务失败时的日志输出级别。

```go
func SetTaskFailLogLevel(lvl LogLevel)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `lvl` | `LogLevel` | `LogLevelError` | 失败日志级别 |

```go
func init() {
    async.SetTaskFailLogLevel(async.LogLevelWarn)
    async.SetTaskFailLogLevel(async.LogLevelSilent) // 关闭失败日志
}
```

---

### SetTraceLogEnabled

控制是否启用 Trace 级别的日志输出。

```go
func SetTraceLogEnabled(enabled bool)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | `bool` | true | 是否启用 Trace 日志 |

---

### SetLogger

注入自定义 Logger 实现，支持 zerolog / zap / logrus 等外部日志库。

```go
func SetLogger(logger Logger)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `logger` | `Logger` | nil（静默） | 自定义日志实现 |

---

### SetTraceIDKey

自定义 trace_id 的 context key，用于对接已有链路追踪系统。

```go
func SetTraceIDKey(key TraceIDKeyType)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `TraceIDKeyType` | context key 类型（通常是自定义的 string 类型） |

---

### GetDefaultTimeout

```go
func GetDefaultTimeout() time.Duration
```

返回当前全局默认超时。

### GetSubmitTimeout

```go
func GetSubmitTimeout() time.Duration
```

返回当前全局提交超时。

### GetMaxCleanupDuration

```go
func GetMaxCleanupDuration() time.Duration
```

返回当前清理最大等待时间。

### GetTaskFailLogLevel

```go
func GetTaskFailLogLevel() LogLevel
```

返回当前任务失败日志级别。

### GetTraceLogEnabled

```go
func GetTraceLogEnabled() bool
```

返回 Trace 日志是否启用。

### GetLogger

```go
func GetLogger() Logger
```

返回当前日志器。

### GetTraceIDKey

```go
func GetTraceIDKey() TraceIDKeyType
```

返回当前生效的 trace_id context key。

---

## 哨兵错误

以下错误在 `async` 包中全局可用：

| 错误 | 说明 |
|------|------|
| `ErrPoolClosed` | 池已关闭，任务被丢弃 |
| `ErrPoolWaited` | 在 Wait 之后调用 Submit，任务被丢弃 |
| `ErrPoolWaiting` | 在 Wait 执行期间调用 Submit |
| `ErrSubmitTimeout` | 提交超时，任务被丢弃 |
| `ErrGroupWaited` | 在 Wait 之后调用 Go，任务被丢弃 |
| `ErrGroupWaiting` | 在 Wait 执行期间调用 Go |
| `ErrSkipped` | 由于之前的失败（FailFast 模式），任务被跳过 |
| `ErrRateLimiterStopped` | 限流器已停止 |
| `ErrTimeout` | 操作超时 |
| `ErrQueueOverflow` | 队列溢出，任务被拒绝 |

---

## 日志级别

| 常量 | 值 | 说明 |
|------|-----|------|
| `LogLevelError` | 1 | 错误 |
| `LogLevelWarn` | 2 | 警告 |
| `LogLevelInfo` | 3 | 信息 |
| `LogLevelDebug` | 4 | 调试 |
| `LogLevelSilent` | 0 | 静默 |

---

## Logger 接口

```go
type Logger interface {
    Debug(msg string, fields ...LogField)
    Info(msg string, fields ...LogField)
    Warn(msg string, fields ...LogField)
    Error(msg string, fields ...LogField)
}
```

---

## 溢出策略

| 常量 | 行为 |
|------|------|
| `OverflowBlock` | 阻塞等待 |
| `OverflowDrop` | 静默丢弃 |
| `OverflowError` | 返回 `ErrQueueOverflow` |

---

## 环形缓冲 RingBuffer

```go
type RingBuffer[T any] struct { /* 固定容量环形缓冲区 */ }
```

方法:

| 方法 | 签名 | 说明 |
|------|------|------|
| `FlushN` | `(maxCount int) []T` | 取出最多 maxCount 个元素，maxCount<=0 取出全部 |
| `Dropped` | `() int64` | 返回因溢出被丢弃的元素数 |

---

## TraceID 管理

### NewTraceID

```go
func NewTraceID() string
```

生成新的随机 trace_id（32 位十六进制字符串）。

---

### EnsureTraceID

```go
func EnsureTraceID(ctx context.Context) context.Context
```

确保 ctx 中有 trace_id，没有则自动生成。建议在所有异步调用的入口处使用。

---

### GetTraceID

```go
func GetTraceID(ctx context.Context) string
```

从 ctx 中提取 trace_id。不存在时返回空字符串。

---

### WithTraceID

```go
func WithTraceID(ctx context.Context, traceID string) context.Context
```

设置指定 trace_id 到 ctx。空 traceID 时自动生成。

---

## Context 感知日志

### LogCtxDebug / LogCtxInfo / LogCtxWarn / LogCtxError

```go
func LogCtxDebug(ctx context.Context, msg string, fields ...LogField)
func LogCtxInfo(ctx context.Context, msg string, fields ...LogField)
func LogCtxWarn(ctx context.Context, msg string, fields ...LogField)
func LogCtxError(ctx context.Context, msg string, fields ...LogField)
```

输出带 TraceID 的日志。从 ctx 中自动提取 trace_id。

### LogTaskFail

```go
func LogTaskFail(ctx context.Context, err error, msg string)
```

记录任务失败的日志，遵循 `SetTaskFailLogLevel` 指定的日志级别。

### LogDebug / LogInfo / LogWarn / LogError

```go
func LogDebug(msg string, fields ...LogField)
func LogInfo(msg string, fields ...LogField)
func LogWarn(msg string, fields ...LogField)
func LogError(msg string, fields ...LogField)
```

输出不带 TraceID 的日志。

---

## 日志字段辅助函数

```go
func Str(key, val string) LogField
func Int(key string, val int) LogField
func Int64(key string, val int64) LogField
func Float64(key string, val float64) LogField
func Bool(key string, val bool) LogField
func Duration(key string, val time.Duration) LogField
func Any(key string, val any) LogField
func Err(err error) LogField
func Bytes(key string, val []byte) LogField
```

---

## 并发度选择指南

### CPU

```go
func CPU() int
```

返回 CPU 密集型并发度 = `runtime.NumCPU()`。适用于纯计算任务。

### IO

```go
func IO() int
```

返回 IO 密集型并发度 = `runtime.NumCPU() * 2`。适用于网络请求、文件读写等 IO 操作。推荐作为默认并发度。

### IOMulti

```go
func IOMulti(n int) int
```

返回自定义倍数的 IO 并发度 = `runtime.NumCPU() * n`。

```go
async.IOMulti(4) // 8 核返回 32
```

---

## AutoScaleConfig 配置

### DefaultAutoScaleConfig

```go
func DefaultAutoScaleConfig() *AutoScaleConfig
```

返回默认自动扩缩容配置。

### AutoScaleConfig 字段

```go
type AutoScaleConfig struct {
    MinWorkers         int           // 最小 worker 数（默认 NumCPU×2）
    MaxWorkers         int           // 最大 worker 数（默认 NumCPU×100）
    CheckInterval      time.Duration // 检查间隔（默认 5s）
    ScaleUpThreshold   float64       // 扩容阈值 busy/concurrency 比率（默认 0.7）
    ScaleDownThreshold float64       // 缩容阈值 busy/concurrency 比率（默认 0.2）
    ScaleUpChecks      int           // 扩容确认次数（连续超阈值次数才扩容，默认 3）
    ScaleDownChecks    int           // 缩容确认次数（连续低于阈值次数才缩容，默认 5）
    ScaleUpFactor      float64       // 扩容因子（默认 1.5）
    ScaleDownFactor    float64       // 缩容因子（默认 0.75）
}
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `MinWorkers` | `int` | `NumCPU×2` | 最小 worker 数 |
| `MaxWorkers` | `int` | `NumCPU×100` | 最大 worker 数 |
| `CheckInterval` | `time.Duration` | 5s | 扩缩容检查间隔 |
| `ScaleUpThreshold` | `float64` | 0.7 | `busy/concurrency > 0.7` 触发扩容 |
| `ScaleDownThreshold` | `float64` | 0.2 | `busy/concurrency < 0.2` 触发缩容 |
| `ScaleUpChecks` | `int` | 3 | 连续 3 次超阈值才扩容，防止瞬时尖峰 |
| `ScaleDownChecks` | `int` | 5 | 连续 5 次低于阈值才缩容，防止短暂低谷 |
| `ScaleUpFactor` | `float64` | 1.5 | 扩容倍数（并发度×1.5） |
| `ScaleDownFactor` | `float64` | 0.75 | 缩容倍数（并发度×0.75） |

---

## 通用工具

### MergeCancel

```go
func MergeCancel(old, new context.CancelFunc) context.CancelFunc
```

合并两个 CancelFunc，调用返回的函数时依次执行 old 和 new。

### WithConfig

```go
func WithConfig(n int) int
```

如果 n > 0 返回 n，否则返回 `IO()`。

### NewPanicError

```go
func NewPanicError(v any) *PanicError
```

创建 PanicError，封装 panic 值和当前调用栈。

### SafeCall

```go
func SafeCall[T any, R any](ctx context.Context, item T, fn func(context.Context, T) (R, error)) (R, error)
```

安全调用 fn，自动捕获 panic 并包装为 PanicError。

### SafeCallVoid

```go
func SafeCallVoid[T any](ctx context.Context, item T, fn func(context.Context, T) error) error
```

安全调用 fn（fn 只返回 error），自动捕获 panic。

### BuildAggregateNoResult

```go
func BuildAggregateNoResult(results []Result[struct{}]) (total int64, failCnt int64, firstErr error)
```

从 NoResult（`[]Result[struct{}]`）构建聚合的统计信息。

### FillNoResultSkipped

```go
func FillNoResultSkipped(results []Result[struct{}], total int) []Result[struct{}]
```

为 NoResult 填充跳过的任务占位（FailFast 导致未执行任务的占位）。

### Must

```go
func Must[T any](v T, err error) T
```

如果 err != nil 则 panic，否则返回 v。

---

## 常见错误

- **❌ `SetDefaultTimeout(d)` 对 Pool 不生效**：`SetDefaultTimeout` 设置的是 `core.defaultTimeout`，但 Pool 的 `applyConfig` 中 `TimeOut` 未设置时用**实例构造时的 `core.GetDefaultTimeout()` 值**，之后再调用 `SetDefaultTimeout` 不会影响已创建的 Pool 实例。

  ```go
  // ❌ 无效：先创建 Pool，后改全局默认
  core.SetDefaultTimeout(60 * time.Second)
  p, _ := pool.New[int](8).WithContext(ctx)
  // p 内部 timeout = 60s（构造时快照）
  core.SetDefaultTimeout(10 * time.Second) // 不影响 p

  // ✅ 正确：Pool.WithTimeout() 或 Builder.Timeout()
  p.WithTimeout(10 * time.Second)
  ```

- **❌ `AutoScaleConfig` 零值 → 从不扩缩容**：`AutoScaleConfig{}` 的 `MinWorkers=0, MaxWorkers=0, CheckInterval=0`，零值配置不会启动任何扩缩容行为。必须用 `DefaultAutoScaleConfig()` 或显式设置所有字段。

  ```go
  // ❌ 无效：零值
  async.Group[int]().EnableAutoScale(&core.AutoScaleConfig{})

  // ✅ 正确
  async.Group[int]().EnableAutoScale(core.DefaultAutoScaleConfig())
  // 或
  async.Group[int]().EnableAutoScale(nil) // nil 自动用 DefaultAutoScaleConfig()
  ```

- **❌ `DefaultWorkerSize` 与 `IO()` 混淆**：`DefaultSizePerShard = 0`，在 applyConfig 时 `0` 自动转为 `WithConfig(0) = IO()`（即 `NumCPU×2`）。显式设 `DefaultWorkerSize` 为 8 则无论 CPU 核数都是 8。注意这个区别。

- **❌ `Must` 在生产代码中使用**：`Must[T](v, err)` 在 `err != nil` 时直接 panic，仅适合初始化阶段的 fatal 错误。业务逻辑中应返回 error。

  ```go
  // ❌ 危险：生产代码 panic
  result := core.Must(callExternalAPI(ctx))

  // ✅ 正确：返回 error
  result, err := callExternalAPI(ctx)
  if err != nil {
      return fmt.Errorf("external API: %w", err)
  }
  ```

- **❌ `SafeCall` 捕获 panic 后不检查 `IsPanic()`**：`SafeCall` / `SafeCallVoid` 自动捕获 panic 转换为 error，但 fn 本身的正常 error 也会返回。需要区分 panic 和业务错误。

  ```go
  r, err := core.SafeCall(ctx, item, fn)
  if err != nil {
      var panicErr *core.PanicError
      if errors.As(err, &panicErr) {
          log.Printf("panic: %v, stack: %s", panicErr.Cause, panicErr.Stack)
      } else {
          log.Printf("business error: %v", err)
      }
  }
  ```

- **❌ `SetTraceIDEnabled(false)` 后仍期望 trace_id**：关闭 trace_id 注入后，`EnsureTraceID(ctx)` 不会向 ctx 注入 trace_id value，也不会从 ctx 提取。日志中的 trace_id 字段为空。

- **❌ `SetDefaultSubmitTimeout` 与 Pool Submit 超时不匹配**：`SubmitTimeout` 是**每次 Submit 调用的超时上限**，而 `SetDefaultTimeout` 是**每个任务的执行超时上限**。两者独立，不要混淆。

- **❌ `MergeCancel` 顺序错误导致资源泄漏**：`MergeCancel(old, new)` 返回的函数先调用 `old()` 再调用 `new()`。如果 `new` 依赖 `old` 的资源（如 span.Finish 依赖 ctx），先 cancel old 会导致 new 执行异常。

---

## 生产推荐配置

```go
package main

import (
    "time"
    "github.com/chichengyu/async"
)

func init() {
    // ========== 必须设置 ==========

    // 1. 默认任务超时：防止单个任务永久阻塞 worker
    async.SetDefaultTimeout(30 * time.Second)

    // 2. 提交超时：worker 满时最多等多久，防止 Submit 无限阻塞
    async.SetSubmitTimeout(5 * time.Second)

    // 3. 最大结果数：防止结果切片无界增长导致 OOM
    async.SetMaxResults(100_000)

    // ========== 推荐设置 ==========

    // 4. 清理超时：WaitTimeout 后残留 goroutine 的最大存活时间
    async.SetMaxCleanupDuration(30 * time.Minute)

    // 5. 失败日志级别：建议生产环境用 Warn，仅打印失败任务
    async.SetTaskFailLogLevel(async.LogLevelWarn)

    // 6. 设置并发度（根据部署环境）
    //    默认值 = NumCPU × 2，对于 8 核以上的容器环境已足够
    //    如果想显式控制：
    // async.SetDefaultWorkerSize(16)

    // ========== 可选设置 ==========

    // 7. TraceID 对接：如果使用 gin/go-zero/tRPC 等框架
    // async.SetTraceIDKey("X-Trace-Id")        // 字符串 key
    // async.SetTraceIDKey(trpc.TraceIDKey)     // tRPC 框架

    // 8. 自定义日志注入
    // async.SetLogger(myLogger)
}
```

> **⚠️ 以上配置建议在 `init()` 中一次性设置**，所有 `Set*` 函数基于 `sync/atomic` 实现，线程安全但运行时修改可能导致瞬时不一致。