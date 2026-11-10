# 全局配置文档

## 概述

async 提供了一套全局配置体系，在程序启动时一次性设置。每个配置项的作用范围不同：有些影响后续创建的 Pool/Group/Task，有些仅保存供读取，具体以各配置项说明为准。

> 全局配置项的同步方式不完全相同。建议在启动并发操作前设置，以避免不同操作观察到不同配置；自定义 logger 的并发安全由其实现负责。
>
> **⚠️ DefaultTimeout vs 单次超时**
>
> `SetDefaultTimeout` 影响之后创建的 Pool 和 Group；TaskBuilder 不读取该全局值，需通过 `WithTimeout(d)` 显式设置。
>
> **⚠️ Logger 设置顺序**
>
> 先 `SetLogger` 再启动并发操作，否则部分 goroutine 可能使用默认静默 logger。

```go
import "github.com/chichengyu/async"

func init() {
    async.SetDefaultTimeout(30 * time.Second)
    async.SetSubmitTimeout(5 * time.Second) // 全局记录值；实例级超时需单独配置
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
- [内部日志辅助函数](#内部日志辅助函数)
  - [LogCtxDebug / LogCtxInfo / LogCtxWarn / LogCtxError](#logctxdebug--logctxinfo--logctxwarn--logctxerror)
  - [LogTaskFail](#logtaskfail)
  - [LogDebug / LogInfo / LogWarn / LogError](#logdebug--loginfo--logwarn--logerror)
- [日志字段辅助函数](#日志字段辅助函数)
- [并发度选择指南](#并发度选择指南)
  - [CPU](#cpu)
  - [IO](#io)
  - [IOMulti](#iomulti)
- [AutoScaleConfig 配置](#autoscaleconfig-配置)
  - [自动扩缩容默认值](#自动扩缩容默认值)
  - [AutoScaleConfig 字段](#autoscaleconfig-字段)
- [通用工具](#通用工具)
  - [MergeCancel / WithConfig](#mergecancel)
  - [NewPanicError](#newpanicerror)
  - [Result[T] 结果容器](#resultt-结果容器)
  - [SafeCall / SafeCallVoid](#safecall)
  - [BuildAggregateNoResult / FillNoResultSkipped](#buildaggregatenoresult)
  - [Must](#must)
- [常见错误](#常见错误)

## 配置项

以下函数在程序启动时（如 `init()`）调用一次，全局生效。

### SetDefaultTimeout

设置随后创建的 Pool 和 Group 的默认任务超时时间；TaskBuilder 不读取此值。

全局初始值为 30 秒，修改不会更新已有实例。Group/GroupVoid 构建器的 `DefaultTimeout()` 保存调用当时的值；若保存的是正数，之后即使全局改变，Build 仍使用该正数。Pool 构建器的 `DefaultTimeout()` 清除覆盖，Run 创建池时才读取全局值。两类构建器的 `Timeout(0)` 或负数都保留创建时的全局默认，不能据此关闭非零全局期限；需要清除具体实例期限时，在提交前调用实例 `WithTimeout(0)`。所有期限都依赖业务响应 context，不强制终止任务。

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

设置全局提交超时值并可通过 `GetSubmitTimeout` 读取。当前 Pool/Group 不会自动读取该全局值；要限制具体实例的 `Submit`/`Go` 等待时间，请调用实例或 Builder 的 `WithSubmitTimeout`/`SubmitTimeout`。

```go
func SetSubmitTimeout(d time.Duration)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 初始值 5s；仅接受 >0 的更新 | 全局记录的提交超时值；不会自动应用到已创建或新建 Pool/Group |

```go
func init() {
    async.SetSubmitTimeout(3 * time.Second) // 供配置读取；实例仍需显式 SubmitTimeout(3*time.Second)
}
```

---

### SetMaxCleanupDuration

保存内部等待辅助函数的清理监视时长。当前 Pool 和 Group 的等待方法不读取此设置，它也不会终止任务。内部辅助函数将非正值按 30 分钟处理。

```go
func SetMaxCleanupDuration(d time.Duration)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 30min | 内部辅助函数的监视时长 |

监视 goroutine 在等待超时或取消后启动，每五分钟记录仍在等待的警告；达到有效监视时长后记录 Error 并退出监视。这个时长从后台监视启动时算起，不是业务截止时间。等待 WaitGroup 和调用 `onDone` 的另一个 goroutine 仍继续运行，任务不返回时仍可能长期占用资源；不能用这个设置代替业务取消和最终收尾。非正值仍有三十分钟的有效监视期限，并不表示永久监视。

```go
previous := async.GetMaxCleanupDuration()
async.SetMaxCleanupDuration(2 * time.Minute)
// 仅配置内部辅助函数的监视；不会给 Pool/Group 任务增加两分钟期限。
async.SetMaxCleanupDuration(previous)
```

生产中在启动时固定此全局值，日志后端应并发安全、及时返回；内部辅助函数的取消和快照回调也同步执行，期限到达不能打断这些回调。需要定制辅助函数时，`cancel` 必须可重复和并发调用；`cancelAll`、快照与 `onDone` 不能 panic 或等待当前等待方法返回。`onDone` 按参数顺序执行，成功返回包含这些清理完成；超时返回的快照不表示清理已结束。当前 Pool/Group 自行管理等待流程，不使用这些辅助函数。

---

### SetTaskFailLogLevel

设置任务失败时的日志输出级别。

```go
func SetTaskFailLogLevel(lvl TaskLogLevel)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `lvl` | `TaskLogLevel` | `LogLevelError` | 失败日志级别 |

```go
func init() {
    async.SetTaskFailLogLevel(async.LogLevelWarn)
    async.SetTaskFailLogLevel(async.LogLevelSilent) // 关闭失败日志
}
```

---

### SetTraceLogEnabled

控制创建 TraceID 时是否将带 trace_id 字段的 logger 注入 context；不影响 TraceID 在 context 中的生成和存储。

只有 EnsureTraceID 的缺失值生成路径，以及 WithTraceID 的非空覆盖路径会调用 logger.With/WithContext；已有非 nil 值直接返回，不会补做注入。关闭开关也不会阻止 LogCtx 系列从已有 context 提取并追加 trace_id 字段；它不是整个日志系统的开关。

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
func SetTraceIDKey(key any)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `any` | 非 nil 且可比较的 context key；否则 panic。推荐使用自定义 key 类型避免冲突 |

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

返回设置的原始清理监视时长；非正值在内部辅助函数中按 30 分钟处理，不限制任务执行时间。

### GetTaskFailLogLevel

```go
func GetTaskFailLogLevel() TaskLogLevel
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
func GetTraceIDKey() any
```

返回当前生效的 trace_id context key。

---

## 哨兵错误

以下错误在 `async` 包中全局可用：

| 错误 | 说明 |
|------|------|
| `ErrPoolClosed` | 池已关闭，任务被丢弃 |
| `ErrPoolWaited` | 在 Wait 之后调用 Submit，任务被丢弃 |
| `ErrPoolWaiting` | 在 Wait 或池调整大小期间调用 Submit；与 ErrPoolWaited 是不同的错误值 |
| `ErrSubmitTimeout` | 提交超时，任务被丢弃 |
| `ErrGroupWaited` | 在 Wait 之后调用 Go，任务被丢弃 |
| `ErrGroupWaiting` | 在 Wait 执行期间调用 Go |
| `ErrSkipped` | 由于之前的失败（FailFast 模式），任务被跳过 |
| `ErrRateLimiterStopped` | 限流器已停止 |
| `ErrTimeout` | 操作超时 |
| `ErrTaskGoexit` | Task/Pool/Group、原生并行/分片 Slice/Map 或原生并行、Stream、Flow Pipeline 业务回调调用 runtime.Goexit，未正常返回；逐项结果值为零，不属于 PanicError；Map 容器不保存失败项值，同步串行分支仍会结束调用 goroutine |
| `ErrQueueOverflow` | 队列溢出，任务被拒绝 |

---

## 日志级别

| 常量 | 值 | 说明 |
|------|-----|------|
| `LogLevelError` | 0 | 错误 |
| `LogLevelWarn` | 1 | 警告 |
| `LogLevelInfo` | 2 | 信息 |
| `LogLevelDebug` | 3 | 调试 |
| `LogLevelSilent` | 4 | 静默 |

这些常量属于 `TaskLogLevel`，用于设置任务失败日志级别；它与 Logger 接收的 `LogLevel` 是不同类型。

---

## Logger 接口

```go
type Logger interface {
    Log(ctx context.Context, level LogLevel, msg string, fields ...LogField)
    With(fields ...LogField) Logger
    WithContext(ctx context.Context) context.Context
}
```

`LogField` 包含 `Key string` 和 `Value any`。实现应支持并发调用；`SetLogger(nil)` 恢复静默 logger。

SetLogger/GetLogger 通过原子容器切换和读取接口，后续内部 Log 调用使用当时的全局 logger；已有 context 中由后端保存的状态不会迁移到新 logger。Log/With/WithContext 同步调用，不恢复后端 panic；后端须快速返回，With 返回可用 logger，WithContext 返回非 nil context。类型化 nil 指针仍是非 nil 接口，SetLogger 不会替它恢复静默实现。

---

## 溢出策略

同一策略在不同组件上的行为不同。以下是 Pool 的任务队列达到 `MaxPending` 时，以及环形结果缓冲写满时的行为；具体提交方法的返回值还应参考 [Pool 文档](pool.md)。

| 常量 | Pool 任务队列 | 环形结果缓冲 |
|------|------|------|
| `OverflowBlock` | 通常等待槽位；`TrySubmit` 不等待 | 拒绝新值，`Push` 返回 false，不等待 |
| `OverflowDrop` | 丢弃新任务，提交方法可能返回 nil；结果中记录溢出错误 | 覆盖最旧结果并增加丢弃计数 |
| `OverflowError` | 拒绝新任务并返回 `ErrQueueOverflow` | 拒绝新值，`Push` 返回 false |

---

## 环形缓冲 RingBuffer

```go
type RingBuffer[T any] struct { /* 固定容量环形缓冲区 */ }
```

`async.RingBuffer[T]` 是内部环形缓冲类型的别名。当前公开包没有提供该类型的构造函数；不要使用零值调用 `Push`，零值没有分配缓冲空间。通常应通过 `Pool.WithRingBuffer` 配置结果缓冲，并通过 Pool 的 `Flush` 和 `RingBufDropped` 操作。

已初始化的缓冲区提供以下方法，操作均可并发调用：

| 方法 | 签名 | 说明 |
|------|------|------|
| `Push` | `(value T) bool` | 写入元素；满时 `OverflowDrop` 覆盖最旧元素，其他策略返回 `false` |
| `Pop` | `() (T, bool)` | 读取并移除最旧元素，空时返回零值和 `false` |
| `Peek` | `() (T, bool)` | 读取最旧元素但不移除，空时返回零值和 `false` |
| `Len` / `Cap` | `() int` | 当前元素数 / 固定容量 |
| `IsFull` | `() bool` | 缓冲区是否已满 |
| `Overflow` | `() OverflowStrategy` | 返回创建时的溢出策略 |
| `Flush` | `() []T` | 按 FIFO 顺序取出全部元素 |
| `FlushN` | `(maxCount int) []T` | 取出最多 maxCount 个元素，maxCount<=0 取出全部 |
| `Dropped` / `Disposed` | `() int64` / `() int` | 返回 `OverflowDrop` 覆盖的元素数，`Reset` 后归零 |
| `Reset` | `()` | 清空元素并重置丢弃计数 |

缓冲区满时，`OverflowBlock` 在此类型上不会等待，而是与 `OverflowError` 一样返回 `false`。`Dropped` 不统计被拒绝的写入。

`FlushN(n)` 对非正数（含最小 `int`）或超过现有数量的值取出全部；不会按过大的 `n` 分配结果。`Flush` 和 `FlushN` 在空缓冲上返回非 nil 空切片。`Pop`、`Flush`、`FlushN` 会清零被移除的内部槽位；`Reset` 清零全部内部槽位和丢弃计数，保留固定数组、容量及策略，重复调用后仍可继续写入。返回值仅浅拷贝，指针元素仍指向原对象；修改返回切片的槽位不会修改缓冲槽位，`Reset` 也不会改写已经返回的结果。生产应在消费后释放自己保留的对象引用，这些清零行为不保证对象立即被垃圾回收。容量、元素数与丢弃计数的多次读取不是一个原子快照。指针存储及极端批量参数的交叉验证见 `TestRingBufferDrainsReleaseSlotsAndPreserveReturnedReferences`。

---

## TraceID 管理

### NewTraceID

```go
func NewTraceID() string
```

生成 32 位十六进制 trace_id：正常路径使用 `crypto/rand`；实现保留随机读取失败后使用时间戳和进程内计数器的回退分支，并记录错误。此标识用于日志关联，不应用作认证令牌或据此承诺跨进程绝对唯一。

---

### EnsureTraceID

```go
func EnsureTraceID(ctx context.Context) context.Context
```

检查当前 key 的值是否为 nil；非 nil 时原样返回 ctx，包括已有空字符串或非 string 值。值缺失时才生成 trace_id；启用 TraceLog 时也只在生成路径调用 logger.With/WithContext。建议在异步入口使用，并确保已有值为非空 string；它不修复无效值，GetTraceID 遇非 string 仍会 panic。

---

### GetTraceID

```go
func GetTraceID(ctx context.Context) string
```

从 ctx 中提取 trace_id。不存在时返回空字符串。

context 必须非 nil；当前 key 下若存储了非 string 值，`GetTraceID` 的类型断言会 panic。与其他追踪系统共用 key 时，应确认值类型为 string。

---

### WithTraceID

```go
func WithTraceID(ctx context.Context, traceID string) context.Context
```

非空 traceID 覆盖当前 key 的值，并在启用 TraceLog 时调用 logger.With/WithContext。空 traceID 等同 EnsureTraceID：已有非 nil 值会保留，只有缺失时才生成；不会清除旧 ID。context 须非 nil，返回的 context 需要继续传递。可运行对照见 [Example_traceIDPreservation](tests/public_examples_test.go)。

---

## 内部日志辅助函数

以下日志函数及字段构造器位于 `internal/core`，并未由根包导出；库外代码无法通过 `async` 调用。库外可实现公开的 `async.Logger` 接口并通过 `SetLogger` 注入。

### LogCtxDebug / LogCtxInfo / LogCtxWarn / LogCtxError

```go
func LogCtxDebug(ctx context.Context, msg string, fields ...LogField)
func LogCtxInfo(ctx context.Context, msg string, fields ...LogField)
func LogCtxWarn(ctx context.Context, msg string, fields ...LogField)
func LogCtxError(ctx context.Context, msg string, fields ...LogField)
```

输出带 TraceID 的日志。从 ctx 中自动提取 trace_id。

ctx 可以为 nil，或在当前 key 下存非 string 值，此时不自动附加 trace 字段；非空 string 自动作为第一个 trace_id 字段追加，其后保留调用者原字段，包括同名字段，最终重复键如何展示由后端决定。空字符串不追加。日志同步传给当前全局 logger，不使用 ctx 选择 logger，也不受 TraceLogEnabled 开关过滤；普通 LogCtxDebug 等也不受 TaskFailLogLevel 过滤。

### LogTaskFail

```go
func LogTaskFail(ctx context.Context, err error, msg string)
```

记录任务失败的日志，遵循 `SetTaskFailLogLevel` 指定的日志级别。

LogTaskFail 把 err 原样放入固定 error 字段，err 为 nil 也不自动跳过。仅 Error/Warn/Info/Debug 四个有效值会输出，Silent 和范围外值均静默；该设置不过滤普通 LogCtx/Log 系列。调用方仍须判断业务是否失败，不要把日志调用当成错误检查。

### LogDebug / LogInfo / LogWarn / LogError

```go
func LogDebug(msg string, fields ...LogField)
func LogInfo(msg string, fields ...LogField)
func LogWarn(msg string, fields ...LogField)
func LogError(msg string, fields ...LogField)
```

输出不带 TraceID 的日志。

内部 core.LogFatal 还会在同步日志后端正常返回后调用 os.Exit(1)，不执行 defer；即使任务日志设为 Silent 或关闭 Trace 注入仍会记录 Error。若日志后端阻塞或 panic，则不会到达退出调用。它只存在于内部包，根包没有 async.LogFatal；应用应在自己的入口决定退出及资源收尾。

---

## 日志字段辅助函数

```go
func Str(key, val string) LogField
func Int64(key string, val int64) LogField
func Dur(key string, d time.Duration) LogField
func Any(key string, val any) LogField
func Err(err error) LogField
func Bytes(key string, val []byte) LogField
```

真实实现只有上述六个字段辅助函数；int、float64 和 bool 使用 Any，时长使用 Dur。字段构造器不输出日志，Err 的字段名固定为 error，其余 key 原样保存。Any/Bytes 不深拷贝引用内容，异步日志后端若需保存快照，应自行复制并同步访问；这些函数位于 internal/core，库外应用使用公开的 async.LogField{Key: ..., Value: ...} 构造字段。

Str 保留 string，Int64 保留 int64，Dur 保留 time.Duration，不提前格式化或转换单位；Bytes 保留切片及其底层数组，Any 保留原值且不调用 String/Error。Err(nil) 仍构造 error 字段，类型化 nil 也不自动归一化。空键、重复键、负时长与负整数都不会被拒绝；后端决定编码及重复键处理。逐方法参数、调用示例、生产建议和注意事项见 [Core API](api/core.md)。

生产中用稳定字段名表达明确单位，先按业务判断错误，再选择日志级别；字段构造不会自动判断失败、输出日志或脱敏。异步后端读取前不能并发改写共享的字节数组、map 或指针对象，需要固定内容时制作快照，并限制大对象被日志队列保留的时间。

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

返回自定义倍数的 IO 并发度 = `runtime.NumCPU() * n`；`n <= 0` 时使用 2。该值依据逻辑 CPU 数，不随 `GOMAXPROCS` 的变化而改变。生产中按下游连接数、内存和实际等待比例测量后设置倍数。

```go
async.IOMulti(4) // 8 核返回 32
```

---

## AutoScaleConfig 配置

### 自动扩缩容默认值

根包没有导出 `DefaultAutoScaleConfig` 构造函数。可使用 `async.AutoScaleConfig` 配置字段，或使用相应 Builder 的 `DefaultAutoScale()`；未指定的非正字段在规范化时使用下表默认值。

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

Normalize 只为非正字段补默认，不校验字段关系或浮点有限性；启用前应自行验证 `1 <= MinWorkers <= MaxWorkers`、`0 < ScaleDownThreshold < ScaleUpThreshold < 1`、有限的 `ScaleUpFactor > 1` 和 `0 < ScaleDownFactor < 1`，并按正 int32 范围及资源预算限制 worker 数。NaN、无穷值和不合理倍数不能靠 Normalize 修正。初始 Worker 应在上下界内；Enable 不会立即把目标夹到配置范围。

检查时严格大于扩容阈值、严格小于缩容阈值才累计对应次数；等号及中间区间清零两边连续计数。确认次数满足后，目标为当前数乘倍数再向零截断；若未向目标方向变化，则尝试增减至少 1，随后应用扩容上限或缩容下限。达到边界而不能调整时，这次确认计数也会清零。

对于有限正数系数，先在浮点域把扩容乘积限制至 `MaxWorkers`，或把不能下降的缩容乘积按当前数量处理，再转为整数，避免大系数或乘积溢出改变调整方向；例如初始 3、上限 5、扩容系数 `1e300` 会调至 5。大系数仍不适合作为生产中的常规策略，worker 上下界必须符合上述范围和资源预算。

上下界不是对初始值的即时纠正器。Pool 的全部 Resize 路径会扩展已保存边界，自动调整也使用此路径：若初始值低于 Min，逐步扩容仍低于原 Min 时可下调保存的 Min；初始值高于 Max 的缩容则可能上调保存的 Max。Group 的自动调整不修改配置边界。需要稳定边界时从范围内的初始值启用，避免把范围外初始值当作纠正入口。

---

## 通用工具

### MergeCancel

```go
func MergeCancel(old, new context.CancelFunc) context.CancelFunc
```

合并两个 CancelFunc；调用返回函数时先执行 new，再执行 old。old 为 nil 时直接返回 new；old 非 nil 而 new 为 nil 时，调用返回函数会 panic。调用方应保证 new 非 nil。

```go
cancelBoth := async.MergeCancel(cancelParent, cancelChild)
cancelBoth() // 先取消 child，再取消 parent
```

参数 `old`、`new` 是调用方持有的 `context.CancelFunc`；返回值仍是取消函数，不会立即执行取消。生产中组合标准 context 的取消函数，并在持有者退出时 `defer cancelBoth()`。该函数同步执行，不增加一次性门控；重复或并发调用会再次调用两个非 nil 回调，回调本身必须支持这些调用。`old` 与 `new` 都为 nil 时返回 nil，不能直接调用。

注意：如果 `new` panic 或调用 `runtime.Goexit`，`old` 不会执行；如果 `old` 异常退出，`new` 已执行。组合函数不恢复异常，也不保证任意自定义回调一定返回，不能把它当作两个清理函数的独立执行保障。

### WithConfig

```go
func WithConfig(n int) int
```

如果 n > 0 返回 n，否则返回 `IO()`。

参数 `n` 是应用配置的并发度，返回正整数建议值；该方法不创建 worker、不读取实时负载，也不限制正数上界。

```go
workers := async.WithConfig(configuredWorkers) // configuredWorkers 为应用读取的 int 配置
g := async.Group[int]().Worker(workers).Build()
defer g.Close()
```

生产中先按下游承载和内存预算校验正数配置；非正值回退的 `IO()` 基于 `runtime.NumCPU()*2`，不按 `GOMAXPROCS` 调整，不能视为已测量的安全并发度。

### NewPanicError

```go
func NewPanicError(v any) *PanicError
```

创建 `*async.PanicError`，把 `v` 原样保存在 `Value`，并捕获调用此构造器时的栈到 `Stack`。`v` 可以是任意值；这不是一次业务调用，也不自动触发 recover。

```go
original := errors.New("business panic")
pe := async.NewPanicError(original)
fmt.Println(errors.Is(pe, original)) // false：Unwrap 始终返回 nil
fmt.Println(pe.Value == original)   // true：原始值保存在 Value
log.Printf("task panicked: %+v", pe) // %+v 包含栈；普通 %v 只显示简短描述
```

生产中用 `errors.As` 从任务错误链识别 `*async.PanicError`，需要原始 panic 对象时检查 `Value`。`Unwrap()` 始终返回 nil，即使原值本身实现 error；不能靠 `errors.Is` 穿透到该原值。栈可能包含业务路径，应记录到受控日志。

`NewPanicError` 捕获创建处当前 goroutine 的栈，原始 `Value` 不深拷贝；公开字段 `Value`、`Stack` 的修改会影响后续格式化，跨 goroutine 修改需要同步。`Format` 仅在动词为 `v` 且带 `+` 时追加栈；其他动词仍输出相同简短描述，宽度和精度不生效。尤其 `fmt.Sprintf("%q", pe)` 不会给描述加引号或转义换行；需要字符串转义时使用 `fmt.Sprintf("%q", pe.Error())`。通常由 fmt 调用该方法；直接传入输出状态时，首次写入失败会停止，追加栈的写入错误不会作为返回值报告。

可执行的转义与错误链示例见 [Example_resultErrorChainAndFormatting](tests/public_examples_test.go)；更完整的格式标记及输出失败边界见 [Result/PanicError 契约测试](../internal/core/result_format_contract_test.go)。

### Result[T] 结果容器

`async.Result[T]` 将任务结果与错误放在一起：`Value` 是回调返回值（出错时也可能非零），`Err` 是错误，`Occupied` 标记结果槽位是否已占用。不同 API 对未完成任务和结果槽位的处理不同；使用带空槽的结果切片时先检查 `Occupied`。

| 方法 | 返回值与行为 |
|---|---|
| `Ok()` | 当且仅当 `Err == nil` 返回 true；不检查 `Occupied`，空槽的零值也可能返回 true |
| `Error()` | 原样返回 `Err`，成功时为 nil |
| `IsPanic()` | 使用 `errors.As` 判断错误链中是否包含 `*async.PanicError` |
| `String()` | 失败时返回 `err=<错误>`，否则返回 `value=<值>`，适合日志展示，不适合解析 |

```go
for _, r := range results {
    if !r.Occupied {
        continue // 此 API 的结果切片可能包含尚未占用的槽位
    }
    if r.IsPanic() {
        log.Printf("task panicked: %v", r.Error())
    } else if !r.Ok() {
        log.Printf("task failed: %v", r.Error())
    } else {
        use(r.Value)
    }
}
```

`Occupied` 主要用于结果槽位管理；不要把 `Ok()` 当作槽位已填充的证明。对于不提供空槽的 API，也应先检查错误后再使用值。

`IsPanic()` 也识别包装或 `errors.Join` 连接的错误链。若 `Err` 保存的是类型化 nil 的 `*async.PanicError`，接口本身非 nil，因而 `Ok()` 为 false、`IsPanic()` 为 true；`errors.As` 提取后仍要检查指针非 nil。`String()` 在有错误时仅显示错误，`Value` 仍保存在结构体中；`fmt.Sprintf("%+v", r)` 不会输出内部 panic 的栈。需要栈时从 `r.Error()` 用 `errors.As` 提取非 nil 的 `*async.PanicError`，再对该对象使用 `%+v`。

### SafeCall

```go
func SafeCall[T any, R any](ctx context.Context, item T, fn func(context.Context, T) (R, error)) (R, error)
```

安全调用 fn，自动捕获 panic 并包装为 PanicError。

调用在当前 goroutine 中同步执行；普通返回的值和错误都原样保留，因此非 nil 错误可以伴随非零结果值。发生 panic 时返回 *async.PanicError，结果值为该类型的零值。ctx 原样传给回调，nil 或已取消的 context 都不会阻止调用，回调需要自行处理。nil 函数引起的调用 panic 也会被包装。

生产中应先检查错误再决定能否使用返回值，并保证日志后端不会 panic：恢复业务 panic 时会同步记录日志，日志后端自身的 panic 会继续传播。保护范围限于当前 goroutine 的 panic；业务另建 goroutine 的 panic、runtime.Goexit 和进程退出均不能转成正常错误返回。runtime.Goexit 仍执行 defer 并结束当前 goroutine。

~~~go
value, err := async.SafeCall(ctx, 21, func(ctx context.Context, n int) (int, error) {
    if err := ctx.Err(); err != nil {
        return 0, err
    }
    return n * 2, nil
})
if err != nil {
    return err
}
fmt.Println(value) // 42
~~~

### SafeCallVoid

```go
func SafeCallVoid[T any](ctx context.Context, item T, fn func(context.Context, T) error) error
```

安全调用 fn（fn 只返回 error），自动捕获 panic。

调用同样是同步的，普通错误原样返回；回调或 nil 函数调用的 panic 返回 *async.PanicError。回调须自行检查 ctx 的取消状态，传入 nil context 时也需要自行处理。生产中按有值版本相同的规则控制日志后端及 goroutine 生命周期；保护不覆盖其他 goroutine 的 panic，runtime.Goexit 不会变成错误返回。

~~~go
err := async.SafeCallVoid(ctx, "item", func(ctx context.Context, item string) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    fmt.Println(item)
    return nil
})
if err != nil {
    return err
}
~~~

### BuildAggregateNoResult

```go
func BuildAggregateNoResult(nr *async.GroupNoResult) (total int64, failCnt int64, firstErr error, results []async.Result[struct{}])
```

从已完成等待的 `NoResult` 构建聚合统计和结果快照。总数只统计已占用的结果；稀疏索引的空槽仍保留在结果快照中。

### FillNoResultSkipped

```go
func FillNoResultSkipped(nr *async.GroupNoResult, total int)
```

在 `Wait` 后为 `NoResult` 填充跳过的任务占位；已有稀疏空槽会优先填充。目标数量不大于已有记录数时不做修改。

例如，业务已知应处理的任务总数，但提前停止了提交时，可在等待已提交任务后补齐占位并检查聚合结果：

```go
nr := async.GroupVoid().Worker(4).Build()
defer nr.Close()
var submitErr error
for _, item := range items {
    item := item
    if err := nr.Go(ctx, func(ctx context.Context) error { return process(ctx, item) }); err != nil {
        submitErr = err
        break
    }
}
nr.Wait()
async.FillNoResultSkipped(nr, len(items))
total, failures, firstErr, results := async.BuildAggregateNoResult(nr)
_, _, _, _ = total, failures, firstErr, results
allErrors := []error{submitErr}
for _, result := range results {
    if result.Occupied && result.Err != nil {
        allErrors = append(allErrors, result.Err)
    }
}
return errors.Join(allErrors...)
```

这两个辅助函数要求非 nil 的实例；补齐数依据 Group 的 `TotalCount()`，包括已记录的提交尝试，而不是实际成功执行的数量。

示例所在函数返回 `error`，参数 `ctx` 为非 nil 请求 context、`items` 为待处理输入，`process` 的签名为 `func(context.Context, T) error`，`T` 与元素类型一致；需导入 `context`、`errors` 和 async。先保留提交错误，等待已接受任务，补齐缺失项后按占用槽位合并所有错误，避免只返回首错而丢失其他任务失败；合成的 `ErrSkipped` 也会出现在错误链中。业务须响应收到的任务 context，取消不会回滚已经完成的工作。

`BuildAggregateNoResult` 返回已占用总数、失败数、按槽位顺序的首错和浅拷贝结果切片；它不等待任务、不执行补齐，空槽会保留在切片中。`FillNoResultSkipped` 的 `total` 为目标任务数，必须在最终等待完成且无活跃提交后调用；非正或不大于现有总数时不改变结果。生产中限制目标数及结果/错误存储预算，补齐不会执行业务，也不会补发回调或结果流。共享错误对象的后续修改仍需同步。

### Must

```go
func Must[T any](v T, err error) T
```

如果 err != nil 则 panic，否则返回 v。

参数 `v` 为业务返回值，`err` 为对应错误；成功时原样返回 `v`，失败时以同一错误对象 panic，不返回该值。

```go
// 仅用于允许初始化失败终止的作用域；loadRequiredConfig 是应用提供的加载函数。
required := async.Must(loadRequiredConfig())
_ = required
```

生产请求路径应检查并返回错误；该方法不恢复 panic、不取消 context，也不释放调用方资源。类型化 nil error 接口仍非 nil，会触发 panic，不能据此判定底层指针有效。

---

## 常见错误

- **❌ `SetDefaultTimeout(d)` 对 Pool 不生效**：`SetDefaultTimeout` 设置的是 `core.defaultTimeout`，但 Pool 的 `applyConfig` 中 `TimeOut` 未设置时用**实例构造时的 `core.GetDefaultTimeout()` 值**，之后再调用 `SetDefaultTimeout` 不会影响已创建的 Pool 实例。

  ```go
  // 构造时使用 60 秒默认值；之后修改全局默认值不会影响已创建的 Pool。
  async.SetDefaultTimeout(60 * time.Second)
  _ = async.Pool[int]().Context(ctx).Worker(8).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
      // p 内部 timeout = 60s（构造时快照）
      async.SetDefaultTimeout(10 * time.Second) // 不影响 p
      p.WithTimeout(10 * time.Second)            // 显式更新 p
      return nil
  })
  ```

- **自动扩缩容默认值**：启用时，`AutoScaleConfig` 中的零值字段会填充默认值。传入 `nil` 或零值配置均使用默认参数；如需自定义，可指定部分字段。

  ```go
  g := async.Group[int]().Build()
  defer g.Close()
  g.EnableAutoScale(&async.AutoScaleConfig{MinWorkers: 2, MaxWorkers: 20})
  defer g.DisableAutoScale()
  ```

- **分片每片 worker 的默认值**：内部 `DefaultSizePerShard = 0` 是自动选择标记，创建 ShardedPool 的每片 Pool 时使用 `IO()`（`NumCPU×2`）；公开的 `.Worker(8)` 才显式设置每片 8 个 worker，不存在 `DefaultWorkerSize` 配置入口。总 worker 预算还需乘以分片数，不要把每片默认值当成整个分片池的总量。

- **❌ `Must` 在生产代码中使用**：`Must[T](v, err)` 在 `err != nil` 时直接 panic，仅适合初始化阶段的 fatal 错误。业务逻辑中应返回 error。

  ```go
  // ❌ 危险：生产代码 panic
  result := async.Must(callExternalAPI(ctx))

  // ✅ 正确：返回 error
  result, err := callExternalAPI(ctx)
  if err != nil {
      return fmt.Errorf("external API: %w", err)
  }
  ```

- **区分 panic 和业务错误**：`SafeCall` / `SafeCallVoid` 自动捕获 panic 转换为 error，但 fn 本身的正常 error 也会返回。需要区分时可用 `errors.As` 检查 `PanicError`。

  ```go
  _, err := async.SafeCall(ctx, item, fn)
  if err != nil {
      var panicErr *async.PanicError
      if errors.As(err, &panicErr) && panicErr != nil {
          log.Printf("panic: %v, stack: %s", panicErr.Value, panicErr.Stack)
      } else {
          log.Printf("business error: %v", err)
      }
  }
  ```

  这里的 `ctx` 为请求 context，`item` 为输入，`fn` 的签名为 `func(context.Context, T) (R, error)`，`T` 与输入类型一致；需导入 `context`、`errors`、`log` 和 async。普通业务错误、包装错误和实际 panic 均经一次调用后分类记录，示例仅演示日志，生产函数仍须把错误交给调用方处理。注意 `errors.As` 对类型化 nil 的 `*async.PanicError` 也会返回 true，解引用 `Value` / `Stack` 前必须再检查指针。该检查不改变 SafeCall 的错误身份或恢复范围。

- **❌ 混淆日志开关和 TraceID**：`SetTraceLogEnabled(false)` 不会禁用 context 中的 TraceID；`EnsureTraceID` 仍会生成并存入 TraceID，只是不执行相应的 logger 上下文注入。

- **❌ 以为 `SetSubmitTimeout` 会自动配置实例**：该函数只保存全局值；实例提交等待上限需在 Pool/Group 上单独设置，任务超时也需分别配置。

- **❌ 假设 `MergeCancel` 先调用 old**：返回的函数先调用 new 再调用 old；安排资源清理依赖时应使用这个顺序。

---

## 生产推荐配置

```go
package main

import (
    "time"
    "github.com/chichengyu/async"
)

func init() {
    // 根据业务负载设置默认值和日志级别

    // 1. 默认任务超时：为任务 context 提供取消信号；任务函数仍需响应 context
    async.SetDefaultTimeout(30 * time.Second)

    // 2. 失败日志级别
    async.SetTaskFailLogLevel(async.LogLevelWarn)
}

// 在创建 Pool 时配置并发、提交等待和结果上限：
// async.Pool[int]().Worker(16).SubmitTimeout(5*time.Second).MaxResults(100_000)
```

> 建议在启动并发操作前设置全局配置；Pool 的并发度与容量应结合实际负载和下游能力调整。
