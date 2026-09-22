# Pool（协程池）文档

## 概述

`Pool[T]` 是泛型协程池，复用 goroutine，适合长期运行、反复提交任务的后台服务场景。

**核心特性**:
- 32 分片无锁存储，千万级并发无锁竞争
- 自动扩缩容（基于 busy/concurrency 比率）
- 流式结果消费（Stream channel + ResultCallback）
- 环形缓冲（固定内存，适合海量任务）
- 背压控制（MaxPending + Overflow 策略）
- FailFast 快速失败
- 丰富的 With* 链式配置组合方法

> **⚠️ 生命周期必须成对**
>
> 创建 Pool 后必须调用 `Close()`、`WaitAndClose()` 或 `CloseAndWait()`，否则 worker goroutine 将永久泄漏。
>
> **⚠️ Wait 一次性**
>
> `Wait()` 只能调用一次，调用后不可再 Submit。如需重复提交+等待，请使用 `Reset()`。
>
> **⚠️ Submit 后必须收集**
>
> Submit 后必须调用 Wait / WaitAndClose / CloseAndWait 中的一种来收集结果，否则创建的 goroutine 会泄漏。
>
> **⚠️ Close 不等待**
>
> `Close()` 会立即停止 worker，不等待正在执行的任务完成。生产环境推荐 `CloseAndWait()`。

## 目录

- [创建](#创建)
  - [NewPool / DefaultPool](#newpool)
  - [NewAutoScalePool](#newautoscalepool)
  - [NewNoResultPool / DefaultNoResultPool](#newnoresultpool--defaultnoresultpool)
- [任务提交](#任务提交)
  - [Submit / TrySubmit / SubmitAt](#submit)
- [关闭与等待](#关闭与等待)
  - [Wait / WaitAndClose / Close / CloseAndWait](#wait)
  - [CloseAndWaitTimeout / CloseByIdle / Reset](#closeandwaittimeout)
- [超时与上下文](#超时与上下文)
  - [WithTimeout / WaitTimeout / WaitContext](#withtimeout)
  - [WithContext / WithFailFast / WithFFCtx](#withcontext)
- [With* 组合方法速查](#with-组合方法速查)
- [结构体类型](#结构体类型)
  - [PoolStats / SubmitResult](#poolstats)
- [状态查询](#状态查询)
  - [Size / Active / Busy / Pending / Stats](#size)
  - [SuccessCount / FailCount / TotalCount / HasError / QueueDepth](#successcount)
- [错误提取](#错误提取)
  - [Errors / FirstError / JoinErrors / Values](#errors)
- [自动扩缩容](#自动扩缩容)
  - [EnableAutoScale / IsAutoScaleEnabled / DisableAutoScale](#enableautoscale)
- [流式结果消费](#流式结果消费)
  - [WithStreaming / StreamResults / StreamDropped / WithResultCallback](#withstreaming)
- [环形缓冲](#环形缓冲)
  - [WithRingBuffer / Flush / RingBufDropped](#withringbuffer)
- [背压控制](#背压控制)
  - [WithMaxPending / WithOverflow / WithMaxResults](#withmaxpending)
  - [Resize / ResizeAndWaitTimeout](#resize)
- [NoResultPool 辅助函数](#noresultpool-辅助函数)
  - [SubmitAction / TrySubmitAction / GoAction](#submitaction)
  - [SubmitActionWithTimeout / GoActionWithTimeout](#submitactionwithtimeout)
  - [BuildAggregateNoResult / FillNoResultSkipped](#buildaggregatenoresult-1)
- [Pool 便捷函数](#pool-便捷函数)
  - [Submit / SubmitN / SubmitSafeN / SubmitBatch / MapPool / ForEachPool](#submit-便捷)
- [MultiPool（水平分片协程池）](#multipool水平分片协程池)
  - [创建：Shard / DefaultShard](#创建shard--defaultshard)
  - [基础查询：ShardCount / GetShard](#基础查询shardcount--getshard)
  - [任务分发：Submit / TrySubmit / SubmitKeyed / TrySubmitKeyed / SubmitBatch](#任务分发submit--trysubmit--submitkeyed--trysubmitkeyed--submitbatch)
  - [结果收集：Wait / WaitAndClose / Close](#结果收集wait--waitandclose--close)
  - [链式配置：WithTimeout / WithSubmitTimeout / WithStreaming / WithResultCallback](#链式配置withtimeout--withsubmittimeout--withstreaming--withresultcallback)
  - [链式配置：WithRingBuffer / WithMaxPending / WithOverflow / WithMaxResults](#链式配置withringbuffer--withmaxpending--withoverflow--withmaxresults)
  - [统计聚合：TotalActive / TotalBusy / TotalPending / TotalWorkerCount](#统计聚合totalactive--totalbusy--totalpending--totalworkercount)
  - [统计聚合：TotalFailCount / TotalSuccessCount / TotalCount](#统计聚合totalfailcount--totalsuccesscount--totalcount)
  - [Flush](#flush-multi)
- [架构说明](#架构说明)
- [性能基准](#性能基准)

## 创建

### NewPool

```go
// 语法
func NewPool[T any](size int) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `size` | `int` | worker 数量，<=0 时自动使用 `core.IO()`（CPU 核数×2） |

```go
p := async.NewPool[string](8)   // 8 个 worker
p := async.NewPool[int](0)      // 使用 IO 并发度
p := async.NewPool[int](async.IO()) // 显式指定 IO 并发度
p := async.NewPool[int](async.CPU()) // CPU 密集型并发度
```

### DefaultPool

```go
// 语法
func DefaultPool[T any]() *Pool[T]
```

等价于 `NewPool[T](core.IO())`，使用默认 IO 并发度。

```go
p := async.DefaultPool[string]()
defer p.Close()
```

### NewAutoScalePool

```go
// 语法
func NewAutoScalePool[T any](size int, cfg *AutoScaleConfig) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `size` | `int` | 初始 worker 数，<=0 使用 IO 并发度 |
| `cfg` | `*AutoScaleConfig` | 扩缩容配置，nil 使用 DefaultAutoScaleConfig() |

```go
p := async.NewAutoScalePool[int](4, nil) // 4 worker + 默认扩缩容配置
defer p.Close()

p := async.NewAutoScalePool[int](4, &async.AutoScaleConfig{
    MinWorkers:       2,
    MaxWorkers:       500,
    CheckInterval:    3 * time.Second,
    ScaleUpThreshold: 0.6,
    ScaleUpFactor:    1.5,
    ScaleDownFactor:  0.7,
})
```

### NewNoResultPool / DefaultNoResultPool

```go
// 语法
func NewNoResultPool(size int) *NoResultPool  // NoResultPool = Pool[struct{}]
func DefaultNoResultPool() *NoResultPool
```

无返回值协程池，适合批量写入、通知发送等只关心错误的场景。

```go
p := async.NewNoResultPool(8)
defer p.Close()

p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
    return struct{}{}, db.Insert(ctx, record)
})
p.Wait()
```

---

## 任务提交

### Submit

```go
// 语法
func (p *Pool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，用于取消和 TraceID |
| `fn` | `func(context.Context) (T, error)` | 任务函数，接收派生 context |
| 返回 | `error` | ErrPoolClosed / ErrPoolWaited / ErrPoolWaiting / ErrSubmitTimeout / ErrQueueOverflow |

提交一个任务到协程池。当 worker 全忙时阻塞等待空闲 worker（受 submitTimeout 限制）。

```go
err := p.Submit(ctx, func(ctx context.Context) (string, error) {
    return process(ctx), nil
})
if err != nil {
    log.Printf("提交失败: %v", err)
}
```

### TrySubmit

```go
// 语法
func (p *Pool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error
```

非阻塞提交，worker 满时立即返回 `ErrSubmitTimeout`。

```go
err := p.TrySubmit(ctx, fn)
if errors.Is(err, async.ErrSubmitTimeout) {
    // 满时降级处理
    fallbackProcess()
}
```

### SubmitAt

```go
// 语法
func (p *Pool[T]) SubmitAt(index int, ctx context.Context, fn func(context.Context) (T, error)) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `index` | `int` | 结果在 `Wait()` 返回切片中的位置 |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |

提交到指定结果位置，结果保持索引顺序。

```go
for i, url := range urls {
    p.SubmitAt(i, ctx, func(ctx context.Context) (string, error) {
        return fetchURL(ctx, url), nil
    })
}
results := p.Wait() // results[i] 对应 urls[i]
```

---

## 关闭与等待

### Wait

```go
// 语法
func (p *Pool[T]) Wait() []core.Result[T]
```

等待所有已提交任务完成，返回结果切片（按提交顺序）。调用后不能再提交新任务。

```go
results := p.Wait()
for _, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

### WaitAndClose

```go
// 语法
func (p *Pool[T]) WaitAndClose() []core.Result[T]
```

等待所有已提交任务完成，关闭池，返回结果切片。

```go
results := p.WaitAndClose()
```

### Close

```go
// 语法
func (p *Pool[T]) Close()
```

停止所有 worker，丢弃未执行的任务。与 `WaitAndClose` 不同，不等待正在执行的任务完成。

```go
defer p.Close()
```

### CloseAndWait

```go
// 语法
func (p *Pool[T]) CloseAndWait()
```

关闭池并等待所有 worker goroutine 退出。

```go
p.CloseAndWait()
```

### CloseAndWaitTimeout

```go
// 语法
func (p *Pool[T]) CloseAndWaitTimeout(timeout time.Duration) (ok bool, done <-chan struct{})
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeout` | `time.Duration` | 等待超时 |
| 返回值1 `ok` | `bool` | 是否在超时内完成关闭 |
| 返回值2 `done` | `<-chan struct{}` | 完成信号 channel |

带超时的关闭并等待 worker 退出。

```go
ok, doneCh := p.CloseAndWaitTimeout(10 * time.Second)
if !ok {
    log.Println("关闭超时")
}
```

### CloseByIdle

```go
// 语法
func (p *Pool[T]) CloseByIdle(idleDuration time.Duration)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `idleDuration` | `time.Duration` | 空闲等待时间 |

在空闲指定时间后自动关闭。适合临时使用的一次性 Pool。

```go
p := async.NewPool[string](8)
// ... 使用 ...
p.CloseByIdle(5 * time.Minute) // 空闲 5 分钟后自动关闭
```

### Reset

```go
// 语法
func (p *Pool[T]) Reset() (*Pool[T], error)
```

关闭旧池，创建同等大小的新池（复用变量）。需在 Wait 后无活跃任务时调用。

```go
p := async.NewPool[string](8)
// ... 第一轮处理 ...
results := p.Wait()

// 重置后开始第二轮
newPool, err := p.Reset()
if err != nil {
    log.Fatal(err)
}
p = newPool
```

---

## 超时与上下文

### WithTimeout

```go
// 语法
func (p *Pool[T]) WithTimeout(d time.Duration) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 每个任务的超时时间 |

设置每个任务的超时时间，超时后任务 context 自动取消。

```go
p := async.NewPool[string](8).WithTimeout(30 * time.Second)
```

### WaitTimeout

```go
// 语法
func (p *Pool[T]) WaitTimeout(timeout time.Duration) (results []core.Result[T], ok bool)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeout` | `time.Duration` | 等待超时 |
| 返回值1 `results` | `[]core.Result[T]` | 已完成任务的结果 |
| 返回值2 `ok` | `bool` | 是否所有任务完成 |

带超时的等待。`ok=false` 表示部分任务未完成。

```go
results, ok := p.WaitTimeout(5 * time.Second)
if !ok {
    log.Println("等待超时，部分任务未完成")
}
```

### WaitContext

```go
// 语法
func (p *Pool[T]) WaitContext(ctx context.Context) (results []core.Result[T], ok bool)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 控制等待的 context |
| 返回值1 `results` | `[]core.Result[T]` | 已完成任务的结果 |
| 返回值2 `ok` | `bool` | 是否所有任务完成 |

通过 context 控制等待。`ctx` 被取消/Done 时返回当前已有结果。

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
results, ok := p.WaitContext(ctx)
```

### WithContext

```go
// 语法
func (p *Pool[T]) WithContext(ctx context.Context) (*Pool[T], context.Context)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 原始上下文 |
| 返回 | `(*Pool[T], context.Context)` | 新 Pool + 可取消的子 context |

创建绑定到 Pool 生命周期的子 context。Pool Close 时自动取消返回的 ctx。

```go
p, boundCtx := p.WithContext(ctx)
// boundCtx 在 Pool.Close() 时自动取消
go func() {
    <-boundCtx.Done()
    log.Println("Pool 已关闭")
}()
```

### WithFailFast

```go
// 语法
func (p *Pool[T]) WithFailFast(ctx context.Context) (*Pool[T], context.Context)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 原始上下文 |
| 返回 | `(*Pool[T], context.Context)` | Pool + 可取消的子 context |

启用 FailFast 模式：第一个任务失败立即取消所有其他任务。

```go
p, ffCtx := p.WithFailFast(ctx)
for _, item := range items {
    p.Submit(ffCtx, func(ctx context.Context) (string, error) {
        return validate(ctx, item) // 任一失败即取消全部
    })
}
```

### WithFFCtx

```go
// 语法
func (p *Pool[T]) WithFFCtx(ctx context.Context) (*Pool[T], context.Context)
```

`WithFailFast` 的缩写形式。

---

## With* 组合方法速查

以下是所有 `With*` 组合方法，命名格式为 `With[FF][Ctx|Timeout|SubmitTO|TraceID]*`：

| 方法 | 说明 | 签名 |
|------|------|------|
| `WithTraceID(ctx)` | 注入 TraceID | `(*Pool[T], context.Context)` |
| `WithContext(ctx)` | Context 绑定 | `(*Pool[T], context.Context)` |
| `WithFailFast(ctx)` | 快速失败 | `(*Pool[T], context.Context)` |
| `WithFFCtx(ctx)` | FailFast 缩写 | `(*Pool[T], context.Context)` |
| `WithTimeout(d)` | 任务超时 | `*Pool[T]` |
| `WithSubmitTimeout(d)` | 提交超时 | `*Pool[T]` |
| `WithFFTraceID(ctx)` | FF + TraceID | `(*Pool[T], context.Context)` |
| `WithFFSubmitTO(ctx, d)` | FF + 提交超时 | `(*Pool[T], context.Context)` |
| `WithFFSubmitTOTraceID(ctx, d)` | FF + 提交超时 + TraceID | `(*Pool[T], context.Context)` |
| `WithFFTimeout(ctx, d)` | FF + 任务超时 | `(*Pool[T], context.Context)` |
| `WithFFTimeoutTraceID(ctx, d)` | FF + 任务超时 + TraceID | `(*Pool[T], context.Context)` |
| `WithFFTimeoutSubmitTO(ctx, d, d)` | FF + 任务超时 + 提交超时 | `(*Pool[T], context.Context)` |
| `WithFFTimeoutSubmitTOTraceID(ctx, d, d)` | FF + 任务超时 + 提交超时 + TraceID | `(*Pool[T], context.Context)` |
| `WithCtxTraceID(ctx)` | Context + TraceID | `(*Pool[T], context.Context)` |
| `WithCtxTimeout(ctx, d)` | Context + 任务超时 | `(*Pool[T], context.Context)` |
| `WithCtxTimeoutTraceID(ctx, d)` | Context + 任务超时 + TraceID | `(*Pool[T], context.Context)` |
| `WithCtxSubmitTO(ctx, d)` | Context + 提交超时 | `(*Pool[T], context.Context)` |
| `WithCtxSubmitTOTraceID(ctx, d)` | Context + 提交超时 + TraceID | `(*Pool[T], context.Context)` |

```go
// 完整组合示例
p, ffCtx := async.NewPool[string](8).
    WithFFTimeoutSubmitTOTraceID(ctx, 10*time.Second, 3*time.Second)
// 等价于: WithFailFast + WithTimeout(10s) + WithSubmitTimeout(3s) + WithTraceID
```

---

## 结构体类型

### PoolStats

`Pool[T].Stats()` 返回的统计信息结构体，包含池的实时运行状态和历史统计。

```go
type PoolStats struct {
    Size        int           // worker 数量
    Active      int           // 当前活跃任务数
    Busy        int           // 当前忙碌任务数（正在执行 fn）
    Pending     int           // 等待中的任务数
    FailFast    bool          // 是否启用 FailFast
    Timeout     time.Duration // 全局任务超时时间
    TotalTask   int64         // 历史提交任务总数
    SuccessTask int64         // 历史成功任务数
    FailTask    int64         // 历史失败任务数
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `Size` | `int` | 当前 worker 数量 |
| `Active` | `int` | 当前已被 worker 接收的任务数（含正在执行的已出队任务） |
| `Busy` | `int` | 当前正在执行 fn 的任务数 |
| `Pending` | `int` | 排队等待 worker 处理的任务数 |
| `FailFast` | `bool` | 是否启用快速失败模式 |
| `Timeout` | `time.Duration` | 全局任务超时时间 |
| `TotalTask` | `int64` | 自创建以来提交的任务总数 |
| `SuccessTask` | `int64` | 成功完成的任务数 |
| `FailTask` | `int64` | 失败的任务数 |

```go
stats := p.Stats()
fmt.Printf("Worker: %d, 活跃: %d, 执行中: %d, 排队: %d\n",
    stats.Size, stats.Active, stats.Busy, stats.Pending)
fmt.Printf("累计: 总提交=%d, 成功=%d, 失败=%d\n",
    stats.TotalTask, stats.SuccessTask, stats.FailTask)

// 计算成功率
if stats.TotalTask > 0 {
    rate := float64(stats.SuccessTask) / float64(stats.TotalTask) * 100
    fmt.Printf("成功率: %.2f%%\n", rate)
}
```

---

### SubmitResult

`SubmitN`/`SubmitSafeN`/`SubmitBatch` 便捷函数返回的提交结果，记录每个任务的提交状态。

```go
type SubmitResult struct {
    Index int   // 任务在结果切片中的索引位置
    Err   error // 提交错误（如超时、池已关闭等）
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `Index` | `int` | 任务在结果切片中的索引，-1 表示追加到末尾 |
| `Err` | `error` | 提交时的错误，nil 表示提交成功 |

```go
p, results, err := async.SubmitN(ctx, fn, 10)
if err != nil {
    log.Fatal(err)
}
defer p.Close()

for _, r := range results {
    if r.Err != nil {
        log.Printf("提交索引 %d 失败: %v", r.Index, r.Err)
    }
}
```

---

## 状态查询

### Size

返回当前 worker 协程的数量。该值在创建时指定，可通过 `Resize` 动态调整。

```go
// 语法
func (p *Pool[T]) Size() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 当前 worker 数量 |

```go
p := async.NewPool[string](8)
fmt.Println(p.Size()) // 8

p.Resize(16)
fmt.Println(p.Size()) // 16
```

---

### Active

返回当前已被 worker 从 channel 取出（即已出队）的任务数量。活跃任务包括正在执行的和已接收但尚未开始执行的。

```go
// 语法
func (p *Pool[T]) Active() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 当前活跃任务数 |

```go
p.Submit(ctx, fn1)
p.Submit(ctx, fn2)
p.Submit(ctx, fn3)

time.Sleep(10 * time.Millisecond)
fmt.Println(p.Active()) // 3（3 个任务已出队）
```

---

### Busy

返回当前正在执行 fn 函数体的 worker 数量。

```go
// 语法
func (p *Pool[T]) Busy() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 当前忙碌 worker 数 |

```go
// 扩缩容监控：当负载过高时扩容
if float64(p.Busy())/float64(p.Size()) > 0.8 {
    p.Resize(p.Size() * 2)
    log.Printf("扩容至 %d worker", p.Size())
}
```

---

### Pending

返回正在排队等待 worker 处理的任务数（已提交但尚未出队）。

```go
// 语法
func (p *Pool[T]) Pending() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 排队中的任务数 |

```go
// 背压监控
if p.Pending() > 1000 {
    log.Printf("积压严重: %d 个任务等待处理", p.Pending())
}
```

---

### Stats

返回 Pool 的完整运行统计快照，包含实时状态和历史累计数据。详见 [PoolStats 结构体](#poolstats)。

```go
// 语法
func (p *Pool[T]) Stats() PoolStats
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `PoolStats` | `PoolStats` | 完整统计信息 |

```go
stats := p.Stats()
fmt.Printf("Pool Stats: %+v\n", stats)
```

---

### SuccessCount

返回自池创建以来成功完成的任务总数（即 fn 返回 err == nil 的任务）。

```go
// 语法
func (p *Pool[T]) SuccessCount() int64
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | 成功任务数 |

```go
p.Wait()
fmt.Printf("成功: %d, 失败: %d\n", p.SuccessCount(), p.FailCount())
```

---

### FailCount

返回自池创建以来失败的任务总数（即 fn 返回 err != nil 的任务，含 panic 恢复）。

```go
// 语法
func (p *Pool[T]) FailCount() int64
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | 失败任务数 |

```go
if p.FailCount() > 0 {
    log.Printf("有 %d 个任务执行失败", p.FailCount())
}
```

---

### TotalCount

返回自池创建以来提交的任务总数（成功 + 失败）。

```go
// 语法
func (p *Pool[T]) TotalCount() int64
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | 总任务数 |

```go
total := p.TotalCount()
fmt.Printf("已处理 %d 个任务\n", total)
```

---

### HasError

返回是否有任务执行失败。建议在 `Wait()` 之后调用以获取完整结果。

```go
// 语法
func (p *Pool[T]) HasError() bool
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `bool` | `bool` | true 表示至少有一个任务失败 |

```go
p.Wait()
if p.HasError() {
    log.Printf("存在失败任务，第一个错误: %v", p.FirstError())
}
```

---

### QueueDepth

返回当前排队深度，等价于 `Pending()`。用于监控和背压控制。

```go
// 语法
func (p *Pool[T]) QueueDepth() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 排队深度 |

```go
// 在 WithMaxPending 配置后，监控排队深度
depth := p.QueueDepth()
if depth > p.Size()*2 {
    log.Printf("排队深度异常: %d (worker: %d)", depth, p.Size())
}
```

---

## 错误提取

### Errors

返回所有失败任务的错误切片（nil error 被跳过）。建议在 `Wait()` 之后调用以获取完整结果。返回 nil 表示所有任务成功。

```go
// 语法
func (p *Pool[T]) Errors() []error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]error` | `[]error` | 所有非 nil 错误的切片 |

```go
p.Wait()
errs := p.Errors()
if len(errs) == 0 {
    fmt.Println("所有任务成功完成")
    return
}
for i, err := range errs {
    log.Printf("错误 #%d: %v", i, err)
}
```

---

### FirstError

返回第一个错误（按任务索引顺序）。无错误时返回 nil。建议在 `Wait()` 之后调用。

```go
// 语法
func (p *Pool[T]) FirstError() error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 第一个错误，无错误时为 nil |

```go
p.Wait()
if firstErr := p.FirstError(); firstErr != nil {
    log.Printf("首个失败: %v", firstErr)
}
```

---

### JoinErrors

将所有错误合并为一个 error，以 `"; "` 分隔每个错误信息。如果所有任务成功则返回 nil。

```go
// 语法
func (p *Pool[T]) JoinErrors() error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 合并后的错误，所有成功时为 nil |

```go
p.Wait()
if joinedErr := p.JoinErrors(); joinedErr != nil {
    log.Printf("批量错误: %v", joinedErr)
    // 输出: "task1 failed: timeout; task3 failed: connection refused"
}
```

---

### Values

返回所有成功任务的返回值切片（失败任务不包含在内）。建议在 `Wait()` 之后调用。

```go
// 语法
func (p *Pool[T]) Values() []T
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]T` | `[]T` | 所有成功任务的返回值 |

```go
p.Wait()
values := p.Values()
fmt.Printf("成功获取 %d 个结果\n", len(values))
for i, v := range values {
    fmt.Printf("结果 #%d: %v\n", i, v)
}
```

---

## 自动扩缩容

### EnableAutoScale

```go
// 语法
func (p *Pool[T]) EnableAutoScale(cfg *AutoScaleConfig)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `cfg` | `*AutoScaleConfig` | 扩缩容配置，nil 使用默认配置 |

```go
p := async.NewPool[int](4)
p.EnableAutoScale(nil) // 使用默认配置

p.EnableAutoScale(&async.AutoScaleConfig{
    MinWorkers:       2,
    MaxWorkers:       500,
    CheckInterval:    3 * time.Second,
    ScaleUpThreshold: 0.6,
    ScaleUpFactor:    1.5,
    ScaleDownFactor:  0.7,
})
```

### IsAutoScaleEnabled

```go
// 语法
func (p *Pool[T]) IsAutoScaleEnabled() bool
```

查询自动扩缩容是否已启用。

### DisableAutoScale

```go
// 语法
func (p *Pool[T]) DisableAutoScale()
```

停止自动扩缩容。

---

## 流式结果消费

### WithStreaming

```go
// 语法
func (p *Pool[T]) WithStreaming(bufSize int) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `bufSize` | `int` | channel 缓冲大小，<=0 时自动使用 `Size()*2` |

开启流式结果消费，任务完成时结果实时通过 channel 发送。

```go
p := async.NewPool[string](8).WithStreaming(0)
defer p.Close()

ch := p.StreamResults()
go func() {
    for r := range ch {
        if r.Ok() {
            fmt.Println("实时收到:", r.Value)
        } else {
            log.Println("失败:", r.Err)
        }
    }
}()

for _, item := range items {
    p.Submit(ctx, func(ctx context.Context) (string, error) {
        return process(ctx, item), nil
    })
}
p.Wait() // channel 在 Wait 完成后自动关闭
```

### StreamResults

```go
// 语法
func (p *Pool[T]) StreamResults() <-chan core.Result[T]
```

返回流式结果的只读 channel，必须在 `WithStreaming` 之后调用。未启用流式时返回 nil。channel 在 Wait 完成后自动关闭。

### StreamDropped

```go
// 语法
func (p *Pool[T]) StreamDropped() int64
```

返回因 stream channel 满而被丢弃的结果数。如果此值持续增长，说明消费者速度跟不上生产者。

```go
go func() {
    ticker := time.NewTicker(10 * time.Second)
    for range ticker.C {
        dropped := p.StreamDropped()
        if dropped > 0 {
            log.Warn("stream drop", dropped)
        }
    }
}()
```

### WithResultCallback

```go
// 语法
func (p *Pool[T]) WithResultCallback(fn func(core.Result[T])) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func(core.Result[T])` | 每个任务完成时调用的回调（在 worker goroutine 中执行，应尽量轻量） |

```go
var successCnt atomic.Int64

p := async.NewPool[string](8).WithResultCallback(func(r core.Result[string]) {
    if r.Ok() {
        successCnt.Add(1)
    }
})
```

---

## 环形缓冲

### WithRingBuffer

```go
// 语法
func (p *Pool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `capacity` | `int` | 缓冲容量 |
| `overflow` | `core.OverflowStrategy` | 满时策略：`OverflowDrop` / `OverflowBlock` / `OverflowError` |

用固定容量环形缓冲替代无限增长的 results 切片，适合千万级任务量。

```go
p := async.NewPool[string](8).WithRingBuffer(10000, async.OverflowDrop)
defer p.Close()

for i := 0; i < 10_000_000; i++ {
    p.Submit(ctx, func(ctx context.Context) (string, error) {
        return heavyWork(ctx), nil
    })
}
```

### Flush

```go
// 语法
func (p *Pool[T]) Flush(maxCount int) []core.Result[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `maxCount` | `int` | 最大取出数量，<=0 取出全部 |
| 返回 | `[]core.Result[T]` | 取出的结果切片 |

从环形缓冲中取出结果。未启用环形缓冲时返回 nil。

```go
for {
    batch := p.Flush(5000)
    if len(batch) == 0 { break }
    consume(batch)
}
remaining := p.Wait()
```

### RingBufDropped

```go
// 语法
func (p *Pool[T]) RingBufDropped() int64
```

返回环形缓冲区因 `OverflowDrop` 覆盖丢弃的元素数。

---

## 背压控制

### WithMaxPending

```go
// 语法
func (p *Pool[T]) WithMaxPending(maxPending int) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `maxPending` | `int` | 最大等待任务数，<=0 无限制 |

### WithOverflow

```go
// 语法
func (p *Pool[T]) WithOverflow(strategy core.OverflowStrategy) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `strategy` | `core.OverflowStrategy` | `OverflowBlock` / `OverflowDrop` / `OverflowError` |

### WithMaxResults

```go
// 语法
func (p *Pool[T]) WithMaxResults(maxResults int) *Pool[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `maxResults` | `int` | results 切片容量上限，0=无限 |

限制 results 切片的内存增长。超出上限后，新结果仅通过流式或环形缓冲区消费。

```go
p := async.NewPool[string](8).
    WithMaxPending(1000).
    WithOverflow(async.OverflowError).
    WithMaxResults(1_000_000)
defer p.Close()

for _, item := range items {
    err := p.Submit(ctx, fn)
    if errors.Is(err, async.ErrQueueOverflow) {
        fallbackProcess(item)
        continue
    }
}

depth := p.QueueDepth()
```

### Resize

```go
// 语法
func (p *Pool[T]) Resize(newSize int) int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `newSize` | `int` | 新 worker 数量 |
| 返回 | `int` | 实际设置的 worker 数 |

调整 worker 数量。扩容立即创建新 worker，缩容通过发送 Quit 信号逐步退出。

```go
newSize := p.Resize(50) // 调整为 50 worker
```

---

### ResizeAndWaitTimeout

```go
// 语法
func (p *Pool[T]) ResizeAndWaitTimeout(newSize int, timeout time.Duration)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `newSize` | `int` | 新 worker 数量 |
| `timeout` | `time.Duration` | 等待正在执行的任务完成的最长时间 |

调整 worker 数量后等待 current 任务完成。先调用 `Resize()` 缩容，然后等待 `sync.WaitGroup` 完成，但有超时保护。

**与 `Resize` 的区别**：`Resize` 立即返回，不等待现有任务。`ResizeAndWaitTimeout` 在 Resize 后阻塞等待现有任务完成（最多等待 timeout 时间）。

**使用示例**:

```go
// 缩容到 5 个 worker，最多等待 10 秒让正在执行的任务完成
p.ResizeAndWaitTimeout(5, 10*time.Second)
```

---

## NoResultPool 辅助函数

这些便捷函数封装了 `NoResultPool`（即 `Pool[struct{}]`）的常见操作模式。

### SubmitAction

```go
// 语法
func SubmitAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `p` | `*NoResultPool` | 无返回值协程池 |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) error` | 任务函数 |
| 返回 | `error` | 提交错误 |

阻塞提交无返回值动作。

```go
async.SubmitAction(p, ctx, func(ctx context.Context) error {
    return processItem(ctx)
})
```

### TrySubmitAction

```go
// 语法
func TrySubmitAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error
```

非阻塞提交无返回值动作，worker 满时返回 `ErrSubmitTimeout`。

### SubmitAtAction

```go
// 语法
func SubmitAtAction(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `index` | `int` | 结果位置索引 |

指定位置提交无返回值动作。

### TrySubmitAtAction

```go
// 语法
func TrySubmitAtAction(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error
```

指定位置非阻塞提交。

### GoAction

```go
// 语法
func GoAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error)
```

提交并断言成功（失败则 panic），适合初始化阶段。

### SubmitActionWithTimeout

```go
// 语法
func SubmitActionWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeout` | `time.Duration` | 任务超时 |

带超时提交无返回值动作。

### SubmitAtActionWithTimeout

```go
// 语法
func SubmitAtActionWithTimeout(p *NoResultPool, index int, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error
```

指定位置带超时提交。

### GoActionWithTimeout

```go
// 语法
func GoActionWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error)
```

带超时提交并断言成功。

### BuildAggregateNoResult

```go
// 语法
func BuildAggregateNoResult(nr *NoResultPool) (*core.AggregateNoResult, error)
```

从 NoResultPool 构建聚合统计信息。

### FillNoResultSkipped

```go
// 语法
func FillNoResultSkipped(nr *NoResultPool, total int) error
```

为 NoResultPool 填充跳过任务的占位（FailFast 被跳过的任务）。

---

## Pool 便捷函数

快速创建池并提交任务的单次操作。

### Submit (便捷)

```go
// 语法
func Submit[T any](ctx context.Context, fn func(context.Context) (T, error)) (*Pool[T], int, error)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回1 | `*Pool[T]` | 创建的协程池（需 Close） |
| 返回2 | `int` | 任务在结果中的索引 |
| 返回3 | `error` | 提交错误 |

```go
p, idx, err := async.Submit(ctx, fn)
defer p.Close()
results := p.Wait()
```

### SubmitN

```go
// 语法
func SubmitN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult, error)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `n` | `int` | 任务数量 |

提交 N 个相同任务。

```go
p, submitResults, err := async.SubmitN(ctx, fn, 100)
defer p.Close()
```

### SubmitSafeN

```go
// 语法
func SubmitSafeN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult)
```

提交 N 个任务（失败 panic）。

```go
p, submitResults := async.SubmitSafeN(ctx, fn, 50)
defer p.Close()
```

### SubmitBatch

```go
// 语法
func SubmitBatch[T, S any](ctx context.Context, items []S, fn func(context.Context, S) (T, error)) (*Pool[T], []SubmitResult, error)
```

对切片批量提交。

```go
users := []string{"alice", "bob", "charlie"}
p, results, err := async.SubmitBatch(ctx, users, func(ctx context.Context, name string) (*User, error) {
    return db.QueryUser(ctx, name)
})
defer p.Close()
```

### MapPool

```go
// 语法
func MapPool[T, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) (*Pool[R], []core.Result[R], error)
```

Pool 版 Map。

### ForEachPool

```go
// 语法
func ForEachPool[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (*NoResultPool, error)
```

Pool 版 ForEach。

---

## MultiPool（水平分片协程池）

`MultiPool[T]` 通过将 N 个 Pool 实例组合在一起，以 round-robin 方式分发任务，实现水平扩展。每个分片池独立运行，互不影响，可突破单 Pool 的 channel 瓶颈（单 Pool 约 37万 QPS，8 分片可达 ~300万 QPS）。

**核心特性**：
- Round-robin 分发（利用 `submitIdx` 原子递增）
- Keyed 分发（按 key 哈希固定分片）
- 批量提交（SubmitBatch）
- 所有链式配置代理到各分片
- 统计聚合（汇总所有分片数据）

> **⚠️ 分片结果不保证全局顺序**
>
> `MultiPool.Wait()` 依次合并各分片的结果，分片 A 的全部结果在分片 B 之前。但分片之间并未按时间排序，因此不保证全局提交顺序。
>
> **⚠️ 通过 Pool.Shard() 创建，不要直接 New**
>
> `MultiPool` 没有公开的构造函数，必须通过 `Pool.Shard()` 或 `Pool.DefaultShard()` 创建。第一个分片复用原 Pool，其余自动克隆配置。
>
> **⚠️ 分片数不能超过 CPU 核心数的 16 倍**
>
> `shards > runtime.GOMAXPROCS(0) * 16` 时返回只有当前 Pool 的单分片 MultiPool。过多分片会增加调度开销，适得其反。

### 创建：Shard / DefaultShard

```go
// 方法签名
func (p *Pool[T]) Shard(shards int) *MultiPool[T]
func (p *Pool[T]) DefaultShard() *MultiPool[T]
```

| 方法 | 说明 |
|------|------|
| `p.Shard(n)` | 创建 n 个分片的 MultiPool，`n <= 1` 返回单分片 |
| `p.DefaultShard()` | 使用 `max(2, GOMAXPROCS)` 作为分片数 |

```go
// 基本用法
mp := async.NewPool[int](100).Shard(8)
defer mp.Close()

// 配合自动扩缩容：8 分片 × 每个 4 worker（32 worker 初始）→ 按需扩缩
mp := async.NewPool[int](4).
    WithMaxPending(10000).
    Shard(8)
defer mp.Close()

// 配合背压控制：16 分片，每个 50 worker，溢出丢弃
mp := async.NewPool[int](50).
    WithMaxPending(5000).
    WithOverflow(core.OverflowDrop).
    Shard(16)
defer mp.Close()

// CPU 自动分片
mp := async.NewPool[int](100).DefaultShard()
```

### 基础查询：ShardCount / GetShard

```go
func (mp *MultiPool[T]) ShardCount() int
func (mp *MultiPool[T]) GetShard(idx int) *Pool[T]
```

| 方法 | 说明 |
|------|------|
| `ShardCount()` | 返回分片数 |
| `GetShard(i)` | 获取第 i 个分片的 Pool 实例，可对其单独配置 AutoScale 等 |

```go
mp := pool.NewPool[int](4).
    WithMaxPending(5000).
    Shard(4)

// 为每个分片单独开启 AutoScale
for i := 0; i < mp.ShardCount(); i++ {
    mp.GetShard(i).EnableAutoScale(nil)
}
```

### 任务分发

```go
func (mp *MultiPool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error
func (mp *MultiPool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error
func (mp *MultiPool[T]) SubmitKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error
func (mp *MultiPool[T]) TrySubmitKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error
func (mp *MultiPool[T]) SubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) []SubmitResult
```

| 方法 | 分发策略 | 说明 |
|------|----------|------|
| `Submit` | Round-robin | 阻塞提交 |
| `TrySubmit` | Round-robin | 非阻塞提交 |
| `SubmitKeyed` | Hash(key) | 同一 key 固定分片，阻塞 |
| `TrySubmitKeyed` | Hash(key) | 同一 key 固定分片，非阻塞 |
| `SubmitBatch` | 逐元素 Round-robin | 批量提交，返回 `[]SubmitResult` |

```go
mp := async.NewPool[int](100).Shard(8)
defer mp.Close()

// Round-robin 分发
for i := 0; i < 10_000_000; i++ {
    mp.Submit(ctx, func(ctx context.Context) (int, error) {
        return i * 2, nil
    })
}
results := mp.Wait()

// Keyed 分发：相同 key 落到同一分片（保证顺序）
for _, userID := range userIDs {
    mp.SubmitKeyed(uint64(userID), ctx, func(ctx context.Context) (int, error) {
        return processUser(ctx, userID)
    })
}

// 批量提交
items := []int{1, 2, 3, 4, 5}
submitResults := mp.SubmitBatch(ctx, items, func(ctx context.Context, n int) (int, error) {
    return n * n, nil
})
for _, sr := range submitResults {
    if sr.Err != nil {
        log.Printf("submit index %d failed: %v", sr.Index, sr.Err)
    }
}
```

### 结果收集：Wait / WaitAndClose / Close

```go
func (mp *MultiPool[T]) Wait() []core.Result[T]
func (mp *MultiPool[T]) WaitAndClose() []core.Result[T]
func (mp *MultiPool[T]) Close()
```

| 方法 | 说明 |
|------|------|
| `Wait()` | 等待所有分片完成，合并结果 |
| `WaitAndClose()` | 等待完成 + 关闭所有分片 |
| `Close()` | 立即关闭所有分片（不等待） |

```go
mp := async.NewPool[int](100).Shard(8)

// 方式一：先 Wait 再 Close
results := mp.Wait()
mp.Close()

// 方式二：一步完成（推荐）
results := mp.WaitAndClose()
```

### 链式配置

MultiPool 的所有 With* 方法代理到内部每个分片：

```go
func (mp *MultiPool[T]) WithTimeout(d time.Duration) *MultiPool[T]
func (mp *MultiPool[T]) WithSubmitTimeout(d time.Duration) *MultiPool[T]
func (mp *MultiPool[T]) WithStreaming(bufSize int) *MultiPool[T]
func (mp *MultiPool[T]) WithResultCallback(fn func(core.Result[T])) *MultiPool[T]
func (mp *MultiPool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *MultiPool[T]
func (mp *MultiPool[T]) WithMaxPending(n int) *MultiPool[T]
func (mp *MultiPool[T]) WithOverflow(strategy core.OverflowStrategy) *MultiPool[T]
func (mp *MultiPool[T]) WithMaxResults(n int) *MultiPool[T]
```

| 方法 | 说明 |
|------|------|
| `WithTimeout(d)` | 为所有分片设置任务超时 |
| `WithSubmitTimeout(d)` | 为所有分片设置提交超时 |
| `WithStreaming(n)` | 为所有分片启用流式结果消费（各分片 channel 独立） |
| `WithResultCallback(fn)` | 为所有分片设置结果回调 |
| `WithRingBuffer(cap, over)` | 为所有分片启用环形缓冲 |
| `WithMaxPending(n)` | 为所有分片设置最大等待数（背压控制） |
| `WithOverflow(strat)` | 为所有分片设置溢出策略 |
| `WithMaxResults(n)` | 为所有分片设置最大结果数 |

```go
mp := async.NewPool[int](100).Shard(8).
    WithTimeout(5 * time.Second).
    WithMaxPending(10000).
    WithOverflow(core.OverflowDrop)
defer mp.Close()
```

> **⚠️ WithStreaming 各分片独立**
>
> 启用流式后，每个分片有独立的 channel。如需统一消费，推荐使用 `WithResultCallback`。

### 统计聚合

```go
func (mp *MultiPool[T]) TotalActive() int
func (mp *MultiPool[T]) TotalBusy() int
func (mp *MultiPool[T]) TotalPending() int
func (mp *MultiPool[T]) TotalWorkerCount() int
func (mp *MultiPool[T]) TotalFailCount() int64
func (mp *MultiPool[T]) TotalSuccessCount() int64
func (mp *MultiPool[T]) TotalCount() int64
func (mp *MultiPool[T]) Flush(maxPerShard int) []core.Result[T]
```

| 方法 | 说明 |
|------|------|
| `TotalActive()` | 所有分片活跃任务总数 |
| `TotalBusy()` | 所有分片忙碌 worker 总数 |
| `TotalPending()` | 所有分片等待队列总长度 |
| `TotalWorkerCount()` | 所有分片 worker 总数 |
| `TotalFailCount()` | 所有分片失败任务总数 |
| `TotalSuccessCount()` | 所有分片成功任务总数 |
| `TotalCount()` | 所有分片任务总数 |
| `Flush(maxPerShard)` | 排空所有分片环形缓冲区结果 |

```go
mp := async.NewPool[int](100).Shard(8)
defer mp.Close()

// 提交任务...
for i := 0; i < 100000; i++ {
    mp.Submit(ctx, fn)
}

// 监控分片状态
fmt.Printf("workers: %d, active: %d, busy: %d, pending: %d\n",
    mp.TotalWorkerCount(), mp.TotalActive(), mp.TotalBusy(), mp.TotalPending())

// 排空环形缓冲区
results := mp.Flush(1000)
```

---

## 架构说明

Pool 内部使用 **32 分片无锁存储**：

```
┌────────────────────────────────────────────┐
│                  Pool[T]                    │
│                                             │
│  submitIdx (atomic) ──→ 分片路由            │
│       │                                     │
│  ┌────┴────┬─────────┬─────────┬─────────┐ │
│  │ Shard 0 │ Shard 1 │  ...    │ Shard 31│ │
│  │ mu      │ mu      │         │ mu      │ │
│  │ results │ results │         │ results │ │
│  │ cancels │ cancels │         │ cancels │ │
│  └─────────┴─────────┴─────────┴─────────┘ │
│                                             │
│  Worker[0] Worker[1] ... Worker[N-1]        │
│     ↑ taskCh (buffered channel)              │
└────────────────────────────────────────────┘
```

- **分片路由**：`shardIdx = submitIdx % 32`，`localIdx = submitIdx / 32`
- **原子序号**：`submitIdx` 用 `atomic.Int64` 递增，无需全局锁
- **分片锁**：每个分片独立 `sync.Mutex`，锁竞争降至 1/32
- **结果顺序**：按 `submitIdx` 确定全局顺序，Wait 时遍历合并

这种设计消除了全局锁竞争，在千万级并发下保持线性吞吐。

---

## 性能基准

| 场景 | 吞吐量 | 说明 |
|------|--------|------|
| Pool.Submit + Wait 10M | **370K ops/s** | 千万任务提交+等待，32分片 |
| AutoScalePool 1M | **117K ops/s** | 自动扩缩容 |
| AutoScalePool TrySubmit 5M | **943K ops/s** | 非阻塞提交+自动扩缩容 |