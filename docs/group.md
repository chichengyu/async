# Group（任务组）文档

## 概述

`Group[T]` 是一次性批量并发任务组，每次 `Go()` 新建 goroutine，用完即销毁。适合数据迁移、批量 API 调用等一次性任务。

**核心特性**:
- 固定并发度控制（信号量模式）
- 自动扩缩容（可配置扩缩因子）
- 流式结果消费
- FailFast 快速失败
- NoResult 无返回值模式
- 丰富的 With* 链式配置组合方法

> **⚠️ Go 后必须 Wait**
>
> 每调用一次 `Group.Go()` 就会创建一个 goroutine，必须调用 `Wait()` 等待全部完成并收集结果，否则 goroutine 泄漏。
>
> **⚠️ Wait 一次性**
>
> `Wait()` 只能调用一次，调用后不可再 `Go()`。如需复用，请使用 `Reset()` 重置 Group。
>
> **⚠️ goroutine 不复用**
>
> 与 Pool 不同，Group 不会复用 goroutine。每个 `Go()` 启动一个新 goroutine，适合几千到几万量级的批量任务，不适合百万级高频提交。
>
> **⚠️ Concurrency 控制**
>
> 并发度限制的是同时运行的 goroutine 数，超出部分会阻塞等待，不是排队丢弃。

**与 Pool 的区别**:

| 特性 | Pool | Group |
|------|------|------|
| goroutine | 复用（常驻 worker） | 每任务新建 |
| 适用场景 | 长期运行的服务 | 一次性批量任务 |
| 生命周期 | 需手动 Close | 用完自动释放 |
| 自动扩缩容 | `EnableAutoScale()` | `EnableAutoScale()` |
| 流式消费 | ✅ | ✅ |

## 目录

- [创建](#创建)
  - [NewGroup / DefaultGroup](#newgroup)
  - [NewNoResult / DefaultNoResult](#newnoresult--defaultnoresult)
- [任务提交](#任务提交)
  - [Go / GoWithTimeout](#go)
  - [GoAt / GoAtWithTimeout](#goat)
- [等待与结果](#等待与结果)
  - [Wait / WaitTimeout / WaitContext](#wait)
- [结构体类型](#结构体类型)
  - [GroupStats](#groupstats)
- [状态查询](#状态查询)
  - [Stats / Concurrency / Active / Busy](#stats)
  - [SuccessCount / FailCount / TotalCount / HasError](#successcount)
- [错误提取](#错误提取)
  - [Errors / FirstError / JoinErrors / Values](#errors)
- [超时与上下文](#超时与上下文)
  - [WithTimeout / WithSubmitTimeout](#withtimeout)
  - [WithContext / WithFailFast / WithFFCtx](#withcontext)
- [With* 组合方法速查](#with-组合方法速查)
- [自动扩缩容](#自动扩缩容)
  - [EnableAutoScale / IsAutoScaleEnabled / DisableAutoScale](#enableautoscale)
- [流式结果消费](#流式结果消费)
  - [WithStreaming / StreamResults / WithResultCallback](#withstreaming)
- [Reset 重置](#reset-重置)
- [NoResult 完整方法](#noresult-完整方法)
  - [任务提交 / 等待 / 状态查询 / 错误提取 / 配置](#noresult-完整方法)
- [NoResult 辅助函数](#noresult-辅助函数)
  - [BuildAggregateNoResult / FillNoResultSkipped](#buildaggregatenoresult)
- [MultiGroup（水平分片任务组）](#multigroup水平分片任务组)
  - [创建：Shard / DefaultShard](#创建shard--defaultshard)
  - [基础查询：ShardCount / GetShard](#基础查询shardcount--getshard)
  - [任务分发：Go / GoKeyed](#任务分发go--gokeyed)
  - [结果收集：Wait / Close](#结果收集wait--close)
  - [链式配置：WithTimeout](#链式配置withtimeout)
  - [统计聚合：TotalActive / TotalBusy / TotalConcurrency](#统计聚合totalactive--totalbusy--totalconcurrency)
  - [统计聚合：TotalTaskCount / TotalFailCount / TotalSuccessCount](#统计聚合totaltaskcount--totalfailcount--totalsuccesscount)
  - [错误/值提取：Errors / Values](#错误值提取errors--values)
- [性能基准](#性能基准)

## 创建

### NewGroup

```go
// 语法
func NewGroup[T any](concurrency int) *Group[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `concurrency` | `int` | 最大并发数，<=0 时默认 1 |

```go
g := async.NewGroup[int](8)        // 最多 8 个任务并发
g := async.NewGroup[string](0)     // 默认 1 并发
```

### DefaultGroup

```go
// 语法
func DefaultGroup[T any]() *Group[T]
```

等价于 `NewGroup[T](core.IO())`，使用 IO 并发度。

```go
g := async.DefaultGroup[int]()
```

### NewNoResult / DefaultNoResult

```go
// 语法
func NewNoResult(concurrency int) *NoResult    // NoResult = Group[struct{}]
func DefaultNoResult() *NoResult
```

无返回值任务组，适合只关心 error 的批量操作。

```go
nr := async.NewNoResult(16)

err := nr.Go(ctx, func(ctx context.Context) error {
    return db.Insert(ctx, record)
})

nr.Wait()
success := nr.SuccessCount()
fail := nr.FailCount()
```

---

## 任务提交

### Go

```go
// 语法
func (g *Group[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回 | `error` | ErrGroupWaited / ErrGroupWaiting / ErrSubmitTimeout |

阻塞提交任务，直到有空闲并发槽位。

```go
err := g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchCount(ctx), nil
})
```

### GoWithTimeout

```go
// 语法
func (g *Group[T]) GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeout` | `time.Duration` | 提交超时 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |

带提交超时的任务提交。

```go
err := g.GoWithTimeout(ctx, 5*time.Second, fn)
```

### GoAt

```go
// 语法
func (g *Group[T]) GoAt(index int, ctx context.Context, fn func(context.Context) (T, error)) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `index` | `int` | 结果在 `Wait()` 返回切片中的位置 |

提交到指定索引位置，结果保持索引顺序。

```go
for i, url := range urls {
    g.GoAt(i, ctx, func(ctx context.Context) (string, error) {
        return fetchURL(ctx, url), nil
    })
}
// results[i] 对应 urls[i]
```

### GoAtWithTimeout

```go
// 语法
func (g *Group[T]) GoAtWithTimeout(index int, ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) error
```

指定索引位置 + 带提交超时。

```go
err := g.GoAtWithTimeout(3, ctx, 5*time.Second, fn)
```

---

## 等待与结果

### Wait

```go
// 语法
func (g *Group[T]) Wait() []core.Result[T]
```

等待所有任务完成，返回结果切片（按提交顺序）。调用后不能再提交新任务。

```go
results := g.Wait()
for _, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

### WaitTimeout

```go
// 语法
func (g *Group[T]) WaitTimeout(timeout time.Duration) (results []core.Result[T], ok bool)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeout` | `time.Duration` | 等待超时 |
| 返回值1 | `[]core.Result[T]` | 已完成任务的结果 |
| 返回值2 | `bool` | 是否所有任务完成 |

```go
results, ok := g.WaitTimeout(5 * time.Second)
if !ok {
    log.Println("部分任务未完成")
}
```

### WaitContext

```go
// 语法
func (g *Group[T]) WaitContext(ctx context.Context) (results []core.Result[T], ok bool)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 控制等待的 context |

通过 context 控制等待。

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
results, ok := g.WaitContext(ctx)
```

---

## 结构体类型

### GroupStats

`Group[T].Stats()` 返回的统计信息结构体，包含 Group 的运行时状态和历史统计。

```go
type GroupStats struct {
    Concurrency int           // 当前并发度
    Active      int           // 当前活跃任务数
    Busy        int           // 当前忙碌任务数（正在执行 fn）
    FailFast    bool          // 是否启用 FailFast
    Timeout     time.Duration // 全局任务超时时间
    TotalTask   int64         // 历史提交任务总数
    SuccessTask int64         // 历史成功任务数
    FailTask    int64         // 历史失败任务数
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `Concurrency` | `int` | 当前并发度（可能因扩缩容而变化） |
| `Active` | `int` | 当前已被 consumer 接收的任务数 |
| `Busy` | `int` | 当前正在执行 fn 函数的任务数 |
| `FailFast` | `bool` | 是否启用快速失败模式 |
| `Timeout` | `time.Duration` | 每个任务的最大执行超时 |
| `TotalTask` | `int64` | 自创建以来通过 Go 提交的任务总数 |
| `SuccessTask` | `int64` | 成功完成的任务数 |
| `FailTask` | `int64` | 失败的任务数（含 panic 恢复） |

```go
stats := g.Stats()
fmt.Printf("并发度: %d, 活跃: %d, 执行中: %d\n",
    stats.Concurrency, stats.Active, stats.Busy)
fmt.Printf("累计: 总提交=%d, 成功=%d, 失败=%d\n",
    stats.TotalTask, stats.SuccessTask, stats.FailTask)

// 计算成功率
if stats.TotalTask > 0 {
    rate := float64(stats.SuccessTask) / float64(stats.TotalTask) * 100
    fmt.Printf("成功率: %.2f%%\n", rate)
}
```

---

## 状态查询

### Stats

返回 Group 的完整运行统计快照，包含实时状态和历史累计数据。详见 [GroupStats 结构体](#groupstats)。

```go
// 语法
func (g *Group[T]) Stats() GroupStats
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `GroupStats` | `GroupStats` | 完整统计信息 |

```go
stats := g.Stats()
fmt.Printf("Group 统计: %+v\n", stats)
```

---

### Concurrency

返回当前并发度（协程数），该值可能在启用自动扩缩容后动态变化。

```go
// 语法
func (g *Group[T]) Concurrency() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 当前并发度 |

```go
g := async.NewGroup[int](8)
fmt.Println(g.Concurrency()) // 8
```

---

### Active

返回当前已被 consumer 协程接收（即已出队）的任务数量。

```go
// 语法
func (g *Group[T]) Active() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 当前活跃任务数 |

```go
g.Go(ctx, fn1)
g.Go(ctx, fn2)
g.Go(ctx, fn3)

time.Sleep(10 * time.Millisecond)
fmt.Println(g.Active()) // 3（3 个任务已出队）
```

---

### Busy

返回当前正在执行 fn 函数体的协程数量。可用于判断是否需要扩容。

```go
// 语法
func (g *Group[T]) Busy() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 当前忙碌协程数 |

```go
// 扩容监控
if g.EnableAutoScale(nil); float64(g.Busy())/float64(g.Concurrency()) > 0.8 {
    log.Printf("负载较高: %d/%d busy", g.Busy(), g.Concurrency())
}
```

---

### SuccessCount

返回自创建以来成功完成的任务总数（fn 返回 err == nil）。

```go
// 语法
func (g *Group[T]) SuccessCount() int64
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | 成功任务数 |

```go
g.Wait()
fmt.Printf("成功: %d, 失败: %d\n", g.SuccessCount(), g.FailCount())
```

---

### FailCount

返回自创建以来失败的任务总数（fn 返回 err != nil 或 panic）。

```go
// 语法
func (g *Group[T]) FailCount() int64
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | 失败任务数 |

```go
if g.FailCount() > 0 {
    log.Printf("有 %d 个任务执行失败", g.FailCount())
}
```

---

### TotalCount

返回自创建以来提交的任务总数（成功 + 失败）。

```go
// 语法
func (g *Group[T]) TotalCount() int64
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | 总任务数 |

```go
total := g.TotalCount()
fmt.Printf("Group 已处理 %d 个任务\n", total)
```

---

### HasError

返回是否有任务执行失败。建议在 `Wait()` 之后调用。

```go
// 语法
func (g *Group[T]) HasError() bool
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `bool` | `bool` | true 表示至少有一个任务失败 |

```go
g.Wait()
if g.HasError() {
    log.Printf("存在失败任务，第一个错误: %v", g.FirstError())
}
```

---

## 错误提取

### Errors

返回所有失败任务的错误切片（nil error 被跳过）。建议在 `Wait()` 之后调用。

```go
// 语法
func (g *Group[T]) Errors() []error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]error` | `[]error` | 所有非 nil 错误的切片，全成功时返回 nil / 空切片 |

```go
g.Wait()
errs := g.Errors()
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

返回第一个错误（按 Go 提交顺序）。无错误时返回 nil。建议在 `Wait()` 之后调用。

```go
// 语法
func (g *Group[T]) FirstError() error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 第一个错误，无错误时为 nil |

```go
g.Wait()
if firstErr := g.FirstError(); firstErr != nil {
    log.Printf("Group 首个失败: %v", firstErr)
}
```

---

### JoinErrors

将所有错误合并为一个 error，以 `"; "` 分隔。所有任务成功时返回 nil。

```go
// 语法
func (g *Group[T]) JoinErrors() error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 合并后的错误，全成功时为 nil |

```go
g.Wait()
if joinedErr := g.JoinErrors(); joinedErr != nil {
    log.Printf("批量错误: %v", joinedErr)
    // 输出: "task1: timeout; task3: connection refused"
}
```

---

### Values

返回所有成功任务的返回值切片（失败任务的零值不包含在内）。建议在 `Wait()` 之后调用。

```go
// 语法
func (g *Group[T]) Values() []T
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]T` | `[]T` | 所有成功任务的返回值 |

```go
g.Wait()
values := g.Values()
fmt.Printf("成功获取 %d 个结果\n", len(values))
for i, v := range values {
    fmt.Printf("结果 #%d: %v\n", i, v)
}
```

---

## 超时与上下文

### WithTimeout

```go
// 语法
func (g *Group[T]) WithTimeout(d time.Duration) *Group[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 每个任务的超时时间 |

```go
g := async.NewGroup[int](8).WithTimeout(30 * time.Second)
```

### WithSubmitTimeout

```go
// 语法
func (g *Group[T]) WithSubmitTimeout(d time.Duration) *Group[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 提交超时时间 |

### WithContext

```go
// 语法
func (g *Group[T]) WithContext(ctx context.Context) (*Group[T], context.Context)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 原始上下文 |
| 返回 | `(*Group[T], context.Context)` | 新 Group + 可取消的子 context |

创建绑定到 Group 生命周期的子 context。Group Wait/Reset 时自动取消。

```go
g, boundCtx := g.WithContext(ctx)
// boundCtx 在 Group Wait/Reset 时自动取消
go func() {
    <-boundCtx.Done()
    log.Println("Group 已完成或重置")
}()
```

### WithFailFast

```go
// 语法
func (g *Group[T]) WithFailFast(ctx context.Context) (*Group[T], context.Context)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 原始上下文 |
| 返回 | `(*Group[T], context.Context)` | Group + 可取消的子 context |

启用 FailFast 模式：第一个任务失败立即取消所有其他任务。

```go
g, ffCtx := g.WithFailFast(ctx)
for _, item := range items {
    g.Go(ffCtx, func(ctx context.Context) (int, error) {
        return validate(ctx, item)
    })
}
```

### WithFFCtx

```go
// 语法
func (g *Group[T]) WithFFCtx(ctx context.Context) (*Group[T], context.Context)
```

`WithFailFast` 的缩写形式。

---

## With* 组合方法速查

| 方法 | 说明 | 签名 |
|------|------|------|
| `WithTraceID(ctx)` | 注入 TraceID | `(*Group[T], context.Context)` |
| `WithContext(ctx)` | Context 绑定 | `(*Group[T], context.Context)` |
| `WithFailFast(ctx)` | 快速失败 | `(*Group[T], context.Context)` |
| `WithFFCtx(ctx)` | FailFast 缩写 | `(*Group[T], context.Context)` |
| `WithTimeout(d)` | 任务超时 | `*Group[T]` |
| `WithSubmitTimeout(d)` | 提交超时 | `*Group[T]` |
| `WithFFTraceID(ctx)` | FF + TraceID | `(*Group[T], context.Context)` |
| `WithFFSubmitTO(ctx, d)` | FF + 提交超时 | `(*Group[T], context.Context)` |
| `WithFFSubmitTOTraceID(ctx, d)` | FF + 提交超时 + TraceID | `(*Group[T], context.Context)` |
| `WithFFTimeout(ctx, d)` | FF + 任务超时 | `(*Group[T], context.Context)` |
| `WithFFTimeoutTraceID(ctx, d)` | FF + 任务超时 + TraceID | `(*Group[T], context.Context)` |
| `WithFFTimeoutSubmitTO(ctx, d, d)` | FF + 任务超时 + 提交超时 | `(*Group[T], context.Context)` |
| `WithFFTimeoutSubmitTOTraceID(ctx, d, d)` | FF + 任务超时 + 提交超时 + TraceID | `(*Group[T], context.Context)` |
| `WithCtxTraceID(ctx)` | Context + TraceID | `(*Group[T], context.Context)` |
| `WithCtxTimeout(ctx, d)` | Context + 任务超时 | `(*Group[T], context.Context)` |
| `WithCtxTimeoutTraceID(ctx, d)` | Context + 任务超时 + TraceID | `(*Group[T], context.Context)` |
| `WithCtxSubmitTO(ctx, d)` | Context + 提交超时 | `(*Group[T], context.Context)` |
| `WithCtxSubmitTOTraceID(ctx, d)` | Context + 提交超时 + TraceID | `(*Group[T], context.Context)` |

```go
// 完整组合示例
g, ffCtx := async.NewGroup[string](10).
    WithFFTimeoutTraceID(ctx, 5*time.Second)
// 等价于: WithFailFast + WithTimeout(5s) + WithTraceID
```

---

## 自动扩缩容

### EnableAutoScale

```go
// 语法
func (g *Group[T]) EnableAutoScale(cfg *AutoScaleConfig)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `cfg` | `*AutoScaleConfig` | 扩缩容配置，nil 使用默认配置 |

```go
g := async.NewGroup[int](4)

// 方式一：Group 自身方法
g.EnableAutoScale(&async.AutoScaleConfig{
    MinWorkers:      2,
    MaxWorkers:      200,
    CheckInterval:   3 * time.Second,
    ScaleUpFactor:   1.5,
    ScaleDownFactor: 0.7,
})

// 方式二：便捷函数
async.EnableGroupAutoScale(g, nil)
```

### IsAutoScaleEnabled

```go
// 语法
func (g *Group[T]) IsAutoScaleEnabled() bool
```

查询是否已启用自动扩缩容。

### DisableAutoScale

```go
// 语法
func (g *Group[T]) DisableAutoScale()
```

停止自动扩缩容。

```go
g.DisableAutoScale()
// 或
async.DisableGroupAutoScale(g)
```

### NoResult 自动扩缩容

NoResult 同样支持自动扩缩容：

```go
nr := async.NewNoResult(4)

// 方式一
nr.EnableAutoScale(nil)

// 方式二：便捷函数
async.EnableNoResultAutoScale(nr, nil)

// 查询
enabled := nr.IsAutoScaleEnabled()

// 停止
nr.DisableAutoScale()
async.DisableNoResultAutoScale(nr)
```

**扩缩容策略**: 基于 `busy/concurrency` 比率周期性检测：
- `busyRatio > ScaleUpThreshold` 连续 `ScaleUpChecks` 次 → 扩容（`cur * ScaleUpFactor`）
- `busyRatio < ScaleDownThreshold` 连续 `ScaleDownChecks` 次 → 缩容（`cur * ScaleDownFactor`）

---

## 流式结果消费

### WithStreaming

```go
// 语法
func (g *Group[T]) WithStreaming(bufSize int) *Group[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `bufSize` | `int` | channel 缓冲大小，<=0 时自动使用 `Concurrency()*2` |

```go
g := async.NewGroup[int](8).WithStreaming(0)

ch := g.StreamResults()
go func() {
    for r := range ch {
        if r.Ok() {
            fmt.Println("实时:", r.Value)
        }
    }
}()

for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return compute(ctx, item), nil
    })
}
g.Wait() // channel 自动关闭
```

### StreamResults

```go
// 语法
func (g *Group[T]) StreamResults() <-chan core.Result[T]
```

返回流式结果 channel，未启用流式时返回 nil。

### WithResultCallback

```go
// 语法
func (g *Group[T]) WithResultCallback(fn func(core.Result[T])) *Group[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func(core.Result[T])` | 每个任务完成时调用的回调 |

```go
g := async.NewGroup[int](8).WithResultCallback(func(r core.Result[int]) {
    if !r.Ok() {
        log.Printf("任务失败: %v", r.Err)
    }
})
```

---

## Reset 重置

```go
// 语法
func (g *Group[T]) Reset() (*Group[T], error)
```

关闭旧组，创建同等并发度的新组（复用变量）。

```go
g := async.NewGroup[int](4)
results := g.Wait()

newG, err := g.Reset()
if err != nil {
    log.Fatal(err)
}
g = newG // 第二轮
```

---

## NoResult 完整方法

`NoResult` 是 `Group[struct{}]` 的类型别名。支持以下方法：

### 任务提交

| 方法 | 语法 | 说明 |
|------|------|------|
| `Go` | `func (nr *NoResult) Go(ctx, fn) error` | 阻塞提交 |
| `GoWithTimeout` | `func (nr *NoResult) GoWithTimeout(ctx, timeout, fn) error` | 带超时提交 |
| `GoAt` | `func (nr *NoResult) GoAt(index, ctx, fn) error` | 指定位置提交 |
| `GoAtWithTimeout` | `func (nr *NoResult) GoAtWithTimeout(index, ctx, timeout, fn) error` | 指定位置+超时 |

### 等待

| 方法 | 语法 | 说明 |
|------|------|------|
| `Wait` | `func (nr *NoResult) Wait()` | 等待所有任务完成 |
| `WaitTimeout` | `func (nr *NoResult) WaitTimeout(timeout) (ok bool)` | 带超时等待 |
| `WaitContext` | `func (nr *NoResult) WaitContext(ctx) (ok bool)` | context 控制等待 |

### 状态查询

| 方法 | 语法 | 说明 |
|------|------|------|
| `SuccessCount` | `func (nr *NoResult) SuccessCount() int64` | 成功数 |
| `FailCount` | `func (nr *NoResult) FailCount() int64` | 失败数 |
| `HasError` | `func (nr *NoResult) HasError() bool` | 是否有失败 |
| `TotalCount` | `func (nr *NoResult) TotalCount() int64` | 总任务数 |
| `Concurrency` | `func (nr *NoResult) Concurrency() int` | 当前并发度 |
| `Active` | `func (nr *NoResult) Active() int` | 活跃任务数 |
| `Busy` | `func (nr *NoResult) Busy() int` | 忙碌任务数 |
| `Stats` | `func (nr *NoResult) Stats() GroupStats` | 统计信息 |

### 错误提取

| 方法 | 语法 | 说明 |
|------|------|------|
| `Errors` | `func (nr *NoResult) Errors() []error` | 所有错误 |
| `FirstError` | `func (nr *NoResult) FirstError() error` | 第一个错误 |
| `JoinErrors` | `func (nr *NoResult) JoinErrors() error` | 合并错误 |

### 配置

| 方法 | 语法 | 说明 |
|------|------|------|
| `WithTimeout` | `func (nr *NoResult) WithTimeout(d) *NoResult` | 设置超时 |
| `WithFailFast` | `func (nr *NoResult) WithFailFast(ctx) (*NoResult, context.Context)` | FailFast |
| `Reset` | `func (nr *NoResult) Reset() (*NoResult, error)` | 重置 |

---

## NoResult 辅助函数

这些独立函数帮助从已完成的 `NoResult` 中提取和构建聚合结果。

### BuildAggregateNoResult

```go
func BuildAggregateNoResult(nr *NoResult) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

从已 `Wait` 的 `NoResult` 中构建聚合结果，用于 `ForEach`/`ForEachWithFailFast` 等无返回值场景。

| 参数 | 类型 | 说明 |
|------|------|------|
| `nr` | `*NoResult` | 已 Wait 的 NoResult 实例 |
| 返回1 | `int64` | total：总任务数 |
| 返回2 | `int64` | failCnt：失败任务数 |
| 返回3 | `error` | firstErr：第一个遇到的错误 |
| 返回4 | `[]core.Result[struct{}]` | results：所有任务的结果详情 |

**实现细节**：将 `NoResult`（底层为 `*Group[struct{}]`）的 results 切片拷贝一份返回，同时统计 total/failCnt/firstErr。

**使用示例**:

```go
nr := group.NewNoResult(8)
for i := 0; i < 100; i++ {
    nr.Go(ctx, func(ctx context.Context) error {
        return processItem(ctx, i)
    })
}
nr.Wait()

total, failCnt, firstErr, results := group.BuildAggregateNoResult(nr)
fmt.Printf("total=%d fail=%d\n", total, failCnt)
if firstErr != nil {
    fmt.Printf("first error: %v\n", firstErr)
}
```

---

### FillNoResultSkipped

```go
func FillNoResultSkipped(nr *NoResult, total int)
```

在 `nr.Wait()` 之后补齐缺失的结果槽位，确保 `TotalCount() == total`。缺失的槽位会被填充为 `core.ErrSkipped` 错误。

| 参数 | 类型 | 说明 |
|------|------|------|
| `nr` | `*NoResult` | 已 Wait 的 NoResult 实例 |
| `total` | `int` | 期望的总任务数 |

**使用场景**：适用于使用 `GoAt` 提交任务后存在索引空洞（跳过的索引），需要让 `TotalCount()` 反映预期总数时。

**使用示例**:

```go
nr := group.NewNoResult(8)
nr.GoAt(0, ctx, fn1)  // 索引 0
nr.GoAt(5, ctx, fn2)  // 索引 5（跳过 1-4）
nr.Wait()

// 补齐跳过的 1-4 槽位为 ErrSkipped
group.FillNoResultSkipped(nr, 6)
fmt.Println(nr.TotalCount()) // 输出: 6
fmt.Println(nr.FailCount())  // 输出: 4（1-4 为 ErrSkipped）
```

---

## MultiGroup（水平分片任务组）

`MultiGroup[T]` 通过将 N 个 Group 实例组合在一起，以 round-robin 方式分发任务，实现极限高并发。每个分片独立运行，互不影响，可突破单 Group 的锁竞争瓶颈。

**核心特性**：
- Round-robin 分发（原子递增路由）
- Keyed 分发（按 key 哈希固定分片）
- 链式配置代理（WithTimeout）
- 统计聚合和结果合并

> **⚠️ 分片结果不保证全局顺序**
>
> `MultiGroup.Wait()` 依次合并各分片的结果，但分片间不按时间排序。
>
> **⚠️ 通过 Group.Shard() 创建**
>
> `MultiGroup` 没有公开的构造函数，必须通过 `Group.Shard()` 或 `Group.DefaultShard()` 创建。第一个分片复用原 Group，其余自动克隆配置。
>
> **⚠️ goroutine 不复用**
>
> 与 MultiPool 不同，MultiGroup 不会复用 goroutine。每个 `Go()` 启动一个新 goroutine，适合一次性批量任务，不适合百万级高频提交。

### 创建：Shard / DefaultShard

```go
func (g *Group[T]) Shard(shards int) *MultiGroup[T]
func (g *Group[T]) DefaultShard() *MultiGroup[T]
```

| 方法 | 说明 |
|------|------|
| `g.Shard(n)` | 创建 n 个分片，`n <= 1` 返回单分片 |
| `g.DefaultShard()` | 使用 `max(2, GOMAXPROCS)` 作为分片数 |

```go
// 8 个分片 × 每个 50 并发 = 400 并发总量
mg := async.NewGroup[int](50).Shard(8)
defer mg.Close()

// CPU 自动分片
mg := async.NewGroup[int](100).DefaultShard()
defer mg.Close()

// 配合 FailFast
mg := async.NewGroup[int](50).Shard(8)
g0 := mg.GetShard(0)
g0.WithFailFast(ctx) // 只对第一个分片启用 FF
```

### 基础查询：ShardCount / GetShard

```go
func (mg *MultiGroup[T]) ShardCount() int
func (mg *MultiGroup[T]) GetShard(idx int) *Group[T]
```

| 方法 | 说明 |
|------|------|
| `ShardCount()` | 返回分片数 |
| `GetShard(i)` | 获取第 i 个分片的 Group 实例 |

```go
mg := async.NewGroup[int](50).Shard(8)
for i := 0; i < mg.ShardCount(); i++ {
    mg.GetShard(i).WithFailFast(ctx)
}
```

### 任务分发：Go / GoKeyed

```go
func (mg *MultiGroup[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error
func (mg *MultiGroup[T]) GoKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error
```

| 方法 | 分发策略 | 说明 |
|------|----------|------|
| `Go` | Round-robin | 阻塞提交，取到信号量后启动 goroutine |
| `GoKeyed` | Hash(key) | 同一 key 固定分片，适合需要同类任务顺序的场景 |

```go
mg := async.NewGroup[int](50).Shard(8)

// Round-robin 分发
for i := 0; i < 10000; i++ {
    mg.Go(ctx, func(ctx context.Context) (int, error) {
        return i * i, nil
    })
}
results := mg.Wait()

// Keyed 分发
for _, userID := range userIDs {
    mg.GoKeyed(uint64(userID), ctx, func(ctx context.Context) (int, error) {
        return processUser(ctx, userID)
    })
}
```

### 结果收集：Wait / Close

```go
func (mg *MultiGroup[T]) Wait() []core.Result[T]
func (mg *MultiGroup[T]) Close()
```

| 方法 | 说明 |
|------|------|
| `Wait()` | 等待所有分片完成，合并结果 |
| `Close()` | 关闭所有分片，释放资源 |

```go
mg := async.NewGroup[int](50).Shard(8)
defer mg.Close()

for i := 0; i < 10000; i++ {
    mg.Go(ctx, fn)
}
results := mg.Wait()
for _, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

### 链式配置：WithTimeout

```go
func (mg *MultiGroup[T]) WithTimeout(d time.Duration) *MultiGroup[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 任务超时时间 |

```go
mg := async.NewGroup[int](50).Shard(8).WithTimeout(5 * time.Second)
defer mg.Close()
```

### 统计聚合

```go
func (mg *MultiGroup[T]) TotalActive() int
func (mg *MultiGroup[T]) TotalBusy() int
func (mg *MultiGroup[T]) TotalConcurrency() int
func (mg *MultiGroup[T]) TotalTaskCount() int64
func (mg *MultiGroup[T]) TotalFailCount() int64
func (mg *MultiGroup[T]) TotalSuccessCount() int64
func (mg *MultiGroup[T]) Errors() []error
func (mg *MultiGroup[T]) Values() []T
```

| 方法 | 说明 |
|------|------|
| `TotalActive()` | 所有分片活跃任务总数 |
| `TotalBusy()` | 所有分片忙碌任务总数 |
| `TotalConcurrency()` | 所有分片并发度之和 |
| `TotalTaskCount()` | 所有分片已提交任务总数 |
| `TotalFailCount()` | 所有分片失败任务总数 |
| `TotalSuccessCount()` | 所有分片成功任务总数 |
| `Errors()` | 所有分片所有任务的错误切片 |
| `Values()` | 所有分片成功任务的结果值 |

```go
mg := async.NewGroup[int](50).Shard(8)
defer mg.Close()

for i := 0; i < 10000; i++ {
    mg.Go(ctx, fn)
}
mg.Wait()

fmt.Printf("concurrency: %d, active: %d, busy: %d\n",
    mg.TotalConcurrency(), mg.TotalActive(), mg.TotalBusy())
fmt.Printf("success: %d, fail: %d, total: %d\n",
    mg.TotalSuccessCount(), mg.TotalFailCount(), mg.TotalTaskCount())

// 提取所有值
values := mg.Values()
// 提取所有错误
errors := mg.Errors()
```

---

## 性能基准

| 场景 | 吞吐量 |
|------|--------|
| Group AutoScale 10M | **788K ops/s** |
| NoResult AutoScale 10M | **805K ops/s** |
| Group AutoScale 200K | **970K ops/s** |