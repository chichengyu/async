# Pool（协程池）文档

## 概述

`Pool[T]` 是泛型协程池，复用固定数量的 goroutine 处理高频并发任务。通过 `Pool[T]()` Builder 链式构建，终端方法 `Run(fn)` 自动管理生命周期。

**三种 Pool 入口**：

| 入口 | 说明 | 终端方法 |
|------|------|----------|
| `async.Pool[T]()` | 单协程池 | `.Run(fn)` |
| `async.PoolMulti[T]()` | 水平分片协程池（MultiPool） | `.Run(fn)` |
| `async.PoolSharded[T]()` | 分片协程池构建器（ShardPool，详见 Shard 文档） | `.Run(fn)` |

> **⚠️ PoolBuilder 只有 `.Run(fn)` 作为终端方法，没有 `.Build()`**
>
> `PoolBuilder[T]` 的唯一终端方法是 `.Run(fn func(ctx context.Context, p *Pool[T]) error) error`。fn 回调接收一个已创建好的 `*Pool[T]`，fn 返回后自动 Close 池。这与 GroupBuilder 不同——GroupBuilder 同时提供 `.Build()` 和 `.Run(fn)` 两种终端方法。
>
> **⚠️ Wait 一次性**
>
> `Wait()`、`WaitTimeout()`、`WaitContext()` 三者互斥，只能调用其中一个。调用后不可再 Submit。如需重复提交+等待，请使用 `ResetWait()` 或 `Reset()`。
>
> **⚠️ Submit 后必须收集**
>
> Submit 后必须调用 Wait / WaitAndClose / CloseAndWait 中的一种来收集结果，否则创建的 goroutine 会泄漏。

---

## 目录

- [PoolBuilder 链式方法速查表](#poolbuilder-链式方法速查表)
- [MultiPoolBuilder 链式方法速查表](#multipoolbuilder-链式方法速查表)
- [Pool 实例方法速查表](#pool-实例方法速查表)
- [MultiPool 实例方法速查表](#multipool-实例方法速查表)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)
- [基本配置与生命周期](#基本配置与生命周期)
- [任务提交](#任务提交)
- [等待与关闭](#等待与关闭)
- [状态查询与错误提取](#状态查询与错误提取)
- [流式结果消费](#流式结果消费)
- [环形缓冲](#环形缓冲)
- [背压控制](#背压控制)
- [MultiPool（水平分片协程池）](#multipool水平分片协程池)
- [AutoScale 自动扩缩容](#autoscale-自动扩缩容)
- [辅助函数](#辅助函数)
- [自定义日志](#自定义日志)
- [常见错误](#常见错误)

---

## PoolBuilder 链式方法速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.Pool[T]()` | 创建协程池链式构建器 |
| `async.PoolMulti[T]()` | 创建水平分片协程池构建器 |
| `async.PoolSharded[T]()` | 创建分片协程池构建器（详见 [Shard 文档](shard.md)） |

### PoolBuilder 链式配置方法

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `context.Background()` |
| `.Worker(n)` | worker 数量（n≤0 自动修正为 `IO()`） | `IO()`（`runtime.NumCPU()×2`） |
| `.DefaultWorker()` | 使用 IO 并发度 `core.IO()`（`NumCPU×2`） | — |
| `.Timeout(d)` | 单任务超时 | `core.GetDefaultTimeout()`（全局默认 30s） |
| `.DefaultTimeout()` | 恢复为全局默认超时（`core.GetDefaultTimeout()`，30s） | — |
| `.SubmitTimeout(d)` | 提交超时 | 无限制（阻塞等待） |
| `.DefaultSubmitTimeout()` | 清除提交超时（设为 0 = 不限时） | — |
| `.FailFast()` | 启用 FailFast（任一失败取消全体） | 关闭 |
| `.DefaultFailFast()` | 关闭 FailFast | — |
| `.MaxPending(n)` | 最大排队任务数 | 无限制（0 = 不限） |
| `.DefaultMaxPending()` | 清除最大排队限制（设为 0） | — |
| `.OverflowBlock()` | 队列满时阻塞等待（**默认**） | 默认 |
| `.OverflowDrop()` | 队列满时丢弃新任务 | — |
| `.OverflowError()` | 队列满时返回 `ErrQueueOverflow` | — |
| `.Overflow(s)` | 设置 Overflow 策略（Block/Drop/Error） | `OverflowBlock`（Pool 结构体零值=0=Block） |
| `.DefaultOverflow()` | 恢复默认溢出策略（`OverflowDrop`） | — |
| `.RingBuf(cap)` | 环形缓冲容量 | 0（禁用） |
| `.DefaultRingBuf()` | 恢复默认（禁用环形缓冲） | — |
| `.MaxResults(n)` | 最大结果存储数（-1=默认100K, 0=无限） | `core.GetDefaultMaxResults()`（全局默认 100_000） |
| `.DefaultMaxResults()` | 恢复默认最大结果数（-1 = 使用全局默认 100K） | — |
| `.Streaming(buf)` | 流式结果 channel 缓冲大小（buf>0 启用） | 0（禁用） |
| `.DefaultStreaming()` | 恢复默认（禁用流式，0） | — |
| `.Config(fn)` | 函数式配置，通过闭包修改 `Config` | — |
| `.DefaultPoolConfig()` | 重置所有配置为 `DefaultConfig()` | — |
| `.Logger(l)` | 注入自定义日志（**全局生效**） | 静默 |
| `.DefaultLogger()` | 恢复默认日志（`core.SetLogger(nil)`） | — |

### PoolBuilder 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Run(fn)` | `error` | 创建池 → 执行 fn → fn 返回后自动 Close |

> **⚠️ `.Run(fn)` 回调签名**
>
> `fn func(ctx context.Context, p *Pool[T]) error`：`ctx` 是 builder 上设置的上下文，`p` 是已创建的协程池实例。fn 返回后自动 `Close` 池。

---

## MultiPoolBuilder 链式方法速查表

`async.PoolMulti[T]()` 返回 `*MultiPoolBuilder[T]`，提供以下链式配置方法：

### MultiPoolBuilder 链式配置方法

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `context.Background()` |
| `.Shards(n)` | 水平分片数（≤1 不启用分片） | 无分片（需显式设置） |
| `.DefaultShards()` | 使用默认分片数（`GOMAXPROCS`，最少 2） | — |
| `.Worker(n)` | 每分片 worker 数量（n≤0 自动修正） | `IO()`（`NumCPU×2`） |
| `.DefaultWorker()` | 使用 IO 并发度 `core.IO()`（`NumCPU×2`） | — |
| `.Timeout(d)` | 单任务超时 | `core.GetDefaultTimeout()`（全局默认 30s） |
| `.DefaultTimeout()` | 恢复为全局默认超时（`core.GetDefaultTimeout()`，30s） | — |
| `.SubmitTimeout(d)` | 提交超时 | 无限制（阻塞等待） |
| `.DefaultSubmitTimeout()` | 清除提交超时（设为 0 = 阻塞等待） | — |
| `.FailFast()` | 启用 FailFast | 关闭 |
| `.DefaultFailFast()` | 关闭 FailFast | — |
| `.MaxPending(n)` | 每分片最大排队任务数 | 无限制 |
| `.DefaultMaxPending()` | 清除最大排队限制（设为 0） | — |
| `.OverflowBlock()` | 队列满时阻塞等待（**默认**） | 默认 |
| `.OverflowDrop()` | 队列满时丢弃新任务 | — |
| `.OverflowError()` | 队列满时返回错误 | — |
| `.Overflow(s)` | 设置 Overflow 策略 | `OverflowBlock`（Pool 结构体零值=0=Block） |
| `.DefaultOverflow()` | 显式设为 `OverflowDrop` | — |
| `.RingBuf(cap)` | 每分片环形缓冲容量 | 0（禁用） |
| `.DefaultRingBuf()` | 恢复默认（禁用环形缓冲） | — |
| `.MaxResults(n)` | 每分片最大结果数（-1=默认100K） | 100_000 |
| `.DefaultMaxResults()` | 恢复默认最大结果数 | — |
| `.Streaming(buf)` | 每分片流式结果 channel 缓冲大小 | 0（禁用） |
| `.DefaultStreaming()` | 恢复默认（禁用流式） | — |
| `.Pool(p)` | 注入外部协程池（分片复用，用户不再使用该池） | 无 |
| `.DefaultPool()` | 清除外部池注入，恢复内部自动创建 | — |
| `.Config(fn)` | 函数式配置，通过闭包修改 `Config` | — |
| `.DefaultMultiPoolConfig()` | 重置所有配置为 `DefaultConfig()` | — |
| `.Logger(l)` | 注入自定义日志（**全局生效**） | 静默 |
| `.DefaultLogger()` | 恢复默认日志（`core.SetLogger(nil)`） | — |

### MultiPoolBuilder 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Run(fn)` | `error` | 创建池 → 分片 → 执行 fn → 自动 Close 所有分片 |

> **⚠️ `.Run(fn)` 回调签名**
>
> `fn func(ctx context.Context, mp *MultiPool[T]) error`：`ctx` 是 builder 上设置的上下文，`mp` 是已创建的分片协程池实例。fn 返回后自动 `Close` 所有分片。

---

## Pool 实例方法速查表

以下方法在 `Run` 回调中的 `p *Pool[T]` 实例上可用：

### 任务提交

| 方法 | 签名 | 说明 |
|------|------|------|
| `Submit` | `func(ctx, fn) error` | 阻塞提交任务，等待有空闲 worker |
| `SubmitAt` | `func(index, ctx, fn) error` | 指定索引位置提交，结果在 results[index] |
| `TrySubmit` | `func(ctx, fn) error` | 非阻塞提交，满时返回 `ErrSubmitTimeout` |

### 等待与关闭

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Wait()` | `[]Result[T]` | 阻塞等待所有任务完成，返回结果切片 |
| `WaitAndClose()` | `[]Result[T]` | 先 Wait 再 Close |
| `WaitTimeout(d)` | `([]Result[T], bool)` | 带超时等待，ok=false 表示超时 |
| `WaitContext(ctx)` | `([]Result[T], bool)` | 带 context 等待 |
| `Close()` | — | 立即关闭，不等待未完成任务 |
| `CloseAndWait()` | — | 等待所有任务完成后关闭 |
| `CloseAndWaitTimeout(d)` | `(bool, <-chan struct{})` | 带超时的等待完成后关闭 |
| `CloseByIdle(d)` | — | 空闲 d 时长后自动关闭 |
| `Reset()` | `(*Pool[T], error)` | 完整重置状态以便复用 |
| `ResetWait()` | `*Pool[T]` | 重置 waited 状态，允许继续 Submit+Wait |

### 状态查询

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Size()` | `int` | 当前 worker 数量 |
| `Active()` | `int` | 当前活跃（已启动未结束）的 goroutine 数 |
| `Busy()` | `int` | 正在执行任务的 goroutine 数 |
| `Pending()` | `int` | 排队中的任务数 |
| `QueueDepth()` | `int` | 队列深度 |
| `TotalCount()` | `int64` | 已提交任务总数 |
| `SuccessCount()` | `int64` | 成功任务数 |
| `FailCount()` | `int64` | 失败任务数 |
| `HasError()` | `bool` | 是否有错误 |
| `Stats()` | `PoolStats` | 运行时统计快照 |

### 错误提取

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Errors()` | `[]error` | 所有非 nil 错误切片 |
| `FirstError()` | `error` | 第一个错误 |
| `JoinErrors()` | `error` | 所有错误合并为一个 error（`errors.Join`） |
| `Values()` | `[]T` | 提取所有成功的值切片 |

### 运行时配置（实例级）

| 方法 | 签名 | 说明 |
|------|------|------|
| `WithTimeout(d)` | `*Pool[T]` | 设置任务超时（d>0 生效） |
| `WithSubmitTimeout(d)` | `*Pool[T]` | 设置提交超时（d>0 生效） |
| `WithFailFast(ctx)` | `(*Pool[T], context.Context)` | 启用 FailFast，返回新的 ffCtx |
| `WithFFCtx(ctx)` | `(*Pool[T], context.Context)` | `WithFailFast` 的缩写 |
| `WithStreaming(bufSize)` | `*Pool[T]` | 启用流式结果（bufSize≤0 自动=Worker×2） |
| `WithResultCallback(fn)` | `*Pool[T]` | 设置每个任务完成时的回调 |
| `WithRingBuffer(cap, overflow)` | `*Pool[T]` | 启用环形缓冲 |
| `WithMaxPending(n)` | `*Pool[T]` | 设置最大排队任务数 |
| `WithOverflow(strategy)` | `*Pool[T]` | 设置溢出策略 |
| `WithMaxResults(n)` | `*Pool[T]` | 设置最大结果数（≤0=无限） |
| `WithTraceID(ctx)` | `(*Pool[T], context.Context)` | 注入 TraceID 并返回新 ctx |
| `WithContext(ctx)` | `(*Pool[T], context.Context)` | 设置池级上下文 |
| `MergeFailFastCancel(cancel)` | `*Pool[T]` | 合并外部 CancelFunc |

### 流式与环形缓冲

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `StreamResults()` | `<-chan Result[T]` | 返回流式结果 channel |
| `StreamDropped()` | `int64` | 因 stream channel 满被丢弃的结果数 |
| `Flush(maxCount)` | `[]Result[T]` | 从环形缓冲中取出最多 maxCount 个结果 |
| `RingBufDropped()` | `int64` | 因环形缓冲溢出被丢弃的元素数 |

### 动态扩缩容

| 方法 | 签名 | 说明 |
|------|------|------|
| `Resize(newSize)` | `int` | 调整 worker 数，返回实际大小 |
| `ResizeAndWaitTimeout(newSize, timeout)` | — | 调整 worker 数并等待超时清理 |
| `EnableAutoScale(config)` | — | 启用自动扩缩容（config=nil 使用默认配置） |
| `DisableAutoScale()` | — | 停止自动扩缩容 |
| `IsAutoScaleEnabled()` | `bool` | 是否已启用自动扩缩容 |
| `Ctx()` | `context.Context` | 获取池级上下文 |

---

## MultiPool 实例方法速查表

以下方法在 `Run` 回调中的 `mp *MultiPool[T]` 实例上可用：

### 任务提交

| 方法 | 签名 | 说明 |
|------|------|------|
| `Submit` | `func(ctx, fn) error` | 阻塞提交，round-robin 分发到某分片 |
| `TrySubmit` | `func(ctx, fn) error` | 非阻塞提交，round-robin 分发 |
| `SubmitKeyed` | `func(key, ctx, fn) error` | 按 key 哈希路由到固定分片（阻塞） |
| `TrySubmitKeyed` | `func(key, ctx, fn) error` | 按 key 哈希路由到固定分片（非阻塞） |
| `SubmitBatch` | `func(ctx, items, fn) []SubmitResult` | 批量提交，返回每项分片信息 |

### 等待与关闭

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Wait()` | `[]Result[T]` | 等待所有分片完成，合并结果 |
| `WaitAndClose()` | `[]Result[T]` | 等待完成并关闭所有分片 |
| `Close()` | — | 关闭所有分片 |

### 查询

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `ShardCount()` | `int` | 分片数量 |
| `GetShard(idx)` | `*Pool[T]` | 获取第 idx 个分片的 Pool 实例，越界返回 nil |

### 运行时配置（代理到所有分片）

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `WithTimeout(d)` | `*MultiPool[T]` | 为所有分片设置任务超时 |
| `WithSubmitTimeout(d)` | `*MultiPool[T]` | 为所有分片设置提交超时 |
| `WithStreaming(bufSize)` | `*MultiPool[T]` | 为所有分片启用流式 |
| `WithResultCallback(fn)` | `*MultiPool[T]` | 为所有分片设置结果回调 |
| `WithRingBuffer(cap, overflow)` | `*MultiPool[T]` | 为所有分片启用环形缓冲 |
| `WithMaxPending(n)` | `*MultiPool[T]` | 为所有分片设置最大排队 |
| `WithOverflow(strategy)` | `*MultiPool[T]` | 为所有分片设置溢出策略 |
| `WithMaxResults(n)` | `*MultiPool[T]` | 为所有分片设置最大结果数 |

### 流式与冲洗

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `StreamResults()` | `<-chan Result[T]` | Fan-in 所有分片的流式结果 channel |
| `Flush(maxPerShard)` | `[]Result[T]` | 从所有分片环形缓冲各取 maxPerShard 个结果 |

### 统计汇总

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `TotalActive()` | `int` | 汇总各分片活跃任务数 |
| `TotalBusy()` | `int` | 汇总各分片忙碌任务数 |
| `TotalPending()` | `int` | 汇总各分片排队任务数 |
| `TotalWorkerCount()` | `int` | 汇总各分片 worker 总数 |
| `TotalCount()` | `int64` | 汇总所有分片任务总数 |
| `TotalSuccessCount()` | `int64` | 汇总各分片成功数 |
| `TotalFailCount()` | `int64` | 汇总各分片失败数 |

---

## 默认值体系

Pool 的默认值遵循**三层架构**：全局级 → Builder 级 → 实例级。

### 一层：全局默认值

| 默认值 | 获取函数 | 修改函数 | 默认值 | 说明 |
|--------|----------|----------|--------|------|
| 默认超时 | `core.GetDefaultTimeout()` | `core.SetDefaultTimeout(d)` | **30s** | 设为 0 关闭默认超时 |
| 提交超时 | `core.GetSubmitTimeout()` | `core.SetSubmitTimeout(d)` | **5s** | NewPool 时的默认 SubmitTimeout |
| IO 并发度 | `core.IO()` | 不可修改 | **`runtime.NumCPU()×2`** | PoolBuilder 默认 Worker 数 |
| 最大结果数 | `core.GetDefaultMaxResults()` | `core.SetDefaultMaxResults(n)` | **100_000** | `MaxResults(-1)` 时使用 |
| 默认环形缓冲容量 | `core.GetDefaultRingBufferCap()` | `core.SetDefaultRingBufferCap(n)` | **0**（不启用） | `RingBuf(0)` 时不启用 |
| 默认溢出策略 | `core.GetDefaultOverflowStrategy()` | `core.SetDefaultOverflowStrategy(s)` | **OverflowDrop** | 环形缓冲满时策略 |

### 二层：Builder 级默认值

| `Default*` 方法 | 源码行为 | 适用于 |
|-----------------|----------|--------|
| `.DefaultWorker()` | `b.cfg.Size = core.IO()`（`NumCPU×2`） | PoolBuilder, MultiPoolBuilder |
| `.DefaultTimeout()` | `b.cfg.Timeout = 0`（applyConfig 不覆盖，池使用全局默认 `core.GetDefaultTimeout()` 30s） | PoolBuilder, MultiPoolBuilder |
| `.DefaultSubmitTimeout()` | `b.cfg.SubmitTimeout = 0`（不限时） | PoolBuilder, MultiPoolBuilder |
| `.DefaultFailFast()` | `b.cfg.FailFast = false` | PoolBuilder, MultiPoolBuilder |
| `.DefaultMaxPending()` | `b.cfg.MaxPending = 0`（不限时） | PoolBuilder, MultiPoolBuilder |
| `.DefaultOverflow()` | `b.cfg.Overflow = OverflowDrop` | PoolBuilder, MultiPoolBuilder |
| `.DefaultRingBuf()` | `b.cfg.RingBufCap = 0`（不启用） | PoolBuilder, MultiPoolBuilder |
| `.DefaultMaxResults()` | `b.cfg.MaxResults = -1`（-1 = 使用全局默认 100K） | PoolBuilder, MultiPoolBuilder |
| `.DefaultStreaming()` | `b.cfg.Streaming = 0`（不启用） | PoolBuilder, MultiPoolBuilder |
| `.DefaultShards()` | `b.shards = 0`（自动：GOMAXPROCS，最少 2） | MultiPoolBuilder |
| `.DefaultPool()` | `b.extP = nil`（恢复内部自动创建） | MultiPoolBuilder |
| `.DefaultPoolConfig()` | `b.cfg = DefaultConfig()`（全部默认） | PoolBuilder |
| `.DefaultMultiPoolConfig()` | `b.cfg = DefaultConfig()`（全部默认） | MultiPoolBuilder |
| `.DefaultLogger()` | `core.SetLogger(nil)`（恢复全局静默日志） | PoolBuilder, MultiPoolBuilder |

### 三层：实例级 fallback

Pool 实例上的配置方法（如 `p.WithTimeout(d)`）在特定条件下有内置 fallback：

| 实例方法 | fallback 行为 |
|----------|--------------|
| `p.WithStreaming(bufSize)` | `bufSize ≤ 0` → `bufSize = p.Size() * 2` |
| `p.EnableAutoScale(config)` | `config == nil` → 使用 `core.DefaultAutoScaleConfig()` |
| `p.WithTimeout(d)` | 直接设置，无 fallback |
| `p.WithSubmitTimeout(d)` | 直接设置，无 fallback |
| `p.Resize(newSize)` | `newSize ≤ 0` → 使用 `core.IO()` |

### 默认值覆盖优先级

```
全局默认值（core.Set*）
    ↓ 被覆盖
Builder 级显式设置（.Worker(n), .Timeout(d) 等）
    ↓ 被覆盖
实例级运行时设置（p.WithTimeout(d), p.Resize(n) 等）
```

---

## 生产环境使用建议

### Pool vs Group 选型

| 场景 | 推荐 | 理由 |
|------|------|------|
| 一次性批量处理 N 条记录 | **Group** | 用完即销毁，代码简单 |
| 长期运行的消费者/定时器 | **Pool** | Worker 复用，避免频繁创建/销毁 |
| 请求量波动的 API 网关 | **Pool + AutoScale** | 动态扩缩容，兼顾低延迟与资源 |
| 海量任务（百万/千万） | **Pool + RingBuffer** | 环形缓冲防止内存爆炸 |

### 生产推荐配置

```go
async.Pool[MyType]().Context(ctx).
    Worker(async.IO()).
    Timeout(30 * time.Second).              // 单任务超时
    SubmitTimeout(5 * time.Second).         // 提交超时
    MaxPending(10_000).                     // 最大排队 = 背压控制
    Overflow(async.OverflowError).          // 超出返回错误供业务降级
    MaxResults(100_000).                    // 限制结果切片（全局默认已是 100K）
    RingBuf(50_000).                        // 环形缓冲兜底
    Streaming(4096).                        // 流式消费降低内存峰值
    Run(func(ctx context.Context, p *Pool[MyType]) error {
        for i := 0; i < 1_000_000; i++ {
            if err := p.Submit(ctx, fn); err != nil {
                return err  // OverflowError 或 SubmitTimeout
            }
        }
        results := p.Wait()
        return handleResults(results)
    })
```

### MultiPool 生产推荐配置

当单 Pool 达到 QPS 上限（~37万/s）时，通过水平分片线性扩展吞吐量：

```go
// 8 分片 MultiPool：吞吐量 ~2.8M ops/s（7.6x 线性扩展）
async.PoolMulti[MyType]().Context(ctx).
    Shards(8).                              // 8 个独立 Pool 分片
    Worker(async.IO()).                     // 每分片 worker 数（总 = 8 × IO）
    Timeout(30 * time.Second).              // 单任务超时
    SubmitTimeout(5 * time.Second).         // 提交超时
    MaxPending(5_000).                      // 每分片最大排队
    Overflow(async.OverflowError).          // 超出返回错误
    MaxResults(100_000).                    // 每分片最大结果数
    RingBuf(50_000).                        // 每分片环形缓冲
    Run(func(ctx context.Context, mp *MultiPool[MyType]) error {
        for i := 0; i < 10_000_000; i++ {
            idx := i
            if err := mp.Submit(ctx, func(ctx context.Context) (MyType, error) {
                return process(ctx, idx)
            }); err != nil {
                return err
            }
        }
        results := mp.WaitAndClose()
        return handleResults(results)
    })

// 16 分片 MultiPool：吞吐量 ~5.5M ops/s（~15x 线性扩展）
// 适合千万级 QPS 的极限场景
async.PoolMulti[MyType]().Context(ctx).
    Shards(16).
    Worker(async.IO()).
    MaxResults(100_000).RingBuf(50_000).
    Overflow(async.OverflowDrop).           // 极限场景用 Drop 避免阻塞
    Run(func(ctx context.Context, mp *MultiPool[MyType]) error {
        for i := 0; i < 100_000_000; i++ {
            idx := i
            mp.Submit(ctx, func(ctx context.Context) (MyType, error) {
                return process(ctx, idx)
            })
        }
        results := mp.WaitAndClose()
        return handleResults(results)
    })
```

### IO 密集型 vs CPU 密集型 Worker 数量

| 类型 | 推荐并发度 | 常量 |
|------|-----------|------|
| IO 密集型（网络/文件） | `NumCPU × 2` | `async.IO()` |
| CPU 密集型（计算） | `NumCPU` | `async.CPU()` |
| 极高并发场景 | `NumCPU × 4` | `async.IOMulti(4)` |

---

## 基本配置与生命周期

### 基础用法

```go
err := async.Pool[int]().Context(ctx).
    Worker(8).
    Run(func(ctx context.Context, p *Pool[int]) error {
        for i := 0; i < 100; i++ {
            idx := i
            p.Submit(ctx, func(ctx context.Context) (int, error) {
                return heavyTask(ctx, idx)
            })
        }
        results := p.Wait()
        return checkResults(results)
    })
if err != nil {
    log.Fatal(err)
}
```

### 完整配置

```go
err := async.Pool[string]().Context(ctx).
    Worker(64).
    Timeout(5 * time.Second).
    FailFast().
    MaxPending(10000).
    Overflow(async.OverflowError).
    Run(func(ctx context.Context, p *Pool[string]) error {
        for _, item := range items {
            err := p.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            })
            if err != nil {
                return err
            }
        }
        results := p.Wait()
        return handleResults(results)
    })
```

### 默认配置快速启动

```go
err := async.Pool[int]().Context(ctx).
    DefaultPoolConfig().
    Run(func(ctx context.Context, p *Pool[int]) error {
        // 使用全部默认值：Worker=IO(), Timeout=30s, 无限制排队...
        return nil
    })
```

---

## 任务提交

### Submit

阻塞提交任务，等待有空闲 worker 为止：

```go
p.Submit(ctx, func(ctx context.Context) (string, error) {
    return fetchData(ctx), nil
})
```

### TrySubmit

非阻塞提交，worker 全忙时立即返回 `ErrSubmitTimeout`：

```go
err := p.TrySubmit(ctx, fn)
if errors.Is(err, async.ErrSubmitTimeout) {
    log.Println("队列已满，任务被丢弃")
}
```

### SubmitAt

在指定索引位置提交任务，结果在 `Wait()` 返回的切片中保持对应位置：

```go
for i, item := range items {
    p.SubmitAt(i, ctx, func(ctx context.Context) (string, error) {
        return process(ctx, items[i])
    })
}
// results[i] 对应 items[i]
```

---

## 等待与关闭

### Wait

阻塞等待所有已提交任务完成，返回按提交顺序排列的结果切片：

```go
results := p.Wait()
for _, r := range results {
    if r.Ok() {
        fmt.Printf("成功: %v\n", r.Value)
    }
}
```

### WaitAndClose

等价于先 `Wait()` 再 `Close()`：

```go
results := p.WaitAndClose()
```

### WaitTimeout / WaitContext

超时后返回部分结果：

```go
// 带超时等待
results, ok := p.WaitTimeout(3 * time.Second)

// 通过 context 控制
results, ok := p.WaitContext(ctx)
```

### Close / CloseAndWait / CloseAndWaitTimeout

| 方法 | 说明 |
|------|------|
| `Close()` | 立即停止 worker，不等待任务完成 |
| `CloseAndWait()` | 等待所有任务完成后关闭 |
| `CloseAndWaitTimeout(d)` | 带超时的等待完成后关闭 |

### ResetWait / Reset

| 方法 | 说明 |
|------|------|
| `ResetWait()` | 重置 waited 状态，允许继续 Submit+Wait |
| `Reset()` | 完整重置，允许继续 Submit+Wait |

---

## 状态查询与错误提取

### 状态查询

| 方法 | 说明 |
|------|------|
| `Size()` | 当前 worker 数量 |
| `Active()` | 活跃 goroutine 数 |
| `Busy()` | 正在执行任务的 goroutine 数 |
| `Pending()` | 排队中的任务数 |
| `QueueDepth()` | 队列深度 |
| `TotalCount()` | 已提交任务总数 |
| `SuccessCount()` | 成功任务数 |
| `FailCount()` | 失败任务数 |
| `HasError()` | 是否有错误 |
| `Stats()` | 运行时统计快照 |

### 错误提取

| 方法 | 说明 |
|------|------|
| `Errors()` | 所有非 nil 错误切片 |
| `FirstError()` | 第一个错误 |
| `JoinErrors()` | 所有错误合并为一个 error |
| `Values()` | 提取所有成功的值切片 |

```go
results := p.Wait()
if p.HasError() {
    for _, e := range p.Errors() {
        log.Printf("错误: %v", e)
    }
    return p.JoinErrors()
}
values := p.Values()
```

---

## 流式结果消费

通过 `.Streaming(buf)` 启用流式结果，结果通过 channel 实时发送：

```go
err := async.Pool[string]().Context(ctx).
    Worker(8).
    Streaming(1024).
    Run(func(ctx context.Context, p *Pool[string]) error {
        ch := p.StreamResults()

        go func() {
            for r := range ch {
                if r.Ok() {
                    fmt.Println("实时收到:", r.Value)
                }
            }
        }()

        for _, item := range items {
            p.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            })
        }
        p.Wait()
        return nil
    })
```

> **⚠️ Streaming 与 Wait 互斥**
>
> `Streaming(bufSize > 0)` 启用流式消费后，`Wait()` 仍可调用但结果为空（结果已通过 channel 发出）。使用流式模式时，建议不依赖 `Wait()` 的返回结果。
>
> **⚠️ 消费者慢时静默丢弃**
>
> stream channel 满时采用非阻塞写入，结果会被静默丢弃。通过 `p.StreamDropped()` 监控丢弃量。

---

## 环形缓冲

环形缓冲适用于海量任务场景，避免 `Wait()` 时内存爆炸。配合 `Flush` 分批消费：

```go
err := async.Pool[string]().Context(ctx).
    Worker(8).
    RingBuf(10000).
    OverflowDrop().
    Run(func(ctx context.Context, p *Pool[string]) error {
        for i := 0; i < 10_000_000; i++ {
            p.Submit(ctx, fn)
        }

        // 分批 Flush 消费结果
        for {
            batch := p.Flush(5000)
            if len(batch) == 0 {
                break
            }
            processBatch(batch)
        }
        return nil
    })
```

> **⚠️ RingBuf 不限制 results 切片**
>
> 环形缓冲和 results 切片是两套并行的存储通道。如需完全避免 results 增长，请配合设置 `MaxResults(0)`。

---

## 背压控制

通过 `MaxPending` + `Overflow` 策略控制队列大小，防止任务堆积导致 OOM：

```go
err := async.Pool[string]().Context(ctx).
    Worker(8).
    MaxPending(1000).
    OverflowError().
    Run(func(ctx context.Context, p *Pool[string]) error {
        for _, item := range items {
            err := p.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            })
            if errors.Is(err, async.ErrQueueOverflow) {
                fallbackProcess(item) // 降级处理
                continue
            }
        }
        results := p.Wait()
        return handleResults(results)
    })
```

---

## MultiPool（水平分片协程池）

当单 Pool 达到 QPS 上限时，通过水平扩展突破瓶颈：

### 基本用法

```go
err := async.PoolMulti[string]().Context(ctx).
    Shards(8).              // 8 个分片
    Worker(16).             // 每分片 16 worker = 128 总 worker
    Run(func(ctx context.Context, mp *MultiPool[string]) error {
        for _, url := range urls {
            mp.Submit(ctx, func(ctx context.Context) (string, error) {
                return httpGet(ctx, url)
            })
        }
        results := mp.WaitAndClose()
        return handleResults(results)
    })
```

### RoundRobin 分发（默认）

```go
err := async.PoolMulti[string]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *MultiPool[string]) error {
        for _, item := range items {
            mp.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            })
        }
        results := mp.Wait()
        handleResults(results)
        return nil
    })
```

### Key 亲和提交

```go
err := async.PoolMulti[int]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *MultiPool[int]) error {
        for _, userID := range userIDs {
            // 同一 userID 始终路由到同一分片
            mp.SubmitKeyed(uint64(userID), ctx, func(ctx context.Context) (int, error) {
                return processUser(ctx, userID)
            })
        }
        results := mp.Wait()
        handleResults(results)
        return nil
    })
```

### 非阻塞批量提交

```go
err := async.PoolMulti[int]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *MultiPool[int]) error {
        results := mp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
            return process(item)
        })
        for _, r := range results {
            if r.Err != nil {
                log.Printf("分片 %d 索引 %d 提交失败", r.ShardIdx, r.Index)
            }
        }
        allResults := mp.Wait()
        handleResults(allResults)
        return nil
    })
```

### 流式消费（Fan-in 所有分片）

```go
err := async.PoolMulti[string]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *MultiPool[string]) error {
        mp.WithStreaming(1024)

        go func() {
            for _, item := range items {
                mp.Submit(ctx, func(ctx context.Context) (string, error) {
                    return process(item)
                })
            }
            mp.WaitAndClose()
        }()

        for r := range mp.StreamResults() {
            if !r.Ok() {
                log.Printf("失败: %v", r.Err)
                continue
            }
            saveResult(r.Value)
        }
        return nil
    })
```

### 运行时配置代理

```go
err := async.PoolMulti[int]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *MultiPool[int]) error {
        // 运行时为所有分片统一设置
        mp.WithTimeout(10 * time.Second)
        mp.WithMaxPending(5000)
        mp.WithMaxResults(50000)

        for _, item := range items {
            mp.Submit(ctx, processItem)
        }
        results := mp.Wait()
        return handleResults(results)
    })
```

### 统计汇总

```go
err := async.PoolMulti[int]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *MultiPool[int]) error {
        for _, item := range items {
            mp.Submit(ctx, processItem)
        }
        results := mp.Wait()

        fmt.Printf("总 worker: %d, 总任务: %d, 成功: %d, 失败: %d\n",
            mp.TotalWorkerCount(),
            mp.TotalCount(),
            mp.TotalSuccessCount(),
            mp.TotalFailCount())
        return nil
    })
```

### 注入外部池

```go
err := async.PoolMulti[string]().Context(ctx).
    Worker(8).
    Shards(4).        // 水平分片
    Run(func(ctx context.Context, mp *MultiPool[string]) error {
        // Run 内部已自动创建外部池并注入
        for _, item := range items {
            mp.Submit(ctx, processItem)
        }
        results := mp.Wait()
        return handleResults(results)
    })
```

---

## AutoScale 自动扩缩容

Pool 支持运行时自动扩缩容，根据 busy/concurrency 比率动态调整 worker 数：

```go
err := async.Pool[int]().Context(ctx).
    Worker(4).
    Run(func(ctx context.Context, p *Pool[int]) error {
        // 运行时启用自动扩缩容
        p.EnableAutoScale(&async.AutoScaleConfig{
            MinWorkers:         2,
            MaxWorkers:         100,
            CheckInterval:      5 * time.Second,
            ScaleUpThreshold:   0.7,
            ScaleDownThreshold: 0.2,
            ScaleUpChecks:      3,
            ScaleDownChecks:    5,
            ScaleUpFactor:      1.5,
            ScaleDownFactor:    0.75,
        })

        for _, item := range items {
            p.Submit(ctx, processItem)
        }
        results := p.Wait()
        return handleResults(results)
    })
```

> **⚠️ AutoScale 相关方法**
>
> - `p.EnableAutoScale(config)`：config 为 nil 时自动使用 `DefaultAutoScaleConfig()`
> - `p.DisableAutoScale()`：停止自动扩缩容
> - `p.IsAutoScaleEnabled()`：查询是否已启用
> - `p.Resize(newSize)`：手动调整 worker 数

---

## 辅助函数

### ForEachPool —— 切片元素协程池并发

`async.ForEachPool` 为切片每个元素创建协程池任务，只关心 error，等价于 Pool 版本的 ForEach。

```go
files := []string{"f1.txt", "f2.txt", "f3.txt"}
_, err := async.ForEachPool(ctx, files, func(ctx context.Context, path string) error {
    return os.Remove(path)
}, async.IO())
if err != nil {
    log.Printf("删除失败: %v", err)
}
```

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `ctx` | 上下文 | — |
| `items` | 切片数据 | — |
| `fn` | 处理函数 `func(context.Context, T) error` | — |
| `concurrency` | 并发度（≤0 使用 `IO()`） | `IO()` |

返回值 `*NoResultPool` 是 `*Pool[struct{}]` 的类型别名，提供 `FirstError()` 获取第一个错误。

### Pool[T].Shard(shards) / DefaultShard()

从单个 `Pool[T]` 实例创建 `MultiPool[T]`，继承原 Pool 的所有配置：

```go
p, ctx := pool.New[int](8).WithContext(ctx)

// 按指定分片数创建 MultiPool
mp := p.Shard(4)

// 按 CPU 核数自动分片（最少 2）
mp := p.DefaultShard()

defer mp.Close()
```

| 方法 | 说明 |
|------|------|
| `.Shard(shards)` | 创建 `shards` 个分片的 MultiPool，继承超时、FailFast、环形缓冲等配置 |
| `.DefaultShard()` | 按 `GOMAXPROCS` 自动分片（最少 2） |

`Shard()` 会复制 Pool 的 `timeout`、`submitTimeout`、`maxPending`、`overflowStrat`、`maxResults`、`failFast`、`ringBuf`、`streamCh`、`resultCb`、`autoScale` 等全部运行时配置到所有分片。

---

## 自定义日志

```go
err := async.Pool[int]().
    Logger(myLogger).    // 全局生效
    Run(fn)

err := async.Pool[int]().
    Logger(myLogger).
    DefaultLogger().     // 全局恢复默认
    Run(fn)
```

---

## 常见错误

- **❌ Submit 后忘记 Wait**：`Submit` 只是提交任务到 channel，worker 异步执行。不调用 `Wait()` / `WaitAndClose()` / `Close()` 会导致 goroutine 泄漏。Run 模式会自动 Close，Build/New 模式必须手动处理。

  ```go
  // ❌ 错误：未 Wait
  p, ctx := pool.New[int](8).WithContext(ctx)
  p.Submit(ctx, fn)
  // 没有 Wait → goroutine 泄漏

  // ✅ 正确
  results := p.Wait()
  ```

- **❌ Wait 后继续 Submit**：`Wait()` 一次性使用，调用后通道被标记为已完成，再次 `Submit` 会返回错误。如需复用请调用 `ResetWait()` 或 `Reset()`。

  ```go
  // ✅ 正确：复用 Pool
  results1 := p.Wait()
  p.ResetWait()
  p.Submit(ctx, fn2)
  results2 := p.Wait()
  ```

- **❌ OverflowBlock + 无 SubmitTimeout → 死锁**：`Pool` 结构体零值 `OverflowStrategy = 0 = OverflowBlock`，即**默认溢出策略是阻塞**。如果 `Worker` 全忙且 `MaxPending` 未设置（不限队列），Submit 会一直阻塞而非快速失败。生产环境建议显式设置 `SubmitTimeout` 或 `Overflow(OverflowError)`。

  ```go
  // ❌ 危险：无限阻塞
  p := async.Pool[int](nil).Worker(8).Run(fn)

  // ✅ 生产级：30s 提交超时或错误降级
  async.Pool[int]().Context(ctx).
      Worker(8).SubmitTimeout(30*time.Second).
      Overflow(async.OverflowError).Run(fn)
  ```

- **❌ DefaultOverflow() ≠ 恢复运行时默认**：`DefaultOverflow()` 将 Builder 的 `cfg.Overflow` 设为 `OverflowDrop`（用于环形缓冲的默认溢出策略），但 Pool 实例的**运行时默认溢出策略是 `OverflowBlock`**。这两个是不同的概念：一个是 Builder 层的全局默认值引用，一个是实例的零值行为。不要混淆。

- **❌ PoolBuilder 没有 `.Build()`**：与 GroupBuilder 不同，`PoolBuilder` 只有 `.Run(fn)` 作为终端方法，没有 `.Build()`。需要手动管理 Pool 生命周期时使用 `pool.New[T](n)`。

  ```go
  // ❌ 错误：PoolBuilder 没有 Build
  p := async.Pool[int]().Build()  // 编译错误

  // ✅ 正确：New 手动管理
  p, _ := pool.New[int](8).WithContext(ctx)
  defer p.Close()
  ```

- **❌ MaxResults 溢出静默截断**：`MaxResults` 默认 100_000。如果提交了 150_000 个任务，超出的 50_000 个结果会被静默丢弃。确认任务量后调整或在 `ring buffer + flush` 模式下工作。

  ```go
  // 场景：提交 500K 任务
  async.Pool[int]().Context(ctx).
      Worker(64).
      MaxResults(0).        // 0 = 无限制（确保不丢结果）
      Streaming(4096).      // + 流式消费降低内存峰值
      Run(fn)
  ```

- **❌ Streaming 未消费导致 worker 阻塞**：启用 `Streaming(buf)` 后，结果写入 stream channel。如果不消费 `StreamResults()`，channel `buf` 写满后 worker 会阻塞等待消费方读取。必须配合消费者 goroutine。

  ```go
  // ✅ 正确：启用 Streaming 后立即消费
  err := async.Pool[int]().Context(ctx).
      Streaming(1024).Run(func(ctx context.Context, p *Pool[int]) error {
          go func() {
              for r := range p.StreamResults() {
                  handle(r)
              }
          }()
          // ... Submit 任务 ...
          return nil
      })
  ```

- **❌ Logger 全局副作用**：`.Logger(l)` 影响**全局**所有 Pool/Group/Retry 等组件（内部调用 `core.SetLogger(l)`）。在测试或需要隔离日志时，记得用 `.DefaultLogger()` 恢复。

- **❌ EnableAutoScale(config) 传入 nil 的歧义**：`config == nil` 时自动使用 `DefaultAutoScaleConfig()`，也就是说 `EnableAutoScale(nil)` 会启用扩缩容而非禁用。要禁调用 `DisableAutoScale()`。

- **❌ RingBuffer 不 Flush 导致结果丢失**：环形缓冲中的结果需要通过 `Flush()` 定时取出，否则被新结果覆盖后永久丢失。长期运行的 Pool 必须定期 Flush。