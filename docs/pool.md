# Pool（协程池）文档

## 概述

从外部模块使用 `Run` 时，回调参数分别使用
`*async.PoolInstance[T]`、`*async.MultiPool[T]` 和
`*async.ShardedPool[T]`，适用于 `Pool`、`PoolMulti` 和
`PoolSharded` 构建器。

`Pool[T]` 是泛型协程池，复用固定数量的 goroutine 处理高频并发任务。通过 `Pool[T]()` Builder 链式构建，终端方法 `Run(fn)` 自动管理生命周期。

**三种 Pool 入口**：

| 入口 | 说明 | 终端方法 |
|------|------|----------|
| `async.Pool[T]()` | 单协程池 | `.Run(fn)` |
| `async.PoolMulti[T]()` | 水平分片协程池（MultiPool） | `.Run(fn)` |
| `async.PoolSharded[T]()` | 分片协程池构建器（ShardPool，详见 Shard 文档） | `.Run(fn)` |

> **⚠️ PoolBuilder 只有 `.Run(fn)` 作为终端方法，没有 `.Build()`**
>
> `PoolBuilder[T]` 的唯一终端方法是 `.Run(fn func(ctx context.Context, p *async.PoolInstance[T]) error) error`。fn 回调接收一个已创建好的 `*async.PoolInstance[T]`，fn 返回后自动 Close 池。这与 GroupBuilder 不同——GroupBuilder 同时提供 `.Build()` 和 `.Run(fn)` 两种终端方法。
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
| `.FailFast()` | 启用 FailFast；Run 回调收到可取消的上下文，应将其传给 Submit | 关闭 |
| `.DefaultFailFast()` | 关闭 FailFast | — |
| `.MaxPending(n)` | 最大排队任务数 | 无限制（0 = 不限） |
| `.DefaultMaxPending()` | 清除最大排队限制（设为 0） | — |
| `.OverflowBlock()` | 队列满时阻塞等待（**默认**） | 默认 |
| `.OverflowDrop()` | 队列满时丢弃新任务 | — |
| `.OverflowError()` | 队列满时返回 `ErrQueueOverflow` | — |
| `.Overflow(s)` | 设置 Overflow 策略（Block/Drop/Error） | `OverflowBlock`（Pool 结构体零值=0=Block） |
| `.DefaultOverflow()` | 显式将任务溢出策略设为 `OverflowDrop` | — |
| `.RingBuf(cap)` | 环形缓冲容量 | 0（禁用） |
| `.DefaultRingBuf()` | 清除构建器覆盖，使用全局默认环形缓冲配置 | — |
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
> `fn func(ctx context.Context, p *async.PoolInstance[T]) error`：`ctx` 是 builder 上设置的上下文，`p` 是已创建的协程池实例。fn 返回后自动 `Close` 池。

---

## MultiPoolBuilder 链式方法速查表

`async.PoolMulti[T]()` 返回 `*MultiPoolBuilder[T]`，提供以下链式配置方法：

### MultiPoolBuilder 链式配置方法

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `context.Background()` |
| `.Shards(n)` | 水平分片数（≤1 不启用分片） | 无分片（需显式设置） |
| `.DefaultShards()` | 恢复默认设置，使用一个池 | — |
| `.Worker(n)` | 每分片 worker 数量（n≤0 自动修正） | `IO()`（`NumCPU×2`） |
| `.DefaultWorker()` | 使用 IO 并发度 `core.IO()`（`NumCPU×2`） | — |
| `.Timeout(d)` | 单任务超时 | `core.GetDefaultTimeout()`（全局默认 30s） |
| `.DefaultTimeout()` | 恢复为全局默认超时（`core.GetDefaultTimeout()`，30s） | — |
| `.SubmitTimeout(d)` | 提交超时 | 无限制（阻塞等待） |
| `.DefaultSubmitTimeout()` | 清除提交超时（设为 0 = 阻塞等待） | — |
| `.FailFast()` | 内部创建池时启用 FailFast；Run 回调收到可取消的上下文，应将其传给 Submit | 关闭 |
| `.DefaultFailFast()` | 关闭 FailFast | — |
| `.MaxPending(n)` | 每分片最大排队任务数 | 无限制 |
| `.DefaultMaxPending()` | 清除最大排队限制（设为 0） | — |
| `.OverflowBlock()` | 队列满时阻塞等待（**默认**） | 默认 |
| `.OverflowDrop()` | 队列满时丢弃新任务 | — |
| `.OverflowError()` | 队列满时返回错误 | — |
| `.Overflow(s)` | 设置 Overflow 策略 | `OverflowBlock`（Pool 结构体零值=0=Block） |
| `.DefaultOverflow()` | 显式设为 `OverflowDrop` | — |
| `.RingBuf(cap)` | 每分片环形缓冲容量 | 0（禁用） |
| `.DefaultRingBuf()` | 清除构建器覆盖，使用全局默认环形缓冲配置 | — |
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
> `fn func(ctx context.Context, mp *async.MultiPool[T]) error`：`ctx` 是 builder 上设置的上下文，`mp` 是已创建的分片协程池实例。fn 返回后自动 `Close` 所有分片。

---

## Pool 实例方法速查表

以下方法在 `Run` 回调中的 `p *async.PoolInstance[T]` 实例上可用：

### 任务提交

| 方法 | 签名 | 说明 |
|------|------|------|
| `Submit` | `func(ctx, fn) error` | 阻塞提交任务，等待有空闲 worker |
| `SubmitAt` | `func(index, ctx, fn) error` | 指定索引位置提交，结果在 results[index] |
| `TrySubmit` | `func(ctx, fn) error` | 非阻塞提交，满时返回 `ErrSubmitTimeout` |

### 完成回调与结果转换

以下方法主要供适配器使用，也可通过 `PoolInstance[T]` 调用。`fn` 的类型为 `func(context.Context) (T, error)`。

| 方法 | 说明 |
|------|------|
| `SubmitWithCompletion(ctx, fn, complete)` | 提交任务，并将终态结果传给 `complete func(async.Result[T])`，包括拒绝、溢出和执行前取消。 |
| `SubmitWithCompletionTimeout(ctx, fn, complete, timeout)` | 同上；正数 `timeout` 覆盖此任务的执行超时，非正数使用池配置。 |
| `SubmitAtWithCompletion(index, ctx, fn, complete)` | 在指定索引提交，并将终态结果传给完成回调。 |
| `SubmitAtWithResultTransform(index, ctx, fn, transform)` | 在记录指定索引的结果前调用 `transform func(async.Result[T]) async.Result[T]`；传入 `nil` 不转换。 |

```go
err := p.SubmitWithCompletion(ctx, work, func(result async.Result[int]) {
    if result.Err != nil {
        log.Printf("任务失败: %v", result.Err)
    }
})
if err != nil {
    log.Printf("提交失败: %v", err)
}
```

完成回调可能在提交期间同步执行，也可能在任务结束时执行；每次提交的回调至多执行一次。回调必须保持短暂，不能等待或关闭同一个池，也不要把它作为可以阻塞的通知通道。完成回调发生 panic 时会被恢复并记录；转换函数发生 panic 时会转为结果错误。提交返回错误时完成回调仍可能已经执行，避免在两个路径重复处理同一结果。

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
| `CloseByIdle(d)` | — | 最多等待 d 进入空闲状态，然后同步关闭；任务未结束时总耗时可能超过 d |
| `Reset()` | `(*async.PoolInstance[T], error)` | 完整重置状态以便复用 |
| `ResetWait()` | `*async.PoolInstance[T]` | 重置 waited 状态，允许继续 Submit+Wait |

### 状态查询

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Size()` | `int` | 当前 worker 数量 |
| `Active()` | `int` | 当前活跃（已启动未结束）的 goroutine 数 |
| `Busy()` | `int` | 正在执行任务的 goroutine 数 |
| `Pending()` | `int` | 排队中的任务数 |
| `QueueDepth()` | `int` | 队列深度 |
| `TotalCount()` | `int64` | 记录的提交次数，包括已记录的拒绝和丢弃；重复索引分别计数 |
| `OkCount()` | `int64` | 已成功完成的任务数，不包含尚未完成的任务和稀疏索引空位 |
| `FailCount()` | `int64` | 失败任务数，包括已记录的拒绝，以及执行中因 FailFast 返回取消错误的任务 |
| `HasError()` | `bool` | 是否有错误 |
| `Stats()` | `PoolStats` | 运行时统计快照 |

> **注意**：`OkCount()` 只统计已完成且成功的任务。运行中、失败、取消、提交被拒绝或 panic 的任务不计入；若要获取最终批次统计，请先 `Wait()`。调用 `Reset()` 后计数会清零。

```go
results := p.Wait()
fmt.Printf("本批完成: total=%d ok=%d fail=%d\n", p.TotalCount(), p.OkCount(), p.FailCount())
```

### 错误提取

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Errors()` | `[]error` | 主结果中的非 nil 错误 |
| `FirstError()` | `error` | 主结果中的第一个错误 |
| `JoinErrors()` | `error` | 合并主结果中的错误（`errors.Join`） |
| `Values()` | `[]T` | 提取主结果中成功的值 |

### 运行时配置（实例级）

| 方法 | 签名 | 说明 |
|------|------|------|
| `WithTimeout(d)` | `*async.PoolInstance[T]` | 设置任务超时（d>0 生效） |
| `WithSubmitTimeout(d)` | `*async.PoolInstance[T]` | 设置提交超时（d>0 生效） |
| `WithFailFast(ctx)` | `(*async.PoolInstance[T], context.Context)` | 启用 FailFast，返回新的 ffCtx |
| `WithFFCtx(ctx)` | `(*async.PoolInstance[T], context.Context)` | `WithFailFast` 的缩写 |
| `WithStreaming(bufSize)` | `*async.PoolInstance[T]` | 启用流式结果（bufSize≤0 自动=Worker×2） |
| `WithResultCallback(fn)` | `*async.PoolInstance[T]` | 设置每个任务完成时的回调；回调 panic 会被捕获并记录，不改变任务结果 |
| `WithRingBuffer(cap, overflow)` | `*async.PoolInstance[T]` | 启用环形缓冲 |
| `WithMaxPending(n)` | `*async.PoolInstance[T]` | 设置最大排队任务数 |
| `WithOverflow(strategy)` | `*async.PoolInstance[T]` | 设置溢出策略 |
| `WithMaxResults(n)` | `*async.PoolInstance[T]` | 设置最大结果数（≤0=无限） |
| `WithTraceID(ctx)` | `(*async.PoolInstance[T], context.Context)` | 注入 TraceID 并返回新 ctx |
| `WithContext(ctx)` | `(*async.PoolInstance[T], context.Context)` | 设置池级上下文 |
| `MergeFailFastCancel(cancel)` | `*async.PoolInstance[T]` | 合并外部 CancelFunc |

`WithResultCallback` 同步执行，多个任务的回调可能并发；任务未执行即被丢弃时，回调也可能在提交或关闭池的 goroutine 中执行。回调需自行保护共享数据，保持轻量，且不能同步调用所属池的 `Wait`、`Close` 或 `CloseAndWait`，否则可能等待自身而死锁。多池的回调同样不能同步等待或关闭所属多池及其分片。

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
| `Resize(newSize)` | `int` | 调整 worker 数，返回新增或移除的数量；关闭后调用返回 0 |
| `ResizeAndWaitTimeout(newSize, timeout)` | — | 调整 worker 数并等待超时清理 |
| `EnableAutoScale(config)` | — | 启用自动扩缩容（config=nil 使用默认配置） |
| `DisableAutoScale()` | — | 停止自动扩缩容 |
| `IsAutoScaleEnabled()` | `bool` | 是否已启用自动扩缩容 |
| `Ctx()` | `context.Context` | 获取池级上下文 |

---

## MultiPool 实例方法速查表

以下方法在 `Run` 回调中的 `mp *async.MultiPool[T]` 实例上可用：

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
| `WaitTimeout(d)` | `([]Result[T], bool)` | 带超时等待，ok=false 表示超时 |
| `WaitContext(ctx)` | `([]Result[T], bool)` | 带 context 等待，ok=false 表示 ctx 取消 |
| `WaitAndClose()` | `[]Result[T]` | 等待完成并关闭所有分片 |
| `Close()` | — | 关闭所有分片 |

多池的结果按分片合并，不保证跨分片的全局提交顺序。各分片启用结果溢出缓冲时，还需注意溢出结果按缓冲写入顺序追加。

### 查询

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `ShardCount()` | `int` | 分片数量 |
| `GetShard(idx)` | `*async.PoolInstance[T]` | 获取第 idx 个分片的 Pool 实例，越界返回 nil |

### 运行时配置（代理到所有分片）

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `WithTimeout(d)` | `*async.MultiPool[T]` | 为所有分片设置任务超时 |
| `WithSubmitTimeout(d)` | `*async.MultiPool[T]` | 为所有分片设置提交超时 |
| `WithStreaming(bufSize)` | `*async.MultiPool[T]` | 为所有分片启用流式 |
| `WithResultCallback(fn)` | `*async.MultiPool[T]` | 为所有分片设置结果回调 |
| `WithRingBuffer(cap, overflow)` | `*async.MultiPool[T]` | 为所有分片启用环形缓冲 |
| `WithMaxPending(n)` | `*async.MultiPool[T]` | 为所有分片设置最大排队 |
| `WithOverflow(strategy)` | `*async.MultiPool[T]` | 为所有分片设置溢出策略 |
| `WithMaxResults(n)` | `*async.MultiPool[T]` | 为所有分片设置最大结果数 |

### 流式与冲洗

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `StreamResults()` | `<-chan Result[T]` | Fan-in 所有分片的流式结果 channel |
| `Flush(maxPerShard)` | `[]Result[T]` | 从所有分片环形缓冲各取 maxPerShard 个结果 |

获取合并流后应持续读取直到通道关闭；若停止消费且合并缓冲填满，分片转发协程会阻塞。流式通道的缓冲和溢出行为仍由各分片配置决定。

### 统计汇总

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `TotalActive()` | `int` | 汇总各分片活跃任务数 |
| `TotalBusy()` | `int` | 汇总各分片忙碌任务数 |
| `TotalPending()` | `int` | 汇总各分片排队任务数 |
| `TotalWorkerCount()` | `int` | 汇总各分片 worker 总数 |
| `TotalCount()` | `int64` | 汇总所有分片任务总数 |
| `TotalOkCount()` | `int64` | 汇总各分片成功数 |
| `TotalFailCount()` | `int64` | 汇总各分片失败数 |

> **并发等待**：同一个 `MultiPool` 上重叠调用 `Wait`、`WaitContext`、`WaitTimeout` 或 `WaitAndClose` 时，只有先进入的等待收集分片结果；其他等待返回空结果，带布尔值的等待返回 `false`。重叠的 `WaitAndClose` 仍会关闭所有分片。单独等待或复位分片时，需要由调用方协调，避免与多池等待并发。

> **建议**：`MultiPool.TotalOkCount()` 汇总所有分片的已完成成功任务；高并发提交期间读取的是实时快照，最终值应在等待结束后读取。

> **💡 取消传播**：`MultiPool.WaitTimeout` → `Pool.WaitContext` → 任务 ctx 取消。调用方主动取消模式与 Pool 一致，详见 [调用者主动取消模式](#调用者主动取消模式)。

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
| 默认环形缓冲容量 | `core.GetDefaultRingBufferCap()` | `core.SetDefaultRingBuffer(n, strategy)` | **0**（不启用） | `RingBuf(0)` 不覆盖全局配置 |
| 默认溢出策略 | `core.GetDefaultOverflowStrategy()` | `core.SetDefaultRingBuffer(n, s)` | **OverflowDrop** | 环形缓冲满时策略 |

### 二层：Builder 级默认值

| `Default*` 方法 | 源码行为 | 适用于 |
|-----------------|----------|--------|
| `.DefaultWorker()` | `b.cfg.Size = core.IO()`（`NumCPU×2`） | PoolBuilder, MultiPoolBuilder |
| `.DefaultTimeout()` | `b.cfg.Timeout = 0`（applyConfig 不覆盖，池使用全局默认 `core.GetDefaultTimeout()` 30s） | PoolBuilder, MultiPoolBuilder |
| `.DefaultSubmitTimeout()` | `b.cfg.SubmitTimeout = 0`（不限时） | PoolBuilder, MultiPoolBuilder |
| `.DefaultFailFast()` | `b.cfg.FailFast = false` | PoolBuilder, MultiPoolBuilder |
| `.DefaultMaxPending()` | `b.cfg.MaxPending = 0`（不限时） | PoolBuilder, MultiPoolBuilder |
| `.DefaultOverflow()` | `b.cfg.Overflow = OverflowDrop` | PoolBuilder, MultiPoolBuilder |
| `.DefaultRingBuf()` | `b.cfg.RingBufCap = 0`（使用全局默认配置） | PoolBuilder, MultiPoolBuilder |
| `.DefaultMaxResults()` | `b.cfg.MaxResults = -1`（-1 = 使用全局默认 100K） | PoolBuilder, MultiPoolBuilder |
| `.DefaultStreaming()` | `b.cfg.Streaming = 0`（不启用） | PoolBuilder, MultiPoolBuilder |
| `.DefaultShards()` | `b.shards = 0`，使用一个池 | MultiPoolBuilder |
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
| 海量任务（百万/千万） | **Pool + MaxResults + RingBuffer** | 限制主结果存储；及时消费环形缓冲，缓冲满时结果可能丢失 |

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
    Run(func(ctx context.Context, p *async.PoolInstance[MyType]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[MyType]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[MyType]) error {
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
    Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
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
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
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
    Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
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

非阻塞提交，任务队列满时立即返回 `ErrSubmitTimeout`。设置 `MaxPending` 后触发溢出时，`OverflowError` 返回 `ErrQueueOverflow`，`OverflowDrop` 返回 `nil`；两者都会记录溢出错误，可通过 `Wait()` 或 `Errors()` 查看（结果存储上限可能限制保留数量）。

```go
err := p.TrySubmit(ctx, fn)
if errors.Is(err, async.ErrSubmitTimeout) {
    log.Println("队列已满，任务被丢弃")
}
```

### SubmitAt

在指定索引位置提交任务；结果保留在主结果存储时，`Wait()` 返回的切片使用对应位置。超过 `MaxResults` 后写入环形缓冲的结果按缓冲写入顺序追加，不能按原索引定位：

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

阻塞等待所有已提交任务完成。主结果按提交顺序排列；配置正数 `MaxResults` 和环形缓冲时，尚未被 `Flush` 取出的溢出结果按写入缓冲的顺序追加，缓冲满时可能丢失：

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

> **⚠️ 超时/取消后会向所有任务发送取消信号**
>
> `WaitTimeout` 超时和 `WaitContext` ctx 取消后，池会取消已登记的任务，并取消随后登记的在途任务，然后返回当前结果。未完成任务和结果流的清理继续在后台进行；`ResetWait` 和 `Reset` 会等待清理完成。任务需要自行响应 context 才能尽早结束。

#### 调用者主动取消模式

结合 builder 链式配置和 `WaitContext`，调用方可在外部主动取消所有正在执行的任务：

```go
// ① 由调用方掌控取消时机
parentCtx, cancel := context.WithCancel(context.Background())

// ② 链式创建 Pool：绑定 parentCtx，拿到任务用的 taskCtx
p, taskCtx := async.Pool[int]().Context(ctx).
    Worker(6).
    WithContext(parentCtx)        // ← 绑定父 context
// 注意：Run 模式不需要 defer Close，Run 返回时自动 Close

// ③ 在 Run 回调中用 taskCtx 提交任务
err := p.Run(func(ctx context.Context, pool *async.PoolInstance[int]) error {
    for i := 0; i < 10; i++ {
        pool.Submit(taskCtx, func(ctx context.Context) (int, error) {
            select {
            case <-ctx.Done():
                return 0, ctx.Err()   // 收到取消信号，尽早返回
            case <-time.After(10 * time.Second):
                return 42, nil
            }
        })
    }

    // ④ 另一 goroutine 主动取消
    go func() {
        time.Sleep(100 * time.Millisecond)
        cancel() // ← 调用者主动 cancel，取消链传播到所有 taskCtx
    }()

    // ⑤ WaitContext 兜底（防止 cancel 传播异常导致永久阻塞）
    waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer waitCancel()
    results, ok := pool.WaitContext(waitCtx)
    // cancel() 触发 → 任务 ctx.Done() → 任务返回 → WaitContext 正常结束
    _ = results
    _ = ok
    return nil
})
```

#### 超时自动取消模式

通过 `WaitTimeout` 或 `context.WithTimeout` + `WaitContext` 实现超时自动停止：

```go
// 方式一：WaitTimeout 直接传超时
results, ok := p.WaitTimeout(100 * time.Millisecond)

// 方式二：WaitContext + context.WithTimeout
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()
results, ok := p.WaitContext(ctx)
```

> 两种方式等效：超时后取消信号都会传播到所有正在执行的任务。

### Close / CloseAndWait / CloseAndWaitTimeout

| 方法 | 说明 |
|------|------|
| `Close()` | 发出关闭信号，等待 worker 退出并清理未执行的任务；并发调用也会等待关闭完成 |
| `CloseAndWait()` | 关闭池并等待清理完成，再调用 `Wait()` |
| `CloseAndWaitTimeout(d)` | 发出关闭信号并等待；超时后返回可等待 worker 退出的通道 |

关闭不会强制终止正在执行的任务，因此任务必须自行响应取消或结束。关闭可能丢弃尚未执行的任务。首次调用 `CloseAndWaitTimeout` 返回 `ok == false` 时，返回的通道关闭表示 worker 已退出，队列和结果清理可能仍在进行；需要等待完整清理时可调用 `Close()`。如果关闭已经进行，超时返回的通道在该次关闭清理完成后关闭。重复调用会等待已有关闭流程，超时内完成时返回 `true, nil`。

### ResetWait / Reset

| 方法 | 说明 |
|------|------|
| `ResetWait()` | 重置 waited 状态，允许继续 Submit+Wait |
| `Reset()` | 完整重置，允许继续 Submit+Wait |

`Wait` 会取消通过 `WithContext` 或 `WithFailFast` 创建的池上下文。`ResetWait` 不重建该上下文；如果下一批任务需要使用池上下文，应以仍有效的父上下文重新调用 `WithContext` 或 `WithFailFast`，并使用新返回的上下文提交任务。不要将已取消的旧上下文作为新父上下文。

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
| `OkCount()` | 成功任务数 |
| `FailCount()` | 失败任务数 |
| `HasError()` | 是否有错误 |
| `Stats()` | 运行时统计快照 |

### 错误提取

| 方法 | 说明 |
|------|------|
| `Errors()` | 主结果中的非 nil 错误 |
| `FirstError()` | 主结果中的第一个错误 |
| `JoinErrors()` | 合并主结果中的错误 |
| `Values()` | 提取主结果中成功的值 |

这些方法只读取主结果存储，不包括超过 `MaxResults` 的溢出结果。`HasError()` 统计所有失败，因此它可能为 true，而 `Errors()` 为空；需要检查 `Wait()` 和 `Flush()` 返回的结果，并处理缓冲区满时可能发生的丢失。

```go
results := p.Wait()
var errs []error
for _, r := range results {
    if r.Err != nil {
        errs = append(errs, r.Err)
    }
}
return errors.Join(errs...)
```

---

## 流式结果消费

通过 `.Streaming(buf)` 启用流式结果，结果通过 channel 实时发送：

```go
err := async.Pool[string]().Context(ctx).
    Worker(8).
    Streaming(1024).
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
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

> **Streaming 与 Wait**
>
> 启用流式消费后，`Wait()` 仍返回保存的结果，受 `MaxResults` 和环形缓冲配置限制。流式 channel 的丢弃不影响已保存的结果。
>
> **⚠️ 消费者慢时静默丢弃**
>
> stream channel 满时采用非阻塞写入，结果会被静默丢弃。通过 `p.StreamDropped()` 监控丢弃量。

---

## 环形缓冲

环形缓冲可配合 `MaxResults` 限制主结果存储，并通过 `Flush` 分批消费溢出结果。需要在任务执行期间及时消费；缓冲容量不足时结果仍可能丢失：

```go
err := async.Pool[string]().Context(ctx).
    Worker(8).
    MaxResults(10000).
    RingBuf(10000).
    OverflowDrop().
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
        for i := 0; i < 10_000_000; i++ {
            p.Submit(ctx, fn)
            if i%1000 == 0 {
                processBatch(p.Flush(5000))
            }
        }

        // Wait 等待已提交任务，并返回主结果及尚未 Flush 的溢出结果。
        processBatch(p.Wait())
        return nil
    })
```

> **⚠️ RingBuf 不限制 results 切片**
>
> 环形缓冲不能禁用 results 存储。设置正数 `MaxResults(n)` 可限制 results 存储；`MaxResults(0)` 表示无限制。超过上限的结果可通过环形缓冲读取，但容量不足时仍可能丢失。

---

## 背压控制

通过 `MaxPending` + `Overflow` 策略控制队列大小，防止任务堆积导致 OOM：

```go
err := async.Pool[string]().Context(ctx).
    Worker(8).
    MaxPending(1000).
    OverflowError().
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[string]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[string]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[int]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[int]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[string]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[int]) error {
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
    Run(func(ctx context.Context, mp *async.MultiPool[int]) error {
        for _, item := range items {
            mp.Submit(ctx, processItem)
        }
        results := mp.Wait()

        fmt.Printf("总 worker: %d, 总任务: %d, 成功: %d, 失败: %d\n",
            mp.TotalWorkerCount(),
            mp.TotalCount(),
            mp.TotalOkCount(),
            mp.TotalFailCount())
        return nil
    })
```

### 注入外部池

```go
err := async.PoolMulti[string]().Context(ctx).
    Worker(8).
    Shards(4).        // 水平分片
    Run(func(ctx context.Context, mp *async.MultiPool[string]) error {
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
    Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
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
>   启用后重复调用不会更改当前配置；要应用新配置，先禁用再启用。配置会在启用时复制。
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

返回值 `*NoResultPool` 是 `*async.PoolInstance[struct{}]` 的类型别名，提供 `FirstError()` 获取第一个错误。

### Pool[T].Shard(shards) / DefaultShard()

在 `Pool` 构建器回调中，可从池实例创建 `MultiPool`：

```go
err := async.Pool[int]().Context(ctx).Worker(8).
    Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
        mp := p.Shard(4) // 或使用 p.DefaultShard()
        defer mp.Close()
        if err := mp.Submit(ctx, func(context.Context) (int, error) {
            return 42, nil
        }); err != nil {
            return err
        }
        results := mp.Wait()
        _ = results
        return nil
    })
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
  err := async.Pool[int]().Worker(8).Run(fn)

  // ✅ 生产级：30s 提交超时或错误降级
  async.Pool[int]().Context(ctx).
      Worker(8).SubmitTimeout(30*time.Second).
      Overflow(async.OverflowError).Run(fn)
  ```

- **❌ DefaultOverflow() ≠ 恢复运行时默认**：`DefaultOverflow()` 显式将任务溢出策略设为 `OverflowDrop`；新建 Pool 的任务溢出策略是 `OverflowBlock`。环形缓冲的溢出策略由 `SetDefaultRingBuffer` 单独配置，不受此方法控制。

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
      Streaming(4096).      // 同时启用流式消费，结果仍保存在 results 中
      Run(fn)
  ```

- **❌ Streaming 消费不及时导致结果丢弃**：stream channel 写满后，新结果不会阻塞 worker，而是从流中丢弃。应及时消费并通过 `StreamDropped()` 监控。

  ```go
  // ✅ 正确：启用 Streaming 后立即消费
  err := async.Pool[int]().Context(ctx).
      Streaming(1024).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
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

- **❌ RingBuffer 不及时消费导致结果丢失**：缓冲区满时，`OverflowDrop` 覆盖最旧结果，其他策略拒绝新写入；应在任务执行期间使用 `Flush()` 消费，并按需配置 `MaxResults` 和缓冲容量。`RingBufDropped()` 只统计被覆盖的结果。
