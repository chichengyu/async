# Shard（分片分发）文档

## 概述

Shard 模块提供分片化并发，将任务按 key 路由到不同的 Pool/Group 实例，适合按用户、租户分片隔离的场景。

**核心特性**:
- RoundRobin（轮询）和 Hash（FNV 哈希）两种路由策略
- 每分片独立 Pool/Group 实例，故障隔离
- 流式消费、环形缓冲、背压控制等高级特性全支持
- 高并发竞态安全

> **⚠️ 资源复用**
>
> 每个 ShardedPool / ShardedGroup 管理 N 个底层 Pool / Group，worker 总数是分片数 × 每分片 worker 数，创建时注意总 goroutine 数量。
>
> **⚠️ Close 必须关闭所有分片**
>
> `ShardedPool.Close()` 会逐一关闭所有分片 Pool，`ShardedGroup` 同理。忘记 Close 会导致底层 goroutine 泄漏。
>
> **⚠️ Hash 路由不保证均匀**
>
> Hash 策略依赖 key 的哈希分布，如果 key 分布不均（如少量热点 key），部分分片会过载。
>
> **⚠️ RoundRobin 与 Keyed**
>
> `RoundRobin` 在不同 Submit 中轮询不同分片。同一次 `Wait()` 返回的是全部分片结果的**合并视图**，不是单分片。`SubmitKeyed` 基于 key 哈希固定路由到同一分片。

**与 Pool 的关系**:

| 特性 | Pool | ShardedPool |
|------|------|-------------|
| 实例数 | 1 个 | N 个 |
| 路由 | 无 | RoundRobin / Hash |
| 隔离性 | 低（共享） | 高（每分片独立） |
| Worker 总数 | workerNum | N × workerNum |
| 适用场景 | 通用 | 多租户、大流量隔离 |

---

## 目录

- [类型速查](#类型速查)
  - [ShardedPool 类型 / ShardedGroup 类型 / 枚举](#shardedpool-类型)
- [ShardedPool（分片协程池）](#shardedpool分片协程池)
  - [创建：DefaultShardedPool / NewShardedPool / ShardPoolConfig](#创建)
  - [基础 API：ShardCount / GetShard / Submit / SubmitAt / TrySubmit](#基础-api)
  - [Keyed API：SubmitKeyed / TrySubmitKeyed / SubmitBatch / TrySubmitBatch](#submitkeyed)
  - [等待结果：Wait / WaitAndClose / Close / Reset](#等待结果)
  - [代理配置：ResizePerShard / WithTimeout / WithSubmitTimeout](#代理配置方法)
  - [流式配置：WithFailFast / WithStreaming / WithResultCallback](#withfailfast)
  - [背压配置：WithRingBuffer / WithMaxPending / WithOverflow](#withringbuffer)
  - [统计聚合：ShardStats / TotalFailCount / TotalSuccessCount](#统计聚合)
  - [统计聚合：TotalActive / TotalBusy / TotalPending / TotalWorkerCount / Flush](#totalactive)
- [ShardedGroup（分片任务组）](#shardedgroup分片任务组)
  - [创建：DefaultShardedGroup / NewShardedGroup / ShardGroupConfig](#创建-1)
  - [基础 API：ShardCount / GetShard / Go / GoAt / GoKeyed / GoBatch](#基础-api-1)
  - [等待结果：Wait / WaitTimeout / WaitContext / Reset](#等待结果-1)
  - [代理配置：WithTimeout / WithSubmitTimeout / WithFailFast / WithFFCtx](#代理配置方法-1)
  - [流式配置：WithStreaming / WithResultCallback](#withstreaming-1)
  - [统计聚合：TotalFailCount / TotalSuccessCount / TotalActive / TotalBusy / TotalConcurrency](#统计聚合-1)
- [路由策略](#路由策略)
  - [RoundRobin / Hash](#roundrobin轮询)
- [分片数建议 / 完整示例](#分片数建议)

## 类型速查

### ShardedPool 类型

| 类型/函数 | 说明 |
|-----------|------|
| `ShardedPool[T]` | 分片协程池 |
| `ShardPoolConfig[T]` | 分片池配置 |
| `DefaultShardedPool[T]()` | 默认配置创建分片池 |
| `NewShardedPool[T](cfg)` | 自定义配置创建分片池 |
| `SubmitBatchResult` | 批量提交结果 |

### ShardedGroup 类型

| 类型/函数 | 说明 |
|-----------|------|
| `ShardedGroup[T]` | 分片任务组 |
| `ShardGroupConfig[T]` | 分片 Group 配置 |
| `DefaultShardedGroup[T]()` | 默认配置创建分片 Group |
| `NewShardedGroup[T](cfg)` | 自定义配置创建分片 Group |
| `GoBatchResult` | 批量分发结果 |

### 枚举

| 常量 | 值 | 说明 |
|------|-----|------|
| `RoundRobin` | 0 | 轮询分发（默认） |
| `Hash` | 1 | FNV 哈希分发 |

---

## ShardedPool（分片协程池）

### 创建

#### DefaultShardedPool

```go
// 语法
func DefaultShardedPool[T any]() *ShardedPool[T]
```

使用默认配置创建分片池：4 分片、每分片 IO 并发度、RoundRobin 分发。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*ShardedPool[T]` | 分片池指针 | 已创建但是否需 defer Close() |

```go
sp := shard.DefaultShardedPool[string]()
defer sp.Close()
```

---

#### NewShardedPool

```go
// 语法
func NewShardedPool[T any](cfg ShardPoolConfig[T]) *ShardedPool[T]
```

自定义配置创建分片池。cfg 中零值字段会使用默认值。

| 参数 | 类型 | 说明 |
|------|------|------|
| `cfg` | `ShardPoolConfig[T]` | 分片池配置 |
| 返回值 | `*ShardedPool[T]` | 分片池指针 |

```go
// 8 分片，每分片 16 worker，Hash 分发
sp := shard.NewShardedPool(shard.ShardPoolConfig[string]{
    Shards:       8,
    SizePerShard: 16,
    Distribution: shard.Hash,
    KeyFn:        func(s string) uint64 { return uint64(len(s)) },
})
defer sp.Close()
```

---

#### ShardPoolConfig

```go
type ShardPoolConfig[T any] struct {
    Shards       int
    SizePerShard int
    Distribution Distribution
    KeyFn        func(T) uint64
}
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `Shards` | `int` | 4 | 分片数 |
| `SizePerShard` | `int` | `core.IO()` | 每分片 worker 数 |
| `Distribution` | `Distribution` | `RoundRobin` | 分发策略 |
| `KeyFn` | `func(T) uint64` | nil | Hash 分发时的自定义 key 函数 |

---

### 基础 API

#### ShardCount

```go
// 语法
func (sp *ShardedPool[T]) ShardCount() int
```

返回分片数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | 分片数 | 创建时指定的 Shards 值 |

```go
fmt.Println(sp.ShardCount()) // 4
```

---

#### GetShard

```go
// 语法
func (sp *ShardedPool[T]) GetShard(idx int) *pool.Pool[T]
```

获取指定分片的 Pool 实例，用于直接调用 Pool 的原生方法。idx 越界返回 nil。

| 参数 | 类型 | 说明 |
|------|------|------|
| `idx` | `int` | 分片索引，范围 [0, ShardCount()) |
| 返回值 | `*pool.Pool[T]` | 分片 Pool 指针，越界返回 nil |

```go
shard0 := sp.GetShard(0)
shard0.WithTimeout(5 * time.Second)
```

---

#### Submit

```go
// 语法
func (sp *ShardedPool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error
```

将任务轮询分发到某个分片提交。分发策略取决于 `Distribution` 配置：RoundRobin 自动轮询，Hash 按 KeyFn 哈希。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回值 | `error` | ErrPoolClosed / ErrSubmitTimeout / 等 Pool 错误 |

```go
err := sp.Submit(ctx, func(ctx context.Context) (string, error) {
    return process(ctx), nil
})
```

---

#### SubmitAt

```go
// 语法
func (sp *ShardedPool[T]) SubmitAt(shardIdx, index int, ctx context.Context, fn func(context.Context) (T, error)) error
```

将任务提交到指定分片的指定索引位置。shardIdx 越界时自动 fallback 到轮询分发。

| 参数 | 类型 | 说明 |
|------|------|------|
| `shardIdx` | `int` | 目标分片索引 |
| `index` | `int` | 在分片结果中的位置索引 |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回值 | `error` | Pool 提交错误 |

```go
// 提交到分片 2，结果位置索引 42
err := sp.SubmitAt(2, 42, ctx, fn)
```

---

#### TrySubmit

```go
// 语法
func (sp *ShardedPool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error
```

非阻塞分发提交。与 Submit 的区别：不会等待空闲 worker，池满时立即返回错误。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回值 | `error` | 池满时返回对应错误 |

```go
err := sp.TrySubmit(ctx, fn)
if err != nil {
    // 池满，可以降级处理
}
```

---

#### SubmitKeyed

```go
// 语法
func (sp *ShardedPool[T]) SubmitKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error
```

按 key 哈希分发到固定分片，保证**同一 key 的所有任务始终路由到同一分片**。适用于需要按用户/租户串行处理的场景。

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 哈希 key，如用户 ID、租户 ID |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回值 | `error` | Pool 提交错误 |

```go
// 用户 user-123 始终路由到同一分片
err := sp.SubmitKeyed("user-123", ctx, func(ctx context.Context) (*Result, error) {
    return processUser(ctx, "user-123"), nil
})
```

---

#### TrySubmitKeyed

```go
// 语法
func (sp *ShardedPool[T]) TrySubmitKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error
```

按 key 哈希非阻塞分发。池满时立即返回错误。

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 哈希 key |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回值 | `error` | 池满错误或提交错误 |

```go
err := sp.TrySubmitKeyed("user-123", ctx, fn)
```

---

#### SubmitBatch

```go
// 语法
func (sp *ShardedPool[T]) SubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]SubmitBatchResult, error)
```

批量提交，每个 item 分发到不同分片。返回 `[]SubmitBatchResult` 记录每个元素提交到哪个分片及是否出错。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待提交的元素列表 |
| `fn` | `func(context.Context, T) (T, error)` | 处理函数（接收 ctx 和元素） |
| 返回值 | `([]SubmitBatchResult, error)` | 每个元素的提交结果；第一个错误 |

```go
results, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item Item) (Item, error) {
    return process(ctx, item), nil
})
for _, r := range results {
    if r.Err != nil {
        log.Printf("元素 %d 提交到分片 %d 失败: %v", r.Index, r.ShardIdx, r.Err)
    }
}
```

---

#### SubmitBatchResult

```go
type SubmitBatchResult struct {
    ShardIdx int
    Index    int
    Err      error
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `ShardIdx` | `int` | 提交到的分片索引 |
| `Index` | `int` | 原始 items 中的位置索引 |
| `Err` | `error` | 提交错误，nil 表示成功 |

---

#### TrySubmitBatch

```go
// 语法
func (sp *ShardedPool[T]) TrySubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]SubmitBatchResult, error)
```

批量非阻塞提交。与 `SubmitBatch` 相同，但池满时立即返回错误。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待提交的元素列表 |
| `fn` | `func(context.Context, T) (T, error)` | 处理函数 |
| 返回值 | `([]SubmitBatchResult, error)` | 提交结果 |

---

### 等待结果

#### Wait

```go
// 语法
func (sp *ShardedPool[T]) Wait() []core.Result[T]
```

等待所有分片完成，合并结果。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]core.Result[T]` | 结果切片 | 所有分片结果的合并（不保证全局顺序） |

```go
results := sp.Wait()
for _, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

---

#### WaitAndClose

```go
// 语法
func (sp *ShardedPool[T]) WaitAndClose() []core.Result[T]
```

等待所有分片完成后关闭所有分片。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]core.Result[T]` | 结果切片 | 合并的所有结果 |

```go
results := sp.WaitAndClose()
// sp 已关闭，无需再调用 Close()
```

---

#### Close

```go
// 语法
func (sp *ShardedPool[T]) Close()
```

关闭所有分片。关闭后无法再提交新任务。

```go
defer sp.Close()
```

---

#### Reset

```go
// 语法
func (sp *ShardedPool[T]) Reset() error
```

重置所有分片（需在 Wait 后无活跃任务时调用）。保留原有的 worker 数、超时等配置。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | nil 表示所有分片重置成功 |

```go
results := sp.Wait()
// 处理 results...
err := sp.Reset()
// sp 可以重新使用
```

---

### 代理配置方法

这些方法会将配置**应用到所有分片**，返回 `*ShardedPool[T]` 以支持链式调用。

#### ResizePerShard

```go
// 语法
func (sp *ShardedPool[T]) ResizePerShard(newSize int) *ShardedPool[T]
```

调整每个分片的 worker 数。**必须在 Submit 前调用**。

| 参数 | 类型 | 说明 |
|------|------|------|
| `newSize` | `int` | 新的 worker 数 |
| 返回值 | `*ShardedPool[T]` | 自身，支持链式调用 |

```go
sp.ResizePerShard(16)
```

---

#### WithTimeout

```go
// 语法
func (sp *ShardedPool[T]) WithTimeout(d time.Duration) *ShardedPool[T]
```

为所有分片设置任务超时。

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 超时时间 |
| 返回值 | `*ShardedPool[T]` | 自身 |

```go
sp.WithTimeout(30 * time.Second)
```

---

#### WithSubmitTimeout

```go
// 语法
func (sp *ShardedPool[T]) WithSubmitTimeout(d time.Duration) *ShardedPool[T]
```

为所有分片设置提交超时。

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 提交超时时间 |
| 返回值 | `*ShardedPool[T]` | 自身 |

```go
sp.WithSubmitTimeout(5 * time.Second)
```

---

#### WithFailFast

```go
// 语法
func (sp *ShardedPool[T]) WithFailFast(ctx context.Context) (*ShardedPool[T], context.Context)
```

为所有分片启用 FailFast 模式。第一个失败的任务会取消其余任务。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 父上下文 |
| 返回值 | `(*ShardedPool[T], context.Context)` | 自身和派生的 cancel context |

```go
sp, ffCtx := sp.WithFailFast(ctx)
```

---

#### WithStreaming

```go
// 语法
func (sp *ShardedPool[T]) WithStreaming(bufSize int) *ShardedPool[T]
```

为所有分片启用流式结果消费。结果通过 channel 实时输出。

| 参数 | 类型 | 说明 |
|------|------|------|
| `bufSize` | `int` | channel 缓冲区大小，0 使用默认值 |
| 返回值 | `*ShardedPool[T]` | 自身 |

```go
sp.WithStreaming(0)
```

---

#### WithResultCallback

```go
// 语法
func (sp *ShardedPool[T]) WithResultCallback(fn func(core.Result[T])) *ShardedPool[T]
```

为所有分片设置结果回调。每个任务完成时触发回调。

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func(core.Result[T])` | 回调函数 |
| 返回值 | `*ShardedPool[T]` | 自身 |

```go
sp.WithResultCallback(func(r core.Result[string]) {
    if !r.Ok() {
        log.Printf("任务失败: %v", r.Err)
    }
})
```

---

#### WithRingBuffer

```go
// 语法
func (sp *ShardedPool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *ShardedPool[T]
```

为所有分片启用环形缓冲区。结果写入固定容量环形缓冲，通过 `Flush` 批量消费。

| 参数 | 类型 | 说明 |
|------|------|------|
| `capacity` | `int` | 环形缓冲区容量 |
| `overflow` | `core.OverflowStrategy` | 溢出策略：OverflowBlock / OverflowDrop / OverflowError |
| 返回值 | `*ShardedPool[T]` | 自身 |

```go
sp.WithRingBuffer(10000, core.OverflowDrop)
```

---

#### WithMaxPending

```go
// 语法
func (sp *ShardedPool[T]) WithMaxPending(n int) *ShardedPool[T]
```

为所有分片设置最大等待任务数（背压控制）。超过此值时根据 Overflow 策略处理。

| 参数 | 类型 | 说明 |
|------|------|------|
| `n` | `int` | 最大 pending 数 |
| 返回值 | `*ShardedPool[T]` | 自身 |

```go
sp.WithMaxPending(1000)
```

---

#### WithOverflow

```go
// 语法
func (sp *ShardedPool[T]) WithOverflow(strategy core.OverflowStrategy) *ShardedPool[T]
```

为所有分片设置溢出策略。

| 参数 | 类型 | 说明 |
|------|------|------|
| `strategy` | `core.OverflowStrategy` | OverflowBlock / OverflowDrop / OverflowError |
| 返回值 | `*ShardedPool[T]` | 自身 |

```go
sp.WithOverflow(core.OverflowError)
```

---

### 统计聚合

#### ShardStats

```go
// 语法
func (sp *ShardedPool[T]) ShardStats() []pool.PoolStats
```

汇总所有分片的统计信息。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]pool.PoolStats` | 统计切片 | 每个分片一个 PoolStats |

```go
for i, stat := range sp.ShardStats() {
    fmt.Printf("分片 %d: 活跃=%d 总提交=%d 总完成=%d\n", i, stat.Active, stat.TotalSubmitted, stat.TotalCompleted)
}
```

---

#### TotalFailCount

```go
// 语法
func (sp *ShardedPool[T]) TotalFailCount() int64
```

汇总所有分片的失败任务数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | 失败总数 | 所有分片之和 |

---

#### TotalSuccessCount

```go
// 语法
func (sp *ShardedPool[T]) TotalSuccessCount() int64
```

汇总所有分片的成功任务数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | 成功总数 | 所有分片之和 |

---

#### TotalActive

```go
// 语法
func (sp *ShardedPool[T]) TotalActive() int
```

汇总所有分片当前活跃任务数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | 活跃任务总数 | 所有分片之和 |

---

#### TotalBusy

```go
// 语法
func (sp *ShardedPool[T]) TotalBusy() int
```

汇总所有分片当前忙碌任务数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | 忙碌任务总数 | 所有分片之和 |

---

#### TotalPending

```go
// 语法
func (sp *ShardedPool[T]) TotalPending() int
```

汇总所有分片等待中任务数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | 等待任务总数 | 所有分片之和 |

---

#### TotalWorkerCount

```go
// 语法
func (sp *ShardedPool[T]) TotalWorkerCount() int
```

汇总所有分片的 worker 总数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | worker 总数 | N × SizePerShard |

---

#### Flush

```go
// 语法
func (sp *ShardedPool[T]) Flush(maxPerShard int) []core.Result[T]
```

从所有分片环形缓冲区排空结果（需提前启用 `WithRingBuffer`）。

| 参数 | 类型 | 说明 |
|------|------|------|
| `maxPerShard` | `int` | 每个分片最多排空条数 |
| 返回值 | `[]core.Result[T]` | 排空的结果切片 |

```go
for {
    batch := sp.Flush(5000)
    if len(batch) == 0 {
        break
    }
    saveBatch(batch)
}
```

---

## ShardedGroup（分片任务组）

### 创建

#### DefaultShardedGroup

```go
// 语法
func DefaultShardedGroup[T any]() *ShardedGroup[T]
```

使用默认配置创建分片 Group：4 分片、每分片 IO 并发度、RoundRobin 分发。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*ShardedGroup[T]` | 分片 Group 指针 | 已创建 |

```go
sg := shard.DefaultShardedGroup[int]()
```

---

#### NewShardedGroup

```go
// 语法
func NewShardedGroup[T any](cfg ShardGroupConfig[T]) *ShardedGroup[T]
```

自定义配置创建分片 Group。cfg 中零值字段会使用默认值。

| 参数 | 类型 | 说明 |
|------|------|------|
| `cfg` | `ShardGroupConfig[T]` | 分片 Group 配置 |
| 返回值 | `*ShardedGroup[T]` | 分片 Group 指针 |

```go
sg := shard.NewShardedGroup(shard.ShardGroupConfig[string]{
    Shards:              8,
    ConcurrencyPerShard: 4,
    Distribution:        shard.RoundRobin,
})
```

---

#### ShardGroupConfig

```go
type ShardGroupConfig[T any] struct {
    Shards              int
    ConcurrencyPerShard int
    Distribution        Distribution
}
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `Shards` | `int` | 4 | 分片数 |
| `ConcurrencyPerShard` | `int` | `core.IO()` | 每分片并发度 |
| `Distribution` | `Distribution` | `RoundRobin` | 分发策略 |

---

### 基础 API

#### ShardCount

```go
// 语法
func (sg *ShardedGroup[T]) ShardCount() int
```

返回分片数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | 分片数 | |

---

#### GetShard

```go
// 语法
func (sg *ShardedGroup[T]) GetShard(idx int) *group.Group[T]
```

获取指定分片的 Group 实例。idx 越界返回 nil。

| 参数 | 类型 | 说明 |
|------|------|------|
| `idx` | `int` | 分片索引 |
| 返回值 | `*group.Group[T]` | Group 指针，越界返回 nil |

---

#### Go

```go
// 语法
func (sg *ShardedGroup[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error
```

分发任务到某个分片执行（按分发策略路由）。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回值 | `error` | Group 提交错误 |

```go
err := sg.Go(ctx, func(ctx context.Context) (string, error) {
    return process(ctx), nil
})
```

---

#### GoAt

```go
// 语法
func (sg *ShardedGroup[T]) GoAt(shardIdx, index int, ctx context.Context, fn func(context.Context) (T, error)) error
```

将任务提交到指定分片的指定索引位置。shardIdx 越界时自动 fallback 到轮询。

| 参数 | 类型 | 说明 |
|------|------|------|
| `shardIdx` | `int` | 目标分片索引 |
| `index` | `int` | 在分片结果中的位置索引 |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回值 | `error` | Group 提交错误 |

---

#### GoKeyed

```go
// 语法
func (sg *ShardedGroup[T]) GoKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error
```

按 key 哈希分发到固定分片，保证同一 key 始终路由到同一分片。

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 哈希 key |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |
| 返回值 | `error` | Group 提交错误 |

```go
// 租户 tenant-abc 始终路由到同一分片
err := sg.GoKeyed("tenant-abc", ctx, fn)
```

---

#### GoBatch

```go
// 语法
func (sg *ShardedGroup[T]) GoBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]GoBatchResult, error)
```

批量分发，每个 item 随机分配到不同分片。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待分发的元素列表 |
| `fn` | `func(context.Context, T) (T, error)` | 处理函数 |
| 返回值 | `([]GoBatchResult, error)` | 分发结果和首个错误 |

---

#### GoBatchResult

```go
type GoBatchResult struct {
    ShardIdx int
    Index    int
    Err      error
}
```

---

### 等待结果

#### Wait

```go
// 语法
func (sg *ShardedGroup[T]) Wait() []core.Result[T]
```

等待所有分片完成，合并结果。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]core.Result[T]` | 结果切片 | 所有分片结果的合并 |

```go
results := sg.Wait()
```

---

#### WaitTimeout

```go
// 语法
func (sg *ShardedGroup[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool)
```

等待所有分片完成或超时。

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 超时时间 |
| 返回值 | `([]core.Result[T], bool)` | 结果切片和是否全部完成 |

```go
results, ok := sg.WaitTimeout(5 * time.Second)
if !ok {
    log.Println("部分分片超时未完成")
}
```

---

#### WaitContext

```go
// 语法
func (sg *ShardedGroup[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool)
```

等待所有分片完成或 ctx 取消。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| 返回值 | `([]core.Result[T], bool)` | 结果切片和是否全部完成 |

---

#### Reset

```go
// 语法
func (sg *ShardedGroup[T]) Reset() error
```

重置所有分片（需在 Wait 后无活跃任务时调用）。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | nil 表示所有分片重置成功 |

---

### 代理配置方法

#### WithTimeout

```go
// 语法
func (sg *ShardedGroup[T]) WithTimeout(d time.Duration) *ShardedGroup[T]
```

为所有分片设置任务超时。

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 超时时间 |
| 返回值 | `*ShardedGroup[T]` | 自身 |

---

#### WithSubmitTimeout

```go
// 语法
func (sg *ShardedGroup[T]) WithSubmitTimeout(d time.Duration) *ShardedGroup[T]
```

为所有分片设置提交超时。

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 提交超时 |
| 返回值 | `*ShardedGroup[T]` | 自身 |

---

#### WithFailFast

```go
// 语法
func (sg *ShardedGroup[T]) WithFailFast(ctx context.Context) (*ShardedGroup[T], context.Context)
```

为所有分片启用 FailFast 模式。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 父上下文 |
| 返回值 | `(*ShardedGroup[T], context.Context)` | 自身和派生 cancel context |

---

#### WithFFCtx

```go
// 语法
func (sg *ShardedGroup[T]) WithFFCtx(ctx context.Context) (*ShardedGroup[T], context.Context)
```

`WithFailFast` 的缩写形式。

---

#### WithStreaming

```go
// 语法
func (sg *ShardedGroup[T]) WithStreaming(bufSize int) *ShardedGroup[T]
```

为所有分片启用流式结果消费。

| 参数 | 类型 | 说明 |
|------|------|------|
| `bufSize` | `int` | channel 缓冲区大小 |
| 返回值 | `*ShardedGroup[T]` | 自身 |

---

#### WithResultCallback

```go
// 语法
func (sg *ShardedGroup[T]) WithResultCallback(fn func(core.Result[T])) *ShardedGroup[T]
```

为所有分片设置结果回调。

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func(core.Result[T])` | 回调函数 |
| 返回值 | `*ShardedGroup[T]` | 自身 |

---

### 统计聚合

#### TotalFailCount

```go
// 语法
func (sg *ShardedGroup[T]) TotalFailCount() int64
```

汇总所有分片失败数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | 失败总数 | |

---

#### TotalSuccessCount

```go
// 语法
func (sg *ShardedGroup[T]) TotalSuccessCount() int64
```

汇总所有分片成功数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | 成功总数 | |

---

#### TotalActive

```go
// 语法
func (sg *ShardedGroup[T]) TotalActive() int
```

汇总所有分片活跃任务数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | 活跃任务总数 | |

---

#### TotalBusy

```go
// 语法
func (sg *ShardedGroup[T]) TotalBusy() int
```

汇总所有分片忙碌任务数。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | 忙碌任务总数 | |

---

#### TotalConcurrency

```go
// 语法
func (sg *ShardedGroup[T]) TotalConcurrency() int
```

汇总所有分片并发度之和。

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | 总并发度 | Shards × ConcurrencyPerShard |

---

## 路由策略

### RoundRobin（轮询）

```go
sp := shard.NewShardedPool(shard.ShardPoolConfig[string]{
    Shards:       4,
    Distribution: shard.RoundRobin,
})
```

- `Submit` / `Go` 自动轮询：分片 0 → 1 → 2 → 3 → 0 → ...
- 适用场景：任务处理时间均匀，不需要关联性

### Hash（哈希分发）

```go
sp := shard.NewShardedPool(shard.ShardPoolConfig[string]{
    Shards:       4,
    Distribution: shard.Hash,
})
```

- `SubmitKeyed(ctx, "user-123", fn)` → `FNV32("user-123") % 4` → 固定的分片
- 同一 key 始终路由到同一分片，避免跨分片竞态
- 适用场景：按用户/租户维度的串行处理

---

## 分片数建议

| 场景 | 推荐分片数 | 每分片 Worker | 说明 |
|------|-----------|---------------|------|
| 低流量（< 1K QPS） | 4 | `IO()` | 最小配置 |
| 中等流量（1K-10K QPS） | 8 | `IO()` | 推荐配置 |
| 高流量（10K-100K QPS） | 16 | `IO()` | 高流量隔离 |
| 超大规模（> 100K QPS） | 32 | `CPU()×2` | 32 分片 + 32 Worker/分片 |

**注意**：分片数固定，创建后无法动态增减。请预先估计流量峰值。

---

## 完整示例

### Kafka Consumer 分片处理

```go
sp := shard.NewShardedPool(shard.ShardPoolConfig[Result]{
    Shards:       32,
    SizePerShard: 8,
}).WithMaxPending(5000).
    WithOverflow(core.OverflowDrop).
    WithRingBuffer(5000, core.OverflowDrop)
defer sp.Close()

// 按分区 key 哈希路由
for msg := range kafkaMessages {
    err := sp.SubmitKeyed(msg.PartitionKey, ctx, func(ctx context.Context) (Result, error) {
        return processMessage(ctx, msg), nil
    })
    if errors.Is(err, core.ErrQueueOverflow) {
        metrics.Inc("kafka.backpressure.drop")
    }
}

// 批量消费结果
go func() {
    for {
        batch := sp.Flush(1000)
        if len(batch) == 0 {
            time.Sleep(100 * time.Millisecond)
            continue
        }
        saveBatch(batch)
    }
}()

results := sp.Wait()
```

### 多租户隔离

```go
// 8 分片，按租户 ID 哈希，可以确保同一租户的操作串行
sg := shard.NewShardedGroup(shard.ShardGroupConfig[Result]{
    Shards:              8,
    ConcurrencyPerShard: 4,
    Distribution:        shard.RoundRobin,
})

for _, tenant := range tenants {
    sg.GoKeyed(tenant.ID, ctx, func(ctx context.Context) (Result, error) {
        return processTenant(ctx, tenant), nil
    })
}

results := sg.Wait()
```

### 链式配置

```go
sp := shard.NewShardedPool(shard.ShardPoolConfig[string]{
    Shards:       8,
    SizePerShard: 16,
}).WithTimeout(30 * time.Second).
    WithSubmitTimeout(5 * time.Second).
    WithMaxPending(5000).
    WithOverflow(core.OverflowError).
    WithStreaming(0)
defer sp.Close()
```