# 分片分发 (Shard)

分片模块 `ShardedPool[T]` / `ShardedGroup[T]` 将任务自动分发到多个 Pool / Group 实例，实现**水平扩展**和**分摊到多实例**。每个分片独立运行，互不影响，适合无需全局顺序的大批量并发场景。

## 目录

- [核心概念](#核心概念)
- [分发策略](#分发策略)
- [ShardedPool — 分片协程池](#shardedpool--分片协程池)
- [ShardedPool 方法详解](#shardedpool-方法详解)
- [ShardedPool 代理配置方法](#shardedpool-代理配置方法)
- [ShardedPool 统计聚合](#shardedpool-统计聚合)
- [ShardedGroup — 分片任务组](#shardedgroup--分片任务组)
- [ShardedGroup 方法详解](#shardedgroup-方法详解)
- [ShardedGroup 代理配置方法](#shardedgroup-代理配置方法)
- [ShardedGroup 统计聚合](#shardedgroup-统计聚合)
- [ShardedPool vs ShardedGroup 选择指南](#shardedpool-vs-shardedgroup-选择指南)
- [分片数选择建议](#分片数选择建议)
- [完整示例](#完整示例)

---

## 核心概念

```
       ┌──────────┐
请求 → │ 分发策略 │ → 分片1: Pool/Group → Wait → 合并结果
       │RoundRobin│ → 分片2: Pool/Group → Wait → 合并结果
       │  Hash    │ → 分片3: Pool/Group → Wait → 合并结果
       └──────────┘ → 分片4: Pool/Group → Wait → 合并结果
```

- **ShardedPool**：长期运行的协程池，分摊到多个 Pool 实例，适合高频反复提交
- **ShardedGroup**：一次性批量任务，分摊到多个 Group 实例，用完即销毁
- **分发策略**：RoundRobin（轮询）或 Hash（按 key 哈希），保证同一 key 路由到相同分片

**何时用 ShardedPool / ShardedGroup？**

| 场景 | 选择 |
|------|------|
| 单 Pool/Group 的并发度不够，需要水平扩展 | ShardedPool / ShardedGroup |
| 需要按用户/租户隔离（同一用户路由到同一分片） | ShardedPool + SubmitKeyed / ShardedGroup + GoKeyed |
| 大批量任务，需要分摊到多个 CPU 核心处理 | ShardedPool / ShardedGroup |
| 单实例内存压力大，需分片降低单实例 result 切片大小 | ShardedPool / ShardedGroup |

---

## 分发策略

```go
const (
    RoundRobin Distribution = iota // 轮询分发（默认）
    Hash                           // 按 key 哈希分发
)
```

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| `RoundRobin` | 每次分发到下一个分片，循环往复 | 无状态任务，负载均衡 |
| `Hash` | 通过 `SubmitKeyed` / `GoKeyed` 按 key 哈希到固定分片 | 需要亲和性，如按用户 ID 隔离 |

> RoundRobin 通过 `atomic.AddUint64` 保证线程安全的轮询递增，单分片每轮递增一次。

---

## ShardedPool — 分片协程池

将任务分发到 N 个 Pool 实例，每个分片独立管理 worker 生命周期。

### 创建

```go
import "github.com/chichengyu/async/shard"

// 方式一：默认配置（4 分片，每分片 IO 并发度，RoundRobin）
sp := shard.DefaultShardedPool[string]()
defer sp.Close()

// 方式二：自定义配置
sp := shard.NewShardedPool(shard.ShardPoolConfig[string]{
    Shards:       8,                    // 8 个分片
    SizePerShard: 16,                   // 每分片 16 个 worker
    Distribution: shard.RoundRobin,     // RoundRobin 分发
})

// 方式三：Hash 分发（需提供 KeyFn，或使用 SubmitKeyed）
sp := shard.NewShardedPool(shard.ShardPoolConfig[MyType]{
    Shards:       8,
    SizePerShard: 10,
    Distribution: shard.Hash,
    KeyFn: func(t MyType) uint64 {
        return uint64(t.UserID)
    },
})
defer sp.Close()
```

**`ShardPoolConfig[T]` 结构：**

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `Shards` | `int` | **4** | 分片数量 |
| `SizePerShard` | `int` | **IO()**（CPU核数×2） | 每个分片内的 worker 数 |
| `Distribution` | `Distribution` | **RoundRobin** | 分发策略 |
| `KeyFn` | `func(T) uint64` | **nil** | Hash 策略的 key 提取函数 |

> **默认值规则**：`Shards ≤ 0` → 4；`SizePerShard ≤ 0` → `core.IO()`。

---

## ShardedPool 方法详解

### 基础 API

#### ShardCount — 分片数

```go
n := sp.ShardCount() // 返回 int，如 4
```

#### GetShard — 获取指定分片

```go
p := sp.GetShard(2) // 获取第 3 个分片的 *Pool[T] 实例
if p == nil {
    log.Println("分片索引越界")
}
// 可直接调用 Pool 的方法，如 p.Stats()、p.Resize() 等
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `idx` | `int` | 分片索引（0 ~ ShardCount-1），越界返回 nil |

**返回：** `*pool.Pool[T]` — 分片池实例

#### Submit — RoundRobin 分发提交

```go
for i := 0; i < 1000; i++ {
    idx := i
    err := sp.Submit(ctx, func(ctx context.Context) (string, error) {
        return processItem(ctx, idx), nil
    })
    if err != nil {
        log.Printf("提交失败: %v", err)
    }
}

results := sp.Wait()
```

**参数：**
- `ctx` — 上下文
- `fn` — `func(context.Context) (T, error)`

**返回：** `error` — 提交错误

#### SubmitAt — 指定分片指定索引提交

```go
// 提交到分片 2 的结果数组索引 5
err := sp.SubmitAt(2, 5, ctx, func(ctx context.Context) (int, error) {
    return processAt(ctx, 5), nil
})
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `shardIdx` | `int` | 分片索引（越界时回退到 RoundRobin） |
| `index` | `int` | 结果数组目标位置 |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |

**返回：** `error` — 提交错误

#### TrySubmit — 非阻塞分发提交

```go
err := sp.TrySubmit(ctx, func(ctx context.Context) (string, error) {
    return quickTask(ctx), nil
})
if errors.Is(err, async.ErrSubmitTimeout) {
    log.Println("分片池满，任务被拒绝")
}
```

**参数/返回：** 同 `Submit`，队列满立即返回 `ErrSubmitTimeout`

#### SubmitKeyed — 按 key 哈希分发

同一 key 保证路由到同一个分片：

```go
// 所有 userId="user-123" 的任务都路由到同一分片
err := sp.SubmitKeyed("user-123", ctx, func(ctx context.Context) (Data, error) {
    return fetchUserData(ctx, "user-123"), nil
})
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 哈希 key |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |

> 内部使用 FNV-32a 哈希算法：`fnv.New32a()` → `h.Sum32() % shardCount`

#### TrySubmitKeyed — 非阻塞按 key 分发

```go
err := sp.TrySubmitKeyed("tenant-a", ctx, fn)
```

**参数/返回：** 同 `SubmitKeyed`，队列满立即返回 `ErrSubmitTimeout`

#### SubmitBatch — 批量分发提交

将切片每个元素分发到不同分片：

```go
items := []string{"url1", "url2", "url3"}

results, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, url string) (string, error) {
    return httpGet(ctx, url)
})

for _, r := range results {
    if r.Err != nil {
        log.Printf("分片 %d 位置 %d 提交失败: %v", r.ShardIdx, r.Index, r.Err)
    }
}

waitResults := sp.Wait()
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 输入元素切片 |
| `fn` | `func(context.Context, T) (T, error)` | 处理函数 |

**返回：**
- `[]SubmitBatchResult` — 每个元素的分片索引、数组位置、提交错误
- `error` — 首个提交错误（nil 表示全部成功）

**`SubmitBatchResult` 结构：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `ShardIdx` | `int` | 分配到的分片索引 |
| `Index` | `int` | 在结果数组中的位置 |
| `Err` | `error` | 提交错误（nil = 成功） |

#### TrySubmitBatch — 批量非阻塞分发

```go
results, err := sp.TrySubmitBatch(ctx, items, fn)
```

**参数/返回：** 同 `SubmitBatch`，使用非阻塞提交

#### Wait — 等待所有分片完成

```go
results := sp.Wait()
// 所有分片结果合并为一个切片
for _, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

**返回：** `[]core.Result[T]` — 合并后的所有分片结果

#### WaitAndClose — 等待后关闭

```go
results := sp.WaitAndClose()
// 等价于 sp.Wait() + sp.Close()
```

#### Close — 关闭所有分片

```go
sp.Close()
// 所有分片池的 worker goroutine 退出
```

#### Reset — 重置所有分片

```go
if err := sp.Reset(); err != nil {
    log.Printf("重置失败: %v", err)
}
```

> 需在 Wait 后无活跃任务时调用，保留原有 worker 数、超时等配置。

---

## ShardedPool 代理配置方法

将配置广播到所有分片，返回自身支持链式调用。

#### ResizePerShard — 调整每分片 worker 数

```go
// 每个分片扩容到 20 个 worker
sp.ResizePerShard(20)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `newSize` | `int` | 新的 worker 数 |

#### WithTimeout — 设置任务超时

```go
sp.WithTimeout(10 * time.Second)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 单个任务超时时间 |

#### WithSubmitTimeout — 设置提交超时

```go
sp.WithSubmitTimeout(3 * time.Second)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 提交等待超时 |

#### WithFailFast — 启用 FailFast

```go
sp, ffCtx := sp.WithFailFast(ctx)
sp.Submit(ffCtx, fn)
// 任一任务失败，其他未执行任务被跳过
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文（自动注入 trace_id） |

**返回：** `(*ShardedPool[T], context.Context)` — 新的分片池引用 + FailFast ctx

#### WithStreaming — 启用流式结果消费

```go
sp.WithStreaming(0) // 0 = 自动计算缓冲区大小
// 或指定缓冲区大小
sp.WithStreaming(1024)

// 消费流式结果
go func() {
    for r := range sp.StreamResults() {
        handleResult(r)
    }
}()
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `bufSize` | `int` | 缓冲区大小（≤0 自动使用 分片大小×2） |

> streamCh 在 `Wait()` / `WaitAndClose()` / `Close()` 时关闭。

#### WithResultCallback — 设置结果回调

```go
sp.WithResultCallback(func(r core.Result[Data]) {
    if r.Ok() {
        notifySuccess(r.Value)
    } else {
        log.Printf("任务失败: %v", r.Err)
    }
})
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func(core.Result[T])` | 回调函数（在 worker goroutine 中执行，应尽量轻量） |

#### WithRingBuffer — 启用环形缓冲

```go
sp.WithRingBuffer(10000, core.OverflowDrop)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `capacity` | `int` | 环形缓冲区容量 |
| `overflow` | `core.OverflowStrategy` | 溢出策略 |

#### WithMaxPending — 背压控制

```go
sp.WithMaxPending(500)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `n` | `int` | 每分片最大等待任务数 |

#### WithOverflow — 溢出策略

```go
sp.WithOverflow(core.OverflowDrop)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `strategy` | `core.OverflowStrategy` | 溢出策略 |

---

## ShardedPool 统计聚合

#### ShardStats — 各分片统计

```go
stats := sp.ShardStats() // []pool.PoolStats
for i, s := range stats {
    fmt.Printf("分片 %d: worker=%d active=%d success=%d fail=%d\n",
        i, s.Size, s.Active, s.SuccessTask, s.FailTask)
}
```

#### TotalFailCount — 汇总失败数

```go
failCount := sp.TotalFailCount() // int64
fmt.Printf("总失败数: %d\n", failCount)
```

#### TotalSuccessCount — 汇总成功数

```go
successCount := sp.TotalSuccessCount() // int64
```

#### TotalActive — 汇总活跃数

```go
active := sp.TotalActive() // int
```

#### TotalBusy — 汇总忙碌数

```go
busy := sp.TotalBusy() // int
```

#### TotalPending — 汇总等待数

```go
pending := sp.TotalPending() // int
```

#### TotalWorkerCount — 汇总 worker 总数

```go
workers := sp.TotalWorkerCount() // int
```

#### Flush — 排空所有分片环形缓冲

```go
// 从所有分片的环形缓冲区取出结果（需提前启用 WithRingBuffer）
results := sp.Flush(100) // 每分片最多取 100 个
// 或取出全部: sp.Flush(0)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `maxPerShard` | `int` | 每分片最多取出的数量（≤0 取出全部） |

---

## ShardedGroup — 分片任务组

将任务分发到 N 个 Group 实例，适合一次性批量任务的水平扩展。

### 创建

```go
import "github.com/chichengyu/async/shard"

// 方式一：默认配置（4 分片，每分片 IO 并发度，RoundRobin）
sg := shard.DefaultShardedGroup[int]()

// 方式二：自定义配置
sg := shard.NewShardedGroup(shard.ShardGroupConfig[int]{
    Shards:              8,     // 8 个分片
    ConcurrencyPerShard: 16,    // 每分片 16 并发
    Distribution:        shard.RoundRobin,
})
```

**`ShardGroupConfig[T]` 结构：**

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `Shards` | `int` | **4** | 分片数量 |
| `ConcurrencyPerShard` | `int` | **IO()**（CPU核数×2） | 每个分片内的并发度 |
| `Distribution` | `Distribution` | **RoundRobin** | 分发策略 |

---

## ShardedGroup 方法详解

#### ShardCount — 分片数

```go
n := sg.ShardCount() // int
```

#### GetShard — 获取指定分片

```go
g := sg.GetShard(0) // *group.Group[T]
if g == nil {
    log.Println("分片索引越界")
}
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `idx` | `int` | 分片索引（0 ~ ShardCount-1），越界返回 nil |

#### Go — RoundRobin 分发提交

```go
for i := 0; i < 1000; i++ {
    idx := i
    sg.Go(ctx, func(ctx context.Context) (int, error) {
        return processItem(ctx, idx), nil
    })
}

results := sg.Wait()
```

**参数：**
- `ctx` — 上下文
- `fn` — `func(context.Context) (T, error)`

**返回：** `error` — 提交错误

#### GoAt — 指定分片指定索引提交

```go
err := sg.GoAt(3, 7, ctx, func(ctx context.Context) (string, error) {
    return processAtPosition(ctx, 7), nil
})
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `shardIdx` | `int` | 分片索引（越界回退到 RoundRobin） |
| `index` | `int` | 结果数组目标位置 |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |

#### GoKeyed — 按 key 哈希分发

```go
// 同一 tenant 路由到相同分片
err := sg.GoKeyed("tenant-abc", ctx, func(ctx context.Context) (Data, error) {
    return processTenant(ctx, "tenant-abc"), nil
})
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | `string` | 哈希 key |
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 任务函数 |

#### GoBatch — 批量分发

```go
items := []User{user1, user2, user3}

results, err := sg.GoBatch(ctx, items, func(ctx context.Context, u User) (string, error) {
    return enrichUser(ctx, u), nil
})

for _, r := range results {
    fmt.Printf("元素 %d → 分片 %d, err=%v\n", r.Index, r.ShardIdx, r.Err)
}

waitResults := sg.Wait()
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 输入元素切片 |
| `fn` | `func(context.Context, T) (T, error)` | 处理函数 |

**返回：**
- `[]GoBatchResult` — 每个元素的分片索引、数组位置、提交错误
- `error` — 首个提交错误

**`GoBatchResult` 结构：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `ShardIdx` | `int` | 分配到的分片索引 |
| `Index` | `int` | 在结果数组中的位置 |
| `Err` | `error` | 提交错误 |

#### Wait — 等待所有分片完成

```go
results := sg.Wait()
```

**返回：** `[]core.Result[T]` — 合并后的所有分片结果

#### WaitTimeout — 带超时等待

```go
results, ok := sg.WaitTimeout(10 * time.Second)
if !ok {
    log.Println("部分分片超时")
}
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `d` | `time.Duration` | 超时时间 |

**返回：** `([]core.Result[T], bool)` — ok=false 表示部分分片超时

#### WaitContext — Context 等待

```go
results, ok := sg.WaitContext(ctx)
if !ok {
    log.Println("context 已取消")
}
```

#### Reset — 重置所有分片

```go
if err := sg.Reset(); err != nil {
    log.Printf("重置失败: %v", err)
}
```

---

## ShardedGroup 代理配置方法

将配置广播到所有分片，返回自身支持链式调用。

#### WithTimeout — 任务超时

```go
sg.WithTimeout(10 * time.Second)
```

#### WithSubmitTimeout — 提交超时

```go
sg.WithSubmitTimeout(3 * time.Second)
```

#### WithFailFast — 启用 FailFast

```go
sg, ffCtx := sg.WithFailFast(ctx)
sg.Go(ffCtx, fn)
// 或缩写形式
sg, ffCtx := sg.WithFFCtx(ctx)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |

**返回：** `(*ShardedGroup[T], context.Context)`

#### WithStreaming — 启用流式结果消费

```go
sg.WithStreaming(2048)

// 消费结果
for r := range sg.StreamResults() {
    processResult(r)
}
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `bufSize` | `int` | 缓冲区大小（≤0 自动使用 并发度×2） |

> streamCh 在 `Wait()` 时关闭（ShardedGroup 无 Close）。

#### WithResultCallback — 结果回调

```go
sg.WithResultCallback(func(r core.Result[int]) {
    metrics.Record(r)
})
```

---

## ShardedGroup 统计聚合

#### TotalFailCount — 汇总失败数

```go
failCount := sg.TotalFailCount() // int64
```

#### TotalSuccessCount — 汇总成功数

```go
successCount := sg.TotalSuccessCount() // int64
```

#### TotalActive — 汇总活跃数

```go
active := sg.TotalActive() // int
```

#### TotalBusy — 汇总忙碌数

```go
busy := sg.TotalBusy() // int
```

---

## ShardedPool vs ShardedGroup 选择指南

| 特性 | ShardedPool | ShardedGroup |
|------|-------------|-------------|
| 底层实例 | 多个 Pool[T] | 多个 Group[T] |
| goroutine 复用 | ✅ 长期复用 | ❌ 每次 Go 新建 |
| 适用场景 | 长期高频反复提交 | 一次性大批量任务 |
| Submit 方式 | Submit / SubmitAt / SubmitKeyed / SubmitBatch | Go / GoAt / GoKeyed / GoBatch |
| 非阻塞提交 | TrySubmit / TrySubmitKeyed / TrySubmitBatch | 不支持（Group 无 TryGo） |
| 环形缓冲 | ✅ WithRingBuffer + Flush | ❌ |
| 背压控制 | ✅ WithMaxPending / WithOverflow | ❌ |
| FailFast | ✅ WithFailFast | ✅ WithFailFast |
| 自动扩缩容 | ✅ 每个分片单独 AutoScale | ✅ 每个分片单独 AutoScale |
| 生命周期 | 需手动 Close | Wait 后自动完成 |

---

## 分片数选择建议

| CPU 核数 | 建议分片数 | 理由 |
|----------|-----------|------|
| 4 核 | 4 | 每核一个分片 |
| 8 核 | 8 | 均衡负载 |
| 16 核 | 8 ~ 16 | 8 分片已足够覆盖大多数场景 |
| 32 核以上 | 16 ~ 32 | 上限取决于业务需求，过多会增加管理开销 |

> **通用公式**：`分片数 = min(CPU核数, 任务量/1000, 32)`，一般不需要超过 32 个分片。

每个分片内的 worker 数 / 并发度建议：

| 任务类型 | 建议每分片 worker 数 |
|----------|-------------------|
| IO 密集型（API 调用、DB 查询） | CPU 核数 × 2 ~ 4 |
| CPU 密集型（加密、压缩、编码） | CPU 核数 |
| 混合型 | CPU 核数 × 2 |

---

## 完整示例

### 示例1：ShardedPool 高频 Web 请求处理

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/chichengyu/async/core"
    "github.com/chichengyu/async/shard"
)

func main() {
    ctx := context.Background()

    // 创建 8 分片、每分片 16 worker 的池
    sp := shard.NewShardedPool(shard.ShardPoolConfig[string]{
        Shards:       8,
        SizePerShard: 16,
    })
    defer sp.Close()

    // 启用流式消费
    sp.WithStreaming(2048)

    // 消费流式结果
    go func() {
        for r := range sp.StreamResults() {
            if r.Ok() {
                fmt.Println("流式结果:", r.Value)
            }
        }
    }()

    // 提交 10000 个任务
    for i := 0; i < 10000; i++ {
        idx := i
        sp.Submit(ctx, func(ctx context.Context) (string, error) {
            return processRequest(ctx, idx)
        })
    }

    results := sp.Wait()

    // 统计
    stats := sp.ShardStats()
    for i, s := range stats {
        fmt.Printf("分片 %d: success=%d fail=%d\n", i, s.SuccessTask, s.FailTask)
    }
    fmt.Printf("总结果数: %d\n", len(results))
}
```

### 示例2：ShardedGroup 按用户 ID 哈希隔离

```go
func processUsersByShard(ctx context.Context, userIDs []int) error {
    // 4 分片，每分片 8 并发
    sg := shard.NewShardedGroup(shard.ShardGroupConfig[int]{
        Shards:              4,
        ConcurrencyPerShard: 8,
    })

    // 按用户 ID 哈希，同一用户落到同一分片
    for _, uid := range userIDs {
        id := uid
        key := fmt.Sprintf("user-%d", id)
        sg.GoKeyed(key, ctx, func(ctx context.Context) (UserData, error) {
            return fetchUserData(ctx, id)
        })
    }

    results := sg.Wait()

    // 检查结果
    successCount := sg.TotalSuccessCount()
    failCount := sg.TotalFailCount()
    fmt.Printf("处理完成: 成功=%d 失败=%d\n", successCount, failCount)

    if failCount > 0 {
        return fmt.Errorf("部分用户查询失败")
    }
    return nil
}
```

### 示例3：ShardedPool + 背压 + 环形缓冲

```go
func processWithBackpressure(ctx context.Context, items []string) {
    sp := shard.NewShardedPool(shard.ShardPoolConfig[string]{
        Shards:       4,
        SizePerShard: 8,
    })
    defer sp.Close()

    // 启用背压控制和环形缓冲
    sp.WithMaxPending(200).       // 每分片最多 200 个等待任务
        WithOverflow(core.OverflowDrop).  // 满则丢弃
        WithRingBuffer(5000, core.OverflowDrop) // 环形缓冲，满则覆盖

    for _, item := range items {
        it := item
        err := sp.Submit(ctx, func(ctx context.Context) (string, error) {
            return heavyProcess(ctx, it)
        })
        if errors.Is(err, async.ErrQueueOverflow) {
            log.Printf("任务被背压拒绝: %s", it)
        }
    }

    results := sp.Wait()
    fmt.Printf("完成任务: %d\n", len(results))
}
```