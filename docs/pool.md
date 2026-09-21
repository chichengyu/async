# 协程池 (Pool)

协程池 `Pool[T]` 是 async 库的核心组件之一，它复用固定数量的 goroutine 来处理高频并发任务。适合**长期运行、反复提交任务**的场景。

## 目录

- [核心概念](#核心概念)
- [创建池](#创建池)
- [提交任务](#提交任务)
- [等待与关闭](#等待与关闭)
- [高级选项](#高级选项)
- [流式结果消费](#流式结果消费)
- [环形缓冲](#环形缓冲)
- [背压控制](#背压控制)
- [查询与统计](#查询与统计)
- [结果提取](#结果提取)
- [动态扩容](#动态扩容)
- [重置](#重置)
- [NoResultPool](#noresultpool)
- [便捷函数](#便捷函数)
- [完整示例](#完整示例)
- [方法速查表](#方法速查表)

---

## 核心概念

```
生命周期：NewPool → Submit → Wait → Close
```

- **有返回值池** `Pool[T]`：每个任务返回 `Result[T]`
- **无返回值池** `NoResultPool`：每个任务只关心 error

**何时用 Pool 而非 Group？**
- Pool：长期运行的后台服务，反复提交任务（如 HTTP server 的 worker 池）
- Group：一次性批量任务，用完即销毁（如定时任务、数据迁移）

---

## 创建池

```go
import "github.com/chichengyu/async"

// 创建 4 个 worker 的协程池
p := async.NewPool[int](4)
defer p.Close()

// 使用默认 IO 并发度创建
p := async.DefaultPool[int]()
defer p.Close()

// 创建无返回值池
nrp := async.NewNoResultPool(10)
defer nrp.Close()

// 使用默认并发度的无返回值池
nrp := async.DefaultNoResultPool()
defer nrp.Close()
```

> **注意**：`NewPool` 会**立即启动** worker goroutine，创建后务必 `defer p.Close()`。

**默认行为：**

| 默认项 | 默认值 | 说明 |
|--------|--------|------|
| 任务超时 | **30s**（全局默认值） | 每个任务最多执行 30s 后超时取消，可通过 `WithTimeout` 覆盖 |
| 提交超时 | **无限等待** | Submit 阻塞等待，不设硬超时；每 30s 输出一次警告 |
| `size <= 0` | **IO()**（CPU 核数×2） | 自动使用 IO 并发度 |
| 任务队列容量 | **size × 2** | 缓冲通道可缓存的待处理任务数 |

---

## 提交任务

### Submit - 阻塞提交

排队等待，直到有空闲 worker 或 ctx 被取消：

```go
p := async.NewPool[string](4)
defer p.Close()

for i := 0; i < 100; i++ {
    idx := i
    err := p.Submit(ctx, func(ctx context.Context) (string, error) {
        return fmt.Sprintf("result-%d", idx), nil
    })
    if err != nil {
        log.Printf("提交失败: %v", err)
    }
}

results := p.Wait()
```

### TrySubmit - 非阻塞提交

队列满时立即返回 `ErrSubmitTimeout`，不等待：

```go
err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
    return processItem(ctx), nil
})
if errors.Is(err, async.ErrSubmitTimeout) {
    log.Println("池已满，任务被拒绝")
}
```

**参数：**
- `ctx` — 上下文
- `fn` — `func(context.Context) (T, error)`
- 返回：提交错误（nil=成功，`ErrSubmitTimeout`=池满，`ErrPoolClosed`=池已关闭）

### SubmitAt - 指定位置阻塞提交

指定结果数组的索引位置，保证与输入顺序一致：

```go
// 结果数组按 items 原始索引排列
for i, item := range items {
    p.SubmitAt(i, ctx, func(ctx context.Context) (int, error) {
        return processItem(ctx, item), nil
    })
}

results := p.Wait()
// results[0] 对应 items[0]
// results[1] 对应 items[1]
// ...
```

**参数：**
- `index` — 结果数组的目标位置 `int`
- `ctx` — 上下文
- `fn` — `func(context.Context) (T, error)`

### TrySubmitAt - 指定位置非阻塞提交

```go
err := p.TrySubmitAt(3, ctx, func(ctx context.Context) (string, error) {
    return fetchData(ctx), nil
})
if err != nil {
    log.Printf("位置 3 提交失败: %v", err)
}
```

**参数：** 同 SubmitAt + 返回提交错误

---

## 等待与关闭

### Wait - 阻塞等待

阻塞直到所有已提交任务执行完成，返回全部结果：

```go
results := p.Wait()
// 此后不可再 Submit（会返回 ErrPoolWaited）

for i, r := range results {
    if r.Ok() {
        fmt.Printf("任务 %d 成功: %v\n", i, r.Value)
    } else {
        log.Printf("任务 %d 失败: %v", i, r.Err)
    }
}
```

### WaitTimeout - 带超时等待

指定超时时间，超时后返回已完成的 results 和 `ok=false`：

```go
results, ok := p.WaitTimeout(5 * time.Second)
if !ok {
    log.Println("等待超时，部分任务可能未完成")
}
// 即使超时也可读取 results，但未完成的任务值可能为空
```

### WaitContext - Context 控制等待

通过 context 的 Done 信号控制等待截止：

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

results, ok := p.WaitContext(ctx)
if !ok {
    log.Println("context 已取消")
}
```

### WaitAndClose - 等待后自动关闭

等价于 `Wait()` + `Close()`：

```go
results := p.WaitAndClose()
// 无需再调用 p.Close()
```

### Close - 关闭池

关闭任务队列，等待所有 worker 退出：

```go
p.Close()
// 此后不可再 Submit（会返回 ErrPoolClosed）
```

### CloseAndWait - 关闭后等待 worker 退出

先关闭队列，然后等待所有 worker 处理完队列中剩余任务：

```go
p.CloseAndWait()
```

### CloseAndWaitTimeout - 带超时的关闭等待

```go
ok, workerDone := p.CloseAndWaitTimeout(30 * time.Second)
if !ok {
    log.Println("关闭超时，worker 可能仍在运行")
    // 可通过 workerDone channel 异步等待
    go func() {
        <-workerDone
        log.Println("所有 worker 已退出")
    }()
}
```

### CloseByIdle - 空闲后关闭

等待所有活跃任务完成，最多等待指定时长后强制关闭：

```go
// 最多等 30 秒让任务自然完成
p.CloseByIdle(30 * time.Second)
```

### CloseByIdle 完整等待（无限等）

```go
// 无限等待直到所有任务完成
p.CloseByIdle(0)
```

---

## 高级选项

### 超时控制

**`WithTimeout`** — 设置单个任务的执行超时时间

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 不设置时使用全局默认值 30s | 每个任务的最长执行时间 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*Pool[T]` | `*Pool[T]` | 链式调用 |

```go
// 设置单个任务的超时（不设置时默认 30 秒，来自全局默认值）
p.WithTimeout(10 * time.Second)
```

**`WithSubmitTimeout`** — 设置 Submit 等待空闲 worker 的超时时间

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `d` | `time.Duration` | 不设置时无限等待 | Submit 等待空闲 worker 的超时，超时返回 ErrSubmitTimeout |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*Pool[T]` | `*Pool[T]` | 链式调用 |

```go
p.WithSubmitTimeout(3 * time.Second)
```

### FailFast 模式

一个任务失败立即取消其他任务：

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

p, ffCtx := p.WithFailFast(ctx)

p.Submit(ffCtx, criticalTask)
p.Submit(ffCtx, anotherTask)
// 任一任务返回错误时，其他未执行的提交会被跳过（ErrSkipped）
```

### Context 注入

```go
// 带取消能力的 context
p, ctx := p.WithContext(ctx)

// 带 TraceID
p, ctx := p.WithTraceID(ctx)

// FailFast + 超时 + Context
p, ctx := p.WithFFTimeout(ctx, 10*time.Second)

// FailFast + 超时 + 提交超时 + TraceID
p, ctx := p.WithFFTimeoutSubmitTOTraceID(ctx, 10*time.Second, 3*time.Second)

// Context + 超时 + TraceID
p, ctx := p.WithCtxTimeoutTraceID(ctx, 10*time.Second)

// Context + 提交超时 + TraceID
p, ctx := p.WithCtxSubmitTOTraceID(ctx, 3*time.Second)
```

> 所有 With* 方法返回新的 Pool 引用（原 Pool 被修改），可用于链式调用。

### With 方法完整速查

| 方法 | 说明 |
|------|------|
| `WithTimeout(d)` | 设置单个任务超时 |
| `WithSubmitTimeout(d)` | 设置提交等待超时 |
| `WithContext(ctx)` | 注入 Context（带取消） |
| `WithTraceID(ctx)` | 确保 context 有 trace_id |
| `WithFailFast(ctx)` | 开启 FailFast 模式 |
| `WithFFCtx(ctx)` | = WithFailFast |
| `WithFFTraceID(ctx)` | FailFast + TraceID |
| `WithFFSubmitTO(ctx, d)` | FailFast + 提交超时 |
| `WithFFSubmitTOTraceID(ctx, d)` | FailFast + 提交超时 + TraceID |
| `WithFFTimeout(ctx, d)` | FailFast + 超时 |
| `WithFFTimeoutTraceID(ctx, d)` | FailFast + 超时 + TraceID |
| `WithFFTimeoutSubmitTO(ctx, d, sd)` | FailFast + 超时 + 提交超时 |
| `WithFFTimeoutSubmitTOTraceID(ctx, d, sd)` | FailFast + 超时 + 提交超时 + TraceID |
| `WithCtxTraceID(ctx)` | Context + TraceID |
| `WithCtxTimeout(ctx, d)` | Context + 超时 |
| `WithCtxTimeoutTraceID(ctx, d)` | Context + 超时 + TraceID |
| `WithCtxSubmitTO(ctx, d)` | Context + 提交超时 |
| `WithCtxSubmitTOTraceID(ctx, d)` | Context + 提交超时 + TraceID |
| `WithStreaming(bufSize)` | **新增** — 启用流式结果消费 |
| `WithResultCallback(fn)` | **新增** — 设置结果回调 |
| `WithRingBuffer(cap, overflow)` | **新增** — 启用环形缓冲区 |
| `WithMaxPending(n)` | **新增** — 背压控制：最大等待任务数 |
| `WithOverflow(strategy)` | **新增** — 队列溢出策略 |

---

## 流式结果消费

传统方式在 `Wait()` 后一次性拿到全部结果。流式消费允许在任务执行过程中实时消费每个完成的结果，减少内存峰值和响应延迟。

### 核心原理

```
Submit → worker 执行 → 结果写入 streamCh → 消费者实时读取 → Wait() 关闭 channel
```

> **注意**：流式消费在 **结果产生时** 实时推送，而非等所有任务完成。`Wait()` 调用后 channel 自动关闭。

### WithStreaming — 启用流式 channel

```go
p := async.NewPool[string](4)
defer p.Close()

// 启用流式消费，bufSize=0 自动使用 pool.Size() * 2
p.WithStreaming(128)

// 启动消费者 goroutine
go func() {
    for r := range p.StreamResults() {
        if r.Ok() {
            fmt.Println("实时结果:", r.Value)
        } else {
            log.Printf("任务失败: %v", r.Err)
        }
    }
    fmt.Println("所有流式结果消费完毕")
}()

// 提交任务
for i := 0; i < 100; i++ {
    p.Submit(ctx, fn)
}

// Wait 会等待所有任务完成后关闭 streamCh
p.Wait()
// 此时 streamCh 已关闭，消费者 goroutine 的 for-range 自动退出
```

**`WithStreaming` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `bufSize` | `int` | **pool.Size() × 2**（≤0 时） | channel 缓冲区大小 |

**返回：** `*Pool[T]` — 支持链式调用

> streamCh 在 `Wait()` / `WaitTimeout()` / `WaitContext()` / `Close()` 时关闭。`bufSize` 建议不低于并发 worker 数，避免消费者跟不上导致 worker 阻塞。

### WithResultCallback — 回调消费

通过回调函数消费结果，无需自己管理 goroutine：

```go
p := async.NewPool[int](8)
p.WithResultCallback(func(r core.Result[int]) {
    if r.Ok() {
        metrics.RecordSuccess(r.Value)
    } else {
        metrics.RecordFailure(r.Err)
    }
})

// 正常提交任务即可
for i := 0; i < 1000; i++ {
    p.Submit(ctx, fn)
}
p.Wait()
```

**`WithResultCallback` 参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func(core.Result[T])` | 回调函数（每个任务完成时同步调用） |

> ⚠️ 回调在 **worker goroutine** 中同步执行，应尽量轻量，避免阻塞其他任务的执行。

### StreamResults — 获取流式 channel

```go
ch := p.StreamResults() // <-chan core.Result[T]

// 在 select 中使用
select {
case r := <-ch:
    handleResult(r)
case <-ctx.Done():
    log.Println("上下文取消")
}
```

**返回：** `<-chan core.Result[T]` — 只读结果 channel（未启用流式消费时返回 nil）

### 流式 vs 传统消费对比

| 特性 | 传统（Wait 后遍历） | 流式消费 |
|------|---------------------|----------|
| 结果获取时机 | 全部完成后 | 实时逐个获取 |
| 内存峰值 | 全部结果同时在内存中 | 结果产生即被消费 |
| 首结果延迟 | 全部完成时刻 | 首个 result 完成时刻 |
| 适用场景 | 需要全部结果做聚合 | 实时处理、流式写入 |
| Wait 行为 | 返回所有结果 | 关闭 streamCh |

---

## 环形缓冲

传统结果用 `[]core.Result[T]` 无限增长，百万级任务会产生大量内存分配和 GC 压力。**环形缓冲**（Ring Buffer）用固定容量循环覆盖，适用于**只需处理最近 N 个结果**的大批量场景。

### 核心原理

```
worker → 写入 ring buffer → 循环覆盖（满时按策略处理）
         ↓
     消费者调用 Flush() 排空 → 继续写入
```

### WithRingBuffer — 启用环形缓冲

```go
p := async.NewPool[string](8)
defer p.Close()

// 容量 10000，满时覆盖最旧结果
p.WithRingBuffer(10000, core.OverflowDrop)

for i := 0; i < 100000; i++ {
    p.Submit(ctx, fn)
}

// 等待完成（ring buffer 只保留最新 10000 条）
p.Wait()

// 排空缓冲区获取结果
results := p.Flush(0) // 0 = 取出全部
fmt.Printf("缓冲区结果数: %d\n", len(results)) // 最多 10000
```

**`WithRingBuffer` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `capacity` | `int` | 无（必传，≤0 无效） | 环形缓冲区容量 |
| `overflow` | `core.OverflowStrategy` | 无（必传） | 满时处理策略 |

**OverflowStrategy 枚举：**

| 常量 | 行为 |
|------|------|
| `core.OverflowDrop` | 覆盖最旧结果（静默丢弃） |
| `core.OverflowBlock` | 阻塞等待 Flush 排空后写入 |
| `core.OverflowError` | 满时记录错误，丢弃当前结果 |

> 启用环形缓冲后，`Wait()` 返回 nil（结果不再存储到传统 slice 中），需要通过 `Flush()` 获取结果。

### Flush — 排空缓冲区

```go
// 取出全部
results := p.Flush(0)

// 最多取出 100 条
results := p.Flush(100)

// 处理结果
for _, r := range results {
    if r.Ok() {
        db.BatchInsert(r.Value)
    }
}
```

**`Flush` 参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `maxCount` | `int` | 最多取出数量（≤0 取出全部） |

**返回：** `[]core.Result[T]` — FIFO 顺序的结果切片（未启用环形缓冲时返回 nil）

> `Flush` 是线程安全的，可在任务执行期间并发调用。排空后缓冲区为空，后续结果继续写入。

### 环形缓冲 vs 传统结果切片

| 特性 | 传统结果切片 | 环形缓冲 |
|------|-------------|----------|
| 内存占用 | O(N)，N=总任务数 | O(capacity)，固定 |
| 结果保留 | 全部保留 | 只保留最近 capacity 条 |
| GC 压力 | 高（大量分配） | 低（固定大小预分配） |
| 适用场景 | 需要全部结果的聚合计算 | 实时流式处理、日志收集 |
| Flush | 不需要 | 定期排空消费 |

---

## 背压控制

背压机制在任务提交速率超过处理速率时保护系统，防止内存无限增长。

### 核心原理

```
Submit → 检查 pending 数量 → 超过 MaxPending？
           ├── 否：正常入队
           └── 是：按 Overflow 策略处理
                    ├── OverflowBlock：阻塞等待（默认）
                    ├── OverflowDrop：静默丢弃，返回 nil
                    └── OverflowError：返回 ErrQueueOverflow
```

### WithMaxPending — 最大等待任务数

```go
p := async.NewPool[int](4)
defer p.Close()

// 最多允许 100 个任务排队等待
p.WithMaxPending(100)
```

**`WithMaxPending` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `maxPending` | `int` | **无限制**（不设置时不生效） | 最大等待任务数（≤0 不生效） |

> 当 `pending >= maxPending` 时，新提交任务触发溢出策略。

### WithOverflow — 溢出策略

```go
// 等待队列满时丢弃新任务
p.WithOverflow(core.OverflowDrop)

// 等待队列满时返回错误
p.WithOverflow(core.OverflowError)
```

**`WithOverflow` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `strategy` | `core.OverflowStrategy` | **OverflowBlock** | 溢出策略 |

**OverflowStrategy：**

| 常量 | Submit 行为 |
|------|------------|
| `core.OverflowBlock` | 默认行为：阻塞等待到队列有空位或 ctx 取消 |
| `core.OverflowDrop` | 静默丢弃，Submit 返回 nil（不报错） |
| `core.OverflowError` | Submit 返回 `core.ErrQueueOverflow` |

### QueueDepth — 当前队列深度

```go
depth := p.QueueDepth() // int，等待中的任务数
fmt.Printf("当前队列深度: %d\n", depth)
```

**返回：** `int` — 等待中的任务数（等价于 `Pending()`）

### 完整背压示例

```go
p := async.NewPool[int](4)
defer p.Close()

// 配置背压：最多 50 个等待任务，超限丢弃
p.WithMaxPending(50).WithOverflow(core.OverflowDrop)

// 模拟高速提交
for i := 0; i < 10000; i++ {
    idx := i
    err := p.Submit(ctx, func(ctx context.Context) (int, error) {
        time.Sleep(100 * time.Millisecond) // 慢速处理
        return idx * idx, nil
    })
    if err != nil {
        log.Printf("任务 %d 被拒绝: %v", idx, err)
    }
}

results := p.Wait()
fmt.Printf("实际完成: %d\n", len(results))
// 由于 OverflowDrop，部分任务被丢弃，实际完成数 < 10000
```

---

## 查询与统计

### Size / Active / Busy / Pending

**`Size`** — Worker 数量（并发度）

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 当前 worker 数 |

```go
fmt.Printf("Worker 数量: %d\n", p.Size()) // → 10
```

**`Active`** — 活跃任务数（所有已提交且未完成的任务，含阻塞排队、执行中和 pending）

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 活跃任务数 |

```go
fmt.Printf("活跃数: %d\n", p.Active())
```

**`Busy`** — 繁忙 worker 数（正在执行 fn 的 worker）

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 繁忙 worker 数，≤ Size() |

```go
fmt.Printf("繁忙数: %d\n", p.Busy())
```

> 空闲 worker 数可推算为 `Size() - Busy()`。

**`Pending`** — 排队等待的任务数

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 在队列中等待分配 worker 的任务数 |

```go
fmt.Printf("排队数: %d\n", p.Pending())
```

### Stats - 完整统计

**`Stats`** — 返回 PoolStats 结构体

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `PoolStats` | `PoolStats` | 池的完整统计信息快照 |

**PoolStats 结构体字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `Size` | `int` | Worker 数量 |
| `Active` | `int` | 活跃任务数 |
| `Busy` | `int` | 繁忙 worker 数 |
| `Pending` | `int` | 排队任务数 |
| `FailFast` | `bool` | 是否启用 FailFast |
| `Timeout` | `time.Duration` | 全局任务超时时间 |
| `TotalTask` | `int64` | 历史提交任务总数 |
| `SuccessTask` | `int64` | 历史成功任务数 |
| `FailTask` | `int64` | 历史失败任务数 |

```go
stats := p.Stats()
fmt.Printf("PoolStats{size=%d, active=%d, busy=%d, pending=%d, failFast=%v, timeout=%v, total=%d, success=%d, fail=%d}\n",
    stats.Size, stats.Active, stats.Busy, stats.Pending,
    stats.FailFast, stats.Timeout,
    stats.TotalTask, stats.SuccessTask, stats.FailTask)
```

### FailCount / SuccessCount / TotalCount / HasError

**`FailCount`** — 失败任务数量

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | Err != nil 的任务数 |

**`SuccessCount`** — 成功任务数量

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | Err == nil 的任务数 |

**`TotalCount`** — 总任务数量

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int64` | `int64` | 历史提交的任务总数（含成功+失败） |

**`HasError`** — 是否存在错误

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `bool` | `bool` | FailCount() > 0 时返回 true |

```go
results := p.Wait()

fmt.Printf("总数: %d, 成功: %d, 失败: %d\n",
    p.TotalCount(), p.SuccessCount(), p.FailCount())

if p.HasError() {
    log.Printf("存在失败的任务")
}
```

---

## 结果提取

### Values - 提取所有成功值

**`Values`** — 提取所有 Err==nil 的值，跳过失败项

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]T` | `[]T` | 所有成功结果的值（顺序为任务完成顺序，非提交顺序） |

```go
results := p.Wait()
values := p.Values() // []T，只包含 Err==nil 的值
fmt.Printf("成功值: %v\n", values)
```

### Errors - 提取所有错误

**`Errors`** — 提取所有非 nil 错误

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]error` | `[]error` | 所有 Err != nil 的错误（跳过 nil） |

```go
errs := p.Errors() // []error
for i, e := range errs {
    log.Printf("错误 %d: %v", i, e)
}
```

### FirstError - 第一个错误

**`FirstError`** — 返回首个非 nil 错误

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 首个 Err != nil 的错误（全部成功时返回 nil） |

```go
if firstErr := p.FirstError(); firstErr != nil {
    log.Printf("首个错误: %v", firstErr)
}
```

### JoinErrors - 合并所有错误

**`JoinErrors`** — 将所有错误合并为单个 error（使用 `errors.Join`）

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | `errors.Join` 合并后的错误（全部成功时返回 nil） |

```go
if err := p.JoinErrors(); err != nil {
    // 使用 errors.Is / errors.As 判断具体错误
    if errors.Is(err, async.ErrTimeout) {
        log.Println("存在超时错误")
    }
}
```

---

## 动态扩容

### Resize - 调整 Worker 数量

运行时动态调整 worker 数量。扩容立即生效，缩容会通知多余 worker 退出：

**`Resize`** — 调整 Pool worker 数量

| 参数 | 类型 | 说明 |
|------|------|------|
| `newSize` | `int` | 新的 worker 数量（ > 0） |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 调整前（旧）的 worker 数量 |

```go
// 扩容到 20 个 worker
oldSize := p.Resize(20)
fmt.Printf("从 %d 扩容到 20\n", oldSize)

// 缩容到 5 个 worker
p.Resize(5)
```

### ResizeAndWaitTimeout - 调整并等待

调整大小后等待指定时间让旧 worker 清理退出：

**`ResizeAndWaitTimeout`** — 调整大小并等待旧 worker 退出

| 参数 | 类型 | 说明 |
|------|------|------|
| `newSize` | `int` | 新的 worker 数量 |
| `timeout` | `time.Duration` | 等待旧 worker 退出的最大时长 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| （无） | — | — |

```go
// 缩容到 5，等待 10 秒让旧 worker 退出
p.ResizeAndWaitTimeout(5, 10*time.Second)
```

---

## 自动扩缩容 (AutoScale)

Pool 支持根据负载**自动调整 worker 数量**。初始 worker 数可以很小（如 4），高并发时自动扩容，低负载时自动缩容。**默认不启用**，需显式调用 `EnableAutoScale`。

### EnableAutoScale - 启用自动扩缩容

```go
p := async.NewPool[int](4)
defer p.Close()

// 方式一：使用默认配置启用
p.EnableAutoScale(nil)

// 方式二：自定义配置
p.EnableAutoScale(&async.AutoScaleConfig{
    MinWorkers:       4,
    MaxWorkers:       2000,
    CheckInterval:    5 * time.Second,
    ScaleUpThreshold: 0.7,   // busy/size > 0.7 触发扩容
    ScaleDownThreshold: 0.2, // busy/size < 0.2 触发缩容
    ScaleUpChecks:    3,     // 连续 3 次满足条件才扩容（防抖动）
    ScaleDownChecks:  5,     // 连续 5 次满足条件才缩容（防抖动）
})
```

**扩容规则**：busy/size 比率超过 `ScaleUpThreshold` 持续 `ScaleUpChecks` 次 → worker 翻倍（上限 MaxWorkers）  
**缩容规则**：busy/size 比率低于 `ScaleDownThreshold` 持续 `ScaleDownChecks` 次 → worker 减半（下限 MinWorkers）

### 默认配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MinWorkers` | **CPU × 2** | 最小 worker 数，缩容不低于此值 |
| `MaxWorkers` | **CPU × 100** | 最大 worker 数，扩容不超此值 |
| `CheckInterval` | **5s** | 后台检测间隔 |
| `ScaleUpThreshold` | **0.7** | busy/total 超过此比例触发扩容计数 |
| `ScaleDownThreshold` | **0.2** | busy/total 低于此比例触发缩容计数 |
| `ScaleUpChecks` | **3** | 连续触发扩容次数（防抖动） |
| `ScaleDownChecks` | **5** | 连续触发缩容次数（防抖动） |

```go
// 传 nil 使用默认配置
p.EnableAutoScale(nil)

// 获取默认配置
config := async.DefaultAutoScaleConfig()
```

**扩容规则**：busy/size 比率超过 `ScaleUpThreshold` 持续 `ScaleUpChecks` 次 → worker 翻倍（上限 MaxWorkers）  
**缩容规则**：busy/size 比率低于 `ScaleDownThreshold` 持续 `ScaleDownChecks` 次 → worker 减半（下限 MinWorkers）

### IsAutoScaleEnabled - 检查状态

```go
if p.IsAutoScaleEnabled() {
    fmt.Println("auto-scale is active")
}
```

### NewAutoScalePool - 快捷创建

```go
// 创建初始 4 worker、自动扩缩容的池（config 为 nil 时使用默认配置）
p := async.NewAutoScalePool[int](4, nil)
defer p.Close()

// 自定义配置
p2 := async.NewAutoScalePool[int](4, &async.AutoScaleConfig{
    MinWorkers: 2,
    MaxWorkers: 500,
})
```

> `config` 为 `nil` 时使用 `DefaultAutoScaleConfig()`，详见上方默认配置表格。

### 线程安全

`EnableAutoScale` 可重复调用（幂等），`Resize` 与自动扩缩容并发调用安全，`Close` 自动停止后台检测 goroutine。

---

## 重置

### Reset - 关闭旧池创建新池

Wait 后需要继续使用池时调用 Reset：

**`Reset`** — 关闭旧池并创建同配置的新池

| 参数 | 类型 | 说明 |
|------|------|------|
| （无参数） | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*Pool[T]` | `*Pool[T]` | 新创建的 Pool（同 size 配置） |
| `error` | `error` | 关闭旧池时可能的错误 |

```go
p.Submit(ctx, task1)
results := p.Wait()

// Wait 后再次使用需要 Reset
newPool, err := p.Reset()
if err != nil {
    log.Printf("重置失败: %v", err)
}
defer newPool.Close()
newPool.Submit(ctx, task2)
newPool.Wait()
```

---

## NoResultPool

无返回值协程池 `Pool[struct{}]` 的别名，适合只关心错误的场景。推荐配合辅助函数使用。

### 使用辅助函数提交

```go
p := async.NewNoResultPool(10)
defer p.Close()

// SubmitAction: 提交无返回值动作（阻塞）
async.SubmitAction(p, ctx, func(ctx context.Context) error {
    return sendEmail(ctx, user)
})

// TrySubmitAction: 非阻塞提交
err := async.TrySubmitAction(p, ctx, func(ctx context.Context) error {
    return logMetrics(ctx, data)
})
if err != nil {
    log.Printf("提交失败: %v", err)
}

// SubmitAtAction: 指定位置提交
async.SubmitAtAction(p, 0, ctx, func(ctx context.Context) error {
    return processFirst(ctx, input)
})

// TrySubmitAtAction: 指定位置非阻塞提交
async.TrySubmitAtAction(p, 1, ctx, func(ctx context.Context) error {
    return processSecond(ctx, input)
})

// GoAction: 提交并断言成功（失败 panic）
async.GoAction(p, ctx, func(ctx context.Context) error {
    return mustSucceed(ctx, data)
})

// SubmitActionWithTimeout: 带超时提交
async.SubmitActionWithTimeout(p, ctx, 5*time.Second, func(ctx context.Context) error {
    return heavyOperation(ctx)
})

// SubmitAtActionWithTimeout: 指定位置带超时提交
async.SubmitAtActionWithTimeout(p, 0, ctx, 3*time.Second, func(ctx context.Context) error {
    return criticalTask(ctx)
})

// GoActionWithTimeout: 带超时提交并断言成功
async.GoActionWithTimeout(p, ctx, 5*time.Second, func(ctx context.Context) error {
    return mustSucceedWithTimeout(ctx)
})

// 等待完成
p.Wait()

// 检查结果
if p.HasError() {
    log.Printf("首个错误: %v", p.FirstError())
}
fmt.Printf("成功: %d, 失败: %d\n", p.SuccessCount(), p.FailCount())
```

**NoResultPool 辅助函数参数速查：**

| 函数 | 参数 | 说明 |
|------|------|------|
| `SubmitAction(p, ctx, fn)` | `p *NoResultPool`, `ctx`, `fn func(ctx) error` | 阻塞提交，队列满时阻塞等待 |
| `TrySubmitAction(p, ctx, fn)` | 同上 | 非阻塞提交，队列满返回 ErrSubmitTimeout |
| `SubmitAtAction(p, idx, ctx, fn)` | `p`, `idx int`, `ctx`, `fn` | 指定结果数组索引位置阻塞提交 |
| `TrySubmitAtAction(p, idx, ctx, fn)` | 同上 | 指定位置非阻塞提交 |
| `GoAction(p, ctx, fn)` | `p`, `ctx`, `fn` | 非阻塞提交，失败时 panic |
| `SubmitActionWithTimeout(p, ctx, d, fn)` | `p`, `ctx`, `d time.Duration`, `fn` | 带超时的阻塞提交 |
| `SubmitAtActionWithTimeout(p, idx, ctx, d, fn)` | `p`, `idx`, `ctx`, `d`, `fn` | 指定位置带超时提交 |
| `GoActionWithTimeout(p, ctx, d, fn)` | `p`, `ctx`, `d`, `fn` | 带超时提交，失败 panic |

## 便捷函数

一行代码快速创建池并提交任务：

### Submit - 快速创建池提交单个任务

创建 `Pool[T]` 并提交一个任务，返回池实例、任务索引和错误：

```go
p, idx, err := async.Submit(ctx, func(ctx context.Context) (string, error) {
    return fetchData(ctx), nil
})
if err != nil {
    log.Printf("提交失败: %v", err)
}
defer p.Close()
results := p.Wait()
fmt.Printf("任务在位置 %d, 结果: %v\n", idx, results[idx].Value)
```

**参数：**
- `ctx` — 上下文
- `fn` — 任务函数 `func(context.Context) (T, error)`

**返回：**
- `*Pool[T]` — 新创建的池（需手动 `Close()`）
- `int` — 任务在结果数组中的索引
- `error` — 提交错误

### SubmitN - 快速提交 N 个相同任务

创建池并提交 N 个相同函数（每个独立执行）：

```go
p, results, err := async.SubmitN(ctx, func(ctx context.Context) (int, error) {
    return rand.Intn(100), nil
}, 10) // 提交 10 个任务
defer p.Close()

for _, r := range results {
    if r.Err != nil {
        log.Printf("任务 %d 提交失败: %v", r.Index, r.Err)
    }
}
waitResults := p.Wait()
```

**参数：**
- `ctx` — 上下文
- `fn` — 任务函数
- `n` — 提交数量

**返回：**
- `*Pool[T]` — 池实例
- `[]SubmitResult` — 每个提交的结果（Index + Err）
- `error` — 整体错误（预留）

### SubmitSafeN - 提交 N 个（忽略提交失败）

与 `SubmitN` 类似，但不返回 error，内部自动忽略提交失败：

```go
p, results := async.SubmitSafeN(ctx, fn, 10)
defer p.Close()

for _, r := range results {
    if r.Err != nil {
        // 某些提交可能失败，但不会打断整体流程
        log.Printf("索引 %d 提交失败: %v", r.Index, r.Err)
    }
}
waitResults := p.Wait()
```

**参数：**
- `ctx` — 上下文
- `fn` — 任务函数
- `n` — 提交数量

### SubmitBatch - 批量提交切片元素

对切片每个元素提交一个任务，适合数据驱动的批处理：

```go
items := []string{"url1", "url2", "url3", "url4"}

p, results, err := async.SubmitBatch(ctx, items, func(ctx context.Context, url string) (string, error) {
    return httpGet(ctx, url)
})
defer p.Close()

if err != nil {
    log.Printf("批量提交失败: %v", err)
}

waitResults := p.Wait()
for _, r := range waitResults {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

**参数：**
- `ctx` — 上下文
- `items` — 输入切片 `~[]E`（支持任何底层为切片的类型）
- `fn` — 处理函数 `func(context.Context, E) (T, error)`

### MapPool - 池化 Map

使用 Pool 执行并发 Map，可获取池实例进行统计：

```go
p, results, err := async.MapPool(ctx, urls, func(ctx context.Context, url string) (string, error) {
    return httpGet(ctx, url)
}, async.IO())
// p 已自动 Wait + Close

fmt.Printf("总数: %d, 成功: %d, 失败: %d\n",
    p.TotalCount(), p.SuccessCount(), p.FailCount())
```

**参数：**
- `ctx` — 上下文
- `items` — 输入切片
- `fn` — 转换函数
- `concurrency` — 并发度

### ForEachPool - 池化 ForEach

使用 NoResultPool 执行并发遍历：

```go
p, err := async.ForEachPool(ctx, users, func(ctx context.Context, user string) error {
    return sendNotification(ctx, user)
}, async.IO())
// p 已自动 Wait + Close

fmt.Printf("推送完成: 成功=%d 失败=%d\n", p.SuccessCount(), p.FailCount())
```

**参数：**
- `ctx` — 上下文
- `items` — 输入切片
- `fn` — 操作函数
- `concurrency` — 并发度

**SubmitResult 类型：**

```go
type SubmitResult struct {
    Index int   // 任务在结果数组中的位置
    Err   error // 提交错误（nil = 成功）
}
```

---

## 完整示例

### 示例1：Worker 池处理 HTTP 请求

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/chichengyu/async"
)

func main() {
    ctx := context.Background()
    ctx = async.EnsureTraceID(ctx)

    // 创建 4 个 worker 的池，任务超时 5 秒
    p := async.NewPool[int](4)
    defer p.Close()
    p.WithTimeout(5 * time.Second)

    // 提交 100 个任务
    for i := 0; i < 100; i++ {
        idx := i
        err := p.Submit(ctx, func(ctx context.Context) (int, error) {
            time.Sleep(100 * time.Millisecond)
            return idx * idx, nil
        })
        if err != nil {
            log.Printf("任务 %d 提交失败: %v", idx, err)
        }
    }

    // 等待并获取结果
    results := p.Wait()

    // 处理结果
    for _, r := range results {
        if r.Ok() {
            fmt.Printf("结果: %d\n", r.Value)
        } else {
            log.Printf("失败: %v", r.Err)
        }
    }

    // 统计
    fmt.Printf("总数: %d, 成功: %d, 失败: %d, 首个错误: %v\n",
        p.TotalCount(), p.SuccessCount(), p.FailCount(), p.FirstError())
}
```

### 示例2：FailFast + 动态扩容

```go
func processWithFailFast() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    p := async.NewPool[string](4)
    defer p.Close()

    // 开启 FailFast + 超时
    p, ffCtx := p.WithFFTimeout(ctx, 30*time.Second)

    // 加载中扩容
    p.Resize(8)

    urls := []string{"url1", "url2", "url3"}
    for i, url := range urls {
        p.SubmitAt(i, ffCtx, func(ctx context.Context) (string, error) {
            return fetchURL(ctx, url)
        })
    }

    results := p.Wait()

    // 检查统计
    stats := p.Stats()
    if stats.FailTask > 0 {
        log.Printf("有 %d 个任务失败: %v", stats.FailTask, p.FirstError())
    }

    for _, r := range results {
        if r.Ok() {
            fmt.Println(r.Value)
        }
    }
}
```

### 示例3：NoResultPool 批量处理

```go
func batchSendEmails(users []string) {
    ctx := context.Background()
    p := async.NewNoResultPool(10)
    defer p.Close()

    for _, user := range users {
        u := user
        async.SubmitAction(p, ctx, func(ctx context.Context) error {
            return emailService.Send(ctx, u, "Hello!")
        })
    }

    p.Wait()

    if p.HasError() {
        log.Printf("发送失败: %v", p.FirstError())
    }
    fmt.Printf("发送: 成功 %d, 失败 %d\n", p.SuccessCount(), p.FailCount())
}
```

---

## Pool 方法速查表

### 创建函数

| 函数 | 完整签名 |
|------|---------|
| `NewPool[T]` | `func NewPool[T any](size int) *Pool[T]` |
| `DefaultPool[T]` | `func DefaultPool[T any]() *Pool[T]` |
| `NewNoResultPool` | `func NewNoResultPool(size int) *NoResultPool` |
| `DefaultNoResultPool` | `func DefaultNoResultPool() *NoResultPool` |
| `NewAutoScalePool[T]` | `func NewAutoScalePool[T any](initialSize int, config *AutoScaleConfig) *Pool[T]` |

### Pool[T] 提交方法

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `Submit` | `func (p *Pool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error` | 阻塞提交，队列满等待直到 submit 超时 |
| `TrySubmit` | `func (p *Pool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error` | 非阻塞提交，队列满立即返回 ErrSubmitTimeout |
| `SubmitAt` | `func (p *Pool[T]) SubmitAt(index int, ctx context.Context, fn func(context.Context) (T, error)) error` | 阻塞提交到指定索引位置 |

### Pool[T] 等待与关闭方法

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `Wait` | `func (p *Pool[T]) Wait() []core.Result[T]` | 阻塞等待，返回全部结果 |
| `WaitTimeout` | `func (p *Pool[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool)` | 带超时等待，第二个返回值指示是否在超时前完成 |
| `WaitContext` | `func (p *Pool[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool)` | Context 控制等待 |
| `WaitAndClose` | `func (p *Pool[T]) WaitAndClose() []core.Result[T]` | Wait 后自动 Close |
| `Close` | `func (p *Pool[T]) Close()` | 关闭任务队列，等待已提交任务执行完成 |
| `CloseAndWait` | `func (p *Pool[T]) CloseAndWait()` | 关闭后等待 worker 处理完剩余任务 |
| `CloseAndWaitTimeout` | `func (p *Pool[T]) CloseAndWaitTimeout(timeout time.Duration) (ok bool, workerDone <-chan struct{})` | 带超时关闭并等待 |
| `CloseByIdle` | `func (p *Pool[T]) CloseByIdle(timeout time.Duration)` | 等待空闲后关闭，最多等待 timeout |

### Pool[T] 选项链式方法（返回新 Pool + Context）

| 方法 | 完整签名 |
|------|---------|
| `WithTraceID` | `func (p *Pool[T]) WithTraceID(ctx context.Context) (*Pool[T], context.Context)` |
| `WithContext` | `func (p *Pool[T]) WithContext(ctx context.Context) (*Pool[T], context.Context)` |
| `WithFailFast` | `func (p *Pool[T]) WithFailFast(ctx context.Context) (*Pool[T], context.Context)` |
| `WithFFCtx` | `func (p *Pool[T]) WithFFCtx(ctx context.Context) (*Pool[T], context.Context)` |
| `WithFFTraceID` | `func (p *Pool[T]) WithFFTraceID(ctx context.Context) (*Pool[T], context.Context)` |
| `WithFFSubmitTO` | `func (p *Pool[T]) WithFFSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context)` |
| `WithFFSubmitTOTraceID` | `func (p *Pool[T]) WithFFSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context)` |
| `WithFFTimeout` | `func (p *Pool[T]) WithFFTimeout(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context)` |
| `WithCtxTraceID` | `func (p *Pool[T]) WithCtxTraceID(ctx context.Context) (*Pool[T], context.Context)` |
| `WithFFTimeoutTraceID` | `func (p *Pool[T]) WithFFTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context)` |
| `WithFFTimeoutSubmitTO` | `func (p *Pool[T]) WithFFTimeoutSubmitTO(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context)` |
| `WithFFTimeoutSubmitTOTraceID` | `func (p *Pool[T]) WithFFTimeoutSubmitTOTraceID(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context)` |
| `WithCtxTimeout` | `func (p *Pool[T]) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context)` |
| `WithCtxTimeoutTraceID` | `func (p *Pool[T]) WithCtxTimeoutTraceID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context)` |
| `WithCtxSubmitTO` | `func (p *Pool[T]) WithCtxSubmitTO(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context)` |
| `WithCtxSubmitTOTraceID` | `func (p *Pool[T]) WithCtxSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context)` |

### Pool[T] 选项链式方法（返回修改后的 Pool）

| 方法 | 完整签名 |
|------|---------|
| `WithTimeout` | `func (p *Pool[T]) WithTimeout(d time.Duration) *Pool[T]` |
| `WithSubmitTimeout` | `func (p *Pool[T]) WithSubmitTimeout(d time.Duration) *Pool[T]` |

### Pool[T] 查询/监控方法

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `Size` | `func (p *Pool[T]) Size() int` | Worker 数量 |
| `Active` | `func (p *Pool[T]) Active() int` | 活跃 worker 数（含等待队列长度） |
| `Busy` | `func (p *Pool[T]) Busy() int` | 繁忙 worker 数 |
| `Pending` | `func (p *Pool[T]) Pending() int` | 排队任务数 |
| `Stats` | `func (p *Pool[T]) Stats() PoolStats` | 完整统计信息 |

### Pool[T] 结果提取方法

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `Values` | `func (p *Pool[T]) Values() []T` | 所有成功值（无序） |
| `Errors` | `func (p *Pool[T]) Errors() []error` | 所有非 nil 错误 |
| `FirstError` | `func (p *Pool[T]) FirstError() error` | 首个错误（可能为 nil） |
| `JoinErrors` | `func (p *Pool[T]) JoinErrors() error` | 合并所有错误为一个 error |
| `FailCount` | `func (p *Pool[T]) FailCount() int64` | 失败数 |
| `SuccessCount` | `func (p *Pool[T]) SuccessCount() int64` | 成功数 |
| `TotalCount` | `func (p *Pool[T]) TotalCount() int64` | 总任务数 |
| `HasError` | `func (p *Pool[T]) HasError() bool` | 是否有错误 |

### Pool[T] 动态管理方法

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `Resize` | `func (p *Pool[T]) Resize(newSize int) int` | 调整 worker 数量，返回旧大小 |
| `ResizeAndWaitTimeout` | `func (p *Pool[T]) ResizeAndWaitTimeout(newSize int, timeout time.Duration)` | 调整并等待旧 worker 退出 |
| `Reset` | `func (p *Pool[T]) Reset() (*Pool[T], error)` | 关闭旧池并创建同配置新池 |
| `EnableAutoScale` | `func (p *Pool[T]) EnableAutoScale(config *core.AutoScaleConfig)` | 启用自动扩缩容（传 nil 使用默认配置） |
| `DisableAutoScale` | `func (p *Pool[T]) DisableAutoScale()` | 停止并禁用自动扩缩容 |
| `IsAutoScaleEnabled` | `func (p *Pool[T]) IsAutoScaleEnabled() bool` | 查询自动扩缩容是否已启用 |

### NoResultPool 辅助函数

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `SubmitAction` | `func SubmitAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error` | 阻塞提交无返回值动作 |
| `TrySubmitAction` | `func TrySubmitAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error` | 非阻塞提交 |
| `SubmitAtAction` | `func SubmitAtAction(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error` | 指定位置阻塞提交 |
| `TrySubmitAtAction` | `func TrySubmitAtAction(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error` | 指定位置非阻塞提交 |
| `GoAction` | `func GoAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error)` | 提交并断言成功（失败则 panic） |
| `SubmitActionWithTimeout` | `func SubmitActionWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error` | 带超时提交 |
| `SubmitAtActionWithTimeout` | `func SubmitAtActionWithTimeout(p *NoResultPool, index int, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error` | 指定位置带超时提交 |
| `GoActionWithTimeout` | `func GoActionWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error)` | 带超时提交并断言成功 |

### 顶层便捷函数

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `Submit[T]` | `func Submit[T any](ctx context.Context, fn func(context.Context) (T, error)) (*Pool[T], int, error)` | 快速创建池提交单个任务，返回池+索引+错误 |
| `SubmitN[T]` | `func SubmitN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult, error)` | 提交 n 个相同任务 |
| `SubmitSafeN[T]` | `func SubmitSafeN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult)` | 提交 n 个（忽略提交失败） |
| `SubmitBatch[T, S]` | `func SubmitBatch[T any, S ~[]E, E any](ctx context.Context, items S, fn func(context.Context, E) (T, error)) (*Pool[T], []SubmitResult, error)` | 批量提交切片元素 |
| `MapPool[T, R]` | `func MapPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) (*Pool[R], []core.Result[R], error)` | 池化 Map，返回 Pool 和结果 |
| `ForEachPool[T]` | `func ForEachPool[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (*NoResultPool, error)` | 池化 ForEach，返回 NoResultPool |

### 类型定义

| 类型 | 定义 |
|------|------|
| `Pool[T]` | `type Pool[T any] = pool.Pool[T]` |
| `NoResultPool` | `type NoResultPool = pool.Pool[struct{}]` |
| `PoolStats` | `struct{ Size, Active, Busy, Pending int; FailFast bool; Timeout time.Duration; TotalTask, SuccessTask, FailTask int64 }` |
| `SubmitResult` | `struct{ Index int; Err error }` |