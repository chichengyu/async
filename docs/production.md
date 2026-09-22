# 生产注意事项

> **⚠️ 以下问题在默认配置下可能导致线上 OOM、goroutine 泄漏或数据丢失，请务必在生产上线前完成配置。**

---

## 目录

- 🔴 [内存控制（OOM 风险）](#oom)
- 🔴 [Pool vs Group 选型边界](#pool-vs-group)
- 🔴 [生命周期必须成对](#lifecycle)
- 🔴 [异步任务（GoResult / GoCancel）生命周期](#async-task)
- 🟡 [流式消费丢数据](#streaming)
- 🟡 [分片不保序](#shard-order)
- 🟡 [BoundedRunner 内存](#bounded-runner)
- 🟡 [Close vs CloseAndWait](#close-vs-closewait)
- 🟡 [FailFast 会丢弃成功结果](#failfast)
- 🟡 [Retry 关键边界](#retry)
- 🟡 [ShardedPool / ShardedGroup 资源总量](#shardedpool)
- 🟢 [生产推荐全局配置](#global-config)
- 🟢 [生产推荐 Pool 完整配置](#pool-config)
- 🟢 [其他注意点速查](#quickref)

---

<a id="oom"></a>

## 🔴 内存控制（OOM 风险）

**WithMaxResults 默认值为 0（无限制）**，所有任务结果永久保留在内存中：

```go
// ❌ 危险：长期运行，千万条结果在内存中，最终 OOM
p := async.NewPool[string](8)
for { p.Submit(ctx, heavyTask) }

// ✅ 必须至少选择一种内存控制策略
p := async.NewPool[string](8).
    WithMaxResults(100_000).                        // 方案1：限制结果切片
    WithRingBuffer(50_000, async.OverflowDrop).     // 方案2：环形缓冲兜底
    WithStreaming(4096)                              // 方案3：流式实时消费
```

> **`WithRingBuffer` 不限制 results 切片**——环形缓冲和 results 切片是两个并行的存储通道，必须同时配置 `WithMaxResults`。

---

<a id="pool-vs-group"></a>

## 🔴 Pool vs Group 选型边界

| 场景 | 推荐 | 原因 |
|------|------|------|
| 任务数 < 5 万，一次性批量 | **Group** | 用完销毁，编码简单 |
| 任务数 > 5 万，或长期运行 | **Pool** | goroutine 复用，无堆积风险 |
| 海量任务（百万/千万） | **Pool + RingBuffer** | Group 每任务新 goroutine，内存爆炸 |

```go
// ❌ 危险：100万任务 → 100万 goroutine → 内存爆炸
g := async.NewGroup[int](500)
for i := 0; i < 1_000_000; i++ { g.Go(ctx, fn) }

// ✅ 海量任务用 Pool
p := async.NewPool[int](async.IO()).
    WithMaxResults(100_000).
    WithRingBuffer(50_000, async.OverflowDrop)
for i := 0; i < 1_000_000; i++ { p.Submit(ctx, fn) }
```

---

<a id="lifecycle"></a>

## 🔴 生命周期必须成对

| 组件 | 创建后必须调用 | 泄漏后果 |
|------|---------------|---------|
| **Pool** | `Close()` / `WaitAndClose()` / `CloseAndWait()` | worker goroutine 永久泄漏 |
| **RateLimiter** | `Close()` | ticker goroutine 泄漏 |
| **BoundedRunner** | 不依赖 Close，但需 `Wait()` | goroutine 未完成即退出 |

```go
// ✅ Pool 推荐 defer Close
p := async.NewPool[string](8)
defer p.Close()  // 或 defer p.CloseAndWait()

// ✅ RateLimiter 必须 Close
rl := async.NewRateLimiter(100, time.Second)
defer rl.Close()
```

---

<a id="async-task"></a>

## 🔴 异步任务（GoResult / GoCancel）生命周期

`GoResult` 和 `GoCancel` 内部启动了 goroutine，必须配对清理：

```go
// ❌ 泄漏：GoResult 不 Wait，goroutine 永久挂起
async.GoResult(ctx, heavyTask) // 忘记 Wait()

// ✅ 必须 Wait 获取结果
ar := async.GoResult(ctx, heavyTask)
result, err := ar.Wait()

// ❌ 泄漏：GoCancel 不 Cancel 也不取结果
task := async.GoCancel[int](ctx, heavyTask) // 忘记 Cancel()

// ✅ 二选一：Cancel 或取结果
task := async.GoCancel[int](ctx, heavyTask)
defer task.Cancel() // 确保取消
```

| 函数 | 必须操作 | 忘记后果 |
|------|---------|---------|
| `GoResult[T]()` | `Wait()` / `WaitTimeout()` | goroutine 泄漏 |
| `GoCancel[T]()` | `Cancel()` 或取结果 | goroutine 泄漏 |
| `GoCancelErr()` | `Cancel()` 或取结果 | goroutine 泄漏 |
| `Go()` / `GoWithTimeout()` | 无需 Wait（进程退出即终止） | 仅日志记录 panic |

> **⚠️ Fire-and-Forget 并非真遗忘**：`Go()` 返回的 `*TaskVoid` 中如果发生 panic，**不会**传播给调用方，仅记录日志。需要感知失败请用 `GoResult`。

---

<a id="streaming"></a>

## 🟡 流式消费丢数据

`WithStreaming` 使用非阻塞写入，消费者慢时**静默丢弃**结果：

```go
p := async.NewPool[string](8).WithStreaming(4096)
results := make(chan core.Result[string], 4096)
p.SetStream(results)

// ⚠️ 监控丢弃量
go func() {
    ticker := time.NewTicker(10 * time.Second)
    for range ticker.C {
        if d := p.StreamDropped(); d > 0 {
            log.Printf("[ALERT] dropped %d stream results", d)
        }
    }
}()

// 消费者必须足够快
for r := range results {
    process(r)
}
```

---

<a id="shard-order"></a>

## 🟡 分片不保序

`MultiPool`、`MultiGroup`、`ParallelPipeline` 的分片模式（`shards >= 2`）下，各分片独立执行，**最终结果不保证与输入顺序一致**。需要保序请使用非分片版本或不调用 `.Shard()`。

---

<a id="bounded-runner"></a>

## 🟡 BoundedRunner 内存

`BoundedRunner` 限制的是**同时运行**的 goroutine 数，不是总任务数。一次性提交千万任务仍需存储等量 `*AsyncResult[T]`：

```go
runner := async.NewBoundedRunner(1000)
results := make([]*async.AsyncResult[int], 0, 10_000_000) // 注意内存
for i := 0; i < 10_000_000; i++ {
    results = append(results, async.BoundedGo(runner, ctx, fn))
}
```

---

<a id="close-vs-closewait"></a>

## 🟡 Close vs CloseAndWait：丢弃正在执行的任务

`Close()` 和 `CloseAndWait()` 行为不同，选错会丢数据：

```go
p := async.NewPool[string](8)
p.Submit(ctx, longRunningTask) // 正在执行中...

// ❌ Close()：立即停止 worker，丢弃正在执行的任务
p.Close()

// ✅ CloseAndWait()：等待所有已提交任务完成后再关闭
results := p.CloseAndWait()
```

| 方法 | 等待已提交任务 | 等待正在执行任务 | 丢弃后果 |
|------|:---:|:---:|---|
| `Close()` | ❌ | ❌ | 已提交+执行中任务全部丢弃 |
| `WaitAndClose()` | ✅ | ✅ | 等待完成，推荐 |
| `CloseAndWait()` | ✅ | ✅ | 等待完成，推荐 |

---

<a id="failfast"></a>

## 🟡 FailFast 会丢弃成功结果

任意一个任务失败后，FailFast 会取消所有其他任务，**已成功的任务结果被丢弃**：

```go
// ❌ 误解：以为能拿到部分成功结果
results, err := async.MapWithFailFast(ctx, items, async.IO(), fn)
// err != nil 时，results 是 nil，成功的 999 个结果全丢了

// ✅ 需要保留部分成功结果，用普通 Map + 业务层处理
results := async.Map(ctx, items, async.IO(), fn)
successes, failures := async.Partition(results) // 手动分区
```

---

<a id="retry"></a>

## 🟡 Retry 关键边界

```go
// ❌ 陷阱1：MaxRetries=0 表示"不重试"，不是"无限重试"
async.RetryWithBackoff(ctx, 0, 100*time.Millisecond, fn) // 只执行一次

// ❌ 陷阱2：Context 取消会立即中止所有重试
ctx, cancel := context.WithTimeout(parentCtx, 1*time.Second)
async.RetryWithBackoff(ctx, 10, 1*time.Second, slowFn) // 1s后强制中止

// ⚠️ 陷阱3：panic 被自动捕获，不会中断重试流程
// panic 达到 MaxRetries+1 次后才返回 *PanicError
```

---

<a id="shardedpool"></a>

## 🟡 ShardedPool / ShardedGroup 资源总量

分片后的总资源是 **分片数 × 每分片资源**：

```go
p := async.NewPool[int](100)
mp := p.Shard(8) // 8分片 × 100 worker = 800 goroutine 上限

g := async.NewGroup[int](500)
mg := g.Shard(8) // 8分片 × 500 并发 = 4000 goroutine 上限

// ⚠️ Hash 路由不保证均匀：热点 key 会导致部分分片过载
mp.SubmitKeyed(hash(userID), ctx, fn) // 同一 userID 固定路由到同一分片
```

---

<a id="global-config"></a>

## 🟢 生产推荐全局配置

```go
func init() {
    async.SetDefaultTimeout(30 * time.Second)        // 默认超时
    async.SetSubmitTimeout(5 * time.Second)           // 提交超时
    async.SetTaskFailLogLevel(async.LogLevelWarn)     // 降级日志减少噪音
    async.SetTraceLogEnabled(false)                   // 关闭 Trace
    async.SetLogger(myProductionLogger)               // 注入自定义 Logger
}
```

---

<a id="pool-config"></a>

## 🟢 生产推荐 Pool 完整配置

```go
p := async.NewPool[MyType](async.IO()).
    WithTimeout(30 * time.Second).              // 单个任务超时
    WithSubmitTimeout(5 * time.Second).         // 提交超时
    WithMaxPending(10_000).                     // 最大排队数
    WithOverflow(async.OverflowError).          // 超出返回错误
    WithMaxResults(100_000).                    // 限制结果切片（默认0=无限！）
    WithRingBuffer(50_000, async.OverflowDrop). // 环形缓冲兜底
    WithResultCallback(recordMetrics)            // 轻量回调
defer p.CloseAndWait()
```

---

<a id="quickref"></a>

## 🟢 其他注意点速查

| 注意点 | 说明 |
|--------|------|
| **Wait 一次性** | `Wait()` 只能调用一次，需重复提交使用 `Reset()` |
| **Map/ForEach 不保序** | 并发版本不保证顺序，需保序用 `MapSerial` / `ForEachSerial` |
| **重试用退避** | `Retry` 无间隔会打爆后端，生产用 `RetryWithBackoff` |
| **RateLimiter Close** | 内部有 ticker goroutine，用完必须 Close |
| **RateLimiter Wait 阻塞** | `Wait()` 令牌不足时阻塞，可能堆积大量 goroutine |
| **RateLimiter Burst 非缓存** | Burst 是初始突发量，用完后需等补充，不是长期可用 |
| **Group AutoScale** | 按需启用，Min/Max 根据下游承受能力调整 |
| **全局配置尽早调用** | `Set*` 函数建议在 `init()` 中调用，避免并发竞态 |
| **FailFast 丢弃成功结果** | 第一个错误后其他成功结果被丢弃，详见上文 |

---

> 更多细节请参阅各模块文档：[Pool 生产最佳实践](pool.md#生产最佳实践) | [Group 生产最佳实践](group.md#生产最佳实践) | [配置文档](config.md)
>
> 返回 [README](../README.md)