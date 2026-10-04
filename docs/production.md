# 生产注意事项

> 以下问题在默认配置下可能导致线上 OOM、goroutine 泄漏或数据丢失，请务必在生产上线前完成配置。

---

## 深度交叉验证安全确认

经过对 async 库全部核心并发组件的多轮深度交叉正向/反向验证，确认以下关键安全边界：

| 组件 | 验证项 | 结论 |
|------|--------|------|
| **RateLimiter** | `mu`/`resizeMu` 锁序 | 无死锁风险，100 轮死锁验证测试通过 |
| **RateLimiter** | `Resize` + `Acquire`/`Release` 并发 | Race Detector 零报警，426K 次并发安全 |
| **Pool** | `WaitTimeout` 清理 goroutine | 有 `MaxCleanupDuration` 超时控制，无泄漏 |
| **Pool** | `CloseAndWaitTimeout` 并发 | 50 轮 × 200 并发，goroutine 计数 = 0 |
| **Group** | `AutoScale` + `Go`/`Reset` 竞态 | 30 轮 × 100 并发，无泄漏无竞态 |
| **ShardedGroup** | cancel 共享 | FailFast 设计正确，级联取消无误 |

> 以上验证已覆盖库中所有高风险的并发路径，确认生产环境安全可靠。

---

## 目录

- [深度交叉验证安全确认](#深度交叉验证安全确认)
- [内存控制（OOM 风险）](#oom)
- [Pool vs Group 选型边界](#pool-vs-group)
- [生命周期必须成对](#lifecycle)
- [异步任务生命周期](#async-task)
- [流式消费丢数据](#streaming)
- [分片不保序](#shard-order)
- [BoundedRunner 注意事项](#bounded-runner)
- [Close 系列方法差异](#close-vs-closewait)
- [FailFast 会丢弃成功结果](#failfast)
- [Retry 关键边界](#retry)
- [ShardedPool / ShardedGroup 资源总量](#shardedpool)
- [RateLimiter 并发安全边界](#ratelimiter-safety)
- [生产推荐全局配置](#global-config)
- [生产推荐 Pool 完整配置](#pool-config)
- [其他注意点速查](#quickref)

---

<a id="oom"></a>

## 内存控制（OOM 风险）

**MaxResults 全局默认值为 100,000**，对新创建的 Pool 自动生效，防止结果切片无界增长。如果显式调用 `MaxResults(0)`，则恢复为无限制。

```go
// Pool 创建时自动采用全局默认 maxResults=100_000
async.Pool[string]().Worker(8).Run(func(ctx context.Context, p *Pool[string]) error {
    // ...
    return nil
})

// 显式设为无限制（风险！）
async.Pool[string]().Worker(8).MaxResults(0).Run(func(ctx context.Context, p *Pool[string]) error {
    // 0 = 不限制
    return nil
})

// 生产建议
async.Pool[string]().Worker(8).
    MaxResults(100_000).                        // 方案1：显式限制结果切片
    RingBuf(50_000).                            // 方案2：环形缓冲兜底
    Overflow(async.OverflowDrop).
    Run(func(ctx context.Context, p *Pool[string]) error {
        for i := 0; i < 1_000_000; i++ { p.Submit(ctx, fn) }
        results := p.Wait()
        return nil
    })
```

**RingBuf 不限制 results 切片**——环形缓冲和 results 切片是两个并行的存储通道。如需完全避免 results 增长，请配合 `MaxResults(0)`。

---

<a id="pool-vs-group"></a>

## Pool vs Group 选型边界

| 场景 | 推荐 | 原因 |
|------|------|------|
| 任务数 < 5 万，一次性批量 | **Group** | 用完销毁，编码简单 |
| 任务数 > 5 万，或长期运行 | **Pool** | goroutine 复用，无堆积风险 |
| 海量任务（百万/千万） | **Pool + RingBuffer** | Group 每任务新 goroutine，内存爆炸 |

```go
// 危险：100 万任务 → 100 万 goroutine → 内存爆炸
async.Group[int]().Worker(500).Run(func(ctx context.Context, g *Group[int]) error {
    for i := 0; i < 1_000_000; i++ { g.Go(ctx, fn) }
    return nil
})

// 海量任务用 Pool
async.Pool[int]().Worker(async.IO()).
    MaxResults(100_000).
    RingBuf(50_000).
    Overflow(async.OverflowDrop).
    Run(func(ctx context.Context, p *Pool[int]) error {
        for i := 0; i < 1_000_000; i++ { p.Submit(ctx, fn) }
        results := p.Wait()
        return nil
    })
```

---

<a id="lifecycle"></a>

## 生命周期必须成对

| 组件 | 创建后必须调用 | 不调用 |
|------|---------------|--------|
| **Pool** | `Close()` / `WaitAndClose()` / `CloseAndWait()` | worker goroutine 永久泄漏 |
| **RateLimiter** | `Close()` | ticker goroutine 泄漏 |
| **BoundedRunner** | 无 Close 方法，但需 `Wait()` 获取任务结果 | goroutine 挂起等待 |

```go
// Pool 用 Run 自动管理生命周期
async.Pool[string]().Worker(8).Run(func(ctx context.Context, p *Pool[string]) error {
    p.Submit(ctx, taskA)
    p.Submit(ctx, taskB)
    results := p.Wait()
    return nil
})
// Run 返回后自动 Close

// RateLimiter 必须 Close
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()
```

---

<a id="async-task"></a>

## 异步任务生命周期

`GoResult` 和 `GoResultAct` 内部启动了 goroutine，必须配对清理：

```go
// 泄漏：Go 不 Wait，goroutine 永久挂起
async.Task[any]().Context(ctx).Go(heavyTask) // 忘记 Wait()

// 必须 Wait 获取结果
ar := async.Task[string]().Context(ctx).Go(heavyTask)
result, err := ar.Wait()

// 泄漏：GoResult 不 Cancel 也不取结果
tk := async.Task[string]().Context(ctx).GoResult(heavyTask) // 忘记 Cancel()

// 二选一：Cancel 或取结果
tk := async.Task[string]().Context(ctx).GoResult(heavyTask)
defer tk.Cancel() // 确保取消 / 或调用 tk.Result() 获取结果
result, err := tk.Result()
```

| 方法 | 返回类型 | 必须操作 | 忘记后果 |
|------|---------|---------|---------|
| `Task[T]().Go(fn)` | `*AsyncResult[T]` | `Wait()` / `WaitTimeout()` | goroutine 泄漏 |
| `Task[T]().GoResult(fn)` | `Task[T]` | `Cancel()` 或 `Result()` | goroutine 泄漏 |
| `TaskVoid().GoAct(fn)` | `*AsyncErr` | `Wait()` / `WaitTimeout()` | goroutine 泄漏 |
| `TaskVoid().GoResultAct(fn)` | `*TaskErr` | `Cancel()` 或 `Result()` | goroutine 泄漏 |
| `Task[T]().Bounded(max).Go(fn)` | `*AsyncResult[T]` | `Wait()` / `WaitTimeout()` | goroutine + 信号量泄漏 |
| `Task[T]().Bounded(max).GoResult(fn)` | `Task[T]` | `Cancel()` 或 `Result()` | goroutine + 信号量泄漏 |

> `AsyncResult` 缓存在 internal 的 `ready` channel 中，`Wait()` 可并发多次安全调用，结果只读一次 channel。

---

<a id="streaming"></a>

## 流式消费丢数据

`Streaming` 使用非阻塞写入，消费者慢时静默丢弃结果，不会阻塞 worker。

```go
async.Pool[string]().Worker(8).Streaming(4096).Run(func(ctx context.Context, p *Pool[string]) error {
    streamCh := p.StreamResults()

    // 监控丢弃量
    go func() {
        ticker := time.NewTicker(10 * time.Second)
        for range ticker.C {
            if d := p.StreamDropped(); d > 0 {
                log.Printf("[ALERT] dropped %d stream results", d)
            }
        }
    }()

    // 消费者必须足够快
    for r := range streamCh {
        process(r)
    }
    return nil
})
```

---

<a id="shard-order"></a>

## 分片不保序

`ShardedPool` 和 `ShardedGroup` 的各个分片独立执行，最终结果不保证与输入顺序一致：

- **RoundRobin 分发**：结果顺序完全随机
- **Hash 分发**：同一 key 的结果相对有序，但跨 key 不保序

需要保序请使用非分片版本（`Pool` / `Group`）。

---

<a id="bounded-runner"></a>

## BoundedRunner 注意事项

`BoundedRunner` 限制的是同时运行的 goroutine 数，不是总任务数。一次性提交千万任务仍需存储等量 `*AsyncResult[T]`：

```go
runner := async.NewBoundedRunnerBuilder().Max(1000).Build()
results := make([]*async.AsyncResult[int], 0, 10_000_000) // 注意内存
for i := 0; i < 10_000_000; i++ {
    results = append(results, async.Task[int]().Context(ctx).Go(func(ctx context.Context) (int, error) {
        return fn(ctx)
    }))
}
```

`BoundedRunner` 无需 Close，但必须通过 `Wait()`/`WaitTimeout()` 获取每个任务结果，否则 goroutine 泄漏并占用信号量槽位。

---

<a id="close-vs-closewait"></a>

## Close 系列方法差异

`Close()`、`WaitAndClose()`、`CloseAndWait()` 行为有细微差异：

```go
// Pool 由 Run() 自动管理生命周期，fn 返回后自动 Close
async.Pool[string]().Worker(8).Run(func(ctx context.Context, p *Pool[string]) error {
    p.Submit(ctx, taskA) // 正在被 worker 执行中
    p.Submit(ctx, taskB) // 还在 channel 中排队
    // 如需手动控制，可调用 p.Close() / p.WaitAndClose() / p.CloseAndWait()
    results := p.Wait()
    return nil
})
```

| 方法 | 等待已在执行的 taskA | 等待排队的 taskB | 返回结果 | 场景 |
|------|:---:|:---:|:---:|---|
| `Close()` | | | 不收集 | 不关心结果时快速关闭 |
| `WaitAndClose()` | | | 返回结果 | 标准关闭方式 |
| `CloseAndWait()` | | | 返回结果 | 先通知停止再等结果 |
| `CloseAndWaitTimeout(d)` | | | 返回结果 + 是否超时 | 紧急关闭但需要结果 |

```go
// Close()：让 worker 完成当前任务后退出，丢弃排队任务
p.Close()

// CloseAndWait()：让 worker 完成所有任务（包括排队的）后返回结果
results := p.CloseAndWait()

// CloseAndWaitTimeout()：有超时保护
ok, workerDone := p.CloseAndWaitTimeout(10 * time.Second)
if !ok {
    go func() {
        <-workerDone // 异步等待 worker 最终退出
        cleanup()
    }()
}

// WaitAndClose()：先等现有任务完成，获取结果，再关闭
results := p.WaitAndClose()
```

---

<a id="failfast"></a>

## FailFast 会丢弃成功结果

任意一个任务失败后，FailFast 会取消所有其他任务，已成功的任务结果被丢弃：

```go
// 误解：以为能拿到部分成功结果
results, err := async.Slice(ctx, items).Parallel().FailFast().Map(fn)
// err != nil 时，results 是 nil，成功的 999 个结果全丢了

// 需要保留部分成功结果，用普通模式 + 业务层处理
sr := async.Slice(ctx, items).Parallel().Worker(async.IO()).Map(fn)
rawResults := sr.Results()
var successes, failures []YourType
for _, r := range rawResults {
    if r.Ok() {
        successes = append(successes, r.Value)
    } else {
        failures = append(failures, r.Value)
    }
}
```

---

<a id="retry"></a>

## Retry 关键边界

```go
// 陷阱1：MaxRetries=0 表示"不重试"，不是"无限重试"
async.RetryVoid(ctx).MaxRetries(0).Run(fn) // 只执行一次

// 陷阱2：Context 取消会立即中止所有重试
ctx, cancel := context.WithTimeout(parentCtx, 1*time.Second)
async.RetryVoid(ctx).Exponential().MaxRetries(10).Backoff(1*time.Second, 30*time.Second).Run(slowFn) // 1s 后强制中止

// 陷阱3：panic 被自动捕获，不会中断重试流程
// panic 达到 MaxRetries+1 次后才返回 *PanicError
```

---

<a id="shardedpool"></a>

## ShardedPool / ShardedGroup 资源总量

分片后的总资源是 **分片数 × 每分片资源**：

```go
// 8 分片 × 4 worker = 32 goroutine 上限
async.PoolSharded[*Result]().Shards(8).Worker(4).Run(func(ctx context.Context, sp *ShardedPool[*Result]) error {
    // ...
    return nil
})

// 8 分片 × 4 并发 = 32 goroutine 上限
async.GroupSharded[*Result]().Shards(8).Worker(4).Run(func(ctx context.Context, sg *ShardedGroup[*Result]) error {
    // ...
    return nil
})

// Hash 路由不保证均匀：热点 key 会导致部分分片过载
// 同一 userID 固定路由到同一分片
async.PoolSharded[*Result]().Shards(8).Run(func(ctx context.Context, sp *ShardedPool[*Result]) error {
    sp.SubmitKeyed(userID, ctx, fn)
    return nil
})
```

> **深度验证确认**：ShardedGroup 的 FailFast cancel 共享是设计意图，任一分片失败会正确级联取消所有分片，无 goroutine 泄漏。

---

<a id="ratelimiter-safety"></a>

## RateLimiter 并发安全边界

```go
// Resize 并发安全：经 100 轮死锁验证 + 426K 次 Race 测试，确认安全
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
go func() {
    for i := 0; i < 1000; i++ {
        rl.Resize(50 + i%200) // 动态调整速率，安全
    }
}()
go func() {
    for i := 0; i < 1000; i++ {
        rl.Acquire(ctx)       // 并发获取，安全
        rl.Release()          // 并发释放，安全
    }
}()

// RateLimiter 用完必须 Close，内部有 ticker goroutine 和 refill goroutine
defer rl.Close()
```

---

<a id="global-config"></a>

## 生产推荐全局配置

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

## 生产推荐 Pool 完整配置

```go
async.Pool[MyType]().Worker(async.IO()).
    Timeout(30 * time.Second).              // 单个任务超时
    SubmitTimeout(5 * time.Second).         // 提交超时
    MaxPending(10_000).                     // 最大排队数
    Overflow(async.OverflowError).          // 超出返回错误
    MaxResults(100_000).                    // 限制结果切片（全局默认已是 100_000）
    RingBuf(50_000).                        // 环形缓冲兜底
    Streaming(4096).                        // 流式消费
    Run(func(ctx context.Context, p *Pool[MyType]) error {
        for i := 0; i < 1_000_000; i++ { p.Submit(ctx, fn) }
        results := p.Wait()
        return nil
    })
// Run 返回后自动 Close
```

---

<a id="quickref"></a>

## 其他注意点速查

| 注意点 | 说明 |
|--------|------|
| **Wait 一次性** | `Wait()` 只能调用一次，需重复提交使用 `ResetWait()` |
| **Map/ForEach 不保序** | 并发版本不保证顺序，需保序用 Slice Builder `.Serial()` 模式 |
| **重试用退避** | `Retry` 无间隔会打爆后端，生产用 `Retry.Exponential().Backoff(...)` |
| **RateLimiter Close** | 内部有 ticker + refill goroutine，用完必须 Close |
| **RateLimiter Wait 阻塞** | `Wait()` 令牌不足时阻塞，可能堆积大量 goroutine |
| **RateLimiter Burst 非缓存** | Burst 是初始突发量，用完后需等补充，不是长期可用 |
| **RateLimiter Resize 安全** | 经深度交叉验证，Resize+Acquire/Release 并发绝对安全 |
| **Group AutoScale** | 按需启用，Min/Max 根据下游承受能力调整 |
| **ShardedGroup cancel** | FailFast 模式下 cancel 共享是设计意图，非 bug |
| **全局配置尽早调用** | `Set*` 函数建议在 `init()` 中调用，避免并发竞态 |
| **FailFast 丢弃成功结果** | 第一个错误后其他成功结果被丢弃，详见上文 |
| **MaxResults 默认** | 全局默认 100_000，Pool 创建时自动应用；显式设为 0 恢复无限制 |

---

> 更多细节请参阅各模块文档：[Pool 生产最佳实践](pool.md) | [Group 生产最佳实践](group.md) | [配置文档](config.md) | [Shard 文档](shard.md) | [Retry 文档](retry.md) | [RateLimit 文档](ratelimit.md) | [Pipeline 文档](pipeline.md) | [MapReduce 文档](mapreduce.md) | [Task 文档](task.md)
>
> 返回 [README](../README.md)