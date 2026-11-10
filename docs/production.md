# 生产注意事项

> 本文列出需要结合业务负载评估的资源与生命周期边界。配置和测试不能替代对任务退出、队列容量及下游承载能力的检查。

---

## 深度交叉验证安全确认

以下是并发场景的测试记录和使用边界；测试结果不能覆盖所有调度和生产负载：

| 组件 | 验证项 | 结论 |
|------|--------|------|
| **RateLimiter** | `mu`/`resizeMu` 锁序 | 相关并发测试通过 |
| **RateLimiter** | `Resize` + `Acquire`/`Release` 并发 | 对应 Race Detector 测试通过；结论限于该测试场景 |
| **Pool** | `WaitTimeout` 清理 goroutine | 超时后清理可继续；任务应响应取消并最终退出 |
| **Pool** | `CloseAndWaitTimeout` 并发 | 对应并发关闭测试通过；首次超时通道仅表示 worker 退出，需用 Close 等待完整清理 |
| **Group** | `AutoScale` + `Go`/`Reset` 竞态 | 对应竞态检测用例通过；结论限于实际测试覆盖范围 |
| **Group** | `WaitContext`/`WaitTimeout` 取消传播 | 等待取消时请求取消任务，后台继续等待任务完成 |
| **ShardedGroup** | cancel 共享 | FailFast 下跨分片取消由共享上下文传播；相关测试通过 |

> 表中数据仅描述对应测试覆盖的场景和观测结果，不代表覆盖所有调度时序或生产负载，也不能据此保证生产环境不存在并发问题。

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
- [FailFast 与部分结果](#failfast)
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

本文的提交、选型、生命周期和关闭示例放在返回 `error` 的函数内，`ctx` 是非 nil 的父 context；导入 `context`、`errors`、`time` 和根包 `async`。`fn`、`taskA`、`taskB` 按对应结果类型使用 `func(context.Context) (T, error)`，`handleResults([]async.Result[T]) error` 必须检查全部实际保留结果并返回合并错误。这些示例将提交错误与结果处理错误同时返回，可用 `errors.Is` / `errors.As` 查询；`Run` 只传播作用域回调返回的错误，调用方也须处理它。

生产中先给父请求设总预算，回调须响应收到的 context；发生提交失败仍等待已接受任务收尾，不会回滚已发生的副作用。有限主存储和环形缓冲可能丢失上限外结果，`handleResults` 无法恢复其错误；需要独立记录全部终态时采用后文完整 Pool 配置的完成回调，并评估回调与持久化成本。这里的百万次是提交目标，失败时提前结束提交。

```go
// Pool 创建时自动采用全局默认 maxResults=100_000
if err := async.Pool[string]().Context(ctx).Worker(8).Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
    // ...
    return nil
}); err != nil { return err }

// 显式设为无限制（风险！）
if err := async.Pool[string]().Context(ctx).Worker(8).MaxResults(0).Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
    // 0 = 不限制
    return nil
}); err != nil { return err }

// 生产建议
if err := async.Pool[string]().Context(ctx).Worker(8).
    MaxResults(100_000).                        // 方案1：显式限制结果切片
    RingBuf(50_000).                            // 方案2：环形缓冲兜底
    Overflow(async.OverflowDrop).
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
        var submitErr error
        for i := 0; i < 1_000_000; i++ {
            if err := p.Submit(ctx, fn); err != nil { submitErr = err; break }
        }
        results := p.Wait() // 先收尾已接受任务
        return errors.Join(submitErr, handleResults(results)) // 只包含实际保留的结果
    }); err != nil { return err }
```

**RingBuf 不限制 results 切片**。设置正数 `MaxResults(n)` 后，环形缓冲仅写入上限外结果；`MaxResults(0)` 表示主存储无限制，并把全部结果另行镜像写入环形缓冲，不能用于限制内存，也不能将 Flush 镜像与 Wait 主结果重复计数。环形缓冲容量有限，满时仍可能丢失结果，应及时消费并按实际负载设置容量。

---

<a id="pool-vs-group"></a>

## Pool vs Group 选型边界

| 场景 | 推荐 | 原因 |
|------|------|------|
| 一次性批量，全部结果可驻留内存 | **Group** | Wait 收尾，适合集中检查结果 |
| 长期运行、反复提交 | **Pool** | worker 可复用，并可限制结果存储 |
| 海量任务（百万/千万） | **Pool + MaxResults + RingBuffer** | 限制主结果存储，并及时消费溢出结果 |

任务体积、排队数量、结果大小和下游吞吐量都需要通过负载测试评估，不按固定任务数决定执行器。

```go
// Group 使用固定 worker，但大量结果和任务元数据仍会占用内存。
if err := async.Group[int]().Context(ctx).Worker(500).Run(func(ctx context.Context, g *async.GroupInstance[int]) error {
    var submitErr error
    for i := 0; i < 1_000_000; i++ {
        if err := g.Go(ctx, fn); err != nil { submitErr = err; break }
    }
    results := g.Wait() // 提交失败也收尾并检查已接受任务
    return errors.Join(submitErr, handleResults(results))
}); err != nil { return err }

// 海量任务用 Pool
if err := async.Pool[int]().Context(ctx).Worker(async.IO()).
    MaxResults(100_000).
    RingBuf(50_000).
    Overflow(async.OverflowDrop).
    Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
        var submitErr error
        for i := 0; i < 1_000_000; i++ {
            if err := p.Submit(ctx, fn); err != nil { submitErr = err; break }
        }
        results := p.Wait()
        return errors.Join(submitErr, handleResults(results)) // 上限之外未保留的业务结果需另外收集
    }); err != nil { return err }
```

---

<a id="lifecycle"></a>

## 生命周期必须成对

| 组件 | 创建后必须调用 | 不调用 |
|------|---------------|--------|
| **Pool** | 手动创建的 Pool 调用 `Close()` 等关闭方法；或使用 `Run` 自动管理 | 手动创建且未关闭时，自有 worker 会持续运行 |
| **RateLimiter** | `Close()` | ticker goroutine 泄漏 |
| **BoundedRunner** | 无 Close 方法；根据需要等待任务结果 | 回调持续阻塞会占用并发槽位 |

```go
// Pool 用 Run 自动管理生命周期
if err := async.Pool[string]().Context(ctx).Worker(8).Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
    var submitErr error
    for _, task := range []func(context.Context) (string, error){taskA, taskB} {
        if err := p.Submit(ctx, task); err != nil { submitErr = err; break }
    }
    results := p.Wait()
    return errors.Join(submitErr, handleResults(results))
}); err != nil { return err }
// Run 返回后自动 Close

// RateLimiter 必须 Close
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()
```

---

<a id="async-task"></a>

## 异步任务生命周期

这些 API 会启动后台 goroutine，但结果 channel 带有一个缓冲槽位。任务函数返回（或 panic 被捕获）后，结果可以写入 channel，后台 goroutine 随即退出；调用方不调用 `Wait()`、`WaitTimeout()`、`WaitContext()` 或 `Result()`，不会单独造成 goroutine 泄漏。等待方法的作用是取得结果、错误并确认完成。

如果任务可能长期运行，应让函数响应传入的 context。对于 `GoResult` / `GoResultAct` 返回的句柄，如果调用方不需要结果，建议保留句柄并在不再需要任务时调用 `Cancel()`，以尽早请求取消任务和解除子 context 关联；这属于生命周期管理，不是为了让已完成的 goroutine 退出。

```go
// 只需要异步执行：任务完成后 goroutine 自动退出
async.Task[string]().Context(ctx).Go(func(ctx context.Context) (string, error) {
    return heavyTask(ctx)
})

// 需要结果时再等待
ar := async.Task[string]().Context(ctx).Go(func(ctx context.Context) (string, error) {
    return heavyTask(ctx)
})
result, err := ar.Wait()
_, _ = result, err

// 可取消任务：不需要结果时，在生命周期结束时取消
tk := async.Task[string]().Context(ctx).GoResult(func(ctx context.Context) (string, error) {
    return heavyTask(ctx)
}) // 忽略结果也不会让已完成任务的 goroutine 挂起
defer tk.Cancel()

// 需要结果时读取；读取本身也会等待任务完成
tk2 := async.Task[string]().Context(ctx).GoResult(func(ctx context.Context) (string, error) {
    return heavyTask(ctx)
})
result2, err2 := tk2.Result()
_, _ = result2, err2
defer tk2.Cancel()
```

| 方法 | 返回类型 | 结果不需要时 | 需要结果时 |
|------|---------|---------|---------|
| `Task[T]().Go(fn)` | `*async.AsyncResult[T]` | 可直接忽略返回值 | 通过 `Wait()` / `WaitTimeout()` / `WaitContext()` 读取结果 |
| `Task[T]().GoResult(fn)` | `async.TaskHandle[T]` | 任务不再需要时调用 `Cancel()` | 通过 `Result()` 读取结果；必要时再调用 `Cancel()` |
| `TaskVoid().GoAct(fn)` | `*async.AsyncErr` | 可直接忽略返回值 | 通过 `Wait()` / `WaitTimeout()` / `WaitContext()` 读取错误 |
| `TaskVoid().GoResultAct(fn)` | `async.TaskErr` | 任务不再需要时调用 `Cancel()` | 通过 `Result()` 读取错误；必要时再调用 `Cancel()` |
| `Task[T]().Bounded(max).Go(fn)` | `*async.AsyncResult[T]` | 可直接忽略返回值；任务返回时自动释放信号量槽位 | 通过等待方法读取结果 |
| `Task[T]().Bounded(max).GoResult(fn)` | `async.TaskHandle[T]` | 任务不再需要时调用 `Cancel()`；任务返回时自动释放信号量槽位 | 通过 `Result()` 读取结果 |

> `AsyncResult` 内部缓存结果，`Wait()`、`WaitTimeout()` 和 `WaitContext()` 可并发调用；结果只从底层 channel 读取一次。无论是否读取结果，函数返回后都不会因为 channel 没有消费者而卡住。

---

<a id="streaming"></a>

## 流式消费丢数据

Pool 的 `Streaming` 使用非阻塞写入，流通道满时丢弃该流结果，不会阻塞 worker；可通过 `StreamDropped()` 检查丢弃数量。Group 的流式实现另有溢出缓冲，其行为见 [Group 文档](group.md)。

Slice/Map 构建器的 `Stream` 使用独立完成回调通道，满时形成背压，与池自身 `StreamResults` 的丢弃规则不同。提前停止消费时取消传给构建器的父 context，再等待输出关闭；FailFast 或单次 Timeout 不代替此取消，已开始回调仍须返回才能完成清理。借用池自动复位会替换原生流并清零 `StreamDropped`，复位后的零值不能证明旧批次没有丢弃；关联说明见 [Map/Reduce Stream](mapreduce.md#终端操作stream流式)。

下例放在返回 `error` 的业务函数中；`ctx` 是父请求 context，`tasks` 的类型为 `[]func(context.Context) (string, error)`，`process(context.Context, string) error` 处理成功值。导入 `context`、`errors`、`fmt` 和根包 `async`。仅消费者写入错误切片，主 goroutine 在完成通道关闭后读取；不要改为在提交期间并发读取它。生产中消费函数须及时返回并响应 context；本例继续消费成功项，错误列表随收到的失败数量增长，大流量应选择有界错误收集或持久化。

注意：本例将提交、已收到任务及消费错误返回，并把结果丢弃数大于零作为错误；它无法恢复未收到结果的值或其业务错误。不能用流里没有错误证明全部任务成功；需要完整终态时应另行可靠记录。即使父 context 已取消，也要等待已接受业务和消费者结束；不响应取消的处理仍会延长返回时间，已发生的消费副作用不自动回滚。

```go
err := async.Pool[string]().Context(ctx).Worker(8).Streaming(4096).Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
    streamCh := p.StreamResults()
    consumed := make(chan struct{})
    var consumeErrors []error // 仅消费者写，主 goroutine 在 consumed 关闭后读取
    go func() {
        defer close(consumed)
        for r := range streamCh {
            if r.Err != nil {
                consumeErrors = append(consumeErrors, r.Err)
                continue
            }
            if err := process(ctx, r.Value); err != nil {
                consumeErrors = append(consumeErrors, err)
            }
        }
    }()
    var submitErr error
    for _, task := range tasks {
        if err := p.Submit(ctx, task); err != nil {
            submitErr = err
            break
        }
    }
    p.Wait() // 等待任务并关闭流，消费者随后退出
    <-consumed
    failures := append(consumeErrors, submitErr, ctx.Err())
    if dropped := p.StreamDropped(); dropped > 0 {
        failures = append(failures, fmt.Errorf("pool stream dropped %d results", dropped))
    }
    return errors.Join(failures...)
})
if err != nil { return err }
```

---

<a id="shard-order"></a>

## 分片不保序

`ShardedPool` 和 `ShardedGroup` 的各个分片独立执行，最终结果不保证与输入顺序一致：

- **RoundRobin 分发**：结果按分片合并，不保证全局提交顺序。
- **Hash 分发**：相同 key 路由到同一分片，但分片内多个任务仍可能并发完成，不能保证结果顺序。

需要还原全局提交顺序时，可在结果中携带序号并在收集后排序。非分片 Pool 的主结果按提交位置排列，但环形缓冲溢出结果按写入顺序追加且可能丢失。Group 的普通 `Go` 结果按结果写入时机收集，不能依赖提交顺序；需要固定位置时可使用不同的 `GoAt` 索引。

---

<a id="bounded-runner"></a>

## BoundedRunner 注意事项

`BoundedRunner` 限制同时持有许可的业务回调数，不限制总任务数、调用方创建的等待提交 goroutine、业务子 goroutine 或结果投递收尾。只有在调用方保留所有结果句柄时，才会额外存储等量句柄；直接丢弃句柄可避免这部分内存，但也无法集中等待或检查每个任务的错误：

下例位于返回 `error` 的函数中，`ctx` 非 nil，`items` 为 `[]int`；需要导入 `context` 和 async。`processItem(context.Context, int) (int, error)` 返回处理值或失败，`handle(int)` 按输入顺序消费成功值。`Bounded(1000)` 设置共享业务许可数，`batchSize=256` 设置每批保留句柄数；该调用方先等完一批，因此实际在途任务不超过256。生产中分别按下游容量和结果内存预算设置这两个参数。

```go
builder := async.Task[int]().Context(ctx).Bounded(1000)
const batchSize = 256 // 句柄保留上限和执行并发度分别配置
for start := 0; start < len(items); start += batchSize {
    end := min(start+batchSize, len(items))
    results := make([]*async.AsyncResult[int], 0, end-start)
    for _, item := range items[start:end] {
        results = append(results, builder.Go(func(ctx context.Context) (int, error) {
            return processItem(ctx, item)
        }))
    }
    var firstErr error
    for _, result := range results {
        value, err := result.Wait()
        if err != nil {
            if firstErr == nil { firstErr = err }
            continue
        }
        handle(value)
    }
    if firstErr != nil { return firstErr }
}
```

`BoundedRunner` 无需 Close；回调返回时自动释放槽位，无需等待结果来释放槽位。使用 `Wait()` 或 `WaitTimeout()` 获取结果并处理错误；持续阻塞且不响应上下文的回调会一直占用槽位。

本例等待本批全部已提交句柄，按输入顺序处理成功值，再返回本批输入顺序中的第一个错误并停止提交下一批；它不汇总其他失败，也不会回滚已处理成功值的副作用。业务 panic 和 Goexit 由 Task 入口分别记录为 `PanicError` 与 `ErrTaskGoexit`。若需要全部错误明细，应在遍历结果时另外收集；`handle` 须及时返回，其 panic 不由这段调用方循环捕获。

达到并发上限时，`builder.Go` 会在调用方等待槽位或 context 取消，因此提交过程本身可能阻塞。若等待期间因 context 取消而未取得槽位，回调不会启动，返回的结果句柄包含取消错误；忽略句柄也会丢失这个错误。取消与槽位同时就绪时，不能依赖一定选择取消，应让回调同样检查 context。

---

<a id="close-vs-closewait"></a>

## Close 系列方法差异

`Close()`、`WaitAndClose()`、`CloseAndWait()` 的顺序和返回值不同：

```go
// Pool 由 Run() 自动管理生命周期，fn 返回后自动 Close
if err := async.Pool[string]().Context(ctx).Worker(8).Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
    var submitErr error
    for _, task := range []func(context.Context) (string, error){taskA, taskB} {
        if err := p.Submit(ctx, task); err != nil { submitErr = err; break }
    }
    // 如需手动控制，可调用 p.Close() / p.WaitAndClose() / p.CloseAndWait()
    results := p.Wait()
    return errors.Join(submitErr, handleResults(results))
}); err != nil { return err }
```

| 方法 | 等待已在执行的 taskA | 等待排队的 taskB | 返回结果 | 场景 |
|------|:---:|:---:|:---:|---|
| `Close()` | 等待 worker 退出及已登记任务收尾 | 队列中任务可能执行，也可能以 `ErrPoolClosed` 记录为丢弃 | 无 | 不再提交任务并关闭池 |
| `WaitAndClose()` | 先等待任务完成，再关闭 | 等待并返回本批结果 | `[]Result[T]` | 等待、收集结果并关闭 |
| `CloseAndWait()` | 关闭后等待任务清理 | 关闭时未能执行的任务会被记录为 `ErrPoolClosed` | 无 | 关闭并等待清理结束 |
| `CloseAndWaitTimeout(d)` | 限时等待关闭；首次调用超时后，返回通道在 worker 退出时关闭，但完整清理可能仍在进行 | 关闭时未能执行的任务会被记录为 `ErrPoolClosed` | `(bool, <-chan struct{})`；关闭清理在本次等待期限内完成时返回 `(true, nil)`，进行中的关闭仍可能超时 | 限制本次关闭等待，超时后清理继续 |

`Close` 本身不请求取消业务 context。关闭唤醒的阻塞提交，还可能在提交调用者中等待全局结果回调和逐项 completion 返回；首次 worker 通道关闭不保证这些通知结束、失败统计更新或结果流关闭。通知须及时返回且不能同步关闭所属池。完整 `Close` 会等待这些清理，耗时不受前一次限时等待约束，见 [Pool 关闭与复用](pool.md#close--closeandwait--closeandwaittimeout)。

下面四段是四种独立用法，每次选择一种并使用尚未关闭的池，不应把它们依次当作同一个池的操作步骤。`p` 为应用持有的 `*async.PoolInstance[T]`；除 `CloseAndWaitTimeout(d)` 接受等待时长外，其余方法无参数。限时示例的 `cleanup` 签名为 `func()`，由应用负责完整关闭后的外部资源清理；需要导入 `time`。

```go
// Close()：关闭提交入口，worker 退出；排队任务可能执行，未能执行的任务记录 ErrPoolClosed
p.Close()

// CloseAndWait()：关闭后等待清理完成，不返回结果
p.CloseAndWait()

// CloseAndWaitTimeout()：有超时保护；workerDone 关闭表示 worker 已退出，完整清理可能仍在进行
ok, workerDone := p.CloseAndWaitTimeout(10 * time.Second)
if !ok {
    go func() {
        <-workerDone // 首次关闭超时时这里只保证 worker 已退出
        p.Close()    // 等待完整关闭和队列清理
        cleanup()
    }()
}

// WaitAndClose()：先等任务完成并获取结果，再关闭
results := p.WaitAndClose()
_ = results
```

生产中需要结果时选择 `WaitAndClose()` 并逐项处理返回错误。`CloseAndWait()` 内部已经消耗本批唯一一次 `Wait()`，既不返回结果，再调用 `Wait()` 也不能重新取得本批结果；如选它且仍要记录终态，应提前配置并发安全的完成通知。

注意：上面的10秒只限制本次关闭等待，不强制结束业务，也不覆盖取得内部调整锁的等待。限时成功时本例不会调用 `cleanup`；超时时清理在另一个 goroutine 中继续，它会再次同步 `Close()`，所以仍可能长期等待不返回的业务或通知。`cleanup` 必须及时返回，不应再使用已关闭池提交任务；应用退出前若需要确认外部清理完成，应另外保存完成信号并等待。清理 goroutine 中的 panic 不由这段示例捕获。

---

<a id="failfast"></a>

## FailFast 与部分结果

切片 Map 的 FailFast 会请求取消其余工作，逐项结果可保留已完成的成功值；尚未执行或响应取消的任务可能返回错误，未填充的结果还需检查 `Occupied`。已开始的回调须自行响应上下文取消。`Values()` 只按 `Err == nil` 过滤，不检查 `Occupied`，不能单独用来确认每项已成功执行。无池 Map 容器的 Map/ForEach/批量路径则只停止后续调度，不取消已开始的回调 context；不能把切片的取消行为套用于所有入口。

```go
sr := async.Slice(ctx, items).Worker(async.IO()).FailFast().Map(fn)
successes := sr.Values()
failures := sr.Errors()
// sr.Results() 保留逐项结果；检查 Occupied 和 Err。sr.Error() 返回错误。
_ = successes
_ = failures
```

---

<a id="retry"></a>

## Retry 关键边界

下面两次调用是独立使用场景，分别放在返回 `error` 的函数中；需要导入 `context`、`errors`、`time` 和 async。第一个场景的 `ctx`、第二个场景的 `parentCtx` 都非 nil；`fn` 签名为 `func() error`，`slowFn` 为 `func(context.Context) error`。`Run(fn)` 的回调无 context 参数，须自行安排其业务退出；需要向业务传递每次调用的 context 时使用 `RunVoid`。

```go
// 陷阱1：MaxRetries=0 表示"不重试"，不是"无限重试"
if err := async.RetryVoid(ctx).MaxRetries(0).Run(fn); err != nil { return err } // 父context有效时至多执行一次

// 陷阱2：Context 取消会阻止后续重试；正在运行的回调必须自行响应取消
ctx, cancel := context.WithTimeout(parentCtx, 1*time.Second)
defer cancel()
retryErr := async.RetryVoid(ctx).Exponential().MaxRetries(10).Backoff(1*time.Second, 30*time.Second).RunVoid(slowFn)
if err := errors.Join(retryErr, ctx.Err()); err != nil { return err }

// 陷阱3：panic 被自动捕获，不会中断重试流程
// 重试耗尽时返回包装错误，可使用 errors.As 查找其中的 *async.PanicError
```

生产中仅对可安全重复执行或有幂等键的操作重试。`MaxRetries(10)` 给出至多11次尝试的次数预算，退避从1秒指数增长到30秒上限；父请求的一秒预算包含业务与退避，因此通常先终止重试。构建器不会强制中断业务，也不会把业务成功自动替换为超时错误；第二个场景显式合并父错误，使晚于预算返回的成功也能报告截止时间。

注意：取消和计时器同时就绪时存在竞争，不能依赖固定尝试次数或绝对不开始下一次调用；`slowFn` 必须检查收到的 context，且已发生的副作用不会回滚。业务错误或 panic 包装错误通常在重试耗尽时保留在错误链；在退避中被父取消中断时，返回值可能只包含父错误，若需每次失败明细应由应用另行记录。最终错误用 `errors.Is` / `errors.As` 判断，不匹配固定文本。

---

<a id="shardedpool"></a>

## ShardedPool / ShardedGroup 资源总量

分片后的总资源是 **分片数 × 每分片资源**：

```go
// 8 分片 × 4 worker = 32 个 worker；还可能有其他辅助 goroutine
if err := async.PoolSharded[*Result]().Context(ctx).Shards(8).Worker(4).Run(func(ctx context.Context, sp *async.ShardedPool[*Result]) error {
    // ...
    return nil
}); err != nil { return err }

// 8 分片 × 4 worker = 32 个 worker；还可能有其他辅助 goroutine
if err := async.GroupSharded[*Result]().Context(ctx).Shards(8).Worker(4).Run(func(ctx context.Context, sg *async.ShardedGroup[*Result]) error {
    // ...
    return nil
}); err != nil { return err }

// Hash 路由不保证均匀：热点 key 会导致部分分片过载
// 同一 userID 固定路由到同一分片
if err := async.PoolSharded[*Result]().Context(ctx).Shards(8).Run(func(ctx context.Context, sp *async.ShardedPool[*Result]) error {
    submitErr := sp.SubmitKeyed(userID, ctx, fn)
    results := sp.Wait()
    return errors.Join(submitErr, handleResults(results))
}); err != nil { return err }
```

> ShardedGroup 的 FailFast 使用共享取消上下文；任一分片失败会请求取消其他分片。任务函数仍需响应 context，测试通过不代表不响应取消的任务也能退出。

---

<a id="ratelimiter-safety"></a>

## RateLimiter 并发安全边界

示例位于返回 `error` 的函数中；`ctx` 为非 nil 的父请求 context，`doRequest` 签名为 `func(context.Context) error`，必须响应收到的 context 并返回业务失败。需要导入 `context`、`errors`、`sync`、`time` 和 async。`Rate(100)` 表示每秒补充100个令牌且初始容量为100；`Resize(50 + i%200)` 演示50到249的动态速率，两个循环各执行至多1000次。

```go
// Resize 与 Acquire/Release 有并发测试覆盖；生产中仍应按负载监控等待和延迟
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()
var wg sync.WaitGroup
var requestErr error // 仅请求 goroutine 写，Wait 返回后调用方读
wg.Add(2)
go func() {
    defer wg.Done()
    for i := 0; i < 1000; i++ {
        rl.Resize(50 + i%200) // 动态调整速率
    }
}()
go func() {
    defer wg.Done()
    for i := 0; i < 1000; i++ {
        if err := ctx.Err(); err != nil {
            requestErr = err
            return
        }
        if err := rl.Acquire(ctx); err != nil {
            requestErr = err
            return
        }
        // 消费令牌；不要立即归还，否则可能超过期望的请求速率
        if err := doRequest(ctx); err != nil {
            requestErr = err
            return
        }
    }
}()

wg.Wait() // 两个 goroutine 结束后再关闭限流器
if err := errors.Join(requestErr, ctx.Err()); err != nil { return err }
```

生产中复用一个限流实例，按监控反馈低频调整速率，并为父 context 设置覆盖整个请求循环的总预算。示例的1000次紧密 Resize 用于展示并发调用边界，不是推荐的调参频率；Resize 也不负责重试或撤销已经发生的业务副作用。错误通过 `errors.Is` 查询；仅有一个请求 goroutine，因此无需为 `requestErr` 加锁，扩展为多个请求 goroutine 时须同步记录错误。业务 panic 与 Goexit 不会被这段直接启动的 goroutine 转换为普通错误，应由应用按业务约定处理。

注意：`Acquire` 有可用令牌时可优先成功，即使 ctx 已取消；示例每轮显式检查取消，并在收尾合并父错误，但取消与业务调用间仍存在竞争，业务函数须自行响应 context。Build 绑定 context 用于实例配置，不会替调用方自动 Close；这里先等待两个 goroutine 退出，再由 defer 关闭。

`Release()` 会将令牌归还限流器，适合明确需要归还令牌的场景；若要限制单位时间内的请求次数，通常应消费令牌并等待定时补充。

---

<a id="global-config"></a>

## 生产推荐全局配置

下面是包级初始化函数，需要导入 `time` 和 async；`myProductionLogger` 是应用提供的、实现 `async.Logger` 的日志适配器。五个 Set 方法均无返回值。应在启动业务 goroutine 和创建执行器前完成配置；这些值影响整个进程，不能用来隔离不同请求。

| 调用参数 | 本例含义与生效边界 |
|---|---|
| `SetDefaultTimeout(30 * time.Second)` | 后续创建的 Pool/Group 保存30秒单任务默认期限；不重新配置已有实例，不提供整个请求的总预算。实例 `WithTimeout` 可再覆盖，业务仍须响应 context。 |
| `SetSubmitTimeout(5 * time.Second)` | 仅保存5秒全局配置值，可用 `GetSubmitTimeout()` 读取；Pool/Group 不自动应用，须显式设置其 `SubmitTimeout` / `WithSubmitTimeout`。 |
| `SetTaskFailLogLevel(LogLevelWarn)` | 把内部任务失败日志路由到 Warn，不按频率采样，也不关闭日志；实际存储与告警由日志后端决定。 |
| `SetTraceLogEnabled(false)` | 停止生成 TraceID 时的日志器自动绑定；仍生成、保存 TraceID，显式带 context 的日志仍可输出并附加该字段。 |
| `SetLogger(myProductionLogger)` | 设置全局日志后端；未类型化的 `nil` 恢复静默实现，类型化 nil 指针仍是已注入对象，不能用它表示关闭日志。 |

```go
func init() {
    async.SetDefaultTimeout(30 * time.Second)        // 默认超时
    async.SetSubmitTimeout(5 * time.Second)           // 记录全局提交超时配置值；需另在 Pool/Group Builder 上设置 SubmitTimeout
    async.SetTaskFailLogLevel(async.LogLevelWarn)     // 任务失败日志使用 Warn；不减少记录次数
    async.SetTraceLogEnabled(false)                   // 关闭 TraceID 生成时的日志器自动绑定；显式日志仍可携带 TraceID
    async.SetLogger(myProductionLogger)               // 注入自定义 Logger
}
```

生产中按下游响应时间设置任务期限，为父请求另设总预算；提交期限还应在每个执行器上显式配置。日志适配器须支持并发调用、及时返回，并负责字段脱敏和输出失败处理；本例将失败日志改为 Warn，不保证日志数量减少。配置 setter 的并发安全不等于注入对象或多项配置的原子切换。

注意：30秒期限只是协作取消信号，不强制中断回调；更早的父期限优先，副作用不回滚。已创建实例继续使用其保存的任务超时；日志级别和全局后端则会影响后续日志调用。需要测试该初始化函数时，应串行保存和恢复全局设置，避免测试之间互相影响。

---

<a id="pool-config"></a>

## 生产推荐 Pool 完整配置

示例位于返回 `error` 的函数中；`ctx` 为非 nil 的父请求 context，`fn` 的签名为 `func(context.Context) (MyType, error)`，`handleResults` 的签名为 `func([]async.Result[MyType]) error`，`MyType` 为应用结果类型。需要导入 `context`、`errors`、`sync`、`time` 和 async。`fn` 应响应收到的任务 context，`handleResults` 负责处理实际保留的结果。

```go
// 完成所有已接受任务，并用终态回调独立记录上限外错误。
var outcomeMu sync.Mutex
var outcomeErr error
err := async.Pool[MyType]().Context(ctx).Worker(async.IO()).
    Timeout(30 * time.Second).              // 单个任务超时
    SubmitTimeout(5 * time.Second).         // 实例提交超时
    MaxPending(10_000).                     // 最大排队数
    Overflow(async.OverflowError).          // 超出返回错误
    MaxResults(100_000).                    // 限制结果切片（全局默认已是 100_000）
    RingBuf(50_000).                        // 环形缓冲兜底
    Run(func(ctx context.Context, p *async.PoolInstance[MyType]) error {
        p.WithResultCallback(func(result async.Result[MyType]) {
            if result.Err != nil {
                outcomeMu.Lock()
                if outcomeErr == nil { outcomeErr = result.Err }
                outcomeMu.Unlock()
            }
        }) // Pool 的结果回调由实例配置，在提交前完成
        var submitErr error
        for i := 0; i < 1_000_000; i++ {
            if err := p.Submit(ctx, fn); err != nil {
                submitErr = err
                break
            }
        }
        results := p.Wait() // 即使提交失败，也先收尾已接受任务
        outcomeMu.Lock()
        taskErr := outcomeErr
        outcomeMu.Unlock()
        if combinedErr := errors.Join(submitErr, taskErr); combinedErr != nil {
            return combinedErr
        }
        return handleResults(results) // 这里只处理被保留的结果
    })
if err != nil { return err }
// Run 返回后自动 Close
```

该示例遇到提交错误先停止继续提交，再等待已接受任务收尾；业务函数须响应 context，才能及时退出。`Run` 只传播作用域回调返回的错误，因此使用 `errors.Join` 同时保留提交错误和终态回调首个观察到的错误，可用 `errors.Is` / `errors.As` 查询。终态回调可能并发执行，也可能报告提交拒绝；示例中的互斥锁只保护首个观察到的错误，它不是输入顺序中的首错，也不汇总全部业务错误。出现这些错误时不会调用 `handleResults`；没有这些错误时，返回该处理函数的错误。

`OverflowError` 与有限环形缓冲不能保证保留全部结果；`handleResults` 接收到的仅为实际保留部分。错误回调不补回被丢弃的成功值，需要全部结果时应另外设计可靠的持久化路径，并评估回调耗时对 worker 的影响。

生产中按下游连接与内存预算调整 worker、排队上限和结果容量；百万次为提交目标，不保证全部被接受。30 秒任务期限只提供取消信号，5 秒提交期限也不能强制结束已开始业务；父期限仍决定整个请求预算。通知回调应及时返回，不能在其中同步等待或关闭所属池。`Run` 返回后池已关闭，不要继续使用其提交入口；取消不会撤销已发生的业务副作用。

---

<a id="quickref"></a>

## 其他注意点速查

| 注意点 | 说明 |
|--------|------|
| **Pool 的批次等待** | Pool 完成一次 `Wait()` 后，如需提交新批次，先调用 `ResetWait()`；其他组件的等待规则见各自文档 |
| **Map/ForEach 顺序** | 并发回调的执行顺序不确定；Slice Map 的逐项结果按输入位置对应，不能据此推断回调副作用顺序 |
| **重试用退避** | `Retry` 无间隔会打爆后端，生产用 `Retry.Exponential().Backoff(...)` |
| **RateLimiter Close** | 内部有 ticker + refill goroutine，用完必须 Close |
| **RateLimiter Wait 阻塞** | `Wait()` 令牌不足时阻塞，可能堆积大量 goroutine |
| **RateLimiter Burst 非缓存** | Burst 是初始突发量，用完后需等补充，不是长期可用 |
| **RateLimiter Resize 安全** | 支持与 Acquire/Release 并发调用，相关场景有竞态测试覆盖 |
| **Group AutoScale** | 按需启用，Min/Max 根据下游承受能力调整 |
| **Group WaitContext/WaitTimeout** | 超时或取消后返回结果快照，后台继续等待任务退出 |
| **ShardedGroup cancel** | FailFast 模式下 cancel 共享是设计意图，非 bug |
| **全局配置尽早调用** | `Set*` 函数建议在 `init()` 中调用，避免并发竞态 |
| **FailFast 与部分结果** | 根据入口停止后续调度或请求取消；检查逐项结果及可能的未占用槽位，区分成功、失败和取消 |
| **MaxResults 默认** | 全局默认 100_000，Pool 创建时自动应用；显式设为 0 恢复无限制 |

---

> 更多细节请参阅各模块文档：[Pool 生产最佳实践](pool.md) | [Group 生产最佳实践](group.md) | [配置文档](config.md) | [Shard 文档](shard.md) | [Retry 文档](retry.md) | [RateLimit 文档](ratelimit.md) | [Pipeline 文档](pipeline.md) | [MapReduce 文档](mapreduce.md) | [Task 文档](task.md)
>
> 返回 [README](../README.md)
