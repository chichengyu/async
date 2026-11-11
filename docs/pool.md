# Pool（协程池）文档

## 概述

从外部模块使用 `Run` 时，回调参数分别使用
`*async.PoolInstance[T]`、`*async.MultiPool[T]` 和
`*async.ShardedPool[T]`，适用于 `Pool`、`PoolMulti` 和
`PoolSharded` 构建器。

`Pool[T]` 是泛型协程池，复用固定数量的 goroutine 处理高频并发任务。通过 `Pool[T]()` Builder 链式构建，终端方法 `Run(fn)` 自动管理生命周期。

业务回调若调用 `runtime.Goexit`，本任务以 `async.ErrTaskGoexit` 完成，值为类型零值，失败计数、结果通知、单次 context 取消和任务登记都会收尾；开启 FailFast 时也触发原取消链。Goexit 结束当前 worker goroutine，池在同一登记名额下补充 worker，后续已接受任务仍可执行，关闭及缩容仍按原规则进行。它不是 panic，不包装为 PanicError，也不重试本业务。生产中应正常返回 error 表示失败，避免依赖主动退出 goroutine；该保证不覆盖进程退出或永久阻塞。公开可执行示例见 [Example_poolAndGroupGoexit](tests/public_examples_test.go)。

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
> **⚠️ 区分结果收集和资源关闭**
>
> 要等待本批已接受任务并取得结果，在关闭前调用 `Wait()`，或使用 `WaitAndClose()`。`Wait()` 本身不关闭常驻 worker，仍须由所有者调用 `Close()`；Builder 的 `Run` 会在回调返回后关闭。直接 `Close()` 也会清理资源，但排队任务可能被丢弃并记录关闭错误，不能代替要求全部业务执行完毕的等待。回调若一直不返回，关闭也可能阻塞。

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
| `.SubmitTimeout(d)` | 设置实例级提交超时 | 无限制（阻塞等待） |
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
| `.MaxResults(n)` | 最大结果存储数（负数保留创建时的全局默认值，0=无限） | `core.GetDefaultMaxResults()`（初始全局默认 100_000） |
| `.DefaultMaxResults()` | 恢复为 -1，使用创建时的全局默认值 | — |
| `.Streaming(buf)` | 流式结果 channel 缓冲大小（buf>0 启用） | 0（禁用） |
| `.DefaultStreaming()` | 恢复默认（禁用流式，0） | — |
| `.Config(fn)` | 函数式配置，通过闭包修改 `Config` | — |
| `.DefaultPoolConfig()` | 重置所有配置为 `DefaultConfig()` | — |
| `.Logger(l)` | 注入自定义日志（**全局生效**） | 静默 |
| `.DefaultLogger()` | 恢复默认日志（`清除当前覆盖，继承全局日志器`） | — |

### PoolBuilder 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Run(fn)` | `error` | 创建池 → 执行 fn → fn 返回后自动 Close |

> **⚠️ `.Run(fn)` 回调签名**
>
> `fn func(ctx context.Context, p *async.PoolInstance[T]) error`：`p` 是已创建的池。未启用 FailFast 时 `ctx` 是 builder 上的上下文；启用 FailFast 时它是池的派生上下文，任务失败会请求取消该上下文。提交任务时应传入该 `ctx`，任务也需响应取消。fn 返回后自动 `Close` 池。

### PoolConfig 值配置

`async.DefaultPoolConfig()` 返回 `async.PoolConfig`：默认 worker 为 `async.IO()`，`MaxResults` 为 -1（保留池创建时的全局默认），其余字段为零值。以下方法都返回修改后的配置副本；原配置不会被修改。通过 Builder 的 `Config(func(async.PoolConfig) async.PoolConfig)` 应用返回值，后续 Builder 设置会覆盖相应字段。

| 方法 | 配置字段及用途 |
|---|---|
| `WithSize(n)` | `Size`：worker 数；非正数创建时回退默认并发度 |
| `WithTimeout(d)` | `Timeout`：单任务超时；非正数不覆盖池创建时默认值 |
| `WithSubmitTimeout(d)` | `SubmitTimeout`：提交等待超时；非正数不启用 |
| `WithFailFast()` | `FailFast`：启用快速失败；任务需响应取消 |
| `WithMaxPending(n)` | `MaxPending`：待处理任务数限制；非正数不设置该限制 |
| `WithOverflow(strategy)` | `Overflow`：任务队列溢出策略 |
| `WithRingBuf(capacity)` | `RingBufCap`：正数启用结果环形缓冲；溢出策略使用创建时全局默认值 |
| `WithMaxResults(n)` | `MaxResults`：正数限制主结果数，0 不限制，负数保留池创建时默认值 |
| `WithStreaming(bufSize)` | `Streaming`：正数启用流通道并设置缓冲大小，非正数不启用 |

`WithOverflow` 控制任务队列，而 `WithRingBuf` 使用全局环形缓冲溢出策略；二者互不替代。环形缓冲满时可能丢失结果，流通道满时也可能丢弃流结果。示例：

```go
err := async.Pool[int]().Config(func(c async.PoolConfig) async.PoolConfig {
    return c.WithSize(8).WithTimeout(3*time.Second).
        WithSubmitTimeout(500*time.Millisecond).WithMaxPending(1000).
        WithOverflow(async.OverflowError).WithMaxResults(10000).WithRingBuf(1000)
}).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
    if err := p.Submit(ctx, fn); err != nil {
        return err
    }
    results := p.Wait()
    _ = results
    return nil
})
_ = err
```

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
| `.SubmitTimeout(d)` | 设置实例级提交超时 | 无限制（阻塞等待） |
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
| `.MaxResults(n)` | 每分片最大结果数（负数保留创建时的全局默认值，0=无限） | 全局默认值（初始为 100_000） |
| `.DefaultMaxResults()` | 恢复为 -1，使用创建时的全局默认值 | — |
| `.Streaming(buf)` | 每分片流式结果 channel 缓冲大小 | 0（禁用） |
| `.DefaultStreaming()` | 恢复默认（禁用流式） | — |
| `.Pool(p)` | 注入外部协程池（分片复用，用户不再使用该池） | 无 |
| `.DefaultPool()` | 清除外部池注入，恢复内部自动创建 | — |
| `.Config(fn)` | 函数式配置，通过闭包修改 `Config` | — |
| `.DefaultMultiPoolConfig()` | 重置所有配置为 `DefaultConfig()` | — |
| `.Logger(l)` | 注入自定义日志（**全局生效**） | 静默 |
| `.DefaultLogger()` | 恢复默认日志（`清除当前覆盖，继承全局日志器`） | — |

### MultiPoolBuilder 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Run(fn)` | `error` | 创建池 → 分片 → 执行 fn → 自动 Close 所有分片 |

> **⚠️ `.Run(fn)` 回调签名**
>
> `fn func(ctx context.Context, mp *async.MultiPool[T]) error`：`mp` 是已创建的分片池。内部创建池并启用 FailFast 时，`ctx` 是创建池时得到的派生上下文；其他情况使用 builder 上的上下文。注入外部池时，Builder 的池配置（包括 FailFast）不会应用于外部池；其现有配置用于分片，回调仍收到 Builder 上下文。fn 返回后自动 `Close` 所有分片，包括注入的池。

---

## Pool 实例方法速查表

以下方法在 `Run` 回调中的 `p *async.PoolInstance[T]` 实例上可用：

### 任务提交

| 方法 | 签名 | 说明 |
|------|------|------|
| `Submit` | `func(ctx, fn) error` | 提交任务；队列或待处理限制阻塞时可能等待 |
| `SubmitAt` | `func(index, ctx, fn) error` | 指定合法索引提交；主结果保留且未被后续覆盖时按该索引读取，拒绝记录位置见下文 |
| `TrySubmit` | `func(ctx, fn) error` | 非阻塞提交；返回值取决于队列状态及溢出策略，见下文 |

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

正常执行的任务按以下顺序收尾：业务返回 → 结果转换（若配置）→ 主结果存储 → 尝试发送原生结果流 → 全局 WithResultCallback → 环形缓冲记录 → 单次 completion（若配置）→ 任务清理。转换与 completion 是不同提交入口，不能在一次公开调用中同时指定。结果流满时仍按既有规则丢弃；全局结果回调和 completion 同步阻塞时，Wait 也会等待它们返回。回调里读到结果流中的值，不代表池的全部统计和清理已经完成。

`SubmitAtWithResultTransform` 只转换保存及通知中的结果，不重算业务成功/失败计数，也不因转换后的错误触发池 FailFast。例如业务成功、转换增加错误时，Wait 返回错误结果但 OkCount 仍增加；业务失败、转换清除错误时，保存结果可为成功，FailCount 和业务失败触发的取消仍保留。转换函数 panic 时 Err 变成 PanicError，Value 保留转换前的业务值；转换函数在 panic 前只修改自己局部副本的字段不会提交这些字段，引用内容的副作用仍可能共享。

合法索引的预检查拒绝也调用转换函数，但提交仍返回原拒绝 error，失败统计也按拒绝累计；这条路径不通知全局结果回调或结果流。非法索引不转换、不保存、不计数。主存储范围内的预检查拒绝不会写入镜像环形缓冲，超出正数主结果上限的拒绝才可写入溢出环形缓冲。生产中不要通过清除 Err 隐藏拒绝，也不要用转换后结果倒推原始业务统计；保留独立业务原因，并按所需范围选择 SubmitWithCompletion。对照见 [转换结果示例](tests/public_examples_test.go) 的 Example_poolTransformStatistics 与 [通知契约测试](../internal/pool/transform_notification_contract_test.go)。

### 等待与关闭

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Wait()` | `[]Result[T]` | 阻塞等待所有任务完成，返回结果切片 |
| `WaitAndClose()` | `[]Result[T]` | 先 Wait 再 Close |
| `WaitTimeout(d)` | `([]Result[T], bool)` | 带超时等待，ok=false 表示超时 |
| `WaitContext(ctx)` | `([]Result[T], bool)` | 带 context 等待 |
| `Close()` | — | 关闭提交入口，等待 worker 退出并完成已登记任务清理；关闭期间未能执行的排队任务会记录 `ErrPoolClosed` |
| `CloseAndWait()` | — | 先 `Close()`，再 `Wait()` 等待清理完成；不返回结果 |
| `CloseAndWaitTimeout(d)` | `(bool, <-chan struct{})` | 先启动关闭，再限时等待关闭清理；重复调用等待同一关闭流程，仍可能超时 |
| `CloseByIdle(d)` | — | 按 100ms 间隔检查空闲或等待期限，再同步关闭；总耗时可能超过 d |
| `Reset()` | `(*async.PoolInstance[T], error)` | 完整重置状态以便复用 |
| `ResetWait()` | `*async.PoolInstance[T]` | 重置 waited 状态，允许继续 Submit+Wait |

### 状态查询

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Size()` | `int` | 配置的目标 worker 数；不表示仍存活的 worker 数量 |
| `Active()` | `int` | 当前已取得任务但尚未结束处理的 worker 数 |
| `Busy()` | `int` | 正在执行任务的 goroutine 数 |
| `Pending()` | `int` | 已取得等待名额且尚未被 worker 取出的任务数，可包含阻塞入队的提交 |
| `QueueDepth()` | `int` | 等同 Pending，不是 channel 长度；不含等待获取 MaxPending 名额的提交 |
| `TotalCount()` | `int64` | 记录的提交次数，包括已记录的拒绝和丢弃；重复索引分别计数 |
| `OkCount()` | `int64` | 已成功完成的任务数，不包含尚未完成的任务和稀疏索引空位 |
| `FailCount()` | `int64` | 失败任务数，包括已记录的拒绝，以及执行中因 FailFast 返回取消错误的任务 |
| `HasError()` | `bool` | 是否有错误 |
| `Stats()` | `PoolStats` | 运行时统计快照 |

> **注意**：`OkCount()` 只统计已完成且成功的任务。运行中、失败、取消、提交被拒绝或 panic 的任务不计入；若要获取最终批次统计，请先 `Wait()`。调用 `Reset()` 后计数会清零。

这些计数在运行中是实时读取，`Stats()` 的各字段分别采样，不能视为同一时刻的事务快照。`Stats()` 读取任务超时配置时没有与 `WithTimeout` 同步，应在并发操作前完成配置，或由调用方协调配置修改和统计读取。`ResetWait()` 也会清零批次计数。

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

以上方法只检查主结果存储，不包括超过 `MaxResults` 后写入环形缓冲的结果；溢出部分须从 `Wait()` 或 `Flush()` 返回值处理。等待前查询只是当前快照，超时等待返回后后台任务仍可能继续完成。`FirstError()` 返回主结果顺序中的第一个错误，不保证是最早发生的错误；`Values()` 仅包含 `Occupied` 且 `Err == nil` 的结果。

```go
results := p.Wait()
for _, r := range results {
    if r.Err != nil {
        log.Printf("任务失败: %v", r.Err)
    }
}
if err := p.JoinErrors(); err != nil {
    log.Printf("主结果错误: %v", err)
}
```

### 运行时配置（实例级）

| 方法 | 签名 | 说明 |
|------|------|------|
| `WithTimeout(d)` | `*async.PoolInstance[T]` | 设置任务超时；非正数不启用任务超时 |
| `WithSubmitTimeout(d)` | `*async.PoolInstance[T]` | 设置提交超时；非正数不启用提交超时 |
| `WithFailFast(ctx)` | `(*async.PoolInstance[T], context.Context)` | 启用 FailFast，返回新的 ffCtx |
| `WithFFCtx(ctx)` | `(*async.PoolInstance[T], context.Context)` | `WithFailFast` 的缩写 |
| `WithStreaming(bufSize)` | `*async.PoolInstance[T]` | 启用流式结果（bufSize≤0 自动=Worker×2） |
| `WithResultCallback(fn)` | `*async.PoolInstance[T]` | 设置每个任务完成时的回调；回调 panic 会被捕获并记录，不改变任务结果 |
| `WithRingBuffer(cap, overflow)` | `*async.PoolInstance[T]` | 启用环形缓冲 |
| `WithMaxPending(n)` | `*async.PoolInstance[T]` | 正数设置最大待处理数；非正数保留原值 |
| `WithOverflow(strategy)` | `*async.PoolInstance[T]` | 设置溢出策略 |
| `WithMaxResults(n)` | `*async.PoolInstance[T]` | 正数限制主结果数；0 不限制；负数保留原值 |
| `WithTraceID(ctx)` | `(*async.PoolInstance[T], context.Context)` | 注入 TraceID 并返回新 ctx |
| `WithContext(ctx)` | `(*async.PoolInstance[T], context.Context)` | 设置池级上下文 |
| `MergeFailFastCancel(cancel)` | `*async.PoolInstance[T]` | 合并非 nil 的外部 CancelFunc；不单独启用 FailFast，须在提交前配置 |

实例配置应在并发提交、执行或查询前完成；不要在运行期间并发修改这些字段。注意实例的 `WithMaxPending(0)` 不会清除已有正数限制，`WithMaxResults(-1)` 也不会恢复默认值。需要清除待处理限制时，应在创建池前通过 Builder 配置。

```go
err := async.Pool[int]().Worker(8).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
    p.WithTimeout(3*time.Second).WithSubmitTimeout(500*time.Millisecond).
        WithMaxPending(1000).WithOverflow(async.OverflowError).WithMaxResults(10000)
    if err := p.Submit(ctx, fn); err != nil {
        return err
    }
    _ = p.Wait()
    return nil
})
_ = err
```

`WithResultCallback` 同步执行，多个任务的回调可能并发；入队等待失败或关闭清理时，回调也可能在提交或关闭池的 goroutine 中执行。普通 `Submit`/`TrySubmit` 在预检查（例如已取消）或背压检查中直接拒绝时，不通知这个回调，也不发送到结果流。需要记录每次提交的终态时使用 `SubmitWithCompletion`，并检查提交错误；`OverflowDrop` 可能返回 nil，但 completion 收到 `ErrQueueOverflow`。回调需自行保护共享数据，保持轻量，且不能同步调用所属池的 `Wait`、`Close` 或 `CloseAndWait`，否则可能等待自身而死锁。多池的回调同样不能同步等待或关闭所属多池及其分片。

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
| `ResizeAndWaitTimeout(newSize, timeout)` | — | 调整 worker 数并限时等待当前任务批次归零；不取消任务，不保证旧 worker 已全部退出 |
| `EnableAutoScale(config)` | — | 启用自动扩缩容（config=nil 使用默认配置） |
| `DisableAutoScale()` | — | 停止自动扩缩容 |
| `IsAutoScaleEnabled()` | `bool` | 是否已启用自动扩缩容 |
| `Ctx()` | `context.Context` | 获取池级上下文 |

`ResizeAndWaitTimeout` 先同步调用 `Resize`，再暂停新提交、等待正在登记的提交收敛，最后才启动 `timeout` 计时器等待已登记任务归零。时长只限制最后的任务等待，不限制前面的锁等待和登记收敛，不能作为整个方法的调用时限。非正时长使最后等待立即到期，但不会取消业务、关闭池或返回完成标记；方法返回后可继续提交，其他等待/关闭流程设置的禁止提交状态仍生效。完整可运行示例见 [Example_poolResizeWaitTimeout](tests/public_examples_test.go)。

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
| `SubmitBatch` | `func(ctx, items, fn) []SubmitResult` | 按轮询顺序逐项提交，返回每项输入索引和提交错误 |

### 等待与关闭

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Wait()` | `[]Result[T]` | 等待所有分片完成，合并结果 |
| `WaitTimeout(d)` | `([]Result[T], bool)` | 带超时等待；false 也可能表示与其他等待重叠或分片已等待 |
| `WaitContext(ctx)` | `([]Result[T], bool)` | 带 context 等待；false 也可能表示与其他等待重叠或分片已等待 |
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
>
> 通过 `WithContext`/`WithFailFast` 创建后再 `Shard`，或通过内部创建且启用 FailFast 的 MultiPoolBuilder 创建时，各片持有独立子 context。正常等待单片不会取消其他片使用的共享层；取得本次等待权的整体 `Wait`/`WaitContext`/`WaitTimeout` 返回前会释放共享层，包括 `false` 的未完成等待。与另一等待重叠而直接被拒绝的调用不释放共享层；最终 `Close` 也会释放。父 context 不被反向取消，整体等待后的共享 `ctx.Err()` 不能当作业务失败依据。多池没有整体 Reset，下一批优先新建；逐片复位仍须重新配置有效的 context。

---

## 默认值体系

Pool 的默认值遵循**三层架构**：全局级 → Builder 级 → 实例级。

### 一层：全局默认值

| 默认值 | 获取函数 | 修改函数 | 默认值 | 说明 |
|--------|----------|----------|--------|------|
| 默认超时 | `core.GetDefaultTimeout()` | `core.SetDefaultTimeout(d)` | **30s** | 设为 0 关闭默认超时 |
| 提交超时 | `async.GetSubmitTimeout()` | `async.SetSubmitTimeout(d)` | 全局值初始为 **5s** | 当前只保存/读取全局值，不会自动应用到 Pool；`NewPool` 默认提交不设超时（阻塞等待） |
| IO 并发度 | `core.IO()` | 不可修改 | **`runtime.NumCPU()×2`** | PoolBuilder 默认 Worker 数 |
| 最大结果数 | `core.GetDefaultMaxResults()` | `core.SetDefaultMaxResults(n)` | **100_000** | `MaxResults(-1)` 时使用 |
| 默认环形缓冲容量 | `core.GetDefaultRingBufferCap()` | `core.SetDefaultRingBuffer(n, strategy)` | **0**（不启用） | `RingBuf(0)` 不覆盖全局配置 |
| 默认溢出策略 | `core.GetDefaultOverflowStrategy()` | `core.SetDefaultRingBuffer(n, s)` | **OverflowDrop** | 环形缓冲满时策略 |

### 二层：Builder 级默认值

| `Default*` 方法 | 源码行为 | 适用于 |
|-----------------|----------|--------|
| `.DefaultWorker()` | `b.cfg.Size = core.IO()`（`NumCPU×2`） | PoolBuilder, MultiPoolBuilder |
| `.DefaultTimeout()` | `b.cfg.Timeout = 0`（清除覆盖，创建池时读取全局默认；初始全局值为 30s） | PoolBuilder, MultiPoolBuilder |
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
| `.DefaultLogger()` | `清除当前覆盖，继承全局日志器`（恢复全局静默日志） | PoolBuilder, MultiPoolBuilder |

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

当测量表明单 Pool 的内部竞争限制吞吐时，可评估多池分片。扩展效果取决于任务耗时、CPU、内存和下游容量，需要通过业务负载测试确认：

```go
// 8 个独立 Pool 分片；总 worker 和缓冲容量也按片数增加
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

// 16 分片 MultiPool：按实际负载设置每片的待处理和结果上限
async.PoolMulti[MyType]().Context(ctx).
    Shards(16).
    Worker(async.IO()).
    MaxResults(100_000).RingBuf(50_000).
    MaxPending(10_000).
    Overflow(async.OverflowDrop).           // 超过每片待处理限制时可能丢弃任务
    Run(func(ctx context.Context, mp *async.MultiPool[MyType]) error {
        for i := 0; i < 100_000_000; i++ {
            idx := i
            mp.Submit(ctx, func(ctx context.Context) (MyType, error) {
                return process(ctx, idx)
            })
        }
        results := mp.WaitAndClose() // 结果上限和环形缓冲满时也可能丢失结果
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

提交任务到任务队列；队列能接收时即可返回，不要求 worker 已开始执行。队列满时可能阻塞，正数 `SubmitTimeout` 限制入队等待；`MaxPending` 和溢出策略也可能使本次提交提前返回，须检查提交错误并最终检查任务结果：

```go
err := p.Submit(ctx, func(ctx context.Context) (string, error) {
    return fetchData(ctx), nil
})
if err != nil {
    log.Printf("提交失败: %v", err)
}
```

### TrySubmit

非阻塞提交，任务 channel 无法立即接收时返回 `ErrSubmitTimeout`。设置正数 `MaxPending` 且达到限制时，`OverflowBlock` 返回 `ErrSubmitTimeout`，`OverflowError` 返回 `ErrQueueOverflow`，`OverflowDrop` 返回 `nil`；被拒绝的任务记录相应错误，可通过 `Wait()` 或 `Errors()` 查看（结果存储上限可能限制保留数量）。因此 `nil` 不一定表示任务已执行或已入队。

```go
err := p.TrySubmit(ctx, fn)
if errors.Is(err, async.ErrSubmitTimeout) {
    log.Println("队列已满，任务被丢弃")
}
```

### SubmitAt

在指定索引位置提交任务；结果保留在主结果存储时，`Wait()` 返回的切片使用对应位置。超过 `MaxResults` 后写入环形缓冲的结果按缓冲写入顺序追加，不能按原索引定位：

参数 `index` 必须在 `0..1<<24` 内（实现常量 `core.MaxPoolIndex`，未在根包导出）；非法索引返回 `ErrInvalidIndex`，不增加任务计数、不存储结果，完成回调入口仍会通知该次失败。生产中按输入分配唯一连续索引，检查每次提交错误及最终 `Result.Err`，并控制结果保留上限。稀疏索引会产生 `Occupied=false` 的空位；有效任务重复使用同一位置会覆盖旧结果，并发重复时最终代表取决于写入顺序。

合法位置的预检查/背压拒绝会记录错误：尚未分配的索引保留在原位置，已分配位置的拒绝追加到新的提交序号，避免覆盖旧结果；因此不能把失败提交一律按请求索引读取。提交已登记后才在入队阶段被拒绝的路径，还取决于本次是否首次占用该序号。错误统计按提交次数累计，不等于可见结果错误条数；优先用完成回调跟踪逐项终态。可运行对照见 [Example_poolIndexedRejection](tests/public_examples_test.go)。

```go
for i, item := range items {
    if err := p.SubmitAt(i, ctx, func(ctx context.Context) (string, error) {
        return process(ctx, item)
    }); err != nil {
        log.Printf("索引 %d 提交失败: %v", i, err)
    }
}
// 仅当对应索引保留在主结果中时，results[i] 才对应 items[i]。
```

---

## 等待与关闭

### Wait

阻塞等待所有已提交任务完成。主结果按提交顺序排列；配置正数 `MaxResults` 和环形缓冲时，尚未被 `Flush` 取出的溢出结果按写入缓冲的顺序追加，缓冲满时可能丢失：

`Wait` 等待任务登记计数归零；当前实现先完成通知、取消单次 context 并递减 `Busy` / `Active`，再释放任务等待登记。在不并发启动下一批或改动生命周期的前提下，成功等完本批时这两个业务计数已归零。限时等待提前返回的快照仍可能包含未完成任务，不能套用该结论。`Wait` 不等待 worker 退出，需要完整关闭时使用 `Close` 或 `WaitAndClose`。

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
// 二选一：带超时等待
results, ok := p.WaitTimeout(3 * time.Second)
// 或使用 p.WaitContext(ctx)，不要对同一批任务再次等待。
```

> **⚠️ 超时/取消后请求取消已登记任务**
>
> `WaitTimeout` 超时和 `WaitContext` ctx 取消后，池会取消已登记的任务，并取消随后登记的在途任务，然后返回当前结果。未完成任务和结果流的清理继续在后台进行；`ResetWait` 和 `Reset` 会等待清理完成。任务需要自行响应 context 才能尽早结束。

#### 调用者主动取消模式

结合 builder 链式配置和 `WaitContext`，调用方可在外部主动取消所有正在执行的任务：

```go
// ① 由调用方掌控取消时机
parentCtx, cancel := context.WithCancel(context.Background())
defer cancel()

// ② 在 Run 回调中提交任务，Run 返回时自动关闭池
err := async.Pool[int]().Context(parentCtx).Worker(6).Run(func(ctx context.Context, pool *async.PoolInstance[int]) error {
    var submitErr error
    for i := 0; i < 10; i++ {
        if err := pool.Submit(ctx, func(ctx context.Context) (int, error) {
            select {
            case <-ctx.Done():
                return 0, ctx.Err()   // 收到取消信号，尽早返回
            case <-time.After(10 * time.Second):
                return 42, nil
            }
        }); err != nil {
            submitErr = err
            break
        }
    }

    // ③ 另一 goroutine 主动取消
    go func() {
        time.Sleep(100 * time.Millisecond)
        cancel() // ← 调用者主动 cancel，取消链传播到所有 taskCtx
    }()

    // ④ 等待任务，超时后请求取消并返回当前结果
    waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer waitCancel()
    results, ok := pool.WaitContext(waitCtx)
    _ = results
    _ = ok
    return submitErr
})
_ = err
```

#### 超时自动取消模式

对同一批任务选择 `WaitTimeout` 或 `context.WithTimeout` + `WaitContext`：

```go
// 方式一：WaitTimeout 直接传超时
results, ok := p.WaitTimeout(100 * time.Millisecond)
_, _ = results, ok
```

```go
// 方式二：WaitContext + context.WithTimeout
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()
results, ok := p.WaitContext(ctx)
_, _ = results, ok
```

> 两种方式在等待超时后都会请求取消已登记任务；已运行的回调必须自行响应 context，不能保证立即停止。两个示例分别用于独立批次，不能对同一批次连续调用。

### Close / CloseAndWait / CloseAndWaitTimeout

| 方法 | 说明 |
|------|------|
| `Close()` | 发出关闭信号，等待 worker 退出并清理未执行的任务；并发调用也会等待关闭完成 |
| `CloseAndWait()` | 关闭池并等待清理完成，再调用 `Wait()` |
| `CloseAndWaitTimeout(d)` | 限时等待关闭清理；首次超时返回 worker 通道，已有关闭流程的超时返回完整清理通道 |

关闭不会强制终止正在执行的任务，因此任务必须自行响应取消或结束。关闭可能丢弃尚未执行的任务。首次调用 `CloseAndWaitTimeout` 返回 `ok == false` 时，返回的通道关闭表示 worker 已退出，队列和结果清理可能仍在进行；需要等待完整清理时可调用 `Close()`。如果关闭已经进行，超时返回的通道在该次关闭清理完成后关闭。重复调用会等待已有关闭流程，超时内完成时返回 `true, nil`。

`Close` 本身不取消业务或池 context；worker 可能继续执行已排队的任务。关闭唤醒的阻塞提交，会在提交调用者中同步记录拒绝结果、发送结果流、调用全局结果回调及逐项 completion。首次返回的 worker 通道关闭时，这些通知仍可能阻塞，流也可能尚未关闭，失败计数尚未增加。阻塞在入队 channel 的提交此时还可能持有 `Pending` 名额；等待 `MaxPending` 名额的提交尚未计入 `Pending`。不要用 worker 通道或统计值替代完整关闭；通知必须及时返回，不能在其中同步关闭所属池。

```go
ok, workersDone := p.CloseAndWaitTimeout(100 * time.Millisecond)
if !ok {
    <-workersDone // 首次关闭只保证 worker 退出
    p.Close()     // 等待已登记提交、同步通知、队列和流清理完成
}
// Close 不取得业务结果；本批尚未调用等待方法时，可再 Wait 并检查逐项 Err。
results := p.Wait()
_ = results
```

生产中由拥有池的调用方管理关闭，并为业务及通知设置可终止的逻辑。完整 `Close` 可能超过前一次限时等待；如需主动取消业务，应管理提交时传入的 context。关闭后复用需要本批先进入 waited 状态，再调用 `Reset()` 重建 worker；仅 `ResetWait()` 不会重新打开已关闭的池。

`CloseAndWaitTimeout` 的计时器在取得关闭/扩缩容互斥锁并更新关闭状态之后创建，`d` 不限制此前的锁等待。`CloseByIdle` 只以 `Active()==0 && Busy()==0` 判断空闲，不检查 `Pending`，不能据此保证排队任务已执行完；它每 100ms 检查一次期限，轮询可能越过该期限，后续同步 `Close` 也不受该时长限制。需要完成全部业务并收集结果时使用 `WaitAndClose`。

### ResetWait / Reset

| 方法 | 说明 |
|------|------|
| `ResetWait()` | 重置 waited 状态，允许继续 Submit+Wait |
| `Reset()` | 完整重置，允许继续 Submit+Wait |

`Wait` 会取消通过 `WithContext` 或 `WithFailFast` 创建的池上下文。`ResetWait` 不重建该上下文；如果下一批任务需要使用池上下文，应以仍有效的父上下文重新调用 `WithContext` 或 `WithFailFast`，并使用新返回的上下文提交任务。不要将已取消的旧上下文作为新父上下文。

`ResetWait()` 应在本批等待方法返回后、下一批提交前调用，并由调用方与其他提交、等待和配置操作协调；它不重启 worker。它清除本批结果、计数器和环形缓冲，启用流式结果时还会创建新的 channel，下一批应重新调用 `StreamResults()` 获取 channel。超时或取消等待后，`ResetWait()` 会等待后台清理和未完成任务退出，因此不保证快速返回。若尚未进入 waited 状态，它直接返回而不重置。

---

## 状态查询与错误提取

### 状态查询

| 方法 | 说明 |
|------|------|
| `Size()` | 配置的目标 worker 数；缩容时旧 worker 可尚未退出，关闭后不归零 |
| `Active()` | 已取得任务但尚未结束处理的 worker 数 |
| `Busy()` | 正在执行任务的 goroutine 数 |
| `Pending()` | 已取得等待名额、尚未被 worker 取出的任务数；可包含阻塞入队的提交 |
| `QueueDepth()` | 等同 Pending；不是底层 channel 的长度，也不含等待获取 MaxPending 名额的提交 |
| `TotalCount()` | 记录的提交尝试数，包括已记录的拒绝和丢弃 |
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
        consumed := make(chan struct{})

        go func() {
            defer close(consumed)
            for r := range ch {
                if r.Ok() {
                    fmt.Println("实时收到:", r.Value)
                }
            }
        }()

        var submitErr error
        for _, item := range items {
            item := item
            if err := p.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            }); err != nil {
                submitErr = err
                break
            }
        }
        p.Wait()
        <-consumed
        return submitErr
    })
```

> **Streaming 与 Wait**
>
> 启用流式消费后，`Wait()` 仍返回保存的结果，受 `MaxResults` 和环形缓冲配置限制。流式 channel 的丢弃不影响已保存的结果。`Wait()` 关闭流通道，但不会等待消费者处理完已接收的结果，需自行同步消费者。
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
            if err := p.Submit(ctx, fn); err != nil { return err }
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

当 MaxResults 为 0 时，完成结果同时写入主存储和环形缓冲；Flush 取得的是镜像结果，不能再与 Wait 的主结果相加计数。此模式的 Wait 不追加或排空镜像缓冲。正数上限时环形缓冲才只存上限外结果，Wait 会追加尚未取出且未丢失的溢出项。

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
            if err != nil { return err } // 取消、关闭和提交超时也需要处理
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
            if err := mp.Submit(ctx, func(ctx context.Context) (string, error) {
                return httpGet(ctx, url)
            }); err != nil { return err }
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
            if err := mp.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            }); err != nil { return err }
        }
        results := mp.Wait()
        return handleResults(results)
    })
```

### Key 亲和提交

```go
err := async.PoolMulti[int]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *async.MultiPool[int]) error {
        for _, userID := range userIDs {
            // 同一 userID 始终路由到同一分片
            if err := mp.SubmitKeyed(uint64(userID), ctx, func(ctx context.Context) (int, error) {
                return processUser(ctx, userID)
            }); err != nil { return err }
        }
        results := mp.Wait()
        return handleResults(results)
    })
```

### 批量提交

`SubmitBatch` 逐项调用分片的 `Submit`，可能因队列限制而阻塞；返回的 `Index` 是输入切片索引，`Err` 是提交错误，不含分片编号。`OverflowDrop` 策略下丢弃任务也可能返回 nil，不能仅凭 `Err` 判断任务已执行。

```go
err := async.PoolMulti[int]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *async.MultiPool[int]) error {
        results := mp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
            return process(item)
        })
        var submitErr error
        for _, r := range results {
            if r.Err != nil {
                log.Printf("索引 %d 提交失败: %v", r.Index, r.Err)
                if submitErr == nil { submitErr = r.Err }
            }
        }
        allResults := mp.Wait()
        resultErr := handleResults(allResults)
        if submitErr != nil { return submitErr }
        return resultErr
    })
```

### 流式消费（Fan-in 所有分片）

```go
err := async.PoolMulti[string]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, mp *async.MultiPool[string]) error {
        mp.WithStreaming(1024)
        // 每片最多保存 1000 个主结果；错误汇总独立于该上限和流。
        mp.WithMaxResults(1000)
        var mu sync.Mutex
        var firstTaskErr error
        mp.WithResultCallback(func(r async.Result[string]) {
            if r.Err != nil {
                mu.Lock()
                if firstTaskErr == nil { firstTaskErr = r.Err }
                mu.Unlock()
            }
        })
        stream := mp.StreamResults()

        done := make(chan error, 1)
        go func() {
            var submitErr error
            for _, item := range items {
                if err := mp.Submit(ctx, func(ctx context.Context) (string, error) {
                    return process(item)
                }); err != nil {
                    submitErr = err
                    break
                }
            }
            mp.WaitAndClose()
            mu.Lock()
            taskErr := firstTaskErr
            mu.Unlock()
            var dropped int64
            for i := 0; i < mp.ShardCount(); i++ {
                dropped += mp.GetShard(i).StreamDropped()
            }
            var streamErr error
            if dropped > 0 { streamErr = fmt.Errorf("结果流丢弃 %d 项", dropped) }
            done <- errors.Join(submitErr, taskErr, streamErr)
        }()

        for r := range stream {
            if !r.Ok() {
                log.Printf("失败: %v", r.Err)
                continue
            }
            saveResult(r.Value)
        }
        return <-done
    })
```

此片段需要导入 `context`、`errors`、`fmt`、`log` 和 `sync`。结果回调可能跨片并发，必须快速返回；这里用互斥锁保留首个收到的任务错误，次序不代表输入顺序。普通预检查拒绝可能不通知回调，因此仍须检查每次 Submit 的返回值。

流只用于及时消费成功值，不能保证全部结果交付；慢消费者可能造成丢弃，示例在最终关闭后汇总各片 StreamDropped 并返回错误。已保存的成功值不会回滚，重试需保证幂等；需要每项结果可靠落库时，应设计持久化确认与重试，不能只靠流。MaxResults 按每片计算且不控制已启用的环形缓冲，生产容量需一并预算。

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
            if err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
                return processItem(ctx, item)
            }); err != nil {
                return err
            }
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
            if err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
                return processItem(ctx, item)
            }); err != nil {
                return err
            }
        }
        results := mp.Wait()

        fmt.Printf("总 worker: %d, 总任务: %d, 成功: %d, 失败: %d\n",
            mp.TotalWorkerCount(),
            mp.TotalCount(),
            mp.TotalOkCount(),
            mp.TotalFailCount())
        return handleResults(results)
    })
```

### 注入外部池

```go
err := async.Pool[string]().Context(ctx).Worker(8).
    Run(func(ctx context.Context, base *async.PoolInstance[string]) error {
        return async.PoolMulti[string]().Context(ctx).Pool(base).Shards(4).
            Run(func(ctx context.Context, mp *async.MultiPool[string]) error {
                for _, item := range items {
                    if err := mp.Submit(ctx, func(ctx context.Context) (string, error) {
                        return processItem(ctx, item)
                    }); err != nil {
                        return err
                    }
                }
                return handleResults(mp.Wait())
            })
    })
```

Pool(base) 把 base 作为第 0 片，其他片复制其配置；MultiPoolBuilder 的 Worker/Timeout 等 cfg 配置不会再应用，Shards 仍生效。内层 Run 接管并关闭全部分片（含 base），返回后不要再用 base 提交；外层 Run 的重复 Close 安全。

---

## AutoScale 自动扩缩容

检测连续次数满足后，扩容取当前数量乘 `ScaleUpFactor` 的整数部分；若不能增长则至少加 1，再限制至 `MaxWorkers`。缩容使用 `ScaleDownFactor`，若不能下降则至少减 1，再限制至 `MinWorkers`。乘积在转为整数之前先应用对应边界，避免有限大系数或乘积溢出导致错误目标。生产中应使用合理的有限正数系数并校验上下界、阈值和资源预算；`Normalize()` 不完整校验配置。

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
            if err := p.Submit(ctx, func(ctx context.Context) (int, error) {
                return processItem(ctx, item)
            }); err != nil {
                return err
            }
        }
        results := p.Wait()
        return handleResults(results)
    })
```

> **⚠️ AutoScale 相关方法**
>
> - `p.EnableAutoScale(config)`：config 为 nil 时自动使用 `DefaultAutoScaleConfig()`
>   启用后重复调用不会更改当前配置；要应用新配置，先禁用再启用。配置会在启用时复制。
> - `p.DisableAutoScale()`：停止自动扩缩容，并把目标 worker 数调到当前配置的 MinWorkers；可能扩容或缩容，不能当作只关闭监视。
> - `p.IsAutoScaleEnabled()`：查询是否已启用
> - `p.Resize(newSize)`：手动调整 worker 数

启用时复制并规范化配置，但保持现有 Size；初始 Worker 应在 Min/Max 范围内。Size 是目标数，缩容返回不代表旧 worker 已全部退出，也不会中断正在运行的回调。手动 Resize 会扩展保存的上下界，自动调整也经过同一路径；范围外初始值可能因此改变保存边界，详见 [AutoScale 配置边界](config.md#autoscaleconfig-字段)。Enable/Disable 的固定可执行对照见 [Example_autoScaleTargetLifecycle](tests/public_examples_test.go)。

确认计数只在严格超过扩容阈值或严格低于缩容阈值时连续累计，等号及中间区间清零；乘倍数截断后若不能向目标方向改变，尝试增减 1 再限制边界。关闭池会结束监视循环，但 IsAutoScaleEnabled 标记仍可能为 true；不要据此标记认定池仍可用。Reset 保留已启用配置并重启监视，Group 的 Wait/Reset 规则不同。

---

## 辅助函数

### ForEachPool —— 切片元素协程池并发

`async.ForEachPool` 为切片元素依次提交协程池任务，等待本批任务结束并关闭池后返回；它不是立即返回的异步方法。返回的 `*async.NoResultPool` 已关闭，不能继续提交任务。

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

返回值 `*NoResultPool` 是 `*async.PoolInstance[struct{}]` 的类型别名；第二个返回值按输入顺序返回首个提交或执行错误，包括取消、拒绝和 panic。错误汇总独立于 `MaxResults`，超出结果保留上限的任务失败也会返回错误；池本身的 `FirstError()` 仍只查看保留的结果。需要逐项确认提交和执行结果时，使用 Pool 显式提交并检查返回值及结果。回调应响应传入的 context；忽略取消且一直阻塞会使该函数无法返回。

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

`Shard()` 把原池作为第 0 片，另外创建其余分片；`shards <= 1` 时仅包装原池。新分片复制 worker 数、任务/提交超时、等待上限、溢出策略、主结果上限、FailFast 和结果回调；已启用的环形缓冲及结果流使用相同容量的新空容器，扩缩容复制当前配置并独立启动。已有任务、结果、计数和缓冲内容不会复制。应在提交前创建分片，回调须支持跨片并发；关闭 MultiPool 也会关闭原池。

---

## 自定义日志

```go
err := async.Pool[int]().
    Logger(myLogger).    // 全局生效
    Run(fn)

err = async.Pool[int]().
    Logger(myLogger).
    DefaultLogger().     // 当前构建器恢复继承全局日志器
    Run(fn)
```

---

## 常见错误

- **❌ 未管理 Pool 生命周期**：`Submit` 只是提交任务，worker 异步执行。通过公开 Builder 的 `Run` 管理池，并在需要完整结果时在回调中调用 `Wait()`；回调返回后 `Run` 会关闭池。

  ```go
  err := async.Pool[int]().Worker(8).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
      if err := p.Submit(ctx, fn); err != nil {
          return err
      }
      results := p.Wait()
      _ = results
      return nil
  })
  _ = err
  ```

- **❌ Wait 后继续 Submit**：`Wait()` 一次性使用，调用后通道被标记为已完成，再次 `Submit` 会返回错误。如需复用请调用 `ResetWait()` 或 `Reset()`。

  ```go
  // ✅ 正确：复用 Pool
  results1 := p.Wait()
  p.ResetWait()
  p.Submit(ctx, fn2)
  results2 := p.Wait()
  ```

- **❌ OverflowBlock + 无 SubmitTimeout 可能长期阻塞**：默认任务溢出策略是阻塞；当任务队列无法接收新任务时，`Submit` 可能等待。生产环境应根据负载设置 `SubmitTimeout`、处理提交错误，并选择合适的溢出策略。

  ```go
  // ❌ 危险：无限阻塞
  err := async.Pool[int]().Worker(8).Run(fn)

  // ✅ 生产级：30s 提交超时或错误降级
  async.Pool[int]().Context(ctx).
      Worker(8).SubmitTimeout(30*time.Second).
      Overflow(async.OverflowError).Run(fn)
  ```

- **❌ DefaultOverflow() ≠ 恢复运行时默认**：`DefaultOverflow()` 显式将任务溢出策略设为 `OverflowDrop`；新建 Pool 的任务溢出策略是 `OverflowBlock`。环形缓冲的溢出策略由 `SetDefaultRingBuffer` 单独配置，不受此方法控制。

- **❌ PoolBuilder 没有 `.Build()`**：与 GroupBuilder 不同，公开的 `PoolBuilder` 只有 `.Run(fn)` 作为终端方法，没有 `.Build()`。在回调内使用池，回调返回后由 `Run` 关闭。

  ```go
  // ❌ 错误：PoolBuilder 没有 Build
  p := async.Pool[int]().Build()  // 编译错误

  // ✅ 正确：在 Run 回调中使用池
  err := async.Pool[int]().Worker(8).Run(fn)
  _ = err
  ```

- **❌ 只查看受限的主结果存储**：未更改全局设置时 `MaxResults` 默认 100_000。提交 150_000 个任务时，主存储最多保留前 100_000 个提交索引的结果，其余结果仍可能通过配置的回调、流或环形缓冲处理；未配置消费路径时无法从主存储取回。错误查询同样只覆盖保留结果，应结合失败统计与消费路径检查。

  ```go
  // 场景：提交 500K 任务
  async.Pool[int]().Context(ctx).
      Worker(64).
      MaxResults(0).        // 不因主结果保留上限截断；须预留足够内存
      Streaming(4096).      // 同时启用流式消费，结果仍保存在 results 中
      Run(fn)
  ```

- **❌ Streaming 消费不及时导致结果丢弃**：stream channel 写满后，新结果不会阻塞 worker，而是从流中丢弃。应及时消费并通过 `StreamDropped()` 监控。

  ```go
  // ✅ 正确：启用 Streaming 后立即消费
  err := async.Pool[int]().Context(ctx).
      Streaming(1024).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
          consumed := make(chan struct{})
          go func() {
              defer close(consumed)
              for r := range p.StreamResults() {
                  handle(r)
              }
          }()
          // ... Submit 任务 ...
          p.Wait()
          <-consumed
          return nil
      })
  ```

- **❌ Logger 全局副作用**：`.Logger(l)` 影响**全局**所有 Pool/Group/Retry 等组件（内部调用 `core.SetLogger(l)`）。在测试或需要隔离日志时，记得用 `.DefaultLogger()` 恢复。

- **❌ EnableAutoScale(config) 传入 nil 的歧义**：`config == nil` 时自动使用 `DefaultAutoScaleConfig()`，也就是说 `EnableAutoScale(nil)` 会启用扩缩容而非禁用。要禁调用 `DisableAutoScale()`。

- **❌ RingBuffer 不及时消费导致结果丢失**：缓冲区满时，`OverflowDrop` 覆盖最旧结果，其他策略拒绝新写入；应在任务执行期间使用 `Flush()` 消费，并按需配置 `MaxResults` 和缓冲容量。`RingBufDropped()` 只统计被覆盖的结果。
