# Group（任务组）文档

## 概述

从外部模块使用 `Run` 时，回调参数可分别声明为
`*async.GroupInstance[T]`、`*async.GroupNoResult`、
`*async.MultiGroup[T]` 或 `*async.ShardedGroup[T]`，
对应 `Group`、`GroupVoid`、`GroupMulti` 和 `GroupSharded` 构建器。

`Group[T]` 按批次管理并发任务，Build/Run 时预创建 Worker goroutine，通过 channel 分发任务，Group Close/Wait 时本批 Worker 全部退出。完整 Wait 后且无活跃任务时，可检查 `Reset()` 的返回错误并开始下一批；Reset 会重新建立 worker 和批次状态。

业务回调调用 `runtime.Goexit` 时，当前任务记录 `async.ErrTaskGoexit` 及类型零值，计一次失败；任务的 Busy/Active/等待登记和派生 context 均收尾，FailFast 仍按原取消链执行。内部 worker 因 Goexit 退出后按原登记名额补充，其他已接受任务仍可执行。注入外部 Pool 时，Group 与外部 Pool 的任务登记分别完成，不因业务未返回而遗失 completion。Goexit 不包装为 PanicError、不自动重试；生产业务应正常返回 error，任意阻塞和进程退出不在此保证范围。公开示例见 [Example_poolAndGroupGoexit](tests/public_examples_test.go)。

> **⚠️ Group 默认就是并行模式，不存在 `.serial()` 串行模式。**
>
> Group 预创建的多个 Worker goroutine 会并发从 channel 中取任务执行，天然并行。如果需要串行执行，直接写普通 for 循环即可，不需要使用 Group。

**四种 Group 入口**：

| 入口 | 说明 | 终端方法 |
|------|------|----------|
| `async.Group[T]()` | 带返回值任务组 | `.Build()` 或 `.Run(fn)` |
| `async.GroupVoid()` | 无返回值任务组 | `.Build()` 或 `.Run(fn)` |
| `async.GroupSharded[T]()` | 分片 Group（详见 [Shard 文档](shard.md)） | `.Run(fn)` |
| `async.GroupMulti[T]()` | 水平分片任务组（MultiGroup 风格） | `.Run(fn)` |

> **⚠️ 两种终端方法：`.Build()` 和 `.Run(fn)`**
>
> - **`.Build()` → `*Group[T]`**：创建 Group 实例，用户自行管理生命周期；建议创建后立即 `defer g.Close()`，最终 `Wait()` 或 `Close()` 完成收尾。
> - **`.Run(fn func(ctx context.Context, g *Group[T]) error) error`**：创建 Group → 执行 fn → fn 返回后**自动 Close**
>
> 这与 PoolBuilder 不同，PoolBuilder 只有 `.Run(fn)` 没有 `.Build()`。
>
> **Run 回调类型**
>
> 使用公开的 `*async.GroupInstance[T]`、`*async.GroupNoResult` 和
> `*async.MultiGroup[T]` 声明回调参数。Build 模式也可使用 `:=` 推断类型。

> **⚠️ goroutine 复用规则**
>
> Group 在创建时预启动 `Worker` 个 goroutine，所有 `Go()` 提交的任务经由 channel 分发给这些 worker 并发执行。同一批任务共享这些 worker；Close/Wait 结束本批并退出 worker，继续提交须先成功 Reset。Pool 的 Wait 不结束 worker 生命周期，关闭由 Close 管理。根据批次边界、结果存储及资源预算选择组件，吞吐需要按业务负载测量，参见 [Pool](pool.md)。

> **⚠️ Go 后必须 Wait（Build 模式）**
>
> Build 模式应以 `Wait()` 或等价 `Close()` 收尾，使本批任务和 worker 退出；仅完成任务不会结束所有后台 worker。Run 模式自动 Close；需要取得结果或传播任务错误时，仍须在作用域回调内 Wait 并检查。

> **⚠️ Wait 与复用**
>
> 首次等待启动关闭流程；后续 `Wait()` 可等待同一流程并获取结果快照。开始等待后不可再 `Go()`；完成清理后如需复用，请使用 `Reset()`。

**与 Pool 的区别**：

| 特性 | Pool | Group |
|------|------|------|
| goroutine | 复用（常驻 worker） | 预创建，Close/Wait 退出 |
| 终端方法 | 仅 `.Run(fn)` | `.Build()` + `.Run(fn)` |
| 适用场景 | 长期运行的服务 | 一次性批量任务 |
| 生命周期 | Run 自动管理 | Build 手动管理 / Run 自动管理 |
| 流式消费 | `.Streaming(buf)` | `.Streaming(buf)` |

---

## 目录

- [GroupBuilder 链式方法速查表](#groupbuilder-链式方法速查表)
- [GroupNoResultBuilder 链式方法速查表](#groupnoresultbuilder-链式方法速查表)
- [创建：Build 模式](#创建build-模式)
- [创建：Run 模式](#创建run-模式)
- [任务提交（Group 实例方法）](#任务提交group-实例方法)
- [等待与结果（Group 实例方法）](#等待与结果group-实例方法)
- [状态查询（Group 实例方法）](#状态查询group-实例方法)
- [GroupVoid（无返回值任务组）](#groupvoid无返回值任务组)
- [流式结果消费](#流式结果消费)
- [外部 Pool 注入](#外部-pool-注入)
- [FailFast 快速失败模式](#failfast-快速失败模式)
- [AutoScale 自动扩缩容](#autoscale-自动扩缩容)
- [MultiGroup（水平分片任务组）](#multigroup水平分片任务组)
- [自定义日志](#自定义日志)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)
- [常见错误](#常见错误)
- [附录：Group 实例完整方法列表](#附录group-实例完整方法列表)

---

## GroupBuilder 链式方法速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.Group[T]()` | 创建带返回值任务组链式构建器 |
| `async.GroupVoid()` | 创建无返回值任务组链式构建器 |
| `async.GroupMulti[T]()` | 创建水平分片任务组链式构建器 |

### GroupBuilder 链式配置方法

> 所有链式方法均返回 `*GroupBuilder[T]`，支持连续调用。

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `context.Background()` |
| `.Worker(n)` | 设置最大并发数（≤0 在 Build 时使用 1） | `async.IO()`（`runtime.NumCPU()×2`） |
| `.DefaultWorker()` | 恢复为默认并发度 `async.IO()`（即 `runtime.NumCPU()×2`） | — |
| `.Timeout(d)` | 设置单任务超时 | `core.GetDefaultTimeout()`（默认 30s，可通过 `core.SetDefaultTimeout` 修改） |
| `.DefaultTimeout()` | 读取并保存调用时的全局默认超时；Build 时仅正值覆盖新 Group 的默认值 | — |
| `.SubmitTimeout(d)` | 设置获取并发槽位的提交超时 | 无限制（阻塞等待） |
| `.DefaultSubmitTimeout()` | 恢复默认：无提交超时限制 | — |
| `.FailFast()` | 启用 FailFast；Run 回调收到可取消上下文，应将其传给 Go | 关闭 |
| `.Streaming(buf)` | 设置流式结果 channel 缓冲大小，**buf>0 才启用**，0 不启用流式 | 0（禁用流式） |
| `.DefaultStreaming()` | 恢复默认：不启用流式 | — |
| `.ResultCallback(fn)` | 设置记录结果路径的同步回调，可能由 worker 或提交拒绝路径调用 | 无 |
| `.DefaultResultCallback()` | 清除结果回调 | — |
| `.AutoScale(config)` | 设置自动扩缩容配置；nil 清除 Builder 中的配置，构建时不启用 | 关闭 |
| `.DefaultAutoScale()` | 用默认配置启用（MinWorkers=`NumCPU×2`, MaxWorkers=`NumCPU×100`，阈值 0.7→0.2，CheckInterval 5s） | — |
| `.Pool(p)` | 注入外部协程池（用户自行管理生命周期） | 无（内部创建 worker） |
| `.DefaultPool()` | 清除外部池注入，恢复内部 worker 模式 | — |
| `.Logger(l)` | 为当前构建器及其后续实例设置日志器 | 继承全局配置 |
| `.DefaultLogger()` | 恢复默认日志 | — |

`ResultCallback`/实例 `WithResultCallback` 观察记录结果的路径，但最早的“已在等待/已经等待完成”提交预检查会直接追加错误而不通知回调。回调次数不能作为全部提交尝试数；始终检查每次 `Go` 返回的提交错误，并在最终等待后核对结果。回调可能并发执行，不在其中同步等待或关闭所属组。

### GroupBuilder 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Build()` | `*Group[T]` | 构建 Group 实例（需手动 `defer g.Close()`） |
| `.Run(fn)` | `error` | 构建 → 执行 → **自动 Close** |

> **⚠️ `.Run(fn)` 回调签名**
>
> `fn func(ctx context.Context, g *Group[T]) error`
>
> - `ctx`：builder 上通过 `.Context(ctx)` 设置的上下文（已注入 TraceID）
> - `g`：已创建的 `*async.GroupInstance[T]` 实例
> - fn 返回后，Group 会自动调用 `g.Close()`（等价于 `g.Wait()`），**无需用户手动 Close**
> - fn 的返回值会作为 `.Run(fn)` 的返回值透传出去

---

## GroupNoResultBuilder 链式方法速查表

### GroupNoResultBuilder 链式配置方法

> `async.GroupVoid()` 返回 `*GroupNoResultBuilder`，配置项少于 `GroupBuilder`，**没有 ResultCallback**。

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `context.Background()` |
| `.Worker(n)` | 设置最大并发数（≤0 在 Build 时使用 1） | `async.IO()`（`runtime.NumCPU()×2`） |
| `.DefaultWorker()` | 恢复为默认并发度 `async.IO()`（即 `runtime.NumCPU()×2`） | — |
| `.Timeout(d)` | 设置单任务超时 | `core.GetDefaultTimeout()`（默认 30s，可通过 `core.SetDefaultTimeout` 修改） |
| `.DefaultTimeout()` | 读取并保存调用时的全局默认超时；Build 时仅正值覆盖新 Group 的默认值 | — |
| `.SubmitTimeout(d)` | 设置获取并发槽位的提交超时 | 无限制（阻塞等待） |
| `.DefaultSubmitTimeout()` | 恢复默认：无提交超时限制 | — |
| `.FailFast()` | 启用 FailFast；Run 回调收到可取消上下文，应将其传给 Go | 关闭 |
| `.Streaming(buf)` | 设置流式结果 channel 缓冲大小，**buf>0 才启用**，0 不启用流式 | 0（禁用流式） |
| `.DefaultStreaming()` | 恢复默认：不启用流式 | — |
| `.AutoScale(config)` | 设置自动扩缩容配置；nil 清除 Builder 中的配置，构建时不启用 | 关闭 |
| `.DefaultAutoScale()` | 用默认配置启用（MinWorkers=`NumCPU×2`, MaxWorkers=`NumCPU×100`，阈值 0.7→0.2，CheckInterval 5s） | — |
| `.Pool(p)` | 注入外部协程池（`*pool.Pool[struct{}]`） | 无（内部创建 worker） |
| `.DefaultPool()` | 清除外部池注入，恢复内部 worker 模式 | — |
| `.Logger(l)` | 为当前构建器及其后续实例设置日志器 | 继承全局配置 |
| `.DefaultLogger()` | 恢复默认日志 | — |

### GroupNoResultBuilder 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Build()` | `*NoResult` | 构建 NoResult 实例（需手动 `defer nr.Close()`） |
| `.Run(fn)` | `error` | 构建 → 执行 → **自动 Close** |

> **⚠️ `.Run(fn)` 回调签名**
>
> `fn func(ctx context.Context, nr *NoResult) error`
>
> - `ctx`：builder 上通过 `.Context(ctx)` 设置的上下文（已注入 TraceID）
> - `nr`：已创建的 `*async.GroupNoResult` 实例
> - fn 返回后自动 `nr.Close()`，**无需用户手动 Close**

---

## 创建：Build 模式

Build 模式下用户自行管理 Group 的生命周期，建议创建后立即 `defer g.Close()`，保证提前返回时也能收尾。正常路径调用完整 `Wait()` 也会关闭内部 worker 和结果流。

### 基础用法

```go
g := async.Group[int]().Context(ctx).
    Worker(8).
    Build()
defer g.Close()

for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    })
}

results := g.Wait()
for _, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

### 完整配置 Build 示例

```go
g := async.Group[int]().Context(ctx).
    Worker(16).
    Timeout(5 * time.Second).
    FailFast().
    Streaming(1024).
    ResultCallback(func(r async.Result[int]) {
        if r.Ok() {
            log.Printf("完成: %d", r.Value)
        }
    }).
    Build()
defer g.Close()
```

> **⚠️ Build 模式下由调用方保证收尾**
>
> `Run` 在回调返回时关闭；`Build` 本身只创建实例。之后完整的 `Wait()` 或 `Close()` 都会等待内部 worker 退出并关闭结果流；两者都不调用才会留下 worker 等待。建议创建后立即 `defer g.Close()` 覆盖提前返回路径；`WaitContext`/`WaitTimeout` 提前返回后收尾仍可能继续，不能将其等同于收尾完成。

> **⚠️ 链式配置顺序**
>
> 不同配置字段可在 `.Build()` 或 `.Run(fn)` 之前设置；同一字段以后一次赋值为准，`Default*` 同样会覆盖已有值。注入外部 Pool 时部分配置的作用范围不同，见下文外部 Pool 章节。

---

## 创建：Run 模式

Run 模式下自动管理生命周期，fn 执行完毕后自动 `Close()`。**推荐日常使用此模式**。

### 基础用法

```go
err := async.Group[int]().Context(ctx).
    Worker(8).
    Timeout(5 * time.Second).
    Run(func(ctx context.Context, g *async.GroupInstance[int]) error {
        for _, item := range items {
            if err := g.Go(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            }); err != nil {
                return err
            }
        }
        g.Wait()
        return g.JoinErrors()
    })
```

> **⚠️ Run 模式下 fn 返回后 Group 已 Close**
>
> `Run(fn)` 内部流程：`Build() → fn(ctx, g) → g.Close()`。
> fn 返回后 Group 已不在可用状态，**不能**在 fn 外部继续使用 g。

### Run 模式在 fn 内调用 Wait

可以在 fn 内部显式调用 `g.Wait()` 获取结果：

```go
var allResults []async.Result[int]

err := async.Group[int]().Context(ctx).
    Worker(8).
    Run(func(ctx context.Context, g *async.GroupInstance[int]) error {
        for _, item := range items {
            if err := g.Go(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            }); err != nil {
                return err
            }
        }
        allResults = g.Wait()
        return g.JoinErrors()
    })

// fn 返回后 g 已自动 Close
for _, r := range allResults {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

上面两例都在回调内传播错误：`Go` 返回提交错误，`Wait` 等待后由 `JoinErrors` 汇总任务错误。`allResults` 只在全部提交成功后赋值；若中途提交失败，Run 仍负责等待已接受任务收尾，但本例直接返回提交错误。生产代码需要保留部分结果时，应在提交失败分支也等待并保存结果。

> **⚠️ fn 内调用 Wait 后不能再 Go**
>
> 首次等待后不能再提交任务；重复调用 `g.Wait()` 可获取结果快照，复用前须完成等待并调用 `Reset()`。

---

## 任务提交（Group 实例方法）

以下方法是 `*Group[T]` 实例上的方法，在 `.Build()` 或 `.Run(fn)` 获得 Group 实例后使用。

### Go

阻塞提交任务，等待有空闲并发槽位。返回 `error` 表示**提交**失败（非任务执行失败）：

```go
err := g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchCount(ctx), nil
})
```

**返回值可能为**：

| 错误 | 说明 |
|------|------|
| `nil` | 提交成功 |
| `ErrGroupWaited` | Wait 之后调用 Go，任务被丢弃 |
| `ErrGroupWaiting` | Wait 执行期间调用 Go |
| `ErrSubmitTimeout` | 等待并发槽位超时（需设置 `.SubmitTimeout(d)`） |
| `context.Canceled` | ctx 已被取消 |
| `context.DeadlineExceeded` | ctx 已超时 |

### GoWithTimeout

提交任务并**单独设置该任务的超时**，覆盖 Group 级别的 `.Timeout(d)`。正数从回调开始执行计时；非正值清除本次任务的 Group 期限，父 context 的期限仍生效。注入外部池时，其原有任务超时还可能限制执行。这与 Pool 的 `SubmitWithCompletionTimeout(..., 0)` 继承池超时不同：

```go
g.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) (string, error) {
    return slowWork(ctx)
})
```

### GoAt

使用互不重复的非负索引提交任务，等待后通过 `results[index]` 取得对应结果，并检查提交及执行错误：

```go
for i, url := range urls {
    g.GoAt(i, ctx, func(ctx context.Context) (string, error) {
        return fetchURL(ctx, url)
    })
}
// results[i] 一定对应 urls[i]
```

> **⚠️ GoAt 索引可以无序调用**
>
> 可以先 `GoAt(5, ...)` 再 `GoAt(0, ...)`，结果数组会自动填充到正确位置。未提交的索引位置保持零值且 `Occupied=false`。
> 负索引会返回 `ErrInvalidIndex`，不会提交任务。
> 稀疏索引会分配到最大索引加一的长度；重复索引可能使已有结果移动到其他槽位，不能用它实现任务替换。生产代码优先使用唯一连续索引。

### GoAtWithTimeout

指定索引 + 单独设置超时；负索引返回 `ErrInvalidIndex`，不会提交任务：

```go
// 第 3 个任务只需 500ms 超时
g.GoAtWithTimeout(2, ctx, 500*time.Millisecond, func(ctx context.Context) (string, error) {
    return quickAPI(ctx)
})
```

---

## 等待与结果（Group 实例方法）

### Wait

等待所有任务完成并返回结果切片。普通 `Go` 的结果在任务结束时写入，不能依赖提交顺序；需要固定位置时使用不同的 `GoAt` 索引。开始等待后不能再提交任务；等待完全结束后可调用 `Reset()` 复用：

```go
results := g.Wait()
for _, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

### WaitTimeout

最多等待 d 时长，超时返回部分结果。`ok=false` 表示超时未全部完成：

```go
results, ok := g.WaitTimeout(5 * time.Second)
if !ok {
    log.Println("部分任务未完成")
}
// results 是当时的结果切片快照；稀疏 GoAt 的空槽需要检查 Occupied
```

> **⚠️ 超时后请求取消已登记任务**
>
> `WaitTimeout` 超时后会请求取消任务并返回当前结果快照，后台继续等待已接收的任务和 worker 退出。任务必须配合上下文取消；如需最终结果或在 `Reset` 前确认清理完成，应随后调用 `Wait()`。

### WaitContext

等待所有任务完成或 ctx 取消。`ok=false` 表示 ctx 提前取消：

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
results, ok := g.WaitContext(ctx)
```

> **⚠️ ctx 取消后请求取消已登记任务**
>
> `WaitContext` 在 ctx 取消后会请求取消已登记任务并返回当前结果快照；后台清理流程继续等待已接收任务结束，再关闭 task channel 并等待 worker 退出。取消只是通知，任务函数必须响应 context 才能及时结束。

结果写入与任务收尾分属不同阶段：Group 先保存结果、尝试发送结果流，再同步调用 `WithResultCallback`。回调仍在运行时，`WaitTimeout`／`WaitContext` 可以返回包含该结果的快照和 `ok=false`；此时 `Active` 尚未归零，`Reset` 仍会返回错误，结果流也尚未关闭。成功任务的 `OkCount` 在结果回调返回后增加，因此不能把快照槽位数、回调次数或 `OkCount` 当作同一个完成标志。该顺序也适用于注入外部 Pool 的正常执行路径。

返回的结果切片是浅拷贝：修改其元素槽位不会改写 Group 保存的槽位，但值中的 map、slice 或指针仍可能引用相同数据。生产中应让结果回调快速返回，超时后按需要调用最终 `Wait()`，再读取完整统计或 `Reset()`；不得在所属回调中等待当前组。可执行边界示例见 [Example_groupWaitDuringResultCallback](tests/public_examples_test.go)。

### 调用者主动取消模式

结合 Builder 的 `Context` 和 `WaitContext`，调用方可以在外部主动取消任务：

```go
// ① 由调用方掌控取消时机
parentCtx, cancel := context.WithCancel(context.Background())
defer cancel()

// ② 链式创建 Group：绑定 parentCtx
g := async.Group[int]().Context(parentCtx).
    Worker(6).
    Timeout(5 * time.Second).
    Build()
defer g.Close()

// ③ 使用 parentCtx 提交任务
for i := 0; i < 5; i++ {
    g.Go(parentCtx, func(ctx context.Context) (int, error) {
        select {
        case <-ctx.Done():
            return 0, ctx.Err()     // 收到取消信号，尽早返回
        case <-time.After(10 * time.Second):
            return 42, nil
        }
    })
}

// ④ 另一 goroutine 主动取消
go func() {
    time.Sleep(100 * time.Millisecond)
    cancel()  // ← 调用者主动 cancel，取消链传播到所有 taskCtx
}()

// ⑤ WaitContext 兜底（防止 cancel 传播异常导致永久阻塞）
waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
defer waitCancel()
results, ok := g.WaitContext(waitCtx)
// cancel() 触发 → 任务 ctx.Done() → 任务返回 → WaitContext 正常结束
```

> **⚠️ Builder 与实例方法**
>
> Builder 使用 `Context` 和 `Timeout`；`WithContext`、`WithTimeout` 和 `WithStreaming` 属于 Build 后的 Group 实例方法。实例的 `WithContext(ctx)` 返回 Group 和派生 context：
>
> ```go
> // ✅ 正确：需要 ctx 的方法放在链最后
> g := async.Group[int]().Worker(6).Build()
> g, taskCtx := g.WithContext(parentCtx)
>
> // ✅ 纯链式（不需要 ctx 的 builder 方法）：可以无限链
> g := async.Group[int]().Worker(6).Timeout(5*time.Second).Streaming(1024).Build()
> ```
>
> Build 创建的 Group 应在使用结束后关闭。

### 超时自动取消模式

通过 `WaitTimeout` 或 `context.WithTimeout` + `WaitContext` 实现超时自动取消：

```go
// 方式一：WaitTimeout 直接传超时
results, ok := g.WaitTimeout(100 * time.Millisecond)

// 方式二：WaitContext + context.WithTimeout
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()
results, ok = g.WaitContext(ctx)
```

> 两种方式等效：超时后取消信号都会传播到所有正在执行的任务。

### Close

关闭任务组，等待所有已提交任务完成并清理资源。等价于 `g.Wait()` 但不返回结果：

```go
g.Close()
```

> **⚠️ Close() 等价于 Wait()**
>
> `Close()` 内部调用 `Wait()`，所以 Close 之后也不能再 Go。

### Values

获取所有成功任务的结果值（需在 Wait 之后调用）：

`Values` 只包含已占用且无错误的槽位。等待前也可调用，但只能看到当时的部分结果；使用 `GoAt` 的稀疏索引时，不会把空槽当成成功值。

```go
g.Wait()
values := g.Values() // []T，只包含成功的值
for _, v := range values {
    fmt.Println(v)
}
```

### Errors / FirstError / JoinErrors

这些方法在等待前返回当前快照，完整结果应先等待结束。`FirstError` 取结果切片顺序中的第一个错误，并非保证最早发生的错误；`JoinErrors` 使用 `errors.Join` 合并当前错误。

```go
g.Wait()

// 获取所有错误
for _, err := range g.Errors() {
    log.Println(err)
}

// 获取第一个错误
if err := g.FirstError(); err != nil {
    log.Printf("首个错误: %v", err)
}

// 使用 errors.Join 合并所有错误
if err := g.JoinErrors(); err != nil {
    log.Printf("有任务失败: %v", err)
}
```

---

## 状态查询（Group 实例方法）

计数查询可在 Wait 前或 Wait 后调用。`Stats()` 汇总多个字段，结果不是同一时刻的原子快照；其超时字段读取与 `WithTimeout` 的并发修改需要由调用方同步。

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Worker()` | `int` | 当前最大并发数（自动扩缩容时可能动态变化） |
| `Active()` | `int` | 已登记但未结束的任务数，包含等待入队、排队和执行中的任务 |
| `Busy()` | `int` | 当前正在执行的任务数，不包含排队任务 |
| `TotalCount()` | `int64` | 记录的提交次数，含运行中任务和已记录的拒绝；无效索引不计数 |
| `FailCount()` | `int64` | 失败任务数 |
| `OkCount()` | `int64` | 已成功完成的任务数，不含运行中任务 |
| `HasError()` | `bool` | 是否有任务失败 |
| `Stats()` | `GroupStats` | 获取运行时统计快照 |

> **注意**：`OkCount()` 只统计已经完成且无错误的任务；运行中、失败、取消、提交拒绝和 panic 任务都不会计入。需要最终值时请先调用 `Wait()`，复用实例时请在 `Reset()` 后重新读取计数。

```go
results := g.Wait()
fmt.Printf("本批完成: total=%d ok=%d fail=%d\n", g.TotalCount(), g.OkCount(), g.FailCount())
```

```go
stats := g.Stats()
fmt.Printf("活跃: %d, 繁忙: %d, 成功: %d, 失败: %d\n",
    stats.Active, stats.Busy, stats.OkTask, stats.FailTask)
```

### Reset

重置 Group 状态以便复用（保留 timeout、submitTimeout、concurrency 等配置）：

```go
results := g.Wait()           // 第一批完成
if _, err := g.Reset(); err != nil {
    return err
}
for _, item := range batch2 {
    if err := g.Go(ctx, func(ctx context.Context) (T, error) {
        return process(ctx, item)
    }); err != nil {
        return err
    }
}
results2 := g.Wait()          // 第二批结果
```

> **⚠️ Reset 必须在 Wait 之后调用**
>
> 源码中如果在 Wait 之前调用 Reset 会返回错误 `"async: Group.Reset called before Wait, concurrent unsafe"`。

> Reset 会清除 FailFast 配置；复用时如需快速失败，应重新调用 `WithFailFast` 并使用返回的 context。

> **⚠️ Reset 后自动扩缩容会被停止**
>
> 如果启用了 AutoScale，Reset 会停止自动扩缩容后台 goroutine。如需继续使用，Reset 后需重新调用 `EnableAutoScale()`。

---

## GroupVoid（无返回值任务组）

适合只关心 error 不关心返回值的批量操作。`async.GroupVoid()` 返回 `*GroupNoResultBuilder`，`.Build()` 返回 `*NoResult`。

### Build 模式

```go
nr := async.GroupVoid().Context(ctx).
    Worker(16).
    Build()
defer nr.Close()

for _, record := range records {
    if err := nr.Go(ctx, func(ctx context.Context) error {
        return db.Insert(ctx, record)
    }); err != nil {
        log.Printf("提交失败: %v", err)
        break
    }
}
nr.Wait()

if nr.HasError() {
    log.Printf("失败 %d 个任务", nr.FailCount())
}
```

### Run 模式

```go
err := async.GroupVoid().Context(ctx).
    Worker(16).
    Run(func(ctx context.Context, nr *async.GroupNoResult) error {
        for _, record := range records {
            if err := nr.Go(ctx, func(ctx context.Context) error {
                return db.Insert(ctx, record)
            }); err != nil {
                return err
            }
        }
        nr.Wait()
        return nr.JoinErrors()
    })
```

`Run` 返回作用域回调的错误，自动 Close 只负责收尾；不会自动把各任务错误汇总成返回错误。示例先检查 `Go` 的提交错误，全部提交后用无返回值的 `Wait()` 等待，再用 `JoinErrors()` 传播业务错误及 panic 错误。生产代码应在提交失败时决定是否汇总已接受任务的结果；此处直接返回提交错误，仍由 Run 的 defer Close 等待它们收尾。

### NoResult 聚合与跳过项补齐

`async.BuildAggregateNoResult(nr)` 和 `async.FillNoResultSkipped(nr, total)` 从根包导出，库内批处理适配也使用其对应实现。两者要求 `nr` 非 nil，已完成最终 `Wait()`，且无任务或结果回调仍在运行。使用上面的 `GroupVoid()` 入口创建 `*async.GroupNoResult`，再按需要聚合或补齐。

聚合会复制当前结果槽位，只对 `Occupied=true` 的项统计总数及失败数，并按槽位顺序选择首错；它不会自行等待或合并外部池的其他任务。稀疏索引的结果长度可能大于总数。修改返回切片槽位不会改写组内槽位，error 接口引用的对象仍可能共享。

补齐只在 `total` 大于当前 `TotalCount()` 时增长：先按索引升序把空槽填成 `ErrSkipped`，再追加占位，每个补项增加总数和失败数。非正目标（包括最小 int）或较小目标均无操作；重复相同目标不会继续增加。补齐会改变随后查询到的首错，例如先前索引 5 的业务失败之前，可能新增索引 0 的 `ErrSkipped`。它不会执行任务、补发结果回调或重开已关闭的结果流。

生产中应根据真实输入规模和内存预算确定目标；正数增长没有上限，每个新增占位需要时间和存储。需要补齐后的错误时重新聚合，不能只统计先前已接收的流事件。调用片段、参数和返回值见 [方法参考](api/async.md#fillnoresultskipped)，极端负目标和首错变化见 [可执行示例](tests/public_examples_test.go)。

### NoResult 实例方法

`NoResult` 是基于 `Group[struct{}]` 的定义类型（`type NoResult Group[struct{}]`），方法与 `Group[T]` 对应但适配无返回值场景。

| 方法 | 签名 | 说明 |
|------|------|------|
| `Go` | `Go(ctx, fn func(ctx) error) error` | 提交无返回值任务 |
| `GoWithTimeout` | `GoWithTimeout(ctx, timeout, fn func(ctx) error) error` | 带超时提交 |
| `GoAt` | `GoAt(index, ctx, fn func(ctx) error) error` | 提交到指定索引 |
| `GoAtWithTimeout` | `GoAtWithTimeout(index, ctx, timeout, fn func(ctx) error) error` | 指定索引 + 带超时 |
| `Wait` | `Wait()` | **无返回值（void）** |
| `WaitTimeout` | `WaitTimeout(d) (int64, bool)` | 返回当前结果切片长度和是否完成等待 |
| `WaitContext` | `WaitContext(ctx) (int64, bool)` | 返回当前结果切片长度和是否完成等待 |
| `FailCount` | `FailCount() int64` | 失败任务数 |
| `OkCount` | `OkCount() int64` | 成功任务数 |
| `HasError` | `HasError() bool` | 是否有失败 |
| `TotalCount` | `TotalCount() int64` | 总任务数 |
| `Worker` | `Worker() int` | 最大并发数 |
| `Active` | `Active() int` | 已登记但未结束的任务数，包含排队任务 |
| `Busy` | `Busy() int` | 正在执行的任务数 |
| `Stats` | `Stats() GroupStats` | 统计快照 |
| `Errors` | `Errors() []error` | 所有错误 |
| `FirstError` | `FirstError() error` | 第一个错误 |
| `JoinErrors` | `JoinErrors() error` | 合并所有错误 |
| `Reset` | `Reset() (*NoResult, error)` | 重置状态 |
| `Close` | `Close()` | 关闭 |
| `StreamResults` | `StreamResults() <-chan Result[struct{}]` | 流式结果 channel |
| `WithStreaming` | `WithStreaming(bufSize int) *NoResult` | 启用流式消费 |
| `WithResultCallback` | `WithResultCallback(fn func(Result[struct{}])) *NoResult` | 设置结果回调 |
| `WithTimeout` | `WithTimeout(d) *NoResult` | 设置单任务超时 |
| `WithSubmitTimeout` | `WithSubmitTimeout(d) *NoResult` | 设置提交超时 |
| `WithTraceID` | `WithTraceID(ctx) (*NoResult, context.Context)` | 注入 TraceID |
| `WithContext` | `WithContext(ctx) (*NoResult, context.Context)` | 绑定 Context |
| `WithFailFast` | `WithFailFast(ctx) (*NoResult, context.Context)` | 启用 FailFast |
| `WithFFCtx` | `WithFFCtx(ctx) (*NoResult, context.Context)` | 同 WithFailFast |
| `EnableAutoScale` | `EnableAutoScale(config *AutoScaleConfig)` | 启用自动扩缩容 |
| `DisableAutoScale` | `DisableAutoScale()` | 停止自动扩缩容 |
| `IsAutoScaleEnabled` | `IsAutoScaleEnabled() bool` | 是否已启用自动扩缩容 |
| 配置组合 | `WithFFTraceID` 等 | 12 个组合方法（同 Group[T]） |

`WaitTimeout` / `WaitContext` 的第一个返回值是结果切片长度，不是严格的已完成任务数：使用稀疏 `GoAt` 索引时可能包含未占用槽位。需要统计已完成任务时，等待结束后结合 `OkCount`、`FailCount` 或 `BuildAggregateNoResult` 检查。

`BuildAggregateNoResult(nr)` 复制结果切片，按 `Occupied` 统计总数和失败数，首错按槽位顺序选取；它不执行等待。`FillNoResultSkipped(nr, total)` 在最终等待结束后，把 `TotalCount` 补到目标值：优先填空槽，再追加 `ErrSkipped`，同时增加失败计数。它不补发结果回调或结果流；目标不大于现有总数时无操作。生产代码应在确认任务已全部收尾后调用，并单独核对稀疏槽位长度与实际任务数；可执行示例见 [public_examples_test.go](tests/public_examples_test.go)。

> **⚠️ `NoResult.Wait()` 无返回值（void）**
>
> `NoResult.Wait()` 签名为 `func (nr *NoResult) Wait()`，**没有任何返回值**。与 `Group[T].Wait()` 返回 `[]Result[T]` 不同。
>
> 判断执行结果应使用 `HasError()`、`FailCount()`、`Errors()` 等方法。

> **⚠️ NoResult.Go() 的任务函数只返回 error**
>
> `nr.Go(ctx, func(ctx context.Context) error { ... })`，不需要返回具体值。

### NoResult 也支持流式消费

```go
nr := async.GroupVoid().Context(ctx).
    Worker(4).
    Streaming(1024).
    Build()
defer nr.Close()

consumed := make(chan struct{})
go func() {
    defer close(consumed)
    for r := range nr.StreamResults() {
        if r.Err != nil {
            log.Printf("任务失败: %v", r.Err)
        }
    }
}()

for _, item := range items {
    nr.Go(ctx, func(ctx context.Context) error {
        return process(ctx, item)
    })
}
nr.Wait()
<-consumed
```

---

## 流式结果消费

### 方式一：StreamResults channel

启用 `.Streaming(buf)` 后，通过 `g.StreamResults()` 获取只读 channel，实时消费结果。以下方式一和方式二放在返回 `error` 的业务函数内，并导入标准库 `errors`；两者都检查提交错误，在完整等待后汇总任务错误：

```go
g := async.Group[int]().Context(ctx).
    Worker(4).
    Streaming(1024).
    Build()
defer g.Close()

ch := g.StreamResults()
consumed := make(chan struct{})

go func() {
    defer close(consumed)
    for r := range ch {
        if r.Ok() {
            fmt.Println(r.Value)
        }
    }
}()

var submitErr error
for _, item := range items {
    if err := g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    }); err != nil {
        submitErr = err
        break
    }
}
g.Wait()
<-consumed // 等待读取和处理已发送的结果
return errors.Join(submitErr, g.JoinErrors())
```

> **⚠️ Streaming channel 在 Wait() 后自动关闭**
>
> `StreamResults()` 返回的 channel 会在 `Wait()`/`Close()` 内部调用 `drainStreaming()` 时自动关闭。所以在 `Wait()` 之后读取 channel 的 goroutine 会自动退出。
>
> **结果可能丢失**：channel 满时结果暂存在溢出缓冲区，但 `Wait()` 结束时只会将当时能写入 channel 的溢出结果送出，其余丢弃；即使之后继续读取也无法恢复。流不能保证完整性；需要完整结果时使用 `Wait()` 的返回值，或在提交前配置 `ResultCallback` 自行可靠保存。
> 溢出缓冲区在等待结束前会随未发送结果增长，持续慢消费可能增加内存占用。通道关闭不代表消费者已处理完缓冲数据；需要等待消费者自身退出。

### 方式二：ResultCallback 回调

记录结果的路径**同步**调用回调函数：正常执行由 Group worker 调用，注入外部 Pool 时可在外部 Pool worker 中调用；提交取消或拒绝也可能在提交调用者内通知。最早的 waited/waiting 预检查只追加错误，不通知回调，因此回调次数不等于全部提交次数：

```go
g := async.Group[int]().Context(ctx).
    Worker(4).
    ResultCallback(func(r async.Result[int]) {
        if r.Ok() {
            log.Printf("实时收到: %d", r.Value)
        }
    }).
    Build()
defer g.Close()

var submitErr error
for _, item := range items {
    if err := g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    }); err != nil {
        submitErr = err
        break
    }
}
g.Wait()
return errors.Join(submitErr, g.JoinErrors())
```

> **⚠️ ResultCallback 同步执行**
> 回调 panic 会被恢复并记录日志，不会替换任务结果或重复调用回调。
>
> 回调函数应尽量轻量，避免阻塞 worker goroutine 影响整体吞吐。如需重量级处理，应在回调内部将结果发送到另一个 channel 异步处理。

### 方式三：Streaming + ResultCallback 并用

两者可以同时使用，互不影响：

```go
g := async.Group[int]().Context(ctx).
    Worker(4).
    Streaming(1024).
    ResultCallback(func(r async.Result[int]) {
        // 轻量统计
        atomic.AddInt64(&counter, 1)
    }).
    Build()
```

> **⚠️ `GroupNoResultBuilder` 没有 `ResultCallback`**
>
> 无返回值任务组的 Builder 不支持 `.ResultCallback()`。如果需要回调，可以在 Build 后通过 `nr.WithResultCallback(fn)` 设置，或者在 Run 模式 fn 内部调用 `nr.WithResultCallback(fn)`。

---

## 外部 Pool 注入

将任务的**实际执行**交给已有的 Pool，Group 本身只负责编排和结果收集：

```go
err := async.Pool[int]().Context(ctx).Worker(8).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
    g := async.Group[int]().Context(ctx).Pool(p).Build()
    defer g.Close()
    for _, item := range items {
        item := item
        if err := g.Go(ctx, func(ctx context.Context) (int, error) {
            return process(ctx, item)
        }); err != nil {
            return err
        }
    }
    results := g.Wait()
    return handleResults(results)
})
```

> **⚠️ 外部 Pool 需自行管理生命周期**
>
> `.Pool(p)` 注入后，Group 不会自动 Close 该 Pool。上例的 Pool 由外层 `Run` 管理，Group 的使用必须在回调返回前结束。

> **⚠️ Pool 注入后并发度由 Pool 控制**
>
> 注入外部 Pool 后，Group 内部的 concurrency worker 仅做任务分发，实际并发度由外部 Pool 的 worker 数量控制。

注入后存在两层队列和两层执行期限：

| 设置 | 实际作用范围 | 生产建议 |
|---|---|---|
| Group `WithSubmitTimeout(d)` | 调用方等待进入 Group 队列；不限制 Group worker 向外部 Pool 提交时的等待 | 对外部 Pool 另设提交期限，并传递有整体期限的父 context |
| Pool `WithSubmitTimeout(d)` | Group worker 分发到 Pool 时，等待其名额和队列的时间 | Pool 拒绝会成为 Group 的最终错误；最初 `g.Go` 仍可能已返回 nil |
| Group `WithTimeout(d)`／`GoWithTimeout` | 业务回调开始后的子 context 期限 | 单任务非正值清除 Group 期限，不能清除 Pool 期限 |
| Pool `WithTimeout(d)` | Pool worker 开始执行包装回调后的 context 期限 | 与 Group 及父期限取较早者，不包含外部 Pool 排队时间 |

期限仅发出取消信号。回调忽略取消而返回 nil 时仍保留成功结果；需要期限失败应自行返回 `ctx.Err()`。等待入 Group、分发入 Pool 及 Pool 队列的总时间应由任务父 context 限制，不能把任一执行期限当作端到端预算。

**业务结果必须从 Group 检查。** 包装回调在 Group 内恢复业务 panic、保存实际值及错误，随后向 Pool 返回类型零值和 nil。因此 Group 的业务失败、panic 或回调期限错误可能对应 Pool 的成功计数，也不会自动触发 Pool 自己的 FailFast；Group 自身的 FailFast 仍按其规则取消组 context。若包装回调在业务执行前被 Pool 拒绝，则两层都记录拒绝错误，但不触发 Group 的业务 FailFast。可执行对照见 [Example_groupExternalPoolResults](tests/public_examples_test.go)。

`g.Wait()` 只等待本组登记的任务，不等待外部 Pool 中其他所有者的任务；它不调用 Pool.Wait、Reset 或 Close。组等待结束后外部 Pool 仍可接受任务，Pool 的最终结果和生命周期由其所有者统一管理，不能在其他组仍提交时提前 Wait/Reset。

---

## FailFast 快速失败模式

任务出错时请求取消其他任务；回调须响应 context 才能退出：

```go
var results []async.Result[int]
err := async.Group[int]().Context(ctx).
    Worker(8).
    FailFast().
    Run(func(ctx context.Context, g *async.GroupInstance[int]) error {
        var submitErr error
        for _, item := range items {
            if err := g.Go(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            }); err != nil {
                submitErr = err
                break
            }
        }
        results = g.Wait()
        return errors.Join(submitErr, g.JoinErrors())
    })
// err 是回调显式汇总的提交/任务错误，Run 本身不自动汇总。
// results 保留所有已记录结果，包括取消和拒绝；未尝试提交的项没有结果。
_ = err
_ = results
```

示例使用标准库 `errors`。即使中途停止提交，也先 `Wait` 再赋值 `results`，避免提前返回而丢掉已接收任务的部分结果；已成功的业务副作用不会回滚。结果顺序依完成及记录顺序，不等于输入顺序。

> **⚠️ FailFast 请求协作取消**
>
> 启用 FailFast 后，一旦任一任务返回 error（非 nil），Group 会取消其 FailFast 上下文。提交任务时应使用 `Run` 回调提供的上下文；使用 `Build` 时，可调用 `g, ffCtx := g.WithFailFast(ctx)`，再将 `ffCtx` 传给 `Go`。其他任务应检查 `ctx.Err()` 并及时退出。
> 外部 Pool 在任务函数执行前拒绝或丢弃任务时，Group 记录错误结果，但该情况不会触发 FailFast 取消。
> FailFast 只请求取消，不保证正在运行的业务停止；若业务忽略 context，Wait 仍会等待它结束，已完成的副作用不会回滚。提交在取消后也可能被拒绝，检查每次 Go 的返回值。

> **⚠️ panic 也会触发 FailFast**
>
> 如果任务 panic，Group 会捕获 panic 并记录错误，同时触发 FailFast 取消其他任务（如果 FailFast 已启用）。
>
> Run 只返回作用域回调显式返回的 error，不会自动汇总任务结果。需要完整业务错误时，在 Wait 后用 `JoinErrors`；Group 保留已记录的结果，按输入规模预算其内存。

---

## AutoScale 自动扩缩容

基于 `busy/concurrency` 比率周期性检测负载，自动调整并发度：

```go
g := async.Group[int]().Context(ctx).
    Worker(4).
    DefaultAutoScale(). // 使用默认配置
    Build()
defer g.Close()

for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    })
}
g.Wait()
// 执行期间并发度会根据负载自动调整
```

**自定义配置**：

```go
g := async.Group[int]().Context(ctx).
    Worker(4).
    AutoScale(&async.AutoScaleConfig{
        MinWorkers:       2,
        MaxWorkers:       500,
        CheckInterval:    3 * time.Second,
        ScaleUpFactor:    1.5,
        ScaleDownFactor:  0.75,
        ScaleUpThreshold: 0.7,
        ScaleDownThreshold: 0.3,
        ScaleUpChecks:    3,
        ScaleDownChecks:  5,
    }).
    Build()
```

> **⚠️ AutoScale 扩容/缩容逻辑**
>
> - **扩容**：`busy/concurrency > ScaleUpThreshold` 持续 `ScaleUpChecks` 次 → 并发度 × `ScaleUpFactor`（上限 `MaxWorkers`）
> - **缩容**：`busy/concurrency < ScaleDownThreshold` 持续 `ScaleDownChecks` 次 → 并发度 × `ScaleDownFactor`（下限 `MinWorkers`）
> - 有滞后保护防止抖动：需要连续 N 次检测满足条件才会触发调整

达到检测次数后，扩容目标按乘积向零取整，若没有增长则至少加 1，再受 `MaxWorkers` 约束；缩容目标若没有下降则至少减 1，再受 `MinWorkers` 约束。浮点乘积在转为整数之前先按对应边界截定，有限大系数不会因转换溢出而反向调整。生产配置仍应采用合理的有限正数系数，并校验 `MinWorkers <= MaxWorkers`、阈值关系及资源预算；`Normalize()` 只填补非正值，不校验所有关系或 NaN/Inf。

阈值等号和中间区间都会清零扩容/缩容连续计数；达到边界时即使不能调整，这次确认计数也会清零。倍数计算截断为整数后若不能向目标方向改变，则尝试增减至少 1，再应用边界。Normalize 不是配置校验器，生产约束见 [AutoScale 配置边界](config.md#autoscaleconfig-字段)。

Enable 保持初始 Worker，不立即调整到范围内；生产中应在配置范围内设置初始值。Disable 除了停止监视，还把目标并发调到 MinWorkers，可能增加或减少 worker；目标数变更不表示所有旧 worker 已退出，在途回调不会被强制中断。固定可执行对照见 [Example_autoScaleTargetLifecycle](tests/public_examples_test.go)。Group 自动调整不改变保存的 Min/Max，Pool 经通用 Resize 扩展边界的规则不同。

> **⚠️ Wait 后自动停止扩缩容**
>
> 开始等待时会停止 AutoScale；`Wait()` 返回后 `IsAutoScaleEnabled()` 为 false。等待期间或等待完成后不能重新启用，Reset 后可再次启用。重复启用使用首次启用的配置。

> **⚠️ Reset 后需重新启用 AutoScale**
>
> `g.Reset()` 会停止 AutoScale。如需在 Reset 后继续使用，需重新调用 `g.EnableAutoScale(config)`。

---

## MultiGroup（水平分片任务组）

单 Group 锁竞争瓶颈时，通过水平分片突破。`async.GroupMulti[T]()` 返回 `*MultiGroupBuilder[T]`，**只有 `.Run(fn)` 终端方法**。

### MultiGroupBuilder 链式方法

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文 | `context.Background()` |
| `.Shards(n)` | 分片数 | 1（单分片） |
| `.Worker(n)` | 每分片并发数 | 1（≤0 自动修正为 1） |
| `.Timeout(d)` | 单任务超时 | `core.GetDefaultTimeout()`（默认 30s，可全局修改） |
| `.SubmitTimeout(d)` | 提交超时 | 无限制 |
| `.FailFast()` | 启用 FailFast；Run 回调收到跨分片共享的可取消上下文 | 关闭 |
| `.Logger(l)` | 设置当前构建器的日志器（仅局部生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志（调用 `清除当前覆盖，继承全局日志器`） | — |
| `.Run(fn)` | **终端**：创建 → 执行 → 自动 Close | — |

### 基础用法

```go
err := async.GroupMulti[int]().Context(ctx).
    Shards(8).
    Worker(50).
    Run(func(ctx context.Context, mg *async.MultiGroup[int]) error {
        var submitErr error
        for i := 0; i < 10000; i++ {
            idx := i
            if err := mg.Go(ctx, func(ctx context.Context) (int, error) {
                return compute(ctx, idx)
            }); err != nil {
                submitErr = err
                break
            }
        }
        results := mg.Wait()
        errs := []error{submitErr}
        for _, result := range results {
            errs = append(errs, result.Err)
        }
        return errors.Join(errs...)
    })
```

> **⚠️ 总并发量 = Shards × Worker**
>
> `Shards(8).Worker(50)` 意味着 8 个分片 × 每分片 50 并发 = **400 并发总量**。

### MultiGroup 实例方法

| 方法 | 说明 |
|------|------|
| `Go(ctx, fn)` | round-robin 分发到某个分片 |
| `GoKeyed(key, ctx, fn)` | `key` 为 `uint64`，按 `key % 分片数` 固定路由，不额外哈希 |
| `Wait()` | 等待所有分片完成，合并结果 |
| `WaitTimeout(d)` | 带超时等待，返回 `(results, ok)` |
| `WaitContext(ctx)` | 带 context 等待，返回 `(results, ok)` |
| `Close()` | 关闭所有分片 |
| `ShardCount()` | 返回分片数 |
| `GetShard(idx)` | 获取指定分片的 `*Group[T]` 实例 |
| `TotalWorker()` | 所有分片的总并发数 |
| `TotalActive()` | 所有分片活跃任务总数 |
| `TotalBusy()` | 所有分片忙碌任务总数 |
| `TotalTaskCount()` | 各片进入提交预检查的尝试数之和，包含预检查拒绝；不是成功入队数 |
| `TotalFailCount()` | 所有分片失败任务总数 |
| `TotalOkCount()` | 所有分片成功任务总数 |
| `Errors()` | 所有分片所有错误 |
| `Values()` | 所有分片成功结果值 |
| `StreamResults()` | 合并所有分片流式结果的 channel |
| `WithTimeout(d)` | 为所有分片设置任务超时 |
| `WithStreaming(bufSize)` | 为所有分片启用流式结果消费 |
| `WithResultCallback(fn)` | 为所有分片设置结果回调 |

> 获取合并流后应持续读取直到通道关闭；若停止消费且合并缓冲填满，分片转发协程会阻塞。慢消费还可能导致各分片丢弃部分流式结果，应以等待返回的结果为准。

> **建议**：`MultiGroup` 的 `TotalOkCount()` 会汇总全部分片；在所有分片 `Wait()` 完成后读取，才能得到稳定的最终统计。

> **⚠️ MultiGroup 的批次生命周期**
>
> 每个分片内部预创建 Worker goroutine，Run 返回后所有分片 Close、所有 Worker 退出。MultiGroup 没有整体 Reset 方法；单片 Group 的 Reset 能力不代表可以在结束的 MultiGroup 作用域中继续提交。持续任务是否采用 MultiPool，应根据等待边界和资源预算决定。
>
> **💡 取消传播**：`MultiGroup.WaitTimeout` → `Group.WaitContext` → `cancelAll()` → 每个分片的 worker 任务均收到取消信号。调用方主动取消模式与 Group 一致，详见 [调用者主动取消模式](#调用者主动取消模式)。
>
> 通过 `WithContext`/`WithFailFast` 创建后再 `Shard`，或通过启用 FailFast 的 MultiGroupBuilder 创建时，各片持有独立子 context。正常等待单片不取消仍在执行的其他片；整体 `Wait`/`Close` 完成后释放共享层。`WaitContext`/`WaitTimeout` 返回前也释放共享层，即使返回 `false`；忽略取消的业务仍可继续，随后调用 `Wait` 获取最终结果。父 context 不会被反向取消，整体等待后的共享 `ctx.Err()` 也不能用来判断业务是否失败，应检查提交和结果错误。

> **⚠️ Wait() 结果不保证全局顺序**
>
> `mg.Wait()` 按分片合并结果，不保证任务的提交顺序。即使使用 `GoKeyed()` 将任务路由到同一分片，并发任务也可能按不同顺序完成。需要还原提交顺序时，应在任务结果中携带提交序号，并在收集后按序号排序。

`Go` 独立维护从第 0 片开始的轮转计数；每次调用先选片，随后由目标片检查提交。目标片已完成等待或 context 已取消而拒绝时，轮转位置仍前进，不会自动改投其他片。`GoKeyed` 不推进此计数，也不对传入的 `uint64` 再做哈希；0、最高位为 1 和 MaxUint64 都是合法 key。业务需要字符串或自定义 ID 的哈希分布时，应先自行生成稳定的 uint64。这里与 `ShardedGroup.GoKeyed(string, ...)` 使用字符串哈希的入口不同。

轮转计数使用 uint64 自然回绕；从 MaxUint64 回到 0 时按回绕后的数值继续选片，并非无限精度的连续序号。保持 key 亲和性不能保证同 key 串行执行；若依靠单 worker，需确认没有注入外部 Pool 或启用 AutoScale 改变实际执行并发，也可增加业务同步。生产代码须检查每次提交的 error，最终 Wait 后再核对分片统计；查询或 Wait 返回的切片槽位可以独立修改，指针等元素引用仍按业务所有权同步。取消拒绝与混合路由的可执行示例见 [Example_multiGroupKeyedAndRoundRobin](tests/public_examples_test.go)。

---

## 自定义日志

```go
g := async.Group[int]().
    Logger(myLogger).
    Build()

nr := async.GroupVoid().
    Logger(myLogger).
    DefaultLogger(). // 恢复默认日志
    Build()
```

> **Logger 的作用范围**
>
> `.Logger(l)` 仅配置当前构建器及其后续创建的实例。替换 Context 时会保留该设置，其他构建器及已构建实例不受影响。
>
> `.DefaultLogger()` 或 `.Logger(nil)` 清除当前覆盖，恢复继承全局日志器；全局配置由 `async.SetLogger(l)` 设置。

---

## 默认值体系

Group 的默认值分三层：**全局级** → **Builder 级** → **实例级**。理解这三层关系有助于在项目中合理控制默认行为。

### 一层：全局可配置默认值

以下列出默认值及其作用范围。根包提供的配置函数使用 `async` 前缀；`internal/core` 不能从库外直接导入。

| 默认值 | 获取函数 | 修改函数 | 默认值 | 说明 |
|--------|----------|----------|--------|------|
| 默认超时 | `async.GetDefaultTimeout()` | `async.SetDefaultTimeout(d)` | **30s** | 设为 0 关闭默认超时 |
| IO 并发度 | `async.IO()` | 不可修改 | **`runtime.NumCPU() × 2`** | GroupBuilder / NoResultBuilder 的 Worker 默认值 |
| AutoScale 最小并发 | `DefaultAutoScaleConfig().MinWorkers` | — | **`NumCPU × 2`** | `DefaultAutoScale()` 使用 |
| AutoScale 最大并发 | `DefaultAutoScaleConfig().MaxWorkers` | — | **`NumCPU × 100`** | 自动扩缩容上限 |
| AutoScale 检查间隔 | `DefaultAutoScaleConfig().CheckInterval` | — | **5s** | 每隔 5s 评估一次负载 |
| AutoScale 扩容阈值 | `DefaultAutoScaleConfig().ScaleUpThreshold` | — | **0.7** | `busy/concurrency > 0.7` 触发扩容 |
| AutoScale 缩容阈值 | `DefaultAutoScaleConfig().ScaleDownThreshold` | — | **0.2** | `busy/concurrency < 0.2` 触发缩容 |
| AutoScale 扩容确认 | `DefaultAutoScaleConfig().ScaleUpChecks` | — | **3** | 连续 3 次超阈值才扩容 |
| AutoScale 缩容确认 | `DefaultAutoScaleConfig().ScaleDownChecks` | — | **5** | 连续 5 次低于阈值才缩容 |
| AutoScale 扩容系数 | `DefaultAutoScaleConfig().ScaleUpFactor` | — | **1.5** | 扩容时并发度 × 1.5 |
| AutoScale 缩容系数 | `DefaultAutoScaleConfig().ScaleDownFactor` | — | **0.75** | 缩容时并发度 × 0.75 |
| 默认最大结果数 | 内部配置 | 根包不导出全局设置函数 | **100,000** | Pool 默认值；Group 不适用，可通过 Pool Builder 配置 |
| 全局日志 | `async.GetLogger()` | `async.SetLogger(l)` | **静默** | 全局生效，传 nil 恢复静默 |

> **⚠️ `async.SetDefaultTimeout(0)` 关闭全局默认超时后，Group Builder 仍可单独设置 `.Timeout(d)`**

### 二层：Builder `Default*()` 方法

每个 Builder 提供 `Default*` 系列方法，将指定选项**重置为上述全局默认值**（或清空为用户未设置状态）：

| Default* 方法 | 效果 | 适用 Builder |
|---------------|------|-------------|
| `.DefaultWorker()` | `b.concurrency = core.IO()`（`NumCPU×2`） | GroupBuilder, GroupNoResultBuilder |
| `.DefaultTimeout()` | 保存调用当时 `core.GetDefaultTimeout()`；仅正数在 Build 时覆盖，初始全局值为 30s | GroupBuilder, GroupNoResultBuilder |
| `.DefaultSubmitTimeout()` | `b.submitTimeout = 0`（无提交超时） | GroupBuilder, GroupNoResultBuilder |
| `.DefaultStreaming()` | `b.streaming = 0`（不启用流式） | GroupBuilder, GroupNoResultBuilder |
| `.DefaultResultCallback()` | `b.resultCb = nil`（取消回调） | 仅 GroupBuilder |
| `.DefaultAutoScale()` | `b.autoScale = core.DefaultAutoScaleConfig()` | GroupBuilder, GroupNoResultBuilder |
| `.DefaultLogger()` | 清除当前覆盖，继承全局日志器 | GroupBuilder, GroupNoResultBuilder, MultiGroupBuilder |
| `.DefaultPool()` | `b.extPool = nil`（清除外部 Pool 注入） | GroupBuilder, GroupNoResultBuilder |

> **⚠️ `Default*()` vs 直接设置的区别**
>
> - `.Worker(0)` → Build 时使用 1 个 worker
> - `.DefaultWorker()` → 显式设置为 `async.IO()`
> - Builder 的 `.AutoScale(nil)` → 清除自动扩缩容配置，构建时不启用
> - Builder 的 `.DefaultAutoScale()` → 启用自动扩缩容并使用完整默认配置
> - 实例的 `g.EnableAutoScale(nil)` 与 Builder 不同：它会使用默认配置启用

### 三层：实例级 fallback

Group 实例上的配置方法各自处理非正值：

| 实例方法 | fallback 行为 |
|----------|--------------|
| `g.WithStreaming(bufSize)` | `bufSize <= 0` → `bufSize = g.Worker() * 2` |
| `g.EnableAutoScale(config)` | `config == nil` → 使用 `core.DefaultAutoScaleConfig()` |
| `g.WithTimeout(d)` | 直接设置，无 fallback |
| `g.WithSubmitTimeout(d)` | 直接设置，无 fallback |

### 默认值覆盖优先级

```
同一 Builder 字段以最后一次设置为准；Build 时只应用正数超时，
否则新 Group 保留创建时读取的全局默认超时。
```

例如：
```go
async.SetDefaultTimeout(10 * time.Second)     // 全局 10s

g := async.Group[int]().Timeout(5*time.Second).Build()  // 显式 5s，覆盖全局
// Build 时 g 的 timeout = 5s
```

---

## 生产环境使用建议

### 1. 并发度选择

| 场景 | 推荐设置 | 说明 |
|------|---------|------|
| IO 密集型批量任务 | `.Worker(0)` 或 `.DefaultWorker()` | 默认 `NumCPU × 2`，适合网络/数据库请求 |
| CPU 密集型计算 | `async.CPU()` 或手动调低 | 避免超出物理核心数导致频繁上下文切换 |
| 外部 Pool 注入 | `.Pool(myPool)` | 并发度由 Pool 控制，Group 内部 Worker 仅做分发 |

```go
// IO 密集型：默认并发度可作为起点，按下游容量压测调整
ioGroup := async.Group[int]().Context(ctx).Build()
defer ioGroup.Close()

// CPU 密集型：降低并发
cpuGroup := async.Group[int]().Context(ctx).Worker(async.CPU()).Build()
defer cpuGroup.Close()

// 外部 Pool 注入：并发度委托给 Pool
poolGroup := async.Group[int]().Context(ctx).Pool(myPool).Build()
defer poolGroup.Close()
```

### 2. Group vs Pool 场景选择

| 场景 | 推荐 | 理由 |
|------|------|------|
| 一次性批量处理 N 条记录 | **Group** | 用完即销毁，代码简单 |
| 长期运行的消费者/定时器 | **Pool** | Worker 复用，避免频繁创建/销毁 |
| 请求量波动的 API 网关 | **Pool + AutoScale** | 动态扩缩容，兼顾低延迟与资源 |
| 需要水平分片分摊锁竞争 | **MultiGroup** | 普通 Go 轮转，GoKeyed 按 uint64 key 取模分发；各分片仍使用内部同步 |
| 海量短任务（百万级 QPS） | **Pool** | Group 的 Worker 创建开销不可忽略 |

### 3. FailFast 使用场景

```go
// 批量任务遇到错误时请求取消其他任务；回调须响应 context
var results []async.Result[int]
err := async.Group[int]().Context(ctx).FailFast().Run(
    func(ctx context.Context, g *async.GroupInstance[int]) error {
        var submitErr error
        for _, item := range items {
            if err := g.Go(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            }); err != nil {
                submitErr = err
                break
            }
        }
        results = g.Wait()
        return errors.Join(submitErr, g.JoinErrors())
    })
// 请求取消其他任务；忽略 context 的业务仍可能完成，成功副作用不回滚。
_ = err
_ = results
```

> **⚠️ FailFast 不适用于"尽量完成"的场景**，如日志采集、数据清点等。

### 4. MultiGroup 分片建议

- **根据总并发预算选择分片数**：比较单组与分片的延迟、锁等待和内存开销，避免仅按 CPU 核数决定
- **片内串行时配置 Worker(1)**：须同时确认该片没有外部 Pool 分发或 AutoScale 改变实际执行并发；普通自有 worker 模式下，同片多个 worker 会并发执行，总并发按各片 worker 数求和
- **使用 `GoKeyed` 确保亲和性**：相同 key 的任务路由到同一分片，适合按用户/订单 ID 分片

```go
// 按用户 ID 分片，每片 1 worker；单个生产者按循环顺序提交
err := async.GroupMulti[int]().Shards(8).Worker(1).
    Run(func(ctx context.Context, mg *async.MultiGroup[int]) error {
        for _, user := range users {
            if err := mg.GoKeyed(hash(user.ID), ctx, func(ctx context.Context) (int, error) {
                return processUser(ctx, user)
            }); err != nil { return err }
        }
        for _, result := range mg.Wait() {
            if result.Err != nil { return result.Err }
        }
        return nil
    })
```

### 5. 超时设置建议

- **按业务 SLA 设置超时并响应取消**：全局初始值为 30s，仅创建回调 context 期限；忽略 context 的业务仍可继续，等待也可能继续阻塞
- **每个任务单独设置超时**：对于不同耗时任务使用 `GoWithTimeout`
- **同时设置提交超时**：避免在高并发下 `Go()` 无限阻塞

```go
// 全局修改默认超时
async.SetDefaultTimeout(5 * time.Second)

// 个别长耗时任务单独设置
g.GoWithTimeout(ctx, 30*time.Second, slowTask)

// 提交层面也加上超时保护
g.WithSubmitTimeout(3 * time.Second).Go(ctx, task)
```

### 6. 流式消费 vs 批量 Wait

| 方式 | 适用场景 | 内存 |
|------|---------|------|
| `g.Wait()` | 全部完成后再处理结果 | 所有结果驻留内存 |
| `StreamResults()` | 尽早处理到达的结果；慢消费可能丢失 | Group 仍保留结果，同时占用 channel 和 overflow buffer |

```go
// 流式消费：尽早处理结果；Group 仍会保留结果
g := async.Group[int]().Context(ctx).Worker(16).Streaming(256).Build()
defer g.Close()
done := make(chan error, 1)

go func() {
    var submitErr error
    for _, item := range items {
        if err := g.Go(ctx, func(ctx context.Context) (int, error) {
            return process(ctx, item)
        }); err != nil {
            submitErr = err
            break
        }
    }
    _ = g.Wait() // 等待所有已提交任务完成并关闭 stream channel
    done <- errors.Join(submitErr, g.JoinErrors())
}()

for r := range g.StreamResults() {
    if r.Occupied && r.Err == nil {
        handle(r.Value)
    }
}
return <-done // 消费完缓冲结果，并等待生产者汇总错误后再返回
```

此片段放在返回 `error` 的函数内并导入标准库 `errors`。错误从完整等待后的保留结果汇总，不能只检查可能丢失的结果流。`handle` 须及时返回；若它可能失败，需另行汇总其处理错误并继续排空或协调取消、收尾。

### 7. 生产推荐配置

```go
// 一次性任务且全部结果可驻留内存：显式检查提交和业务错误
err := async.Group[Record]().Context(ctx).
    Worker(async.IO()).                 // NumCPU×2，按业务和下游容量调整
    Timeout(30 * time.Second).          // 单任务超时
    FailFast().                         // 任一失败时请求取消，业务函数须协作退出
    Run(func(ctx context.Context, g *async.GroupInstance[Record]) error {
        for _, r := range records {
            if err := g.Go(ctx, func(ctx context.Context) (Record, error) {
                return process(ctx, r)
            }); err != nil { return err }
        }
        _ = g.Wait()
        return g.FirstError()
    })
if err != nil { return err }

// 需要尽早处理到达结果：流式消费仍须为结果数组预留内存
g := async.Group[Record]().Context(ctx).
    Worker(async.IO()).
    Timeout(30 * time.Second).
    Streaming(2048).                    // 流式结果 channel
    Build()
defer g.Close()

producerDone := make(chan error, 1)
go func() {
    var submitErr error
    for _, r := range records {
        if err := g.Go(ctx, func(ctx context.Context) (Record, error) {
            return process(ctx, r)
        }); err != nil {
            submitErr = err
            break
        }
    }
    _ = g.Wait() // 即使提交失败也等待已接受的任务并关闭 stream
    producerDone <- submitErr
}()

for r := range g.StreamResults() {
    if r.Ok() { handle(r.Value) }
}
if err := <-producerDone; err != nil { return err }
if err := g.FirstError(); err != nil { return err }

// 只需保留部分结果：按实测内存预算选择主结果与溢出缓冲容量
// OverflowDrop 在缓冲满时会丢弃结果；需要全部结果/错误时须另设可靠持久化路径
err = async.Pool[Record]().Context(ctx).
    Worker(async.IO()).
    MaxResults(100_000).RingBuf(50_000).
    Overflow(async.OverflowDrop).
    Run(func(ctx context.Context, p *async.PoolInstance[Record]) error {
        for _, r := range records {
            if err := p.Submit(ctx, func(ctx context.Context) (Record, error) {
                return process(ctx, r)
            }); err != nil { return err }
        }
        results := p.Wait()
        return handleResults(results)
    })
if err != nil { return err }

// 外部 Pool 注入：共享已有协程池
err = async.Pool[int]().Context(ctx).Worker(32).Run(func(ctx context.Context, myPool *async.PoolInstance[int]) error {
    g := async.Group[int]().Context(ctx).Pool(myPool).Build()
    defer g.Close() // Group Close 不会 Close 外部 Pool
    // 在这里提交并等待 Group 任务；Pool 在外层 Run 返回后关闭。
    return nil
})
if err != nil { return err }

// ========== GroupVoid（无返回值）：仅关心错误 ==========
err = async.GroupVoid().Context(ctx).
    Worker(async.IO()).
    Timeout(30 * time.Second).
    FailFast().
    Run(func(ctx context.Context, nr *async.GroupNoResult) error {
        for _, record := range records {
            if err := nr.Go(ctx, func(ctx context.Context) error {
                return db.Insert(ctx, record)
            }); err != nil { return err }
        }
        nr.Wait()
        return nr.FirstError()
    })
if err != nil { return err }

// ========== MultiGroup（8 分片）：水平扩展避免单 Group 锁竞争 ==========
err = async.GroupMulti[int]().Context(ctx).
    Shards(8).                              // 8 个独立 Group 分片
    Worker(1).                              // 每分片 1 worker（分片本身提供并行度）
    Timeout(30 * time.Second).
    FailFast().
    Run(func(ctx context.Context, mg *async.MultiGroup[int]) error {
        for _, user := range users {
            if err := mg.GoKeyed(hash(user.ID), ctx, func(ctx context.Context) (int, error) {
                return processUser(ctx, user)
            }); err != nil { return err }
        }
        for _, result := range mg.Wait() {
            if result.Err != nil { return result.Err }
        }
        return nil
    })
if err != nil { return err }
```

上述容量和并发数仅作配置示例，不按任务数量划固定阈值；应结合每项结果大小、保留方式、处理延迟及下游并发预算压测。`Run` 自动关闭实例，但其返回错误只来自作用域回调，不会自动汇总任务错误，因此示例在回调内等待并检查。Group 的流式通道在慢消费时可能丢失结果；需要每项结果都处理时，使用 `Wait()` 返回值或设计可靠的回调持久化，不要仅依赖 `StreamResults()`。

可执行对照见 [Example_groupRunErrors](tests/public_examples_test.go)：有返回值 Group 的作用域回调返回 nil 时，即使任务失败，Run 仍返回 nil；GroupVoid 示例通过 Wait 和 FirstError 显式传播业务错误。

### 8. 错误处理最佳实践

```go
g := async.Group[int]().Context(ctx).Build()
defer g.Close()

// 提交任务
for _, item := range items {
    if err := g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    }); err != nil {
        log.Printf("提交失败: %v", err)
        break
    }
}

results := g.Wait()

// 集中处理错误
for _, r := range results {
    if r.IsPanic() {
        // %+v 会调用 PanicError.Format 并输出调用栈；Result 没有 Stack 字段
        log.Printf("任务 panic: %+v", r.Err)
        continue
    }
    if r.Err != nil {
        log.Printf("任务失败: %v", r.Err)
        continue
    }
    handle(r.Value)
}
```

### 9. 内存控制

- **海量任务**：Group 的流式消费仍保留结果数组；应估算结果内存，或使用配置了结果存储上限的 Pool
- **GoAt 大索引场景**：结果数组按最大索引分配，`GoAt(999999, ...)` 会分配 100 万个元素；每个元素的大小取决于 `T` 和运行平台，慎用
- **Run 模式自动 Close**：不会遗漏资源释放，生产环境优先使用

### 10. 快速参考卡片

```
┌─────────────────────────────────────────────────────────┐
│  Group 选择流程                                          │
│                                                         │
│  有返回值？ ──Yes──> async.Group[T]()                    │
│     │                                                   │
│     No                                                  │
│     v                                                   │
│  async.GroupVoid() (→ NoResult)                         │
│                                                         │
│  需要分片？ ──Yes──> async.GroupMulti[T]()               │
│     (高并发、避免锁竞争)                                  │
│                                                         │
│  生产环境必备：                                          │
│  • .Timeout(d) / .DefaultTimeout()                      │
│  • .FailFast()（按需）                                   │
│  • defer g.Close() 或 .Run(fn)                          │
│  • 估算结果保留内存；Streaming 不减少结果数组             │
└─────────────────────────────────────────────────────────┘
```

---

## 常见错误

- **❌ Go 后忘记 Wait**：`Go` 只是提交任务，不等待结果。手动创建的 Group 应调用 `Wait()` 或 `Close()` 完成生命周期管理；`Run` 会自动关闭。

  ```go
  // ❌ 错误：Build 后未 Wait
  g := async.Group[int]().Build()
  g.Go(ctx, fn)
  // 缺少 g.Wait() 或 g.Close()

  // ✅ 正确
  g2 := async.Group[int]().Build()
  g2.Go(ctx, fn)
  results := g2.Wait()
  ```

- **❌ Wait 后继续 Go**：开始 `Wait()` 后不能再提交任务。等待完全结束后可调用 `Reset()` 再复用，或 `Build()` 新 Group。

  ```go
  // ❌ 错误：Wait 后复用
  g := async.Group[int]().Build()
  g.Go(ctx, fn1)
  results1 := g.Wait()
  g.Go(ctx, fn2)  // 错误：通道已关闭

  // ✅ 正确：等待结束后重置，并检查错误
  if _, err := g.Reset(); err == nil {
      g.Go(ctx, fn2)
      _ = g.Wait()
  }
  ```

- **❌ GoAt 大索引造成内存浪费**：`GoAt(index, ctx, fn)` 的结果数组按最大索引扩展。`GoAt(999999, ...)` 会分配 100 万个 `Result[T]` 槽位；单个槽位大小取决于 T 和平台。非连续索引场景慎用。

  ```go
  // ❌ 危险：分配 100 万个 Result
  g.GoAt(999999, ctx, fn)

  // ✅ 正确：稀疏索引用 Map 或连续 Go
  g.Go(ctx, fn)
  ```

- **❌ WithFailFast 不传递返回的 ctx**：`g.WithFailFast(ctx)` 返回一个新的 `failFastCtx`。后续 `Go` 必须使用此 `failFastCtx`，否则 FailFast 不生效（因为 Group 检查的是传入的 ctx）。

  ```go
  // ❌ 错误：FailFast 不生效
  g, _ = g.WithFailFast(ctx)
  g.Go(ctx, fn)  // 使用了原始 ctx → FailFast 无效

  // ✅ 正确：传递 failFastCtx
  g, ffCtx := g.WithFailFast(ctx)
  g.Go(ffCtx, fn)
  ```

- **❌ GroupVoid 回调签名错误**：`GroupVoid().Build()` 返回 `*async.GroupNoResult`。使用 `Go(ctx, fn)`，其中 `fn` 的签名是 `func(context.Context) error`。

  ```go
  nr := async.GroupVoid().Build()
  defer nr.Close()
  if err := nr.Go(ctx, func(ctx context.Context) error {
      return process(ctx)
  }); err != nil {
      return err
  }
  nr.Wait()
  ```

- **❌ 依赖未及时消费的 Streaming 获取完整结果**：Group 的 stream channel 满时，结果暂存到溢出缓冲区；`Wait()` 关闭 stream 时，仍无法写入 channel 的溢出结果会丢弃。需要完整结果时使用 `Wait()` 返回值，或通过 `ResultCallback` 保存。

  ```go
  // ✅ 完整生命周期：启动消费者、提交、等待任务、等待消费者
  g := async.Group[int]().Streaming(1024).Build()
  defer g.Close()
  consumerDone := make(chan struct{})
  go func() {
      defer close(consumerDone)
      for r := range g.StreamResults() {
          handle(r)
      }
  }()
  if err := g.Go(ctx, fn); err != nil {
      log.Printf("提交失败: %v", err)
  }
  results := g.Wait() // 完整结果；stream 仍可能遗漏溢出项
  <-consumerDone
  _ = results
  ```

- **❌ 混淆初始并发度和扩缩容配置**：Builder 的 `Worker(n)` 设置初始 worker 数；`DefaultAutoScale()` 启用默认扩缩容配置。未设置 `Worker` 时使用 `async.IO()`，启用时不会立即把初始值调整到 Min/Max 范围内。

  ```go
  // ✅ 推荐：显式设置全部
  async.Group[int]().Context(ctx).
      Worker(16).                       // 初始并发
      DefaultAutoScale().               // 默认扩缩容配置
      Build()
  ```

- **Logger 隔离**：`.Logger(l)` 只配置当前构建器及其后续实例。`.DefaultLogger()` 只清除当前覆盖，不重置应用的全局日志器，也不修改其他实例。

- **❌ 依赖重复的 GoAt 索引保持提交顺序**：同一 index 的多个任务都会提交；后完成的结果占据该索引，原有结果移到其他空槽或追加。并发时完成顺序不确定，需要稳定的索引对应关系时应使用不同索引。

---

## 附录：Group 实例完整方法列表

以下是通过 `.Build()` 获得的 `*Group[T]` 实例上可用的所有方法：

| 分类 | 方法 | 签名 |
|------|------|------|
| 任务提交 | `Go` | `Go(ctx, fn func(ctx) (T, error)) error` |
| 任务提交 | `GoWithTimeout` | `GoWithTimeout(ctx, timeout, fn func(ctx) (T, error)) error` |
| 任务提交 | `GoAt` | `GoAt(index, ctx, fn func(ctx) (T, error)) error` |
| 任务提交 | `GoAtWithTimeout` | `GoAtWithTimeout(index, ctx, timeout, fn func(ctx) (T, error)) error` |
| 等待结果 | `Wait` | `Wait() []Result[T]` |
| 等待结果 | `WaitTimeout` | `WaitTimeout(d) ([]Result[T], bool)` |
| 等待结果 | `WaitContext` | `WaitContext(ctx) ([]Result[T], bool)` |
| 结果查询 | `Values` | `Values() []T` |
| 结果查询 | `Errors` | `Errors() []error` |
| 结果查询 | `FirstError` | `FirstError() error` |
| 结果查询 | `JoinErrors` | `JoinErrors() error` |
| 状态查询 | `Worker` | `Worker() int` |
| 状态查询 | `Active` | `Active() int` |
| 状态查询 | `Busy` | `Busy() int` |
| 状态查询 | `TotalCount` | `TotalCount() int64` |
| 状态查询 | `FailCount` | `FailCount() int64` |
| 状态查询 | `OkCount` | `OkCount() int64` |
| 状态查询 | `HasError` | `HasError() bool` |
| 状态查询 | `Stats` | `Stats() GroupStats` |
| 流式消费 | `StreamResults` | `StreamResults() <-chan Result[T]` |
| 配置 | `WithTimeout` | `WithTimeout(d) *Group[T]` |
| 配置 | `WithSubmitTimeout` | `WithSubmitTimeout(d) *Group[T]` |
| 配置 | `WithStreaming` | `WithStreaming(bufSize) *Group[T]` |
| 配置 | `WithResultCallback` | `WithResultCallback(fn) *Group[T]` |
| 配置 | `WithPool` | `WithPool(p) *Group[T]` |
| 配置 | `WithContext` | `WithContext(ctx) (*Group[T], context.Context)` |
| 配置 | `WithFailFast` | `WithFailFast(ctx) (*Group[T], context.Context)` |
| 配置 | `WithTraceID` | `WithTraceID(ctx) (*Group[T], context.Context)` |
| 配置 | `WithFFCtx` | `WithFFCtx(ctx) (*Group[T], context.Context)`（等同于 WithFailFast） |
| 配置 | `MergeFailFastCancel` | `MergeFailFastCancel(cancel) *Group[T]` |
| 配置组合 | 见下表 | 12 个组合方法；GroupVoid 提供同名方法 |
| 生命周期 | `Reset` | `Reset() (*Group[T], error)` |
| 生命周期 | `Close` | `Close()`（等价于 `Wait()`） |
| 扩缩容 | `EnableAutoScale` | `EnableAutoScale(config)` |
| 扩缩容 | `DisableAutoScale` | `DisableAutoScale()` |
| 扩缩容 | `IsAutoScaleEnabled` | `IsAutoScaleEnabled() bool` |
| 分片 | `Shard` | `Shard(shards) *MultiGroup[T]` |
| 分片 | `DefaultShard` | `DefaultShard() *MultiGroup[T]`（按 GOMAXPROCS 分片，最少 2） |

### 实例组合配置

下表同时适用于 `*async.GroupInstance[T]` 和 `*async.GroupNoResult`。每个方法返回实例本身和派生的 `context.Context`；提交任务时使用返回的 context。`FF` 启用 FailFast，`Ctx` 创建可取消的派生 context，`TO` 是每个任务的超时，`Sto` 是提交超时，`TID` 确保 context 有 TraceID。`timeout` 与 `submitTimeout` 的类型都是 `time.Duration`，设置为 0 时对应超时不启用。

| 方法及参数 | 配置效果 |
|---|---|
| `WithFFTraceID(ctx)` | FailFast、TraceID |
| `WithFFSto(ctx, submitTimeout)` | FailFast、提交超时 |
| `WithFFStoTID(ctx, submitTimeout)` | FailFast、提交超时、TraceID |
| `WithFFTimeout(ctx, timeout)` | FailFast、任务超时 |
| `WithFFTOTID(ctx, timeout)` | FailFast、任务超时、TraceID |
| `WithFFTOSto(ctx, timeout, submitTimeout)` | FailFast、任务超时、提交超时 |
| `WithFFTOStoTID(ctx, timeout, submitTimeout)` | FailFast、任务超时、提交超时、TraceID |
| `WithCtxTraceID(ctx)` | 可取消 context、TraceID |
| `WithCtxTimeout(ctx, timeout)` | 可取消 context、任务超时 |
| `WithCtxTOTID(ctx, timeout)` | 可取消 context、任务超时、TraceID |
| `WithCtxSto(ctx, submitTimeout)` | 可取消 context、提交超时 |
| `WithCtxStoTID(ctx, submitTimeout)` | 可取消 context、提交超时、TraceID |

配置应在并发提交任务前完成；任务超时及取消需要回调配合检查 context，不能强制终止不响应取消的回调。例如，需要快速失败、任务和提交超时及 TraceID 时：

```go
g := async.Group[int]().Worker(8).Build()
g, taskCtx := g.WithFFTOStoTID(ctx, 3*time.Second, 500*time.Millisecond)
if err := g.Go(taskCtx, fn); err != nil {
    log.Printf("submit: %v", err)
}
results := g.Wait()
_ = results
```

不需要结果值时可使用 `async.GroupVoid().Worker(8).Build()`，并向同名 `Go` 传入 `func(context.Context) error`。`Reset()` 会清除 FailFast 配置；复用前应重新配置并使用新返回的 context。
