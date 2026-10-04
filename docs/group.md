# Group（任务组）文档

## 概述

`Group[T]` 是一次性批量并发任务组，Build/Run 时预创建 Worker goroutine，通过 channel 分发任务，Group Close/Wait 时全部 Worker 退出。

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
> - **`.Build()` → `*Group[T]`**：创建 Group 实例，用户自行管理生命周期（**必须 `defer g.Close()`**）
> - **`.Run(fn func(ctx context.Context, g *Group[T]) error) error`**：创建 Group → 执行 fn → fn 返回后**自动 Close**
>
> 这与 PoolBuilder 不同，PoolBuilder 只有 `.Run(fn)` 没有 `.Build()`。
>
> **⚠️ Run 回调中 Group/NoResult/MultiGroup 的类型需要导入内部包**
>
> `Group[T]`、`NoResult`、`MultiGroup[T]` 等实例类型位于 `github.com/chichengyu/async/internal/group`，Run 回调中需显式声明参数类型时需导入：
>
> ```go
> import "github.com/chichengyu/async/internal/group"
> ```
>
> Build 模式下使用 `:=` 可自动推断类型，无需显式导入。

> **⚠️ goroutine 复用规则**
>
> Group 在创建时预启动 `Worker` 个 goroutine，所有 `Go()` 提交的任务经由 channel 分发给这些 worker 并发执行。与 Pool 不同之处在于：Group 是**一次性**的，Close/Wait 后所有 worker 退出，而 Pool 的 worker 持续复用。海量短任务请使用 [Pool](pool.md)。

> **⚠️ Go 后必须 Wait（Build 模式）**
>
> Group 内的 Worker goroutine 在 `Wait()`（或 `Close()`）返回前不会自行退出。Build 模式下提交任务后**必须调用 `Wait()`（或等价 `Close()`）等待全部完成**，否则 worker goroutine 泄漏。Run 模式会自动 Close，不需要手动 Wait。

> **⚠️ Wait 一次性**
>
> `Wait()` 只能调用一次，调用后不可再 `Go()`。如需复用，请使用 `Reset()` 重置 Group。

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
| `.Worker(n)` | 设置最大并发数（≤0 自动修正为 `async.IO()`） | `async.IO()`（`runtime.NumCPU()×2`） |
| `.DefaultWorker()` | 恢复为默认并发度 `async.IO()`（即 `runtime.NumCPU()×2`） | — |
| `.Timeout(d)` | 设置单任务超时 | `core.GetDefaultTimeout()`（默认 30s，可通过 `core.SetDefaultTimeout` 修改） |
| `.DefaultTimeout()` | 恢复为默认超时 `core.GetDefaultTimeout()`（30s） | — |
| `.SubmitTimeout(d)` | 设置获取并发槽位的提交超时 | 无限制（阻塞等待） |
| `.DefaultSubmitTimeout()` | 恢复默认：无提交超时限制 | — |
| `.FailFast()` | 启用 FailFast 快速失败模式 | 关闭 |
| `.Streaming(buf)` | 设置流式结果 channel 缓冲大小，**buf>0 才启用**，0 不启用流式 | 0（禁用流式） |
| `.DefaultStreaming()` | 恢复默认：不启用流式 | — |
| `.ResultCallback(fn)` | 设置每个任务完成时的回调函数 | 无 |
| `.DefaultResultCallback()` | 清除结果回调 | — |
| `.AutoScale(config)` | 设置自动扩缩容配置，**config 为 nil 时自动使用 `DefaultAutoScaleConfig()`** | 关闭 |
| `.DefaultAutoScale()` | 用默认配置启用（MinWorkers=`NumCPU×2`, MaxWorkers=`NumCPU×100`，阈值 0.7→0.2，CheckInterval 5s） | — |
| `.Pool(p)` | 注入外部协程池（用户自行管理生命周期） | 无（内部创建 worker） |
| `.DefaultPool()` | 清除外部池注入，恢复内部 worker 模式 | — |
| `.Logger(l)` | 注入自定义日志（**全局生效**） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |

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
> - `g`：已创建的 `*Group[T]` 实例（内部类型为 `*group.Group[T]`，需 `import "github.com/chichengyu/async/internal/group"`）
> - fn 返回后，Group 会自动调用 `g.Close()`（等价于 `g.Wait()`），**无需用户手动 Close**
> - fn 的返回值会作为 `.Run(fn)` 的返回值透传出去

---

## GroupNoResultBuilder 链式方法速查表

### GroupNoResultBuilder 链式配置方法

> `async.GroupVoid()` 返回 `*GroupNoResultBuilder`，配置项少于 `GroupBuilder`，**没有 ResultCallback**。

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `context.Background()` |
| `.Worker(n)` | 设置最大并发数（≤0 自动修正为 `async.IO()`） | `async.IO()`（`runtime.NumCPU()×2`） |
| `.DefaultWorker()` | 恢复为默认并发度 `async.IO()`（即 `runtime.NumCPU()×2`） | — |
| `.Timeout(d)` | 设置单任务超时 | `core.GetDefaultTimeout()`（默认 30s，可通过 `core.SetDefaultTimeout` 修改） |
| `.DefaultTimeout()` | 恢复为默认超时 `core.GetDefaultTimeout()`（30s） | — |
| `.SubmitTimeout(d)` | 设置获取并发槽位的提交超时 | 无限制（阻塞等待） |
| `.DefaultSubmitTimeout()` | 恢复默认：无提交超时限制 | — |
| `.FailFast()` | 启用 FailFast 快速失败模式 | 关闭 |
| `.Streaming(buf)` | 设置流式结果 channel 缓冲大小，**buf>0 才启用**，0 不启用流式 | 0（禁用流式） |
| `.DefaultStreaming()` | 恢复默认：不启用流式 | — |
| `.AutoScale(config)` | 设置自动扩缩容配置，**config 为 nil 时自动使用 `DefaultAutoScaleConfig()`** | 关闭 |
| `.DefaultAutoScale()` | 用默认配置启用（MinWorkers=`NumCPU×2`, MaxWorkers=`NumCPU×100`，阈值 0.7→0.2，CheckInterval 5s） | — |
| `.Pool(p)` | 注入外部协程池（`*pool.Pool[struct{}]`） | 无（内部创建 worker） |
| `.DefaultPool()` | 清除外部池注入，恢复内部 worker 模式 | — |
| `.Logger(l)` | 注入自定义日志（**全局生效**） | 静默 |
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
> - `nr`：已创建的 `*NoResult` 实例（内部类型为 `*group.NoResult`，需 `import "github.com/chichengyu/async/internal/group"`）
> - fn 返回后自动 `nr.Close()`，**无需用户手动 Close**

---

## 创建：Build 模式

Build 模式下用户自行管理 Group 的生命周期，**必须 `defer g.Close()`**。

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

> **⚠️ Build 模式下必须 `defer g.Close()`**
>
> 与 Run 模式不同，Build 模式创建的 Group 不会自动清理。忘记 `Close()` 会导致 goroutine 泄漏和 channel 未关闭。

> **⚠️ 链式配置方法调用顺序无关紧要**
>
> 所有 `.Context()`、`.Worker()`、`.Timeout()` 等链式配置方法的调用顺序不影响结果，只需在 `.Build()` 或 `.Run(fn)` 之前调用即可。

---

## 创建：Run 模式

Run 模式下自动管理生命周期，fn 执行完毕后自动 `Close()`。**推荐日常使用此模式**。

### 基础用法

```go
err := async.Group[int]().Context(ctx).
    Worker(8).
    Timeout(5 * time.Second).
    Run(func(ctx context.Context, g *group.Group[int]) error {
        for _, item := range items {
            g.Go(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            })
        }
        return nil
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
    Run(func(ctx context.Context, g *group.Group[int]) error {
        for _, item := range items {
            g.Go(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            })
        }
        allResults = g.Wait()
        return nil
    })

// fn 返回后 g 已自动 Close
for _, r := range allResults {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
```

> **⚠️ fn 内调用 Wait 后不能再 Go**
>
> `Wait()` 只能调用一次。在 fn 内调用 `g.Wait()` 后，不能再调用 `g.Go()`。

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

提交任务并**单独设置该任务的超时**，覆盖 Group 级别的 `.Timeout(d)`：

```go
g.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) (string, error) {
    return slowWork(ctx)
})
```

### GoAt

指定索引位置提交任务，保证结果在 `results[index]`：

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

### GoAtWithTimeout

指定索引 + 单独设置超时：

```go
// 第 3 个任务只需 500ms 超时
g.GoAtWithTimeout(2, ctx, 500*time.Millisecond, func(ctx context.Context) (string, error) {
    return quickAPI(ctx)
})
```

---

## 等待与结果（Group 实例方法）

### Wait

等待所有任务完成，返回按提交顺序排列的结果切片。调用后 Group 进入 waited 状态，**不能再提交任务**：

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
// results 包含已完成任务的结果
```

### WaitContext

等待所有任务完成或 ctx 取消。`ok=false` 表示 ctx 提前取消：

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
results, ok := g.WaitContext(ctx)
```

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

```go
g.Wait()
values := g.Values() // []T，只包含成功的值
for _, v := range values {
    fmt.Println(v)
}
```

### Errors / FirstError / JoinErrors

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

以下方法**线程安全**，可在 Wait 前或 Wait 后调用。

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `Worker()` | `int` | 当前最大并发数（自动扩缩容时可能动态变化） |
| `Active()` | `int` | 当前活跃（已启动未结束）的 goroutine 数 |
| `Busy()` | `int` | 当前正在执行任务的 goroutine 数 |
| `TotalCount()` | `int64` | 已提交任务总数（含成功和失败） |
| `FailCount()` | `int64` | 失败任务数 |
| `SuccessCount()` | `int64` | 成功任务数 |
| `HasError()` | `bool` | 是否有任务失败 |
| `Stats()` | `GroupStats` | 获取运行时统计快照 |

```go
stats := g.Stats()
fmt.Printf("活跃: %d, 繁忙: %d, 成功: %d, 失败: %d\n",
    stats.Active, stats.Busy, stats.SuccessTask, stats.FailTask)
```

### Reset

重置 Group 状态以便复用（保留 timeout、submitTimeout、concurrency 等配置）：

```go
results := g.Wait()           // 第一批完成
g.Reset()                     // 重置状态
for _, item := range batch2 {
    g.Go(ctx, process(item))
}
results2 := g.Wait()          // 第二批结果
```

> **⚠️ Reset 必须在 Wait 之后调用**
>
> 源码中如果在 Wait 之前调用 Reset 会返回错误 `"async: Group.Reset called before Wait, concurrent unsafe"`。

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
    nr.Go(ctx, func(ctx context.Context) error {
        return db.Insert(ctx, record)
    })
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
    Run(func(ctx context.Context, nr *group.NoResult) error {
        for _, record := range records {
            nr.Go(ctx, func(ctx context.Context) error {
                return db.Insert(ctx, record)
            })
        }
        return nil
    })
```

### NoResult 实例方法

`NoResult` 是基于 `Group[struct{}]` 的定义类型（`type NoResult Group[struct{}]`），方法与 `Group[T]` 对应但适配无返回值场景。

| 方法 | 签名 | 说明 |
|------|------|------|
| `Go` | `Go(ctx, fn func(ctx) error) error` | 提交无返回值任务 |
| `GoWithTimeout` | `GoWithTimeout(ctx, timeout, fn func(ctx) error) error` | 带超时提交 |
| `GoAt` | `GoAt(index, ctx, fn func(ctx) error) error` | 提交到指定索引 |
| `GoAtWithTimeout` | `GoAtWithTimeout(index, ctx, timeout, fn func(ctx) error) error` | 指定索引 + 带超时 |
| `Wait` | `Wait()` | **无返回值（void）** |
| `WaitTimeout` | `WaitTimeout(d) (int64, bool)` | 返回已完成任务数和是否全部完成 |
| `WaitContext` | `WaitContext(ctx) (int64, bool)` | 返回已完成任务数和是否全部完成 |
| `FailCount` | `FailCount() int64` | 失败任务数 |
| `SuccessCount` | `SuccessCount() int64` | 成功任务数 |
| `HasError` | `HasError() bool` | 是否有失败 |
| `TotalCount` | `TotalCount() int64` | 总任务数 |
| `Worker` | `Worker() int` | 最大并发数 |
| `Active` | `Active() int` | 活跃 goroutine 数 |
| `Busy` | `Busy() int` | 忙碌 goroutine 数 |
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

go func() {
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
```

---

## 流式结果消费

### 方式一：StreamResults channel

启用 `.Streaming(buf)` 后，通过 `g.StreamResults()` 获取只读 channel，实时消费结果：

```go
g := async.Group[int]().Context(ctx).
    Worker(4).
    Streaming(1024).
    Build()
defer g.Close()

ch := g.StreamResults()

go func() {
    for r := range ch {
        if r.Ok() {
            fmt.Println(r.Value)
        }
    }
}()

for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    })
}
g.Wait()
// Wait() 返回后 channel 自动关闭
```

> **⚠️ Streaming channel 在 Wait() 后自动关闭**
>
> `StreamResults()` 返回的 channel 会在 `Wait()`/`Close()` 内部调用 `drainStreaming()` 时自动关闭。所以在 `Wait()` 之后读取 channel 的 goroutine 会自动退出。

### 方式二：ResultCallback 回调

每个任务完成时**同步**调用回调函数，在 worker goroutine 中执行：

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

for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    })
}
g.Wait()
```

> **⚠️ ResultCallback 在 worker goroutine 中同步执行**
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
p := async.Pool[int]().Worker(8).Build()
defer p.Close()

g := async.Group[int]().Context(ctx).
    Pool(p).
    Build()
defer g.Close()

for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    })
}
results := g.Wait()
```

> **⚠️ 外部 Pool 需自行管理生命周期**
>
> `.Pool(p)` 注入后，Group 不会自动 Close 该 Pool。用户需在适当的时机 `defer p.Close()`。

> **⚠️ Pool 注入后并发度由 Pool 控制**
>
> 注入外部 Pool 后，Group 内部的 concurrency worker 仅做任务分发，实际并发度由外部 Pool 的 worker 数量控制。

---

## FailFast 快速失败模式

任一任务出错时，**立即取消所有其他任务**：

```go
g := async.Group[int]().Context(ctx).
    Worker(8).
    FailFast().
    Build()
defer g.Close()

for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    })
}
results := g.Wait()
// 任一任务失败后，其他未完成的任务会收到 context.Canceled
```

> **⚠️ FailFast 会取消所有剩余任务**
>
> 启用 FailFast 后，一旦任一任务返回 error（非 nil），Group 会调用内部 cancel 函数取消所有其他任务的 context。其他任务应检查 `ctx.Err()` 并及时退出。

> **⚠️ panic 也会触发 FailFast**
>
> 如果任务 panic，Group 会捕获 panic 并记录错误，同时触发 FailFast 取消其他任务（如果 FailFast 已启用）。

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

> **⚠️ Wait 后自动停止扩缩容**
>
> Group.Wait() 后 AutoScale 后台 goroutine 会自动检测到 done channel 关闭并退出。

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
| `.FailFast()` | 启用 FailFast | 关闭 |
| `.Logger(l)` | 注入自定义日志（全局生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志（调用 `core.SetLogger(nil)`） | — |
| `.Run(fn)` | **终端**：创建 → 执行 → 自动 Close | — |

### 基础用法

```go
err := async.GroupMulti[int]().Context(ctx).
    Shards(8).
    Worker(50).
    Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
        for i := 0; i < 10000; i++ {
            idx := i
            mg.Go(ctx, func(ctx context.Context) (int, error) {
                return compute(ctx, idx)
            })
        }
        results := mg.Wait()
        // 处理 results...
        return nil
    })
```

> **⚠️ 总并发量 = Shards × Worker**
>
> `Shards(8).Worker(50)` 意味着 8 个分片 × 每分片 50 并发 = **400 并发总量**。

### MultiGroup 实例方法

| 方法 | 说明 |
|------|------|
| `Go(ctx, fn)` | round-robin 分发到某个分片 |
| `GoKeyed(key, ctx, fn)` | 按 key 哈希路由到固定分片（同一 key 的任务落到同一分片） |
| `Wait()` | 等待所有分片完成，合并结果 |
| `Close()` | 关闭所有分片 |
| `ShardCount()` | 返回分片数 |
| `GetShard(idx)` | 获取指定分片的 `*Group[T]` 实例 |
| `TotalWorker()` | 所有分片的总并发数 |
| `TotalActive()` | 所有分片活跃任务总数 |
| `TotalBusy()` | 所有分片忙碌任务总数 |
| `TotalTaskCount()` | 所有分片已提交任务总数 |
| `TotalFailCount()` | 所有分片失败任务总数 |
| `TotalSuccessCount()` | 所有分片成功任务总数 |
| `Errors()` | 所有分片所有错误 |
| `Values()` | 所有分片成功结果值 |
| `StreamResults()` | 合并所有分片流式结果的 channel |
| `WithTimeout(d)` | 为所有分片设置任务超时 |
| `WithStreaming(bufSize)` | 为所有分片启用流式结果消费 |
| `WithResultCallback(fn)` | 为所有分片设置结果回调 |

> **⚠️ MultiGroup 同 Group 也是"一次性"的**
>
> 每个分片内部预创建 Worker goroutine，Run 返回后所有分片 Close、所有 Worker 退出。海量持续性任务请使用 MultiPool。

> **⚠️ Wait() 结果不保证全局顺序**
>
> `mg.Wait()` 合并结果时保持各分片内部顺序，但分片间不保证全局顺序。如果需要按原始提交顺序排列结果，请使用 `GoKeyed()` 确保同一批任务落到同一分片。

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

> **⚠️ Logger 是全局生效的**
>
> `.Logger(l)` 内部调用 `core.SetLogger(l)` 设置全局日志器，会影响所有后续创建的 Group/Pool 等组件。
>
> `.DefaultLogger()` 内部调用 `core.SetLogger(nil)` 恢复默认日志实现。

---

## 默认值体系

Group 的默认值分三层：**全局级** → **Builder 级** → **实例级**。理解这三层关系有助于在项目中合理控制默认行为。

### 一层：全局可配置默认值

以下默认值通过 `core` 包的全局变量控制，影响所有后续创建的 Group / Pool：

| 默认值 | 获取函数 | 修改函数 | 默认值 | 说明 |
|--------|----------|----------|--------|------|
| 默认超时 | `core.GetDefaultTimeout()` | `core.SetDefaultTimeout(d)` | **30s** | 设为 0 关闭默认超时 |
| IO 并发度 | `core.IO()` | 不可修改 | **`runtime.NumCPU() × 2`** | GroupBuilder / NoResultBuilder 的 Worker 默认值 |
| AutoScale 最小并发 | `DefaultAutoScaleConfig().MinWorkers` | — | **`NumCPU × 2`** | `DefaultAutoScale()` 使用 |
| AutoScale 最大并发 | `DefaultAutoScaleConfig().MaxWorkers` | — | **`NumCPU × 100`** | 自动扩缩容上限 |
| AutoScale 检查间隔 | `DefaultAutoScaleConfig().CheckInterval` | — | **5s** | 每隔 5s 评估一次负载 |
| AutoScale 扩容阈值 | `DefaultAutoScaleConfig().ScaleUpThreshold` | — | **0.7** | `busy/concurrency > 0.7` 触发扩容 |
| AutoScale 缩容阈值 | `DefaultAutoScaleConfig().ScaleDownThreshold` | — | **0.2** | `busy/concurrency < 0.2` 触发缩容 |
| AutoScale 扩容确认 | `DefaultAutoScaleConfig().ScaleUpChecks` | — | **3** | 连续 3 次超阈值才扩容 |
| AutoScale 缩容确认 | `DefaultAutoScaleConfig().ScaleDownChecks` | — | **5** | 连续 5 次低于阈值才缩容 |
| AutoScale 扩容系数 | `DefaultAutoScaleConfig().ScaleUpFactor` | — | **1.5** | 扩容时并发度 × 1.5 |
| AutoScale 缩容系数 | `DefaultAutoScaleConfig().ScaleDownFactor` | — | **0.75** | 缩容时并发度 × 0.75 |
| 默认最大结果数 | `core.GetDefaultMaxResults()` | `core.SetDefaultMaxResults(n)` | **100,000** | Pool 用，Group 不适用 |
| 全局日志 | 通过 `core.SetLogger(l)` | `core.SetLogger(nil)` | **静默** | 全局生效 |

> **⚠️ `core.SetDefaultTimeout(0)` 关闭全局默认超时后，Group 实例仍可单独设置 `.Timeout(d)`**

### 二层：Builder `Default*()` 方法

每个 Builder 提供 `Default*` 系列方法，将指定选项**重置为上述全局默认值**（或清空为用户未设置状态）：

| Default* 方法 | 效果 | 适用 Builder |
|---------------|------|-------------|
| `.DefaultWorker()` | `b.concurrency = core.IO()`（`NumCPU×2`） | GroupBuilder, GroupNoResultBuilder |
| `.DefaultTimeout()` | `b.timeout = core.GetDefaultTimeout()`（30s） | GroupBuilder, GroupNoResultBuilder |
| `.DefaultSubmitTimeout()` | `b.submitTimeout = 0`（无提交超时） | GroupBuilder, GroupNoResultBuilder |
| `.DefaultStreaming()` | `b.streaming = 0`（不启用流式） | GroupBuilder, GroupNoResultBuilder |
| `.DefaultResultCallback()` | `b.resultCb = nil`（取消回调） | 仅 GroupBuilder |
| `.DefaultAutoScale()` | `b.autoScale = core.DefaultAutoScaleConfig()` | GroupBuilder, GroupNoResultBuilder |
| `.DefaultLogger()` | `core.SetLogger(nil)`（恢复全局静默日志） | GroupBuilder, GroupNoResultBuilder, MultiGroupBuilder |
| `.DefaultPool()` | `b.extPool = nil`（清除外部 Pool 注入） | GroupBuilder, GroupNoResultBuilder |

> **⚠️ `Default*()` vs 直接设置的区别**
>
> - `.Worker(0)` → Build 时自动修正为 `core.IO()`（等同于不设置）
> - `.DefaultWorker()` → 显式设置为 `core.IO()`，与 `.Worker(0)` 效果相同
> - `.AutoScale(nil)` → **会**启用 AutoScale（`AutoScale()` 内部检测到 nil 自动调用 `DefaultAutoScaleConfig()`）
> - `.DefaultAutoScale()` → **会**启用 AutoScale（使用完整默认配置，与 `.AutoScale(nil)` 效果相同）

### 三层：实例级 fallback

Group 实例上的配置方法（如 `g.WithTimeout(d)`）在 `d <= 0` 时采用内置 fallback：

| 实例方法 | fallback 行为 |
|----------|--------------|
| `g.WithStreaming(bufSize)` | `bufSize <= 0` → `bufSize = g.Worker() * 2` |
| `g.EnableAutoScale(config)` | `config == nil` → 使用 `core.DefaultAutoScaleConfig()` |
| `g.WithTimeout(d)` | 直接设置，无 fallback |
| `g.WithSubmitTimeout(d)` | 直接设置，无 fallback |

### 默认值覆盖优先级

```
代码显式设置 > Builder Default*() > 全局默认值 > 硬编码兜底值
```

例如：
```go
core.SetDefaultTimeout(10 * time.Second)     // 全局 10s

g := async.Group[int]().Timeout(5*time.Second).Build()  // 显式 5s，覆盖全局
// Build 时 g 的 timeout = 5s
```

---

## 生产环境使用建议

### 1. 并发度选择

| 场景 | 推荐设置 | 说明 |
|------|---------|------|
| IO 密集型批量任务 | `.Worker(0)` 或 `.DefaultWorker()` | 默认 `NumCPU × 2`，适合网络/数据库请求 |
| CPU 密集型计算 | `core.CPU()` 或手动调低 | 避免超出物理核心数导致频繁上下文切换 |
| 外部 Pool 注入 | `.Pool(myPool)` | 并发度由 Pool 控制，Group 内部 Worker 仅做分发 |

```go
// IO 密集型：默认即最佳
g := async.Group[int]().Context(ctx).Build()

// CPU 密集型：降低并发
g := async.Group[int]().Context(ctx).Worker(core.CPU()).Build()

// 外部 Pool 注入：并发度委托给 Pool
g := async.Group[int]().Context(ctx).Pool(myPool).Build()
```

### 2. Group vs Pool 场景选择

| 场景 | 推荐 | 理由 |
|------|------|------|
| 一次性批量处理 N 条记录 | **Group** | 用完即销毁，代码简单 |
| 长期运行的消费者/定时器 | **Pool** | Worker 复用，避免频繁创建/销毁 |
| 请求量波动的 API 网关 | **Pool + AutoScale** | 动态扩缩容，兼顾低延迟与资源 |
| 需要水平分片避免锁竞争 | **MultiGroup** | 按 key 哈希分发，无锁竞争 |
| 海量短任务（百万级 QPS） | **Pool** | Group 的 Worker 创建开销不可忽略 |

### 3. FailFast 使用场景

```go
// ✅ 推荐：批量任务遇到错误应立即停止的场景
g := async.Group[int]().Context(ctx).FailFast().Build()
for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    })
}
results := g.Wait()
// 任一任务失败时，其他正在运行的任务会被 cancel
```

> **⚠️ FailFast 不适用于"尽量完成"的场景**，如日志采集、数据清点等。

### 4. MultiGroup 分片建议

- **分片数 ≥ CPU 核数**：避免单个 Group 成为瓶颈
- **每分片并发度建议 1**：分片本身已提供并行度，过高并发会增加锁竞争
- **使用 `GoKeyed` 确保亲和性**：相同 key 的任务路由到同一分片，适合按用户/订单 ID 分片

```go
// ✅ 推荐：按用户 ID 分片，保证同用户任务串行
err := async.GroupMulti[int]().Shards(8).Worker(1).
    Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
        for _, user := range users {
            mg.GoKeyed(hash(user.ID), ctx, func(ctx context.Context) (int, error) {
                return processUser(ctx, user)
            })
        }
        return nil
    })
```

### 5. 超时设置建议

- **生产环境务必设置超时**：全局默认 30s 是兜底，建议根据业务 SLA 调整
- **每个任务单独设置超时**：对于不同耗时任务使用 `GoWithTimeout`
- **同时设置提交超时**：避免在高并发下 `Go()` 无限阻塞

```go
// 全局修改默认超时
core.SetDefaultTimeout(5 * time.Second)

// 个别长耗时任务单独设置
g.GoWithTimeout(ctx, 30*time.Second, slowTask)

// 提交层面也加上超时保护
g.WithSubmitTimeout(3 * time.Second).Go(ctx, task)
```

### 6. 流式消费 vs 批量 Wait

| 方式 | 适用场景 | 内存 |
|------|---------|------|
| `g.Wait()` | 全部完成后再处理结果 | 所有结果驻留内存 |
| `StreamResults()` | 结果到达即处理，减少峰值内存 | 仅 channel 缓冲 + overflow buffer |

```go
// 流式消费：十万级任务场景推荐
g := async.Group[int]().Context(ctx).Worker(16).Streaming(256).Build()

go func() {
    for _, item := range items {
        g.Go(ctx, func(ctx context.Context) (int, error) {
            return process(ctx, item)
        })
    }
    g.Wait() // 等待所有任务完成并关闭 stream channel
}()

for r := range g.StreamResults() {
    if r.Occupied && r.Err == nil {
        handle(r.Value)
    }
}
```

### 7. 错误处理最佳实践

```go
g := async.Group[int]().Context(ctx).Build()
defer g.Close()

// 提交任务
for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return process(ctx, item)
    })
}

results := g.Wait()

// 集中处理错误
for _, r := range results {
    if r.IsPanic() {
        log.Printf("任务 panic: %v, stack: %s", r.Err, r.Stack)
        continue
    }
    if r.Err != nil {
        log.Printf("任务失败: %v", r.Err)
        continue
    }
    handle(r.Value)
}
```

### 8. 内存控制

- **海量任务（>10万）**：使用流式消费避免结果数组 OOM
- **GoAt 大索引场景**：结果数组按最大索引分配，`GoAt(999999, ...)` 会分配 100 万个元素（每个 `Result[T]` ≈ 200+ 字节），慎用
- **Run 模式自动 Close**：不会遗漏资源释放，生产环境优先使用

### 9. 快速参考卡片

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
│  • 海量任务用 .Streaming(buf)                            │
└─────────────────────────────────────────────────────────┘
```

---

## 常见错误

- **❌ Go/GoArg 后忘记 Wait**：`Go` 只是提交任务，不等待结果。忘记 `Wait()` / `WaitAndClose()` 会导致 goroutine 泄漏。`Run` 模式会自动 Close，`Build` 模式必须手动处理。

  ```go
  // ❌ 错误：Build 后未 Wait
  g := async.Group[int]().Build()
  g.Go(ctx, fn)
  // 缺少 g.Wait() → goroutine 泄漏 + 结果丢失

  // ✅ 正确
  g := async.Group[int]().Build()
  g.Go(ctx, fn)
  results := g.Wait()
  ```

- **❌ Wait 后继续 Go**：`Wait()` 调用后 Group 通道关闭，再次 `Go` 会返回错误。需要复用请 `Build()` 新 Group。

  ```go
  // ❌ 错误：Wait 后复用
  g := async.Group[int]().Build()
  g.Go(ctx, fn1)
  results1 := g.Wait()
  g.Go(ctx, fn2)  // 错误：通道已关闭

  // ✅ 正确：重新 Build
  g2 := async.Group[int]().Build()
  ```

- **❌ GoAt 大索引造成内存浪费**：`GoAt(index, ctx, fn)` 的结果数组按 `max(index)` 分配。`GoAt(999999, ...)` 会预分配 100 万个 `Result[T]` 元素（每个约 200+ 字节，总计约 200MB+），即使只提交一个任务。非连续索引场景慎用。

  ```go
  // ❌ 危险：分配 100 万个 Result
  g.GoAt(999999, ctx, fn)

  // ✅ 正确：稀疏索引用 Map 或连续 Go
  g.Go(ctx, fn)
  ```

- **❌ WithFailFast 不传递返回的 ctx**：`g.WithFailFast(ctx)` 返回一个新的 `failFastCtx`。后续 `Go` 必须使用此 `failFastCtx`，否则 FailFast 不生效（因为 Group 检查的是传入的 ctx）。

  ```go
  // ❌ 错误：FailFast 不生效
  g, ffCtx := g.WithFailFast(ctx)
  g.Go(ctx, fn)  // 使用了原始 ctx → FailFast 无效

  // ✅ 正确：传递 failFastCtx
  g, ffCtx := g.WithFailFast(ctx)
  g.Go(ffCtx, fn)
  ```

- **❌ GroupVoid 用 Go 而非 GoVoid**：`GroupVoid().Build()` 返回的是 `*NoResult`，应该用 `GoVoid(ctx, fn)` 而不是 `Go(ctx, fn)`。使用 `Go` 会编译错误。

  ```go
  // ❌ 编译错误
  nr := async.GroupVoid().Build()
  nr.Go(ctx, fn)  // fn 签名不匹配

  // ✅ 正确
  nr.GoVoid(ctx, fn)
  ```

- **❌ Streaming 未消费导致 worker 阻塞**：与 Pool 相同，启用 `Streaming(buf)` 后结果写入 stream channel，不消费则 `buf` 写满后 worker 阻塞。

  ```go
  // ✅ 正确：流式消费
  g := async.Group[int]().Streaming(1024).Build()
  go func() {
      for r := range g.StreamResults() {
          handle(r)
      }
  }()
  g.Go(ctx, fn)
  ```

- **❌ AutoScale 配置不设置 Concurrency**：`EnableAutoScale(config)` 启用后，`config.MinWorkers` 和 `config.MaxWorkers` 控制并发边界，但 `Concurrency()` 设置的是初始并发度（在 Min/Max 范围内）。省略 `Concurrency()` 时初始并发 = `MinWorkers`。

  ```go
  // ✅ 推荐：显式设置全部
  async.Group[int]().Context(ctx).
      Concurrency(16).                  // 初始并发
      EnableAutoScale(nil).             // nil = DefaultAutoScaleConfig (Min=2×CPU, Max=100×CPU)
      Build()
  ```

- **❌ Logger 全局副作用**：`.Logger(l)` 影响全局所有组件（内部调用 `core.SetLogger(l)`）。测试隔离用 `.DefaultLogger()` 恢复。

- **❌ GoAt(index, ...) index 重复**：同一 index 第二次 `GoAt` 会**覆盖**第一次提交的任务，第一次任务的 fn 可能不会被执行（取决于 worker 调度时机）。需要同一 index 多个结果时用 map 聚合。

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
| 等待结果 | `Close` | `Close()` |
| 结果查询 | `Values` | `Values() []T` |
| 结果查询 | `Errors` | `Errors() []error` |
| 结果查询 | `FirstError` | `FirstError() error` |
| 结果查询 | `JoinErrors` | `JoinErrors() error` |
| 状态查询 | `Worker` | `Worker() int` |
| 状态查询 | `Active` | `Active() int` |
| 状态查询 | `Busy` | `Busy() int` |
| 状态查询 | `TotalCount` | `TotalCount() int64` |
| 状态查询 | `FailCount` | `FailCount() int64` |
| 状态查询 | `SuccessCount` | `SuccessCount() int64` |
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
| 配置组合 | `WithFFTraceID` 等 | 12 个组合方法，命名规则 `With[FF|Ctx][Timeout|Sto|TID]*` |
| 生命周期 | `Reset` | `Reset() (*Group[T], error)` |
| 生命周期 | `Close` | `Close()`（等价于 `Wait()`） |
| 扩缩容 | `EnableAutoScale` | `EnableAutoScale(config)` |
| 扩缩容 | `DisableAutoScale` | `DisableAutoScale()` |
| 扩缩容 | `IsAutoScaleEnabled` | `IsAutoScaleEnabled() bool` |
| 分片 | `Shard` | `Shard(shards) *MultiGroup[T]` |
| 分片 | `DefaultShard` | `DefaultShard() *MultiGroup[T]`（按 CPU 核数分片，最少 2） |