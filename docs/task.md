# Task（异步任务）文档

## 概述

Task 提供单个异步任务的执行能力。通过 `Task[T]()` / `TaskVoid()` Builder 链式构建。

**四种模式对照表**：

| 模式 | Entry | 终端方法 | 返回值类型 | 特点 |
|------|-------|----------|-----------|------|
| 带返回值异步 | `Task[T]()` | `.Go(fn)` | `*AsyncResult[T]` | `Wait()` 获取值+error |
| 无返回值异步 | `TaskVoid()` | `.GoAct(fn)` | `*AsyncResult[NoResult]` | 只关心 error |
| 可取消+返回值 | `Task[T]()` | `.GoResult(fn)` | `Task[T]` | 支持 `Cancel()` |
| 可取消+无返回值 | `TaskVoid()` | `.GoResultAct(fn)` | `Task[NoResult]` | 可取消+只关心 error |

> **GoResult 与取消**
>
> `GoResult` 返回 `Task[T]`。任务函数返回后，即使不调用 `Result()` 或 `Cancel()`，结果发送也不会阻塞；取消只发出 context 信号，任务函数需要配合退出。
>
> **⚠️ ctx 自动注入 TraceID**
>
> 所有 API 内部自动调用 `EnsureTraceID(ctx)`。

---

## 目录

- [Builder 链式方法速查表](#builder-链式方法速查表)
- [带返回值异步](#带返回值异步)
- [无返回值异步](#无返回值异步)
- [可取消异步任务](#可取消异步任务)
- [AsyncResult 方法详解](#asyncresult-方法详解)
- [TaskHandle 方法详解](#taskhandle-方法详解)
- [超时控制](#超时控制)
- [限流控制（Bounded）](#限流控制bounded)
- [BoundedRunner（限流执行器）](#boundedrunner限流执行器)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)
- [自定义日志](#自定义日志)
- [Mu 并发安全切片](#mu-并发安全切片)

---

## Builder 链式方法速查表

### 入口函数

| 方法 | 说明 |
|------|------|
| `async.Task[T]()` | 创建带返回值 Builder（泛型 `T` 为返回值类型） |
| `async.TaskVoid()` | 创建无返回值 Builder（等价于 `Task[struct{}]()`） |

### 链式配置方法

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `context.Background()` |
| `.WithTimeout(d)` | 设置任务超时时间 | 无超时 |
| `.DefaultTimeout()` | 清除超时设置 | — |
| `.Bounded(max)` | 限制最大并发 goroutine 数 | 无限制 |
| `.DefaultBounded()` | 清除并发限制 | — |
| `.Logger(l)` | 注入自定义日志（全局生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |

### 终端方法

| 方法 | fn 签名 | 返回值 | 说明 |
|------|---------|--------|------|
| `.Go(fn)` | `func(ctx) (T, error)` | `*AsyncResult[T]` | 启动带返回值异步任务 |
| `.GoResult(fn)` | `func(ctx) (T, error)` | `Task[T]` | 启动可取消异步任务 |
| `.GoAct(fn)` | `func(ctx) error` | `*AsyncResult[NoResult]` | 启动无返回值异步任务 |
| `.GoResultAct(fn)` | `func(ctx) error` | `Task[NoResult]` | 启动可取消无返回值异步任务 |

---

## 带返回值异步

使用 `Task[T]()` 创建 Builder，`.Go(fn)` 启动异步任务，返回 `*AsyncResult[T]`：

```go
// 基础用法
ar := async.Task[*User]().Context(ctx).
    Go(func(ctx context.Context) (*User, error) {
        return db.GetUser(ctx, userID)
    })

user, err := ar.Wait()  // 阻塞等待结果
```

`*AsyncResult[T]` 提供以下方法：

| 方法 | 签名 | 说明 |
|------|------|------|
| `Wait()` | `(T, error)` | 阻塞等待结果（幂等，多次调用返回相同值） |
| `Values()` | `(T, error)` | `Wait()` 的别名 |
| `Error()` | `error` | 阻塞等待并获取错误 |
| `WaitCh()` | 只读结果 channel | 返回任务结果，可用于 select |
| `Cancel()` | `(T, error)` | 结果已就绪时返回结果，否则返回 `context.Canceled`；不取消任务 |
| `Ok()` | `bool` | 是否成功（无错误、无 panic） |
| `IsPanic()` | `bool` | 是否因 panic 失败 |
| `WaitTimeout(d)` | `(T, error, bool)` | 带超时等待，第三个返回值 `false` 表示超时 |
| `WaitContext(ctx)` | `(T, error, bool)` | 带 Context 控制等待，`false` 表示 ctx 取消 |

```go
ar := async.Task[string]().Context(ctx).
    Go(func(ctx context.Context) (string, error) {
        return fetchData(ctx)
    })

// 阻塞等待
val, err := ar.Wait()

// 非阻塞检查完成状态
select {
case <-ar.WaitCh():
    val, err = ar.Wait()
default:
    fmt.Println("任务尚未完成")
}

// 带超时等待
val, err, ok := ar.WaitTimeout(3 * time.Second)
if !ok {
    fmt.Println("超时")
}
```

---

## 无返回值异步

使用 `TaskVoid()` 创建 Builder，`.GoAct(fn)` 启动异步任务：

```go
ae := async.TaskVoid().Context(ctx).
    GoAct(func(ctx context.Context) error {
        return sendNotification(ctx, userID, msg)
    })

_, err := ae.Wait()  // 阻塞等待，忽略空值
```

> **⚠️ TaskVoid 的实质**
>
> `GoAct()` 使用命名空类型 `NoResult`，`Wait()` 返回 `(NoResult, error)`，通常只取第二个返回值。

---

## 可取消异步任务

### 可取消 + 带返回值

使用 `Task[T]()` 的 `.GoResult(fn)` 启动可取消任务，返回 `Task[T]`：

```go
t := async.Task[*Data]().Context(ctx).
    GoResult(func(ctx context.Context) (*Data, error) {
        return longRunningWork(ctx)
    })

// 随时取消
t.Cancel()

// 获取任务函数返回的结果；函数应自行响应 ctx 取消
result, err := t.Result()
```

`Task[T]` 提供以下字段和方法：

| 方法 | 签名 | 说明 |
|------|------|------|
| `Cancel` | `context.CancelFunc` | 取消内部 context，调用方式为 `t.Cancel()` |
| `Result` | `func() (T, error)` | 阻塞获取结果（幂等，内部缓存） |
| `Ctx` | `context.Context` | 内部 context |
| `Error()` | `error` | 阻塞等待并获取错误 |

```go
t := async.Task[*Order]().Context(ctx).
    GoResult(func(ctx context.Context) (*Order, error) {
        return paymentService.Process(ctx, orderID)
    })

// 10 秒后自动取消
go func() {
    time.Sleep(10 * time.Second)
    t.Cancel()
}()

order, err := t.Result()
```

### 可取消 + 无返回值

使用 `TaskVoid()` 的 `.GoResultAct(fn)`：

```go
tk := async.TaskVoid().Context(ctx).
    GoResultAct(func(ctx context.Context) error {
        return uploadFile(ctx, filepath)
    })

tk.Cancel()
_, err := tk.Result()  // 忽略空值，只处理 error
```

---

## AsyncResult 方法详解

### Wait / Values / Error

```go
ar := async.Task[int]().Context(ctx).
    Go(func(ctx context.Context) (int, error) {
        return compute(ctx)
    })

// 三种等价值获取方式
val, err := ar.Wait()    // 阻塞等待
val, err = ar.Values()   // Wait 的别名
errOnly := ar.Error()    // 阻塞等待，仅获取 error
```

### WaitCh / Cancel

```go
ar := async.Task[string]().Context(ctx).
    Go(func(ctx context.Context) (string, error) {
        time.Sleep(10 * time.Second)
        return "result", nil
    })

// 复用一个结果 channel，避免每次轮询都创建等待 goroutine：
done := ar.WaitCh()
for {
    select {
    case <-done:
        val, err := ar.Wait()
        return val, err
    case <-time.After(500 * time.Millisecond):
        if shouldAbort() {
            // 仅停止等待；如需取消任务，应取消传给任务的 context。
            return "", context.Canceled
        }
    }
}
```

### Ok / IsPanic

```go
ar := async.Task[int]().Context(ctx).
    Go(func(ctx context.Context) (int, error) {
        panic("oops")
    })

val, err := ar.Wait()
fmt.Println(ar.Ok())      // false
fmt.Println(ar.IsPanic()) // true
// val = 0, err 是 *PanicError 类型
```

### WaitTimeout

```go
ar := async.Task[string]().Context(ctx).
    Go(func(ctx context.Context) (string, error) {
        time.Sleep(5 * time.Second)
        return "done", nil
    })

val, err, ok := ar.WaitTimeout(2 * time.Second)
if !ok {
    fmt.Println("超时")  // ok=false, val 为零值, err 可能为 nil
}
```

---

## TaskHandle 方法详解

### Ctx / Result / Cancel

```go
t := async.Task[string]().Context(ctx).
    WithTimeout(5 * time.Second).
    GoResult(func(ctx context.Context) (string, error) {
        // 此处 ctx 是内部 context，受 WithTimeout 和 Cancel 控制
        return slowWork(ctx)
    })

// 获取内部 context（可用于日志追踪）
innerCtx := t.Ctx

// 等待结果
result, err := t.Result()

// 或主动取消
t.Cancel()
```

---

## 超时控制

通过 `.WithTimeout(d)` 设置任务超时：

```go
// 5 秒超时
ar := async.Task[string]().Context(ctx).
    WithTimeout(5 * time.Second).
    Go(func(ctx context.Context) (string, error) {
        // ctx 会在 5 秒后自动取消
        return slowWork(ctx)
    })

val, err := ar.Wait()

// 清除超时设置
ar := async.Task[string]().Context(ctx).
    WithTimeout(5 * time.Second).  // 先设置
    DefaultTimeout().               // 再清除
    Go(fn)
```

> **⚠️ WithTimeout 与 GoResult 配合使用**
>
> `WithTimeout` 设置后，`GoResult` 返回的 `Task[T]` 内部 context 会包含该超时。取消或超时会通知任务函数；`Result()` 返回函数实际返回的值和错误。

---

## 限流控制（Bounded）

通过 `.Bounded(max)` 限制同时运行的 goroutine 数：

```go
// 限制最多 100 并发
ar := async.Task[int]().Context(ctx).
    Bounded(100).
    Go(func(ctx context.Context) (int, error) {
        return process(ctx)
    })

// 清除并发限制
ar := async.Task[int]().Context(ctx).
    Bounded(1000).
    DefaultBounded().
    Go(fn)
```

> **⚠️ Bounded 限制的是同时运行任务数**
>
> `Bounded(max)` 通过内部信号量控制最大并发 goroutine 数。`max <= 0` 时使用默认 IO 并发度；`DefaultBounded()` 清除限制。Bounded 限制的是同时运行的任务数，不是总任务数。保留大量 `*AsyncResult[T]` 仍会占用内存。

---

## WaitContext（带 Context 等待）

`WaitContext` 允许调用方通过外部 `context.Context` 控制等待行为：

```go
ar := async.Task[string]().Context(ctx).
    Go(func(ctx context.Context) (string, error) {
        time.Sleep(5 * time.Second)
        return "done", nil
    })

// 通过 context 控制等待
waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

val, err, ok := ar.WaitContext(waitCtx)
if !ok {
    fmt.Println("等待超时") // ok=false，val 为零值
}
```

> `WaitContext` 只控制本次等待，不取消任务。任务完成后仍可通过 `Wait` 获取缓存的结果。

### 调用者主动取消模式

结合 `context.WithCancel` 外部取消 + `WaitContext` 兜底等待：

```go
// ① 调用方掌控取消
parentCtx, cancel := context.WithCancel(context.Background())

// ② 异步任务绑定 parentCtx
ar := async.Task[int]().Context(parentCtx).
    WithTimeout(10 * time.Second).
    Go(func(ctx context.Context) (int, error) {
        select {
        case <-ctx.Done():
            return 0, ctx.Err()     // 收到取消信号
        case <-time.After(30 * time.Second):
            return 42, nil
        }
    })

// ③ 任意位置主动取消
go func() {
    time.Sleep(100 * time.Millisecond)
    cancel() // ← 传播到任务 ctx
}()

// ④ WaitContext 兜底（防止取消传播异常导致永久阻塞）
waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
defer waitCancel()
val, err, ok := ar.WaitContext(waitCtx)
// cancel() 触发 → 任务 ctx.Done() → 任务返回 → WaitContext 正常结束
```

### 限制等待时间

通过 `WaitTimeout` 或 `WaitContext` + `context.WithTimeout` 限制等待时间：

```go
// 方式一：WaitTimeout 直接传超时
val, err, ok := ar.WaitTimeout(100 * time.Millisecond)

// 方式二：WaitContext + context.WithTimeout
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()
val, err, ok := ar.WaitContext(ctx)
```

> 等待超时后 `ok=false`，`val` 为零值；任务不会因此自动取消，也不会返回部分结果。使用任务的取消函数或取消传入任务的 context，并确保回调响应取消。

---

## BoundedRunner（限流执行器）

BoundedRunner 通过内部信号量控制最大并发 goroutine 数。公开的 `TaskBuilder.Bounded(n)` 会创建执行器；复用同一个 builder 提交任务即可共享并发上限。

### 创建方式

```go
// 方式一：链式构建器（推荐）
runner := async.NewBoundedRunnerBuilder().Context(ctx).Max(1000).Build()

// 用 TaskBuilder 创建并复用限流器
builder := async.Task[int]().Context(ctx).Bounded(1000)
```

### BoundedRunnerBuilder 链式方法速查表

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `NewBoundedRunnerBuilder()` | 入口 | — |
| `.Context(ctx)` | 设置构建器上下文并注入 TraceID；`Build()` 不将其传给执行器 | `context.Background()` |
| `.Max(n)` | 最大并发 goroutine 数（≤0 使用 `IO()`） | `IO()` |
| `.Logger(l)` | 注入自定义日志（全局生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |
| `.Build()` | 创建 `*BoundedRunner` | — |

### BoundedRunner 实例方法

| 方法 | 签名 | 说明 |
|------|------|------|
| `Max()` | `int` | 返回最大并发 goroutine 数 |
| `Available()` | `int` | 返回当前可用的并发槽位数 |
| `Busy()` | `int` | 返回当前正在执行任务的 goroutine 数 |

### 配合 Task 使用（独立复用）

```go
// 复用同一个 builder 共享限流器
builder := async.Task[int]().Context(ctx).Bounded(1000)

for i := 0; i < 10_000_000; i++ {
    idx := i
    builder.Go(func(ctx context.Context) (int, error) {
        return process(ctx, idx)
    })
}
```

`NewBoundedRunnerBuilder` 构建的执行器可用于查看 `Max`、`Available` 和 `Busy`；当前公开的 TaskBuilder 接受并发数，并不接受已有执行器。

> **⚠️ ctx 响应要求**
>
> `fn` 必须正确响应 `ctx.Done()` 以按时释放信号量槽位。如果 `fn` 忽略 ctx 取消而持续阻塞（例如未在 HTTP 请求中传递 ctx），对应的信号量槽位将永久占用，最终耗尽所有槽位导致 BoundedRunner 完全阻塞。建议在 `fn` 内部所有 I/O 操作中传递 `ctx`，或设置合理的业务超时。

> **⚠️ 内存注意**
>
> BoundedRunner 限制的是**同时运行**的 goroutine 数，不是总任务数。一次性提交千万个任务需要存储等量的 `*AsyncResult[T]`，注意内存占用。

---

## 默认值体系

Task 的默认值遵循两层架构：全局级 → Builder 级。

### 一层：全局默认值

| 默认值 | 获取函数 | 修改函数 | 默认值 | 说明 |
|--------|----------|----------|--------|------|
| IO 并发度 | `core.IO()` | 不可修改 | **`runtime.NumCPU()×2`** | 无直接使用（Task 单任务模式） |

### 二层：Builder 级默认值

| `Default*` 方法 | 源码行为 | 适用 Builder |
|-----------------|----------|-------------|
| `.DefaultTimeout()` | `p.withTimeout = 0`（清除超时限制） | Task[T], TaskVoid |
| `.DefaultBounded()` | `p.bounded = 0`（清除并发限制） | Task[T], TaskVoid |
| `.DefaultLogger()` | `core.SetLogger(nil)`（恢复全局静默日志） | Task[T], TaskVoid |

### 默认值覆盖优先级

```
全局默认值
    ↓ 被覆盖
Builder 级显式设置（.WithTimeout(d), .Bounded(n) 等）
    ↓ 覆盖
Go/GoResult/GoAct/GoResultAct 执行
```

---

## 生产环境使用建议

### Task 模式选型

| 场景 | 推荐 | 理由 |
|------|------|------|
| 简单的"发射后不管"异步任务 | `.Go(fn)` | 返回 AsyncResult，Wait() 获取结果 |
| 需要在任意时刻取消的任务 | `.GoResult(fn)` | 返回 TaskHandle，支持 Cancel() |
| 只关心 error 不关心返回值 | `.GoAct(fn)` | 无泛型值，代码更简洁 |
| 定时检查/delay 检查场景 | `.Go(fn)` + `WaitCh()` | 非阻塞轮询完成状态 |

### 超时配置

```go
// 推荐：始终设置超时，防止 goroutine 泄漏
ar := async.Task[Data]().Context(ctx).
    WithTimeout(30 * time.Second).   // 单任务兜底超时
    Bounded(100).                    // 生产环境建议限制并发
    Go(func(ctx context.Context) (Data, error) {
        return callExternalAPI(ctx, req)
    })

data, err := ar.Wait()
```

### Bounded 并发限制建议

| 任务类型 | 推荐 Bounded | 理由 |
|----------|-------------|------|
| IO 密集型（网络/DB） | `NumCPU × 4` | 避免连接数暴增 |
| CPU 密集型（计算） | `NumCPU` | 避免 CPU 争抢 |
| 调用三方 API（有 QPS 限制） | 按下游限制值 | 避免触发下游限流 |

### AsyncResult vs TaskHandle 选择

```go
// AsyncResult: 简单场景，只需 Wait 等待结果
ar := async.Task[string]().Context(ctx).
    WithTimeout(5 * time.Second).
    Go(longRunningWork)

// 最多等待 5 秒；等待超时本身不会取消任务。上面的 WithTimeout
// 单独控制任务 context，任务函数需要检查 context 才能响应取消。
val, err, completed := ar.WaitTimeout(5 * time.Second)
if !completed {
    // 可稍后再次等待结果
} else {
    _ = val
    _ = err
}

// TaskHandle: 需要随时取消的场景
t := async.Task[string]().Context(ctx).
    WithTimeout(10 * time.Second).
    GoResult(longRunningWork)

// 用户终止请求时取消
go func() {
    <-userCancelCh
    t.Cancel()
}()

result, err := t.Result()
```

### 生产推荐配置

```go
// ========== 基础异步任务：生产标准配置 ==========
ar := async.Task[Data]().Context(ctx).
    WithTimeout(30 * time.Second).      // ⚠️ 必须设置兜底超时，防止 goroutine 泄漏
    Bounded(100).                       // ⚠️ 必须限制并发，防止连接数/Goroutine 爆炸
    Go(func(ctx context.Context) (Data, error) {
        return callExternalAPI(ctx, req)
    })

data, err := ar.Wait()
if err != nil {
    return fmt.Errorf("task failed: %w", err)
}

// ========== 可取消任务：支持主动终止 ==========
t := async.Task[*Order]().Context(ctx).
    WithTimeout(60 * time.Second).      // 长耗时任务加大超时
    Bounded(200).                       // IO 密集型可适当放宽
    GoResult(func(ctx context.Context) (*Order, error) {
        return paymentService.Process(ctx, orderID)
    })

// 超时自动取消
go func() {
    select {
    case <-time.After(10 * time.Second):
        t.Cancel()
    case <-ctx.Done():
        t.Cancel()
    }
}()

order, err := t.Result()

// ========== 千万元素批量异步：BoundedRunner ==========
builder := async.Task[int]().Context(ctx).Bounded(1000)
results := make([]*async.AsyncResult[int], 0, 10_000_000)
for i := 0; i < 10_000_000; i++ {
    idx := i
    results = append(results, builder.Go(func(ctx context.Context) (int, error) {
        return process(ctx, idx)
    }))
}
for _, ar := range results {
    val, err := ar.Wait()
    _ = val; _ = err
}

// ========== Fire-and-Forget：发射后不管 ==========
async.TaskVoid().Context(ctx).
    WithTimeout(5 * time.Second).
    GoAct(func(ctx context.Context) error {
        return metrics.Report(ctx, event)
    })
// 不调用 Wait()——适合日志、监控等非关键路径
```

### 常见错误

- **❌ 任务忽略 context**：取消或超时仅通知任务，任务函数仍需检查 context 并主动退出
- **❌ 无限期等待**：需要限制等待时间时使用 `WaitTimeout` 或 `WaitContext`；任务超时也需要函数配合处理
- **❌ 轮询时反复创建 WaitCh**：每次调用会创建等待 goroutine，应复用同一个 channel；`WaitCh` 与 `Wait` 可以混用
- **❌ Bounded 误解**：Bounded 限制的是**同时执行**的任务数，不是总任务数；一次性大量创建 AsyncResult 对象仍会占用内存

---

## 自定义日志

```go
// 注入自定义日志
async.Task[int]().
    Logger(myLogger).    // 全局生效
    Go(fn)

// 恢复默认日志（静默）
async.Task[int]().
    Logger(myLogger).
    DefaultLogger().     // 全局恢复
    Go(fn)
```

---

## Mu 并发安全切片

`async.Mu[T]` 是一个轻量级泛型并发安全切片容器，适用于多个 goroutine 并发收集结果的场景。内部使用 `sync.Mutex` 保护底层切片，提供 `Append`（追加）和 `Snapshot`（快照）两个核心方法。

> **⚠️ Mu 不是协程池/任务组的替代品**
>
> `Mu[T]` 只负责安全地并发追加和读取数据，不管理 goroutine 生命周期。无法设置并发度、超时、FailFast 等参数。需要这些能力请使用 Pool 或 Group。

### 创建

```go
var mu async.Mu[string]     // 零值即可使用，无需初始化
var intMu async.Mu[int]
```

### Append

线程安全地追加元素。参数 `add` 是一个工厂函数，在锁外执行以保证并发性能，`append` 操作在锁内执行保证线程安全。

| 参数 | 类型 | 说明 |
|------|------|------|
| `add` | `func() T` | 生成要追加元素的工厂函数（在锁外执行） |

```go
var mu async.Mu[int]

mu.Append(func() int { return 42 })
mu.Append(func() int { return computeSomething() })
```

### Snapshot

返回当前所有元素的副本，线程安全。每次调用都会分配新切片，原数据不受后续 `Append` 影响。

```go
items := mu.Snapshot()
for _, item := range items {
    fmt.Println(item)
}
```

### 完整示例

```go
var mu async.Mu[string]
var wg sync.WaitGroup

for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(n int) {
        defer wg.Done()
        mu.Append(func() string {
            return fmt.Sprintf("result-%d", n)
        })
    }(i)
}

wg.Wait()
all := mu.Snapshot() // 获取所有结果的副本
fmt.Printf("收集到 %d 条结果\n", len(all))
```
