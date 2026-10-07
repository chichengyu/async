# Task（异步任务）文档

## 概述

Task 提供单个异步任务的执行能力。通过 `Task[T]()` / `TaskVoid()` Builder 链式构建。

**四种模式对照表**：

| 模式 | Entry | 终端方法 | 返回值类型 | 特点 |
|------|-------|----------|-----------|------|
| 带返回值异步 | `Task[T]()` | `.Go(fn)` | `*AsyncResult[T]` | `Wait()` 获取值+error |
| 无返回值异步 | `TaskVoid()` | `.GoAct(fn)` | `*AsyncResult[struct{}]` | 只关心 error |
| 可取消+返回值 | `Task[T]()` | `.GoResult(fn)` | `Task[T]` | 支持 `Cancel()` |
| 可取消+无返回值 | `TaskVoid()` | `.GoResultAct(fn)` | `Task[struct{}]` | 可取消+只关心 error |

> **⚠️ GoResult 必须 Wait/Cancel**
>
> `GoResult` 返回 `Task[T]`（即 `*TaskHandle[T]`），内部启用了独立的 goroutine。必须调用 `Result()` 或 `Cancel()`，否则 goroutine 泄漏。
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
| `.GoAct(fn)` | `func(ctx) error` | `*AsyncResult[struct{}]` | 启动无返回值异步任务 |
| `.GoResultAct(fn)` | `func(ctx) error` | `Task[struct{}]` | 启动可取消无返回值异步任务 |

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
| `Error()` | `error` | 获取错误（不阻塞） |
| `WaitCh()` | `<-chan struct{}` | 返回完成通知 channel |
| `Cancel()` | — | 取消任务 |
| `Ok()` | `bool` | 是否成功（无错误、无 panic） |
| `IsPanic()` | `bool` | 是否因 panic 失败 |
| `WaitTimeout(d)` | `(T, error, bool)` | 带超时等待，第三个返回值 `false` 表示超时 |

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

err := ae.Wait()  // 阻塞等待，只返回 error
```

> **⚠️ TaskVoid 的实质**
>
> `TaskVoid()` 等价于 `Task[struct{}]()`，返回值是 `*AsyncResult[struct{}]`。`Wait()` 返回 `(struct{}, error)`，通常只取第二个返回值。

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

// 获取结果（已 Cancel 则返回 context.Canceled）
result, err := t.Result()
```

`Task[T]`（即 `*TaskHandle[T]`）提供以下方法：

| 方法 | 签名 | 说明 |
|------|------|------|
| `Cancel()` | — | 取消异步任务（关闭内部 context） |
| `Result()` | `(T, error)` | 获取结果（幂等，内部缓存） |
| `Ctx()` | `context.Context` | 获取内部 context |
| `Error()` | `error` | 获取错误（不阻塞） |

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
err := tk.Result()  // 返回 error
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
errOnly := ar.Error()    // 仅获取 error（不阻塞）
```

### WaitCh / Cancel

```go
ar := async.Task[string]().Context(ctx).
    Go(func(ctx context.Context) (string, error) {
        time.Sleep(10 * time.Second)
        return "result", nil
    })

// 非阻塞轮询：
for {
    select {
    case <-ar.WaitCh():
        val, err := ar.Wait()
        return val, err
    case <-time.After(500 * time.Millisecond):
        if shouldAbort() {
            ar.Cancel()
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
innerCtx := t.Ctx()

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
> `WithTimeout` 设置后，`GoResult` 返回的 `Task[T]` 内部 context 会包含该超时。调用 `Cancel()` 或超时触发后，`Result()` 返回 `context.DeadlineExceeded`。

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
> `Bounded(max)` 通过内部信号量控制最大并发 Goroutine 数。`max <= 0` 时无限制。注意：Bounded 限制的是**同时运行**的任务数，不是总任务数。一次性提交大量任务时，`*AsyncResult[T]` 对象仍会全部创建，注意内存占用。

---

## BoundedRunner（限流执行器）

BoundedRunner 是独立的限流执行器，通过内部信号量控制最大并发 goroutine 数。与 `.Bounded(n)` 内联不同，它可以**提前创建并跨多个 Task 复用**，适合千万级高并发场景。

### 创建方式

```go
// 方式一：链式构建器（推荐）
runner := async.NewBoundedRunnerBuilder().Context(ctx).Max(1000).Build()

// 方式二：直接创建
runner := async.NewBoundedRunner(1000)

// 方式三：使用默认并发度
runner := async.NewDefaultBoundedRunner()
```

### BoundedRunnerBuilder 链式方法速查表

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `NewBoundedRunnerBuilder()` | 入口 | — |
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `context.Background()` |
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
// 创建后跨多个 Task 复用
runner := async.NewBoundedRunnerBuilder().Max(1000).Build()

for i := 0; i < 10_000_000; i++ {
    idx := i
    async.Task[int]().Context(ctx).
        Bounded(runner).
        Go(func(ctx context.Context) (int, error) {
            return process(ctx, idx)
        })
}
```

### 独立使用（不依赖 Task Builder）

BoundedRunner 也可以独立使用，通过包级函数 `BoundedGo` / `BoundedGoAct` / `BoundedGoResult`：

| 函数 | 签名 | 说明 |
|------|------|------|
| `BoundedGo[T](r, ctx, fn)` | `*AsyncResult[T]` | 限流后启动带返回值异步任务 |
| `BoundedGoAct(r, ctx, fn)` | `*AsyncResult[struct{}]` | 限流后启动无返回值异步任务 |
| `BoundedGoResult[T](r, ctx, fn)` | `Task[T]` | 限流后启动可取消异步任务 |

```go
runner := async.NewBoundedRunner(1000)

results := make([]*async.AsyncResult[int], 0, 10000)
for i := 0; i < 10000; i++ {
    idx := i
    ar := async.BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
        return heavyCompute(ctx, idx)
    })
    results = append(results, ar)
}

for _, ar := range results {
    val, err := ar.Wait()
    fmt.Println(val)
}
```

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

// 5 秒后检查是否完成
select {
case <-ar.WaitCh():
    val, err := ar.Wait()
default:
    ar.Cancel() // 超时取消
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
runner := async.NewBoundedRunnerBuilder().Max(1000).Build()
var wg sync.WaitGroup
for i := 0; i < 10_000_000; i++ {
    idx := i
    wg.Add(1)
    go func() {
        defer wg.Done()
        ar := async.Task[int]().Context(ctx).
            Bounded(runner).            // 全局信号量限流，最多 1000 并发
            Go(func(ctx context.Context) (int, error) {
                return process(ctx, idx)
            })
        val, err := ar.Wait()
        _ = val; _ = err
    }()
}
wg.Wait()

// ========== Fire-and-Forget：发射后不管 ==========
async.TaskVoid().Context(ctx).
    WithTimeout(5 * time.Second).
    GoAct(func(ctx context.Context) error {
        return metrics.Report(ctx, event)
    })
// 不调用 Wait()——适合日志、监控等非关键路径
```

### 常见错误

- **❌ GoResult 不调用 Result/Cancel**：`GoResult` 内部启用了独立 goroutine，必须调用 `Result()` 或 `Cancel()`，否则 goroutine 泄漏
- **❌ 无限期 Wait 不设超时**：建议始终设置 `WithTimeout` 兜底
- **❌ WaitCh 与 Wait 混用导致竞态**：`WaitCh` 只通知完成状态，不要同时依赖 Wait 的阻塞语义
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