# Task（异步任务）文档

## 概述

Task 提供单个异步任务的执行能力。通过 `Task[T]()` / `TaskVoid()` Builder 链式构建。

**四种模式对照表**：

| 模式 | Entry | 终端方法 | 返回值类型 | 特点 |
|------|-------|----------|-----------|------|
| 带返回值异步 | `Task[T]()` | `.Go(fn)` | `*async.AsyncResult[T]` | `Wait()` 获取值+error |
| 无返回值异步 | `TaskVoid()` | `.GoAct(fn)` | `*async.AsyncErr` | 只关心 error |
| 可取消+返回值 | `Task[T]()` | `.GoResult(fn)` | `async.TaskHandle[T]` | 支持 `Cancel()` |
| 可取消+无返回值 | `TaskVoid()` | `.GoResultAct(fn)` | `async.TaskErr` | 可取消+只关心 error |

> **GoResult 与取消**
>
> `GoResult` 返回 `async.TaskHandle[T]`。任务函数返回后，即使不调用 `Result()` 或 `Cancel()`，结果发送也不会阻塞；取消只发出 context 信号，任务函数需要配合退出。
>
> **⚠️ ctx 自动注入 TraceID**
>
> Task 的 Context 设置及任务启动入口会调用 `EnsureTraceID(ctx)`；结果等待、查询与 Mu 等方法不负责注入 TraceID。向业务函数传递这些入口返回或提供的 context。

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
| `.Bounded(max)` | 限制同时持有许可的业务回调数 | 无限制 |
| `.DefaultBounded()` | 清除并发限制 | — |
| `.Logger(l)` | 设置当前构建器的日志器（仅局部生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |

### 终端方法

| 方法 | fn 签名 | 返回值 | 说明 |
|------|---------|--------|------|
| `.Go(fn)` | `func(ctx) (T, error)` | `*async.AsyncResult[T]` | 启动带返回值异步任务 |
| `.GoResult(fn)` | `func(ctx) (T, error)` | `async.TaskHandle[T]` | 启动可取消异步任务 |
| `.GoAct(fn)` | `func(ctx) error` | `*async.AsyncErr` | 启动无返回值异步任务 |
| `.GoResultAct(fn)` | `func(ctx) error` | `async.TaskErr` | 启动可取消无返回值异步任务 |

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
| `WaitCh()` | `<-chan async.Result[T]` | 每次调用创建桥接 goroutine；channel 收到一个结果后关闭，可用于 select |
| `Cancel()` | `(T, error)` | 检查结果并最多短暂等待约 1ms；仍未就绪时返回 `context.Canceled`，不会取消任务 |
| `Ok()` | `bool` | 阻塞等待完成，判断结果错误是否为 nil |
| `IsPanic()` | `bool` | 阻塞等待完成，判断错误链是否包含 panic 错误 |
| `WaitTimeout(d)` | `(T, error, bool)` | 带超时等待，第三个返回值 `false` 表示超时 |
| `WaitContext(ctx)` | `(T, error, bool)` | 带 Context 控制等待，`false` 表示 ctx 取消 |

`WaitTimeout` 超时返回 `(T 的零值, async.ErrTimeout, false)`；`WaitContext` 等待上下文结束时返回 `(T 的零值, ctx.Err(), false)`。两者只结束本次等待，不取消任务；后台共享等待者会继续等任务返回并缓存结果。若任务一直不返回，这个等待者也会继续存在。

调用时若结果已经就绪，会先返回结果，即使等待上下文已经取消或超时时间不为正；若结果与超时或取消在等待期间同时就绪，`select` 的选择不保证固定顺序。`ok == true` 只表示取得了任务结果，仍须检查 `err`。

```go
ar := async.Task[string]().Context(ctx).
    Go(func(ctx context.Context) (string, error) {
        return fetchData(ctx)
    })

// 非阻塞检查这个通知通道。WaitCh 每次调用会创建一个桥接 goroutine；
// 即使任务已经完成，桥接 goroutine 尚未发送时也可能走 default。
// 若任务尚未完成，桥接 goroutine 会等到任务最终返回后退出。
var val string
var err error
select {
case <-ar.WaitCh():
    val, err = ar.Wait()
default:
    fmt.Println("当前没有可立即读取的完成通知")
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

使用 `Task[T]()` 的 `.GoResult(fn)` 启动可取消任务，返回 `async.TaskHandle[T]`：

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

`async.TaskHandle[T]` 提供以下字段和方法：

| 方法 | 签名 | 说明 |
|------|------|------|
| `Cancel` | `context.CancelFunc` | 取消内部 context，调用方式为 `t.Cancel()` |
| `Result` | `func() (T, error)` | 阻塞获取结果（幂等，内部缓存） |
| `Ctx` | `context.Context` | 可取消的外层任务 context；不包含 Builder 的回调超时，不是完成通知 |
| `Error()` | `error` | 阻塞等待并获取错误 |

由这些启动入口创建的 `TaskHandle` / `TaskErr` 可并发重复调用 `Result()`；复制句柄值后仍共享同一次执行和缓存。`Cancel()` 发出取消信号，不会用 `context.Canceled` 覆盖业务函数实际返回的值与错误；业务忽略取消并正常返回时，`Result()` 仍返回该正常结果。包含指针、切片或 map 的返回值只共享缓存引用，不提供深拷贝，调用方后续修改须自行同步。应保留构造器提供的函数字段，使用后调用 `Cancel()` 释放派生 context。

```go
t := async.Task[*Order]().Context(ctx).
    GoResult(func(ctx context.Context) (*Order, error) {
        return paymentService.Process(ctx, orderID)
    })

defer t.Cancel() // 释放派生 context；任务函数仍须协作响应取消
// 10 秒后发出取消信号；任务提前完成时停止尚未触发的定时器
cancelTimer := time.AfterFunc(10*time.Second, t.Cancel)
defer cancelTimer.Stop()

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

`AsyncResult` / `AsyncErr` 必须通过启动任务的入口创建，零值不可用；应共享返回的指针，不要复制包含锁与缓存状态的句柄。以下等待与查询方法可以并发调用。

结果缓存只按 Go 的值语义返回 `T`，不深拷贝切片、map 或指针。`Wait`、`Values`、成功取得结果的 `WaitTimeout`/`WaitContext`、取得结果的 `Cancel` 及各次 `WaitCh` 通知都保留这些引用；业务返回非 nil 错误时，直接任务入口仍保留同时返回的值。结构体结果的普通字段和切片头是副本，给某次返回值的字段重新赋值不会替换缓存字段；但修改切片元素、map 条目或指针目标会影响其他读取者看到的内容。长度为零而容量非零的切片也可能共享底层数组。

生产中应把发布后的结果视为只读；需要修改时自行按业务结构复制或同步，缓存锁不保护引用对象内部。下例在任务完成后顺序修改切片元素，第二次等待可见修改；若有并发读取者，须先协调所有者或另作副本。

```go
ar := async.Task[[]int]().Go(func(context.Context) ([]int, error) {
    return []int{1, 2}, nil
})
first, err := ar.Wait()
if err != nil {
    panic(err)
}
first[0] = 9
again, err := ar.Wait()
if err != nil {
    panic(err)
}
fmt.Println(again) // [9 2]
```

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

两个方法均无参数。`WaitCh()` 每次创建一个容量为 1 的独立通知通道及 goroutine；业务完成后发送一次缓存结果并关闭。多次调用可分别接收同一结果，但通知仍需 goroutine 调度。生产中应保存一次调用返回的通道供 `select` 使用，读取时检查 `ok`；关闭后的零值不是新的成功结果。停止接收不取消业务，业务始终不返回时，通知 goroutine 也会继续等待。

`Cancel()` 优先返回已缓存或可立即读到的实际结果；否则创建 1ms 定时器，等待结果或定时器。定时分支返回 `(类型零值, context.Canceled)`，只结束本次等待，不会把取消错误写入结果缓存，也不会取消业务 context 或提前释放 BoundedRunner 的业务许可。1ms 是当前实现的等待窗口，不是包含调度、锁等待在内的硬性返回时限；结果与定时器同时就绪时不保证分支顺序。业务本身也可能返回 `context.Canceled`，需要区分来源时使用带 `ok` 的 `WaitContext` / `WaitTimeout`。

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

#### Cancel 后仍可获取结果

```go
release := make(chan struct{})
ar := async.Task[int]().Go(func(context.Context) (int, error) {
    <-release
    return 42, nil
})

_, waitErr := ar.Cancel() // 业务被屏障阻塞，本次等待返回 context.Canceled
close(release)
value, businessErr := ar.Wait() // 仍返回 42, nil
fmt.Println(errors.Is(waitErr, context.Canceled), value, businessErr)
```

该示例用屏障明确区分等待结束和业务完成。生产业务须由其所有者持有并取消任务 context，同时安排最终收尾；不要把 `AsyncResult.Cancel()` 当作中止任务或完成通知。

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

参数 `timeout` 是本次等待的定时时长；非正值创建立即到期的定时器，但已缓存或可立即读到的结果仍优先返回。末尾 `ok=true` 表示取得业务结果，业务成功与否须另查 `err`；`ok=false` 返回类型零值与 `async.ErrTimeout`。超时不取消业务，不覆盖最终结果，也不提前释放业务许可；结果和定时器在阻塞等待阶段同时就绪时不保证选择顺序。生产中应给业务 context 和调用方等待分别设置预算，并安排超时后的业务收尾。

```go
ar := async.Task[string]().Context(ctx).
    Go(func(ctx context.Context) (string, error) {
        time.Sleep(5 * time.Second)
        return "done", nil
    })

val, err, ok := ar.WaitTimeout(2 * time.Second)
if !ok {
    fmt.Println("超时")  // ok=false, val 为零值, err 为 async.ErrTimeout
}
```

### WaitContext

参数 `ctx` 必须非 nil，仅控制本次等待；取消分支返回 `(类型零值, ctx.Err(), false)`。已经缓存或可以立即读到的结果优先，即便等待 context 已取消；进入阻塞等待后，取消和结果同时就绪时不保证顺序。`ok=true` 时仍须检查业务错误；业务返回 `context.Canceled` 或 `context.DeadlineExceeded` 也属于取得结果。生产中保存等待 context 的取消函数并及时释放，业务停止仍须通过传给任务的 context 协作实现。

```go
ar := async.Task[int]().Go(func(context.Context) (int, error) {
    return 42, nil
})
_, _ = ar.Wait() // 建立缓存，便于明确演示优先级
waitCtx, cancelWait := context.WithCancel(context.Background())
cancelWait()
value, err, ok := ar.WaitContext(waitCtx) // 42, nil, true
fmt.Println(value, err, ok)
```

---

## TaskHandle 方法详解

### Ctx / Result / Cancel

```go
t := async.Task[string]().Context(ctx).
    WithTimeout(5 * time.Second).
    GoResult(func(ctx context.Context) (string, error) {
        // 此处 ctx 是 WithTimeout 子 context；Cancel 取消其父任务 context
        return slowWork(ctx)
    })

// TaskHandle.Ctx 是可由 Cancel 取消的任务 context；Builder.WithTimeout 创建的子 context
// 只传给回调，不暴露在 t.Ctx 上。
innerCtx := t.Ctx

// 等待结果
result, err := t.Result()

// 或主动取消
t.Cancel()
```

`t.Ctx.Done()` 不作为任务完成通知：回调正常返回、报错或 panic 后，外层 context 仍可保持有效，直到 `Cancel()` 或父 context 结束。相反，外层已取消也不表示回调已返回。需要确认结束时调用 `Result()` 或 `Error()`，并在使用后调用 `Cancel()`。正数 Builder 超时创建的回调子 context 会在回调退出时立即取消；不要让需要继续工作的子 goroutine 依赖它。

---

## 超时控制

通过 `.WithTimeout(d)` 设置任务超时：

`d > 0` 时，在业务回调开始执行时派生期限，`d <= 0` 及 `DefaultTimeout()` 不设置此期限，父期限仍然生效。启用 `Bounded` 后，终端调用会先在调用方等待空位；这段等待不计入 `d`，且句柄要取得许可或父 context 结束后才返回。若生产请求需要约束等待空位和业务的总耗时，应通过 `Context(parent)` 传入有期限的父 context；仅在句柄返回后调用 `WaitTimeout` 不能限制此前的提交阻塞。

超时只改变回调收到的 context，不能强制结束业务、提前回收许可或自动把成功改为超时错误。回调须检查 context，并按业务要求返回 `ctx.Err()`；忽略取消后返回的值与 nil 错误仍会缓存为成功结果。四种终端 `Go`、`GoAct`、`GoResult`、`GoResultAct` 均遵循这项约定。调用终端时捕获的超时不会被后续顺序修改 Builder 改写；仍不可在终端调用尚未返回时并发修改 Builder。

```go
// 5 秒超时
ar := async.Task[string]().Context(ctx).
    WithTimeout(5 * time.Second).
    Go(func(ctx context.Context) (string, error) {
        // ctx 会在 5 秒后自动取消
        return slowWork(ctx)
    })

val, err := ar.Wait()

// 清除 Builder 超时设置（父 context 自带的 deadline 仍然有效）
withoutTimeout := async.Task[string]().Context(ctx).
    WithTimeout(5 * time.Second).  // 先设置
    DefaultTimeout().               // 再清除
    Go(fn)
_, _ = withoutTimeout.Wait() // 根据业务处理返回值和错误
```

> **⚠️ WithTimeout 与 GoResult 配合使用**
>
> `WithTimeout` 在回调开始执行时创建超时子 context，并将它传给回调；`GoResult` 返回的 `TaskHandle.Ctx` 不包含这个子超时。Bounded 任务在获得槽位前等待时也尚未创建该超时 context。超时只通知回调，回调必须响应取消才能及时返回；`Result()` 返回回调实际返回的值和错误。

---

## 限流控制（Bounded）

通过 `.Bounded(max)` 限制同时持有许可的业务回调数：

```go
// 限制最多 100 并发
ar := async.Task[int]().Context(ctx).
    Bounded(100).
    Go(func(ctx context.Context) (int, error) {
        return process(ctx)
    })

// 清除并发限制
unbounded := async.Task[int]().Context(ctx).
    Bounded(1000).
    DefaultBounded().
    Go(fn)
_, _ = ar.Wait()
_, _ = unbounded.Wait() // 等待两个示例任务结束，并按业务处理错误
```

> **⚠️ Bounded 限制的是同时运行任务数**
>
> `Bounded(max)` 通过内部信号量控制同时持有许可的业务回调数。`max <= 0` 时使用默认 IO 并发度；`DefaultBounded()` 清除限制。不限制调用方为等待提交而创建的 goroutine、业务子 goroutine 或结果投递收尾。获取槽位发生在终端方法调用期间，因此并发达到上限时 `Go`/`GoResult` 等调用可能阻塞；保留大量 `*AsyncResult[T]` 仍会占用内存。

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

> `WaitContext` 只控制本次等待，不取消任务。已缓存或已到达结果通道的结果会优先返回 `(value, taskErr, true)`，即使等待 context 已取消；否则等待中选择取消时返回 `(zero, ctx.Err(), false)`。结果就绪和取消同时发生时不能依赖固定选择，任务完成后仍可通过 `Wait` 获取缓存结果。传入非 nil context。

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
val, err, ok = ar.WaitContext(ctx)
```

> 等待超时后 `ok=false`，`val` 为零值；任务不会因此自动取消，也不会返回部分结果。使用任务的取消函数或取消传入任务的 context，并确保回调响应取消。

---

## BoundedRunner（限流执行器）

BoundedRunner 通过内部信号量限制同时占用许可的业务回调数。公开的 `TaskBuilder.Bounded(n)` 会创建执行器；复用同一个 builder 提交任务即可共享并发上限。

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
| `.Max(n)` | 业务许可容量（≤0 使用 `IO()`） | `IO()` |
| `.Logger(l)` | 设置当前构建器的日志器（仅局部生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |
| `.Build()` | 创建 `*BoundedRunner` | — |

### BoundedRunner 实例方法

取得槽位后，trace 日志注入仍在提交方同步执行；自定义 logger 的 `With` 或 `WithContext` 若 panic，原 panic 向外传播；若调用 `runtime.Goexit`，会结束提交 goroutine。两种情况都不返回任务句柄、不执行业务，并由提交方 defer 归还已取得的槽位。成功启动后改由业务回调退出时释放，普通返回、业务 panic 和 Goexit 都不遗漏槽位；回调仍在运行时，终端方法返回不会提前释放。日志实现应并发安全、及时返回且不主动异常退出。可执行示例见 [Example_boundedStartupRecovery](tests/public_examples_test.go)，四入口及正常交接契约见 [启动清理测试](../internal/task/bounded_startup_cleanup_test.go)。

业务回调调用 `runtime.Goexit` 时，Go/GoAct/GoResult/GoResultAct 及其有界、构建器入口记录类型零值与 `async.ErrTaskGoexit`，完成结果通道并供所有等待者缓存读取；它不是 PanicError，也不重新执行业务。有界许可和超时子 context 仍在业务退出时清理；Task.Cancel 仍由调用方管理。生产业务应正常返回 error 表示失败，此保证不覆盖启动前日志注入异常、进程退出或永久阻塞。

| 方法 | 签名 | 说明 |
|------|------|------|
| `Max()` | `int` | 返回业务许可容量；不代表进程 goroutine 数上限 |
| `Available()` | `int` | 返回当前可用的并发槽位数 |
| `Busy()` | `int` | 返回当前已占用槽位数，包含已取得槽位但 goroutine 尚未开始执行的任务 |

### 配合 Task 使用（独立复用）

```go
// 复用同一个 builder 共享限流器
builder := async.Task[int]().Context(ctx).Bounded(100)
results := make([]*async.AsyncResult[int], 0, 256)

for i := 0; i < 256; i++ {
    idx := i
    results = append(results, builder.Go(func(ctx context.Context) (int, error) {
        return process(ctx, idx)
    }))
}
// 等待所有已提交任务后再返回首个错误，避免遗漏本批任务的收尾
var firstErr error
for _, ar := range results {
    value, err := ar.Wait()
    if err != nil {
        if firstErr == nil { firstErr = err }
        continue
    }
    handle(value)
}
if firstErr != nil { return firstErr }
```

这里保留最多 256 个句柄，并读取每个结果；更大输入应按后文生产示例分批处理。此示例位于返回 `error` 的业务函数中，`process` 应响应传入的 context。

`NewBoundedRunnerBuilder` 构建的执行器可用于查看 `Max`、`Available` 和 `Busy`；当前公开的 TaskBuilder 接受并发数，并不接受已有执行器。

等待槽位时父 context 取消会返回带相应错误的结果句柄，业务不执行，也不消耗槽位；调用方仍须读取句柄中的错误。如果空位与取消同时就绪，可能仍取得槽位并启动业务，因此回调必须检查 context，不能依靠提交方取消来保证业务一定未开始。

取得许可后，许可会一直保留到业务回调退出，返回错误或 panic 都会释放；仅取消 context 不会直接释放许可。业务忽略取消并正常返回时，句柄仍保留该业务值及 nil 错误，不自动替换为取消错误。不得在已占用同一执行器许可的回调中同步提交并等待下一项：许可耗尽且提交 context 未取消时会互相等待，并发度为 1 时外层自身就占用了全部许可；生产中应将依赖编排放在外层，或为嵌套工作使用独立限额。可运行的取消/复用对照见 [Example_taskBoundedCancellationAndReuse](tests/public_examples_test.go)。

> **⚠️ ctx 响应要求**
>
> `fn` 必须正确响应 `ctx.Done()` 以按时释放信号量槽位。如果 `fn` 忽略 ctx 取消而持续阻塞（例如未在 HTTP 请求中传递 ctx），对应的信号量槽位将永久占用，最终耗尽所有槽位导致 BoundedRunner 完全阻塞。建议在 `fn` 内部所有 I/O 操作中传递 `ctx`，或设置合理的业务超时。

> **⚠️ 内存注意**
>
> BoundedRunner 限制的是持有许可的业务回调数，不限制总任务数、调用方创建的等待提交 goroutine、回调自行创建的子 goroutine，或回调返回后仍在完成结果投递的内部 goroutine。应在固定数量的生产者中同步提交，避免先为全部输入创建 goroutine 再等待许可。保留所有结果句柄也会占用等量句柄内存，应分批读取；丢弃句柄则无法集中等待或检查各任务错误。

---

## 默认值体系

TaskBuilder 创建时默认不设超时，也不限制并发；它不读取全局 SetDefaultTimeout。显式调用 Bounded(n) 且 n 非正时，BoundedRunner 才使用全局 IO() 计算的并发度。

### 一层：全局默认值

| 默认值 | 获取函数 | 修改函数 | 默认值 | 说明 |
|--------|----------|----------|--------|------|
| IO 并发度 | `core.IO()` | 不可修改 | **`runtime.NumCPU()×2`** | 显式 `Bounded(n)` 的 n 非正时采用此值 |

### 二层：Builder 级默认值

| `Default*` 方法 | 源码行为 | 适用 Builder |
|-----------------|----------|-------------|
| `.DefaultTimeout()` | `b.timeout = 0`（清除 Builder 超时设置） | TaskBuilder[T] |
| `.DefaultBounded()` | `b.bounded = nil`（清除并发限制） | TaskBuilder[T] |
| `.DefaultLogger()` | `清除当前覆盖，继承全局日志器`（恢复默认日志实现） | TaskBuilder[T] |

### 配置覆盖顺序

同一配置以后一次调用为准，DefaultTimeout() 清除本构建器的超时，DefaultBounded() 清除并发限制。终端方法执行时读取当前配置；重复使用同一构建器的 BoundedRunner 可以共享并发槽位，但不得在提交期间并发修改构建器。

---

## 生产环境使用建议

### Task 模式选型

| 场景 | 推荐 | 理由 |
|------|------|------|
| 简单的"发射后不管"异步任务 | `.Go(fn)` | 返回 AsyncResult，Wait() 获取结果 |
| 需要在任意时刻取消的任务 | `.GoResult(fn)` | 返回 TaskHandle，支持 Cancel() |
| 只关心 error 不关心返回值 | `.GoAct(fn)` | 无泛型值，代码更简洁 |
| 需要 select 等待完成或取消 | `.Go(fn)` + 单次 `WaitCh()` | 完成通知；重复调用会各自创建桥接 goroutine |

### 超时配置

```go
// 为回调提供超时信号；回调仍须响应 ctx 才能及时退出
ar := async.Task[Data]().Context(ctx).
    WithTimeout(30 * time.Second).   // 单任务超时信号
    Bounded(100).                    // 生产环境建议限制并发
    Go(func(ctx context.Context) (Data, error) {
        return callExternalAPI(ctx, req)
    })

data, err := ar.Wait()
```

### Bounded 并发限制建议

| 任务类型 | 推荐 Bounded | 理由 |
|----------|-------------|------|
| IO 密集型（网络/DB） | 按连接池容量与延迟测量 | 限制同时占用的连接和内存，预留其他调用的容量 |
| CPU 密集型（计算） | 从 `CPU()` 开始测量 | 根据 CPU 配额及任务成本调整 |
| 调用三方 API（有 QPS 限制） | 按允许的同时在途数配置，并另设速率限制 | Bounded 仅限制并发数，短任务仍可能超过每秒请求额度 |

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
defer t.Cancel() // 释放 GoResult 派生的 context

// 用户终止请求时取消
taskDone := make(chan struct{})
go func() {
    select {
    case <-userCancelCh:
        t.Cancel()
    case <-taskDone:
        // 任务已完成，无需一直等待用户取消
    }
}()

result, err := t.Result()
close(taskDone)
```

### 生产推荐配置

该段位于返回 `error` 的函数中，参数 `ctx` 为非 nil 请求 context，`req`、`orderID`、`event` 及 `items` 为应用输入；需要导入 `context`、`fmt`、`time` 和 async。应用提供 `callExternalAPI(context.Context, Req) (Data, error)`、`paymentService.Process(context.Context, OrderID) (*Order, error)`、`process(context.Context, Item) (int, error)`、`handle(int)` 及 `metrics.Report(context.Context, Event) error`；输入类型按业务定义。

```go
// ========== 基础异步任务：生产标准配置 ==========
ar := async.Task[Data]().Context(ctx).
    WithTimeout(30 * time.Second).      // 任务超时信号；回调须响应 ctx
    Bounded(100).                       // 按下游容量设置并发上限
    Go(func(ctx context.Context) (Data, error) {
        return callExternalAPI(ctx, req)
    })

data, err := ar.Wait()
if err != nil {
    return fmt.Errorf("task failed: %w", err)
}

// ========== 可取消任务：支持主动终止 ==========
t := async.Task[*Order]().Context(ctx).
    WithTimeout(60 * time.Second).      // 长耗时任务的超时信号
    Bounded(200).                       // IO 密集型可适当放宽
    GoResult(func(ctx context.Context) (*Order, error) {
        return paymentService.Process(ctx, orderID)
    })
defer t.Cancel()

// 到时主动取消；任务函数需响应 ctx 才能退出。父 ctx 取消也会传给任务。
cancelTimer := time.AfterFunc(10*time.Second, t.Cancel)

order, err := t.Result()
cancelTimer.Stop() // 任务提前完成时停止定时器；已启动的回调仍可能运行
if err != nil {
    return fmt.Errorf("payment failed: %w", err)
}

// ========== 大批量异步：复用 BoundedRunner，并分批回收结果句柄 ==========
builder := async.Task[int]().Context(ctx).Bounded(100)
const batchSize = 256 // 句柄保留量；与同时执行上限分别配置，按业务预算调整
for start := 0; start < len(items); start += batchSize {
    end := min(start+batchSize, len(items))
    results := make([]*async.AsyncResult[int], 0, end-start)
    for _, item := range items[start:end] {
        results = append(results, builder.Go(func(ctx context.Context) (int, error) {
            return process(ctx, item)
        }))
    }
    // 全部等待后再返回错误，避免本批已启动的任务未收尾
    var firstErr error
    for _, ar := range results {
        value, err := ar.Wait()
        if err != nil {
            if firstErr == nil { firstErr = err }
            continue
        }
        handle(value)
    }
    if firstErr != nil { return firstErr }
}

// ========== Fire-and-Forget：发射后不管 ==========
async.TaskVoid().Context(ctx).
    WithTimeout(5 * time.Second).
    GoAct(func(ctx context.Context) error {
        return metrics.Report(ctx, event)
    })
// 不调用 Wait() 也不会阻塞结果发送；适用于可接受结果/错误不可见的非关键路径。
// WithTimeout 只有在 Report 响应 ctx 时才能让任务按时结束。
```

`Bounded` 仅限制同时运行的任务，仍需控制保留句柄和结果的数量。上例每批最多保留 256 个句柄，全部收尾后才进入下一批；输入本身的内存及 handle 中保存的结果另行预算。任务忽略取消时，批次等待仍可能阻塞。

前两步错误带操作名以 `%w` 返回；批次等待全部已提交任务后返回遇到的首错，其他失败不会汇入返回值，若业务需要完整错误链应改为收集并合并。成功项才调用 `handle`，它没有错误返回且应及时返回；不要把已处理结果当成可回滚事务。最后的监控上报不等待，业务错误、panic 或 Goexit 均不会作为外围函数返回值；进程退出前若必须确认上报完成，应保存句柄并等待。

支付步骤的 10 秒定时取消早于 60 秒回调期限；业务协作等待取消时会先收到 `context.Canceled`。父 context 取消与空闲 Bounded 许可同时可用时，回调仍可能被接收，必须自行响应取消；回调在发出取消后成功返回也不会被该入口改写为失败。每次链式新建 Builder 的 Bounded 容量相互独立，跨请求共享上限时须复用配置固定的 Builder。生产中按连接预算设置并发度，给整个请求配置父期限，并为不可返回的业务安排所有者和收尾。

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
    DefaultLogger().     // 当前构建器恢复继承全局日志器
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

多个 `add` 可以并发执行，其访问的共享数据须自行同步；追加顺序由各调用取得锁的顺序决定，不保证调用顺序。`add` 为 nil 或发生 panic 时，panic 会向调用方传播，不会追加该值。不要在首次使用后复制 `Mu`，因为它包含互斥锁。

`add` 在当前调用 goroutine 内运行，`Append` 不会为它启动 goroutine；它可以调用同一容器的 `Snapshot` 或嵌套 `Append`。嵌套追加在外层返回前完成，外层随后 panic 也不会回滚这些已完成追加或其他副作用。生产中应保持生成函数短小，并自行处理业务副作用的幂等和补偿；容器没有事务或回滚机制。

| 参数 | 类型 | 说明 |
|------|------|------|
| `add` | `func() T` | 生成要追加元素的工厂函数（在锁外执行） |

```go
var mu async.Mu[int]

mu.Append(func() int { return 42 })
mu.Append(func() int { return computeSomething() })
```

### Snapshot

返回当前所有元素的浅拷贝，线程安全。非 nil 接收者返回独立切片，后续 `Append` 不改变该快照的长度和元素槽位；若元素包含指针、map 或切片，引用的数据仍然共享，需自行同步。nil 的 `*async.Mu[T]` 调用 `Snapshot()` 返回 nil；`Append` 不支持 nil 接收者。

此方法没有参数，也不等待仍在生成值的 `Append`：快照只包括取得快照锁之前已经追加的元素。若需完整收集，调用方须先用 WaitGroup、任务句柄或其他完成通知等待生产者。未追加过元素的非 nil 容器返回非 nil 的空切片；快照复制成本随当前元素数增长，应按内存预算控制元素总数和调用频率。上述重入、panic 及浅拷贝规则的可执行对照见 [Example_muFactoryAndSnapshot](tests/public_examples_test.go)。

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
