# Task（异步任务）文档

## 概述

Task 提供多种异步任务模式，适合 fire-and-forget 场景。所有函数在顶层 `async` 包中可用。

**模式对照表**:

| 模式 | 函数 | 返回值 | 特点 |
|------|------|--------|------|
| Fire-and-forget | `Go()` / `GoWithTimeout()` | `*TaskVoid` | 无返回值，不管结果 |
| Fire-and-forget (err only) | `GoErr()` | `*AsyncErr` | 只关心错误 |
| 带返回值 | `GoResult[T]()` / `GoResultWithTimeout[T]()` | `*AsyncResult[T]` | 可获取值和错误 |
| 可取消 + 返回值 | `GoCancel[T]()` | `*Task[T]` | 支持取消 |
| 可取消 (err only) | `GoCancelErr()` | `*TaskErr` | 可取消 + 只关心错误 |

---

## 目录

- [Fire-and-Forget](#fire-and-forget)
  - [Go / GoWithTimeout / GoErr / GoCancelErr](#go)
  - [GoAction / GoResultAction](#goaction)
- [带返回值](#带返回值)
  - [GoResult / GoResultWithTimeout](#goresult)
- [可取消](#可取消)
  - [GoCancel](#gocancel)
- [AsyncResult[T] 方法详解](#asyncresultt-方法详解)
  - [Wait / WaitTimeout / WaitCh / Cancel / Done / Ok / IsPanic](#wait)
- [Task[T] 方法详解](#taskt-方法详解)
- [TaskVoid 方法详解](#taskvoid-方法详解)
- [AsyncErr 方法](#asyncerr-方法)
- [TaskErr 方法](#taskerr-方法)
- [安全调用](#安全调用)
  - [SafeCall / SafeCallVoid / SafeCallWithResult](#safecallwithresult)
- [通用工具](#通用工具)
  - [Mu[T] 并发安全容器：Append / Snapshot](#mut-方法详解)
- [完整示例](#完整示例)
- [所有异步任务函数速查](#所有异步任务函数速查)

## Fire-and-Forget

### Go

```go
// 语法
func Go(ctx context.Context, fn func(context.Context)) *TaskVoid
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context)` | 无返回值的任务 |
| 返回 | `*TaskVoid` | 可查询状态的异步结果 |

```go
task := async.Go(ctx, func(ctx context.Context) {
    metrics.Record(ctx, "request.count", 1)
})

// 查询状态
err := task.Wait()    // 等待完成
ok := task.Ok()       // 是否成功（无 error 无 panic）
isPanic := task.IsPanic() // 是否因 panic 失败
```

### GoWithTimeout

```go
// 语法
func GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context)) *TaskVoid
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeout` | `time.Duration` | 任务超时时间 |

```go
task := async.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) {
    slowCleanup(ctx)
})
```

### GoErr

```go
// 语法
func GoErr(ctx context.Context, fn func(context.Context) error) *AsyncErr
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func(context.Context) error` | 可能返回错误的任务 |
| 返回 | `*AsyncErr` | 只包含错误的异步结果 |

```go
arErr := async.GoErr(ctx, func(ctx context.Context) error {
    return doSomething(ctx)
})
err := arErr.Wait()
```

### GoCancelErr

```go
// 语法
func GoCancelErr(ctx context.Context, fn func(context.Context) error) *TaskErr
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func(context.Context) error` | 可能返回错误的任务 |
| 返回 | `*TaskErr` | 可取消的错误异步结果 |

```go
taskErr := async.GoCancelErr(ctx, func(ctx context.Context) error {
    return longOp(ctx)
})
taskErr.Cancel()
err := taskErr.Wait()
```

### GoAction

```go
// 语法
func GoAction(ctx context.Context, fn func(context.Context) error) *AsyncResult[NoResult]
```

启动一个无返回值（NoResult）的异步任务，返回 `*AsyncResult[NoResult]`。fn 只返回 error，自动注入 trace_id 到 context。内置 panic 恢复机制，panic 将被包装为 `core.PanicError`。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文（自动注入 trace_id） |
| `fn` | `func(context.Context) error` | 异步执行的函数，只返回 error |
| 返回 | `*AsyncResult[NoResult]` | 可获取结果的异步句柄 |

**与 `GoErr` 的区别**：`GoErr` 返回 `*AsyncErr`（仅包装 error），`GoAction` 返回 `*AsyncResult[NoResult]`（完整 Result 结构，包含 trace_id 等信息）。

**使用示例**:

```go
ar := task.GoAction(ctx, func(ctx context.Context) error {
    return sendNotification(ctx, userID, msg)
})
// 忽略 NoResult 值，只关心 error
_, err := ar.Wait()

// 通过 async 顶层包
ar := async.GoAction(ctx, fn)
```

### GoResultAction

```go
// 语法
func GoResultAction(ctx context.Context, fn func(context.Context) error) Task[NoResult]
```

启动一个无返回值（NoResult）的**可取消**异步任务，返回 `Task[NoResult]`。fn 只返回 error，自动注入 trace_id 到 context。通过返回的 `Task.Cancel()` 可随时取消任务。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文（自动注入 trace_id） |
| `fn` | `func(context.Context) error` | 异步执行的函数，只返回 error |
| 返回 | `Task[NoResult]` | 可取消的任务（包含 Ctx、Cancel、Result 字段） |

**与 `GoAction` 的区别**：`GoAction` 返回不可取消的 `*AsyncResult[NoResult]`；`GoResultAction` 返回可取消的 `Task[NoResult]`，可通过 `task.Cancel()` 主动取消。

**使用示例**:

```go
// 启动可取消的后台任务
t := task.GoResultAction(ctx, func(ctx context.Context) error {
    return uploadFile(ctx, filepath)
})

// 可随时取消（例如超时控制）
time.AfterFunc(10*time.Second, func() {
    t.Cancel()
})

// 获取结果
_, err := t.Result()

// 通过 async 顶层包
t := async.GoResultAction(ctx, fn)
```

---

## 带返回值

### GoResult

```go
// 语法
func GoResult[T any](ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 返回值和错误的任务 |
| 返回 | `*AsyncResult[T]` | 可获取结果的异步句柄 |

```go
ar := async.GoResult(ctx, func(ctx context.Context) (*User, error) {
    return db.GetUser(ctx, userID)
})

user, err := ar.Wait()
```

### GoResultWithTimeout

```go
// 语法
func GoResultWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) *AsyncResult[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeout` | `time.Duration` | 任务超时时间 |

```go
ar := async.GoResultWithTimeout(ctx, 5*time.Second, func(ctx context.Context) (*Order, error) {
    return paymentService.Process(ctx, orderID)
})
order, err := ar.Wait()
```

---

## 可取消

### GoCancel

```go
// 语法
func GoCancel[T any](ctx context.Context, fn func(context.Context) (T, error)) *Task[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 返回值和错误的任务 |
| 返回 | `*Task[T]` | 可取消的任务句柄 |

```go
task := async.GoCancel(ctx, func(ctx context.Context) (*Data, error) {
    return longRunningWork(ctx), nil
})

// 取消任务
task.Cancel()

// 获取结果
result, err := task.Result()
```

---

## AsyncResult[T] 方法详解

`*AsyncResult[T]` 是 `GoResult` / `GoResultWithTimeout` 的返回值类型。

### Wait

```go
// 语法
func (ar *AsyncResult[T]) Wait() (T, error)
```

阻塞等待结果。返回 `(value, error)`。

```go
data, err := ar.Wait()
if err != nil {
    log.Printf("任务失败: %v", err)
}
fmt.Println(data)
```

### WaitTimeout

```go
// 语法
func (ar *AsyncResult[T]) WaitTimeout(timeout time.Duration) (T, error, bool)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `timeout` | `time.Duration` | 等待超时 |
| 返回1 | `T` | 结果值 |
| 返回2 | `error` | 错误 |
| 返回3 | `bool` | 是否在超时内完成 |

```go
data, err, ok := ar.WaitTimeout(3 * time.Second)
if !ok {
    log.Println("异步任务超时")
}
```

### WaitCh

```go
// 语法
func (ar *AsyncResult[T]) WaitCh() <-chan struct{}
```

返回完成信号 channel。可选择监听完成事件。

```go
select {
case <-ar.WaitCh():
    data, err = ar.Wait()
    fmt.Println("完成:", data)
case <-time.After(5 * time.Second):
    log.Println("超时")
}
```

### Cancel

```go
// 语法
func (ar *AsyncResult[T]) Cancel() (T, error)
```

取消任务并返回已有结果。

```go
data, err := ar.Cancel()
```

### Done

```go
// 语法
func (ar *AsyncResult[T]) Done() bool
```

非阻塞检查是否完成。

```go
if ar.Done() {
    data, _ := ar.Wait()
}
```

### Ok

```go
// 语法
func (ar *AsyncResult[T]) Ok() bool
```

阻塞等待完成，返回是否成功（无错误无 panic）。

```go
if ar.Ok() {
    fmt.Println("任务成功")
}
```

### IsPanic

```go
// 语法
func (ar *AsyncResult[T]) IsPanic() bool
```

检查是否因 panic 失败。

```go
if ar.IsPanic() {
    log.Println("任务 panic")
}
```

---

## Task[T] 方法详解

`*Task[T]` 是 `GoCancel` 的返回值类型。

| 方法 | 语法 | 说明 |
|------|------|------|
| `Cancel` | `func (t *Task[T]) Cancel()` | 取消任务 |
| `Result` | `func (t *Task[T]) Result() (T, error)` | 获取结果（阻塞） |
| `Done` | `func (t *Task[T]) Done() bool` | 是否完成 |
| `Ctx` | `func (t *Task[T]) Ctx() context.Context` | 获取任务的 context |

```go
task := async.GoCancel(ctx, fn)

task.Cancel()                // 取消
result, err := task.Result() // 获取结果
ok := task.Done()            // 是否完成
ctx := task.Ctx()            // 获取 context
```

---

## TaskVoid 方法详解

`*TaskVoid` 是 `Go` / `GoWithTimeout` 的返回值类型。

| 方法 | 语法 | 说明 |
|------|------|------|
| `Wait` | `func (t *TaskVoid) Wait() error` | 等待完成 |
| `Ok` | `func (t *TaskVoid) Ok() bool` | 是否成功（无 error 无 panic） |
| `IsPanic` | `func (t *TaskVoid) IsPanic() bool` | 是否因 panic 失败 |

```go
task := async.Go(ctx, fn)

err := task.Wait()   // 等待完成
ok := task.Ok()      // 是否成功
task.IsPanic()       // 是否 panic
```

---

## AsyncErr 方法

`*AsyncErr` 是 `GoErr` 的返回值类型。

| 方法 | 语法 | 说明 |
|------|------|------|
| `Wait` | `func (ae *AsyncErr) Wait() error` | 等待完成，返回错误 |

```go
arErr := async.GoErr(ctx, fn)
err := arErr.Wait()
```

---

## TaskErr 方法

`*TaskErr` 是 `GoCancelErr` 的返回值类型。

| 方法 | 语法 | 说明 |
|------|------|------|
| `Cancel` | `func (te *TaskErr) Cancel()` | 取消任务 |
| `Wait` | `func (te *TaskErr) Wait() error` | 等待完成 |

```go
taskErr := async.GoCancelErr(ctx, fn)
taskErr.Cancel()
err := taskErr.Wait()
```

---

## 安全调用

以下函数对 panic 自动恢复，将 panic 转换为 error 返回，避免 goroutine 崩溃。

### SafeCall

安全调用带返回值的函数，捕获 panic 并转为 error 返回。

```go
// 语法
func SafeCall(fn func() error) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func() error` | 要安全调用的函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 函数返回的 error，或 panic 转换的 *PanicError |

```go
err := async.SafeCall(func() error {
    panic("unexpected error")
})
// err 类型为 *PanicError
fmt.Println(err) // "panic: unexpected error"
```

> **注意**：如果 fn 本身返回 error，SafeCall 直接透传该 error。仅当发生 panic 时才生成 *PanicError。

### SafeCallVoid

安全调用无返回值的函数，捕获 panic 并转为 error 返回。

```go
// 语法
func SafeCallVoid(fn func()) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func()` | 要安全调用的函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | panic 转换的 *PanicError（正常完成返回 nil） |

```go
err := async.SafeCallVoid(func() {
    // 可能 panic 的操作
    writeToClosedChannel(ch, data)
})
if err != nil {
    log.Printf("操作失败: %v", err)
}
```

> **注意**：SafeCallVoid 仅对 panic 做转换。如果 fn 本身无任何异常，返回 nil。

### SafeCallWithResult

安全调用带返回值的函数，捕获 panic 并转为 error，正常完成则返回值和 nil。

```go
// 语法
func SafeCallWithResult[T any](fn func() (T, error)) (T, error)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `fn` | `func() (T, error)` | 要安全调用的函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `T` | `T` | 零值（发生 panic 时）或 fn 返回的值 |
| `error` | `error` | fn 返回的 error 或 panic 转换的 *PanicError |

```go
data, err := async.SafeCallWithResult(func() (*Config, error) {
    return loadConfig("config.yaml")
})
if err != nil {
    log.Printf("加载配置失败: %v", err)
}
```

> **注意**：发生 panic 时返回 `(T 零值, *PanicError)`。不会吞掉真实的业务 error。

---

## Mu[T] 方法详解

`Mu[T]` 是线程安全的泛型切片容器，基于 `sync.Mutex` 实现。nil receiver 安全，所有方法在 nil 值上调用不会 panic。

```go
// 语法
var mu async.Mu[int]
```

| 特性 | 说明 |
|------|------|
| 并发安全 | 基于 `sync.Mutex`，所有操作原子性 |
| nil 安全 | nil receiver 上的方法调用不会 panic |
| 泛型 | 支持任意类型 `T` |
| 无锁设计 | 使用互斥锁而非 channel，适合高频追加场景 |

> **适用场景**：需要在多个 goroutine 中并发收集结果，且只需要追加和快照操作时使用。对于更复杂的集合操作，请使用 `sync.Map` 或 channel。

---

### Append

线程安全地向切片末尾追加元素。`add` 函数在锁内执行，保证写入的原子性。

```go
// 语法
func (m *Mu[T]) Append(add func() T)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `add` | `func() T` | 生成待追加元素的函数，在互斥锁保护下执行 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| 无 | — | — |

```go
var mu async.Mu[int]

// 基本用法
mu.Append(func() int { return 42 })

// 计算密集型值
mu.Append(func() int {
    result := expensiveComputation()
    return result
})

// 并发追加
var wg sync.WaitGroup
for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(val int) {
        defer wg.Done()
        mu.Append(func() int { return val })
    }(i)
}
wg.Wait()

all := mu.Snapshot()
fmt.Println(len(all)) // 100（顺序不一定连续）
```

> **注意**：追加的顺序不一定是提交顺序。由于 goroutine 调度和锁竞争，元素的最终顺序取决于谁先获取到互斥锁。

---

### Snapshot

返回当前切片的完整副本，线程安全。返回的切片与内部存储完全独立，修改副本不会影响原数据。

```go
// 语法
func (m *Mu[T]) Snapshot() []T
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `[]T` | `[]T` | 当前所有元素的副本；nil receiver 返回 nil 而非空切片 |

```go
// 获取快照
all := mu.Snapshot()
fmt.Printf("共收集 %d 个元素\n", len(all))

// 遍历处理
for i, v := range all {
    fmt.Printf("元素 #%d: %v\n", i, v)
}

// nil receiver 安全
var nilMu *async.Mu[string]
result := nilMu.Snapshot() // 返回 nil，不会 panic
if result == nil {
    fmt.Println("nil Mu, 无需处理")
}
```

---

## 完整示例

```go
// 并发启动 3 个任务，收集结果
tasks := make([]*async.AsyncResult[*Response], 3)

tasks[0] = async.GoResult(ctx, func(ctx context.Context) (*Response, error) {
    return api1.Call(ctx)
})
tasks[1] = async.GoResult(ctx, func(ctx context.Context) (*Response, error) {
    return api2.Call(ctx)
})
tasks[2] = async.GoResultWithTimeout(ctx, 2*time.Second, func(ctx context.Context) (*Response, error) {
    return api3.Call(ctx)
})

// 按顺序收集
for i, t := range tasks {
    resp, err := t.WaitTimeout(5 * time.Second)
    if err != nil {
        log.Printf("api%d 失败: %v", i+1, err)
        continue
    }
    fmt.Printf("api%d 结果: %v\n", i+1, resp)
}
```

---

## 所有异步任务函数速查

| 函数 | 语法 | 返回类型 | 说明 |
|------|------|----------|------|
| `Go` | `Go(ctx, fn)` | `*TaskVoid` | 无返回值异步 |
| `GoWithTimeout` | `GoWithTimeout(ctx, d, fn)` | `*TaskVoid` | 无返回值+超时 |
| `GoResult` | `GoResult[T](ctx, fn)` | `*AsyncResult[T]` | 带返回值异步 |
| `GoResultWithTimeout` | `GoResultWithTimeout[T](ctx, d, fn)` | `*AsyncResult[T]` | 带返回值+超时 |
| `GoCancel` | `GoCancel[T](ctx, fn)` | `*Task[T]` | 可取消+返回值 |
| `GoErr` | `GoErr(ctx, fn)` | `*AsyncErr` | 只关心错误 |
| `GoCancelErr` | `GoCancelErr(ctx, fn)` | `*TaskErr` | 可取消+错误 |