# 异步任务 (Task / Go)

异步任务模块用于**启动单个异步任务并非阻塞获取结果**。提供了 `Go`（fire-and-forget）、`GoResult`（带返回值异步）、`GoWithTimeout`（带超时）等多种启动方式和 `AsyncResult` 等待模式。

## 目录

- [核心概念](#核心概念)
- [无返回值异步任务 (Go)](#无返回值异步任务-go)
- [带返回值异步任务 (GoResult)](#带返回值异步任务-goresult)
- [AsyncResult 等待模式](#asyncresult-等待模式)
- [Task 可取消任务](#task-可取消任务)
- [Mu\[T\] 线程安全切片](#mut-线程安全切片)
- [完整示例](#完整示例)
- [方法速查表](#方法速查表)

---

## 核心概念

```
Go / GoResult → 启动异步任务
             ↓
AsyncResult  → Wait / WaitTimeout / WaitCh / Cancel / Ok / IsPanic
             ↓
Task         → Ctx + Cancel + Result（可主动取消）
```

三种任务类型：

| 类型 | 创建方式 | 返回值 | 可取消 |
|------|---------|--------|--------|
| `TaskVoid` | `Go(ctx, fn)` | 无 | ❌ |
| `AsyncResult[T]` | `GoResult(ctx, fn)` | `(T, error)` | ❌ |
| `Task[T]` | 子包 `task.GoResult(ctx, fn)` | `(T, error)` | ✅ |

---

## 无返回值异步任务 (Go)

适合**日志上报、指标采集、缓存刷新**等不需要等待结果的后台操作。

### 基础用法

```go
// fire-and-forget：不等待结果
async.Go(ctx, func(ctx context.Context) {
    metrics.Record(ctx, "request.count", 1)
})
// 继续处理主逻辑...

// 需要等待完成时，保存 TaskVoid 句柄
task := async.Go(ctx, func(ctx context.Context) {
    uploadFile(ctx, data)
})
// 做其他事情...
if err := task.Wait(); err != nil {
    log.Printf("任务失败: %v", err)
}
```

**`Go` 参数：**
- `ctx` — 上下文
- `fn` — 任务函数 `func(context.Context)`（无返回值）

**返回：** `*TaskVoid` — 异步任务句柄

### 带超时

```go
// 最多执行 3 秒
task := async.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) {
    slowCleanup(ctx)
})
```

**`GoWithTimeout` 参数：**
- `ctx` — 上下文
- `timeout` — 超时时间
- `fn` — 任务函数 `func(context.Context)`

### TaskVoid 方法

```go
task := async.Go(ctx, fn)

err := task.Wait()       // 阻塞等待完成，返回 error
ok := task.Ok()          // 阻塞等待并返回是否成功（nil error）
panic := task.IsPanic()  // 阻塞等待并返回是否由 panic 导致
```

---

## 带返回值异步任务 (GoResult)

适合**并发执行多个独立任务并获取各自结果**的场景。

### 基础用法

```go
// 并发查询多个数据源
ar1 := async.GoResult(ctx, func(ctx context.Context) (*User, error) {
    return db.GetUser(ctx, userID)
})
ar2 := async.GoResult(ctx, func(ctx context.Context) ([]Order, error) {
    return db.GetOrders(ctx, userID)
})

// 等待结果
user, err1 := ar1.Wait()
orders, err2 := ar2.Wait()

if err1 != nil {
    log.Printf("查询用户失败: %v", err1)
}
if err2 != nil {
    log.Printf("查询订单失败: %v", err2)
}
```

**`GoResult` 参数：**
- `ctx` — 上下文
- `fn` — `func(context.Context) (T, error)` 

**返回：** `*AsyncResult[T]` — 结果句柄

### 带超时

```go
ar := async.GoResultWithTimeout(ctx, 5*time.Second, func(ctx context.Context) (*Data, error) {
    return fetchFromAPI(ctx)
})

val, err := ar.Wait()
```

**`GoResultWithTimeout` 参数：**
- `ctx` — 上下文
- `timeout` — 超时时间
- `fn` — `func(context.Context) (T, error)`

---

## AsyncResult 等待模式

`AsyncResult[T]` 是带返回值异步任务的结果句柄，支持多种等待方式。

### Wait（阻塞等待）

```go
ar := async.GoResult(ctx, fn)
value, err := ar.Wait()  // 返回 (T, error)
if err != nil {
    log.Printf("任务失败: %v", err)
}
// 多个 goroutine 并发调用 Wait 是安全的
```

### WaitTimeout（带超时等待）

```go
val, err, ok := ar.WaitTimeout(3 * time.Second)  // 返回 (T, error, bool)
if !ok {
    fmt.Println("任务超时，已丢弃结果")
    return
}
```

### WaitCh（Channel 等待，适合 select）

```go
select {
case r := <-ar.WaitCh():  // <-chan Result[T]
    fmt.Println("任务完成:", r.Value)
case <-ctx.Done():
    fmt.Println("上下文取消")
}
```

### Cancel（取消等待）

```go
ar := async.GoResult(ctx, slowFn)
time.Sleep(100 * time.Millisecond)
if val, err := ar.Cancel(); err != nil {
    fmt.Println("任务被取消")
}
```

### Ok / IsPanic（状态查询）

```go
if ar.Ok() {        // 阻塞等待并返回是否成功
    fmt.Println("任务成功")
}
if ar.IsPanic() {   // 阻塞等待并返回是否 panic
    fmt.Println("任务 panic 了")
}
```

---

## Task 可取消任务

`Task[T]` 提供了 `Cancel` 方法可主动取消正在执行的任务：

```go
// 使用子包直接创建
t := task.GoResult(ctx, func(ctx context.Context) (*Data, error) {
    return longRunningTask(ctx)
})

// 超时后取消
time.Sleep(5 * time.Second)
t.Cancel()

// 获取结果（可能是取消前的部分结果）
result, err := t.Result()

// 也可以直接用 context
t.Ctx  // 任务上下文
```

| 字段/方法 | 说明 |
|-----------|------|
| `t.Ctx` | 任务上下文 |
| `t.Cancel()` | 取消任务 |
| `t.Result()` | 获取结果 |

### GoAction / GoResultAction（task 子包）

`task` 包还提供了无需自己处理泛型的便捷函数：

```go
// GoAction: 无返回值 → AsyncResult[NoResult]，只需关心 error
ar := task.GoAction(ctx, func(ctx context.Context) error {
    return sendNotification(ctx, userID, msg)
})
_, err := ar.Wait() // 忽略 NoResult 值

// GoResultAction: 无返回值 → Task[NoResult]，可主动 Cancel
t := task.GoResultAction(ctx, func(ctx context.Context) error {
    return slowWork(ctx)
})
t.Cancel()
result := t.Result()
```

| 函数 | 签名 | 返回类型 | 特点 |
|------|------|---------|------|
| `task.Go(ctx, fn)` | `func(ctx) (T, error)` | `*AsyncResult[T]` | 标准泛型 |
| `task.GoResult(ctx, fn)` | `func(ctx) (T, error)` | `Task[T]` | 可主动 Cancel |
| `task.GoAction(ctx, fn)` | `func(ctx) error` | `*AsyncResult[NoResult]` | 无返回值便捷版 |
| `task.GoResultAction(ctx, fn)` | `func(ctx) error` | `Task[NoResult]` | 无返回值 + 可 Cancel |

---

## SafeCall - Panic 保护

`SafeCall` 和 `SafeCallVoid` 在执行函数时自动捕获 panic 并转换为 error：

```go
// 带返回值，panic 转为 error
val, err := async.SafeCall(ctx, rawData, func(ctx context.Context, data string) (Parsed, error) {
    return parse(data) // panic 被捕获并转为 error
})
if err != nil {
    log.Printf("解析失败或 panic: %v", err)
}

// 无返回值版本
err := async.SafeCallVoid(ctx, record, func(ctx context.Context, r Record) error {
    // 即使这里 panic，也会被转为 error 返回
    validate(r)
    return nil
})
```

**`SafeCall` 参数：**
- `ctx` — 上下文
- `item` — 输入值 `T`
- `fn` — `func(context.Context, T) (R, error)` — 自动捕获 panic 的函数

**`SafeCallVoid` 参数：**
- `ctx` — 上下文
- `item` — 输入值 `T`
- `fn` — `func(context.Context, T) error` — 自动捕获 panic 的无返回值函数

> 通常配合 `Map`/`ForEach` 内部使用，确保单个元素的 panic 不影响整体流程。本身不创建 goroutine，需调用方并发使用。

---

## Mu[T] 线程安全切片

`async.Mu[T]` 是线程安全的切片容器，支持并发安全的 `Append` 和 `Snapshot`。适用于多个 goroutine 需要安全收集结果的场景。

### 基本用法

```go
var mu async.Mu[int]
var wg sync.WaitGroup

for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(val int) {
        defer wg.Done()
        mu.Append(func() int { return val })
    }(i)
}

wg.Wait()

// 获取所有结果的副本（线程安全）
all := mu.Snapshot()
fmt.Printf("收集到 %d 个结果\n", len(all))
```

### 方法速查

| 方法 | 说明 |
|------|------|
| `Append(fn)` | 线程安全地追加元素（fn 在锁内执行） |
| `Snapshot()` | 返回所有元素的副本 |

> `Append` 接收一个返回值的函数而非直接传值，保证值计算和追加的原子性。
>
> `Snapshot()` 在 `nil` receiver 上调用安全，返回 `nil`。

---

## 完整示例

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/chichengyu/async"
)

func main() {
    ctx := context.Background()
    ctx = async.EnsureTraceID(ctx)

    // 示例1：fire-and-forget
    async.Go(ctx, func(ctx context.Context) {
        time.Sleep(100 * time.Millisecond)
        fmt.Println("日志上报完成")
    })

    // 示例2：并发查询多个数据源
    ar1 := async.GoResult(ctx, func(ctx context.Context) (string, error) {
        time.Sleep(200 * time.Millisecond)
        return "data-from-a", nil
    })

    ar2 := async.GoResult(ctx, func(ctx context.Context) (string, error) {
        time.Sleep(300 * time.Millisecond)
        return "data-from-b", nil
    })

    // 示例3：使用 WaitCh 进行 select 多路复用
    select {
    case r := <-ar1.WaitCh():
        fmt.Println("A 先完成:", r.Value)
    case r := <-ar2.WaitCh():
        fmt.Println("B 先完成:", r.Value)
    case <-time.After(500 * time.Millisecond):
        fmt.Println("超时")
    }

    // 获取另一个结果
    _, _ = ar1.Wait() // 已有缓存，不阻塞
    _, _ = ar2.Wait()

    // 示例4：带超时的异步任务
    ar3 := async.GoResultWithTimeout(ctx, 2*time.Second, func(ctx context.Context) (int, error) {
        time.Sleep(1 * time.Second)
        return 42, nil
    })

    val, err := ar3.Wait()
    if err != nil {
        log.Printf("失败: %v", err)
    } else {
        fmt.Printf("结果: %d\n", val)
    }
}
```

---

## 方法速查表

### 异步任务启动

| 函数 | 说明 |
|------|------|
| `Go(ctx, fn)` | 启动无返回值异步任务，返回 `*TaskVoid` |
| `GoWithTimeout(ctx, d, fn)` | 带超时的无返回值异步任务 |
| `GoResult[T](ctx, fn)` | 启动带返回值异步任务，返回 `*AsyncResult[T]` |
| `GoResultWithTimeout[T](ctx, d, fn)` | 带超时的带返回值异步任务 |

### AsyncResult[T] 方法

| 方法 | 说明 |
|------|------|
| `Wait()` | 阻塞等待，返回 `(T, error)` |
| `WaitTimeout(d)` | 带超时等待，返回 `(T, error, ok)` |
| `WaitCh()` | 返回结果 channel，可用于 select |
| `Cancel()` | 取消等待 |
| `Ok()` | 阻塞等待并返回是否成功 |
| `IsPanic()` | 阻塞等待并返回是否 panic |

### TaskVoid 方法

| 方法 | 说明 |
|------|------|
| `Wait()` | 阻塞等待，返回 error |
| `Ok()` | 阻塞等待并返回是否成功 |
| `IsPanic()` | 阻塞等待并返回是否 panic |

### Mu[T] 线程安全切片

| 方法 | 说明 |
|------|------|
| `Append(fn)` | 线程安全追加元素 |
| `Snapshot()` | 返回所有元素副本 |

### 类型

| 类型 | 说明 |
|------|------|
| `TaskVoid` | 无返回值异步任务句柄 |
| `AsyncResult[T]` | 带返回值异步结果句柄 |
| `Task[T]` | 可取消的异步任务 |
| `Mu[T]` | 线程安全切片容器 |