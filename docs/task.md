# 异步任务 (Task / Go)

异步任务模块用于**启动单个异步任务并非阻塞获取结果**。提供了 `Go`（fire-and-forget）、`GoResult`（带返回值异步）、`GoWithTimeout`（带超时）等多种启动方式和 `AsyncResult` 等待模式。

## 目录

- [核心概念](#核心概念)
- [无返回值异步任务 (Go)](#无返回值异步任务-go)
- [带返回值异步任务 (GoResult)](#带返回值异步任务-goresult)
- [AsyncResult 等待模式](#asyncresult-等待模式)
- [Task 可取消任务](#task-可取消任务)
- [完整示例](#完整示例)

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

### 带超时

```go
// 最多执行 3 秒
async.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) {
    slowCleanup(ctx)
})
```

### TaskVoid 方法

```go
task := async.Go(ctx, fn)

err := task.Wait()    // 阻塞等待完成
ok := task.Ok()       // 阻塞等待并返回是否成功
panic := task.IsPanic() // 阻塞等待并返回是否由 panic 导致
```

---

## 带返回值异步任务 (GoResult)

适合**并发执行多个独立任务并获取各自结果**的场景。

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

### 带超时

```go
ar := async.GoResultWithTimeout(ctx, 5*time.Second, func(ctx context.Context) (*Data, error) {
    return fetchFromAPI(ctx)
})

val, err := ar.Wait()
```

---

## AsyncResult 等待模式

`AsyncResult[T]` 是带返回值异步任务的结果句柄，支持多种等待方式。

### Wait（阻塞等待）

```go
ar := async.GoResult(ctx, fn)
value, err := ar.Wait()
if err != nil {
    log.Printf("任务失败: %v", err)
}
// 多个 goroutine 并发调用 Wait 是安全的
```

### WaitTimeout（带超时等待）

```go
val, err, ok := ar.WaitTimeout(3 * time.Second)
if !ok {
    fmt.Println("任务超时，已丢弃结果")
    return
}
```

### WaitCh（Channel 等待，适合 select）

```go
select {
case r := <-ar.WaitCh():
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
if ar.Ok() {
    fmt.Println("任务成功")
}
if ar.IsPanic() {
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