# 协程池 (Pool)

协程池 `Pool[T]` 是 async 库的核心组件之一，它复用固定数量的 goroutine 来处理高频并发任务。适合**长期运行、反复提交任务**的场景。

## 目录

- [核心概念](#核心概念)
- [创建池](#创建池)
- [提交任务](#提交任务)
- [等待与关闭](#等待与关闭)
- [高级选项](#高级选项)
- [NoResultPool](#noresultpool)
- [完整示例](#完整示例)

---

## 核心概念

```
生命周期：NewPool → Submit → Wait → Close
```

- **有返回值池** `Pool[T]`：每个任务返回 `Result[T]`
- **无返回值池** `NoResultPool`：每个任务只关心 error

**何时用 Pool 而非 Group？**
- Pool：长期运行的后台服务，反复提交任务（如 HTTP server 的 worker 池）
- Group：一次性批量任务，用完即销毁（如定时任务、数据迁移）

---

## 创建池

```go
import "github.com/chichengyu/async"

// 创建 4 个 worker 的协程池
p := async.NewPool[int](4)
defer p.Close()

// 使用默认 IO 并发度创建
p := async.DefaultPool[int]()
defer p.Close()

// 创建无返回值池
nrp := async.NewNoResultPool(10)
defer nrp.Close()

// 使用默认并发度的无返回值池
nrp := async.DefaultNoResultPool()
defer nrp.Close()
```

> **注意**：`NewPool` 会**立即启动** worker goroutine，创建后务必 `defer p.Close()`。

---

## 提交任务

### Submit（阻塞提交）

```go
p := async.NewPool[string](4)
defer p.Close()

// 提交任务，worker 满时阻塞等待
for i := 0; i < 100; i++ {
    err := p.Submit(ctx, func(ctx context.Context) (string, error) {
        time.Sleep(100 * time.Millisecond)
        return fmt.Sprintf("result-%d", i), nil
    })
    if err != nil {
        log.Printf("提交失败: %v", err)
    }
}

results := p.Wait()
```

### TrySubmit（非阻塞提交）

```go
// 队列满时立即返回 ErrSubmitTimeout，不等待
err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
    return processItem(ctx), nil
})
if errors.Is(err, async.ErrSubmitTimeout) {
    // 池已满，可以降级处理或丢弃
    log.Println("池已满，任务被拒绝")
}
```

### SubmitAt / TrySubmitAt（指定位置提交）

```go
// 指定结果数组索引位置提交，保证结果顺序
for i, item := range items {
    p.SubmitAt(i, ctx, func(ctx context.Context) (int, error) {
        return processItem(ctx, item), nil
    })
}
```

---

## 等待与关闭

### Wait（阻塞等待）

```go
// 阻塞等待所有已提交任务完成，返回结果切片
results, err := p.Wait()
for i, r := range results {
    if r.Ok() {
        fmt.Printf("任务 %d 结果: %v\n", i, r.Value)
    } else {
        log.Printf("任务 %d 失败: %v\n", i, r.Err)
    }
}
```

> **调用 Wait 后不能再 Submit**，否则返回 `ErrPoolWaited`。

### WaitTimeout（带超时等待）

```go
results, err := p.WaitTimeout(5 * time.Second)
if errors.Is(err, async.ErrTimeout) {
    // 超时：部分任务可能未完成
}
```

### WaitContext（Context 控制等待）

```go
// 通过 context 控制等待
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

results, err := p.WaitContext(ctx)
```

### Close（关闭池）

```go
// 关闭池，释放所有 worker goroutine
p.Close()

// ⚠️ 关闭后不能再提交任务
```

---

## 高级选项

Pool 支持链式配置，在创建后通过 `With*` 方法设置：

```go
p := async.NewPool[int](4)

// 设置超时（覆盖全局默认值）
p.WithTimeout(10 * time.Second)

// 设置提交超时
p.WithSubmitTimeout(2 * time.Second)

// 开启 FailFast（一个任务失败，取消其他任务）
p, ffCtx := p.WithFailFast(ctx)

// 组合使用：FailFast + 超时
p, ffCtx := p.WithFFTimeout(ctx, 5*time.Second)

// 带 TraceID
p, ctx := p.WithTraceID(ctx)
```

### 查询池状态

```go
// 获取池中 worker 数量
size := p.Size()

// 获取当前活跃的 worker 数量
active := p.Active()

// 获取当前空闲的 worker 数量
idle := p.Idle()

// 获取排队中的任务数量
pending := p.Pending()
```

---

## NoResultPool

无返回值池适合**只关心错误**的批量操作，配合便捷提交函数使用：

```go
p := async.NewNoResultPool(10)
defer p.Close()

// SubmitAction：提交无返回值动作
for _, item := range items {
    err := async.SubmitAction(p, ctx, func(ctx context.Context) error {
        return processItem(ctx, item)
    })
    if err != nil {
        log.Printf("提交失败: %v", err)
    }
}
p.Wait()

// GoAction：提交并断言成功（失败时 panic）
async.GoAction(p, ctx, func(ctx context.Context) error {
    return mustSucceed(ctx)
})

// TrySubmitAction：非阻塞提交
err := async.TrySubmitAction(p, ctx, func(ctx context.Context) error {
    return processItem(ctx)
})

// 带超时提交
err = async.SubmitActionWithTimeout(p, ctx, 3*time.Second, myFunc)
```

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

    // 1. 创建 IO 密集型协程池
    p := async.NewPool[string](async.IO())
    defer p.Close()

    // 2. 设置超时
    p.WithTimeout(30 * time.Second)

    // 3. 批量提交任务
    urls := []string{"url1", "url2", "url3", "url4", "url5"}
    for i, url := range urls {
        idx := i
        u := url
        err := p.Submit(ctx, func(ctx context.Context) (string, error) {
            // 模拟 HTTP 请求
            time.Sleep(200 * time.Millisecond)
            return fmt.Sprintf("[%d] fetched: %s", idx, u), nil
        })
        if err != nil {
            log.Printf("任务 %d 提交失败: %v", idx, err)
        }
    }

    // 4. 等待结果
    results, err := p.Wait()
    if err != nil {
        log.Printf("Wait 返回错误: %v", err)
    }

    // 5. 处理结果
    for i, r := range results {
        if r.Ok() {
            fmt.Println(r.Value)
        } else {
            log.Printf("任务 %d 失败: %v", i, r.Err)
        }
    }
}
```

---

## Pool 方法速查表

| 方法 | 说明 |
|------|------|
| `NewPool[T](size)` | 创建指定大小的泛型协程池 |
| `DefaultPool[T]()` | 使用默认 IO 并发度创建 |
| `NewNoResultPool(size)` | 创建无返回值池 |
| `Submit(ctx, fn)` | 阻塞提交任务 |
| `TrySubmit(ctx, fn)` | 非阻塞提交任务 |
| `SubmitAt(index, ctx, fn)` | 指定位置提交任务 |
| `TrySubmitAt(index, ctx, fn)` | 指定位置非阻塞提交 |
| `Wait()` | 阻塞等待所有任务完成 |
| `WaitTimeout(d)` | 带超时等待 |
| `WaitContext(ctx)` | Context 控制等待 |
| `Close()` | 关闭池 |
| `Size()` | Worker 数量 |
| `Active()` | 活跃 worker 数 |
| `Idle()` | 空闲 worker 数 |
| `Pending()` | 排队任务数 |

## NoResultPool 辅助函数速查表

| 函数 | 说明 |
|------|------|
| `SubmitAction(p, ctx, fn)` | 提交无返回值动作 |
| `TrySubmitAction(p, ctx, fn)` | 非阻塞提交无返回值动作 |
| `SubmitAtAction(p, idx, ctx, fn)` | 指定位置提交无返回值动作 |
| `TrySubmitAtAction(p, idx, ctx, fn)` | 指定位置非阻塞提交 |
| `GoAction(p, ctx, fn)` | 提交并断言成功（失败 panic） |
| `SubmitActionWithTimeout(p, ctx, d, fn)` | 带超时提交 |
| `SubmitAtActionWithTimeout(p, idx, ctx, d, fn)` | 指定位置带超时提交 |
| `GoActionWithTimeout(p, ctx, d, fn)` | 带超时提交并断言成功 |