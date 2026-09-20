# 协程池 (Pool)

协程池 `Pool[T]` 是 async 库的核心组件之一，它复用固定数量的 goroutine 来处理高频并发任务。适合**长期运行、反复提交任务**的场景。

## 目录

- [核心概念](#核心概念)
- [创建池](#创建池)
- [提交任务](#提交任务)
- [等待与关闭](#等待与关闭)
- [高级选项](#高级选项)
- [查询与统计](#查询与统计)
- [结果提取](#结果提取)
- [动态扩容](#动态扩容)
- [重置](#重置)
- [NoResultPool](#noresultpool)
- [便捷函数](#便捷函数)
- [完整示例](#完整示例)
- [方法速查表](#方法速查表)

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

### Submit - 阻塞提交

排队等待，直到有空闲 worker 或 ctx 被取消：

```go
p := async.NewPool[string](4)
defer p.Close()

for i := 0; i < 100; i++ {
    idx := i
    err := p.Submit(ctx, func(ctx context.Context) (string, error) {
        return fmt.Sprintf("result-%d", idx), nil
    })
    if err != nil {
        log.Printf("提交失败: %v", err)
    }
}

results := p.Wait()
```

### TrySubmit - 非阻塞提交

队列满时立即返回 `ErrSubmitTimeout`，不等待：

```go
err := p.TrySubmit(ctx, func(ctx context.Context) (int, error) {
    return processItem(ctx), nil
})
if errors.Is(err, async.ErrSubmitTimeout) {
    log.Println("池已满，任务被拒绝")
}
```

### SubmitAt - 指定位置阻塞提交

指定结果数组的索引位置，保证与输入顺序一致：

```go
// 结果数组按 items 原始索引排列
for i, item := range items {
    p.SubmitAt(i, ctx, func(ctx context.Context) (int, error) {
        return processItem(ctx, item), nil
    })
}

results := p.Wait()
// results[0] 对应 items[0]
// results[1] 对应 items[1]
// ...
```

### TrySubmitAt - 指定位置非阻塞提交

```go
err := p.TrySubmitAt(3, ctx, func(ctx context.Context) (string, error) {
    return fetchData(ctx), nil
})
if err != nil {
    log.Printf("位置 3 提交失败: %v", err)
}
```

---

## 等待与关闭

### Wait - 阻塞等待

阻塞直到所有已提交任务执行完成，返回全部结果：

```go
results := p.Wait()
// 此后不可再 Submit（会返回 ErrPoolWaited）

for i, r := range results {
    if r.Ok() {
        fmt.Printf("任务 %d 成功: %v\n", i, r.Value)
    } else {
        log.Printf("任务 %d 失败: %v", i, r.Err)
    }
}
```

### WaitTimeout - 带超时等待

指定超时时间，超时后返回已完成的 results 和 `ok=false`：

```go
results, ok := p.WaitTimeout(5 * time.Second)
if !ok {
    log.Println("等待超时，部分任务可能未完成")
}
// 即使超时也可读取 results，但未完成的任务值可能为空
```

### WaitContext - Context 控制等待

通过 context 的 Done 信号控制等待截止：

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

results, ok := p.WaitContext(ctx)
if !ok {
    log.Println("context 已取消")
}
```

### WaitAndClose - 等待后自动关闭

等价于 `Wait()` + `Close()`：

```go
results := p.WaitAndClose()
// 无需再调用 p.Close()
```

### Close - 关闭池

关闭任务队列，等待所有 worker 退出：

```go
p.Close()
// 此后不可再 Submit（会返回 ErrPoolClosed）
```

### CloseAndWait - 关闭后等待 worker 退出

先关闭队列，然后等待所有 worker 处理完队列中剩余任务：

```go
p.CloseAndWait()
```

### CloseAndWaitTimeout - 带超时的关闭等待

```go
ok, workerDone := p.CloseAndWaitTimeout(30 * time.Second)
if !ok {
    log.Println("关闭超时，worker 可能仍在运行")
    // 可通过 workerDone channel 异步等待
    go func() {
        <-workerDone
        log.Println("所有 worker 已退出")
    }()
}
```

### CloseByIdle - 空闲后关闭

等待所有活跃任务完成，最多等待指定时长后强制关闭：

```go
// 最多等 30 秒让任务自然完成
p.CloseByIdle(30 * time.Second)
```

### CloseByIdle 完整等待（无限等）

```go
// 无限等待直到所有任务完成
p.CloseByIdle(0)
```

---

## 高级选项

### 超时控制

```go
// 设置单个任务的超时（覆盖全局默认值）
p.WithTimeout(10 * time.Second)

// 设置 Submit 等待空闲 worker 的超时
p.WithSubmitTimeout(3 * time.Second)
```

### FailFast 模式

一个任务失败立即取消其他任务：

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

p, ffCtx := p.WithFailFast(ctx)

p.Submit(ffCtx, criticalTask)
p.Submit(ffCtx, anotherTask)
// 任一任务返回错误时，其他未执行的提交会被跳过（ErrSkipped）
```

### Context 注入

```go
// 带取消能力的 context
p, ctx := p.WithContext(ctx)

// 带 TraceID
p, ctx := p.WithTraceID(ctx)

// FailFast + 超时 + Context
p, ctx := p.WithFFTimeout(ctx, 10*time.Second)

// FailFast + 超时 + 提交超时 + TraceID
p, ctx := p.WithFFTimeoutSubmitTOTraceID(ctx, 10*time.Second, 3*time.Second)

// Context + 超时 + TraceID
p, ctx := p.WithCtxTimeoutTraceID(ctx, 10*time.Second)

// Context + 提交超时 + TraceID
p, ctx := p.WithCtxSubmitTOTraceID(ctx, 3*time.Second)
```

> 所有 With* 方法返回新的 Pool 引用（原 Pool 被修改），可用于链式调用。

### With 方法完整速查

| 方法 | 说明 |
|------|------|
| `WithTimeout(d)` | 设置单个任务超时 |
| `WithSubmitTimeout(d)` | 设置提交等待超时 |
| `WithContext(ctx)` | 注入 Context（带取消） |
| `WithTraceID(ctx)` | 确保 context 有 trace_id |
| `WithFailFast(ctx)` | 开启 FailFast 模式 |
| `WithFFCtx(ctx)` | = WithFailFast |
| `WithFFTraceID(ctx)` | FailFast + TraceID |
| `WithFFSubmitTO(ctx, d)` | FailFast + 提交超时 |
| `WithFFSubmitTOTraceID(ctx, d)` | FailFast + 提交超时 + TraceID |
| `WithFFTimeout(ctx, d)` | FailFast + 超时 |
| `WithFFTimeoutTraceID(ctx, d)` | FailFast + 超时 + TraceID |
| `WithFFTimeoutSubmitTO(ctx, d, sd)` | FailFast + 超时 + 提交超时 |
| `WithFFTimeoutSubmitTOTraceID(ctx, d, sd)` | FailFast + 超时 + 提交超时 + TraceID |
| `WithCtxTraceID(ctx)` | Context + TraceID |
| `WithCtxTimeout(ctx, d)` | Context + 超时 |
| `WithCtxTimeoutTraceID(ctx, d)` | Context + 超时 + TraceID |
| `WithCtxSubmitTO(ctx, d)` | Context + 提交超时 |
| `WithCtxSubmitTOTraceID(ctx, d)` | Context + 提交超时 + TraceID |

---

## 查询与统计

### Size / Active / Busy / Idle / Pending

```go
// Worker 数量（并发度）
fmt.Printf("Worker 数量: %d\n", p.Size())

// 正在执行任务的 worker 数
fmt.Printf("活跃数: %d\n", p.Active())

// 繁忙 worker 数（处理任务中）
fmt.Printf("繁忙数: %d\n", p.Busy())

// 空闲 worker 数
fmt.Printf("空闲数: %d\n", p.Idle())

// 排队等待的任务数
fmt.Printf("排队数: %d\n", p.Pending())
```

### Stats - 完整统计

```go
stats := p.Stats()
fmt.Printf("PoolStats{size=%d, active=%d, busy=%d, pending=%d, failFast=%v, timeout=%v, total=%d, success=%d, fail=%d}\n",
    stats.Size, stats.Active, stats.Busy, stats.Pending,
    stats.FailFast, stats.Timeout,
    stats.TotalTask, stats.SuccessTask, stats.FailTask)
```

### FailCount / SuccessCount / TotalCount / HasError

```go
results := p.Wait()

fmt.Printf("总数: %d, 成功: %d, 失败: %d\n",
    p.TotalCount(), p.SuccessCount(), p.FailCount())

if p.HasError() {
    log.Printf("存在失败的任务")
}
```

---

## 结果提取

### Values - 提取所有成功值

```go
results := p.Wait()
values := p.Values() // []T，只包含 Err==nil 的值
fmt.Printf("成功值: %v\n", values)
```

### Errors - 提取所有错误

```go
errs := p.Errors() // []error
for i, e := range errs {
    log.Printf("错误 %d: %v", i, e)
}
```

### FirstError - 第一个错误

```go
if firstErr := p.FirstError(); firstErr != nil {
    log.Printf("首个错误: %v", firstErr)
}
```

### JoinErrors - 合并所有错误

```go
if err := p.JoinErrors(); err != nil {
    // 使用 errors.Is / errors.As 判断具体错误
    if errors.Is(err, async.ErrTimeout) {
        log.Println("存在超时错误")
    }
}
```

---

## 动态扩容

### Resize - 调整 Worker 数量

运行时动态调整 worker 数量。扩容立即生效，缩容会通知多余 worker 退出：

```go
// 扩容到 20 个 worker
oldSize := p.Resize(20)
fmt.Printf("从 %d 扩容到 20\n", oldSize)

// 缩容到 5 个 worker
p.Resize(5)
```

### ResizeAndWaitTimeout - 调整并等待

调整大小后等待指定时间让旧 worker 清理退出：

```go
// 缩容到 5，等待 10 秒让旧 worker 退出
p.ResizeAndWaitTimeout(5, 10*time.Second)
```

---

## 自动扩缩容 (AutoScale)

Pool 支持根据负载**自动调整 worker 数量**。初始 worker 数可以很小（如 4），高并发时自动扩容，低负载时自动缩容。**默认不启用**，需显式调用 `EnableAutoScale`。

### EnableAutoScale - 启用自动扩缩容

```go
p := async.NewPool[int](4)
defer p.Close()

// 方式一：使用默认配置启用
p.EnableAutoScale(nil)

// 方式二：自定义配置
p.EnableAutoScale(&async.AutoScaleConfig{
    MinWorkers:       4,
    MaxWorkers:       2000,
    CheckInterval:    5 * time.Second,
    ScaleUpThreshold: 0.7,   // busy/size > 0.7 触发扩容
    ScaleDownThreshold: 0.2, // busy/size < 0.2 触发缩容
    ScaleUpChecks:    3,     // 连续 3 次满足条件才扩容（防抖动）
    ScaleDownChecks:  5,     // 连续 5 次满足条件才缩容（防抖动）
})
```

**扩容规则**：busy/size 比率超过 `ScaleUpThreshold` 持续 `ScaleUpChecks` 次 → worker 翻倍（上限 MaxWorkers）  
**缩容规则**：busy/size 比率低于 `ScaleDownThreshold` 持续 `ScaleDownChecks` 次 → worker 减半（下限 MinWorkers）

### 默认配置

```go
// DefaultAutoScaleConfig 返回：
// MinWorkers=CPU×2, MaxWorkers=CPU×100, CheckInterval=5s
// ScaleUpThreshold=0.7, ScaleDownThreshold=0.2
// ScaleUpChecks=3, ScaleDownChecks=5
config := async.DefaultAutoScaleConfig()
```

### DisableAutoScale - 禁用自动扩缩容

```go
p.DisableAutoScale()
// 禁用后 worker 数恢复为 MinWorkers
```

### IsAutoScaleEnabled - 检查状态

```go
if p.IsAutoScaleEnabled() {
    fmt.Println("auto-scale is active")
}
```

### NewAutoScalePool - 快捷创建

```go
// 创建初始 4 worker、自动扩缩容的池
p := async.NewAutoScalePool[int](4, nil)
defer p.Close()

// 自定义配置
p2 := async.NewAutoScalePool[int](4, &async.AutoScaleConfig{
    MinWorkers: 2,
    MaxWorkers: 500,
})
```

### 线程安全

`EnableAutoScale` 可重复调用（幂等），`Resize` 与自动扩缩容并发调用安全，`Close` 自动停止后台检测 goroutine。

---

## 重置

### Reset - 关闭旧池创建新池

Wait 后需要继续使用池时调用 Reset：

```go
p.Submit(ctx, task1)
results := p.Wait()

// Wait 后再次使用需要 Reset
newPool, err := p.Reset()
if err != nil {
    log.Printf("重置失败: %v", err)
}
newPool.Submit(ctx, task2)
newPool.Wait()
```

---

## NoResultPool

无返回值协程池 `Pool[struct{}]` 的别名，适合只关心错误的场景。推荐配合辅助函数使用。

### 使用辅助函数提交

```go
p := async.NewNoResultPool(10)
defer p.Close()

// SubmitAction: 提交无返回值动作（阻塞）
async.SubmitAction(p, ctx, func(ctx context.Context) error {
    return sendEmail(ctx, user)
})

// TrySubmitAction: 非阻塞提交
err := async.TrySubmitAction(p, ctx, func(ctx context.Context) error {
    return logMetrics(ctx, data)
})
if err != nil {
    log.Printf("提交失败: %v", err)
}

// SubmitAtAction: 指定位置提交
async.SubmitAtAction(p, 0, ctx, func(ctx context.Context) error {
    return processFirst(ctx, input)
})

// TrySubmitAtAction: 指定位置非阻塞提交
async.TrySubmitAtAction(p, 1, ctx, func(ctx context.Context) error {
    return processSecond(ctx, input)
})

// GoAction: 提交并断言成功（失败 panic）
async.GoAction(p, ctx, func(ctx context.Context) error {
    return mustSucceed(ctx, data)
})

// SubmitActionWithTimeout: 带超时提交
async.SubmitActionWithTimeout(p, ctx, 5*time.Second, func(ctx context.Context) error {
    return heavyOperation(ctx)
})

// SubmitAtActionWithTimeout: 指定位置带超时提交
async.SubmitAtActionWithTimeout(p, 0, ctx, 3*time.Second, func(ctx context.Context) error {
    return criticalTask(ctx)
})

// GoActionWithTimeout: 带超时提交并断言成功
async.GoActionWithTimeout(p, ctx, 5*time.Second, func(ctx context.Context) error {
    return mustSucceedWithTimeout(ctx)
})

// 等待完成
p.Wait()

// 检查结果
if p.HasError() {
    log.Printf("首个错误: %v", p.FirstError())
}
fmt.Printf("成功: %d, 失败: %d\n", p.SuccessCount(), p.FailCount())
```

---

## 便捷函数

一行代码快速创建池并提交任务：

### Submit - 快速创建池提交单个任务

```go
p, idx, err := async.Submit(ctx, func(ctx context.Context) (string, error) {
    return fetchData(ctx), nil
})
if err != nil {
    log.Printf("提交失败: %v", err)
}
defer p.Close()
results := p.Wait()
```

### SubmitN - 快速提交 N 个相同任务

```go
p, results, err := async.SubmitN(ctx, func(ctx context.Context) (int, error) {
    return rand.Intn(100), nil
}, 10) // 提交 10 个任务
defer p.Close()
for _, r := range results {
    if r.Err != nil {
        log.Printf("任务 %d 提交失败: %v", r.Index, r.Err)
    }
}
waitResults := p.Wait()
```

### SubmitSafeN - 提交 N 个（忽略提交失败）

```go
// 不返回 error，内部自动处理
p, results := async.SubmitSafeN(ctx, fn, 10)
defer p.Close()
waitResults := p.Wait()
```

### SubmitBatch - 批量提交切片元素

```go
items := []string{"url1", "url2", "url3", "url4"}

p, results, err := async.SubmitBatch(ctx, items, func(ctx context.Context, url string) (string, error) {
    return httpGet(ctx, url)
})
defer p.Close()
if err == nil {
    waitResults := p.Wait()
    for _, r := range waitResults {
        if r.Ok() {
            fmt.Println(r.Value)
        }
    }
}
```

### MapPool - 池化 Map

使用 Pool 执行并发 Map（可获取池实例进行进一步控制）：

```go
p, results, err := async.MapPool(ctx, urls, func(ctx context.Context, url string) (string, error) {
    return httpGet(ctx, url)
}, async.IO())
// p 已自动 Wait + Close
fmt.Printf("总数: %d, 失败: %d\n", p.TotalCount(), p.FailCount())
```

### ForEachPool - 池化 ForEach

```go
p, err := async.ForEachPool(ctx, users, func(ctx context.Context, user string) error {
    return sendNotification(ctx, user)
}, async.IO())
// p 已自动 Wait + Close
fmt.Printf("成功: %d, 失败: %d\n", p.SuccessCount(), p.FailCount())
```

---

## 完整示例

### 示例1：Worker 池处理 HTTP 请求

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

    // 创建 4 个 worker 的池，任务超时 5 秒
    p := async.NewPool[int](4)
    defer p.Close()
    p.WithTimeout(5 * time.Second)

    // 提交 100 个任务
    for i := 0; i < 100; i++ {
        idx := i
        err := p.Submit(ctx, func(ctx context.Context) (int, error) {
            time.Sleep(100 * time.Millisecond)
            return idx * idx, nil
        })
        if err != nil {
            log.Printf("任务 %d 提交失败: %v", idx, err)
        }
    }

    // 等待并获取结果
    results := p.Wait()

    // 处理结果
    for _, r := range results {
        if r.Ok() {
            fmt.Printf("结果: %d\n", r.Value)
        } else {
            log.Printf("失败: %v", r.Err)
        }
    }

    // 统计
    fmt.Printf("总数: %d, 成功: %d, 失败: %d, 首个错误: %v\n",
        p.TotalCount(), p.SuccessCount(), p.FailCount(), p.FirstError())
}
```

### 示例2：FailFast + 动态扩容

```go
func processWithFailFast() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    p := async.NewPool[string](4)
    defer p.Close()

    // 开启 FailFast + 超时
    p, ffCtx := p.WithFFTimeout(ctx, 30*time.Second)

    // 加载中扩容
    p.Resize(8)

    urls := []string{"url1", "url2", "url3"}
    for i, url := range urls {
        p.SubmitAt(i, ffCtx, func(ctx context.Context) (string, error) {
            return fetchURL(ctx, url)
        })
    }

    results := p.Wait()

    // 检查统计
    stats := p.Stats()
    if stats.FailTask > 0 {
        log.Printf("有 %d 个任务失败: %v", stats.FailTask, p.FirstError())
    }

    for _, r := range results {
        if r.Ok() {
            fmt.Println(r.Value)
        }
    }
}
```

### 示例3：NoResultPool 批量处理

```go
func batchSendEmails(users []string) {
    ctx := context.Background()
    p := async.NewNoResultPool(10)
    defer p.Close()

    for _, user := range users {
        u := user
        async.SubmitAction(p, ctx, func(ctx context.Context) error {
            return emailService.Send(ctx, u, "Hello!")
        })
    }

    p.Wait()

    if p.HasError() {
        log.Printf("发送失败: %v", p.FirstError())
    }
    fmt.Printf("发送: 成功 %d, 失败 %d\n", p.SuccessCount(), p.FailCount())
}
```

---

## Pool 方法速查表

### 创建

| 函数 | 说明 |
|------|------|
| `NewPool[T](size)` | 创建指定大小的泛型协程池 |
| `DefaultPool[T]()` | 使用默认 IO 并发度创建 |
| `NewNoResultPool(size)` | 创建无返回值池 |
| `DefaultNoResultPool()` | 创建默认并发度无返回值池 |

### 提交

| 方法 | 说明 |
|------|------|
| `Submit(ctx, fn)` | 阻塞提交任务 |
| `TrySubmit(ctx, fn)` | 非阻塞提交（队列满返回 ErrSubmitTimeout） |
| `SubmitAt(index, ctx, fn)` | 指定结果索引位置阻塞提交 |
| `TrySubmitAt(index, ctx, fn)` | 指定结果索引位置非阻塞提交 |

### 等待与关闭

| 方法 | 说明 |
|------|------|
| `Wait()` | 阻塞等待，返回全部结果 |
| `WaitTimeout(d)` | 带超时等待，返回 `([]Result, bool)` |
| `WaitContext(ctx)` | Context 控制等待 |
| `WaitAndClose()` | Wait 后自动 Close |
| `Close()` | 关闭任务队列，等待 worker 退出 |
| `CloseAndWait()` | 关闭后等待 worker 处理完剩余任务 |
| `CloseAndWaitTimeout(d)` | 带超时关闭 |
| `CloseByIdle(d)` | 空闲后关闭，最多等 d |

### 高级选项

| 方法 | 说明 |
|------|------|
| `WithTimeout(d)` | 设置任务超时 |
| `WithSubmitTimeout(d)` | 设置提交等待超时 |
| `WithContext(ctx)` | 注入 Context |
| `WithTraceID(ctx)` | 确保 trace_id |
| `WithFailFast(ctx)` | 开启 FailFast |
| `WithFFTimeout(ctx, d)` | FailFast + 超时 |
| `WithFFSubmitTO(ctx, d)` | FailFast + 提交超时 |
| `WithCtxTimeout(ctx, d)` | Context + 超时 |
| ... 及其 TraceID 组合变体 | (共 16 种组合) |

### 查询

| 方法 | 说明 |
|------|------|
| `Size()` | Worker 数量 |
| `Active()` | 活跃 worker 数 |
| `Busy()` | 繁忙 worker 数 |
| `Idle()` | 空闲 worker 数 |
| `Pending()` | 排队任务数 |
| `Stats()` | 完整统计 PoolStats |

### 结果提取

| 方法 | 说明 |
|------|------|
| `Values()` | 成功值切片 |
| `Errors()` | 错误切片 |
| `FirstError()` | 首个错误 |
| `JoinErrors()` | 合并所有错误 |
| `FailCount()` | 失败数 |
| `SuccessCount()` | 成功数 |
| `TotalCount()` | 总数 |
| `HasError()` | 是否有错误 |

### 动态管理

| 方法 | 说明 |
|------|------|
| `Resize(newSize)` | 调整 worker 数量 |
| `ResizeAndWaitTimeout(newSize, d)` | 调整并等待 |
| `Reset()` | 关闭旧池创建新池 |

### NoResultPool 辅助函数

| 函数 | 说明 |
|------|------|
| `SubmitAction(p, ctx, fn)` | 阻塞提交无返回值动作 |
| `TrySubmitAction(p, ctx, fn)` | 非阻塞提交 |
| `SubmitAtAction(p, idx, ctx, fn)` | 指定位置阻塞提交 |
| `TrySubmitAtAction(p, idx, ctx, fn)` | 指定位置非阻塞提交 |
| `GoAction(p, ctx, fn)` | 提交并断言成功（失败 panic） |
| `SubmitActionWithTimeout(p, ctx, d, fn)` | 带超时提交 |
| `SubmitAtActionWithTimeout(p, idx, ctx, d, fn)` | 指定位置带超时提交 |
| `GoActionWithTimeout(p, ctx, d, fn)` | 带超时提交并断言成功 |

### 便捷函数

| 函数 | 说明 |
|------|------|
| `Submit(ctx, fn)` | 快速创建池提交单个任务 |
| `SubmitN(ctx, fn, n)` | 提交 N 个相同任务 |
| `SubmitSafeN(ctx, fn, n)` | 提交 N 个（忽略失败） |
| `SubmitBatch(ctx, items, fn)` | 批量提交切片元素 |
| `MapPool(ctx, items, fn, c)` | 池化 Map |
| `ForEachPool(ctx, items, fn, c)` | 池化 ForEach |

### 类型

| 类型 | 说明 |
|------|------|
| `Pool[T]` | 泛型协程池 |
| `NoResultPool` | 无返回值协程池 |
| `PoolStats` | 统计信息结构体 |
| `SubmitResult` | 提交结果（包含 Index 和 Err） |