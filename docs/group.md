# 任务组 (Group)

任务组 `Group[T]` / `NoResult` 适合**一次性批量并发任务**，每次 `Go` 新建 goroutine，任务完成后销毁。用完即走，不需要维护生命周期。

## 目录

- [核心概念](#核心概念)
- [创建任务组](#创建任务组)
- [提交任务](#提交任务)
- [等待结果](#等待结果)
- [高级选项](#高级选项)
- [FailFast 模式](#failfast-模式)
- [查询 Group 状态](#查询-group-状态)
- [结果提取](#结果提取)
- [带超时提交](#带超时提交)
- [Reset 重置](#reset-重置)
- [NoResult 无返回值任务组](#noresult-无返回值任务组)
- [Group/NoResult 对比 Pool](#groupnoresult-对比-pool)
- [完整示例](#完整示例)
- [方法速查表](#group-方法速查表)

---

## 核心概念

```
生命周期：NewGroup → Go / GoAt → Wait → 销毁
```

- **Group[T]**：每个任务返回 `Result[T]`，适合需要聚合结果的场景
- **NoResult**：每个任务只返回 error，适合批量写入、通知等场景

**何时用 Group 而非 Pool？**
- 一次性批量任务（如数据迁移、批量 API 调用）
- 任务数量已知且有限
- 不需要长期维护 goroutine 生命周期

---

## 创建任务组

```go
import "github.com/chichengyu/async"

// 创建并发度为 4 的任务组
g := async.NewGroup[string](4)

// 使用默认 IO 并发度创建
g := async.DefaultGroup[string]()

// 创建无返回值任务组
nr := async.NewNoResult(8)

// 使用默认并发度的无返回值任务组
nr := async.DefaultNoResult()
```

> 并发度 `<=0` 时默认为 1。

---

## 提交任务

### Go - 追加提交

```go
g := async.NewGroup[int](4)

// 提交任务，结果追加到结果列表末尾
g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchUserCount(ctx), nil
})

g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchOrderCount(ctx), nil
})

results := g.Wait()
// results[0] = fetchUserCount 的结果
// results[1] = fetchOrderCount 的结果
```

### GoAt - 指定位置提交

```go
// 指定结果数组索引，保证结果顺序
users := []string{"alice", "bob", "charlie"}
for i, user := range users {
    u := user
    idx := i
    g.GoAt(idx, ctx, func(ctx context.Context) (string, error) {
        return getUserInfo(ctx, u), nil
    })
}

results := g.Wait()
// results[0] 一定对应 "alice"
// results[1] 一定对应 "bob"
// results[2] 一定对应 "charlie"
```

### GoAt 注意事项

```go
// 跳空提交：位置 0 无任务，位置 0 的结果为 Result{T, Err: nil, Occupied: false}
g.GoAt(1, ctx, func(ctx context.Context) (string, error) {
    return "only position 1", nil
})
results := g.Wait()
// results[0]: Occupied=false, Err=nil
// results[1]: Value="only position 1"
```

---

## 等待结果

### Wait - 阻塞等待

```go
results := g.Wait()
// 此后不可再 Go（会返回 ErrGroupWaited）

for i, r := range results {
    if r.Ok() {
        fmt.Printf("任务 %d 成功: %v\n", i, r.Value)
    } else {
        log.Printf("任务 %d 失败: %v", i, r.Err)
    }
}
```

### WaitTimeout - 带超时等待

```go
results, ok := g.WaitTimeout(5 * time.Second)
if !ok {
    log.Println("等待超时，部分任务可能未完成")
}
```

### WaitContext - Context 控制等待

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

results, ok := g.WaitContext(ctx)
if !ok {
    log.Println("context 已取消")
}
```

---

## 高级选项

### 超时控制

```go
// 设置单个任务的超时（覆盖全局默认值）
g.WithTimeout(10 * time.Second)

// 设置 Go 等待并发槽位的超时
g.WithSubmitTimeout(2 * time.Second)
```

### Context 注入

```go
// 带取消能力的 context
g, ctx := g.WithContext(ctx)

// 带 TraceID
g, ctx := g.WithTraceID(ctx)

// Context + 超时
g, ctx := g.WithCtxTimeout(ctx, 10*time.Second)

// Context + 超时 + TraceID
g, ctx := g.WithCtxTimeoutTraceID(ctx, 10*time.Second)

// Context + 提交超时 + TraceID
g, ctx := g.WithCtxSubmitTOTraceID(ctx, 3*time.Second)
```

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

## FailFast 模式

```go
g := async.NewGroup[string](5)

// 开启 FailFast 模式
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
g, ffCtx := g.WithFailFast(ctx)

g.Go(ffCtx, someTask)
g.Go(ffCtx, anotherTask)
// 任一任务返回错误时，其他未运行的提交将被跳过（ErrSkipped）
```

---

## 查询 Group 状态

### Concurrency / Active / Idle / Busy

```go
// 并发度
fmt.Printf("并发度: %d\n", g.Concurrency())

// 活跃任务数
fmt.Printf("活跃任务: %d\n", g.Active())

// 空闲槽位数
fmt.Printf("空闲槽位: %d\n", g.Idle())

// 繁忙槽位数
fmt.Printf("繁忙槽位: %d\n", g.Busy())
```

### Stats - 完整统计

```go
stats := g.Stats()
fmt.Printf("GroupStats{concurrency=%d, active=%d, busy=%d, failFast=%v, timeout=%v, total=%d, success=%d, fail=%d}\n",
    stats.Concurrency, stats.Active, stats.Busy,
    stats.FailFast, stats.Timeout,
    stats.TotalTask, stats.SuccessTask, stats.FailTask)
```

### FailCount / SuccessCount / TotalCount / HasError

```go
results := g.Wait()

fmt.Printf("总数: %d, 成功: %d, 失败: %d\n",
    g.TotalCount(), g.SuccessCount(), g.FailCount())

if g.HasError() {
    log.Printf("存在失败的任务")
}
```

---

## 结果提取

### Values - 提取成功值

```go
results := g.Wait()
values := g.Values() // []T，只包含成功的值
fmt.Printf("成功值: %v\n", values)
```

### Errors - 提取所有错误

```go
errs := g.Errors() // []error
for i, e := range errs {
    log.Printf("错误 %d: %v", i, e)
}
```

### FirstError - 首个错误

```go
if firstErr := g.FirstError(); firstErr != nil {
    log.Printf("首个错误: %v", firstErr)
}
```

### JoinErrors - 合并错误

```go
if err := g.JoinErrors(); err != nil {
    if errors.Is(err, async.ErrTimeout) {
        log.Println("存在超时错误")
    }
}
```

---

## 带超时提交

### GoWithTimeout - 追加带超时提交

```go
g := async.NewGroup[string](5)

// 单个任务最多执行 3 秒
g.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) (string, error) {
    return fetchData(ctx)
})
```

### GoAtWithTimeout - 指定位置带超时提交

```go
// 位置 0，单个任务最多执行 3 秒
g.GoAtWithTimeout(0, ctx, 3*time.Second, func(ctx context.Context) (string, error) {
    return fetchData(ctx)
})
```

---

## Reset 重置

Wait 后需要再次使用同一个 Group 实例时调用 Reset：

```go
g := async.NewGroup[string](5)
g.Go(ctx, task1)
results := g.Wait()

// Wait 后再次使用需要 Reset
newG, err := g.Reset()
if err != nil {
    log.Printf("重置失败: %v", err)
}
newG.Go(ctx, task2)
newResults := newG.Wait()
```

> 调用 Reset 前必须已完成 Wait()，否则返回错误。

---

## NoResult 无返回值任务组

适合批量写入、通知发送、文件下载等**只关心错误**的场景。

### 基本使用

```go
nr := async.NewNoResult(8)

// 批量发送通知
for _, user := range users {
    u := user
    nr.Go(ctx, func(ctx context.Context) error {
        return sendNotification(ctx, u, message)
    })
}

// 等待完成
nr.Wait()

// 检查是否有错误
if err := nr.FirstError(); err != nil {
    log.Printf("通知发送失败: %v", err)
}

// 查看统计
fmt.Printf("成功: %d, 失败: %d\n", nr.SuccessCount(), nr.FailCount())
```

### NoResult - Go / GoWithTimeout / GoAt / GoAtWithTimeout

```go
nr := async.NewNoResult(4)

// 追加提交
nr.Go(ctx, func(ctx context.Context) error {
    return processItem(ctx)
})

// 追加提交带超时
nr.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) error {
    return processItemWithTimeout(ctx)
})

// 指定位置提交
nr.GoAt(0, ctx, func(ctx context.Context) error {
    return processFirst(ctx)
})

// 指定位置带超时提交
nr.GoAtWithTimeout(1, ctx, 3*time.Second, func(ctx context.Context) error {
    return processSecond(ctx)
})

nr.Wait()
```

### NoResult 高级选项

```go
// FailFast
nr, ffCtx := nr.WithFailFast(ctx)

// FailFast + 提交超时
nr, ffCtx := nr.WithFFSubmitTO(ctx, 5*time.Second)

// FailFast + 超时
nr, ffCtx := nr.WithFFTimeout(ctx, 10*time.Second)

// FailFast + 超时 + 提交超时 + TraceID
nr, ffCtx := nr.WithFFTimeoutSubmitTOTraceID(ctx, 10*time.Second, 3*time.Second)
```

### NoResult 统计方法

| 方法 | 说明 |
|------|------|
| `SuccessCount()` | 成功任务数 |
| `FailCount()` | 失败任务数 |
| `TotalCount()` | 总任务数 |
| `HasError()` | 是否有错误 |
| `FirstError()` | 第一个错误 |
| `Errors()` | 所有错误 |
| `JoinErrors()` | 合并所有错误 |
| `Stats()` | 完整统计信息 |
| `Concurrency()` | 并发度 |
| `Active()` | 活跃任务数 |
| `Busy()` | 繁忙任务数 |
| `WaitTimeout(d)` | 带超时等待 |
| `WaitContext(ctx)` | Context 控制等待 |
| `Reset()` | 重置 |

---

## Group/NoResult 对比 Pool

| 特性 | Group | Pool |
|------|-------|------|
| goroutine 复用 | ❌ 每次 Go 新建 | ✅ 复用固定数量 |
| 适用场景 | 一次性批量任务 | 长期运行反复提交 |
| 生命周期管理 | 用完即销毁 | 需手动 Close |
| 结果顺序保证 | ✅ GoAt 保证 | ✅ SubmitAt 保证 |
| Wait 后可继续提交 | ✅ (需 Reset) | ❌ |
| 动态调整并发度 | ❌ | ✅ Resize |

---

## 完整示例

### 示例1：并发查询用户信息

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/chichengyu/async"
)

func main() {
    ctx := context.Background()
    ctx = async.EnsureTraceID(ctx)

    userIDs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

    // 创建并发度为 5 的任务组
    g := async.NewGroup[string](5)

    // 提交任务（保证结果顺序）
    for i, uid := range userIDs {
        idx := i
        id := uid
        g.GoAt(idx, ctx, func(ctx context.Context) (string, error) {
            name, err := queryUser(ctx, id)
            if err != nil {
                return "", fmt.Errorf("查询用户 %d 失败: %w", id, err)
            }
            return name, nil
        })
    }

    // 等待结果
    results := g.Wait()

    // 处理结果
    var names []string
    for _, r := range results {
        if r.Ok() {
            names = append(names, r.Value)
        } else {
            log.Printf("任务失败: %v", r.Err)
        }
    }

    // 统计
    fmt.Printf("成功查询 %d 个用户: %v\n", len(names), names)
    fmt.Printf("总数: %d, 成功: %d, 失败: %d\n",
        g.TotalCount(), g.SuccessCount(), g.FailCount())
}

func queryUser(ctx context.Context, id int) (string, error) {
    return fmt.Sprintf("User-%d", id), nil
}
```

### 示例2：NoResult 批量文件下载

```go
func downloadFiles(urls []string) error {
    ctx := context.Background()
    nr := async.NewNoResult(8)
    nr.WithTimeout(30 * time.Second) // 每个文件最多 30 秒

    for _, url := range urls {
        u := url
        nr.Go(ctx, func(ctx context.Context) error {
            return downloadFile(ctx, u)
        })
    }

    nr.Wait()

    // 检查结果
    stats := nr.Stats()
    fmt.Printf("下载完成: 成功=%d 失败=%d\n", stats.SuccessTask, stats.FailTask)

    if err := nr.FirstError(); err != nil {
        log.Printf("首个失败: %v", err)
        // 合并所有错误
        if joinedErr := nr.JoinErrors(); joinedErr != nil {
            return fmt.Errorf("批量下载失败: %w", joinedErr)
        }
    }
    return nil
}
```

### 示例3：FailFast + 验证

```go
func validateAll(items []string) error {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    g, ffCtx := async.NewGroup[bool](3).WithFailFast(ctx)

    for i, item := range items {
        idx := i
        it := item
        g.GoAt(idx, ffCtx, func(ctx context.Context) (bool, error) {
            if !isValid(it) {
                return false, fmt.Errorf("验证失败: %s", it)
            }
            return true, nil
        })
    }

    results := g.Wait()

    // 检查结果
    if g.HasError() {
        return fmt.Errorf("验证失败 (首个): %w", g.FirstError())
    }

    // 全部通过
    values := g.Values()
    fmt.Printf("全部验证通过，共 %d 项\n", len(values))
    return nil
}
```

---

## Group 方法速查表

### 创建

| 函数 | 说明 |
|------|------|
| `NewGroup[T](concurrency)` | 创建泛型任务组 |
| `DefaultGroup[T]()` | 使用默认 IO 并发度创建 |
| `NewNoResult(concurrency)` | 创建无返回值任务组 |
| `DefaultNoResult()` | 创建默认并发度无返回值任务组 |

### 提交

| 方法 | 说明 |
|------|------|
| `Go(ctx, fn)` | 追加到任务队列 |
| `GoAt(index, ctx, fn)` | 指定结果位置添加任务 |
| `GoWithTimeout(ctx, d, fn)` | 追加带超时任务 |
| `GoAtWithTimeout(index, ctx, d, fn)` | 指定位置带超时任务 |

### 等待

| 方法 | 说明 |
|------|------|
| `Wait()` | 阻塞等待，返回全部结果 |
| `WaitTimeout(d)` | 带超时等待 |
| `WaitContext(ctx)` | Context 控制等待 |

### 高级选项

| 方法 | 说明 |
|------|------|
| `WithTimeout(d)` | 设置超时 |
| `WithSubmitTimeout(d)` | 设置提交超时 |
| `WithFailFast(ctx)` | 开启 FailFast |
| `WithContext(ctx)` | 设置上下文 |
| `WithTraceID(ctx)` | 设置 TraceID |
| ... 及其组合变体 | (共 16 种组合) |

### 查询

| 方法 | 说明 |
|------|------|
| `Concurrency()` | Worker 数量 |
| `Active()` | 活跃任务数 |
| `Idle()` | 空闲槽位数 |
| `Busy()` | 繁忙槽位数 |
| `Stats()` | 完整统计 GroupStats |

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
| `Reset()` | 关闭旧组创建新组 |