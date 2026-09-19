# 任务组 (Group)

任务组 `Group[T]` / `NoResult` 适合**一次性批量并发任务**，每次 `Go` 新建 goroutine，任务完成后销毁。用完即走，不需要维护生命周期。

## 目录

- [核心概念](#核心概念)
- [创建任务组](#创建任务组)
- [提交任务](#提交任务)
- [等待结果](#等待结果)
- [高级选项](#高级选项)
- [NoResult 无返回值任务组](#noresult-无返回值任务组)
- [Group/NoResult 对比 Pool](#groupnoresult-对比-pool)
- [完整示例](#完整示例)

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

### Go（追加提交）

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

### GoAt（指定位置提交）

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

---

## 等待结果

### Wait

```go
// 阻塞等待所有任务完成
results := g.Wait()

for i, r := range results {
    if r.Ok() {
        fmt.Printf("成功: %v\n", r.Value)
    } else if r.IsPanic() {
        fmt.Printf("panic: %v\n", r.Err)
    } else {
        fmt.Printf("失败: %v\n", r.Err)
    }
}
```

### WaitTimeout

```go
// 最多等 5 秒
results := g.WaitTimeout(5 * time.Second)
```

### WaitContext

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
results := g.WaitContext(ctx)
```

**与 Pool 的重要区别**：Group 调用 Wait 后立即可以继续使用 `Go/GoAt`，不会被标记为 "已等待"。

---

## 高级选项

```go
g := async.NewGroup[int](4)

// 单个任务超时（覆盖全局默认值）
g.WithTimeout(10 * time.Second)

// 提交超时
g.WithSubmitTimeout(2 * time.Second)

// FailFast：一个任务失败，立即取消其他任务
g, ffCtx := g.WithFailFast(ctx)

// 组合使用：FailFast + 超时
g, ffCtx := g.WithFFTimeout(ctx, 5*time.Second)

// 带 TraceID
g, ctx := g.WithTraceID(ctx)

// FailFast + TraceID
g, ctx := g.WithFFTraceID(ctx)
```

### 查询 Group 状态

```go
// Worker 数量（即并发度）
size := g.Size()

// 当前活跃任务数
active := g.Active()

// 当前空闲槽位数
idle := g.Idle()

// 失败任务数
errCnt := g.ErrCount()

// 第一个错误
firstErr := g.FirstError()
```

---

## NoResult 无返回值任务组

适合批量写入、通知发送、文件下载等**只关心错误**的场景。

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

### NoResult 高级选项

```go
// FailFast
nr, ffCtx := nr.WithFailFast(ctx)

// FailFast + 提交超时
nr, ffCtx := nr.WithFFSubmitTO(ctx, 5*time.Second)

// FailFast + 超时
nr, ffCtx := nr.WithFFTimeout(ctx, 10*time.Second)

// 组合：FailFast + 超时 + 提交超时 + TraceID
nr, ffCtx := nr.WithFFTimeoutSubmitTOTraceID(ctx, 10*time.Second, 3*time.Second)
```

### NoResult 统计方法

| 方法 | 说明 |
|------|------|
| `SuccessCount()` | 成功任务数 |
| `FailCount()` | 失败任务数 |
| `ErrCount()` | 错误数 |
| `FirstError()` | 第一个错误 |
| `Stats()` | 完整统计信息 |

---

## Group/NoResult 对比 Pool

| 特性 | Group | Pool |
|------|-------|------|
| goroutine 复用 | ❌ 每次 Go 新建 | ✅ 复用固定数量 |
| 适用场景 | 一次性批量任务 | 长期运行反复提交 |
| 生命周期管理 | 用完即销毁 | 需手动 Close |
| 结果顺序保证 | ✅ GoAt 保证 | ✅ SubmitAt 保证 |
| Wait 后可继续提交 | ✅ | ❌ |

---

## 完整示例

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

    // 场景：并发查询用户信息并统计
    userIDs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

    // 创建并发度为 5 的任务组
    g := async.NewGroup[string](5)

    // 提交任务
    for i, uid := range userIDs {
        idx := i
        id := uid
        g.GoAt(idx, ctx, func(ctx context.Context) (string, error) {
            // 模拟查询用户信息
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

    fmt.Printf("成功查询 %d 个用户: %v\n", len(names), names)
}

func queryUser(ctx context.Context, id int) (string, error) {
    // 实际业务逻辑
    return fmt.Sprintf("User-%d", id), nil
}
```

---

## 方法速查表

| 方法 | 说明 |
|------|------|
| `NewGroup[T](concurrency)` | 创建泛型任务组 |
| `DefaultGroup[T]()` | 使用默认 IO 并发度创建 |
| `Go(ctx, fn)` | 追加提交任务 |
| `GoAt(index, ctx, fn)` | 指定位置提交任务 |
| `Wait()` | 阻塞等待所有任务 |
| `WaitTimeout(d)` | 带超时等待 |
| `WaitContext(ctx)` | Context 控制等待 |
| `WithTimeout(d)` | 设置超时 |
| `WithSubmitTimeout(d)` | 设置提交超时 |
| `WithFailFast(ctx)` | 开启 FailFast |
| `WithContext(ctx)` | 设置上下文 |
| `WithTraceID(ctx)` | 设置 TraceID |
| `Size()` | Worker 数量 |
| `Active()` | 活跃任务数 |
| `Idle()` | 空闲槽位数 |
| `ErrCount()` | 错误数量 |
| `FirstError()` | 第一个错误 |