# 重试机制 (Retry)

重试模块提供指数退避、线性退避和每次调用超时控制等重试策略，适用于调用外部 API、数据库连接等可能发生瞬时故障的场景。

## 目录

- [核心概念](#核心概念)
- [指数退避重试](#指数退避重试)
- [线性退避重试](#线性退避重试)
- [带每次调用超时的重试](#带每次调用超时的重试)
- [Result\[T\] 返回值便捷重试](#resultt-返回值便捷重试)
- [简单函数式重试](#简单函数式重试)
- [超时与截止时间包装](#超时与截止时间包装)
- [Worker 绑定重试](#worker-绑定重试)
- [完整示例](#完整示例)

---

## 核心概念

```
重试策略：
  无退避：每次立即重试
  指数退避：100ms → 200ms → 400ms → 800ms → ...
  线性退避：1s → 1s → 1s → 1s → ...

退避公式（指数）：
  backoff = min(initialBackoff × 2^attempt, maxBackoff)

重试终止条件：
  1. fn 返回 nil（成功）
  2. 达到最大重试次数
  3. ctx 被取消
```

---

## 指数退避重试

### Retry（无退避，立即重试）

```go
// 最多尝试 5 次（1 次初始 + 4 次重试）
err := async.Retry(ctx, 4, func(ctx context.Context) error {
    return db.Ping(ctx)
})
```

### RetryWithBackoff

```go
// 最多重试 3 次，初始退避 100ms
// 退避序列: 100ms → 200ms → 400ms
err := async.RetryWithBackoff(ctx, 3, 100*time.Millisecond, func(ctx context.Context) error {
    return callExternalAPI(ctx, request)
})

// 设置最大退避上限 5 秒
err := async.RetryWithBackoff(ctx, 5, 100*time.Millisecond, func(ctx context.Context) error {
    return callExternalAPI(ctx, request)
})
```

> `RetryWithBackoff` 的第三个参数是 `maxBackoff`。当设为 `0` 时不设上限，退避会无限翻倍。

---

## 线性退避重试

每次重试等待相同的退避时间：

```go
// 每次失败后等 1 秒，最多重试 5 次
err := async.RetryWithLinearBackoff(ctx, 5, 1*time.Second, func(ctx context.Context) error {
    return callSlowService(ctx, data)
})
```

---

## 带每次调用超时的重试

`RetryWithConfig` 相比 `RetryWithBackoff` 增加了 `PerCallTimeout`，控制每次 fn 调用的超时：

```go
result, err := async.RetryWithConfig(ctx,
    func(ctx context.Context) (*Data, error) {
        return fetchData(ctx, id)
    },
    3,                      // 最多重试 3 次
    100*time.Millisecond,   // 初始退避
    5*time.Second,          // 最大退避
    async.TimeoutOpt{PerCallTimeout: 2 * time.Second}, // 每次调用最多 2 秒
)

if err != nil {
    // 可能的重试耗尽错误
    log.Printf("重试耗尽: %v", err)
}
```

**重要**：`RetryWithConfig` 会区分 `context.DeadlineExceeded` 和 `context.Canceled` 错误，这两种错误**不会触发重试**。

---

## Result[T] 返回值便捷重试

当不需要区分 `context.DeadlineExceeded` / `context.Canceled` 时，使用返回 `Result[T]` 的便捷方法更简洁：

```go
// RetryWithBackoffResult：指数退避，返回 Result[T]
r := async.RetryWithBackoffResult(ctx, func(ctx context.Context) (*Data, error) {
    return fetchData(ctx, id)
}, 3, 100*time.Millisecond, 5*time.Second)

if r.Ok() {
    fmt.Println(r.Value)
} else {
    log.Printf("重试失败: %v", r.Err)
}

// RetryWithLinearBackoffResult：线性退避，返回 Result[T]
r := async.RetryWithLinearBackoffResult(ctx, func(ctx context.Context) (string, error) {
    return callService(ctx)
}, 5, 1*time.Second)

if !r.Ok() {
    log.Printf("服务调用失败: %v", r.Err)
}
```

> `Result[T]` 提供 `Ok()`、`IsPanic()` 等方法链式处理结果，适合在中间件或管道中使用。

---

## 简单函数式重试

将普通函数（无 context 参数）包装为 `RetryFn` 后链式调用：

```go
// 简单重试：最多 3 次
err := async.RetryFn(func() error {
    return doSomething()
}).WithRetry(3)

// 带 panic 保护
err := async.RetryFn(func() error {
    return riskyOperation()
}).WithRetry(2)
```

---

## 超时与截止时间包装

### WithTimeout

给单个函数调用加上超时：

```go
val, err := async.WithTimeout(ctx, 3*time.Second, func(ctx context.Context) (string, error) {
    return httpGet(ctx, url)
})

// 无返回值版本
err := async.WithTimeoutVoid(ctx, 5*time.Second, func(ctx context.Context) error {
    return kafkaProducer.Send(ctx, msg)
})
```

### WithDeadline

给单个函数调用加上截止时间：

```go
deadline := time.Now().Add(5 * time.Second)
val, err := async.WithDeadline(ctx, deadline, func(ctx context.Context) (*Report, error) {
    return generateReport(ctx)
})

// 无返回值版本
err := async.WithDeadlineVoid(ctx, deadline, func(ctx context.Context) error {
    return uploadFile(ctx, file)
})
```

---

## Worker 绑定重试

向协程池提交任务时，遇到 `ErrSubmitTimeout`（池满）自动退避重试：

```go
// pool 实现了 WorkerPoolBackend 接口
err := async.BindRetryToWorker(ctx, pool, func(ctx context.Context) error {
    return processItem(ctx, item)
}, 3, 10*time.Millisecond, 1*time.Second)
```

> 适用于高负载场景下提交任务时池满需要重试的情况。

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

    // 示例1：调用外部 API，指数退避重试
    result, err := callWithRetry(ctx, "https://api.example.com/data")
    if err != nil {
        log.Printf("API 调用最终失败: %v", err)
        return
    }
    fmt.Printf("结果: %s\n", result)

    // 示例2：数据库操作，线性退避重试
    err = async.RetryWithLinearBackoff(ctx, 5, 1*time.Second, func(ctx context.Context) error {
        return db.Exec(ctx, "UPDATE users SET status = ?", "active")
    })
    if err != nil {
        log.Printf("数据库更新失败: %v", err)
    }

    // 示例3：简单函数式重试
    err = async.RetryFn(func() error {
        return connectToCache()
    }).WithRetry(3)
}

func callWithRetry(ctx context.Context, url string) (string, error) {
    return async.RetryWithConfig(ctx,
        func(ctx context.Context) (string, error) {
            return httpGet(ctx, url)
        },
        3,                      // 最多重试 3 次
        100*time.Millisecond,   // 初始退避 100ms
        5*time.Second,          // 最大退避 5s
        async.TimeoutOpt{PerCallTimeout: 3 * time.Second},
    )
}
```

---

## 方法速查表

### 重试函数

| 函数 | 退避策略 | 说明 |
|------|---------|------|
| `Retry(ctx, maxRetries, fn)` | 无 | 立即重试 |
| `RetryWithBackoff(ctx, max, backoff, fn)` | 指数 | 指数退避重试（无返回值） |
| `RetryWithLinearBackoff(ctx, max, backoff, fn)` | 线性 | 等间隔重试（无返回值） |
| `RetryWithConfig[T](ctx, fn, max, init, max, opts)` | 指数 | 带每次调用超时（有返回值） |
| `RetryWithConfigVoid(ctx, fn, max, init, max, opts)` | 指数 | 带每次调用超时（无返回值） |
| `RetryWithBackoffResult[T](ctx, fn, max, init, max)` | 指数 | 指数退避，返回 `Result[T]` |
| `RetryWithLinearBackoffResult[T](ctx, fn, max, backoff)` | 线性 | 等间隔重试，返回 `Result[T]` |

### 超时/截止时间包装

| 函数 | 说明 |
|------|------|
| `WithTimeout[T](ctx, d, fn)` | 带超时包装（有返回值） |
| `WithTimeoutVoid(ctx, d, fn)` | 带超时包装（无返回值） |
| `WithDeadline[T](ctx, dl, fn)` | 带截止时间包装（有返回值） |
| `WithDeadlineVoid(ctx, dl, fn)` | 带截止时间包装（无返回值） |

### 其他

| 函数/类型 | 说明 |
|-----------|------|
| `RetryFn(fn).WithRetry(n)` | 简单函数式重试 |
| `BindRetryToWorker(ctx, pool, fn, max, init, max)` | Worker 提交重试 |
| `TimeoutOpt{PerCallTimeout: d}` | 每次调用超时配置 |
| `WorkerPoolBackend` | Worker 池接口 |