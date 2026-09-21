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

**`Retry` 参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文（取消即停止重试） |
| `maxRetries` | `int` | 最大重试次数（不含首次调用） |
| `fn` | `func(context.Context) error` | 要重试的函数 |

### RetryWithBackoff（默认配置）

```go
// 最多重试 3 次，初始退避 100ms（最大退避默认 30s）
// 退避序列: 100ms → 200ms → 400ms → ... → 上限 30s
err := async.RetryWithBackoff(ctx, 3, 100*time.Millisecond, func(ctx context.Context) error {
    return callExternalAPI(ctx, request)
})

// 重试 5 次，初始退避 1s
err := async.RetryWithBackoff(ctx, 5, 1*time.Second, func(ctx context.Context) error {
    return callSlowAPI(ctx, request)
})
```

**`RetryWithBackoff` 参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `maxRetries` | `int` | 最大重试次数 |
| `backoff` | `time.Duration` | 初始退避时间（每次翻倍） |
| `fn` | `func(context.Context) error` | 要重试的函数 |

> 默认最大退避上限：**30 秒**（内部固定，不需要传参）。退避公式：`min(backoff × 2^attempt, 30s)`

### RetryWithConfig（完整自定义配置）

```go
err := async.RetryWithConfig(ctx, &async.RetryConfig{
    MaxRetries:   3,
    InitialDelay: 200 * time.Millisecond, // 初始退避
    MaxDelay:     5 * time.Second,        // 最大退避上限
    Factor:       2.5,                     // 乘数因子
}, func(ctx context.Context) error {
    return callAPI(ctx)
})
```

**`RetryConfig` 结构：**
| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `MaxRetries` | `int` | 0 (无默认) | 最大重试次数 |
| `InitialDelay` | `time.Duration` | 100ms | 初始退避时间 |
| `MaxDelay` | `time.Duration` | 0（无上限） | 退避最大上限 |
| `Factor` | `float64` | 2.0 | 指数乘数因子 |

---

## 线性退避重试

每次重试等待相同的退避时间：

```go
// 每次失败后等 1 秒，最多重试 5 次
err := async.RetryWithLinearBackoff(ctx, 5, 1*time.Second, func(ctx context.Context) error {
    return callSlowService(ctx, data)
})
```

**`RetryWithLinearBackoff` 参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `maxRetries` | `int` | 最大重试次数 |
| `backoff` | `time.Duration` | 每次重试固定等待时间 |
| `fn` | `func(context.Context) error` | 要重试的函数 |

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

**`RetryWithConfig` 参数：**
- `ctx` — 上下文
- `fn` — `func(context.Context) (T, error)`
- `retries` — 最大重试次数
- `initialDelay` — 初始退避
- `maxDelay` — 最大退避上限
- `opts` — `TimeoutOpt{PerCallTimeout}` 超时配置

**重要**：`RetryWithConfig` 会区分 `context.DeadlineExceeded` 和 `context.Canceled` 错误，这两种错误**不会触发重试**。

---

## Result[T] 返回值便捷重试

当不需要区分 `context.DeadlineExceeded` / `context.Canceled` 时，使用返回 `Result[T]` 的便捷方法更简洁：

```go
// RetryWithBackoffResult：指数退避，返回 Result[T]
r := async.RetryWithBackoffResult(ctx, func(ctx context.Context) (*Data, error) {
    return fetchData(ctx, id)
}, 3, 100*time.Millisecond, 5*time.Second)

// RetryWithLinearBackoffResult：线性退避，返回 Result[T]
r := async.RetryWithLinearBackoffResult(ctx, func(ctx context.Context) (string, error) {
    return callService(ctx)
}, 5, 1*time.Second)
```

**便捷函数参数（均与对应非 Result 版本一致）：**
- `RetryWithBackoffResult(ctx, fn, retries, maxBackoff)` → `Result[T]`
- `RetryWithLinearBackoffResult(ctx, fn, retries, interval)` → `Result[T]`

### retry 子包 Void 便捷函数

当只关心 error 不需要返回值时，`retry` 子包提供无返回值的便捷版本：

```go
// retry.RetryWithBackoffVoid — 指数退避，只返回 error
err := retry.RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
    return sendRequest(ctx, payload)
}, 3, 100*time.Millisecond, 30*time.Second)

// retry.RetryWithLinearBackoffVoid — 线性退避，只返回 error
err := retry.RetryWithLinearBackoffVoid(ctx, func(ctx context.Context) error {
    return pollStatus(ctx, jobID)
}, 5, 2*time.Second)
```

| 函数 | 签名 | 说明 |
|------|------|------|
| `retry.RetryWithBackoffVoid` | `(ctx, fn, maxRetries, initialBackoff, maxBackoff) error` | 指数退避，fn 签名 `func(ctx) error` |
| `retry.RetryWithLinearBackoffVoid` | `(ctx, fn, maxRetries, backoff) error` | 线性退避，fn 签名 `func(ctx) error` |

**`RetryWithBackoffVoid` 参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) error` | 要重试的函数 |
| `maxRetries` | `int` | 最大重试次数 |
| `initialBackoff` | `time.Duration` | 初始退避时间 |
| `maxBackoff` | `time.Duration` | 最大退避时间上限 |

**`RetryWithLinearBackoffVoid` 参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) error` | 要重试的函数 |
| `maxRetries` | `int` | 最大重试次数 |
| `backoff` | `time.Duration` | 每次重试的固定等待时间 |

> **说明：** 这两个函数在 `retry` 子包中导出，同时被顶层的 `RetryWithBackoff` / `RetryWithLinearBackoff` 内部调用。用户如果已经在使用 `import "..." retry` 可以直接使用 Void 版本。


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

**`RetryFn` 用法：**
- `async.RetryFn(func() error)` — 包装函数，返回 `RetryFnWrapper`
- `.WithRetry(n)` — 设置最大重试次数并执行

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

**`WithTimeout` 参数：**
- `ctx` — 上下文
- `timeout` — 超时时间
- `fn` — `func(context.Context) (T, error)`

**`WithTimeoutVoid` 参数：** 同上，但 fn 签名为 `func(context.Context) error`

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

**`WithDeadline` 参数：**
- `ctx` — 上下文
- `deadline` — 截止时间点 `time.Time`
- `fn` — `func(context.Context) (T, error)`

---

## Worker 绑定重试

向协程池提交任务时，遇到 `ErrSubmitTimeout`（池满）自动退避重试：

```go
// pool 实现了 WorkerPoolBackend 接口
err := async.BindRetryToWorker(ctx, pool, func(ctx context.Context) error {
    return processItem(ctx, item)
}, 3, 10*time.Millisecond, 1*time.Second)
```

**`BindRetryToWorker` 参数：**
- `ctx` — 上下文
- `backend` — 实现了 `WorkerPoolBackend` 接口（`Submit(ctx, fn) error`）的池
- `fn` — `func(context.Context) error`
- `maxRetries` — 最大重试次数
- `initialBackoff` — 初始退避时间
- `maxBackoff` — 最大退避上限

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

### 重试函数（顶层 async 包）

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `RetryWithBackoff[T]` | `func RetryWithBackoff[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration) (T, error)` | 指数退避重试（有返回值） |
| `RetryWithBackoffVoid` | `func RetryWithBackoffVoid(ctx context.Context, fn func(context.Context) error, maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration) error` | 指数退避重试（无返回值） |
| `RetryWithBackoffResult[T]` | `func RetryWithBackoffResult[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration) core.Result[T]` | 指数退避，返回 `Result[T]`（不返回 error，所有错误都在 Result 中） |
| `RetryWithLinearBackoff[T]` | `func RetryWithLinearBackoff[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, backoff time.Duration) (T, error)` | 线性退避重试（有返回值） |
| `RetryWithLinearBackoffVoid` | `func RetryWithLinearBackoffVoid(ctx context.Context, fn func(context.Context) error, maxRetries int, backoff time.Duration) error` | 线性退避重试（无返回值） |
| `RetryWithLinearBackoffResult[T]` | `func RetryWithLinearBackoffResult[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, backoff time.Duration) core.Result[T]` | 线性退避，返回 `Result[T]` |
| `RetryWithConfig[T]` | `func RetryWithConfig[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration, opts ...TimeoutOpt) (T, error)` | 带每次调用超时配置的指数退避（有返回值） |
| `RetryWithConfigVoid` | `func RetryWithConfigVoid(ctx context.Context, fn func(context.Context) error, maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration, opts ...TimeoutOpt) error` | 带每次调用超时配置的指数退避（无返回值） |

### 重试函数（retry 子包）

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `retry.RetryWithBackoff[T]` | `func RetryWithBackoff[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration) (T, error)` | 指数退避（有返回值） |
| `retry.RetryWithBackoffVoid` | `func RetryWithBackoffVoid(ctx context.Context, fn func(context.Context) error, maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration) error` | 指数退避（无返回值） |
| `retry.RetryWithBackoffResult[T]` | `func RetryWithBackoffResult[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration) core.Result[T]` | 指数退避返回 Result |
| `retry.RetryWithLinearBackoff[T]` | `func RetryWithLinearBackoff[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, backoff time.Duration) (T, error)` | 线性退避（有返回值） |
| `retry.RetryWithLinearBackoffVoid` | `func RetryWithLinearBackoffVoid(ctx context.Context, fn func(context.Context) error, maxRetries int, backoff time.Duration) error` | 线性退避（无返回值） |
| `retry.RetryWithLinearBackoffResult[T]` | `func RetryWithLinearBackoffResult[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, backoff time.Duration) core.Result[T]` | 线性退避返回 Result |
| `retry.RetryWithConfig[T]` | `func RetryWithConfig[T any](ctx context.Context, fn func(context.Context) (T, error), maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration, opts ...TimeoutOpt) (T, error)` | 带每次超时配置 |
| `retry.RetryWithConfigVoid` | `func RetryWithConfigVoid(ctx context.Context, fn func(context.Context) error, maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration, opts ...TimeoutOpt) error` | 带每次超时配置（无返回值） |

### 超时/截止时间包装（顶层 async 包）

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `WithTimeout[T]` | `func WithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (T, error)) (T, error)` | 带超时包装（有返回值） |
| `WithTimeoutVoid` | `func WithTimeoutVoid(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error` | 带超时包装（无返回值） |
| `WithDeadline[T]` | `func WithDeadline[T any](ctx context.Context, deadline time.Time, fn func(ctx context.Context) (T, error)) (T, error)` | 带截止时间包装（有返回值） |
| `WithDeadlineVoid` | `func WithDeadlineVoid(ctx context.Context, deadline time.Time, fn func(ctx context.Context) error) error` | 带截止时间包装（无返回值） |

### 超时/截止时间包装（retry 子包）

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `retry.WithTimeout[T]` | `func WithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (T, error)) (T, error)` | 带超时包装 |
| `retry.WithTimeoutVoid` | `func WithTimeoutVoid(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error` | 无返回值带超时包装 |
| `retry.WithDeadline[T]` | `func WithDeadline[T any](ctx context.Context, deadline time.Time, fn func(ctx context.Context) (T, error)) (T, error)` | 带截止时间包装 |
| `retry.WithDeadlineVoid` | `func WithDeadlineVoid(ctx context.Context, deadline time.Time, fn func(ctx context.Context) error) error` | 无返回值带截止时间包装 |

### 函数式重试与 Worker 绑定

| 函数/类型 | 完整签名 | 说明 |
|-----------|---------|------|
| `RetryFn` | `type RetryFn func() error` | 函数式重试封装 |
| `(RetryFn).WithRetry` | `func (rf RetryFn) WithRetry(n int) error` | 简单无退避重试 n 次 |
| `BindRetryToWorker` | `func BindRetryToWorker(ctx context.Context, pool WorkerPoolBackend, fn RetryFn, maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration)` | 绑定重试到 Worker 池执行 |

### 配置类型

| 类型 | 完整定义 | 说明 |
|------|---------|------|
| `TimeoutOpt` | `type TimeoutOpt struct{ PerCallTimeout time.Duration }` | 每次调用超时配置（`RetryWithConfig` 的可变参数） |
| `WorkerPoolBackend` | `interface{ Submit(ctx context.Context, fn func(context.Context) error) error }` | Worker 池抽象接口（Pool 和 Group 均实现） |