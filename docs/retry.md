# Retry（重试）文档

## 概述

Retry 模块提供灵活的重试机制，支持多种退避策略、超时控制和错误过滤。

**核心特性**:
- 指数退避（Exponential Backoff）
- 线性退避（Linear Backoff）
- 每次调用超时控制
- 截止时间（Deadline）控制
- 错误过滤
- Panic 自动恢复
- Worker 池提交重试

---

## 目录

- [函数速查表](#函数速查表)
- [简便重试（无退避）](#简便重试无退避)
  - [Retry / RetryBackoff / RetryLinear](#retry)
- [指数退避重试](#指数退避重试)
  - [RetryWithBackoff / RetryWithBackoffVoid / RetryWithBackoffResult](#retrywithbackoff)
- [线性退避重试](#线性退避重试)
  - [RetryWithLinearBackoff / RetryWithLinearBackoffVoid / RetryWithLinearBackoffResult](#retrywithlinearbackoff)
- [每次调用超时重试](#每次调用超时重试)
  - [RetryWithConfig / RetryWithConfigVoid / TimeoutOpt](#retrywithconfig)
- [超时/截止时间包装器](#超时截止时间包装器)
  - [WithTimeout / WithTimeoutVoid / WithDeadline / WithDeadlineVoid](#withtimeout)
- [RetryFn（函数式重试）](#retryfn函数式重试)
  - [RetryFn / WithRetry](#retryfn)
- [BindRetryToWorker（Worker 池提交重试）](#bindretrytoworker)
  - [BindRetryToWorker / WorkerPoolBackend 接口](#bindretrytoworker)
- [退避算法](#退避算法)
- [上下文取消传播 / Panic 自动恢复](#context-取消传播)
- [最佳实践](#最佳实践)
- [高级示例](#高级示例)
- [性能基准](#性能基准)

## 函数速查表

| 函数 | 重试次数 | 退避方式 | 每次调用超时 | 返回值 |
|------|----------|----------|-------------|--------|
| `Retry` | maxRetries | 无 | ❌ | `error` |
| `RetryWithBackoff` | maxRetries | 指数 | ❌ | `error` |
| `RetryWithLinearBackoff` | maxRetries | 线性 | ❌ | `error` |
| `RetryWithBackoff[T]` | maxRetries | 指数 | ❌ | `(T, error)` |
| `RetryWithBackoffVoid` | maxRetries | 指数 | ❌ | `error` |
| `RetryWithBackoffResult[T]` | maxRetries | 指数 | ❌ | `Result[T]` |
| `RetryWithLinearBackoff[T]` | maxRetries | 线性 | ❌ | `(T, error)` |
| `RetryWithLinearBackoffVoid` | maxRetries | 线性 | ❌ | `error` |
| `RetryWithLinearBackoffResult[T]` | maxRetries | 线性 | ❌ | `Result[T]` |
| `RetryWithConfig[T]` | maxRetries | 指数 | ✅ | `(T, error)` |
| `RetryWithConfigVoid` | maxRetries | 指数 | ✅ | `error` |
| `WithTimeout[T]` | - | - | ✅ | `(T, error)` |
| `WithTimeoutVoid` | - | - | ✅ | `error` |
| `WithDeadline[T]` | - | - | ✅ | `(T, error)` |
| `WithDeadlineVoid` | - | - | ✅ | `error` |
| `RetryFn.WithRetry` | maxRetries | 无 | ❌ | `error` |
| `BindRetryToWorker` | maxRetries | 指数 | ❌ | `error` |

---

## 简便重试（无退避）

以下为 `async` 包根级别的简便重试函数，签名更简洁，适合快速使用。

### Retry

简单重试，每次重试无等待间隔。适合瞬时故障场景。

```go
// 语法
func Retry(ctx context.Context, maxRetries int, fn func(ctx context.Context) error) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `maxRetries` | `int` | 最大重试次数（共 maxRetries+1 次尝试） |
| `fn` | `func(context.Context) error` | 要重试的函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 所有重试失败后的最终错误 |

```go
err := async.Retry(ctx, 3, func(ctx context.Context) error {
    return redis.Ping(ctx)
})
// 共 4 次尝试（1 次原始 + 3 次重试），无等待间隔
```

> **注意**：无退避间隔，高频重试可能加重后端压力。生产环境推荐使用 `RetryWithBackoff`/`RetryBackoff`。

### RetryBackoff

指数退避重试的 Void 版本，只返回 error。调用 `retry.RetryWithBackoffVoid`。

```go
// 语法
func RetryBackoff(ctx context.Context, fn func(context.Context) error, maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) error` | 要重试的函数 |
| `maxRetries` | `int` | 最大重试次数 |
| `initialBackoff` | `time.Duration` | 初始退避时间 |
| `maxBackoff` | `time.Duration` | 最大退避上限 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 所有重试失败后的最终错误 |

```go
err := async.RetryBackoff(ctx, func(ctx context.Context) error {
    return sendMessage(ctx, msg)
}, 3, 10*time.Millisecond, 1*time.Second)
// 退避序列: 10ms → 20ms → 40ms（上限 1s）
```

### RetryLinear

线性退避重试的 Void 版本，每次等待固定间隔。调用 `retry.RetryWithLinearBackoffVoid`。

```go
// 语法
func RetryLinear(ctx context.Context, fn func(context.Context) error, maxRetries int, backoff time.Duration) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) error` | 要重试的函数 |
| `maxRetries` | `int` | 最大重试次数 |
| `backoff` | `time.Duration` | 每次重试的固定等待时间 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 所有重试失败后的最终错误 |

```go
err := async.RetryLinear(ctx, func(ctx context.Context) error {
    return writeDB(ctx, record)
}, 5, 100*time.Millisecond)
// 每次失败等 100ms，最多重试 5 次
```

---

## 指数退避重试

### RetryWithBackoff

```go
// 语法
func RetryWithBackoff[T any](
    ctx context.Context,
    fn func(ctx context.Context) (T, error),
    maxRetries int,
    initialBackoff time.Duration,
    maxBackoff time.Duration,
) (T, error)
```

使用指数退避策略执行 fn，最多执行 `maxRetries+1` 次（首次 + maxRetries 次重试）。

退避时间计算公式：`backoff = min(initialBackoff × 2^attempt, maxBackoff)`

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，取消后终止重试 |
| `fn` | `func(context.Context) (T, error)` | 要执行的函数，每次尝试会派生新的 context |
| `maxRetries` | `int` | 最大重试次数（**不含首次尝试**，总执行次数 = maxRetries + 1） |
| `initialBackoff` | `time.Duration` | 初始退避时间，设为 0 则不等待直接重试 |
| `maxBackoff` | `time.Duration` | 最大退避时间上限，防止退避无限增长 |
| 返回 | `(T, error)` | 成功时返回值和 nil；重试耗尽时返回零值和 "retry exhausted" 错误 |

**重试耗尽时会返回包裹原始错误的 error**。

```go
// 调用 RPC，最多重试3次（共4次尝试），退避从100ms开始指数增长到最多5s
result, err := retry.RetryWithBackoff(ctx, func(ctx context.Context) (*Response, error) {
    return rpcClient.Call(ctx, request)
}, 3, 100*time.Millisecond, 5*time.Second)

if err != nil {
    log.Printf("RPC 调用失败: %v", err)
}
```

---

### RetryWithBackoffVoid

```go
// 语法
func RetryWithBackoffVoid(
    ctx context.Context,
    fn func(ctx context.Context) error,
    maxRetries int,
    initialBackoff time.Duration,
    maxBackoff time.Duration,
) error
```

与 `RetryWithBackoff` 相同，但 fn 只返回 error，适合发送消息、写日志等无返回值的操作。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，取消后终止重试 |
| `fn` | `func(context.Context) error` | 要执行的函数，只返回 error |
| `maxRetries` | `int` | 最大重试次数（不含首次尝试） |
| `initialBackoff` | `time.Duration` | 初始退避时间 |
| `maxBackoff` | `time.Duration` | 最大退避时间上限 |
| 返回 | `error` | nil 表示成功，否则为重试耗尽的包裹错误 |

```go
// 重试发送消息
err := retry.RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
    return kafkaProducer.Send(ctx, msg)
}, 3, 100*time.Millisecond, 5*time.Second)
```

---

### RetryWithBackoffResult

```go
// 语法
func RetryWithBackoffResult[T any](
    ctx context.Context,
    fn func(ctx context.Context) (T, error),
    maxRetries int,
    initialBackoff time.Duration,
    maxBackoff time.Duration,
) core.Result[T]
```

与 `RetryWithBackoff` 相同，但返回 `Result[T]` 而非两个返回值，便于链式处理。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 要执行的函数 |
| `maxRetries` | `int` | 最大重试次数 |
| `initialBackoff` | `time.Duration` | 初始退避时间 |
| `maxBackoff` | `time.Duration` | 最大退避时间上限 |
| 返回 | `core.Result[T]` | 结果容器，通过 `Ok()` 判断成功，`Value` / `Err` 获取值/错误 |

```go
r := retry.RetryWithBackoffResult(ctx, fn, 3, 100*time.Millisecond, 5*time.Second)
if !r.Ok() {
    log.Printf("重试失败: %v", r.Err)
    return
}
process(r.Value)
```

---

## 线性退避重试

### RetryWithLinearBackoff

```go
// 语法
func RetryWithLinearBackoff[T any](
    ctx context.Context,
    fn func(ctx context.Context) (T, error),
    maxRetries int,
    backoff time.Duration,
) (T, error)
```

使用固定退避时间执行 fn，每次重试等待相同的 `backoff` 时间。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，取消后终止重试 |
| `fn` | `func(context.Context) (T, error)` | 要执行的函数 |
| `maxRetries` | `int` | 最大重试次数（不含首次尝试） |
| `backoff` | `time.Duration` | 固定退避时间 |
| 返回 | `(T, error)` | 成功时返回值和 nil；重试耗尽时返回错误 |

```go
// 每次重试等1秒，最多重试5次
val, err := retry.RetryWithLinearBackoff(ctx, fn, 5, 1*time.Second)
```

---

### RetryWithLinearBackoffVoid

```go
// 语法
func RetryWithLinearBackoffVoid(
    ctx context.Context,
    fn func(ctx context.Context) error,
    maxRetries int,
    backoff time.Duration,
) error
```

与 `RetryWithLinearBackoff` 相同，但 fn 只返回 error。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) error` | 要执行的函数，只返回 error |
| `maxRetries` | `int` | 最大重试次数 |
| `backoff` | `time.Duration` | 固定退避时间 |
| 返回 | `error` | nil 表示成功 |

```go
// 每次重试等500ms，最多重试3次
err := retry.RetryWithLinearBackoffVoid(ctx, func(ctx context.Context) error {
    return sendEmail(ctx, to, body)
}, 3, 500*time.Millisecond)
```

---

### RetryWithLinearBackoffResult

```go
// 语法
func RetryWithLinearBackoffResult[T any](
    ctx context.Context,
    fn func(ctx context.Context) (T, error),
    maxRetries int,
    backoff time.Duration,
) core.Result[T]
```

与 `RetryWithLinearBackoff` 相同，但返回 `Result[T]`。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) (T, error)` | 要执行的函数 |
| `maxRetries` | `int` | 最大重试次数 |
| `backoff` | `time.Duration` | 固定退避时间 |
| 返回 | `core.Result[T]` | 结果容器 |

```go
r := retry.RetryWithLinearBackoffResult(ctx, fn, 5, 1*time.Second)
if r.Ok() {
    process(r.Value)
}
```

---

## 每次调用超时重试

### RetryWithConfig

```go
// 语法
func RetryWithConfig[T any](
    ctx context.Context,
    fn func(ctx context.Context) (T, error),
    maxRetries int,
    initialBackoff time.Duration,
    maxBackoff time.Duration,
    opts ...TimeoutOpt,
) (T, error)
```

指数退避重试，**额外支持每次 fn 调用的超时控制**。

与 `RetryWithBackoff` 的区别：
- 每次调用 fn 都会包裹在一个 `context.WithTimeout` 中
- 会区分 `DeadlineExceeded` 和 `Canceled` 错误，这两种错误不重试直接返回

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，取消后终止重试 |
| `fn` | `func(context.Context) (T, error)` | 要执行的函数 |
| `maxRetries` | `int` | 最大重试次数（不含首次） |
| `initialBackoff` | `time.Duration` | 初始退避时间 |
| `maxBackoff` | `time.Duration` | 最大退避时间上限 |
| `opts` | `...TimeoutOpt` | 可选配置 `TimeoutOpt{PerCallTimeout}` |
| 返回 | `(T, error)` | 成功时返回值；重试耗尽或超时取消时返回错误 |

```go
// 每次调用最多2秒，最多重试3次，退避100ms到5s
result, err := retry.RetryWithConfig(ctx, func(ctx context.Context) (*Data, error) {
    return fetchData(ctx, id)
}, 3, 100*time.Millisecond, 5*time.Second,
    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})

if err != nil {
    if errors.Is(err, context.DeadlineExceeded) {
        log.Println("调用超时")
    }
}
```

---

### RetryWithConfigVoid

```go
// 语法
func RetryWithConfigVoid(
    ctx context.Context,
    fn func(ctx context.Context) error,
    maxRetries int,
    initialBackoff time.Duration,
    maxBackoff time.Duration,
    opts ...TimeoutOpt,
) error
```

与 `RetryWithConfig` 相同，但 fn 只返回 error。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `fn` | `func(context.Context) error` | 要执行的函数，只返回 error |
| `maxRetries` | `int` | 最大重试次数 |
| `initialBackoff` | `time.Duration` | 初始退避时间 |
| `maxBackoff` | `time.Duration` | 最大退避时间上限 |
| `opts` | `...TimeoutOpt` | 可选 `TimeoutOpt{PerCallTimeout}` |
| 返回 | `error` | nil 或包裹的错误 |

```go
err := retry.RetryWithConfigVoid(ctx, func(ctx context.Context) error {
    return callExternalAPI(ctx, req)
}, 3, 100*time.Millisecond, 5*time.Second,
    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})
```

---

### TimeoutOpt

```go
type TimeoutOpt struct {
    PerCallTimeout time.Duration
}
```

用于 `RetryWithConfig` / `RetryWithConfigVoid` 的每次调用超时配置。

| 字段 | 类型 | 说明 |
|------|------|------|
| `PerCallTimeout` | `time.Duration` | 每次调用（含重试中的每次尝试）的超时时间 |

---

## 超时/截止时间包装器

### WithTimeout

```go
// 语法
func WithTimeout[T any](
    ctx context.Context,
    timeout time.Duration,
    fn func(ctx context.Context) (T, error),
) (T, error)
```

包装 fn，使其在指定超时后自动取消。不涉及重试逻辑。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 父上下文 |
| `timeout` | `time.Duration` | 超时时间 |
| `fn` | `func(context.Context) (T, error)` | 要执行的函数 |
| 返回 | `(T, error)` | 成功时返回值；超时时 `context.DeadlineExceeded` |

```go
// 单个调用最多3秒
val, err := retry.WithTimeout(ctx, 3*time.Second, func(ctx context.Context) (string, error) {
    return httpGet(ctx, url)
})
```

---

### WithTimeoutVoid

```go
// 语法
func WithTimeoutVoid(
    ctx context.Context,
    timeout time.Duration,
    fn func(ctx context.Context) error,
) error
```

与 `WithTimeout` 相同，但 fn 只返回 error。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 父上下文 |
| `timeout` | `time.Duration` | 超时时间 |
| `fn` | `func(context.Context) error` | 要执行的函数，只返回 error |
| 返回 | `error` | nil 表示成功；超时或执行出错时返回 error |

```go
err := retry.WithTimeoutVoid(ctx, 5*time.Second, func(ctx context.Context) error {
    return kafkaProducer.Send(ctx, msg)
})
```

---

### WithDeadline

```go
// 语法
func WithDeadline[T any](
    ctx context.Context,
    deadline time.Time,
    fn func(ctx context.Context) (T, error),
) (T, error)
```

包装 fn，使其在指定截止时间后自动取消。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 父上下文 |
| `deadline` | `time.Time` | 截止时间 |
| `fn` | `func(context.Context) (T, error)` | 要执行的函数 |
| 返回 | `(T, error)` | 成功时返回值；超时时 `context.DeadlineExceeded` |

```go
deadline := time.Now().Add(5 * time.Second)
val, err := retry.WithDeadline(ctx, deadline, fn)
```

---

### WithDeadlineVoid

```go
// 语法
func WithDeadlineVoid(
    ctx context.Context,
    deadline time.Time,
    fn func(ctx context.Context) error,
) error
```

与 `WithDeadline` 相同，但 fn 只返回 error。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 父上下文 |
| `deadline` | `time.Time` | 截止时间 |
| `fn` | `func(context.Context) error` | 要执行的函数，只返回 error |
| 返回 | `error` | nil 表示成功 |

```go
err := retry.WithDeadlineVoid(ctx, time.Now().Add(30*time.Second), func(ctx context.Context) error {
    return batchProcess(ctx, items)
})
```

---

## RetryFn（函数式重试）

### RetryFn

```go
type RetryFn func() error
```

函数式重试辅助类型，支持方法链式调用。将普通函数转换为 RetryFn 后直接调用 `WithRetry`。

特点：
- 无需 context 参数（适合简单无上下文函数）
- 自动捕获 panic 并转为 error
- 无退避等待，立即重试

---

### WithRetry

```go
// 语法
func (r RetryFn) WithRetry(maxRetries int) error
```

执行 fn，最多执行 `maxRetries+1` 次，自动捕获 panic。

| 参数 | 类型 | 说明 |
|------|------|------|
| `maxRetries` | `int` | 最大重试次数（不含首次） |
| 返回 | `error` | nil 表示成功；重试耗尽时返回 "retry exhausted" 包裹错误 |

```go
// 简单重试：最多3次
err := retry.RetryFn(func() error {
    return doSomething()
}).WithRetry(3)

// 带 panic 保护的重试
err := retry.RetryFn(func() error {
    return riskyOperation()
}).WithRetry(2)
```

---

## BindRetryToWorker

### BindRetryToWorker

```go
// 语法
func BindRetryToWorker(
    ctx context.Context,
    backend WorkerPoolBackend,
    fn func(ctx context.Context) error,
    maxRetries int,
    initialBackoff time.Duration,
    maxBackoff time.Duration,
) error
```

向 worker 池提交任务，遇到 `ErrSubmitTimeout` 时自动退避重试。适用高负载场景下提交任务时池满的处理。

**只重试提交超时错误**，其他错误（如 `ErrPoolClosed`）立即返回不重试。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，取消后终止重试 |
| `backend` | `WorkerPoolBackend` | 实现了 `Submit(ctx, fn) error` 接口的后端（Pool 实现此接口） |
| `fn` | `func(context.Context) error` | 要提交执行的任务函数 |
| `maxRetries` | `int` | 最大重试次数（不含首次提交） |
| `initialBackoff` | `time.Duration` | 初始退避时间 |
| `maxBackoff` | `time.Duration` | 最大退避时间上限 |
| 返回 | `error` | nil 表示成功提交；其他 pool 错误或重试耗尽时返回 error |

```go
// 向协程池提交任务，提交超时时自动退避重试
err := retry.BindRetryToWorker(ctx, pool, func(ctx context.Context) error {
    return processItem(ctx, item)
}, 3, 10*time.Millisecond, 1*time.Second)
```

---

### WorkerPoolBackend 接口

```go
type WorkerPoolBackend interface {
    Submit(ctx context.Context, fn func(ctx context.Context) error) error
}
```

Pool 自动实现此接口，可以直接作为 `BindRetryToWorker` 的 backend 参数。

---

## 退避算法

退避时间计算：

```go
backoff = min(initialBackoff × 2^attempt, maxBackoff)
```

| attempt | initialBackoff=100ms | maxBackoff=5s |
|---------|---------------------|---------------|
| 0 | 100ms | |
| 1 | 200ms | |
| 2 | 400ms | |
| 3 | 800ms | |
| 4 | 1.6s | |
| 5 | 3.2s | |
| 6 | 5s（触及上限） | |
| 7+ | 5s（保持上限） | |

---

## Context 取消传播

所有重试函数都接受 `context.Context`，当 ctx 被取消时：
- 正在执行的任务不会立即中断（取决于 fn 内部是否检查 ctx.Done()）
- 重试循环终止，返回 `ctx.Err()`
- 已经执行成功的结果不会丢失

```go
ctx, cancel := context.WithCancel(parentCtx)
defer cancel()

// 5秒后取消重试
go func() {
    time.Sleep(5 * time.Second)
    cancel()
}()

err := retry.RetryWithBackoffVoid(ctx, fn, 100, 100*time.Millisecond, 1*time.Second)
// 5秒后返回 context.Canceled
```

---

## Panic 自动恢复

所有重试函数都通过 `invokeSafely` 包装 fn 调用，自动将 panic 转为 `PanicError`：

```go
err := retry.RetryWithCount(ctx, 3, func(ctx context.Context) error {
    var db *Database
    db.Query(ctx, "SELECT 1") // panic: nil pointer dereference
    return nil
})
// err 类型为 *PanicError，包含完整调用栈
```

---

## 最佳实践

### 1. 总是设置最大重试次数

```go
retry.RetryWithBackoff(ctx, fn, 3, ...)    // ✅ 好：最多3次重试
retry.Retry(ctx, fn)                        // ⚠️ 慎用：无限重试，可能永远阻塞
```

### 2. 使用 RetryWithConfig 控制每次调用超时

```go
// 防止单次调用无限等待
retry.RetryWithConfig(ctx, fn, 3, 100*time.Millisecond, 5*time.Second,
    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})
```

### 3. 区分可重试和不可重试的错误

```go
cfg := async.RetryConfig{
    MaxRetries: 3,
    ShouldRetry: func(err error) bool {
        return errors.Is(err, ErrNetwork) ||
               errors.Is(err, ErrTimeout) ||
               errors.Is(err, context.DeadlineExceeded)
    },
}
```

### 4. 使用 Jitter 避免惊群效应

当多个客户端同时重试时，加入随机抖动避免同时打到服务端：

```go
cfg := async.RetryConfig{
    MaxRetries:   5,
    InitialDelay: 100 * time.Millisecond,
    Jitter:       0.3, // 实际延迟 = delay × [0.7, 1.3]
}
```

### 5. 记录重试日志

```go
OnRetry: func(attempt int, err error, delay time.Duration) {
    async.LogWarn("重试中", async.Int("attempt", attempt), async.Err(err))
}
```

---

## 高级示例

### 重试 + 超时 + 退避组合

```go
// 完整的生产级重试策略
result, err := retry.RetryWithConfig(ctx, func(ctx context.Context) (*Response, error) {
    return httpClient.Do(ctx, req)
}, 5,                          // 最多重试5次
    100*time.Millisecond,       // 初始退避100ms
    10*time.Second,             // 最大退避10s
    retry.TimeoutOpt{PerCallTimeout: 3 * time.Second}, // 每次调用最多3秒
)
```

### Worker池提交重试

```go
pool := async.NewPool[Result](async.IO())
defer pool.Close()

// 高负载下提交可能超时，自动退避重试
err := retry.BindRetryToWorker(ctx, pool, func(ctx context.Context) error {
    return processItem(ctx, item)
}, 3, 10*time.Millisecond, 500*time.Millisecond)
```

### 不同退避策略对比

```go
// 指数退避：适合依赖后端恢复的场景（如 RPC 重连）
retry.RetryWithBackoff(ctx, fn, 5, 100*time.Millisecond, 30*time.Second)

// 线性退避：适合需要稳定间隔的轮询场景
retry.RetryWithLinearBackoff(ctx, fn, 10, 5*time.Second)

// 无退避：适合短暂故障立即重试
retry.RetryFn(func() error { return tryLock() }).WithRetry(3)
```

---

## 性能基准

| 场景 | 数据量 | 吞吐量 |
|------|--------|--------|
| Retry 无错误（首次成功） | 1M | **51M ops/s** |
| Retry Backoff Race | 100K × 100并发 | **零竞态** |