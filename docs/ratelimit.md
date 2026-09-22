# 限流器（RateLimiter）文档

## 概述

async 提供四种限流器，覆盖不同场景：

| 限流器 | 算法 | 适用场景 | 吞吐量(10M) |
|--------|------|----------|-------------|
| `RateLimiter` | 令牌桶（批量补充） | 精确 QPS 控制 | **194万/s** |
| `TokenBucket` | 经典令牌桶 | 简单速率限制 | **194万/s** |
| `SlidingWindow` | 滑动窗口 | 精确窗口限制 | **159万/s** |
| `AdaptiveRateLimiter` | 自适应 | 动态负载调整 | — |

---

## 目录

- [RateLimiter（批量补充令牌桶）](#ratelimiter批量补充令牌桶)
  - [创建：NewRateLimiter / NewRateLimiterWithBurst](#创建)
  - [使用模式：Wait/Release / Token / Acquire](#使用模式)
  - [基础方法：Wait / Acquire / Release / Token](#wait)
  - [配置方法：WithStrategy / WithTraceID / Resize](#withstrategy)
  - [状态查询：Size / Available](#size)
  - [生命周期：Stop / Close](#stop)
- [TokenBucket（经典令牌桶）](#tokenbucket经典令牌桶)
  - [NewTokenBucket / Allow / AllowN](#newtokenbucket)
- [SlidingWindow（滑动窗口）](#slidingwindow滑动窗口)
  - [NewSlidingWindowRateLimiter / Allow / AllowN](#newslidingwindowratelimiter)
- [AdaptiveRateLimiter（自适应限流）](#adaptiveratelimiter自适应限流)
  - [New / Acquire / Release / RecordSuccess / RecordFailure](#newadaptiveratelimiter)
- [批量补充优化](#批量补充优化)
- [架构说明](#架构说明)
- [性能基准](#性能基准)

## RateLimiter（批量补充令牌桶）

**v2 优化**: 高速率场景使用 **100ms 批量补充**替代逐令牌补充，CPU 开销大幅降低。

### 创建

#### NewRateLimiter

```go
// 语法
func NewRateLimiter(rate int, interval time.Duration) *RateLimiter
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `rate` | `int` | 每个 interval 内的令牌数 |
| `interval` | `time.Duration` | 补充间隔 |

```go
rl := async.NewRateLimiter(100, time.Second) // 每秒 100 个令牌
defer rl.Close()
```

#### NewRateLimiterWithBurst

```go
// 语法
func NewRateLimiterWithBurst(rate int, interval time.Duration, burst int) *RateLimiter
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `rate` | `int` | 令牌速率 |
| `interval` | `time.Duration` | 补充间隔 |
| `burst` | `int` | 突发容量（允许短时超过 rate） |

```go
rl := async.NewRateLimiterWithBurst(50, time.Second, 200)
```

---

### 使用模式

#### 模式一：Wait/Release（推荐）

```go
rl.Wait(ctx)      // 等待一个令牌
doRequest()
rl.Release()      // 释放令牌（归还）
```

#### 模式二：Token defer（最安全）

```go
token, err := rl.Token(ctx)
if err != nil {
    return
}
defer token.Release()
doRequest()
```

#### 模式三：Acquire + 策略切换

```go
rl.WithStrategy(async.Reject) // 满时拒绝
rl.Acquire(ctx)                // 按当前策略获取
doRequest()
rl.Release()
```

---

### 完整方法列表

| 方法 | 语法 | 说明 |
|------|------|------|
| `Wait` | `func (rl *RateLimiter) Wait(ctx context.Context) error` | 阻塞等待一个令牌 |
| `Token` | `func (rl *RateLimiter) Token(ctx context.Context) (*Token, error)` | 获取令牌包装器（配合 defer） |
| `Acquire` | `func (rl *RateLimiter) Acquire(ctx context.Context) error` | 按当前策略获取令牌 |
| `Release` | `func (rl *RateLimiter) Release()` | 释放一个令牌（归还） |
| `WithStrategy` | `func (rl *RateLimiter) WithStrategy(s Strategy) *RateLimiter` | 切换策略 |
| `Resize` | `func (rl *RateLimiter) Resize(newRate int)` | 调整速率 |
| `Size` | `func (rl *RateLimiter) Size() int` | 当前令牌总数 |
| `Available` | `func (rl *RateLimiter) Available() int` | 可用令牌数 |
| `Stop` | `func (rl *RateLimiter) Stop()` | 停止补充（优雅关闭前） |
| `Close` | `func (rl *RateLimiter) Close()` | 完全关闭 |

---

### Wait

阻塞等待一个可用令牌，在令牌可用前一直阻塞。这是最基础的限流方式，适合需要严格速率控制的场景。

```go
// 语法
func (rl *RateLimiter) Wait(ctx context.Context) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，取消时返回 ctx.Err() |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | nil 表示获取成功；context 取消时返回 ctx.Err()；限流器关闭时返回 ErrRateLimitExceeded |

```go
rl := async.NewRateLimiter(10, time.Second)
defer rl.Close()

for i := 0; i < 100; i++ {
    if err := rl.Wait(ctx); err != nil {
        log.Printf("等待令牌失败: %v", err)
        break
    }
    go doRequest()
    rl.Release() // 归还令牌
}
```

---

### Acquire

按当前策略获取令牌。策略由 `WithStrategy` 设置，默认 `Block`（阻塞等待）。

```go
// 语法
func (rl *RateLimiter) Acquire(ctx context.Context) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，策略为 Block 时取消会中断等待 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | nil 表示获取成功；Reject 策略满时返回 ErrRateLimitExceeded |

| 策略 | 行为 |
|------|------|
| `Block`（默认） | 阻塞等待，直到有空位或 context 取消 |
| `Reject` | 满时立即返回 ErrRateLimitExceeded |
| `BlockForce` | 强制阻塞，忽略 context 取消（慎用） |

```go
rl := async.NewRateLimiter(5, time.Second)

// 阻塞等待模式（默认）
if err := rl.Acquire(ctx); err != nil {
    return err
}
defer rl.Release()
doRequest()

// 拒绝模式：满时立即返回错误
if err := rl.WithStrategy(async.Reject).Acquire(ctx); err != nil {
    if errors.Is(err, async.ErrRateLimitExceeded) {
        return fmt.Errorf("系统繁忙，请稍后重试")
    }
    return err
}
defer rl.Release()
doRequest()
```

---

### Release

归还一个令牌，释放一个槽位供后续请求使用。与 `Wait`/`Acquire`/`Token` 配对使用。

```go
// 语法
func (rl *RateLimiter) Release()
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

```go
rl.Wait(ctx)     // 获取令牌
defer rl.Release() // 确保归还
doRequest()
```

---

### Token

获取令牌包装器，配合 `defer token.Release()` 实现最安全的使用模式。令牌在函数返回时自动释放，避免因提前 return 导致令牌泄漏。

```go
// 语法
func (rl *RateLimiter) Token(ctx context.Context) (*Token, error)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*Token` | `*Token` | 令牌包装器，调用 `Release()` 归还 |
| `error` | `error` | 获取失败时的错误 |

```go
// 推荐：defer 模式（最安全）
token, err := rl.Token(ctx)
if err != nil {
    return err
}
defer token.Release()

// 执行需要限流的操作
resp, err := callExternalAPI(ctx, req)
if err != nil {
    return err // defer 自动释放，不会泄漏
}
processResponse(resp)
```

---

### Token.Release

归还通过 `Token()` 获取的令牌。线程安全，可安全地在 defer 中调用。

```go
// 语法
func (t *Token) Release()
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

```go
token, err := rl.Token(ctx)
if err != nil {
    return err
}
defer token.Release()
```

---

### WithStrategy

切换获取令牌的策略，返回 `*RateLimiter` 以支持链式调用。

```go
// 语法
func (rl *RateLimiter) WithStrategy(s Strategy) *RateLimiter
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `s` | `Strategy` | 策略常量：`Block`、`Reject`、`BlockForce` |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*RateLimiter` | `*RateLimiter` | 自身实例，支持链式调用 |

| 策略常量 | 值 | 说明 |
|----------|-----|------|
| `async.Block` | 0 | 阻塞等待（默认），context 取消时返回 |
| `async.Reject` | 1 | 满时立即拒绝，返回 `ErrRateLimitExceeded` |
| `async.BlockForce` | 2 | 强制阻塞，忽略 context 取消（慎用） |

```go
// 动态切换策略
rl := async.NewRateLimiter(100, time.Second)

// 高峰期：拒绝策略
if isPeakHour() {
    rl.WithStrategy(async.Reject)
}

if err := rl.Acquire(ctx); err != nil {
    return err
}
defer rl.Release()
doRequest()

// 链式调用
token, err := rl.WithStrategy(async.Block).Token(ctx)
if err != nil {
    return err
}
defer token.Release()
```

---

### WithTraceID

注入 TraceID 到 context，返回新的 context 和限流器自身，用于分布式追踪。

```go
// 语法
func (rl *RateLimiter) WithTraceID(ctx context.Context) (*RateLimiter, context.Context)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 原始上下文 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*RateLimiter` | `*RateLimiter` | 自身实例 |
| `context.Context` | `context.Context` | 注入 TraceID 后的新 context |

```go
rl, tracedCtx := rl.WithTraceID(ctx)
if err := rl.Wait(tracedCtx); err != nil {
    return err
}
defer rl.Release()
doRequest()
```

---

### Resize

运行时动态调整令牌速率。并发安全，可在不关闭限流器的情况下调整限流强度。

```go
// 语法
func (rl *RateLimiter) Resize(newRate int)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `newRate` | `int` | 新的令牌速率（每个 interval 补充的令牌数） |

```go
rl := async.NewRateLimiter(100, time.Second)

// 监控到流量激增，动态扩容
if monitor.CurrentQPS() > 80 {
    rl.Resize(200) // 改为每秒 200 个令牌
}

// 低谷期降低速率
if offPeak() {
    rl.Resize(50) // 降为每秒 50 个令牌
}
```

---

### Size

返回令牌桶的容量，即创建时的 `rate` 参数值。

```go
// 语法
func (rl *RateLimiter) Size() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 令牌桶的总容量 |

```go
rl := async.NewRateLimiter(100, time.Second)
fmt.Println(rl.Size()) // 100
```

---

### Available

返回当前可用的令牌数，用于监控和告警。

```go
// 语法
func (rl *RateLimiter) Available() int
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `int` | `int` | 当前可用令牌数（可能瞬时变化） |

```go
rl := async.NewRateLimiter(100, time.Second)

// 监控可用令牌
go func() {
    ticker := time.NewTicker(5 * time.Second)
    for range ticker.C {
        avail := rl.Available()
        if avail < 10 {
            log.Printf("限流器可用令牌过低: %d/%d", avail, rl.Size())
        }
    }
}()
```

---

### Stop

停止令牌的自动补充。调用后不会再有新令牌写入 channel，但已存在的令牌仍然可以消费。通常用于优雅关闭前的第一步。

```go
// 语法
func (rl *RateLimiter) Stop()
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

```go
rl.Stop() // 停止补充

// 等待所有正在进行的请求完成
wg.Wait()

rl.Close() // 完全关闭
```

---

### Close

完全关闭限流器，释放所有资源。调用后 `Wait`/`Acquire`/`Token` 均返回 `ErrRateLimitExceeded`。

```go
// 语法
func (rl *RateLimiter) Close()
```

| 参数 | 类型 | 说明 |
|------|------|------|
| 无 | — | — |

```go
rl.Stop()   // 优雅关闭：先停止补充令牌
// ... 等待所有进行中的请求完成 ...
rl.Close()  // 完全关闭

// 或者直接强制关闭
rl.Close() // 立即释放所有资源
```

---

## TokenBucket（经典令牌桶）

无需 goroutine 的轻量级令牌桶。

### NewTokenBucket

```go
// 语法
func NewTokenBucket(rate float64, capacity int64) *TokenBucket
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `rate` | `float64` | 每秒生成令牌数 |
| `capacity` | `int64` | 最大令牌存储量（突发容量） |

```go
tb := async.NewTokenBucket(100, 200) // 速率 100/s，容量 200
```

### Allow

```go
// 语法
func (tb *TokenBucket) Allow() bool
```

消耗 1 个令牌。返回 true 表示允许通过。

```go
if tb.Allow() {
    doRequest()
}
```

### AllowN

```go
// 语法
func (tb *TokenBucket) AllowN(n float64) bool
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `n` | `float64` | 消耗的令牌数 |

消耗 N 个令牌。

```go
if tb.AllowN(10) {
    batchProcess()
}
```

---

## SlidingWindow（滑动窗口）

基于时间的滑动窗口限流器。

### NewSlidingWindowRateLimiter

```go
// 语法
func NewSlidingWindowRateLimiter(limit int, window time.Duration) *SlidingWindowRateLimiter
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `limit` | `int` | 窗口内最多允许次数 |
| `window` | `time.Duration` | 窗口大小 |

```go
sw := async.NewSlidingWindowRateLimiter(100, 10*time.Second)
// 每 10 秒最多 100 次
```

### Allow

```go
// 语法
func (sw *SlidingWindowRateLimiter) Allow() bool
```

检查是否允许 1 次请求。

```go
if sw.Allow() {
    doRequest()
}
```

### AllowN

```go
// 语法
func (sw *SlidingWindowRateLimiter) AllowN(n int) bool
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `n` | `int` | 请求次数 |

```go
if sw.AllowN(5) {
    doBatchRequest()
}
```

---

## AdaptiveRateLimiter（自适应限流）

根据成功率自动调整并发度的限流器。

### NewAdaptiveRateLimiter

```go
// 语法
func NewAdaptiveRateLimiter(minConcurrency, maxConcurrency int) *AdaptiveRateLimiter
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `minConcurrency` | `int` | 最小并发度 |
| `maxConcurrency` | `int` | 最大并发度 |

```go
al := async.NewAdaptiveRateLimiter(5, 100)
```

### Acquire

```go
// 语法
func (al *AdaptiveRateLimiter) Acquire(ctx context.Context) (*AdaptiveToken, error)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| 返回 | `(*AdaptiveToken, error)` | 令牌，配合 defer Release |

获取执行槽位。

### Release

```go
// 语法
func (al *AdaptiveRateLimiter) Release()
```

释放槽位。

### RecordSuccess / RecordFailure

```go
// 语法
func (al *AdaptiveRateLimiter) RecordSuccess()
func (al *AdaptiveRateLimiter) RecordFailure()
```

记录执行结果，用于自动调整并发度。

```go
token, err := al.Acquire(ctx)
if err != nil {
    return err
}
defer token.Release()

if err := doRequest(); err == nil {
    al.RecordSuccess()  // 成功 → 可能自动扩容
} else {
    al.RecordFailure()  // 失败 → 可能自动缩容
}
```

---

## 批量补充优化

v2 版本中，`RateLimiter.startRefill()` 使用自适应批量补充策略：

- **高速率**（间隔 < 100ms）：100ms 批量补充，一次补充 `100ms / 间隔` 个令牌
- **中等速率**：逐令牌补充
- **极低速率**（间隔 > 1s）：最长 1 秒补充一次，每次补充 1 个令牌

性能提升：TokenBucket 千万次 Allow 从 1.74M→1.94M ops/s（+20%）。

---

## 架构说明

```
┌─────────────────────────────────────┐
│            RateLimiter               │
│  ┌─────────┐  ┌───────────────────┐ │
│  │ tokens  │  │  startRefill()    │ │
│  │ channel │◄─┤  100ms batch      │ │
│  │ (buf)   │  │  refill goroutine │ │
│  └────┬────┘  └───────────────────┘ │
│       │                              │
│  ┌────▼────┐  ┌───────────────────┐ │
│  │  Wait   │  │     Release       │ │
│  │ (consume)│  │     (return)      │ │
│  └─────────┘  └───────────────────┘ │
└─────────────────────────────────────┘
```

---

## 性能基准

| 场景 | 吞吐量 | 说明 |
|------|--------|------|
| TokenBucket Allow 10M | **194万/s** | 批量补充优化后 ↑20% |
| SlidingWindow Allow 10M | **159万/s** | 滑动窗口 |
| RateLimiter Acquire/Release 1M | **924万/s** | 1M ops/s |