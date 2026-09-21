# 限流器 (RateLimiter)

限流模块提供了四种限流器实现，用于控制并发速率，保护下游服务不被过载。

## 目录

- [核心概念](#核心概念)
- [RateLimiter - 令牌桶限流器](#ratelimiter---令牌桶限流器)
- [SlidingWindowRateLimiter - 滑动窗口](#slidingwindowratelimiter---滑动窗口)
- [TokenBucket - 经典令牌桶](#tokenbucket---经典令牌桶)
- [AdaptiveRateLimiter - 自适应限流](#adaptiveratelimiter---自适应限流)
- [限流策略](#限流策略)
- [完整示例](#完整示例)

---

## 核心概念

```
四种限流器：
  RateLimiter              → 定时补充令牌，支持策略切换
  TokenBucket              → 经典令牌桶算法，按速率生成令牌
  SlidingWindowRateLimiter → 滑动时间窗口计数
  AdaptiveRateLimiter      → 根据成功率自动调整并发度

限流策略：
  Block      → 阻塞等待令牌（默认）
  Reject     → 队列满时立即拒绝
  BlockForce → 强制阻塞，忽略 ctx 取消
```

---

## RateLimiter - 令牌桶限流器

定时按速率补充令牌，适合大多数速率控制场景。

### 创建

```go
// 每秒最多 100 个请求
rl := async.NewRateLimiter(100, time.Second)
defer rl.Close()
```

**`NewRateLimiter` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `rate` | `int` | **IO()**（CPU 核数×2，rate≤0 时） | 每个 `perDuration` 补充的令牌数 |
| `perDuration` | `time.Duration` | 无（必传） | 令牌补充间隔 |
| 默认策略 | — | **Block**（阻塞等待） | 令牌不足时阻塞直到 ctx 取消 |
| 初始令牌 | — | **0**（由 refill goroutine 逐步补充） | refill 间隔为 `perDuration/rate` |

```go
// 支持突发容量：每秒 50 个，最多突发 200 个
rl := async.NewRateLimiterWithBurst(50, time.Second, 200)
defer rl.Close()
```

**`NewRateLimiterWithBurst` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `rate` | `int` | **IO()**（rate≤0 时） | 每 `perDuration` 补充的令牌数 |
| `perDuration` | `time.Duration` | 无（必传） | 令牌补充间隔 |
| `burst` | `int` | **max(rate, burst)**（burst<rate 时至少取 rate） | 突发容量，令牌池最多缓存的令牌数 |

### 使用获取令牌

```go
rl := async.NewRateLimiter(10, time.Second)
defer rl.Close()

// 方式1：Wait（阻塞等待令牌）
if err := rl.Wait(ctx); err != nil {
    return err
}
doRequest()

// 方式2：Acquire（与 Wait 等价）
if err := rl.Acquire(ctx); err != nil {
    return err
}
doRequest()

// 方式3：Token（defer 自动释放）
token, err := rl.Token(ctx)
if err != nil {
    return err
}
defer token.Release()
doRequest()
```

### 释放令牌

```go
rl.Release() // 归还令牌（手动方式，Token 方式不需要）
```

### 动态调整和查询

```go
// 动态调整速率（newRate <= 0 时忽略，不变更）
rl.Resize(200) // 调整到每秒 200 个

// 查询当前大小
size := rl.Size()

// 查询可用令牌数
available := rl.Available()
```

### 策略切换

```go
// 阻塞等待（默认）
rl.WithStrategy(async.Block)

// 立即拒绝
rl.WithStrategy(async.Reject)

// 强制阻塞忽略 ctx 取消
rl.WithStrategy(async.BlockForce)
```

---

## SlidingWindowRateLimiter - 滑动窗口

基于滑动时间窗口的计数限流。窗口内请求数超过限制时拒绝。

```go
// 每 10 秒最多 100 次
sw := async.NewSlidingWindowRateLimiter(100, 10*time.Second)
```

**`NewSlidingWindowRateLimiter` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `limit` | `int` | 无（必传） | 窗口内最大请求数 |
| `window` | `time.Duration` | 无（必传） | 滑动窗口时间长度 |

```go
// 检查是否允许
if sw.Allow() {
    doRequest()
} else {
    http.Error(w, "rate limit exceeded", 429)
}

// 批量检查 n 个请求（n <= 0 时始终返回 true）
if sw.AllowN(5) {
    batchProcess(5)
}
```

---

## TokenBucket - 经典令牌桶

按固定速率生成令牌，支持浮点数速率。

```go
// 每秒生成 10 个令牌，最多存储 20 个
tb := async.NewTokenBucket(10, 20)
```

**`NewTokenBucket` 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `rate` | `float64` | 无（必传） | 每秒生成令牌数（支持浮点） |
| `capacity` | `float64` | 无（必传） | 令牌桶最大容量，初始令牌数为 capacity |

```go
if tb.Allow() {
    doRequest()
}

// 批量消费 n 个令牌（n <= 0 时始终返回 true）
if tb.AllowN(3) {
    batchProcess(3)
}
```

---

## AdaptiveRateLimiter - 自适应限流

根据请求成功率自动调整并发度。成功率低时降低并发度，成功率高时提升并发度。

```go
// 并发度范围 5-100，初始值为中点 52
al := async.NewAdaptiveRateLimiter(5, 100)
```

**`NewAdaptiveRateLimiter` 参数与默认值：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `minRate` | `int` | **1**（minRate≤0 时） | 最小并发度（下限） |
| `maxRate` | `int` | **minRate**（maxRate≤minRate 时） | 最大并发度（上限） |
| 初始并发度 | `int` | **(minRate+maxRate)/2** | 首次运行时使用的并发度 |
| 上调阈值 | `float64` | **0.2** | 失败率 ≤20% 时按 1 递增 |
| 下调阈值 | `float64` | **0.5** | 失败率 >20% 时乘 `(1-0.5)` 递减 |
| 最少样本数 | `int` | **10** | 至少积累 10 次调用后才开始自动调整 |

```go
// 获取许可
if err := al.Acquire(ctx); err != nil {
    return err
}

// 执行请求
if err := doRequest(); err == nil {
    al.RecordSuccess() // 记录成功
} else {
    al.RecordFailure() // 记录失败
}

// 释放许可
al.Release()
```

**工作原理**：
- 累计 10 次请求后开始评估
- 失败率 > 20%：降低并发度（乘以 0.5）
- 失败率 < 10% 且未达上限：提升并发度
- 升降后重置计数器

---

## 限流策略

| 策略 | 常量 | 行为 |
|------|------|------|
| 阻塞等待 | `async.Block` | 令牌不足时阻塞，ctx 取消时返回错误 |
| 立即拒绝 | `async.Reject` | 令牌不足时立即返回 `ErrRateLimitExceeded` |
| 强制阻塞 | `async.BlockForce` | 令牌不足时阻塞，忽略 ctx 取消信号 |

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

    // 示例1：RateLimiter 控制 API 调用频率
    rl := async.NewRateLimiter(10, time.Second) // 每秒 10 次
    defer rl.Close()

    for i := 0; i < 100; i++ {
        if err := rl.Wait(ctx); err != nil {
            log.Printf("等待令牌失败: %v", err)
            break
        }
        fmt.Printf("请求 %d 发送\n", i)
    }

    // 示例2：使用 Token 模式 + defer
    rl2 := async.NewRateLimiter(50, time.Second)
    defer rl2.Close()

    for _, url := range urls {
        token, err := rl2.Token(ctx)
        if err != nil {
            log.Printf("获取令牌失败: %v", err)
            continue
        }
        go func(u string) {
            defer token.Release()
            fetchURL(u)
        }(url)
    }

    // 示例3：自适应限流
    al := async.NewAdaptiveRateLimiter(5, 100)
    for _, task := range tasks {
        if err := al.Acquire(ctx); err != nil {
            log.Printf("获取许可失败: %v", err)
            continue
        }

        go func(t Task) {
            defer al.Release()
            if err := t.Execute(ctx); err != nil {
                al.RecordFailure()
            } else {
                al.RecordSuccess()
            }
        }(task)
    }

    // 示例4：滑动窗口做 Web 中间件
    sw := async.NewSlidingWindowRateLimiter(100, time.Minute) // 每分钟 100 次

    // 在 HTTP handler 中
    if !sw.Allow() {
        // 返回 429 Too Many Requests
        return
    }
    handleRequest(w, r)
}
```

---

## 方法速查表

### RateLimiter

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `NewRateLimiter` | `func NewRateLimiter(rate int, perDuration time.Duration) *RateLimiter` | 创建定时补充令牌的限流器 |
| `NewRateLimiterWithBurst` | `func NewRateLimiterWithBurst(rate int, perDuration time.Duration, burst int) *RateLimiter` | 创建支持突发容量的限流器 |
| `Wait` | `func (rl *RateLimiter) Wait(ctx context.Context) error` | 阻塞等待直到获取令牌 |
| `Acquire` | `func (rl *RateLimiter) Acquire(ctx context.Context) error` | 获取令牌（`Wait` 的别名） |
| `Release` | `func (rl *RateLimiter) Release()` | 归还令牌 |
| `Token` | `func (rl *RateLimiter) Token(ctx context.Context) (*Token, error)` | 获取可 `defer t.Release()` 释放的 Token |
| `Close` | `func (rl *RateLimiter) Close()` | 关闭限流器，清理资源 |
| `Stop` | `func (rl *RateLimiter) Stop()` | 停止限流器（`Close` 的别名） |
| `Resize` | `func (rl *RateLimiter) Resize(newRate int)` | 动态调整速率 |
| `Size` | `func (rl *RateLimiter) Size() int` | 当前速率大小（每秒令牌数） |
| `Available` | `func (rl *RateLimiter) Available() int` | 当前可用令牌数 |
| `WithStrategy` | `func (rl *RateLimiter) WithStrategy(s Strategy) *RateLimiter` | 切换限流策略 |
| `WithTraceID` | `func (rl *RateLimiter) WithTraceID(ctx context.Context) (*RateLimiter, context.Context)` | 设置 TraceID |

### Token（RateLimiter 返回的令牌句柄）

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `Release` | `func (t *Token) Release()` | 归还令牌 |

### SlidingWindowRateLimiter

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `NewSlidingWindowRateLimiter` | `func NewSlidingWindowRateLimiter(limit int, window time.Duration) *SlidingWindowRateLimiter` | 创建滑动窗口限流器 |
| `Allow` | `func (sw *SlidingWindowRateLimiter) Allow() bool` | 检查 1 个请求是否允许 |
| `AllowN` | `func (sw *SlidingWindowRateLimiter) AllowN(n int) bool` | 检查 n 个请求是否允许 |

### TokenBucket（经典令牌桶，无时间维度）

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `NewTokenBucket` | `func NewTokenBucket(rate float64, capacity float64) *TokenBucket` | 创建经典令牌桶 |
| `Allow` | `func (tb *TokenBucket) Allow() bool` | 消费 1 个令牌 |
| `AllowN` | `func (tb *TokenBucket) AllowN(n float64) bool` | 消费 n 个令牌 |

### AdaptiveRateLimiter

| 方法 | 完整签名 | 说明 |
|------|---------|------|
| `NewAdaptiveRateLimiter` | `func NewAdaptiveRateLimiter(minRate, maxRate int) *AdaptiveRateLimiter` | 创建自适应限流器 |
| `Acquire` | `func (a *AdaptiveRateLimiter) Acquire(ctx context.Context) error` | 获取许可（阻塞等待） |
| `Release` | `func (a *AdaptiveRateLimiter) Release()` | 释放许可 |
| `RecordSuccess` | `func (a *AdaptiveRateLimiter) RecordSuccess()` | 记录成功（降低限流压力） |
| `RecordFailure` | `func (a *AdaptiveRateLimiter) RecordFailure()` | 记录失败（增大限流压力，速率衰减） |

### 限流策略常量

| 常量 | 值 | 说明 |
|------|---|------|
| `StrategyRateLimiter` | 固定速率补充令牌 | 默认策略 |
| `StrategyPriorityRateLimiter` | 优先级感知限流 | 高优先级请求优先获取令牌 |

### 类型定义

| 类型 | 说明 |
|------|------|
| `RateLimiter` | 基于令牌桶的通用限流器 |
| `SlidingWindowRateLimiter` | 基于滑动窗口的限流器 |
| `TokenBucket` | 经典令牌桶（无时间维度） |
| `AdaptiveRateLimiter` | 自适应限流器，根据成功率自动调速率 |