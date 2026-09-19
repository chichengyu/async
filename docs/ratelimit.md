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

// 支持突发容量：每秒 50 个，最多突发 200 个
rl := async.NewRateLimiterWithBurst(50, time.Second, 200)
defer rl.Close()
```

### 获取令牌

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
// 动态调整速率
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

// 检查是否允许
if sw.Allow() {
    doRequest()
} else {
    http.Error(w, "rate limit exceeded", 429)
}

// 批量检查（n 个请求）
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

if tb.Allow() {
    doRequest()
}

// 批量消费 n 个令牌
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
| 立即拒绝 | `async.Reject` | 令牌不足时立即返回 `ErrRateLimiterStopped` |
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

| 方法 | 说明 |
|------|------|
| `NewRateLimiter(rate, perDuration)` | 创建定时补充令牌的限流器 |
| `NewRateLimiterWithBurst(rate, d, burst)` | 创建支持突发容量的限流器 |
| `Wait(ctx)` / `Acquire(ctx)` | 获取令牌 |
| `Release()` | 归还令牌 |
| `Token(ctx)` | 获取可 defer 释放的 Token |
| `Close()` / `Stop()` | 关闭限流器 |
| `Resize(newRate)` | 动态调整速率 |
| `Size()` | 当前速率大小 |
| `Available()` | 当前可用令牌数 |
| `WithStrategy(s)` | 切换策略 |

### SlidingWindowRateLimiter

| 方法 | 说明 |
|------|------|
| `NewSlidingWindowRateLimiter(limit, window)` | 创建滑动窗口限流器 |
| `Allow()` | 检查 1 个请求是否允许 |
| `AllowN(n)` | 检查 n 个请求是否允许 |

### TokenBucket

| 方法 | 说明 |
|------|------|
| `NewTokenBucket(rate, capacity)` | 创建经典令牌桶 |
| `Allow()` | 消费 1 个令牌 |
| `AllowN(n)` | 消费 n 个令牌 |

### AdaptiveRateLimiter

| 方法 | 说明 |
|------|------|
| `NewAdaptiveRateLimiter(min, max)` | 创建自适应限流器 |
| `Acquire(ctx)` | 获取许可 |
| `Release()` | 释放许可 |
| `RecordSuccess()` | 记录成功 |
| `RecordFailure()` | 记录失败 |