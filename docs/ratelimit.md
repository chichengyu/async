# 限流器（RateLimiter）文档

## 概述

async 提供四种限流器，覆盖不同场景。所有限流器通过 `async.Ratelimit(ctx)` 链式构建器创建：

| 限流器 | 算法 | 适用场景 | 吞吐量(10M) |
|--------|------|----------|-------------|
| `RateLimiter` | 令牌桶（定时补充，可手动归还） | 令牌获取与归还 | **194万/s** |
| `TokenBucket` | 经典令牌桶 | 简单速率限制 | **194万/s** |
| `SlidingWindow` | 滑动窗口 | 精确窗口限制 | **159万/s** |
| `AdaptiveRateLimiter` | 自适应 | 动态负载调整 | — |

> `Resize`、`Acquire` 和 `Release` 支持并发调用；相关并发场景有竞态和压力测试覆盖。
>
> **⚠️ RateLimiter 需手动 Close**
>
> `RateLimiter` 内部有 ticker goroutine，使用完毕后必须调用 `Close()` 或 `Stop()` 释放资源，否则 goroutine 泄漏。
>
> **⚠️ 默认策略会阻塞**
>
> 默认 `Block` 策略下，`Wait(ctx)` 和 `Acquire(ctx)` 在令牌不足时都会等待，直到取得令牌或上下文取消。需要立即返回时，可配置 `Reject` 策略并检查返回错误。
>
> **⚠️ Burst 不是缓存**
>
> `Burst` 是初始和最大令牌容量。消耗后令牌可由定时补充或 `Release` 归还，因此使用 `Release` 时不保证严格的时间窗口请求上限。需要严格限制窗口内请求次数时可考虑 `SlidingWindow`。
> 直接构造 `NewRateLimiterWithBurst` 时，若 burst 小于有效 rate，实际容量按 rate 设置；新建时令牌桶填满该容量。

---

## 目录

- [链式构建 API 速查表](#链式构建-api-速查表)
- [RateLimiter（批量补充令牌桶）](#ratelimiter批量补充令牌桶)
  - [构建：Ratelimit().RateLimiter()](#构建-ratelimiter)
  - [使用模式：Wait/Release / Token / Acquire](#使用模式)
  - [基础方法：Wait / Acquire / Release / Token](#wait)
  - [配置方法：WithStrategy / WithTraceID / Resize](#withstrategy)
  - [状态查询：Size / Available](#size)
  - [生命周期：Stop / Close](#stop)
- [TokenBucket（经典令牌桶）](#tokenbucket经典令牌桶)
  - [构建：Ratelimit().TokenBucket()](#构建-tokenbucket)
  - [Allow / AllowN](#allow)
- [SlidingWindow（滑动窗口）](#slidingwindow滑动窗口)
  - [构建：Ratelimit().SlidingWindow()](#构建-slidingwindow)
  - [Allow / AllowN](#allow-1)
- [AdaptiveRateLimiter（自适应限流）](#adaptiveratelimiter自适应限流)
  - [构建：Ratelimit().Adaptive()](#构建-adaptive)
  - [Acquire / Release / RecordOk / RecordFailure](#acquire)
- [分片限流器](#分片限流器)
  - [构建：Ratelimit().Sharded()](#构建-sharded)
  - [ShardedRateLimiter / ShardedTokenBucket / ShardedSlidingWindow / ShardedAdaptive](#分片类型)
- [批量补充优化](#批量补充优化)
- [架构说明](#架构说明)
- [性能基准](#性能基准)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)

## 链式构建 API 速查表

| 入口 | 说明 |
|------|------|
| `async.Ratelimit(ctx)` | 创建限流器链式构建器 |

### 普通模式构建链路

| 链路 | 返回类型 | 示例 |
|------|---------|------|
| `.RateLimiter().Rate(n).Per(d).Burst(n).Build()` | `*RateLimiter` | 每秒 100 令牌 |
| `.TokenBucket().Rate(n).Capacity(n).Build()` | `*TokenBucket` | 速率 10/s，容量 20 |
| `.SlidingWindow().Limit(n).Window(d).Build()` | `*SlidingWindowRateLimiter` | 每 10 秒 100 次 |
| `.Adaptive().MinWorker(n).MaxWorker(n).Build()` | `*AdaptiveRateLimiter` | 并发度 5~100 |

### 分片模式构建链路

| 链路 | 返回类型 | 示例 |
|------|---------|------|
| `.Sharded().RateLimiter().Shards(n).Rate(n).Per(d).Burst(n).Build()` | `*ShardedRateLimiter` | 16 分片，总速率 10000/s |
| `.Sharded().TokenBucket().Shards(n).Rate(n).Capacity(n).Build()` | `*ShardedTokenBucket` | 16 分片令牌桶 |
| `.Sharded().SlidingWindow().Shards(n).Limit(n).Window(d).Build()` | `*ShardedSlidingWindow` | 8 分片滑动窗口 |
| `.Sharded().Adaptive().Shards(n).MinWorker(n).MaxWorker(n).Build()` | `*ShardedAdaptive` | 16 分片自适应 |

---

## RateLimiter（批量补充令牌桶）

**v2 优化**: 高速率场景使用 **100ms 批量补充**替代逐令牌补充，CPU 开销大幅降低。

### 构建 RateLimiter

所有限流器均通过 `async.Ratelimit(ctx)` 链式构建：

```go
// 基础：每秒 100 个令牌
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

// 带突发容量
rl := async.Ratelimit(ctx).RateLimiter().Rate(50).Per(time.Second).Burst(200).Build()
defer rl.Close()

// 使用默认值
rl := async.Ratelimit(ctx).RateLimiter().DefaultRate().DefaultPer().Build()
defer rl.Close()
```

| 构建方法 | 说明 |
|----------|------|
| `.Rate(n int)` | 每时间窗口操作次数 |
| `.Per(d)` | 时间窗口大小 |
| `.Burst(n int)` | 突发容量 |
| `.DefaultRate()` | 默认速率 = `core.IO()` |
| `.DefaultPer()` | 默认窗口 = 1 秒 |
| `.DefaultBurst()` | 默认突发 = `IO() × 2` |
| `.Build()` | 构建 `*RateLimiter` 实例 |

### 使用模式

#### 模式一：Wait/Release（推荐）

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

if err := rl.Wait(ctx); err != nil {
    return err
}
defer rl.Release()
doRequest()
```

#### 模式二：Token defer（最安全）

```go
token, err := rl.Token(ctx)
if err != nil {
    return err
}
defer token.Release()
doRequest()
```

同一个 `*Token` 的 `Release` 可以重复或并发调用，只会归还一次令牌。不要复制 `Token` 的值；需要共享时传递指针。

#### 模式三：Acquire + 策略切换

```go
rl.WithStrategy(async.Reject) // 满时拒绝
if err := rl.Acquire(ctx); err != nil {
    return err
}
defer rl.Release()
doRequest()
```

### 完整方法列表

| 方法 | 语法 | 说明 |
|------|------|------|
| `Wait` | `func (rl *RateLimiter) Wait(ctx context.Context) error` | 与 `Acquire` 相同，按当前策略获取令牌 |
| `Token` | `func (rl *RateLimiter) Token(ctx context.Context) (*Token, error)` | 获取令牌包装器（配合 defer） |
| `Acquire` | `func (rl *RateLimiter) Acquire(ctx context.Context) error` | 按当前策略获取令牌 |
| `Release` | `func (rl *RateLimiter) Release()` | 释放一个令牌（归还） |
| `WithStrategy` | `func (rl *RateLimiter) WithStrategy(s Strategy) *RateLimiter` | 切换策略 |
| `Resize` | `func (rl *RateLimiter) Resize(newRate int)` | 调整速率 |
| `Size` | `func (rl *RateLimiter) Size() int` | 当前配置速率（不等同突发容量或可用令牌数） |
| `Available` | `func (rl *RateLimiter) Available() int` | 可用令牌数 |
| `Stop` | `func (rl *RateLimiter) Stop()` | Close 的别名，行为完全一致 |
| `Close` | `func (rl *RateLimiter) Close()` | 停止补充并释放所有资源 |

### Wait

按当前策略获取一个令牌，默认策略会等待令牌或上下文取消。`Wait` 等同于 `Acquire`，因此也会受 `WithStrategy` 影响；归还令牌可能让后续请求立即通过。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
defer rl.Close()

var wg sync.WaitGroup
for i := 0; i < 100; i++ {
    if err := rl.Wait(ctx); err != nil {
        log.Printf("等待令牌失败: %v", err)
        break
    }
    wg.Add(1)
    go func() {
        defer wg.Done()
        defer rl.Release()
        doRequest()
    }()
}
wg.Wait()
```

### Acquire

按当前策略获取令牌。策略由 `WithStrategy` 设置，默认 `Block`（阻塞等待）。

| 策略 | 行为 |
|------|------|
| `Block`（默认） | 阻塞等待，直到有空位或 context 取消 |
| `Reject` | 满时立即返回 ErrRateLimitExceeded |
| `BlockForce` | 强制阻塞，忽略 context 取消（慎用） |

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(5).Per(time.Second).Build()
defer rl.Close()

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

### Release

归还一个令牌，释放一个槽位供后续请求使用。`Wait` 或 `Acquire` 成功后使用限流器的 `Release`；`Token` 成功后则使用返回的 `token.Release()`，不要将两种释放方式混用。

```go
if err := rl.Wait(ctx); err != nil {
    return err
}
defer rl.Release() // 确保归还
doRequest()
```

### Token

获取令牌包装器，配合 `defer token.Release()` 实现最安全的使用模式。令牌在函数返回时自动释放，避免因提前 return 导致令牌泄漏。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

token, err := rl.Token(ctx)
if err != nil {
    return err
}
defer token.Release()

resp, err := callExternalAPI(ctx, req)
if err != nil {
    return err // defer 自动释放，不会泄漏
}
processResponse(resp)
```

### WithStrategy

切换获取令牌的策略，返回 `*RateLimiter` 以支持链式调用。

| 策略常量 | 值 | 说明 |
|----------|-----|------|
| `async.Block` | 0 | 阻塞等待（默认），context 取消时返回 |
| `async.Reject` | 1 | 满时立即拒绝，返回 `ErrRateLimitExceeded` |
| `async.BlockForce` | 2 | 强制阻塞，忽略 context 取消（慎用） |

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

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

### WithTraceID

注入 TraceID 到 context，返回新的 context 和限流器自身，用于分布式追踪。

```go
rl, tracedCtx := rl.WithTraceID(ctx)
if err := rl.Wait(tracedCtx); err != nil {
    return err
}
defer rl.Release()
doRequest()
```

### Resize

运行时动态调整令牌速率。并发安全，可在不关闭限流器的情况下调整限流强度。

> 并发调整、获取、释放和补充令牌的场景有测试覆盖；应用仍应结合自身负载和超时配置进行验证。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

// 监控到流量激增，动态扩容
if monitor.CurrentQPS() > 80 {
    rl.Resize(200) // 改为每秒 200 个令牌
}

// 低谷期降低速率
if offPeak() {
    rl.Resize(50) // 降为每秒 50 个令牌
}
```

### Size

返回令牌桶的容量，即创建时的 `rate` 参数值。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
fmt.Println(rl.Size()) // 100
```

### Available

返回当前可用的令牌数，用于监控和告警。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()

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

### Stop

`Stop()` 是 `Close()` 的别名，行为完全一致：关闭 refill goroutine，关闭 tokens channel，broadcast 唤醒所有等待者。多个调用方并发关闭同一限流器时，每次调用都会等待关闭清理完成后返回。

```go
rl.Stop() // 等价于 rl.Close()
```

### Close

关闭限流器并停止补充令牌。关闭完成后 `Wait`、`Acquire` 和 `Token` 均返回 `ErrRateLimiterStopped`。

```go
rl.Close() // 停止补充令牌 + 关闭 channel + 释放资源
```

> **⚠️ Stop 与 Close 等价**
>
> `Stop()` 内部直接调用 `Close()`，二者行为完全相同。调用其中任意一个即可，不需要先 `Stop()` 再 `Close()`。

---

## TokenBucket（经典令牌桶）

无需 goroutine 的轻量级令牌桶。

### 构建 TokenBucket

```go
tb := async.Ratelimit(ctx).TokenBucket().Rate(100).Capacity(200).Build()
// 速率 100/s，容量 200
```

| 构建方法 | 说明 |
|----------|------|
| `.Rate(n float64)` | 每秒生成的令牌数 |
| `.Capacity(n float64)` | 最大令牌容量（突发容量） |
| `.DefaultRate()` | 默认速率 = `core.IO()` |
| `.DefaultCapacity()` | 默认容量 = `IO() × 2` |
| `.Build()` | 构建 `*TokenBucket` 实例 |

### Allow

消耗 1 个令牌。返回 true 表示允许通过。

```go
tb := async.Ratelimit(ctx).TokenBucket().Rate(100).Capacity(200).Build()

if tb.Allow() {
    doRequest()
}
```

### AllowN

消耗 N 个令牌。

```go
if tb.AllowN(10) {
    batchProcess()
}
```

---

## SlidingWindow（滑动窗口）

基于时间的滑动窗口限流器。

### 构建 SlidingWindow

```go
sw := async.Ratelimit(ctx).SlidingWindow().Limit(100).Window(10*time.Second).Build()
// 每 10 秒最多 100 次
```

| 构建方法 | 说明 |
|----------|------|
| `.Limit(n int)` | 窗口内最大请求数 |
| `.Window(d)` | 时间窗口大小 |
| `.DefaultLimit()` | 默认限制 = `IO()` |
| `.DefaultWindow()` | 默认窗口 = 1 秒 |
| `.Build()` | 构建 `*SlidingWindowRateLimiter` 实例 |

### Allow

检查是否允许 1 次请求。

```go
if sw.Allow() {
    doRequest()
}
```

### AllowN

检查是否允许 N 次请求。

```go
if sw.AllowN(5) {
    doBatchRequest()
}
```

---

## AdaptiveRateLimiter（自适应限流）

根据成功率自动调整并发度的限流器。

### 构建 Adaptive

```go
al := async.Ratelimit(ctx).Adaptive().MinWorker(5).MaxWorker(100).Build()
// 并发度 5~100 自适应
```

| 构建方法 | 说明 |
|----------|------|
| `.MinWorker(n int)` | 最小并发度 |
| `.MaxWorker(n int)` | 最大并发度 |
| `.DefaultMinWorker()` | 默认最小值 = `IO()` |
| `.DefaultMaxWorker()` | 默认最大值 = `IO() × 10` |
| `.Build()` | 构建 `*AdaptiveRateLimiter` 实例 |

### Acquire

获取执行槽位。

```go
if err := al.Acquire(ctx); err != nil {
    return err
}
defer al.Release()
```

### Release

释放槽位。

### RecordOk / RecordFailure

记录执行结果，用于自动调整并发度。

> 注意：成功结果的 API 名称统一为 RecordOk()。它只记录一次已经完成的成功调用，不会获取或释放并发槽位；每次 Acquire 成功后仍必须配对调用 Release。失败、取消或超时的调用应调用 RecordFailure()。

```go
if err := al.Acquire(ctx); err != nil {
    return err
}
defer al.Release()

if err := doRequest(); err == nil {
    al.RecordOk()  // 成功 → 可能自动扩容
} else {
    al.RecordFailure()  // 失败 → 可能自动缩容
}
```

分片自适应限流器使用相同的 RecordOk() / RecordFailure() 接口；记录方法会按 round-robin 分发到具体分片。不要在同一次请求中重复记录，否则会改变自适应算法观察到的成功率。

---

## 分片限流器

通过 `Ratelimit(ctx).Sharded()` 进入分片链，各分片独立运行，round-robin 分发请求，将锁竞争降低到 1/N。

### 构建 Sharded

```go
// 分片 RateLimiter：16 分片，总速率 10000/s
srl := async.Ratelimit(ctx).Sharded().RateLimiter().Shards(16).Rate(10000).Per(time.Second).Burst(2000).Build()
defer srl.Close()

// 分片 TokenBucket：16 分片令牌桶
stb := async.Ratelimit(ctx).Sharded().TokenBucket().Shards(16).Rate(10).Capacity(20).Build()

// 分片 SlidingWindow：8 分片滑动窗口
ssw := async.Ratelimit(ctx).Sharded().SlidingWindow().Shards(8).Limit(1000).Window(time.Second).Build()

// 分片 Adaptive：16 分片自适应
sal := async.Ratelimit(ctx).Sharded().Adaptive().Shards(16).MinWorker(5).MaxWorker(100).Build()
```

| 构建方法 | 说明 |
|----------|------|
| `.Shards(n int)` | 水平分片数（<=0 使用默认） |
| `.Rate(n)` | RateLimiter 的总速率按分片分配；TokenBucket 的速率应用于每个分片 |
| `.Per(d)` | 时间窗口 |
| `.Burst(n)` | 突发容量 |
| `.Limit(n)` | 窗口限制（SlidingWindow） |
| `.Window(d)` | 窗口大小（SlidingWindow） |
| `.Capacity(n float64)` | 令牌容量（TokenBucket，float64 类型） |
| `.MinWorker(n)` | 最小并发度（Adaptive） |
| `.MaxWorker(n)` | 最大并发度（Adaptive） |
| `.DefaultShards()` | 默认分片数 = `DefaultShardCount()` |
| `.Build()` | 构建对应分片实例 |

各分片类型提供对应普通类型的操作，获取或检查请求按 round-robin 分发到各分片。分片 TokenBucket 的 `Rate` 和 `Capacity` 均为每片的值，因此总速率和总容量分别为单片值乘以分片数；单次 `AllowN` 只检查选中的一个分片。分片 RateLimiter 和 SlidingWindow 的总配额按分片分配，目标值小于分片数时每片至少为 1。

```go
srl := async.Ratelimit(ctx).Sharded().RateLimiter().Shards(16).Rate(10000).Per(time.Second).Build()
defer srl.Close()

if err := srl.Wait(ctx); err != nil {
    return err
}
defer srl.Release() // 归还一个已成功获取的分片令牌
doRequest()
```

`Release` 归还任一尚未归还的分片令牌，不保证与当前调用者获取的分片相同。每次成功的 `Wait` 或 `Acquire` 必须恰好配对一次 `Release`；需要令牌与分片一一绑定时使用 `Token` 返回的句柄。

---

## 批量补充优化

`RateLimiter` 每 100ms 检查一次，根据实际经过的时间和配置速率批量补充令牌，并保留不足一个令牌的部分供下次累计。桶满时不保留超出容量的额度；低速率下，两次补充之间可能超过一秒。

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
| TokenBucket Allow 10M | **194万/s** | 历史测量值，实际结果取决于环境 |
| SlidingWindow Allow 10M | **159万/s** | 滑动窗口 |
| RateLimiter Acquire/Release 1M | **924万/s** | 1M ops/s |

---

## 默认值体系

### 全局默认值（core 包）

| 默认值 | 获取函数 | 修改函数 | 默认值 | 说明 |
|--------|----------|----------|--------|------|
| IO 并发度 | `core.IO()` | 不可修改 | **`runtime.NumCPU()×2`** | DefaultRate/DefaultLimit/DefaultMinWorker 使用 |
| 默认分片数 | `ratelimit.DefaultShardCount()`（内部） | 不可修改 | **`max(2, core.IO())`** | 分片构建器 DefaultShards 使用 |

### RateLimiter 构建器默认值

| `Default*` 方法 | 源码行为 | 默认值 |
|-----------------|----------|--------|
| `.DefaultRate()` | `b.rate = core.IO()` | `NumCPU×2`（操作/秒） |
| `.DefaultPer()` | `b.perDuration = time.Second` | 1 秒 |
| `.DefaultBurst()` | `b.burst = core.IO() * 2` | `NumCPU×4` |

### TokenBucket 构建器默认值

| `Default*` 方法 | 源码行为 | 默认值 |
|-----------------|----------|--------|
| `.DefaultRate()` | `b.rate = float64(core.IO())` | `NumCPU×2`（令牌/秒） |
| `.DefaultCapacity()` | `b.capacity = float64(core.IO() * 2)` | `NumCPU×4` |

### SlidingWindow 构建器默认值

| `Default*` 方法 | 源码行为 | 默认值 |
|-----------------|----------|--------|
| `.DefaultLimit()` | `b.limit = core.IO()` | `NumCPU×2`（请求/窗口） |
| `.DefaultWindow()` | `b.window = time.Second` | 1 秒 |

### Adaptive 构建器默认值

| `Default*` 方法 | 源码行为 | 默认值 |
|-----------------|----------|--------|
| `.DefaultMinWorker()` | `b.minRate = core.IO()` | `NumCPU×2` |
| `.DefaultMaxWorker()` | `b.maxRate = core.IO() * 10` | `NumCPU×20` |

### 分片构建器默认值

| `Default*` 方法 | 源码行为 | 默认值 |
|-----------------|----------|--------|
| `.DefaultShards()` | `b.shards = ratelimit.DefaultShardCount()` | `max(2, core.IO())` |

未调用 `DefaultShards()` 且分片数为非正数时，底层构造函数也使用 `core.IO()`（`runtime.NumCPU() * 2`，至少 2）。

### 默认值覆盖优先级

```
Default* 方法（设置标准默认值）
    ↓ 被覆盖
其他链式配置方法（.Rate(n), .Per(d), .Burst(n) 等）
    ↓ 构建
Build() 创建实例
```

---

## 生产环境使用建议

### 限流器选型

| 场景 | 推荐 | 理由 |
|------|------|------|
| API 网关窗口内请求数限制 | **SlidingWindow** | 按窗口内请求数判断 |
| 轻量级速率检查 | **TokenBucket** | 无需 goroutine，开销最小 |
| 窗口期内精确限制 | **SlidingWindow** | 窗口级精确计数 |
| 依赖下游成功率自适应 | **AdaptiveRateLimiter** | 根据成功/失败率动态调整 |
| 高 QPS 需降低锁竞争 | **Sharded* 系列** | 分片后锁竞争降至 1/N |

### RateLimiter 生产推荐配置

```go
// 生产级 QPS 限流配置
rl := async.Ratelimit(ctx).RateLimiter().
    Rate(1000).                        // 目标 QPS: 1000
    Per(time.Second).                  // 时间窗口: 1s
    Burst(2000).                       // 突发容量 = 2×QPS（缓冲短期尖峰）
    Build()
defer rl.Close()

// 使用 Token defer 模式，防止令牌泄漏
token, err := rl.Token(ctx)
if err != nil {
    return err
}
defer token.Release()
doRequest()
```

### 突发容量 (Burst) 与速率 (Rate) 的关系

| QPS | 推荐 Burst | 理由 |
|-----|-----------|------|
| ≤100 | `Rate × 2` | 小流量允许适度突发 |
| 100~1000 | `Rate × 2` | 标准推荐 |
| >1000 | `Rate × 1.5` | 高流量严格限流 |

### 分片数选择

| 预期 QPS | 推荐分片数 | 理由 |
|----------|-----------|------|
| ≤1000 | 不分片（1） | 单实例足够 |
| 1000~10000 | 4~8 | 降低锁竞争 |
| >10000 | `GOMAXPROCS`~16 | 锁竞争降至最低 |

### RateLimiter 常见错误

- **❌ 忘记 `defer rl.Close()`**：内部有 ticker goroutine，忘记 Close 会泄漏
- **❌ Wait/Release 未配对**：获取令牌后忘记 Release，导致令牌耗尽
- **❌ 将 Burst 当 Rate 使用**：Burst 是瞬时突发，长期速率仍是 Rate/Per
- **❌ 高 QPS 用单实例**：QPS > 10000 建议使用 Sharded 系列分片限流
