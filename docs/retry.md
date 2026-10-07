# Retry（重试）文档

## 概述

`RetryChain[T]` 是泛型重试链式构建器，提供声明式重试 API。支持指数/线性退避，以及 4 种限流模式（RateLimiter / TokenBucket / SlidingWindow / Adaptive）。

**两种入口**：

| 入口 | 说明 | 终端方法 |
|------|------|----------|
| `async.Retry[T](ctx)` | 带返回值重试 | `.Execute(fn) / .Run(fn)` |
| `async.RetryVoid(ctx)` | 无返回值重试 | `.ExecuteVoid(fn) / .RunVoid(fn) / .Run(fn)` |

> **⚠️ 默认配置**
>
> 默认值：指数退避，最多重试 3 次（总共 4 次尝试），退避 100ms~30s，无限流。
>
> **⚠️ 重试次数含义**
>
> `MaxRetries(n)` 中的 n 是重试次数，总执行次数 = n + 1（一次初试 + n 次重试）。
>
> **⚠️ 4 种限流模式互斥**
>
> `.RateLimiter()` / `.TokenBucket()` / `.SlidingWindow()` / `.Adaptive()` 四种模式只能选择一种，后调用的会覆盖前者。不调用任何限流模式则无限流（纯退避重试）。
>
> **⚠️ 限流在重试之间生效**
>
> 限流是在每次重试前检查的（不是限制用户调用 API 的频率）。例如 `RateLimiter().Rate(10).Per(time.Second)` 表示两次重试尝试之间至少间隔 1/10 秒。

---

## 目录

- [RetryChain 链式方法速查表](#retrychain-链式方法速查表)
- [终端方法速查](#终端方法速查)
- [基础退避重试](#基础退避重试)
- [模式一：RateLimiter（令牌补充限流）](#模式一ratelimiter令牌补充限流)
- [模式二：TokenBucket（经典令牌桶）](#模式二tokenbucket经典令牌桶)
- [模式三：SlidingWindow（滑动窗口）](#模式三slidingwindow滑动窗口)
- [模式四：Adaptive（自适应并发限流）](#模式四adaptive自适应并发限流)
- [无返回值重试（RetryVoid）](#无返回值重试retryvoid)
- [每次调用超时](#每次调用超时)
- [Run / RunVoid 简化终端](#run--runvoid-简化终端)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)
- [自定义日志](#自定义日志)

---

## RetryChain 链式方法速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.Retry[T](ctx)` | 创建带返回值重试链式构建器 |
| `async.RetryVoid(ctx)` | 创建无返回值重试链式构建器 |

### 策略配置方法

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Exponential()` | 指数退避（backoff × 2^attempt） | 默认 |
| `.Linear()` | 线性退避（固定间隔） | — |
| `.MaxRetries(n)` | 最大重试次数（n < 0 时设为 0） | 3 |
| `.Backoff(initial, max)` | 退避时间（指数：首间隔+上限；线性：固定间隔，max 忽略） | 100ms, 30s |
| `.PerCallTimeout(d)` | 每次 fn 调用的超时（0=不限时，超时后继续重试） | 0 |
| `.Context(ctx)` | 链式设置上下文（自动注入 TraceID） | — |
| `.DefaultConfig()` | 重置所有配置为默认值 | — |
| `.DefaultMaxRetries()` | 恢复默认重试次数（3） | — |
| `.DefaultBackoff()` | 恢复默认退避（100ms~30s） | — |
| `.DefaultPerCallTimeout()` | 恢复默认超时（不限时） | — |
| `.DefaultRate()` | 恢复默认速率（10/s） | — |
| `.DefaultPer()` | 恢复默认时间窗口（1s） | — |
| `.DefaultBurst()` | 恢复默认突发容量（0=关闭） | — |
| `.DefaultCapacity()` | 恢复默认令牌桶容量（速率×2） | — |
| `.DefaultLimit()` | 恢复默认滑动窗口限制（100） | — |
| `.DefaultWindow()` | 恢复默认滑动窗口（1s） | — |
| `.DefaultMinWorker()` | 恢复默认最小并发度（1） | — |
| `.DefaultMaxWorker()` | 恢复默认最大并发度（Min×10） | — |
| `.DefaultShards()` | 恢复默认分片（不启用分片） | — |

### 限流模式选择（四选一，互斥）

| 方法 | 说明 |
|------|------|
| `.RateLimiter()` | 令牌补充限流器，阻塞等待令牌 |
| `.TokenBucket()` | 经典令牌桶，非阻塞检查 |
| `.SlidingWindow()` | 滑动窗口精确计数 |
| `.Adaptive()` | 自适应并发限流，根据成功率动态调整 |

### RateLimiter 模式配置

| 方法 | 说明 |
|------|------|
| `.Rate(n int)` | 每 Per 时间窗口内的操作次数 |
| `.Per(d time.Duration)` | 时间窗口大小 |
| `.Burst(n int)` | 突发容量 |
| `.Shards(n int)` | 水平分片数（n≤0 时自动使用默认值） |

### TokenBucket 模式配置

| 方法 | 说明 |
|------|------|
| `.Rate(n float64)` | 每秒令牌生成速率（float64 版） |
| `.Capacity(n float64)` | 最大令牌容量 |
| `.Shards(n int)` | 水平分片数 |

### SlidingWindow 模式配置

| 方法 | 说明 |
|------|------|
| `.Limit(n int)` | 窗口内最大请求数 |
| `.Window(d time.Duration)` | 时间窗口大小 |
| `.Shards(n int)` | 水平分片数 |

### Adaptive 模式配置

| 方法 | 说明 |
|------|------|
| `.MinWorker(n int)` | 最小并发度 |
| `.MaxWorker(n int)` | 最大并发度 |
| `.Shards(n int)` | 水平分片数 |

### 通用方法

| 方法 | 说明 |
|------|------|
| `.Logger(l)` | 注入自定义日志（全局生效） |
| `.DefaultLogger()` | 恢复默认日志 |

---

## 终端方法速查

| 方法 | 签名 | 返回值 | 说明 |
|------|------|--------|------|
| `.Execute(fn)` | `fn func(context.Context) (T, error)` | `(T, error)` | 带返回值执行重试 |
| `.ExecuteVoid(fn)` | `fn func(context.Context) error` | `error` | 无返回值执行重试 |
| `.Run(fn)` | `fn func() error` | `error` | 简化版（fn 无 context 参数） |
| `.RunVoid(fn)` | `fn func(context.Context) error` | `error` | 等同于 ExecuteVoid |

> **⚠️ `.Execute(fn)` vs `.Run(fn)`**
>
> - `.Execute(fn)`：fn 有 `func(context.Context) (T, error)` 签名，可获得 ctx（带 trace_id 的 ctx）
> - `.Run(fn)`：fn 是 `func() error`，更简洁，ctx 由链内部保持。无需 context 时推荐使用

---

## 基础退避重试

### 指数退避（默认）

```go
val, err := async.Retry[string](ctx).
    Exponential().
    MaxRetries(3).                           // 最多 3 次重试 = 共 4 次尝试
    Backoff(100*time.Millisecond, 5*time.Second).  // 100ms → 200ms → 400ms → 800ms (上限 5s)
    Execute(func(ctx context.Context) (string, error) {
        return httpGet(ctx, url)
    })
```

### 线性退避

```go
err := async.RetryVoid(ctx).
    Linear().
    MaxRetries(5).
    Backoff(1*time.Second, 0).   // 每次重试固定等 1s（max 被忽略）
    ExecuteVoid(func(ctx context.Context) error {
        return sendEmail(ctx, to, body)
    })
```

### 默认配置微调

```go
val, err := async.Retry[int](ctx).
    DefaultConfig().       // 重置到默认：指数、3次重试、100ms~30s
    MaxRetries(10).        // 覆盖：改为最多 10 次重试
    Execute(fn)
```

---

## 模式一：RateLimiter（令牌补充限流）

阻塞式限流：每次重试前阻塞等待令牌，被限流时阻塞而非跳过。

```go
val, err := async.Retry[string](ctx).
    Exponential().MaxRetries(5).
    Backoff(100*time.Millisecond, 10*time.Second).
    RateLimiter().          // 启用 RateLimiter
    Rate(10).               // 每秒 10 次
    Per(time.Second).
    Burst(50).              // 突发容量（允许瞬时 50 次）
    Shards(8).              // 水平分片降低锁竞争
    Execute(func(ctx context.Context) (string, error) {
        return callAPI(ctx)
    })
```

> **⚠️ RateLimiter 阻塞等待**
>
> `RateLimiter` 被限流时会阻塞等待令牌，不会跳过重试。适合需要保证每次请求都发出、但需要限速的场景。

---

## 模式二：TokenBucket（经典令牌桶）

非阻塞式限流：每次重试前检查令牌，拿不到时跳过本次重试等待退避后重试。

```go
err := async.RetryVoid(ctx).
    Exponential().MaxRetries(10).
    Backoff(100*time.Millisecond, 5*time.Second).
    TokenBucket().          // 启用 TokenBucket
    Rate(5).                // 每秒 5 个令牌（float64）
    Capacity(20).           // 最多积压 20 个令牌
    Shards(4).              // 水平分片
    ExecuteVoid(func(ctx context.Context) error {
        return sendMessage(ctx, msg)
    })
```

> **⚠️ TokenBucket 非阻塞**
>
> 令牌不足时不会阻塞等待，而是返回错误并进入退避重试循环。适合不想阻塞等待令牌的场景。
>
> **⚠️ `.Rate()` vs `.Rate()`**
>
> `.Rate(n int)` 用于 RateLimiter 和 SlidingWindow 模式。`.Rate(n float64)` 的 float64 签名用于 TokenBucket 模式（因为 TokenBucket 支持小数速率如 Rate(0.5) = 每 2 秒 1 个令牌）。方法名相同但参数类型不同决定了用于哪个模式。

---

## 模式三：SlidingWindow（滑动窗口）

精确计数式限流：在滑动时间窗口内精确限制请求数。比固定窗口更平滑，无边界突刺问题。

```go
val, err := async.Retry[string](ctx).
    Linear().MaxRetries(5).
    Backoff(200*time.Millisecond, 0).
    SlidingWindow().        // 启用滑动窗口
    Limit(100).             // 每窗口最多 100 次
    Window(10*time.Second). // 10 秒滑动窗口
    Shards(8).              // 水平分片
    Execute(func(ctx context.Context) (string, error) {
        return fetchData(ctx, id)
    })
```

> **⚠️ SlidingWindow vs RateLimiter**
>
> - RateLimiter：阻塞等待令牌，保证请求均匀分布
> - SlidingWindow：非阻塞精确计数，超限时进入退避重试

---

## 模式四：Adaptive（自适应并发限流）

根据成功率动态调整并发度。成功率高时自动扩容（增加并发），失败率高时自动缩容（减少并发）。

```go
err := async.RetryVoid(ctx).
    Exponential().MaxRetries(10).
    Backoff(50*time.Millisecond, 5*time.Second).
    Adaptive().             // 启用自适应限流
    MinWorker(5).           // 最小并发度 5
    MaxWorker(100).         // 最大并发度 100
    Shards(16).             // 水平分片
    ExecuteVoid(func(ctx context.Context) error {
        return callUnstableService(ctx)
    })
```

> **⚠️ Adaptive 动态并发**
>
> 每次重试前 `Acquire` 并发槽位，执行后 `Release`。并发度在 MinWorker~MaxWorker 之间根据成功率自适应变化。适合下游服务容量不稳定的场景。

---

## 无返回值重试（RetryVoid）

`RetryVoid(ctx)` 等价于 `Retry[struct{}](ctx)`，提供更简洁的无返回值重试语义：

```go
err := async.RetryVoid(ctx).
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    ExecuteVoid(func(ctx context.Context) error {
        return db.Write(ctx, record)
    })
```

对比带返回值版：

```go
val, err := async.Retry[int](ctx).
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    Execute(func(ctx context.Context) (int, error) {
        return compute(ctx)
    })
```

---

## 每次调用超时

`PerCallTimeout(d)` 设置每次 fn 调用的超时。超时后不会终止重试循环，而是进入退避后下一次重试：

```go
val, err := async.Retry[string](ctx).
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    PerCallTimeout(2 * time.Second).   // 每次调用最多 2 秒
    Execute(func(ctx context.Context) (string, error) {
        return slowRPC(ctx, req)       // 超过 2s 自动取消本次调用，进入重试
    })
```

> **⚠️ PerCallTimeout 超时 ≠ 取消**
>
> `PerCallTimeout` 超时不会终止整个重试循环，只是单次调用超时。超时后仍会按退避策略进入下一次重试。要完全终止可使用 `context.WithTimeout` 包装外部 ctx。

---

## Run / RunVoid 简化终端

`Run(fn)` 是简化版终端，fn 不需要接收 context 参数：

```go
// Execute：需要 context 参数
err := async.RetryVoid(ctx).
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    ExecuteVoid(func(ctx context.Context) error {
        return doSomething(ctx)   // fn 有 ctx 参数
    })

// Run：不需要 context 参数，更简洁
err := async.RetryVoid(ctx).
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    Run(func() error {
        return doSomething()      // fn 无 ctx 参数
    })

// RunVoid：等价于 ExecuteVoid，fn 有 ctx 参数
err := async.RetryVoid(ctx).
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    RunVoid(func(ctx context.Context) error {
        return doSomething(ctx)
    })
```

> **⚠️ Execute / ExecuteVoid / Run / RunVoid 区别**
>
> | 方法 | fn 签名 | 返回值 | 说明 |
> |------|------|--------|------|
> | `Execute(fn)` | `func(context.Context) (T, error)` | `(T, error)` | 带返回值 |
> | `ExecuteVoid(fn)` | `func(context.Context) error` | `error` | 无返回值 |
> | `Run(fn)` | `func() error` | `error` | 简化无 ctx 无返回值 |
> | `RunVoid(fn)` | `func(context.Context) error` | `error` | 同 ExecuteVoid |
>
> `.Execute(fn)` 只能用于 `Retry[T](ctx)`（T ≠ struct{}）。`.ExecuteVoid(fn)` / `.Run(fn)` / `.RunVoid(fn)` 只能用于 `RetryVoid(ctx)`（或 `Retry[struct{}](ctx)`）。

---

## 默认值体系

### 重试策略默认值

| 默认值 | 获取/恢复方法 | 默认值 | 说明 |
|--------|-------------|--------|------|
| 默认退避策略 | `.Exponential()` | **指数退避** | 首次创建时就是指数退避 |
| 默认重试次数 | `.DefaultMaxRetries()` | **3 次** | 总执行次数 = 4（1 次初试 + 3 次重试） |
| 默认退避时间 | `.DefaultBackoff()` | **100ms ~ 30s** | 指数模式：首间隔+上限；线性模式：固定间隔 |
| 默认单次超时 | `.DefaultPerCallTimeout()` | **不限时**（0） | 超时后继续重试 |
| 默认完整配置 | `.DefaultConfig()` | — | 重置所有配置为默认值 |

### 限流默认值（四选一模式）

| 模式 | `Default*` 方法 | 默认值 | 说明 |
|------|----------------|--------|------|
| RateLimiter | `.DefaultRate()` | **10/秒** | 每 Per 内操作数 |
| RateLimiter | `.DefaultPer()` | **1 秒** | 时间窗口 |
| RateLimiter | `.DefaultBurst()` | **0**（不开启突发） | 突发容量 |
| TokenBucket | `.DefaultRate()` | **10/秒** | float64 签名 |
| TokenBucket | `.DefaultCapacity()` | **速率×2** | 令牌桶容量 |
| SlidingWindow | `.DefaultLimit()` | **100** | 每窗口请求数 |
| SlidingWindow | `.DefaultWindow()` | **1 秒** | 窗口大小 |
| Adaptive | `.DefaultMinWorker()` | **1** | 最小并发度 |
| Adaptive | `.DefaultMaxWorker()` | **Min×10** | 最大并发度 |
| 所有限流模式 | `.DefaultShards()` | **不启用分片** | 分片数=0 |

### 默认值覆盖优先级

```
Default* 方法（设置标准默认值）
    ↓ 被覆盖
显式设置（.MaxRetries(n), .Backoff(...), .Rate(n) 等）
    ↓ 构建
Execute / ExecuteVoid / Run / RunVoid 执行
```

---

## 生产环境使用建议

### 重试策略选型

| 场景 | 推荐策略 | 理由 |
|------|---------|------|
| 依赖服务偶发不可用（502/503） | 指数退避 | 减少对下游的压力 |
| 定时轮询间隔固定 | 线性退避 | 间隔均匀 |
| 限流器 429 响应 | RateLimiter + 指数退避 | 限流后等待更久再试 |
| 调用下游有 QPS 配额 | TokenBucket | 精确控制频率 |
| 下游容量不稳定 | Adaptive | 根据成功率自动调整 |

### 退避时间建议

| 下游恢复时间 | 推荐 Backoff(min, max) | 理由 |
|-------------|----------------------|------|
| 快速恢复（<1s） | `(100ms, 3s)` | 短间隔快速重试 |
| 中等恢复（1~10s） | `(500ms, 15s)` | 给下游留足恢复时间 |
| 慢速恢复（>10s） | `(1s, 60s)` | 长间隔避免雪崩 |

### 重试次数建议

| 场景 | 推荐 MaxRetries | 理由 |
|------|----------------|------|
| 关键数据写入 | **5~10** | 宁可多试几次 |
| API 查询（幂等） | **2~3** | 幂等操作可放心重试 |
| 非关键通知 | **1~2** | 失败可丢弃 |
| 限流场景 | **10+** | 配合限流器等待令牌 |

### 限流 + 重试组合推荐

```go
// 推荐配置：指数退避 + RateLimiter 限流
val, err := async.Retry[string](ctx).
    Exponential().MaxRetries(5).
    Backoff(200*time.Millisecond, 10*time.Second).
    PerCallTimeout(3 * time.Second).    // 单次调用超时防止卡死
    RateLimiter().
    Rate(10).Per(time.Second).          // 每秒最多 10 次重试
    Burst(20).                          // 允许短暂突发 20 次
    Execute(func(ctx context.Context) (string, error) {
        return callExternalAPI(ctx)
    })
```

### 生产推荐配置

```go
// ========== 标准 RPC 重试：指数退避 + 超时 ==========
// 适用场景：下游偶发 502/503，幂等查询
data, err := async.Retry[*Data](ctx).
    Exponential().
    MaxRetries(3).                                      // 初试 + 3 次重试 = 共 4 次
    Backoff(200*time.Millisecond, 10*time.Second).      // 200ms→400ms→800ms→1600ms
    PerCallTimeout(5 * time.Second).                    // ⚠️ 单次调用兜底，防止 hang
    Execute(func(ctx context.Context) (*Data, error) {
        return rpcClient.Query(ctx, req)
    })
// 总最长耗时 ≈ (10s + 5s) × 4 = 60s

// ========== 限流下游重试：指数退避 + RateLimiter ==========
// 适用场景：下游有 QPS 限制，需限速重试
data, err := async.Retry[*Data](ctx).
    Exponential().
    MaxRetries(10).                                     // 限流场景多试几次
    Backoff(500*time.Millisecond, 30*time.Second).      // 长间隔避让
    PerCallTimeout(10 * time.Second).                   // 单次超时
    RateLimiter().
    Rate(10).Per(time.Second).Shards(4).               // 每秒最多 10 次重试
    Burst(20).                                          // 允许瞬时突发 20 次
    Execute(func(ctx context.Context) (*Data, error) {
        return rateLimitedAPI.Query(ctx, req)
    })

// ========== 自适应限流重试：动态调整并发 ==========
// 适用场景：下游容量不稳定，需根据成功率自适应
data, err := async.Retry[*Data](ctx).
    Exponential().MaxRetries(5).
    Backoff(100*time.Millisecond, 15*time.Second).
    PerCallTimeout(3 * time.Second).
    Adaptive().
    MinWorker(2).MaxWorker(50).                         // 并发度 2~50 自动调节
    Execute(func(ctx context.Context) (*Data, error) {
        return unstableSvc.Query(ctx, req)
    })

// ========== 关键写入重试：最大努力保证 ==========
// ⚠️ 确认下游支持幂等，否则勿用
err := async.RetryVoid(ctx).
    Exponential().MaxRetries(10).
    Backoff(1*time.Second, 60*time.Second).             // 最长等 60s
    PerCallTimeout(30 * time.Second).
    ExecuteVoid(func(ctx context.Context) error {
        return db.WriteWithIdempotentKey(ctx, key, data)
    })
```

### 常见错误

- **❌ 不设 PerCallTimeout**：下游 hang 住时重试循环卡死，建议总超时 = `(BackoffMax + PerCallTimeout) × (MaxRetries + 1)`
- **❌ 重试非幂等操作**：Write/Delete 类操作重试前确认下游支持幂等
- **❌ 指数退避上限过小**：如果 max 小于 `initial × 2^retries`，后续间隔时间相同，退避效果不明显
- **❌ RateLimiter 和 TokenBucket 混淆**：前者阻塞等令牌，后者非阻塞跳过，根据场景选择
- **❌ 忘记使用 DefaultConfig() 清理**：链式构建器可能继承之前调用的残留状态，使用前先 `.DefaultConfig()`

---

## 自定义日志

```go
val, err := async.Retry[string](ctx).
    Logger(myLogger).     // 全局生效
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    Execute(fn)

err := async.RetryVoid(ctx).
    Logger(myLogger).
    DefaultLogger().      // 全局恢复默认
    Run(func() error { return doSomething() })
```