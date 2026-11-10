# 限流器（RateLimiter）文档

## 概述

async 提供四种限流器，覆盖不同场景。所有限流器通过 `async.Ratelimit(ctx)` 链式构建器创建：

| 限流器 | 算法 | 适用场景 |
|--------|------|----------|
| `RateLimiter` | 令牌桶（定时补充，可手动归还） | 令牌获取与归还 |
| `TokenBucket` | 经典令牌桶 | 简单速率限制 |
| `SlidingWindow` | 滑动窗口 | 精确窗口限制 |
| `AdaptiveRateLimiter` | 自适应 | 动态负载调整 |

> 单个 `RateLimiter` 的 `Resize`、`Acquire` 和 `Release` 支持并发调用。分片 `ShardedRateLimiter.Resize` 需在没有活跃获取和未归还许可时调用，详见下文；不能把单实例的并发保证推广到分片批量调整。
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
  - [Acquire / Release / RecordOk / RecordFailure](#acquire-1)
- [分片限流器](#分片限流器)
  - [构建：Ratelimit().Sharded()](#构建-sharded)
  - [ShardedRateLimiter / ShardedTokenBucket / ShardedSlidingWindow / ShardedAdaptive](#分片限流器)
- [批量补充优化](#批量补充优化)
- [架构说明](#架构说明)
- [性能基准](#性能基准)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)

## 链式构建 API 速查表

| 入口 | 说明 |
|------|------|
| `async.Ratelimit(ctx)` | 创建限流器链式构建器 |

入口及其各模式构建器提供 `Context(ctx)`、`Logger(l)` 和 `DefaultLogger()`：`Context` 保存经 trace ID 处理的上下文并返回当前构建器，但当前各模式的 `Build()` 不将该上下文传给所创建的限流器；阻塞获取时仍须向 `Wait(ctx)`、`Acquire(ctx)` 或 `Token(ctx)` 传入用于取消等待的 context。`Logger(l)` 通过 `async.Logger` 接口设置全局日志器，`DefaultLogger()` 恢复默认日志器；两者影响全库，而非仅影响当前实例。建议在应用初始化时统一配置，避免不同组件相互覆盖。

```go
builder := async.Ratelimit(ctx).Logger(myLogger)
rl := builder.RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()
if err := rl.Acquire(ctx); err != nil {
    return err
}
defer rl.Release()
// 如需恢复全局默认日志器：builder.DefaultLogger()
```

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
| `.Sharded().SlidingWindow().Shards(n).Limit(n).Window(d).Build()` | `*ShardedSlidingWindowRateLimiter` | 8 分片滑动窗口 |
| `.Sharded().Adaptive().Shards(n).MinWorker(n).MaxWorker(n).Build()` | `*ShardedAdaptiveRateLimiter` | 16 分片自适应 |

---

## RateLimiter（批量补充令牌桶）

当前实现每 **100ms** 根据累计经过时间批量补充令牌；具体 CPU 开销和等待延迟取决于速率、突发容量与负载，应在目标环境测量。

### 构建 RateLimiter

所有限流器均通过 `async.Ratelimit(ctx)` 链式构建：

```go
// 基础：每秒 100 个令牌
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

// 带突发容量
burstLimiter := async.Ratelimit(ctx).RateLimiter().Rate(50).Per(time.Second).Burst(200).Build()
defer burstLimiter.Close()

// 使用默认值
defaultLimiter := async.Ratelimit(ctx).RateLimiter().DefaultRate().DefaultPer().Build()
defer defaultLimiter.Close()
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

以下三种模式均位于返回 `error` 的业务函数中，`doRequest` 的签名为 `func(context.Context) error`。参数 `ctx` 为非 nil 请求 context，`rl` 为调用方持有的限流器；获取错误和业务错误均返回给上层。生产中复用实例，并在成功获取后注册一次释放，业务须响应传入的取消和期限。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

if err := rl.Wait(ctx); err != nil {
    return err
}
defer rl.Release()
return doRequest(ctx)
```

#### 模式二：Token defer（最安全）

```go
token, err := rl.Token(ctx)
if err != nil {
    return err
}
defer token.Release()
return doRequest(ctx)
```

同一个 `*Token` 的 `Release` 可以重复或并发调用，只会归还一次令牌。不要复制 `Token` 的值；需要共享时传递指针。

#### 模式三：Acquire + 策略切换

```go
rl.WithStrategy(async.Reject) // 满时拒绝
if err := rl.Acquire(ctx); err != nil {
    return err
}
defer rl.Release()
return doRequest(ctx)
```

### 完整方法列表

| 方法 | 语法 | 说明 |
|------|------|------|
| `Wait` | `func (rl *RateLimiter) Wait(ctx context.Context) error` | 与 `Acquire` 相同，按当前策略获取令牌 |
| `Token` | `func (rl *RateLimiter) Token(ctx context.Context) (*Token, error)` | 获取令牌包装器（配合 defer） |
| `Acquire` | `func (rl *RateLimiter) Acquire(ctx context.Context) error` | 按当前策略获取令牌 |
| `Release` | `func (rl *RateLimiter) Release()` | 释放一个令牌（归还） |
| `WithStrategy` | `func (rl *RateLimiter) WithStrategy(s Strategy) *RateLimiter` | 切换策略 |
| `WithTraceID` | `func (rl *RateLimiter) WithTraceID(ctx context.Context) (*RateLimiter, context.Context)` | 保存确保 TraceID 后的日志 context，返回原实例及该 context；获取时另传所需 context |
| `Resize` | `func (rl *RateLimiter) Resize(newRate int)` | 调整速率 |
| `Size` | `func (rl *RateLimiter) Size() int` | 当前配置速率（不等同突发容量或可用令牌数） |
| `Available` | `func (rl *RateLimiter) Available() int` | 可用令牌数 |
| `Stop` | `func (rl *RateLimiter) Stop()` | Close 的别名，行为完全一致 |
| `Close` | `func (rl *RateLimiter) Close()` | 停止补充并释放所有资源 |

表内 `Token` 在根包公开为 `async.Token`。通过 `tok, err := rl.Token(ctx)` 接收；先检查 `err`，成功后 `defer tok.Release()`，不要复制包含 `sync.Once` 的句柄，也不要再调用 `rl.Release()` 重复归还。

### Wait

按当前策略获取一个令牌，默认策略会等待令牌或上下文取消。`Wait` 等同于 `Acquire`，因此也会受 `WithStrategy` 影响；归还令牌可能让后续请求立即通过。

参数 `ctx` 必须非 nil。`Block` 优先获取已有令牌，即使 `ctx` 已取消也可能成功；仅在没有令牌时响应取消。`Reject` 与 `BlockForce` 忽略 context 取消，前者无令牌立即返回限流错误，后者等待归还、补充或关闭。`Acquire` 和 `Token` 遵循相同规则。生产业务应接收并响应传入的 context，不能把取得许可当成业务成功；若要求开始前拒绝已取消请求，另在业务入口检查 `ctx.Err()`，该检查仍不能阻止随后发生的取消。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Second).Build()
defer rl.Close()

var wg sync.WaitGroup
businessErrors := make([]error, 100)
var waitErr error
for i := 0; i < 100; i++ {
    if err := rl.Wait(ctx); err != nil {
        waitErr = err
        break
    }
    wg.Add(1)
    go func(index int) {
        defer wg.Done()
        defer rl.Release()
        businessErrors[index] = doRequest(ctx)
    }(i)
}
wg.Wait()
return errors.Join(waitErr, errors.Join(businessErrors...))
```

示例需导入 `sync`、`errors`、`time`、`context` 和 async，所在函数返回 `error`，业务签名为 `doRequest(context.Context) error`。每个成功获取的请求使用独立索引登记业务错误，等待全部已启动业务结束后合并等待错误与业务错误；等待失败只停止后续启动，不丢弃已接受业务的结果。生产中根据请求量设置错误存储预算，并由业务及时响应 context；这里未设置遇业务错误即取消其余任务。计时补充仍可能在已有业务运行时提供新令牌，不能把 `Rate(10)` 解释为严格同时最多十个任务。

### Acquire

按当前策略获取令牌。策略由 `WithStrategy` 设置，默认 `Block`（阻塞等待）。

| 策略 | 行为 |
|------|------|
| `Block`（默认） | 优先获取已有令牌；无令牌时等待空位或 context 取消 |
| `Reject` | 满时立即返回 ErrRateLimitExceeded |
| `BlockForce` | 强制阻塞，忽略 context 取消（慎用） |

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(5).Per(time.Second).Build()
defer rl.Close()

// 阻塞等待模式（默认）
if err := func() error {
    if err := rl.Acquire(ctx); err != nil {
        return err
    }
    defer rl.Release()
    return doRequest(ctx)
}(); err != nil {
    return err
}

// 拒绝模式：满时立即返回错误
return func() error {
    if err := rl.WithStrategy(async.Reject).Acquire(ctx); err != nil {
        // 当前根包没有导出专用的额度耗尽错误常量；保留原始错误供调用方记录。
        return fmt.Errorf("请求未获准: %w", err)
    }
    defer rl.Release()
    return doRequest(ctx)
}()
```

参数 `ctx` 为非 nil 请求 context，业务签名为 `doRequest(context.Context) error`，所在函数返回 `error`。示例串行演示两个策略，每次业务结束即归还对应许可；首段失败会直接返回，成功才继续第二段。生产中通常按业务选用一种策略；调用 `WithStrategy` 会改变共享实例后续获取采用的策略，不是仅为当前请求指定策略。业务错误原样返回，第二段获取错误使用 `%w` 保留错误链；所需导入为 `context`、`fmt`、`time` 和 async。

### Release

归还一个令牌，释放一个槽位供后续请求使用。`Wait` 或 `Acquire` 成功后使用限流器的 `Release`；`Token` 成功后则使用返回的 `token.Release()`，不要将两种释放方式混用。

```go
if err := rl.Wait(ctx); err != nil {
    return err
}
defer rl.Release() // 确保归还
return doRequest(ctx)
```

### Token

获取令牌包装器，配合 `defer token.Release()` 实现最安全的使用模式。令牌在函数返回时自动释放，避免因提前 return 导致令牌泄漏。

下面位于返回 `error` 的函数中，参数 `ctx` 为非 nil 请求 context，`req` 为业务请求；`callExternalAPI(context.Context, Request) (*Response, error)` 与 `processResponse(context.Context, *Response) error` 均由应用实现，Request/Response为应用类型。取得许可后先执行RPC，仅在RPC成功时将同一个响应传入处理函数；任一步业务失败原样返回错误。

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
return processResponse(ctx, resp)
```

生产建议：复用服务级限流实例，最外层在停止接收请求并等待业务收尾后统一Close；上例展示完整资源作用域。Token从获取成功到响应处理结束一直持有，业务错误、取消、panic或Goexit均执行释放defer。两步业务都应响应传入context；RPC返回nil错误后若父context取消，响应处理仍会被调用并须自行检查取消。不要在两步之间再次获取同一许可，也不要把业务成功改写为之后的父取消错误；本示例按最后实际业务返回值决定结果。Token不可复制，同一个指针重复Release只归还一次，不与rl.Release混用；定时补充期间即使Token仍持有也可能提供新额度，不能把它当严格并发上限。

### WithStrategy

切换获取令牌的策略，返回 `*RateLimiter` 以支持链式调用。

| 策略常量 | 值 | 说明 |
|----------|-----|------|
| `async.Block` | 0 | 优先获取已有令牌（默认），无令牌时响应 context 取消 |
| `async.Reject` | 1 | 满时立即拒绝，返回 `ErrRateLimitExceeded` |
| `async.BlockForce` | 2 | 强制阻塞，忽略 context 取消（慎用） |

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

// 高峰期：拒绝策略
if isPeakHour() {
    rl.WithStrategy(async.Reject)
}

if err := func() error {
    if err := rl.Acquire(ctx); err != nil {
        return err
    }
    defer rl.Release()
    return doRequest(ctx)
}(); err != nil {
    return err
}

// 链式调用
token, err := rl.WithStrategy(async.Block).Token(ctx)
if err != nil {
    return err
}
defer token.Release()
```

该段位于返回 `error` 的函数中，`ctx` 非 nil，`isPeakHour() bool` 由应用提供，业务签名为 `doRequest(context.Context) error`。第一段业务结束并归还许可后才演示 Token 链式获取，Token 段没有再次调用业务；外围函数可在该段后继续使用许可，结束时返回 nil。第一段失败立即返回原错误。生产中通常选择一种获取方式，并在共享实例上预先配置策略；策略切换不会重新配置已经开始等待的获取调用。

### WithTraceID

参数 `ctx` 须非 nil。本方法调用 `EnsureTraceID`：已有有效 TraceID 时保留原 context，缺失时派生带 TraceID 的 context，并将它保存为限流器的日志 context。按 `(原限流器指针, 处理后的 context)` 返回；不会创建新的限流实例，也不会补充或消耗令牌。

生产环境在共享实例前配置追踪。每次 `Wait`/`Acquire` 仍须另传控制本次等待的 context；保存的日志 context 不会自动成为这些方法的取消来源。TraceID 注入全局开关和日志后端的边界见 [配置文档](config.md)。

```go
rl, tracedCtx := rl.WithTraceID(ctx)
if err := rl.Wait(tracedCtx); err != nil {
    return err
}
defer rl.Release()
return doRequest(tracedCtx)
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

返回当前配置的补充额度，即每个 `Per` 窗口的 `rate`；`Resize` 生效后随之更新。它不表示突发容量或当前可用令牌数：例如 `Rate(50).Burst(200)` 的 `Size()` 为 50，初始 `Available()` 为 200。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()
fmt.Println(rl.Size()) // 100
```

### Available

返回当前可用的令牌数，用于监控和告警。

`Available()` 无参数，返回调用瞬间的可用额度快照，不预留令牌；不能先检查它再假定随后 Acquire 一定成功。下例放在持有限流实例的业务作用域中，`ctx` 非 nil，需要导入 `context`、`log`、`time` 和 async。每5秒读取一次，低于10时输出日志，`Size()` 是当前每个时间窗口的配置速率。

```go
rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()
defer rl.Close()

monitorCtx, stopMonitor := context.WithCancel(ctx)
monitorDone := make(chan struct{})
defer func() {
    stopMonitor()
    <-monitorDone
}()
go func() {
    defer close(monitorDone)
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-monitorCtx.Done():
            return
        case <-ticker.C:
            if avail := rl.Available(); avail < 10 {
                log.Printf("限流器可用令牌过低: %d（配置速率=%d/窗口）", avail, rl.Size())
            }
        }
    }
}()
```

生产中在该作用域内执行实际业务并复用 `rl`，按监控成本调整5秒周期和10个令牌的告警阈值；仅创建监控然后立即返回时，defer 会立即停止它。注意 defer 后进先出：先停止并等待监控 goroutine，再关闭限流实例；父 context 取消也会请求监控退出。取消与 tick 同时就绪时仍可能多读一次，日志输出须及时返回，不能依靠 context 强制中断阻塞的 Logger。

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

新构建器的速率和容量均为 0，`Build()` 原样使用它们；若不显式配置，正数令牌请求不会通过。`DefaultRate()` 和 `DefaultCapacity()` 只有调用时才写入表中的值。`AllowN(n)` 在 `n <= 0` 时直接返回 true；建议配置有限、正数的速率和容量。

### Allow

消耗 1 个令牌。返回 true 表示允许通过。

下面的业务函数返回 `error`，参数 `ctx` 非 nil，`tb` 为上文创建并跨请求复用的实例，业务签名为 `doRequest(context.Context) error`。`errRateLimited` 由应用预先定义，例如 `var errRateLimited = errors.New("请求额度不足")`，不是 async 根包导出的常量。成功获取额度才调用业务，并原样返回业务错误；拒绝时返回应用错误。

```go
if !tb.Allow() {
    return errRateLimited
}
return doRequest(ctx)
```

生产中保持实例生命周期与限流范围一致，每请求新建实例会重置满桶。`Allow`/`AllowN` 没有 context 参数，不检查父取消；额度通过后业务仍应检查并响应 `ctx`。若业务失败、取消、panic 或 Goexit，已消费额度不会回退；没有 Release 或 Close。若要求开始前拒绝已取消业务，在应用入口另行检查 `ctx.Err()`，该检查不能阻止随后发生的取消。

### AllowN

尝试一次消耗 `n` 个令牌，有限正数请求按整笔额度接受或拒绝，支持小数。每次正数检查先按距该桶上次正数检查的实际时间补充，再判断额度；拒绝不会扣除请求量，但会保留这次补充并更新补充时间。`n<=0` 直接成功，不补充、不扣除，也不更新该桶时间。

容量同时决定初始额度与最大余额，空闲时超出容量的积累会丢弃，不能留给下一轮突发。若容量小于 1，`Allow()` 即使长时间空闲也不能成功，但不超过容量的 `AllowN` 小数请求仍可通过；单笔请求大于容量时也不会靠等待积累到足够额度。生产中应先明确每个请求的计量单位，使最大单笔额度不超过单片容量；有小数计算时需考虑浮点舍入，不用余额推导精确十进制账目。可执行对照见 [Example_tokenBucketFractionalCapacity](tests/public_examples_test.go)。

```go
if !tb.AllowN(10) {
    return errRateLimited
}
return batchProcess(ctx)
```

该段位于返回 `error` 的函数中，参数 `ctx`、`tb` 和应用拒绝错误沿用上例，`batchProcess(context.Context) error` 由应用实现。这里一次消费10个令牌，仅调用一次批量业务；批量大小和令牌计量关系由应用保证。额度不足时整笔拒绝且不调用业务，业务失败不退回额度。生产中确保容量能容纳单笔申请，不要把重试业务当作免费重试。

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

新构建器的限额和窗口均为 0，`Build()` 不自动调用 `DefaultLimit()` 或 `DefaultWindow()`；零限额会拒绝正数请求，零窗口也不是有效的生产限流窗口。显式设置正数限额和窗口。`AllowN(n)` 的 `n <= 0` 时直接返回 true。

普通/分片构造器和构建器都原样保存窗口，不自动拒绝或修正非正值。窗口 `<=0`（包括最小 `time.Duration`）时，每次正数请求检查都会使此前记录过期；单次 `AllowN(n)` 仍须不超过该实例或所选分片的限额，拒绝不会记录请求。正数窗口使用 `(当前时间-window, 当前时间]` 的记录，恰好到窗口边界的旧记录会过期。生产配置应在入口校验正数窗口和限额，避免把无跨调用累计效果的非正窗口当成限流保障。期限只控制历史计数，不限制业务执行时长；需要取消或并发许可时另外设置业务 context 和并发控制。
正数和非正窗口的带输出对照见 `Example_slidingWindowDurationBoundary`。

### Allow

检查是否允许 1 次请求。

下面两段位于返回 `error` 的函数中，参数 `ctx` 非 nil，`sw` 为上文创建并跨请求复用的滑动窗口实例。业务分别为 `doRequest(context.Context) error` 和 `doBatchRequest(context.Context) error`；应用预先定义 `errRateLimited`（例如使用 `errors.New`），该错误不是 async 导出常量。拒绝时返回此错误且不调用业务，接受后原样返回业务错误。

```go
if !sw.Allow() {
    return errRateLimited
}
return doRequest(ctx)
```

生产中复用实例以保留窗口历史。Allow/AllowN只判断并记录额度，没有context参数，不等待额度、不取消业务，也不限制业务同时运行数量。业务失败、取消、panic或Goexit均不会撤销已记录请求；没有Release或Close。业务必须响应传入context，需要开始前拒绝已取消请求时在应用入口检查ctx.Err()。

### AllowN

检查是否允许 N 次请求。

```go
if !sw.AllowN(5) {
    return errRateLimited
}
return doBatchRequest(ctx)
```

`n=5` 表示一次占用5份窗口计数；该段只调用一次批量业务，批量内错误和执行数量由应用处理。参数、返回及context约定沿用Allow示例。生产中让单笔请求不超过实例限额，接受后业务失败不退回计数；非正n虽可通过，也不代表业务输入有效，应由应用先校验。

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

新构建器未设置最小和最大值时，两字段均为 0；`Build()` 会将最小值修正为 1，并将最大值修正为不小于最小值，因此实际范围为 1 到 1。`DefaultMinWorker()` 和 `DefaultMaxWorker()` 是显式设置 IO 默认值的方法，不会在创建时自动调用。普通 Adaptive 的底层使用无补充 goroutine 的简化限流器，公开类型没有 `Close` 方法。

初始容量取归一化上下限的整数中点，以 `min+(max-min)/2` 计算，避免 32 位下两个合法正 `int32` 边界相加溢出。比如下限 1、上限 2,147,483,647 时，中点为 1,073,741,824；这是数值边界示例，生产应根据下游承载能力选择较小的有限容量。构造时会逐个填充许可，大容量会增加初始化耗时；当前容量字段使用 `int32`，上下限仍应保持在正 `int32` 范围并符合资源预算。分片入口对各片分配后的边界使用相同计算。

### Acquire

获取执行槽位。普通及分片 Adaptive 都固定使用 `Block`，没有公开的 `WithStrategy` 方法；有空位时，即使 ctx 已取消也可能成功，无空位时等待归还或 ctx 取消。分片入口只等待轮转选中的一片，不搜索其他片的空闲槽位；分片关闭也会唤醒等待。使用非 nil context，并且仅在获取成功后配对释放。

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

err := doRequest(ctx) // 业务签名：func(context.Context) error
if err != nil {
    al.RecordFailure()  // 失败 → 可能自动缩容
    return err
}
al.RecordOk()  // 成功 → 可能自动扩容
return nil
```

示例位于返回 `error` 的业务函数中，参数 `ctx` 为非 nil 请求 context，`al` 为跨请求复用的限流器，`doRequest(ctx)` 必须及时响应取消并返回业务错误。获取失败时直接返回获取错误；获取成功后仅记录一次正常返回的业务结果，并原样返回业务错误。业务取消或超时返回非 nil 错误时计入失败；业务已成功返回 nil 后发生的父取消不改写该次成功。`defer al.Release()` 在正常返回、panic 和 `runtime.Goexit` 时均归还许可，但记录代码不会在 panic/Goexit 后执行；应用若需要统计这些异常，应在业务边界另行处理，不能把它们默认为已记录失败。

分片自适应限流器使用相同的 RecordOk() / RecordFailure() 接口；记录方法使用独立的 round-robin 计数器，记录到的分片不一定是该请求获取槽位的分片。不要在同一次请求中重复记录，否则会改变自适应算法观察到的成功率。

单个 Adaptive 实例初始并发度为修正后 `(min+max)/2`。累计至少 10 次完成记录才评估一次：失败比例严格大于 20% 时减半（不低于 min），严格小于 10% 时增加剩余容量的 20%（向下取整、至少增加 1，不超过 max），其余比例保持不变；该批记录随后清零。并发记录可能使一次评估包含多于 10 条，分片则各自累计，不能把此规则理解为每 10 个全局请求必定调整。缩容不会中断已运行的业务，新获取需等待在途许可归还至允许范围。

---

## 分片限流器

通过 `Ratelimit(ctx).Sharded()` 进入分片链，各分片独立运行，获取或检查请求按 round-robin 分发。实际锁竞争和吞吐量应结合负载测量。

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
defer sal.Close()
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

分片 TokenBucket 的每次 `Allow`/`AllowN` 都推进轮询，拒绝或 `n<=0` 也会占用本次轮询位置；非正请求不会消耗该片令牌。`AllowN` 支持有限小数令牌，整笔请求由所选片接受或拒绝，不向其他片借额度。`GetShard(i)` 返回原实例，直接调用它不会推进外层轮询，但会改变后续外层调用观察到的余额；越界返回 nil。生产中应固定入口和计量单位，避免把全局剩余额度误当作单次可申请量。

分片 Adaptive 的最小值和最大值也按分片分配，每片至少为 1；因此上例的最小总并发度为 16，而不是 5。使用完分片 Adaptive 后调用 `Close()` 关闭各分片。

```go
srl := async.Ratelimit(ctx).Sharded().RateLimiter().Shards(16).Rate(10000).Per(time.Second).Build()
defer srl.Close()

if err := srl.Wait(ctx); err != nil {
    return err
}
defer srl.Release() // 归还一个已成功获取的分片令牌
return doRequest(ctx)
```

`Release` 归还任一尚未归还的分片令牌，不保证与当前调用者获取的分片相同。上例选择执行后归还额度；如果选择依靠定时补充控制请求速率，则不要再手动归还。需要令牌与分片一一绑定并在结束后归还时使用 `Token` 返回的句柄；不要再为同一个句柄调用分片容器的 `Release`。

分片 RateLimiter 的 `ShardCount()`、`Size()` / `TotalRate()` 和 `Available()` 返回分片数量、当前配置的总补充额度和可用令牌总数；突发容量可能大于 Size。`GetShard(i)` 越界返回 nil，`WithStrategy(s)` 为各分片设置策略。`Resize(n)` 按分片重新分配正数总值，非正数及关闭后调用无效；建议在没有活跃 `Acquire` / `Release` 时调整，以免调整期间短暂超发或欠发。

普通 RateLimiter 的 `Resize(newRate)` 在速率变化时同时把桶容量调整为 `newRate`，原来的 Burst 容量不会保留；如果值等于当前速率，则整个调用无操作，也不改变 Burst。调整前已消耗的许可会影响调整后的可用量；定时补充仍可重新提供额度，因此它不能保证同一时刻运行任务数始终不超过 Size。需要按业务完成释放的并发槽位时使用 Adaptive，并配对 Release。

速率发生变化时，`Resize` 先按旧速率结算上次补充以来的经过时间，再调整容量和速率；尚不足一个令牌的余量保留到后续补充，桶满时则清除多余额度。原有 100ms ticker 继续运行，不会因调整而重新计时或立即补满新容量。

分片 Adaptive 的 `ShardCount()`、`TotalMin()` 和 `TotalMax()` 分别返回分片数量及各片上下限之和，`GetShard(i)` 越界返回 nil。直接操作分片会影响该片状态，应避免与统一的分片接口混用而破坏配对关系。

分片滑动窗口的 `ShardCount()` 返回分片数，`TotalLimit()` 返回各片配额之和，`GetShard(i)` 越界返回 nil。正数窗口配置下，同一滚动窗口内的接受数不超过 `TotalLimit()`；目标额度小于片数时，实际总额度会因每片至少为 1 而提高。`Allow()` 和 `AllowN(n)` 只检查轮询选中的单个分片，不会合并其他分片的剩余额度；单次批量申请可能因该片配额不足而失败，其他片仍可能有余额。

```go
// sw 在服务启动时创建并复用：Shards(4).Limit(100).Window(time.Second).Build()
fmt.Println(sw.ShardCount(), sw.TotalLimit()) // 4 100
if !sw.AllowN(5) {
    return errRateLimited
}
return doBatchRequest(ctx)
```

该段所在函数返回error，参数ctx非nil，sw为服务启动时通过`async.Ratelimit(ctx).Sharded().SlidingWindow().Shards(4).Limit(100).Window(time.Second).Build()`创建并复用的实例；打印值对应这组配置。业务签名及应用定义的errRateLimited沿用普通滑动窗口示例。生产中不要为每请求重建实例；一次接受只消费所选片5份计数并调用一次批量业务，拒绝或业务失败均须由上层处理，业务失败不会退回已消费计数。

---

## 批量补充优化

`RateLimiter` 每 100ms 检查一次，根据实际经过的时间和配置速率批量补充令牌，并保留不足一个令牌的部分供下次累计。桶满时不保留超出容量的额度；低速率下，两次补充之间可能超过一秒。

---

## 限流器作用域与业务完成

内部 `ratelimit` 包的 `WithRateLimiter`、`WithRateLimiterWithBurst`、`WithShardedRateLimiter`、`WithShardedRateLimiterWithBurst` 和 `WithShardedAdaptiveRateLimiter` 便捷函数创建实例后同步调用回调，并用 `defer Close()` 管理其资源。它们没有在根包提供同名入口；外部使用者按前文构建器创建实例并自行 `defer Close()`。回调错误原样返回，panic 在完成关闭后继续传播。关闭会结束各片的令牌补充并唤醒等待获取的调用，包括忽略 context 取消的 `BlockForce`；成功取得许可的业务仍由调用方等待和管理，自动关闭不会代替业务的完成通知。已取得许可在关闭后归还会安全忽略，不恢复已关闭实例的额度。生产中应先等待自己启动的业务完成，再结束资源作用域；不要将实例带出作用域继续获取。对应虚拟时间交叉验证见 `TestLimiterScopeClosesWaitersWithoutJoiningBusiness`。

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

本轮未产生可用于容量规划的基准结果。原表的固定吞吐数字缺少配套工具链、硬件、并发度和完整日志，已移除。评估时分别测量令牌检查开销、拒绝比例与实际业务吞吐；`Acquire/Release` 的调用速度不等于受限请求速率，成功请求数与总检查数也不能混用。

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
| `.DefaultRate()` | `b.rate = core.IO()` | 每个配置的 Per 窗口补充 `NumCPU×2` 个令牌；不修改 Per |
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

### 配置覆盖顺序

同一字段以后一次配置调用为准。`Default*` 是普通设置方法，也会覆盖之前设置的值；例如 `Rate(100).DefaultRate()` 最终使用 `IO()`，反过来调用则使用 100。`Build()` 的参数修正因限流器类型而异，不应假设每个 `Default*` 会自动调用。

生产中显式设置 RateLimiter 的 `Per` 或调用 `DefaultPer()`：`DefaultRate()` 本身不把窗口设为一秒。零值 Per 仍为 0，底层每个补充 tick 按满桶处理；这与每秒补充 IO() 个令牌不同。子构建器分别保存参数，多次选择同一模式会得到新的零值子构建器，多次 Build 则创建互不共享余额的实例。

---

## 生产环境使用建议

### 限流器选型

| 场景 | 推荐 | 理由 |
|------|------|------|
| API 网关窗口内请求数限制 | **SlidingWindow** | 按窗口内请求数判断 |
| 轻量级速率检查 | **TokenBucket** | 无后台 goroutine，按调用时的经过时间补充令牌 |
| 窗口期内精确限制 | **SlidingWindow** | 窗口级精确计数 |
| 依赖下游成功率自适应 | **AdaptiveRateLimiter** | 根据成功/失败率动态调整 |
| 高 QPS 需降低锁竞争 | **Sharded* 系列** | 分片独立处理，可通过实际负载测试评估效果 |

### RateLimiter 生产推荐配置

参数 `ctx` 为非 nil 请求 context，业务函数签名为 `doRequest(context.Context) error`；所在函数返回 `error`。示例需导入 `context`、`time` 和 `github.com/chichengyu/async`。获取失败返回获取错误，业务完成后原样返回业务错误；业务必须自行响应 context，获取成功不代表 context 尚未取消。

```go
// 令牌补充和并发槽位配置；Release 会归还令牌，不能保证严格 QPS 上限
rl := async.Ratelimit(ctx).RateLimiter().
    Rate(1000).                        // 每秒补充令牌的配置速率
    Per(time.Second).                  // 时间窗口: 1s
    Burst(2000).                       // 示例初始/最大容量，按下游突发预算调整
    Build()
defer rl.Close()

// 使用 Token defer 模式，防止令牌泄漏
token, err := rl.Token(ctx)
if err != nil {
    return err
}
defer token.Release()
return doRequest(ctx)
```

生产建议：限流器通常在服务启动时创建并跨请求复用，停止接收请求并等待业务结束后，由持有者统一 `Close`。上例展示单个完整作用域的资源收尾；每请求都创建实例会重置初始突发额度，不能作为服务整体限流。`Token.Release()` 仅归还本次许可，`Close()` 仅停止限流资源，不等待已开始的业务；不得把 `Close` 当业务等待方法。业务错误、取消、panic 或 `runtime.Goexit` 都会触发已注册的 defer，panic/Goexit 不会被这里转换为普通 error。不要复制 Token，也不要同时调用 `rl.Release()` 重复归还。

### 突发容量 (Burst) 与速率 (Rate) 的关系

`Burst` 是桶容量及初始可用令牌数，须按下游允许的瞬时突发预算配置，不能只用 Rate 的固定倍数决定。使用 `Release` 时令牌也会提前归还，不能据此推导严格的 QPS 上限；只依赖定时补充时，应核对 `Rate`、`Per` 和允许突发量。

### 分片数选择

先测单实例的锁等待、延迟和吞吐，再决定是否分片。分片 RateLimiter 和 SlidingWindow 每片至少分配 1 份额度，目标总额小于分片数时实际总额会提高；分片 TokenBucket 则对每片使用相同速率与容量。增加分片前先确认这些额度语义符合业务限制。

### RateLimiter 常见错误

- **❌ 忘记 `defer rl.Close()`**：内部有 ticker goroutine，忘记 Close 会泄漏
- **❌ 混淆令牌补充和归还**：并发槽位模式在成功获取后配对 Release；依靠定时补充限制请求频率时不要额外归还，同一次许可不要重复 Release
- **❌ 将 Burst 当 Rate 使用**：Burst 是桶容量和初始突发；定时补充取决于 Rate/Per，Release 会改变实际请求频率
- **❌ 只凭 QPS 选择分片**：先测量单实例争用，并核对分片后实际总额度
