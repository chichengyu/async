# Retry（重试）文档

## 概述

`RetryChain[T]` 是泛型重试链式构建器，提供声明式重试 API。支持指数/线性退避，以及 4 种限流模式（RateLimiter / TokenBucket / SlidingWindow / Adaptive）。

**两种入口**：

| 入口 | 说明 | 终端方法 |
|------|------|----------|
| `async.Retry[T](ctx)` | 带返回值重试 | `.Execute(fn)`；需要仅返回错误时用 `.Run(fn)` |
| `async.RetryVoid(ctx)` | 无返回值重试 | `.ExecuteVoid(fn) / .RunVoid(fn) / .Run(fn)` |

> **⚠️ 默认配置**
>
> 默认值：指数退避，最多重试 3 次（总共 4 次尝试），退避 100ms~30s，无限流。
>
> **⚠️ 重试次数含义**
>
> `MaxRetries(n)` 中的 n 是重试次数，总执行次数 = n + 1（一次初试 + n 次重试）。

日志和耗尽错误中的总次数使用 `uint(n)+1` 计算（负数先按 0 处理），因此最大 `int` 也不会被格式化成负数。64 位下 `n=9223372036854775807` 的总预算显示为 `9223372036854775808`，32 位下 `n=2147483647` 显示为 `2147483648`。该处理只修正计数显示；成功、取消、退避和限流的执行规则保持原有约定。生产仍应选用可承受的有限重试次数并设置父 context 期限，切勿把极大预算当成安全的重试策略。`TestRetryAttemptCountBoundary` 以失败后成功及失败后取消验证此边界，只实际执行一至两次，未执行最大预算的完整耗尽过程。

`RetryFn.WithRetry(n)` 同步立即执行，负次数按 0；最后一次失败后直接结束并包装该错误，避免最大 `int` 的循环计数递增溢出后超额调用。`TestRetryFnMaxIntExhaustion386` 在 32 位非 short、`audit` 标记下实际耗尽 2,147,483,648 次预算；其他平台不以跳过测试冒充完整耗尽验证。此入口没有 context 或退避，生产需要可取消等待时应使用带 context 的重试入口，并让业务主动检查取消。
>
> **⚠️ 4 种限流模式互斥**
>
> `.RateLimiter()` / `.TokenBucket()` / `.SlidingWindow()` / `.Adaptive()` 四种模式只能选择一种，后调用的会覆盖前者。不调用任何限流模式则无限流（纯退避重试）。
>
> **⚠️ 限流在重试之间生效**
>
> 每次尝试（包括首次）前都会检查或获取限流许可，失败后还会按配置退避。实际间隔取决于许可状态、回调耗时和退避时间；令牌突发及释放行为不保证相邻尝试至少间隔 `Per/Rate`。限流器在每次执行重试链时创建，不能用它限制多个独立执行之间的总请求速率。

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
- [内部单次超时辅助函数](#内部单次超时辅助函数)
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
| `.PerCallTimeout(d)` | 每次调用的 context 超时（0=不限时）；回调须响应取消并返回错误才会进入后续重试 | 0 |
| `.Context(ctx)` | 链式设置上下文（自动注入 TraceID） | — |
| `.DefaultConfig()` | 重置退避策略、重试次数、退避时间和单次调用超时；保留限流模式和限流参数 | — |
| `.DefaultMaxRetries()` | 恢复默认重试次数（3） | — |
| `.DefaultBackoff()` | 恢复默认退避（100ms~30s） | — |
| `.DefaultPerCallTimeout()` | 恢复默认超时（不限时） | — |
| `.DefaultRate()` | 恢复有效 Rate 为 10，保留 Per；RateLimiter 按 Per 窗口，TokenBucket 按秒 | — |
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
| `.Rate(n int)` | 每秒令牌生成速率；非正数使用 10 |
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
| `.Logger(l)` | 设置当前构建器的日志器（仅局部生效） |
| `.DefaultLogger()` | 恢复默认日志 |

---

## 终端方法速查

| 方法 | 签名 | 返回值 | 说明 |
|------|------|--------|------|
| `.Execute(fn)` | `fn func(context.Context) (T, error)` | `(T, error)` | 带返回值执行重试 |
| `.ExecuteVoid(fn)` | `fn func(context.Context) error` | `error` | 无返回值执行重试 |
| `.Run(fn)` | `fn func() error` | `error` | 简化版（fn 无 context 参数） |
| `.RunVoid(fn)` | `fn func(context.Context) error` | `error` | 等同于 ExecuteVoid |

业务失败导致重试耗尽时，返回的错误包装最后一次业务错误，可用 `errors.Is` 或 `errors.As` 检查；此前尝试的错误不会包含在返回值中。TokenBucket/SlidingWindow 拒绝也占用尝试次数，最后一次若因限流未执行业务，则返回“rate limit exceeded”耗尽错误，不包装之前的业务错误。有值版本最终失败返回 T 的零值。

父 context 的取消在每次尝试前及退避等待中检查，不会统一覆盖已经返回的业务结果：回调返回 nil 错误时立即成功；最后一次回调取消父 context 后返回业务错误，仍以耗尽错误包装该业务错误；尚有尝试次数时，则在退避或下一轮检查中返回父 context 错误。等待阻塞许可失败发生在最后一次时，也可能被耗尽错误包装。判断取消使用 `errors.Is(err, context.Canceled)` / `errors.Is(err, context.DeadlineExceeded)`，不要依赖错误字符串或直接相等；业务必须响应取消才能及时返回。对照见 `Example_retryCancellationResultPriority`。

> **⚠️ `.Execute(fn)` vs `.Run(fn)`**
>
> - `.Execute(fn)`：fn 有 `func(context.Context) (T, error)` 签名，接收构造时保存的父 context；正数 `PerCallTimeout` 会为每次调用派生期限。`Retry(ctx)` / `RetryVoid(ctx)` 和终端执行不自动补充 trace_id，需要时先传入 `async.EnsureTraceID(ctx)`，或显式调用 `.Context(ctx)`。
> - `.Run(fn)`：fn 是 `func() error`，更简洁，ctx 由链内部保持。无需 context 时推荐使用

---

## 基础退避重试

### 指数退避（默认）

```go
val, err := async.Retry[string](ctx).
    Exponential().
    MaxRetries(3).                           // 最多 3 次重试 = 共 4 次尝试
    Backoff(100*time.Millisecond, 5*time.Second).  // 三次重试前分别等待 100ms、200ms、400ms
    Execute(func(ctx context.Context) (string, error) {
        return httpGet(ctx, url)
    })
```

### 退避与回调耗时

退避从失败回调返回后开始；首次调用之前、成功之后和最后一次失败之后均不等待。无额外限流等待、每次回调耗时 250ms，且 `MaxRetries(3).Backoff(2*time.Second, 5*time.Second)` 的四次调用都失败时：

| 策略 | 相邻调用之间的退避 | 四次回调相对起始时间 | 最终返回时间 |
|---|---|---|---|
| `Exponential()` | 2s、4s、5s | 0、2.25s、6.5s、11.75s | 12s |
| `Linear()` | 2s、2s、2s，忽略 max | 0、2.25s、4.5s、6.75s | 7s |

`initial` 非正时不等待；指数模式的正数 `max` 限制每段退避，非正 `max` 不设置此上限，时长计算到 `time.Duration` 上界后饱和。若初始时长大于正数上限，第一次退避就使用上限。没有随机抖动。表中时间是确定性调度模型，真实运行还包含调度开销；配置限流时还可能等待许可或因拒绝消耗尝试次数。

生产中应按总截止时间安排重试预算，计入每次回调、获取许可和退避的耗时。`PerCallTimeout` 仅覆盖业务回调，每次重新创建 context，并在该次回调返回后取消；不覆盖许可等待或退避。父期限更早时以父期限为准，回调仍须响应取消。固定退避不能保证调用起始时间等距。实现核对见 `TestRetryBusinessBackoffSchedule`、`TestRetryPerCallContextLifetime` 和 `TestRetryPerCallDeadlineSchedule`。

非阻塞限流的拒绝也会推进退避次数。例如不分片的 `TokenBucket().Rate(1).Capacity(1)`，或 `SlidingWindow().Limit(1).Window(time.Second)`，使用 `MaxRetries(4).Backoff(400*time.Millisecond, 1200*time.Millisecond)`，回调立即失败时：

| 策略 | 五次许可检查的相对时间 | 实际业务回调的相对时间 | 最终错误 |
|---|---|---|---|
| `Exponential()` | 0、0.4s、1.2s、2.4s、3.6s | 0、1.2s、2.4s、3.6s | 包装最后一次业务错误 |
| `Linear()` | 0、0.4s、0.8s、1.2s、1.6s | 0、1.2s | 限流耗尽，不包含之前的业务错误 |

这是无其他等待的确定性示例，不能由此推断分片或不同容量的调用次数。分片按各自额度和路由变化；普通/分片与两种策略的动态交叉验证见 `TestRetryLimiterBackoffTimeline`。

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
> `RateLimiter` 在额度不足时等待令牌或 context 取消。成功取得许可后才调用业务函数；取消、尝试次数上限和业务错误仍可能结束重试，不能保证每个请求最终发出。

---

## 模式二：TokenBucket（经典令牌桶）

非阻塞式限流：每次重试前检查令牌，拿不到时跳过本次重试等待退避后重试。

```go
err := async.RetryVoid(ctx).
    Exponential().MaxRetries(10).
    Backoff(100*time.Millisecond, 5*time.Second).
    TokenBucket().          // 启用 TokenBucket
    Rate(5).                // 每秒 5 个令牌，参数为 int
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
> **⚠️ RetryChain 的 Rate 参数**
>
> `RetryChain.Rate(n int)` 同时配置 RateLimiter 的每 Per 窗口额度和 TokenBucket 的每秒令牌数，先选模式或先设 Rate 均可；SlidingWindow 使用 `Limit(n)`。重试链没有 `Rate(float64)` 重载，`Rate(0.5)` 不能编译。需要小数速率的独立令牌桶可使用 `async.Ratelimit(ctx).TokenBucket().Rate(0.5).Capacity(1).Build()`，其构建器与 RetryChain 是不同类型。

启用分片时，TokenBucket 的 Rate 和 Capacity **按每片配置**，总补充速率与初始容量分别乘以片数；Per 不参与其计算。它与下面按总 Limit 分配的 SlidingWindow 不同，生产配置应先确定整体额度，再换算每片参数。

令牌会在退避经过的时间内积累，并在下一次正数检查时结算，因此先前拒绝后仍可能恢复执行业务。每个尝试都推进所选分片，不能只用总经过时间预测单片余额。以 `Linear().MaxRetries(4).Backoff(250*time.Millisecond, 0).TokenBucket().Rate(1).Capacity(1).Shards(1)` 为例，业务立即失败时，尝试发生于 0／250／500／750／1000ms，只有 0 和 1000ms 真正调用业务；其余三次拒绝仍占重试预算。显式 `Shards(1)` 使用单片，`Shards(0)` 会选择默认片数，不能用于关闭分片。生产中应把业务耗时、退避及每片恢复时间一起计入父期限；虚拟时间交叉验证见 `TestRetryTokenRefillDuringBackoff`。

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
> - RateLimiter：额度不足时等待令牌或取消；初始突发和许可归还会影响实际间隔，不保证请求均匀分布
> - SlidingWindow：非阻塞精确计数，超限时进入退避重试

分片 SlidingWindow 将总 Limit 按余数分配，每片至少 1。因此 Limit 小于分片数时实际总额度会增大，例如 Limit(1).Shards(4) 在窗口内可接受 4 次，而非 1 次。轮询只检查选中片，不借用其他片剩余额度；总额度未耗尽时也可能拒绝一次尝试，后续尝试可能在别的片成功。严格配额应使分片数不超过额度，并评估这种局部拒绝，必要时使用不分片的共享限流器。可执行对照见 `Example_retryShardedWindowBudget`。

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
> 每次尝试前 `Acquire`，执行后记录结果并 `Release`。同一次 Execute 的尝试依次执行，每次 Execute 又创建独立限流器，因此这个模式不会把不同 Execute 的回调合并为共享并发限制；默认 4 次尝试也不足以达到自适应算法至少 10 次记录的调整门槛。需要跨调用调节下游并发时，使用共享的独立 AdaptiveRateLimiter，在业务调用外配对 Acquire/Release 并记录结果。

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

`PerCallTimeout(d)` 为每次 fn 调用创建带超时的 context。回调必须响应该 context 并返回错误；在父 context 仍有效且重试次数未用尽时，才会退避并再次尝试：

```go
val, err := async.Retry[string](ctx).
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    PerCallTimeout(2 * time.Second).   // 2 秒后通知本次调用取消，回调需配合退出
    Execute(func(ctx context.Context) (string, error) {
        return slowRPC(ctx, req)       // RPC 须响应取消并返回错误，父 context 有效时才继续重试
    })
```

> **⚠️ PerCallTimeout 不能强制中断回调**
>
> 单次超时会取消传给回调的子 context，但不会强制终止忽略 context 的回调；回调一直阻塞时也无法进入下一次重试。外部 context 可请求终止整个重试流程，正在执行的回调同样需要配合取消。
> 超时通过传给 `fn` 的 context 通知；`fn` 及其阻塞的 I/O 必须响应取消。若 `fn` 忽略 context，重试调用仍会等待它返回，单次超时和外部超时都无法强制中断它。

单次调用结束后通过 defer 取消该次派生 context，不取消父 context；回调启动的工作如果继续持有这个 context，会收到取消。普通业务 panic 被恢复后仍遵循重试策略；`runtime.Goexit` 会结束调用 goroutine，不会转换为错误或触发下一次尝试，但单次 context 仍被清理。内部 `RetryWithConfig/RetryWithConfigVoid` 与链式入口按这一规则处理，见 [Goexit 清理回归](../internal/retry/timeout_lifetime_contract_test.go)。日志后端若在 panic 记录期间自身 panic，该 panic 继续传播，单次 context 同样执行清理。

RateLimiter、Adaptive 及其分片模式还会为每次成功获取的许可设置局部 defer，`Goexit` 或日志后端 panic 展开时也归还；正常返回时仍先记录 Adaptive 成功/失败，再归还许可，退避和下一次尝试开始前已完成释放。未获取许可不执行释放，TokenBucket/SlidingWindow 消耗型额度不按此规则归还。`Goexit` 或日志后端 panic 没有产生普通调用结果，因此不追加 Adaptive 成功/失败记录。生产中优先正常返回值/错误管理重试，日志后端应及时返回且不 panic；不要把清理保证理解为这些异常会被转换成普通结果。

公开入口的Goexit和单次context清理示例见 [Example_retryGoexitCleanup](tests/public_examples_test.go)，四类许可的异常退出、退避前释放与获取失败验证见 [许可清理契约测试](../internal/retry/permit_exit_contract_test.go)。

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
err = async.RetryVoid(ctx).
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    Run(func() error {
        return doSomethingWithoutContext()      // fn 无 ctx 参数
    })

// RunVoid：等价于 ExecuteVoid，fn 有 ctx 参数
err = async.RetryVoid(ctx).
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
> 这些方法都属于 `RetryChain[T]`，可用于任意 T；`Execute` 返回 T，其他三个方法只返回 error。无需返回值时使用 `RetryVoid(ctx)` 更直观。

---

## 默认值体系

### 重试策略默认值

| 默认值 | 获取/恢复方法 | 默认值 | 说明 |
|--------|-------------|--------|------|
| 默认退避策略 | `.Exponential()` | **指数退避** | 首次创建时就是指数退避 |
| 默认重试次数 | `.DefaultMaxRetries()` | **3 次** | 总执行次数 = 4（1 次初试 + 3 次重试） |
| 默认退避时间 | `.DefaultBackoff()` | **100ms ~ 30s** | 指数模式：首间隔+上限；线性模式：固定间隔 |
| 默认单次超时 | `.DefaultPerCallTimeout()` | **不限时**（0） | 重置每次调用的 context 超时 |
| 默认重试配置 | `.DefaultConfig()` | — | 重置退避、重试次数和单次超时，保留限流模式及参数 |

### 限流默认值（四选一模式）

| 模式 | `Default*` 方法 | 默认值 | 说明 |
|------|----------------|--------|------|
| RateLimiter | `.DefaultRate()` | **每 Per 窗口 10 个** | 只重置 Rate，保留自定义 Per；Per 默认才为 1 秒 |
| RateLimiter | `.DefaultPer()` | **1 秒** | 时间窗口 |
| RateLimiter | `.DefaultBurst()` | **0**（不开启突发） | 突发容量 |
| TokenBucket | `.DefaultRate()` | **10/秒** | 与 RateLimiter 共用 Rate(int) 配置字段 |
| TokenBucket | `.DefaultCapacity()` | **速率×2** | 令牌桶容量 |
| SlidingWindow | `.DefaultLimit()` | **100** | 每窗口请求数 |
| SlidingWindow | `.DefaultWindow()` | **1 秒** | 窗口大小 |
| Adaptive | `.DefaultMinWorker()` | **1** | 最小并发度 |
| Adaptive | `.DefaultMaxWorker()` | **Min×10** | 最大并发度 |
| 所有限流模式 | `.DefaultShards()` | **不启用分片** | 分片数=0 |

### 配置覆盖顺序

同一字段以后一次调用为准，`Default*` 也是普通赋值方法；`Rate(50).DefaultRate()` 最终恢复为 10，而 `DefaultRate().Rate(50)` 使用 50。`DefaultConfig()` 会同时重置退避类型、次数、退避和单次超时，保留限流模式及其配置；模式方法也以前一次被后一次覆盖。执行方法读取最后保存的配置并为本次执行创建限流器。

`DefaultConfig()` 也保留父 context 和分片数；已经取消的父 context 不会因此恢复。它返回同一构建器，配置应在执行前完成，不能与执行并发修改。每次终端调用都从新限流器开始，所以相同构建器重复执行不累计上次的令牌或窗口占用；生产中需要跨请求共享额度时，应在外层复用独立限流器。

例如，以下两次执行各有一个窗口许可；每次业务第一次失败后，余下三次尝试均被窗口拒绝，最终错误是限流错误。`DefaultConfig()` 保留一小时窗口和每窗口一个许可，同时恢复默认的三次重试；业务回调总共执行两次。

```go
chain := async.Retry[int](ctx).
    SlidingWindow().Limit(1).Window(time.Hour).
    MaxRetries(8).DefaultConfig()
calls := 0
for range 2 {
    _, err := chain.Execute(func(context.Context) (int, error) {
        calls++
        return 0, errors.New("temporary failure")
    })
    fmt.Println(err != nil)
}
fmt.Println(calls) // 2；约两次 700ms 退避，业务耗时另计
```

注意 `DefaultShards()` 将各模式分片字段清零，执行时走单片路径；`Shards(0)` 则使用 `DefaultShardCount()`，两者用途不同。`DefaultCapacity()` 和 `DefaultMaxWorker()` 的默认值在执行时分别依有效速率和有效最小并发度计算，不在调用这些方法时固定。

固定配置的同一构建器被多个终端调用读取时，各次执行仍创建自己的限流器和单次调用 context，错误与尝试次数也分别计算；没有跨执行的共享额度。所有配置方法应先完成，不能与执行并发修改，业务闭包共享的数据须由应用同步。两个执行同时阻塞在首次业务时仍能各自取得 RateLimiter/Adaptive 容量为 1 的许可，成功、错误、panic 和 Goexit 的结果与 context 清理由 `TestRetryConcurrentExecutionIsolation` 交叉验证；Goexit 沿调用 goroutine 退出，不产生普通终端返回。

---

## 生产环境使用建议

### 重试策略选型

| 场景 | 推荐策略 | 理由 |
|------|---------|------|
| 依赖服务偶发不可用（502/503） | 指数退避 | 减少对下游的压力 |
| 失败后固定等待再轮询 | 线性退避 | 固定退避时长，调用起始间隔还包含回调和许可等待 |
| 限流器 429 响应 | RateLimiter + 指数退避 | 限流后等待更久再试 |
| 调用下游有 QPS 配额 | 共享的独立 TokenBucket / SlidingWindow | 重试链每次执行会新建限流器，不能限制多个调用的总频率 |
| 需要跨调用根据成功率调节并发 | 共享的独立 AdaptiveRateLimiter + 重试 | 在重试链外共享并发额度与成功/失败统计 |

### 退避时间建议

| 下游恢复时间 | 推荐 Backoff(min, max) | 理由 |
|-------------|----------------------|------|
| 快速恢复（<1s） | `(100ms, 3s)` | 短间隔快速重试 |
| 中等恢复（1~10s） | `(500ms, 15s)` | 给下游留足恢复时间 |
| 慢速恢复（>10s） | `(1s, 60s)` | 长间隔避免雪崩 |

### 重试次数建议

| 场景 | 推荐 MaxRetries | 理由 |
|------|----------------|------|
| 关键数据写入 | 按截止时间和错误类型决定 | 使用幂等键或可重入操作；超时可能已写入，不能直接重复执行 |
| API 查询（幂等） | 可从 **2~3** 次试起 | 受总截止时间和下游承载能力限制 |
| 非关键通知 | **1~2** | 失败可丢弃 |
| 限流场景 | 按总截止时间与恢复信号决定 | 区分阻塞等许可和非阻塞拒绝；重试链不共享跨调用额度，拒绝也消耗尝试次数 |

### 限流 + 重试组合推荐

```go
// 推荐配置：指数退避 + RateLimiter 限流
val, err := async.Retry[string](ctx).
    Exponential().MaxRetries(5).
    Backoff(200*time.Millisecond, 10*time.Second).
    PerCallTimeout(3 * time.Second).    // 通知响应 context 的回调超时退出
    RateLimiter().
    Rate(10).Per(time.Second).          // 本次执行使用的限流器参数
    Burst(20).                          // 允许短暂突发 20 次
    Execute(func(ctx context.Context) (string, error) {
        return callExternalAPI(ctx)
    })
```

### 生产推荐配置

下面是按顺序执行的四步业务流程。`ctx` 是调用方的非 nil 父 context，`req` 是查询输入，`key` 是应用生成的幂等键；三个 `Query` 返回 `(*Data, error)`，写入方法返回 `error`，均须使用传入的单次 context。函数在任一步失败时返回错误并停止后续步骤；写入使用第一步的 `rpcData`，另外两个查询结果需按业务消费。`Retry` 失败时返回零值数据，不能在错误后继续使用失败结果。

以下预算只统计业务每次立即失败时的退避，实际耗时还包含业务和许可等待。生产应为整个流程设置父期限，核对重试错误是否可恢复，并为写入保留幂等键；单次超时只请求取消，不能强制中止不协作的下游。回调返回 nil 错误时保留成功，即使该次已取消父 context；若还有后续步骤，它会在尝试前观察取消，最后一步成功则整个流程仍可成功。四步原文的成功、各步耗尽、首次失败后恢复、单次超时后恢复、父预取消及各步取消错误/成功，共 22 个场景可用 `python docs/scripts/check_retry_production_examples.py` 验证，需要 Python 3、Go 1.25 以上及支持 race 的工具链。

| 步骤 | 最多实际业务调用 | 立即失败耗尽的累计退避 | 首次单次超时、第二次成功时的耗时 |
|---|---|---|---|
| 标准 RPC | 4 | 1.4s | 5.2s |
| 本次执行 RateLimiter | 11 | 151.5s | 10.5s |
| 本次执行 Adaptive | 6 | 3.1s | 3.1s |
| 幂等写入 | 11 | 303s | 31s |

表中除所测步骤外，其余步骤均立即成功，没有计入额外耗时；限流器每次尝试后归还许可，所以这些顺序调用不因容量再等待，也不构成跨请求速率限制。单次回调超时返回错误后，在父 context 仍有效且还有次数时继续重试。

```go
// ========== 标准 RPC 重试：指数退避 + 超时 ==========
// 适用场景：下游偶发 502/503，幂等查询
rpcData, rpcErr := async.Retry[*Data](ctx).
    Exponential().
    MaxRetries(3).                                      // 初试 + 3 次重试 = 共 4 次
    Backoff(200*time.Millisecond, 10*time.Second).      // 三次重试前分别等待 200ms、400ms、800ms
    PerCallTimeout(5 * time.Second).                    // 回调需响应 context 才能及时退出
    Execute(func(ctx context.Context) (*Data, error) {
        return rpcClient.Query(ctx, req)
    })
// 总耗时还取决于回调是否及时响应取消和实际退避；外部 context 可请求停止整个流程
if rpcErr != nil { return rpcErr }

// ========== 限流下游重试：指数退避 + RateLimiter ==========
// 适用场景：控制单次重试流程；跨请求的下游限流需使用共享限流器
limitedData, limitedErr := async.Retry[*Data](ctx).
    Exponential().
    MaxRetries(10).                                     // 限流场景多试几次
    Backoff(500*time.Millisecond, 30*time.Second).      // 长间隔避让
    PerCallTimeout(10 * time.Second).                   // 单次超时
    RateLimiter().
    Rate(10).Per(time.Second).Shards(4).               // 本次执行使用的分片限流器参数
    Burst(20).                                          // 允许瞬时突发 20 次
    Execute(func(ctx context.Context) (*Data, error) {
        return rateLimitedAPI.Query(ctx, req)
    })
if limitedErr != nil { return limitedErr }

// ========== 单次重试的 Adaptive 模式 ==========
// 此限流器仅供本次 Execute 使用，不限制不同 Execute 之间的总并发
adaptiveData, adaptiveErr := async.Retry[*Data](ctx).
    Exponential().MaxRetries(5).
    Backoff(100*time.Millisecond, 15*time.Second).
    PerCallTimeout(3 * time.Second).
    Adaptive().
    MinWorker(2).MaxWorker(50).                         // 本次执行的额度范围；最多 6 次尝试不足以触发调整
    Execute(func(ctx context.Context) (*Data, error) {
        return unstableSvc.Query(ctx, req)
    })
if adaptiveErr != nil { return adaptiveErr }
// rpcData、limitedData 和 adaptiveData 在此按业务消费。

// ========== 关键写入重试：最大努力保证 ==========
// ⚠️ 确认下游支持幂等，否则勿用
writeErr := async.RetryVoid(ctx).
    Exponential().MaxRetries(10).
    Backoff(1*time.Second, 60*time.Second).             // 最长等 60s
    PerCallTimeout(30 * time.Second).
    ExecuteVoid(func(ctx context.Context) error {
        return db.WriteWithIdempotentKey(ctx, key, rpcData)
    })
if writeErr != nil { return writeErr }
```

### 常见错误

- **❌ 忽略 context 取消**：下游调用可能使重试循环一直等待。为每次调用设置 `PerCallTimeout`，并确保下游 I/O 使用该 context；外部 context 可限制整个重试流程。
- **❌ 重试非幂等操作**：Write/Delete 类操作重试前确认下游支持幂等
- **❌ 指数退避上限过小**：如果 max 小于 `initial × 2^retries`，后续间隔时间相同，退避效果不明显
- **❌ RateLimiter 和 TokenBucket 混淆**：前者阻塞等令牌，后者非阻塞跳过，根据场景选择
- **❌ 假设 DefaultConfig() 清除限流配置**：它只重置重试策略和单次超时；需要全新配置时重新创建构建器。

---

## 内部单次超时辅助函数

`WithTimeout`、`WithTimeoutVoid`、`WithDeadline`、`WithDeadlineVoid` 定义在 `internal/retry`，根包没有同名入口，外部使用者不能导入本项目的 internal 包。这里用于说明库内辅助方法及其与公开重试链的区别；外部需要重试时用 `async.Retry/RetryVoid`，只需单次 context 期限时直接用标准库 `context.WithTimeout/WithDeadline`。

| 内部方法 | 参数与返回值 |
|---|---|
| `WithTimeout[T](ctx, timeout, fn)` | 非 nil 父 context、相对时长、`func(context.Context) (T,error)`；原样返回回调的值与错误 |
| `WithTimeoutVoid(ctx, timeout, fn)` | 非 nil 父 context、相对时长、`func(context.Context) error`；原样返回回调错误 |
| `WithDeadline[T](ctx, deadline, fn)` | 非 nil 父 context、绝对截止时间、`func(context.Context) (T,error)`；原样返回回调的值与错误 |
| `WithDeadlineVoid(ctx, deadline, fn)` | 非 nil 父 context、绝对截止时间、`func(context.Context) error`；原样返回回调错误 |

四方法都在调用方 goroutine 同步调用回调一次，继承父 context 的值并采用更早期限。父已取消、非正 timeout、已过期或零值 deadline 仍会调用回调。期限只发取消信号：回调忽略取消时包装函数继续等待，回调最终返回 nil 错误也仍表示成功；非 nil 错误同时附带的值不被清零。回调或 nil 函数调用的 panic 原样传播，不像重试入口那样包装为 `PanicError`。

生产中传入正确的父 context，让下游 I/O 响应取消，并明确处理回调错误。普通返回、panic 展开和 `runtime.Goexit` 都会执行 defer 取消派生 context，父 context 保持原状。回调自建 goroutine 共享此 context 时，包装调用结束就会收到取消；它不会延长派生 context 的生命周期，仍需由业务负责退出与等待。不要把此类辅助函数视为异步任务句柄或独立后台 context。

使用及输出断言见 [ExampleWithDeadline_lifetime](../internal/retry/timeout_example_test.go)，已过期仍调用的示例见同文件 `ExampleWithTimeout`。四入口的成功/失败/panic、父取消/期限、Goexit清理和到期后等待回调验证见 [生命周期契约测试](../internal/retry/timeout_lifetime_contract_test.go)。逐方法完整签名、调用示例、参数及生产注意事项见 [Retry API](api/retry.md)。

## 自定义日志

```go
val, err := async.Retry[string](ctx).
    Logger(myLogger).     // 只配置当前重试链及其后续执行
    Exponential().MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    Execute(fn)

err = async.RetryVoid(ctx).
    Logger(myLogger).
    DefaultLogger().      // 当前构建器恢复继承全局日志器
    Run(func() error { return doSomethingWithoutContext() })
```
