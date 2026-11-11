# retry 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [retry](../retry.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*RetryChain[T].Adaptive`

```go
func (c *RetryChain[T]) Adaptive() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L406) · [行为专题](../retry.md)：Adaptive 启用自适应限流模式。根据成功率自动调整并发度（成功率高→扩并发，失败率高→缩并发）。

**源码说明与使用片段**（原文）：

Adaptive 启用自适应限流模式。根据成功率自动调整并发度（成功率高→扩并发，失败率高→缩并发）。 每次重试前 Acquire 并发槽位，执行完毕后 Release。 配置方法：MinWorker / MaxWorker / Shards。

示例：

	err := retry.NewVoid(ctx).Exponential().MaxRetries(10).Backoff(50*time.Millisecond, 5*time.Second).
	    Adaptive().MinWorker(5).MaxWorker(100).Shards(16).ExecuteVoid(fn)


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Adaptive()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：一次 Execute 内的尝试依次执行，每次 Execute 各建独立限流器，不能限制跨调用的总并发。自适应至少需要 10 次成功/失败记录；默认 4 次尝试不会触发调整。需要跨调用调节时在重试链外使用共享 AdaptiveRateLimiter。

## `*RetryChain[T].Backoff`

```go
func (c *RetryChain[T]) Backoff(initial, max time.Duration) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L181) · [行为专题](../retry.md)：Backoff 设置退避时间参数。

**源码说明与使用片段**（原文）：

Backoff 设置退避时间参数。 initial 为首次重试等待时间，max 为退避上限（0 = 无上限）。 线性策略下 initial 是固定等待时间，max 被忽略。

示例：

	// 指数：100ms → 200ms → 400ms → 800ms（上限 5s）
	.Backoff(100*time.Millisecond, 5*time.Second)

	// 线性：每次等 500ms
	.Linear().Backoff(500*time.Millisecond, 0)


**参数说明**：

- `initial` (`time.Duration`)：首次重试前的等待时长；线性模式固定使用此值，指数模式逐次翻倍。非正值不等待，持续失败时可能快速重试。
- `max` (`time.Duration`)：指数退避的正数时长上限；非正值表示不设置此上限。链式 Linear 忽略它并使用固定 initial。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Backoff(initial, max)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：只设置退避字段，不选择策略；DefaultBackoff 使用 100ms/30s。非正 initial 不等待，非正 max 不设指数上限；Linear 忽略 max。业务失败回调返回或非阻塞限流拒绝之后开始退避，首次调用前、成功后及最后一次之后不再等待；回调耗时与许可等待另计，固定退避不保证调用起始时间等距，不加入随机抖动。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.initialBackoff = initial
c.maxBackoff = max
```

## `*RetryChain[T].Burst`

```go
func (c *RetryChain[T]) Burst(n int) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L332) · [行为专题](../retry.md)：Burst 设置 RateLimiter 突发容量，允许短时间超过 Rate 速率的请求数。

**源码说明与使用片段**（原文）：

Burst 设置 RateLimiter 突发容量，允许短时间超过 Rate 速率的请求数。

示例：

	.RateLimiter().Rate(50).Per(time.Second).Burst(200)


**参数说明**：

- `n` (`int`)：RateLimiter 突发容量；正数启用 burst 构造路径，单片实际容量至少为有效 Rate，非正值使用普通构造路径。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Burst(n)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 RateLimiter 模式使用；正数开启突发容量，实际容量至少为有效 Rate，DefaultBurst 将字段清零并使用普通构造路径。分片时按总量分配且每片至少 1；单次执行的限流器与其他 Execute 不共享。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.rlBurst = n
```

## `*RetryChain[T].Capacity`

```go
func (c *RetryChain[T]) Capacity(n float64) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L356) · [行为专题](../retry.md)：Capacity 设置 TokenBucket 容量（最大积压令牌数）。

**源码说明与使用片段**（原文）：

Capacity 设置 TokenBucket 容量（最大积压令牌数）。

示例：

	.TokenBucket().Rate(10).Capacity(20)  // 每秒 10 个令牌，最多积压 20 个


**参数说明**：

- `n` (`float64`)：TokenBucket 最大积压令牌数及初始令牌数；非正值使用有效 Rate 的两倍。分片模式对每片应用此容量。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Capacity(n)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 TokenBucket 模式使用，容量同时决定初始令牌数；非正值及 DefaultCapacity 按执行时有效 Rate 的两倍计算。分片时各片都有此容量，不能当成总容量；应使用有限正数，NaN/Inf 不经业务校验。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.tbCapacity = n
```

## `*RetryChain[T].Context`

```go
func (c *RetryChain[T]) Context(ctx context.Context) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L125) · [行为专题](../retry.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：替换后续 Execute 使用的父 context 并确保 trace_id，须非 nil；不重置策略或限流设置。父取消会阻止后续尝试及退避等待，正在运行的回调仍须响应取消；配置不与执行并发。

## `*RetryChain[T].DefaultBackoff`

```go
func (c *RetryChain[T]) DefaultBackoff() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L223) · [行为专题](../retry.md)：DefaultBackoff 使用默认退避参数：初始 100ms，最大 30s。

**源码说明与使用片段**（原文）：

DefaultBackoff 使用默认退避参数：初始 100ms，最大 30s。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBackoff()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：只设置退避字段，不选择策略；DefaultBackoff 使用 100ms/30s。非正 initial 不等待，非正 max 不设指数上限；Linear 忽略 max。业务失败回调返回或非阻塞限流拒绝之后开始退避，首次调用前、成功后及最后一次之后不再等待；回调耗时与许可等待另计，固定退避不保证调用起始时间等距，不加入随机抖动。

## `*RetryChain[T].DefaultBurst`

```go
func (c *RetryChain[T]) DefaultBurst() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L249) · [行为专题](../retry.md)：DefaultBurst 使用默认突发容量（0=不开启）。

**源码说明与使用片段**（原文）：

DefaultBurst 使用默认突发容量（0=不开启）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBurst()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 RateLimiter 模式使用；正数开启突发容量，实际容量至少为有效 Rate，DefaultBurst 将字段清零并使用普通构造路径。分片时按总量分配且每片至少 1；单次执行的限流器与其他 Execute 不共享。

## `*RetryChain[T].DefaultCapacity`

```go
func (c *RetryChain[T]) DefaultCapacity() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L255) · [行为专题](../retry.md)：DefaultCapacity 使用默认令牌桶容量（速率 × 2）。

**源码说明与使用片段**（原文）：

DefaultCapacity 使用默认令牌桶容量（速率 × 2）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultCapacity()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 TokenBucket 模式使用，容量同时决定初始令牌数；非正值及 DefaultCapacity 按执行时有效 Rate 的两倍计算。分片时各片都有此容量，不能当成总容量；应使用有限正数，NaN/Inf 不经业务校验。

## `*RetryChain[T].DefaultConfig`

```go
func (c *RetryChain[T]) DefaultConfig() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L207) · [行为专题](../retry.md)：DefaultConfig 恢复默认配置：指数退避，最多重试 3 次，退避 100ms~30s，无每次调用超时。

**源码说明与使用片段**（原文）：

DefaultConfig 恢复默认配置：指数退避，最多重试 3 次，退避 100ms~30s，无每次调用超时。 保留父 context、限流模式、各模式参数和分片数；不等同于重新创建构建器。

示例：

	// 重置到默认再微调
	retry.New[string](ctx).DefaultConfig().MaxRetries(10)


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultConfig()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：只重置五个重试策略字段：指数退避、3 次重试、100ms 起始/30s 上限、无单次超时；保留父 context、当前限流模式、各模式参数及分片。返回同一构建器；配置不能与执行并发。每次终端重新创建限流器，重置策略不提供跨执行共享额度；需要全新配置时重新创建构建器。

## `*RetryChain[T].DefaultLimit`

```go
func (c *RetryChain[T]) DefaultLimit() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L261) · [行为专题](../retry.md)：DefaultLimit 使用默认滑动窗口限制（100）。

**源码说明与使用片段**（原文）：

DefaultLimit 使用默认滑动窗口限制（100）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLimit()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 SlidingWindow 模式读取；非正 Limit 使用 100，非正 Window 使用 1 秒，DefaultLimit/DefaultWindow 分别清零相应字段以恢复这些值。分片按总 Limit 分配且每片至少 1；每次 Execute 都新建空窗口，不能跨调用累计。

## `*RetryChain[T].DefaultLogger`

```go
func (c *RetryChain[T]) DefaultLogger() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L458) · [行为专题](../retry.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*RetryChain[T].DefaultMaxRetries`

```go
func (c *RetryChain[T]) DefaultMaxRetries() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L217) · [行为专题](../retry.md)：DefaultMaxRetries 使用默认最大重试次数（3）。

**源码说明与使用片段**（原文）：

DefaultMaxRetries 使用默认最大重试次数（3）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxRetries()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：负次数按 0，DefaultMaxRetries 设为 3；初次尝试不计入重试次数，最多次数还受 context 与限流约束。限流拒绝也占用尝试次数，不保证回调一定执行 n+1 次。日志和耗尽错误中的总次数先转为 uint 再加 1，兼容最大 int；这不缩小执行预算，生产应配置可承受的有限次数及父 context 期限。

## `*RetryChain[T].DefaultMaxWorker`

```go
func (c *RetryChain[T]) DefaultMaxWorker() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L279) · [行为专题](../retry.md)：DefaultMaxWorker 使用默认最大并发度（最小并发度 × 10）。

**源码说明与使用片段**（原文）：

DefaultMaxWorker 使用默认最大并发度（最小并发度 × 10）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxWorker()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 Adaptive 模式使用；非正最小值按 1，非正最大值按有效最小值的 10 倍，Default* 清零相应字段。最大值低于最小值时构造器提升至最小值；初始容量为二者中点。配置应避免整数溢出，单次 Execute 的尝试依次执行，不提供跨调用共享并发控制。

## `*RetryChain[T].DefaultMinWorker`

```go
func (c *RetryChain[T]) DefaultMinWorker() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L273) · [行为专题](../retry.md)：DefaultMinWorker 使用默认最小并发度（1）。

**源码说明与使用片段**（原文）：

DefaultMinWorker 使用默认最小并发度（1）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMinWorker()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 Adaptive 模式使用；非正最小值按 1，非正最大值按有效最小值的 10 倍，Default* 清零相应字段。最大值低于最小值时构造器提升至最小值；初始容量为二者中点。配置应避免整数溢出，单次 Execute 的尝试依次执行，不提供跨调用共享并发控制。

## `*RetryChain[T].DefaultPer`

```go
func (c *RetryChain[T]) DefaultPer() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L243) · [行为专题](../retry.md)：DefaultPer 使用默认限流时间窗口（1s）。

**源码说明与使用片段**（原文）：

DefaultPer 使用默认限流时间窗口（1s）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPer()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 RateLimiter 模式读取此窗口，非正值及 DefaultPer 在执行时使用 1 秒；TokenBucket 的 Rate 仍按秒计算。设置不会切换限流模式；RateLimiter 每次业务结束会归还令牌，不能据窗口推断严格的业务 QPS。

## `*RetryChain[T].DefaultPerCallTimeout`

```go
func (c *RetryChain[T]) DefaultPerCallTimeout() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L230) · [行为专题](../retry.md)：DefaultPerCallTimeout 使用默认单次调用超时（不限时）。

**源码说明与使用片段**（原文）：

DefaultPerCallTimeout 使用默认单次调用超时（不限时）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPerCallTimeout()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：正数为每次业务调用独立创建 context 期限，父期限更早时从父期限，在该次回调返回后立即取消；非正值及 DefaultPerCallTimeout 不设置单次期限。不包括获取限流许可或退避等待；同步调用不强制终止业务，也不自动把 nil 错误替换为超时错误。Run 的无参回调拿不到此 context。

## `*RetryChain[T].DefaultRate`

```go
func (c *RetryChain[T]) DefaultRate() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L237) · [行为专题](../retry.md)：DefaultRate 将有效 Rate 恢复为 10，保留 Per。

**源码说明与使用片段**（原文）：

DefaultRate 将有效 Rate 恢复为 10，保留 Per。 RateLimiter 按 Per 窗口计数，TokenBucket 按秒计数。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultRate()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：清零 RateLimiter/TokenBucket 共用的 Rate 字段，后续执行使用 10；不会改动模式、Per、Burst 或 Capacity。RateLimiter 按 Per 窗口补充，TokenBucket 按秒生成，分片总量规则不同。

## `*RetryChain[T].DefaultShards`

```go
func (c *RetryChain[T]) DefaultShards() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L285) · [行为专题](../retry.md)：DefaultShards 使用默认分片数（不启用分片模式）。

**源码说明与使用片段**（原文）：

DefaultShards 使用默认分片数（不启用分片模式）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShards()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：将四种限流模式的分片字段均清零，终端使用单片路径；Shards(0) 则使用 DefaultShardCount，二者不同。模式及额度保持，后续 Shards 会再次覆盖；每次 Execute 的额度仍独立。

## `*RetryChain[T].DefaultWindow`

```go
func (c *RetryChain[T]) DefaultWindow() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L267) · [行为专题](../retry.md)：DefaultWindow 使用默认滑动窗口大小（1s）。

**源码说明与使用片段**（原文）：

DefaultWindow 使用默认滑动窗口大小（1s）。


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWindow()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 SlidingWindow 模式读取；非正 Limit 使用 100，非正 Window 使用 1 秒，DefaultLimit/DefaultWindow 分别清零相应字段以恢复这些值。分片按总 Limit 分配且每片至少 1；每次 Execute 都新建空窗口，不能跨调用累计。

## `*RetryChain[T].Execute`

```go
func (c *RetryChain[T]) Execute(fn func(context.Context) (T, error)) (T, error)
```

[实现](../../internal/retry/retry_builder.go#L474) · [行为专题](../retry.md)：Execute 执行重试链，返回 (T, error)。

**源码说明与使用片段**（原文）：

Execute 执行重试链，返回 (T, error)。 每次尝试前或退避时观察到父取消则返回 context 错误；不覆盖回调返回的成功， 最后一次业务失败仍包装该错误，等待许可失败也可能以耗尽错误包装取消。 最后一次若被 TokenBucket/SlidingWindow 拒绝，只包装该次限流错误，不保留此前业务错误。

示例：

	val, err := retry.New[string](ctx).
	    RateLimiter().Rate(10).Per(time.Second).
	    Execute(func(ctx context.Context) (string, error) {
	        return httpGet(ctx, url)
	    })


**参数说明**：

- `fn` (`func(context.Context) (T, error)`)：每次尝试同步调用的业务函数，接收该次 context 并返回结果与错误；nil 错误结束重试，panic 转为 PanicError 后按重试策略处理。须幂等且响应取消。ExecuteVoid/RunVoid 仅返回 error。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Execute(fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：单次执行内同步依次尝试，每次终端新建独立限流器，不能限制跨 Execute 总流量；配置固定时并发终端的许可、尝试次数、错误及单次子context仍独立，配置方法不能与执行并发，业务闭包共享数据由应用同步。RateLimiter/Adaptive及其分片每次成功获取许可后以局部defer释放，包含Goexit和日志后端panic，正常路径仍先记录Adaptive结果再释放，退避前已归还；获取失败不释放。Goexit不转换为错误或继续重试，日志后端panic仍传播，二者未产生普通结果时不补Adaptive成功/失败记录。TokenBucket/SlidingWindow拒绝也消耗尝试，最后被拒绝不保留此前业务错误。有值版本失败返回零值。父取消在尝试前/退避时检查；nil错误优先成功，最后业务失败仍包装业务错误，最后等许可失败也可能包装取消，判断用errors.Is。普通业务panic可恢复并重试；Run无参回调拿不到单次context，自有goroutine由业务管理。

## `*RetryChain[T].ExecuteVoid`

```go
func (c *RetryChain[T]) ExecuteVoid(fn func(context.Context) error) error
```

[实现](../../internal/retry/retry_builder.go#L532) · [行为专题](../retry.md)：ExecuteVoid 无返回值版 Execute，fn 只返回 error。

**源码说明与使用片段**（原文）：

ExecuteVoid 无返回值版 Execute，fn 只返回 error。

示例：

	err := retry.NewVoid(ctx).
	    RateLimiter().Rate(10).Per(time.Second).
	    ExecuteVoid(func(ctx context.Context) error {
	        return sendMessage(ctx, msg)
	    })


**参数说明**：

- `fn` (`func(context.Context) error`)：每次尝试同步调用的业务函数，接收该次 context 并返回结果与错误；nil 错误结束重试，panic 转为 PanicError 后按重试策略处理。须幂等且响应取消。ExecuteVoid/RunVoid 仅返回 error。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecuteVoid(fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：单次执行内同步依次尝试，每次终端新建独立限流器，不能限制跨 Execute 总流量；配置固定时并发终端的许可、尝试次数、错误及单次子context仍独立，配置方法不能与执行并发，业务闭包共享数据由应用同步。RateLimiter/Adaptive及其分片每次成功获取许可后以局部defer释放，包含Goexit和日志后端panic，正常路径仍先记录Adaptive结果再释放，退避前已归还；获取失败不释放。Goexit不转换为错误或继续重试，日志后端panic仍传播，二者未产生普通结果时不补Adaptive成功/失败记录。TokenBucket/SlidingWindow拒绝也消耗尝试，最后被拒绝不保留此前业务错误。有值版本失败返回零值。父取消在尝试前/退避时检查；nil错误优先成功，最后业务失败仍包装业务错误，最后等许可失败也可能包装取消，判断用errors.Is。普通业务panic可恢复并重试；Run无参回调拿不到单次context，自有goroutine由业务管理。

## `*RetryChain[T].Exponential`

```go
func (c *RetryChain[T]) Exponential() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L138) · [行为专题](../retry.md)：Exponential 设置指数退避策略（默认）。退避公式：min(initialBackoff * 2^attempt, maxBackoff)。

**源码说明与使用片段**（原文）：

Exponential 设置指数退避策略（默认）。退避公式：min(initialBackoff \* 2^attempt, maxBackoff)。 每次重试等待时间翻倍，直至达到 maxBackoff 上限。

示例：

	retry.New[string](ctx).Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second)


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Exponential()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：只切换退避类型，不重置次数、时长或限流配置。Linear 固定使用 initialBackoff，忽略 maxBackoff；Exponential 逐次翻倍并受正数上限约束。首次尝试之前不退避，等待响应父 context。

## `*RetryChain[T].Limit`

```go
func (c *RetryChain[T]) Limit(n int) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L381) · [行为专题](../retry.md)：Limit 设置滑动窗口在指定时间窗口内允许的最大请求数。

**源码说明与使用片段**（原文）：

Limit 设置滑动窗口在指定时间窗口内允许的最大请求数。

示例：

	.SlidingWindow().Limit(100).Window(10*time.Second)  // 每 10 秒最多 100 次


**参数说明**：

- `n` (`int`)：SlidingWindow 窗口内最大请求额度；非正值使用 100。分片模式分配总额度，但每片至少 1。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Limit(n)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 SlidingWindow 模式读取；非正 Limit 使用 100，非正 Window 使用 1 秒，DefaultLimit/DefaultWindow 分别清零相应字段以恢复这些值。分片按总 Limit 分配且每片至少 1；每次 Execute 都新建空窗口，不能跨调用累计。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.swLimit = n
```

## `*RetryChain[T].Linear`

```go
func (c *RetryChain[T]) Linear() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L148) · [行为专题](../retry.md)：Linear 设置线性退避策略，每次重试等待相同的 backoff 时间。

**源码说明与使用片段**（原文）：

Linear 设置线性退避策略，每次重试等待相同的 backoff 时间。

示例：

	retry.NewVoid(ctx).Linear().MaxRetries(5).Backoff(1*time.Second, 0)


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Linear()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：只切换退避类型，不重置次数、时长或限流配置。Linear 固定使用 initialBackoff，忽略 maxBackoff；Exponential 逐次翻倍并受正数上限约束。首次尝试之前不退避，等待响应父 context。

## `*RetryChain[T].Logger`

```go
func (c *RetryChain[T]) Logger(l core.Logger) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L452) · [行为专题](../retry.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*RetryChain[T].MaxRetries`

```go
func (c *RetryChain[T]) MaxRetries(n int) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L162) · [行为专题](../retry.md)：MaxRetries 设置最大重试次数（总执行次数 = MaxRetries + 1）。

**源码说明与使用片段**（原文）：

MaxRetries 设置最大重试次数（总执行次数 = MaxRetries + 1）。

n: 最大重试次数，n \< 0 时设为 0。

示例：

	.MaxRetries(3)  // 最多 3 次重试 = 共 4 次尝试


**参数说明**：

- `n` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxRetries(n)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：负次数按 0，DefaultMaxRetries 设为 3；初次尝试不计入重试次数，最多次数还受 context 与限流约束。限流拒绝也占用尝试次数，不保证回调一定执行 n+1 次。日志和耗尽错误中的总次数先转为 uint 再加 1，兼容最大 int；这不缩小执行预算，生产应配置可承受的有限次数及父 context 期限。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.maxRetries = n
```

## `*RetryChain[T].MaxWorker`

```go
func (c *RetryChain[T]) MaxWorker(n int) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L426) · [行为专题](../retry.md)：MaxWorker 设置 AdaptiveRateLimiter 的最大并发度（负载高时不会超过此值）。

**源码说明与使用片段**（原文）：

MaxWorker 设置 AdaptiveRateLimiter 的最大并发度（负载高时不会超过此值）。

示例：

	.Adaptive().MinWorker(10).MaxWorker(200)


**参数说明**：

- `n` (`int`)：MaxWorker 设置 AdaptiveRateLimiter 的最大并发度（负载高时不会超过此值）。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxWorker(n)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 Adaptive 模式使用；非正最小值按 1，非正最大值按有效最小值的 10 倍，Default* 清零相应字段。最大值低于最小值时构造器提升至最小值；初始容量为二者中点。配置应避免整数溢出，单次 Execute 的尝试依次执行，不提供跨调用共享并发控制。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.adMaxRate = n
```

## `*RetryChain[T].MinWorker`

```go
func (c *RetryChain[T]) MinWorker(n int) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L416) · [行为专题](../retry.md)：MinWorker 设置 AdaptiveRateLimiter 的最小并发度（负载低时不会低于此值）。

**源码说明与使用片段**（原文）：

MinWorker 设置 AdaptiveRateLimiter 的最小并发度（负载低时不会低于此值）。

示例：

	.Adaptive().MinWorker(5).MaxWorker(100)  // 并发度 5~100 自适应


**参数说明**：

- `n` (`int`)：MinWorker 设置 AdaptiveRateLimiter 的最小并发度（负载低时不会低于此值）。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MinWorker(n)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 Adaptive 模式使用；非正最小值按 1，非正最大值按有效最小值的 10 倍，Default* 清零相应字段。最大值低于最小值时构造器提升至最小值；初始容量为二者中点。配置应避免整数溢出，单次 Execute 的尝试依次执行，不提供跨调用共享并发控制。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.adMinRate = n
```

## `*RetryChain[T].Per`

```go
func (c *RetryChain[T]) Per(d time.Duration) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L322) · [行为专题](../retry.md)：Per 设置 RateLimiter 时间窗口大小。

**源码说明与使用片段**（原文）：

Per 设置 RateLimiter 时间窗口大小。

示例：

	.RateLimiter().Rate(10).Per(time.Minute)  // 每分钟 10 次


**参数说明**：

- `d` (`time.Duration`)：RateLimiter 的补充时间窗口；非正值恢复 1 秒，不影响 TokenBucket 每秒生成令牌的单位。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Per(d)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 RateLimiter 模式读取此窗口，非正值及 DefaultPer 在执行时使用 1 秒；TokenBucket 的 Rate 仍按秒计算。设置不会切换限流模式；RateLimiter 每次业务结束会归还令牌，不能据窗口推断严格的业务 QPS。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.rlPer = d
```

## `*RetryChain[T].PerCallTimeout`

```go
func (c *RetryChain[T]) PerCallTimeout(d time.Duration) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L195) · [行为专题](../retry.md)：PerCallTimeout 设置每次 fn 调用的超时时间。超时会取消该次调用的 context；

**源码说明与使用片段**（原文）：

PerCallTimeout 设置每次 fn 调用的超时时间。超时会取消该次调用的 context； fn 需要响应取消并返回错误，父 context 仍有效时才会继续重试。 0 表示不设置每次调用超时（默认）。

示例：

	// 每次调用最多 2 秒，超时后自动重试
	.PerCallTimeout(2 * time.Second)


**参数说明**：

- `d` (`time.Duration`)：每次业务调用的 context 超时；正数生效，非正值不限时。业务函数必须响应 context 后返回，超时本身不会启动额外 goroutine 或强制停止业务。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.PerCallTimeout(d)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：正数为每次业务调用独立创建 context 期限，父期限更早时从父期限，在该次回调返回后立即取消；非正值及 DefaultPerCallTimeout 不设置单次期限。不包括获取限流许可或退避等待；同步调用不强制终止业务，也不自动把 nil 错误替换为超时错误。Run 的无参回调拿不到此 context。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.perCallTimeout = d
```

## `*RetryChain[T].Rate`

```go
func (c *RetryChain[T]) Rate(n int) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L312) · [行为专题](../retry.md)：Rate 设置 RateLimiter 每 Per 窗口额度或 TokenBucket 每秒令牌数。

**源码说明与使用片段**（原文）：

Rate 设置 RateLimiter 每 Per 窗口额度或 TokenBucket 每秒令牌数。 n \<= 0 时使用 10；与模式选择的调用顺序无关。SlidingWindow 使用 Limit 设置额度。

示例：

	.RateLimiter().Rate(100).Per(time.Second)  // 每秒 100 次


**参数说明**：

- `n` (`int`)：RateLimiter 每个 Per 窗口的补充额度，或 TokenBucket 每秒生成的令牌数；非正值使用 10。分片 RateLimiter 分配总额度（每片至少 1），分片 TokenBucket 使用每片额度。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Rate(n)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：模式与参数保存至本次构建器，后一次同字段设置覆盖前一次；每个终端调用使用独立限流器。分片 RateLimiter/SlidingWindow 平分总额度且每片至少 1，TokenBucket 采用每片速率与容量。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.rlRate = n
```

## `*RetryChain[T].RateLimiter`

```go
func (c *RetryChain[T]) RateLimiter() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L301) · [行为专题](../retry.md)：RateLimiter 启用 RateLimiter 限流模式。每次重试前阻塞等待令牌，被限流时阻塞。

**源码说明与使用片段**（原文）：

RateLimiter 启用 RateLimiter 限流模式。每次重试前阻塞等待令牌，被限流时阻塞。 配置方法：Rate / Per / Burst / Shards。

示例：

	retry.New[string](ctx).Exponential().MaxRetries(5).Backoff(100*time.Millisecond, 5*time.Second).
	    RateLimiter().Rate(10).Per(time.Second).Execute(fn)


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RateLimiter()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅选择后续执行的模式，不清空其他模式字段；后一次模式选择覆盖前一次，每次终端创建全新额度。RateLimiter 阻塞等许可并在调用后 Release；TokenBucket/SlidingWindow 非阻塞拒绝，拒绝占尝试并进入退避。分片 RateLimiter/SlidingWindow 分配总量且每片至少 1，TokenBucket 每片使用完整 Rate/Capacity。

## `*RetryChain[T].Run`

```go
func (c *RetryChain[T]) Run(fn func() error) error
```

[实现](../../internal/retry/retry_builder.go#L556) · [行为专题](../retry.md)：Run 统一执行方法，fn 签名为 func() error，不接收链内或单次调用的 context。

**源码说明与使用片段**（原文）：

Run 统一执行方法，fn 签名为 func() error，不接收链内或单次调用的 context。 需要业务响应取消或单次超时时，使用 ExecuteVoid / RunVoid 并在回调内检查 context。

示例：

	err := retry.NewVoid(ctx).
	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
	    Run(func() error {
	        return doSomething()
	    })

	err := retry.NewVoid(ctx).
	    RateLimiter().Rate(10).Per(time.Second).
	    Run(func() error {
	        return callAPI()
	    })


**参数说明**：

- `fn` (`func() error`)：每次尝试同步调用的无参数函数，仅返回 error；nil 表示成功，panic 转为 PanicError 后按策略处理。此签名不传递链内 context 或单次超时 context；需要响应单次取消时使用 ExecuteVoid/RunVoid，并使业务幂等。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：单次执行内同步依次尝试，每次终端新建独立限流器，不能限制跨 Execute 总流量；配置固定时并发终端的许可、尝试次数、错误及单次子context仍独立，配置方法不能与执行并发，业务闭包共享数据由应用同步。RateLimiter/Adaptive及其分片每次成功获取许可后以局部defer释放，包含Goexit和日志后端panic，正常路径仍先记录Adaptive结果再释放，退避前已归还；获取失败不释放。Goexit不转换为错误或继续重试，日志后端panic仍传播，二者未产生普通结果时不补Adaptive成功/失败记录。TokenBucket/SlidingWindow拒绝也消耗尝试，最后被拒绝不保留此前业务错误。有值版本失败返回零值。父取消在尝试前/退避时检查；nil错误优先成功，最后业务失败仍包装业务错误，最后等许可失败也可能包装取消，判断用errors.Is。普通业务panic可恢复并重试；Run无参回调拿不到单次context，自有goroutine由业务管理。

## `*RetryChain[T].RunVoid`

```go
func (c *RetryChain[T]) RunVoid(fn func(context.Context) error) error
```

[实现](../../internal/retry/retry_builder.go#L573) · [行为专题](../retry.md)：RunVoid 与 ExecuteVoid 相同，fn 签名为 func(context.Context) error，使用 Run* 统一命名。

**源码说明与使用片段**（原文）：

RunVoid 与 ExecuteVoid 相同，fn 签名为 func(context.Context) error，使用 Run\* 统一命名。

示例：

	err := retry.NewVoid(ctx).
	    TokenBucket().Rate(5).Capacity(20).
	    RunVoid(func(ctx context.Context) error {
	        return callAPI(ctx, req)
	    })


**参数说明**：

- `fn` (`func(context.Context) error`)：每次尝试同步调用的业务函数，接收该次 context 并返回结果与错误；nil 错误结束重试，panic 转为 PanicError 后按重试策略处理。须幂等且响应取消。ExecuteVoid/RunVoid 仅返回 error。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RunVoid(fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：单次执行内同步依次尝试，每次终端新建独立限流器，不能限制跨 Execute 总流量；配置固定时并发终端的许可、尝试次数、错误及单次子context仍独立，配置方法不能与执行并发，业务闭包共享数据由应用同步。RateLimiter/Adaptive及其分片每次成功获取许可后以局部defer释放，包含Goexit和日志后端panic，正常路径仍先记录Adaptive结果再释放，退避前已归还；获取失败不释放。Goexit不转换为错误或继续重试，日志后端panic仍传播，二者未产生普通结果时不补Adaptive成功/失败记录。TokenBucket/SlidingWindow拒绝也消耗尝试，最后被拒绝不保留此前业务错误。有值版本失败返回零值。父取消在尝试前/退避时检查；nil错误优先成功，最后业务失败仍包装业务错误，最后等许可失败也可能包装取消，判断用errors.Is。普通业务panic可恢复并重试；Run无参回调拿不到单次context，自有goroutine由业务管理。

## `*RetryChain[T].Shards`

```go
func (c *RetryChain[T]) Shards(n int) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L441) · [行为专题](../retry.md)：Shards 设置本次执行所创建限流器的分片数（4 种模式通用）。

**源码说明与使用片段**（原文）：

Shards 设置本次执行所创建限流器的分片数（4 种模式通用）。 单次 Execute 的尝试依次执行，分片不会使这些尝试并行，也不共享其他 Execute 的额度。 n \<= 0 时自动使用 DefaultShardCount()。

示例：

	.RateLimiter().Rate(10000).Per(time.Second).Shards(16)
	.SlidingWindow().Limit(500).Window(time.Second).Shards(8)


**参数说明**：

- `n` (`int`)：本次 Execute 内部限流器的分片数量；非正值使用 DefaultShardCount()，1 使用单片，DefaultShards 则关闭分片。尝试仍依次执行，每次 Execute 的额度独立，分片不提供跨调用共享限流。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：模式与参数保存至本次构建器，后一次同字段设置覆盖前一次；每个终端调用使用独立限流器。分片 RateLimiter/SlidingWindow 平分总额度且每片至少 1，TokenBucket 采用每片速率与容量。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.rlShards = n
c.swShards = n
c.adShards = n
```

## `*RetryChain[T].SlidingWindow`

```go
func (c *RetryChain[T]) SlidingWindow() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L371) · [行为专题](../retry.md)：SlidingWindow 启用滑动窗口限流模式。在指定时间窗口内精确限制请求数，比固定窗口更平滑。

**源码说明与使用片段**（原文）：

SlidingWindow 启用滑动窗口限流模式。在指定时间窗口内精确限制请求数，比固定窗口更平滑。 每次重试前检查窗口计数，超过限制时跳过等待退避重试。 配置方法：Limit / Window / Shards。

示例：

	retry.New[string](ctx).Linear().MaxRetries(5).Backoff(200*time.Millisecond, 0).
	    SlidingWindow().Limit(100).Window(10*time.Second).Shards(8).Execute(fn)


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SlidingWindow()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅选择后续执行的模式，不清空其他模式字段；后一次模式选择覆盖前一次，每次终端创建全新额度。RateLimiter 阻塞等许可并在调用后 Release；TokenBucket/SlidingWindow 非阻塞拒绝，拒绝占尝试并进入退避。分片 RateLimiter/SlidingWindow 分配总量且每片至少 1，TokenBucket 每片使用完整 Rate/Capacity。

## `*RetryChain[T].TokenBucket`

```go
func (c *RetryChain[T]) TokenBucket() *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L346) · [行为专题](../retry.md)：TokenBucket 启用 TokenBucket 令牌桶限流模式。每次重试前检查令牌，拿不到时跳过等待退避重试。

**源码说明与使用片段**（原文）：

TokenBucket 启用 TokenBucket 令牌桶限流模式。每次重试前检查令牌，拿不到时跳过等待退避重试。 配置方法：Rate(int) / Capacity(float64) / Shards(int)。

示例：

	retry.NewVoid(ctx).Exponential().MaxRetries(10).Backoff(100*time.Millisecond, 5*time.Second).
	    TokenBucket().Rate(10).Capacity(20).ExecuteVoid(fn)


**参数说明**：无显式参数。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TokenBucket()
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅选择后续执行的模式，不清空其他模式字段；后一次模式选择覆盖前一次，每次终端创建全新额度。RateLimiter 阻塞等许可并在调用后 Release；TokenBucket/SlidingWindow 非阻塞拒绝，拒绝占尝试并进入退避。分片 RateLimiter/SlidingWindow 分配总量且每片至少 1，TokenBucket 每片使用完整 Rate/Capacity。

## `*RetryChain[T].Window`

```go
func (c *RetryChain[T]) Window(d time.Duration) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L391) · [行为专题](../retry.md)：Window 设置滑动窗口的时间窗口大小。

**源码说明与使用片段**（原文）：

Window 设置滑动窗口的时间窗口大小。

示例：

	.SlidingWindow().Limit(50).Window(5*time.Second)  // 每 5 秒最多 50 次


**参数说明**：

- `d` (`time.Duration`)：SlidingWindow 计数的时间范围；非正值使用 1 秒。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Window(d)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：仅 SlidingWindow 模式读取；非正 Limit 使用 100，非正 Window 使用 1 秒，DefaultLimit/DefaultWindow 分别清零相应字段以恢复这些值。分片按总 Limit 分配且每片至少 1；每次 Execute 都新建空窗口，不能跨调用累计。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.swWindow = d
```

## `BindRetryToWorker`

```go
func BindRetryToWorker(
	ctx context.Context,
	backend WorkerPoolBackend,
	fn func(ctx context.Context) error,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) error
```

[实现](../../internal/retry/helper.go#L85) · [行为专题](../retry.md)：BindRetryToWorker 向 worker 池提交任务，并在遇到 ErrSubmitTimeout 时自动重试。

**源码说明与使用片段**（原文）：

BindRetryToWorker 向 worker 池提交任务，并在遇到 ErrSubmitTimeout 时自动重试。 适用于高负载场景下提交任务时池满需要重试的情况。 只重试提交，不主动执行业务函数；提交成功即返回，不等待业务完成。 后端必须保证提交超时代表任务未被接受，否则重试可能重复执行。后端 panic 直接传播。

参数：

  - ctx：上下文，取消后终止重试
  - backend：WorkerPoolBackend 适配器；泛型 Pool 的 Submit 任务回调签名不同，不能直接实现此接口
  - fn：要提交执行的任务函数
  - maxRetries：最大重试次数，负数按 0 处理
  - initialBackoff：初始退避时间
  - maxBackoff：最大退避时间上限

使用示例：

	// 向协程池提交任务，提交超时时自动退避重试
	err := retry.BindRetryToWorker(ctx, pool, func(ctx context.Context) error {
	    return processItem(ctx, item)
	}, 3, 10*time.Millisecond, 1*time.Second)


**参数说明**：

- `ctx` (`context.Context`)：上下文，取消后终止重试
- `backend` (`WorkerPoolBackend`)：非 nil 的提交适配器，同步调用其 Submit；泛型 Pool 的 Submit 签名不同，须显式适配。只有 errors.Is(err, ErrSubmitTimeout) 才退避重试，其他错误立即返回。
- `fn` (`func(ctx context.Context) error`)：原样传给 backend.Submit 的任务函数；是否异步执行、如何传播执行错误以及 panic 规则均由后端决定。本辅助函数只重试提交，不主动执行或重试业务函数，也不恢复后端 panic。
- `maxRetries` (`int`)：首次提交超时之后允许的重提次数；负数按 0，最多调用 backend.Submit 共 maxRetries+1 次。其他提交错误不重试。
- `initialBackoff` (`time.Duration`)：首次提交超时后的指数退避起始时长，后续逐次翻倍并按 maxBackoff 截断；非正值不等待。等待期间响应 ctx 取消。
- `maxBackoff` (`time.Duration`)：指数退避的正数上限；非正值不设置上限，initialBackoff 非正时也不等待。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.BindRetryToWorker(ctx, backend, fn, maxRetries, initialBackoff, maxBackoff)
```

**生产建议**：为提交阶段设置有截止时间的 context，并确认后端超时未接受任务；对成功提交的任务单独管理等待、业务错误和关闭。通过稳定任务标识防止后端不确定接收状态造成重复执行。

**注意事项**：只对 ErrSubmitTimeout（含包装错误）重试提交，负次数按 0，非正退避不等待；只在后续还可尝试时退避。每次提交前检查父 context，不中断阻塞的后端 Submit。提交成功立即返回 nil，即使后端刚取消父 context；其他提交错误原样返回，最终超时包装该错误，尚有尝试时下一轮/退避才返回父取消。后端必须约定超时意味着未接受任务，否则重提可能重复执行；不主动调用业务或等待完成，任务错误与生命周期须另管。后端 panic 直接传播。

## `New`

```go
func New[T any](ctx context.Context) *RetryChain[T]
```

[实现](../../internal/retry/retry_builder.go#L104) · [行为专题](../retry.md)：New 创建带返回值的重试链式构建器。

**源码说明与使用片段**（原文）：

New 创建带返回值的重试链式构建器。

示例：

	val, err := retry.New[string](ctx).
	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
	    Execute(myFunc)


**参数说明**：

- `ctx` (`context.Context`)：原样保存的父 context，终端执行时须非 nil；构造和执行不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用构建器 Context(ctx)；正数 PerCallTimeout 仅为每次业务派生期限。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
retry.New[T](ctx)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：创建独立可变构建器，不立即执行或分配限流器。默认指数退避、3 次重试、100ms 起始/30s 上限、无单次超时和限流；原样保存父 context，执行时须非 nil，构造及 Execute 不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用 Context(ctx)。反复 Execute 每次新建独立限流器；不能与配置修改并发，也不以重新调用构造器维持跨调用额度。

## `NewVoid`

```go
func NewVoid(ctx context.Context) *RetryChain[struct{}]
```

[实现](../../internal/retry/retry_builder.go#L120) · [行为专题](../retry.md)：NewVoid 创建无返回值的重试链式构建器（fn 只返回 error）。

**源码说明与使用片段**（原文）：

NewVoid 创建无返回值的重试链式构建器（fn 只返回 error）。

示例：

	err := retry.NewVoid(ctx).
	    Linear().MaxRetries(5).Backoff(200*time.Millisecond, 0).
	    ExecuteVoid(myFunc)


**参数说明**：

- `ctx` (`context.Context`)：原样保存的父 context，终端执行时须非 nil；构造和执行不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用构建器 Context(ctx)；正数 PerCallTimeout 仅为每次业务派生期限。

**返回值签名**：`*RetryChain[struct{}]`。

**使用示例**（已初始化参数的调用片段）：

```go
retry.NewVoid(ctx)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：创建独立可变构建器，不立即执行或分配限流器。默认指数退避、3 次重试、100ms 起始/30s 上限、无单次超时和限流；原样保存父 context，执行时须非 nil，构造及 Execute 不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用 Context(ctx)。反复 Execute 每次新建独立限流器；不能与配置修改并发，也不以重新调用构造器维持跨调用额度。

## `RetryWithBackoff`

```go
func RetryWithBackoff[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) (T, error)
```

[实现](../../internal/retry/retry.go#L55) · [行为专题](../retry.md)：RetryWithBackoff 使用指数退避策略执行 fn，最多执行 maxRetries+1 次。

**源码说明与使用片段**（原文）：

RetryWithBackoff 使用指数退避策略执行 fn，最多执行 maxRetries+1 次。 退避时间计算公式：min(initialBackoff \* 2^attempt, maxBackoff)。 如果 initialBackoff 为 0，则不等待直接重试。

参数：

  - ctx：上下文，取消后终止重试
  - fn：要执行的函数
  - maxRetries：最大重试次数（负数按 0 处理；总执行次数 = maxRetries + 1）
  - initialBackoff：初始退避时间
  - maxBackoff：最大退避时间上限

使用示例：

	// 调用 RPC，最多重试3次（共4次尝试），退避从100ms开始指数增长到最多5s
	result, err := retry.RetryWithBackoff(ctx, func(ctx context.Context) (*Response, error) {
	    return rpcClient.Call(ctx, request)
	}, 3, 100*time.Millisecond, 5*time.Second)


**参数说明**：

- `ctx` (`context.Context`)：上下文，取消后终止重试
- `fn` (`func(ctx context.Context) (T, error)`)：每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。
- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。
- `initialBackoff` (`time.Duration`)：首次重试前的等待时长；线性模式固定使用此值，指数模式逐次翻倍。非正值不等待，持续失败时可能快速重试。
- `maxBackoff` (`time.Duration`)：指数退避的正数时长上限；非正值表示不设置此上限。链式 Linear 忽略它并使用固定 initial。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.RetryWithBackoff[T](ctx, fn, maxRetries, initialBackoff, maxBackoff)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：每次尝试前检查父 context，同步执行 fn；nil 错误立即成功，即使回调期间父 context 已取消也不替换成功返回。panic 转为 PanicError 并按普通失败重试；最终失败包装最后一次错误，有值版本丢弃失败回调返回的值并返回零值。负次数按 0，非正退避不等待，指数计算饱和而不溢出；无随机抖动。Result 变体不设置 Occupied。

## `RetryWithBackoffResult`

```go
func RetryWithBackoffResult[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) core.Result[T]
```

[实现](../../internal/retry/retry.go#L136) · [行为专题](../retry.md)：RetryWithBackoffResult 与 RetryWithBackoff 相同，但返回 Result[T] 而非两个返回值。

**源码说明与使用片段**（原文）：

RetryWithBackoffResult 与 RetryWithBackoff 相同，但返回 Result\[T] 而非两个返回值。

参数：

  - ctx：上下文
  - fn：要执行的函数
  - maxRetries：最大重试次数
  - initialBackoff：初始退避时间
  - maxBackoff：最大退避时间上限

使用示例：

	// 返回 Result 类型便于统一处理
	r := retry.RetryWithBackoffResult(ctx, fn, 3, 100*time.Millisecond, 5*time.Second)
	if !r.Ok() {
	    log.Printf("重试失败: %v", r.Err)
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `fn` (`func(ctx context.Context) (T, error)`)：每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。
- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。
- `initialBackoff` (`time.Duration`)：首次重试前的等待时长；线性模式固定使用此值，指数模式逐次翻倍。非正值不等待，持续失败时可能快速重试。
- `maxBackoff` (`time.Duration`)：指数退避的正数时长上限；非正值表示不设置此上限。链式 Linear 忽略它并使用固定 initial。

**返回值签名**：`core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
retry.RetryWithBackoffResult[T](ctx, fn, maxRetries, initialBackoff, maxBackoff)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：每次尝试前检查父 context，同步执行 fn；nil 错误立即成功，即使回调期间父 context 已取消也不替换成功返回。panic 转为 PanicError 并按普通失败重试；最终失败包装最后一次错误，有值版本丢弃失败回调返回的值并返回零值。负次数按 0，非正退避不等待，指数计算饱和而不溢出；无随机抖动。Result 变体不设置 Occupied。

## `RetryWithBackoffVoid`

```go
func RetryWithBackoffVoid(
	ctx context.Context,
	fn func(ctx context.Context) error,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
) error
```

[实现](../../internal/retry/retry.go#L107) · [行为专题](../retry.md)：RetryWithBackoffVoid 与 RetryWithBackoff 相同，但 fn 只返回 error。

**源码说明与使用片段**（原文）：

RetryWithBackoffVoid 与 RetryWithBackoff 相同，但 fn 只返回 error。

参数：

  - ctx：上下文，取消后终止重试
  - fn：要执行的函数（只返回 error）
  - maxRetries：最大重试次数
  - initialBackoff：初始退避时间
  - maxBackoff：最大退避时间上限

使用示例：

	// 重试发送消息
	err := retry.RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
	    return kafkaProducer.Send(ctx, msg)
	}, 3, 100*time.Millisecond, 5*time.Second)


**参数说明**：

- `ctx` (`context.Context`)：上下文，取消后终止重试
- `fn` (`func(ctx context.Context) error`)：每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。
- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。
- `initialBackoff` (`time.Duration`)：首次重试前的等待时长；线性模式固定使用此值，指数模式逐次翻倍。非正值不等待，持续失败时可能快速重试。
- `maxBackoff` (`time.Duration`)：指数退避的正数时长上限；非正值表示不设置此上限。链式 Linear 忽略它并使用固定 initial。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.RetryWithBackoffVoid(ctx, fn, maxRetries, initialBackoff, maxBackoff)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：每次尝试前检查父 context，同步执行 fn；nil 错误立即成功，即使回调期间父 context 已取消也不替换成功返回。panic 转为 PanicError 并按普通失败重试；最终失败包装最后一次错误，有值版本丢弃失败回调返回的值并返回零值。负次数按 0，非正退避不等待，指数计算饱和而不溢出；无随机抖动。Result 变体不设置 Occupied。

## `RetryWithConfig`

```go
func RetryWithConfig[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	opts ...TimeoutOpt,
) (T, error)
```

[实现](../../internal/retry/timeout.go#L122) · [行为专题](../retry.md)：RetryWithConfig 支持每次调用超时的指数退避重试。

**源码说明与使用片段**（原文）：

RetryWithConfig 支持每次调用超时的指数退避重试。 相比 RetryWithBackoff，增加了对每次 fn 调用的超时控制， 父 context 取消后终止重试；单次调用超时或取消而父 context 仍有效时继续重试。 超时通过 context 协作实现，fn 必须响应 context 取消才能及时返回。

参数：

  - ctx：上下文，取消后终止重试
  - fn：要执行的函数
  - maxRetries：最大重试次数，负数按 0 处理
  - initialBackoff：初始退避时间
  - maxBackoff：最大退避时间上限
  - opts：可选的 TimeoutOpt{PerCallTimeout}，设置每次调用的超时

使用示例：

	// 每次调用最多2秒，最多重试3次，退避100ms到5s
	result, err := retry.RetryWithConfig(ctx, func(ctx context.Context) (*Data, error) {
	    return fetchData(ctx, id)
	}, 3, 100*time.Millisecond, 5*time.Second,
	    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})
	if err != nil {
	    // 可能的重试耗尽错误
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文，取消后终止重试
- `fn` (`func(ctx context.Context) (T, error)`)：每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。
- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。
- `initialBackoff` (`time.Duration`)：首次重试前的等待时长；线性模式固定使用此值，指数模式逐次翻倍。非正值不等待，持续失败时可能快速重试。
- `maxBackoff` (`time.Duration`)：指数退避的正数时长上限；非正值表示不设置此上限。链式 Linear 忽略它并使用固定 initial。
- `opts` (`...TimeoutOpt`)：可选的单次调用超时配置；仅使用第一个元素的正数 PerCallTimeout，其余元素忽略。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.RetryWithConfig[T](ctx, fn, maxRetries, initialBackoff, maxBackoff, opts...)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：只读取 opts 第一个元素的正数 PerCallTimeout；每次调用单独派生 context，以 defer 在单次调用结束时取消，含runtime.Goexit及日志后端panic展开，不取消父context。Goexit结束调用goroutine，不转换为错误、不继续重试；日志后端panic仍传播。超时不强制结束业务，也不自动覆盖 nil 错误。父 context 仍存活时，回调返回的 Canceled/DeadlineExceeded 仍可重试；父已取消且回调返回这类错误时直接返回该错误，否则在下一轮/退避中处理父取消。最终失败返回零值并包装最后一次业务错误，普通业务panic可恢复并重试。

## `RetryWithConfigVoid`

```go
func RetryWithConfigVoid(
	ctx context.Context,
	fn func(ctx context.Context) error,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	opts ...TimeoutOpt,
) error
```

[实现](../../internal/retry/timeout.go#L199) · [行为专题](../retry.md)：RetryWithConfigVoid 与 RetryWithConfig 相同，但 fn 只返回 error。

**源码说明与使用片段**（原文）：

RetryWithConfigVoid 与 RetryWithConfig 相同，但 fn 只返回 error。

参数：

  - ctx：上下文
  - fn：要执行的函数（只返回 error）
  - maxRetries：最大重试次数
  - initialBackoff：初始退避时间
  - maxBackoff：最大退避时间上限
  - opts：可选的 TimeoutOpt{PerCallTimeout}

使用示例：

	// 带每次调用超时的重试
	err := retry.RetryWithConfigVoid(ctx, func(ctx context.Context) error {
	    return callExternalAPI(ctx, req)
	}, 3, 100*time.Millisecond, 5*time.Second,
	    retry.TimeoutOpt{PerCallTimeout: 2 * time.Second})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `fn` (`func(ctx context.Context) error`)：每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。
- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。
- `initialBackoff` (`time.Duration`)：首次重试前的等待时长；线性模式固定使用此值，指数模式逐次翻倍。非正值不等待，持续失败时可能快速重试。
- `maxBackoff` (`time.Duration`)：指数退避的正数时长上限；非正值表示不设置此上限。链式 Linear 忽略它并使用固定 initial。
- `opts` (`...TimeoutOpt`)：可选的单次调用超时配置；仅使用第一个元素的正数 PerCallTimeout，其余元素忽略。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.RetryWithConfigVoid(ctx, fn, maxRetries, initialBackoff, maxBackoff, opts...)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：只读取 opts 第一个元素的正数 PerCallTimeout；每次调用单独派生 context，以 defer 在单次调用结束时取消，含runtime.Goexit及日志后端panic展开，不取消父context。Goexit结束调用goroutine，不转换为错误、不继续重试；日志后端panic仍传播。超时不强制结束业务，也不自动覆盖 nil 错误。父 context 仍存活时，回调返回的 Canceled/DeadlineExceeded 仍可重试；父已取消且回调返回这类错误时直接返回该错误，否则在下一轮/退避中处理父取消。最终失败返回零值并包装最后一次业务错误，普通业务panic可恢复并重试。

## `RetryWithLinearBackoff`

```go
func RetryWithLinearBackoff[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	backoff time.Duration,
) (T, error)
```

[实现](../../internal/retry/retry.go#L160) · [行为专题](../retry.md)：RetryWithLinearBackoff 使用线性退避策略（固定退避时间）执行 fn。

**源码说明与使用片段**（原文）：

RetryWithLinearBackoff 使用线性退避策略（固定退避时间）执行 fn。 每次重试等待相同的 backoff 时间。

参数：

  - ctx：上下文，取消后终止重试
  - fn：要执行的函数
  - maxRetries：最大重试次数
  - backoff：固定退避时间

使用示例：

	// 每次重试等1秒，最多重试5次
	val, err := retry.RetryWithLinearBackoff(ctx, fn, 5, 1*time.Second)


**参数说明**：

- `ctx` (`context.Context`)：上下文，取消后终止重试
- `fn` (`func(ctx context.Context) (T, error)`)：每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。
- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。
- `backoff` (`time.Duration`)：固定退避时间

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.RetryWithLinearBackoff[T](ctx, fn, maxRetries, backoff)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：每次尝试前检查父 context，同步执行 fn；nil 错误立即成功，即使回调期间父 context 已取消也不替换成功返回。panic 转为 PanicError 并按普通失败重试；最终失败包装最后一次错误，有值版本丢弃失败回调返回的值并返回零值。负次数按 0，非正退避不等待，指数计算饱和而不溢出；无随机抖动。Result 变体不设置 Occupied。

## `RetryWithLinearBackoffResult`

```go
func RetryWithLinearBackoffResult[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
	maxRetries int,
	backoff time.Duration,
) core.Result[T]
```

[实现](../../internal/retry/retry.go#L210) · [行为专题](../retry.md)：RetryWithLinearBackoffResult 与 RetryWithLinearBackoff 相同，但返回 Result[T]。

**源码说明与使用片段**（原文）：

RetryWithLinearBackoffResult 与 RetryWithLinearBackoff 相同，但返回 Result\[T]。

参数：

  - ctx：上下文
  - fn：要执行的函数
  - maxRetries：最大重试次数
  - backoff：固定退避时间

使用示例：

	// 每次等1s，返回 Result 便于链式处理
	r := retry.RetryWithLinearBackoffResult(ctx, fn, 5, 1*time.Second)
	if r.Ok() {
	    process(r.Value)
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `fn` (`func(ctx context.Context) (T, error)`)：每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。
- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。
- `backoff` (`time.Duration`)：固定退避时间

**返回值签名**：`core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
retry.RetryWithLinearBackoffResult[T](ctx, fn, maxRetries, backoff)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：每次尝试前检查父 context，同步执行 fn；nil 错误立即成功，即使回调期间父 context 已取消也不替换成功返回。panic 转为 PanicError 并按普通失败重试；最终失败包装最后一次错误，有值版本丢弃失败回调返回的值并返回零值。负次数按 0，非正退避不等待，指数计算饱和而不溢出；无随机抖动。Result 变体不设置 Occupied。

## `RetryWithLinearBackoffVoid`

```go
func RetryWithLinearBackoffVoid(
	ctx context.Context,
	fn func(ctx context.Context) error,
	maxRetries int,
	backoff time.Duration,
) error
```

[实现](../../internal/retry/retry.go#L183) · [行为专题](../retry.md)：RetryWithLinearBackoffVoid 与 RetryWithLinearBackoff 相同，但 fn 只返回 error。

**源码说明与使用片段**（原文）：

RetryWithLinearBackoffVoid 与 RetryWithLinearBackoff 相同，但 fn 只返回 error。

参数：

  - ctx：上下文
  - fn：要执行的函数（只返回 error）
  - maxRetries：最大重试次数
  - backoff：固定退避时间

使用示例：

	// 每次重试等500ms，最多重试3次
	err := retry.RetryWithLinearBackoffVoid(ctx, func(ctx context.Context) error {
	    return sendEmail(ctx, to, body)
	}, 3, 500*time.Millisecond)


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `fn` (`func(ctx context.Context) error`)：每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。
- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。
- `backoff` (`time.Duration`)：固定退避时间

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.RetryWithLinearBackoffVoid(ctx, fn, maxRetries, backoff)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：每次尝试前检查父 context，同步执行 fn；nil 错误立即成功，即使回调期间父 context 已取消也不替换成功返回。panic 转为 PanicError 并按普通失败重试；最终失败包装最后一次错误，有值版本丢弃失败回调返回的值并返回零值。负次数按 0，非正退避不等待，指数计算饱和而不溢出；无随机抖动。Result 变体不设置 Occupied。

## `WithDeadline`

```go
func WithDeadline[T any](ctx context.Context, deadline time.Time, fn func(ctx context.Context) (T, error)) (T, error)
```

[实现](../../internal/retry/timeout.go#L64) · [行为专题](../retry.md)：WithDeadline 为 fn 提供截止时间上下文，并同步等待 fn 返回。

**源码说明与使用片段**（原文）：

WithDeadline 为 fn 提供截止时间上下文，并同步等待 fn 返回。 截止时间只取消上下文，fn 必须响应取消才能及时返回。

参数：

  - ctx：父上下文
  - deadline：截止时间
  - fn：要执行的函数

使用示例：

	// 为调用设置 5 秒后的截止时间
	deadline := time.Now().Add(5 * time.Second)
	val, err := retry.WithDeadline(ctx, deadline, fn)


**参数说明**：

- `ctx` (`context.Context`)：非 nil 的父 context；值继承到派生 context，已有取消立即传播，更早的父期限优先。即使父已取消仍调用 fn，包装结束只取消派生 context，不取消父 context。
- `deadline` (`time.Time`)：本次同步调用的绝对截止时间，与父期限取更早者；过去时间及零值 time.Time 都使派生 context 立即到期，但仍调用 fn，不自动改写 fn 的返回值或错误。
- `fn` (`func(ctx context.Context) (T, error)`)：在当前 goroutine 同步调用一次的业务函数，接收派生 context，直接保留签名中的值与错误；已取消时仍调用，panic 原样传播。普通返回、panic展开或runtime.Goexit时均取消派生context；自建goroutine不延长其存活时间。回调须非nil并自行响应context，nil函数调用panic也不被恢复。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.WithDeadline[T](ctx, deadline, fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：同步直接调用一次 fn，不捕获 panic、不替回调返回 context 错误，有值版本原样保留同时返回的值与错误。父 context 必须非 nil；即使父已取消、期限已过或 timeout 非正仍调用 fn，回调须检查 context，否则可成功或无限阻塞。保留父值并采用更早期限；普通返回、panic展开及runtime.Goexit均执行defer取消派生context，不取消父context。回调自建goroutine若继续使用该context，会在包装调用结束时收到取消，生命周期仍由业务管理。这些函数只在internal/retry，根包未提供同名入口，外部项目不能直接导入internal；公开重试用Retry/RetryVoid，单次协作期限可用context.WithTimeout/WithDeadline。

## `WithDeadlineVoid`

```go
func WithDeadlineVoid(ctx context.Context, deadline time.Time, fn func(ctx context.Context) error) error
```

[实现](../../internal/retry/timeout.go#L83) · [行为专题](../retry.md)：WithDeadlineVoid 与 WithDeadline 相同，但 fn 只返回 error。

**源码说明与使用片段**（原文）：

WithDeadlineVoid 与 WithDeadline 相同，但 fn 只返回 error。

参数：

  - ctx：父上下文
  - deadline：截止时间
  - fn：要执行的函数（只返回 error）

使用示例：

	// 必须在截止时间前完成
	err := retry.WithDeadlineVoid(ctx, time.Now().Add(30*time.Second), func(ctx context.Context) error {
	    return batchProcess(ctx, items)
	})


**参数说明**：

- `ctx` (`context.Context`)：非 nil 的父 context；值继承到派生 context，已有取消立即传播，更早的父期限优先。即使父已取消仍调用 fn，包装结束只取消派生 context，不取消父 context。
- `deadline` (`time.Time`)：本次同步调用的绝对截止时间，与父期限取更早者；过去时间及零值 time.Time 都使派生 context 立即到期，但仍调用 fn，不自动改写 fn 的返回值或错误。
- `fn` (`func(ctx context.Context) error`)：在当前 goroutine 同步调用一次的业务函数，接收派生 context，直接保留签名中的值与错误；已取消时仍调用，panic 原样传播。普通返回、panic展开或runtime.Goexit时均取消派生context；自建goroutine不延长其存活时间。回调须非nil并自行响应context，nil函数调用panic也不被恢复。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.WithDeadlineVoid(ctx, deadline, fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：同步直接调用一次 fn，不捕获 panic、不替回调返回 context 错误，有值版本原样保留同时返回的值与错误。父 context 必须非 nil；即使父已取消、期限已过或 timeout 非正仍调用 fn，回调须检查 context，否则可成功或无限阻塞。保留父值并采用更早期限；普通返回、panic展开及runtime.Goexit均执行defer取消派生context，不取消父context。回调自建goroutine若继续使用该context，会在包装调用结束时收到取消，生命周期仍由业务管理。这些函数只在internal/retry，根包未提供同名入口，外部项目不能直接导入internal；公开重试用Retry/RetryVoid，单次协作期限可用context.WithTimeout/WithDeadline。

## `WithTimeout`

```go
func WithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (T, error)) (T, error)
```

[实现](../../internal/retry/timeout.go#L26) · [行为专题](../retry.md)：WithTimeout 为 fn 提供超时上下文，并同步等待 fn 返回。

**源码说明与使用片段**（原文）：

WithTimeout 为 fn 提供超时上下文，并同步等待 fn 返回。 超时只取消上下文，fn 必须响应取消才能及时返回。

参数：

  - ctx：父上下文
  - timeout：超时时间
  - fn：要执行的函数

使用示例：

	// 为调用设置 3 秒的上下文超时
	val, err := retry.WithTimeout(ctx, 3*time.Second, func(ctx context.Context) (string, error) {
	    return httpGet(ctx, url)
	})


**参数说明**：

- `ctx` (`context.Context`)：非 nil 的父 context；值继承到派生 context，已有取消立即传播，更早的父期限优先。即使父已取消仍调用 fn，包装结束只取消派生 context，不取消父 context。
- `timeout` (`time.Duration`)：为本次同步调用派生 context 的时长；非正值立即到期，但仍调用 fn。返回值完全来自 fn，超时不自动替换其返回错误，也不能强制停止业务。
- `fn` (`func(ctx context.Context) (T, error)`)：在当前 goroutine 同步调用一次的业务函数，接收派生 context，直接保留签名中的值与错误；已取消时仍调用，panic 原样传播。普通返回、panic展开或runtime.Goexit时均取消派生context；自建goroutine不延长其存活时间。回调须非nil并自行响应context，nil函数调用panic也不被恢复。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.WithTimeout[T](ctx, timeout, fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：同步直接调用一次 fn，不捕获 panic、不替回调返回 context 错误，有值版本原样保留同时返回的值与错误。父 context 必须非 nil；即使父已取消、期限已过或 timeout 非正仍调用 fn，回调须检查 context，否则可成功或无限阻塞。保留父值并采用更早期限；普通返回、panic展开及runtime.Goexit均执行defer取消派生context，不取消父context。回调自建goroutine若继续使用该context，会在包装调用结束时收到取消，生命周期仍由业务管理。这些函数只在internal/retry，根包未提供同名入口，外部项目不能直接导入internal；公开重试用Retry/RetryVoid，单次协作期限可用context.WithTimeout/WithDeadline。

## `WithTimeoutVoid`

```go
func WithTimeoutVoid(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error
```

[实现](../../internal/retry/timeout.go#L45) · [行为专题](../retry.md)：WithTimeoutVoid 与 WithTimeout 相同，但 fn 只返回 error。

**源码说明与使用片段**（原文）：

WithTimeoutVoid 与 WithTimeout 相同，但 fn 只返回 error。

参数：

  - ctx：父上下文
  - timeout：超时时间
  - fn：要执行的函数（只返回 error）

使用示例：

	// 发送请求最多5秒
	err := retry.WithTimeoutVoid(ctx, 5*time.Second, func(ctx context.Context) error {
	    return kafkaProducer.Send(ctx, msg)
	})


**参数说明**：

- `ctx` (`context.Context`)：非 nil 的父 context；值继承到派生 context，已有取消立即传播，更早的父期限优先。即使父已取消仍调用 fn，包装结束只取消派生 context，不取消父 context。
- `timeout` (`time.Duration`)：为本次同步调用派生 context 的时长；非正值立即到期，但仍调用 fn。返回值完全来自 fn，超时不自动替换其返回错误，也不能强制停止业务。
- `fn` (`func(ctx context.Context) error`)：在当前 goroutine 同步调用一次的业务函数，接收派生 context，直接保留签名中的值与错误；已取消时仍调用，panic 原样传播。普通返回、panic展开或runtime.Goexit时均取消派生context；自建goroutine不延长其存活时间。回调须非nil并自行响应context，nil函数调用panic也不被恢复。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
retry.WithTimeoutVoid(ctx, timeout, fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：同步直接调用一次 fn，不捕获 panic、不替回调返回 context 错误，有值版本原样保留同时返回的值与错误。父 context 必须非 nil；即使父已取消、期限已过或 timeout 非正仍调用 fn，回调须检查 context，否则可成功或无限阻塞。保留父值并采用更早期限；普通返回、panic展开及runtime.Goexit均执行defer取消派生context，不取消父context。回调自建goroutine若继续使用该context，会在包装调用结束时收到取消，生命周期仍由业务管理。这些函数只在internal/retry，根包未提供同名入口，外部项目不能直接导入internal；公开重试用Retry/RetryVoid，单次协作期限可用context.WithTimeout/WithDeadline。

## `RetryFn.WithRetry`

```go
func (r RetryFn) WithRetry(maxRetries int) error
```

[实现](../../internal/retry/helper.go#L34) · [行为专题](../retry.md)：WithRetry 执行 fn，最多执行 maxRetries+1 次，自动捕获 panic。

**源码说明与使用片段**（原文）：

WithRetry 执行 fn，最多执行 maxRetries+1 次，自动捕获 panic。

参数：

  - maxRetries：最大重试次数，负数按 0 处理


**参数说明**：

- `maxRetries` (`int`)：首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithRetry(maxRetries)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：同步立即重试，负次数按 0，最多 maxRetries+1 次，无退避、context 或超时；最后一次失败后在递增计数前退出，最大 int 的预算不会因循环计数溢出而多执行。panic 转为 PanicError 后继续重试，最终错误包装最后一次错误。须限制次数并保证幂等，需要可取消等待时使用 RetryWithBackoff 或 RetryChain。

## `WorkerPoolBackend.Submit`

```go
func (instance WorkerPoolBackend) Submit(ctx context.Context, fn func(ctx context.Context) error) error
```

[实现](../../internal/retry/helper.go#L63) · [行为专题](../retry.md)：向适配后端提交返回 error 的任务；Submit 自身 error 表示提交结果，业务错误如何获取由后端约定。泛型 Pool 需要适配器。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(ctx context.Context) error`)：交给适配后端的任务函数，接收执行上下文并返回业务 error；后端如何保存或报告此错误、是否并发及 panic 规则由实现约定。Submit 自身的 error 按后端协议表示提交结果，不能假定它等待并返回业务错误。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Submit(ctx, fn)
```

**生产建议**：为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。

**注意事项**：接口签名为 Submit(ctx,func(ctx) error) error，是否阻塞、取消及关闭行为由注入后端实现决定。Submit 的 error 按后端协议表示提交结果，异步业务错误需按后端约定另行获取；泛型 Pool 需要适配器。后端必须明确接受与拒绝的边界，供 BindRetryToWorker 判断提交超时是否可安全重试。

