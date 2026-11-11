# ratelimit 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [ratelimit](../ratelimit.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*AdaptiveRateLimiter.Acquire`

```go
func (a *AdaptiveRateLimiter) Acquire(ctx context.Context) error
```

[实现](../../internal/ratelimit/adaptive.go#L73) · [行为专题](../ratelimit.md)：Acquire 获取一个并发槽位；有空位时优先获取，无空位时等待许可或 ctx 取消。

**源码说明与使用片段**（原文）：

Acquire 获取一个并发槽位；有空位时优先获取，无空位时等待许可或 ctx 取消。

参数：

  - ctx：非 nil 上下文；无空位时响应取消，已取消但有空位仍可能成功


**参数说明**：

- `ctx` (`context.Context`)：本次获取槽位的非 nil context；固定使用 Block，有空位时已取消仍可成功，无空位时等待归还或取消。Adaptive 没有公开的策略切换方法；分片只等待选中一片，不搜索其他片。仅成功获取后配对同一入口的 Release。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Acquire(ctx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：无定时补充，许可须成功 Acquire 后成对 Release；失败不得释放，多余 Release 可能提前归还他人持有的槽位。Acquire 仅在无可用许可时响应 context，已取消而有空位仍可能成功；不会自动执行或记录业务结果。普通 Adaptive 无 Close 方法。

## `*AdaptiveRateLimiter.RecordFailure`

```go
func (a *AdaptiveRateLimiter) RecordFailure()
```

[实现](../../internal/ratelimit/adaptive.go#L95) · [行为专题](../ratelimit.md)：RecordFailure 记录一次失败调用，用于自适应调整算法。

**源码说明与使用片段**（原文）：

RecordFailure 记录一次失败调用，用于自适应调整算法。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RecordFailure()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：只记录业务完成情况，不获取或释放许可；每次成功 Acquire 仍须配对 Release，每次完成只记录一次。单片累计至少 10 次记录才评估：失败比例>20% 缩容一半且不低于最小值，<10% 按剩余容量的 20%（至少 1）扩容，其他比例不调整；之后该批计数清零。分片记录使用独立轮转，不绑定获取许可的片。

## `*AdaptiveRateLimiter.RecordOk`

```go
func (a *AdaptiveRateLimiter) RecordOk()
```

[实现](../../internal/ratelimit/adaptive.go#L89) · [行为专题](../ratelimit.md)：RecordOk 记录一次成功调用，用于自适应调整算法。

**源码说明与使用片段**（原文）：

RecordOk 记录一次成功调用，用于自适应调整算法。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RecordOk()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：只记录业务完成情况，不获取或释放许可；每次成功 Acquire 仍须配对 Release，每次完成只记录一次。单片累计至少 10 次记录才评估：失败比例>20% 缩容一半且不低于最小值，<10% 按剩余容量的 20%（至少 1）扩容，其他比例不调整；之后该批计数清零。分片记录使用独立轮转，不绑定获取许可的片。

## `*AdaptiveRateLimiter.Release`

```go
func (a *AdaptiveRateLimiter) Release()
```

[实现](../../internal/ratelimit/adaptive.go#L81) · [行为专题](../ratelimit.md)：Release 释放一个并发槽位。

**源码说明与使用片段**（原文）：

Release 释放一个并发槽位。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Release()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：无定时补充，许可须成功 Acquire 后成对 Release；失败不得释放，多余 Release 可能提前归还他人持有的槽位。Acquire 仅在无可用许可时响应 context，已取消而有空位仍可能成功；不会自动执行或记录业务结果。普通 Adaptive 无 Close 方法。

## `*AdaptiveSubBuilder.Build`

```go
func (b *AdaptiveSubBuilder) Build() *AdaptiveRateLimiter
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L327) · [行为专题](../ratelimit.md)：Build 构建 AdaptiveRateLimiter 实例。

**源码说明与使用片段**（原文）：

Build 构建 AdaptiveRateLimiter 实例。


**参数说明**：无显式参数。

**返回值签名**：`*AdaptiveRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次创建独立实例，不回写规范化后的字段；不继承所保存 context 的取消、期限和业务值，RateLimiter 及其分片保留局部日志设置，阻塞获取须另传 context。单片最小值非正按 1，最大值不高于最小值时提升至最小值，初始为两者中点。分片先分配总最小/最大值，每片至少 1，再规范化；许可须成对释放。普通 Adaptive 无 Close 方法且无补充 goroutine，分片版本用完 Close。

## `*AdaptiveSubBuilder.Context`

```go
func (b *AdaptiveSubBuilder) Context(ctx context.Context) *AdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L309) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*AdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*AdaptiveSubBuilder.DefaultLogger`

```go
func (b *AdaptiveSubBuilder) DefaultLogger() *AdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L321) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*AdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*AdaptiveSubBuilder.DefaultMaxWorker`

```go
func (b *AdaptiveSubBuilder) DefaultMaxWorker() *AdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L338) · [行为专题](../ratelimit.md)：DefaultMaxWorker 使用默认最大并发度（core.IO() × 10）。

**源码说明与使用片段**（原文）：

DefaultMaxWorker 使用默认最大并发度（core.IO() × 10）。


**参数说明**：无显式参数。

**返回值签名**：`*AdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*AdaptiveSubBuilder.DefaultMinWorker`

```go
func (b *AdaptiveSubBuilder) DefaultMinWorker() *AdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L332) · [行为专题](../ratelimit.md)：DefaultMinWorker 使用默认最小并发度（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultMinWorker 使用默认最小并发度（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*AdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMinWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*AdaptiveSubBuilder.Logger`

```go
func (b *AdaptiveSubBuilder) Logger(l core.Logger) *AdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L315) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*AdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*AdaptiveSubBuilder.MaxWorker`

```go
func (b *AdaptiveSubBuilder) MaxWorker(n int) *AdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L303) · [行为专题](../ratelimit.md)：MaxWorker 设置并发度的上限阈值。

**源码说明与使用片段**（原文）：

MaxWorker 设置并发度的上限阈值。


**参数说明**：

- `n` (`int`)：MaxWorker 设置并发度的上限阈值。

**返回值签名**：`*AdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxWorker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.maxRate = n
```

## `*AdaptiveSubBuilder.MinWorker`

```go
func (b *AdaptiveSubBuilder) MinWorker(n int) *AdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L297) · [行为专题](../ratelimit.md)：MinWorker 设置最小并发度（负载低时不会低于此值）。

**源码说明与使用片段**（原文）：

MinWorker 设置最小并发度（负载低时不会低于此值）。


**参数说明**：

- `n` (`int`)：MinWorker 设置最小并发度（负载低时不会低于此值）。

**返回值签名**：`*AdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MinWorker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.minRate = n
```

## `*RateLimiter.Acquire`

```go
func (rl *RateLimiter) Acquire(ctx context.Context) error
```

[实现](../../internal/ratelimit/ratelimit.go#L291) · [行为专题](../ratelimit.md)：Acquire 根据当前策略获取令牌。默认 Block 先取可用令牌，无令牌时等待许可或 ctx 取消。

**源码说明与使用片段**（原文）：

Acquire 根据当前策略获取令牌。默认 Block 先取可用令牌，无令牌时等待许可或 ctx 取消。 当限流器通过 Resize 动态调整速率导致内部通道重建时，Acquire 会自动重试获取新令牌。

参数：

  - ctx：非 nil 上下文；Block 仅在无令牌时响应取消，有令牌时可成功，Reject/BlockForce 忽略取消

使用示例：

	if err := rl.Acquire(ctx); err != nil {
	    return err // ctx 取消或限流器已停止
	}
	defer rl.Release()
	doRequest()


**参数说明**：

- `ctx` (`context.Context`)：本次获取许可的非 nil context；Block 只在无可用许可时响应取消，有空位时已取消仍可成功，Reject/BlockForce 忽略取消。分片只访问选中一片，获取失败不得 Release；Token 成功后通过句柄释放。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Acquire(ctx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：按当前策略获取：Reject 无令牌立即报错并忽略 context；BlockForce 忽略取消，直到获取或 Close；Block 在无可用令牌时检查取消，已取消但有令牌仍可成功。成功后按用途决定是否归还；定时补充与 Release 均可增加余额，不能当成严格并发上限或严格窗口次数。Token 成功后只通过句柄 Release。

## `*RateLimiter.Available`

```go
func (rl *RateLimiter) Available() int
```

[实现](../../internal/ratelimit/ratelimit.go#L511) · [行为专题](../ratelimit.md)：Available 返回当前可用令牌数。

**源码说明与使用片段**（原文）：

Available 返回当前可用令牌数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Available()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：Size 返回每 Per 窗口的配置额度，Available 返回当时可用令牌数；突发容量可大于 Size，不能用两者差值推导持有许可数。查询不是额度预留，随后 Acquire 可能阻塞或被拒；关闭后的状态读数不表示实例仍可用。

## `*RateLimiter.Close`

```go
func (rl *RateLimiter) Close()
```

[实现](../../internal/ratelimit/ratelimit.go#L418) · [行为专题](../ratelimit.md)：Close 停止限流器的令牌补充 goroutine 并关闭令牌通道。并发调用均等待关闭完成。

**源码说明与使用片段**（原文）：

Close 停止限流器的令牌补充 goroutine 并关闭令牌通道。并发调用均等待关闭完成。

使用示例：

	rl := ratelimit.NewRateLimiter(100, time.Second)
	defer rl.Close()


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：Stop 委托 Close，重复/并发 Close 等待同一次资源清理完成；停止补充 goroutine 并关闭令牌通道，唤醒等待。不等待业务完成、不取消业务 context；已持有许可的业务仍须自行收尾，之后不可重开实例。

## `*RateLimiter.Release`

```go
func (rl *RateLimiter) Release()
```

[实现](../../internal/ratelimit/ratelimit.go#L379) · [行为专题](../ratelimit.md)：Release 归还令牌到限流器。通常在 defer 中调用。

**源码说明与使用片段**（原文）：

Release 归还令牌到限流器。通常在 defer 中调用。 关闭的限流器上调用 Release 会安全忽略（不会 panic）。

使用示例：

	if err := rl.Acquire(ctx); err != nil {
	    return err
	}
	defer rl.Release()
	doRequest()


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Release()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即尝试归还一个令牌，满桶或关闭时忽略，不检查调用方是否实际获取。多余调用在桶未满时可提前增加额度，不能随意重复释放；Token 句柄已负责释放时不要再调用本方法。按时间额度限流且希望持续限速时，业务后主动 Release 会改变预期速率。

## `*RateLimiter.Resize`

```go
func (rl *RateLimiter) Resize(newRate int)
```

[实现](../../internal/ratelimit/ratelimit.go#L456) · [行为专题](../ratelimit.md)：Resize 动态调整限流速率。newRate <= 0 或等于当前值时忽略。

**源码说明与使用片段**（原文）：

Resize 动态调整限流速率。newRate \<= 0 或等于当前值时忽略。 已关闭的限流器上调用无效。

参数：

  - newRate：新的每 perDuration 窗口补充额度，同时调整令牌容量

使用示例：

	// 高峰期扩大速率
	rl.Resize(200)


**参数说明**：

- `newRate` (`int`)：新的每 perDuration 窗口补充额度，同时调整令牌容量

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Resize(newRate)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：非正值、相同速率或已关闭时无操作；速率变化时先按旧速率结算经过时间，再把容量改为新速率，原 Burst 不保留。保留尚不足一个令牌的余量，桶满时清除超额；100ms ticker 不重新计时。保持已有未归还许可的容量影响；定时补充型仍会随时间恢复额度，不能当作严格并发限制。分片按片依次调整，观测不是全局原子快照。

## `*RateLimiter.Size`

```go
func (rl *RateLimiter) Size() int
```

[实现](../../internal/ratelimit/ratelimit.go#L506) · [行为专题](../ratelimit.md)：Size 返回当前配置额度（每 perDuration 窗口补充的令牌数，不固定为每秒）。

**源码说明与使用片段**（原文）：

Size 返回当前配置额度（每 perDuration 窗口补充的令牌数，不固定为每秒）。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Size()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：Size 返回每 Per 窗口的配置额度，Available 返回当时可用令牌数；突发容量可大于 Size，不能用两者差值推导持有许可数。查询不是额度预留，随后 Acquire 可能阻塞或被拒；关闭后的状态读数不表示实例仍可用。

## `*RateLimiter.Stop`

```go
func (rl *RateLimiter) Stop()
```

[实现](../../internal/ratelimit/ratelimit.go#L241) · [行为专题](../ratelimit.md)：Stop 等同 Close，停止限流器。

**源码说明与使用片段**（原文）：

Stop 等同 Close，停止限流器。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stop()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：Stop 委托 Close，重复/并发 Close 等待同一次资源清理完成；停止补充 goroutine 并关闭令牌通道，唤醒等待。不等待业务完成、不取消业务 context；已持有许可的业务仍须自行收尾，之后不可重开实例。

## `*RateLimiter.Token`

```go
func (rl *RateLimiter) Token(ctx context.Context) (*Token, error)
```

[实现](../../internal/ratelimit/ratelimit.go#L554) · [行为专题](../ratelimit.md)：Token 获取一个自动释放的令牌包装器，配合 defer 使用更安全。

**源码说明与使用片段**（原文）：

Token 获取一个自动释放的令牌包装器，配合 defer 使用更安全。

参数：

  - ctx：非 nil 上下文；遵循 Acquire 当前策略，有令牌时已取消仍可能获取成功

使用示例：

	t, err := rl.Token(ctx)
	if err != nil {
	    return err
	}
	defer t.Release()


**参数说明**：

- `ctx` (`context.Context`)：本次获取许可的非 nil context；Block 只在无可用许可时响应取消，有空位时已取消仍可成功，Reject/BlockForce 忽略取消。分片只访问选中一片，获取失败不得 Release；Token 成功后通过句柄释放。

**返回值签名**：`*Token, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Token(ctx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：按当前策略获取：Reject 无令牌立即报错并忽略 context；BlockForce 忽略取消，直到获取或 Close；Block 在无可用令牌时检查取消，已取消但有令牌仍可成功。成功后按用途决定是否归还；定时补充与 Release 均可增加余额，不能当成严格并发上限或严格窗口次数。Token 成功后只通过句柄 Release。

## `*RateLimiter.Wait`

```go
func (rl *RateLimiter) Wait(ctx context.Context) error
```

[实现](../../internal/ratelimit/ratelimit.go#L236) · [行为专题](../ratelimit.md)：Wait 等同 Acquire，按当前 Block、Reject 或 BlockForce 策略获取令牌。

**源码说明与使用片段**（原文）：

Wait 等同 Acquire，按当前 Block、Reject 或 BlockForce 策略获取令牌。

参数：

  - ctx：非 nil 上下文；Block 仅在无令牌时响应取消，Reject/BlockForce 忽略取消

使用示例：

	if err := rl.Wait(ctx); err == nil {
	    doRequest() // 定时补充额度；Release 会提前归还令牌
	}


**参数说明**：

- `ctx` (`context.Context`)：本次获取许可的非 nil context；Block 只在无可用许可时响应取消，有空位时已取消仍可成功，Reject/BlockForce 忽略取消。分片只访问选中一片，获取失败不得 Release；Token 成功后通过句柄释放。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：按当前策略获取：Reject 无令牌立即报错并忽略 context；BlockForce 忽略取消，直到获取或 Close；Block 在无可用令牌时检查取消，已取消但有令牌仍可成功。成功后按用途决定是否归还；定时补充与 Release 均可增加余额，不能当成严格并发上限或严格窗口次数。Token 成功后只通过句柄 Release。

## `*RateLimiter.WithStrategy`

```go
func (rl *RateLimiter) WithStrategy(s Strategy) *RateLimiter
```

[实现](../../internal/ratelimit/ratelimit.go#L254) · [行为专题](../ratelimit.md)：WithStrategy 设置限流策略：Block（阻塞等待）、Reject（立即拒绝）、BlockForce（强制阻塞）。

**源码说明与使用片段**（原文）：

WithStrategy 设置限流策略：Block（阻塞等待）、Reject（立即拒绝）、BlockForce（强制阻塞）。

参数：

  - s：策略常量（Block/Reject/BlockForce）

使用示例：

	// 令牌不够时立即拒绝而非阻塞
	rl.WithStrategy(ratelimit.Reject)


**参数说明**：

- `s` (`Strategy`)：策略常量（Block/Reject/BlockForce）

**返回值签名**：`*RateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStrategy(s)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：原样保存策略，后续 Acquire 读取；未识别值走默认 Block 分支。不会改变已开始等待的 Acquire 所选策略，也不改变额度；BlockForce 不响应 context 取消，只能靠归还、补充或关闭唤醒。配置建议在共享实例前完成。

## `*RateLimiter.WithTraceID`

```go
func (rl *RateLimiter) WithTraceID(ctx context.Context) (*RateLimiter, context.Context)
```

[实现](../../internal/ratelimit/ratelimit.go#L267) · [行为专题](../ratelimit.md)：WithTraceID 绑定 TraceID 用于日志追踪。

**源码说明与使用片段**（原文）：

WithTraceID 绑定 TraceID 用于日志追踪。

参数：

  - ctx：上下文，若不含 trace\_id 则自动生成

使用示例：

	rl, ctx := ratelimit.NewRateLimiter(100, time.Second).WithTraceID(ctx)


**参数说明**：

- `ctx` (`context.Context`)：上下文，若不含 trace_id 则自动生成

**返回值签名**：`*RateLimiter, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTraceID(ctx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：传非 nil context。EnsureTraceID 仅检查当前 key 的值是否为 nil，非 nil 时原样返回，包括空字符串和非 string；它不修复类型。WithTraceID 的空 id 委托 EnsureTraceID，保留已有值；非空 id 才覆盖。启用 TraceLog 时，仅生成/显式覆盖路径调用 logger.With/WithContext，后端必须返回有效 context。GetTraceID 对非 string 值会 panic。自定义 key 在初始化时固定，已有 context 的旧值不会自动迁移；继续传递返回 context。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
rl.ctx = ctx
```

## `*RatelimitBuilder.Adaptive`

```go
func (b *RatelimitBuilder) Adaptive() *AdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L56) · [行为专题](../ratelimit.md)：Adaptive 返回 Adaptive 子构建器。

**源码说明与使用片段**（原文）：

Adaptive 返回 Adaptive 子构建器。


**参数说明**：无显式参数。

**返回值签名**：`*AdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Adaptive()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*RatelimitBuilder.Context`

```go
func (b *RatelimitBuilder) Context(ctx context.Context) *RatelimitBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L66) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*RatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*RatelimitBuilder.DefaultLogger`

```go
func (b *RatelimitBuilder) DefaultLogger() *RatelimitBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L78) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*RatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*RatelimitBuilder.Logger`

```go
func (b *RatelimitBuilder) Logger(l core.Logger) *RatelimitBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L72) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*RatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*RatelimitBuilder.RateLimiter`

```go
func (b *RatelimitBuilder) RateLimiter() *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L41) · [行为专题](../ratelimit.md)：RateLimiter 返回 RateLimiter 子构建器。

**源码说明与使用片段**（原文）：

RateLimiter 返回 RateLimiter 子构建器。


**参数说明**：无显式参数。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RateLimiter()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*RatelimitBuilder.Sharded`

```go
func (b *RatelimitBuilder) Sharded() *ShardedRatelimitBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L61) · [行为专题](../ratelimit.md)：Sharded 返回分片限流器入口构建器。

**源码说明与使用片段**（原文）：

Sharded 返回分片限流器入口构建器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Sharded()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*RatelimitBuilder.SlidingWindow`

```go
func (b *RatelimitBuilder) SlidingWindow() *SlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L51) · [行为专题](../ratelimit.md)：SlidingWindow 返回 SlidingWindow 子构建器。

**源码说明与使用片段**（原文）：

SlidingWindow 返回 SlidingWindow 子构建器。


**参数说明**：无显式参数。

**返回值签名**：`*SlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SlidingWindow()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*RatelimitBuilder.TokenBucket`

```go
func (b *RatelimitBuilder) TokenBucket() *TokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L46) · [行为专题](../ratelimit.md)：TokenBucket 返回 TokenBucket 子构建器。

**源码说明与使用片段**（原文）：

TokenBucket 返回 TokenBucket 子构建器。


**参数说明**：无显式参数。

**返回值签名**：`*TokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TokenBucket()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*RatelimiterSubBuilder.Build`

```go
func (b *RatelimiterSubBuilder) Build() *RateLimiter
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L134) · [行为专题](../ratelimit.md)：Build 构建 RateLimiter 实例。

**源码说明与使用片段**（原文）：

Build 构建 RateLimiter 实例。


**参数说明**：无显式参数。

**返回值签名**：`*RateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次创建独立实例，不回写规范化后的字段；不继承所保存 context 的取消、期限和业务值，RateLimiter 及其分片保留局部日志设置，阻塞获取须另传 context。单片非正 Rate 按 IO()，分片按总量分配且每片至少 1；正数 Burst 开启突发路径。未配置的 Per 为 0，每个 100ms tick 补满，DefaultPer 才设为 1 秒。用完必须 Close。

## `*RatelimiterSubBuilder.Burst`

```go
func (b *RatelimiterSubBuilder) Burst(n int) *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L110) · [行为专题](../ratelimit.md)：Burst 设置突发容量，允许短时间超过 Rate 速率的请求数。

**源码说明与使用片段**（原文）：

Burst 设置突发容量，允许短时间超过 Rate 速率的请求数。


**参数说明**：

- `n` (`int`)：Burst 设置突发容量，允许短时间超过 Rate 速率的请求数。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Burst(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.burst = n
```

## `*RatelimiterSubBuilder.Context`

```go
func (b *RatelimiterSubBuilder) Context(ctx context.Context) *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L116) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*RatelimiterSubBuilder.DefaultBurst`

```go
func (b *RatelimiterSubBuilder) DefaultBurst() *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L158) · [行为专题](../ratelimit.md)：DefaultBurst 使用默认突发容量（core.IO() × 2）。

**源码说明与使用片段**（原文）：

DefaultBurst 使用默认突发容量（core.IO() × 2）。


**参数说明**：无显式参数。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBurst()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*RatelimiterSubBuilder.DefaultLogger`

```go
func (b *RatelimiterSubBuilder) DefaultLogger() *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L128) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*RatelimiterSubBuilder.DefaultPer`

```go
func (b *RatelimiterSubBuilder) DefaultPer() *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L152) · [行为专题](../ratelimit.md)：DefaultPer 使用默认时间窗口（1 秒）。

**源码说明与使用片段**（原文）：

DefaultPer 使用默认时间窗口（1 秒）。


**参数说明**：无显式参数。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPer()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*RatelimiterSubBuilder.DefaultRate`

```go
func (b *RatelimiterSubBuilder) DefaultRate() *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L146) · [行为专题](../ratelimit.md)：DefaultRate 使用默认速率（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultRate 使用默认速率（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultRate()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*RatelimiterSubBuilder.Logger`

```go
func (b *RatelimiterSubBuilder) Logger(l core.Logger) *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L122) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*RatelimiterSubBuilder.Per`

```go
func (b *RatelimiterSubBuilder) Per(d time.Duration) *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L104) · [行为专题](../ratelimit.md)：Per 设置时间窗口大小。

**源码说明与使用片段**（原文）：

Per 设置时间窗口大小。


**参数说明**：

- `d` (`time.Duration`)：RateLimiter 速率的时间窗口；非正值不会自动变为 DefaultPer，而是在每个补充 tick 把桶补满。需要每秒速率时显式传 time.Second。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Per(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.perDuration = d
```

## `*RatelimiterSubBuilder.Rate`

```go
func (b *RatelimiterSubBuilder) Rate(n int) *RatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L98) · [行为专题](../ratelimit.md)：Rate 设置速率：每 PerDuration 内允许的操作次数。

**源码说明与使用片段**（原文）：

Rate 设置速率：每 PerDuration 内允许的操作次数。


**参数说明**：

- `n` (`int`)：Rate 设置速率：每 PerDuration 内允许的操作次数。

**返回值签名**：`*RatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Rate(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.rate = n
```

## `*ShardedAdaptiveRateLimiter.Acquire`

```go
func (sa *ShardedAdaptiveRateLimiter) Acquire(ctx context.Context) error
```

[实现](../../internal/ratelimit/sharded.go#L423) · [行为专题](../ratelimit.md)：Acquire round-robin 分发获取令牌。

**源码说明与使用片段**（原文）：

Acquire round-robin 分发获取令牌。


**参数说明**：

- `ctx` (`context.Context`)：本次获取槽位的非 nil context；固定使用 Block，有空位时已取消仍可成功，无空位时等待归还或取消。Adaptive 没有公开的策略切换方法；分片只等待选中一片，不搜索其他片。仅成功获取后配对同一入口的 Release。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Acquire(ctx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：轮转选择一片并以固定 Block 获取槽位，只等待所选片，不搜索其他片空闲槽位；有空位时已取消仍可能成功，无空位时响应取消或关闭。Adaptive 不提供 WithStrategy。仅成功获取后配对外层 Release，不与 GetShard 直接获取混用；业务完成后另行记录结果。

## `*ShardedAdaptiveRateLimiter.Close`

```go
func (sa *ShardedAdaptiveRateLimiter) Close()
```

[实现](../../internal/ratelimit/sharded.go#L452) · [行为专题](../ratelimit.md)：Close 关闭所有分片。

**源码说明与使用片段**（原文）：

Close 关闭所有分片。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：逐片关闭底层许可器，唤醒阻塞等待；不等待业务回调完成、不强制终止任务。RateLimiter 分片同时停止补充 goroutine，Adaptive 分片关闭无补充的许可器；关闭期间跨片观测不原子，关闭后不再复用。

## `*ShardedAdaptiveRateLimiter.GetShard`

```go
func (sa *ShardedAdaptiveRateLimiter) GetShard(idx int) *AdaptiveRateLimiter
```

[实现](../../internal/ratelimit/sharded.go#L464) · [行为专题](../ratelimit.md)：GetShard 获取指定分片实例。

**源码说明与使用片段**（原文）：

GetShard 获取指定分片实例。


**参数说明**：

- `idx` (`int`)：目标分片的零起始索引；负值或不小于分片数时返回 nil。

**返回值签名**：`*AdaptiveRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShard(idx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：负索引或超出分片数返回 nil；有效索引返回原实例指针，不复制。直接使用该片绕过外层轮转和许可记录，不能把直接获取与外层 Release 混用；直接调整也可能使外层汇总配置失真，统一管理生命周期。

## `*ShardedAdaptiveRateLimiter.RecordFailure`

```go
func (sa *ShardedAdaptiveRateLimiter) RecordFailure()
```

[实现](../../internal/ratelimit/sharded.go#L446) · [行为专题](../ratelimit.md)：RecordFailure round-robin 分发记录失败。

**源码说明与使用片段**（原文）：

RecordFailure round-robin 分发记录失败。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RecordFailure()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：只记录业务完成情况，不获取或释放许可；每次成功 Acquire 仍须配对 Release，每次完成只记录一次。单片累计至少 10 次记录才评估：失败比例>20% 缩容一半且不低于最小值，<10% 按剩余容量的 20%（至少 1）扩容，其他比例不调整；之后该批计数清零。分片记录使用独立轮转，不绑定获取许可的片。

## `*ShardedAdaptiveRateLimiter.RecordOk`

```go
func (sa *ShardedAdaptiveRateLimiter) RecordOk()
```

[实现](../../internal/ratelimit/sharded.go#L440) · [行为专题](../ratelimit.md)：RecordOk round-robin 分发记录成功。

**源码说明与使用片段**（原文）：

RecordOk round-robin 分发记录成功。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RecordOk()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：只记录业务完成情况，不获取或释放许可；每次成功 Acquire 仍须配对 Release，每次完成只记录一次。单片累计至少 10 次记录才评估：失败比例>20% 缩容一半且不低于最小值，<10% 按剩余容量的 20%（至少 1）扩容，其他比例不调整；之后该批计数清零。分片记录使用独立轮转，不绑定获取许可的片。

## `*ShardedAdaptiveRateLimiter.Release`

```go
func (sa *ShardedAdaptiveRateLimiter) Release()
```

[实现](../../internal/ratelimit/sharded.go#L433) · [行为专题](../ratelimit.md)：Release 释放一个成功获取的槽位；多余的 Release 不增加容量。

**源码说明与使用片段**（原文）：

Release 释放一个成功获取的槽位；多余的 Release 不增加容量。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Release()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：仅配对外层成功 Acquire/Wait 的许可，内部记录选择有未归还许可的一片，未绑定调用方 goroutine；多余外层 Release 无操作。Token 句柄和直接 GetShard 获取须用各自释放途径；错误配对会提前归还其他业务持有的许可。

## `*ShardedAdaptiveRateLimiter.ShardCount`

```go
func (sa *ShardedAdaptiveRateLimiter) ShardCount() int
```

[实现](../../internal/ratelimit/sharded.go#L459) · [行为专题](../ratelimit.md)：ShardCount 返回分片数。

**源码说明与使用片段**（原文）：

ShardCount 返回分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedAdaptiveRateLimiter.TotalMax`

```go
func (sa *ShardedAdaptiveRateLimiter) TotalMax() int
```

[实现](../../internal/ratelimit/sharded.go#L481) · [行为专题](../ratelimit.md)：TotalMax 返回所有分片 maxRate 之和。

**源码说明与使用片段**（原文）：

TotalMax 返回所有分片 maxRate 之和。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalMax()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedAdaptiveRateLimiter.TotalMin`

```go
func (sa *ShardedAdaptiveRateLimiter) TotalMin() int
```

[实现](../../internal/ratelimit/sharded.go#L472) · [行为专题](../ratelimit.md)：TotalMin 返回所有分片 minRate 之和。

**源码说明与使用片段**（原文）：

TotalMin 返回所有分片 minRate 之和。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalMin()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedAdaptiveSubBuilder.Build`

```go
func (b *ShardedAdaptiveSubBuilder) Build() *ShardedAdaptiveRateLimiter
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L681) · [行为专题](../ratelimit.md)：Build 构建 ShardedAdaptiveRateLimiter 实例。

**源码说明与使用片段**（原文）：

Build 构建 ShardedAdaptiveRateLimiter 实例。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedAdaptiveRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次创建独立实例，不回写规范化后的字段；不继承所保存 context 的取消、期限和业务值，RateLimiter 及其分片保留局部日志设置，阻塞获取须另传 context。单片最小值非正按 1，最大值不高于最小值时提升至最小值，初始为两者中点。分片先分配总最小/最大值，每片至少 1，再规范化；许可须成对释放。普通 Adaptive 无 Close 方法且无补充 goroutine，分片版本用完 Close。

## `*ShardedAdaptiveSubBuilder.Context`

```go
func (b *ShardedAdaptiveSubBuilder) Context(ctx context.Context) *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L663) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*ShardedAdaptiveSubBuilder.DefaultLogger`

```go
func (b *ShardedAdaptiveSubBuilder) DefaultLogger() *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L675) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedAdaptiveSubBuilder.DefaultMaxWorker`

```go
func (b *ShardedAdaptiveSubBuilder) DefaultMaxWorker() *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L698) · [行为专题](../ratelimit.md)：DefaultMaxWorker 使用默认最大并发度（core.IO() × 10）。

**源码说明与使用片段**（原文）：

DefaultMaxWorker 使用默认最大并发度（core.IO() × 10）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedAdaptiveSubBuilder.DefaultMinWorker`

```go
func (b *ShardedAdaptiveSubBuilder) DefaultMinWorker() *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L692) · [行为专题](../ratelimit.md)：DefaultMinWorker 使用默认最小并发度（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultMinWorker 使用默认最小并发度（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMinWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedAdaptiveSubBuilder.DefaultShards`

```go
func (b *ShardedAdaptiveSubBuilder) DefaultShards() *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L686) · [行为专题](../ratelimit.md)：DefaultShards 使用默认分片数（core.IO()，最少 2）。

**源码说明与使用片段**（原文）：

DefaultShards 使用默认分片数（core.IO()，最少 2）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShards()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedAdaptiveSubBuilder.Logger`

```go
func (b *ShardedAdaptiveSubBuilder) Logger(l core.Logger) *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L669) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedAdaptiveSubBuilder.MaxWorker`

```go
func (b *ShardedAdaptiveSubBuilder) MaxWorker(n int) *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L657) · [行为专题](../ratelimit.md)：MaxWorker 设置最大并发度，平均分配到各分片。

**源码说明与使用片段**（原文）：

MaxWorker 设置最大并发度，平均分配到各分片。


**参数说明**：

- `n` (`int`)：MaxWorker 设置最大并发度，平均分配到各分片。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxWorker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.maxRate = n
```

## `*ShardedAdaptiveSubBuilder.MinWorker`

```go
func (b *ShardedAdaptiveSubBuilder) MinWorker(n int) *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L651) · [行为专题](../ratelimit.md)：MinWorker 设置最小并发度，平均分配到各分片。

**源码说明与使用片段**（原文）：

MinWorker 设置最小并发度，平均分配到各分片。


**参数说明**：

- `n` (`int`)：MinWorker 设置最小并发度，平均分配到各分片。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MinWorker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.minRate = n
```

## `*ShardedAdaptiveSubBuilder.Shards`

```go
func (b *ShardedAdaptiveSubBuilder) Shards(n int) *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L645) · [行为专题](../ratelimit.md)：Shards 设置水平分片数。

**源码说明与使用片段**（原文）：

Shards 设置水平分片数。


**参数说明**：

- `n` (`int`)：Shards 设置水平分片数。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.shards = n
```

## `*ShardedRateLimiter.Acquire`

```go
func (sl *ShardedRateLimiter) Acquire(ctx context.Context) error
```

[实现](../../internal/ratelimit/sharded.go#L164) · [行为专题](../ratelimit.md)：Acquire 获取令牌，round-robin 分发。

**源码说明与使用片段**（原文）：

Acquire 获取令牌，round-robin 分发。


**参数说明**：

- `ctx` (`context.Context`)：本次获取许可的非 nil context；Block 只在无可用许可时响应取消，有空位时已取消仍可成功，Reject/BlockForce 忽略取消。分片只访问选中一片，获取失败不得 Release；Token 成功后通过句柄释放。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Acquire(ctx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：轮转选择一片并按其策略获取，不寻找其他空闲分片；所选片阻塞时即使别片有余额也会等待。仅成功获取后配对外层 Release，不与 GetShard 直接获取或 Token 句柄混用；回调须自己管理执行和错误记录，获取成功不代表业务完成。

## `*ShardedRateLimiter.Available`

```go
func (sl *ShardedRateLimiter) Available() int
```

[实现](../../internal/ratelimit/sharded.go#L203) · [行为专题](../ratelimit.md)：Available 返回所有分片当前可用令牌总数。

**源码说明与使用片段**（原文）：

Available 返回所有分片当前可用令牌总数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Available()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedRateLimiter.Close`

```go
func (sl *ShardedRateLimiter) Close()
```

[实现](../../internal/ratelimit/sharded.go#L181) · [行为专题](../ratelimit.md)：Close 关闭所有分片的限流器。

**源码说明与使用片段**（原文）：

Close 关闭所有分片的限流器。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：逐片关闭底层许可器，唤醒阻塞等待；不等待业务回调完成、不强制终止任务。RateLimiter 分片同时停止补充 goroutine，Adaptive 分片关闭无补充的许可器；关闭期间跨片观测不原子，关闭后不再复用。

## `*ShardedRateLimiter.GetShard`

```go
func (sl *ShardedRateLimiter) GetShard(idx int) *RateLimiter
```

[实现](../../internal/ratelimit/sharded.go#L151) · [行为专题](../ratelimit.md)：GetShard 获取指定分片的 RateLimiter 实例。

**源码说明与使用片段**（原文）：

GetShard 获取指定分片的 RateLimiter 实例。 idx 越界返回 nil。


**参数说明**：

- `idx` (`int`)：目标分片的零起始索引；负值或不小于分片数时返回 nil。

**返回值签名**：`*RateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShard(idx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：负索引或超出分片数返回 nil；有效索引返回原实例指针，不复制。直接使用该片绕过外层轮转和许可记录，不能把直接获取与外层 Release 混用；直接调整也可能使外层汇总配置失真，统一管理生命周期。

## `*ShardedRateLimiter.Release`

```go
func (sl *ShardedRateLimiter) Release()
```

[实现](../../internal/ratelimit/sharded.go#L174) · [行为专题](../ratelimit.md)：Release 归还一个成功获取的令牌；多余的 Release 不增加容量。

**源码说明与使用片段**（原文）：

Release 归还一个成功获取的令牌；多余的 Release 不增加容量。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Release()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：仅配对外层成功 Acquire/Wait 的许可，内部记录选择有未归还许可的一片，未绑定调用方 goroutine；多余外层 Release 无操作。Token 句柄和直接 GetShard 获取须用各自释放途径；错误配对会提前归还其他业务持有的许可。

## `*ShardedRateLimiter.Resize`

```go
func (sl *ShardedRateLimiter) Resize(newTotalRate int)
```

[实现](../../internal/ratelimit/sharded.go#L221) · [行为专题](../ratelimit.md)：Resize 调整总速率，余数优先分配到各分片。非正数会被忽略。

**源码说明与使用片段**（原文）：

Resize 调整总速率，余数优先分配到各分片。非正数会被忽略。 注意：应在无活跃 Acquire/Release 时调用，否则可能短暂超发或欠发。


**参数说明**：

- `newTotalRate` (`int`)：调整后的总补充额度；非正值忽略。按余数分配至各片且每片至少 1，因此请求值小于片数时实际总额为片数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Resize(newTotalRate)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：非正值、相同速率或已关闭时无操作；速率变化时先按旧速率结算经过时间，再把容量改为新速率，原 Burst 不保留。保留尚不足一个令牌的余量，桶满时清除超额；100ms ticker 不重新计时。保持已有未归还许可的容量影响；定时补充型仍会随时间恢复额度，不能当作严格并发限制。分片按片依次调整，观测不是全局原子快照。

## `*ShardedRateLimiter.ShardCount`

```go
func (sl *ShardedRateLimiter) ShardCount() int
```

[实现](../../internal/ratelimit/sharded.go#L140) · [行为专题](../ratelimit.md)：ShardCount 返回分片数。

**源码说明与使用片段**（原文）：

ShardCount 返回分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedRateLimiter.Size`

```go
func (sl *ShardedRateLimiter) Size() int
```

[实现](../../internal/ratelimit/sharded.go#L194) · [行为专题](../ratelimit.md)：Size 返回所有分片的配置速率之和，单位为每 perDuration 窗口的令牌数。

**源码说明与使用片段**（原文）：

Size 返回所有分片的配置速率之和，单位为每 perDuration 窗口的令牌数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Size()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedRateLimiter.Token`

```go
func (sl *ShardedRateLimiter) Token(ctx context.Context) (*Token, error)
```

[实现](../../internal/ratelimit/sharded.go#L238) · [行为专题](../ratelimit.md)：Token 获取一个带 Release 的令牌句柄，round-robin 分发。

**源码说明与使用片段**（原文）：

Token 获取一个带 Release 的令牌句柄，round-robin 分发。


**参数说明**：

- `ctx` (`context.Context`)：本次获取许可的非 nil context；Block 只在无可用许可时响应取消，有空位时已取消仍可成功，Reject/BlockForce 忽略取消。分片只访问选中一片，获取失败不得 Release；Token 成功后通过句柄释放。

**返回值签名**：`*Token, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Token(ctx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：轮转选择一片并返回绑定该片的 Token；成功后调用句柄 Release，失败时不要释放。该路径不登记外层 Acquire 的许可记录，不能再调用外层 Release；不要复制包含 sync.Once 的 Token。

## `*ShardedRateLimiter.TotalRate`

```go
func (sl *ShardedRateLimiter) TotalRate() int
```

[实现](../../internal/ratelimit/sharded.go#L145) · [行为专题](../ratelimit.md)：TotalRate 返回所有分片当前速率之和。目标速率小于分片数时，每片至少为 1。

**源码说明与使用片段**（原文）：

TotalRate 返回所有分片当前速率之和。目标速率小于分片数时，每片至少为 1。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalRate()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedRateLimiter.Wait`

```go
func (sl *ShardedRateLimiter) Wait(ctx context.Context) error
```

[实现](../../internal/ratelimit/sharded.go#L159) · [行为专题](../ratelimit.md)：Wait 获取令牌，round-robin 分发到某个分片。

**源码说明与使用片段**（原文）：

Wait 获取令牌，round-robin 分发到某个分片。


**参数说明**：

- `ctx` (`context.Context`)：本次获取许可的非 nil context；Block 只在无可用许可时响应取消，有空位时已取消仍可成功，Reject/BlockForce 忽略取消。分片只访问选中一片，获取失败不得 Release；Token 成功后通过句柄释放。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：轮转选择一片并按其策略获取，不寻找其他空闲分片；所选片阻塞时即使别片有余额也会等待。仅成功获取后配对外层 Release，不与 GetShard 直接获取或 Token 句柄混用；回调须自己管理执行和错误记录，获取成功不代表业务完成。

## `*ShardedRateLimiter.WithStrategy`

```go
func (sl *ShardedRateLimiter) WithStrategy(s Strategy) *ShardedRateLimiter
```

[实现](../../internal/ratelimit/sharded.go#L212) · [行为专题](../ratelimit.md)：WithStrategy 为所有分片设置策略。

**源码说明与使用片段**（原文）：

WithStrategy 为所有分片设置策略。


**参数说明**：

- `s` (`Strategy`)：令牌获取策略：Block 等待令牌或取消，Reject 立即拒绝，BlockForce 忽略 context 取消并等待令牌或关闭。

**返回值签名**：`*ShardedRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStrategy(s)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：按片依次原样保存策略，非跨片原子更新；只影响各片后续 Acquire，已经等待的调用保留原策略。未知策略按 Block，Reject 忽略取消，BlockForce 只靠额度或关闭解除等待；应用启动时配置，不能用策略切换取消已有等待。

## `*ShardedRatelimitBuilder.Adaptive`

```go
func (b *ShardedRatelimitBuilder) Adaptive() *ShardedAdaptiveSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L367) · [行为专题](../ratelimit.md)：Adaptive 返回分片 Adaptive 子构建器。

**源码说明与使用片段**（原文）：

Adaptive 返回分片 Adaptive 子构建器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedAdaptiveSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Adaptive()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*ShardedRatelimitBuilder.Context`

```go
func (b *ShardedRatelimitBuilder) Context(ctx context.Context) *ShardedRatelimitBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L372) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedRatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*ShardedRatelimitBuilder.DefaultLogger`

```go
func (b *ShardedRatelimitBuilder) DefaultLogger() *ShardedRatelimitBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L384) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedRatelimitBuilder.Logger`

```go
func (b *ShardedRatelimitBuilder) Logger(l core.Logger) *ShardedRatelimitBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L378) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*ShardedRatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedRatelimitBuilder.RateLimiter`

```go
func (b *ShardedRatelimitBuilder) RateLimiter() *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L352) · [行为专题](../ratelimit.md)：RateLimiter 返回分片 RateLimiter 子构建器。

**源码说明与使用片段**（原文）：

RateLimiter 返回分片 RateLimiter 子构建器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RateLimiter()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*ShardedRatelimitBuilder.SlidingWindow`

```go
func (b *ShardedRatelimitBuilder) SlidingWindow() *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L362) · [行为专题](../ratelimit.md)：SlidingWindow 返回分片 SlidingWindow 子构建器。

**源码说明与使用片段**（原文）：

SlidingWindow 返回分片 SlidingWindow 子构建器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SlidingWindow()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*ShardedRatelimitBuilder.TokenBucket`

```go
func (b *ShardedRatelimitBuilder) TokenBucket() *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L357) · [行为专题](../ratelimit.md)：TokenBucket 返回分片 TokenBucket 子构建器。

**源码说明与使用片段**（原文）：

TokenBucket 返回分片 TokenBucket 子构建器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TokenBucket()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：每次分配新的子构建器，复制当前 context 及其局部日志设置，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不继承所保存 context 的取消、期限和业务值。

## `*ShardedRatelimiterSubBuilder.Build`

```go
func (b *ShardedRatelimiterSubBuilder) Build() *ShardedRateLimiter
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L447) · [行为专题](../ratelimit.md)：Build 构建 ShardedRateLimiter 实例。

**源码说明与使用片段**（原文）：

Build 构建 ShardedRateLimiter 实例。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次创建独立实例，不回写规范化后的字段；不继承所保存 context 的取消、期限和业务值，RateLimiter 及其分片保留局部日志设置，阻塞获取须另传 context。单片非正 Rate 按 IO()，分片按总量分配且每片至少 1；正数 Burst 开启突发路径。未配置的 Per 为 0，每个 100ms tick 补满，DefaultPer 才设为 1 秒。用完必须 Close。

## `*ShardedRatelimiterSubBuilder.Burst`

```go
func (b *ShardedRatelimiterSubBuilder) Burst(n int) *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L423) · [行为专题](../ratelimit.md)：Burst 设置突发容量。

**源码说明与使用片段**（原文）：

Burst 设置突发容量。


**参数说明**：

- `n` (`int`)：Burst 设置突发容量。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Burst(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.burst = n
```

## `*ShardedRatelimiterSubBuilder.Context`

```go
func (b *ShardedRatelimiterSubBuilder) Context(ctx context.Context) *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L429) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*ShardedRatelimiterSubBuilder.DefaultBurst`

```go
func (b *ShardedRatelimiterSubBuilder) DefaultBurst() *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L479) · [行为专题](../ratelimit.md)：DefaultBurst 使用默认突发容量（core.IO() × 2）。

**源码说明与使用片段**（原文）：

DefaultBurst 使用默认突发容量（core.IO() × 2）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBurst()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedRatelimiterSubBuilder.DefaultLogger`

```go
func (b *ShardedRatelimiterSubBuilder) DefaultLogger() *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L441) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedRatelimiterSubBuilder.DefaultPer`

```go
func (b *ShardedRatelimiterSubBuilder) DefaultPer() *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L473) · [行为专题](../ratelimit.md)：DefaultPer 使用默认时间窗口（1 秒）。

**源码说明与使用片段**（原文）：

DefaultPer 使用默认时间窗口（1 秒）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPer()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedRatelimiterSubBuilder.DefaultRate`

```go
func (b *ShardedRatelimiterSubBuilder) DefaultRate() *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L467) · [行为专题](../ratelimit.md)：DefaultRate 使用默认速率（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultRate 使用默认速率（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultRate()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedRatelimiterSubBuilder.DefaultShards`

```go
func (b *ShardedRatelimiterSubBuilder) DefaultShards() *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L461) · [行为专题](../ratelimit.md)：DefaultShards 使用默认分片数（core.IO()，最少 2）。

**源码说明与使用片段**（原文）：

DefaultShards 使用默认分片数（core.IO()，最少 2）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShards()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedRatelimiterSubBuilder.Logger`

```go
func (b *ShardedRatelimiterSubBuilder) Logger(l core.Logger) *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L435) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedRatelimiterSubBuilder.Per`

```go
func (b *ShardedRatelimiterSubBuilder) Per(d time.Duration) *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L417) · [行为专题](../ratelimit.md)：Per 设置时间窗口大小。

**源码说明与使用片段**（原文）：

Per 设置时间窗口大小。


**参数说明**：

- `d` (`time.Duration`)：RateLimiter 速率的时间窗口；非正值不会自动变为 DefaultPer，而是在每个补充 tick 把桶补满。需要每秒速率时显式传 time.Second。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Per(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.perDuration = d
```

## `*ShardedRatelimiterSubBuilder.Rate`

```go
func (b *ShardedRatelimiterSubBuilder) Rate(n int) *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L411) · [行为专题](../ratelimit.md)：Rate 设置总速率（每 PerDuration），平均分配到各分片。

**源码说明与使用片段**（原文）：

Rate 设置总速率（每 PerDuration），平均分配到各分片。


**参数说明**：

- `n` (`int`)：Rate 设置总速率（每 PerDuration），平均分配到各分片。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Rate(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.rate = n
```

## `*ShardedRatelimiterSubBuilder.Shards`

```go
func (b *ShardedRatelimiterSubBuilder) Shards(n int) *ShardedRatelimiterSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L405) · [行为专题](../ratelimit.md)：Shards 设置水平分片数。<=0 时构造函数使用 core.IO()。

**源码说明与使用片段**（原文）：

Shards 设置水平分片数。\<=0 时构造函数使用 core.IO()。


**参数说明**：

- `n` (`int`)：Shards 设置水平分片数。<=0 时构造函数使用 core.IO()。

**返回值签名**：`*ShardedRatelimiterSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.shards = n
```

## `*ShardedSlidingWindowRateLimiter.Allow`

```go
func (sw *ShardedSlidingWindowRateLimiter) Allow() bool
```

[实现](../../internal/ratelimit/sharded.go#L347) · [行为专题](../ratelimit.md)：Allow round-robin 分发到一个分片检查。

**源码说明与使用片段**（原文）：

Allow round-robin 分发到一个分片检查。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示本次额度检查通过；false 表示当前额度不足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Allow()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：每次轮转选择一片，只检查该片，不搜索其他片空闲额度、不跨片拼接 AllowN。拒绝和非正请求也推进外层轮转，非正请求直接成功且不扣额度；正数大于单片容量可拒绝，即使全局汇总仍有余额。TokenBucket 的有限小数请求按整笔额度处理，配置按每片应用；SlidingWindow 配置按总量分配。GetShard 直接请求会改变该片余额但不推进外层轮转；无后台 goroutine 或 Close 方法。

## `*ShardedSlidingWindowRateLimiter.AllowN`

```go
func (sw *ShardedSlidingWindowRateLimiter) AllowN(n int) bool
```

[实现](../../internal/ratelimit/sharded.go#L353) · [行为专题](../ratelimit.md)：AllowN round-robin 分发。

**源码说明与使用片段**（原文）：

AllowN round-robin 分发。


**参数说明**：

- `n` (`int`)：本次需要消耗的请求/令牌数；非正值直接返回 true。分片版本仅检查轮转选中的一片，不能合并多片余额。float64 参数须为有限值。

**返回值签名**：`bool`。true 表示本次额度检查通过；false 表示当前额度不足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AllowN(n)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：每次轮转选择一片，只检查该片，不搜索其他片空闲额度、不跨片拼接 AllowN。拒绝和非正请求也推进外层轮转，非正请求直接成功且不扣额度；正数大于单片容量可拒绝，即使全局汇总仍有余额。TokenBucket 的有限小数请求按整笔额度处理，配置按每片应用；SlidingWindow 配置按总量分配。GetShard 直接请求会改变该片余额但不推进外层轮转；无后台 goroutine 或 Close 方法。

## `*ShardedSlidingWindowRateLimiter.GetShard`

```go
func (sw *ShardedSlidingWindowRateLimiter) GetShard(idx int) *SlidingWindowRateLimiter
```

[实现](../../internal/ratelimit/sharded.go#L369) · [行为专题](../ratelimit.md)：GetShard 获取指定分片实例。

**源码说明与使用片段**（原文）：

GetShard 获取指定分片实例。


**参数说明**：

- `idx` (`int`)：目标分片的零起始索引；负值或不小于分片数时返回 nil。

**返回值签名**：`*SlidingWindowRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShard(idx)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：负索引或超出分片数返回 nil；有效索引返回原实例指针，不复制。直接使用该片绕过外层轮转和许可记录，不能把直接获取与外层 Release 混用；直接调整也可能使外层汇总配置失真，统一管理生命周期。

## `*ShardedSlidingWindowRateLimiter.ShardCount`

```go
func (sw *ShardedSlidingWindowRateLimiter) ShardCount() int
```

[实现](../../internal/ratelimit/sharded.go#L359) · [行为专题](../ratelimit.md)：ShardCount 返回分片数。

**源码说明与使用片段**（原文）：

ShardCount 返回分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedSlidingWindowRateLimiter.TotalLimit`

```go
func (sw *ShardedSlidingWindowRateLimiter) TotalLimit() int
```

[实现](../../internal/ratelimit/sharded.go#L364) · [行为专题](../ratelimit.md)：TotalLimit 返回各分片限额之和；目标小于分片数时每片至少为 1。

**源码说明与使用片段**（原文）：

TotalLimit 返回各分片限额之和；目标小于分片数时每片至少为 1。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalLimit()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedSlidingWindowSubBuilder.Build`

```go
func (b *ShardedSlidingWindowSubBuilder) Build() *ShardedSlidingWindowRateLimiter
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L608) · [行为专题](../ratelimit.md)：Build 构建 ShardedSlidingWindowRateLimiter 实例。

**源码说明与使用片段**（原文）：

Build 构建 ShardedSlidingWindowRateLimiter 实例。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedSlidingWindowRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：每次创建独立实例，不回写规范化后的字段；不继承所保存 context 的取消、期限和业务值，RateLimiter 及其分片保留局部日志设置，阻塞获取须另传 context。Window 原样传入，非正值在后续检查立即过期；单片非正 Limit 拒绝正数请求，分片总 Limit 则分配至每片至少 1。需要有效窗口/额度时显式设置。

## `*ShardedSlidingWindowSubBuilder.Context`

```go
func (b *ShardedSlidingWindowSubBuilder) Context(ctx context.Context) *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L590) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*ShardedSlidingWindowSubBuilder.DefaultLimit`

```go
func (b *ShardedSlidingWindowSubBuilder) DefaultLimit() *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L619) · [行为专题](../ratelimit.md)：DefaultLimit 使用默认限制次数（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultLimit 使用默认限制次数（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLimit()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedSlidingWindowSubBuilder.DefaultLogger`

```go
func (b *ShardedSlidingWindowSubBuilder) DefaultLogger() *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L602) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedSlidingWindowSubBuilder.DefaultShards`

```go
func (b *ShardedSlidingWindowSubBuilder) DefaultShards() *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L613) · [行为专题](../ratelimit.md)：DefaultShards 使用默认分片数（core.IO()，最少 2）。

**源码说明与使用片段**（原文）：

DefaultShards 使用默认分片数（core.IO()，最少 2）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShards()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedSlidingWindowSubBuilder.DefaultWindow`

```go
func (b *ShardedSlidingWindowSubBuilder) DefaultWindow() *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L625) · [行为专题](../ratelimit.md)：DefaultWindow 使用默认时间窗口（1 秒）。

**源码说明与使用片段**（原文）：

DefaultWindow 使用默认时间窗口（1 秒）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWindow()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedSlidingWindowSubBuilder.Limit`

```go
func (b *ShardedSlidingWindowSubBuilder) Limit(n int) *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L578) · [行为专题](../ratelimit.md)：Limit 设置总限制次数（每窗口），平均分配到各分片。

**源码说明与使用片段**（原文）：

Limit 设置总限制次数（每窗口），平均分配到各分片。


**参数说明**：

- `n` (`int`)：各片合计的窗口请求额度，余数优先分配且每片至少 1；目标小于片数（含非正值）时实际总额为片数。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Limit(n)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.limit = n
```

## `*ShardedSlidingWindowSubBuilder.Logger`

```go
func (b *ShardedSlidingWindowSubBuilder) Logger(l core.Logger) *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L596) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedSlidingWindowSubBuilder.Shards`

```go
func (b *ShardedSlidingWindowSubBuilder) Shards(n int) *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L572) · [行为专题](../ratelimit.md)：Shards 设置水平分片数。

**源码说明与使用片段**（原文）：

Shards 设置水平分片数。


**参数说明**：

- `n` (`int`)：Shards 设置水平分片数。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.shards = n
```

## `*ShardedSlidingWindowSubBuilder.Window`

```go
func (b *ShardedSlidingWindowSubBuilder) Window(d time.Duration) *ShardedSlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L584) · [行为专题](../ratelimit.md)：Window 设置时间窗口大小。

**源码说明与使用片段**（原文）：

Window 设置时间窗口大小。


**参数说明**：

- `d` (`time.Duration`)：滑动窗口保留请求记录的时长；需要有限窗口限制时传正值。非正值不会自动使用 DefaultWindow，旧记录通常在下次检查时立即过期。

**返回值签名**：`*ShardedSlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Window(d)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.window = d
```

## `*ShardedTokenBucket.Allow`

```go
func (st *ShardedTokenBucket) Allow() bool
```

[实现](../../internal/ratelimit/sharded.go#L277) · [行为专题](../ratelimit.md)：Allow 获取一个令牌，round-robin 分发。

**源码说明与使用片段**（原文）：

Allow 获取一个令牌，round-robin 分发。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示本次额度检查通过；false 表示当前额度不足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Allow()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：每次轮转选择一片，只检查该片，不搜索其他片空闲额度、不跨片拼接 AllowN。拒绝和非正请求也推进外层轮转，非正请求直接成功且不扣额度；正数大于单片容量可拒绝，即使全局汇总仍有余额。TokenBucket 的有限小数请求按整笔额度处理，配置按每片应用；SlidingWindow 配置按总量分配。GetShard 直接请求会改变该片余额但不推进外层轮转；无后台 goroutine 或 Close 方法。

## `*ShardedTokenBucket.AllowN`

```go
func (st *ShardedTokenBucket) AllowN(n float64) bool
```

[实现](../../internal/ratelimit/sharded.go#L283) · [行为专题](../ratelimit.md)：AllowN 获取 n 个令牌，round-robin 分发。

**源码说明与使用片段**（原文）：

AllowN 获取 n 个令牌，round-robin 分发。


**参数说明**：

- `n` (`float64`)：本次需要消耗的请求/令牌数；非正值直接返回 true。分片版本仅检查轮转选中的一片，不能合并多片余额。float64 参数须为有限值。

**返回值签名**：`bool`。true 表示本次额度检查通过；false 表示当前额度不足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AllowN(n)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：每次轮转选择一片，只检查该片，不搜索其他片空闲额度、不跨片拼接 AllowN。拒绝和非正请求也推进外层轮转，非正请求直接成功且不扣额度；正数大于单片容量可拒绝，即使全局汇总仍有余额。TokenBucket 的有限小数请求按整笔额度处理，配置按每片应用；SlidingWindow 配置按总量分配。GetShard 直接请求会改变该片余额但不推进外层轮转；无后台 goroutine 或 Close 方法。

## `*ShardedTokenBucket.GetShard`

```go
func (st *ShardedTokenBucket) GetShard(idx int) *TokenBucket
```

[实现](../../internal/ratelimit/sharded.go#L294) · [行为专题](../ratelimit.md)：GetShard 获取指定分片的 TokenBucket 实例。

**源码说明与使用片段**（原文）：

GetShard 获取指定分片的 TokenBucket 实例。


**参数说明**：

- `idx` (`int`)：目标分片的零起始索引；负值或不小于分片数时返回 nil。

**返回值签名**：`*TokenBucket`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShard(idx)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：负索引或超出分片数返回 nil；有效索引返回原实例指针，不复制。直接使用该片绕过外层轮转和许可记录，不能把直接获取与外层 Release 混用；直接调整也可能使外层汇总配置失真，统一管理生命周期。

## `*ShardedTokenBucket.ShardCount`

```go
func (st *ShardedTokenBucket) ShardCount() int
```

[实现](../../internal/ratelimit/sharded.go#L289) · [行为专题](../ratelimit.md)：ShardCount 返回分片数。

**源码说明与使用片段**（原文）：

ShardCount 返回分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。

## `*ShardedTokenBucketSubBuilder.Build`

```go
func (b *ShardedTokenBucketSubBuilder) Build() *ShardedTokenBucket
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L535) · [行为专题](../ratelimit.md)：Build 构建 ShardedTokenBucket 实例。

**源码说明与使用片段**（原文）：

Build 构建 ShardedTokenBucket 实例。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedTokenBucket`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：每次创建独立实例，不回写规范化后的字段；不继承所保存 context 的取消、期限和业务值，RateLimiter 及其分片保留局部日志设置，阻塞获取须另传 context。Rate/Capacity 原样传入，未配置仍为 0，不自动采用 DefaultRate/DefaultCapacity；须使用有限正数。分片对每片应用相同速率与容量。

## `*ShardedTokenBucketSubBuilder.Capacity`

```go
func (b *ShardedTokenBucketSubBuilder) Capacity(n float64) *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L511) · [行为专题](../ratelimit.md)：Capacity 设置最大令牌容量。

**源码说明与使用片段**（原文）：

Capacity 设置最大令牌容量。


**参数说明**：

- `n` (`float64`)：最大积压及初始令牌数，应为有限正数；零值和负值原样传入构造器，不自动填入 DefaultCapacity。分片版本对每片应用同一容量。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Capacity(n)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.capacity = n
```

## `*ShardedTokenBucketSubBuilder.Context`

```go
func (b *ShardedTokenBucketSubBuilder) Context(ctx context.Context) *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L517) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*ShardedTokenBucketSubBuilder.DefaultCapacity`

```go
func (b *ShardedTokenBucketSubBuilder) DefaultCapacity() *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L552) · [行为专题](../ratelimit.md)：DefaultCapacity 使用默认容量（core.IO() × 2）。

**源码说明与使用片段**（原文）：

DefaultCapacity 使用默认容量（core.IO() × 2）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultCapacity()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedTokenBucketSubBuilder.DefaultLogger`

```go
func (b *ShardedTokenBucketSubBuilder) DefaultLogger() *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L529) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedTokenBucketSubBuilder.DefaultRate`

```go
func (b *ShardedTokenBucketSubBuilder) DefaultRate() *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L546) · [行为专题](../ratelimit.md)：DefaultRate 使用默认令牌生成速率（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultRate 使用默认令牌生成速率（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultRate()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedTokenBucketSubBuilder.DefaultShards`

```go
func (b *ShardedTokenBucketSubBuilder) DefaultShards() *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L540) · [行为专题](../ratelimit.md)：DefaultShards 使用默认分片数（core.IO()，最少 2）。

**源码说明与使用片段**（原文）：

DefaultShards 使用默认分片数（core.IO()，最少 2）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShards()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*ShardedTokenBucketSubBuilder.Logger`

```go
func (b *ShardedTokenBucketSubBuilder) Logger(l core.Logger) *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L523) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*ShardedTokenBucketSubBuilder.Rate`

```go
func (b *ShardedTokenBucketSubBuilder) Rate(n float64) *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L505) · [行为专题](../ratelimit.md)：Rate 设置每片每秒生成的令牌数。

**源码说明与使用片段**（原文）：

Rate 设置每片每秒生成的令牌数。


**参数说明**：

- `n` (`float64`)：每秒补充的令牌数，应为有限正数；零值和负值原样传入构造器，不自动填入 DefaultRate。分片版本对每片应用同一速率。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Rate(n)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.rate = n
```

## `*ShardedTokenBucketSubBuilder.Shards`

```go
func (b *ShardedTokenBucketSubBuilder) Shards(n int) *ShardedTokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L499) · [行为专题](../ratelimit.md)：Shards 设置水平分片数。

**源码说明与使用片段**（原文）：

Shards 设置水平分片数。


**参数说明**：

- `n` (`int`)：Shards 设置水平分片数。

**返回值签名**：`*ShardedTokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.shards = n
```

## `*SlidingWindowRateLimiter.Allow`

```go
func (sw *SlidingWindowRateLimiter) Allow() bool
```

[实现](../../internal/ratelimit/slidingwindow.go#L45) · [行为专题](../ratelimit.md)：Allow 检查是否允许 1 次请求通过。线程安全。

**源码说明与使用片段**（原文）：

Allow 检查是否允许 1 次请求通过。线程安全。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示本次额度检查通过；false 表示当前额度不足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Allow()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：同步在锁内清理窗口并检查，允许时立即记录请求数量；拒绝不占额度。AllowN 的非正 n 直接 true 且不记录或清理，正数请求原子地全量接受或拒绝；window<=0 时此前记录全部过期，仍检查单次请求限额，正数窗口恰好到边界的记录过期。生产校验正数窗口，无 context、Release 或 Close，也不自动限制业务并发。

## `*SlidingWindowRateLimiter.AllowN`

```go
func (sw *SlidingWindowRateLimiter) AllowN(n int) bool
```

[实现](../../internal/ratelimit/slidingwindow.go#L70) · [行为专题](../ratelimit.md)：AllowN 检查是否允许 n 次请求同时通过。

**源码说明与使用片段**（原文）：

AllowN 检查是否允许 n 次请求同时通过。

参数：

  - n：请求次数（n \<= 0 时始终返回 true）


**参数说明**：

- `n` (`int`)：本次需要消耗的请求/令牌数；非正值直接返回 true。分片版本仅检查轮转选中的一片，不能合并多片余额。float64 参数须为有限值。

**返回值签名**：`bool`。true 表示本次额度检查通过；false 表示当前额度不足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AllowN(n)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：同步在锁内清理窗口并检查，允许时立即记录请求数量；拒绝不占额度。AllowN 的非正 n 直接 true 且不记录或清理，正数请求原子地全量接受或拒绝；window<=0 时此前记录全部过期，仍检查单次请求限额，正数窗口恰好到边界的记录过期。生产校验正数窗口，无 context、Release 或 Close，也不自动限制业务并发。

## `*SlidingWindowSubBuilder.Build`

```go
func (b *SlidingWindowSubBuilder) Build() *SlidingWindowRateLimiter
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L267) · [行为专题](../ratelimit.md)：Build 构建 SlidingWindowRateLimiter 实例。

**源码说明与使用片段**（原文）：

Build 构建 SlidingWindowRateLimiter 实例。


**参数说明**：无显式参数。

**返回值签名**：`*SlidingWindowRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：每次创建独立实例，不回写规范化后的字段；不继承所保存 context 的取消、期限和业务值，RateLimiter 及其分片保留局部日志设置，阻塞获取须另传 context。Window 原样传入，非正值在后续检查立即过期；单片非正 Limit 拒绝正数请求，分片总 Limit 则分配至每片至少 1。需要有效窗口/额度时显式设置。

## `*SlidingWindowSubBuilder.Context`

```go
func (b *SlidingWindowSubBuilder) Context(ctx context.Context) *SlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L249) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*SlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*SlidingWindowSubBuilder.DefaultLimit`

```go
func (b *SlidingWindowSubBuilder) DefaultLimit() *SlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L272) · [行为专题](../ratelimit.md)：DefaultLimit 使用默认限制次数（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultLimit 使用默认限制次数（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*SlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLimit()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*SlidingWindowSubBuilder.DefaultLogger`

```go
func (b *SlidingWindowSubBuilder) DefaultLogger() *SlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L261) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*SlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*SlidingWindowSubBuilder.DefaultWindow`

```go
func (b *SlidingWindowSubBuilder) DefaultWindow() *SlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L278) · [行为专题](../ratelimit.md)：DefaultWindow 使用默认时间窗口（1 秒）。

**源码说明与使用片段**（原文）：

DefaultWindow 使用默认时间窗口（1 秒）。


**参数说明**：无显式参数。

**返回值签名**：`*SlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWindow()
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*SlidingWindowSubBuilder.Limit`

```go
func (b *SlidingWindowSubBuilder) Limit(n int) *SlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L237) · [行为专题](../ratelimit.md)：Limit 设置时间窗口内允许的最大请求数。

**源码说明与使用片段**（原文）：

Limit 设置时间窗口内允许的最大请求数。


**参数说明**：

- `n` (`int`)：窗口内允许的请求数；非正值使正数请求被拒绝，不会自动使用 DefaultLimit。

**返回值签名**：`*SlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Limit(n)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.limit = n
```

## `*SlidingWindowSubBuilder.Logger`

```go
func (b *SlidingWindowSubBuilder) Logger(l core.Logger) *SlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L255) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*SlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*SlidingWindowSubBuilder.Window`

```go
func (b *SlidingWindowSubBuilder) Window(d time.Duration) *SlidingWindowSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L243) · [行为专题](../ratelimit.md)：Window 设置滑动窗口的时间范围。

**源码说明与使用片段**（原文）：

Window 设置滑动窗口的时间范围。


**参数说明**：

- `d` (`time.Duration`)：滑动窗口保留请求记录的时长；需要有限窗口限制时传正值。非正值不会自动使用 DefaultWindow，旧记录通常在下次检查时立即过期。

**返回值签名**：`*SlidingWindowSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Window(d)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.window = d
```

## `*Token.Release`

```go
func (t *Token) Release()
```

[实现](../../internal/ratelimit/ratelimit.go#L533) · [行为专题](../ratelimit.md)：Release 归还令牌；重复或并发调用只归还一次。

**源码说明与使用片段**（原文）：

Release 归还令牌；重复或并发调用只归还一次。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Release()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：仅为 Token 成功返回的句柄调用；同一指针可重复或并发 Release，只归还一次。不要复制已使用的 Token（包含 sync.Once），也不要再对同一次许可调用所属限流器 Release。

## `*TokenBucket.Allow`

```go
func (tb *TokenBucket) Allow() bool
```

[实现](../../internal/ratelimit/tokenbucket.go#L49) · [行为专题](../ratelimit.md)：Allow 尝试消耗 1 个令牌，成功返回 true。线程安全，无阻塞。

**源码说明与使用片段**（原文）：

Allow 尝试消耗 1 个令牌，成功返回 true。线程安全，无阻塞。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示本次额度检查通过；false 表示当前额度不足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Allow()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：正数检查先按经过时间补充并截至容量，再整笔接受或拒绝；拒绝不扣额度但更新时间与补充余额。AllowN 非正 n 直接 true，不补充或更新时间。初始满桶且不能积攒超过容量的空闲额度；Allow 固定需 1，容量小于 1 时永不能通过，但小数 AllowN 可用。配置应有限且容量覆盖最大单笔请求，浮点额度不作精确十进制账目，无后台资源或 Close。

## `*TokenBucket.AllowN`

```go
func (tb *TokenBucket) AllowN(n float64) bool
```

[实现](../../internal/ratelimit/tokenbucket.go#L57) · [行为专题](../ratelimit.md)：AllowN 尝试消耗 n 个令牌，成功返回 true。

**源码说明与使用片段**（原文）：

AllowN 尝试消耗 n 个令牌，成功返回 true。

参数：

  - n：需要消耗的令牌数（n \<= 0 时始终返回 true）


**参数说明**：

- `n` (`float64`)：本次需要消耗的请求/令牌数；非正值直接返回 true。分片版本仅检查轮转选中的一片，不能合并多片余额。float64 参数须为有限值。

**返回值签名**：`bool`。true 表示本次额度检查通过；false 表示当前额度不足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AllowN(n)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：正数检查先按经过时间补充并截至容量，再整笔接受或拒绝；拒绝不扣额度但更新时间与补充余额。AllowN 非正 n 直接 true，不补充或更新时间。初始满桶且不能积攒超过容量的空闲额度；Allow 固定需 1，容量小于 1 时永不能通过，但小数 AllowN 可用。配置应有限且容量覆盖最大单笔请求，浮点额度不作精确十进制账目，无后台资源或 Close。

## `*TokenBucketSubBuilder.Build`

```go
func (b *TokenBucketSubBuilder) Build() *TokenBucket
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L207) · [行为专题](../ratelimit.md)：Build 构建 TokenBucket 实例。

**源码说明与使用片段**（原文）：

Build 构建 TokenBucket 实例。


**参数说明**：无显式参数。

**返回值签名**：`*TokenBucket`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：每次创建独立实例，不回写规范化后的字段；不继承所保存 context 的取消、期限和业务值，RateLimiter 及其分片保留局部日志设置，阻塞获取须另传 context。Rate/Capacity 原样传入，未配置仍为 0，不自动采用 DefaultRate/DefaultCapacity；须使用有限正数。分片对每片应用相同速率与容量。

## `*TokenBucketSubBuilder.Capacity`

```go
func (b *TokenBucketSubBuilder) Capacity(n float64) *TokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L183) · [行为专题](../ratelimit.md)：Capacity 设置最大令牌容量（允许的突发流量）。

**源码说明与使用片段**（原文）：

Capacity 设置最大令牌容量（允许的突发流量）。


**参数说明**：

- `n` (`float64`)：最大积压及初始令牌数，应为有限正数；零值和负值原样传入构造器，不自动填入 DefaultCapacity。分片版本对每片应用同一容量。

**返回值签名**：`*TokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Capacity(n)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.capacity = n
```

## `*TokenBucketSubBuilder.Context`

```go
func (b *TokenBucketSubBuilder) Context(ctx context.Context) *TokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L189) · [行为专题](../ratelimit.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*TokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：替换构建器保存的 context 并确保 trace_id，须非 nil；保留当前局部日志设置，Build 不继承其取消、期限和业务值。RateLimiter 及其分片实例保留局部日志设置，WithTraceID 换绑后仍使用它；其他模式没有内部日志调用。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。

## `*TokenBucketSubBuilder.DefaultCapacity`

```go
func (b *TokenBucketSubBuilder) DefaultCapacity() *TokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L218) · [行为专题](../ratelimit.md)：DefaultCapacity 使用默认容量（core.IO() × 2）。

**源码说明与使用片段**（原文）：

DefaultCapacity 使用默认容量（core.IO() × 2）。


**参数说明**：无显式参数。

**返回值签名**：`*TokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultCapacity()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*TokenBucketSubBuilder.DefaultLogger`

```go
func (b *TokenBucketSubBuilder) DefaultLogger() *TokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L201) · [行为专题](../ratelimit.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*TokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*TokenBucketSubBuilder.DefaultRate`

```go
func (b *TokenBucketSubBuilder) DefaultRate() *TokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L212) · [行为专题](../ratelimit.md)：DefaultRate 使用默认令牌生成速率（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultRate 使用默认令牌生成速率（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*TokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultRate()
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。

## `*TokenBucketSubBuilder.Logger`

```go
func (b *TokenBucketSubBuilder) Logger(l core.Logger) *TokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L195) · [行为专题](../ratelimit.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*TokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*TokenBucketSubBuilder.Rate`

```go
func (b *TokenBucketSubBuilder) Rate(n float64) *TokenBucketSubBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L177) · [行为专题](../ratelimit.md)：Rate 设置每秒生成的令牌数。

**源码说明与使用片段**（原文）：

Rate 设置每秒生成的令牌数。


**参数说明**：

- `n` (`float64`)：每秒补充的令牌数，应为有限正数；零值和负值原样传入构造器，不自动填入 DefaultRate。分片版本对每片应用同一速率。

**返回值签名**：`*TokenBucketSubBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Rate(n)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.rate = n
```

## `DefaultShardCount`

```go
func DefaultShardCount() int
```

[实现](../../internal/ratelimit/helper.go#L13) · [行为专题](../ratelimit.md)：DefaultShardCount 返回默认分片数（core.IO()，最少 2）。

**源码说明与使用片段**（原文）：

DefaultShardCount 返回默认分片数（core.IO()，最少 2）。 可在创建 ShardedPool/ShardedGroup/ShardedRateLimiter 等时使用。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.DefaultShardCount()
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：返回 max(2,IO())，即至少 2 的 NumCPU×2，不读取 GOMAXPROCS，也不分配资源。实际总容量可能因每片至少 1 而超过请求额度，片数应按资源预算显式选择。

## `NewAdaptiveRateLimiter`

```go
func NewAdaptiveRateLimiter(minRate, maxRate int) *AdaptiveRateLimiter
```

[实现](../../internal/ratelimit/adaptive.go#L48) · [行为专题](../ratelimit.md)：NewAdaptiveRateLimiter 创建自适应限流器。

**源码说明与使用片段**（原文）：

NewAdaptiveRateLimiter 创建自适应限流器。 初始并发度为归一化边界的整数中点，以 minRate + (maxRate-minRate)/2 避免加法溢出。

参数：

  - minRate：最小并发度（负载低时也不会低于此值）
  - maxRate：最大并发度（负载高时也不会超过此值）

使用示例：

	// 并发度在 5-100 之间自适应调整
	al := ratelimit.NewAdaptiveRateLimiter(5, 100)


**参数说明**：

- `minRate` (`int`)：最小并发度（负载低时也不会低于此值）
- `maxRate` (`int`)：最大并发度（负载高时也不会超过此值）

**返回值签名**：`*AdaptiveRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewAdaptiveRateLimiter(minRate, maxRate)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：minRate 非正按 1，maxRate 不大于有效 min 时提升至 min，初始并发以 min+(max-min)/2 计算整数中点，避免边界相加溢出；无需后台补充或 Close。成功 Acquire 必须配对 Release，业务完成显式 RecordOk/Failure 才调整并发。边界须满足正 int32 和内存及初始化耗时预算；不自动执行回调。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
maxRate = minRate
```

## `NewRateLimiter`

```go
func NewRateLimiter(rate int, perDuration time.Duration) *RateLimiter
```

[实现](../../internal/ratelimit/ratelimit.go#L91) · [行为专题](../ratelimit.md)：NewRateLimiter 创建定时补充令牌的限流器。

**源码说明与使用片段**（原文）：

NewRateLimiter 创建定时补充令牌的限流器。

参数：

  - rate：每 perDuration 内允许的操作次数，rate \<= 0 时使用 core.IO()
  - perDuration：时间窗口大小

使用示例：

	// 每秒补充 100 个令牌，初始可突发 100 个；请求消耗额度，令牌由定时器补充
	rl := ratelimit.NewRateLimiter(100, time.Second)
	defer rl.Close()
	if err := rl.Wait(ctx); err == nil {
	    doRequest()
	}


**参数说明**：

- `rate` (`int`)：每 perDuration 内允许的操作次数，rate <= 0 时使用 core.IO()
- `perDuration` (`time.Duration`)：每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。

**返回值签名**：`*RateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewRateLimiter(rate, perDuration)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建满桶及定时补充 goroutine，非正 rate 按 IO()；普通容量等于有效 rate，Burst 容量至少为有效 rate。每 100ms 按累计时间补额度，非正 perDuration 每 tick 补满；不是精确滑动窗口或严格并发上限。容量按内存预算且 rate 在正 int32 范围内，最终 Close；Release 会提前恢复额度。

## `NewRateLimiterWithBurst`

```go
func NewRateLimiterWithBurst(rate int, perDuration time.Duration, burst int) *RateLimiter
```

[实现](../../internal/ratelimit/ratelimit.go#L124) · [行为专题](../ratelimit.md)：NewRateLimiterWithBurst 创建支持突发容量的限流器。

**源码说明与使用片段**（原文）：

NewRateLimiterWithBurst 创建支持突发容量的限流器。

参数：

  - rate：每 perDuration 内允许的操作次数
  - perDuration：时间窗口大小
  - burst：突发容量，低于 rate 时按 rate 处理，初始令牌数等于实际容量

使用示例：

	// 每秒 50 个，初始可突发 200 个
	rl := ratelimit.NewRateLimiterWithBurst(50, time.Second, 200)


**参数说明**：

- `rate` (`int`)：每 perDuration 内允许的操作次数
- `perDuration` (`time.Duration`)：每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。
- `burst` (`int`)：突发容量，低于 rate 时按 rate 处理，初始令牌数等于实际容量

**返回值签名**：`*RateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewRateLimiterWithBurst(rate, perDuration, burst)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建满桶及定时补充 goroutine，非正 rate 按 IO()；普通容量等于有效 rate，Burst 容量至少为有效 rate。每 100ms 按累计时间补额度，非正 perDuration 每 tick 补满；不是精确滑动窗口或严格并发上限。容量按内存预算且 rate 在正 int32 范围内，最终 Close；Release 会提前恢复额度。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
cap = rate
```

## `NewRatelimitBuilder`

```go
func NewRatelimitBuilder(ctx context.Context) *RatelimitBuilder
```

[实现](../../internal/ratelimit/ratelimit_builder.go#L36) · [行为专题](../ratelimit.md)：NewRatelimitBuilder 创建限流器链式构建器。

**源码说明与使用片段**（原文）：

NewRatelimitBuilder 创建限流器链式构建器。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*RatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewRatelimitBuilder(ctx)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：只保存 context，不立即创建限流器；后续模式入口创建独立子构建器，Build 时才创建资源。保存的 context 当前不传给限流实例，Acquire/Wait 须单独传 context；需要后台补充的实例最终 Close。

## `NewShardedAdaptiveRateLimiter`

```go
func NewShardedAdaptiveRateLimiter(shards int, minRate int, maxRate int) *ShardedAdaptiveRateLimiter
```

[实现](../../internal/ratelimit/sharded.go#L402) · [行为专题](../ratelimit.md)：NewShardedAdaptiveRateLimiter 创建分片自适应限流器。

**源码说明与使用片段**（原文）：

NewShardedAdaptiveRateLimiter 创建分片自适应限流器。 shards\<=0 时使用 core.IO()（runtime.NumCPU()\*2），最少 2 片。 minRate 和 maxRate 各自按余数优先分配，保证每个分片 max >= min。


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `minRate` (`int`)：自适应并发度的最小/最大总额度；须满足正数及 minRate <= maxRate。
- `maxRate` (`int`)：自适应并发度的最小/最大总额度；须满足正数及 minRate <= maxRate。

**返回值签名**：`*ShardedAdaptiveRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewShardedAdaptiveRateLimiter(shards, minRate, maxRate)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建多片无定时补充的许可器，非正片数按 max(2,IO())；min/max 总量分别按余数优先分配，每片至少 1 且 max>=min，实际总量可被提升。获取及结果记录各自轮转，不绑定同一片；成功获取配对外层 Release，最终 Close，边界遵循单片 int32/内存预算。

## `NewShardedRateLimiter`

```go
func NewShardedRateLimiter(shards int, rate int, perDuration time.Duration) *ShardedRateLimiter
```

[实现](../../internal/ratelimit/sharded.go#L95) · [行为专题](../ratelimit.md)：NewShardedRateLimiter 创建分片限流器。

**源码说明与使用片段**（原文）：

NewShardedRateLimiter 创建分片限流器。 shards\<=0 时使用 core.IO()（runtime.NumCPU()\*2），最少 2 片。 rate >= shards 时总速率为 rate，余数优先分配到前几个分片； rate \< shards 时为兼容旧行为每片至少 1，总速率为 shards。

参数：

  - shards：分片数
  - rate：总允许速率（每 perDuration）
  - perDuration：时间窗口


**参数说明**：

- `shards` (`int`)：分片数
- `rate` (`int`)：总允许速率（每 perDuration）
- `perDuration` (`time.Duration`)：每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。

**返回值签名**：`*ShardedRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewShardedRateLimiter(shards, rate, perDuration)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建各片满桶及各自补充 goroutine，非正片数按 max(2,IO())。rate/burst 总量分别按余数优先分配，每片至少 1，容量至少为片速率；总量低于片数会被提升。获取只轮询选择一片，不搜索其他余额，最终 Close 全部；配置总量和各片 int32 速率须符合内存预算。

## `NewShardedRateLimiterWithBurst`

```go
func NewShardedRateLimiterWithBurst(shards int, rate int, perDuration time.Duration, burst int) *ShardedRateLimiter
```

[实现](../../internal/ratelimit/sharded.go#L116) · [行为专题](../ratelimit.md)：NewShardedRateLimiterWithBurst 创建支持突发容量的分片限流器。

**源码说明与使用片段**（原文）：

NewShardedRateLimiterWithBurst 创建支持突发容量的分片限流器。 rate 和 burst 各自按余数优先分配，每片至少为 1；实际片容量至少等于该片速率。 总配置小于片数时，总额度会大于请求值；burst 小于 rate 时容量会提升。


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `rate` (`int`)：每个 perDuration 窗口的总操作额度；时间窗口及非正值处理由对应构造器决定。
- `perDuration` (`time.Duration`)：每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。
- `burst` (`int`)：初始可用的突发额度与缓冲容量，构造器会与 rate 比较确定实际容量。

**返回值签名**：`*ShardedRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewShardedRateLimiterWithBurst(shards, rate, perDuration, burst)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建各片满桶及各自补充 goroutine，非正片数按 max(2,IO())。rate/burst 总量分别按余数优先分配，每片至少 1，容量至少为片速率；总量低于片数会被提升。获取只轮询选择一片，不搜索其他余额，最终 Close 全部；配置总量和各片 int32 速率须符合内存预算。

## `NewShardedSlidingWindowRateLimiter`

```go
func NewShardedSlidingWindowRateLimiter(shards int, limit int, window time.Duration) *ShardedSlidingWindowRateLimiter
```

[实现](../../internal/ratelimit/sharded.go#L326) · [行为专题](../ratelimit.md)：NewShardedSlidingWindowRateLimiter 创建分片滑动窗口限流器。

**源码说明与使用片段**（原文）：

NewShardedSlidingWindowRateLimiter 创建分片滑动窗口限流器。 shards\<=0 时使用 core.IO()（runtime.NumCPU()\*2），最少 2 片。 limit 按余数优先分配，总 limit 精确等于传入值（limit>=shards时）。


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `limit` (`int`)：滑动时间窗口内允许的请求总数；分片构造器将其分配到各片。
- `window` (`time.Duration`)：滑动窗口保留请求记录的时长；正数形成窗口限制，非正值使旧记录在后续检查中立即过期，不自动恢复默认值。

**返回值签名**：`*ShardedSlidingWindowRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewShardedSlidingWindowRateLimiter(shards, limit, window)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：非正片数按 max(2,IO())，总 limit 按余数优先分配且每片至少 1，总额度低于片数时被提升；window 原样传入，应为正。正数窗口内接受数不超过 TotalLimit，但请求只选择一片，其他片有余额也可拒绝，不跨片拼接额度；无后台资源和 Close，内存随窗口内接受的请求数增长。

## `NewShardedTokenBucket`

```go
func NewShardedTokenBucket(shards int, rate float64, capacity float64) *ShardedTokenBucket
```

[实现](../../internal/ratelimit/sharded.go#L262) · [行为专题](../ratelimit.md)：NewShardedTokenBucket 创建分片令牌桶。

**源码说明与使用片段**（原文）：

NewShardedTokenBucket 创建分片令牌桶。 shards\<=0 时使用 core.IO()（runtime.NumCPU()\*2），最少 2 片。


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `rate` (`float64`)：每秒生成的令牌数；分片令牌桶构造器将此值用于每个分片。
- `capacity` (`float64`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。

**返回值签名**：`*ShardedTokenBucket`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewShardedTokenBucket(shards, rate, capacity)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：速率与容量应为有限的正数；直接构造器不替调用方校验它们。分片令牌桶对每片应用相同速率与容量；AllowN 单次只访问一片，不会跨片汇集额度。

## `NewSlidingWindowRateLimiter`

```go
func NewSlidingWindowRateLimiter(limit int, window time.Duration) *SlidingWindowRateLimiter
```

[实现](../../internal/ratelimit/slidingwindow.go#L37) · [行为专题](../ratelimit.md)：NewSlidingWindowRateLimiter 创建滑动窗口限流器。

**源码说明与使用片段**（原文）：

NewSlidingWindowRateLimiter 创建滑动窗口限流器。

参数：

  - limit：时间窗口内允许的最大请求数
  - window：时间窗口大小

使用示例：

	// 每 10 秒最多 100 次
	sw := ratelimit.NewSlidingWindowRateLimiter(100, 10*time.Second)


**参数说明**：

- `limit` (`int`)：时间窗口内允许的最大请求数
- `window` (`time.Duration`)：滑动窗口保留请求记录的时长；正数形成窗口限制，非正值使旧记录在后续检查中立即过期，不自动恢复默认值。

**返回值签名**：`*SlidingWindowRateLimiter`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewSlidingWindowRateLimiter(limit, window)
```

**生产建议**：复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。

**注意事项**：原样保存 limit/window，不补默认值；limit<=0 拒绝正数请求，window<=0（包括最小 time.Duration）使既有记录在后续正数检查中过期，但单次请求仍不能超过 limit。正数窗口的精确边界记录过期，生产入口校验正数窗口。无后台资源，无 Release/Close；成功请求记录在窗口内保留，按请求量预算内存，不能用它限制正在执行的并发。

## `NewTokenBucket`

```go
func NewTokenBucket(rate float64, capacity float64) *TokenBucket
```

[实现](../../internal/ratelimit/tokenbucket.go#L39) · [行为专题](../ratelimit.md)：NewTokenBucket 创建令牌桶。

**源码说明与使用片段**（原文）：

NewTokenBucket 创建令牌桶。

参数：

  - rate：每秒生成的令牌数
  - capacity：最大令牌容量（决定允许的突发流量大小）

使用示例：

	// 每秒 10 个令牌，最多存储 20 个（允许 2 秒的突发流量）
	tb := ratelimit.NewTokenBucket(10, 20)


**参数说明**：

- `rate` (`float64`)：每秒生成的令牌数
- `capacity` (`float64`)：最大令牌容量（决定允许的突发流量大小）

**返回值签名**：`*TokenBucket`。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.NewTokenBucket(rate, capacity)
```

**生产建议**：复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。

**注意事项**：速率与容量应为有限的正数；直接构造器不替调用方校验它们。分片令牌桶对每片应用相同速率与容量；AllowN 单次只访问一片，不会跨片汇集额度。

## `WithRateLimiter`

```go
func WithRateLimiter(rate int, perDuration time.Duration, fn func(rl *RateLimiter) error) error
```

[实现](../../internal/ratelimit/helper.go#L34) · [行为专题](../ratelimit.md)：WithRateLimiter 创建限流器并执行 fn，执行完毕后自动 Close。

**源码说明与使用片段**（原文）：

WithRateLimiter 创建限流器并执行 fn，执行完毕后自动 Close。 fn 接收已创建好的 \*RateLimiter，可返回 error。

示例：

	err := ratelimit.WithRateLimiter(1000, time.Second, func(rl *RateLimiter) error {
	    if err := rl.Wait(ctx); err != nil { return err }
	    return doRequest()
	})


**参数说明**：

- `rate` (`int`)：每个 perDuration 窗口的总操作额度；时间窗口及非正值处理由对应构造器决定。
- `perDuration` (`time.Duration`)：每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。
- `fn` (`func(rl *RateLimiter) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.WithRateLimiter(rate, perDuration, fn)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建对应限流器并同步调用非 nil fn，返回后 defer Close；error 只来自 fn且保持身份，panic 直接传播但仍关闭。返回前各片补充goroutine已退出，阻塞Acquire（含BlockForce）被关闭唤醒；关闭不等待已获取许可的业务，随后Release安全忽略。必须在作用域内等待自己启动的业务结束，不把实例带出作用域继续获取，参数规则遵循对应New构造器。

## `WithRateLimiterWithBurst`

```go
func WithRateLimiterWithBurst(rate int, perDuration time.Duration, burst int, fn func(rl *RateLimiter) error) error
```

[实现](../../internal/ratelimit/helper.go#L48) · [行为专题](../ratelimit.md)：WithRateLimiterWithBurst 创建带突发容量的限流器并执行 fn，执行完毕后自动 Close。

**源码说明与使用片段**（原文）：

WithRateLimiterWithBurst 创建带突发容量的限流器并执行 fn，执行完毕后自动 Close。

示例：

	err := ratelimit.WithRateLimiterWithBurst(50, time.Second, 200, func(rl *RateLimiter) error {
	    if err := rl.Wait(ctx); err != nil { return err }
	    return doRequest()
	})


**参数说明**：

- `rate` (`int`)：每个 perDuration 窗口的总操作额度；时间窗口及非正值处理由对应构造器决定。
- `perDuration` (`time.Duration`)：每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。
- `burst` (`int`)：初始可用的突发额度与缓冲容量，构造器会与 rate 比较确定实际容量。
- `fn` (`func(rl *RateLimiter) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.WithRateLimiterWithBurst(rate, perDuration, burst, fn)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建对应限流器并同步调用非 nil fn，返回后 defer Close；error 只来自 fn且保持身份，panic 直接传播但仍关闭。返回前各片补充goroutine已退出，阻塞Acquire（含BlockForce）被关闭唤醒；关闭不等待已获取许可的业务，随后Release安全忽略。必须在作用域内等待自己启动的业务结束，不把实例带出作用域继续获取，参数规则遵循对应New构造器。

## `WithShardedAdaptiveRateLimiter`

```go
func WithShardedAdaptiveRateLimiter(shards int, minRate int, maxRate int, fn func(sa *ShardedAdaptiveRateLimiter) error) error
```

[实现](../../internal/ratelimit/helper.go#L95) · [行为专题](../ratelimit.md)：WithShardedAdaptiveRateLimiter 创建分片自适应限流器并执行 fn，执行完毕后自动 Close。

**源码说明与使用片段**（原文）：

WithShardedAdaptiveRateLimiter 创建分片自适应限流器并执行 fn，执行完毕后自动 Close。 shards \<= 0 时自动使用 DefaultShardCount()。

示例：

	err := ratelimit.WithShardedAdaptiveRateLimiter(16, 32, 800, func(sa *ShardedAdaptiveRateLimiter) error {
	    if err := sa.Acquire(ctx); err != nil { return err }
	    defer sa.Release()
	    err := doRequest()
	    if err != nil { sa.RecordFailure() } else { sa.RecordOk() }
	    return err
	})


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `minRate` (`int`)：自适应并发度的最小/最大总额度；须满足正数及 minRate <= maxRate。
- `maxRate` (`int`)：自适应并发度的最小/最大总额度；须满足正数及 minRate <= maxRate。
- `fn` (`func(sa *ShardedAdaptiveRateLimiter) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.WithShardedAdaptiveRateLimiter(shards, minRate, maxRate, fn)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建对应限流器并同步调用非 nil fn，返回后 defer Close；error 只来自 fn且保持身份，panic 直接传播但仍关闭。返回前各片补充goroutine已退出，阻塞Acquire（含BlockForce）被关闭唤醒；关闭不等待已获取许可的业务，随后Release安全忽略。必须在作用域内等待自己启动的业务结束，不把实例带出作用域继续获取，参数规则遵循对应New构造器。

## `WithShardedRateLimiter`

```go
func WithShardedRateLimiter(shards int, rate int, perDuration time.Duration, fn func(rl *ShardedRateLimiter) error) error
```

[实现](../../internal/ratelimit/helper.go#L63) · [行为专题](../ratelimit.md)：WithShardedRateLimiter 创建分片限流器并执行 fn，执行完毕后自动 Close。

**源码说明与使用片段**（原文）：

WithShardedRateLimiter 创建分片限流器并执行 fn，执行完毕后自动 Close。 shards \<= 0 时自动使用 DefaultShardCount()。

示例：

	err := ratelimit.WithShardedRateLimiter(16, 10000, time.Second, func(rl *ShardedRateLimiter) error {
	    if err := rl.Wait(ctx); err != nil { return err }
	    return doRequest()
	})


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `rate` (`int`)：每个 perDuration 窗口的总操作额度；时间窗口及非正值处理由对应构造器决定。
- `perDuration` (`time.Duration`)：每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。
- `fn` (`func(rl *ShardedRateLimiter) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.WithShardedRateLimiter(shards, rate, perDuration, fn)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建对应限流器并同步调用非 nil fn，返回后 defer Close；error 只来自 fn且保持身份，panic 直接传播但仍关闭。返回前各片补充goroutine已退出，阻塞Acquire（含BlockForce）被关闭唤醒；关闭不等待已获取许可的业务，随后Release安全忽略。必须在作用域内等待自己启动的业务结束，不把实例带出作用域继续获取，参数规则遵循对应New构造器。

## `WithShardedRateLimiterWithBurst`

```go
func WithShardedRateLimiterWithBurst(shards int, rate int, perDuration time.Duration, burst int, fn func(rl *ShardedRateLimiter) error) error
```

[实现](../../internal/ratelimit/helper.go#L77) · [行为专题](../ratelimit.md)：WithShardedRateLimiterWithBurst 创建带突发容量的分片限流器并执行 fn，执行完毕后自动 Close。

**源码说明与使用片段**（原文）：

WithShardedRateLimiterWithBurst 创建带突发容量的分片限流器并执行 fn，执行完毕后自动 Close。

示例：

	err := ratelimit.WithShardedRateLimiterWithBurst(16, 100, time.Second, 500, func(rl *ShardedRateLimiter) error {
	    if err := rl.Wait(ctx); err != nil { return err }
	    return doRequest()
	})


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `rate` (`int`)：每个 perDuration 窗口的总操作额度；时间窗口及非正值处理由对应构造器决定。
- `perDuration` (`time.Duration`)：每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。
- `burst` (`int`)：初始可用的突发额度与缓冲容量，构造器会与 rate 比较确定实际容量。
- `fn` (`func(rl *ShardedRateLimiter) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
ratelimit.WithShardedRateLimiterWithBurst(shards, rate, perDuration, burst, fn)
```

**生产建议**：根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。

**注意事项**：立即创建对应限流器并同步调用非 nil fn，返回后 defer Close；error 只来自 fn且保持身份，panic 直接传播但仍关闭。返回前各片补充goroutine已退出，阻塞Acquire（含BlockForce）被关闭唤醒；关闭不等待已获取许可的业务，随后Release安全忽略。必须在作用域内等待自己启动的业务结束，不把实例带出作用域继续获取，参数规则遵循对应New构造器。

