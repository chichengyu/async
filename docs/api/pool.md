# pool 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [pool](../pool.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*MultiPoolBuilder[T].Config`

```go
func (b *MultiPoolBuilder[T]) Config(fn func(Config) Config) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L120) · [行为专题](../pool.md)：Config 函数式配置，允许通过闭包修改 Config

**源码说明与使用片段**（原文）：

Config 函数式配置，允许通过闭包修改 Config


**参数说明**：

- `fn` (`func(Config) Config`)：接收当前配置副本并返回需要应用的配置；必须返回修改后的副本，调用会同步执行。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Config(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：同步以当前 Config 值调用 fn 并用返回值整体替换，fn 须非 nil，panic 直接传播；闭包返回零值 Config 会把 MaxResults 改为 0（无限保留）。MultiPool 注入外部池时 cfg 不应用；配置须在 Run 前完成。

## `*MultiPoolBuilder[T].Context`

```go
func (b *MultiPoolBuilder[T]) Context(ctx context.Context) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L30) · [行为专题](../pool.md)：Context 链式设置上下文

**源码说明与使用片段**（原文）：

Context 链式设置上下文


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存非 nil context 并确保 trace_id，供后续 Run 的作用域使用；PoolBuilder 只在 FailFast 时将其绑定到池内部 context，提交仍须传递作用域 context。MultiPool 注入外部池时不应用 cfg/FailFast，外部池原设置继续生效；修改不影响已运行实例，配置不得与 Run 并发。

## `*MultiPoolBuilder[T].DefaultFailFast`

```go
func (b *MultiPoolBuilder[T]) DefaultFailFast() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L66) · [行为专题](../pool.md)：DefaultFailFast 关闭快速失败模式（默认关闭）。

**源码说明与使用片段**（原文）：

DefaultFailFast 关闭快速失败模式（默认关闭）。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultFailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].DefaultLogger`

```go
func (b *MultiPoolBuilder[T]) DefaultLogger() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L189) · [行为专题](../pool.md)：DefaultLogger 重置为默认日志实现。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*MultiPoolBuilder[T].DefaultMaxPending`

```go
func (b *MultiPoolBuilder[T]) DefaultMaxPending() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L72) · [行为专题](../pool.md)：DefaultMaxPending 使用默认最大待处理数（0=无限制）。

**源码说明与使用片段**（原文）：

DefaultMaxPending 使用默认最大待处理数（0=无限制）。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxPending()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].DefaultMaxResults`

```go
func (b *MultiPoolBuilder[T]) DefaultMaxResults() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L108) · [行为专题](../pool.md)：DefaultMaxResults 清除构建器覆盖，内部创建池时使用当时的全局默认最大结果数。

**源码说明与使用片段**（原文）：

DefaultMaxResults 清除构建器覆盖，内部创建池时使用当时的全局默认最大结果数。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxResults()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：清除当前字段的构建器覆盖，内部创建池时使用当时的全局默认值；不一定表示不限时、禁用缓冲或无限结果。MultiPool 注入外部池时这些设置不应用。需要明确覆盖时检查对应实例方法的边界。

## `*MultiPoolBuilder[T].DefaultMultiPoolConfig`

```go
func (b *MultiPoolBuilder[T]) DefaultMultiPoolConfig() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L36) · [行为专题](../pool.md)：DefaultMultiPoolConfig 重置为默认配置

**源码说明与使用片段**（原文）：

DefaultMultiPoolConfig 重置为默认配置


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMultiPoolConfig()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只整体重置 cfg：Size=IO()、MaxResults=-1，其余字段为零；不重置 context，也不清除 MultiPool 的外部池引用或分片数。内部创建时仍继承创建当时的全局默认超时、结果上限和环形缓冲。

## `*MultiPoolBuilder[T].DefaultOverflow`

```go
func (b *MultiPoolBuilder[T]) DefaultOverflow() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L78) · [行为专题](../pool.md)：DefaultOverflow 将任务队列溢出策略设为 OverflowDrop。

**源码说明与使用片段**（原文）：

DefaultOverflow 将任务队列溢出策略设为 OverflowDrop。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultOverflow()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].DefaultPool`

```go
func (b *MultiPoolBuilder[T]) DefaultPool() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L219) · [行为专题](../pool.md)：DefaultPool 恢复为内部自动创建协程池（默认行为）。

**源码说明与使用片段**（原文）：

DefaultPool 恢复为内部自动创建协程池（默认行为）。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPool()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：MultiPoolBuilder.Pool 注入的原池作为第 0 片，Run 结束会连同其他片一起关闭；注入时 cfg 中 Worker/Timeout/FailFast 等配置不应用，Shards 仍生效。Pool(nil)/DefaultPool 清除注入，恢复内部创建；外部池不要再由其他使用者并发执行生命周期操作。

## `*MultiPoolBuilder[T].DefaultRingBuf`

```go
func (b *MultiPoolBuilder[T]) DefaultRingBuf() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L102) · [行为专题](../pool.md)：DefaultRingBuf 清除构建器覆盖，使用创建池时的全局默认环形缓冲配置。

**源码说明与使用片段**（原文）：

DefaultRingBuf 清除构建器覆盖，使用创建池时的全局默认环形缓冲配置。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultRingBuf()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：清除当前字段的构建器覆盖，内部创建池时使用当时的全局默认值；不一定表示不限时、禁用缓冲或无限结果。MultiPool 注入外部池时这些设置不应用。需要明确覆盖时检查对应实例方法的边界。

## `*MultiPoolBuilder[T].DefaultShards`

```go
func (b *MultiPoolBuilder[T]) DefaultShards() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L42) · [行为专题](../pool.md)：DefaultShards 恢复默认分片数；默认只使用一个池。

**源码说明与使用片段**（原文）：

DefaultShards 恢复默认分片数；默认只使用一个池。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShards()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存 Run 时的水平分片数，非正值及 1 仅包装原池；DefaultShards 保存 0，实际一片。正数多片时 Worker/内存配置按每片计算，总资源随片数增加，分片不会迁移原池已有任务。

## `*MultiPoolBuilder[T].DefaultStreaming`

```go
func (b *MultiPoolBuilder[T]) DefaultStreaming() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L114) · [行为专题](../pool.md)：DefaultStreaming 恢复默认设置（不启用流式结果）。

**源码说明与使用片段**（原文）：

DefaultStreaming 恢复默认设置（不启用流式结果）。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultStreaming()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].DefaultSubmitTimeout`

```go
func (b *MultiPoolBuilder[T]) DefaultSubmitTimeout() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L60) · [行为专题](../pool.md)：DefaultSubmitTimeout 使用默认提交超时（不限时）。

**源码说明与使用片段**（原文）：

DefaultSubmitTimeout 使用默认提交超时（不限时）。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultSubmitTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].DefaultTimeout`

```go
func (b *MultiPoolBuilder[T]) DefaultTimeout() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L54) · [行为专题](../pool.md)：DefaultTimeout 恢复为池创建时的全局默认任务超时。

**源码说明与使用片段**（原文）：

DefaultTimeout 恢复为池创建时的全局默认任务超时。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：清除当前字段的构建器覆盖，内部创建池时使用当时的全局默认值；不一定表示不限时、禁用缓冲或无限结果。MultiPool 注入外部池时这些设置不应用。需要明确覆盖时检查对应实例方法的边界。

## `*MultiPoolBuilder[T].DefaultWorker`

```go
func (b *MultiPoolBuilder[T]) DefaultWorker() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L48) · [行为专题](../pool.md)：DefaultWorker 使用默认 IO 并发度（CPU 核数的两倍）。

**源码说明与使用片段**（原文）：

DefaultWorker 使用默认 IO 并发度（CPU 核数的两倍）。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].FailFast`

```go
func (b *MultiPoolBuilder[T]) FailFast() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L150) · [行为专题](../pool.md)：FailFast 启用快速失败模式

**源码说明与使用片段**（原文）：

FailFast 启用快速失败模式


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].Logger`

```go
func (b *MultiPoolBuilder[T]) Logger(l core.Logger) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L186) · [行为专题](../pool.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*MultiPoolBuilder[T].MaxPending`

```go
func (b *MultiPoolBuilder[T]) MaxPending(n int) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L156) · [行为专题](../pool.md)：MaxPending 设置最大待处理数

**源码说明与使用片段**（原文）：

MaxPending 设置最大待处理数


**参数说明**：

- `n` (`int`)：MaxPending 设置最大待处理数

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxPending(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.MaxPending = n
```

## `*MultiPoolBuilder[T].MaxResults`

```go
func (b *MultiPoolBuilder[T]) MaxResults(n int) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L174) · [行为专题](../pool.md)：MaxResults 设置最大结果数

**源码说明与使用片段**（原文）：

MaxResults 设置最大结果数


**参数说明**：

- `n` (`int`)：内部创建池时每池主结果的保留上限；0 无限制，正数保留前 n 个索引，负数不覆盖创建池时的全局默认值。MultiPool 按每片独立计数；注入外部池时 MultiPoolBuilder 的此配置不应用，使用外部池原有设置。结果溢出策略需另行配置。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxResults(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.MaxResults = n
```

## `*MultiPoolBuilder[T].Overflow`

```go
func (b *MultiPoolBuilder[T]) Overflow(s core.OverflowStrategy) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L162) · [行为专题](../pool.md)：Overflow 设置队列溢出策略

**源码说明与使用片段**（原文）：

Overflow 设置队列溢出策略


**参数说明**：

- `s` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Overflow(s)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Overflow = s
```

## `*MultiPoolBuilder[T].OverflowBlock`

```go
func (b *MultiPoolBuilder[T]) OverflowBlock() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L90) · [行为专题](../pool.md)：OverflowBlock 队列满时阻塞等待空位。

**源码说明与使用片段**（原文）：

OverflowBlock 队列满时阻塞等待空位。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OverflowBlock()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].OverflowDrop`

```go
func (b *MultiPoolBuilder[T]) OverflowDrop() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L84) · [行为专题](../pool.md)：OverflowDrop 达到 MaxPending 时丢弃新任务。

**源码说明与使用片段**（原文）：

OverflowDrop 达到 MaxPending 时丢弃新任务。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OverflowDrop()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].OverflowError`

```go
func (b *MultiPoolBuilder[T]) OverflowError() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L96) · [行为专题](../pool.md)：OverflowError 队列满时返回错误。

**源码说明与使用片段**（原文）：

OverflowError 队列满时返回错误。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OverflowError()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*MultiPoolBuilder[T].Pool`

```go
func (b *MultiPoolBuilder[T]) Pool(p *Pool[T]) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L213) · [行为专题](../pool.md)：Pool 注入外部协程池，MultiPool 使用该池进行分片。

**源码说明与使用片段**（原文）：

Pool 注入外部协程池，MultiPool 使用该池进行分片。 注入后 Worker/Timeout 等保存到 cfg 的配置不再应用；Shards 仍决定分片数。 外部池生命周期由 Run 接管：外部池成为 shard\[0]，defer mp.Close() 会同时关闭它。 调用方不得在 Run 之后再使用该池。

示例：

	myPool := pool.NewPool[string](8).WithTimeout(5 * time.Second)
	err := async.PoolMulti[string]().Context(ctx).Pool(myPool).Shards(4).Run(func(ctx context.Context, mp *async.MultiPool[string]) error {
	    if err := mp.Submit(ctx, fn); err != nil {
	        return err
	    }
	    for _, result := range mp.Wait() {
	        if result.Err != nil {
	            return result.Err
	        }
	    }
	    return nil
	})
	if err != nil {
	    log.Printf("多池失败: %v", err)
	}


**参数说明**：

- `p` (`*Pool[T]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool(p)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：MultiPoolBuilder.Pool 注入的原池作为第 0 片，Run 结束会连同其他片一起关闭；注入时 cfg 中 Worker/Timeout/FailFast 等配置不应用，Shards 仍生效。Pool(nil)/DefaultPool 清除注入，恢复内部创建；外部池不要再由其他使用者并发执行生命周期操作。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.extP = p
```

## `*MultiPoolBuilder[T].RingBuf`

```go
func (b *MultiPoolBuilder[T]) RingBuf(cap int) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L168) · [行为专题](../pool.md)：RingBuf 设置环形缓冲容量

**源码说明与使用片段**（原文）：

RingBuf 设置环形缓冲容量


**参数说明**：

- `cap` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RingBuf(cap)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.RingBufCap = cap
```

## `*MultiPoolBuilder[T].Run`

```go
func (b *MultiPoolBuilder[T]) Run(fn func(ctx context.Context, mp *MultiPool[T]) error) error
```

[实现](../../internal/pool/multi_pool_builder.go#L225) · [行为专题](../pool.md)：Run 终端方法：创建 Pool（或使用外部注入的 Pool）→ 应用配置 → 分片 → 执行 fn → Close 所有分片。

**源码说明与使用片段**（原文）：

Run 终端方法：创建 Pool（或使用外部注入的 Pool）→ 应用配置 → 分片 → 执行 fn → Close 所有分片。


**参数说明**：

- `fn` (`func(ctx context.Context, mp *MultiPool[T]) error`)：同步接收本次执行器和上下文（以签名为准）的作用域函数；其返回后触发关闭，返回的 error 向外传播。需处理全部任务时在函数内等待并检查结果，避免把执行器带出作用域继续使用。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：作用域回调返回后自动关闭，但 Run 的 error 仅来自该回调，不自动汇总任务失败。须在回调内检查提交错误并 Wait 后检查结果或 FirstError；回调 panic 直接传播，defer 仍执行关闭。不要在 Run 返回后继续使用实例提交任务。

## `*MultiPoolBuilder[T].Shards`

```go
func (b *MultiPoolBuilder[T]) Shards(n int) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L126) · [行为专题](../pool.md)：Shards 设置分片数

**源码说明与使用片段**（原文）：

Shards 设置分片数


**参数说明**：

- `n` (`int`)：Shards 设置分片数

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存 Run 时的水平分片数，非正值及 1 仅包装原池；DefaultShards 保存 0，实际一片。正数多片时 Worker/内存配置按每片计算，总资源随片数增加，分片不会迁移原池已有任务。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.shards = n
```

## `*MultiPoolBuilder[T].Streaming`

```go
func (b *MultiPoolBuilder[T]) Streaming(buf int) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L180) · [行为专题](../pool.md)：Streaming 设置流式 buf 大小

**源码说明与使用片段**（原文）：

Streaming 设置流式 buf 大小


**参数说明**：

- `buf` (`int`)：Streaming 设置流式 buf 大小

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Streaming(buf)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Streaming = buf
```

## `*MultiPoolBuilder[T].SubmitTimeout`

```go
func (b *MultiPoolBuilder[T]) SubmitTimeout(d time.Duration) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L144) · [行为专题](../pool.md)：SubmitTimeout 设置提交超时

**源码说明与使用片段**（原文）：

SubmitTimeout 设置提交超时


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitTimeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.SubmitTimeout = d
```

## `*MultiPoolBuilder[T].Timeout`

```go
func (b *MultiPoolBuilder[T]) Timeout(d time.Duration) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L138) · [行为专题](../pool.md)：Timeout 设置任务超时

**源码说明与使用片段**（原文）：

Timeout 设置任务超时


**参数说明**：

- `d` (`time.Duration`)：创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Timeout = d
```

## `*MultiPoolBuilder[T].Worker`

```go
func (b *MultiPoolBuilder[T]) Worker(n int) *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L132) · [行为专题](../pool.md)：Worker 设置 worker 数量

**源码说明与使用片段**（原文）：

Worker 设置 worker 数量


**参数说明**：

- `n` (`int`)：Worker 设置 worker 数量

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Size = n
```

## `*MultiPool[T].Close`

```go
func (mp *MultiPool[T]) Close()
```

[实现](../../internal/pool/multpool.go#L143) · [行为专题](../pool.md)：依次关闭各片并取消共享上下文；回调须配合退出。

**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：按片号同步关闭，全部返回后取消共享 context；不先取消全部业务，忽略取消且不返回的回调可阻塞。片内 Close 可重复等待同一次清理，但不能在片内任务/结果回调中关闭所属多池；不要与单片 Reset 或配置竞争。

## `*MultiPool[T].Flush`

```go
func (mp *MultiPool[T]) Flush(maxPerShard int) []core.Result[T]
```

[实现](../../internal/pool/multpool.go#L288) · [行为专题](../pool.md)：按分片顺序取出环形缓冲结果，最多取 maxPerShard 个/片；主结果存储不在本方法中读取。

**参数说明**：

- `maxPerShard` (`int`)：本次从每片环形结果缓冲取出的最大项数；非正值取出每片全部现有结果，不读取主结果存储。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Flush(maxPerShard)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：各片独立 Flush(maxPerShard)，按片号拼接，不保持跨片完成顺序；正数总返回可达片数×上限，非正取每片全部。消费溢出缓冲会影响随后 Wait 的附加结果，镜像模式则可能与 Wait 重复；不等待在途业务，仍需统一 Wait/Close。

## `*MultiPool[T].GetShard`

```go
func (mp *MultiPool[T]) GetShard(idx int) *Pool[T]
```

[实现](../../internal/pool/multpool.go#L24) · [行为专题](../pool.md)：返回零起始索引对应的分片；越界返回 nil。

**参数说明**：

- `idx` (`int`)：目标分片的零起始索引；负值或不小于分片数时返回 nil。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShard(idx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：ShardCount 只读当前片数，GetShard 越界返回 nil，有效时返回原池指针，不复制；直接修改、等待、复位或关闭会影响多池。统一管理所有片的生命周期，不把单片索引当全局任务序号，配置避免与提交竞争。

## `*MultiPool[T].ShardCount`

```go
func (mp *MultiPool[T]) ShardCount() int
```

[实现](../../internal/pool/multpool.go#L20) · [行为专题](../pool.md)：返回分片数量。

**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：ShardCount 只读当前片数，GetShard 越界返回 nil，有效时返回原池指针，不复制；直接修改、等待、复位或关闭会影响多池。统一管理所有片的生命周期，不把单片索引当全局任务序号，配置避免与提交竞争。

## `*MultiPool[T].StreamResults`

```go
func (mp *MultiPool[T]) StreamResults() <-chan core.Result[T]
```

[实现](../../internal/pool/multpool.go#L182) · [行为专题](../pool.md)：并发合并各片已启用的结果流；片内未启用的流被跳过，全部转发结束后关闭合并通道。持续消费以免合并 goroutine 阻塞，各片流仍可能丢弃满缓冲结果。

**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StreamResults()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：每次调用创建合并通道及转发 goroutine，重复调用会竞争分片流。持续消费至关闭，否则转发可能阻塞；没有独立的合并取消入口。原分片流仍可能因缓冲满而丢弃，普通预检查拒绝也不进入流；要核对全部提交使用逐项 completion。

## `*MultiPool[T].Submit`

```go
func (mp *MultiPool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/pool/multpool.go#L31) · [行为专题](../pool.md)：按 round-robin 选择一片，委托该片的同名提交方法；检查提交错误。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Submit(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。普通提交按独立轮询计数选片，Keyed 按 uint64 key 对片数取模且不推进轮询计数；同 key 只保证同片，不保证顺序执行。Try 只尝试选中的片，不搜索其他空闲片；批量继续提交全部元素并逐项报告错误。必须从 Shard/Builder 创建，零值多池没有片，提交会 panic。

## `*MultiPool[T].SubmitBatch`

```go
func (mp *MultiPool[T]) SubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) []SubmitResult
```

[实现](../../internal/pool/multpool.go#L51) · [行为专题](../pool.md)：依输入顺序轮转分片提交，返回每项提交状态；返回值不是任务执行结果。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`[]SubmitResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitBatch(ctx, items, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。普通提交按独立轮询计数选片，Keyed 按 uint64 key 对片数取模且不推进轮询计数；同 key 只保证同片，不保证顺序执行。Try 只尝试选中的片，不搜索其他空闲片；批量继续提交全部元素并逐项报告错误。必须从 Shard/Builder 创建，零值多池没有片，提交会 panic。

## `*MultiPool[T].SubmitKeyed`

```go
func (mp *MultiPool[T]) SubmitKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/pool/multpool.go#L41) · [行为专题](../pool.md)：按 key 对分片数取模选片；同一个 key 路由到同一片，但不保证串行执行。

**参数说明**：

- `key` (`uint64`)：键或路由标识；类型及匹配规则由接收者决定。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitKeyed(key, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。普通提交按独立轮询计数选片，Keyed 按 uint64 key 对片数取模且不推进轮询计数；同 key 只保证同片，不保证顺序执行。Try 只尝试选中的片，不搜索其他空闲片；批量继续提交全部元素并逐项报告错误。必须从 Shard/Builder 创建，零值多池没有片，提交会 panic。

## `*MultiPool[T].TotalActive`

```go
func (mp *MultiPool[T]) TotalActive() int
```

[实现](../../internal/pool/multpool.go#L232) · [行为专题](../pool.md)：依次读取各片对应的 Active 指标并求和；这是跨片非原子快照，WorkerCount 使用各片 Size。

**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalActive()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次读取各片指标后求和，非跨片原子快照；TotalWorkerCount 使用配置 Size，并非仍存活 worker 数。TotalCount/失败统计可含拒绝和取消，不等于执行数量，也不受结果保留上限限制；不能用多个字段差值判断任务清理已完成。

## `*MultiPool[T].TotalBusy`

```go
func (mp *MultiPool[T]) TotalBusy() int
```

[实现](../../internal/pool/multpool.go#L240) · [行为专题](../pool.md)：依次读取各片对应的 Busy 指标并求和；这是跨片非原子快照，WorkerCount 使用各片 Size。

**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalBusy()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次读取各片指标后求和，非跨片原子快照；TotalWorkerCount 使用配置 Size，并非仍存活 worker 数。TotalCount/失败统计可含拒绝和取消，不等于执行数量，也不受结果保留上限限制；不能用多个字段差值判断任务清理已完成。

## `*MultiPool[T].TotalCount`

```go
func (mp *MultiPool[T]) TotalCount() int64
```

[实现](../../internal/pool/multpool.go#L280) · [行为专题](../pool.md)：依次读取各片对应的 Count 指标并求和；这是跨片非原子快照，WorkerCount 使用各片 Size。

**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次读取各片指标后求和，非跨片原子快照；TotalWorkerCount 使用配置 Size，并非仍存活 worker 数。TotalCount/失败统计可含拒绝和取消，不等于执行数量，也不受结果保留上限限制；不能用多个字段差值判断任务清理已完成。

## `*MultiPool[T].TotalFailCount`

```go
func (mp *MultiPool[T]) TotalFailCount() int64
```

[实现](../../internal/pool/multpool.go#L264) · [行为专题](../pool.md)：依次读取各片对应的 FailCount 指标并求和；这是跨片非原子快照，WorkerCount 使用各片 Size。

**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalFailCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次读取各片指标后求和，非跨片原子快照；TotalWorkerCount 使用配置 Size，并非仍存活 worker 数。TotalCount/失败统计可含拒绝和取消，不等于执行数量，也不受结果保留上限限制；不能用多个字段差值判断任务清理已完成。

## `*MultiPool[T].TotalOkCount`

```go
func (mp *MultiPool[T]) TotalOkCount() int64
```

[实现](../../internal/pool/multpool.go#L272) · [行为专题](../pool.md)：依次读取各片对应的 OkCount 指标并求和；这是跨片非原子快照，WorkerCount 使用各片 Size。

**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalOkCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次读取各片指标后求和，非跨片原子快照；TotalWorkerCount 使用配置 Size，并非仍存活 worker 数。TotalCount/失败统计可含拒绝和取消，不等于执行数量，也不受结果保留上限限制；不能用多个字段差值判断任务清理已完成。

## `*MultiPool[T].TotalPending`

```go
func (mp *MultiPool[T]) TotalPending() int
```

[实现](../../internal/pool/multpool.go#L248) · [行为专题](../pool.md)：依次读取各片对应的 Pending 指标并求和；这是跨片非原子快照，WorkerCount 使用各片 Size。

**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalPending()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次读取各片指标后求和，非跨片原子快照；TotalWorkerCount 使用配置 Size，并非仍存活 worker 数。TotalCount/失败统计可含拒绝和取消，不等于执行数量，也不受结果保留上限限制；不能用多个字段差值判断任务清理已完成。

## `*MultiPool[T].TotalWorkerCount`

```go
func (mp *MultiPool[T]) TotalWorkerCount() int
```

[实现](../../internal/pool/multpool.go#L256) · [行为专题](../pool.md)：依次读取各片对应的 WorkerCount 指标并求和；这是跨片非原子快照，WorkerCount 使用各片 Size。

**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalWorkerCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次读取各片指标后求和，非跨片原子快照；TotalWorkerCount 使用配置 Size，并非仍存活 worker 数。TotalCount/失败统计可含拒绝和取消，不等于执行数量，也不受结果保留上限限制；不能用多个字段差值判断任务清理已完成。

## `*MultiPool[T].TrySubmit`

```go
func (mp *MultiPool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/pool/multpool.go#L36) · [行为专题](../pool.md)：按 round-robin 选择一片，委托该片的同名提交方法；检查提交错误。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TrySubmit(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。普通提交按独立轮询计数选片，Keyed 按 uint64 key 对片数取模且不推进轮询计数；同 key 只保证同片，不保证顺序执行。Try 只尝试选中的片，不搜索其他空闲片；批量继续提交全部元素并逐项报告错误。必须从 Shard/Builder 创建，零值多池没有片，提交会 panic。

## `*MultiPool[T].TrySubmitKeyed`

```go
func (mp *MultiPool[T]) TrySubmitKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/pool/multpool.go#L46) · [行为专题](../pool.md)：按 key 对分片数取模选片；同一个 key 路由到同一片，但不保证串行执行。

**参数说明**：

- `key` (`uint64`)：键或路由标识；类型及匹配规则由接收者决定。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TrySubmitKeyed(key, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。普通提交按独立轮询计数选片，Keyed 按 uint64 key 对片数取模且不推进轮询计数；同 key 只保证同片，不保证顺序执行。Try 只尝试选中的片，不搜索其他空闲片；批量继续提交全部元素并逐项报告错误。必须从 Shard/Builder 创建，零值多池没有片，提交会 panic。

## `*MultiPool[T].Wait`

```go
func (mp *MultiPool[T]) Wait() []core.Result[T]
```

[实现](../../internal/pool/multpool.go#L63) · [行为专题](../pool.md)：等待各片并按分片序号合并结果；并发重复等待立即返回 nil。

**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：按分片序号拼接结果，不保持跨片全局提交顺序；每片等待仍是一次性入口。并发多池等待立即返回空/false，该被拒绝调用不取消共享链；取得等待权的 Wait/WaitContext/WaitTimeout 返回前取消 sharedCancel，即使本次 bool 为 false。正常单片等待只取消该片子 context，整体收尾才取消共享层；超时请求任务退出，不强制停止业务。复用需逐片复位并重建适当 context，优先为新批次新建多池；WaitAndClose 随后关闭所有片，其他等待方法不自动关闭 worker。

## `*MultiPool[T].WaitAndClose`

```go
func (mp *MultiPool[T]) WaitAndClose() []core.Result[T]
```

[实现](../../internal/pool/multpool.go#L82) · [行为专题](../pool.md)：等待并合并分片结果后关闭各片。

**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitAndClose()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：按分片序号拼接结果，不保持跨片全局提交顺序；每片等待仍是一次性入口。并发多池等待立即返回空/false，该被拒绝调用不取消共享链；取得等待权的 Wait/WaitContext/WaitTimeout 返回前取消 sharedCancel，即使本次 bool 为 false。正常单片等待只取消该片子 context，整体收尾才取消共享层；超时请求任务退出，不强制停止业务。复用需逐片复位并重建适当 context，优先为新批次新建多池；WaitAndClose 随后关闭所有片，其他等待方法不自动关闭 worker。

## `*MultiPool[T].WaitContext`

```go
func (mp *MultiPool[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool)
```

[实现](../../internal/pool/multpool.go#L97) · [行为专题](../pool.md)：各片并行等待同一个截止时间，按分片序号合并快照；false 表示未全部完成或有并发等待。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitContext(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：按分片序号拼接结果，不保持跨片全局提交顺序；每片等待仍是一次性入口。并发多池等待立即返回空/false，该被拒绝调用不取消共享链；取得等待权的 Wait/WaitContext/WaitTimeout 返回前取消 sharedCancel，即使本次 bool 为 false。正常单片等待只取消该片子 context，整体收尾才取消共享层；超时请求任务退出，不强制停止业务。复用需逐片复位并重建适当 context，优先为新批次新建多池；WaitAndClose 随后关闭所有片，其他等待方法不自动关闭 worker。

## `*MultiPool[T].WaitTimeout`

```go
func (mp *MultiPool[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool)
```

[实现](../../internal/pool/multpool.go#L88) · [行为专题](../pool.md)：各片并行等待同一个截止时间，按分片序号合并快照；false 表示未全部完成或有并发等待。

**参数说明**：

- `d` (`time.Duration`)：本次等待的最长时长；非正值使等待 context 立即过期，完成与超时同时就绪时不保证 bool 的选择。超时返回部分结果并通知取消，不能强制停止任务；分片共享同一截止时间，复用须遵守该实例的等待/复位约定。

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitTimeout(d)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：按分片序号拼接结果，不保持跨片全局提交顺序；每片等待仍是一次性入口。并发多池等待立即返回空/false，该被拒绝调用不取消共享链；取得等待权的 Wait/WaitContext/WaitTimeout 返回前取消 sharedCancel，即使本次 bool 为 false。正常单片等待只取消该片子 context，整体收尾才取消共享层；超时请求任务退出，不强制停止业务。复用需逐片复位并重建适当 context，优先为新批次新建多池；WaitAndClose 随后关闭所有片，其他等待方法不自动关闭 worker。

## `*MultiPool[T].WithMaxPending`

```go
func (mp *MultiPool[T]) WithMaxPending(n int) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L211) · [行为专题](../pool.md)：把正数 n 设置为每片最大等待任务数，非正值不更新已有配置；这不是多池总量限制。

**参数说明**：

- `n` (`int`)：把正数 n 设置为每片最大等待任务数，非正值不更新已有配置；这不是多池总量限制。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithMaxPending(n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次对每片应用，非跨片原子配置，容量/上限按每片独立计算。只限制等待任务，不含正在执行业务的 worker；MaxPending 仅正数更新，非正不清除现有上限，也不改变底层 channel 容量。Drop 达限丢弃且提交可返回 nil，Error 返回 ErrQueueOverflow，Block 等空位并响应 context/提交期限/关闭。此策略不代替结果环形缓冲策略；配置在提交前完成。

## `*MultiPool[T].WithMaxResults`

```go
func (mp *MultiPool[T]) WithMaxResults(n int) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L225) · [行为专题](../pool.md)：把非负 n 设置为每片主结果保留上限，0 为不限制，负值不更新；溢出结果依配置通过流、回调或环形缓冲消费。

**参数说明**：

- `n` (`int`)：每池主结果的保留上限；0 无限制，正数按提交索引限制，负数忽略并保留原配置。应在提交前设置，超出部分通过已配置的环形缓冲、流或回调处理，否则可能丢失。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithMaxResults(n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次对每片应用，非跨片原子配置，容量/上限按每片独立计算。非负更新主结果保留上限，0 无限制，负值忽略；正数按提交索引保留，非按完成先后。更改不会回收已分配结果槽位，提交期间改动可能使不同任务采用不同上限快照，须提前配置。超限结果需单独用 completion/流/环形缓冲处理；错误计数不受保留上限约束。

## `*MultiPool[T].WithOverflow`

```go
func (mp *MultiPool[T]) WithOverflow(strategy core.OverflowStrategy) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L218) · [行为专题](../pool.md)：为每片设置等待队列溢出策略；与每片 WithMaxPending 配合使用。

**参数说明**：

- `strategy` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithOverflow(strategy)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次对每片应用，非跨片原子配置，容量/上限按每片独立计算。只限制等待任务，不含正在执行业务的 worker；MaxPending 仅正数更新，非正不清除现有上限，也不改变底层 channel 容量。Drop 达限丢弃且提交可返回 nil，Error 返回 ErrQueueOverflow，Block 等空位并响应 context/提交期限/关闭。此策略不代替结果环形缓冲策略；配置在提交前完成。

## `*MultiPool[T].WithResultCallback`

```go
func (mp *MultiPool[T]) WithResultCallback(fn func(core.Result[T])) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L175) · [行为专题](../pool.md)：WithResultCallback 为所有分片设置同步结果回调。回调可能并发执行，

**源码说明与使用片段**（原文）：

WithResultCallback 为所有分片设置同步结果回调。回调可能并发执行， 不能同步等待或关闭所属多池及其分片，否则可能死锁。


**参数说明**：

- `fn` (`func(core.Result[T])`)：接收已进入队列登记路径的任务结果；nil 清除回调。同步且可能并发，panic 被记录；普通 Submit/TrySubmit 的预检查拒绝或背压直接拒绝不通知此回调。需要每次提交的完整终态时使用 SubmitWithCompletion，并检查提交错误；不得在通知中同步 Wait/Close 本池。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithResultCallback(fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次对全部片保存同一同步回调，nil 清除，不是跨片原子配置。回调可跨片并发且 panic 被记录；普通预检查/直接背压拒绝不保证通知，不能统计全部提交尝试。不在回调中 Wait/Close 多池或其分片，配置须在提交前完成。

## `*MultiPool[T].WithRingBuffer`

```go
func (mp *MultiPool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L204) · [行为专题](../pool.md)：为每片创建 capacity 容量的环形缓冲并设置 overflow 策略；不会单独限制主结果存储，总容量按片数累加。

**参数说明**：

- `capacity` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。
- `overflow` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithRingBuffer(capacity, overflow)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：主结果仍独立保留。MaxResults>0 时只写上限外结果，Wait 追加未取出的溢出项；MaxResults==0 时镜像全部结果，Flush 与 Wait 可能重复取得同一任务，Wait 不排空镜像缓冲。满时 Drop 覆盖最旧项，Block/Error 拒绝新写入而不阻塞。

## `*MultiPool[T].WithStreaming`

```go
func (mp *MultiPool[T]) WithStreaming(bufSize int) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L166) · [行为专题](../pool.md)：为每片创建 bufSize 容量的结果通道，非正值使用该片 worker 数的两倍；在提交前配置，再通过 StreamResults 合并消费。

**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStreaming(bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：依次对每片应用，非跨片原子配置，容量/上限按每片独立计算。WithStreaming 立即替换通道，非正容量按 Size×2；只在提交前配置一次，旧引用不自动更新/关闭。StreamResults 未启用返回 nil、启用返回同一通道，多消费者竞争；满时直接丢结果并增 StreamDropped，Wait/关闭收尾关通道。普通预检查拒绝不进入流，不能把流当全部尝试审计，最终结果保留仍独立。

## `*MultiPool[T].WithSubmitTimeout`

```go
func (mp *MultiPool[T]) WithSubmitTimeout(d time.Duration) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L159) · [行为专题](../pool.md)：为每片设置提交等待超时 d；检查提交错误，提交前完成配置。

**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithSubmitTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次对每片应用，非跨片原子配置，容量/上限按每片独立计算。原地保存时长，非正任务超时清除池执行期限，非正提交超时取消显式入队计时器；父 context 仍有效。任务期限从 worker 开始执行计时，不含排队；提交期限包含 MaxPending 等空位及 channel 等待。回调须响应取消，配置不得与提交竞争。

## `*MultiPool[T].WithTimeout`

```go
func (mp *MultiPool[T]) WithTimeout(d time.Duration) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L152) · [行为专题](../pool.md)：为每片设置单任务超时 d；超时依赖回调响应 context，提交前完成配置。

**参数说明**：

- `d` (`time.Duration`)：每个任务开始执行后的 context 超时；正数生效，非正值清除本实例配置的执行期限，不包含等待入队时间。分片方法对每片应用，回调必须响应 context；在提交前配置。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次对每片应用，非跨片原子配置，容量/上限按每片独立计算。原地保存时长，非正任务超时清除池执行期限，非正提交超时取消显式入队计时器；父 context 仍有效。任务期限从 worker 开始执行计时，不含排队；提交期限包含 MaxPending 等空位及 channel 等待。回调须响应取消，配置不得与提交竞争。

## `*PoolBuilder[T].Config`

```go
func (b *PoolBuilder[T]) Config(fn func(Config) Config) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L109) · [行为专题](../pool.md)：Config 函数式配置，允许通过闭包修改 Config

**源码说明与使用片段**（原文）：

Config 函数式配置，允许通过闭包修改 Config


**参数说明**：

- `fn` (`func(Config) Config`)：接收当前配置副本并返回需要应用的配置；必须返回修改后的副本，调用会同步执行。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Config(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：同步以当前 Config 值调用 fn 并用返回值整体替换，fn 须非 nil，panic 直接传播；闭包返回零值 Config 会把 MaxResults 改为 0（无限保留）。MultiPool 注入外部池时 cfg 不应用；配置须在 Run 前完成。

## `*PoolBuilder[T].Context`

```go
func (b *PoolBuilder[T]) Context(ctx context.Context) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L25) · [行为专题](../pool.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存非 nil context 并确保 trace_id，供后续 Run 的作用域使用；PoolBuilder 只在 FailFast 时将其绑定到池内部 context，提交仍须传递作用域 context。MultiPool 注入外部池时不应用 cfg/FailFast，外部池原设置继续生效；修改不影响已运行实例，配置不得与 Run 并发。

## `*PoolBuilder[T].DefaultFailFast`

```go
func (b *PoolBuilder[T]) DefaultFailFast() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L55) · [行为专题](../pool.md)：DefaultFailFast 关闭快速失败模式（默认关闭）。

**源码说明与使用片段**（原文）：

DefaultFailFast 关闭快速失败模式（默认关闭）。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultFailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].DefaultLogger`

```go
func (b *PoolBuilder[T]) DefaultLogger() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L172) · [行为专题](../pool.md)：DefaultLogger 重置为默认日志实现。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*PoolBuilder[T].DefaultMaxPending`

```go
func (b *PoolBuilder[T]) DefaultMaxPending() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L61) · [行为专题](../pool.md)：DefaultMaxPending 使用默认最大待处理数（0=无限制）。

**源码说明与使用片段**（原文）：

DefaultMaxPending 使用默认最大待处理数（0=无限制）。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxPending()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].DefaultMaxResults`

```go
func (b *PoolBuilder[T]) DefaultMaxResults() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L97) · [行为专题](../pool.md)：DefaultMaxResults 清除构建器覆盖，使用创建池时的全局默认最大结果数。

**源码说明与使用片段**（原文）：

DefaultMaxResults 清除构建器覆盖，使用创建池时的全局默认最大结果数。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxResults()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：清除当前字段的构建器覆盖，内部创建池时使用当时的全局默认值；不一定表示不限时、禁用缓冲或无限结果。MultiPool 注入外部池时这些设置不应用。需要明确覆盖时检查对应实例方法的边界。

## `*PoolBuilder[T].DefaultOverflow`

```go
func (b *PoolBuilder[T]) DefaultOverflow() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L67) · [行为专题](../pool.md)：DefaultOverflow 将任务队列溢出策略设为 OverflowDrop。

**源码说明与使用片段**（原文）：

DefaultOverflow 将任务队列溢出策略设为 OverflowDrop。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultOverflow()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].DefaultPoolConfig`

```go
func (b *PoolBuilder[T]) DefaultPoolConfig() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L31) · [行为专题](../pool.md)：DefaultPoolConfig 重置为默认配置

**源码说明与使用片段**（原文）：

DefaultPoolConfig 重置为默认配置


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPoolConfig()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只整体重置 cfg：Size=IO()、MaxResults=-1，其余字段为零；不重置 context，也不清除 MultiPool 的外部池引用或分片数。内部创建时仍继承创建当时的全局默认超时、结果上限和环形缓冲。

## `*PoolBuilder[T].DefaultRingBuf`

```go
func (b *PoolBuilder[T]) DefaultRingBuf() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L91) · [行为专题](../pool.md)：DefaultRingBuf 清除构建器覆盖，使用创建池时的全局默认环形缓冲配置。

**源码说明与使用片段**（原文）：

DefaultRingBuf 清除构建器覆盖，使用创建池时的全局默认环形缓冲配置。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultRingBuf()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：清除当前字段的构建器覆盖，内部创建池时使用当时的全局默认值；不一定表示不限时、禁用缓冲或无限结果。MultiPool 注入外部池时这些设置不应用。需要明确覆盖时检查对应实例方法的边界。

## `*PoolBuilder[T].DefaultStreaming`

```go
func (b *PoolBuilder[T]) DefaultStreaming() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L103) · [行为专题](../pool.md)：DefaultStreaming 恢复默认设置（不启用流式结果）。

**源码说明与使用片段**（原文）：

DefaultStreaming 恢复默认设置（不启用流式结果）。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultStreaming()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].DefaultSubmitTimeout`

```go
func (b *PoolBuilder[T]) DefaultSubmitTimeout() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L49) · [行为专题](../pool.md)：DefaultSubmitTimeout 使用默认提交超时（不限时）。

**源码说明与使用片段**（原文）：

DefaultSubmitTimeout 使用默认提交超时（不限时）。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultSubmitTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].DefaultTimeout`

```go
func (b *PoolBuilder[T]) DefaultTimeout() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L43) · [行为专题](../pool.md)：DefaultTimeout 恢复为池创建时的全局默认任务超时。

**源码说明与使用片段**（原文）：

DefaultTimeout 恢复为池创建时的全局默认任务超时。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：清除当前字段的构建器覆盖，内部创建池时使用当时的全局默认值；不一定表示不限时、禁用缓冲或无限结果。MultiPool 注入外部池时这些设置不应用。需要明确覆盖时检查对应实例方法的边界。

## `*PoolBuilder[T].DefaultWorker`

```go
func (b *PoolBuilder[T]) DefaultWorker() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L37) · [行为专题](../pool.md)：DefaultWorker 使用默认 IO 并发度（CPU 核数的两倍）。

**源码说明与使用片段**（原文）：

DefaultWorker 使用默认 IO 并发度（CPU 核数的两倍）。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].FailFast`

```go
func (b *PoolBuilder[T]) FailFast() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L133) · [行为专题](../pool.md)：FailFast 启用快速失败模式

**源码说明与使用片段**（原文）：

FailFast 启用快速失败模式


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].Logger`

```go
func (b *PoolBuilder[T]) Logger(l core.Logger) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L169) · [行为专题](../pool.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*PoolBuilder[T].MaxPending`

```go
func (b *PoolBuilder[T]) MaxPending(n int) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L139) · [行为专题](../pool.md)：MaxPending 设置最大待处理数

**源码说明与使用片段**（原文）：

MaxPending 设置最大待处理数


**参数说明**：

- `n` (`int`)：MaxPending 设置最大待处理数

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxPending(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.MaxPending = n
```

## `*PoolBuilder[T].MaxResults`

```go
func (b *PoolBuilder[T]) MaxResults(n int) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L157) · [行为专题](../pool.md)：MaxResults 设置最大结果数

**源码说明与使用片段**（原文）：

MaxResults 设置最大结果数


**参数说明**：

- `n` (`int`)：内部创建池时每池主结果的保留上限；0 无限制，正数保留前 n 个索引，负数不覆盖创建池时的全局默认值。MultiPool 按每片独立计数；注入外部池时 MultiPoolBuilder 的此配置不应用，使用外部池原有设置。结果溢出策略需另行配置。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxResults(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.MaxResults = n
```

## `*PoolBuilder[T].Overflow`

```go
func (b *PoolBuilder[T]) Overflow(s core.OverflowStrategy) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L145) · [行为专题](../pool.md)：Overflow 设置队列溢出策略

**源码说明与使用片段**（原文）：

Overflow 设置队列溢出策略


**参数说明**：

- `s` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Overflow(s)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Overflow = s
```

## `*PoolBuilder[T].OverflowBlock`

```go
func (b *PoolBuilder[T]) OverflowBlock() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L79) · [行为专题](../pool.md)：OverflowBlock 队列满时阻塞等待空位。

**源码说明与使用片段**（原文）：

OverflowBlock 队列满时阻塞等待空位。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OverflowBlock()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].OverflowDrop`

```go
func (b *PoolBuilder[T]) OverflowDrop() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L73) · [行为专题](../pool.md)：OverflowDrop 达到 MaxPending 时丢弃新任务。

**源码说明与使用片段**（原文）：

OverflowDrop 达到 MaxPending 时丢弃新任务。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OverflowDrop()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].OverflowError`

```go
func (b *PoolBuilder[T]) OverflowError() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L85) · [行为专题](../pool.md)：OverflowError 队列满时返回错误。

**源码说明与使用片段**（原文）：

OverflowError 队列满时返回错误。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OverflowError()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

## `*PoolBuilder[T].RingBuf`

```go
func (b *PoolBuilder[T]) RingBuf(cap int) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L151) · [行为专题](../pool.md)：RingBuf 设置环形缓冲容量

**源码说明与使用片段**（原文）：

RingBuf 设置环形缓冲容量


**参数说明**：

- `cap` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RingBuf(cap)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.RingBufCap = cap
```

## `*PoolBuilder[T].Run`

```go
func (b *PoolBuilder[T]) Run(fn func(ctx context.Context, p *Pool[T]) error) error
```

[实现](../../internal/pool/pool_builder.go#L175) · [行为专题](../pool.md)：Run 终端方法：创建协程池，执行 fn，fn 返回后自动 Close。

**源码说明与使用片段**（原文）：

Run 终端方法：创建协程池，执行 fn，fn 返回后自动 Close。


**参数说明**：

- `fn` (`func(ctx context.Context, p *Pool[T]) error`)：同步接收本次执行器和上下文（以签名为准）的作用域函数；其返回后触发关闭，返回的 error 向外传播。需处理全部任务时在函数内等待并检查结果，避免把执行器带出作用域继续使用。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：作用域回调返回后自动关闭，但 Run 的 error 仅来自该回调，不自动汇总任务失败。须在回调内检查提交错误并 Wait 后检查结果或 FirstError；回调 panic 直接传播，defer 仍执行关闭。不要在 Run 返回后继续使用实例提交任务。

## `*PoolBuilder[T].Streaming`

```go
func (b *PoolBuilder[T]) Streaming(buf int) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L163) · [行为专题](../pool.md)：Streaming 设置流式 buf 大小

**源码说明与使用片段**（原文）：

Streaming 设置流式 buf 大小


**参数说明**：

- `buf` (`int`)：Streaming 设置流式 buf 大小

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Streaming(buf)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Streaming = buf
```

## `*PoolBuilder[T].SubmitTimeout`

```go
func (b *PoolBuilder[T]) SubmitTimeout(d time.Duration) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L127) · [行为专题](../pool.md)：SubmitTimeout 设置提交超时

**源码说明与使用片段**（原文）：

SubmitTimeout 设置提交超时


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitTimeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.SubmitTimeout = d
```

## `*PoolBuilder[T].Timeout`

```go
func (b *PoolBuilder[T]) Timeout(d time.Duration) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L121) · [行为专题](../pool.md)：Timeout 设置任务超时

**源码说明与使用片段**（原文）：

Timeout 设置任务超时


**参数说明**：

- `d` (`time.Duration`)：创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Timeout = d
```

## `*PoolBuilder[T].Worker`

```go
func (b *PoolBuilder[T]) Worker(n int) *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L115) · [行为专题](../pool.md)：Worker 设置 worker 数量

**源码说明与使用片段**（原文）：

Worker 设置 worker 数量


**参数说明**：

- `n` (`int`)：Worker 设置 worker 数量

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Size = n
```

## `*Pool[T].Active`

```go
func (p *Pool[T]) Active() int
```

[实现](../../internal/pool/pool.go#L2239) · [行为专题](../pool.md)：Active 返回当前活跃（取得任务）的 worker 数量。

**源码说明与使用片段**（原文）：

Active 返回当前活跃（取得任务）的 worker 数量。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Active()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].Busy`

```go
func (p *Pool[T]) Busy() int
```

[实现](../../internal/pool/pool.go#L2244) · [行为专题](../pool.md)：Busy 返回当前正在执行任务的 worker 数量。

**源码说明与使用片段**（原文）：

Busy 返回当前正在执行任务的 worker 数量。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Busy()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].Close`

```go
func (p *Pool[T]) Close()
```

[实现](../../internal/pool/pool.go#L2056) · [行为专题](../pool.md)：Close 关闭协程池，等待所有 worker 退出后返回。

**源码说明与使用片段**（原文）：

Close 关闭协程池，等待所有 worker 退出后返回。 关闭后不能再提交新任务。

示例：

	p.Close()


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：关闭提交入口后等待 worker 及已登记任务，不先取消业务或池 context；排队任务可能执行或记 ErrPoolClosed。完整清理还等待阻塞提交调用者中的同步结果回调/completion，再关闭结果流，通知须及时返回且不得在其中同步关闭本池。首次 CloseAndWaitTimeout 超时通道只表示 worker 退出，通知、Pending 释放和失败计数可能尚未完成；重复关闭的超时通道表示完整清理，true 时返回 nil 通道，限时不含此前锁等待。需要完整收尾再 Close；CloseAndWait 随后 Wait，普通 Close 不取得结果。关闭后先进入 waited 再 Reset 才可重建 worker，ResetWait 不重新打开池。

## `*Pool[T].CloseAndWait`

```go
func (p *Pool[T]) CloseAndWait()
```

[实现](../../internal/pool/pool.go#L2110) · [行为专题](../pool.md)：CloseAndWait 关闭池并等待所有任务完成。

**源码说明与使用片段**（原文）：

CloseAndWait 关闭池并等待所有任务完成。 等价于先调用 Close() 再调用 Wait()。

示例：

	p.CloseAndWait()


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.CloseAndWait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：关闭提交入口后等待 worker 及已登记任务，不先取消业务或池 context；排队任务可能执行或记 ErrPoolClosed。完整清理还等待阻塞提交调用者中的同步结果回调/completion，再关闭结果流，通知须及时返回且不得在其中同步关闭本池。首次 CloseAndWaitTimeout 超时通道只表示 worker 退出，通知、Pending 释放和失败计数可能尚未完成；重复关闭的超时通道表示完整清理，true 时返回 nil 通道，限时不含此前锁等待。需要完整收尾再 Close；CloseAndWait 随后 Wait，普通 Close 不取得结果。关闭后先进入 waited 再 Reset 才可重建 worker，ResetWait 不重新打开池。

## `*Pool[T].CloseAndWaitTimeout`

```go
func (p *Pool[T]) CloseAndWaitTimeout(timeout time.Duration) (ok bool, workerDone <-chan struct{})
```

[实现](../../internal/pool/pool.go#L2129) · [行为专题](../pool.md)：CloseAndWaitTimeout 启动关闭并限时等待；timeout 不覆盖取得 resizeMu 的锁等待。

**源码说明与使用片段**（原文）：

CloseAndWaitTimeout 启动关闭并限时等待；timeout 不覆盖取得 resizeMu 的锁等待。 ok 为 true 表示在超时内正常完成，workerDone channel 可用于异步等待 worker 退出。 首次关闭超时返回的 workerDone 关闭时，队列清理可能仍在进行；调用 Close 可等待完整清理。 若关闭已在进行，等待同一次关闭；超时时返回的通道在关闭清理完成后关闭。

示例：

	ok, workerDone := p.CloseAndWaitTimeout(10 * time.Second)
	if !ok {
	    go func() {
	        <-workerDone
	        log.Println("worker 已全部退出")
	    }()
	}


**参数说明**：

- `timeout` (`time.Duration`)：取得关闭/扩缩容互斥锁并更新关闭状态之后，等待关闭完成的时长；不限制此前锁等待，不是整体调用时限。非正值立即触发计时器，若清理也已完成则 select 可能选中任一路径。超时返回后清理继续，不会强制结束回调；首次返回的 workerDone 只表示 worker 退出，完整清理另用 Close 等待。

**返回值签名**：`ok bool, workerDone <-chan struct{}`。ok=true 表示完整关闭清理已完成，此时 workerDone=nil；ok=false 表示本次等待期限到达，关闭仍继续。首次关闭返回的非 nil 通道只在 worker 退出后关闭；已有关闭流程的重复调用返回完整清理通道。需要完整收尾可再 Close，业务错误另行检查本批结果。

**使用示例**（已初始化参数的调用片段）：

```go
instance.CloseAndWaitTimeout(timeout)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：关闭提交入口后等待 worker 及已登记任务，不先取消业务或池 context；排队任务可能执行或记 ErrPoolClosed。完整清理还等待阻塞提交调用者中的同步结果回调/completion，再关闭结果流，通知须及时返回且不得在其中同步关闭本池。首次 CloseAndWaitTimeout 超时通道只表示 worker 退出，通知、Pending 释放和失败计数可能尚未完成；重复关闭的超时通道表示完整清理，true 时返回 nil 通道，限时不含此前锁等待。需要完整收尾再 Close；CloseAndWait 随后 Wait，普通 Close 不取得结果。关闭后先进入 waited 再 Reset 才可重建 worker，ResetWait 不重新打开池。

## `*Pool[T].CloseByIdle`

```go
func (p *Pool[T]) CloseByIdle(timeout time.Duration)
```

[实现](../../internal/pool/pool.go#L2189) · [行为专题](../pool.md)：CloseByIdle 最多等待 timeout 使池进入空闲状态，然后同步关闭池。

**源码说明与使用片段**（原文）：

CloseByIdle 最多等待 timeout 使池进入空闲状态，然后同步关闭池。 空闲指 Active()==0 且 Busy()==0。关闭会等待 worker 退出， 因此任务未结束时总耗时可能超过 timeout。

参数：

  - timeout：等待空闲的时间上限，不限制关闭耗时

示例：

	// 最多等待 30 秒让池进入空闲状态
	p.CloseByIdle(30 * time.Second)


**参数说明**：

- `timeout` (`time.Duration`)：等待空闲的时间上限，不限制关闭耗时

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.CloseByIdle(timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：关闭提交入口后等待 worker 和已登记任务收尾，排队任务可能执行或被记录为 ErrPoolClosed。不要在所属任务/结果回调中同步关闭本池；忽略取消且不返回的业务可能阻塞关闭。CloseByIdle 每 100ms 检查 Active/Busy 是否为零，不检查 Pending，轮询可越过期限且同步关闭不受其限制。CloseAndWaitTimeout 首次超时返回的 workerDone 只标志 worker 退出，完整队列清理可再调用 Close 等待；重复调用也可能超时。

## `*Pool[T].Ctx`

```go
func (p *Pool[T]) Ctx() context.Context
```

[实现](../../internal/pool/pool.go#L342) · [行为专题](../pool.md)：Ctx 返回 Pool 内部存储的上下文。

**源码说明与使用片段**（原文）：

Ctx 返回 Pool 内部存储的上下文。 当使用 WithFailFast / WithContext 后，此上下文为可取消的子 context； 当使用 Config 的 FailFast: true 后，通过此方法获取 ffCtx 以传递给 Submit。


**参数说明**：无显式参数。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Ctx()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].DefaultShard`

```go
func (p *Pool[T]) DefaultShard() *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L359) · [行为专题](../pool.md)：以 GOMAXPROCS（至少 2）为片数调用 Shard 创建多池；每片保留原池 worker 数，总 worker 数会增加。

**参数说明**：无显式参数。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShard()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：原实例为第 0 片，其他片新建相同 worker 数并复制配置，不复制已有任务/结果/计数。流及环形缓冲使用独立空容器；AutoScale 复制配置并分别启动。shards<=1 仅包装原实例，DefaultShard 按 max(2,GOMAXPROCS)。总 worker 数相乘，统一 Wait/Close 多池；共享 context 的等待/取消会影响各片。

## `*Pool[T].DisableAutoScale`

```go
func (p *Pool[T]) DisableAutoScale()
```

[实现](../../internal/pool/pool.go#L2627) · [行为专题](../pool.md)：DisableAutoScale 停止自动扩缩容，将 worker 数量调整到配置的 MinWorkers。

**源码说明与使用片段**（原文）：

DisableAutoScale 停止自动扩缩容，将 worker 数量调整到配置的 MinWorkers。

停止后仍可通过 Resize 手动调整。如果自动扩缩容未启用，调用无效果。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DisableAutoScale()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Enable 首次复制并 Normalize 配置后启动监视，保持当前 Size，重复调用不更新配置。初始值应在范围内：所有 Resize 路径会扩展保存边界，自动调整也经过此路径；范围外初始值可改变保存的 Min/Max。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验关系或浮点有限性。Disable 停监视并调目标至当前 Min，可能扩容或缩容；IsAutoScaleEnabled 只读标记，不证明后台仍运行。关闭后先 Reset 再启用，Reset 保留已启用配置并重启监视。

## `*Pool[T].EnableAutoScale`

```go
func (p *Pool[T]) EnableAutoScale(config *core.AutoScaleConfig)
```

[实现](../../internal/pool/pool.go#L2598) · [行为专题](../pool.md)：EnableAutoScale 启用自动扩缩容。基于 busy/size 比率周期性检测负载：

**源码说明与使用片段**（原文）：

EnableAutoScale 启用自动扩缩容。基于 busy/size 比率周期性检测负载： - busy/size > ScaleUpThreshold 持续 ScaleUpChecks 次 → 扩容（乘以 ScaleUpFactor，上限 MaxWorkers） - busy/size \< ScaleDownThreshold 持续 ScaleDownChecks 次 → 缩容（乘以 ScaleDownFactor，下限 MinWorkers）

config 为 nil 时使用 DefaultAutoScaleConfig()（CPU\*2 ~ CPU\*100，每 5s 检测）。 默认扩容因子 1.5（增加 50%），缩容因子 0.75（保留 75%），比翻倍/减半更加平滑。

重复调用是安全的（幂等）。调用后启动后台 goroutine 进行负载检测。

示例：

	// 使用默认配置
	p.EnableAutoScale(nil)

	// 自定义配置：10~500 worker，每次扩容 20%，每 3 秒检测
	p.EnableAutoScale(&core.AutoScaleConfig{
	    MinWorkers: 10,
	    MaxWorkers: 500,
	    CheckInterval: 3 * time.Second,
	    ScaleUpFactor: 1.2,
	})


**参数说明**：

- `config` (`*core.AutoScaleConfig`)：自动扩缩容配置指针；nil 时使用默认配置，启用时复制并在副本上 Normalize，不修改调用方配置。读取配置时不得并发修改；启用后的修改不影响已保存的副本。调用会启动后台监视，应在任务前配置并按实例生命周期停止。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.EnableAutoScale(config)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Enable 首次复制并 Normalize 配置后启动监视，保持当前 Size，重复调用不更新配置。初始值应在范围内：所有 Resize 路径会扩展保存边界，自动调整也经过此路径；范围外初始值可改变保存的 Min/Max。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验关系或浮点有限性。Disable 停监视并调目标至当前 Min，可能扩容或缩容；IsAutoScaleEnabled 只读标记，不证明后台仍运行。关闭后先 Reset 再启用，Reset 保留已启用配置并重启监视。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.autoScale = config
```

## `*Pool[T].Errors`

```go
func (p *Pool[T]) Errors() []error
```

[实现](../../internal/pool/pool.go#L2301) · [行为专题](../pool.md)：Errors 返回主结果存储中的非 nil 错误（建议在 Wait 后调用）。

**源码说明与使用片段**（原文）：

Errors 返回主结果存储中的非 nil 错误（建议在 Wait 后调用）。 超出 MaxResults 的溢出结果不包含在内，应从 Wait 或 Flush 返回值中处理。

示例：

	p.Wait()
	for _, err := range p.Errors() {
	    log.Printf("任务错误: %v", err)
	}


**参数说明**：无显式参数。

**返回值签名**：`[]error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Errors()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只读取主结果存储，不包含上限外环形缓冲结果；应先完成等待。FirstError 按主结果索引选择，可能遗漏上限外业务失败；完整错误统计应检查提交错误并用 SubmitWithCompletion 收集每次终态。Values 同时要求 Occupied 且 Err==nil，元素浅拷贝；HasError/FailCount 的统计范围与保留错误列表不同。

## `*Pool[T].FailCount`

```go
func (p *Pool[T]) FailCount() int64
```

[实现](../../internal/pool/pool.go#L2206) · [行为专题](../pool.md)：FailCount 返回已记录的失败任务数，包含执行错误、取消和提交拒绝。

**源码说明与使用片段**（原文）：

FailCount 返回已记录的失败任务数，包含执行错误、取消和提交拒绝。 统计基于转换前的执行/拒绝结果；结果转换函数不改写此计数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].FirstError`

```go
func (p *Pool[T]) FirstError() error
```

[实现](../../internal/pool/pool.go#L2325) · [行为专题](../pool.md)：FirstError 返回主结果存储中的首个任务错误（建议在 Wait 后调用）。

**源码说明与使用片段**（原文）：

FirstError 返回主结果存储中的首个任务错误（建议在 Wait 后调用）。 超出 MaxResults 的溢出结果不包含在内。

示例：

	p.Wait()
	if firstErr := p.FirstError(); firstErr != nil {
	    log.Printf("首个失败: %v", firstErr)
	}


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FirstError()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只读取主结果存储，不包含上限外环形缓冲结果；应先完成等待。FirstError 按主结果索引选择，可能遗漏上限外业务失败；完整错误统计应检查提交错误并用 SubmitWithCompletion 收集每次终态。Values 同时要求 Occupied 且 Err==nil，元素浅拷贝；HasError/FailCount 的统计范围与保留错误列表不同。

## `*Pool[T].Flush`

```go
func (p *Pool[T]) Flush(maxCount int) []core.Result[T]
```

[实现](../../internal/pool/pool.go#L661) · [行为专题](../pool.md)：Flush 从环形缓冲区中取出最多 maxCount 个结果。

**源码说明与使用片段**（原文）：

Flush 从环形缓冲区中取出最多 maxCount 个结果。 未启用环形缓冲时返回 nil。 maxCount \<= 0 取出全部。


**参数说明**：

- `maxCount` (`int`)：本次从环形缓冲取出的最大结果项数；非正值取出全部现有结果。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Flush(maxCount)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Flush 只排空环形缓冲，非正数量取全部，未启用时 nil；不等待业务、不读取主存储。MaxResults=0 时缓冲是镜像，与 Wait 可重复取得同任务；正数上限时 Flush 会消费本来可由 Wait 追加的溢出项。RingBufDropped 只计覆盖最旧项，不计 Block/Error 拒写或流丢弃，不能据 0 证明完整。

## `*Pool[T].HasError`

```go
func (p *Pool[T]) HasError() bool
```

[实现](../../internal/pool/pool.go#L2222) · [行为专题](../pool.md)：HasError 返回是否有任务失败。

**源码说明与使用片段**（原文）：

HasError 返回是否有任务失败。

示例：

	if p.HasError() {
	    log.Printf("有 %d 个任务失败", p.FailCount())
	}


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示观察到了错误，使用 Error/Errors 获取错误值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.HasError()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].IsAutoScaleEnabled`

```go
func (p *Pool[T]) IsAutoScaleEnabled() bool
```

[实现](../../internal/pool/pool.go#L2646) · [行为专题](../pool.md)：IsAutoScaleEnabled 返回是否已启用自动扩缩容。

**源码说明与使用片段**（原文）：

IsAutoScaleEnabled 返回是否已启用自动扩缩容。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsAutoScaleEnabled()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Enable 首次复制并 Normalize 配置后启动监视，保持当前 Size，重复调用不更新配置。初始值应在范围内：所有 Resize 路径会扩展保存边界，自动调整也经过此路径；范围外初始值可改变保存的 Min/Max。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验关系或浮点有限性。Disable 停监视并调目标至当前 Min，可能扩容或缩容；IsAutoScaleEnabled 只读标记，不证明后台仍运行。关闭后先 Reset 再启用，Reset 保留已启用配置并重启监视。

## `*Pool[T].JoinErrors`

```go
func (p *Pool[T]) JoinErrors() error
```

[实现](../../internal/pool/pool.go#L2349) · [行为专题](../pool.md)：JoinErrors 合并主结果存储中的错误（使用 errors.Join）。

**源码说明与使用片段**（原文）：

JoinErrors 合并主结果存储中的错误（使用 errors.Join）。 超出 MaxResults 的溢出结果不包含在内。

示例：

	p.Wait()
	if err := p.JoinErrors(); err != nil {
	    if errors.Is(err, core.ErrTimeout) {
	        log.Println("存在超时错误")
	    }
	}


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.JoinErrors()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只读取主结果存储，不包含上限外环形缓冲结果；应先完成等待。FirstError 按主结果索引选择，可能遗漏上限外业务失败；完整错误统计应检查提交错误并用 SubmitWithCompletion 收集每次终态。Values 同时要求 Occupied 且 Err==nil，元素浅拷贝；HasError/FailCount 的统计范围与保留错误列表不同。

## `*Pool[T].MergeFailFastCancel`

```go
func (p *Pool[T]) MergeFailFastCancel(cancel context.CancelFunc) *Pool[T]
```

[实现](../../internal/pool/pool.go#L406) · [行为专题](../pool.md)：MergeFailFastCancel 将外部 cancel 函数合并到当前 failFast 取消链中。

**源码说明与使用片段**（原文）：

MergeFailFastCancel 将外部 cancel 函数合并到当前 failFast 取消链中。 用于 ShardedPool 等上层组件在多个 Pool 间共享 failFast 取消信号， 确保任一池的任务失败能取消所有池。


**参数说明**：

- `cancel` (`context.CancelFunc`)：合并到 FailFast 取消链的新取消函数，应非 nil；同步执行且须并发安全、快速返回。已有非 nil 取消链时合并 nil 会使后续触发取消 panic；配置须在提交任务前完成。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MergeFailFastCancel(cancel)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只合并取消通知，不会单独启用 FailFast。传入非 nil 的并发安全 CancelFunc，先完成配置再提交；通知不能强制中断回调，也不要在通知中等待或关闭当前执行器。

## `*Pool[T].OkCount`

```go
func (p *Pool[T]) OkCount() int64
```

[实现](../../internal/pool/pool.go#L2211) · [行为专题](../pool.md)：OkCount 返回执行成功的任务数。

**源码说明与使用片段**（原文）：

OkCount 返回执行成功的任务数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OkCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].Pending`

```go
func (p *Pool[T]) Pending() int
```

[实现](../../internal/pool/pool.go#L2249) · [行为专题](../pool.md)：Pending 返回排队中的任务数量。

**源码说明与使用片段**（原文）：

Pending 返回排队中的任务数量。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pending()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].QueueDepth`

```go
func (p *Pool[T]) QueueDepth() int
```

[实现](../../internal/pool/pool.go#L724) · [行为专题](../pool.md)：QueueDepth 返回当前队列深度（等待中的任务数）。

**源码说明与使用片段**（原文）：

QueueDepth 返回当前队列深度（等待中的任务数）。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.QueueDepth()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：等同 Pending，读取已取得等待名额而尚未被 worker 取出的任务数，可包含尚未进入 channel 的任务；等待获取 MaxPending 名额的提交尚不计入。不是 len(taskCh)，也不包含正在执行业务的任务。瞬时快照不预留容量，随后提交仍可能阻塞/拒绝，关闭完成需使用生命周期等待。

## `*Pool[T].Reset`

```go
func (p *Pool[T]) Reset() (*Pool[T], error)
```

[实现](../../internal/pool/pool.go#L2444) · [行为专题](../pool.md)：Reset 在同一 Pool 实例内等待旧 worker 退出并重建任务队列、worker、流和环形缓冲。

**源码说明与使用片段**（原文）：

Reset 在同一 Pool 实例内等待旧 worker 退出并重建任务队列、worker、流和环形缓冲。 保留并发度及任务/提交超时等配置，清零结果与计数，并创建新的 context。 必须在 Wait 之后调用，否则返回错误；与提交、等待及其他配置操作须外部协调。

使用示例：

	p.Wait()
	newP, err := p.Reset()
	if err != nil {
	    log.Fatal(err)
	}
	defer newP.Close()


**参数说明**：无显式参数。

**返回值签名**：`*Pool[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reset()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：返回同一 Pool 指针，在实例内部重建任务队列、worker、流和环形缓冲；不是新分配的独立池。要求本批已进入 waited 状态，并等待超时后的后台任务收尾。保留并发度、任务/提交超时、排队/溢出策略、结果上限、回调、FailFast 及已启用的 AutoScale 配置；清零结果/计数并重建 context。须重新取得流通道，不与提交/等待/配置并发调用。

## `*Pool[T].ResetWait`

```go
func (p *Pool[T]) ResetWait() *Pool[T]
```

[实现](../../internal/pool/pool.go#L2016) · [行为专题](../pool.md)：ResetWait 轻量复位池的 Wait/submit 状态以支持复用。

**源码说明与使用片段**（原文）：

ResetWait 轻量复位池的 Wait/submit 状态以支持复用。 不会重启 worker，仅清零计数器与结果存储。 必须在 Wait() 之后、下次 Submit() 之前调用。 After a timed wait it joins unfinished tasks before resetting their state. ResetWait 不重建 WithContext 或 WithFailFast 创建的上下文；Wait 后它们已取消， 如需在下一批使用池上下文，须使用新的父上下文重新调用相应配置方法。

示例：

	p.Submit(ctx, task1)
	r1 := p.Wait()
	p.ResetWait()
	p.Submit(ctx, task2)
	r2 := p.Wait()


**参数说明**：无显式参数。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ResetWait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：只有已经进入 waited 状态才复位；等待未结束任务后清空本批结果、计数和环形缓冲，重建已启用的流通道，不重启 worker。不会重新打开已关闭的池，也不重建 Wait 已取消的池 context；下一批需要时重新配置 context/FailFast。须外部协调，不与提交或等待并发。

## `*Pool[T].Resize`

```go
func (p *Pool[T]) Resize(newSize int) int
```

[实现](../../internal/pool/pool.go#L1601) · [行为专题](../pool.md)：Resize 动态调整 worker 数量，返回新增或移除的 worker 数量。

**源码说明与使用片段**（原文）：

Resize 动态调整 worker 数量，返回新增或移除的 worker 数量。 newSize 小于等于零时使用 core.IO()。 扩容时立即启动新 worker，缩容时向退出的 worker 发送退出信号。

参数：

  - newSize：目标 worker 数量

示例：

	added := p.Resize(8)  // 扩容到 8 个 worker
	fmt.Printf("新增 %d 个 worker\n", added)
	removed := p.Resize(2) // 缩容到 2 个 worker
	fmt.Printf("移除 %d 个 worker\n", removed)


**参数说明**：

- `newSize` (`int`)：目标 worker 数量

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Resize(newSize)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：非正目标按 IO()，已关闭池不调整；Size 先更新为目标，缩容 worker 随后退出，不能据 Size 判断旧 worker 已结束。Resize 返回新增或退出名额的非负数量，同大小为 0；不会重建队列容量。全部 Resize 路径都扩展已启用 AutoScale 的上下界。ResizeAndWaitTimeout 在 Resize 后暂停新提交并等待已登记任务归零，不取消任务，也不返回完成标记；计时不覆盖 Resize 和登记收敛，返回后恢复本次暂停，其他等待/关闭设置的提交限制仍有效。

## `*Pool[T].ResizeAndWaitTimeout`

```go
func (p *Pool[T]) ResizeAndWaitTimeout(newSize int, timeout time.Duration)
```

[实现](../../internal/pool/pool.go#L1675) · [行为专题](../pool.md)：ResizeAndWaitTimeout 调整 worker 数量，随后限时等待已登记任务完成。

**源码说明与使用片段**（原文）：

ResizeAndWaitTimeout 调整 worker 数量，随后限时等待已登记任务完成。 timeout 仅限制最后的任务等待，不覆盖 Resize 或正在登记的提交收敛。

参数：

  - newSize：目标 worker 数量
  - timeout：等待已登记任务归零的时长；非正值立即到期，不取消任务

示例：

	// 缩容到 5 个 worker，在登记收敛后最多等待 10 秒让已登记任务完成
	p.ResizeAndWaitTimeout(5, 10*time.Second)


**参数说明**：

- `newSize` (`int`)：目标 worker 数量
- `timeout` (`time.Duration`)：仅限制最后等待已登记任务归零的时长；Resize 的同步锁等待和正在登记的提交收敛均发生在计时之前，不是整个方法的调用时限。非正值使最后等待立即到期，超时不取消执行。未返回是否完成的标记，也不保证所有旧 worker 均已退出。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ResizeAndWaitTimeout(newSize, timeout)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：非正目标按 IO()，已关闭池不调整；Size 先更新为目标，缩容 worker 随后退出，不能据 Size 判断旧 worker 已结束。Resize 返回新增或退出名额的非负数量，同大小为 0；不会重建队列容量。全部 Resize 路径都扩展已启用 AutoScale 的上下界。ResizeAndWaitTimeout 在 Resize 后暂停新提交并等待已登记任务归零，不取消任务，也不返回完成标记；计时不覆盖 Resize 和登记收敛，返回后恢复本次暂停，其他等待/关闭设置的提交限制仍有效。

## `*Pool[T].RingBufDropped`

```go
func (p *Pool[T]) RingBufDropped() int64
```

[实现](../../internal/pool/pool.go#L669) · [行为专题](../pool.md)：RingBufDropped 返回环形缓冲区因 OverflowDrop 覆盖丢弃的元素数。

**源码说明与使用片段**（原文）：

RingBufDropped 返回环形缓冲区因 OverflowDrop 覆盖丢弃的元素数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.RingBufDropped()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Flush 只排空环形缓冲，非正数量取全部，未启用时 nil；不等待业务、不读取主存储。MaxResults=0 时缓冲是镜像，与 Wait 可重复取得同任务；正数上限时 Flush 会消费本来可由 Wait 追加的溢出项。RingBufDropped 只计覆盖最旧项，不计 Block/Error 拒写或流丢弃，不能据 0 证明完整。

## `*Pool[T].Shard`

```go
func (p *Pool[T]) Shard(shards int) *MultiPool[T]
```

[实现](../../internal/pool/multpool.go#L296) · [行为专题](../pool.md)：创建多池，原池作为第 0 片，其余片复制当前配置与 worker 数；shards <= 1 仅包装原池。须在提交前完成配置并统一关闭多池。

**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。

**返回值签名**：`*MultiPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shard(shards)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：原实例为第 0 片，其他片新建相同 worker 数并复制配置，不复制已有任务/结果/计数。流及环形缓冲使用独立空容器；AutoScale 复制配置并分别启动。shards<=1 仅包装原实例，DefaultShard 按 max(2,GOMAXPROCS)。总 worker 数相乘，统一 Wait/Close 多池；共享 context 的等待/取消会影响各片。

## `*Pool[T].Size`

```go
func (p *Pool[T]) Size() int
```

[实现](../../internal/pool/pool.go#L2234) · [行为专题](../pool.md)：Size 返回当前目标 worker 数量。

**源码说明与使用片段**（原文）：

Size 返回当前目标 worker 数量。 缩容时 worker 可尚未退出，关闭后也不自动归零，不表示实时存活数量。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Size()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].Stats`

```go
func (p *Pool[T]) Stats() PoolStats
```

[实现](../../internal/pool/pool.go#L2278) · [行为专题](../pool.md)：Stats 返回池的完整统计信息。

**源码说明与使用片段**（原文）：

Stats 返回池的完整统计信息。

示例：

	stats := p.Stats()
	fmt.Printf("并发度: %d, 成功: %d, 失败: %d\n",
	    stats.Size, stats.OkTask, stats.FailTask)


**参数说明**：无显式参数。

**返回值签名**：`PoolStats`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stats()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].StreamDropped`

```go
func (p *Pool[T]) StreamDropped() int64
```

[实现](../../internal/pool/pool.go#L633) · [行为专题](../pool.md)：StreamDropped 返回因 stream channel 满而被丢弃的结果数。

**源码说明与使用片段**（原文）：

StreamDropped 返回因 stream channel 满而被丢弃的结果数。 仅在启用了 WithStreaming 时有效。 如果此值持续增长，说明消费者速度跟不上生产者，应增大 channel 缓冲或加速消费。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StreamDropped()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：WithStreaming 立即替换通道，非正容量按 Size×2；只在提交前配置一次，旧引用不自动更新/关闭。StreamResults 未启用返回 nil、启用返回同一通道，多消费者竞争；满时直接丢结果并增 StreamDropped，Wait/关闭收尾关通道。普通预检查拒绝不进入流，不能把流当全部尝试审计，最终结果保留仍独立。

## `*Pool[T].StreamResults`

```go
func (p *Pool[T]) StreamResults() <-chan core.Result[T]
```

[实现](../../internal/pool/pool.go#L626) · [行为专题](../pool.md)：StreamResults 返回流式结果的只读 channel，必须在 WithStreaming 之后调用。

**源码说明与使用片段**（原文）：

StreamResults 返回流式结果的只读 channel，必须在 WithStreaming 之后调用。 channel 在 Wait() 完成后自动关闭。 不启用流式消费时返回 nil。


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StreamResults()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：WithStreaming 立即替换通道，非正容量按 Size×2；只在提交前配置一次，旧引用不自动更新/关闭。StreamResults 未启用返回 nil、启用返回同一通道，多消费者竞争；满时直接丢结果并增 StreamDropped，Wait/关闭收尾关通道。普通预检查拒绝不进入流，不能把流当全部尝试审计，最终结果保留仍独立。

## `*Pool[T].Submit`

```go
func (p *Pool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/pool/pool.go#L980) · [行为专题](../pool.md)：Submit 向协程池提交一个任务，返回提交结果（无索引）。

**源码说明与使用片段**（原文）：

Submit 向协程池提交一个任务，返回提交结果（无索引）。

参数：

  - ctx：上下文，用于取消和 TraceID
  - fn：任务执行函数，接收派生 context，返回结果和错误

示例：

	err := p.Submit(ctx, func(ctx context.Context) (string, error) {
	    return fetchData(ctx)
	})
	if err != nil {
	    log.Printf("提交失败: %v", err)
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文，用于取消和 TraceID
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Submit(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。返回 error 仅代表提交状态，普通 Submit 可等待队列，TrySubmit 不等空位且队列满返回 ErrSubmitTimeout。TrySubmit 对已关闭/已等待/正在等待或等待缩容的池直接拒绝，不增加 Total/Fail 或保存结果；通过这些状态检查后，已取消 context 和队列拒绝才记录。普通 Submit 的状态拒绝仍记录结果/计数。MaxPending 配合 OverflowDrop 可丢弃新任务而返回 nil，必须独立核对提交错误和终态。SubmitAt 宜用唯一连续合法索引，稀疏位置产生 Occupied=false 空位，有效任务重复索引可覆盖旧结果。需要记录的预检查/直接背压拒绝在新索引原位或已分配序号之后记录，不进入普通结果回调或结果流；非法索引不存储/不计数。业务 panic 转为错误，最终等待并检查逐项 Err。

## `*Pool[T].SubmitAt`

```go
func (p *Pool[T]) SubmitAt(index int, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/pool/pool.go#L1284) · [行为专题](../pool.md)：SubmitAt 在指定索引位置提交任务，保证结果的有序性。

**源码说明与使用片段**（原文）：

SubmitAt 在指定索引位置提交任务，保证结果的有序性。

参数：

  - index：任务在结果切片中的索引
  - ctx：上下文
  - fn：任务执行函数

示例：

	for i, item := range items {
	    idx := i
	    p.SubmitAt(idx, ctx, func(ctx context.Context) (string, error) {
	        return process(ctx, items[idx])
	    })
	}


**参数说明**：

- `index` (`int`)：本池结果的零起始索引；负值或大于 core.MaxPoolIndex 时返回 ErrInvalidIndex。需稳定排序时使用唯一连续索引；稀疏索引增加存储成本，MaxResults 可限制该位置是否保留。
- `ctx` (`context.Context`)：上下文
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitAt(index, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。返回 error 仅代表提交状态，普通 Submit 可等待队列，TrySubmit 不等空位且队列满返回 ErrSubmitTimeout。TrySubmit 对已关闭/已等待/正在等待或等待缩容的池直接拒绝，不增加 Total/Fail 或保存结果；通过这些状态检查后，已取消 context 和队列拒绝才记录。普通 Submit 的状态拒绝仍记录结果/计数。MaxPending 配合 OverflowDrop 可丢弃新任务而返回 nil，必须独立核对提交错误和终态。SubmitAt 宜用唯一连续合法索引，稀疏位置产生 Occupied=false 空位，有效任务重复索引可覆盖旧结果。需要记录的预检查/直接背压拒绝在新索引原位或已分配序号之后记录，不进入普通结果回调或结果流；非法索引不存储/不计数。业务 panic 转为错误，最终等待并检查逐项 Err。

## `*Pool[T].SubmitAtWithCompletion`

```go
func (p *Pool[T]) SubmitAtWithCompletion(index int, ctx context.Context, fn func(context.Context) (T, error), complete func(core.Result[T])) error
```

[实现](../../internal/pool/pool.go#L1290) · [行为专题](../pool.md)：SubmitAtWithCompletion reports each terminal submission outcome to an internal adapter.

**源码说明与使用片段**（原文）：

SubmitAtWithCompletion reports each terminal submission outcome to an internal adapter. The callback runs synchronously and must not wait for or close this pool.


**参数说明**：

- `index` (`int`)：本池结果的零起始索引；负值或大于 core.MaxPoolIndex 时返回 ErrInvalidIndex。需稳定排序时使用唯一连续索引；稀疏索引增加存储成本，MaxResults 可限制该位置是否保留。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。
- `complete` (`func(core.Result[T])`)：接收该次提交终态（包括拒绝、取消及执行结果）的同步通知，最多调用一次。可能在提交线程或 worker 调用；须非 nil、支持并发，panic 被记录；不得同步等待或关闭本池。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitAtWithCompletion(index, ctx, fn, complete)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。complete 每次提交最多调用一次，覆盖预检查拒绝、背压丢弃及执行结果，可能在提交调用返回前同步执行。OverflowDrop 的提交 error 可为 nil，complete 的 Err 仍为 ErrQueueOverflow；不能只靠返回 error 计成功。回调 panic 被恢复并记录，不更改已保存结果；业务 callback 和 complete 都须及时返回，不在其中 Wait/Close 本池。

## `*Pool[T].SubmitAtWithResultTransform`

```go
func (p *Pool[T]) SubmitAtWithResultTransform(index int, ctx context.Context, fn func(context.Context) (T, error), transform func(core.Result[T]) core.Result[T]) error
```

[实现](../../internal/pool/pool.go#L1313) · [行为专题](../pool.md)：SubmitAtWithResultTransform lets an internal adapter transform a result before

**源码说明与使用片段**（原文）：

SubmitAtWithResultTransform lets an internal adapter transform a result before it is recorded. The transform is synchronous and must not wait for or close the pool. A transform panic is converted to a result error.


**参数说明**：

- `index` (`int`)：本池结果的零起始索引；负值或大于 core.MaxPoolIndex 时返回 ErrInvalidIndex。需稳定排序时使用唯一连续索引；稀疏索引增加存储成本，MaxResults 可限制该位置是否保留。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。
- `transform` (`func(core.Result[T]) core.Result[T]`)：写入存储前同步转换本项 Result，返回实际保存的 Result；nil 保留原结果，panic 转为该项 PanicError。多个任务可并发调用，不得在回调内等待或关闭本池。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitAtWithResultTransform(index, ctx, fn, transform)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。按 SubmitAt 提交并在记录路径调用 transform，nil 不转换；同步且可并发。panic 将 Err 改为 PanicError 并保留转换前 Value，局部副本在 panic 前的字段修改不提交，引用副作用仍可能共享。返回的 Value/Err 改变保存、流和环形缓冲内容，Pool 计数及 FailFast 仍基于业务执行或拒绝路径，转换错误不单独触发池取消、清除错误也不撤销已发生取消。合法索引的预检查/背压拒绝也转换但返回原拒绝 error，不通知全局回调或结果流；非法索引不转换/保存/计数。不得在其中 Wait/Close 本池，Wait 会等待同步转换返回。

## `*Pool[T].SubmitWithCompletion`

```go
func (p *Pool[T]) SubmitWithCompletion(ctx context.Context, fn func(context.Context) (T, error), complete func(core.Result[T])) error
```

[实现](../../internal/pool/pool.go#L988) · [行为专题](../pool.md)：SubmitWithCompletion notifies internal adapters of every terminal outcome,

**源码说明与使用片段**（原文）：

SubmitWithCompletion notifies internal adapters of every terminal outcome, including rejection, overflow and cancellation before execution. The callback runs synchronously and must not wait for or close this pool.


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。
- `complete` (`func(core.Result[T])`)：接收该次提交终态（包括拒绝、取消及执行结果）的同步通知，最多调用一次。可能在提交线程或 worker 调用；须非 nil、支持并发，panic 被记录；不得同步等待或关闭本池。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitWithCompletion(ctx, fn, complete)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。complete 每次提交最多调用一次，覆盖预检查拒绝、背压丢弃及执行结果，可能在提交调用返回前同步执行。OverflowDrop 的提交 error 可为 nil，complete 的 Err 仍为 ErrQueueOverflow；不能只靠返回 error 计成功。回调 panic 被恢复并记录，不更改已保存结果；业务 callback 和 complete 都须及时返回，不在其中 Wait/Close 本池。

## `*Pool[T].SubmitWithCompletionTimeout`

```go
func (p *Pool[T]) SubmitWithCompletionTimeout(ctx context.Context, fn func(context.Context) (T, error), complete func(core.Result[T]), timeout time.Duration) error
```

[实现](../../internal/pool/pool.go#L993) · [行为专题](../pool.md)：SubmitWithCompletionTimeout overrides the timeout for this task when positive.

**源码说明与使用片段**（原文）：

SubmitWithCompletionTimeout overrides the timeout for this task when positive.


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。
- `complete` (`func(core.Result[T])`)：接收该次提交终态（包括拒绝、取消及执行结果）的同步通知，最多调用一次。可能在提交线程或 worker 调用；须非 nil、支持并发，panic 被记录；不得同步等待或关闭本池。
- `timeout` (`time.Duration`)：此任务的执行超时覆盖；仅正数覆盖池的任务超时，非正值继续使用池设置，不表示清除池超时。等待入队仍由提交超时和 context 控制。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitWithCompletionTimeout(ctx, fn, complete, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。complete 每次提交最多调用一次，覆盖预检查拒绝、背压丢弃及执行结果，可能在提交调用返回前同步执行。OverflowDrop 的提交 error 可为 nil，complete 的 Err 仍为 ErrQueueOverflow；不能只靠返回 error 计成功。回调 panic 被恢复并记录，不更改已保存结果；业务 callback 和 complete 都须及时返回，不在其中 Wait/Close 本池。

## `*Pool[T].TotalCount`

```go
func (p *Pool[T]) TotalCount() int64
```

[实现](../../internal/pool/pool.go#L2228) · [行为专题](../pool.md)：TotalCount 返回已记录的提交尝试数，包含已记录的拒绝和丢弃。

**源码说明与使用片段**（原文）：

TotalCount 返回已记录的提交尝试数，包含已记录的拒绝和丢弃。 非法索引的 SubmitAt 在计数之前返回；此值不等于实际执行或保留结果的数量。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。

## `*Pool[T].TrySubmit`

```go
func (p *Pool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/pool/pool.go#L1467) · [行为专题](../pool.md)：TrySubmit 非阻塞向协程池提交任务。如果任务队列已满则立即返回错误。

**源码说明与使用片段**（原文）：

TrySubmit 非阻塞向协程池提交任务。如果任务队列已满则立即返回错误。

参数：

  - ctx：上下文
  - fn：任务执行函数

示例：

	err := p.TrySubmit(ctx, fn)
	if errors.Is(err, core.ErrSubmitTimeout) {
	    log.Println("队列已满，任务被丢弃")
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TrySubmit(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。返回 error 仅代表提交状态，普通 Submit 可等待队列，TrySubmit 不等空位且队列满返回 ErrSubmitTimeout。TrySubmit 对已关闭/已等待/正在等待或等待缩容的池直接拒绝，不增加 Total/Fail 或保存结果；通过这些状态检查后，已取消 context 和队列拒绝才记录。普通 Submit 的状态拒绝仍记录结果/计数。MaxPending 配合 OverflowDrop 可丢弃新任务而返回 nil，必须独立核对提交错误和终态。SubmitAt 宜用唯一连续合法索引，稀疏位置产生 Occupied=false 空位，有效任务重复索引可覆盖旧结果。需要记录的预检查/直接背压拒绝在新索引原位或已分配序号之后记录，不进入普通结果回调或结果流；非法索引不存储/不计数。业务 panic 转为错误，最终等待并检查逐项 Err。

## `*Pool[T].Values`

```go
func (p *Pool[T]) Values() []T
```

[实现](../../internal/pool/pool.go#L2421) · [行为专题](../pool.md)：Values 提取主结果存储中成功的值（建议在 Wait 后调用）。

**源码说明与使用片段**（原文）：

Values 提取主结果存储中成功的值（建议在 Wait 后调用）。 超出 MaxResults 的溢出结果不包含在内。

示例：

	p.Wait()
	values := p.Values()
	fmt.Printf("成功值: %v\n", values)


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只读取主结果存储，不包含上限外环形缓冲结果；应先完成等待。FirstError 按主结果索引选择，可能遗漏上限外业务失败；完整错误统计应检查提交错误并用 SubmitWithCompletion 收集每次终态。Values 同时要求 Occupied 且 Err==nil，元素浅拷贝；HasError/FailCount 的统计范围与保留错误列表不同。

## `*Pool[T].Wait`

```go
func (p *Pool[T]) Wait() []core.Result[T]
```

[实现](../../internal/pool/pool.go#L1962) · [行为专题](../pool.md)：Wait 阻塞等待所有已提交的任务完成。主结果按提交顺序排列；

**源码说明与使用片段**（原文）：

Wait 阻塞等待所有已提交的任务完成。主结果按提交顺序排列； 配置 MaxResults 和环形缓冲时，剩余溢出结果按写入缓冲的顺序追加，可能因缓冲满而丢失。 Wait 只能调用一次，多次调用返回空切片。

示例：

	results := p.Wait()
	for _, r := range results {
	    if r.Ok() {
	        fmt.Printf("成功: %v\n", r.Value)
	    } else {
	        log.Printf("失败: %v\n", r.Err)
	    }
	}


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：每批 Wait/WaitContext/WaitTimeout 共用一次性等待入口，重复调用返回 nil（带 bool 时为 false）。等待后禁止提交，下一批先复位；超时/取消只请求任务退出，后台清理继续。主结果按索引保留，MaxResults>0 时追加仍在环形缓冲的溢出项，可能已丢失；MaxResults==0 的镜像不会被 Wait 排空。当前任务先递减 Busy/Active 再释放等待登记，无并发复位或新批次时，成功等完本批的业务计数已归零；提前返回不保证。WaitAndClose 在等待后关闭池；其他等待方法不关闭 worker，需 Close 或 Run 管理。

## `*Pool[T].WaitAndClose`

```go
func (p *Pool[T]) WaitAndClose() []core.Result[T]
```

[实现](../../internal/pool/pool.go#L1996) · [行为专题](../pool.md)：WaitAndClose 等待所有任务完成后关闭池，返回结果。

**源码说明与使用片段**（原文）：

WaitAndClose 等待所有任务完成后关闭池，返回结果。 等价于先调用 Wait() 再调用 Close()。

示例：

	results := p.WaitAndClose()


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitAndClose()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：每批 Wait/WaitContext/WaitTimeout 共用一次性等待入口，重复调用返回 nil（带 bool 时为 false）。等待后禁止提交，下一批先复位；超时/取消只请求任务退出，后台清理继续。主结果按索引保留，MaxResults>0 时追加仍在环形缓冲的溢出项，可能已丢失；MaxResults==0 的镜像不会被 Wait 排空。当前任务先递减 Busy/Active 再释放等待登记，无并发复位或新批次时，成功等完本批的业务计数已归零；提前返回不保证。WaitAndClose 在等待后关闭池；其他等待方法不关闭 worker，需 Close 或 Run 管理。

## `*Pool[T].WaitContext`

```go
func (p *Pool[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool)
```

[实现](../../internal/pool/pool.go#L2380) · [行为专题](../pool.md)：WaitContext 使用 Context 控制等待，ctx 取消或超时后返回。

**源码说明与使用片段**（原文）：

WaitContext 使用 Context 控制等待，ctx 取消或超时后返回。

参数：

  - ctx：上下文，取消后返回部分结果

示例：

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results, ok := p.WaitContext(ctx)


**参数说明**：

- `ctx` (`context.Context`)：上下文，取消后返回部分结果

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitContext(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：每批 Wait/WaitContext/WaitTimeout 共用一次性等待入口，重复调用返回 nil（带 bool 时为 false）。等待后禁止提交，下一批先复位；超时/取消只请求任务退出，后台清理继续。主结果按索引保留，MaxResults>0 时追加仍在环形缓冲的溢出项，可能已丢失；MaxResults==0 的镜像不会被 Wait 排空。当前任务先递减 Busy/Active 再释放等待登记，无并发复位或新批次时，成功等完本批的业务计数已归零；提前返回不保证。WaitAndClose 在等待后关闭池；其他等待方法不关闭 worker，需 Close 或 Run 管理。

## `*Pool[T].WaitTimeout`

```go
func (p *Pool[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool)
```

[实现](../../internal/pool/pool.go#L2364) · [行为专题](../pool.md)：WaitTimeout 带超时的任务等待，ok 为 true 表示在超时前完成。

**源码说明与使用片段**（原文）：

WaitTimeout 带超时的任务等待，ok 为 true 表示在超时前完成。

参数：

  - d：最长等待时间

示例：

	results, ok := p.WaitTimeout(5 * time.Second)
	if !ok {
	    log.Println("等待超时")
	}


**参数说明**：

- `d` (`time.Duration`)：本次等待的最长时长；非正值使等待 context 立即过期，完成与超时同时就绪时不保证 bool 的选择。超时返回部分结果并通知取消，不能强制停止任务；分片共享同一截止时间，复用须遵守该实例的等待/复位约定。

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitTimeout(d)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：每批 Wait/WaitContext/WaitTimeout 共用一次性等待入口，重复调用返回 nil（带 bool 时为 false）。等待后禁止提交，下一批先复位；超时/取消只请求任务退出，后台清理继续。主结果按索引保留，MaxResults>0 时追加仍在环形缓冲的溢出项，可能已丢失；MaxResults==0 的镜像不会被 Wait 排空。当前任务先递减 Busy/Active 再释放等待登记，无并发复位或新批次时，成功等完本批的业务计数已归零；提前返回不保证。WaitAndClose 在等待后关闭池；其他等待方法不关闭 worker，需 Close 或 Run 管理。

## `*Pool[T].WithContext`

```go
func (p *Pool[T]) WithContext(ctx context.Context) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L330) · [行为专题](../pool.md)：WithContext 设置池的上下文，基于传入 ctx 创建可取消的子 context。

**源码说明与使用片段**（原文）：

WithContext 设置池的上下文，基于传入 ctx 创建可取消的子 context。

参数：

  - ctx：原始上下文

示例：

	p, ctx := pool.NewPool[string](4).WithContext(ctx)
	// ctx 是可取消的子 context


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithContext(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.ctx = ctx
```

## `*Pool[T].WithCtxSto`

```go
func (p *Pool[T]) WithCtxSto(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L566) · [行为专题](../pool.md)：WithCtxSto 设置 Context 和提交超时（Sto = SubmitTimeout）。

**源码说明与使用片段**（原文）：

WithCtxSto 设置 Context 和提交超时（Sto = SubmitTimeout）。

参数：

  - ctx：原始上下文
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `submitTimeout` (`time.Duration`)：提交超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxSto(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithCtxStoTID`

```go
func (p *Pool[T]) WithCtxStoTID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L577) · [行为专题](../pool.md)：WithCtxStoTID 设置 Context、提交超时并注入 trace_id。

**源码说明与使用片段**（原文）：

WithCtxStoTID 设置 Context、提交超时并注入 trace\_id。

参数：

  - ctx：原始上下文
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `submitTimeout` (`time.Duration`)：提交超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxStoTID(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithCtxTOTID`

```go
func (p *Pool[T]) WithCtxTOTID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L530) · [行为专题](../pool.md)：WithCtxTOTID 设置 Context、任务超时并注入 trace_id。

**源码说明与使用片段**（原文）：

WithCtxTOTID 设置 Context、任务超时并注入 trace\_id。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：任务超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTOTID(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithCtxTimeout`

```go
func (p *Pool[T]) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L519) · [行为专题](../pool.md)：WithCtxTimeout 设置 Context 和任务超时。

**源码说明与使用片段**（原文）：

WithCtxTimeout 设置 Context 和任务超时。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：任务超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTimeout(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithCtxTraceID`

```go
func (p *Pool[T]) WithCtxTraceID(ctx context.Context) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L476) · [行为专题](../pool.md)：WithCtxTraceID 设置 Context 并注入 trace_id。

**源码说明与使用片段**（原文）：

WithCtxTraceID 设置 Context 并注入 trace\_id。

参数：

  - ctx：原始上下文


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFFCtx`

```go
func (p *Pool[T]) WithFFCtx(ctx context.Context) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L417) · [行为专题](../pool.md)：WithFFCtx 等价于 WithFailFast，是 FailFast + Context 的缩写形式。

**源码说明与使用片段**（原文）：

WithFFCtx 等价于 WithFailFast，是 FailFast + Context 的缩写形式。

参数：

  - ctx：原始上下文


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFCtx(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFFSto`

```go
func (p *Pool[T]) WithFFSto(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L440) · [行为专题](../pool.md)：WithFFSto 启用 FailFast 并设置提交超时（Sto = SubmitTimeout）。

**源码说明与使用片段**（原文）：

WithFFSto 启用 FailFast 并设置提交超时（Sto = SubmitTimeout）。 submitTimeout 控制任务入队的最大等待时间。

参数：

  - ctx：原始上下文
  - submitTimeout：提交超时时间

示例：

	p, ffCtx := pool.NewPool[string](4).WithFFSto(ctx, 5*time.Second)


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `submitTimeout` (`time.Duration`)：提交超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFSto(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFFStoTID`

```go
func (p *Pool[T]) WithFFStoTID(ctx context.Context, submitTimeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L451) · [行为专题](../pool.md)：WithFFStoTID 启用 FailFast、设置提交超时并注入 trace_id。

**源码说明与使用片段**（原文）：

WithFFStoTID 启用 FailFast、设置提交超时并注入 trace\_id。

参数：

  - ctx：原始上下文
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `submitTimeout` (`time.Duration`)：提交超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFStoTID(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFFTOSto`

```go
func (p *Pool[T]) WithFFTOSto(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L497) · [行为专题](../pool.md)：WithFFTOSto 启用 FailFast、设置任务超时和提交超时（TO=Timeout, Sto=SubmitTimeout）。

**源码说明与使用片段**（原文）：

WithFFTOSto 启用 FailFast、设置任务超时和提交超时（TO=Timeout, Sto=SubmitTimeout）。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：任务超时时间
- `submitTimeout` (`time.Duration`)：提交超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOSto(ctx, timeout, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFFTOStoTID`

```go
func (p *Pool[T]) WithFFTOStoTID(ctx context.Context, timeout, submitTimeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L509) · [行为专题](../pool.md)：WithFFTOStoTID 启用 FailFast、设置超时、提交超时并注入 trace_id（TO=Timeout, Sto=SubmitTimeout, TID=TraceID）。

**源码说明与使用片段**（原文）：

WithFFTOStoTID 启用 FailFast、设置超时、提交超时并注入 trace\_id（TO=Timeout, Sto=SubmitTimeout, TID=TraceID）。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：任务超时时间
- `submitTimeout` (`time.Duration`)：提交超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOStoTID(ctx, timeout, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFFTOTID`

```go
func (p *Pool[T]) WithFFTOTID(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L486) · [行为专题](../pool.md)：WithFFTOTID 启用 FailFast、设置超时并注入 trace_id（TO=Timeout, TID=TraceID）。

**源码说明与使用片段**（原文）：

WithFFTOTID 启用 FailFast、设置超时并注入 trace\_id（TO=Timeout, TID=TraceID）。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：任务超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOTID(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFFTimeout`

```go
func (p *Pool[T]) WithFFTimeout(ctx context.Context, timeout time.Duration) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L466) · [行为专题](../pool.md)：WithFFTimeout 启用 FailFast 并设置每个任务的超时时间。

**源码说明与使用片段**（原文）：

WithFFTimeout 启用 FailFast 并设置每个任务的超时时间。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间

示例：

	p, ffCtx := pool.NewPool[string](4).WithFFTimeout(ctx, 10*time.Second)
	p.Submit(ffCtx, longRunningTask)


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：任务超时时间

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTimeout(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFFTraceID`

```go
func (p *Pool[T]) WithFFTraceID(ctx context.Context) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L425) · [行为专题](../pool.md)：WithFFTraceID 启用 FailFast 并注入 trace_id。

**源码说明与使用片段**（原文）：

WithFFTraceID 启用 FailFast 并注入 trace\_id。

参数：

  - ctx：原始上下文


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

## `*Pool[T].WithFailFast`

```go
func (p *Pool[T]) WithFailFast(ctx context.Context) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L393) · [行为专题](../pool.md)：WithFailFast 启用 FailFast 模式：任一任务失败后立即取消其余任务。

**源码说明与使用片段**（原文）：

WithFailFast 启用 FailFast 模式：任一任务失败后立即取消其余任务。 失败时自动调用 cancel 触发 ctx 的 Done 信号。

参数：

  - ctx：原始上下文

示例：

	p, ffCtx := pool.NewPool[string](4).WithFailFast(ctx)
	for _, item := range items {
	    p.Submit(ffCtx, func(ctx context.Context) (string, error) {
	        return validate(ctx, item)
	    })
	}
	results := p.Wait()


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFailFast(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.ctx = ctx
```

## `*Pool[T].WithMaxPending`

```go
func (p *Pool[T]) WithMaxPending(maxPending int) *Pool[T]
```

[实现](../../internal/pool/pool.go#L700) · [行为专题](../pool.md)：WithMaxPending 设置最大等待任务数，配合 WithOverflow 使用。

**源码说明与使用片段**（原文）：

WithMaxPending 设置最大等待任务数，配合 WithOverflow 使用。 maxPending > 0 时更新限制；非正值保留已有配置，不能用于清除已有上限。 当 Pending() >= maxPending 时，新提交根据溢出策略处理。

示例：

	p := pool.NewPool[string](8).
	    WithMaxPending(1000).
	    WithOverflow(core.OverflowError)


**参数说明**：

- `maxPending` (`int`)：等待提交的任务数上限，以任务数计；零值与溢出处理见组件配置。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithMaxPending(maxPending)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只限制等待任务，不含正在执行业务的 worker；MaxPending 仅正数更新，非正不清除现有上限，也不改变底层 channel 容量。Drop 达限丢弃且提交可返回 nil，Error 返回 ErrQueueOverflow，Block 等空位并响应 context/提交期限/关闭。此策略不代替结果环形缓冲策略；配置在提交前完成。

## `*Pool[T].WithMaxResults`

```go
func (p *Pool[T]) WithMaxResults(maxResults int) *Pool[T]
```

[实现](../../internal/pool/pool.go#L684) · [行为专题](../pool.md)：WithMaxResults 设置 results 切片的容量上限（0=无限；默认使用全局配置）。

**源码说明与使用片段**（原文）：

WithMaxResults 设置 results 切片的容量上限（0=无限；默认使用全局配置）。 当提交数达到上限后，后续任务的结果只通过流式或环形缓冲区消费。 这是防止长期运行的池内存持续增长的关键配置。

示例：

	// 最多保留 100 万个结果在内存中
	p := pool.NewPool[string](8).WithMaxResults(1_000_000)


**参数说明**：

- `maxResults` (`int`)：每池主结果的保留上限；0 无限制，正数按提交索引限制，负数忽略并保留原配置。应在提交前设置，超出部分通过已配置的环形缓冲、流或回调处理，否则可能丢失。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithMaxResults(maxResults)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：非负更新主结果保留上限，0 无限制，负值忽略；正数按提交索引保留，非按完成先后。更改不会回收已分配结果槽位，提交期间改动可能使不同任务采用不同上限快照，须提前配置。超限结果需单独用 completion/流/环形缓冲处理；错误计数不受保留上限约束。

## `*Pool[T].WithOverflow`

```go
func (p *Pool[T]) WithOverflow(strategy core.OverflowStrategy) *Pool[T]
```

[实现](../../internal/pool/pool.go#L718) · [行为专题](../pool.md)：WithOverflow 设置队列溢出策略。

**源码说明与使用片段**（原文）：

WithOverflow 设置队列溢出策略。

	core.OverflowBlock：阻塞等待（默认行为）
	core.OverflowDrop：静默丢弃
	core.OverflowError：返回 ErrQueueOverflow

示例：

	p := pool.NewPool[string](8).
	    WithMaxPending(2000).
	    WithOverflow(core.OverflowDrop)


**参数说明**：

- `strategy` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithOverflow(strategy)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只限制等待任务，不含正在执行业务的 worker；MaxPending 仅正数更新，非正不清除现有上限，也不改变底层 channel 容量。Drop 达限丢弃且提交可返回 nil，Error 返回 ErrQueueOverflow，Block 等空位并响应 context/提交期限/关闭。此策略不代替结果环形缓冲策略；配置在提交前完成。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.overflowStrat = strategy
```

## `*Pool[T].WithResultCallback`

```go
func (p *Pool[T]) WithResultCallback(fn func(core.Result[T])) *Pool[T]
```

[实现](../../internal/pool/pool.go#L618) · [行为专题](../pool.md)：WithResultCallback 设置结果回调，每个任务完成时同步调用。

**源码说明与使用片段**（原文）：

WithResultCallback 设置结果回调，每个任务完成时同步调用。 回调同步执行，可能并发调用；任务未执行就被丢弃时也可能由提交或关闭池的 goroutine 调用。 回调应尽量轻量，不能同步调用同一池的 Wait、Close 或 CloseAndWait，否则可能死锁。

示例：

	p := pool.NewPool[string](8).WithResultCallback(func(r core.Result[string]) {
	    if r.Ok() {
	        metrics.RecordOk()
	    }
	})


**参数说明**：

- `fn` (`func(core.Result[T])`)：接收已进入队列登记路径的任务结果；nil 清除回调。同步且可能并发，panic 被记录；普通 Submit/TrySubmit 的预检查拒绝或背压直接拒绝不通知此回调。需要每次提交的完整终态时使用 SubmitWithCompletion，并检查提交错误；不得在通知中同步 Wait/Close 本池。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithResultCallback(fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：普通提交在预检查或背压检查中被直接拒绝时，只记录主结果/计数，不调用此回调，也不发往结果流；入队等待失败、关闭清理和实际执行路径才通知。不要据回调次数推断全部提交次数；需要每次提交的终态使用 SubmitWithCompletion。回调可并发，panic 被记录且不改变原结果。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.resultCb = fn
```

## `*Pool[T].WithRingBuffer`

```go
func (p *Pool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *Pool[T]
```

[实现](../../internal/pool/pool.go#L652) · [行为专题](../pool.md)：WithRingBuffer 启用环形缓冲区；它不会单独限制 results 切片的增长。

**源码说明与使用片段**（原文）：

WithRingBuffer 启用环形缓冲区；它不会单独限制 results 切片的增长。 capacity 为缓冲区容量，overflow 为满时策略：

	core.OverflowDrop：覆盖最旧结果
	core.OverflowBlock：返回 false（不阻塞）
	core.OverflowError：返回 false

未限制 results 时，完成的结果同时写入 results 和环形缓冲。 设置正数 WithMaxResults 后，环形缓冲仅保存超出该上限的结果；0 表示无限制。

示例：

	p := pool.NewPool[string](8).WithRingBuffer(10000, core.OverflowDrop)
	// 提交大量任务...
	batch := p.Flush(5000) // 取出一批结果


**参数说明**：

- `capacity` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。
- `overflow` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithRingBuffer(capacity, overflow)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：主结果仍独立保留。MaxResults>0 时只写上限外结果，Wait 追加未取出的溢出项；MaxResults==0 时镜像全部结果，Flush 与 Wait 可能重复取得同一任务，Wait 不排空镜像缓冲。满时 Drop 覆盖最旧项，Block/Error 拒绝新写入而不阻塞。

## `*Pool[T].WithStreaming`

```go
func (p *Pool[T]) WithStreaming(bufSize int) *Pool[T]
```

[实现](../../internal/pool/pool.go#L599) · [行为专题](../pool.md)：WithStreaming 启用流式结果消费，结果通过 channel 实时发送。

**源码说明与使用片段**（原文）：

WithStreaming 启用流式结果消费，结果通过 channel 实时发送。 bufSize 控制 channel 缓冲大小，0 使用 size\*2 的默认值。 调用 StreamResults() 获取只读 channel，在 Wait() 后自动关闭。

示例：

	p := pool.NewPool[string](8).WithStreaming(256)
	stream := p.StreamResults()
	go func() {
	    for r := range stream {
	        if r.Ok() {
	            fmt.Println(r.Value)
	        }
	    }
	}()
	// ... submit tasks ...
	p.Wait() // 关闭 stream channel


**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStreaming(bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：WithStreaming 立即替换通道，非正容量按 Size×2；只在提交前配置一次，旧引用不自动更新/关闭。StreamResults 未启用返回 nil、启用返回同一通道，多消费者竞争；满时直接丢结果并增 StreamDropped，Wait/关闭收尾关通道。普通预检查拒绝不进入流，不能把流当全部尝试审计，最终结果保留仍独立。

## `*Pool[T].WithSubmitTimeout`

```go
func (p *Pool[T]) WithSubmitTimeout(d time.Duration) *Pool[T]
```

[实现](../../internal/pool/pool.go#L556) · [行为专题](../pool.md)：WithSubmitTimeout 设置任务提交超时。超过此时间任务未能入队则丢弃。

**源码说明与使用片段**（原文）：

WithSubmitTimeout 设置任务提交超时。超过此时间任务未能入队则丢弃。

参数：

  - d：提交超时时间

示例：

	p := pool.NewPool[string](4).WithSubmitTimeout(3 * time.Second)


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithSubmitTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：原地保存时长，非正任务超时清除池执行期限，非正提交超时取消显式入队计时器；父 context 仍有效。任务期限从 worker 开始执行计时，不含排队；提交期限包含 MaxPending 等空位及 channel 等待。回调须响应取消，配置不得与提交竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.submitTimeout = d
```

## `*Pool[T].WithTimeout`

```go
func (p *Pool[T]) WithTimeout(d time.Duration) *Pool[T]
```

[实现](../../internal/pool/pool.go#L543) · [行为专题](../pool.md)：WithTimeout 设置每个任务的超时时间。

**源码说明与使用片段**（原文）：

WithTimeout 设置每个任务的超时时间。

参数：

  - d：任务超时时间

示例：

	p := pool.NewPool[string](4).WithTimeout(10 * time.Second)


**参数说明**：

- `d` (`time.Duration`)：每个任务开始执行后的 context 超时；正数生效，非正值清除本实例配置的执行期限，不包含等待入队时间。分片方法对每片应用，回调必须响应 context；在提交前配置。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：原地保存时长，非正任务超时清除池执行期限，非正提交超时取消显式入队计时器；父 context 仍有效。任务期限从 worker 开始执行计时，不含排队；提交期限包含 MaxPending 等空位及 channel 等待。回调须响应取消，配置不得与提交竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.timeout = d
```

## `*Pool[T].WithTraceID`

```go
func (p *Pool[T]) WithTraceID(ctx context.Context) (*Pool[T], context.Context)
```

[实现](../../internal/pool/pool.go#L313) · [行为专题](../pool.md)：WithTraceID 设置池的上下文并注入 trace_id。

**源码说明与使用片段**（原文）：

WithTraceID 设置池的上下文并注入 trace\_id。

参数：

  - ctx：原始上下文

示例：

	p, ctx := pool.NewPool[string](4).WithTraceID(ctx)
	p.Submit(ctx, fn) // 使用带 trace_id 的 ctx 提交


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Pool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.ctx = ctx
```

## `DefaultConfig`

```go
func DefaultConfig() Config
```

[实现](../../internal/pool/pool.go#L219) · [行为专题](../pool.md)：DefaultConfig 返回使用默认 Size（core.IO()）的 Config。

**源码说明与使用片段**（原文）：

DefaultConfig 返回使用默认 Size（core.IO()）的 Config。 MaxResults 默认值为 -1，表示不覆盖 Pool 创建时的全局默认值（初始 100\_000）。 调用链式方法 .WithMaxResults(n) 可显式覆盖（n=0 恢复无限，n>0 设置上限）。


**参数说明**：无显式参数。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
pool.DefaultConfig()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

## `DefaultPool`

```go
func DefaultPool[T any]() *Pool[T]
```

[实现](../../internal/pool/pool.go#L181) · [行为专题](../pool.md)：DefaultPool 使用默认 IO 并发度创建协程池。

**源码说明与使用片段**（原文）：

DefaultPool 使用默认 IO 并发度创建协程池。 等价于 NewPool\[T](core.IO())。

示例：

	p := pool.DefaultPool[string]()
	defer p.Close()


**参数说明**：无显式参数。

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
pool.DefaultPool[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：构造时立即启动 worker，非正 size 按 IO()，队列容量为有效 worker 数的两倍。复制当时的全局任务超时、结果上限和默认环形缓冲设置，不读取全局 SubmitTimeout；调用方最终 Close。不支持复制 Pool 或以零值替代构造器，worker/结果容量须按内存预算设置。

## `ForEachPool`

```go
func ForEachPool[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (*NoResultPool, error)
```

[实现](../../internal/pool/helper.go#L84) · [行为专题](../pool.md)：创建池并逐项执行，等待后关闭；按输入顺序返回首个提交或执行错误，独立于结果保留上限。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) error`)：逐项执行的业务函数；返回 error 或 panic 均计入错误汇总，受 concurrency 限制并发执行，必须响应 ctx 才能及时取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`*NoResultPool, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pool.ForEachPool[T](ctx, items, fn, concurrency)
```

**生产建议**：检查第二返回值；它按输入顺序汇总全部提交和执行的首个错误，不受 MaxResults 限制。回调中使用有截止时间的下游调用。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。同步处理并等待关闭池，返回的池已关闭；按最小输入索引汇总提交/执行业务错误，独立于主结果保留上限，空输入返回 nil 错误。非正并发按 IO()，panic 转为 PanicError；不会 FailFast 或自动回滚，调用方仍须控制 context 和业务幂等。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.ctx = ctx
```

## `MapPool`

```go
func MapPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) (*Pool[R], []core.Result[R], error)
```

[实现](../../internal/pool/helper.go#L64) · [行为专题](../pool.md)：创建池并映射输入，等待后关闭；返回的池已关闭，末尾 error 当前为 nil，业务错误须逐项检查 Result.Err。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) (R, error)`)：并发接收 context 和当前元素，返回该项映射值及错误；panic 转为 PanicError。结果可受 MaxResults 截断；末尾 error 当前不汇总业务错误，须检查保留结果中的 Err。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`*Pool[R], []core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pool.MapPool[T, R](ctx, items, fn, concurrency)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。同步等待映射后关闭池，返回的池已经关闭；末尾 error 当前固定 nil，逐项检查 Result.Err。主结果及环形缓冲保留规则仍生效，不能按返回长度假定全量；非正并发按 IO()，空输入最终仍创建默认大小池再关闭。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.ctx = ctx
```

## `NewBuilder`

```go
func NewBuilder[T any]() *PoolBuilder[T]
```

[实现](../../internal/pool/pool_builder.go#L20) · [行为专题](../pool.md)：NewBuilder 创建协程池构造器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewBuilder 创建协程池构造器，默认使用 context.Background()。 通过 .Context(ctx) 链式设置上下文。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
pool.NewBuilder[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建可变构建器，context 为 Background，cfg 为 DefaultConfig（Size=IO()、MaxResults=-1），此时不启动 worker。MultiBuilder 的片数初始 0，Run 实际一片；通常 Run 新建资源并关闭，MultiBuilder 注入 Pool 后则使用并关闭外部池及其分片。配置不能与 Run 并发，已关闭外部池不可直接重复运行。

## `NewMultiBuilder`

```go
func NewMultiBuilder[T any]() *MultiPoolBuilder[T]
```

[实现](../../internal/pool/multi_pool_builder.go#L22) · [行为专题](../pool.md)：NewMultiBuilder 创建分片协程池构造器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewMultiBuilder 创建分片协程池构造器，默认使用 context.Background()。 通过 .Context(ctx) 链式设置上下文。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
pool.NewMultiBuilder[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建可变构建器，context 为 Background，cfg 为 DefaultConfig（Size=IO()、MaxResults=-1），此时不启动 worker。MultiBuilder 的片数初始 0，Run 实际一片；通常 Run 新建资源并关闭，MultiBuilder 注入 Pool 后则使用并关闭外部池及其分片。配置不能与 Run 并发，已关闭外部池不可直接重复运行。

## `NewPool`

```go
func NewPool[T any](size int) *Pool[T]
```

[实现](../../internal/pool/pool.go#L148) · [行为专题](../pool.md)：NewPool 创建指定大小的泛型协程池，立即启动 worker goroutine。

**源码说明与使用片段**（原文）：

NewPool 创建指定大小的泛型协程池，立即启动 worker goroutine。 size\<=0 时自动使用 IO 并发度（CPU 核数×2）。

参数：

  - size：worker 数量，\<=0 时使用默认 IO 并发度

示例：

	// 创建 8 个 worker 的协程池
	p := pool.NewPool[string](8)
	defer p.Close()

	// 使用 CPU 密集型并发度
	p := pool.NewPool[int](core.CPU())


**参数说明**：

- `size` (`int`)：worker 数量，<=0 时使用默认 IO 并发度

**返回值签名**：`*Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
pool.NewPool[T](size)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：构造时立即启动 worker，非正 size 按 IO()，队列容量为有效 worker 数的两倍。复制当时的全局任务超时、结果上限和默认环形缓冲设置，不读取全局 SubmitTimeout；调用方最终 Close。不支持复制 Pool 或以零值替代构造器，worker/结果容量须按内存预算设置。

## `Submit`

```go
func Submit[T any](ctx context.Context, fn func(context.Context) (T, error)) (*Pool[T], int, error)
```

[实现](../../internal/pool/helper.go#L22) · [行为专题](../pool.md)：创建默认池并提交一个任务，返回池、提交索引和提交错误；调用方须等待并关闭池。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`*Pool[T], int, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pool.Submit[T](ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。立即创建默认 Pool 并提交，返回后池仍开放，调用方必须等待和关闭，包括空输入/零次提交。N 入口 n 必须非负，负数在创建池后分配切片时 panic；SafeN 只额外记录提交错误，不表示业务成功。Batch/N 返回逐项提交状态，末尾 error 当前为 nil；业务结果另行 Wait 并检查，结果可受全局上限截断。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.ctx = ctx
```

## `SubmitBatch`

```go
func SubmitBatch[T any, S ~[]E, E any](ctx context.Context, items S, fn func(context.Context, E) (T, error)) (*Pool[T], []SubmitResult, error)
```

[实现](../../internal/pool/helper.go#L49) · [行为专题](../pool.md)：创建默认池，按输入顺序逐项提交；每项提交错误在 SubmitResult.Err 中，末尾 error 当前为 nil；调用方负责等待和关闭。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`S`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, E) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`*Pool[T], []SubmitResult, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pool.SubmitBatch[T, S, E](ctx, items, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。立即创建默认 Pool 并提交，返回后池仍开放，调用方必须等待和关闭，包括空输入/零次提交。N 入口 n 必须非负，负数在创建池后分配切片时 panic；SafeN 只额外记录提交错误，不表示业务成功。Batch/N 返回逐项提交状态，末尾 error 当前为 nil；业务结果另行 Wait 并检查，结果可受全局上限截断。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.ctx = ctx
```

## `SubmitN`

```go
func SubmitN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult, error)
```

[实现](../../internal/pool/helper.go#L29) · [行为专题](../pool.md)：创建默认池并重复提交同一回调 n 次；n 必须非负，每项提交错误在 SubmitResult.Err 中，末尾 error 当前为 nil；调用方负责等待和关闭。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。
- `n` (`int`)：创建默认池并重复提交同一回调 n 次；n 必须非负，每项提交错误在 SubmitResult.Err 中，末尾 error 当前为 nil；调用方负责等待和关闭。

**返回值签名**：`*Pool[T], []SubmitResult, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pool.SubmitN[T](ctx, fn, n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即创建默认 Pool 并提交，返回后池仍开放，调用方必须等待和关闭，包括空输入/零次提交。N 入口 n 必须非负，负数在创建池后分配切片时 panic；SafeN 只额外记录提交错误，不表示业务成功。Batch/N 返回逐项提交状态，末尾 error 当前为 nil；业务结果另行 Wait 并检查，结果可受全局上限截断。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.ctx = ctx
```

## `SubmitSafeN`

```go
func SubmitSafeN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult)
```

[实现](../../internal/pool/helper.go#L40) · [行为专题](../pool.md)：调用 SubmitN，记录每项提交错误日志并返回提交状态；Safe 不代表任务成功或自动关闭，n 必须非负。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。
- `n` (`int`)：调用 SubmitN，记录每项提交错误日志并返回提交状态；Safe 不代表任务成功或自动关闭，n 必须非负。

**返回值签名**：`*Pool[T], []SubmitResult`。

**使用示例**（已初始化参数的调用片段）：

```go
pool.SubmitSafeN[T](ctx, fn, n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即创建默认 Pool 并提交，返回后池仍开放，调用方必须等待和关闭，包括空输入/零次提交。N 入口 n 必须非负，负数在创建池后分配切片时 panic；SafeN 只额外记录提交错误，不表示业务成功。Batch/N 返回逐项提交状态，末尾 error 当前为 nil；业务结果另行 Wait 并检查，结果可受全局上限截断。

## `WithCfg`

```go
func WithCfg[T any](ctx context.Context, cfg Config, fn func(p *Pool[T]) error) error
```

[实现](../../internal/pool/pool.go#L297) · [行为专题](../pool.md)：WithCfg 使用 Config 创建协程池并执行 fn，fn 返回后自动 Close 释放资源。

**源码说明与使用片段**（原文）：

WithCfg 使用 Config 创建协程池并执行 fn，fn 返回后自动 Close 释放资源。 支持完整 Config 传入或链式设置后传入。

使用方式一：完整传入

	err := pool.WithCfg[string](ctx, pool.Config{
	    Size: 64, Timeout: 5 * time.Second, FailFast: true,
	}, func(p *Pool[string]) error {
	    for _, item := range items {
	        p.Submit(p.Ctx(), func(ctx context.Context) (string, error) {
	            return process(item)
	        })
	    }
	    results := p.Wait()
	    return check(results)
	})

使用方式二：链式设置

	cfg := pool.DefaultConfig().WithSize(64).WithTimeout(5 * time.Second).WithFailFast()
	err := pool.WithCfg[string](ctx, cfg, func(p *Pool[string]) error { ... })


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `cfg` (`Config`)：配置或执行策略；各字段默认值与组合优先级见专题。
- `fn` (`func(p *Pool[T]) error`)：同步接收新建池的作用域函数，返回业务错误；外层在函数返回时关闭池。需取得全部任务结果时在函数内 Wait 并检查逐项 Err，不在函数返回后继续提交。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pool.WithCfg[T](ctx, cfg, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：同步调用非 nil 作用域 fn，返回后 defer Close，fn 的 error 原样返回、panic 直接传播。Close 可能执行排队任务但不替调用方汇总业务错误；需要结果时在 fn 内 Wait 并检查逐项 Err。WithCfg 的 ctx 只在 FailFast 配置时用作池 context，提交仍需明确传入；不要把实例带出作用域继续提交。

## `WithMultiPool`

```go
func WithMultiPool[T any](size int, shards int, fn func(mp *MultiPool[T]) error) error
```

[实现](../../internal/pool/multpool.go#L367) · [行为专题](../pool.md)：创建每片 size 个 worker、共 shards 片的多池，同步调用 fn 并在返回时关闭；需要完成任务时在 fn 内等待。

**参数说明**：

- `size` (`int`)：创建每片 size 个 worker、共 shards 片的多池，同步调用 fn 并在返回时关闭；需要完成任务时在 fn 内等待。
- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `fn` (`func(mp *MultiPool[T]) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pool.WithMultiPool[T](size, shards, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：同步调用非 nil 作用域 fn，返回后 defer Close，fn 的 error 原样返回、panic 直接传播。Close 可能执行排队任务但不替调用方汇总业务错误；需要结果时在 fn 内 Wait 并检查逐项 Err。WithCfg 的 ctx 只在 FailFast 配置时用作池 context，提交仍需明确传入；不要把实例带出作用域继续提交。

## `WithPool`

```go
func WithPool[T any](size int, fn func(p *Pool[T]) error) error
```

[实现](../../internal/pool/pool.go#L270) · [行为专题](../pool.md)：WithPool 创建协程池并执行 fn，fn 返回后自动 Close 释放资源。

**源码说明与使用片段**（原文）：

WithPool 创建协程池并执行 fn，fn 返回后自动 Close 释放资源。 size \<= 0 时使用 core.IO() 作为默认并发度。

示例：

	err := pool.WithPool(64, func(p *Pool[int]) error {
	    for _, item := range items {
	        p.Submit(ctx, func(ctx context.Context) (int, error) {
	            return process(item)
	        })
	    }
	    results := p.Wait()
	    for _, r := range results {
	        if r.Err != nil { return r.Err }
	    }
	    return nil
	})


**参数说明**：

- `size` (`int`)：WithPool 创建协程池并执行 fn，fn 返回后自动 Close 释放资源。
- `fn` (`func(p *Pool[T]) error`)：同步接收新建池的作用域函数，返回业务错误；外层在函数返回时关闭池。需取得全部任务结果时在函数内 Wait 并检查逐项 Err，不在函数返回后继续提交。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pool.WithPool[T](size, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：同步调用非 nil 作用域 fn，返回后 defer Close，fn 的 error 原样返回、panic 直接传播。Close 可能执行排队任务但不替调用方汇总业务错误；需要结果时在 fn 内 Wait 并检查逐项 Err。WithCfg 的 ctx 只在 FailFast 配置时用作池 context，提交仍需明确传入；不要把实例带出作用域继续提交。

## `Config.WithFailFast`

```go
func (c Config) WithFailFast() Config
```

[实现](../../internal/pool/pool.go#L233) · [行为专题](../pool.md)：WithFailFast 启用 FailFast 模式（任一任务失败即取消其余任务）。

**源码说明与使用片段**（原文）：

WithFailFast 启用 FailFast 模式（任一任务失败即取消其余任务）。


**参数说明**：无显式参数。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

## `Config.WithMaxPending`

```go
func (c Config) WithMaxPending(n int) Config
```

[实现](../../internal/pool/pool.go#L236) · [行为专题](../pool.md)：WithMaxPending 设置最大等待任务数（背压控制）。

**源码说明与使用片段**（原文）：

WithMaxPending 设置最大等待任务数（背压控制）。


**参数说明**：

- `n` (`int`)：WithMaxPending 设置最大等待任务数（背压控制）。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithMaxPending(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.MaxPending = n
```

## `Config.WithMaxResults`

```go
func (c Config) WithMaxResults(n int) Config
```

[实现](../../internal/pool/pool.go#L246) · [行为专题](../pool.md)：WithMaxResults 设置结果切片的容量上限（0=无限）。

**源码说明与使用片段**（原文）：

WithMaxResults 设置结果切片的容量上限（0=无限）。


**参数说明**：

- `n` (`int`)：内部创建池时每池主结果的保留上限；0 无限制，正数保留前 n 个索引，负数不覆盖创建池时的全局默认值。MultiPool 按每片独立计数；注入外部池时 MultiPoolBuilder 的此配置不应用，使用外部池原有设置。结果溢出策略需另行配置。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithMaxResults(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.MaxResults = n
```

## `Config.WithOverflow`

```go
func (c Config) WithOverflow(s core.OverflowStrategy) Config
```

[实现](../../internal/pool/pool.go#L239) · [行为专题](../pool.md)：WithOverflow 设置队列溢出策略。

**源码说明与使用片段**（原文）：

WithOverflow 设置队列溢出策略。


**参数说明**：

- `s` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithOverflow(s)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.Overflow = s
```

## `Config.WithRingBuf`

```go
func (c Config) WithRingBuf(capacity int) Config
```

[实现](../../internal/pool/pool.go#L243) · [行为专题](../pool.md)：WithRingBuf 启用环形缓冲区，capacity 为容量。

**源码说明与使用片段**（原文）：

WithRingBuf 启用环形缓冲区，capacity 为容量。 溢出策略使用默认值 core.GetDefaultOverflowStrategy()。


**参数说明**：

- `capacity` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithRingBuf(capacity)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.RingBufCap = capacity
```

## `Config.WithSize`

```go
func (c Config) WithSize(n int) Config
```

[实现](../../internal/pool/pool.go#L224) · [行为专题](../pool.md)：WithSize 设置 worker 数量。

**源码说明与使用片段**（原文）：

WithSize 设置 worker 数量。


**参数说明**：

- `n` (`int`)：WithSize 设置 worker 数量。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithSize(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.Size = n
```

## `Config.WithStreaming`

```go
func (c Config) WithStreaming(bufSize int) Config
```

[实现](../../internal/pool/pool.go#L249) · [行为专题](../pool.md)：WithStreaming 启用流式结果消费，bufSize 为 channel 缓冲大小。

**源码说明与使用片段**（原文）：

WithStreaming 启用流式结果消费，bufSize 为 channel 缓冲大小。


**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStreaming(bufSize)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.Streaming = bufSize
```

## `Config.WithSubmitTimeout`

```go
func (c Config) WithSubmitTimeout(d time.Duration) Config
```

[实现](../../internal/pool/pool.go#L230) · [行为专题](../pool.md)：WithSubmitTimeout 设置任务提交超时时间。

**源码说明与使用片段**（原文）：

WithSubmitTimeout 设置任务提交超时时间。


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithSubmitTimeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.SubmitTimeout = d
```

## `Config.WithTimeout`

```go
func (c Config) WithTimeout(d time.Duration) Config
```

[实现](../../internal/pool/pool.go#L227) · [行为专题](../pool.md)：WithTimeout 设置每个任务的超时时间。

**源码说明与使用片段**（原文）：

WithTimeout 设置每个任务的超时时间。


**参数说明**：

- `d` (`time.Duration`)：创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.Timeout = d
```

## `PoolStats.String`

```go
func (s PoolStats) String() string
```

[实现](../../internal/pool/pool.go#L2266) · [行为专题](../pool.md)：返回统计快照的格式化字符串。

**参数说明**：无显式参数。

**返回值签名**：`string`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.String()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅格式化已经保存的统计字段，不重新采样池、不等待任务；快照不是事务一致状态。不能从字符串或计数字段差值推断关闭/复位完成，需要对应生命周期方法等待。

