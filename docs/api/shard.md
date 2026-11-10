# shard 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [shard](../shard.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*ShardPoolBuilder[T].Config`

```go
func (b *ShardPoolBuilder[T]) Config(fn func(ShardPoolConfig[T]) ShardPoolConfig[T]) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L75) · [行为专题](../shard.md)：Config 函数式配置，允许通过闭包修改 ShardPoolConfig

**源码说明与使用片段**（原文）：

Config 函数式配置，允许通过闭包修改 ShardPoolConfig


**参数说明**：

- `fn` (`func(ShardPoolConfig[T]) ShardPoolConfig[T]`)：接收当前配置副本并返回需要应用的配置；必须返回修改后的副本，调用会同步执行。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Config(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：同步用当前配置值调用非 nil fn，并以返回值整体替换；回调 panic 直接传播。零配置在创建时使用 4 片、每片 IO()，不会重置构建器 context。ShardPool 的资源/上限按每片计算，闭包中应明确设置容量及溢出策略。

## `*ShardPoolBuilder[T].Context`

```go
func (b *ShardPoolBuilder[T]) Context(ctx context.Context) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L27) · [行为专题](../shard.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存并确保非 nil context 的 trace_id，只影响后续 Run；传给作用域回调后仍需用于提交。Pool 的 FailFast 模式派生跨片取消 context；ShardedGroupBuilder 不自动为各片绑定此 context，任务 context 仍由每次 Go 传入。配置不与 Run 并发。

## `*ShardPoolBuilder[T].DefaultDistribution`

```go
func (b *ShardPoolBuilder[T]) DefaultDistribution() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L51) · [行为专题](../shard.md)：DefaultDistribution 使用默认分发策略（RoundRobin）。

**源码说明与使用片段**（原文）：

DefaultDistribution 使用默认分发策略（RoundRobin）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultDistribution()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：RoundRobin 从第 0 片轮询；Hash 的 SubmitBatch/TrySubmitBatch 使用非 nil KeyFn(item) 对片数取模，KeyFn 在提交方同步执行，panic 直接传播。普通 Submit 无元素，Hash 路径固定第 0 片；Hash 缺少 KeyFn 及未知策略也落第 0 片。SubmitKeyed 独立按字符串 FNV-1a 路由，不使用此函数，同片不保证串行。

## `*ShardPoolBuilder[T].DefaultFailFast`

```go
func (b *ShardPoolBuilder[T]) DefaultFailFast() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L63) · [行为专题](../shard.md)：DefaultFailFast 关闭快速失败模式（默认关闭）。

**源码说明与使用片段**（原文）：

DefaultFailFast 关闭快速失败模式（默认关闭）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultFailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只保存后续 Run 使用的配置，任务 Timeout 和 MaxPending 仅正数覆盖底层默认；DefaultTimeout 清零覆盖以继承全局任务期限，DefaultMaxPending 清零则新池不设排队上限。FailFast 使各片共享失败取消链，仍须让任务响应 context；配置不能与执行竞争。

## `*ShardPoolBuilder[T].DefaultLogger`

```go
func (b *ShardPoolBuilder[T]) DefaultLogger() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L139) · [行为专题](../shard.md)：DefaultLogger 重置为默认日志实现。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*ShardPoolBuilder[T].DefaultMaxPending`

```go
func (b *ShardPoolBuilder[T]) DefaultMaxPending() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L69) · [行为专题](../shard.md)：DefaultMaxPending 使用默认最大待处理数（0=无限制）。

**源码说明与使用片段**（原文）：

DefaultMaxPending 使用默认最大待处理数（0=无限制）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxPending()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只保存后续 Run 使用的配置，任务 Timeout 和 MaxPending 仅正数覆盖底层默认；DefaultTimeout 清零覆盖以继承全局任务期限，DefaultMaxPending 清零则新池不设排队上限。FailFast 使各片共享失败取消链，仍须让任务响应 context；配置不能与执行竞争。

## `*ShardPoolBuilder[T].DefaultMaxResults`

```go
func (b *ShardPoolBuilder[T]) DefaultMaxResults() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L130) · [行为专题](../shard.md)：DefaultMaxResults 使用默认最大结果数（无限）

**源码说明与使用片段**（原文）：

DefaultMaxResults 使用默认最大结果数（无限）


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMaxResults()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每片独立保留主结果；正数限制至索引前 n 项，非正数在构造时统一调用 WithMaxResults(0)，明确无限制。DefaultMaxResults 虽保存 -1，仍为无限，不继承 Pool 的全局上限。上限外错误仍计入失败统计，完整业务错误应以回调独立收集。

## `*ShardPoolBuilder[T].DefaultShardPoolConfig`

```go
func (b *ShardPoolBuilder[T]) DefaultShardPoolConfig() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L33) · [行为专题](../shard.md)：DefaultShardPoolConfig 重置为默认配置

**源码说明与使用片段**（原文）：

DefaultShardPoolConfig 重置为默认配置


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShardPoolConfig()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只重置 cfg，不重置已保存的 context，也不修改已运行实例。后续构造使用 4 片、每片 IO()；Pool 的任务超时和环形缓冲仍继承创建时全局默认值，但主结果上限明确设为无限。

## `*ShardPoolBuilder[T].DefaultShards`

```go
func (b *ShardPoolBuilder[T]) DefaultShards() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L39) · [行为专题](../shard.md)：DefaultShards 使用默认分片数（4）。

**源码说明与使用片段**（原文）：

DefaultShards 使用默认分片数（4）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShards()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。

## `*ShardPoolBuilder[T].DefaultTimeout`

```go
func (b *ShardPoolBuilder[T]) DefaultTimeout() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L57) · [行为专题](../shard.md)：DefaultTimeout 清除分片配置中的任务超时覆盖，保留创建各池时的全局默认任务超时。

**源码说明与使用片段**（原文）：

DefaultTimeout 清除分片配置中的任务超时覆盖，保留创建各池时的全局默认任务超时。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只保存后续 Run 使用的配置，任务 Timeout 和 MaxPending 仅正数覆盖底层默认；DefaultTimeout 清零覆盖以继承全局任务期限，DefaultMaxPending 清零则新池不设排队上限。FailFast 使各片共享失败取消链，仍须让任务响应 context；配置不能与执行竞争。

## `*ShardPoolBuilder[T].DefaultWorker`

```go
func (b *ShardPoolBuilder[T]) DefaultWorker() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L45) · [行为专题](../shard.md)：DefaultWorker 清除每片 worker 覆盖，创建时使用 core.IO()（NumCPU×2）。

**源码说明与使用片段**（原文）：

DefaultWorker 清除每片 worker 覆盖，创建时使用 core.IO()（NumCPU×2）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。

## `*ShardPoolBuilder[T].Distribution`

```go
func (b *ShardPoolBuilder[T]) Distribution(d Distribution) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L93) · [行为专题](../shard.md)：Distribution 设置分发策略

**源码说明与使用片段**（原文）：

Distribution 设置分发策略


**参数说明**：

- `d` (`Distribution`)：Distribution 设置分发策略

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Distribution(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：RoundRobin 从第 0 片轮询；Hash 的 SubmitBatch/TrySubmitBatch 使用非 nil KeyFn(item) 对片数取模，KeyFn 在提交方同步执行，panic 直接传播。普通 Submit 无元素，Hash 路径固定第 0 片；Hash 缺少 KeyFn 及未知策略也落第 0 片。SubmitKeyed 独立按字符串 FNV-1a 路由，不使用此函数，同片不保证串行。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Distribution = d
```

## `*ShardPoolBuilder[T].FailFast`

```go
func (b *ShardPoolBuilder[T]) FailFast() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L112) · [行为专题](../shard.md)：FailFast 启用快速失败模式

**源码说明与使用片段**（原文）：

FailFast 启用快速失败模式


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只保存后续 Run 使用的配置，任务 Timeout 和 MaxPending 仅正数覆盖底层默认；DefaultTimeout 清零覆盖以继承全局任务期限，DefaultMaxPending 清零则新池不设排队上限。FailFast 使各片共享失败取消链，仍须让任务响应 context；配置不能与执行竞争。

## `*ShardPoolBuilder[T].KeyFn`

```go
func (b *ShardPoolBuilder[T]) KeyFn(fn func(T) uint64) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L100) · [行为专题](../shard.md)：KeyFn 设置 Hash 模式下批量提交的元素路由函数。

**源码说明与使用片段**（原文）：

KeyFn 设置 Hash 模式下批量提交的元素路由函数。 Submit 不接收元素，Hash 模式下使用第 0 片；单项路由可使用 SubmitKeyed。


**参数说明**：

- `fn` (`func(T) uint64`)：元素路由回调：接收业务元素，返回稳定的 uint64 哈希键；可被不同批次并发调用，nil 表示使用配置的默认路由。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.KeyFn(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：RoundRobin 从第 0 片轮询；Hash 的 SubmitBatch/TrySubmitBatch 使用非 nil KeyFn(item) 对片数取模，KeyFn 在提交方同步执行，panic 直接传播。普通 Submit 无元素，Hash 路径固定第 0 片；Hash 缺少 KeyFn 及未知策略也落第 0 片。SubmitKeyed 独立按字符串 FNV-1a 路由，不使用此函数，同片不保证串行。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.KeyFn = fn
```

## `*ShardPoolBuilder[T].Logger`

```go
func (b *ShardPoolBuilder[T]) Logger(l core.Logger) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L136) · [行为专题](../shard.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*ShardPoolBuilder[T].MaxPending`

```go
func (b *ShardPoolBuilder[T]) MaxPending(n int) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L118) · [行为专题](../shard.md)：MaxPending 设置最大待处理数

**源码说明与使用片段**（原文）：

MaxPending 设置最大待处理数


**参数说明**：

- `n` (`int`)：MaxPending 设置最大待处理数

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxPending(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只保存后续 Run 使用的配置，任务 Timeout 和 MaxPending 仅正数覆盖底层默认；DefaultTimeout 清零覆盖以继承全局任务期限，DefaultMaxPending 清零则新池不设排队上限。FailFast 使各片共享失败取消链，仍须让任务响应 context；配置不能与执行竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.PoolCfg.MaxPending = n
```

## `*ShardPoolBuilder[T].MaxResults`

```go
func (b *ShardPoolBuilder[T]) MaxResults(n int) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L124) · [行为专题](../shard.md)：MaxResults 设置结果存储上限（0=无限）

**源码说明与使用片段**（原文）：

MaxResults 设置结果存储上限（0=无限）


**参数说明**：

- `n` (`int`)：每片主结果的保留上限；正数限制结果，非正数在创建分片时均应用为无限制，不继承 Pool 的全局上限。结果溢出路径须另行配置和消费。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxResults(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每片独立保留主结果；正数限制至索引前 n 项，非正数在构造时统一调用 WithMaxResults(0)，明确无限制。DefaultMaxResults 虽保存 -1，仍为无限，不继承 Pool 的全局上限。上限外错误仍计入失败统计，完整业务错误应以回调独立收集。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.PoolCfg.MaxResults = n
```

## `*ShardPoolBuilder[T].Run`

```go
func (b *ShardPoolBuilder[T]) Run(fn func(ctx context.Context, sp *ShardedPool[T]) error) error
```

[实现](../../internal/shard/shard_builder.go#L143) · [行为专题](../shard.md)：Run 终端方法：创建分片池，执行 fn，fn 返回后自动 Close 所有分片。

**源码说明与使用片段**（原文）：

Run 终端方法：创建分片池，执行 fn，fn 返回后自动 Close 所有分片。 FailFast 时 fn 接收跨片共享 context；各片正常 Wait 不提前取消它，整体收尾后释放。


**参数说明**：

- `fn` (`func(ctx context.Context, sp *ShardedPool[T]) error`)：同步接收本次执行器和上下文（以签名为准）的作用域函数；其返回后触发关闭，返回的 error 向外传播。需处理全部任务时在函数内等待并检查结果，避免把执行器带出作用域继续使用。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：创建分片池、同步调用 fn，退出时关闭所有片；只返回 fn 的错误，不自动汇总业务错误。启用 FailFast 时传给 fn 的是跨片共享 context，正常完成某一片不会取消它，所有片 Wait/Close 完成后释放。未启用时传保存的父 context。应显式处理提交错误并等待业务结果，不能以收尾后 ctx.Err() 判断业务失败；panic 直接传播但仍清理。

## `*ShardPoolBuilder[T].Shards`

```go
func (b *ShardPoolBuilder[T]) Shards(n int) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L81) · [行为专题](../shard.md)：Shards 设置分片数

**源码说明与使用片段**（原文）：

Shards 设置分片数


**参数说明**：

- `n` (`int`)：Shards 设置分片数

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Shards = n
```

## `*ShardPoolBuilder[T].Timeout`

```go
func (b *ShardPoolBuilder[T]) Timeout(d time.Duration) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L106) · [行为专题](../shard.md)：Timeout 设置任务超时

**源码说明与使用片段**（原文）：

Timeout 设置任务超时


**参数说明**：

- `d` (`time.Duration`)：创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只保存后续 Run 使用的配置，任务 Timeout 和 MaxPending 仅正数覆盖底层默认；DefaultTimeout 清零覆盖以继承全局任务期限，DefaultMaxPending 清零则新池不设排队上限。FailFast 使各片共享失败取消链，仍须让任务响应 context；配置不能与执行竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.PoolCfg.Timeout = d
```

## `*ShardPoolBuilder[T].Worker`

```go
func (b *ShardPoolBuilder[T]) Worker(n int) *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L87) · [行为专题](../shard.md)：Worker 设置每个分片的 worker 数量

**源码说明与使用片段**（原文）：

Worker 设置每个分片的 worker 数量


**参数说明**：

- `n` (`int`)：Worker 设置每个分片的 worker 数量

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.SizePerShard = n
```

## `*ShardedGroupBuilder[T].Config`

```go
func (b *ShardedGroupBuilder[T]) Config(fn func(ShardGroupConfig[T]) ShardGroupConfig[T]) *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L57) · [行为专题](../shard.md)：Config 函数式配置

**源码说明与使用片段**（原文）：

Config 函数式配置


**参数说明**：

- `fn` (`func(ShardGroupConfig[T]) ShardGroupConfig[T]`)：接收当前配置副本并返回需要应用的配置；必须返回修改后的副本，调用会同步执行。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Config(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：同步用当前配置值调用非 nil fn，并以返回值整体替换；回调 panic 直接传播。零配置在创建时使用 4 片、每片 IO()，不会重置构建器 context。ShardPool 的资源/上限按每片计算，闭包中应明确设置容量及溢出策略。

## `*ShardedGroupBuilder[T].Context`

```go
func (b *ShardedGroupBuilder[T]) Context(ctx context.Context) *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L27) · [行为专题](../shard.md)：Context 链式设置上下文

**源码说明与使用片段**（原文）：

Context 链式设置上下文


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存并确保非 nil context 的 trace_id，只影响后续 Run；传给作用域回调后仍需用于提交。Pool 的 FailFast 模式派生跨片取消 context；ShardedGroupBuilder 不自动为各片绑定此 context，任务 context 仍由每次 Go 传入。配置不与 Run 并发。

## `*ShardedGroupBuilder[T].DefaultDistribution`

```go
func (b *ShardedGroupBuilder[T]) DefaultDistribution() *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L51) · [行为专题](../shard.md)：DefaultDistribution 使用默认分发策略（RoundRobin）。

**源码说明与使用片段**（原文）：

DefaultDistribution 使用默认分发策略（RoundRobin）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultDistribution()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存配置字段；ShardedGroup 的普通 Go/GoBatch 实际始终轮询，不读取 Distribution，即使设置 Hash。按键亲和须使用 GoKeyed(string)，它按 FNV-1a 路由；同片允许并发，不保证同 key 串行。

## `*ShardedGroupBuilder[T].DefaultLogger`

```go
func (b *ShardedGroupBuilder[T]) DefaultLogger() *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L87) · [行为专题](../shard.md)：DefaultLogger 重置为默认日志实现。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*ShardedGroupBuilder[T].DefaultShardedGroupConfig`

```go
func (b *ShardedGroupBuilder[T]) DefaultShardedGroupConfig() *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L33) · [行为专题](../shard.md)：DefaultShardedGroupConfig 重置为默认配置

**源码说明与使用片段**（原文）：

DefaultShardedGroupConfig 重置为默认配置


**参数说明**：无显式参数。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShardedGroupConfig()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只重置 cfg，不重置已保存的 context，也不修改已运行实例。后续构造使用 4 片、每片 IO()；Pool 的任务超时和环形缓冲仍继承创建时全局默认值，但主结果上限明确设为无限。

## `*ShardedGroupBuilder[T].DefaultShards`

```go
func (b *ShardedGroupBuilder[T]) DefaultShards() *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L39) · [行为专题](../shard.md)：DefaultShards 使用默认分片数（4）。

**源码说明与使用片段**（原文）：

DefaultShards 使用默认分片数（4）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShards()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。

## `*ShardedGroupBuilder[T].DefaultWorker`

```go
func (b *ShardedGroupBuilder[T]) DefaultWorker() *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L45) · [行为专题](../shard.md)：DefaultWorker 清除每片并发度覆盖，创建时使用 core.IO()（NumCPU×2）。

**源码说明与使用片段**（原文）：

DefaultWorker 清除每片并发度覆盖，创建时使用 core.IO()（NumCPU×2）。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。

## `*ShardedGroupBuilder[T].Distribution`

```go
func (b *ShardedGroupBuilder[T]) Distribution(d Distribution) *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L75) · [行为专题](../shard.md)：Distribution 设置分发策略

**源码说明与使用片段**（原文）：

Distribution 设置分发策略


**参数说明**：

- `d` (`Distribution`)：保存的分发策略值；当前 Group 的 Go/GoBatch 始终轮转，GoKeyed 始终按字符串哈希，均不读取此字段。需要亲和路由时显式调用 GoKeyed。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Distribution(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存配置字段；ShardedGroup 的普通 Go/GoBatch 实际始终轮询，不读取 Distribution，即使设置 Hash。按键亲和须使用 GoKeyed(string)，它按 FNV-1a 路由；同片允许并发，不保证同 key 串行。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Distribution = d
```

## `*ShardedGroupBuilder[T].Logger`

```go
func (b *ShardedGroupBuilder[T]) Logger(l core.Logger) *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L81) · [行为专题](../shard.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*ShardedGroupBuilder[T].Run`

```go
func (b *ShardedGroupBuilder[T]) Run(fn func(ctx context.Context, sg *ShardedGroup[T]) error) error
```

[实现](../../internal/shard/sharded_group_builder.go#L94) · [行为专题](../shard.md)：Run 终端方法：创建 ShardedGroup → 执行 fn → 关闭所有分片。

**源码说明与使用片段**（原文）：

Run 终端方法：创建 ShardedGroup → 执行 fn → 关闭所有分片。 调用方应在 fn 内调用 sg.Wait() / sg.WaitTimeout() / sg.WaitContext() 等待结果。


**参数说明**：

- `fn` (`func(ctx context.Context, sg *ShardedGroup[T]) error`)：同步接收本次执行器和上下文（以签名为准）的作用域函数；其返回后触发关闭，返回的 error 向外传播。需处理全部任务时在函数内等待并检查结果，避免把执行器带出作用域继续使用。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：创建分片 Group、同步调用 fn，退出时关闭所有片；传给 fn 的是保存的父 context，不自动启用跨片 FailFast。需要快速失败时在 fn 中调用 WithFailFast 并传递其返回 context；最终等待后检查业务结果并返回错误，Run 不自动汇总。panic 直接传播但仍清理资源。

## `*ShardedGroupBuilder[T].Shards`

```go
func (b *ShardedGroupBuilder[T]) Shards(n int) *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L63) · [行为专题](../shard.md)：Shards 设置分片数

**源码说明与使用片段**（原文）：

Shards 设置分片数


**参数说明**：

- `n` (`int`)：Shards 设置分片数

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.Shards = n
```

## `*ShardedGroupBuilder[T].Worker`

```go
func (b *ShardedGroupBuilder[T]) Worker(n int) *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L69) · [行为专题](../shard.md)：Worker 设置每分片并发度

**源码说明与使用片段**（原文）：

Worker 设置每分片并发度


**参数说明**：

- `n` (`int`)：Worker 设置每分片并发度

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.cfg.ConcurrencyPerShard = n
```

## `*ShardedGroup[T].Close`

```go
func (sg *ShardedGroup[T]) Close()
```

[实现](../../internal/shard/shardgroup.go#L192) · [行为专题](../shard.md)：Close 关闭所有分片 Group，等待所有已提交任务完成并清理资源。

**源码说明与使用片段**（原文）：

Close 关闭所有分片 Group，等待所有已提交任务完成并清理资源。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：逐片关闭并等待已登记任务及资源清理，全部结束后释放共享 FailFast context（包括重复配置产生的旧派生 context）；父 context 不被取消。不是先广播共享取消再关闭，忽略取消且不返回的任务可能使关闭阻塞。在所属任务或结果回调中同步 Close 会自等待；统一由所有者关闭，不同时对 GetShard 返回的片执行生命周期操作。

## `*ShardedGroup[T].GetShard`

```go
func (sg *ShardedGroup[T]) GetShard(idx int) *group.Group[T]
```

[实现](../../internal/shard/shardgroup.go#L69) · [行为专题](../shard.md)：GetShard 获取指定分片的 Group 实例。

**源码说明与使用片段**（原文）：

GetShard 获取指定分片的 Group 实例。 idx 越界返回 nil。


**参数说明**：

- `idx` (`int`)：目标分片的零起始索引；负值或不小于分片数时返回 nil。

**返回值签名**：`*group.Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShard(idx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：返回内部真实执行器指针，越界返回 nil，不复制也不转移单片生命周期。对该片设置会影响后续路由到它的任务；统一在提交前完成配置，避免同整体 Reset/Wait/Close 并发操作。

## `*ShardedGroup[T].Go`

```go
func (sg *ShardedGroup[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/shard/shardgroup.go#L77) · [行为专题](../shard.md)：Go 分发任务到某个分片执行。

**源码说明与使用片段**（原文）：

Go 分发任务到某个分片执行。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Go(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。只提交所选片，不搜索其他空闲片；Pool 普通提交按 Distribution 选片，Hash 无元素时固定第 0 片，Group 普通 Go 始终轮询。同片仍可并发执行，error 只表示提交状态，Drop 还可能返回 nil 却丢弃任务；最终等待检查逐项 Err。Try 不等待队列空位，满队列返回 ErrSubmitTimeout。

## `*ShardedGroup[T].GoAt`

```go
func (sg *ShardedGroup[T]) GoAt(shardIdx, index int, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/shard/shardgroup.go#L83) · [行为专题](../shard.md)：GoAt 将任务提交到指定分片的指定索引位置。

**源码说明与使用片段**（原文）：

GoAt 将任务提交到指定分片的指定索引位置。


**参数说明**：

- `shardIdx` (`int`)：目标片的零起始索引；越界时按单项默认路由选片：Pool 的 RoundRobin 轮转、Hash 或未知策略选第 0 片，Group 轮转。结果按分片合并，该值不表示合并后结果索引。
- `index` (`int`)：目标分片内部的结果索引；负值被拒绝，Pool 另外限制为不大于 core.MaxPoolIndex。稳定排序需在片内使用唯一连续索引；各片索引不是全局索引。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoAt(shardIdx, index, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。shardIdx 越界时按普通分发规则选片，index 是该片内部的结果索引。不同任务应使用唯一连续索引：Pool 重复索引可能覆盖结果，Group 会把旧结果移至其他槽位，均不能用来替换任务。大索引可分配大量内存，负索引返回提交错误；Pool 还检查 MaxPoolIndex。返回 nil 仍须等待业务完成并检查结果。

## `*ShardedGroup[T].GoBatch`

```go
func (sg *ShardedGroup[T]) GoBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]GoBatchResult, error)
```

[实现](../../internal/shard/shardgroup.go#L106) · [行为专题](../shard.md)：GoBatch 按轮询顺序将元素分发到各分片，并返回每项的提交结果。

**源码说明与使用片段**（原文）：

GoBatch 按轮询顺序将元素分发到各分片，并返回每项的提交结果。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`[]GoBatchResult, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoBatch(ctx, items, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐项尝试全部输入，首个提交错误不停止后续项；返回每项原输入索引、片号、提交错误，Index 不是片内结果索引，第二返回值仅为首个提交错误。Pool Hash 使用 KeyFn；Group 始终轮询。选片不检查该片是否已关闭，也不搜索其他可用片，拒绝仍推进普通轮询。TrySubmitBatch 对关闭/已等待/正在等待片的状态拒绝不保存主结果或增加计数，必须检查批量返回项；SubmitBatch/GoBatch 的状态拒绝仍记录。回调及结果仍异步完成，Go/Submit 的 nil 不保证业务成功；Drop 策略还可能丢弃任务。

## `*ShardedGroup[T].GoKeyed`

```go
func (sg *ShardedGroup[T]) GoKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/shard/shardgroup.go#L91) · [行为专题](../shard.md)：GoKeyed 按 key 哈希分发到固定分片。

**源码说明与使用片段**（原文）：

GoKeyed 按 key 哈希分发到固定分片。


**参数说明**：

- `key` (`string`)：键或路由标识；类型及匹配规则由接收者决定。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoKeyed(key, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。按字符串 key 的 FNV-1a 32 位值对片数取模，不读取 Distribution/KeyFn。相同 key 路由到同片，但该片多 worker 时可并发、完成顺序不保证；热点不会自动迁移到空闲片。返回的是提交错误，仍须等待并检查业务结果。

## `*ShardedGroup[T].Reset`

```go
func (sg *ShardedGroup[T]) Reset() error
```

[实现](../../internal/shard/shardgroup.go#L203) · [行为专题](../shard.md)：Reset 重置所有分片（需在 Wait 后无活跃任务时调用）。

**源码说明与使用片段**（原文）：

Reset 重置所有分片（需在 Wait 后无活跃任务时调用）。 Reset 会清除 FailFast 配置；复用时需重新调用 WithFailFast 并使用其返回的 context。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reset()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片复位，遇错误立即返回，之前已复位的片不会回滚，因此必须预先协调全部分片已完成等待且无任务/配置竞争。Pool 保留配置并重建失败取消链；Group 清除 context/FailFast/AutoScale，需重新设置。复位后重新取得各片 context 和流通道，不使用旧引用提交新批次。

## `*ShardedGroup[T].ShardCount`

```go
func (sg *ShardedGroup[T]) ShardCount() int
```

[实现](../../internal/shard/shardgroup.go#L63) · [行为专题](../shard.md)：ShardCount 返回分片数。

**源码说明与使用片段**（原文）：

ShardCount 返回分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：返回已创建分片数量，不查询存活 worker 或空闲容量；所有片关闭后片数仍保持。零值可返回 0，但没有片的提交入口不能使用，必须通过构造器初始化；不要依据片数推断资源可用。

## `*ShardedGroup[T].StreamResults`

```go
func (sg *ShardedGroup[T]) StreamResults() <-chan core.Result[T]
```

[实现](../../internal/shard/shardgroup.go#L273) · [行为专题](../shard.md)：StreamResults 返回合并所有分片流式结果的只读 channel。

**源码说明与使用片段**（原文）：

StreamResults 返回合并所有分片流式结果的只读 channel。 每个分片的流式结果被 fan-in 到此 channel，所有分片 channel 关闭后自动关闭此 channel。


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StreamResults()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：每次新建合并通道和转发 goroutine，跳过未启用的各片流；所有转发完成后关闭合并通道。只调用一次并持续消费，重复调用会竞争分片结果，停止消费可能阻塞转发 goroutine。单片流仍可能丢结果，完整性应另核对 Wait/计数及上限策略。

## `*ShardedGroup[T].TotalActive`

```go
func (sg *ShardedGroup[T]) TotalActive() int
```

[实现](../../internal/shard/shardgroup.go#L316) · [行为专题](../shard.md)：TotalActive 汇总所有分片活跃任务数。

**源码说明与使用片段**（原文）：

TotalActive 汇总所有分片活跃任务数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalActive()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedGroup[T].TotalBusy`

```go
func (sg *ShardedGroup[T]) TotalBusy() int
```

[实现](../../internal/shard/shardgroup.go#L325) · [行为专题](../shard.md)：TotalBusy 汇总所有分片忙碌任务数。

**源码说明与使用片段**（原文）：

TotalBusy 汇总所有分片忙碌任务数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalBusy()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedGroup[T].TotalFailCount`

```go
func (sg *ShardedGroup[T]) TotalFailCount() int64
```

[实现](../../internal/shard/shardgroup.go#L298) · [行为专题](../shard.md)：TotalFailCount 汇总所有分片失败数。

**源码说明与使用片段**（原文）：

TotalFailCount 汇总所有分片失败数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalFailCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedGroup[T].TotalOkCount`

```go
func (sg *ShardedGroup[T]) TotalOkCount() int64
```

[实现](../../internal/shard/shardgroup.go#L307) · [行为专题](../shard.md)：TotalOkCount 汇总所有分片成功数。

**源码说明与使用片段**（原文）：

TotalOkCount 汇总所有分片成功数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalOkCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedGroup[T].TotalWorker`

```go
func (sg *ShardedGroup[T]) TotalWorker() int
```

[实现](../../internal/shard/shardgroup.go#L334) · [行为专题](../shard.md)：TotalWorker 汇总所有分片并发度之和。

**源码说明与使用片段**（原文）：

TotalWorker 汇总所有分片并发度之和。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalWorker()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedGroup[T].Wait`

```go
func (sg *ShardedGroup[T]) Wait() []core.Result[T]
```

[实现](../../internal/shard/shardgroup.go#L124) · [行为专题](../shard.md)：Wait 等待所有分片完成，合并结果。

**源码说明与使用片段**（原文）：

Wait 等待所有分片完成，合并结果。 全部完成后释放共享 FailFast context，取消状态不代表业务失败。


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：逐片等待并按片号拼接结果，不保持全局提交顺序；全部完成后释放整体 WithFailFast 共享 context，不能凭收尾后的 ctx.Err() 判断业务失败。单片正常等待只取消该片子 context，跨片任务应使用整体 WithFailFast 返回值或 Pool Builder 的 Run context。Pool 的等待入口每批一次，重复/并发 Wait 返回 nil；Group 可再次读取最终结果。WaitAndClose 还关闭 Pool worker，其他 Pool 等待须另行 Close。复用须先成功 Reset；回调不返回时等待可能阻塞。

## `*ShardedGroup[T].WaitContext`

```go
func (sg *ShardedGroup[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool)
```

[实现](../../internal/shard/shardgroup.go#L147) · [行为专题](../shard.md)：WaitContext 等待所有分片完成或 ctx 取消，所有分片并行等待共享同一个 ctx。

**源码说明与使用片段**（原文）：

WaitContext 等待所有分片完成或 ctx 取消，所有分片并行等待共享同一个 ctx。 全部完成时释放共享 FailFast context；未完成返回后须再 Wait/Close 完成收尾。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitContext(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：各 Group 分片并行等待共享 context/截止时间，按片号拼接独立槽位快照，含稀疏索引空槽；bool 表示全部等待完成，不表示业务成功。业务结果已保存但同步通知阻塞时仍可返回 false，成功值不因等待取消改写；不得据结果长度或全部 Err 为 nil 判定可复用。全部完成时释放 WithFailFast 创建的共享 context，ctx.Err() 因此不能用作业务失败标志；返回 false 时不额外取消共享链，各片已登记任务仍接收等待取消，共享 context 可能仍有效，后台收尾可继续。最终再 Wait/Close 等待通知和业务结束、取得最终结果并释放共享链，随后再 Reset；零片返回 nil,true。

## `*ShardedGroup[T].WaitTimeout`

```go
func (sg *ShardedGroup[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool)
```

[实现](../../internal/shard/shardgroup.go#L136) · [行为专题](../shard.md)：WaitTimeout 等待所有分片完成或超时，所有分片并行等待共享同一个超时时钟。

**源码说明与使用片段**（原文）：

WaitTimeout 等待所有分片完成或超时，所有分片并行等待共享同一个超时时钟。


**参数说明**：

- `d` (`time.Duration`)：本次等待的最长时长；非正值使等待 context 立即过期，完成与超时同时就绪时不保证 bool 的选择。超时返回部分结果并通知取消，不能强制停止任务；分片共享同一截止时间，复用须遵守该实例的等待/复位约定。

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitTimeout(d)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：各 Group 分片并行等待共享 context/截止时间，按片号拼接独立槽位快照，含稀疏索引空槽；bool 表示全部等待完成，不表示业务成功。业务结果已保存但同步通知阻塞时仍可返回 false，成功值不因等待取消改写；不得据结果长度或全部 Err 为 nil 判定可复用。全部完成时释放 WithFailFast 创建的共享 context，ctx.Err() 因此不能用作业务失败标志；返回 false 时不额外取消共享链，各片已登记任务仍接收等待取消，共享 context 可能仍有效，后台收尾可继续。最终再 Wait/Close 等待通知和业务结束、取得最终结果并释放共享链，随后再 Reset；零片返回 nil,true。

## `*ShardedGroup[T].WithFFCtx`

```go
func (sg *ShardedGroup[T]) WithFFCtx(ctx context.Context) (*ShardedGroup[T], context.Context)
```

[实现](../../internal/shard/shardgroup.go#L251) · [行为专题](../shard.md)：WithFFCtx 等价于 WithFailFast，缩写形式。

**源码说明与使用片段**（原文）：

WithFFCtx 等价于 WithFailFast，缩写形式。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedGroup[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFCtx(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：在各片之间建立共享失败取消链，父 context 须非 nil；使用返回的共享 context 提交，单片正常等待不取消其他片。业务失败或父取消可请求退出；所有片整体 Wait/Close 完成也取消共享 context，因此收尾后 Err 非 nil 不证明业务失败。重复配置产生的旧派生 context 在整体收尾一起释放；配置须在提交前完成，不能与等待/复位竞争。Pool Reset 保留并重建共享链，Group Reset 清除、须重新启用。

## `*ShardedGroup[T].WithFailFast`

```go
func (sg *ShardedGroup[T]) WithFailFast(ctx context.Context) (*ShardedGroup[T], context.Context)
```

[实现](../../internal/shard/shardgroup.go#L236) · [行为专题](../shard.md)：WithFailFast 为所有分片启用 FailFast 模式。

**源码说明与使用片段**（原文）：

WithFailFast 为所有分片启用 FailFast 模式。 返回的共享 context 在整体等待或关闭完成后取消，父 context 不受影响。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedGroup[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFailFast(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：在各片之间建立共享失败取消链，父 context 须非 nil；使用返回的共享 context 提交，单片正常等待不取消其他片。业务失败或父取消可请求退出；所有片整体 Wait/Close 完成也取消共享 context，因此收尾后 Err 非 nil 不证明业务失败。重复配置产生的旧派生 context 在整体收尾一起释放；配置须在提交前完成，不能与等待/复位竞争。Pool Reset 保留并重建共享链，Group Reset 清除、须重新启用。

## `*ShardedGroup[T].WithResultCallback`

```go
func (sg *ShardedGroup[T]) WithResultCallback(fn func(core.Result[T])) *ShardedGroup[T]
```

[实现](../../internal/shard/shardgroup.go#L264) · [行为专题](../shard.md)：WithResultCallback 为所有分片设置结果回调。

**源码说明与使用片段**（原文）：

WithResultCallback 为所有分片设置结果回调。


**参数说明**：

- `fn` (`func(core.Result[T])`)：接收一个任务终态 Result 的通知函数；可能由 worker 或提交拒绝路径并发调用。检查 Err，避免阻塞和共享状态竞态；不要在此同步 Wait/Close 当前执行器。nil 清除回调。

**返回值签名**：`*ShardedGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithResultCallback(fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。保存同步通知函数，nil 清除；可能从 worker、拒绝或清理路径并发调用，panic 被记录，不改原结果。正常执行先保存结果并尝试发送流，再同步通知；通知返回后成功计数才增加，Active/Busy 才释放，最终等待和流关闭也等待通知。最早的 waited/waiting 预检查分支直接追加错误，不通知回调，不能按通知数统计全部提交尝试。必须快速返回且并发安全，不在其中 Wait/Close 当前组。

## `*ShardedGroup[T].WithStreaming`

```go
func (sg *ShardedGroup[T]) WithStreaming(bufSize int) *ShardedGroup[T]
```

[实现](../../internal/shard/shardgroup.go#L256) · [行为专题](../shard.md)：WithStreaming 为所有分片启用流式结果消费。

**源码说明与使用片段**（原文）：

WithStreaming 为所有分片启用流式结果消费。


**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`*ShardedGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStreaming(bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。立即新建结果通道，非正 bufSize 使用 Worker×2；重复调用替换引用而不关闭旧通道，应在提交前配置一次并重新取得 StreamResults。慢消费时暂存，Wait 收尾无法刷入通道的结果会丢失；完整结果用 Wait。不要在业务或结果通知中等待自身流关闭。

## `*ShardedGroup[T].WithSubmitTimeout`

```go
func (sg *ShardedGroup[T]) WithSubmitTimeout(d time.Duration) *ShardedGroup[T]
```

[实现](../../internal/shard/shardgroup.go#L227) · [行为专题](../shard.md)：WithSubmitTimeout 为所有分片设置提交超时。

**源码说明与使用片段**（原文）：

WithSubmitTimeout 为所有分片设置提交超时。


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*ShardedGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithSubmitTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。原地保存设置并返回自身；WithTimeout 正数从业务开始计时，非正值清除组期限，外部池自身期限仍可能生效。WithSubmitTimeout 只限制等待入组队列，不覆盖任务执行或分发进外部池的等待；非正值无计时器，仍受 context/等待关闭影响。配置在提交前完成，检查提交错误及最终业务结果。

## `*ShardedGroup[T].WithTimeout`

```go
func (sg *ShardedGroup[T]) WithTimeout(d time.Duration) *ShardedGroup[T]
```

[实现](../../internal/shard/shardgroup.go#L219) · [行为专题](../shard.md)：WithTimeout 为所有分片设置任务超时。

**源码说明与使用片段**（原文）：

WithTimeout 为所有分片设置任务超时。


**参数说明**：

- `d` (`time.Duration`)：每个任务开始执行后的 context 超时；正数生效，非正值清除本实例配置的执行期限，不包含等待入队时间。分片方法对每片应用，回调必须响应 context；在提交前配置。

**返回值签名**：`*ShardedGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。原地保存设置并返回自身；WithTimeout 正数从业务开始计时，非正值清除组期限，外部池自身期限仍可能生效。WithSubmitTimeout 只限制等待入组队列，不覆盖任务执行或分发进外部池的等待；非正值无计时器，仍受 context/等待关闭影响。配置在提交前完成，检查提交错误及最终业务结果。

## `*ShardedPool[T].Close`

```go
func (sp *ShardedPool[T]) Close()
```

[实现](../../internal/shard/shard.go#L306) · [行为专题](../shard.md)：Close 逐片关闭，全部完成后释放共享 FailFast context。

**源码说明与使用片段**（原文）：

Close 逐片关闭，全部完成后释放共享 FailFast context。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：逐片关闭并等待已登记任务及资源清理，全部结束后释放共享 FailFast context（包括重复配置产生的旧派生 context）；父 context 不被取消。不是先广播共享取消再关闭，忽略取消且不返回的任务可能使关闭阻塞。在所属任务或结果回调中同步 Close 会自等待；统一由所有者关闭，不同时对 GetShard 返回的片执行生命周期操作。

## `*ShardedPool[T].Ctx`

```go
func (sp *ShardedPool[T]) Ctx() context.Context
```

[实现](../../internal/shard/shard.go#L451) · [行为专题](../shard.md)：Ctx 返回第一个分片 Pool 的内部上下文。

**源码说明与使用片段**（原文）：

Ctx 返回第一个分片 Pool 的内部上下文。 当使用 WithFailFast 后，可通过此方法获取 ffCtx 以传递给 Submit。


**参数说明**：无显式参数。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Ctx()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：返回第 0 片保存的 context，零片返回 Background；共享 FailFast 取消链时可用此 context 提交。普通配置/等待/复位会影响其有效性，复位后重新取得；获取 context 不代表池仍能接受任务，也不自动替换每次提交参数。

## `*ShardedPool[T].Flush`

```go
func (sp *ShardedPool[T]) Flush(maxPerShard int) []core.Result[T]
```

[实现](../../internal/shard/shard.go#L525) · [行为专题](../shard.md)：Flush 从所有分片环形缓冲区排空结果（需提前启用 WithRingBuffer）。

**源码说明与使用片段**（原文）：

Flush 从所有分片环形缓冲区排空结果（需提前启用 WithRingBuffer）。


**参数说明**：

- `maxPerShard` (`int`)：本次从每片环形结果缓冲取出的最大项数；非正值取出每片全部现有结果，不读取主结果存储。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Flush(maxPerShard)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按片序逐片 Flush(maxPerShard)，非正取各片全部，正数总量可达片数×上限。只消费环形缓冲、不等待；镜像模式可能与 Wait 重复，溢出模式消费后 Wait 不再追加这些项。不能作为全局完成顺序或完整结果证明。

## `*ShardedPool[T].GetShard`

```go
func (sp *ShardedPool[T]) GetShard(idx int) *pool.Pool[T]
```

[实现](../../internal/shard/shard.go#L196) · [行为专题](../shard.md)：GetShard 获取指定分片的 Pool 实例，用于直接调用 Pool 的方法。

**源码说明与使用片段**（原文）：

GetShard 获取指定分片的 Pool 实例，用于直接调用 Pool 的方法。 idx 越界返回 nil。


**参数说明**：

- `idx` (`int`)：目标分片的零起始索引；负值或不小于分片数时返回 nil。

**返回值签名**：`*pool.Pool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShard(idx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：返回内部真实执行器指针，越界返回 nil，不复制也不转移单片生命周期。对该片设置会影响后续路由到它的任务；统一在提交前完成配置，避免同整体 Reset/Wait/Close 并发操作。

## `*ShardedPool[T].Reset`

```go
func (sp *ShardedPool[T]) Reset() error
```

[实现](../../internal/shard/shard.go#L318) · [行为专题](../shard.md)：Reset 重置所有分片（需在 Wait 后无活跃任务时调用）。

**源码说明与使用片段**（原文）：

Reset 重置所有分片（需在 Wait 后无活跃任务时调用）。 保留原有的 worker 数、超时等配置。 启用 FailFast 时重建跨分片的取消链；提交新一批任务前，须重新获取分片的 Ctx。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reset()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片复位，遇错误立即返回，之前已复位的片不会回滚，因此必须预先协调全部分片已完成等待且无任务/配置竞争。Pool 保留配置并重建失败取消链；Group 清除 context/FailFast/AutoScale，需重新设置。复位后重新取得各片 context 和流通道，不使用旧引用提交新批次。

## `*ShardedPool[T].ResizePerShard`

```go
func (sp *ShardedPool[T]) ResizePerShard(newSize int) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L337) · [行为专题](../shard.md)：ResizePerShard 调整每个分片的 worker 数（必须在 Submit 前调用）。

**源码说明与使用片段**（原文）：

ResizePerShard 调整每个分片的 worker 数（必须在 Submit 前调用）。


**参数说明**：

- `newSize` (`int`)：调整后的 worker 数量，缩容期间已有任务继续执行；等待缩容完成需使用对应等待方法。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ResizePerShard(newSize)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片 Resize，非正 newSize 各片按 IO()；总目标为片数×每片目标，不是全局总 worker。各片先更新 Size，缩容退出随后发生；不会重建队列容量。按公开约定在提交前调用，统一管理生命周期，不依据返回自身判定旧 worker 全部退出。

## `*ShardedPool[T].ShardCount`

```go
func (sp *ShardedPool[T]) ShardCount() int
```

[实现](../../internal/shard/shard.go#L190) · [行为专题](../shard.md)：ShardCount 返回分片数。

**源码说明与使用片段**（原文）：

ShardCount 返回分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：返回已创建分片数量，不查询存活 worker 或空闲容量；所有片关闭后片数仍保持。零值可返回 0，但没有片的提交入口不能使用，必须通过构造器初始化；不要依据片数推断资源可用。

## `*ShardedPool[T].ShardStats`

```go
func (sp *ShardedPool[T]) ShardStats() []pool.PoolStats
```

[实现](../../internal/shard/shard.go#L461) · [行为专题](../shard.md)：ShardStats 汇总所有分片的统计信息。

**源码说明与使用片段**（原文）：

ShardStats 汇总所有分片的统计信息。


**参数说明**：无显式参数。

**返回值签名**：`[]pool.PoolStats`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardStats()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedPool[T].StreamResults`

```go
func (sp *ShardedPool[T]) StreamResults() <-chan core.Result[T]
```

[实现](../../internal/shard/shard.go#L395) · [行为专题](../shard.md)：StreamResults 返回合并所有分片流式结果的只读 channel。

**源码说明与使用片段**（原文）：

StreamResults 返回合并所有分片流式结果的只读 channel。 每个分片的流式结果被 fan-in 到此 channel，所有分片 channel 关闭后自动关闭此 channel。


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StreamResults()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：每次新建合并通道和转发 goroutine，跳过未启用的各片流；所有转发完成后关闭合并通道。只调用一次并持续消费，重复调用会竞争分片结果，停止消费可能阻塞转发 goroutine。单片流仍可能丢结果，完整性应另核对 Wait/计数及上限策略。

## `*ShardedPool[T].Submit`

```go
func (sp *ShardedPool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/shard/shard.go#L204) · [行为专题](../shard.md)：Submit 将任务分发到某个分片提交。

**源码说明与使用片段**（原文）：

Submit 将任务分发到某个分片提交。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Submit(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。只提交所选片，不搜索其他空闲片；Pool 普通提交按 Distribution 选片，Hash 无元素时固定第 0 片，Group 普通 Go 始终轮询。同片仍可并发执行，error 只表示提交状态，Drop 还可能返回 nil 却丢弃任务；最终等待检查逐项 Err。Try 不等待队列空位，满队列返回 ErrSubmitTimeout。

## `*ShardedPool[T].SubmitAt`

```go
func (sp *ShardedPool[T]) SubmitAt(shardIdx, index int, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/shard/shard.go#L210) · [行为专题](../shard.md)：SubmitAt 将任务分发到指定分片的指定索引。

**源码说明与使用片段**（原文）：

SubmitAt 将任务分发到指定分片的指定索引。


**参数说明**：

- `shardIdx` (`int`)：目标片的零起始索引；越界时按单项默认路由选片：Pool 的 RoundRobin 轮转、Hash 或未知策略选第 0 片，Group 轮转。结果按分片合并，该值不表示合并后结果索引。
- `index` (`int`)：目标分片内部的结果索引；负值被拒绝，Pool 另外限制为不大于 core.MaxPoolIndex。稳定排序需在片内使用唯一连续索引；各片索引不是全局索引。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitAt(shardIdx, index, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。shardIdx 越界时按普通分发规则选片，index 是该片内部的结果索引。不同任务应使用唯一连续索引：Pool 重复索引可能覆盖结果，Group 会把旧结果移至其他槽位，均不能用来替换任务。大索引可分配大量内存，负索引返回提交错误；Pool 还检查 MaxPoolIndex。返回 nil 仍须等待业务完成并检查结果。

## `*ShardedPool[T].SubmitBatch`

```go
func (sp *ShardedPool[T]) SubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]SubmitBatchResult, error)
```

[实现](../../internal/shard/shard.go#L247) · [行为专题](../shard.md)：按配置的轮转或 KeyFn 路由逐项提交，返回与输入对应的分片、输入索引及提交错误；末尾 error 为首个提交错误。执行结果另行等待或消费。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`[]SubmitBatchResult, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitBatch(ctx, items, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。逐项尝试全部输入，首个提交错误不停止后续项；返回每项原输入索引、片号、提交错误，Index 不是片内结果索引，第二返回值仅为首个提交错误。Pool Hash 使用 KeyFn；Group 始终轮询。选片不检查该片是否已关闭，也不搜索其他可用片，拒绝仍推进普通轮询。TrySubmitBatch 对关闭/已等待/正在等待片的状态拒绝不保存主结果或增加计数，必须检查批量返回项；SubmitBatch/GoBatch 的状态拒绝仍记录。回调及结果仍异步完成，Go/Submit 的 nil 不保证业务成功；Drop 策略还可能丢弃任务。SubmitBatch 按输入顺序同步提交，前项等待热点片时后项即使目标空闲也等待；正数提交超时按项重新计时，总耗时可累加，整批预算放在父 context。MaxPending 满时 Block 的 Try 返回 ErrSubmitTimeout，Error 返回 ErrQueueOverflow，Drop 返回 nil 但记录 ErrQueueOverflow 并计失败。KeyFn 同步 panic 直接传播并中止，无法返回完整清单；此前已接受项不回滚，仍须收尾。

## `*ShardedPool[T].SubmitKeyed`

```go
func (sp *ShardedPool[T]) SubmitKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/shard/shard.go#L224) · [行为专题](../shard.md)：SubmitKeyed 按 key 哈希分发到固定分片，保证同一 key 的任务落到同一分片。

**源码说明与使用片段**（原文）：

SubmitKeyed 按 key 哈希分发到固定分片，保证同一 key 的任务落到同一分片。


**参数说明**：

- `key` (`string`)：键或路由标识；类型及匹配规则由接收者决定。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitKeyed(key, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。按字符串 key 的 FNV-1a 32 位值对片数取模，不读取 Distribution/KeyFn。相同 key 路由到同片，但该片多 worker 时可并发、完成顺序不保证；热点不会自动迁移到空闲片。返回的是提交错误，仍须等待并检查业务结果。

## `*ShardedPool[T].TotalActive`

```go
func (sp *ShardedPool[T]) TotalActive() int
```

[实现](../../internal/shard/shard.go#L488) · [行为专题](../shard.md)：TotalActive 汇总所有分片当前活跃任务数。

**源码说明与使用片段**（原文）：

TotalActive 汇总所有分片当前活跃任务数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalActive()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedPool[T].TotalBusy`

```go
func (sp *ShardedPool[T]) TotalBusy() int
```

[实现](../../internal/shard/shard.go#L497) · [行为专题](../shard.md)：TotalBusy 汇总所有分片当前忙碌任务数。

**源码说明与使用片段**（原文）：

TotalBusy 汇总所有分片当前忙碌任务数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalBusy()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedPool[T].TotalFailCount`

```go
func (sp *ShardedPool[T]) TotalFailCount() int64
```

[实现](../../internal/shard/shard.go#L470) · [行为专题](../shard.md)：TotalFailCount 汇总所有分片的失败数。

**源码说明与使用片段**（原文）：

TotalFailCount 汇总所有分片的失败数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalFailCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedPool[T].TotalOkCount`

```go
func (sp *ShardedPool[T]) TotalOkCount() int64
```

[实现](../../internal/shard/shard.go#L479) · [行为专题](../shard.md)：TotalOkCount 汇总所有分片的成功数。

**源码说明与使用片段**（原文）：

TotalOkCount 汇总所有分片的成功数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalOkCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedPool[T].TotalPending`

```go
func (sp *ShardedPool[T]) TotalPending() int
```

[实现](../../internal/shard/shard.go#L506) · [行为专题](../shard.md)：TotalPending 汇总所有分片等待中任务数。

**源码说明与使用片段**（原文）：

TotalPending 汇总所有分片等待中任务数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalPending()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。

## `*ShardedPool[T].TotalWorkerCount`

```go
func (sp *ShardedPool[T]) TotalWorkerCount() int
```

[实现](../../internal/shard/shard.go#L516) · [行为专题](../shard.md)：TotalWorkerCount 汇总所有分片 Size 的目标 worker 数。

**源码说明与使用片段**（原文）：

TotalWorkerCount 汇总所有分片 Size 的目标 worker 数。 缩容退出和关闭清理期间，此值不代表仍存活的 goroutine 数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalWorkerCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取 Pool.Size 并求和，是目标 worker 数，不是全局原子快照。缩容先更新 Size、旧 worker 随后退出，关闭后 Size 也不能表示存活 goroutine 数；不要据此判断清理完成或当前可用容量。

## `*ShardedPool[T].TrySubmit`

```go
func (sp *ShardedPool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/shard/shard.go#L218) · [行为专题](../shard.md)：TrySubmit 非阻塞分发提交。

**源码说明与使用片段**（原文）：

TrySubmit 非阻塞分发提交。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TrySubmit(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。只提交所选片，不搜索其他空闲片；Pool 普通提交按 Distribution 选片，Hash 无元素时固定第 0 片，Group 普通 Go 始终轮询。同片仍可并发执行，error 只表示提交状态，Drop 还可能返回 nil 却丢弃任务；最终等待检查逐项 Err。Try 不等待队列空位，满队列返回 ErrSubmitTimeout。

## `*ShardedPool[T].TrySubmitBatch`

```go
func (sp *ShardedPool[T]) TrySubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]SubmitBatchResult, error)
```

[实现](../../internal/shard/shard.go#L264) · [行为专题](../shard.md)：TrySubmitBatch 批量非阻塞提交；Hash 模式下使用 KeyFn(item) 选择分片。

**源码说明与使用片段**（原文）：

TrySubmitBatch 批量非阻塞提交；Hash 模式下使用 KeyFn(item) 选择分片。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`[]SubmitBatchResult, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TrySubmitBatch(ctx, items, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐项尝试全部输入，首个提交错误不停止后续项；返回每项原输入索引、片号、提交错误，Index 不是片内结果索引，第二返回值仅为首个提交错误。Pool Hash 使用 KeyFn；Group 始终轮询。选片不检查该片是否已关闭，也不搜索其他可用片，拒绝仍推进普通轮询。TrySubmitBatch 对关闭/已等待/正在等待片的状态拒绝不保存主结果或增加计数，必须检查批量返回项；SubmitBatch/GoBatch 的状态拒绝仍记录。回调及结果仍异步完成，Go/Submit 的 nil 不保证业务成功；Drop 策略还可能丢弃任务。SubmitBatch 按输入顺序同步提交，前项等待热点片时后项即使目标空闲也等待；正数提交超时按项重新计时，总耗时可累加，整批预算放在父 context。MaxPending 满时 Block 的 Try 返回 ErrSubmitTimeout，Error 返回 ErrQueueOverflow，Drop 返回 nil 但记录 ErrQueueOverflow 并计失败。KeyFn 同步 panic 直接传播并中止，无法返回完整清单；此前已接受项不回滚，仍须收尾。

## `*ShardedPool[T].TrySubmitKeyed`

```go
func (sp *ShardedPool[T]) TrySubmitKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/shard/shard.go#L232) · [行为专题](../shard.md)：TrySubmitKeyed 按 key 哈希非阻塞分发。

**源码说明与使用片段**（原文）：

TrySubmitKeyed 按 key 哈希非阻塞分发。


**参数说明**：

- `key` (`string`)：键或路由标识；类型及匹配规则由接收者决定。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TrySubmitKeyed(key, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。按字符串 key 的 FNV-1a 32 位值对片数取模，不读取 Distribution/KeyFn。相同 key 路由到同片，但该片多 worker 时可并发、完成顺序不保证；热点不会自动迁移到空闲片。返回的是提交错误，仍须等待并检查业务结果。

## `*ShardedPool[T].Wait`

```go
func (sp *ShardedPool[T]) Wait() []core.Result[T]
```

[实现](../../internal/shard/shard.go#L283) · [行为专题](../shard.md)：Wait 等待所有分片完成，合并结果。

**源码说明与使用片段**（原文）：

Wait 等待所有分片完成，合并结果。 多次调用返回 nil（与 Pool.Wait 行为一致）。 所有分片完成后释放整体 FailFast context，不据此判断业务是否失败。


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：逐片等待并按片号拼接结果，不保持全局提交顺序；全部完成后释放整体 WithFailFast 共享 context，不能凭收尾后的 ctx.Err() 判断业务失败。单片正常等待只取消该片子 context，跨片任务应使用整体 WithFailFast 返回值或 Pool Builder 的 Run context。Pool 的等待入口每批一次，重复/并发 Wait 返回 nil；Group 可再次读取最终结果。WaitAndClose 还关闭 Pool worker，其他 Pool 等待须另行 Close。复用须先成功 Reset；回调不返回时等待可能阻塞。

## `*ShardedPool[T].WaitAndClose`

```go
func (sp *ShardedPool[T]) WaitAndClose() []core.Result[T]
```

[实现](../../internal/shard/shard.go#L299) · [行为专题](../shard.md)：WaitAndClose 等价于先调用 Wait 再调用 Close。若已有 Wait 在执行，

**源码说明与使用片段**（原文）：

WaitAndClose 等价于先调用 Wait 再调用 Close。若已有 Wait 在执行， 本次 Wait 返回 nil 后仍会执行 Close，并等待所有分片关闭。


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitAndClose()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：逐片等待并按片号拼接结果，不保持全局提交顺序；全部完成后释放整体 WithFailFast 共享 context，不能凭收尾后的 ctx.Err() 判断业务失败。单片正常等待只取消该片子 context，跨片任务应使用整体 WithFailFast 返回值或 Pool Builder 的 Run context。Pool 的等待入口每批一次，重复/并发 Wait 返回 nil；Group 可再次读取最终结果。WaitAndClose 还关闭 Pool worker，其他 Pool 等待须另行 Close。复用须先成功 Reset；回调不返回时等待可能阻塞。

## `*ShardedPool[T].WithFailFast`

```go
func (sp *ShardedPool[T]) WithFailFast(ctx context.Context) (*ShardedPool[T], context.Context)
```

[实现](../../internal/shard/shard.go#L362) · [行为专题](../shard.md)：WithFailFast 为所有分片启用 FailFast 模式。

**源码说明与使用片段**（原文）：

WithFailFast 为所有分片启用 FailFast 模式。 返回的共享 context 在整体 Wait/Close 完成后取消；单片正常等待不会取消它。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*ShardedPool[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFailFast(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：在各片之间建立共享失败取消链，父 context 须非 nil；使用返回的共享 context 提交，单片正常等待不取消其他片。业务失败或父取消可请求退出；所有片整体 Wait/Close 完成也取消共享 context，因此收尾后 Err 非 nil 不证明业务失败。重复配置产生的旧派生 context 在整体收尾一起释放；配置须在提交前完成，不能与等待/复位竞争。Pool Reset 保留并重建共享链，Group Reset 清除、须重新启用。

## `*ShardedPool[T].WithMaxPending`

```go
func (sp *ShardedPool[T]) WithMaxPending(n int) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L426) · [行为专题](../shard.md)：WithMaxPending 为所有分片设置最大等待任务数（背压控制）。

**源码说明与使用片段**（原文）：

WithMaxPending 为所有分片设置最大等待任务数（背压控制）。


**参数说明**：

- `n` (`int`)：WithMaxPending 为所有分片设置最大等待任务数（背压控制）。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithMaxPending(n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。只限制等待任务，不含正在执行业务的 worker；MaxPending 仅正数更新，非正不清除现有上限，也不改变底层 channel 容量。Drop 达限丢弃且提交可返回 nil，Error 返回 ErrQueueOverflow，Block 等空位并响应 context/提交期限/关闭。此策略不代替结果环形缓冲策略；配置在提交前完成。

## `*ShardedPool[T].WithMaxResults`

```go
func (sp *ShardedPool[T]) WithMaxResults(n int) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L434) · [行为专题](../shard.md)：WithMaxResults 为所有分片设置结果存储上限。

**源码说明与使用片段**（原文）：

WithMaxResults 为所有分片设置结果存储上限。


**参数说明**：

- `n` (`int`)：每池主结果的保留上限；0 无限制，正数按提交索引限制，负数忽略并保留原配置。应在提交前设置，超出部分通过已配置的环形缓冲、流或回调处理，否则可能丢失。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithMaxResults(n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。非负更新主结果保留上限，0 无限制，负值忽略；正数按提交索引保留，非按完成先后。更改不会回收已分配结果槽位，提交期间改动可能使不同任务采用不同上限快照，须提前配置。超限结果需单独用 completion/流/环形缓冲处理；错误计数不受保留上限约束。

## `*ShardedPool[T].WithOverflow`

```go
func (sp *ShardedPool[T]) WithOverflow(strategy core.OverflowStrategy) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L442) · [行为专题](../shard.md)：WithOverflow 为所有分片设置溢出策略。

**源码说明与使用片段**（原文）：

WithOverflow 为所有分片设置溢出策略。


**参数说明**：

- `strategy` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithOverflow(strategy)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。只限制等待任务，不含正在执行业务的 worker；MaxPending 仅正数更新，非正不清除现有上限，也不改变底层 channel 容量。Drop 达限丢弃且提交可返回 nil，Error 返回 ErrQueueOverflow，Block 等空位并响应 context/提交期限/关闭。此策略不代替结果环形缓冲策略；配置在提交前完成。

## `*ShardedPool[T].WithResultCallback`

```go
func (sp *ShardedPool[T]) WithResultCallback(fn func(core.Result[T])) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L386) · [行为专题](../shard.md)：WithResultCallback 为所有分片设置结果回调。

**源码说明与使用片段**（原文）：

WithResultCallback 为所有分片设置结果回调。


**参数说明**：

- `fn` (`func(core.Result[T])`)：接收一个任务终态 Result 的通知函数；可能由 worker 或提交拒绝路径并发调用。检查 Err，避免阻塞和共享状态竞态；不要在此同步 Wait/Close 当前执行器。nil 清除回调。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithResultCallback(fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。普通提交在预检查或背压检查中被直接拒绝时，只记录主结果/计数，不调用此回调，也不发往结果流；入队等待失败、关闭清理和实际执行路径才通知。不要据回调次数推断全部提交次数；需要每次提交的终态使用 SubmitWithCompletion。回调可并发，panic 被记录且不改变原结果。

## `*ShardedPool[T].WithRingBuffer`

```go
func (sp *ShardedPool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L418) · [行为专题](../shard.md)：WithRingBuffer 为所有分片启用环形缓冲区。

**源码说明与使用片段**（原文）：

WithRingBuffer 为所有分片启用环形缓冲区。


**参数说明**：

- `capacity` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。
- `overflow` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithRingBuffer(capacity, overflow)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。主结果仍独立保留。MaxResults>0 时只写上限外结果，Wait 追加未取出的溢出项；MaxResults==0 时镜像全部结果，Flush 与 Wait 可能重复取得同一任务，Wait 不排空镜像缓冲。满时 Drop 覆盖最旧项，Block/Error 拒绝新写入而不阻塞。

## `*ShardedPool[T].WithStreaming`

```go
func (sp *ShardedPool[T]) WithStreaming(bufSize int) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L378) · [行为专题](../shard.md)：WithStreaming 为所有分片启用流式结果消费。

**源码说明与使用片段**（原文）：

WithStreaming 为所有分片启用流式结果消费。


**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStreaming(bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。WithStreaming 立即替换通道，非正容量按 Size×2；只在提交前配置一次，旧引用不自动更新/关闭。StreamResults 未启用返回 nil、启用返回同一通道，多消费者竞争；满时直接丢结果并增 StreamDropped，Wait/关闭收尾关通道。普通预检查拒绝不进入流，不能把流当全部尝试审计，最终结果保留仍独立。

## `*ShardedPool[T].WithSubmitTimeout`

```go
func (sp *ShardedPool[T]) WithSubmitTimeout(d time.Duration) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L353) · [行为专题](../shard.md)：WithSubmitTimeout 为所有分片设置提交超时。

**源码说明与使用片段**（原文）：

WithSubmitTimeout 为所有分片设置提交超时。


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithSubmitTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。原地保存时长，非正任务超时清除池执行期限，非正提交超时取消显式入队计时器；父 context 仍有效。任务期限从 worker 开始执行计时，不含排队；提交期限包含 MaxPending 等空位及 channel 等待。回调须响应取消，配置不得与提交竞争。

## `*ShardedPool[T].WithTimeout`

```go
func (sp *ShardedPool[T]) WithTimeout(d time.Duration) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L345) · [行为专题](../shard.md)：WithTimeout 为所有分片设置任务超时。

**源码说明与使用片段**（原文）：

WithTimeout 为所有分片设置任务超时。


**参数说明**：

- `d` (`time.Duration`)：每个任务开始执行后的 context 超时；正数生效，非正值清除本实例配置的执行期限，不包含等待入队时间。分片方法对每片应用，回调必须响应 context；在提交前配置。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片应用同名设置，非跨片原子配置，容量按各片独立计算。原地保存时长，非正任务超时清除池执行期限，非正提交超时取消显式入队计时器；父 context 仍有效。任务期限从 worker 开始执行计时，不含排队；提交期限包含 MaxPending 等空位及 channel 等待。回调须响应取消，配置不得与提交竞争。

## `DefaultShardConfig`

```go
func DefaultShardConfig[T any]() ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L70) · [行为专题](../shard.md)：DefaultShardConfig 返回使用默认值（4 分片、IO 并发度、RoundRobin）的 ShardPoolConfig。

**源码说明与使用片段**（原文）：

DefaultShardConfig 返回使用默认值（4 分片、IO 并发度、RoundRobin）的 ShardPoolConfig。 配合链式设置使用：

	cfg := shard.DefaultShardConfig[string]().
	    WithTimeout(5 * time.Second).
	    WithFailFast()


**参数说明**：无显式参数。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.DefaultShardConfig[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：返回独立配置值，默认 4 片、每片 worker 覆盖为 0（创建时用 IO()）、RoundRobin，不创建资源。PoolCfg 为零值，构造时主结果上限明确无限；修改配置须保存值方法的返回值，后续总并发按片数乘每片并发计算。

## `DefaultShardedGroup`

```go
func DefaultShardedGroup[T any]() *ShardedGroup[T]
```

[实现](../../internal/shard/shardgroup.go#L33) · [行为专题](../shard.md)：DefaultShardedGroup 使用默认配置创建分片 Group。

**源码说明与使用片段**（原文）：

DefaultShardedGroup 使用默认配置创建分片 Group。 默认 4 个分片，每个分片 IO 并发度，RoundRobin 分发。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.DefaultShardedGroup[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即分配各片执行器；有效片数非正按 4，每片并发非正按 IO()。各片配置/内存独立计算，最终由调用方 Close；仅完成 Wait 不等于所有类型都释放常驻 worker。不要使用未初始化的零值或复制含同步字段的实例，同 key 落同片也可能并发执行。

## `DefaultShardedPool`

```go
func DefaultShardedPool[T any]() *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L56) · [行为专题](../shard.md)：DefaultShardedPool 使用默认配置创建分片池。

**源码说明与使用片段**（原文）：

DefaultShardedPool 使用默认配置创建分片池。 默认 4 个分片，每个分片 IO 并发度，RoundRobin 分发。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.DefaultShardedPool[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即分配各片执行器；有效片数非正按 4，每片并发非正按 IO()。各片配置/内存独立计算，最终由调用方 Close；仅完成 Wait 不等于所有类型都释放常驻 worker。不要使用未初始化的零值或复制含同步字段的实例，同 key 落同片也可能并发执行。

## `DefaultShardedPoolWith`

```go
func DefaultShardedPoolWith[T any](shards int) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L561) · [行为专题](../shard.md)：DefaultShardedPoolWith 用自定义分片数创建分片池，其余使用默认值（IO 并发度、RoundRobin）。

**源码说明与使用片段**（原文）：

DefaultShardedPoolWith 用自定义分片数创建分片池，其余使用默认值（IO 并发度、RoundRobin）。 shards 是分片数量，\<=0 时使用默认 4 个分片。

使用示例：

	// 16 个分片，每个分片 IO 并发度
	p := shard.DefaultShardedPoolWith[string](16)
	defer p.Close()


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.DefaultShardedPoolWith[T](shards)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即分配各片执行器；有效片数非正按 4，每片并发非正按 IO()。各片配置/内存独立计算，最终由调用方 Close；仅完成 Wait 不等于所有类型都释放常驻 worker。不要使用未初始化的零值或复制含同步字段的实例，同 key 落同片也可能并发执行。

## `NewAutoScaleShardedPool`

```go
func NewAutoScaleShardedPool[T any](shards int, initialSizePerShard int, config *core.AutoScaleConfig) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L588) · [行为专题](../shard.md)：NewAutoScaleShardedPool 创建带自动扩缩容的分片池。

**源码说明与使用片段**（原文）：

NewAutoScaleShardedPool 创建带自动扩缩容的分片池。 每个分片都会启用独立的自动扩缩容，根据各自负载独立调整 worker 数量。

参数：

  - shards：分片数量，\<=0 时默认 4
  - initialSizePerShard：每个分片的初始 worker 数量，\<=0 时使用 IO 并发度
  - config：自动扩缩容配置，nil 时使用 DefaultAutoScaleConfig()

使用示例：

	// 默认自动扩缩容配置
	sp := shard.NewAutoScaleShardedPool[string](8, 4, nil)
	defer sp.Close()

	// 自定义扩缩容配置
	sp := shard.NewAutoScaleShardedPool[int](4, 8, &core.AutoScaleConfig{
	    MinWorkers:    2,
	    MaxWorkers:    200,
	    CheckInterval: 3 * time.Second,
	    ScaleUpFactor: 1.5,
	})


**参数说明**：

- `shards` (`int`)：分片数量，<=0 时默认 4
- `initialSizePerShard` (`int`)：每个分片的初始 worker 数量，<=0 时使用 IO 并发度
- `config` (`*core.AutoScaleConfig`)：自动扩缩容配置，nil 时使用 DefaultAutoScaleConfig()

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.NewAutoScaleShardedPool[T](shards, initialSizePerShard, config)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即分配各片执行器；有效片数非正按 4，每片并发非正按 IO()。各片配置/内存独立计算，最终由调用方 Close；仅完成 Wait 不等于所有类型都释放常驻 worker。不要使用未初始化的零值或复制含同步字段的实例，同 key 落同片也可能并发执行。

## `NewGroupBuilder`

```go
func NewGroupBuilder[T any]() *ShardedGroupBuilder[T]
```

[实现](../../internal/shard/sharded_group_builder.go#L19) · [行为专题](../shard.md)：NewGroupBuilder 创建分片 Group 构造器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewGroupBuilder 创建分片 Group 构造器，默认使用 context.Background()。 通过 .Context(ctx) 链式设置上下文。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.NewGroupBuilder[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建可变构建器，context 初始 Background，配置为对应默认分片配置；此时不创建 worker。后续 Run 新建资源并管理关闭，须在终端前完成配置，不能并发修改；总并发及容量按每片累加，作用域中仍需检查提交错误并等待业务结果。

## `NewPoolBuilder`

```go
func NewPoolBuilder[T any]() *ShardPoolBuilder[T]
```

[实现](../../internal/shard/shard_builder.go#L19) · [行为专题](../shard.md)：NewPoolBuilder 创建分片池构造器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewPoolBuilder 创建分片池构造器，默认使用 context.Background()。 通过 .Context(ctx) 链式设置上下文。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.NewPoolBuilder[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建可变构建器，context 初始 Background，配置为对应默认分片配置；此时不创建 worker。后续 Run 新建资源并管理关闭，须在终端前完成配置，不能并发修改；总并发及容量按每片累加，作用域中仍需检查提交错误并等待业务结果。

## `NewShardedGroup`

```go
func NewShardedGroup[T any](cfg ShardGroupConfig[T]) *ShardedGroup[T]
```

[实现](../../internal/shard/shardgroup.go#L43) · [行为专题](../shard.md)：NewShardedGroup 创建分片 Group。

**源码说明与使用片段**（原文）：

NewShardedGroup 创建分片 Group。 cfg 中零值字段会使用默认值（4 分片、IO 并发度、RoundRobin）。


**参数说明**：

- `cfg` (`ShardGroupConfig[T]`)：配置或执行策略；各字段默认值与组合优先级见专题。

**返回值签名**：`*ShardedGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.NewShardedGroup[T](cfg)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即分配各片执行器；有效片数非正按 4，每片并发非正按 IO()。各片配置/内存独立计算，最终由调用方 Close；仅完成 Wait 不等于所有类型都释放常驻 worker。不要使用未初始化的零值或复制含同步字段的实例，同 key 落同片也可能并发执行。

## `NewShardedPool`

```go
func NewShardedPool[T any](cfg ShardPoolConfig[T]) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L81) · [行为专题](../shard.md)：NewShardedPool 创建分片池。

**源码说明与使用片段**（原文）：

NewShardedPool 创建分片池。 每个分片内 worker 数 = sizePerShard。 cfg 中零值字段会使用默认值（4 分片、IO 并发度、RoundRobin）。


**参数说明**：

- `cfg` (`ShardPoolConfig[T]`)：配置或执行策略；各字段默认值与组合优先级见专题。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.NewShardedPool[T](cfg)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即分配各片执行器；有效片数非正按 4，每片并发非正按 IO()。各片配置/内存独立计算，最终由调用方 Close；仅完成 Wait 不等于所有类型都释放常驻 worker。不要使用未初始化的零值或复制含同步字段的实例，同 key 落同片也可能并发执行。

## `NewShardedPoolSimple`

```go
func NewShardedPoolSimple[T any](shards int, sizePerShard int) *ShardedPool[T]
```

[实现](../../internal/shard/shard.go#L546) · [行为专题](../shard.md)：NewShardedPoolSimple 用简单参数创建分片池，无需配置结构体。

**源码说明与使用片段**（原文）：

NewShardedPoolSimple 用简单参数创建分片池，无需配置结构体。 shards 是分片数量，sizePerShard 是每个分片的 worker 数量（\<=0 时使用 IO 并发度）。

使用示例：

	// 8 个分片，每个分片 16 个 worker
	p := shard.NewShardedPoolSimple[string](8, 16)
	defer p.Close()

	// 4 个分片，每个分片使用默认 IO 并发度
	p := shard.NewShardedPoolSimple[int](4, 0)


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `sizePerShard` (`int`)：每个分片的 worker 数；总 worker 容量还需乘以分片数量。

**返回值签名**：`*ShardedPool[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
shard.NewShardedPoolSimple[T](shards, sizePerShard)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即分配各片执行器；有效片数非正按 4，每片并发非正按 IO()。各片配置/内存独立计算，最终由调用方 Close；仅完成 Wait 不等于所有类型都释放常驻 worker。不要使用未初始化的零值或复制含同步字段的实例，同 key 落同片也可能并发执行。

## `WithShardCfg`

```go
func WithShardCfg[T any](ctx context.Context, cfg ShardPoolConfig[T], fn func(sp *ShardedPool[T]) error) error
```

[实现](../../internal/shard/shard.go#L662) · [行为专题](../shard.md)：WithShardCfg 使用 ShardPoolConfig 创建分片池并执行 fn，fn 返回后自动 Close。

**源码说明与使用片段**（原文）：

WithShardCfg 使用 ShardPoolConfig 创建分片池并执行 fn，fn 返回后自动 Close。 支持完整 Config 传入或链式设置后传入。

使用方式一：完整传入

	err := shard.WithShardCfg(ctx, shard.ShardPoolConfig[string]{
	    Shards: 8, SizePerShard: 16,
	    PoolCfg: pool.Config{Timeout: 5 * time.Second, FailFast: true},
	}, func(sp *ShardedPool[string]) error {
	    for _, item := range items {
	        sp.Submit(sp.Ctx(), func(ctx context.Context) (string, error) {
	            return process(item)
	        })
	    }
	    results := sp.Wait()
	    return check(results)
	})

使用方式二：链式设置（通过 NewShardedPoolConfig 便捷构造）

	cfg := shard.DefaultShardConfig[string]().
	    WithTimeout(5 * time.Second).
	    WithFailFast()
	err := shard.WithShardCfg(ctx, cfg, func(sp *ShardedPool[string]) error { ... })


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `cfg` (`ShardPoolConfig[T]`)：配置或执行策略；各字段默认值与组合优先级见专题。
- `fn` (`func(sp *ShardedPool[T]) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
shard.WithShardCfg[T](ctx, cfg, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即创建分片池并在回调退出时 Close；返回错误只来自作用域回调，任务失败需在回调内等待并检查。检查每次提交错误，提前退出可能使尚未执行的任务得到关闭错误。WithShardCfg 在显式环形缓冲配置下重新应用全局缓冲溢出策略；输入 context 只在启用 FailFast 时绑定取消链。

## `WithShardedPool`

```go
func WithShardedPool[T any](shards int, sizePerShard int, fn func(sp *ShardedPool[T]) error) error
```

[实现](../../internal/shard/shard.go#L632) · [行为专题](../shard.md)：WithShardedPool 创建分片池并执行 fn，fn 返回后自动 Close 所有分片释放资源。

**源码说明与使用片段**（原文）：

WithShardedPool 创建分片池并执行 fn，fn 返回后自动 Close 所有分片释放资源。 shards \<= 0 时使用默认分片数，sizePerShard \<= 0 时使用 IO 并发度。

示例：

	err := shard.WithShardedPool(16, 64, func(sp *ShardedPool[int]) error {
	    for _, item := range items {
	        sp.Submit(ctx, func(ctx context.Context) (int, error) {
	            return process(item)
	        })
	    }
	    results := sp.Wait()
	    for _, r := range results {
	        if r.Err != nil { return r.Err }
	    }
	    return nil
	})


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `sizePerShard` (`int`)：每个分片的 worker 数；总 worker 容量还需乘以分片数量。
- `fn` (`func(sp *ShardedPool[T]) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
shard.WithShardedPool[T](shards, sizePerShard, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即创建分片池并在回调退出时 Close；返回错误只来自作用域回调，任务失败需在回调内等待并检查。检查每次提交错误，提前退出可能使尚未执行的任务得到关闭错误。WithShardCfg 在显式环形缓冲配置下重新应用全局缓冲溢出策略；输入 context 只在启用 FailFast 时绑定取消链。

## `ShardPoolConfig[T].KeyFn`

```go
KeyFn func(T) uint64
```

[实现](../../internal/shard/shard.go#L50) · [行为专题](../shard.md)：可调用函数字段：

**参数说明**：

- `arg1` (`T`)：待路由的业务元素；回调返回稳定的 uint64 哈希键，必须支持并发调用。

**返回值签名**：`uint64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.KeyFn(arg1)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

## `ShardPoolConfig[T].WithFailFast`

```go
func (c ShardPoolConfig[T]) WithFailFast() ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L146) · [行为专题](../shard.md)：WithFailFast 启用 FailFast 模式（Config 级别，需配合 WithShardCfg 使用）。

**源码说明与使用片段**（原文）：

WithFailFast 启用 FailFast 模式（Config 级别，需配合 WithShardCfg 使用）。


**参数说明**：无显式参数。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFailFast()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

## `ShardPoolConfig[T].WithPoolCfg`

```go
func (c ShardPoolConfig[T]) WithPoolCfg(pc pool.Config) ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L137) · [行为专题](../shard.md)：WithPoolCfg 设置分片内 Pool 级别的配置。

**源码说明与使用片段**（原文）：

WithPoolCfg 设置分片内 Pool 级别的配置。


**参数说明**：

- `pc` (`pool.Config`)：应用到各分片的 Pool 配置副本；调用后继续使用返回的 ShardPoolConfig。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithPoolCfg(pc)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.PoolCfg = pc
```

## `ShardPoolConfig[T].WithShardMaxPending`

```go
func (c ShardPoolConfig[T]) WithShardMaxPending(n int) ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L158) · [行为专题](../shard.md)：WithShardMaxPending 设置最大等待任务数（背压控制）。

**源码说明与使用片段**（原文）：

WithShardMaxPending 设置最大等待任务数（背压控制）。


**参数说明**：

- `n` (`int`)：WithShardMaxPending 设置最大等待任务数（背压控制）。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithShardMaxPending(n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.PoolCfg.MaxPending = n
```

## `ShardPoolConfig[T].WithShardMaxResults`

```go
func (c ShardPoolConfig[T]) WithShardMaxResults(n int) ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L176) · [行为专题](../shard.md)：WithShardMaxResults 设置结果切片容量上限。

**源码说明与使用片段**（原文）：

WithShardMaxResults 设置结果切片容量上限。


**参数说明**：

- `n` (`int`)：每片主结果的保留上限；正数限制结果，非正数在创建分片时均应用为无限制，不继承 Pool 的全局上限。结果溢出路径须另行配置和消费。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithShardMaxResults(n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.PoolCfg.MaxResults = n
```

## `ShardPoolConfig[T].WithShardOverflow`

```go
func (c ShardPoolConfig[T]) WithShardOverflow(s core.OverflowStrategy) ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L164) · [行为专题](../shard.md)：WithShardOverflow 设置溢出策略。

**源码说明与使用片段**（原文）：

WithShardOverflow 设置溢出策略。


**参数说明**：

- `s` (`core.OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithShardOverflow(s)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.PoolCfg.Overflow = s
```

## `ShardPoolConfig[T].WithShardRingBuf`

```go
func (c ShardPoolConfig[T]) WithShardRingBuf(capacity int) ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L170) · [行为专题](../shard.md)：WithShardRingBuf 启用环形缓冲区。

**源码说明与使用片段**（原文）：

WithShardRingBuf 启用环形缓冲区。


**参数说明**：

- `capacity` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithShardRingBuf(capacity)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.PoolCfg.RingBufCap = capacity
```

## `ShardPoolConfig[T].WithShardStreaming`

```go
func (c ShardPoolConfig[T]) WithShardStreaming(bufSize int) ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L182) · [行为专题](../shard.md)：WithShardStreaming 启用流式结果消费。

**源码说明与使用片段**（原文）：

WithShardStreaming 启用流式结果消费。


**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithShardStreaming(bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.PoolCfg.Streaming = bufSize
```

## `ShardPoolConfig[T].WithShardSubmitTimeout`

```go
func (c ShardPoolConfig[T]) WithShardSubmitTimeout(d time.Duration) ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L152) · [行为专题](../shard.md)：WithShardSubmitTimeout 设置提交超时。

**源码说明与使用片段**（原文）：

WithShardSubmitTimeout 设置提交超时。


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithShardSubmitTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.PoolCfg.SubmitTimeout = d
```

## `ShardPoolConfig[T].WithTimeout`

```go
func (c ShardPoolConfig[T]) WithTimeout(d time.Duration) ShardPoolConfig[T]
```

[实现](../../internal/shard/shard.go#L140) · [行为专题](../shard.md)：WithTimeout 为所有分片设置任务超时（Config 级别，需配合 WithShardCfg 使用）。

**源码说明与使用片段**（原文）：

WithTimeout 为所有分片设置任务超时（Config 级别，需配合 WithShardCfg 使用）。


**参数说明**：

- `d` (`time.Duration`)：创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。

**返回值签名**：`ShardPoolConfig[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
c.PoolCfg.Timeout = d
```

