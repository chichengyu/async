# pipeline 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [pipeline](../pipeline.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*FlowBuilder[T].BufSize`

```go
func (fb *FlowBuilder[T]) BufSize(n int) *FlowBuilder[T]
```

[实现](../../internal/pipeline/pipeline_flow_builder.go#L57) · [行为专题](../pipeline.md)：BufSize 设置阶段间 channel 缓冲大小（<=0 使用默认值 256）。

**源码说明与使用片段**（原文）：

BufSize 设置阶段间 channel 缓冲大小（\<=0 使用默认值 256）。


**参数说明**：

- `n` (`int`)：BufSize 设置阶段间 channel 缓冲大小（<=0 使用默认值 256）。

**返回值签名**：`*FlowBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.BufSize(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变后续 Build/Run 的阶段列表或缓冲配置，不修改已启动的 Flow。Stage 追加而非替换，阶段名不保证唯一；非正并发按 IO()，DefaultStage 直接写 IO()，非正缓冲按 256。worker 和 channel 内存按全部阶段累加。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
fb.bufSize = n
```

## `*FlowBuilder[T].Build`

```go
func (fb *FlowBuilder[T]) Build(
	ctx context.Context,
	fn func(ctx context.Context, stage string, item T) (T, error),
) *Flow[T]
```

[实现](../../internal/pipeline/pipeline_flow_builder.go#L86) · [行为专题](../pipeline.md)：Build 构建 Flow，启动常驻 worker goroutine。

**源码说明与使用片段**（原文）：

Build 构建 Flow，启动常驻 worker goroutine。 stages 为空时会 panic（与 NewFlow 行为一致）。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：各阶段 worker 调用的处理函数：接收 Flow 的 context、阶段名和当前元素，返回下一阶段的值及错误，可并发调用。panic 转为 PanicError 并保留该阶段输入值；显式 Stage.SkipOnError=true 时跳过前错，否则后续成功可覆盖它。链式 Stage 未提供该字段设置入口；取消须回调协作。

**返回值签名**：`*Flow[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build(ctx, fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：业务 runtime.Goexit 在当前阶段记录零值和 ErrTaskGoexit，worker 以同一登记名额接替并继续处理后续输入。Stream 保留输入索引并设置 Occupied，已启用 FailFast 时取消且保留该失败项；Flow 计一次历史阶段失败并归还 Active，Close 继续排空。Flow 普通 panic 仍保留阶段输入值。后续未设置 SkipOnError 时可覆盖前错；取消仍可能丢弃未转发结果。生产应正常返回 error，此保证不覆盖任意通知回调异常。每次立即启动新的常驻 Flow，阶段为空 panic；Build 使用显式 ctx，Run 使用构建器 ctx，必须非 nil。阶段并发非正按 IO()，缓冲非正按 256。须持续消费 Results 并最终 Close，合并 Close 返回的残留结果；回调不响应取消时清理可能阻塞。

## `*FlowBuilder[T].Context`

```go
func (fb *FlowBuilder[T]) Context(ctx context.Context) *FlowBuilder[T]
```

[实现](../../internal/pipeline/pipeline_flow_builder.go#L40) · [行为专题](../pipeline.md)：Context 设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*FlowBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存并确保 context 的 trace_id，只供后续 Run 使用；Build(ctx,fn) 的显式 ctx 覆盖它。配置不影响已创建的 Flow，须在启动前完成，不与创建竞争。

## `*FlowBuilder[T].DefaultStage`

```go
func (fb *FlowBuilder[T]) DefaultStage(name string) *FlowBuilder[T]
```

[实现](../../internal/pipeline/pipeline_flow_builder.go#L52) · [行为专题](../pipeline.md)：DefaultStage 使用默认 IO 并发度追加处理阶段。

**源码说明与使用片段**（原文）：

DefaultStage 使用默认 IO 并发度追加处理阶段。


**参数说明**：

- `name` (`string`)：阶段或诊断名称，用于定位执行步骤。

**返回值签名**：`*FlowBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultStage(name)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变后续 Build/Run 的阶段列表或缓冲配置，不修改已启动的 Flow。Stage 追加而非替换，阶段名不保证唯一；非正并发按 IO()，DefaultStage 直接写 IO()，非正缓冲按 256。worker 和 channel 内存按全部阶段累加。

## `*FlowBuilder[T].Logger`

```go
func (fb *FlowBuilder[T]) Logger(l core.Logger) *FlowBuilder[T]
```

[实现](../../internal/pipeline/pipeline_flow_builder.go#L63) · [行为专题](../pipeline.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*FlowBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*FlowBuilder[T].Run`

```go
func (fb *FlowBuilder[T]) Run(
	fn func(ctx context.Context, stage string, item T) (T, error),
) *Flow[T]
```

[实现](../../internal/pipeline/pipeline_flow_builder.go#L78) · [行为专题](../pipeline.md)：Run 构建 Flow 并启动常驻 worker，使用 builder 内部 ctx。

**源码说明与使用片段**（原文）：

Run 构建 Flow 并启动常驻 worker，使用 builder 内部 ctx。 等价于 Build(fb.ctx, fn)，推荐配合 defer fl.Close() 使用。

使用示例：

	fl := async.Pipeline[int](nil).
	    Stage("parse", 4).Stage("enrich", 8).
	    Flow().
	    Run(fn)
	defer fl.Close()


**参数说明**：

- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：各阶段 worker 调用的处理函数：接收 Flow 的 context、阶段名和当前元素，返回下一阶段的值及错误，可并发调用。panic 转为 PanicError 并保留该阶段输入值；显式 Stage.SkipOnError=true 时跳过前错，否则后续成功可覆盖它。链式 Stage 未提供该字段设置入口；取消须回调协作。

**返回值签名**：`*Flow[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：为常驻处理器指定可取消的 context，启动后持续消费 Results，停止提交后调用 Close 并合并其残留；按全部阶段的并发和缓冲估算资源。业务回调使用有期限的下游调用，避免关闭无限等待。

**注意事项**：业务 runtime.Goexit 在当前阶段记录零值和 ErrTaskGoexit，worker 以同一登记名额接替并继续处理后续输入。Stream 保留输入索引并设置 Occupied，已启用 FailFast 时取消且保留该失败项；Flow 计一次历史阶段失败并归还 Active，Close 继续排空。Flow 普通 panic 仍保留阶段输入值。后续未设置 SkipOnError 时可覆盖前错；取消仍可能丢弃未转发结果。生产应正常返回 error，此保证不覆盖任意通知回调异常。每次立即启动新的常驻 Flow，阶段为空 panic；Build 使用显式 ctx，Run 使用构建器 ctx，必须非 nil。阶段并发非正按 IO()，缓冲非正按 256。须持续消费 Results 并最终 Close，合并 Close 返回的残留结果；回调不响应取消时清理可能阻塞。

## `*FlowBuilder[T].Stage`

```go
func (fb *FlowBuilder[T]) Stage(name string, concurrency int) *FlowBuilder[T]
```

[实现](../../internal/pipeline/pipeline_flow_builder.go#L46) · [行为专题](../pipeline.md)：Stage 追加一个处理阶段。

**源码说明与使用片段**（原文）：

Stage 追加一个处理阶段。


**参数说明**：

- `name` (`string`)：阶段或诊断名称，用于定位执行步骤。
- `concurrency` (`int`)：此阶段的并发 worker 数；非正值使用 core.IO()。串行模式忽略此值，Parallel 的 Worker 覆盖阶段值；有限输入路径最多使用输入项数个 worker，Flow 常驻指定数量。

**返回值签名**：`*FlowBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stage(name, concurrency)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只改变后续 Build/Run 的阶段列表或缓冲配置，不修改已启动的 Flow。Stage 追加而非替换，阶段名不保证唯一；非正并发按 IO()，DefaultStage 直接写 IO()，非正缓冲按 256。worker 和 channel 内存按全部阶段累加。

## `*Flow[T].Close`

```go
func (fl *Flow[T]) Close() []core.Result[T]
```

[实现](../../internal/pipeline/pipeline_flow.go#L253) · [行为专题](../pipeline.md)：Close 关闭管道，执行优雅关闭：

**源码说明与使用片段**（原文）：

Close 关闭管道，执行优雅关闭：

 1. 停止接收新提交
 2. 等待已提交元素逐阶段排空处理
 3. 关闭结果 channel

返回值：resultCh 中未被读取的残留结果。 若调用方已通过 Results() 实时消费，返回值通常为空或少量元素。

Close 可以安全地多次调用（幂等）。只有首次调用等待关闭完成并返回残留结果； 并发的后续调用立即返回 nil，不表示首次关闭已完成。


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：首次调用等待在途提交与阶段 worker，期间与 Results 消费者竞争排空结果；须合并两路结果，消费者及关闭回调可能并发执行。后续调用立即返回 nil，不等待首次关闭完成。业务不返回时可永久阻塞。

## `*Flow[T].ErrCount`

```go
func (fl *Flow[T]) ErrCount() int64
```

[实现](../../internal/pipeline/pipeline_flow.go#L288) · [行为专题](../pipeline.md)：ErrCount 返回阶段回调失败、panic 或业务 Goexit 的历史次数；同一元素可在多阶段重复计数。

**源码说明与使用片段**（原文）：

ErrCount 返回阶段回调失败、panic 或业务 Goexit 的历史次数；同一元素可在多阶段重复计数。 后续阶段成功不会扣减它，不等于最终失败元素数量。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ErrCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：统计阶段回调的错误、panic 或业务 Goexit 次数，同一元素可在多阶段重复计数；后续阶段成功也不扣减历史计数。不是最终失败元素数量，SkipOnError 的直接透传不重复计数。

## `*Flow[T].Results`

```go
func (fl *Flow[T]) Results() <-chan core.Result[T]
```

[实现](../../internal/pipeline/pipeline_flow.go#L239) · [行为专题](../pipeline.md)：Results 返回结果 channel，调用方通过 range 实时消费处理结果。

**源码说明与使用片段**（原文）：

Results 返回结果 channel，调用方通过 range 实时消费处理结果。 channel 在 Close() 完成后自动关闭。

示例：

	for r := range fl.Results() {
	    if r.Ok() {
	        fmt.Println(r.Value)
	    }
	}


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Results()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：结果按完成顺序输出，Result 不包含提交序号，应在元素中携带关联标识。首次 Close 同时排空此通道，须处理其返回的残留结果；外部取消可能丢结果，通道需 Close 才关闭。

## `*Flow[T].Stats`

```go
func (fl *Flow[T]) Stats() FlowStats
```

[实现](../../internal/pipeline/pipeline_flow.go#L422) · [行为专题](../pipeline.md)：Stats 返回 Flow 的完整运行时统计快照。

**源码说明与使用片段**（原文）：

Stats 返回 Flow 的完整运行时统计快照。 各计数可并发读取，但各字段不是同一时刻的事务一致快照。

使用示例：

	s := fl.Stats()
	for _, st := range s.Stages {
	    fmt.Printf("阶段 %s: 活跃=%d/%d, 输入积压=%d\n",
	        st.Name, st.Active, st.Concurrency, st.InputBuf)
	}


**参数说明**：无显式参数。

**返回值签名**：`FlowStats`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stats()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：各字段分别读取，非事务一致快照。TotalError 为阶段错误、panic 或业务 Goexit 次数，TotalResult 为成功写入结果通道的次数；Closed 在首次 Close 开始时置 true，不代表关闭已完成。

## `*Flow[T].Submit`

```go
func (fl *Flow[T]) Submit(ctx context.Context, item T) (int, error)
```

[实现](../../internal/pipeline/pipeline_flow.go#L193) · [行为专题](../pipeline.md)：Submit 向管道提交一个元素，返回提交序号和可能的错误。

**源码说明与使用片段**（原文）：

Submit 向管道提交一个元素，返回提交序号和可能的错误。 序号在尝试入队前分配；失败可能留下间隔。并发提交时，序号不保证与入队顺序一致。 Result 不包含该序号；如需关联结果或排序，应在 item 中携带业务标识。

参数：

  - ctx：控制等待入队；提交成功后不用于取消该元素的处理。阶段回调接收管道上下文。
  - item：待处理的元素

返回值：

  - int：提交序号
  - error：提交失败原因（管道已关闭、context 取消等）


**参数说明**：

- `ctx` (`context.Context`)：控制等待入队；提交成功后不用于取消该元素的处理。阶段回调接收管道上下文。
- `item` (`T`)：待处理的元素

**返回值签名**：`int, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Submit(ctx, item)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：提交 context 仅限制等待入队，处理使用管道 context；同一时刻取消和入队均就绪时可成功入队。序号不保证入队或完成顺序，失败可留间隔；批量遇首个提交错误停止，已接受元素继续处理。

## `*Flow[T].SubmitBatch`

```go
func (fl *Flow[T]) SubmitBatch(ctx context.Context, items []T) (int, error)
```

[实现](../../internal/pipeline/pipeline_flow.go#L220) · [行为专题](../pipeline.md)：SubmitBatch 批量提交元素，返回成功提交的数量。

**源码说明与使用片段**（原文）：

SubmitBatch 批量提交元素，返回成功提交的数量。 遇到第一个提交错误时立即返回（含已成功提交的数量）。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`int, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitBatch(ctx, items)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：提交 context 仅限制等待入队，处理使用管道 context；同一时刻取消和入队均就绪时可成功入队。序号不保证入队或完成顺序，失败可留间隔；批量遇首个提交错误停止，已接受元素继续处理。

## `*Flow[T].SubmitCount`

```go
func (fl *Flow[T]) SubmitCount() int64
```

[实现](../../internal/pipeline/pipeline_flow.go#L282) · [行为专题](../pipeline.md)：SubmitCount 返回成功入队的元素数量，不包含被拒绝的尝试。

**源码说明与使用片段**（原文）：

SubmitCount 返回成功入队的元素数量，不包含被拒绝的尝试。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只计成功入队的元素，不是提交尝试数或最终结果数；取消管道可能丢弃已接受元素的后续转发或结果。

## `*ParallelChain[T].AutoScale`

```go
func (b *ParallelChain[T]) AutoScale(config *core.AutoScaleConfig) *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L55) · [行为专题](../pipeline.md)：AutoScale 启用自动扩缩容。

**源码说明与使用片段**（原文）：

AutoScale 启用自动扩缩容。


**参数说明**：

- `config` (`*core.AutoScaleConfig`)：保存供创建执行器时使用的自动扩缩容配置指针；nil 清除构建器中的配置、不启用扩缩容。DefaultAutoScale 保存默认配置；配置指针及字段不得在执行期间并发修改。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AutoScale(config)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：普通 Execute/Run 的路径优先级为 Pool、Shard>1、AutoScale、原生；同时配置时仅采用最先命中的路径。Shard<=1 不分片，DefaultShard=max(2,GOMAXPROCS)，AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置。ExecuteWithMeta/ExecutePipe 不采用这些路径；分片/AutoScale 的结果顺序不保证与输入一致。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.autoScaleCfg = config
```

## `*ParallelChain[T].DefaultAutoScale`

```go
func (b *ParallelChain[T]) DefaultAutoScale() *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L61) · [行为专题](../pipeline.md)：DefaultAutoScale 使用默认配置启用自动扩缩容。

**源码说明与使用片段**（原文）：

DefaultAutoScale 使用默认配置启用自动扩缩容。


**参数说明**：无显式参数。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultAutoScale()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：普通 Execute/Run 的路径优先级为 Pool、Shard>1、AutoScale、原生；同时配置时仅采用最先命中的路径。Shard<=1 不分片，DefaultShard=max(2,GOMAXPROCS)，AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置。ExecuteWithMeta/ExecutePipe 不采用这些路径；分片/AutoScale 的结果顺序不保证与输入一致。

## `*ParallelChain[T].DefaultPool`

```go
func (b *ParallelChain[T]) DefaultPool() *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L32) · [行为专题](../pipeline.md)：DefaultPool 创建默认并发度的协程池并自动管理生命周期。

**源码说明与使用片段**（原文）：

DefaultPool 创建默认并发度的协程池并自动管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPool()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 借用外部池并在阶段间复位等待状态，PoolAuto 在普通 Execute/Run 结束关闭所注入的池，DefaultPool 标记各阶段内部建池并关闭；设置本身不创建/关闭池。不可与外部池其他任务混用。ExecuteWithMeta/ExecutePipe 使用另一原生路径，不按此池配置运行或关闭池。

## `*ParallelChain[T].DefaultShard`

```go
func (b *ParallelChain[T]) DefaultShard() *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L45) · [行为专题](../pipeline.md)：DefaultShard 使用默认分片数（GOMAXPROCS，最少 2）启用分片。

**源码说明与使用片段**（原文）：

DefaultShard 使用默认分片数（GOMAXPROCS，最少 2）启用分片。


**参数说明**：无显式参数。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShard()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：普通 Execute/Run 的路径优先级为 Pool、Shard>1、AutoScale、原生；同时配置时仅采用最先命中的路径。Shard<=1 不分片，DefaultShard=max(2,GOMAXPROCS)，AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置。ExecuteWithMeta/ExecutePipe 不采用这些路径；分片/AutoScale 的结果顺序不保证与输入一致。

## `*ParallelChain[T].DefaultWorker`

```go
func (b *ParallelChain[T]) DefaultWorker() *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L73) · [行为专题](../pipeline.md)：DefaultWorker 使用 Stage 自带的并发度。

**源码说明与使用片段**（原文）：

DefaultWorker 使用 Stage 自带的并发度。


**参数说明**：无显式参数。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：正数覆盖各阶段并发，非正值及 DefaultWorker 恢复 Stage.Concurrency，再对非正阶段值使用 IO()；有限输入通常取不超过当前项数的 worker。外部池还有自身容量限制，分片可能按每片重复并发配置；Worker 不负责调整外部池。

## `*ParallelChain[T].Execute`

```go
func (b *ParallelChain[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_parallel.go#L79) · [行为专题](../pipeline.md)：Execute 并行执行所有阶段。

**源码说明与使用片段**（原文）：

Execute 并行执行所有阶段。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Execute(fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。

## `*ParallelChain[T].ExecutePipe`

```go
func (b *ParallelChain[T]) ExecutePipe(fn func(context.Context, string, T) (T, error), bufSize int) <-chan core.Result[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L94) · [行为专题](../pipeline.md)：ExecutePipe 并行执行管道，通过 channel 返回最终阶段结果。

**源码说明与使用片段**（原文）：

ExecutePipe 并行执行管道，通过 channel 返回最终阶段结果。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecutePipe(fn, bufSize)
```

**生产建议**：持续消费结果并逐项处理 Err；为生产方设置可取消的 context，消费方提前退出时及时取消或协调其他消费者排空。Drain 回调保持短小，可能失败的外部写入应有明确重试或补偿；通道关闭后再统计已收到的结果。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。输出最终阶段 Result channel，部分模式先同步完成之前阶段，不能假定立即返回。持续消费并检查逐项 Err，提前停止时取消执行 context；默认缓冲有界，不保证容纳全部结果。Parallel 此入口走原生路径，忽略 Pool/Shard/AutoScale，不能依此自动关闭注入池。

## `*ParallelChain[T].ExecuteWithMeta`

```go
func (b *ParallelChain[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L89) · [行为专题](../pipeline.md)：ExecuteWithMeta 并行执行并返回带阶段元信息的结果。

**源码说明与使用片段**（原文）：

ExecuteWithMeta 并行执行并返回带阶段元信息的结果。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]ResultWithMeta[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecuteWithMeta(fn)
```

**生产建议**：按阶段数与输入项数估算历史结果内存，逐项检查阶段名和 Err；需要定位失败时保留中间记录，不能只检查最后一阶段。Stream 的标签使用末阶段名，诊断真实出错阶段时另配 OnResult。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。默认/Serial/Parallel 按阶段及输入索引追加记录，空阶段为空；支持 SkipOnError 的路径也为跳过项保留透传记录，Serial 不读取此字段。Stream 全部槽位均标记配置中的末阶段名，包含中途取消保留的失败，不能定位实际失败阶段；空阶段返回原输入和空阶段名，取消须检查 Occupied。最终成功可能掩盖此前失败。Parallel 此入口使用原生执行，忽略 Pool/Shard/AutoScale，池生命周期由调用方处理。

## `*ParallelChain[T].Pipe`

```go
func (b *ParallelChain[T]) Pipe() *PipelinePipeBuilder[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L99) · [行为专题](../pipeline.md)：Pipe 切换为管道输出模式。

**源码说明与使用片段**（原文）：

Pipe 切换为管道输出模式。


**参数说明**：无显式参数。

**返回值签名**：`*PipelinePipeBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pipe()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建输出构建器并绑定当前执行配置的浅副本，此时不执行。之后在源构建器替换 Context/Timeout/FailFast/OnResult 或追加 Stage 不会更新此输出构建器；输入切片、context 对象、外部池和部分引用配置仍共享，原父 context 的取消仍生效。每次 Run 使用当时可见的共享输入并创建独立通道，须等上次执行结束再修改输入。不能当作深拷贝，须持续消费并检查逐项错误；Serial 非最终阶段 FailFast 错误仅由 OnResult 观察，不发到输出。

## `*ParallelChain[T].Pool`

```go
func (b *ParallelChain[T]) Pool(p *pool.Pool[T]) *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L18) · [行为专题](../pipeline.md)：Pool 注入外部协程池，启用池化执行。

**源码说明与使用片段**（原文）：

Pool 注入外部协程池，启用池化执行。


**参数说明**：

- `p` (`*pool.Pool[T]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool(p)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 借用外部池并在阶段间复位等待状态，PoolAuto 在普通 Execute/Run 结束关闭所注入的池，DefaultPool 标记各阶段内部建池并关闭；设置本身不创建/关闭池。不可与外部池其他任务混用。ExecuteWithMeta/ExecutePipe 使用另一原生路径，不按此池配置运行或关闭池。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.pool = p
```

## `*ParallelChain[T].PoolAuto`

```go
func (b *ParallelChain[T]) PoolAuto(p *pool.Pool[T]) *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L25) · [行为专题](../pipeline.md)：PoolAuto 注入外部协程池，终端方法执行完成后自动 Close。

**源码说明与使用片段**（原文）：

PoolAuto 注入外部协程池，终端方法执行完成后自动 Close。


**参数说明**：

- `p` (`*pool.Pool[T]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.PoolAuto(p)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Pool 借用外部池并在阶段间复位等待状态，PoolAuto 在普通 Execute/Run 结束关闭所注入的池，DefaultPool 标记各阶段内部建池并关闭；设置本身不创建/关闭池。不可与外部池其他任务混用。ExecuteWithMeta/ExecutePipe 使用另一原生路径，不按此池配置运行或关闭池。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.pool = p
```

## `*ParallelChain[T].Run`

```go
func (b *ParallelChain[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_parallel.go#L84) · [行为专题](../pipeline.md)：Run 是 Execute 的别名。

**源码说明与使用片段**（原文）：

Run 是 Execute 的别名。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。

## `*ParallelChain[T].Shard`

```go
func (b *ParallelChain[T]) Shard(shards int) *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L39) · [行为专题](../pipeline.md)：Shard 设置水平分片数，shards <= 1 不启用分片。

**源码说明与使用片段**（原文）：

Shard 设置水平分片数，shards \<= 1 不启用分片。


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shard(shards)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：普通 Execute/Run 的路径优先级为 Pool、Shard>1、AutoScale、原生；同时配置时仅采用最先命中的路径。Shard<=1 不分片，DefaultShard=max(2,GOMAXPROCS)，AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置。ExecuteWithMeta/ExecutePipe 不采用这些路径；分片/AutoScale 的结果顺序不保证与输入一致。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.shards = shards
```

## `*ParallelChain[T].Worker`

```go
func (b *ParallelChain[T]) Worker(n int) *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_parallel.go#L67) · [行为专题](../pipeline.md)：Worker 覆盖所有阶段的并发度。

**源码说明与使用片段**（原文）：

Worker 覆盖所有阶段的并发度。


**参数说明**：

- `n` (`int`)：覆盖全部阶段的并发数；正数生效，非正数恢复各 Stage 的 Concurrency，再对非正阶段值使用 IO()。有限输入最多使用输入项数个 worker；外部池的容量还会限制实际并发。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：正数覆盖各阶段并发，非正值及 DefaultWorker 恢复 Stage.Concurrency，再对非正阶段值使用 IO()；有限输入通常取不超过当前项数的 worker。外部池还有自身容量限制，分片可能按每片重复并发配置；Worker 不负责调整外部池。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.concurrency = n
```

## `*PipelineBuilder[T].Execute`

```go
func (b *PipelineBuilder[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_builder.go#L75) · [行为专题](../pipeline.md)：Execute 依次执行各阶段，前一阶段输出作为后一阶段输入。

**源码说明与使用片段**（原文）：

Execute 依次执行各阶段，前一阶段输出作为后一阶段输入。 未启用 FailFast 时调用核心 Execute；启用后使用支持失败取消的内部执行路径。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Execute(fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。

## `*PipelineBuilder[T].ExecutePipe`

```go
func (b *PipelineBuilder[T]) ExecutePipe(fn func(context.Context, string, T) (T, error), bufSize int) <-chan core.Result[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L109) · [行为专题](../pipeline.md)：ExecutePipe 执行管道，通过 channel 返回最终阶段结果。

**源码说明与使用片段**（原文）：

ExecutePipe 执行管道，通过 channel 返回最终阶段结果。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecutePipe(fn, bufSize)
```

**生产建议**：持续消费结果并逐项处理 Err；为生产方设置可取消的 context，消费方提前退出时及时取消或协调其他消费者排空。Drain 回调保持短小，可能失败的外部写入应有明确重试或补偿；通道关闭后再统计已收到的结果。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。输出最终阶段 Result channel，部分模式先同步完成之前阶段，不能假定立即返回。持续消费并检查逐项 Err，提前停止时取消执行 context；默认缓冲有界，不保证容纳全部结果。Parallel 此入口走原生路径，忽略 Pool/Shard/AutoScale，不能依此自动关闭注入池。

## `*PipelineBuilder[T].ExecuteWithMeta`

```go
func (b *PipelineBuilder[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L101) · [行为专题](../pipeline.md)：ExecuteWithMeta 与 Execute 相同，但返回带阶段元信息的结果。

**源码说明与使用片段**（原文）：

ExecuteWithMeta 与 Execute 相同，但返回带阶段元信息的结果。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]ResultWithMeta[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecuteWithMeta(fn)
```

**生产建议**：按阶段数与输入项数估算历史结果内存，逐项检查阶段名和 Err；需要定位失败时保留中间记录，不能只检查最后一阶段。Stream 的标签使用末阶段名，诊断真实出错阶段时另配 OnResult。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。默认/Serial/Parallel 按阶段及输入索引追加记录，空阶段为空；支持 SkipOnError 的路径也为跳过项保留透传记录，Serial 不读取此字段。Stream 全部槽位均标记配置中的末阶段名，包含中途取消保留的失败，不能定位实际失败阶段；空阶段返回原输入和空阶段名，取消须检查 Occupied。最终成功可能掩盖此前失败。Parallel 此入口使用原生执行，忽略 Pool/Shard/AutoScale，池生命周期由调用方处理。

## `*PipelineBuilder[T].Flow`

```go
func (b *PipelineBuilder[T]) Flow() *FlowBuilder[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L67) · [行为专题](../pipeline.md)：Flow 切换到常驻管道模式，返回 FlowBuilder。

**源码说明与使用片段**（原文）：

Flow 切换到常驻管道模式，返回 FlowBuilder。 用于持续高频数据流处理场景，goroutine 常驻，通过 Build() 构建后 Submit() 使用。 PipelineBuilder 上已配置的 ctx 和 stages 会传递给 FlowBuilder。 Timeout/FailFast/OnResult 等一次性执行专属配置在 Flow 模式下被忽略。


**参数说明**：无显式参数。

**返回值签名**：`*FlowBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Flow()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只复制当前 context 和阶段列表到新的 FlowBuilder，不使用输入 items，也不继承 Timeout/FailFast/OnResult/MaxResults 等一次性配置。此时不启动任务，后续 Build/Run 才启动；FlowBuilder 缓冲初始 0，构造时使用 256。

## `*PipelineBuilder[T].Parallel`

```go
func (b *PipelineBuilder[T]) Parallel() *ParallelChain[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L52) · [行为专题](../pipeline.md)：Parallel 切换到并行模式，返回 ParallelChain。

**源码说明与使用片段**（原文）：

Parallel 切换到并行模式，返回 ParallelChain。 继承 PipelineBuilder 上已配置的所有公共参数，并暴露 Pool/Shard/AutoScale/Worker 方法。


**参数说明**：无显式参数。

**返回值签名**：`*ParallelChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Parallel()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：创建对应模式构建器，复制阶段列表、保留输入引用与公共配置；输入元素内部引用仍共享。Serial 每次执行一个回调，Parallel 每阶段完成后进入下一阶段，Stream 各阶段同时流转；各模式支持的专属配置/输出边界不同，切换后不并发修改共享输入。

## `*PipelineBuilder[T].Pipe`

```go
func (b *PipelineBuilder[T]) Pipe() *PipelinePipeBuilder[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L120) · [行为专题](../pipeline.md)：Pipe 切换为管道输出模式。

**源码说明与使用片段**（原文）：

Pipe 切换为管道输出模式。 返回 PipelinePipeBuilder，通过 .Buf(n).Run(fn).Receive() 获取结果 channel。


**参数说明**：无显式参数。

**返回值签名**：`*PipelinePipeBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pipe()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：创建输出构建器并绑定当前执行配置的浅副本，此时不执行。之后在源构建器替换 Context/Timeout/FailFast/OnResult 或追加 Stage 不会更新此输出构建器；输入切片、context 对象、外部池和部分引用配置仍共享，原父 context 的取消仍生效。每次 Run 使用当时可见的共享输入并创建独立通道，须等上次执行结束再修改输入。不能当作深拷贝，须持续消费并检查逐项错误；Serial 非最终阶段 FailFast 错误仅由 OnResult 观察，不发到输出。

## `*PipelineBuilder[T].Run`

```go
func (b *PipelineBuilder[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_builder.go#L96) · [行为专题](../pipeline.md)：Run 是 Execute 的别名。

**源码说明与使用片段**（原文）：

Run 是 Execute 的别名。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。

## `*PipelineBuilder[T].Serial`

```go
func (b *PipelineBuilder[T]) Serial() *SerialChain[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L46) · [行为专题](../pipeline.md)：Serial 切换到串行模式，返回 SerialChain。

**源码说明与使用片段**（原文）：

Serial 切换到串行模式，返回 SerialChain。 继承 PipelineBuilder 上已配置的所有公共参数（Timeout/FailFast/Logger/OnResult/Stage）。


**参数说明**：无显式参数。

**返回值签名**：`*SerialChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Serial()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：创建对应模式构建器，复制阶段列表、保留输入引用与公共配置；输入元素内部引用仍共享。Serial 每次执行一个回调，Parallel 每阶段完成后进入下一阶段，Stream 各阶段同时流转；各模式支持的专属配置/输出边界不同，切换后不并发修改共享输入。

## `*PipelineBuilder[T].Stream`

```go
func (b *PipelineBuilder[T]) Stream() *StreamChain[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L59) · [行为专题](../pipeline.md)：Stream 切换到一次性流水线模式，返回 StreamChain。

**源码说明与使用片段**（原文）：

Stream 切换到一次性流水线模式，返回 StreamChain。 所有阶段同时运转，元素流经阶段间 channel（无阶段 barrier）。 继承 PipelineBuilder 上已配置的所有公共参数，不暴露 Pool/Shard/AutoScale/Worker。


**参数说明**：无显式参数。

**返回值签名**：`*StreamChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：创建对应模式构建器，复制阶段列表、保留输入引用与公共配置；输入元素内部引用仍共享。Serial 每次执行一个回调，Parallel 每阶段完成后进入下一阶段，Stream 各阶段同时流转；各模式支持的专属配置/输出边界不同，切换后不并发修改共享输入。

## `*PipelinePipeBuilder[T].Buf`

```go
func (b *PipelinePipeBuilder[T]) Buf(n int) *PipelinePipeBuilder[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L134) · [行为专题](../pipeline.md)：Buf 设置 channel 缓冲大小（<=0 使用默认值）。

**源码说明与使用片段**（原文）：

Buf 设置 channel 缓冲大小（\<=0 使用默认值）。


**参数说明**：

- `n` (`int`)：Buf 设置 channel 缓冲大小（<=0 使用默认值）。

**返回值签名**：`*PipelinePipeBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Buf(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Buf/DefaultBuf 配置后续 Run 的输出 channel 缓冲，非正值由原执行路径确定；不能用缓冲代替持续消费。每次 Run 重新执行并返回独立通道，不缓存上次结果；输入引用仍共享，须等上次执行完全结束再修改。Run 调用 Pipe 时绑定的 ExecutePipe，部分模式先同步执行前面阶段。Serial 开启 FailFast 后非最终阶段失败会直接结束，不发送该失败项，须用创建 Pipe 前设置的 OnResult 观察；检查输出 Err 不能覆盖这些未发送的错误。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.buf = n
```

## `*PipelinePipeBuilder[T].DefaultBuf`

```go
func (b *PipelinePipeBuilder[T]) DefaultBuf() *PipelinePipeBuilder[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L140) · [行为专题](../pipeline.md)：DefaultBuf 重置 channel 缓冲大小为默认值（自动计算）。

**源码说明与使用片段**（原文）：

DefaultBuf 重置 channel 缓冲大小为默认值（自动计算）。


**参数说明**：无显式参数。

**返回值签名**：`*PipelinePipeBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBuf()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Buf/DefaultBuf 配置后续 Run 的输出 channel 缓冲，非正值由原执行路径确定；不能用缓冲代替持续消费。每次 Run 重新执行并返回独立通道，不缓存上次结果；输入引用仍共享，须等上次执行完全结束再修改。Run 调用 Pipe 时绑定的 ExecutePipe，部分模式先同步执行前面阶段。Serial 开启 FailFast 后非最终阶段失败会直接结束，不发送该失败项，须用创建 Pipe 前设置的 OnResult 观察；检查输出 Err 不能覆盖这些未发送的错误。

## `*PipelinePipeBuilder[T].DefaultLogger`

```go
func (b *PipelinePipeBuilder[T]) DefaultLogger() *PipelinePipeBuilder[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L152) · [行为专题](../pipeline.md)：DefaultLogger 重置为默认日志实现。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现。


**参数说明**：无显式参数。

**返回值签名**：`*PipelinePipeBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*PipelinePipeBuilder[T].Logger`

```go
func (b *PipelinePipeBuilder[T]) Logger(l core.Logger) *PipelinePipeBuilder[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L146) · [行为专题](../pipeline.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*PipelinePipeBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*PipelinePipeBuilder[T].Run`

```go
func (b *PipelinePipeBuilder[T]) Run(fn func(context.Context, string, T) (T, error)) *PipelinePipe[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L158) · [行为专题](../pipeline.md)：Run 启动管道，返回句柄。

**源码说明与使用片段**（原文）：

Run 启动管道，返回句柄。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`*PipelinePipe[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：在源构建器创建 Pipe 前设置 context 和结果观察，再配置有界缓冲；Run 后持续消费 Receive，提前退出时取消执行 context。需要中间失败明细时使用 OnResult，输出项仍须逐项检查 Err。

**注意事项**：Buf/DefaultBuf 配置后续 Run 的输出 channel 缓冲，非正值由原执行路径确定；不能用缓冲代替持续消费。每次 Run 重新执行并返回独立通道，不缓存上次结果；输入引用仍共享，须等上次执行完全结束再修改。Run 调用 Pipe 时绑定的 ExecutePipe，部分模式先同步执行前面阶段。Serial 开启 FailFast 后非最终阶段失败会直接结束，不发送该失败项，须用创建 Pipe 前设置的 OnResult 观察；检查输出 Err 不能覆盖这些未发送的错误。

## `*PipelinePipe[T].Drain`

```go
func (s *PipelinePipe[T]) Drain(fn func(core.Result[T]))
```

[实现](../../internal/pipeline/pipeline_builder.go#L174) · [行为专题](../pipeline.md)：Drain 通过回调逐条消费结果。

**源码说明与使用片段**（原文）：

Drain 通过回调逐条消费结果。


**参数说明**：

- `fn` (`func(core.Result[T])`)：在调用 Drain 的 goroutine 同步接收每个 Result 的消费函数，无返回值，不自动过滤 Err；函数返回后才读取下一项。传非 nil 回调；nil 仅在实际读到结果时 panic。回调 panic 原样传播，当前项已从通道取出且不会重放，不自动取消生产方；继续消费或取消须由调用方协调。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Drain(fn)
```

**生产建议**：持续消费结果并逐项处理 Err；为生产方设置可取消的 context，消费方提前退出时及时取消或协调其他消费者排空。Drain 回调保持短小，可能失败的外部写入应有明确重试或补偿；通道关闭后再统计已收到的结果。

**注意事项**：在调用方 goroutine 同步消费同一 channel，fn 返回后才读取下一项；取消不能强制中断已开始的消费回调。传非 nil 且及时返回的函数，nil 在读到结果时 panic，已关闭空通道不会调用它。panic 原样传播，当前项已经消费而不重放，不自动取消生产方，后者可能阻塞；可协调继续消费剩余项，或取消执行 context 后排空。与 Receive 并用竞争结果，不自动汇总业务错误，须检查 Err；取消后已缓冲结果仍可读取。

## `*PipelinePipe[T].Receive`

```go
func (s *PipelinePipe[T]) Receive() <-chan core.Result[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L169) · [行为专题](../pipeline.md)：Receive 返回结果 channel。

**源码说明与使用片段**（原文）：

Receive 返回结果 channel。


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Receive()
```

**生产建议**：持续消费结果并逐项处理 Err；为生产方设置可取消的 context，消费方提前退出时及时取消或协调其他消费者排空。Drain 回调保持短小，可能失败的外部写入应有明确重试或补偿；通道关闭后再统计已收到的结果。

**注意事项**：每次返回同一只读 channel，不复制或广播；多个消费者竞争结果而不会各获全量。须持续消费到关闭或取消执行 context，逐项检查 Err；channel 关闭只表示输出结束，取消时可能只得到部分结果。

## `*Pipeline[T].DefaultShard`

```go
func (p *Pipeline[T]) DefaultShard() *Pipeline[T]
```

[实现](../../internal/pipeline/pipeline.go#L444) · [行为专题](../pipeline.md)：DefaultShard 使用默认分片数（runtime.GOMAXPROCS(0)，最少 2）启用分片。

**源码说明与使用片段**（原文）：

DefaultShard 使用默认分片数（runtime.GOMAXPROCS(0)，最少 2）启用分片。


**参数说明**：无显式参数。

**返回值签名**：`*Pipeline[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShard()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Shard 原地保存片数，<=1 的有效片数为 1，DefaultShard 使用 max(2,GOMAXPROCS)，ShardCount 返回该有效片数；此时不创建 worker。只影响 Execute/ExecuteWithMeta 的分片路径，ExecutePipe 忽略它；不能与执行或配置修改并发。

## `*Pipeline[T].Execute`

```go
func (p *Pipeline[T]) Execute(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline.go#L464) · [行为专题](../pipeline.md)：Execute 执行管道。

**源码说明与使用片段**（原文）：

Execute 执行管道。 当前 shards \<= 1 时，行为与顶层 Execute() 一致（goroutine 分块处理，保持元素顺序）。 当 shards >= 2 时，每个阶段使用 MultiGroup 水平分片执行（不保证全局顺序）。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Execute(ctx, items, fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。Shard<=1 使用原生路径，阶段之间等待，Execute 按输入索引保序；Shard>1 每阶段用 MultiGroup，按片拼接而不保证全局顺序。检查逐项 Err，Execute 的顶层错误不汇总全部业务失败；Meta 追加所有阶段记录，空阶段返回空记录。SkipOnError 决定透传前错，分片执行继承 Group 的默认期限；配置与阶段切片不与执行竞争。

## `*Pipeline[T].ExecutePipe`

```go
func (p *Pipeline[T]) ExecutePipe(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
	bufSize int,
) <-chan core.Result[T]
```

[实现](../../internal/pipeline/pipeline.go#L697) · [行为专题](../pipeline.md)：ExecutePipe 委托自由函数 ExecutePipe，忽略 Pipeline 的 Shard 配置。

**源码说明与使用片段**（原文）：

ExecutePipe 委托自由函数 ExecutePipe，忽略 Pipeline 的 Shard 配置。 非最终阶段同步完成，最终阶段用 Group 全部等待后再逐条发送到 channel。

参数：

  - ctx：上下文
  - items：初始数据
  - fn：处理函数，接收 ctx、阶段名和当前元素
  - bufSize：channel 缓冲区大小，\<=0 时自动计算

返回的 channel 在最终阶段所有任务完成后自动关闭。


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：初始数据
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。
- `bufSize` (`int`)：channel 缓冲区大小，<=0 时自动计算

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecutePipe(ctx, items, fn, bufSize)
```

**生产建议**：持续消费结果并逐项处理 Err；为生产方设置可取消的 context，消费方提前退出时及时取消或协调其他消费者排空。Drain 回调保持短小，可能失败的外部写入应有明确重试或补偿；通道关闭后再统计已收到的结果。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。此入口直接调用自由函数 ExecutePipe，忽略当前 Shard 配置。空阶段或空输入返回已关闭空通道；前面阶段同步完成后才返回，最后阶段在后台 Group 全部 Wait 后输出，不保证逐任务完成时立即发送。最后阶段继承 Group 的全局默认任务期限；默认缓冲 min(项数,16384)。持续消费并检查 Err，提前退出须取消 ctx，取消会丢弃未发送结果，SkipOnError 决定是否透传前错。

## `*Pipeline[T].ExecuteWithMeta`

```go
func (p *Pipeline[T]) ExecuteWithMeta(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) []ResultWithMeta[T]
```

[实现](../../internal/pipeline/pipeline.go#L571) · [行为专题](../pipeline.md)：ExecuteWithMeta 带阶段元信息执行（分片模式下不保证全局顺序）。

**源码说明与使用片段**（原文）：

ExecuteWithMeta 带阶段元信息执行（分片模式下不保证全局顺序）。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]ResultWithMeta[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecuteWithMeta(ctx, items, fn)
```

**生产建议**：按阶段数与输入项数估算历史结果内存，逐项检查阶段名和 Err；需要定位失败时保留中间记录，不能只检查最后一阶段。Stream 的标签使用末阶段名，诊断真实出错阶段时另配 OnResult。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。Shard<=1 使用原生路径，阶段之间等待，Execute 按输入索引保序；Shard>1 每阶段用 MultiGroup，按片拼接而不保证全局顺序。检查逐项 Err，Execute 的顶层错误不汇总全部业务失败；Meta 追加所有阶段记录，空阶段返回空记录。SkipOnError 决定透传前错，分片执行继承 Group 的默认期限；配置与阶段切片不与执行竞争。

## `*Pipeline[T].Shard`

```go
func (p *Pipeline[T]) Shard(shards int) *Pipeline[T]
```

[实现](../../internal/pipeline/pipeline.go#L438) · [行为专题](../pipeline.md)：Shard 设置水平分片数，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Shard 设置水平分片数，返回自身以支持链式调用。 shards \<= 1 时不启用分片，行为与原始 Execute() 一致。


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。

**返回值签名**：`*Pipeline[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shard(shards)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Shard 原地保存片数，<=1 的有效片数为 1，DefaultShard 使用 max(2,GOMAXPROCS)，ShardCount 返回该有效片数；此时不创建 worker。只影响 Execute/ExecuteWithMeta 的分片路径，ExecutePipe 忽略它；不能与执行或配置修改并发。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.shards = shards
```

## `*Pipeline[T].ShardCount`

```go
func (p *Pipeline[T]) ShardCount() int
```

[实现](../../internal/pipeline/pipeline.go#L454) · [行为专题](../pipeline.md)：ShardCount 返回当前设置的分片数。

**源码说明与使用片段**（原文）：

ShardCount 返回当前设置的分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Shard 原地保存片数，<=1 的有效片数为 1，DefaultShard 使用 max(2,GOMAXPROCS)，ShardCount 返回该有效片数；此时不创建 worker。只影响 Execute/ExecuteWithMeta 的分片路径，ExecutePipe 忽略它；不能与执行或配置修改并发。

## `*SerialChain[T].Execute`

```go
func (s *SerialChain[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_serial.go#L18) · [行为专题](../pipeline.md)：Execute 串行执行所有阶段，前一阶段输出作为后一阶段输入。

**源码说明与使用片段**（原文）：

Execute 串行执行所有阶段，前一阶段输出作为后一阶段输入。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：逐项同步调用的阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；ExecutePipe 在后台逐项调用。panic 转为 PanicError 且值为零值。Serial 不读取 Stage.SkipOnError；FailFast 可停止后续调用，非最终阶段 Pipe 失败不会作为输出发送，须用 OnResult 观察；取消须回调协作。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Execute(fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。

## `*SerialChain[T].ExecutePipe`

```go
func (s *SerialChain[T]) ExecutePipe(fn func(context.Context, string, T) (T, error), bufSize int) <-chan core.Result[T]
```

[实现](../../internal/pipeline/pipeline_serial.go#L34) · [行为专题](../pipeline.md)：ExecutePipe 串行执行管道，通过 channel 返回最终阶段结果。

**源码说明与使用片段**（原文）：

ExecutePipe 串行执行管道，通过 channel 返回最终阶段结果。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：逐项同步调用的阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；ExecutePipe 在后台逐项调用。panic 转为 PanicError 且值为零值。Serial 不读取 Stage.SkipOnError；FailFast 可停止后续调用，非最终阶段 Pipe 失败不会作为输出发送，须用 OnResult 观察；取消须回调协作。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecutePipe(fn, bufSize)
```

**生产建议**：持续消费结果并逐项处理 Err；为生产方设置可取消的 context，消费方提前退出时及时取消或协调其他消费者排空。Drain 回调保持短小，可能失败的外部写入应有明确重试或补偿；通道关闭后再统计已收到的结果。

**注意事项**：后台串行执行，只发送最终阶段结果；非最终阶段遇到 FailFast 错误直接关闭输出，不发送该失败项，须用 OnResult 观察中间阶段错误。未开启 FailFast 时后续成功可覆盖前错。持续消费并检查逐项 Err，提前退出取消父 context；超时不强制终止业务，取消可丢弃未发送结果。

## `*SerialChain[T].ExecuteWithMeta`

```go
func (s *SerialChain[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T]
```

[实现](../../internal/pipeline/pipeline_serial.go#L29) · [行为专题](../pipeline.md)：ExecuteWithMeta 串行执行并返回带阶段元信息的结果。

**源码说明与使用片段**（原文）：

ExecuteWithMeta 串行执行并返回带阶段元信息的结果。 按阶段及输入顺序追加；空阶段返回空切片，不读取 Stage.SkipOnError。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：逐项同步调用的阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；ExecutePipe 在后台逐项调用。panic 转为 PanicError 且值为零值。Serial 不读取 Stage.SkipOnError；FailFast 可停止后续调用，非最终阶段 Pipe 失败不会作为输出发送，须用 OnResult 观察；取消须回调协作。

**返回值签名**：`[]ResultWithMeta[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecuteWithMeta(fn)
```

**生产建议**：按阶段数与输入项数估算历史结果内存，逐项检查阶段名和 Err；需要定位失败时保留中间记录，不能只检查最后一阶段。Stream 的标签使用末阶段名，诊断真实出错阶段时另配 OnResult。

**注意事项**：默认/Serial/Parallel 按阶段及输入索引追加记录，空阶段为空；支持 SkipOnError 的路径也为跳过项保留透传记录，Serial 不读取此字段。Stream 全部槽位均标记配置中的末阶段名，包含中途取消保留的失败，不能定位实际失败阶段；空阶段返回原输入和空阶段名，取消须检查 Occupied。最终成功可能掩盖此前失败。Parallel 此入口使用原生执行，忽略 Pool/Shard/AutoScale，池生命周期由调用方处理。

## `*SerialChain[T].Pipe`

```go
func (s *SerialChain[T]) Pipe() *PipelinePipeBuilder[T]
```

[实现](../../internal/pipeline/pipeline_serial.go#L39) · [行为专题](../pipeline.md)：Pipe 切换为管道输出模式（串行版）。

**源码说明与使用片段**（原文）：

Pipe 切换为管道输出模式（串行版）。


**参数说明**：无显式参数。

**返回值签名**：`*PipelinePipeBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pipe()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建输出构建器并绑定当前执行配置的浅副本，此时不执行。之后在源构建器替换 Context/Timeout/FailFast/OnResult 或追加 Stage 不会更新此输出构建器；输入切片、context 对象、外部池和部分引用配置仍共享，原父 context 的取消仍生效。每次 Run 使用当时可见的共享输入并创建独立通道，须等上次执行结束再修改输入。不能当作深拷贝，须持续消费并检查逐项错误；Serial 非最终阶段 FailFast 错误仅由 OnResult 观察，不发到输出。

## `*SerialChain[T].Run`

```go
func (s *SerialChain[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_serial.go#L23) · [行为专题](../pipeline.md)：Run 是 Execute 的别名。

**源码说明与使用片段**（原文）：

Run 是 Execute 的别名。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：逐项同步调用的阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；ExecutePipe 在后台逐项调用。panic 转为 PanicError 且值为零值。Serial 不读取 Stage.SkipOnError；FailFast 可停止后续调用，非最终阶段 Pipe 失败不会作为输出发送，须用 OnResult 观察；取消须回调协作。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。

## `*StreamChain[T].Execute`

```go
func (w *StreamChain[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_stream.go#L275) · [行为专题](../pipeline.md)：Execute 以流水线方式执行所有阶段。

**源码说明与使用片段**（原文）：

Execute 以流水线方式执行所有阶段。 Timeout/FailFast/OnResult 等公共配置生效。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Execute(fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在当前阶段记录零值和 ErrTaskGoexit，worker 以同一登记名额接替并继续处理后续输入。Stream 保留输入索引并设置 Occupied，已启用 FailFast 时取消且保留该失败项；Flow 计一次历史阶段失败并归还 Active，Close 继续排空。Flow 普通 panic 仍保留阶段输入值。后续未设置 SkipOnError 时可覆盖前错；取消仍可能丢弃未转发结果。生产应正常返回 error，此保证不覆盖任意通知回调异常。同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。

## `*StreamChain[T].ExecuteWithMeta`

```go
func (w *StreamChain[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T]
```

[实现](../../internal/pipeline/pipeline_stream.go#L288) · [行为专题](../pipeline.md)：ExecuteWithMeta 以流水线方式执行，返回带阶段元信息的结果。

**源码说明与使用片段**（原文）：

ExecuteWithMeta 以流水线方式执行，返回带阶段元信息的结果。 注意：流水线内部不追踪每阶段中间结果，仅包含最终阶段信息。 空阶段返回原输入及空阶段名；取消后须先检查每个槽位的 Occupied。 Stage 统一标注配置中的末阶段名，中途失败也不能据此定位实际失败阶段。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]ResultWithMeta[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ExecuteWithMeta(fn)
```

**生产建议**：按阶段数与输入项数估算历史结果内存，逐项检查阶段名和 Err；需要定位失败时保留中间记录，不能只检查最后一阶段。Stream 的标签使用末阶段名，诊断真实出错阶段时另配 OnResult。

**注意事项**：业务 runtime.Goexit 在当前阶段记录零值和 ErrTaskGoexit，worker 以同一登记名额接替并继续处理后续输入。Stream 保留输入索引并设置 Occupied，已启用 FailFast 时取消且保留该失败项；Flow 计一次历史阶段失败并归还 Active，Close 继续排空。Flow 普通 panic 仍保留阶段输入值。后续未设置 SkipOnError 时可覆盖前错；取消仍可能丢弃未转发结果。生产应正常返回 error，此保证不覆盖任意通知回调异常。默认/Serial/Parallel 按阶段及输入索引追加记录，空阶段为空；支持 SkipOnError 的路径也为跳过项保留透传记录，Serial 不读取此字段。Stream 全部槽位均标记配置中的末阶段名，包含中途取消保留的失败，不能定位实际失败阶段；空阶段返回原输入和空阶段名，取消须检查 Occupied。最终成功可能掩盖此前失败。Parallel 此入口使用原生执行，忽略 Pool/Shard/AutoScale，池生命周期由调用方处理。

## `*StreamChain[T].Run`

```go
func (w *StreamChain[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_stream.go#L280) · [行为专题](../pipeline.md)：Run 是 Execute 的别名。

**源码说明与使用片段**（原文）：

Run 是 Execute 的别名。


**参数说明**：

- `fn` (`func(context.Context, string, T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在当前阶段记录零值和 ErrTaskGoexit，worker 以同一登记名额接替并继续处理后续输入。Stream 保留输入索引并设置 Occupied，已启用 FailFast 时取消且保留该失败项；Flow 计一次历史阶段失败并归还 Active，Close 继续排空。Flow 普通 panic 仍保留阶段输入值。后续未设置 SkipOnError 时可覆盖前错；取消仍可能丢弃未转发结果。生产应正常返回 error，此保证不覆盖任意通知回调异常。同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。

## `*pipelineBase[T, S].Context`

```go
func (b *pipelineBase[T, S]) Context(ctx context.Context) S
```

[实现](../../internal/pipeline/pipeline_base.go#L123) · [行为专题](../pipeline.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存后续终端使用的非 nil context 并确保 trace_id，输入引用、阶段及其他配置保持；不影响已启动的输出流。构建器不带配置锁，配置不得与终端执行或其他修改竞争。

## `*pipelineBase[T, S].DefaultFailFast`

```go
func (b *pipelineBase[T, S]) DefaultFailFast() S
```

[实现](../../internal/pipeline/pipeline_base.go#L159) · [行为专题](../pipeline.md)：DefaultFailFast 关闭快速失败模式。

**源码说明与使用片段**（原文）：

DefaultFailFast 关闭快速失败模式。


**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultFailFast()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：FailFast 在错误后停止后续调度或取消执行派生 context，不能中断忽略取消的回调；具体终端对跳过项和输出的处理不同。DefaultFailFast 清除策略开关，不恢复已经取消的父 context；任务错误须逐项检查，不以顶层 nil error 判断全量成功。

## `*pipelineBase[T, S].DefaultLogger`

```go
func (b *pipelineBase[T, S]) DefaultLogger() S
```

[实现](../../internal/pipeline/pipeline_base.go#L171) · [行为专题](../pipeline.md)：DefaultLogger 重置为默认日志实现。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现。


**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*pipelineBase[T, S].DefaultOnResult`

```go
func (b *pipelineBase[T, S]) DefaultOnResult() S
```

[实现](../../internal/pipeline/pipeline_base.go#L183) · [行为专题](../pipeline.md)：DefaultOnResult 清除结果回调。

**源码说明与使用片段**（原文）：

DefaultOnResult 清除结果回调。


**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultOnResult()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：设置同步阶段结果观察函数，nil/DefaultOnResult 清除；并行路径可并发调用，须并发安全并快速返回。普通 Execute/Run 的 Serial/原生并行已清理单项 context 后才观察；默认无 FailFast、Shard/AutoScale/Stream 在任务内观察。Pool 无 FailFast 在阶段 Wait 后观察，有 FailFast 在任务内观察且拒绝结果也可调用。业务保存的 context 可能已取消，不能依赖它完成观察阶段的必需写入；观察阻塞会阻止执行完成，Timeout 不能强制中断。回调 panic 转为当前结果的 PanicError，可能覆盖原错误；Result 为值传递，引用字段内容仍共享。已进入业务的 Pipe 最终阶段、Shard 和 AutoScale 即使 runtime.Goexit 也先通知该阶段的 ErrTaskGoexit；父取消可能丢弃 Pipe 输出，但不跳过已进入业务的此通知。观察回调本身 Goexit 不会重放。Flow 不继承此观察函数。

## `*pipelineBase[T, S].DefaultStage`

```go
func (b *pipelineBase[T, S]) DefaultStage(name string) S
```

[实现](../../internal/pipeline/pipeline_base.go#L135) · [行为专题](../pipeline.md)：DefaultStage 使用默认 IO 并发度追加处理阶段。

**源码说明与使用片段**（原文）：

DefaultStage 使用默认 IO 并发度追加处理阶段。


**参数说明**：

- `name` (`string`)：阶段或诊断名称，用于定位执行步骤。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultStage(name)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：追加阶段而非替换，阶段名不保证唯一；DefaultStage 使用 IO()，非正 Stage 并发在执行时也回退 IO()。链式创建的 SkipOnError 为 false 且无设置入口，后续阶段可能覆盖此前错误；阶段间数据始终同一泛型类型。

## `*pipelineBase[T, S].DefaultTimeout`

```go
func (b *pipelineBase[T, S]) DefaultTimeout() S
```

[实现](../../internal/pipeline/pipeline_base.go#L147) · [行为专题](../pipeline.md)：DefaultTimeout 清除超时设置。

**源码说明与使用片段**（原文）：

DefaultTimeout 清除超时设置。


**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：正数限制一次终端执行的共享父 context，全部阶段共享同一绝对截止时间，后续阶段不会重新取得完整时长；DefaultTimeout 和非正值清除此覆盖，仍受父 context 约束。期限到达不清空业务返回值，不保证顶层 error 汇总超时，逐项检查 Err，Stream 还须检查 Occupied。已开始的业务及结果回调须返回才能完成清理，不能强制终止或保证方法准时返回。Flow 不继承此设置。

## `*pipelineBase[T, S].FailFast`

```go
func (b *pipelineBase[T, S]) FailFast() S
```

[实现](../../internal/pipeline/pipeline_base.go#L153) · [行为专题](../pipeline.md)：FailFast 启用快速失败模式：任一项失败立即取消其余任务。

**源码说明与使用片段**（原文）：

FailFast 启用快速失败模式：任一项失败立即取消其余任务。


**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：FailFast 在错误后停止后续调度或取消执行派生 context，不能中断忽略取消的回调；具体终端对跳过项和输出的处理不同。DefaultFailFast 清除策略开关，不恢复已经取消的父 context；任务错误须逐项检查，不以顶层 nil error 判断全量成功。

## `*pipelineBase[T, S].Logger`

```go
func (b *pipelineBase[T, S]) Logger(l core.Logger) S
```

[实现](../../internal/pipeline/pipeline_base.go#L165) · [行为专题](../pipeline.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*pipelineBase[T, S].MaxResults`

```go
func (b *pipelineBase[T, S]) MaxResults(n int) S
```

[实现](../../internal/pipeline/pipeline_base.go#L190) · [行为专题](../pipeline.md)：MaxResults 设置内部协程池的结果存储上限（n=0 无限，负数使用创建池时的全局默认值，初始为 100_000）。

**源码说明与使用片段**（原文）：

MaxResults 设置内部协程池的结果存储上限（n=0 无限，负数使用创建池时的全局默认值，初始为 100\_000）。 仅对 Pipeline 内部创建的 Pool 生效；外部注入的 Pool 不受影响。


**参数说明**：

- `n` (`int`)：MaxResults 设置内部协程池的结果存储上限（n=0 无限，负数使用创建池时的全局默认值，初始为 100_000）。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxResults(n)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只影响内部创建 Pool 的主结果保留上限，0 无限、负数保留创建时全局默认；不更改外部池，也不限制原生/分片/AutoScale 执行、最终结果切片或 Stream 的全部输入结果。Pool 上限可使后续阶段只处理保留结果，生产需按完整性要求配置并核对数量。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.maxResults = n
```

## `*pipelineBase[T, S].OnResult`

```go
func (b *pipelineBase[T, S]) OnResult(cb func(string, core.Result[T])) S
```

[实现](../../internal/pipeline/pipeline_base.go#L177) · [行为专题](../pipeline.md)：OnResult 设置阶段结果回调，每个元素在每个阶段完成后同步调用。

**源码说明与使用片段**（原文）：

OnResult 设置阶段结果回调，每个元素在每个阶段完成后同步调用。


**参数说明**：

- `cb` (`func(string, core.Result[T])`)：接收阶段名及该次阶段 Result 的同步通知；并行/流模式可并发调用，nil 清除回调。回调阻塞会延迟任务，panic 被转为该项 PanicError 并替换原错误。Flow 不使用本配置。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OnResult(cb)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：设置同步阶段结果观察函数，nil/DefaultOnResult 清除；并行路径可并发调用，须并发安全并快速返回。普通 Execute/Run 的 Serial/原生并行已清理单项 context 后才观察；默认无 FailFast、Shard/AutoScale/Stream 在任务内观察。Pool 无 FailFast 在阶段 Wait 后观察，有 FailFast 在任务内观察且拒绝结果也可调用。业务保存的 context 可能已取消，不能依赖它完成观察阶段的必需写入；观察阻塞会阻止执行完成，Timeout 不能强制中断。回调 panic 转为当前结果的 PanicError，可能覆盖原错误；Result 为值传递，引用字段内容仍共享。已进入业务的 Pipe 最终阶段、Shard 和 AutoScale 即使 runtime.Goexit 也先通知该阶段的 ErrTaskGoexit；父取消可能丢弃 Pipe 输出，但不跳过已进入业务的此通知。观察回调本身 Goexit 不会重放。Flow 不继承此观察函数。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.resultCb = cb
```

## `*pipelineBase[T, S].Stage`

```go
func (b *pipelineBase[T, S]) Stage(name string, concurrency int) S
```

[实现](../../internal/pipeline/pipeline_base.go#L129) · [行为专题](../pipeline.md)：Stage 追加一个处理阶段。

**源码说明与使用片段**（原文）：

Stage 追加一个处理阶段。


**参数说明**：

- `name` (`string`)：阶段或诊断名称，用于定位执行步骤。
- `concurrency` (`int`)：此阶段的并发 worker 数；非正值使用 core.IO()。串行模式忽略此值，Parallel 的 Worker 覆盖阶段值；有限输入路径最多使用输入项数个 worker，Flow 常驻指定数量。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stage(name, concurrency)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：追加阶段而非替换，阶段名不保证唯一；DefaultStage 使用 IO()，非正 Stage 并发在执行时也回退 IO()。链式创建的 SkipOnError 为 false 且无设置入口，后续阶段可能覆盖此前错误；阶段间数据始终同一泛型类型。

## `*pipelineBase[T, S].Timeout`

```go
func (b *pipelineBase[T, S]) Timeout(d time.Duration) S
```

[实现](../../internal/pipeline/pipeline_base.go#L141) · [行为专题](../pipeline.md)：Timeout 设置一次终端执行共享的上下文超时，所有阶段共享同一期限。

**源码说明与使用片段**（原文）：

Timeout 设置一次终端执行共享的上下文超时，所有阶段共享同一期限。 正数生效，非正值不限时；回调须响应上下文取消。Flow 不使用此配置。


**参数说明**：

- `d` (`time.Duration`)：一次终端执行整体的 context 超时，正数生效，非正值不限时；所有阶段共享此期限，Flow 忽略该配置。不会强制中止业务函数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：正数限制一次终端执行的共享父 context，全部阶段共享同一绝对截止时间，后续阶段不会重新取得完整时长；DefaultTimeout 和非正值清除此覆盖，仍受父 context 约束。期限到达不清空业务返回值，不保证顶层 error 汇总超时，逐项检查 Err，Stream 还须检查 Occupied。已开始的业务及结果回调须返回才能完成清理，不能强制终止或保证方法准时返回。Flow 不继承此设置。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.timeout = d
```

## `Execute`

```go
func Execute[T any](
	ctx context.Context,
	stages []Stage[T],
	initialItems []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline.go#L89) · [行为专题](../pipeline.md)：Execute 依次执行各阶段，前一个阶段的输出作为后一个阶段的输入。

**源码说明与使用片段**（原文）：

Execute 依次执行各阶段，前一个阶段的输出作为后一个阶段的输入。 每个阶段用分块并发的方式处理所有元素。

参数：

  - ctx：上下文
  - stages：阶段定义列表
  - initialItems：初始数据
  - fn：处理函数，接收 ctx、阶段名和当前元素，返回处理后的元素

使用示例：

	// 数据清洗管道：去重 -> 标准化 -> 校验
	stages := []pipeline.Stage[Record]{
	    {Name: "dedup", Concurrency: 2},
	    {Name: "normalize", Concurrency: 4},
	    {Name: "validate", Concurrency: 2},
	}
	results, err := pipeline.Execute(ctx, stages, records, func(ctx context.Context, stage string, r Record) (Record, error) {
	    switch stage {
	    case "dedup":
	        return dedupRecord(ctx, r)
	    case "normalize":
	        return normalizeRecord(ctx, r)
	    case "validate":
	        return validateRecord(ctx, r)
	    }
	    return r, nil
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `stages` (`[]Stage[T]`)：阶段定义列表
- `initialItems` (`[]T`)：初始数据
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pipeline.Execute[T](ctx, stages, initialItems, fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。按输入索引保序，每阶段全部完成后进入下一阶段；并发非正按 IO() 并限制到项数。顶层 error 当前为 nil，必须逐项检查 Err；panic 转为 PanicError。空阶段返回输入值结果，Occupied 不作为此入口完成依据；不主动按 ctx 停止调度，无 SkipOnError 的后续成功会覆盖前错，回调须响应取消。

## `ExecutePipe`

```go
func ExecutePipe[T any](
	ctx context.Context,
	stages []Stage[T],
	initialItems []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
	bufSize int,
) <-chan core.Result[T]
```

[实现](../../internal/pipeline/pipeline.go#L287) · [行为专题](../pipeline.md)：ExecutePipe 执行多阶段管道，通过 channel 返回最终阶段的结果。

**源码说明与使用片段**（原文）：

ExecutePipe 执行多阶段管道，通过 channel 返回最终阶段的结果。 非最终阶段分批并发并保序，调用返回前同步完成；最终阶段使用 Group， 等该阶段全部任务完成后再把结果逐条发送到 channel。

参数：

  - ctx：上下文
  - stages：阶段定义列表
  - initialItems：初始数据
  - fn：处理函数，接收 ctx、阶段名和当前元素
  - bufSize：channel 缓冲区大小，\<=0 时自动计算

返回的 channel 在最终阶段所有任务完成后自动关闭。消费者提前停止读取时应取消 ctx， 以解除被阻塞的结果发送；取消后未发送的结果可能丢弃。

使用示例：

	stages := []pipeline.Stage[Record]{
	    {Name: "parse", Concurrency: 4},
	    {Name: "validate", Concurrency: 2},
	}
	ch := pipeline.ExecutePipe(ctx, stages, records, func(ctx context.Context, stage string, r Record) (Record, error) {
	    switch stage {
	    case "parse":
	        return parseRecord(ctx, r)
	    case "validate":
	        return validateRecord(ctx, r)
	    }
	    return r, nil
	}, 1024)
	for r := range ch {
	    if r.Ok() {
	        saveRecord(r.Value)
	    }
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `stages` (`[]Stage[T]`)：阶段定义列表
- `initialItems` (`[]T`)：初始数据
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。
- `bufSize` (`int`)：channel 缓冲区大小，<=0 时自动计算

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
pipeline.ExecutePipe[T](ctx, stages, initialItems, fn, bufSize)
```

**生产建议**：持续消费结果并逐项处理 Err；为生产方设置可取消的 context，消费方提前退出时及时取消或协调其他消费者排空。Drain 回调保持短小，可能失败的外部写入应有明确重试或补偿；通道关闭后再统计已收到的结果。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。空阶段或空输入返回已关闭空通道；前面阶段同步完成后才返回，最后阶段在后台 Group 全部 Wait 后输出，不保证逐任务完成时立即发送。最后阶段继承 Group 的全局默认任务期限；默认缓冲 min(项数,16384)。持续消费并检查 Err，提前退出须取消 ctx，取消会丢弃未发送结果，SkipOnError 决定是否透传前错。

## `ExecuteStream`

```go
func ExecuteStream[T any](
	ctx context.Context,
	stages []Stage[T],
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline_stream.go#L57) · [行为专题](../pipeline.md)：ExecuteStream 一次性流水线执行：所有阶段同时运转，元素流经阶段间 channel。

**源码说明与使用片段**（原文）：

ExecuteStream 一次性流水线执行：所有阶段同时运转，元素流经阶段间 channel。

与 Execute() 的核心区别：

  - Execute：Stage 0 处理完全部元素 → Stage 1 开始 → Stage 2 开始（阶段间有 barrier）
  - ExecuteStream：Stage 1 在处理 item1 的同时 Stage 0 已在处理 item2（无 barrier）

与 Flow 的核心区别：

  - Flow：goroutine 常驻，Submit 持续接收，Results 流式输出
  - ExecuteStream：一次性传入全部元素，goroutine 执行完即销毁，返回完整结果切片

适用场景：元素处理耗时差异大时，快的元素可以提前穿透全程，避免慢元素阻塞整体。

参数：

  - ctx：上下文
  - stages：阶段定义列表
  - items：初始数据
  - fn：处理函数，接收 ctx、阶段名和当前元素，返回处理后的元素

使用示例：

	results, err := pipeline.ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item Data) (Data, error) {
	    switch stage {
	    case "parse":
	        return parse(ctx, item)
	    case "enrich":
	        return enrich(ctx, item)
	    }
	    return item, nil
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `stages` (`[]Stage[T]`)：阶段定义列表
- `items` (`[]T`)：初始数据
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pipeline.ExecuteStream[T](ctx, stages, items, fn)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在当前阶段记录零值和 ErrTaskGoexit，worker 以同一登记名额接替并继续处理后续输入。Stream 保留输入索引并设置 Occupied，已启用 FailFast 时取消且保留该失败项；Flow 计一次历史阶段失败并归还 Active，Close 继续排空。Flow 普通 panic 仍保留阶段输入值。后续未设置 SkipOnError 时可覆盖前错；取消仍可能丢弃未转发结果。生产应正常返回 error，此保证不覆盖任意通知回调异常。各阶段并行流转，返回切片长度等于输入长度并按原索引收集；空阶段返回输入值且 Occupied=true，空阶段或空输入直接返回 nil error 而不检查取消。其他路径 ctx 取消可提前停止发送，最终 error 为 ctx.Err()，未完成槽位须核对 Occupied；逐项业务错误不自动变成顶层 error。SkipOnError 决定透传前错，业务 panic 转为 PanicError，等待仍依赖回调及时返回。

## `ExecuteWithGroup`

```go
func ExecuteWithGroup[T any](
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, item T) (T, error),
	concurrency int,
) ([]core.Result[T], error)
```

[实现](../../internal/pipeline/pipeline.go#L236) · [行为专题](../pipeline.md)：ExecuteWithGroup 使用 Group 执行所有项并等待，按结果记录顺序返回。

**源码说明与使用片段**（原文）：

ExecuteWithGroup 使用 Group 执行所有项并等待，按结果记录顺序返回。 第二返回值当前为 nil；调用方须检查返回的每项 Result.Err。

参数：

  - ctx：上下文，控制全局取消
  - items：待处理的元素列表
  - fn：处理函数，接收 ctx 和元素，返回处理结果
  - concurrency：并发度

使用示例：

	// 用 Group 并发处理所有元素
	results, err := pipeline.ExecuteWithGroup(ctx, items, func(ctx context.Context, item string) (string, error) {
	    return processItem(ctx, item)
	}, 8)
	if err != nil {
	    log.Printf("处理出错: %v", err)
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文，控制全局取消
- `items` (`[]T`)：待处理的元素列表
- `fn` (`func(ctx context.Context, item T) (T, error)`)：接收执行 context 和当前元素，返回该项值及错误，不接收阶段名；可并发调用，panic 转为 PanicError。结果按记录完成顺序返回，顶层 error 不汇总业务错误，必须逐项检查 Err。
- `concurrency` (`int`)：并发度

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
pipeline.ExecuteWithGroup[T](ctx, items, fn, concurrency)
```

**生产建议**：按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。创建 Group 执行并同步 Wait，显式清除其任务超时，非正并发按 Group 的 1；结果按记录完成顺序，不保证输入顺序。末尾 error 当前固定 nil，提交和业务错误需从逐项结果检查；panic 转为 PanicError。没有 FailFast 或回滚，回调须响应传入 context。

## `ExecuteWithMeta`

```go
func ExecuteWithMeta[T any](
	ctx context.Context,
	stages []Stage[T],
	initialItems []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) []ResultWithMeta[T]
```

[实现](../../internal/pipeline/pipeline.go#L158) · [行为专题](../pipeline.md)：ExecuteWithMeta 和 Execute 类似，但返回带 stage 信息的 ResultWithMeta。

**源码说明与使用片段**（原文）：

ExecuteWithMeta 和 Execute 类似，但返回带 stage 信息的 ResultWithMeta。 每个阶段处理后的每个元素都会生成一个 ResultWithMeta 记录，便于追踪每项在哪一阶段产生。

参数：

  - ctx：上下文
  - stages：阶段定义列表
  - initialItems：初始数据
  - fn：处理函数，接收 ctx、阶段名和当前元素，返回处理后的元素

使用示例：

	// 追踪每个元素在各阶段的处理情况
	metaResults := pipeline.ExecuteWithMeta(ctx, stages, items, fn)
	for _, mr := range metaResults {
	    fmt.Printf("阶段=%s 值=%v 错误=%v\n", mr.Stage, mr.Value, mr.Err)
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `stages` (`[]Stage[T]`)：阶段定义列表
- `initialItems` (`[]T`)：初始数据
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]ResultWithMeta[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
pipeline.ExecuteWithMeta[T](ctx, stages, initialItems, fn)
```

**生产建议**：按阶段数与输入项数估算历史结果内存，逐项检查阶段名和 Err；需要定位失败时保留中间记录，不能只检查最后一阶段。Stream 的标签使用末阶段名，诊断真实出错阶段时另配 OnResult。

**注意事项**：业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。逐阶段、逐输入索引追加记录，通常长度为阶段数×输入数；空阶段返回空结果。同步等待阶段结束，panic 转为 PanicError；SkipOnError 的跳过项也追加透传记录，并不表示该阶段调用了业务。否则后续成功可覆盖前错。需要额外内存保存全部阶段记录，不主动按 ctx 停止调度，逐项检查 Err。

## `NewFlow`

```go
func NewFlow[T any](
	ctx context.Context,
	cfg FlowConfig[T],
	fn func(ctx context.Context, stage string, item T) (T, error),
) *Flow[T]
```

[实现](../../internal/pipeline/pipeline_flow.go#L118) · [行为专题](../pipeline.md)：NewFlow 创建 Flow。

**源码说明与使用片段**（原文）：

NewFlow 创建 Flow。 ctx 为管道级别的上下文，Close 时自动取消。 fn 为每阶段共用的处理函数，签名与 Execute() 完全一致。

参数：

  - ctx：管道上下文，控制全局生命周期
  - cfg：管道配置（阶段列表、缓冲大小）
  - fn：处理函数，接收 ctx、阶段名、当前元素，返回处理后的元素和可能的错误

返回值：

  - \*Flow\[T]：可立即使用的管道实例


**参数说明**：

- `ctx` (`context.Context`)：管道上下文，控制全局生命周期
- `cfg` (`FlowConfig[T]`)：管道配置（阶段列表、缓冲大小）
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：各阶段 worker 调用的处理函数：接收 Flow 的 context、阶段名和当前元素，返回下一阶段的值及错误，可并发调用。panic 转为 PanicError 并保留该阶段输入值；显式 Stage.SkipOnError=true 时跳过前错，否则后续成功可覆盖它。链式 Stage 未提供该字段设置入口；取消须回调协作。

**返回值签名**：`*Flow[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
pipeline.NewFlow[T](ctx, cfg, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务 runtime.Goexit 在当前阶段记录零值和 ErrTaskGoexit，worker 以同一登记名额接替并继续处理后续输入。Stream 保留输入索引并设置 Occupied，已启用 FailFast 时取消且保留该失败项；Flow 计一次历史阶段失败并归还 Active，Close 继续排空。Flow 普通 panic 仍保留阶段输入值。后续未设置 SkipOnError 时可覆盖前错；取消仍可能丢弃未转发结果。生产应正常返回 error，此保证不覆盖任意通知回调异常。立即启动各阶段常驻 worker，阶段为空或 ctx 为 nil 会 panic；非正阶段并发按 IO()，非正缓冲按 256。阶段字段构造时复制，fn 可并发调用且业务 panic 转为错误。持续消费 Results，最终合并 Close 返回的残留；取消可能丢弃转发结果，回调忽略取消时关闭仍可阻塞。

## `NewPipeline`

```go
func NewPipeline[T any](stages []Stage[T]) *Pipeline[T]
```

[实现](../../internal/pipeline/pipeline.go#L432) · [行为专题](../pipeline.md)：NewPipeline 创建管道实例。

**源码说明与使用片段**（原文）：

NewPipeline 创建管道实例。


**参数说明**：

- `stages` (`[]Stage[T]`)：按执行顺序排列的阶段定义，每项含 Name、Concurrency、SkipOnError；Flow 要求至少一个阶段。

**返回值签名**：`*Pipeline[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
pipeline.NewPipeline[T](stages)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存 stages 切片引用，不复制、不启动 worker；外部修改阶段会影响后续执行，必须同步。初始不分片，Shard>1 才启用分片；空阶段的 Execute 返回原始元素结果，ExecutePipe 返回已关闭的空通道。

## `NewPipelineBuilder`

```go
func NewPipelineBuilder[T any](items []T) *PipelineBuilder[T]
```

[实现](../../internal/pipeline/pipeline_builder.go#L38) · [行为专题](../pipeline.md)：NewPipelineBuilder 创建管道构建器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewPipelineBuilder 创建管道构建器，默认使用 context.Background()。


**参数说明**：

- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*PipelineBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
pipeline.NewPipelineBuilder[T](items)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存输入切片引用并创建可变构建器，context 初始 Background，此时不启动 worker；元素内部引用也共享。终端执行期间不能修改共享输入或构建器，阶段列表在后续 Stage 调用中追加，空阶段规则由终端入口决定。

