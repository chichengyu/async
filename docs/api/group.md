# group 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [group](../group.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*GroupBuilder[T].AutoScale`

```go
func (b *GroupBuilder[T]) AutoScale(config *core.AutoScaleConfig) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L140) · [行为专题](../group.md)：AutoScale 保存自动扩缩容配置；nil 清除本构建器配置，Build 时不启用。

**源码说明与使用片段**（原文）：

AutoScale 保存自动扩缩容配置；nil 清除本构建器配置，Build 时不启用。 需默认配置时使用 DefaultAutoScale。


**参数说明**：

- `config` (`*core.AutoScaleConfig`)：保存供创建执行器时使用的自动扩缩容配置指针；nil 清除构建器中的配置、不启用扩缩容。DefaultAutoScale 保存默认配置；配置指针及字段不得在执行期间并发修改。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AutoScale(config)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：构建器保存配置指针，Build 时由 EnableAutoScale 复制并补齐默认值；Build 前对该指针的修改会影响后续构建，须同步。AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置；并非 Default 前缀都表示关闭。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.autoScale = config
```

## `*GroupBuilder[T].Build`

```go
func (b *GroupBuilder[T]) Build() *Group[T]
```

[实现](../../internal/group/group_builder.go#L172) · [行为专题](../group.md)：Build 创建并配置 Group，返回后调用方需自行管理生命周期（手动 Close / Wait）。

**源码说明与使用片段**（原文）：

Build 创建并配置 Group，返回后调用方需自行管理生命周期（手动 Close / Wait）。


**参数说明**：无显式参数。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次创建独立 Group；终端使用时按本次构建器字段配置，正数 Timeout/SubmitTimeout/Streaming 才应用。调用方最终 Wait/Close，普通 Go 的结果顺序不保证；注入的外部池仍由外层所有者关闭。不要并发修改构建器或与 Build 竞争。

## `*GroupBuilder[T].Context`

```go
func (b *GroupBuilder[T]) Context(ctx context.Context) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L64) · [行为专题](../group.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存非 nil 父 context 并确保 trace_id，后续 Build 的 Group context 与 Run 的作用域 context 由此派生；任务提交仍应传递该 context。取消只请求业务退出，配置不影响已经构建的实例，也不能与构建或 Run 并发。MultiGroup 未启用 FailFast 时不把此字段绑定到各片内部 context。

## `*GroupBuilder[T].DefaultAutoScale`

```go
func (b *GroupBuilder[T]) DefaultAutoScale() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L146) · [行为专题](../group.md)：DefaultAutoScale 使用默认配置启用自动扩缩容。

**源码说明与使用片段**（原文）：

DefaultAutoScale 使用默认配置启用自动扩缩容。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultAutoScale()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：构建器保存配置指针，Build 时由 EnableAutoScale 复制并补齐默认值；Build 前对该指针的修改会影响后续构建，须同步。AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置；并非 Default 前缀都表示关闭。

## `*GroupBuilder[T].DefaultLogger`

```go
func (b *GroupBuilder[T]) DefaultLogger() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L158) · [行为专题](../group.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*GroupBuilder[T].DefaultPool`

```go
func (b *GroupBuilder[T]) DefaultPool() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L167) · [行为专题](../group.md)：DefaultPool 清除外部 Pool 引用，恢复由 Group 自有 worker 直接执行任务。

**源码说明与使用片段**（原文）：

DefaultPool 清除外部 Pool 引用，恢复由 Group 自有 worker 直接执行任务。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPool()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只影响后续 Build/Run；Pool(nil) 与 DefaultPool 清除外部池引用。外部池归调用方管理，Group 的等待和 Reset 不负责复位外部池；外部池自身的超时和队列配置仍可能影响执行。

## `*GroupBuilder[T].DefaultResultCallback`

```go
func (b *GroupBuilder[T]) DefaultResultCallback() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L133) · [行为专题](../group.md)：DefaultResultCallback 取消结果回调。

**源码说明与使用片段**（原文）：

DefaultResultCallback 取消结果回调。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultResultCallback()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存回调供后续 Build/Run 使用，nil 和 DefaultResultCallback 清除；实例在结果记录路径同步调用，可能由不同任务并发触发。回调须并发安全、及时返回，不在其中等待/关闭所属 Group，不把它当作可以修改已保存 Result 的钩子。

## `*GroupBuilder[T].DefaultStreaming`

```go
func (b *GroupBuilder[T]) DefaultStreaming() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L120) · [行为专题](../group.md)：DefaultStreaming 清除流式配置，Build 时不启用流式结果。

**源码说明与使用片段**（原文）：

DefaultStreaming 清除流式配置，Build 时不启用流式结果。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultStreaming()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：正数在后续 Build/Run 创建结果流，非正数和 DefaultStreaming 不启用；实例 WithStreaming(0) 则使用 worker×2，边界不同。流需持续消费；等待时无法刷入流的余项可能丢弃，完整结果应由 Wait 获取。

## `*GroupBuilder[T].DefaultSubmitTimeout`

```go
func (b *GroupBuilder[T]) DefaultSubmitTimeout() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L102) · [行为专题](../group.md)：DefaultSubmitTimeout 使用默认提交超时。

**源码说明与使用片段**（原文）：

DefaultSubmitTimeout 使用默认提交超时。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultSubmitTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存等待任务入队的期限，仅正数在终端创建时应用；DefaultSubmitTimeout 清零，恢复新 Group 的无显式提交计时器状态。不是业务执行期限，仍受提交 context、关闭和 FailFast 影响，必须处理提交错误。

## `*GroupBuilder[T].DefaultTimeout`

```go
func (b *GroupBuilder[T]) DefaultTimeout() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L90) · [行为专题](../group.md)：DefaultTimeout 使用默认超时（core.GetDefaultTimeout()）。

**源码说明与使用片段**（原文）：

DefaultTimeout 使用默认超时（core.GetDefaultTimeout()）。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Timeout 保存原值，DefaultTimeout 读取并保存调用当时全局默认值；Build/Run 仅把正数覆盖到新 Group，非正数保留创建时的全局期限，因此 Timeout(0) 不能清除非零全局默认。需明确取消期限时在实例提交前 WithTimeout(0)，业务仍须响应 context。

## `*GroupBuilder[T].DefaultWorker`

```go
func (b *GroupBuilder[T]) DefaultWorker() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L78) · [行为专题](../group.md)：DefaultWorker 使用默认并发数（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultWorker 使用默认并发数（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存每个 Group 的并发数，非正值在 NewGroup 按 1；Group/GroupVoid 初始值和 DefaultWorker 显式使用调用时 IO()。GroupMulti 初始值为 0，因此未设置时每片只有 1；分片后总并发乘以片数。不要与终端执行并发修改构建器。

## `*GroupBuilder[T].FailFast`

```go
func (b *GroupBuilder[T]) FailFast() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L108) · [行为专题](../group.md)：FailFast 启用快速失败模式。

**源码说明与使用片段**（原文）：

FailFast 启用快速失败模式。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：标记后续构建的 Group 启用失败取消，当前构建器没有反向关闭开关；作用域 Run 使用派生 context。只请求取消，不强制结束回调，也不使 Run 自动返回业务错误；仍需处理提交错误并在等待后检查结果。

## `*GroupBuilder[T].Logger`

```go
func (b *GroupBuilder[T]) Logger(l core.Logger) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L152) · [行为专题](../group.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*GroupBuilder[T].Pool`

```go
func (b *GroupBuilder[T]) Pool(p *pool.Pool[T]) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L164) · [行为专题](../group.md)：Pool 注入外部协程池，构建的 Group 将使用该池执行任务。

**源码说明与使用片段**（原文）：

Pool 注入外部协程池，构建的 Group 将使用该池执行任务。


**参数说明**：

- `p` (`*pool.Pool[T]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool(p)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只影响后续 Build/Run；Pool(nil) 与 DefaultPool 清除外部池引用。外部池归调用方管理，Group 的等待和 Reset 不负责复位外部池；外部池自身的超时和队列配置仍可能影响执行。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.extPool = p
```

## `*GroupBuilder[T].ResultCallback`

```go
func (b *GroupBuilder[T]) ResultCallback(fn func(core.Result[T])) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L127) · [行为专题](../group.md)：ResultCallback 设置结果记录路径的同步回调。

**源码说明与使用片段**（原文）：

ResultCallback 设置结果记录路径的同步回调。 worker、提交取消或拒绝路径均可能调用；最早的 waited/waiting 拒绝不通知。


**参数说明**：

- `fn` (`func(core.Result[T])`)：接收记录结果路径的同步通知，nil 清除；worker、取消或拒绝路径可并发触发，但最早的 waited/waiting 预检查直接追加错误而不通知。不能按回调数统计全部尝试；须并发安全、快速返回，panic 被记录，不在通知中 Wait/Close 本组。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ResultCallback(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存回调供后续 Build/Run 使用，nil 和 DefaultResultCallback 清除；实例在结果记录路径同步调用，可能由不同任务并发触发。回调须并发安全、及时返回，不在其中等待/关闭所属 Group，不把它当作可以修改已保存 Result 的钩子。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.resultCb = fn
```

## `*GroupBuilder[T].Run`

```go
func (b *GroupBuilder[T]) Run(fn func(ctx context.Context, g *Group[T]) error) error
```

[实现](../../internal/group/group_builder.go#L221) · [行为专题](../group.md)：Run 创建→执行→关闭。

**源码说明与使用片段**（原文）：

Run 创建→执行→关闭。 内部流程：Build() → 调用 fn → Close()（Wait + 清理）。 未设置 Worker 时使用 IO 并发度。

示例：

	async.Group[string]().Context(ctx).
	    Worker(10).
	    Timeout(5 * time.Second).
	    Run(func(ctx context.Context, g *async.GroupInstance[string]) error {
	        for _, item := range items {
	            if err := g.Go(ctx, func(ctx context.Context) (string, error) {
	                return process(item)
	            }); err != nil { return err }
	        }
	        g.Wait()
	        return g.FirstError()
	    })


**参数说明**：

- `fn` (`func(ctx context.Context, g *Group[T]) error`)：同步接收本次执行器和上下文（以签名为准）的作用域函数；其返回后触发关闭，返回的 error 向外传播。需处理全部任务时在函数内等待并检查结果，避免把执行器带出作用域继续使用。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：作用域回调返回后自动关闭，但 Run 的 error 仅来自该回调，不自动汇总任务失败。须在回调内检查提交错误并 Wait 后检查结果或 FirstError；回调 panic 直接传播，defer 仍执行关闭。不要在 Run 返回后继续使用实例提交任务。

## `*GroupBuilder[T].Streaming`

```go
func (b *GroupBuilder[T]) Streaming(buf int) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L114) · [行为专题](../group.md)：Streaming 设置流式结果通道缓冲大小；正数在 Build 时启用，非正数不启用。

**源码说明与使用片段**（原文）：

Streaming 设置流式结果通道缓冲大小；正数在 Build 时启用，非正数不启用。


**参数说明**：

- `buf` (`int`)：构建器保存的结果通道缓冲容量；仅正数在 Build 时启用流，非正数不启用。实例 WithStreaming(0) 则启用 worker×2 缓冲，二者边界不同。流可能丢结果，须及时消费。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Streaming(buf)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：正数在后续 Build/Run 创建结果流，非正数和 DefaultStreaming 不启用；实例 WithStreaming(0) 则使用 worker×2，边界不同。流需持续消费；等待时无法刷入流的余项可能丢弃，完整结果应由 Wait 获取。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.streaming = buf
```

## `*GroupBuilder[T].SubmitTimeout`

```go
func (b *GroupBuilder[T]) SubmitTimeout(d time.Duration) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L96) · [行为专题](../group.md)：SubmitTimeout 设置获取并发槽位的等待超时。

**源码说明与使用片段**（原文）：

SubmitTimeout 设置获取并发槽位的等待超时。


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitTimeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存等待任务入队的期限，仅正数在终端创建时应用；DefaultSubmitTimeout 清零，恢复新 Group 的无显式提交计时器状态。不是业务执行期限，仍受提交 context、关闭和 FailFast 影响，必须处理提交错误。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.submitTimeout = d
```

## `*GroupBuilder[T].Timeout`

```go
func (b *GroupBuilder[T]) Timeout(d time.Duration) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L84) · [行为专题](../group.md)：Timeout 设置每个任务的超时时间。

**源码说明与使用片段**（原文）：

Timeout 设置每个任务的超时时间。


**参数说明**：

- `d` (`time.Duration`)：创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Timeout 保存原值，DefaultTimeout 读取并保存调用当时全局默认值；Build/Run 仅把正数覆盖到新 Group，非正数保留创建时的全局期限，因此 Timeout(0) 不能清除非零全局默认。需明确取消期限时在实例提交前 WithTimeout(0)，业务仍须响应 context。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.timeout = d
```

## `*GroupBuilder[T].Worker`

```go
func (b *GroupBuilder[T]) Worker(n int) *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L72) · [行为专题](../group.md)：Worker 设置最大并发数。

**源码说明与使用片段**（原文）：

Worker 设置最大并发数。


**参数说明**：

- `n` (`int`)：每个 Group 的最大同时执行数；非正值在构造时使用 1。GroupBuilder 默认已保存 IO()，MultiGroupBuilder 零值则使用 1；分片模式按每片计算，总容量乘以片数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存每个 Group 的并发数，非正值在 NewGroup 按 1；Group/GroupVoid 初始值和 DefaultWorker 显式使用调用时 IO()。GroupMulti 初始值为 0，因此未设置时每片只有 1；分片后总并发乘以片数。不要与终端执行并发修改构建器。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.concurrency = n
```

## `*GroupNoResultBuilder.AutoScale`

```go
func (b *GroupNoResultBuilder) AutoScale(config *core.AutoScaleConfig) *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L115) · [行为专题](../group.md)：AutoScale 保存自动扩缩容配置；nil 清除本构建器配置，Build 时不启用。

**源码说明与使用片段**（原文）：

AutoScale 保存自动扩缩容配置；nil 清除本构建器配置，Build 时不启用。 需默认配置时使用 DefaultAutoScale。


**参数说明**：

- `config` (`*core.AutoScaleConfig`)：保存供创建执行器时使用的自动扩缩容配置指针；nil 清除构建器中的配置、不启用扩缩容。DefaultAutoScale 保存默认配置；配置指针及字段不得在执行期间并发修改。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AutoScale(config)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：构建器保存配置指针，Build 时由 EnableAutoScale 复制并补齐默认值；Build 前对该指针的修改会影响后续构建，须同步。AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置；并非 Default 前缀都表示关闭。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.autoScale = config
```

## `*GroupNoResultBuilder.Build`

```go
func (b *GroupNoResultBuilder) Build() *NoResult
```

[实现](../../internal/group/noresult_builder.go#L150) · [行为专题](../group.md)：Build 创建并配置 NoResult，返回后调用方需自行管理生命周期。

**源码说明与使用片段**（原文）：

Build 创建并配置 NoResult，返回后调用方需自行管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*NoResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：每次创建独立 Group；终端使用时按本次构建器字段配置，正数 Timeout/SubmitTimeout/Streaming 才应用。调用方最终 Wait/Close，普通 Go 的结果顺序不保证；注入的外部池仍由外层所有者关闭。不要并发修改构建器或与 Build 竞争。

## `*GroupNoResultBuilder.Context`

```go
func (b *GroupNoResultBuilder) Context(ctx context.Context) *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L52) · [行为专题](../group.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存非 nil 父 context 并确保 trace_id，后续 Build 的 Group context 与 Run 的作用域 context 由此派生；任务提交仍应传递该 context。取消只请求业务退出，配置不影响已经构建的实例，也不能与构建或 Run 并发。MultiGroup 未启用 FailFast 时不把此字段绑定到各片内部 context。

## `*GroupNoResultBuilder.DefaultAutoScale`

```go
func (b *GroupNoResultBuilder) DefaultAutoScale() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L121) · [行为专题](../group.md)：DefaultAutoScale 使用默认配置启用自动扩缩容。

**源码说明与使用片段**（原文）：

DefaultAutoScale 使用默认配置启用自动扩缩容。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultAutoScale()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：构建器保存配置指针，Build 时由 EnableAutoScale 复制并补齐默认值；Build 前对该指针的修改会影响后续构建，须同步。AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置；并非 Default 前缀都表示关闭。

## `*GroupNoResultBuilder.DefaultLogger`

```go
func (b *GroupNoResultBuilder) DefaultLogger() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L133) · [行为专题](../group.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*GroupNoResultBuilder.DefaultPool`

```go
func (b *GroupNoResultBuilder) DefaultPool() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L145) · [行为专题](../group.md)：DefaultPool 清除外部 Pool 引用，恢复由 Group 自有 worker 直接执行任务。

**源码说明与使用片段**（原文）：

DefaultPool 清除外部 Pool 引用，恢复由 Group 自有 worker 直接执行任务。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPool()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只影响后续 Build/Run；Pool(nil) 与 DefaultPool 清除外部池引用。外部池归调用方管理，Group 的等待和 Reset 不负责复位外部池；外部池自身的超时和队列配置仍可能影响执行。

## `*GroupNoResultBuilder.DefaultStreaming`

```go
func (b *GroupNoResultBuilder) DefaultStreaming() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L108) · [行为专题](../group.md)：DefaultStreaming 清除流式配置，Build 时不启用流式结果。

**源码说明与使用片段**（原文）：

DefaultStreaming 清除流式配置，Build 时不启用流式结果。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultStreaming()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：正数在后续 Build/Run 创建结果流，非正数和 DefaultStreaming 不启用；实例 WithStreaming(0) 则使用 worker×2，边界不同。流需持续消费；等待时无法刷入流的余项可能丢弃，完整结果应由 Wait 获取。

## `*GroupNoResultBuilder.DefaultSubmitTimeout`

```go
func (b *GroupNoResultBuilder) DefaultSubmitTimeout() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L90) · [行为专题](../group.md)：DefaultSubmitTimeout 使用默认提交超时。

**源码说明与使用片段**（原文）：

DefaultSubmitTimeout 使用默认提交超时。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultSubmitTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存等待任务入队的期限，仅正数在终端创建时应用；DefaultSubmitTimeout 清零，恢复新 Group 的无显式提交计时器状态。不是业务执行期限，仍受提交 context、关闭和 FailFast 影响，必须处理提交错误。

## `*GroupNoResultBuilder.DefaultTimeout`

```go
func (b *GroupNoResultBuilder) DefaultTimeout() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L78) · [行为专题](../group.md)：DefaultTimeout 使用默认超时。

**源码说明与使用片段**（原文）：

DefaultTimeout 使用默认超时。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Timeout 保存原值，DefaultTimeout 读取并保存调用当时全局默认值；Build/Run 仅把正数覆盖到新 Group，非正数保留创建时的全局期限，因此 Timeout(0) 不能清除非零全局默认。需明确取消期限时在实例提交前 WithTimeout(0)，业务仍须响应 context。

## `*GroupNoResultBuilder.DefaultWorker`

```go
func (b *GroupNoResultBuilder) DefaultWorker() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L66) · [行为专题](../group.md)：DefaultWorker 使用默认并发数（core.IO()）。

**源码说明与使用片段**（原文）：

DefaultWorker 使用默认并发数（core.IO()）。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存每个 Group 的并发数，非正值在 NewGroup 按 1；Group/GroupVoid 初始值和 DefaultWorker 显式使用调用时 IO()。GroupMulti 初始值为 0，因此未设置时每片只有 1；分片后总并发乘以片数。不要与终端执行并发修改构建器。

## `*GroupNoResultBuilder.FailFast`

```go
func (b *GroupNoResultBuilder) FailFast() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L96) · [行为专题](../group.md)：FailFast 启用快速失败模式。

**源码说明与使用片段**（原文）：

FailFast 启用快速失败模式。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：标记后续构建的 Group 启用失败取消，当前构建器没有反向关闭开关；作用域 Run 使用派生 context。只请求取消，不强制结束回调，也不使 Run 自动返回业务错误；仍需处理提交错误并在等待后检查结果。

## `*GroupNoResultBuilder.Logger`

```go
func (b *GroupNoResultBuilder) Logger(l core.Logger) *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L127) · [行为专题](../group.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*GroupNoResultBuilder.Pool`

```go
func (b *GroupNoResultBuilder) Pool(p *pool.Pool[struct{}]) *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L139) · [行为专题](../group.md)：Pool 注入外部协程池，构建的 Group 将使用该池执行任务。

**源码说明与使用片段**（原文）：

Pool 注入外部协程池，构建的 Group 将使用该池执行任务。


**参数说明**：

- `p` (`*pool.Pool[struct{}]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool(p)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只影响后续 Build/Run；Pool(nil) 与 DefaultPool 清除外部池引用。外部池归调用方管理，Group 的等待和 Reset 不负责复位外部池；外部池自身的超时和队列配置仍可能影响执行。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.extPool = p
```

## `*GroupNoResultBuilder.Run`

```go
func (b *GroupNoResultBuilder) Run(fn func(ctx context.Context, nr *NoResult) error) error
```

[实现](../../internal/group/noresult_builder.go#L181) · [行为专题](../group.md)：Run 创建→执行→关闭。

**源码说明与使用片段**（原文）：

Run 创建→执行→关闭。 内部流程：Build() → 调用 fn → Close()（Wait + 清理）。


**参数说明**：

- `fn` (`func(ctx context.Context, nr *NoResult) error`)：同步接收本次执行器和上下文（以签名为准）的作用域函数；其返回后触发关闭，返回的 error 向外传播。需处理全部任务时在函数内等待并检查结果，避免把执行器带出作用域继续使用。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：作用域回调返回后自动关闭，但 Run 的 error 仅来自该回调，不自动汇总任务失败。须在回调内检查提交错误并 Wait 后检查结果或 FirstError；回调 panic 直接传播，defer 仍执行关闭。不要在 Run 返回后继续使用实例提交任务。

## `*GroupNoResultBuilder.Streaming`

```go
func (b *GroupNoResultBuilder) Streaming(buf int) *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L102) · [行为专题](../group.md)：Streaming 设置流式结果通道缓冲大小。

**源码说明与使用片段**（原文）：

Streaming 设置流式结果通道缓冲大小。


**参数说明**：

- `buf` (`int`)：构建器保存的结果通道缓冲容量；仅正数在 Build 时启用流，非正数不启用。实例 WithStreaming(0) 则启用 worker×2 缓冲，二者边界不同。流可能丢结果，须及时消费。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Streaming(buf)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：正数在后续 Build/Run 创建结果流，非正数和 DefaultStreaming 不启用；实例 WithStreaming(0) 则使用 worker×2，边界不同。流需持续消费；等待时无法刷入流的余项可能丢弃，完整结果应由 Wait 获取。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.streaming = buf
```

## `*GroupNoResultBuilder.SubmitTimeout`

```go
func (b *GroupNoResultBuilder) SubmitTimeout(d time.Duration) *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L84) · [行为专题](../group.md)：SubmitTimeout 设置获取并发槽位的等待超时。

**源码说明与使用片段**（原文）：

SubmitTimeout 设置获取并发槽位的等待超时。


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitTimeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存等待任务入队的期限，仅正数在终端创建时应用；DefaultSubmitTimeout 清零，恢复新 Group 的无显式提交计时器状态。不是业务执行期限，仍受提交 context、关闭和 FailFast 影响，必须处理提交错误。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.submitTimeout = d
```

## `*GroupNoResultBuilder.Timeout`

```go
func (b *GroupNoResultBuilder) Timeout(d time.Duration) *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L72) · [行为专题](../group.md)：Timeout 设置每个任务的超时时间。

**源码说明与使用片段**（原文）：

Timeout 设置每个任务的超时时间。


**参数说明**：

- `d` (`time.Duration`)：创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Timeout 保存原值，DefaultTimeout 读取并保存调用当时全局默认值；Build/Run 仅把正数覆盖到新 Group，非正数保留创建时的全局期限，因此 Timeout(0) 不能清除非零全局默认。需明确取消期限时在实例提交前 WithTimeout(0)，业务仍须响应 context。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.timeout = d
```

## `*GroupNoResultBuilder.Worker`

```go
func (b *GroupNoResultBuilder) Worker(n int) *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L60) · [行为专题](../group.md)：Worker 设置最大并发数。

**源码说明与使用片段**（原文）：

Worker 设置最大并发数。


**参数说明**：

- `n` (`int`)：每个 Group 的最大同时执行数；非正值在构造时使用 1。GroupBuilder 默认已保存 IO()，MultiGroupBuilder 零值则使用 1；分片模式按每片计算，总容量乘以片数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存每个 Group 的并发数，非正值在 NewGroup 按 1；Group/GroupVoid 初始值和 DefaultWorker 显式使用调用时 IO()。GroupMulti 初始值为 0，因此未设置时每片只有 1；分片后总并发乘以片数。不要与终端执行并发修改构建器。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.concurrency = n
```

## `*Group[T].Active`

```go
func (g *Group[T]) Active() int
```

[实现](../../internal/group/group.go#L1047) · [行为专题](../group.md)：Active 返回已登记但尚未结束的任务数，包含等待入队、队列内和正在执行的任务。

**源码说明与使用片段**（原文）：

Active 返回已登记但尚未结束的任务数，包含等待入队、队列内和正在执行的任务。 它不是 goroutine 数或 worker 数；实际执行中的任务数见 Busy。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Active()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*Group[T].Busy`

```go
func (g *Group[T]) Busy() int
```

[实现](../../internal/group/group.go#L1052) · [行为专题](../group.md)：Busy 返回当前正在执行任务的数量，不包含排队任务。

**源码说明与使用片段**（原文）：

Busy 返回当前正在执行任务的数量，不包含排队任务。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Busy()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*Group[T].Close`

```go
func (g *Group[T]) Close()
```

[实现](../../internal/group/group.go#L1006) · [行为专题](../group.md)：Close 关闭任务组，等待所有已提交任务完成并清理资源。

**源码说明与使用片段**（原文）：

Close 关闭任务组，等待所有已提交任务完成并清理资源。 等价于 g.Wait() 但不返回结果。由构建器的 Run 终端方法自动调用。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：Group.Close 调用 Wait，等待已接受任务、worker 及流式收尾；不先强制取消运行中的业务，回调不返回可能一直等待。开始等待后禁止新任务；超时等待后可再 Wait 取得最终结果，复用先在无活跃任务且等待完成时 Reset。

## `*Group[T].DefaultShard`

```go
func (g *Group[T]) DefaultShard() *MultiGroup[T]
```

[实现](../../internal/group/group.go#L1585) · [行为专题](../group.md)：DefaultShard 使用默认分片数（runtime.GOMAXPROCS(0)，最少 2）对当前 Group 进行水平分片。

**源码说明与使用片段**（原文）：

DefaultShard 使用默认分片数（runtime.GOMAXPROCS(0)，最少 2）对当前 Group 进行水平分片。 等效于 g.Shard(max(2, runtime.GOMAXPROCS(0)))，无需手动指定分片数。

使用示例：

	// 按 GOMAXPROCS 分片，最少 2 片
	mg := group.NewGroup[int](50).DefaultShard()
	defer mg.Close()


**参数说明**：无显式参数。

**返回值签名**：`*MultiGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShard()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：第 0 片复用原 Group；其余片只复制并发度、任务/提交超时、context/FailFast 关系、流容量及结果回调，不复制外部 Pool、AutoScale 或已有结果。片数非正或为 1 时只包装原实例，DefaultShard 使用 max(2,GOMAXPROCS)。分片应在提交前完成，并统一关闭全部分片。

## `*Group[T].DisableAutoScale`

```go
func (g *Group[T]) DisableAutoScale()
```

[实现](../../internal/group/group.go#L1376) · [行为专题](../group.md)：DisableAutoScale 停止自动扩缩容，将并发度恢复到配置的 MinWorkers 值。

**源码说明与使用片段**（原文）：

DisableAutoScale 停止自动扩缩容，将并发度恢复到配置的 MinWorkers 值。

停止后仍可通过手动方式调整。如果自动扩缩容未启用，调用无效果。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DisableAutoScale()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：首次 Enable 复制并 Normalize 配置，nil 用默认，保持初始 Worker，应先设在上下界内；已启用重复调用不更新配置，等待中或已等待后不启用。Disable 停监视并调目标至 Min，可能扩容或缩容；Wait 自动停监视，Reset 清配置。目标改变不代表旧 worker 全退出，自动调整不改配置边界。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验全部关系/浮点值；外部池仍限制实际执行并发。

## `*Group[T].EnableAutoScale`

```go
func (g *Group[T]) EnableAutoScale(config *core.AutoScaleConfig)
```

[实现](../../internal/group/group.go#L1352) · [行为专题](../group.md)：EnableAutoScale 启用自动扩缩容。基于 busy/concurrency 比率周期性检测负载：

**源码说明与使用片段**（原文）：

EnableAutoScale 启用自动扩缩容。基于 busy/concurrency 比率周期性检测负载： - busy/concurrency > ScaleUpThreshold 持续 ScaleUpChecks 次 → 扩容（乘以 ScaleUpFactor，上限 MaxWorkers） - busy/concurrency \< ScaleDownThreshold 持续 ScaleDownChecks 次 → 缩容（乘以 ScaleDownFactor，下限 MinWorkers）

config 为 nil 时使用 DefaultAutoScaleConfig()（CPU\*2 ~ CPU\*100，每 5s 检测）。 默认扩容因子 1.5（增加 50%），缩容因子 0.75（保留 75%），比翻倍/减半更加平滑。

重复调用是安全的（幂等）。调用后启动后台 goroutine 进行负载检测。

注意：Group.Wait 后自动停止扩缩容。如需复用（Reset 后），需重新调用 EnableAutoScale。

示例：

	g := group.NewGroup[int](4)

	// 使用默认配置
	g.EnableAutoScale(nil)

	// 自定义配置
	g.EnableAutoScale(&core.AutoScaleConfig{
	    MinWorkers: 2,
	    MaxWorkers: 500,
	    CheckInterval: 3 * time.Second,
	})


**参数说明**：

- `config` (`*core.AutoScaleConfig`)：自动扩缩容配置指针；nil 时使用默认配置，启用时复制并在副本上 Normalize，不修改调用方配置。读取配置时不得并发修改；启用后的修改不影响已保存的副本。调用会启动后台监视，应在任务前配置并按实例生命周期停止。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.EnableAutoScale(config)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：首次 Enable 复制并 Normalize 配置，nil 用默认，保持初始 Worker，应先设在上下界内；已启用重复调用不更新配置，等待中或已等待后不启用。Disable 停监视并调目标至 Min，可能扩容或缩容；Wait 自动停监视，Reset 清配置。目标改变不代表旧 worker 全退出，自动调整不改配置边界。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验全部关系/浮点值；外部池仍限制实际执行并发。

## `*Group[T].Errors`

```go
func (g *Group[T]) Errors() []error
```

[实现](../../internal/group/group.go#L1101) · [行为专题](../group.md)：Errors 返回所有任务的错误切片。建议在 Wait 后调用以获取完整结果。

**源码说明与使用片段**（原文）：

Errors 返回所有任务的错误切片。建议在 Wait 后调用以获取完整结果。

使用示例：

	results := g.Wait()
	for _, err := range g.Errors() {
	    log.Println(err)
	}


**参数说明**：无显式参数。

**返回值签名**：`[]error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Errors()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：先完成 Wait 再读取完整结果，提前读取只是当时快照。单 Group.Values 同时检查 Occupied 与 Err==nil，返回元素浅拷贝；FirstError 按结果存储顺序选择，不保证最先发生或最早提交。JoinErrors 使用 errors.Join，保留可供 errors.Is/As 检查的错误链。

## `*Group[T].FailCount`

```go
func (g *Group[T]) FailCount() int64
```

[实现](../../internal/group/group.go#L1020) · [行为专题](../group.md)：FailCount 返回失败任务数（线程安全，可在 Wait 前调用）。

**源码说明与使用片段**（原文）：

FailCount 返回失败任务数（线程安全，可在 Wait 前调用）。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*Group[T].FirstError`

```go
func (g *Group[T]) FirstError() error
```

[实现](../../internal/group/group.go#L1117) · [行为专题](../group.md)：FirstError 返回第一个错误（按 results 顺序）。无错误返回 nil。

**源码说明与使用片段**（原文）：

FirstError 返回第一个错误（按 results 顺序）。无错误返回 nil。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FirstError()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：先完成 Wait 再读取完整结果，提前读取只是当时快照。单 Group.Values 同时检查 Occupied 与 Err==nil，返回元素浅拷贝；FirstError 按结果存储顺序选择，不保证最先发生或最早提交。JoinErrors 使用 errors.Join，保留可供 errors.Is/As 检查的错误链。

## `*Group[T].Go`

```go
func (g *Group[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/group/group.go#L721) · [行为专题](../group.md)：Go 提交一个任务到 Group 中执行。fn 通过 context 支持取消和超时。

**源码说明与使用片段**（原文）：

Go 提交一个任务到 Group 中执行。fn 通过 context 支持取消和超时。 返回 error 表示提交失败（如槽位满超时、ctx 已取消等），不代表任务执行失败。

参数：

  - ctx：上下文，用于取消和 TraceID 追踪
  - fn：任务执行函数，接收派生 context，返回结果和错误

使用示例：

	g := group.DefaultGroup[int]()
	g.WithTimeout(5 * time.Second)
	for _, id := range ids {
	    g.Go(ctx, func(ctx context.Context) (int, error) {
	        return fetchScore(ctx, id)
	    })
	}
	results := g.Wait()


**参数说明**：

- `ctx` (`context.Context`)：上下文，用于取消和 TraceID 追踪
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Go(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。error 表示提交是否接受，不表示业务成功；普通 Go 的结果按记录完成顺序保存，不保证提交顺序。等待开始/完成后拒绝新任务，预检查拒绝也可能增加总数和错误存储。业务 panic 转为 PanicError，context 及超时只请求协作退出；提交可能因队列满阻塞，须处理提交错误并最终等待检查逐项 Err。

## `*Group[T].GoAt`

```go
func (g *Group[T]) GoAt(index int, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/group/group.go#L901) · [行为专题](../group.md)：GoAt 提交任务到指定索引位置，结果会写入 results[index]。

**源码说明与使用片段**（原文）：

GoAt 提交任务到指定索引位置，结果会写入 results\[index]。 适用于需要按索引排列结果的场景。

参数：

  - index：在结果切片中的索引位置
  - ctx：上下文
  - fn：任务执行函数

使用示例：

	// 按原始顺序排列结果
	for i, url := range urls {
	    g.GoAt(i, ctx, func(ctx context.Context) (string, error) {
	        return httpGet(ctx, url)
	    })
	}


**参数说明**：

- `index` (`int`)：结果切片的零起始索引；负值返回 ErrInvalidIndex。稀疏索引会分配到 index+1 长度并留下未占用槽位；输入顺序需要不同且连续的索引。
- `ctx` (`context.Context`)：上下文
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoAt(index, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。负索引直接返回 ErrInvalidIndex 且不计数；非负索引可扩展结果并产生空槽，没有 Pool 的 MaxPoolIndex 限制，须控制内存。重复索引会将旧结果移动到其他槽位，不能实现任务替换；使用唯一连续索引保持对应关系。返回 error 只表示提交，业务 panic 转为 PanicError，最终 Wait 并核对 Err/Occupied。

## `*Group[T].GoAtWithTimeout`

```go
func (g *Group[T]) GoAtWithTimeout(index int, ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) error
```

[实现](../../internal/group/group.go#L788) · [行为专题](../group.md)：GoAtWithTimeout 提交任务到指定索引并单独设置超时时间。

**源码说明与使用片段**（原文）：

GoAtWithTimeout 提交任务到指定索引并单独设置超时时间。

参数：

  - index：在结果切片中的索引位置
  - ctx：上下文
  - timeout：该任务的超时时间
  - fn：任务执行函数

使用示例：

	// 第 3 个任务只需 500ms
	g.GoAtWithTimeout(2, ctx, 500*time.Millisecond, fn)


**参数说明**：

- `index` (`int`)：结果切片的零起始索引；负值返回 ErrInvalidIndex。稀疏索引会分配到 index+1 长度并留下未占用槽位；输入顺序需要不同且连续的索引。
- `ctx` (`context.Context`)：上下文
- `timeout` (`time.Duration`)：覆盖本次任务的 Group 执行超时；正数从回调开始执行时计时，非正值清除 Group 级期限，仍受父 context 约束。注入外部池时，该池原有任务超时仍可能生效。与 Pool.SubmitWithCompletionTimeout 的非正值继承规则不同。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoAtWithTimeout(index, ctx, timeout, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。负索引直接返回 ErrInvalidIndex 且不计数；非负索引可扩展结果并产生空槽，没有 Pool 的 MaxPoolIndex 限制，须控制内存。重复索引会将旧结果移动到其他槽位，不能实现任务替换；使用唯一连续索引保持对应关系。返回 error 只表示提交，业务 panic 转为 PanicError，最终 Wait 并核对 Err/Occupied。

## `*Group[T].GoWithTimeout`

```go
func (g *Group[T]) GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) error
```

[实现](../../internal/group/group.go#L755) · [行为专题](../group.md)：GoWithTimeout 提交任务并单独设置该任务的超时时间（覆盖 Group 级别的 timeout）。

**源码说明与使用片段**（原文）：

GoWithTimeout 提交任务并单独设置该任务的超时时间（覆盖 Group 级别的 timeout）。

参数：

  - ctx：上下文
  - timeout：该任务的超时时间（覆盖 Group 级别默认值）
  - fn：任务执行函数

使用示例：

	// 组默认 5s 超时，但这个特定任务只有 1s
	g.GoWithTimeout(ctx, 1*time.Second, func(ctx context.Context) (string, error) {
	    return quickAPI(ctx)
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `timeout` (`time.Duration`)：覆盖本次任务的 Group 执行超时；正数从回调开始执行时计时，非正值清除 Group 级期限，仍受父 context 约束。注入外部池时，该池原有任务超时仍可能生效。与 Pool.SubmitWithCompletionTimeout 的非正值继承规则不同。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoWithTimeout(ctx, timeout, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。error 表示提交是否接受，不表示业务成功；普通 Go 的结果按记录完成顺序保存，不保证提交顺序。等待开始/完成后拒绝新任务，预检查拒绝也可能增加总数和错误存储。业务 panic 转为 PanicError，context 及超时只请求协作退出；提交可能因队列满阻塞，须处理提交错误并最终等待检查逐项 Err。

## `*Group[T].HasError`

```go
func (g *Group[T]) HasError() bool
```

[实现](../../internal/group/group.go#L1030) · [行为专题](../group.md)：HasError 检查是否有任务失败。

**源码说明与使用片段**（原文）：

HasError 检查是否有任务失败。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示观察到了错误，使用 Error/Errors 获取错误值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.HasError()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*Group[T].IsAutoScaleEnabled`

```go
func (g *Group[T]) IsAutoScaleEnabled() bool
```

[实现](../../internal/group/group.go#L1391) · [行为专题](../group.md)：IsAutoScaleEnabled 返回是否已启用自动扩缩容。

**源码说明与使用片段**（原文）：

IsAutoScaleEnabled 返回是否已启用自动扩缩容。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsAutoScaleEnabled()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：首次 Enable 复制并 Normalize 配置，nil 用默认，保持初始 Worker，应先设在上下界内；已启用重复调用不更新配置，等待中或已等待后不启用。Disable 停监视并调目标至 Min，可能扩容或缩容；Wait 自动停监视，Reset 清配置。目标改变不代表旧 worker 全退出，自动调整不改配置边界。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验全部关系/浮点值；外部池仍限制实际执行并发。

## `*Group[T].JoinErrors`

```go
func (g *Group[T]) JoinErrors() error
```

[实现](../../internal/group/group.go#L1162) · [行为专题](../group.md)：JoinErrors 使用 errors.Join 合并所有错误。

**源码说明与使用片段**（原文）：

JoinErrors 使用 errors.Join 合并所有错误。

使用示例：

	g.Wait()
	if err := g.JoinErrors(); err != nil {
	    log.Printf("有任务失败: %v", err)
	}


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.JoinErrors()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：先完成 Wait 再读取完整结果，提前读取只是当时快照。单 Group.Values 同时检查 Occupied 与 Err==nil，返回元素浅拷贝；FirstError 按结果存储顺序选择，不保证最先发生或最早提交。JoinErrors 使用 errors.Join，保留可供 errors.Is/As 检查的错误链。

## `*Group[T].MergeFailFastCancel`

```go
func (g *Group[T]) MergeFailFastCancel(cancel context.CancelFunc) *Group[T]
```

[实现](../../internal/group/group.go#L214) · [行为专题](../group.md)：MergeFailFastCancel 将外部 cancel 函数合并到当前 failFast 取消链中。

**源码说明与使用片段**（原文）：

MergeFailFastCancel 将外部 cancel 函数合并到当前 failFast 取消链中。 用于 ShardedGroup 等上层组件在多个 Group 间共享 failFast 取消信号， 确保任一 Group 的任务失败能取消所有 Group。


**参数说明**：

- `cancel` (`context.CancelFunc`)：合并到 FailFast 取消链的新取消函数，应非 nil；同步执行且须并发安全、快速返回。已有非 nil 取消链时合并 nil 会使后续触发取消 panic；配置须在提交任务前完成。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MergeFailFastCancel(cancel)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只合并取消通知，不会单独启用 FailFast。传入非 nil 的并发安全 CancelFunc，先完成配置再提交；通知不能强制中断回调，也不要在通知中等待或关闭当前执行器。

## `*Group[T].OkCount`

```go
func (g *Group[T]) OkCount() int64
```

[实现](../../internal/group/group.go#L1025) · [行为专题](../group.md)：OkCount 返回成功任务数。

**源码说明与使用片段**（原文）：

OkCount 返回成功任务数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OkCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*Group[T].Reset`

```go
func (g *Group[T]) Reset() (*Group[T], error)
```

[实现](../../internal/group/group.go#L1179) · [行为专题](../group.md)：Reset 重置 Group 状态以便复用。保留 timeout、submitTimeout、concurrency 配置。

**源码说明与使用片段**（原文）：

Reset 重置 Group 状态以便复用。保留 timeout、submitTimeout、concurrency 配置。 必须在 Wait 之后且无活跃任务时调用。

使用示例：

	g.Wait()                  // 等待第一批完成
	g.Reset()                 // 重置状态
	for _, item := range batch2 {
	    if err := g.Go(ctx, func(ctx context.Context) (string, error) {
	        return process(item)
	    }); err != nil { return err }
	}
	g.Wait()


**参数说明**：无显式参数。

**返回值签名**：`*Group[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reset()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：单 Group 要求已经完成等待且无活跃任务，否则返回错误并保持当前实例。清除结果、计数、context/FailFast 和 AutoScale；保留并发度、任务/提交超时、结果回调、流容量以及外部池引用。须在外部同步下单独调用，不与提交/等待竞争；重新取得 StreamResults，新批次重新设置 context/FailFast/AutoScale。

## `*Group[T].Shard`

```go
func (g *Group[T]) Shard(shards int) *MultiGroup[T]
```

[实现](../../internal/group/group.go#L1530) · [行为专题](../group.md)：Shard 把当前 Group 包装为多个 Group 实例，第 0 片复用当前 Group。

**源码说明与使用片段**（原文）：

Shard 把当前 Group 包装为多个 Group 实例，第 0 片复用当前 Group。 其余片复制并发度、任务/提交超时、context/FailFast 关系、流容量及结果回调； 不复制已存结果、外部 Pool 注入或 AutoScale 配置。应在提交前分片并统一关闭。

使用示例：

	// 8 个分片，每个 50 并发 → 400 并发总量
	mg := group.NewGroup[int](50).Shard(8)
	defer mg.Close()


**参数说明**：

- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。

**返回值签名**：`*MultiGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shard(shards)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：第 0 片复用原 Group；其余片只复制并发度、任务/提交超时、context/FailFast 关系、流容量及结果回调，不复制外部 Pool、AutoScale 或已有结果。片数非正或为 1 时只包装原实例，DefaultShard 使用 max(2,GOMAXPROCS)。分片应在提交前完成，并统一关闭全部分片。

## `*Group[T].Stats`

```go
func (g *Group[T]) Stats() GroupStats
```

[实现](../../internal/group/group.go#L1080) · [行为专题](../group.md)：Stats 获取当前 Group 的运行时统计快照。

**源码说明与使用片段**（原文）：

Stats 获取当前 Group 的运行时统计快照。

使用示例：

	stats := g.Stats()
	fmt.Printf("活跃: %d, 繁忙: %d, 成功: %d, 失败: %d\n",
	    stats.Active, stats.Busy, stats.OkTask, stats.FailTask)


**参数说明**：无显式参数。

**返回值签名**：`GroupStats`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stats()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*Group[T].StreamResults`

```go
func (g *Group[T]) StreamResults() <-chan core.Result[T]
```

[实现](../../internal/group/group.go#L445) · [行为专题](../group.md)：StreamResults 返回流式结果的只读 channel。

**源码说明与使用片段**（原文）：

StreamResults 返回流式结果的只读 channel。 必须在 WithStreaming 之后调用，否则返回 nil。


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StreamResults()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：Group 未启用流时返回 nil；慢消费时溢出暂存，Wait 最终刷新仍写不进 channel 的项会丢失。MultiGroup 每次调用创建新的合并转发器，重复调用会竞争分片流；应只调用一次并持续消费至关闭，完整结果使用 Wait 返回值。

## `*Group[T].TotalCount`

```go
func (g *Group[T]) TotalCount() int64
```

[实现](../../internal/group/group.go#L1036) · [行为专题](../group.md)：TotalCount 返回进入提交预检查的尝试数（包含预检查拒绝及执行成功/失败）。

**源码说明与使用片段**（原文）：

TotalCount 返回进入提交预检查的尝试数（包含预检查拒绝及执行成功/失败）。 GoAt/GoAtWithTimeout 的负索引在预检查前拒绝，不计入。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：计数包含进入 Group 提交预检查的尝试，即使预检查拒绝也可能增加；负索引在预检查前拒绝而不计数。不是成功入队或执行完成数量，MultiGroup 按片求和且不是跨片原子快照。

## `*Group[T].Values`

```go
func (g *Group[T]) Values() []T
```

[实现](../../internal/group/group.go#L1139) · [行为专题](../group.md)：Values 返回所有成功任务的结果值切片。建议在 Wait 后调用。

**源码说明与使用片段**（原文）：

Values 返回所有成功任务的结果值切片。建议在 Wait 后调用。

使用示例：

	g.Wait()
	for _, val := range g.Values() {
	    fmt.Println(val)
	}


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：先完成 Wait 再读取完整结果，提前读取只是当时快照。单 Group.Values 同时检查 Occupied 与 Err==nil，返回元素浅拷贝；FirstError 按结果存储顺序选择，不保证最先发生或最早提交。JoinErrors 使用 errors.Join，保留可供 errors.Is/As 检查的错误链。

## `*Group[T].Wait`

```go
func (g *Group[T]) Wait() []core.Result[T]
```

[实现](../../internal/group/group.go#L971) · [行为专题](../group.md)：Wait 等待所有任务完成并返回结果切片。调用后 Group 进入 waited 状态，不能再次提交任务。

**源码说明与使用片段**（原文）：

Wait 等待所有任务完成并返回结果切片。调用后 Group 进入 waited 状态，不能再次提交任务。 应先调用 WithTimeout / WithContext 等配置方法，再调用 Go/GoAt，最后调用 Wait。

使用示例：

	results := g.Wait()
	for _, r := range results {
	    if r.Ok() {
	        fmt.Println(r.Value)
	    }
	}


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：Group.Close 调用 Wait，等待已接受任务、worker 及流式收尾；不先强制取消运行中的业务，回调不返回可能一直等待。开始等待后禁止新任务；超时等待后可再 Wait 取得最终结果，复用先在无活跃任务且等待完成时 Reset。

## `*Group[T].WaitContext`

```go
func (g *Group[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool)
```

[实现](../../internal/group/group.go#L1272) · [行为专题](../group.md)：WaitContext 等待所有任务完成或 ctx 取消。返回 (结果, true) 或 (部分结果, false)。

**源码说明与使用片段**（原文）：

WaitContext 等待所有任务完成或 ctx 取消。返回 (结果, true) 或 (部分结果, false)。 The result stream closes only after all accepted tasks have finished.

参数：

  - ctx：上下文，取消后立即返回部分结果

使用示例：

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, ok := g.WaitContext(ctx)


**参数说明**：

- `ctx` (`context.Context`)：上下文，取消后立即返回部分结果

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitContext(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：首次等待关闭提交入口并启动唯一后台收尾，完成或本次 context 取消后返回结果浅拷贝快照；超时/取消会通知已登记任务及组 context，不强制结束业务或同步结果通知。结果已保存但通知未结束时仍可能 ok=false、Active 非零且不能 Reset；之后再 Wait 取得最终结果，重复等待不清空已保存结果。bool 仅表示等待完成，逐项检查 Err/Occupied，不在所属回调中等待自身。

## `*Group[T].WaitTimeout`

```go
func (g *Group[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool)
```

[实现](../../internal/group/group.go#L1255) · [行为专题](../group.md)：WaitTimeout 等待所有任务完成，最多等待 d 时长。返回 (结果, true) 或 (部分结果, false)。

**源码说明与使用片段**（原文）：

WaitTimeout 等待所有任务完成，最多等待 d 时长。返回 (结果, true) 或 (部分结果, false)。 On timeout cleanup continues; Wait retrieves the final results before Reset.

参数：

  - d：最长等待时间

使用示例：

	results, ok := g.WaitTimeout(10 * time.Second)
	if !ok {
	    log.Println("部分任务未完成")
	}


**参数说明**：

- `d` (`time.Duration`)：本次等待的最长时长；非正值使等待 context 立即过期，完成与超时同时就绪时不保证 bool 的选择。超时返回部分结果并通知取消，不能强制停止任务；分片共享同一截止时间，复用须遵守该实例的等待/复位约定。

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitTimeout(d)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：首次等待关闭提交入口并启动唯一后台收尾，完成或本次 context 取消后返回结果浅拷贝快照；超时/取消会通知已登记任务及组 context，不强制结束业务或同步结果通知。结果已保存但通知未结束时仍可能 ok=false、Active 非零且不能 Reset；之后再 Wait 取得最终结果，重复等待不清空已保存结果。bool 仅表示等待完成，逐项检查 Err/Occupied，不在所属回调中等待自身。

## `*Group[T].WithContext`

```go
func (g *Group[T]) WithContext(ctx context.Context) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L174) · [行为专题](../group.md)：WithContext 绑定 context，支持通过 cancel 统一取消所有任务。

**源码说明与使用片段**（原文）：

WithContext 绑定 context，支持通过 cancel 统一取消所有任务。 返回的 ctx 是派生的可取消 context。

参数：

  - ctx：原始上下文

使用示例：

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, taskCtx := group.DefaultGroup[string]().WithContext(parent)
	defer g.Close()
	_ = g.Go(taskCtx, func(ctx context.Context) (string, error) { return fetch(ctx) })
	results := g.Wait() // 检查每项 Result.Err 后再使用 Value


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithContext(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
g.ctx = ctx
```

## `*Group[T].WithCtxSto`

```go
func (g *Group[T]) WithCtxSto(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L397) · [行为专题](../group.md)：WithCtxSto 设置 Context 和提交超时（Sto = SubmitTimeout）。

**源码说明与使用片段**（原文）：

WithCtxSto 设置 Context 和提交超时（Sto = SubmitTimeout）。

参数：

  - ctx：原始上下文
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxSto(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithCtxStoTID`

```go
func (g *Group[T]) WithCtxStoTID(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L408) · [行为专题](../group.md)：WithCtxStoTID 设置 Context、提交超时并注入 trace_id。

**源码说明与使用片段**（原文）：

WithCtxStoTID 设置 Context、提交超时并注入 trace\_id。

参数：

  - ctx：原始上下文
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxStoTID(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithCtxTOTID`

```go
func (g *Group[T]) WithCtxTOTID(ctx context.Context, timeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L360) · [行为专题](../group.md)：WithCtxTOTID 设置 Context、任务超时并注入 trace_id。

**源码说明与使用片段**（原文）：

WithCtxTOTID 设置 Context、任务超时并注入 trace\_id。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTOTID(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithCtxTimeout`

```go
func (g *Group[T]) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L349) · [行为专题](../group.md)：WithCtxTimeout 设置 Context 和任务超时。

**源码说明与使用片段**（原文）：

WithCtxTimeout 设置 Context 和任务超时。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTimeout(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithCtxTraceID`

```go
func (g *Group[T]) WithCtxTraceID(ctx context.Context) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L306) · [行为专题](../group.md)：WithCtxTraceID 设置 Context 并注入 trace_id。

**源码说明与使用片段**（原文）：

WithCtxTraceID 设置 Context 并注入 trace\_id。

参数：

  - ctx：原始上下文


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFFCtx`

```go
func (g *Group[T]) WithFFCtx(ctx context.Context) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L207) · [行为专题](../group.md)：WithFFCtx 等同 WithFailFast。提供更短的别名。

**源码说明与使用片段**（原文）：

WithFFCtx 等同 WithFailFast。提供更短的别名。

参数：

  - ctx：原始上下文


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFCtx(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFFSto`

```go
func (g *Group[T]) WithFFSto(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L275) · [行为专题](../group.md)：WithFFSto 启用 FailFast 并设置提交超时（Sto = SubmitTimeout）。

**源码说明与使用片段**（原文）：

WithFFSto 启用 FailFast 并设置提交超时（Sto = SubmitTimeout）。

参数：

  - ctx：原始上下文
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFSto(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFFStoTID`

```go
func (g *Group[T]) WithFFStoTID(ctx context.Context, submitTimeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L286) · [行为专题](../group.md)：WithFFStoTID 启用 FailFast、设置提交超时并注入 trace_id。

**源码说明与使用片段**（原文）：

WithFFStoTID 启用 FailFast、设置提交超时并注入 trace\_id。

参数：

  - ctx：原始上下文
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFStoTID(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFFTOSto`

```go
func (g *Group[T]) WithFFTOSto(ctx context.Context, timeout time.Duration, submitTimeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L327) · [行为专题](../group.md)：WithFFTOSto 启用 FailFast、设置任务超时和提交超时（TO=Timeout, Sto=SubmitTimeout）。

**源码说明与使用片段**（原文）：

WithFFTOSto 启用 FailFast、设置任务超时和提交超时（TO=Timeout, Sto=SubmitTimeout）。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOSto(ctx, timeout, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFFTOStoTID`

```go
func (g *Group[T]) WithFFTOStoTID(ctx context.Context, timeout time.Duration, submitTimeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L339) · [行为专题](../group.md)：WithFFTOStoTID 启用 FailFast、设置超时、提交超时并注入 trace_id（TO=Timeout, Sto=SubmitTimeout, TID=TraceID）。

**源码说明与使用片段**（原文）：

WithFFTOStoTID 启用 FailFast、设置超时、提交超时并注入 trace\_id（TO=Timeout, Sto=SubmitTimeout, TID=TraceID）。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间
  - submitTimeout：提交超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOStoTID(ctx, timeout, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFFTOTID`

```go
func (g *Group[T]) WithFFTOTID(ctx context.Context, timeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L316) · [行为专题](../group.md)：WithFFTOTID 启用 FailFast、设置超时并注入 trace_id（TO = Timeout, TID = TraceID）。

**源码说明与使用片段**（原文）：

WithFFTOTID 启用 FailFast、设置超时并注入 trace\_id（TO = Timeout, TID = TraceID）。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOTID(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFFTimeout`

```go
func (g *Group[T]) WithFFTimeout(ctx context.Context, timeout time.Duration) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L296) · [行为专题](../group.md)：WithFFTimeout 启用 FailFast 并设置任务超时。

**源码说明与使用片段**（原文）：

WithFFTimeout 启用 FailFast 并设置任务超时。

参数：

  - ctx：原始上下文
  - timeout：任务超时时间


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTimeout(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFFTraceID`

```go
func (g *Group[T]) WithFFTraceID(ctx context.Context) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L265) · [行为专题](../group.md)：WithFFTraceID 启用 FailFast 并注入 trace_id。

**源码说明与使用片段**（原文）：

WithFFTraceID 启用 FailFast 并注入 trace\_id。

参数：

  - ctx：原始上下文


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*Group[T].WithFailFast`

```go
func (g *Group[T]) WithFailFast(ctx context.Context) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L192) · [行为专题](../group.md)：WithFailFast 启用快速失败模式：任一任务出错时取消所有其他任务。

**源码说明与使用片段**（原文）：

WithFailFast 启用快速失败模式：任一任务出错时取消所有其他任务。

参数：

  - ctx：原始上下文

使用示例：

	g, ctx := group.DefaultGroup[string]().WithFailFast(ctx)


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFailFast(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
g.ctx = ctx
```

## `*Group[T].WithPool`

```go
func (g *Group[T]) WithPool(p *pool.Pool[T]) *Group[T]
```

[实现](../../internal/group/group.go#L438) · [行为专题](../group.md)：WithPool 注入外部协程池，任务执行时通过 pool.Submit 调度而非内部 worker goroutine。

**源码说明与使用片段**（原文）：

WithPool 注入外部协程池，任务执行时通过 pool.Submit 调度而非内部 worker goroutine。 外部 Pool 控制实际并发度，Group 内部的 concurrency worker 仅做分发。 为 nil 时恢复默认行为（使用内部 worker 池）。


**参数说明**：

- `p` (`*pool.Pool[T]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithPool(p)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：注入外部池供内部 Group worker 分发，nil 恢复自有执行；组和池各有队列及超时，池容量限制实际业务并发。执行期限从实际回调开始并与父/Pool 期限取早，Group 提交期限不覆盖向 Pool 分发时的等待。业务值/错误/panic 保存在组内，Pool 所见包装回调返回零值 nil，可能计成功且不触发 Pool FailFast，必须检查组结果；业务执行前被 Pool 拒绝则记录拒绝错误，不触发组业务 FailFast。Group Wait 仅等待本组任务，不调用 Pool.Wait/Reset/Close，池仍可接受任务；所有者统一管理，配置不与生命周期竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
g.pool = p
```

## `*Group[T].WithResultCallback`

```go
func (g *Group[T]) WithResultCallback(fn func(core.Result[T])) *Group[T]
```

[实现](../../internal/group/group.go#L430) · [行为专题](../group.md)：WithResultCallback 设置结果记录路径的同步回调。

**源码说明与使用片段**（原文）：

WithResultCallback 设置结果记录路径的同步回调。 worker、提交取消或拒绝路径均可能调用；最早的 waited/waiting 拒绝不通知。 回调可能由 worker、提交拒绝或关闭清理路径并发调用，应尽量轻量。 不得在回调中同步等待或关闭当前 Group；nil 清除回调，回调 panic 被恢复并记录。


**参数说明**：

- `fn` (`func(core.Result[T])`)：接收记录结果路径的同步通知，nil 清除；worker、取消或拒绝路径可并发触发，但最早的 waited/waiting 预检查直接追加错误而不通知。不能按回调数统计全部尝试；须并发安全、快速返回，panic 被记录，不在通知中 Wait/Close 本组。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithResultCallback(fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存同步通知函数，nil 清除；可能从 worker、拒绝或清理路径并发调用，panic 被记录，不改原结果。正常执行先保存结果并尝试发送流，再同步通知；通知返回后成功计数才增加，Active/Busy 才释放，最终等待和流关闭也等待通知。最早的 waited/waiting 预检查分支直接追加错误，不通知回调，不能按通知数统计全部提交尝试。必须快速返回且并发安全，不在其中 Wait/Close 当前组。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
g.resultCb = fn
```

## `*Group[T].WithStreaming`

```go
func (g *Group[T]) WithStreaming(bufSize int) *Group[T]
```

[实现](../../internal/group/group.go#L418) · [行为专题](../group.md)：WithStreaming 启用流式结果消费，结果通过 channel 实时发送。

**源码说明与使用片段**（原文）：

WithStreaming 启用流式结果消费，结果通过 channel 实时发送。 bufSize 控制 channel 缓冲大小，0 使用 concurrency\*2 的默认值。 调用 StreamResults() 获取只读 channel，在 Wait() 后自动关闭。


**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStreaming(bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：立即新建结果通道，非正 bufSize 使用 Worker×2；重复调用替换引用而不关闭旧通道，应在提交前配置一次并重新取得 StreamResults。慢消费时暂存，Wait 收尾无法刷入通道的结果会丢失；完整结果用 Wait。不要在业务或结果通知中等待自身流关闭。

## `*Group[T].WithSubmitTimeout`

```go
func (g *Group[T]) WithSubmitTimeout(d time.Duration) *Group[T]
```

[实现](../../internal/group/group.go#L387) · [行为专题](../group.md)：WithSubmitTimeout 设置获取并发槽位的等待超时。0 表示无限等待。

**源码说明与使用片段**（原文）：

WithSubmitTimeout 设置获取并发槽位的等待超时。0 表示无限等待。 高负载时建议设置以避免长时间阻塞提交 goroutine。

参数：

  - d：提交超时时间，0 表示无限等待

使用示例：

	g := group.DefaultGroup[string]().WithSubmitTimeout(500 * time.Millisecond)


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithSubmitTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：原地保存设置并返回自身；WithTimeout 正数从业务开始计时，非正值清除组期限，外部池自身期限仍可能生效。WithSubmitTimeout 只限制等待入组队列，不覆盖任务执行或分发进外部池的等待；非正值无计时器，仍受 context/等待关闭影响。配置在提交前完成，检查提交错误及最终业务结果。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
g.submitTimeout = d
```

## `*Group[T].WithTimeout`

```go
func (g *Group[T]) WithTimeout(d time.Duration) *Group[T]
```

[实现](../../internal/group/group.go#L373) · [行为专题](../group.md)：WithTimeout 设置每个任务的执行超时时间（默认 core.GetDefaultTimeout()）。

**源码说明与使用片段**（原文）：

WithTimeout 设置每个任务的执行超时时间（默认 core.GetDefaultTimeout()）。

参数：

  - d：任务超时时间，0 表示无限等待

使用示例：

	g := group.DefaultGroup[string]().WithTimeout(3 * time.Second)


**参数说明**：

- `d` (`time.Duration`)：每个任务开始执行后的 context 超时；正数生效，非正值清除本实例配置的执行期限，不包含等待入队时间。分片方法对每片应用，回调必须响应 context；在提交前配置。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：原地保存设置并返回自身；WithTimeout 正数从业务开始计时，非正值清除组期限，外部池自身期限仍可能生效。WithSubmitTimeout 只限制等待入组队列，不覆盖任务执行或分发进外部池的等待；非正值无计时器，仍受 context/等待关闭影响。配置在提交前完成，检查提交错误及最终业务结果。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
g.timeout = d
```

## `*Group[T].WithTraceID`

```go
func (g *Group[T]) WithTraceID(ctx context.Context) (*Group[T], context.Context)
```

[实现](../../internal/group/group.go#L151) · [行为专题](../group.md)：WithTraceID 确保 context 中包含 TraceID，便于链路追踪。

**源码说明与使用片段**（原文）：

WithTraceID 确保 context 中包含 TraceID，便于链路追踪。

参数：

  - ctx：原始上下文（没有 TraceID 时自动生成）

使用示例：

	g, ctx := group.DefaultGroup[string]().WithTraceID(ctx)


**参数说明**：

- `ctx` (`context.Context`)：原始上下文（没有 TraceID 时自动生成）

**返回值签名**：`*Group[T], context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
g.ctx = ctx
```

## `*Group[T].Worker`

```go
func (g *Group[T]) Worker() int
```

[实现](../../internal/group/group.go#L1041) · [行为专题](../group.md)：Worker 返回当前最大并发数（自动扩缩容时可能动态变化）。

**源码说明与使用片段**（原文）：

Worker 返回当前最大并发数（自动扩缩容时可能动态变化）。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*MultiGroupBuilder[T].Context`

```go
func (b *MultiGroupBuilder[T]) Context(ctx context.Context) *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L31) · [行为专题](../group.md)：Context 链式设置上下文

**源码说明与使用片段**（原文）：

Context 链式设置上下文


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存非 nil 父 context 并确保 trace_id，后续 Build 的 Group context 与 Run 的作用域 context 由此派生；任务提交仍应传递该 context。取消只请求业务退出，配置不影响已经构建的实例，也不能与构建或 Run 并发。MultiGroup 未启用 FailFast 时不把此字段绑定到各片内部 context。

## `*MultiGroupBuilder[T].DefaultLogger`

```go
func (b *MultiGroupBuilder[T]) DefaultLogger() *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L73) · [行为专题](../group.md)：DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。

**源码说明与使用片段**（原文）：

DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。


**参数说明**：无显式参数。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*MultiGroupBuilder[T].FailFast`

```go
func (b *MultiGroupBuilder[T]) FailFast() *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L61) · [行为专题](../group.md)：FailFast 启用快速失败模式

**源码说明与使用片段**（原文）：

FailFast 启用快速失败模式


**参数说明**：无显式参数。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：标记后续构建的 Group 启用失败取消，当前构建器没有反向关闭开关；作用域 Run 使用派生 context。只请求取消，不强制结束回调，也不使 Run 自动返回业务错误；仍需处理提交错误并在等待后检查结果。

## `*MultiGroupBuilder[T].Logger`

```go
func (b *MultiGroupBuilder[T]) Logger(l core.Logger) *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L67) · [行为专题](../group.md)：Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。

**源码说明与使用片段**（原文）：

Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*MultiGroupBuilder[T].Run`

```go
func (b *MultiGroupBuilder[T]) Run(fn func(ctx context.Context, mg *MultiGroup[T]) error) error
```

[实现](../../internal/group/multi_group_builder.go#L79) · [行为专题](../group.md)：Run 终端方法：创建 Group → 应用配置 → 分片 → 执行 fn → Close 所有分片。

**源码说明与使用片段**（原文）：

Run 终端方法：创建 Group → 应用配置 → 分片 → 执行 fn → Close 所有分片。


**参数说明**：

- `fn` (`func(ctx context.Context, mg *MultiGroup[T]) error`)：同步接收本次执行器和上下文（以签名为准）的作用域函数；其返回后触发关闭，返回的 error 向外传播。需处理全部任务时在函数内等待并检查结果，避免把执行器带出作用域继续使用。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Run(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：作用域回调返回后自动关闭，但 Run 的 error 仅来自该回调，不自动汇总任务失败。须在回调内检查提交错误并 Wait 后检查结果或 FirstError；回调 panic 直接传播，defer 仍执行关闭。不要在 Run 返回后继续使用实例提交任务。

## `*MultiGroupBuilder[T].Shards`

```go
func (b *MultiGroupBuilder[T]) Shards(n int) *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L37) · [行为专题](../group.md)：Shards 设置分片数

**源码说明与使用片段**（原文）：

Shards 设置分片数


**参数说明**：

- `n` (`int`)：Shards 设置分片数

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：GroupMulti 保存水平分片数，非正值及 1 都只包装一片，不采用 ShardedGroupBuilder 的默认 4 片。Worker 是每片并发，片数和 worker 相乘决定资源；Run 负责关闭全部分片，结果按片号拼接，不保持全局提交顺序。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.shards = n
```

## `*MultiGroupBuilder[T].SubmitTimeout`

```go
func (b *MultiGroupBuilder[T]) SubmitTimeout(d time.Duration) *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L55) · [行为专题](../group.md)：SubmitTimeout 设置提交超时

**源码说明与使用片段**（原文）：

SubmitTimeout 设置提交超时


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SubmitTimeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存等待任务入队的期限，仅正数在终端创建时应用；DefaultSubmitTimeout 清零，恢复新 Group 的无显式提交计时器状态。不是业务执行期限，仍受提交 context、关闭和 FailFast 影响，必须处理提交错误。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.submitTimeout = d
```

## `*MultiGroupBuilder[T].Timeout`

```go
func (b *MultiGroupBuilder[T]) Timeout(d time.Duration) *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L49) · [行为专题](../group.md)：Timeout 设置任务超时

**源码说明与使用片段**（原文）：

Timeout 设置任务超时


**参数说明**：

- `d` (`time.Duration`)：创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Timeout 保存原值，DefaultTimeout 读取并保存调用当时全局默认值；Build/Run 仅把正数覆盖到新 Group，非正数保留创建时的全局期限，因此 Timeout(0) 不能清除非零全局默认。需明确取消期限时在实例提交前 WithTimeout(0)，业务仍须响应 context。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.timeout = d
```

## `*MultiGroupBuilder[T].Worker`

```go
func (b *MultiGroupBuilder[T]) Worker(n int) *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L43) · [行为专题](../group.md)：Worker 设置并发度

**源码说明与使用片段**（原文）：

Worker 设置并发度


**参数说明**：

- `n` (`int`)：每个 Group 的最大同时执行数；非正值在构造时使用 1。GroupBuilder 默认已保存 IO()，MultiGroupBuilder 零值则使用 1；分片模式按每片计算，总容量乘以片数。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存每个 Group 的并发数，非正值在 NewGroup 按 1；Group/GroupVoid 初始值和 DefaultWorker 显式使用调用时 IO()。GroupMulti 初始值为 0，因此未设置时每片只有 1；分片后总并发乘以片数。不要与终端执行并发修改构建器。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.concurrency = n
```

## `*MultiGroup[T].Close`

```go
func (mg *MultiGroup[T]) Close()
```

[实现](../../internal/group/multigroup.go#L126) · [行为专题](../group.md)：Close 关闭所有分片，释放资源。

**源码说明与使用片段**（原文）：

Close 关闭所有分片，释放资源。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：按片号同步等待全部片收尾后取消共享 context；正常完成首片不会取消其他片使用的共享层。Close 调用各片 Wait，重复 Wait 可读取保存结果，但本批开始等待后不能继续提交。忽略取消且永不返回的业务仍能阻塞，不在所属业务或同步结果回调内 Wait/Close 多组及其分片；下一批优先新建多组。共享层正常收尾后也取消，不能据 ctx.Err() 判断业务失败。

## `*MultiGroup[T].Errors`

```go
func (mg *MultiGroup[T]) Errors() []error
```

[实现](../../internal/group/multigroup.go#L192) · [行为专题](../group.md)：Errors 返回所有分片所有任务的错误切片。

**源码说明与使用片段**（原文）：

Errors 返回所有分片所有任务的错误切片。


**参数说明**：无显式参数。

**返回值签名**：`[]error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Errors()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取当时保存的结果并按片号拼接，不会等待任务，也不是跨片原子快照；完整输出须先整体 Wait。Errors 只收集非 nil Err，Values 只收集 Occupied 且 Err 为 nil 的值，不把稀疏 GoAt 的空槽当成功。返回新切片但元素是浅拷贝，指针或引用成员仍共享；片内是存储顺序，不代表全局提交顺序。

## `*MultiGroup[T].GetShard`

```go
func (mg *MultiGroup[T]) GetShard(idx int) *Group[T]
```

[实现](../../internal/group/multigroup.go#L38) · [行为专题](../group.md)：GetShard 获取指定分片的 Group 实例，用于直接调用 Group 的方法。

**源码说明与使用片段**（原文）：

GetShard 获取指定分片的 Group 实例，用于直接调用 Group 的方法。 idx 越界返回 nil。


**参数说明**：

- `idx` (`int`)：目标分片的零起始索引；负值或不小于分片数时返回 nil。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShard(idx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：ShardCount 返回已创建片数，GetShard 越界返回 nil，有效时返回原片指针，不复制；直接修改/等待该片会影响外层生命周期。各片没有统一 Reset 入口，需要新批次时优先新建多组，勿将单片等待与外层继续提交混用。

## `*MultiGroup[T].Go`

```go
func (mg *MultiGroup[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/group/multigroup.go#L48) · [行为专题](../group.md)：Go 将任务以 round-robin 方式分发到某个分片执行；被目标片拒绝的尝试也消耗轮转位置。

**源码说明与使用片段**（原文）：

Go 将任务以 round-robin 方式分发到某个分片执行；被目标片拒绝的尝试也消耗轮转位置。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Go(ctx, fn)
```

**生产建议**：检查提交错误及最终结果，按每片实际执行资源估算总并发。需要亲和路由时使用稳定 uint64 key；同 key 串行要求可由业务同步保证。使用单 worker 时须确认没有外部 Pool 分发或 AutoScale 改变执行并发；热点片不会自动借用其他片容量。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。普通 Go 从第0片开始原子轮转，使用 uint64 自然回绕；目标片已关闭或 context 取消而拒绝时仍前进。GoKeyed 直接用 uint64 key 对片数取模，不额外哈希、不推进轮转；同 key 只保证同片，片内可并发、不保证业务次序。只提交所选片，不搜索空闲片，返回 error 为该片提交结果；零值没有片会 panic，从 Shard/Builder 创建并统一管理。

## `*MultiGroup[T].GoKeyed`

```go
func (mg *MultiGroup[T]) GoKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error
```

[实现](../../internal/group/multigroup.go#L55) · [行为专题](../group.md)：GoKeyed 按 key % 分片数分发到固定分片，不额外哈希 key 或推进 Go 的轮转位置。

**源码说明与使用片段**（原文）：

GoKeyed 按 key % 分片数分发到固定分片，不额外哈希 key 或推进 Go 的轮转位置。 同一 key 的任务落到同一分片，但执行顺序由该片并发度及调度决定。


**参数说明**：

- `key` (`uint64`)：无符号64位路由值，直接按 key%分片数选片；所有 uint64 值均合法，不额外哈希、不推进普通 Go 的轮转。字符串或业务ID需要哈希分布时先自行生成稳定值。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoKeyed(key, ctx, fn)
```

**生产建议**：检查提交错误及最终结果，按每片实际执行资源估算总并发。需要亲和路由时使用稳定 uint64 key；同 key 串行要求可由业务同步保证。使用单 worker 时须确认没有外部 Pool 分发或 AutoScale 改变执行并发；热点片不会自动借用其他片容量。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。普通 Go 从第0片开始原子轮转，使用 uint64 自然回绕；目标片已关闭或 context 取消而拒绝时仍前进。GoKeyed 直接用 uint64 key 对片数取模，不额外哈希、不推进轮转；同 key 只保证同片，片内可并发、不保证业务次序。只提交所选片，不搜索空闲片，返回 error 为该片提交结果；零值没有片会 panic，从 Shard/Builder 创建并统一管理。

## `*MultiGroup[T].ShardCount`

```go
func (mg *MultiGroup[T]) ShardCount() int
```

[实现](../../internal/group/multigroup.go#L32) · [行为专题](../group.md)：ShardCount 返回分片数。

**源码说明与使用片段**（原文）：

ShardCount 返回分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ShardCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：ShardCount 返回已创建片数，GetShard 越界返回 nil，有效时返回原片指针，不复制；直接修改/等待该片会影响外层生命周期。各片没有统一 Reset 入口，需要新批次时优先新建多组，勿将单片等待与外层继续提交混用。

## `*MultiGroup[T].StreamResults`

```go
func (mg *MultiGroup[T]) StreamResults() <-chan core.Result[T]
```

[实现](../../internal/group/multigroup.go#L237) · [行为专题](../group.md)：StreamResults 返回合并所有分片流式结果的只读 channel。

**源码说明与使用片段**（原文）：

StreamResults 返回合并所有分片流式结果的只读 channel。 每个分片的流式结果会被 fan-in 到此 channel，所有分片的 channel 关闭后自动关闭此 channel。


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StreamResults()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：Group 未启用流时返回 nil；慢消费时溢出暂存，Wait 最终刷新仍写不进 channel 的项会丢失。MultiGroup 每次调用创建新的合并转发器，重复调用会竞争分片流；应只调用一次并持续消费至关闭，完整结果使用 Wait 返回值。

## `*MultiGroup[T].TotalActive`

```go
func (mg *MultiGroup[T]) TotalActive() int
```

[实现](../../internal/group/multigroup.go#L147) · [行为专题](../group.md)：TotalActive 汇总所有分片当前活跃任务数。

**源码说明与使用片段**（原文）：

TotalActive 汇总所有分片当前活跃任务数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalActive()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按分片依次读对应计数后求和，不是全局原子快照；TotalWorker 是配置并发，Active 含登记/排队/执行，Busy 仅执行业务。TotalTaskCount 包含进入预检查的尝试而非接受数，成功/失败也可能与结果记录分时更新；不要用字段差值判断资源已清理。

## `*MultiGroup[T].TotalBusy`

```go
func (mg *MultiGroup[T]) TotalBusy() int
```

[实现](../../internal/group/multigroup.go#L156) · [行为专题](../group.md)：TotalBusy 汇总所有分片当前忙碌任务数。

**源码说明与使用片段**（原文）：

TotalBusy 汇总所有分片当前忙碌任务数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalBusy()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按分片依次读对应计数后求和，不是全局原子快照；TotalWorker 是配置并发，Active 含登记/排队/执行，Busy 仅执行业务。TotalTaskCount 包含进入预检查的尝试而非接受数，成功/失败也可能与结果记录分时更新；不要用字段差值判断资源已清理。

## `*MultiGroup[T].TotalFailCount`

```go
func (mg *MultiGroup[T]) TotalFailCount() int64
```

[实现](../../internal/group/multigroup.go#L174) · [行为专题](../group.md)：TotalFailCount 汇总所有分片失败任务数。

**源码说明与使用片段**（原文）：

TotalFailCount 汇总所有分片失败任务数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalFailCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按分片依次读对应计数后求和，不是全局原子快照；TotalWorker 是配置并发，Active 含登记/排队/执行，Busy 仅执行业务。TotalTaskCount 包含进入预检查的尝试而非接受数，成功/失败也可能与结果记录分时更新；不要用字段差值判断资源已清理。

## `*MultiGroup[T].TotalOkCount`

```go
func (mg *MultiGroup[T]) TotalOkCount() int64
```

[实现](../../internal/group/multigroup.go#L183) · [行为专题](../group.md)：TotalOkCount 汇总所有分片成功任务数。

**源码说明与使用片段**（原文）：

TotalOkCount 汇总所有分片成功任务数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalOkCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按分片依次读对应计数后求和，不是全局原子快照；TotalWorker 是配置并发，Active 含登记/排队/执行，Busy 仅执行业务。TotalTaskCount 包含进入预检查的尝试而非接受数，成功/失败也可能与结果记录分时更新；不要用字段差值判断资源已清理。

## `*MultiGroup[T].TotalTaskCount`

```go
func (mg *MultiGroup[T]) TotalTaskCount() int64
```

[实现](../../internal/group/multigroup.go#L165) · [行为专题](../group.md)：TotalTaskCount 汇总各片进入提交预检查的尝试数，包含预检查拒绝；不是成功入队数。

**源码说明与使用片段**（原文）：

TotalTaskCount 汇总各片进入提交预检查的尝试数，包含预检查拒绝；不是成功入队数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalTaskCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按分片依次读对应计数后求和，不是全局原子快照；TotalWorker 是配置并发，Active 含登记/排队/执行，Busy 仅执行业务。TotalTaskCount 包含进入预检查的尝试而非接受数，成功/失败也可能与结果记录分时更新；不要用字段差值判断资源已清理。

## `*MultiGroup[T].TotalWorker`

```go
func (mg *MultiGroup[T]) TotalWorker() int
```

[实现](../../internal/group/multigroup.go#L138) · [行为专题](../group.md)：TotalWorker 返回所有分片的总并发数。

**源码说明与使用片段**（原文）：

TotalWorker 返回所有分片的总并发数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalWorker()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：按分片依次读对应计数后求和，不是全局原子快照；TotalWorker 是配置并发，Active 含登记/排队/执行，Busy 仅执行业务。TotalTaskCount 包含进入预检查的尝试而非接受数，成功/失败也可能与结果记录分时更新；不要用字段差值判断资源已清理。

## `*MultiGroup[T].Values`

```go
func (mg *MultiGroup[T]) Values() []T
```

[实现](../../internal/group/multigroup.go#L201) · [行为专题](../group.md)：Values 返回所有分片成功任务的结果值。

**源码说明与使用片段**（原文）：

Values 返回所有分片成功任务的结果值。


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：逐片读取当时保存的结果并按片号拼接，不会等待任务，也不是跨片原子快照；完整输出须先整体 Wait。Errors 只收集非 nil Err，Values 只收集 Occupied 且 Err 为 nil 的值，不把稀疏 GoAt 的空槽当成功。返回新切片但元素是浅拷贝，指针或引用成员仍共享；片内是存储顺序，不代表全局提交顺序。

## `*MultiGroup[T].Wait`

```go
func (mg *MultiGroup[T]) Wait() []core.Result[T]
```

[实现](../../internal/group/multigroup.go#L63) · [行为专题](../group.md)：Wait 等待所有分片完成，合并结果（保持各分片内部顺序，分片间不保证全局顺序）。

**源码说明与使用片段**（原文）：

Wait 等待所有分片完成，合并结果（保持各分片内部顺序，分片间不保证全局顺序）。


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：按片号同步等待全部片收尾后取消共享 context；正常完成首片不会取消其他片使用的共享层。Close 调用各片 Wait，重复 Wait 可读取保存结果，但本批开始等待后不能继续提交。忽略取消且永不返回的业务仍能阻塞，不在所属业务或同步结果回调内 Wait/Close 多组及其分片；下一批优先新建多组。共享层正常收尾后也取消，不能据 ctx.Err() 判断业务失败。

## `*MultiGroup[T].WaitContext`

```go
func (mg *MultiGroup[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool)
```

[实现](../../internal/group/multigroup.go#L85) · [行为专题](../group.md)：WaitContext 等待所有分片完成或 ctx 取消，所有分片并行等待共享同一个 ctx。

**源码说明与使用片段**（原文）：

WaitContext 等待所有分片完成或 ctx 取消，所有分片并行等待共享同一个 ctx。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`[]core.Result[T], bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitContext(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：各片并行等待共享 context/截止时间，按片号拼接快照，bool 仅表示全部等待完成而非业务成功。超时/取消请求任务退出，后台仍可收尾；返回前取消共享组 context，最终可再 Wait 读取完整结果。零片返回 nil,true，不保持跨片全局提交顺序。

## `*MultiGroup[T].WaitTimeout`

```go
func (mg *MultiGroup[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool)
```

[实现](../../internal/group/multigroup.go#L75) · [行为专题](../group.md)：WaitTimeout 等待所有分片完成或超时，所有分片并行等待共享同一个超时时钟。

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

**注意事项**：各片并行等待共享 context/截止时间，按片号拼接快照，bool 仅表示全部等待完成而非业务成功。超时/取消请求任务退出，后台仍可收尾；返回前取消共享组 context，最终可再 Wait 读取完整结果。零片返回 nil,true，不保持跨片全局提交顺序。

## `*MultiGroup[T].WithResultCallback`

```go
func (mg *MultiGroup[T]) WithResultCallback(fn func(core.Result[T])) *MultiGroup[T]
```

[实现](../../internal/group/multigroup.go#L228) · [行为专题](../group.md)：WithResultCallback 为所有分片设置结果回调。

**源码说明与使用片段**（原文）：

WithResultCallback 为所有分片设置结果回调。


**参数说明**：

- `fn` (`func(core.Result[T])`)：接收记录结果路径的同步通知，nil 清除；worker、取消或拒绝路径可并发触发，但最早的 waited/waiting 预检查直接追加错误而不通知。不能按回调数统计全部尝试；须并发安全、快速返回，panic 被记录，不在通知中 Wait/Close 本组。

**返回值签名**：`*MultiGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithResultCallback(fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次对全部片应用同名设置，没有跨片原子配置，须在提交前完成。WithTimeout 非正清除各片期限，流非正缓冲按各片 Worker×2；重复启用流替换通道，须重新取得合并流。结果回调可跨片并发，最早预检查拒绝不保证通知，不能用它审计全部尝试。

## `*MultiGroup[T].WithStreaming`

```go
func (mg *MultiGroup[T]) WithStreaming(bufSize int) *MultiGroup[T]
```

[实现](../../internal/group/multigroup.go#L220) · [行为专题](../group.md)：WithStreaming 为所有分片启用流式结果消费。

**源码说明与使用片段**（原文）：

WithStreaming 为所有分片启用流式结果消费。


**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`*MultiGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStreaming(bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：依次对全部片应用同名设置，没有跨片原子配置，须在提交前完成。WithTimeout 非正清除各片期限，流非正缓冲按各片 Worker×2；重复启用流替换通道，须重新取得合并流。结果回调可跨片并发，最早预检查拒绝不保证通知，不能用它审计全部尝试。

## `*MultiGroup[T].WithTimeout`

```go
func (mg *MultiGroup[T]) WithTimeout(d time.Duration) *MultiGroup[T]
```

[实现](../../internal/group/multigroup.go#L212) · [行为专题](../group.md)：WithTimeout 为所有分片设置任务超时。

**源码说明与使用片段**（原文）：

WithTimeout 为所有分片设置任务超时。


**参数说明**：

- `d` (`time.Duration`)：每个任务开始执行后的 context 超时；正数生效，非正值清除本实例配置的执行期限，不包含等待入队时间。分片方法对每片应用，回调必须响应 context；在提交前配置。

**返回值签名**：`*MultiGroup[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：依次对全部片应用同名设置，没有跨片原子配置，须在提交前完成。WithTimeout 非正清除各片期限，流非正缓冲按各片 Worker×2；重复启用流替换通道，须重新取得合并流。结果回调可跨片并发，最早预检查拒绝不保证通知，不能用它审计全部尝试。

## `*NoResult.Active`

```go
func (nr *NoResult) Active() int
```

[实现](../../internal/group/noreturn.go#L263) · [行为专题](../group.md)：Active 返回已登记但未结束的任务数，包含等待入队、排队和执行中的任务。

**源码说明与使用片段**（原文）：

Active 返回已登记但未结束的任务数，包含等待入队、排队和执行中的任务。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Active()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*NoResult.Busy`

```go
func (nr *NoResult) Busy() int
```

[实现](../../internal/group/noreturn.go#L268) · [行为专题](../group.md)：Busy 返回当前执行任务的 goroutine 数。

**源码说明与使用片段**（原文）：

Busy 返回当前执行任务的 goroutine 数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Busy()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*NoResult.Close`

```go
func (nr *NoResult) Close()
```

[实现](../../internal/group/noreturn.go#L299) · [行为专题](../group.md)：Close 关闭任务组，等待所有已提交任务完成并清理资源。

**源码说明与使用片段**（原文）：

Close 关闭任务组，等待所有已提交任务完成并清理资源。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Close()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：Group.Close 调用 Wait，等待已接受任务、worker 及流式收尾；不先强制取消运行中的业务，回调不返回可能一直等待。开始等待后禁止新任务；超时等待后可再 Wait 取得最终结果，复用先在无活跃任务且等待完成时 Reset。

## `*NoResult.DisableAutoScale`

```go
func (nr *NoResult) DisableAutoScale()
```

[实现](../../internal/group/noreturn.go#L309) · [行为专题](../group.md)：DisableAutoScale 停止自动扩缩容（委托给 Group[struct{}]）。

**源码说明与使用片段**（原文）：

DisableAutoScale 停止自动扩缩容（委托给 Group\[struct{}]）。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DisableAutoScale()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：首次 Enable 复制并 Normalize 配置，nil 用默认，保持初始 Worker，应先设在上下界内；已启用重复调用不更新配置，等待中或已等待后不启用。Disable 停监视并调目标至 Min，可能扩容或缩容；Wait 自动停监视，Reset 清配置。目标改变不代表旧 worker 全退出，自动调整不改配置边界。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验全部关系/浮点值；外部池仍限制实际执行并发。

## `*NoResult.EnableAutoScale`

```go
func (nr *NoResult) EnableAutoScale(config *core.AutoScaleConfig)
```

[实现](../../internal/group/noreturn.go#L304) · [行为专题](../group.md)：EnableAutoScale 启用自动扩缩容（委托给 Group[struct{}]）。

**源码说明与使用片段**（原文）：

EnableAutoScale 启用自动扩缩容（委托给 Group\[struct{}]）。


**参数说明**：

- `config` (`*core.AutoScaleConfig`)：自动扩缩容配置指针；nil 时使用默认配置，启用时复制并在副本上 Normalize，不修改调用方配置。读取配置时不得并发修改；启用后的修改不影响已保存的副本。调用会启动后台监视，应在任务前配置并按实例生命周期停止。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.EnableAutoScale(config)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：首次 Enable 复制并 Normalize 配置，nil 用默认，保持初始 Worker，应先设在上下界内；已启用重复调用不更新配置，等待中或已等待后不启用。Disable 停监视并调目标至 Min，可能扩容或缩容；Wait 自动停监视，Reset 清配置。目标改变不代表旧 worker 全退出，自动调整不改配置边界。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验全部关系/浮点值；外部池仍限制实际执行并发。

## `*NoResult.Errors`

```go
func (nr *NoResult) Errors() []error
```

[实现](../../internal/group/noreturn.go#L278) · [行为专题](../group.md)：Errors 返回所有错误。

**源码说明与使用片段**（原文）：

Errors 返回所有错误。


**参数说明**：无显式参数。

**返回值签名**：`[]error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Errors()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：先完成 Wait 再读取完整结果，提前读取只是当时快照。单 Group.Values 同时检查 Occupied 与 Err==nil，返回元素浅拷贝；FirstError 按结果存储顺序选择，不保证最先发生或最早提交。JoinErrors 使用 errors.Join，保留可供 errors.Is/As 检查的错误链。

## `*NoResult.FailCount`

```go
func (nr *NoResult) FailCount() int64
```

[实现](../../internal/group/noreturn.go#L238) · [行为专题](../group.md)：FailCount 返回失败任务数。

**源码说明与使用片段**（原文）：

FailCount 返回失败任务数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*NoResult.FirstError`

```go
func (nr *NoResult) FirstError() error
```

[实现](../../internal/group/noreturn.go#L283) · [行为专题](../group.md)：FirstError 返回第一个错误。

**源码说明与使用片段**（原文）：

FirstError 返回第一个错误。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FirstError()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：先完成 Wait 再读取完整结果，提前读取只是当时快照。单 Group.Values 同时检查 Occupied 与 Err==nil，返回元素浅拷贝；FirstError 按结果存储顺序选择，不保证最先发生或最早提交。JoinErrors 使用 errors.Join，保留可供 errors.Is/As 检查的错误链。

## `*NoResult.Go`

```go
func (nr *NoResult) Go(ctx context.Context, fn func(ctx context.Context) error) error
```

[实现](../../internal/group/noreturn.go#L167) · [行为专题](../group.md)：Go 提交无返回值任务。

**源码说明与使用片段**（原文）：

Go 提交无返回值任务。

参数：

  - ctx：上下文
  - fn：任务执行函数，只返回 error

使用示例：

	nr := group.DefaultNoResult()
	nr.Go(ctx, func(ctx context.Context) error {
	    return db.Insert(ctx, record)
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `fn` (`func(ctx context.Context) error`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Go(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。error 表示提交是否接受，不表示业务成功；普通 Go 的结果按记录完成顺序保存，不保证提交顺序。等待开始/完成后拒绝新任务，预检查拒绝也可能增加总数和错误存储。业务 panic 转为 PanicError，context 及超时只请求协作退出；提交可能因队列满阻塞，须处理提交错误并最终等待检查逐项 Err。

## `*NoResult.GoAt`

```go
func (nr *NoResult) GoAt(index int, ctx context.Context, fn func(ctx context.Context) error) error
```

[实现](../../internal/group/noreturn.go#L191) · [行为专题](../group.md)：GoAt 提交无返回值任务到指定索引。

**源码说明与使用片段**（原文）：

GoAt 提交无返回值任务到指定索引。

参数：

  - index：在结果切片中的索引位置
  - ctx：上下文
  - fn：任务执行函数


**参数说明**：

- `index` (`int`)：结果切片的零起始索引；负值返回 ErrInvalidIndex。稀疏索引会分配到 index+1 长度并留下未占用槽位；输入顺序需要不同且连续的索引。
- `ctx` (`context.Context`)：上下文
- `fn` (`func(ctx context.Context) error`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoAt(index, ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。负索引直接返回 ErrInvalidIndex 且不计数；非负索引可扩展结果并产生空槽，没有 Pool 的 MaxPoolIndex 限制，须控制内存。重复索引会将旧结果移动到其他槽位，不能实现任务替换；使用唯一连续索引保持对应关系。返回 error 只表示提交，业务 panic 转为 PanicError，最终 Wait 并核对 Err/Occupied。

## `*NoResult.GoAtWithTimeout`

```go
func (nr *NoResult) GoAtWithTimeout(index int, ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error
```

[实现](../../internal/group/noreturn.go#L204) · [行为专题](../group.md)：GoAtWithTimeout 提交无返回值任务到指定索引并设置超时。

**源码说明与使用片段**（原文）：

GoAtWithTimeout 提交无返回值任务到指定索引并设置超时。

参数：

  - index：在结果切片中的索引位置
  - ctx：上下文
  - timeout：任务超时时间
  - fn：任务执行函数


**参数说明**：

- `index` (`int`)：结果切片的零起始索引；负值返回 ErrInvalidIndex。稀疏索引会分配到 index+1 长度并留下未占用槽位；输入顺序需要不同且连续的索引。
- `ctx` (`context.Context`)：上下文
- `timeout` (`time.Duration`)：覆盖本次任务的 Group 执行超时；正数从回调开始执行时计时，非正值清除 Group 级期限，仍受父 context 约束。注入外部池时，该池原有任务超时仍可能生效。与 Pool.SubmitWithCompletionTimeout 的非正值继承规则不同。
- `fn` (`func(ctx context.Context) error`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoAtWithTimeout(index, ctx, timeout, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。负索引直接返回 ErrInvalidIndex 且不计数；非负索引可扩展结果并产生空槽，没有 Pool 的 MaxPoolIndex 限制，须控制内存。重复索引会将旧结果移动到其他槽位，不能实现任务替换；使用唯一连续索引保持对应关系。返回 error 只表示提交，业务 panic 转为 PanicError，最终 Wait 并核对 Err/Occupied。

## `*NoResult.GoWithTimeout`

```go
func (nr *NoResult) GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error
```

[实现](../../internal/group/noreturn.go#L179) · [行为专题](../group.md)：GoWithTimeout 提交无返回值任务并设置超时。

**源码说明与使用片段**（原文）：

GoWithTimeout 提交无返回值任务并设置超时。

参数：

  - ctx：上下文
  - timeout：任务超时时间
  - fn：任务执行函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `timeout` (`time.Duration`)：覆盖本次任务的 Group 执行超时；正数从回调开始执行时计时，非正值清除 Group 级期限，仍受父 context 约束。注入外部池时，该池原有任务超时仍可能生效。与 Pool.SubmitWithCompletionTimeout 的非正值继承规则不同。
- `fn` (`func(ctx context.Context) error`)：被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoWithTimeout(ctx, timeout, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。error 表示提交是否接受，不表示业务成功；普通 Go 的结果按记录完成顺序保存，不保证提交顺序。等待开始/完成后拒绝新任务，预检查拒绝也可能增加总数和错误存储。业务 panic 转为 PanicError，context 及超时只请求协作退出；提交可能因队列满阻塞，须处理提交错误并最终等待检查逐项 Err。

## `*NoResult.HasError`

```go
func (nr *NoResult) HasError() bool
```

[实现](../../internal/group/noreturn.go#L248) · [行为专题](../group.md)：HasError 检查是否有任务失败。

**源码说明与使用片段**（原文）：

HasError 检查是否有任务失败。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示观察到了错误，使用 Error/Errors 获取错误值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.HasError()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*NoResult.IsAutoScaleEnabled`

```go
func (nr *NoResult) IsAutoScaleEnabled() bool
```

[实现](../../internal/group/noreturn.go#L314) · [行为专题](../group.md)：IsAutoScaleEnabled 返回是否已启用自动扩缩容。

**源码说明与使用片段**（原文）：

IsAutoScaleEnabled 返回是否已启用自动扩缩容。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsAutoScaleEnabled()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：首次 Enable 复制并 Normalize 配置，nil 用默认，保持初始 Worker，应先设在上下界内；已启用重复调用不更新配置，等待中或已等待后不启用。Disable 停监视并调目标至 Min，可能扩容或缩容；Wait 自动停监视，Reset 清配置。目标改变不代表旧 worker 全退出，自动调整不改配置边界。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验全部关系/浮点值；外部池仍限制实际执行并发。

## `*NoResult.JoinErrors`

```go
func (nr *NoResult) JoinErrors() error
```

[实现](../../internal/group/noreturn.go#L288) · [行为专题](../group.md)：JoinErrors 使用 errors.Join 合并所有错误。

**源码说明与使用片段**（原文）：

JoinErrors 使用 errors.Join 合并所有错误。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.JoinErrors()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：先完成 Wait 再读取完整结果，提前读取只是当时快照。单 Group.Values 同时检查 Occupied 与 Err==nil，返回元素浅拷贝；FirstError 按结果存储顺序选择，不保证最先发生或最早提交。JoinErrors 使用 errors.Join，保留可供 errors.Is/As 检查的错误链。

## `*NoResult.OkCount`

```go
func (nr *NoResult) OkCount() int64
```

[实现](../../internal/group/noreturn.go#L243) · [行为专题](../group.md)：OkCount 返回成功任务数。

**源码说明与使用片段**（原文）：

OkCount 返回成功任务数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OkCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*NoResult.Reset`

```go
func (nr *NoResult) Reset() (*NoResult, error)
```

[实现](../../internal/group/noreturn.go#L293) · [行为专题](../group.md)：Reset 重置 NoResult 状态以便复用。

**源码说明与使用片段**（原文）：

Reset 重置 NoResult 状态以便复用。


**参数说明**：无显式参数。

**返回值签名**：`*NoResult, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reset()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：单 Group 要求已经完成等待且无活跃任务，否则返回错误并保持当前实例。清除结果、计数、context/FailFast 和 AutoScale；保留并发度、任务/提交超时、结果回调、流容量以及外部池引用。须在外部同步下单独调用，不与提交/等待竞争；重新取得 StreamResults，新批次重新设置 context/FailFast/AutoScale。

## `*NoResult.Stats`

```go
func (nr *NoResult) Stats() GroupStats
```

[实现](../../internal/group/noreturn.go#L273) · [行为专题](../group.md)：Stats 获取运行时统计快照。

**源码说明与使用片段**（原文）：

Stats 获取运行时统计快照。


**参数说明**：无显式参数。

**返回值签名**：`GroupStats`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stats()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `*NoResult.StreamResults`

```go
func (nr *NoResult) StreamResults() <-chan core.Result[struct{}]
```

[实现](../../internal/group/noreturn.go#L151) · [行为专题](../group.md)：StreamResults 返回流式结果的只读 channel，实时消费任务完成事件。

**源码说明与使用片段**（原文）：

StreamResults 返回流式结果的只读 channel，实时消费任务完成事件。


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[struct{}]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StreamResults()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：Group 未启用流时返回 nil；慢消费时溢出暂存，Wait 最终刷新仍写不进 channel 的项会丢失。MultiGroup 每次调用创建新的合并转发器，重复调用会竞争分片流；应只调用一次并持续消费至关闭，完整结果使用 Wait 返回值。

## `*NoResult.TotalCount`

```go
func (nr *NoResult) TotalCount() int64
```

[实现](../../internal/group/noreturn.go#L253) · [行为专题](../group.md)：TotalCount 返回已提交任务总数。

**源码说明与使用片段**（原文）：

TotalCount 返回已提交任务总数。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TotalCount()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：计数包含进入 Group 提交预检查的尝试，即使预检查拒绝也可能增加；负索引在预检查前拒绝而不计数。不是成功入队或执行完成数量，MultiGroup 按片求和且不是跨片原子快照。

## `*NoResult.Wait`

```go
func (nr *NoResult) Wait()
```

[实现](../../internal/group/noreturn.go#L211) · [行为专题](../group.md)：Wait 等待所有无返回值任务完成。

**源码说明与使用片段**（原文）：

Wait 等待所有无返回值任务完成。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：Group.Close 调用 Wait，等待已接受任务、worker 及流式收尾；不先强制取消运行中的业务，回调不返回可能一直等待。开始等待后禁止新任务；超时等待后可再 Wait 取得最终结果，复用先在无活跃任务且等待完成时 Reset。

## `*NoResult.WaitContext`

```go
func (nr *NoResult) WaitContext(ctx context.Context) (completed int64, ok bool)
```

[实现](../../internal/group/noreturn.go#L229) · [行为专题](../group.md)：WaitContext 等待任务完成或 ctx 取消。

**源码说明与使用片段**（原文）：

WaitContext 等待任务完成或 ctx 取消。 completed 是当前结果切片长度，含 GoAt 产生的空槽，并非成功或已完成任务数。 等待取消后任务可能继续退出清理，应在最终 Wait 后查询完整统计。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`completed int64, ok bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitContext(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：completed 为本次快照的结果切片长度，包含稀疏 GoAt 的空槽，不能当完成/成功数；bool 只标识等待结束，不表示业务成功。结果已保存时同步通知仍可能阻塞，ok=false 时不能据槽位数 Reset；超时/取消请求任务退出，后台收尾继续，最终再 Wait 后检查完整计数/错误；不要在所属业务或结果通知中等待自身。

## `*NoResult.WaitTimeout`

```go
func (nr *NoResult) WaitTimeout(d time.Duration) (completed int64, ok bool)
```

[实现](../../internal/group/noreturn.go#L218) · [行为专题](../group.md)：WaitTimeout 等待任务完成，最多等待 d。

**源码说明与使用片段**（原文）：

WaitTimeout 等待任务完成，最多等待 d。 completed 是当前结果切片长度，含 GoAt 产生的空槽，并非成功或已完成任务数。 ok 只表示等待完成，业务失败数请在等待后通过 FailCount 查询。


**参数说明**：

- `d` (`time.Duration`)：本次等待的最长时长；非正值使等待 context 立即过期，完成与超时同时就绪时不保证 bool 的选择。超时返回部分结果并通知取消，不能强制停止任务；分片共享同一截止时间，复用须遵守该实例的等待/复位约定。

**返回值签名**：`completed int64, ok bool`。末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitTimeout(d)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：completed 为本次快照的结果切片长度，包含稀疏 GoAt 的空槽，不能当完成/成功数；bool 只标识等待结束，不表示业务成功。结果已保存时同步通知仍可能阻塞，ok=false 时不能据槽位数 Reset；超时/取消请求任务退出，后台收尾继续，最终再 Wait 后检查完整计数/错误；不要在所属业务或结果通知中等待自身。

## `*NoResult.WithContext`

```go
func (nr *NoResult) WithContext(ctx context.Context) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L49) · [行为专题](../group.md)：委托同名实现：WithContext 绑定 context，支持通过 cancel 统一取消所有任务。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithContext(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithCtxSto`

```go
func (nr *NoResult) WithCtxSto(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L116) · [行为专题](../group.md)：委托同名实现：WithCtxSto 设置 Context 和提交超时（Sto = SubmitTimeout）。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxSto(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithCtxStoTID`

```go
func (nr *NoResult) WithCtxStoTID(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L121) · [行为专题](../group.md)：委托同名实现：WithCtxStoTID 设置 Context、提交超时并注入 trace_id。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxStoTID(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithCtxTOTID`

```go
func (nr *NoResult) WithCtxTOTID(ctx context.Context, timeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L131) · [行为专题](../group.md)：委托同名实现：WithCtxTOTID 设置 Context、任务超时并注入 trace_id。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTOTID(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithCtxTimeout`

```go
func (nr *NoResult) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L126) · [行为专题](../group.md)：委托同名实现：WithCtxTimeout 设置 Context 和任务超时。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTimeout(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithCtxTraceID`

```go
func (nr *NoResult) WithCtxTraceID(ctx context.Context) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L111) · [行为专题](../group.md)：委托同名实现：WithCtxTraceID 设置 Context 并注入 trace_id。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithCtxTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFFCtx`

```go
func (nr *NoResult) WithFFCtx(ctx context.Context) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L59) · [行为专题](../group.md)：委托同名实现：WithFFCtx 等同 WithFailFast。提供更短的别名。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFCtx(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFFSto`

```go
func (nr *NoResult) WithFFSto(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L81) · [行为专题](../group.md)：委托同名实现：WithFFSto 启用 FailFast 并设置提交超时（Sto = SubmitTimeout）。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFSto(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFFStoTID`

```go
func (nr *NoResult) WithFFStoTID(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L91) · [行为专题](../group.md)：委托同名实现：WithFFStoTID 启用 FailFast、设置提交超时并注入 trace_id。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFStoTID(ctx, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFFTOSto`

```go
func (nr *NoResult) WithFFTOSto(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L101) · [行为专题](../group.md)：委托同名实现：WithFFTOSto 启用 FailFast、设置任务超时和提交超时（TO=Timeout, Sto=SubmitTimeout）。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOSto(ctx, timeout, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFFTOStoTID`

```go
func (nr *NoResult) WithFFTOStoTID(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L106) · [行为专题](../group.md)：委托同名实现：WithFFTOStoTID 启用 FailFast、设置超时、提交超时并注入 trace_id（TO=Timeout, Sto=SubmitTimeout, TID=TraceID）。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。
- `submitTimeout` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOStoTID(ctx, timeout, submitTimeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFFTOTID`

```go
func (nr *NoResult) WithFFTOTID(ctx context.Context, timeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L96) · [行为专题](../group.md)：委托同名实现：WithFFTOTID 启用 FailFast、设置超时并注入 trace_id（TO = Timeout, TID = TraceID）。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTOTID(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFFTimeout`

```go
func (nr *NoResult) WithFFTimeout(ctx context.Context, timeout time.Duration) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L86) · [行为专题](../group.md)：委托同名实现：WithFFTimeout 启用 FailFast 并设置任务超时。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTimeout(ctx, timeout)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFFTraceID`

```go
func (nr *NoResult) WithFFTraceID(ctx context.Context) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L76) · [行为专题](../group.md)：委托同名实现：WithFFTraceID 启用 FailFast 并注入 trace_id。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFFTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithFailFast`

```go
func (nr *NoResult) WithFailFast(ctx context.Context) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L54) · [行为专题](../group.md)：委托同名实现：WithFailFast 启用快速失败模式：任一任务出错时取消所有其他任务。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithFailFast(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.WithResultCallback`

```go
func (nr *NoResult) WithResultCallback(fn func(core.Result[struct{}])) *NoResult
```

[实现](../../internal/group/noreturn.go#L145) · [行为专题](../group.md)：WithResultCallback 设置结果记录路径的同步回调，委托 Group。

**源码说明与使用片段**（原文）：

WithResultCallback 设置结果记录路径的同步回调，委托 Group。 worker、提交取消或拒绝路径均可能调用；最早的 waited/waiting 拒绝不通知。


**参数说明**：

- `fn` (`func(core.Result[struct{}])`)：接收记录结果路径的同步通知，nil 清除；worker、取消或拒绝路径可并发触发，但最早的 waited/waiting 预检查直接追加错误而不通知。不能按回调数统计全部尝试；须并发安全、快速返回，panic 被记录，不在通知中 Wait/Close 本组。

**返回值签名**：`*NoResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithResultCallback(fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存同步通知函数，nil 清除；可能从 worker、拒绝或清理路径并发调用，panic 被记录，不改原结果。正常执行先保存结果并尝试发送流，再同步通知；通知返回后成功计数才增加，Active/Busy 才释放，最终等待和流关闭也等待通知。最早的 waited/waiting 预检查分支直接追加错误，不通知回调，不能按通知数统计全部提交尝试。必须快速返回且并发安全，不在其中 Wait/Close 当前组。

## `*NoResult.WithStreaming`

```go
func (nr *NoResult) WithStreaming(bufSize int) *NoResult
```

[实现](../../internal/group/noreturn.go#L138) · [行为专题](../group.md)：WithStreaming 启用流式结果消费，结果通过 channel 实时发送。

**源码说明与使用片段**（原文）：

WithStreaming 启用流式结果消费，结果通过 channel 实时发送。 bufSize 控制 channel 缓冲大小，0 使用 concurrency\*2 的默认值。


**参数说明**：

- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`*NoResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithStreaming(bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：立即新建结果通道，非正 bufSize 使用 Worker×2；重复调用替换引用而不关闭旧通道，应在提交前配置一次并重新取得 StreamResults。慢消费时暂存，Wait 收尾无法刷入通道的结果会丢失；完整结果用 Wait。不要在业务或结果通知中等待自身流关闭。

## `*NoResult.WithSubmitTimeout`

```go
func (nr *NoResult) WithSubmitTimeout(d time.Duration) *NoResult
```

[实现](../../internal/group/noreturn.go#L71) · [行为专题](../group.md)：WithSubmitTimeout 设置获取并发槽位的等待超时。

**源码说明与使用片段**（原文）：

WithSubmitTimeout 设置获取并发槽位的等待超时。


**参数说明**：

- `d` (`time.Duration`)：等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。

**返回值签名**：`*NoResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithSubmitTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：原地保存设置并返回自身；WithTimeout 正数从业务开始计时，非正值清除组期限，外部池自身期限仍可能生效。WithSubmitTimeout 只限制等待入组队列，不覆盖任务执行或分发进外部池的等待；非正值无计时器，仍受 context/等待关闭影响。配置在提交前完成，检查提交错误及最终业务结果。

## `*NoResult.WithTimeout`

```go
func (nr *NoResult) WithTimeout(d time.Duration) *NoResult
```

[实现](../../internal/group/noreturn.go#L65) · [行为专题](../group.md)：WithTimeout 设置每个任务超时时间。

**源码说明与使用片段**（原文）：

WithTimeout 设置每个任务超时时间。


**参数说明**：

- `d` (`time.Duration`)：每个任务开始执行后的 context 超时；正数生效，非正值清除本实例配置的执行期限，不包含等待入队时间。分片方法对每片应用，回调必须响应 context；在提交前配置。

**返回值签名**：`*NoResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：原地保存设置并返回自身；WithTimeout 正数从业务开始计时，非正值清除组期限，外部池自身期限仍可能生效。WithSubmitTimeout 只限制等待入组队列，不覆盖任务执行或分发进外部池的等待；非正值无计时器，仍受 context/等待关闭影响。配置在提交前完成，检查提交错误及最终业务结果。

## `*NoResult.WithTraceID`

```go
func (nr *NoResult) WithTraceID(ctx context.Context) (*NoResult, context.Context)
```

[实现](../../internal/group/noreturn.go#L44) · [行为专题](../group.md)：委托同名实现：WithTraceID 确保 context 中包含 TraceID，便于链路追踪。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*NoResult, context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。

## `*NoResult.Worker`

```go
func (nr *NoResult) Worker() int
```

[实现](../../internal/group/noreturn.go#L258) · [行为专题](../group.md)：Worker 返回最大并发数。

**源码说明与使用片段**（原文）：

Worker 返回最大并发数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。

## `BuildAggregateNoResult`

```go
func BuildAggregateNoResult(nr *NoResult) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/group/noreturn.go#L324) · [行为专题](../group.md)：BuildAggregateNoResult 从已 Wait 的 NoResult 中构建聚合结果，用于 ForEach/ForEachWithFailFast 等无返回值场景。

**源码说明与使用片段**（原文）：

BuildAggregateNoResult 从已 Wait 的 NoResult 中构建聚合结果，用于 ForEach/ForEachWithFailFast 等无返回值场景。

参数：

  - nr：已 Wait 的 NoResult 实例


**参数说明**：

- `nr` (`*NoResult`)：已 Wait 的 NoResult 实例

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
group.BuildAggregateNoResult(nr)
```

**生产建议**：在最终 Wait 后读取，占位与成功需区分；检查 failCnt/firstErr，并按 Occupied 过滤稀疏空槽。修改返回切片前确认 error 中共享对象的所有权；外部业务使用 async.BuildAggregateNoResult。

**注意事项**：用于非 nil 且已完成最终 Wait 的 NoResult；复制结果槽位后仅统计 Occupied 项，首错按槽位顺序选择，不按发生时间。返回结果含稀疏空槽，其长度可能大于 total；结果及元素为浅拷贝。它不等待、不关闭、不修改 Group 计数，也不合并外部 Pool 的其他任务。

## `DefaultGroup`

```go
func DefaultGroup[T any]() *Group[T]
```

[实现](../../internal/group/group.go#L139) · [行为专题](../group.md)：DefaultGroup 使用 core.IO() 作为默认并发数创建 Group。

**源码说明与使用片段**（原文）：

DefaultGroup 使用 core.IO() 作为默认并发数创建 Group。

使用示例：

	g := group.DefaultGroup[string]()
	g.Go(ctx, myTask)


**参数说明**：无显式参数。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
group.DefaultGroup[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：构造时立即启动 worker，非正 concurrency 按 1，Default 入口显式使用 IO()；队列容量为有效 worker 数。复制创建时全局任务超时，不读取全局提交超时，不是零值可用类型。调用方最终 Wait/Close，配置在提交前完成；NoResult 是基于 Group[struct{}] 定义的独立类型，Wait 本身不返回结果。

## `DefaultNoResult`

```go
func DefaultNoResult() *NoResult
```

[实现](../../internal/group/noreturn.go#L40) · [行为专题](../group.md)：DefaultNoResult 使用默认并发数创建 NoResult。

**源码说明与使用片段**（原文）：

DefaultNoResult 使用默认并发数创建 NoResult。


**参数说明**：无显式参数。

**返回值签名**：`*NoResult`。

**使用示例**（已初始化参数的调用片段）：

```go
group.DefaultNoResult()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：构造时立即启动 worker，非正 concurrency 按 1，Default 入口显式使用 IO()；队列容量为有效 worker 数。复制创建时全局任务超时，不读取全局提交超时，不是零值可用类型。调用方最终 Wait/Close，配置在提交前完成；NoResult 是基于 Group[struct{}] 定义的独立类型，Wait 本身不返回结果。

## `FillNoResultSkipped`

```go
func FillNoResultSkipped(nr *NoResult, total int)
```

[实现](../../internal/group/noreturn.go#L350) · [行为专题](../group.md)：FillNoResultSkipped 在 nr.Wait() 之后补齐缺失的结果槽位；仅在 total 大于当前总数时增长至 total。

**源码说明与使用片段**（原文）：

FillNoResultSkipped 在 nr.Wait() 之后补齐缺失的结果槽位；仅在 total 大于当前总数时增长至 total。

参数：

  - nr：已 Wait 的 NoResult 实例
  - total：期望的总任务数；非正值或不大于当前总数时无操作


**参数说明**：

- `nr` (`*NoResult`)：已 Wait 的 NoResult 实例
- `total` (`int`)：期望的总任务数；非正值或不大于当前总数时无操作

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
group.FillNoResultSkipped(nr, total)
```

**生产建议**：传入已知输入任务总数，最终 Wait 后补齐；先验证总数与内存预算。需增长时会扫描现有槽位，最坏时间 O(结果长度+新增数)；若追加触发扩容，新数组空间也与结果长度加新增数成正比。随后重新聚合失败，不依赖结果流或回调接收合成占位；外部业务使用 async.FillNoResultSkipped。

**注意事项**：仅在非 nil NoResult 的最终 Wait 完成且无活跃任务后使用。total 大于 TotalCount 时先按索引升序填空槽、再追加 ErrSkipped 占位，并增加总数和失败数；非正目标（包括 MinInt）及不大于现有总数的目标均无操作，不缩短或校正已有结果。不会执行业务、补发结果回调或向已关闭的流发送，填补之后通过聚合或错误查询读取。正数目标不设上限，须控制分配预算。

## `NewGroup`

```go
func NewGroup[T any](concurrency int) *Group[T]
```

[实现](../../internal/group/group.go#L111) · [行为专题](../group.md)：NewGroup 创建一个新的任务组，限制最多 concurrency 个 goroutine 同时执行。

**源码说明与使用片段**（原文）：

NewGroup 创建一个新的任务组，限制最多 concurrency 个 goroutine 同时执行。 concurrency \<= 0 时自动设为 1。

参数：

  - concurrency：最大并发数，\<=0 时默认 1

使用示例：

	g := group.NewGroup[string](5) // 最多 5 个任务并发
	g.WithTimeout(10 * time.Second)
	for _, url := range urls {
	    g.Go(ctx, func(ctx context.Context) (string, error) {
	        return httpGet(ctx, url)
	    })
	}
	results := g.Wait()


**参数说明**：

- `concurrency` (`int`)：每个 Group 的最大同时执行数；非正值在构造时使用 1。GroupBuilder 默认已保存 IO()，MultiGroupBuilder 零值则使用 1；分片模式按每片计算，总容量乘以片数。

**返回值签名**：`*Group[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
group.NewGroup[T](concurrency)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：构造时立即启动 worker，非正 concurrency 按 1，Default 入口显式使用 IO()；队列容量为有效 worker 数。复制创建时全局任务超时，不读取全局提交超时，不是零值可用类型。调用方最终 Wait/Close，配置在提交前完成；NoResult 是基于 Group[struct{}] 定义的独立类型，Wait 本身不返回结果。

## `NewGroupBuilder`

```go
func NewGroupBuilder[T any]() *GroupBuilder[T]
```

[实现](../../internal/group/group_builder.go#L56) · [行为专题](../group.md)：NewGroupBuilder 创建任务组构造器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewGroupBuilder 创建任务组构造器，默认使用 context.Background()。 通过 .Context(ctx) 链式设置上下文。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
group.NewGroupBuilder[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建独立可变构建器，父 context 初始 Background，并发初始 IO()，此时不启动 worker。配置供后续 Build/Run 使用，不能与终端执行并发修改。Run 管理关闭，Build 返回实例由调用方 Wait/Close。

## `NewGroupNoResultBuilder`

```go
func NewGroupNoResultBuilder() *GroupNoResultBuilder
```

[实现](../../internal/group/noresult_builder.go#L44) · [行为专题](../group.md)：NewGroupNoResultBuilder 创建无返回值任务组构造器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewGroupNoResultBuilder 创建无返回值任务组构造器，默认使用 context.Background()。 通过 .Context(ctx) 链式设置上下文。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
group.NewGroupNoResultBuilder()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建独立可变构建器，父 context 初始 Background，并发初始 IO()，此时不启动 worker。配置供后续 Build/Run 使用，不能与终端执行并发修改。Run 管理关闭，Build 返回实例由调用方 Wait/Close。

## `NewMultiBuilder`

```go
func NewMultiBuilder[T any]() *MultiGroupBuilder[T]
```

[实现](../../internal/group/multi_group_builder.go#L24) · [行为专题](../group.md)：NewMultiBuilder 创建分片任务组构造器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewMultiBuilder 创建分片任务组构造器，默认使用 context.Background()。 通过 .Context(ctx) 链式设置上下文。


**参数说明**：无显式参数。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
group.NewMultiBuilder[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建独立可变构建器，父 context 初始 Background，并发和片数初始 0，Run 创建时每片并发按 1 且共一片；此时不启动 worker。仅有 Run 终端并管理关闭，仍须在回调中等待和检查业务结果；配置不能与 Run 并发。

## `NewNoResult`

```go
func NewNoResult(concurrency int) *NoResult
```

[实现](../../internal/group/noreturn.go#L35) · [行为专题](../group.md)：NewNoResult 创建 NoResult 类型任务组。

**源码说明与使用片段**（原文）：

NewNoResult 创建 NoResult 类型任务组。

参数：

  - concurrency：最大并发数，\<=0 时默认 1


**参数说明**：

- `concurrency` (`int`)：每个 Group 的最大同时执行数；非正值在构造时使用 1。GroupBuilder 默认已保存 IO()，MultiGroupBuilder 零值则使用 1；分片模式按每片计算，总容量乘以片数。

**返回值签名**：`*NoResult`。

**使用示例**（已初始化参数的调用片段）：

```go
group.NewNoResult(concurrency)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：构造时立即启动 worker，非正 concurrency 按 1，Default 入口显式使用 IO()；队列容量为有效 worker 数。复制创建时全局任务超时，不读取全局提交超时，不是零值可用类型。调用方最终 Wait/Close，配置在提交前完成；NoResult 是基于 Group[struct{}] 定义的独立类型，Wait 本身不返回结果。

## `WithMultiGroup`

```go
func WithMultiGroup[T any](concurrency int, shards int, fn func(mg *MultiGroup[T]) error) error
```

[实现](../../internal/group/multigroup.go#L279) · [行为专题](../group.md)：WithMultiGroup 创建分片任务组并执行 fn，fn 返回后自动 Close 所有分片释放资源。

**源码说明与使用片段**（原文）：

WithMultiGroup 创建分片任务组并执行 fn，fn 返回后自动 Close 所有分片释放资源。 内部先创建并发度为 concurrency 的 Group，再水平分片为 shards 份。 concurrency \<= 0 时默认 1，shards \<= 1 时等同于单 Group。 Group 和 MultiGroup 均提供 Close；本函数会在 fn 返回后关闭 MultiGroup。

示例：

	err := group.WithMultiGroup(64, 16, func(mg *MultiGroup[string]) error {
	    for _, item := range items {
	        if err := mg.Go(ctx, func(ctx context.Context) (string, error) {
	            return process(item)
	        }); err != nil { return err }
	    }
	    for _, result := range mg.Wait() {
	        if result.Err != nil { return result.Err }
	    }
	    return nil
	})


**参数说明**：

- `concurrency` (`int`)：每片 Group 的并发上限；非正值使用 1，总容量乘以有效分片数。作用域函数返回后关闭全部分片，需取得全部结果时在函数内等待并检查错误。
- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。
- `fn` (`func(mg *MultiGroup[T]) error`)：同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
group.WithMultiGroup[T](concurrency, shards, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即创建每片 concurrency 个 worker 的多组，非正并发按 1，shards<=1 只有一片；同步调用非 nil fn 后 defer Close 全部。error 仅来自 fn，panic 直接传播；业务失败需在 fn 内等待并检查，不能将实例带出作用域继续提交。

## `GroupStats.String`

```go
func (s GroupStats) String() string
```

[实现](../../internal/group/group.go#L1068) · [行为专题](../group.md)：返回统计快照的格式化字符串。

**参数说明**：无显式参数。

**返回值签名**：`string`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.String()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅格式化该统计值已保存的字段，不刷新 Group，也不等待任务；字段来自采样时刻且非事务一致快照。日志中需要错误详情时另行读取已完成批次的结果，不从字符串反推执行完成。

