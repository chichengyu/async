# async 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [config](../config.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

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
async.BuildAggregateNoResult(nr)
```

**生产建议**：在最终 Wait 后读取，占位与成功需区分；检查 failCnt/firstErr，并按 Occupied 过滤稀疏空槽。修改返回切片前确认 error 中共享对象的所有权；外部业务使用 async.BuildAggregateNoResult。

**注意事项**：用于非 nil 且已完成最终 Wait 的 NoResult；复制结果槽位后仅统计 Occupied 项，首错按槽位顺序选择，不按发生时间。返回结果含稀疏空槽，其长度可能大于 total；结果及元素为浅拷贝。它不等待、不关闭、不修改 Group 计数，也不合并外部 Pool 的其他任务。

## `CPU`

```go
func CPU() int
```

[实现](../../internal/core/core.go#L666) · [行为专题](../config.md)：CPU 返回 CPU 密集型并发度，等于 runtime.NumCPU()。

**源码说明与使用片段**（原文）：

CPU 返回 CPU 密集型并发度，等于 runtime.NumCPU()。 适用于计算密集型任务，建议每个 CPU 核心分配一个 goroutine。

使用示例：

	// 8 核机器返回 8
	async.Pool[int]().Worker(async.CPU()).Run(fn)


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
async.CPU()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。

## `DefaultPoolConfig`

```go
func DefaultPoolConfig() Config
```

[实现](../../internal/pool/pool.go#L219) · [行为专题](../pool.md)：DefaultConfig 返回使用默认 Size（core.IO()）的 Config。

**源码说明与使用片段**（原文）：

DefaultConfig 返回使用默认 Size（core.IO()）的 Config。 MaxResults 默认值为 -1，表示不覆盖 Pool 创建时的全局默认值（初始 100\_000）。 调用链式方法 .WithMaxResults(n) 可显式覆盖（n=0 恢复无限，n>0 设置上限）。


**参数说明**：无显式参数。

**返回值签名**：`Config`。

**使用示例**（已初始化参数的调用片段）：

```go
async.DefaultPoolConfig()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。

## `EnsureTraceID`

```go
func EnsureTraceID(ctx context.Context) context.Context
```

[实现](../../internal/core/core.go#L792) · [行为专题](../config.md)：EnsureTraceID 保留当前 key 下的已有非 nil 值；缺失时自动生成 trace_id。

**源码说明与使用片段**（原文）：

EnsureTraceID 保留当前 key 下的已有非 nil 值；缺失时自动生成 trace\_id。 只有缺失值生成路径在启用 TraceLog 时注入 logger 上下文；不修复空字符串或非 string 值。

参数：

  - ctx：原始上下文

使用示例：

	ctx := core.EnsureTraceID(context.Background())
	// 后续所有使用该 ctx 的操作都会携带相同的 trace_id
	traceID := core.GetTraceID(ctx) // 如 "a1b2c3d4e5f6..."


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
async.EnsureTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：传非 nil context。EnsureTraceID 仅检查当前 key 的值是否为 nil，非 nil 时原样返回，包括空字符串和非 string；它不修复类型。WithTraceID 的空 id 委托 EnsureTraceID，保留已有值；非空 id 才覆盖。启用 TraceLog 时，仅生成/显式覆盖路径调用 logger.With/WithContext，后端必须返回有效 context。GetTraceID 对非 string 值会 panic。自定义 key 在初始化时固定，已有 context 的旧值不会自动迁移；继续传递返回 context。

## `ExecuteStream`

```go
func ExecuteStream[T any](
	ctx context.Context,
	stages []Stage[T],
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error)
```

[实现](../../async.go#L876) · [行为专题](../pipeline.md)：ExecuteStream 一次性流水线执行：所有阶段同时运转，元素流经阶段间 channel。

**源码说明与使用片段**（原文）：

ExecuteStream 一次性流水线执行：所有阶段同时运转，元素流经阶段间 channel。 与 Execute() 不同：Stage 1 处理 item1 时 Stage 0 已在处理 item2（无阶段间 barrier）。 与 Flow 不同：goroutine 是一次性的，调用结束即销毁。

使用示例：

	results, err := async.ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item Data) (Data, error) {
	    switch stage {
	    case "parse":
	        return parse(ctx, item)
	    case "enrich":
	        return enrich(ctx, item)
	    }
	    return item, nil
	})


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `stages` (`[]Stage[T]`)：按执行顺序排列的阶段定义，每项含 Name、Concurrency、SkipOnError；Flow 要求至少一个阶段。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。

**返回值签名**：`[]core.Result[T], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
async.ExecuteStream[T](ctx, stages, items, fn)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：各阶段并行流转，返回切片长度等于输入长度并按原索引收集；空阶段返回输入值且 Occupied=true，空阶段或空输入直接返回 nil error 而不检查取消。其他路径 ctx 取消可提前停止发送，最终 error 为 ctx.Err()，未完成槽位须核对 Occupied；逐项业务错误不自动变成顶层 error。SkipOnError 决定透传前错，业务 panic 转为 PanicError，等待仍依赖回调及时返回。

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
async.FillNoResultSkipped(nr, total)
```

**生产建议**：传入已知输入任务总数，最终 Wait 后补齐；先验证总数与内存预算。需增长时会扫描现有槽位，最坏时间 O(结果长度+新增数)；若追加触发扩容，新数组空间也与结果长度加新增数成正比。随后重新聚合失败，不依赖结果流或回调接收合成占位；外部业务使用 async.FillNoResultSkipped。

**注意事项**：仅在非 nil NoResult 的最终 Wait 完成且无活跃任务后使用。total 大于 TotalCount 时先按索引升序填空槽、再追加 ErrSkipped 占位，并增加总数和失败数；非正目标（包括 MinInt）及不大于现有总数的目标均无操作，不缩短或校正已有结果。不会执行业务、补发结果回调或向已关闭的流发送，填补之后通过聚合或错误查询读取。正数目标不设上限，须控制分配预算。

## `ForEachPool`

```go
func ForEachPool[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (*NoResultPool, error)
```

[实现](../../async.go#L926) · [行为专题](../pool.md)：ForEachPool 为切片每个元素创建协程池任务，只关心错误。

**源码说明与使用片段**（原文）：

ForEachPool 为切片每个元素创建协程池任务，只关心错误。 等价于 Pool 版本的 ForEach。

示例：

	files := []string{"f1.txt", "f2.txt", "f3.txt"}
	_, err := async.ForEachPool(ctx, files, func(ctx context.Context, path string) error {
	    return os.Remove(path)
	}, async.IO())
	if err != nil {
	    log.Printf("删除失败: %v", err)
	}


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) error`)：逐项执行的业务函数；返回 error 或 panic 均计入错误汇总，受 concurrency 限制并发执行，必须响应 ctx 才能及时取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`*NoResultPool, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
async.ForEachPool[T](ctx, items, fn, concurrency)
```

**生产建议**：检查第二返回值；它按输入顺序汇总全部提交和执行的首个错误，不受 MaxResults 限制。回调中使用有截止时间的下游调用。

**注意事项**：业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。同步处理并等待关闭池，返回的池已关闭；按最小输入索引汇总提交/执行业务错误，独立于主结果保留上限，空输入返回 nil 错误。非正并发按 IO()，panic 转为 PanicError；不会 FailFast 或自动回滚，调用方仍须控制 context 和业务幂等。

## `GetDefaultTimeout`

```go
func GetDefaultTimeout() time.Duration
```

[实现](../../internal/core/core.go#L133) · [行为专题](../config.md)：GetDefaultTimeout 返回当前全局默认超时时间，0 表示已关闭默认超时。

**源码说明与使用片段**（原文）：

GetDefaultTimeout 返回当前全局默认超时时间，0 表示已关闭默认超时。


**参数说明**：无显式参数。

**返回值签名**：`time.Duration`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GetDefaultTimeout()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetLogger`

```go
func GetLogger() Logger
```

[实现](../../internal/core/log.go#L178) · [行为专题](../config.md)：GetLogger 返回当前日志器。

**源码说明与使用片段**（原文）：

GetLogger 返回当前日志器。


**参数说明**：无显式参数。

**返回值签名**：`Logger`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GetLogger()
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `GetMaxCleanupDuration`

```go
func GetMaxCleanupDuration() time.Duration
```

[实现](../../internal/core/core.go#L110) · [行为专题](../config.md)：GetMaxCleanupDuration 返回设置的原始值；非正值由辅助函数按 30 分钟处理。

**源码说明与使用片段**（原文）：

GetMaxCleanupDuration 返回设置的原始值；非正值由辅助函数按 30 分钟处理。


**参数说明**：无显式参数。

**返回值签名**：`time.Duration`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GetMaxCleanupDuration()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetSubmitTimeout`

```go
func GetSubmitTimeout() time.Duration
```

[实现](../../internal/core/core.go#L159) · [行为专题](../config.md)：GetSubmitTimeout 返回当前保存的全局提交超时配置值。

**源码说明与使用片段**（原文）：

GetSubmitTimeout 返回当前保存的全局提交超时配置值。


**参数说明**：无显式参数。

**返回值签名**：`time.Duration`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GetSubmitTimeout()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetTaskFailLogLevel`

```go
func GetTaskFailLogLevel() TaskLogLevel
```

[实现](../../internal/core/core.go#L297) · [行为专题](../config.md)：GetTaskFailLogLevel 返回当前任务失败日志级别。

**源码说明与使用片段**（原文）：

GetTaskFailLogLevel 返回当前任务失败日志级别。


**参数说明**：无显式参数。

**返回值签名**：`TaskLogLevel`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GetTaskFailLogLevel()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：读取当前全局配置，不修改实例；并发读取同步，但先读再设置或连续读取多个字段不是原子事务。trace key 若运行中更换，已有 context 的旧值不会迁移；日志设置影响后续全库日志调用。

## `GetTraceID`

```go
func GetTraceID(ctx context.Context) string
```

[实现](../../internal/core/core.go#L809) · [行为专题](../config.md)：GetTraceID 从 ctx 中提取 trace_id，不存在则返回空字符串。

**源码说明与使用片段**（原文）：

GetTraceID 从 ctx 中提取 trace\_id，不存在则返回空字符串。

参数：

  - ctx：上下文


**参数说明**：

- `ctx` (`context.Context`)：上下文

**返回值签名**：`string`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GetTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：传非 nil context。EnsureTraceID 仅检查当前 key 的值是否为 nil，非 nil 时原样返回，包括空字符串和非 string；它不修复类型。WithTraceID 的空 id 委托 EnsureTraceID，保留已有值；非空 id 才覆盖。启用 TraceLog 时，仅生成/显式覆盖路径调用 logger.With/WithContext，后端必须返回有效 context。GetTraceID 对非 string 值会 panic。自定义 key 在初始化时固定，已有 context 的旧值不会自动迁移；继续传递返回 context。

## `GetTraceIDKey`

```go
func GetTraceIDKey() interface{}
```

[实现](../../internal/core/core.go#L777) · [行为专题](../config.md)：GetTraceIDKey 返回当前生效的 trace_id context key。

**源码说明与使用片段**（原文）：

GetTraceIDKey 返回当前生效的 trace\_id context key。


**参数说明**：无显式参数。

**返回值签名**：`interface{}`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GetTraceIDKey()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：读取当前全局配置，不修改实例；并发读取同步，但先读再设置或连续读取多个字段不是原子事务。trace key 若运行中更换，已有 context 的旧值不会迁移；日志设置影响后续全库日志调用。

## `GetTraceLogEnabled`

```go
func GetTraceLogEnabled() bool
```

[实现](../../internal/core/core.go#L322) · [行为专题](../config.md)：GetTraceLogEnabled 返回 Trace 日志是否启用。

**源码说明与使用片段**（原文）：

GetTraceLogEnabled 返回 Trace 日志是否启用。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
async.GetTraceLogEnabled()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：读取当前全局配置，不修改实例；并发读取同步，但先读再设置或连续读取多个字段不是原子事务。trace key 若运行中更换，已有 context 的旧值不会迁移；日志设置影响后续全库日志调用。

## `Group`

```go
func Group[T any]() *GroupBuilder[T]
```

[实现](../../async_group.go#L32) · [行为专题](../group.md)：Group 创建带返回值的任务组构造器。

**源码说明与使用片段**（原文）：

Group 创建带返回值的任务组构造器。 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*GroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.Group[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建独立可变构建器，父 context 初始 Background，并发初始 IO()，此时不启动 worker。配置供后续 Build/Run 使用，不能与终端执行并发修改。Run 管理关闭，Build 返回实例由调用方 Wait/Close。

## `GroupMulti`

```go
func GroupMulti[T any]() *MultiGroupBuilder[T]
```

[实现](../../async_group.go#L50) · [行为专题](../group.md)：GroupMulti 创建分片任务组构造器（MultiPool 风格）。

**源码说明与使用片段**（原文）：

GroupMulti 创建分片任务组构造器（MultiPool 风格）。 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*MultiGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GroupMulti[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建独立可变构建器，父 context 初始 Background，并发和片数初始 0，Run 创建时每片并发按 1 且共一片；此时不启动 worker。仅有 Run 终端并管理关闭，仍须在回调中等待和检查业务结果；配置不能与 Run 并发。

## `GroupSharded`

```go
func GroupSharded[T any]() *ShardedGroupBuilder[T]
```

[实现](../../async_group.go#L44) · [行为专题](../shard.md)：GroupSharded 创建分片任务组构造器。

**源码说明与使用片段**（原文）：

GroupSharded 创建分片任务组构造器。 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*ShardedGroupBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GroupSharded[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建可变构建器，context 初始 Background，配置为对应默认分片配置；此时不创建 worker。后续 Run 新建资源并管理关闭，须在终端前完成配置，不能并发修改；总并发及容量按每片累加，作用域中仍需检查提交错误并等待业务结果。

## `GroupVoid`

```go
func GroupVoid() *GroupNoResultBuilder
```

[实现](../../async_group.go#L38) · [行为专题](../group.md)：GroupVoid 创建无返回值任务组构造器。

**源码说明与使用片段**（原文）：

GroupVoid 创建无返回值任务组构造器。 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*GroupNoResultBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
async.GroupVoid()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建独立可变构建器，父 context 初始 Background，并发初始 IO()，此时不启动 worker。配置供后续 Build/Run 使用，不能与终端执行并发修改。Run 管理关闭，Build 返回实例由调用方 Wait/Close。

## `IO`

```go
func IO() int
```

[实现](../../internal/core/core.go#L685) · [行为专题](../config.md)：IO 返回 IO 密集型并发度，等于 runtime.NumCPU() * 2。

**源码说明与使用片段**（原文）：

IO 返回 IO 密集型并发度，等于 runtime.NumCPU() \* 2。 适用于网络请求、文件读写等 IO 密集型任务，默认倍数为 2。

使用示例：

	// 8 核机器返回 16
	err := async.Pool[string]().Worker(async.IO()).Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
	    for _, url := range urls {
	        if err := p.Submit(ctx, func(ctx context.Context) (string, error) {
	            return httpGet(ctx, url)
	        }); err != nil { return err }
	    }
	    p.Wait()
	    return p.FirstError()
	})


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
async.IO()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。

## `IOMulti`

```go
func IOMulti(multiplier int) int
```

[实现](../../internal/core/core.go#L698) · [行为专题](../config.md)：IOMulti 返回自定义倍数的 IO 密集型并发度，等于 runtime.NumCPU() * multiplier。

**源码说明与使用片段**（原文）：

IOMulti 返回自定义倍数的 IO 密集型并发度，等于 runtime.NumCPU() \* multiplier。

参数：

  - multiplier：CPU 核数的倍数，\<= 0 时默认使用 2

使用示例：

	// 8 核机器，倍数 4，返回 32（高并发 RPC 场景）
	async.Pool[string]().Worker(async.IOMulti(4)).Run(fn)


**参数说明**：

- `multiplier` (`int`)：逻辑 CPU 数的乘数；非正值使用 2，返回 NumCPU×有效倍数，与 GOMAXPROCS 无关。避免超大倍数造成整数溢出或不可承受的 worker 数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
async.IOMulti(multiplier)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。

## `MapFrom`

```go
func MapFrom[K comparable, V any](m map[K]V) *Map[K, V]
```

[实现](../../async.go#L972) · [行为专题](../mapreduce.md)：MapFrom 从原生 map 创建 Map 容器，浅拷贝键值对；引用类型的值仍共享其底层数据。

**源码说明与使用片段**（原文）：

MapFrom 从原生 map 创建 Map 容器，浅拷贝键值对；引用类型的值仍共享其底层数据。


**参数说明**：

- `m` (`map[K]V`)：待复制的原生 map；复制键值形成新容器，引用类型值内部仍共享。nil 输入创建可写的空 map。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.MapFrom[K, V](m)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：复制键值对形成独立 map，nil 输入也创建可写空 map；引用类型值内部仍共享。复制期间不能与输入写入并发；修改新容器的键不会修改原 map。

## `MapFromRef`

```go
func MapFromRef[K comparable, V any](m map[K]V) *Map[K, V]
```

[实现](../../async.go#L977) · [行为专题](../mapreduce.md)：MapFromRef 从原生 map 创建 Map 容器（直接引用，不拷贝）。

**源码说明与使用片段**（原文）：

MapFromRef 从原生 map 创建 Map 容器（直接引用，不拷贝）。


**参数说明**：

- `m` (`map[K]V`)：要直接持有的原生 map；非 nil 时双方修改互相可见，nil 时创建可写的空 map。共享访问中有写入时须同步。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.MapFromRef[K, V](m)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：非 nil 输入直接共享底层 map，双方写入互相可见；nil 输入会创建新的可写空 map。无内部锁，有写入时同步所有读写；需要键值独立时使用 From，引用类型值仍须按业务复制。

## `MergeCancel`

```go
func MergeCancel(oldCancel, newCancel context.CancelFunc) context.CancelFunc
```

[实现](../../internal/core/core.go#L448) · [行为专题](../config.md)：MergeCancel 合并两个 CancelFunc，调用时依次执行新旧 cancel。

**源码说明与使用片段**（原文）：

MergeCancel 合并两个 CancelFunc，调用时依次执行新旧 cancel。

参数：

  - oldCancel：旧的 CancelFunc（可能为 nil）
  - newCancel：新的 CancelFunc


**参数说明**：

- `oldCancel` (`context.CancelFunc`)：原有取消函数，可为 nil；非 nil 时返回的闭包先调用 newCancel 再调用它。
- `newCancel` (`context.CancelFunc`)：新增取消函数；oldCancel 非 nil 时它也必须非 nil，否则调用组合函数时 panic。oldCancel 为 nil 时直接返回此值（包括 nil）。

**返回值签名**：`context.CancelFunc`。

**使用示例**（已初始化参数的调用片段）：

```go
async.MergeCancel(oldCancel, newCancel)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：oldCancel 非 nil 时返回闭包，先执行 newCancel 再执行 oldCancel；此时 newCancel 必须非 nil。oldCancel 为 nil 则直接返回 newCancel（可为 nil）。同步执行，不添加 once、不恢复 panic；newCancel panic/Goexit 时 oldCancel 不执行，oldCancel 异常时 newCancel 已执行。重复/并发调用是否安全取决于注入函数；context 原生取消函数支持重复调用。

## `Must`

```go
func Must[T any](val T, err error) T
```

[实现](../../async.go#L1027) · [行为专题](../config.md)：Must 提取值，如果 err != nil 则 panic。

**源码说明与使用片段**（原文）：

Must 提取值，如果 err != nil 则 panic。 适用于初始化阶段或测试代码中确保操作必然成功。

示例：

	// 初始化时确保配置加载成功
	cfg := async.Must(loadConfig("config.yaml"))

	// 测试代码
	val := async.Must(someFn(ctx, input))


**参数说明**：

- `val` (`T`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。
- `err` (`error`)：要检查、封装或记录的错误；nil 表示无错误，Must 遇到非 nil 错误会 panic。

**返回值签名**：`T`。

**使用示例**（已初始化参数的调用片段）：

```go
async.Must[T](val, err)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：err 非 nil 时在调用方直接 panic(err)，否则返回原 val；不恢复 panic、不记录日志、不复制引用内容。仅适用于初始化等可接受 panic 的路径，生产请求路径应显式处理错误。

## `NewBoundedRunnerBuilder`

```go
func NewBoundedRunnerBuilder() *BoundedRunnerBuilder
```

[实现](../../async.go#L908) · [行为专题](../task.md)：NewBoundedRunnerBuilder 创建 BoundedRunner 链式构建器。

**源码说明与使用片段**（原文）：

NewBoundedRunnerBuilder 创建 BoundedRunner 链式构建器。


**参数说明**：无显式参数。

**返回值签名**：`*BoundedRunnerBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
async.NewBoundedRunnerBuilder()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立构建器，context 初始为 Background，尚不启动业务。设置只供后续终端操作使用，不能与启动并发修改；TaskBuilder.Bounded 每次创建新的限额，BoundedRunnerBuilder 的 context 当前不传入 Build 返回的执行器。

## `NewFlow`

```go
func NewFlow[T any](
	ctx context.Context,
	cfg FlowConfig[T],
	fn func(ctx context.Context, stage string, item T) (T, error),
) *Flow[T]
```

[实现](../../async.go#L851) · [行为专题](../pipeline.md)：NewFlow 创建 Flow。

**源码说明与使用片段**（原文）：

NewFlow 创建 Flow。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `cfg` (`FlowConfig[T]`)：配置或执行策略；各字段默认值与组合优先级见专题。
- `fn` (`func(ctx context.Context, stage string, item T) (T, error)`)：各阶段 worker 调用的处理函数：接收 Flow 的 context、阶段名和当前元素，返回下一阶段的值及错误，可并发调用。panic 转为 PanicError 并保留该阶段输入值；显式 Stage.SkipOnError=true 时跳过前错，否则后续成功可覆盖它。链式 Stage 未提供该字段设置入口；取消须回调协作。

**返回值签名**：`*Flow[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.NewFlow[T](ctx, cfg, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：立即启动各阶段常驻 worker，阶段为空或 ctx 为 nil 会 panic；非正阶段并发按 IO()，非正缓冲按 256。阶段字段构造时复制，fn 可并发调用且业务 panic 转为错误。持续消费 Results，最终合并 Close 返回的残留；取消可能丢弃转发结果，回调忽略取消时关闭仍可阻塞。

## `NewMap`

```go
func NewMap[K comparable, V any]() *Map[K, V]
```

[实现](../../async.go#L962) · [行为专题](../mapreduce.md)：NewMap 创建空的泛型 key-value 容器。

**源码说明与使用片段**（原文）：

NewMap 创建空的泛型 key-value 容器。


**参数说明**：无显式参数。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.NewMap[K, V]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建可写空容器；New 的 capacity 仅为预分配提示，不是容量上限，负数按 Go make(map) 规则处理。容器不带锁，使用后共享给多个 goroutine 时有写入则须同步全部访问。

## `NewMapCap`

```go
func NewMapCap[K comparable, V any](capacity int) *Map[K, V]
```

[实现](../../async.go#L967) · [行为专题](../mapreduce.md)：NewMapCap 创建空的泛型 key-value 容器，预分配 capacity 大小的空间。

**源码说明与使用片段**（原文）：

NewMapCap 创建空的泛型 key-value 容器，预分配 capacity 大小的空间。


**参数说明**：

- `capacity` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.NewMapCap[K, V](capacity)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建可写空容器；New 的 capacity 仅为预分配提示，不是容量上限，负数按 Go make(map) 规则处理。容器不带锁，使用后共享给多个 goroutine 时有写入则须同步全部访问。

## `NewMapChain`

```go
func NewMapChain[K comparable, V any](ctx context.Context, data map[K]V) *MapChain[K, V, V]
```

[实现](../../async.go#L1006) · [行为专题](../mapreduce.md)：NewMapChain 从原生 map 创建 MapChain，输出类型与输入 V 一致。

**源码说明与使用片段**（原文）：

NewMapChain 从原生 map 创建 MapChain，输出类型与输入 V 一致。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `data` (`map[K]V`)：直接持有的输入 map 引用，不复制。nil 可读取和遍历，但写入条目的链式操作会 panic；需写入时传入已 make 的 map。

**返回值签名**：`*MapChain[K, V, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.NewMapChain[K, V](ctx, data)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：构造时保留输入 map 的引用且确保非 nil context 的 trace_id，不复制输入；构造后的顺序修改会被后续终端读取。部分终端在执行时复制条目以便调度，但不提供与并发写入同步的快照或写入锁。nil 输入支持读取/空终端，链式写入须先初始化 map；启动 Stream 到消费结束期间也不能并发修改输入。

## `NewMapChainWith`

```go
func NewMapChainWith[K comparable, V any, R any](ctx context.Context, data map[K]V) *MapChain[K, V, R]
```

[实现](../../async.go#L1011) · [行为专题](../mapreduce.md)：NewMapChainWith 从原生 map 创建 MapChain，可指定输出类型 R。

**源码说明与使用片段**（原文）：

NewMapChainWith 从原生 map 创建 MapChain，可指定输出类型 R。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `data` (`map[K]V`)：直接持有的输入 map 引用，不复制。nil 可读取和遍历，但写入条目的链式操作会 panic；需写入时传入已 make 的 map。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.NewMapChainWith[K, V, R](ctx, data)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：构造时保留输入 map 的引用且确保非 nil context 的 trace_id，不复制输入；构造后的顺序修改会被后续终端读取。部分终端在执行时复制条目以便调度，但不提供与并发写入同步的快照或写入锁。nil 输入支持读取/空终端，链式写入须先初始化 map；启动 Stream 到消费结束期间也不能并发修改输入。

## `NewPanicError`

```go
func NewPanicError(r any) *PanicError
```

[实现](../../internal/core/core.go#L434) · [行为专题](../config.md)：NewPanicError 创建 PanicError，捕获 panic 值和当前调用栈。

**源码说明与使用片段**（原文）：

NewPanicError 创建 PanicError，捕获 panic 值和当前调用栈。

参数：

  - r：recover() 捕获的 panic 原始值


**参数说明**：

- `r` (`any`)：recover() 捕获的 panic 原始值

**返回值签名**：`*PanicError`。

**使用示例**（已初始化参数的调用片段）：

```go
async.NewPanicError(r)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：NewPanicError 捕获创建处当前 goroutine 的栈，保留原 Value 引用，不深拷贝；Value/Stack 是可变公开字段，后续格式化读取当前值，共享修改须同步。Unwrap 始终返回 nil，即使 Value 本身为 error 也不加入错误链。Format 对各动词均写出 panic: 加原值，仅 'v' 且有 '+' 时追加换行和当前 Stack；忽略宽度/精度，不为 %q 加引号或转义。需转义先 Error() 再格式化字符串，生产中仅向受控日志输出栈；Result 的 %+v 不转发此标记。

## `NewTraceID`

```go
func NewTraceID() string
```

[实现](../../internal/core/core.go#L846) · [行为专题](../config.md)：NewTraceID 生成新的随机 trace_id（32 位十六进制字符串）。

**源码说明与使用片段**（原文）：

NewTraceID 生成新的随机 trace\_id（32 位十六进制字符串）。 使用 crypto/rand 生成，失败时回退到时间戳+计数器方案。

使用示例：

	traceID := core.NewTraceID() // 如 "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"


**参数说明**：无显式参数。

**返回值签名**：`string`。

**使用示例**（已初始化参数的调用片段）：

```go
async.NewTraceID()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：输出为 32 位十六进制，正常使用 crypto/rand；实现保留随机读取失败后使用时间戳和进程内计数器的回退分支。用于日志关联，不承诺绝对唯一，也不作为认证令牌。

## `Pipeline`

```go
func Pipeline[T any](items []T) *PipelineBuilder[T]
```

[实现](../../async.go#L770) · [行为专题](../pipeline.md)：Pipeline 创建管道链式构建器，统一入口。

**源码说明与使用片段**（原文）：

Pipeline 创建管道链式构建器，统一入口。 通过 .Context(ctx) 设置上下文，.Run(fn) / .Execute(fn) 执行管道。 通过 .Serial() / .Parallel() 切换串行/并行模式。


**参数说明**：

- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*PipelineBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.Pipeline[T](items)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存输入切片引用并创建可变构建器，context 初始 Background，此时不启动 worker；元素内部引用也共享。终端执行期间不能修改共享输入或构建器，阶段列表在后续 Stage 调用中追加，空阶段规则由终端入口决定。

## `Pool`

```go
func Pool[T any]() *PoolBuilder[T]
```

[实现](../../async_pool.go#L22) · [行为专题](../pool.md)：Pool 创建协程池构建器。

**源码说明与使用片段**（原文）：

Pool 创建协程池构建器。 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*PoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.Pool[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建可变构建器，context 为 Background，cfg 为 DefaultConfig（Size=IO()、MaxResults=-1），此时不启动 worker。MultiBuilder 的片数初始 0，Run 实际一片；通常 Run 新建资源并关闭，MultiBuilder 注入 Pool 后则使用并关闭外部池及其分片。配置不能与 Run 并发，已关闭外部池不可直接重复运行。

## `PoolMulti`

```go
func PoolMulti[T any]() *MultiPoolBuilder[T]
```

[实现](../../async_pool.go#L28) · [行为专题](../pool.md)：PoolMulti 创建分片协程池构造器。

**源码说明与使用片段**（原文）：

PoolMulti 创建分片协程池构造器。 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*MultiPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.PoolMulti[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只创建可变构建器，context 为 Background，cfg 为 DefaultConfig（Size=IO()、MaxResults=-1），此时不启动 worker。MultiBuilder 的片数初始 0，Run 实际一片；通常 Run 新建资源并关闭，MultiBuilder 注入 Pool 后则使用并关闭外部池及其分片。配置不能与 Run 并发，已关闭外部池不可直接重复运行。

## `PoolSharded`

```go
func PoolSharded[T any]() *ShardPoolBuilder[T]
```

[实现](../../async_pool.go#L34) · [行为专题](../shard.md)：PoolSharded 创建分片池构造器。

**源码说明与使用片段**（原文）：

PoolSharded 创建分片池构造器。 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。


**参数说明**：无显式参数。

**返回值签名**：`*ShardPoolBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.PoolSharded[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建可变构建器，context 初始 Background，配置为对应默认分片配置；此时不创建 worker。后续 Run 新建资源并管理关闭，须在终端前完成配置，不能并发修改；总并发及容量按每片累加，作用域中仍需检查提交错误并等待业务结果。

## `Ratelimit`

```go
func Ratelimit(ctx context.Context) *RatelimitBuilder
```

[实现](../../async.go#L656) · [行为专题](../ratelimit.md)：Ratelimit 创建限流器链式构建器，统一入口。

**源码说明与使用片段**（原文）：

Ratelimit 创建限流器链式构建器，统一入口。 每个模式有独立的子构建器，只暴露对应模式的参数方法，Go 类型系统天然隔离不通用参数。

示例：

	// RateLimiter 模式：Rate / Per / Burst + Default*()
	rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Burst(200).Build()
	rl := async.Ratelimit(ctx).RateLimiter().DefaultRate().DefaultPer().Build()

	// TokenBucket 模式：Rate / Capacity + Default*()
	tb := async.Ratelimit(ctx).TokenBucket().Rate(10).Capacity(20).Build()
	tb := async.Ratelimit(ctx).TokenBucket().DefaultRate().DefaultCapacity().Build()

	// SlidingWindow 模式：Limit / Window + Default*()
	sw := async.Ratelimit(ctx).SlidingWindow().Limit(100).Window(10*time.Second).Build()
	sw := async.Ratelimit(ctx).SlidingWindow().DefaultLimit().DefaultWindow().Build()

	// Adaptive 模式：MinWorker / MaxWorker + Default*()
	al := async.Ratelimit(ctx).Adaptive().MinWorker(5).MaxWorker(100).Build()
	al := async.Ratelimit(ctx).Adaptive().DefaultMinWorker().DefaultMaxWorker().Build()

	// Sharded 模式：Shards + 模式参数 + Default*()
	srl := async.Ratelimit(ctx).Sharded().RateLimiter().Shards(16).Rate(10000).Per(time.Second).Build()
	srl := async.Ratelimit(ctx).Sharded().RateLimiter().DefaultShards().DefaultRate().DefaultPer().Build()


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*RatelimitBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
async.Ratelimit(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：只保存 context，不立即创建限流器；后续模式入口创建独立子构建器，Build 时才创建资源。保存的 context 当前不传给限流实例，Acquire/Wait 须单独传 context；需要后台补充的实例最终 Close。

## `Retry`

```go
func Retry[T any](ctx context.Context) *RetryChain[T]
```

[实现](../../async.go#L699) · [行为专题](../retry.md)：Retry 创建重试链式构建器，统一入口。

**源码说明与使用片段**（原文）：

Retry 创建重试链式构建器，统一入口。 默认配置：指数退避，最多 3 次重试，退避 100ms~30s。

带返回值：

	val, err := async.Retry[string](ctx).
	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
	    Execute(fn)

无返回值（用 struct{} 或 RetryVoid）：

	err := async.RetryVoid(ctx).
	    Linear().MaxRetries(5).Backoff(1*time.Second, 0).
	    Run(func() error { return doSomething() })


**参数说明**：

- `ctx` (`context.Context`)：原样保存的父 context，终端执行时须非 nil；构造和执行不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用构建器 Context(ctx)；正数 PerCallTimeout 仅为每次业务派生期限。

**返回值签名**：`*RetryChain[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.Retry[T](ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立可变构建器，不立即执行或分配限流器。默认指数退避、3 次重试、100ms 起始/30s 上限、无单次超时和限流；原样保存父 context，执行时须非 nil，构造及 Execute 不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用 Context(ctx)。反复 Execute 每次新建独立限流器；不能与配置修改并发，也不以重新调用构造器维持跨调用额度。

## `RetryVoid`

```go
func RetryVoid(ctx context.Context) *RetryChain[struct{}]
```

[实现](../../async.go#L709) · [行为专题](../retry.md)：RetryVoid 创建无返回值重试构造器。

**源码说明与使用片段**（原文）：

RetryVoid 创建无返回值重试构造器。 等价于 Retry\[struct{}](ctx)，提供更简洁的无返回值重试语义。

	err := async.RetryVoid(ctx).
	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
	    Run(func() error { return doSomething() })


**参数说明**：

- `ctx` (`context.Context`)：原样保存的父 context，终端执行时须非 nil；构造和执行不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用构建器 Context(ctx)；正数 PerCallTimeout 仅为每次业务派生期限。

**返回值签名**：`*RetryChain[struct{}]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.RetryVoid(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立可变构建器，不立即执行或分配限流器。默认指数退避、3 次重试、100ms 起始/30s 上限、无单次超时和限流；原样保存父 context，执行时须非 nil，构造及 Execute 不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用 Context(ctx)。反复 Execute 每次新建独立限流器；不能与配置修改并发，也不以重新调用构造器维持跨调用额度。

## `SafeCall`

```go
func SafeCall[T any, R any](ctx context.Context, item T, fn func(ctx context.Context, item T) (R, error)) (R, error)
```

[实现](../../async.go#L264) · [行为专题](../config.md)：SafeCall 安全调用 fn，自动捕获 panic 并包装为 PanicError。

**源码说明与使用片段**（原文）：

SafeCall 安全调用 fn，自动捕获 panic 并包装为 PanicError。

适用场景：在不希望 panic 导致整个程序崩溃的边界处调用。

示例：

	result, err := async.SafeCall(ctx, input, func(ctx context.Context, item MyType) (string, error) {
	    return item.Process(ctx)
	})


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `item` (`T`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：同步接收 context 和输入项的业务函数；返回签名中的值和错误。panic 被恢复并包装为 PanicError；不设置超时、不另启 goroutine，回调须自行响应 context。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
async.SafeCall[T, R](ctx, item, fn)
```

**生产建议**：在调用方管理回调耗时和下游 context，检查返回 error 并用 errors.As 区分 PanicError；需要异步执行时选用 Task 等入口。日志后端须支持并发调用且不 panic，业务自建 goroutine 由业务自己等待和恢复。

**注意事项**：在调用方同步执行一次回调，不启动 goroutine、不检查 context 取消，也不设置期限；nil 或已取消 context 均原样传入。普通返回的值和错误原样保留，非 nil 错误可以伴随非零值；业务或 nil 函数调用的 panic 包装为 PanicError 并记录日志，有值版本 panic 时返回零值。只捕获同一 goroutine 的 panic，业务自建 goroutine 的 panic 不在范围内；runtime.Goexit 仍执行 defer 并结束当前 goroutine，不转换成错误返回。日志后端若 panic 仍会向外传播。

## `SafeCallVoid`

```go
func SafeCallVoid[T any](ctx context.Context, item T, fn func(ctx context.Context, item T) error) error
```

[实现](../../async.go#L275) · [行为专题](../config.md)：SafeCallVoid 安全调用 fn（无返回值），自动捕获 panic。

**源码说明与使用片段**（原文）：

SafeCallVoid 安全调用 fn（无返回值），自动捕获 panic。

示例：

	err := async.SafeCallVoid(ctx, input, func(ctx context.Context, item MyType) error {
	    return item.DoSomething(ctx)
	})


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `item` (`T`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。
- `fn` (`func(ctx context.Context, item T) error`)：同步接收 context 和输入项的业务函数；返回签名中的值和错误。panic 被恢复并包装为 PanicError；不设置超时、不另启 goroutine，回调须自行响应 context。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
async.SafeCallVoid[T](ctx, item, fn)
```

**生产建议**：在调用方管理回调耗时和下游 context，检查返回 error 并用 errors.As 区分 PanicError；需要异步执行时选用 Task 等入口。日志后端须支持并发调用且不 panic，业务自建 goroutine 由业务自己等待和恢复。

**注意事项**：在调用方同步执行一次回调，不启动 goroutine、不检查 context 取消，也不设置期限；nil 或已取消 context 均原样传入。普通返回的值和错误原样保留，非 nil 错误可以伴随非零值；业务或 nil 函数调用的 panic 包装为 PanicError 并记录日志，有值版本 panic 时返回零值。只捕获同一 goroutine 的 panic，业务自建 goroutine 的 panic 不在范围内；runtime.Goexit 仍执行 defer 并结束当前 goroutine，不转换成错误返回。日志后端若 panic 仍会向外传播。

## `SetDefaultTimeout`

```go
func SetDefaultTimeout(d time.Duration)
```

[实现](../../internal/core/core.go#L128) · [行为专题](../config.md)：SetDefaultTimeout 设置全局默认超时时间，影响后续创建的 Group 和 Pool。

**源码说明与使用片段**（原文）：

SetDefaultTimeout 设置全局默认超时时间，影响后续创建的 Group 和 Pool。 当 Group/Pool 未通过 WithTimeout 单独设置时，使用此全局默认值。 设为 0 可关闭默认超时。

参数：

  - d：默认超时时间，0 表示无超时限制

使用示例：

	// 设置全局默认超时为 10 秒
	core.SetDefaultTimeout(10 * time.Second)

	// 关闭默认超时（任务无超时限制）
	core.SetDefaultTimeout(0)


**参数说明**：

- `d` (`time.Duration`)：后续新建 Pool/Group 保存的默认任务超时；正数设置回调 context 期限，非正值不设置期限。不会更新已有实例，TaskBuilder 不读取此全局值。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
async.SetDefaultTimeout(d)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetLogger`

```go
func SetLogger(l Logger)
```

[实现](../../internal/core/log.go#L169) · [行为专题](../config.md)：SetLogger 注入自定义日志实现。传入 nil 则恢复为默认空日志实现。

**源码说明与使用片段**（原文）：

SetLogger 注入自定义日志实现。传入 nil 则恢复为默认空日志实现。

使用示例：

	// 注入自定义日志
	core.SetLogger(myZerologAdapter)

	// 恢复静默模式
	core.SetLogger(nil)


**参数说明**：

- `l` (`Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
async.SetLogger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `SetMaxCleanupDuration`

```go
func SetMaxCleanupDuration(d time.Duration)
```

[实现](../../internal/core/core.go#L105) · [行为专题](../config.md)：SetMaxCleanupDuration 设置内部等待辅助函数的清理监视时长。

**源码说明与使用片段**（原文）：

SetMaxCleanupDuration 设置内部等待辅助函数的清理监视时长。 非正值在这些辅助函数中按 WaitContextCleanupError 处理；该设置不终止任务。 当前 Pool 和 Group 的等待方法不使用这些辅助函数。

参数：

  - d：监视时长，非正值使用 30 分钟

使用示例：

	// 设置内部等待辅助函数的监视时长为 5 分钟，不终止任务
	core.SetMaxCleanupDuration(5 * time.Minute)


**参数说明**：

- `d` (`time.Duration`)：内部 Wait 辅助函数在提前返回后监视清理的时长，原样保存；非正值在辅助函数中使用 30 分钟。只停止监视，不会终止任务。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
async.SetMaxCleanupDuration(d)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetSubmitTimeout`

```go
func SetSubmitTimeout(d time.Duration)
```

[实现](../../internal/core/core.go#L147) · [行为专题](../config.md)：SetSubmitTimeout 设置并保存全局提交超时配置值。当前 Pool/Group 创建路径不自动读取此值；

**源码说明与使用片段**（原文）：

SetSubmitTimeout 设置并保存全局提交超时配置值。当前 Pool/Group 创建路径不自动读取此值； d 必须大于 0，否则不更新。具体实例的提交等待需通过实例或 Builder 显式设置。

参数：

  - d：提交超时时间，必须 > 0

使用示例：

	// 设置提交超时为 3 秒
	core.SetSubmitTimeout(3 * time.Second)


**参数说明**：

- `d` (`time.Duration`)：保存供 GetSubmitTimeout 读取的全局时长，仅正数更新。Pool/Group 不自动读取，限制入队等待需在具体实例或构建器另行配置。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
async.SetSubmitTimeout(d)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetTaskFailLogLevel`

```go
func SetTaskFailLogLevel(level TaskLogLevel)
```

[实现](../../internal/core/core.go#L292) · [行为专题](../config.md)：SetTaskFailLogLevel 设置任务失败时的日志级别。

**源码说明与使用片段**（原文）：

SetTaskFailLogLevel 设置任务失败时的日志级别。 默认为 LogLevelError，可设置为 LogLevelSilent 关闭失败日志。

参数：

  - level：日志级别（LogLevelError/Warn/Info/Debug/Silent）

使用示例：

	// 任务失败只打印 Warn 级别日志
	core.SetTaskFailLogLevel(core.LogLevelWarn)

	// 关闭任务失败日志
	core.SetTaskFailLogLevel(core.LogLevelSilent)


**参数说明**：

- `level` (`TaskLogLevel`)：任务失败日志的全局级别；Error/Warn/Info/Debug 分别输出相应级别，Silent 与范围外值均不输出。原值按 int32 保存，不校验枚举范围；普通 Log/LogCtx 不受其过滤。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
async.SetTaskFailLogLevel(level)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetTraceIDKey`

```go
func SetTraceIDKey(key interface{})
```

[实现](../../internal/core/core.go#L769) · [行为专题](../config.md)：SetTraceIDKey 自定义 trace_id 的 context key。

**源码说明与使用片段**（原文）：

SetTraceIDKey 自定义 trace\_id 的 context key。 设置后将替换默认的 TraceIDKey，EnsureTraceID/GetTraceID/WithTraceID 都会使用新 key。

使用场景：对接已有链路追踪系统（如 trpc、go-zero、自定义中间件）， 这些系统通常使用自己的 key 类型存储 trace\_id。

参数：

  - key：自定义 context key，通常为框架中存储 trace\_id 的 key

使用示例：

	// trpc 场景：对接 trpc 的 trace_id key
	core.SetTraceIDKey(trpc.TraceIDKey)

	// go-zero 场景
	core.SetTraceIDKey(trace.TraceKey)

	// 自定义字符串 key
	core.SetTraceIDKey("X-Trace-Id")

注意：请在最早期（init 或 main 开头）调用，避免并发读写导致的瞬时不一致。 key 必须非 nil 且其值可安全比较；结构体或数组中接口字段的动态值也须可比较。 无效 key 会在更新全局配置前 panic。


**参数说明**：

- `key` (`interface{}`)：自定义 context key，通常为框架中存储 trace_id 的 key

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
async.SetTraceIDKey(key)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：key 必须非 nil 且其值可安全比较；结构体或数组中接口字段的动态值也须可比较，否则在更新全局配置前 panic。设置影响全库后续 context 读写；在启动时统一配置，已有 context 不会把旧 key 的值迁移到新 key。

## `SetTraceLogEnabled`

```go
func SetTraceLogEnabled(enabled bool)
```

[实现](../../internal/core/core.go#L313) · [行为专题](../config.md)：SetTraceLogEnabled 启用或禁用 Trace 日志。

**源码说明与使用片段**（原文）：

SetTraceLogEnabled 启用或禁用 Trace 日志。 启用时，EnsureTraceID 生成新值或 WithTraceID 显式覆盖时调用日志上下文注入。 不影响 TraceID 的存储，也不屏蔽 LogCtx 系列自动附加 trace\_id 字段。 默认为启用状态。

参数：

  - enabled：true=启用，false=禁用

使用示例：

	// 禁用 Trace 日志
	core.SetTraceLogEnabled(false)


**参数说明**：

- `enabled` (`bool`)：是否在生成或显式覆盖 TraceID 时调用 logger.With/WithContext；不关闭 TraceID 生成存储，也不过滤 LogCtx 自动附加的 trace_id。已有非 nil 值的 EnsureTraceID 路径不补做注入。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
async.SetTraceLogEnabled(enabled)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：只控制新 TraceID 或显式非空覆盖时的日志 context 注入；已有值不补做注入。false 不关闭日志，也不屏蔽 LogCtx 从 context 自动追加 trace_id；原子设置不迁移已有 context 或后端状态。

## `Slice`

```go
func Slice[T any](ctx context.Context, items []T) *SliceBuilder[T, T]
```

[实现](../../async.go#L386) · [行为专题](../mapreduce.md)：Slice 创建同类型切片构建器（R=T），默认为并行模式。

**源码说明与使用片段**（原文）：

Slice 创建同类型切片构建器（R=T），默认为并行模式。

	ctx:   上下文（自动注入 TraceID）。
	items: 原始切片数据。

	results := async.Slice(ctx, []int{1, 2, 3}).Sort(cmp).Map(fn)


**参数说明**：

- `ctx` (`context.Context`)：上下文（自动注入 TraceID）。
- `items` (`[]T`)：原始切片数据。

**返回值签名**：`*SliceBuilder[T, T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.Slice[T](ctx, items)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存原输入切片引用并确保非 nil context 的 trace_id，不复制输入或立即启动业务；纯数据方法可能修改底层数组，终端读取期间不得并发修改。Builder 默认 DefPar，Runner 保存显式 Policy；需要独立元素时先复制，元素内部引用还须按业务隔离。

## `SliceWith`

```go
func SliceWith[R any, T any](ctx context.Context, items []T) *SliceBuilder[T, R]
```

[实现](../../async.go#L396) · [行为专题](../mapreduce.md)：SliceWith 创建跨类型切片构建器（T→R），显式指定输出类型 R，输入类型 T 由 items 自动推断。

**源码说明与使用片段**（原文）：

SliceWith 创建跨类型切片构建器（T→R），显式指定输出类型 R，输入类型 T 由 items 自动推断。 默认为并行模式。

	results := async.SliceWith[string](ctx, []int{1, 2, 3}).Map(func(ctx context.Context, n int) (string, error) {
	    return strconv.Itoa(n), nil
	})


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.SliceWith[R, T](ctx, items)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：保存原输入切片引用并确保非 nil context 的 trace_id，不复制输入或立即启动业务；纯数据方法可能修改底层数组，终端读取期间不得并发修改。Builder 默认 DefPar，Runner 保存显式 Policy；需要独立元素时先复制，元素内部引用还须按业务隔离。

## `Task`

```go
func Task[T any]() *TaskBuilder[T]
```

[实现](../../async.go#L551) · [行为专题](../task.md)：Task 创建任务链式构建器，统一入口。

**源码说明与使用片段**（原文）：

Task 创建任务链式构建器，统一入口。 通过 .Context(ctx) 设置上下文，使用 .Go(fn) 或 .GoResult(fn) 启动任务。


**参数说明**：无显式参数。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.Task[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立构建器，context 初始为 Background，尚不启动业务。设置只供后续终端操作使用，不能与启动并发修改；TaskBuilder.Bounded 每次创建新的限额，BoundedRunnerBuilder 的 context 当前不传入 Build 返回的执行器。

## `TaskVoid`

```go
func TaskVoid() *TaskBuilder[struct{}]
```

[实现](../../async.go#L557) · [行为专题](../task.md)：TaskVoid 创建无返回值异步任务构造器，语义等价于 Task[struct{}]()。

**源码说明与使用片段**（原文）：

TaskVoid 创建无返回值异步任务构造器，语义等价于 Task\[struct{}]()。 通过 TaskVoid().Context(ctx).GoAct(fn) 启动无需返回值的异步任务。


**参数说明**：无显式参数。

**返回值签名**：`*TaskBuilder[struct{}]`。

**使用示例**（已初始化参数的调用片段）：

```go
async.TaskVoid()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立构建器，context 初始为 Background，尚不启动业务。设置只供后续终端操作使用，不能与启动并发修改；TaskBuilder.Bounded 每次创建新的限额，BoundedRunnerBuilder 的 context 当前不传入 Build 返回的执行器。

## `WithConfig`

```go
func WithConfig(configured int) int
```

[实现](../../internal/core/core.go#L714) · [行为专题](../config.md)：WithConfig 如果 configured > 0 则返回 configured，否则返回 IO()。

**源码说明与使用片段**（原文）：

WithConfig 如果 configured > 0 则返回 configured，否则返回 IO()。 用于统一处理用户指定的并发度：传 0 或不传时自动使用合理默认值。

参数：

  - configured：用户指定的并发度，0 表示使用默认 IO()

使用示例：

	concurrency := core.WithConfig(userConfig) // userConfig=0 时自动使用 IO()


**参数说明**：

- `configured` (`int`)：用户指定的并发度；正数原样返回，0 或负数返回 IO()，不检查上界或分配资源。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
async.WithConfig(configured)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。

## `WithTraceID`

```go
func WithTraceID(ctx context.Context, id string) context.Context
```

[实现](../../internal/core/core.go#L828) · [行为专题](../config.md)：WithTraceID 设置指定的 trace_id 到 ctx 中。

**源码说明与使用片段**（原文）：

WithTraceID 设置指定的 trace\_id 到 ctx 中。 如果 id 为空字符串，则等同于 EnsureTraceID：保留已有非 nil 值，缺失时才生成。 非空 id 覆盖或空 id 缺失生成时，启用 TraceLog 才注入 logger 上下文。

参数：

  - ctx：原始上下文
  - id：要设置的 trace\_id；空字符串保留当前非 nil 值，缺失时才生成

使用示例：

	// 从上游请求中透传 trace_id
	ctx := core.WithTraceID(ctx, upstreamTraceID)


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `id` (`string`)：关联标识字符串，非空时覆盖当前 key 下的值；空字符串调用 EnsureTraceID，保留已有的非 nil 值，缺失时生成新标识。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
async.WithTraceID(ctx, id)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：传非 nil context。EnsureTraceID 仅检查当前 key 的值是否为 nil，非 nil 时原样返回，包括空字符串和非 string；它不修复类型。WithTraceID 的空 id 委托 EnsureTraceID，保留已有值；非空 id 才覆盖。启用 TraceLog 时，仅生成/显式覆盖路径调用 logger.With/WithContext，后端必须返回有效 context。GetTraceID 对非 string 值会 panic。自定义 key 在初始化时固定，已有 context 的旧值不会自动迁移；继续传递返回 context。

