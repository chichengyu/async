# sliceops 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [mapreduce](../mapreduce.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*ForEachResult.Err`

```go
func (r *ForEachResult) Err() bool
```

[实现](../../internal/sliceops/slice_result.go#L129) · [行为专题](../mapreduce.md)：Err 表示已保存错误，需要错误值时调用 Error。

**源码说明与使用片段**（原文）：

Err 表示已保存错误，需要错误值时调用 Error。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示观察到了错误，使用 Error/Errors 获取错误值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Err()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取终端保存的统计和错误，不重新执行任务；Err 返回 bool，Error 才返回错误对象，Ok/Err 以是否保存首错为准。Total/FailCount 的单位和未执行项规则由具体入口决定，批量模式可能按批次，不能仅凭 Ok 推断全部输入都已执行。

## `*ForEachResult.Error`

```go
func (r *ForEachResult) Error() error
```

[实现](../../internal/sliceops/slice_result.go#L123) · [行为专题](../mapreduce.md)：Error 返回执行器保存的首个错误，成功时为 nil。

**源码说明与使用片段**（原文）：

Error 返回执行器保存的首个错误，成功时为 nil。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Error()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取终端保存的统计和错误，不重新执行任务；Err 返回 bool，Error 才返回错误对象，Ok/Err 以是否保存首错为准。Total/FailCount 的单位和未执行项规则由具体入口决定，批量模式可能按批次，不能仅凭 Ok 推断全部输入都已执行。

## `*ForEachResult.FailCount`

```go
func (r *ForEachResult) FailCount() int64
```

[实现](../../internal/sliceops/slice_result.go#L135) · [行为专题](../mapreduce.md)：FailCount 返回执行器统计的失败项数，包含执行错误及取消结果。

**源码说明与使用片段**（原文）：

FailCount 返回执行器统计的失败项数，包含执行错误及取消结果。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailCount()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取终端保存的统计和错误，不重新执行任务；Err 返回 bool，Error 才返回错误对象，Ok/Err 以是否保存首错为准。Total/FailCount 的单位和未执行项规则由具体入口决定，批量模式可能按批次，不能仅凭 Ok 推断全部输入都已执行。

## `*ForEachResult.Ok`

```go
func (r *ForEachResult) Ok() bool
```

[实现](../../internal/sliceops/slice_result.go#L126) · [行为专题](../mapreduce.md)：Ok 表示未保存错误，空输入也为 true。

**源码说明与使用片段**（原文）：

Ok 表示未保存错误，空输入也为 true。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示对应结果成功；空集合规则见方法说明。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Ok()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取终端保存的统计和错误，不重新执行任务；Err 返回 bool，Error 才返回错误对象，Ok/Err 以是否保存首错为准。Total/FailCount 的单位和未执行项规则由具体入口决定，批量模式可能按批次，不能仅凭 Ok 推断全部输入都已执行。

## `*ForEachResult.OkCount`

```go
func (r *ForEachResult) OkCount() int64
```

[实现](../../internal/sliceops/slice_result.go#L138) · [行为专题](../mapreduce.md)：OkCount 返回 Total - FailCount。

**源码说明与使用片段**（原文）：

OkCount 返回 Total - FailCount。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OkCount()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取终端保存的统计和错误，不重新执行任务；Err 返回 bool，Error 才返回错误对象，Ok/Err 以是否保存首错为准。Total/FailCount 的单位和未执行项规则由具体入口决定，批量模式可能按批次，不能仅凭 Ok 推断全部输入都已执行。

## `*ForEachResult.Total`

```go
func (r *ForEachResult) Total() int64
```

[实现](../../internal/sliceops/slice_result.go#L132) · [行为专题](../mapreduce.md)：Total 返回执行器统计的总项数；批处理模式的统计单位见对应终端方法。

**源码说明与使用片段**（原文）：

Total 返回执行器统计的总项数；批处理模式的统计单位见对应终端方法。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Total()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取终端保存的统计和错误，不重新执行任务；Err 返回 bool，Error 才返回错误对象，Ok/Err 以是否保存首错为准。Total/FailCount 的单位和未执行项规则由具体入口决定，批量模式可能按批次，不能仅凭 Ok 推断全部输入都已执行。

## `*ParallelSlice[T, R].Chunk`

```go
func (b *ParallelSlice[T, R]) Chunk(size int) *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L81) · [行为专题](../mapreduce.md)：设置批处理或分块调度的元素数；每批回调与逐元素回调的区别见终端方法。

**参数说明**：

- `size` (`int`)：设置批处理或分块调度的元素数；每批回调与逐元素回调的区别见终端方法。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Chunk(size)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].DefaultChunk`

```go
func (b *ParallelSlice[T, R]) DefaultChunk() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L86) · [行为专题](../mapreduce.md)：将分块大小显式设置为模块默认批量大小。

**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultChunk()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].DefaultPool`

```go
func (b *ParallelSlice[T, R]) DefaultPool() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L42) · [行为专题](../mapreduce.md)：标记后续终端操作使用内部创建的默认池，完成后关闭。

**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPool()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：设置池引用或创建标记，此时不分配池；Pool 借用，PoolAuto 接管关闭，DefaultPool 标记终端内部创建。借用池非空 Map/MapBatch 及空/非空 Each/EachBatch/Stream 等待后 ResetWait，空 Map/MapBatch 直接返回；接管池空输入也关闭。Fold 同步聚合不复位借用池，但关闭已接管池。不能与外部批次混用，Worker 不 Resize 外部池；DefaultPool 在终端按 IO() 创建，也不采用策略 Worker，Worker(1).DefaultPool() 仍可并发启动多个回调。固定并发数须注入对应容量的池。

## `*ParallelSlice[T, R].DefaultShard`

```go
func (b *ParallelSlice[T, R]) DefaultShard() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L72) · [行为专题](../mapreduce.md)：设置执行策略分片数量为 GOMAXPROCS，至少为 2。

**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShard()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].DefaultTimeout`

```go
func (b *ParallelSlice[T, R]) DefaultTimeout() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L62) · [行为专题](../mapreduce.md)：将策略超时显式设置为当前模块的默认常量（见赋值及专题）。

**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].DefaultWorker`

```go
func (b *ParallelSlice[T, R]) DefaultWorker() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L27) · [行为专题](../mapreduce.md)：显式将 worker 配置恢复为 core.IO()。

**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].FailFast`

```go
func (b *ParallelSlice[T, R]) FailFast() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L47) · [行为专题](../mapreduce.md)：启用失败取消策略；回调忽略 context 时不会被强制停止。

**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].ForEach`

```go
func (b *ParallelSlice[T, R]) ForEach(fn func(context.Context, T) error) *ForEachResult
```

[实现](../../internal/sliceops/slice_parallel.go#L106) · [行为专题](../mapreduce.md)：按配置策略逐项执行，返回总数、失败数和首个错误的汇总。

**参数说明**：

- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`*ForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。统计单位为元素，等待后返回汇总；首错选择依执行路径，不保证最先提交或发生。无池超时及 FailFast 路径经无序 NoResult 汇总；启用 FailFast 时，取消拒绝可先于触发错误记录，firstErr 因而可能为 Canceled；完整原因须另存业务错误或核对逐项结果。Pool 优先，其他路径先 Shards、Timeout/FailFast 再 Chunk/普通执行；预取消时 Group/池路径可在回调前拒绝，但并行度降为 1 会同步调用且不预检查 context，Chunked Each 的单元素块也会如此。业务 panic 转为 PanicError，回调须检查并响应 context；池化 Runner.Each 的最后 results 返回值为 nil，计数/错误仍有效。

## `*ParallelSlice[T, R].ForEachBatch`

```go
func (b *ParallelSlice[T, R]) ForEachBatch(fn func(context.Context, []T) error) *ForEachResult
```

[实现](../../internal/sliceops/slice_parallel.go#L111) · [行为专题](../mapreduce.md)：按 Chunk 分组，每批执行一次回调，统计单位为批次。

**参数说明**：

- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`*ForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先按 Chunk 分组，非正值使用 1，大于输入长度的正数仅生成一个非空批次，支持 MaxInt；每批调用一次接收 []T 的回调。子切片共享输入，不追加至批次或修改其他批次可见数据。携带同一策略调度批次，MapBatch 每批一个 Result，错误项仍保留返回值，panic 转为 PanicError 且值为零；FailValues 展开失败批次全部原始元素，不证明逐元素执行或回滚。无值统计及有效并行度裁剪按批次数；EachBatch 单批可串行回退且不预检查 context。池结果上限和环形缓冲不截断本终端结果与统计。正数超时按整批计时；重试须考虑部分副作用和幂等。空 MapBatch 不等待或复位借用池，空 EachBatch 仍等待复位，接管池均关闭；核对汇总及逐批错误。

## `*ParallelSlice[T, R].Logger`

```go
func (b *ParallelSlice[T, R]) Logger(l core.Logger) *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L91) · [行为专题](../mapreduce.md)：设置全局日志实现，并返回当前构建器；不局限于本实例。

**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*ParallelSlice[T, R].Map`

```go
func (b *ParallelSlice[T, R]) Map(fn func(context.Context, T) (R, error)) *SliceResult[R]
```

[实现](../../internal/sliceops/slice_parallel.go#L96) · [行为专题](../mapreduce.md)：按配置的执行策略逐项映射，返回包含成功值与错误的结果容器。

**参数说明**：

- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`*SliceResult[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待映射完成，结果按输入/批次索引保存，检查汇总及逐项 Err；取消/FailFast 的空槽和未执行规则依路径。优先 Pool、Shards、Chunk、Timeout/FailFast、普通并行/串行，早期路径可能忽略串行标志。预取消时普通并行 Map（未启用 FailFast）与串行 Map 仍调用回调，业务须检查 context；其他调度路径可在回调前拒绝。空 Map 不等待或复位借用池，但关闭类型匹配的接管池；非空借用池等待后复位。业务 panic 转错误，超时依赖回调配合。

## `*ParallelSlice[T, R].MapBatch`

```go
func (b *ParallelSlice[T, R]) MapBatch(fn func(context.Context, []T) (R, error)) *SliceResult[R]
```

[实现](../../internal/sliceops/slice_parallel.go#L101) · [行为专题](../mapreduce.md)：按 Chunk 分组，每批调用一次映射回调，返回结果容器。

**参数说明**：

- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`*SliceResult[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先按 Chunk 分组，非正值使用 1，大于输入长度的正数仅生成一个非空批次，支持 MaxInt；每批调用一次接收 []T 的回调。子切片共享输入，不追加至批次或修改其他批次可见数据。携带同一策略调度批次，MapBatch 每批一个 Result，错误项仍保留返回值，panic 转为 PanicError 且值为零；FailValues 展开失败批次全部原始元素，不证明逐元素执行或回滚。无值统计及有效并行度裁剪按批次数；EachBatch 单批可串行回退且不预检查 context。池结果上限和环形缓冲不截断本终端结果与统计。正数超时按整批计时；重试须考虑部分副作用和幂等。空 MapBatch 不等待或复位借用池，空 EachBatch 仍等待复位，接管池均关闭；核对汇总及逐批错误。

## `*ParallelSlice[T, R].NoFailFast`

```go
func (b *ParallelSlice[T, R]) NoFailFast() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L52) · [行为专题](../mapreduce.md)：关闭失败取消策略，返回自身。

**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.NoFailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].Pool`

```go
func (b *ParallelSlice[T, R]) Pool(p *pool.Pool[R]) *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L32) · [行为专题](../mapreduce.md)：注入外部池，调用方保留生命周期责任；结果类型必须匹配。

**参数说明**：

- `p` (`*pool.Pool[R]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool(p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：设置池引用或创建标记，此时不分配池；Pool 借用，PoolAuto 接管关闭，DefaultPool 标记终端内部创建。借用池非空 Map/MapBatch 及空/非空 Each/EachBatch/Stream 等待后 ResetWait，空 Map/MapBatch 直接返回；接管池空输入也关闭。Fold 同步聚合不复位借用池，但关闭已接管池。不能与外部批次混用，Worker 不 Resize 外部池；DefaultPool 在终端按 IO() 创建，也不采用策略 Worker，Worker(1).DefaultPool() 仍可并发启动多个回调。固定并发数须注入对应容量的池。

## `*ParallelSlice[T, R].PoolAuto`

```go
func (b *ParallelSlice[T, R]) PoolAuto(p *pool.Pool[R]) *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L37) · [行为专题](../mapreduce.md)：注入外部池并转移生命周期责任，终端操作完成后关闭池。

**参数说明**：

- `p` (`*pool.Pool[R]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.PoolAuto(p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：设置池引用或创建标记，此时不分配池；Pool 借用，PoolAuto 接管关闭，DefaultPool 标记终端内部创建。借用池非空 Map/MapBatch 及空/非空 Each/EachBatch/Stream 等待后 ResetWait，空 Map/MapBatch 直接返回；接管池空输入也关闭。Fold 同步聚合不复位借用池，但关闭已接管池。不能与外部批次混用，Worker 不 Resize 外部池；DefaultPool 在终端按 IO() 创建，也不采用策略 Worker，Worker(1).DefaultPool() 仍可并发启动多个回调。固定并发数须注入对应容量的池。

## `*ParallelSlice[T, R].Shards`

```go
func (b *ParallelSlice[T, R]) Shards(n int) *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L67) · [行为专题](../mapreduce.md)：设置执行策略分片数量，非正值采用 GOMAXPROCS 且至少为 2。

**参数说明**：

- `n` (`int`)：设置执行策略分片数量，非正值采用 GOMAXPROCS 且至少为 2。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].Stream`

```go
func (b *ParallelSlice[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R]
```

[实现](../../internal/sliceops/slice_parallel.go#L116) · [行为专题](../mapreduce.md)：按执行策略映射并通过只读 channel 输出；提前停止消费时取消执行上下文。

**参数说明**：

- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(fn, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出 Result，不携带原索引；有关联要求在返回值内携带标识。正数 bufSize 优先，其次 Buf，仍非正按 min(输入长度,16384)；持续消费或取消父 context，停止消费可能阻塞回调及清理。Pool 优先，无池用 Group worker，不采用 serial/Shards/Chunk；正数 Timeout/FailFast 生效，panic 转错误。池化 Timeout 只约束回调 context，不解除结果投递背压或重写已返回的成功；FailFast 派生取消也不替代父取消，提交拒绝结果可先于触发错误到达。父取消可丢弃待投递结果，清理仍等待所有已开始回调返回。输出独立于池自身 StreamResults 的丢弃；借用池复位会替换其原生流通道并清零 StreamDropped，不能用复位后的计数判断旧批次。空输入仍等待并复位借用池，接管池在输出关闭前关闭。

## `*ParallelSlice[T, R].Timeout`

```go
func (b *ParallelSlice[T, R]) Timeout(d time.Duration) *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L57) · [行为专题](../mapreduce.md)：设置执行策略中的单任务超时，回调须响应 context。

**参数说明**：

- `d` (`time.Duration`)：设置执行策略中的单任务超时，回调须响应 context。 使用 time.Millisecond / time.Second 等时长单位；零值与负值规则见本方法说明。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*ParallelSlice[T, R].ToSerial`

```go
func (b *ParallelSlice[T, R]) ToSerial() *SerialSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L16) · [行为专题](../mapreduce.md)：保留输入引用和 context，创建串行构建器并重置执行策略；需重新应用原策略设置。

**参数说明**：无显式参数。

**返回值签名**：`*SerialSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ToSerial()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无参数并返回新构建器，复制当前切片头与context引用，源配置保持；目标策略整体重置为Seq/DefPar，此前Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。当前长度/容量/数组保留，后续Take/Drop/Append/替换数组只更新各自切片头，共享槽位写入互相可见。context引用保留仍受父取消，但不保留任务超时策略；后续字段赋值不联动。旧池不自动关闭且目标执行不替源收尾，切换后重新配置并安排旧池生命周期；隔离输入须先复制。

## `*ParallelSlice[T, R].Worker`

```go
func (b *ParallelSlice[T, R]) Worker(n int) *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_parallel.go#L22) · [行为专题](../mapreduce.md)：设置后续终端操作的并发度，非正值使用 core.IO()。

**参数说明**：

- `n` (`int`)：设置后续终端操作的并发度，非正值使用 core.IO()。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*Runner[T, R].Each`

```go
func (r *Runner[T, R]) Each(fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/runner.go#L45) · [行为专题](../mapreduce.md)：Each 根据 Policy 策略执行 ForEach 操作。若池已注入则使用池化执行。

**源码说明与使用片段**（原文）：

Each 根据 Policy 策略执行 ForEach 操作。若池已注入则使用池化执行。


**参数说明**：

- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Each(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。统计单位为元素，等待后返回汇总；首错选择依执行路径，不保证最先提交或发生。无池超时及 FailFast 路径经无序 NoResult 汇总；启用 FailFast 时，取消拒绝可先于触发错误记录，firstErr 因而可能为 Canceled；完整原因须另存业务错误或核对逐项结果。Pool 优先，其他路径先 Shards、Timeout/FailFast 再 Chunk/普通执行；预取消时 Group/池路径可在回调前拒绝，但并行度降为 1 会同步调用且不预检查 context，Chunked Each 的单元素块也会如此。业务 panic 转为 PanicError，回调须检查并响应 context；池化 Runner.Each 的最后 results 返回值为 nil，计数/错误仍有效。

## `*Runner[T, R].EachBatch`

```go
func (r *Runner[T, R]) EachBatch(fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/runner.go#L81) · [行为专题](../mapreduce.md)：EachBatch 分块遍历，fn 接收整个 chunk []T。需配合 Policy.Chunk() 使用。

**源码说明与使用片段**（原文）：

EachBatch 分块遍历，fn 接收整个 chunk \[]T。需配合 Policy.Chunk() 使用。


**参数说明**：

- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.EachBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：先按 Chunk 分组，非正值使用 1，大于输入长度的正数仅生成一个非空批次，支持 MaxInt；每批调用一次接收 []T 的回调。子切片共享输入，不追加至批次或修改其他批次可见数据。携带同一策略调度批次，MapBatch 每批一个 Result，错误项仍保留返回值，panic 转为 PanicError 且值为零；FailValues 展开失败批次全部原始元素，不证明逐元素执行或回滚。无值统计及有效并行度裁剪按批次数；EachBatch 单批可串行回退且不预检查 context。池结果上限和环形缓冲不截断本终端结果与统计。正数超时按整批计时；重试须考虑部分副作用和幂等。空 MapBatch 不等待或复位借用池，空 EachBatch 仍等待复位，接管池均关闭；核对汇总及逐批错误。

## `*Runner[T, R].Fold`

```go
func (r *Runner[T, R]) Fold(initial R, fn func(context.Context, R, T) (R, error)) (R, error)
```

[实现](../../internal/sliceops/runner.go#L63) · [行为专题](../mapreduce.md)：Fold 串行聚合，对每个元素调用 fn(acc, item) 累积结果。本质串行，不受并行配置影响。

**源码说明与使用片段**（原文）：

Fold 串行聚合，对每个元素调用 fn(acc, item) 累积结果。本质串行，不受并行配置影响。


**参数说明**：

- `initial` (`R`)：聚合的初始累积值。
- `fn` (`func(context.Context, R, T) (R, error)`)：按输入顺序同步接收 context、当前累积值和元素，返回新累积值与错误；非 nil 错误立即结束，panic 直接传播。不使用并行度或策略超时，回调须自行检查 context。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Fold(initial, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：始终按输入顺序同步聚合，空输入返回 initial；遇错返回该次回调给出的累积值和错误，不恢复 panic。忽略策略 Worker/Timeout/FailFast/Shards/Chunk，不自动检查父 context；回调须自行检查。Runner.Fold/Builder.Reduce 仍关闭被接管的外部池，纯串行 SerialSlice.Reduce 直接调用独立 Reduce。

## `*Runner[T, R].Map`

```go
func (r *Runner[T, R]) Map(fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/runner.go#L40) · [行为专题](../mapreduce.md)：Map 根据 Policy 策略执行 Map 操作。策略优先级由 execMap 内部决定。

**源码说明与使用片段**（原文）：

Map 根据 Policy 策略执行 Map 操作。策略优先级由 execMap 内部决定。


**参数说明**：

- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待映射完成，结果按输入/批次索引保存，检查汇总及逐项 Err；取消/FailFast 的空槽和未执行规则依路径。优先 Pool、Shards、Chunk、Timeout/FailFast、普通并行/串行，早期路径可能忽略串行标志。预取消时普通并行 Map（未启用 FailFast）与串行 Map 仍调用回调，业务须检查 context；其他调度路径可在回调前拒绝。空 Map 不等待或复位借用池，但关闭类型匹配的接管池；非空借用池等待后复位。业务 panic 转错误，超时依赖回调配合。

## `*Runner[T, R].MapBatch`

```go
func (r *Runner[T, R]) MapBatch(fn func(context.Context, []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/runner.go#L71) · [行为专题](../mapreduce.md)：MapBatch 分块映射，fn 接收整个 chunk []T。需配合 Policy.Chunk() 使用。

**源码说明与使用片段**（原文）：

MapBatch 分块映射，fn 接收整个 chunk \[]T。需配合 Policy.Chunk() 使用。


**参数说明**：

- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先按 Chunk 分组，非正值使用 1，大于输入长度的正数仅生成一个非空批次，支持 MaxInt；每批调用一次接收 []T 的回调。子切片共享输入，不追加至批次或修改其他批次可见数据。携带同一策略调度批次，MapBatch 每批一个 Result，错误项仍保留返回值，panic 转为 PanicError 且值为零；FailValues 展开失败批次全部原始元素，不证明逐元素执行或回滚。无值统计及有效并行度裁剪按批次数；EachBatch 单批可串行回退且不预检查 context。池结果上限和环形缓冲不截断本终端结果与统计。正数超时按整批计时；重试须考虑部分副作用和幂等。空 MapBatch 不等待或复位借用池，空 EachBatch 仍等待复位，接管池均关闭；核对汇总及逐批错误。

## `*Runner[T, R].Stream`

```go
func (r *Runner[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R]
```

[实现](../../internal/sliceops/runner.go#L54) · [行为专题](../mapreduce.md)：Stream 流式映射，通过 channel 边执行边返回。

**源码说明与使用片段**（原文）：

Stream 流式映射，通过 channel 边执行边返回。


**参数说明**：

- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(fn, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出 Result，不携带原索引；有关联要求在返回值内携带标识。正数 bufSize 优先，其次 Buf，仍非正按 min(输入长度,16384)；持续消费或取消父 context，停止消费可能阻塞回调及清理。Pool 优先，无池用 Group worker，不采用 serial/Shards/Chunk；正数 Timeout/FailFast 生效，panic 转错误。池化 Timeout 只约束回调 context，不解除结果投递背压或重写已返回的成功；FailFast 派生取消也不替代父取消，提交拒绝结果可先于触发错误到达。父取消可丢弃待投递结果，清理仍等待所有已开始回调返回。输出独立于池自身 StreamResults 的丢弃；借用池复位会替换其原生流通道并清零 StreamDropped，不能用复位后的计数判断旧批次。空输入仍等待并复位借用池，接管池在输出关闭前关闭。

## `*SerialSlice[T, R].DefaultTimeout`

```go
func (b *SerialSlice[T, R]) DefaultTimeout() *SerialSlice[T, R]
```

[实现](../../internal/sliceops/slice_serial.go#L35) · [行为专题](../mapreduce.md)：将策略超时显式设置为当前模块的默认常量（见赋值及专题）。

**参数说明**：无显式参数。

**返回值签名**：`*SerialSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SerialSlice[T, R].FailFast`

```go
func (b *SerialSlice[T, R]) FailFast() *SerialSlice[T, R]
```

[实现](../../internal/sliceops/slice_serial.go#L20) · [行为专题](../mapreduce.md)：启用失败取消策略；回调忽略 context 时不会被强制停止。

**参数说明**：无显式参数。

**返回值签名**：`*SerialSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SerialSlice[T, R].ForEach`

```go
func (b *SerialSlice[T, R]) ForEach(fn func(context.Context, T) error) *ForEachResult
```

[实现](../../internal/sliceops/slice_serial.go#L50) · [行为专题](../mapreduce.md)：按配置策略逐项执行，返回总数、失败数和首个错误的汇总。

**参数说明**：

- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`*ForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。统计单位为元素，等待后返回汇总；首错选择依执行路径，不保证最先提交或发生。无池超时及 FailFast 路径经无序 NoResult 汇总；启用 FailFast 时，取消拒绝可先于触发错误记录，firstErr 因而可能为 Canceled；完整原因须另存业务错误或核对逐项结果。Pool 优先，其他路径先 Shards、Timeout/FailFast 再 Chunk/普通执行；预取消时 Group/池路径可在回调前拒绝，但并行度降为 1 会同步调用且不预检查 context，Chunked Each 的单元素块也会如此。业务 panic 转为 PanicError，回调须检查并响应 context；池化 Runner.Each 的最后 results 返回值为 nil，计数/错误仍有效。

## `*SerialSlice[T, R].Logger`

```go
func (b *SerialSlice[T, R]) Logger(l core.Logger) *SerialSlice[T, R]
```

[实现](../../internal/sliceops/slice_serial.go#L40) · [行为专题](../mapreduce.md)：设置全局日志实现，并返回当前构建器；不局限于本实例。

**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*SerialSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*SerialSlice[T, R].Map`

```go
func (b *SerialSlice[T, R]) Map(fn func(context.Context, T) (R, error)) *SliceResult[R]
```

[实现](../../internal/sliceops/slice_serial.go#L45) · [行为专题](../mapreduce.md)：按配置的执行策略逐项映射，返回包含成功值与错误的结果容器。

**参数说明**：

- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`*SliceResult[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待映射完成，结果按输入/批次索引保存，检查汇总及逐项 Err；取消/FailFast 的空槽和未执行规则依路径。优先 Pool、Shards、Chunk、Timeout/FailFast、普通并行/串行，早期路径可能忽略串行标志。预取消时普通并行 Map（未启用 FailFast）与串行 Map 仍调用回调，业务须检查 context；其他调度路径可在回调前拒绝。空 Map 不等待或复位借用池，但关闭类型匹配的接管池；非空借用池等待后复位。业务 panic 转错误，超时依赖回调配合。

## `*SerialSlice[T, R].NoFailFast`

```go
func (b *SerialSlice[T, R]) NoFailFast() *SerialSlice[T, R]
```

[实现](../../internal/sliceops/slice_serial.go#L25) · [行为专题](../mapreduce.md)：关闭失败取消策略，返回自身。

**参数说明**：无显式参数。

**返回值签名**：`*SerialSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.NoFailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SerialSlice[T, R].Reduce`

```go
func (b *SerialSlice[T, R]) Reduce(initial R, fn func(context.Context, R, T) (R, error)) (R, error)
```

[实现](../../internal/sliceops/slice_serial.go#L55) · [行为专题](../mapreduce.md)：按输入顺序同步累积，空输入返回 initial；遇错误返回该次回调给出的累积值及错误。不恢复 panic，不强制检查或终止未响应取消的回调。

**参数说明**：

- `initial` (`R`)：聚合的初始累积值。
- `fn` (`func(context.Context, R, T) (R, error)`)：同步接收 context、当前累积值及输入元素；返回下一累积值和错误，非 nil 错误终止遍历并返回该次累积值，panic 向调用方传播。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reduce(initial, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：始终按输入顺序同步聚合，空输入返回 initial；遇错返回该次回调给出的累积值和错误，不恢复 panic。忽略策略 Worker/Timeout/FailFast/Shards/Chunk，不自动检查父 context；回调须自行检查。Runner.Fold/Builder.Reduce 仍关闭被接管的外部池，纯串行 SerialSlice.Reduce 直接调用独立 Reduce。

## `*SerialSlice[T, R].Stream`

```go
func (b *SerialSlice[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R]
```

[实现](../../internal/sliceops/slice_serial.go#L59) · [行为专题](../mapreduce.md)：按执行策略映射并通过只读 channel 输出；提前停止消费时取消执行上下文。

**参数说明**：

- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(fn, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出 Result，不携带原索引；有关联要求在返回值内携带标识。正数 bufSize 优先，其次 Buf，仍非正按 min(输入长度,16384)；持续消费或取消父 context，停止消费可能阻塞回调及清理。Pool 优先，无池用 Group worker，不采用 serial/Shards/Chunk；正数 Timeout/FailFast 生效，panic 转错误。池化 Timeout 只约束回调 context，不解除结果投递背压或重写已返回的成功；FailFast 派生取消也不替代父取消，提交拒绝结果可先于触发错误到达。父取消可丢弃待投递结果，清理仍等待所有已开始回调返回。输出独立于池自身 StreamResults 的丢弃；借用池复位会替换其原生流通道并清零 StreamDropped，不能用复位后的计数判断旧批次。空输入仍等待并复位借用池，接管池在输出关闭前关闭。

## `*SerialSlice[T, R].Timeout`

```go
func (b *SerialSlice[T, R]) Timeout(d time.Duration) *SerialSlice[T, R]
```

[实现](../../internal/sliceops/slice_serial.go#L30) · [行为专题](../mapreduce.md)：设置执行策略中的单任务超时，回调须响应 context。

**参数说明**：

- `d` (`time.Duration`)：设置执行策略中的单任务超时，回调须响应 context。 使用 time.Millisecond / time.Second 等时长单位；零值与负值规则见本方法说明。

**返回值签名**：`*SerialSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SerialSlice[T, R].ToParallel`

```go
func (b *SerialSlice[T, R]) ToParallel() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_serial.go#L14) · [行为专题](../mapreduce.md)：保留输入引用和 context，创建默认并行构建器并重置执行策略。

**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ToParallel()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无参数并返回新构建器，复制当前切片头与context引用，源配置保持；目标策略整体重置为Seq/DefPar，此前Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。当前长度/容量/数组保留，后续Take/Drop/Append/替换数组只更新各自切片头，共享槽位写入互相可见。context引用保留仍受父取消，但不保留任务超时策略；后续字段赋值不联动。旧池不自动关闭且目标执行不替源收尾，切换后重新配置并安排旧池生命周期；隔离输入须先复制。

## `*SliceBuilder[T, R].Buf`

```go
func (b *SliceBuilder[T, R]) Buf(size int) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L275) · [行为专题](../mapreduce.md)：Buf 设置 Stream 缓冲区大小。

**源码说明与使用片段**（原文）：

Buf 设置 Stream 缓冲区大小。

	size: 通道缓冲区大小。

	ch := sb.Buf(2048).Stream(fn, 0)


**参数说明**：

- `size` (`int`)：通道缓冲区大小。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Buf(size)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].Chunk`

```go
func (b *SliceBuilder[T, R]) Chunk(size int) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L257) · [行为专题](../mapreduce.md)：Chunk 启用分块模式，size 为每块大小。

**源码说明与使用片段**（原文）：

Chunk 启用分块模式，size 为每块大小。 分块模式逐步提交大小为 size 的批次进行并行处理。

	size: 每块元素数量，如 100 表示每次处理 100 个元素。

	sb.Chunk(100).Map(fn)


**参数说明**：

- `size` (`int`)：每块元素数量，如 100 表示每次处理 100 个元素。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Chunk(size)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].Context`

```go
func (b *SliceBuilder[T, R]) Context(ctx context.Context) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L121) · [行为专题](../mapreduce.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存经 trace_id 处理的非 nil context，只影响后续终端，输入引用和策略保持。不会替换已启动 Stream 的 context；构建器及共享切片访问须由调用方同步，不与配置或终端执行竞争。

## `*SliceBuilder[T, R].DefaultBuf`

```go
func (b *SliceBuilder[T, R]) DefaultBuf() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L283) · [行为专题](../mapreduce.md)：DefaultBuf 使用默认流式缓冲（自适应）。

**源码说明与使用片段**（原文）：

DefaultBuf 使用默认流式缓冲（自适应）。

	ch := sb.DefaultBuf().Stream(fn, 0)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBuf()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].DefaultChunk`

```go
func (b *SliceBuilder[T, R]) DefaultChunk() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L265) · [行为专题](../mapreduce.md)：DefaultChunk 使用默认分块大小（100）。

**源码说明与使用片段**（原文）：

DefaultChunk 使用默认分块大小（100）。

	sb.DefaultChunk().Map(fn)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultChunk()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].DefaultFailFast`

```go
func (b *SliceBuilder[T, R]) DefaultFailFast() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L223) · [行为专题](../mapreduce.md)：DefaultFailFast 关闭 FailFast（默认行为：失败不中断其他任务）。

**源码说明与使用片段**（原文）：

DefaultFailFast 关闭 FailFast（默认行为：失败不中断其他任务）。

	sb.DefaultFailFast().Map(fn)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultFailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].DefaultLogger`

```go
func (b *SliceBuilder[T, R]) DefaultLogger() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L139) · [行为专题](../mapreduce.md)：DefaultLogger 重置为默认日志实现（静默模式）。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现（静默模式）。

	sb.DefaultLogger().Map(fn)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*SliceBuilder[T, R].DefaultPool`

```go
func (b *SliceBuilder[T, R]) DefaultPool() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L166) · [行为专题](../mapreduce.md)：DefaultPool 标记终端方法使用内部创建的默认协程池，此时不创建池。

**源码说明与使用片段**（原文）：

DefaultPool 标记终端方法使用内部创建的默认协程池，此时不创建池。 池由内部自动管理生命周期，终端方法执行完毕后自动 Close。

	sb.DefaultPool().Map(fn)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPool()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：设置池引用或创建标记，此时不分配池；Pool 借用，PoolAuto 接管关闭，DefaultPool 标记终端内部创建。借用池非空 Map/MapBatch 及空/非空 Each/EachBatch/Stream 等待后 ResetWait，空 Map/MapBatch 直接返回；接管池空输入也关闭。Fold 同步聚合不复位借用池，但关闭已接管池。不能与外部批次混用，Worker 不 Resize 外部池；DefaultPool 在终端按 IO() 创建，也不采用策略 Worker，Worker(1).DefaultPool() 仍可并发启动多个回调。固定并发数须注入对应容量的池。

## `*SliceBuilder[T, R].DefaultShard`

```go
func (b *SliceBuilder[T, R]) DefaultShard() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L242) · [行为专题](../mapreduce.md)：DefaultShard 使用默认水平分片数（GOMAXPROCS，最少 2）。

**源码说明与使用片段**（原文）：

DefaultShard 使用默认水平分片数（GOMAXPROCS，最少 2）。

	sb.DefaultShard().Map(fn)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShard()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].DefaultTimeout`

```go
func (b *SliceBuilder[T, R]) DefaultTimeout() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L207) · [行为专题](../mapreduce.md)：DefaultTimeout 将单任务超时设置为固定的 30 秒，不受全局默认超时配置影响。

**源码说明与使用片段**（原文）：

DefaultTimeout 将单任务超时设置为固定的 30 秒，不受全局默认超时配置影响。

	sb.DefaultTimeout().Map(fn)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].DefaultWorker`

```go
func (b *SliceBuilder[T, R]) DefaultWorker() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L157) · [行为专题](../mapreduce.md)：DefaultWorker 使用默认 IO 并发度（推荐，语义更清晰）。

**源码说明与使用片段**（原文）：

DefaultWorker 使用默认 IO 并发度（推荐，语义更清晰）。

	sb.DefaultWorker().Map(fn)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].FailFast`

```go
func (b *SliceBuilder[T, R]) FailFast() *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L215) · [行为专题](../mapreduce.md)：FailFast 启用 FailFast 模式：失败触发取消，其余任务须响应 context 才能及时退出。

**源码说明与使用片段**（原文）：

FailFast 启用 FailFast 模式：失败触发取消，其余任务须响应 context 才能及时退出。

	sb.FailFast().Map(fn)


**参数说明**：无显式参数。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].ForEach`

```go
func (b *SliceBuilder[T, R]) ForEach(fn func(context.Context, T) error) *ForEachResult
```

[实现](../../internal/sliceops/slice_builder.go#L340) · [行为专题](../mapreduce.md)：ForEach 根据已配置的策略执行 ForEach 操作，返回 ForEachResult。

**源码说明与使用片段**（原文）：

ForEach 根据已配置的策略执行 ForEach 操作，返回 ForEachResult。

	fn: 回调函数 func(context.Context, T) error。

	r := sb.ForEach(func(ctx context.Context, v int) error {
	    log.Printf("processing %d", v)
	    return nil
	})
	if r.Err() { return r.Error() }


**参数说明**：

- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`*ForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。统计单位为元素，等待后返回汇总；首错选择依执行路径，不保证最先提交或发生。无池超时及 FailFast 路径经无序 NoResult 汇总；启用 FailFast 时，取消拒绝可先于触发错误记录，firstErr 因而可能为 Canceled；完整原因须另存业务错误或核对逐项结果。Pool 优先，其他路径先 Shards、Timeout/FailFast 再 Chunk/普通执行；预取消时 Group/池路径可在回调前拒绝，但并行度降为 1 会同步调用且不预检查 context，Chunked Each 的单元素块也会如此。业务 panic 转为 PanicError，回调须检查并响应 context；池化 Runner.Each 的最后 results 返回值为 nil，计数/错误仍有效。

## `*SliceBuilder[T, R].ForEachBatch`

```go
func (b *SliceBuilder[T, R]) ForEachBatch(fn func(context.Context, []T) error) *ForEachResult
```

[实现](../../internal/sliceops/slice_builder.go#L352) · [行为专题](../mapreduce.md)：ForEachBatch 分块遍历，fn 接收整个 chunk []T。需配合 Chunk(size) 使用。

**源码说明与使用片段**（原文）：

ForEachBatch 分块遍历，fn 接收整个 chunk \[]T。需配合 Chunk(size) 使用。

	fn: 回调函数 func(context.Context, []T) error，接收整个分块。

	r := sb.Chunk(100).ForEachBatch(func(ctx context.Context, chunk []int) error {
	    return batchInsert(ctx, chunk)
	})


**参数说明**：

- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`*ForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachBatch(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先按 Chunk 分组，非正值使用 1，大于输入长度的正数仅生成一个非空批次，支持 MaxInt；每批调用一次接收 []T 的回调。子切片共享输入，不追加至批次或修改其他批次可见数据。携带同一策略调度批次，MapBatch 每批一个 Result，错误项仍保留返回值，panic 转为 PanicError 且值为零；FailValues 展开失败批次全部原始元素，不证明逐元素执行或回滚。无值统计及有效并行度裁剪按批次数；EachBatch 单批可串行回退且不预检查 context。池结果上限和环形缓冲不截断本终端结果与统计。正数超时按整批计时；重试须考虑部分副作用和幂等。空 MapBatch 不等待或复位借用池，空 EachBatch 仍等待复位，接管池均关闭；核对汇总及逐批错误。

## `*SliceBuilder[T, R].Logger`

```go
func (b *SliceBuilder[T, R]) Logger(l core.Logger) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L131) · [行为专题](../mapreduce.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。

	l: core.Logger 接口实现。

	sb.Logger(&MyLogger{}).Map(fn)


**参数说明**：

- `l` (`core.Logger`)：core.Logger 接口实现。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*SliceBuilder[T, R].Map`

```go
func (b *SliceBuilder[T, R]) Map(fn func(context.Context, T) (R, error)) *SliceResult[R]
```

[实现](../../internal/sliceops/slice_builder.go#L312) · [行为专题](../mapreduce.md)：Map 根据已配置的策略执行 Map 操作，返回 SliceResult。

**源码说明与使用片段**（原文）：

Map 根据已配置的策略执行 Map 操作，返回 SliceResult。 fn 接收 context.Context 和每个输入元素，返回输出值和错误。

	fn: 转换函数 func(context.Context, T) (R, error)。

	r := sb.Worker(16).Map(func(ctx context.Context, v int) (string, error) {
	    return strconv.Itoa(v), nil
	})
	if r.Err() { return r.Error() }
	vals := r.Values()


**参数说明**：

- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`*SliceResult[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待映射完成，结果按输入/批次索引保存，检查汇总及逐项 Err；取消/FailFast 的空槽和未执行规则依路径。优先 Pool、Shards、Chunk、Timeout/FailFast、普通并行/串行，早期路径可能忽略串行标志。预取消时普通并行 Map（未启用 FailFast）与串行 Map 仍调用回调，业务须检查 context；其他调度路径可在回调前拒绝。空 Map 不等待或复位借用池，但关闭类型匹配的接管池；非空借用池等待后复位。业务 panic 转错误，超时依赖回调配合。

## `*SliceBuilder[T, R].MapBatch`

```go
func (b *SliceBuilder[T, R]) MapBatch(fn func(context.Context, []T) (R, error)) *SliceResult[R]
```

[实现](../../internal/sliceops/slice_builder.go#L326) · [行为专题](../mapreduce.md)：MapBatch 分块映射，fn 接收整个 chunk []T。需配合 Chunk(size) 使用。

**源码说明与使用片段**（原文）：

MapBatch 分块映射，fn 接收整个 chunk \[]T。需配合 Chunk(size) 使用。

	fn: 转换函数 func(context.Context, []T) (R, error)，接收整个分块。

	r := sb.Chunk(100).MapBatch(func(ctx context.Context, chunk []int) (int, error) {
	    sum := 0
	    for _, v := range chunk { sum += v }
	    return sum, nil
	})


**参数说明**：

- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`*SliceResult[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapBatch(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先按 Chunk 分组，非正值使用 1，大于输入长度的正数仅生成一个非空批次，支持 MaxInt；每批调用一次接收 []T 的回调。子切片共享输入，不追加至批次或修改其他批次可见数据。携带同一策略调度批次，MapBatch 每批一个 Result，错误项仍保留返回值，panic 转为 PanicError 且值为零；FailValues 展开失败批次全部原始元素，不证明逐元素执行或回滚。无值统计及有效并行度裁剪按批次数；EachBatch 单批可串行回退且不预检查 context。池结果上限和环形缓冲不截断本终端结果与统计。正数超时按整批计时；重试须考虑部分副作用和幂等。空 MapBatch 不等待或复位借用池，空 EachBatch 仍等待复位，接管池均关闭；核对汇总及逐批错误。

## `*SliceBuilder[T, R].Parallel`

```go
func (b *SliceBuilder[T, R]) Parallel() *ParallelSlice[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L112) · [行为专题](../mapreduce.md)：Parallel 切换为并行模式，返回 ParallelSlice。

**源码说明与使用片段**（原文）：

Parallel 切换为并行模式，返回 ParallelSlice。 并行模式下暴露所有并行配置方法及数据操作。

	sb.Parallel().Worker(8).FailFast().Map(fn).Values()


**参数说明**：无显式参数。

**返回值签名**：`*ParallelSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Parallel()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：无参数并返回新构建器，复制当前切片头与context引用，源配置保持；目标策略整体重置为Seq/DefPar，此前Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。当前长度/容量/数组保留，后续Take/Drop/Append/替换数组只更新各自切片头，共享槽位写入互相可见。context引用保留仍受父取消，但不保留任务超时策略；后续字段赋值不联动。旧池不自动关闭且目标执行不替源收尾，切换后重新配置并安排旧池生命周期；隔离输入须先复制。

## `*SliceBuilder[T, R].Pool`

```go
func (b *SliceBuilder[T, R]) Pool(p *pool.Pool[R]) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L178) · [行为专题](../mapreduce.md)：Pool 注入外部协程池，启用池化执行。

**源码说明与使用片段**（原文）：

Pool 注入外部协程池，启用池化执行。 用户自行管理池生命周期（需 defer p.Close()）。

	p: 外部创建的协程池。

	p := pool.NewPool[int](16); defer p.Close()
	r := sb.Pool(p).Map(fn)


**参数说明**：

- `p` (`*pool.Pool[R]`)：外部创建的协程池。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool(p)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：设置池引用或创建标记，此时不分配池；Pool 借用，PoolAuto 接管关闭，DefaultPool 标记终端内部创建。借用池非空 Map/MapBatch 及空/非空 Each/EachBatch/Stream 等待后 ResetWait，空 Map/MapBatch 直接返回；接管池空输入也关闭。Fold 同步聚合不复位借用池，但关闭已接管池。不能与外部批次混用，Worker 不 Resize 外部池；DefaultPool 在终端按 IO() 创建，也不采用策略 Worker，Worker(1).DefaultPool() 仍可并发启动多个回调。固定并发数须注入对应容量的池。

## `*SliceBuilder[T, R].PoolAuto`

```go
func (b *SliceBuilder[T, R]) PoolAuto(p *pool.Pool[R]) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L189) · [行为专题](../mapreduce.md)：PoolAuto 注入外部协程池，终端方法执行完成后自动 Close。

**源码说明与使用片段**（原文）：

PoolAuto 注入外部协程池，终端方法执行完成后自动 Close。 无需用户 defer p.Close()。

	p: 外部创建的协程池。

	r := sb.PoolAuto(pool.NewPool[int](16)).Map(fn)


**参数说明**：

- `p` (`*pool.Pool[R]`)：外部创建的协程池。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.PoolAuto(p)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：设置池引用或创建标记，此时不分配池；Pool 借用，PoolAuto 接管关闭，DefaultPool 标记终端内部创建。借用池非空 Map/MapBatch 及空/非空 Each/EachBatch/Stream 等待后 ResetWait，空 Map/MapBatch 直接返回；接管池空输入也关闭。Fold 同步聚合不复位借用池，但关闭已接管池。不能与外部批次混用，Worker 不 Resize 外部池；DefaultPool 在终端按 IO() 创建，也不采用策略 Worker，Worker(1).DefaultPool() 仍可并发启动多个回调。固定并发数须注入对应容量的池。

## `*SliceBuilder[T, R].Reduce`

```go
func (b *SliceBuilder[T, R]) Reduce(initial R, fn func(context.Context, R, T) (R, error)) (R, error)
```

[实现](../../internal/sliceops/slice_builder.go#L365) · [行为专题](../mapreduce.md)：Reduce 串行聚合（始终串行执行，不受策略中并发配置影响）。

**源码说明与使用片段**（原文）：

Reduce 串行聚合（始终串行执行，不受策略中并发配置影响）。

	initial: 初始聚合值。
	fn:      聚合函数 func(context.Context, R, T) (R, error)，R 为累积值，T 为当前元素。

	sum, err := sb.Reduce(0, func(ctx context.Context, acc int, v int) (int, error) {
	    return acc + v, nil
	})


**参数说明**：

- `initial` (`R`)：初始聚合值。
- `fn` (`func(context.Context, R, T) (R, error)`)：同步接收 context、当前累积值及输入元素；返回下一累积值和错误，非 nil 错误终止遍历并返回该次累积值，panic 向调用方传播。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reduce(initial, fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：始终按输入顺序同步聚合，空输入返回 initial；遇错返回该次回调给出的累积值和错误，不恢复 panic。忽略策略 Worker/Timeout/FailFast/Shards/Chunk，不自动检查父 context；回调须自行检查。Runner.Fold/Builder.Reduce 仍关闭被接管的外部池，纯串行 SerialSlice.Reduce 直接调用独立 Reduce。

## `*SliceBuilder[T, R].Serial`

```go
func (b *SliceBuilder[T, R]) Serial() *SerialSlice[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L102) · [行为专题](../mapreduce.md)：Serial 切换为串行模式，返回 SerialSlice。

**源码说明与使用片段**（原文）：

Serial 切换为串行模式，返回 SerialSlice。 串行模式下仅暴露串行安全的方法，不暴露 Concurrency/Pool/Shards/Chunk/Buf 等并行配置。

	sb.Serial().Sort(cmp).Filter(pred).Map(fn).Values()


**参数说明**：无显式参数。

**返回值签名**：`*SerialSlice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Serial()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：无参数并返回新构建器，复制当前切片头与context引用，源配置保持；目标策略整体重置为Seq/DefPar，此前Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。当前长度/容量/数组保留，后续Take/Drop/Append/替换数组只更新各自切片头，共享槽位写入互相可见。context引用保留仍受父取消，但不保留任务超时策略；后续字段赋值不联动。旧池不自动关闭且目标执行不替源收尾，切换后重新配置并安排旧池生命周期；隔离输入须先复制。

## `*SliceBuilder[T, R].Shards`

```go
func (b *SliceBuilder[T, R]) Shards(n int) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L234) · [行为专题](../mapreduce.md)：Shards 设置水平分片数，n <= 0 使用 runtime.GOMAXPROCS(0)。

**源码说明与使用片段**（原文）：

Shards 设置水平分片数，n \<= 0 使用 runtime.GOMAXPROCS(0)。 分片模式将切片均匀切分为 n 份并行处理。

	n: 分片数量。

	sb.Shards(8).Map(fn)


**参数说明**：

- `n` (`int`)：分片数量。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].Slice`

```go
func (b *SliceBuilder[T, R]) Slice(i, j int) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L296) · [行为专题](../mapreduce.md)：Slice 截取 [i, j) 范围的子切片，返回自身。等价于 SliceRange（向后兼容命名）。

**源码说明与使用片段**（原文）：

Slice 截取 \[i, j) 范围的子切片，返回自身。等价于 SliceRange（向后兼容命名）。

	i: 起始索引（含）。
	j: 结束索引（不含）。

	sb.Slice(1, 3).Values() // [2, 3]


**参数说明**：

- `i` (`int`)：左闭区间起点；负值修正为 0。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。
- `j` (`int`)：右开区间终点；超出长度修正为长度。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Slice(i, j)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：兼容命名，委托SliceRange更新当前视图并返回自身；负i修为0、超长j修为长度，修正后i>=j以原视图[:0]清空并保留起点和容量。非空区间共享原数组并按起点减少容量，不清零排除槽位；最小/最大int适用相同规则。旧视图头不更新，不应用执行策略；保留输入须先复制，释放旧数组须另行Clip并释放其他引用。

## `*SliceBuilder[T, R].Stream`

```go
func (b *SliceBuilder[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R]
```

[实现](../../internal/sliceops/slice_builder.go#L381) · [行为专题](../mapreduce.md)：Stream 流式 Map，通过 channel 边执行边返回。

**源码说明与使用片段**（原文）：

Stream 流式 Map，通过 channel 边执行边返回。

	fn:      转换函数 func(context.Context, T) (R, error)。
	bufSize: 通道缓冲区大小，<= 0 时使用 Buf() 设置的值。

	ch := sb.Worker(16).Stream(func(ctx context.Context, v int) (int, error) {
	    return v * 2, nil
	}, 1024)
	for res := range ch {
	    if res.Err != nil { ... }
	    fmt.Println(res.Value)
	}


**参数说明**：

- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `bufSize` (`int`)：通道缓冲区大小，<= 0 时使用 Buf() 设置的值。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(fn, bufSize)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出 Result，不携带原索引；有关联要求在返回值内携带标识。正数 bufSize 优先，其次 Buf，仍非正按 min(输入长度,16384)；持续消费或取消父 context，停止消费可能阻塞回调及清理。Pool 优先，无池用 Group worker，不采用 serial/Shards/Chunk；正数 Timeout/FailFast 生效，panic 转错误。池化 Timeout 只约束回调 context，不解除结果投递背压或重写已返回的成功；FailFast 派生取消也不替代父取消，提交拒绝结果可先于触发错误到达。父取消可丢弃待投递结果，清理仍等待所有已开始回调返回。输出独立于池自身 StreamResults 的丢弃；借用池复位会替换其原生流通道并清零 StreamDropped，不能用复位后的计数判断旧批次。空输入仍等待并复位借用池，接管池在输出关闭前关闭。

## `*SliceBuilder[T, R].Timeout`

```go
func (b *SliceBuilder[T, R]) Timeout(d time.Duration) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L199) · [行为专题](../mapreduce.md)：Timeout 设置单任务超时时间。

**源码说明与使用片段**（原文）：

Timeout 设置单任务超时时间。

	d: 每个任务的最大执行时间。

	sb.Timeout(5*time.Second).Map(fn)


**参数说明**：

- `d` (`time.Duration`)：每个任务的最大执行时间。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceBuilder[T, R].Worker`

```go
func (b *SliceBuilder[T, R]) Worker(n int) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L149) · [行为专题](../mapreduce.md)：Worker 设置并发度，n <= 0 恢复默认 IO 并发度。

**源码说明与使用片段**（原文）：

Worker 设置并发度，n \<= 0 恢复默认 IO 并发度。

	n: 并发 goroutine 数量。

	sb.Worker(32).Map(fn)


**参数说明**：

- `n` (`int`)：并发 goroutine 数量。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。

## `*SliceResult[R].Err`

```go
func (r *SliceResult[R]) Err() bool
```

[实现](../../internal/sliceops/slice_result.go#L54) · [行为专题](../mapreduce.md)：Err 表示 Error() 非 nil，不返回 error 对象；需要错误值时调用 Error。

**源码说明与使用片段**（原文）：

Err 表示 Error() 非 nil，不返回 error 对象；需要错误值时调用 Error。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示观察到了错误，使用 Error/Errors 获取错误值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Err()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：汇总错误优先；没有汇总错误时按保留结果顺序找首个非 nil Err。Ok/Err 是布尔判断，空结果且没有汇总错误时 Ok=true；不检查 Occupied，不能证明全部输入已执行，需结合入口的取消/截断规则。

## `*SliceResult[R].Error`

```go
func (r *SliceResult[R]) Error() error
```

[实现](../../internal/sliceops/slice_result.go#L38) · [行为专题](../mapreduce.md)：Error 返回汇总错误；没有汇总错误时按保留结果顺序返回首个逐项错误。

**源码说明与使用片段**（原文）：

Error 返回汇总错误；没有汇总错误时按保留结果顺序返回首个逐项错误。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Error()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：汇总错误优先；没有汇总错误时按保留结果顺序找首个非 nil Err。Ok/Err 是布尔判断，空结果且没有汇总错误时 Ok=true；不检查 Occupied，不能证明全部输入已执行，需结合入口的取消/截断规则。

## `*SliceResult[R].Errors`

```go
func (r *SliceResult[R]) Errors() []error
```

[实现](../../internal/sliceops/slice_result.go#L70) · [行为专题](../mapreduce.md)：Errors 先返回非摘要顶层错误，再按槽位顺序返回逐项错误；不重复添加摘要，

**源码说明与使用片段**（原文）：

Errors 先返回非摘要顶层错误，再按槽位顺序返回逐项错误；不重复添加摘要， 相同 error 值仍按失败槽位分别保留。


**参数说明**：无显式参数。

**返回值签名**：`[]error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Errors()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：先加入非摘要的顶层错误，再按保留顺序加入逐项 Err；不重复加入标记为摘要的顶层错误，但不会对相同 error 值去重。返回新切片，error 对象仍共享；空槽与结果保留上限可能使列表不能覆盖全部输入。

## `*SliceResult[R].FailValues`

```go
func (r *SliceResult[R]) FailValues() []any
```

[实现](../../internal/sliceops/slice_result.go#L85) · [行为专题](../mapreduce.md)：FailValues 返回失败项原始输入的内部切片引用，批处理失败时含该批的输入元素。

**源码说明与使用片段**（原文）：

FailValues 返回失败项原始输入的内部切片引用，批处理失败时含该批的输入元素。 此列表在终端执行结束时收集，不随 Results 的后续修改重新计算。


**参数说明**：无显式参数。

**返回值签名**：`[]any`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailValues()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回内部切片引用，不提供修改隔离；应在执行结束后读取。FailValues 在执行结束时收集，批次失败包含该批全部输入，不随 Results 后续修改重新计算；顶层错误也独立保存。修改 Results 可能使当前逐项查询与保存的首错/失败输入不一致，生产按只读处理；须区分 Err/Occupied 并按需复制引用内容。

## `*SliceResult[R].First`

```go
func (r *SliceResult[R]) First() (R, bool)
```

[实现](../../internal/sliceops/slice_result.go#L106) · [行为专题](../mapreduce.md)：First 返回首个 Err 为 nil 的保留槽位值，不检查 Occupied；零值空槽也可返回零值和 true。

**源码说明与使用片段**（原文）：

First 返回首个 Err 为 nil 的保留槽位值，不检查 Occupied；零值空槽也可返回零值和 true。 没有 Err 为 nil 的槽位时返回零值和 false。


**参数说明**：无显式参数。

**返回值签名**：`R, bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.First()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：值筛选只检查 Err==nil，不检查 Occupied；零值空槽也可能作为成功零值返回。取消或提前返回时用 Results 逐项核对槽位与错误，不能仅凭 Values 长度证明全部业务已执行。元素内部引用仍共享，Must 遇到汇总/逐项错误会 panic。

## `*SliceResult[R].Len`

```go
func (r *SliceResult[R]) Len() int
```

[实现](../../internal/sliceops/slice_result.go#L102) · [行为专题](../mapreduce.md)：Len 返回保留槽位数量，包含失败项和未占用空槽；它不等于 Values 的长度或已执行任务数。

**源码说明与使用片段**（原文）：

Len 返回保留槽位数量，包含失败项和未占用空槽；它不等于 Values 的长度或已执行任务数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Len()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回保留结果槽位数量，含失败与未占用空槽，不等于成功数、完成数或输入数量。不要用 Len 与 Values 的差值推导全部业务失败数；先按入口的结果完整性规则核对。

## `*SliceResult[R].Must`

```go
func (r *SliceResult[R]) Must() []R
```

[实现](../../internal/sliceops/slice_result.go#L91) · [行为专题](../mapreduce.md)：Must 在 Error 非 nil 时 panic，否则返回 Values；适用于初始化或测试中的强断言。

**源码说明与使用片段**（原文）：

Must 在 Error 非 nil 时 panic，否则返回 Values；适用于初始化或测试中的强断言。


**参数说明**：无显式参数。

**返回值签名**：`[]R`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Must()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：值筛选只检查 Err==nil，不检查 Occupied；零值空槽也可能作为成功零值返回。取消或提前返回时用 Results 逐项核对槽位与错误，不能仅凭 Values 长度证明全部业务已执行。元素内部引用仍共享，Must 遇到汇总/逐项错误会 panic。

## `*SliceResult[R].Ok`

```go
func (r *SliceResult[R]) Ok() bool
```

[实现](../../internal/sliceops/slice_result.go#L51) · [行为专题](../mapreduce.md)：Ok 表示 Error() 为 nil；空结果且没有汇总错误时也返回 true。

**源码说明与使用片段**（原文）：

Ok 表示 Error() 为 nil；空结果且没有汇总错误时也返回 true。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示对应结果成功；空集合规则见方法说明。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Ok()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：汇总错误优先；没有汇总错误时按保留结果顺序找首个非 nil Err。Ok/Err 是布尔判断，空结果且没有汇总错误时 Ok=true；不检查 Occupied，不能证明全部输入已执行，需结合入口的取消/截断规则。

## `*SliceResult[R].Results`

```go
func (r *SliceResult[R]) Results() []core.Result[R]
```

[实现](../../internal/sliceops/slice_result.go#L88) · [行为专题](../mapreduce.md)：Results 返回内部逐项结果切片引用；需要隔离修改时调用方须自行复制。

**源码说明与使用片段**（原文）：

Results 返回内部逐项结果切片引用；需要隔离修改时调用方须自行复制。


**参数说明**：无显式参数。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Results()
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：返回内部切片引用，不提供修改隔离；应在执行结束后读取。FailValues 在执行结束时收集，批次失败包含该批全部输入，不随 Results 后续修改重新计算；顶层错误也独立保存。修改 Results 可能使当前逐项查询与保存的首错/失败输入不一致，生产按只读处理；须区分 Err/Occupied 并按需复制引用内容。

## `*SliceResult[R].Unwrap`

```go
func (r *SliceResult[R]) Unwrap() ([]R, error)
```

[实现](../../internal/sliceops/slice_result.go#L99) · [行为专题](../mapreduce.md)：Unwrap 返回 Values 的新切片及 Error，发生部分失败时仍可能有值；不检查 Occupied。

**源码说明与使用片段**（原文）：

Unwrap 返回 Values 的新切片及 Error，发生部分失败时仍可能有值；不检查 Occupied。


**参数说明**：无显式参数。

**返回值签名**：`[]R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Unwrap()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：值筛选只检查 Err==nil，不检查 Occupied；零值空槽也可能作为成功零值返回。取消或提前返回时用 Results 逐项核对槽位与错误，不能仅凭 Values 长度证明全部业务已执行。元素内部引用仍共享，Must 遇到汇总/逐项错误会 panic。

## `*SliceResult[R].Values`

```go
func (r *SliceResult[R]) Values() []R
```

[实现](../../internal/sliceops/slice_result.go#L58) · [行为专题](../mapreduce.md)：Values 返回 Err 为 nil 的保留槽位值组成的新切片，不检查 Occupied；零值空槽也会保留。

**源码说明与使用片段**（原文）：

Values 返回 Err 为 nil 的保留槽位值组成的新切片，不检查 Occupied；零值空槽也会保留。 元素内部的引用仍共享，失败项被省略。


**参数说明**：无显式参数。

**返回值签名**：`[]R`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：值筛选只检查 Err==nil，不检查 Occupied；零值空槽也可能作为成功零值返回。取消或提前返回时用 Results 逐项核对槽位与错误，不能仅凭 Values 长度证明全部业务已执行。元素内部引用仍共享，Must 遇到汇总/逐项错误会 panic。

## `*Slice[T, R].All`

```go
func (s *Slice[T, R]) All(pred func(T) bool) bool
```

[实现](../../internal/sliceops/slice.go#L167) · [行为专题](../mapreduce.md)：All 检查所有元素是否都满足 pred。

**源码说明与使用片段**（原文）：

All 检查所有元素是否都满足 pred。


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。旧 Slice 数据操作同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.All(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：始终同步按输入顺序调用谓词，panic 直接传播，不采用 worker/超时。Contains/Any 遇匹配终止、All 遇不匹配终止，空输入 All=true、Contains/Any=false、Count=0。Filter 原地保留匹配项但不清零尾部；回调不要修改正在遍历的输入。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Any`

```go
func (s *Slice[T, R]) Any(pred func(T) bool) bool
```

[实现](../../internal/sliceops/slice.go#L177) · [行为专题](../mapreduce.md)：Any 检查是否存在任意元素满足 pred。

**源码说明与使用片段**（原文）：

Any 检查是否存在任意元素满足 pred。


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。旧 Slice 数据操作同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Any(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：始终同步按输入顺序调用谓词，panic 直接传播，不采用 worker/超时。Contains/Any 遇匹配终止、All 遇不匹配终止，空输入 All=true、Contains/Any=false、Count=0。Filter 原地保留匹配项但不清零尾部；回调不要修改正在遍历的输入。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Append`

```go
func (s *Slice[T, R]) Append(items ...T) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L278) · [行为专题](../mapreduce.md)：Append 在末尾追加元素，返回自身。

**源码说明与使用片段**（原文）：

Append 在末尾追加元素，返回自身。


**参数说明**：

- `items` (`...T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Append(items...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：更新当前对象切片头并返回自身，元素内部引用不复制；Append/Insert 容量足够时可复用接收者数组。Prepend 以参数 vals 为追加目标，容量足够时采用参数数组并覆盖 vals 长度外的槽位，空参数视图也可能如此。支持同数组重叠参数；Insert 将负索引修正到头部、超过长度修正到尾部。需要保留任一共享视图时先复制对应槽位。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].BinarySearchFunc`

```go
func (s *Slice[T, R]) BinarySearchFunc(target T, cmp func(a, b T) int) (int, bool)
```

[实现](../../internal/sliceops/slice.go#L226) · [行为专题](../mapreduce.md)：BinarySearchFunc 在有序切片中二分查找 target。

**源码说明与使用片段**（原文）：

BinarySearchFunc 在有序切片中二分查找 target。


**参数说明**：

- `target` (`T`)：搜索目标值；二分搜索要求先按同一比较规则排序。
- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`int, bool`。bool 表示目标是否存在；索引为匹配位置或保持排序的插入位置。

**使用示例**（已初始化参数的调用片段）：

```go
instance.BinarySearchFunc(target, cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Chunk`

```go
func (s *Slice[T, R]) Chunk(chunkSize int) [][]T
```

[实现](../../internal/sliceops/slice.go#L438) · [行为专题](../mapreduce.md)：Chunk 将切片按大小分块，返回 [][]T（不再封装为 Slice）。

**源码说明与使用片段**（原文）：

Chunk 将切片按大小分块，返回 \[]\[]T（不再封装为 Slice）。


**参数说明**：

- `chunkSize` (`int`)：每块最大元素数；非正值或空输入返回 nil，末块可不足此数。子切片共享输入且保留剩余容量，append 可能改写后续块。

**返回值签名**：`[][]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Chunk(chunkSize)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按指定大小返回子切片，末块可不足大小，大小非正或空输入返回 nil。每块共享输入数组且未限制 cap，向一块 append 可能覆盖后续块；按需复制或限制容量，引用元素内部仍共享。分块本身不启动任务，也不启用 Chunk 执行策略。

## `*Slice[T, R].ChunkN`

```go
func (s *Slice[T, R]) ChunkN(n int) [][]T
```

[实现](../../internal/sliceops/slice.go#L443) · [行为专题](../mapreduce.md)：ChunkN 按 ceil(len/n) 的块长切分，返回最多 n 份；实际份数可能少于 n。

**源码说明与使用片段**（原文）：

ChunkN 按 ceil(len/n) 的块长切分，返回最多 n 份；实际份数可能少于 n。


**参数说明**：

- `n` (`int`)：用于计算子切片长度 ceil(len/n) 的目标份数；非正值或空输入返回 nil。实际份数可能小于 n，子切片共享输入底层数组。

**返回值签名**：`[][]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ChunkN(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按 ceil(len/n) 计算固定块长再切分，实际可能少于 n 块；n<=0 或空输入返回 nil。每块共享输入数组且未限制 cap，向一块 append 可能覆盖后续块；按需复制或限制容量，引用元素内部仍共享。与 Chunk 策略配置的逐元素调度不同。

## `*Slice[T, R].Clip`

```go
func (s *Slice[T, R]) Clip() *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L381) · [行为专题](../mapreduce.md)：Clip 在容量大于长度时复制到新数组，使 cap == len，返回自身。

**源码说明与使用片段**（原文）：

Clip 在容量大于长度时复制到新数组，使 cap == len，返回自身。 旧数组仍被其他引用持有时不会回收；复制可能暂时增加峰值内存。


**参数说明**：无显式参数。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Clip()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：仅在 cap>len 时复制到 cap==len 的新数组，否则保持原引用；是浅拷贝，不清理其他视图。复制有瞬时内存成本，只有所有旧引用释放后原数组才可回收；不提供深拷贝或并发快照。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Clone`

```go
func (s *Slice[T, R]) Clone() []T
```

[实现](../../internal/sliceops/slice.go#L411) · [行为专题](../mapreduce.md)：Clone 返回切片的浅拷贝副本。

**源码说明与使用片段**（原文）：

Clone 返回切片的浅拷贝副本。


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Clone()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：复制当前全部元素槽位到新切片，空输入返回可用空切片；指针/map/切片元素的内部数据仍共享。复制不能与输入写入并发，需要独立引用内容时自行深拷贝。

## `*Slice[T, R].CompactFunc`

```go
func (s *Slice[T, R]) CompactFunc(eq func(a, b T) bool) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L253) · [行为专题](../mapreduce.md)：CompactFunc 将连续相等（按 eq 判断）的元素去重，原地操作后返回自身。

**源码说明与使用片段**（原文）：

CompactFunc 将连续相等（按 eq 判断）的元素去重，原地操作后返回自身。


**参数说明**：

- `eq` (`func(a, b T) bool`)：相等比较函数；返回 true 表示两元素相等，应满足自反、对称与传递性。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.CompactFunc(eq)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只去除相邻相等项，保留各段首次代表与顺序；eq 在长度至少 2 时必须非 nil，panic 直接传播，不自动排序。原地压缩但不清零缩短后的尾部，含引用时可能继续保留对象；全局去重用 Dedup。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].ContainsFunc`

```go
func (s *Slice[T, R]) ContainsFunc(pred func(T) bool) bool
```

[实现](../../internal/sliceops/slice.go#L127) · [行为专题](../mapreduce.md)：ContainsFunc 判断切片中是否存在满足 pred 的元素。

**源码说明与使用片段**（原文）：

ContainsFunc 判断切片中是否存在满足 pred 的元素。


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。旧 Slice 数据操作同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ContainsFunc(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：始终同步按输入顺序调用谓词，panic 直接传播，不采用 worker/超时。Contains/Any 遇匹配终止、All 遇不匹配终止，空输入 All=true、Contains/Any=false、Count=0。Filter 原地保留匹配项但不清零尾部；回调不要修改正在遍历的输入。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Count`

```go
func (s *Slice[T, R]) Count(pred func(T) bool) int
```

[实现](../../internal/sliceops/slice.go#L187) · [行为专题](../mapreduce.md)：Count 统计满足 pred 的元素个数。

**源码说明与使用片段**（原文）：

Count 统计满足 pred 的元素个数。


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。旧 Slice 数据操作同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Count(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：始终同步按输入顺序调用谓词，panic 直接传播，不采用 worker/超时。Contains/Any 遇匹配终止、All 遇不匹配终止，空输入 All=true、Contains/Any=false、Count=0。Filter 原地保留匹配项但不清零尾部；回调不要修改正在遍历的输入。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].DedupFunc`

```go
func (s *Slice[T, R]) DedupFunc(eq func(a, b T) bool) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L270) · [行为专题](../mapreduce.md)：DedupFunc 移除所有重复元素（不考虑是否连续，按 eq 判断），原地操作后返回自身。

**源码说明与使用片段**（原文）：

DedupFunc 移除所有重复元素（不考虑是否连续，按 eq 判断），原地操作后返回自身。 最坏 O(n²) 次比较；eq 为 nil 时使用 Go 相等性，元素必须可作为 map key。


**参数说明**：

- `eq` (`func(a, b T) bool`)：相等比较函数，返回 true 表示属于同一类；保留首次代表值。非 nil 时支持切片等非可比较元素，最坏 O(n²)；nil 使用 Go 相等性且非可比较值可能 panic。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DedupFunc(eq)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按 eq(已保留代表,当前项) 全局去重，保留首次代表及输入顺序，清零移除尾部；自定义 eq 最坏 O(n²)，须构成等价关系且 panic 直接传播。nil eq 使用 Go map 相等性，动态值不可比较时 panic，NaN 按 Go 相等性不相等；大输入按业务键建索引。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].DefaultForEach`

```go
func (s *Slice[T, R]) DefaultForEach(ctx context.Context, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L223) · [行为专题](../mapreduce.md)：DefaultForEach 使用默认并发度的 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEach 使用默认并发度的 ForEach。 Deprecated: 推荐使用 Each(ctx, fn, DefPar())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEach(ctx, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].DefaultForEachChunk`

```go
func (s *Slice[T, R]) DefaultForEachChunk(ctx context.Context, batchSize int, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L290) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。使用默认并发度 core.IO()。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachChunk(ctx, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].DefaultForEachChunkWithFFTimeout`

```go
func (s *Slice[T, R]) DefaultForEachChunkWithFFTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L308) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。使用默认并发度 core.IO()。失败时取消派生上下文，回调须配合取消。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachChunkWithFFTimeout(ctx, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].DefaultForEachChunkWithFailFast`

```go
func (s *Slice[T, R]) DefaultForEachChunkWithFailFast(ctx context.Context, batchSize int, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L296) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。使用默认并发度 core.IO()。失败时取消派生上下文，回调须配合取消。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachChunkWithFailFast(ctx, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].DefaultForEachChunkWithTimeout`

```go
func (s *Slice[T, R]) DefaultForEachChunkWithTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L302) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。使用默认并发度 core.IO()。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachChunkWithTimeout(ctx, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].DefaultForEachChunked`

```go
func (s *Slice[T, R]) DefaultForEachChunked(ctx context.Context, batchSize int, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L316) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。使用默认并发度 core.IO()。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachChunked(ctx, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].DefaultForEachChunkedWithFFTimeout`

```go
func (s *Slice[T, R]) DefaultForEachChunkedWithFFTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L334) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。使用默认并发度 core.IO()。失败时取消派生上下文，回调须配合取消。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `timeout` (`time.Duration`)：单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachChunkedWithFFTimeout(ctx, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].DefaultForEachChunkedWithFailFast`

```go
func (s *Slice[T, R]) DefaultForEachChunkedWithFailFast(ctx context.Context, batchSize int, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L322) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。使用默认并发度 core.IO()。失败时取消派生上下文，回调须配合取消。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachChunkedWithFailFast(ctx, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].DefaultForEachChunkedWithTimeout`

```go
func (s *Slice[T, R]) DefaultForEachChunkedWithTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L328) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。使用默认并发度 core.IO()。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `timeout` (`time.Duration`)：单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachChunkedWithTimeout(ctx, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].DefaultForEachWithFFTimeout`

```go
func (s *Slice[T, R]) DefaultForEachWithFFTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L274) · [行为专题](../mapreduce.md)：DefaultForEachWithFFTimeout 使用默认并发度的 FailFast + 超时 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachWithFFTimeout 使用默认并发度的 FailFast + 超时 ForEach。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachWithFFTimeout(ctx, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].DefaultForEachWithFailFast`

```go
func (s *Slice[T, R]) DefaultForEachWithFailFast(ctx context.Context, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L229) · [行为专题](../mapreduce.md)：DefaultForEachWithFailFast 使用默认并发度的 FailFast ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachWithFailFast 使用默认并发度的 FailFast ForEach。 Deprecated: 推荐使用 Each(ctx, fn, DefPar().FF())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachWithFailFast(ctx, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].DefaultForEachWithTimeout`

```go
func (s *Slice[T, R]) DefaultForEachWithTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L264) · [行为专题](../mapreduce.md)：DefaultForEachWithTimeout 使用默认并发度的带超时 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachWithTimeout 使用默认并发度的带超时 ForEach。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultForEachWithTimeout(ctx, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].DefaultMap`

```go
func (s *Slice[T, R]) DefaultMap(ctx context.Context, fn func(context.Context, T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L82) · [行为专题](../mapreduce.md)：DefaultMap 使用默认并发度的 Map。

**源码说明与使用片段**（原文）：

DefaultMap 使用默认并发度的 Map。 Deprecated: 推荐使用 Do(ctx, fn, DefPar())，取 Values。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMap(ctx, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].DefaultMapChunk`

```go
func (s *Slice[T, R]) DefaultMapChunk(ctx context.Context, batchSize int, fn func(context.Context, []T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L155) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。使用默认并发度 core.IO()。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapChunk(ctx, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].DefaultMapChunkWithFFTimeout`

```go
func (s *Slice[T, R]) DefaultMapChunkWithFFTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L173) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。使用默认并发度 core.IO()。失败时取消派生上下文，回调须配合取消。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapChunkWithFFTimeout(ctx, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].DefaultMapChunkWithFailFast`

```go
func (s *Slice[T, R]) DefaultMapChunkWithFailFast(ctx context.Context, batchSize int, fn func(context.Context, []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L161) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。使用默认并发度 core.IO()。失败时取消派生上下文，回调须配合取消。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapChunkWithFailFast(ctx, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].DefaultMapChunkWithTimeout`

```go
func (s *Slice[T, R]) DefaultMapChunkWithTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L167) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。使用默认并发度 core.IO()。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapChunkWithTimeout(ctx, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].DefaultMapChunked`

```go
func (s *Slice[T, R]) DefaultMapChunked(ctx context.Context, batchSize int, fn func(context.Context, T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L182) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。使用默认并发度 core.IO()。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapChunked(ctx, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].DefaultMapChunkedWithFFTimeout`

```go
func (s *Slice[T, R]) DefaultMapChunkedWithFFTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L203) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。使用默认并发度 core.IO()。失败时取消派生上下文，回调须配合取消。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `timeout` (`time.Duration`)：每个元素的 context 超时；正数覆盖底层 Group 的默认期限，非正值且 batchSize 为正时不覆盖创建时的全局默认超时。调用的是元素回调，回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapChunkedWithFFTimeout(ctx, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].DefaultMapChunkedWithFailFast`

```go
func (s *Slice[T, R]) DefaultMapChunkedWithFailFast(ctx context.Context, batchSize int, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L189) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。使用默认并发度 core.IO()。失败时取消派生上下文，回调须配合取消。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapChunkedWithFailFast(ctx, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].DefaultMapChunkedWithTimeout`

```go
func (s *Slice[T, R]) DefaultMapChunkedWithTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L196) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。使用默认并发度 core.IO()。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `timeout` (`time.Duration`)：每个元素的 context 超时；正数覆盖底层 Group 的默认期限，非正值且 batchSize 为正时不覆盖创建时的全局默认超时。调用的是元素回调，回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapChunkedWithTimeout(ctx, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].DefaultMapWithFFTimeout`

```go
func (s *Slice[T, R]) DefaultMapWithFFTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L139) · [行为专题](../mapreduce.md)：DefaultMapWithFFTimeout 使用默认并发度的 FailFast + 超时 Map。

**源码说明与使用片段**（原文）：

DefaultMapWithFFTimeout 使用默认并发度的 FailFast + 超时 Map。 Deprecated: 推荐使用 Do(ctx, fn, DefPar().FF().TO(timeout))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapWithFFTimeout(ctx, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].DefaultMapWithFailFast`

```go
func (s *Slice[T, R]) DefaultMapWithFailFast(ctx context.Context, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L89) · [行为专题](../mapreduce.md)：DefaultMapWithFailFast 使用默认并发度的 FailFast Map。

**源码说明与使用片段**（原文）：

DefaultMapWithFailFast 使用默认并发度的 FailFast Map。 Deprecated: 推荐使用 Do(ctx, fn, DefPar().FF())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapWithFailFast(ctx, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].DefaultMapWithTimeout`

```go
func (s *Slice[T, R]) DefaultMapWithTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context, T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L126) · [行为专题](../mapreduce.md)：DefaultMapWithTimeout 使用默认并发度的带超时 Map。

**源码说明与使用片段**（原文）：

DefaultMapWithTimeout 使用默认并发度的带超时 Map。 Deprecated: 推荐使用 Do(ctx, fn, DefPar().TO(timeout))，取 Values。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultMapWithTimeout(ctx, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].Delete`

```go
func (s *Slice[T, R]) Delete(idx int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L304) · [行为专题](../mapreduce.md)：Delete 删除指定位置的元素，返回自身。

**源码说明与使用片段**（原文）：

Delete 删除指定位置的元素，返回自身。


**参数说明**：

- `idx` (`int`)：待删除元素的零起始索引；越界无操作。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Delete(idx)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地移动后续元素并缩短当前视图，不清零移除后的尾部，引用元素可能继续占用内存。Delete 越界无操作；DeleteRange 将负 i 修为 0、超长 j 修为长度，修正后 i>=j 无操作。其他共享视图长度不随之更新。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].DeleteRange`

```go
func (s *Slice[T, R]) DeleteRange(i, j int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L313) · [行为专题](../mapreduce.md)：DeleteRange 删除 [i, j) 范围内的元素，返回自身。

**源码说明与使用片段**（原文）：

DeleteRange 删除 \[i, j) 范围内的元素，返回自身。


**参数说明**：

- `i` (`int`)：左闭区间起点；负值修正为 0。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。
- `j` (`int`)：右开区间终点；超出长度修正为长度。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DeleteRange(i, j)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地移动后续元素并缩短当前视图，不清零移除后的尾部，引用元素可能继续占用内存。Delete 越界无操作；DeleteRange 将负 i 修为 0、超长 j 修为长度，修正后 i>=j 无操作。其他共享视图长度不随之更新。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Do`

```go
func (s *Slice[T, R]) Do(ctx context.Context, fn func(context.Context, T) (R, error), p Policy) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L36) · [行为专题](../mapreduce.md)：Do 根据 Policy 策略执行 Map 操作，统一串行/并行/分块/分片/超时/FailFast。

**源码说明与使用片段**（原文）：

Do 根据 Policy 策略执行 Map 操作，统一串行/并行/分块/分片/超时/FailFast。

使用示例：

	// 并行 + FailFast + 超时
	results, err := s.Do(ctx, fn, sliceops.Par(8).FF().TO(5 * time.Second))

	// 串行
	results, err := s.Do(ctx, fn, sliceops.Seq())

	// 串行 + FailFast
	results, err := s.Do(ctx, fn, sliceops.Seq().FF())

	// 分块并行
	results, err := s.Do(ctx, fn, sliceops.Par(16).Chunk(100))

	// 水平分片
	results, err := s.Do(ctx, fn, sliceops.Par(8).Shard(4))


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `p` (`Policy`)：执行策略值，包含串行/并行、worker、超时、分片等设置；以返回的新策略继续配置。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Do(ctx, fn, p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].Drop`

```go
func (s *Slice[T, R]) Drop(n int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L350) · [行为专题](../mapreduce.md)：Drop 丢弃前 n 个元素，返回自身。

**源码说明与使用片段**（原文）：

Drop 丢弃前 n 个元素，返回自身。


**参数说明**：

- `n` (`int`)：丢弃的前 n 个元素；n<=0 无操作，n>=长度清空当前切片视图。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Drop(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：仅修改当前切片视图，不复制或清零被排除元素，底层大数组仍可能保留。Take非正清空、过大无操作；Drop非正无操作、n>=len清空；SliceRange先修正负i/超长j，i>=j清空。清空均使用原视图[:0]并保留起点和容量；Drop有效前缀或非空SliceRange才移动起点并减少容量。不使用端点相加，极大/极小int遵循同样规则；其他视图头不更新，需要缩小保留数组时再Clip。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Each`

```go
func (s *Slice[T, R]) Each(ctx context.Context, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error)
```

[实现](../../internal/sliceops/mapreduce.go#L41) · [行为专题](../mapreduce.md)：Each 根据 Policy 策略执行 ForEach 操作。

**源码说明与使用片段**（原文）：

Each 根据 Policy 策略执行 ForEach 操作。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `p` (`Policy`)：执行策略值，包含串行/并行、worker、超时、分片等设置；以返回的新策略继续配置。

**返回值签名**：`total int64, failCnt int64, firstErr error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Each(ctx, fn, p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].EachFull`

```go
func (s *Slice[T, R]) EachFull(ctx context.Context, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L47) · [行为专题](../mapreduce.md)：EachFull 与 Each 相同，额外返回完整结果切片。

**源码说明与使用片段**（原文）：

EachFull 与 Each 相同，额外返回完整结果切片。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `p` (`Policy`)：执行策略值，包含串行/并行、worker、超时、分片等设置；以返回的新策略继续配置。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.EachFull(ctx, fn, p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].EachStream`

```go
func (s *Slice[T, R]) EachStream(ctx context.Context, fn func(context.Context, T) error, p Policy) <-chan core.Result[struct{}]
```

[实现](../../internal/sliceops/mapreduce.go#L57) · [行为专题](../mapreduce.md)：EachStream 根据 Policy 策略执行流式 ForEach 操作。

**源码说明与使用片段**（原文）：

EachStream 根据 Policy 策略执行流式 ForEach 操作。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `p` (`Policy`)：执行策略值，包含串行/并行、worker、超时、分片等设置；以返回的新策略继续配置。

**返回值签名**：`<-chan core.Result[struct{}]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.EachStream(ctx, fn, p)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出无值 Result，不携带输入索引；持续消费至关闭或取消父 context，缓冲满可能阻塞回调。此旧 Slice 无值流忽略 Policy 的 Pool/Shards/Chunk/serial，使用 Group worker；正数 Timeout/FailFast 生效，panic 转为 PanicError。非正缓冲按 min(输入长度,16384)，取消可能只输出部分项。

## `*Slice[T, R].Filter`

```go
func (s *Slice[T, R]) Filter(pred func(T) bool) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L240) · [行为专题](../mapreduce.md)：Filter 保留满足 pred 的元素，原地过滤后返回自身。

**源码说明与使用片段**（原文）：

Filter 保留满足 pred 的元素，原地过滤后返回自身。 不清零旧尾部；谓词 panic 原样传播，长度未更新但前部可能已改写。


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。旧 Slice 数据操作同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Filter(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐元素稳定过滤并原地压缩，不清零旧尾部，旧视图长度不随结果缩短；不检查context或采用worker/超时。谓词panic原样传播，内部切片长度尚未更新但前部可能已改写，不回滚；recover后应重建有效输入。保留输入须过滤前复制元素槽位，之后Values不能撤销改写，引用内容仍共享。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].FindFunc`

```go
func (s *Slice[T, R]) FindFunc(pred func(T) bool) (val T, found bool)
```

[实现](../../internal/sliceops/slice.go#L147) · [行为专题](../mapreduce.md)：FindFunc 返回首个满足 pred 的元素。

**源码说明与使用片段**（原文）：

FindFunc 返回首个满足 pred 的元素。


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。旧 Slice 数据操作同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`val T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FindFunc(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步查找并在首次符合所需方向的匹配处终止，FindLast 从尾部扫描；Index 未匹配返回 -1，Find/FindLast 未匹配返回零值和 false。回调须非 nil，panic 直接传播；不采用 worker/超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].FindLastFunc`

```go
func (s *Slice[T, R]) FindLastFunc(pred func(T) bool) (val T, found bool)
```

[实现](../../internal/sliceops/slice.go#L157) · [行为专题](../mapreduce.md)：FindLastFunc 返回最后一个满足 pred 的元素。

**源码说明与使用片段**（原文）：

FindLastFunc 返回最后一个满足 pred 的元素。


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。旧 Slice 数据操作同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`val T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FindLastFunc(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步查找并在首次符合所需方向的匹配处终止，FindLast 从尾部扫描；Index 未匹配返回 -1，Find/FindLast 未匹配返回零值和 false。回调须非 nil，panic 直接传播；不采用 worker/超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].First`

```go
func (s *Slice[T, R]) First() (val T, found bool)
```

[实现](../../internal/sliceops/slice.go#L76) · [行为专题](../mapreduce.md)：First 返回第一个元素，空切片则返回零值和 false。

**源码说明与使用片段**（原文）：

First 返回第一个元素，空切片则返回零值和 false。


**参数说明**：无显式参数。

**返回值签名**：`val T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.First()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].ForEach`

```go
func (s *Slice[T, R]) ForEach(ctx context.Context, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L211) · [行为专题](../mapreduce.md)：ForEach 并发遍历切片。

**源码说明与使用片段**（原文）：

ForEach 并发遍历切片。 Deprecated: 推荐使用 Each(ctx, fn, Par(concurrency))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(ctx, fn, concurrency)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachChunk`

```go
func (s *Slice[T, R]) ForEachChunk(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L287) · [行为专题](../mapreduce.md)：ForEachChunk 先分块再并发处理每个块，fn 接收整个 chunk。

**源码说明与使用片段**（原文）：

ForEachChunk 先分块再并发处理每个块，fn 接收整个 chunk。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachChunk(ctx, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].ForEachChunkWithFFTimeout`

```go
func (s *Slice[T, R]) ForEachChunkWithFFTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L305) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。失败时取消派生上下文，回调须配合取消。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachChunkWithFFTimeout(ctx, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].ForEachChunkWithFailFast`

```go
func (s *Slice[T, R]) ForEachChunkWithFailFast(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L293) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。失败时取消派生上下文，回调须配合取消。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachChunkWithFailFast(ctx, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].ForEachChunkWithTimeout`

```go
func (s *Slice[T, R]) ForEachChunkWithTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L299) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachChunkWithTimeout(ctx, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].ForEachChunked`

```go
func (s *Slice[T, R]) ForEachChunked(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L313) · [行为专题](../mapreduce.md)：ForEachChunked 先分块再逐元素并发执行。

**源码说明与使用片段**（原文）：

ForEachChunked 先分块再逐元素并发执行。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachChunked(ctx, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachChunkedWithFFTimeout`

```go
func (s *Slice[T, R]) ForEachChunkedWithFFTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L331) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。失败时取消派生上下文，回调须配合取消。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `timeout` (`time.Duration`)：单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachChunkedWithFFTimeout(ctx, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachChunkedWithFailFast`

```go
func (s *Slice[T, R]) ForEachChunkedWithFailFast(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L319) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。失败时取消派生上下文，回调须配合取消。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachChunkedWithFailFast(ctx, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachChunkedWithTimeout`

```go
func (s *Slice[T, R]) ForEachChunkedWithTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L325) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `timeout` (`time.Duration`)：单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachChunkedWithTimeout(ctx, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachSerial`

```go
func (s *Slice[T, R]) ForEachSerial(ctx context.Context, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L235) · [行为专题](../mapreduce.md)：ForEachSerial 串行遍历切片。

**源码说明与使用片段**（原文）：

ForEachSerial 串行遍历切片。 Deprecated: 推荐使用 Each(ctx, fn, Seq())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachSerial(ctx, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachSerialFailFast`

```go
func (s *Slice[T, R]) ForEachSerialFailFast(ctx context.Context, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L241) · [行为专题](../mapreduce.md)：ForEachSerialFailFast 串行带快速失败的 ForEach。

**源码说明与使用片段**（原文）：

ForEachSerialFailFast 串行带快速失败的 ForEach。 Deprecated: 推荐使用 Each(ctx, fn, Seq().FF())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachSerialFailFast(ctx, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachSharded`

```go
func (s *Slice[T, R]) ForEachSharded(ctx context.Context, fn func(context.Context, T) error, concurrency int, shards int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L280) · [行为专题](../mapreduce.md)：ForEachSharded 水平分片并发 ForEach。

**源码说明与使用片段**（原文）：

ForEachSharded 水平分片并发 ForEach。 Deprecated: 推荐使用 Each(ctx, fn, Par(concurrency).Shard(shards))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachSharded(ctx, fn, concurrency, shards)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachStream`

```go
func (s *Slice[T, R]) ForEachStream(ctx context.Context, fn func(context.Context, T) error, concurrency int, bufSize int) <-chan core.Result[struct{}]
```

[实现](../../internal/sliceops/mapreduce.go#L247) · [行为专题](../mapreduce.md)：ForEachStream 并发遍历，通过 channel 流式返回。

**源码说明与使用片段**（原文）：

ForEachStream 并发遍历，通过 channel 流式返回。 Deprecated: 推荐使用 EachStream(ctx, fn, Par(concurrency).Buf(bufSize))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[struct{}]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachStream(ctx, fn, concurrency, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出无值 Result，不携带输入索引；持续消费至关闭或取消父 context，缓冲满可能阻塞回调。此旧 Slice 无值流忽略 Policy 的 Pool/Shards/Chunk/serial，使用 Group worker；正数 Timeout/FailFast 生效，panic 转为 PanicError。非正缓冲按 min(输入长度,16384)，取消可能只输出部分项。

## `*Slice[T, R].ForEachStreamWithFailFast`

```go
func (s *Slice[T, R]) ForEachStreamWithFailFast(ctx context.Context, fn func(context.Context, T) error, concurrency int, bufSize int) <-chan core.Result[struct{}]
```

[实现](../../internal/sliceops/mapreduce.go#L253) · [行为专题](../mapreduce.md)：ForEachStreamWithFailFast 带 FailFast 的流式 ForEach。

**源码说明与使用片段**（原文）：

ForEachStreamWithFailFast 带 FailFast 的流式 ForEach。 Deprecated: 推荐使用 EachStream(ctx, fn, Par(concurrency).FF().Buf(bufSize))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[struct{}]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachStreamWithFailFast(ctx, fn, concurrency, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出无值 Result，不携带输入索引；持续消费至关闭或取消父 context，缓冲满可能阻塞回调。此旧 Slice 无值流忽略 Policy 的 Pool/Shards/Chunk/serial，使用 Group worker；正数 Timeout/FailFast 生效，panic 转为 PanicError。非正缓冲按 min(输入长度,16384)，取消可能只输出部分项。

## `*Slice[T, R].ForEachWithFFTimeout`

```go
func (s *Slice[T, R]) ForEachWithFFTimeout(ctx context.Context, concurrency int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L269) · [行为专题](../mapreduce.md)：ForEachWithFFTimeout 带 FailFast 和单任务超时的 ForEach。

**源码说明与使用片段**（原文）：

ForEachWithFFTimeout 带 FailFast 和单任务超时的 ForEach。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `timeout` (`time.Duration`)：单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachWithFFTimeout(ctx, concurrency, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachWithFailFast`

```go
func (s *Slice[T, R]) ForEachWithFailFast(ctx context.Context, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L217) · [行为专题](../mapreduce.md)：ForEachWithFailFast 并发 ForEach，首个失败时取消其余任务。

**源码说明与使用片段**（原文）：

ForEachWithFailFast 并发 ForEach，首个失败时取消其余任务。 Deprecated: 推荐使用 Each(ctx, fn, Par(concurrency).FF())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachWithFailFast(ctx, fn, concurrency)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].ForEachWithTimeout`

```go
func (s *Slice[T, R]) ForEachWithTimeout(ctx context.Context, concurrency int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/mapreduce.go#L259) · [行为专题](../mapreduce.md)：ForEachWithTimeout 带单任务超时的 ForEach。

**源码说明与使用片段**（原文）：

ForEachWithTimeout 带单任务超时的 ForEach。 Deprecated: 推荐使用 Each(ctx, fn, Par(concurrency).TO(timeout))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `timeout` (`time.Duration`)：单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachWithTimeout(ctx, concurrency, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。

## `*Slice[T, R].Grow`

```go
func (s *Slice[T, R]) Grow(n int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L391) · [行为专题](../mapreduce.md)：Grow 确保容量至少容纳 len+n 个元素，不改变长度，返回自身；n <= 0 无操作。

**源码说明与使用片段**（原文）：

Grow 确保容量至少容纳 len+n 个元素，不改变长度，返回自身；n \<= 0 无操作。


**参数说明**：

- `n` (`int`)：额外容量需求；非正值无操作，正数追加零值再恢复原长度，可能改写共享数组长度外槽位。须控制 len+n 和分配预算。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Grow(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：n<=0 无操作；正数通过追加 n 个零值再恢复原长度确保容量，可能清零原数组长度外的槽位。它不改变长度，不保证精确容量，也不是无分配检查；控制 n 和内存预算，旧共享视图可能观察到尾部变化。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].IndexFunc`

```go
func (s *Slice[T, R]) IndexFunc(pred func(T) bool) int
```

[实现](../../internal/sliceops/slice.go#L137) · [行为专题](../mapreduce.md)：IndexFunc 返回首个满足 pred 的元素的索引，-1 表示不存在。

**源码说明与使用片段**（原文）：

IndexFunc 返回首个满足 pred 的元素的索引，-1 表示不存在。


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。旧 Slice 数据操作同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IndexFunc(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步查找并在首次符合所需方向的匹配处终止，FindLast 从尾部扫描；Index 未匹配返回 -1，Find/FindLast 未匹配返回零值和 false。回调须非 nil，panic 直接传播；不采用 worker/超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Insert`

```go
func (s *Slice[T, R]) Insert(idx int, items ...T) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L291) · [行为专题](../mapreduce.md)：Insert 在指定位置插入元素，返回自身。若 idx < 0 插到头部，idx > len 插到尾部。

**源码说明与使用片段**（原文）：

Insert 在指定位置插入元素，返回自身。若 idx \< 0 插到头部，idx > len 插到尾部。


**参数说明**：

- `idx` (`int`)：零起始插入位置；负数插到头部，大于长度插到末尾。
- `items` (`...T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Insert(idx, items...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：更新当前对象切片头并返回自身，元素内部引用不复制；Append/Insert 容量足够时可复用接收者数组。Prepend 以参数 vals 为追加目标，容量足够时采用参数数组并覆盖 vals 长度外的槽位，空参数视图也可能如此。支持同数组重叠参数；Insert 将负索引修正到头部、超过长度修正到尾部。需要保留任一共享视图时先复制对应槽位。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].IsEmpty`

```go
func (s *Slice[T, R]) IsEmpty() bool
```

[实现](../../internal/sliceops/slice.go#L71) · [行为专题](../mapreduce.md)：IsEmpty 返回切片是否为空。

**源码说明与使用片段**（原文）：

IsEmpty 返回切片是否为空。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsEmpty()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].IsSortedFunc`

```go
func (s *Slice[T, R]) IsSortedFunc(cmp func(a, b T) int) bool
```

[实现](../../internal/sliceops/slice.go#L110) · [行为专题](../mapreduce.md)：IsSortedFunc 检查切片是否已按 cmp 有序。

**源码说明与使用片段**（原文）：

IsSortedFunc 检查切片是否已按 cmp 有序。


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsSortedFunc(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Items`

```go
func (s *Slice[T, R]) Items() []T
```

[实现](../../internal/sliceops/slice.go#L61) · [行为专题](../mapreduce.md)：Items 返回内部切片引用（不复制，修改会反映到 Slice 内部）。

**源码说明与使用片段**（原文）：

Items 返回内部切片引用（不复制，修改会反映到 Slice 内部）。


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Items()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回内部切片引用，元素写入立即影响当前对象；返回的切片头是副本，对它 append 不会自动更新对象长度。后续对象重新分配/截取不会更新旧视图，需独立元素槽位时用 Values/Clone。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Last`

```go
func (s *Slice[T, R]) Last() (val T, found bool)
```

[实现](../../internal/sliceops/slice.go#L84) · [行为专题](../mapreduce.md)：Last 返回最后一个元素，空切片则返回零值和 false。

**源码说明与使用片段**（原文）：

Last 返回最后一个元素，空切片则返回零值和 false。


**参数说明**：无显式参数。

**返回值签名**：`val T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Last()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Len`

```go
func (s *Slice[T, R]) Len() int
```

[实现](../../internal/sliceops/slice.go#L66) · [行为专题](../mapreduce.md)：Len 返回元素个数。

**源码说明与使用片段**（原文）：

Len 返回元素个数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Len()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Map`

```go
func (s *Slice[T, R]) Map(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L70) · [行为专题](../mapreduce.md)：Map 并发处理切片元素，返回 Result 切片。concurrency <= 0 时使用 core.IO()。

**源码说明与使用片段**（原文）：

Map 并发处理切片元素，返回 Result 切片。concurrency \<= 0 时使用 core.IO()。 Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(ctx, fn, concurrency)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapChunk`

```go
func (s *Slice[T, R]) MapChunk(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L152) · [行为专题](../mapreduce.md)：MapChunk 先分块再并发处理每个块，fn 接收整个 chunk。

**源码说明与使用片段**（原文）：

MapChunk 先分块再并发处理每个块，fn 接收整个 chunk。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapChunk(ctx, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].MapChunkWithFFTimeout`

```go
func (s *Slice[T, R]) MapChunkWithFFTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L170) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。失败时取消派生上下文，回调须配合取消。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapChunkWithFFTimeout(ctx, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].MapChunkWithFailFast`

```go
func (s *Slice[T, R]) MapChunkWithFailFast(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L158) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。失败时取消派生上下文，回调须配合取消。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapChunkWithFailFast(ctx, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].MapChunkWithTimeout`

```go
func (s *Slice[T, R]) MapChunkWithTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L164) · [行为专题](../mapreduce.md)：将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapChunkWithTimeout(ctx, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。

## `*Slice[T, R].MapChunked`

```go
func (s *Slice[T, R]) MapChunked(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L178) · [行为专题](../mapreduce.md)：MapChunked 先分块再逐元素并发调用 fn。

**源码说明与使用片段**（原文）：

MapChunked 先分块再逐元素并发调用 fn。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapChunked(ctx, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapChunkedWithFFTimeout`

```go
func (s *Slice[T, R]) MapChunkedWithFFTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L200) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。失败时取消派生上下文，回调须配合取消。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `timeout` (`time.Duration`)：每个元素的 context 超时；正数覆盖底层 Group 的默认期限，非正值且 batchSize 为正时不覆盖创建时的全局默认超时。调用的是元素回调，回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapChunkedWithFFTimeout(ctx, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapChunkedWithFailFast`

```go
func (s *Slice[T, R]) MapChunkedWithFailFast(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L186) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。失败时取消派生上下文，回调须配合取消。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapChunkedWithFailFast(ctx, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapChunkedWithTimeout`

```go
func (s *Slice[T, R]) MapChunkedWithTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L192) · [行为专题](../mapreduce.md)：分块调度并逐元素调用回调；结果及统计单位为元素。为单次回调设置超时上下文，不能强制终止业务函数。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。
- `timeout` (`time.Duration`)：每个元素的 context 超时；正数覆盖底层 Group 的默认期限，非正值且 batchSize 为正时不覆盖创建时的全局默认超时。调用的是元素回调，回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapChunkedWithTimeout(ctx, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapSerial`

```go
func (s *Slice[T, R]) MapSerial(ctx context.Context, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L95) · [行为专题](../mapreduce.md)：MapSerial 串行处理切片元素，返回 Result 切片。

**源码说明与使用片段**（原文）：

MapSerial 串行处理切片元素，返回 Result 切片。 Deprecated: 推荐使用 Do(ctx, fn, Seq())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapSerial(ctx, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapSerialFailFast`

```go
func (s *Slice[T, R]) MapSerialFailFast(ctx context.Context, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L101) · [行为专题](../mapreduce.md)：MapSerialFailFast 串行带快速失败的 Map。

**源码说明与使用片段**（原文）：

MapSerialFailFast 串行带快速失败的 Map。 Deprecated: 推荐使用 Do(ctx, fn, Seq().FF())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapSerialFailFast(ctx, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapSharded`

```go
func (s *Slice[T, R]) MapSharded(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int, shards int) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L145) · [行为专题](../mapreduce.md)：MapSharded 水平分片并发 Map。

**源码说明与使用片段**（原文）：

MapSharded 水平分片并发 Map。 Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency).Shard(shards))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `shards` (`int`)：水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapSharded(ctx, fn, concurrency, shards)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapStream`

```go
func (s *Slice[T, R]) MapStream(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int, bufSize int) <-chan core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L107) · [行为专题](../mapreduce.md)：MapStream 并发处理切片元素，通过 channel 流式返回结果。

**源码说明与使用片段**（原文）：

MapStream 并发处理切片元素，通过 channel 流式返回结果。 Deprecated: 推荐使用 Stream(ctx, fn, Par(concurrency).Buf(bufSize))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapStream(ctx, fn, concurrency, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出 Result，不携带输入索引，需要关联时在值中携带标识。持续消费至关闭或取消父 context，缓冲满可阻塞回调；Pool 优先，否则使用 Group worker 且不采用 Shards/Chunk/serial。正数 Timeout/FailFast 生效，panic 转为 PanicError；Pool 引用须匹配结果类型，借用池会 ResetWait，接管池最终关闭。

## `*Slice[T, R].MapStreamWithFailFast`

```go
func (s *Slice[T, R]) MapStreamWithFailFast(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int, bufSize int) <-chan core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L113) · [行为专题](../mapreduce.md)：MapStreamWithFailFast 带 FailFast 的流式 Map。

**源码说明与使用片段**（原文）：

MapStreamWithFailFast 带 FailFast 的流式 Map。 Deprecated: 推荐使用 Stream(ctx, fn, Par(concurrency).FF().Buf(bufSize))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapStreamWithFailFast(ctx, fn, concurrency, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出 Result，不携带输入索引，需要关联时在值中携带标识。持续消费至关闭或取消父 context，缓冲满可阻塞回调；Pool 优先，否则使用 Group worker 且不采用 Shards/Chunk/serial。正数 Timeout/FailFast 生效，panic 转为 PanicError；Pool 引用须匹配结果类型，借用池会 ResetWait，接管池最终关闭。

## `*Slice[T, R].MapWithFFTimeout`

```go
func (s *Slice[T, R]) MapWithFFTimeout(ctx context.Context, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L133) · [行为专题](../mapreduce.md)：MapWithFFTimeout 带 FailFast 和单任务超时的并发 Map。

**源码说明与使用片段**（原文）：

MapWithFFTimeout 带 FailFast 和单任务超时的并发 Map。 Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency).FF().TO(timeout))。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapWithFFTimeout(ctx, concurrency, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapWithFailFast`

```go
func (s *Slice[T, R]) MapWithFailFast(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/mapreduce.go#L76) · [行为专题](../mapreduce.md)：MapWithFailFast 并发 Map，首个失败时取消其余任务。

**源码说明与使用片段**（原文）：

MapWithFailFast 并发 Map，首个失败时取消其余任务。 Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency).FF())。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapWithFailFast(ctx, fn, concurrency)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MapWithTimeout`

```go
func (s *Slice[T, R]) MapWithTimeout(ctx context.Context, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L119) · [行为专题](../mapreduce.md)：MapWithTimeout 带单任务超时的并发 Map。

**源码说明与使用片段**（原文）：

MapWithTimeout 带单任务超时的并发 Map。 Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency).TO(timeout))，取 Values。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapWithTimeout(ctx, concurrency, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。

## `*Slice[T, R].MaxFunc`

```go
func (s *Slice[T, R]) MaxFunc(cmp func(a, b T) int) (max T, found bool)
```

[实现](../../internal/sliceops/slice.go#L198) · [行为专题](../mapreduce.md)：MaxFunc 返回 cmp 下最大的元素。

**源码说明与使用片段**（原文）：

MaxFunc 返回 cmp 下最大的元素。


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`max T, found bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MaxFunc(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].MinFunc`

```go
func (s *Slice[T, R]) MinFunc(cmp func(a, b T) int) (min T, found bool)
```

[实现](../../internal/sliceops/slice.go#L212) · [行为专题](../mapreduce.md)：MinFunc 返回 cmp 下最小的元素。

**源码说明与使用片段**（原文）：

MinFunc 返回 cmp 下最小的元素。


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`min T, found bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MinFunc(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Prepend`

```go
func (s *Slice[T, R]) Prepend(items ...T) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L285) · [行为专题](../mapreduce.md)：Prepend 在头部插入元素，返回自身。

**源码说明与使用片段**（原文）：

Prepend 在头部插入元素，返回自身。 容量足够时复用参数数组并覆盖参数原长度外的槽位，共享视图可见。


**参数说明**：

- `items` (`...T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Prepend(items...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：更新当前对象切片头并返回自身，元素内部引用不复制；Append/Insert 容量足够时可复用接收者数组。Prepend 以参数 vals 为追加目标，容量足够时采用参数数组并覆盖 vals 长度外的槽位，空参数视图也可能如此。支持同数组重叠参数；Insert 将负索引修正到头部、超过长度修正到尾部。需要保留任一共享视图时先复制对应槽位。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Reduce`

```go
func (s *Slice[T, R]) Reduce(ctx context.Context, initial R, fn func(context.Context, R, T) (R, error)) (R, error)
```

[实现](../../internal/sliceops/mapreduce.go#L62) · [行为专题](../mapreduce.md)：Reduce 串行聚合（始终串行，无需 Policy）。

**源码说明与使用片段**（原文）：

Reduce 串行聚合（始终串行，无需 Policy）。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `initial` (`R`)：聚合的初始累积值。
- `fn` (`func(context.Context, R, T) (R, error)`)：同步接收 context、当前累积值及输入元素；返回下一累积值和错误，非 nil 错误终止遍历并返回该次累积值，panic 向调用方传播。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reduce(ctx, initial, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按输入顺序同步聚合，空输入返回 initial；遇错返回本次回调的累积值和错误，panic 直接传播。不自动检查 context 或设置超时，业务须自行响应；输入与其他视图共享，不与修改竞争。

## `*Slice[T, R].Repeat`

```go
func (s *Slice[T, R]) Repeat(n int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L419) · [行为专题](../mapreduce.md)：Repeat 将切片重复 n 次后替换内部数据，返回自身。

**源码说明与使用片段**（原文）：

Repeat 将切片重复 n 次后替换内部数据，返回自身。 空输入直接返回并保留现有视图；非空输入重复追加，元素内部引用仍共享。


**参数说明**：

- `n` (`int`)：原切片的总重复次数；非正值清空，1 无操作，更大值反复浅追加。调用前校验 len×n 的数量和内存预算，元素内部引用不复制。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Repeat(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：n<=0 清空当前视图，n==1 无操作；空输入直接返回并保留空视图及容量，不按 n 循环。非空且 n>1 时复制原元素槽位后重复追加，容量足够可复用数组，引用类型内容并不隔离。总元素数约 len×n，须预先校验数量/内存预算；此方法没有上限、取消或分配失败恢复。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Replace`

```go
func (s *Slice[T, R]) Replace(i, j int, items ...T) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L330) · [行为专题](../mapreduce.md)：Replace 将 [i, j) 范围内的元素替换为 items，返回自身。

**源码说明与使用片段**（原文）：

Replace 将 \[i, j) 范围内的元素替换为 items，返回自身。 支持重叠参数；缩短时清零新长度到旧长度之间的槽位，共享视图可见。


**参数说明**：

- `i` (`int`)：替换区间的起点（含）；先将负 i 修正为 0，再将大于修正后 j 的 i 降为 j。修正后仍非法的边界会 panic。
- `j` (`int`)：替换区间的终点（不含）；大于长度时修正为长度，负数不修正并会 panic。
- `items` (`...T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Replace(i, j, items...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：先修正负 i 为 0、超长 j 为长度，再把 i>j 修为 j；负 j 仍会 panic，调用前校验边界。随后用 slices.Replace 替换 [i,j)，支持重叠参数，可变更长度和分配数组；缩短时清零新长度到旧长度之间的槽位，共享旧视图会观察到清零。元素引用仍共享，修改不回滚。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Reverse`

```go
func (s *Slice[T, R]) Reverse() *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L117) · [行为专题](../mapreduce.md)：Reverse 原地反转切片，返回自身。

**源码说明与使用片段**（原文）：

Reverse 原地反转切片，返回自身。


**参数说明**：无显式参数。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reverse()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地调整当前视图的元素顺序并返回自身，空/单元素无变化；Shuffle 可能保持原序或重复上次结果，使用全局伪随机源，不提供安全随机或独立种子保证。共享视图会看到修改，不应用执行策略。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Shuffle`

```go
func (s *Slice[T, R]) Shuffle() *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L403) · [行为专题](../mapreduce.md)：Shuffle 使用随机源原地打乱切片，返回自身。

**源码说明与使用片段**（原文）：

Shuffle 使用随机源原地打乱切片，返回自身。


**参数说明**：无显式参数。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shuffle()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地调整当前视图的元素顺序并返回自身，空/单元素无变化；Shuffle 可能保持原序或重复上次结果，使用全局伪随机源，不提供安全随机或独立种子保证。共享视图会看到修改，不应用执行策略。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].SliceRange`

```go
func (s *Slice[T, R]) SliceRange(i, j int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L363) · [行为专题](../mapreduce.md)：SliceRange 保留 [i, j) 范围的元素，返回自身。

**源码说明与使用片段**（原文）：

SliceRange 保留 \[i, j) 范围的元素，返回自身。


**参数说明**：

- `i` (`int`)：左闭区间起点；负值修正为 0。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。
- `j` (`int`)：右开区间终点；超出长度修正为长度。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SliceRange(i, j)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：仅修改当前切片视图，不复制或清零被排除元素，底层大数组仍可能保留。Take非正清空、过大无操作；Drop非正无操作、n>=len清空；SliceRange先修正负i/超长j，i>=j清空。清空均使用原视图[:0]并保留起点和容量；Drop有效前缀或非空SliceRange才移动起点并减少容量。不使用端点相加，极大/极小int遵循同样规则；其他视图头不更新，需要缩小保留数组时再Clip。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].SortFunc`

```go
func (s *Slice[T, R]) SortFunc(cmp func(a, b T) int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L94) · [行为专题](../mapreduce.md)：SortFunc 使用 cmp 对切片进行原地排序，返回自身。

**源码说明与使用片段**（原文）：

SortFunc 使用 cmp 对切片进行原地排序，返回自身。


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SortFunc(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Split`

```go
func (s *Slice[T, R]) Split(pred func(T) bool) (matched []T, unmatched []T)
```

[实现](../../internal/sliceops/slice.go#L449) · [行为专题](../mapreduce.md)：Split 按条件拆分为匹配和不匹配两部分，各自保留输入顺序。

**源码说明与使用片段**（原文）：

Split 按条件拆分为匹配和不匹配两部分，各自保留输入顺序。 返回切片的元素槽位独立，元素内部引用仍与输入共享；空分组为 nil。


**参数说明**：

- `pred` (`func(T) bool`)：同步按输入顺序判断元素；true 放入 matched，false 放入 unmatched。返回两个新切片但元素内部引用仍共享，panic 直接传播，不修改输入切片。

**返回值签名**：`matched []T, unmatched []T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Split(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步按输入顺序调用谓词，true/false 分别进入两个新切片，原输入不改变；空输入两者为 nil。元素引用仍共享，谓词须非 nil 且 panic 直接传播；不应用并发策略，不与共享输入写入竞争。

## `*Slice[T, R].StableSortFunc`

```go
func (s *Slice[T, R]) StableSortFunc(cmp func(a, b T) int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L102) · [行为专题](../mapreduce.md)：StableSortFunc 使用 cmp 对切片进行原地稳定排序，返回自身。

**源码说明与使用片段**（原文）：

StableSortFunc 使用 cmp 对切片进行原地稳定排序，返回自身。


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StableSortFunc(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Stream`

```go
func (s *Slice[T, R]) Stream(ctx context.Context, fn func(context.Context, T) (R, error), p Policy) <-chan core.Result[R]
```

[实现](../../internal/sliceops/mapreduce.go#L52) · [行为专题](../mapreduce.md)：Stream 根据 Policy 策略执行流式 Map 操作。

**源码说明与使用片段**（原文）：

Stream 根据 Policy 策略执行流式 Map 操作。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `p` (`Policy`)：执行策略值，包含串行/并行、worker、超时、分片等设置；以返回的新策略继续配置。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(ctx, fn, p)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。异步按完成顺序输出 Result，不携带输入索引，需要关联时在值中携带标识。持续消费至关闭或取消父 context，缓冲满可阻塞回调；Pool 优先，否则使用 Group worker 且不采用 Shards/Chunk/serial。正数 Timeout/FailFast 生效，panic 转为 PanicError；Pool 引用须匹配结果类型，借用池会 ResetWait，接管池最终关闭。

## `*Slice[T, R].Take`

```go
func (s *Slice[T, R]) Take(n int) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L338) · [行为专题](../mapreduce.md)：Take 保留前 n 个元素，返回自身。

**源码说明与使用片段**（原文）：

Take 保留前 n 个元素，返回自身。


**参数说明**：

- `n` (`int`)：保留的前 n 个元素；n<=0 清空当前切片视图，n>=长度无操作。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Take(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：仅修改当前切片视图，不复制或清零被排除元素，底层大数组仍可能保留。Take非正清空、过大无操作；Drop非正无操作、n>=len清空；SliceRange先修正负i/超长j，i>=j清空。清空均使用原视图[:0]并保留起点和容量；Drop有效前缀或非空SliceRange才移动起点并减少容量。不使用端点相加，极大/极小int遵循同样规则；其他视图头不更新，需要缩小保留数组时再Clip。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*Slice[T, R].Values`

```go
func (s *Slice[T, R]) Values() []T
```

[实现](../../internal/sliceops/slice.go#L54) · [行为专题](../mapreduce.md)：Values 返回所有元素的切片副本。

**源码说明与使用片段**（原文）：

Values 返回所有元素的切片副本。


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：复制当前全部元素槽位到新切片，空输入返回可用空切片；指针/map/切片元素的内部数据仍共享。复制不能与输入写入并发，需要独立引用内容时自行深拷贝。

## `*sliceBase[T, S].All`

```go
func (b *sliceBase[T, S]) All(pred func(T) bool) bool
```

[实现](../../internal/sliceops/slice_base.go#L447) · [行为专题](../mapreduce.md)：All 判断是否所有元素都满足 pred。

**源码说明与使用片段**（原文）：

All 判断是否所有元素都满足 pred。 空切片返回 true。元素数 >= 100 且策略为并行时自动启用并行检查。

	pred: 谓词函数。

	sb.All(func(v int) bool { return v%2 == 0 }) // true


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.All(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按策略和输入大小选择同步或并行谓词；Filter/DeleteFunc 在并行且至少 1000 项时分块过滤，查询阈值见链接实现。谓词须并发安全且不 panic，worker panic 无法由调用方 recover。空输入 All=true、Contains/Any=false、Count=0；过滤保留顺序，纯数据操作不使用超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Any`

```go
func (b *sliceBase[T, S]) Any(pred func(T) bool) bool
```

[实现](../../internal/sliceops/slice_base.go#L463) · [行为专题](../mapreduce.md)：Any 判断是否存在至少一个元素满足 pred。

**源码说明与使用片段**（原文）：

Any 判断是否存在至少一个元素满足 pred。 空切片返回 false。元素数 >= 100 且策略为并行时自动启用并行检查。

	pred: 谓词函数。

	sb.Any(func(v int) bool { return v%2 == 0 }) // false


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Any(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按策略和输入大小选择同步或并行谓词；Filter/DeleteFunc 在并行且至少 1000 项时分块过滤，查询阈值见链接实现。谓词须并发安全且不 panic，worker panic 无法由调用方 recover。空输入 All=true、Contains/Any=false、Count=0；过滤保留顺序，纯数据操作不使用超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Append`

```go
func (b *sliceBase[T, S]) Append(vals ...T) S
```

[实现](../../internal/sliceops/slice_base.go#L150) · [行为专题](../mapreduce.md)：Append 在切片末尾追加元素，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Append 在切片末尾追加元素，返回自身以支持链式调用。

	vals: 要追加的元素。

	sb.Append(3, 4).Values() // [1, 2, 3, 4]


**参数说明**：

- `vals` (`...T`)：要追加的元素。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Append(vals...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：更新当前对象切片头并返回自身，元素内部引用不复制；Append/Insert 容量足够时可复用接收者数组。Prepend 以参数 vals 为追加目标，容量足够时采用参数数组并覆盖 vals 长度外的槽位，空参数视图也可能如此。支持同数组重叠参数；Insert 将负索引修正到头部、超过长度修正到尾部。需要保留任一共享视图时先复制对应槽位。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].BinarySearch`

```go
func (b *sliceBase[T, S]) BinarySearch(target T, cmp func(a, b T) int) (int, bool)
```

[实现](../../internal/sliceops/slice_base.go#L532) · [行为专题](../mapreduce.md)：BinarySearch 在已排序（按 cmp 升序）的切片中二分查找 target。

**源码说明与使用片段**（原文）：

BinarySearch 在已排序（按 cmp 升序）的切片中二分查找 target。 返回插入位置及是否找到完全匹配项。

	target: 查找目标值。
	cmp:    比较函数，规则同 Sort。

	idx, found := sb.BinarySearch(2, cmp) // idx=1, found=true


**参数说明**：

- `target` (`T`)：查找目标值。
- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`int, bool`。bool 表示目标是否存在；索引为匹配位置或保持排序的插入位置。

**使用示例**（已初始化参数的调用片段）：

```go
instance.BinarySearch(target, cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Chunked`

```go
func (b *sliceBase[T, S]) Chunked(chunkSize int) [][]T
```

[实现](../../internal/sliceops/slice_base.go#L543) · [行为专题](../mapreduce.md)：Chunked 将切片按 chunkSize 切分为多个子切片，返回 [][]T。

**源码说明与使用片段**（原文）：

Chunked 将切片按 chunkSize 切分为多个子切片，返回 \[]\[]T。 最后一个子切片可能不足 chunkSize。不修改内部切片，但子切片共享输入数组， 保留剩余容量，向一块追加可能覆盖后续块；大小非正或空输入返回 nil。

	chunkSize: 每个子切片的期望大小。

	chunks := sb.Chunked(2) // [[1,2], [3,4], [5]]


**参数说明**：

- `chunkSize` (`int`)：每块最大元素数；非正值或空输入返回 nil，末块可不足此数。子切片共享输入且保留剩余容量，append 可能改写后续块。

**返回值签名**：`[][]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Chunked(chunkSize)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按指定大小返回子切片，末块可不足大小，大小非正或空输入返回 nil。每块共享输入数组且未限制 cap，向一块 append 可能覆盖后续块；按需复制或限制容量，引用元素内部仍共享。分块本身不启动任务，也不启用 Chunk 执行策略。

## `*sliceBase[T, S].ChunkedN`

```go
func (b *sliceBase[T, S]) ChunkedN(n int) [][]T
```

[实现](../../internal/sliceops/slice_base.go#L554) · [行为专题](../mapreduce.md)：ChunkedN 按 ceil(len/n) 的块大小切分，返回最多 n 个子切片。

**源码说明与使用片段**（原文）：

ChunkedN 按 ceil(len/n) 的块大小切分，返回最多 n 个子切片。 实际块数可能少于 n（例如 10 个元素、n=6 时返回 5 块）；n\<=0 或空输入返回 nil。 不修改内部切片，但返回的子切片共享原底层数组。

	n: 期望的子切片数量。

	chunks := sb.ChunkedN(2) // [[1,2,3], [4,5]]


**参数说明**：

- `n` (`int`)：用于计算子切片长度 ceil(len/n) 的目标份数；非正值或空输入返回 nil。实际份数可能小于 n，子切片共享输入底层数组。

**返回值签名**：`[][]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ChunkedN(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按 ceil(len/n) 计算固定块长再切分，实际可能少于 n 块；n<=0 或空输入返回 nil。每块共享输入数组且未限制 cap，向一块 append 可能覆盖后续块；按需复制或限制容量，引用元素内部仍共享。与 Chunk 策略配置的逐元素调度不同。

## `*sliceBase[T, S].Clip`

```go
func (b *sliceBase[T, S]) Clip() S
```

[实现](../../internal/sliceops/slice_base.go#L126) · [行为专题](../mapreduce.md)：Clip 在容量大于长度时复制到新数组，使 cap == len，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Clip 在容量大于长度时复制到新数组，使 cap == len，返回自身以支持链式调用。 旧数组只有在所有引用释放后才能回收，复制会有瞬时内存开销。

	sb.Filter(pred).Clip() // 过滤后释放多余容量


**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Clip()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：仅在 cap>len 时复制到 cap==len 的新数组，否则保持原引用；是浅拷贝，不清理其他视图。复制有瞬时内存成本，只有所有旧引用释放后原数组才可回收；不提供深拷贝或并发快照。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Clone`

```go
func (b *sliceBase[T, S]) Clone() []T
```

[实现](../../internal/sliceops/slice_base.go#L381) · [行为专题](../mapreduce.md)：Clone 返回切片元素的浅拷贝，等价于 Values()。

**源码说明与使用片段**（原文）：

Clone 返回切片元素的浅拷贝，等价于 Values()。


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Clone()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：复制当前全部元素槽位到新切片，空输入返回可用空切片；指针/map/切片元素的内部数据仍共享。复制不能与输入写入并发，需要独立引用内容时自行深拷贝。

## `*sliceBase[T, S].Compact`

```go
func (b *sliceBase[T, S]) Compact(eq func(a, b T) bool) S
```

[实现](../../internal/sliceops/slice_base.go#L95) · [行为专题](../mapreduce.md)：Compact 移除相邻的重复元素，使用 eq 判断是否相等，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Compact 移除相邻的重复元素，使用 eq 判断是否相等，返回自身以支持链式调用。 注意：仅移除相邻重复，如需全局去重请使用 Dedup。

	eq: 相等判断函数，返回 true 表示两元素相等。

	sb.Compact(func(a, b int) bool { return a == b }).Values() // [1, 2, 3]


**参数说明**：

- `eq` (`func(a, b T) bool`)：相等比较函数；返回 true 表示两元素相等，应满足自反、对称与传递性。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Compact(eq)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只去除相邻相等项，保留各段首次代表与顺序；eq 在长度至少 2 时必须非 nil，panic 直接传播，不自动排序。原地压缩但不清零缩短后的尾部，含引用时可能继续保留对象；全局去重用 Dedup。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Contains`

```go
func (b *sliceBase[T, S]) Contains(pred func(T) bool) bool
```

[实现](../../internal/sliceops/slice_base.go#L389) · [行为专题](../mapreduce.md)：Contains 判断是否存在满足 pred 的元素，存在返回 true。

**源码说明与使用片段**（原文）：

Contains 判断是否存在满足 pred 的元素，存在返回 true。 元素数 >= 100 且策略为并行时自动启用并行搜索（提前退出）。

	pred: 谓词函数，返回 true 表示匹配。

	sb.Contains(func(v int) bool { return v > 2 }) // true


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Contains(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按策略和输入大小选择同步或并行谓词；Filter/DeleteFunc 在并行且至少 1000 项时分块过滤，查询阈值见链接实现。谓词须并发安全且不 panic，worker panic 无法由调用方 recover。空输入 All=true、Contains/Any=false、Count=0；过滤保留顺序，纯数据操作不使用超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Count`

```go
func (b *sliceBase[T, S]) Count(pred func(T) bool) int
```

[实现](../../internal/sliceops/slice_base.go#L479) · [行为专题](../mapreduce.md)：Count 统计满足 pred 的元素数量。

**源码说明与使用片段**（原文）：

Count 统计满足 pred 的元素数量。 元素数 >= 100 且策略为并行时自动启用并行计数。

	pred: 谓词函数。

	sb.Count(func(v int) bool { return v%2 == 0 }) // 2


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Count(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按策略和输入大小选择同步或并行谓词；Filter/DeleteFunc 在并行且至少 1000 项时分块过滤，查询阈值见链接实现。谓词须并发安全且不 panic，worker panic 无法由调用方 recover。空输入 All=true、Contains/Any=false、Count=0；过滤保留顺序，纯数据操作不使用超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Dedup`

```go
func (b *sliceBase[T, S]) Dedup(eq func(a, b T) bool) S
```

[实现](../../internal/sliceops/slice_base.go#L117) · [行为专题](../mapreduce.md)：Dedup 按 eq 全局去重，保留每组首次出现的元素，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Dedup 按 eq 全局去重，保留每组首次出现的元素，返回自身以支持链式调用。 非 nil eq 支持任意元素类型，最坏 O(n²) 次比较；大数据量建议先按业务键建立索引。 eq 为 nil 时兼容 Go 相等性判重，元素必须可作为 map key，否则会 panic。

	eq: 相等判断函数，返回 true 表示两元素相等。

	sb.Dedup(func(a, b int) bool { return a == b }).Values() // [3, 1, 2]


**参数说明**：

- `eq` (`func(a, b T) bool`)：相等比较函数，返回 true 表示属于同一类；保留首次代表值。非 nil 时支持切片等非可比较元素，最坏 O(n²)；nil 使用 Go 相等性且非可比较值可能 panic。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Dedup(eq)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按 eq(已保留代表,当前项) 全局去重，保留首次代表及输入顺序，清零移除尾部；自定义 eq 最坏 O(n²)，须构成等价关系且 panic 直接传播。nil eq 使用 Go map 相等性，动态值不可比较时 panic，NaN 按 Go 相等性不相等；大输入按业务键建索引。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Delete`

```go
func (b *sliceBase[T, S]) Delete(i int) S
```

[实现](../../internal/sliceops/slice_base.go#L191) · [行为专题](../mapreduce.md)：Delete 删除索引 i 处的单个元素，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Delete 删除索引 i 处的单个元素，返回自身以支持链式调用。 i 越界时无操作。

	i: 要删除的元素索引。

	sb.Delete(1).Values() // [1, 3]


**参数说明**：

- `i` (`int`)：待删除元素的零起始索引；越界无操作。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Delete(i)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地移动后续元素并缩短当前视图，不清零移除后的尾部，引用元素可能继续占用内存。Delete 越界无操作；DeleteRange 将负 i 修为 0、超长 j 修为长度，修正后 i>=j 无操作。其他共享视图长度不随之更新。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].DeleteFunc`

```go
func (b *sliceBase[T, S]) DeleteFunc(pred func(T) bool) S
```

[实现](../../internal/sliceops/slice_base.go#L85) · [行为专题](../mapreduce.md)：DeleteFunc 使用 pred 谓词删除切片元素，删除 pred 返回 true 的元素，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

DeleteFunc 使用 pred 谓词删除切片元素，删除 pred 返回 true 的元素，返回自身以支持链式调用。 与 Filter 语义相反：Filter 保留满足条件的，DeleteFunc 删除满足条件的。

	pred: 谓词函数，返回 true 删除该元素。

	sb.DeleteFunc(func(v int) bool { return v%2 == 0 }).Values() // [1, 3]


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DeleteFunc(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：稳定过滤，正常返回时每元素调用一次谓词；并行策略且当前长度至少1000时由worker调用并分配结果槽位，即便Worker(1)亦如此，原输入槽位不变。其他路径原地压缩，不清零旧尾部；旧视图长度不随结果缩短。槽位复制不复制引用内容，保留输入须过滤前复制，之后Values不能撤销已发生的改写。不检查context或应用Timeout/FailFast，不恢复panic；同步panic保留原切片长度及已发生的部分写入，worker panic无法由调用方recover捕获并导致进程退出。谓词须并发安全且不panic。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].DeleteRange`

```go
func (b *sliceBase[T, S]) DeleteRange(i, j int) S
```

[实现](../../internal/sliceops/slice_base.go#L206) · [行为专题](../mapreduce.md)：DeleteRange 删除索引范围 [i, j) 的元素，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

DeleteRange 删除索引范围 \[i, j) 的元素，返回自身以支持链式调用。 i \< 0 修正为 0，j > len 修正为 len。

	i: 起始索引（含）。
	j: 结束索引（不含）。

	sb.DeleteRange(1, 3).Values() // [1, 4]


**参数说明**：

- `i` (`int`)：左闭区间起点；负值修正为 0。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。
- `j` (`int`)：右开区间终点；超出长度修正为长度。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DeleteRange(i, j)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地移动后续元素并缩短当前视图，不清零移除后的尾部，引用元素可能继续占用内存。Delete 越界无操作；DeleteRange 将负 i 修为 0、超长 j 修为长度，修正后 i>=j 无操作。其他共享视图长度不随之更新。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Drop`

```go
func (b *sliceBase[T, S]) Drop(n int) S
```

[实现](../../internal/sliceops/slice_base.go#L255) · [行为专题](../mapreduce.md)：Drop 丢弃前 n 个元素，保留其余，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Drop 丢弃前 n 个元素，保留其余，返回自身以支持链式调用。 n \<= 0 时无操作，n >= len 时清空切片。

	n: 要丢弃的元素数量。

	sb.Drop(2).Values() // [3, 4]


**参数说明**：

- `n` (`int`)：丢弃的前 n 个元素；n<=0 无操作，n>=长度清空当前切片视图。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Drop(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：仅修改当前切片视图，不复制或清零被排除元素，底层大数组仍可能保留。Take非正清空、过大无操作；Drop非正无操作、n>=len清空；SliceRange先修正负i/超长j，i>=j清空。清空均使用原视图[:0]并保留起点和容量；Drop有效前缀或非空SliceRange才移动起点并减少容量。不使用端点相加，极大/极小int遵循同样规则；其他视图头不更新，需要缩小保留数组时再Clip。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Filter`

```go
func (b *sliceBase[T, S]) Filter(pred func(T) bool) S
```

[实现](../../internal/sliceops/slice_base.go#L74) · [行为专题](../mapreduce.md)：Filter 使用 pred 谓词过滤切片，保留 pred 返回 true 的元素，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Filter 使用 pred 谓词过滤切片，保留 pred 返回 true 的元素，返回自身以支持链式调用。 元素数 >= 1000 且执行策略为并行时启用并行过滤；串行策略始终同步调用谓词。 并行过滤另分配结果槽位，其他路径原地压缩且不清零旧尾部。 同步谓词 panic 原样传播，切片长度未更新但前部可能已改写；worker panic 会终止进程。

	pred: 谓词函数，返回 true 保留该元素，false 丢弃。

	sb.Filter(func(v int) bool { return v%2 == 0 }).Values() // [2, 4]


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Filter(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：稳定过滤，正常返回时每元素调用一次谓词；并行策略且当前长度至少1000时由worker调用并分配结果槽位，即便Worker(1)亦如此，原输入槽位不变。其他路径原地压缩，不清零旧尾部；旧视图长度不随结果缩短。槽位复制不复制引用内容，保留输入须过滤前复制，之后Values不能撤销已发生的改写。不检查context或应用Timeout/FailFast，不恢复panic；同步panic保留原切片长度及已发生的部分写入，worker panic无法由调用方recover捕获并导致进程退出。谓词须并发安全且不panic。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Find`

```go
func (b *sliceBase[T, S]) Find(pred func(T) bool) (val T, found bool)
```

[实现](../../internal/sliceops/slice_base.go#L418) · [行为专题](../mapreduce.md)：Find 返回第一个满足 pred 的元素及是否找到。

**源码说明与使用片段**（原文）：

Find 返回第一个满足 pred 的元素及是否找到。

	pred: 谓词函数。

	val, ok := sb.Find(func(v int) bool { return v > 1 }) // val=2, ok=true


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`val T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Find(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步查找并在首次符合所需方向的匹配处终止，FindLast 从尾部扫描；Index 未匹配返回 -1，Find/FindLast 未匹配返回零值和 false。回调须非 nil，panic 直接传播；不采用 worker/超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].FindLast`

```go
func (b *sliceBase[T, S]) FindLast(pred func(T) bool) (val T, found bool)
```

[实现](../../internal/sliceops/slice_base.go#L432) · [行为专题](../mapreduce.md)：FindLast 返回最后一个满足 pred 的元素及是否找到。

**源码说明与使用片段**（原文）：

FindLast 返回最后一个满足 pred 的元素及是否找到。

	pred: 谓词函数。

	val, ok := sb.FindLast(func(v int) bool { return v == 2 }) // val=2(索引3), ok=true


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`val T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FindLast(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步查找并在首次符合所需方向的匹配处终止，FindLast 从尾部扫描；Index 未匹配返回 -1，Find/FindLast 未匹配返回零值和 false。回调须非 nil，panic 直接传播；不采用 worker/超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].First`

```go
func (b *sliceBase[T, S]) First() (val T, found bool)
```

[实现](../../internal/sliceops/slice_base.go#L344) · [行为专题](../mapreduce.md)：First 返回第一个元素及是否存在的标志。

**源码说明与使用片段**（原文）：

First 返回第一个元素及是否存在的标志。

	返回: val 第一个元素，found 是否存在。

	val, ok := sb.First() // val=1, ok=true


**参数说明**：无显式参数。

**返回值签名**：`val T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.First()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Grow`

```go
func (b *sliceBase[T, S]) Grow(n int) S
```

[实现](../../internal/sliceops/slice_base.go#L140) · [行为专题](../mapreduce.md)：Grow 确保容量至少可容纳 len+n 个元素（不改变 len），返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Grow 确保容量至少可容纳 len+n 个元素（不改变 len），返回自身以支持链式调用。

	n: 需要额外容纳的元素数；负数会 panic。

	sb.Grow(100).Append(v1, v2) // 预先扩容，避免 Append 多次分配


**参数说明**：

- `n` (`int`)：额外容量需求；负值 panic，非负值追加零值再恢复长度，可能改写共享数组长度外槽位。须控制 len+n 和分配预算。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Grow(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：负 n 在 make([]T,n) 时 panic；非负值通过追加 n 个零值再恢复长度确保容量，可能清零原数组长度外的槽位。与旧 Slice.Grow 的负值无操作不同，调用前校验 n，控制分配预算；不改变长度或保证精确容量。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Index`

```go
func (b *sliceBase[T, S]) Index(pred func(T) bool) int
```

[实现](../../internal/sliceops/slice_base.go#L404) · [行为专题](../mapreduce.md)：Index 返回第一个满足 pred 的元素索引，未找到返回 -1。

**源码说明与使用片段**（原文）：

Index 返回第一个满足 pred 的元素索引，未找到返回 -1。

	pred: 谓词函数。

	idx := sb.Index(func(v int) bool { return v == 2 }) // 1


**参数说明**：

- `pred` (`func(T) bool`)：元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Index(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步查找并在首次符合所需方向的匹配处终止，FindLast 从尾部扫描；Index 未匹配返回 -1，Find/FindLast 未匹配返回零值和 false。回调须非 nil，panic 直接传播；不采用 worker/超时/FailFast。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Insert`

```go
func (b *sliceBase[T, S]) Insert(idx int, vals ...T) S
```

[实现](../../internal/sliceops/slice_base.go#L173) · [行为专题](../mapreduce.md)：Insert 在指定位置 idx 插入元素，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Insert 在指定位置 idx 插入元素，返回自身以支持链式调用。 idx 越界时自动修正：\<0 修正为 0，>= len 修正为 len（即末尾追加）。

	idx:  插入位置索引。
	vals: 要插入的元素。

	sb.Insert(1, 2).Values() // [1, 2, 3]


**参数说明**：

- `idx` (`int`)：零起始插入位置；负数插到头部，大于长度插到末尾。
- `vals` (`...T`)：要插入的元素。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Insert(idx, vals...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：更新当前对象切片头并返回自身，元素内部引用不复制；Append/Insert 容量足够时可复用接收者数组。Prepend 以参数 vals 为追加目标，容量足够时采用参数数组并覆盖 vals 长度外的槽位，空参数视图也可能如此。支持同数组重叠参数；Insert 将负索引修正到头部、超过长度修正到尾部。需要保留任一共享视图时先复制对应槽位。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].IsEmpty`

```go
func (b *sliceBase[T, S]) IsEmpty() bool
```

[实现](../../internal/sliceops/slice_base.go#L335) · [行为专题](../mapreduce.md)：IsEmpty 切片为空返回 true。

**源码说明与使用片段**（原文）：

IsEmpty 切片为空返回 true。

	sb.IsEmpty() // true


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsEmpty()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].IsSorted`

```go
func (b *sliceBase[T, S]) IsSorted(cmp func(a, b T) int) bool
```

[实现](../../internal/sliceops/slice_base.go#L50) · [行为专题](../mapreduce.md)：IsSorted 判断切片是否已按 cmp 升序排列。

**源码说明与使用片段**（原文）：

IsSorted 判断切片是否已按 cmp 升序排列。

	cmp: 比较函数，规则同 Sort。

	sb.IsSorted(cmp.Compare[int]) // import "cmp"；先确认输入确实有序


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsSorted(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Items`

```go
func (b *sliceBase[T, S]) Items() []T
```

[实现](../../internal/sliceops/slice_base.go#L367) · [行为专题](../mapreduce.md)：Items 返回内部切片的直接引用（非拷贝），修改返回值会影响构建器内部状态。

**源码说明与使用片段**（原文）：

Items 返回内部切片的直接引用（非拷贝），修改返回值会影响构建器内部状态。 如需安全拷贝请使用 Values()。

	items := sb.Filter(pred).Items() // 直接引用，慎改


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Items()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回内部切片引用，元素写入立即影响当前对象；返回的切片头是副本，对它 append 不会自动更新对象长度。后续对象重新分配/截取不会更新旧视图，需独立元素槽位时用 Values/Clone。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Last`

```go
func (b *sliceBase[T, S]) Last() (val T, found bool)
```

[实现](../../internal/sliceops/slice_base.go#L356) · [行为专题](../mapreduce.md)：Last 返回最后一个元素及是否存在的标志。

**源码说明与使用片段**（原文）：

Last 返回最后一个元素及是否存在的标志。

	返回: val 最后一个元素，found 是否存在。

	val, ok := sb.Last() // val=3, ok=true


**参数说明**：无显式参数。

**返回值签名**：`val T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Last()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Len`

```go
func (b *sliceBase[T, S]) Len() int
```

[实现](../../internal/sliceops/slice_base.go#L328) · [行为专题](../mapreduce.md)：Len 返回当前切片元素数量。

**源码说明与使用片段**（原文）：

Len 返回当前切片元素数量。

	sb.Len() // 3


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Len()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Max`

```go
func (b *sliceBase[T, S]) Max(cmp func(a, b T) int) (max T, found bool)
```

[实现](../../internal/sliceops/slice_base.go#L494) · [行为专题](../mapreduce.md)：Max 使用 cmp 比较函数找出最大元素。

**源码说明与使用片段**（原文）：

Max 使用 cmp 比较函数找出最大元素。

	cmp: 比较函数，规则同 Sort。

	max, ok := sb.Max(cmp.Compare[int]) // import "cmp"；max=3, ok=true


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`max T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Max(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Min`

```go
func (b *sliceBase[T, S]) Min(cmp func(a, b T) int) (min T, found bool)
```

[实现](../../internal/sliceops/slice_base.go#L512) · [行为专题](../mapreduce.md)：Min 使用 cmp 比较函数找出最小元素。

**源码说明与使用片段**（原文）：

Min 使用 cmp 比较函数找出最小元素。

	cmp: 比较函数，规则同 Sort。

	min, ok := sb.Min(cmp.Compare[int]) // import "cmp"；min=1, ok=true


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`min T, found bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Min(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Prepend`

```go
func (b *sliceBase[T, S]) Prepend(vals ...T) S
```

[实现](../../internal/sliceops/slice_base.go#L161) · [行为专题](../mapreduce.md)：Prepend 在切片头部插入元素，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Prepend 在切片头部插入元素，返回自身以支持链式调用。 容量足够时复用 vals 的数组，可能覆盖 vals 长度之外的共享槽位。

	vals: 要插入的元素，按顺序出现在切片头部。

	sb.Prepend(1, 2).Values() // [1, 2, 3, 4]


**参数说明**：

- `vals` (`...T`)：要插入的元素，按顺序出现在切片头部。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Prepend(vals...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：更新当前对象切片头并返回自身，元素内部引用不复制；Append/Insert 容量足够时可复用接收者数组。Prepend 以参数 vals 为追加目标，容量足够时采用参数数组并覆盖 vals 长度外的槽位，空参数视图也可能如此。支持同数组重叠参数；Insert 将负索引修正到头部、超过长度修正到尾部。需要保留任一共享视图时先复制对应槽位。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Repeat`

```go
func (b *sliceBase[T, S]) Repeat(n int) S
```

[实现](../../internal/sliceops/slice_base.go#L307) · [行为专题](../mapreduce.md)：Repeat 将现有切片重复 n 次拼接，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Repeat 将现有切片重复 n 次拼接，返回自身以支持链式调用。 n \<= 0 时清空切片，n == 1 时无操作。 空输入直接返回，不按 n 循环；保留现有空视图和容量。

	n: 重复次数。

	sb.Repeat(3).Values() // [1, 2, 1, 2, 1, 2]


**参数说明**：

- `n` (`int`)：原切片的总重复次数；非正值清空，1 无操作，更大值反复浅追加。调用前校验 len×n 的数量和内存预算，元素内部引用不复制。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Repeat(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：n<=0 清空当前视图，n==1 无操作；空输入直接返回并保留空视图及容量，不按 n 循环。非空且 n>1 时复制原元素槽位后重复追加，容量足够可复用数组，引用类型内容并不隔离。总元素数约 len×n，须预先校验数量/内存预算；此方法没有上限、取消或分配失败恢复。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Replace`

```go
func (b *sliceBase[T, S]) Replace(i, j int, vals ...T) S
```

[实现](../../internal/sliceops/slice_base.go#L229) · [行为专题](../mapreduce.md)：Replace 替换索引范围 [i, j) 的元素为新值 vals，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Replace 替换索引范围 \[i, j) 的元素为新值 vals，返回自身以支持链式调用。 支持重叠参数；缩短时清零新长度到旧长度之间的槽位，共享视图可见。

	i:    起始索引（含）。
	j:    结束索引（不含）。
	vals: 替换后的新元素。

	sb.Replace(1, 2, 99).Values() // [1, 99, 3]


**参数说明**：

- `i` (`int`)：替换区间的起点（含）；先将负 i 修正为 0，再将大于修正后 j 的 i 降为 j。修正后仍非法的边界会 panic。
- `j` (`int`)：替换区间的终点（不含）；大于长度时修正为长度，负数不修正并会 panic。
- `vals` (`...T`)：替换后的新元素。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Replace(i, j, vals...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：先修正负 i 为 0、超长 j 为长度，再把 i>j 修为 j；负 j 仍会 panic，调用前校验边界。随后用 slices.Replace 替换 [i,j)，支持重叠参数，可变更长度和分配数组；缩短时清零新长度到旧长度之间的槽位，共享旧视图会观察到清零。元素引用仍共享，修改不回滚。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Reverse`

```go
func (b *sliceBase[T, S]) Reverse() S
```

[实现](../../internal/sliceops/slice_base.go#L59) · [行为专题](../mapreduce.md)：Reverse 原地反转切片元素顺序，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Reverse 原地反转切片元素顺序，返回自身以支持链式调用。

	sb.Reverse().Values() // [3, 2, 1]


**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reverse()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地调整当前视图的元素顺序并返回自身，空/单元素无变化；Shuffle 可能保持原序或重复上次结果，使用全局伪随机源，不提供安全随机或独立种子保证。共享视图会看到修改，不应用执行策略。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Shuffle`

```go
func (b *sliceBase[T, S]) Shuffle() S
```

[实现](../../internal/sliceops/slice_base.go#L293) · [行为专题](../mapreduce.md)：Shuffle 随机打乱切片元素顺序，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Shuffle 随机打乱切片元素顺序，返回自身以支持链式调用。

	sb.Shuffle() // 随机排列；可能与原顺序或上次结果相同


**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shuffle()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地调整当前视图的元素顺序并返回自身，空/单元素无变化；Shuffle 可能保持原序或重复上次结果，使用全局伪随机源，不提供安全随机或独立种子保证。共享视图会看到修改，不应用执行策略。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].SliceRange`

```go
func (b *sliceBase[T, S]) SliceRange(i, j int) S
```

[实现](../../internal/sliceops/slice_base.go#L274) · [行为专题](../mapreduce.md)：SliceRange 截取 [i, j) 范围的子切片，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

SliceRange 截取 \[i, j) 范围的子切片，返回自身以支持链式调用。 i \< 0 修正为 0，j > len 修正为 len。

	i: 起始索引（含）。
	j: 结束索引（不含）。

	sb.SliceRange(1, 3).Values() // [2, 3]


**参数说明**：

- `i` (`int`)：左闭区间起点；负值修正为 0。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。
- `j` (`int`)：右开区间终点；超出长度修正为长度。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SliceRange(i, j)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：仅修改当前切片视图，不复制或清零被排除元素，底层大数组仍可能保留。Take非正清空、过大无操作；Drop非正无操作、n>=len清空；SliceRange先修正负i/超长j，i>=j清空。清空均使用原视图[:0]并保留起点和容量；Drop有效前缀或非空SliceRange才移动起点并减少容量。不使用端点相加，极大/极小int遵循同样规则；其他视图头不更新，需要缩小保留数组时再Clip。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Sort`

```go
func (b *sliceBase[T, S]) Sort(cmp func(a, b T) int) S
```

[实现](../../internal/sliceops/slice_base.go#L30) · [行为专题](../mapreduce.md)：Sort 使用 cmp 比较函数对切片进行非稳定排序，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Sort 使用 cmp 比较函数对切片进行非稳定排序，返回自身以支持链式调用。

	cmp: 比较函数，返回 <0 表示 a 排在 b 前，>0 表示 b 排在 a 前，==0 表示相等。

	sb.Sort(cmp.Compare[int]) // import "cmp"；避免整数相减溢出


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Sort(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Split`

```go
func (b *sliceBase[T, S]) Split(pred func(T) bool) (matched []T, unmatched []T)
```

[实现](../../internal/sliceops/slice_base.go#L565) · [行为专题](../mapreduce.md)：Split 将切片按 pred 拆分为两组：matched（满足 pred）和 unmatched（不满足 pred）。

**源码说明与使用片段**（原文）：

Split 将切片按 pred 拆分为两组：matched（满足 pred）和 unmatched（不满足 pred）。 不修改内部切片。

	pred: 谓词函数。

	m, u := sb.Split(func(v int) bool { return v%2 == 0 })
	// m = [2, 4], u = [1, 3]


**参数说明**：

- `pred` (`func(T) bool`)：同步按输入顺序判断元素；true 放入 matched，false 放入 unmatched。返回两个新切片但元素内部引用仍共享，panic 直接传播，不修改输入切片。

**返回值签名**：`matched []T, unmatched []T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Split(pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步按输入顺序调用谓词，true/false 分别进入两个新切片，原输入不改变；空输入两者为 nil。元素引用仍共享，谓词须非 nil 且 panic 直接传播；不应用并发策略，不与共享输入写入竞争。

## `*sliceBase[T, S].StableSort`

```go
func (b *sliceBase[T, S]) StableSort(cmp func(a, b T) int) S
```

[实现](../../internal/sliceops/slice_base.go#L40) · [行为专题](../mapreduce.md)：StableSort 使用 cmp 比较函数对切片进行稳定排序，相等元素保持原有相对顺序。

**源码说明与使用片段**（原文）：

StableSort 使用 cmp 比较函数对切片进行稳定排序，相等元素保持原有相对顺序。

	cmp: 比较函数，规则同 Sort。

	sb.StableSort(func(a, b MyStruct) int { return cmp.Compare(a.Priority, b.Priority) }) // import "cmp"


**参数说明**：

- `cmp` (`func(a, b T) int`)：三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.StableSort(cmp)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Take`

```go
func (b *sliceBase[T, S]) Take(n int) S
```

[实现](../../internal/sliceops/slice_base.go#L240) · [行为专题](../mapreduce.md)：Take 保留前 n 个元素，丢弃其余，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Take 保留前 n 个元素，丢弃其余，返回自身以支持链式调用。 n \<= 0 时清空切片，n >= len 时无操作。

	n: 要保留的元素数量。

	sb.Take(2).Values() // [1, 2]


**参数说明**：

- `n` (`int`)：保留的前 n 个元素；n<=0 清空当前切片视图，n>=长度无操作。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Take(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：仅修改当前切片视图，不复制或清零被排除元素，底层大数组仍可能保留。Take非正清空、过大无操作；Drop非正无操作、n>=len清空；SliceRange先修正负i/超长j，i>=j清空。清空均使用原视图[:0]并保留起点和容量；Drop有效前缀或非空SliceRange才移动起点并减少容量。不使用端点相加，极大/极小int遵循同样规则；其他视图头不更新，需要缩小保留数组时再Clip。不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。

## `*sliceBase[T, S].Values`

```go
func (b *sliceBase[T, S]) Values() []T
```

[实现](../../internal/sliceops/slice_base.go#L374) · [行为专题](../mapreduce.md)：Values 返回切片元素的浅拷贝；替换返回切片的元素不影响构建器，元素内部引用仍可能共享。

**源码说明与使用片段**（原文）：

Values 返回切片元素的浅拷贝；替换返回切片的元素不影响构建器，元素内部引用仍可能共享。

	vals := sb.Sort(cmp).Values() // 可独立替换元素，引用类型内容仍需同步


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：复制当前全部元素槽位到新切片，空输入返回可用空切片；指针/map/切片元素的内部数据仍共享。复制不能与输入写入并发，需要独立引用内容时自行深拷贝。

## `AnyError`

```go
func AnyError[T any](results []core.Result[T]) bool
```

[实现](../../internal/sliceops/extended_impl.go#L344) · [行为专题](../mapreduce.md)：AnyError 检查是否有非 nil Err；不检查 Occupied，空切片返回 false。

**源码说明与使用片段**（原文）：

AnyError 检查是否有非 nil Err；不检查 Occupied，空切片返回 false。

参数：

  - results：Result 切片


**参数说明**：

- `results` (`[]core.Result[T]`)：Result 切片

**返回值签名**：`bool`。true 表示观察到了错误，使用 Error/Errors 获取错误值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.AnyError[T](results)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只查看输入槽位的非 nil Err，不检查 Occupied，也不补充汇总错误；空集合没有错误。错误列表按输入顺序，返回新切片且 error 对象仍共享；结果截断或空槽时不能据此证明所有业务均已成功。

## `Chunk`

```go
func Chunk[T any](items []T, chunkSize int) [][]T
```

[实现](../../internal/sliceops/exec_impl.go#L1086) · [行为专题](../mapreduce.md)：Chunk 按指定大小切分，末块可不足大小；大小非正或空输入返回 nil。

**源码说明与使用片段**（原文）：

Chunk 按指定大小切分，末块可不足大小；大小非正或空输入返回 nil。 子切片共享输入数组并保留剩余容量，向一块追加可能覆盖后续块。


**参数说明**：

- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `chunkSize` (`int`)：每块最大元素数；非正值或空输入返回 nil，末块可不足此数。子切片共享输入且保留剩余容量，append 可能改写后续块。

**返回值签名**：`[][]T`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Chunk[T](items, chunkSize)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按指定大小返回子切片，末块可不足大小，大小非正或空输入返回 nil。每块共享输入数组且未限制 cap，向一块 append 可能覆盖后续块；按需复制或限制容量，引用元素内部仍共享。分块本身不启动任务，也不启用 Chunk 执行策略。

## `ChunkN`

```go
func ChunkN[T any](items []T, n int) [][]T
```

[实现](../../internal/sliceops/exec_impl.go#L1110) · [行为专题](../mapreduce.md)：ChunkN 按 ceil(len(items)/n) 的块大小切分，返回最多 n 块，实际块数可能少于 n。

**源码说明与使用片段**（原文）：

ChunkN 按 ceil(len(items)/n) 的块大小切分，返回最多 n 块，实际块数可能少于 n。 n\<=0 或空输入返回 nil；返回的子切片共享 items 的底层数组。


**参数说明**：

- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `n` (`int`)：用于计算子切片长度 ceil(len/n) 的目标份数；非正值或空输入返回 nil。实际份数可能小于 n，子切片共享输入底层数组。

**返回值签名**：`[][]T`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ChunkN[T](items, n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按 ceil(len/n) 计算固定块长再切分，实际可能少于 n 块；n<=0 或空输入返回 nil。每块共享输入数组且未限制 cap，向一块 append 可能覆盖后续块；按需复制或限制容量，引用元素内部仍共享。与 Chunk 策略配置的逐元素调度不同。

## `DefPar`

```go
func DefPar() Policy
```

[实现](../../internal/sliceops/policy.go#L58) · [行为专题](../mapreduce.md)：DefPar 使用默认 IO 并发度的并行策略便捷方法。

**源码说明与使用片段**（原文）：

DefPar 使用默认 IO 并发度的并行策略便捷方法。


**参数说明**：无显式参数。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefPar()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回独立策略值，不创建 worker；Seq 标记串行并保存并发 1，Par 非正并发使用 IO()，DefPar 显式使用调用时 IO()。其他字段初始为零，未主动调用 Default* 不代表超时 30s/分块 100 已启用。必须接收链式设置返回值，池引用依然共享。

## `DefaultForEach`

```go
func DefaultForEach[T any](ctx context.Context, items []T, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L176) · [行为专题](../mapreduce.md)：DefaultForEach 使用默认并发度的 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEach 使用默认并发度的 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - fn：处理函数，只返回 error

使用示例：

	total, fail, firstErr, _ := sliceops.DefaultForEach(ctx, msgs, func(ctx context.Context, msg string) error {
	    return send(ctx, msg)
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEach[T](ctx, items, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `DefaultForEachChunk`

```go
func DefaultForEachChunk[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L666) · [行为专题](../mapreduce.md)：DefaultForEachChunk 使用默认并发度的分块 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachChunk 使用默认并发度的分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, chunk []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachChunk[T](ctx, items, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `DefaultForEachChunkWithFFTimeout`

```go
func DefaultForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L748) · [行为专题](../mapreduce.md)：DefaultForEachChunkWithFFTimeout 使用默认并发度的 FailFast + 超时分块 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachChunkWithFFTimeout 使用默认并发度的 FailFast + 超时分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, chunk []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachChunkWithFFTimeout[T](ctx, items, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `DefaultForEachChunkWithFailFast`

```go
func DefaultForEachChunkWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L692) · [行为专题](../mapreduce.md)：DefaultForEachChunkWithFailFast 使用默认并发度的 FailFast 分块 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachChunkWithFailFast 使用默认并发度的 FailFast 分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, chunk []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachChunkWithFailFast[T](ctx, items, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `DefaultForEachChunkWithTimeout`

```go
func DefaultForEachChunkWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L720) · [行为专题](../mapreduce.md)：DefaultForEachChunkWithTimeout 使用默认并发度的带超时分块 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachChunkWithTimeout 使用默认并发度的带超时分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, chunk []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachChunkWithTimeout[T](ctx, items, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `DefaultForEachChunked`

```go
func DefaultForEachChunked[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L780) · [行为专题](../mapreduce.md)：DefaultForEachChunked 使用默认并发度的逐元素分块 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachChunked 使用默认并发度的逐元素分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachChunked[T](ctx, items, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。

## `DefaultForEachChunkedWithFFTimeout`

```go
func DefaultForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L867) · [行为专题](../mapreduce.md)：DefaultForEachChunkedWithFFTimeout 使用默认并发度的 FailFast + 超时逐元素分块 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachChunkedWithFFTimeout 使用默认并发度的 FailFast + 超时逐元素分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：每个批次共享的 context 超时；从外层批次任务开始执行计时，包含批内调度及全部元素执行。正数生效，非正值清除外层期限，批内 Group 仍可能继承全局默认超时；不是每个元素重新计时，回调须响应 context。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachChunkedWithFFTimeout[T](ctx, items, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。

## `DefaultForEachChunkedWithFailFast`

```go
func DefaultForEachChunkedWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L807) · [行为专题](../mapreduce.md)：DefaultForEachChunkedWithFailFast 使用默认并发度的 FailFast 逐元素分块 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachChunkedWithFailFast 使用默认并发度的 FailFast 逐元素分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachChunkedWithFailFast[T](ctx, items, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。

## `DefaultForEachChunkedWithTimeout`

```go
func DefaultForEachChunkedWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L837) · [行为专题](../mapreduce.md)：DefaultForEachChunkedWithTimeout 使用默认并发度的带超时逐元素分块 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachChunkedWithTimeout 使用默认并发度的带超时逐元素分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：每个批次共享的 context 超时；从外层批次任务开始执行计时，包含批内调度及全部元素执行。正数生效，非正值清除外层期限，批内 Group 仍可能继承全局默认超时；不是每个元素重新计时，回调须响应 context。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachChunkedWithTimeout[T](ctx, items, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。

## `DefaultForEachWithFFTimeout`

```go
func DefaultForEachWithFFTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L292) · [行为专题](../mapreduce.md)：DefaultForEachWithFFTimeout 使用默认并发度的 FailFast + 超时 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachWithFFTimeout 使用默认并发度的 FailFast + 超时 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - timeout：单任务超时时间
  - fn：处理函数，只返回 error

使用示例：

	total, fail, firstErr, _ := sliceops.DefaultForEachWithFFTimeout(ctx, tasks, 3*time.Second, fn)


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachWithFFTimeout[T](ctx, items, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `DefaultForEachWithFailFast`

```go
func DefaultForEachWithFailFast[T any](ctx context.Context, items []T, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L190) · [行为专题](../mapreduce.md)：DefaultForEachWithFailFast 使用默认并发度的 FailFast ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachWithFailFast 使用默认并发度的 FailFast ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - fn：处理函数，只返回 error

使用示例：

	total, fail, firstErr, _ := sliceops.DefaultForEachWithFailFast(ctx, tasks, fn)


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachWithFailFast[T](ctx, items, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `DefaultForEachWithTimeout`

```go
func DefaultForEachWithTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L239) · [行为专题](../mapreduce.md)：DefaultForEachWithTimeout 使用默认并发度的带超时 ForEach。

**源码说明与使用片段**（原文）：

DefaultForEachWithTimeout 使用默认并发度的带超时 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - timeout：单任务超时时间
  - fn：处理函数，只返回 error

使用示例：

	total, fail, firstErr, _ := sliceops.DefaultForEachWithTimeout(ctx, msgs, 2*time.Second, sendFn)


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultForEachWithTimeout[T](ctx, items, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `DefaultMap`

```go
func DefaultMap[T any, R any](ctx context.Context, items []T, fn func(ctx context.Context, item T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L28) · [行为专题](../mapreduce.md)：DefaultMap 使用 core.IO() 作为默认并发度的 Map。

**源码说明与使用片段**（原文）：

DefaultMap 使用 core.IO() 作为默认并发度的 Map。 此包装只返回逐项 Result，舍弃 Map 的汇总 error；必须检查每项 Result.Err。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - fn：处理函数，接收 ctx 和元素，返回结果和错误

使用示例：

	results := sliceops.DefaultMap(ctx, ids, func(ctx context.Context, id int) (*User, error) {
	    return userRepo.FindByID(ctx, id)
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMap[T, R](ctx, items, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `DefaultMapChunk`

```go
func DefaultMapChunk[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L399) · [行为专题](../mapreduce.md)：DefaultMapChunk 使用默认并发度的分块 Map。

**源码说明与使用片段**（原文）：

DefaultMapChunk 使用默认并发度的分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - fn：处理函数，接收 ctx 和整个 chunk


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, chunk []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapChunk[T, R](ctx, items, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `DefaultMapChunkWithFFTimeout`

```go
func DefaultMapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L475) · [行为专题](../mapreduce.md)：DefaultMapChunkWithFFTimeout 使用默认并发度的 FailFast + 超时分块 Map。

**源码说明与使用片段**（原文）：

DefaultMapChunkWithFFTimeout 使用默认并发度的 FailFast + 超时分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, chunk []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapChunkWithFFTimeout[T, R](ctx, items, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `DefaultMapChunkWithFailFast`

```go
func DefaultMapChunkWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L423) · [行为专题](../mapreduce.md)：DefaultMapChunkWithFailFast 使用默认并发度的 FailFast 分块 Map。

**源码说明与使用片段**（原文）：

DefaultMapChunkWithFailFast 使用默认并发度的 FailFast 分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, chunk []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapChunkWithFailFast[T, R](ctx, items, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `DefaultMapChunkWithTimeout`

```go
func DefaultMapChunkWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L449) · [行为专题](../mapreduce.md)：DefaultMapChunkWithTimeout 使用默认并发度的带超时分块 Map。

**源码说明与使用片段**（原文）：

DefaultMapChunkWithTimeout 使用默认并发度的带超时分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, chunk []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapChunkWithTimeout[T, R](ctx, items, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `DefaultMapChunked`

```go
func DefaultMapChunked[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L517) · [行为专题](../mapreduce.md)：DefaultMapChunked 使用默认并发度的逐元素分块 Map。

**源码说明与使用片段**（原文）：

DefaultMapChunked 使用默认并发度的逐元素分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - fn：处理函数，接收 ctx 和单个元素


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapChunked[T, R](ctx, items, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。

## `DefaultMapChunkedWithFFTimeout`

```go
func DefaultMapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L638) · [行为专题](../mapreduce.md)：DefaultMapChunkedWithFFTimeout 使用默认并发度的 FailFast + 超时逐元素分块 Map。

**源码说明与使用片段**（原文）：

DefaultMapChunkedWithFFTimeout 使用默认并发度的 FailFast + 超时逐元素分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapChunkedWithFFTimeout[T, R](ctx, items, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。

## `DefaultMapChunkedWithFailFast`

```go
func DefaultMapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L559) · [行为专题](../mapreduce.md)：DefaultMapChunkedWithFailFast 使用默认并发度的 FailFast 逐元素分块 Map。

**源码说明与使用片段**（原文）：

DefaultMapChunkedWithFailFast 使用默认并发度的 FailFast 逐元素分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapChunkedWithFailFast[T, R](ctx, items, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。

## `DefaultMapChunkedWithTimeout`

```go
func DefaultMapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L597) · [行为专题](../mapreduce.md)：DefaultMapChunkedWithTimeout 使用默认并发度的带超时逐元素分块 Map。

**源码说明与使用片段**（原文）：

DefaultMapChunkedWithTimeout 使用默认并发度的带超时逐元素分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapChunkedWithTimeout[T, R](ctx, items, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。

## `DefaultMapWithFFTimeout`

```go
func DefaultMapWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L158) · [行为专题](../mapreduce.md)：DefaultMapWithFFTimeout 使用默认并发度的 FailFast + 超时 Map。

**源码说明与使用片段**（原文）：

DefaultMapWithFFTimeout 使用默认并发度的 FailFast + 超时 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - timeout：单任务超时时间
  - fn：处理函数

使用示例：

	results, err := sliceops.DefaultMapWithFFTimeout(ctx, ids, 3*time.Second, fn)


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapWithFFTimeout[T, R](ctx, items, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `DefaultMapWithFailFast`

```go
func DefaultMapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L45) · [行为专题](../mapreduce.md)：DefaultMapWithFailFast 使用默认并发度的 FailFast Map。

**源码说明与使用片段**（原文）：

DefaultMapWithFailFast 使用默认并发度的 FailFast Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - fn：处理函数，接收 ctx 和元素，返回结果和错误

使用示例：

	results, err := sliceops.DefaultMapWithFailFast(ctx, ids, func(ctx context.Context, id int) (*User, error) {
	    return userRepo.FindByID(ctx, id)
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapWithFailFast[T, R](ctx, items, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `DefaultMapWithTimeout`

```go
func DefaultMapWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L98) · [行为专题](../mapreduce.md)：DefaultMapWithTimeout 使用默认并发度的带超时 Map。

**源码说明与使用片段**（原文）：

DefaultMapWithTimeout 使用默认并发度的带超时 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - timeout：单任务超时时间
  - fn：处理函数

使用示例：

	results := sliceops.DefaultMapWithTimeout(ctx, ids, 3*time.Second, fn)


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.DefaultMapWithTimeout[T, R](ctx, items, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `Every`

```go
func Every[T any](results []core.Result[T]) bool
```

[实现](../../internal/sliceops/extended_impl.go#L321) · [行为专题](../mapreduce.md)：Every 检查所有槽位的 Err 是否均为 nil；不检查 Occupied，空切片返回 true。

**源码说明与使用片段**（原文）：

Every 检查所有槽位的 Err 是否均为 nil；不检查 Occupied，空切片返回 true。


**参数说明**：

- `results` (`[]core.Result[T]`)：待遍历或转换的执行结果，逐项检查 Err 后使用 Value。

**返回值签名**：`bool`。true 表示对应结果成功；空集合规则见方法说明。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Every[T](results)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只按 Err 是否为 nil 判断成功，不检查 Occupied，零值空槽也计成功。空集合 Every=true、Some=false；ResultValues/PartitionResults 保留各类在原输入中的相对顺序，返回新切片但内部引用仍共享。应在任务最终完成后按入口约定核对完整性。

## `Flat`

```go
func Flat[T any](results []core.Result[T]) []T
```

[实现](../../internal/sliceops/exec_impl.go#L1139) · [行为专题](../mapreduce.md)：Flat 从 Result 切片中提取所有值。

**源码说明与使用片段**（原文）：

Flat 从 Result 切片中提取所有值。


**参数说明**：

- `results` (`[]core.Result[T]`)：待遍历或转换的执行结果，逐项检查 Err 后使用 Value。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Flat[T](results)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按原顺序提取所有槽位的 Value，包含失败项与未占用空槽，不检查 Err/Occupied；返回新切片但元素内部引用仍共享。需要成功业务结果时先按入口约定筛选，不能把 Flat 当作成功过滤器。

## `ForEach`

```go
func ForEach[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/exec_impl.go#L1197) · [行为专题](../mapreduce.md)：ForEach 并发遍历切片。

**源码说明与使用片段**（原文）：

ForEach 并发遍历切片。 Deprecated: 推荐使用 execEach 结合 Policy。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEach[T](ctx, items, fn, concurrency)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `ForEachChunk`

```go
func ForEachChunk[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L652) · [行为专题](../mapreduce.md)：ForEachChunk 先分块，再对每块并发执行只返回 error 的操作。

**源码说明与使用片段**（原文）：

ForEachChunk 先分块，再对每块并发执行只返回 error 的操作。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - fn：处理函数，接收 ctx 和整个 chunk，只返回 error


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, chunk []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachChunk[T](ctx, items, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `ForEachChunkWithFFTimeout`

```go
func ForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L733) · [行为专题](../mapreduce.md)：ForEachChunkWithFFTimeout 带 FailFast 和超时的分块 ForEach。

**源码说明与使用片段**（原文）：

ForEachChunkWithFFTimeout 带 FailFast 和超时的分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, chunk []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachChunkWithFFTimeout[T](ctx, items, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `ForEachChunkWithFailFast`

```go
func ForEachChunkWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L678) · [行为专题](../mapreduce.md)：ForEachChunkWithFailFast 带 FailFast 的分块 ForEach。

**源码说明与使用片段**（原文）：

ForEachChunkWithFailFast 带 FailFast 的分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, chunk []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachChunkWithFailFast[T](ctx, items, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `ForEachChunkWithTimeout`

```go
func ForEachChunkWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L705) · [行为专题](../mapreduce.md)：ForEachChunkWithTimeout 带超时的分块 ForEach。

**源码说明与使用片段**（原文）：

ForEachChunkWithTimeout 带超时的分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, chunk []T) error`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachChunkWithTimeout[T](ctx, items, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `ForEachChunked`

```go
func ForEachChunked[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L765) · [行为专题](../mapreduce.md)：ForEachChunked 先分块，再对每个元素并发执行只返回 error 的操作。

**源码说明与使用片段**（原文）：

ForEachChunked 先分块，再对每个元素并发执行只返回 error 的操作。 返回的总数、失败数和 results 均按批次计数；一批内多个失败合并为一个失败批次。 外层批次与批内元素各自使用 concurrency，总并发可能超过 concurrency。 Slice.ForEachChunked 方法按元素统计，与本独立函数不同。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - fn：处理函数，接收 ctx 和单个元素，只返回 error


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachChunked[T](ctx, items, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。

## `ForEachChunkedWithFFTimeout`

```go
func ForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L851) · [行为专题](../mapreduce.md)：ForEachChunkedWithFFTimeout 带 FailFast 和超时的逐元素分块 ForEach。

**源码说明与使用片段**（原文）：

ForEachChunkedWithFFTimeout 带 FailFast 和超时的逐元素分块 ForEach。 timeout 由整个批次共享，结果及统计按批次返回；取消仍需元素回调配合。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：每个批次共享的 context 超时；从外层批次任务开始执行计时，包含批内调度及全部元素执行。正数生效，非正值清除外层期限，批内 Group 仍可能继承全局默认超时；不是每个元素重新计时，回调须响应 context。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachChunkedWithFFTimeout[T](ctx, items, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。

## `ForEachChunkedWithFailFast`

```go
func ForEachChunkedWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L792) · [行为专题](../mapreduce.md)：ForEachChunkedWithFailFast 带 FailFast 的逐元素分块 ForEach。

**源码说明与使用片段**（原文）：

ForEachChunkedWithFailFast 带 FailFast 的逐元素分块 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachChunkedWithFailFast[T](ctx, items, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。

## `ForEachChunkedWithTimeout`

```go
func ForEachChunkedWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L821) · [行为专题](../mapreduce.md)：ForEachChunkedWithTimeout 带超时的逐元素分块 ForEach。

**源码说明与使用片段**（原文）：

ForEachChunkedWithTimeout 带超时的逐元素分块 ForEach。 timeout 从外层批次开始执行计时，批内全部元素共享期限；结果及统计按批次返回。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：每个批次共享的 context 超时；从外层批次任务开始执行计时，包含批内调度及全部元素执行。正数生效，非正值清除外层期限，批内 Group 仍可能继承全局默认超时；不是每个元素重新计时，回调须响应 context。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachChunkedWithTimeout[T](ctx, items, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。

## `ForEachWithFFTimeout`

```go
func ForEachWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L255) · [行为专题](../mapreduce.md)：ForEachWithFFTimeout 带 FailFast 和单任务超时的 ForEach。

**源码说明与使用片段**（原文）：

ForEachWithFFTimeout 带 FailFast 和单任务超时的 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - timeout：单任务超时时间
  - fn：处理函数，只返回 error

使用示例：

	total, fail, firstErr, _ := sliceops.ForEachWithFFTimeout(ctx, tasks, 8, 3*time.Second, fn)


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachWithFFTimeout[T](ctx, items, concurrency, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `ForEachWithFailFast`

```go
func ForEachWithFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/exec_impl.go#L1203) · [行为专题](../mapreduce.md)：ForEachWithFailFast 并发 ForEach，首个失败时取消其余任务。

**源码说明与使用片段**（原文）：

ForEachWithFailFast 并发 ForEach，首个失败时取消其余任务。 Deprecated: 推荐使用 execEach 结合 Policy.FF()。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachWithFailFast[T](ctx, items, fn, concurrency)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `ForEachWithTimeout`

```go
func ForEachWithTimeout[T any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(ctx context.Context, item T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

[实现](../../internal/sliceops/extended_impl.go#L206) · [行为专题](../mapreduce.md)：ForEachWithTimeout 带单任务超时的 ForEach。

**源码说明与使用片段**（原文）：

ForEachWithTimeout 带单任务超时的 ForEach。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - timeout：单任务超时时间
  - fn：处理函数，只返回 error

使用示例：

	total, fail, firstErr, _ := sliceops.ForEachWithTimeout(ctx, msgs, 10, 2*time.Second, sendFn)


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) error`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`total int64, failCnt int64, firstErr error, results []core.Result[struct{}]`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ForEachWithTimeout[T](ctx, items, concurrency, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `Map`

```go
func Map[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/exec_impl.go#L1185) · [行为专题](../mapreduce.md)：Map 并发处理切片元素，返回 Result 切片。

**源码说明与使用片段**（原文）：

Map 并发处理切片元素，返回 Result 切片。 Deprecated: 推荐使用 execMap 结合 Policy。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Map[T, R](ctx, items, fn, concurrency)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `MapChunk`

```go
func MapChunk[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L386) · [行为专题](../mapreduce.md)：MapChunk 先按 batchSize 分块，再并发处理每个块。fn 接收整个 chunk。

**源码说明与使用片段**（原文）：

MapChunk 先按 batchSize 分块，再并发处理每个块。fn 接收整个 chunk。 适合批量 RPC 调用、batch DB 操作等场景。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - fn：处理函数，接收 ctx 和整个 chunk


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, chunk []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapChunk[T, R](ctx, items, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `MapChunkWithFFTimeout`

```go
func MapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L462) · [行为专题](../mapreduce.md)：MapChunkWithFFTimeout 带 FailFast 和超时的分块 Map。

**源码说明与使用片段**（原文）：

MapChunkWithFFTimeout 带 FailFast 和超时的分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数，接收 ctx 和整个 chunk


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, chunk []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapChunkWithFFTimeout[T, R](ctx, items, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `MapChunkWithFailFast`

```go
func MapChunkWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, chunk []T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L411) · [行为专题](../mapreduce.md)：MapChunkWithFailFast 带 FailFast 的分块 Map。

**源码说明与使用片段**（原文）：

MapChunkWithFailFast 带 FailFast 的分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - fn：处理函数，接收 ctx 和整个 chunk


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, chunk []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapChunkWithFailFast[T, R](ctx, items, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `MapChunkWithTimeout`

```go
func MapChunkWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, chunk []T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L436) · [行为专题](../mapreduce.md)：MapChunkWithTimeout 带超时的分块 Map。

**源码说明与使用片段**（原文）：

MapChunkWithTimeout 带超时的分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数，接收 ctx 和整个 chunk


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, chunk []T) (R, error)`)：每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapChunkWithTimeout[T, R](ctx, items, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。

## `MapChunked`

```go
func MapChunked[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L490) · [行为专题](../mapreduce.md)：MapChunked 先分块，再对每个元素并发调用 fn。

**源码说明与使用片段**（原文）：

MapChunked 先分块，再对每个元素并发调用 fn。 与 MapChunk 的区别：fn 接收单个元素，适合逐元素处理的大数据量场景。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - fn：处理函数，接收 ctx 和单个元素


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：底层 Group 的同时执行上限；非正值使用 1，Default* 入口显式传入 core.IO()。分块只改变提交遍历，仍由一个 Group 调度全部元素。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapChunked[T, R](ctx, items, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。

## `MapChunkedWithFFTimeout`

```go
func MapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L610) · [行为专题](../mapreduce.md)：MapChunkedWithFFTimeout 带 FailFast 和超时的逐元素分块 Map。

**源码说明与使用片段**（原文）：

MapChunkedWithFFTimeout 带 FailFast 和超时的逐元素分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数，接收 ctx 和单个元素


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：底层 Group 的同时执行上限；非正值使用 1，Default* 入口显式传入 core.IO()。分块只改变提交遍历，仍由一个 Group 调度全部元素。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapChunkedWithFFTimeout[T, R](ctx, items, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。

## `MapChunkedWithFailFast`

```go
func MapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L529) · [行为专题](../mapreduce.md)：MapChunkedWithFailFast 带 FailFast 的逐元素分块 Map。

**源码说明与使用片段**（原文）：

MapChunkedWithFailFast 带 FailFast 的逐元素分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - fn：处理函数，接收 ctx 和单个元素


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：底层 Group 的同时执行上限；非正值使用 1，Default* 入口显式传入 core.IO()。分块只改变提交遍历，仍由一个 Group 调度全部元素。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapChunkedWithFailFast[T, R](ctx, items, concurrency, batchSize, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。

## `MapChunkedWithTimeout`

```go
func MapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L572) · [行为专题](../mapreduce.md)：MapChunkedWithTimeout 带超时的逐元素分块 Map。

**源码说明与使用片段**（原文）：

MapChunkedWithTimeout 带超时的逐元素分块 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度
  - batchSize：每块大小
  - timeout：单任务超时时间
  - fn：处理函数，接收 ctx 和单个元素


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：底层 Group 的同时执行上限；非正值使用 1，Default* 入口显式传入 core.IO()。分块只改变提交遍历，仍由一个 Group 调度全部元素。
- `batchSize` (`int`)：每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapChunkedWithTimeout[T, R](ctx, items, concurrency, batchSize, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。

## `MapWithFFTimeout`

```go
func MapWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/extended_impl.go#L119) · [行为专题](../mapreduce.md)：MapWithFFTimeout 带 FailFast 和单任务超时的并发 Map。

**源码说明与使用片段**（原文）：

MapWithFFTimeout 带 FailFast 和单任务超时的并发 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度，\<=0 使用 core.IO()
  - timeout：单任务超时时间
  - fn：处理函数，接收 ctx 和元素，返回结果和错误

使用示例：

	results, err := sliceops.MapWithFFTimeout(ctx, urls, 8, 3*time.Second, func(ctx context.Context, url string) (*Body, error) {
	    return httpGet(ctx, url)
	})
	if err != nil {
	    log.Printf("首个失败: %v", err)
	}


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapWithFFTimeout[T, R](ctx, items, concurrency, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `MapWithFailFast`

```go
func MapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error)
```

[实现](../../internal/sliceops/exec_impl.go#L1191) · [行为专题](../mapreduce.md)：MapWithFailFast 并发 Map，首个失败时取消其余任务。

**源码说明与使用片段**（原文）：

MapWithFailFast 并发 Map，首个失败时取消其余任务。 Deprecated: 推荐使用 execMap 结合 Policy.FF()。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `fn` (`func(context.Context, T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。

**返回值签名**：`[]core.Result[R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapWithFailFast[T, R](ctx, items, fn, concurrency)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `MapWithTimeout`

```go
func MapWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(ctx context.Context, item T) (R, error)) []core.Result[R]
```

[实现](../../internal/sliceops/extended_impl.go#L63) · [行为专题](../mapreduce.md)：MapWithTimeout 带单任务超时的并发 Map。

**源码说明与使用片段**（原文）：

MapWithTimeout 带单任务超时的并发 Map。

参数：

  - ctx：上下文
  - items：待处理的元素切片
  - concurrency：并发度，\<=0 使用 core.IO()
  - timeout：单任务超时时间
  - fn：处理函数，接收 ctx 和元素，返回结果和错误

使用示例：

	results := sliceops.MapWithTimeout(ctx, urls, 8, 5*time.Second, func(ctx context.Context, url string) (*Body, error) {
	    return httpGet(ctx, url)
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `items` (`[]T`)：待处理的元素切片
- `concurrency` (`int`)：任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。
- `timeout` (`time.Duration`)：单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。
- `fn` (`func(ctx context.Context, item T) (R, error)`)：逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。

**返回值签名**：`[]core.Result[R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.MapWithTimeout[T, R](ctx, items, concurrency, timeout, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。

## `Must`

```go
func Must[T any](val T, err error) T
```

[实现](../../internal/sliceops/exec_impl.go#L1131) · [行为专题](../mapreduce.md)：Must 错误时 panic。

**源码说明与使用片段**（原文）：

Must 错误时 panic。


**参数说明**：

- `val` (`T`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。
- `err` (`error`)：要检查、封装或记录的错误；nil 表示无错误，Must 遇到非 nil 错误会 panic。

**返回值签名**：`T`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Must[T](val, err)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：err 非 nil 时原样 panic，否则直接返回 val，不检查业务完整性或复制引用内容。仅用于调用方明确接受 panic 的初始化/测试；生产请求路径应显式处理 error。

## `New`

```go
func New[T any](items []T) *Slice[T, T]
```

[实现](../../internal/sliceops/slice.go#L36) · [行为专题](../mapreduce.md)：New 创建 Slice 对象，默认 R=T（适用于纯切片操作，无需关注 R）。

**源码说明与使用片段**（原文）：

New 创建 Slice 对象，默认 R=T（适用于纯切片操作，无需关注 R）。


**参数说明**：

- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*Slice[T, T]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.New[T](items)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只保存输入切片引用，nil 规范化为非 nil 空切片，不创建 worker。后续原地操作可能修改输入底层数组；需要隔离时先复制，指针/map/切片元素仍须按业务深拷贝。Slice 不带锁，不与共享输入读写并发修改。

## `NewRunner`

```go
func NewRunner[T any, R any](ctx context.Context, items []T, policy Policy) *Runner[T, R]
```

[实现](../../internal/sliceops/runner.go#L31) · [行为专题](../mapreduce.md)：NewRunner 创建执行器。ctx 自动注入 trace_id。

**源码说明与使用片段**（原文）：

NewRunner 创建执行器。ctx 自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `policy` (`Policy`)：执行策略值，包含串行/并行、worker、超时、分片等设置；以返回的新策略继续配置。

**返回值签名**：`*Runner[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.NewRunner[T, R](ctx, items, policy)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：保存原输入切片引用并确保非 nil context 的 trace_id，不复制输入或立即启动业务；纯数据方法可能修改底层数组，终端读取期间不得并发修改。Builder 默认 DefPar，Runner 保存显式 Policy；需要独立元素时先复制，元素内部引用还须按业务隔离。

## `NewSliceBuilder`

```go
func NewSliceBuilder[T any](ctx context.Context, items []T) *SliceBuilder[T, T]
```

[实现](../../internal/sliceops/slice_builder.go#L75) · [行为专题](../mapreduce.md)：NewSliceBuilder 创建同类型切片构建器（R=T），默认为并行模式。

**源码说明与使用片段**（原文）：

NewSliceBuilder 创建同类型切片构建器（R=T），默认为并行模式。

	ctx:   上下文（自动注入 TraceID）。
	items: 原始切片数据。

	sb := NewSliceBuilder(ctx, []int{1, 2, 3}).Sort(cmp).Map(fn)


**参数说明**：

- `ctx` (`context.Context`)：上下文（自动注入 TraceID）。
- `items` (`[]T`)：原始切片数据。

**返回值签名**：`*SliceBuilder[T, T]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.NewSliceBuilder[T](ctx, items)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：保存原输入切片引用并确保非 nil context 的 trace_id，不复制输入或立即启动业务；纯数据方法可能修改底层数组，终端读取期间不得并发修改。Builder 默认 DefPar，Runner 保存显式 Policy；需要独立元素时先复制，元素内部引用还须按业务隔离。

## `NewSliceWithBuilder`

```go
func NewSliceWithBuilder[R any, T any](ctx context.Context, items []T) *SliceBuilder[T, R]
```

[实现](../../internal/sliceops/slice_builder.go#L90) · [行为专题](../mapreduce.md)：NewSliceWithBuilder 创建跨类型切片构建器（T→R），显式指定输出类型 R，输入类型 T 由 items 自动推断。

**源码说明与使用片段**（原文）：

NewSliceWithBuilder 创建跨类型切片构建器（T→R），显式指定输出类型 R，输入类型 T 由 items 自动推断。 默认为并行模式。

	ctx:   上下文（自动注入 TraceID）。
	items: 原始切片数据（类型 T）。

	sb := NewSliceWithBuilder[string](ctx, []int{1, 2, 3}).Map(func(ctx context.Context, n int) (string, error) {
	    return strconv.Itoa(n), nil
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文（自动注入 TraceID）。
- `items` (`[]T`)：原始切片数据（类型 T）。

**返回值签名**：`*SliceBuilder[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.NewSliceWithBuilder[R, T](ctx, items)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：保存原输入切片引用并确保非 nil context 的 trace_id，不复制输入或立即启动业务；纯数据方法可能修改底层数组，终端读取期间不得并发修改。Builder 默认 DefPar，Runner 保存显式 Policy；需要独立元素时先复制，元素内部引用还须按业务隔离。

## `NewWithResult`

```go
func NewWithResult[T any, R any](items []T) *Slice[T, R]
```

[实现](../../internal/sliceops/slice.go#L44) · [行为专题](../mapreduce.md)：NewWithResult 创建 Slice 对象并显式声明 R 类型（适用于 T→R 的 Map/Reduce 操作）。

**源码说明与使用片段**（原文）：

NewWithResult 创建 Slice 对象并显式声明 R 类型（适用于 T→R 的 Map/Reduce 操作）。


**参数说明**：

- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*Slice[T, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.NewWithResult[T, R](items)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只保存输入切片引用，nil 规范化为非 nil 空切片，不创建 worker。后续原地操作可能修改输入底层数组；需要隔离时先复制，指针/map/切片元素仍须按业务深拷贝。Slice 不带锁，不与共享输入读写并发修改。

## `OnlyErrors`

```go
func OnlyErrors[T any](results []core.Result[T]) []error
```

[实现](../../internal/sliceops/exec_impl.go#L1148) · [行为专题](../mapreduce.md)：OnlyErrors 从 Result 切片中提取所有非 nil 错误。

**源码说明与使用片段**（原文）：

OnlyErrors 从 Result 切片中提取所有非 nil 错误。


**参数说明**：

- `results` (`[]core.Result[T]`)：待遍历或转换的执行结果，逐项检查 Err 后使用 Value。

**返回值签名**：`[]error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.OnlyErrors[T](results)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只查看输入槽位的非 nil Err，不检查 Occupied，也不补充汇总错误；空集合没有错误。错误列表按输入顺序，返回新切片且 error 对象仍共享；结果截断或空槽时不能据此证明所有业务均已成功。

## `Par`

```go
func Par(n int) Policy
```

[实现](../../internal/sliceops/policy.go#L50) · [行为专题](../mapreduce.md)：Par 创建并行执行策略。n <= 0 时使用 core.IO() 默认并发度。

**源码说明与使用片段**（原文）：

Par 创建并行执行策略。n \<= 0 时使用 core.IO() 默认并发度。


**参数说明**：

- `n` (`int`)：Par 创建并行执行策略。n <= 0 时使用 core.IO() 默认并发度。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Par(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回独立策略值，不创建 worker；Seq 标记串行并保存并发 1，Par 非正并发使用 IO()，DefPar 显式使用调用时 IO()。其他字段初始为零，未主动调用 Default* 不代表超时 30s/分块 100 已启用。必须接收链式设置返回值，池引用依然共享。

## `Partition`

```go
func Partition[T any](items []T, pred func(T) bool) (matched []T, unmatched []T)
```

[实现](../../internal/sliceops/exec_impl.go#L1119) · [行为专题](../mapreduce.md)：Partition 按条件将切片拆分为匹配和不匹配两个切片。

**源码说明与使用片段**（原文）：

Partition 按条件将切片拆分为匹配和不匹配两个切片。


**参数说明**：

- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `pred` (`func(T) bool`)：同步按输入顺序判断元素；true 放入 matched，false 放入 unmatched。返回两个新切片但元素内部引用仍共享，panic 直接传播，不修改输入切片。

**返回值签名**：`matched []T, unmatched []T`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Partition[T](items, pred)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步按输入顺序把匹配/不匹配元素追加到两个新切片，原输入不改变；空输入返回两份 nil。谓词 panic 直接传播，不使用执行策略、取消检查或错误恢复；元素内部引用仍共享，输入不得并发修改。

## `PartitionResults`

```go
func PartitionResults[T any](results []core.Result[T]) (successes []T, failures []error)
```

[实现](../../internal/sliceops/extended_impl.go#L362) · [行为专题](../mapreduce.md)：PartitionResults 按 Err 是否为 nil 拆分槽位值与错误；不检查 Occupied，空槽归入值切片。

**源码说明与使用片段**（原文）：

PartitionResults 按 Err 是否为 nil 拆分槽位值与错误；不检查 Occupied，空槽归入值切片。

参数：

  - results：Result 切片

使用示例：

	successes, failures := sliceops.PartitionResults(results)
	fmt.Printf("成功: %d, 失败: %d\n", len(successes), len(failures))


**参数说明**：

- `results` (`[]core.Result[T]`)：Result 切片

**返回值签名**：`successes []T, failures []error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.PartitionResults[T](results)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只按 Err 是否为 nil 判断成功，不检查 Occupied，零值空槽也计成功。空集合 Every=true、Some=false；ResultValues/PartitionResults 保留各类在原输入中的相对顺序，返回新切片但内部引用仍共享。应在任务最终完成后按入口约定核对完整性。

## `Reduce`

```go
func Reduce[T any, R any](ctx context.Context, items []T, initial R, fn func(context.Context, R, T) (R, error)) (R, error)
```

[实现](../../internal/sliceops/exec_impl.go#L1104) · [行为专题](../mapreduce.md)：Reduce 串行聚合：对每个元素调用 fn(acc, item)，累积结果。

**源码说明与使用片段**（原文）：

Reduce 串行聚合：对每个元素调用 fn(acc, item)，累积结果。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `items` (`[]T`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。
- `initial` (`R`)：聚合的初始累积值。
- `fn` (`func(context.Context, R, T) (R, error)`)：同步接收 context、当前累积值及输入元素；返回下一累积值和错误，非 nil 错误终止遍历并返回该次累积值，panic 向调用方传播。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Reduce[T, R](ctx, items, initial, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按输入顺序同步聚合，空输入返回 initial；遇错误返回该次回调的新累积值及错误，panic 直接传播。确保非 nil context 的 trace_id，但不自动检查取消或派生超时；回调须自行响应，不修改正在遍历的输入。

## `ResultErrors`

```go
func ResultErrors[T any](results []core.Result[T]) []error
```

[实现](../../internal/sliceops/extended_impl.go#L310) · [行为专题](../mapreduce.md)：ResultErrors 提取所有非 nil Err；不检查 Occupied。

**源码说明与使用片段**（原文）：

ResultErrors 提取所有非 nil Err；不检查 Occupied。


**参数说明**：

- `results` (`[]core.Result[T]`)：待遍历或转换的执行结果，逐项检查 Err 后使用 Value。

**返回值签名**：`[]error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ResultErrors[T](results)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只查看输入槽位的非 nil Err，不检查 Occupied，也不补充汇总错误；空集合没有错误。错误列表按输入顺序，返回新切片且 error 对象仍共享；结果截断或空槽时不能据此证明所有业务均已成功。

## `ResultValues`

```go
func ResultValues[T any](results []core.Result[T]) []T
```

[实现](../../internal/sliceops/extended_impl.go#L299) · [行为专题](../mapreduce.md)：ResultValues 提取 Err 为 nil 的槽位值；不检查 Occupied，未占用空槽也会返回零值。

**源码说明与使用片段**（原文）：

ResultValues 提取 Err 为 nil 的槽位值；不检查 Occupied，未占用空槽也会返回零值。


**参数说明**：

- `results` (`[]core.Result[T]`)：待遍历或转换的执行结果，逐项检查 Err 后使用 Value。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.ResultValues[T](results)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只按 Err 是否为 nil 判断成功，不检查 Occupied，零值空槽也计成功。空集合 Every=true、Some=false；ResultValues/PartitionResults 保留各类在原输入中的相对顺序，返回新切片但内部引用仍共享。应在任务最终完成后按入口约定核对完整性。

## `SafeCallWithResult`

```go
func SafeCallWithResult[T any, R any](ctx context.Context, item T, fn func(context.Context, T) (R, error)) (val R, err error)
```

[实现](../../internal/sliceops/exec_impl.go#L1159) · [行为专题](../mapreduce.md)：SafeCallWithResult 带 panic 恢复的安全调用。

**源码说明与使用片段**（原文）：

SafeCallWithResult 带 panic 恢复的安全调用。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `item` (`T`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。
- `fn` (`func(context.Context, T) (R, error)`)：同步接收 context 和输入项的业务函数；返回签名中的值和错误。panic 被恢复并包装为 PanicError；不设置超时、不另启 goroutine，回调须自行响应 context。

**返回值签名**：`val R, err error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.SafeCallWithResult[T, R](ctx, item, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步直接调用一次 fn，把同一 goroutine 的 panic 转为包含堆栈的 PanicError 并记录日志；子 goroutine panic 不在恢复范围。context 原样传入，不自动检查取消或设置期限；正常错误保留，panic 时值为零值，日志器须能安全返回。

## `Seq`

```go
func Seq() Policy
```

[实现](../../internal/sliceops/policy.go#L45) · [行为专题](../mapreduce.md)：Seq 创建串行执行策略。

**源码说明与使用片段**（原文）：

Seq 创建串行执行策略。


**参数说明**：无显式参数。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Seq()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回独立策略值，不创建 worker；Seq 标记串行并保存并发 1，Par 非正并发使用 IO()，DefPar 显式使用调用时 IO()。其他字段初始为零，未主动调用 Default* 不代表超时 30s/分块 100 已启用。必须接收链式设置返回值，池引用依然共享。

## `Some`

```go
func Some[T any](results []core.Result[T]) bool
```

[实现](../../internal/sliceops/extended_impl.go#L331) · [行为专题](../mapreduce.md)：Some 检查是否至少有一个槽位的 Err 为 nil；不检查 Occupied，空切片返回 false。

**源码说明与使用片段**（原文）：

Some 检查是否至少有一个槽位的 Err 为 nil；不检查 Occupied，空切片返回 false。


**参数说明**：

- `results` (`[]core.Result[T]`)：待遍历或转换的执行结果，逐项检查 Err 后使用 Value。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
sliceops.Some[T](results)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只按 Err 是否为 nil 判断成功，不检查 Occupied，零值空槽也计成功。空集合 Every=true、Some=false；ResultValues/PartitionResults 保留各类在原输入中的相对顺序，返回新切片但内部引用仍共享。应在任务最终完成后按入口约定核对完整性。

## `Policy.Buf`

```go
func (p Policy) Buf(n int) Policy
```

[实现](../../internal/sliceops/policy.go#L111) · [行为专题](../mapreduce.md)：Buf 设置流式缓冲区大小。

**源码说明与使用片段**（原文）：

Buf 设置流式缓冲区大小。


**参数说明**：

- `n` (`int`)：Buf 设置流式缓冲区大小。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Buf(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按值返回策略副本，必须保存返回值；Worker 非正按 IO()，Shard 非正按 max(2,GOMAXPROCS)，其余保存原值。Worker 不切换 serial 标志，Seq().Worker(n) 仍标记串行；实际采用哪些字段由终端路径决定。以 Seq/Par/DefPar 为起点，零 Policy 在部分终端会整体替换为 DefPar，单独设置 TO/FF/Chunk 等可能被丢弃。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.bufSize = n
```

## `Policy.Chunk`

```go
func (p Policy) Chunk(size int) Policy
```

[实现](../../internal/sliceops/policy.go#L84) · [行为专题](../mapreduce.md)：Chunk 设置分块大小，逐元素调用 fn，但按块并发调度。

**源码说明与使用片段**（原文）：

Chunk 设置分块大小，逐元素调用 fn，但按块并发调度。


**参数说明**：

- `size` (`int`)：Chunk 设置分块大小，逐元素调用 fn，但按块并发调度。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Chunk(size)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按值返回策略副本，必须保存返回值；Worker 非正按 IO()，Shard 非正按 max(2,GOMAXPROCS)，其余保存原值。Worker 不切换 serial 标志，Seq().Worker(n) 仍标记串行；实际采用哪些字段由终端路径决定。以 Seq/Par/DefPar 为起点，零 Policy 在部分终端会整体替换为 DefPar，单独设置 TO/FF/Chunk 等可能被丢弃。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.chunkSize = size
```

## `Policy.FF`

```go
func (p Policy) FF() Policy
```

[实现](../../internal/sliceops/policy.go#L66) · [行为专题](../mapreduce.md)：FF 启用 FailFast；Slice Runner 的执行路径在失败时取消派生 context。

**源码说明与使用片段**（原文）：

FF 启用 FailFast；Slice Runner 的执行路径在失败时取消派生 context。 Map 的无池执行路径只停止后续调度，不主动取消已开始回调；行为由使用此策略的执行器决定。


**参数说明**：无显式参数。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FF()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按值返回修改后的策略，必须保存返回值；不修改原策略，也不取消已启动任务。Slice Runner 的失败取消与无池 Map 只停止后续调度的行为不同，Fold 不读取此开关；回调仍须响应 context，不能把取消当作回滚。

## `Policy.GetBuf`

```go
func (p Policy) GetBuf() int
```

[实现](../../internal/sliceops/policy.go#L187) · [行为专题](../mapreduce.md)：GetBuf 流式缓冲区大小。

**源码说明与使用片段**（原文）：

GetBuf 流式缓冲区大小。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetBuf()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.GetChunkSize`

```go
func (p Policy) GetChunkSize() int
```

[实现](../../internal/sliceops/policy.go#L184) · [行为专题](../mapreduce.md)：GetChunkSize 分块大小。

**源码说明与使用片段**（原文）：

GetChunkSize 分块大小。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetChunkSize()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.GetShards`

```go
func (p Policy) GetShards() int
```

[实现](../../internal/sliceops/policy.go#L181) · [行为专题](../mapreduce.md)：GetShards 分片数。

**源码说明与使用片段**（原文）：

GetShards 分片数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetShards()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.GetTimeout`

```go
func (p Policy) GetTimeout() time.Duration
```

[实现](../../internal/sliceops/policy.go#L175) · [行为专题](../mapreduce.md)：GetTimeout 超时时间。

**源码说明与使用片段**（原文）：

GetTimeout 超时时间。


**参数说明**：无显式参数。

**返回值签名**：`time.Duration`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetTimeout()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.GetWorker`

```go
func (p Policy) GetWorker() int
```

[实现](../../internal/sliceops/policy.go#L172) · [行为专题](../mapreduce.md)：GetWorker 并发度。

**源码说明与使用片段**（原文）：

GetWorker 并发度。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetWorker()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.IsFailFast`

```go
func (p Policy) IsFailFast() bool
```

[实现](../../internal/sliceops/policy.go#L178) · [行为专题](../mapreduce.md)：IsFailFast 是否快速失败。

**源码说明与使用片段**（原文）：

IsFailFast 是否快速失败。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsFailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.IsParallel`

```go
func (p Policy) IsParallel() bool
```

[实现](../../internal/sliceops/policy.go#L169) · [行为专题](../mapreduce.md)：IsParallel 判断是否并行模式。

**源码说明与使用片段**（原文）：

IsParallel 判断是否并行模式。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsParallel()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.IsPoolOwned`

```go
func (p Policy) IsPoolOwned() bool
```

[实现](../../internal/sliceops/policy.go#L146) · [行为专题](../mapreduce.md)：IsPoolOwned 返回终端执行是否负责关闭池。

**源码说明与使用片段**（原文）：

IsPoolOwned 返回终端执行是否负责关闭池。 WithDefaultPool 创建的池和 WithOwnedPool 接管的池均返回 true。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsPoolOwned()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.IsSerial`

```go
func (p Policy) IsSerial() bool
```

[实现](../../internal/sliceops/policy.go#L166) · [行为专题](../mapreduce.md)：IsSerial 判断是否串行模式。

**源码说明与使用片段**（原文）：

IsSerial 判断是否串行模式。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsSerial()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.NoFF`

```go
func (p Policy) NoFF() Policy
```

[实现](../../internal/sliceops/policy.go#L72) · [行为专题](../mapreduce.md)：NoFF 关闭 FailFast（恢复默认）。

**源码说明与使用片段**（原文）：

NoFF 关闭 FailFast（恢复默认）。


**参数说明**：无显式参数。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.NoFF()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按值返回修改后的策略，必须保存返回值；不修改原策略，也不取消已启动任务。Slice Runner 的失败取消与无池 Map 只停止后续调度的行为不同，Fold 不读取此开关；回调仍须响应 context，不能把取消当作回滚。

## `Policy.Pool`

```go
func (p Policy) Pool() any
```

[实现](../../internal/sliceops/policy.go#L140) · [行为专题](../mapreduce.md)：Pool 返回外部协程池引用，nil 表示未设置。

**源码说明与使用片段**（原文）：

Pool 返回外部协程池引用，nil 表示未设置。


**参数说明**：无显式参数。

**返回值签名**：`any`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。

## `Policy.Shard`

```go
func (p Policy) Shard(n int) Policy
```

[实现](../../internal/sliceops/policy.go#L90) · [行为专题](../mapreduce.md)：Shard 设置水平分片数，n <= 0 时使用 GOMAXPROCS（至少 2）。

**源码说明与使用片段**（原文）：

Shard 设置水平分片数，n \<= 0 时使用 GOMAXPROCS（至少 2）。


**参数说明**：

- `n` (`int`)：Shard 设置水平分片数，n <= 0 时使用 GOMAXPROCS（至少 2）。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shard(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按值返回策略副本，必须保存返回值；Worker 非正按 IO()，Shard 非正按 max(2,GOMAXPROCS)，其余保存原值。Worker 不切换 serial 标志，Seq().Worker(n) 仍标记串行；实际采用哪些字段由终端路径决定。以 Seq/Par/DefPar 为起点，零 Policy 在部分终端会整体替换为 DefPar，单独设置 TO/FF/Chunk 等可能被丢弃。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.shards = n
```

## `Policy.TO`

```go
func (p Policy) TO(d time.Duration) Policy
```

[实现](../../internal/sliceops/policy.go#L78) · [行为专题](../mapreduce.md)：TO 设置单任务超时。

**源码说明与使用片段**（原文）：

TO 设置单任务超时。


**参数说明**：

- `d` (`time.Duration`)：TO 设置单任务超时。 使用 time.Millisecond / time.Second 等时长单位；零值与负值规则见本方法说明。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.TO(d)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按值返回策略副本，必须保存返回值；Worker 非正按 IO()，Shard 非正按 max(2,GOMAXPROCS)，其余保存原值。Worker 不切换 serial 标志，Seq().Worker(n) 仍标记串行；实际采用哪些字段由终端路径决定。以 Seq/Par/DefPar 为起点，零 Policy 在部分终端会整体替换为 DefPar，单独设置 TO/FF/Chunk 等可能被丢弃。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.timeout = d
```

## `Policy.WithDefaultPool`

```go
func (p Policy) WithDefaultPool() Policy
```

[实现](../../internal/sliceops/policy.go#L133) · [行为专题](../mapreduce.md)：WithDefaultPool 标记使用默认协程池，执行时自动创建并在 Wait 后 Close。

**源码说明与使用片段**（原文）：

WithDefaultPool 标记使用默认协程池，执行时自动创建并在 Wait 后 Close。


**参数说明**：无显式参数。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithDefaultPool()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回持有同一池引用的策略副本，不创建池、不关闭此前引用。WithPool 借用，WithOwnedPool 接管关闭，WithDefaultPool 标记终端内部按 IO() 创建，忽略策略 Worker；借用及接管池同样由其自身容量控制并发。结果类型须匹配 *Pool[R]。错误动态类型不一定报错，可能回退原生执行或创建默认池，应使用明确类型。池化路径优先于其他配置，不与外部任务混用。

## `Policy.WithOwnedPool`

```go
func (p Policy) WithOwnedPool(pool any) Policy
```

[实现](../../internal/sliceops/policy.go#L126) · [行为专题](../mapreduce.md)：WithOwnedPool 注入外部协程池，但内部接管生命周期。

**源码说明与使用片段**（原文）：

WithOwnedPool 注入外部协程池，但内部接管生命周期。 终端方法执行完成后自动 Close，无需用户 defer。


**参数说明**：

- `pool` (`any`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithOwnedPool(pool)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回持有同一池引用的策略副本，不创建池、不关闭此前引用。WithPool 借用，WithOwnedPool 接管关闭，WithDefaultPool 标记终端内部按 IO() 创建，忽略策略 Worker；借用及接管池同样由其自身容量控制并发。结果类型须匹配 *Pool[R]。错误动态类型不一定报错，可能回退原生执行或创建默认池，应使用明确类型。池化路径优先于其他配置，不与外部任务混用。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.pool = pool
```

## `Policy.WithPool`

```go
func (p Policy) WithPool(pool any) Policy
```

[实现](../../internal/sliceops/policy.go#L118) · [行为专题](../mapreduce.md)：WithPool 注入外部协程池（*pool.Pool[R]），启用池化执行。

**源码说明与使用片段**（原文）：

WithPool 注入外部协程池（\*pool.Pool\[R]），启用池化执行。 用户自行管理池的生命周期（需 defer p.Close()）。


**参数说明**：

- `pool` (`any`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithPool(pool)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回持有同一池引用的策略副本，不创建池、不关闭此前引用。WithPool 借用，WithOwnedPool 接管关闭，WithDefaultPool 标记终端内部按 IO() 创建，忽略策略 Worker；借用及接管池同样由其自身容量控制并发。结果类型须匹配 *Pool[R]。错误动态类型不一定报错，可能回退原生执行或创建默认池，应使用明确类型。池化路径优先于其他配置，不与外部任务混用。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.pool = pool
```

## `Policy.Worker`

```go
func (p Policy) Worker(n int) Policy
```

[实现](../../internal/sliceops/policy.go#L102) · [行为专题](../mapreduce.md)：Worker 覆盖并发度，n <= 0 时使用 core.IO()。

**源码说明与使用片段**（原文）：

Worker 覆盖并发度，n \<= 0 时使用 core.IO()。


**参数说明**：

- `n` (`int`)：Worker 覆盖并发度，n <= 0 时使用 core.IO()。

**返回值签名**：`Policy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按值返回策略副本，必须保存返回值；Worker 非正按 IO()，Shard 非正按 max(2,GOMAXPROCS)，其余保存原值。Worker 不切换 serial 标志，Seq().Worker(n) 仍标记串行；实际采用哪些字段由终端路径决定。以 Seq/Par/DefPar 为起点，零 Policy 在部分终端会整体替换为 DefPar，单独设置 TO/FF/Chunk 等可能被丢弃。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
p.concurrency = n
```

