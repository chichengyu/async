# Map/Reduce（数据并行）文档

## 概述

Slice 模块提供切片元素的并发/串行处理，支持 Map / ForEach / Reduce / Stream 四种终端操作。通过 `SliceBuilder[T,R]` 链式构建器配置策略和模式。

原生并行 Map 及水平分片 Map/Each 的业务回调调用 `runtime.Goexit` 时，该项记录零值与 `async.ErrTaskGoexit`，其余索引由后继 worker 继续处理；每项只执行一次。分片 Each 的失败数包含该项。FailFast 将其作为错误并按原规则取消后续项；未启用 FailFast 的原生 Map 顶层错误仍可为 nil，须逐项检查 `Err`。普通并行 Map 不额外设置 `Occupied`，分片路径保持 `Occupied=true`。

生产回调应正常返回 `error`。同步串行路径仍会结束调用 goroutine，包括普通并行 Each 在有效并发度为 1 时采用的串行分支；Reduce 同样不转换此退出。单项超时 context 会在退出时清理，且超时始终需要业务协作，不能中断阻塞回调。

池化 Map/Each、流式 Map/Each 的业务 `runtime.Goexit` 由 Pool/Group 登记错误并接替 worker，且触发已启用的 FailFast，阻止后续未开始业务。流式结果仍按完成顺序发送，取消时可以丢弃尚未转发项；持续消费到关闭，或取消父 context 解除背压。

**两种入口**：

| 入口 | 说明 | 终端方法 |
|------|------|----------|
| `async.Slice[T](ctx, items)` | 同类型切片构建器（R=T） | `.Map(fn) / .ForEach(fn) / .Reduce(init, fn) / .Stream(fn, buf)` |
| `async.SliceWith[R](ctx, items)` | 跨类型切片构建器（T→R） | `.Map(fn) / .ForEach(fn) / .Reduce(init, fn) / .Stream(fn, buf)` |

**三种模式**：

| 模式 | 获取方式 | 说明 |
|------|------|------|
| 默认并行模式 | `async.Slice(ctx, items)` 直接使用 | 默认并发度 IO |
| `.Parallel()` | `async.Slice(ctx, items).Parallel()` | 显式并行+可配置并发/分片/池化等 |
| `.Serial()` | `async.Slice(ctx, items).Serial()` | 串行模式，不并发 |

> **⚠️ 默认就是并行模式**
>
> `async.Slice(ctx, items)` 创建后直接就是并行模式（默认并发度 IO）。不需要调用 `.Parallel()` 就已经是并行。`.Parallel()` 用于在串行模式后切换回并行模式。
>
> **⚠️ 模式切换**
>
> - `SliceBuilder` → `.Serial()` → `SerialSlice` → `.ToParallel()` → `ParallelSlice`
> - `SliceBuilder` → `.Parallel()` → `ParallelSlice` → `.ToSerial()` → `SerialSlice`
> - 这些切换会保留上下文和当前数据，但重置执行策略；请在切换后重新设置超时、FailFast 等配置。
> - 如果切换前设置了 `PoolAuto(p)`，新构建器不会接管该池；仅执行新构建器不会关闭旧池，调用方应确保旧池被关闭。
> - `SliceBuilder` 默认已是并行，不需要 `.Parallel()` 即可使用并行配置方法
>
> **⚠️ 串行模式下不暴露并行配置方法**
>
> `SerialSlice` 只有 `FailFast()` / `NoFailFast()` / `Timeout()` / `DefaultTimeout()` / `Logger()`，没有 `Worker()` / `Pool()` / `Shards()` / `Chunk()` 等并行配置。如需并行配置，先 `.ToParallel()` 切换到 `ParallelSlice`。
>
> **⚠️ Reduce 始终串行**
>
> `Reduce(initial, fn)` 不受策略中并发配置影响，始终串行聚合。

---

## 目录

- [SliceBuilder 链式方法速查表](#slicebuilder-链式方法速查表)
- [SerialSlice 方法速查表](#serialslice-方法速查表)
- [ParallelSlice 方法速查表](#parallelslice-方法速查表)
- [数据操作方法](#数据操作方法)
- [终端操作：Map](#终端操作map)
- [终端操作：ForEach](#终端操作foreach)
- [终端操作：Reduce](#终端操作reduce)
- [终端操作：Stream（流式）](#终端操作stream流式)
- [SliceResult 结果访问](#sliceresult-结果访问)
- [ForEachResult 结果访问](#foreachresult-结果访问)
- [模式切换完整示例](#模式切换完整示例)
- [默认并行模式](#默认并行模式)
- [Parallel 模式（显式并行）](#parallel-模式显式并行)
- [SerialSlice 方法速查表（串行）](#serialslice-方法速查表)
- [跨类型 Map（SliceWith）](#跨类型-mapslicewith)
- [MapBatch / ForEachBatch（批量处理）](#mapbatch--foreachbatch批量处理)
- [Pool 注入](#pool-注入)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)
- [自定义日志](#自定义日志)
- [Map 容器（K-V 泛型容器）](#map-容器k-v-泛型容器)

---

## SliceBuilder 链式方法速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.Slice[T](ctx, items)` | 同类型切片构建器（默认并行） |
| `async.SliceWith[R](ctx, items)` | 跨类型切片构建器（默认并行） |

### 配置方法（SliceBuilder 可直接使用）

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文（自动注入 TraceID） | `ctx` 参数 |
| `.Worker(n)` | 并发度（n ≤ 0 恢复 IO 并发度） | `IO()` |
| `.DefaultWorker()` | 使用默认 IO 并发度 | — |
| `.DefaultPool()` | 标记终端执行时创建默认协程池，并自动管理生命周期 | — |
| `.Pool(p)` | 注入外部协程池（用户管理生命周期） | 无 |
| `.PoolAuto(p)` | 注入外部协程池（自动 Close） | 无 |
| `.Timeout(d)` | 单任务超时 | 不设置 |
| `.DefaultTimeout()` | 恢复默认超时（30s） | — |
| `.FailFast()` | 失败触发取消，其余回调需响应 context | 关闭 |
| `.DefaultFailFast()` | 关闭 FailFast | — |
| `.Shards(n)` | 水平分片数（n ≤ 0 用 GOMAXPROCS） | 无分片 |
| `.DefaultShard()` | 使用默认分片（GOMAXPROCS，最少 2） | — |
| `.Chunk(size)` | 分块处理大小 | 无分块 |
| `.DefaultChunk()` | 使用默认分块（100） | — |
| `.Buf(size)` | Stream 缓冲区大小 | 0（自适应） |
| `.DefaultBuf()` | 使用默认流式缓冲 | — |
| `.Logger(l)` | 设置当前构建器的日志器（仅局部生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |

使用 `.Pool(p)` 时，正数的 `.Timeout(d)` 仅应用于当前切片操作中的每个任务，优先于池原有的任务超时，不会更改池的超时配置。`.FailFast()` 在任务函数返回错误或发生 panic 时取消当前操作的其他任务；任务执行前被池拒绝或丢弃不会触发取消。执行后借用池的复位规则见下表，调用方负责关闭它。
池化终端若执行等待，会等待池中所有已提交任务，包括调用方单独提交的任务；借用池随后通过 `ResetWait` 清空结果和统计计数。因此操作期间不要让其他调用方并发提交、等待或复用该池；需要保留池结果或统计时，应在使用这些操作前另行处理。池的上下文若因等待而取消，复位不会重建它，下一批使用池上下文前须重新配置。下表针对类型匹配的有效池，接管指 `.PoolAuto(p)` 或 `WithOwnedPool(p)`。

| Slice Runner 终端 | 借用池：非空输入 | 借用池：空输入 | 接管池 |
|---|---|---|---|
| Map / MapBatch | 等待并 ResetWait | 直接返回，不等待、不复位 | 空/非空均关闭 |
| ForEach / Each / ForEachBatch / EachBatch | 等待并 ResetWait | 仍等待并 ResetWait | 空/非空均关闭 |
| Stream | 等待并 ResetWait 后关闭输出 | 仍等待并 ResetWait 后关闭输出 | 空/非空均在输出关闭前关闭池 |
| Builder.Reduce / Runner.Fold | 串行直接聚合，不使用或复位池 | 返回 initial，不使用或复位池 | 返回或 panic 展开时关闭已接管池 |

`SerialSlice.Reduce` 直接调用独立 Reduce，不承担池关闭；旧 Slice 的无值 ForEach/Each 也不使用策略中的池。空输入不应作为无副作用的池探测：选择不同终端可能清理或关闭已有池。
池设置了 `OverflowDrop` 或 `OverflowError` 且等待队列已满时，被拒绝的元素结果包含 `ErrQueueOverflow`；Map 的汇总错误在没有优先报告的其他错误时反映该错误，ForEach 将其计入失败数，Stream 会发送错误结果。取消上下文可能使 Stream 提前停止发送结果。
池化 Map 和 ForEach 的 FailFast 汇总优先报告触发取消的回调错误，避免其他任务的 `context.Canceled` 掩盖原始错误；并发失败时先记录的回调错误取决于调度。

串行 FailFast 提前停止后，未执行的元素标记为 `context.Canceled`；它们不计入成功值，ForEach 的失败数包含这些取消的元素。
Runner 的输入 context 若在调用前已取消，回调是否运行取决于终端实际选中的调度路径。下表假设非空输入，且回调忽略取消并正常返回；只描述预取消，执行中取消时还会受已经启动的任务影响。

| 实际路径 | Map 回调 | Each / ForEach 回调 |
|---|---|---|
| 普通串行（包括串行 FailFast） | 全部调用 | 全部调用 |
| 普通并行，未启用 FailFast | 全部调用，不预检查 context | 有效并行度大于 1 时由 Group 拒绝；裁剪至 1 时同步调用 |
| 普通并行，启用 FailFast | 预检查取消，不调用 | 有效并行度大于 1 时由 Group 拒绝；裁剪至 1 时同步调用 |
| 分片、正数超时或类型匹配的池化路径 | 在回调前拒绝 | 在回调前拒绝 |
| 正数 Chunk 选中的路径 | 由 Group 拒绝 | 逐块按有效并行度决定；为 1 时同步调用，否则由 Group 拒绝 |

有效并行度会按当前输入长度裁剪；Chunked Each 按当前块长度裁剪。因此 Par(2).Chunk(1) 可对全部元素调用，Par(2).Chunk(3) 对四项输入可只调用最后一项。Each 的分片、正数 Timeout、并行 FailFast 路径优先于 Chunk；Map 的 Chunk 则先于 Timeout/FailFast 分派，并在该路径中应用这些设置。MapBatch/EachBatch 先将元素转为批次，再携带同一策略调度，因而输入长度与并行度裁剪此时按批次数。例如四项输入、Par(2).Chunk(4).FF() 的 EachBatch 只有一个批次，会串行回退并调用一次；MapBatch 则在 Group 中拒绝。统计和结果槽仍按各路径规则生成，不能据 Total 推算回调次数。生产回调应检查传入 context；若业务要求同步回退或普通并行 Map 也跳过已取消工作，应在回调入口显式检查 ctx.Err()。
公开构建器对照见 [预取消调度示例](tests/public_examples_test.go) 中的 Example_slicePreCanceledDispatch，包含回调检查取消及结果错误统计。
未使用池、分片、分块或超时的并行 Map 在 FailFast 回调报错或 panic 后取消操作；panic 只记录在发生 panic 的元素上，因取消而未执行的元素记录上下文取消错误。已经开始的回调需要自行响应上下文取消。
该路径的 `Error()` 返回首次触发 FailFast 的回调错误；多个回调同时失败时，具体哪个错误先记录取决于调度。
分片执行时，若上下文取消导致元素未执行，ForEach 会将其计入失败；没有回调错误时，汇总错误反映该取消。FailFast Map 同样会在没有回调错误时报告取消。

未启用 FailFast 时，Map 回调发生 panic 会转换为对应元素的错误，其他元素仍会继续处理，包括同一并行分块中的后续元素。

### 模式切换

| 方法 | 返回类型 | 说明 |
|------|------|------|
| `.Serial()` | `*SerialSlice[T,R]` | 切换为串行模式 |
| `.Parallel()` | `*ParallelSlice[T,R]` | 切换为并行模式 |

### 终端方法

Slice 的错误汇总顺序随执行路径而异。ForEach 的无池超时及 FailFast 路径通过 NoResult 收集记录；启用 FailFast 时，触发失败后的取消拒绝可先于触发错误写入，所以汇总错误可能是 `context.Canceled`，即使实际回调因 `DeadlineExceeded` 失败。不能用汇总错误推断首次失败的时间顺序；需要完整诊断时保存业务错误或检查入口提供的逐项结果。回调应响应 context，超时不能强制终止不配合的业务。

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Map(fn)` | `*SliceResult[R]` | 并行/串行 Map 处理 |
| `.MapBatch(fn)` | `*SliceResult[R]` | 分块 Map（需配合 Chunk） |
| `.ForEach(fn)` | `*ForEachResult` | 并行/串行 ForEach |
| `.ForEachBatch(fn)` | `*ForEachResult` | 分块 ForEach（需配合 Chunk） |
| `.Reduce(initial, fn)` | `(R, error)` | 串行聚合（始终串行） |
| `.Stream(fn, bufSize)` | `<-chan Result[R]` | 流式 Map |

---

## SerialSlice 方法速查表

### 配置方法

| 方法 | 说明 |
|------|------|
| `.FailFast()` | 启用 FailFast |
| `.NoFailFast()` | 关闭 FailFast |
| `.Timeout(d)` | 单任务超时 |
| `.DefaultTimeout()` | 恢复默认超时 |
| `.Logger(l)` | 注入自定义日志 |

### 模式切换

| 方法 | 返回类型 | 说明 |
|------|------|------|
| `.ToParallel()` | `*ParallelSlice[T,R]` | 切换到并行模式 |

### 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Map(fn)` | `*SliceResult[R]` | 串行 Map |
| `.ForEach(fn)` | `*ForEachResult` | 串行 ForEach |
| `.Reduce(initial, fn)` | `(R, error)` | 串行聚合 |
| `.Stream(fn, bufSize)` | `<-chan Result[R]` | 串行流式 |

> **⚠️ SerialSlice 没有 Worker/Pool/Shards/Chunk/Buf/MapBatch/ForEachBatch 方法**
>
> 串行模式不涉及并发，因此这些并行配置方法和批量处理方法不可用。需要时先 `.ToParallel()` 切换。

---

## ParallelSlice 方法速查表

### 配置方法

| 方法 | 说明 |
|------|------|
| `.Worker(n)` | 并发度 |
| `.DefaultWorker()` | 默认 IO 并发度 |
| `.Pool(p)` | 注入外部协程池 |
| `.PoolAuto(p)` | 注入外部协程池（自动 Close） |
| `.DefaultPool()` | 标记终端执行时创建并关闭默认池 |
| `.FailFast()` | 启用 FailFast |
| `.NoFailFast()` | 关闭 FailFast |
| `.Timeout(d)` | 单任务超时 |
| `.DefaultTimeout()` | 恢复默认超时（30s） |
| `.Shards(n)` | 水平分片数 |
| `.DefaultShard()` | 默认分片数 |
| `.Chunk(size)` | 分块大小 |
| `.DefaultChunk()` | 默认分块（100） |
| `.Logger(l)` | 注入自定义日志 |

### 模式切换

| 方法 | 返回类型 | 说明 |
|------|------|------|
| `.ToSerial()` | `*SerialSlice[T,R]` | 切换到串行模式 |

### 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Map(fn)` | `*SliceResult[R]` | 并行 Map |
| `.MapBatch(fn)` | `*SliceResult[R]` | 分块并行 Map |
| `.ForEach(fn)` | `*ForEachResult` | 并行 ForEach |
| `.ForEachBatch(fn)` | `*ForEachResult` | 分块并行 ForEach |
| `.Stream(fn, bufSize)` | `<-chan Result[R]` | 并行流式 |

---

## 数据操作方法

以下数据操作由三个构建器（`SliceBuilder`、`ParallelSlice`、`SerialSlice`）共享。变换方法返回构建器以支持链式调用；查询和分块方法返回查询结果，不能继续链式调用。

### 排序

| 方法 | 说明 |
|------|------|
| `.Sort(cmp)` | 非稳定排序 |
| `.StableSort(cmp)` | 稳定排序 |
| `.IsSorted(cmp)` | 判断是否已排序 |
| `.Reverse()` | 原地反转 |

比较器必须给出一致的严格弱序，返回负数/零/正数分别表示小于/等价/大于，搜索前须用同一规则排序。浮点建议使用 `cmp.Compare[float64]`：它将 NaN 排在其他数之前，多个 NaN 互相等价，正负零也等价；降序可比较 `cmp.Compare(b,a)`。不要用 `int(a-b)` 比较浮点数，舍入、无穷和 NaN 都会破坏比较规则。`StableSort` 保留等价元素的原顺序及位模式；`Min`/`Max` 在等价极值中保留原输入首次代表，`BinarySearch` 返回首个等价位置。生产中需区分 NaN 位模式或正负零时，应显式定义一致规则并在排序、搜索和去重中统一使用。可执行示例见 `Example_sliceFloatComparison`。

### 过滤与去重

| 方法 | 说明 |
|------|------|
| `.Filter(pred)` | 保留满足条件的元素；并行策略且元素数 ≥1000 时并行执行谓词 |
| `.DeleteFunc(pred)` | 删除满足条件的元素（与 Filter 语义相反） |
| `.Compact(eq)` | 移除相邻重复 |
| `.Dedup(eq)` | 按 `eq` 全局去重，保留每组首次出现的值；支持不可比较类型 |

`Compact(eq)` 只移除相邻重复元素，长度至少为 2 时 eq 必须非 nil，否则 panic；`Dedup(eq)` 和旧入口 `SliceData.DedupFunc(eq)` 按比较函数全局去重，并保留首次出现的代表值。`eq(a,b)` 应满足相等关系，返回 true 表示同一组。无参数以外的状态由调用方保证稳定；方法同步执行并原地压缩切片。Dedup 比较器为非 nil 时最坏需要 O(n²) 次比较，大批量生产数据建议自行按业务键建立哈希索引。仅 Dedup 的 `eq=nil` 使用 Go 相等性判重，此时元素必须可作为 map key；`==` 和 nil 回退都会保留每个 NaN，但正负零合为首次代表。若需按排序等价去重，应传入 `func(a,b float64) bool { return cmp.Compare(a,b)==0 }`，此时多个 NaN 也合为首次代表。不要假定排序时等价就会被另一种 eq 合并。

```go
values := async.Slice(ctx, []string{"Alpha", "beta", "ALPHA"}).
    Dedup(strings.EqualFold).Values() // [Alpha beta]，需 import "strings"
```

#### 过滤的数组所有权与异常

`Filter(pred)` 的参数 `pred` 对当前每个元素返回是否保留；`DeleteFunc(pred)` 返回 true 表示删除。正常返回时各元素调用一次且结果顺序与输入一致。并行策略且当前长度至少为 1000 时，谓词由 worker 并发执行，结果槽位另行分配，原输入槽位不改写；即便 `.Worker(1)` 也采用该路径。当前长度小于 1000、显式串行或旧 `SliceData.Filter` 使用原地压缩，原输入的前部槽位可能被覆盖，旧视图的长度不会随构建器缩短，尾部也不清零。因此，不能仅凭 `Filter` 方法名或并行配置推断输入是否保持原样。

```go
source := []int{0, 1, 2, 3}
owned := append([]int(nil), source...) // 先复制元素槽位，隔离小输入的原地压缩
filtered := async.Slice(ctx, owned).Serial().Filter(func(v int) bool {
    return v%2 == 1
}).Values() // [1 3]，source 仍为 [0 1 2 3]
```

生产中若要保留输入，无论当前长度多少，都应在过滤前复制元素槽位；指针、map 和切片元素的内部内容仍共享，需按业务再复制。`Values()` 在过滤完成后复制，不能撤销过滤已经对输入做出的改写。可执行对照见 [过滤所有权示例](tests/public_examples_test.go) 中的 `Example_sliceFilterOwnership`。

这些纯数据方法不检查任务 context，不使用 `Timeout` 或 `FailFast`。同步谓词 panic 会原样传播：内部切片长度尚未更新，但已经压缩的前部槽位不会回滚；recover 后继续复用可能读到重复或部分改写的内容，应重新建立有效输入。并行路径不捕获 worker panic，调用方 recover 无法捕获其他 goroutine 的 panic，会导致进程退出。应使用不会 panic 且可安全并发调用的谓词，需要将异常转换为业务错误时选用有相应约定的 `Map` / `ForEach`。

### 增删改

| 方法 | 说明 |
|------|------|
| `.Append(vals...)` | 末尾追加 |
| `.Prepend(vals...)` | 头部插入 |
| `.Insert(idx, vals...)` | 指定位置插入 |
| `.Delete(i)` | 删除单个元素 |
| `.DeleteRange(i, j)` | 删除范围 |
| `.Replace(i, j, vals...)` | 替换范围 |
| `.Take(n)` | 保留前 n 个 |
| `.Drop(n)` | 丢弃前 n 个 |
| `.SliceRange(i, j)` | 截取子切片 |
| `.Clip()` | 容量大于长度时复制到新数组，使 cap == len；旧数组回收取决于其他引用 |
| `.Grow(n)` | 确保容量能容纳 len+n 个元素，保持长度 |
| `.Shuffle()` | 使用全局伪随机源随机排列，可能保持原序或重复上次结果 |
| `.Repeat(n)` | 重复拼接 |

`Repeat` 对空输入直接返回并保留空视图的容量和底层数组，不随重复次数循环。非空输入在 n<=0 时清空，n==1 时无操作；n>1 时按原顺序重复追加，容量足够时会复用数组并改写共享视图可见的尾部槽位。元素槽位独立，指针等引用内容仍共享；生产中应先校验非空输入的总元素数和内存预算，该方法不提供取消或分配失败恢复。
空视图与引用元素的可执行对照见 [Repeat 示例](tests/public_examples_test.go) 中的 Example_sliceRepeatEmptyAndReferences。

`Insert` 将负索引视为 0，将大于长度的索引视为末尾；插入值可以引用当前切片的底层数组。`Items()` 返回直接引用，后续原地修改可能改变该数组；需要独立切片时使用 `Values()`。
`Append` 和 `Insert` 在容量足够时可复用接收者数组；`Prepend(vals...)` 以 vals 为追加目标，容量足够时结果改为引用参数数组，并覆盖 vals 原长度之外的槽位。即使 vals 是长度为零但有容量的视图，也可能发生这种覆盖。要保留参数数组的其他视图，应先复制参数槽位。

`Delete` 越界时不操作；`DeleteRange` 和 `SliceRange` 使用左闭右开区间，并分别将负的起点修正为 0、过大的终点修正为长度。修正后起点不小于终点时，前者不操作，后者清空。`Replace` 同样修正负起点和过大终点，但负终点仍可能导致 panic，应确保 `0 <= i <= j <= Len()`。链式 SliceBuilder、SerialSlice、ParallelSlice 的 `Grow(n)` 在 n 为负数时会 panic；旧 SliceData 的 `Grow(n)` 则对非正数无操作。

`Take(n<=0)`、`Drop(n>=Len())` 和修正后空区间的 `SliceRange` 都以当前视图的 `[:0]` 清空，保留原起点和容量；它们不清零底层数组，也不把空视图移到数组末端。非空的 `Drop` 和 `SliceRange` 才将起点向后移动，并相应减少容量。边界比较对最小/最大 `int` 也适用：`Take(MaxInt)` 不截断，`Drop(MinInt)` 不改变视图，`SliceRange(MinInt, MaxInt)` 保留当前全部元素，逆序区间清空；`SliceBuilder.Slice(i,j)` 是 `SliceRange` 的兼容命名。生产若需释放被排除元素保留的大数组，可在截取后使用 `Clip`，并释放其他旧视图；若需保留输入槽位，先复制再操作。这些方法同步返回接收者，不采用 worker、任务超时或取消策略。
`Replace` 支持与接收者重叠的替换值；缩短时清零新长度到旧长度之间的槽位，旧长度的共享视图会看到零值。这与下面 Delete/DeleteRange 不清零尾部的规则不同；需要保留旧槽位内容时先复制接收者。
参数数组复用与缩短尾部的可执行对照见 [共享数组变更示例](tests/public_examples_test.go) 中的 Example_sliceMutationSharedArrays。

`Grow` 当前通过追加零值后恢复长度来确保容量；如果复用底层数组，它会写入原长度外的槽位，其他更长的共享视图可能观察到这些零值。`Delete`、`DeleteRange` 和串行 `Filter`/`Compact` 缩短视图时不清零尾部，引用元素可能继续保留内存；大量缩短后可按内存预算调用 `Clip`，旧数组仍须等全部引用释放。数据操作需要所有权隔离时，先复制元素槽位，再按业务需要复制引用内容。

```go
items := make([]int, 4, 8)
copy(items, []int{1, 2, 3, 4})
b := async.Slice(ctx, items)
b.Insert(1, b.Items()[2:]...)
fmt.Println(b.Values()) // [1 3 4 2 3 4]
```

### 查询

| 方法 | 说明 |
|------|------|
| `.Len()` | 元素数量 |
| `.IsEmpty()` | 是否为空 |
| `.First()` | 第一个元素 |
| `.Last()` | 最后一个元素 |
| `.Items()` | 直接引用（慎改） |
| `.Values()` | 浅拷贝 |
| `.Clone()` | 等价于 Values() |
| `.Contains(pred)` | 是否包含（并行策略且 ≥100 时并行） |
| `.Index(pred)` | 第一个匹配的索引 |
| `.Find(pred)` | 第一个匹配的元素 |
| `.FindLast(pred)` | 最后一个匹配的元素 |
| `.All(pred)` | 是否全满足；空切片返回 true |
| `.Any(pred)` | 是否有满足；空切片返回 false |
| `.Count(pred)` | 满足条件的数量 |
| `.Max(cmp)` / `.Min(cmp)` | 返回最大/最小元素及是否存在；空切片返回零值和 false |
| `.BinarySearch(target, cmp)` | 在已按相同比较规则排序的数据中返回匹配位置或插入位置及是否找到 |
| `.Chunked(size)` | 按每块最多 size 个元素切分；size 非正或输入为空返回 nil |
| `.ChunkedN(n)` | 按 `ceil(len/n)` 的块大小切分，最多 n 块；n 非正或输入为空返回 nil |
| `.Split(pred)` | 返回满足和不满足条件的两个切片，各自保持原有顺序 |

`All`、`Any` 和 `Count` 在并行策略且元素数至少为 100 时并行执行谓词；谓词应能安全并发调用。这些纯数据操作及 Filter/DeleteFunc 不恢复 panic；并行路径中发生 panic 会终止进程，调用方 recover 不能捕获其他 goroutine 的 panic。回调应先校验输入；需要业务错误处理时使用 Map/ForEach。纯数据操作不使用策略 Timeout/FailFast。`Chunked` 和 `ChunkedN` 的子切片引用当前底层数组，修改子切片可能影响构建器；需要独立数据时先复制。`Values` 和 `Clone` 是浅拷贝，元素内部引用仍可能共享。

这些子切片保留原数组的剩余容量，向一块追加元素可能覆盖下一块。需要向块中追加时，先复制该块，或使用三下标切片将其容量限制为长度（例如 `part = part[:len(part):len(part)]`）；后者只隔离后续追加分配，不隔离已有元素或其内部引用。

`ChunkedN` 使用固定块长，不保证恰好 n 块：10 项按 n=6 切分时块长为 2，实际为 5 块。`Split` 则为两组分别分配元素槽位，改写返回切片的槽位不会改变输入；指针、map 或切片元素内部的对象仍共享，空分组返回 nil。需要深度隔离时按业务复制对象。

```go
b := async.Slice(ctx, []int{1, 2, 3, 4, 5})
chunks := b.ChunkedN(2) // [[1 2 3] [4 5]]
even, odd := b.Split(func(v int) bool { return v%2 == 0 })
idx, found := b.BinarySearch(3, func(a, b int) int { return cmp.Compare(a, b) })
_, _, _, _ = chunks, even, odd, idx
_ = found
```

---

## 终端操作：Map

### 基本 Map（默认并行）

```go
results := async.SliceWith[string](ctx, []int{1, 2, 3, 4, 5}).
    Worker(8).
    Map(func(ctx context.Context, v int) (string, error) {
        return strconv.Itoa(v * 2), nil
    })

if results.Err() {
    log.Fatal(results.Error())
}
vals := results.Values()  // ["2", "4", "6", "8", "10"]
```

### 并行 + FailFast + 超时

```go
results := async.SliceWith[[]byte](ctx, urls).
    Worker(16).
    FailFast().
    Timeout(5 * time.Second).
    Map(func(ctx context.Context, url string) ([]byte, error) {
        return httpGet(ctx, url)
    })
```

### 并行 + 分片

```go
results := async.Slice[int](ctx, bigData).
    Worker(8).
    Shards(4).   // 4 分片并行处理
    Map(heavyCompute)
```

---

## 终端操作：ForEach

无返回值的遍历操作，适合写数据库、发消息等副作用操作。

旧版独立函数与 Slice 方法的同名入口存在差异：独立的 sliceops.ForEachChunked* 逐元素调用业务函数，但按批次返回总数、失败数和结果，一批内多个错误只算一个失败批次；超时变体为整个批次设置共享期限。外层批次和批内分别使用并发上限，总在途回调可能超过传入的 concurrency。旧版 Slice.ForEachChunked* 方法则按元素统计，超时按元素设置。生产代码优先使用根包构建器的 ForEach / ForEachBatch，按其明确的统计单位处理结果；可执行对照见 [chunked_example_test.go](../internal/sliceops/chunked_example_test.go)。

旧版 MapChunked 的边界也要按入口区分：独立函数的 concurrency 非正时使用 1，batchSize 非正时不执行任何元素；Slice 方法的 concurrency 非正时使用 IO 默认值，batchSize 非正时退回普通逐元素执行。独立 MapChunkedWithTimeout(..., 0, fn) 清除任务超时；同名 Slice 方法在正数 batchSize 路径中保留底层 Group 创建时的全局默认超时。生产调用请显式给出正数并发、块大小和预期超时，避免依赖这些兼容差异。

```go
r := async.Slice[string](ctx, records).
    Worker(16).
    ForEach(func(ctx context.Context, record string) error {
        return db.Insert(ctx, record)
    })

if r.Err() {
    log.Printf("失败 %d / %d", r.FailCount(), r.Total())
}
```

---

## 终端操作：Reduce

串行聚合，不受并发配置影响。

```go
sum, err := async.Slice[int](ctx, nums).
    Reduce(0, func(ctx context.Context, acc int, v int) (int, error) {
        return acc + v, nil
    })
```

---

## 终端操作：Stream（流式）

边执行边通过 channel 返回结果，适合大批量数据实时处理。

消费者较慢时，结果缓冲区满会使任务等待，直到结果被消费。提前停止消费时应取消传入的 context，以解除阻塞的结果发送。取消后，尚未发送的结果可能被丢弃；已开始的回调仍需自行响应 context 或返回。

`.Timeout(d)` 为每个 Stream 回调设置独立的上下文截止时间。回调需要检查该上下文并返回错误，超时不会强制中止忽略 context 的函数；启用 FailFast 时，回调返回的超时错误会取消其他任务。

池化 Stream 的结果投递使用父 context。FailFast 取消派生任务 context 后，触发错误仍需等待消费者腾出缓冲；提交拒绝产生的取消结果可能先到，不能用接收位置推断最先发生的业务错误。单次回调期限届满也不会解除投递背压，已返回的成功值不会因此改成超时错误。提前停止消费须取消父 context，不能仅依赖 FailFast 或 Timeout；父取消会使尚未投递的结果可能丢失，而通道关闭、借用池复位或接管池关闭仍须等待已开始回调实际返回。可执行边界见 [Stream 投递与清理测试](../internal/sliceops/stream_delivery_contract_test.go)。

配置 `.Pool(p)` 后，Stream 使用该池执行任务，并在结果通道关闭前等待和复位池；调用方负责关闭池。`.PoolAuto(p)` 则在通道关闭前关闭池。流执行期间不要并发复用同一个池，且应持续消费结果或取消传入的 context。
提前停止时，取消后仍消费到关闭，再用未取消的 owner context 复用借用池；完整用法见 [公开示例](tests/public_examples_test.go) 中的 Example_poolStreamEarlyStop。

池自身启用 `WithStreaming` 后的 `StreamResults()` 是另一条独立通道，满时直接丢弃并增加 `StreamDropped`；它的丢弃不截断 Slice/Map Stream 的完成回调输出。借用池在 Stream 收尾时执行 `ResetWait`，会替换池自身的流通道并清零 `StreamDropped` 等批次计数；旧通道引用只代表旧批次，不能在复位后用计数为零推断旧批次未丢结果。

```go
ch := async.Slice[int](ctx, items).
    Worker(16).
    Buf(1024).            // channel 缓冲
    Stream(func(ctx context.Context, v int) (int, error) {
        return process(ctx, v)
    }, 0)                // bufSize=0 使用 Buf() 设置的值

for res := range ch {
    if res.Err != nil {
        log.Printf("错误: %v", res.Err)
        continue
    }
    fmt.Println(res.Value)
}
```

---

## SliceResult 结果访问

Slice 构建器和 Runner 的 `MapBatch` / `ForEachBatch`（Runner 名称为 `EachBatch`）先按 `Chunk(size)` 切分输入。参数 `size <= 0` 时每元素一批；正数大于输入长度时整个非空输入只有一批，包括 `MaxInt`，不会按该参数分配 MaxInt 个槽位。空输入不调用业务。每次回调接收一批 `[]T`，MapBatch 每批保留一个结果，ForEachBatch 的 Total/FailCount/OkCount 也按批次计数。独立旧版 Chunk 函数的非正参数规则不同，见前文。

MapBatch 回调返回值和 error 时，逐批 `Results()` 保留两者，`Values()` 只列成功批次；panic 变为 PanicError 且该槽位值为零。`FailValues()` 按输入批次顺序展开失败批次的全部原始元素，不能把它理解为逐元素调用记录或事务回滚证明。池的结果保留上限及环形缓冲不截断这里的批次结果和统计。使用示例见 [批次失败映射示例](tests/public_examples_test.go) 的 Example_sliceBatchFailureProjection，极值和错误组合见 [批处理契约](../internal/sliceops/batch_extreme_contract_test.go)。

生产中按数据库参数上限、单批内存和可接受延迟选择有限块大小；批次仍共享输入底层数组，回调期间不要追加到批次或修改其他批次可见的数据。失败批次可能已产生部分副作用，重试前须按业务保证幂等或确认处理状态。超时按整批回调设置 context 期限，不能强制结束业务或自动撤销副作用。

| 方法 | 说明 |
|------|------|
| `.Error()` | 优先返回操作整体错误；否则按结果顺序返回第一个元素错误 |
| `.Ok()` | `Error() == nil`，空结果且没有汇总错误时也为 true |
| `.Err()` | 有错误返回 true |
| `.Values()` | 保留槽位中 Err 为 nil 的值组成的新切片，不检查 Occupied |
| `.Errors()` | 先放非摘要顶层错误，再按槽位顺序放逐项错误；标记为摘要的顶层错误不重复添加，相同 error 值仍按失败项分别保留 |
| `.FailValues()` | 失败输入值（原始 T 类型元素）；`MapBatch` 返回失败批次中的全部元素 |
| `.Results()` | 内部保留 Result[R] 切片的直接引用 |
| `.Must()` | 有错误时 panic |
| `.Unwrap()` | `([]R, error)` |
| `.Len()` | 保留结果槽位数，包含失败项和未占用空槽 |
| `.First()` | 首个 Err 为 nil 的槽位值，不检查 Occupied |

这些查询只反映保留结果与保存的汇总错误，不能单凭 `Ok()` 或 `Values()` 长度证明全部输入均已执行。取消、FailFast、结果上限及稀疏空槽需按对应入口核对；`Values()` 和 `First()` 可能把零值空槽作为无错误值返回。`Results()` 与 `FailValues()` 返回内部引用，需要隔离修改时先复制。

`FailValues()` 在终端执行结束时按错误槽位收集，不会随之后的 `Results()` 修改重新计算；批次失败时保存该批所有输入，不代表其中每个元素都已单独执行或失败。顶层错误也单独保存，因此清除 `Results()[i].Err` 后，`Errors()` 可能变为空，而 `Error()` 仍返回原顶层错误。生产代码应将结果当作只读对象，复制后再做展示或重试标记；含类型为指针且值为 nil 的 error 接口仍是非 nil 错误。

```go
results := async.Slice[int](ctx, nums).Worker(8).Map(fn)

if results.Err() {
    for _, e := range results.Errors() {
        log.Printf("错误: %v", e)
    }
    for _, fv := range results.FailValues() {
        log.Printf("失败元素的原始值: %v", fv)
    }
    return
}

// 链式 Must 取结果
values := results.Must()
```

---

## ForEachResult 结果访问

| 方法 | 说明 |
|------|------|
| `.Error()` | 第一个错误 |
| `.Ok()` | 无错误返回 true |
| `.Err()` | 有错误返回 true |
| `.Total()` | 执行器记录的总项数，批量入口的单位见对应方法 |
| `.FailCount()` | 执行器记录的失败项数 |
| `.OkCount()` | `Total() - FailCount()` |

> **注意**：统计在 `ForEach` 返回后保存在结果对象中，读取不会重新执行任务。空输入的 `.Ok()` 也为 true；取消或 FailFast 后核对全量完成情况时，还需结合具体入口的未执行项及统计单位规则。

---

## 模式切换完整示例

### 默认并行模式

不需要 `.Parallel()`，直接使用：

```go
results := async.SliceWith[string](ctx, items).
    Sort(cmp.Compare[int]).                      // import "cmp"；避免整数相减溢出
    Filter(func(v int) bool { return v > 0 }).    // 数据操作
    Worker(16).                                   // 并行配置
    FailFast().
    Map(func(ctx context.Context, v int) (string, error) {
        return process(ctx, v)
    })
```

### Parallel 模式（显式并行）

```go
results := async.Slice[int](ctx, items).
    Parallel().             // 显式切换并行模式
    Worker(16).             // 只能用 ParallelSlice 的并行配置
    Shards(8).
    FailFast().
    Map(fn)
```

### 串行模式

```go
results := async.Slice[int](ctx, items).
    Serial().               // 串行模式
    FailFast().
    Map(fn)                 // 串行执行
```

### 模式来回切换

```go
serial := async.Slice[int](ctx, items).Serial().Sort(cmp).Filter(pred)
// 使用筛选后的串行构建器切回并行模式。
result := serial.ToParallel().Worker(8).Map(fn)

// 或者链式：
result = async.Slice[int](ctx, items).Serial().
    Filter(pred).ToParallel().Worker(8).Map(fn)
```

> **⚠️ SerialSlice → ParallelSlice 后可以继续链式数据操作**
>
> `SerialSlice` 和 `ParallelSlice` 都嵌入 `*sliceBase`，共享数据操作方法（Sort/Filter/Chunked/Values 等）。`Chunked` 返回子切片，`Chunk` 配置并行执行的批大小。切换到 `ParallelSlice` 后既可以继续数据操作，也可以配置并发参数。

#### 切换后的视图与配置

`Serial()`、`Parallel()`、`ToSerial()`、`ToParallel()` 均无参数，并返回新构建器。它们复制当前切片头与 context 引用：当前长度、容量及底层数组保留，但后续对一方执行 `Take`、`Drop`、`Append` 或替换数组不会自动更新另一方的切片头。仍共享的槽位改写则互相可见。源构建器的配置保持，新构建器重置为 `Seq()` 或 `DefPar()`；超时、FailFast、worker、池及分片等设置都应在切换后重新配置。context 引用保留意味着父取消继续生效，不代表旧的每任务超时策略被保留。

```go
source := async.Slice(ctx, []int{1, 2, 3}).Timeout(time.Hour)
target := source.Serial() // 不继承 source 的每任务超时
target.Take(1).Items()[0] = 9
fmt.Println(source.Values(), target.Values()) // [9 2 3] [9]
target.Timeout(2 * time.Second).FailFast()    // 在目标模式上重新配置
```

生产中应保存并继续使用目标构建器；需要输入隔离时先复制元素槽位及必要的引用内容。执行新构建器不会消费或关闭源构建器的旧池，无论原来是 `Pool` 还是 `PoolAuto`，都须由其所有者安排收尾。不要并发配置或修改共享输入；带输出的切片／map 对照见 [模式切换示例](tests/public_examples_test.go) 中的 `Example_builderModeSwitchViews`。

---

## 跨类型 Map（SliceWith）

输入类型为 T，输出类型为 R：

```go
results := async.SliceWith[string](ctx, []int{1, 2, 3}).
    Worker(8).
    Map(func(ctx context.Context, n int) (string, error) {
        return strconv.Itoa(n), nil
    })
vals := results.Values()  // ["1", "2", "3"]
```

> **⚠️ 泛型参数顺序**
>
> `async.SliceWith[R](ctx, items)`：R 是输出类型，T 由 items 自动推断。注意泛型参数顺序是 `[R, T]`，调用时只需显式提供 R。

---

## MapBatch / ForEachBatch（批量处理）

配合 `Chunk(size)` 使用，fn 接收整个分块切片：

```go
// MapBatch：每个分块返回一个结果
results := async.Slice[int](ctx, items).
    Chunk(100).            // 每 100 个元素为一块
    Worker(8).
    MapBatch(func(ctx context.Context, chunk []int) (int, error) {
        sum := 0
        for _, v := range chunk {
            sum += v
        }
        return sum, nil    // 每个 chunk 一个结果
    })

// ForEachBatch：每个分块执行副作用
r := async.Slice[string](ctx, records).
    Chunk(200).
    Worker(4).
    ForEachBatch(func(ctx context.Context, chunk []string) error {
        return db.BatchInsert(ctx, chunk)
    })
```

> **⚠️ Chunk ≠ Shards**
>
> - `Chunk(n)`：将切片按每 n 个元素分块，逐步提交给并发 worker 处理。适合限制单次内存占用的场景。
> - `Shards(n)`：将切片均匀分成最多 n 个非空分片，片内顺序执行，由最多 `Worker` 个 goroutine 调度。

---

## Pool 注入

将任务提交到外部协程池中执行：

```go
// Pool(p)：在 Pool.Run 回调中使用池；Run 返回时关闭池。
err := async.Pool[string]().Context(ctx).Worker(16).
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
        results := async.Slice[string](ctx, items).Pool(p).Map(processFunc)
        return results.Error()
    })

// DefaultPool()：内部创建并自动管理池。
results := async.Slice[string](ctx, items).
    DefaultPool().
    Map(processFunc)
```

---

## 默认值体系

### SliceBuilder 配置默认值

| 默认值 | 获取/恢复方法 | 默认值 | 说明 |
|--------|-------------|--------|------|
| 默认并发度 | `.DefaultWorker()` | **`IO()`**（`NumCPU×2`） | 默认并行模式使用 |
| 默认超时设置 | `.DefaultTimeout()` | **30s** | 新建构建器默认不启用超时，调用该方法后启用；此值固定，不受 `SetDefaultTimeout` 影响 |
| 默认 FailFast | `.DefaultFailFast()` | **关闭** | 不启用快速失败 |
| 默认分片 | `.DefaultShard()` | **GOMAXPROCS**（最少 2） | 不启用分片时无此行 |
| 默认分块 | `.DefaultChunk()` | **100** | 常量 `defaultBatchSize` |
| 默认 Buf | `.DefaultBuf()` | **自适应**（≤0 时：n≤16384 则=n，否则=16384） | Stream channel 缓冲 |
| 默认池 | `.DefaultPool()` | **终端执行时按 IO() 内部创建** | 不采用策略 Worker；清除外部池引用，此前外部池不由此设置关闭 |
| 默认日志 | `.DefaultLogger()` | **静默** | `清除当前覆盖，继承全局日志器` |

### 模式专属默认值

| 模式 | 专有 Default* | 说明 |
|------|--------------|------|
| SliceBuilder（默认并行） | `.DefaultWorker()`, `.DefaultShard()`, `.DefaultChunk()`, `.DefaultPool()`, `.DefaultBuf()`, `.DefaultTimeout()`, `.DefaultFailFast()`, `.DefaultLogger()` | 以本类型实际提供的方法为准 |
| ParallelSlice | `.DefaultWorker()`, `.DefaultShard()`, `.DefaultChunk()`, `.DefaultPool()` | 并行配置可通过 Default* 恢复 |
| SerialSlice | `.DefaultTimeout()` | 仅超时可恢复默认 |

### 默认值覆盖优先级

```
全局默认值（core.Set*）
    ↓ 被覆盖
Builder 级设置（.Worker(n), .Timeout(d) 等）
    ↓ 被覆盖
Default* 恢复为特定默认值
    ↓ 覆盖
终端操作（.Map(fn) / .ForEach(fn) 等）
```

---

## 生产环境使用建议

### 模式选型速查

| 场景 | 推荐 | 理由 |
|------|------|------|
| 逐元素转换或副作用 | **Map / ForEach** | 从默认配置开始测量 |
| 需要评估分片的 IO 操作 | **Map + Shards** | 与不分片基线比较调度成本和负载均衡 |
| 实时流式消费 | **Stream** | 边执行边消费 |
| 汇总/聚合 | **Reduce** | 同次调用串行；共享输入和外部状态仍需同步 |
| 按连续批次处理 | **MapBatch / ForEachBatch** | 按 Chunk 大小切批，不按业务 key 分组 |
| 外部已有协程池复用 | **Pool 注入** | 避免重复创建 worker |

### 并发度与分片数建议

CPU 密集型可从 `CPU()` 开始评估，IO 回调可从 `IO()` 开始评估；它们只是按 CPU 核数计算的候选值。并发度还应受下游连接数、配额和内存预算限制。先测不分片的基线，再根据锁等待与吞吐决定是否分片；不能仅按元素数量推导最佳 worker 或分片数。

### Chunk 与 MapBatch 配合

```go
// 批量处理：每 500 个元素一批
results := async.SliceWith[[]int64](ctx, millions).
    Worker(16).
    Shards(8).
    Chunk(500).                    // 每批 500 个
    MapBatch(func(ctx context.Context, batch []string) ([]int64, error) {
        return batchInsertDB(ctx, batch)
    })
```

### Stream 缓冲设置建议

先使用 0，让执行器按输入数量选择缓冲；再根据生产和消费速度、单项大小及内存预算调整。4096 或 16384 只是可供压测的容量，不能按输入数量直接断言最佳值。缓冲不能代替持续消费，提前退出时应取消 context。

### 生产推荐配置

```go
// ========== 逐项转换：string → Record ==========
results := async.SliceWith[Record](ctx, items).
    Worker(16).
    Timeout(30 * time.Second).          // 单元素超时
    FailFast().                         // 任一失败时请求取消，业务函数须协作退出
    Map(func(ctx context.Context, item string) (Record, error) {
        return transform(ctx, item)
    })
values := results.Values()
if results.Err() {
    log.Printf("部分失败: %v", results.Error())
}

// ========== 分片转换：按业务负载评估调度效果 ==========
shardedResults := async.SliceWith[Record](ctx, items).
    Worker(async.IO()).                 // CPU×2
    FailFast().
    Shards(16).                         // 最多 16 个非空分片
    Map(func(ctx context.Context, item string) (Record, error) {
        return ioBoundProcess(ctx, item)
    })
for _, r := range shardedResults.Results() {
    if r.Ok() { handle(r.Value) }
}

// ========== 流式消费：边处理边消费 ==========
ch := async.Slice[Record](ctx, records).
    Worker(async.IO()).
    FailFast().
    Timeout(10 * time.Second).
    Stream(func(ctx context.Context, r Record) (Record, error) {
        return process(ctx, r)
    }, 4096)                            // channel 缓冲
for res := range ch {
    if res.Err != nil {
        log.Printf("处理失败: %v", res.Err)
        continue
    }
    saveToDB(res.Value)
}

// ========== 批量写入：每批返回一组 ID ==========
batchResults := async.SliceWith[[]int64](ctx, millions).
    Worker(16).
    Shards(16).
    Chunk(500).                         // 每 500 条一批
    Timeout(60 * time.Second).          // 批量操作加大超时
    MapBatch(func(ctx context.Context, batch []Record) ([]int64, error) {
        return db.BatchInsert(ctx, batch)
    })
if err := batchResults.Error(); err != nil {
    log.Printf("批量写入失败: %v", err)
}
```

### 常见错误

- **❌ 忽略大切片的内存开销**：池默认保留最多 100K 个结果；池化 Map 为返回完整映射结果还会按输入数量保存结果。海量数据应考虑分批处理，并评估各批次的内存占用。
- **❌ 未测量就增加分片**：分片数可以大于 worker 数，但不增加同时执行的 worker 上限；过多分片有调度成本，过少分片可能限制并发。按回调耗时和负载分布压测。
- **❌ Reduce 期望并行**：Reduce 始终串行，忽略 Worker/Shards；Slice 的 Reduce 还忽略策略 Timeout/FailFast，回调应自行检查 context。
- **❌ ForEach 结果未检查**：`ForEachResult.Err()` 可能为 true，建议始终检查
- **❌ 并发修改 SliceBuilder**：构建器可顺序复用，但不能并发修改配置或输入。PoolAuto 注入的池在终端操作后关闭，后续执行前需更换池；共享输入仍须同步。

---

## 自定义日志

```go
results := async.Slice[int](ctx, items).
    Logger(myLogger).   // 全局生效
    Worker(8).
    Map(fn)

results = async.Slice[int](ctx, items).
    Logger(myLogger).
    DefaultLogger().    // 当前构建器恢复继承全局日志器
    Map(fn)
```

---

## Map 容器（K-V 泛型容器）

`async.Map[K, V]` 是泛型 key-value 容器，支持链式调用。`K` 必须满足 `comparable` 约束，`V` 为任意类型。并发/串行处理由单独的 `MapChain` 提供。

原生并行 `Map`、`ForEach`、`Stream`、`MapBatch`、`ForEachBatch` 的业务回调调用 `runtime.Goexit` 时，保存 `async.ErrTaskGoexit` 并由后继继续同片及剩余任务。Map/MapBatch 不保存失败值；ForEach 计一个失败元素，ForEachBatch 将整批元素计失败；Stream 发送零值与该错误。无池 FailFast 只停止后续调度，不主动取消已开始回调，单项派生 context 仍在退出时清理。

生产应正常返回 `error`。同步串行和 Reduce 不转换该退出；串行 Stream 会结束其生产 goroutine 并关闭通道，尚未发送项不会补发。以上业务退出保证不包含任意日志或通知后端异常。

> **并发访问**：`Map` 本身没有锁；并发访问中只要有写入，调用方必须同步所有访问，包括 `AsMap()` 和 `MapFromRef()` 暴露的共享 map。`MapChain` 直接引用输入 map，构建器不可并发修改，执行期间也不得并发修改输入 map；并行回调访问其他共享数据时需自行同步。

### 创建入口

| 函数 | 说明 |
|------|------|
| `async.NewMap[K,V]()` | 创建空容器 |
| `async.NewMapCap[K,V](capacity)` | 创建空容器，预分配 capacity 容量 |
| `async.MapFrom[K,V](m)` | 从原生 map 创建（浅拷贝键值对） |
| `async.MapFromRef[K,V](m)` | 从原生 map 创建（直接引用，不拷贝） |

`MapFrom` 和 `MapFromRef` 收到 nil map 时都会创建可写的空 map。需要隔离键值对的增删改时使用 `MapFrom`；它只做浅拷贝，引用类型值内部的数据仍可能共享。

```go
m := async.NewMap[string, int]()
m.Set("a", 1).Set("b", 2)

// 浅拷贝键值对，引用类型的值仍共享其底层数据
m2 := async.MapFrom(map[string]int{"x": 1, "y": 2})

// 引用（外部修改会影响 m3）
raw := map[string]int{"x": 1}
m3 := async.MapFromRef(raw)
```

### Map 容器方法速查表

#### 基础读写

| 方法 | 签名 | 说明 |
|------|------|------|
| `Get(key)` | `(V, bool)` | 获取键值，不存在时返回零值和 false |
| `Set(key, value)` | `*Map[K,V]` | 设置键值对，覆盖已有值 |
| `SetAll(entries)` | `*Map[K,V]` | 批量设置键值对 |
| `Delete(keys...)` | `*Map[K,V]` | 删除一个或多个键 |
| `GetAndDelete(key)` | `(V, bool)` | 获取后删除 |
| `GetOrDefault(key, defaultVal)` | `V` | 获取键值，不存在时返回默认值 |
| `GetOrSet(key, defaultVal)` | `(V, bool)` | 获取已有值；不存在时设置默认值，第二个返回值 true 表示已设置新值 |

#### 基础查询

| 方法 | 签名 | 说明 |
|------|------|------|
| `Has(key)` | `bool` | 键是否存在 |
| `Len()` | `int` | 键值对数量 |
| `IsEmpty()` | `bool` | 是否为空 |
| `Clear()` | `*Map[K,V]` | 清空所有键值对 |

#### 遍历

| 方法 | 签名 | 说明 |
|------|------|------|
| `Range(fn)` | `*Map[K,V]` | 遍历所有键值对，`fn` 返回 false 时提前终止 |
| `ForEach(fn)` | `*Map[K,V]` | 遍历所有键值对（不可提前终止） |

#### 提取

| 方法 | 签名 | 说明 |
|------|------|------|
| `Keys()` | `[]K` | 返回所有键 |
| `Values()` | `[]V` | 返回所有值 |
| `Clone()` | `map[K]V` | 返回底层原生 map 的浅拷贝 |
| `AsMap()` | `map[K]V` | 返回底层原生 map 的直接引用 |

#### 过滤与变换

| 方法 | 签名 | 说明 |
|------|------|------|
| `Filter(fn)` | `*Map[K,V]` | 过滤键值对，保留 fn 返回 true 的条目（原地操作） |
| `FilterKeys(fn)` | `*Map[K,V]` | 过滤键，保留 fn 返回 true 的条目 |
| `FilterValues(fn)` | `*Map[K,V]` | 过滤值，保留 fn 返回 true 的条目 |
| `Reject(fn)` | `*Map[K,V]` | 删除 fn 返回 true 的条目（与 Filter 语义相反） |
| `MapValues(fn)` | `*Map[K,V]` | 原地变换所有值，fn 接收 key 和 value 返回新 value |

#### 合并

| 方法 | 签名 | 说明 |
|------|------|------|
| `Merge(other)` | `*Map[K,V]` | 将原生 map 合并到当前容器，同名键会被覆盖 |
| `MergeMap(other)` | `*Map[K,V]` | 将另一个 Map 合并到当前容器，同名键会被覆盖 |
| `MergeWithDefault(other)` | `*Map[K,V]` | 合并原生 map，同名键不会覆盖已有值 |

`SetAll`、`Merge`、`MergeMap` 和 `MergeWithDefault` 对来源中的每个原条目复制一次，支持来源与接收者共享同一个底层 map。NaN 等非自反键无法匹配已有键，每个来源条目都会新增一项，`MergeWithDefault` 也如此；含两个 NaN 条目和一个普通条目的 map 自合并后为五项，普通键不增加。实现先暂存非自反条目，来源遍历结束后再写入，避免重新复制本次新增项；该路径额外空间 O(k)，k 为来源非自反条目数，普通键路径不分配条目快照。生产中仍应按合并后规模安排内存，重复自合并会使 NaN 条目数逐次翻倍。值是浅拷贝，共享 map 的写入由调用方同步。带输出示例见 `Example_mapAliasedMerge`。

#### 集合运算

| 方法 | 签名 | 说明 |
|------|------|------|
| `Intersect(other)` | `*Map[K,V]` | 按键取交集，新 Map 使用当前容器的值 |
| `Union(other)` | `*Map[K,V]` | 返回新 Map；同名键使用 `other` 的值 |
| `Difference(other)` | `*Map[K,V]` | 返回当前容器中有而 `other` 中没有的键值对（新 Map） |

以上集合运算不会修改原容器，`other` 不能为 nil；结果中的引用类型值仍是浅拷贝。`MergeMap(nil)` 则是无操作。

集合运算按键判断成员，不比较值：同键不同值仍属于交集，也会从差集中排除。交集保留接收者的值，并集保留 other 的值。新容器的键增删独立，但指针、切片或 map 值的内部内容仍共享；修改这些内容也会影响原输入。带预期输出的浅拷贝与集合示例见 [Example_mapShallowValuesAndSets](tests/public_examples_test.go)。

成员查询同样遵循 Go 相等性：含 NaN 的 map 与自身取交集时不包含非自反条目，取差集时仍保留这些条目，取并集时这些条目分别从两边复制，数量翻倍。`RemapValues` 保留这些键；`RemapKeys`/`Invert` 生成非自反目标键时，每条来源记录各自生成一个条目，即使 NaN 的位模式相同也不合并。不能将位模式一致误作 map 键相等。契约验证见 `TestNonReflexiveMapSetAndConversionContracts`。

#### 高级查询

| 方法 | 签名 | 说明 |
|------|------|------|
| `Find(fn)` | `(K, V, bool)` | 查找第一个满足 fn 的键值对 |
| `All(fn)` | `bool` | 检查所有条目是否都满足 fn |
| `Any(fn)` | `bool` | 检查是否存在满足 fn 的条目 |
| `Count(fn)` | `int` | 统计满足 fn 的条目数 |

Map 遍历顺序不确定，包括 `Range`、`ForEach`、`Find`、`Keys` 和 `Values`；`Find` 的第一个匹配项不保证固定。`All` 对空容器返回 true，`Any` 返回 false。过滤、`Reject`、`MapValues` 和合并方法会原地修改容器；需要保留原数据时，先用 `async.MapFrom(m.Clone())` 创建独立容器。

`comparable` 不代表键必定等于自身。浮点 NaN，以及包含 NaN 的复数、数组、结构体或接口键，可能有 `key != key`；Go 的普通按键查询和删除无法再次定位这些条目，重复 `Set` 会新增条目。若业务需要按键检索，应在写入前拒绝此类键或规范化为稳定标识。`Filter`/`FilterKeys`/`FilterValues`/`Reject` 按遍历到的条目过滤，`MapValues` 对每个原条目变换一次，支持这些非自反键；涉及移除或变换时可能使用 O(n) 临时存储及 `clear` 重建，但保留底层 map 身份，`FromRef`/`AsMap` 别名会看到结果。`Clear` 可移除全部条目。回调不要自行修改容器，panic 直接传播，已做修改不保证回滚。公开验证见 `Example_mapNaNEntryMutation`。

Range 在回调返回 false 后停止；Find 和 Any 在首次匹配后停止，All 在首次不匹配后停止，Count 则检查全部条目。它们同步执行，不应用 Worker、Timeout 或 FailFast，回调 panic 直接传播。Find 未匹配时返回键和值的零值及 false；存在零键零值条目时同样可能返回这两个零值，必须检查 found。不要用会提前停止的查询承担全量业务副作用。

后面的容器示例只做同步操作；需要有错误返回和取消控制的逐项业务，应使用 MapChain 的终端方法并检查结果。

```go
// 链式操作
m := async.NewMap[string, int]().
    Set("a", 1).Set("b", 2).Set("c", 3)

// 过滤 + 变换
m.Filter(func(k string, v int) bool { return v > 1 }).
  MapValues(func(k string, v int) int { return v * 10 })

// 遍历（可提前终止）
m.Range(func(k string, v int) bool {
    fmt.Println(k, v)
    return true
})

// 集合运算
a := async.NewMap[int, string]().Set(1, "a").Set(2, "b")
b := async.NewMap[int, string]().Set(2, "x").Set(3, "y")
union := a.Union(b)      // {1:"a", 2:"x", 3:"y"}
inter := a.Intersect(b)  // {2:"b"}
diff := a.Difference(b)  // {1:"a"}

// 高级查询
k, v, found := m.Find(func(k string, v int) bool { return v > 5 })
all := m.All(func(k string, v int) bool { return v > 0 })   // true
any := m.Any(func(k string, v int) bool { return v == 20 }) // true
cnt := m.Count(func(k string, v int) bool { return v > 2 }) // 2
```

---

### 内部转换函数与键冲突

internal/maps 的 RemapValues、RemapKeys、ToSlice 和 Invert 是包内实现入口，目前没有对应的 async 根包导出函数；外部程序可通过 Range 自行转换。RemapValues 保留键并转换值，RemapKeys 保留值并转换键，ToSlice 每条目产生一个元素，Invert 将原值作为键、原键作为值；这些操作创建新容器或切片。

RemapKeys 和 Invert 遇到相同目标键时，保留后遍历的那个值；Go map 遍历顺序不确定，最终代表不可预测。生产中需要保留全部冲突项时应按目标键收集切片，需要固定代表时应先明确排序和选择规则。ToSlice 的输出顺序同样不固定，需要顺序时转换后再排序。所有回调同步执行，panic 直接传播，不使用 MapChain 的并行、超时或 FailFast 设置。

### MapChain（Map 并发操作链构建器）

`MapChain` 提供 map 的并发/串行处理能力，支持模式切换、分片、超时等配置。

#### 入口

| 函数 | 说明 |
|------|------|
| `async.NewMapChain[K,V](ctx, data)` | 创建 MapChain，输出类型与 V 一致 |
| `async.NewMapChainWith[K,V,R](ctx, data)` | 创建 MapChain，输出类型为 R |

构造器保留输入 map 的引用；如果需要调用 `Set` 等写操作，请传入已初始化的非 nil map。nil map 可读取，但对其新增键会 panic。

#### MapChain 链式配置方法

| 方法 | 说明 | 默认值 |
|------|------|--------|
| `.Context(ctx)` | 设置上下文 | 入口 ctx |
| `.Serial()` | 切换到串行模式 | — |
| `.Parallel()` | 切换到并行模式 | 默认 |
| `.Worker(n)` | 设置并发度 | `IO()` |
| `.DefaultWorker()` | 恢复默认 IO 并发度 | — |
| `.Pool(p)` | 注入外部协程池 | — |
| `.PoolAuto(p)` | 注入协程池（自动 Close） | — |
| `.DefaultPool()` | 使用内置协程池 | — |
| `.Timeout(d)` | 设置单任务超时 | 不设置 |
| `.DefaultTimeout()` | 将单任务超时设置为固定的 30 秒，不受 `SetDefaultTimeout` 影响 | — |
| `.FailFast()` | 启用快速失败 | 关闭 |
| `.DefaultFailFast()` | 关闭快速失败 | — |
| `.Shards(n)` | 为无池的并行操作设置分片数 | 不分片 |
| `.DefaultShard()` | 按 GOMAXPROCS 分片，最少 2 片 | — |
| `.Buf(size)` | 设置 Stream 缓冲区 | 0（自适应） |
| `.DefaultBuf()` | 清除 Buf 设置 | — |
| `.Chunk(size)` | 设置分块大小；非正数在批量执行时按 1 处理 | 未设置，批量执行时按 1 处理 |
| `.DefaultChunk()` | 将分块大小设置为 100 | — |
| `.Logger(l)` | 设置当前构建器的日志器（仅局部生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |

MapChain 的 `Worker` 和 `DefaultWorker` 只更改并发度，保留超时、FailFast、池和其他设置。`Pool` / `PoolAuto` 适用于 Map、ForEach、Stream 和批量操作；Filter 和 Reduce 保持串行，`PoolAuto` 的池在操作结束时关闭。

池化 `Map` / `ForEach` / `MapBatch` / `ForEachBatch` 的完整结果或统计由执行器的完成回调单独保存，不从池自身保留的结果中重建。池的 `MaxResults`、环形缓冲容量及丢弃策略不会截断这些终端的成功条目、改变原键关联或漏计失败；提交拒绝和取消仍按各终端规则记录。执行器会建立全部输入条目的快照，并为每个输入或批次分配结果槽位，`MaxResults` 因而不能限制本操作的全部内存。海量输入应在调用前分批，并评估回调值和快照的占用；流式结果的消费与取消要求另见 Stream 说明。用法见 [公开示例](tests/public_examples_test.go) 中的 Example_mapPoolRetention，组合契约见 [池化结果与统计测试](../internal/maps/pool_projection_contract_test.go)。

无池的 Map / ForEach / Stream 将条目分片，片内顺序执行；批量操作先按 `Chunk` 划分批次，再对批次分片。最多 `Worker` 个分片同时运行。池化操作由实际池控制并发：`Worker` 不调整借用或接管池；`DefaultPool` 在终端执行时按 `IO()` 创建，同样不采用策略 `Worker`。因此 `Worker(1).DefaultPool()` 仍可同时启动多个回调，`FailFast` 时已开始的多个回调仍可能返回失败。需要固定池化并发数时，用 `async.Pool[R]().Worker(n).Run` 创建所需容量的池，在其作用域内注入 `Pool(p)`，并等待当前操作收尾。Filter 和 Reduce 保持串行。
MapChain 的模式切换同样创建新构建器并重置策略，源配置保持；两者共享当前 map 的键值内容，任一方的 Set/Delete/Clear 都会被另一方观察到。context 保存的是切换时的引用，后续在一方调用 Context 不会更新另一方；父取消仍作用于双方原来共享的 context。旧构建器中设置的池不会转移或自动关闭，调用方须安排其生命周期；执行新构建器不会替源构建器收尾。需要独立键值集合时先复制输入 map，引用类型值仍须按业务复制。

无池的 Map / ForEach 及批量操作在 FailFast 观察到错误后停止后续调度，不为已开始的回调主动取消 context；它们也不因输入 context 已取消而自动跳过回调，业务函数须检查 context 并返回错误。Stream 会检查输入 context，但其无池 FailFast 同样只停止后续处理；池化路径采用 Slice Runner 的派生 context 取消策略。这些行为不能从共享的 FailFast 方法名推断为完全相同。

`Timeout` 为回调创建带期限的 context，并不能强制中断忽略 context 的回调。`Filter` 和 `Reduce` 串行直接调用回调，不会自动捕获回调 panic；它们也不主动因 context 取消而终止遍历，回调应自行检查 context。`Reduce` 遍历顺序不确定，顺序敏感的聚合应先对键排序后自行处理。

Filter 的正常返回错误始终为 nil；回调即使发现取消或期限到达，只要返回 true，该条目仍被保留。需要在业务层区分“没有匹配项”和“查询被取消”，应在回调及终端返回后按业务要求检查 context；Filter 的 error 返回值不提供这一判断。返回容器的键值对独立，值内部的指针、切片等引用仍与输入共享。

Reduce 遇到非 nil 错误立即停止，并返回这一次失败回调给出的值，不会恢复到上一次累积值或 initial。该值可能包含尚未提交的业务变更，调用方必须先处理错误再决定是否采用部分值；空输入则返回 initial 和 nil。固定可运行对照见 [聚合结果示例](tests/public_examples_test.go) 中的 Example_mapAggregateResults。

#### MapParallel 专属方法

| 方法 | 说明 |
|------|------|
| `.ToSerial()` | 切换到串行模式 |
| `.Worker(n)` | 设置并发度 |
| `.DefaultWorker()` | 恢复默认 |
| `.Pool(p)` | 注入外部协程池 |
| `.PoolAuto(p)` | 注入协程池（自动 Close） |
| `.DefaultPool()` | 使用内置 |
| `.FailFast()` | 启用 FailFast |
| `.NoFailFast()` | 关闭 FailFast |
| `.DefaultFailFast()` | 恢复默认 |
| `.Timeout(d)` | 设置超时 |
| `.DefaultTimeout()` | 恢复默认 |
| `.Shards(n)` | 分片数 |
| `.DefaultShard()` | 默认分片 |
| `.Buf(size)` | 缓冲区 |
| `.DefaultBuf()` | 清除缓冲 |
| `.Chunk(size)` | 分块大小 |
| `.DefaultChunk()` | 默认分块 |
| `.Logger(l)` | 自定义日志 |

#### MapSerial 专属方法

| 方法 | 说明 |
|------|------|
| `.ToParallel()` | 切换到并行模式 |
| `.FailFast()` | 启用 FailFast |
| `.NoFailFast()` | 关闭 FailFast |
| `.Timeout(d)` | 设置超时 |
| `.DefaultTimeout()` | 恢复默认 |
| `.Logger(l)` | 自定义日志 |

#### MapChain 终端方法

| 方法 | 签名 | 返回 | 说明 |
|------|------|------|------|
| `.Map(fn)` | `(ctx, K, V) → (R, error)` | `*MapResult[K,R]` | 并发映射 |
| `.MapToFn(fn)` | `(ctx, K, V) → (R, error)` | `*MapResult[K,R]` | Map 别名（向后兼容） |
| `.ForEach(fn)` | `(ctx, K, V) → error` | `*MapForEachResult` | 并发遍历 |
| `.Filter(fn)` | `(ctx, K, V) → bool` | `(*Map[K,V], error)` | 串行过滤 |
| `.Stream(fn, bufSize)` | `(ctx, K, V) → (R, error)` | `<-chan async.Result[R]` | 流式映射 |
| `.Reduce(initial, fn)` | `(ctx, R, K, V) → (R, error)` | `(R, error)` | 串行聚合 |
| `.MapBatch(fn)` | `(ctx, []MapEntry[K,V]) → (R, error)` | `*MapResult[K,R]` | 批量映射 |
| `.ForEachBatch(fn)` | `(ctx, []MapEntry[K,V]) → error` | `*MapForEachResult` | 批量遍历 |

`MapBatch` 为每个成功批次保存一个结果，以该批第一个条目的键作为结果键。输入 map 的遍历顺序不确定，因此批次成员和结果键也不固定；需要稳定分组时应先对输入排序并自行分批。串行模式构建器不提供批量方法。

#### MapChain 数据操作方法（链式）

这些方法继承自 `MapBase`，支持在链式构建器中直接操作底层数据：

| 方法 | 签名 | 说明 |
|------|------|------|
| `.Has(key)` | `bool` | 键是否存在 |
| `.Get(key)` | `(V, bool)` | 获取键值 |
| `.Set(key, value)` | `*MapChain` | 设置键值对 |
| `.SetAll(entries)` | `*MapChain` | 批量设置 |
| `.Delete(keys...)` | `*MapChain` | 删除键 |
| `.Clear()` | `*MapChain` | 清空所有键值对 |
| `.Len()` | `int` | 返回键值对数量 |
| `.IsEmpty()` | `bool` | 是否为空 |
| `.Keys()` | `[]K` | 所有键 |
| `.Values()` | `[]V` | 所有值 |
| `.AsMap()` | `map[K]V` | 底层原生 map 的引用 |
| `.Range(fn)` | `*MapChain` | 遍历（可提前终止） |
| `.Find(fn)` | `(K, V, bool)` | 查找第一个满足 fn 的条目 |
| `.All(fn)` | `bool` | 是否所有条目都满足 fn |
| `.Any(fn)` | `bool` | 是否存在满足 fn 的条目 |
| `.Count(fn)` | `int` | 统计满足 fn 的条目数 |
| `.FilterKV(fn)` | `*MapChain` | 过滤键值对（原地操作） |
| `.Reject(fn)` | `*MapChain` | 删除满足 fn 的条目 |
| `.MapValues(fn)` | `*MapChain` | 原地变换所有值 |
| `.Merge(other)` | `*MapChain` | 合并原生 map |
| `.MergeMap(m)` | `*MapChain` | 合并另一个 Map |
| `.MergeWithDefault(other)` | `*MapChain` | 合并不覆盖已有键 |

#### MapResult 方法

| 方法 | 签名 | 说明 |
|------|------|------|
| `Data()` | `map[K]R` | 返回内部结果 map 的直接引用；修改会影响结果对象 |
| `Len()` | `int` | 返回条目数 |
| `Keys()` | `[]K` | 返回所有键 |
| `Values()` | `[]R` | 返回所有值 |
| `Error()` | `error` | 返回第一个错误 |
| `Err()` | `bool` | 是否有错误 |

`MapRes[K,R]` 是 `MapResult[K,R]` 的向后兼容别名。
`Keys()` 和 `Values()` 各自遍历 map，顺序不确定，不能按索引配对。并发读写 `Data()` 返回的 map 时须由调用方同步。

Map 终端按实际键值条目执行，NaN 及包含 NaN 的键也会传入回调；成功映射保留原键，批量映射仍只保留每批首条目的键。结果中的非自反键无法通过 `Data()[key]` 回查，即使 key 来自 `Keys()`；需要关联键和值时直接 `for key, value := range result.Data()`，或在 Stream 返回值中携带业务标识。生产中若需稳定查询，应在输入阶段拒绝或转换此类键，转换时自行确定冲突策略；不要把查不到条目误判为回调未执行。

#### MapForEachResult 方法

MapChain 的 ForEachBatch 按元素统计 Total/FailCount：无池时，每次开始处理包含 k 条目的批次，Total 增加 k；该批失败时 FailCount 增加 k。池化路径为所有输入批次建立结果槽，Total 因而等于输入元素数；提交拒绝、取消或未执行批次也按其元素数计失败，即使业务回调从未运行。无池 FailFast 提前停止时，Total 则可能小于输入数。Slice 的 ForEachBatch 按批次统计，不能混用这两种单位。可执行对照见 [批量统计示例](tests/public_examples_test.go)。

| 方法 | 签名 | 说明 |
|------|------|------|
| `Total()` | `int64` | 按终端入口记录的元素数；池化 ForEachBatch 包含拒绝和未执行批次 |
| `FailCount()` | `int64` | 失败数 |
| `OkCount()` | `int64` | 成功数 |
| `Error()` | `error` | 第一个错误 |
| `Err()` | `bool` | 是否有错误 |

ForEach 单项统计也区分池化与无池：无池 Total 记录已开始的元素，FailFast 可使其小于输入数；池化 Total 记录全部输入，提交拒绝、取消或未执行元素同样计失败。父 context 预先取消时，无池仍可能调用忽略取消的回调并记录成功，池化则可能在回调前拒绝。可执行对照见 [单项统计示例](tests/public_examples_test.go) 中的 Example_mapForEachRejectedStatistics；不要用 Total 或 FailCount 推算业务回调次数。

#### MapEntry 类型

```go
type MapEntry[K comparable, V any] struct {
    Key K
    Val V
}
```

用于 `MapBatch` / `ForEachBatch` 的批量操作条目。

#### 使用示例

```go
data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}

// 并发 Map
result := async.NewMapChainWith[string, int, string](ctx, data).
    Worker(8).
    Shards(4).
    Map(func(ctx context.Context, k string, v int) (string, error) {
        return strings.ToUpper(k) + "_" + strconv.Itoa(v*10), nil
    })

fmt.Println(result.Data()) // map[a:A_10 b:B_20 c:C_30 d:D_40]

// 串行 + 过滤
filtered, err := async.NewMapChain[string, int](ctx, data).
    Serial().
    Filter(func(ctx context.Context, k string, v int) bool {
        return v > 2
    })
if err != nil {
    log.Printf("过滤失败: %v", err)
} else {
    fmt.Println(filtered.AsMap())
}

// 流式处理
ch := async.NewMapChainWith[string, int, string](ctx, data).
    Parallel().Worker(4).
    Stream(func(ctx context.Context, k string, v int) (string, error) {
        return k, nil
    }, 1024)

for r := range ch {
    if r.Err != nil {
        log.Printf("处理失败: %v", r.Err)
        continue
    }
    fmt.Println(r.Value)
}

// 批量处理
batchResult := async.NewMapChainWith[string, int, string](ctx, data).
    Chunk(2).
    MapBatch(func(ctx context.Context, batch []async.MapEntry[string, int]) (string, error) {
        return batch[0].Key, nil
    })
if err := batchResult.Error(); err != nil {
    log.Printf("批量处理失败: %v", err)
}
```

---

Map 的 `Stream` 将回调中的 panic 转为包含 `PanicError` 的结果。启用 `FailFast` 后，遇到错误会停止后续处理；并行模式下，已经开始的回调仍可能返回结果。

取消传入的 context 后，`Stream` 停止启动后续回调，阻塞的结果发送也可以退出；尚未发送的结果可能被丢弃。已开始的回调需要自行响应 context 或返回，通道才会关闭。提前停止消费时，应取消该 context。

### Map 容器 & MapChain 生产推荐配置

以下六段是独立用法，放在返回 `error` 的业务函数中使用；导入 `context`、`errors`、`fmt`、`time` 和根包 `async`。`ctx` 是调用方提供的父 context；`data` 是业务的 `map[string]int`，第一段另建四条演示数据；`recordData` 是调用方提供的 `map[string]Record`。业务接口为 `processKV(context.Context, string, int) (string, error)`、`saveResult(context.Context, string) error`、`db.BatchUpsert(context.Context, []async.MapEntry[string, Record]) (int64, error)` 和 `heavyTransform(context.Context, int) (*Processed, error)`；`Processed.Score` 为 `float64`，成功时返回非 nil 指针。

生产建议：在请求入口设置总预算，再按单条目或整批设置有限超时；业务回调必须响应传入 context。`Worker` 和 `Shards` 是调度上限，按下游容量压测；`Chunk(200)` 将条目分组，每批共享一分钟期限。MapBatch 每批只保留一个返回值，以该批首条目键存储；map 遍历、分组和结果键均无固定顺序。Reduce 始终同步串行，Worker、Shards 和 FailFast 不改变其执行方式；正数 Timeout 为每次回调建立期限，回调返回首个错误即结束。

注意：Map/MapBatch 的 `Error()` 只保存首个观察到的错误，并不枚举全部失败；FailFast 仍等待已开始的回调返回。Stream 必须消费到通道关闭，本例收集收到的业务与保存错误；取消会丢弃尚未发送的结果，因此再检查父 context，也仍无法恢复全部业务错误。本例继续保存其余成功项，错误列表最多随输入数增长；大流量服务应按业务决定首错或有界收集策略。中途取消、失败和 panic 不能回滚已经提交的副作用，重试需幂等控制。同步 Reduce 的 panic 直接传播、Goexit 结束调用 goroutine；不要将其当作可转换的业务错误。同步容器操作不使用 context；共享数据有写入时须外部同步。

```go
// ========== MapChain 并发映射：标准 K-V 并行处理 ==========
data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}

result := async.NewMapChainWith[string, int, string](ctx, data).
    Worker(16).
    Shards(8).                              // 最多 8 个分片并行
    Timeout(30 * time.Second).              // 单条目超时
    FailFast().                             // 发生失败后停止后续工作；已开始的回调可能继续
    Map(func(ctx context.Context, k string, v int) (string, error) {
        return processKV(ctx, k, v)
    })
if err := errors.Join(result.Error(), ctx.Err()); err != nil {
    return err // 成功条目可能已完成；按业务决定是否使用部分结果
}
fmt.Println(result.Data())                  // map[string]string

// ========== MapChain 流式消费：边处理边消费 ==========
var streamErrors []error
ch := async.NewMapChainWith[string, int, string](ctx, data).
    Worker(16).Shards(8).
    Buf(4096).                              // channel 缓冲
    Stream(func(ctx context.Context, k string, v int) (string, error) {
        return processKV(ctx, k, v)
    }, 4096)
for r := range ch {
    if r.Err != nil {
        streamErrors = append(streamErrors, r.Err)
        continue
    }
    if err := saveResult(ctx, r.Value); err != nil {
        streamErrors = append(streamErrors, err)
    }
}
if err := errors.Join(append(streamErrors, ctx.Err())...); err != nil {
    return err
}

// ========== MapChain 批量 + 分块：数据库批量操作 ==========
// recordData 为调用方提供的 map[string]Record，示例处理全部条目。
batchResult := async.NewMapChainWith[string, Record, int64](ctx, recordData).
    Worker(8).Shards(4).
    Chunk(200).                             // 每 200 条一批
    Timeout(60 * time.Second).              // 批量操作加大超时
    MapBatch(func(ctx context.Context, batch []async.MapEntry[string, Record]) (int64, error) {
        return db.BatchUpsert(ctx, batch)
    })
if err := errors.Join(batchResult.Error(), ctx.Err()); err != nil {
    return err
}

// ========== MapChain 聚合：同步串行 Reduce ==========
sum, err := async.NewMapChain[string, int](ctx, data).
    Worker(16).
    Timeout(30 * time.Second).
    FailFast().
    Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
        if err := ctx.Err(); err != nil { return acc, err }
        return acc + v, nil
    })
if err := errors.Join(err, ctx.Err()); err != nil {
    return err
}
fmt.Println(sum)
// 或先并发转换再串行聚合
r := async.NewMapChainWith[string, int, *Processed](ctx, data).
    Worker(16).Shards(8).FailFast().
    Map(func(ctx context.Context, k string, v int) (*Processed, error) {
        return heavyTransform(ctx, v)
    })
if err := errors.Join(r.Error(), ctx.Err()); err != nil {
    return err // 根据业务决定是否使用部分结果
}
reduced, err := async.NewMapChainWith[string, *Processed, float64](ctx, r.Data()).
    Serial().                               // 显式串行；Reduce 本身始终串行
    Reduce(0.0, func(ctx context.Context, acc float64, k string, v *Processed) (float64, error) {
        if err := ctx.Err(); err != nil { return acc, err }
        return acc + v.Score, nil
    })
if err := errors.Join(err, ctx.Err()); err != nil {
    return err
}
fmt.Println(reduced)

// ========== Map 操作链：同步过滤与值变换 ==========
// 同步操作；共享容器并且存在写入时需要外部同步
m := async.NewMap[string, int]().
    Set("a", 1).Set("b", 2).Set("c", 3).
    Filter(func(k string, v int) bool { return v > 1 }).
    MapValues(func(k string, v int) int { return v * 10 })
// m.AsMap() → map["b":20 "c":30]
```
