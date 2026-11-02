# Map/Reduce（数据并行）文档

## 概述

Slice 模块提供切片元素的并发/串行处理，支持 Map / ForEach / Reduce / Stream 四种终端操作。通过 `SliceBuilder[T,R]` 链式构建器配置策略和模式。

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
- [Serial 模式（串行）](#serial-模式串行)
- [跨类型 Map（SliceWith）](#跨类型-map-slicewith)
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
| `.DefaultPool()` | 创建默认并发度协程池并注入（自动管理生命周期） | — |
| `.Pool(p)` | 注入外部协程池（用户管理生命周期） | 无 |
| `.PoolAuto(p)` | 注入外部协程池（自动 Close） | 无 |
| `.Timeout(d)` | 单任务超时 | 不设置 |
| `.DefaultTimeout()` | 恢复默认超时（30s） | — |
| `.FailFast()` | 启用 FailFast（任一失败立即终止其余） | 关闭 |
| `.DefaultFailFast()` | 关闭 FailFast | — |
| `.Shards(n)` | 水平分片数（n ≤ 0 用 GOMAXPROCS） | 无分片 |
| `.DefaultShard()` | 使用默认分片（GOMAXPROCS，最少 2） | — |
| `.Chunk(size)` | 分块处理大小 | 无分块 |
| `.DefaultChunk()` | 使用默认分块（100） | — |
| `.Buf(size)` | Stream 缓冲区大小 | 0（自适应） |
| `.DefaultBuf()` | 使用默认流式缓冲 | — |
| `.Logger(l)` | 注入自定义日志（全局生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |

使用 `.Pool(p)` 时，正数的 `.Timeout(d)` 仅应用于当前切片操作中的每个任务，优先于池原有的任务超时，不会更改池的超时配置。`.FailFast()` 在任务函数返回错误或发生 panic 时取消当前操作的其他任务；任务执行前被池拒绝或丢弃不会触发取消。操作完成后池会复位以供复用，调用方负责关闭它。
池化 Map、ForEach 和 Stream 会等待池中所有已提交任务，包括调用方单独提交的任务，然后通过 `ResetWait` 清空池的结果和统计计数。因此操作期间不要让其他调用方并发提交、等待或复用该池；需要保留池结果或统计时，应在使用这些操作前另行处理。池的上下文若因等待而取消，复位不会重建它，下一批使用池上下文前须重新配置。
池设置了 `OverflowDrop` 或 `OverflowError` 且等待队列已满时，被拒绝的元素结果包含 `ErrQueueOverflow`；Map 的汇总错误在没有优先报告的其他错误时反映该错误，ForEach 将其计入失败数，Stream 会发送错误结果。取消上下文可能使 Stream 提前停止发送结果。
池化 Map 和 ForEach 的 FailFast 汇总优先报告触发取消的回调错误，避免其他任务的 `context.Canceled` 掩盖原始错误；并发失败时先记录的回调错误取决于调度。

串行 FailFast 提前停止后，未执行的元素标记为 `context.Canceled`；它们不计入成功值，ForEach 的失败数包含这些取消的元素。
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
| `.DefaultPool()` | 创建默认池 |
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

以下方法均可链式调用，在 `Map/ForEach` 执行之前对切片进行变换。所有三个构建器（`SliceBuilder`、`ParallelSlice`、`SerialSlice`）均共享这些方法。

### 排序

| 方法 | 说明 |
|------|------|
| `.Sort(cmp)` | 非稳定排序 |
| `.StableSort(cmp)` | 稳定排序 |
| `.IsSorted(cmp)` | 判断是否已排序 |
| `.Reverse()` | 原地反转 |

### 过滤与去重

| 方法 | 说明 |
|------|------|
| `.Filter(pred)` | 保留满足条件的元素（≥1000 自动并行） |
| `.DeleteFunc(pred)` | 删除满足条件的元素（与 Filter 语义相反） |
| `.Compact(eq)` | 移除相邻重复 |
| `.Dedup(eq)` | 全局去重（需 comparable） |

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
| `.Clip()` | 释放多余容量 |
| `.Grow(n)` | 扩展容量 |
| `.Shuffle()` | 随机打乱 |
| `.Repeat(n)` | 重复拼接 |

`Insert` 将负索引视为 0，将大于长度的索引视为末尾；插入值可以引用当前切片的底层数组。`Items()` 返回直接引用，后续原地修改可能改变该数组；需要独立切片时使用 `Values()`。

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
| `.Contains(pred)` | 是否包含（≥100 自动并行） |
| `.Index(pred)` | 第一个匹配的索引 |
| `.Find(pred)` | 第一个匹配的元素 |
| `.FindLast(pred)` | 最后一个匹配的元素 |
| `.All(pred)` | 是否全满足（≥100 自动并行） |
| `.Any(pred)` | 是否有满足（≥100 自动并行） |
| `.Count(pred)` | 满足条件的数量（≥100 自动并行） |

---

## 终端操作：Map

### 基本 Map（默认并行）

```go
results := async.Slice[int](ctx, []int{1, 2, 3, 4, 5}).
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
results := async.Slice[string](ctx, urls).
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

配置 `.Pool(p)` 后，Stream 使用该池执行任务，并在结果通道关闭前等待和复位池；调用方负责关闭池。`.PoolAuto(p)` 则在通道关闭前关闭池。流执行期间不要并发复用同一个池，且应持续消费结果或取消传入的 context。

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

| 方法 | 说明 |
|------|------|
| `.Error()` | 优先返回操作整体错误；否则按结果顺序返回第一个元素错误 |
| `.Ok()` | 全部成功返回 true |
| `.Err()` | 有错误返回 true |
| `.Values()` | 所有成功的值切片 |
| `.Errors()` | 各失败元素的错误切片；FailFast 的首个错误摘要不会重复添加 |
| `.FailValues()` | 失败输入值（原始 T 类型元素）；`MapBatch` 返回失败批次中的全部元素 |
| `.Results()` | 原始 Result[R] 切片 |
| `.Must()` | 有错误时 panic |
| `.Unwrap()` | `([]R, error)` |
| `.Len()` | 结果数量 |
| `.First()` | 第一个成功值 |

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
| `.Total()` | 总任务数 |
| `.FailCount()` | 失败任务数 |
| `.OkCount()` | 成功任务数 |

> **注意**：`.OkCount()` 只包含成功完成的元素，失败元素不计入；调用结果访问方法前应先让 `ForEach` 返回。

---

## 模式切换完整示例

### 默认并行模式

不需要 `.Parallel()`，直接使用：

```go
results := async.Slice[int](ctx, items).
    Sort(func(a, b int) int { return a - b }).   // 数据操作
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
result := async.Slice[int](ctx, items).Serial().
    Filter(pred).ToParallel().Worker(8).Map(fn)
```

> **⚠️ SerialSlice → ParallelSlice 后可以继续链式数据操作**
>
> `SerialSlice` 和 `ParallelSlice` 都嵌入 `*sliceBase`，共享所有数据操作方法（Sort/Filter/Chunk/Values 等约 40 个方法）。切换到 `ParallelSlice` 后既可以继续数据操作，也可以配置并发参数。

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
| 默认池 | `.DefaultPool()` | **内部自动创建** | 注入外部池后恢复为默认 |
| 默认日志 | `.DefaultLogger()` | **静默** | `core.SetLogger(nil)` |

### 模式专属默认值

| 模式 | 专有 Default* | 说明 |
|------|--------------|------|
| SliceBuilder（默认并行） | 除 Logger 外全部可用 | 等同于 ParallelSlice |
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
| 小数据量（<100条）快速处理 | **Map / ForEach** | 无需复杂配置 |
| 大切片（>10000条）IO操作 | **Map + Shards** | 分片减少锁竞争 |
| 实时流式消费 | **Stream** | 边执行边消费 |
| 汇总/聚合 | **Reduce** | 始终串行，天然安全 |
| 需要按 key 分组处理 | **MapBatch / ForEachBatch** | 分组批量处理 |
| 外部已有协程池复用 | **Pool 注入** | 避免重复创建 worker |

### 并发度与分片数建议

| 数据量 | 推荐 Worker | 推荐 Shards | 理由 |
|--------|------------|------------|------|
| <100 | `NumCPU` | 不分片 | 小数据量无需分片 |
| 100~10000 | `IO()` | 不分片 | IO 密集型直接并发 |
| 10000~100000 | `IO()` | `GOMAXPROCS` | 分片降低锁竞争 |
| >100000 | `IOMulti(4)` | `GOMAXPROCS`~16 | 充分横向扩展 |

### Chunk 与 MapBatch 配合

```go
// 批量处理：每 500 个元素一批
results := async.Slice[string](ctx, millions).
    Worker(16).
    Shards(8).
    Chunk(500).                    // 每批 500 个
    MapBatch(func(ctx context.Context, batch []string) ([]Result[string], error) {
        return batchInsertDB(ctx, batch)
    })
```

### Stream 缓冲设置建议

| 数据量 | 推荐 Buf | 理由 |
|--------|---------|------|
| <1000 | 0（自适应） | 自动计算 |
| 1000~10000 | 2048 | 平衡内存与吞吐 |
| >10000 | 16384 | 最大默认值 |

### 生产推荐配置

```go
// ========== 小数据量（< 1 万）：标准 Map ==========
results := async.Slice[string](ctx, items).
    Worker(16).
    Timeout(30 * time.Second).          // 单元素超时
    FailFast().                         // 任一失败立即停止
    Map(func(ctx context.Context, item string) (Result, error) {
        return transform(ctx, item)
    })
values := results.Values()
if results.Err() {
    log.Printf("部分失败: %v", results.Error())
}

// ========== 中数据量（1~10 万）+ 分片：降低锁竞争 ==========
results := async.Slice[string](ctx, items).
    Worker(async.IO()).                 // CPU×2
    FailFast().
    Shards(16).                         // 16 路分片，锁竞争降低 16 倍
    Map(func(ctx context.Context, item string) (Result, error) {
        return ioBoundProcess(ctx, item)
    })
for _, r := range results.Results() {
    if r.Ok() { handle(r.Value) }
}

// ========== 大数据量（10~100 万）+ 流式消费：边处理边消费 ==========
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

// ========== 海量数据（> 100 万）+ 分块批量：减少提交开销 ==========
async.Slice[Record](ctx, millions).
    Worker(16).
    Shards(16).
    Chunk(500).                         // 每 500 条一批
    Timeout(60 * time.Second).          // 批量操作加大超时
    MapBatch(func(ctx context.Context, batch []Record) ([]int64, error) {
        return db.BatchInsert(ctx, batch)
    })
```

### 常见错误

- **❌ 忽略大切片的内存开销**：池默认保留最多 100K 个结果；池化 Map 为返回完整映射结果还会按输入数量保存结果。海量数据应考虑分批处理，并评估各批次的内存占用。
- **❌ 小切片过度分片**：Shards > 数据量/Worker 时无意义，分片数不超过并发度
- **❌ Reduce 期望并行**：Reduce 永远串行，不支持 Worker/Shards 配置
- **❌ ForEach 结果未检查**：`ForEachResult.Err()` 可能为 true，建议始终检查
- **❌ SliceBuilder 链式构建器复用**：构建器非线程安全，每次使用应创建新实例

---

## 自定义日志

```go
results := async.Slice[int](ctx, items).
    Logger(myLogger).   // 全局生效
    Worker(8).
    Map(fn)

results := async.Slice[int](ctx, items).
    Logger(myLogger).
    DefaultLogger().    // 全局恢复默认
    Map(fn)
```

---

## Map 容器（K-V 泛型容器）

`async.Map[K, V]` 是泛型 key-value 容器，支持链式调用。`K` 必须满足 `comparable` 约束，`V` 为任意类型。并发/串行处理由单独的 `MapChain` 提供。

> **并发访问**：`Map` 本身没有锁；并发访问中只要有写入，调用方必须同步所有访问，包括 `AsMap()` 和 `MapFromRef()` 暴露的共享 map。`MapChain` 直接引用输入 map，构建器不可并发修改，执行期间也不得并发修改输入 map；并行回调访问其他共享数据时需自行同步。

### 创建入口

| 函数 | 说明 |
|------|------|
| `async.NewMap[K,V]()` | 创建空容器 |
| `async.NewMapCap[K,V](capacity)` | 创建空容器，预分配 capacity 容量 |
| `async.MapFrom[K,V](m)` | 从原生 map 创建（浅拷贝键值对） |
| `async.MapFromRef[K,V](m)` | 从原生 map 创建（直接引用，不拷贝） |

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

#### 集合运算

| 方法 | 签名 | 说明 |
|------|------|------|
| `Intersect(other)` | `*Map[K,V]` | 返回与 other 的交集（新 Map） |
| `Union(other)` | `*Map[K,V]` | 返回与 other 的并集（新 Map） |
| `Diff(other)` | `*Map[K,V]` | 返回差集（当前有 other 无的键，新 Map） |
| `Equal(other)` | `bool` | 比较两个 Map 是否相等 |

#### 高级查询

| 方法 | 签名 | 说明 |
|------|------|------|
| `Find(fn)` | `(K, V, bool)` | 查找第一个满足 fn 的键值对 |
| `All(fn)` | `bool` | 检查所有条目是否都满足 fn |
| `Any(fn)` | `bool` | 检查是否存在满足 fn 的条目 |
| `Count(fn)` | `int` | 统计满足 fn 的条目数 |

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
diff := a.Diff(b)        // {1:"a"}

// 高级查询
k, v, found := m.Find(func(k string, v int) bool { return v > 5 })
all := m.All(func(k string, v int) bool { return v > 0 })   // true
any := m.Any(func(k string, v int) bool { return v == 2 })  // true
cnt := m.Count(func(k string, v int) bool { return v > 2 }) // 1
```

---

### MapChain（Map 并发操作链构建器）

`MapChain` 提供 map 的并发/串行处理能力，支持模式切换、分片、超时等配置。

#### 入口

| 函数 | 说明 |
|------|------|
| `async.NewMapChain[K,V](ctx, data)` | 创建 MapChain，输出类型与 V 一致 |
| `async.NewMapChainWith[K,V,R](ctx, data)` | 创建 MapChain，输出类型为 R |

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
| `.DefaultShard()` | 按 GOMAXPROCS 分片 | — |
| `.Buf(size)` | 设置 Stream 缓冲区 | 0（自适应） |
| `.DefaultBuf()` | 清除 Buf 设置 | — |
| `.Chunk(size)` | 设置分块大小 | 100 |
| `.DefaultChunk()` | 恢复默认分块大小 | — |
| `.Logger(l)` | 注入自定义日志（全局生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |

MapChain 的 `Worker` 和 `DefaultWorker` 只更改并发度，保留超时、FailFast、池和其他设置。`Pool` / `PoolAuto` 适用于 Map、ForEach、Stream 和批量操作；Filter 和 Reduce 保持串行，`PoolAuto` 的池在操作结束时关闭。
无池的 Map / ForEach / Stream 将条目分片，片内顺序执行；批量操作先按 `Chunk` 划分批次，再对批次分片。最多 `Worker` 个分片同时运行。池化操作由池控制并发；Filter 和 Reduce 保持串行。
MapChain 的模式切换同样创建新构建器并重置策略；旧构建器中设置的池不会转移，调用方须确保不再使用的旧池被关闭。

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
| `.Filter(fn)` | `(ctx, K, V) → bool` | `(*Map[K,V], error)` | 并发过滤 |
| `.Stream(fn, bufSize)` | `(ctx, K, V) → (R, error)` | `<-chan async.Result[R]` | 流式映射 |
| `.Reduce(initial, fn)` | `(ctx, R, K, V) → (R, error)` | `(R, error)` | 串行聚合 |
| `.MapBatch(fn)` | `(ctx, []MapEntry[K,V]) → (R, error)` | `*MapResult[K,R]` | 批量映射 |
| `.ForEachBatch(fn)` | `(ctx, []MapEntry[K,V]) → error` | `*MapForEachResult` | 批量遍历 |

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
| `Data()` | `map[K]R` | 返回结果 map |
| `Len()` | `int` | 返回条目数 |
| `Keys()` | `[]K` | 返回所有键 |
| `Values()` | `[]R` | 返回所有值 |
| `Error()` | `error` | 返回第一个错误 |
| `Err()` | `bool` | 是否有错误 |

`MapRes[K,R]` 是 `MapResult[K,R]` 的向后兼容别名。

#### MapForEachResult 方法

| 方法 | 签名 | 说明 |
|------|------|------|
| `Total()` | `int64` | 总处理数 |
| `FailCount()` | `int64` | 失败数 |
| `OkCount()` | `int64` | 成功数 |
| `Error()` | `error` | 第一个错误 |
| `Err()` | `bool` | 是否有错误 |

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

// 流式处理
ch := async.NewMapChainWith[string, int, string](ctx, data).
    Parallel().Worker(4).
    Stream(func(ctx context.Context, k string, v int) (string, error) {
        return k, nil
    }, 1024)

for r := range ch {
    fmt.Println(r.Value)
}

// 批量处理
result := async.NewMapChainWith[string, int, string](ctx, data).
    Chunk(2).
    MapBatch(func(ctx context.Context, batch []async.MapEntry[string, int]) (string, error) {
        return batch[0].Key, nil
    })
```

---

Map 的 `Stream` 将回调中的 panic 转为包含 `PanicError` 的结果。启用 `FailFast` 后，遇到错误会停止后续处理；并行模式下，已经开始的回调仍可能返回结果。

取消传入的 context 后，`Stream` 停止启动后续回调，阻塞的结果发送也可以退出；尚未发送的结果可能被丢弃。已开始的回调需要自行响应 context 或返回，通道才会关闭。提前停止消费时，应取消该 context。

### Map 容器 & MapChain 生产推荐配置

```go
// ========== MapChain 并发映射：标准 K-V 并行处理 ==========
data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}

result := async.NewMapChainWith[string, int, string](ctx, data).
    Worker(16).
    Shards(8).                              // 最多 8 个分片并行
    Timeout(30 * time.Second).              // 单条目超时
    FailFast().                             // 任一失败立即停止
    Map(func(ctx context.Context, k string, v int) (string, error) {
        return processKV(ctx, k, v)
    })
if result.Err() {
    log.Printf("部分失败: %v", result.Error())
}
fmt.Println(result.Data())                  // map[string]string

// ========== MapChain 流式消费：边处理边消费 ==========
ch := async.NewMapChainWith[string, int, string](ctx, data).
    Worker(16).Shards(8).
    Buf(4096).                              // channel 缓冲
    Stream(func(ctx context.Context, k string, v int) (string, error) {
        return processKV(ctx, k, v)
    }, 4096)
for r := range ch {
    if r.Err != nil {
        log.Printf("处理失败: %v", r.Err)
        continue
    }
    saveResult(r.Value)
}

// ========== MapChain 批量 + 分块：数据库批量操作 ==========
recordData := map[string]Record{ /* 按业务填充 */ }
batchResult := async.NewMapChainWith[string, Record, int64](ctx, recordData).
    Worker(8).Shards(4).
    Chunk(200).                             // 每 200 条一批
    Timeout(60 * time.Second).              // 批量操作加大超时
    MapBatch(func(ctx context.Context, batch []async.MapEntry[string, Record]) (int64, error) {
        return db.BatchUpsert(ctx, batch)
    })

// ========== MapChain 聚合（Reduce + 并发前处理）==========
result, err := async.NewMapChain[string, int](ctx, data).
    Worker(16).
    Timeout(30 * time.Second).
    FailFast().
    Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
        return acc + v, nil
    })
if err != nil {
    log.Printf("聚合失败: %v", err)
}
// 或先并发转换再串行聚合
r := async.NewMapChainWith[string, int, *Processed](ctx, data).
    Worker(16).Shards(8).FailFast().
    Map(func(ctx context.Context, k string, v int) (*Processed, error) {
        return heavyTransform(ctx, v), nil
    })
reduced, err := async.NewMapChainWith[string, *Processed, float64](ctx, r.Data()).
    Serial().                               // 聚合必须串行
    Reduce(0.0, func(ctx context.Context, acc float64, k string, v *Processed) (float64, error) {
        return acc + v.Score, nil
    })

// ========== Map 操作链（同步操作，构建查询 ==========
// 同步操作；共享容器并且存在写入时需要外部同步
m := async.NewMap[string, int]().
    Set("a", 1).Set("b", 2).Set("c", 3).
    Filter(func(k string, v int) bool { return v > 1 }).
    MapValues(func(k string, v int) int { return v * 10 })
// m.AsMap() → map["b":20 "c":30]
```
