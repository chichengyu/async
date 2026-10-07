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
| `.Timeout(d)` | 单任务超时 | 30s |
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
| `.Error()` | 第一个错误（遍历所有结果） |
| `.Ok()` | 全部成功返回 true |
| `.Err()` | 有错误返回 true |
| `.Values()` | 所有成功的值切片 |
| `.Errors()` | 所有错误切片 |
| `.FailValues()` | 失败输入值（原始 T 类型元素） |
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
| `.SuccessCount()` | 成功任务数 |

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
sb := async.Slice[int](ctx, items)
// 串行模式做确定性操作
sb.Serial().Sort(cmp).Filter(pred)
// 切回并行做批量处理
result := sb.Parallel().Worker(8).Map(fn)

// 或者链式：
serial := async.Slice[int](ctx, items).Serial()
serial.Filter(pred)
result := serial.ToParallel().Worker(8).Map(fn)
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
> - `Shards(n)`：将切片均匀分成 n 份，每份由一个独立 goroutine 全权处理。适合减少锁竞争。

---

## Pool 注入

将任务提交到外部协程池中执行：

```go
// 方式一：Pool(p)，用户管理生命周期
p := pool.NewPool[string](16)
defer p.Close()

results := async.Slice[string](ctx, items).
    Pool(p).
    Map(processFunc)

// 方式二：PoolAuto(p)，自动 Close
results := async.Slice[string](ctx, items).
    PoolAuto(pool.NewPool[string](16)).
    Map(processFunc)

// 方式三：DefaultPool()，内部创建并自动管理
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
| 默认超时 | `.DefaultTimeout()` | **30s** | 全局默认超时 |
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

- **❌ 大切片不设 MaxResults 用 Wait 取全部**：默认 100K 可能 OOM，建议用 Stream 流式消费或配合 Chunk 批量处理
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

`async.Map[K, V]` 是泛型 key-value 容器，支持链式调用和并发/串行处理。`K` 必须满足 `comparable` 约束，`V` 为任意类型。

### 创建入口

| 函数 | 说明 |
|------|------|
| `async.NewMap[K,V]()` | 创建空容器 |
| `async.NewMapCap[K,V](capacity)` | 创建空容器，预分配 capacity 容量 |
| `async.MapFrom[K,V](m)` | 从原生 map 创建（深拷贝） |
| `async.MapFromRef[K,V](m)` | 从原生 map 创建（直接引用，不拷贝） |

```go
m := async.NewMap[string, int]()
m.Set("a", 1).Set("b", 2)

// 深拷贝
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
| `.Timeout(d)` | 设置单任务超时 | 30s |
| `.DefaultTimeout()` | 恢复默认超时 | — |
| `.FailFast()` | 启用快速失败 | 关闭 |
| `.DefaultFailFast()` | 关闭快速失败 | — |
| `.Shards(n)` | 设置水平分片数 | 不分片 |
| `.DefaultShard()` | 按 GOMAXPROCS 分片 | — |
| `.Buf(size)` | 设置 Stream 缓冲区 | 0（自适应） |
| `.DefaultBuf()` | 清除 Buf 设置 | — |
| `.Chunk(size)` | 设置分块大小 | 100 |
| `.DefaultChunk()` | 恢复默认分块大小 | — |
| `.Logger(l)` | 注入自定义日志（全局生效） | 静默 |
| `.DefaultLogger()` | 恢复默认日志 | — |

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
| `.Stream(fn, bufSize)` | `(ctx, K, V) → (R, error)` | `<-chan core.Result[R]` | 流式映射 |
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
| `SuccessCount()` | `int64` | 成功数 |
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
result := async.NewMapChain[string, int](ctx, data).
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
ch := async.NewMapChain[string, int](ctx, data).
    Parallel().Worker(4).
    Stream(func(ctx context.Context, k string, v int) (string, error) {
        return k, nil
    }, 1024)

for r := range ch {
    fmt.Println(r.Value)
}

// 批量处理
result := async.NewMapChain[string, int](ctx, data).
    Chunk(2).
    MapBatch(func(ctx context.Context, batch []async.MapEntry[string, int]) (string, error) {
        return batch[0].Key, nil
    })
```

---

### Map 容器 & MapChain 生产推荐配置

```go
// ========== MapChain 并发映射：标准 K-V 并行处理 ==========
data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}

result := async.NewMapChain[string, int](ctx, data).
    Worker(16).
    Shards(8).                              // 8 分片降低锁竞争
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
ch := async.NewMapChain[string, int](ctx, data).
    Worker(16).Shards(8).
    Buf(4096).                              // channel 缓冲
    Stream(func(ctx context.Context, k string, v int) (string, error) {
        return processKV(ctx, k, v)
    }, 4096)
for r := range ch {
    if r.Err != nil {
        log.Printf("处理失败 key=%s: %v", r.Value, r.Err)
        continue
    }
    saveResult(r.Value)
}

// ========== MapChain 批量 + 分块：数据库批量操作 ==========
result := async.NewMapChain[string, Record](ctx, data).
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
r, _ := async.NewMapChainWith[string, int](ctx, data).  // With 改变输出类型
    Worker(16).Shards(8).FailFast().
    Map(func(ctx context.Context, k string, v int) (*Processed, error) {
        return heavyTransform(ctx, v), nil
    })
reduced, err := async.NewMapChain[string, *Processed](ctx, r.Data()).
    Serial().                               // 聚合必须串行
    Reduce(0.0, func(ctx context.Context, acc float64, k string, v *Processed) (float64, error) {
        return acc + v.Score, nil
    })

// ========== Map 操作链（同步操作，构建查询 ==========
// 不涉及并发，结构 线程安全
m := async.NewMap[string, int]().
    Set("a", 1).Set("b", 2).Set("c", 3).
    Filter(func(k string, v int) bool { return v > 1 }).
    MapValues(func(k string, v int) int { return v * 10 })
// m.AsMap() → map["b":20 "c":30]
```