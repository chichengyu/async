# MapReduce（数据并行）文档

## 概述

MapReduce 模块对集合数据并行处理，灵感来自函数式编程，但使用 goroutine 并发执行。

**核心特性**:
- 并发 Map / ForEach / Reduce
- Chunk（分块）处理大数据集
- Chunked（逐元素分块）精细控制内存
- FailFast 快速失败模式
- Timeout 超时控制
- FailFast + Timeout 组合（FFTimeout）
- 串行版本（Serial）
- Result 辅助函数

> **⚠️ Map / ForEach 不保序**
>
> `Map()` / `ForEach()` 并发执行不保证元素处理顺序。如果顺序重要，使用 `MapSerial()` / `ForEachSerial()`。
>
> **⚠️ Chunk 分块粒度**
>
> `Chunk` 将原始切片按块大小拆分，`Chunked` 是逐元素均匀分配。大数据集推荐 `Chunked` 避免某块过大导致内存峰值。
>
> **⚠️ FailFast 立即返回**
>
> `MapWithFailFast` / `ForEachWithFailFast` 任意一个元素失败即中断所有并发任务并返回错误，其他成功的结果会被丢弃。
>
> **⚠️ 零元素切片**
>
> 传入空切片时所有 Map / ForEach 函数返回空的 `[]Result[T]`，不会报错。

**包路径**: `github.com/chichengyu/async/mapreduce`

**顶层便捷封装**: 所有函数同时通过 `async.*` 在顶层包中暴露，使用方式为 `async.Map(...)`、`async.ForEach(...)` 等。

## 目录

- [概述](#概述)
- [函数命名约定](#函数命名约定)
- [公共类型](#公共类型)
  - [Result](#result)
- [Map（并发映射）](#map并发映射)
  - [Map](#map)
  - [MapWithFailFast](#mapwithfailfast)
  - [MapSerial](#mapserial)
  - [MapSerialFailFast](#mapserialfailfast)
- [ForEach（并发遍历）](#foreach并发遍历)
  - [ForEach](#foreach)
  - [ForEachWithFailFast](#foreachwithfailfast)
  - [ForEachSerial](#foreachserial)
  - [ForEachSerialFailFast](#foreachserialfailfast)
- [Reduce（串行归约）](#reduce串行归约)
  - [Reduce（串行）](#reduce串行)
- [Reduce（并发归约）](#reduce并发归约)
  - [Reduce（并发）](#reduce并发)
  - [DefaultReduce](#defaultreduce)
  - [ReduceWithFailFast](#reducewithfailfast)
  - [DefaultReduceWithFailFast](#defaultreducewithfailfast)
  - [ReduceWithTimeout](#reducewithtimeout)
  - [DefaultReduceWithTimeout](#defaultreducewithtimeout)
  - [ReduceWithFFTimeout](#reducewithfftimeout)
  - [DefaultReduceWithFFTimeout](#defaultreducewithfftimeout)
- [Chunk（切片分块）](#chunk切片分块)
  - [Chunk](#chunk)
  - [ChunkN](#chunkn)
- [Result 辅助函数](#result-辅助函数)
  - [ResultValues](#resultvalues)
  - [ResultErrors](#resulterrors)
  - [Every](#every)
  - [Some](#some)
  - [AnyError](#anyerror)
  - [PartitionResults](#partitionresults)
  - [Flat](#flat)
  - [OnlyErrors](#onlyerrors)
- [其他辅助函数](#其他辅助函数)
  - [Partition](#partition)
  - [Must](#must)
  - [SafeCall](#safecall)
  - [SafeCallVoid](#safecallvoid)
  - [SafeCallWithResult](#safecallwithresult)
- [Map 扩展变体](#map-扩展变体)
  - [DefaultMap](#defaultmap)
  - [DefaultMapWithFailFast](#defaultmapwithfailfast)
  - [MapWithTimeout](#mapwithtimeout)
  - [DefaultMapWithTimeout](#defaultmapwithtimeout)
  - [MapWithFFTimeout](#mapwithfftimeout)
  - [DefaultMapWithFFTimeout](#defaultmapwithfftimeout)
- [ForEach 扩展变体](#foreach-扩展变体)
  - [DefaultForEach](#defaultforeach)
  - [DefaultForEachWithFailFast](#defaultforeachwithfailfast)
  - [ForEachWithTimeout](#foreachwithtimeout)
  - [DefaultForEachWithTimeout](#defaultforeachwithtimeout)
  - [ForEachWithFFTimeout](#foreachwithfftimeout)
  - [DefaultForEachWithFFTimeout](#defaultforeachwithfftimeout)
- [MapChunk（分块 Map）](#mapchunk分块-map)
  - [MapChunk](#mapchunk)
  - [DefaultMapChunk](#defaultmapchunk)
  - [MapChunkWithFailFast](#mapchunkwithfailfast)
  - [DefaultMapChunkWithFailFast](#defaultmapchunkwithfailfast)
  - [MapChunkWithTimeout](#mapchunkwithtimeout)
  - [DefaultMapChunkWithTimeout](#defaultmapchunkwithtimeout)
  - [MapChunkWithFFTimeout](#mapchunkwithfftimeout)
  - [DefaultMapChunkWithFFTimeout](#defaultmapchunkwithfftimeout)
- [MapChunked（逐元素分块 Map）](#mapchunked逐元素分块-map)
  - [MapChunked](#mapchunked)
  - [DefaultMapChunked](#defaultmapchunked)
  - [MapChunkedWithFailFast](#mapchunkedwithfailfast)
  - [DefaultMapChunkedWithFailFast](#defaultmapchunkedwithfailfast)
  - [MapChunkedWithTimeout](#mapchunkedwithtimeout)
  - [DefaultMapChunkedWithTimeout](#defaultmapchunkedwithtimeout)
  - [MapChunkedWithFFTimeout](#mapchunkedwithfftimeout)
  - [DefaultMapChunkedWithFFTimeout](#defaultmapchunkedwithfftimeout)
- [ForEachChunk（分块 ForEach）](#foreachchunk分块-foreach)
  - [ForEachChunk](#foreachchunk)
  - [DefaultForEachChunk](#defaultforeachchunk)
  - [ForEachChunkWithFailFast](#foreachchunkwithfailfast)
  - [DefaultForEachChunkWithFailFast](#defaultforeachchunkwithfailfast)
  - [ForEachChunkWithTimeout](#foreachchunkwithtimeout)
  - [DefaultForEachChunkWithTimeout](#defaultforeachchunkwithtimeout)
  - [ForEachChunkWithFFTimeout](#foreachchunkwithfftimeout)
  - [DefaultForEachChunkWithFFTimeout](#defaultforeachchunkwithfftimeout)
- [ForEachChunked（逐元素分块 ForEach）](#foreachchunked逐元素分块-foreach)
  - [ForEachChunked](#foreachchunked)
  - [DefaultForEachChunked](#defaultforeachchunked)
  - [ForEachChunkedWithFailFast](#foreachchunkedwithfailfast)
  - [DefaultForEachChunkedWithFailFast](#defaultforeachchunkedwithfailfast)
  - [ForEachChunkedWithTimeout](#foreachchunkedwithtimeout)
  - [DefaultForEachChunkedWithTimeout](#defaultforeachchunkedwithtimeout)
  - [ForEachChunkedWithFFTimeout](#foreachchunkedwithfftimeout)
  - [DefaultForEachChunkedWithFFTimeout](#defaultforeachchunkedwithfftimeout)
- [变体选择指南](#变体选择指南)
- [完整示例](#完整示例)
- [性能基准](#性能基准)

---

## 函数命名约定

| 前缀/后缀 | 含义 | 示例 |
|-----------|------|------|
| `Default*` | 使用 `core.IO()` 作为默认并发度 | `DefaultMap` |
| `*WithFailFast` | 第一个任务失败时取消其余任务 | `MapWithFailFast` |
| `*WithTimeout` | 每个任务有独立超时限制 | `MapWithTimeout` |
| `*WithFFTimeout` | FailFast + Timeout 组合 | `MapWithFFTimeout` |
| `*Chunk` | fn 接收整个 chunk（批量处理） | `MapChunk` |
| `*Chunked` | fn 接收单个元素（逐元素） | `MapChunked` |
| `*Serial` | 串行执行 | `MapSerial` |

---

## 公共类型

### Result

```go
type Result[T any] struct {
    Value T
    Err   error
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `Value` | `T` | 结果值（仅 Err == nil 时有效） |
| `Err` | `error` | 错误（nil 表示成功） |

**方法**:

| 方法 | 签名 | 说明 |
|------|------|------|
| `Ok` | `func (r Result[T]) Ok() bool` | Err == nil 返回 true |
| `IsPanic` | `func (r Result[T]) IsPanic() bool` | 错误由 panic 导致返回 true |

```go
r := core.Result[int]{Value: 42, Err: nil}
if r.Ok() {
    fmt.Println(r.Value) // 42
}

r2 := core.Result[int]{Err: errors.New("fail")}
if !r2.Ok() {
    fmt.Println(r2.Err) // fail
}
```

---

## Map（并发映射）

### Map

```go
func Map[T any, R any](
    ctx context.Context,
    items []T,
    fn func(context.Context, T) (R, error),
    concurrency int,
) ([]core.Result[R], error)
```

并发处理切片中的每个元素，返回 Result 切片。内部使用 chunk 分块 + goroutine 池实现，panic 自动恢复并包装为 `PanicError`。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待处理的元素切片 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数，接收 ctx 和元素，返回结果和错误 |
| `concurrency` | `int` | 并发度，<=0 时使用 `core.IO()`；超过 len(items) 时截断为 len(items) |
| 返回 | `([]core.Result[R], error)` | 结果切片（保持原始顺序）和错误（非 FailFast 时始终为 nil） |

**使用示例**:

```go
items := []int{1, 2, 3, 4, 5}

results, err := mapreduce.Map(ctx, items, func(ctx context.Context, x int) (string, error) {
    return fmt.Sprintf("item-%d", x), nil
}, 4)
if err != nil {
    log.Fatal(err)
}
// results = [{Value:"item-1"}, {Value:"item-2"}, {Value:"item-3"}, {Value:"item-4"}, {Value:"item-5"}]

for _, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}

// 使用 async 顶层包
results2 := async.Map(ctx, items, async.IO(), func(ctx context.Context, x int) (string, error) {
    return fmt.Sprintf("item-%d", x), nil
})
```

**处理含错误的结果**:

```go
input := []int{1, 2, 3, 4, 5}
results, _ := mapreduce.Map(ctx, input, func(ctx context.Context, v int) (int, error) {
    if v%2 == 0 {
        return 0, fmt.Errorf("skip even: %d", v)
    }
    return v * 10, nil
}, 2)

for i, r := range results {
    if r.Ok() {
        fmt.Printf("[%d] 成功: %d\n", i, r.Value)  // [0] 10, [2] 30, [4] 50
    } else {
        fmt.Printf("[%d] 失败: %v\n", i, r.Err)    // [1] skip even: 2, [3] skip even: 4
    }
}
```

**空切片和 nil 处理**:

```go
// 空切片
results, _ := mapreduce.Map(ctx, []int{}, fn, 4)
// results = nil

// nil 切片
results, _ := mapreduce.Map(ctx, ([]int)(nil), fn, 4)
// results = nil
```

---

### MapWithFailFast

```go
func MapWithFailFast[T any, R any](
    ctx context.Context,
    items []T,
    fn func(context.Context, T) (R, error),
    concurrency int,
) ([]core.Result[R], error)
```

与 Map 相同，但第一个任务失败时取消其余任务。**返回第一个错误**。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待处理的元素切片 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| `concurrency` | `int` | 并发度，<=0 使用 core.IO() |
| 返回 | `([]core.Result[R], error)` | 结果切片和第一个错误（失败后剩余结果可能为零值） |

**内部机制**: 通过 `context.WithCancel` 创建可取消的子 context，任一 goroutine 失败时调用 `cancel()`，其他 goroutine 检测到 `ctx.Done()` 后跳过后续元素并设置 `Err = ctx.Err()`。

**使用示例**:

```go
results, err := mapreduce.MapWithFailFast(ctx, items, func(ctx context.Context, x int) (int, error) {
    if x == 500000 {
        return 0, errors.New("fail at 500000")
    }
    return x, nil
}, 8)
if err != nil {
    log.Printf("快速失败: %v", err)
}

// 通过 async 顶层包
results, err := async.MapWithFailFast(ctx, items, async.IO(), fn)
```

---

### MapSerial

```go
func MapSerial[T any, R any](
    ctx context.Context,
    items []T,
    fn func(context.Context, T) (R, error),
) ([]core.Result[R], error)
```

串行处理切片元素，返回 Result 切片。适合数据量小或需要严格顺序的场景。内部使用 `core.SafeCall` 自动捕获 panic。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待处理的元素切片 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `([]core.Result[R], error)` | 结果切片，非 FailFast 时 Err 始终为 nil |

**使用示例**:

```go
// 基础用法
results, err := mapreduce.MapSerial(ctx, items, fn)

// 通过 async 顶层包
results := async.MapSerial(ctx, items, fn)

// 小数据量有序处理
results, _ := mapreduce.MapSerial(ctx, []int{1, 2, 3}, func(ctx context.Context, n int) (int, error) {
    return n * 2, nil
})
// results = [{Value:2}, {Value:4}, {Value:6}]
```

---

### MapSerialFailFast

```go
func MapSerialFailFast[T any, R any](
    ctx context.Context,
    items []T,
    fn func(context.Context, T) (R, error),
) ([]core.Result[R], error)
```

串行带快速失败的 Map，首个失败立即返回，不继续处理后续元素。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待处理的元素切片 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `([]core.Result[R], error)` | 结果切片和第一个错误 |

**使用示例**:

```go
results, err := mapreduce.MapSerialFailFast(ctx, items, fn)
if err != nil {
    log.Printf("第 %d 个元素失败: %v", len(results)-1, err)
}

// 通过 async 顶层包
results, err := async.MapSerialFailFast(ctx, items, fn)
```

---

## ForEach（并发遍历）

### ForEach

```go
func ForEach[T any](
    ctx context.Context,
    items []T,
    fn func(context.Context, T) error,
    concurrency int,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

并发遍历切片，执行只返回 error 的函数。使用 Group 实现并发控制。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待遍历的元素切片 |
| `fn` | `func(context.Context, T) error` | 处理函数，只返回 error |
| `concurrency` | `int` | 并发度，<=0 使用 core.IO()；concurrency<=1 时走串行路径 |
| 返回 `total` | `int64` | 总任务数 |
| 返回 `failCnt` | `int64` | 失败任务数 |
| 返回 `firstErr` | `error` | 第一个错误（nil 表示全部成功） |
| 返回 `results` | `[]core.Result[struct{}]` | 每个任务的详细结果 |

**使用示例**:

```go
total, fail, firstErr, results := mapreduce.ForEach(ctx, msgs, func(ctx context.Context, msg string) error {
    return send(ctx, msg)
}, 10)
fmt.Printf("总数=%d 失败=%d\n", total, fail)

// 通过 async 顶层包
nr, err := async.ForEach(ctx, msgs, async.IO(), func(ctx context.Context, msg string) error {
    return send(ctx, msg)
})
if err != nil {
    log.Printf("发送失败: %v", err)
}
fmt.Printf("成功: %d, 失败: %d\n", nr.SuccessCount(), nr.FailCount())
```

**空切片和 nil 处理**:

```go
total, _, _, _ := mapreduce.ForEach(ctx, []int{}, fn, 4)
// total = 0

total, _, _, _ := mapreduce.ForEach(ctx, ([]int)(nil), fn, 4)
// total = 0
```

**panic 自动恢复**:

```go
total, failCnt, _, _ := mapreduce.ForEach(ctx, []int{1, 2, 3}, func(ctx context.Context, v int) error {
    if v == 2 {
        panic("foreach panic at 2")
    }
    return nil
}, 2)
// total = 3, failCnt >= 1
```

---

### ForEachWithFailFast

```go
func ForEachWithFailFast[T any](
    ctx context.Context,
    items []T,
    fn func(context.Context, T) error,
    concurrency int,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

与 ForEach 相同，但第一个任务失败时取消其余任务。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待遍历元素 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| `concurrency` | `int` | 并发度，<=0 使用 core.IO() |
| 返回 | `(int64, int64, error, []core.Result[struct{}])` | 统计信息 |

**使用示例**:

```go
var processed atomic.Int64
total, failCnt, firstErr, _ := mapreduce.ForEachWithFailFast(ctx, items, func(ctx context.Context, v int) error {
    processed.Add(1)
    if v == 2 {
        return fmt.Errorf("fail fast at %d", v)
    }
    return nil
}, 2)
// total = len(items), failCnt >= 1, firstErr != nil

// 通过 async 顶层包
nr, err := async.ForEachWithFailFast(ctx, items, async.IO(), fn)
```

---

### ForEachSerial

```go
func ForEachSerial[T any](
    ctx context.Context,
    items []T,
    fn func(context.Context, T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

串行遍历切片，逐个执行并收集错误。内部使用 `core.SafeCallVoid` 自动捕获 panic。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待遍历元素 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回 | 统计信息 | 同 ForEach |

**使用示例**:

```go
total, failCnt, firstErr, _ := mapreduce.ForEachSerial(ctx, []int{10, 20, 30}, func(ctx context.Context, v int) error {
    return nil
})
// total = 3, failCnt = 0, firstErr = nil

// 通过 async 顶层包
nr, err := async.ForEachSerial(ctx, items, fn)
```

---

### ForEachSerialFailFast

```go
func ForEachSerialFailFast[T any](
    ctx context.Context,
    items []T,
    fn func(context.Context, T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

串行带快速失败的 ForEach，首个错误立即返回。内部使用 `core.SafeCallVoid` + `context.WithCancel` 实现。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待遍历元素 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回 | 统计信息 | 同 ForEach |

**使用示例**:

```go
total, failCnt, firstErr, _ := mapreduce.ForEachSerialFailFast(ctx, items, fn)

// 通过 async 顶层包
nr, err := async.ForEachSerialFailFast(ctx, items, fn)
```

---

## Reduce（串行归约）

### Reduce（串行）

```go
func Reduce[T any, R any](
    ctx context.Context,
    items []T,
    initial R,
    fn func(context.Context, R, T) (R, error),
) (R, error)
```

串行聚合：对每个元素调用 `fn(acc, item)`，累积结果。**Reduce 始终串行执行**。这是 mapreduce.go 中定义的纯串行 Reduce。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，自动注入 TraceID |
| `items` | `[]T` | 待聚合的元素切片 |
| `initial` | `R` | 初始累加值 |
| `fn` | `func(context.Context, R, T) (R, error)` | 聚合函数，接收 ctx、累加值和当前元素 |
| 返回 | `(R, error)` | 最终累加值和可能的错误 |

**使用示例**:

```go
// 数值求和
nums := []int{1, 2, 3, 4, 5}
total, err := mapreduce.Reduce(ctx, nums, 0, func(ctx context.Context, acc int, n int) (int, error) {
    return acc + n, nil
})
// total = 15

// 空切片返回初始值
result, _ := mapreduce.Reduce(ctx, []int{}, 100, func(ctx context.Context, acc int, v int) (int, error) {
    return acc + v, nil
})
// result = 100

// 单元素
result, _ := mapreduce.Reduce(ctx, []int{42}, 0, func(ctx context.Context, acc int, v int) (int, error) {
    return acc + v, nil
})
// result = 42

// 字符串拼接
result, _ := mapreduce.Reduce(ctx, []string{"a", "b", "c"}, "", func(ctx context.Context, acc string, v string) (string, error) {
    return acc + v, nil
})
// result = "abc"

// 自定义初始值计算平均值
avg, err := mapreduce.Reduce(ctx, ratings, 0.0, func(ctx context.Context, acc float64, r float64) (float64, error) {
    return acc + r / float64(len(ratings)), nil
})
```

**中途失败**:

```go
sum, err := mapreduce.Reduce(ctx, nums, 0, func(ctx context.Context, acc int, n int) (int, error) {
    if n < 0 {
        return acc, fmt.Errorf("negative number: %d", n)
    }
    return acc + n, nil
})
if err != nil {
    // 返回已累积的结果和错误
}
```

---

## Reduce（并发归约）

> **注意**：上文 [Reduce（串行）](#reduce串行) 是串行版本（`fn(acc, item)`）。本节是**并发版本**——先通过 Map 并发计算每个元素的值，再串行归约。

并发 Reduce 分两阶段执行：
1. **Map 阶段**：并发调用 `mapFn` 计算每个元素的值
2. **Reduce 阶段**：串行调用 `reduceFn` 累积成功的结果（失败值跳过）

### Reduce（并发）

```go
func Reduce[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    mapFn func(context.Context, T) (R, error),
    initial R,
    reduceFn func(R, R) R,
) (R, error)
```

并发 Map + 串行 Reduce。先对每个元素并发调用 `mapFn`，再用 `reduceFn` 累积所有成功值（失败值跳过）。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理的元素切片 |
| `concurrency` | `int` | Map 阶段的并发度 |
| `mapFn` | `func(context.Context, T) (R, error)` | Map 阶段处理函数 |
| `initial` | `R` | 聚合初始值 |
| `reduceFn` | `func(R, R) R` | 聚合函数，接收累积值和当前值 |
| 返回 | `(R, error)` | 最终累积值和第一个错误 |

**使用示例**:

```go
// 并发计算所有数的平方和：1² + 2² + 3² + 4² + 5² = 55
sum, err := mapreduce.Reduce(ctx, []int{1, 2, 3, 4, 5}, 4,
    func(ctx context.Context, n int) (int, error) {
        return n * n, nil
    },
    0,
    func(acc, val int) int {
        return acc + val
    },
)
// sum = 55

// 通过 async 顶层包
sum, err := async.Reduce(ctx, []int{1, 2, 3, 4, 5}, async.IO(),
    func(ctx context.Context, n int) (int, error) { return n * n, nil },
    0,
    func(acc, val int) int { return acc + val },
)
```

---

### DefaultReduce

```go
func DefaultReduce[T any, R any](
    ctx context.Context,
    items []T,
    mapFn func(context.Context, T) (R, error),
    initial R,
    reduceFn func(R, R) R,
) (R, error)
```

使用默认 IO 并发度的并发 Reduce。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `mapFn` | `func(context.Context, T) (R, error)` | Map 阶段处理函数 |
| `initial` | `R` | 聚合初始值 |
| `reduceFn` | `func(R, R) R` | 聚合函数 |
| 返回 | `(R, error)` | 最终结果和错误 |

**使用示例**:

```go
sum, err := mapreduce.DefaultReduce(ctx, nums, mapFn, 0, reduceFn)

// 通过 async 顶层包
sum, err := async.DefaultReduce(ctx, nums, mapFn, 0, reduceFn)
```

---

### ReduceWithFailFast

```go
func ReduceWithFailFast[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    mapFn func(context.Context, T) (R, error),
    initial R,
    reduceFn func(R, R) R,
) (R, error)
```

带 FailFast 的并发 Reduce。任一 Map 任务失败立即取消其余任务。失败时返回 `initial` 和第一个错误。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | Map 阶段并发度 |
| `mapFn` | `func(context.Context, T) (R, error)` | Map 阶段处理函数 |
| `initial` | `R` | 聚合初始值 |
| `reduceFn` | `func(R, R) R` | 聚合函数 |
| 返回 | `(R, error)` | 失败时返回 initial 和第一个错误 |

**使用示例**:

```go
sum, err := mapreduce.ReduceWithFailFast(ctx, nums, 8, mapFn, 0, reduceFn)
if err != nil {
    log.Printf("计算失败，任一元素出错则快速终止: %v", err)
}

// 通过 async 顶层包
sum, err := async.ReduceWithFailFast(ctx, nums, async.IO(), mapFn, 0, reduceFn)
```

---

### DefaultReduceWithFailFast

```go
func DefaultReduceWithFailFast[T any, R any](
    ctx context.Context,
    items []T,
    mapFn func(context.Context, T) (R, error),
    initial R,
    reduceFn func(R, R) R,
) (R, error)
```

使用默认并发度的 FailFast Reduce。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `mapFn` | `func(context.Context, T) (R, error)` | Map 阶段处理函数 |
| `initial` | `R` | 聚合初始值 |
| `reduceFn` | `func(R, R) R` | 聚合函数 |
| 返回 | `(R, error)` | 最终结果和错误 |

**使用示例**:

```go
sum, err := mapreduce.DefaultReduceWithFailFast(ctx, nums, mapFn, 0, reduceFn)

// 通过 async 顶层包
sum, err := async.DefaultReduceWithFailFast(ctx, nums, mapFn, 0, reduceFn)
```

---

### ReduceWithTimeout

```go
func ReduceWithTimeout[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    timeout time.Duration,
    mapFn func(context.Context, T) (R, error),
    initial R,
    reduceFn func(R, R) R,
) (R, error)
```

带单任务超时的并发 Reduce。超时任务的 mapFn 结果跳过不参与归约。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | Map 阶段并发度 |
| `timeout` | `time.Duration` | 单任务超时时间 |
| `mapFn` | `func(context.Context, T) (R, error)` | Map 阶段处理函数 |
| `initial` | `R` | 聚合初始值 |
| `reduceFn` | `func(R, R) R` | 聚合函数 |
| 返回 | `(R, error)` | 最终结果和第一个错误 |

**使用示例**:

```go
sum, err := mapreduce.ReduceWithTimeout(ctx, urls, 8, 3*time.Second, computeFn, 0, addFn)

// 通过 async 顶层包
sum, err := async.ReduceWithTimeout(ctx, urls, async.IO(), 3*time.Second, computeFn, 0, addFn)
```

---

### DefaultReduceWithTimeout

```go
func DefaultReduceWithTimeout[T any, R any](
    ctx context.Context,
    items []T,
    timeout time.Duration,
    mapFn func(context.Context, T) (R, error),
    initial R,
    reduceFn func(R, R) R,
) (R, error)
```

使用默认并发度的带超时 Reduce。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `timeout` | `time.Duration` | 单任务超时 |
| `mapFn` | `func(context.Context, T) (R, error)` | Map 阶段处理函数 |
| `initial` | `R` | 聚合初始值 |
| `reduceFn` | `func(R, R) R` | 聚合函数 |
| 返回 | `(R, error)` | 最终结果和错误 |

**使用示例**:

```go
sum, err := mapreduce.DefaultReduceWithTimeout(ctx, urls, 3*time.Second, computeFn, 0, addFn)

// 通过 async 顶层包
sum, err := async.DefaultReduceWithTimeout(ctx, urls, 3*time.Second, computeFn, 0, addFn)
```

---

### ReduceWithFFTimeout

```go
func ReduceWithFFTimeout[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    timeout time.Duration,
    mapFn func(context.Context, T) (R, error),
    initial R,
    reduceFn func(R, R) R,
) (R, error)
```

带 FailFast 和单任务超时的并发 Reduce。任一 Map 任务超时或失败立即取消其余任务。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | Map 阶段并发度 |
| `timeout` | `time.Duration` | 单任务超时 |
| `mapFn` | `func(context.Context, T) (R, error)` | Map 阶段处理函数 |
| `initial` | `R` | 聚合初始值 |
| `reduceFn` | `func(R, R) R` | 聚合函数 |
| 返回 | `(R, error)` | 失败时返回 initial 和第一个错误 |

**使用示例**:

```go
sum, err := mapreduce.ReduceWithFFTimeout(ctx, urls, 8, 2*time.Second, computeFn, 1.0, productFn)

// 通过 async 顶层包
sum, err := async.ReduceWithFFTimeout(ctx, urls, async.IO(), 2*time.Second, computeFn, 1.0, productFn)
```

---

### DefaultReduceWithFFTimeout

```go
func DefaultReduceWithFFTimeout[T any, R any](
    ctx context.Context,
    items []T,
    timeout time.Duration,
    mapFn func(context.Context, T) (R, error),
    initial R,
    reduceFn func(R, R) R,
) (R, error)
```

使用默认并发度的 FailFast + 超时 Reduce。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `timeout` | `time.Duration` | 单任务超时 |
| `mapFn` | `func(context.Context, T) (R, error)` | Map 阶段处理函数 |
| `initial` | `R` | 聚合初始值 |
| `reduceFn` | `func(R, R) R` | 聚合函数 |
| 返回 | `(R, error)` | 最终结果和错误 |

**使用示例**:

```go
sum, err := mapreduce.DefaultReduceWithFFTimeout(ctx, urls, 3*time.Second, computeFn, 1.0, productFn)
```

---

## Chunk（切片分块）

### Chunk

```go
func Chunk[T any](items []T, chunkSize int) [][]T
```

将切片按大小均分。适用于将大数据集拆分为并发处理的小块。

| 参数 | 类型 | 说明 |
|------|------|------|
| `items` | `[]T` | 待分割的元素切片 |
| `chunkSize` | `int` | 每块大小，<=0 或切片为空时返回 nil |
| 返回 | `[][]T` | 分块后的二维切片 |

**使用示例**:

```go
// 均匀分块
input := []int{1, 2, 3, 4, 5, 6}
chunks := mapreduce.Chunk(input, 2)
// chunks = [[1,2], [3,4], [5,6]]

// 不均匀分块
chunks = mapreduce.Chunk([]int{1, 2, 3, 4, 5}, 2)
// chunks = [[1,2], [3,4], [5]]

// 单元素
chunks = mapreduce.Chunk([]int{42}, 3)
// chunks = [[42]]

// 大数据集
bigSlice := make([]int, 1000000)
chunks = mapreduce.Chunk(bigSlice, 1000)
// 分成约 1000 块，每块约 1000 个元素

// 边界情况
chunks = mapreduce.Chunk([]int{}, 3)       // nil
chunks = mapreduce.Chunk(([]int)(nil), 3)  // nil
chunks = mapreduce.Chunk([]int{1, 2, 3}, 0) // nil

// 配合并发使用
for _, chunk := range chunks {
    g.Go(ctx, func(ctx context.Context) (Result, error) {
        return batchProcess(ctx, chunk), nil
    })
}

// 通过 async 顶层包
chunks := async.Chunk(bigSlice, 1000)
```

---

### ChunkN

```go
func ChunkN[T any](items []T, n int) [][]T
```

将切片按份数均分。`n` 为目标份数。内部先计算 `chunkSize = ceil(len(items) / n)`，再调用 `Chunk`。

| 参数 | 类型 | 说明 |
|------|------|------|
| `items` | `[]T` | 待分割的元素切片 |
| `n` | `int` | 目标份数，<=0 或切片为空时返回 nil |
| 返回 | `[][]T` | 分块后的二维切片 |

**使用示例**:

```go
// 将 100 个元素均分成 4 份
input := make([]int, 100)
chunks := mapreduce.ChunkN(input, 4)
// 4 个 chunk，每个约 25 个元素

// 将切片均分为 CPU 核心数份
chunks := mapreduce.ChunkN(items, runtime.NumCPU())

// 通过 async 顶层包
chunks := async.ChunkN(items, 5)
```

---

## Result 辅助函数

### ResultValues

```go
func ResultValues[T any](results []core.Result[T]) []T
```

从 Result 切片中提取所有成功值（跳过错误结果）。

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]core.Result[T]` | Result 切片 |
| 返回 | `[]T` | 成功值切片（仅包含 Err == nil 的 Value） |

**使用示例**:

```go
results := mapreduce.DefaultMap(ctx, items, fn)
vals := mapreduce.ResultValues(results)
// 只包含 Err == nil 的 Value

// 通过 async 顶层包
values := async.ResultValues(results)
```

---

### ResultErrors

```go
func ResultErrors[T any](results []core.Result[T]) []error
```

从 Result 切片中提取所有错误（跳过成功结果）。

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]core.Result[T]` | Result 切片 |
| 返回 | `[]error` | 错误切片（仅包含非 nil 错误） |

**使用示例**:

```go
errs := mapreduce.ResultErrors(results)

// 通过 async 顶层包
errs := async.ResultErrors(results)
if len(errs) > 0 {
    log.Printf("有 %d 个任务失败", len(errs))
}
```

---

### Every

```go
func Every[T any](results []core.Result[T]) bool
```

检查所有 Result 是否都成功。

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]core.Result[T]` | Result 切片 |
| 返回 | `bool` | 全部成功返回 true（空切片也返回 true） |

**使用示例**:

```go
if mapreduce.Every(results) {
    fmt.Println("全部成功")
}

// 通过 async 顶层包
if async.Every(results) {
    fmt.Println("全部成功")
}
```

---

### Some

```go
func Some[T any](results []core.Result[T]) bool
```

检查是否至少有一个 Result 成功。

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]core.Result[T]` | Result 切片 |
| 返回 | `bool` | 至少有一个成功返回 true |

**使用示例**:

```go
if mapreduce.Some(results) {
    fmt.Println("至少有一个成功")
}

// 通过 async 顶层包
if async.Some(results) {
    fmt.Println("至少有一个成功")
}
```

---

### AnyError

```go
func AnyError[T any](results []core.Result[T]) bool
```

检查是否有任何失败的 Result。

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]core.Result[T]` | Result 切片 |
| 返回 | `bool` | 有任何一个失败返回 true |

**使用示例**:

```go
if mapreduce.AnyError(results) {
    log.Println("存在失败的任务")
}

// 通过 async 顶层包
if async.AnyError(results) {
    fmt.Println("存在失败的任务")
}
```

---

### PartitionResults

```go
func PartitionResults[T any](results []core.Result[T]) (successes []T, failures []error)
```

将 Result 切片拆分为成功值和失败错误两个切片。

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]core.Result[T]` | Result 切片 |
| 返回 `successes` | `[]T` | 成功值切片 |
| 返回 `failures` | `[]error` | 失败错误切片 |

**使用示例**:

```go
successes, failures := mapreduce.PartitionResults(results)
fmt.Printf("成功: %d, 失败: %d\n", len(successes), len(failures))

// 通过 async 顶层包（函数名不同：Partition）
values, errors := async.Partition(results)
fmt.Printf("成功 %d 个, 失败 %d 个\n", len(values), len(errors))
```

---

### Flat

```go
func Flat[T any](results []core.Result[T]) []T
```

从 Result 切片中提取所有值，**丢弃错误（包括错误结果的零值也会被包含）**。

> **注意**：与 `ResultValues` 的区别是 `Flat` 不跳过错误结果的 Value（零值），而 `ResultValues` 跳过错结果。

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]core.Result[T]` | Result 切片 |
| 返回 | `[]T` | 所有 Value（包括错误结果的零值） |

**使用示例**:

```go
allVals := mapreduce.Flat(results)
// 结果数与 results 长度一致

// 通过 async 顶层包
values := async.Flat(results)
```

---

### OnlyErrors

```go
func OnlyErrors[T any](results []core.Result[T]) []error
```

从 Result 切片中提取所有非 nil 错误。

| 参数 | 类型 | 说明 |
|------|------|------|
| `results` | `[]core.Result[T]` | Result 切片 |
| 返回 | `[]error` | 所有非 nil 错误 |

**使用示例**:

```go
errs := mapreduce.OnlyErrors(results)

// 通过 async 顶层包
errs := async.OnlyErrors(results)
```

---

## 其他辅助函数

### Partition

```go
func Partition[T any](items []T, pred func(T) bool) (matched []T, unmatched []T)
```

按条件将切片拆分为匹配和不匹配两个切片。不会修改原始切片。

| 参数 | 类型 | 说明 |
|------|------|------|
| `items` | `[]T` | 原始切片 |
| `pred` | `func(T) bool` | 判断函数，true 进入 matched |
| 返回 `matched` | `[]T` | 匹配的元素切片 |
| 返回 `unmatched` | `[]T` | 不匹配的元素切片 |

**使用示例**:

```go
// 分离奇偶数
evens, odds := mapreduce.Partition([]int{1, 2, 3, 4, 5, 6}, func(n int) bool {
    return n%2 == 0
})
// evens = [2, 4, 6], odds = [1, 3, 5]

// 结合并发处理
items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
evens, odds := mapreduce.Partition(items, func(n int) bool { return n%2 == 0 })
evenResults := mapreduce.DefaultMap(ctx, evens, processEven)
oddResults := mapreduce.DefaultMap(ctx, odds, processOdd)
```

---

### Must

```go
func Must[T any](val T, err error) T
```

错误时 panic，适用于测试或初始化场景。

| 参数 | 类型 | 说明 |
|------|------|------|
| `val` | `T` | 结果值 |
| `err` | `error` | 可能的错误 |
| 返回 | `T` | val（err 为 nil 时） |

**使用示例**:

```go
cfg := mapreduce.Must(loadConfig("config.yaml"))

// 适用于变量初始化
db := mapreduce.Must(sql.Open("postgres", dsn))
```

---

### SafeCall

安全调用 fn，自动捕获 panic 并包装为 `PanicError`。使用 `core.SafeCall`。

```go
// 语法
func SafeCall[T any, R any](ctx context.Context, item T, fn func(context.Context, item T) (R, error)) (R, error)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `item` | `T` | 传递给 fn 的参数 |
| `fn` | `func(context.Context, T) (R, error)` | 要执行的函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `(R, error)` | `(R, error)` | 结果值和错误（panic 时包装为 PanicError） |

```go
val, err := async.SafeCall(ctx, input, func(ctx context.Context, item MyType) (string, error) {
    return item.DoSomething(ctx)
})
if err != nil {
    log.Printf("调用失败: %v", err)
}
```

### SafeCallVoid

安全调用 fn（无返回值），自动捕获 panic 转换为 error。使用 `core.SafeCallVoid`。

```go
// 语法
func SafeCallVoid[T any](ctx context.Context, item T, fn func(context.Context, item T) error) error
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `item` | `T` | 传递给 fn 的参数 |
| `fn` | `func(context.Context, T) error` | 要执行的函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `error` | `error` | 错误（panic 时包装为 PanicError，正常完成返回 nil） |

```go
err := async.SafeCallVoid(ctx, input, func(ctx context.Context, item MyType) error {
    return item.DoSomething(ctx)
})
```

### SafeCallWithResult

安全调用 fn，自动捕获 panic 并包装为 `PanicError`。除捕获 panic 外还额外包含 stack trace 日志记录。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `item` | `T` | 传递给 fn 的参数 |
| `fn` | `func(context.Context, T) (R, error)` | 要执行的函数 |
| 返回 `val` | `R` | 结果值（panic 时为零值） |
| 返回 `err` | `error` | 错误（panic 时包装为 PanicError） |

**使用示例**:

```go
val, err := mapreduce.SafeCallWithResult(ctx, input, func(ctx context.Context, item MyType) (string, error) {
    return item.Process(ctx)
})
if err != nil {
    var pe *core.PanicError
    if errors.As(err, &pe) {
        log.Printf("函数 panic 了: %v\n堆栈: %s", pe.Value, pe.Stack)
    }
}
```

---

## Map 扩展变体

扩展变体位于 `extended.go`，同时通过 `async.*` 在顶层包暴露。

### DefaultMap

```go
func DefaultMap[T any, R any](
    ctx context.Context,
    items []T,
    fn func(ctx context.Context, item T) (R, error),
) []core.Result[R]
```

使用 `core.IO()` 作为默认并发度的 Map。忽略 Map 返回的 error（因为 Map 非 FailFast 不会返回错误）。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理的元素切片 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `[]core.Result[R]` | 结果切片 |

**使用示例**:

```go
results := mapreduce.DefaultMap(ctx, ids, func(ctx context.Context, id int) (*User, error) {
    return userRepo.FindByID(ctx, id)
})

// 通过 async 顶层包
results := async.DefaultMap(ctx, ids, fn)
```

---

### DefaultMapWithFailFast

```go
func DefaultMapWithFailFast[T any, R any](
    ctx context.Context,
    items []T,
    fn func(ctx context.Context, item T) (R, error),
) ([]core.Result[R], error)
```

使用默认并发度的 FailFast Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `([]core.Result[R], error)` | 结果和第一个错误 |

**使用示例**:

```go
results, err := mapreduce.DefaultMapWithFailFast(ctx, ids, fn)

// 通过 async 顶层包
results, err := async.DefaultMapWithFailFast(ctx, ids, fn)
```

---

### MapWithTimeout

```go
func MapWithTimeout[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) (R, error),
) []core.Result[R]
```

带单任务超时的并发 Map。使用 Group 实现，超时任务的结果 Err 为 `context.DeadlineExceeded`。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度，<=0 使用 core.IO() |
| `timeout` | `time.Duration` | **每个任务的超时时间** |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `[]core.Result[R]` | 结果切片（超时任务 Err 为 DeadlineExceeded） |

**使用示例**:

```go
results := mapreduce.MapWithTimeout(ctx, urls, 8, 5*time.Second, func(ctx context.Context, url string) (*Body, error) {
    return httpGet(ctx, url)
})

// 通过 async 顶层包
results := async.MapWithTimeout(ctx, urls, async.IO(), 5*time.Second, fn)
```

---

### DefaultMapWithTimeout

```go
func DefaultMapWithTimeout[T any, R any](
    ctx context.Context,
    items []T,
    timeout time.Duration,
    fn func(ctx context.Context, item T) (R, error),
) []core.Result[R]
```

使用默认并发度的带超时 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `timeout` | `time.Duration` | 单任务超时 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `[]core.Result[R]` | 结果切片 |

**使用示例**:

```go
results := mapreduce.DefaultMapWithTimeout(ctx, ids, 3*time.Second, fn)

// 通过 async 顶层包
results := async.DefaultMapWithTimeout(ctx, ids, 3*time.Second, fn)
```

---

### MapWithFFTimeout

```go
func MapWithFFTimeout[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) (R, error),
) ([]core.Result[R], error)
```

带 FailFast 和单任务超时的并发 Map。任一任务超时或失败都会触发 FailFast 取消。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度，<=0 使用 core.IO() |
| `timeout` | `time.Duration` | 单任务超时 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `([]core.Result[R], error)` | 结果和第一个错误 |

**内部机制**: 通过 Group 的 `WithFailFast(ctx)` + `WithTimeout(timeout)` 组合实现。

**使用示例**:

```go
results, err := mapreduce.MapWithFFTimeout(ctx, urls, 8, 3*time.Second, func(ctx context.Context, url string) (*Body, error) {
    return httpGet(ctx, url)
})
if err != nil {
    log.Printf("首个失败: %v", err)
}

// 通过 async 顶层包
results, err := async.MapWithFFTimeout(ctx, urls, async.IO(), 3*time.Second, fn)
```

---

### DefaultMapWithFFTimeout

```go
func DefaultMapWithFFTimeout[T any, R any](
    ctx context.Context,
    items []T,
    timeout time.Duration,
    fn func(ctx context.Context, item T) (R, error),
) ([]core.Result[R], error)
```

使用默认并发度的 FailFast + 超时 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `timeout` | `time.Duration` | 单任务超时 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `([]core.Result[R], error)` | 结果和错误 |

**使用示例**:

```go
results, err := mapreduce.DefaultMapWithFFTimeout(ctx, ids, 3*time.Second, fn)

// 通过 async 顶层包
results, err := async.DefaultMapWithFFTimeout(ctx, ids, 3*time.Second, fn)
```

---

## ForEach 扩展变体

### DefaultForEach

```go
func DefaultForEach[T any](
    ctx context.Context,
    items []T,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待遍历元素 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回 | 统计信息 | 同 ForEach |

**使用示例**:

```go
total, fail, firstErr, _ := mapreduce.DefaultForEach(ctx, msgs, sendFn)

// 通过 async 顶层包
nr, err := async.DefaultForEach(ctx, msgs, sendFn)
```

---

### DefaultForEachWithFailFast

```go
func DefaultForEachWithFailFast[T any](
    ctx context.Context,
    items []T,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的 FailFast ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待遍历元素 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回 | 统计信息 | 同 ForEach |

**使用示例**:

```go
total, fail, firstErr, _ := mapreduce.DefaultForEachWithFailFast(ctx, tasks, fn)

// 通过 async 顶层包
nr, err := async.DefaultForEachWithFailFast(ctx, tasks, fn)
```

---

### ForEachWithTimeout

```go
func ForEachWithTimeout[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

带单任务超时的 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待遍历元素 |
| `concurrency` | `int` | 并发度 |
| `timeout` | `time.Duration` | 单任务超时 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回 | 统计信息 | 同 ForEach |

**使用示例**:

```go
total, fail, firstErr, _ := mapreduce.ForEachWithTimeout(ctx, msgs, 10, 2*time.Second, sendFn)

// 通过 async 顶层包
nr, err := async.ForEachWithTimeout(ctx, msgs, async.IO(), 2*time.Second, sendFn)
```

---

### DefaultForEachWithTimeout

```go
func DefaultForEachWithTimeout[T any](
    ctx context.Context,
    items []T,
    timeout time.Duration,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的带超时 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待遍历元素 |
| `timeout` | `time.Duration` | 单任务超时 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回 | 统计信息 | 同 ForEach |

**使用示例**:

```go
total, fail, firstErr, _ := mapreduce.DefaultForEachWithTimeout(ctx, msgs, 2*time.Second, sendFn)

// 通过 async 顶层包
nr, err := async.DefaultForEachWithTimeout(ctx, msgs, 2*time.Second, sendFn)
```

---

### ForEachWithFFTimeout

```go
func ForEachWithFFTimeout[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

带 FailFast 和单任务超时的 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待遍历元素 |
| `concurrency` | `int` | 并发度 |
| `timeout` | `time.Duration` | 单任务超时 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回 | 统计信息 | 同 ForEach |

**使用示例**:

```go
total, fail, firstErr, _ := mapreduce.ForEachWithFFTimeout(ctx, tasks, 8, 3*time.Second, fn)

// 通过 async 顶层包
nr, err := async.ForEachWithFFTimeout(ctx, tasks, async.IO(), 3*time.Second, fn)
```

---

### DefaultForEachWithFFTimeout

```go
func DefaultForEachWithFFTimeout[T any](
    ctx context.Context,
    items []T,
    timeout time.Duration,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的 FailFast + 超时 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待遍历元素 |
| `timeout` | `time.Duration` | 单任务超时 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回 | 统计信息 | 同 ForEach |

**使用示例**:

```go
total, fail, firstErr, _ := mapreduce.DefaultForEachWithFFTimeout(ctx, tasks, 3*time.Second, fn)

// 通过 async 顶层包
nr, err := async.DefaultForEachWithFFTimeout(ctx, tasks, 3*time.Second, fn)
```

---

## MapChunk（分块 Map）

`MapChunk` 系列函数的 fn 接收**整个 chunk 切片**，适合批量 RPC 调用、batch DB 操作等需要一次处理多条数据的场景。内部先调用 `Chunk` 分块，再对分块后的切片调用对应的 Map 函数。

### MapChunk

```go
func MapChunk[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    fn func(ctx context.Context, chunk []T) (R, error),
) []core.Result[R]
```

先按 `batchSize` 分块，再并发处理每个块。fn 接收整个 chunk。每个 chunk 生成一个 Result，所以结果数量 = chunk 数量。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度（处理 chunk 的并发数） |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, []T) (R, error)` | 处理函数，接收整个 chunk |
| 返回 | `[]core.Result[R]` | 结果切片（每个 chunk 一个结果） |

**使用示例**:

```go
results := mapreduce.MapChunk(ctx, items, 8, 100, fn)

// 通过 async 顶层包
results := async.MapChunk(ctx, items, 8, 100, fn)
```

---

### DefaultMapChunk

```go
func DefaultMapChunk[T any, R any](
    ctx context.Context,
    items []T,
    batchSize int,
    fn func(ctx context.Context, chunk []T) (R, error),
) []core.Result[R]
```

使用默认并发度（= `len(chunkedItems)`，即 chunk 数量）的分块 Map。无需手动指定并发度。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, []T) (R, error)` | 处理函数，接收整个 chunk |
| 返回 | `[]core.Result[R]` | 结果切片（每个 chunk 一个结果） |

**使用示例**:

```go
results := mapreduce.DefaultMapChunk(ctx, items, 100, fn)

// 通过 async 顶层包
results := async.DefaultMapChunk(ctx, items, 100, fn)
```

> **⚠️ 默认并发度等于 chunk 数**
>
> `DefaultMapChunk` 使用 `len(chunkedItems)` 作为并发度。如果数据量大、chunk 数多（如 100 万条数据分 10000 个 chunk），会瞬间启动大量 goroutine。此时建议用 `MapChunk` 手动控制并发度。

---

### MapChunkWithFailFast

```go
func MapChunkWithFailFast[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    fn func(ctx context.Context, chunk []T) (R, error),
) ([]core.Result[R], error)
```

先按 `batchSize` 分块，再以 FailFast 模式并发处理每个块。**任何 chunk 处理失败时立即取消其余任务**。第二个返回值 `error` 表示第一个遇到的错误。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度（处理 chunk 的并发数） |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, []T) (R, error)` | 处理函数，接收整个 chunk |
| 返回1 | `[]core.Result[R]` | 结果切片（可能不完整，因为被提前取消） |
| 返回2 | `error` | 第一个遇到的错误，无错误时为 nil |

**使用示例**:

```go
results, err := mapreduce.MapChunkWithFailFast(ctx, items, 4, 100, fn)
if err != nil {
    log.Printf("chunk processing failed early: %v", err)
}

// 通过 async 顶层包
results, err := async.MapChunkWithFailFast(ctx, items, 4, 100, fn)
```

---

### DefaultMapChunkWithFailFast

```go
func DefaultMapChunkWithFailFast[T any, R any](
    ctx context.Context,
    items []T,
    batchSize int,
    fn func(ctx context.Context, chunk []T) (R, error),
) ([]core.Result[R], error)
```

使用默认并发度的 FailFast 分块 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, []T) (R, error)` | 处理函数 |
| 返回1 | `[]core.Result[R]` | 结果切片 |
| 返回2 | `error` | 第一个遇到的错误 |

**使用示例**:

```go
results, err := mapreduce.DefaultMapChunkWithFailFast(ctx, items, 100, fn)
```

---

### MapChunkWithTimeout

```go
func MapChunkWithTimeout[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, chunk []T) (R, error),
) []core.Result[R]
```

先分块，再给每个 chunk 添加超时并并发处理。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单 chunk 超时时间 |
| `fn` | `func(context.Context, []T) (R, error)` | 处理函数 |
| 返回 | `[]core.Result[R]` | 结果切片（超时的 chunk 结果为 `context.DeadlineExceeded`） |

**使用示例**:

```go
results := mapreduce.MapChunkWithTimeout(ctx, items, 4, 100, 5*time.Second, fn)
```

---

### DefaultMapChunkWithTimeout

```go
func DefaultMapChunkWithTimeout[T any, R any](
    ctx context.Context,
    items []T,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, chunk []T) (R, error),
) []core.Result[R]
```

使用默认并发度的带超时分块 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单 chunk 超时时间 |
| `fn` | `func(context.Context, []T) (R, error)` | 处理函数 |
| 返回 | `[]core.Result[R]` | 结果切片 |

**使用示例**:

```go
results := mapreduce.DefaultMapChunkWithTimeout(ctx, items, 100, 5*time.Second, fn)
```

---

### MapChunkWithFFTimeout

```go
func MapChunkWithFFTimeout[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, chunk []T) (R, error),
) ([]core.Result[R], error)
```

先分块，再以 FailFast + 超时模式并发处理每个块。任何 chunk 超时或失败时立即取消其余任务。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单 chunk 超时时间 |
| `fn` | `func(context.Context, []T) (R, error)` | 处理函数 |
| 返回1 | `[]core.Result[R]` | 结果切片 |
| 返回2 | `error` | 第一个遇到的错误 |

**使用示例**:

```go
results, err := mapreduce.MapChunkWithFFTimeout(ctx, items, 4, 100, 5*time.Second, fn)
if err != nil {
    log.Printf("chunk processing failed: %v", err)
}
```

---

### DefaultMapChunkWithFFTimeout

```go
func DefaultMapChunkWithFFTimeout[T any, R any](
    ctx context.Context,
    items []T,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, chunk []T) (R, error),
) ([]core.Result[R], error)
```

使用默认并发度的 FailFast + 超时分块 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单 chunk 超时时间 |
| `fn` | `func(context.Context, []T) (R, error)` | 处理函数 |
| 返回1 | `[]core.Result[R]` | 结果切片 |
| 返回2 | `error` | 第一个遇到的错误 |

**使用示例**:

```go
results, err := mapreduce.DefaultMapChunkWithFFTimeout(ctx, items, 100, 5*time.Second, fn)
```

---

## MapChunked（逐元素分块 Map）

`MapChunked` 系列函数先分块，但 fn 接收**单个元素**，效果等同于先分块再对每块的每个元素并发处理。适合需要在分块粒度控制内存但 fn 仍是逐元素操作的场景（如分块读取文件后逐行处理）。**结果数组长度 = items 数量**。

### MapChunked

```go
func MapChunked[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    fn func(ctx context.Context, item T) (R, error),
) []core.Result[R]
```

先按 `batchSize` 分块，再对每块的每个元素并发执行 fn。结果按原始顺序排列，数量等于 `len(items)`。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小（用于控制单次处理的内存占用） |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数，接收单个元素 |
| 返回 | `[]core.Result[R]` | 结果切片（长度 = len(items)） |

**使用示例**:

```go
results := mapreduce.MapChunked(ctx, items, 8, 200, func(ctx context.Context, item Item) (Result, error) {
    return processItem(ctx, item)
})

// 通过 async 顶层包
results := async.MapChunked(ctx, items, 8, 200, fn)
```

---

### DefaultMapChunked

```go
func DefaultMapChunked[T any, R any](
    ctx context.Context,
    items []T,
    batchSize int,
    fn func(ctx context.Context, item T) (R, error),
) []core.Result[R]
```

使用默认并发度的逐元素分块 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `[]core.Result[R]` | 结果切片 |

**使用示例**:

```go
results := mapreduce.DefaultMapChunked(ctx, items, 200, fn)
```

---

### MapChunkedWithFailFast

```go
func MapChunkedWithFailFast[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    fn func(ctx context.Context, item T) (R, error),
) ([]core.Result[R], error)
```

逐元素分块 Map + FailFast 模式。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回1 | `[]core.Result[R]` | 结果切片 |
| 返回2 | `error` | 第一个遇到的错误 |

**使用示例**:

```go
results, err := mapreduce.MapChunkedWithFailFast(ctx, items, 8, 200, fn)
if err != nil {
    log.Printf("processing failed early: %v", err)
}
```

---

### DefaultMapChunkedWithFailFast

```go
func DefaultMapChunkedWithFailFast[T any, R any](
    ctx context.Context,
    items []T,
    batchSize int,
    fn func(ctx context.Context, item T) (R, error),
) ([]core.Result[R], error)
```

使用默认并发度的 FailFast 逐元素分块 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回1 | `[]core.Result[R]` | 结果切片 |
| 返回2 | `error` | 第一个遇到的错误 |

**使用示例**:

```go
results, err := mapreduce.DefaultMapChunkedWithFailFast(ctx, items, 200, fn)
```

---

### MapChunkedWithTimeout

```go
func MapChunkedWithTimeout[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) (R, error),
) []core.Result[R]
```

逐元素分块 Map + 超时模式。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单元素超时时间 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `[]core.Result[R]` | 结果切片（超时元素结果为 `context.DeadlineExceeded`） |

**使用示例**:

```go
results := mapreduce.MapChunkedWithTimeout(ctx, items, 8, 200, 3*time.Second, fn)
```

---

### DefaultMapChunkedWithTimeout

```go
func DefaultMapChunkedWithTimeout[T any, R any](
    ctx context.Context,
    items []T,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) (R, error),
) []core.Result[R]
```

使用默认并发度的带超时逐元素分块 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单元素超时时间 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回 | `[]core.Result[R]` | 结果切片 |

**使用示例**:

```go
results := mapreduce.DefaultMapChunkedWithTimeout(ctx, items, 200, 3*time.Second, fn)
```

---

### MapChunkedWithFFTimeout

```go
func MapChunkedWithFFTimeout[T any, R any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) (R, error),
) ([]core.Result[R], error)
```

逐元素分块 Map + FailFast + 超时模式。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单元素超时时间 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回1 | `[]core.Result[R]` | 结果切片 |
| 返回2 | `error` | 第一个遇到的错误 |

**使用示例**:

```go
results, err := mapreduce.MapChunkedWithFFTimeout(ctx, items, 8, 200, 3*time.Second, fn)
```

---

### DefaultMapChunkedWithFFTimeout

```go
func DefaultMapChunkedWithFFTimeout[T any, R any](
    ctx context.Context,
    items []T,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) (R, error),
) ([]core.Result[R], error)
```

使用默认并发度的 FailFast + 超时逐元素分块 Map。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单元素超时时间 |
| `fn` | `func(context.Context, T) (R, error)` | 处理函数 |
| 返回1 | `[]core.Result[R]` | 结果切片 |
| 返回2 | `error` | 第一个遇到的错误 |

**使用示例**:

```go
results, err := mapreduce.DefaultMapChunkedWithFFTimeout(ctx, items, 200, 3*time.Second, fn)
```

---

## ForEachChunk（分块 ForEach）

`ForEachChunk` 系列函数的 fn 接收**整个 chunk 切片**，只返回 error。适用于分块执行无返回值的副作用操作（如批量写入、批量删除）。内部先调用 `Chunk` 分块，再对分块后的切片调用对应的 ForEach 函数。返回值包含总数、失败数、首个错误、详细结果。

**ForEachChunk 的返回签名与 MapChunk 不同**：所有 ForEach/ForEachChunk/ForEachChunked 系列函数的返回值统一为 `(total int64, failCnt int64, firstErr error, results []core.Result[struct{}])`。

### ForEachChunk

```go
func ForEachChunk[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    fn func(ctx context.Context, chunk []T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

先按 `batchSize` 分块，再并发处理每个块，fn 只返回 error。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, []T) error` | 处理函数，接收整个 chunk，只返回 error |
| 返回1 | `int64` | total：总 chunk 数 |
| 返回2 | `int64` | failCnt：失败 chunk 数 |
| 返回3 | `error` | firstErr：第一个遇到的错误 |
| 返回4 | `[]core.Result[struct{}]` | results：各 chunk 的处理结果详情 |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.ForEachChunk(ctx, items, 4, 100, func(ctx context.Context, batch []Item) error {
    return batchInsert(ctx, batch)
})
if firstErr != nil {
    log.Printf("batch insert: %d/%d failed, first error: %v", failCnt, total, firstErr)
}

// 通过 async 顶层包
total, failCnt, firstErr, results := async.ForEachChunk(ctx, items, 4, 100, fn)
```

---

### DefaultForEachChunk

```go
func DefaultForEachChunk[T any](
    ctx context.Context,
    items []T,
    batchSize int,
    fn func(ctx context.Context, chunk []T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的分块 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, []T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunk |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.DefaultForEachChunk(ctx, items, 100, fn)
```

---

### ForEachChunkWithFailFast

```go
func ForEachChunkWithFailFast[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    fn func(ctx context.Context, chunk []T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

分块 ForEach + FailFast 模式。任何 chunk 失败时立即取消其余任务。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, []T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunk（FailFast 下结果可能不完整） |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.ForEachChunkWithFailFast(ctx, items, 4, 100, fn)
```

---

### DefaultForEachChunkWithFailFast

```go
func DefaultForEachChunkWithFailFast[T any](
    ctx context.Context,
    items []T,
    batchSize int,
    fn func(ctx context.Context, chunk []T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的 FailFast 分块 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, []T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunk |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.DefaultForEachChunkWithFailFast(ctx, items, 100, fn)
```

---

### ForEachChunkWithTimeout

```go
func ForEachChunkWithTimeout[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, chunk []T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

分块 ForEach + 超时模式。每个 chunk 有独立的超时限制。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单 chunk 超时时间 |
| `fn` | `func(context.Context, []T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunk |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.ForEachChunkWithTimeout(ctx, items, 4, 100, 10*time.Second, fn)
```

---

### DefaultForEachChunkWithTimeout

```go
func DefaultForEachChunkWithTimeout[T any](
    ctx context.Context,
    items []T,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, chunk []T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的带超时分块 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单 chunk 超时时间 |
| `fn` | `func(context.Context, []T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunk |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.DefaultForEachChunkWithTimeout(ctx, items, 100, 10*time.Second, fn)
```

---

### ForEachChunkWithFFTimeout

```go
func ForEachChunkWithFFTimeout[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, chunk []T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

分块 ForEach + FailFast + 超时模式。任何 chunk 超时或失败时立即取消其余任务。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单 chunk 超时时间 |
| `fn` | `func(context.Context, []T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunk |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.ForEachChunkWithFFTimeout(ctx, items, 4, 100, 10*time.Second, fn)
```

---

### DefaultForEachChunkWithFFTimeout

```go
func DefaultForEachChunkWithFFTimeout[T any](
    ctx context.Context,
    items []T,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, chunk []T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的 FailFast + 超时分块 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单 chunk 超时时间 |
| `fn` | `func(context.Context, []T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunk |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.DefaultForEachChunkWithFFTimeout(ctx, items, 100, 10*time.Second, fn)
```

---

## ForEachChunked（逐元素分块 ForEach）

`ForEachChunked` 系列函数先分块，但 fn 接收**单个元素**，只返回 error。适合需要在分块粒度控制内存但 fn 仍是逐元素副作用操作的场景。内部先按 `batchSize` 分块，对每块内的元素逐个并发调用 `ForEach` / `ForEachWithFailFast`。**结果数组长度 = chunk 数量 = ceil(len(items)/batchSize)**。

### ForEachChunked

```go
func ForEachChunked[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

先按 `batchSize` 分块，再对每块内的元素逐一并发执行 fn。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小（控制单次处理的内存占用） |
| `fn` | `func(context.Context, T) error` | 处理函数，接收单个元素，只返回 error |
| 返回1 | `int64` | total：总任务数 |
| 返回2 | `int64` | failCnt：失败任务数 |
| 返回3 | `error` | firstErr：第一个遇到的错误 |
| 返回4 | `[]core.Result[struct{}]` | results：各 chunk 的处理结果详情（长度 = chunk 数） |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.ForEachChunked(ctx, items, 8, 200, func(ctx context.Context, item Item) error {
    return processItem(ctx, item)
})

// 通过 async 顶层包
total, failCnt, firstErr, results := async.ForEachChunked(ctx, items, 8, 200, fn)
```

---

### DefaultForEachChunked

```go
func DefaultForEachChunked[T any](
    ctx context.Context,
    items []T,
    batchSize int,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的逐元素分块 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunked |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.DefaultForEachChunked(ctx, items, 200, fn)
```

---

### ForEachChunkedWithFailFast

```go
func ForEachChunkedWithFailFast[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

逐元素分块 ForEach + FailFast 模式。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunked |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.ForEachChunkedWithFailFast(ctx, items, 8, 200, fn)
```

---

### DefaultForEachChunkedWithFailFast

```go
func DefaultForEachChunkedWithFailFast[T any](
    ctx context.Context,
    items []T,
    batchSize int,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的 FailFast 逐元素分块 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunked |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.DefaultForEachChunkedWithFailFast(ctx, items, 200, fn)
```

---

### ForEachChunkedWithTimeout

```go
func ForEachChunkedWithTimeout[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

逐元素分块 ForEach + 超时模式。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单元素超时时间 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunked |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.ForEachChunkedWithTimeout(ctx, items, 8, 200, 3*time.Second, fn)
```

---

### DefaultForEachChunkedWithTimeout

```go
func DefaultForEachChunkedWithTimeout[T any](
    ctx context.Context,
    items []T,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的带超时逐元素分块 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单元素超时时间 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunked |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.DefaultForEachChunkedWithTimeout(ctx, items, 200, 3*time.Second, fn)
```

---

### ForEachChunkedWithFFTimeout

```go
func ForEachChunkedWithFFTimeout[T any](
    ctx context.Context,
    items []T,
    concurrency int,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

逐元素分块 ForEach + FailFast + 超时模式。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `concurrency` | `int` | 并发度 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单元素超时时间 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunked |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.ForEachChunkedWithFFTimeout(ctx, items, 8, 200, 3*time.Second, fn)
```

---

### DefaultForEachChunkedWithFFTimeout

```go
func DefaultForEachChunkedWithFFTimeout[T any](
    ctx context.Context,
    items []T,
    batchSize int,
    timeout time.Duration,
    fn func(ctx context.Context, item T) error,
) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}])
```

使用默认并发度的 FailFast + 超时逐元素分块 ForEach。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `items` | `[]T` | 待处理元素 |
| `batchSize` | `int` | 每块大小 |
| `timeout` | `time.Duration` | 单元素超时时间 |
| `fn` | `func(context.Context, T) error` | 处理函数 |
| 返回1~4 | — | 同 ForEachChunked |

**使用示例**:

```go
total, failCnt, firstErr, results := mapreduce.DefaultForEachChunkedWithFFTimeout(ctx, items, 200, 3*time.Second, fn)
```

---

## 变体选择指南

### Map/ForEach 函数对照表

| 系列 | fn 接收 | 结果长度 | 适用场景 |
|------|---------|----------|----------|
| `Map` | 单个元素 | `len(items)` | 标准并发处理 |
| `MapChunk` | 整个 chunk | chunk 数 | 批量 RPC / 批量 DB 操作 |
| `MapChunked` | 单个元素 | `len(items)` | 分块控制内存，逐元素处理 |
| `ForEach` | 单个元素 | `len(items)` | 无返回值的副作用操作 |
| `ForEachChunk` | 整个 chunk | chunk 数 | 批量写入 / 批量删除 |
| `ForEachChunked` | 单个元素 | chunk 数 | 分块控制内存，逐元素副作用 |

### 后缀变体选择

| 后缀 | 用法 | 说明 |
|------|------|------|
| _（无后缀） | `Map(...)` | 基础并发版本，不提前终止 |
| `WithFailFast` | `MapWithFailFast(...)` | 第一个错误发生时立即取消所有剩余任务 |
| `WithTimeout` | `MapWithTimeout(...)` | 每个任务有独立的超时限制 |
| `WithFFTimeout` | `MapWithFFTimeout(...)` | FailFast + 超时模式，最先超时或出错即取消 |
| `Default` | `DefaultMap(...)` | 使用 `core.IO()` 自动推断并发度 |

### MapChunk vs MapChunked 的区别

| 维度 | MapChunk | MapChunked |
|------|----------|------------|
| fn 参数 | `[]T`（整个 chunk） | `T`（单个元素） |
| 结果数量 | chunk 数量 | `len(items)` |
| 内部实现 | 调用 `Map` | 内部遍历 chunk，对每个元素调用 `GoAt` |
| 适用场景 | 批量 RPC / batch DB | 分块控制内存，fn 不变 |

---

## 完整示例

### 批量 RPC 调用（MapChunk）

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/yourusername/async/mapreduce"
)

func main() {
    ctx := context.Background()
    userIDs := make([]int64, 10000)
    for i := range userIDs {
        userIDs[i] = int64(i + 1)
    }

    // 每批 100 个 ID，4 个并发 worker，FailFast + 每批 5s 超时
    results, err := mapreduce.MapChunkWithFFTimeout(ctx, userIDs, 4, 100, 5*time.Second,
        func(ctx context.Context, batch []int64) ([]User, error) {
            return batchQueryUsers(ctx, batch)
        },
    )
    if err != nil {
        log.Fatalf("batch query failed: %v", err)
    }

    // 汇总结果
    allUsers := make([]User, 0, len(userIDs))
    for _, r := range results {
        if r.Err != nil {
            log.Printf("batch failed: %v", r.Err)
            continue
        }
        allUsers = append(allUsers, r.Value...)
    }
    fmt.Printf("queried %d users\n", len(allUsers))
}
```

### 分布式 Reduce

```go
// 先并发计算所有分段的 local count，再串行 merge
chunks := mapreduce.Chunk(data, 10000)
counts, _ := mapreduce.MapWithFailFast(ctx, chunks, func(ctx context.Context, chunk []int64) (int64, error) {
    return localCount(chunk), nil
}, 8)

// 串行归约：计算最终总和
totalCount := mapreduce.Reduce(counts, func(ctx context.Context, acc int64, cnt int64) (int64, error) {
    return acc + cnt, nil
}, 0)
fmt.Printf("total count: %d\n", totalCount)
```

### 大文件逐块处理（ForEachChunked）

```go
lines := readAllLines("data.txt")
total, failCnt, firstErr, _ := mapreduce.ForEachChunked(ctx, lines, 8, 1000,
    func(ctx context.Context, line string) error {
        return processLine(ctx, line)
    },
)
if firstErr != nil {
    log.Printf("processed %d/%d lines, first error: %v", total-failCnt, total, firstErr)
}
```

---

## 性能基准

以下为典型场景的性能参考（仅供参考，实际性能取决于 fn 的具体实现和硬件环境）：

| 场景 | 数据量 | 函数 | 并发度 | 批次大小 | 预期耗时对比（vs 串行） |
|------|--------|------|--------|----------|------------------------|
| 单元素轻量计算 | 10000 | `Map` | 8 | — | ~8x 加速 |
| 批量 RPC | 10000 | `MapChunk` | 4 | 100 | ~80x 加速（减少 RPC 次数） |
| 大文件逐行处理 | 1000000 | `ForEachChunked` | 8 | 5000 | ~8x 加速，内存可控 |
| 分布式 Reduce | 1000000 | `Map` + `Reduce` | 16 | — | ~16x 加速（Map 阶段） |

**调优建议**：
- **IO 密集型**：并发度可设置为 CPU 核数的 2~4 倍
- **CPU 密集型**：并发度建议等于 CPU 核数
- **分块策略**：RPC/DB 场景 batchSize 建议 50~200；内存敏感场景按实际可用内存 / 单元素大小计算
- **超时时间**：建议设置为 P99 延迟的 2~3 倍
- **FailFast**：适合错误需要立即感知的场景；不适合需要尽力完成所有任务的场景
- **Panic 恢复**：所有函数内置 panic 恢复机制，会通过 `core.PanicError` 包装
- **Cancellation**：所有函数通过 context 支持取消、超时传播