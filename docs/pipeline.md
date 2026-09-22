# Pipeline（管道）文档

## 概述

Pipeline 模块支持多阶段串行数据处理管道，每个阶段可以指定独立的并发度。阶阶段之间串行执行，阶段内部并发处理，前一个阶段的输出作为后一个阶段的输入。

**核心特性**:
- 多阶段串行管道编排
- 每个阶段独立并发度
- 阶段顺序保证（Stage 2 必须等待 Stage 1 完成）
- Metadata 携带（`ExecuteWithMeta`）
- Context 取消传播

---

## 目录

- [类型速查](#类型速查)
- [Pipeline（串行管道）](#pipeline串行管道)
  - [NewPipeline / Run / WithTraceID / Stages](#newpipeline)
- [Stage（阶段定义）](#stage阶段定义)
  - [Stage / ResultWithMeta](#stage)
- [Execute（执行管道）](#execute执行管道)
  - [Execute / ExecuteWithMeta / ExecuteWithGroup](#execute)
- [管道执行模型](#管道执行模型)
- [完整示例](#完整示例)
  - [ETL 管道 / 多阶段数据转换 / 带 Metadata 追踪 / 简单并发处理](#etl-管道)
- [并发度建议](#并发度建议)
- [性能基准](#性能基准)

## 类型速查

| 类型 | 说明 |
|------|------|
| `Stage[T]` | 管道阶段定义 |
| `ResultWithMeta[T]` | 带阶段信息的 Result |
| `Pipeline[T]` | 串行管道，各阶段严格顺序执行 |
| `Execute[T]` | 执行多阶段管道 |
| `ExecuteWithMeta[T]` | 执行管道并返回阶段元信息 |
| `ExecuteWithGroup[T]` | 使用 Group 并发执行所有元素 |

---

## Pipeline（串行管道）

与 `Execute` 的多阶段模型不同，`Pipeline` 采用更简单的串行模式：各阶段严格顺序执行，前一个阶段的输出是后一个阶段的输入。适合阶段间有严格依赖关系的场景。

### NewPipeline

创建串行管道，stages 按声明顺序执行。

```go
// 语法
func NewPipeline[T any](ctx context.Context, stages ...func(context.Context, T) (T, error)) *Pipeline[T]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | Context，用于取消和 TraceID 传播 |
| `stages` | `...func(context.Context, T) (T, error)` | 按顺序执行的处理函数 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `*Pipeline[T]` | `*Pipeline[T]` | Pipeline 实例 |

```go
p := async.NewPipeline[int](ctx,
    func(ctx context.Context, n int) (int, error) { return n * 2, nil },
    func(ctx context.Context, n int) (int, error) { return n + 1, nil },
)
result, err := p.Run(5)
// 结果: 11 = (5*2)+1
fmt.Println(result) // 11
```

### Run

串行执行所有阶段。

```go
// 语法
func (p *Pipeline[T]) Run(input T) (T, error)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `input` | `T` | 初始输入值 |

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `(T, error)` | `(T, error)` | 最终结果和错误（首个阶段返回错误时立即中断后续阶段） |

```go
p := async.NewPipeline[string](ctx,
    func(ctx context.Context, s string) (string, error) {
        return strings.ToUpper(s), nil
    },
    func(ctx context.Context, s string) (string, error) {
        return s + "!", nil
    },
)
result, err := p.Run("hello") // "HELLO!"
```

### WithTraceID

替换 Pipeline 使用的 context（用于注入 trace_id）。

```go
// 语法
func (p *Pipeline[T]) WithTraceID(ctx context.Context)
```

```go
p := async.NewPipeline[int](ctx, stage1, stage2)
traceCtx := async.WithTraceID(context.Background(), "my-trace-123")
p.WithTraceID(traceCtx)
result, _ := p.Run(42)
```

### Stages

返回管道中的阶段数量。

```go
// 语法
func (p *Pipeline[T]) Stages() int
```

```go
p := async.NewPipeline[int](ctx, stage1, stage2, stage3)
fmt.Println(p.Stages()) // 3
```

---

## Stage（阶段定义）

### Stage

```go
type Stage[T any] struct {
    Name        string
    Concurrency int
}
```

管道中的一个处理阶段。

| 字段 | 类型 | 说明 |
|------|------|------|
| `Name` | `string` | 阶段名称，可用于日志和 `ExecuteWithMeta` 中的阶段标识 |
| `Concurrency` | `int` | 该阶段的并发度，<=0 时使用默认 IO 并发度（`runtime.NumCPU() × 2`） |

```go
stages := []pipeline.Stage[int]{
    {Name: "multiply", Concurrency: 4}, // 阶段1：乘以2，4个并发
    {Name: "add", Concurrency: 2},      // 阶段2：加1，2个并发
}
```

---

### ResultWithMeta

```go
type ResultWithMeta[T any] struct {
    core.Result[T]
    Stage string
}
```

带阶段信息的 Result，由 `ExecuteWithMeta` 返回。每个元素在每一阶段处理后都会生成一条记录。

| 字段 | 类型 | 说明 |
|------|------|------|
| `Result[T]` | `core.Result[T]` | 嵌入的标准 Result，包含 `Value` 和 `Err` |
| `Stage` | `string` | 产生此结果的阶段名称 |

---

## Execute（执行管道）

### Execute

```go
// 语法
func Execute[T any](
    ctx context.Context,
    stages []Stage[T],
    initialItems []T,
    fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error)
```

依次执行各阶段，前一个阶段的输出作为后一个阶段的输入。每个阶段用分块并发的方式处理所有元素。

**执行流程**：
1. 传入 `initialItems` 作为初始数据
2. 对每个 `Stage`，按 `Concurrency` 分块并发调用 fn
3. 当前阶段所有结果收集完毕后，作为下一阶段的输入
4. 最终返回所有阶段处理完成后的结果

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，控制全局取消 |
| `stages` | `[]Stage[T]` | 阶段定义列表，按数组顺序依次执行 |
| `initialItems` | `[]T` | 初始数据切片 |
| `fn` | `func(context.Context, string, T) (T, error)` | 处理函数，接收 ctx、阶段名和当前元素，返回处理后的元素 |
| 返回 | `([]core.Result[T], error)` | 最终结果切片（长度与 initialItems 一致）和错误 |

```go
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

for _, r := range results {
    if r.Ok() {
        fmt.Println("处理后:", r.Value)
    } else {
        log.Printf("处理失败: %v", r.Err)
    }
}
```

---

### ExecuteWithMeta

```go
// 语法
func ExecuteWithMeta[T any](
    ctx context.Context,
    stages []Stage[T],
    initialItems []T,
    fn func(ctx context.Context, stage string, item T) (T, error),
) []ResultWithMeta[T]
```

和 `Execute` 类似，但返回带 stage 信息的 `ResultWithMeta`。每个阶段处理后的每个元素都会生成一条 `ResultWithMeta` 记录，便于追踪每项在各阶段的处理情况。

**注意**：如果有 N 个元素和 M 个阶段，返回的切片长度为 `N × M`（每个元素在每阶段都产生一条记录）。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文 |
| `stages` | `[]Stage[T]` | 阶段定义列表 |
| `initialItems` | `[]T` | 初始数据切片 |
| `fn` | `func(context.Context, string, T) (T, error)` | 处理函数 |
| 返回 | `[]ResultWithMeta[T]` | 带阶段元信息的结果切片 |

```go
// 追踪每个元素在各阶段的处理情况
metaResults := pipeline.ExecuteWithMeta(ctx, stages, items, fn)
for _, mr := range metaResults {
    fmt.Printf("阶段=%s 值=%v 错误=%v\n", mr.Stage, mr.Value, mr.Err)
}
```

---

### ExecuteWithGroup

```go
// 语法
func ExecuteWithGroup[T any](
    ctx context.Context,
    items []T,
    fn func(ctx context.Context, item T) (T, error),
    concurrency int,
) ([]core.Result[T], error)
```

使用 Group 并发处理所有元素，支持错误聚合。与 `Execute` 不同，这是单阶段并发处理，不涉及多阶段流水线。

**适用场景**：一次性并发处理所有元素，需要统一获取错误信息。

| 参数 | 类型 | 说明 |
|------|------|------|
| `ctx` | `context.Context` | 上下文，控制全局取消 |
| `items` | `[]T` | 待处理的元素切片 |
| `fn` | `func(context.Context, T) (T, error)` | 处理函数，接收 ctx 和元素 |
| `concurrency` | `int` | 并发度，<=0 使用默认 IO 并发度 |
| 返回 | `([]core.Result[T], error)` | 结果切片和错误 |

```go
// 用 Group 并发处理所有元素
results, err := pipeline.ExecuteWithGroup(ctx, items, func(ctx context.Context, item string) (string, error) {
    return processItem(ctx, item)
}, 8)
if err != nil {
    log.Printf("处理出错: %v", err)
}
```

---

## 管道执行模型

```
输入切片 [item1, item2, item3, item4]
   │
   ▼
┌────────────────────────────┐
│  Stage 1 (Concurrency=2)  │
│  ├─ goroutine 0: item0-1  │
│  └─ goroutine 1: item2-3  │
│  输出: [r1, r2, r3, r4]   │
└─────────────┬──────────────┘
              ▼
┌────────────────────────────┐
│  Stage 2 (Concurrency=4)  │
│  ├─ goroutine 0: item0    │
│  ├─ goroutine 1: item1    │
│  ├─ goroutine 2: item2    │
│  └─ goroutine 3: item3    │
│  输出: [r1', r2', r3', r4'] │
└─────────────┬──────────────┘
              ▼
         最终结果
```

- **阶段间串行**：Stage 2 必须等待 Stage 1 完全完成后才能开始
- **阶段内并发**：每个 Stage 内部按 Concurrency 分块并发处理
- **Context 传播**：任何阶段发生错误不会自动取消后续阶段，依赖 fn 内部检查 ctx

---

## 完整示例

### ETL 管道

```go
// Extract: 从文件读取
extract := func(ctx context.Context, stage string, filename string) (*Record, error) {
    records := readFile(filename)
    return records, nil
}

// Transform: 数据清洗
transform := func(ctx context.Context, stage string, r *Record) (*CleanRecord, error) {
    clean, err := cleanRecord(r)
    if err != nil {
        return nil, err
    }
    return clean, nil
}

// Load: 写入数据库
load := func(ctx context.Context, stage string, r *CleanRecord) (int64, error) {
    id, err := db.Insert(ctx, r)
    return id, err
}
```

### 多阶段数据转换管道

```go
stages := []pipeline.Stage[int]{
    {Name: "multiply", Concurrency: 4},
    {Name: "add", Concurrency: 2},
    {Name: "format", Concurrency: 1},
}

items := []int{1, 2, 3, 4, 5}

results, err := pipeline.Execute(ctx, stages, items, func(ctx context.Context, stage string, n int) (int, error) {
    switch stage {
    case "multiply":
        return n * 2, nil
    case "add":
        return n + 1, nil
    case "format":
        // format 阶段串行执行（Concurrency=1）
        return n, nil
    }
    return n, nil
})
// items: [1,2,3,4,5]
// multiply: [2,4,6,8,10]
// add: [3,5,7,9,11]
// format: [3,5,7,9,11]
```

### 带 Metadata 追踪

```go
stages := []pipeline.Stage[string]{
    {Name: "parse", Concurrency: 2},
    {Name: "validate", Concurrency: 4},
}

inputs := []string{"data1", "data2", "data3"}

metaResults := pipeline.ExecuteWithMeta(ctx, stages, inputs, fn)
// 返回 6 条记录：
// [{Stage:"parse" Value:...}, {Stage:"parse" Value:...}, {Stage:"parse" Value:...},
//  {Stage:"validate" Value:...}, {Stage:"validate" Value:...}, {Stage:"validate" Value:...}]

for _, mr := range metaResults {
    if mr.Err != nil {
        log.Printf("阶段[%s]处理失败: %v", mr.Stage, mr.Err)
    }
}
```

### 简单并发处理（ExecuteWithGroup）

```go
items := []string{"a", "b", "c", "d", "e"}

results, _ := pipeline.ExecuteWithGroup(ctx, items, func(ctx context.Context, s string) (string, error) {
    return strings.ToUpper(s), nil
}, 4)
// results = [{Value:"A"}, {Value:"B"}, {Value:"C"}, {Value:"D"}, {Value:"E"}]
```

---

## 并发度建议

| 阶段类型 | 推荐 Concurrency | 说明 |
|----------|-----------------|------|
| CPU 密集型（解析、编码） | `runtime.NumCPU()` | 不超过 CPU 核心数 |
| IO 密集型（读文件、网络请求） | `runtime.NumCPU() × 2` | 可超额订阅 |
| 需要顺序保证 | `1` | 串行执行该阶段 |
| 高吞吐阶段 | `runtime.NumCPU() × 4` | 极高并发场景 |

---

## 性能基准

| 场景 | 数据量 | 吞吐量 |
|------|--------|--------|
| Pipeline 2 阶段 | 10M | **85M ops/s** |