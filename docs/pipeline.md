# 管道 (Pipeline)

管道模块提供多阶段数据处理能力，适用于 ETL、多步数据清洗、请求-响应处理链等场景。

## 目录

- [核心概念](#核心概念)
- [多阶段并发管道 (Execute)](#多阶段并发管道-execute)
- [带元信息的管道 (ExecuteWithMeta)](#带元信息的管道-executewithmeta)
- [使用 Group 的管道 (ExecuteWithGroup)](#使用-group-的管道-executewithgroup)
- [串行管道 (Pipeline)](#串行管道-pipeline)
- [完整示例](#完整示例)

---

## 核心概念

```
多阶段管道 (Execute)：
  输入 → [阶段1: 并发处理] → [阶段2: 并发处理] → [阶段3: 并发处理] → 输出
  前一阶段的所有输出 → 后一阶段的输入
  
串行管道 (Pipeline)：
  单个元素 → [阶段1] → [阶段2] → [阶段3] → 结果
  前一个阶段的单一输出 → 后一个阶段的输入
```

**两种管道的区别：**

| 特性 | Execute（多阶段） | Pipeline（串行） |
|------|-------------------|-------------------|
| 数据单位 | 切片（批量） | 单个元素 |
| 阶段间处理 | 并发处理整个切片 | 串行处理单个元素 |
| 适用场景 | ETL、批量数据清洗 | 请求处理链、中间件链 |

---

## 多阶段并发管道 (Execute)

`Execute` 依次执行多个阶段，每个阶段使用分块并发处理所有元素，前一阶段的全部输出作为后一阶段的全部输入。

### 定义阶段

```go
stages := []async.Stage[Record]{
    {Name: "parse", Concurrency: 4},     // 解析阶段：4 并发
    {Name: "enrich", Concurrency: 8},    // 增强阶段：8 并发
    {Name: "validate", Concurrency: 2},  // 校验阶段：2 并发
}
```

每个阶段定义：
- `Name`：阶段名称（用于日志和元信息追踪）
- `Concurrency`：该阶段的并发度（<=0 使用默认 IO 并发度）

### 执行管道

`Execute` 使用分块并发执行每个阶段：

```go
results, err := async.Execute(ctx, stages, rawRecords, func(ctx context.Context, stage string, r Record) (Record, error) {
    switch stage {
    case "parse":
        return parseRecord(ctx, r)
    case "enrich":
        return enrichRecord(ctx, r)
    case "validate":
        return validateRecord(ctx, r)
    }
    return r, nil
})

// results 只包含最后一个阶段的输出
for _, r := range results {
    if r.Ok() {
        fmt.Println("最终结果:", r.Value)
    } else {
        log.Printf("处理失败: %v", r.Err)
    }
}
```

**参数：**
- `ctx` — 上下文（控制取消和超时）
- `stages` — 阶段定义切片 `[]Stage[T]`
- `items` — 输入数据切片 `[]T`
- `fn` — 处理函数 `func(context.Context, string, T) (T, error)`（stage 参数为当前阶段名）

**返回：**
- `[]Result[T]` — 最后一个阶段的全部结果
- `error` — 整体错误

> **注意**：处理函数 `fn` 接收 `stage` 参数，需要在函数内部根据阶段名分发逻辑。

---

## 带元信息的管道 (ExecuteWithMeta)

`ExecuteWithMeta` 返回每个阶段、每个元素的处理结果，便于追踪和调试。**前一阶段的所有输出作为后一阶段的输入**（不累积旧数据）：

```go
metaResults := async.ExecuteWithMeta(ctx, stages, items, func(ctx context.Context, stage string, item string) (string, error) {
    switch stage {
    case "step1":
        return processStep1(ctx, item)
    case "step2":
        return processStep2(ctx, item)
    }
    return item, nil
})

// 每个元素在每个阶段都会产生一条记录，结果数 = len(items) × len(stages)
for _, mr := range metaResults {
    fmt.Printf("[%s] %v (err=%v)\n", mr.Stage, mr.Value, mr.Err)
}
```

**参数：** 同 `Execute`。

**返回：** `[]ResultWithMeta[T]` — 每个阶段每个元素一条记录。

`ResultWithMeta[T]` 结构：

```go
type ResultWithMeta[T] struct {
    Result[T]           // 嵌入标准 Result，包含 Value/Err/Ok/IsPanic
    Stage     string    // 产生此结果的阶段名称
}
```

---

## 使用 Group 的管道 (ExecuteWithGroup)

`ExecuteWithGroup` 使用 Group 执行单阶段并发处理，支持错误聚合。相当于自动扩缩容的 Map：

```go
results, err := async.ExecuteWithGroup(ctx, items, func(ctx context.Context, item Item) (Item, error) {
    return processItem(ctx, item)
}, async.IO())

fmt.Printf("处理完成: %d 个结果\n", len(results))
```

**参数：**
- `ctx` — 上下文
- `items` — 输入切片 `[]T`
- `fn` — 处理函数 `func(context.Context, T) (T, error)`
- `concurrency` — 并发度

**返回：**
- `[]core.Result[T]` — 所有元素的结果
- `error` — 整体错误

> 内部实现：创建 `Group[T]` → `GoAt` 逐元素提交 → `Wait`。先 FailFast Map，如果有元素失败则回退到非 FailFast 重跑失败元素。

---

## 串行管道 (Pipeline)

`Pipeline` 是串行管道，每个阶段串行处理单个元素，适合阶段间有严格依赖关系的场景。

### 创建与执行

```go
// 创建管道：乘以 2 → 加 1 → 转为字符串
p := async.NewPipeline[int](ctx,
    func(ctx context.Context, n int) (int, error) { return n * 2, nil },
    func(ctx context.Context, n int) (int, error) { return n + 1, nil },
)

// 执行单个输入
result, err := p.Run(5)
fmt.Println(result) // 11 = (5*2)+1
```

**`NewPipeline` 参数：**
- `ctx` — 上下文
- `stages` — 变长阶段函数 `func(context.Context, T) (T, error)`

**`Run` 参数：**
- `input` — 输入值 `T`
- 返回 `(T, error)` — 最终结果

### 管道方法

```go
// 设置 TraceID（影响后续 Run 调用）
p.WithTraceID(tracedCtx)

// 查看阶段数量
fmt.Println(p.Stages()) // 返回 int

---

## 完整示例

### 示例1：ETL 数据处理

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/chichengyu/async"
)

type RawData struct {
    ID    int
    Value string
}

type CleanData struct {
    ID       int
    Value    string
    IsValid  bool
    Enriched string
}

func main() {
    ctx := context.Background()
    ctx = async.EnsureTraceID(ctx)

    // 原始数据
    rawData := []RawData{
        {ID: 1, Value: "hello"},
        {ID: 2, Value: "world"},
        {ID: 3, Value: ""},
    }

    // 定义 ETL 阶段
    stages := []async.Stage[CleanData]{
        {Name: "parse", Concurrency: 4},
        {Name: "enrich", Concurrency: 2},
        {Name: "validate", Concurrency: 2},
    }

    results, err := async.Execute(ctx, stages, rawData, func(ctx context.Context, stage string, r RawData) (CleanData, error) {
        switch stage {
        case "parse":
            return CleanData{ID: r.ID, Value: r.Value, IsValid: r.Value != ""}, nil
        case "enrich":
            return CleanData{ID: r.ID, Value: r.Value, IsValid: true, Enriched: r.Value + "_processed"}, nil
        case "validate":
            if !r.IsValid {
                return CleanData{}, fmt.Errorf("数据 %d 无效", r.ID)
            }
            return r, nil
        }
        return CleanData{}, nil
    })

    if err != nil {
        log.Printf("管道执行出错: %v", err)
    }

    for _, r := range results {
        if r.Ok() {
            fmt.Printf("结果: %+v\n", r.Value)
        }
    }
}
```

### 示例2：请求处理链

```go
// 定义请求处理步骤
authenticate := func(ctx context.Context, req *http.Request) (*http.Request, error) {
    if req.Header.Get("Authorization") == "" {
        return nil, fmt.Errorf("未授权")
    }
    return req, nil
}

validateInput := func(ctx context.Context, req *http.Request) (*http.Request, error) {
    if req.Body == nil {
        return nil, fmt.Errorf("请求体为空")
    }
    return req, nil
}

logRequest := func(ctx context.Context, req *http.Request) (*http.Request, error) {
    log.Printf("[%s] %s %s", async.GetTraceID(ctx), req.Method, req.URL.Path)
    return req, nil
}

// 创建请求处理管道
p := async.NewPipeline(ctx, authenticate, validateInput, logRequest)

req, err := p.Run(inputRequest)
if err != nil {
    http.Error(w, err.Error(), 400)
    return
}
processRequest(w, req)
```

---

## 方法速查表

### 多阶段管道

| 函数 | 说明 |
|------|------|
| `Execute[T](ctx, stages, items, fn)` | 执行多阶段并发管道 |
| `ExecuteWithMeta[T](ctx, stages, items, fn)` | 执行管道并返回阶段元信息 |
| `ExecuteWithGroup[T](ctx, items, fn, c)` | 使用 Group 执行单阶段管道 |

### Stage 类型

| 字段 | 说明 |
|------|------|
| `Stage.Name` | 阶段名称 |
| `Stage.Concurrency` | 阶段并发度 |

### ResultWithMeta 类型

| 字段 | 说明 |
|------|------|
| `ResultWithMeta.Stage` | 阶段名称 |
| `ResultWithMeta.Value` | 结果值 |
| `ResultWithMeta.Err` | 错误 |

### 串行管道 (Pipeline)

| 方法 | 说明 |
|------|------|
| `NewPipeline[T](ctx, stages...)` | 创建串行管道 |
| `p.Run(input)` | 串行执行所有阶段 |
| `p.Stages()` | 返回阶段数量 |
| `p.WithTraceID(ctx)` | 设置 TraceID |