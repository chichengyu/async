# 数据并行 (Map / ForEach / Reduce / Chunk)

数据并行模块提供对切片元素的并发处理操作，包括 `Map`、`ForEach`、`Reduce`、`Chunk` 等，是 async 库最常用的功能之一。

## 目录

- [Map - 并发映射](#map---并发映射)
- [ForEach - 并发遍历](#foreach---并发遍历)
- [Reduce - 并发聚合](#reduce---并发聚合)
- [Chunk - 分块处理](#chunk---分块处理)
- [Result 辅助函数](#result-辅助函数)
- [完整示例](#完整示例)

---

## Map - 并发映射

对切片的每个元素并发执行转换函数，返回 `[]Result[R]`。

### 基础用法

```go
results := async.Map(ctx, items, async.IO(), func(ctx context.Context, n int) (string, error) {
    return fmt.Sprintf("result-%d", n*2), nil
})

// 结果与输入顺序一致（底层用 chunk 分片实现）
for i, r := range results {
    if r.Ok() {
        fmt.Println(r.Value)
    } else {
        log.Printf("第 %d 个元素失败: %v", i, r.Err)
    }
}
```

**参数说明：**

| 参数 | 说明 |
|------|------|
| `ctx` | 上下文（控制取消和超时） |
| `items` | 输入元素切片 |
| `concurrency` | 并发度（建议用 `async.IO()` 或 `async.CPU()`） |
| `fn` | 处理函数：接收 ctx 和元素，返回处理结果 |

### 变体一览

| 函数 | FailFast | 超时 | 说明 |
|------|----------|------|------|
| `Map` | ❌ | ❌ | 基础并发映射 |
| `MapWithFailFast` | ✅ | ❌ | 首个失败立即停止 |
| `MapWithTimeout` | ❌ | ✅ | 指定总超时 |
| `MapWithFFTimeout` | ✅ | ✅ | FailFast + 超时 |
| `DefaultMap` | ❌ | ❌ | 使用默认 IO 并发度 |
| `DefaultMapWithFailFast` | ✅ | ❌ | 默认并发度 + FailFast |
| `DefaultMapWithTimeout` | ❌ | ✅ | 默认并发度 + 超时 |
| `DefaultMapWithFFTimeout` | ✅ | ✅ | 默认并发度 + FailFast + 超时 |
| `MapSerial` | ❌ | ❌ | 串行处理 |
| `MapSerialFailFast` | ✅ | ❌ | 串行 FailFast |

### 使用示例

```go
ctx := context.Background()

// 批量 API 调用
urls := []string{"url1", "url2", "url3"}
results := async.Map(ctx, urls, async.IO(), func(ctx context.Context, url string) (*http.Response, error) {
    req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
    return http.DefaultClient.Do(req)
})

// FailFast：首个失败就停止
results, err := async.MapWithFailFast(ctx, items, async.IO(), func(ctx context.Context, item string) (bool, error) {
    if !isValid(item) {
        return false, fmt.Errorf("无效输入: %s", item)
    }
    return true, nil
})
if err != nil {
    log.Printf("验证失败（首个错误）: %v", err)
}

// 带总超时：整体操作最多 10 秒
results := async.MapWithTimeout(ctx, items, async.IO(), 10*time.Second, fn)

// 串行处理：数据量小或需要严格顺序
results := async.MapSerial(ctx, items, fn)
```

---

## ForEach - 并发遍历

对切片的每个元素并发执行操作，只关心错误。适合**批量发送消息、写入数据库、清理资源**等场景。

### 基础用法

```go
nr, err := async.ForEach(ctx, users, async.IO(), func(ctx context.Context, user User) error {
    return sendNotification(ctx, user, msg)
})

// 查看统计
fmt.Printf("成功: %d, 失败: %d\n", nr.SuccessCount(), nr.FailCount())

if err != nil {
    log.Printf("第一个错误: %v", err)
}
```

### 变体一览

| 函数 | FailFast | 超时 | 说明 |
|------|----------|------|------|
| `ForEach` | ❌ | ❌ | 基础并发遍历 |
| `ForEachWithFailFast` | ✅ | ❌ | 首个失败立即停止 |
| `ForEachWithTimeout` | ❌ | ✅ | 指定总超时 |
| `ForEachWithFFTimeout` | ✅ | ✅ | FailFast + 超时 |
| `ForEachSerial` | ❌ | ❌ | 串行遍历 |
| `ForEachSerialFailFast` | ✅ | ❌ | 串行 FailFast |
| `DefaultForEach` | ❌ | ❌ | 默认 IO 并发度 |
| `DefaultForEachWithFailFast` | ✅ | ❌ | 默认并发度 + FailFast |
| `DefaultForEachWithTimeout` | ❌ | ✅ | 默认并发度 + 超时 |
| `DefaultForEachWithFFTimeout` | ✅ | ✅ | 默认并发度 + FailFast + 超时 |

### 使用示例

```go
// 并发发送推送通知
nr, err := async.ForEach(ctx, devices, async.IO(), func(ctx context.Context, device Device) error {
    return pushService.Send(ctx, device.Token, notification)
})
fmt.Printf("推送发送完毕: 成功 %d, 失败 %d\n", nr.SuccessCount(), nr.FailCount())

// 批量写入数据库
nr, err := async.ForEachWithFailFast(ctx, records, async.IO(), func(ctx context.Context, r Record) error {
    return db.Insert(ctx, r)
})
if err != nil {
    log.Printf("写入失败，后续记录已取消: %v", err)
}
```

---

## Reduce - 并发聚合

先并发 Map 再串行聚合，等价于 `Map + Reduce`。适合**统计汇总、求和、拼接**等场景。

### 基础用法

```go
// 并发计算所有数字的平方和
sum, err := async.Reduce(ctx, []int{1, 2, 3, 4, 5}, async.IO(),
    func(ctx context.Context, n int) (int, error) {
        return n * n, nil // Map 阶段：计算平方
    },
    0,                       // 初始值
    func(acc, val int) int { // Reduce 阶段：累加
        return acc + val
    },
)
fmt.Println(sum) // 55 = 1+4+9+16+25
```

**参数说明：**

| 参数 | 说明 |
|------|------|
| `ctx` | 上下文 |
| `items` | 输入切片 |
| `concurrency` | Map 阶段并发度 |
| `mapFn` | Map 阶段处理函数 |
| `initial` | 聚合初始值 |
| `reduceFn` | 聚合函数：`(累加器, 下一个值) -> 新累加器` |

### 变体一览

| 函数 | FailFast | 超时 |
|------|----------|------|
| `Reduce` | ❌ | ❌ |
| `ReduceWithFailFast` | ✅ | ❌ |
| `ReduceWithTimeout` | ❌ | ✅ |
| `ReduceWithFFTimeout` | ✅ | ✅ |
| `DefaultReduce` | ❌ | ❌ |
| `DefaultReduceWithFailFast` | ✅ | ❌ |
| `DefaultReduceWithTimeout` | ❌ | ✅ |
| `DefaultReduceWithFFTimeout` | ✅ | ✅ |

### 使用示例

```go
// 并发统计用户订单总额
total, err := async.Reduce(ctx, userIDs, async.IO(),
    func(ctx context.Context, uid int) (float64, error) {
        return db.GetOrderTotal(ctx, uid)
    },
    0.0,
    func(acc, val float64) float64 {
        return acc + val
    },
)

// URL 拼接
urls, err := async.Reduce(ctx, paths, async.IO(),
    func(ctx context.Context, p string) (string, error) {
        return baseURL + p, nil
    },
    "",
    func(acc, path string) string {
        if acc == "" {
            return path
        }
        return acc + "\n" + path
    },
)
```

---

## Chunk - 分块处理

将大切片按指定大小分割成多个批次，配合 `MapChunk` / `ForEachChunk` 实现批量处理。

### 基础分块

```go
items := make([]int, 1000)

// 按大小分块
chunks := async.Chunk(items, 100) // 10 个批次，每批 100 个
for _, chunk := range chunks {
    processBatch(ctx, chunk)
}

// 按数量均分
chunks := async.ChunkN(items, 5) // 均分为 5 个批次
```

### MapChunk（分块并发 Map）

处理函数接收整个 chunk，适合**数据库批量 INSERT** 等场景：

```go
results := async.MapChunk(ctx, records, 4, 100, func(ctx context.Context, batch []Record) (int64, error) {
    return db.BatchInsert(ctx, batch)
})
```

### MapChunked（分块元素 Map）

内部自动分块，处理函数仍然接收单个元素：

```go
results := async.MapChunked(ctx, items, async.IO(), 100, func(ctx context.Context, item Item) (Result, error) {
    return processItem(ctx, item)
})
```

### MapChunk / MapChunked 变体

两种函数都有完整的变体矩阵：

- `MapChunk` / `DefaultMapChunk`
- `MapChunkWithFailFast` / `DefaultMapChunkWithFailFast`
- `MapChunkWithTimeout` / `DefaultMapChunkWithTimeout`
- `MapChunkWithFFTimeout` / `DefaultMapChunkWithFFTimeout`
- `MapChunked` / ...（同上）

### ForEachChunk（分块并发遍历）

```go
// 每 50 个用户一批，批量发送推送
nr, err := async.ForEachChunk(ctx, users, async.IO(), 50, func(ctx context.Context, batch []User) error {
    return pushService.BatchSend(ctx, batch, notification)
})
```

> ForEachChunk / ForEachChunked 同样有完整的 FailFast / Timeout / Default 变体矩阵。

---

## Result 辅助函数

处理 `[]Result[T]` 切片时的便捷函数：

### 提取值

```go
results := async.Map(ctx, items, async.IO(), fn)

// 提取所有成功的值（忽略错误）
values := async.ResultValues(results) // []string

// 提取所有值（包括错误的零值），存到预分配的切片
dst := make([]string, len(results))
async.Flat(results) // []string

// 提取所有非 nil 错误
errs := async.ResultErrors(results)
if len(errs) > 0 {
    log.Printf("有 %d 个任务失败", len(errs))
}
```

### 条件判断

```go
if async.Every(results) {
    fmt.Println("全部成功")
}

if async.Some(results) {
    fmt.Println("至少有一个成功")
}

if async.AnyError(results) {
    fmt.Println("存在失败的任务")
}
```

### 分区

```go
// 分离成功值和错误
values, errors := async.Partition(results)
fmt.Printf("成功 %d 个, 失败 %d 个\n", len(values), len(errors))

for _, v := range values {
    fmt.Println("成功:", v)
}
for _, e := range errors {
    fmt.Println("失败:", e)
}
```

---

## 完整示例

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/chichengyu/async"
)

func main() {
    ctx := context.Background()
    ctx = async.EnsureTraceID(ctx)

    // 场景：批量处理用户数据
    users := []User{
        {ID: 1, Name: "Alice"},
        {ID: 2, Name: "Bob"},
        {ID: 3, Name: "Charlie"},
    }

    // 1. Map：并发查询用户详情
    results := async.Map(ctx, users, async.IO(), func(ctx context.Context, u User) (*UserDetail, error) {
        return fetchUserDetail(ctx, u.ID)
    })

    // 2. 检查是否全部成功
    if async.Every(results) {
        fmt.Println("全部查询成功")
    }

    // 3. 提取成功的值
    details := async.ResultValues(results)

    // 4. ForEach：并发发送通知
    nr, err := async.ForEach(ctx, details, async.IO(), func(ctx context.Context, d *UserDetail) error {
        return sendWelcomeEmail(ctx, d.Email)
    })

    fmt.Printf("通知发送: 成功 %d, 失败 %d\n", nr.SuccessCount(), nr.FailCount())

    // 5. Reduce：统计
    if async.Every(results) {
        total, _ := async.Reduce(ctx, details, async.IO(),
            func(ctx context.Context, d *UserDetail) (int, error) {
                return d.Score, nil
            },
            0,
            func(acc, val int) int { return acc + val },
        )
        fmt.Printf("总分: %d\n", total)
    }
}
```

---

## 方法速查表

### Map 系列

| 函数 | 说明 |
|------|------|
| `Map(ctx, items, c, fn)` | 并发映射 |
| `MapWithFailFast(ctx, items, c, fn)` | FailFast 映射 |
| `MapWithTimeout(ctx, items, c, d, fn)` | 带超时映射 |
| `MapWithFFTimeout(ctx, items, c, d, fn)` | FailFast + 超时映射 |
| `MapSerial(ctx, items, fn)` | 串行映射 |
| `MapSerialFailFast(ctx, items, fn)` | 串行 FailFast 映射 |

### ForEach 系列

| 函数 | 说明 |
|------|------|
| `ForEach(ctx, items, c, fn)` | 并发遍历 |
| `ForEachWithFailFast(ctx, items, c, fn)` | FailFast 遍历 |
| `ForEachWithTimeout(ctx, items, c, d, fn)` | 带超时遍历 |
| `ForEachWithFFTimeout(ctx, items, c, d, fn)` | FailFast + 超时遍历 |
| `ForEachSerial(ctx, items, fn)` | 串行遍历 |
| `ForEachSerialFailFast(ctx, items, fn)` | 串行 FailFast 遍历 |

### Reduce 系列

| 函数 | 说明 |
|------|------|
| `Reduce(ctx, items, c, mapFn, init, reduceFn)` | 并发聚合 |
| `ReduceWithFailFast(...)` | FailFast 聚合 |
| `ReduceWithTimeout(...)` | 带超时聚合 |
| `ReduceWithFFTimeout(...)` | FailFast + 超时聚合 |

### Chunk 系列

| 函数 | 说明 |
|------|------|
| `Chunk(items, batchSize)` | 按大小分块 |
| `ChunkN(items, n)` | 按数量均分 |

### MapChunk 系列（fn 接收 chunk）

| 函数 | 说明 |
|------|------|
| `MapChunk(ctx, items, c, batch, fn)` | 分块并发映射 |
| `MapChunkWithFailFast(...)` | FailFast 分块 |
| `MapChunkWithTimeout(...)` | 带超时分块 |
| `MapChunkWithFFTimeout(...)` | FailFast + 超时分块 |

### MapChunked 系列（fn 接收单元素）

| 函数 | 说明 |
|------|------|
| `MapChunked(ctx, items, c, batch, fn)` | 分块元素映射 |
| `MapChunkedWithFailFast(...)` | FailFast 分块元素 |
| `MapChunkedWithTimeout(...)` | 带超时分块元素 |
| `MapChunkedWithFFTimeout(...)` | FailFast + 超时分块元素 |

### ForEachChunk / ForEachChunked 系列

| 函数 | 说明 |
|------|------|
| `ForEachChunk(ctx, items, c, batch, fn)` | 分块遍历（fn 接收 chunk） |
| `ForEachChunked(ctx, items, c, batch, fn)` | 分块遍历（fn 接收单元素） |
| 及其 FailFast / Timeout / Default 变体 | |

### Result 辅助函数

| 函数 | 说明 |
|------|------|
| `ResultValues(results)` | 提取所有成功的值 |
| `ResultErrors(results)` | 提取所有非 nil 错误 |
| `Flat(results)` | 提取所有值（包括错误零值） |
| `Every(results)` | 是否全部成功 |
| `Some(results)` | 是否至少有一个成功 |
| `AnyError(results)` | 是否存在任何错误 |
| `Partition(results)` | 分离值和错误 |
| `OnlyErrors(results)` | 提取错误（跳过 nil） |