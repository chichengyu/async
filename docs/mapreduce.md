# 数据并行 (Map / ForEach / Reduce / Chunk)

数据并行模块提供对切片元素的并发处理操作，包括 `Map`、`ForEach`、`Reduce`、`Chunk` 等，是 async 库最常用的功能之一。

## 目录

- [Map - 并发映射](#map---并发映射)
- [ForEach - 并发遍历](#foreach---并发遍历)
- [Reduce - 并发聚合](#reduce---并发聚合)
- [Chunk - 分块处理](#chunk---分块处理)
- [MapPool / ForEachPool - 池化处理](#mappool--foreachpool---池化处理)
- [Result 辅助函数](#result-辅助函数)
- [Must - err!=nil 则 panic](#must---errnil-则-panic)
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
| `concurrency` | 并发度（建议用 `async.IO()` 或 `async.CPU()`；**≤0 时默认 IO()**） |
| `fn` | 处理函数：接收 ctx 和元素，返回处理结果 |

> **安全性**：并发的 Map 阶段内置 panic recovery，单个元素的 panic 不会导致整个操作崩溃，会包装为 `PanicError` 返回。

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

### Default 变体 — 省略并发度参数

所有 `Default*` 变体省略了 `concurrency` 参数，自动使用 `IO()` 并发度：

```go
// 带并发度的完整版
results := async.Map(ctx, items, async.IO(), fn)

// Default 快捷版（等价）
results := async.DefaultMap(ctx, items, fn)

// 其他 Default 变体：
results, err := async.DefaultMapWithFailFast(ctx, items, fn)
results := async.DefaultMapWithTimeout(ctx, items, 10*time.Second, fn)
results, err := async.DefaultMapWithFFTimeout(ctx, items, 10*time.Second, fn)
```

| 完整版 | Default 快捷版 | 说明 |
|--------|---------------|------|
| `Map(ctx, items, c, fn)` | `DefaultMap(ctx, items, fn)` | 基础映射 |
| `MapWithFailFast(ctx, items, c, fn)` | `DefaultMapWithFailFast(ctx, items, fn)` | FailFast |
| `MapWithTimeout(ctx, items, c, d, fn)` | `DefaultMapWithTimeout(ctx, items, d, fn)` | 超时 |
| `MapWithFFTimeout(ctx, items, c, d, fn)` | `DefaultMapWithFFTimeout(ctx, items, d, fn)` | FF+超时 |

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

### Default 变体

```go
// 带并发度
nr, err := async.ForEach(ctx, items, async.IO(), fn)

// Default 快捷版
nr, err := async.DefaultForEach(ctx, items, fn)
nr, err := async.DefaultForEachWithFailFast(ctx, items, fn)
nr, err := async.DefaultForEachWithTimeout(ctx, items, 10*time.Second, fn)
nr, err := async.DefaultForEachWithFFTimeout(ctx, items, 10*time.Second, fn)
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

### Default 变体

```go
// 带并发度
total, err := async.Reduce(ctx, items, async.IO(), mapFn, 0, reduceFn)

// Default 快捷版
total, err := async.DefaultReduce(ctx, items, mapFn, 0, reduceFn)
total, err := async.DefaultReduceWithFailFast(ctx, items, mapFn, 0, reduceFn)
total, err := async.DefaultReduceWithTimeout(ctx, items, 10*time.Second, mapFn, 0, reduceFn)
total, err := async.DefaultReduceWithFFTimeout(ctx, items, 10*time.Second, mapFn, 0, reduceFn)
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

**参数：**

| 参数 | 类型 | 默认行为 | 说明 |
|------|------|----------|------|
| `items` | `[]T` | — | 输入切片 |
| `batchSize`（Chunk） | `int` | **≤0 返回 nil** | 每批最大元素数 |
| `n`（ChunkN） | `int` | **≤0 返回 nil** | 批次数（均分，最后一组可能略小） |

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

```go
// MapChunk（fn 接收整个 chunk []T）
results := async.MapChunk(ctx, items, 4, 100, fn)
results, err := async.MapChunkWithFailFast(ctx, items, 4, 100, fn)
results := async.MapChunkWithTimeout(ctx, items, 4, 100, 10*time.Second, fn)
results, err := async.MapChunkWithFFTimeout(ctx, items, 4, 100, 10*time.Second, fn)

// MapChunked（内部自动分块，fn 接收单个元素 T）
results := async.MapChunked(ctx, items, 4, 100, fn)
results, err := async.MapChunkedWithFailFast(ctx, items, 4, 100, fn)
results := async.MapChunkedWithTimeout(ctx, items, 4, 100, 10*time.Second, fn)
results, err := async.MapChunkedWithFFTimeout(ctx, items, 4, 100, 10*time.Second, fn)

// Default* 快捷版（省略 concurrency 参数）
results := async.DefaultMapChunk(ctx, items, 100, fn)
results, err := async.DefaultMapChunkWithFailFast(ctx, items, 100, fn)
results := async.DefaultMapChunkWithTimeout(ctx, items, 100, 10*time.Second, fn)
results, err := async.DefaultMapChunkWithFFTimeout(ctx, items, 100, 10*time.Second, fn)

// MapChunked Default* 快捷版（内部自动分块，省略 concurrency 参数）
results := async.DefaultMapChunked(ctx, items, 100, fn)
results, err := async.DefaultMapChunkedWithFailFast(ctx, items, 100, fn)
results := async.DefaultMapChunkedWithTimeout(ctx, items, 100, 10*time.Second, fn)
results, err := async.DefaultMapChunkedWithFFTimeout(ctx, items, 100, 10*time.Second, fn)
```

### ForEachChunk（分块并发遍历）

```go
// 每 50 个用户一批，批量发送推送
nr, err := async.ForEachChunk(ctx, users, async.IO(), 50, func(ctx context.Context, batch []User) error {
    return pushService.BatchSend(ctx, batch, notification)
})

// ForEachChunked（fn 接收单元素，内部自动分块）
nr, err := async.ForEachChunked(ctx, users, async.IO(), 50, func(ctx context.Context, user User) error {
    return sendOne(ctx, user)
})

// ── ForEachChunk 完整变体矩阵 ──

// FailFast：首个失败立即停止
nr, err := async.ForEachChunkWithFailFast(ctx, items, c, 100, fn)
nr, err := async.ForEachChunkedWithFailFast(ctx, items, c, 100, fn)

// Timeout：指定总超时
nr := async.ForEachChunkWithTimeout(ctx, items, c, 100, 30*time.Second, fn)
nr := async.ForEachChunkedWithTimeout(ctx, items, c, 100, 30*time.Second, fn)

// FailFast + Timeout
nr, err := async.ForEachChunkWithFFTimeout(ctx, items, c, 100, 30*time.Second, fn)
nr, err := async.ForEachChunkedWithFFTimeout(ctx, items, c, 100, 30*time.Second, fn)

// ── Default* 快捷版（省略 concurrency 参数）──

// ForEachChunk Default
nr, err := async.DefaultForEachChunk(ctx, items, 100, fn)
nr, err := async.DefaultForEachChunkWithFailFast(ctx, items, 100, fn)
nr := async.DefaultForEachChunkWithTimeout(ctx, items, 100, 30*time.Second, fn)
nr, err := async.DefaultForEachChunkWithFFTimeout(ctx, items, 100, 30*time.Second, fn)

// ForEachChunked Default
nr, err := async.DefaultForEachChunked(ctx, items, 100, fn)
nr, err := async.DefaultForEachChunkedWithFailFast(ctx, items, 100, fn)
nr := async.DefaultForEachChunkedWithTimeout(ctx, items, 100, 30*time.Second, fn)
nr, err := async.DefaultForEachChunkedWithFFTimeout(ctx, items, 100, 30*time.Second, fn)
```

---

## MapPool / ForEachPool - 池化处理

`MapPool` 返回一个 `Pool` 用于后续复用，`ForEachPool` 返回一个 `NoResultPool`。与普通 Map/ForEach 不同的是，**返回值包含池对象**，适合需要多次提交额外任务的场景。

### MapPool — 返回 Pool[R]

```go
records := []string{"a", "b", "c"}

// 提交切片元素，返回 Pool 和首批结果
p, results, err := async.MapPool(ctx, records, func(ctx context.Context, s string) (Processed, error) {
    return process(ctx, s)
}, async.IO())

// Pool 仍然可用，可以继续提交新任务
if err == nil {
    go func() {
        for _, r := range results {
            if r.Ok() {
                fmt.Println(r.Value)
            }
        }
    }()
    _ = p.Submit(ctx, func(ctx context.Context) (Processed, error) {
        return process(ctx, "extra task")
    })
}

// 用完记得关闭
defer p.Close()
```

**参数：**
- `ctx` — 上下文
- `items` — 输入切片 `[]T`
- `fn` — 处理函数 `func(context.Context, T) (R, error)`
- `concurrency` — 并发度

**返回：**
- `*Pool[R]` — 仍可复用的池（需手动 `Close()`）
- `[]core.Result[R]` — items 对应的结果
- `error` — 整体提交错误

### ForEachPool — 返回 NoResultPool

```go
files := []string{"f1.txt", "f2.txt", "f3.txt"}

p, err := async.ForEachPool(ctx, files, func(ctx context.Context, path string) error {
    return os.Remove(path)
}, async.IO())

if err != nil {
    log.Printf("删除失败: %v", err)
}

// Pool 仍可用，可以继续提交
_ = p.SubmitAction(ctx, func(ctx context.Context) error {
    return os.Remove("extra.txt")
})
defer p.Close()
```

**参数：**
- `ctx` — 上下文
- `items` — 输入切片 `[]T`
- `fn` — 处理函数 `func(context.Context, T) error`
- `concurrency` — 并发度

**返回：**
- `*NoResultPool` — 仍可复用的池（需手动 `Close()`）
- `error` — 整体提交错误

### 与 Map/ForEach 的区别

| 特性 | Map/ForEach | MapPool/ForEachPool |
|------|-------------|---------------------|
| 返回 pool 对象 | ❌ | ✅（可复用） |
| 适合批量一次性 | ✅ | ❌ |
| 适合动态追加任务 | ❌ | ✅ |
| 需手动 Close | ❌ | ✅ |
| Wait 自动触发 | ✅ | ❌ |

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

// OnlyErrors: 与 ResultErrors 等价，更简洁的命名
errs := async.OnlyErrors(results)

// 同时获取值和错误（两个切片按结果对应）
values, errs := async.Partition(results)
```

**辅助函数参数：**
| 函数 | 签名 | 返回格式 |
|------|------|----------|
| `ResultValues(results)` | `[]Result[T] → []T` | 成功值（跳过失败项） |
| `ResultErrors(results)` | `[]Result[T] → []error` | 非 nil 错误（跳过 nil） |
| `OnlyErrors(results)` | `[]Result[T] → []error` | 同上，更简洁命名 |
| `Flat(results)` | `[]Result[T] → []T` | 全部值（失败项为零值） |
| `Partition(results)` | `[]Result[T] → ([]T, []error)` | 同时分离值和错误 |
| `Every(results)` | `[]Result[T] → bool` | 是否全部成功 |
| `Some(results)` | `[]Result[T] → bool` | 是否至少一个成功 |
| `AnyError(results)` | `[]Result[T] → bool` | 是否存在失败任务 |
| `Must(results)` | `[]Result[T] → T` | 取出值，err != nil 时 panic |

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

### Must - err!=nil 则 panic

适用于初始化或测试代码中直接提取值，避免重复的 `if err != nil`：

```go
// 初始化阶段
config := async.Must(loadConfig("config.json"))

// 测试代码
val := async.Must(async.Map(ctx, items, async.IO(), fn)).Values()
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

| 函数 | 完整签名 |
|------|---------|
| `Map[T, R]` | `func Map[T any, R any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) (R, error)) []Result[R]` |
| `MapWithFailFast[T, R]` | `func MapWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |
| `MapWithTimeout[T, R]` | `func MapWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R]` |
| `MapWithFFTimeout[T, R]` | `func MapWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |
| `MapSerial[T, R]` | `func MapSerial[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) []Result[R]` |
| `MapSerialFailFast[T, R]` | `func MapSerialFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |
| `DefaultMap[T, R]` | `func DefaultMap[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) []Result[R]` |
| `DefaultMapWithFailFast[T, R]` | `func DefaultMapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |
| `DefaultMapWithTimeout[T, R]` | `func DefaultMapWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R]` |
| `DefaultMapWithFFTimeout[T, R]` | `func DefaultMapWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |

### ForEach 系列

| 函数 | 完整签名 |
|------|---------|
| `ForEach[T]` | `func ForEach[T any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) error) (*NoResult, error)` |
| `ForEachWithFailFast[T]` | `func ForEachWithFailFast[T any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) error) (*NoResult, error)` |
| `ForEachWithTimeout[T]` | `func ForEachWithTimeout[T any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error)` |
| `ForEachWithFFTimeout[T]` | `func ForEachWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error)` |
| `ForEachSerial[T]` | `func ForEachSerial[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error)` |
| `ForEachSerialFailFast[T]` | `func ForEachSerialFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error)` |
| `DefaultForEach[T]` | `func DefaultForEach[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error)` |
| `DefaultForEachWithFailFast[T]` | `func DefaultForEachWithFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error)` |
| `DefaultForEachWithTimeout[T]` | `func DefaultForEachWithTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error)` |
| `DefaultForEachWithFFTimeout[T]` | `func DefaultForEachWithFFTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error)` |

### Reduce 系列

| 函数 | 完整签名 |
|------|---------|
| `Reduce[T, R]` | `func Reduce[T any, R any](ctx context.Context, items []T, concurrency int, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error)` |
| `ReduceWithFailFast[T, R]` | `func ReduceWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error)` |
| `ReduceWithTimeout[T, R]` | `func ReduceWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error)` |
| `ReduceWithFFTimeout[T, R]` | `func ReduceWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error)` |
| `DefaultReduce[T, R]` | `func DefaultReduce[T any, R any](ctx context.Context, items []T, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error)` |
| `DefaultReduceWithFailFast[T, R]` | `func DefaultReduceWithFailFast[T any, R any](ctx context.Context, items []T, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error)` |
| `DefaultReduceWithTimeout[T, R]` | `func DefaultReduceWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error)` |
| `DefaultReduceWithFFTimeout[T, R]` | `func DefaultReduceWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error)` |

### Chunk 分块工具

| 函数 | 完整签名 |
|------|---------|
| `Chunk[T]` | `func Chunk[T any](items []T, batchSize int) [][]T` |
| `ChunkN[T]` | `func ChunkN[T any](items []T, n int) [][]T` |

### MapChunk 系列（fn 接收整个 chunk `[]T`）

| 函数 | 完整签名 |
|------|---------|
| `MapChunk[T, R]` | `func MapChunk[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) []Result[R]` |
| `MapChunkWithFailFast[T, R]` | `func MapChunkWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) ([]Result[R], error)` |
| `MapChunkWithTimeout[T, R]` | `func MapChunkWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []Result[R]` |
| `MapChunkWithFFTimeout[T, R]` | `func MapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]Result[R], error)` |
| `DefaultMapChunk[T, R]` | `func DefaultMapChunk[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) (R, error)) []Result[R]` |
| `DefaultMapChunkWithFailFast[T, R]` | `func DefaultMapChunkWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) (R, error)) ([]Result[R], error)` |
| `DefaultMapChunkWithTimeout[T, R]` | `func DefaultMapChunkWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []Result[R]` |
| `DefaultMapChunkWithFFTimeout[T, R]` | `func DefaultMapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]Result[R], error)` |

### MapChunked 系列（fn 接收单元素 `T`，内部自动分块）

| 函数 | 完整签名 |
|------|---------|
| `MapChunked[T, R]` | `func MapChunked[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) []Result[R]` |
| `MapChunkedWithFailFast[T, R]` | `func MapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |
| `MapChunkedWithTimeout[T, R]` | `func MapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R]` |
| `MapChunkedWithFFTimeout[T, R]` | `func MapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |
| `DefaultMapChunked[T, R]` | `func DefaultMapChunked[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) (R, error)) []Result[R]` |
| `DefaultMapChunkedWithFailFast[T, R]` | `func DefaultMapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |
| `DefaultMapChunkedWithTimeout[T, R]` | `func DefaultMapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R]` |
| `DefaultMapChunkedWithFFTimeout[T, R]` | `func DefaultMapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error)` |

### ForEachChunk 系列（fn 接收整个 chunk `[]T`）

| 函数 | 完整签名 |
|------|---------|
| `ForEachChunk[T]` | `func ForEachChunk[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) error) (*NoResult, error)` |
| `ForEachChunkWithFailFast[T]` | `func ForEachChunkWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) error) (*NoResult, error)` |
| `ForEachChunkWithTimeout[T]` | `func ForEachChunkWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error)` |
| `ForEachChunkWithFFTimeout[T]` | `func ForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error)` |
| `DefaultForEachChunk[T]` | `func DefaultForEachChunk[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) error) (*NoResult, error)` |
| `DefaultForEachChunkWithFailFast[T]` | `func DefaultForEachChunkWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) error) (*NoResult, error)` |
| `DefaultForEachChunkWithTimeout[T]` | `func DefaultForEachChunkWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error)` |
| `DefaultForEachChunkWithFFTimeout[T]` | `func DefaultForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error)` |

### ForEachChunked 系列（fn 接收单元素 `T`，内部自动分块）

| 函数 | 完整签名 |
|------|---------|
| `ForEachChunked[T]` | `func ForEachChunked[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) error) (*NoResult, error)` |
| `ForEachChunkedWithFailFast[T]` | `func ForEachChunkedWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) error) (*NoResult, error)` |
| `ForEachChunkedWithTimeout[T]` | `func ForEachChunkedWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error)` |
| `ForEachChunkedWithFFTimeout[T]` | `func ForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error)` |
| `DefaultForEachChunked[T]` | `func DefaultForEachChunked[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) error) (*NoResult, error)` |
| `DefaultForEachChunkedWithFailFast[T]` | `func DefaultForEachChunkedWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) error) (*NoResult, error)` |
| `DefaultForEachChunkedWithTimeout[T]` | `func DefaultForEachChunkedWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error)` |
| `DefaultForEachChunkedWithFFTimeout[T]` | `func DefaultForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error)` |

### MapPool / ForEachPool 系列

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `MapPool[T, R]` | `func MapPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) (*Pool[R], []core.Result[R], error)` | 返回可复用的 `*Pool[R]` 和结果 |
| `ForEachPool[T]` | `func ForEachPool[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (*NoResultPool, error)` | 返回可复用的 `*NoResultPool` |

### Result 辅助函数

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `ResultValues[T]` | `func ResultValues[T any](results []Result[T]) []T` | 提取所有成功的值 |
| `ResultErrors[T]` | `func ResultErrors[T any](results []Result[T]) []error` | 提取所有非 nil 错误 |
| `Flat[T]` | `func Flat[T any](results []Result[T]) []T` | 提取所有值（包括错误前的零值） |
| `OnlyErrors[T]` | `func OnlyErrors[T any](results []Result[T]) []error` | 提取错误（跳过 nil） |
| `Every[T]` | `func Every[T any](results []Result[T]) bool` | 是否全部成功 |
| `Some[T]` | `func Some[T any](results []Result[T]) bool` | 是否至少有一个成功 |
| `AnyError[T]` | `func AnyError[T any](results []Result[T]) bool` | 是否存在任何错误 |
| `Partition[T]` | `func Partition[T any](results []Result[T]) (values []T, errors []error)` | 分离值和错误 |

### 通用工具

| 函数 | 完整签名 | 说明 |
|------|---------|------|
| `Must[T]` | `func Must[T any](val T, err error) T` | 提取值，err != nil 时 panic |
| `SafeCall[T, R]` | `func SafeCall[T any, R any](ctx context.Context, item T, fn func(ctx context.Context, item T) (R, error)) (R, error)` | 安全调用元素级函数，捕获 panic |
| `SafeCallVoid[T]` | `func SafeCallVoid[T any](ctx context.Context, item T, fn func(ctx context.Context, item T) error) error` | 无返回值安全调用，捕获 panic |

### 类型定义

| 类型 | 定义 |
|------|------|
| `Result[T]` | `type Result[T any] = core.Result[T]`（含 `Value T`、`Err error`、`Ok()`、`IsPanic()`） |
| `NoResult` | `type NoResult = group.NoResult` |
| `PanicError` | `type PanicError = core.PanicError` |