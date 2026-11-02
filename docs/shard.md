# Shard（分片）文档

## 概述

Shard 提供水平分片能力，将任务分发到 N 个独立的 Pool 或 Group 实例，实现水平扩展。每个分片独立运行，互不影响。

**核心组件**：

| 组件 | 入口 | 说明 |
|------|------|------|
| `ShardPoolBuilder` | `async.PoolSharded[T]()` | 分片协程池构建器 |
| `ShardedPool` | `ShardPoolBuilder.Run(fn)` | 分片协程池实例 |
| `ShardedGroupBuilder` | `async.GroupSharded[T]()` | 分片 Group 构建器 |
| `ShardedGroup` | `ShardedGroupBuilder.Run(fn)` | 分片 Group 实例 |

**分发策略**：

| 策略 | 常量 | 说明 |
|------|------|------|
| `RoundRobin` | `async.RoundRobin` | 轮询分发（默认） |
| `Hash` | `async.Hash` | 按 Key 哈希分发到固定分片 |

> **⚠️ 分发策略选择**
>
> - `RoundRobin`：任务均匀分布于各分片，适合无亲和性要求的场景
> - `Hash`：同一 Key 始终落在同一分片，适合需要亲和性的场景（如用户 ID 分片）
>
> 分片池的 `KeyFn` 用于 `SubmitBatch` / `TrySubmitBatch` 的元素路由；单个 `Submit` 没有元素参数，Hash 模式下使用第 0 片。单个任务可通过 `SubmitKeyed` 指定 key。分片 Group 使用 `GoKeyed` 指定 key。

---

## 目录

- [ShardPool 链式 API 速查表](#shardpool-链式-api-速查表)
- [ShardedGroup 链式 API 速查表](#shardedgroup-链式-api-速查表)
- [ShardPool 构建器入口](#shardpool-构建器入口)
- [ShardPool 基本配置](#shardpool-基本配置)
- [ShardPool 外部池配置](#shardpool-外部池配置)
- [ShardPool 实例方法](#shardpool-实例方法)
- [ShardedGroup 构建器入口](#shardedgroup-构建器入口)
- [ShardedGroup 详细配置](#shardedgroup-详细配置)
- [ShardedGroup 实例方法](#shardedgroup-实例方法)
- [完整示例](#完整示例)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)

---

## ShardPool 链式 API 速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.PoolSharded[T]()` | 创建分片池构建器 |

### 构建器方法（ShardPoolBuilder）

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Shards(n)` | 分片数（默认 4） |
| `.DefaultShards()` | 恢复默认分片数（4） |
| `.Worker(n)` | 每分片 worker 数（默认 `IO()`） |
| `.DefaultWorker()` | 恢复默认每分片 worker 数 |
| `.Distribution(d)` | 分发策略（RoundRobin / Hash） |
| `.DefaultDistribution()` | 恢复默认分发策略（RoundRobin） |
| `.KeyFn(fn)` | 分片 Key 函数（Hash 模式下使用） |
| `.Timeout(d)` | 任务超时 |
| `.DefaultTimeout()` | 清除超时（不限时） |
| `.FailFast()` | 启用快速失败；Run 回调收到可取消的上下文，应将其传给 Submit |
| `.DefaultFailFast()` | 关闭快速失败 |
| `.MaxPending(n)` | 最大待处理任务数 |
| `.DefaultMaxPending()` | 恢复默认最大待处理数（无限制） |
| `.MaxResults(n)` | 结果存储上限（0=无限） |
| `.DefaultMaxResults()` | 恢复默认最大结果数（无限） |
| `.DefaultShardPoolConfig()` | 重置所有配置为默认值 |
| `.Config(func)` | 函数式配置，通过闭包修改完整配置 |
| `.Logger(l)` | 注入自定义日志（全局生效） |
| `.DefaultLogger()` | 恢复默认日志 |

### 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Run(fn)` | `error` | 创建分片池 → 执行 fn → 自动 Close 所有分片 |

> **⚠️ Run 回调参数类型**
>
> `fn func(ctx context.Context, sp *async.ShardedPool[T]) error`
>
> 第二个参数 `sp` 是分片协程池实例，可用其 `Submit`/`SubmitKeyed`/`Wait` 等方法完成任务调度。
> Run 返回时自动 `Close` 所有分片。

---

## ShardedGroup 链式 API 速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.GroupSharded[T]()` | 创建分片 Group 构建器 |

### 构建器方法（ShardedGroupBuilder）

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Shards(n)` | 分片数（默认 4） |
| `.DefaultShards()` | 恢复默认分片数（4） |
| `.Worker(n)` | 每分片并发度（默认 `IO()`） |
| `.DefaultWorker()` | 恢复默认每分片并发度 |
| `.Distribution(d)` | 分发策略（RoundRobin / Hash） |
| `.DefaultDistribution()` | 恢复默认分发策略（RoundRobin） |
| `.DefaultShardedGroupConfig()` | 重置所有配置为默认值 |
| `.Config(func)` | 函数式配置，通过闭包修改完整配置 |
| `.Logger(l)` | 注入自定义日志（全局生效） |
| `.DefaultLogger()` | 恢复默认日志 |

### 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Run(fn)` | `error` | 创建 ShardedGroup → 执行 fn → 自动 Close 所有分片 |

> **⚠️ Run 回调参数类型**
>
> `fn func(ctx context.Context, sg *async.ShardedGroup[T]) error`
>
> 第二个参数 `sg` 是分片 Group 实例，可用其 `Go`/`GoKeyed`/`Wait` 等方法完成任务调度。
> Run 返回时自动 `Close` 所有分片 Group。

---

## ShardPool 构建器入口

```go
// 创建分片池链式构建器
b := async.PoolSharded[T]()
```

---

## ShardPool 基本配置

### 分片数 + 每片 Worker

```go
err := async.PoolSharded[int]().
    Shards(8).        // 8 个分片
    Worker(4).        // 每分片 4 个 worker
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        for i := 0; i < 100; i++ {
            sp.Submit(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, i)
            })
        }
        results := sp.Wait()
        for _, r := range results {
            if !r.Ok() {
                log.Printf("失败: %v", r.Err)
            }
        }
        return nil
    })
```

### RoundRobin 分发（默认）

```go
err := async.PoolSharded[string]().
    Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[string]) error {
        // 任务轮询分发到 8 个分片
        for _, item := range items {
            sp.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            })
        }
        results := sp.Wait()
        handleResults(results)
        return nil
    })
```

### Hash 分发（Key 亲和）

```go
err := async.PoolSharded[int]().
    Shards(8).Worker(4).
    Distribution(async.Hash).    // Hash 分发
    KeyFn(func(item int) uint64 {
        return uint64(item % 100) // 按 item mod 100 分到固定分片
    }).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        _, err := sp.SubmitBatch(ctx, userIDs, func(ctx context.Context, userID int) (int, error) {
            return processUser(ctx, userID)
        })
        if err != nil {
            return err
        }
        results := sp.Wait()
        handleResults(results)
        return nil
    })
```

### 带 Key 的运行时提交

```go
// SubmitKeyed：按 key 哈希分发，无需在构建器设置 KeyFn
err := async.PoolSharded[int]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        for _, item := range items {
            sp.SubmitKeyed(item.UserKey, ctx, func(ctx context.Context) (int, error) {
                return process(item)
            })
        }
        results := sp.Wait()
        handleResults(results)
        return nil
    })
```

### 非阻塞提交 TrySubmit

```go
err := async.PoolSharded[int]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        for _, item := range items {
            err := sp.TrySubmit(ctx, func(ctx context.Context) (int, error) {
                return process(item)
            })
            if err != nil {
                log.Printf("提交失败: %v", err)
            }
        }
        results := sp.Wait()
        handleResults(results)
        return nil
    })
```

### 定时提交 + Key 亲和

```go
// TrySubmitKeyed：按 key 哈希非阻塞分发
err := async.PoolSharded[int]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        for _, item := range items {
            err := sp.TrySubmitKeyed(item.UserKey, ctx, func(ctx context.Context) (int, error) {
                return process(item)
            })
            if err != nil {
                log.Printf("提交失败: %v", err)
            }
        }
        results := sp.Wait()
        handleResults(results)
        return nil
    })
```

### 批量提交

```go
err := async.PoolSharded[int]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        results, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
            return process(item)
        })
        for _, r := range results {
            if r.Err != nil {
                log.Printf("分片 %d 索引 %d 提交失败", r.ShardIdx, r.Index)
            }
        }
        _ = err
        // 等待完成
        allResults := sp.Wait()
        handleResults(allResults)
        return nil
    })
```

### 批量非阻塞提交

```go
err := async.PoolSharded[int]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        results, err := sp.TrySubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
            return process(item)
        })
        for _, r := range results {
            if r.Err != nil {
                log.Printf("分片 %d 索引 %d 提交失败", r.ShardIdx, r.Index)
            }
        }
        _ = err
        allResults := sp.Wait()
        handleResults(allResults)
        return nil
    })
```

### 指定分片索引提交

```go
err := async.PoolSharded[int]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        // 提交到分片 0 的索引位置 0
        sp.SubmitAt(0, 0, ctx, func(ctx context.Context) (int, error) {
            return process(item)
        })
        results := sp.Wait()
        handleResults(results)
        return nil
    })
```

---

## ShardPool 外部池配置

### Timeout + FailFast + MaxPending

```go
err := async.PoolSharded[int]().Context(ctx).
    Shards(8).Worker(4).
    Timeout(5 * time.Second).   // 每任务超时 5s
    FailFast().                  // 任一失败立即取消全体
    MaxPending(10000).           // 每个分片最大等待任务数
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        for _, item := range items {
            sp.Submit(ctx, func(ctx context.Context) (int, error) {
                return process(item)
            })
        }
        results := sp.Wait()
        handleResults(results)
        return nil
    })
```

### MaxResults + DefaultMaxResults

```go
// 设置结果上限
err := async.PoolSharded[int]().
    Shards(8).Worker(4).
    MaxResults(100000).
    Run(fn)

// 恢复默认无限
err := async.PoolSharded[int]().
    Shards(8).Worker(4).
    MaxResults(50000).
    DefaultMaxResults().
    Run(fn)
```

### Config 函数式配置

`async.ShardPoolConfig[T]` 的方法返回配置副本，可在 `Config` 回调中链式使用：

| 方法 | 对每个分片的作用 |
|------|------------------|
| `WithPoolCfg(pc)` | 整体替换底层 `async.PoolConfig`；后续链式设置再覆盖对应字段。`pc.Size` 不控制分片 worker 数，应使用 `SizePerShard` 或构建器的 `Worker`。 |
| `WithTimeout(d)` | 正数覆盖每片任务超时；非正值保留底层池的全局默认超时。 |
| `WithFailFast()` | 启用跨分片快速失败；提交时应使用 `Run` 回调传入的 context。 |
| `WithShardSubmitTimeout(d)` | 设置提交超时；非正值不启用。 |
| `WithShardMaxPending(n)` | 设置每片待处理任务上限；非正值不设置上限。 |
| `WithShardOverflow(strategy)` | 设置各片队列和环形缓冲的溢出策略。 |
| `WithShardRingBuf(capacity)` | 正数配置每片环形缓冲；非正值保留底层池创建时的全局默认环形缓冲配置。 |
| `WithShardMaxResults(n)` | 正数限制每片主结果存储；非正值表示不限制。 |
| `WithShardStreaming(bufSize)` | 正数启用每片流式结果通道；非正值不启用。 |

例如，可在现有配置上指定每片限制：

```go
builder := async.PoolSharded[int]().Config(func(cfg async.ShardPoolConfig[int]) async.ShardPoolConfig[int] {
    return cfg.WithShardMaxPending(1000).
        WithShardOverflow(async.OverflowError).
        WithShardMaxResults(10000).
        WithShardStreaming(256)
})
_ = builder
```

各分片的流式通道写入不阻塞；消费者跟不上时结果可能被丢弃。通过 `StreamResults()` 获取合并流后，应持续读取直到通道关闭；若提前停止消费且合并缓冲填满，分片转发协程会阻塞。需要完整结果时应结合结果存储上限、环形缓冲及任务量进行容量规划。

```go
err := async.PoolSharded[int]().
    Config(func(cfg async.ShardPoolConfig[int]) async.ShardPoolConfig[int] {
        cfg.Shards = 12
        cfg.SizePerShard = 6
        cfg.Distribution = async.Hash
        cfg.KeyFn = func(i int) uint64 { return uint64(i) }
        return cfg
    }).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        _, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
            return process(item)
        })
        if err != nil {
            return err
        }
        results := sp.Wait()
        handleResults(results)
        return nil
    })
```

---

## ShardPool 实例方法

在 `Run` 回调中，`sp *async.ShardedPool[T]` 提供以下方法：

### 提交流

| 方法 | 说明 |
|------|------|
| `sp.Submit(ctx, fn)` | 阻塞提交到某分片 |
| `sp.SubmitAt(shardIdx, index, ctx, fn)` | 提交到指定分片指定索引 |
| `sp.TrySubmit(ctx, fn)` | 非阻塞提交 |
| `sp.SubmitKeyed(key, ctx, fn)` | 按 key 哈希分发（阻塞） |
| `sp.TrySubmitKeyed(key, ctx, fn)` | 按 key 哈希分发（非阻塞） |
| `sp.SubmitBatch(ctx, items, fn)` | 批量提交，返回每项的分片信息 |
| `sp.TrySubmitBatch(ctx, items, fn)` | 批量非阻塞提交 |

### 等待与关闭

| 方法 | 说明 |
|------|------|
| `sp.Wait()` | 等待所有分片完成，合并结果切片 |
| `sp.WaitAndClose()` | 先调用 `Wait()` 收集结果，再关闭所有分片；与 `Wait()` 共用单次结果获取 |
| `sp.Close()` | 关闭所有分片 |
| `sp.Reset()` | 重置所有分片（等待后无活跃任务时可用）；保留 FailFast 并重建跨分片取消链，原上下文不可复用，重新通过 `sp.GetShard(i).Ctx()` 获取各分片上下文 |

若 `Wait()` 已在其他 goroutine 中执行，`WaitAndClose()` 不会取得那次等待的结果，但仍会开始关闭并等待分片关闭；调用方应等待首次 `Wait()` 返回以获取结果。

### 查询

| 方法 | 说明 |
|------|------|
| `sp.ShardCount()` | 返回分片数 |
| `sp.GetShard(idx)` | 获取指定分片的 `*async.PoolInstance[T]` 实例 |

### 配置代理

| 方法 | 说明 |
|------|------|
| `sp.ResizePerShard(n)` | 调整每分片 worker 数 |
| `sp.WithTimeout(d)` | 为所有分片设置任务超时 |
| `sp.WithSubmitTimeout(d)` | 为所有分片设置提交超时 |
| `sp.WithFailFast(ctx)` | 为所有分片启用 FailFast |
| `sp.WithStreaming(bufSize)` | 为所有分片启用流式结果消费 |
| `sp.WithResultCallback(fn)` | 为所有分片设置结果回调 |
| `sp.StreamResults()` | Fan-in 所有分片的流式结果 channel |

---

## ShardedGroup 构建器入口

```go
// 创建分片 Group 链式构建器
b := async.GroupSharded[T]()
```

---

## ShardedGroup 详细配置

### 基本分片 + 每片并发

```go
err := async.GroupSharded[string]().
    Shards(8).        // 8 个分片
    Worker(4).        // 每分片 4 并发
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        for _, item := range items {
            sg.Go(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            })
        }
        results := sg.Wait()
        for _, r := range results {
            if !r.Ok() {
                log.Printf("失败: %v", r.Err)
            }
        }
        return nil
    })
```

### RoundRobin 分发

```go
err := async.GroupSharded[string]().Shards(8).Worker(4).
    // Distribution 默认是 RoundRobin，可省略
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        for _, item := range items {
            sg.Go(ctx, func(ctx context.Context) (string, error) {
                return process(item)
            })
        }
        results := sg.Wait()
        handleResults(results)
        return nil
    })
```

### Hash 亲和分发 + GoKeyed

```go
err := async.GroupSharded[string]().Shards(8).Worker(4).
    Distribution(async.Hash).
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        for _, user := range users {
            // 按用户 ID 哈希，保证同用户任务落在同一分片
            sg.GoKeyed(user.ID, ctx, func(ctx context.Context) (string, error) {
                return processUser(ctx, user)
            })
        }
        results := sg.Wait()
        handleResults(results)
        return nil
    })
```

### 指定分片索引 + GoAt

```go
err := async.GroupSharded[string]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        // 提交到分片 2 的索引位置 0
        sg.GoAt(2, 0, ctx, func(ctx context.Context) (string, error) {
            return process(item)
        })
        // 分片索引越界会自动使用轮询分发
        sg.GoAt(999, 0, ctx, func(ctx context.Context) (string, error) {
            return process(otherItem)
        })
        results := sg.Wait()
        handleResults(results)
        return nil
    })
```

### 批量分发 GoBatch

`GoBatch` 按轮询方式逐项分发；某项提交失败后仍会尝试提交后续项。返回的切片包含每项的输入索引、分片索引和提交错误，另外返回遇到的第一个提交错误。分片已经调用 `Wait` 时，该分片拒绝的提交会记录为 `ErrGroupWaited` 错误结果，并包含在后续 `sg.Wait()` 的结果中。

```go
err := async.GroupSharded[string]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        results, err := sg.GoBatch(ctx, items, func(ctx context.Context, item string) (string, error) {
            return process(item)
        })
        for _, r := range results {
            if r.Err != nil {
                log.Printf("分片 %d 索引 %d 提交失败", r.ShardIdx, r.Index)
            }
        }
        _ = err
        allResults := sg.Wait()
        handleResults(allResults)
        return nil
    })
```

### Config 函数式配置

```go
err := async.GroupSharded[int]().
    Config(func(cfg ShardGroupConfig[int]) ShardGroupConfig[int] {
        cfg.Shards = 16
        cfg.ConcurrencyPerShard = 8
        cfg.Distribution = RoundRobin
        return cfg
    }).
    Run(func(ctx context.Context, sg *async.ShardedGroup[int]) error {
        for _, item := range items {
            sg.Go(ctx, func(ctx context.Context) (int, error) {
                return process(item)
            })
        }
        results := sg.Wait()
        handleResults(results)
        return nil
    })
```

---

## ShardedGroup 实例方法

在 `Run` 回调中，`sg *async.ShardedGroup[T]` 提供以下方法：

### 任务分发

| 方法 | 说明 |
|------|------|
| `sg.Go(ctx, fn)` | 轮询分发到某分片 |
| `sg.GoAt(shardIdx, index, ctx, fn)` | 分发到指定分片指定索引 |
| `sg.GoKeyed(key, ctx, fn)` | 按 key 哈希分发 |
| `sg.GoBatch(ctx, items, fn)` | 批量分发，返回每项分片信息 |

### 等待与关闭

| 方法 | 说明 |
|------|------|
| `sg.Wait()` | 等待所有分片完成，合并结果 |
| `sg.WaitTimeout(d)` | 带超时等待，返回 `(results, ok)` |
| `sg.WaitContext(ctx)` | 带 context 等待，返回 `(results, ok)` |
| `sg.Close()` | 关闭所有分片 Group |
| `sg.Reset()` | 在等待完成且没有活跃任务后重置所有分片；会清除 FailFast，复用时需重新调用 `WithFailFast` 并使用其返回的 context |

`WaitTimeout` 或 `WaitContext` 返回 `ok == false` 时，任务可能仍在后台清理。需要最终结果或重置复用时，应随后调用 `sg.Wait()` 等待所有分片结束。

### 查询

| 方法 | 说明 |
|------|------|
| `sg.ShardCount()` | 返回分片数 |
| `sg.GetShard(idx)` | 获取指定分片的 `*Group[T]` 实例 |

### 配置代理

| 方法 | 说明 |
|------|------|
| `sg.WithTimeout(d)` | 为所有分片设置任务超时 |
| `sg.WithSubmitTimeout(d)` | 为所有分片设置提交超时 |
| `sg.WithFailFast(ctx)` | 为所有分片启用 FailFast，返回新 ctx |
| `sg.WithFFCtx(ctx)` | `WithFailFast` 的缩写 |
| `sg.WithStreaming(bufSize)` | 为所有分片启用流式结果消费 |
| `sg.WithResultCallback(fn)` | 为所有分片设置结果回调 |
| `sg.StreamResults()` | Fan-in 所有分片的流式结果 channel |

### 统计汇总

| 方法 | 说明 |
|------|------|
| `sg.TotalFailCount()` | 汇总各分片失败数 |
| `sg.TotalOkCount()` | 汇总各分片成功数 |

> **建议**：`TotalOkCount()` 聚合全部分片的已完成成功任务。提交仍在进行或分片尚未等待完成时，返回值是实时快照；需要最终值时请先 `Wait()`。
| `sg.TotalActive()` | 汇总各分片活跃任务数 |
| `sg.TotalBusy()` | 汇总各分片忙碌任务数 |
| `sg.TotalWorker()` | 汇总各分片并发度之和 |

---

## 完整示例

### ShardPool：大规模数据处理

```go
func processLargeDataset(ctx context.Context, items []*Record) error {
    return async.PoolSharded[*Record]().Context(ctx).
        Shards(16).           // 16 个分片
        Worker(8).            // 每分片 8 worker = 总共 128 worker
        MaxPending(100000).   // 每分片最大待处理
        Timeout(10 * time.Second).
        FailFast().
        Run(func(ctx context.Context, sp *async.ShardedPool[*Record]) error {
            for _, item := range items {
                sp.Submit(ctx, func(ctx context.Context) (*Record, error) {
                    return enrichRecord(ctx, item)
                })
            }
            results := sp.Wait()
            for _, r := range results {
                if !r.Ok() {
                    log.Printf("处理失败: %v", r.Err)
                }
            }
            return nil
        })
}
```

### ShardPool + StreamResults 流式消费

```go
func processWithStreaming(ctx context.Context, items []string) error {
    return async.PoolSharded[string]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sp *async.ShardedPool[string]) error {
            sp.WithStreaming(1024)

            go func() {
                for _, item := range items {
                    sp.Submit(ctx, func(ctx context.Context) (string, error) {
                        return process(item)
                    })
                }
                sp.WaitAndClose()
            }()

            for r := range sp.StreamResults() {
                if !r.Ok() {
                    log.Printf("失败: %v", r.Err)
                    continue
                }
                saveResult(r.Value)
            }
            return nil
        })
}
```

### ShardedGroup + 分批处理

```go
func batchProcessUsers(ctx context.Context, users []User) error {
    return async.GroupSharded[User]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sg *async.ShardedGroup[User]) error {
            const batchSize = 100
            for i := 0; i < len(users); i += batchSize {
                end := i + batchSize
                if end > len(users) {
                    end = len(users)
                }
                batch := users[i:end]
                results, err := sg.GoBatch(ctx, batch, func(ctx context.Context, u User) (User, error) {
                    return validateUser(ctx, u)
                })
                if err != nil {
                    return err
                }
                for _, r := range results {
                    if r.Err != nil {
                        log.Printf("提交失败: 分片=%d 索引=%d", r.ShardIdx, r.Index)
                    }
                }
            }
            all := sg.Wait()
            for _, r := range all {
                if !r.Ok() {
                    log.Printf("处理失败: %v", r.Err)
                }
            }
            return nil
        })
}
```

### ShardedGroup + WaitContext 超时等待

```go
func processWithDeadline(ctx context.Context, items []string) error {
    ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()

    return async.GroupSharded[string]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
            for _, item := range items {
                sg.Go(ctx, func(ctx context.Context) (string, error) {
                    return process(item)
                })
            }
            results, ok := sg.WaitTimeout(25 * time.Second)
            if !ok {
                log.Println("等待超时，部分任务可能未完成")
            }
            handleResults(results)
            return nil
        })
}
```

### ShardedGroup + 调用者主动取消

结合 `context.WithCancel` 和 `WaitContext`，调用方可在外部主动取消所有分片中的任务：

```go
func processWithCancel(ctx context.Context, items []string) error {
    // ① 由调用方掌控取消时机
    parentCtx, cancel := context.WithCancel(context.Background())

    // ② 另一 goroutine 主动取消
    go func() {
        time.Sleep(500 * time.Millisecond)
        cancel() // ← 取消链传播到每个分片的 Group
    }()

    // ③ 所有分片任务绑定 parentCtx
    err := async.GroupSharded[string]().Context(parentCtx).
        Shards(4).Worker(2).
        Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
            for _, item := range items {
                sg.Go(ctx, func(ctx context.Context) (string, error) {
                    select {
                    case <-ctx.Done():
                        return "", ctx.Err()
                    case <-time.After(10 * time.Second):
                        return process(item)
                    }
                })
            }

            // ④ WaitContext 兜底
            waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
            defer waitCancel()
            results, ok := sg.WaitContext(waitCtx)
            // cancel() 触发 → 任务 Done → WaitContext 正常结束
            _ = results
            _ = ok
            return nil
        })
    return err
}
```

### ShardedGroup + 超时自动取消

通过 `WaitTimeout` 或 `context.WithTimeout` + `WaitContext` 在超时后向各分片任务发送取消信号：

```go
// 方式一：WaitTimeout 直接传超时
results, ok := sg.WaitTimeout(100 * time.Millisecond)

// 方式二：WaitContext + context.WithTimeout
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()
results, ok := sg.WaitContext(ctx)
```

> 两种方式都使用共享的等待期限。超时后各分片向任务上下文发送取消信号；任务必须主动响应上下文才能及时退出，忽略取消的任务仍会继续运行。返回的结果是当时的快照；需要最终结果或复用前应释放阻塞的任务并再次调用 `Wait()` 等待清理完成。

### ShardedGroup + FailFast 快速失败

```go
func processWithFailFast(ctx context.Context, items []string) error {
    return async.GroupSharded[string]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
            sg, ffCtx := sg.WithFailFast(ctx)
            for _, item := range items {
                sg.Go(ffCtx, func(ctx context.Context) (string, error) {
                    return process(item)
                })
            }
            results := sg.Wait()

            if ffCtx.Err() != nil {
                log.Println("FailFast 触发，部分任务被取消")
            }
            handleResults(results)
            return nil
        })
}
```

---

## 默认值体系

### ShardPoolBuilder 默认值

| 默认值 | 获取/恢复方法 | 默认值（源码常量） | 说明 |
|--------|-------------|-------------------|------|
| 分片数 | `.DefaultShards()` | **4** | `DefaultShardCount = 4` |
| 每片 Worker | `.DefaultWorker()` | **`IO()`**（`NumCPU×2`） | `DefaultSizePerShard = 0`（0 自动=IO） |
| 分发策略 | `.DefaultDistribution()` | **RoundRobin** | `DefaultDistribution = RoundRobin` |
| 超时 | `.DefaultTimeout()` | **不限时**（0） | 清除超时设置 |
| FailFast | `.DefaultFailFast()` | **关闭**（false） | 关闭快速失败 |
| 最大排队 | `.DefaultMaxPending()` | **无限制**（0） | 清除排队限制 |
| 最大结果 | `.DefaultMaxResults()` | 全局默认值（初始为 100,000） | 不覆盖 Pool 的默认结果上限 |
| 完整配置 | `.DefaultShardPoolConfig()` | — | 重置所有配置为 `DefaultConfig()` |
| 日志 | `.DefaultLogger()` | **静默** | `core.SetLogger(nil)` |

### ShardedGroupBuilder 默认值

| 默认值 | 获取/恢复方法 | 默认值（源码常量） | 说明 |
|--------|-------------|-------------------|------|
| 分片数 | `.DefaultShards()` | **4** | `DefaultShardCount = 4` |
| 每片并发度 | `.DefaultWorker()` | **`IO()`**（`NumCPU×2`） | `DefaultSizePerShard = 0`（0 自动=IO） |
| 分发策略 | `.DefaultDistribution()` | **RoundRobin** | `DefaultDistribution = RoundRobin` |
| 完整配置 | `.DefaultShardedGroupConfig()` | — | 重置所有配置为 `DefaultConfig()` |
| 日志 | `.DefaultLogger()` | **静默** | `core.SetLogger(nil)` |

### 默认值覆盖优先级

```
全局常量（DefaultShardCount=4, DefaultSizePerShard=0, DefaultDistribution=RoundRobin）
    ↓ 被覆盖
Builder 级设置（.Shards(n), .Worker(n), .Distribution(d) 等）
    ↓ 被覆盖
Default* 恢复为特定默认值
    ↓ 构建
Run() 创建 ShardedPool / ShardedGroup 实例
```

---

## 生产环境使用建议

### PoolSharded vs GroupSharded 选型

| 场景 | 推荐 | 理由 |
|------|------|------|
| 长期运行的服务消费 | **PoolSharded** | Worker 复用，持续处理 |
| 一次性批量任务 | **GroupSharded** | 用完即销毁，代码简单 |
| 高 QPS 需横向扩展 | **PoolSharded** | 分片 Pool 长期复用 |
| Key 亲和分发 | **两者均可** | 都支持 Hash+Key 路由 |

### 分片数选择

| 预期并发度 | 推荐分片数 | 理由 |
|-----------|-----------|------|
| < 50 | 4（默认） | 默认已足够 |
| 50 ~ 200 | 8~16 | 充分分散锁竞争 |
| > 200 | GOMAXPROCS~32 | 高并发充分横向扩展 |

> **⚠️ 分片越多不一定越好**：分片数和每片 Worker 的乘积 = 总 Worker，过多分片会增加调度开销。

### 分发策略选择

| 场景 | 推荐策略 | 理由 |
|------|---------|------|
| 任务均匀、无亲和需求 | **RoundRobin** | 负载最均匀 |
| 需要同一用户/Key 的分片亲和性 | **Hash + KeyFn**（批量）或 `SubmitKeyed` | 同 key 任务落在同一分片，但不保证串行 |
| 不可预测的任务量 | **RoundRobin** | 按提交顺序轮流选择分片 |

### 生产推荐配置

```go
// ShardPool 生产级配置
err := async.PoolSharded[Record]().Context(ctx).
    Shards(16).                       // 16 分片
    Worker(8).                        // 每片 8 worker = 128 总 worker
    Distribution(async.Hash).         // Hash 分发保证同 key 在同一分片
    KeyFn(func(r Record) uint64 {
        return uint64(r.UserID)
    }).
    Timeout(30 * time.Second).       // 单任务超时
    FailFast().                        // 任一失败立即停止
    MaxPending(100000).               // 每片最大排队
    Run(func(ctx context.Context, sp *async.ShardedPool[Record]) error {
        _, err := sp.SubmitBatch(ctx, records, func(ctx context.Context, r Record) (Record, error) {
            return process(ctx, r)
        })
        if err != nil {
            return err
        }
        results := sp.Wait()
        return handleResults(results)
    })

// ShardedGroup 生产级配置
err = async.GroupSharded[Record]().Context(ctx).
    Shards(16).                             // 16 分片
    Worker(4).                              // 每分片 4 worker = 64 总 worker
    Run(func(ctx context.Context, sg *async.ShardedGroup[Record]) error {
        sg.WithTimeout(30 * time.Second)    // 单任务超时
        _, ffCtx := sg.WithFailFast(ctx)    // 任一任务失败时取消其他任务
        for _, v := range records {
            if err := sg.GoKeyed(fmt.Sprint(v.UserID), ffCtx, func(ctx context.Context) (Record, error) {
                return process(ctx, v)
            }); err != nil {
                return err
            }
        }
        return handleResults(sg.Wait())
    })
```

### 常见错误

- **❌ PoolSharded 不 Wait 直接返回**：Run 回调结束时自动 Close，必须 Wait 后再返回
- **❌ 假设同片任务一定串行**：同片可有多个 worker；需要顺序时须另行串行化。批量 Hash 未设 KeyFn 时落在第 0 片。
- **❌ SubmitAt 跨分片**：`SubmitAt(shardIdx, index, ...)` 的 shardIdx 必须 < Shards 数
- **❌ GetShard 返回的 Pool 直接使用**：修改分片 Pool 的配置需通过 `With*` 代理方法，直接操作可能导致分片间不一致
- **❌ 忽略 ShardedGroup 的 `WithFailFast` 返回值**：`sg, ffCtx := sg.WithFailFast(ctx)` 返回新的 ffCtx，后续 Go 必须使用此 ctx
```
