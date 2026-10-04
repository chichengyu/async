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
> Hash 策略需配合 `KeyFn` 使用，或在运行时通过 `SubmitKeyed`/`GoKeyed` 方法指定 key。

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
| `.FailFast()` | 启用快速失败 |
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
> `fn func(ctx context.Context, sp *ShardedPool[T]) error`
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
> `fn func(ctx context.Context, sg *ShardedGroup[T]) error`
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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
    Run(func(ctx context.Context, sp *ShardedPool[string]) error {
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
        for _, userID := range userIDs {
            sp.Submit(ctx, func(ctx context.Context) (int, error) {
                return processUser(ctx, userID)
            })
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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

```go
err := async.PoolSharded[int]().
    Config(func(cfg ShardPoolConfig[int]) ShardPoolConfig[int] {
        cfg.Shards = 12
        cfg.SizePerShard = 6
        cfg.Distribution = Hash
        cfg.KeyFn = func(i int) uint64 { return uint64(i) }
        return cfg
    }).
    Run(func(ctx context.Context, sp *ShardedPool[int]) error {
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

---

## ShardPool 实例方法

在 `Run` 回调中，`sp *ShardedPool[T]` 提供以下方法：

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
| `sp.WaitAndClose()` | 等待完成并关闭 |
| `sp.Close()` | 关闭所有分片 |
| `sp.Reset()` | 重置所有分片（等待后无活跃任务时可用） |

### 查询

| 方法 | 说明 |
|------|------|
| `sp.ShardCount()` | 返回分片数 |
| `sp.GetShard(idx)` | 获取指定分片的 `*Pool[T]` 实例 |

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
    Run(func(ctx context.Context, sg *ShardedGroup[string]) error {
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
    Run(func(ctx context.Context, sg *ShardedGroup[string]) error {
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
    Run(func(ctx context.Context, sg *ShardedGroup[string]) error {
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
    Run(func(ctx context.Context, sg *ShardedGroup[string]) error {
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

```go
err := async.GroupSharded[string]().Shards(8).Worker(4).
    Run(func(ctx context.Context, sg *ShardedGroup[string]) error {
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
    Run(func(ctx context.Context, sg *ShardedGroup[int]) error {
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

在 `Run` 回调中，`sg *ShardedGroup[T]` 提供以下方法：

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
| `sg.Reset()` | 重置所有分片 |

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
| `sg.TotalSuccessCount()` | 汇总各分片成功数 |
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
        Run(func(ctx context.Context, sp *ShardedPool[*Record]) error {
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
        Run(func(ctx context.Context, sp *ShardedPool[string]) error {
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
        Run(func(ctx context.Context, sg *ShardedGroup[User]) error {
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
        Run(func(ctx context.Context, sg *ShardedGroup[string]) error {
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

### ShardedGroup + FailFast 快速失败

```go
func processWithFailFast(ctx context.Context, items []string) error {
    return async.GroupSharded[string]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sg *ShardedGroup[string]) error {
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
| 最大结果 | `.DefaultMaxResults()` | **无限**（0） | 清除结果上限 |
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
| 同一用户/Key 需顺序处理 | **Hash + KeyFn** | 同 key 任务在同一分片 |
| 需要优先级队列 | 自定义 KeyFn | 按优先级哈希分组 |
| 不可预测的任务量 | **RoundRobin** | 动态负载均衡 |

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
    Run(func(ctx context.Context, sp *ShardedPool[Record]) error {
        for _, r := range records {
            sp.Submit(ctx, func(ctx context.Context) (Record, error) {
                return process(ctx, r)
            })
        }
        results := sp.Wait()
        return handleResults(results)
    })
```

### 常见错误

- **❌ PoolSharded 不 Wait 直接返回**：Run 回调结束时自动 Close，必须 Wait 后再返回
- **❌ Hash 模式未设 KeyFn**：Hash 模式依赖 KeyFn 确定分片，未设置时行为不可预测
- **❌ SubmitAt 跨分片**：`SubmitAt(shardIdx, index, ...)` 的 shardIdx 必须 < Shards 数
- **❌ GetShard 返回的 Pool 直接使用**：修改分片 Pool 的配置需通过 `With*` 代理方法，直接操作可能导致分片间不一致
- **❌ 忽略 ShardedGroup 的 `WithFailFast` 返回值**：`sg, ffCtx := sg.WithFailFast(ctx)` 返回新的 ffCtx，后续 Go 必须使用此 ctx
```