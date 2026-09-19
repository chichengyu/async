# async

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/chichengyu/async.svg)](https://pkg.go.dev/github.com/chichengyu/async)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat)](LICENSE)

泛型 Go 并发工具库，提供协程池、任务组、异步任务、Map/Reduce、重试、限流、管道等开箱即用的并发原语。

## 安装

```bash
go get github.com/chichengyu/async
```

## 快速开始

```go
package main

import (
    "context"
    "fmt"

    "github.com/chichengyu/async"
)

func main() {
    ctx := context.Background()

    // 并发 Map：对每个元素并发执行转换
    results, _ := async.Map(ctx, []int{1, 2, 3, 4, 5}, func(ctx context.Context, n int) (int, error) {
        return n * 2, nil
    }, 3)

    for _, r := range results {
        fmt.Println(r.Value) // 2, 4, 6, 8, 10
    }
}
```

## 模块概览

| 包 | 说明 |
|---|---|
| `async` | 顶层入口，重导出所有子包的类型与函数 |
| `core` | 基础类型、错误、日志、全局配置 |
| `pool` | 泛型协程池 `Pool[T]`，复用 goroutine 处理高频小任务 |
| `group` | 泛型任务组 `Group[T]`，一次性批量并发任务 |
| `task` | 单个异步任务 `Task[T]` 与可取消的 `AsyncResult[T]` |
| `mapreduce` | 并发 `Map`、`ForEach`、`Reduce`、`Chunk` 等数据并行操作 |
| `retry` | 指数退避重试与超时控制 |
| `ratelimit` | 速率限制器，支持阻塞/拒绝/强制阻塞策略 |
| `pipeline` | 多阶段数据处理管道 |

## 主要功能

### Pool - 协程池

适合高频小任务的协程复用场景，支持超时控制、快速失败、提交超时。

```go
p := async.NewPool[int](4)
defer p.Close()

for i := 0; i < 10; i++ {
    p.Submit(ctx, i, func(ctx context.Context, n int) (int, error) {
        return n * n, nil
    })
}

results, err := p.Wait()
```

### Group - 任务组

适合一次性批量并发任务，每次 `Go` 新建 goroutine。

```go
g := async.NewGroup[string](4)
g.Go(ctx, "hello", func(ctx context.Context, s string) (string, error) {
    return s + " world", nil
})
results, err := g.Wait()
```

### Map / ForEach / Reduce

对集合进行并发操作。

```go
// Map
results, _ := async.Map(ctx, items, transformFn, concurrency)

// ForEach
async.ForEach(ctx, items, fn, concurrency)

// Reduce
result, _ := async.Reduce(ctx, items, initial, reducer, concurrency)
```

### Retry - 重试

指数退避重试，支持最大重试次数和退避时间限制。

```go
val, err := async.RetryWithBackoff(ctx, fn, 3, 100*time.Millisecond, 5*time.Second)
```

### RateLimiter - 限流

```go
rl := async.NewRateLimiter(10, time.Second) // 每秒 10 次
rl.Wait(ctx)
```

### Pipeline - 管道

多阶段数据处理，前一阶段输出作为后一阶段输入。

```go
stages := []async.Stage[int]{
    {Name: "stage1", Concurrency: 2},
    {Name: "stage2", Concurrency: 3},
}
results, _ := async.Execute(ctx, stages, items, fn)
```

## 配置

```go
// 全局默认超时（影响 Group 和 Pool）
async.SetDefaultTimeout(30 * time.Second)

// Pool 提交超时
async.SetSubmitTimeout(5 * time.Second)

// 清理 goroutine 最大存活时间
async.SetMaxCleanupDuration(30 * time.Minute)

// 任务失败日志级别
async.SetTaskFailLogLevel(async.LogLevelWarn)

// Trace 日志开关
async.SetTraceLogEnabled(true)
```

## 日志接口

本库不依赖任何第三方日志框架，采用接口注入方式。默认使用空日志实现（静默运行），你可以注入任意日志实现。

```go
// 注入自定义日志实现（例如基于 zerolog）
async.SetLogger(myLogger)

// 获取当前日志器
logger := async.GetLogger()
```

如果需要与 zerolog 集成，可以实现 `async.Logger` 接口：

```go
type Logger interface {
    Log(ctx context.Context, level LogLevel, msg string, fields ...LogField)
    With(fields ...LogField) Logger
    WithContext(ctx context.Context) context.Context
}
```

## 并发度控制

```go
async.CPU()   // 返回 CPU 密集型并发度，默认为 runtime.NumCPU()
async.IO()    // 返回 IO 密集型并发度，默认为 runtime.NumCPU() * 2
```

## 依赖

零外部依赖，仅需 Go 1.25+。

## License

MIT