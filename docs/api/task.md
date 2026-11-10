# task 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [task](../task.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*AsyncResult[T].Cancel`

```go
func (ar *AsyncResult[T]) Cancel() (T, error)
```

[实现](../../internal/task/task.go#L178) · [行为专题](../task.md)：Cancel 优先返回已缓存或可立即读取的结果，否则等待结果或 1ms 定时器。

**源码说明与使用片段**（原文）：

Cancel 优先返回已缓存或可立即读取的结果，否则等待结果或 1ms 定时器。 定时分支返回类型零值和 context.Canceled，不取消业务或覆盖最终缓存。 1ms 是定时器时长，不保证包含调度和锁等待的总耗时上限。

使用示例：

	ar := task.Go(ctx, slowFn)
	time.Sleep(100 * time.Millisecond)
	if _, err := ar.Cancel(); err != nil {
	    fmt.Println("本次等待已结束或任务返回错误:", err) // 任务执行未被此方法取消
	}


**参数说明**：无显式参数。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Cancel()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：通过 Go/GoAct 或对应构建器创建并共享指针，零值不可用，不要复制包含锁的句柄。返回T只作值复制，切片/map/指针内容共享；缓存锁不保护引用对象内部，后续读写须由业务同步或复制。优先返回已缓存或可立即读到的实际结果，否则创建1ms定时器等待；定时分支返回类型零值和context.Canceled，不覆盖最终缓存、不取消业务或提前释放业务许可。1ms仅为定时器时长，不保证包含调度及锁等待的总耗时上限；结果和定时器同时就绪不保证分支。业务本身也可能返回context.Canceled；需区分来源时用带ok的WaitContext/WaitTimeout。取消业务使用任务context或Task.Cancel。

## `*AsyncResult[T].Error`

```go
func (ar *AsyncResult[T]) Error() error
```

[实现](../../internal/task/task.go#L140) · [行为专题](../task.md)：Error 阻塞等待任务完成，只返回错误（成功时为 nil）。

**源码说明与使用片段**（原文）：

Error 阻塞等待任务完成，只返回错误（成功时为 nil）。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Error()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：通过 Go/GoAct 或对应构建器创建并共享指针，零值不可用，不要复制包含锁的句柄。返回T只作值复制，切片/map/指针内容共享；缓存锁不保护引用对象内部，后续读写须由业务同步或复制。阻塞直到任务返回并共享缓存结果；可并发重复调用，不能以 Ok/IsPanic 作非阻塞轮询。任务忽略 context 时可能永久等待，应为等待另设 WaitTimeout/WaitContext。

## `*AsyncResult[T].IsPanic`

```go
func (ar *AsyncResult[T]) IsPanic() bool
```

[实现](../../internal/task/task.go#L237) · [行为专题](../task.md)：IsPanic 阻塞等待并返回错误是否为 panic 导致。

**源码说明与使用片段**（原文）：

IsPanic 阻塞等待并返回错误是否为 panic 导致。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsPanic()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：通过 Go/GoAct 或对应构建器创建并共享指针，零值不可用，不要复制包含锁的句柄。返回T只作值复制，切片/map/指针内容共享；缓存锁不保护引用对象内部，后续读写须由业务同步或复制。阻塞直到任务返回并共享缓存结果；可并发重复调用，不能以 Ok/IsPanic 作非阻塞轮询。任务忽略 context 时可能永久等待，应为等待另设 WaitTimeout/WaitContext。

## `*AsyncResult[T].Ok`

```go
func (ar *AsyncResult[T]) Ok() bool
```

[实现](../../internal/task/task.go#L231) · [行为专题](../task.md)：Ok 阻塞等待并返回任务是否成功（无错误无 panic）。

**源码说明与使用片段**（原文）：

Ok 阻塞等待并返回任务是否成功（无错误无 panic）。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示对应结果成功；空集合规则见方法说明。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Ok()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：通过 Go/GoAct 或对应构建器创建并共享指针，零值不可用，不要复制包含锁的句柄。返回T只作值复制，切片/map/指针内容共享；缓存锁不保护引用对象内部，后续读写须由业务同步或复制。阻塞直到任务返回并共享缓存结果；可并发重复调用，不能以 Ok/IsPanic 作非阻塞轮询。任务忽略 context 时可能永久等待，应为等待另设 WaitTimeout/WaitContext。

## `*AsyncResult[T].Values`

```go
func (ar *AsyncResult[T]) Values() (T, error)
```

[实现](../../internal/task/task.go#L135) · [行为专题](../task.md)：Values 是 Wait 的别名，统一命名风格。

**源码说明与使用片段**（原文）：

Values 是 Wait 的别名，统一命名风格。 阻塞等待任务完成，返回值和错误。


**参数说明**：无显式参数。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：通过 Go/GoAct 或对应构建器创建并共享指针，零值不可用，不要复制包含锁的句柄。返回T只作值复制，切片/map/指针内容共享；缓存锁不保护引用对象内部，后续读写须由业务同步或复制。阻塞直到任务返回并共享缓存结果；可并发重复调用，不能以 Ok/IsPanic 作非阻塞轮询。任务忽略 context 时可能永久等待，应为等待另设 WaitTimeout/WaitContext。

## `*AsyncResult[T].Wait`

```go
func (ar *AsyncResult[T]) Wait() (T, error)
```

[实现](../../internal/task/task.go#L128) · [行为专题](../task.md)：Wait 阻塞等待任务完成，返回值和错误。

**源码说明与使用片段**（原文）：

Wait 阻塞等待任务完成，返回值和错误。 多个 goroutine 可同时调用 Wait，结果是安全的。

使用示例：

	ar := task.Go(ctx, fn)
	value, err := ar.Wait()
	if err != nil {
	    log.Printf("任务失败: %v", err)
	}


**参数说明**：无显式参数。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Wait()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：通过 Go/GoAct 或对应构建器创建并共享指针，零值不可用，不要复制包含锁的句柄。返回T只作值复制，切片/map/指针内容共享；缓存锁不保护引用对象内部，后续读写须由业务同步或复制。阻塞直到任务返回并共享缓存结果；可并发重复调用，不能以 Ok/IsPanic 作非阻塞轮询。任务忽略 context 时可能永久等待，应为等待另设 WaitTimeout/WaitContext。

## `*AsyncResult[T].WaitCh`

```go
func (ar *AsyncResult[T]) WaitCh() <-chan core.Result[T]
```

[实现](../../internal/task/task.go#L157) · [行为专题](../task.md)：WaitCh 返回只读结果 channel，可用于 select 多路复用。

**源码说明与使用片段**（原文）：

WaitCh 返回只读结果 channel，可用于 select 多路复用。 每次调用创建新的容量为 1 的通知通道及 goroutine，发送一次缓存结果后关闭。 即使任务已完成，通知也须等待 goroutine 调度；读取关闭后的通道须检查 ok。

使用示例：

	select {
	case r := <-ar.WaitCh():
	    fmt.Println("任务完成:", r.Value)
	case <-ctx.Done():
	    fmt.Println("上下文取消")
	}


**参数说明**：无显式参数。

**返回值签名**：`<-chan core.Result[T]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitCh()
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：通过 Go/GoAct 或对应构建器创建并共享指针，零值不可用，不要复制包含锁的句柄。返回T只作值复制，切片/map/指针内容共享；缓存锁不保护引用对象内部，后续读写须由业务同步或复制。每次调用另建一个容量为1的通知通道及goroutine，共享同一缓存结果；就绪结果也须等待通知goroutine调度。发送一次后关闭，重复读取须检查ok，关闭后的零值不表示新的任务成功。生产复用一次调用返回的通道；停止接收不取消业务，业务不返回则通知goroutine继续等待。

## `*AsyncResult[T].WaitContext`

```go
func (ar *AsyncResult[T]) WaitContext(ctx context.Context) (T, error, bool)
```

[实现](../../internal/task/task.go#L325) · [行为专题](../task.md)：WaitContext 带 Context 控制等待，返回 (值, 错误, 是否取得任务结果)。

**源码说明与使用片段**（原文）：

WaitContext 带 Context 控制等待，返回 (值, 错误, 是否取得任务结果)。 已缓存或可立即读到的结果优先返回；否则等待结果或 ctx 取消。 取消分支返回 (zero, ctx.Err(), false)，不取消业务执行。 多个 goroutine 可同时调用，结果是安全的。

参数：

  - ctx：非 nil 的等待上下文；取消与结果同时就绪时不保证选择顺序

使用示例：

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	val, err, ok := ar.WaitContext(ctx)
	if !ok {
	    log.Printf("等待被取消: %v", err)
	    return
	}


**参数说明**：

- `ctx` (`context.Context`)：仅控制此次等待的非 nil context；若缓存或结果通道已有结果，会优先返回实际值/错误和 true，即使此 context 已取消。否则等待结果与取消，取消返回类型零值、ctx.Err() 和 false；完成与取消同时就绪时 select 可选任一路径。

**返回值签名**：`T, error, bool`。检查错误后再使用成功值。末尾 bool 为 true 表示取得任务结果，业务成功与否仍看 error；false 表示本次等待超时或取消，任务继续运行，稍后可再次获取结果。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitContext(ctx)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：通过任务入口创建并共享句柄指针，零值不可用，不要复制句柄。已缓存或可立即读到的结果优先，包括非正timeout或已取消等待context；进入等待后结果与取消/超时同时就绪不保证选择顺序。false返回零值与ErrTimeout或ctx.Err()；true取得实际业务结果，仍须检查error，业务也可能返回context错误。T内切片/map/指针引用共享，缓存锁不保护其内部，后续修改须自行同步或复制。这里只限制本次等待，不覆盖缓存、不停止任务或提前释放业务许可；取消业务须用任务context或Task.Cancel并由回调协作退出。

## `*AsyncResult[T].WaitTimeout`

```go
func (ar *AsyncResult[T]) WaitTimeout(timeout time.Duration) (T, error, bool)
```

[实现](../../internal/task/task.go#L257) · [行为专题](../task.md)：WaitTimeout 带超时等待，返回 (值, 错误, 是否取得任务结果)。

**源码说明与使用片段**（原文）：

WaitTimeout 带超时等待，返回 (值, 错误, 是否取得任务结果)。 已缓存或可立即读取的结果优先，包括 timeout 非正时；业务错误另查 error。 超时返回 (类型零值, core.ErrTimeout, false)，不取消业务或覆盖最终缓存。

参数：

  - timeout：本次等待的定时器时长，非正值立即到期；不保证总耗时上限

使用示例：

	ar := task.Go(ctx, fn)
	val, err, ok := ar.WaitTimeout(3 * time.Second)
	if !ok {
	    fmt.Println("等待超时，稍后仍可获取结果")
	    return
	}


**参数说明**：

- `timeout` (`time.Duration`)：本次等待结果的时长；非正值不阻塞等待，但已缓存或已到达的结果仍优先返回。等待超时返回 ErrTimeout 和 false，不取消任务，可稍后再次等待。

**返回值签名**：`T, error, bool`。检查错误后再使用成功值。末尾 bool 为 true 表示取得任务结果，业务成功与否仍看 error；false 表示本次等待超时或取消，任务继续运行，稍后可再次获取结果。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WaitTimeout(timeout)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：通过任务入口创建并共享句柄指针，零值不可用，不要复制句柄。已缓存或可立即读到的结果优先，包括非正timeout或已取消等待context；进入等待后结果与取消/超时同时就绪不保证选择顺序。false返回零值与ErrTimeout或ctx.Err()；true取得实际业务结果，仍须检查error，业务也可能返回context错误。T内切片/map/指针引用共享，缓存锁不保护其内部，后续修改须自行同步或复制。这里只限制本次等待，不覆盖缓存、不停止任务或提前释放业务许可；取消业务须用任务context或Task.Cancel并由回调协作退出。

## `*BoundedRunner.Available`

```go
func (r *BoundedRunner) Available() int
```

[实现](../../internal/task/bounded.go#L103) · [行为专题](../task.md)：Available 返回当前可用的并发槽位数。

**源码说明与使用片段**（原文）：

Available 返回当前可用的并发槽位数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Available()
```

**生产建议**：复用同一执行器并限制生产者数量，传可取消 context，逐项等待结果并处理错误。许可只约束业务回调，查询不预留容量；业务须协作退出，避免持有同一许可时同步嵌套提交。

**注意事项**：使用 NewBoundedRunner 或其 Builder 初始化并复用；不提供 Close/Wait 方法，任务结果由各句柄等待。Busy 为已占用槽位数，包含获得槽位后尚未开始的任务；Available/Busy 分别读取，不是联合原子快照，查询后另行提交仍可能阻塞。上限约束业务许可，不限制调用方等待提交或业务另建的 goroutine；取消不强制回调退出或释放许可。

## `*BoundedRunner.Busy`

```go
func (r *BoundedRunner) Busy() int
```

[实现](../../internal/task/bounded.go#L109) · [行为专题](../task.md)：Busy 返回当前已占用槽位数，包含取得槽位后尚未开始执行的任务。

**源码说明与使用片段**（原文）：

Busy 返回当前已占用槽位数，包含取得槽位后尚未开始执行的任务。 与 Available 分别读取，不保证两次调用形成同一时刻的快照。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Busy()
```

**生产建议**：复用同一执行器并限制生产者数量，传可取消 context，逐项等待结果并处理错误。许可只约束业务回调，查询不预留容量；业务须协作退出，避免持有同一许可时同步嵌套提交。

**注意事项**：使用 NewBoundedRunner 或其 Builder 初始化并复用；不提供 Close/Wait 方法，任务结果由各句柄等待。Busy 为已占用槽位数，包含获得槽位后尚未开始的任务；Available/Busy 分别读取，不是联合原子快照，查询后另行提交仍可能阻塞。上限约束业务许可，不限制调用方等待提交或业务另建的 goroutine；取消不强制回调退出或释放许可。

## `*BoundedRunner.Max`

```go
func (r *BoundedRunner) Max() int
```

[实现](../../internal/task/bounded.go#L98) · [行为专题](../task.md)：Max 返回业务许可容量，不是进程 goroutine 数上限。

**源码说明与使用片段**（原文）：

Max 返回业务许可容量，不是进程 goroutine 数上限。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Max()
```

**生产建议**：复用同一执行器并限制生产者数量，传可取消 context，逐项等待结果并处理错误。许可只约束业务回调，查询不预留容量；业务须协作退出，避免持有同一许可时同步嵌套提交。

**注意事项**：使用 NewBoundedRunner 或其 Builder 初始化并复用；不提供 Close/Wait 方法，任务结果由各句柄等待。Busy 为已占用槽位数，包含获得槽位后尚未开始的任务；Available/Busy 分别读取，不是联合原子快照，查询后另行提交仍可能阻塞。上限约束业务许可，不限制调用方等待提交或业务另建的 goroutine；取消不强制回调退出或释放许可。

## `*BoundedRunnerBuilder.Build`

```go
func (b *BoundedRunnerBuilder) Build() *BoundedRunner
```

[实现](../../internal/task/bounded.go#L79) · [行为专题](../task.md)：Build 创建 BoundedRunner 实例。

**源码说明与使用片段**（原文）：

Build 创建 BoundedRunner 实例。


**参数说明**：无显式参数。

**返回值签名**：`*BoundedRunner`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Build()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Max 保存数值，Build 每次创建独立信号量；非正并发度构建时按 IO()，不限制正值上界。同一执行器须在循环外复用才能限制整体并发；等待空位发生在提交方，无 Close 或统一 Wait，分别保存/等待任务句柄。

## `*BoundedRunnerBuilder.Context`

```go
func (b *BoundedRunnerBuilder) Context(ctx context.Context) *BoundedRunnerBuilder
```

[实现](../../internal/task/bounded.go#L58) · [行为专题](../task.md)：Context 设置构建器上下文并注入 trace_id；Build 创建的执行器不保存该上下文。

**源码说明与使用片段**（原文）：

Context 设置构建器上下文并注入 trace\_id；Build 创建的执行器不保存该上下文。


**参数说明**：

- `ctx` (`context.Context`)：仅保存在构建器中的上下文，会注入 trace_id；Build 不将它保存到 BoundedRunner，提交时必须另外传入所需任务 context。

**返回值签名**：`*BoundedRunnerBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：只保存经 trace_id 处理的 context，须非 nil；Build 创建的 BoundedRunner 不保存此字段，不能用它取消后续任务。每次 BoundedGo/BoundedGoResult 必须另传业务 context，配置不与 Build 并发。

## `*BoundedRunnerBuilder.DefaultLogger`

```go
func (b *BoundedRunnerBuilder) DefaultLogger() *BoundedRunnerBuilder
```

[实现](../../internal/task/bounded.go#L76) · [行为专题](../task.md)：DefaultLogger 重置为默认日志实现。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现。


**参数说明**：无显式参数。

**返回值签名**：`*BoundedRunnerBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*BoundedRunnerBuilder.Logger`

```go
func (b *BoundedRunnerBuilder) Logger(l core.Logger) *BoundedRunnerBuilder
```

[实现](../../internal/task/bounded.go#L70) · [行为专题](../task.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*BoundedRunnerBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*BoundedRunnerBuilder.Max`

```go
func (b *BoundedRunnerBuilder) Max(n int) *BoundedRunnerBuilder
```

[实现](../../internal/task/bounded.go#L64) · [行为专题](../task.md)：Max 设置业务许可容量。<=0 使用默认 IO 并发度。

**源码说明与使用片段**（原文）：

Max 设置业务许可容量。\<=0 使用默认 IO 并发度。


**参数说明**：

- `n` (`int`)：Max 设置业务许可容量。<=0 使用默认 IO 并发度。

**返回值签名**：`*BoundedRunnerBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Max(n)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：Max 保存数值，Build 每次创建独立信号量；非正并发度构建时按 IO()，不限制正值上界。同一执行器须在循环外复用才能限制整体并发；等待空位发生在提交方，无 Close 或统一 Wait，分别保存/等待任务句柄。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.max = n
```

## `*Mu[T].Append`

```go
func (m *Mu[T]) Append(add func() T)
```

[实现](../../internal/task/mu.go#L43) · [行为专题](../task.md)：Append 线程安全地追加元素。add 函数在锁外执行以保证并发性能，

**源码说明与使用片段**（原文）：

Append 线程安全地追加元素。add 函数在锁外执行以保证并发性能， append 操作在锁内执行保证线程安全。 add 须非 nil；其 panic 直接传播且不会追加元素。接收者须非 nil，Mu 使用后不要复制。 生成函数可重入本容器；其已完成的嵌套追加和其他副作用不因外层 panic 回滚。

参数：

  - add：生成要追加元素的函数（在锁外执行，可并发）

使用示例：

	var mu task.Mu[int]
	mu.Append(func() int { return 42 })
	mu.Append(func() int { return computeSomething() })


**参数说明**：

- `add` (`func() T`)：在调用方锁外同步执行的元素生成函数，必须非 nil；其返回值随后在锁内追加，不为生成过程创建 goroutine。可重入同一容器的 Append/Snapshot；panic 直接传播，不追加该次返回值，但已完成的嵌套追加和其他副作用不回滚。多个调用可同时生成，须同步共享数据。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Append(add)
```

**生产建议**：共享同一个零值容器指针；生成函数保持短小并自行同步共享副作用，外层 panic 不回滚已完成操作。调用方管理生产者生命周期，按内存预算限制累计元素数。

**注意事项**：零值可用，使用后不要复制包含互斥锁的 Mu。Append 在调用方锁外同步生成，可重入且 panic 直接传播；已完成的嵌套追加和其他副作用不回滚。Snapshot 不等待仍在生成值的生产者，只复制当前元素槽位，引用内容仍共享；完整收集先由调用方等待生产者。非 nil 容器空快照非 nil，nil 接收者仅支持 Snapshot 并返回 nil。

## `*Mu[T].Snapshot`

```go
func (m *Mu[T]) Snapshot() []T
```

[实现](../../internal/task/mu.go#L60) · [行为专题](../task.md)：Snapshot 返回当前所有元素的浅拷贝，线程安全；nil 接收者返回 nil。

**源码说明与使用片段**（原文）：

Snapshot 返回当前所有元素的浅拷贝，线程安全；nil 接收者返回 nil。 元素中的指针、map、切片仍共享，修改其引用内容须另外同步。 不等待仍在锁外生成值的 Append；完整收集前须由调用方等待生产者。

使用示例：

	items := mu.Snapshot()
	for _, item := range items {
	    fmt.Println(item)
	}


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Snapshot()
```

**生产建议**：完整收集前先等待生产者结束，再取得快照；按元素总量和读取频率预算复制成本，引用元素的读写另行同步。需要长期大量快照时评估内存峰值。

**注意事项**：零值可用，使用后不要复制包含互斥锁的 Mu。Append 在调用方锁外同步生成，可重入且 panic 直接传播；已完成的嵌套追加和其他副作用不回滚。Snapshot 不等待仍在生成值的生产者，只复制当前元素槽位，引用内容仍共享；完整收集先由调用方等待生产者。非 nil 容器空快照非 nil，nil 接收者仅支持 Snapshot 并返回 nil。

## `*TaskBuilder[T].Bounded`

```go
func (b *TaskBuilder[T]) Bounded(max int) *TaskBuilder[T]
```

[实现](../../internal/task/task_builder.go#L65) · [行为专题](../task.md)：Bounded 使用 BoundedRunner 限制同时持有许可的业务回调数。

**源码说明与使用片段**（原文）：

Bounded 使用 BoundedRunner 限制同时持有许可的业务回调数。 max 为最大并发数，\<=0 时使用默认 IO 并发度。


**参数说明**：

- `max` (`int`)：共享本构建器的终端调用可同时运行的最大任务数；非正值使用 core.IO()。每次调用 Bounded 都创建新执行器，因此应配置一次并复用构建器。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Bounded(max)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：在循环外配置并复用同一个构建器。每次新建 Task().Bounded(max) 都产生独立限额，无法约束跨实例总并发；等槽位发生在调用方，任务超时从回调开始执行时计时。

## `*TaskBuilder[T].Context`

```go
func (b *TaskBuilder[T]) Context(ctx context.Context) *TaskBuilder[T]
```

[实现](../../internal/task/task_builder.go#L46) · [行为专题](../task.md)：Context 链式设置上下文，自动注入 trace_id。

**源码说明与使用片段**（原文）：

Context 链式设置上下文，自动注入 trace\_id。


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：保存父 context 并确保 trace_id，须非 nil；只影响后续启动的任务，不更换已运行任务的 context。取消后回调仍须配合返回；配置不能与终端启动并发修改。

## `*TaskBuilder[T].DefaultBounded`

```go
func (b *TaskBuilder[T]) DefaultBounded() *TaskBuilder[T]
```

[实现](../../internal/task/task_builder.go#L71) · [行为专题](../task.md)：DefaultBounded 清除 BoundedRunner 限制。

**源码说明与使用片段**（原文）：

DefaultBounded 清除 BoundedRunner 限制。


**参数说明**：无显式参数。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBounded()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：清除当前构建器的限额引用，后续任务直接启动，不影响此前已启动任务或其信号量。它不会等待此前任务完成；重新 Bounded 会创建新的独立限额，跨新旧批次并发可能超过原限制。

## `*TaskBuilder[T].DefaultLogger`

```go
func (b *TaskBuilder[T]) DefaultLogger() *TaskBuilder[T]
```

[实现](../../internal/task/task_builder.go#L80) · [行为专题](../task.md)：DefaultLogger 重置为默认日志实现。

**源码说明与使用片段**（原文）：

DefaultLogger 重置为默认日志实现。


**参数说明**：无显式参数。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*TaskBuilder[T].DefaultTimeout`

```go
func (b *TaskBuilder[T]) DefaultTimeout() *TaskBuilder[T]
```

[实现](../../internal/task/task_builder.go#L58) · [行为专题](../task.md)：DefaultTimeout 清除超时设置。

**源码说明与使用片段**（原文）：

DefaultTimeout 清除超时设置。


**参数说明**：无显式参数。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：正数期限从回调开始执行时创建，不含 Bounded 等空位时间；非正值及 DefaultTimeout 清除此构建器的显式期限，不读取全局默认任务超时。回调退出时取消该子 context，Task.Ctx 是外层 context，不暴露此期限，也不自动表示任务完成。业务须响应 context，否则不会被强制结束、提前释放许可或自动改写返回错误。

## `*TaskBuilder[T].Go`

```go
func (b *TaskBuilder[T]) Go(fn func(context.Context) (T, error)) *AsyncResult[T]
```

[实现](../../internal/task/task_builder.go#L87) · [行为专题](../task.md)：Go 启动异步任务，返回 AsyncResult[T]。

**源码说明与使用片段**（原文）：

Go 启动异步任务，返回 AsyncResult\[T]。 通过 Wait() 获取结果；可取消传入的父 context，业务函数须响应取消。 返回句柄不提供取消任务的函数，AsyncResult.Cancel 只取消等待。


**参数说明**：

- `fn` (`func(context.Context) (T, error)`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`*AsyncResult[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Go(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：先保存当前超时包装再启动回调；启用 Bounded 时终端在调用方等待许可或父取消，返回句柄前不受回调期限或后续 WaitTimeout 限制，整体预算须放在父 context。已获取许可但启动前trace日志注入panic或Goexit时，提交方defer归还许可，原异常不转为任务错误，也不返回句柄；成功启动后仅由业务退出时释放，包含panic及Goexit。超时从实际回调执行计时，退出时取消子 context；Task.Ctx 不暴露该期限或充当完成通知，Task 用后应 Cancel。业务 panic 转为 PanicError，业务Goexit记录零值与ErrTaskGoexit，结果供所有等待者缓存读取，不属于PanicError；业务应正常返回error。忽略取消返回的业务结果仍被保留；Go/GoAct 句柄的 Cancel 只限制等待，GoResult/GoResultAct 的 Task.Cancel 才请求业务取消。父已取消不保证回调不会开始，须自行检查；必须回收句柄及错误。

## `*TaskBuilder[T].GoAct`

```go
func (b *TaskBuilder[T]) GoAct(fn func(context.Context) error) *AsyncResult[NoResult]
```

[实现](../../internal/task/task_builder.go#L107) · [行为专题](../task.md)：GoAct 启动无返回值异步任务，返回 AsyncResult[NoResult]。

**源码说明与使用片段**（原文）：

GoAct 启动无返回值异步任务，返回 AsyncResult\[NoResult]。 fn 签名为 func(ctx) error。


**参数说明**：

- `fn` (`func(context.Context) error`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`*AsyncResult[NoResult]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoAct(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：先保存当前超时包装再启动回调；启用 Bounded 时终端在调用方等待许可或父取消，返回句柄前不受回调期限或后续 WaitTimeout 限制，整体预算须放在父 context。已获取许可但启动前trace日志注入panic或Goexit时，提交方defer归还许可，原异常不转为任务错误，也不返回句柄；成功启动后仅由业务退出时释放，包含panic及Goexit。超时从实际回调执行计时，退出时取消子 context；Task.Ctx 不暴露该期限或充当完成通知，Task 用后应 Cancel。业务 panic 转为 PanicError，业务Goexit记录零值与ErrTaskGoexit，结果供所有等待者缓存读取，不属于PanicError；业务应正常返回error。忽略取消返回的业务结果仍被保留；Go/GoAct 句柄的 Cancel 只限制等待，GoResult/GoResultAct 的 Task.Cancel 才请求业务取消。父已取消不保证回调不会开始，须自行检查；必须回收句柄及错误。

## `*TaskBuilder[T].GoResult`

```go
func (b *TaskBuilder[T]) GoResult(fn func(context.Context) (T, error)) Task[T]
```

[实现](../../internal/task/task_builder.go#L97) · [行为专题](../task.md)：GoResult 启动可取消的异步任务，返回 Task[T]。

**源码说明与使用片段**（原文）：

GoResult 启动可取消的异步任务，返回 Task\[T]。 通过 Task.Cancel() 可主动取消，通过 Task.Result() 获取结果。


**参数说明**：

- `fn` (`func(context.Context) (T, error)`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`Task[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoResult(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：先保存当前超时包装再启动回调；启用 Bounded 时终端在调用方等待许可或父取消，返回句柄前不受回调期限或后续 WaitTimeout 限制，整体预算须放在父 context。已获取许可但启动前trace日志注入panic或Goexit时，提交方defer归还许可，原异常不转为任务错误，也不返回句柄；成功启动后仅由业务退出时释放，包含panic及Goexit。超时从实际回调执行计时，退出时取消子 context；Task.Ctx 不暴露该期限或充当完成通知，Task 用后应 Cancel。业务 panic 转为 PanicError，业务Goexit记录零值与ErrTaskGoexit，结果供所有等待者缓存读取，不属于PanicError；业务应正常返回error。忽略取消返回的业务结果仍被保留；Go/GoAct 句柄的 Cancel 只限制等待，GoResult/GoResultAct 的 Task.Cancel 才请求业务取消。父已取消不保证回调不会开始，须自行检查；必须回收句柄及错误。

## `*TaskBuilder[T].GoResultAct`

```go
func (b *TaskBuilder[T]) GoResultAct(fn func(context.Context) error) Task[NoResult]
```

[实现](../../internal/task/task_builder.go#L117) · [行为专题](../task.md)：GoResultAct 启动无返回值的可取消异步任务，返回 Task[NoResult]。

**源码说明与使用片段**（原文）：

GoResultAct 启动无返回值的可取消异步任务，返回 Task\[NoResult]。 fn 签名为 func(ctx) error，可通过 Task.Cancel() 主动取消。


**参数说明**：

- `fn` (`func(context.Context) error`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`Task[NoResult]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GoResultAct(fn)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：先保存当前超时包装再启动回调；启用 Bounded 时终端在调用方等待许可或父取消，返回句柄前不受回调期限或后续 WaitTimeout 限制，整体预算须放在父 context。已获取许可但启动前trace日志注入panic或Goexit时，提交方defer归还许可，原异常不转为任务错误，也不返回句柄；成功启动后仅由业务退出时释放，包含panic及Goexit。超时从实际回调执行计时，退出时取消子 context；Task.Ctx 不暴露该期限或充当完成通知，Task 用后应 Cancel。业务 panic 转为 PanicError，业务Goexit记录零值与ErrTaskGoexit，结果供所有等待者缓存读取，不属于PanicError；业务应正常返回error。忽略取消返回的业务结果仍被保留；Go/GoAct 句柄的 Cancel 只限制等待，GoResult/GoResultAct 的 Task.Cancel 才请求业务取消。父已取消不保证回调不会开始，须自行检查；必须回收句柄及错误。

## `*TaskBuilder[T].Logger`

```go
func (b *TaskBuilder[T]) Logger(l core.Logger) *TaskBuilder[T]
```

[实现](../../internal/task/task_builder.go#L77) · [行为专题](../task.md)：Logger 注入自定义日志实现，全局生效。

**源码说明与使用片段**（原文）：

Logger 注入自定义日志实现，全局生效。


**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。

**注意事项**：立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。

## `*TaskBuilder[T].WithTimeout`

```go
func (b *TaskBuilder[T]) WithTimeout(d time.Duration) *TaskBuilder[T]
```

[实现](../../internal/task/task_builder.go#L52) · [行为专题](../task.md)：WithTimeout 设置任务超时。超时 context 会注入到 fn 的 ctx 参数中。

**源码说明与使用片段**（原文）：

WithTimeout 设置任务超时。超时 context 会注入到 fn 的 ctx 参数中。


**参数说明**：

- `d` (`time.Duration`)：业务回调开始执行后的 context 超时；正数生效，非正值不限时。Bounded 等槽位时间不计入此时长，回调须响应 context 才能退出。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithTimeout(d)
```

**生产建议**：在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。

**注意事项**：正数期限从回调开始执行时创建，不含 Bounded 等空位时间；非正值及 DefaultTimeout 清除此构建器的显式期限，不读取全局默认任务超时。回调退出时取消该子 context，Task.Ctx 是外层 context，不暴露此期限，也不自动表示任务完成。业务须响应 context，否则不会被强制结束、提前释放许可或自动改写返回错误。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.timeout = d
```

## `BoundedGo`

```go
func BoundedGo[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T]
```

[实现](../../internal/task/bounded.go#L120) · [行为专题](../task.md)：BoundedGo 通过信号量限流后启动异步任务，返回 AsyncResult[T]。

**源码说明与使用片段**（原文）：

BoundedGo 通过信号量限流后启动异步任务，返回 AsyncResult\[T]。 当业务许可已耗尽时，在调用方阻塞等待 slot 释放或 ctx 取消。

重要：fn 必须正确响应 ctx.Done() 以按时释放信号量槽位。 如果 fn 忽略 ctx 取消而持续阻塞（例如未在 HTTP 请求中使用 ctx）， 对应的信号量槽位将永久占用，最终耗尽所有槽位导致 BoundedRunner 完全阻塞。 建议在 fn 内部所有 I/O 操作中传递 ctx，或设置合理的业务超时。


**参数说明**：

- `r` (`*BoundedRunner`)：复用的有界任务执行器，所有需要共享并发上限的提交使用同一个实例。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`*AsyncResult[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
task.BoundedGo[T](r, ctx, fn)
```

**生产建议**：复用同一执行器并限制生产者数量，传可取消 context，逐项等待结果并处理错误。许可只约束业务回调，查询不预留容量；业务须协作退出，避免持有同一许可时同步嵌套提交。

**注意事项**：提交方先阻塞获取共享执行器许可，空位和取消同时就绪时可能仍开始任务；取消分支返回已保存 context 错误的句柄，不调用业务。获取后若启动前trace日志注入panic或Goexit，提交方defer归还许可，原异常仍传播或结束提交goroutine，不返回句柄也不调用业务；成功启动后由回调退出时释放，错误/panic/Goexit也释放，取消不直接释放。业务Goexit不是panic，等待得到零值与ErrTaskGoexit并可重复读取，业务退出时许可照常归还，生产应正常返回error。忽略取消后正常返回仍保留业务结果。勿在持有同一执行器许可的回调中同步提交并等待下一项，满额时会死锁。不限制提交方和业务另建的 goroutine。执行器须在循环外复用，逐句柄等待/处理错误，无统一 Close/Wait。

## `BoundedGoAct`

```go
func BoundedGoAct(r *BoundedRunner, ctx context.Context, fn func(context.Context) error) *AsyncResultNoResult
```

[实现](../../internal/task/bounded.go#L149) · [行为专题](../task.md)：BoundedGoAct 带限流的无返回值异步任务，返回 AsyncResultNoResult。

**源码说明与使用片段**（原文）：

BoundedGoAct 带限流的无返回值异步任务，返回 AsyncResultNoResult。 关于 ctx 响应的注意事项与 BoundedGo 相同。


**参数说明**：

- `r` (`*BoundedRunner`)：复用的有界任务执行器，所有需要共享并发上限的提交使用同一个实例。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) error`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`*AsyncResultNoResult`。

**使用示例**（已初始化参数的调用片段）：

```go
task.BoundedGoAct(r, ctx, fn)
```

**生产建议**：复用同一执行器并限制生产者数量，传可取消 context，逐项等待结果并处理错误。许可只约束业务回调，查询不预留容量；业务须协作退出，避免持有同一许可时同步嵌套提交。

**注意事项**：提交方先阻塞获取共享执行器许可，空位和取消同时就绪时可能仍开始任务；取消分支返回已保存 context 错误的句柄，不调用业务。获取后若启动前trace日志注入panic或Goexit，提交方defer归还许可，原异常仍传播或结束提交goroutine，不返回句柄也不调用业务；成功启动后由回调退出时释放，错误/panic/Goexit也释放，取消不直接释放。业务Goexit不是panic，等待得到零值与ErrTaskGoexit并可重复读取，业务退出时许可照常归还，生产应正常返回error。忽略取消后正常返回仍保留业务结果。勿在持有同一执行器许可的回调中同步提交并等待下一项，满额时会死锁。不限制提交方和业务另建的 goroutine。执行器须在循环外复用，逐句柄等待/处理错误，无统一 Close/Wait。

## `BoundedGoResult`

```go
func BoundedGoResult[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) Task[T]
```

[实现](../../internal/task/bounded.go#L175) · [行为专题](../task.md)：BoundedGoResult 带限流的可取消异步任务，返回 Task[T]。

**源码说明与使用片段**（原文）：

BoundedGoResult 带限流的可取消异步任务，返回 Task\[T]。 关于 ctx 响应的注意事项与 BoundedGo 相同。


**参数说明**：

- `r` (`*BoundedRunner`)：复用的有界任务执行器，所有需要共享并发上限的提交使用同一个实例。
- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `fn` (`func(context.Context) (T, error)`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`Task[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
task.BoundedGoResult[T](r, ctx, fn)
```

**生产建议**：复用同一执行器并限制生产者数量，传可取消 context，逐项等待结果并处理错误。许可只约束业务回调，查询不预留容量；业务须协作退出，避免持有同一许可时同步嵌套提交。

**注意事项**：提交方先阻塞获取共享执行器许可，空位和取消同时就绪时可能仍开始任务；取消分支返回已保存 context 错误的句柄，不调用业务。获取后若启动前trace日志注入panic或Goexit，提交方defer归还许可，原异常仍传播或结束提交goroutine，不返回句柄也不调用业务；成功启动后由回调退出时释放，错误/panic/Goexit也释放，取消不直接释放。业务Goexit不是panic，等待得到零值与ErrTaskGoexit并可重复读取，业务退出时许可照常归还，生产应正常返回error。忽略取消后正常返回仍保留业务结果。勿在持有同一执行器许可的回调中同步提交并等待下一项，满额时会死锁。不限制提交方和业务另建的 goroutine。执行器须在循环外复用，逐句柄等待/处理错误，无统一 Close/Wait。

## `Go`

```go
func Go[T any](ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T]
```

[实现](../../internal/task/task.go#L391) · [行为专题](../task.md)：Go 启动一个异步任务，返回 AsyncResult[T]。

**源码说明与使用片段**（原文）：

Go 启动一个异步任务，返回 AsyncResult\[T]。 自动捕获 panic 并包装为 PanicError。 ctx 会自动注入 trace\_id（通过 EnsureTraceID）。

参数：

  - ctx：上下文，自动注入 trace\_id
  - fn：异步执行的函数

使用示例：

	// 启动异步 HTTP 请求
	ar := task.Go(ctx, func(ctx context.Context) (*http.Response, error) {
	    req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	    return http.DefaultClient.Do(req)
	})
	// 等待结果
	resp, err := ar.Wait()


**参数说明**：

- `ctx` (`context.Context`)：上下文，自动注入 trace_id
- `fn` (`func(context.Context) (T, error)`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`*AsyncResult[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
task.Go[T](ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：启动前在调用方同步注入trace_id，自定义logger.With/WithContext异常直接传播或结束调用goroutine，不产生句柄或业务调用。正常立即启动 goroutine，父 context 须非 nil，不以父已取消为由自动跳过 fn；回调须主动检查并响应 context。业务 panic 转为 PanicError，子 goroutine panic 不在恢复范围内。业务Goexit不是panic，记录零值与ErrTaskGoexit供所有等待者缓存读取，不重试业务，生产仍应正常返回error表达失败。结果经句柄缓存，可并发重复等待；Go/GoAct 的 AsyncResult.Cancel 只限制等待，GoResult/GoResultAct 才提供取消业务 context 的 Task.Cancel。Task 用完应调用 Cancel 释放派生 context。

## `GoAct`

```go
func GoAct(ctx context.Context, fn func(context.Context) error) *AsyncResult[NoResult]
```

[实现](../../internal/task/noresult.go#L33) · [行为专题](../task.md)：GoAct 启动一个无返回值的异步任务，返回 AsyncResult[NoResult]。

**源码说明与使用片段**（原文）：

GoAct 启动一个无返回值的异步任务，返回 AsyncResult\[NoResult]。

参数：

  - ctx：上下文，自动注入 trace\_id
  - fn：异步执行的函数，只返回 error

使用示例：

	ar := task.GoAct(ctx, func(ctx context.Context) error {
	    return sendNotification(ctx, userID, msg)
	})
	_, err := ar.Wait() // 忽略 NoResult 值，只关心 error


**参数说明**：

- `ctx` (`context.Context`)：上下文，自动注入 trace_id
- `fn` (`func(context.Context) error`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`*AsyncResult[NoResult]`。

**使用示例**（已初始化参数的调用片段）：

```go
task.GoAct(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：启动前在调用方同步注入trace_id，自定义logger.With/WithContext异常直接传播或结束调用goroutine，不产生句柄或业务调用。正常立即启动 goroutine，父 context 须非 nil，不以父已取消为由自动跳过 fn；回调须主动检查并响应 context。业务 panic 转为 PanicError，子 goroutine panic 不在恢复范围内。业务Goexit不是panic，记录零值与ErrTaskGoexit供所有等待者缓存读取，不重试业务，生产仍应正常返回error表达失败。结果经句柄缓存，可并发重复等待；Go/GoAct 的 AsyncResult.Cancel 只限制等待，GoResult/GoResultAct 才提供取消业务 context 的 Task.Cancel。Task 用完应调用 Cancel 释放派生 context。

## `GoResult`

```go
func GoResult[T any](ctx context.Context, fn func(context.Context) (T, error)) Task[T]
```

[实现](../../internal/task/task.go#L444) · [行为专题](../task.md)：GoResult 启动一个可取消的异步任务，返回 Task[T]。

**源码说明与使用片段**（原文）：

GoResult 启动一个可取消的异步任务，返回 Task\[T]。 与 Go 的区别：返回 Task 包含 Cancel 方法，可主动取消。

参数：

  - ctx：上下文，自动注入 trace\_id
  - fn：异步执行的函数

使用示例：

	// 启动可取消的长任务
	t := task.GoResult(ctx, func(ctx context.Context) ([]Item, error) {
	    return fetchItems(ctx)
	})
	// 5秒后取消
	time.AfterFunc(5*time.Second, t.Cancel)
	items, err := t.Result()


**参数说明**：

- `ctx` (`context.Context`)：上下文，自动注入 trace_id
- `fn` (`func(context.Context) (T, error)`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`Task[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
task.GoResult[T](ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：启动前在调用方同步注入trace_id，自定义logger.With/WithContext异常直接传播或结束调用goroutine，不产生句柄或业务调用。正常立即启动 goroutine，父 context 须非 nil，不以父已取消为由自动跳过 fn；回调须主动检查并响应 context。业务 panic 转为 PanicError，子 goroutine panic 不在恢复范围内。业务Goexit不是panic，记录零值与ErrTaskGoexit供所有等待者缓存读取，不重试业务，生产仍应正常返回error表达失败。结果经句柄缓存，可并发重复等待；Go/GoAct 的 AsyncResult.Cancel 只限制等待，GoResult/GoResultAct 才提供取消业务 context 的 Task.Cancel。Task 用完应调用 Cancel 释放派生 context。

## `GoResultAct`

```go
func GoResultAct(ctx context.Context, fn func(context.Context) error) Task[NoResult]
```

[实现](../../internal/task/noresult.go#L69) · [行为专题](../task.md)：GoResultAct 启动一个无返回值的可取消异步任务，返回 Task[NoResult]。

**源码说明与使用片段**（原文）：

GoResultAct 启动一个无返回值的可取消异步任务，返回 Task\[NoResult]。

参数：

  - ctx：上下文，自动注入 trace\_id
  - fn：异步执行的函数，只返回 error

使用示例：

	// 启动可取消的后台任务
	t := task.GoResultAct(ctx, func(ctx context.Context) error {
	    return uploadFile(ctx, filepath)
	})
	// 可随时取消
	time.AfterFunc(10*time.Second, t.Cancel)
	_, err := t.Result()


**参数说明**：

- `ctx` (`context.Context`)：上下文，自动注入 trace_id
- `fn` (`func(context.Context) error`)：在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。

**返回值签名**：`Task[NoResult]`。

**使用示例**（已初始化参数的调用片段）：

```go
task.GoResultAct(ctx, fn)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：启动前在调用方同步注入trace_id，自定义logger.With/WithContext异常直接传播或结束调用goroutine，不产生句柄或业务调用。正常立即启动 goroutine，父 context 须非 nil，不以父已取消为由自动跳过 fn；回调须主动检查并响应 context。业务 panic 转为 PanicError，子 goroutine panic 不在恢复范围内。业务Goexit不是panic，记录零值与ErrTaskGoexit供所有等待者缓存读取，不重试业务，生产仍应正常返回error表达失败。结果经句柄缓存，可并发重复等待；Go/GoAct 的 AsyncResult.Cancel 只限制等待，GoResult/GoResultAct 才提供取消业务 context 的 Task.Cancel。Task 用完应调用 Cancel 释放派生 context。

## `NewBoundedRunner`

```go
func NewBoundedRunner(max int) *BoundedRunner
```

[实现](../../internal/task/bounded.go#L85) · [行为专题](../task.md)：NewBoundedRunner 创建一个限制同时持有许可的业务回调数的执行器。

**源码说明与使用片段**（原文）：

NewBoundedRunner 创建一个限制同时持有许可的业务回调数的执行器。 max \<= 0 时使用默认 IO 并发度。


**参数说明**：

- `max` (`int`)：NewBoundedRunner 创建一个限制同时持有许可的业务回调数的执行器。

**返回值签名**：`*BoundedRunner`。

**使用示例**（已初始化参数的调用片段）：

```go
task.NewBoundedRunner(max)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立信号量，不立即启动 worker；非正最大并发按调用时 IO()，没有正数上界校验。零值执行器的 nil 信号量不能替代构造器。复用同一指针才能约束总并发，Busy 包括已取得但尚未执行的槽位；查询不预留额度，下一次提交仍可阻塞。

## `NewBoundedRunnerBuilder`

```go
func NewBoundedRunnerBuilder() *BoundedRunnerBuilder
```

[实现](../../internal/task/bounded.go#L50) · [行为专题](../task.md)：NewBoundedRunnerBuilder 创建 BoundedRunner 链式构建器。

**源码说明与使用片段**（原文）：

NewBoundedRunnerBuilder 创建 BoundedRunner 链式构建器。


**参数说明**：无显式参数。

**返回值签名**：`*BoundedRunnerBuilder`。

**使用示例**（已初始化参数的调用片段）：

```go
task.NewBoundedRunnerBuilder()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立构建器，context 初始为 Background，尚不启动业务。设置只供后续终端操作使用，不能与启动并发修改；TaskBuilder.Bounded 每次创建新的限额，BoundedRunnerBuilder 的 context 当前不传入 Build 返回的执行器。

## `NewDefaultBoundedRunner`

```go
func NewDefaultBoundedRunner() *BoundedRunner
```

[实现](../../internal/task/bounded.go#L93) · [行为专题](../task.md)：NewDefaultBoundedRunner 使用默认 IO 并发度创建执行器。

**源码说明与使用片段**（原文）：

NewDefaultBoundedRunner 使用默认 IO 并发度创建执行器。


**参数说明**：无显式参数。

**返回值签名**：`*BoundedRunner`。

**使用示例**（已初始化参数的调用片段）：

```go
task.NewDefaultBoundedRunner()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立信号量，不立即启动 worker；非正最大并发按调用时 IO()，没有正数上界校验。零值执行器的 nil 信号量不能替代构造器。复用同一指针才能约束总并发，Busy 包括已取得但尚未执行的槽位；查询不预留额度，下一次提交仍可阻塞。

## `NewTaskBuilder`

```go
func NewTaskBuilder[T any]() *TaskBuilder[T]
```

[实现](../../internal/task/task_builder.go#L41) · [行为专题](../task.md)：NewTaskBuilder 创建任务构建器，默认使用 context.Background()。

**源码说明与使用片段**（原文）：

NewTaskBuilder 创建任务构建器，默认使用 context.Background()。 通过 .Context(ctx) 链式设置上下文。


**参数说明**：无显式参数。

**返回值签名**：`*TaskBuilder[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
task.NewTaskBuilder[T]()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：创建独立构建器，context 初始为 Background，尚不启动业务。设置只供后续终端操作使用，不能与启动并发修改；TaskBuilder.Bounded 每次创建新的限额，BoundedRunnerBuilder 的 context 当前不传入 Build 返回的执行器。

## `Task[T].Error`

```go
func (t Task[T]) Error() error
```

[实现](../../internal/task/task.go#L61) · [行为专题](../task.md)：Error 阻塞等待任务完成，只返回错误（成功时为 nil）。

**源码说明与使用片段**（原文）：

Error 阻塞等待任务完成，只返回错误（成功时为 nil）。


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Error()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：应使用 GoResult/GoResultAct 或对应构建器创建句柄，零值的 Result/Cancel 字段为 nil。构造器提供的 Result 可并发重复调用并缓存实际结果；句柄值副本共享缓存，T 内切片/map/指针引用不深拷贝，后续修改需自行同步。Cancel 只发出 context 信号，不强制中止任务，手动替换函数字段后须自行维护这些约定。

## `Task[T].Result`

```go
Result func() (T, error)
```

[实现](../../internal/task/task.go#L57) · [行为专题](../task.md)：可调用函数字段：获取结果的函数

**源码说明与使用片段**（原文）：

获取结果的函数


**参数说明**：无显式参数。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Result()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：应使用 GoResult/GoResultAct 或对应构建器创建句柄，零值的 Result/Cancel 字段为 nil。构造器提供的 Result 可并发重复调用并缓存实际结果；句柄值副本共享缓存，T 内切片/map/指针引用不深拷贝，后续修改需自行同步。Cancel 只发出 context 信号，不强制中止任务，手动替换函数字段后须自行维护这些约定。

