# core 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [config](../config.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*AutoScaleConfig.Normalize`

```go
func (c *AutoScaleConfig) Normalize()
```

[实现](../../internal/core/autoscale.go#L61) · [行为专题](../config.md)：Normalize 原地把非正数字段填为默认值，不校验字段之间的约束或浮点有限性。

**源码说明与使用片段**（原文）：

Normalize 原地把非正数字段填为默认值，不校验字段之间的约束或浮点有限性。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Normalize()
```

**生产建议**：先补齐默认值，再验证最小/最大 worker、有限阈值及扩缩容倍数的业务约束；以副本完成配置后再启用实例。

**注意事项**：原地把非正字段填为默认值，不校验 MinWorkers<=MaxWorkers、浮点有限性或阈值及缩放倍数的相互关系。先校验业务配置，在实例启用前调用，不与其他读写并发。

## `*PanicError.Error`

```go
func (e *PanicError) Error() string
```

[实现](../../internal/core/core.go#L411) · [行为专题](../config.md)：Error 返回 panic 错误的简短描述。

**源码说明与使用片段**（原文）：

Error 返回 panic 错误的简短描述。


**参数说明**：无显式参数。

**返回值签名**：`string`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Error()
```

**生产建议**：用 errors.As 识别错误链中的 PanicError，并按需检查 Value；仅在受控日志中使用 fmt 的 %+v 输出调用栈。通常让 fmt 调用 Format，不直接构造 fmt.State。

**注意事项**：NewPanicError 捕获创建处当前 goroutine 的栈，保留原 Value 引用，不深拷贝；Value/Stack 是可变公开字段，后续格式化读取当前值，共享修改须同步。Unwrap 始终返回 nil，即使 Value 本身为 error 也不加入错误链。Format 对各动词均写出 panic: 加原值，仅 'v' 且有 '+' 时追加换行和当前 Stack；忽略宽度/精度，不为 %q 加引号或转义。需转义先 Error() 再格式化字符串，生产中仅向受控日志输出栈；Result 的 %+v 不转发此标记。

## `*PanicError.Format`

```go
func (e *PanicError) Format(s fmt.State, verb rune)
```

[实现](../../internal/core/core.go#L421) · [行为专题](../config.md)：Format 实现 fmt.Formatter，支持 %+v 输出完整调用栈。

**源码说明与使用片段**（原文）：

Format 实现 fmt.Formatter，支持 %+v 输出完整调用栈。


**参数说明**：

- `s` (`fmt.State`)：fmt 传入的输出状态；读取 Writer 和 '+' 标记，不读取宽度或精度。通常让 fmt 自动调用；首次写入出错立即返回，追加栈的写入错误不向外返回。
- `verb` (`rune`)：fmt 格式动词；仅 'v' 且带 '+' 时追加当前 Stack。其他动词仍输出相同简短描述，%q 不加引号或转义，%x 不做十六进制编码；需转义时先 Error() 得到字符串再格式化。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Format(s, verb)
```

**生产建议**：用 errors.As 识别错误链中的 PanicError，并按需检查 Value；仅在受控日志中使用 fmt 的 %+v 输出调用栈。通常让 fmt 调用 Format，不直接构造 fmt.State。

**注意事项**：NewPanicError 捕获创建处当前 goroutine 的栈，保留原 Value 引用，不深拷贝；Value/Stack 是可变公开字段，后续格式化读取当前值，共享修改须同步。Unwrap 始终返回 nil，即使 Value 本身为 error 也不加入错误链。Format 对各动词均写出 panic: 加原值，仅 'v' 且有 '+' 时追加换行和当前 Stack；忽略宽度/精度，不为 %q 加引号或转义。需转义先 Error() 再格式化字符串，生产中仅向受控日志输出栈；Result 的 %+v 不转发此标记。

## `*PanicError.Unwrap`

```go
func (e *PanicError) Unwrap() error
```

[实现](../../internal/core/core.go#L416) · [行为专题](../config.md)：Unwrap 返回 nil（PanicError 不可解包）。

**源码说明与使用片段**（原文）：

Unwrap 返回 nil（PanicError 不可解包）。


**参数说明**：无显式参数。

**返回值签名**：`error`。始终为 nil；panic 的原始 Value 即使实现 error 也不作为底层错误返回。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Unwrap()
```

**生产建议**：用 errors.As 识别错误链中的 PanicError，并按需检查 Value；仅在受控日志中使用 fmt 的 %+v 输出调用栈。通常让 fmt 调用 Format，不直接构造 fmt.State。

**注意事项**：NewPanicError 捕获创建处当前 goroutine 的栈，保留原 Value 引用，不深拷贝；Value/Stack 是可变公开字段，后续格式化读取当前值，共享修改须同步。Unwrap 始终返回 nil，即使 Value 本身为 error 也不加入错误链。Format 对各动词均写出 panic: 加原值，仅 'v' 且有 '+' 时追加换行和当前 Stack；忽略宽度/精度，不为 %q 加引号或转义。需转义先 Error() 再格式化字符串，生产中仅向受控日志输出栈；Result 的 %+v 不转发此标记。

## `*RingBuffer[T].Cap`

```go
func (rb *RingBuffer[T]) Cap() int
```

[实现](../../internal/core/ringbuffer.go#L141) · [行为专题](../config.md)：Cap 返回缓冲区总容量。

**源码说明与使用片段**（原文）：

Cap 返回缓冲区总容量。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Cap()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

## `*RingBuffer[T].Disposed`

```go
func (rb *RingBuffer[T]) Disposed() int
```

[实现](../../internal/core/ringbuffer.go#L131) · [行为专题](../config.md)：Disposed 返回因 OverflowDrop 策略而被丢弃的元素总数。

**源码说明与使用片段**（原文）：

Disposed 返回因 OverflowDrop 策略而被丢弃的元素总数。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Disposed()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

## `*RingBuffer[T].Dropped`

```go
func (rb *RingBuffer[T]) Dropped() int64
```

[实现](../../internal/core/ringbuffer.go#L136) · [行为专题](../config.md)：Dropped Disposed 的别名，兼容旧 API。

**源码说明与使用片段**（原文）：

Dropped Disposed 的别名，兼容旧 API。


**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Dropped()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

## `*RingBuffer[T].Flush`

```go
func (rb *RingBuffer[T]) Flush() []T
```

[实现](../../internal/core/ringbuffer.go#L106) · [行为专题](../config.md)：Flush 排空并返回所有元素（FIFO 顺序）。

**源码说明与使用片段**（原文）：

Flush 排空并返回所有元素（FIFO 顺序）。


**参数说明**：无显式参数。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Flush()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：按FIFO取出并清零相应内部槽位，返回独立结果切片；空时返回非nil空切片。FlushN非正（含MinInt）或大于现有数量时取全部，MaxInt不会按该值分配。元素为浅拷贝，返回引用仍由调用方持有；取出不清除Dropped计数，Reset才清零。

## `*RingBuffer[T].FlushN`

```go
func (rb *RingBuffer[T]) FlushN(n int) []T
```

[实现](../../internal/core/ringbuffer.go#L111) · [行为专题](../config.md)：FlushN 批量读取最多 n 个元素。

**源码说明与使用片段**（原文）：

FlushN 批量读取最多 n 个元素。


**参数说明**：

- `n` (`int`)：按 FIFO 顺序取出的最大元素数；非正值或超过现有元素数时取出全部，读取后移除相应元素。

**返回值签名**：`[]T`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FlushN(n)
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：按FIFO取出并清零相应内部槽位，返回独立结果切片；空时返回非nil空切片。FlushN非正（含MinInt）或大于现有数量时取全部，MaxInt不会按该值分配。元素为浅拷贝，返回引用仍由调用方持有；取出不清除Dropped计数，Reset才清零。

## `*RingBuffer[T].IsFull`

```go
func (rb *RingBuffer[T]) IsFull() bool
```

[实现](../../internal/core/ringbuffer.go#L99) · [行为专题](../config.md)：IsFull 返回缓冲区是否已满。

**源码说明与使用片段**（原文）：

IsFull 返回缓冲区是否已满。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsFull()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

## `*RingBuffer[T].Len`

```go
func (rb *RingBuffer[T]) Len() int
```

[实现](../../internal/core/ringbuffer.go#L92) · [行为专题](../config.md)：Len 返回缓冲区中当前元素数量。

**源码说明与使用片段**（原文）：

Len 返回缓冲区中当前元素数量。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Len()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

## `*RingBuffer[T].Overflow`

```go
func (rb *RingBuffer[T]) Overflow() OverflowStrategy
```

[实现](../../internal/core/ringbuffer.go#L146) · [行为专题](../config.md)：Overflow 返回当前溢出策略。

**源码说明与使用片段**（原文）：

Overflow 返回当前溢出策略。


**参数说明**：无显式参数。

**返回值签名**：`OverflowStrategy`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Overflow()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

## `*RingBuffer[T].Peek`

```go
func (rb *RingBuffer[T]) Peek() (T, bool)
```

[实现](../../internal/core/ringbuffer.go#L79) · [行为专题](../config.md)：Peek 读取最旧元素但不移除。

**源码说明与使用片段**（原文）：

Peek 读取最旧元素但不移除。


**参数说明**：无显式参数。

**返回值签名**：`T, bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Peek()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

## `*RingBuffer[T].Pop`

```go
func (rb *RingBuffer[T]) Pop() (T, bool)
```

[实现](../../internal/core/ringbuffer.go#L61) · [行为专题](../config.md)：Pop 读取并移除最旧元素。

**源码说明与使用片段**（原文）：

Pop 读取并移除最旧元素。


**参数说明**：无显式参数。

**返回值签名**：`T, bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pop()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：按FIFO取出原元素并清零对应内部槽位，空时返回零值和false；不深拷贝引用内容，返回对象仍可被调用方持有。操作后固定容量和丢弃计数保持；不与其他调用组合成原子事务。

## `*RingBuffer[T].Push`

```go
func (rb *RingBuffer[T]) Push(val T) bool
```

[实现](../../internal/core/ringbuffer.go#L38) · [行为专题](../config.md)：Push 写入一个元素。满时根据策略处理：

**源码说明与使用片段**（原文）：

Push 写入一个元素。满时根据策略处理： OverflowDrop：覆盖最旧元素 OverflowBlock：返回 false OverflowError：返回 false


**参数说明**：

- `val` (`T`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Push(val)
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
rb.buf[rb.tail] = val
rb.buf[rb.head] = val
```

## `*RingBuffer[T].Reset`

```go
func (rb *RingBuffer[T]) Reset()
```

[实现](../../internal/core/ringbuffer.go#L151) · [行为专题](../config.md)：Reset 清空缓冲区，重置所有指针。

**源码说明与使用片段**（原文）：

Reset 清空缓冲区，重置所有指针。


**参数说明**：无显式参数。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reset()
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：清零全部内部槽位、head/tail/size及丢弃计数，保留固定存储数组、容量和策略；重复调用可用，之后继续Push。不会改写已返回的结果切片或深度清理元素对象；旧引用仍存活时不保证对象回收。

## `Any`

```go
func Any(key string, val any) LogField
```

[实现](../../internal/core/log.go#L97) · [行为专题](../config.md)：Any 创建任意类型值的日志字段。

**源码说明与使用片段**（原文）：

Any 创建任意类型值的日志字段。

参数：

  - key：字段名
  - val：任意类型的字段值

使用示例：

	core.LogCtxDebug(ctx, "请求详情", core.Any("headers", reqHeaders), core.Any("body", reqBody))


**参数说明**：

- `key` (`string`)：原样保存的日志字段名，空字符串也接受；不校验、不去重，也不自动加前缀。应使用稳定、无敏感信息的业务字段名。
- `val` (`any`)：原样保存的任意值，包括 nil 和类型化 nil；不调用 String/Error，不序列化或深拷贝，引用内容仍共享。

**返回值签名**：`LogField`。

**使用示例**（已初始化参数的调用片段）：

```go
core.Any(key, val)
```

**生产建议**：只记录必要且已脱敏的数据；按后端消费时点管理引用生命周期。后端异步保存字段时，应在提交前或后端接收时制作业务需要的快照，并控制大对象/字节数组的保留开销。

**注意事项**：只构造 LogField，不调用 logger、不分配执行器、不格式化或校验内容。Err 固定使用 error 键，其余键原样保存且允许空/重复键；Any/Bytes/Err 内部引用不复制。构造完成后再修改共享对象会影响后端读取，跨 goroutine 使用须同步；nil 字段如何编码由后端决定。

## `Bytes`

```go
func Bytes(key string, val []byte) LogField
```

[实现](../../internal/core/log.go#L108) · [行为专题](../config.md)：Bytes 创建字节切片类型的日志字段。

**源码说明与使用片段**（原文）：

Bytes 创建字节切片类型的日志字段。

参数：

  - key：字段名
  - val：字节切片值

使用示例：

	core.LogCtxError(ctx, "panic 调用栈", core.Bytes("stack", debug.Stack()))


**参数说明**：

- `key` (`string`)：原样保存的日志字段名，空字符串也接受；不校验、不去重，也不自动加前缀。应使用稳定、无敏感信息的业务字段名。
- `val` (`[]byte`)：原样保存的 []byte，包括 nil；不复制底层数组、不做字符串或 base64 转换，后续修改与字段共享同一数据。

**返回值签名**：`LogField`。

**使用示例**（已初始化参数的调用片段）：

```go
core.Bytes(key, val)
```

**生产建议**：只记录必要且已脱敏的数据；按后端消费时点管理引用生命周期。后端异步保存字段时，应在提交前或后端接收时制作业务需要的快照，并控制大对象/字节数组的保留开销。

**注意事项**：只构造 LogField，不调用 logger、不分配执行器、不格式化或校验内容。Err 固定使用 error 键，其余键原样保存且允许空/重复键；Any/Bytes/Err 内部引用不复制。构造完成后再修改共享对象会影响后端读取，跨 goroutine 使用须同步；nil 字段如何编码由后端决定。

## `CPU`

```go
func CPU() int
```

[实现](../../internal/core/core.go#L666) · [行为专题](../config.md)：CPU 返回 CPU 密集型并发度，等于 runtime.NumCPU()。

**源码说明与使用片段**（原文）：

CPU 返回 CPU 密集型并发度，等于 runtime.NumCPU()。 适用于计算密集型任务，建议每个 CPU 核心分配一个 goroutine。

使用示例：

	// 8 核机器返回 8
	async.Pool[int]().Worker(async.CPU()).Run(fn)


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
core.CPU()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。

## `CopyLogger`

```go
func CopyLogger(src, dst context.Context) context.Context
```

[实现](../../internal/core/local_logger.go#L25) · [行为专题](../config.md)：CopyLogger carries only the local logging setting to another context.

**源码说明与使用片段**（原文）：

CopyLogger carries only the local logging setting to another context. Cancellation, deadlines and business values remain those of dst.


**参数说明**：

- `src` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `dst` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
core.CopyLogger(src, dst)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：取消和超时依靠回调协作，不能强制结束忽略 context 的业务函数；配置与等待的作用范围见专题。

## `DefaultAutoScaleConfig`

```go
func DefaultAutoScaleConfig() *AutoScaleConfig
```

[实现](../../internal/core/autoscale.go#L46) · [行为专题](../config.md)：DefaultAutoScaleConfig 返回默认自动扩缩容配置；生产容量需按业务负载验证：

**源码说明与使用片段**（原文）：

DefaultAutoScaleConfig 返回默认自动扩缩容配置；生产容量需按业务负载验证：

	MinWorkers = CPU*2
	MaxWorkers = CPU*100
	CheckInterval = 5s
	ScaleUpThreshold = 0.7 (70% busy)
	ScaleDownThreshold = 0.2 (20% busy)
	ScaleUpChecks = 3 (连续3次才扩容)
	ScaleDownChecks = 5 (连续5次才缩容)
	ScaleUpFactor = 1.5 (每次扩容增加 50%)
	ScaleDownFactor = 0.75 (每次缩容保留 75%)


**参数说明**：无显式参数。

**返回值签名**：`*AutoScaleConfig`。

**使用示例**（已初始化参数的调用片段）：

```go
core.DefaultAutoScaleConfig()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：每次返回独立配置指针，数值是起点建议，不根据业务动态测算。修改后需校验 worker 上下界、阈值及扩缩倍数，Normalize 只补齐非正值；通过构建器保存指针时须在终端创建前完成配置。

## `Dur`

```go
func Dur(key string, d time.Duration) LogField
```

[实现](../../internal/core/log.go#L86) · [行为专题](../config.md)：Dur 创建时间段类型的日志字段。

**源码说明与使用片段**（原文）：

Dur 创建时间段类型的日志字段。

参数：

  - key：字段名
  - d：时间间隔

使用示例：

	core.LogCtxInfo(ctx, "处理完成", core.Dur("elapsed", time.Since(start)))


**参数说明**：

- `key` (`string`)：原样保存的日志字段名，空字符串也接受；不校验、不去重，也不自动加前缀。应使用稳定、无敏感信息的业务字段名。
- `d` (`time.Duration`)：原样保存为 time.Duration 的时长，负值和零值也接受；不换算单位或调用 String，实际编码由日志后端决定。

**返回值签名**：`LogField`。

**使用示例**（已初始化参数的调用片段）：

```go
core.Dur(key, d)
```

**生产建议**：使用稳定字段名、明确数值和时长单位，再传给 LogCtx/Log 等输出入口。值不会自动脱敏，避免放入凭证或完整用户数据；编码格式在后端统一配置。

**注意事项**：只构造 LogField，不调用 logger、不分配执行器、不格式化或校验内容。Err 固定使用 error 键，其余键原样保存且允许空/重复键；Any/Bytes/Err 内部引用不复制。构造完成后再修改共享对象会影响后端读取，跨 goroutine 使用须同步；nil 字段如何编码由后端决定。

## `EnsureTraceID`

```go
func EnsureTraceID(ctx context.Context) context.Context
```

[实现](../../internal/core/core.go#L792) · [行为专题](../config.md)：EnsureTraceID 保留当前 key 下的已有非 nil 值；缺失时自动生成 trace_id。

**源码说明与使用片段**（原文）：

EnsureTraceID 保留当前 key 下的已有非 nil 值；缺失时自动生成 trace\_id。 只有缺失值生成路径在启用 TraceLog 时注入 logger 上下文；不修复空字符串或非 string 值。

参数：

  - ctx：原始上下文

使用示例：

	ctx := core.EnsureTraceID(context.Background())
	// 后续所有使用该 ctx 的操作都会携带相同的 trace_id
	traceID := core.GetTraceID(ctx) // 如 "a1b2c3d4e5f6..."


**参数说明**：

- `ctx` (`context.Context`)：原始上下文

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
core.EnsureTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：传非 nil context。EnsureTraceID 仅检查当前 key 的值是否为 nil，非 nil 时原样返回，包括空字符串和非 string；它不修复类型。WithTraceID 的空 id 委托 EnsureTraceID，保留已有值；非空 id 才覆盖。启用 TraceLog 时，仅生成/显式覆盖路径调用 logger.With/WithContext，后端必须返回有效 context。GetTraceID 对非 string 值会 panic。自定义 key 在初始化时固定，已有 context 的旧值不会自动迁移；继续传递返回 context。

## `Err`

```go
func Err(err error) LogField
```

[实现](../../internal/core/log.go#L75) · [行为专题](../config.md)：Err 创建错误类型的日志字段，key 固定为 "error"。

**源码说明与使用片段**（原文）：

Err 创建错误类型的日志字段，key 固定为 "error"。

参数：

  - err：错误对象

使用示例：

	core.LogCtxError(ctx, "任务失败", core.Err(err))


**参数说明**：

- `err` (`error`)：原样保存的 error，nil 和类型化 nil 也接受；字段名固定为 error。不调用 Error、不解包、不输出日志，也不判断任务是否失败。

**返回值签名**：`LogField`。

**使用示例**（已初始化参数的调用片段）：

```go
core.Err(err)
```

**生产建议**：先按业务判断 err，再将字段传给合适级别的日志入口；Err(nil) 仍会生成字段，不代表需要输出失败日志。异步后端需要稳定错误内容时，应保存快照，并对错误消息中的敏感信息脱敏。

**注意事项**：只构造 LogField，不调用 logger、不分配执行器、不格式化或校验内容。Err 固定使用 error 键，其余键原样保存且允许空/重复键；Any/Bytes/Err 内部引用不复制。构造完成后再修改共享对象会影响后端读取，跨 goroutine 使用须同步；nil 字段如何编码由后端决定。

## `GetDefaultMaxResults`

```go
func GetDefaultMaxResults() int
```

[实现](../../internal/core/core.go#L184) · [行为专题](../config.md)：GetDefaultMaxResults 返回当前全局默认最大结果数，默认 100_000，0 表示无限制。

**源码说明与使用片段**（原文）：

GetDefaultMaxResults 返回当前全局默认最大结果数，默认 100\_000，0 表示无限制。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetDefaultMaxResults()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetDefaultOverflowStrategy`

```go
func GetDefaultOverflowStrategy() OverflowStrategy
```

[实现](../../internal/core/core.go#L212) · [行为专题](../config.md)：GetDefaultOverflowStrategy 返回当前全局默认环形缓冲区溢出策略。

**源码说明与使用片段**（原文）：

GetDefaultOverflowStrategy 返回当前全局默认环形缓冲区溢出策略。


**参数说明**：无显式参数。

**返回值签名**：`OverflowStrategy`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetDefaultOverflowStrategy()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetDefaultRingBufferCap`

```go
func GetDefaultRingBufferCap() int
```

[实现](../../internal/core/core.go#L207) · [行为专题](../config.md)：GetDefaultRingBufferCap 返回当前全局默认环形缓冲区容量，0 表示不启用。

**源码说明与使用片段**（原文）：

GetDefaultRingBufferCap 返回当前全局默认环形缓冲区容量，0 表示不启用。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetDefaultRingBufferCap()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetDefaultTimeout`

```go
func GetDefaultTimeout() time.Duration
```

[实现](../../internal/core/core.go#L133) · [行为专题](../config.md)：GetDefaultTimeout 返回当前全局默认超时时间，0 表示已关闭默认超时。

**源码说明与使用片段**（原文）：

GetDefaultTimeout 返回当前全局默认超时时间，0 表示已关闭默认超时。


**参数说明**：无显式参数。

**返回值签名**：`time.Duration`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetDefaultTimeout()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetLogger`

```go
func GetLogger() Logger
```

[实现](../../internal/core/log.go#L178) · [行为专题](../config.md)：GetLogger 返回当前日志器。

**源码说明与使用片段**（原文）：

GetLogger 返回当前日志器。


**参数说明**：无显式参数。

**返回值签名**：`Logger`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetLogger()
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `GetMaxCleanupDuration`

```go
func GetMaxCleanupDuration() time.Duration
```

[实现](../../internal/core/core.go#L110) · [行为专题](../config.md)：GetMaxCleanupDuration 返回设置的原始值；非正值由辅助函数按 30 分钟处理。

**源码说明与使用片段**（原文）：

GetMaxCleanupDuration 返回设置的原始值；非正值由辅助函数按 30 分钟处理。


**参数说明**：无显式参数。

**返回值签名**：`time.Duration`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetMaxCleanupDuration()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetSubmitTimeout`

```go
func GetSubmitTimeout() time.Duration
```

[实现](../../internal/core/core.go#L159) · [行为专题](../config.md)：GetSubmitTimeout 返回当前保存的全局提交超时配置值。

**源码说明与使用片段**（原文）：

GetSubmitTimeout 返回当前保存的全局提交超时配置值。


**参数说明**：无显式参数。

**返回值签名**：`time.Duration`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetSubmitTimeout()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetSubmitTimeoutValue`

```go
func GetSubmitTimeoutValue() time.Duration
```

[实现](../../internal/core/core.go#L154) · [行为专题](../config.md)：GetSubmitTimeoutValue 返回当前提交超时（内部使用，区分包级别和池级别）。

**源码说明与使用片段**（原文）：

GetSubmitTimeoutValue 返回当前提交超时（内部使用，区分包级别和池级别）。


**参数说明**：无显式参数。

**返回值签名**：`time.Duration`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetSubmitTimeoutValue()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `GetTaskFailLogLevel`

```go
func GetTaskFailLogLevel() TaskLogLevel
```

[实现](../../internal/core/core.go#L297) · [行为专题](../config.md)：GetTaskFailLogLevel 返回当前任务失败日志级别。

**源码说明与使用片段**（原文）：

GetTaskFailLogLevel 返回当前任务失败日志级别。


**参数说明**：无显式参数。

**返回值签名**：`TaskLogLevel`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetTaskFailLogLevel()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：读取当前全局配置，不修改实例；并发读取同步，但先读再设置或连续读取多个字段不是原子事务。trace key 若运行中更换，已有 context 的旧值不会迁移；全局日志设置影响未单独配置日志器的后续调用。

## `GetTraceID`

```go
func GetTraceID(ctx context.Context) string
```

[实现](../../internal/core/core.go#L809) · [行为专题](../config.md)：GetTraceID 从 ctx 中提取 trace_id，不存在则返回空字符串。

**源码说明与使用片段**（原文）：

GetTraceID 从 ctx 中提取 trace\_id，不存在则返回空字符串。

参数：

  - ctx：上下文


**参数说明**：

- `ctx` (`context.Context`)：上下文

**返回值签名**：`string`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetTraceID(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：传非 nil context。EnsureTraceID 仅检查当前 key 的值是否为 nil，非 nil 时原样返回，包括空字符串和非 string；它不修复类型。WithTraceID 的空 id 委托 EnsureTraceID，保留已有值；非空 id 才覆盖。启用 TraceLog 时，仅生成/显式覆盖路径调用 logger.With/WithContext，后端必须返回有效 context。GetTraceID 对非 string 值会 panic。自定义 key 在初始化时固定，已有 context 的旧值不会自动迁移；继续传递返回 context。

## `GetTraceIDKey`

```go
func GetTraceIDKey() interface{}
```

[实现](../../internal/core/core.go#L777) · [行为专题](../config.md)：GetTraceIDKey 返回当前生效的 trace_id context key。

**源码说明与使用片段**（原文）：

GetTraceIDKey 返回当前生效的 trace\_id context key。


**参数说明**：无显式参数。

**返回值签名**：`interface{}`。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetTraceIDKey()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：读取当前全局配置，不修改实例；并发读取同步，但先读再设置或连续读取多个字段不是原子事务。trace key 若运行中更换，已有 context 的旧值不会迁移；全局日志设置影响未单独配置日志器的后续调用。

## `GetTraceLogEnabled`

```go
func GetTraceLogEnabled() bool
```

[实现](../../internal/core/core.go#L322) · [行为专题](../config.md)：GetTraceLogEnabled 返回 Trace 日志是否启用。

**源码说明与使用片段**（原文）：

GetTraceLogEnabled 返回 Trace 日志是否启用。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
core.GetTraceLogEnabled()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：读取当前全局配置，不修改实例；并发读取同步，但先读再设置或连续读取多个字段不是原子事务。trace key 若运行中更换，已有 context 的旧值不会迁移；全局日志设置影响未单独配置日志器的后续调用。

## `IO`

```go
func IO() int
```

[实现](../../internal/core/core.go#L685) · [行为专题](../config.md)：IO 返回 IO 密集型并发度，等于 runtime.NumCPU() * 2。

**源码说明与使用片段**（原文）：

IO 返回 IO 密集型并发度，等于 runtime.NumCPU() \* 2。 适用于网络请求、文件读写等 IO 密集型任务，默认倍数为 2。

使用示例：

	// 8 核机器返回 16
	err := async.Pool[string]().Worker(async.IO()).Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
	    for _, url := range urls {
	        if err := p.Submit(ctx, func(ctx context.Context) (string, error) {
	            return httpGet(ctx, url)
	        }); err != nil { return err }
	    }
	    p.Wait()
	    return p.FirstError()
	})


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
core.IO()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。

## `IOMulti`

```go
func IOMulti(multiplier int) int
```

[实现](../../internal/core/core.go#L698) · [行为专题](../config.md)：IOMulti 返回自定义倍数的 IO 密集型并发度，等于 runtime.NumCPU() * multiplier。

**源码说明与使用片段**（原文）：

IOMulti 返回自定义倍数的 IO 密集型并发度，等于 runtime.NumCPU() \* multiplier。

参数：

  - multiplier：CPU 核数的倍数，\<= 0 时默认使用 2

使用示例：

	// 8 核机器，倍数 4，返回 32（高并发 RPC 场景）
	async.Pool[string]().Worker(async.IOMulti(4)).Run(fn)


**参数说明**：

- `multiplier` (`int`)：逻辑 CPU 数的乘数；非正值使用 2，返回 NumCPU×有效倍数，与 GOMAXPROCS 无关。避免超大倍数造成整数溢出或不可承受的 worker 数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
core.IOMulti(multiplier)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。

## `InheritLogger`

```go
func InheritLogger(src, dst context.Context) context.Context
```

[实现](../../internal/core/local_logger.go#L36) · [行为专题](../config.md)：InheritLogger uses an instance logger only when the submitted work has no

**源码说明与使用片段**（原文）：

InheritLogger uses an instance logger only when the submitted work has no explicit scope of its own (for example work delegated by another builder).


**参数说明**：

- `src` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `dst` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
core.InheritLogger(src, dst)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：取消和超时依靠回调协作，不能强制结束忽略 context 的业务函数；配置与等待的作用范围见专题。

## `Int64`

```go
func Int64(key string, val int64) LogField
```

[实现](../../internal/core/log.go#L119) · [行为专题](../config.md)：Int64 创建 int64 类型的日志字段。

**源码说明与使用片段**（原文）：

Int64 创建 int64 类型的日志字段。

参数：

  - key：字段名
  - val：int64 值

使用示例：

	core.LogCtxInfo(ctx, "统计", core.Int64("count", 100), core.Int64("concurrency", 8))


**参数说明**：

- `key` (`string`)：原样保存的日志字段名，空字符串也接受；不校验、不去重，也不自动加前缀。应使用稳定、无敏感信息的业务字段名。
- `val` (`int64`)：原样保存的 int64 字段值；不转为字符串、不校验正负或业务单位。

**返回值签名**：`LogField`。

**使用示例**（已初始化参数的调用片段）：

```go
core.Int64(key, val)
```

**生产建议**：使用稳定字段名、明确数值和时长单位，再传给 LogCtx/Log 等输出入口。值不会自动脱敏，避免放入凭证或完整用户数据；编码格式在后端统一配置。

**注意事项**：只构造 LogField，不调用 logger、不分配执行器、不格式化或校验内容。Err 固定使用 error 键，其余键原样保存且允许空/重复键；Any/Bytes/Err 内部引用不复制。构造完成后再修改共享对象会影响后端读取，跨 goroutine 使用须同步；nil 字段如何编码由后端决定。

## `LogCtxDebug`

```go
func LogCtxDebug(ctx context.Context, msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L263) · [行为专题](../config.md)：LogCtxDebug 输出 Debug 级别日志，自动附带 ctx 中的 trace_id。

**源码说明与使用片段**（原文）：

LogCtxDebug 输出 Debug 级别日志，自动附带 ctx 中的 trace\_id。

参数：

  - ctx：上下文
  - msg：日志消息
  - fields：可选的日志字段

使用示例：

	core.LogCtxDebug(ctx, "请求参数", core.Any("params", params))


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogCtxDebug(ctx, msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：同步调用 ctx 中的局部 logger，未设置或已清除覆盖时继承全局 logger。nil ctx、缺失/空/非 string TraceID 不附加字段；非空 string 自动作为第一个 trace_id 字段，调用者原字段随后追加，不去重。TraceLogEnabled 和 TaskFailLogLevel 都不过滤这些直接日志入口；后端 panic 向外传播，异步保存引用字段需自行复制。

## `LogCtxError`

```go
func LogCtxError(ctx context.Context, msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L221) · [行为专题](../config.md)：LogCtxError 输出 Error 级别日志，自动附带 ctx 中的 trace_id。

**源码说明与使用片段**（原文）：

LogCtxError 输出 Error 级别日志，自动附带 ctx 中的 trace\_id。

参数：

  - ctx：上下文（用于获取 trace\_id）
  - msg：日志消息
  - fields：可选的日志字段

使用示例：

	core.LogCtxError(ctx, "数据库查询失败", core.Err(err), core.Str("sql", query))


**参数说明**：

- `ctx` (`context.Context`)：上下文（用于获取 trace_id）
- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogCtxError(ctx, msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：同步调用 ctx 中的局部 logger，未设置或已清除覆盖时继承全局 logger。nil ctx、缺失/空/非 string TraceID 不附加字段；非空 string 自动作为第一个 trace_id 字段，调用者原字段随后追加，不去重。TraceLogEnabled 和 TaskFailLogLevel 都不过滤这些直接日志入口；后端 panic 向外传播，异步保存引用字段需自行复制。

## `LogCtxInfo`

```go
func LogCtxInfo(ctx context.Context, msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L249) · [行为专题](../config.md)：LogCtxInfo 输出 Info 级别日志，自动附带 ctx 中的 trace_id。

**源码说明与使用片段**（原文）：

LogCtxInfo 输出 Info 级别日志，自动附带 ctx 中的 trace\_id。

参数：

  - ctx：上下文
  - msg：日志消息
  - fields：可选的日志字段

使用示例：

	core.LogCtxInfo(ctx, "任务完成", core.Int64("total", 100))


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogCtxInfo(ctx, msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：同步调用 ctx 中的局部 logger，未设置或已清除覆盖时继承全局 logger。nil ctx、缺失/空/非 string TraceID 不附加字段；非空 string 自动作为第一个 trace_id 字段，调用者原字段随后追加，不去重。TraceLogEnabled 和 TaskFailLogLevel 都不过滤这些直接日志入口；后端 panic 向外传播，异步保存引用字段需自行复制。

## `LogCtxWarn`

```go
func LogCtxWarn(ctx context.Context, msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L235) · [行为专题](../config.md)：LogCtxWarn 输出 Warn 级别日志，自动附带 ctx 中的 trace_id。

**源码说明与使用片段**（原文）：

LogCtxWarn 输出 Warn 级别日志，自动附带 ctx 中的 trace\_id。

参数：

  - ctx：上下文
  - msg：日志消息
  - fields：可选的日志字段

使用示例：

	core.LogCtxWarn(ctx, "响应时间较长", core.Dur("elapsed", elapsed))


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogCtxWarn(ctx, msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：同步调用 ctx 中的局部 logger，未设置或已清除覆盖时继承全局 logger。nil ctx、缺失/空/非 string TraceID 不附加字段；非空 string 自动作为第一个 trace_id 字段，调用者原字段随后追加，不去重。TraceLogEnabled 和 TaskFailLogLevel 都不过滤这些直接日志入口；后端 panic 向外传播，异步保存引用字段需自行复制。

## `LogDebug`

```go
func LogDebug(msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L303) · [行为专题](../config.md)：LogDebug 输出全局 Debug 级别日志（不使用 ctx）。

**源码说明与使用片段**（原文）：

LogDebug 输出全局 Debug 级别日志（不使用 ctx）。

参数：

  - msg：日志消息
  - fields：可选的日志字段


**参数说明**：

- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogDebug(msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `LogError`

```go
func LogError(msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L276) · [行为专题](../config.md)：LogError 输出全局 Error 级别日志（不使用 ctx）。

**源码说明与使用片段**（原文）：

LogError 输出全局 Error 级别日志（不使用 ctx）。

参数：

  - msg：日志消息
  - fields：可选的日志字段

使用示例：

	core.LogError("服务启动失败", core.Err(err))


**参数说明**：

- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogError(msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `LogFatal`

```go
func LogFatal(msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L317) · [行为专题](../config.md)：LogFatal 输出 Error 日志后调用 os.Exit(1) 终止进程。

**源码说明与使用片段**（原文）：

LogFatal 输出 Error 日志后调用 os.Exit(1) 终止进程。 仅在无法恢复的致命错误时使用。

参数：

  - msg：日志消息
  - fields：可选的日志字段

使用示例：

	core.LogFatal("无法连接数据库", core.Err(err))


**参数说明**：

- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogFatal(msg, fields...)
```

**生产建议**：仅在应用最外层确需立即结束进程时使用；先完成必须的资源清理与日志落盘。后端异步写日志时应自行保证致命消息已落盘，os.Exit 不等待后台队列或执行 defer。

**注意事项**：同步调用当前全局 logger，使用 Background context 和 Error 级别，然后 os.Exit(1)；不附加 trace_id，不受 TaskFailLogLevel/TraceLogEnabled 过滤，进程退出不执行 defer。后端阻塞会延迟退出；后端 panic/Goexit 则不会到达 os.Exit，不保证异步后端落盘。

## `LogInfo`

```go
func LogInfo(msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L294) · [行为专题](../config.md)：LogInfo 输出全局 Info 级别日志（不使用 ctx）。

**源码说明与使用片段**（原文）：

LogInfo 输出全局 Info 级别日志（不使用 ctx）。

参数：

  - msg：日志消息
  - fields：可选的日志字段


**参数说明**：

- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogInfo(msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `LogTaskFail`

```go
func LogTaskFail(ctx context.Context, err error, msg string)
```

[实现](../../internal/core/core.go#L332) · [行为专题](../config.md)：LogTaskFail 记录任务失败日志（使用当前全局日志级别）。

**源码说明与使用片段**（原文）：

LogTaskFail 记录任务失败日志（使用当前全局日志级别）。

参数：

  - ctx：上下文（用于获取 trace\_id）
  - err：失败错误
  - msg：日志消息


**参数说明**：

- `ctx` (`context.Context`)：上下文（用于获取 trace_id）
- `err` (`error`)：失败错误
- `msg` (`string`)：日志消息

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogTaskFail(ctx, err, msg)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：只有 TaskFailLogLevel 的 Error/Warn/Info/Debug 四值会输出，Silent 和范围外值不输出；该过滤不作用于普通日志入口。LogTaskFail 即使 err 为 nil 也照常构造 error 字段，不代替业务错误判断。同步调用 context 中的局部 logger，未设置时继承全局 logger；自动 trace 字段在调用者字段前，不去重；后端须并发安全、快速返回且不 panic。

## `LogTaskFailCtx`

```go
func LogTaskFailCtx(ctx context.Context, msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L328) · [行为专题](../config.md)：LogTaskFailCtx 根据当前 TaskFailLogLevel 配置输出任务失败日志。

**源码说明与使用片段**（原文）：

LogTaskFailCtx 根据当前 TaskFailLogLevel 配置输出任务失败日志。 如果级别设置为 LogLevelSilent，则不输出任何日志。

参数：

  - ctx：上下文
  - msg：日志消息
  - fields：可选的日志字段


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogTaskFailCtx(ctx, msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：只有 TaskFailLogLevel 的 Error/Warn/Info/Debug 四值会输出，Silent 和范围外值不输出；该过滤不作用于普通日志入口。LogTaskFail 即使 err 为 nil 也照常构造 error 字段，不代替业务错误判断。同步调用 context 中的局部 logger，未设置时继承全局 logger；自动 trace 字段在调用者字段前，不去重；后端须并发安全、快速返回且不 panic。

## `LogWarn`

```go
func LogWarn(msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L285) · [行为专题](../config.md)：LogWarn 输出全局 Warn 级别日志（不使用 ctx）。

**源码说明与使用片段**（原文）：

LogWarn 输出全局 Warn 级别日志（不使用 ctx）。

参数：

  - msg：日志消息
  - fields：可选的日志字段


**参数说明**：

- `msg` (`string`)：日志消息
- `fields` (`...LogField`)：可选的日志字段

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.LogWarn(msg, fields...)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `LoggerForContext`

```go
func LoggerForContext(ctx context.Context) Logger
```

[实现](../../internal/core/local_logger.go#L14) · [行为专题](../config.md)：LoggerForContext resolves a local logger before falling back to the global.

**源码说明与使用片段**（原文）：

LoggerForContext resolves a local logger before falling back to the global.


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`Logger`。

**使用示例**（已初始化参数的调用片段）：

```go
core.LoggerForContext(ctx)
```

**生产建议**：按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `MergeCancel`

```go
func MergeCancel(oldCancel, newCancel context.CancelFunc) context.CancelFunc
```

[实现](../../internal/core/core.go#L448) · [行为专题](../config.md)：MergeCancel 合并两个 CancelFunc，调用时依次执行新旧 cancel。

**源码说明与使用片段**（原文）：

MergeCancel 合并两个 CancelFunc，调用时依次执行新旧 cancel。

参数：

  - oldCancel：旧的 CancelFunc（可能为 nil）
  - newCancel：新的 CancelFunc


**参数说明**：

- `oldCancel` (`context.CancelFunc`)：原有取消函数，可为 nil；非 nil 时返回的闭包先调用 newCancel 再调用它。
- `newCancel` (`context.CancelFunc`)：新增取消函数；oldCancel 非 nil 时它也必须非 nil，否则调用组合函数时 panic。oldCancel 为 nil 时直接返回此值（包括 nil）。

**返回值签名**：`context.CancelFunc`。

**使用示例**（已初始化参数的调用片段）：

```go
core.MergeCancel(oldCancel, newCancel)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：oldCancel 非 nil 时返回闭包，先执行 newCancel 再执行 oldCancel；此时 newCancel 必须非 nil。oldCancel 为 nil 则直接返回 newCancel（可为 nil）。同步执行，不添加 once、不恢复 panic；newCancel panic/Goexit 时 oldCancel 不执行，oldCancel 异常时 newCancel 已执行。重复/并发调用是否安全取决于注入函数；context 原生取消函数支持重复调用。

## `NewPanicError`

```go
func NewPanicError(r any) *PanicError
```

[实现](../../internal/core/core.go#L434) · [行为专题](../config.md)：NewPanicError 创建 PanicError，捕获 panic 值和当前调用栈。

**源码说明与使用片段**（原文）：

NewPanicError 创建 PanicError，捕获 panic 值和当前调用栈。

参数：

  - r：recover() 捕获的 panic 原始值


**参数说明**：

- `r` (`any`)：recover() 捕获的 panic 原始值

**返回值签名**：`*PanicError`。

**使用示例**（已初始化参数的调用片段）：

```go
core.NewPanicError(r)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：NewPanicError 捕获创建处当前 goroutine 的栈，保留原 Value 引用，不深拷贝；Value/Stack 是可变公开字段，后续格式化读取当前值，共享修改须同步。Unwrap 始终返回 nil，即使 Value 本身为 error 也不加入错误链。Format 对各动词均写出 panic: 加原值，仅 'v' 且有 '+' 时追加换行和当前 Stack；忽略宽度/精度，不为 %q 加引号或转义。需转义先 Error() 再格式化字符串，生产中仅向受控日志输出栈；Result 的 %+v 不转发此标记。

## `NewRingBuffer`

```go
func NewRingBuffer[T any](capacity int, overflow OverflowStrategy) *RingBuffer[T]
```

[实现](../../internal/core/ringbuffer.go#L22) · [行为专题](../config.md)：NewRingBuffer 创建指定容量的环形缓冲区。

**源码说明与使用片段**（原文）：

NewRingBuffer 创建指定容量的环形缓冲区。


**参数说明**：

- `capacity` (`int`)：固定缓冲容量；小于 1 时使用 1024。满时按 overflow 处理，之后不会自动扩容。
- `overflow` (`OverflowStrategy`)：达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。

**返回值签名**：`*RingBuffer[T]`。

**使用示例**（已初始化参数的调用片段）：

```go
core.NewRingBuffer[T](capacity, overflow)
```

**生产建议**：按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。

**注意事项**：使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。

## `NewTraceID`

```go
func NewTraceID() string
```

[实现](../../internal/core/core.go#L846) · [行为专题](../config.md)：NewTraceID 生成新的随机 trace_id（32 位十六进制字符串）。

**源码说明与使用片段**（原文）：

NewTraceID 生成新的随机 trace\_id（32 位十六进制字符串）。 使用 crypto/rand 生成，失败时回退到时间戳+计数器方案。

使用示例：

	traceID := core.NewTraceID() // 如 "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"


**参数说明**：无显式参数。

**返回值签名**：`string`。

**使用示例**（已初始化参数的调用片段）：

```go
core.NewTraceID()
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：输出为 32 位十六进制，正常使用 crypto/rand；实现保留随机读取失败后使用时间戳和进程内计数器的回退分支。用于日志关联，不承诺绝对唯一，也不作为认证令牌。

## `SafeCall`

```go
func SafeCall[T any, R any](ctx context.Context, item T, fn func(ctx context.Context, item T) (R, error)) (val R, err error)
```

[实现](../../internal/core/core.go#L878) · [行为专题](../config.md)：SafeCall 安全调用 fn，自动捕获 panic 并包装为 PanicError。

**源码说明与使用片段**（原文）：

SafeCall 安全调用 fn，自动捕获 panic 并包装为 PanicError。 用于串行执行路径的 panic 保护。

参数：

  - ctx：上下文
  - item：传递给 fn 的参数
  - fn：要安全执行的函数，接收 ctx 和 item

使用示例：

	// 安全的串行调用
	result, err := core.SafeCall(ctx, item, func(ctx context.Context, item MyType) (MyResult, error) {
	    return doSomething(ctx, item)
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `item` (`T`)：传递给 fn 的参数
- `fn` (`func(ctx context.Context, item T) (R, error)`)：同步接收 context 和输入项的业务函数；返回签名中的值和错误。panic 被恢复并包装为 PanicError；不设置超时、不另启 goroutine，回调须自行响应 context。

**返回值签名**：`val R, err error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
core.SafeCall[T, R](ctx, item, fn)
```

**生产建议**：在调用方管理回调耗时和下游 context，检查返回 error 并用 errors.As 区分 PanicError；需要异步执行时选用 Task 等入口。日志后端须支持并发调用且不 panic，业务自建 goroutine 由业务自己等待和恢复。

**注意事项**：在调用方同步执行一次回调，不启动 goroutine、不检查 context 取消，也不设置期限；nil 或已取消 context 均原样传入。普通返回的值和错误原样保留，非 nil 错误可以伴随非零值；业务或 nil 函数调用的 panic 包装为 PanicError 并记录日志，有值版本 panic 时返回零值。只捕获同一 goroutine 的 panic，业务自建 goroutine 的 panic 不在范围内；runtime.Goexit 仍执行 defer 并结束当前 goroutine，不转换成错误返回。日志后端若 panic 仍会向外传播。

## `SafeCallVoid`

```go
func SafeCallVoid[T any](ctx context.Context, item T, fn func(ctx context.Context, item T) error) (err error)
```

[实现](../../internal/core/core.go#L902) · [行为专题](../config.md)：SafeCallVoid 安全调用无返回值的 fn，自动捕获 panic。

**源码说明与使用片段**（原文）：

SafeCallVoid 安全调用无返回值的 fn，自动捕获 panic。

参数：

  - ctx：上下文
  - item：传递给 fn 的参数
  - fn：要安全执行的函数，接收 ctx 和 item，只返回 error

使用示例：

	// 安全的串行调用（无返回值）
	err := core.SafeCallVoid(ctx, item, func(ctx context.Context, item MyType) error {
	    return doSomething(ctx, item)
	})


**参数说明**：

- `ctx` (`context.Context`)：上下文
- `item` (`T`)：传递给 fn 的参数
- `fn` (`func(ctx context.Context, item T) error`)：同步接收 context 和输入项的业务函数；返回签名中的值和错误。panic 被恢复并包装为 PanicError；不设置超时、不另启 goroutine，回调须自行响应 context。

**返回值签名**：`err error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
core.SafeCallVoid[T](ctx, item, fn)
```

**生产建议**：在调用方管理回调耗时和下游 context，检查返回 error 并用 errors.As 区分 PanicError；需要异步执行时选用 Task 等入口。日志后端须支持并发调用且不 panic，业务自建 goroutine 由业务自己等待和恢复。

**注意事项**：在调用方同步执行一次回调，不启动 goroutine、不检查 context 取消，也不设置期限；nil 或已取消 context 均原样传入。普通返回的值和错误原样保留，非 nil 错误可以伴随非零值；业务或 nil 函数调用的 panic 包装为 PanicError 并记录日志，有值版本 panic 时返回零值。只捕获同一 goroutine 的 panic，业务自建 goroutine 的 panic 不在范围内；runtime.Goexit 仍执行 defer 并结束当前 goroutine，不转换成错误返回。日志后端若 panic 仍会向外传播。

## `SetDefaultMaxResults`

```go
func SetDefaultMaxResults(n int)
```

[实现](../../internal/core/core.go#L177) · [行为专题](../config.md)：SetDefaultMaxResults 设置全局默认最大结果数，影响后续创建的 Pool。

**源码说明与使用片段**（原文）：

SetDefaultMaxResults 设置全局默认最大结果数，影响后续创建的 Pool。 当 Pool 未通过 WithMaxResults 单独设置时，使用此全局默认值。 默认值为 100\_000，设为 0 可恢复无限制（向后兼容）。 长期运行的 Pool 建议设置此值防止 results 切片无界增长导致 OOM。

参数：

  - n：全局默认最大结果数，0 表示无限制

使用示例：

	core.SetDefaultMaxResults(10000) // 后续创建的池默认保留至多 10000 项
	p := pool.NewPool[int](8)
	defer p.Close()
	p.WithMaxResults(0) // 本池覆盖为无限制


**参数说明**：

- `n` (`int`)：后续新建 Pool 的主结果保留上限；0 不限制，正数限制提交索引，负数不更新。已有 Pool、Group 和 Flow 不因此改变。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetDefaultMaxResults(n)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetDefaultRingBuffer`

```go
func SetDefaultRingBuffer(capacity int, overflow OverflowStrategy)
```

[实现](../../internal/core/core.go#L199) · [行为专题](../config.md)：SetDefaultRingBuffer 设置全局默认环形缓冲区配置，影响后续创建的 Pool。

**源码说明与使用片段**（原文）：

SetDefaultRingBuffer 设置全局默认环形缓冲区配置，影响后续创建的 Pool。 当 Pool 未通过 WithRingBuffer 单独设置时，使用此全局默认值。 默认 capacity=0（不启用），保持向后兼容。overflow 默认 OverflowDrop。

参数：

  - capacity：环形缓冲区容量，0 表示不启用
  - overflow：满时策略（OverflowDrop/OverflowBlock/OverflowError）

使用示例：

	core.SetDefaultRingBuffer(5000, core.OverflowDrop)  // 全局兜底


**参数说明**：

- `capacity` (`int`)：后续新建 Pool 的环形缓冲容量；0 关闭默认缓冲，负数使整个配置调用无操作。底层以 int32 保存，生产配置应在 0..MaxInt32 内并按内存预算设置。
- `overflow` (`OverflowStrategy`)：环形缓冲满时的策略；Drop 覆盖最旧结果并计数，Block/Error 拒绝新写入，不阻塞 worker。只有 capacity 非负时同时更新；两项全局值分别存储。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetDefaultRingBuffer(capacity, overflow)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetDefaultTimeout`

```go
func SetDefaultTimeout(d time.Duration)
```

[实现](../../internal/core/core.go#L128) · [行为专题](../config.md)：SetDefaultTimeout 设置全局默认超时时间，影响后续创建的 Group 和 Pool。

**源码说明与使用片段**（原文）：

SetDefaultTimeout 设置全局默认超时时间，影响后续创建的 Group 和 Pool。 当 Group/Pool 未通过 WithTimeout 单独设置时，使用此全局默认值。 设为 0 可关闭默认超时。

参数：

  - d：默认超时时间，0 表示无超时限制

使用示例：

	// 设置全局默认超时为 10 秒
	core.SetDefaultTimeout(10 * time.Second)

	// 关闭默认超时（任务无超时限制）
	core.SetDefaultTimeout(0)


**参数说明**：

- `d` (`time.Duration`)：后续新建 Pool/Group 保存的默认任务超时；正数设置回调 context 期限，非正值不设置期限。不会更新已有实例，TaskBuilder 不读取此全局值。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetDefaultTimeout(d)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetLogger`

```go
func SetLogger(l Logger)
```

[实现](../../internal/core/log.go#L169) · [行为专题](../config.md)：SetLogger 注入自定义日志实现。传入 nil 则恢复为默认空日志实现。

**源码说明与使用片段**（原文）：

SetLogger 注入自定义日志实现。传入 nil 则恢复为默认空日志实现。

使用示例：

	// 注入自定义日志
	core.SetLogger(myZerologAdapter)

	// 恢复静默模式
	core.SetLogger(nil)


**参数说明**：

- `l` (`Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetLogger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `SetMaxCleanupDuration`

```go
func SetMaxCleanupDuration(d time.Duration)
```

[实现](../../internal/core/core.go#L105) · [行为专题](../config.md)：SetMaxCleanupDuration 设置内部等待辅助函数的清理监视时长。

**源码说明与使用片段**（原文）：

SetMaxCleanupDuration 设置内部等待辅助函数的清理监视时长。 非正值在这些辅助函数中按 WaitContextCleanupError 处理；该设置不终止任务。 当前 Pool 和 Group 的等待方法不使用这些辅助函数。

参数：

  - d：监视时长，非正值使用 30 分钟

使用示例：

	// 设置内部等待辅助函数的监视时长为 5 分钟，不终止任务
	core.SetMaxCleanupDuration(5 * time.Minute)


**参数说明**：

- `d` (`time.Duration`)：内部 Wait 辅助函数在提前返回后监视清理的时长，原样保存；非正值在辅助函数中使用 30 分钟。只停止监视，不会终止任务。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetMaxCleanupDuration(d)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetSubmitTimeout`

```go
func SetSubmitTimeout(d time.Duration)
```

[实现](../../internal/core/core.go#L147) · [行为专题](../config.md)：SetSubmitTimeout 设置并保存全局提交超时配置值。当前 Pool/Group 创建路径不自动读取此值；

**源码说明与使用片段**（原文）：

SetSubmitTimeout 设置并保存全局提交超时配置值。当前 Pool/Group 创建路径不自动读取此值； d 必须大于 0，否则不更新。具体实例的提交等待需通过实例或 Builder 显式设置。

参数：

  - d：提交超时时间，必须 > 0

使用示例：

	// 设置提交超时为 3 秒
	core.SetSubmitTimeout(3 * time.Second)


**参数说明**：

- `d` (`time.Duration`)：保存供 GetSubmitTimeout 读取的全局时长，仅正数更新。Pool/Group 不自动读取，限制入队等待需在具体实例或构建器另行配置。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetSubmitTimeout(d)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetTaskFailLogLevel`

```go
func SetTaskFailLogLevel(level TaskLogLevel)
```

[实现](../../internal/core/core.go#L292) · [行为专题](../config.md)：SetTaskFailLogLevel 设置任务失败时的日志级别。

**源码说明与使用片段**（原文）：

SetTaskFailLogLevel 设置任务失败时的日志级别。 默认为 LogLevelError，可设置为 LogLevelSilent 关闭失败日志。

参数：

  - level：日志级别（LogLevelError/Warn/Info/Debug/Silent）

使用示例：

	// 任务失败只打印 Warn 级别日志
	core.SetTaskFailLogLevel(core.LogLevelWarn)

	// 关闭任务失败日志
	core.SetTaskFailLogLevel(core.LogLevelSilent)


**参数说明**：

- `level` (`TaskLogLevel`)：任务失败日志的全局级别；Error/Warn/Info/Debug 分别输出相应级别，Silent 与范围外值均不输出。原值按 int32 保存，不校验枚举范围；普通 Log/LogCtx 不受其过滤。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetTaskFailLogLevel(level)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。

## `SetTraceIDKey`

```go
func SetTraceIDKey(key interface{})
```

[实现](../../internal/core/core.go#L769) · [行为专题](../config.md)：SetTraceIDKey 自定义 trace_id 的 context key。

**源码说明与使用片段**（原文）：

SetTraceIDKey 自定义 trace\_id 的 context key。 设置后将替换默认的 TraceIDKey，EnsureTraceID/GetTraceID/WithTraceID 都会使用新 key。

使用场景：对接已有链路追踪系统（如 trpc、go-zero、自定义中间件）， 这些系统通常使用自己的 key 类型存储 trace\_id。

参数：

  - key：自定义 context key，通常为框架中存储 trace\_id 的 key

使用示例：

	// trpc 场景：对接 trpc 的 trace_id key
	core.SetTraceIDKey(trpc.TraceIDKey)

	// go-zero 场景
	core.SetTraceIDKey(trace.TraceKey)

	// 自定义字符串 key
	core.SetTraceIDKey("X-Trace-Id")

注意：请在最早期（init 或 main 开头）调用，避免并发读写导致的瞬时不一致。 key 必须非 nil 且其值可安全比较；结构体或数组中接口字段的动态值也须可比较。 无效 key 会在更新全局配置前 panic。


**参数说明**：

- `key` (`interface{}`)：自定义 context key，通常为框架中存储 trace_id 的 key

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetTraceIDKey(key)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：key 必须非 nil 且其值可安全比较；结构体或数组中接口字段的动态值也须可比较，否则在更新全局配置前 panic。设置影响全库后续 context 读写；在启动时统一配置，已有 context 不会把旧 key 的值迁移到新 key。

## `SetTraceLogEnabled`

```go
func SetTraceLogEnabled(enabled bool)
```

[实现](../../internal/core/core.go#L313) · [行为专题](../config.md)：SetTraceLogEnabled 启用或禁用 Trace 日志。

**源码说明与使用片段**（原文）：

SetTraceLogEnabled 启用或禁用 Trace 日志。 启用时，EnsureTraceID 生成新值或 WithTraceID 显式覆盖时调用日志上下文注入。 不影响 TraceID 的存储，也不屏蔽 LogCtx 系列自动附加 trace\_id 字段。 默认为启用状态。

参数：

  - enabled：true=启用，false=禁用

使用示例：

	// 禁用 Trace 日志
	core.SetTraceLogEnabled(false)


**参数说明**：

- `enabled` (`bool`)：是否在生成或显式覆盖 TraceID 时调用 logger.With/WithContext；不关闭 TraceID 生成存储，也不过滤 LogCtx 自动附加的 trace_id。已有非 nil 值的 EnsureTraceID 路径不补做注入。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
core.SetTraceLogEnabled(enabled)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：只控制新 TraceID 或显式非空覆盖时的日志 context 注入；已有值不补做注入。false 不关闭日志，也不屏蔽 LogCtx 从 context 自动追加 trace_id；原子设置不迁移已有 context 或后端状态。

## `Str`

```go
func Str(key, val string) LogField
```

[实现](../../internal/core/log.go#L65) · [行为专题](../config.md)：Str 创建字符串类型的日志字段。

**源码说明与使用片段**（原文）：

Str 创建字符串类型的日志字段。

参数：

  - key：字段名
  - val：字段值

使用示例：

	core.LogCtxInfo(ctx, "请求完成", core.Str("method", "GET"), core.Str("path", "/api/users"))


**参数说明**：

- `key` (`string`)：原样保存的日志字段名，空字符串也接受；不校验、不去重，也不自动加前缀。应使用稳定、无敏感信息的业务字段名。
- `val` (`string`)：原样保存的 string 字段值；不格式化、不脱敏，也不立即输出日志。

**返回值签名**：`LogField`。

**使用示例**（已初始化参数的调用片段）：

```go
core.Str(key, val)
```

**生产建议**：使用稳定字段名、明确数值和时长单位，再传给 LogCtx/Log 等输出入口。值不会自动脱敏，避免放入凭证或完整用户数据；编码格式在后端统一配置。

**注意事项**：只构造 LogField，不调用 logger、不分配执行器、不格式化或校验内容。Err 固定使用 error 键，其余键原样保存且允许空/重复键；Any/Bytes/Err 内部引用不复制。构造完成后再修改共享对象会影响后端读取，跨 goroutine 使用须同步；nil 字段如何编码由后端决定。

## `WaitContextImpl`

```go
func WaitContextImpl[T any](
	ctx context.Context,
	wg *sync.WaitGroup,
	cancel context.CancelFunc,
	submitGuard *atomic.Bool,
	waited *atomic.Bool,
	cancelAll func(),
	copyResults func() []Result[T],
	logCtx context.Context,
	callerType string,
	onDone ...func(),
) ([]Result[T], bool)
```

[实现](../../internal/core/core.go#L577) · [行为专题](../config.md)：WaitContextImpl 实现基于 context 的 Wait 逻辑，内部使用。

**源码说明与使用片段**（原文）：

WaitContextImpl 实现基于 context 的 Wait 逻辑，内部使用。

参数说明：

  - submitGuard：可 nil；由调用方预先设 true，返回前设 false。辅助函数不管理提交入口。
  - cancel：可 nil，返回路径同步调用；不会强制结束业务。
  - cancelAll/copyResults：非 nil、并发安全且及时返回；取消和快照同步执行，可延迟返回。
  - onDone：任务生产者完成后按参数顺序执行，非 nil、不 panic；取消返回后仍执行。

当前 Pool/Group 使用各自的等待流程，不调用本辅助函数。


**参数说明**：

- `ctx` (`context.Context`)：本次等待使用的非 nil context；取消后同步调用cancel，更新waited/submitGuard，再调用cancelAll和copyResults，返回false。这些回调阻塞会延迟返回；不强制停止任务，完成和取消同时就绪时select可选任一路径。
- `wg` (`*sync.WaitGroup`)：跟踪任务结束的计数器；调用前完成登记，避免计数归零时与新增任务竞争。
- `cancel` (`context.CancelFunc`)：可 nil，返回路径同步调用；不会强制结束业务。
- `submitGuard` (`*atomic.Bool`)：等待期间的提交保护标记；可为 nil。非 nil 时由调用方预先置 true，本方法在返回前置 false，生命周期仍须与 waited 标记和实际任务同步。
- `waited` (`*atomic.Bool`)：等待状态标记，返回前写入 true；须传有效指针。
- `cancelAll` (`func()`)：在等待完成或提前结束时同步取消已登记任务的函数，必须非 nil、并发安全且及时返回；只能发出取消信号。WaitTimeoutImpl 内部用 once 合并返回路径与 AfterFunc 的调用，可能在 waited/submitGuard 更新之前开始；不能依赖标记已更新或在回调中等待本次等待返回。
- `copyResults` (`func() []Result[T]`)：同步创建当前结果快照的函数，必须非 nil 并自行同步结果存储；等待超时时生产者可能仍在写入。
- `logCtx` (`context.Context`)：仅用于清理监视日志及 trace 字段的上下文，不控制等待或业务执行；nil 可传入，日志后端须支持此值。
- `callerType` (`string`)：用于清理日志的调用方名称，不影响任务调度。
- `onDone` (`...func()`)：所有任务生产者完成后在等待 goroutine 中按参数顺序执行的清理函数；每个函数须非 nil，不得 panic。等待超时或取消返回后仍会执行；清理完成后才关闭内部 done 通知。

**返回值签名**：`[]Result[T], bool`。首值由copyResults同步返回，本方法不额外复制或校验业务错误。bool=true表示WaitGroup和全部onDone完成后选中完成分支，false表示选中超时/取消分支；两者都同步取消并取得快照。完成与取消同时就绪时不保证分支，false不证明生产者仍在运行。没有拒绝重复或并发等待的内部门控，需调用方协调。

**使用示例**（已初始化参数的调用片段）：

```go
core.WaitContextImpl[T](ctx, wg, cancel, submitGuard, waited, cancelAll, copyResults, logCtx, callerType, onDone...)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：内部等待辅助入口，当前Pool/Group不调用它，不能代替执行器的提交门控。WaitGroup与waited必须有效；cancel可nil，非nil时须支持重复及并发调用，WaitTimeoutImpl的返回与AfterFunc可能各调用一次。cancelAll/copyResults/onDone须非nil、不panic且及时返回，快照自行同步。成功须等WaitGroup及全部onDone按顺序完成；超时可在onDone仍阻塞时返回，但同步取消/快照仍会延迟返回。仅返回路径更新waited/submitGuard并复制一次；没有一次性等待门控。提前返回后独立等待goroutine继续，监视每五分钟告警，到配置时长（非正按三十分钟）记Error后退出，不终止任务或取消后续onDone；阈值与告警同时就绪时日志先后不保证。

## `WaitTimeoutImpl`

```go
func WaitTimeoutImpl[T any](
	d time.Duration,
	wg *sync.WaitGroup,
	cancel context.CancelFunc,
	submitGuard *atomic.Bool,
	waited *atomic.Bool,
	cancelAll func(),
	copyResults func() []Result[T],
	logCtx context.Context,
	onDone ...func(),
) ([]Result[T], bool)
```

[实现](../../internal/core/core.go#L469) · [行为专题](../config.md)：WaitTimeoutImpl 实现带超时的 Wait 逻辑，内部使用。

**源码说明与使用片段**（原文）：

WaitTimeoutImpl 实现带超时的 Wait 逻辑，内部使用。

参数说明：

  - submitGuard：可 nil；由调用方预先设 true，返回前设 false。辅助函数不管理提交入口。
  - cancel：可 nil；非 nil 时须支持重复和并发调用，返回路径和 AfterFunc 可能各调用一次。
  - cancelAll/copyResults：非 nil、并发安全且及时返回；取消和快照同步执行，可延迟超时返回。
  - onDone：任务生产者完成后按参数顺序执行，非 nil、不 panic；超时返回后仍执行。

当前 Pool/Group 使用各自的等待流程，不调用本辅助函数。


**参数说明**：

- `d` (`time.Duration`)：内部等待 context 的期限；非正值立即过期，不是整个调用的耗时上限。到期后仍同步执行 cancel/cancelAll 和 copyResults，回调阻塞会延迟返回。完成与超时同时就绪时 select 可选任一路径，业务仍须协作退出。
- `wg` (`*sync.WaitGroup`)：跟踪任务结束的计数器；调用前完成登记，避免计数归零时与新增任务竞争。
- `cancel` (`context.CancelFunc`)：可 nil；非 nil 时须支持重复和并发调用，返回路径和 AfterFunc 可能各调用一次。
- `submitGuard` (`*atomic.Bool`)：等待期间的提交保护标记；可为 nil。非 nil 时由调用方预先置 true，本方法在返回前置 false，生命周期仍须与 waited 标记和实际任务同步。
- `waited` (`*atomic.Bool`)：等待状态标记，返回前写入 true；须传有效指针。
- `cancelAll` (`func()`)：在等待完成或提前结束时同步取消已登记任务的函数，必须非 nil、并发安全且及时返回；只能发出取消信号。WaitTimeoutImpl 内部用 once 合并返回路径与 AfterFunc 的调用，可能在 waited/submitGuard 更新之前开始；不能依赖标记已更新或在回调中等待本次等待返回。
- `copyResults` (`func() []Result[T]`)：同步创建当前结果快照的函数，必须非 nil 并自行同步结果存储；等待超时时生产者可能仍在写入。
- `logCtx` (`context.Context`)：仅用于清理监视日志及 trace 字段的上下文，不控制等待或业务执行；nil 可传入，日志后端须支持此值。
- `onDone` (`...func()`)：所有任务生产者完成后在等待 goroutine 中按参数顺序执行的清理函数；每个函数须非 nil，不得 panic。等待超时或取消返回后仍会执行；清理完成后才关闭内部 done 通知。

**返回值签名**：`[]Result[T], bool`。首值由copyResults同步返回，本方法不额外复制或校验业务错误。bool=true表示WaitGroup和全部onDone完成后选中完成分支，false表示选中超时/取消分支；两者都同步取消并取得快照。完成与取消同时就绪时不保证分支，false不证明生产者仍在运行。没有拒绝重复或并发等待的内部门控，需调用方协调。

**使用示例**（已初始化参数的调用片段）：

```go
core.WaitTimeoutImpl[T](d, wg, cancel, submitGuard, waited, cancelAll, copyResults, logCtx, onDone...)
```

**生产建议**：在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。

**注意事项**：内部等待辅助入口，当前Pool/Group不调用它，不能代替执行器的提交门控。WaitGroup与waited必须有效；cancel可nil，非nil时须支持重复及并发调用，WaitTimeoutImpl的返回与AfterFunc可能各调用一次。cancelAll/copyResults/onDone须非nil、不panic且及时返回，快照自行同步。成功须等WaitGroup及全部onDone按顺序完成；超时可在onDone仍阻塞时返回，但同步取消/快照仍会延迟返回。仅返回路径更新waited/submitGuard并复制一次；没有一次性等待门控。提前返回后独立等待goroutine继续，监视每五分钟告警，到配置时长（非正按三十分钟）记Error后退出，不终止任务或取消后续onDone；阈值与告警同时就绪时日志先后不保证。

## `WithConfig`

```go
func WithConfig(configured int) int
```

[实现](../../internal/core/core.go#L714) · [行为专题](../config.md)：WithConfig 如果 configured > 0 则返回 configured，否则返回 IO()。

**源码说明与使用片段**（原文）：

WithConfig 如果 configured > 0 则返回 configured，否则返回 IO()。 用于统一处理用户指定的并发度：传 0 或不传时自动使用合理默认值。

参数：

  - configured：用户指定的并发度，0 表示使用默认 IO()

使用示例：

	concurrency := core.WithConfig(userConfig) // userConfig=0 时自动使用 IO()


**参数说明**：

- `configured` (`int`)：用户指定的并发度；正数原样返回，0 或负数返回 IO()，不检查上界或分配资源。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
core.WithConfig(configured)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。

## `WithLogger`

```go
func WithLogger(ctx context.Context, l Logger) context.Context
```

[实现](../../internal/core/local_logger.go#L9) · [行为专题](../config.md)：WithLogger sets an immutable logger override for this context only.

**源码说明与使用片段**（原文）：

WithLogger sets an immutable logger override for this context only. nil clears the local override and resumes inheriting the global logger.


**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `l` (`Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
core.WithLogger(ctx, l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：取消和超时依靠回调协作，不能强制结束忽略 context 的业务函数；配置与等待的作用范围见专题。

## `WithTraceID`

```go
func WithTraceID(ctx context.Context, id string) context.Context
```

[实现](../../internal/core/core.go#L828) · [行为专题](../config.md)：WithTraceID 设置指定的 trace_id 到 ctx 中。

**源码说明与使用片段**（原文）：

WithTraceID 设置指定的 trace\_id 到 ctx 中。 如果 id 为空字符串，则等同于 EnsureTraceID：保留已有非 nil 值，缺失时才生成。 非空 id 覆盖或空 id 缺失生成时，启用 TraceLog 才注入 logger 上下文。

参数：

  - ctx：原始上下文
  - id：要设置的 trace\_id；空字符串保留当前非 nil 值，缺失时才生成

使用示例：

	// 从上游请求中透传 trace_id
	ctx := core.WithTraceID(ctx, upstreamTraceID)


**参数说明**：

- `ctx` (`context.Context`)：原始上下文
- `id` (`string`)：关联标识字符串，非空时覆盖当前 key 下的值；空字符串调用 EnsureTraceID，保留已有的非 nil 值，缺失时生成新标识。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
core.WithTraceID(ctx, id)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：传非 nil context。EnsureTraceID 仅检查当前 key 的值是否为 nil，非 nil 时原样返回，包括空字符串和非 string；它不修复类型。WithTraceID 的空 id 委托 EnsureTraceID，保留已有值；非空 id 才覆盖。启用 TraceLog 时，仅生成/显式覆盖路径调用 logger.With/WithContext，后端必须返回有效 context。GetTraceID 对非 string 值会 panic。自定义 key 在初始化时固定，已有 context 的旧值不会自动迁移；继续传递返回 context。

## `Logger.Log`

```go
func (instance Logger) Log(ctx context.Context, level LogLevel, msg string, fields ...LogField)
```

[实现](../../internal/core/log.go#L138) · [行为专题](../config.md)：输出一条指定级别的结构化日志；注入的实现须支持并发调用。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `level` (`LogLevel`)：日志级别，使用 LevelDebug/LevelInfo/LevelWarn/LevelError 常量。
- `msg` (`string`)：日志消息正文，动态业务字段通过 fields 传入。
- `fields` (`...LogField`)：结构化日志字段列表；库外使用 async.LogField 字面量，内部可用 Str/Int64/Dur/Any/Err/Bytes。无 Int 辅助函数，重复键不自动去重，引用值应按后端消费时点同步或复制，并先脱敏。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Log(ctx, level, msg, fields...)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `Logger.With`

```go
func (instance Logger) With(fields ...LogField) Logger
```

[实现](../../internal/core/log.go#L139) · [行为专题](../config.md)：返回携带指定字段的日志器；字段合并规则由注入实现定义。

**参数说明**：

- `fields` (`...LogField`)：结构化日志字段列表；库外使用 async.LogField 字面量，内部可用 Str/Int64/Dur/Any/Err/Bytes。无 Int 辅助函数，重复键不自动去重，引用值应按后端消费时点同步或复制，并先脱敏。

**返回值签名**：`Logger`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.With(fields...)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `Logger.WithContext`

```go
func (instance Logger) WithContext(ctx context.Context) context.Context
```

[实现](../../internal/core/log.go#L140) · [行为专题](../config.md)：返回日志实现关联或包装的 context；行为由注入实现定义。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithContext(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `PoolTask[T].Fn`

```go
Fn func(context.Context) (T, error)
```

[实现](../../internal/core/core.go#L342) · [行为专题](../config.md)：可调用函数字段：任务执行函数

**源码说明与使用片段**（原文）：

任务执行函数


**参数说明**：

- `arg1` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`T, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Fn(arg1)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：这是函数字段而非方法；调用前必须确认字段非 nil。字段的赋值、调用和并发安全由所属对象及调用方约定。

## `Result[T].Error`

```go
func (r Result[T]) Error() error
```

[实现](../../internal/core/core.go#L397) · [行为专题](../config.md)：Error 返回任务的错误，成功时为 nil。统一命名，与其他 Result 类型一致。

**源码说明与使用片段**（原文）：

Error 返回任务的错误，成功时为 nil。统一命名，与其他 Result 类型一致。


**参数说明**：无显式参数。

**返回值签名**：`error`。原样返回接收者 Err；不检查 Occupied，不清除或修改 Value。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Error()
```

**生产建议**：按产生该结果的入口判断 Occupied 是否需要检查，再处理 Err 和 Value；IsPanic 通过 errors.As 识别 PanicError，其他业务错误仍需单独处理。

**注意事项**：Ok 只检查 Err 是否为 nil，零值 Result 也返回 true；Error 原样返回 Err，不清除同时保存的 Value。Occupied 是否有意义由产生结果的入口决定。IsPanic 使用 errors.As，支持包装/多错误链；Err 是类型化 nil 的 *PanicError 时也为 true，提取后仍须判空。String 优先格式化非 nil Err，否则格式化 Value；对整个 Result 使用 %+v 不会把 '+' 传给内部错误，需提取 PanicError 再输出栈。

## `Result[T].IsPanic`

```go
func (r Result[T]) IsPanic() bool
```

[实现](../../internal/core/core.go#L391) · [行为专题](../config.md)：IsPanic 判断错误是否为 panic 导致的。

**源码说明与使用片段**（原文）：

IsPanic 判断错误是否为 panic 导致的。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsPanic()
```

**生产建议**：按产生该结果的入口判断 Occupied 是否需要检查，再处理 Err 和 Value；IsPanic 通过 errors.As 识别 PanicError，其他业务错误仍需单独处理。

**注意事项**：Ok 只检查 Err 是否为 nil，零值 Result 也返回 true；Error 原样返回 Err，不清除同时保存的 Value。Occupied 是否有意义由产生结果的入口决定。IsPanic 使用 errors.As，支持包装/多错误链；Err 是类型化 nil 的 *PanicError 时也为 true，提取后仍须判空。String 优先格式化非 nil Err，否则格式化 Value；对整个 Result 使用 %+v 不会把 '+' 传给内部错误，需提取 PanicError 再输出栈。

## `Result[T].Ok`

```go
func (r Result[T]) Ok() bool
```

[实现](../../internal/core/core.go#L378) · [行为专题](../config.md)：Ok 仅判断 Err 是否为 nil，不检查 Occupied，也不证明任务已经执行。

**源码说明与使用片段**（原文）：

Ok 仅判断 Err 是否为 nil，不检查 Occupied，也不证明任务已经执行。 使用可能含空槽的结果切片时，应按对应入口约定另外检查槽位状态。


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示对应结果成功；空集合规则见方法说明。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Ok()
```

**生产建议**：按产生该结果的入口判断 Occupied 是否需要检查，再处理 Err 和 Value；IsPanic 通过 errors.As 识别 PanicError，其他业务错误仍需单独处理。

**注意事项**：Ok 只检查 Err 是否为 nil，零值 Result 也返回 true；Error 原样返回 Err，不清除同时保存的 Value。Occupied 是否有意义由产生结果的入口决定。IsPanic 使用 errors.As，支持包装/多错误链；Err 是类型化 nil 的 *PanicError 时也为 true，提取后仍须判空。String 优先格式化非 nil Err，否则格式化 Value；对整个 Result 使用 %+v 不会把 '+' 传给内部错误，需提取 PanicError 再输出栈。

## `Result[T].String`

```go
func (r Result[T]) String() string
```

[实现](../../internal/core/core.go#L383) · [行为专题](../config.md)：String 返回 Result 的字符串表示。

**源码说明与使用片段**（原文）：

String 返回 Result 的字符串表示。


**参数说明**：无显式参数。

**返回值签名**：`string`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.String()
```

**生产建议**：按产生该结果的入口判断 Occupied 是否需要检查，再处理 Err 和 Value；IsPanic 通过 errors.As 识别 PanicError，其他业务错误仍需单独处理。

**注意事项**：Ok 只检查 Err 是否为 nil，零值 Result 也返回 true；Error 原样返回 Err，不清除同时保存的 Value。Occupied 是否有意义由产生结果的入口决定。IsPanic 使用 errors.As，支持包装/多错误链；Err 是类型化 nil 的 *PanicError 时也为 true，提取后仍须判空。String 优先格式化非 nil Err，否则格式化 Value；对整个 Result 使用 %+v 不会把 '+' 传给内部错误，需提取 PanicError 再输出栈。

## `nopLogger.Log`

```go
func (n nopLogger) Log(_ context.Context, _ LogLevel, _ string, _ ...LogField)
```

[实现](../../internal/core/log.go#L146) · [行为专题](../config.md)：内部空日志实现，忽略输出参数，不执行日志 I/O。

**参数说明**：

- `arg1` (`context.Context`)：空日志器忽略此参数；该类型是内部实现，不供外部构造。
- `arg2` (`LogLevel`)：空日志器忽略此参数；该类型是内部实现，不供外部构造。
- `arg3` (`string`)：空日志器忽略此参数；该类型是内部实现，不供外部构造。
- `arg4` (`...LogField`)：空日志器忽略此参数；该类型是内部实现，不供外部构造。

**返回值**：无。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Log(arg1, arg2, arg3, arg4...)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `nopLogger.With`

```go
func (n nopLogger) With(_ ...LogField) Logger
```

[实现](../../internal/core/log.go#L147) · [行为专题](../config.md)：内部空日志实现，忽略输出参数，不执行日志 I/O。

**参数说明**：

- `arg1` (`...LogField`)：空日志器忽略此参数；该类型是内部实现，不供外部构造。

**返回值签名**：`Logger`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.With(arg1...)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

## `nopLogger.WithContext`

```go
func (n nopLogger) WithContext(ctx context.Context) context.Context
```

[实现](../../internal/core/log.go#L148) · [行为专题](../config.md)：内部空日志实现，忽略输出参数，不执行日志 I/O。

**参数说明**：

- `ctx` (`context.Context`)：空日志器忽略此参数；该类型是内部实现，不供外部构造。

**返回值签名**：`context.Context`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.WithContext(ctx)
```

**生产建议**：用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。

**注意事项**：日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。

