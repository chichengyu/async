package main

import "strings"

// These overrides describe implementation-specific boundaries that a parameter
// name or type alone cannot express. They are shared by all thin facades.
func semanticParameter(e entry, p parameter) string {
	if e.pkg == "maps" && p.name == "other" {
		switch e.name {
		case "Intersect":
			return "非 nil 的另一容器；只用其键判定是否保留，结果值取接收者，不比较两边值是否相等。"
		case "Difference":
			return "非 nil 的排除容器；其已有键从结果中排除，即使两边对应值不同。"
		case "Union":
			return "非 nil 的另一容器；并入新结果，相同键使用此容器的值覆盖接收者值，原输入均不修改。"
		}
	}
	if e.pkg == "maps" && p.name == "fn" && !strings.Contains(p.typ, "context.Context") {
		switch e.name {
		case "All":
			return "同步谓词；返回 false 立即终止并使 All 返回 false；全部为 true 或空输入时 All 返回 true。"
		case "Any", "Find":
			return "同步匹配谓词；首个 true 后立即停止，Any 返回 true，Find 返回该次键值及 found=true。遍历顺序不确定。"
		case "Count":
			return "同步匹配谓词；每项调用一次，返回 true 的条目累计到结果，空输入返回 0。"
		}
	}
	if strings.HasPrefix(e.file, "internal/sliceops/") {
		if e.name == "Grow" && p.name == "n" {
			if strings.HasPrefix(e.receiver, "*Slice[") {
				return "额外容量需求；非正值无操作，正数追加零值再恢复原长度，可能改写共享数组长度外槽位。须控制 len+n 和分配预算。"
			}
			return "额外容量需求；负值 panic，非负值追加零值再恢复长度，可能改写共享数组长度外槽位。须控制 len+n 和分配预算。"
		}
		if e.name == "Repeat" && p.name == "n" {
			return "原切片的总重复次数；非正值清空，1 无操作，更大值反复浅追加。调用前校验 len×n 的数量和内存预算，元素内部引用不复制。"
		}
		if (e.name == "Chunk" || e.name == "Chunked") && p.name == "chunkSize" {
			return "每块最大元素数；非正值或空输入返回 nil，末块可不足此数。子切片共享输入且保留剩余容量，append 可能改写后续块。"
		}
	}
	if e.pkg == "task" && strings.Contains(e.receiver, "Mu[") && p.name == "add" {
		return "在调用方锁外同步执行的元素生成函数，必须非 nil；其返回值随后在锁内追加，不为生成过程创建 goroutine。可重入同一容器的 Append/Snapshot；panic 直接传播，不追加该次返回值，但已完成的嵌套追加和其他副作用不回滚。多个调用可同时生成，须同步共享数据。"
	}
	if e.pkg == "maps" && e.receiver == "*Map[K, V]" && p.name == "fn" {
		switch e.name {
		case "ForEach":
			return "同步逐项接收键和值，无返回值，不能提前终止遍历；map 顺序不确定，panic 直接传播。"
		case "FilterKeys":
			return "同步接收每个键的谓词；true 保留该键值对，false 原地删除，遍历顺序不确定，panic 直接传播。"
		case "FilterValues":
			return "同步接收每个值的谓词；true 保留对应键值对，false 原地删除，遍历顺序不确定，panic 直接传播。"
		}
	}
	if e.pkg == "async" && strings.HasPrefix(e.file, "internal/") {
		e.pkg = strings.Split(e.file, "/")[1]
	}
	if e.pkg == "async" && (e.name == "NewFlow" || e.name == "ExecuteStream") {
		e.pkg = "pipeline"
	}
	if e.pkg == "retry" && (e.name == "WithTimeout" || e.name == "WithTimeoutVoid" || e.name == "WithDeadline" || e.name == "WithDeadlineVoid") {
		if p.name == "ctx" {
			return "非 nil 的父 context；值继承到派生 context，已有取消立即传播，更早的父期限优先。即使父已取消仍调用 fn，包装结束只取消派生 context，不取消父 context。"
		}
		if p.name == "deadline" {
			return "本次同步调用的绝对截止时间，与父期限取更早者；过去时间及零值 time.Time 都使派生 context 立即到期，但仍调用 fn，不自动改写 fn 的返回值或错误。"
		}
	}
	if p.name == "ctx" && (e.pkg == "retry" && (e.name == "New" || e.name == "NewVoid") || e.pkg == "async" && (e.name == "Retry" || e.name == "RetryVoid")) {
		return "原样保存的父 context，终端执行时须非 nil；构造和执行不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用构建器 Context(ctx)；正数 PerCallTimeout 仅为每次业务派生期限。"
	}
	if p.name == "ctx" && e.pkg == "ratelimit" && (e.name == "Acquire" || e.name == "Wait" || e.name == "Token") {
		if strings.Contains(e.receiver, "AdaptiveRateLimiter") {
			return "本次获取槽位的非 nil context；固定使用 Block，有空位时已取消仍可成功，无空位时等待归还或取消。Adaptive 没有公开的策略切换方法；分片只等待选中一片，不搜索其他片。仅成功获取后配对同一入口的 Release。"
		}
		return "本次获取许可的非 nil context；Block 只在无可用许可时响应取消，有空位时已取消仍可成功，Reject/BlockForce 忽略取消。分片只访问选中一片，获取失败不得 Release；Token 成功后通过句柄释放。"
	}
	if e.pkg == "core" {
		if e.receiver == "" {
			switch e.name {
			case "Str", "Any", "Bytes", "Int64", "Dur":
				if p.name == "key" {
					return "原样保存的日志字段名，空字符串也接受；不校验、不去重，也不自动加前缀。应使用稳定、无敏感信息的业务字段名。"
				}
				switch e.name {
				case "Str":
					return "原样保存的 string 字段值；不格式化、不脱敏，也不立即输出日志。"
				case "Int64":
					return "原样保存的 int64 字段值；不转为字符串、不校验正负或业务单位。"
				case "Dur":
					return "原样保存为 time.Duration 的时长，负值和零值也接受；不换算单位或调用 String，实际编码由日志后端决定。"
				case "Bytes":
					return "原样保存的 []byte，包括 nil；不复制底层数组、不做字符串或 base64 转换，后续修改与字段共享同一数据。"
				case "Any":
					return "原样保存的任意值，包括 nil 和类型化 nil；不调用 String/Error，不序列化或深拷贝，引用内容仍共享。"
				}
			case "Err":
				return "原样保存的 error，nil 和类型化 nil 也接受；字段名固定为 error。不调用 Error、不解包、不输出日志，也不判断任务是否失败。"
			}
		}
		if strings.Contains(e.receiver, "PanicError") && e.name == "Format" {
			if p.name == "s" {
				return "fmt 传入的输出状态；读取 Writer 和 '+' 标记，不读取宽度或精度。通常让 fmt 自动调用；首次写入出错立即返回，追加栈的写入错误不向外返回。"
			}
			if p.name == "verb" {
				return "fmt 格式动词；仅 'v' 且带 '+' 时追加当前 Stack。其他动词仍输出相同简短描述，%q 不加引号或转义，%x 不做十六进制编码；需转义时先 Error() 得到字符串再格式化。"
			}
		}
		switch e.name {
		case "SetTraceLogEnabled":
			return "是否在生成或显式覆盖 TraceID 时调用 logger.With/WithContext；不关闭 TraceID 生成存储，也不过滤 LogCtx 自动附加的 trace_id。已有非 nil 值的 EnsureTraceID 路径不补做注入。"
		case "SetTaskFailLogLevel":
			return "任务失败日志的全局级别；Error/Warn/Info/Debug 分别输出相应级别，Silent 与范围外值均不输出。原值按 int32 保存，不校验枚举范围；普通 Log/LogCtx 不受其过滤。"
		case "SetDefaultTimeout":
			return "后续新建 Pool/Group 保存的默认任务超时；正数设置回调 context 期限，非正值不设置期限。不会更新已有实例，TaskBuilder 不读取此全局值。"
		case "SetSubmitTimeout":
			return "保存供 GetSubmitTimeout 读取的全局时长，仅正数更新。Pool/Group 不自动读取，限制入队等待需在具体实例或构建器另行配置。"
		case "SetMaxCleanupDuration":
			return "内部 Wait 辅助函数在提前返回后监视清理的时长，原样保存；非正值在辅助函数中使用 30 分钟。只停止监视，不会终止任务。"
		case "SetDefaultMaxResults":
			return "后续新建 Pool 的主结果保留上限；0 不限制，正数限制提交索引，负数不更新。已有 Pool、Group 和 Flow 不因此改变。"
		case "SetDefaultRingBuffer":
			if p.name == "capacity" {
				return "后续新建 Pool 的环形缓冲容量；0 关闭默认缓冲，负数使整个配置调用无操作。底层以 int32 保存，生产配置应在 0..MaxInt32 内并按内存预算设置。"
			}
			return "环形缓冲满时的策略；Drop 覆盖最旧结果并计数，Block/Error 拒绝新写入，不阻塞 worker。只有 capacity 非负时同时更新；两项全局值分别存储。"
		case "IOMulti":
			return "逻辑 CPU 数的乘数；非正值使用 2，返回 NumCPU×有效倍数，与 GOMAXPROCS 无关。避免超大倍数造成整数溢出或不可承受的 worker 数。"
		case "WithConfig":
			return "用户指定的并发度；正数原样返回，0 或负数返回 IO()，不检查上界或分配资源。"
		case "WithTraceID":
			if p.name == "id" {
				return "关联标识字符串，非空时覆盖当前 key 下的值；空字符串调用 EnsureTraceID，保留已有的非 nil 值，缺失时生成新标识。"
			}
		case "WaitContextImpl", "WaitTimeoutImpl":
			switch p.name {
			case "ctx":
				return "本次等待使用的非 nil context；取消后同步调用cancel，更新waited/submitGuard，再调用cancelAll和copyResults，返回false。这些回调阻塞会延迟返回；不强制停止任务，完成和取消同时就绪时select可选任一路径。"
			case "logCtx":
				return "仅用于清理监视日志及 trace 字段的上下文，不控制等待或业务执行；nil 可传入，日志后端须支持此值。"
			}
		}
	}
	if e.name == "BindRetryToWorker" {
		switch p.name {
		case "maxRetries":
			return "首次提交超时之后允许的重提次数；负数按 0，最多调用 backend.Submit 共 maxRetries+1 次。其他提交错误不重试。"
		case "maxBackoff":
			return "指数退避的正数上限；非正值不设置上限，initialBackoff 非正时也不等待。"
		case "backend":
			return "非 nil 的提交适配器，同步调用其 Submit；泛型 Pool 的 Submit 签名不同，须显式适配。只有 errors.Is(err, ErrSubmitTimeout) 才退避重试，其他错误立即返回。"
		case "fn":
			return "原样传给 backend.Submit 的任务函数；是否异步执行、如何传播执行错误以及 panic 规则均由后端决定。本辅助函数只重试提交，不主动执行或重试业务函数，也不恢复后端 panic。"
		case "initialBackoff":
			return "首次提交超时后的指数退避起始时长，后续逐次翻倍并按 maxBackoff 截断；非正值不等待。等待期间响应 ctx 取消。"
		}
	}
	if e.name == "MergeCancel" {
		if p.name == "oldCancel" {
			return "原有取消函数，可为 nil；非 nil 时返回的闭包先调用 newCancel 再调用它。"
		}
		return "新增取消函数；oldCancel 非 nil 时它也必须非 nil，否则调用组合函数时 panic。oldCancel 为 nil 时直接返回此值（包括 nil）。"
	}
	if e.name == "MergeFailFastCancel" && p.name == "cancel" {
		return "合并到 FailFast 取消链的新取消函数，应非 nil；同步执行且须并发安全、快速返回。已有非 nil 取消链时合并 nil 会使后续触发取消 panic；配置须在提交任务前完成。"
	}
	if p.name == "cmp" && strings.HasPrefix(p.typ, "func") && e.pkg == "sliceops" {
		return "三路比较函数：负数、零、正数分别表示 a 小于、等于、大于 b，须提供一致的排序关系。整数建议用 cmp.Compare，直接 a-b 可能溢出；二分搜索须先按同一规则排序，比较回调 panic 直接传播。"
	}
	if (e.name == "WaitContextImpl" || e.name == "WaitTimeoutImpl") && p.name == "submitGuard" {
		return "等待期间的提交保护标记；可为 nil。非 nil 时由调用方预先置 true，本方法在返回前置 false，生命周期仍须与 waited 标记和实际任务同步。"
	}
	if p.typ == "time.Duration" {
		if e.pkg == "retry" && (e.name == "WithTimeout" || e.name == "WithTimeoutVoid") {
			return "为本次同步调用派生 context 的时长；非正值立即到期，但仍调用 fn。返回值完全来自 fn，超时不自动替换其返回错误，也不能强制停止业务。"
		}
		if e.pkg == "group" && (e.name == "GoWithTimeout" || e.name == "GoAtWithTimeout") {
			return "覆盖本次任务的 Group 执行超时；正数从回调开始执行时计时，非正值清除 Group 级期限，仍受父 context 约束。注入外部池时，该池原有任务超时仍可能生效。与 Pool.SubmitWithCompletionTimeout 的非正值继承规则不同。"
		}
		if e.pkg == "core" && e.name == "WaitTimeoutImpl" {
			return "内部等待 context 的期限；非正值立即过期，不是整个调用的耗时上限。到期后仍同步执行 cancel/cancelAll 和 copyResults，回调阻塞会延迟返回。完成与超时同时就绪时 select 可选任一路径，业务仍须协作退出。"
		}
		if e.name == "WaitTimeout" && e.pkg == "task" {
			return "本次等待结果的时长；非正值不阻塞等待，但已缓存或已到达的结果仍优先返回。等待超时返回 ErrTimeout 和 false，不取消任务，可稍后再次等待。"
		}
		if e.name == "WaitTimeout" && (e.pkg == "pool" || e.pkg == "group" || e.pkg == "shard") {
			return "本次等待的最长时长；非正值使等待 context 立即过期，完成与超时同时就绪时不保证 bool 的选择。超时返回部分结果并通知取消，不能强制停止任务；分片共享同一截止时间，复用须遵守该实例的等待/复位约定。"
		}
		if e.pkg == "pool" && e.name == "ResizeAndWaitTimeout" {
			return "仅限制最后等待已登记任务归零的时长；Resize 的同步锁等待和正在登记的提交收敛均发生在计时之前，不是整个方法的调用时限。非正值使最后等待立即到期，超时不取消执行。未返回是否完成的标记，也不保证所有旧 worker 均已退出。"
		}
		if (e.pkg == "pool" || e.pkg == "group" || e.pkg == "shard") && (e.name == "Timeout" || e.name == "WithTimeout") {
			if strings.Contains(e.receiver, "Builder") || e.receiver == "Config" || strings.HasPrefix(e.receiver, "ShardPoolConfig[") {
				return "创建执行器时使用的单任务 context 超时；只应用正数，非正值保留创建时的全局默认任务超时，不能据此清除非零全局默认值。外部池路径还受其原有超时设置影响。"
			}
			return "每个任务开始执行后的 context 超时；正数生效，非正值清除本实例配置的执行期限，不包含等待入队时间。分片方法对每片应用，回调必须响应 context；在提交前配置。"
		}
		if (e.pkg == "pool" || e.pkg == "group" || e.pkg == "shard") && (e.name == "SubmitTimeout" || e.name == "WithSubmitTimeout" || e.name == "WithShardSubmitTimeout") {
			return "等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受 context 和关闭状态影响。构建器及 Config 创建路径仅应用正数；分片按每片独立控制，在提交前配置。"
		}
		if e.pkg == "ratelimit" && p.name == "perDuration" {
			return "每 rate 个令牌的补充时间窗口；正数按时间补充，非正值每个 100ms tick 把桶补满。Release 会提前归还令牌；需持续按速率限流时不要每次业务后主动归还。"
		}
		if e.pkg == "ratelimit" && p.name == "window" {
			return "滑动窗口保留请求记录的时长；正数形成窗口限制，非正值使旧记录在后续检查中立即过期，不自动恢复默认值。"
		}
		if e.pkg == "group" && p.name == "submitTimeout" {
			return "等待任务入队的最长时间；正数生效，非正值不设置提交计时器，仍受提交 context 和关闭状态影响。与任务执行超时不同，在提交前配置。"
		}
		if e.pkg == "group" && p.name == "timeout" && strings.HasPrefix(e.name, "With") {
			return "每个任务开始执行后的 context 超时；正数设置截止时间，非正值不限时，不包含等待入队时间。业务函数须自行响应 context。"
		}
		if e.pkg == "sliceops" && p.name == "timeout" && (strings.Contains(e.name, "Map") || strings.Contains(e.name, "ForEach")) {
			if strings.Contains(e.name, "ForEachChunked") && e.receiver == "" {
				return "每个批次共享的 context 超时；从外层批次任务开始执行计时，包含批内调度及全部元素执行。正数生效，非正值清除外层期限，批内 Group 仍可能继承全局默认超时；不是每个元素重新计时，回调须响应 context。"
			}
			if strings.Contains(e.name, "MapChunked") && e.receiver != "" {
				return "每个元素的 context 超时；正数覆盖底层 Group 的默认期限，非正值且 batchSize 为正时不覆盖创建时的全局默认超时。调用的是元素回调，回调须响应 context，不能强制中止。"
			}
			if strings.Contains(e.name, "ForEach") && e.receiver != "" && (!strings.Contains(e.name, "Chunk") || strings.Contains(e.name, "Chunked")) {
				return "单个元素回调的 context 超时；正数生效，非正值不应用显式超时。并行 Group 路径仍可能继承创建时全局默认超时，串行直接调用路径不设置期限；回调须响应 context。"
			}
			return "单次业务回调的 context 超时；正数设置截止时间，非正值不限时。Chunk 批处理每批计时，MapChunked 和 Slice.ForEachChunked 按元素计时；回调须响应 context，不能强制中止。"
		}
		if e.pkg == "pool" && e.name == "SubmitWithCompletionTimeout" {
			return "此任务的执行超时覆盖；仅正数覆盖池的任务超时，非正值继续使用池设置，不表示清除池超时。等待入队仍由提交超时和 context 控制。"
		}
		if e.pkg == "pool" && e.name == "CloseAndWaitTimeout" {
			return "取得关闭/扩缩容互斥锁并更新关闭状态之后，等待关闭完成的时长；不限制此前锁等待，不是整体调用时限。非正值立即触发计时器，若清理也已完成则 select 可能选中任一路径。超时返回后清理继续，不会强制结束回调；首次返回的 workerDone 只表示 worker 退出，完整清理另用 Close 等待。"
		}
	}
	if e.pkg == "group" && (e.name == "NewGroup" || e.name == "NewNoResult" || e.name == "Worker") && (p.name == "concurrency" || p.name == "n") {
		return "每个 Group 的最大同时执行数；非正值在构造时使用 1。GroupBuilder 默认已保存 IO()，MultiGroupBuilder 零值则使用 1；分片模式按每片计算，总容量乘以片数。"
	}
	if e.pkg == "group" && e.name == "WithMultiGroup" && p.name == "concurrency" {
		return "每片 Group 的并发上限；非正值使用 1，总容量乘以有效分片数。作用域函数返回后关闭全部分片，需取得全部结果时在函数内等待并检查错误。"
	}
	if e.pkg == "sliceops" && e.receiver == "" && strings.Contains(e.name, "MapChunked") && p.name == "concurrency" {
		return "底层 Group 的同时执行上限；非正值使用 1，Default* 入口显式传入 core.IO()。分块只改变提交遍历，仍由一个 Group 调度全部元素。"
	}
	if e.pkg == "sliceops" && (p.name == "batchSize" || p.name == "chunkSize") && strings.Contains(e.name, "Chunk") && (strings.Contains(e.name, "Map") || strings.Contains(e.name, "ForEach")) {
		if strings.Contains(e.name, "Chunked") && e.receiver != "" {
			return "分块调度的元素数；正数启用分块，非正值在 Slice 的策略路径退回普通逐元素执行，不会丢弃输入。元素统计与回调签名不变。"
		}
		return "每批最多包含的元素数；独立函数或接收 []T 的批处理入口在非正值时不产生批次，因而不调用业务函数并返回空结果/零统计。子切片共享输入底层数组。"
	}
	if e.pkg == "pipeline" && e.name == "Worker" {
		return "覆盖全部阶段的并发数；正数生效，非正数恢复各 Stage 的 Concurrency，再对非正阶段值使用 IO()。有限输入最多使用输入项数个 worker；外部池的容量还会限制实际并发。"
	}
	if (e.pkg == "pool" || e.pkg == "group" || e.pkg == "shard" || e.pkg == "pipeline") && (e.name == "AutoScale" || e.name == "EnableAutoScale") && (p.name == "config" || p.name == "cfg") {
		if e.name == "AutoScale" {
			return "保存供创建执行器时使用的自动扩缩容配置指针；nil 清除构建器中的配置、不启用扩缩容。DefaultAutoScale 保存默认配置；配置指针及字段不得在执行期间并发修改。"
		}
		return "自动扩缩容配置指针；nil 时使用默认配置，启用时复制并在副本上 Normalize，不修改调用方配置。读取配置时不得并发修改；启用后的修改不影响已保存的副本。调用会启动后台监视，应在任务前配置并按实例生命周期停止。"
	}
	if e.pkg == "group" && e.name == "Streaming" {
		return "构建器保存的结果通道缓冲容量；仅正数在 Build 时启用流，非正数不启用。实例 WithStreaming(0) 则启用 worker×2 缓冲，二者边界不同。流可能丢结果，须及时消费。"
	}
	if (e.pkg == "pool" || e.pkg == "shard") && (e.name == "MaxResults" || e.name == "WithMaxResults" || e.name == "WithShardMaxResults") {
		if e.pkg == "shard" && !strings.Contains(e.receiver, "*ShardedPool[") {
			return "每片主结果的保留上限；正数限制结果，非正数在创建分片时均应用为无限制，不继承 Pool 的全局上限。结果溢出路径须另行配置和消费。"
		}
		if e.receiver == "Config" || strings.Contains(e.receiver, "Builder") {
			return "内部创建池时每池主结果的保留上限；0 无限制，正数保留前 n 个索引，负数不覆盖创建池时的全局默认值。MultiPool 按每片独立计数；注入外部池时 MultiPoolBuilder 的此配置不应用，使用外部池原有设置。结果溢出策略需另行配置。"
		}
		return "每池主结果的保留上限；0 无限制，正数按提交索引限制，负数忽略并保留原配置。应在提交前设置，超出部分通过已配置的环形缓冲、流或回调处理，否则可能丢失。"
	}
	if e.pkg == "shard" && e.name == "Distribution" && strings.Contains(e.receiver, "ShardedGroupBuilder") {
		return "保存的分发策略值；当前 Group 的 Go/GoBatch 始终轮转，GoKeyed 始终按字符串哈希，均不读取此字段。需要亲和路由时显式调用 GoKeyed。"
	}
	if e.pkg == "pool" && e.name == "MapPool" && p.name == "fn" {
		return "并发接收 context 和当前元素，返回该项映射值及错误；panic 转为 PanicError。结果可受 MaxResults 截断；末尾 error 当前不汇总业务错误，须检查保留结果中的 Err。"
	}
	if e.pkg == "pool" && (e.name == "WithPool" || e.name == "WithCfg") && p.name == "fn" {
		return "同步接收新建池的作用域函数，返回业务错误；外层在函数返回时关闭池。需取得全部任务结果时在函数内 Wait 并检查逐项 Err，不在函数返回后继续提交。"
	}
	if e.receiver == "WorkerPoolBackend" && p.name == "fn" {
		return "交给适配后端的任务函数，接收执行上下文并返回业务 error；后端如何保存或报告此错误、是否并发及 panic 规则由实现约定。Submit 自身的 error 按后端协议表示提交结果，不能假定它等待并返回业务错误。"
	}
	if (e.name == "Split" || e.name == "Partition") && strings.HasPrefix(p.typ, "func") {
		return "同步按输入顺序判断元素；true 放入 matched，false 放入 unmatched。返回两个新切片但元素内部引用仍共享，panic 直接传播，不修改输入切片。"
	}
	if strings.HasPrefix(p.typ, "func") && p.name == "complete" {
		return "接收该次提交终态（包括拒绝、取消及执行结果）的同步通知，最多调用一次。可能在提交线程或 worker 调用；须非 nil、支持并发，panic 被记录；不得同步等待或关闭本池。"
	}
	if strings.HasPrefix(p.typ, "func") && p.name == "transform" {
		return "写入存储前同步转换本项 Result，返回实际保存的 Result；nil 保留原结果，panic 转为该项 PanicError。多个任务可并发调用，不得在回调内等待或关闭本池。"
	}
	if strings.HasPrefix(p.typ, "func") && p.name == "fn" {
		switch e.pkg {
		case "pipeline":
			if e.name == "ExecuteWithGroup" {
				return "接收执行 context 和当前元素，返回该项值及错误，不接收阶段名；可并发调用，panic 转为 PanicError。结果按记录完成顺序返回，顶层 error 不汇总业务错误，必须逐项检查 Err。"
			}
			if e.name == "Drain" {
				return "在调用 Drain 的 goroutine 同步接收每个 Result 的消费函数，无返回值，不自动过滤 Err；函数返回后才读取下一项。传非 nil 回调；nil 仅在实际读到结果时 panic。回调 panic 原样传播，当前项已从通道取出且不会重放，不自动取消生产方；继续消费或取消须由调用方协调。"
			}
			if e.name == "NewFlow" || e.receiver == "*FlowBuilder[T]" {
				return "各阶段 worker 调用的处理函数：接收 Flow 的 context、阶段名和当前元素，返回下一阶段的值及错误，可并发调用。panic 转为 PanicError 并保留该阶段输入值；显式 Stage.SkipOnError=true 时跳过前错，否则后续成功可覆盖它。链式 Stage 未提供该字段设置入口；取消须回调协作。"
			}
			if e.receiver == "*SerialChain[T]" {
				return "逐项同步调用的阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；ExecutePipe 在后台逐项调用。panic 转为 PanicError 且值为零值。Serial 不读取 Stage.SkipOnError；FailFast 可停止后续调用，非最终阶段 Pipe 失败不会作为输出发送，须用 OnResult 观察；取消须回调协作。"
			}
			return "阶段处理函数：接收 context、阶段名和当前元素，返回下一阶段的值及错误；有限输入路径 panic 转为 PanicError 且值为零值，取消须回调协作。默认/Parallel/Stream 路径可并发调用，Serial 逐项调用。显式传入 stages 的 Execute/ExecuteWithMeta/ExecuteStream/ExecutePipe 支持 SkipOnError；链式 Stage 创建的该字段为 false 且无设置入口，Serial 也不读取它。后续成功可能覆盖前错，逐项检查结果并按需要观察阶段事件。"
		case "task":
			return "在异步 goroutine 执行的业务函数，接收任务 context 并返回签名中的值与错误；panic 缓存为 PanicError。取消不会强制中止函数；Bounded 满时调用方先等待槽位，重用构建器的多次提交可并发调用此函数。"
		case "retry":
			if e.name == "WithTimeout" || e.name == "WithTimeoutVoid" || e.name == "WithDeadline" || e.name == "WithDeadlineVoid" {
				return "在当前 goroutine 同步调用一次的业务函数，接收派生 context，直接保留签名中的值与错误；已取消时仍调用，panic 原样传播。普通返回、panic展开或runtime.Goexit时均取消派生context；自建goroutine不延长其存活时间。回调须非nil并自行响应context，nil函数调用panic也不被恢复。"
			}
			if strings.HasPrefix(e.name, "RetryWith") {
				return "每次尝试同步调用的业务函数，接收当前 context，返回值及错误或仅错误；nil 错误结束重试，panic 转为 PanicError 后按策略重试。回调须幂等并响应取消，最终失败时有值版本返回类型零值和包装错误。"
			}
			if strings.Contains(e.receiver, "RetryChain") {
				if e.name == "Run" {
					return "每次尝试同步调用的无参数函数，仅返回 error；nil 表示成功，panic 转为 PanicError 后按策略处理。此签名不传递链内 context 或单次超时 context；需要响应单次取消时使用 ExecuteVoid/RunVoid，并使业务幂等。"
				}
				return "每次尝试同步调用的业务函数，接收该次 context 并返回结果与错误；nil 错误结束重试，panic 转为 PanicError 后按重试策略处理。须幂等且响应取消。ExecuteVoid/RunVoid 仅返回 error。"
			}
		case "maps":
			if strings.Contains(p.typ, "context.Context") && e.name != "Filter" && e.name != "Reduce" {
				desc := "处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。"
				if strings.Contains(p.typ, "[]MapEntry") {
					desc = "每批接收 context 和 []MapEntry，每条包含 Key/Val；返回本批映射值及错误，或只返回错误。批次来自无序 map 遍历，按策略并发执行，panic 转为错误；不按相同键分组。"
				}
				return desc
			}
		case "sliceops":
			if strings.Contains(p.typ, "context.Context") && e.name != "Reduce" && e.name != "Fold" && !strings.HasPrefix(e.name, "SafeCall") {
				if strings.Contains(p.typ, "[]T") {
					return "每批接收 context 和输入子切片，返回本批映射值及错误，或只返回错误；每批调用一次。按执行策略并发，panic 转为错误；子切片共享输入数组，不能与其他访问并发修改。"
				}
				return "逐项接收 context 和输入元素，返回该项映射值及错误，或只返回错误；按执行策略并发调用，panic 转为错误。Chunked 仍逐项调用；需幂等或自行同步共享状态，并响应取消。"
			}
		case "pool", "group", "shard":
			if strings.HasPrefix(e.name, "Submit") || strings.HasPrefix(e.name, "TrySubmit") || strings.HasPrefix(e.name, "Go") {
				return "被接受的任务在 worker 内调用，接收任务 context（批量入口另接收输入项），返回结果与错误或仅错误；多个任务可并发，panic 转为 PanicError。提交成功不等于业务成功，应等待并检查 Result.Err，取消须协作。"
			}
		}
	}
	if (e.name == "WaitContextImpl" || e.name == "WaitTimeoutImpl") && p.name == "cancelAll" {
		return "在等待完成或提前结束时同步取消已登记任务的函数，必须非 nil、并发安全且及时返回；只能发出取消信号。WaitTimeoutImpl 内部用 once 合并返回路径与 AfterFunc 的调用，可能在 waited/submitGuard 更新之前开始；不能依赖标记已更新或在回调中等待本次等待返回。"
	}
	if (e.name == "WaitContextImpl" || e.name == "WaitTimeoutImpl") && p.name == "copyResults" {
		return "同步创建当前结果快照的函数，必须非 nil 并自行同步结果存储；等待超时时生产者可能仍在写入。"
	}
	if (e.name == "WaitContextImpl" || e.name == "WaitTimeoutImpl") && p.name == "onDone" {
		return "所有任务生产者完成后在等待 goroutine 中按参数顺序执行的清理函数；每个函数须非 nil，不得 panic。等待超时或取消返回后仍会执行；清理完成后才关闭内部 done 通知。"
	}
	if (e.name == "SafeCall" || e.name == "SafeCallVoid" || e.name == "SafeCallWithResult") && strings.HasPrefix(p.typ, "func") {
		return "同步接收 context 和输入项的业务函数；返回签名中的值和错误。panic 被恢复并包装为 PanicError；不设置超时、不另启 goroutine，回调须自行响应 context。"
	}
	if e.name == "Drain" && e.pkg == "pipeline" {
		return "同步处理从输出通道读出的每个 Result，直到通道关闭；检查 Err 后使用 Value。panic 直接传播，阻塞会阻止继续排空。"
	}
	if e.name == "Fold" && strings.HasPrefix(p.typ, "func") {
		return "按输入顺序同步接收 context、当前累积值和元素，返回新累积值与错误；非 nil 错误立即结束，panic 直接传播。不使用并行度或策略超时，回调须自行检查 context。"
	}
	if e.pkg == "maps" && strings.HasPrefix(p.typ, "func") && e.name == "ForEach" && !strings.Contains(p.typ, "context.Context") {
		return "同步接收每个键和值，无返回值且不能提前停止；遍历顺序不确定，panic 直接传播。不要并发写入共享 map。"
	}
	if strings.HasPrefix(p.typ, "func") && (strings.HasPrefix(e.name, "WithMulti") || strings.HasPrefix(e.name, "WithShard") || strings.HasPrefix(e.name, "WithRateLimiter")) {
		return "同步接收新创建的执行器/限流器的作用域函数，返回其业务错误；外层在函数返回后关闭资源。需完成全部任务时在函数内等待，不应在返回后继续使用该实例。"
	}
	if e.pkg == "task" && strings.Contains(e.receiver, "TaskBuilder") && e.name == "WithTimeout" {
		return "业务回调开始执行后的 context 超时；正数生效，非正值不限时。Bounded 等槽位时间不计入此时长，回调须响应 context 才能退出。"
	}
	if e.pkg == "task" && strings.Contains(e.receiver, "BoundedRunnerBuilder") && e.name == "Context" {
		return "仅保存在构建器中的上下文，会注入 trace_id；Build 不将它保存到 BoundedRunner，提交时必须另外传入所需任务 context。"
	}
	if e.pkg == "task" && e.name == "WaitContext" && p.name == "ctx" {
		return "仅控制此次等待的非 nil context；若缓存或结果通道已有结果，会优先返回实际值/错误和 true，即使此 context 已取消。否则等待结果与取消，取消返回类型零值、ctx.Err() 和 false；完成与取消同时就绪时 select 可选任一路径。"
	}
	if e.pkg == "pipeline" && e.name == "Stage" && p.name == "concurrency" {
		return "此阶段的并发 worker 数；非正值使用 core.IO()。串行模式忽略此值，Parallel 的 Worker 覆盖阶段值；有限输入路径最多使用输入项数个 worker，Flow 常驻指定数量。"
	}
	if e.pkg == "pipeline" && e.name == "Timeout" {
		return "一次终端执行整体的 context 超时，正数生效，非正值不限时；所有阶段共享此期限，Flow 忽略该配置。不会强制中止业务函数。"
	}
	if p.name == "concurrency" && (e.pkg == "sliceops" || e.name == "ForEachPool" || e.name == "MapPool") {
		return "任务并发上限；非正值采用 core.IO()。分片包装的总并发还受分片数及各片配置影响，外部池执行则使用池的 worker 容量。"
	}
	if e.name == "NewMapResult" && p.name == "data" {
		return "结果 map 的直接引用，不复制；nil 输入创建可写的空 map。Data 返回同一引用，修改它会影响结果对象，须自行同步。"
	}
	if e.pkg == "pipeline" && e.name == "OnResult" {
		return "接收阶段名及该次阶段 Result 的同步通知；并行/流模式可并发调用，nil 清除回调。回调阻塞会延迟任务，panic 被转为该项 PanicError 并替换原错误。Flow 不使用本配置。"
	}
	if e.pkg == "sliceops" {
		if e.name == "Chunk" && e.receiver == "*Slice[T, R]" && p.name == "chunkSize" {
			return "每个子切片的最大长度；非正值或空输入返回 nil。子切片共享当前底层数组，不修改 Slice 自身的切片长度。"
		}
		if p.name == "pred" && strings.HasPrefix(p.typ, "func") {
			desc := "元素谓词：true 表示匹配，Filter 保留匹配项，DeleteFunc 删除匹配项；查找返回首个/末个匹配项，计数统计匹配数。"
			if strings.HasPrefix(e.receiver, "*sliceBase[") {
				desc += "并行策略下，Filter/DeleteFunc 在长度至少 1000 时并发调用，Contains/All/Any/Count 在长度至少 100 时并发调用；其他查找同步执行。"
			} else {
				desc += "旧 Slice 数据操作同步执行。"
			}
			return desc + "纯数据操作不自动恢复 panic，不受策略 Timeout/FailFast 控制；回调须并发安全且不 panic。"
		}
		if (e.name == "Chunk" && e.receiver == "" || e.name == "Chunked") && p.name == "chunkSize" {
			return "每个子切片的最大长度；非正值或空输入返回 nil。末批可能不足此长度，子切片共享输入底层数组。"
		}
		if (e.name == "ChunkN" || e.name == "ChunkedN") && p.name == "n" {
			return "用于计算子切片长度 ceil(len/n) 的目标份数；非正值或空输入返回 nil。实际份数可能小于 n，子切片共享输入底层数组。"
		}
	}
	if e.pkg == "maps" && (e.name == "RemapKeys" || e.name == "RemapValues" || e.name == "ToSlice") && strings.HasPrefix(p.typ, "func") {
		return "同步接收键和值并产生签名所示的新键、新值或切片元素；遍历顺序不确定，panic 直接传播。RemapKeys 发生键冲突时后遍历的值覆盖，不能依赖覆盖顺序。"
	}
	if e.pkg == "maps" && e.name == "Filter" && strings.Contains(p.typ, "context.Context") {
		return "同步接收 context、键和值的谓词；true 将该条目加入新容器，false 跳过，不删除原 map 的条目。正数策略 Timeout 为每次回调设置期限；回调须自行检查 context，panic 直接传播。"
	}
	if e.pkg == "maps" && e.name == "Reduce" && strings.HasPrefix(p.typ, "func") {
		return "同步接收 context、当前累积值、键和值，返回新累积值及错误；map 遍历顺序不确定，遇错误立即返回该次累积值及错误，panic 直接传播。正数 Timeout 对每次回调设置期限；Worker/Shards/FailFast 不改变聚合执行。"
	}
	if p.name == "m" {
		switch e.name {
		case "From", "MapFrom":
			return "待复制的原生 map；复制键值形成新容器，引用类型值内部仍共享。nil 输入创建可写的空 map。"
		case "FromRef", "MapFromRef":
			return "要直接持有的原生 map；非 nil 时双方修改互相可见，nil 时创建可写的空 map。共享访问中有写入时须同步。"
		case "Invert", "RemapKeys", "RemapValues", "ToSlice":
			return "同步读取的 Map 容器，指针不能为 nil；转换生成新容器/切片，但引用类型内容仍为浅拷贝，map 遍历顺序不确定。"
		}
	}
	if e.name == "NewMapResult" && p.name == "err" {
		return "保存在结果对象中的汇总错误；nil 表示没有汇总错误，通过 Error/Err 检查，不会在构造时 panic。"
	}
	if (e.name == "NewMapChain" || e.name == "NewMapChainWith" || e.name == "NewMapRunner") && p.name == "data" {
		return "直接持有的输入 map 引用，不复制。nil 可读取和遍历，但写入条目的链式操作会 panic；需写入时传入已 make 的 map。"
	}
	if (e.name == "ResultCallback" || e.name == "WithResultCallback") && strings.HasPrefix(p.typ, "func") {
		if e.pkg == "pool" {
			return "接收已进入队列登记路径的任务结果；nil 清除回调。同步且可能并发，panic 被记录；普通 Submit/TrySubmit 的预检查拒绝或背压直接拒绝不通知此回调。需要每次提交的完整终态时使用 SubmitWithCompletion，并检查提交错误；不得在通知中同步 Wait/Close 本池。"
		}
		if e.pkg == "group" {
			return "接收记录结果路径的同步通知，nil 清除；worker、取消或拒绝路径可并发触发，但最早的 waited/waiting 预检查直接追加错误而不通知。不能按回调数统计全部尝试；须并发安全、快速返回，panic 被记录，不在通知中 Wait/Close 本组。"
		}
		return "接收一个任务终态 Result 的通知函数；可能由 worker 或提交拒绝路径并发调用。检查 Err，避免阻塞和共享状态竞态；不要在此同步 Wait/Close 当前执行器。nil 清除回调。"
	}
	if e.name == "Run" && strings.HasPrefix(p.typ, "func") && (e.pkg == "pool" || e.pkg == "group" || e.pkg == "shard") {
		return "同步接收本次执行器和上下文（以签名为准）的作用域函数；其返回后触发关闭，返回的 error 向外传播。需处理全部任务时在函数内等待并检查结果，避免把执行器带出作用域继续使用。"
	}
	if strings.Contains(e.receiver, "TaskBuilder") && e.name == "Bounded" {
		return "共享本构建器的终端调用可同时运行的最大任务数；非正值使用 core.IO()。每次调用 Bounded 都创建新执行器，因此应配置一次并复用构建器。"
	}
	if e.pkg == "group" && (e.name == "GoAt" || e.name == "GoAtWithTimeout") && p.name == "index" {
		return "结果切片的零起始索引；负值返回 ErrInvalidIndex。稀疏索引会分配到 index+1 长度并留下未占用槽位；输入顺序需要不同且连续的索引。"
	}
	if e.pkg == "pool" && strings.HasPrefix(e.name, "SubmitAt") && p.name == "index" {
		return "本池结果的零起始索引；负值或大于 core.MaxPoolIndex 时返回 ErrInvalidIndex。需稳定排序时使用唯一连续索引；稀疏索引增加存储成本，MaxResults 可限制该位置是否保留。"
	}
	if e.pkg == "shard" && (e.name == "SubmitAt" || e.name == "GoAt") {
		if p.name == "shardIdx" {
			return "目标片的零起始索引；越界时按单项默认路由选片：Pool 的 RoundRobin 轮转、Hash 或未知策略选第 0 片，Group 轮转。结果按分片合并，该值不表示合并后结果索引。"
		}
		if p.name == "index" {
			return "目标分片内部的结果索引；负值被拒绝，Pool 另外限制为不大于 core.MaxPoolIndex。稳定排序需在片内使用唯一连续索引；各片索引不是全局索引。"
		}
	}
	if e.name == "GetShard" && (p.name == "idx" || p.name == "index") {
		return "目标分片的零起始索引；负值或不小于分片数时返回 nil。"
	}
	if e.pkg == "group" && strings.Contains(e.receiver, "MultiGroup[") && e.name == "GoKeyed" && p.name == "key" {
		return "无符号64位路由值，直接按 key%分片数选片；所有 uint64 值均合法，不额外哈希、不推进普通 Go 的轮转。字符串或业务ID需要哈希分布时先自行生成稳定值。"
	}
	if strings.Contains(e.receiver, "RingBuffer") && e.name == "FlushN" {
		return "按 FIFO 顺序取出的最大元素数；非正值或超过现有元素数时取出全部，读取后移除相应元素。"
	}
	if e.name == "NewRingBuffer" && p.name == "capacity" {
		return "固定缓冲容量；小于 1 时使用 1024。满时按 overflow 处理，之后不会自动扩容。"
	}
	if e.pkg == "ratelimit" {
		switch {
		case e.name == "AllowN" && p.name == "n":
			return "本次需要消耗的请求/令牌数；非正值直接返回 true。分片版本仅检查轮转选中的一片，不能合并多片余额。float64 参数须为有限值。"
		case e.name == "Resize" && p.name == "newTotalRate":
			return "调整后的总补充额度；非正值忽略。按余数分配至各片且每片至少 1，因此请求值小于片数时实际总额为片数。"
		case e.name == "Rate" && strings.Contains(e.receiver, "TokenBucket"):
			return "每秒补充的令牌数，应为有限正数；零值和负值原样传入构造器，不自动填入 DefaultRate。分片版本对每片应用同一速率。"
		case e.name == "Capacity" && strings.Contains(e.receiver, "TokenBucket"):
			return "最大积压及初始令牌数，应为有限正数；零值和负值原样传入构造器，不自动填入 DefaultCapacity。分片版本对每片应用同一容量。"
		case e.name == "Per":
			return "RateLimiter 速率的时间窗口；非正值不会自动变为 DefaultPer，而是在每个补充 tick 把桶补满。需要每秒速率时显式传 time.Second。"
		case e.name == "Window":
			return "滑动窗口保留请求记录的时长；需要有限窗口限制时传正值。非正值不会自动使用 DefaultWindow，旧记录通常在下次检查时立即过期。"
		case e.name == "Limit":
			if strings.Contains(e.receiver, "Sharded") {
				return "各片合计的窗口请求额度，余数优先分配且每片至少 1；目标小于片数（含非正值）时实际总额为片数。"
			}
			return "窗口内允许的请求数；非正值使正数请求被拒绝，不会自动使用 DefaultLimit。"
		}
	}
	if strings.Contains(e.receiver, "RetryChain") || strings.HasPrefix(e.file, "internal/retry/") {
		switch {
		case e.name == "Backoff" && p.name == "initial", p.name == "initialBackoff":
			return "首次重试前的等待时长；线性模式固定使用此值，指数模式逐次翻倍。非正值不等待，持续失败时可能快速重试。"
		case e.name == "Backoff" && p.name == "max", p.name == "maxBackoff":
			return "指数退避的正数时长上限；非正值表示不设置此上限。链式 Linear 忽略它并使用固定 initial。"
		case e.name == "MaxRetries", p.name == "maxRetries":
			return "首次尝试之后允许的重试次数；负值按 0 处理。总尝试次数最多为 n+1（这里 n 指此参数）；配置限流的链式执行中，限流拒绝也消耗一次尝试。"
		case e.name == "Rate":
			return "RateLimiter 每个 Per 窗口的补充额度，或 TokenBucket 每秒生成的令牌数；非正值使用 10。分片 RateLimiter 分配总额度（每片至少 1），分片 TokenBucket 使用每片额度。"
		case e.name == "Capacity":
			return "TokenBucket 最大积压令牌数及初始令牌数；非正值使用有效 Rate 的两倍。分片模式对每片应用此容量。"
		case e.name == "Burst":
			return "RateLimiter 突发容量；正数启用 burst 构造路径，单片实际容量至少为有效 Rate，非正值使用普通构造路径。"
		case e.name == "Per":
			return "RateLimiter 的补充时间窗口；非正值恢复 1 秒，不影响 TokenBucket 每秒生成令牌的单位。"
		case e.name == "Limit":
			return "SlidingWindow 窗口内最大请求额度；非正值使用 100。分片模式分配总额度，但每片至少 1。"
		case e.name == "Window":
			return "SlidingWindow 计数的时间范围；非正值使用 1 秒。"
		case e.name == "PerCallTimeout":
			return "每次业务调用的 context 超时；正数生效，非正值不限时。业务函数必须响应 context 后返回，超时本身不会启动额外 goroutine 或强制停止业务。"
		case e.name == "Shards":
			return "本次 Execute 内部限流器的分片数量；非正值使用 DefaultShardCount()，1 使用单片，DefaultShards 则关闭分片。尝试仍依次执行，每次 Execute 的额度独立，分片不提供跨调用共享限流。"
		case p.name == "opts":
			return "可选的单次调用超时配置；仅使用第一个元素的正数 PerCallTimeout，其余元素忽略。"
		}
	}
	if strings.HasPrefix(e.file, "internal/sliceops/") {
		switch {
		case e.name == "Replace" && p.name == "i":
			return "替换区间的起点（含）；先将负 i 修正为 0，再将大于修正后 j 的 i 降为 j。修正后仍非法的边界会 panic。"
		case e.name == "Replace" && p.name == "j":
			return "替换区间的终点（不含）；大于长度时修正为长度，负数不修正并会 panic。"
		case e.name == "Insert" && p.name == "idx":
			return "零起始插入位置；负数插到头部，大于长度插到末尾。"
		case (e.name == "Delete" || e.name == "DeleteRange" || e.name == "SliceRange" || e.name == "Slice") && (p.name == "i" || p.name == "idx"):
			if e.name == "Delete" {
				return "待删除元素的零起始索引；越界无操作。"
			}
			return "左闭区间起点；负值修正为 0。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。"
		case (e.name == "DeleteRange" || e.name == "SliceRange" || e.name == "Slice") && p.name == "j":
			return "右开区间终点；超出长度修正为长度。修正后 i>=j 时 DeleteRange 无操作，SliceRange/Slice 清空当前视图。"
		case e.name == "Take" && p.name == "n":
			return "保留的前 n 个元素；n<=0 清空当前切片视图，n>=长度无操作。"
		case e.name == "Drop" && p.name == "n":
			return "丢弃的前 n 个元素；n<=0 无操作，n>=长度清空当前切片视图。"
		}
	}
	return ""
}

func semanticCaution(e entry) string {
	if e.pkg == "async" && strings.HasPrefix(e.file, "internal/") {
		e.pkg = strings.Split(e.file, "/")[1]
	}
	if e.pkg == "async" && e.receiver == "" {
		switch e.name {
		case "DefaultPoolConfig":
			e.pkg, e.name = "pool", "DefaultConfig"
		case "Group":
			e.pkg, e.name = "group", "NewGroupBuilder"
		case "GroupVoid":
			e.pkg, e.name = "group", "NewGroupNoResultBuilder"
		case "GroupMulti":
			e.pkg, e.name = "group", "NewMultiBuilder"
		case "Pool":
			e.pkg, e.name = "pool", "NewBuilder"
		case "PoolMulti":
			e.pkg, e.name = "pool", "NewMultiBuilder"
		case "PoolSharded":
			e.pkg, e.name = "shard", "NewPoolBuilder"
		case "GroupSharded":
			e.pkg, e.name = "shard", "NewGroupBuilder"
		case "Ratelimit":
			e.pkg, e.name = "ratelimit", "NewRatelimitBuilder"
		case "Pipeline":
			e.pkg, e.name = "pipeline", "NewPipelineBuilder"
		case "ExecuteStream", "NewFlow":
			e.pkg = "pipeline"
		case "SafeCall", "SafeCallVoid":
			e.pkg = "core"
		case "Must":
			return "err 非 nil 时在调用方直接 panic(err)，否则返回原 val；不恢复 panic、不记录日志、不复制引用内容。仅适用于初始化等可接受 panic 的路径，生产请求路径应显式处理错误。"
		case "NewMap", "NewMapChain", "NewMapChainWith":
			e.pkg = "maps"
		case "NewMapCap":
			e.pkg, e.name = "maps", "New"
		case "MapFrom":
			e.pkg, e.name = "maps", "From"
		case "MapFromRef":
			e.pkg, e.name = "maps", "FromRef"
		case "Retry", "RetryVoid":
			e.pkg, e.name = "retry", "New"
		case "Task", "TaskVoid":
			e.pkg, e.name = "task", "NewTaskBuilder"
		case "NewBoundedRunnerBuilder":
			e.pkg = "task"
		case "Slice", "SliceWith":
			e.pkg, e.name = "sliceops", "NewSliceBuilder"
		case "ForEachPool":
			e.pkg = "pool"
		}
	}
	if advice := builderCaution(e); advice != "" {
		return advice
	}
	if e.pkg == "group" {
		if advice := groupCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "pool" {
		if advice := poolCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "retry" {
		if advice := retryCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "task" {
		if advice := taskCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "sliceops" {
		if advice := sliceDataCaution(e); advice != "" {
			return advice
		}
		if advice := sliceExecutionCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "shard" {
		if advice := shardCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "ratelimit" {
		if advice := limiterCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "maps" {
		if advice := mapCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "pipeline" {
		if advice := pipelineCaution(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "core" {
		switch e.name {
		case "LogFatal":
			return "同步调用当前全局 logger，使用 Background context 和 Error 级别，然后 os.Exit(1)；不附加 trace_id，不受 TaskFailLogLevel/TraceLogEnabled 过滤，进程退出不执行 defer。后端阻塞会延迟退出；后端 panic/Goexit 则不会到达 os.Exit，不保证异步后端落盘。"
		case "Str", "Err", "Dur", "Any", "Bytes", "Int64":
			return "只构造 LogField，不调用 logger、不分配执行器、不格式化或校验内容。Err 固定使用 error 键，其余键原样保存且允许空/重复键；Any/Bytes/Err 内部引用不复制。构造完成后再修改共享对象会影响后端读取，跨 goroutine 使用须同步；nil 字段如何编码由后端决定。"
		case "SetTraceLogEnabled":
			return "只控制新 TraceID 或显式非空覆盖时的日志 context 注入；已有值不补做注入。false 不关闭日志，也不屏蔽 LogCtx 从 context 自动追加 trace_id；原子设置不迁移已有 context 或后端状态。"
		case "LogTaskFail", "LogTaskFailCtx":
			return "只有 TaskFailLogLevel 的 Error/Warn/Info/Debug 四值会输出，Silent 和范围外值不输出；该过滤不作用于普通日志入口。LogTaskFail 即使 err 为 nil 也照常构造 error 字段，不代替业务错误判断。同步调用当前全局 logger，自动 trace 字段在调用者字段前，不去重；后端须并发安全、快速返回且不 panic。"
		case "LogCtxDebug", "LogCtxInfo", "LogCtxWarn", "LogCtxError":
			return "同步调用当前全局 logger，不从 ctx 选择 logger。nil ctx、缺失/空/非 string TraceID 不附加字段；非空 string 自动作为第一个 trace_id 字段，调用者原字段随后追加，不去重。TraceLogEnabled 和 TaskFailLogLevel 都不过滤这些直接日志入口；后端 panic 向外传播，异步保存引用字段需自行复制。"
		case "CPU", "IO", "IOMulti", "WithConfig":
			return "仅返回并发度建议值，不分配 worker 或测量实际负载。CPU 使用 NumCPU，IO 使用其两倍，IOMulti 的非正倍数按 2；均不依据 GOMAXPROCS。WithConfig 仅对非正配置回退 IO，不限制正值上界；生产需按资源预算与实际任务压测。"
		case "SafeCall", "SafeCallVoid":
			return "在调用方同步执行一次回调，不启动 goroutine、不检查 context 取消，也不设置期限；nil 或已取消 context 均原样传入。普通返回的值和错误原样保留，非 nil 错误可以伴随非零值；业务或 nil 函数调用的 panic 包装为 PanicError 并记录日志，有值版本 panic 时返回零值。只捕获同一 goroutine 的 panic，业务自建 goroutine 的 panic 不在范围内；runtime.Goexit 仍执行 defer 并结束当前 goroutine，不转换成错误返回。日志后端若 panic 仍会向外传播。"
		case "MergeCancel":
			return "oldCancel 非 nil 时返回闭包，先执行 newCancel 再执行 oldCancel；此时 newCancel 必须非 nil。oldCancel 为 nil 则直接返回 newCancel（可为 nil）。同步执行，不添加 once、不恢复 panic；newCancel panic/Goexit 时 oldCancel 不执行，oldCancel 异常时 newCancel 已执行。重复/并发调用是否安全取决于注入函数；context 原生取消函数支持重复调用。"
		case "DefaultAutoScaleConfig":
			return "每次返回独立配置指针，数值是起点建议，不根据业务动态测算。修改后需校验 worker 上下界、阈值及扩缩倍数，Normalize 只补齐非正值；通过构建器保存指针时须在终端创建前完成配置。"
		case "GetTraceIDKey", "GetTaskFailLogLevel", "GetTraceLogEnabled":
			return "读取当前全局配置，不修改实例；并发读取同步，但先读再设置或连续读取多个字段不是原子事务。trace key 若运行中更换，已有 context 的旧值不会迁移；日志设置影响后续全库日志调用。"
		}
		if strings.HasPrefix(e.receiver, "Result[") {
			return "Ok 只检查 Err 是否为 nil，零值 Result 也返回 true；Error 原样返回 Err，不清除同时保存的 Value。Occupied 是否有意义由产生结果的入口决定。IsPanic 使用 errors.As，支持包装/多错误链；Err 是类型化 nil 的 *PanicError 时也为 true，提取后仍须判空。String 优先格式化非 nil Err，否则格式化 Value；对整个 Result 使用 %+v 不会把 '+' 传给内部错误，需提取 PanicError 再输出栈。"
		}
		if strings.Contains(e.receiver, "RingBuffer[") || e.name == "NewRingBuffer" {
			switch e.name {
			case "Pop":
				return "按FIFO取出原元素并清零对应内部槽位，空时返回零值和false；不深拷贝引用内容，返回对象仍可被调用方持有。操作后固定容量和丢弃计数保持；不与其他调用组合成原子事务。"
			case "Flush", "FlushN":
				return "按FIFO取出并清零相应内部槽位，返回独立结果切片；空时返回非nil空切片。FlushN非正（含MinInt）或大于现有数量时取全部，MaxInt不会按该值分配。元素为浅拷贝，返回引用仍由调用方持有；取出不清除Dropped计数，Reset才清零。"
			case "Reset":
				return "清零全部内部槽位、head/tail/size及丢弃计数，保留固定存储数组、容量和策略；重复调用可用，之后继续Push。不会改写已返回的结果切片或深度清理元素对象；旧引用仍存活时不保证对象回收。"
			}
			return "使用 NewRingBuffer 初始化，零值不是已分配的缓冲；使用后不要复制含锁实例。满缓冲只在 Drop 时覆盖并计数，其他策略（含未知值）立即拒绝，OverflowBlock 在此不阻塞。每次操作分别同步，Len/IsFull 后再 Push 不是原子组合；返回的元素仅浅拷贝，Reset 同时清零丢弃计数。"
		}
		if e.name == "NewTraceID" {
			return "输出为 32 位十六进制，正常使用 crypto/rand；实现保留随机读取失败后使用时间戳和进程内计数器的回退分支。用于日志关联，不承诺绝对唯一，也不作为认证令牌。"
		}
		if e.name == "WaitContextImpl" || e.name == "WaitTimeoutImpl" {
			return "内部等待辅助入口，当前Pool/Group不调用它，不能代替执行器的提交门控。WaitGroup与waited必须有效；cancel可nil，非nil时须支持重复及并发调用，WaitTimeoutImpl的返回与AfterFunc可能各调用一次。cancelAll/copyResults/onDone须非nil、不panic且及时返回，快照自行同步。成功须等WaitGroup及全部onDone按顺序完成；超时可在onDone仍阻塞时返回，但同步取消/快照仍会延迟返回。仅返回路径更新waited/submitGuard并复制一次；没有一次性等待门控。提前返回后独立等待goroutine继续，监视每五分钟告警，到配置时长（非正按三十分钟）记Error后退出，不终止任务或取消后续onDone；阈值与告警同时就绪时日志先后不保证。"
		}
		if strings.Contains(e.receiver, "PanicError") || e.name == "NewPanicError" {
			return "NewPanicError 捕获创建处当前 goroutine 的栈，保留原 Value 引用，不深拷贝；Value/Stack 是可变公开字段，后续格式化读取当前值，共享修改须同步。Unwrap 始终返回 nil，即使 Value 本身为 error 也不加入错误链。Format 对各动词均写出 panic: 加原值，仅 'v' 且有 '+' 时追加换行和当前 Stack；忽略宽度/精度，不为 %q 加引号或转义。需转义先 Error() 再格式化字符串，生产中仅向受控日志输出栈；Result 的 %+v 不转发此标记。"
		}
		if strings.Contains(e.receiver, "Logger") || strings.HasPrefix(e.name, "Log") || e.name == "SetLogger" || e.name == "GetLogger" || strings.Contains(e.file, "log.go") {
			if e.name != "LogFatal" {
				return "日志调用同步执行且不恢复后端 panic，后端须并发安全、及时返回。SetLogger(nil) 恢复静默实现；类型化 nil 指针仍是非 nil 接口，需由实现支持。LogField 不深拷贝引用内容，异步日志后端须管理数据快照；日志字段避免敏感信息。"
			}
		}
		if strings.HasPrefix(e.name, "Set") || strings.HasPrefix(e.name, "GetDefault") || e.name == "GetSubmitTimeout" || e.name == "GetSubmitTimeoutValue" || e.name == "GetMaxCleanupDuration" {
			if e.name != "SetTraceIDKey" {
				return "这是全局配置/读取入口，建议在应用启动时固定；单次读写同步不代表多字段配置的事务一致性。已有实例是否更新取决于该字段的读取位置，不能把保存的全局值当作所有实例的实时设置。"
			}
		}
	}
	if e.pkg == "sliceops" && strings.HasPrefix(e.receiver, "*sliceBase[") {
		switch e.name {
		case "Filter", "DeleteFunc":
			return sliceFilterCaution(false)
		case "Replace":
			return "替换左闭右开区间，可能复用输入底层数组，须同步共享访问。先把负 i 改为 0、过大的 j 改为长度，再把 i 降至 j；负 j 不修正，最终切片边界非法会 panic。返回自身，调用者保存的旧视图不会自动更新，不应用策略 Timeout/FailFast。"
		case "Sort", "StableSort", "Reverse", "Shuffle", "Compact", "Append", "Prepend", "Insert", "Delete", "DeleteRange", "Take", "Drop", "SliceRange", "Repeat":
			return "修改当前构建器持有的切片或视图，可能写入/复用与原输入共享的底层数组，不能与共享输入的其他访问并发修改。需要隔离时先浅拷贝输入，引用类型元素还需按业务复制。纯数据操作不应用 Timeout/FailFast，也不恢复回调 panic；缩短视图不保证释放旧数组中的元素引用，需要释放多余容量时评估 Clip 的复制开销。"
		case "Items":
			return "返回当前内部切片引用，不复制；修改元素直接影响构建器。保存的旧视图不会随后续扩容或替换自动更新，引用内容共享的所有访问须同步；需独立替换元素时使用 Values/Clone。"
		case "Values", "Clone":
			return "复制当前切片的元素，替换返回切片元素不会改变构建器；指针、map、切片等元素的内部内容仍共享。复制过程中不能并发修改构建器或其输入；空结果由 make 创建，不承诺返回 nil。"
		case "Chunked", "ChunkedN":
			return "不修改内部切片，但返回的各子切片共享当前底层数组，修改子切片元素会影响输入。非正分块参数或空输入返回 nil；ChunkedN 的实际份数可能小于目标份数。共享访问须同步。"
		case "Split":
			return "同步逐项调用谓词并按原顺序追加到两个新切片，不修改原切片；引用类型元素内部仍共享。空分组可能为 nil；谓词 panic 直接传播，不应用策略 Timeout/FailFast，也不会自动并发。"
		case "Contains", "All", "Any", "Count":
			return "并行策略且长度至少 100 时并发调用谓词，谓词应为并发安全的纯函数；panic 不恢复，不能依赖调用次数/顺序。空切片 All 为 true、Contains/Any 为 false、Count 为 0。不应用 Timeout/FailFast，不与数据修改并发。"
		case "Index", "Find", "FindLast", "First", "Last", "Max", "Min", "IsSorted", "BinarySearch", "Len", "IsEmpty":
			return "这些查询同步读取当前切片，策略 Worker/Timeout/FailFast 不改变执行；不能与数据修改并发。返回元素是浅拷贝，未找到时应检查 bool 或 Index 的 -1；BinarySearch 返回插入点，必须先按同一比较规则排序。比较/谓词 panic 直接传播。"
		}
	}
	if e.pkg == "maps" && (strings.HasPrefix(e.receiver, "*Map[") || strings.HasPrefix(e.receiver, "*MapBase[")) {
		switch e.name {
		case "Get", "GetOrDefault", "Has", "Len", "IsEmpty":
			return "只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。"
		case "Keys", "Values", "Clone":
			if strings.HasPrefix(e.receiver, "*MapBase[") {
				return "返回新建的键或值切片，空输入返回非 nil 空切片；map 顺序不确定，分别调用 Keys/Values 不能按位置配对。引用类型元素内容仍共享；复制期间不能与共享 map 的写入并发，不应用执行策略。"
			}
			return "复制键、值或键值对，不提供确定的 map 遍历顺序；分别调用 Keys/Values 不能按位置配对。复制为浅拷贝，引用类型值内容仍共享；复制期间不能与共享 map 写入并发，Clone 对内部 nil map 返回 nil。"
		case "AsMap":
			return "返回底层 map 的直接引用，不复制，双方写入互相可见；此引用的访问和容器方法共用同一同步约定。有并发写入时必须同步所有读写，零值 Map 的内部 map 为 nil，需要写入应使用构造器初始化。"
		case "Delete", "GetAndDelete", "Clear":
			return "原地修改底层 map，FromRef 或 AsMap 取得的引用同步看到删除；内部 nil map 删除/清空无操作，但接收者指针须有效。不提供并发锁，须同步共享访问；GetAndDelete 的 bool 区分缺失与零值。NaN 或包含 NaN 的非自反键无法按键定位删除，Clear 可清除包括这些键在内的全部条目，但不承诺立即释放底层预分配容量。"
		case "GetOrSet":
			return "键不存在才写入默认值，bool 为 true 表示发生新写入；这是无锁复合操作，不能作为并发去重或初始化保障。缺失键写入要求内部 map 非 nil；返回的值及默认值内部引用仍共享。NaN 或包含 NaN 的非自反键每次查找均不命中并新增条目，需要稳定检索时先校验或规范化键。"
		case "Filter", "FilterKeys", "FilterValues", "Reject", "MapValues", "ForEach":
			return "回调在调用方同步执行，map 遍历顺序不确定，panic 直接传播。Filter/Reject/MapValues 原地修改，FromRef/AsMap 引用同步可见；ForEach 无提前终止返回值。不要在回调中增加或删除正在遍历的键，也不要与共享 map 的其他写入并发。"
		}
	}
	if e.pkg == "maps" && strings.HasPrefix(e.receiver, "*MapResult[") {
		return "Error/Err 读取保存的汇总错误，数据仍可能包含部分结果，应按业务决定是否使用。Data 返回内部 map 引用；Keys/Values 返回新的浅拷贝切片且顺序不确定，不能按位置配对。NaN 等非自反键即使来自 Keys，也不能用 Data()[key] 回查，须直接 range Data 同时读取键值。结果容器没有并发锁，不要与 Data 的写入并发访问。"
	}
	if e.pkg == "pool" && strings.HasPrefix(e.receiver, "*Pool[") {
		switch e.name {
		case "WithResultCallback":
			return "普通提交在预检查或背压检查中被直接拒绝时，只记录主结果/计数，不调用此回调，也不发往结果流；入队等待失败、关闭清理和实际执行路径才通知。不要据回调次数推断全部提交次数；需要每次提交的终态使用 SubmitWithCompletion。回调可并发，panic 被记录且不改变原结果。"
		case "SubmitWithCompletion", "SubmitWithCompletionTimeout", "SubmitAtWithCompletion":
			return "complete 每次提交最多调用一次，覆盖预检查拒绝、背压丢弃及执行结果，可能在提交调用返回前同步执行。OverflowDrop 的提交 error 可为 nil，complete 的 Err 仍为 ErrQueueOverflow；不能只靠返回 error 计成功。回调 panic 被恢复并记录，不更改已保存结果；业务 callback 和 complete 都须及时返回，不在其中 Wait/Close 本池。"
		case "Reset":
			return "返回同一 Pool 指针，在实例内部重建任务队列、worker、流和环形缓冲；不是新分配的独立池。要求本批已进入 waited 状态，并等待超时后的后台任务收尾。保留并发度、任务/提交超时、排队/溢出策略、结果上限、回调、FailFast 及已启用的 AutoScale 配置；清零结果/计数并重建 context。须重新取得流通道，不与提交/等待/配置并发调用。"
		case "ResetWait":
			return "只有已经进入 waited 状态才复位；等待未结束任务后清空本批结果、计数和环形缓冲，重建已启用的流通道，不重启 worker。不会重新打开已关闭的池，也不重建 Wait 已取消的池 context；下一批需要时重新配置 context/FailFast。须外部协调，不与提交或等待并发。"
		case "Wait", "WaitContext", "WaitTimeout", "WaitAndClose":
			return "每批 Wait/WaitContext/WaitTimeout 共用一次性等待入口，重复调用返回 nil（带 bool 时为 false）。等待后禁止提交，下一批先复位；超时/取消只请求任务退出，后台清理继续。主结果按索引保留，MaxResults>0 时追加仍在环形缓冲的溢出项，可能已丢失；MaxResults==0 的镜像不会被 Wait 排空。当前任务先递减 Busy/Active 再释放等待登记，无并发复位或新批次时，成功等完本批的业务计数已归零；提前返回不保证。WaitAndClose 在等待后关闭池；其他等待方法不关闭 worker，需 Close 或 Run 管理。"
		case "Errors", "FirstError", "JoinErrors", "Values":
			return "只读取主结果存储，不包含上限外环形缓冲结果；应先完成等待。FirstError 按主结果索引选择，可能遗漏上限外业务失败；完整错误统计应检查提交错误并用 SubmitWithCompletion 收集每次终态。Values 同时要求 Occupied 且 Err==nil，元素浅拷贝；HasError/FailCount 的统计范围与保留错误列表不同。"
		case "Active", "Busy", "Pending", "Size", "TotalCount", "OkCount", "FailCount", "HasError", "Stats":
			return "统计分字段读取，不能用不同字段的差值推导同一瞬间的状态；Size 为配置 worker 数，Active 为已取得任务的 worker 数，Busy 为执行回调的 worker 数，Pending 为等待中的任务数。失败统计不受结果保留上限限制，可能包含拒绝/取消；Stats 读取非原子配置时须避免并发修改该配置，Reset/ResetWait 清零批次计数。"
		case "Close", "CloseAndWait", "CloseAndWaitTimeout", "CloseByIdle":
			return "关闭提交入口后等待 worker 和已登记任务收尾，排队任务可能执行或被记录为 ErrPoolClosed。不要在所属任务/结果回调中同步关闭本池；忽略取消且不返回的业务可能阻塞关闭。CloseByIdle 每 100ms 检查 Active/Busy 是否为零，不检查 Pending，轮询可越过期限且同步关闭不受其限制。CloseAndWaitTimeout 首次超时返回的 workerDone 只标志 worker 退出，完整队列清理可再调用 Close 等待；重复调用也可能超时。"
		}
	}
	if e.name == "Run" && (e.pkg == "group" || e.pkg == "pool" || e.pkg == "shard") {
		return "作用域回调返回后自动关闭，但 Run 的 error 仅来自该回调，不自动汇总任务失败。须在回调内检查提交错误并 Wait 后检查结果或 FirstError；回调 panic 直接传播，defer 仍执行关闭。不要在 Run 返回后继续使用实例提交任务。"
	}
	if e.pkg == "ratelimit" {
		if strings.Contains(e.receiver, "AdaptiveRateLimiter") && (e.name == "RecordOk" || e.name == "RecordFailure") {
			return "只记录业务完成情况，不获取或释放许可；每次成功 Acquire 仍须配对 Release，每次完成只记录一次。单片累计至少 10 次记录才评估：失败比例>20% 缩容一半且不低于最小值，<10% 按剩余容量的 20%（至少 1）扩容，其他比例不调整；之后该批计数清零。分片记录使用独立轮转，不绑定获取许可的片。"
		}
		if e.name == "Resize" && strings.Contains(e.receiver, "RateLimiter") {
			return "非正值、相同速率或已关闭时无操作；速率变化时先按旧速率结算经过时间，再把容量改为新速率，原 Burst 不保留。保留尚不足一个令牌的余量，桶满时清除超额；100ms ticker 不重新计时。保持已有未归还许可的容量影响；定时补充型仍会随时间恢复额度，不能当作严格并发限制。分片按片依次调整，观测不是全局原子快照。"
		}
		if e.name == "Release" && e.receiver == "*Token" {
			return "仅为 Token 成功返回的句柄调用；同一指针可重复或并发 Release，只归还一次。不要复制已使用的 Token（包含 sync.Once），也不要再对同一次许可调用所属限流器 Release。"
		}
	}
	if e.pkg == "group" {
		if e.name == "BuildAggregateNoResult" {
			return "用于非 nil 且已完成最终 Wait 的 NoResult；复制结果槽位后仅统计 Occupied 项，首错按槽位顺序选择，不按发生时间。返回结果含稀疏空槽，其长度可能大于 total；结果及元素为浅拷贝。它不等待、不关闭、不修改 Group 计数，也不合并外部 Pool 的其他任务。"
		}
		if e.name == "FillNoResultSkipped" {
			return "仅在非 nil NoResult 的最终 Wait 完成且无活跃任务后使用。total 大于 TotalCount 时先按索引升序填空槽、再追加 ErrSkipped 占位，并增加总数和失败数；非正目标（包括 MinInt）及不大于现有总数的目标均无操作，不缩短或校正已有结果。不会执行业务、补发结果回调或向已关闭的流发送，填补之后通过聚合或错误查询读取。正数目标不设上限，须控制分配预算。"
		}
		if e.receiver == "*NoResult" && (e.name == "WaitTimeout" || e.name == "WaitContext") {
			return "completed 原样为当前结果切片长度，包含稀疏 GoAt 的空槽，不能当成已完成或成功任务数。bool 只标识等待完成，不表示业务成功；取消后请求任务退出但清理可继续，最终再 Wait 后读取完整计数/错误。不要在同组业务或结果回调中等待自身。"
		}
		if strings.Contains(e.receiver, "Builder") {
			switch e.name {
			case "Build":
				return "每次创建独立 Group；终端使用时按本次构建器字段配置，正数 Timeout/SubmitTimeout/Streaming 才应用。调用方最终 Wait/Close，普通 Go 的结果顺序不保证；注入的外部池仍由外层所有者关闭。不要并发修改构建器或与 Build 竞争。"
			case "AutoScale", "DefaultAutoScale":
				return "构建器保存配置指针，Build 时由 EnableAutoScale 复制并补齐默认值；Build 前对该指针的修改会影响后续构建，须同步。AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置；并非 Default 前缀都表示关闭。"
			case "Pool", "DefaultPool":
				return "只影响后续 Build/Run；Pool(nil) 与 DefaultPool 清除外部池引用。外部池归调用方管理，Group 的等待和 Reset 不负责复位外部池；外部池自身的超时和队列配置仍可能影响执行。"
			default:
				return "原地配置当前构建器并返回自身，只影响后续 Build/Run；须在终端方法之前完成配置，不能与构建或其他配置并发修改。同一字段以后一次设置为准；Logger/DefaultLogger 直接修改全局日志器。"
			}
		}
		switch e.name {
		case "Close", "Wait":
			if strings.Contains(e.receiver, "MultiGroup[") {
				return "逐片等待已接受任务、worker 及流式收尾，全部返回后取消分片共享 context；Wait 按分片序号拼接结果。不会先强制取消正在执行的业务，不返回的回调可能阻塞；等待开始后禁止新任务。MultiGroup 没有整体 Reset 方法，新批次应新建实例。"
			}
			return "Group.Close 调用 Wait，等待已接受任务、worker 及流式收尾；不先强制取消运行中的业务，回调不返回可能一直等待。开始等待后禁止新任务；超时等待后可再 Wait 取得最终结果，复用先在无活跃任务且等待完成时 Reset。"
		case "Reset":
			return "单 Group 要求已经完成等待且无活跃任务，否则返回错误并保持当前实例。清除结果、计数、context/FailFast 和 AutoScale；保留并发度、任务/提交超时、结果回调、流容量以及外部池引用。须在外部同步下单独调用，不与提交/等待竞争；重新取得 StreamResults，新批次重新设置 context/FailFast/AutoScale。"
		case "Active", "Busy", "Worker", "OkCount", "FailCount", "HasError", "Stats":
			return "Active 包含等待入队、排队和执行中的已登记任务，Busy 只统计执行中任务，Worker 为当前配置并发度。成功/失败计数与结果记录可能分时更新，分别调用或 Stats 的多个字段不是事务一致快照；读取 Stats 时不要并发修改非原子配置。分片汇总还会逐片读取。"
		case "Values", "Errors", "FirstError", "JoinErrors":
			return "先完成 Wait 再读取完整结果，提前读取只是当时快照。单 Group.Values 同时检查 Occupied 与 Err==nil，返回元素浅拷贝；FirstError 按结果存储顺序选择，不保证最先发生或最早提交。JoinErrors 使用 errors.Join，保留可供 errors.Is/As 检查的错误链。"
		case "Shard", "DefaultShard":
			return "第 0 片复用原 Group；其余片只复制并发度、任务/提交超时、context/FailFast 关系、流容量及结果回调，不复制外部 Pool、AutoScale 或已有结果。片数非正或为 1 时只包装原实例，DefaultShard 使用 max(2,GOMAXPROCS)。分片应在提交前完成，并统一关闭全部分片。"
		case "TotalCount", "TotalTaskCount":
			return "计数包含进入 Group 提交预检查的尝试，即使预检查拒绝也可能增加；负索引在预检查前拒绝而不计数。不是成功入队或执行完成数量，MultiGroup 按片求和且不是跨片原子快照。"
		case "StreamResults":
			return "Group 未启用流时返回 nil；慢消费时溢出暂存，Wait 最终刷新仍写不进 channel 的项会丢失。MultiGroup 每次调用创建新的合并转发器，重复调用会竞争分片流；应只调用一次并持续消费至关闭，完整结果使用 Wait 返回值。"
		}
	}
	if strings.Contains(e.receiver, "Flow[") {
		switch e.name {
		case "ErrCount":
			return "统计阶段回调的错误、panic 或业务 Goexit 次数，同一元素可在多阶段重复计数；后续阶段成功也不扣减历史计数。不是最终失败元素数量，SkipOnError 的直接透传不重复计数。"
		case "SubmitCount":
			return "只计成功入队的元素，不是提交尝试数或最终结果数；取消管道可能丢弃已接受元素的后续转发或结果。"
		case "Stats":
			return "各字段分别读取，非事务一致快照。TotalError 为阶段错误、panic 或业务 Goexit 次数，TotalResult 为成功写入结果通道的次数；Closed 在首次 Close 开始时置 true，不代表关闭已完成。"
		case "Close":
			return "首次调用等待在途提交与阶段 worker，期间与 Results 消费者竞争排空结果；须合并两路结果，消费者及关闭回调可能并发执行。后续调用立即返回 nil，不等待首次关闭完成。业务不返回时可永久阻塞。"
		case "Results":
			return "结果按完成顺序输出，Result 不包含提交序号，应在元素中携带关联标识。首次 Close 同时排空此通道，须处理其返回的残留结果；外部取消可能丢结果，通道需 Close 才关闭。"
		case "Submit", "SubmitBatch":
			return "提交 context 仅限制等待入队，处理使用管道 context；同一时刻取消和入队均就绪时可成功入队。序号不保证入队或完成顺序，失败可留间隔；批量遇首个提交错误停止，已接受元素继续处理。"
		}
	}
	if e.name == "BindRetryToWorker" {
		return "只对 ErrSubmitTimeout（含包装错误）重试提交，负次数按 0，非正退避不等待；只在后续还可尝试时退避。每次提交前检查父 context，不中断阻塞的后端 Submit。提交成功立即返回 nil，即使后端刚取消父 context；其他提交错误原样返回，最终超时包装该错误，尚有尝试时下一轮/退避才返回父取消。后端必须约定超时意味着未接受任务，否则重提可能重复执行；不主动调用业务或等待完成，任务错误与生命周期须另管。后端 panic 直接传播。"
	}
	if e.pkg == "retry" && e.name == "WithRetry" {
		return "同步立即重试，负次数按 0，最多 maxRetries+1 次，无退避、context 或超时；最后一次失败后在递增计数前退出，最大 int 的预算不会因循环计数溢出而多执行。panic 转为 PanicError 后继续重试，最终错误包装最后一次错误。须限制次数并保证幂等，需要可取消等待时使用 RetryWithBackoff 或 RetryChain。"
	}
	if e.pkg == "pool" && e.name == "WithRingBuffer" {
		return "主结果仍独立保留。MaxResults>0 时只写上限外结果，Wait 追加未取出的溢出项；MaxResults==0 时镜像全部结果，Flush 与 Wait 可能重复取得同一任务，Wait 不排空镜像缓冲。满时 Drop 覆盖最旧项，Block/Error 拒绝新写入而不阻塞。"
	}
	if e.pkg == "task" && strings.HasPrefix(e.receiver, "Task[") {
		return "应使用 GoResult/GoResultAct 或对应构建器创建句柄，零值的 Result/Cancel 字段为 nil。构造器提供的 Result 可并发重复调用并缓存实际结果；句柄值副本共享缓存，T 内切片/map/指针引用不深拷贝，后续修改需自行同步。Cancel 只发出 context 信号，不强制中止任务，手动替换函数字段后须自行维护这些约定。"
	}
	if e.pkg == "task" && strings.Contains(e.receiver, "AsyncResult[") {
		const handle = "通过 Go/GoAct 或对应构建器创建并共享指针，零值不可用，不要复制包含锁的句柄。返回T只作值复制，切片/map/指针内容共享；缓存锁不保护引用对象内部，后续读写须由业务同步或复制。"
		switch e.name {
		case "Cancel":
			return handle + "优先返回已缓存或可立即读到的实际结果，否则创建1ms定时器等待；定时分支返回类型零值和context.Canceled，不覆盖最终缓存、不取消业务或提前释放业务许可。1ms仅为定时器时长，不保证包含调度及锁等待的总耗时上限；结果和定时器同时就绪不保证分支。业务本身也可能返回context.Canceled；需区分来源时用带ok的WaitContext/WaitTimeout。取消业务使用任务context或Task.Cancel。"
		case "WaitCh":
			return handle + "每次调用另建一个容量为1的通知通道及goroutine，共享同一缓存结果；就绪结果也须等待通知goroutine调度。发送一次后关闭，重复读取须检查ok，关闭后的零值不表示新的任务成功。生产复用一次调用返回的通道；停止接收不取消业务，业务不返回则通知goroutine继续等待。"
		case "Wait", "Values", "Error", "Ok", "IsPanic":
			return handle + "阻塞直到任务返回并共享缓存结果；可并发重复调用，不能以 Ok/IsPanic 作非阻塞轮询。任务忽略 context 时可能永久等待，应为等待另设 WaitTimeout/WaitContext。"
		}
	}
	if e.pkg == "task" && strings.Contains(e.receiver, "Mu[") {
		return "零值可用，使用后不要复制包含互斥锁的 Mu。Append 在调用方锁外同步生成，可重入且 panic 直接传播；已完成的嵌套追加和其他副作用不回滚。Snapshot 不等待仍在生成值的生产者，只复制当前元素槽位，引用内容仍共享；完整收集先由调用方等待生产者。非 nil 容器空快照非 nil，nil 接收者仅支持 Snapshot 并返回 nil。"
	}
	if e.pkg == "task" && e.receiver == "*BoundedRunner" {
		return "使用 NewBoundedRunner 或其 Builder 初始化并复用；不提供 Close/Wait 方法，任务结果由各句柄等待。Busy 为已占用槽位数，包含获得槽位后尚未开始的任务；Available/Busy 分别读取，不是联合原子快照，查询后另行提交仍可能阻塞。上限约束业务许可，不限制调用方等待提交或业务另建的 goroutine；取消不强制回调退出或释放许可。"
	}
	if e.name == "MergeFailFastCancel" {
		return "只合并取消通知，不会单独启用 FailFast。传入非 nil 的并发安全 CancelFunc，先完成配置再提交；通知不能强制中断回调，也不要在通知中等待或关闭当前执行器。"
	}
	if e.pkg == "maps" {
		if e.receiver == "*Map[K, V]" {
			switch e.name {
			case "ForEach", "Filter", "FilterKeys", "FilterValues":
				return "此基础容器操作同步执行，map 遍历顺序不确定，回调 panic 直接传播；过滤原地修改容器，已处理部分不会回滚。不要与其他 goroutine 对共享 map 的访问并发修改。"
			}
		}
		switch e.name {
		case "FailFast":
			return "无池 Map/ForEach/批量操作只在观察到错误后停止后续调度，不派生并取消其他回调的 context；已开始的回调继续执行。Pool 路径使用 Slice Runner 的失败取消策略。Filter/Reduce 不读取此配置。"
		case "Map", "MapTo", "MapToFn", "ForEach", "MapBatch", "ForEachBatch":
			for _, p := range e.params {
				if strings.Contains(p.typ, "context.Context") && strings.HasPrefix(p.typ, "func") {
					return "无池路径不因输入 context 已取消而自动跳过回调；回调须主动检查 context 并返回错误。FailFast 停止后续调度，不取消已开始的回调；池化路径按 Slice Runner 执行。MapBatch 的每批结果使用本批首个条目的键保存，map 遍历顺序不确定。"
				}
			}
		}
	}
	if e.name == "LogFatal" {
		return "同步调用当前全局 logger，使用 Background context 和 Error 级别，原样传递消息与字段；TaskFailLogLevel 和 TraceLogEnabled 不屏蔽此调用。日志后端正常返回后执行 os.Exit(1)，整个进程退出且 defer 不执行；后端若阻塞或 panic，尚未执行退出。只在应用入口的致命失败路径使用。"
	}
	if e.pkg == "core" && e.name == "Normalize" {
		return "原地把非正字段填为默认值，不校验 MinWorkers<=MaxWorkers、浮点有限性或阈值及缩放倍数的相互关系。先校验业务配置，在实例启用前调用，不与其他读写并发。"
	}
	if e.pkg == "core" && e.name == "Ok" && strings.HasPrefix(e.receiver, "Result[") {
		return "只判断 Err==nil；零值 Result 的 Ok 也为 true。Occupied 的设置依入口而异，按该入口约定区分空槽，不把 Ok 当作任务已执行的证明。"
	}
	if e.pkg == "sliceops" {
		if strings.Contains(e.receiver, "SliceResult[") {
			switch e.name {
			case "Values", "First", "Unwrap", "Must":
				return "值筛选只检查 Err==nil，不检查 Occupied；零值空槽也可能作为成功零值返回。取消或提前返回时用 Results 逐项核对槽位与错误，不能仅凭 Values 长度证明全部业务已执行。元素内部引用仍共享，Must 遇到汇总/逐项错误会 panic。"
			case "Results", "FailValues":
				return "返回内部切片引用，不提供修改隔离；应在执行结束后读取。FailValues 在执行结束时收集，批次失败包含该批全部输入，不随 Results 后续修改重新计算；顶层错误也独立保存。修改 Results 可能使当前逐项查询与保存的首错/失败输入不一致，生产按只读处理；须区分 Err/Occupied 并按需复制引用内容。"
			}
		}
		if strings.Contains(e.name, "ForEachChunked") && e.receiver == "" {
			return "此独立函数逐元素调用 fn，但总数、失败数及结果切片按批次计数，一批多个失败只记一个失败批次；外层与批内分别受 concurrency 限制，总在途回调可能超过该值。超时由批内元素共享。Slice 的同名方法使用另一实现，按元素统计。"
		}
		switch e.name {
		case "Filter", "DeleteFunc", "Contains", "All", "Any", "Count":
			if strings.HasPrefix(e.receiver, "*sliceBase[") {
				return "大输入与并行策略可使谓词在 worker 中并发执行；这些数据操作不恢复 panic，worker panic 会终止进程，调用方 recover 不能捕获其他 goroutine 的 panic。使用不会 panic 的谓词，先校验输入；需要业务错误处理时使用 Map/ForEach。"
			}
		case "Values", "Clone":
			return "只复制切片元素；元素为指针、map 或切片时内部引用仍共享。替换元素与修改引用指向的内容不同，需要独立内容时自行深拷贝。"
		}
	}
	if e.name == "EnsureTraceID" || e.name == "GetTraceID" || e.name == "WithTraceID" {
		return "传非 nil context。EnsureTraceID 仅检查当前 key 的值是否为 nil，非 nil 时原样返回，包括空字符串和非 string；它不修复类型。WithTraceID 的空 id 委托 EnsureTraceID，保留已有值；非空 id 才覆盖。启用 TraceLog 时，仅生成/显式覆盖路径调用 logger.With/WithContext，后端必须返回有效 context。GetTraceID 对非 string 值会 panic。自定义 key 在初始化时固定，已有 context 的旧值不会自动迁移；继续传递返回 context。"
	}
	if e.name == "SetTraceIDKey" {
		return "key 必须非 nil 且可比较，否则 panic。设置影响全库后续 context 读写；在启动时统一配置，已有 context 不会把旧 key 的值迁移到新 key。"
	}
	if e.pkg == "retry" && (e.name == "WithTimeout" || e.name == "WithTimeoutVoid" || e.name == "WithDeadline" || e.name == "WithDeadlineVoid") {
		return "同步直接调用 fn，不捕获 panic、不替回调返回 context 错误。即使 deadline 已过或 timeout 非正仍会调用 fn；回调须检查 context 并返回错误，否则仍可能返回成功或无限阻塞。"
	}
	if e.name == "KeyFn" {
		return "仅对批量元素路由生效，须配置 Hash；Hash 且未设置 KeyFn 时使用第 0 片。回调可以并发调用且须稳定，同一片内仍可并发执行多个任务；单项 Submit 不接收元素。"
	}
	if e.pkg == "core" && strings.Contains(e.receiver, "RingBuffer") {
		switch e.name {
		case "Push":
			return "满缓冲时 OverflowDrop 覆盖最旧元素并返回 true；OverflowBlock/OverflowError 均立即返回 false，不等待或抛出错误。调用方自行处理拒绝，零值 RingBuffer 不能替代构造器。"
		case "Reset":
			return "清空元素并将丢弃计数归零，容量与溢出策略保留。不要把 Reset 当成关闭方法；缓冲仍可继续使用。"
		}
	}
	if strings.Contains(e.receiver, "TaskBuilder") && e.name == "Bounded" {
		return "在循环外配置并复用同一个构建器。每次新建 Task().Bounded(max) 都产生独立限额，无法约束跨实例总并发；等槽位发生在调用方，任务超时从回调开始执行时计时。"
	}
	if e.pkg == "task" && (e.name == "WaitContext" || e.name == "WaitTimeout") {
		return "通过任务入口创建并共享句柄指针，零值不可用，不要复制句柄。已缓存或可立即读到的结果优先，包括非正timeout或已取消等待context；进入等待后结果与取消/超时同时就绪不保证选择顺序。false返回零值与ErrTimeout或ctx.Err()；true取得实际业务结果，仍须检查error，业务也可能返回context错误。T内切片/map/指针引用共享，缓存锁不保护其内部，后续修改须自行同步或复制。这里只限制本次等待，不覆盖缓存、不停止任务或提前释放业务许可；取消业务须用任务context或Task.Cancel并由回调协作退出。"
	}
	if e.pkg == "group" && (e.name == "GoAt" || e.name == "GoAtWithTimeout") {
		return "稀疏索引增加结果存储及复制成本；重复索引可能把旧结果移动到其他槽位，不能用它实现任务替换。稳定排序需为每项分配唯一连续索引，等待结束后再读取结果。"
	}
	if strings.Contains(e.receiver, "RetryChain") {
		switch e.name {
		case "Adaptive":
			return "一次 Execute 内的尝试依次执行，每次 Execute 各建独立限流器，不能限制跨调用的总并发。自适应至少需要 10 次成功/失败记录；默认 4 次尝试不会触发调整。需要跨调用调节时在重试链外使用共享 AdaptiveRateLimiter。"
		case "Execute", "ExecuteVoid", "Run", "RunVoid":
			return "每次执行均创建独立限流器，不能约束多次执行的总流量。RateLimiter 执行后会归还令牌；TokenBucket/SlidingWindow 拒绝也消耗尝试次数，最后一次若被限流拒绝则不包装之前的业务错误。有值版本最终失败返回零值；回调须幂等并响应 context，同一构建器不可与配置修改并发。"
		case "Rate", "RateLimiter", "TokenBucket", "SlidingWindow", "Shards":
			return "模式与参数保存至本次构建器，后一次同字段设置覆盖前一次；每个终端调用使用独立限流器。分片 RateLimiter/SlidingWindow 平分总额度且每片至少 1，TokenBucket 采用每片速率与容量。"
		case "DefaultConfig":
			return "只重置退避类型、重试次数、退避和单次调用超时；保留 context、限流模式和限流参数。Default* 方法也遵循最后一次赋值生效。"
		}
	}
	if e.pkg == "ratelimit" && strings.Contains(e.receiver, "Builder") && e.name == "Build" {
		return "Build 读取当前字段；未调用 Default* 不代表已填入其默认值。构建器保存的 context 当前不传入限流实例，阻塞获取须单独传 context；配置结束后再共享实例。"
	}
	if e.pkg == "ratelimit" && (strings.Contains(e.receiver, "TokenBucket") || strings.Contains(e.name, "TokenBucket")) {
		return "速率与容量应为有限的正数；直接构造器不替调用方校验它们。分片令牌桶对每片应用相同速率与容量；AllowN 单次只访问一片，不会跨片汇集额度。"
	}
	return ""
}

// Descriptions here apply only to declarations lacking their own Go comment.
func methodSummary(e entry) string {
	if e.pkg == "maps" && e.name == "FailFast" {
		return "启用快速失败：无池路径遇错后停止后续调度，池化路径取消当前操作的派生 context；均不能强制停止业务函数。"
	}
	if e.pkg == "maps" && (e.name == "DefaultFailFast" || e.name == "NoFailFast") {
		return "关闭快速失败，允许后续条目在前一项业务失败后继续处理；不恢复已取消的 context。"
	}
	if strings.HasPrefix(e.file, "internal/sliceops/") && strings.Contains(e.name, "Chunk") && (strings.Contains(e.name, "Map") || strings.Contains(e.name, "ForEach")) {
		summary := "分块调度并逐元素调用回调；结果及统计单位为元素。"
		if strings.Contains(e.name, "ForEachChunked") && e.receiver == "" {
			summary = "先按批次并发，再在批内逐元素调用回调；结果与统计单位为批次，批次超时由批内全部元素共享。"
		}
		if !strings.Contains(e.name, "Chunked") {
			summary = "将输入分为批次，每批调用一次接收 []T 的回调；结果及统计单位为批次。"
		}
		if strings.HasPrefix(e.name, "Default") {
			summary += "使用默认并发度 core.IO()。"
		}
		if strings.Contains(e.name, "FailFast") || strings.Contains(e.name, "FFTimeout") {
			summary += "失败时取消派生上下文，回调须配合取消。"
		}
		if strings.Contains(e.name, "Timeout") {
			summary += "为单次回调设置超时上下文，不能强制终止业务函数。"
		}
		return summary
	}
	if e.receiver == "WorkerPoolBackend" {
		return "向适配后端提交返回 error 的任务；Submit 自身 error 表示提交结果，业务错误如何获取由后端约定。泛型 Pool 需要适配器。"
	}
	if e.receiver == "" {
		switch e.name {
		case "Submit":
			return "创建默认池并提交一个任务，返回池、提交索引和提交错误；调用方须等待并关闭池。"
		case "SubmitN":
			return "创建默认池并重复提交同一回调 n 次；n 必须非负，每项提交错误在 SubmitResult.Err 中，末尾 error 当前为 nil；调用方负责等待和关闭。"
		case "SubmitSafeN":
			return "调用 SubmitN，记录每项提交错误日志并返回提交状态；Safe 不代表任务成功或自动关闭，n 必须非负。"
		case "SubmitBatch":
			return "创建默认池，按输入顺序逐项提交；每项提交错误在 SubmitResult.Err 中，末尾 error 当前为 nil；调用方负责等待和关闭。"
		case "MapPool":
			return "创建池并映射输入，等待后关闭；返回的池已关闭，末尾 error 当前为 nil，业务错误须逐项检查 Result.Err。"
		case "ForEachPool":
			return "创建池并逐项执行，等待后关闭；按输入顺序返回首个提交或执行错误，独立于结果保留上限。"
		case "WithMultiPool":
			return "创建每片 size 个 worker、共 shards 片的多池，同步调用 fn 并在返回时关闭；需要完成任务时在 fn 内等待。"
		case "NewMapChain", "NewMapChainWith":
			return "创建默认并行 map 构建器并保留输入 map 引用；不复制数据，不能与终端操作并发修改输入。"
		case "NewMapRunner":
			return "以给定 context、map 引用和执行策略创建执行器；输入不复制，各终端方法按其支持范围使用策略。"
		}
	}
	if e.receiver == "*Pool[T]" {
		switch e.name {
		case "Shard":
			return "创建多池，原池作为第 0 片，其余片复制当前配置与 worker 数；shards <= 1 仅包装原池。须在提交前完成配置并统一关闭多池。"
		case "DefaultShard":
			return "以 GOMAXPROCS（至少 2）为片数调用 Shard 创建多池；每片保留原池 worker 数，总 worker 数会增加。"
		}
	}
	if strings.Contains(e.receiver, "ShardedPool") && e.name == "SubmitBatch" {
		return "按配置的轮转或 KeyFn 路由逐项提交，返回与输入对应的分片、输入索引及提交错误；末尾 error 为首个提交错误。执行结果另行等待或消费。"
	}
	if e.receiver == "nopLogger" {
		return "内部空日志实现，忽略输出参数，不执行日志 I/O。"
	}
	if e.receiver == "Logger" {
		switch e.name {
		case "Log":
			return "输出一条指定级别的结构化日志；注入的实现须支持并发调用。"
		case "With":
			return "返回携带指定字段的日志器；字段合并规则由注入实现定义。"
		case "WithContext":
			return "返回日志实现关联或包装的 context；行为由注入实现定义。"
		}
	}
	if strings.Contains(e.receiver, "Map") {
		switch e.name {
		case "Len":
			return "返回当前条目数。"
		case "IsEmpty":
			return "当前 map 没有条目时返回 true。"
		case "Has":
			return "检查键是否存在。"
		case "Get":
			return "返回键的值和存在标记；缺失时返回零值和 false。"
		case "Keys", "Values":
			return "返回键或值的新切片；map 遍历顺序不确定，引用类型元素仍共享底层数据。"
		case "AsMap":
			return "返回底层原生 map 的直接引用；修改该引用会改变当前容器。"
		case "Set", "SetAll":
			return "原地写入键值，同名键覆盖；返回自身，调用方须同步共享 map 的访问。"
		case "Delete":
			return "原地删除指定键，不存在时无操作，返回自身。"
		case "Clear":
			return "原地删除全部条目，返回自身。"
		case "Range":
			return "同步遍历键值，回调返回 false 时停止；顺序不确定。"
		case "FilterKV":
			return "同步原地过滤，保留回调返回 true 的条目，返回自身。"
		case "Filter":
			if strings.Contains(e.signature, "context.Context") {
				return "同步遍历并返回匹配条目的新容器；不修改输入，不使用并行调度或 panic 恢复。"
			}
			return "同步原地过滤，保留回调返回 true 的条目，返回自身。"
		case "Reject":
			return "同步原地过滤，删除回调返回 true 的条目。"
		case "MapValues":
			return "同步原地更新每个键的值；顺序不确定。"
		case "Merge", "MergeMap":
			return "把其他 map 的键值写入当前 map，同名键覆盖；返回自身。"
		case "MergeWithDefault":
			return "仅写入当前 map 不存在的键，保留已有值。"
		case "Find":
			return "返回遍历中首个匹配的键值和存在标记；map 顺序不确定。"
		case "All":
			return "所有条目都匹配时返回 true；空 map 返回 true。"
		case "Any":
			return "存在匹配条目时返回 true；空 map 返回 false。"
		case "Count":
			return "返回同步遍历中匹配的条目数量。"
		case "Reduce":
			return "同步按 map 遍历顺序累积；顺序不确定，遇错误返回该次回调给出的累积值和错误；不恢复回调 panic。"
		}
	}
	if strings.Contains(e.receiver, "MultiPool") {
		switch e.name {
		case "Wait":
			return "等待各片并按分片序号合并结果；并发重复等待立即返回 nil。"
		case "Close":
			return "依次关闭各片并取消共享上下文；回调须配合退出。"
		case "WaitAndClose":
			return "等待并合并分片结果后关闭各片。"
		case "Submit", "TrySubmit":
			return "按 round-robin 选择一片，委托该片的同名提交方法；检查提交错误。"
		case "SubmitKeyed", "TrySubmitKeyed":
			return "按 key 对分片数取模选片；同一个 key 路由到同一片，但不保证串行执行。"
		case "SubmitBatch":
			return "依输入顺序轮转分片提交，返回每项提交状态；返回值不是任务执行结果。"
		case "WaitContext", "WaitTimeout":
			return "各片并行等待同一个截止时间，按分片序号合并快照；false 表示未全部完成或有并发等待。"
		case "GetShard":
			return "返回零起始索引对应的分片；越界返回 nil。"
		case "ShardCount":
			return "返回分片数量。"
		case "TotalActive", "TotalBusy", "TotalPending", "TotalWorkerCount", "TotalFailCount", "TotalOkCount", "TotalCount":
			return "依次读取各片对应的 " + strings.TrimPrefix(e.name, "Total") + " 指标并求和；这是跨片非原子快照，WorkerCount 使用各片 Size。"
		case "StreamResults":
			return "并发合并各片已启用的结果流；片内未启用的流被跳过，全部转发结束后关闭合并通道。持续消费以免合并 goroutine 阻塞，各片流仍可能丢弃满缓冲结果。"
		case "WithTimeout":
			return "为每片设置单任务超时 d；超时依赖回调响应 context，提交前完成配置。"
		case "WithSubmitTimeout":
			return "为每片设置提交等待超时 d；检查提交错误，提交前完成配置。"
		case "WithStreaming":
			return "为每片创建 bufSize 容量的结果通道，非正值使用该片 worker 数的两倍；在提交前配置，再通过 StreamResults 合并消费。"
		case "WithRingBuffer":
			return "为每片创建 capacity 容量的环形缓冲并设置 overflow 策略；不会单独限制主结果存储，总容量按片数累加。"
		case "WithMaxPending":
			return "把正数 n 设置为每片最大等待任务数，非正值不更新已有配置；这不是多池总量限制。"
		case "WithMaxResults":
			return "把非负 n 设置为每片主结果保留上限，0 为不限制，负值不更新；溢出结果依配置通过流、回调或环形缓冲消费。"
		case "WithOverflow":
			return "为每片设置等待队列溢出策略；与每片 WithMaxPending 配合使用。"
		case "Flush":
			return "按分片顺序取出环形缓冲结果，最多取 maxPerShard 个/片；主结果存储不在本方法中读取。"
		}
	}
	switch e.name {
	case "Reduce":
		if e.pkg == "maps" {
			return "按不确定的 map 遍历顺序同步累积，空输入返回 initial；遇错误返回该次回调的累积值及错误。不恢复 panic，策略 Timeout 为每次回调设置期限，Worker/Shards/FailFast 不参与聚合。"
		}
		return "按输入顺序同步累积，空输入返回 initial；遇错误返回该次回调给出的累积值及错误。不恢复 panic，不强制检查或终止未响应取消的回调。"
	case "Context":
		return "替换构建器上下文并确保 trace_id；后续执行使用该上下文。"
	case "ToSerial", "Serial":
		return "保留输入引用和 context，创建串行构建器并重置执行策略；需重新应用原策略设置。"
	case "ToParallel", "Parallel":
		return "保留输入引用和 context，创建默认并行构建器并重置执行策略。"
	case "Logger":
		return "设置全局日志实现，并返回当前构建器；不局限于本实例。"
	case "DefaultLogger":
		return "恢复全局空日志实现，返回自身。"
	case "Worker":
		return "设置后续终端操作的并发度，非正值使用 core.IO()。"
	case "DefaultWorker":
		return "显式将 worker 配置恢复为 core.IO()。"
	case "Pool":
		return "注入外部池，调用方保留生命周期责任；结果类型必须匹配。"
	case "PoolAuto":
		return "注入外部池并转移生命周期责任，终端操作完成后关闭池。"
	case "DefaultPool":
		return "标记后续终端操作使用内部创建的默认池，完成后关闭。"
	case "Timeout":
		return "设置执行策略中的单任务超时，回调须响应 context。"
	case "DefaultTimeout":
		return "将策略超时显式设置为当前模块的默认常量（见赋值及专题）。"
	case "FailFast":
		return "启用失败取消策略；回调忽略 context 时不会被强制停止。"
	case "NoFailFast", "DefaultFailFast":
		return "关闭失败取消策略，返回自身。"
	case "Shards":
		return "设置执行策略分片数量，非正值采用 GOMAXPROCS 且至少为 2。"
	case "DefaultShard":
		return "设置执行策略分片数量为 GOMAXPROCS，至少为 2。"
	case "Chunk":
		return "设置批处理或分块调度的元素数；每批回调与逐元素回调的区别见终端方法。"
	case "DefaultChunk":
		return "将分块大小显式设置为模块默认批量大小。"
	case "Buf":
		return "设置流式结果的 channel 缓冲元素数；较大缓冲不替代消费。"
	case "DefaultBuf":
		return "将策略 buf 配置设为 0，实际 channel 大小由终端方法确定。"
	case "Map":
		return "按配置的执行策略逐项映射，返回包含成功值与错误的结果容器。"
	case "MapBatch":
		return "按 Chunk 分组，每批调用一次映射回调，返回结果容器。"
	case "ForEach":
		return "按配置策略逐项执行，返回总数、失败数和首个错误的汇总。"
	case "ForEachBatch":
		if e.pkg == "maps" {
			return "按 Chunk 分组，每批执行一次回调；统计单位为元素，失败批次按其元素数累计失败。map 遍历顺序不确定。"
		}
		return "按 Chunk 分组，每批执行一次回调，统计单位为批次。"
	case "Stream":
		return "按执行策略映射并通过只读 channel 输出；提前停止消费时取消执行上下文。"
	case "Total":
		if e.pkg == "maps" {
			return "返回结果对象记录的处理元素数；ForEachBatch 按每批元素数累计。"
		}
		return "返回结果对象记录的总项数，批处理时单位为批次。"
	case "FailCount":
		return "返回结果对象记录的失败项数。"
	case "OkCount":
		return "返回总项数减去失败项数。"
	case "Error":
		return "返回结果对象保存的错误；成功时为 nil。"
	case "Err":
		return "返回结果对象是否保存错误；错误值通过 Error 获取。"
	case "String":
		return "返回统计快照的格式化字符串。"
	}
	return "调用入口及参数见下方签名；生命周期与组合边界见行为专题和链接实现。"
}
