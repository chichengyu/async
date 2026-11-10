package main

import "strings"

// These boundaries are checked against the individual builder assignments and
// their terminal constructors, rather than inferred from Default* names.
func builderCaution(e entry) string {
	if e.name == "Logger" || e.name == "DefaultLogger" {
		return "立即修改全库日志器，并非只配置当前构建器，也不等待 Build/Run 才生效。DefaultLogger 或 Logger(nil) 恢复静默实现；日志后端须并发安全、及时返回，调用不恢复其 panic。应在启动时统一设置，避免业务组件相互覆盖。"
	}
	if e.pkg == "group" && strings.Contains(e.receiver, "Builder") {
		switch e.name {
		case "Context":
			return "保存非 nil 父 context 并确保 trace_id，后续 Build 的 Group context 与 Run 的作用域 context 由此派生；任务提交仍应传递该 context。取消只请求业务退出，配置不影响已经构建的实例，也不能与构建或 Run 并发。MultiGroup 未启用 FailFast 时不把此字段绑定到各片内部 context。"
		case "Worker", "DefaultWorker":
			return "保存每个 Group 的并发数，非正值在 NewGroup 按 1；Group/GroupVoid 初始值和 DefaultWorker 显式使用调用时 IO()。GroupMulti 初始值为 0，因此未设置时每片只有 1；分片后总并发乘以片数。不要与终端执行并发修改构建器。"
		case "Timeout", "DefaultTimeout":
			return "Timeout 保存原值，DefaultTimeout 读取并保存调用当时全局默认值；Build/Run 仅把正数覆盖到新 Group，非正数保留创建时的全局期限，因此 Timeout(0) 不能清除非零全局默认。需明确取消期限时在实例提交前 WithTimeout(0)，业务仍须响应 context。"
		case "SubmitTimeout", "DefaultSubmitTimeout":
			return "保存等待任务入队的期限，仅正数在终端创建时应用；DefaultSubmitTimeout 清零，恢复新 Group 的无显式提交计时器状态。不是业务执行期限，仍受提交 context、关闭和 FailFast 影响，必须处理提交错误。"
		case "Streaming", "DefaultStreaming":
			return "正数在后续 Build/Run 创建结果流，非正数和 DefaultStreaming 不启用；实例 WithStreaming(0) 则使用 worker×2，边界不同。流需持续消费；等待时无法刷入流的余项可能丢弃，完整结果应由 Wait 获取。"
		case "ResultCallback", "DefaultResultCallback":
			return "保存回调供后续 Build/Run 使用，nil 和 DefaultResultCallback 清除；实例在结果记录路径同步调用，可能由不同任务并发触发。回调须并发安全、及时返回，不在其中等待/关闭所属 Group，不把它当作可以修改已保存 Result 的钩子。"
		case "FailFast":
			return "标记后续构建的 Group 启用失败取消，当前构建器没有反向关闭开关；作用域 Run 使用派生 context。只请求取消，不强制结束回调，也不使 Run 自动返回业务错误；仍需处理提交错误并在等待后检查结果。"
		case "Shards":
			return "GroupMulti 保存水平分片数，非正值及 1 都只包装一片，不采用 ShardedGroupBuilder 的默认 4 片。Worker 是每片并发，片数和 worker 相乘决定资源；Run 负责关闭全部分片，结果按片号拼接，不保持全局提交顺序。"
		}
	}
	if strings.Contains(e.receiver, "BoundedRunnerBuilder") {
		switch e.name {
		case "Context":
			return "只保存经 trace_id 处理的 context，须非 nil；Build 创建的 BoundedRunner 不保存此字段，不能用它取消后续任务。每次 BoundedGo/BoundedGoResult 必须另传业务 context，配置不与 Build 并发。"
		case "Max", "Build":
			return "Max 保存数值，Build 每次创建独立信号量；非正并发度构建时按 IO()，不限制正值上界。同一执行器须在循环外复用才能限制整体并发；等待空位发生在提交方，无 Close 或统一 Wait，分别保存/等待任务句柄。"
		}
	}
	if strings.Contains(e.receiver, "TaskBuilder[") {
		switch e.name {
		case "Context":
			return "保存父 context 并确保 trace_id，须非 nil；只影响后续启动的任务，不更换已运行任务的 context。取消后回调仍须配合返回；配置不能与终端启动并发修改。"
		case "WithTimeout", "DefaultTimeout":
			return "正数期限从回调开始执行时创建，不含 Bounded 等空位时间；非正值及 DefaultTimeout 清除此构建器的显式期限，不读取全局默认任务超时。回调退出时取消该子 context，Task.Ctx 是外层 context，不暴露此期限，也不自动表示任务完成。业务须响应 context，否则不会被强制结束、提前释放许可或自动改写返回错误。"
		case "DefaultBounded":
			return "清除当前构建器的限额引用，后续任务直接启动，不影响此前已启动任务或其信号量。它不会等待此前任务完成；重新 Bounded 会创建新的独立限额，跨新旧批次并发可能超过原限制。"
		case "Go", "GoAct", "GoResult", "GoResultAct":
			return "先保存当前超时包装再启动回调；启用 Bounded 时终端在调用方等待许可或父取消，返回句柄前不受回调期限或后续 WaitTimeout 限制，整体预算须放在父 context。已获取许可但启动前trace日志注入panic或Goexit时，提交方defer归还许可，原异常不转为任务错误，也不返回句柄；成功启动后仅由业务退出时释放，包含panic及Goexit。超时从实际回调执行计时，退出时取消子 context；Task.Ctx 不暴露该期限或充当完成通知，Task 用后应 Cancel。业务 panic 转为 PanicError，业务Goexit记录零值与ErrTaskGoexit，结果供所有等待者缓存读取，不属于PanicError；业务应正常返回error。忽略取消返回的业务结果仍被保留；Go/GoAct 句柄的 Cancel 只限制等待，GoResult/GoResultAct 的 Task.Cancel 才请求业务取消。父已取消不保证回调不会开始，须自行检查；必须回收句柄及错误。"
		}
	}
	if strings.Contains(e.receiver, "RetryChain[") {
		switch e.name {
		case "DefaultConfig":
			return "只重置五个重试策略字段：指数退避、3 次重试、100ms 起始/30s 上限、无单次超时；保留父 context、当前限流模式、各模式参数及分片。返回同一构建器；配置不能与执行并发。每次终端重新创建限流器，重置策略不提供跨执行共享额度；需要全新配置时重新创建构建器。"
		case "Context":
			return "替换后续 Execute 使用的父 context 并确保 trace_id，须非 nil；不重置策略或限流设置。父取消会阻止后续尝试及退避等待，正在运行的回调仍须响应取消；配置不与执行并发。"
		case "Exponential", "Linear":
			return "只切换退避类型，不重置次数、时长或限流配置。Linear 固定使用 initialBackoff，忽略 maxBackoff；Exponential 逐次翻倍并受正数上限约束。首次尝试之前不退避，等待响应父 context。"
		case "MaxRetries", "DefaultMaxRetries":
			return "负次数按 0，DefaultMaxRetries 设为 3；初次尝试不计入重试次数，最多次数还受 context 与限流约束。限流拒绝也占用尝试次数，不保证回调一定执行 n+1 次。日志和耗尽错误中的总次数先转为 uint 再加 1，兼容最大 int；这不缩小执行预算，生产应配置可承受的有限次数及父 context 期限。"
		case "Backoff", "DefaultBackoff":
			return "只设置退避字段，不选择策略；DefaultBackoff 使用 100ms/30s。非正 initial 不等待，非正 max 不设指数上限；Linear 忽略 max。业务失败回调返回或非阻塞限流拒绝之后开始退避，首次调用前、成功后及最后一次之后不再等待；回调耗时与许可等待另计，固定退避不保证调用起始时间等距，不加入随机抖动。"
		case "PerCallTimeout", "DefaultPerCallTimeout":
			return "正数为每次业务调用独立创建 context 期限，父期限更早时从父期限，在该次回调返回后立即取消；非正值及 DefaultPerCallTimeout 不设置单次期限。不包括获取限流许可或退避等待；同步调用不强制终止业务，也不自动把 nil 错误替换为超时错误。Run 的无参回调拿不到此 context。"
		case "Per", "DefaultPer":
			return "仅 RateLimiter 模式读取此窗口，非正值及 DefaultPer 在执行时使用 1 秒；TokenBucket 的 Rate 仍按秒计算。设置不会切换限流模式；RateLimiter 每次业务结束会归还令牌，不能据窗口推断严格的业务 QPS。"
		case "Burst", "DefaultBurst":
			return "仅 RateLimiter 模式使用；正数开启突发容量，实际容量至少为有效 Rate，DefaultBurst 将字段清零并使用普通构造路径。分片时按总量分配且每片至少 1；单次执行的限流器与其他 Execute 不共享。"
		case "Capacity", "DefaultCapacity":
			return "仅 TokenBucket 模式使用，容量同时决定初始令牌数；非正值及 DefaultCapacity 按执行时有效 Rate 的两倍计算。分片时各片都有此容量，不能当成总容量；应使用有限正数，NaN/Inf 不经业务校验。"
		case "Limit", "DefaultLimit", "Window", "DefaultWindow":
			return "仅 SlidingWindow 模式读取；非正 Limit 使用 100，非正 Window 使用 1 秒，DefaultLimit/DefaultWindow 分别清零相应字段以恢复这些值。分片按总 Limit 分配且每片至少 1；每次 Execute 都新建空窗口，不能跨调用累计。"
		case "MinWorker", "DefaultMinWorker", "MaxWorker", "DefaultMaxWorker":
			return "仅 Adaptive 模式使用；非正最小值按 1，非正最大值按有效最小值的 10 倍，Default* 清零相应字段。最大值低于最小值时构造器提升至最小值；初始容量为二者中点。配置应避免整数溢出，单次 Execute 的尝试依次执行，不提供跨调用共享并发控制。"
		case "DefaultRate":
			return "清零 RateLimiter/TokenBucket 共用的 Rate 字段，后续执行使用 10；不会改动模式、Per、Burst 或 Capacity。RateLimiter 按 Per 窗口补充，TokenBucket 按秒生成，分片总量规则不同。"
		case "DefaultShards":
			return "将四种限流模式的分片字段均清零，终端使用单片路径；Shards(0) 则使用 DefaultShardCount，二者不同。模式及额度保持，后续 Shards 会再次覆盖；每次 Execute 的额度仍独立。"
		}
	}
	if e.pkg == "ratelimit" && strings.Contains(e.receiver, "Builder") {
		switch e.name {
		case "Context":
			return "只替换构建器保存的 context 并确保 trace_id，须非 nil；当前 Build 不把此字段传入最终限流器。阻塞 Wait/Acquire 需另传可取消 context，关闭仍由实例所有者管理；已创建的子构建器不随父构建器变更。"
		case "RateLimiter", "TokenBucket", "SlidingWindow", "Adaptive", "Sharded":
			return "每次分配新的子构建器，只复制当前 context，模式参数初始为零；不会沿用此前子构建器的设置。Default* 需显式调用，其值不一定等于零值构建行为；Build 不传入所保存的 context。"
		case "Build":
			advice := "每次创建独立实例，不回写规范化后的字段；保存的 context 不传入实例，阻塞获取须另传 context。"
			switch {
			case strings.Contains(e.receiver, "TokenBucket"):
				advice += "Rate/Capacity 原样传入，未配置仍为 0，不自动采用 DefaultRate/DefaultCapacity；须使用有限正数。分片对每片应用相同速率与容量。"
			case strings.Contains(e.receiver, "SlidingWindow"):
				advice += "Window 原样传入，非正值在后续检查立即过期；单片非正 Limit 拒绝正数请求，分片总 Limit 则分配至每片至少 1。需要有效窗口/额度时显式设置。"
			case strings.Contains(e.receiver, "Adaptive"):
				advice += "单片最小值非正按 1，最大值不高于最小值时提升至最小值，初始为两者中点。分片先分配总最小/最大值，每片至少 1，再规范化；许可须成对释放。普通 Adaptive 无 Close 方法且无补充 goroutine，分片版本用完 Close。"
			default:
				advice += "单片非正 Rate 按 IO()，分片按总量分配且每片至少 1；正数 Burst 开启突发路径。未配置的 Per 为 0，每个 100ms tick 补满，DefaultPer 才设为 1 秒。用完必须 Close。"
			}
			return advice
		case "DefaultRate", "DefaultCapacity", "DefaultBurst", "DefaultLimit", "DefaultPer", "DefaultWindow", "DefaultMinWorker", "DefaultMaxWorker", "DefaultShards":
			return "显式赋值而非清零：Rate/Limit/MinWorker 为 IO()，Capacity/Burst 为 2×IO()，MaxWorker 为 10×IO()，Per/Window 为 1 秒，Shards 为 max(2,IO())；本方法只设置其名称对应字段，不依其他自定义字段重新计算。分片 TokenBucket 额度按每片应用，其他模式按总量分配且每片至少 1。"
		case "Rate", "Capacity", "Burst", "Per", "Limit", "Window", "MinWorker", "MaxWorker", "Shards":
			return "原样保存参数至当前子构建器，后一次同字段设置覆盖；不在此校验或填入 Default*，实际修正规则由 Build 的构造器决定。配置结束后再创建/共享实例，不与 Build 或其他设置并发；分片额度规则见 Build。"
		}
	}
	if e.pkg == "pool" && strings.Contains(e.receiver, "Builder[") && e.name != "Run" {
		switch e.name {
		case "Context":
			return "保存非 nil context 并确保 trace_id，供后续 Run 的作用域使用；PoolBuilder 只在 FailFast 时将其绑定到池内部 context，提交仍须传递作用域 context。MultiPool 注入外部池时不应用 cfg/FailFast，外部池原设置继续生效；修改不影响已运行实例，配置不得与 Run 并发。"
		case "Pool", "DefaultPool":
			return "MultiPoolBuilder.Pool 注入的原池作为第 0 片，Run 结束会连同其他片一起关闭；注入时 cfg 中 Worker/Timeout/FailFast 等配置不应用，Shards 仍生效。Pool(nil)/DefaultPool 清除注入，恢复内部创建；外部池不要再由其他使用者并发执行生命周期操作。"
		case "Config":
			return "同步以当前 Config 值调用 fn 并用返回值整体替换，fn 须非 nil，panic 直接传播；闭包返回零值 Config 会把 MaxResults 改为 0（无限保留）。MultiPool 注入外部池时 cfg 不应用；配置须在 Run 前完成。"
		case "DefaultPoolConfig", "DefaultMultiPoolConfig":
			return "只整体重置 cfg：Size=IO()、MaxResults=-1，其余字段为零；不重置 context，也不清除 MultiPool 的外部池引用或分片数。内部创建时仍继承创建当时的全局默认超时、结果上限和环形缓冲。"
		case "Shards", "DefaultShards":
			return "保存 Run 时的水平分片数，非正值及 1 仅包装原池；DefaultShards 保存 0，实际一片。正数多片时 Worker/内存配置按每片计算，总资源随片数增加，分片不会迁移原池已有任务。"
		case "DefaultTimeout", "DefaultRingBuf", "DefaultMaxResults":
			return "清除当前字段的构建器覆盖，内部创建池时使用当时的全局默认值；不一定表示不限时、禁用缓冲或无限结果。MultiPool 注入外部池时这些设置不应用。需要明确覆盖时检查对应实例方法的边界。"
		case "Worker", "DefaultWorker", "Timeout", "SubmitTimeout", "DefaultSubmitTimeout", "FailFast", "DefaultFailFast", "MaxPending", "DefaultMaxPending", "Overflow", "DefaultOverflow", "OverflowDrop", "OverflowBlock", "OverflowError", "RingBuf", "MaxResults", "Streaming", "DefaultStreaming":
			return "只改变当前 cfg，后续 Run 才应用：Timeout/SubmitTimeout/MaxPending/RingBuf/Streaming 仅正数生效，MaxResults 非负才覆盖（0 无限）。DefaultOverflow 显式选 Drop，初始零值则为 Block。MultiPool 注入外部池时 cfg 不应用，分片资源按每片计算；配置不与 Run 并发。"
		}
	}
	return ""
}
