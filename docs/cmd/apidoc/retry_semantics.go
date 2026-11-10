package main

import "strings"

func retryCaution(e entry) string {
	if e.receiver == "WorkerPoolBackend" && e.name == "Submit" {
		return "接口签名为 Submit(ctx,func(ctx) error) error，是否阻塞、取消及关闭行为由注入后端实现决定。Submit 的 error 按后端协议表示提交结果，异步业务错误需按后端约定另行获取；泛型 Pool 需要适配器。后端必须明确接受与拒绝的边界，供 BindRetryToWorker 判断提交超时是否可安全重试。"
	}
	if e.receiver == "" {
		switch e.name {
		case "New", "NewVoid":
			return "创建独立可变构建器，不立即执行或分配限流器。默认指数退避、3 次重试、100ms 起始/30s 上限、无单次超时和限流；原样保存父 context，执行时须非 nil，构造及 Execute 不自动补充 trace_id。需要追踪时先 EnsureTraceID，或显式调用 Context(ctx)。反复 Execute 每次新建独立限流器；不能与配置修改并发，也不以重新调用构造器维持跨调用额度。"
		case "RetryWithBackoff", "RetryWithBackoffVoid", "RetryWithBackoffResult", "RetryWithLinearBackoff", "RetryWithLinearBackoffVoid", "RetryWithLinearBackoffResult":
			return "每次尝试前检查父 context，同步执行 fn；nil 错误立即成功，即使回调期间父 context 已取消也不替换成功返回。panic 转为 PanicError 并按普通失败重试；最终失败包装最后一次错误，有值版本丢弃失败回调返回的值并返回零值。负次数按 0，非正退避不等待，指数计算饱和而不溢出；无随机抖动。Result 变体不设置 Occupied。"
		case "RetryWithConfig", "RetryWithConfigVoid":
			return "只读取 opts 第一个元素的正数 PerCallTimeout；每次调用单独派生 context，以 defer 在单次调用结束时取消，含runtime.Goexit及日志后端panic展开，不取消父context。Goexit结束调用goroutine，不转换为错误、不继续重试；日志后端panic仍传播。超时不强制结束业务，也不自动覆盖 nil 错误。父 context 仍存活时，回调返回的 Canceled/DeadlineExceeded 仍可重试；父已取消且回调返回这类错误时直接返回该错误，否则在下一轮/退避中处理父取消。最终失败返回零值并包装最后一次业务错误，普通业务panic可恢复并重试。"
		case "WithTimeout", "WithTimeoutVoid", "WithDeadline", "WithDeadlineVoid":
			return "同步直接调用一次 fn，不捕获 panic、不替回调返回 context 错误，有值版本原样保留同时返回的值与错误。父 context 必须非 nil；即使父已取消、期限已过或 timeout 非正仍调用 fn，回调须检查 context，否则可成功或无限阻塞。保留父值并采用更早期限；普通返回、panic展开及runtime.Goexit均执行defer取消派生context，不取消父context。回调自建goroutine若继续使用该context，会在包装调用结束时收到取消，生命周期仍由业务管理。这些函数只在internal/retry，根包未提供同名入口，外部项目不能直接导入internal；公开重试用Retry/RetryVoid，单次协作期限可用context.WithTimeout/WithDeadline。"
		}
	}
	if strings.HasPrefix(e.receiver, "*RetryChain[") {
		switch e.name {
		case "Execute", "ExecuteVoid", "Run", "RunVoid":
			return "单次执行内同步依次尝试，每次终端新建独立限流器，不能限制跨 Execute 总流量；配置固定时并发终端的许可、尝试次数、错误及单次子context仍独立，配置方法不能与执行并发，业务闭包共享数据由应用同步。RateLimiter/Adaptive及其分片每次成功获取许可后以局部defer释放，包含Goexit和日志后端panic，正常路径仍先记录Adaptive结果再释放，退避前已归还；获取失败不释放。Goexit不转换为错误或继续重试，日志后端panic仍传播，二者未产生普通结果时不补Adaptive成功/失败记录。TokenBucket/SlidingWindow拒绝也消耗尝试，最后被拒绝不保留此前业务错误。有值版本失败返回零值。父取消在尝试前/退避时检查；nil错误优先成功，最后业务失败仍包装业务错误，最后等许可失败也可能包装取消，判断用errors.Is。普通业务panic可恢复并重试；Run无参回调拿不到单次context，自有goroutine由业务管理。"
		case "RateLimiter", "TokenBucket", "SlidingWindow":
			return "仅选择后续执行的模式，不清空其他模式字段；后一次模式选择覆盖前一次，每次终端创建全新额度。RateLimiter 阻塞等许可并在调用后 Release；TokenBucket/SlidingWindow 非阻塞拒绝，拒绝占尝试并进入退避。分片 RateLimiter/SlidingWindow 分配总量且每片至少 1，TokenBucket 每片使用完整 Rate/Capacity。"
		}
	}
	return ""
}
