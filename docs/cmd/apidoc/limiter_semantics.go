package main

import "strings"

func limiterCaution(e entry) string {
	if e.receiver == "" {
		switch e.name {
		case "DefaultShardCount":
			return "返回 max(2,IO())，即至少 2 的 NumCPU×2，不读取 GOMAXPROCS，也不分配资源。实际总容量可能因每片至少 1 而超过请求额度，片数应按资源预算显式选择。"
		case "NewRatelimitBuilder":
			return "只保存 context，不立即创建限流器；后续模式入口创建独立子构建器，Build 时才创建资源。保存的 context 当前不传给限流实例，Acquire/Wait 须单独传 context；需要后台补充的实例最终 Close。"
		case "NewRateLimiter", "NewRateLimiterWithBurst":
			return "立即创建满桶及定时补充 goroutine，非正 rate 按 IO()；普通容量等于有效 rate，Burst 容量至少为有效 rate。每 100ms 按累计时间补额度，非正 perDuration 每 tick 补满；不是精确滑动窗口或严格并发上限。容量按内存预算且 rate 在正 int32 范围内，最终 Close；Release 会提前恢复额度。"
		case "NewShardedRateLimiter", "NewShardedRateLimiterWithBurst":
			return "立即创建各片满桶及各自补充 goroutine，非正片数按 max(2,IO())。rate/burst 总量分别按余数优先分配，每片至少 1，容量至少为片速率；总量低于片数会被提升。获取只轮询选择一片，不搜索其他余额，最终 Close 全部；配置总量和各片 int32 速率须符合内存预算。"
		case "NewAdaptiveRateLimiter":
			return "minRate 非正按 1，maxRate 不大于有效 min 时提升至 min，初始并发以 min+(max-min)/2 计算整数中点，避免边界相加溢出；无需后台补充或 Close。成功 Acquire 必须配对 Release，业务完成显式 RecordOk/Failure 才调整并发。边界须满足正 int32 和内存及初始化耗时预算；不自动执行回调。"
		case "NewShardedAdaptiveRateLimiter":
			return "立即创建多片无定时补充的许可器，非正片数按 max(2,IO())；min/max 总量分别按余数优先分配，每片至少 1 且 max>=min，实际总量可被提升。获取及结果记录各自轮转，不绑定同一片；成功获取配对外层 Release，最终 Close，边界遵循单片 int32/内存预算。"
		case "NewSlidingWindowRateLimiter":
			return "原样保存 limit/window，不补默认值；limit<=0 拒绝正数请求，window<=0（包括最小 time.Duration）使既有记录在后续正数检查中过期，但单次请求仍不能超过 limit。正数窗口的精确边界记录过期，生产入口校验正数窗口。无后台资源，无 Release/Close；成功请求记录在窗口内保留，按请求量预算内存，不能用它限制正在执行的并发。"
		case "NewShardedSlidingWindowRateLimiter":
			return "非正片数按 max(2,IO())，总 limit 按余数优先分配且每片至少 1，总额度低于片数时被提升；window 原样传入，应为正。正数窗口内接受数不超过 TotalLimit，但请求只选择一片，其他片有余额也可拒绝，不跨片拼接额度；无后台资源和 Close，内存随窗口内接受的请求数增长。"
		case "WithRateLimiter", "WithRateLimiterWithBurst", "WithShardedRateLimiter", "WithShardedRateLimiterWithBurst", "WithShardedAdaptiveRateLimiter":
			return "立即创建对应限流器并同步调用非 nil fn，返回后 defer Close；error 只来自 fn且保持身份，panic 直接传播但仍关闭。返回前各片补充goroutine已退出，阻塞Acquire（含BlockForce）被关闭唤醒；关闭不等待已获取许可的业务，随后Release安全忽略。必须在作用域内等待自己启动的业务结束，不把实例带出作用域继续获取，参数规则遵循对应New构造器。"
		}
	}
	if strings.Contains(e.receiver, "Builder") {
		return ""
	}
	if e.receiver == "*TokenBucket" && (e.name == "Allow" || e.name == "AllowN") {
		return "正数检查先按经过时间补充并截至容量，再整笔接受或拒绝；拒绝不扣额度但更新时间与补充余额。AllowN 非正 n 直接 true，不补充或更新时间。初始满桶且不能积攒超过容量的空闲额度；Allow 固定需 1，容量小于 1 时永不能通过，但小数 AllowN 可用。配置应有限且容量覆盖最大单笔请求，浮点额度不作精确十进制账目，无后台资源或 Close。"
	}
	sharded := strings.Contains(e.receiver, "Sharded")
	if sharded {
		switch e.name {
		case "WithStrategy":
			return "按片依次原样保存策略，非跨片原子更新；只影响各片后续 Acquire，已经等待的调用保留原策略。未知策略按 Block，Reject 忽略取消，BlockForce 只靠额度或关闭解除等待；应用启动时配置，不能用策略切换取消已有等待。"
		case "GetShard":
			return "负索引或超出分片数返回 nil；有效索引返回原实例指针，不复制。直接使用该片绕过外层轮转和许可记录，不能把直接获取与外层 Release 混用；直接调整也可能使外层汇总配置失真，统一管理生命周期。"
		case "ShardCount", "Size", "Available", "TotalRate", "TotalLimit", "TotalMin", "TotalMax":
			return "ShardCount 为构建时片数；汇总值按各字段约定返回，不能视为同一时刻的全局原子快照。Size/TotalRate 是 RateLimiter 配置速率，Available 是当前令牌数；TotalMin/TotalMax 是 Adaptive 配置边界，TotalLimit 是窗口额度。总额度小于片数时每片至少 1，实际总量可超过请求值。"
		case "Allow", "AllowN":
			return "每次轮转选择一片，只检查该片，不搜索其他片空闲额度、不跨片拼接 AllowN。拒绝和非正请求也推进外层轮转，非正请求直接成功且不扣额度；正数大于单片容量可拒绝，即使全局汇总仍有余额。TokenBucket 的有限小数请求按整笔额度处理，配置按每片应用；SlidingWindow 配置按总量分配。GetShard 直接请求会改变该片余额但不推进外层轮转；无后台 goroutine 或 Close 方法。"
		case "Acquire", "Wait":
			if strings.Contains(e.receiver, "Adaptive") {
				return "轮转选择一片并以固定 Block 获取槽位，只等待所选片，不搜索其他片空闲槽位；有空位时已取消仍可能成功，无空位时响应取消或关闭。Adaptive 不提供 WithStrategy。仅成功获取后配对外层 Release，不与 GetShard 直接获取混用；业务完成后另行记录结果。"
			}
			return "轮转选择一片并按其策略获取，不寻找其他空闲分片；所选片阻塞时即使别片有余额也会等待。仅成功获取后配对外层 Release，不与 GetShard 直接获取或 Token 句柄混用；回调须自己管理执行和错误记录，获取成功不代表业务完成。"
		case "Release":
			return "仅配对外层成功 Acquire/Wait 的许可，内部记录选择有未归还许可的一片，未绑定调用方 goroutine；多余外层 Release 无操作。Token 句柄和直接 GetShard 获取须用各自释放途径；错误配对会提前归还其他业务持有的许可。"
		case "Token":
			return "轮转选择一片并返回绑定该片的 Token；成功后调用句柄 Release，失败时不要释放。该路径不登记外层 Acquire 的许可记录，不能再调用外层 Release；不要复制包含 sync.Once 的 Token。"
		case "Close":
			return "逐片关闭底层许可器，唤醒阻塞等待；不等待业务回调完成、不强制终止任务。RateLimiter 分片同时停止补充 goroutine，Adaptive 分片关闭无补充的许可器；关闭期间跨片观测不原子，关闭后不再复用。"
		}
	}
	if e.receiver == "*RateLimiter" {
		switch e.name {
		case "Acquire", "Wait", "Token":
			return "按当前策略获取：Reject 无令牌立即报错并忽略 context；BlockForce 忽略取消，直到获取或 Close；Block 在无可用令牌时检查取消，已取消但有令牌仍可成功。成功后按用途决定是否归还；定时补充与 Release 均可增加余额，不能当成严格并发上限或严格窗口次数。Token 成功后只通过句柄 Release。"
		case "Release":
			return "立即尝试归还一个令牌，满桶或关闭时忽略，不检查调用方是否实际获取。多余调用在桶未满时可提前增加额度，不能随意重复释放；Token 句柄已负责释放时不要再调用本方法。按时间额度限流且希望持续限速时，业务后主动 Release 会改变预期速率。"
		case "Close", "Stop":
			return "Stop 委托 Close，重复/并发 Close 等待同一次资源清理完成；停止补充 goroutine 并关闭令牌通道，唤醒等待。不等待业务完成、不取消业务 context；已持有许可的业务仍须自行收尾，之后不可重开实例。"
		case "Size", "Available":
			return "Size 返回每 Per 窗口的配置额度，Available 返回当时可用令牌数；突发容量可大于 Size，不能用两者差值推导持有许可数。查询不是额度预留，随后 Acquire 可能阻塞或被拒；关闭后的状态读数不表示实例仍可用。"
		case "WithStrategy":
			return "原样保存策略，后续 Acquire 读取；未识别值走默认 Block 分支。不会改变已开始等待的 Acquire 所选策略，也不改变额度；BlockForce 不响应 context 取消，只能靠归还、补充或关闭唤醒。配置建议在共享实例前完成。"
		}
	}
	if e.receiver == "*AdaptiveRateLimiter" {
		switch e.name {
		case "Acquire", "Release":
			return "无定时补充，许可须成功 Acquire 后成对 Release；失败不得释放，多余 Release 可能提前归还他人持有的槽位。Acquire 仅在无可用许可时响应 context，已取消而有空位仍可能成功；不会自动执行或记录业务结果。普通 Adaptive 无 Close 方法。"
		}
	}
	if e.receiver == "*SlidingWindowRateLimiter" {
		switch e.name {
		case "Allow", "AllowN":
			return "同步在锁内清理窗口并检查，允许时立即记录请求数量；拒绝不占额度。AllowN 的非正 n 直接 true 且不记录或清理，正数请求原子地全量接受或拒绝；window<=0 时此前记录全部过期，仍检查单次请求限额，正数窗口恰好到边界的记录过期。生产校验正数窗口，无 context、Release 或 Close，也不自动限制业务并发。"
		}
	}
	return ""
}
