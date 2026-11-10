# 全库审查执行记录（2026-11-05）

测试目录调整（2026-11-10）：原 docs 根目录的6个 Go 测试文件已集中到 docs/tests，当前运行命令为 go test -race -tags audit ./docs/tests。下文历史命令中的 ./docs 为调整前的包路径，原始测试记录与结果不改写。生成器测试仍位于 docs/cmd/apidoc，基准测试位于 docs/benchmarks。

最终审计结论（2026-11-10）：已确认的25项库运行缺陷均已修复并回归，当前没有待修复的已确认缺陷或已发现文档错误。十个内部包的独立只读复审未发现新的可操作问题，详见 [独立审查报告](independent-review.md)。1,415个方法条目包含参数说明、使用示例、生产建议与注意事项；声明对照、示例编译、原文场景执行和并发/边界验证分别留有证据。当前运行源码与全库Linux short audit race通过快照一致。结论是本次审计范围内没有遗留已知问题，不是对所有输入、调度或第三方回调的绝对无缺陷证明。以下“进行中”“active”及旧计数均为阶段历史，以本结论和文末终态为准。

最新第25项修复终态与消费边界（2026-11-10）：当前全库Linux short audit race13测试包全部通过、辅助包无测试，根包680.618s；运行源码与该次快照一致。旧根包nonshort同二进制66批完成，441顶层无重漏，438 PASS/3显式SKIP，另有当前Pipeline完整nonshort及公开入口补验。100组观察契约三环境各100轮、账本race fuzz58,737次；只读CLI独立复审限定范围无可操作问题。另两基础Pipe消费示例先复现误处理失败值再修正，去重原文136→138，25项运行修复、93公开Examples不变；目标active，继续逐方法语义复核。

最新第25项运行修复（2026-11-10）：Pipeline的Pipe最终阶段/Shard/AutoScale业务Goexit漏阶段OnResult已先复现再修复。52组定向契约三环境各100轮通过，四旧行为变体检出；当前Pipeline完整nonshort Linux race及根包9项公开Pipeline测试补验通过。消费原文扩至Pipeline15/Flow14场景三环境各25轮及Linux race通过，去重136、93公开Examples不变。旧根包长测前54/66批verified，只归属修复前二进制；独立复审和剩余语义仍开放，目标active。

最新Pipeline/Flow消费原文复核（2026-11-10）：两块分别14类场景在Go1.25race、Go1.26race、Windows386各25轮及Linux race通过；各六错误变体检出。修复示例吞阶段/存储/取消错误、Flow未继承处理context及并发存储问题，库运行实现未改。去重原文134→136，24项库运行修复、93公开Examples不变。根包nonshort race前33批通过，百万自定义比较去重已完成；独立复审与剩余逐方法语义开放，目标active。

最新关闭原文与等待统计复核（2026-11-10）：四种独立关闭用法九场景三环境各25轮、Linux race通过，五错误变体检出；新增Pool四等待入口×四结果16组契约三环境各100轮及Linux race通过、三变体检出。修正Active递减晚于等待的过时说明，更新API生成器。去重原文133→134，24项库运行修复及93公开Examples不变。根包nonshort race前26批通过，第27批百万自定义比较去重运行；独立复审与剩余逐方法语义开放，目标active。

最新全局启动配置原文复核（2026-11-10）：十场景在Go1.25race、Go1.26race、Windows386各25轮及Linux race通过，五可编译错误变体被行为断言检出；补五参数、生效时机、实例提交预算和Logger要求，修正Trace开关及Warn降级说明。去重原文132→133，库运行修复仍24项，93公开Examples不变。根包nonshort race前21批通过，第22批分片百万Timeout运行；独立复审工具仍未成功启动，剩余逐方法语义开放，目标active。

最新生产生命周期原文复核（2026-11-10）：Available监控五场景、Bounded分批十七场景均三环境各25轮及Linux race通过，4/6可编译错误变体被行为断言检出；补参数、作用域、关闭顺序、批次首错与副作用边界。两块已在旧88内，去重仍132，24项库运行修复及93公开Examples不变。根包nonshort race前十五批通过，第十六批百万Adaptive仍运行；独立复审及剩余逐方法语义开放，目标active。

最新生产Retry边界原文复核（2026-11-10）：两独立调用吞业务、panic包装与父取消错误已复现并修正，十场景三环境各25轮及Linux race通过，六可编译错误变体检出；文档明确总预算、取消竞争及退避中断可能不保留前次业务错误。累计24项库运行修复、132去重原文、93公开Examples；根包非short race第七批百万Pool Timeout已通过，该批其余测试继续。独立复审与剩余逐方法语义仍开放，目标active。

最新生产动态限流原文复核（2026-11-10）：并发Resize/Acquire原文七种异常吞错已复现并修正，八场景Go1.25race、Go1.26race和Windows386各25轮及Linux race单轮通过；六可编译错误变体被行为断言检出。累计24项库运行修复、131去重原文、93公开Examples；根包非short Linux race前六批通过，第七批百万Pool Timeout仍运行。独立复审工具参数解析失败，剩余逐方法语义开放，目标active。

最新生产流式原文复核（2026-11-10）：修正遗漏父context、Run/任务/消费错误和结果丢弃报告，九场景三环境各25轮及Linux race单轮通过；5000任务配合阻塞消费者验证4096缓冲溢出，五可编译错误变体有效检出。累计24项库运行修复、130去重原文、93公开Examples；根包非short Linux race分批续跑，独立复审及剩余逐方法语义开放，目标active。

最新生产资源与提交原文复核（2026-11-10）：五原文块/六独立步骤39场景在Go1.25race、Go1.26race与Windows386各25轮通过、Linux race单轮通过；补调用方Context、Run返回错误检查、提交失败后合并实际保留结果错误。六可编译错误变体检出。累计24项库运行修复、129去重原文、93公开Examples；Map同步终端60组四环境补验通过。根包非short Linux race续跑前六批已完成，第三个百万慢测Pool Timeout仍运行；独立复审及剩余逐方法语义开放，目标active。

最新MapChain生产原文复核（2026-11-10）：六独立段34场景与7组批次边界在Go1.25race、Go1.26race和Windows386各25轮通过，Linux race单轮通过；修正映射/批量仅日志、流式保存错误未检查、转换回调固定nil错误及父取消遗漏。七可编译错误变体均被行为断言检出。累计24项库运行修复、129去重原文、93公开Examples。根包非short Linux race首轮30分钟超时（166顶层通过、无断言失败或race告警），同二进制分批续跑其余275项；独立复审及剩余逐方法语义仍开放，目标active。

最新Pipeline生产原文复核（2026-11-10）：修正原文仅返回顶层err而遗漏逐项错误及父取消，23场景三环境各25轮、Linux race单轮通过，六可编译错误变体检出；明确阶段依赖和五分钟共享预算。累计24项库运行修复、128去重原文、93公开Examples。根包完整非short Linux race继续运行，独立复审及剩余逐方法语义仍开放，目标active。

最新Task与Pool生产原文复核（2026-11-10）：Task原文33场景、Pool原文6场景在Go1.25race、Go1.26race与Windows386各25轮通过，Linux race单轮通过。Pool示例已复现并修正提交错误遮蔽已记录业务错误，合并提交与回调首错；11个可编译错误变体被行为断言检出。累计仍24项库运行修复、127去重原文、93公开Examples；根包完整非short Linux race正在运行，独立复审及剩余逐方法语义仍开放，目标active。

最新Linux长测与fuzz诊断终态（2026-11-10）：十个内部包非short Linux race全部通过，清单无重漏且内部源码哈希核对一致；全库Linux short race已通过。TokenBucket四worker race fuzz342060次后worker EOF，内核明确OOM终止；保存输入两平台race100轮通过，单worker15秒批次30287次通过。不把资源失败算通过或库缺陷。累计24项运行修复、127去重原文、93公开Examples；独立复审及剩余逐方法语义仍开放，目标active。

最新聚合原文与Linux非short复核（2026-11-10）：config聚合示例保留提交错误、等待已接受业务并合并全部占用槽位错误，7场景三环境各25轮通过，五错误变体检出、Linux race通过。限流包完整非short Linux race144.504s通过，其他九个内部包非short race运行至task；全库Linux short race已全部通过。累计24项运行修复、127去重原文、93公开Examples，目标active。

最新Linux race终态（2026-11-10）：全库short audit race单进程根包670.563s通过，13测试包全部通过、辅助包无测试，14包清单及JSON事件核验无失败；新增通用工具契约和三个原文包另行Linux race通过。同一百万并发Token用例Linux race通过（子用例19.96s），Windows独立重跑仍TSan分配1455，保留平台资源边界。限流包完整非short Linux race继续运行。累计24项运行修复、126去重原文、93公开Examples；目标active。

最新通用工具与错误识别复核（2026-11-10）：config原文对类型化nil的PanicError解引用可panic，先复现三类错误链再补非nil检查；9场景三环境各25轮通过。MergeCancel新增18组顺序/nil/异常/嵌套/并发契约三环境100轮通过，另三段工具原文9场景三环境25轮通过，八错误变体检出。局部C工具链已支持Linux race，Token及通用工具实测通过；全库Linux short race继续运行。运行修复仍24项、去重原文126、93公开Examples，目标active。

最新Token原文与生命周期复核（2026-11-10）：修正RPC响应处理原文吞错误并传递同一context，16场景三环境各25轮通过；新增普通/一片/三片Token在释放、扩缩容、补充和关闭后的15组并发幂等/归属契约，三环境各100轮通过。七个可编译错误变体检出，两个新测试包Linux普通实际通过。累计运行修复仍24项，去重原文122、公开Examples93；包级race、生成编译及结构检查通过，完整目标active。

最新非阻塞原文与Linux复核（2026-11-10）：修正TokenBucket/SlidingWindow五段原文吞业务错误及拒绝结果，改为复用实例并明确应用定义拒绝错误；95场景三环境各25轮通过，六可编译错误变体检出。新增WSL实际运行脚本，当前14包清单已核验：13测试二进制Linux普通short实际全部通过，辅助包无测试，根包418.665s；三个限流原文包Linux实际通过。累计24项运行修复，去重原文片段121、公开Examples93；本轮未改库运行实现，目标active。

最新限流等待与策略复核（2026-11-10）：修正Wait并发原文丢失等待/业务错误，Acquire及策略示例补业务context、错误返回和局部许可收尾，三段31场景三环境各25轮通过。新增等待策略快照72组及新获取策略81组，三环境各100轮通过；九个对应可编译错误变体被检出。README Adaptive补defer释放与错误返回，完整原文及复用业务段扩展至98场景三环境25轮，三个变体检出。累计运行修复仍24项，去重原文片段116，公开Examples93，目标active。

最新限流原文复核（2026-11-09）：修正生产及Adaptive示例吞业务错误，另六段基础/分片用法统一传递业务context并返回错误。八段原文80场景在Go1.25race、Go1.26race和Windows386各25轮通过，六可编译错误变体被识别；补许可获取与取消优先级、统计/异常边界及共享实例关闭建议。累计24项运行修复，本轮未改库实现；去重原文片段113组，公开Examples仍93。独立审查工具无法启动，完整目标保持active。

最新重试交叉复核（2026-11-09）：新增固定构建器两个并发终端的5模式、单/三片、4终端、6结果含共同父取消，共240组确定契约，Go1.25race、Go1.26race和Windows386各100轮通过。Retry生产四步原文22场景三环境各25轮通过，检查步骤停止、业务值身份/幂等键、重试次数、退避与单次context；十可编译错误变体被识别。补生产参数、返回、累计预算和并发配置边界，运行实现未修改，累计24项；105去重片段及93公开Examples不变，目标active。

最新生产示例复核（2026-11-09）：Shard生产配置两作用域在排队中取消后只返回提交取消而遗失已开始业务错误，原文测试已复现；现先等待已接收任务，再合并提交及业务结果错误。新增严格typed原文脚本，空/成功/业务错/父预取消/提交中取消10场景，Go1.25race、Go1.26race、Windows386各25轮通过。105组去重原文片段及93公开Examples计数不变，累计24项运行修复，本轮没有改库实现，目标active。

最新基础示例复核（2026-11-09）：修正Shard十二段配置示例遗漏提交/业务错误与父context，统一另六段既有批量/索引/FailFast示例向业务传递任务context。十八段原文严格typed业务替身、成功/业务错/父预取消/执行中取消共72场景，Go1.25race、Go1.26race和Windows386各25轮通过；四可编译错误变体被识别。新增十二段与此前五段完整示例、既有88段去重后共105原文片段，93公开Examples不变。运行实现未修改，累计仍24项，目标active。

最新分片复核（2026-11-09）：新增ShardedGroup单/多片、业务/通知阻塞、限时/已取消等待及FailFast开关16组确定契约，Go1.25/Go1.26 race与Windows386各100轮通过，五错误变体被识别。修正Shard五个完整示例遗漏提交/业务错误、取消context传递及取消goroutine收尾，22个有效原文场景三环境各25轮通过，保留可运行脚本。运行实现未修改，累计仍24项；独立复核工具仍无法启动，目标保持active。

最新交叉复核（2026-11-09）：新增Map五终端、原生/默认池/借用池三路径、混合成功/error/panic/Goexit、随机分片/批大小及FailFast模型，最终race fuzz52,010次通过，五个可编译错误变体被行为断言识别。新增Slice/Map十终端乘默认/借用池20组实际并发选择契约，三环境100轮通过；文档及生成器明确DefaultPool在终端按IO()创建，策略Worker不调整池并发。没有新增运行实现修改，累计仍为24项；完整目标与既有环境、独立复核边界保持。

最新终态（2026-11-09）：累计二十四项运行修复。第二十四项全库普通short全部通过（根包428.277s），441项根包short audit race经74个独立进程全部通过，清单无重漏且二进制SHA256已核验。93个公开Examples经五轮独立race进程全部通过；新增Task.Result四入口四结果并发取消/句柄副本模型race fuzz66,110次通过。1415方法条目的参数、示例、生产建议、注意事项结构齐全；1388声明调用、3完整程序、88原文局部片段及专题返回表编译通过，376围栏、378锚点、vet/diff检查通过。独立复核工具未成功启动，Linux实际运行、Windows非short百万race及剩余逐方法语义覆盖边界仍开放，目标保持active。以下保留阶段历史，较早进行中措辞以此终态为准。

最新补充（2026-11-09）：第二十四项真实缺陷已修复：Task八入口业务Goexit只关闭通道，所有等待者误读零值/nil成功。四基础入口增加正常返回标记，异常结束登记ErrTaskGoexit；正常业务值/错误、panic、缓存和有界许可规则保持。八入口并发等待、启动清理及扩展缓存/许可模型race100轮和386100轮通过，race fuzz分别55,947/55,047次，四错误变体被检出；93公开示例，Task/docs/工具包级race通过。第二十三项全库普通与441项分进程race已全部通过，第二十四项重新全库验证仍在运行。

最新补充（2026-11-09）：第二十三项运行缺陷已复现并修复：原生Map/ForEach/Stream/MapBatch/ForEachBatch业务Goexit遗失失败登记及剩余工作，现保存分片cursor、在原WaitGroup登记下接替worker并逐项defer收尾。五入口原缺陷与混合结果/context清理契约race100轮通过，独立五入口模型race fuzz63,085次通过，386100轮及三个可编译错误变体通过。92个公开示例；最终全库普通与根包race仍收尾，不以有限模型宣称全库无bug。

最新补充（2026-11-09）：新增两项真实运行缺陷。第二十一项原生并行Map/分片Map与Each业务Goexit遗失当前结果及剩余工作，现以原登记名额接替并用单项defer登记；第二十二项池化Map/流式Map/流式Each业务Goexit遗漏FailFast取消，现补退出检测。均先观察红测试再修复。定向race100轮与386100轮通过，独立混合模型race fuzz分别61,032/48,950次通过。最新全库与根包race仍收尾；一次全库编译因内存分配失败，保留记录并待降低并发重跑。

最新补充（2026-11-09）：第二十项运行缺陷已复现并修复：Stream/Flow业务Goexit导致worker退出，Stream未完成槽位返回、Flow已接受输入遗失且Active不归零。业务退出登记零值/ErrTaskGoexit、同一worker登记名额接替、defer转发和计数清理。12组确定契约及混合结果种子三环境各100轮通过，三路径独立模型race fuzz72,517次通过，六可编译错误变体被行为断言识别；89公开示例五独立race进程通过，全内部/docs/工具short race及Go1.25/386包级通过。第二十项全库short普通全部通过（根包438.585s），441项根包short audit race经74新进程全部通过，清单无重漏且二进制SHA256已核验。详见 [Stream 与 Flow 业务异常退出及已接受输入修复](audit-results.md#stream-与-flow-业务异常退出及已接受输入修复2026-11-09)。

最新补充（2026-11-09）：第十九项运行缺陷已复现并修复：原生Pipeline业务Goexit结束整块goroutine，后续项未执行却返回零值成功，Meta遗失记录；默认构建器观察包装还会漏通知。八处原生分块共享异常接替与单项defer终结，正常分块复用方式不变。21组确定场景及混合结果种子三环境各100轮通过，7入口独立结果模型race fuzz84,046次通过，三个错误变体被行为断言识别。88公开示例五独立race进程及全内部/docs/工具short race通过；本版本全库short普通全部通过（根包436.566s），441项根包short audit race经74新进程全部通过，清单并集与二进制SHA256已核验。此证据对应第十九项版本，后续Stream/Flow修复另行重验。详见 [原生 Pipeline 分块异常退出与阶段结果修复](audit-results.md#原生-pipeline-分块异常退出与阶段结果修复2026-11-09)。

最新补充（2026-11-09）：第十八项运行缺陷已复现并修复：Pool业务Goexit遗失结果/completion及worker容量，Group还遗漏任务计数收尾，导致等待不能完成。增加ErrTaskGoexit、业务退出记录、defer计数收尾及原登记名额下worker接替。22组确定契约和混合结果种子三环境各100轮通过，六错误变体识别；Pool/Group单worker race fuzz最终分别44,986/58,231次通过，87公开示例五独立race进程通过。最终缩容修正版全库short普通全部通过（根包434.041s），441项根包short audit race经74个新进程全部通过，清单并集无重漏、二进制SHA256已核验。此证据对应第十八项版本；后续原生Pipeline修复单独重验。详见 [Pool 与 Group 业务 Goexit 完成登记及 worker 容量修复](audit-results.md#pool-与-group-业务-goexit-完成登记及-worker-容量修复2026-11-09)。

最新补充（2026-11-09）：第十七项运行缺陷已复现并修复：Bounded四入口取得槽位后、trace日志注入若panic/Goexit，尚未启动业务且许可遗漏。提交方增加启动失败defer，成功后交由回调原defer释放。48组契约三环境各100轮、四错误变体、49,549次race fuzz及全部内部包short race通过；86公开示例五独立race进程通过。本版本全库short普通全部通过（根包435.226s），本版本441项根包short audit race完整清单经74个新进程全部通过，清单并集无重漏。详见 [BoundedRunner 启动前异常槽位归还修复](audit-results.md#boundedrunner-启动前异常槽位归还修复2026-11-09)。

最新补充（2026-11-09）：第十六项运行缺陷已复现并修复：RetryChain业务Goexit或恢复日志后端panic跳过普通Release，四类许可未归还。改为每次尝试局部defer；52组许可契约及既有8组三环境各100轮通过，另10组Adaptive统计三环境各100轮通过，十个错误变体被行为断言识别。85公开示例五独立race进程通过，全内部short race及本版本全库short普通通过（根包437.686s）。详见 [Retry 许可异常退出配对释放修复](audit-results.md#retry-许可异常退出配对释放修复2026-11-09)。

最新补充（2026-11-09）：第十五项运行缺陷已复现并修复：RetryWithConfig及Void回调Goexit或panic日志后端再次panic时，跳过普通cancel而保留单次context。改用既有WithTimeout的defer清理，正常重试语义保持。98组新增生命周期契约及既有24组计时契约三环境各100轮通过；原缺陷两路径overlay均复现，六辅助函数错误变体被识别。84个示例五独立race进程通过，全内部包/docs/工具short race及第十五项版本全库short普通全部通过（根包436.943s）。详见 [Retry 单次 context 异常退出清理修复](audit-results.md#retry-单次-context-异常退出清理修复2026-11-09)。

最新补充（2026-11-09）：Result/PanicError新增28组错误链、类型化nil、格式动词、输出失败及引用契约，三环境各100轮通过，五错误变体被行为断言识别。补全宽度/精度不生效、%q不转义、Result不转发栈标记等说明；83公开示例五独立race进程通过。本轮运行实现不变，累计十四项修复。详见 [Result 错误链与格式化边界复核](audit-results.md#result-错误链与格式化边界复核2026-11-09)。

最新补充（2026-11-09）：Drain新增18组同步/取消/panic/空结果契约，三环境各100轮通过，四错误变体被识别；修正Pipeline参数说明的Serial/Flow/Drain混用概括，82公开示例五独立race进程通过。TokenBucket四worker race fuzz异常捕获TSan内存分配1455；完全不导入本库的synctest最小程序复现EOF及同类报告。本轮运行实现不变，累计十四项修复。详见 [Pipeline 消费回调与 fuzz 检测器诊断](audit-results.md#pipeline-消费回调与-fuzz-检测器诊断2026-11-09)。

最新补充（2026-11-09）：Pipeline/Pipe/Flow共167组配置捕获、输入共享、重复执行及消费契约，三环境各100轮通过，12个错误变体被行为断言识别。修正Pipe配置捕获时点、Serial中间FailFast错误不输出等文档；81个公开示例五独立race进程通过。本轮运行实现不变，累计十四项修复。根包第十四项版本441项short audit race完整清单重验通过：前220项分批、余221项独立进程，GOMAXPROCS=2，测试规模未改；初始第12批TSan内存分配1455仍保留，不能称单进程全套通过。全内部包/docs/工具最新short race也全部通过。详见 [Pipeline 配置捕获与重复执行交叉复核](audit-results.md#pipeline-配置捕获与重复执行交叉复核2026-11-09)。

最新补充（2026-11-09）：第十四项运行缺陷已复现并修复：Pool/Group把有限大缩放乘积先转int，扩容错误只加1、缩容错误直达Min；现先应用相应边界再转换。56组数值矩阵及20组公开ticker契约三环境各100轮通过，四错误变体识别。第十四项版本全库short普通全部通过（根包438.235s），全内部包/docs/工具short race通过；末增测试另有最终包级证据。详见 [AutoScale 有限大系数转换溢出修复](audit-results.md#autoscale-有限大系数转换溢出修复2026-11-09)。

最新补充（2026-11-09）：Task结果引用所有权144组，Go1.26/Go1.25 amd64 race及Go1.25 Windows386各100轮通过；五错误变体识别，79个公开示例五独立race进程通过。补AsyncResult浅拷贝及引用读写责任，原文局部编译增至88组；本轮运行实现不变，累计十三项修复。详见 [Task 结果引用所有权与通知交叉复核](audit-results.md#task-结果引用所有权与通知交叉复核2026-11-09)。

最新补充（2026-11-09）：Retry 默认配置与父取消共160组虚拟时间契约，Go1.26/Go1.25 amd64 race和Go1.25 Windows386各100轮通过，六个可编译错误变体识别；78个公开示例五个独立race进程通过。补重置范围、每次执行独立额度和DefaultShards区别，原文局部编译增至87组。本轮运行实现不变，累计十三项修复。详见 [Retry 默认配置保留与重复执行交叉复核](audit-results.md#retry-默认配置保留与重复执行交叉复核2026-11-09)。

最新补充（2026-11-09）：MultiGroup混合轮转/keyed、uint64回绕、取消拒绝和已关闭片共54组，三环境各100轮通过；四错误变体识别。补直接取模、拒绝仍轮转、合并顺序及单worker前提，77个公开示例显式五进程race通过；本轮运行实现不变，仍十三项修复。详见 [MultiGroup 混合路由与计数回绕交叉复核](audit-results.md#multigroup-混合路由与计数回绕交叉复核2026-11-09)。

最新补充（2026-11-09）：第十三项运行缺陷已复现并修复：FillNoResultSkipped最小负目标减去非零总数溢出，误入巨量追加；改为先比较再减。486组聚合/稀疏/重复索引/跳过项契约三环境各25轮通过，四错误变体识别；公开示例增至76。详见 [NoResult 聚合槽位与最小负目标溢出修复](audit-results.md#noresult-聚合槽位与最小负目标溢出修复2026-11-09)。

最新补充（2026-11-09）：RingBuffer指针元素、五种存储形状、四策略和极端FlushN数量共480组，三环境各25轮通过；四个错误变体识别，core/pool/docs/工具short race通过。补内部槽位清零、独立返回切片、浅拷贝和Reset责任说明，运行实现不变，仍十二项修复。详见 [RingBuffer 引用清理与极端批量交叉复核](audit-results.md#ringbuffer-引用清理与极端批量交叉复核2026-11-09)。

最新补充（2026-11-09）：Slice的Take/Drop/SliceRange及兼容Slice共1431组极端索引、空/偏移视图契约，三环境各25轮通过，四个错误变体识别。补清空保留起点容量、共享槽位及MinInt/MaxInt说明；包级short race通过，本轮运行实现不变，累计十二项修复。详见 [Slice 极端范围与视图容量交叉复核](audit-results.md#slice-极端范围与视图容量交叉复核2026-11-09)。

最新补充（2026-11-09）：修复第十二项运行缺陷，Adaptive合法int32边界在32位初始化中点相加溢出；采用min+(max-min)/2。48组四构造入口契约三环境各100轮、32位大容量四入口实际构造、三个错误变体、30秒race fuzz通过；修复后全库short普通及全内部包short race通过。详见 [Adaptive 初始容量中点溢出修复](audit-results.md#adaptive-初始容量中点溢出修复2026-11-09)。

最新补充（2026-11-09）：限流作用域关闭共27组虚拟时间契约，Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过；三个错误变体被识别。补齐等待唤醒、BlockForce、业务不自动等待、关闭后释放及内部便捷函数可见范围说明。本轮不改运行实现，累计十一项修复。详见 [限流作用域关闭与在途业务交叉复核](audit-results.md#限流作用域关闭与在途业务交叉复核2026-11-09)。

最新补充（2026-11-09）：实测修复两项重试整数边界缺陷，累计十一项运行修复：最大int预算显示负数，以及RetryFn耗尽后计数溢出而超额执行。154组有限契约三环境各一百轮通过，32位完整21亿次预算连续三轮通过，四个错误变体被识别；全内部包short audit race通过。详见 [Retry 最大次数计数与耗尽溢出修复](audit-results.md#retry-最大次数计数与耗尽溢出修复2026-11-09)。

最新补充（2026-11-09）：Slice/Map四种模式切换各36组、共72组契约，Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过；四个错误变体识别，75 Examples显式五进程race通过。补源配置保留、数据视图及旧池责任说明，原文局部编译增至86组，运行实现不变。详见 [Slice 与 Map 模式切换状态交叉复核](audit-results.md#slice-与-map-模式切换状态交叉复核2026-11-09)。

最新补充（2026-11-09）：Slice过滤阈值、单worker、DeleteFunc、引用及异常共128组契约，Go1.26/Go1.25 amd64 race及Go1.25 Windows386各二十五轮通过；四个错误变体识别，74 Examples显式五进程race通过。补原地压缩/新数组与panic不回滚说明，原文局部编译增至85组，运行实现不变。详见 [Slice 过滤所有权与异常传播交叉复核](audit-results.md#slice-过滤所有权与异常传播交叉复核2026-11-09)。

最新补充（2026-11-09）：Task四种执行入口的提前等待及Cancel窗口共48组虚拟时间契约，最终Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过，四个可编译错误变体识别。补缓存优先、等待错误与业务错误、许可及通知生命周期说明；原文局部编译增至84组，本轮运行实现不变。详见 [Task 等待提前返回与最终结果交叉复核](audit-results.md#task-等待提前返回与最终结果交叉复核2026-11-09)。

最新补充（2026-11-09）：Core等待辅助函数24组虚拟时间契约，最终Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过，五个可编译错误变体识别。补同步取消/快照、重复取消、onDone顺序及监视退出说明，纠正源码中已过时的Pool/Group调用关系；原文局部编译增至82组，运行实现不变。详见 [Core 等待辅助函数与监视生命周期交叉复核](audit-results.md#core-等待辅助函数与监视生命周期交叉复核2026-11-09)。

最新补充（2026-11-09）：Pool限时关闭与阻塞提交同步通知四组受控契约，最终Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过，三个可编译错误变体识别。补全worker信号、完整清理、Pending/失败计数、context及Reset说明；局部Markdown原文编译增至81组。本轮无运行实现修改，详见 [Pool 限时关闭与阻塞提交通知交叉复核](audit-results.md#pool-限时关闭与阻塞提交通知交叉复核2026-11-09)。

最新补充（2026-11-09）：TokenBucket时间补充独立整数模型、24组空闲/拒绝边界及24组公开Retry退避交叉契约通过；最终race fuzz 95,238次，三平台定向各一百轮，五个可编译错误变体识别。73 Examples显式五次独立进程race通过，ratelimit/retry/docs/工具short audit race通过（7.343/4.860/7.837/3.055s）。本轮不改运行实现，累计九项修复保持，详见 [TokenBucket 补充额度与重试交叉复核](audit-results.md#tokenbucket-补充额度与重试交叉复核2026-11-09)。

最新补充（2026-11-09）：Group／NoResult 等待超时、同步结果通知及 Reset 共12组受控契约，最终 Go1.26/Go1.25 amd64 race、Go1.25 Windows386各一百轮通过；三个可编译错误变体被识别。72 Examples显式五次独立进程race通过，group/pool/docs/工具short audit race通过（48.030/22.755/7.217/2.663s）。本轮未改运行实现，累计九项修复保持，详见 [Group 等待超时与同步通知交叉复核](audit-results.md#group-等待超时与同步通知交叉复核2026-11-09)。

最新补充（2026-11-09）：Pool 结果转换、业务统计/FailFast、存储/流/环形缓冲、同步回调顺序及拒绝通知共28组契约，Go1.26/Go1.25 amd64 race、Go1.25 Windows386实测各一百轮通过；四个可编译错误变体被识别。71 Examples显式五次独立进程race通过，pool/docs/工具short audit race通过（21.883/6.145/3.089s）。本轮未改运行实现，累计九项修复保持，详见 [Pool 结果转换与同步通知交叉复核](audit-results.md#pool-结果转换与同步通知交叉复核2026-11-09)。

最新补充（2026-11-09）：Pipeline 七种路径、FailFast 开关、父期限及观察函数共 58 组虚拟时间契约，Go1.26/Go1.25 amd64 race 和 Go1.25 Windows 386 实测各一百轮通过；四个可编译错误变体被识别。新增观察时点示例后共 70 Examples，显式五次独立进程 race 通过；pipeline/docs/工具 short audit race 通过（22.200/7.632/4.019s）。本轮没有修改运行实现，累计九项修复保持，详见 [Pipeline 共享期限与观察函数生命周期交叉复核](audit-results.md#pipeline-共享期限与观察函数生命周期交叉复核2026-11-09)。

验证次数更正（2026-11-09）：Go testing 对 Example 每个测试进程只执行一次，-count 不会重复 Example。本文历史命令中的 -count=5/25/100 只证明普通 Test 的对应重复次数；此前将 Example 写成五轮、二十五轮或一百轮的表述均以本更正为准，不能据此计算示例运行次数。当前 73 个可执行示例另以显式五次 go test -count=1 进程调用完成 race 验证，日志为 .gotmp/bucket-timing-examples-1.log 至 -5.log；最低版本及 386 新示例各有一次实际执行证据。

最新补充（2026-11-09）：Slice 批处理 432 组独立模型组合，Go1.26/Go1.25 amd64 race、Go1.25 Windows 386 实测各二十五轮通过；四个可编译错误变体被识别。69 Examples race 单次命令（传 -count=5，每例实际一次）及 sliceops/docs/工具 short audit race 通过（15.756/6.371/4.515s）。补极值、失败批次映射和幂等说明，本轮无运行实现修改，累计九项修复保持。详见 [Slice 批处理极值与失败批次映射交叉复核](audit-results.md#slice-批处理极值与失败批次映射交叉复核2026-11-09)。

目标：以实际代码为准审查正确性、并发和生命周期，修正 Markdown，补齐方法使用说明。

最新追加：Group 注入外部 Pool 的十四组期限/排队/统计/FailFast 契约，Go1.26/Go1.25 amd64 race 与 Go1.25 Windows 386 实测一百轮通过，五个错误变体识别。68 Examples race 单次命令（传 -count=5，每例实际一次）和 group/pool/docs/工具 short audit race 通过；补两层队列、期限及结果归属说明，本轮不改运行实现，详见 [Group 外部池双层期限与结果交叉复核](audit-results.md#group-外部池双层期限与结果交叉复核2026-11-09)。

最新追加：分片批量热点背压十五组受控契约，Go1.26/Go1.25 amd64 race 与 Go1.25 Windows 386 实测一百轮通过，四个错误变体识别；67 Examples race 单次命令（传 -count=5，每例实际一次）及 shard/pool/docs/工具 short audit race 通过。本轮补逐项超时、父期限、Drop 结果与 KeyFn panic 部分提交说明，不改运行实现，详见 [分片批量背压与部分提交交叉复核](audit-results.md#分片批量背压与部分提交交叉复核2026-11-09)。

最新追加：Task Builder 四终端共 32 组超时/许可/清理契约及新增示例，Go1.26/Go1.25 amd64 race 和 Go1.25 Windows 386 实测一百轮通过；三个错误变体被识别。66 Examples race 单次命令（传 -count=5，每例实际一次）、task/docs/工具 short audit race 通过，文档补整体预算与 Ctx 完成通知边界；无新增运行修复，详情见 [Task Builder 超时与句柄生命周期交叉复核](audit-results.md#task-builder-超时与句柄生命周期交叉复核2026-11-09)。

约束：保持公开签名及正常路径兼容；保留开始时已有的未提交修改；每个实际缺陷先复现、再修复、最后回归。有限测试不能证明所有输入和调度均无缺陷。

最新阶段（2026-11-09）：确认并修复第九项运行缺陷，SlidingWindow.Allow/AllowN 对最小 time.Duration 取负溢出而保留旧记录；改为只对正窗口取负，非正窗口直接使旧记录过期。最终回归/构建路径/种子 race 二十轮及独立年龄模型 race fuzz 217,764 次通过，四个错误变体被识别，Go1.25 amd64 包级 race 和 386 定向实际运行通过。公开示例增至 63 个，race 五轮通过；第九项 Go1.26 全库 short race 原会话已退出 1：根包和其余包通过，sliceops 的无序首错测试假设失败；复现并修正后，新增结果契约与修正测试合并 race 二百轮通过，运行实现未因此修改。采用 -p 1 的全库重验中其余包全部通过，根包 TSan 资源失败；根包 441 项分批均已通过（前 220 项默认设置、后 221 项 GOMAXPROCS=2），未改任务档位；不能计为单进程全库通过，详见 [ForEach 超时首错顺序复核](audit-results.md#foreach-超时首错顺序复核2026-11-09)。详见 [滑动窗口最小时长取负溢出修复](audit-results.md#滑动窗口最小时长取负溢出修复2026-11-09)。以下结果为阶段历史。

追加文档复核：Adaptive 公开 Acquire 固定 Block，没有 WithStrategy，已修正套用 RateLimiter 策略的参数说明；普通/分片直接构造与 builder 及选中片等待的六组契约 race 二十轮、Go1.25 386 实际运行和三个错误变体识别通过。新增测试后 ratelimit/docs/工具 short audit race 通过，运行修复仍为九项，详见 [Adaptive 公开获取策略文档复核](audit-results.md#adaptive-公开获取策略文档复核2026-11-09)。

最终包级补验：限流包 GOMAXPROCS=2 的非 short 普通全包通过（20.682s）；相同设置 race 在百万级 Token 用例仍报 TracePart 1455（13.426s），不计通过。根包两份 manifest 已核对全部 441 名称恰好覆盖，单进程 race、Linux 实际执行、独立复核及逐方法剩余语义缺口保留；所有测试会话均已结束，详见 [最终包级与资源边界](audit-results.md#第九项版本最终包级与资源边界2026-11-09)。

最新阶段（2026-11-09）：Slice 浮点秩桶/位模式模型最终 race fuzz 80,824 次，四入口 Compact nil 谓词边界和最终种子 race 二十轮通过；五个错误变体被识别。公开浮点排序/去重示例加入后 62 Examples race 单次命令（传 -count=5，每例实际一次）通过，sliceops/docs/工具 short audit race 通过（15.800s/5.595s/2.471s）。澄清 NaN/正负零和 Compact 的 nil 边界，本轮不改运行实现，累计修复仍为八项。详见 [Slice 浮点排序与判等复核](audit-results.md#slice-浮点排序与判等复核2026-11-09)。以下结果为阶段历史。

当前结果（2026-11-09）：第八项运行修复使 Map/MapBase 的 SetAll/Merge/MergeMap/MergeWithDefault 在源目标别名时只复制每个源非自反条目一次；普通键覆盖、默认值保护、底层 map 引用和 nil 边界保持。全库 short audit race 已完整退出 0（根包 661.851s），后补 nil/空、键签名及集合契约有包级 race。最终模型 race fuzz 64,347 次通过，八个错误变体均被识别，公开示例增至 61 个并通过 race 五轮。详见 [Map 别名合并的非自反键重复复制修复](audit-results.md#map-别名合并的非自反键重复复制修复2026-11-09)。此前结果如下，旧计数对应当时版本。

最新阶段：Retry 业务退避 120 组、单次 context 生命周期 18 组、期限 6 组、极大退避取消 6 组及限流动态时间表 8 组完成 race 复核；11 个临时错误变体均由行为断言识别。补文档调用时间表与固定退避区别，无新增运行修复。详见 [Retry 业务退避与单次期限复核](audit-results.md#retry-业务退避与单次期限复核2026-11-06)。以下其他进展为阶段历史。

当前进展：重叠参数快照模型验证四入口 Append/Prepend/Insert/Replace 的值、共享槽位与边界，普通 fuzz 2,001,151 次、最终 race fuzz 145,104 次通过，六个变体均被识别。文档补 Prepend 参数数组复用及 Replace 缩短清零规则，新增公开示例后 59 Examples race 单次命令（传 -count=5，每例实际一次）通过。无新增运行修复，六项版本全库 short audit race 根包 648.330s 的既有证据保留；新测试有包级 race。详见 [重叠参数变更与共享槽位复核](audit-results.md#重叠参数变更与共享槽位复核2026-11-06)。

上一阶段：Slice Runner 六种终端、空/非空输入、借用/接管池共 24 组生命周期矩阵 race 二十轮及普通随机一百轮通过；六个临时生命周期错误变体均被识别。修正文档中过度概括的池 ResetWait 说明，补终端和所有权矩阵。本批未改运行实现，累计修复仍为五项。详见 [Slice Runner 池所有权终端矩阵复核](audit-results.md#slice-runner-池所有权终端矩阵复核2026-11-06)。

最新进展：ExecuteStream、ETL 和完整 Flow 三段原文先复现错误覆盖、未完成值打印、部分成功 ID 丢失与管道 context 未传递，再修正文档，分别十一/八/八路径 race 五轮通过。4200 项背压取消及空边界 race 二十轮通过，运行修复仍为既有五项。全部方法四项结构已有覆盖，剩余逐方法业务语义及外部执行证据继续审查；根包长测已结束。详见 [流水线原文与取消背压复核](audit-results.md#流水线原文与取消背压复核2026-11-06)。

最新结果：根包原非 short 无 race 长测已退出 0（5132.560s）；启动后新增测试另有包级 race 证据。限流五种等待者转换 race 二十轮及三个错误变体通过；公开默认超时读取时机 race 二十轮及三个变体通过，Group/Pool/配置文档同步补充调用时和创建时读取的差异。当前未新增运行修复，Windows 百万档 race、Linux 执行、独立复核及逐方法剩余语义仍未完成。详见 [默认超时读取时机与根包长测完成](audit-results.md#默认超时读取时机与根包长测完成2026-11-06)。

最新追加：两段 FailFast 原文补提交错误、完整等待后的部分结果和错误汇总，六路径 race 五轮通过；生产流式原文补错误通道及收尾，三段流式/回调五路径 race 五轮通过。真实补充 ticker/Resize 虚拟历史最终四种子 race fuzz 369,462 次通过，跨 tick 调整测试与种子 race 二十轮通过，四个临时变体被识别；运行修复仍为五项。最终 ratelimit/docs/工具 short audit race 通过，根包无 race 非 short 同一会话仍运行。详见 [FailFast 原文与定时限流历史复核](audit-results.md#failfast-原文与定时限流历史复核2026-11-06)。

最新进展：普通 Group Run 两段和 StreamResults/ResultCallback 两段原文先复现错误漏报，再补提交检查及等待后的错误汇总；共四段五路径 race 五轮通过，六个临时错误变体被识别。BindRetryToWorker 最终七种子 race fuzz 540,810 次及虚拟时间契约 race 二十轮通过，协议/计时各三个错误变体被识别。Retry DefaultRate 的 Per/秒单位及生成摘要同步修正，无新增运行修复。最终 retry/docs/工具 short audit race、文档编译/结构/链接及 go vet 通过；根包无 race 非 short 原会话仍运行，目标未完成。完整范围见 [Group Run 原文与提交重试协议复核](audit-results.md#group-run-原文与提交重试协议复核2026-11-06)。

最新追加：Maps、Pool、Task、Core、Retry、Shard、SliceOps、Group、Pipeline 及 MultiGroup/NoResult/MultiPool 的源码与交叉语义核对已有阶段记录，具体 fuzz 执行数和模型边界见 [审查记录](audit-results.md)。这些阶段没有发现新增可复现缺陷；这不等于尚未覆盖的每个方法与组合均已穷尽。当前没有新增运行修复，剩余逐方法语义、Linux 实际执行、Windows 百万档 race 与独立复核仍待完成。

Core 环形缓冲 FIFO/并发历史与 Trace 日志模型及包级 race 通过，三个 fuzz 分别 2,744,471/1,584,717/2,234,510 次；未发现可复现新缺陷。配置容量/溢出策略并发更新被记录为未承诺原子快照的边界，不改行为。详见 [Core 环形缓冲与日志契约交叉复核](audit-results.md#core-环形缓冲与日志契约交叉复核2026-11-06)。

Retry、Shard、SliceOps 包级 short race 与对应退避/尝试/限流/提交协议、分片拒绝/路由/生命周期、切片变更/执行/谓词/排序/去重模型通过；执行计数及边界详见 [Retry、Shard 与 Slice 交叉语义复核](audit-results.md#retryshard-与-slice-交叉语义复核2026-11-06)。本批未发现新运行缺陷或专题文档矛盾；全库语义审查仍继续。

Group short race 和索引生命周期/并发关闭/AutoScale 三模型及提交结果通知边界测试通过，未发现新缺陷或专题文档矛盾；MultiGroup/NoResult/MultiPool 源码与现有说明也已交叉核对，尚未加入新的动态模型。证据见 [Group 生命周期与通知交叉复核](audit-results.md#group-生命周期与通知交叉复核2026-11-06)。

Pipeline 的有限输入/元数据/Flow 关闭与并发提交模型及 short race 通过，未发现新缺陷或专题文档矛盾；精确计数与范围见 [Pipeline 有限与常驻执行交叉复核](audit-results.md#pipeline-有限与常驻执行交叉复核2026-11-06)。

## 执行步骤

空输入 Repeat 活性修复计划（2026-11-06）：四入口子进程已复现 Repeat(MaxInt) 超过五秒不返回，根因是空追加循环仍执行 n-1 次。采用在既有 n<=0 分支之后对 len(items)==0 提前返回的最小方案，分别修改 sliceBase.Repeat 与旧 Slice.Repeat；不引入数量上限、分配策略或新的 API。回归验证 nil/分配空切片/偏移空视图的长度、容量、nilness、底层槽位与源数组不变，并保持非空 Repeat 的现有值/共享引用语义；随后运行定向 race、原混合变更 fuzz、包级和全库回归，更新专题和生成注意事项。

- [x] 基线：核对工具链、现有改动，运行普通测试、audit 测试和静态检查。长测及受限项见结果记录。
- [x] 实现审查：覆盖 core、pool、group、shard、task、retry、ratelimit、pipeline、sliceops、maps。重点检查取消、超时、关闭、复用、panic、空输入、非法参数与跨组件组合。
- [x] 已确认二十五项缺陷修复：Dedup 比较函数、ForEachPool 上限外错误、Retry TokenBucket Rate 配置、Pool 提交/关闭通道竞态、Shard 共享取消生命周期、空 Slice.Repeat 极大次数空转、Map 非自反键条目变更、Map 别名合并重复复制非自反键、SlidingWindow 最小负时长溢出、重试总预算格式化、RetryFn耗尽后计数递增溢出、Adaptive初始中点相加溢出、FillNoResultSkipped最小负目标减法溢出、AutoScale有限大系数转换溢出、Retry单次context异常清理、Retry许可异常释放、Bounded启动前许可归还、Pool/Group业务Goexit登记及worker接替、原生Pipeline分块Goexit、Stream/Flow业务Goexit、原生Slice分块Goexit、池化/流式Slice Goexit漏FailFast、原生Map分片Goexit、Task业务Goexit误报成功、Pipeline Group路径业务Goexit漏阶段观察；均先观察失败，再修复回归，保持公开签名。
- [x] 文档审查：方法签名与参数对照源码；说明返回值、示例、生产建议和注意事项；删除未经证实的绝对保证与性能阈值。
- [x] 交叉验证：执行全量、race、随机顺序与边界测试，核对链接和文档覆盖；记录实际结果和未完成项。

## 初始事实

- 起始有 14 个已修改的跟踪文件，涉及 README、11 份 docs 文档及两处提交超时注释；不回滚这些修改。
- `docs/tests/review_repro_test.go` 通过 `audit` 构建标签启用，包含已有生命周期/取消/限流疑点。疑点不等于当前缺陷，必须实测。
- 默认 `GOROOT=D:\soft\pkg` 缺少工具。当前进程使用完整的 `D:\soft\go`，不改系统环境；Go 1.26.0，Windows amd64，CGO 开启。

后续验证结果与发现写入 `audit-results.md`。本记录不是 API 文档。

## 当前状态与后续记录

最新追加：池化 Stream 八组投递/取消/清理及原生流丢弃契约与新增公开示例，Go1.25 amd64 race 与 Windows 386 实际一百轮通过，五个错误变体被识别；区分两条通道及父取消/任务取消/回调期限的文档已更新。全部 65 Examples race 单次命令（传 -count=5，每例实际一次）和最终 sliceops/maps/docs/工具 short audit race 通过，未新增运行修复。详细边界见 [Stream 投递取消与清理交叉复核](audit-results.md#stream-投递取消与清理交叉复核2026-11-09)，完整目标继续 active。

最新追加：Map 池化完成回调与结果保留路径已反查，128 组构建器组合和四个错误变体、含借用池/部分批次失败的最终模型 race fuzz 11,571 次通过，Go1.25 amd64 race 与 Windows 386 实测通过。补 MaxResults 内存边界和构造/执行时条目复制说明，公开示例增至 64 个并通过 race 五轮。无新增运行修复，九项累计保持；独立 reviewer 工具仍参数解析失败，目标未闭合。详见 [Map 池化结果与保留策略交叉复核](audit-results.md#map-池化结果与保留策略交叉复核2026-11-09)。

继续补齐：限流“完整列表”先发现遗漏 WithTraceID，再补齐并编译全部 11 个实例返回契约；GroupNoResult 的 32 项返回及组合表覆盖全部 44 个公开方法。GroupVoid Run 原文先复现业务失败返回 nil，再补提交错误处理和 Wait/JoinErrors，五路径 race 五轮通过，两个吞错变体被识别。本批未新增运行修复，根包非 short 原会话仍运行。

继续补齐：21 项 Task 返回/字段契约、Group 37 项返回与 24 项组合调用编译通过，Group 附录覆盖全部 49 个公开实例方法，两个/三个错误变体分别被识别。修正 Group Reset 生命周期概述、附录重复 Close；两批次模型种子 race 二十轮通过。ratelimit 非 short 无 race 和十万档 race 分别通过，百万档 race 环境缺口保留。根包非 short 原会话仍未退出。

最新追加：分片令牌桶路由模型 race fuzz 476,977 次、作用域关闭及构建器状态契约 race 二十轮通过；各两个错误变体被识别。纠正限流表格两处返回类型及 DefaultRate 单位，八项限流和 23 项流水线表格返回契约编译通过；56 Examples race 单次命令（传 -count=5，每例实际一次）通过。本批无运行修改。当前内部/docs 非 short race 除 ratelimit 的 Windows TSan 1455 分配失败外均通过，根包非 short 无 race 仍运行，详情见 [分片令牌桶路由与限流返回契约复核](audit-results.md#分片令牌桶路由与限流返回契约复核2026-11-06)。

最新追加：TraceID/日志模型最终 race fuzz 610,409 次通过，四个错误变体最终均被识别。SafeCall 两入口契约 race 二十轮、LogFatal 子进程 race 十轮及各两个错误变体检查通过；55 Examples race 单次命令（传 -count=5，每例实际一次）和最终 core/docs/工具回归通过。当前五项运行修复的全库 short audit race 全包通过（根包 637.910s），最后追加测试另有包级证据；本批未改运行实现。逐方法语义、当前版本非 short 长测、Linux 实际运行及独立审查仍未完成，准确范围见 [TraceID 日志与安全调用契约复核](audit-results.md#traceid-日志与安全调用契约复核2026-11-06)。

最新追加：MultiPool 流式原文先复现业务错误返回 nil，再补独立错误回调、提交错误及流丢弃汇总；包含上限外错误和确定性慢消费的原文 race 五轮通过，三个错误变体被识别。配置文档四个不存在的字段函数先编译失败再纠正，真实六签名编译通过；TraceID 空值保留示例加入后共 54 Examples race 单次命令（传 -count=5，每例实际一次）通过。80 组原文局部编译通过，本批无新增运行修复，证据见 [流式错误与配置原文复核](audit-results.md#流式错误与配置原文复核2026-11-06)。

最新追加：反查 MultiPool/MultiGroup 同源生命周期，未复现新的运行缺陷；两引擎正常等待与未完成等待的公开入口检查 race 20 轮通过，四个临时错误变体被识别。修正 MultiGroup 基础原文的提交/业务错误处理、补多池多组共享取消及结果查询边界；79 组局部原文编译和三路径原文 race 五轮通过。仍为五项修复，详细证据和开放项见 [多池多组共享取消与原文复核](audit-results.md#多池多组共享取消与原文复核2026-11-06)。

最新追加：第五项运行缺陷 Shard 共享取消生命周期已先复现再修复；正常首片等待不提前取消 Run context，整体收尾释放共享及重复配置的旧派生 context。定向 race 二十轮、生命周期模型 race fuzz 70,902 次、四个错误变体识别、Shard 非 short race 及 53 Examples race 单次命令（传 -count=5，每例实际一次）通过；原文局部编译增至 78 组。全库运行版本区别与未完成边界见 [分片共享取消链生命周期修复](audit-results.md#分片共享取消链生命周期修复2026-11-06)。

本阶段全库 short audit race 全包通过（根包 651.252s），根包二进制早于后续重复配置补充；最终代码根包分片定向 short audit race 另通过（91.706s）。最终 Shard/根包 Linux 交叉编译通过、未执行；231 锚点全部匹配。非 short 根包长测、逐方法语义及独立复核仍未全部完成。

最新追加：分片混合路由/拒绝模型修正 Try 状态计数及有限队列假设后，race fuzz 154,532 次通过；错误注入促成首错续提定向种子，两个变体被识别，最终种子 race 十轮通过。Pool/Shard 文档补充非阻塞状态拒绝不计数/不存结果；52 Examples race 单次命令（传 -count=5，每例实际一次）通过。无新增运行修复，详见 [分片混合路由与拒绝统计复核](audit-results.md#分片混合路由与拒绝统计复核2026-11-06)。

分片原文追加：五组提交示例补齐错误返回，取消检查先稳定失败，再修正缺少 .Context(ctx) 导致回调使用 Background 的问题；最终成功/业务失败/取消三路径 race 五轮通过（2.271s）。原文局部编译总数增至 77 组并通过，未修改库运行实现。

最新追加：README 速查表纠正返回类型、默认背压与配置作用域；五组 Pool 原文示例补全结果/错误检查、跨批环形缓冲和消费者等待。72 组局部片段编译、五组原文受控 race 五轮、16 条返回类型编译及三个 Markdown 错误变体识别通过；docs/工具包级 race 通过。无新增库运行修改，51 Examples 全套最近运行属于前一批，详见 [README 速查表与原文示例复核](audit-results.md#readme-速查表与原文示例复核2026-11-06)。

最新追加：Mu 六操作并发历史模型 race fuzz 484,145 次通过，Bounded 四入口共享许可模型最终版 race fuzz 186,242 次通过；四个 overlay 错误变体均被识别。新增重入/浅拷贝、取消/复用/清除限额示例，纠正 README DefaultBounded 与 GoResult 返回类型说明，51 Examples race 单次命令（传 -count=5，每例实际一次）及 Task/docs/工具包级回归通过。无新增运行修复，详见 [Mu 快照历史与共享任务许可复核](audit-results.md#mu-快照历史与共享任务许可复核2026-11-06)。

最新追加：Pool 提交/关闭模型增加扩缩容交叉，race fuzz 306,727 次通过；新增索引结果模型修正 Wait/Active 屏障假设后 race fuzz 249,638 次通过，两个临时错误变体被识别。关闭顺序、超时范围、索引拒绝位置和统计观察窗口已修正文档；49 Examples race 单次命令（传 -count=5，每例实际一次）与 Pool/docs/工具包级回归通过。无新增运行修复，详见 [Pool 关闭扩缩容与索引结果复核](audit-results.md#pool-关闭扩缩容与索引结果复核2026-11-06)。

最新追加：Pool/Group 自动扩缩容整数决策模型 race fuzz 360,934/387,179 次通过，故障注入后补 Group 确认次数种子，最终六种子 race 十轮通过。文档澄清范围外初始值、Pool 保存边界变化、Disable 目标调整及阈值/舍入规则；46 Examples race 单次命令（传 -count=5，每例实际一次）、Pool/Group/docs/工具 short audit race 与编译/结构检查通过。无新增运行逻辑修改，详细模型假设与未覆盖范围见 [自动扩缩容决策与边界复核](audit-results.md#自动扩缩容决策与边界复核2026-11-06)。

最新追加：Map 查询/转换独立模型最终版 race 十轮及 race fuzz 288,504 次通过，两个临时错误变体被识别；细化短路/空查询、集合值来源及转换碰撞规则，45 Examples race 单次命令（传 -count=5，每例实际一次）通过。maps/docs/文档工具包级回归和文档编译/结构检查通过，无新增运行修复，见 [Map 查询与转换复核](audit-results.md#map-查询与转换复核2026-11-06)。

最新追加：Map Filter/Reduce 七入口模型 race 种子十轮、race fuzz 223,756 次通过；两个 Go overlay 错误变体被识别。修正 Filter 取消后正常 nil 错误、Reduce 失败累积值及池化 ForEach 未执行项统计说明；新增两个公开示例后共 44 Examples race 单次命令（传 -count=5，每例实际一次）通过。无新增运行库修改，详情见 [Map 聚合与单项统计复核](audit-results.md#map-聚合与单项统计复核2026-11-06)。以下各段为阶段历史，不作为最新计数。

最新追加：修复审查工具对波浪线/长围栏的 API 名称与完整程序编译漏查，先观察错误 fixture 未被检查，再修复并通过回归；Map 批量拒绝统计说明已补可执行示例，总数 38。Adaptive 单片/分片调整的整数比例模型 race fuzz 75,669 次通过。原文局部编译增至 65 组，锚点增至 209 个并通过。详见 [文档围栏检查与批量拒绝统计](audit-results.md#文档围栏检查与批量拒绝统计2026-11-06)。下段为此批追加之前的状态记录。

截至 2026-11-06：已确认四项运行缺陷均有修复和回归；最新全库 short audit race 全包通过（根包 658.299s）；37 个 Examples race 单次命令（传 -count=5，每例实际一次）、Group 并发关闭模型 race fuzz 263,645 次、分片窗口模型 384,935 次、限流补充模型 502,932 次、退避饱和模型 547,811 次、环形缓冲并发历史模型 561,855 次通过。环形缓冲两种故障注入均被新种子识别，最终真实源码 core race 通过。文档签名调用、完整程序、63 组局部片段及静态检查通过。逐方法业务语义审查继续，独立复核、Linux 运行和 RateLimit 百万级 race 仍有缺口；完整结果见 [审查记录](audit-results.md#数值边界与统计文档复核2026-11-06)。以下为按阶段追加的历史记录，旧计数和“三项修复”描述对应当时版本，不代表最新总量。

- 根包非 short 长测已通过（5178.433s）；其二进制早于 Retry 修复，已结合修复后交叉回归记录。
- RateLimit 非 short race 在低 GOMAXPROCS 重跑仍遇内存分配失败；short race 两轮通过，保留百万级 race 未完成边界。
- 最终生成文档、签名/链接/编译检查与静态检查，并继续核对逐方法参数和注意事项。
- 独立复审工具调用未成功，尚无独立复审结论；不把自行检查描述为独立复审。
- 三项缺陷定向 race 三轮及全部内部包/docs 的 short audit race 已通过；十个可执行示例已纳入。
- 1415 条方法参考已生成；签名调用编译、文件链接及单独的 204 个本地锚点核查已通过。逐方法业务语义的人工对照仍不能由这些数字替代。
- 去注释的 AST 差异核查确认已有运行逻辑变更局限于三项缺陷；其他源码修改为注释。
- 内部包及 docs 的 short audit 覆盖率运行通过，逐包数值见结果记录；没有合并根包测试的覆盖率。
- 可执行示例扩至 18 个，并通过 race 随机顺序五轮；继续核对独立函数、旧 Slice 方法及构建器/实例的超时差异。
- 追加无池 Map FailFast context 对照后共 19 个示例，race 随机顺序五轮通过；RingBuffer FIFO 模型 30 秒 fuzz 通过 8,629,633 次执行。
- 修正 README 七组局部示例并直接抽取编译通过；补充 Retry 最终限流拒绝的错误链示例后，共 20 个示例通过 race 随机五轮。三项以外没有新增运行逻辑修改。
- 补充 Map/Slice Reduce 策略超时差异后共 21 个可执行示例，race 随机五轮通过；README/MapReduce/Pipeline 共 11 组局部片段抽取编译通过，仍继续逐方法语义核对。
- Map 容器状态模型 30 秒 fuzz 通过 851,888 次；Pipeline 三执行引擎交叉模型通过 2,024,656 次；常驻 Flow 生命周期模型通过 1,703,815 次，均未替代取消调度的穷举验证。
- 新模型纳入全部内部包/docs 的 short audit race 一轮通过；Flow 模型加入后 pipeline/retry/docs 随机两轮通过。
- BindRetryToWorker 提交协议与 Flow panic 值语义输出示例加入后共 23 个 Examples，race 随机五轮通过；局部 Markdown 抽取编译扩至 13 组。
- 修正 Pipeline Serial 与默认阶段内并发的混淆、Stream 的 MaxResults 作用域、Flow 关闭排空结果的等待示例，以及 BindRetryToWorker 只重试提交的参数与生产建议。仍需继续审查其余逐方法业务说明。
- Group/Pool 示例继续纠正不存在的 Result.Stack、未使用输入、外部池未注入、遗漏提交错误与 Run 不汇总业务错误；Group 生产/流式示例直接抽取编译通过。
- Task 生产示例改为分批回收句柄，并在任务完成时结束取消监听/停止定时器；Shard 生产示例显式检查 Run 错误。共 24 组局部 Markdown 编译检查已保存为 docs/cmd/check_markdown_fragments.py，尚不能替代全部局部片段或逐方法语义核对。
- 新增 Group Run 错误传播输出示例，共 24 个 Examples 通过 race 随机五轮；三项之外仍无新增运行逻辑修改。
- Flow 并发提交/关闭模型：管道 context 存活版普通 fuzz 3,441,395 次、race fuzz 254,931 次；扩展外部取消后 race fuzz 287,492 次，均为 30 秒。成功入队精确一次、允许取消丢结果、禁止重复和拒绝项结果，并检查清理与计数。
- 切片混合变更模型 30 秒 fuzz 通过 671,943 次（34.239s）；四种入口对照独立区间模型，seeds 的 race 随机三轮通过（4.674s）。未覆盖达到并行 Filter 阈值的大切片。
- Flow 并发关闭新 seeds 纳入 pipeline/group/docs 的 short audit race 随机两轮通过（39.514s/93.571s/9.602s）。
- Pool 的 Key/批量提交示例补齐错误处理，分片说明区分原实例、独立空缓冲与已有状态；局部 Markdown 抽取编译扩至 30 组并通过。
- Core 参考补充全局配置、等待辅助、环形缓冲、日志与 PanicError 的参数/注意事项；PanicError 原值与错误链输出示例加入后共 25 个 Examples，race 随机五轮通过。

- 限流许可模型 30s fuzz 5,077,204 次通过；大切片谓词策略 race fuzz 189,533 次通过，补充跨越 100/1000 阈值的独立 Filter/DeleteFunc 检查。全部内部包/docs short audit race 逐包随机一轮通过，明细见结果记录。
- 可执行 Examples 扩至 27 个，race 随机五轮通过；Markdown 原文局部编译扩至 55 组通过，纠正同作用域重复声明和 Pipeline 中不存在的 PoolBuilder.Build。
- 继续补充 Group/Slice 逐方法注意事项及生产指南资源回收，不以通用占位说明数量作为语义审核完成证明。
- 排序/搜索独立模型普通 fuzz 659,268 次通过；Map 执行/批量模型普通 fuzz 167,561 次及 race fuzz 41,785 次通过；各自范围及此前模型版本见结果记录。
- 分片 Pool/Group 批量路由 race fuzz 153,176 次通过；Group 连续索引/重复等待/Reset/外部 Pool 生命周期 race fuzz 83,384 次通过。无超时/取消/溢出模型，不代表所有生命周期组合均已穷举。
- Examples 扩至 30 个并通过 race 随机五轮；局部 Markdown 原文编译扩至 57 组通过。继续补充构建器/限流/Map/Pipeline/Shard/Task 的具体边界，最新生成检查与 go vet 通过；逐方法语义审查仍未完成。
- Group 新模型加入后 group/shard/docs short audit race 随机一轮通过（47.137s/12.178s/5.120s）。切片六种执行策略模型 race fuzz 26,849 次通过（33.602s），sliceops/docs 随机两轮通过（20.714s/8.554s）；修正 DefaultPool 延迟创建及 Chunked/Chunk 名称混淆。
- Pool 结果保留/取消/复位模型 race fuzz 165,680 次通过（33.421s）。逐项 completion 与普通结果回调的预检查拒绝覆盖不同，新增公开可执行示例通过 race 五轮（2.552s），文档已区分。
- 新并发关闭模型复现 trySend 与 Close 的发送/关闭 race；引入任务发送读写锁后原模型 race fuzz 60s 449,068 次通过，增加限时关闭分支后的模型 race fuzz 30s 179,173 次通过。定向关闭回归十轮通过；依赖 Pool 的上层 short audit race 及根包回归继续收尾。
- 独立复审工具再次因参数解析失败（missing message）未派出审查者，仍无独立结论。

- 四项修复后的根包 short audit race 已通过（640.074s），Pool 非 short race 通过（22.541s），上层交叉回归及 Task/Retry 新模型结果已补入 audit-results。
- Task 缓存并发模型 race fuzz 326,227 次、Retry 尝试序列模型 race fuzz 312,593 次通过；各自边界见结果记录。33 个 Examples race 单次命令（传 -count=5，每例实际一次）通过，新增共享容量、NoResult 聚合/填补说明。
- 切片纯数据与旧 Slice 逐方法说明继续补齐，区分其与 Runner 的池配置、统计、结果空槽及超时路径。根包直接包装函数复用已核对的委托注意事项；方法参考需随生成器最新修改再生成核验。

- 逐方法注意事项补入剩余 Pipeline/RateLimit/Shard/根包入口，修正旧 Pipeline.ExecutePipe 的分片忽略及等待后输出、WorkerPoolBackend 回调签名、Group 回调拒绝覆盖与 Pool 转换计数边界。
- 1415 条参考结构齐全且无通用注意事项回退文本；1388 声明调用和 3 完整程序编译、go vet 通过。该结构指标不能替代方法语义验证。
- 35 个 Examples race 单次命令（传 -count=5，每例实际一次）、全部内部包/docs short audit race、前三项缺陷定向 race 十轮及 Pool 关闭 race fuzz 214,726 次通过；Linux amd64 CGO=0 交叉编译通过。独立复审、Linux 运行及 RateLimit 百万级 race 的环境缺口仍保留，详见最新审查结果。
- 36 个 Examples race 单次命令（传 -count=5，每例实际一次）通过；补齐 Retry trace_id 构造与 Context 差异、限流有许可时的取消规则、SliceResult 空槽与内部引用。最新方法参考 1388 声明调用/3 完整程序编译、go vet、1415 条目结构及 206 锚点检查通过。
- Group 并发提交/取消/等待/Close 模型首版 race fuzz 260,026 次通过；加入稀疏索引后的最终版 263,645 次通过，两版种子 race 各随机 20 轮通过。group/shard/docs short audit race 一轮通过；没有新增运行逻辑改动。独立复核工具仍解析失败，现有验证缺口保留。
- 最终 Group 模型纳入 group/shard/docs short audit race 再次通过（49.251s/15.179s/7.538s）。368 个 README/专题 Go 代码块语法通过（包含两组波浪线围栏），原文局部编译扩至 63 组；纠正 README 不存在的 Current/Rate 查询、Pool Submit/Active/SubmitAt 及 Group 完整 Wait 的资源清理说明。
- 加入 SliceResult 空槽示例后共 37 个 Examples，race 随机五轮通过（docs 4.408s/retry 3.504s/sliceops 4.585s）。新增分片窗口轮询/批量/过期预算模型，种子 race 十轮及 race fuzz 384,935 次通过；纠正分片窗口额度边界的说明，无新增运行逻辑修改。
- 全内部包/docs 的 short audit race 随机一轮通过；随后新增 Task.Result 函数字段/句柄副本/并发取消模型，种子 race 十轮、race fuzz 320,214 次及 task/docs short audit race 通过。结构检查仍为 1415 条齐全，37 Examples，368 围栏、207 锚点；独立复核工具接口缺口未解决。

- Task 结果缓存模型扩为 Go/BoundedGo/GoAct/BoundedGoAct 四入口及九类并发读取/取消操作，种子 race 20 轮和 race fuzz 182,929 次通过；两个 overlay 错误变体均被模型识别。最终 task/docs/文档工具 short audit race 通过，修改后的 Task 示例加入原文局部编译，总计 67 组通过。未新增库运行逻辑修改，验证范围及开放项见 audit-results 最新节。

- 新增 Pipeline Meta 历史独立模型，最终七路径 race fuzz 127,664 次通过，观察者错误和末阶段标签两个 overlay 变体被识别。新增两个公开示例后共 40 个 Examples race 单次命令（传 -count=5，每例实际一次）通过；pipeline/docs/文档工具 short audit race 通过，修正 Meta 历史范围、Stream 中途失败标名及 Flow 阶段错误统计/缓冲背压说明。未新增运行修复，既有验证缺口保留。

- 新增 Retry 非阻塞限流尝试预算独立模型，race fuzz 393,163 次通过，错误注入后补最终业务错误/panic 定向种子，最终六种子 race 十轮及包级回归通过。补正父取消与最后业务结果的优先级、分片 TokenBucket 和 SlidingWindow 的配额差异；42 Examples race 单次命令（传 -count=5，每例实际一次）、retry/ratelimit/docs/工具 short audit race 通过。无新增运行修复，模型限制见审查记录。

后补 MapRunner 非自反键四策略七终端共 28 组 race 二十轮通过，两个错误变体被识别；新增矩阵后的 Maps/docs/工具 short audit race 通过（5.521s/5.676s/2.805s）。MapResult 和专题补充 NaN 键不能按 Keys 回查，最终生成/编译、结构/锚点/围栏及 vet 通过。此矩阵晚于全库二进制，证据以上述包级结果为准，未新增运行修复。


第二十四项最终内部非short补验：Go1.25、GOMAXPROCS=2、-p=1、-tags audit、-count=1，internal全部十包通过（task-goexit-internal-nonshort.log）：core12.106/group46.710/maps4.211/pipeline24.454/pool18.769/ratelimit12.719/retry7.460/shard5.923/sliceops10.808/task20.457s。包含最终构建器清理契约及四入口Task.Result种子；此为普通执行，不覆盖Windows非short百万race缺口。报告更新后378锚点、go vet与diff检查再次通过。所有本轮验证进程已结束。


第二十四项工具链与构建补验：Go1.26本地工具链下，八入口/构建器Goexit契约及缓存、Result、许可三个最终模型种子race100轮通过（task-goexit-final-go126.log，3.148s）。Go1.25、GOOS=linux、GOARCH=amd64、CGO_ENABLED=0，go test -p=1 -tags audit -c -o .gotmp/linux_24/ ./...退出0，生成全部13个含测试包二进制（task-goexit-linux-compile.log）；只完成交叉编译，未实际运行Linux二进制。

## 2026-11-10 MapChain生产原文与根包Linux race续跑

- 新增 docs/scripts/check_mapchain_production_example.py，逐字提取 mapreduce.md 的生产原文并拆成六独立段执行；仅外围增加导入、业务类型/实现及返回nil。去重原文计数128→129，六独立段不重复计六块。第一段自带四条示例输入；批处理使用调用方提供的401条记录，保留Worker16/8、Shards8/4、Chunk200和30秒/一分钟期限。
- 原文失败记录 .gotmp/mapchain-production-red.log 复现映射/流式/聚合错误遗漏；批处理旧原文的空占位map导致回调未执行，该项是业务输入占位，不能算库缺陷。初次提取支架因分段数及参数重声明编译失败，已修正；不将支架错误计缺陷。
- 修正文档：Map/MapBatch首错与ctx.Err合并；Stream检查收到的业务及saveResult错误，消费到关闭后检查父取消；heavyTransform显式返回业务error；两处Reduce检查业务context。补外部参数/业务签名、生产预算/容量、无序批次/结果键、部分副作用与不可回滚、串行Reduce panic/Goexit契约。同步容器方法保持无context语义。
- 34场景覆盖六段成功、适用路径的预取消、业务错误、取消后错误/成功、panic、Goexit、条目/批次期限和父期限、保存错误；另7组批次边界0/1/199/200/201/400/401。检查键值身份、成功结果、401条各执行一次、批大小和次数、context值/期限/清理。Stream取消允许丢失业务错误，必须可识别父取消，不声称所有业务错误均返回。synctest推进协作期限，不缩减文档调度常量。
- Go1.25race25轮2.410s、Go1.26race25轮2.421s、Windows38625轮0.809s（mapchain-production-final-*.log）；Linux race单轮manifest verified=true、命令16.727s（linux_mapchain_production_final_race）。docs/文档工具short audit race6.552/2.093s；1388声明调用/3完整程序/88旧局部片段编译通过。1415方法结构、378锚点及378围栏语法、vet和diff检查通过；这些不是逐方法全部语义已核对的证明。
- .gotmp/mapchain_production_mutants/manifest.json 七变体均编译并由行为断言检出：丢Map错误、丢Stream父取消、丢保存错误、错误业务context、丢转换错误、批大小201、删除批次期限。不用编译失败、命令超时作为检出依据；本轮无新增库运行逻辑改动。
- Linux根包完整非short audit race首轮1800.106s到期退出1；JSON中166个完整顶层通过，仅包级fail，无Test fail、无race告警。保存二进制SHA256 47e1250f95c154c15e76efe3fa3ebc5c9d6ca1659f180a7bb00447e1a55605a8，同二进制441顶层清单，已完成166、剩余275分66批继续。中断父测试整项重跑；不使用short、不减百万档位、不把原整包超时抹成通过。进度证据 .gotmp/linux_root_nonshort_resumed/manifest.json；续跑尚未完成。
- requesting-code-review技能要求的独立审查仍无法启动：工具暴露空参数schema，实际调用缺message即解析失败；本轮再次失败并保留开放项。其余未覆盖方法语义继续审查，目标active。

## 2026-11-10 Map同步终端期限与失败值补验

新增 internal/maps/serial_terminal_contract_test.go，四入口（Runner/Chain/Serial/Parallel）Filter和Reduce共60组：成功、超时但返回成功、协作超时、预取消、空输入取消、首个业务错误、panic与Goexit。用synctest检查每次回调新期限、前次context已释放、成功Filter独立结果、失败Reduce返回当次值9001/9002、总虚拟耗时、同步异常传播及不取消caller context。Go1.25race50轮2.432s、Go1.26race50轮2.240s、Windows38650轮1.063s；Linux选定该测试单轮race verified=true（linux_map_serial_terminal_race）。四个源码overlay错误变体均编译且被对应断言检出：Filter删除期限、Reduce错误时回退initial、预取消时跳过Filter、回调前立即cancel；证据map_serial_mutants/manifest.json。最终maps/docs/工具short audit race3.069/7.022/2.284s通过，无新增库运行改动或已确认文档矛盾。该新测试晚于根包长测保存二进制，按独立补验记录，不混作旧二进制覆盖。

根包续跑支架修正：首批测试进程exit0，但原核验只接受顶层PASS，遇到测试自身SKIP误标verified=false。读取原始日志后确认5项PASS、1项SKIP，其中MultiGroup百万超时子例659.05s通过；更新核验分别记录pass/skip，重核首批再继续。SKIP不算执行通过，未更改任何测试跳过条件；当前第二批仍运行。原首轮整包30分钟超时证据继续保留。保存清单涉及的源码哈希再次核对未变化。

## 2026-11-10 生产资源、提交与结果错误传播原文

新增 docs/scripts/check_production_retained_examples.py，逐字提取生产页的内存控制、Pool/Group选型、生命周期、Close示例及分片资源五块，选型块含两个独立步骤，合计六步骤39场景。五块已在旧88编译片段中，去重累计129不增加。百万循环目标及原Worker/MaxResults/RingBuf常量保留；业务提前失败，synctest在一秒取消父context结束提交；不把这次执行说成百万任务全成功压测。两项生命周期步骤和单项分片步骤另验成功，其余覆盖预取消、成功响应取消、业务错误、panic、Goexit和延迟错误，检查错误链、父context值及已接受任务/子context清理。

原文两任务示例直接Run不检查返回值、构建器遗漏Context(ctx)，实际错误或父取消被忽略；production-retained-red-two.log共12失败断言。分片原文六异常场景同样丢失错误（production-retained-shard-red.log）；初次分片支架userID用int不符string参数，先修支架再取得有效失败证据。首轮百万支架因未连接父context而一直提交，60秒到期，另一选定Pool原文20秒到期；这些命令超时不算缺陷或有效变体检出。

文档现在传递父Context、显式检查每次Run错误，提交失败后Wait收尾并errors.Join(submitErr,handleResults(results))，不跳过已保留结果检查；外部处理函数必须汇总其收到的全部结果。补参数类型、imports、总预算、不可回滚、副作用幂等和存储边界：此写法只处理实际保留结果，不能恢复容量外丢失的错误，要独立记录全部终态仍须采用后文完成回调方案。库实现未改变。

最终Go1.25race25轮3.968s、Go1.26race25轮4.431s、Windows38625轮1.190s（production-retained-final-*.log）；Linux race单轮verified=true（linux_production_retained_final_race）。最终五原文重新编入88片段、1388声明调用和3完整程序通过，docs/工具short audit race6.913/2.110s通过（后续只补分片原文，最终编译再次通过）。1415条结构、378锚点、378围栏、vet、diff检查通过。

六个可编译overlay错误变体由指定断言检出：丢调用方context、忽略Run返回、Pool提交错误优先遮蔽、漏结果处理、Group提交错误优先遮蔽、分片忽略Run错误（production_retained_mutants/manifest.json）。首轮漏结果变体未用results导致编译失败，随后加unused sink有效检出；父context错误变体在百万路径到期，不计检出，改为两任务路径由错误断言检出。未把这些支架边界算库缺陷。

## 2026-11-10 生产页Pool流式消费原文

新增 docs/scripts/check_production_stream_example.py，逐字提取流式消费原文；不在旧88局部片段或其他原文执行脚本中，去重129→130。原文六失败断言（production-stream-red.log）复现父取消、业务error、panic、Goexit、消费error与等待取消均返回nil；初始支架fmt未用先编译失败，修正后才记录有效行为失败。

修改文档：配置Context(ctx)，外层检查Run返回；提交失败停止继续提交并保存error；消费者检查Result.Err，仅成功项调用process(ctx,value)并登记处理error；Wait后等consumed关闭再读取消费者错误，避免并发读写切片；合并提交/消费/父取消并对StreamDropped大于零返回明确错误。参数、业务签名、imports、总预算、消费者响应取消、错误列表内存和已丢结果不可恢复均补说明。不把未收到错误当全量成功，不改Pool自身流式丢弃行为，无新增库实现改动。

九场景：success/empty/pre-canceled/error/panic/goexit/consumer-error/wait/dropped。最后一项保留原Streaming4096，用5000任务并让消费者等全部业务进入后再读，确定产生溢出；检查调用5000、接收少于5000且函数返回stream dropped错误。三环境各25轮：Go1.25race2.655s、Go1.26race2.766s、Windows3860.647s；Linux race单轮verified=true（linux_production_stream_final_race）。docs/工具short audit race6.624/1.910s通过。

五可编译overlay错误变体均有效检出（production_stream_mutants/manifest.json）：丢任务error、丢消费error、丢Runerror、丢丢弃报告、消费者错误context。最后一项明确由context身份panic终止测试进程检出，其余由错误断言检出；未用编译失败或超时作检出。同步消费者错误列表是在关闭完成通道后读取，race覆盖当前场景，不代表穷举所有调度。


## 生产动态限流原文错误与取消传播复核（2026-11-10）

新增 docs/scripts/check_production_resize_example.py，逐字提取生产文档的RateLimiter并发安全示例。该块不在旧88片段或其他原文脚本中，去重130→131。保留1000次Resize、1000次业务目标、100/秒初始速率与50至249动态速率，使用虚拟时钟验证补充与取消。原文七个有效行为失败已保存 production-resize-red.log：业务error后继续调用1000次，预取消、执行中取消、等待取消、截止时间和取消后业务error最终均返回nil。

仅改文档：每轮显式检查ctx.Err，保存Acquire及doRequest错误，等待两goroutine结束后合并业务错误与父错误，defer最后关闭实例。补齐ctx/业务签名/imports、速率及循环参数、共享实例、总预算、低频调参、单写者同步边界、取消竞争及直接goroutine不捕获panic/Goexit等注意。库运行实现未改，累计修复仍24项。

八场景success/error/pre-canceled/cancel-error/cancel-success/wait/deadline/late-error，业务context身份、成功调用1000次、失败停止、业务与取消双错误、预取消零调用均检查；成功耗时至少一虚拟秒验证不会通过立即归还令牌绕过速率。Go1.25race6.246s、Go1.26race6.263s、Windows3861.509s，各25轮；Linux race单轮verified=true（linux_production_resize_final_race/manifest.json）。docs/工具short audit race6.595/2.049s通过，1388声明调用/3完整程序和旧88片段编译通过，1415结构、378anchors、378fences、vet及diff检查通过。

六可编译overlay错误变体（production_resize_mutants/manifest.json）均由行为断言检出：丢业务错误、丢最终错误、丢预检查、丢父错误、错误业务context、提前归还消耗令牌；不以编译失败或超时作检出。独立审查工具本轮仍在派发前参数解析失败，无成功独立审查。根包nonshort race前六批通过，第七批百万Pool Timeout继续，不把尚未结束的用例记为通过。


## 生产Retry边界原文错误与总预算复核（2026-11-10）

新增 docs/scripts/check_production_retry_boundary.py，逐字提取Retry边界原文并分两独立调用执行。该块不在旧88及其他执行脚本中，去重131→132。原文八行为失败（production-retry-boundary-red-final.log）确认第一次业务error/panic包装、第二次业务/父预取消/早截止/等待取消/超时后success均未返回错误。最初支架把早截止当成功业务及假定同时截止只一尝试，已修正独立期望后再记录最终red，不把这些支架问题算库缺陷。

仅修正文档：第一段检查Run返回；第二段保存RunVoid返回并合并一秒父context错误；明确两调用独立、fn与slowFn签名、imports、无context Run不能自动取消业务、10次重试至多11次尝试、1秒总预算包含退避、取消和timer同时就绪竞争、幂等副作用、panic识别及退避取消时前次业务错误可能不保留。保留原MaxRetries(0/10)、指数退避1至30秒及同步协作取消行为，库运行实现未改。

十场景：首调用success/error/panic；次调用success/error/panic/pre-canceled/earlier-deadline/wait/late-success。核对首调用仅一次、预取消零调用、业务context身份与期限清理、一秒总预算及250ms更早父期限；对定时器与截止时间同到允许1或2次失败尝试，不把允许竞争错当bug。三环境各25轮：Go1.25race1.729s、Go1.26race1.859s、Windows3860.778s；Linux race单轮verified=true（linux_production_retry_boundary_final_race）。docs/工具short audit race6.597/2.098s、1388声明调用/3完整程序及旧88片段编译通过。

六可编译overlay变体均由断言检出（production_retry_boundary_mutants/manifest.json）：丢首调用错误、错误增加重试、丢父错误、丢最终错误、错误业务context、两秒错误预算。原文失败和变体检出均非编译失败或超时。根包第七批百万Pool Timeout已完成通过，其余测试继续，尚未把整批记为通过。


## 监控与Bounded生产生命周期原文复核（2026-11-10）

新增 docs/scripts/check_ratelimit_monitor_example.py，直接提取ratelimit.md的Available监控原文，保持Rate100和五秒ticker，在外围业务将实例Resize(1)使低令牌日志确定发生。五场景return-now/two-ticks/parent-cancel/pre-canceled/deadline检查日志次数、动态速率、父取消后及函数返回后停止日志、limiter关闭后Acquire返回ErrRateLimiterStopped。Go1.25race1.881s、Go1.26race1.797s、Windows3860.817s各25轮及Linux race通过（linux_ratelimit_monitor_race）。四可编译变体删Close/禁告警/15秒周期/错误静态配置速率被断言检出（ratelimit_monitor_mutants/manifest.json），非编译失败或超时。

原文没有确认实现矛盾，仅补无参返回快照、不预留令牌、ctx/imports、五秒和十令牌参数、应在作用域内执行实际业务、defer先停监控再Close、取消tick竞争、阻塞Logger不能被强制中断等说明。该块已在旧88，不增加去重计数。

新增 docs/scripts/check_production_bounded_example.py，逐字提取Bounded分批原文，保留1000许可和256句柄。七成功边界0/1/255/256/257/512/513及五失败模式乘首/第二批位置，共17场景；验证值/顺序、业务context、在途不超过256、error/panic/Goexit正确报告、失败停止后续批、全部已接受回调收尾。drain模式以通道确保后一任务已阻塞一虚拟秒，直接在run返回时读取active与elapsed，再等待测试清理，避免先synctest.Wait掩盖提前返回。

最终三环境各25轮：Go1.25race2.422s、Go1.26race2.729s、Windows3860.707s，Linux race通过（linux_production_bounded_final_race）；初版正常通过后强化直接返回观察，再重验所有环境。六可编译变体丢错误/首错立即返回/错误输入/错误context/漏成功消费/错误批界全部由断言检出（production_bounded_mutants/manifest.json），不以超时或编译失败检出。补业务签名、参数、实际最多256在途、按输入处理成功值、当前批全部等待后返回首错、不汇总其余失败及副作用不回滚。块已在旧88，无新增运行实现或去重计数。

最后docs/工具short audit race6.677/1.973s、1388声明调用/3完整程序及旧88编译通过（production-lifecycle-*.log）。累计24项库运行修复、132去重原文、93公开Examples。根包nonshort Linux race前十五批通过，第十六批百万Adaptive运行时实测仍用CPU、约13GB RSS，未观察断言失败/race告警，不将进行中记为通过。


## 全局启动配置原文与实例生效边界复核（2026-11-10）

新增 docs/scripts/check_production_global_example.py，逐字提取production全局配置块；仅将init声明改为configure以便在测试中保存和恢复全局设置，不自动执行测试包init。不在旧88或其他原文脚本，去重132→133。没有发现库实现缺陷；修正文档将Trace开关说成关闭trace日志、Warn说成减少噪音的含混说明，明确日志器自动绑定与显式日志输出不同，Warn不减少记录次数。

十场景：创建前后及重复配置快照、日志级别/TraceID/错误身份、Pool与Group默认30秒和显式3秒业务期限、两执行器只设全局5秒提交值与显式2秒实例提交期限。提交场景以通道阻塞首任务并填满队列；只设全局值时六虚拟秒后仍在等待，释放业务后成功，显式实例设置则两秒返回ErrSubmitTimeout。业务期限测试直接等待context到期，验证实际任务error及虚拟耗时。所有全局设置串行保存恢复。

三环境各25轮：Go1.25race1.773s、Go1.26race2.084s、Windows3860.787s（production-global-*.log）；Linux race单轮verified=true（linux_production_global_race/manifest.json）。五可编译overlay变体均由行为断言检出：错误任务期限、错误全局提交值、Silent失败级别、开启Trace绑定、清除生产Logger（production_global_mutants/manifest.json）；不以编译失败或超时算检出。首轮原文正常通过，文档说明修正不属于运行缺陷，无新增运行实现改动。

1388声明调用/3完整程序、88原文编译、1415方法结构、378锚点、378围栏、vet和diff检查通过。计数133是已执行去重原文块，不代表每个方法语义已穷举。根包nonshort race前21批通过，第22批分片百万Timeout运行，保留最初整包30分钟预算超时。独立review再次派发前missing field message，未成功启动；不把自审算成独立审查。


## 关闭原文独立作用域与等待统计说明复核（2026-11-10）

新增 docs/scripts/check_production_close_examples.py，提取production关闭方法第二个Go块，按四种独立用法分别执行，不串联在同一个已关闭池上。该块不在旧88编译或其他原文脚本，去重133→134；前一个Run生命周期块早已计入，不重复计数。六场景验证Close/CloseAndWait/WaitAndClose各自正常与业务错误、等待业务结束及关闭后拒绝；三限时场景验证空池立即完成、业务阻塞十秒超时、worker退出但拒绝completion仍阻塞时不能开始cleanup。最后场景用通知门闩确认示例再次Close确实等待完整清理。

首轮错误支架在CloseAndWait后又Wait并要求结果，失败后对照实现确认其已消费唯一Wait，改用预装完成通知观察终态；原文无运行缺陷，不将该支架错误算库bug。最终三环境各25轮Go1.25race1.909s、Go1.26race1.805s、Windows3860.638s（production-close-*.log），Linux race单轮verified=true（linux_production_close_race）。五可编译错误变体均被行为断言检出：普通Close提前消费Wait、CloseAndWait漏Wait、错误十一秒预算、异步清理漏完整Close、丢返回结果（production_close_mutants/manifest.json）。补参数、四种用法相互独立、错误处理、限时不强制业务结束、cleanup仅超时执行与应用退出前另行等待等说明。

另发现docs/pool.md及API生成器仍称任务计数先归零、Active后递减，与现代码defer顺序相反；修正为通知及单次context清理、Busy/Active递减之后再释放等待登记。限时提前返回不保证业务计数归零，Wait也不等待worker退出。新增 internal/pool/wait_counters_contract_test.go：四等待入口Wait/WaitContext/WaitTimeout/WaitAndClose乘success/error/panic/Goexit共16组，回调门闩阻止首项通知，第二worker完成另一项，检查不能提前等完、返回时即刻读取业务计数归零、逐项结果及统计保留。panic/Goexit通知时Busy的defer已执行为零、Active仍1，初版错误要求Busy始终1已修正，不算库bug。

初始单worker支架未检出提前finishTask变体，因为后一排队任务还持有等待登记；加强为双worker后，三个最终可编译变体漏Active清理/漏Busy清理/提前释放登记全部由指定断言检出（pool_wait_counters_mutants/manifest-final.json）。最终三环境各100轮Go1.25race2.240s、Go1.26race2.346s、Windows3861.064s；Linux最终版本race通过（linux_pool_wait_counters_final_race）。旧单workerLinux通过记录不替代最终版本。

最后Pool/docs/生成工具short audit race20.382/6.594/2.027s通过，1388声明调用/3完整程序、88局部原文编译、1415条结构、378锚点/围栏、vet/diff检查通过。运行实现未改，根包长测二进制对应运行源码哈希仍一致；测试新增与文档生成器修改另验。累计24项运行修复、134去重原文、93公开Examples；nonshort根包前26批已完成，第27批百万自定义eq去重为O(n²)慢路径，未将运行中记为通过。


## Pipeline与Flow消费原文错误及生命周期复核（2026-11-10）

新增 docs/scripts/check_pipeline_consumption_example.py 与 check_flow_consumption_example.py，逐字提取“完整示例”下两消费块。两块未包含于旧88编译或已有原文脚本，去重134→136。原文复现日志 pipeline-consumption-before.log / flow-consumption-before.log 保留：Pipeline的parse error/panic/Goexit可能被后续成功覆盖并误存，最终阶段错误、存储错误及父取消只日志或未检查；Flow阶段错误、存储错误和多个错误均未返回，管道处理context没有继承调用方ctx。均为文档用法问题，库运行实现未改。

Pipeline改为完整 consumePipeline：保留8/4并发和1000输出缓冲，使用公开Builder的FailFast及OnResult收集阶段错误，互斥保护与消费端共享的合并错误，Drain检查Occupied/Err后串行存储，返回合并父ctx错误。FailFast可能取消其他在途项，不承诺全部成功项送达；同一错误可能从observer和最终结果重复记录。初版尝试未公开async.ExecutePipe被编译检查发现，在进入最终验证前改用公开API；不将此编辑期错误算库bug。多错支架最初假定两个回调必启动，遇允许取消竞争后增加WaitGroup门闩使两业务先进入，验证已观察到的两错误都保留，不把未启动项当已发生错误。

Pipeline十四场景：success/large1003/empty、两阶段error/panic/Goexit、预取消、成功时取消、业务错与父取消同时、存储错误、两个同时业务错。断言失败parse不能进convert、失败/未完成值不得保存、Context值和存储ctx、错误身份、成功项数、重复写；大输入超过输出缓冲。最终三环境各25轮：Go1.25race2.016s、Go1.26race2.057s、Windows3860.707s（pipeline-consumption-final-*.log），最终Linux race verified=true（linux_pipeline_consumption_final_race）。六可编译变体漏FailFast/漏observer/漏存储错/漏父错/错误存储context/漏保存全部行为检出（pipeline_consumption_mutants/manifest.json），不以编译失败或超时检出。

Flow改为完整 consumeFlow：NewFlow接收ctx，enrich SkipOnError保留前序失败，生产select支持输入空闲时取消；Close残留结果通过容量1完成通道交给同一消费者保存，消费Results后接收残留并等待Close完成，合并已送达处理/存储/提交/父错误。save全在调用方goroutine串行执行，输入msgCh由调用方关闭。取消可能丢弃结果，不承诺保留未送达业务错误或外部副作用全部对账。

Flow十四场景：success/empty/large1031、Close残留、两阶段error/panic/Goexit、存储错误、多业务错、空闲取消、执行中取消。Close残留使用首项存储门闩和后项处理门闩，先阻塞消费者再释放其他结果，使Close确定取走残留，检查每项保存一次；存储调用原子计数断言无重叠。最终三环境各25轮Go1.25race1.836s、Go1.26race1.779s、Windows3860.650s（flow-consumption-final-*.log），最终Linux race verified=true（linux_flow_consumption_final_race）。六可编译变体漏SkipOnError/漏处理错/漏存储错/漏Close残留/错误管道context/漏取消错均行为检出（flow_consumption_mutants/manifest.json），不以编译或超时检出。

补两函数所有参数、签名调用、返回错误、并发、取消、缓冲、panic及幂等建议。Pipeline/docs/生成器short audit race20.004/6.861/1.792s通过（pipeline-flow-consumption-packages.log）；1388声明调用/3完整程序、旧88编译、1415方法结构、378锚点/围栏、vet/diff通过。独立review仍因工具参数接口缺少message在派发前失败，未成功启动，不把自审算独立审查。

根包同一Linux race二进制nonshort续跑前33批verified；第27批总耗时1638.161s，含百万自定义比较去重，原输入、比较器、race和worker未减档。先前单进程整包30分钟预算超时记录保留；尚未完成的批次及SKIP不计为执行通过。累计24项运行修复、136去重原文、93公开Examples，方法结构不是语义穷举证明，目标继续active。


## 第25项：Pipeline的Group路径业务Goexit漏阶段观察修复（2026-11-10）

交叉检查消费原文发现：Pipe最终阶段、Shard、AutoScale的适配器在safeCall普通返回后才调用OnResult，业务runtime.Goexit会退出该goroutine并跳过观察。外层Group虽记录ErrTaskGoexit，仍无法补阶段观察；父context同时取消时Pipe还可能丢弃最终输出，消费者只看到context取消而无法从observer保留已进入业务的Goexit。internal/pipeline/group_observer_goexit_contract_test.go先复现（pipeline-group-observer-before.log），12个业务Goexit及父取消组合失败，Pool对照路径正常；不是编译失败或超时。

在native_chunk.go增加callObservedStage，复用既有finishNativeStage的defer终态发布，正常返回后保留observer对结果panic转换的现有行为，业务Goexit先通知再由外层Group完成异常收尾。pipeline_base.go仅替换execParallelSharded、execParallelAutoScale、execParallelPipe最终阶段三处适配器。不改变普通值/error、阶段调度、结果发送、FailFast和协作取消策略；观察回调自身异常不重放。与旧根包二进制快照比对，运行源码只差native_chunk.go和pipeline_base.go，记录在.gotmp/pipeline_observer_source_delta.json；旧长测证据不能直接归属新版本。

40组四路径pipe/shard/autoscale/pool×FailFast开关×success/error/panic/goexit/cancel-goexit；另12组三受影响路径×FailFast开关×观察回调panic/Goexit，检查已进入业务一次观察、Occupied、value、错误身份、panic包装、父取消下观察仍发生、异常观察不重放。Go1.25race2.650s、Go1.26race3.020s、Windows3861.208s各100轮（pipeline-group-observer-final-*.log）。四可编译overlay变体分别恢复三个旧适配器及漏Goexit通知，均行为检出（pipeline_group_observer_mutants/manifest.json），不以编译失败/超时计检出。

当前版本Pipeline完整nonshort Linux race61.768s通过，两个消费原文包同次通过（linux_pipeline_group_observer_nonshort_race）；当前根包选择^TestPipeline的9个顶层测试nonshort Linux race1.039s通过（linux_root_pipeline_observer_nonshort_race），不把选择性运行说成全根包。Windows Go1.25三个包nonshort audit race61.355/6.605/2.116s通过（pipeline-group-observer-packages.log）。

Pipeline原文新增cancel-goexit场景，保持最终阶段业务Goexit同时取消父context，要求返回同时含ErrTaskGoexit及Canceled；共15场景最终三环境各25轮Go1.25race2.033s、Go1.26race2.012s、Windows3860.842s（pipeline-consumption-goexit-final-*.log），最终Linux race通过（linux_pipeline_consumption_goexit_final_race）。Flow14场景在修复后也三环境各25轮通过（flow-consumption-fixed-*.log）；其运行实现未改。更新专题及生成器OnResult异常终态说明，并重新生成API文档。累计25项库运行修复、136去重原文、93公开Examples。

旧根包nonshort race续跑当前前54/66批verified，第27批1638.161s包含百万自定义比较去重，仍同修复前二进制。保留初次整包30分钟预算超时和SKIP边界；完成后需核清单覆盖并联合当前受影响包补验，不能把源码后来改变倒填进旧二进制。独立review工具参数接口仍无法启动，不宣称独立审查通过；目标active，剩余逐方法语义继续复核。


## 第25项全库回归、独立复审与基础Pipe消费终态（2026-11-10）

旧根包同一Linux race二进制完成166个首轮PASS加66批续跑；.gotmp/verify_root_nonshort_union.py核SHA256、顶层终态/逐批exit0/PASS、无FAIL或race告警、输入清单并集无重漏。441顶层为438 PASS/3 SKIP（linux_root_nonshort_verified_union.json）；三SKIP是MultiGroup/MultiPool/ShardedGroup的FailFast，原测试百/千/万/十万子档均PASS，百万档在父测试显式t.Skip，不能称百万档通过。首轮单进程30分钟包级超时保留；分批未减输入、worker、race或加入short。此二进制属第25项修复前。

第25项修复后完整全库Linux short audit race已终态verified=true（linux_library_observer_final_short_race/manifest.json）：根包680.618s、docs5.670、生成器3.778、core1.276、group46.948、maps1.872、pipeline18.670、pool19.467、ratelimit3.555、retry3.189、shard10.433、sliceops12.652、task3.326，13测试包pass，辅助test无测试。运行源码逐项哈希与本次快照一致；后追加的多阶段测试/账本fuzz由专项证据覆盖，不倒填本二进制。当前Pipeline完整nonshort Linux race61.768s及当前根包9项^TestPipeline nonshort补验仍各自独立，不说成当前根包整包nonshort重跑。

观察测试扩至100组：原52组之外追加三受影响路径×FailFast开关×两目标阶段×success/error/panic/goexit共48组。以实际进入业务次数为预言，观察次数须一致；正常多阶段完整处理、无FailFast时后续可恢复原错误、FailFast中间失败不能进入下一阶段。三环境各100轮Go1.25race4.062s、Go1.26race3.741s、Windows3861.299s（pipeline-group-observer-multistage-*.log），Linux race专项通过。新增独立账本FuzzPipelineGroupObserverTerminalLedger比较各类实际进入终态与观察value/error类别计数，不假定FailFast必启动所有排队业务；race15秒2worker共58,737次通过（pipeline-group-observer-ledger-fuzz.log），种子三环境各100轮2.209/2.406/0.881s及Linux race通过（linux_pipeline_observer_ledger_seeds_race）。输入至多16项、1至4worker；有限模型不穷举全部调度或动态扩缩。

内置spawn接口仍参数解析失败，改用本机codex exec --ephemeral -s read-only独立CLI会话审查此次helper/三调用点、100组测试及两完整消费原文/提取脚本、OnResult文档。该会话最终退出0，限定范围No actionable findings；报告.gotmp/pipeline-observer-independent-review.md。审查者独立确认defer阶段通知在Group终态之前、正常结果/观察panic转换保持、回调Goexit不重放、错误聚合互斥及Flow残留传递和串行存储成立。审查者新Go测试因只读沙箱拒绝创建临时构建目录未执行；其验证为读代码、已有证据、Python语法及原文匹配，不冒称独立新测通过。采样曾遇并发限额重连后完成，不再把独立审查写成从未启动，也不将限定审查算全库独立审查。

继续发现两基础Pipe原文错误：ExecutePipe例打印最终Err后仍handleResult，Serial Pipe例无结果检查；error伴随-99、panic零值均被误作成功处理（pipeline-channel-before.log）。修正两块仅将Ok结果交handleResult，失败记录日志并continue；补fn/context/缓冲参数、handleResult签名、显式日志继续策略、通道关闭不保证成功、中间错误覆盖/FailFast漏输出及Serial Goexit提前结束边界。Serial Goexit原测试初版错误要求后继成功项，按实际既有串行goroutine退出语义修正，不算新库bug，不改变运行实现。

新增 docs/scripts/check_pipeline_channel_examples.py逐字提取两块；两模式×success/empty/error/panic/goexit/pre-canceled共12场景检查最终失败值不消费、成功集合、唯一性、串行异常结束范围。三环境各25轮Go1.25race1.747s、Go1.26race1.714s、Windows3860.688s（pipeline-channel-*.log），Linux race通过（linux_pipeline_channel_race）；四可编译错误变体各段漏失败跳过/漏成功消费均由断言检出（pipeline_channel_mutants/manifest.json）。两块此前不在旧88及其他执行脚本，去重136→138。生成/1388声明调用/3完整程序、旧88编译重新通过（pipeline-channel-api.log / fragments.log）。累计25项运行修复、138去重原文、93公开Examples；API条目1415的结构齐全不代表全部方法语义穷举，目标保持active。


## 三项FailFast百万档显式跳过补验（2026-11-10）

旧根包清单的3个SKIP并非资源失败，而是原测试在tier.Size>100000时于父测试调用t.Skip。为补真实规模执行，.gotmp/check_linux_failfast_million_overlay.py只用Go overlay去掉async_multi_group_test.go、async_multi_pool_test.go、async_sharded_group_test.go各一处父级显式跳过分支，工作树测试文件哈希前后一致；不改变业务断言、百万输入、Shards2、Worker4、Timeout5s及每十项失败规则。选择三项的百万级_1M子用例，在当前实现Linux nonshort race实跑，三子用例和三父项全部PASS，exit0，无SKIP/FAIL，包耗时12.437s、含编译总30.170s（linux_failfast_million_overlay/manifest.json及events.jsonl）。

这是独立overlay补验，不篡改旧长测438 PASS/3 SKIP记录，也不把只检查errCount>0的原断言说成全部生命周期模型验证；精确生命周期由既有单独契约负责。库运行实现与工作树测试代码未因本补验改变。全库只读独立审查正在进行，尚无最终结论。

## 全库独立复审与最终证据核验（2026-11-10）

全库独立CLI只读审查已退出0，最终结论为所审阅代码及改动中未发现可操作运行缺陷或文档问题。十个内部包均经过重点审阅：core的结果/取消/环形缓冲；pool的提交关闭、等待通知、溢出、扩缩容；group的接收登记、聚合、共享取消；shard的路由、背压和等待快照；pipeline的各执行路径、异常阶段通知和流式关闭；sliceops的所有权、索引投影、分块与批量；maps的非自反键和别名合并；task的缓存等待与有界许可；retry的计数边界、预算与异常清理；ratelimit的许可归属、扩缩容、补充和时间窗口。完整原始报告保存在 [独立审查报告](independent-review.md)。

独立审查另逐项对照1,415个API声明与链接源码：1,410直接匹配，余5项是公开函数别名或接口方法表示差异，均已确认；所有条目均有参数、示例、生产建议和注意事项。行为说明经过重点人工对照，没有宣称逐段穷举验证全部语义。该审查没有运行测试或编译，不将主审已有测试结果写成独立执行通过。

主审最终重新运行生成器的-check -compile、旧88局部原文编译、方法结构、锚点、Go围栏、go vet和diff检查。结果为1,388声明调用、3完整程序可编译，1,415方法结构完整，原有378锚点和378围栏均通过；本节及新增独立报告的链接又单独核验。去重原文行为执行累计138块，公开Examples93个；这些计数分别反映执行或结构范围，不能互相替代。

重新核验9份关键Linux manifest均verified=true；全库short快照中的71个库运行源码哈希全部与当前一致。与257项源文件快照相比，只有后增多阶段契约的group_observer_goexit_contract_test.go内容变化，并新增group_observer_goexit_fuzz_test.go；两者有100轮多环境专项和Linux race证据，并已再跑当前Pipeline/docs/生成器包级race。独立审查所称辅助包skip指test包没有测试，不是漏跑有测试的包。

根包nonshort旧二进制清单再次核验：首轮166个PASS与66续跑批次合并，441顶层无重漏，438 PASS/3原测试显式SKIP。3项百万档已通过仅去除父skip的overlay在当前实现Linux nonshort race实际补验，工作树测试文件哈希保持。第25项影响的当前Pipeline完整nonshort及9项公开Pipeline根测试另行通过，保留旧整包30分钟超时和二进制版本界线，不宣称当前根包完整nonshort重新执行。

当前没有待修复的已确认问题。25项运行修复均有原症状复现与回归，关键修复另有模型/fuzz和可编译错误变体的行为检出。公开签名保持；正常值/error、取消、超时、结果所有权等已有契约通过对应回归。有限测试不能证明任意输入与并发调度绝对无bug，也不能保证用户自定义回调的副作用；这些是验证边界，不是本次已知未修复缺陷。审计工作在上述证据范围内闭合，未提交或推送现有工作区改动。

交付前当前包级复跑：go test -race -tags audit -short -count=1 ./internal/pipeline ./docs ./docs/cmd/apidoc，三包分别20.202s、6.955s、2.088s通过（.gotmp/final-reviewed-packages.log）；文档更新后再验1415方法结构、378专题锚点/围栏与git diff --check均通过。正式独立审查报告和本次新增链接另按实际目标文件与行号核验。
