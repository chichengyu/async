# 全库审查结果与边界（2026-11-05）

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

最新补充（2026-11-09）：第二十项运行缺陷已复现并修复：Stream/Flow业务Goexit导致worker退出，Stream未完成槽位返回、Flow已接受输入遗失且Active不归零。业务退出登记零值/ErrTaskGoexit、同一worker登记名额接替、defer转发和计数清理。12组确定契约及混合结果种子三环境各100轮通过，三路径独立模型race fuzz72,517次通过，六可编译错误变体被行为断言识别；89公开示例五独立race进程通过，全内部/docs/工具short race及Go1.25/386包级通过。第二十项全库short普通全部通过（根包438.585s），441项根包short audit race经74新进程全部通过，清单无重漏且二进制SHA256已核验。详见 [Stream 与 Flow 业务异常退出及已接受输入修复](#stream-与-flow-业务异常退出及已接受输入修复2026-11-09)。

最新补充（2026-11-09）：第十九项运行缺陷已复现并修复：原生Pipeline业务Goexit结束整块goroutine，后续项未执行却返回零值成功，Meta遗失记录；默认构建器观察包装还会漏通知。八处原生分块共享异常接替与单项defer终结，正常分块复用方式不变。21组确定场景及混合结果种子三环境各100轮通过，7入口独立结果模型race fuzz84,046次通过，三个错误变体被行为断言识别。88公开示例五独立race进程及全内部/docs/工具short race通过；本版本全库short普通全部通过（根包436.566s），441项根包short audit race经74新进程全部通过，清单并集与二进制SHA256已核验。此证据对应第十九项版本，后续Stream/Flow修复另行重验。详见 [原生 Pipeline 分块异常退出与阶段结果修复](#原生-pipeline-分块异常退出与阶段结果修复2026-11-09)。

最新补充（2026-11-09）：第十八项运行缺陷已复现并修复：Pool业务Goexit遗失结果/completion及worker容量，Group还遗漏任务计数收尾，导致等待不能完成。增加ErrTaskGoexit、业务退出记录、defer计数收尾及原登记名额下worker接替。22组确定契约和混合结果种子三环境各100轮通过，六错误变体识别；Pool/Group单worker race fuzz最终分别44,986/58,231次通过，87公开示例五独立race进程通过。最终缩容修正版全库short普通全部通过（根包434.041s），441项根包short audit race经74个新进程全部通过，清单并集无重漏、二进制SHA256已核验。此证据对应第十八项版本；后续原生Pipeline修复单独重验。详见 [Pool 与 Group 业务 Goexit 完成登记及 worker 容量修复](#pool-与-group-业务-goexit-完成登记及-worker-容量修复2026-11-09)。

最新补充（2026-11-09）：第十七项运行缺陷已复现并修复：Bounded四入口取得槽位后、trace日志注入若panic/Goexit，尚未启动业务且许可遗漏。提交方增加启动失败defer，成功后交由回调原defer释放。48组契约三环境各100轮、四错误变体、49,549次race fuzz及全部内部包short race通过；86公开示例五独立race进程通过，本版本全库short普通全部通过（根包435.226s）。本版本441项根包short audit race完整清单经74个新进程全部通过，清单并集无重漏。详见 [BoundedRunner 启动前异常槽位归还修复](#boundedrunner-启动前异常槽位归还修复2026-11-09)。

最新补充（2026-11-09）：第十六项运行缺陷已复现并修复：RetryChain业务Goexit或恢复日志后端panic跳过普通Release，四类许可未归还。改为每次尝试局部defer；52组许可契约及既有8组三环境各100轮通过，另10组Adaptive统计三环境各100轮通过，十个错误变体被行为断言识别。85公开示例五独立race进程通过，全内部short race及本版本全库short普通通过（根包437.686s）。详见 [Retry 许可异常退出配对释放修复](#retry-许可异常退出配对释放修复2026-11-09)。

最新补充（2026-11-09）：第十五项运行缺陷已复现并修复：RetryWithConfig及Void回调Goexit或panic日志后端再次panic时，跳过普通cancel而保留单次context。改用既有WithTimeout的defer清理，正常重试语义保持。98组新增生命周期契约及既有24组计时契约三环境各100轮通过；原缺陷两路径overlay均复现，六辅助函数错误变体被识别。84个示例五独立race进程通过，全内部包/docs/工具short race及第十五项版本全库short普通全部通过（根包436.943s）。详见 [Retry 单次 context 异常退出清理修复](#retry-单次-context-异常退出清理修复2026-11-09)。

最新补充（2026-11-09）：Result/PanicError新增28组错误链、类型化nil、格式动词、输出失败及引用契约，三环境各100轮通过，五错误变体被行为断言识别。补全宽度/精度不生效、%q不转义、Result不转发栈标记等说明；83公开示例五独立race进程通过。本轮运行实现不变，累计十四项修复。详见 [Result 错误链与格式化边界复核](#result-错误链与格式化边界复核2026-11-09)。

最新补充（2026-11-09）：Drain新增18组同步/取消/panic/空结果契约，三环境各100轮通过，四错误变体被识别；修正Pipeline参数说明的Serial/Flow/Drain混用概括，82公开示例五独立race进程通过。TokenBucket四worker race fuzz异常捕获TSan内存分配1455；完全不导入本库的synctest最小程序复现EOF及同类报告。本轮运行实现不变，累计十四项修复。详见 [Pipeline 消费回调与 fuzz 检测器诊断](#pipeline-消费回调与-fuzz-检测器诊断2026-11-09)。

最新补充（2026-11-09）：Pipeline/Pipe/Flow共167组配置捕获、输入共享、重复执行及消费契约，三环境各100轮通过，12个错误变体被行为断言识别。修正Pipe配置捕获时点、Serial中间FailFast错误不输出等文档；81个公开示例五独立race进程通过。本轮运行实现不变，累计十四项修复。根包第十四项版本441项short audit race完整清单重验通过：前220项分批、余221项独立进程，GOMAXPROCS=2，测试规模未改；初始第12批TSan内存分配1455仍保留，不能称单进程全套通过。全内部包/docs/工具最新short race也全部通过。详见 [Pipeline 配置捕获与重复执行交叉复核](#pipeline-配置捕获与重复执行交叉复核2026-11-09)。

最新补充（2026-11-09）：第十四项运行缺陷已复现并修复：Pool/Group把有限大缩放乘积先转int，扩容错误只加1、缩容错误直达Min；现先应用相应边界再转换。56组数值矩阵及20组公开ticker契约三环境各100轮通过，四错误变体识别。第十四项版本全库short普通全部通过（根包438.235s），全内部包/docs/工具short race通过；末增测试另有最终包级证据。详见 [AutoScale 有限大系数转换溢出修复](#autoscale-有限大系数转换溢出修复2026-11-09)。

最新补充（2026-11-09）：Task结果引用所有权144组，Go1.26/Go1.25 amd64 race及Go1.25 Windows386各100轮通过；五错误变体识别，79个公开示例五独立race进程通过。补AsyncResult浅拷贝及引用读写责任，原文局部编译增至88组；本轮运行实现不变，累计十三项修复。详见 [Task 结果引用所有权与通知交叉复核](#task-结果引用所有权与通知交叉复核2026-11-09)。

最新补充（2026-11-09）：Retry 默认配置与父取消共160组虚拟时间契约，Go1.26/Go1.25 amd64 race和Go1.25 Windows386各100轮通过，六个可编译错误变体识别；78个公开示例五个独立race进程通过。补重置范围、每次执行独立额度和DefaultShards区别，原文局部编译增至87组。本轮运行实现不变，累计十三项修复。详见 [Retry 默认配置保留与重复执行交叉复核](#retry-默认配置保留与重复执行交叉复核2026-11-09)。

最新补充（2026-11-09）：MultiGroup混合轮转/keyed、uint64回绕、取消拒绝和已关闭片共54组，三环境各100轮通过；四错误变体识别。补直接取模、拒绝仍轮转、合并顺序及单worker前提，77个公开示例显式五进程race通过；本轮运行实现不变，仍十三项修复。详见 [MultiGroup 混合路由与计数回绕交叉复核](#multigroup-混合路由与计数回绕交叉复核2026-11-09)。

最新补充（2026-11-09）：第十三项运行缺陷已复现并修复：FillNoResultSkipped最小负目标减去非零总数溢出，误入巨量追加；改为先比较再减。486组聚合/稀疏/重复索引/跳过项契约三环境各25轮通过，四错误变体识别；公开示例增至76。详见 [NoResult 聚合槽位与最小负目标溢出修复](#noresult-聚合槽位与最小负目标溢出修复2026-11-09)。

最新补充（2026-11-09）：RingBuffer指针元素、五种存储形状、四策略和极端FlushN数量共480组，三环境各25轮通过；四个错误变体识别，core/pool/docs/工具short race通过。补内部槽位清零、独立返回切片、浅拷贝和Reset责任说明，运行实现不变，仍十二项修复。详见 [RingBuffer 引用清理与极端批量交叉复核](#ringbuffer-引用清理与极端批量交叉复核2026-11-09)。

最新补充（2026-11-09）：Slice的Take/Drop/SliceRange及兼容Slice共1431组极端索引、空/偏移视图契约，三环境各25轮通过，四个错误变体识别。补清空保留起点容量、共享槽位及MinInt/MaxInt说明；包级short race通过，本轮运行实现不变，累计十二项修复。详见 [Slice 极端范围与视图容量交叉复核](#slice-极端范围与视图容量交叉复核2026-11-09)。

最新补充（2026-11-09）：修复第十二项运行缺陷，Adaptive合法int32边界在32位初始化中点相加溢出；采用min+(max-min)/2。48组四构造入口契约三环境各100轮、32位大容量四入口实际构造、三个错误变体、30秒race fuzz通过；修复后全库short普通及全内部包short race通过。详见 [Adaptive 初始容量中点溢出修复](#adaptive-初始容量中点溢出修复2026-11-09)。

最新补充（2026-11-09）：限流作用域关闭共27组虚拟时间契约，Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过；三个错误变体被识别。补齐等待唤醒、BlockForce、业务不自动等待、关闭后释放及内部便捷函数可见范围说明。本轮不改运行实现，累计十一项修复。详见 [限流作用域关闭与在途业务交叉复核](#限流作用域关闭与在途业务交叉复核2026-11-09)。

最新补充（2026-11-09）：实测修复两项重试整数边界缺陷，累计十一项运行修复：最大int预算显示负数，以及RetryFn耗尽后计数溢出而超额执行。154组有限契约三环境各一百轮通过，32位完整21亿次预算连续三轮通过，四个错误变体被识别；全内部包short audit race通过。详见 [Retry 最大次数计数与耗尽溢出修复](#retry-最大次数计数与耗尽溢出修复2026-11-09)。

最新补充（2026-11-09）：Slice/Map四种模式切换各36组、共72组契约，Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过；四个错误变体识别，75 Examples显式五进程race通过。补源配置保留、数据视图及旧池责任说明，原文局部编译增至86组，运行实现不变。详见 [Slice 与 Map 模式切换状态交叉复核](#slice-与-map-模式切换状态交叉复核2026-11-09)。

最新补充（2026-11-09）：Slice过滤阈值、单worker、DeleteFunc、引用及异常共128组契约，Go1.26/Go1.25 amd64 race及Go1.25 Windows386各二十五轮通过；四个错误变体识别，74 Examples显式五进程race通过。补原地压缩/新数组与panic不回滚说明，原文局部编译增至85组，运行实现不变。详见 [Slice 过滤所有权与异常传播交叉复核](#slice-过滤所有权与异常传播交叉复核2026-11-09)。

最新补充（2026-11-09）：Task四种执行入口的提前等待及Cancel窗口共48组虚拟时间契约，最终Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过，四个可编译错误变体识别。补缓存优先、等待错误与业务错误、许可及通知生命周期说明；原文局部编译增至84组，本轮运行实现不变。详见 [Task 等待提前返回与最终结果交叉复核](#task-等待提前返回与最终结果交叉复核2026-11-09)。

最新补充（2026-11-09）：Core等待辅助函数24组虚拟时间契约，最终Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过，五个可编译错误变体识别。补同步取消/快照、重复取消、onDone顺序及监视退出说明，纠正源码中已过时的Pool/Group调用关系；原文局部编译增至82组，运行实现不变。详见 [Core 等待辅助函数与监视生命周期交叉复核](#core-等待辅助函数与监视生命周期交叉复核2026-11-09)。

最新补充（2026-11-09）：Pool限时关闭与阻塞提交同步通知四组受控契约，最终Go1.26/Go1.25 amd64 race及Go1.25 Windows386各一百轮通过，三个可编译错误变体识别。补全worker信号、完整清理、Pending/失败计数、context及Reset说明；局部Markdown原文编译增至81组。本轮无运行实现修改，详见 [Pool 限时关闭与阻塞提交通知交叉复核](#pool-限时关闭与阻塞提交通知交叉复核2026-11-09)。

最新补充（2026-11-09）：TokenBucket时间补充独立整数模型、24组空闲/拒绝边界及24组公开Retry退避交叉契约通过；最终race fuzz 95,238次，三平台定向各一百轮，五个可编译错误变体识别。73 Examples显式五次独立进程race通过，ratelimit/retry/docs/工具short audit race通过（7.343/4.860/7.837/3.055s）。本轮不改运行实现，累计九项修复保持，详见 [TokenBucket 补充额度与重试交叉复核](#tokenbucket-补充额度与重试交叉复核2026-11-09)。

最新补充（2026-11-09）：Group／NoResult 等待超时、同步结果通知及 Reset 共12组受控契约，最终 Go1.26/Go1.25 amd64 race、Go1.25 Windows386各一百轮通过；三个可编译错误变体被识别。72 Examples显式五次独立进程race通过，group/pool/docs/工具short audit race通过（48.030/22.755/7.217/2.663s）。本轮未改运行实现，累计九项修复保持，详见 [Group 等待超时与同步通知交叉复核](#group-等待超时与同步通知交叉复核2026-11-09)。

最新补充（2026-11-09）：Pool 结果转换、业务统计/FailFast、存储/流/环形缓冲、同步回调顺序及拒绝通知共28组契约，Go1.26/Go1.25 amd64 race、Go1.25 Windows386实测各一百轮通过；四个可编译错误变体被识别。71 Examples显式五次独立进程race通过，pool/docs/工具short audit race通过（21.883/6.145/3.089s）。本轮未改运行实现，累计九项修复保持，详见 [Pool 结果转换与同步通知交叉复核](#pool-结果转换与同步通知交叉复核2026-11-09)。

最新补充（2026-11-09）：Pipeline 七种路径、FailFast 开关、父期限及观察函数共 58 组虚拟时间契约，Go1.26/Go1.25 amd64 race 和 Go1.25 Windows 386 实测各一百轮通过；四个可编译错误变体被识别。新增观察时点示例后共 70 Examples，显式五次独立进程 race 通过；pipeline/docs/工具 short audit race 通过（22.200/7.632/4.019s）。本轮没有修改运行实现，累计九项修复保持，详见 [Pipeline 共享期限与观察函数生命周期交叉复核](#pipeline-共享期限与观察函数生命周期交叉复核2026-11-09)。

验证次数更正（2026-11-09）：Go testing 对 Example 每个测试进程只执行一次，-count 不会重复 Example。本文历史命令中的 -count=5/25/100 只证明普通 Test 的对应重复次数；此前将 Example 写成五轮、二十五轮或一百轮的表述均以本更正为准，不能据此计算示例运行次数。当前 73 个可执行示例另以显式五次 go test -count=1 进程调用完成 race 验证，日志为 .gotmp/bucket-timing-examples-1.log 至 -5.log；最低版本及 386 新示例各有一次实际执行证据。

最新补充（2026-11-09）：Slice 批处理 432 组独立模型组合，Go1.26/Go1.25 amd64 race、Go1.25 Windows 386 实测各二十五轮通过；四个可编译错误变体被识别。69 Examples race 单次命令（传 -count=5，每例实际一次）及 sliceops/docs/工具 short audit race 通过（15.756/6.371/4.515s）。补极值、失败批次映射和幂等说明，本轮无运行实现修改，累计九项修复保持。详见 [Slice 批处理极值与失败批次映射交叉复核](#slice-批处理极值与失败批次映射交叉复核2026-11-09)。

本轮以代码为准，从正常调用、取消/超时、非法边界、资源关闭、错误反查和跨组件组合检查。起始已有的代码注释及 Markdown 修改予以保留。以下证据不能证明所有输入和 goroutine 调度绝对无缺陷。

最新补充：[Group 外部池双层期限与结果交叉复核](#group-外部池双层期限与结果交叉复核2026-11-09) 完成十四组外部池执行/提交期限、结果与 FailFast 契约，Go1.26/Go1.25 amd64 race 和 Go1.25 Windows 386 实测一百轮通过，五个错误变体被识别。补两层队列/期限、业务错误与包装回调成功的区别及所有者生命周期说明；本轮无运行修改，示例增至 68。

最新补充：[分片批量背压与部分提交交叉复核](#分片批量背压与部分提交交叉复核2026-11-09) 完成十五组热点片/三溢出策略/父期限/KeyFn panic 契约，Go1.26/Go1.25 amd64 race 和 Go1.25 Windows 386 实测一百轮通过，四个错误变体被识别。补逐项提交期限与整体预算、Drop nil 返回及部分提交收尾说明；不改运行实现，公开示例增至 67。

最新补充：[Task Builder 超时与句柄生命周期交叉复核](#task-builder-超时与句柄生命周期交叉复核2026-11-09) 完成四终端共 32 组虚拟时间契约，Go1.26/Go1.25 amd64 race 和 Go1.25 Windows 386 实测一百轮通过，三个错误变体均被行为断言识别。补充父期限覆盖排队、回调期限与外层 Task.Ctx 的差异及完成通知规则；本轮不改运行实现，示例增至 66。

最新补充：[Stream 投递取消与清理交叉复核](#stream-投递取消与清理交叉复核2026-11-09) 验证八组背压/取消/触发错误/期限及原生流丢弃契约，Go1.25 amd64 race 和 Windows 386 实测一百轮通过，五个错误变体均被识别。补充两种 Stream 通道、父取消与任务取消、回调期限与结果投递的区别；运行修复仍九项，公开示例增至 65。

最新补充：[Map 池化结果与保留策略交叉复核](#map-池化结果与保留策略交叉复核2026-11-09) 完成 128 组公开构建器组合、四个错误变体、含借用池及部分批次失败的模型验证；补充 MaxResults 内存边界和构造/执行时条目复制说明。本批没有新增运行缺陷，示例总数增至 64；先前全库验证的资源及独立复核缺口仍保留。

最新执行：[ForEach 超时首错顺序复核](#foreach-超时首错顺序复核2026-11-09) 记录第九项版本全库 short race 的终态：根包及其余包通过，sliceops 的既有测试因无序首错假设失败；已复现并修正测试，运行实现未因此修改。新增结果契约与修正测试合并 race 二百轮通过；串行包级全库重验中其余包全部通过，根包 TSan 资源失败，根包 441 项按完整清单分批重验已全部通过（前 220 项默认设置、后 221 项 GOMAXPROCS=2）。第九项运行修复及其定向证据见下文，以下历史进展和未完成边界继续保留。

当前累计二十四项运行逻辑修复、93个可执行示例、1415个方法参考条目、88组原文局部编译及六个日志字段原文签名。第二十四项全库普通short及441项根包分进程short race全部通过；最后新增构建器清理契约和Task.Result模型有随后包级验证，未倒填旧二进制证据。文档结构覆盖不代表全部语义完成；Linux实际执行、Windows非short百万race及独立复核仍有边界，目标active。下文保留各版本历史证据，较早的计数及进行中措辞仅描述写入时状态。

## 已复现并修复

| 缺陷 | 复现证据 | 根因与处理 | 兼容性 |
|---|---|---|---|
| Slice 全局去重忽略 eq | EqualFold 四个入口返回错误集合；[][]int 触发 unhashable type panic | 新旧入口共用比较函数驱动实现，保留首个代表值，清空压缩后的尾部引用 | 公开签名不变；非 nil 比较器最坏 O(n²)，大批量数据建议按业务键哈希索引；nil 保留 Go 相等性路径 |
| ForEachPool 可能把失败返回为成功 | 全局 MaxResults=1，输入两项，首项成功、次项失败，修复前返回 nil | 原实现依赖只查看保留结果的 FirstError；改用每项终态通知独立汇总，按输入顺序保留首个错误 | 不扩大结果存储，不改 FirstError 语义，正常成功路径和返回类型不变；上限外错误现在正确返回 |
| RetryChain TokenBucket 忽略 Rate(n) | Rate(3) 未配置 Capacity 时，修复前可取 7 个令牌，预期默认容量为 6；回归失败已记录 | Rate 写入 rlRate，但 TokenBucket 读取永远未赋值的 tbRate；移除该孤立字段，两模式共用 Rate(int) 与默认值路径 | 不改公开签名；保留模式选择顺序无关；TokenBucket 的非默认显式速率现在正确应用，SlidingWindow 的 Limit 不变 |
| Pool 提交与关闭任务通道的数据竞争 | 并发提交/取消/双 Close 的 race fuzz 在 trySend 的 chansend 与 Close 的 closechan 间报告 DATA RACE，完整堆栈保存于 .gotmp/pool_close_tsan.* | addInFlight 只保护到任务登记，不覆盖随后入队；recover 不能同步通道发送与关闭。blockSend/trySend 及普通/限时关闭共用任务发送读写锁，先广播 done 唤醒阻塞发送再锁定关闭 | 公开签名、队列、错误和回调规则保持；只为发送与关闭建立同步，新增锁存在少量提交开销，生产吞吐仍需按实际负载测量 |
| 分片共享取消链的提前取消及遗漏释放 | Pool Builder 的正常首片 Wait 取消 Run context；整体 Pool/Group Wait 后共享派生 context 仍存活；重复配置的旧派生 context 也未释放，均有修复前失败 | Run 改用独立共享层 context，各片保留子 context；所有片等待/关闭完成后释放共享取消链，重复配置合并取消函数，成功 Reset 清理旧引用 | 公开签名与 Ctx() 返回第 0 片的契约不变；正常跨片任务不再提前取消；整体收尾后共享 ctx.Err() 会为取消，业务成功与否应检查提交错误和逐项结果 |
| 空 Slice.Repeat 随重复次数空转 | 四入口对空输入 Repeat(MaxInt) 均在五秒子进程期限内未返回 | sliceBase.Repeat 与旧 Slice.Repeat 在空输入时直接返回，避免 n-1 次空追加 | 公开签名、空视图容量/引用、非空拼接值及引用共享不变；只去除无元素工作时的次数循环 |
| Map 非自反键的过滤失效和值变换新增条目 | 四入口 Filter/Reject 未删除三个应移除的 NaN/复合键；MapValues 再次遍历新插入的 NaN 条目，触发调用上限回归断言 | Go map 的 NaN 键不等于自身，delete/赋值不能定位原条目；共享条目处理函数暂存非自反结果，必要时 clear 并重建同一个 map | 签名和普通键即时修改保持；FromRef/AsMap 别名继续有效，额外空间最坏 O(n)。普通按键查询/删除保留 Go 相等性，不把 NaN 定义为相等 |
| Map 别名合并重复复制新插入的 NaN 条目 | 四入口四种批量合并共 16 组，65 项输入自合并预期 129 项，修复前实际 154～176 项且随遍历变化 | 同一底层 map 的 range 可再次访问新插入条目；先暂存源非自反条目，遍历结束后逐项插入一次 | 普通键覆盖/默认值、返回接收者、底层 map 引用及 nil 目标 panic 保持；NaN 自合并仍新增每个源条目一次，额外空间 O(k) |
| 滑动窗口最小负时长取负溢出 | MinInt64 窗口的 Allow/AllowN 保留旧记录而错误拒绝，两个回归修复前失败 | 非正窗口直接取 now 为截止点，仅正窗口才取负，避免 MinInt64 溢出 | 保留签名、锁、单次限额、非正请求和正窗口边界；非正窗口须由生产配置入口校验 |
| 重试总预算格式化溢出 | MaxInt首次失败日志显示负总次数，九入口十八个边界子例失败 | 日志及耗尽错误先将非负重试次数转uint再加1 | 不截断预算，不改执行流程及错误包装，最大预算耗尽显示未完整实耗验证 |
| RetryFn耗尽后超额执行 | Windows386实耗2,147,483,648次后仍额外调用业务，额外成功使原有失败返回nil | 最后失败保存错误后、计数递增前break | 常规次数、成功、panic及最终错误包装保持；修复后三轮完整32位预算通过 |
| Adaptive初始中点相加溢出 | Windows386下1与MaxInt32的中点应为1,073,741,824，实际回退到默认56 | 归一化边界采用min+(max-min)/2 | 保留中点向下取整及非正/颠倒边界修正，构造及许可操作实测；int32容量范围与初始化预算仍需遵守 |
| FillNoResultSkipped负目标溢出导致错误追加 | amd64下单个成功任务最终Wait后传入MinInt，隔离子进程观察到计数增长并退出3，修复前回归失败 | 先比较目标与当前非负总数，仅需要增长时再做减法 | 签名、正数填充顺序、统计和通知规则保持；非正/较小目标安全无操作，正数增长仍需业务内存预算 |
| Pool/Group自动扩缩容有限大系数转换溢出 | 初始3、Min1/Max5，系数1e20/1e300/MaxFloat64的扩缩共12子例修复前全部失败，扩至4而非5、缩至1而非2 | 原先先把超范围float乘积转int再限界；现先应用扩容上限或缩容当前值边界，再转换并保留既有增减1规则 | 公开签名和正常有限系数、确认次数、生命周期及上下界规则保持；实际创建最多5worker验证，NaN/Inf与不合法worker预算仍须配置校验 |
| RetryWithConfig单次context异常退出未清理 | Goexit回归在config/config-void两子例观察child.Err()==nil；旧实现overlay亦复现日志后端panic清理失败 | 正常调用后的cancel无法覆盖栈展开；复用WithTimeout的每次调用defer，退出时立即清理子context | 不把Goexit改为错误，不吞日志后端panic；保留正常值/错、业务panic恢复、次数、退避和父取消规则 |
| RetryChain异常退出遗漏许可释放 | 四模式×两单次超时×Goexit/日志后端panic共16组在旧实现观察许可未归还 | 业务调用后普通Release无法覆盖栈展开；每次尝试局部defer在正常Adaptive记录后释放，退避前归还，获取失败不注册 | 保留正常值/错、业务panic恢复、统计和重试时序；Goexit及日志后端panic保持原退出语义，消耗型额度不退款 |
| BoundedRunner启动前异常遗漏槽位归还 | 四入口×logger.With/WithContext×panic/Goexit共16组在原实现均Available0/Busy1，业务未调用 | 获取许可后同步EnsureTraceID可异常，worker释放defer尚未建立；提交方局部defer保护启动阶段，成功返回句柄后交给原业务defer | 保留日志异常传播/Goexit、原trace时点、正常许可持有期、取消拒绝和值/错；不新增恢复或自动重试 |
| Pool/Group业务Goexit遗漏终态与worker容量 | 原Pool Active1、后继已接受任务等待超时且无Goexit completion；原Group Active1/Busy1/Fail0且Wait超时 | 未返回业务跳过结果和普通计数清理并结束worker；用ErrTaskGoexit记录终态，defer完成计数，原worker登记名额交给后继 | 普通值/错及panic恢复保持，不重试业务、不把Goexit包装为panic；FailFast、索引、外部Pool、缩容/关闭/复位均覆盖 |

回归代码：`internal/sliceops/dedup_regression_test.go`、`internal/pool/helper_regression_test.go`。ForEachPool 定向 race 随机顺序运行 5 轮通过，另覆盖错误顺序和取消前拒绝。

Retry 回归见 `internal/retry/rate_regression_test.go`，包括配置顺序和 DefaultRate 恢复；整个 Retry 包 race 随机顺序 3 轮通过（71.177s，`.gotmp/audit-retry-rate-race.log`）。旧文档的 RetryChain.Rate(float64) 签名不存在，已修正为 Rate(int)；独立 Ratelimit 构建器仍支持小数速率。

## 已核查的行为

- Pool：提交保护、拒绝结果、取消登记、结果上限、stream、ResetWait、关闭清理；错误查询只覆盖主结果存储，统计可包含上限外失败。
- Group/Shard：外部池组合、FailFast 的上下文传播、等待超时快照、分片配置与生命周期。
- Pipeline：满结果缓冲下 Close 的排空、panic 和阶段清理；回调不响应取消时无法保证及时关闭。
- Task：结果缓存和重复等待、等待取消与任务取消的区别、BoundedRunner 槽位释放。
- Retry/RateLimit：取消前预检查、退避、低速补充、负请求量、Resize 与 Acquire/Release 组合。
- Slice/Map：共享输入、顺序、同步操作与并发终端操作的区别、自定义相等关系。

这些是检查主题，不能当作每个方法每个分支都经过动态执行的声明。已有 audit 复现套件覆盖上述部分组合，未复现的问题不按缺陷修改既有语义。

## 验证记录

环境：Windows amd64，Go 1.26.0，CGO_ENABLED=1。系统 GOROOT 指向不完整目录，命令进程临时使用 `D:/soft/go`；缓存与临时文件放在本仓库被忽略的目录。

| 命令/范围 | 结果 |
|---|---|
| go vet -tags audit ./... | 通过，含文档生成器；18:44 对当前源代码再次检查通过 |
| go test -tags audit ./docs -count=5 -shuffle=on -timeout=60s | 通过 |
| go test -short -race ./... -count=1 -timeout=30m | 通过，根包 649.356s；发生在 ForEachPool 修复之前 |
| go test -short -race -tags audit ./internal/... ./docs/... -count=2 -shuffle=on -timeout=5m | 通过，发生在 ForEachPool 修复之前 |
| go test -race ./internal/pool -run 'Test(ForEachPool|Helper_)' -count=5 -shuffle=on -timeout=2m | 通过，覆盖 ForEachPool 修复 |
| go test -tags audit ./internal/... ./docs/... -count=1 -timeout=10m | 通过，不使用 short，包含 ForEachPool 修复 |
| go test -tags audit -race -short ./... -count=1 -timeout=30m | 通过，根包 640.126s，包含两项修复及 audit 复现套件 |
| go test -race -tags audit ./docs -count=3 -shuffle=on -timeout=60s | 通过，含九个根包可执行示例，11.816s |
| go run ./docs/cmd/apidoc -check | 签名漂移及本地文件链接检查通过；不检查站外链接和页面锚点 |
| go run ./docs/cmd/apidoc -check -compile | 1388 个声明调用编译通过；README 两个完整 main 和 config 一个完整 main 编译通过；不执行这些片段 |
| Dedup fuzz，30s，4 workers | 通过，1,184,483 次执行；独立等价类与 nil 比较器预言检查，输入长度上限 512 |
| go test -race ./internal/retry -count=3 -shuffle=on -timeout=5m | 通过，71.177s，包含 TokenBucket Rate 修复 |
| 修复后内部包非 short race、随机顺序一轮 | core/group/maps/pipeline/pool/retry/shard/sliceops 通过；ratelimit/task 因 ThreadSanitizer 分配内存失败退出；docs 新示例曾出现结果顺序假设错误，已纠正 |
| 逐包 race 重跑 task/retry/docs | 通过，分别 122.342s / 22.858s / 6.323s；包含后来增加的分片 Rate 回归和 GoAt 示例 |
| ratelimit 非 short race，GOMAXPROCS=4 | 在 TestRateLimiter_Token_Concurrent/百万级_1M 因 ThreadSanitizer 内存分配失败退出（1455），不计为通过 |
| ratelimit/docs short race，逐包随机顺序两轮 | 通过，分别 6.984s / 8.466s；不能替代未完成的百万级 race 检查 |
| Retry Rate 定向 race 随机顺序十轮 | 通过，含配置顺序、默认恢复与分片构造路径 |
| go test -v -tags audit . -count=1 -timeout=90m | 通过，5178.433s；包含百万级实际任务，自定义比较器去重 1M 子用例耗时 536.27s |
| 三项缺陷定向回归及全部 Example，race 随机顺序三轮 | 通过，sliceops 4.361s / pool 4.169s / retry 2.577s / docs 4.125s；含十个可执行示例，日志 audit-cross-latest.log |
| go test -race -short -tags audit -p 1 ./internal/... ./docs/... -count=1 -shuffle=on -timeout=5m | 全部通过；发生在三项运行逻辑修复和十个示例之后，日志 audit-internal-latest-short-race.log |
| 本地 Markdown 片段锚点核查 | 204 个本地 Markdown 锚点，0 个未匹配；临时脚本 .gotmp/check_anchors.py，独立于生成器 -check，不含站外链接 |
| 修正后的 IO / Group.WithContext 源码使用片段抽取编译 | 通过；补齐测试业务函数及导入，修正未定义变量、公开类型别名和 Submit 返回值用法；只编译不执行 |
| 去注释 Go 语法树差异对照 HEAD | 已有声明的运行逻辑变化仅为 ForEachPool、Dedup/DedupFunc 及共享实现、RetryChain 速率字段与读取；其他跟踪源码差异为注释 |
| 内部包及 docs 的 short audit 覆盖率运行，逐包一次 | 通过；core 90.5%、group 76.4%、maps 91.2%、pipeline 89.9%、pool 81.5%、ratelimit 91.5%、retry 96.4%、shard 93.0%、sliceops 79.1%、task 96.7%。不含根包测试，不能作为全库合并覆盖率；profile 为 .gotmp/audit-internal-coverage.out |
| 18 个可执行 Example，race 随机顺序五轮 | 通过；13 个根包示例、4 个 Slice 分块示例及 1 个 Retry 超时包装示例；retry 3.387s / sliceops 5.372s / docs 4.642s。覆盖分块统计、零块大小、批内共享期限、零超时覆盖差异 |
| 19 个可执行 Example，race 随机顺序五轮 | 通过；在前述基础上增加无池 Map FailFast context 对照，retry 3.487s / sliceops 5.038s / docs 4.194s |
| RingBuffer 状态机 fuzz，30s，4 workers | 通过，8,629,633 次执行；独立 FIFO 切片模型对照混合 Push/Pop/Peek/Flush/FlushN/Reset、满缓冲拒绝、丢弃计数与元信息；容量 1~32，单输入最多 2048 步，不替代并发调度检查 |
| 19 个示例及 RingBuffer fuzz seeds 纳入后的内部包/docs short audit race | 全部通过，逐包随机顺序一轮；core 3.564s / group 48.987s / maps 4.980s / pipeline 21.610s / pool 22.934s / ratelimit 6.852s / retry 5.860s / shard 14.844s / sliceops 11.127s / task 7.205s / docs 7.055s |
| README 七组局部片段抽取编译 | 通过；AsyncResult、TaskHandle、AsyncErr、TaskErr、同/跨类型 Map、跨类型 MapBatch、Mu；仅补导入、业务桩与未使用值处理，未执行，脚本 .gotmp/check_readme_fragments.py |
| 20 个可执行 Example，race 随机顺序五轮 | 通过；增加最终尝试被限流拒绝时错误链与零值行为的示例；retry 3.106s / sliceops 4.730s / docs 4.043s |
| 21 个可执行 Example，race 随机顺序五轮 | 通过；增加 Map.Reduce 与 Slice.Reduce 的策略 Timeout 对照；retry 4.116s / sliceops 4.448s / docs 4.259s |
| README 与专题局部片段抽取编译 | 通过；七组 README、MapReduce 默认并行/批量/生产三组及 Pipeline ETL 一组，共 11 组；补齐导入、业务桩和未使用值处理，只编译不执行 |

日志位于 `.gotmp/audit-race-full-short.log`、`.gotmp/audit-internal.log`、`.gotmp/audit-nonshort-internal.log`、`.gotmp/audit-race-final-short.log`、`.gotmp/audit-dedup-fuzz.log`、`.gotmp/audit-apidoc-final.log`、`.gotmp/audit-root-nonshort-verbose.log`；这些文件是本地验证产物，不纳入版本控制。最初 180s 的根包测试因大规模定时任务超时，记录为未完成，不算通过。

16:08 启动的一次根包非 short 运行，在约 64 分钟后接受 Delve 附加诊断时退出（Windows 状态 0xc0000354，日志 `.gotmp/audit-root-nonshort.log` 与 `.gotmp/audit-root-stacks.log`）。诊断未能读取 goroutine 数组；该次属于诊断干扰下的未完成运行，不计为通过，也没有据此认定库的业务断言失败。17:14 使用详细日志重新运行，不再附加调试器。

17:14 启动的根包长测二进制早于 TokenBucket Rate 修复；其结果须与修复后 Retry 包完整回归及后续交叉验证结合记录，不能声称该长测已经执行了后来的源代码。

后续非 short race 的失败日志为 `.gotmp/audit-internal-final-after-retry.log`、`.gotmp/audit-serial-recheck.log` 和 `.gotmp/audit-ratelimit-race-limited.log`。ThreadSanitizer 明确报告无法分配内存/映射内存（Windows 1455），不是数据竞态报告。Group 示例的失败来自本轮新增示例依赖普通 Go 的完成顺序，改为不同 GoAt 索引后通过。后续 short race 重跑日志为 `.gotmp/audit-ratelimit-docs-short-final.log`。

## 文档处理与剩余核验

修正各专题的默认值、超时/取消、共享输入、结果保留、生命周期和返回值说明；删除验证报告中无可追溯证据的吞吐、跨平台及绝对保证；纠正分片存储无锁的说法。新增 `docs/api/` 逐声明参考和源码生成器，含参数、调用片段、生产建议及注意事项；补入函数变量别名和接口方法，保留源码详细说明供核对。

目前生成参考以静态签名和源码注释为基础，覆盖 1415 个声明条目（含 27 个根包函数变量别名）；调用片段依赖已初始化的参数。1388 个实现声明的调用形式、泛型约束及变长参数已通过编译，27 个别名从其目标提取签名。已按实现补齐原来缺失的旧批处理、多池汇总、构造器等方法摘要，区分每批与每元素回调、输入顺序与完成顺序、取出数量与存储上限，并修正 WithMaxPending(0)、Clip、Grow、全局日志和取消边界说明。

18:44 的生成文档校验通过，1388 个声明调用片段及 3 个完整 Markdown main 编译通过。继续修正了 Flow 的 BufSize 调用位置、Task 的两类取消、Map 无序示例、ChunkN 实际份数、RingBuffer 满缓冲返回值、流水线阶段回调 panic 与并发边界，以及 Task 的默认有界并发说明；删除剩余无测量依据的吞吐数字和缓冲增大保证。

后续交叉核对补充：Map.ForEachBatch 的统计单位为元素，Slice.ForEachBatch 为批次，新增带输出断言的第十个示例。修正四处失效目录锚点，完善等待清理回调、纯数据谓词并发与 panic、浅拷贝和 Retry.Run 无 context 回调的说明。分片越界路由须区分 Pool Hash（第 0 片）和轮转；WithShardedPool 的非正 worker 参数实际使用 IO()，未按疑点误改实现。

继续修正生成参考引用的旧源码注释：Pool/MultiPool 的 DefaultMaxResults 使用创建时全局值，分片 DefaultWorker 使用 IO()，分片 DefaultTimeout 保留各池创建时的全局超时；Group 构建器的 AutoScale(nil) 不启用扩缩容，Streaming(0) 不启用流。移除按固定 QPS/元素数直接断言最佳分片数的表格，补充 Bounded 只能限制同时在途数、不能替代速率限制；修正 MultiGroup 示例缺少等待和错误检查，以及 FailFast 被写成强制立即停止的说明。

后续对照进一步区分独立 ForEachChunked 的按批统计、批内共享期限及双层并发，与旧 Slice 同名方法的逐元素统计；独立 MapChunked 的非正块大小不执行元素，而 Slice 策略路径退回普通逐元素执行。按实现说明非正超时保留全局默认值的构建器路径、清除期限的实例路径，以及 Pool 单任务超时覆盖的继承路径。新增四个分块可执行示例和一个 Group/Pool 超时对照示例；直接 Retry.WithTimeout 的非正值使 context 立即到期但仍调用 fn，并原样返回 fn 的结果，也已由示例核对。

纠正生成参数中 MergeFailFastCancel 的 nil 说明：它合并外部通知但不独立启用 FailFast；已有非 nil 取消链时合并 nil 会使后续取消调用 panic。调用方应在提交前传入非 nil 且并发安全的 CancelFunc。Pool 专题的 ResizeAndWaitTimeout 也已改为等待当前任务批次，明确不取消任务、不保证旧 worker 已全部退出。以上仅改说明与示例，本阶段未新增运行时修复。

无池 Map 的 FailFast 不主动取消已开始回调的 context，且普通 Map/ForEach/批量路径不因输入 context 已取消而自动跳过回调；原先共用的“失败取消”摘要已按执行器拆分，补入独立可执行例证。Policy.FF 的源码说明也已注明执行器差异；保留现有兼容行为。

README 局部片段继续修正不存在的 AsyncResult.Done、WaitContext 少接返回值、Cancel 被误作取消执行、跨类型 MapBatch 的入口及 GoResultAct 的返回类型；Mu 使用零值声明，无 Mu[T]() 函数。AsyncErr/TaskErr 经源码别名和赋值编译核实为正确名称。WaitCh 每次另起通知 goroutine，通道尚无通知不能断言业务仍执行，发送后关闭须检查读取 ok。同步 Map 基础容器的 ForEach/FilterKeys/FilterValues 说明已细分回调参数和原地修改边界。

Retry 最后一次若被 TokenBucket/SlidingWindow 拒绝，返回限流耗尽错误，不包装之前的业务错误；有值版本最终失败返回零值，已用独立输出示例验证。修正三次重试示例的退避序列为 100ms、200ms、400ms。以上阶段只增加说明和例证，已有运行逻辑改动仍限于三项缺陷；当前生成检查、1388 个声明调用和 3 个完整 main 编译、go vet 再次通过。独立复审工具仍在参数解析阶段失败，尚无复审结论。

继续修正 MapReduce 的跨类型生产示例：string→Record 及每批返回 []int64 均使用 SliceWith[R]；批量错误显式检查，删除分片降低锁竞争 16 倍及按固定输入规模选缓冲的结论。构建器可顺序复用，但不可并发修改，PoolAuto 转移的池在终端调用后关闭。Map.Reduce 的遍历顺序不确定，且会应用单次回调的 Timeout；Slice.Reduce 不读取策略 Timeout，新增可执行输出示例验证这一差异。Pipeline ETL 使用统一 ETLItem 传递阶段数据，提取 ID 并检查顶层及逐项错误，SkipOnError 防止错误项继续写入。Pool 环形缓冲补充 MaxResults(0) 镜像全部结果时可能与 Wait 重复取得同一任务的边界。

上述局部示例共 11 组抽取编译通过；当前文档生成、1388 个声明及 3 个完整程序编译、204 个锚点、go vet、AST 运行逻辑差异和 git diff --check 再次通过。21 个可执行示例通过 race 随机顺序五轮。这里的局部片段编译只补业务桩，不执行数据库或网络副作用。

继续添加独立状态模型及执行路径交叉检查：

| 模型 | 30 秒 fuzz 结果 | 范围及边界 |
|---|---|---|
| Map 容器 | 851,888 次，通过 | 固定 16 槽预言，最多 1024 次 Set/Delete/GetAndDelete/GetOrSet/Clear/过滤/映射/合并/集合运算；逐步核对值、存在性、键值集合和长度。没有并发 map 访问。 |
| Pipeline 有限输入三引擎 | 2,024,656 次，通过 | Execute / ExecuteStream / NewPipeline.Execute 与顺序模型交叉核对；0~4 阶段、0~8 元素、1~4 并发，业务错误、panic、错误跳过及覆盖。context 未取消，不比较 Occupied 差异。 |
| 常驻 Flow 生命周期 | 1,703,815 次，通过，33.635s | 1~4 阶段、0~16 元素、1~3 worker/阶段；Results 消费与 Close 排空竞争，合并结果核对数量/唯一性/值/错误及最终阶段错误计数，重复 Close 和关闭后提交。context 未取消，不代表已穷举取消调度。 |

Map/Pipeline 新模型加入后的全部内部包及 docs short audit race 随机一轮通过：core 3.857s、group 48.421s、maps 5.380s、pipeline 22.803s、pool 22.977s、ratelimit 6.460s、retry 5.205s、shard 14.171s、sliceops 10.575s、task 6.558s、docs 8.493s。Flow 模型加入后 pipeline/retry/docs short audit race 随机两轮通过（37.780s/7.476s/8.788s）。更早一次 maps/docs 随机两轮通过（7.481s/10.867s）。

修正 BindRetryToWorker 的参数、注意事项和生产建议：它只对 ErrSubmitTimeout（含包装错误）重试 backend.Submit，提交成功即返回，不等待业务完成；后端必须保证超时没有接受任务。新增独立队列后端输出示例验证这一协议。Retry Adaptive 每次 Execute 独立且顺序尝试，至少 10 次记录才调整；删除限流场景固定推荐 10+ 次重试。

Pipeline 进一步修正显式 Serial 与默认阶段内并发的等价性错误、流模式忽略 MaxResults 及无依据的固定性能判断。Flow 首次 Close 与 Results 消费者竞争排空，完整生产示例增加 producerDone 并等待残留结果处理；处理函数须支持并发。Flow 在阶段 panic 时保留该阶段输入，有限输入的 Execute/ExecuteStream 使用零值，新增输出示例验证这一既有差异。通用生产指南的 FailFast 说明补充无池 Map 只停止后续调度而不取消回调 context 的边界。

当前共 23 个可执行示例通过 race 随机五轮（retry 2.253s / sliceops 3.534s / docs 2.631s）；README/MapReduce/Pipeline/Retry 共 13 组局部片段抽取编译通过，仅补业务桩、导入和未使用变量处理，不执行副作用。最新生成检查、1388 个声明调用和 3 个完整程序编译、go vet、204 个锚点与 git diff --check 通过。去注释 AST 对照仍确认运行逻辑修改限于三项缺陷。独立复审工具仍在参数解析阶段失败，没有复审结论。

编译不能验证全部参数描述与业务语义；仍在逐项核对说明与源码，不能仅凭条目数量宣称文档绝对无误。根包非 short 的百万级全套已通过；本轮未执行 Linux，也没有取得独立复审结果，RateLimit 百万级 race 仍受环境内存分配失败限制。目标保持进行中。

## 后续 Group/Pool/Task 文档与关闭竞争核查

修正 Group 错误示例对不存在的 Result.Stack 字段的访问，改用 PanicError 的格式化输出；Pool 四组示例显式使用当前输入并检查提交错误，“注入外部池”通过 PoolMulti().Pool(base) 实际完成注入。Group 生产示例去除未经测量的任务数量阈值，显式等待和返回业务错误；流式示例补充结束协调，区分 StreamResults 的丢失边界与完整 Wait 结果。Group.Shard 只复制明确列出的配置，不复制外部 Pool、AutoScale 或已有结果；TotalCount 包含进入预检查的拒绝尝试，Flow.ErrCount 是阶段错误次数，均同步修正源码注释与生成参考。

Task 示例原来一次保留一千万个句柄，现改为共享 Bounded 执行器、每批保留至多 256 个句柄，并在整批等待后返回错误；任务完成后结束取消监听或停止定时器。Shard 示例检查 Run 返回值。通用生产 Pool 示例等待已接受任务，通过实例 WithResultCallback 记录上限外错误，再处理保留结果；第一次草稿误用 Builder.ResultCallback，抽取编译失败后改为实际存在的实例方法，再次通过。未因此修改运行逻辑。

验证范围增加如下：

| 范围 | 结果 |
|---|---|
| 24 个可执行 Examples，race 随机五轮 | 通过；docs 2.964s、retry 2.933s、sliceops 2.466s；新增 Group/GroupVoid 的 Run 只传播作用域回调错误的对照 |
| 24 组局部 Markdown 抽取编译 | 通过；7 README、3 MapReduce、2 Pipeline、1 Retry、4 Pool、3 Group、2 Task、1 Shard、1 通用生产 Pool。补导入/业务桩/未使用值处理，不执行业务；可复现脚本 docs/cmd/check_markdown_fragments.py |
| Flow 并发 Submit/Close fuzz，30s、4 workers，管道 context 存活 | 普通 3,441,395 次/32.372s，race 254,931 次/34.847s，均通过；四生产者与关闭竞争，混合已取消的提交 context |
| 同模型扩展外部管道取消，race fuzz 30s、4 workers | 通过，287,492 次/35.516s；管道存活时成功接受项精确一次，取消时允许缺失但禁止重复/拒绝项结果，核查最终计数与排空 |

当前 AST 对照仍确认运行逻辑变更只涉及最初三项缺陷。独立复审派发仍在工具参数解析阶段失败，没有取得独立复审结论。逐方法语义核查、Linux 与 RateLimit 百万级 race 的缺口仍保留，不将本记录写成全库无 bug 证明。

## 后续切片变更模型及 Core/MultiPool 文档核查

`FuzzSliceMutationState` 通过独立的逻辑区间重建模型，交叉检查 SliceBuilder、Serial、Parallel、旧 Slice 的 14 类混合操作，包括 Append/Prepend/Insert/DeleteRange/合法 Replace/Take/Drop/SliceRange/Reverse/Repeat/Clip/Grow/Filter/Values 拷贝，并核对 Len/IsEmpty/First/Last。每输入至多 256 步，Repeat 限制模型大小；没有覆盖达到并行 Filter 阈值的大切片，也没有以共享切片并发修改作为受支持用法。

MultiPool Key 和批量提交示例改为显式传播提交及保留结果错误；批量先等待已接受任务再返回。分片说明区分原池第 0 片、复制的配置、独立空环形缓冲/流和不复制的任务/结果/计数。IOMulti 补充非正值恢复 2、依据 NumCPU；TraceID 说明随机路径及实现保留的回退分支。Core 参考继续补充全局设置的具体读取范围、内部等待辅助函数、环形缓冲拒绝策略和日志后端责任。

逐项对照时纠正 PanicError 说明草稿：实际 Unwrap 始终返回 nil，即使 Value 为 error。修正后的输出示例断言 errors.As 可识别 PanicError、errors.Is 不穿透到原值，以及 Value/Stack 的保存行为；没有修改运行逻辑。

| 范围 | 结果 |
|---|---|
| 切片混合变更模型 fuzz，30s、4 workers | 通过，671,943 次，34.239s；四种入口/独立模型，范围如上 |
| 上述模型 seeds 的 race 随机三轮 | 通过，4.674s；不是长时间 race fuzz |
| Flow 新 seeds 纳入 pipeline/group/docs short audit race 随机两轮 | 通过，39.514s / 93.571s / 9.602s |
| 30 组 Markdown 局部片段抽取编译 | 通过；7 README、3 MapReduce、2 Pipeline、1 Retry、10 Pool、3 Group、2 Task、1 Shard、1 通用生产 Pool。新增桩函数第一次参数不匹配，修正桩后通过；未为通过编译而改写原片段 |
| 25 个可执行 Examples，race 随机五轮 | 通过；docs 9.271s、retry 2.483s、sliceops 5.933s；根包 18 个、Slice 4 个、Retry 3 个 |
| 生成参考 -check -compile、go vet -tags audit ./... | 通过；1388 声明调用与 3 个完整程序，只编译不执行业务 |
| 本地锚点与 git diff --check | 通过；204 锚点、0 未匹配；diff 只有行尾格式提示，没有空白错误 |

本轮仍未增加三项已修复缺陷之外的运行逻辑修改。方法参考的机械覆盖不能替代逐项业务语义核查；保留全库审查未完成状态及既有环境限制。


## 后续限流/大切片模型与文档原文编译复核

新增限流许可独立模型，检查 Adaptive 的持有许可数与底层 Size/Available、成对释放、成功/失败记录；同时交叉检查零补充 TokenBucket 的半单位消耗与一小时无过期 SlidingWindow 额度。没有覆盖定时补充/过期或并发许可模型。新增大切片谓词策略模型，覆盖 0..2048 元素、1..17 workers、100/1000 阈值附近，交叉核对 Builder/Serial/Parallel 的 Count/Contains/Any/All、稳定 Filter 和独立输入的 DeleteFunc；使用纯谓词，不测试不受支持的共享输入并发变更。

| 范围 | 结果 |
|---|---|
| 限流许可模型 seeds 的 race 随机三轮 | 通过，3.965s |
| 限流许可模型 fuzz，30s、4 workers | 通过，5,077,204 次，33.204s |
| 限流新模型纳入 ratelimit/docs short audit race 随机两轮 | 通过，7.830s / 9.971s |
| 大切片谓词策略最终模型 seeds 的 race 随机三轮 | 通过，5.075s |
| 大切片谓词策略最终模型 race fuzz，30s、4 workers | 通过，189,533 次，44.829s；此前变体只在 Filter 后删除余项，最终版另用全新输入检查 DeleteFunc |
| 所有内部包/docs short audit race，逐包随机一轮 | 通过；core 3.352s、group 48.922s、maps 3.447s、pipeline 21.772s、pool 21.391s、ratelimit 5.440s、retry 5.810s、shard 12.099s、sliceops 10.659s、task 6.057s、docs 5.810s |
| 27 个可执行 Examples，race 随机五轮 | 通过；docs 5.772s、retry 2.123s、sliceops 4.226s；增加缓存结果优先于已取消等待及 Pipeline 外部池所有权对照 |
| 55 组 Markdown 原文局部片段编译 | 通过；在 30 组上增加 19 组替代方案及 6 组生产资源/提交/生命周期片段，脚本 docs/cmd/check_markdown_fragments.py。补导入、业务桩、未使用值处理，不执行业务 |

原文抽取进一步发现多个替代方案在同一代码块重复声明变量，以及 Pipeline 两个示例调用不存在的 PoolBuilder.Build。现保留共同作用域修正声明，Pool 用 Run 获得实例并管理生命周期；源码已有测试及新增输出示例验证 Pool(p) 执行后可继续提交、PoolAuto(p) 执行后已关闭。通用生产指南补齐提交错误、先等待已接受任务再返回错误的流程，Bounded 示例分批保留句柄；Pipeline 通道示例持续消费并处理逐项错误。编译桩曾误把 ShardedPool 的字符串 key 写成 int，修正桩后通过，原文未因此改写。

逐方法说明继续补充 Adaptive 调整阈值、Resize 丢失 Burst 的边界、Task 缓存结果等待优先级、Busy 包括已取得但尚未开始的槽位、Result.Ok 不检查 Occupied，以及 SliceResult 的内部引用和过滤规则。GroupBuilder.DefaultPool 注释纠正为清除外部池并恢复自有 worker；Group 的构建/关闭/复位/计数/错误读取及切片共享视图/查询/谓词/分块说明继续按源码补充。

本阶段生成参考及 1388 声明调用/3 完整程序编译、go vet、204 个锚点和 git diff --check 均通过（后续若再修改生成器需重新生成检查）。AST 去注释对照仍确认运行逻辑只涉及原三项缺陷。独立复审调用仍在工具参数解析阶段失败，没有独立复审结论；Linux、RateLimit 百万级 race 环境限制与逐方法语义核查未完成状态仍保留。

## 后续执行策略、路由及复位模型与方法语义核查

切片排序/搜索模型以独立直方图排序及线性查找为预言，检查四种入口和机器整数极值。Map 执行模型以固定键槽位核对串行、并行、分片、默认池的 Map/ForEach/Stream，随后增加 MapBatch 的首键结果/分块访问及 ForEachBatch 元素失败计数；不依赖 map 遍历顺序。分片批量模型独立计算轮询及 Hash 片号，检查 Pool 与 Group 的每项提交状态、精确一次执行和业务错误。Group 生命周期模型对照自有 worker 与外部 Pool，使用连续 GoAt 索引核查成功/错误/panic、重复 Wait 和 Reset 后第二批，检查回调次数与计数。

| 范围 | 结果 |
|---|---|
| 排序/搜索模型 seeds，race 随机三轮 | 通过，2.298s |
| 排序/搜索模型普通 fuzz，30s、4 workers | 通过，659,268 次/33.212s |
| Map 最初执行模型 seeds race 三轮及普通 fuzz 30s | 通过，2.788s；197,472 次/31.945s；未含批量 |
| Map 扩展批量后的 seeds race 三轮 | 通过，2.084s |
| Map 扩展模型普通/race fuzz，各 30s、4 workers | 通过，167,561 次/31.240s；41,785 次/33.322s |
| Map/docs short audit race 随机两轮 | 通过，3.479s / 8.218s |
| 分片批量路由 seeds race 随机三轮 | 通过，2.441s |
| 分片批量路由 race fuzz，30s、4 workers | 通过，153,176 次/32.904s |
| 新分片模型纳入 shard/group/docs short audit race | 通过，13.062s / 46.464s / 4.986s，逐包随机一轮；此时尚未增加 Group 生命周期模型 |
| Group 索引/复位模型 seeds race 随机三轮 | 通过，3.799s |
| Group 索引/复位模型 race fuzz，30s、4 workers | 通过，83,384 次/42.189s（含构建及清理耗时） |
| 30 个 Examples race 单次命令（传 -count=5，每例实际一次） | 通过，docs 3.143s、retry 2.506s、sliceops 2.197s；新增构建器 context、显式默认值及 Pipeline 输出池归属 |
| 57 组 Markdown 原文局部片段编译 | 通过；新增限流 Size 和监控退出流程，补导入/业务桩/未使用值，不执行业务 |
| 最新方法参考生成、1388 声明调用及 3 个完整程序编译、go vet | 通过；编译与覆盖数量不代表全部业务语义已验证 |

限流文档纠正 Size 为配置速率而非突发容量，监控示例明确停止 ticker、取消并等待监控 goroutine。Pipeline 文档限定普通 Execute/Run 才按 PoolAuto 接管外部池；元信息/通道输出使用原生路径并由调用方管理注入池。方法参考继续按源码补充 Retry/限流/Pool/Group/Task 构建器、Map 执行与容器、Pipeline 模式、Shard 路由/结果上限/Reset/流的语义。分片 DefaultMaxResults 的 -1 经构造器转换为无限，原说明正确，未改成继承全局值；这项疑点不计为缺陷。GoAt 重复索引会移动旧结果而 Pool 可能覆盖，已在分片注意事项中区分。

上述模型不覆盖全部大输入、超时/取消、溢出或并发生命周期调度；Group Reset 仅在无活跃任务且等待完成后调用。没有新增三项以外的运行逻辑修改。逐方法语义审查仍未完成，独立复审未取得结果；Linux 和 RateLimit 百万级 race 缺口继续保留，目标未标为完成。

## 后续 Pool 关闭竞态修复与回归

前一节“三项运行逻辑修改”是该阶段快照。本阶段并发关闭模型新增确认第四项缺陷：Close 在等待 worker 后关闭 taskCh，但提交在 addInFlight 减一后仍可能调用 trySend/blockSend，故已有关闭检查及 recover 不能消除 closechan/chansend 竞争。两次 race fuzz 报告中一次保存完整堆栈，见 `.gotmp/pool_close_tsan.*`。单独重放第一个输入 100 次未复现，随机调度 fuzz 再次复现，不把单个输入通过当作排除证据。

修复在 blockSend/trySend 的检查及发送期间持有 taskSendMu 读锁，普通 Close 和 CloseAndWaitTimeout 实际关闭 taskCh 时持写锁。done 在取写锁前关闭，持读锁的阻塞发送可被唤醒；锁释放不包含结果通知，不在用户回调期间持锁。Resize 的退出信号已由 resizeMu 与关闭标记隔离，Reset 在外部协调且任务已等待后重建队列；未更改公开签名、既有拒绝规则或回调覆盖。

| 范围 | 结果 |
|---|---|
| Group 新生命周期模型加入后的 group/shard/docs short audit race | 通过，47.137s / 12.178s / 5.120s，逐包随机一轮 |
| Slice Runner 六策略 seeds race 三轮 | 通过，2.691s；最多 32 项，Seq/Par/Shard/Chunk/TO/DefaultPool 的 Map/Each/Stream |
| Slice Runner 六策略 race fuzz 30s、4 workers | 通过，26,849 次/33.602s；成功/业务错/panic，不含 FailFast/取消/溢出，Stream 不注入 panic |
| sliceops/docs short audit race 随机两轮 | 通过，20.714s / 8.554s |
| Pool 结果保留生命周期 seeds race 随机三轮 | 通过，2.266s；正数/无限主结果、足量环形缓冲、取消前拒绝及成功/错误/panic、Reset/ResetWait 后第二批 |
| Pool 结果保留生命周期 race fuzz 30s、4 workers | 通过，165,680 次/33.421s；最多 48 项，无并发配置及 FailFast |
| Pool/docs short audit race 随机两轮（关闭修复前） | 通过，39.288s / 8.338s；不覆盖后来确认的关闭竞态 |
| Pool 拒绝 completion 输出示例 race 随机五轮 | 通过，2.552s；共 31 个 Examples，此命令仅执行新增的一个 |
| 并发提交/取消/双 Close 原模型 race fuzz | 修复前失败，完整堆栈确认 trySend/Close 的通道竞争；模型先遗漏 ErrPoolWaiting，修正合法返回集合后 race 仍复现 |
| 定向关闭/阻塞提交/重复关闭/限时等待回归 race 十轮 | 修复后通过，4.224s |
| 并发关闭原模型 race fuzz 60s、4 workers | 修复后通过，449,068 次/63.145s；该二进制尚未加入限时关闭分支 |
| 加入 CloseAndWaitTimeout 后的最终模型 race fuzz 30s、4 workers | 通过，179,173 次/33.544s；每输入最多 24 项，三种背压策略/父取消/双关闭，不含并发 Reset |

方法参考补充 Pool Config 值语义、上下文传递、Resize/AutoScale、分片提交与流；WithResultCallback 不覆盖直接预检查/背压拒绝，完整终态应使用 SubmitWithCompletion，文档和输出示例均已区分。README/MapReduce 的 DefaultPool 改为终端时创建，切换引用不会关闭旧外部池；共享数据操作名称改为 Chunked。新增模型之后尚需记录最终生成、静态、上层及根包回归。

## 后续缓存/重试模型与切片文档复核（2026-11-06 收尾记录）

以下回归均使用包含四项运行逻辑修复的源码。Task 模型用同一任务结果对照并发 Wait/Values/WaitCh/WaitContext/WaitTimeout/Cancel，覆盖普通与 Bounded 启动、成功/业务错/panic；等待取消和超时不替换真实缓存，业务最多执行一次，Bounded 槽位释放。Retry 模型以有界确定性结果序列计算期望尝试次数，覆盖七个指数/线性/Config/Result/链式入口及负重试数；退避设为零，不覆盖计时、取消或限流调度。

| 范围 | 实际结果 |
|---|---|
| Pool 修复后 pool/group/shard/sliceops/maps/pipeline/docs 的 short audit race，逐包随机一轮 | 通过，22.168s / 46.668s / 13.139s / 9.131s / 4.183s / 19.532s / 5.134s |
| Pool 非 short race，随机一轮 | 通过，22.541s |
| 根包 short audit race，随机一轮 | 通过，640.074s；日志 .gotmp/audit-root-pool-close-race.log，包含四项修复 |
| Task 缓存模型 seeds race 随机十轮 | 通过，2.076s |
| Task 缓存模型 race fuzz 30s、4 workers | 通过，326,227 次 / 33.597s；最多 16 次等待操作，未覆盖 Task.Result 字段接口及业务定时到期 |
| Retry 尝试序列模型 seeds race 随机三轮 | 通过，2.341s |
| Retry 尝试序列模型 race fuzz 30s、4 workers | 通过，312,593 次 / 33.277s；最多 8 个业务结果，重试配置 -2..7 |
| 新模型纳入 task/retry/group/docs short audit race，逐包随机一轮 | 通过，4.232s / 3.991s / 46.932s / 5.202s |
| 32 个 Examples race 单次命令（传 -count=5，每例实际一次） | 通过，docs 2.121s / retry 2.602s / sliceops 3.861s；新增 NoResult 稀疏槽位填补与聚合 |
| 增加共享容量对照后 33 个 Examples race 单次命令（传 -count=5，每例实际一次） | 通过，docs 2.022s / retry 2.655s / sliceops 2.440s；验证 Grow 写入共享数组尾部与 Chunk 子切片 append 覆盖 |
| 上述 33 个示例版本的方法参考检查、1388 声明调用/3 完整程序编译与 go vet | 通过；后续继续编辑语义生成器时须重新生成检查 |

逐方法参考进一步区分旧 Slice 与 Runner 的执行路径：旧 Slice 的 Each/EachFull/EachStream 忽略 Policy 池配置；有值 Map/Stream 使用池优先路径。数据操作说明补充 Items 切片头副本、Values 浅拷贝、空集合、搜索前置排序、Compact 与 Dedup、删除尾部引用、Grow 不同负值规则、Repeat 内存预算，以及 Chunk 子切片未限制容量的影响。MapReduce 专题新增共享容量的说明，未因此改变旧 API 行为。

独立复审再次因工具参数解析错误未启动；没有把自行核对或编译称为独立复审。Linux、RateLimit 百万级 race 的 Windows 内存分配失败限制仍保留；逐方法业务语义审查继续进行，不把有限测试通过等同于不存在任何 bug。

## 方法说明与交叉复核（2026-11-06）

本阶段继续按实现核对 Group、Pool、Map、Pipeline、RateLimit、Shard 及根包包装入口，修改参数、构造默认值、提交/业务错误、取消、缓冲、分片汇总与资源所有权说明。修复 Group 配置表被段落打断的问题。旧 pipeline.Pipeline.ExecutePipe 忽略 Shard 并委托自由函数；该函数先同步执行前面阶段，最后阶段全部等待后才输出，源码注释、参考和专题已纠正。WorkerPoolBackend 的回调签名为 func(context.Context) error，不能写成无返回值回调；Submit 返回错误与回调业务错误由后端协议区分。

新增 Example_poolRejectedTransform 和 Example_groupRejectedCallback：合法索引在 Pool 预检查拒绝时仍转换，转换后的 Err 不会改写执行/拒绝计数；非法索引不转换；Group 在首次等待之后最早的拒绝分支不通知结果回调。新增示例最初引用根包未导出的 ErrInvalidIndex 导致编译失败，改为核对返回非 nil 错误后通过，没有为示例新增或修改公共 API。

| 检查 | 本阶段实际结果 |
|---|---|
| 35 个 Examples，race 随机五轮 | 通过：docs 3.402s / retry 4.040s / sliceops 4.694s；包含两个新增拒绝路径示例 |
| 全部内部包及 docs short audit race，逐包随机一轮 | 通过：core 4.642s / group 49.069s / maps 5.136s / pipeline 21.726s / pool 23.105s / ratelimit 6.708s / retry 6.206s / shard 15.505s / sliceops 11.366s / task 6.506s / docs 7.796s |
| Dedup / ForEachPool / Retry Rate 定向 race，随机十轮 | 通过：sliceops 4.680s / pool 4.553s / retry 4.999s |
| Pool 并发提交/取消/双关闭/限时关闭模型 race fuzz 30s、4 workers | 通过：214,726 次，35.652s；最多 24 项，三种背压策略，不含并发配置或 Reset |
| 最新方法参考一致性及调用编译 | 通过：1388 个实现声明调用片段、3 个完整 Markdown 程序；编译不执行业务 |
| 方法参考结构检查 | 1415 个声明条目均有参数、使用片段、生产建议和注意事项；无三类通用注意事项回退文本。此检查不证明每个业务分支的语义均已动态验证 |
| go vet -tags audit ./... | 通过，含新增文档辅助代码 |
| GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./... | 通过；首次在沙箱内报告模块统计缓存写入被拒，获授权重跑后干净退出。只验证非 CGO 编译，未运行 Linux 测试或 race |
| 57 组局部 Markdown 片段编译 | 通过；本阶段开始时沙箱中 Go 删除临时 test.exe 被拒，获授权重跑后通过。这些片段只补业务桩并编译，不执行副作用 |

AST 对照再次确认运行逻辑仍仅涉及已记录的四项修复；Pipeline 的新增改动仅为注释。开始时已有的修改未回滚，未提交或合并。独立复审工具仍因 message 参数接口解析失败未启动；WSL 发行版列表获授权查询为空，因此未取得 Linux 运行环境。RateLimit 百万级 race 的 Windows 1455 内存限制、Linux 运行验证和独立复审仍是缺口。当前未出现尚未修复的已确认新缺陷，但不能据此宣称全库与全部文档绝对无错误；目标继续有效。

## Group 并发收尾与文档语义复核（2026-11-06）

本阶段没有新增运行逻辑修改。新增 Group 并发提交/取消/等待/关闭模型，最多 24 项，覆盖自有 worker 与外部 Pool、Go/GoWithTimeout、稀疏 GoAt/GoAtWithTimeout、成功/业务错误/panic、双等待及取消等待后 Close。预期不依赖调度顺序：所有调用退出后，每次尝试有一个已占用结果、回调至多执行一次、执行项有唯一对应结果、成功/失败/总数一致且 Active/Busy 归零。索引结果允许空槽，等待快照不用于推断后来拒绝的提交；配置和 Reset 不并发，回调及时返回，不覆盖真实超时或不协作回调。

文档语义进一步纠正：Retry/New 构造及 Execute 不自动补 trace_id，显式 Context(ctx) 才补；RateLimiter 默认 Block 在有令牌时先获取，已取消 context 仍可成功，Reject/BlockForce 忽略取消；Adaptive 使用相同的先取可用许可规则；ShardedRateLimiter.Size 是配置速率之和。README/MapReduce 的 SliceResult 表格改为保留槽位和已记录错误语义，Values/First 不检查 Occupied，不能推断全部输入已经执行；Results/FailValues 暴露内部切片。源码注释及生成参考同步修改。新增 Retry trace 输出示例，并扩展限流取消示例。

| 检查 | 实际结果与边界 |
|---|---|
| 36 个 Examples，race 随机五轮 | 通过：docs 4.145s / retry 4.349s / sliceops 5.143s；docs 29、retry 3、sliceops 4 |
| 加入 SliceResult 空槽对照后 37 个 Examples，race 随机五轮 | 通过：docs 4.408s / retry 3.504s / sliceops 4.585s；docs 30、retry 3、sliceops 4。验证共享 Results 的外部修改、Values/First 的 Err 过滤及空集合 Ok |
| Group 首版并发模型种子 race 随机 20 轮 | 通过，4.537s；首版无索引提交 |
| Group 首版 race fuzz，30s、4 workers | 通过，260,026 次/35.474s |
| 扩展稀疏索引后的最终模型种子 race 随机 20 轮 | 通过，3.415s |
| 最终 Group race fuzz，30s、4 workers | 通过，263,645 次/34.663s |
| group/shard/docs short audit race 随机一轮 | 首版模型纳入后通过，49.778s / 15.027s / 7.489s；最终索引模型纳入后再次通过，49.251s / 15.179s / 7.538s |
| 方法参考一致性及调用编译 | 通过，1388 声明调用片段、3 完整 Markdown 程序；不执行业务 |
| 方法参考结构 | 1415 个条目均有参数、示例、生产建议和注意事项；结构完整不等于业务语义穷举正确 |
| go vet -tags audit ./... | 通过，最终索引模型及文档代码纳入后重跑 |
| 本地 Markdown 锚点 | 更新报告入口链接后 207 个，0 未匹配 |
| 局部 Markdown 原文编译 | 63 组通过；新增 Pool Submit/TrySubmit/SubmitAt 三组及 README SlidingWindow/TokenBucket/Adaptive 三组，仅补桩/导入，不执行业务 |
| README/专题 Go 代码块语法 | 368 组全部可解析为文件、声明或函数体片段（包含两组波浪线围栏）；短变量重复声明扫描无报告，不能替代类型编译 |
| 去注释 AST 对照及 git diff --check | 通过；运行逻辑范围仍为已记录的四项修复；限流 Wait/Acquire 摘要及 Release 示例随后同步生成，编译再通过 |

独立复核仍在工具参数解析阶段失败，没有启动审查者。本阶段未发现新增可复现运行缺陷；独立复核、Linux 运行及 RateLimit 百万级 race 的既有缺口仍保留，不据有限模型宣称全库或全部文档绝对无错误。

后续部分类型核对发现 README 调用了不存在的 SlidingWindow.Current()/Adaptive.Current()，表格还列有不存在的 RateLimiter.Rate() 查询；现删除 Current 调用，Rate 查询改为实际 Size/Available，并加入原文编译。Pool 的 Submit 说明改为等待入队而非等待 worker 空闲，SubmitAt 示例修正未使用循环变量并处理提交错误；结果索引对应受保留上限限制。Retry Execute 最终限流拒绝及 SliceResult.Unwrap 空槽规则同步源码注释。Group Build 的关闭建议改为实际生命周期：完整 Wait 或 Close 都收尾，defer Close 用于覆盖提前返回；配置中单独 EnableAutoScale 的例子补 defer Close。部分类型核对忽略未定义业务符号，剩余诊断包括签名展示、函数体片段的 return 和明确标注的编译错误反例，不将其统计为完整编译通过。

分片滑动窗口说明进一步纠正：正数窗口内的接受数不超过各片额度之和，不会因窗口边界不同而额外超发；目标值小于片数时，每片至少 1 才会提升实际总额度。只检查所选片，别片仍有余额时可能拒绝。分片 Adaptive 源码使用片段补齐 Acquire 错误处理及 defer Release；ResultValues/Every/Some/PartitionResults 等源码摘要补充不检查 Occupied 的规则。以上仅修改文档/注释，运行逻辑仍限于四项修复。

新增 FuzzShardedWindowRouting：1..8 片、目标额度 -2..65、每输入最多 256 次操作，以独立片额预算核对 Allow/AllowN、非正请求仍推进轮询、超大请求拒绝、部分历史过期和查询汇总。通过测试内重写有序时间戳并调用拒绝的大请求触发清理，不睡眠、不引入生产时钟接口；这不验证真实时钟漂移、恰好过期边界或并发调度。种子 race 随机十轮通过（4.803s），race fuzz 30s、4 workers 通过 384,935 次（35.285s），没有发现新增运行缺陷。

该窗口模型及 37 Examples 纳入后的全部内部包/docs short audit race 随机一轮通过：core 4.735s、group 49.056s、maps 5.093s、pipeline 21.846s、pool 23.175s、ratelimit 6.589s、retry 6.123s、shard 15.521s、sliceops 11.215s、task 6.232s、docs 7.747s。该次运行之后才加入 Task.Result 独立模型，不把它计入此轮；最后少量 Result 帮助函数摘要更正只改注释。63 组原文片段编译、1388 声明调用/3 完整程序编译及 go vet 再次通过。

新增 FuzzTaskResultCache，直接检查此前 AsyncResult 模型未覆盖的 Task.Result 函数字段：普通 GoResult、BoundedGoResult、GoResultAct 三入口，每输入最多 16 个并发读取/取消操作，Task 值副本共享一次业务执行及实际缓存。业务及时返回成功、带值错误或 panic；取消不覆盖不检查 context 的业务返回值。检查重复 Result、取消后再取结果、业务至多一次和 Bounded 槽位释放，不覆盖 Bounded 拒绝分支或真实执行超时。种子 race 随机十轮通过（4.389s）；race fuzz 30s、4 workers 通过 320,214 次（35.323s）。新模型纳入 task/docs short audit race 后通过（5.586s/7.416s）。没有新增运行逻辑修改或已确认未修复缺陷。

最新生成检查及 1388 声明调用/3 完整程序编译、go vet、1415 条方法结构、368 围栏语法、207 本地锚点和 git diff --check 均通过；AST 仍仅四项运行逻辑修复。代码复审技能要求派出独立审查者，但当前工具暴露的参数 schema 为 {}，调用仍报 missing field message，未启动审查者；没有把此次自行核对计为独立复审。

## 数值边界与统计文档复核（2026-11-06）

继续检查了 BoundedRunner 等待许可时的取消、Pool 扩缩容与计数、RateLimiter 补充定点计算、Retry 指数退避溢出及 Pipeline/Map 结果边界。未发现这些新增检查主题中可复现的运行缺陷。修正 Pool.FailCount/TotalCount/Size 与 PoolStats 的源码摘要：失败数含执行错误、取消及已记录拒绝；总数是已记录提交尝试且不等同执行数；Size 是目标 worker 数，并不保证实时存活数。ErrSubmitTimeout 注释改为等待提交/背压许可超期。方法参考已再生成并含具体说明。

新增 FuzzRateLimiterRefillArithmetic，以 math/big 独立预言覆盖累计时间与 rate 相乘溢出、分数令牌余量、倒退/相等时钟、满桶后不累计额度及非正周期路径；种子 race 随机十轮通过，30 秒四 worker race fuzz 502,932 次通过（35.305s）。新增 FuzzBackoffSaturation 以任意精度整数对照指数退避饱和；种子 race 随机十轮通过（2.334s）。现有 BoundedRunner 四入口槽位耗尽后取消测试及 Builder 超时槽位复用测试随机 race 二十轮通过（4.933s）。这些检查分别不模拟真实定时器精度、所有并发 Resize 调度、任意超长输入或非协作回调。

本节新增 Retry/RateLimit 数值模型及源码摘要修正之后，重新执行 `go test -tags audit -short -race -p 1 ./... -count=1 -shuffle=on -timeout=30m`，全包通过：根包 658.299s，docs 5.367s，core/group/maps/pipeline/pool/ratelimit/retry/shard/sliceops/task 也全部通过。随后 `go vet -tags audit ./...`、`go test -tags audit -run '^$' ./...`、API `-check -compile`（1388 声明片段及 3 个完整程序）、63 组 Markdown 原文片段、368 个围栏语法、207 个本地锚点及 `git diff --check` 均通过。该阶段最后一次 Examples 命令只覆盖 docs 的 30 个示例并通过 race 随机五轮；全部 37 个的前次记录见上节。去注释 AST 对照已通过，仍仅涉及四项已记录运行修复；逐方法业务语义、Linux 运行、RateLimit 百万级 race 和独立复审仍是开放项。

继续核对：`FuzzBackoffSaturation` 在 race 下运行 30 秒、4 workers，通过 547,811 次执行（包耗时 35.445s）。该模型用 big.Int 精确乘法检查正初始退避、非正初始值、最大退避边界及超大指数，不执行真实等待或重试任务。补正 ChunkedN/ChunkN 的“最多 n 块”及共享底层数组说明、ShardedPool 的跨片取消说明，并明确 TotalWorkerCount 汇总的是 Size 目标值。此批修改仅为文档、注释及文档生成器，无新增运行逻辑修改。

新增 `FuzzRingBufferConcurrentHistory`：3 个 goroutine 各执行 2 个操作，以单调事件序号记录调用/返回区间，搜索满足真实先后约束的 FIFO 串行历史。覆盖 Push、Pop、Peek、FlushN、Flush、Reset、Len、IsFull、Dropped，容量 1～4、三种溢出策略，并核对最后排空值及丢弃计数。它不预测 mutex 顺序、不读取内部 head/tail；最多搜索 6! 条顺序，不覆盖任意长度历史或所有调度。首版 3 个种子 race 随机 20 轮通过（3.300s），race fuzz 30s、4 workers 通过 561,855 次（35.341s）。随后补入全 Peek 和满桶全 Push 两个定向种子；仅在 `.gotmp` 用 Go overlay 注入“Peek 错读队尾”和“覆盖不计丢弃”两个变体，两个种子分别按预期失败，真实源码未改动。最终 5 种子纳入 core short audit race 通过（3.996s）。

本次文档生成检查/编译再次通过（1388 声明调用及 3 个完整程序）；全部 37 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 3.395s / retry 4.394s / sliceops 4.501s）。core/shard/docs short audit race 一轮通过（5.588s / 17.028s / 9.213s，早于最后两个定向种子），两个种子加入后 core 的最终结果如上。63 组 Markdown 原文片段编译、go vet、1415 条参考的四项结构、368 围栏语法、207 本地锚点、git diff --check 及去注释 AST 范围检查通过；运行修复仍只有原四项。再次尝试独立审查工具仍在参数解析阶段报 missing field message，没有启动审查者或得到复审结论。当前没有新增已确认运行缺陷；逐方法业务语义、Linux 运行及百万级 race 的既有缺口继续保留。

## 文档围栏检查与批量拒绝统计（2026-11-06）

复查 Adaptive 构造路径：超出正 int32 或容量/内存预算的输入不在当前说明约束内；本轮没有据此新增运行修复。继续用独立整数比例模型 `FuzzAdaptiveAdjustmentThresholds` 检查单实例及分片的顺序记录：至少 10 条评估一次，严格高于 20% 缩容、严格低于 10% 扩容，中间区间保持，整数舍入、上下界及记录轮转与当前实现对照。首版 3 种子 race 十轮通过（3.217s），race fuzz 30s、4 workers 通过 75,669 次（35.218s）。模型容量最大 104、分片 1～4、最多 1024 条记录，不覆盖并发 Record 调度或极大容量；在途许可与并发调整另由既有模型/回归核查。随后补 20% 等号种子，最终 4 种子 race 五轮通过（2.596s）。

文档工具有两处已复现漏查：`checkRootReferences` 只切换三反引号状态，会漏报 `~~~go` 内不存在的 API，也会把四反引号块中的三反引号误判为结束；`compilePrograms` 按字符串分隔三反引号 Go 块，会把波浪线围栏内错误完整程序跳过，报告编译 0 个。先观察回归失败，再按围栏字符和长度判断起止，支持缩进至 3 空格及波浪线。新增引用位置、长短围栏、非 Go 块、未闭合块抽取和实际子进程编译回归；最终工具 race 五轮通过（3.209s），完整程序错误 fixture 的编译非零退出由回归明确捕获。这是审查工具修复，不计入库的四项运行修复。检查器仍只核对根包名称，不替代局部片段完整类型/语义检查。

新增公开 `Example_mapBatchRejectedStatistics`：池化 Map 的输入共 3 元素，父 context 已取消时业务回调 0 次，而 Total=3、FailCount=3、OkCount=0，错误为 context.Canceled。专题与逐方法注意事项现区分池化路径所有批次结果槽、无池 FailFast 部分处理及批次元素单位。示例扩至 38 个（docs 31/retry 3/sliceops 4），race 随机五轮通过（4.005s/4.097s/4.230s）；本节实现未改变库运行逻辑。

maps/ratelimit/docs/文档工具 short audit race 一轮通过（5.289s/6.146s/7.937s/3.099s，早于最后补入的等号种子及编译器断言加强；最终定向检查见上文）。API 生成检查及 1388 声明调用/3 完整程序编译、63 组 Markdown 原文片段编译、go vet、全包仅编译、1415 条方法参考四项结构、368 围栏语法、207 锚点和 git diff --check 通过。去注释 AST 对照仍为四项运行修复；根包 full short audit race 最新记录仍是前节 658.299s，此后没有运行库改动。逐方法业务语义审查、Linux 实际运行、Windows 百万级 race 内存限制及独立复审的缺口继续保留，目标未标为完成。

本节报告更新后，本地锚点增至 209 个并全部通过；原文抽取编译再加入 config 中两组 `~~~go` 的 SafeCall/SafeCallVoid 示例，总数由 63 增至 65，全部编译通过。只补导入、函数上下文与 return nil，不更改示例业务代码，不执行副作用。最新生成一致性、go vet、AST 范围及 diff 空白检查再通过。

## 任务等待与缓存路径补查（2026-11-06）

`FuzzTaskConcurrentCache` 从 Go/BoundedGo 两入口扩展至 GoAct/BoundedGoAct，共四入口；后两者通过结果通道填充缓存，与有值路径预先写缓存不同。最多 16 个并发操作覆盖 Wait、Values、WaitCh、已取消 context 等待、零超时、Cancel、Error、Ok 和 IsPanic；核对成功、业务错误和 panic 的实际缓存、重复读取及一次执行，并在最终缓存就绪后检查已取消 context 不覆盖结果。6 个种子 race 随机 20 轮通过（4.280s），30s、4 workers race fuzz 182,929 次通过（35.539s）。随后将 Error 查询放到 Wait 前，最终版本纳入 task short audit race 通过（6.506s）。模型不覆盖任意长度历史、真实执行超时、Bounded 槽位拒绝或所有调度；这些边界由其他回归补充，不能据此证明无缺陷。

仅在 `.gotmp` 用 Go overlay 注入 GoAct 丢弃业务错误、Ok 判断反转两个可编译错误变体；种子分别以预期业务断言失败，日志保存在 `.gotmp/task_cache_mutants/`。真实源码未被这些变体改动，检验目的在于确认模型能够识别错误而非仅重复实现。

Task 专题与源码摘要现明确 WaitContext 优先返回已缓存或可立即读取的结果，进入等待后取消与结果同时就绪无固定顺序；AsyncResult 必须由任务入口初始化并共享指针，零值不可用、不能复制。BoundedRunnerBuilder 只采用同一槽位机制，创建的独立执行器不能直接传入公开 TaskBuilder。支付取消示例改用可停止定时器并释放派生 context；共享 Bounded 示例保留 256 个句柄，等待全部结果后处理首个错误。原文抽取编译加入这两组示例，总计 67 组全部通过；生成 API 的 1388 声明调用及 3 个完整程序也再次编译通过。

最终 task/docs/文档工具 short audit race 随机一轮通过（6.506s / 8.014s / 4.597s），go vet 通过。此轮只改变测试、文档、注释和文档生成器，没有新增库运行修复；全库 short audit race 的最近记录仍为根包 658.299s 的那一轮，其后新增模型按上文定向回归。逐方法业务语义核查、Linux 实际运行、Windows 百万级 race 内存限制及独立复审缺口仍保留，目标未标为完成。

本轮报告补齐后，368 个 README/专题 Go 围栏语法及 210 个本地锚点通过；生成一致性、去注释 AST 范围和 git diff --check 再通过。独立审查仍因工具参数解析 missing field message 未启动，未取得审查结论。

## 流水线阶段历史与监控复核（2026-11-06）

新增 `FuzzPipelineMetadataHistory`，以独立数值/错误类别模型核对阶段历史顺序、阶段名、值、业务错误、业务 panic、观察回调 panic、业务调用次数及观察通知次数。覆盖自由 ExecuteWithMeta、Pipeline 原生 Meta、配置观察者的默认 Builder、Parallel.Worker(2)、Serial、无观察者 Stream、带观察者 Stream 共七条路径；0～4 阶段、0～8 元素、阶段并发 1～4。模型单独区分 Serial 不检查 SkipOnError、其他路径跳过时保留记录、Stream 只保留最终槽位及空阶段恒等值。它不检查 FailFast 的任意并发调度、实时超时、分片或外部 Pool，这些不能由本模型通过而推断已穷举。

首版六路径 4 种子 race 随机十轮通过（3.746s），race fuzz 30s、4 workers 162,708 次通过（35.709s）。加入有观察者 Stream 和 Occupied 断言后，最终七路径种子 race 十轮通过（2.756s），race fuzz 30s、4 workers 127,664 次通过（34.065s）。仅在 `.gotmp/pipeline_meta_mutants/` 用 Go overlay 注入“丢失观察者 panic 错误”和“Stream 错标第一阶段名”两个可编译变体，种子均按预期业务断言失败；真实源码未改变。

修正 Pipeline 专题和生成参考：默认/Serial/Parallel Meta 按阶段保留历史，跳过项也可能有记录，Stream 只标记配置中的末阶段名，连 FailFast 保留的中途失败也使用这个名称，不能定位真实失败阶段。空阶段时前三者返回空记录，Stream 返回原值和空阶段名。新增 `Example_pipelineMetadataHistory` 对照错误被后续成功覆盖、Stream 中间错误不可见、FailFast 中途失败标名及空阶段差异。源码摘要只调整说明。

Flow 监控表原来将 TotalError/TotalSubmit 作为元素错误率、把结果积压增长描述为通道自身 OOM 风险，现按实现改为阶段失败次数比和固定容量背压。新增 `Example_flowStageErrorCounts` 验证一个元素在两阶段失败时 TotalSubmit=1、TotalError=2、TotalResult=1。监控区分阶段累计次数与最终失败元素数，按阶段数及区间增量制定告警。

总计 40 个 Examples（docs 33 / retry 3 / sliceops 4）race 随机五轮通过（2.067s / 2.287s / 2.614s）。最终 pipeline/docs/文档工具 short audit race 随机一轮通过（19.534s / 5.785s / 2.272s）；1388 声明调用和 3 完整 Markdown 程序、67 组原文局部编译、go vet、1415 条四项说明结构、368 围栏语法及去注释 AST 范围检查通过。全库 short audit race 的最近记录仍为根包 658.299s 的阶段；此后无新增库运行修复。逐方法语义核查、Linux 运行、百万级 race 内存限制及独立复审缺口仍保留，目标未标完成。

报告更新后本地锚点增至 211 个，0 个未匹配；最终 API 生成一致性与 git diff --check 再通过。

## 重试限流额度与取消优先级（2026-11-06）

新增 FuzzRetryLimiterAttemptBudget，以独立整数预算和业务结果序列交叉核对单实例/分片 TokenBucket、单实例/分片 SlidingWindow 四条限流重试路径。覆盖 1～16 次尝试、1～5 额度、1～4 片，成功、业务错误、panic、局部拒绝及后续恢复，检查实际回调次数、最终零值、最后错误包装与拒绝覆盖先前业务错误。SlidingWindow 走公开 Execute 并使用一小时窗口；TokenBucket 为使模型不依赖真实时钟，通过 executeCore 注入零补充速率的桶（公开 Retry.Rate(0) 会回退默认 10，不能把此注入当作公开配置语义）。不模拟真实定时补充、退避等待、阻塞许可、全部模式并发或外部取消调度，其他回归分别补充。

首版种子 race 十轮通过（2.223s），将 SlidingWindow 改为终端路径后的四种子 race 十轮通过（2.104s），race fuzz 30s、4 workers 393,163 次通过（33.083s）。Go overlay 的“跳过许可检查”变体被识别；“业务错误由 %w 改成 %v”最初未被固定种子识别，发现固定种子缺少最终业务失败输入后补入最终业务错误及最终 panic 两项种子。最终六种子对两变体均以预期断言失败，真实实现六种子 race 十轮通过（2.094s），最后纳入包级回归。变体仅在 .gotmp/retry_budget_mutants/，未修改真实库实现。这一区分避免把首版 fuzz 结果错误归给后来新增种子版本。

修正 Execute 源码摘要及生成参考：父取消在每次尝试前和退避等待中检查，不统一覆盖已返回业务结果；nil 错误立即成功，最后一次业务失败仍包装业务错误，最后等待许可失败也可能包装取消。文档建议用 errors.Is 判断取消和期限错误。新增 Example_retryCancellationResultPriority，固定验证 MaxRetries(0) 和 MaxRetries(1) 下同一次取消/业务失败的不同结果，以及取消后业务成功仍返回成功。

补充 Retry 专题中的分片配额：TokenBucket 每片使用完整 Rate/Capacity，总补充速率及初始容量乘以片数，Per 不参与；SlidingWindow 分配总 Limit 但每片至少 1，Limit 小于片数时实际额度增大，局部拒绝不等于总额度用尽。新增 Example_retryShardedWindowBudget，Limit(1) 配一片和四片分别执行业务 1 次/4 次后耗尽。两新增示例 race 随机十轮通过（2.105s）。

共 42 Examples（docs 35 / retry 3 / sliceops 4）race 随机五轮通过（1.973s / 2.265s / 2.653s）。最终 retry/ratelimit/docs/文档工具 short audit race 一轮通过（4.581s / 5.144s / 5.209s / 2.043s）。1388 声明调用及 3 完整程序、67 组原文局部编译、go vet、1415 条四项说明结构、368 围栏语法和去注释 AST 范围检查通过。运行库仍只有此前四项修复，此轮只新增测试并纠正文档/注释；最新全库 short audit race 仍为根包 658.299s 的阶段。逐方法业务语义审查、Linux 实际执行、百万级 race 内存限制和独立复审的既有缺口仍开放，目标未标完成。

本节加入后，本地锚点共 212 个，0 个未匹配；报告最终更新后的生成一致性和空白检查再次通过。

## Map 聚合与单项统计复核（2026-11-06）

新增 FuzzMapAggregateContracts，使用固定键槽、数值和实际回调访问历史作为预言，不预测 Go map 遍历顺序。覆盖 Runner 的 Seq/并行分片/DefaultPool，以及公开链的默认/Serial/Parallel/DefaultPool 七条入口；0～16 元素、正负值、已取消/存活父 context、无超时/一小时逐回调超时、业务错误及 panic。验证 Filter 条目集合和键值独立、输入不变、Reduce 累积传递、错误时保留失败回调返回值、停止调用次数、panic 原值传播及每次超时 context 在正常/错误/panic 后释放。使用一小时避免真实时钟干扰，期限到达由既有 TestMapTimeoutAllCallbacksRegression 补充；模型不覆盖任意并发修改、全部调度、外部池生命周期或所有引用类型的深复制。

6 个种子 race 随机十轮通过（3.333s），30s、4 workers race fuzz 223,756 次通过（包耗时 41.564s）。仅在 .gotmp/map_aggregate_mutants/ 用 Go overlay 注入“归约错误返回 initial”和“吞掉 Filter panic”两个可编译错误变体，种子均以预期业务断言失败；真实实现未改动。随后 maps/docs/文档工具 short audit race 通过（4.120s/6.053s/4.220s，早于最后单项统计示例与其生成说明）。

专题和逐方法参考补充：Filter 正常返回 error 始终为 nil，已取消回调仍可返回 true 保留条目；Reduce 返回失败回调给出的值，不回滚到之前累积值或 initial。新增 Example_mapAggregateResults 固定验证这两个边界与独立结果键。继续交叉核对发现 ForEach 的“按已处理元素计数”说明不适用于池化路径，现明确池化 Total 包含全部输入，拒绝/取消和未执行业务也计失败；无池只计已开始元素，且不主动跳过已取消父 context。新增 Example_mapForEachRejectedStatistics，固定验证相同已取消输入下无池回调 3 次且成功，池化回调 0 次但 Total=3、FailCount=3。这是文档语义修正，没有更改现有取消和统计功能。

共 44 Examples（docs 37 / retry 3 / sliceops 4）race 随机五轮通过（4.307s/2.544s/3.651s）。67 组 Markdown 原文局部编译和 368 个 README/专题围栏语法通过；去注释 AST 对照仍只有此前四项运行修复。最新全库 short audit race 仍为根包 658.299s 的阶段，此后没有新增库运行改动。逐方法语义审查、Linux 实际执行、百万级 race 内存限制及独立复审仍有缺口，目标保持未完成。

最后的单项统计说明与示例加入后，maps/docs/文档工具 short audit race 再次通过（5.232s/7.775s/4.372s）。API 生成一致性、1388 声明调用/3 完整程序编译、go vet、1415 条四项说明结构、214 个本地锚点（0 未匹配）和 git diff --check 通过。结构和编译指标仅说明被检查部分，不能代替逐方法业务语义核对。

## Map 查询与转换复核（2026-11-06）

新增 FuzzMapQueryTransforms，以最多 64 个固定键槽、int8 值和独立条件模型检查基础 Map、默认链、Serial、Parallel 的 All/Any/Count/Find；验证匹配结果、空输入、Find 零值与 found、短路后不再调用及每键最多一次。转换覆盖 RemapKeys 1～8 类碰撞、Invert 重复值、RemapValues 新类型、ToSlice 完整键值对应及转换后键独立，碰撞代表只要求属于来源类，不假设 map 顺序。首版 4 种子 race 随机十轮通过（4.446s），race fuzz 30s、4 workers 294,755 次通过（35.482s）。加入四入口 Range 提前终止后，最终种子 race 十轮通过（3.293s），race fuzz 288,504 次通过（35.407s）。模型不覆盖浮点非自反键、任意接口动态键、并发输入修改、所有引用值或回调副作用；业务 panic 及其他变更路径另由现有回归补充。

仅在 .gotmp/map_query_mutants/ 用 Go overlay 注入“Any 条件反转”和“RemapKeys 丢失代表值”，最终种子均以预期断言失败，真实实现未改动。注入脚本最初因断言的文本搜索跨越其他方法而未运行，收窄到指定函数后才产生上述有效证据；不把脚本自身失败作为测试识别能力。

Map 方法参考的查询说明现逐项给出短路、空输入和 found 规则，集合运算说明明确按键判断、交集保留接收者值/并集保留 other 值/差集不比较值，并补上具体生产建议。转换专题明确内部入口不等于根包公开函数、碰撞覆盖代表不稳定和 ToSlice 排序要求。新增 Example_mapShallowValuesAndSets，对照集合值来源、独立结果键与共享指针值、有效零键零值及空查询结果。共 45 Examples（docs 38 / retry 3 / sliceops 4）race 随机五轮通过（2.380s/3.285s/3.455s）。

最终 maps/docs/文档工具 short audit race 通过（3.244s/5.275s/1.975s）；API 生成一致性、1388 声明调用及 3 完整程序编译、go vet、67 组 Markdown 原文局部编译、1415 条四项说明结构、368 围栏语法和去注释 AST 对照通过。仍仅有此前四项运行修复；最新全库 short audit race 是根包 658.299s 的阶段，此后没有新增运行库修改。逐方法业务语义、Linux 实际运行、百万级 race 内存限制及独立复审缺口继续保留，目标未标完成。

报告更新后，本地锚点增至 216 个且全部匹配，git diff --check 通过。

## 自动扩缩容决策与边界复核（2026-11-06）

新增 FuzzPoolAutoScaleDecisions 与 FuzzGroupAutoScaleDecisions，使用整数交叉乘法和有理数倍数模型核对严格 3/4 扩容阈值、1/4 缩容阈值、等号、中间区间、连续确认计数、目标方向至少调整 1、上下界、暂停检查及 Disable 目标。每例最多 32 次负载检查，初始 worker 1～8、配置最小 1～3/最大 4～10；注入 Busy 原子计数而非提交真实业务，一小时 ticker 避免真实时钟干扰。初始值允许在范围外，验证 Enable 不立刻夹取目标；Pool 通用 Resize 会改变已保存边界，Group 不改变。模型不覆盖任意阈值/浮点非有限数、巨大容量、真实任务负载/全部调度或实时 ticker；既有关闭、满队列缩容、忙 worker 缩后扩、复位和并发配置回归补充生命周期检查。

Pool 首版 4 种子 race 十轮通过（2.572s），Group 同版通过（2.022s）。加入等号/方向性种子后的首轮 Pool fuzz 报 disabled target=2 want 3，最小输入保留在 testdata/fuzz/FuzzPoolAutoScaleDecisions/c5d7f6a1e611fc5c。沿 resizeLocked 反查发现：全部 Resize 路径都扩展上下界，自动从范围外初始值向边界移动也调用此路径，会改变保存的 Min/Max；首次模型错误假定它们不变。更新模型明确跟踪保存边界后，Pool 种子 race 十轮通过（2.028s），race fuzz 30s、4 workers 360,934 次通过（33.255s）。Group 五种子 race fuzz 同时通过 387,179 次（33.437s）。这是对既有行为与模型假设的修正，没有据此修改运行库；生产文档现要求在配置范围内设置初始值，并说明范围外初始化不用于自动纠正边界。

仅在 .gotmp/autoscale_mutants/ 用 Go overlay 为 Pool/Group 各注入阈值包含等号、忽略连续确认次数两个错误变体。前三项被种子识别；Group 忽略确认次数最初未被固定种子识别，补入 config=1/单次高负载定向种子后，最终全部四变体以预期业务断言失败。最终各六个显式种子（Pool 另有上述复现语料）race 随机十轮通过（Pool 2.190s/Group 2.104s）。前述 30 秒 fuzz 属于补该种子之前版本，不归给最终新增种子。

config/Pool/Group 专题和逐方法参考补充 Enable 保持初始值、Normalize 不校验关系及有限性、严格阈值/等号清计数、乘倍数截断/至少调整 1、边界不变时确认计数清零、Disable 调至 Min 而非单纯停监视、目标数不代表实际 worker 已退出，以及 Pool Reset 保留重启与 Group Wait/Reset 清配置的区别。新增 Example_autoScaleTargetLifecycle 验证 Enable 初始 1 保持不变、Disable 在 Min=2 时反而增加目标，公开示例 race 十轮通过（2.154s）。示例初稿误用不存在的 PoolBuilder.Build，经编译识别后改用公开 Run 再通过，未写入 Markdown 错误示例。

最终 Pool/Group/docs/文档工具 short audit race 通过（20.912s/46.920s/5.139s/2.129s），包含真实在途任务及生命周期回归。共 46 Examples（docs 39/retry 3/sliceops 4）race 随机五轮通过（2.384s/2.244s/2.411s）。API 生成一致性、1388 声明调用和 3 完整程序、67 组 Markdown 原文局部编译、go vet、1415 条四项说明结构、368 围栏语法及去注释 AST 对照通过。仍只有此前四项运行修复，最新全库 short audit race 仍为根包 658.299s 的阶段；Linux 实际运行、百万级 race 内存限制、独立复审和逐方法业务语义的既有缺口继续保留，目标未标完成。

本节更新后，220 个本地 Markdown 锚点全部匹配，git diff --check 通过。

## Pool 关闭扩缩容与索引结果复核（2026-11-06）

扩展 FuzzPoolConcurrentClose，在最多 24 项提交、取消、业务 panic、三种背压及双重关闭之外，加入手动 Resize、重复 ResizeAndWaitTimeout、启用后并发 DisableAutoScale，以及业务主动让出调度。五个显式种子与既有语料 race 随机 20 轮通过（2.592s）；45s、4 workers race fuzz 306,727 次通过（49.360s）。模型验证每次提交终态恰好一次、业务至多一次、结果/计数对应及关闭后 Pending/Busy/Active 为零，不预测竞争的胜者；不覆盖并发 Reset、回调永不返回、所有超时或生产负载。

新增 FuzzPoolIndexedResults，用独立位置字典检查最多 32 次顺序提交的稀疏/重复有效索引、已取消父 context、上下界外非法索引、业务错误及 MaxResults=0～4。每项完成后才发下一项，固定覆盖次序；检查完成回调和业务次数、原位/追加拒绝记录、Occupied 空位、保留上限和按提交计数。首轮四种子 race 十轮通过（4.041s），首次随机运行在 Active=1 的瞬时状态失败；语料 e6f9f9118e5f0836 已保留。源码确认 finishTask 先释放等待，worker 随后才递减 Active，原模型把 Wait 当成 worker 退出屏障不成立。改在 Close 后检查资源归零，没有修改库执行行为；修正种子 race 十轮通过（3.474s），race fuzz 30s、4 workers 249,638 次通过（35.625s）。不覆盖并发重复索引、环形缓冲/结果流、全部入队拒绝或任意大索引分配，相关既有回归在包级测试中补充。

仅在 .gotmp/indexed_mutants/ 用 Go overlay 注入“拒绝覆盖旧位置”和“错误结果不保存”，两个可编译错误变体均被固定种子的预期结果断言识别；真实运行实现未变。

文档更正 CloseAndWaitTimeout 先启动关闭再等待、重复关闭仍可能超时、首次超时通道不代表完整清理，以及计时不覆盖前置互斥锁等待；ResizeAndWaitTimeout 的时长不覆盖 Resize/登记收敛，不取消业务，返回后可以继续提交。补充 Wait 返回瞬间 Active 可暂时非零、CloseByIdle 不检查 Pending 且按 100ms 轮询，不能作为总调用时限；SubmitAt 合法拒绝记录位置、重复覆盖、稀疏空位和非法索引计数规则已按源码细化。新增 Example_poolResizeWaitTimeout、Example_poolRepeatedTimedClose、Example_poolIndexedRejection，分别 race 十轮通过（前两者合计 2.454s、后一项 2.266s）。中间核对发现 MaxPoolIndex 没有根包导出，说明已改为实现常量及数值，未给出不存在的根包符号。

最终 Pool/docs/文档工具 short audit race 通过（22.373s/6.518s/3.713s）；49 Examples（docs 42/retry 3/sliceops 4）race 随机五轮通过（3.908s/3.439s/4.409s）。方法参考一致性、1388 声明调用/3 完整 Markdown 程序、67 组原文局部编译、1415 条四项说明结构、368 围栏语法、go vet 及去注释 AST 对照通过；最后补充的关闭计时与轮询文字另作生成/编译复核。运行库仍只有此前四项修复，最新全库 short audit race 仍为根包 658.299s 的阶段，此后只有测试、工具、注释和文档追加。逐方法业务语义、Linux 实际运行、百万级 race 内存限制及独立复审缺口继续保留，目标未标完成。

最后的计时/轮询说明与本节更新后，方法参考重新生成，一致性及 1388 声明调用/3 完整程序编译通过；1415 条四项结构与 222 个本地 Markdown 锚点全部匹配，git diff --check 通过。

## Mu 快照历史与共享任务许可复核（2026-11-06）

新增 FuzzMuConcurrentHistory，三个并发生产者各执行两项 Append/Snapshot/生成函数 panic/nil 生成函数操作。记录调用/返回次序，以最多 6! 个合法串行历史检查快照内容和最终结果，不假定锁获取顺序；另检查工厂调用一次、panic 原值、失败不追加，以及并发操作结束后修改快照不会改变容器槽位。四个种子与 TestMuFactoryReentryAndPanic 的 race 随机十轮通过（4.217s），race fuzz 30s、4 workers 484,145 次通过（33.573s）。重入回归检查嵌套 Append/Snapshot 不死锁，外层 panic 后已完成的内层追加仍保留。仅在 .gotmp/mu_mutants/ 用 Go overlay 注入“快照别名内部切片”和“每次追加两份”，两者均被模型预期断言识别。模型仅用六操作与 int 槽位，不覆盖任意历史长度、用户共享引用的全部同步方式或内存耗尽。

新增 FuzzBoundedRunnerMixedOutcomes，在最多 24 项并发提交、1～4 许可、成功/业务错误/panic、初始取消/竞争取消下，核对业务至多一次、未执行项为取消、实际执行业务结果不被父取消替换、panic 原值、并发峰值不越界，以及全部句柄完成后 Busy=0/Available=Max。初版 BoundedGo/BoundedGoAct/BoundedGoResult 三路径四种子 race 十轮通过（11.159s），race fuzz 209,393 次通过（33.632s）。加入 boundedGoResultAct 第四路径后的最终四种子 race 十轮通过（2.546s），race fuzz 30s、4 workers 186,242 次通过（34.701s）。用户回调主动让出调度并立即结束，不覆盖永久阻塞、任意超时、runtime.Goexit、非法零值执行器或全部 TaskBuilder 配置组合。仅在 .gotmp/bounded_mutants/ 注入“许可容量加一”和“吞掉业务错误”，两个可编译变体均被预期断言识别。

Mu 说明补充同步生成、重入及外层 panic 不回滚副作用，Snapshot 不等待仍在生成值的生产者、空快照 nil 区别和复制成本；逐方法生产建议改为共享容器、等待生产者和引用元素同步。Bounded 说明明确上限约束业务许可，调用方另建等待提交 goroutine、业务子 goroutine 和许可释放后的结果投递收尾不受约束；补充取消不直接释放许可、正常返回保留业务结果及满额同步嵌套提交的死锁条件。README 的 DefaultBounded 误写为使用默认执行器已改为清除限制，GoResult 返回值误写为指针已改为值类型 TaskHandle[T]，模块表区分 AsyncResult 等待取消和 Task 业务 context 取消。

新增 Example_muFactoryAndSnapshot 和 Example_taskBoundedCancellationAndReuse；后者最终还验证清除限额后的新任务可以运行，已启动任务仍持有原许可。初次 Mu 示例 race 十轮通过（2.627s）；两例初版合计 race 十轮通过（2.286s，早于 DefaultBounded 段）。最终共 51 Examples（docs 44/retry 3/sliceops 4）race 随机五轮通过（2.075s/2.089s/2.143s）；最终 Task/docs/文档工具 short audit race 通过（4.500s/5.434s/2.022s）。方法参考一致性、1388 声明调用/3 完整程序编译、67 组 Markdown 原文局部编译、1415 条四项说明结构、368 围栏语法、go vet 与去注释 AST 对照通过。

本批没有新增库运行逻辑修改，仍只有此前四项修复；最新全库 short audit race 仍是根包 658.299s 的阶段，未把后来新增模型归入该历史运行。逐方法业务语义、Linux 实际执行、百万级 race 内存限制和独立复审的既有缺口继续保留，目标保持未完成。

本节及报告更新后，225 个本地 Markdown 锚点全部匹配，git diff --check 通过。

## README 速查表与原文示例复核（2026-11-06）

核对 README 返回类型表与实际声明，修正 Pipeline ExecuteWithMeta 的单返回值、GroupInstance 类型名、Slice Map/MapBatch/Stream 泛型结果、TaskHandle 值类型，以及 ForEachPool 的池和错误双返回。说明同时更正 Slice Reduce 输入顺序串行归约、默认背压阻塞、OverflowDrop 适用条件、SetDefaultTimeout 的未来 Pool/Group 回调范围，以及 SetSubmitTimeout 只保存配置、需显式传给 Pool/Group 的实际行为。

五组 Pool 详细原文示例加入 docs/cmd/check_markdown_fragments.py，首次编译识别原文未使用的 results/item，随后修正源示例。生命周期、超时、背压和流消费示例补齐提交与业务结果检查；流示例等待消费者结束。环形缓冲示例按最多 1000 项分批覆盖全部输入，每批新池、MaxResults(0) 保留有限批结果，业务结束后分段 Flush 镜像且只处理一次。生成的 TestReadmePoolDetails 用受控业务桩执行五组原文，包括业务失败传播和 1001 项跨批镜像检查；race 随机五轮通过（2.357s）。此检查不覆盖全部拒绝时机、流丢弃或任意业务副作用。

原文局部编译增至 72 组并通过；只补导入、业务桩和明确的片段包装，不把编译算作真实业务执行。一次中间运行在全部包通过后遇 Windows unlinkat Access is denied 清理临时 exe 失败，后续完整重跑退出码为 0；不把该次失败计为通过或库缺陷。

新增 docs/cmd/check_readme_returns.py，直接读取 README 中 16 条终端方法返回类型，实例化 T=int/R=string 后对公开调用编译；这独立于自动生成的声明调用。三个临时 Markdown 错误变体分别恢复错误 Group 类型、TaskHandle 指针及 Meta 双返回，均被对应编译诊断识别；随后真实 README 再次通过。初版工具的正则转义/表格提取及变体诊断匹配问题已在开发时修正，不把脚本自身错误算作有效注入证据。

最终 docs/文档工具 short audit race 通过（8.726s/5.533s）；1388 声明调用与 3 完整 Markdown 程序编译、go vet、1415 条四项说明结构、368 围栏语法以及去注释 AST 范围检查通过。本批只改 README、检查工具和记录，没有新增库运行修改。可执行 Examples 仍为 51 个，本批未重跑其全套，最近 race 五轮结果属于上一节；最新全库 short audit race 仍为根包 658.299s 的历史阶段。逐方法业务语义、Linux 实际执行、百万级 race 内存限制及独立复审缺口继续保留，目标未标完成。

本节更新后，227 个本地 Markdown 锚点全部匹配，git diff --check 通过。

## 分片混合路由与拒绝统计复核（2026-11-06）

新增 FuzzShardRejectedRouting，对 Pool/Group 混合普通提交、非阻塞提交、字符串 Keyed、越界片号回退的稀疏索引提交及批量入口，使用独立整数轮询、FNV-1a 算术、每片结果集合及原子访问次数核对。每例最多 24 个操作、72 个输入、1～4 片、每片 2 worker，覆盖三种分发配置、已取消 context、事先关闭单片、业务成功/失败；批量核对所有输入与首个错误。最终按每片检查结果、拒绝错误、计数及每项业务执行次数，不预测业务完成顺序。模型不覆盖并发配置/关闭、Reset、任意大索引、背压策略或全部生产调度。

首版固定种子失败显示 Pool TrySubmit 对关闭片的拒绝不增加计数；反查源码发现 closed/waited/submitGuard/resizeGuards 状态直接返回，而普通 Submit 会记录这些拒绝。修正预言后 race 十轮通过（3.044s）。首次 fuzz 在第 352 次执行发现无显式背压限制的有限任务 channel 仍可填满，TrySubmit 合法返回 ErrSubmitTimeout；这是模型错误假设，语料 672601a9d6b0c3a8 已保留。模型改为允许非阻塞接受或队列拒绝，但要求后续结果、访问和计数完全一致，没有修改库行为。修正种子 race 十轮通过（2.528s）；30s、4 workers race fuzz 154,532 次通过（33.561s）。

临时 Go overlay 注入“批量遇首错即停止”和“字符串 Keyed 路由偏移一片”两个错误变体。第一个最初未被固定种子识别，补入首项关闭拒绝且后续仍有输入的定向种子；最终两个变体均被预期断言识别，真实实现最终五个显式种子与复现语料 race 十轮通过（2.159s）。上述 30 秒 fuzz 早于补种子；没有把它归给后来的最终版本。临时变体保存在 .gotmp/shard_rejection_mutants/，不改真实源码。

Pool/Shard 方法参考与分片专题明确非阻塞状态拒绝不计数/不存结果、已取消或队列拒绝在状态检查通过后才记录，以及选片不跳过关闭片、不改投空闲片。新增 Example_shardRejectedSubmissions，固定验证 4 项尝试中关闭片拒绝 2 项，其余 2 项完成，Wait 长度/Total=2 而 Fail=0；定向 race 十轮通过（2.024s）。最终 52 Examples（docs 45/retry 3/sliceops 4）race 随机五轮通过（2.036s/2.113s/2.467s）。

Shard/docs/文档工具 short audit race 通过（12.465s/5.412s/2.001s，早于最后定向种子）；1388 声明调用/3 完整 Markdown 程序编译、go vet、1415 条四项说明结构、368 围栏语法与去注释 AST 范围检查通过。本批没有新增库运行修改，仍只有此前四项修复；最新全库 short audit race 仍为根包 658.299s 的历史阶段。独立复核工具本批再次在参数校验阶段报 missing field message，未启动审查，不能算作独立复核。逐方法业务语义、Linux 实际执行、百万级 race 内存限制和独立复核缺口继续开放，目标未标完成。

最终定向种子纳入后，Shard 包 short audit race 随机一轮再次通过（13.845s）。记录更新后，229 个本地 Markdown 锚点全部匹配，git diff --check 通过。后续继续核对专题原文中的提交错误处理及其抽取编译覆盖。

随后补查分片专题五组原文：Pool 批量、Try 批量、索引，以及 Group 批量和索引。原文丢弃提交错误、仅显示业务结果，现先 Wait 已接收任务，再通过 errors.Join 返回提交与结果错误。加入原文抽取器后，成功/业务失败受控 race 五轮通过（1.807s）。追加已取消输入后五轮稳定报 nil，沿原文反查发现构建器未配置 Context，Run 回调的同名 ctx 实际为默认 Background；修正文档五处为 .Context(ctx)，未修改库。最终成功/业务失败/预先取消三路径原文 race 随机五轮通过（2.271s），所有 77 组局部原文编译通过。受控桩只返回有限值/错误，不代表所有业务实现、队列压力或流式调度；77 组仍不覆盖所有围栏。上述 52 Examples 和包级检查早于本段纯 Markdown/抽取器修改。

## 分片共享取消链生命周期修复（2026-11-06）

发现并修复第五项运行缺陷。Pool Builder.Run 原先以 sp.Ctx()（第 0 片的子 context）调用作用域；正常 Wait 结束该片后取消其子 context，其他片若使用 Run context 提交，也被提前取消。TestPoolBuilderNormalWaitKeepsOtherShardContext 修复前稳定失败；另有仅调用整体 sp.Wait、后片等待首片正常完成才返回的确定性 TestPoolBuilderFullWaitDoesNotCancelLateShard，避免依赖调度速度。直接 WithFailFast 返回的共享 context 原先在所有片完成后仍存活，Pool/Group 的定向测试也先观察失败。重复配置测试进一步确认旧的共享派生 context 被覆盖而不释放。

修复把共享 context 保存在 ShardedPool.ffCtx，Builder.Run 在 FailFast 时采用该层；保持公开 Ctx() 返回第 0 片 context 的旧契约。各片正常等待仅释放各自子 context，整体 Wait/Close 完成后取消共享链；Group 成功 WaitContext 也释放，false 分支留给最终 Wait/Close 收尾。重复 WithFailFast 合并取消函数，Pool 成功 Reset 清旧链再重建，Group 成功 Reset 清旧链及配置。父 context 不被取消，提交/业务错误与结果顺序均不变。修复前的正常等待、共享链遗漏释放和重复配置问题是同一生命周期缺陷的三个表现，不重复计为三项修复。

初步定向及既有跨片失败/复位回归 race 随机十轮通过（2.158s）；追加重复配置后相关回归二十轮通过（3.668s），补整体 Wait 确定性回归后再二十轮通过（2.559s）。未完成 Group WaitContext 保留共享 context、后续 Wait 释放的检查另 race 二十轮通过（3.012s）。临时 Go overlay 还原“Builder 使用首片子 context”“Pool 缺共享收尾”“Group 缺共享收尾”“重复配置覆盖旧 cancel”四个错误变体，均被对应业务断言识别；真实源码不受注入修改。

新增 FuzzShardSharedContextLifecycle，2～4 片、每片 1 worker、最多 4 轮顺序 Reset，以通道屏障确保每片任务都已开始，再触发指定片正常完成、父取消、业务失败或 panic。独立结果集合核对正常单片等待不取消共享链、失败跨片取消、每片结果、父 context 值及取消隔离、最终共享释放。三个种子 race 随机十轮通过（2.405s），30s、4 workers race fuzz 70,902 次通过（33.557s）；不覆盖永久阻塞、并发配置/Reset、全部超时和任意负载调度。

专题与逐方法参考补充共享/子 context 的区别、整体收尾的取消、重复配置释放及复位边界；FailFast 原文去掉 Wait 后以 ffCtx.Err() 判断业务失败的错误示范，改为汇总提交与结果错误。新增 Example_shardSharedContextCleanup 定向 race 二十轮通过（2.490s）；53 Examples（docs 46/retry 3/sliceops 4）race 随机五轮通过（2.720s/2.467s/3.146s）。局部原文编译增至 78 组通过，六组分片原文成功/业务失败/取消三路径 race 五轮通过（2.099s）。

最终 Shard/docs/文档工具 short audit race 通过（13.559s/6.400s/3.563s）；追加未完成等待定向测试后，Shard 非 short race 通过（38.790s）。1388 声明调用/3 完整 Markdown 程序编译、go vet、1415 条四项结构、368 围栏语法与去注释 AST 范围检查通过；AST 运行修改从此前四项扩展到本次 Shard 共享取消生命周期，未把其他注释修改算作运行修复。全库 short audit race 的进行中命令先启动于本次初步修复、早于重复配置合并和后续模型，准确结果待结束再追加；不将早期二进制称为最终全库验证。

逐方法业务语义审查、Linux 实际执行、百万级 race 内存限制及独立复核缺口仍保留，目标未标完成。

全库命令已结束：go test -tags audit -short -race -p 1 ./... -count=1 -shuffle=on -timeout=30m 全包通过，根包 651.252s、Shard 15.533s，完整输出在 .gotmp/audit-shard-context-full-short-race.log。该命令从初步共享 context 修复时启动，根包二进制早于重复配置合并和后续测试，因此不将它单独作为最终全部改动的证明。最终代码另以根包分片全部顶层测试 short audit race 随机一轮通过（91.706s），结合上述最终 Shard 非 short race、定向回归、公开示例及原文检查。根包非 short 百万级长测仍未在第五项修复后重跑。

最终源码的 Shard 与根包测试二进制 Linux amd64、CGO=0 交叉编译通过（只编译、未执行、无 Linux race）；231 个本地 Markdown 锚点全部匹配，git diff --check 通过。独立复核仍无有效结论，不能以自行检查替代。

## 多池多组共享取消与原文复核（2026-11-06）

沿第五项缺陷反查 Pool.Shard/Group.Shard 和两个 MultiBuilder.Run。两种构建器都在分片前保留原共享 context，分片后各自另建子 context，未复现 ShardBuilder 原先以首片子 context 作为作用域的缺陷。本批没有修改库运行代码，仍为五项修复。新增 docs/tests/multi_context_lifecycle_test.go，通过公开入口以通道屏障验证两种引擎的首片正常等待、后片成功、整体共享链释放、预先取消的未完成等待也释放共享层，以及父 context 不被反向取消；race 随机 20 轮通过（3.637s）。测试初稿误用不存在的 Group.Ctx，经编译识别后改用首片 Wait 结束的显式屏障；没有为此添加或修改公共 API。

在 .gotmp/multi_context_mutants/ 用 Go overlay 分别注入 Pool/Group 首片不派生子 context、Pool/Group 遗漏共享收尾，共四个变体，全部被预期业务断言识别。真实源码未受注入修改。定向检查不覆盖并发配置/Reset、任意任务负载、永久不返回业务；Group 的并发等待与 Pool 的独占等待语义仍按既有实现保留，没有为统一行为修改运行库。

Pool 专题及方法参考现说明：取得等待权的整体 Wait/WaitContext/WaitTimeout 返回前释放 sharedCancel，即使 bool 为 false；重叠而直接拒绝的多池等待不会释放；单片正常等待只释放子层，父 context 不受影响。Group 专题补同类生命周期与业务错误判断，MultiGroup.Values/Errors 方法参考明确按片号拼接、瞬时快照、Occupied 过滤及浅拷贝，Wait/Close 补充回调重入死锁和重复读取边界。

MultiGroup 基础原文修正忽略提交和业务错误的问题：记录首个提交错误，等待已接收任务，再 errors.Join 合并逐项错误。原文直接抽取后以成功、业务失败、预先取消三种受控桩执行，race 随机五轮通过（3.278s）；局部原文编译扩至 79 组并通过。受控桩不替代真实业务副作用、全部拒绝时机或高压调度。

本批 Pool/Group/docs/文档工具 short audit race 随机一轮通过（21.191s/47.418s/5.304s/10.153s）；1388 声明调用和 3 完整 Markdown 程序编译、go vet、1415 条四项说明结构、368 围栏语法与去注释 AST 范围检查通过。最后新增的 MultiGroup 注意事项仅为文档生成器文本，后续再核对生成一致性。53 Examples 数量未变，完整 race 五轮证据沿用上一节，不声称在本批重跑全套。全库历史运行版本区别和 Linux/百万级 race/逐方法语义缺口继续保留。

独立复核工具本批仍在参数校验阶段报告 missing field message，未启动审查，无独立结论。目标保持 active，未把有限测试或文档结构覆盖当作绝对无缺陷证明。

最后的 MultiGroup 注意事项生成后，一致性/1388 声明调用/3 完整程序编译与 1415 条结构检查再次通过；233 个本地锚点全部匹配。最终文档工具 short audit race（2.065s）和 Multi 生命周期回归 race（2.178s）通过，git diff --check 通过。继续审查 MultiPool 流式原文的错误汇总与保留上限说明。

## 流式错误与配置原文复核（2026-11-06）

直接抽取 Pool 专题 MultiPool 流式原文执行，业务失败时仅打印日志、Run 最终返回 nil，受控断言先观察到失败（3.134s）。根因是原文忽略每次提交与 WaitAndClose 的错误，并把可丢弃结果流当成错误观察路径；Run 本身只返回作用域回调错误。修正原文在提交前配置同步结果回调，以互斥锁保留首个收到的业务错误，独立于每片 MaxResults(1000) 和流。生产者记录首个提交错误，等待关闭全部片后汇总 StreamDropped，再经带缓冲 done 通道返回 errors.Join；消费者持续读完流并只处理成功值。首错顺序不代表输入顺序，预检查拒绝仍检查 Submit 返回值；文档明确结果流不保证可靠交付、成功副作用不回滚、重试须幂等及环形缓冲额外容量预算。

原文加入 docs/cmd/check_markdown_fragments.py，共 80 组局部片段编译通过。新增 TestMultiStreamErrors 验证成功值、8001 个输入中第 0 片第 1001 项的上限外业务失败、panic、预先取消拒绝和空输入；另以通道屏障阻止消费者处理，直到 12000 项业务全部结束，超过每片和合并通道容量，确定性制造真实流丢弃。race 随机五轮通过（2.985s）。此检查用有限同步桩，不覆盖任意 I/O、副作用、消费者 panic 或全部调度；初版抽取器误用了被后续循环覆盖的 section 变量，在提取阶段修正后才得到上述有效业务失败证据。三个仅存在抽取副本中的错误变体（忽略业务、提交、流丢弃）均被预期断言识别，真实 Markdown/源码未受变体修改。

继续对照配置原文，发现内部日志字段列表写有不存在的 Int/Float64/Bool/Duration。新增 docs/cmd/check_core_fields.py 直接提取签名与真实 core 函数做编译赋值，首先报告四个 undefined；原文现只保留真实 Str/Int64/Dur/Any/Err/Bytes，六个签名编译通过。说明补充字段不输出日志、Err 固定 key、引用不深拷贝及库外通过 async.LogField 构造。TraceID 原文与源码注释纠正空 id 必然生成新值的说法：WithTraceID(ctx, "") 委托 EnsureTraceID，已有非 nil 值包括空字符串均保留，缺失才生成；EnsureTraceID 不修复非 string 值。方法参考补充生成与显式覆盖才调用日志上下文注入。新增 Example_traceIDPreservation，定向 race 20 轮通过（2.950s）。

同时纠正 Task/生产文档残留的 goroutine 总数限制表述，与 Bounded 实际业务许可保持一致；Flow Stats 说明明确各字段分开采样、不能从差值判断清理完成。只改 Markdown、生成器、注释和验证工具，没有新增库运行改动。去注释 AST 对照仍限定此前五项修复。

最终共 54 Examples（docs 47/retry 3/sliceops 4）race 随机五轮通过（3.109s/3.671s/3.950s）。最终 core/docs/文档工具 short audit race 通过（3.934s/8.030s/4.457s）；Task/Pipeline 同类回归通过（6.607s/22.205s，早于最后纯 core 文档说明）。1388 声明调用及 3 完整 Markdown 程序编译、go vet、1415 条四项说明结构、368 围栏语法通过。全库最新历史命令的版本区别、Linux 实际执行、百万级 race 内存限制与逐方法语义缺口保留，不把本批包级检查当成重跑最终全库。独立复核工具再次在参数校验阶段失败，仍无独立结论；目标保持 active。

最终六字段检查另对照 log.go 中所有返回 LogField 的包级辅助函数名称，确认原文无重复且集合相等，随后再次编译通过；该工具只检查这组六签名，不扩称全配置声明审查。配置专题删除不存在的 DefaultWorkerSize 入口，改用实际 Worker(8) 和每片默认 IO 预算说明。修改后生成一致性/1388 调用/3 完整程序再通过，80 组原文局部编译重新通过，235 个本地锚点全部匹配，git diff --check 通过。

## TraceID 日志与安全调用契约复核（2026-11-06）

新增 FuzzTraceLogContract，以最多 48 项顺序操作核对已有 TraceID 的原 context 保留、缺失值生成、显式覆盖、空字符串/非 string 值、注入开关、nil/不同具体类型日志器切换、直接日志字段顺序及调用者字段保留、任务日志级别与非法值过滤。记录器检查 With 接收的 trace_id 内容及返回日志器的 WithContext 调用，不只计数。保存并恢复全局配置，不并发修改全局项。初版 race fuzz 642,165 次通过；最后五个定向种子加入并检查注入字段后，种子 race 随机二十轮通过（2.290s），最终 30s、4 workers race fuzz 610,409 次通过（33.440s）。

四个 Go overlay 错误变体分别破坏已有值保留、关闭注入时的直接日志字段、任务级别和调用者字段。首次固定种子没有识别第二项，补入有效 trace + 禁用注入 + 直接日志的序列后，四项均被预期断言识别；没有把首次漏检算作通过。模型不覆盖并发换 key/全局配置、任意后端、日志引用字段的深拷贝、随机源失败或永久阻塞。配置专题和方法参考明确注入开关不关闭直接日志、已有值不补注入、自动 trace 在调用者字段前且不去重，以及后端同步调用/类型化 nil 边界。

新增 TestSafeCallOutcomeContracts、TestSafeCallLoggerPanicPropagates 和 TestSafeCallGoexitRunsDefers，两个入口分别检查 nil/预取消 context、成功、带非零值的业务错误、字符串/error/类型化 nil panic、nil 函数、调用次数与参数原样传递、PanicError 栈/原值/不解包以及包装后的 IsPanic。另以可结束的 goroutine 核对 runtime.Goexit 执行 defer 但不返回，以及日志后端 panic 的传播。定向 race 随机二十轮通过（5.368s）。两个临时 overlay 变体（错误时清零业务值、预检查取消跳过回调）均被预期断言识别。未测试进程退出或任意永久阻塞；没有修改 SafeCall 运行实现。

SafeCall 专题和逐方法说明补充上述边界，新增 Example_safeCallReturnedValues 展示取消仍进入回调且保留非零失败值。最终共 55 Examples（docs 48/retry 3/sliceops 4）race 随机五轮通过（4.196s/4.928s/4.740s）。首轮 core/docs/文档工具 short audit race 通过（3.216s/5.802s/4.629s）。运行库仍为五项修复，本批只新增测试、注释和说明。

另补 TestLogFatalProcessContract，在独立测试子进程验证 Error 级别、Background context、原字段传递、退出码 1、defer 不执行及日志开关不屏蔽，race 随机十轮通过（4.367s）。两个 overlay 变体（退出码 0、Warn 级别）均被进程断言识别；最初注入脚本匹配到注释和代码多处，在唯一性预检查失败，收紧匹配后才执行有效变体。补正逐方法说明：日志后端正常返回后才退出，后端阻塞或 panic 时尚未调用 os.Exit；明确根包无 async.LogFatal。最终 core/docs/工具 short audit race 再通过（4.444s/6.036s/4.402s），go vet 通过。

当前五项运行修复的完整命令 go test -tags audit -short -race -p 1 ./... -count=1 -shuffle=on -timeout=30m 已退出 0，全包通过：根包 637.910s/docs 7.589s/工具 4.222s/core 3.442s/group 49.108s/maps 4.158s/pipeline 21.619s/pool 22.069s/ratelimit 6.722s/retry 6.209s/shard 15.466s/sliceops 11.253s/task 5.197s。原始输出见 .gotmp/audit-final-trace-full-short-race.log。命令启动后的新增 SafeCall/LogFatal 测试和最后纯文档说明另有上述定向及包级证据；本批始终未改运行逻辑，不需将此前第五项修复的旧二进制充作当前全库结果。

最终说明版本的 1388 声明调用/3 完整程序、80 原文局部片段、六字段签名、16 README 返回契约编译通过；1415 条结构、368 围栏语法、237 本地锚点及 git diff --check 通过。最后 EnsureTraceID/WithTraceID 注释消除必然注入的旧摘要后，生成一致性/声明调用/完整程序重新通过。独立审查工具仍在参数解析阶段失败，未启动审查，不能算独立结论。逐方法语义、当前运行版本的非 short 长测、Linux 实际执行及百万级 race 环境缺口继续保留，目标保持 active。

## 分片令牌桶路由与限流返回契约复核（2026-11-06）

新增 FuzzShardedBucketRouting，用独立整数四分之一令牌模型核对 1～8 片、每片 0～8 令牌、最多 64 项 Allow/AllowN/直接 GetShard 请求的接受结果和每片余额。速率固定为零，去除墙钟补充影响；失败和非正请求仍推进外层轮转，直接分片调用不推进。四种子 race 随机二十轮通过（3.926s），30s、4 workers race fuzz 476,977 次通过（34.902s）。两个 Go overlay 错误变体（非正请求跳过轮转、容量再除以片数）均被模型识别。该模型仅覆盖顺序有限四分之一请求，不替代正速率实时补充、并发路由、NaN/Inf/负容量或 uint64 轮转溢出检查。

新增 TestLimiterScopeClosesOnEveryOutcome，对五个 With 限流作用域分别执行成功、返回业务错误和 panic，核对回调恰好一次、作用域内可以获取、错误及 panic 原值、作用域后底层关闭、补充 goroutine 完成信号和逃逸指针无法再获取。race 随机二十轮通过（2.214s）；省略 Close、吞掉回调错误两个临时变体均被识别。使用小容量、长补充周期及能结束的同步回调，不据此保证任意用户任务会自行结束。

限流专题表格中的 *ShardedSlidingWindow 和 *ShardedAdaptive 不是实际返回类型；先抽取原文编译，观察 undefined 失败，再改为 *ShardedSlidingWindowRateLimiter 和 *ShardedAdaptiveRateLimiter。新增 docs/cmd/check_limiter_returns.py 编译八项表格返回契约，两个还原拼写错误的 Markdown 变体均被编译器拒绝，真实文档重新通过。DefaultRate 表原称“操作/秒”，实际仅设置 Rate 并保留 Per，现按“每个配置 Per 窗口”说明；补充未配置 Per 为零、DefaultPer 才设一秒等差异。

新增 Example_shardedBucketFractionalRouting，展示小数额度、零请求和拒绝都推进轮转、各片容量独立。共 56 Examples（docs 49/retry 3/sliceops 4）race 随机五轮通过（2.244s/2.165s/3.433s）。限流专题及逐方法参考补充整笔小数请求、GetShard 直接调用只改变片余额等契约。首轮 ratelimit/docs/工具 short audit race 通过（4.608s/5.788s/2.036s），go vet 通过；本批没有新增运行修复。

追加 TestLimiterBuilderDefaultsAndOwnership，从构造出的实际实例核对零值与显式 Default 的区别、单片及分片额度规范化、原始零窗口保留、小数令牌预算、重复 Build 的资源/额度独立、Close 不影响另一次 Build、保存的已取消 context 不绑定实例，以及新子构建器不继承旧配置。race 随机二十轮通过（2.165s）；“DefaultRate 重置 Per”和“TokenBucket 隐式补容量”两个 Go overlay 变体被预期断言识别。测试不等待补充 tick，采用短小资源配置；不能据此推出构建器可并发修改。

新增 docs/cmd/check_pipeline_returns.py，抽取流水线表格的五项模式切换及十八项终端返回类型，用 T=int 编译通过（首次成功 1.072s，错误变体检查后真实原文 1.471s）；不存在的串行类型和把 Meta 结果写为普通 Result 两个 Markdown 变体均被拒绝。工具初版正则转义失效导致未加包名前缀，修正工具后才进行有效检查，没有把该失败算为文档缺陷。另明确 Pipe 两个句柄定义在内部包，根包无对应类型别名，外部代码通过类型推断接收或传递公开结果 channel。全部 23 项只编译、不执行业务。

当前五项运行修复的内部包及 docs 非 short race 命令已退出 1：core 3.073s/group 46.946s/maps 7.853s/pipeline 66.312s/pool 21.058s/retry 22.073s/shard 34.974s/sliceops 35.065s/task 120.291s/docs 6.548s/工具 2.383s 全部通过；ratelimit 在 8.632s 因 ThreadSanitizer 无法分配 TracePart、Windows 1455 退出，未出现 DATA RACE 断言但该包不能算通过。日志 .gotmp/audit-current-internal-nonshort-race.log。当前根包 go test -tags audit . -count=1 -shuffle=on -timeout=3h 无 race 长测仍在同一会话运行，未退出前不算通过。上述非 short 命令启动后新增的构建器测试另有定向 race 证据。

最后追加说明后的 1388 声明调用/3 完整 Markdown 程序、80 组原文局部片段重新编译通过；去注释 AST 差异仍限于既有五项运行修复。逐方法业务语义、Linux 实际执行、百万级 race 内存限制和独立审查缺口继续保留；独立审查工具仍未成功启动，目标保持 active。

最后构建器测试加入后的 ratelimit/docs/工具 short audit race 再通过（4.684s/5.188s/2.333s），go vet 通过。另补当前 ratelimit 非 short 无 race 全包运行通过（9.907s，.gotmp/audit-current-ratelimit-nonshort.log），以及十个十万档并发子用例和匹配的并发回归 race 通过（8.683s，.gotmp/audit-ratelimit-100k-race-current.log）。普通非 short 覆盖当前 UseTier 中的百万档；十万档 race 命令明确不执行百万子用例，不能填平此前 Windows TSan 分配失败缺口。

继续抽取 Task 专题四种模式、四个终端、九项 AsyncResult 等待/查询和四项 TaskHandle 字段/方法，共 21 项原文返回/字段契约。新增 docs/cmd/check_task_returns.py 编译通过（0.852s，两个变体后真实文档 0.842s）；将具名空类型错误写成 struct{} 的 AsyncResult，以及将 CancelFunc 写成返回 error 的函数，均被编译器拒绝。初版预期条目数多算一个，在工具计数断言处失败，修正为实际 21 项后才做有效检查；没有修改运行库。

Group 概述将“只能一次性”更正为按批次运行、完整 Wait 后成功 Reset 可以复用；与 MultiGroup 没有整体 Reset 的差异分开说明，并删除附录重复的 Close 行。原有两批次生命周期模型（自有 worker/注入 Pool，成功/业务错误/panic、重复 Wait 和 Reset）种子 race 随机二十轮通过（4.512s），核对该说明。新增 docs/cmd/check_group_returns.py 编译附录 37 个返回契约及十二组合配置在两种实例上的 24 个调用（2.984s），并比对源码确认 49 个公开 Group 实例方法全部在附录或组合表表示。错误 Wait 类型、漏掉 WithContext 的 context 返回值、漏一组合配置三个 Markdown 变体均被识别；初版替换器错误造成的工具编译失败不计作库或文档缺陷。

Group 说明更新后，生成一致性、1388 声明调用及 3 完整 Markdown 程序再次通过。Group 附录工具消除正则警告后再次通过（4.727s）；最终说明的 80 组原文局部编译、1415 条四项说明结构、368 围栏语法、239 本地 Markdown 锚点及 git diff --check 通过。本批累计运行修复仍为五项，新增代码均为测试和文档工具。根包当前无 race 非 short 长测仍在原会话运行，未将它计为完成。

继续扩展限流表格检查至全部 11 个 RateLimiter 实例方法，源码/表格集合对比先报告遗漏 WithTraceID，再补入实际双返回签名。补充保存日志 context 与逐次 Acquire/Wait context 的区别、已有 TraceID 保留原 context 的规则；文档编译及三项错误变体检查通过（最终真实表格 1.361s）。核对中曾误认为 Token 没有根包别名，随即查到 async.go 的公开 Token alias，已纠正文档和工具以 async.Token 编译；没有保留这一错误说明。

Group 表格工具扩展至 NoResult 的 32 项返回契约，结合组合配置覆盖 NoResult 全部 44 个公开方法；Group 仍覆盖全部 49 个方法。最终编译通过（3.142s），新增“无返回值 Wait 错写 error”和“等待数量 int64 错写 int”两个变体，连同此前三个共五个均被识别。工具不创建 Group、不执行业务，其覆盖是名称及返回/调用编译契约，不是全部参数语义和并发行为证明。

新增 docs/cmd/check_group_void_example.py 抽取 GroupVoid Run 原文，使用本地数据库桩实测。原文对业务错误返回 nil 的失败先在 race 五轮重复出现；修正为检查 Go 的提交错误、全部提交后 Wait 并 JoinErrors，随后成功/业务失败/panic/预取消/空输入五路径 race 五轮通过（2.155s）。Build 例也补提交错误处理，正文明确 Run 只返回作用域回调错误而 Close 负责收尾，不自动汇总。两个吞掉业务或提交错误的原文变体被预期断言识别；最初“continue 后仍 JoinErrors”变体可从已记录结果返回取消错误，属于可替代处理而非此预言可识别的错误，未把它算作已识别缺陷。最后 docs 的 49 Examples 单包 race 五轮通过（4.401s）；全套仍为此前 56 Examples，未新增 Example 数。

GroupVoid 修正后的最终 group/docs/工具 short audit race 通过（49.233s/5.486s/2.312s）。最终生成一致性、1388 声明调用/3 完整程序、80 原文局部编译、go vet、1415 条四项结构、368 围栏语法、239 本地锚点和 git diff --check 通过。最后仅追加本段验证记录，未改变运行实现。

## Group Run 原文与提交重试协议复核（2026-11-06）

新增 docs/cmd/check_group_run_examples.py，直接抽取普通 Group Run 的基础用法和 fn 内 Wait 两段原文，使用本地 process 桩检查成功、业务失败、panic、预取消提交和空输入。修正前两个例子的业务错误都返回 nil，race 五轮稳定观察失败（3.561s）；修正为检查 Go 的提交错误、Wait 后返回 JoinErrors，五路径 race 五轮通过（1.901s）。正文明确 Run 只传播作用域回调错误，提前返回仍由 Close 等待已接收任务，结果变量在提交中途返回时不会被赋值；需要部分结果时应显式收尾。吞掉业务或提交错误两个临时抽取副本均被断言识别，真实原文没有被错误注入修改。

新增 internal/retry/worker_contract_fuzz_test.go，用独立提交协议模型核对最多 16 项后端动作、负重试次数与尝试预算、直接或包装的 ErrSubmitTimeout、其他错误、后端内取消、成功、panic 和延迟执行的原业务回调。后端不执行回调，Bind 返回后才手动执行，核对 context 原样传递、错误原值和恰好一次。初始五种子 race 二十轮通过（4.411s），初版 30s/4 workers race fuzz 538,978 次通过（34.605s）；补取消后仍有尝试和取消并返回其他错误两个定向种子后，最终七种子 race 二十轮通过（3.478s），最终模型 race fuzz 540,810 次通过（35.156s）。这两个 fuzz 数量属于不同版本，不相加成单次最终覆盖。

三个 Go overlay 变体分别破坏包装超时识别、以后端完成后的取消覆盖返回结果、提前主动调用业务函数，全部被协议模型的预期断言识别。模型中 backoff 固定为零，不覆盖墙钟、任意并发后端或永久阻塞。逐方法参考补充：每次尝试前检查取消，但不能打断同步 Submit；后端返回 nil 仍为提交成功，其他错误保持原值；最后一次超时包装为 exhausted，仍有重试预算时才进入下一轮或退避取消。后端必须保证超时表示未接收任务，否则可能产生重复副作用；后端 panic 原样传播。BindRetryToWorker 位于 internal/retry，根包没有该入口，未伪造公开调用示例。

继续新增 worker_timing_contract_test.go，使用 Go testing/synctest 虚拟时间核对指数时间线、正数上限、首次退避超过上限、零/负初值、最后一次失败后无等待、退避中取消，以及可释放阻塞后端在取消后仍须完成 Submit。race 随机二十轮通过（2.323s）；忽略上限、忽略退避取消、最后一次后仍等待三个 overlay 变体均被断言识别。采用能结束的后端和虚拟计时，不声称库能终止任意业务或后端。没有修改重试运行逻辑。

Retry 专题两处 DefaultRate 的“10/秒”说明改为区分模式：RateLimiter 每个 Per 窗口 10 个，TokenBucket 每秒 10 个；DefaultRate 只恢复有效 Rate，并保留自定义 Per。同步修正 retry_builder.go 摘要注释并重新生成方法参考，消除正文正确但生成摘要仍错误的不一致。

另新增 docs/cmd/check_group_stream_examples.py，直接执行 Group 的 StreamResults 和 ResultCallback 两段原文。旧文在业务失败、panic、预取消三路径均漏报错误，race 五轮观察失败（2.719s）；修正为保存首个提交错误、完整 Wait 后 errors.Join 合并 JoinErrors，流式例继续等待消费者完成。成功/业务失败/panic/预取消/空输入五路径 race 五轮通过（4.039s）。两种模式各自吞业务和吞提交错误的四个副本均被断言识别；提交拒绝也可保存在 Group 结果中，因此提交错误变体须同时破坏这两个错误来源才确实漏报，没有把只保留其中一路的有效写法判作缺陷。测试只检查小批成功值完全消费，不保证过载或慢消费下流无丢失。

最后新增定时测试后的 retry/docs/工具 short audit race 全部通过（4.557s/5.932s/2.429s）。生成一致性、1388 声明调用/3 完整 Markdown 程序、80 组局部片段、go vet、1415 条四项结构、368 围栏语法、239 本地锚点及 git diff --check 通过；新增四段 Group 原文的执行证据独立记录，没有把它们重复计入 80 个编译片段。Examples 仍为此前 56 个，本批未新增 Example 函数。去注释 AST 差异仍限于既有五项运行修复，本批只改文档、注释、验证工具和测试。

根包当前无 race 非 short 长测仍在原会话运行，2026-11-06 15:38 CST 进程仍存活，未退出前不能记为通过。既有 ratelimit 百万档 race 的 Windows TSan 分配失败、Linux 实际执行、全部逐方法业务语义及独立审查缺口保留。独立审查工具仍在参数解析阶段失败，未启动审查，没有独立结论。目标保持 active。

随后对照记录路径，统一 Group/NoResult/Builder 回调摘要和专题正文：回调不只发生在 worker 中，提交取消或拒绝也可在调用方同步通知，而最早 waited/waiting 拒绝不通知。新增 TestResultCallbackSubmissionBoundary，验证预取消在 Go 返回前通知、业务未执行、Wait 后拒绝仍保留错误且不新增通知；race 随机二十轮通过（2.260s），遗漏取消通知和错误新增 waited 通知两个 overlay 变体被断言识别。新测试加入后的 group/docs/工具 short audit race 通过（46.788s/5.765s/2.033s）；源码注释再同步到 NoResult 后仅需重新生成和一致性检查，运行代码未变。新增报告链接后本地锚点为 241 个，均匹配；最后 go vet 与去注释 AST 范围复核通过。

NoResult 最后注释同步后已重新生成；最终一致性、1388 声明调用及 3 完整程序再次编译通过，1415 条四项结构、241 本地锚点和 git diff --check 通过。根包长测仍在原会话，未结束，继续保留待验证状态。

## FailFast 原文与定时限流历史复核（2026-11-06）

新增 docs/cmd/check_group_failfast_examples.py，直接抽取 Group 的基础及生产 FailFast 两段原文，用本地 process 桩验证成功值、业务失败、panic、预取消、空输入及协作取消。最后一项用通道屏障确保等待取消的任务已开始，再让另一任务失败，不使用墙钟睡眠。原例忽略每次 Go 错误、回调始终返回 nil，预取消还会继续尝试后续项；受控业务断言在 race 五轮观察失败（1.389s）。其中基础例本来明确把业务错误留在 results，不能把此返回策略本身误算为库缺陷；本次按生产使用要求补全错误处理。现保存首个提交错误并停止后续提交，完整 Wait 后赋值 results，再 errors.Join 合并 JoinErrors，六路径 race 五轮通过（3.976s）。

两种 FailFast 原文各注入“吞业务错误”“提交拒绝后提前返回、遗漏部分结果”共四个临时副本，均被对应断言识别。正文明确 Run 不自动汇总错误、未尝试提交的项没有结果、结果按记录顺序而非输入顺序、FailFast 仅协作取消、成功副作用不回滚；忽略取消且不返回的业务仍会阻塞 Wait。没有更改 Group 的运行行为。

Group 流式原文检查扩至第三段生产示例，先观察业务错误、panic 和预取消均返回 nil（race 五轮失败，2.134s）。修正为 defer Close、带缓冲 done 错误通道，生产者完成 Wait 后通过保留结果汇总错误，消费者排空后读取 done 再返回。两个吞业务/提交错误的副本被识别；三段流式/回调原文五路径 race 五轮全部通过（2.625s）。有限小批次检查不证明慢消费不丢失，处理函数须及时返回，其自身失败须另行设计错误汇总及收尾。

80 组局部原文编译器跟随生产流式片段改为 error 返回并导入 errors；首次因夹具遗留未使用的 log 导入失败，清除夹具导入后全部重新编译通过，没有修改原文来迁就夹具。Examples 数量仍为 56，本批新增的是原文执行工具和测试，不重复计数。

新增 FuzzRateLimiterTimedHistory，在 Go testing/synctest 内使用真实 100ms ticker 和补充 goroutine，独立整数模型核对最多 32 项串行 Acquire/Release/推进 tick/Resize、1～8 的速率及容量、可用额度和半令牌余量。固定 Per=200ms，Reject 策略，每个虚拟 tick 后显式等待协程阻塞再观察，不覆盖任意并发配置、任意周期或超大额度。初始三种子 race 随机二十轮通过（3.822s）、30s/4 workers race fuzz 369,532 次通过（33.706s）；增加跨 Resize 余量定向种子后，最终四种子版本 race fuzz 369,462 次通过（35.451s）。分别记录版本，不把两次执行数相加充作一次最终模型覆盖。

另增 TestRateLimiterResizeBetweenTicks，在首次 tick 前的 50ms 进行升速/降速，100ms 时核对旧速率时间与新速率时间各自结算的一个令牌。该测试与最终模型四种子合并 race 随机二十轮通过（4.775s）。四个 Go overlay 变体分别破坏 Resize 余量保留、调整后许可余额、ticker 小数累计及调整前旧速率时间结算，均被模型或定向断言识别。第四项初次匹配同时命中 ticker 和 Resize，在唯一性预检查处拒绝运行，收紧到独立 Resize 语句后才取得有效失败；不把脚本匹配失败算作库缺陷。

限流专题及方法参考补充 Resize 先结算旧速率时间、保留未满令牌余量、桶满清余量，以及原 ticker 不重新计时。最终 ratelimit/docs/文档工具 short audit race 通过（6.753s/7.491s/4.218s），生成一致性、1388 声明调用/3 完整程序、go vet、1415 条四项结构、368 围栏语法、241 本地锚点及 git diff --check 通过；去注释 AST 差异仍仅既有五项运行修复。本批无新增运行缺陷或实现改动。根包无 race 非 short 长测原会话仍存活且未退出，继续保持待验证，不重启。逐方法剩余语义、Linux 实际执行、百万档 race Windows 资源限制、独立复核缺口保留，目标保持 active。

追加 TestRateLimiterWaiterTransitions，使用 synctest.Wait 确认两个等待者确已阻塞，再分别触发 Block 取消、BlockForce 取消后 Release、Resize 扩容、Close、真实 ticker 补充。五路径 race 随机二十轮通过（4.231s）；BlockForce 错误响应取消、Release 遗漏 Signal、Resize 遗漏 Broadcast 三个 overlay 变体均被确定性业务断言识别。同步关闭会清理仍阻塞者，没有用永久阻塞测试占用工作进程。加入该测试后的 ratelimit/docs/工具 short audit race 再通过（6.218s/6.318s/4.377s），go vet 通过。新增报告链接后 243 个本地锚点均匹配。

## 默认超时读取时机与根包长测完成（2026-11-06）

新增 docs/tests/default_timeout_contract_test.go，通过公开 Group/GroupVoid/Pool 入口验证 DefaultTimeout 的读取时机、正数覆盖、零/负数回退及实例 WithTimeout(0) 清除期限；修改全局配置时串行执行并 defer 恢复原值，不启动业务任务、不使用真实时间等待。Group/GroupVoid 的 DefaultTimeout 保存调用当时正数值，Pool 的同名方法清除覆盖并在 Run 创建池时读取全局；保存非正数的 Group 构建器也回退到创建时的全局值。定向 race 随机二十轮通过（3.355s）。初稿误用了 PoolBuilder 不存在的 Build，在编译阶段失败，改用真实 Run 入口后才取得有效运行证据；没有为测试添加公共 API。

三个临时 Go overlay 变体分别使 Group 延迟读取默认、Pool 提前保存默认、Group 零值直接清除全局默认，均被公开结果快照断言识别。配置专题补充两类构建器的差异、改变全局不影响已有实例和实例清除期限；Group/Pool 表格把固定“30s”改为初始全局值，Group 超时建议删除“兜底”表述，明确期限依赖业务协作、不强制停止任务。既有逐方法构建器说明已准确描述该区别，本次没有重复修改实现。加入测试及最后说明后的 docs/工具 short audit race 通过（5.882s/2.981s）。

当前五项运行修复的根包非 short 原运行已终止且退出 0：go test -tags audit . -count=1 -shuffle=on -timeout=3h，根包 5132.560s。原始输出 .gotmp/audit-current-root-nonshort.log。该命令没有 race，启动后本批仅新增测试和文档/注释，没有修改五项运行修复；因此是当前运行逻辑的根包长测证据，但不声称其二进制包含后来新增的测试。追加测试各有上述定向和包级 race 结果。此前 ratelimit 百万档 race 的 Windows TSan 资源失败仍未被这次普通长测替代，Linux 实际执行及独立复核缺口不变。逐方法业务语义仍在继续，目标保持 active。

最终配置说明及报告追加后，生成一致性、1388 声明调用/3 完整 Markdown 程序、80 组原文局部片段重新编译通过，go vet 通过；1415 条四项结构、368 围栏语法、245 本地锚点及 git diff --check 通过。本批所有已启动定向、变体、包级和根包长测会话均已取得终态，不再将根包标为运行中。以上结构/编译检查仍不等于全部业务语义已核对，继续保留剩余审查范围。

## 流水线原文与取消背压复核（2026-11-06）

新增 docs/cmd/check_pipeline_stream_example.py，直接抽取 ExecuteStream 使用示例，仅补导入、包装和本地业务桩，捕获原文实际 stdout。原文没有 SkipOnError 且不检查逐项 Err/Occupied，race 五轮先观察到早期业务失败或 panic 被后续成功覆盖、失败值及预取消零值被当成功打印（失败包 1.410s，完整输出 .gotmp/pipeline-stream-original-red.log）。此为示例的错误处理问题，默认 SkipOnError=false 的覆盖语义保持。修正后显式跳过前错、先检查 Occupied、errors.Join 汇总顶层与逐项错误，只打印成功项；初版十路径 race 五轮通过（3.455s），增加部分成功路径后的最终十一条路径版本通过（6.326s）。不将两版数量相加充作新 Example；Examples 仍为此前 56 个。

新增 docs/cmd/check_pipeline_etl_example.py，抽取完整 processRecords 函数。原文按输入顺序遇错即返回，在首项读取失败而后两项已经写入时返回空 ID，另会遗漏后续清洗错误；race 五轮先失败（2.233s）。这属于示例选择首错与部分结果策略，不能当作库运行缺陷。按生产要求改为收集全部已送达成功 ID 并汇总全部已送达错误，成功、部分失败、混合错误、清洗失败、写入失败、panic、预取消、空输入八路径 race 五轮通过（2.505s）。业务桩不改 Record/CleanRecord 的原文类型，使用原子计数验证成功 ID 集与写入数，不假定并发调用顺序。

ETL 正文补函数参数、返回值、所需导入和生产边界：取消可能使写入成功但未送达收集器，业务也可能在写入后返回错误，因此 ids 不是全部数据库副作用的清单；须用业务键幂等和处理记录对账，不能错误后整批无条件重放。80 组原文局部编译工具为 ETL 新增 errors 导入后全部通过。

新增 docs/cmd/check_pipeline_example_mutants.py，真实 Markdown 不注入错误，只运行五个临时副本：恢复早期错误覆盖、吞逐项错误、遗漏 Occupied 检查、遇首错遗漏后续成功 ID、只保留首错。五者均由对应业务断言识别，未将编译失败算作识别成功。副本执行后其临时 Go 夹具保留变体内容，最后须重新执行两个真实原文工具恢复并确认绿色。

新增 internal/pipeline/stream_cancellation_contract_test.go。背压测试在 synctest 内以 4200 项填满 4096 输入缓冲，八个回调在屏障等待；取消解除提交阻塞，但方法必须等待这些回调真正退出，并保留它们返回的业务错误或 panic，未开始项维持未 Occupied，后阶段不调用。初版 race 随机二十轮通过（4.557s），丢弃运行中错误与吞顶层取消两个 Go overlay 变体被对应断言识别。加入预取消空阶段/空输入边界后，两测试合并 race 随机二十轮通过（3.359s）；空边界直接返回 nil error 是既有行为，不修改实现。专题及逐方法参考同步说明返回长度固定为输入长度、空边界不检查取消。该测试约束特定屏障历史，不声称覆盖全部调度或可终止忽略取消的任意业务。

初版新增背压测试后的 pipeline/docs/工具 short audit race 通过（21.181s/7.703s/2.465s）；后续加入空边界及最终文档说明后的最终验证另记于下。生成一致性、1388 声明调用/3 完整 Markdown 程序、go vet、1415 四项结构、368 围栏语法及 git diff --check 已通过。本批去注释 AST 核对仍只显示既有五项运行修复，无新增运行实现修改。根包 5132.560s 长测已结束，不重启；Windows 百万档 race、Linux 实际运行、独立审查和逐方法剩余业务语义仍有缺口，目标保持 active。

空边界测试加入及方法参考重新生成后，pipeline/docs/工具 short audit race 再通过（21.407s/5.890s/3.374s）。原文恢复运行时 ExecuteStream 十一路径和 ETL 八路径再次通过（2.232s/3.963s）。随后继续检查完整 Flow 示例，不把上述结果声称为后续 Flow 修改后的验证。

新增 docs/cmd/check_pipeline_flow_example.py，直接执行“完整示例：常驻数据管道”，以并发安全 sink 保存结果，按原文日志策略断言失败可见，不强求 void 片段返回汇总错误。旧文 decode 错误/panic 被 process 成功覆盖并写入 sink，且 Flow 构建时使用 Background，Submit 的请求 ctx 没有传到处理函数，race 五轮先失败（3.136s）。改用公开 NewFlow(ctx, FlowConfig)，显式 process.SkipOnError=true；不新增链式 API、不更改库默认值。正文补管道与提交上下文区别、并发存储、日志策略、取消可丢结果及幂等边界。

Flow 初版成功/空输入/两阶段业务与 panic/上下文七路径 race 五轮通过（10.510s）；补 600 项跨缓冲输入检查完整消费与关闭排空，最终八路径 race 五轮通过（4.689s）。原文关闭排空与 Results 两条消费路径可能竞争取得结果，因此只断言保存集合完整且无重复，不要求特定消费者一定收到某项。80 组局部片段在 Flow 原文变化后重新编译通过。变体工具增加错误覆盖与丢管道 context 两个副本；首次 context 匹配命中两处，在唯一性预检查拒绝执行，收窄到完整 Flow 章节后重跑，预检查失败不计为业务缺陷识别。

最终七个 Markdown 变体均被预期业务断言识别，随后重新运行真实原文恢复全部临时夹具：ExecuteStream 十一路径、ETL 八路径、Flow 八路径 race 五轮通过（2.996s/2.155s/2.103s）。最终生成一致性、1388 声明调用及 3 完整程序编译、go vet、1415 四项结构、368 围栏语法、247 本地锚点及 git diff --check 通过。全部本批启动会话已退出；没有遗留运行中的根包或变体任务。文档结构覆盖与本批小业务桩不证明全部 API 语义，剩余方法、组合场景及先前外部证据缺口继续保留。

## Maps 执行策略与纯查询交叉复核（2026-11-06）

复核 internal/maps 现有独立模型及调用路径：FuzzMapAggregateContracts 逐项比较 Filter/Reduce 在直接串行、普通分片、默认池、串行/并行构建器及 FailFast 组合下的键值集合、回调调用、错误/panic 与 timeout context 生命周期；FuzzMapQueryTransforms 对原生容器和链式串行/并行构建器检查 All/Any/Count/Find 短路及 RemapKeys/RemapValues/Invert/ToSlice 的冲突与浅拷贝语义；FuzzMapRunnerStrategies 对 map 顺序无关地核对 Map/ForEach/Stream/MapBatch/ForEachBatch 的成功值、逐项调用及错误计数。

本轮 internal/maps race 随机二十轮通过（121.665s）；三个独立 fuzz 分别通过 801,773 / 1,304,090 / 62,602 次执行（20s 预算；实际总时长 21.989s/21.996s/21.609s）。用例使用固定键位作为 oracle，不假设 map 顺序；Runner 模型采用 48 项上限、策略为串行/并行/分片/默认池，不代表全部键类型或任意并发池生命周期。

代码交叉阅读发现 execMapPool 的失败快停错误源于已运行回调，提交拒绝由 completion 槽记录；随后 Wait/ResetWait 和结果快照在锁内收尾。Owned Filter/Reduce 虽然保持同步回调实现，也按配置关闭被接管池；对应既有测试检查了池关闭及空操作。未得到新的、可重复的运行缺陷证据；不将接受 any 的 WithPool 错误动态类型后退回无池执行直接归为 bug，因为类型合同明确限定 *Pool[R]，尚无相反文档保证。

其中 FuzzMapRunnerContracts 明确报告 no fuzz tests：误指定了不存在的 fuzz 名称；该调用不计执行覆盖。后续以日志明确列出的三个已存在 fuzz 名称分别运行，三者均有实际执行计数并通过。此处保留失败命令记录，避免把成功的普通 Go 包构建误报为模糊测试成功。当前未改 maps 运行实现或 mapreduce 文档，继续核查全库其他执行引擎与组合；目标保持 active。

## Pool 结果索引与并发关闭复核（2026-11-06）

复核 Pool 当前索引结果 oracle、限量及环形缓冲保留、明确索引拒绝、Overflow 策略、重复 Wait/Reset、提交关闭竞争和 AutoScale 状态模型，并交叉查看 completion adapter 对被接收、超时、拒绝结果的完成通知。

重跑既有 internal/pool fuzz：FuzzPoolIndexedResults 的 race seed 已由本轮 short 包 race 执行；独立 20s 普通 fuzz 实际 936,380 次通过；FuzzPoolConcurrentClose 的 race seed 通过且 20s fuzz 568,007 次通过；FuzzPoolRetainedLifecycle 的 race seed 通过且 20s fuzz 274,032 次通过。测试限制在最多 32、24、48 输入动作，池大小/索引/错误路径有定向种子，不代表任意业务阻塞长度。internal/pool 当前 audit short race 随机顺序通过（23.979s）。

关闭模型同时执行 SubmitWithCompletion、Close、取消、Resize、AutoScale 开关及短时关闭等待，核验各终态回调恰好一次、业务执行至多一次、结果占位和计数相符。索引模型将非法索引排除在提交计数之外，并测试拒绝复用索引时保留既有结果；保留模型逐轮检查成功、业务错误、panic、预取消、结果限额/溢出镜像及 ResetWait/Reset 后复用。未出现与模型合同不符的结果覆盖、重复回调或终态计数不一致。AutoScale 在此由包 race seeds 和源代码状态模型交叉检查；本轮三个独立 fuzz 分别聚焦索引结果、并发关闭及保留生命周期。

当前未发现新的 Pool 运行缺陷或文档矛盾；运行实现和 pool 文档未改。本轮只核验上述三个明确命名的 fuzz 和包测试，其他 pool API 组合与外部百万档 race/Linux 执行仍属开放审查范围，目标保持 active。

## Task 结果缓存与限流许可交叉复核（2026-11-06）

本轮 task 包级 audit short race 随机顺序通过（4.687s）。四个既有 fuzz 在普通模式、每项 20s 预算、2 workers 下通过：FuzzBoundedRunnerMixedOutcomes 276,141 次（21.283s）、FuzzTaskResultCache 488,690 次（21.189s）、FuzzTaskConcurrentCache 331,918 次（21.315s）、FuzzMuConcurrentHistory 921,942 次（21.257s）。

交叉模型覆盖 AsyncResult 多读取者、Wait/Values/WaitCh/WaitTimeout/WaitContext/Cancel/Ok/IsPanic 对同一业务结果的观察、Task 值拷贝共享缓存、业务错误与 panic 保真；Bounded 四入口在成功/业务错误/panic/预取消/并发取消下的回调至多一次、峰值许可及最终归还；Mu Append 工厂 panic、nil、重入、Snapshot 可修改副本与六操作并发历史。受控回调均最终退出；模型没有证明忽略 context 的用户回调可终止，也没有穷尽所有调度。

代码与 docs/task.md 对照未发现新的可复现缺陷或事实矛盾。AsyncResult.Cancel 只短暂等待并可能返回 context.Canceled，不取消业务；Task.Cancel 取消任务上下文但需业务协作；WaitTimeout/WaitContext 结束单次等待，后台结果仍可缓存。Bounded 获取许可与取消同时就绪时由 select 决定，文档已有对应边界说明。Task 专题文档未因本轮检查而修改。目标仍 active：其他 API 组合与方法级业务语义、Linux 实际执行、Windows 百万档 race 及独立复核尚未完成。

## Core 环形缓冲与日志契约交叉复核（2026-11-06）

internal/core audit short race 随机顺序通过（2.469s）。FuzzRingBufferStateTransitions 普通 20s/2 workers 通过 2,744,471 次，FuzzRingBufferConcurrentHistory 通过 1,584,717 次，FuzzTraceLogContract 通过 2,234,510 次（各约 21.3s/21.6s/21.3s）。环形缓冲模型与实现独立，检查 FIFO、Drop/Block/Error、回绕、FlushN 非正值、Reset 与丢弃计数；并发历史搜索线性化顺序，覆盖 Push/Pop/Peek/Flush/Reset/元数据查询。Trace 模型检查日志注入开关、现存 TraceID、空 ID 和 logger 上下文行为。

代码与生成的 core 方法参考对照未发现新的可复现缺陷或错误描述。并发修改默认环形缓冲的容量和溢出策略与创建 Pool 同时发生时，Pool 两次读取可以采到不同 setter 调用的字段；公开文档没有承诺两个字段的原子快照，且尚无消费者依赖该原子性或稳定失败证据，因此记录为未确认语义边界，不改实现也不声称其原子。Core 专题文档未因本批发现而修改。

## Retry、Shard 与 Slice 交叉语义复核（2026-11-06）

Retry 的 audit short race 随机顺序通过（4.874s）。四个既有普通 fuzz 各以 20s/2 workers 运行并通过：退避饱和 2,344,355 次、尝试次数/错误模型 255,982 次、限流预算 501,706 次、BindRetryToWorker 提交协议 2,001,472 次（实际约 23.3s/22.2s/21.6s/21.6s）。源码及专题文档对照确认：最终业务失败包装最后一次错误，TokenBucket/SlidingWindow 拒绝消耗尝试预算，许可等待受父 context 控制，BindRetryToWorker 只重试 ErrSubmitTimeout 且提交成功不代表业务执行完成；未发现新矛盾。

Shard audit short race 随机顺序通过（12.417s）。普通 FuzzShardRejectedRouting 319,432 次、FuzzShardBatchRouting 414,167 次、FuzzShardSharedContextLifecycle 195,736 次通过（各 20s/2 workers，约 21.8s/21.7s/21.6s）。模型检查 Pool/Group 路由差异、单片关闭/等待状态拒绝、Try 与普通提交计数差异、取消拒绝记录、批次错误首项及共享取消清理。模型不含并发配置修改或关闭竞争调度；这些由包测试与既有专门关闭模型补充。文档的已关闭分片不会自动改投、Try 状态拒绝不计数等说明与代码相符。

SliceOps audit short race 随机顺序通过（10.038s）。五个普通 fuzz 通过：混合变更状态 587,389 次、策略执行器 15,222 次、谓词策略 325,112 次、排序/搜索 115,744 次、自定义 Dedup 等价类 554,637 次（20s/2 workers；实际约 22.2s/22.0s/21.6s/22.8s/23.7s）。独立状态模型覆盖四种 builder/旧式入口的切片边界、插入/替换/范围删除/Clip/Grow/Repeat，策略执行成功/错误/panic 与每项调用，排序/搜索及自定义比较器 Dedup 的稳定代表元；没有发现新的可复现错误。高耗时谓词及排序输入会使单轮 fuzz 的执行次数下降，计数不等于覆盖所有输入。

本批没有新增运行代码修改或需要纠正的专题文档描述。此前一次错误拼写的 Shard fuzz 名称只完成普通包构建，不计模型覆盖；随后按源码中的 FuzzShardSharedContextLifecycle 名称实际执行并通过。全库仍有其他 API 组合及平台验证工作，目标保持 active。

## Group 生命周期与通知交叉复核（2026-11-06）

internal/group audit short race 随机顺序通过（47.969s）。FuzzGroupIndexedLifecycle 普通 20s/2 workers 通过 80,703 次；FuzzGroupConcurrentClose 通过 626,988 次；FuzzGroupAutoScaleDecisions 通过 1,091,725 次（实际约 22.9s/21.3s/21.7s）。另有 TestResultCallbackSubmissionBoundary 检查预取消提交通知、Wait 后拒绝追加但不发通知。

模型检查自有 worker/外部 Pool、稀疏索引、成功/业务错误/panic、重复 Wait、Reset 后第二轮、并发 Go/Wait/WaitContext/Close、取消、结果槽位与计数、AutoScale 归一化与决策。除独立关闭模型外，Group 模型没有并发配置或 Reset；不协作回调仍可延迟收尾。docs/group.md 对 ResultCallback 的同步/可能并发调用和最早状态拒绝不触发回调、提交错误需逐次检查、超时等待返回快照与后台清理等说明符合代码。未发现新的可复现缺陷或文档矛盾。

本轮未改 Group 运行代码或专题文档；MultiGroup/NoResult 的复用、合并错误和跨分片关闭仍需继续单独核查。目标保持 active。

## Pipeline 有限与常驻执行交叉复核（2026-11-06）

internal/pipeline audit short race 随机顺序通过（20.411s）。四个普通 fuzz 在 20s/2 workers 下通过：FuzzPipelineEngines 462,927 次、FuzzPipelineMetadataHistory 82,481 次、FuzzFlowLifecycle 440,497 次、FuzzFlowConcurrentSubmitClose 765,241 次（实际约 21.3s/21.3s/21.4s/21.1s）。

独立逐项模型交叉比较有限输入 Execute/ExecuteStream/Pipeline native 三引擎的阶段值/错误/panic 覆盖语义；builder、Serial、Stream、元数据与结果回调观察路径；常驻 Flow 的阶段错误计数、Close 与 Results 并发取结果、提交/关闭竞态、取消时可丢未完成结果、提交序号唯一性、缓冲最终清空和统计一致。模型不穷举所有生产者/消费者调度；另有 4200 项背压取消契约 race、三段专题原文受控测试作为定向证据。docs/pipeline.md 对 SkipOnError 的入口差异、Occupied、上下文职责、关闭双路结果消费、慢消费者阻塞及取消可丢结果与当前源码一致，未发现新矛盾。

本批未更改 Pipeline 运行实现或专题文档。补充核对 MultiGroup、NoResult 与 MultiPool 源码及对应 Group/Pool 文档：Values 沿用 Group 的 Occupied+Err==nil 筛选；NoResult 等待不返回结果但等待结果槽仍按长度统计；MultiPool 仅一个并发 Wait 可取得结果，WaitAndClose 即使未取得结果也会关闭各分片。已有文档已说明这些边界；未覆盖新的并发压力模型，因此不据此扩张动态测试结论。其他跨组件 API 组合仍继续审查，目标保持 active。

## 分片混合许可归属与文档证据复核（2026-11-06）

新增 FuzzShardedMixedPermitOwnership，混合外层 Acquire、Token 句柄和 GetShard 直接获取，三套独立持有计数逐步核对每片余额。外层 Release 可选择任一尚有外层许可的分片，模型观察归还结果而不复制选择算法；Token 重复释放不重复归还，直接获取只由直接 Release 配对。模型包含拒绝、额外外层释放及仅持有句柄时调用外层 Release。使用 1～4 片、1～16 总额度、最多 64 项顺序动作、Reject 策略和一小时补充窗口隔离实际 tick；不覆盖并发 Resize、自动补充或全部并发调度。

最初三个种子 race 随机二十轮通过（2.216s），普通 30s/2 workers fuzz 2,200,604 次通过（31.266s）。临时 Go overlay 的“漏记外层许可”被识别，但“把 Token 登记为外层许可”首次没有被三个种子识别；追加仅持有 Token 后调用外层 Release 的第四个种子，再运行三个变体（漏记、句柄误登记、句柄立即释放），均被预期归属或余额断言识别。最终四种子 race 随机二十轮通过（4.248s），30s/2 workers race fuzz 187,007 次通过（35.336s）。没有把首次变体漏检算为成功，也没有据此归为运行库缺陷。

源码和专题文档复核未发现新增可复现的限流运行缺陷。限流文档仍有“CPU 开销大幅降低”的无条件性能表述，已改为每 100ms 按累计时间补充的实现机制及需在目标负载测量的建议；未改运行实现。报告同时纠正 Retry 首个 fuzz 时间、排序稳定性覆盖措辞、两处新锚点，并补齐 Group/Pipeline 既有实测。首次局部片段检查漏设命令级 GOROOT，报 no such tool vet；用既定工具链环境重跑已通过，不将环境配置失败计作文档缺陷。

文档生成与 1388 声明调用/3 完整程序、80 原文局部片段编译、1415 条四项说明结构、368 围栏语法、255 本地锚点与 go vet 均通过。上述文档检查早于最后性能措辞修正，最后版另行核验；运行差异仍限于既有五项修复。全库逐方法业务语义与跨组件组合尚未全部完成，平台和独立复核缺口继续保留，目标保持 active。

最终四种子加入后的 ratelimit/docs/文档工具 short audit race 随机一轮通过（8.356s/8.730s/4.721s）。最后限流性能措辞及报告版本的生成一致性、1388 声明调用/3 完整程序、80 原文局部片段编译、257 本地锚点（零未匹配）、go vet 和 git diff --check 通过。本批全部启动命令均已退出；没有待完成的测试会话。结构和围栏检查沿用本批较早结果，期间 API 文档及围栏内容未改。

按 requesting-code-review 技能再次尝试独立只读复核，工具仍在参数解析阶段报 missing field message，没有启动审查，不能计为独立结论。随后自行复核五项修复中的 Pool/ForEachPool/Shard 差异：业务发送与两条关闭路径共用 taskSendMu；退出信号发送由 resizeMu/closed 约束；completion 在 finishTask 前同步完成且首错汇总有锁；共享取消在整体等待/关闭后释放。没有发现新的可复现缺陷，自行核对不替代独立复核或此前未完成的平台运行。

## 稳定排序代表值与查询方向复核（2026-11-06）

新增 FuzzSliceStableRepresentativesAndQueries，使用带原始序号的记录和独立的八键桶模型，对照 SliceBuilder、SerialSlice、ParallelSlice 与旧 Slice 四个入口。最多 256 项，包含空输入、相等键、升降序、缺失查询；检查稳定排序的相等键顺序、Max/Min 相等极值保留原输入首个代表、Find/FindLast 的结果和谓词扫描顺序/停止位置、Index 的结果和谓词调用数。此前整数排序模型无法区分相等键重排，因此本批补充这项覆盖，不把此前结果追溯解释为稳定性证据。

三种子 race 随机二十轮通过（2.927s）；30s/2 workers race fuzz 15,101 次通过（34.083s），另一次普通 fuzz 481,765 次通过（33.082s）。六个临时 Go overlay 变体分别破坏新旧稳定排序、把最大值相等时更新为后一个代表、将 FindLast 改为从头扫描，均被预期顺序、最大值或扫描断言识别，且没有以编译错误冒充识别。临时变体不写入库运行源码。

源码 Shuffle 示例错误声称“每次执行结果不同”，已改为可能与原顺序或上次结果相同，并同步专题表格、生成规则和 API 参考。库运行实现没有改变，累计运行修复仍为五项。最后 sliceops/docs/文档工具 short audit race 随机一轮通过（13.302s/8.305s/4.594s），生成一致性、1388 声明调用与 3 完整程序编译、80 原文局部片段、1415 条四项结构、368 围栏语法及 go vet 通过。此模型不测试任意有缺陷比较器、数据操作的并发修改或全部排序输入；有限 fuzz 不是全库无缺陷证明。逐方法剩余业务语义、平台实跑与独立复核继续开放，目标保持 active。

报告追加后 257 本地 Markdown 锚点零未匹配，git diff --check 通过。再次尝试只读独立审查，仍在工具参数解析阶段报 missing field message，没有启动审查；未把失败调用计为独立证据。本批测试会话均已退出。

## 分块视图与拆分引用归属复核（2026-11-06）

新增 FuzzSliceChunkAndPartitionOwnership，以最多 64 项、带前后填充的偏移视图，对照独立函数、三个构建器和旧 Slice 共五入口。分块 oracle 逐项组装索引，另用商和余数计算 ChunkN 块长，不复用生产的 ceil 表达式；覆盖零/负数、机器整数两端、期望六块实际五块和空输入。核对块数、各块长度、剩余容量、元素槽位地址及源输入不变，再检查限制容量后的 append 分配隔离、未限制 append 写入源数组及已有槽位共享。Split/Partition 检查顺序谓词、两组顺序、空组 nil、元素槽位独立和指针内容共享。

六种子 race 随机二十轮通过（4.631s），30s/2 workers race fuzz 140,712 次通过（35.482s），普通 fuzz 1,601,050 次通过（31.974s）。临时 Go overlay 的限制块容量、复制块数组、ChunkN 向下取整、反转拆分条件及复用输入槽位五种变体均被预期断言识别。最初复制变体由容量断言识别，随后改为保留完全相同容量的复制变体，重跑五项仍全部识别，复制变体此时由元素槽位归属断言识别。变体不是库运行修复。

源码摘要“按大小均分”改为明确末块可不足大小，并说明共享数组和追加覆盖；旧 Split 的“两个独立切片”补充槽位独立与引用内容共享。专题新增 ChunkedN 的十项/六块反例和 Split 所有权说明，API 文档同步生成。未发现新增可复现运行缺陷，运行逻辑仍仅既有五项修复。最后 sliceops/docs/工具 short audit race 随机一轮通过（9.653s/5.266s/4.162s）；生成一致性、1388 声明调用/3 完整程序编译、80 局部片段编译、1415 四项结构、368 围栏语法、go vet 和运行差异检查通过。模型是有限顺序数据操作，不包含并发写入、任意对象图深拷贝或全部参数组合；全库逐方法剩余语义、平台执行和独立复核仍开放，目标保持 active。

最终报告链接检查为 257 本地锚点、零未匹配，git diff --check 通过；全部本批测试命令均已退出，无待运行会话。没有重复尝试此前已确认无法解析参数的独立审查调用，独立复核缺口仍保留。

## 结果投影与失败输入保存复核（2026-11-06）

新增 FuzzSliceResultProjections，以最多 64 个槽位独立组合指针值/空指针、Occupied、四种错误（nil、两种业务错误、包含 nil 指针的非 nil error 接口）及顶层摘要标记。oracle 从输入编码独立构造成功值、全部值和错误序列，核对 SliceResult 的 Error/Ok/Err、Errors 顺序与重复、Values/First、Len、Unwrap 部分值、Must 的 panic 错误身份，并交叉验证 Flat/ResultValues/ResultErrors/OnlyErrors/PartitionResults/Every/Some/AnyError。附加检查返回投影的槽位复制、Results/FailValues 的槽位共享和修改 Results 后保存首错/失败输入不重算。内部构造的状态包含公开构建器不一定自然产生的组合，不据此扩大运行引擎状态覆盖。

新增 TestSliceResultFailureInputsStaySaved，三个构建器 Map 和两个构建器 MapBatch 实际执行：两项失败、同一错误重复、批大小 -1/0/1/2/3/MaxInt，逐组核对失败输入和错误数量，清除结果槽位错误后核对失败输入仍保留。首版三个模型种子 race 随机二十轮通过（2.804s）；加入真实构建器定向测试后合并 race 随机二十轮通过（4.449s）。30s/2 workers race fuzz 163,629 次通过（36.334s），普通 fuzz 2,317,510 次通过（32.506s）。

五个临时 Go overlay 错误变体（错误加入 Occupied 筛选、重复添加摘要、破坏顶层错误优先级、Results 改为复制、失败批次仅保存首项）均由预期投影/所有权/失败输入断言识别，不是编译错误。文档 Errors 表格补齐顺序与重复规则，说明 Results 可变而 FailValues 和顶层错误单独保存、批次失败输入不等于逐元素失败以及 typed nil error 规则；源码注释和 API 注意事项同步。没有新增运行缺陷，未改运行实现，累计修复仍五项。

最终 sliceops/docs/工具 short audit race 随机一轮通过（10.740s/7.462s/3.688s）；生成一致性、1388 声明调用/3 完整程序、80 局部片段编译、1415 四项结构、368 围栏语法、go vet 和运行差异检查通过。本模型只检验同步结果查询，不能替代全部终端策略、取消/截断组合或数据竞争验证；全部剩余逐方法业务语义、Linux 实际运行、Windows 百万 race 和独立复核缺口保留，目标保持 active。

报告最终版 257 本地锚点零未匹配，git diff --check 通过；本批测试与变体命令均已退出，无运行中的测试会话。独立审查工具此前持续参数解析失败，本批未重复无效调用。

## Slice Runner 池所有权终端矩阵复核（2026-11-06）

检查发现池生命周期不能概括为“所有池化操作完成后都 ResetWait”。execMap 在输入为空时先于池化分派返回：借用池不等待/复位；类型匹配且接管的池仍在空输入分支关闭。execMapPool 对非空映射等待后复位借用池，接管池关闭。Runner.Each 即使空输入仍进入 execEachPool 并等待/复位；EachBatch 非正块大小改为 1 后仍经过该路径。streamPool 也为零输入启动收尾 goroutine、等待并复位借用池，接管池在输出关闭时关闭。Runner.Fold 同步调用 Reduce，不使用或复位借用池；若池为接管所有权则 defer Close，包括错误或 panic 展开。SerialSlice.Reduce 不经 Runner，不持有池生命周期责任；旧 Slice 的无值 ForEach/Each/Batch 入口实现另有路径。

新增 TestRunnerPoolOwnershipByTerminal，六种 Runner 终端 × 空/非空输入 × 借用/接管共 24 组组合。先向池提交已完成的无关 sentinel，以保留 TotalCount 观察复位；检查业务结果/统计、池历史、复用能力和关闭错误。race 随机二十轮通过（2.466s），普通测试随机一百轮通过（1.228s）。六个临时 Go overlay 变体分别删除 Map 借用池 ResetWait、Map 接管池关闭、令空 Map 进入池路径、删除空 Map 接管关闭、删除 Stream ResetWait、删除 Fold 接管关闭，均由生命周期后置条件识别，没有把编译失败算作发现。

mapreduce.md 原有池化章节将 ResetWait 扩称所有池化路径，现新增按终端与空输入的借用/接管矩阵，补充 Fold、SerialSlice 和旧 Slice 边界。API 注释针对 Map、MapBatch、Each/EachBatch、Stream 和 Fold 同步细化。没有发现新增运行缺陷或改变生命周期实现，累计五项修复保持不变。重新生成 API 文档，1388 声明调用及 3 完整程序编译通过；最终 sliceops/docs/工具 short audit race 随机一轮通过（9.500s/5.464s/2.249s）；1415 四项方法结构、368 围栏语法、go vet 和运行差异检查通过。本节末尾的 257 锚点零未匹配及 git diff --check 通过。

## 预取消父上下文调度复核（2026-11-06）

加入公开示例后的全部 57 Examples 再次 race 随机五轮通过（docs 2.467s、retry 2.808s、sliceops 2.508s）；最终报告 258 本地锚点零未匹配、git diff --check 通过。全部本轮测试/变体命令均已退出，没有待完成测试会话。

最终追加公开 Example_slicePreCanceledDispatch，以真实构建器验证普通 Map 调用四次、ForEach 不调用、单批 ForEachBatch 调用一次，并在业务回调返回 ctx.Err()；该示例 race 随机二十轮通过（2.340s），累计 57 Examples。最终逐元素/批量矩阵普通随机一百轮通过（3.219s），sliceops/docs/工具 short audit race 通过（11.760s/7.482s/4.327s，早于该公开示例）。1415 四项方法结构、368 围栏语法、go vet、258 本地锚点零未匹配和 git diff --check 通过，AST 运行差异仍仅五项既有修复。逐方法剩余语义、Linux 实际运行、Windows 百万 race 与独立复核缺口未关闭，目标保持 active。

后续交叉核对发现专题首版“普通并行路径由 Group 拒绝”的表述范围过宽，只有 Each 使用该拒绝路径；未启用 FailFast 的普通并行 Map 仍调用全部回调。现以 Map/Each 对照表替代含混段落，分别说明串行、普通并行、FailFast、分片/超时/池及 Chunk 路径。补充 Map 回调收到的 context.Err 断言，临时添加的“所有结果均 Occupied”断言在普通 Map 失败；源码确认该路径保存 Value/Err 而不设置 Occupied，已删除错误假设，不将其计为库缺陷。此阶段逐元素矩阵 race 随机 50 轮通过（2.776s）。五个临时 Go overlay 变体分别丢失普通并行/串行 Map 的 context、强制 Each 串行回退、丢失 Chunked Each 的 context、跳过 FailFast Map 的取消预检查，均被回调 context 或次数断言识别，未把编译失败算作识别。

新增 TestCanceledParentBatchDispatchByTerminalPath，以四项输入检查 12 种批处理组合，包括非正 Chunk 回退、块大小 1/2/3/4、Seq+Chunk、FailFast、超时和分片。核对每批值/错误、回调收到的取消、回调次数及按批统计。批次仍携带原策略调度，有效并行度按批次数裁剪，单批 EachBatch 可串行回退；逐元素与批量合并 race 随机 50 轮通过（5.108s）。专题及生成 API 的 Map/批处理注意事项已同步，运行实现未改，累计修复仍五项。

本轮全库 short audit race 随机一轮完整通过：根包 645.098s，core 3.099s、group 49.742s、maps 3.306s、pipeline 21.520s、pool 23.429s、ratelimit 6.194s、retry 4.455s、shard 13.252s、sliceops 11.078s、task 5.311s、docs 6.704s、文档工具 2.234s。该全库测试二进制早于随后追加的批量矩阵，不能追溯覆盖新增测试；批量有上述单独 race 证据。最终生成一致性及 1388 声明调用/3 完整程序编译、80 原文局部片段编译通过。下段保留首阶段经过，后续结果以上述记录为准。

新增 TestCanceledParentDispatchByTerminalPath，使用调用前已取消的 context，对照串行、并行度 1/2、分片、正数超时、Chunk 长度 1/2/3、FailFast 及池化 Map/Each；验证回调次数、回调收到的 context.Err、结果槽、Total/FailCount 和汇总错误。测试先将 Chunked Each 的单元素块误判为 Group 拒绝（预期 0，实测 4），继而按 execEach -> eachPar 的并行度裁剪与串行回退更正 oracle；又发现 Chunk+FailFast 的 FailFast 路径优先于 Chunk，因此四项输入、Chunk(3).FF 的回调数是 0，而普通 Chunk(3) 为 1。该差异由实际分支推导并在 race/普通测试中重复验证，不是运行缺陷。race 随机 50 轮通过（5.704s），普通随机 100 轮通过（3.251s）。mapreduce.md 增加预取消时按调度路径区分的回调契约和 context 检查建议；生成 API 的 Each 注意事项同步说明并行度为 1 与单元素块的回退。API 生成检查及 1388 声明调用/3 完整程序编译、go vet、diff check 通过。

## 空切片重复的活性修复（2026-11-06）

本轮从切片变更模型的限制反查 Repeat：此前 fuzz 将重复次数限定为小值，未覆盖空输入配极大次数。TestRepeatEmptyLargeCount 先以四个独立子进程分别执行 SliceBuilder、SerialSlice、ParallelSlice 和旧 Slice，Repeat(MaxInt) 均触发五秒期限失败（合计测试 22.742s）。每个进程可被独立终止，避免在测试进程泄漏无限忙循环；失败来自未完成，非编译或断言语法错误。

根因是两份 Repeat 对空输入仍复制空槽位并执行 n-1 次空追加。现仅在原 n<=0 分支之后，将 n==1 返回条件扩为 n==1 或 len(items)==0，非空路径保持原实现。修复后空输入 nil/已分配/偏移视图共三形态，四入口检查长度、容量、nilness、底层槽位地址与源数组不变，race 随机三轮通过（14.360s）。TestRepeatElementValuesAndOwnership 另对四入口、-1/0/1/2/3 次、容量 2/8 的 40 组检查值、容量充足时复用、扩容、尾部覆盖、指针内容共享与槽位独立赋值，race 随机五十轮通过（4.062s）。

原混合变更模型普通 30s/2 workers fuzz 1,020,406 次通过（34.779s），race fuzz 123,262 次通过（34.790s）；模型仍有限，不验证所有分配规模或并发共享修改。SliceOps 非 short race 随机一轮通过（46.249s），根包原 TestSlice_Chain_Repeat 非 short race 随机三轮通过（4.266s）。六项修复版本全库 short audit race 仍在原会话运行，未完成前不计通过。源码注释、专题和生成 API 注意事项已说明空输入直接返回及非空引用/内存边界；1388 声明调用/3 完整程序编译与 go vet 通过。

按 requesting-code-review 技能调用独立审查，工具仍在参数解析报 missing field message，没有启动 reviewer，不能计为独立复核。变异反证和最终文档/全库结果随后补充；Linux 实际执行、Windows 百万 race 与逐方法剩余语义缺口保留，目标 active。
本阶段四个临时 Go overlay 变体均被预期断言识别：删除 sliceBase/旧 Slice 的空输入保护由子进程超时识别，两处循环各多追加一次由长度断言识别，没有以编译失败冒充成功。源码 AST 差异确认新增运行变化仅为两份 Repeat 的返回条件。最终 80 原文局部片段、1415 四项方法结构、368 围栏语法、258 本地锚点与 git diff --check 通过。

六项修复全库命令 go test -tags audit -short -race ./... -count=1 -shuffle=on -timeout=30m 已退出 0：根包 648.330s、docs 7.047s、工具 3.137s、core 3.125s、group 48.160s、maps 3.207s、pipeline 21.082s、pool 21.953s、ratelimit 6.574s、retry 4.627s、shard 13.559s、sliceops 15.102s、task 6.001s。该二进制早于后补的非空归属测试/公开示例，后补测试有上述独立 race 证据。公开 Example_sliceRepeatEmptyAndReferences race 二十轮通过（5.266s），全部 58 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 4.490s/retry 8.247s/sliceops 4.704s）。此前根包 5132.560s 非 short 无 race 属于五项版本；第六项另有非 short SliceOps 全包及根包 Repeat 定向 race，不将旧长测冒充新版本全库非 short。
最终后补测试/示例纳入后的 sliceops/docs/工具 short audit race 随机一轮通过（15.468s/7.216s/2.842s），最终报告 258 本地锚点零未匹配及 git diff --check 通过。全部本轮测试和变异命令均已退出，无活动测试会话。目标保持 active；独立审查解析失败、Linux 实际执行、Windows 百万 race 和逐方法剩余语义缺口未关闭。

## 重叠参数变更与共享槽位复核（2026-11-06）

新增 FuzzSliceOverlappingMutation，用调用前的输入/参数快照逐元素重建 oracle，不调用 slices.Insert/Replace 生成预期值。最多 32 个输入元素，接收者是带前后填充数组的偏移视图，参数可取该数组的任意连续区间；分别限制/保留容量，检查 Append/Prepend/Insert/Replace 的值顺序、原数组写入、目标数组槽位地址、容量不足时分配、Replace 缩短尾部清零和非法负终点 panic 前无修改。四入口为 SliceBuilder、SerialSlice、ParallelSlice、旧 Slice；索引包含负数、反向区间及机器整数两端。模型只做顺序、整数槽位变更，不覆盖并发修改、任意对象图深拷贝或所有分配规模。

首版七种子 race 随机二十轮通过（4.033s），普通 30s/2 workers fuzz 2,001,151 次通过（33.711s），race fuzz 148,911 次通过（35.972s）。加入反向区间及清空区间后九种子 race 二十轮通过（7.277s），最终 race fuzz 145,104 次通过（35.176s）。首次编译因测试遗留未使用变量失败，修正后才有以上有效证据，不按运行缺陷统计。

六个临时 overlay 变体分别令新旧 Prepend 复制参数数组、新旧 Insert 用不安全嵌套追加、Replace 不清零缩短尾部、Replace 将负终点静默修正。最终均由目标数组归属、值顺序、共享数组内容或 panic 断言识别，未以编译失败冒充识别。首个 Prepend 变体实际由地址归属识别，脚本原预期共享内容诊断而拒绝计成功；修正脚本诊断后重跑六项均通过。替换变体通过保存参数/尾部快照，隔离掉值重叠问题，单独验证清零副作用。

源码摘要、mapreduce.md 与生成 API 注意事项补齐 Prepend 采用参数数组、空参数视图容量仍可被复用，以及 Replace 缩短清零对其他共享视图可见的区别。新增公开 Example_sliceMutationSharedArrays 展示实值与源数组变化；首次 Output 与普通注释同块导致 Go 不识别，命令报告 no tests to run，未计为通过。分离注释后示例 race 随机二十轮实际通过（2.930s），全部 59 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 3.006s/retry 3.317s/sliceops 5.016s）。

最终 sliceops/docs/文档工具 short audit race 通过（14.765s/7.060s/4.212s）；生成一致性、1388 声明调用/3 完整程序编译、80 原文局部片段、1415 四项说明结构、368 围栏语法与 go vet 通过。本轮只新增测试、说明与源码注释，运行差异仍为既有六项修复；此前六项版本全库 short race 根包 648.330s 的证据保留，后补测试有上述包级验证。Linux 实际运行、Windows 百万 race、独立复核及逐方法剩余业务语义缺口未关闭，目标 active。
最终 go test -list 确认 59 个可执行 Examples（docs 52/retry 3/sliceops 4），258 本地锚点零未匹配，git diff --check 通过；去注释 AST 仍只显示六项运行修复。本批全部测试与变体会话均已退出，没有运行中测试。独立审查接口此前持续参数解析失败，本批未重复无效调用，仍无独立复核结论。

## Retry 业务退避与单次期限复核（2026-11-06）

新增 internal/retry/business_timing_contract_test.go，使用 testing/synctest 推进实际定时器，与写死的预期时间表比较，不调用 computeBackoff 生成预期。TestRetryBusinessBackoffSchedule 覆盖十种函数式/Result/Void/链式入口、六组初始/上限参数及成功/耗尽两条路径，共 120 组；回调每次耗时 250ms，核对开始时间、最终值/错误和返回时刻，区分回调耗时与退避，验证成功及最后失败后无额外等待。

TestRetryPerCallContextLifetime 的 18 组验证正/零/负超时、普通错误/panic、每次独立 context、下次调用前及结束时子 context 已取消、父仍有效，以及 RetryWithConfig/Void 只读取首个 TimeoutOpt。TestRetryPerCallDeadlineSchedule 的 6 组验证 2s 单次超时加 1s 起始指数退避，调用时间为 0/3/7s、最终 9s；5s 父期限时调用只有 0/3s，整体 5s 返回。TestRetryExtremeBackoffCancellation 的 6 组使用 MaxInt 重试预算及 MaxInt64 时长，首个极大退避在 1s 父取消后返回；不穷举极大次数，也不把测试中的 MaxInt 当作生产推荐。四项共 150 组最终 race 随机二十轮通过（7.438s）。

TestRetryLimiterBackoffTimeline 的 8 组让虚拟经过时间实际驱动 TokenBucket 补充或 SlidingWindow 过期，覆盖普通/两片、指数/固定退避，分别验证回调时间、拒绝占用尝试和最终错误来源。初始 400ms/上限 1200ms、五次许可检查；不分片固定模式仅 0/1.2s 调用，1.6s 最后拒绝；指数模式 0/1.2/2.4/3.6s 调用，最终包装业务错误。分片预期按轮询和每片额度另列。race 随机二十轮通过（3.872s）。该模型不覆盖浮点所有速率、随机抖动、并发修改构建器或所有调度。

临时 Go overlay 共 11 变体被预期行为断言识别，未把编译失败算作识别：函数式/配置/链式退避次数偏移，链式线性误用 max，两条路径不取消子 context，读取末个 TimeoutOpt，耗尽后多等一秒，以及限流拒绝后不退避/退避重置为首段/拒绝仍执行业务。脚本和各项日志保存在 .gotmp/check_business_timing_mutants.py、.gotmp/check_retry_limiter_timing_mutants.py 及各自目录。

docs/retry.md 将“线性退避间隔均匀”改为失败后固定等待，加入回调耗时/最后不等待/超时范围和限流拒绝两张时间表，说明真实调度及许可等待另计。同步生成方法注意事项，并修正 acquire 私有注释中错误的“内部自动退避”归属。本批运行实现未改，去注释 AST 仍仅六项既有修复；未发现新的运行缺陷。

最终 retry/docs/文档工具 short audit race 通过（4.970s/6.625s/4.906s）；生成一致性、1388 声明调用及 3 完整程序编译、80 原文局部片段编译、1415 方法四项结构、368 围栏语法、go vet 和差异空白检查通过。已有六项修复全库 short race 根包 648.330s 证据保留，本批新增测试以上述包级结果为准。59 Examples 数量不变，无新增公开 Example。逐方法剩余语义、Linux 实际执行、Windows 百万档 race 和独立复核仍未闭合，目标保持 active。

## Map 非自反键条目变更修复（2026-11-09）

从比较器与集合边界反查 Go map 相等性，发现 NaN 以及包含 NaN 的复数、数组、结构体、接口键虽然满足 comparable，却可能 key!=key。原 Map/MapBase.Filter 用 delete(data,key) 不能删除遍历到的这类条目，MapValues 用 data[key]=newValue 新增条目而非更新，并可能再次遍历到新增条目。独立 Map、默认链、Serial、Parallel 四入口的 Filter/Reject 八组修复前均保留错误值 [1 2 3 10]，预期仅 [10]；MapValues 修复前触发“newly inserted NaN entry visited”调用上限，完整失败日志 .gotmp/nonreflexive-before.log。最初回归使用错误的构造泛型参数数量导致编译失败，修正测试后才取得以上运行失败证据，没有把编译失败算作缺陷复现。

新增 entry_mutation.go 共用处理：普通键继续即时原地更新/删除；非自反条目暂存结果，过滤确实删除此类条目或发生值变换时，收集剩余普通条目并 clear 原 map 后重建。没有换成新 map，FromRef/AsMap 及链式共享引用保持有效；没有重新定义 NaN 相等性，Get/Delete/GetOrSet 等按键方法仍遵循 Go 规则。普通键不分配条目临时切片，非自反路径最坏额外 O(n) 空间；回调不能自行修改容器，不保证 panic 后回滚。

TestNonReflexiveMapEntryTransforms 四入口共 12 组回归 race 二十轮通过（3.322s），后与 FuzzNonReflexiveMapTransforms 种子及 TestMapEntryMutationPreservesOrdinaryProgressAndPanic 合并 race 二十轮通过（3.164s）。后者逐次读取原 map，检查普通键先前的更新/删除在下一回调前已可见，第三次 panic 原样传播，已完成修改保留。模型最多 64 项，以整数值标识条目，用独立值列表 oracle 检查 Filter/FilterKeys/FilterValues/Reject/MapValues 五类操作，覆盖同一 NaN 位模式的多个不同条目及六种动态键，验证条目数、回调数、结果、共享 map Clear；不依赖按 NaN 查询或 map 遍历顺序。

普通 fuzz 391,148 次（33.033s）、race fuzz 180,753 次（41.478s）通过。追加输出键类型和浮点位模式校验后，种子 race 二十轮通过（4.248s），最终 race fuzz 104,957 次（35.100s）通过。后者验证键未规范化、NaN 位模式与对应值关系保留；这些有限模型不覆盖所有键类型、并发写入或回调自行修改容器。六个 overlay 变体分别恢复 Map/MapBase 的旧过滤和值变换、重建时换 map 丢失别名、重建保留原 NaN 条目，全部由条目/回调断言识别，脚本 .gotmp/check_nonreflexive_mutants.py。

专题与生成方法说明补齐非自反条目、Go 按键查询边界、临时存储/重建和生产键校验建议；新增公开 Example_mapNaNEntryMutation，演示相同 NaN 两个条目、Get 不命中、变换不增加条目、按值过滤及 Clear 别名可见。示例 race 二十轮通过（2.868s），全部 60 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 3.417s/retry 2.129s/sliceops 2.633s），Maps 非 short race 全包通过（9.634s）。

七项修复全库命令 go test -race -tags audit -short ./... -count=1 -shuffle=on -timeout=30m 已退出 0：根包 648.850s、docs 10.637s、工具 3.421s、core 7.204s、group 47.865s、maps 5.112s、pipeline 23.149s、pool 22.595s、ratelimit 7.230s、retry 4.451s、shard 13.872s、sliceops 16.354s、task 7.343s，日志 .gotmp/audit-seven-fixes-short-race.log。该二进制早于后补的普通键即时修改/panic、键位模式及公开示例，后补测试以以上包级证据为准。工具返回跨环境暂停后的极长 wall_time 不当成 fuzz 执行时长，以 Go 输出记录的有效时长为准。

独立审查技能要求派发 reviewer，本阶段调用仍在参数解析报 missing field message，未启动 reviewer，不能计为复核。WSL 普通调用受访问限制，提权只读确认仅有 docker-desktop；Docker 引擎沙箱外只读检查仍报告管道不存在，因此尚未运行 Linux，未启动或安装服务。Windows 百万档 race、Linux 实际执行、独立复核及逐方法剩余语义仍待完成，目标 active。

最终补验 Maps/docs/工具 short audit race 通过（6.099s/6.026s/2.559s）；go test -list 确认 60 Examples（docs 53/retry 3/sliceops 4），生成一致性、1415 方法结构、263 本地锚点零未匹配、go vet 与 git diff --check 通过。此前同批 1388 声明调用/3 完整程序及 80 局部片段编译、368 围栏语法也通过。运行差异核对脚本补纳入未跟踪 internal 非测试源码，列出新 entry_mutation.go 的三个 helper 和私有条目类型，避免漏看新增实现；其余差异符合既有六项加本次第七项修复。全部测试、fuzz 和变体会话已终止，无运行中测试。

## Map 别名合并的非自反键重复复制修复（2026-11-09）

继续从第七项修复反查批量写入，确认源和目标共享同一底层 map 时，SetAll/Merge/MergeMap/MergeWithDefault 会在 range 期间插入 NaN 键新条目，后续可能再次访问并复制这些新条目。TestMapAliasedMergeCopiesEachSourceEntryOnce 覆盖独立 Map、默认链、Serial、Parallel 四入口和四种操作，共 16 组；输入 64 个同位模式 NaN 条目及一个普通键，自合并预期 129 项，修复前首轮实际 154～176 项，第二轮仍失败且数量变化，日志 .gotmp/merge-alias-before.log。

共用 copyMapEntries 保留普通键即时写入路径，仅暂存源非自反条目，源遍历结束后逐项插入一次。SetAll/Merge/MergeWithDefault 的 Map 与 MapBase 六个实现委托此 helper，MergeMap 沿用原委托。没有用反射判断 map 身份，没有让自合并静默无操作，也没有把 NaN 定义为相等；自合并恰好新增每个源 NaN 条目一次。普通键覆盖和默认值保护、返回接收者、浅拷贝与底层 map 别名不变，普通键不分配 pending 切片，源中 k 个非自反条目需要 O(k) 额外空间。nil 目标加非空源仍 panic，nil/空源仍无操作，新增兼容性回归覆盖这三种情况。

FuzzMapMergeSourceSnapshot 用普通键独立槽位模型和非自反条目列表校验四操作、同源/独立源、源不变、接收者返回、别名结果及逐源条目次数；输入最多 64 项。最终加入键类型/位模式签名，防止只检查值时漏掉键被替换。初版普通 fuzz 1,357,447 次（33.270s）通过；新增普通键碰撞种子后的 race fuzz 137,138 次（35.517s）通过；最终包含键签名的 race fuzz 64,347 次（36.614s）通过。测试不覆盖并发写同一 map，生产使用必须自行同步。

八个临时 overlay 变体分别恢复 Map/MapBase 的 SetAll/Merge/MergeWithDefault 旧循环六项、跳过暂存条目、覆盖默认值。首次最后一项变体存活，暴露种子缺少独立源普通键碰撞；添加第四种子 {0,3,6,9},false 后全部八项均被行为断言识别，未把编译失败算作识别。脚本 .gotmp/check_merge_alias_mutants.py。最终回归、nil/空兼容、四种子及集合/转换契约合并 race 二十轮通过（4.929s）。

新增 TestNonReflexiveMapSetAndConversionContracts 验证 NaN 的自/克隆交集只含普通相等键，差集保留 NaN，独立结果的并集复制双方 NaN；Clear 结果不改变输入。RemapValues 保留 NaN 位模式，RemapKeys/Invert 的 NaN 目标不合并条目。这些是 Go 相等性契约，无新增运行修复。专题和生成方法说明同步补充相关边界。新增公开 Example_mapAliasedMerge，示范自合并条目数及默认值保留；race 二十轮通过（4.333s）。全部 61 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 4.448s/retry 3.652s/sliceops 4.985s）。

Maps 非 short race 全包通过（12.140s），早于后补的键签名和集合契约测试，后补以上述定向证据及最终 short 包级结果为准。最终 Maps/docs/文档工具 short audit race 已退出 0（3.881s/8.147s/4.901s）；go vet、包含未跟踪源码的运行 AST 差异核对及 80 原文局部片段编译通过。八项修复的全库 short audit race 原会话 9289 已退出 0，详见下文终态；未重启替换该次运行。

独立审查派发仍在参数解析阶段报 missing field message，未启动 reviewer。Windows 百万档 race 的 TSan 内存分配失败、Linux 实际执行、独立复核及逐方法剩余业务语义缺口保留；目标保持 active。

最终全库命令 go test -race -tags audit -short ./... -count=1 -shuffle=on -timeout=30m 退出 0：根包 661.851s、docs 5.935s、工具 3.325s、core 5.631s、group 49.850s、maps 7.057s、pipeline 23.568s、pool 22.588s、ratelimit 6.372s、retry 6.433s、shard 13.967s、sliceops 16.017s、task 6.251s，日志 .gotmp/audit-eight-fixes-short-race.log。该二进制早于后补 nil/空兼容、键签名、集合转换及公开示例；后补有上述定向、fuzz、Example 与包级证据，不能称全部后补已在该次全库二进制中执行。go test -list 确认 61 Examples（docs 54/retry 3/sliceops 4）；1415 四项结构、368 围栏语法及 262 本地锚点零未匹配通过。

继续沿 MapRunner/MapResult 反查非自反键：新增 TestNonReflexiveMapRunnerEntryContracts，四种 Seq/Par/分片/默认池策略与 Map/ForEach/Stream/Filter/Reduce/MapBatch/ForEachBatch 七类终端共 28 组。输入含同位模式重复 NaN、float32、复数、数组、结构体键，以唯一值和键签名检查回调逐条精确一次、转换后的原键、流式结果、过滤/聚合和批次首键规则，避免用无法命中的 NaN lookup 构建预期。race 随机二十轮通过（3.847s），未发现新增运行缺陷。两个 overlay 变体将键值快照改为重新按键查询、跳过非自反键，均由条目或访问断言识别，脚本 .gotmp/check_nonreflexive_runner_mutants.py。模型不覆盖业务失败、取消调度或并发修改输入，既有整数模型覆盖部分错误路径，不能互相替代。

MapResult 注意事项与专题补充 Keys 中 NaN 也无法通过 Data()[key] 回查，关联读取应 range Data 或在流式结果携带业务标识；输入需要稳定查询时由业务显式转换并处理碰撞。本批不改运行实现。新增矩阵后的 Maps/docs/工具 short audit race 退出 0（5.521s/5.676s/2.805s）；最终重新生成并通过 1388 声明调用/3 完整程序编译、1415 四项结构、368 围栏、262 锚点、go vet 与 git diff --check。新增矩阵不在此前全库二进制内，以本段包级验证为准。所有本阶段测试会话均已终止，目标仍 active，剩余验证缺口未关闭。

## Slice 浮点排序与判等复核（2026-11-09）

新增 FuzzSliceFloatOrderingAndEquality，四入口（默认链、Serial、Parallel、旧 Slice）对照独立秩桶和浮点位模式 oracle，覆盖正/负 NaN 位模式、正负零、正负无穷、最大有限值、最小非零值及正负 1。输入最多 64 项，升/降序均检查输入有序性、非稳定排序的值多重集、稳定排序的等价代表顺序、Min/Max 原输入首次代表和十二个目标的搜索首个等价位置/插入点；不以调用排序或生产比较函数生成预期值。nil/==/cmp.Compare 等价三类去重及两类 Compact 检查首次代表位模式，NaN 在 Go 相等性下不合并、正负零保留首次代表。模型不覆盖任意 float64 位模式、所有非传递比较器、并发修改或全部分配规模。

首版三种子 race 随机二十轮通过（4.386s），首版 race fuzz 94,815 次（34.665s）；追加输入有序性检查后的最终模型 race fuzz 80,824 次（35.921s）通过。另 TestSliceCompactNilPredicateBoundary 覆盖四入口及空/单项/双项共 12 组，前两种无回调不 panic，双项 nil 谓词 panic；与最终 fuzz 种子合并 race 二十轮通过（3.242s）。未发现新的可复现运行缺陷，累计修复仍为八项。

五个临时 overlay 变体将新旧 BinarySearch 跳过等价位置、新旧 Min 在相等时改用后代表、自定义 Dedup 忽略 eq 改用 Go 相等，全部被搜索/代表/去重行为断言识别，未把编译失败算作识别。脚本 .gotmp/check_float_ordering_mutants.py。

专题澄清 Compact(nil) 不提供 Dedup 的 nil 回退，补严格弱序、cmp.Compare 的 NaN/正负零规则、浮点 int(a-b) 的失真风险及排序/搜索/去重须统一等价关系；生成方法参考同步补比较语义。新增公开 Example_sliceFloatComparison，演示 NaN 搜索、稳定正负零及两种去重数量，race 二十轮通过（4.249s）。全部 62 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 5.278s/retry 3.480s/sliceops 3.699s）。

新增模型和说明后的 sliceops/docs/工具 short audit race 通过（15.800s/5.595s/2.471s），生成一致性、1388 声明调用及 3 完整程序编译、1415 四项结构、368 Go 围栏语法和 go vet 通过。去注释 AST 与未跟踪运行源码核对仍仅既有八项修复，本轮无运行逻辑修改。此前八项修复全库 short audit race 根包 661.851s 属于前一批版本，本批新增测试以上述包级证据为准，未声称该次全库已执行后补模型。逐方法剩余语义、Linux 实际执行、Windows 百万档 race 和独立复核的既有缺口保留，目标 active。
最终本阶段 go test -list 确认 62 Examples（docs 55/retry 3/sliceops 4），265 本地锚点零未匹配，git diff --check 通过；80 局部 Markdown 编译也完整退出 0。最低支持版本之前无实际执行证据，本阶段 GOTOOLCHAIN=go1.25.0 的 go version 已成功获得 go1.25.0 windows/amd64，继续运行包含所有后补测试的全库 short audit race，结果以下一阶段终态为准，不提前计通过。

## 最低 Go 版本与 Windows 386 实际执行（2026-11-09）

GOTOOLCHAIN=go1.25.0 成功获得并执行 go1.25.0 windows/amd64；go vet -tags audit ./... 通过。八项运行修复版本的 go test -race -tags audit -short ./... -count=1 -shuffle=on -timeout=30m 原会话 74958 已退出 1：根包在 418.476s 报 ThreadSanitizer failed to allocate TracePart、1455、unable to mmap，属于 TSan 资源失败，未出现可用的业务断言或 race 诊断，不能计为全库通过。docs/工具/core/group/maps/pipeline/pool/ratelimit/retry/shard/sliceops/task 分别 7.286/7.722/4.742/49.637/6.495/21.092/23.335/7.266/7.481/15.122/15.853/5.734s 通过。日志 .gotmp/audit-go125-eight-fixes-short-race.log，该二进制早于第九项修复。

同一最低工具链设置 GOARCH=386、CGO_ENABLED=0 实际执行 go test -tags audit -short -p 1 ./internal/... ./docs/... -count=1 -shuffle=on -timeout=5m 已退出 0：core 3.393s、group 46.433s、maps 3.556s、pipeline 18.851s、pool 20.789s、ratelimit 3.024s、retry 6.127s、shard 5.004s、sliceops 8.454s、task 3.867s、docs 4.163s、工具 3.446s。日志 .gotmp/audit-go125-386-internal-short.log，不含根包、不含 race，是实际执行而非只编译；早于第九项修复。未发现该范围的 32 位整数/原子对齐运行失败，不可外推到所有平台。

## 滑动窗口最小时长取负溢出修复（2026-11-09）

反向核对非正窗口文档与 now.Add(-window)，确认 window=time.Duration(math.MinInt64) 取负溢出，Allow/AllowN 将截止点放到过去而保留旧记录，违背其他非正窗口的即时过期语义。修复前 TestSlidingWindowNonPositiveDurationExpires 两路径均由 nonpositive window retained prior calls 断言失败，日志 .gotmp/window-duration-before.log。初版测试还让 synctest 时钟从 2000 年推进整段 MaxInt64 时长；其绝对时钟上限不足以推进完整 292 年，最大正窗口的边界失败来自测试假设，已改为一小时内保留验证，未按第二项库缺陷统计。

第九项修复在两方法中以 now 为初始 cutoff，仅 window>0 才执行取负与减法；非正值（含 MinInt64）直接使既有记录过期，保留单次请求限额、n<=0 无操作、正数窗口和精确边界规则。未修改公开签名、锁或构造器默认值。16 组普通回归（十组非正/六组正数），加十二组普通/分片直接构造与 builder 路径及四个 fuzz 种子，最终 race 二十轮通过（4.164s）。

FuzzSlidingWindowDurationBoundaries 使用独立整数经过时间/记录年龄模型，不取负 duration、不改私有时钟，synctest 推进真实 time.Now；duration 为任意 int64，每项最多 64 操作，混合 Allow/AllowN/非正与超限请求及 1ns/1s/1h 前进，检查接受、拒绝和保留条目数。race fuzz 217,764 次（35.774s）通过。四个 overlay 变体恢复 Allow/AllowN 的旧取负、把正窗口也即时过期、保留精确边界记录，最终均被接受/过期行为断言识别，脚本 .gotmp/check_window_duration_mutants.py。首次脚本因失败来自更早的记录数断言而拒绝计成功；移除两类定向用例之间的交叉清理后重跑，四项均由预期断言识别，没有把编译失败计成功。模型不覆盖系统时钟逆行、并发修改配置或无限记录规模。

专题和生成 API 说明补非正窗口含最小值的规则、正窗口精确边界、单次请求限额与生产正数校验要求。第九项修复后的 Go1.25 amd64 ratelimit/retry/docs/工具 short audit race 通过（7.039s/6.580s/7.849s/4.348s）；Go1.25 386 定向回归/种子二十轮通过（ratelimit 3.015s），当时新 Example 的 Output 与普通注释同块导致 no tests to run，不计该 Example 通过，已分离并重跑。

生成一致性、1388 声明调用/3 完整程序编译、1415 方法结构、368 围栏、265 锚点、go vet 及 diff --check 通过；去注释 AST 仅比此前八项新增 Allow/AllowN cutoff 两处。第九项 Go1.26 全库 short audit race 原会话 25071 后来退出 1，详见下一节，不计全库通过。独立复核、Linux 实际执行、Windows 百万档 race 及逐方法剩余语义未闭合，目标 active。
后补 Example_slidingWindowDurationBoundary 的 Output 注释分离后，示例 race 二十轮实际通过（4.451s），全部 63 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 4.982s/retry 4.059s/sliceops 3.998s）；Go1.25 386 同示例二十轮实际通过（3.095s）。go test -list 确认 63 Examples，266 本地锚点零未匹配、diff --check 通过。额外 Go1.26 ratelimit 非 short race 在 3.891s 报 ERROR: Failed to mmap 退出 1，不能计通过；不是业务断言或数据竞争报告。

额外 ratelimit 非 short 普通运行也未取得有效结果：Go 驱动在收集子进程输出时 VirtualAlloc 256MiB 返回 1455，fatal out of memory，退出 1；该次未使用 race，不能简单归为 TSan，也不能计通过。该失败与前述全库终态分开记录，后续按资源条件安排低并行验证。

## ForEach 超时首错顺序复核（2026-11-09）

第九项版本全库命令 `go test -race -tags audit -short ./... -count=1 -shuffle=on -timeout=30m` 原会话 25071 已退出 1，日志 `.gotmp/audit-nine-fixes-short-race.log`。根包 662.218s；docs/工具/core/group/maps/pipeline/pool/ratelimit/retry/shard/task 分别 6.597/3.046/4.467/47.912/6.901/22.840/24.469/7.378/6.699/14.439/6.839s 通过。sliceops 在 15.081s 因 TestSerialFailFastTimeoutStopsRemainingCallbacks/foreach 返回 context.Canceled 而失败，不能将这次全库运行计为通过。

用相同 shuffle 种子 1794186761410777700 对原测试执行 race 二百轮，在 2.185s 再现失败，日志 `.gotmp/serial-ff-repro.log`。反查 eachTOFF → NoResult → Group.runTaskImpl，触发回调返回错误后，Group 先取消共享 context 再记录该错误；等待提交可先记录取消拒绝，再记录实际 DeadlineExceeded；BuildAggregateNoResult 取记录顺序中的首错，没有承诺时间顺序。原测试把无序首错固定为超时，属于测试假设错误。本轮没有为它更改运行实现或新增运行缺陷计数。

修正既有测试后仍严格要求仅一次回调、全部三项失败，并新增回调实际错误必须为 DeadlineExceeded 的断言；汇总只接受 DeadlineExceeded 或 Canceled。修正测试 race 二百轮通过（3.189s）。新增 TestEachTimeoutFailFastRetainsTriggerAndRejectedResults，以 synctest 精确检查三项 Occupied 结果中恰好一个 DeadlineExceeded 和两个 Canceled，以及汇总等于首个实际记录错误；两测试合并同种子 race 二百轮通过（2.996s）。计数、触发错误及取消结果没有放宽。

专题与生成方法注意事项同步说明首错不能推断时间顺序，完整诊断须保存业务错误或检查逐项结果。重新生成后的 1388 声明调用/3 完整程序、80 原文局部片段编译，1415 四项结构、368 围栏语法、266 本地锚点、go vet 和 diff --check 通过；去注释运行差异仍为九项既有修复。

采用 `-p 1` 的同范围全库 short audit race 原会话 85493 已退出 1，日志 `.gotmp/audit-nine-fixes-final-short-race.log`：根包 315.441s 报 ThreadSanitizer failed to allocate TracePart、1455、unable to mmap，不计通过；docs/工具/core/group/maps/pipeline/pool/ratelimit/retry/shard/sliceops/task 分别 7.297/4.429/4.486/48.707/5.479/21.928/21.316/6.517/6.690/14.750/15.151/6.087s 通过，确认修正后的 sliceops 全包有效。根包此前同一运行实现的 662.218s 通过记录保留，但不把两次运行拼成单命令通过。

后续根包改用 `.gotmp/check_root_partitioned.py`，先编译同一 race/audit 二进制并获取全部顶层 Test/Fuzz/Example 名称，再每二十项在新进程执行原 short、随机顺序和原始档位。清单与逐批退出码写入 `.gotmp/root_partitioned_race/manifest.json`，每批完整日志单独保存。该方式用于缩短单进程存活时间，不能替代单进程跨测试状态的检查，也不能算此前失败命令通过。发现 441 项顶层测试，前十一批共 220 项通过；第十二批在 TestRatelimit_Chain_HighConcurrency_RateLimiter/十万级_100K 再次报 TracePart 1455，退出 66，并未取得完整覆盖。改用 .gotmp/check_root_remaining.py 保持同一二进制及原任务档位，设置 GOMAXPROCS=2，从第十二批起重新执行余下 221 项，日志与清单另存 .gotmp/root_partitioned_cpu2，该续跑已退出 0，十二批共 221 项全部通过。两清单的成功批次合并后严格等于初始 441 个名称，零遗漏、零重复；只表示每项均在所记录设置下执行过，不能推断原调度或单进程全库已通过。

## Adaptive 公开获取策略文档复核（2026-11-09）

发现生成参数说明把 RateLimiter 的 Reject/BlockForce 可选策略套用于 Adaptive.Acquire，而普通及分片 Adaptive 的公开方法没有 WithStrategy，内部构造固定 Block。已按实际接收者区分参数和分片注意事项，专题同步说明有空位时已取消仍可成功、耗尽时等待取消或归还，以及分片只等待选中片。没有新增运行实现修改。

新增 TestAdaptivePublicBlockAdmission，普通/两片与直接构造/builder 共四组，通过公开入口验证已取消 context 有空位仍取得槽位、耗尽取消返回 Canceled、后台等待在虚拟一小时后仍阻塞、成功归还后继续。TestShardedAdaptiveWaiterStaysOnSelectedShard 两组以 GetShard(0) 排空选中片而保留另一片空位，验证外层不搜索其他片，取消及关闭分别唤醒为 Canceled/ErrRateLimiterStopped；直接许可由原片释放。全部六组 race 随机二十轮通过（4.132s），Go1.25 Windows 386 实际二十轮通过（1.805s）。此前 permit fuzz 改用了私有 Reject 以检查有限状态，不能视为这个公开 Block 契约的覆盖。

三个临时 overlay 变体分别提前检查取消而忽略可用槽位、将简单构造器改为 Reject、将外层首选片偏移一片，均被获取/等待行为断言识别，脚本 `.gotmp/check_adaptive_public_mutants.py`；没有把编译失败计为识别。该测试不覆盖动态 Resize、并发成功率调整或任意并发调度。新增测试后的 ratelimit/docs/工具 short audit race 已退出 0（6.919/5.872/4.075s）；生成一致性及 1388 声明调用/3 完整程序重新编译通过。
最终追加验证：Go1.25 amd64 的 Adaptive 六组和 ForEach 超时两项合并 race 随机二十轮通过（ratelimit 2.780s/sliceops 4.695s）。ForEach 新契约还识别了将触发超时错误替换为取消、漏提交最后一项两个变体，分别由 deadline=0 canceled=3 与 total=2 failed=2 的断言失败定位，脚本 .gotmp/check_failfast_timeout_mutants.py。最终 1415 方法结构、368 围栏、268 本地锚点、go vet 和 diff --check 通过；运行修复仍为九项。

## 第九项版本最终包级与资源边界（2026-11-09）

根包原分批会话 19392 已退出 66：前十一批 220 项通过，第十二批 TSan 资源失败。续跑会话 14777 已退出 0：GOMAXPROCS=2 下第十二至二十三批共 221 项通过。独立读取两个 manifest 并核对初始名称集合，成功批次恰好覆盖全部 441 项，无遗漏和重复；各批均保留 -race/-short、原任务档位及随机顺序。不能把它写成单次 go test ./... 通过。

限流包最新非 short 普通命令 go test -v -tags audit -p 1 ./internal/ratelimit -count=1 -shuffle=on -timeout=5m（GOMAXPROCS=2）完整退出 0，20.682s，日志 .gotmp/ratelimit-nine-fixes-nonshort-cpu2.log，包含百万档用例及最终 Adaptive 新契约。相同设置追加 -race 后，在 TestRateLimiter_Token_Concurrent/百万级_1M 报 Failed to mmap、TracePart 1455，13.426s 退出 1，日志 .gotmp/ratelimit-nine-fixes-nonshort-race-cpu2.log；无可用业务断言或数据竞争报告，仍不计通过。

WSL 只读复查仍只有停止状态的 docker-desktop，没有普通 Linux 发行版；未启动、安装或修改服务。独立 reviewer 接口依旧在参数解析阶段失败，未产生审查结论。Linux 实际执行、Windows 百万并发 race、独立复核以及逐方法剩余语义审查仍未闭合，目标保持 active。当前全部测试/变体会话均已终止，工作区保留全部修改，未提交或推送。

收尾文档验证：最终生成一致性、1388 声明调用及 3 完整程序编译、1415 方法四项结构、368 Go 围栏语法、272 本地锚点零未匹配、go vet 与 git diff --check 通过。gofmt 后再次 -check 通过；本批未新增公开示例，总量仍为 63。最终 Go 运行差异仍仅九项既有修复，新增 ForEach 与 Adaptive 的契约测试不改变运行实现。

## Map 池化结果与保留策略交叉复核（2026-11-09）

反查 MapRunner.Map/MapBatch/ForEachBatch 对结果下标的使用，起初怀疑 Pool.Wait 在 MaxResults 截断或追加完成顺序的环形结果时可能造成原键错配。实际 sliceops.execMapPool 通过 SubmitWithCompletionTimeout 为每个输入保存独立结果槽位，Pool.Wait 仅用于等待，返回的保留结果不参与重建；未复现这项运行疑点，没有为它修改实现。

新增 TestMapPoolProjectionIndependentOfRetention：MapChain/MapParallel 两入口，MaxResults=1 下无环形及容量 1 的 Drop/Error/Block，成功/部分失败/预取消/已关闭四状态，Map/ForEach/MapBatch/ForEachBatch 四终端，共 128 组，每组同一借用池顺序执行两次。synctest 使首个开始的回调最后完成，按回调实际收到的键、批次首键和批长建立预期，检查成功值与原键、各元素恰好一次、拒绝不进业务、Total/FailCount 和复用。Go1.26 race 随机二十轮通过（4.074s），Go1.25 amd64 race 二十轮通过（5.832s），Go1.25 Windows 386 实际二十轮通过（3.445s）。测试未开启 FailFast 或模拟并发修改输入。

四个 overlay 错误变体分别改用 Pool.Wait 保留结果、偏移原条目键、偏移批次首键、将元素总数改为批次数，均被映射/统计断言识别；脚本 .gotmp/check_map_pool_projection_mutants.py。首次脚本过宽匹配所有 pl.Wait 导致自身断言失败，缩小为 execMapPool 后四项有效；不把脚本断言或编译失败计为识别。

扩展 FuzzMapRunnerStrategies 加入借用池、MaxResults=1、三种容量 1 环形策略，并添加按实际批次首键判失败的部分批次结果和按实际批长累计失败元素的模型。最终 race fuzz 11,571 次通过（39.199s），日志 .gotmp/map-runner-retention-final-fuzz.log；此前只有借用池扩展的阶段版本 18,883 次通过（40.136s），不混算最终模型执行数。模型最多 48 输入，覆盖五种策略与 Map/ForEach/Stream/全部失败及部分失败的批次，不覆盖任意规模或无限并发调度。最终新增契约/五种子/公开示例在 Go1.25 amd64 合并 race 二十轮通过（maps 5.211s/docs 4.286s）。

专题与生成 API 补充：MaxResults/环形保留不截断这些终端自己的结果及统计，执行器仍按输入或批次数另存槽位，且 Map 复制全部条目用于调度，不能据池上限推算全部内存。构造器的“不提供输入快照”改为构造时不复制、部分终端执行时复制条目但不与写入同步，避免与实际实现矛盾。公开 Example_mapPoolRetention 演示全量 Map 和失败批次按元素累计及把错误传回池 owner；初稿误用 async.Map 后编译失败，已改为真实 NewMapChain 入口。最终示例 race 二十轮通过（4.230s），Go1.25 386 实际二十轮通过（2.945s），全部 64 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 8.675s/retry 8.298s/sliceops 7.088s）。

最终 maps/docs/工具 short audit race 通过（6.244/7.479/2.749s）；80 原文局部片段编译、1415 方法结构、368 围栏语法及 go vet/diff 检查通过，运行差异仍仅九项。独立 reviewer 再次在工具参数解析阶段失败，无审查结论；Linux 实际、Windows 百万档 race 和其余逐方法语义审查仍未闭合。保留全部工作区修改，未提交或推送，目标 active。

追加 maps 非 short audit race 全包通过（10.225s）；构造器说明修正后的生成一致性、1388 声明调用及 3 完整程序编译通过，全部 64 Examples 名称已重新列出核对。最新静态检查为 1415 四项结构、368 围栏、275 本地锚点零未匹配、go vet、gofmt 和 diff --check 通过。当前所有测试会话均已结束。

## Stream 投递取消与清理交叉复核（2026-11-09）

本轮从 sliceops.streamPool 的完成回调反查，结果发送 select 使用父 context，而 FailFast 只取消派生任务 context；逐次任务 Timeout 也不控制结果发送。没有发现新的可复现运行缺陷，未修改运行实现。本轮所述计时、取消和收尾保证均以回调最终返回、父 context 非 nil、构建器及池不与其他批次并发复用为前提。

新增 stream_delivery_contract_test.go 四项八组契约。TestPoolStreamCanceledDeliveryJoinsRunningCallback 为借用/接管与 FailFast 开/关四组：用回调门闩使容量 1 输出填满、一 worker 阻塞投递、另一回调等待；父取消释放投递，但不在业务回调返回前复位池，最终输出只保留原缓冲值；借用池可用新 context 复用，接管池已关闭。TestPoolStreamFailFastPreservesBlockedTriggerDelivery 两组验证业务错误及 panic 在满缓冲后触发派生取消，继续消费后恰好保留一个触发错误，其余仅允许取消结果；父 context 保持有效，借用池在通道关闭前复位。TestPoolStreamCallbackTimeoutDoesNotCancelDelivery 用虚拟时间验证成功返回的回调 context 之后过期，不丢失或重写成功结果，输出背压直至被消费。前三项七组 Go1.26 race 随机一百轮通过（5.176s）。

初版对结果顺序与取消屏障作了过强假设：要求触发错误第二条到达，被同步提交拒绝的 Canceled 抢先投递反例推翻；要求取消后恰好只开始三个回调，被四个调用反例推翻。反查标准 context 的父 Done 先关闭再逐一取消子 context，释放投递的 worker 可能在这段传播中通过另一排队任务的子 context 预检查。修改测试为所有尚未返回回调共用门闩，取消完成并 synctest.Wait 后记录实际在途数量，要求此后不再增加；触发错误按完整流恰好出现一次检查，不要求固定接收位置。第一次 Fatal 后触发 synctest 清理死锁也属于错误测试假设的后果，未按库缺陷统计。日志 .gotmp/pool-stream-delivery-contract.log、.gotmp/pool-stream-delivery-final-contract.log。

TestPoolStreamAdapterIndependentOfNativeStreamingDrops 使池原生 StreamResults 容量 1、MaxResults=1、Drop 环形容量 1，同时适配器输出容量 1。停消费后原生流丢弃一次、适配器形成背压；恢复消费后适配器交付全部四个结果，含成功、业务错误及保留 panic 原值。随后验证旧原生通道关闭且只保留首项，借用池复位替换原生通道、清零 StreamDropped 和提交计数。该项 Go1.26 race 一百轮通过（4.539s）。最终全部八组与新增公开示例在 Go1.25 amd64 合并 race 一百轮通过（sliceops 3.213s/docs 5.022s）；Go1.25 Windows 386 实际一百轮通过（sliceops 3.535s/docs 4.272s）。这些测试覆盖受控调度，不声称覆盖任意并发历史。

五个临时 overlay 变体分别改用派生 taskCtx 取消结果发送、漏借用池复位、漏接管池关闭、漏逐次 Timeout、把原生流丢弃错误传播为适配器丢弃；全部由对应背压/生命周期断言识别，脚本 .gotmp/check_stream_delivery_mutants.py。脚本首次分别因派生取消及原生丢弃变体触发更早的预期行为断言而未计通过，按实际断言定位后完整五项重跑有效；未将脚本或编译失败计为识别。

专题、生产指南和生成 API 已补父取消/FailFast/Timeout 与投递背压区别、提交拒绝可早于业务触发错误输出、回调返回前不能完成清理、原生池流与构建器输出独立，以及复位后丢弃计数不能用于旧批次审计。新增 Example_poolStreamEarlyStop 演示取消独立流 context、消费到关闭，再用有效 owner context 复用借用池。该示例 Go1.26 race 一百轮通过（4.422s）；全部 65 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 8.081s/retry 8.033s/sliceops 8.399s），名称保存 .gotmp/stream-final-example-names.log。

最终 sliceops/maps/docs/工具 short audit race 通过（15.970/4.025/7.563/4.272s）；1388 声明调用及 3 完整程序编译、80 原文局部片段编译、1415 四项结构、368 围栏语法、go vet 和 diff 检查通过，最新本地锚点 276 个零未匹配。本轮无新增运行修复，累计九项保持；Linux 实际、Windows 百万档 race、独立复核和其余逐方法语义仍未闭合。所有测试与变体会话已终止，未提交或推送，目标 active。

记录更新后的最终静态复验：279 本地锚点零未匹配，1415 方法四项结构、368 围栏语法、生成一致性、go vet、gofmt 与 diff --check 通过。

## Task Builder 超时与句柄生命周期交叉复核（2026-11-09）

从 TaskBuilder.wrapTimeout/wrapTimeoutAct 反查四种终端、Bounded 取得许可及 Task.Result 缓存。未复现新的运行缺陷，本轮没有修改运行实现。既有缓存 fuzz 覆盖并发等待/取消，但未精确约束 Builder 超时起点、外层与回调 context 生命周期，故另加虚拟时间契约，不把原 fuzz 的执行次数视为这些边界的覆盖。

新增 builder_lifecycle_contract_test.go：TestTaskBuilderTimeoutExcludesAdmissionAndJoinsCallback 四组，以占满许可并等待虚拟十秒验证两秒回调期限不约束排队；取得空位后期限恰为两秒，超时仍保留在途许可且结果等待未返回，忽略取消后成功返回仍缓存成功，并可复用许可。TestTaskBuilderParentDeadlineRejectsBlockedAdmission 四组验证三秒父期限结束等待，业务不调用、拒绝不占用或释放其他任务许可，返回 DeadlineExceeded。TestTaskBuilderCallbackContextCleanupAndCapturedTimeout 为四终端×有无 Bounded×成功/错误/panic 共二十四组，验证终端保存超时值、回调退出时取消子 context、外层 Task.Ctx 继续存活直至显式取消、取消不改写缓存业务结果及许可释放。总计三十二组，均只覆盖受控调度，不声称穷举并发历史。

最终三测试与新增 Example_taskCallbackDeadline 合并 Go1.26 race 随机一百轮通过（task 4.866s/docs 2.270s）；Go1.25 amd64 race 一百轮通过（5.120/2.194s）；Go1.25 Windows 386 实际一百轮通过（2.402/0.953s）。此前首两测试 Go1.26 race 一百轮通过（3.760s），不与最终版重复计数。三个临时 Go overlay 变体分别遗漏回调 context 清理、闭包读取后续修改的 Builder 超时、将过期 context 错误覆盖业务成功，均由生命周期/结果断言识别，未将编译失败计为识别；脚本 .gotmp/check_task_builder_lifecycle_mutants.py 及日志保留。

Task 专题与生成方法注意事项补充：Bounded 终端在返回句柄前会等待许可，因此整体请求预算应由父 context 限制，事后调用 WaitTimeout 不覆盖此前阻塞；Task.Ctx 不暴露 Builder 的回调子期限，也不自动充当完成通知。回调子 context 正常/错误/panic 退出均取消，需要继续工作的子 goroutine 不应依赖其生命周期；Task.Result 才能等待真实返回，Task.Cancel 仍须显式释放外层派生 context。新公开示例展示回调期限后返回成功与外层显式清理，不鼓励生产回调忽略取消。全部 66 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 1.712s/retry 1.837s/sliceops 1.966s），名称保存 .gotmp/task-final-example-names.log。

最终 task/docs/工具 short audit race 通过（4.930/5.481/2.043s），1388 声明调用/3 完整 Markdown 程序编译、80 原文局部片段编译、1415 方法四项结构、368 围栏语法、go vet、gofmt 和 diff --check 通过，更新记录前 279 本地锚点零未匹配。去注释运行差异仍仅九项既有修复，没有为这些契约调整功能。Linux 实际运行、Windows 百万档 race、独立复核和其余逐方法语义仍未闭合，目标保持 active；全部测试会话已退出，未提交或推送。

本节及索引更新后再次核对：282 个本地 Markdown 锚点零未匹配，1415 方法四项结构、生成一致性和 diff --check 通过。

## 分片批量背压与部分提交交叉复核（2026-11-09）

从 ShardedPool.SubmitBatch/TrySubmitBatch 追踪 pickItem → Pool.Submit/TrySubmit → pending 名额及 enqueueTask。现有路由 fuzz 的闭片、预取消及普通队列满覆盖，不能证明 MaxPending 三策略和按项计时，故新增 batch_backpressure_contract_test.go。未复现新的运行缺陷，本轮只新增测试和文档。

TestShardedBatchHotShardBackpressure 共十二组：RoundRobin/Hash×Block/Error/Drop×SubmitBatch/TrySubmitBatch。第零片持有一个运行回调和一个等待项，另一片容量可容纳全部冷项；用虚拟时间验证热点片拒绝不改投、后续冷项继续、每项索引与片号、统计和最终结果。Block 的普通提交在第一热项等待两秒期间不提交冷项，之后冷项执行、第二热项再等两秒，整批共四秒；Try 即时返回 ErrSubmitTimeout，Error 即时返回 ErrQueueOverflow，Drop 返回 nil 而最终保留两个 ErrQueueOverflow 并计失败。TestShardedBatchParentDeadlineStopsLaterAdmission 一组验证一秒父期限结束整体排队，全部四项记 DeadlineExceeded、无业务调用、未释放原任务名额。TestShardedBatchKeyFnPanicLeavesAdmittedPrefix 两组验证 KeyFn 对第三项 panic 直接传播，中止批量且返回变量未赋值，前两项仍执行并可 Wait 收集，没有自动回滚或把路由 panic 当业务失败。合计十五组，不覆盖任意并发调度及同时配置修改。

测试开发中纠正了三项自身假设/问题：冷片 MaxPending=1 的 Try 可在 worker 未调度时拒绝第二冷项，改为容纳整批冷项，隔离所测热点；父期限测试在完成前读取未同步局部切片，被 race 报告定位，改为完成通道观察；Go1.25 386 曾观察 Wait 刚返回时 TotalActive 尚未递减，结果集合完全正确，反查 worker 在 processTask.finishTask 后才 active.Add(-1)，契约改为 synctest.Wait 调度收敛后断言 Active=0。首次测试 Fatal 的清理死锁源于未释放测试门闩，已加入 defer 释放。以上没有按运行库缺陷统计，也未通过改运行实现迎合测试。

最终三测试加 Example_shardBatchDrop 的 Go1.26 race 随机一百轮通过（shard 4.718s/docs 4.057s），Go1.25 amd64 race 一百轮通过（5.819/3.114s），Go1.25 Windows 386 实际一百轮通过（2.857/2.292s）。新增公开示例在 Go1.26 race 单独一百轮通过（4.548s），展示所有提交返回 nil 但仅冷项执行业务，最终结果含队列溢出并由 Run owner 返回。全部 67 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 3.013s/retry 2.920s/sliceops 4.422s），名称保存 .gotmp/shard-batch-final-example-names.log。

四个临时 overlay 变体：把期限共享给整个批次、首个提交错误后 break、批量忽略元素 KeyFn、Drop 改为返回提交错误；均由背压、路由或结果断言识别，脚本 .gotmp/check_shard_batch_backpressure_mutants.py。初稿对路由错误变体还运行阻塞式 panic 测试，错误路由导致它在断言前排满单片并超时；最终把该四变体的测试选择限定为热点契约，以明确行为断言识别，完整重跑全部四项成功，未把超时或编译失败计入识别。

专题、生成方法注意事项补每项独立提交计时、前项阻塞影响后项、整体预算放入父 context、Index 是本次输入下标、KeyFn panic 后部分提交须收尾、MaxPending 满时三种策略的返回/结果差异及 Wait 与 TotalActive 的收尾窗口。最终 shard/pool/docs/工具 short audit race 通过（15.610/22.070/7.641/4.165s）；1388 声明调用/3 完整 Markdown 程序、80 原文局部片段编译、1415 方法结构、368 围栏语法、go vet 和 diff --check 通过，更新审查记录前 282 本地锚点零未匹配。去注释运行差异仍仅九项既有修复。Linux 实际运行、Windows 百万档 race、独立复核及其余逐方法语义仍未闭合；目标 active，未提交或推送。

最后追加 TotalActive 生成说明并更新记录后，生成一致性及 1388 声明调用/3 完整程序重新编译通过；285 本地锚点零未匹配、1415 方法结构、go vet、gofmt 和 diff --check 通过。所有测试与错误变体会话均已结束。

## Group 外部池双层期限与结果交叉复核（2026-11-09）

反查 Group.executeTask → Pool.SubmitWithCompletion → Group.runTaskImpl 及完成通知。现有生命周期 fuzz 覆盖结果缓存、错误/panic、复位及外部池，但没有虚拟时间验证双层期限；已有拒绝回归覆盖关闭/Drop/Error，不能替代提交超时的时间范围。本轮新增 external_pool_timing_contract_test.go，没有发现新的可复现运行缺陷，没有修改运行实现。

TestGroupExternalPoolExecutionDeadlinesStartAfterQueue 为 Pool 期限 0/2s/5s × Group 单项期限 0/1s/3s 共九组：外部池先被独立回调占满，Group 包装任务排队虚拟十秒后才开始；检查回调期限从执行时起，并取两层有效期限较早者，单项 0 清除原 Group 的一小时期限但不清 Pool 期限。检查 Group 保存实际 42 和业务期限错误、Pool 保存零值 nil 包装成功；Group.Wait 后还能向 Pool 提交下一项，Pool 总成功为三项，不把业务错误算作 Pool 错误。TestGroupBusinessFailureDoesNotTriggerExternalPoolFailFast 三组为业务错误/panic/回调超时：组启用 FailFast 且父来自 Pool 的 FailFast context，分别检查实际值、错误链和 panic 原值，并在 Wait 前确认组已取消、Pool 仍有效，再核对包装成功和后续池任务。这个 Wait 前检查避免组等待自身取消掩盖漏发业务 FailFast。

TestGroupSubmitTimeoutDoesNotCoverExternalPoolDispatch 两组为 Go/GoAt：外部 Pool MaxPending=1、Block、提交期限三秒，Group 提交及执行期限均一秒。g.Go/GoAt 立即成功，向外部池分发阻塞两秒后业务仍未执行也未记错；三秒后 Pool 提交超时，Group 收到唯一 Occupied ErrSubmitTimeout、Active 清零且共享组 context 在 Wait 前仍有效。Group.Wait 在无关外部池回调仍阻塞时完成，说明仅等待本组；释放独立回调后 Pool.Wait 得两成功一拒绝。合计十四组受控调度，不覆盖同时修改配置、任意外部池生命周期竞争或全部父期限组合。

最终十四组 Go1.26 race 随机一百轮通过（3.531s）；与新增公开示例合并的 Go1.25 amd64 race 一百轮通过（group 7.790s/docs 3.574s），Go1.25 Windows 386 实际一百轮通过（5.374/3.299s）。此前未加入 Wait 前取消断言的阶段版 Go1.26 race 一百轮通过（4.118s），最终证据以上述强化版本为准。Example_groupExternalPoolResults 单独 Go1.26 race 一百轮通过（4.338s），展示组保存业务值/错误和池包装零值/成功，owner 显式返回 JoinErrors。全部 68 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 7.971s/retry 8.328s/sliceops 8.486s），名称保存 .gotmp/group-external-final-example-names.log。

五个临时 overlay 变体分别延长 Group 回调期限、向 Pool 传播组业务错误、遗漏组业务 FailFast、把分发拒绝当作业务 FailFast、遗漏完成通知的 Active 递减；均被相应期限/结果/取消/计数断言识别，脚本 .gotmp/check_group_external_timing_mutants.py。最终完整五项重跑有效，脚本排除编译失败及整体测试超时，不以这些基础设施失败计为识别。

专题外部池章节和生成 WithPool 注意事项补两层队列及期限表、提交 nil 与异步分发错误的区别、业务错误/捕获 panic 与池成功统计/FailFast 的区别，以及 Group.Wait 不等待无关池任务、不复位关闭外部池的所有者职责。最终 group/pool/docs/工具 short audit race 通过（48.290/21.961/6.081/4.075s）；1388 声明调用/3 完整 Markdown 程序、80 原文局部片段编译、1415 方法结构、368 围栏语法、go vet、diff --check 通过，更新记录前 285 本地锚点零未匹配。运行差异仍仅九项既有修复；Linux 实际、Windows 百万档 race、独立复核及其余逐方法语义仍未闭合，目标 active，未提交或推送。

本节及索引更新后最终静态复验：288 本地 Markdown 锚点零未匹配，1415 方法四项结构、生成一致性、gofmt 和 diff --check 通过。所有测试及变体会话已退出。


## Slice 批处理极值与失败批次映射交叉复核（2026-11-09）

从 Chunk/ChunkN 的溢出规避算法反查 Runner.MapBatch/EachBatch、SliceBuilder.MapBatch/ForEachBatch 和 collectBatchFailValues。既有极值测试只验证纯分块及查询，本轮新增 batch_extreme_contract_test.go，以逐元素构造的独立批次模型校验实际执行，不复用 Chunk、向上取整或结果收集器作为 oracle。未复现新的运行缺陷，没有修改运行实现。

TestBatchExtremeSizesAndFailureProjection 为输入长度 0/1/7 × MinInt/-1/0/1/2/3/MaxInt-1/MaxInt × serial/parallel/timeout/shard/池仅保留一个结果/池仅保留一个加容量一的丢弃环形缓冲 × 成功/错误/panic 起始模式，共 432 组。每组分别运行 MapBatch 和 ForEachBatch，核对批次成员、回调恰好一次、输入不变、按批次结果索引和统计、正常错误与返回值共存、panic 值和零值、成功 Values、完整失败输入 FailValues、Errors 数与错误状态；单个超大批次也经过成功、错误、panic。它不穷举 FailFast、拒绝、取消或任意配置并发。

最终 Go1.26 race 随机二十五轮通过（5.239s）；Go1.25 amd64 race 二十五轮通过（5.716s）；Go1.25 Windows 386 实际二十五轮通过（4.416s）。新增 Example_sliceBatchFailureProjection 最终 Go1.26 race 实际一次通过（4.048s），Go1.25 race 实际一次通过（4.352s），Go1.25 386 实际一次通过（2.912s）。第一次联合执行时该新示例 Output 与说明注释同组，Go 将其视为不可执行示例并报告 no tests to run；已加空行分离，重新以 -v 观察一次 RUN/PASS；-count=25 不重复 Example，先前该示例未执行的命令不计验证。测试编辑脚本一度因换行转义语法失败，未影响文件；修正后才产生最终 432 组版本，旧 144 组结果不充作最终结果。

四个 Go overlay 错误变体由模型和投影断言识别：非正块大小错误回退为二、失败批次只收集首个元素、Total 错误加入元素数、遗漏最后一批。首次统计变体因变量未使用导致编译失败，明确排除；修正为可编译的错误统计后重新执行，四个均为断言失败，无超时或编译失败计入。脚本 .gotmp/check_batch_extreme_mutants.py 及日志保留。

专题、生成方法注意事项补非正参数每元素一批、MaxInt 对非空短输入为一批、逐批值与错误、失败输入展开、池保留策略不截断本终端结果/统计，以及批次共享数组和部分副作用的幂等要求。69 Examples race 单次命令（传 -count=5，每例实际一次）通过（docs 8.295/retry 7.717/sliceops 7.928s），名称清单 .gotmp/batch-final-example-names.log。sliceops/docs/工具 short audit race 通过（15.756/6.371/4.515s）；1388 声明调用与 3 完整 Markdown 程序编译、80 原文局部片段编译、1415 方法四项结构、368 围栏语法、go vet 和 diff --check 通过，更新记录前 288 本地锚点零未匹配。去注释运行差异仍仅九项既有修复。Linux 实际执行、Windows 百万档 race、独立复核及剩余逐方法语义缺口仍保留，目标 active；全部测试会话退出，未提交或推送。

## Example 实际执行次数更正（2026-11-09）

从三份新示例 -v 日志分别只出现一次 RUN/PASS 追踪 Go testing.M.runTests 和 runExamples：普通 Test 在 count 循环内，Examples 在循环外，每个测试进程仅执行一次。历史文档把 -count 参数当作 Examples 重复轮数，属于验证报告错误；已在三份审查记录顶部显著更正并改写 Examples race 五轮的历史表述。混合 Test/Example 命令中的普通 Test 重复证据仍有效；历史单示例的一百轮措辞不代表实际一百次，均受顶部更正约束。

为获得真实重复证据，显式循环五次独立 go test -race -tags audit ./docs ./internal/retry ./internal/sliceops -run ^Example -count=1 -shuffle=on，五次均退出 0：docs/retry/sliceops 依次为 4.781/4.448/4.672s、3.128/5.047/4.496s、4.786/2.435/4.791s、4.276/4.541/4.442s、3.425/4.630/4.202s。当前清单 69 个 Example，每次进程各执行一次，共五次；日志分别保留。新增单示例的 Go1.25 race 与 386 -v 日志各确认一次 RUN/PASS。没有更改运行实现或普通测试，此次纠正不消除现存环境及语义审查缺口。


## Pipeline 共享期限与观察函数生命周期交叉复核（2026-11-09）

从 applyTimeout → 各路径回调包装 → itemCancel → OnResult → Wait 反向追踪，已有 Meta 模型覆盖结果/观察 panic，但没有精确证明整体期限及观察函数进入时 context 状态。新增 deadline_observer_contract_test.go，不修改运行实现；本轮未复现新的运行缺陷。

TestPipelineOverallDeadlineWaitsForCallbacks 共 28 组：默认/Serial/Parallel原生/Shard/AutoScale/借用Pool/Stream × FailFast开关 × Pipeline五秒期限或三秒父期限。第一阶段等待虚拟一秒后转第二阶段，逐次核对绝对截止时间仍为起点加整体预算；第二阶段等待到期后继续阻塞，验证终端尚未返回，释放后业务值30与DeadlineExceeded均保留；Stream另有顶层期限错误和Occupied，其他路径顶层nil并检查逐项错误，执行不取消未过期caller父context。

TestPipelineObserverContextCleanupByPath 十四组验证两个阶段成功结果、观察次数及保存的业务context：正数Pipeline Timeout下，Serial/Parallel原生/默认FailFast在观察前清理单项context；默认无FailFast、Shard/AutoScale/Stream及池FailFast在任务内观察；池无FailFast在阶段Wait后观察，任务子context已清理。执行结束后保存context均结束，caller父context仍有效。TestPipelineDeadlineDoesNotInterruptObserver 十四组把末阶段OnResult阻塞至六秒，证明五秒整体期限不能中断观察或让终端提前返回。TestPipelinePoolObserverStageBarrierDependsOnFailFast 两组用两个任务，其中一项持有屏障，验证无FailFast在阶段结束前零通知，有FailFast则先通知已完成项；释放后每项只通知一次。总计58组，不穷举所有输入、并发观察、池拒绝或外部配置竞争。

最终58组Go1.26 race随机一百轮通过（4.481s），Go1.25 amd64 race一百轮通过（8.006s），Go1.25 Windows386实际一百轮通过（6.930s）。初始42组及56组均有阶段验证，但最终证据以58组为准。新增Example_pipelineObserverContext展示Serial观察前业务context已取消；Go1.26race/Go1.25race/386与56组联合命令分别通过（docs4.363/2.919/4.842s），Example在每个进程仅一次，-count=100只作用普通Test。

四个Go overlay错误变体均由行为断言识别：整体预算加一秒、Serial每阶段重设完整预算、遗漏Serial单项context清理、过期后错误清空业务返回值。四个均可编译，未以超时/构建失败计入；脚本.gotmp/check_pipeline_deadline_mutants.py及输出保留。

专题及生成Timeout/OnResult注意事项补全部阶段共享绝对期限、整体到期仍须等待回调、返回值和逐项错误保留、观察函数在不同路径的位置及持久化建议；明确表格只约束普通Execute/Run且context状态不能替代ctx.Err检查。新增公开示例后70 Examples用显式五次独立go test -count=1进程race运行：docs/retry/sliceops分别4.109/4.044/5.097s、4.647/3.391/4.550s、4.063/3.214/4.143s、4.092/3.090/4.177s、3.723/4.023/4.452s。日志.gotmp/pipeline-explicit-examples-1.log至-5.log和名称清单pipeline-observer-final-example-names.log保留。

最终pipeline/docs/工具short audit race通过（22.200/7.632/4.019s）；1388声明调用/3完整Markdown程序、80原文局部片段编译、1415方法四项结构、368围栏语法、go vet、diff --check通过，更新记录前291本地锚点零未匹配。去注释运行差异仍仅九项既有修复。Linux实际执行、Windows百万档race、独立复核和其余逐方法语义缺口保留，目标active；全部测试会话已退出，未提交或推送。


## Pool 结果转换与同步通知交叉复核（2026-11-09）

依据requesting-code-review技能尝试只读独立审查，子代理工具仍只接受空对象并在解析阶段报告缺少message，未创建审查者；没有把本轮自行核对称作独立复核。现有九项修复与代码diff保持，继续检查Pool.processTask → record/transform → resultsSet/recordToStream/ringBuf → completion → finishTask，与合法/非法索引拒绝支路对照。本轮没有复现新运行缺陷，未修改运行实现。

新增transform_notification_contract_test.go。TestPoolTransformResultVersusBusinessOutcome十八组为业务成功/错误/panic × 转换增加错误/清除错误/panic × 无限主存储镜像环形缓冲/主上限一的溢出环形缓冲。用索引1保留零号稀疏空槽，独立检查原始转换输入、转换一次、business→transform→observer顺序、全局观察前流中已存在同一结果、Wait槽位/镜像或溢出环形缓冲一致，及原业务统计。Wait前synctest.Wait确认任务已结束后核对FailFast，避免Wait本身取消掩盖问题。转换panic前显式把局部Value改为-1000，验证保存仍是原业务值及转换PanicError；没有将局部修改当已提交值。

TestPoolTransformRejectedSubmissionNotifications六组为合法预取消/非法索引 × 转换成功/错误/panic。合法拒绝在Submit返回前转换一次、返回原Canceled、Total/Fail仍各一、不触发FailFast、无全局通知/结果流、主范围内拒绝不镜像环形缓冲；非法索引不转换/保存/计数。TestPoolCompletionWaitIncludesObserverAndCompletion四组为顺序/指定索引入口 × 观察和completion是否panic，以双屏障验证先通知全局观察再单次completion，Wait两处均不会越过阻塞，流在观察前已发送；两个通知panic均恢复且业务42/nil和成功统计保留。总计28组，不穷举关闭/溢出/拒绝的所有并发历史或动态配置。

最终强化版本Go1.26 race随机一百轮通过（3.295s）；Go1.25 amd64 race一百轮通过（pool2.855/docs2.334s），Go1.25 Windows386实际一百轮通过（pool2.148/docs1.063s）。联合命令中新Example_poolTransformStatistics在每进程实际一次，-count=100只作用普通Test；该示例Go1.26单次race -v通过（2.507s）。初始22组及未加入panic前局部修改的28组结果为阶段证据，不代替最终版本。

四个Go overlay错误变体分别忽略转换、因转换错误触发池FailFast、转换panic清空原Value、向全局通知/结果流发送预检查拒绝，均由业务结果/通知断言识别，未以编译失败或超时计入；脚本.gotmp/check_pool_transform_mutants.py及日志保留。专题及生成SubmitAtWithResultTransform注意事项补同步顺序、Wait等待通知、转换结果与业务计数/取消的区别、panic保留原值、合法/非法索引拒绝和镜像/溢出缓冲规则；新示例要求owner显式返回转换错误。

全部71 Examples显式五次独立go test -race -count=1进程通过：docs/retry/sliceops依次2.352/2.351/2.583s、2.383/2.095/2.221s、2.432/2.462/2.398s、2.401/2.337/2.200s、2.451/2.090/2.033s；日志pool-transform-examples-1.log至-5.log及名称pool-transform-example-names.log保留。最终pool/docs/工具short audit race通过（21.883/6.145/3.089s）。1388声明调用/3完整Markdown程序、80原文局部片段编译、1415方法四项结构、368围栏语法、go vet、diff --check通过；更新记录前294本地锚点零未匹配。去注释运行diff仍仅九项既有修复。Linux实际运行、Windows百万档race、独立复核及其余逐方法语义缺口保留，目标active；全部测试会话已退出，未提交或推送。


## Group 等待超时与同步通知交叉复核（2026-11-09）

反查 waitContext、recordToStream、executeTask、Reset 和 NoResult 等待委托，新增 wait_notification_contract_test.go，本轮未复现新的运行缺陷，没有修改运行实现。TestGroupBoundedWaitIncludesResultNotification 八组为原生/外部池 × 业务成功/失败 × WaitTimeout/已取消 WaitContext；TestNoResultBoundedWaitCountsSparseSlotsDuringNotification 四组为业务成功/失败 × 两种等待，共十二组。

同步结果回调被受控阻塞时，索引3的结果已保存且流已发送，快照长4但前3槽未占用；等待返回false，Active/Busy仍为1，成功计数尚未增加，流未关闭，Reset拒绝。等待中再次提交记录ErrGroupWaiting但不调用业务及通知；修改旧快照元素不改保存区，重复等待仍返回原值及新增拒绝；最终Wait必须等待通知释放，随后核对所有计数、流关闭、Reset清空状态/重建流/保留通知、第二批结果，以及外部池仍可接受任务。NoResult在通知阻塞时completed=4，TotalCount=1、OkCount=0；最终完成后仍返回4，确认第一个返回值含空槽且不是完成任务数。测试不穷举任意配置并发、同一索引多任务或所有拒绝路径。

最终强化版本Go1.26 race随机一百轮通过（4.796s）；Go1.25 amd64 race一百轮通过（group 6.746s/docs 2.442s），Go1.25 Windows386实际一百轮通过（3.374/1.695s）。普通Test实际一百轮，合并命令中的新Example每进程一次。Example_groupWaitDuringResultCallback单独Go1.26 race -v确认RUN/PASS（2.431s）。首版快照测试在追加拒绝引发扩容后才修改旧快照，未识别共享切片变体；调整为扩容前修改，强化后的三平台结果以上述最终值为准。

三个Go overlay错误变体分别返回共享结果切片、提前增加成功计数、把NoResult槽位数改为任务数，均被快照/通知/计数断言识别，无编译失败或测试超时计入；脚本.gotmp/check_group_notification_mutants.py及日志保留。第一次mutant运行通过不算识别，亦不算运行代码存在缺陷；它揭示的是首版断言对扩容的覆盖缺口。

专题与生成WaitContext/WaitTimeout/WithResultCallback注意事项补结果保存、流投递、同步通知及成功计数顺序，区分结果槽位与任务收尾；补快照浅拷贝及引用成员共享、超时后最终Wait再Reset建议，并纠正超时示例把快照直接称为已完成结果的注释。

72 Examples显式五次独立go test -race -count=1进程通过，docs/retry/sliceops依次6.375/4.301/6.256s、3.152/4.331/4.431s、3.814/3.424/4.628s、4.374/4.028/4.117s、4.335/4.616/4.155s；日志group-notification-examples-1.log至-5.log及名称清单保留。最终group/pool/docs/工具short audit race通过（48.030/22.755/7.217/2.663s）。1388声明调用/3完整Markdown程序、80原文局部片段编译、1415方法四项结构、368围栏语法、go vet和diff --check通过；更新本节前297本地锚点零未匹配。去注释运行diff仍仅九项既有修复。独立复核工具再次报missing message，未产生审查者或独立结论；Linux实际运行、Windows百万档race及其余逐方法语义缺口保留，目标active；全部测试会话退出，未提交或推送。

本节与索引写入后复验：300本地Markdown锚点零未匹配，1415方法结构、生成一致性、gofmt及diff --check通过。


## TokenBucket 补充额度与重试交叉复核（2026-11-09）

检查极端配置说明后，现有构造/生成参考已要求RateLimiter与Adaptive的int32/内存边界、有限正数TokenBucket配置；未凭缺少非法配置校验改变既有行为。新增bucket_timing_contract_test.go和token_refill_contract_test.go，未复现新的运行缺陷，没有修改运行实现。

FuzzTokenBucketTimedLedger用十六分之一令牌整数账本、四分之一令牌/秒速率和四分之一秒时间步，独立核对直接普通/Builder普通/直接三片/Builder三片四入口。覆盖正数及非正请求、Allow固定一个、AllowN小数、直接GetShard、拒绝也推进轮询、每片各自补充时间、容量截断、空闲额度丢弃、所有桶余额和lastRefill。参数限为速率0～8、容量0～8、最多64操作；二进制精确分数避免oracle重复浮点误差，不覆盖任意十进制舍入、非法浮点或所有并发历史。TestTokenBucketIdleCreditAndRejectedRequest共24组（直接/Builder × 速率0/0.25/1.5/8 × 容量0.5/1.5/4），验证初始满桶、64秒空闲后超容量请求拒绝、余额可随后整笔消费、不储备第二轮突发、下一秒重新补充。

首版四种子和24组Go1.26 race一百轮通过（4.687s）。四worker race fuzz在121,936次后minimizing阶段EOF异常退出，属于失败运行，不计通过且根因未证实；生成输入80a74fec92da48be单独race一百轮重放通过（5.470s），输入保留在testdata。降低GOMAXPROCS=2和parallel=1后race fuzz完成96,053次通过（42.934s）。错误变体检查发现首版种子没有强制触发不足一秒的补充断言，新增两个精确种子；最终六显式种子及保存输入的Go1.26 race一百轮通过（ratelimit 5.756s/docs 4.012s），Go1.25 race一百轮通过（5.986/4.108s），Go1.25 Windows386实际一百轮通过（1.542/3.170s）；合并命令里的Example每进程一次。最终六种子版本race fuzz 95,238次通过（35.388s，GOMAXPROCS=2/parallel=1），不能由降低资源后的通过推断首轮EOF确定无代码原因。

TestRetryTokenRefillDuringBackoff为Rate1/2 × Shards1/2 × 固定退避250/500/1000ms × 全失败/第三次业务调用成功，共24组公开Execute。五次尝试用独立四分之一令牌账本预测每片恢复与实际业务调用时间、总耗时、成功42、最后拒绝覆盖先前业务错误、最后业务失败保留错误链及最终零值。首版错误使用Shards(0)代表单片，失败后反查Shards明确将非正值展开为默认片数，修正测试为Shards(1)，不把测试假设错误算运行缺陷。最终Go1.26race一百轮通过（3.075s），Go1.25race一百轮通过（3.333s），Go1.25Windows386实际一百轮通过（2.901s）。

五个可编译Go overlay错误变体分别截断小数经过时间、越过桶容量积累、只在接受请求后更新时间、每次重试重建满桶、业务后额外停顿一秒；均被余额/时间/调用时刻断言识别，没有以编译失败或超时计入。脚本check_bucket_timing_mutants.py和check_retry_token_refill_mutants.py及日志保留；首版截断时间变体未识别不计成功，新增精确种子后的三项重跑均识别。

专题与生成Allow/AllowN注意事项补正数请求先补充再整笔判定、拒绝不扣量但更新余额和时间、非正请求不补充、不跨容量保存空闲积累、容量小于1的Allow永远拒绝但小数AllowN可用、计量单位与浮点舍入。Retry专题补250ms退避下仅0/1000ms执行业务的例子及Shards(0)/Shards(1)区别。新增Example_tokenBucketFractionalCapacity单独race -v确认RUN/PASS（4.239s）。73Examples显式五次独立进程race通过，docs/retry/sliceops依次7.548/6.518/7.092s、4.130/4.129/4.482s、4.154/4.013/4.133s、4.049/4.063/4.149s、4.292/4.074/3.558s，名称与日志保留。

最终新增Retry测试后的ratelimit/retry/docs/工具short audit race通过（7.343/4.860/7.837/3.055s）；前一包级版本（7.219/6.506/6.895/4.387s）不含该Retry新测试，以最终值为准。1388声明调用/3完整Markdown程序、80原文局部片段编译、1415方法四项结构、368围栏语法、go vet、gofmt和diff --check通过；本节写入前300本地锚点零未匹配。去注释运行diff仍为九项既有修复。独立复核接口既有缺口未消除，本轮未重复相同失败调用；Linux实际运行、Windows百万档race及其余逐方法语义缺口保留，目标active。所有会话已退出，未提交或推送。

本节与索引写入后复验：303本地Markdown锚点零未匹配，1415方法结构、生成一致性及diff --check通过。


## Pool 限时关闭与阻塞提交通知交叉复核（2026-11-09）

反查Close/CloseAndWaitTimeout、worker/processTask、enqueueTask/discardTask、recordToStream/completion、Wait及Reset。本轮未复现新的运行缺陷，未改运行实现。新增close_notification_contract_test.go中TestTimedCloseWorkerExitBeforeSubmissionNotifications四组：阻塞入队channel/等待MaxPending名额 × 全局通知和completion正常返回/各自panic。使用synctest和独立屏障，所有提交均经公开方法。

一个运行任务及一至两个排队任务占满池，下一次提交已登记并阻塞。首次限时关闭返回false；运行context、池context与调用方parent仍有效。释放业务后worker信号关闭，排队业务在本次受控历史中仍执行成功；关闭拒绝结果已保存并进入流，但提交调用者中的全局通知/completion仍可阻塞。分别验证两处屏障都阻止完整清理、重复限时关闭返回不同的完整清理通道、并发Close等待同一次收尾；此时FailCount尚为0，无MaxPending的阻塞发送仍占Pending=1，等待名额路径Pending=0。通知panic恢复后，提交返回ErrPoolClosed，流最终关闭且逐类结果数量正确，失败计数为1。随后Wait取得原业务结果并只取消池派生context，Reset清空结果/计数及重建流/worker，第二批成功。没有穷举任意关闭/配置/重置竞争。

首版测试错误要求worker退出后Pending始终为0，在普通channel阻塞路径失败；反查discardTask确认通知完成后才释放等待名额，修正断言而不改运行代码。错误变体首次取消context会先触发completion的错误断言，主测试却等待仅ErrPoolClosed触发的观察屏障而死锁，该次不算成功识别；把context检查提前至等待观察屏障前，最终三个可编译Go overlay变体（提前worker信号、跳过已登记提交收尾、Close额外取消context）均由行为断言识别，未计编译失败、超时或死锁。脚本check_pool_close_notification_mutants.py及最终日志保留。

最终逐类核对流结果的强化版本Go1.26 race随机一百轮通过（4.443s），Go1.25 amd64 race一百轮通过（4.718s），Go1.25 Windows386实际一百轮通过（3.612s）。前一阶段版本三平台也通过（7.782/8.440/7.645s），不代替最终强化版本。全部内部包及docs/工具short audit race随机一轮通过：core4.362、group49.053、maps5.208、pipeline23.814、pool23.421、ratelimit6.907、retry5.221、shard14.604、sliceops17.910、task5.729、docs7.527、工具6.002s；该次构建在最后的流分类断言之前，最终Pool/docs/工具随后再次short audit race通过（22.789/7.693/4.792s）。

Pool专题、生产指南和生成参考补完整清理等待提交调用者的同步通知、首次/重复关闭的通道差异、true/nil返回、Close不先取消context、统计快照及关闭后先Wait再Reset；纠正Size/QueueDepth表格。新增关闭局部示例纳入原文抽取编译，从80增至81组；本轮没有新增Example，73个可执行示例计数及显式五进程证据仍见上轮，新包级验证实际执行各例一次。1388声明调用/3完整Markdown程序、1415方法四项结构、369围栏语法、go vet和diff --check通过；索引更新前304本地锚点零未匹配。去注释运行差异仍仅九项既有修复。

独立复核工具再次在参数解析阶段返回missing message，未创建审查者；没有独立审查结论。Linux实际执行、Windows百万档race、此前四worker TokenBucket fuzz EOF原因及其余逐方法语义缺口继续保留，目标active。未提交或推送。

全库普通short audit最终退出0，根包435.389s，其余docs3.178、工具3.012、core3.045、group46.633、maps4.110、pipeline18.861、pool20.730、ratelimit5.152、retry3.644、shard12.474、sliceops10.037、task3.798s；日志.gotmp/pool-close-full-short.log。该命令构建早于最终流分类断言；运行实现相同，最后增强的测试由三平台定向及最终Pool包级race另外覆盖，不把该全库普通通过写成全库race通过。所有测试会话已退出。审查记录写入后复验307本地锚点零未匹配，生成一致性、gofmt及diff --check通过；两处状态查询表均已同步纠正。


## Core 等待辅助函数与监视生命周期交叉复核（2026-11-09）

从core.go的WaitTimeoutImpl/WaitContextImpl反查所有调用处，当前Pool/Group已有独立等待实现，不调用这两个辅助函数；纠正源码说明中仍称Pool使用submitGuard及Group传nil的过时文字。新增wait_lifecycle_contract_test.go，本轮未复现新的运行缺陷，没有修改运行实现。

TestWaitHelperOnDoneBarrier八组为timeout/context × 正常/超时返回 × 有无submitGuard：生产者未结束前无取消、标记或快照；WaitGroup完成后onDone按参数顺序执行，第一项受控阻塞时不能成功返回，超时则可以返回42快照且后续清理仍继续。取消与标记在快照前完成，cancelAll与复制各一次，guard由调用方预先设置；正常返回在两个清理函数后，提前返回不再次复制。TestWaitHelperTimeoutWaitsForSynchronousHooks六组阻塞cancel/cancelAll/copyResults，验证期限到达不能绕过同步回调；context路径cancel一次，timeout返回路径和AfterFunc可同时两次调用cancel，必须支持并发与重复。timeout的cancelAll由once合并，不保证调用前waited/submitGuard已经更新。

TestWaitHelperMonitorDoesNotStopCleanup六组为两入口 × 配置2s/0/-1s：提前返回后独立监视创建计时器，正数按配置、非正按三十分钟；五分钟间隔Warn、到有效时长一条Error后监视不再输出。默认三十分钟边界与第六次Warn同时就绪，允许五或六条Warn，不凭调度强制固定数。监视退出后仍未调用onDone，释放生产者后清理一次，快照不重取。TestWaitHelperCompletedCleanupStopsMonitor四组为两入口 × 生产者/onDone阻塞，提前返回后及时释放，跨越三十五分钟虚拟时间没有残留监视日志。合计24组，不覆盖任意配置并发、回调panic、全部取消/完成同时就绪历史或进程退出。

测试初版把timeout/cancelAll屏障后的time.Sleep交给虚拟时钟，但AfterFunc争用sync.Once底层mutex并非synctest的durably blocked等待，导致首轮120s及单独20s验证超时；堆栈显示测试主goroutine睡眠、两条取消路径各阻塞于屏障/once，不据此修改库。改为该组合直接检查屏障而不推进时钟，又发现AfterFunc可能先于标记更新，修正不成立的测试假设。首版错误变体虽触发默认期限断言，却因失败时没有释放生产者而bubble死锁，该次不计识别；增加失败路径的producerDone.Once清理后最终五个变体均由断言识别。

最终强化重复取消断言的24组Go1.26 race随机一百轮通过（4.616s），Go1.25 amd64 race一百轮通过（4.376s），Go1.25 Windows386实际一百轮通过（2.328s）。十四/二十/未强化取消次数的二十四组阶段结果不代替最终版本。最终core/docs/生成器short audit race通过（4.767/6.313/4.033s），日志core-wait-final3-packages.log。五个可编译Go overlay变体为done先于清理回调、逆序onDone、非正监视默认缩短至1s、复制快照两次、监视忽略完成信号；均由生命周期/顺序/时间/快照断言识别，无编译失败、超时或死锁计入，脚本check_core_wait_mutants.py及日志保留。

配置专题新增有效监视期限、监视退出不终止独立等待、同步回调可延迟返回及生产生命周期建议，新增保存/恢复配置示例纳入原文抽取，局部片段增至82组。生成参考补cancel并发/重复要求、取消与标记顺序、once和AfterFunc、onDone完成屏障、监视计时起点及非正回退；源码只修改注释。1388声明调用/3完整Markdown程序、1415方法四项结构、370围栏语法、82原文局部编译、go vet、gofmt及diff --check通过，记录写入前307本地锚点零未匹配。73个Examples本轮无新增，包级检查各执行一次；五个独立进程证据沿用既有记录，不增加次数。

去注释运行差异仍仅九项既有修复；本轮未重复全库长测，上一轮全库普通short及内部包race结果不能称为新增测试后的全库通过。独立审查工具的已知参数接口缺口未消除，本轮没有重复相同失败调用，也无独立审查结论。Linux实际执行、Windows百万档race、四worker TokenBucket fuzz EOF原因及其余逐方法语义缺口保留，目标active；所有测试会话已退出，未提交或推送。

记录写入后再纠正生成返回值通用说明：两个内部辅助函数没有拒绝重复/并发等待的门控；false只说明选中超时/取消分支，不证明生产者仍活跃；首值来自copyResults，本方法不再复制。最后生成/1388声明调用/3完整程序及生成器race（4.308s）通过；310本地锚点零未匹配、1415方法结构、go vet、gofmt和diff --check通过，全部会话终态。


## Task 等待提前返回与最终结果交叉复核（2026-11-09）

反查AsyncResult.getResult、Cancel、WaitTimeout、WaitContext、WaitCh、Go/GoAct及BoundedGo/BoundedGoAct。Mu仅有Append/Snapshot，既有锁外回调/重入/浅快照测试已覆盖，不根据不存在的索引或批量方法新增接口。本轮未复现新的运行缺陷，运行源码只纠正等待注释，没有修改实现。

新增wait_timing_contract_test.go。TestAsyncResultEarlyWaitPreservesBusinessAndNotifications共36组：四种执行入口 × 业务成功/原样返回错误/panic × Cancel/WaitTimeout/WaitContext。用屏障阻塞业务，分别核对1ms/2s/3s虚拟等待、零值及取消/超时错误、parent和业务context仍有效、业务只运行一次、Bounded许可仍占用。计时后建立三个独立WaitCh，释放业务后各收到一次相同值与错误身份并关闭；panic保存原值但不把原error接入errors.Is链。最终Wait取得原始结果，缓存优先于已取消WaitContext、负时长WaitTimeout及Cancel，查询一致且不再次执行业务；业务许可归还后可复用。TestAsyncResultCancelAcceptsGraceWindowResult共12组：四入口 × 三结果，业务在500us虚拟时间完成时Cancel返回实际结果，并与最终缓存一致。合计48组，不穷举所有调度、业务永不返回、引用成员修改或任意并发配置。

初版在计时前创建多个WaitCh，sync.Once的mutex争用不属于synctest的durably blocked等待，导致120s/15s验证超时；堆栈显示业务屏障和结果读取/Once争用，不把该测试设计问题算库缺陷。调整为计时断言后才创建通知通道。最初无等待窗口错误变体触发断言后，业务仍在虚拟睡眠，bubble退出报deadlock，该次不计识别；加入失败路径的虚拟时间推进及收尾后，最终四个Go overlay变体均可编译并由断言识别：取消窗口删除、丢弃Cancel缓存结果、超时返回错误类型错误、WaitCh丢失业务错误。没有把编译失败、超时或死锁计入成功识别；脚本check_task_wait_mutants.py及四份日志保留。

最终含失败清理的48组Go1.26 race随机一百轮通过（3.385s），Go1.25 amd64 race一百轮通过（2.747s），Go1.25 Windows386实际一百轮通过（0.966s）。前一未加失败清理版本通过不代替最终版本。task/docs/生成器short audit race通过（3.923/5.568/2.079s），日志task-wait-final-packages.log。

专题与生成参考补Cancel的1ms定时窗口及非硬性返回上限、同时就绪不保证分支、等待取消不覆盖缓存或释放业务许可、带ok方法区分业务context错误、WaitCh各建通知/一次结果/关闭/未完成资源生命周期。WaitTimeout/WaitContext补已缓存或立即可读结果优先，包括非正时长和已取消context；ok代表取得结果而不是业务成功。源码WaitTimeout/WaitContext原来的“是否在超时/取消前完成”注释改成“是否取得任务结果”，避免缓存优先时的误解。新增Cancel后等待与取消context读缓存两个局部示例，84原文片段编译通过，没有新增Example（仍73个）；本轮包级各执行一次，显式五进程证据沿用既有记录。

1388声明调用片段与3完整Markdown程序编译、1415方法四项结构、372围栏语法、go vet及diff --check通过；索引更新前310本地锚点零未匹配。结构、语法和编译检查不等同于每条说明语义已经无误。去注释运行差异仍为九项既有修复。未重跑全库长测，上一轮全库普通short及内部包race不能称为本轮新增测试后的全库通过；独立审查接口缺口、Linux实际执行、Windows百万档race、四worker TokenBucket fuzz EOF原因及其余逐方法语义缺口保留，目标active。全部测试会话已退出，未提交或推送。

最后源码注释及生成说明写入后，task/docs/生成器short audit race再次通过（4.628/5.066/2.199s），日志task-wait-final2-packages.log；生成一致性、1388调用/3完整程序编译、1415方法结构、372围栏、313本地锚点、go vet及diff --check通过。所有会话终态。


## Slice 过滤所有权与异常传播交叉复核（2026-11-09）

反查sliceBase及旧Slice的索引修正、重叠插入、Replace、Grow和视图方法。现有mutation/overlap/ownership模型已覆盖主要边界，本轮聚焦filterItems/filterSerial/filterParallel及Filter/DeleteFunc调用，未复现新的运行缺陷，没有修改运行实现。

新增filter_ownership_contract_test.go。TestFilterThresholdSlotOwnership最终120组为八路径 × 长度0/1/999/1000/1001 × 全拒绝/全保留/奇数匹配；路径含Builder三worker/单worker、显式Parallel三worker/单worker、Serial、旧Slice、Builder/Parallel的DeleteFunc。使用前后各两槽的指针元素输入，逐项原子记录正常返回时恰一次调用，独立构建预期保留列表及整个输入数组。核对稳定顺序、非nil空结果、阈值处结果是否复用原槽位、未处理尾部不清零、指针对象共享及替换返回槽位的可见性。调用方context预先取消，部分路径设置1ns Timeout/FailFast，数据方法仍全部遍历；没有启动业务终端，不据此外推任务调度行为。

TestSerialFilterPanicKeepsOldHeaderAndPartialWrites四组覆盖Builder/Serial/Parallel小输入/旧Slice：输入[0,1,2,3,4]保留奇数并在3处panic，原panic对象向外传播且调用顺序为0/1/2/3；原长度5及数组身份保留，但数据已为[1,1,2,3,4]。recover后再过滤得到[1,1,3]，明确不回滚。TestParallelFilterPanicExitsProcess四组覆盖Builder/Parallel × Filter/DeleteFunc，在1000项时由子进程验证worker panic标记、非零退出且调用方recover未触发；15s仅为进程存活保护，不把超时或任意非零退出算通过。全部128组不覆盖所有非法回调、输入并发改写、外部副作用或任意内存分配失败。

最终增强为120项所有权矩阵后的Go1.26 race随机二十五轮通过（4.929s）、Go1.25 amd64 race二十五轮通过（5.176s）、Go1.25 Windows386实际二十五轮通过（2.841s）。前一60项矩阵及其阶段包结果不代替最终版本。四个Go overlay错误变体分别延后1000项阈值、并行结果复用输入、串行额外清尾、同步吞掉panic，均由所有权/数组/异常断言识别，无编译失败、超时或死锁计入；最终脚本check_filter_ownership_mutants.py和四份日志保留。

专题新增过滤数组所有权与异常段，明确Worker(1)仍由worker执行、稳定顺序不等于槽位独立、Values在过滤后不能撤销对输入的改写、同步panic时长度与部分写入、worker panic导致进程退出以及过滤前复制的生产建议。生成器实际优先走sliceDataCaution，故在slice_data_semantics.go补专用共享说明并由通用分支复用，实际生成的旧Slice.Filter与sliceBase.Filter/DeleteFunc均已反查；没有只修改被前置分支遮蔽的文案。源码仅新增所有权与panic注释。新增原文复制槽位示例纳入85组局部编译并通过。

新增Example_sliceFilterOwnership，Go1.26 race -v实际RUN/PASS（1.785s），Go1.25 race -v一次通过（1.894s）、Go1.25 Windows386实际一次通过（0.860s）。共74 Examples显式五次独立go test -race -count=1进程调用通过，docs/retry/sliceops依次2.007/1.919/2.057s、2.176/1.946/2.034s、2.038/2.456/2.449s、2.478/2.183/2.355s、3.215/2.133/2.287s；日志filter-ownership-examples-1.log至-5.log及名称清单保留，未以-count重复计算Example。

最终sliceops/docs/生成器short audit race通过（13.849/6.548/2.321s），日志filter-ownership-packages.log。1388声明调用片段和3完整Markdown程序编译、1415方法四项结构、373围栏语法、go vet及diff --check通过；本节写入前313本地锚点零未匹配。去注释运行差异仍为九项既有修复。未重跑全库长测，前轮全库结果不是本轮新增测试后的全库通过。独立审查工具再次在参数解析时缺少message，未创建审查者，无独立结论；Linux实际执行、Windows百万档race、四worker TokenBucket fuzz EOF原因及其余逐方法语义缺口保留。结构/语法/编译不能证明逐方法语义无误，目标active。全部会话已退出，未提交或推送。

本节与索引写入后复验：316本地Markdown锚点零未匹配，生成一致性、1415方法结构、373围栏语法及diff --check通过。


## Slice 与 Map 模式切换状态交叉复核（2026-11-09）

反查SliceBuilder.Serial/Parallel、SerialSlice.ToParallel、ParallelSlice.ToSerial及MapChain/MapSerial/MapParallel的四个对应入口，策略和共享数据构造均已有明确Seq/DefPar重置实现；生成参考已说明不继承策略。本轮补源构建器状态、字段引用和池生命周期的交叉证据，未复现新的运行缺陷，没有修改运行源码或实现。

新增两个mode_switch_contract_test.go。Slice的TestSliceModeSwitchSnapshotsHeaderAndResetsPolicy为四入口 × nil/有容量空视图/偏移非空视图 × 无池/借用/接管三状态，共36组；Map的TestMapModeSwitchSharesDataAndResetsPolicy为四入口 × nil/可写空map/非空map × 三池状态，共36组。切换前保存源policy，核对目标等于Seq/DefPar且源不变、context身份及数据形状保留；目标公开Map成功且业务context无源的1ns任务deadline、原context值保留，执行后源配置和输入保持。

Slice检查当前len/cap/nil及整个数组身份，目标改写槽位后源可见，目标缩短视图不改变源长度，目标policy修改不影响源，源context字段重置不更新目标。Map检查目标键写入与源删除互相可见，目标map字段重赋值不改变源集合；目标配置和源context字段重赋值分别不联动。原池不论借用或接管，在切换及目标Map后仍可接受公开提交并完成，由测试所有者统一Close。ToParallel从Serial来源的池已在更早Serial切换时丢弃引用，本测试不把它称为Serial持有并转移池；直接SliceBuilder/MapChain及ToSerial路径覆盖实际持池源。测试不穷举任意连续转换、并发配置、共享输入竞态或每个终端的池收尾组合，旧配置的源终端未运行，不外推其执行语义。

最终72组Go1.26 race随机一百轮通过（sliceops2.402/maps2.588s）、Go1.25 amd64 race一百轮通过（2.318/2.333s）、Go1.25 Windows386实际一百轮通过（0.757/0.982s）。四个可编译Go overlay变体分别让Slice继承旧policy、Slice额外复制槽位、Map丢失context、Map切换修改源policy，均被策略/形状/上下文断言识别，无编译失败、超时或死锁计入；脚本check_mode_switch_mutants.py与四份日志保留。复制槽位变体首先在空视图形状断言被识别，不将它写成所有子用例均走到底层数组身份断言。

专题新增切换后的视图与配置说明和原文示例，区分源配置保留、目标重置、切片头独立与共享数组、map键集合共享、context引用保留与后续字段赋值不联动、旧池不自动关闭。生成Slice/Map四种切换说明同步补无参数、返回新构建器及生产责任。新增原文片段纳入编译后共86组，均通过。新增Example_builderModeSwitchViews显示Slice源长3/目标长1而槽位共享，目标不继承源任务超时，Map双方增删可见；Go1.25 race -v确认RUN/PASS（1.949s）及Windows386 -v实际一次通过（0.716s），Go1.26由全套与包级各执行。

共75 Examples显式五次独立go test -race -count=1进程调用通过，docs/retry/sliceops依次2.492/1.958/2.254s、1.885/2.022/2.772s、2.241/1.770/2.427s、2.458/2.347/2.243s、2.249/1.780/2.498s；日志mode-switch-examples-1.log至-5.log及名称清单保留，不将-count作为Example重复次数。最终sliceops/maps/docs/生成器short audit race通过（15.438/2.860/5.302/1.930s），日志mode-switch-packages.log。

1388声明调用片段和3完整Markdown程序编译、1415方法四项结构、374围栏语法、go vet及diff --check通过；本节写入前316本地锚点零未匹配。去注释运行差异仍为九项既有修复。未重跑全库长测，前轮全库结果不是本轮新增测试后的全库通过。独立审查工具的已知接口缺口未消除，本轮未重复相同失败调用，未获得独立审查结论；Linux实际执行、Windows百万档race、四worker TokenBucket fuzz EOF原因及其余逐方法语义缺口保留。结构/语法/编译不能证明逐方法语义无误，目标active。全部测试会话已退出，未提交或推送。

本节后实际运行全部内部包及docs/生成器short audit race随机一轮，退出0：core2.581、group47.028、maps2.846、pipeline20.647、pool20.883、ratelimit5.067、retry3.985、shard13.400、sliceops14.008、task5.364、docs5.086、生成器1.955s，日志mode-switch-all-internal-race.log。这次包含最近Core/Task/过滤/模式切换新增测试；命令范围不包含根包，也不含非short长测，不称为全库race通过。记录与索引写入后319本地锚点零未匹配、生成一致性、gofmt及diff --check通过。所有会话已终态。


## Retry 最大次数计数与耗尽溢出修复（2026-11-09）

数值边界反查确认两项运行缺陷，均先写失败回归再修复。其一，maxRetries为最大int时，RetryWithBackoff/Linear/Config及其Void/Result包装和BindRetryToWorker在第一次失败日志中已将总预算显示为负数；amd64实测为-9223372036854775808。TestRetryAttemptCountBoundary为九入口 × MinInt/-1/0/1/MaxInt-1/MaxInt × 失败后成功/失败后取消，共108组；预算oracle使用math/big，检查调用次数、成功值、取消、耗尽错误、日志消息和error字段身份。修复前18个MaxInt子例失败，日志attempt-count-before.log。

日志和耗尽错误的全部n+1格式化处改为先转uint再加1，兼容32/64位，不截断预算或改变重试、退避、成功及取消优先级；没有引入额外分配或共享计数。耗尽错误的MaxInt显示未通过完整直接重试路径实际执行，正确性依据同类表达式及常规耗尽契约，不把提前结束的日志测试当成全预算耗尽验证。

其二，RetryFn.WithRetry的最后一次失败仍会执行for的attempt++，MaxInt溢出后继续调用。新增audit标记的TestRetryFnMaxIntExhaustion386：在Go1.25 Windows386非short中实际允许2,147,483,648次失败，以额外一次成功作为错误实现的逃逸条件。修复前实测2,147,483,649次并返回nil（9.393s，回调循环7.31s），日志attempt-exhaustion-before.log。修复仅在最后失败保存lastErr后、计数递增前break，正常预算不变，错误包装仍为retry exhausted。修复后同环境连续三轮完整预算通过，共27.201s，日志attempt-exhaustion-after.log；64位和short按条件跳过，未声称实耗64位上限。新增46组TestRetryFnAttemptBoundaryPreservesOutcome覆盖MinInt/-1/0/1/4/MaxInt、成功发生位置、普通错误及panic；MaxInt仅有限提前成功，完整耗尽另由386测试覆盖。

最终154组有限契约Go1.26 amd64 race随机一百轮通过4.687s，Go1.25 amd64 race一百轮4.908s，Go1.25 Windows386实际一百轮3.413s；日志retry-extreme-go126/go125/386.log。四个可编译overlay错误变体：Backoff、Config、Worker日志恢复有符号加法，RetryFn删除最后失败的break；前三者被公开日志断言识别，第四个以386完整预算重现额外调用后失败（8.517s），无编译错误、超时或死锁计入。脚本check_attempt_count_mutants.py与四份日志保留于.gotmp。

最后修复后的全内部包、docs和生成器short audit race随机一轮全部通过，日志retry-extreme-all-internal-race.log：core4.225、group49.749、maps5.230、pipeline23.112、pool23.358、ratelimit6.953、retry6.755、shard14.552、sliceops17.576、task5.463、docs6.345、工具3.223s。此命令不含根包和非short测试。更早attempt-count-all-internal-race.log仅含计数显示修复，不作为RetryFn修复后的证据。

专题与生成参考补充无符号总预算、有限生产次数、父期限、RetryFn无取消/退避及最后失败退出说明，删除以规避n+1溢出为前提的旧建议。1388声明调用片段、3完整Markdown程序及86原文局部示例编译，1415方法四项结构、374围栏语法、go vet和diff --check通过；结构/语法/编译仍不等于每个方法语义无误。既有九项加本轮两项，共十一项运行修复，未提交或推送。

本轮按requesting-code-review技能尝试独立审查，但collaboration spawn接口仍只允许空对象，调用在创建前报missing field message，未获得独立审查结论。Linux实际执行、Windows百万档race、此前四worker TokenBucket fuzz EOF原因和其余逐方法语义缺口仍保留；目标保持active。全库含根包short测试已启动，结果在终态后追加，不能将当前内部包结果称为全库通过。

全库short audit随机一轮随后全部通过，退出0，日志retry-extreme-full-short.log：根包436.886、docs4.471、生成器3.060、core1.264、group46.003、maps2.804、pipeline16.626、pool18.936、ratelimit3.751、retry2.956、shard2.869、sliceops7.823、task2.229s；test目录无测试。该次包含两项重试运行修复及其全部有限契约，386完整耗尽测试因平台/short条件跳过，单独证据见上。最后记录写入前322本地Markdown锚点零未匹配，所有测试会话已终态。此结果是全库short普通执行通过，不等同于全库race或全库非short通过。


## 限流作用域关闭与在途业务交叉复核（2026-11-09）

五个With*便捷入口已有TestLimiterScopeClosesOnEveryOutcome覆盖成功/错误/panic后关闭。本轮新增TestLimiterScopeClosesWaitersWithoutJoiningBusiness：四种定时限流入口 × Block/BlockForce × 三结果，加分片Adaptive固定Block × 三结果，共27组虚拟时间契约。每组耗尽所有片可用许可，启动被业务门控制的持有者和每片一个等待获取者，synctest.Wait建立等待后才退出作用域。BlockForce的等待context预先取消仍保持阻塞，其他Block的context维持有效至断言结束。

核对回调仅一次、错误/panic身份保持；作用域退出后各片关闭、refillDone已完成、等待者返回ErrRateLimiterStopped，而业务门仍未放行且业务未完成。随后放行业务，其Release安全完成且关闭实例余额仍为0，逃逸实例再次Acquire失败，重复Close安全返回。测试只证明该门控业务不被自动等待，不穷举业务自建goroutine、外部回调、并发重复关闭及跨片逐步关闭时序。首版测试错误地在回调defer取消context，造成关闭前就可能取消等待者；这是测试建模错误，已改为t.Cleanup，在运行实现未改的条件下最终通过，首版失败不列为运行缺陷。

Go1.26 amd64 race随机100轮4.530s、Go1.25 amd64 race100轮4.535s、Go1.25 Windows386实际100轮2.664s；日志scope-waiter-go126/go125/386.log。三个可编译overlay变体吞错误、吞panic和回调前关闭均被结果身份或获取错误断言识别，未将编译失败/超时/死锁作为成功识别。脚本check_scope_waiter_mutants.py及三日志保留。

ratelimit/retry/docs/生成器short audit race随机一轮通过6.459/5.999/7.665/4.234s（scope-waiter-packages.log）。生成参考补充等待唤醒与业务责任；专题新增“限流器作用域与业务完成”，说明五便捷函数位于internal包且根包无同名入口，外部通过构建器创建实例并自行管理Close。未添加不可访问的外部调用示例，原有1415方法四项结构与1388调用片段保留，3完整MD程序编译、374围栏语法、go vet、322本地锚点及diff --check通过。未新增Example，仍75例，前轮86原文片段编译结果不因本次新增纯说明文字改变。

本轮未复现新的运行缺陷，没有改运行实现，累计十一项修复。两项重试修复后的全库short普通执行已在retry-extreme-full-short.log终态通过；本轮新增27组测试晚于该全库运行，只以当前定向及包级race证据覆盖，不冒称新增后的全库通过。独立审查接口、Linux实际执行、Windows百万档race及此前fuzz EOF解释缺口仍保留，目标active。全部测试会话已退出，未提交或推送。


## Adaptive 初始容量中点溢出修复（2026-11-09）

NewAdaptiveRateLimiter使用(minRate+maxRate)/2，即使两个边界各自符合正int32约束，在32位相加仍可溢出。新增audit标记非short/386实际构造回归，最小1、最大MaxInt32，应初始化1,073,741,824个许可；修复前初始容量回退到默认IO()=56（4.634s，adaptive-midpoint-before.log），current还会保存负中点。修复前失败已观察，运行表达式改为minRate+(maxRate-minRate)/2；归一化后1<=min<=max，差值及最终和都不超过max，因此不溢出且保留向下取整，未改变公开签名或正常参数含义。容量字段仍为int32，本修复不将超出其范围的容量定义为合法生产配置。

修复后初版单路径Go1.25 Windows386实际初始化及Acquire/Release余额验证通过28.109s（adaptive-midpoint-after.log）。最终TestAdaptiveMidpointInt32Boundary386扩为直接/普通构建器/单片分片直接/单片分片构建器四入口，均实建同样大容量、核对Size/Available与获取/归还，合并69.893s（adaptive-midpoint-four-paths-386.log）；amd64及short明确跳过，不以跳过称32位实测通过。没有按MaxInt64实际初始化，巨大容量会消耗显著启动时间，不能作为推荐生产参数。

新增TestAdaptiveInitialMidpointAcrossConstructors，12组非正/逆序/相等/奇偶上下限 × 四构造入口，共48组。检查归一化min/max、atomic current、公开Size/Available，实际耗尽初始许可后取消获取、全数Release恢复容量。独立小整数相加oracle保留旧中点取整语义。Go1.26 amd64 race随机100轮4.467s、Go1.25 amd64 race100轮4.536s、Go1.25 Windows386实际100轮1.838s（adaptive-midpoint-go126/go125/386.log）。三个可编译overlay变体恢复有符号中点相加、改为向上取整、丢最小值偏移，分别被386真实构造及48组状态断言识别，未把编译错误/超时算通过；脚本check_adaptive_midpoint_mutant.py与日志保留。

独立整数调整模型FuzzAdaptiveAdjustmentThresholds，在GOMAXPROCS=2、race、1worker、30秒配置下通过3,476次执行，35.799s（adaptive-midpoint-fuzz.log）。该模型仅小边界调整和记录，不代表对全部int32状态fuzz；后半段执行数无增长但进程终态PASS，未声称高吞吐或额外执行。最终全内部包/docs/生成器short audit race通过，日志adaptive-midpoint-all-internal-race.log：core4.224、group49.253、maps5.267、pipeline22.324、pool23.383、ratelimit5.806、retry5.311、shard12.773、sliceops15.830、task5.880、docs7.563、工具3.339s。

含根包全库short audit普通随机一轮全部通过，退出0，adaptive-midpoint-full-short.log：根包435.732、docs4.342、生成器2.464、core3.244、group48.366、maps4.153、pipeline19.438、pool21.227、ratelimit4.088、retry5.385、shard3.596、sliceops9.572、task4.418s，test目录无测试。此版本包含48组和27组限流作用域契约；大容量测试在short中跳过，其四入口版本晚于全库启动，但386独立实际运行如上。不是全库race或全库非short验证。

75 Examples显式五次独立race进程均通过，docs/retry/sliceops依次4.790/4.802/4.286s、2.540/4.351/3.574s、3.429/2.299/3.272s、4.023/4.021/4.113s、4.447/4.022/3.435s（adaptive-midpoint-examples-1.log至-5.log），未用-count冒充Example重复执行。专题和生成参考改为无溢出中点，注明正int32范围、下游预算、容量初始化耗时；源码注释同步。审查结果当前摘要和执行计划修复清单由九项更新至十二项，并补表中遗漏的三项新修复。

最终1388调用片段、3完整MD程序及86原文局部片段编译，1415方法四项结构、374Go围栏语法、go vet及diff --check通过。逐方法语义未据结构检查宣告全部完成。独立审查spawn仍在创建前缺message参数失败，本轮无独立审查结论；Linux实际执行、Windows百万档race及此前四worker TokenBucket fuzz EOF原因等缺口继续保留。目标active，所有测试会话已退出，未提交或推送。


## Slice 极端范围与视图容量交叉复核（2026-11-09）

检查Take/Drop/SliceRange边界源码，与已有混合变更模型交叉；先前模型偏重有限逻辑值，本轮新增TestSliceViewExtremeBoundsPreserveArrayAndHeader验证完整切片头和外部数组。四入口Builder/Serial/Parallel/旧Slice × nil/有容量空/偏移非空 × Take九数量、Drop九数量、SliceRange九乘九端点，共1188组；Builder兼容Slice额外243组，总1431组。数量端点为MinInt/-1/0/1/2/4/5/6/MaxInt，非空长度5。

核对返回原接收者、长度、容量、nil性、全部值、数组起点身份、整个padding数组不变和外部切片头保留；非空结果写首槽后原输入及旧视图可见。nil形状按构造后的before作基准，旧Slice构造规范化nil，不误写成所有入口都保留nil输入。范围oracle不进行越界端点相加，非空范围再按安全的小长度构造期望。未穷举连续混合截取、引用内容、并发操作和所有纯数据方法；原有混合变更fuzz另有覆盖。

Take非正、Drop过大及SliceRange修正后i>=j均使用原视图[:0]清空，保留原起点和容量且不清零；有效非空Drop/SliceRange才移动起点并减少cap。极端端点不会因相加溢出改变规则。专题补生产复制/Clip/旧引用责任，生成纯数据方法和兼容Slice注意事项同步。未发现新的运行缺陷，没有改运行实现或公开签名，仍累计十二项修复。

最终1431组Go1.26 amd64 race随机25轮4.992s、Go1.25 amd64 race25轮3.037s、Go1.25 Windows386实际25轮2.014s（view-bounds-final-go126/go125/386.log）。旧view-bounds-go126/go125/386.log在兼容Slice243组加入前，不当作最终矩阵证据。四个可编译overlay变体把Drop空视图移到末尾、Take限制cap、SliceRange复制槽位、兼容Slice错误委托Drop，均被视图形状/容量断言识别；复制槽位首先由形状断言识别，不声称每个身份断言都触发。脚本首版Take替换串匹配两处而中止，已收窄唯一匹配后四例全部完成，脚本准备失败不记运行缺陷。

最终sliceops/docs/生成器short audit race通过14.774/5.220/2.673s（view-bounds-packages.log）。1388调用片段和3完整MD程序编译、1415方法四项结构、374围栏语法、go vet及diff --check通过；纯说明新增未改变86原文局部代码片段，本轮未新增Example，仍75。第十二项修复后的全库short普通已通过（adaptive-midpoint-full-short.log），本轮新增矩阵晚于该全库测试，只以定向和包级证据覆盖，不称新增后的全库通过。

独立审查spawn再次在创建前因message字段无法传入而失败，无独立结论。Linux实际执行、Windows百万档race、此前四worker TokenBucket fuzz EOF原因及余下逐方法语义仍未闭合；结构编译通过不证明无任何bug。目标active，所有测试会话已终态，未提交或推送。


## RingBuffer 引用清理与极端批量交叉复核（2026-11-09）

现有FIFO与并发历史模型使用int元素，本轮新增TestRingBufferDrainsReleaseSlotsAndPreserveReturnedReferences直接检查指针槽位，不依赖GC或finalizer。三容量1/3/5 × 四策略Block/Drop/Error/未知99 × 五形状空/部分/满/绕回/溢出 × 八批量数量MinInt/-1/0/1/cap-1/cap/cap+1/MaxInt，共480次子测试调用；容量1的数量0/1有重复，统计为实际生成调用数而非480个完全不同输入。

独立FIFO指针队列逐步核对Push接受/拒绝、Drop计数、Peek/Pop/FlushN/Flush、重复Reset与复用。检查固定存储数组身份不变，每个存储中的非nil指针必须仍在逻辑队列，每个存活对象仅有一个槽位；Pop/批量移除及Reset后无过期引用残留。返回批量切片必须非nil且顺序/身份保持，修改引用对象内容在原对象可见，替换返回槽位不改变内部存储；随后Reset不改返回切片。FlushN非正或超过当前长度均只按现有元素数读取，不按MaxInt分配；未知策略只在满时拒绝。

此测试访问内部槽位以证明持有关系，不声称被移除对象立即被垃圾回收，不穷尽并发GC、对象图、nil元素/重复对象指针和所有容量。并发线性化由既有历史模型另证。Go1.26 amd64 race随机25轮5.096s、Go1.25 amd64 race25轮4.820s、Go1.25 Windows386实际25轮2.190s（ring-reference-go126/go125/386.log）。

四个可编译overlay变体分别删除Pop、FlushN、Reset的清零，以及负FlushN返回空列表，均被过期存储引用或FIFO批量断言识别；无编译失败或超时计入。check_ring_reference_mutants.py及四日志保留。初次文档补丁因匹配源码条件不精确而未应用，核对当前条件后重施成功，无运行源码变更。

core/pool/docs/生成器short audit race随机一轮通过1.925/22.334/5.720/2.470s（ring-reference-packages.log）。配置专题和实际优先级生效的core生成分支补清零、返回切片、指针内容、极端批量及统计快照说明；不向外部示例加入不可访问的NewRingBuffer。1388声明调用片段和3完整MD程序编译、1415方法四项结构、374围栏语法、331本地锚点、go vet及diff --check通过。新增纯文字不改变86原文局部代码片段和75可执行Example，本轮未重跑全部原文片段及五进程示例。

无新运行缺陷，累计十二项修复。第十二项版本全库short普通已有终态通过，但本轮480组测试晚于该全库启动，只以当前定向/包级race结果覆盖。Linux执行、Windows百万档race、此前四worker TokenBucket fuzz EOF原因、独立审查接口及逐方法余下语义缺口仍保留，目标active。全部测试会话已终态，未提交或推送。


## NoResult 聚合槽位与最小负目标溢出修复（2026-11-09）

先审查Group记录与NoResult聚合、FillNoResultSkipped及既有稀疏测试。初始378组中等目标组合通过；检查负边界发现原表达式int64(total)-g.totalCnt.Load()在amd64下MinInt减非零总数发生有符号溢出，正need使原本无操作的调用进入巨量填充。新增TestFillNoResultSkippedMinimumTargetDoesNotGrow，单个成功任务最终Wait后传MinInt，在隔离子进程监测原子总数；首次错误增长即退出3，不等待MaxInt64循环或OOM。修复前实际失败3.177s，输出minimum negative fill target grew task count（aggregate-target-before.log）。这证明错误增长已发生，不声称实耗整个追加预算。

运行修复只改FillNoResultSkipped：在持有同一结果锁时读取current，先判断int64(total)>current，再计算差值。现有总数从0开始，仅增加或在Reset置0；在此非负计数不变量下，比较后的减法不会因负目标溢出。正数目标、升序填空、追加顺序、ErrSkipped、计数、回调/stream规则和公开函数签名保持。补源码注释原先过度承诺的“确保TotalCount==total”，明确较小目标无操作。32位int目标转换int64后原表达式并不触发这个特定溢出；386用于兼容性验证，不能当作原缺陷的复现平台。

TestNoResultAggregateSparseDuplicateAndSkippedSlots最终486组=三入口直接构造/构建器/外部池MaxResults1 × 六提交历史（含空、稀疏、重复及普通Go混合）× 三结果模式成功/交替业务失败/取消拒绝混合 × 九目标MinInt/-MaxInt/-1/0/1/3/5/6/9。每次提交等待结果通知后再提交下一项，以受控完成顺序的独立逻辑槽位模型核对；没有用freeIndices或查询结果生成期望。检查聚合Occupancy、长度、错误身份和槽位首错，Errors/JoinErrors的顺序和重复项，统计、Active/Busy、调用及通知次数；填空/追加、重复较小目标、浅切片独立、已关闭流不增加事件、Reset和复用。该模型不穷尽并发交错、所有索引和巨大正目标。

最终486组及隔离回归Go1.26 amd64 race随机25轮31.886s、Go1.25 amd64 race25轮32.833s、Go1.25 Windows386实际25轮3.783s（aggregate-slots-final-go126/go125/386.log）。四个可编译overlay分别恢复溢出表达式、计入空槽、返回内部切片、删除失败计数，均被运行断言识别，不计编译失败和超时（check_aggregate_slots_mutants.py和aggregate_slots_mutants目录）。

第十三项版本全内部包/docs/工具short audit race通过：core9.131s、group54.364s、maps9.335s、pipeline26.034s、pool26.218s、ratelimit7.781s、retry9.432s、shard17.574s、sliceops16.343s、task6.623s、docs7.606s、工具4.196s（aggregate-slots-internal-race.log）。后来只改文案并增加公开Example，最终docs/工具short audit race再验7.925/3.990s（aggregate-slots-final-docs-race.log）。

补Group专题及两种函数的参数/生产建议/注意，区分任务数与空槽、错误顺序变化、通知不补发、浅拷贝和巨大目标的分配责任。最初新增专题误写两辅助函数不由根包导出，核对async.go:244/248发现实际导出别名后已纠正；未保留错误说法。新增Example_noResultExtremeTargetAndFirstError实际调用公开函数，验证MinInt无操作、补齐后首错变ErrSkipped且业务失败保留、重复补齐不重复通知。新例Go1.26 race4.218s、Go1.25 race4.239s、Go1.25 Windows3862.080s通过。

现在76个可执行Examples显式五次独立race进程通过，docs/retry/sliceops分别4.938/4.371/4.353s、4.333/3.044/4.225s、3.145/4.827/5.035s、4.507/4.383/4.169s、2.256/3.454/4.579s（aggregate-slots-examples-1.log至-5.log）。1388声明调用片段及3个完整MD程序重新编译；86组原文局部片段编译、1415方法四项结构、374围栏语法、335本地锚点、go vet和diff检查通过。后续新增审查索引会增加锚点数量，最终检查以追加后的日志为准。

按requesting-code-review尝试独立审查，工具仍在创建前报missing field message，未创建审查者，不能记作独立审查。Linux实际执行、Windows百万档race、此前四worker TokenBucket fuzz EOF原因、独立审查接口及逐方法余下语义缺口继续保留。完整目标active，未提交或推送。

第十三项修复后的全库short普通终态已取得：go test -short -tags audit ./... -count=1 -shuffle=on -timeout=900s退出0，根包438.973s，docs3.530s、工具2.990s、core3.278s、group47.747s、maps6.216s、pipeline20.713s、pool20.843s、ratelimit5.266s、retry4.767s、shard5.249s、sliceops9.871s、task5.611s（aggregate-slots-full-short.log）。包括当前486组和极端负目标回归；新增第76个Example晚于该全库进程启动，以后来三环境执行、五独立race进程和最终docs包级race覆盖。最终338本地锚点、1415方法结构、374围栏、生成一致性、go vet和diff检查通过。所有本轮测试会话均已终态。


## MultiGroup 混合路由与计数回绕交叉复核（2026-11-09）

本轮从multigroup.go、Group.Shard、MultiGroupBuilder及现有测试反查路由与统计。GoKeyed参数为uint64，不存在直接负key；专题把它笼统称为哈希路由，实际是key%片数，没有额外哈希。原有测试包括单个高计数提交与普通key，但没有完整混合路由/拒绝历史。未复现新的运行缺陷，本轮只改注释、文档和验证代码，累计十三项运行修复保持。

新增TestMultiGroupMixedRoutingAcrossCounterWrapAndRejection共54组：三构造入口Group.Shard/MultiGroupBuilder.Run/WithMultiGroup × 1/3/5片 × 无关闭片/预先关闭第0片 × 初始轮转计数0/2^63/MaxUint64-2。每组18项混合普通Go和GoKeyed，key含0/1/2^63/MaxUint64/MaxUint64-1/42，交叉业务失败及已取消context。big.Int独立进行模2^64计数及模片数，不复用库内路由表达式；每个有通知路径等结果回调保存确认后再提交下一项，控制片内完成顺序而不用sleep。

验证GetShard极端int越界、每项目标片、Go拒绝也消耗轮转、GoKeyed不推进轮转；已关闭片优先返回ErrGroupWaited且不通知，未关闭片的已取消任务记录context.Canceled且通知；拒绝不执行业务。按受控记录历史核对每片结果和总数，重复Wait按片号合并，Values/Errors顺序与切片槽位独立，TotalTask/Fail/Ok/Active/Busy及业务执行次数一致。该模型不穷尽并发提交/配置、流式转发、所有计数和任意key；已有其他生命周期与通知契约仍单独适用。

Go1.26 amd64 race随机100轮4.939s、Go1.25 amd64 race100轮5.566s、Go1.25 Windows386实际100轮12.249s（multigroup-routing-go126/go125/386.log）。四可编译overlay把轮转起点后移1、keyed推进轮转、key先转换int再取模、合并顺序反转，均被路由/计数/顺序断言或索引panic识别，没有将编译失败/超时计入（check_multigroup_routing_mutants.py及multigroup_routing_mutants目录）。

修正源码Go/GoKeyed摘要、专题方法表和选择表；明确直接uint64取模与ShardedGroup字符串哈希入口不同，回绕按uint64而非无限序号、热点片不改投、同key不保证串行。单Worker建议补无外部Pool/AutoScale改变实际并发的前提。新增生成器参数和生产建议；初次注意说明写入semantics.go被groupCaution优先分支覆盖，检查实际生成文本后改到group_semantics.go生效分支并删除本轮无效重复分支。

新增公开Example_multiGroupKeyedAndRoundRobin，用可等待通知依次验证取消拒绝落第0片、key5落第2片而不推进轮转、随后普通Go落第1/2片，结果按片拼为[20 50 30]，尝试/成功/失败为4/3/1。新例Go1.25 race3.268s、Go1.25 Windows3862.869s实际通过，Go1.26含于后续包级和示例运行。77个Examples显式五次独立race进程通过，docs/retry/sliceops分别2.472/4.375/4.945s、4.580/4.392/5.081s、4.080/4.341/4.494s、4.047/4.016/4.222s、4.414/4.100/3.714s（multigroup-routing-examples-1.log至-5.log）。

group/pool/docs/生成器short audit race随机一轮50.445/23.258/7.638/4.027s通过（multigroup-routing-packages.log），之后只有生产说明和优先级分支文案调整。1388声明调用及3完整MD程序编译、86原文局部片段编译、1415方法四项结构、374围栏语法、338本地锚点、go vet和diff检查通过；追加索引后的最终锚点数以新检查输出为准。第十三项版本全库short普通已终态通过，但本轮新54组和新示例晚于该全库启动，以当前定向/包级证据覆盖，不能称本轮新测试已全库重跑。

Windows百万档race、Linux实际执行、此前四worker TokenBucket fuzz EOF原因、独立复核接口及逐方法余下语义缺口保持，目标active，未提交或推送。

最终文案与优先级修正后docs/生成器short audit race再验7.820/3.243s通过（multigroup-routing-final-docs.log）；实际生成Go/GoKeyed条目已包含拒绝仍前进和不额外哈希。最终341本地锚点无失效，生成一致性、1415方法结构、374围栏、go vet、diff检查通过，所有本轮测试会话终态。


## Retry 默认配置保留与重复执行交叉复核（2026-11-09）

本轮反查RetryChain的全部Default*赋值、模式切换、终端新建限流器及已有虚拟时间契约。DefaultConfig只写isLinear/maxRetries/initialBackoff/maxBackoff/perCallTimeout五字段，保留父context、模式及各模式参数和分片。RateLimiter/Adaptive在业务结束归还许可；TokenBucket/SlidingWindow拒绝仍消耗尝试，最终拒绝不保留之前业务错误。没有复现新的运行缺陷，运行实现保持十三项修复。

新增internal/retry/default_config_contract_test.go。TestRetryDefaultConfigPreservesLimiterAndFreshExecution共120组：无限流/RateLimiter/TokenBucket/SlidingWindow/Adaptive × 1/2片 × Execute/ExecuteVoid/Run/RunVoid × 成功/业务错误/panic。先配置非默认策略、1ns单次期限及各模式额度，再DefaultConfig；全配置快照检查返回接收者及仅五字段改变，独立的字面时间表0/100/300/700ms检查恢复指数退避。每组同一构建器执行两次，验证父context值、无单次期限、业务调用时刻/次数、最终错误与panic身份、有值终端失败返回零值、执行不回写配置。容量0.5的桶不能凑足一个令牌；Limit1的一小时窗口在单片允许一次、双片因每片至少1允许两次，两次终端分别恢复这些初始额度。

另TestRetryDefaultConfigRetainsParentCancellation共40组（5模式×2片×4终端），第一次成功后取消原父context，再DefaultConfig和执行；所有终端立即返回context.Canceled，不调用业务、不退避。两个测试共160组，Go1.26 amd64 race随机100轮4.744s、Go1.25 amd64 race100轮5.682s、Go1.25 Windows386实际100轮2.145s（retry-defaults-final-go126/go125/386.log）。这不是任意配置、所有默认值和并发修改的穷尽测试；配置与执行并发不在支持范围。

六个可编译overlay分别清除模式、保留旧单次超时、执行时忽略模式、忽略单片窗口额度、忽略分片窗口额度、忽略父取消；均由配置/时间表/调用次数断言识别，无编译失败或超时计入。见check_retry_defaults_mutants.py及retry_defaults_mutants目录；改错文件仅在overlay路径，不覆盖工作区实现。

补DefaultConfig源码摘要、生成器实际生效的builderCaution分支和专题：保留父context/分片，取消不会恢复，配置不能与执行并发，每次终端新建限流器，跨请求控制应外层复用独立实例。说明DefaultShards清零走单片而Shards(0)使用DefaultShardCount，DefaultCapacity/DefaultMaxWorker在执行时依当前有效Rate/Min计算。文档中的整数溢出及底层int32容量限制仍保留，极端默认最大并发度尚未作为本轮已验证合同，不据此任意截断预算。

新增公开Example_retryDefaultConfigRetainsLimiter：相同构建器两次执行，每次一小时窗口只执行业务一次并失败，后续尝试被拒绝；输出两次最终限流拒绝和总业务两次。Go1.25 race3.377s；Go1.25 Windows386与初期120组合同同命令通过，retry2.301s/docs2.668s（retry-defaults-386.log，Example实际只执行一次）。78 Examples显式五次独立race进程通过，docs/retry/sliceops分别5.430/5.052/4.475s、6.166/4.931/5.520s、4.468/12.075/4.158s、5.461/3.020/4.229s、5.792/2.959/4.482s（retry-defaults-examples-1.log至-5.log）。

专题新增原文片段纳入check_markdown_fragments.py，总计87组选定局部片段。新增提取文件初次误用包名auditreadme，与原目录auditretry冲突导致setup failed；已改提取器为auditretry并重跑全部通过（retry-defaults-fragments.log），不是库运行缺陷或Markdown业务代码缺陷。1388声明调用、3完整Markdown程序编译通过，1415方法四项结构通过，375专题/README围栏语法通过。最终包级short audit race随机一轮retry8.406s/docs11.735s/生成器7.657s全部通过（retry-defaults-final-packages.log）；go vet、运行声明差异清单和diff检查通过。最终锚点检查见本轮工具输出。

第十三项版本全库short普通及全内部包/docs/工具short race证据继续有效，本轮新增合同与示例由上述定向及包级结果覆盖，不声称又完整重跑全库。Windows百万档race、Linux实际执行、先前四worker TokenBucket fuzz EOF原因、独立复核接口及逐方法剩余语义缺口仍保留。目标active，未提交或推送，不将本轮通过写成绝对无bug。


## Task 结果引用所有权与通知交叉复核（2026-11-09）

本轮先反查Mu、BoundedRunner、TaskBuilder、AsyncResult及Task.Result缓存与既有并发/虚拟时间模型。Mu已有重入、panic、并发历史和快照所有权模型；Task现有模型主要采用整数返回值，不能直接证明复合值的引用所有权。源码只按T复制，TaskHandle专题已有简短共享引用说明，AsyncResult专题与方法注意事项尚缺完整说明。本轮没有复现新的运行缺陷，保持十三项运行修复。

新增internal/task/result_reference_contract_test.go的TestTaskResultReferenceOwnershipAcrossEntrypoints共144组：Go/BoundedGo/BuilderGo/BoundedBuilderGo/GoResult/BoundedGoResult/BuilderGoResult/BoundedBuilderGoResult八入口 × 非空引用/空但容量2的切片与非nil空map/nil引用三形态 × 成功/业务错误/panic三结果 × 父context保留/回调开始后取消。所有业务先通过屏障确认开始，再交叉并发Wait/Values或复制Task的Result及三个独立WaitCh，门闩放行业务后收集全部读取。未使用sleep推测完成。

每组验证仅执行业务一次，成功或错误时保留结构体字段、切片长度容量及底层元素/指针身份，错误保持同一对象；panic各读取共享同一个PanicError并返回零值。所有读取完成后顺序修改引用对象，检查其他已保存结果及后续缓存读取都可见；空切片重切后检查同一底层数组。随后替换本地结构体字段，缓存仍保留原字段槽位，引用内容修改保留。AsyncResult另查已缓存时负时长WaitTimeout、已取消WaitContext、Cancel均返回实际引用；Bounded入口最终无许可泄漏。取消父context不覆盖忽略取消后返回的业务值/错误。该测试不授权并发修改引用，不穷尽所有T、无值入口或非法零值句柄。

最终Go1.26 amd64 race随机100轮5.795s、Go1.25 amd64 race100轮13.608s、Go1.25 Windows386实际100轮3.354s（task-reference-go126/go125/386.log）。Windows386同命令新公开Example实际执行一次，docs2.931s，不把-count=100写成Example执行100次。

五个可编译overlay依次丢弃Go的错误返回值、丢弃Task的错误返回值、用父取消替换Task成功、WaitCh丢弃载荷、getResult用反射复制Items底层数组；全部由业务结果或引用身份/内容断言识别，不把构建失败或超时计入。反射只存在第五个临时overlay，不进入库源码。见check_task_reference_mutants.py及task_reference_mutants目录。

补AsyncResult源码类型摘要、专题及生成器实际生效的AsyncResult/Task/WaitTimeout/WaitContext注意分支，区分缓存同步与引用内容同步；说明业务错误可携带值、结构体字段槽位独立、空切片也可共享数组。生产建议发布后只读，需要修改时按业务结构复制或同步。新增Example_taskResultReferences验证结构体Label局部替换不写回缓存，而切片元素/map条目更新在再次Wait和WaitCh均可见；替换本地slice/map字段不替换缓存。Go1.25 race4.283s通过。

79个公开Examples显式五独立race进程通过，docs/retry/sliceops分别5.682/3.531/4.894s、5.653/4.466/5.364s、4.820/3.269/4.835s、5.442/12.266/3.226s、5.758/4.538/4.495s（task-reference-examples-1.log至-5.log）。本轮task/docs/生成器short audit race随机一轮7.342/9.157/4.044s通过（task-reference-packages.log）。

新增专题原文片段纳入check_markdown_fragments.py，88组选定局部原文编译通过（task-reference-fragments.log）；1388声明调用、3完整Markdown程序编译通过，1415方法四项结构、376专题/README围栏语法、go vet和diff检查通过。最终追加索引后的本地锚点检查见工具输出。运行实现仅原13项修复，源码本轮只有类型注释变化。

全库short普通及全内部包/docs/工具short race的第十三项版本既有证据保留，本轮新144组及示例由定向/包级检查覆盖；Windows百万档race、Linux实际执行、先前四worker TokenBucket fuzz EOF原因、独立复核接口及剩余逐方法语义仍未闭合。目标active，未提交或推送。


## AutoScale 有限大系数转换溢出修复（2026-11-09）

反查Pool/Group performAutoScaleCheck时发现，两处扩容及两处缩容都先int(float64(current)*factor)，再应用方向及边界。浮点转整数超出范围在Go中不能依赖具体结果；Windows amd64实测得到负值，触发错误回退。先添加隔离的包级回归：3个worker、Min1/Max5、单次确认，有限正数1e20/1e300/MaxFloat64各扩缩两方向，两包共12子例全部失败。扩容实际4而非5，缩容实际1而非既有大于等于当前值时的一步缩容2；日志autoscale-factor-before.log。测试只用小worker预算，没有制造大规模分配。

第十四项运行修复仅改Pool/Group的四处目标计算：扩容先以float比较MaxWorkers，超过则直接选择Max，再保留原有未增长至少加1和上限处理；缩容先比较当前worker数，不能下降时预选current-1，否则转整数，再保留原有方向/Min处理。有限输入乘积溢为+Inf也在比较阶段截定，不再把它转换int。使用否定>=条件使NaN仍进入既有转换路径，不在本轮定义新的NaN校验策略；生产仍要求有限合理系数与合法int32/内存预算。没有更改公开签名、Normalize、锁、启停/确认计数或Pool与Group已有边界差异。

最终两个回归文件internal/pool/autoscale_factor_regression_test.go和internal/group/autoscale_factor_regression_test.go扩为56组：14个系数×扩缩×2包。系数包括三种大值、SmallestNonzeroFloat64、0.25/0.5/0.75、1两侧Nextafter及1/1.5/2、0/-1（后两者恢复各自默认）。字面预期核对初始3/Min1/Max5的目标；调整后提交任务并核对值7和无错误，避免只查目标统计而遗漏实际可用性。使用注入busy模型，不把它描述为真实负载。

另新增docs/tests/autoscale_factor_contract_test.go的TestPublicAutoScaleFiniteFactorTicker，共20组：Pool/Group×1.5/2/1e20/1e300/MaxFloat64×扩缩。使用导出Builder/实例方法，扩容时通过屏障让三个真实回调同时阻塞，启动实际监视ticker，synctest虚拟推进1秒并等待本次检查完成；缩容保持空闲。检查目标、放行业务、Wait验证三项成功结果，使用真实worker和计时循环，不调用私有检查函数。

56组回归、20组公开ticker及既有两包扩缩模型种子组合，随机100轮：Go1.26 amd64 race pool7.994/group8.196/docs3.908s；Go1.25 amd64 race8.421/6.323/3.545s；Go1.25 Windows386实际4.872/3.526/3.087s（autoscale-factor-final-go126/go125/386.log）。四overlay分别还原两包扩容或缩容的先转换逻辑，均被包级回归和公开ticker两类行为断言识别，没有构建失败/超时计入（check_autoscale_factor_mutants.py及autoscale_factor_mutants目录）。

既有FuzzPoolAutoScaleDecisions/FuzzGroupAutoScaleDecisions各单worker race fuzz30s通过84,753次/84,115次，耗时34.220/34.115s（autoscale-factor-pool/group-fuzz.log）。这些模型主要使用小数值和二进制可精确表达系数，验证正常决策兼容，不替代新极值回归。

修复后全库go test -short -tags audit ./... -count=1 -shuffle=on -timeout=900s全部通过（autoscale-factor-full-short.log）：根包438.235s，docs6.646、工具3.305、core3.086、group47.040、maps4.452、pipeline18.818、pool20.924、ratelimit5.525、retry5.081、shard4.710、sliceops10.522、task6.103s。运行修复已进入该二进制；公开ticker和最终Pool缩容后的业务检查晚于全库命令启动，另由最终定向和包级证据覆盖，不能称末增测试已经全库重跑。

全内部包/docs/生成器short audit race随机一轮全部通过（autoscale-factor-internal-race.log）：core4.727、group50.440、maps6.402、pipeline24.089、pool22.303、ratelimit6.490、retry7.166、shard13.369、sliceops14.610、task6.305、docs7.833、工具4.094s。包含末增测试的最终pool/group/docs/工具short audit race22.363/48.849/8.965/3.341s通过（autoscale-factor-final-packages.log）。根包AutoScale/Resize筛选五项实际AutoScale测试race三轮5.474s通过（autoscale-factor-root-race.log），不是根包全量race。

补Pool/Group/配置专题及生成器优先poolCaution/groupCaution的有效分支，说明先边界再转换、向零截断、方向最少一步、资源及配置校验责任。79 Examples未新增，本轮之前五进程race证据为第十三项版本；当前包级race执行docs中新例及合同，不能写成第十四项版全79例又五进程。1388声明调用、3完整Markdown程序编译、88组局部原文编译、go vet及diff检查通过，最终结构/围栏/锚点数以末次工具输出为准。

按requesting-code-review技能尝试派出独立审查，工具仍在创建前报missing field message，未派出审查者；没有独立审查结论。Windows百万档race、Linux实际执行、先前四worker TokenBucket fuzz EOF根因及剩余逐方法语义仍未闭合。目标active，不宣称绝对无bug，未提交或推送。


## Pipeline 配置捕获与重复执行交叉复核（2026-11-09）

本轮未改运行实现，累计十四项修复。新增internal/pipeline/pipe_snapshot_contract_test.go：配置主矩阵144组（每组两次Run，共288次执行）（default/serial/parallel × FailFast开关 × Timeout开关 × OnResult开关 × 成败 × 3种缓冲）；每组先创建Pipe，再用公开方法将源context改成已取消替代对象、切换FailFast/Timeout、替换观察函数并追加阶段。两次执行前分别修改共享输入，验证捕获配置与原观察函数、共享输入可见、两次Run通道不同、同句柄Receive相同、Buf/DefaultBuf控制下次通道容量、Drain与Receive无重放。虚拟时间检查第一阶段实际期限；最终Group可能施加自己的默认任务期限，没有错误要求所有入口都无期限。

另1组Receive先取一项再Drain其余，核对三个输入各消费一次；3组任务开始后取消原父context，核对业务与观察错误并等待输出关闭；3组Serial/Parallel/Stream分支，在原阶段切片有备用容量时向分支及源追加不同阶段，重复执行核对阶段隔离与共享输入。共151组，不并发修改构建器或输入。

初始统一预期Serial也发送非最终FailFast错误，12子例失败。追踪execSerialPipe发现仅在isFinal分支发送，随后FailFast直接返回；默认/Parallel有提前结束时发送已处理项的独立分支。原行为未改，本轮按既有契约修正测试，补ExecutePipe/Run/Pipe生成说明与专题，明确Serial须用OnResult观察未发送的中间错误。该期望纠正未计为运行缺陷。

151组最终契约随机100轮：Go1.26 amd64 race5.716s、Go1.25 amd64 race5.567s、Go1.25 Windows386实际3.700s（pipe-snapshot-final-go126/go125/386.log）。第一次扩展测试误用TestPipe宽筛选匹配旧Pipeline长测，已主动中止，未计通过；之后三环境均用四个完整测试名锚定筛选。此前仅144组主矩阵的100轮记录不替代最终证据。

新增flow_builder_snapshot_contract_test.go的16组（Run/Build × Buf -1/0/1/7 × 首阶段成功/失败）。Flow只继承context与阶段，不提交源items，不继承1ns Timeout/FailFast/OnResult/MaxResults；业务虚拟耗时超过1ns仍完成，首阶段错误允许后续恢复，两个提交均产出结果。Build显式context覆盖构建器已取消context，Run使用捕获context；创建Flow后再追加阶段/改Buf和context不影响已启动实例。核对索引、Occupied、原观察函数零调用、两次共八次业务及Close无剩余。三环境各100轮：Go1.26 race4.836s、Go1.25 race4.461s、Windows386实际3.009s（flow-snapshot-go126/go125/386.log）。本轮共167组。

8个Pipe/mode overlay：三个Pipe改为动态读取源配置、Pipe创建时复制输入、忽略缓冲、三模式直接共享阶段列表；另4个Flow overlay：共享阶段、Run丢ctx、Build丢显式ctx、忽略缓冲。全部由行为断言识别，没有将编译失败/超时计入（check_pipe_snapshot_mutants.py/pipe_snapshot_mutants及check_flow_snapshot_mutants.py/flow_snapshot_mutants）。父取消测试对错误context会等待，故动态配置变体只运行有限的配置/阶段矩阵，不拿死锁证明检测能力。

新增Example_pipelinePipeConfigurationSnapshot及Example_pipelineSerialPipeEarlyFailure，总数81。两新例Go1.25 race4.250s、Windows386实际1.657s通过。全部81例按五个独立Go1.26 race进程执行（每进程docs/retry/sliceops）均通过：5.476/4.024/5.393s、5.431/4.057/4.152s、5.887/4.355/4.139s、5.442/4.068/4.135s、3.883/2.890/2.783s（pipe-snapshot-examples-1至5.log），没有误用-count=5宣称Examples重复。

含全部新增测试的pipeline/docs/生成器short audit race随机一轮20.685/9.064/4.081s通过（pipe-flow-snapshot-final-packages.log）。生成一致性、1388声明调用、3完整Markdown程序、88原文局部编译、go vet、1415方法四项结构、376围栏语法、350本地锚点及diff检查通过。新增审计节锚点数以最后重跑为准。去注释AST检查未增加运行实现改动。独立审查再次在创建前报missing field message，没有审查者或独立结论。

更新根包旧race证据的新脚本check_root_partitioned_14.py使用独立root_partitioned_14_cpu2目录、含14修复的当前二进制、GOMAXPROCS=2，完整441项清单每20项新进程执行short/race/audit，不覆盖旧日志或更改测试尺寸。记录时仍运行，未计通过，终态另补。Windows百万档TSan、Linux实际执行、此前四worker TokenBucket fuzz EOF根因和剩余逐方法语义缺口保留，目标active，未提交推送。


本节终态补记：初始根包分批脚本前11批共220项通过，第12批在TestRatelimit_Chain_HighConcurrency_Adaptive十万档遇到ThreadSanitizer分配TracePart失败，错误码1455、进程66，未出现DATA RACE断言但该批不能计通过。随后check_root_partitioned_14_resume.py使用同一二进制和GOMAXPROCS=2，对未通过批及尚未执行部分共221项逐项启动独立进程，全部exit0，包括原失败Adaptive测试；short原本跳过的百万子例仍跳过，原本未跳过的大样本未改尺寸。两个manifest并集经脚本核对441项无重漏，与完整发现清单完全一致。证据root_partitioned_14_cpu2/manifest.json及individual/manifest.json，不声称单进程全套race通过，不覆盖历史资源失败。

包含本轮最终测试的全内部包/docs/工具short audit race全部通过（pipe-flow-all-internal-race.log）：core2.310、group48.830、maps3.684、pipeline20.743、pool21.355、ratelimit5.602、retry4.595、shard12.619、sliceops14.900、task5.123、docs6.595、工具2.174s。最终生成一致性/编译、go vet、1415方法四项结构、376围栏语法、353本地锚点和diff检查再次通过；353含本节新增三处索引链接。目标仍active，剩余逐方法语义、非short百万race资源、Linux实际运行及独立审查等边界未以本轮通过替代。


## Pipeline 消费回调与 fuzz 检测器诊断（2026-11-09）

本轮未改运行实现，累计十四项修复。审查参数生成器发现对全部pipeline阶段fn统一说明SkipOnError决定后续执行，与Serial不读取该字段、链式Stage无设置入口的实现不符。分开Flow（panic保留输入）、有限输入（panic值为零）、Serial（逐项、不读取SkipOnError及Pipe中间FailFast输出边界）、Drain（同步消费且不恢复panic）及ExecuteWithGroup（fn无阶段名）说明，并重新生成API。新增10组生成器入口分派检查，含根包NewFlow/ExecuteStream映射，防止已具体化的说明又被通用分支覆盖；没有把该文字检查当作业务执行证明。

新增internal/pipeline/pipe_drain_contract_test.go共18组：default/serial/parallel × 三项同步及错误保留；三模式 × 是否在panic后取消共6项，生产方全输出缓冲后消费首项panic，核对原panic对象、当前项消耗且无重放、其余两项可继续Drain、业务执行恰三次且结果Err未被消费方panic污染；三模式 × 空输入/空阶段/一项结果共9项，nil消费者只在读到结果时panic，空/已排空的关闭通道不调用。同步测试先阻塞消费回调再取消父context，证实Drain仍等待回调放行；使用synctest屏障，无真实睡眠概率断言。三环境各100轮：Go1.26 amd64 race pipeline4.138/生成器11.744s；Go1.25 amd64 race7.244/docs4.870/生成器2.947s；Go1.25 Windows386实际2.622/docs3.184/生成器3.348s（pipe-drain-final-go126/go125/386.log）。docs中的Example在-count=100命令仍只执行一次，没有计为100次。

四overlay错误变体（吞消费panic、Drain只消费一项、清空业务Err、nil回调直接无操作）全部由具体行为断言识别，无构建失败/超时计入（check_pipe_drain_mutants.py及pipe_drain_mutants）。新增Example_pipelineDrainPanicAndResume，示例总数82。全部82例五个独立Go1.26 race进程运行（每进程docs/retry/sliceops）全部通过：4.598/4.659/4.667s、6.148/5.069/3.660s、5.696/3.379/5.151s、5.719/4.295/4.160s、5.654/4.570/4.515s（pipe-drain-examples-1至5.log）。专题及生成API明确Drain调用者goroutine同步回调、取消不打断消费回调、panic当前项已取出、恢复后只读剩余、缓冲结果取消后可读及nil边界。

本轮继续调查旧TokenBucket四worker race fuzz EOF。开启GODEBUG=fuzzdebug=1，约79,319次执行后worker以exit66终止，新增语料da2948988a6ab4d6；输出没有具体TSan报告，不能仅凭退出码定性。随后配置GORACE相对log_path后重跑，约63,439次执行后退出66，诊断文件tokenbucket_race_diagnostics/report.14720/39032/41348/42448分别记录三个worker的TracePart分配与主进程约56MiB内存分配失败，全部Windows错误1455，没有DATA RACE报告。首次尝试带盘符路径被GORACE选项解析拒绝，属诊断配置错误，未计为被测库失败；成功采集使用相对路径，未改系统分页/服务/Go安装配置。

为隔离库逻辑，在.gotmp/synctest_fuzz_probe创建独立Go模块，imports仅testing/testing/synctest/time。FuzzVirtualClockOnly反复建立虚拟时间bubble并执行至多64次Sleep，没有导入或执行async库。四worker race fuzz约28,977次后在最小化期间EOF，生成e48ac6c147fa472a，与此前失败形式一致；clock-only.16880/25296/40728/6776四报告记录TracePart或SizeClassAllocator分配失败，全部1455。该最小程序的保存语料直接race重放100轮5.509s通过。另无synctest的FuzzIntegerLedgerOnly整数时长计算对照，四worker race fuzz30s、829,745次、35.102s通过，没有诊断报告。这证明相同异常可由不含本库的检测器/虚拟时钟高频组合产生，不是修改TokenBucket业务逻辑的依据；未据此声称Go本身必然存在特定泄漏，旧EOF原始日志没有保留进程诊断，其逐进程历史根因仍无法追溯。

TokenBucket当前普通四worker fuzz30s、3,814,519次、32.359s通过（tokenbucket-timed-fourworker-normal.log）；单worker race fuzz30s、79,769次、34.437s通过（tokenbucket-timed-oneworker-current.log）。两份语料80a74fec92da48be及da2948988a6ab4d6直接race100轮4.084s通过（tokenbucket-timed-replay-two.log）。未删除新语料，未以普通fuzz替代四worker race通过；四worker race仍明确受资源失败限制。最小模块/日志仅作审计诊断，未加入库运行代码。

最终pipeline/ratelimit/docs/生成器short audit race随机一轮25.313/10.042/8.155/4.019s通过（pipe-drain-final-packages.log）。生成一致性及1388声明调用/3完整Markdown程序、88局部原文编译、go vet、1415方法四项结构、376围栏语法、353旧锚点与diff检查通过；新增本节索引后锚点数量以最终检查为准。根包运行实现无变动，上一轮当前14修复441项完整清单race通过证据仍有效；新测试由以上定向及包级证据覆盖。独立审查工具未修复，未重复相同失败调用，仍无独立结论。目标active，Linux实际执行、非short百万race及剩余逐方法语义等缺口保留，未提交推送。

本节最终复核：新增三处索引后356本地锚点全部匹配，1415方法四项结构、376围栏语法、1388声明调用/3完整Markdown程序编译、go vet及diff检查再次通过；去注释AST范围仍为原14项运行修复，没有本轮新增运行实现改动。


## Result 错误链与格式化边界复核（2026-11-09）

继续核对slice Grow、SubmitN及结果错误投影：Grow通过追加零值再恢复长度、负n的新旧差异，以及SubmitN负n在建池后panic的现有边界已由既有文档明确。本轮没有将这些既有行为改成不同语义，没有新增运行修复，累计十四项。对core结果格式化补充行为证据，避免把缺少说明直接当作运行缺陷。

新增internal/core/result_format_contract_test.go四个测试，共28组（错误链7种×Occupied两值共14，格式动词/标记10，输出失败3，构造引用1）：Error原样返回且不清除Value，Ok独立于Occupied；直接、包装、Join和类型化nil的PanicError由errors.As识别，Unwrap不穿透Value；Result的String在非nil错误时选择错误，%+v不向内部错误转发栈标记。格式矩阵核对%v/%s/%q/%x/%#v/宽度精度/+s/+q/+v组合；只有v带+追加当前Stack，其余无引号、无转义、不应用宽度/精度。直接fmt.State写入失败时首次失败即停止、第二次失败不panic且无错误返回；Width/Precision探针会panic，确保不是因为测试状态缺省值而遗漏读取。构造测试保留原对象身份和创建栈，修改原引用及公开Stack后格式化读当前值。测试未改变运行代码。

Go1.26 amd64 race100轮6.584s（result-format-go126.log）；Go1.25 amd64 race100轮core5.518/docs4.021s（result-format-go125.log）；Go1.25 Windows386实际100轮core2.493/docs3.233s（result-format-386.log）。后两命令里的新Example仍只执行一次，未以-count=100宣称示例重复。五overlay错误变体分别让Ok依赖Occupied、IsPanic只用直接类型断言、所有+动词加栈、Result转发+标记、首次写入失败后继续输出；全部因具体行为断言失败，未把构建错误或超时计入（check_result_format_mutants.py/result_format_mutants/manifest.json）。

生成器参数说明细化Format的输出状态与verb，Result及PanicError注意事项补全类型化nil、保留Value、Stack/Value引用责任、格式标记与Result栈输出区别；docs/config.md同步补充，并链接可执行Example_resultErrorChainAndFormatting及契约测试。没有声称所有%q都会按字符串转义，建议需要转义时先Error()取字符串；errors.As提取后检查非nil再输出栈。新增示例后共83例，五个独立Go1.26 race进程执行（每次docs/retry/sliceops），均通过：5.443/3.210/4.147s、5.698/4.070/5.068s、4.357/4.028/4.420s、5.510/4.260/4.181s、5.754/3.677/4.439s（result-format-examples-1至5.log）。

最终core/docs/生成器short audit race随机一轮4.631/8.610/4.366s通过（result-format-final-packages.log）。生成一致性、1388声明调用及3完整Markdown程序、88原文局部片段编译通过；go vet、1415方法四项结构、376围栏语法、356既有锚点及diff检查通过。新增本节三处索引后的锚点最终重验另记。去注释AST范围未增加本轮运行实现改动，前轮当前14修复根包441项和内部包证据仍按其执行范围保留，没有将局部新测试扩展为又一次全库race。

尝试独立审查仍在派发前报missing field message，工具当前暴露参数为空；没有审查者启动或独立结论。此技术限制不作为通过或新的运行缺陷。Linux实际执行、非short百万race资源限制、剩余逐方法语义审查继续保留；目标active，未提交推送，未宣称无bug或所有文档语义零错误。

本节最终复核：新增索引后的359本地锚点全部匹配，83个Example声明计数与五次执行范围一致；1415方法四项结构、376围栏语法及diff检查通过。


## Retry 单次 context 异常退出清理修复（2026-11-09）

本轮从四个单次超时辅助函数的defer清理交叉检查到RetryWithConfig：原实现派生timeoutCtx后调用invokeSafely，正常返回才cancel。runtime.Goexit会执行defer但不返回到cancel行；业务panic的恢复日志后端若再次panic也会跳过该行。子context及其定时器因而可持续到原期限，依赖子context.Done的业务自建工作不会随该次调用结束取消。链式RetryChain.invokeSafely已有defer，不存在此子context缺口。没有把该区别解释为任意改变Goexit返回行为的授权。

先新增TestRetryPerCallGoexitCancelsChild，原实现Go1.26 race真实运行config/config-void两项失败：per-call child survives Goexit: <nil>，chain/chain-void对照通过（retry-goexit-before.log）。修复仅把RetryWithConfig正数单次超时分支改为调用既有WithTimeout，函数内仍invokeSafely；cancel由每次调用局部defer执行，避免在整个重试函数上累计defer。普通业务panic仍被包装、正常返回值和错误仍由原重试核心处理，父context不取消，Goexit仍结束当前goroutine且不继续重试。第十五项运行修复，未改签名或重试流程。

新增timeout_lifetime_contract_test.go共98组：四超时入口×五父状态×三结果60；四入口Goexit4；nil父/函数8；零/负相对时间及零绝对deadline10；到期后继续阻塞直到回调放行8；Config/ConfigVoid/Chain/ChainVoid的Goexit及日志后端panic清理各4共8。使用synctest和完成通道屏障，不以睡眠猜测清理时点。父值继承、更早期限、取消错误来源、原panic身份、有错仍保留返回值、父context不受子取消影响均有断言。既有TestRetryPerCallContextLifetime18组与TestRetryPerCallDeadlineSchedule6组一起三环境各100轮：Go1.26 amd64 race11.475s、Go1.25 amd64 race10.555s、Go1.25 Windows386实际4.220s（retry-cleanup-final-go126/go125/386.log）。命令内Example只执行一次，未以-count=100计为百次。

check_retry_goexit_old_cleanup.py以overlay只恢复本次旧cancel路径，在当前测试上分别复现Goexit和日志后端panic两种失败，不改真实源码，均为行为断言失败（retry_goexit_old_cleanup内两日志）。check_timeout_lifetime_mutants.py六种可编译变体删除清理、提前清理、丢父值/期限、只在正常返回后清理两种签名，全部由行为断言识别，无构建错误或超时计通过。FuzzRetryAttemptModel单worker race30s、68,700次、35.355s通过（retry-cleanup-attempt-fuzz.log），独立七入口结果序列模型核对次数、成功值、最终错误与panic值；不覆盖退避时间，计时由前述契约验证。最低Go1.25全retry包short race6.448s、Windows386实际short4.776s亦通过。

文档补齐internal/retry四单次辅助函数在专题中的参数/使用入口/生产建议/异常退出规则，并明确外部不能导入本项目internal，根包没有同名函数；外部公开入口为Retry/RetryVoid，单次context期限用标准库。生成API保留四方法逐项调用片段，fn、ctx、deadline参数改为实现特定说明。新增ExampleWithDeadline_lifetime后共84例，五独立Go1.26 race进程（docs/retry/sliceops）全部通过：5.787/4.100/4.445s、5.065/3.115/3.194s、5.451/3.999/4.470s、5.482/3.323/4.731s、5.634/4.273/4.223s（retry-cleanup-examples-1至5.log），没有用-count=5冒充示例重复。此前timeout-lifetime-examples系列为运行修复前的辅助函数验证，只保留阶段证据，最终以retry-cleanup系列为准。

含第十五项修复全部内部包/docs/工具short audit race随机一轮全部通过（retry-cleanup-all-internal-race.log）：core3.135、group50.554、maps5.644、pipeline21.528、pool23.002、ratelimit7.236、retry6.631、shard15.101、sliceops18.513、task5.318、docs8.968、工具4.110s。根包-run Retry命令返回no tests to run，未计为Retry业务验证，根包测试列表无该命名；生产搜索亦无RetryWithConfig调用者。全库普通short实际执行中，终态另补，不使用较早十四项版本证据冒充新版全库执行。

生成一致性、1388声明调用/3完整Markdown程序、88原文局部编译、go vet、1415方法四项结构、376围栏语法、360既有锚点和diff检查通过。AST范围对比仅增加RetryWithConfig本次函数体改动；最终新增本节三索引后的锚点另记。独立审查工具仍派发前报missing field message，没有审查者或独立结论；Linux实际执行、非short百万race资源及剩余逐方法语义审查保留。目标active，未提交推送。

本节终态补记：第十五项修复版本go test -short -tags audit ./... -count=1 -shuffle=on -timeout=900s全部通过，根包436.943s（retry-cleanup-full-short.log），其余包也全部exit0；未将此普通执行记为全库race。新增索引后的363本地锚点全部匹配，最终生成一致性及diff检查通过。上述“实际执行中”保留为当时进度，由此终态补记取代。


## Retry 许可异常退出配对释放修复（2026-11-09）

沿上一轮单次context清理继续核对executeCore：成功获取RateLimiter、ShardedRateLimiter、Adaptive或ShardedAdaptive许可后，原先仅在invokeSafely正常返回并完成Adaptive记录后Release。业务runtime.Goexit或业务panic的恢复日志后端再次panic不返回该位置，四类许可均遗漏配对释放。公开Execute每次新建独立限流器，外层部分模式会Close，不能把后续另一次Execute成功解释为原许可已归还。许可自身的证据由注入真实限流器的内部契约取得。

先新增permit_exit_contract_test.go并真实运行未修复实现：四模式×timeout零/一小时×Goexit/日志后端panic共16组失败，Available或已取消context下的有限Acquire证明许可未归还（retry-permit-exit-before.log）；正常成功/失败/业务panic对照通过。修复仅将业务调用、原Adaptive记录封装在每次尝试局部函数，defer对应Release，避免在整个重试函数上累计defer。正常仍先记录后释放，退避前归还；获取失败不注册清理，不释放其他持有者。TokenBucket和SlidingWindow仍为消耗额度，不增加退款。累计第十六项运行修复，无签名变更。

新增52组许可契约：四模式×两超时×五结果共40组；四模式×第二次成功/失败8组，在虚拟时间退避等待点核对全容量已归还及最终两次业务、一秒退避；四模式获取失败4组，外部预占满容量、父期限拒绝后无业务调用且不创造许可。原panic对象、Goexit不返回、单次context清理、父仍存活和正常返回值/最终错误均有检查。旧8组普通失败/panic重试一并三环境各100轮：Go1.26 amd64 race5.165s、Go1.25 amd64 race5.110s、Go1.25 Windows386实际2.580s（retry-permit-exit-final-go126/go125/386.log）。用完成通道和synctest屏障，不依赖真实睡眠。

check_retry_permit_mutants.py六overlay分别删除四类Release、恢复旧普通返回清理、把defer推迟到整个executeCore退出；全部由具体行为断言识别，无构建失败/超时计通过。另新增adaptive_exit_record_contract_test.go十组，通过真实重试先累积九次/每片九次，再让第十次为五种退出结果，有限公开Acquire核对实际容量：成功由3扩至4、失败/普通panic缩至1、Goexit/日志后端panic保持3，分片只应改变对应记录片。三环境各100轮：Go1.26 race2.428s、Go1.25 race2.568s、Windows386实际1.218s（retry-permit-adaptive-record-go126/go125/386.log）。删除两类Adaptive记录及在任意退出额外记录成功共四错误变体也被行为断言识别（retry_adaptive_record_mutants/manifest.json）；运行实现没有因该补充测试再改变。

FuzzRetryAttemptModel单worker race30s、62,861次、34.468s通过（retry-permit-attempt-fuzz.log）。全内部包/docs/工具short audit race随机一轮全部通过（retry-permit-all-internal-race.log）：core4.263、group50.176、maps5.705、pipeline30.019、pool23.289、ratelimit5.593、retry13.081、shard15.115、sliceops16.621、task6.342、docs8.699、工具4.139s。该命令在最后新公开示例/统计测试前启动；新增内容另有定向证据。最终最低Go1.25包级short race retry5.437/docs9.446/工具5.347s、Windows386实际retry5.422/docs4.466/工具3.045s通过（retry-permit-final-go125/386-packages.log）。

本版本全库普通short audit随机一轮全部通过（retry-permit-full-short.log）：根包437.686s，其余包亦全部exit0。该进程在新增公开示例及统计测试前启动，不声称包含后来新增测试；新内容以上另有直接执行证据。更新root_partitioned_16_cpu2脚本重新构建本版本race二进制并保存SHA256，GOMAXPROCS=2，普通每十项、HighConcurrency/Million独立进程执行完整发现清单，保留原short跳过规则及未跳过测试尺寸，终态另补。没有复用第十四项旧二进制充作新版通过。

Retry专题及生成API明确四类许可成功获取后配对释放、退避时点、异常传播、失败获取及Adaptive记录规则，消耗型额度不退款。新增Example_retryGoexitCleanup，验证公开业务Goexit不返回/不重试、单次context取消而父存活、同构建器新执行可成功；新执行新建限流器，不能单独证明前一执行许可释放。共85例，五独立Go1.26 race进程（docs/retry/sliceops）全部通过：4.091/4.275/4.337s、5.890/4.475/4.681s、5.419/5.916/5.316s、5.407/4.452/3.214s、5.761/4.061/4.149s（retry-permit-examples-1至5.log）。最低版本与386新例包含在上述最终包级实际执行中，-count没有用于推算Example重复次数。

最终生成一致性及1388声明调用/3完整Markdown程序、88原文局部编译、1415方法四项结构、376围栏语法、363既有本地锚点、go vet和diff检查通过；新增本节三索引后锚点另重验。AST变更列表中executeCore此前已有预算格式修复，本次局部defer以函数具体diff及回归证明，不拿函数条目计数代替bug数。独立审查再次在派发前报missing field message，未产生审查者或独立结论。目标仍active，剩余逐方法语义、Linux实际执行及非short百万race等边界保留，未提交推送。


## BoundedRunner 启动前异常槽位归还修复（2026-11-09）

沿资源获取至业务启动边界审查BoundedGo、BoundedGoAct、BoundedGoResult及TaskBuilder.GoResultAct使用的boundedGoResultAct。四入口先写入runner.sem占用许可，再调用Go/GoAct/GoResult/GoResultAct；这些函数在调用方同步EnsureTraceID，启用trace注入且context无既有trace时会调用全局logger.With/WithContext。该后端若panic或Goexit，业务goroutine尚未启动，原本仅存在于业务闭包中的释放defer不会执行，同一执行器此槽位长期遗失。没有把不可靠后端当作忽略资源配对的理由，也不把异常改成业务错误返回。

新增bounded_startup_cleanup_test.go的TestBoundedStartupAbnormalExitReturnsPermit，四入口×两个注入点×panic/Goexit共16组。未改运行源码先race实际执行，16组均以startup leaked permit: available=0 busy=1失败（bounded-startup-before.log），断言业务未调用、提交不返回句柄、原panic对象保持或Goexit无panic。修复在成功获取后注册提交方defer，started为false时归还；仅在原Go函数正常返回句柄后设true，正常业务仍由原闭包defer归还，变量只由提交方访问，无跨goroutine共享标志。没有提前EnsureTraceID改变日志/取消的原调用顺序，不通过recover吞掉异常，不使用每次提交额外Once/原子分配。累计第十七项运行修复，公开签名保持。

TestBoundedStartupTransfersPermitToCallback另32组，四入口×trace开关×成功/错误/panic/业务Goexit；启动返回后回调阻塞在显式通道，验证许可仍占用，放行后用synctest屏障确认完全释放、原值/错/业务panic保持。业务Goexit不是panic，原实现只关闭结果通道，当前等待返回零值/nil；本轮保留并文档明确该边界，不将其包装为成功任务的推荐写法。48组最终三环境各100轮：Go1.26 amd64 race2.126s、Go1.25 amd64 race2.524s、Go1.25 Windows386实际1.668s（bounded-startup-final-go126/go125/386.log）。此前只有16组初绿10.217s不代替48组最终证据。

check_bounded_startup_mutants.py逐入口overlay删除提交方清理，四变体均由对应Available/Busy行为断言识别，没有构建失败/超时计通过（bounded_startup_mutants/manifest.json）。既有FuzzBoundedRunnerMixedOutcomes单worker race30s、49,549次、35.287s通过（bounded-startup-mixed-fuzz.log），四种入口并发结果与许可独立oracle覆盖成功/错误/panic/取消竞争；该fuzz不使用异常logger，异常注入由上述16组确定测试验证，不混淆覆盖范围。

最终全部内部包/docs/工具short audit race随机一轮通过（bounded-startup-all-internal-race.log）：core2.399、group50.445、maps6.824、pipeline22.564、pool22.511、ratelimit7.088、retry6.782、shard15.105、sliceops18.070、task6.564、docs9.008、工具4.177s。Go1.25最终task/docs/工具short race5.896/8.153/3.372s，Windows386实际4.338/5.514/3.572s也通过（bounded-startup-final-go125/386-packages.log）。本版本全库普通short audit运行中（bounded-startup-full-short.log），终态另补。

第十六项版本root_partitioned_16_cpu2完整441项发现清单74个独立进程全部exit0，manifest并集无重漏，GOMAXPROCS2，没有资源失败批次、未改原short跳过或测试尺寸，二进制SHA256保存。此二进制在本轮Task修改前构建，仅记为第十六项版本证据。第十七项另用check_root_partitioned_17.py重建新二进制和独立目录验证，终态另补；没有将后续源文件修改影响已构建二进制的版本归属。

Task专题、TaskBuilder四终端和Bounded内部三公开函数生成说明补启动前同步日志异常、调用者清理和成功后业务清理责任，明确业务Goexit的零值/nil现状，生产建议正常返回error、日志及时返回且不异常退出。新增公开Example_boundedStartupRecovery，复用同一Bounded(1)构建器，首次logger.With panic、未执行业务，修复后同构建器第二次正常执行取得7；其成功依赖原许可释放，非重新创建限流器。共86例，五独立Go1.26 race进程（docs/retry/sliceops）均通过：5.076/3.065/3.941s、3.882/4.085/3.947s、5.884/4.721/3.282s、5.475/1.974/4.204s、5.817/3.902/4.881s（bounded-startup-examples-1至5.log）。新例最低版本/386实际各一次包含在最终包级执行中，不用-count推算Example重复。

生成一致性及1388声明调用/3完整Markdown程序、88原文局部编译、1415方法四项结构、376围栏语法、366旧锚点、go vet及diff检查通过；本节新增三处索引后的锚点另记。去注释AST只增加四个Bounded函数的启动清理变更，既有修改均保留。独立审查工具参数错误仍未解决，不重复相同失败调用，也没有独立审查结论。目标active，Linux实际执行、非short百万race及剩余逐方法语义审查继续，未提交推送。


第十七项验证终态补记：bounded-startup-full-short.log的全库short audit普通测试全部exit0，根包435.226s。root_partitioned_17_cpu2重建的新race二进制发现441项，74个新进程全部exit0；manifest完成清单与发现清单完全相等、每项恰好一次，GOMAXPROCS=2，原short跳过及测试尺寸保持。二进制SHA256为4bf3924c40dd76ce99a72b9eab705e2d589a4543425bab31cdedc63b762168d9。该结果取代本节前文写入时的进行中状态；它是分进程完整清单通过，不宣称单进程或非short百万race通过。最终文档补充后docs/工具short race通过（4.592/4.611s），生成一致性及编译、1415方法结构、1388声明调用、3完整Markdown程序、88原文局部编译、376围栏、369锚点、go vet和diff检查通过。


## Pool 与 Group 业务 Goexit 完成登记及 worker 容量修复（2026-11-09）

从Bounded启动清理交叉检查Pool/Group执行链，先用真实业务runtime.Goexit复现：Pool.processTask的finishTask和busy defer执行，但业务后的record未执行、worker退出且active普通递减遗漏；单worker池随后接受的任务无人处理。Group内部executeTask的busy/active/wg.Done都位于普通返回路径，worker退出后waitContext的wg.Wait永久不完成；外部Pool路径也依赖遗漏的completion。原实现定向race两测试真实失败，worker-goexit-before.log分别记录Active1、后继等待超时、结果空槽及Group Active1/Busy1/Fail0。失败回归使用synctest与有限WaitTimeout；Group仅在已断言原缺陷后做测试内部计数修复以结束原后台等待，不添加生产测试钩子。

新增公开哨兵async.ErrTaskGoexit（core同值），仅业务回调已开始且未返回、recover为nil时记录类型零值及此错误，计失败并触发已启用FailFast；不把它包装为PanicError，不自动重试。业务标志只由执行goroutine访问，不给每个正常任务另建goroutine。Pool和Group的任务计数改用相应defer清理；业务Goexit不能recover，因此worker最外defer把已有workerWg登记名额交给新worker，Group同样保留workerCount登记，后继正常退出才减登记。无Add/Wait归零窗口，不改普通任务执行、取消重检和panic恢复顺序；缩容名额和close-time drain仍走原循环。

21组确定场景覆盖两个原缺陷、8组Pool索引/FailFast/执行中关闭、8组Group内部/外部池及索引/FailFast、两个双worker Goexit后缩容到1再Reset，以及ForEachPool失败位于主结果上限之外仍汇总正确错误。回调阻塞时检查Busy/Active持有、退出后检查父/子context、结果及stream唯一性、completion、失败/成功计数和后续任务；缩容以实际并发峰值1及正常后继结果验证，不只读取目标配置。确定矩阵与混合结果fuzz种子Go1.26 amd64 race100轮7.202/6.358s、Go1.25 race100轮5.635/6.138s、Windows386实际100轮12.185/2.071s；末增ForEach场景三环境100轮4.591/4.137/1.942s。对应worker-goexit-final-all-go126/go125/386.log及worker-goexit-foreach-*.log。

新增FuzzPoolGoexitMixedOutcomes和FuzzGroupGoexitMixedOutcomes，在最多32项、1至4worker、成功/非零值业务错误/panic/Goexit混合下用输入独立oracle检查每项执行和终态一次、原值/错、全部计数及容量；Group包含外部Pool。Pool首轮实际发现Wait返回后Active仍为1（worker-goexit-pool-fuzz.log，保存输入29c53143febb0f8e），未将失败隐藏为通过。根因是active递减注册在processTask调用方，晚于processTask内finishTask；改为processTask内先注册finishTask、再注册active清理，使计数先收尾后发布完成。保存输入race100轮4.500s，最终Pool单worker race30s57,641次35.278s通过；Group单worker race30s58,231次39.511s通过。故初版二进制root_partitioned_18_cpu2及普通435.121s仅属初版，不代替最终Active顺序版本；最终使用root_partitioned_18_final_cpu2和worker-goexit-final-full-short.log。

五个可编译overlay错误变体分别去除Pool worker接替、Pool错误、Group接替、Group Done及Group错误；对应测试全部以具体行为断言失败，构建失败/超时不计检出（worker_goexit_mutants/manifest.json）。新增缩容测试首次方法名误写导致编译失败，改为真实resizeWorkerPool后才执行三环境最终矩阵，该编译失败不算红绿缺陷证据。

初版全内部/docs/工具short audit race全部通过；最终Active顺序版本Pool/Group/docs/工具short race24.265/50.067/8.752/3.162s，Go1.25为24.052/50.358/8.829/4.114s，386实际20.807/46.961/5.485/3.458s通过。末增ForEach后的最终全内部short race终态另补。最终全库普通与根包新race完整清单仍运行，不能用已通过的第十七项或初版结果顶替。

Pool/Group专题、哨兵表与对应逐方法生成说明补业务Goexit语义、正常返回error建议及适用边界；Task/Retry原有Goexit行为保持，不扩张该哨兵到其他入口。公开Example_poolAndGroupGoexit复用单worker Pool先Goexit再取得7，并检查Group Wait返回及计数归零。87例五独立race进程docs/retry/sliceops分别4.335/3.178/4.332、5.633/4.070/4.547、5.490/4.824/4.411、5.763/3.156/3.236、5.469/3.029/4.180s通过。六个日志字段函数补具体类型、nil/空重复键、引用快照、无格式化/无日志副作用及用途对应生产建议，避免用执行器初始化套话；内部辅助函数依旧未新增公开别名。

生成检查发现运行实现行号变化后pool.md未同步，重新生成后-check -compile通过，1388声明调用及3完整Markdown程序可编译；1415四项结构、88原文局部编译、376围栏、369旧锚点、go vet及diff检查通过。本节三处新索引后的锚点另验。独立review工具本次仍以missing field message拒绝空schema调用，未启动reviewer，不宣称有独立审查结论；不再重复该无效调用。目标继续active，未提交推送。


第十八项补充交叉回归：在两个业务阻塞时先排入两个正常任务、Resize(1)、再让两业务Goexit；原worker补充方案未先处理已有quitting名额，后继可先选taskCh，实际并发峰值仍为2（worker-goexit-shrink-queued-before.log，100轮真实重复失败）。修复在异常worker最外defer先tryQuit，取得退出名额则Done并退出，只有未取得名额才交给后继；保留原处理完任务后退役时点，Group原循环已在接任务前判断workerCount/目标，不需改。现在22组确定场景及两混合结果fuzz种子三环境100轮：Go1.26 race5.044/3.995s、Go1.25 race5.654/4.580s、386实际1.941/1.576s（worker-goexit-shrink-final-*.log）。累计仍为同一第十八项修复，未把修复迭代另计缺陷。新增去除退役检查mutant，六变体均被具体行为断言识别；最终Pool单worker race fuzz44,986次33.532s通过（worker-goexit-shrink-pool-fuzz.log），Group实现未再变，58,231次证据仍对应。

缩容修正前版本worker-goexit-final-full-short.log全部通过、根包439.864s；root_partitioned_18_final_cpu2的441项74进程也全部exit0，仅属此前Active顺序版本。缩容修正后必须使用worker-goexit-shrink-full-short.log及新root_partitioned_18_shrink_cpu2，仍运行，不能合并版本。最终全内部/docs/工具short race已过（worker-goexit-shrink-all-internal-race.log）：core2.186、group47.963、maps3.108、pipeline21.679、pool21.049、ratelimit5.514、retry4.351、shard12.928、sliceops13.723、task4.287、docs6.609、工具2.048s。最终Go1.25/386包级验证仍运行，终态另补。

最后文档语义补充：为SafeCall和Log/LogCtx提供匹配用途的生产建议、LogFatal明确同步Error输出后os.Exit(1)且不执行defer/不保证异步落盘，PanicError.Unwrap明确始终nil、Result.Error明确原Err，不再套用成功值提示；字段参数残留Str/Int/Err误写已纠为实际六辅助函数，并明确库外LogField字面量。根包SafeCall/ForEachPool薄入口同步对应建议。没有新增运行函数或改动Task/Retry异常语义。生成一致性与1388声明调用、3完整MD程序、1415结构、88原文局部编译、376围栏、372锚点、go vet/diff全部重新通过；最终文档测试另执行通过6.898/3.275s（worker-goexit-final-docs-2.log），末次薄入口文案仅生成后重验编译。


第十八项最终收尾补记：worker-goexit-shrink-full-short.log 全部包通过，根包434.041s。root_partitioned_18_shrink_cpu2 的441项经74新进程全部exit0，重新核验完成清单并集与发现清单相等且无重复；二进制SHA256为29e712aa754a9d0bc95041fa68ae13e1bd2ceaa0a2254dcb078b3b643158473a。末次Go1.25包级pool21.105/group48.664/docs6.341/工具2.000s，386实际18.636/45.748/3.447/.884s通过；最终五独立Examples进程docs/retry/sliceops分别3.444/3.162/2.686、3.636/2.375/2.188、3.555/2.074/2.317、3.203/1.822/2.299、3.322/1.824/1.939s均通过。新增8组Pipeline Pool/外部Pool/Shard/AutoScale×FailFast适配契约：Go1.26race100轮3.478s，Go1.25race4.669s、386实际3.241s；此8组晚于原内部构建，后续第十九项包级包含它们。前文“仍运行”仅为历史状态，以此补记为准。

## 原生 Pipeline 分块异常退出与阶段结果修复（2026-11-09）

本项真实红测试native-pipeline-goexit-before.log：Execute/ExecuteWithMeta、Pipeline实例的两终端、Parallel普通/Meta共六入口，单worker输入1/2/3且首项runtime.Goexit，实际calls=1，普通结果三个零值nil错误、Parallel Meta只保留一个零值记录。根因是safeCall只恢复panic，Goexit继续退出整块goroutine；普通wg.Done完成后主调度把未赋值槽位视为成功，整块剩余业务被跳过。补充默认构建器OnResult用例又观察calls=3/callbacks=2（native-pipeline-observer-before.log），业务包裹层通知也在正常返回之后。未把此修复迭代另计缺陷。

新native_chunk.go的startNativeChunk在调用单项前推进next索引，异常退出defer将同一WaitGroup登记名额交给后继，仅处理剩余索引；最后一项或正常完成才Done，没有Done再Add的归零窗口。finishNativeStage在业务Goexit时同步记录零值/ErrTaskGoexit，运行原有单次context取消、记录、OnResult、FailFast收尾；正常返回与panic仍由原safeCall路径处理，不为每项新建goroutine。八处原生分块：自由Execute/Meta/Pipe非最终阶段、Pipeline实例Execute/Meta、构建器Parallel普通/Meta/Pipe非最终阶段均使用同一辅助；默认PipelineBuilder的OnResult包装补相同终结。Serial/Stream/Flow、Task/Retry行为未扩张。任意通知回调Goexit不承诺生成该业务错误。

确定契约21组：六原缺陷入口、三Pipe中间阶段（自由/实例/Parallel）、三FailFast（普通/Meta/Pipe）、一默认构建器逐项通知、四单次context在观察前取消（普通/Meta×FailFast）、四后续阶段透传或覆盖前错（普通/Meta×SkipOnError）。最终21组及混合结果种子三环境100轮通过：Go1.26race5.359s、Go1.25race5.701s、386实际2.764s（native-pipeline-expanded-*.log）。后两类遵守原正常语义：后续成功可覆盖前错，启用SkipOnError才透传，链式Stage没有设置入口；该错误不能机械变成所有阶段永久失败。

FuzzNativePipelineMixedTerminalOutcomes用独立输入oracle检查7入口、最多32项、1–4worker，四结果为成功/非零值业务错/panic/Goexit，检查调用一次、输入索引结果、原值和错误身份、Meta阶段名，以及默认OnResult通知总数。首版六路径86,813次35.132s通过；最终加入默认观察路径后七入口race fuzz30s单worker84,046次35.414s通过（native-pipeline-final-fuzz.log）。有限输入模型不覆盖全部取消交织或任意回调异常。三个Go overlay可编译错误变体（丢后继、清除业务错误、跳过观察包装）均被具体行为断言识别，manifest为native_pipeline_mutants/manifest.json；错误变体初次未使用import导致编译失败，不算有效检出，修正变体后全部重跑。

最终全内部/docs/工具short audit race通过（native-pipeline-final-internal-race.log）：core4.288/group50.162/maps4.576/pipeline21.848/pool22.956/ratelimit7.275/retry5.934/shard14.894/sliceops16.862/task7.463/docs9.001/工具4.066s。扩展的八组context/后续策略契约晚于此构建，另有三环境100轮证据，后续再做包含新测试的包级验证。公开Example_pipelineNativeGoexit检查顶层nil错误、哨兵非panic及后续20/30；88个Examples五个独立race进程docs/retry/sliceops依次5.470/4.205/4.167、4.685/4.040/5.148、5.791/4.007/4.145、4.820/4.133/3.008、5.676/4.722/4.150s通过。

文档生成器先追加适用入口注意事项，再生成方法参考；ErrTaskGoexit公开注释、config哨兵表及Pipeline专题明确只覆盖原生并行/池化路径、FailFast、Meta/OnResult、中间阶段覆盖边界及生产正常返回error建议。1415条参数/示例/生产建议/注意结构齐全、1388声明调用和3完整程序编译、88原文局部编译、376围栏语法、372锚点、go vet/diff全部通过。结构计数不替代逐方法语义完成。独立review工具仍missing message，未派出审查者，不能称独立复核完成。全库普通native-pipeline-full-short.log和根包root_partitioned_19_cpu2仍运行；不能引用第十八项二进制代表此版本。


第十九项终态补记：native-pipeline-full-short.log全包通过，根包436.566s；root_partitioned_19_cpu2的441项经74新进程全部exit0，完成并集与发现清单相等、无重复且SHA256核验953b351ca5500a8806dbabe140811c0d2d5d79284b43cbb2170e41186a9ed065。包含扩展测试最终pipeline/docs/工具short race22.774/9.107/4.487s；Go1.25race21.232/8.951/4.032s、386实际18.944/5.911/2.642s通过。末次Pipeline终端生产建议改成并发容量、双层错误检查、阶段历史内存与消费取消具体说明，docs/tool race8.629/4.803s及生成编译/1415结构/375锚点/vet/diff再验通过。此终态仅适用第十九项，不覆盖随后Stream/Flow运行变更。

## Stream 与 Flow 业务异常退出及已接受输入修复（2026-11-09）

真实红测试stream-flow-goexit-before.log：自由Stream/构建器Stream单worker输入1/2/3，首项Goexit后calls=1且返回三个空槽、顶层nil错误；Flow同样只调用一项，Close返回空结果，Stats.Active=1、InputBuf=2、TotalSubmit=3、TotalError/Result=0。根因是业务Goexit跳过普通转发、Active递减及worker剩余循环，而worker的WaitGroup却已Done，使阶段提前关闭，遗失已接受输入；超过缓冲还可能让上游失去消费者。先观察真实失败后改实现。

Stream worker用原登记名额交接后继，业务开始前标记、正常结束后复原；当前项用单项defer终结记录ErrTaskGoexit/零值、转发，取消时沿既有失败直达结果收集路径。构建器wrappedFn同样defer执行原OnResult和FailFast，不漏异常项通知。Flow的worker异常路径把原登记名额交给后继，Active加减放单项defer；processStageItem保留原panic恢复及阶段输入值，另检测业务Goexit为零值/ErrTaskGoexit，历史错误加一，defer调用新forwardStageItem。正常任务仍在同一worker循环处理，不为每项新建goroutine，不在WaitGroup归零后再Add；关闭/背压/取消的原有转发选择保持。Serial、Task/Retry及任意日志/通知异常保证未扩张。

12组确定契约：自由/构建器Stream两组原缺陷、Flow接受输入与计数一组、Stream/Flow×SkipOnError四组128项连续Goexit/单worker/缓冲背压、Stream普通/Meta两组FailFast保留首错与OnResult、自由Stream/构建器Stream/Flow三组业务内父取消清理。最终确定契约及混合种子三环境100轮：Go1.26race5.978s、Go1.25race5.346s、386实际4.097s（stream-flow-final-*.log）。取消仍可丢未转发项，Flow Close只验证清理、已接受统计及Active而不强求取消后全量结果；Stream失败直达collector使原失败保留，不以末阶段标签定位实际失败阶段。

FuzzStreamFlowMixedTerminalOutcomes独立模型：自由Stream、构建器Stream+OnResult、Flow并发Results消费与Close回收三路径；最多32项、1–4worker、success/非零业务错误/panic/Goexit四结果，按预期结果多重集及每项调用一次核查，无序Flow不能套索引顺序。Flow普通panic仍保留输入值，Stream panic为零，业务Goexit两者为零；全部检查Occupied，Flow还核对Submit/Err/Result及Active，构建器核对观察次数。种子race100轮4.116s，单worker race fuzz30s72,517次35.473s通过（stream-flow-goexit-fuzz.log）。六overlay错误变体（Stream无后继/无通知，Flow无后继/无Active清理/清除错误/不转发）均被行为断言识别，manifest为stream_flow_mutants/manifest.json。脚本初版匹配初始化与后继两处而断言失败不算检出；修正为末次匹配后全部重跑。

全内部/docs/工具short audit race通过（stream-flow-all-internal-race.log）：core3.044/group51.207/maps6.659/pipeline25.360/pool24.574/ratelimit8.701/retry7.069/shard13.875/sliceops23.857/task5.576/docs10.682/工具4.121s。Go1.25包级pipeline/docs/工具22.192/8.940/3.955s、386实际18.954/12.461/1.928s通过；新增父取消三组晚于上述全包构建，三环境100轮已有结果，最终包级stream-flow-final-packages.log仍运行。

公开Example_pipelineStreamAndFlowGoexit演示Stream Occupied/哨兵及后续20/30、Flow Close结果数3/ErrCount1/Active0/哨兵及后续值，示例总89。五独立race进程docs/retry/sliceops依次5.739/4.015/3.607、5.795/4.267/4.809、5.682/3.859/4.093、9.242/6.751/8.270、5.019/3.142/3.086s全部通过。生成器、公开哨兵注释、config/Pipeline专题及Flow计数说明同步新行为；Serial仍结束调用goroutine。1415方法结构、1388声明调用/3完整MD程序编译、88原文局部编译、376围栏语法、375锚点/vet/diff通过；随后仅追加Flow统计语句的Goexit枚举需再验片段。

第二十项全库普通stream-flow-full-short.log及根包重建root_partitioned_20_cpu2仍运行。独立review工具参数解析缺陷未解决，无独立结论；Linux实际执行和Windows非short百万race环境边界保留。有限模型与文档结构不代表全库无bug或全部语义完成，目标仍active。


第二十项终态补记：stream-flow-full-short.log全库short普通全部通过，根包438.585s；root_partitioned_20_cpu2的441项经74新进程全部exit0，完成并集与发现清单相等、无重复，二进制SHA256核验645a53c286760f1b339718ab4722cc6c410318d2e9dc16f61893788b2f8fd665。包含新增父取消契约最终pipeline/docs/工具short race22.951/8.849/4.767s；Flow ErrCount/Stats语义补Goexit枚举后docs/工具race9.177/4.414s。前文进行中仅为历史，不代表当前结果。

新增FuzzStreamFlowStageOutcomeHistory独立多阶段预言：1–3阶段、1–3worker、最多16项，按当前值与输入字节决定成功/非零业务错误/panic/Goexit，逐阶段模拟SkipOnError、后错覆盖，核对各阶段实际调用数、最终结果多重集、Flow历史错误/成功结果计数和Active。Flow普通panic保留输入值的差异在预言中显式区分。race种子100轮5.213s，race fuzz30s单worker68,997次34.486s通过（stream-flow-stage-history-fuzz.log）；Go1.25race种子100轮4.107s、386实际2.292s通过。新模型晚于全库二进制，另最终pipeline short race20.981s通过，不倒填全库测试包含此模型。

末次文档生成一致性与1388声明调用/3完整MD程序编译通过；88原文局部、1415方法结构、376围栏语法、378锚点、go vet/diff再验通过。源码Group旧说明“每次Go新建goroutine”改为构造时启动worker、队列复用、Wait后退出；仅注释，无运行改动。Serial同步业务Goexit仍沿调用goroutine退出不扩大保证，后续审查继续。


## Slice 原生分块退出与流式池化 FailFast 修复（2026-11-09）

第二十一项真实红测试 slice-native-goexit-before.log：Par(1)并行Map首项Goexit后calls=1，其余五项没有执行、结果呈现零值成功，顶层nil；FF同样没有首错及取消项。分片Map/Each还跳过当前片剩余项和未领取分片，Each失败数为0、结果未Occupied。根因是业务退出跳过普通结果登记和剩余循环，而原defer却完成WaitGroup登记。

native_worker.go新增原登记名额的worker接替与持久分片cursor：取出索引先推进，异常后从下一项继续，普通项仍在当前goroutine运行。finishNativeTask在业务调用栈退出时登记ErrTaskGoexit与零值，保留各路径已有panic恢复、超时清理、结果Occupied、首错、取消与失败统计。仅修改mapParallel/mapParallelFF/mapSharded/eachSharded；同步串行、单workerEach串行降级及Reduce不扩张语义。原生Map未FF顶层仍可nil，普通Map不额外设置Occupied。

四原缺陷契约race100轮4.951s；添加分片FF/父取消/超时清理及混合模型后race100轮4.796s，386100轮sliceops3.063/docs1.653s。FuzzSliceNativeMixedTerminalOutcomes按最多32输入、1–4worker、三路径逐项核对success/非零值业务错误/panic/Goexit、执行一次、Occupied及Each统计；race fuzz30s共61,032次35.140s通过。四overlay变体（无后继/清除哨兵/跳过下一项/重复执行）均被行为断言识别，manifest在slice_native_mutants。

第二十二项真实红测试 slice-worker-goexit-before.log：map-pool/stream/stream-pool/each-stream四入口单worker、首项Goexit，虽有一个ErrTaskGoexit，但calls=6，启用的FailFast未阻止其余业务。根因是包装层只在普通error与panic调用取消，没有识别Goexit。增加returned标记及defer退出检测：池化Map用triggerOnce登记首错并取消，流式三路径在退出时取消；原panic重新抛交Pool/Group处理，不重复生成结果、不改变普通返回值。单worker定向race100轮1.950s。

FuzzSliceWorkerFailFastOutcomes独立顺序预言覆盖四入口、FF开关、最多24项、success/error/panic/Goexit；FF仅允许到首失败的业务调用，关闭FF须全部执行，观察结果按四类计数，允许未执行项取消结果。种子race100轮2.236s，race fuzz30s48,950次33.447s通过。最终386100轮sliceops1.223/docs0.927s通过，Examples在-count=100命令内只各执行一次，不把它当100轮示例。

第二十一项全内部/docs/工具short race均通过（slice-native-internal-race.log），最终第二十二项包级sliceops/docs/工具race13.848/7.040/2.098s通过（slice-final-packages.log）。新增公开示例Example_sliceNativeGoexit和Example_sliceStreamGoexitFailFast，总91。同步修改哨兵注释、生成器及config/mapreduce专题，生成参考保留1415条每项参数/示例/生产建议/注意。第二十一项文档1388声明调用/3完整程序、88原文局部、376围栏、378锚点/vet/diff通过；最终第二十二项须再次执行文档一致性核查。

第二十一项全库普通slice-native-full-short.log与root_partitioned_21_cpu2仍收尾，其二进制不含第二十二项修复；不得将其作为最终版本证明。第二十二项slice-worker-full-short.log编译docs_test时出现runtime cannot allocate memory，本次不能计通过，等待降低构建并发重跑。独立review再次因工具参数missing field message失败，未启动审查者；Linux实际执行与Windows非short百万race环境边界保留。目标仍active，未声称全库无bug或全部文档语义完成。


第二十一项终态补记：slice-native-full-short.log全包通过，根包439.517s；root_partitioned_21_cpu2的441项经74新进程全部exit0，完成并集与发现清单相等、无重复且SHA256核验cb4435c6e1d5fbb900920c47de674f52a5441d98531fb27fa973c2ad387b5d77。仅覆盖第二十一项实现。第二十二项全库初次构建因编译器内存分配失败，降低并发重跑时正新增Map辅助文件，构建发现列表不含新文件，vet报undefined mapShardCursor；两次都不能计通过。最终版另启map-native-full-short.log，不隐去失败。

第二十二项四overlay变体（execMapPool/streamPool/streamFF/eachStreamFF分别删除Goexit取消）全部被真实calls=6断言识别，manifest在slice_worker_mutants。初次streamPool mutant编译进程长时间无CPU进展，停止该审计子进程后以GOMAXPROCS=2/-p=1/120秒子进程上限重跑全部四项通过；停止前不计检出。最新第二十三项方法参考生成、1388声明调用/3完整MD程序、88局部片段、1415条结构、376围栏、378锚点、vet/diff完整通过，独立复核仍因工具参数解析失败未启动。

## Map 原生执行器业务异常退出修复（2026-11-09）

第二十三项真实红测试map-native-goexit-before.log：五入口单worker、六元素、首项Goexit后calls=1；Map/MapBatch返回空map与nil错误，ForEach/ForEachBatch total=1/fails=0/err=nil，Stream结果数0。初版测试误用不存在的values字段只属编译错误，改用Len/Data/Error公开访问后五入口均观察真实行为失败，再改实现。

map_runner.go五路径统一使用mapShardCursor保持当前片及其进度；startMapWorker在业务Goexit后以同一WaitGroup名额交接，正常任务仍复用goroutine。finishMapTask对业务Goexit登记ErrTaskGoexit，普通panic仍交原保护；保护外panic保持各入口既有收尾，不额外作为业务Goexit。Map/MapBatch只存成功项，ForEach失败按元素数计，ForEachBatch整批元素计失败，Stream发零值与哨兵。无池FF只停后续调度、不主动取消已开始业务，父取消及背压选择不变。同步串行、串行Stream生产goroutine和Reduce不扩大保证。

五入口原缺陷race100轮2.709s，maps short race3.229s；增加固定混合success/error/panic/Goexit、三worker两分片、单项TO context清理契约后与模型种子race100轮2.901s（map-native-mixed-final.log）。固定混合Map只保留键4/6值40/60，Each total6/fail4，Stream类别2次Goexit/1次panic/1次业务错误/2次成功，全部每项一次且六单项context已Canceled。

FuzzMapNativeTerminalOutcomes五入口独立模型：最多24项、1–4worker、1–8分片、1–6批次大小、FF开关及四种统一业务结果；不假定map遍历次序，检查成功结果数和值、失败类型、每项最多一次及非FF全量执行、按元素批次统计、单workerFF只允许首失败。race种子100轮2.849s，race fuzz30s63,085次33.252s通过；该模型用统一结果覆盖任意实际分块，混合按键结果由上述固定契约另验，不把统一模型描述为随机混合。386100轮maps0.814/docs0.772s通过。

三个可编译overlay错误变体（无worker后继/清除Goexit错误/跳过下一索引）均被行为断言识别，manifest在map_native_mutants。最后maps/sliceops/docs/工具short race2.654/13.220/6.747/2.055s通过；固定混合契约晚于此包级命令，另100轮验证，不倒填旧证据。新增公开Example_mapNativeGoexit，92个总Examples（docs84、其他包8）；此前只统计docs的84不代表全库总数。

生成器及公开哨兵注释、config和mapreduce专题同步Map结果保留、FF、批次统计、context清理及串行边界。map-native-full-short.log与root_partitioned_23_cpu2运行中；本项未完成最终全库验证。独立review工具再次missing field message未启动，无独立结论，目标仍active。


第二十三项终态补记：map-native-full-short.log全库普通short全包通过，根包431.468s；root_partitioned_23_cpu2的441项经74新进程全部exit0，清单无重漏、SHA256核验1c3f95f65764604a83b45c03e236515e19dcdfbda9eab0754154bed36b9739c6。最终混合契约386100轮maps1.041s；92Examples五独立race进程docs/retry/sliceops依次4.337/1.724/2.413、3.584/1.719/2.317、3.356/2.226/1.883、3.694/1.993/1.935、3.339/1.994/2.340s通过。新增混合契约后全内部/docs/工具short race通过（map-native-all-internal-race.log），该二进制在Task运行修复前构建，只覆盖第二十三项。

## Task 业务异常退出失败结果修复（2026-11-09）

第二十四项真实红测试task-business-goexit-before.log：Go/GoAct/GoResult/GoResultAct、四种Bounded入口，业务Goexit后八并发等待者均得到零值/nil错误，误报成功。初版测试误用不存在的导出BoundedGoResultAct只属编译错误，改为真实内部boundedGoResultAct后八入口行为全部失败，再修改实现。旧文档虽然如实记录行为，仍不能让未返回任务假装成功。

task.go与noresult.go四基础入口增加returned标记，业务普通返回后立即置true；终结defer仍优先处理原panic，只有recover为空且业务未返回时发送零值/ErrTaskGoexit。Go同步保存缓存及发送通知，其他入口保留原结果通道与缓存方式。结果只发送一次、完成关闭、不重试业务；不改变普通返回非零值+error、panic包装、Task.Cancel责任、TO子context及Bounded许可defer。启动前trace异常仍发生在提交方，不产生句柄或新失败结果。

八入口×八等待者定向race100轮1.963s；加入旧启动许可契约与扩展FuzzTaskConcurrentCache种子后race100轮2.633s。启动清理测试原先将业务Goexit视作nil成功，期望已改为新失败哨兵；启动前异常释放测试保留不变。缓存独立模型将原三结果扩为四结果，四引擎/九类并发等待读取/取消等待覆盖，race fuzz30s55,947次33.104s通过。

FuzzBoundedRunnerMixedOutcomes原三结果扩为四结果，四入口最多24任务、1–4共享许可、父取消并发、业务Goexit，检查每项0/1次调用、实际结果/拒绝取消、最大在途和许可最终完整归还。种子race100轮2.384s，race fuzz30s55,047次33.216s通过。八入口、启动清理、两个模型种子386100轮task1.105/docs0.645s通过；Examples在-count=100命令内各执行一次。四基础入口删除Goexit登记的可编译overlay变体均由value=0 err=nil行为断言检出，manifest在task_goexit_mutants。

新增公开Example_taskBusinessGoexit（总93），说明零值/ErrTaskGoexit/Ok=false/IsPanic=false及重复等待错误身份。Task/docs/工具short race4.240/6.755/2.095s通过。task/builder生成器、config、Task专题和公开哨兵注释同步修正旧nil成功描述，文档生成完成；最终文档/全内部race/全库普通task-goexit-full-short.log及root_partitioned_24_cpu2运行中。独立review工具仍missing field message未派出；Linux实际执行与Windows非short百万race环境边界保留，不声称绝对无bug。


第二十四项最终收尾补记：task-goexit-full-short.log全库普通short全部通过，根包428.277s；root_partitioned_24_cpu2的441项经74新进程全部exit0，重新核验发现/完成名称并集与次数，无重漏，二进制SHA256为ecae2d264a0f93914b7c6e546bb564891da557f28ab1bee9bcd0617ea3d82091。根包二进制晚于Task运行修复，早于后来内部Task测试扩展，根包无新增测试或实现变化。全内部/docs/工具short race首次Task测试误用不存在的Timeout方法而编译失败，其他包均通过（task-goexit-all-internal-race.log）；该失败不计全包通过。改为真实WithTimeout后Task包race6.404s通过；再加强清理断言及扩展Result模型后，Task/docs/工具short race最终6.167/8.648/3.074s通过（task-goexit-final-packages.log）。

八种builder终端（是否Bounded乘四终端）在业务Goexit后检查超时子context已Canceled、有界许可Busy归零。Task.Cancel延迟到断言之后，防止调用方取消掩盖清理缺陷；连同八基础/有界入口并发等待契约race100轮5.916s、386实际100轮2.568s通过。扩展FuzzTaskResultCache为GoResult/BoundedGoResult/GoResultAct/有界Act四入口，成功/非零值业务错/panic/Goexit四结果；最多16并发操作、句柄副本、并发Cancel和重复Result检查同一结果、不重复业务及许可归还。最终种子及契约race100轮2.646s、386100轮1.355s通过；race fuzz30s、单worker66,110次34.368s通过（task-goexit-result-fuzz.log）。两个遗漏Goexit登记与两个遗漏builder超时cancel的可编译overlay变体全部被具体结果或context断言识别，task_goexit_cross_mutants/manifest.json；未改运行实现，不新增缺陷计数。

93个Examples（docs85、其他包8）经五次独立race命令，docs/retry/sliceops分别8.977/4.006/2.424、5.393/3.319/2.857、5.469/2.854/3.726、5.532/2.728/4.133、4.843/2.934/3.055s全部通过。最终生成检查/1388声明调用/3完整MD程序、88原文局部片段、1415条目四项结构、376Go围栏、378本地锚点以及go vet/diff全部通过。另直接抽取编译6个日志字段、37个Group/32个NoResult返回及24组合、8个限流builder/11实例、5模式/18终端Pipeline、16README和21Task返回/字段原文契约通过。编译不执行业务、结构不证明全部语义。独立审查工具本轮再次在参数解析阶段报missing field message未派出审查者；不以自查替代独立结论。Linux实际执行与Windows非short百万race的既有边界不变。


第二十四项最终内部非short补验：Go1.25、GOMAXPROCS=2、-p=1、-tags audit、-count=1，internal全部十包通过（task-goexit-internal-nonshort.log）：core12.106/group46.710/maps4.211/pipeline24.454/pool18.769/ratelimit12.719/retry7.460/shard5.923/sliceops10.808/task20.457s。包含最终构建器清理契约及四入口Task.Result种子；此为普通执行，不覆盖Windows非short百万race缺口。报告更新后378锚点、go vet与diff检查再次通过。所有本轮验证进程已结束。


第二十四项工具链与构建补验：Go1.26本地工具链下，八入口/构建器Goexit契约及缓存、Result、许可三个最终模型种子race100轮通过（task-goexit-final-go126.log，3.148s）。Go1.25、GOOS=linux、GOARCH=amd64、CGO_ENABLED=0，go test -p=1 -tags audit -c -o .gotmp/linux_24/ ./...退出0，生成全部13个含测试包二进制（task-goexit-linux-compile.log）；只完成交叉编译，未实际运行Linux二进制。


## Map 混合异常批次与默认池并发选择复核（2026-11-09）

FuzzMapMixedTerminalStrategies补此前统一结果模型的交叉覆盖：Map/ForEach/Stream/MapBatch/ForEachBatch五终端，原生分片/终端创建默认池/借用池三路径；最多32个键、1至4worker、1至7分片和批大小、FF开关。每次实际随机批次以最小键选择该次成功/非零值业务错/panic/Goexit，记录实际批次成员，独立检查每项一次、成功首键和值、逐元素失败统计、流式错误类别和值、汇总错误身份、业务子context清理。FF时原生和借用单worker只允许首错前调用；默认池按IO()创建，允许多个已开始回调失败。池化Each统计包括未执行输入，原生只计实际开始元素；流式允许池化取消拒绝通知，不把它当作业务调用。未测试父context取消与停止消费、不协作回调或任意并发配置，此模型不替代那些专门契约。

首轮map-mixed-terminal-seeds.log因把DefaultPool视为Worker(1)而观察多业务失败；源码确认终端调用pool.DefaultPool而非读取Policy.Worker，是测试假设错误，未改运行代码。修正oracle后100轮race通过；首版较弱流式值检查race fuzz56,059次33.226s通过。最终加强实际流式值/错误身份、首错后不可再调用及无错全量执行断言，race100轮2.996s；最终race fuzz30s、单worker52,010次34.989s通过（map-mixed-terminal-final-fuzz.log），不把旧版次数叠加成最终覆盖。

TestDataDefaultPoolWorkerSelection使用synctest让两项回调阻塞于gate，等待静止再断言实际启动数；Slice的Map/ForEach/Stream/MapBatch/ForEachBatch与Map同五终端，默认池Worker(1)实际启动两项、借用容量1池只启动一项，放行后均完成两项。初次误用不存在的async.NewPool只属测试编译错误，修为内部pool.NewPool创建实际池后20组race100轮3.262s通过。最终模型和契约Go1.26race100轮maps5.004/docs2.836s，386实际100轮maps1.267/docs1.014s通过。

五个可编译Go overlay错误变体：去worker后继、Goexit改成ErrSkipped、批失败按批数、成功批次存尾键、默认池改读策略Worker；全部由对应访问次数/错误身份/统计/关联键/并发启动数断言检出，manifest在map_mixed_terminal_mutants。没有修改库运行实现，累计24项不变。Map/Slice生成器及mapreduce专题补DefaultPool并发选择和固定并发池作用域建议；生成参考重新生成且1388声明调用与3完整MD程序编译通过。最终maps/sliceops/docs/工具short audit race3.458/14.017/6.878/2.193s通过，93个既有Examples包含在包级执行。88原文片段、1415四项结构、376Go围栏、378本地锚点、go vet及diff检查通过。根包代码和测试本轮未改，不重复运行既已验证的441项；本轮新增模型仅有上述新的包级证据。独立复核工具和平台执行边界继续保留，不宣称全库零缺陷。


## 分片限时等待快照与完整示例错误传播复核（2026-11-09）

运行实现没有修改，累计仍24项。新增internal/shard/bounded_wait_snapshot_contract_test.go，16组为单/三片、业务/同步通知阻塞、限时/已取消等待、FailFast开关。稀疏索引2形成每片3槽，逐片等待已完成早片，以最后一片阻塞证明提前返回；核对任务context已Canceled、父与共享context仍有效、成功结果保留、未完成槽位、独立快照、片号拼接、最终等待仍阻塞、通知全部结束后统计归零及Reset新批次。

初版测试在Reset后保留通知回调却把通知通道容量设为第一批数量，导致测试自建通知阻塞、synctest死锁；shard-bounded-wait-first.log记录该测试错误。容量增加1容纳第二批后通过，未改运行实现。Go1.25race100轮2.415s（shard-bounded-wait-seeds.log），Go1.26race100轮10.219s（shard-bounded-wait-go126.log），Windows386实际100轮0.976s（shard-bounded-wait-386.log）。五可编译overlay变体提前共享取消/漏任务取消/反转片号/丢稀疏槽/错误等待预算均由具体行为断言识别，manifest为.gotmp/shard_bounded_wait_mutants/manifest.json。

Shard专题五个完整处理示例逐段原文提取验证：最初两段在业务失败、父取消时仍返回nil（shard-wait-doc-red.log）；主动取消示例空输入快速结束后，取消goroutine未被等待，synctest检测到bubble已结束仍有睡眠goroutine，属于文档示例生命周期问题。其余三段在大数据业务失败/父取消、流消费业务失败、分批业务失败时同样返回nil（shard-outcome-doc-red.log）。编译阶段一次fixture残留未使用log导入，明确只是测试支架错误（shard-wait-doc-green-initial.log）。

修正五示例：统一传递业务context，汇总提交和最终业务错误；等待context提前返回时保留等待错误并取得最终结果；分批提交错误后停止新批次、等待已接收任务；流式结果可能丢失，业务错误从主结果检查；主动取消使用可停止timer与done通道，并在函数退出时取消及等待取消goroutine。文档补非nil父context/输入、业务函数约定、返回错误、生产预算、幂等及不协作回调边界。原文仍保留示例自己的片数和worker预算，未修改库行为。

可重跑脚本docs/scripts/check_shard_examples.py以真实公开builder执行原文，只补imports及业务替身；最终替身采用固定typed context签名，期望错误由fixture独立定义。五入口各空/成功/业务失败/父取消，加两种等待取消共22有效场景，另3个无等待期限组合明确skip。Go1.25race25轮2.372s（shard-outcome-doc-final.log）；Go1.26race25轮2.547s（shard-outcome-doc-go126.log），386实际25轮0.990s（shard-outcome-doc-386.log）；保存脚本后同一原文25轮race3.201s（shard-outcome-doc-tracked.log）。较早两示例单独证据不充当五示例终态；原文局部片段既有88组外新增这5组，共93组，93个公开Go Examples数量不变。

最终shard/docs/工具short audit race12.714/11.310/2.819s通过（shard-bounded-wait-final-packages.log）；该命令不执行.gotmp原文包，原文包按上述独立验证。1388声明调用及3完整程序编译通过（shard-bounded-wait-doc-compile.log）；1415方法四项结构、376围栏语法、378本地锚点0未匹配、生成一致性、vet及diff检查通过。库运行逻辑和根包测试未改，沿用第24项根包441项/全库普通short证据，不将旧二进制计为新增内部测试通过。

独立review再次因工具schema missing field message无法启动；未获得独立复核结论。Linux实际运行、Windows非short百万race资源与剩余逐方法语义审查仍开放，本组契约不证明全库绝对无bug，目标仍active。


## 分片基础配置示例错误与取消契约复核（2026-11-09）

本轮未修改库运行实现，累计仍24项。Shard专题十二段配置原文的旧版本在业务失败和父context取消时返回nil，shard-basic-doc-red.log记录真实行为失败；初版替身使用变长any，只用于复现。修正十二段：调用方Context(ctx)、业务process(ctx,item)、保存逐项或批量提交错误、等待已接收任务并汇总最终业务错误。将“定时提交 + Key亲和”标题改成实际非阻塞提交；新增参数/返回约定、imports、整体预算、每片资源、Key亲和不保证执行顺序和Run不自动传播业务结果错误的说明。

新增可重跑docs/scripts/check_shard_basic_examples.py，固定typed context及输入/输出签名，不使用any模拟业务。业务替身具有独立错误哨兵与原子调用次数；在执行中取消场景，等待首个业务启动后取消父context，业务只等待传入context；成功场景要求实际调用总数等于输入量，含固定100项示例。最初十二段48场景Go1.25race、Go1.26race及Windows386各25轮通过，对应shard-basic-doc-typed.log、shard-basic-doc-go126.log、shard-basic-doc-386.log。

扩展同一脚本覆盖此前已有的六段批量/指定索引/FailFast原文；严格业务签名先报告六包调用参数缺少context（shard-basic-context-red.log），这是文档与新明确业务约定不一致的编译证据，不计新增库运行缺陷。六段统一传递任务context后，十八段乘成功/业务失败/父预取消/执行中取消共72场景，在Go1.25race、Go1.26race和Windows386实际各25轮全部通过。最终日志shard-basic-expanded-final.log、shard-basic-expanded-go126.log、shard-basic-expanded-386.log各18包exit0。

四可编译overlay故意错误为漏业务错误、漏父context、漏业务context及跳过业务调用，分别被错误身份、父取消、synctest死锁及调用次数断言检出。首轮第三项错误检测器预期字符串“deadlock detected”与Go实际“deadlock: all goroutines in bubble are blocked”不一致，虽然变体已失败，验证脚本仍退出1；调整匹配文本后四项重新运行全部通过，manifest在.gotmp/shard_basic_example_mutants/manifest.json。测试缺陷与库缺陷不混计。

两份原文脚本支持缺少.gotmp时创建父目录；check_markdown_fragments.py的六段旧业务替身同步context签名，88段局部编译重新通过（shard-basic-fragments.log）。新增十二段加此前五完整示例，与旧88段中的六段重叠去重后共105组，不把本轮18段全数重复相加；93个公开Go Examples数量不变。五完整示例脚本本轮重新race25轮2.013s通过（shard-basic-complete-examples.log）。

最终shard/docs/工具short audit race12.127/6.731/2.997s通过（shard-basic-final-packages.log）；1388声明调用、3完整MD程序及生成一致检查通过（shard-basic-api-compile.log）。1415方法四项结构、378本地锚点0未匹配、go vet通过。376围栏检查包括在工具包测试中；结构/编译检查不证明逐方法全部语义。根包实现/测试未改，未重复已经通过的441项；本轮测试证据不倒填早期二进制。

独立review仍在工具参数解析阶段missing field message而未启动，无独立结论。Linux实际执行、Windows非short百万race资源及剩余逐方法语义边界保留，目标仍active。


## 分片生产推荐配置提交中取消错误汇总复核（2026-11-09）

生产推荐配置原文的Pool SubmitBatch与Group GoKeyed在提交报错后直接返回，只保留提交错误，已开始业务最后返回的其他错误未检查。docs/scripts/check_shard_production_examples.py严格typed提取原文两个作用域，仅分开包装并补应用实现。控制100项相同用户ID发往同片，启动任务阻塞于业务context，synctest静止后取消父context，业务返回独立accepted business failure。旧原文两作用域均只返回context canceled而不包含业务哨兵（shard-production-doc-red.log，25轮重复实际失败）；不是库运行缺陷。

修正Pool先记录批量提交错误、Wait、errors.Join(handleResults)；Group保存首个提交错误后停止新提交，仍Wait已接收任务，再合并错误。handleResults在正文明确必须检查全部业务结果，补父context/输入/返回值、整体资源预算及提交失败仍收尾建议。原文保留自己的16片、每片8/4worker及超时资源设置；已开始回调必须响应context。外部持久化与幂等仍由应用负责。

新脚本两作用域乘空/成功/业务错误/父预取消/提交中取消共10场景，Go1.25race25轮2.155s、Go1.26race25轮2.484s、Windows386实际25轮1.775s全部通过，日志shard-production-doc-final.log、shard-production-doc-go126.log、shard-production-doc-386.log。取消期间同时检查提交取消及已接收业务错误；成功/空输入核对实际调用数。两个作用域属于既有88局部片段中同一生产块，不新增重复数量，去重片段仍105，公开Examples仍93。

check_markdown_fragments.py生产业务支架补标准库errors导入，88原文片段编译再次通过（shard-production-fragments-final.log）；仅编译不等于业务已执行。十八段配置/批量/索引/FailFast脚本在最终父目录创建修正后重新race25轮全部18包通过（shard-basic-scripts-final.log）。最终docs/工具short audit race6.594/2.371s通过（shard-production-final-packages.log），1388声明调用与3完整Markdown程序及生成一致检查通过（shard-production-api-compile.log）。运行实现及根包测试本阶段未改；累计24项，不把文档错误加为库缺陷。独立review工具与Linux实际执行、Windows百万race资源边界继续保留，目标active。


## Retry 并发终端隔离与生产原文预算复核（2026-11-09）

本轮没有新增运行缺陷或修改库运行实现，累计仍24项。新增internal/retry/concurrent_execution_contract_test.go：同一配置固定构建器的两个终端同时执行，5模式（none/RateLimiter/TokenBucket/SlidingWindow/Adaptive）、1/3片、Execute/ExecuteVoid/Run/RunVoid、成功/业务错/panic/Goexit/父取消后成功/父取消后业务错，共240组。首次回调阻塞gate，synctest静止后要求两个回调均启动，证明RateLimiter/Adaptive单许可没有跨执行共享；检查context值及每次独立子context、错误身份无交叉、失败零值、成功值、实际调用数、Goexit沿goroutine退出不普通返回、父取消优先级、配置不被终端写入及零虚拟时间额外等待。无取消时父context须保持有效，调用方显式取消的两结果分别检查成功保留/停止剩余尝试并返回Canceled。

最初160组四结果race25轮2.932s，加入零时间断言race100轮4.193s，Go1.26race100轮4.314s、386实际100轮1.767s；这些仅为较早阶段。最终240组含父取消：Go1.25race100轮4.869s（retry-concurrent-cancel-final.log）、Go1.26race100轮5.020s（retry-concurrent-cancel-go126.log）、386实际100轮2.146s（retry-concurrent-cancel-386.log）。后续失败支架只保存放行前启动数，防止错误共享许可在gate释放后启动第二业务掩盖真实同时启动数；不改正常行为，定向验证另记终态。

六可编译overlay为终端写入配置、漏单次cancel、丢成功值、漏RateLimiter Release、漏Adaptive Release、跨执行共享Adaptive。分别由配置/数据竞争、context、值、零额外等待、synctest死锁及同时启动数断言检出，manifest在.gotmp/retry_concurrent_mutants/manifest.json。初版检测脚本把漏RateLimiter释放误期待成死锁，实际定时补充允许2小时虚拟等待后完成，零时间断言已检出；改检测文本后重跑。共享Adaptive初次-run子段没有首锚，success还匹配canceled-success，跨bubble持有channel发生synctest异常，该次不计预期断言验证；各子段加双锚后六项全部按指定断言识别。

新增可重跑docs/scripts/check_retry_production_examples.py，逐字提取Retry生产推荐配置四步原文，只补imports、typed服务、输入与未用值接收。检查成功；每一步业务耗尽/首次失败恢复/单次超时恢复/回调取消父后错误/取消父后成功；父预取消，共22场景。检查失败后后续步骤不运行、各步次数及独立错误身份、返回成功的RPC指针原样传入写入、幂等键、单次context结束清理、父context不被执行额外取消及独立虚拟时间期望。四步立即失败累计退避分别1.4/151.5/3.1/303秒；第一次单次超时第二次成功分别5.2/10.5/3.1/31秒。三类服务和写入均及时响应传入context；不把有限模型推广到不协作I/O或外部实际网络耗时。

原文第一次race25轮2.364s即通过，未把新增覆盖当作新库缺陷。最终加强RPC返回指针身份后Go1.25race25轮2.430s、Go1.26race25轮4.230s、386实际25轮1.069s通过，对应retry-production-workflow-identity-final.log、-go126.log、-386.log。四可编译原文overlay：忽略第一步错误、写入错用第二步数据、错误重试预算、错误单次期限，均被步骤调用数/错误/值/耗时断言识别，manifest在.gotmp/retry_production_mutants/manifest.json。

Retry专题补固定配置并发读取、每执行独立许可/子context/错误、配置不能并发修改及业务共享状态同步说明；生产示例补父context/输入/typed函数/返回错误、幂等键与第一步数据来源、整体预算和取消成功优先级、四步预算表。生成器同步终端并发边界并重新生成api/retry.md。生产原文已属于既有88组局部片段，不重复增加计数，去重片段仍105，公开Examples仍93。

retry/docs/工具short audit race5.360/6.406/5.545s通过（retry-isolation-final-packages.log），包含最终240组父取消版本，早于只改失败支架启动数；1388声明调用与3完整MD程序编译及生成一致性通过（retry-isolation-api-compile.log），88局部原文编译通过（retry-isolation-fragments.log）；1415方法四项结构、378锚点0未匹配与go vet通过。本轮根包实现和根包测试未改，沿用第24项441项根包证据，不假定其覆盖后来新增内部契约。

独立审查本轮仍在工具参数解析阶段missing field message，未派出审查者，无独立结论。Linux实际执行、Windows非short百万race资源及剩余逐方法语义边界仍开放；目标active，不宣称全库绝对无bug。


Retry本节终态补记：保存放行前启动数之后，最终240组契约重新三环境各100轮：Go1.25race5.769s、Go1.26race5.785s、Windows386实际2.768s，日志retry-isolation-terminal-go125.log、-go126.log、-386.log。原文脚本另逐行检查四行预算表的调用次数/累计退避/单次超时恢复时间，表格与独立数值期望一致，22场景race25轮2.652s通过（retry-production-budget-table-final.log）。后者只加强表格检查，生成Go代码与此前三环境指针身份版本相同，不冒称表格脚本三环境都执行过。最终1415方法结构、378本地锚点、生成一致、go vet与diff检查再次通过。本轮所有验证进程已结束，目标保持active。

## 限流生产与基础原文错误返回及统计复核（2026-11-09）

本轮未改库运行实现，累计仍24项。RateLimiter生产原文直接忽略doRequest返回值，Adaptive记录失败后也未向调用者返回错误；初始原文测试真实复现两入口的业务失败、父预取消、执行中取消和超时误返回nil。ratelimit-production-red.log保存失败证据；最初仅为兼容旧原文无参数调用使用变长context替身，修正后固定为doRequest(context.Context) error，独立错误哨兵不从被测结果生成。

修正两段原文向业务传ctx并原样返回错误。另六段Wait/Token/Reject/Release/WithTraceID/分片用法也统一传递业务context并返回错误，TraceID示例传处理后的tracedCtx。补非nil参数、业务签名、返回、生产共享实例/统一关闭与取消注意事项；精确说明Block优先已有许可、Reject/BlockForce忽略取消，不能把取得许可当业务成功。消除概述对分片Resize的并发保证泛化，生产Burst改按下游突发预算选择。

新增可重跑docs/scripts/check_ratelimit_production_examples.py，逐段提取原文，不改原文语句，只补imports、函数签名、业务替身、输入和外围资源。八段原文各九结果：成功、业务错误、父预取消、取消并返回错误、取消但业务成功、等待父取消、期限到达、panic、Goexit，共72场景；另Adaptive七种十次反馈场景及一次满额取消拒绝场景，共80场景。严格检查原样错误身份、业务次数、context值/身份或TraceID派生、零额外虚拟等待或精确3秒期限、panic/Goexit不普通返回；Adaptive还独立耗尽容量确认许可归还与反馈后容量6/2/5。取消但业务返回nil按成功记录；panic/Goexit归还许可但不执行普通结果记录。拒绝不得调用业务、归还他人许可或计入反馈。

最终加强错误身份版本三环境各25轮通过：Go1.25race2.335s（ratelimit-production-terminal-go125.log）、Go1.26race2.259s（ratelimit-production-terminal-go126.log）、Windows386实际0.980s（ratelimit-production-terminal-386.log）。较早26场景以及后续八段80场景的兼容版本日志保留为历史。六可编译overlay错误：忽略生产错误、忽略Adaptive错误、错误业务context、遗漏Adaptive释放、错误反馈、拒绝时释放他人许可，均被指定行为断言检出；manifest在.gotmp/ratelimit_production_mutants/manifest.json，最终错误身份版本已重跑全部六项通过，日志ratelimit-production-terminal-mutants.log。

相关ratelimit/docs/工具short audit race4.658/6.758/1.698s通过（ratelimit-doc-final-packages.log）；1388声明调用及3完整程序编译、生成一致性、既有88局部原文编译通过（ratelimit-doc-api-compile.log、ratelimit-doc-fragments.log）。本次八段均不在旧88或此前17段中，去重原文片段由105增至113；公开Go Examples仍93。1415方法四项结构、378本地锚点与go vet/diff检查通过；方法结构齐全不等于逐方法语义已经穷尽。

原文公开fixture没有取得生产局部rl句柄，不以该fixture单独声称已验证Close内部信号；既有TestLimiterScopeClosesWaitersWithoutJoiningBusiness检查关闭停止补充/唤醒等待且不等待已开始业务。本轮库实现、根包测试未改，沿用第24项根包441项/全库普通short证据，不将旧二进制算作新原文测试。独立审查工具仍在参数解析报missing field message，未启动审查者。Linux实际运行、Windows非short百万race资源和剩余逐方法语义复核边界仍开放，目标active。

## 限流并发等待、策略切换与 README 异常释放复核（2026-11-10）

本轮继续审查剩余原文，未改库运行实现，累计仍24项。Wait原文只记录等待错误日志且丢弃业务错误；Acquire忽略两次业务结果，策略示例也忽略业务错误。docs/scripts/check_ratelimit_wait_examples.py保留原文先复现，ratelimit-wait-red.log记录业务/等待取消误nil成功。第一版替身用可变context兼容旧无参数业务调用，最终固定doRequest(context.Context) error。最初错误地假设策略段末尾Token获取会再调用一次业务，已修正为实际一次业务；不将测试假设问题计为库缺陷。

Wait原文改为每个已接受业务以独立索引登记error，WaitGroup完成后errors.Join等待与业务错误，等待失败只停止后续启动。Acquire两次业务使用局部作用域及时defer释放，业务失败停止后续步骤；第二次获取错误保持%w包装。策略示例第一段业务结束释放后再展示Token获取，保持一次业务调用。三段均补参数、typed业务签名、返回、导入、生产预算/共享策略及取消注意。Wait的100次业务错误使用每次独立错误身份，检查全部100个错误而非仅一条哨兵；排队中取消时先gate阻塞10个已接受业务，确认父取消只能结束后续等待、函数仍等这10个业务完成，之后同时保留取消与10个业务错误。

三段原文31执行场景：Wait成功/业务失败/父预取消/排队中取消后业务失败/排队中取消后业务成功共5组；Acquire两种峰值参数各8组、策略两种峰值各5组，共26串行场景。前者isPeakHour未使用，保留共同fixture参数，不算不同库路径；按行为去重共23场景。固定typed及错误身份版本三环境各25轮通过：Go1.25race3.068s（ratelimit-wait-typed-go125.log）、Go1.26race2.088s（ratelimit-wait-typed-go126.log）、386实际0.974s（ratelimit-wait-typed-386.log）。六可编译错误变体为漏等待错误、漏业务错误、跳过WaitGroup、传错context、跳过业务、忽略第一段业务错误，均被指定行为断言检出；manifest在.gotmp/ratelimit_wait_mutants/manifest.json。首轮漏等待错误变体因waitErr未使用编译失败，明确不计行为检出；补未使用值接收后六项重跑全通过（ratelimit-wait-mutants-final.log）。

新增internal/ratelimit/strategy_snapshot_contract_test.go：普通/一片/三片乘Acquire/Wait/Token，等待中的Block或BlockForce切换至另外策略，随后取消并释放或关闭，共72组；另3×3×3×3共81组新获取，耗尽额度后逐个核对Block响应取消、Reject拒绝及BlockForce等待直到Close。验证旧调用保存策略快照，新调用读取新策略，不用调度sleep。两项共153组三环境各100轮通过：Go1.25race3.371s、Go1.26race3.812s、386实际1.276s（ratelimit-strategy-terminal-go125.log、-go126.log、-386.log）。忽略策略设置、Force误用Block、Block误用Force三个可编译overlay均被旧调用契约检出（ratelimit-strategy-mutants.log），manifest在.gotmp/ratelimit_strategy_mutants/manifest.json。新增测试本身不代表新运行缺陷。

README Adaptive旧段调用无context、吞业务错误，并在业务之后普通Release；Goexit会跳过释放。先以严格签名得到不传context及命名返回被局部err遮蔽的编译错误（ratelimit-readme-context-red.log），该命名返回是初版fixture为兼容旧bare return选择的支架，不能当库编译错误。README改为error函数语义、成功获取后立即defer释放、传ctx、业务错误原样返回与正常结果单次记录；补参数/生产复用/取消/异常统计边界。同步check_markdown_fragments.py的README typed替身及error作用域。

check_ratelimit_production_examples.py扩展README完整原文与同段获取至结果记录的复用业务后缀，不改任何原文语句；后缀使用传入Adaptive，在Goexit/panic返回后独立耗尽容量确认许可归还。既有80场景加完整及后缀各9场景，共98场景；三环境各25轮通过，Go1.25race4.095s、Go1.26race3.005s、386实际1.131s（ratelimit-readme-shared-go125.log、-go126.log、-386.log）。三个后缀可编译变体：Release移到业务之后、吞业务错误、错传context，均被许可/错误/context断言检出（ratelimit-readme-mutants.log）；manifest在.gotmp/readme_adaptive_mutants/manifest.json。Release旧结构由Goexit行为变体明确复现，不把前述编译红测试当释放行为证据。

相关ratelimit/docs/工具short audit race4.707/6.715/1.977s通过（ratelimit-wait-final-packages.log），最后新增新获取81组后又单独重跑完整限流包race4.425s（ratelimit-strategy-terminal-package.log）。1388声明调用及3完整程序编译、生成一致性、88既有原文编译通过（ratelimit-wait-readme-api.log、ratelimit-wait-readme-fragments.log）。三段新原文使去重片段113增至116；README已在旧88内，业务后缀不重复计数，93公开Examples不变。1415方法四项结构和378锚点通过。根包实现和根包测试未改，沿用第24项证据；不声称旧根包二进制执行过新增内部契约。独立审查工具既有参数解析问题仍开放；Linux实际运行、Windows非short百万race及剩余逐方法语义覆盖仍待推进。

## 非阻塞限流原文与 Linux 实际执行（2026-11-10）

本轮未改库运行实现，累计仍24项。TokenBucket Allow/AllowN、普通SlidingWindow Allow/AllowN及分片窗口批量示例共五段忽略业务error，拒绝后外围返回nil。旧TokenBucket Allow原文每次构建满桶，不适合作为跨请求全局限流。docs/scripts/check_nonblocking_limiter_examples.py先执行旧原文，nonblocking-limiter-red.log记录错误、拒绝及共享额度行为失败；原文没有承诺error函数，测试明确采用生产调用方需要观察拒绝/业务错误的契约，不将其归为库实现缺陷。初版可变context替身仅兼容旧无参数调用，最终固定context签名。

五段改为使用调用方持有的共享实例，额度拒绝时返回应用预定义errRateLimited，接受后向业务传ctx并原样返回错误。明确该哨兵不是async导出常量；批量业务只调用一次，消费份数与业务批量关系由应用管理。补参数、签名、返回、导入和生产建议，明确Allow无context、不响应父取消、不执行或等待业务，业务失败/取消/panic/Goexit不退额度，没有Release/Close；实例不可每请求重建。分片示例打印对应4片100份配置，实际只申请所选片5份。

最终五段各11个结果共55场景：成功、error、预取消、取消-error、取消-success、等待取消、期限、panic、Goexit、拒绝、已取消且拒绝；分别检查错误身份、业务次数、context身份/值、虚拟时间与异常不普通返回。五段各7结果的耗尽模型共35场景，逐次成功/失败/预取消/取消-error/取消-success/panic/Goexit后额度不退，达到独立期望次数后拒绝且不再业务。另5组恢复边界：令牌桶1秒/10秒及窗口1小时之前1ns仍拒绝，正好边界通过，共95场景。三环境各25轮：Go1.25race2.188s、Go1.26race2.233s、386实际1.008s（nonblocking-limiter-terminal-go125.log、-go126.log、-386.log）。较早80场景为历史。六可编译错误变体吞业务错误/吞拒绝/错context/每调用重建桶/批量额度错/跳过业务均被行为断言检出，最终manifest在.gotmp/nonblocking_limiter_mutants/manifest.json，日志nonblocking-limiter-terminal-mutants.log。

相关ratelimit/docs/工具short audit race4.492/6.427/2.100s通过（nonblocking-limiter-final-packages.log）；1388声明调用、3完整MD程序编译、生成一致性及88既有原文编译通过（nonblocking-limiter-api-compile.log、nonblocking-limiter-fragments.log）。五段新原文由116增至121去重片段，公开Go Examples仍93。1415方法四项结构及378本地锚点检查通过。

本机WSL现可运行docker-desktop发行版（Linux6.18.54.1、x86_64、musl）；Docker daemon不可连接不影响直接执行静态Go测试。新增docs/scripts/check_linux_packages.py，以Windows Go1.25交叉编译linux/amd64、CGO_ENABLED=0、audit标签，再实际执行short/count1，保存各二进制SHA256/退出码/日志。非Linux race证据。十个内部包全部通过；根包同一脚本运行中，manifest在.gotmp/linux_library_short_actual。首轮PowerShell把-test.short参数误传成-test，改用Linux shell原样传参解决；保留nonblocking-ratelimit-linux.log失败与-final.log通过。

工具包测试会启动go，初轮WSL缺编译器报环境错误（linux_short_actual/docs_cmd_apidoc.log）。下载官方go1.25.0.linux-amd64.tar.gz并对go.dev元数据校验SHA256=2852af0cb20a13139b3448992e69b868e50ed0f8a1e5940ee1de9e19a123b613，解压仅在.gotmp/linux_go_toolchain，不安装系统包。继承Windows PATH含空格的一次工具包启动exit127，改固定Linux PATH后通过。最终docs及工具包实际通过2.650/2.973s，test辅助包无测试二进制，manifest在.gotmp/linux_docs_final_actual。三个限流原文包也在Linux实际通过（.gotmp/linux_limiter_docs_actual），分别95/98/31场景；单轮普通执行，不冒称25轮或race。

Linux终态补记：根包实际普通short通过，耗时418.665s。将linux_library_short_actual与linux_docs_final_actual的清单并集对照当前go list ./...，14包无重漏，其中13二进制实际执行通过，test辅助包无测试。逐项核验run_exit=0、最终PASS、无失败测试条目、二进制SHA256一致，汇总在.gotmp/linux_short_verified_union.json。核对脚本最初对所有FAIL文本一概拒绝，误识别工具包故意编译错误程序的预期子进程输出；改按顶层退出码、测试条目和最终PASS核对后通过，未忽略真实测试失败。此证据关闭当前版本Linux普通short实际运行缺口；Linux race、Windows非short百万race及剩余逐方法语义边界仍开放。全库go vet及最终文档结构/锚点/diff检查通过，所有本轮进程均已结束。

## Token 响应处理原文与并发释放生命周期复核（2026-11-10）

本轮未改库运行实现，累计仍24项。docs/ratelimit.md的Token/RPC原文忽略响应处理错误；check_token_response_example.py先执行旧原文，在token-response-red.log记录业务错误、取消和期限误返回nil。原文现向processResponse传同一ctx并原样返回错误，补两个业务函数签名、参数、响应指针传递、共享实例、跨两步持有许可及defer异常收尾说明。目录Adaptive链接此前指向普通Acquire，改为实际Adaptive Acquire锚点。

新脚本逐字提取该段，只补函数作用域、imports、输入和typed服务替身。16场景覆盖成功/预取消及两个步骤的error、取消-error、取消-success、等待取消、期限、panic、Goexit；检查步骤次数、错误身份、RPC响应指针、输入、context身份和值、零额外等待或精确3秒期限、异常不普通返回。第一步取消后成功仍进入第二步，由第二步响应取消；第二步取消后成功保留nil。最终三环境各25轮通过：Go1.25race1.962s、Go1.26race1.976s、386实际0.967s（token-response-green.log、token-response-go126.log、token-response-386.log）。吞两步骤错误、错响应指针、错context四个可编译overlay均被指定行为断言检出，manifest在.gotmp/token_response_mutants/manifest.json。

新增internal/ratelimit/token_lifecycle_contract_test.go，普通/一片/三片乘释放、扩容、缩容、补充、关闭共15组。先取得绑定首片的Token并耗尽所有片，独立检查各片余额；32个goroutine并发Release只能归还一次且归属正确，再耗尽后重复32次释放及分片外层Release不得恢复额度。失败Token须nil并返回相应拒绝/关闭错误。最初补充边界读取先于ticker完成导致支架失败，已使用synctest.Wait确定等待；将一小时周期改为一秒以减少虚拟tick数量。失败日志token-lifecycle-go125.log保留，不计为库缺陷。最终三环境各100轮通过：Go1.25race2.277s、Go1.26race2.321s、386实际1.141s（token-lifecycle-final-go125.log、-go126.log、-386.log）。遗漏释放、重复释放、错片三个可编译overlay检出，manifest在.gotmp/token_lifecycle_mutants/manifest.json。

两个新测试包Linux普通实际通过（.gotmp/linux_token_contract_actual/manifest.json），限流包1.756s、原文包0.092s，单轮short/count1。相关ratelimit/docs/工具short audit race4.522/7.476/2.134s通过（token-final-packages.log）。1388声明调用、3完整Markdown程序、生成一致性、88既有原文编译通过（token-final-api-compile.log、token-final-fragments.log），1415方法四项结构、378本地锚点及全库vet/diff检查通过。Token新原文不在旧88及此前专题片段中，去重121增至122；93公开Examples不变。

独立review工具仍在参数解析报missing field message，未启动审查者；没有独立结论。根包运行实现及根包测试未改，沿用第24项根包证据，不把旧二进制算作新内部契约的执行。Linux race、Windows非short百万race资源及剩余逐方法语义仍需继续，目标active。

## 通用工具取消链与类型化 nil 错误识别复核（2026-11-10）

本轮未改库运行实现，累计仍24项。docs/config.md的SafeCall错误分类示例仅检查errors.As成功便解引用PanicError.Value/Stack；业务直接返回、包装或Join类型化nil的*PanicError均会再次panic。docs/scripts/check_panic_logging_example.py逐字提取旧原文，panic-logging-red.log在typed-nil/wrapped-nil/joined-nil三场景复现nil解引用。修正为As成功且指针非nil，补参数、签名、imports、错误处理和解引用注意。该问题在示例而非SafeCall运行实现，不加为第25项运行缺陷。

最终九场景为成功、业务错、实际panic、直接PanicError、包装PanicError、Join PanicError及上述三种nil错误链；逐项检查一次业务调用、输入/context身份及日志分类。三环境各25轮通过：Go1.25race1.984s、Go1.26race2.264s、386实际0.877s（panic-logging-green-go125.log、-go126.log、-386.log）。真实panic仍由SafeCall包装并记录，其余错误身份规则未修改。

新增internal/core/merge_cancel_contract_test.go：旧/新回调各nil/return/panic/Goexit的16种组合，另嵌套重复与64并发调用两组，共18组。检查new先于old、nil返回/调用panic、原panic身份、Goexit不普通返回、new异常时old不执行、嵌套逆序及每次并发调用均执行两个回调并取消真实context。三环境各100轮通过：Go1.25race2.415s、Go1.26race2.025s、386实际0.689s（merge-cancel-contract-go125.log、-go126.log、-386.log）。没有把任意自定义回调异常后的old不执行视为库bug或擅改为defer，保留同步顺序与既有行为。生成器同步说明异常边界，已重新生成async/core方法参考。

MergeCancel/WithConfig/Must专题补参数、使用示例、生产建议与注意事项。新增docs/scripts/check_core_utility_examples.py执行三段原文：MergeCancel真实父子context顺序1组、WithConfig四输入4组、Must成功/空值成功/业务错/类型化nil4组，共9组。三环境各25轮通过：Go1.25race1.824s、Go1.26race1.919s、386实际1.038s（core-utility-examples-go125.log、-go126.log、-386.log）。WithConfig实际创建Group后defer Close；不在原文中创建极大worker数。八个可编译overlay：颠倒取消顺序、旧取消移到defer、添加once、遗漏old、去nil保护、错误使用类型断言替代As、忽略worker配置、忽略Must错误均被特定行为断言检出，manifest在.gotmp/core_utility_mutants/manifest.json（core-utility-mutants-final.log）。

相关core/docs/工具short audit race2.278/6.739/1.950s通过（core-utility-final-packages.log）；1388声明调用、3完整Markdown程序、生成一致性、88既有原文编译、1415方法四项结构、378锚点、全库vet/diff通过（core-utility-api.log、core-utility-fragments.log）。本轮四段原文均不在旧88或此前专题脚本，去重122增至126；93公开Examples不变。

Linux C编译条件已补齐：使用WSL apk的签名包索引，将gcc15.2/musl-dev/binutils及依赖安装在.gotmp/linux_c_toolchain，--root/--no-scripts，不改发行版系统包；.gotmp/linux_cc.sh用局部sysroot及动态库路径。Token生命周期Linux race100轮1.407s（linux-token-race-first.log），MergeCancel契约Linux race100轮1.163s，错误识别与Token原文各race25轮1.066/1.072s（linux-core-utility-race.log）。四包单轮race又用新docs/scripts/check_linux_race.py通过，manifest在.gotmp/linux_utility_verified_race，含前置普通源码hash及生成fixture补记hash/记录时点。新脚本只使用现成Linux工具链，记录JSON事件、源码hash、包清单、退出码及失败条目，不安装系统组件。

全库Linux short audit race已启动（linux-full-short-race.jsonl、linux-full-short-race.stderr.log），根包完成前仍不能算通过；根包二进制hash保存在linux-full-short-race-binaries.json。该进程启动在新增MergeCancel契约之前，新增契约由独立Linux命令证明，不能倒填二进制覆盖。独立审查仍未启动，Windows非short百万race及剩余逐方法语义开放，目标active。

Linux race终态补记：上述全库命令exit0，JSON事件无fail；14包清单与当前go list ./...核对无重漏，13测试包pass、辅助包无测试skip，汇总linux-full-short-race-outcomes.json。包耗时：根包670.563/docs5.661/工具11.216/core1.263/group46.961/maps1.870/pipeline18.640/pool19.582/ratelimit3.541/retry3.167/shard10.109/sliceops12.554/task3.247s。根包二进制SHA256为47e1250f95c154c15e76efe3fa3ebc5c9d6ca1659f180a7bb00447e1a55605a8；运行实现未在此期间修改，MergeCancel新增契约由独立命令覆盖。最终脚本含前置生成fixture哈希的四包race也通过（linux_utility_verified_final/manifest.json），core1.240/工具原文1.023/错误识别1.021/Token原文1.034s，无选定run过滤。

Windows同一Token百万用例独立重跑exit66，windows-token-million-race-isolated.log及windows_token_million_race_report.30896记录ThreadSanitizer分配8MiB失败、error1455，无DATA RACE报告。诊断最初写入包工作目录internal/ratelimit/.gotmp，已复制到根.gotmp保留；清理副本被执行策略拒绝，未变通删除，副本仍在原位置。不能以此失败修复库逻辑或修改系统分页配置。同一用例Linux非short race实际通过，子用例19.96s、包21.257s（linux_token_million_race/manifest.json，verified=true），严格匹配原百万子用例，不减小规模。限流包完整非short Linux race另在运行，不能把单用例通过扩张为全部非short通过。

限流包完整非short Linux race终态：go test -race -tags=audit -p=1 -count=1 -timeout=1800s -json ./internal/ratelimit实际exit0，包144.504s，全部JSON测试事件无fail，manifest在.gotmp/linux_ratelimit_nonshort_race/manifest.json，verified=true。包括原有百万档，不降低规模，不代表Windows同规模通过。其他九个内部包另以同一参数开始非short Linux race（linux_internal_nonshort_race/manifest.json），仍在运行。全库根包非short race不在这两命令范围；已有全库short证据不能替代。

## 聚合辅助函数原文错误与已接受业务收尾复核（2026-11-10）

docs/config.md的FillNoResultSkipped/BuildAggregateNoResult示例仅读取统计并把四个变量赋给空白标识符，未向调用方传递提交及业务失败。新增docs/scripts/check_aggregate_example.py逐字提取，以返回error的生产作用域先执行旧原文；aggregate-example-red.log复现业务失败、预取消、panic及Goexit结果返回nil。修正示例先保存submitErr，最终Wait后补齐，再从占用槽位收集全部错误，以errors.Join返回；补defer Close、参数、签名、imports、浅拷贝/空槽/合成ErrSkipped/内存预算及取消不回滚说明。运行实现未修改，不增加运行bug计数。

七场景包括空输入、成功、每项独立业务错误、父预取消、单项panic、单项Goexit、业务运行中取消。逐项核对业务次数、context值、每条错误身份、PanicError值与ErrTaskGoexit；运行中取消用gate阻塞已开始业务，取消后确认函数仍未返回，放行后保留每个已开始项的错误，不对不确定的排队接受数量作错误假设。最终三环境各25轮通过：Go1.25race1.651s、Go1.26race1.914s、386实际0.571s（aggregate-example-terminal-go125.log、-go126.log、-386.log）。五可编译变体：丢全部错误、仅首错、错context、跳业务、跳Wait均被指定行为断言检出，manifest在.gotmp/aggregate_example_mutants/manifest.json，日志aggregate-example-mutants-final.log。首次跳业务因局部item未使用编译失败，补未用值接收后重跑五项通过，编译失败不计行为检出。

原文Linux race单轮通过1.039s，manifest在.gotmp/linux_aggregate_example_race/manifest.json，源码含生成fixture在执行前记录hash。docs/工具short audit race6.886/1.907s通过（aggregate-example-packages.log），1388声明调用及3程序编译、生成一致性、88既有局部片段编译、1415方法四项结构、378锚点、全库vet/diff检查通过（aggregate-example-api.log、aggregate-example-fragments.log）。这段不在旧88及其他专题脚本，去重126增至127，公开Examples仍93。独立审查再次在派发前参数解析missing field message，未启动审查者；目标active，文档结构与有限模型不等于全库绝对无bug。

Linux内部非short终态补记：另外九包race全部exit0、manifest verified=true，core2.676/group46.937/maps6.798/pipeline57.402/pool19.630/retry28.782/shard37.834/sliceops44.333/task143.547s。与ratelimit144.504s并集为10内部包，无重漏、全部pass，运行前记录的internal源码哈希与当前一致，汇总.gotmp/linux_internal_nonshort_verified_union.json。此结果包括原有百万任务，不代表根包全部非short race或Windows同规模通过。

Linux fuzz诊断：四worker、GOMAXPROCS=4、race、FuzzTokenBucketTimedLedger、目标60s，实际47.940s在342060次执行后最小化worker EOF（linux-tokenbucket-ledger-race-fuzz.log）。没有模型行为断言或DATA RACE报告。保存语料c35928bd074df0fe在Linux race100轮1.692s、Windows race100轮2.421s通过（linux-bucket-fuzz-replay.log、windows-linux-bucket-seed-replay.log）。内核日志linux-tokenbucket-oom-kernel.log明确global_oom及Killed process 6760 (ratelimit.test)，四worker各约3.6至4.0GiB RSS、4GiB swap耗尽，与worker终止对应；不能将资源EOF算通过或直接归为库bug。未调整系统内存/交换配置，保留语料作重放证据，不称可复现业务失败。改为GOMAXPROCS=2、单worker、15s批次，30287次、18.192s实际exit0（linux-tokenbucket-ledger-race-oneworker.log）；这不替代原60s四worker成功，也不反证所有历史EOF的根因。当前所有Linux长测和fuzz进程已结束。

新Linux runner补充选择校验：给定-run表达式时要求至少一个实际run事件，记录executed_tests。故意不存在的表达式顶层go退出0但脚本verified=false/退出1（linux_race_no_match_probe），有效MergeCancel表达式verified=true（linux_race_match_probe），两探针符合预期；不将无匹配测试计通过。剩余逐方法语义、独立审查工具及Windows百万race平台资源边界继续保留，目标active。

## Task 生产工作流原文与批次边界复核（2026-11-10）

新增docs/scripts/check_task_production_example.py，逐字提取docs/task.md生产推荐配置完整代码块，仅补imports、error函数作用域、输入、typed业务替身及未用值接收。25组成功/预取消/各步骤error、panic、Goexit、取消后error或success、期限场景，再加0/1/100/256/257/513六组批次边界和第二批失败/首错后延迟业务收尾两组，共33组。检查输入、context值和期限、异常错误身份、业务计数、成功项顺序、批次停止、返回前active归零及正超时子context清理。支付10秒取消早于60秒任务期限；最后监控不等待是示例明确契约，fixture另外等待上报完成再读取观测。

首轮把取消与空闲Bounded许可同时可用时的回调数固定、把支付期待为60秒DeadlineExceeded以及未等待监控完成，均是支架预期错误，task-production-first.log保留；386重复执行又暴露cancel-error批次可少于3次回调，已按实际合法范围修正，不改库取消选择语义。新增延迟业务使提前返回错误变体被accepted batch not drained断言检出。最终三环境各25轮通过：Go1.25race2.265s、Go1.26race1.896s、386实际0.711s（task-production-final-go125.log、-go126.log、-386.log）。八可编译overlay吞RPC/支付错误、错context、遗漏timeout、错误定时取消、遗漏handle、首错立即返回、跳第二批，均被指定行为断言检出（task-production-mutants-final.log、task_production_mutants/manifest.json）；吞错断言在等待监控前检查，避免以支架死锁代替检出。Linux race单轮通过，前置fixture源码hash在linux_task_production_final_race/manifest.json。

docs/task.md补参数/业务函数签名、首错与全部错误的区别、共享固定Builder、独立容量、父请求预算、取消接受竞争、不可回滚副作用及监控结果不可见边界。task/docs/工具short audit race4.591/6.641/1.913s通过（task-production-packages.log）。1388声明调用、3完整程序、88既有原文编译及1415方法四项结构、378锚点、vet/diff检查通过。该原文已在旧88内，127去重计数与93公开Examples不变；运行实现未改，累计24项库运行修复。独立review派发仍在参数解析missing field message，未启动审查者。

## Pool 生产原文提交错误遮蔽与回调首错复核（2026-11-10）

docs/production.md完整Pool示例虽等待已接受任务，但立即返回submitErr，遮蔽outcomeErr中已记录的失败。新增docs/scripts/check_pool_production_example.py按原文百万提交目标及原容量执行，以业务提前失败和父取消结束测试，不替换工作量常量。旧原文在cancel-error/panic/goexit三场景只返回context canceled（pool-production-red.log）。现Wait后加锁读取outcomeErr，再errors.Join(submitErr, taskErr)；同时保留可查询的提交错误与回调首错。补参数/业务签名/imports、错误返回、worker/排队/结果预算、有限保留、取消协作及回调重入注意。88片段编译支架补errors导入。

重复验证发现多个回调及拒绝同时发生时，outcomeErr确实可能先记录取消或拒绝，不能声称总保留特定业务错误。fixture改为第一个业务先失败、其他业务阻塞、1秒虚拟定时父取消；迟发错误另延迟100毫秒，以确定先记录目标错误。文档明确只保留回调首个观察错误，可能来自提交拒绝，不汇总全部业务错误。这是支架顺序修正，不是新增库缺陷。最终六场景预取消、取消成功、error/panic/Goexit、迟发错误，三环境各25轮通过：Go1.25race2.237s、Go1.26race2.250s、386实际0.881s（pool-production-final-go125.log、-go126.log、-386.log）。Linux race单轮通过，manifest在linux_pool_production_final_race。

三个可编译overlay恢复提交错误优先、遗漏回调错误登记、业务context值错误，均被指定行为断言检出（pool-production-mutants-final.log、pool_production_mutants/manifest.json）。初轮一个未用taskErr编译失败、错context导致大量panic与超时不计检出，已补未用值接收、只让首业务检查错误值后重跑。删除Wait变体未被此fixture检出，记录为模型边界且不计入三项；Run的最终Close仍会等待，不以该变体通过推断Wait无用。

docs/工具short audit race6.576/1.661s通过（pool-production-packages.log），1388声明调用、3完整程序及88既有原文片段编译通过（pool-production-api.log、pool-production-fragments.log）。该原文在旧88内，去重127、公开Examples93不变；累计24项库运行修复，未改库运行实现。根包完整非short Linux race正在执行，记录linux_root_nonshort_race/manifest.json及events.jsonl，完成前不计通过；全库short及十内部包非short Linux race既有证据保持。目标active，独立审查及剩余逐方法语义继续开放。

## Pipeline 生产配置逐项错误与累计期限复核（2026-11-10）

新增docs/scripts/check_pipeline_production_example.py逐字提取docs/pipeline.md的标准三阶段生产配置，补error函数作用域、typed业务及输入。旧原文只赋值results/err，以正常函数作用域返回顶层err时，预取消、三阶段业务错误/panic/Goexit、取消后error/success、两类期限全部被漏报（pipeline-production-red.log）。该执行路径顶层err为nil不表示逐项成功，原文现errors.Join合并顶层err、返回时父ctx.Err及每项result.Err；不修改库实际返回规则。补参数/imports/业务签名、部分结果、纯计算校验、协作取消、总预算及副作用说明。选型表把“阶段间无依赖”改为单阶段分片调度与复用池，明确后续阶段依赖前阶段输出。

最终23场景：成功/空/预取消各1，三阶段error/panic/Goexit/cancel-error/cancel-success共15，parse/enrich各两类期限共4，第一阶段消耗2分钟、第二阶段剩余3分钟的累计期限1。检查记录ID及阶段输入、context值与5分钟或1秒父期限、步数、错误身份、业务Goexit转换及子context结束清理。三环境各25轮通过：Go1.25race2.171s、Go1.26race1.917s、386实际0.639s（pipeline-production-final-go125.log、-go126.log、-386.log）。单输入用于确定失败停止边界，不声称覆盖全部多输入调度；已有独立Pipeline模型及包级测试另行覆盖。

六个可编译overlay忽略逐项错误、忽略父取消、错误context、遗漏timeout、关闭FailFast、错误阶段名均被指定行为断言检出（pipeline-production-mutants-final.log、pipeline_production_mutants/manifest.json）。Linux race单轮通过，最终23场景含累计预算的前置fixture哈希与事件在linux_pipeline_production_final_race/manifest.json，verified=true。docs/工具short audit race6.768/2.065s通过（pipeline-production-packages.log），1388声明调用、3完整程序及88既有片段编译通过；1415方法四项结构、378锚点、vet/diff再次通过。此原文不在旧88及其他专题脚本，去重127增至128，93公开Examples不变；库运行修复仍24项，目标active。

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
