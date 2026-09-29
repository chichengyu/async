package async

import (
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ── 所有 Builder 和 Chain 的默认值统一从此处引用，确保单一来源 ──

// defaultIO 返回 IO 密集型操作的推荐并发度（全局单一定义）。
func defaultIO() int { return core.IO() }

// defaultShards 水平分片默认值，0 表示运行时自动决定（runtime.GOMAXPROCS(0)，最少 2）。
const defaultShards = 0

// defaultTimeout 默认单任务超时（Map/ForEach/Group/Pool 共用）。
const defaultTimeout = 30 * time.Second

// defaultFailFast 默认 FailFast 开关。
const defaultFailFast = false

// defaultSubmitTimeout 默认提交超时（提交任务到池的等待上限）。
const defaultSubmitTimeout = 5 * time.Second

// defaultStreamingBuf 默认流式缓冲大小，0 表示自适应（concurrency*2）。
const defaultStreamingBuf = 0

// defaultRingBufCap 默认环形缓冲区容量。
const defaultRingBufCap = 4096

// defaultMaxRetries 默认最大重试次数。
const defaultMaxRetries = 3

// defaultInitialBackoff 默认退避初始间隔。
const defaultInitialBackoff = 100 * time.Millisecond

// defaultMaxBackoff 默认退避最大间隔。
const defaultMaxBackoff = 10 * time.Second

// defaultLinearBackoff 默认线性退避间隔。
const defaultLinearBackoff = 1 * time.Second

// defaultPerCallTimeout 默认每次重试调用的超时。
const defaultPerCallTimeout = 5 * time.Second

// defaultBatchSize 默认分块大小（Chunk 操作的单批元素数）。
// 100 是数据库批量操作、RPC 批量调用的业界通用值，兼顾内存与效率。
const defaultBatchSize = 100

// defaultRateLimitBurst 默认限流器突发容量。
const defaultRateLimitBurst = 1
