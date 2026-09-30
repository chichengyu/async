// Package sliceops 执行策略定义。
// Policy 封装串行/并行执行模式及所有可选配置（超时、FailFast、水平分片、分块、流式缓冲）。
// 使用值接收者链式调用，每次返回新 Policy（不可变），天然并发安全。
//
// 设计模式：Strategy Pattern —— 将执行算法族封装为可组合的策略对象，
// 消除 MapWithFailFast / MapWithTimeout / MapWithFFTimeout 等组合爆炸。
//
// 使用示例：
//
//	// 串行
//	p := sliceops.Seq()
//
//	// 并行 + FailFast + 超时
//	p := sliceops.Par(8).FF().TO(5 * time.Second)
//
//	// 并行 + 水平分片 + FailFast
//	p := sliceops.Par(64).Shard(16).FF()
//
//	// 并行 + 分块 + 流式缓冲
//	p := sliceops.Par(8).Chunk(100).Buf(1024)
package sliceops

import (
	"runtime"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Policy 执行策略，串行或并行模式 + 可选配置。
type Policy struct {
	serial      bool
	concurrency int
	timeout     time.Duration
	failFast    bool
	shards      int
	chunkSize   int
	bufSize     int
	pool        any  // *pool.Pool[R]，nil 表示不使用外部池
	poolOwned   bool // DefaultPool() 创建的池，使用后需 Close
}

// Seq 创建串行执行策略。
func Seq() Policy {
	return Policy{serial: true, concurrency: 1}
}

// Par 创建并行执行策略。n <= 0 时使用 core.IO() 默认并发度。
func Par(n int) Policy {
	if n <= 0 {
		n = core.IO()
	}
	return Policy{concurrency: n}
}

// DefPar 使用默认 IO 并发度的并行策略便捷方法。
func DefPar() Policy {
	return Par(core.IO())
}

// ──────────────────────────── 链式配置方法 ────────────────────────────

// FF 启用 FailFast：任一任务失败立即终止其余任务。
func (p Policy) FF() Policy {
	p.failFast = true
	return p
}

// NoFF 关闭 FailFast（恢复默认）。
func (p Policy) NoFF() Policy {
	p.failFast = false
	return p
}

// TO 设置单任务超时。
func (p Policy) TO(d time.Duration) Policy {
	p.timeout = d
	return p
}

// Chunk 设置分块大小，逐元素调用 fn，但按块并发调度。
func (p Policy) Chunk(size int) Policy {
	p.chunkSize = size
	return p
}

// Shard 设置水平分片数，n <= 0 时使用 GOMAXPROCS。
func (p Policy) Shard(n int) Policy {
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
		if n < 2 {
			n = 2
		}
	}
	p.shards = n
	return p
}

// Worker 覆盖并发度，n <= 0 时使用 core.IO()。
func (p Policy) Worker(n int) Policy {
	if n <= 0 {
		n = core.IO()
	}
	p.concurrency = n
	return p
}

// Buf 设置流式缓冲区大小。
func (p Policy) Buf(n int) Policy {
	p.bufSize = n
	return p
}

// WithPool 注入外部协程池（*pool.Pool[R]），启用池化执行。
// 用户自行管理池的生命周期（需 defer p.Close()）。
func (p Policy) WithPool(pool any) Policy {
	p.pool = pool
	p.poolOwned = false
	return p
}

// WithOwnedPool 注入外部协程池，但内部接管生命周期。
// 终端方法执行完成后自动 Close，无需用户 defer。
func (p Policy) WithOwnedPool(pool any) Policy {
	p.pool = pool
	p.poolOwned = true
	return p
}

// WithDefaultPool 标记使用默认协程池，执行时自动创建并在 Wait 后 Close。
func (p Policy) WithDefaultPool() Policy {
	p.pool = nil
	p.poolOwned = true
	return p
}

// Pool 返回外部协程池引用，nil 表示未设置。
func (p Policy) Pool() any {
	return p.pool
}

// IsPoolOwned 池是否由内部创建（DefaultPool），使用后需自动 Close。
func (p Policy) IsPoolOwned() bool {
	return p.poolOwned
}

// ──────────────────────────── 内部方法 ────────────────────────────

// isZero 判断是否为未初始化的零值 Policy。
func (p Policy) isZero() bool {
	return p.concurrency == 0 && !p.serial
}

// isSerial 判断是否为串行模式（内部使用）。
func (p Policy) isSerial() bool { return p.serial }

// isParallel 判断是否为并行模式（内部使用）。
func (p Policy) isParallel() bool { return !p.serial }

// ──────────────────────────── 只读访问器 ────────────────────────────

// IsSerial 判断是否串行模式。
func (p Policy) IsSerial() bool { return p.serial }

// IsParallel 判断是否并行模式。
func (p Policy) IsParallel() bool { return !p.serial }

// GetWorker 并发度。
func (p Policy) GetWorker() int { return p.concurrency }

// GetTimeout 超时时间。
func (p Policy) GetTimeout() time.Duration { return p.timeout }

// IsFailFast 是否快速失败。
func (p Policy) IsFailFast() bool { return p.failFast }

// GetShards 分片数。
func (p Policy) GetShards() int { return p.shards }

// GetChunkSize 分块大小。
func (p Policy) GetChunkSize() int { return p.chunkSize }

// GetBuf 流式缓冲区大小。
func (p Policy) GetBuf() int { return p.bufSize }
