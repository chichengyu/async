package core

import (
	"sync"
	"sync/atomic"
)

// RingBuffer 固定容量环形缓冲区，线程安全。
// 使用分片设计降低锁竞争：内部按 16 个分片组织数据，
// 每个分片有独立的 mutex，写操作均匀分布到各分片。
type RingBuffer[T any] struct {
	shards   [ringBufShardCount]ringBufShard[T]
	capacity int
	overflow OverflowStrategy
	writeIdx atomic.Uint64 // 全局写入序号，用于分片路由
	readIdx  atomic.Uint64 // 全局读取序号
	dropped  atomic.Int64  // 因 OverflowDrop 被覆盖的元素数
	resetMu  sync.RWMutex  // 保护 Reset 与其他操作之间的并发安全
}

const ringBufShardCount = 16

type ringBufShard[T any] struct {
	mu       sync.Mutex
	buf      []T
	head     int
	tail     int
	size     int
	capacity int // 每个分片的容量
}

// NewRingBuffer 创建指定容量的环形缓冲区。
// capacity 会被向上取整到 ringBufShardCount 的倍数。
func NewRingBuffer[T any](capacity int, overflow OverflowStrategy) *RingBuffer[T] {
	if capacity < 1 {
		capacity = 1024
	}
	// 向上取整到分片数的倍数，每个分片至少 1 个槽位
	perShard := (capacity + ringBufShardCount - 1) / ringBufShardCount
	if perShard < 1 {
		perShard = 1
	}
	rb := &RingBuffer[T]{
		capacity: perShard * ringBufShardCount,
		overflow: overflow,
	}
	for i := range rb.shards {
		rb.shards[i].buf = make([]T, perShard)
		rb.shards[i].capacity = perShard
	}
	return rb
}

// Push 写入一个元素。满时根据策略处理：
// OverflowDrop：覆盖最旧元素
// OverflowBlock：返回 false
// OverflowError：返回 false
func (rb *RingBuffer[T]) Push(val T) bool {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()
	widx := rb.writeIdx.Add(1) - 1
	shardIdx := int(widx % ringBufShardCount)
	s := &rb.shards[shardIdx]

	s.mu.Lock()
	if s.size == s.capacity {
		switch rb.overflow {
		case OverflowDrop:
			rb.dropped.Add(1)
			s.buf[s.head] = val
			s.head = (s.head + 1) % s.capacity
			s.tail = (s.tail + 1) % s.capacity
			s.mu.Unlock()
			return true
		default:
			// Block/Error 策略：写失败不推进写入序号（回退）
			rb.writeIdx.Add(^uint64(0)) // -1
			s.mu.Unlock()
			return false
		}
	}
	s.buf[s.tail] = val
	s.tail = (s.tail + 1) % s.capacity
	s.size++
	s.mu.Unlock()
	return true
}

// Pop 读取并移除最旧元素。空时返回零值和 false。
func (rb *RingBuffer[T]) Pop() (T, bool) {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()
	return rb.popUnsafe()
}

func (rb *RingBuffer[T]) popUnsafe() (T, bool) {
	ridx := rb.readIdx.Load()
	widx := rb.writeIdx.Load()
	if ridx >= widx {
		var zero T
		return zero, false
	}

	shardIdx := int(ridx % ringBufShardCount)
	s := &rb.shards[shardIdx]

	s.mu.Lock()
	if s.size == 0 {
		s.mu.Unlock()
		var zero T
		return zero, false
	}
	val := s.buf[s.head]
	var zero T
	s.buf[s.head] = zero
	s.head = (s.head + 1) % s.capacity
	s.size--
	s.mu.Unlock()

	rb.readIdx.Add(1)
	return val, true
}

// Peek 读取最旧元素但不移除。
func (rb *RingBuffer[T]) Peek() (T, bool) {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()
	ridx := rb.readIdx.Load()
	widx := rb.writeIdx.Load()
	if ridx >= widx {
		var zero T
		return zero, false
	}

	shardIdx := int(ridx % ringBufShardCount)
	s := &rb.shards[shardIdx]

	s.mu.Lock()
	if s.size == 0 {
		s.mu.Unlock()
		var zero T
		return zero, false
	}
	val := s.buf[s.head]
	s.mu.Unlock()
	return val, true
}

// Len 返回当前元素数。
func (rb *RingBuffer[T]) Len() int {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()
	widx := rb.writeIdx.Load()
	ridx := rb.readIdx.Load()
	return int(widx - ridx)
}

// Cap 返回容量。
func (rb *RingBuffer[T]) Cap() int {
	return rb.capacity
}

// OverflowStrategy 返回溢出策略。
func (rb *RingBuffer[T]) OverflowStrategy() OverflowStrategy {
	return rb.overflow
}

// Dropped 返回因 OverflowDrop 策略被覆盖丢弃的元素数。
func (rb *RingBuffer[T]) Dropped() int64 {
	return rb.dropped.Load()
}

// IsFull 返回是否已满。
func (rb *RingBuffer[T]) IsFull() bool {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()
	widx := rb.writeIdx.Load()
	ridx := rb.readIdx.Load()
	return int(widx-ridx) >= rb.capacity
}

// Flush 排空并返回所有元素（FIFO 顺序）。
func (rb *RingBuffer[T]) Flush() []T {
	return rb.FlushN(0)
}

// FlushN 排空并返回最多 n 个元素（FIFO 顺序，保留超出的元素）。
// n <= 0 取出全部。
func (rb *RingBuffer[T]) FlushN(n int) []T {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()
	widx := rb.writeIdx.Load()
	ridx := rb.readIdx.Load()
	total := int(widx - ridx)
	if total == 0 {
		return nil
	}
	if n <= 0 || n > total {
		n = total
	}

	result := make([]T, 0, n)
	for i := 0; i < n; i++ {
		if val, ok := rb.popUnsafe(); ok {
			result = append(result, val)
		} else {
			break
		}
	}
	return result
}

// Reset 清空缓冲区。
func (rb *RingBuffer[T]) Reset() {
	rb.resetMu.Lock()
	defer rb.resetMu.Unlock()
	rb.readIdx.Store(0)
	rb.writeIdx.Store(0)
	rb.dropped.Store(0)
	for i := range rb.shards {
		s := &rb.shards[i]
		s.mu.Lock()
		for j := range s.buf {
			var zero T
			s.buf[j] = zero
		}
		s.head = 0
		s.tail = 0
		s.size = 0
		s.mu.Unlock()
	}
}
