package core

import (
	"sync"
	"sync/atomic"
)

// RingBuffer 固定容量环形缓冲区，线程安全。
// 使用单一 mutex 保证所有操作的可线性化，消除分片设计中读写路由不一致的根本矛盾。
type RingBuffer[T any] struct {
	mu       sync.Mutex
	buf      []T
	head     int
	tail     int
	size     int
	capacity int
	overflow OverflowStrategy
	dropped  atomic.Int64 // 因 OverflowDrop 被覆盖的元素数
	resetMu  sync.RWMutex // 保护 Reset 与 Push/Pop 之间的并发安全
}

// NewRingBuffer 创建指定容量的环形缓冲区。
func NewRingBuffer[T any](capacity int, overflow OverflowStrategy) *RingBuffer[T] {
	if capacity < 1 {
		capacity = 1024
	}
	rb := &RingBuffer[T]{
		capacity: capacity,
		overflow: overflow,
	}
	rb.buf = make([]T, capacity)
	return rb
}

// Push 写入一个元素。满时根据策略处理：
// OverflowDrop：覆盖最旧元素
// OverflowBlock：返回 false
// OverflowError：返回 false
func (rb *RingBuffer[T]) Push(val T) bool {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size < rb.capacity {
		rb.buf[rb.tail] = val
		rb.tail = (rb.tail + 1) % rb.capacity
		rb.size++
		return true
	}

	if rb.overflow == OverflowDrop {
		rb.dropped.Add(1)
		rb.buf[rb.head] = val
		rb.head = (rb.head + 1) % rb.capacity
		rb.tail = (rb.tail + 1) % rb.capacity
		return true
	}

	return false
}

// Pop 读取并移除最旧元素。
func (rb *RingBuffer[T]) Pop() (T, bool) {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 {
		var zero T
		return zero, false
	}

	val := rb.buf[rb.head]
	var zero T
	rb.buf[rb.head] = zero
	rb.head = (rb.head + 1) % rb.capacity
	rb.size--
	return val, true
}

// Peek 读取最旧元素但不移除。
func (rb *RingBuffer[T]) Peek() (T, bool) {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 {
		var zero T
		return zero, false
	}

	return rb.buf[rb.head], true
}

// Len 返回缓冲区中当前元素数量。
func (rb *RingBuffer[T]) Len() int {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()

	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.size
}

// IsFull 返回缓冲区是否已满。
func (rb *RingBuffer[T]) IsFull() bool {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()

	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.size >= rb.capacity
}

// Flush 排空并返回所有元素（FIFO 顺序）。
func (rb *RingBuffer[T]) Flush() []T {
	return rb.FlushN(0)
}

// FlushN 批量读取最多 n 个元素。
func (rb *RingBuffer[T]) FlushN(n int) []T {
	rb.resetMu.RLock()
	defer rb.resetMu.RUnlock()

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if n <= 0 || n > rb.size {
		n = rb.size
	}
	result := make([]T, 0, n)
	for i := 0; i < n; i++ {
		val := rb.buf[rb.head]
		var zero T
		rb.buf[rb.head] = zero
		rb.head = (rb.head + 1) % rb.capacity
		rb.size--
		result = append(result, val)
	}
	return result
}

// Disposed 返回因 OverflowDrop 策略而被丢弃的元素总数。
func (rb *RingBuffer[T]) Disposed() int {
	return int(rb.dropped.Load())
}

// Dropped Disposed 的别名，兼容旧 API。
func (rb *RingBuffer[T]) Dropped() int64 {
	return rb.dropped.Load()
}

// Cap 返回缓冲区总容量。
func (rb *RingBuffer[T]) Cap() int {
	return rb.capacity
}

// OverflowStrategy 返回当前溢出策略。
func (rb *RingBuffer[T]) OverflowStrategy() OverflowStrategy {
	return rb.overflow
}

// Reset 清空缓冲区，重置所有指针。
func (rb *RingBuffer[T]) Reset() {
	rb.resetMu.Lock()
	defer rb.resetMu.Unlock()

	rb.mu.Lock()
	defer rb.mu.Unlock()

	var zero T
	for i := range rb.buf {
		rb.buf[i] = zero
	}
	rb.head = 0
	rb.tail = 0
	rb.size = 0
	rb.dropped.Store(0)
}
