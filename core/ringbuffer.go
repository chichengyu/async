package core

import "sync"

// RingBuffer 固定容量环形缓冲区，线程安全。
type RingBuffer[T any] struct {
	buf      []T
	head     int
	tail     int
	size     int
	capacity int
	mu       sync.Mutex
	overflow OverflowStrategy
}

// NewRingBuffer 创建指定容量的环形缓冲区。
func NewRingBuffer[T any](capacity int, overflow OverflowStrategy) *RingBuffer[T] {
	if capacity < 1 {
		capacity = 1024
	}
	return &RingBuffer[T]{
		buf:      make([]T, capacity),
		capacity: capacity,
		overflow: overflow,
	}
}

// Push 写入一个元素。满时根据策略处理：
// OverflowDrop：覆盖最旧元素
// OverflowBlock：返回 false
// OverflowError：返回 false
func (rb *RingBuffer[T]) Push(val T) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.size == rb.capacity {
		switch rb.overflow {
		case OverflowDrop:
			rb.buf[rb.head] = val
			rb.head = (rb.head + 1) % rb.capacity
			rb.tail = (rb.tail + 1) % rb.capacity
			return true
		default:
			return false
		}
	}
	rb.buf[rb.tail] = val
	rb.tail = (rb.tail + 1) % rb.capacity
	rb.size++
	return true
}

// Pop 读取并移除最旧元素。空时返回零值和 false。
func (rb *RingBuffer[T]) Pop() (T, bool) {
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
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.size == 0 {
		var zero T
		return zero, false
	}
	return rb.buf[rb.head], true
}

// Len 返回当前元素数。
func (rb *RingBuffer[T]) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.size
}

// Cap 返回容量。
func (rb *RingBuffer[T]) Cap() int {
	return rb.capacity
}

// IsFull 返回是否已满。
func (rb *RingBuffer[T]) IsFull() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.size == rb.capacity
}

// Flush 排空并返回所有元素（FIFO 顺序）。
func (rb *RingBuffer[T]) Flush() []T {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.size == 0 {
		return nil
	}
	result := make([]T, rb.size)
	idx := 0
	for rb.size > 0 {
		result[idx] = rb.buf[rb.head]
		var zero T
		rb.buf[rb.head] = zero
		rb.head = (rb.head + 1) % rb.capacity
		rb.size--
		idx++
	}
	return result
}

// FlushN 排空并返回最多 n 个元素（FIFO 顺序，保留超出的元素）。
func (rb *RingBuffer[T]) FlushN(n int) []T {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if n <= 0 || n > rb.size {
		n = rb.size
	}
	if n == 0 {
		return nil
	}
	result := make([]T, n)
	for i := 0; i < n; i++ {
		result[i] = rb.buf[rb.head]
		var zero T
		rb.buf[rb.head] = zero
		rb.head = (rb.head + 1) % rb.capacity
		rb.size--
	}
	return result
}

// Reset 清空缓冲区。
func (rb *RingBuffer[T]) Reset() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for i := range rb.buf {
		var zero T
		rb.buf[i] = zero
	}
	rb.head = 0
	rb.tail = 0
	rb.size = 0
}
