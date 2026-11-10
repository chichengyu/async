package task

import (
	"sync"
)

// ──────────────────────────── Mu - 并发安全切片 ────────────────────────────

// Mu 泛型并发安全切片，支持并发追加和快照。
// 适用于多个 goroutine 并发收集结果的场景。
//
// 使用示例：
//
//	var results task.Mu[string]
//	var wg sync.WaitGroup
//	for i := 0; i < 10; i++ {
//	    wg.Add(1)
//	    go func(n int) {
//	        defer wg.Done()
//	        results.Append(func() string { return fmt.Sprintf("result-%d", n) })
//	    }(i)
//	}
//	wg.Wait()
//	all := results.Snapshot() // 获取所有结果的副本
type Mu[T any] struct {
	mu sync.Mutex // 保护 ts 的互斥锁
	ts []T        // 内部切片
}

// Append 线程安全地追加元素。add 函数在锁外执行以保证并发性能，
// append 操作在锁内执行保证线程安全。
// add 须非 nil；其 panic 直接传播且不会追加元素。接收者须非 nil，Mu 使用后不要复制。
// 生成函数可重入本容器；其已完成的嵌套追加和其他副作用不因外层 panic 回滚。
//
// 参数：
//   - add：生成要追加元素的函数（在锁外执行，可并发）
//
// 使用示例：
//
//	var mu task.Mu[int]
//	mu.Append(func() int { return 42 })
//	mu.Append(func() int { return computeSomething() })
func (m *Mu[T]) Append(add func() T) {
	v := add()
	m.mu.Lock()
	m.ts = append(m.ts, v)
	m.mu.Unlock()
}

// Snapshot 返回当前所有元素的浅拷贝，线程安全；nil 接收者返回 nil。
// 元素中的指针、map、切片仍共享，修改其引用内容须另外同步。
// 不等待仍在锁外生成值的 Append；完整收集前须由调用方等待生产者。
//
// 使用示例：
//
//	items := mu.Snapshot()
//	for _, item := range items {
//	    fmt.Println(item)
//	}
func (m *Mu[T]) Snapshot() []T {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]T, len(m.ts))
	copy(out, m.ts)
	return out
}
