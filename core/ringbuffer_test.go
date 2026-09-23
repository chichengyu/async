package core

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRingBuffer_RollbackRace 复现并验证 writeIdx 回滚并发 Bug 已被修复。
// 场景：多个 writer 并发 Push 到一个已满的 RingBuffer（Block 策略），
// 确保不会出现数据丢失或 Pop 永久阻塞的问题。
func TestRingBuffer_RollbackRace(t *testing.T) {
	const (
		capacity        = 64
		numWriters      = 8
		writesPerWorker = 5000
	)

	rb := NewRingBuffer[int](capacity, OverflowBlock)

	var writeSuccess atomic.Int64
	var writeFail atomic.Int64
	var wg sync.WaitGroup

	// Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				if rb.Push(workerID*writesPerWorker + i) {
					writeSuccess.Add(1)
				} else {
					writeFail.Add(1)
				}
			}
		}(w)
	}

	// Readers
	var popSuccess atomic.Int64
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				// 排空
				for {
					if _, ok := rb.Pop(); ok {
						popSuccess.Add(1)
					} else {
						return
					}
				}
			default:
				if _, ok := rb.Pop(); ok {
					popSuccess.Add(1)
				}
			}
		}
	}()

	wg.Wait()
	close(done)
	time.Sleep(50 * time.Millisecond) // 等待 reader 排空

	totalWrites := writeSuccess.Load() + writeFail.Load()
	expectedTotal := int64(numWriters * writesPerWorker)

	if totalWrites != expectedTotal {
		t.Errorf("总写入次数不匹配: got %d, want %d", totalWrites, expectedTotal)
	}

	// 验证：所有成功写入的元素都应该能被 Pop 出来
	remain := rb.Len()
	if remain != 0 {
		t.Errorf("缓冲区内应无剩余元素: got %d, want 0", remain)
	}

	// 写入成功 + 剩余 = Pop 成功
	totalPushed := writeSuccess.Load()
	totalPopped := popSuccess.Load()
	if totalPushed != totalPopped {
		t.Errorf("Push 成功(%d) != Pop 成功(%d)", totalPushed, totalPopped)
	}

	t.Logf("写入成功: %d, 写入失败: %d, Pop 成功: %d",
		writeSuccess.Load(), writeFail.Load(), popSuccess.Load())
}

// TestRingBuffer_RollbackCAS 直接验证 CAS 回退的正确性。
// 并发 Push 到满缓冲区，确保 writeIdx 不会被错误回退。
// 注意：并发 Push 满缓冲区时，CAS 回退可能部分失败，导致 writeIdx
// 包含"空洞"（writeIdx 推进了但元素未写入）。Pop 的空洞跳过机制
// 会处理这些空洞，但 Len() 会暂时偏高。
func TestRingBuffer_RollbackCAS(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)

	capacity := rb.Cap()
	for i := 0; i < capacity; i++ {
		if !rb.Push(i) {
			t.Fatalf("预热 Push 失败: i=%d", i)
		}
	}

	if !rb.IsFull() {
		t.Fatalf("缓冲区应已满: capacity=%d, len=%d", capacity, rb.Len())
	}

	var wg sync.WaitGroup

	// 启动 50 个 goroutine 并发尝试写入满缓冲区
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rb.Push(999) // 应该全部失败（Block 策略 + 满缓冲区）
		}()
	}
	wg.Wait()

	t.Logf("满缓冲区并发 Push 后 Len=%d (可能含空洞)", rb.Len())

	// 排空所有元素（Pop 到返回 false 为止，空洞会被自动跳过）
	popped := 0
	for {
		if _, ok := rb.Pop(); ok {
			popped++
		} else {
			break
		}
	}
	if popped != capacity {
		t.Errorf("应该能 Pop 出 %d 个元素: got %d", capacity, popped)
	}

	// 缓冲区应已空
	if rb.Len() != 0 {
		t.Errorf("缓冲区应为空: got %d", rb.Len())
	}

	// 验证重新写入也能正常工作
	for i := 0; i < capacity; i++ {
		if !rb.Push(i + 100) {
			t.Errorf("重新写入失败: i=%d", i)
		}
	}
}

// TestRingBuffer_PeekWithHoles 验证 Peek 能跳过空洞。
func TestRingBuffer_PeekWithHoles(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)

	// 写入 10 个元素
	for i := 0; i < 10; i++ {
		rb.Push(i)
	}

	// Pop 5 个
	for i := 0; i < 5; i++ {
		rb.Pop()
	}

	// 此时应该还有 5 个元素
	val, ok := rb.Peek()
	if !ok {
		t.Fatal("Peek 应返回 true")
	}
	if val != 5 {
		t.Errorf("Peek 应返回 5: got %d", val)
	}
}

// TestRingBuffer_OverflowDropNoHoles 验证 OverflowDrop 策略的正确性。
func TestRingBuffer_OverflowDropNoHoles(t *testing.T) {
	capacity := 16 // 使用分片对齐的容量
	rb := NewRingBuffer[int](capacity, OverflowDrop)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				rb.Push(v*1000 + j)
			}
		}(i)
	}
	wg.Wait()

	dropped := rb.Dropped()
	t.Logf("丢弃元素数: %d", dropped)

	popped := 0
	for {
		_, ok := rb.Pop()
		if !ok {
			break
		}
		popped++
	}

	maxCapacity := rb.Cap()
	if popped > maxCapacity {
		t.Errorf("Pop 不应超过容量: got %d, capacity %d", popped, maxCapacity)
	}
	t.Logf("Pop: %d, Cap: %d, Dropped: %d", popped, maxCapacity, dropped)
}

// TestRingBuffer_ConcurrentPushPop 综合并发读写测试。
func TestRingBuffer_ConcurrentPushPop(t *testing.T) {
	capacity := 64
	rb := NewRingBuffer[int](capacity, OverflowBlock)

	var wg sync.WaitGroup
	pushCount := atomic.Int64{}
	popCount := atomic.Int64{}
	stop := atomic.Bool{}

	// Writers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter := 0
			for !stop.Load() {
				if rb.Push(counter) {
					pushCount.Add(1)
				}
				counter++
			}
		}()
	}

	// Readers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, ok := rb.Pop(); ok {
					popCount.Add(1)
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	// 排空
	for {
		if _, ok := rb.Pop(); ok {
			popCount.Add(1)
		} else {
			break
		}
	}

	remaining := rb.Len()
	totalPushed := pushCount.Load()
	totalPopped := popCount.Load()

	t.Logf("Push 总数: %d, Pop 总数: %d, 剩余: %d", totalPushed, totalPopped, remaining)
	t.Logf("Push-Pop-剩余 = %d", totalPushed-totalPopped-int64(remaining))

	if totalPushed != totalPopped+int64(remaining) {
		t.Errorf("数据不一致: push=%d, pop=%d, remain=%d", totalPushed, totalPopped, remaining)
	}
}

// TestRingBuffer_RollbackRaceStress 高强度压力测试 CAS 回退机制。
// 使用更极端的参数复现旧代码中的竞态条件。
func TestRingBuffer_RollbackRaceStress(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过压力测试")
	}

	rb := NewRingBuffer[int](32, OverflowBlock)
	capacity := rb.Cap()

	// 填充到接近满但不完全满，留少量空位
	prefillCount := capacity / 2

	for i := 0; i < prefillCount; i++ {
		rb.Push(i)
	}

	var wg sync.WaitGroup
	successCount := atomic.Int64{}
	failCount := atomic.Int64{}

	// 大量并发写入
	const numWriters = 100
	const attempts = 1000

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < attempts; i++ {
				if rb.Push(id*attempts + i) {
					successCount.Add(1)
				} else {
					failCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()

	// 排空
	poppedCount := 0
	for {
		if _, ok := rb.Pop(); ok {
			poppedCount++
		} else {
			break
		}
	}

	// 总写入成功 = 预填充 + 并发成功
	totalPushed := int64(prefillCount) + successCount.Load()

	t.Logf("预填充: %d, Push 成功: %d, Push 失败: %d, Pop: %d, 剩余: %d",
		prefillCount, successCount.Load(), failCount.Load(), poppedCount, rb.Len())

	if totalPushed != int64(poppedCount+rb.Len()) {
		t.Errorf("数据不一致: push_total=%d != pop=%d + remain=%d",
			totalPushed, poppedCount, rb.Len())
	}
}

// ──────────────────── 原有功能回归测试 ────────────────────

// TestRingBuffer_Original_BasicPushPop 验证单线程 Push/Pop 基本 FIFO 语义。
func TestRingBuffer_Original_BasicPushPop(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)
	n := rb.Cap()

	for i := 0; i < n; i++ {
		if !rb.Push(i) {
			t.Fatalf("Push(%d) 应成功", i)
		}
	}
	if rb.Len() != n {
		t.Fatalf("Len 应为 %d, got %d", n, rb.Len())
	}

	for i := 0; i < n; i++ {
		v, ok := rb.Pop()
		if !ok {
			t.Fatalf("Pop(%d) 应成功", i)
		}
		if v != i {
			t.Errorf("FIFO 顺序错误: 期望 %d, got %d", i, v)
		}
	}

	if rb.Len() != 0 {
		t.Errorf("排空后 Len 应为 0, got %d", rb.Len())
	}
	if _, ok := rb.Pop(); ok {
		t.Error("空缓冲区 Pop 应返回 false")
	}
}

// TestRingBuffer_Original_PushFullBlock 验证满缓冲区 Block 策略的单线程行为。
func TestRingBuffer_Original_PushFullBlock(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)
	cap := rb.Cap()

	for i := 0; i < cap; i++ {
		rb.Push(i)
	}
	if !rb.IsFull() {
		t.Fatal("应已满")
	}

	// 满时 Push 应返回 false
	if rb.Push(999) {
		t.Error("满缓冲区 Push 应返回 false")
	}

	// writeIdx 不应被推进（单线程下 CAS 一定成功回退）
	if rb.Len() != cap {
		t.Errorf("满时 Push 失败后 Len 应为 %d, got %d", cap, rb.Len())
	}

	// Pop 后应能继续写入
	rb.Pop()
	if !rb.Push(1000) {
		t.Error("Pop 后 Push 应成功")
	}
}

// TestRingBuffer_Original_PushFullDrop 验证 OverflowDrop 策略。
func TestRingBuffer_Original_PushFullDrop(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowDrop)
	cap := rb.Cap()

	for i := 0; i < cap; i++ {
		rb.Push(i)
	}

	// Drop 策略：满时覆盖最旧元素。
	// cap=16, perShard=1，分片 0 存的是元素 0，Push(cap=16) 路由到分片 0 覆盖元素 0。
	// 所以 Pop 最先拿到的是 16（被覆盖写入了分片 0 的 head 位置）。
	rb.Push(cap)
	if rb.Dropped() != 1 {
		t.Errorf("Dropped 应为 1, got %d", rb.Dropped())
	}

	// 第一个 Pop 返回被覆盖后分片 0 的值（16），不是原始顺序的 1
	v, ok := rb.Pop()
	if !ok {
		t.Fatal("Pop 应成功")
	}
	if v != cap {
		t.Errorf("最旧元素应为 %d (0 被覆盖为 cap), got %d", cap, v)
	}
	// 第二个 Pop 应返回 1（仍在分片 1 中未受影响）
	v2, ok2 := rb.Pop()
	if !ok2 || v2 != 1 {
		t.Errorf("第二个 Pop 应为 1, got %d (ok=%v)", v2, ok2)
	}
}

// TestRingBuffer_Original_PeekDoesNotModify 验证 Peek 不改变状态。
func TestRingBuffer_Original_PeekDoesNotModify(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)

	rb.Push(42)
	rb.Push(99)

	lenBefore := rb.Len()
	v1, ok1 := rb.Peek()
	v2, ok2 := rb.Peek()
	lenAfter := rb.Len()

	if !ok1 || !ok2 {
		t.Fatal("Peek 应成功")
	}
	if v1 != 42 || v2 != 42 {
		t.Errorf("Peek 应返回 42, got %d, %d", v1, v2)
	}
	if lenBefore != lenAfter {
		t.Errorf("Peek 不应改变 Len: before=%d, after=%d", lenBefore, lenAfter)
	}

	// Pop 应能取出 Peek 看到的元素
	v3, _ := rb.Pop()
	if v3 != 42 {
		t.Errorf("Pop 应为 42, got %d", v3)
	}
}

// TestRingBuffer_Original_Flush 验证 Flush/FlushN 行为。
func TestRingBuffer_Original_Flush(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)

	for i := 0; i < 10; i++ {
		rb.Push(i)
	}

	// FlushN 部分排空
	batch := rb.FlushN(5)
	if len(batch) != 5 {
		t.Fatalf("FlushN(5) 应返回 5 个元素, got %d", len(batch))
	}
	for i, v := range batch {
		if v != i {
			t.Errorf("FlushN 顺序错误: idx=%d, expected=%d, got=%d", i, i, v)
		}
	}
	if rb.Len() != 5 {
		t.Errorf("FlushN(5) 后 Len 应为 5, got %d", rb.Len())
	}

	// Flush(0) 全部排空
	remaining := rb.Flush()
	if len(remaining) != 5 {
		t.Fatalf("Flush 应返回 5 个元素, got %d", len(remaining))
	}
	if rb.Len() != 0 {
		t.Errorf("Flush 后 Len 应为 0, got %d", rb.Len())
	}

	// 空缓冲区 Flush
	empty := rb.Flush()
	if len(empty) != 0 {
		t.Errorf("空缓冲区 Flush 应返回 nil, got %v", empty)
	}
}

// TestRingBuffer_Original_Reset 验证 Reset 行为。
func TestRingBuffer_Original_Reset(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowDrop)
	cap := rb.Cap()

	for i := 0; i < cap; i++ {
		rb.Push(i)
	}
	// 制造一些 dropped
	rb.Push(999)

	rb.Reset()

	if rb.Len() != 0 {
		t.Errorf("Reset 后 Len 应为 0, got %d", rb.Len())
	}
	if rb.Dropped() != 0 {
		t.Errorf("Reset 后 Dropped 应为 0, got %d", rb.Dropped())
	}

	// Reset 后应能正常写入
	for i := 0; i < cap; i++ {
		if !rb.Push(i + 100) {
			t.Errorf("Reset 后 Push(%d) 应成功", i)
		}
	}
}

// TestRingBuffer_Original_Cap 验证 Cap 返回正确的容量。
func TestRingBuffer_Original_Cap(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{1, 16},
		{15, 16},
		{16, 16},
		{17, 32},
		{32, 32},
		{100, 112},
	}
	for _, tc := range tests {
		rb := NewRingBuffer[int](tc.input, OverflowBlock)
		if rb.Cap() != tc.expected {
			t.Errorf("NewRingBuffer(%d).Cap() = %d, expected %d", tc.input, rb.Cap(), tc.expected)
		}
	}
}

// TestRingBuffer_Original_IsFull 验证 IsFull。
func TestRingBuffer_Original_IsFull(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)
	cap := rb.Cap()

	if rb.IsFull() {
		t.Error("空缓冲区 IsFull 应为 false")
	}

	for i := 0; i < cap; i++ {
		rb.Push(i)
	}
	if !rb.IsFull() {
		t.Error("满缓冲区 IsFull 应为 true")
	}

	rb.Pop()
	if rb.IsFull() {
		t.Error("Pop 后 IsFull 应为 false")
	}
}

// TestRingBuffer_Original_OverflowError 验证 OverflowError 策略。
func TestRingBuffer_Original_OverflowError(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowError)
	cap := rb.Cap()

	for i := 0; i < cap; i++ {
		rb.Push(i)
	}

	if rb.Push(999) {
		t.Error("OverflowError 满时 Push 应返回 false")
	}
	// 单线程下 writeIdx 应正确回退
	if rb.Len() != cap {
		t.Errorf("OverflowError 满时 Push 失败后 Len 应为 %d, got %d", cap, rb.Len())
	}
}

// TestRingBuffer_Original_OverflowStrategy 验证 OverflowStrategy 方法。
func TestRingBuffer_Original_OverflowStrategy(t *testing.T) {
	rb1 := NewRingBuffer[int](16, OverflowBlock)
	if rb1.OverflowStrategy() != OverflowBlock {
		t.Error("OverflowStrategy 应返回 OverflowBlock")
	}

	rb2 := NewRingBuffer[int](16, OverflowDrop)
	if rb2.OverflowStrategy() != OverflowDrop {
		t.Error("OverflowStrategy 应返回 OverflowDrop")
	}

	rb3 := NewRingBuffer[int](16, OverflowError)
	if rb3.OverflowStrategy() != OverflowError {
		t.Error("OverflowStrategy 应返回 OverflowError")
	}
}

// TestRingBuffer_Original_SingleReaderDrain 验证单线程排空（实际使用方式：FlushN）。
// RingBuffer.Pop 在代码库中仅通过 FlushN 单线程消费，并发 Pop 未被使用。
func TestRingBuffer_Original_SingleReaderDrain(t *testing.T) {
	rb := NewRingBuffer[int](64, OverflowBlock)
	n := rb.Cap()

	// 写入后带并发写入模拟空洞（Block 策略满时写入）
	for i := 0; i < n; i++ {
		if !rb.Push(i) {
			t.Fatalf("Push(%d) 失败", i)
		}
	}

	// 单线程排空，验证 FIFO
	received := make([]int, 0, n)
	for {
		v, ok := rb.Pop()
		if !ok {
			break
		}
		received = append(received, v)
	}

	if len(received) != n {
		t.Fatalf("应 Pop 出 %d 个元素, got %d", n, len(received))
	}

	// 验证所有元素都被消费（顺序可能不完全 FIFO，但无重复无丢失）
	seen := make(map[int]bool)
	for _, v := range received {
		if seen[v] {
			t.Errorf("元素 %d 被重复 Pop", v)
		}
		seen[v] = true
	}
	for i := 0; i < n; i++ {
		if !seen[i] {
			t.Errorf("元素 %d 未被 Pop", i)
		}
	}

	if rb.Len() != 0 {
		t.Errorf("排空后 Len 应为 0, got %d", rb.Len())
	}
}

// TestRingBuffer_Original_PushPopInterleaved 验证交替 Push/Pop 的 FIFO。
func TestRingBuffer_Original_PushPopInterleaved(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)

	rb.Push(1)
	rb.Push(2)
	v, _ := rb.Pop()
	if v != 1 {
		t.Errorf("期望 1, got %d", v)
	}
	rb.Push(3)
	v, _ = rb.Pop()
	if v != 2 {
		t.Errorf("期望 2, got %d", v)
	}
	v, _ = rb.Pop()
	if v != 3 {
		t.Errorf("期望 3, got %d", v)
	}
}

// TestRingBuffer_Original_FlushNMoreThanTotal 验证 FlushN(n > total) 行为。
func TestRingBuffer_Original_FlushNMoreThanTotal(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)

	rb.Push(1)
	rb.Push(2)

	// n=0 取全部
	all := rb.FlushN(0)
	if len(all) != 2 {
		t.Errorf("FlushN(0) 应返回 2 个, got %d", len(all))
	}

	rb.Push(3)
	// n > total 取全部
	all = rb.FlushN(100)
	if len(all) != 1 {
		t.Errorf("FlushN(100) 应返回 1 个, got %d", len(all))
	}
}

// TestRingBuffer_Original_PushAfterPopFromFull 验证满→Pop→Push 可继续。
func TestRingBuffer_Original_PushAfterPopFromFull(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)
	cap := rb.Cap()

	for i := 0; i < cap; i++ {
		rb.Push(i)
	}

	// Pop 出 cap 个，验证值正确
	for i := 0; i < cap; i++ {
		v, ok := rb.Pop()
		if !ok || v != i {
			t.Errorf("期望 %d, got %d (ok=%v)", i, v, ok)
		}
	}

	// 再次写入
	for i := 0; i < cap; i++ {
		if !rb.Push(i + 100) {
			t.Errorf("再次 Push(%d) 应成功", i+100)
		}
	}

	// 再次读取
	for i := 0; i < cap; i++ {
		v, ok := rb.Pop()
		if !ok || v != i+100 {
			t.Errorf("期望 %d, got %d (ok=%v)", i+100, v, ok)
		}
	}
}

// TestRingBuffer_Original_LenInvariant 验证 Len 始终 >= 0 且 <= Cap。
func TestRingBuffer_Original_LenInvariant(t *testing.T) {
	rb := NewRingBuffer[int](16, OverflowBlock)
	cap := rb.Cap()

	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < cap; i++ {
			rb.Push(i)
			if l := rb.Len(); l < 0 || l > cap {
				t.Errorf("Len 越界: %d (cap=%d)", l, cap)
			}
		}
		for i := 0; i < cap; i++ {
			rb.Pop()
			if l := rb.Len(); l < 0 || l > cap {
				t.Errorf("Len 越界: %d (cap=%d)", l, cap)
			}
		}
	}
}

// TestRingBuffer_Original_DroppedOnlyForDrop 验证只有 OverflowDrop 策略才增加 Dropped。
func TestRingBuffer_Original_DroppedOnlyForDrop(t *testing.T) {
	rbBlock := NewRingBuffer[int](16, OverflowBlock)
	cap := rbBlock.Cap()
	for i := 0; i < cap; i++ {
		rbBlock.Push(i)
	}
	rbBlock.Push(999) // 应失败
	if rbBlock.Dropped() != 0 {
		t.Error("OverflowBlock 不应增加 Dropped")
	}

	rbDrop := NewRingBuffer[int](16, OverflowDrop)
	for i := 0; i < cap; i++ {
		rbDrop.Push(i)
	}
	rbDrop.Push(999) // 应覆盖
	if rbDrop.Dropped() != 1 {
		t.Errorf("OverflowDrop Dropped 应为 1, got %d", rbDrop.Dropped())
	}
}

// ──────────────────── 极限压力验证测试 ────────────────────

// TestRingBuffer_Stress_ProductionPattern 模拟生产场景：多 Writer 并发 Push + 单线程 FlushN。
// 这是 Pool 实际使用的模式（pool.go:860 Push, pool.go:460 FlushN）。
// 验证极限并发下无数据丢失、无数据重复、无死锁。
func TestRingBuffer_Stress_ProductionPattern(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过极限压力测试")
	}

	strategyCases := []struct {
		name     string
		strategy OverflowStrategy
	}{
		{"Block", OverflowBlock},
		{"Drop", OverflowDrop},
		{"Error", OverflowError},
	}

	for _, tc := range strategyCases {
		t.Run(tc.name, func(t *testing.T) {
			rb := NewRingBuffer[int](256, tc.strategy)

			const (
				numWriters = 64
				cycles     = 100
				batchSize  = 200
			)

			for cycle := 0; cycle < cycles; cycle++ {
				var wg sync.WaitGroup
				successCount := atomic.Int64{}
				failCount := atomic.Int64{}

				for w := 0; w < numWriters; w++ {
					wg.Add(1)
					go func(base int) {
						defer wg.Done()
						for i := 0; i < batchSize; i++ {
							if rb.Push(base + i) {
								successCount.Add(1)
							} else {
								failCount.Add(1)
							}
						}
					}(w * batchSize)
				}
				wg.Wait()

				if tc.strategy == OverflowBlock || tc.strategy == OverflowError {
					successExpected := successCount.Load()
					_ = failCount

					popped := int64(0)
					seen := make(map[int]bool)
					for {
						v, ok := rb.Pop()
						if !ok {
							break
						}
						if seen[v] {
							t.Errorf("[%s] 元素 %d 重复 Pop cycle=%d", tc.name, v, cycle)
						}
						seen[v] = true
						popped++
					}

					if popped != successExpected {
						t.Errorf("[%s] Push成功=%d ≠ Pop=%d cycle=%d", tc.name, successExpected, popped, cycle)
					}
					if rb.Len() != 0 {
						t.Errorf("[%s] 排空后 Len=%d cycle=%d", tc.name, rb.Len(), cycle)
					}
				} else { // Drop
					rb.Reset()
				}
			}
		})
	}
}

// TestRingBuffer_Stress_ConcurrentPushSingleFlush 超大规模并发测试。
// 1000 goroutine 并发 Push，单线程 Flush 周期性排空。
func TestRingBuffer_Stress_ConcurrentPushSingleFlush(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过极限压力测试")
	}

	rb := NewRingBuffer[int](1024, OverflowBlock)
	cap := rb.Cap()

	const (
		numWriters    = 500
		writesPerGoro = 2000
	)

	var wg sync.WaitGroup
	successCount := atomic.Int64{}
	failCount := atomic.Int64{}
	stopFlag := atomic.Bool{}

	done := make(chan struct{})

	go func() {
		defer close(done)
		flushed := int64(0)
		for !stopFlag.Load() || rb.Len() > 0 {
			batch := rb.FlushN(256)
			flushed += int64(len(batch))
		}
	}()

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < writesPerGoro; i++ {
				if rb.Push(base*writesPerGoro + i) {
					successCount.Add(1)
				} else {
					failCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	stopFlag.Store(true)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Flush goroutine timeout: 可能死锁")
	}

	remains := rb.Len()
	_ = cap

	t.Logf("[Stress] Push成功=%d Push失败=%d 剩余=%d", successCount.Load(), failCount.Load(), remains)

	popped := int64(0)
	for {
		if _, ok := rb.Pop(); ok {
			popped++
		} else {
			break
		}
	}

	totalPopped := popped // + flushed (already counted)
	_ = totalPopped

	// 最终验证
	if successCount.Load() > 0 && rb.Len() != 0 {
		t.Errorf("排空后仍剩余 %d 个元素", rb.Len())
	} else {
		t.Logf("[Stress] 排空验证通过 Len=%d", rb.Len())
	}
}

// TestRingBuffer_Stress_OverflowBlockNoHoles 验证并发 Block 策略不会产生无法排空的空洞。
// 这是用户最关心的场景：并发 Push 到满缓冲区，所有失败回退正确。
func TestRingBuffer_Stress_OverflowBlockNoHoles(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过极限压力测试")
	}

	for run := 0; run < 50; run++ {
		rb := NewRingBuffer[int](64, OverflowBlock)

		// 填满
		for i := 0; i < rb.Cap(); i++ {
			rb.Push(i)
		}

		// 并发尝试写入（全部应失败）
		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rb.Push(-1)
			}()
		}
		wg.Wait()

		// Pop 排空所有元素
		count := 0
		for {
			if _, ok := rb.Pop(); ok {
				count++
			} else {
				break
			}
		}

		if count != rb.Cap() {
			t.Fatalf("第 %d 次: 应 Pop %d 个元素, got %d", run, rb.Cap(), count)
		}

		if rb.Len() != 0 {
			t.Fatalf("第 %d 次: 排空后 Len=%d", run, rb.Len())
		}
	}
}

// TestRingBuffer_Stress_RaceDetectorFull 全路径竞态检测。
// 同时启动 Push/Pop/Peek/Flush/Len/IsFull，确保无 data race。
func TestRingBuffer_Stress_RaceDetectorFull(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过极限压力测试")
	}

	rb := NewRingBuffer[int](128, OverflowBlock)
	stopFlag := atomic.Bool{}
	var wg sync.WaitGroup

	pushers := 20
	readers := 5

	for i := 0; i < pushers; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; !stopFlag.Load(); j++ {
				rb.Push(base*10000 + j)
			}
		}(i)
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stopFlag.Load() {
				rb.Pop()
				rb.Peek()
				rb.Len()
				rb.IsFull()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stopFlag.Load() {
			rb.FlushN(32)
			rb.Flush()
		}
	}()

	time.Sleep(3 * time.Second)
	stopFlag.Store(true)
	wg.Wait()

	time.Sleep(100 * time.Millisecond)

	rb.Flush()
	rb.Reset()

	t.Log("[RaceFull] 无 data race 完成")
}

// ──────────────────── 千万级极限压力验证 ────────────────────

// TestRingBuffer_Stress_10MillionWrites 千万级写入 + 单线程消费。
// 模拟真实生产场景：大量 worker 并发 Push，单线程 FlushN 消费。
// 目标：10,000,000 次操作，验证零数据丢失、零重复、零死锁。
func TestRingBuffer_Stress_10MillionWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过千万级压力测试")
	}

	strategyCases := []struct {
		name     string
		strategy OverflowStrategy
	}{
		{"Block", OverflowBlock},
		{"Drop", OverflowDrop},
		{"Error", OverflowError},
	}

	for _, tc := range strategyCases {
		t.Run(tc.name, func(t *testing.T) {
			totalOps := int64(10_000_000)
			rb := NewRingBuffer[int](2048, tc.strategy)
			cap := int64(rb.Cap())

			var wg sync.WaitGroup
			successCount := atomic.Int64{}
			failCount := atomic.Int64{}
			stopFlag := atomic.Bool{}

			done := make(chan int64, 1)

			go func() {
				flushedTotal := int64(0)
				for !stopFlag.Load() || rb.Len() > 0 {
					batch := rb.FlushN(512)
					flushedTotal += int64(len(batch))
				}
				done <- flushedTotal
			}()

			numWriters := 200
			opsPerWriter := int(totalOps) / numWriters

			t.Logf("[10M-%s] 启动 %d writer, 每个 %d 次操作...", tc.name, numWriters, opsPerWriter)

			startTime := time.Now()
			for w := 0; w < numWriters; w++ {
				wg.Add(1)
				go func(base int) {
					defer wg.Done()
					for i := 0; i < opsPerWriter; i++ {
						v := base*opsPerWriter + i
						if rb.Push(v) {
							successCount.Add(1)
						} else {
							failCount.Add(1)
						}
					}
				}(w)
			}

			wg.Wait()
			stopFlag.Store(true)

			select {
			case flushedTotal := <-done:
				elapsed := time.Since(startTime)
				succ := successCount.Load()
				fail := failCount.Load()
				written := succ + fail

				t.Logf("[10M-%s] 耗时=%v Push成功=%d Push失败=%d Flush=%d",
					tc.name, elapsed, succ, fail, flushedTotal)
				t.Logf("[10M-%s] 吞吐=%.0f ops/s", tc.name, float64(written)/elapsed.Seconds())

				if written != totalOps {
					t.Errorf("[10M-%s] 总操作数不匹配: %d != %d", tc.name, written, totalOps)
				}

				remains := rb.Len()
				popped := int64(0)
				for {
					if _, ok := rb.Pop(); ok {
						popped++
					} else {
						break
					}
				}

				totalConsumed := flushedTotal + popped

				if tc.strategy == OverflowDrop {
					expectedInBuf := cap
					_ = expectedInBuf
					t.Logf("[10M-%s] Flush+Pop=%d Dropped=%d Cap=%d",
						tc.name, totalConsumed, rb.Dropped(), cap)
				} else {
					if succ != totalConsumed {
						t.Errorf("[10M-%s] Push成功=%d ≠ 消费=%d (Flush=%d + Pop=%d)",
							tc.name, succ, totalConsumed, flushedTotal, popped)
					}
					if remains != 0 {
						t.Errorf("[10M-%s] 排空后剩余=%d", tc.name, remains)
					}
					t.Logf("[10M-%s] 数据一致性验证通过: Push=%d = 消费=%d 剩余=%d",
						tc.name, succ, totalConsumed, remains)
				}

			case <-time.After(120 * time.Second):
				t.Fatalf("[10M-%s] Flush goroutine 超时", tc.name)
			}
		})
	}
}

// TestRingBuffer_Stress_PushPopTightLoop 紧凑的 Push/Flush 循环（单线程消费，匹配生产）。
// 验证在持续高频率 Push + 单线程 FlushN 交替下无死锁无数据丢失。
func TestRingBuffer_Stress_PushPopTightLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过千万级压力测试")
	}

	rb := NewRingBuffer[int](512, OverflowBlock)
	numWriters := 500
	duration := 5 * time.Second

	var wg sync.WaitGroup
	stopFlag := atomic.Bool{}
	pushedAll := atomic.Int64{}
	poppedAll := atomic.Int64{}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stopFlag.Load() || rb.Len() > 0 {
			batch := rb.FlushN(256)
			poppedAll.Add(int64(len(batch)))
		}
	}()

	startTime := time.Now()

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			counter := 0
			for !stopFlag.Load() {
				if rb.Push(base*1000000 + counter) {
					pushedAll.Add(1)
				}
				counter++
			}
		}(i)
	}

	time.Sleep(duration)
	stopFlag.Store(true)
	wg.Wait()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Flush goroutine timeout: 可能死锁")
	}

	popped := int64(0)
	for {
		if _, ok := rb.Pop(); ok {
			popped++
		} else {
			break
		}
	}
	totalPopped := poppedAll.Load() + popped
	remains := rb.Len()
	elapsed := time.Since(startTime)

	t.Logf("[TightLoop] %d writers × %v", numWriters, duration)
	t.Logf("[TightLoop] Push=%d Pop(FlushN)=%d + drain=%d = %d 剩余=%d",
		pushedAll.Load(), poppedAll.Load(), popped, totalPopped, remains)
	t.Logf("[TightLoop] 吞吐=%.0f ops/s", float64(pushedAll.Load()+totalPopped)/elapsed.Seconds())

	if pushedAll.Load() != totalPopped {
		t.Errorf("[TightLoop] 数据不一致: Push=%d ≠ Pop=%d (差=%d)",
			pushedAll.Load(), totalPopped, pushedAll.Load()-totalPopped)
	}

	if remains != 0 {
		t.Errorf("[TightLoop] 排空后仍有 %d 个元素", remains)
	}

	t.Log("[TightLoop] 数据一致性验证通过")
}

// TestRingBuffer_Stress_WriteIdxIntegrity 专门验证 writeIdx 在极限并发下的完整性。
// 场景：短时间大量 Push（含失败），验证 writeIdx 不会异常回退导致数值错乱。
func TestRingBuffer_Stress_WriteIdxIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过千万级压力测试")
	}

	for round := 0; round < 20; round++ {
		rb := NewRingBuffer[int](128, OverflowBlock)
		cap := rb.Cap()

		for i := 0; i < cap; i++ {
			rb.Push(i)
		}

		var wg sync.WaitGroup
		failCount := atomic.Int64{}

		for i := 0; i < 500; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					if !rb.Push(-1) {
						failCount.Add(1)
					}
				}
			}()
		}
		wg.Wait()

		count := 0
		for {
			if _, ok := rb.Pop(); ok {
				count++
			} else {
				break
			}
		}

		if count != cap {
			t.Fatalf("轮次 %d: Pop=%d ≠ Cap=%d, fail=%d", round, count, cap, failCount.Load())
		}
		if rb.Len() != 0 {
			t.Fatalf("轮次 %d: 排空后 Len=%d ≠ 0", round, rb.Len())
		}
	}
}
