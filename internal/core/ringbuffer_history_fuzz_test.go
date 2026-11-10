package core_test

import (
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

type ringHistoryOperation struct {
	kind, arg  int
	start, end int64
	value      int
	ok         bool
	values     []int
	count      int64
}

// Search legal FIFO histories rather than predicting lock acquisition order.
// Six overlapping operations keep exhaustive search bounded (at most 6!).
func FuzzRingBufferConcurrentHistory(f *testing.F) {
	f.Add(uint8(1), uint8(0), []byte{0, 1, 2, 3, 4, 5})
	f.Add(uint8(2), uint8(1), []byte{0, 0, 1, 3, 6, 7})
	f.Add(uint8(3), uint8(2), []byte{5, 0, 8, 1, 2, 4})
	f.Add(uint8(2), uint8(0), []byte{2, 2, 2, 2, 2, 2})
	f.Add(uint8(0), uint8(1), []byte{0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, capacityByte, strategyByte uint8, input []byte) {
		capacity := int(capacityByte%4) + 1
		strategy := core.OverflowStrategy(strategyByte % 3)
		buffer := core.NewRingBuffer[int](capacity, strategy)
		initial := make([]int, capacity)
		for i := range initial {
			initial[i] = -i - 1
			buffer.Push(initial[i])
		}
		history := make([]ringHistoryOperation, 6)
		for i := range history {
			var b byte
			if len(input) > 0 {
				b = input[i%len(input)]
			}
			history[i].kind = int(b % 9)
			history[i].arg = i + 1
			if history[i].kind == 3 {
				history[i].arg = int(b/9) - 1
			}
		}
		var clock atomic.Int64
		start := make(chan struct{})
		var workers sync.WaitGroup
		for worker := 0; worker < 3; worker++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for i := worker * 2; i < worker*2+2; i++ {
					op := &history[i]
					op.start = clock.Add(1)
					runtime.Gosched()
					switch op.kind {
					case 0:
						op.ok = buffer.Push(op.arg)
					case 1:
						op.value, op.ok = buffer.Pop()
					case 2:
						op.value, op.ok = buffer.Peek()
					case 3:
						op.values = buffer.FlushN(op.arg)
					case 4:
						op.values = buffer.Flush()
					case 5:
						buffer.Reset()
					case 6:
						op.value = buffer.Len()
					case 7:
						op.ok = buffer.IsFull()
					case 8:
						op.count = buffer.Dropped()
					}
					op.end = clock.Add(1)
				}
			}()
		}
		close(start)
		workers.Wait()
		final := buffer.Flush()
		finalDropped := buffer.Dropped()
		predecessors := make([]uint8, len(history))
		for i, op := range history {
			for j, other := range history {
				if other.end < op.start {
					predecessors[i] |= 1 << j
				}
			}
		}
		var search func(uint8, []int, int64) bool
		search = func(done uint8, queue []int, dropped int64) bool {
			if done == 1<<len(history)-1 {
				return slices.Equal(queue, final) && dropped == finalDropped
			}
			for i, op := range history {
				if done&(1<<i) != 0 || predecessors[i]&done != predecessors[i] {
					continue
				}
				next := slices.Clone(queue)
				nextDropped := dropped
				match := true
				switch op.kind {
				case 0:
					accepted := len(next) < capacity || strategy == core.OverflowDrop
					match = op.ok == accepted
					if accepted {
						if len(next) == capacity {
							next = next[1:]
							nextDropped++
						}
						next = append(next, op.arg)
					}
				case 1, 2:
					value := 0
					found := len(next) > 0
					if found {
						value = next[0]
					}
					match = op.ok == found && op.value == value
					if found && op.kind == 1 {
						next = next[1:]
					}
				case 3, 4:
					n := len(next)
					if op.kind == 3 && op.arg > 0 && op.arg < n {
						n = op.arg
					}
					match = slices.Equal(op.values, next[:n])
					next = next[n:]
				case 5:
					next = nil
					nextDropped = 0
				case 6:
					match = op.value == len(next)
				case 7:
					match = op.ok == (len(next) == capacity)
				case 8:
					match = op.count == dropped
				}
				if match && search(done|(1<<i), next, nextDropped) {
					return true
				}
			}
			return false
		}
		if !search(0, initial, 0) {
			t.Fatalf("no legal FIFO history: capacity=%d strategy=%d history=%+v final=%v dropped=%d", capacity, strategy, history, final, finalDropped)
		}
	})
}
