package core_test

import (
	"slices"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// A FIFO slice model checks mixed operations, wraparound, rejection and reset.
// The model never uses the buffer's head/tail indexes or storage layout.
func FuzzRingBufferStateTransitions(f *testing.F) {
	f.Add(uint8(1), uint8(0), []byte{0, 8, 16, 3, 0, 1, 2, 4, 5, 6})
	f.Add(uint8(3), uint8(1), []byte{0, 8, 16, 24, 3, 0, 8, 2, 4, 5})
	f.Add(uint8(5), uint8(2), []byte{0, 8, 16, 24, 32, 40, 1, 6, 0, 5})
	f.Fuzz(func(t *testing.T, capacityByte, strategyByte uint8, operations []byte) {
		capacity := int(capacityByte%32) + 1
		strategy := core.OverflowStrategy(strategyByte % 3)
		buffer := core.NewRingBuffer[int](capacity, strategy)
		queue := make([]int, 0, capacity)
		var dropped int64
		if len(operations) > 2048 {
			operations = operations[:2048]
		}
		for step, operation := range operations {
			switch operation % 8 {
			case 0, 7:
				value := int(operation)
				accepted := len(queue) < capacity || strategy == core.OverflowDrop
				if got := buffer.Push(value); got != accepted {
					t.Fatalf("step %d: Push=%v want %v", step, got, accepted)
				}
				if accepted {
					if len(queue) == capacity {
						queue = queue[1:]
						dropped++
					}
					queue = append(queue, value)
				}
			case 1, 2:
				var value int
				found := len(queue) > 0
				if found {
					value = queue[0]
				}
				var got int
				var ok bool
				if operation%8 == 1 {
					got, ok = buffer.Pop()
					if found {
						queue = queue[1:]
					}
				} else {
					got, ok = buffer.Peek()
				}
				if got != value || ok != found {
					t.Fatalf("step %d: read=(%d,%v) want (%d,%v)", step, got, ok, value, found)
				}
			case 3, 4, 6:
				n := int(operation/8) - 2
				if operation%8 == 4 {
					n = 0
				}
				count := n
				if count <= 0 || count > len(queue) {
					count = len(queue)
				}
				var got []int
				if operation%8 == 4 {
					got = buffer.Flush()
				} else {
					got = buffer.FlushN(n)
				}
				if !slices.Equal(got, queue[:count]) {
					t.Fatalf("step %d: flush=%v want %v", step, got, queue[:count])
				}
				queue = queue[count:]
			case 5:
				buffer.Reset()
				queue = queue[:0]
				dropped = 0
			}
			if buffer.Len() != len(queue) || buffer.IsFull() != (len(queue) == capacity) ||
				buffer.Dropped() != dropped || buffer.Disposed() != int(dropped) ||
				buffer.Cap() != capacity || buffer.Overflow() != strategy {
				t.Fatalf("step %d: metadata differs from FIFO model", step)
			}
		}
		if got := buffer.Flush(); !slices.Equal(got, queue) {
			t.Fatalf("final flush=%v want %v", got, queue)
		}
	})
}
