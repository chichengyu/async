package core

import (
	"fmt"
	"slices"
	"testing"
)

type ringReferenceValue struct{ payload []int }

// Check storage references directly instead of relying on nondeterministic GC
// or finalizers. The queue oracle does not use the ring's head/tail positions.
func TestRingBufferDrainsReleaseSlotsAndPreserveReturnedReferences(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, capacity := range []int{1, 3, 5} {
		for _, strategy := range []OverflowStrategy{OverflowBlock, OverflowDrop, OverflowError, OverflowStrategy(99)} {
			for _, shape := range []string{"empty", "partial", "full", "wrapped", "overflow"} {
				for _, count := range []int{-maxInt - 1, -1, 0, 1, capacity - 1, capacity, capacity + 1, maxInt} {
					t.Run(fmt.Sprintf("cap=%d/strategy=%d/%s/n=%d", capacity, strategy, shape, count), func(t *testing.T) {
						ring := NewRingBuffer[*ringReferenceValue](capacity, strategy)
						storage := &ring.buf[0]
						var queue []*ringReferenceValue
						var dropped int64
						next := 0
						check := func() {
							t.Helper()
							if ring.Len() != len(queue) || ring.IsFull() != (len(queue) == capacity) || ring.Dropped() != dropped || ring.Disposed() != int(dropped) || ring.Cap() != capacity || ring.Overflow() != strategy {
								t.Fatal("ring metadata diverged from reference queue")
							}
							for _, slot := range ring.buf {
								if slot != nil && !slices.Contains(queue, slot) {
									t.Fatal("removed value retained in storage")
								}
							}
							for _, live := range queue {
								matches := 0
								for _, slot := range ring.buf {
									if slot == live {
										matches++
									}
								}
								if matches != 1 {
									t.Fatalf("live reference stored %d times", matches)
								}
							}
							front, ok := ring.Peek()
							if len(queue) == 0 {
								if ok || front != nil {
									t.Fatal("empty Peek retained a value")
								}
							} else if !ok || front != queue[0] {
								t.Fatal("Peek lost FIFO reference")
							}
							if &ring.buf[0] != storage {
								t.Fatal("drain unexpectedly replaced fixed storage")
							}
						}
						push := func() {
							next++
							value := &ringReferenceValue{payload: []int{next}}
							accepted := len(queue) < capacity || strategy == OverflowDrop
							if ring.Push(value) != accepted {
								t.Fatal("Push acceptance differs from queue")
							}
							if accepted {
								if len(queue) == capacity {
									queue = queue[1:]
									dropped++
								}
								queue = append(queue, value)
							}
							check()
						}
						pop := func() {
							got, ok := ring.Pop()
							if len(queue) == 0 {
								if ok || got != nil {
									t.Fatal("empty Pop retained a value")
								}
							} else {
								if !ok || got != queue[0] {
									t.Fatal("Pop lost FIFO reference")
								}
								queue = queue[1:]
							}
							check()
						}
						check()
						switch shape {
						case "partial":
							for range max(1, capacity-1) {
								push()
							}
						case "full":
							for range capacity {
								push()
							}
						case "wrapped":
							for range capacity {
								push()
							}
							pop()
							push()
						case "overflow":
							for range capacity + 2 {
								push()
							}
						}
						wantCount := len(queue)
						if count > 0 && count < wantCount {
							wantCount = count
						}
						want := slices.Clone(queue[:wantCount])
						result := ring.FlushN(count)
						queue = queue[wantCount:]
						if result == nil || !slices.Equal(result, want) {
							t.Fatal("FlushN did not return allocated FIFO result")
						}
						check()
						if len(result) > 0 {
							result[0].payload[0] = -99
							if want[0].payload[0] != -99 {
								t.Fatal("returned element was deep copied")
							}
							result[0] = &ringReferenceValue{payload: []int{-77}}
							check()
						}
						remaining := ring.Flush()
						if remaining == nil || !slices.Equal(remaining, queue) {
							t.Fatal("Flush lost FIFO references")
						}
						queue = nil
						check()
						for range capacity + 1 {
							push()
						}
						ring.Reset()
						queue = nil
						dropped = 0
						check()
						ring.Reset()
						check()
						if len(result) > 0 && result[0].payload[0] != -77 {
							t.Fatal("Reset changed returned result slots")
						}
						push()
						pop()
						pop()
					})
				}
			}
		}
	}
}
