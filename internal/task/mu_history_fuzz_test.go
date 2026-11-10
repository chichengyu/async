package task

import (
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

type muHistoryOperation struct {
	kind, value, calls int
	start, end         int64
	values             []int
	panicked           bool
}

// Check all serial histories consistent with the observed call/return order.
// Six operations bound the search at 6!, without assuming a scheduler order.
func FuzzMuConcurrentHistory(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 0, 1})
	f.Add([]byte{0, 0, 0, 0, 0, 0})
	f.Add([]byte{2, 1, 3, 0, 1, 2})
	f.Add([]byte{1, 1, 1, 1, 1, 1})
	f.Fuzz(func(t *testing.T, input []byte) {
		var m Mu[int]
		m.Append(func() int { return -1 })
		history := make([]muHistoryOperation, 6)
		for i := range history {
			var b byte
			if len(input) > 0 {
				b = input[i%len(input)]
			}
			history[i].kind, history[i].value = int(b%4), i+1
		}
		var clock atomic.Int64
		start := make(chan struct{})
		var workers sync.WaitGroup
		for worker := range 3 {
			workers.Go(func() {
				<-start
				for i := worker * 2; i < worker*2+2; i++ {
					op := &history[i]
					op.start = clock.Add(1)
					runtime.Gosched()
					func() {
						defer func() {
							if recovered := recover(); recovered != nil {
								op.panicked = true
								if op.kind == 2 && recovered != op.value {
									t.Errorf("changed panic: %v want %d", recovered, op.value)
								}
							}
						}()
						switch op.kind {
						case 0, 2:
							m.Append(func() int {
								op.calls++
								runtime.Gosched()
								if op.kind == 2 {
									panic(op.value)
								}
								return op.value
							})
						case 1:
							op.values = m.Snapshot()
						case 3:
							m.Append(nil)
						}
					}()
					op.end = clock.Add(1)
				}
			})
		}
		close(start)
		workers.Wait()
		final := slices.Clone(m.Snapshot())
		predecessors := make([]uint8, len(history))
		for i, op := range history {
			wantCalls := 0
			if op.kind == 0 || op.kind == 2 {
				wantCalls = 1
			}
			if op.calls != wantCalls || op.panicked != (op.kind >= 2) {
				t.Fatalf("factory outcome %+v want calls=%d", op, wantCalls)
			}
			for j, other := range history {
				if other.end < op.start {
					predecessors[i] |= 1 << j
				}
			}
		}
		var search func(uint8, []int) bool
		search = func(done uint8, values []int) bool {
			if done == 1<<len(history)-1 {
				return slices.Equal(values, final)
			}
			for i, op := range history {
				if done&(1<<i) != 0 || predecessors[i]&done != predecessors[i] {
					continue
				}
				next := slices.Clone(values)
				if op.kind == 0 {
					next = append(next, op.value)
				}
				if op.kind == 1 && !slices.Equal(op.values, values) {
					continue
				}
				if search(done|(1<<i), next) {
					return true
				}
			}
			return false
		}
		if !search(0, []int{-1}) {
			t.Fatalf("no legal Mu history: %+v final=%v", history, final)
		}
		// Modify each saved snapshot only after concurrent operations finish.
		for _, op := range history {
			if op.kind == 1 && len(op.values) > 0 {
				op.values[0] = 999
			}
		}
		if got := m.Snapshot(); !slices.Equal(got, final) {
			t.Fatalf("snapshot aliases storage: got %v want %v", got, final)
		}
	})
}

func TestMuFactoryReentryAndPanic(t *testing.T) {
	var m Mu[int]
	m.Append(func() int {
		m.Append(func() int { return 1 })
		return len(m.Snapshot()) + 1
	})
	func() {
		defer func() {
			if recovered := recover(); recovered != "outer" {
				t.Errorf("panic=%v want outer", recovered)
			}
		}()
		m.Append(func() int {
			m.Append(func() int { return 3 })
			panic("outer")
		})
	}()
	if got := m.Snapshot(); !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("reentrant effects got %v", got)
	}
}
