package ratelimit

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Exercise the real refill goroutine and Resize together with an integer model.
// Each virtual tick is synchronized before observing state. At most 32 serial
// operations are used; no arbitrary concurrent configuration is modeled.
func FuzzRateLimiterTimedHistory(f *testing.F) {
	f.Add([]byte{0, 0, 0, 2, 0, 3, 0, 1, 2}, uint8(0), uint8(2))
	f.Add([]byte{0, 0, 3, 7, 2, 2, 1, 0, 2}, uint8(6), uint8(0))
	f.Add([]byte{2, 2, 0, 3, 0, 0, 2}, uint8(1), uint8(7))
	f.Add([]byte{0, 0, 0, 2, 7, 3, 2}, uint8(0), uint8(3))
	f.Fuzz(func(t *testing.T, operations []byte, rateByte, capacityByte uint8) {
		if len(operations) > 32 {
			operations = operations[:32]
		}
		synctest.Test(t, func(t *testing.T) {
			rate := 1 + int(rateByte%8)
			capacity := max(rate, 1+int(capacityByte%8))
			available, credit := capacity, 0
			rl := NewRateLimiterWithBurst(rate, 200*time.Millisecond, capacity).WithStrategy(Reject)
			defer rl.Close()
			synctest.Wait()
			for step, op := range operations {
				switch op % 4 {
				case 0:
					err := rl.Acquire(context.Background())
					if available > 0 {
						if err != nil {
							t.Fatalf("step %d acquire=%v with room=%d", step, err, available)
						}
						available--
					} else if !errors.Is(err, core.ErrRateLimitExceeded) {
						t.Fatalf("step %d acquire=%v without room", step, err)
					}
				case 1:
					rl.Release()
					available = min(capacity, available+1)
				case 2:
					time.Sleep(100 * time.Millisecond)
					synctest.Wait()
					credit += rate
					added := min(capacity-available, credit/2)
					available += added
					credit %= 2
					if available == capacity {
						credit = 0
					}
				case 3:
					next := 1 + int(op/4%8)
					rl.Resize(next)
					if next != rate {
						used := capacity - available
						rate, capacity = next, next
						available = max(0, capacity-used)
					}
				}
				if got := rl.Available(); got != available || rl.Size() != rate {
					t.Fatalf("step %d op=%d available=%d/%d size=%d/%d credit=%d", step, op, got, available, rl.Size(), rate, credit)
				}
			}
		})
	})
}
