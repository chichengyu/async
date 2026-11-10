package ratelimit

import (
	"math"
	"testing"
	"time"
)

// Compare round-robin admission against independent per-shard budgets. Aging
// an oldest prefix of a shard's records exercises expiry without a clock sleep.
// All calls are sequential; concurrent admission is covered by existing tests.
func FuzzShardedWindowRouting(f *testing.F) {
	f.Add([]byte{3, 9, 1, 2, 3, 0, 255, 128, 7, 8})
	f.Add([]byte{7, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{0, 255, 255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		if len(data) > 258 {
			data = data[:258]
		}
		shards, total := 1+int(data[0]%8), int(data[1]%68)-2
		limiter := NewShardedSlidingWindowRateLimiter(shards, total, time.Hour)
		limits, used := make([]int, shards), make([]int, shards)
		wantTotal := 0
		for i := range limits {
			limits[i] = 1
			if total >= shards {
				limits[i] = total / shards
				if i < total%shards {
					limits[i]++
				}
			}
			wantTotal += limits[i]
		}
		if limiter.TotalLimit() != wantTotal || limiter.ShardCount() != shards {
			t.Fatalf("total=%d shards=%d, want %d/%d", limiter.TotalLimit(), limiter.ShardCount(), wantTotal, shards)
		}
		next := 0
		for step, op := range data[2:] {
			if op&128 != 0 {
				idx := int(op) % shards
				shard := limiter.GetShard(idx)
				shard.mu.Lock()
				// Rebase live records far from either boundary, then expire a prefix.
				// Only test state changes: no production clock hook is introduced.
				now := time.Now()
				expired := int(op) % (len(shard.timestamps) + 1)
				for i := range shard.timestamps {
					shard.timestamps[i] = now.Add(-30 * time.Minute)
					if i < expired {
						shard.timestamps[i] = now.Add(-2 * time.Hour)
					}
				}
				shard.mu.Unlock()
				used[idx] -= expired
				// Cleanup through a rejected oversized call, without changing routing.
				if shard.AllowN(math.MaxInt) {
					t.Fatal("oversized direct request accepted")
				}
			}
			n := int(op%24) - 2
			if op%31 == 0 {
				n = math.MaxInt
			}
			allowOne := op%7 == 0
			if allowOne {
				n = 1
			}
			want := n <= 0 || n <= limits[next]-used[next]
			got := false
			if allowOne {
				got = limiter.Allow()
			} else {
				got = limiter.AllowN(n)
			}
			if got != want {
				t.Fatalf("step %d shard %d request %d: got %v want %v; used=%v limits=%v", step, next, n, got, want, used, limits)
			}
			if got && n > 0 {
				used[next] += n
			}
			next = (next + 1) % shards
			for i := range used {
				shard := limiter.GetShard(i)
				if len(shard.timestamps) != used[i] {
					t.Fatalf("step %d shard %d retained=%d want=%d", step, i, len(shard.timestamps), used[i])
				}
			}
		}
	})
}
