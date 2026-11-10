package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Mix the three admission APIs while keeping their release responsibilities
// separate. Outer Release may choose any outstanding outer permit; its choice
// is observed rather than duplicated by this model.
func FuzzShardedMixedPermitOwnership(f *testing.F) {
	f.Add(uint8(1), uint8(3), []byte{0, 1, 2, 3, 4, 5, 3, 3, 0, 0, 0, 3})
	f.Add(uint8(2), uint8(0), []byte{0, 0, 0, 0, 1, 3, 2, 4, 5, 3, 3})
	f.Add(uint8(0), uint8(0), []byte{1, 4, 4, 2, 5, 5, 3})
	f.Add(uint8(0), uint8(1), []byte{1, 3, 4, 3})
	f.Fuzz(func(t *testing.T, shardByte, rateByte uint8, operations []byte) {
		shards, total := 1+int(shardByte%4), 1+int(rateByte%16)
		l := NewShardedRateLimiter(shards, total, time.Hour).WithStrategy(Reject)
		defer l.Close()
		if len(operations) > 64 {
			operations = operations[:64]
		}
		capacity := make([]int, shards)
		outer, direct, handles := make([]int, shards), make([]int, shards), make([]int, shards)
		for i := range capacity {
			capacity[i] = max(1, total/shards)
			if total >= shards && i < total%shards {
				capacity[i]++
			}
		}
		type heldToken struct {
			token    *Token
			shard    int
			released bool
		}
		var tokens []heldToken
		next := 0
		ctx := context.Background()
		check := func(step int) {
			t.Helper()
			for i, c := range capacity {
				want := c - outer[i] - direct[i] - handles[i]
				if got := l.GetShard(i).Available(); got != want {
					t.Fatalf("step=%d shard=%d available=%d want=%d outer=%v direct=%v handles=%v", step, i, got, want, outer, direct, handles)
				}
			}
		}
		for step, op := range operations {
			switch op % 6 {
			case 0, 1, 2:
				idx := next % shards
				if op%6 == 2 {
					idx = int(op/6) % shards
				} else {
					next++
				}
				room := capacity[idx]-outer[idx]-direct[idx]-handles[idx] > 0
				var err error
				var token *Token
				switch op % 6 {
				case 0:
					err = l.Acquire(ctx)
				case 1:
					token, err = l.Token(ctx)
				case 2:
					err = l.GetShard(idx).Acquire(ctx)
				}
				if room {
					if err != nil {
						t.Fatalf("step=%d admission=%d unexpected error: %v", step, op%6, err)
					}
					switch op % 6 {
					case 0:
						outer[idx]++
					case 1:
						handles[idx]++
						tokens = append(tokens, heldToken{token: token, shard: idx})
					case 2:
						direct[idx]++
					}
				} else if !errors.Is(err, core.ErrRateLimitExceeded) || token != nil {
					t.Fatalf("step=%d exhausted admission=%d err=%v token=%v", step, op%6, err, token)
				}
			case 3:
				before := make([]int, shards)
				count := 0
				for i := range before {
					before[i] = l.GetShard(i).Available()
					count += outer[i]
				}
				l.Release()
				changed := 0
				for i := range before {
					delta := l.GetShard(i).Available() - before[i]
					if delta == 0 {
						continue
					}
					if delta != 1 || outer[i] == 0 {
						t.Fatalf("step=%d release changed unowned shard=%d delta=%d outer=%v", step, i, delta, outer)
					}
					outer[i]--
					changed++
				}
				if changed != min(count, 1) {
					t.Fatalf("step=%d release changed=%d held=%d", step, changed, count)
				}
			case 4:
				if len(tokens) > 0 {
					h := &tokens[int(op/6)%len(tokens)]
					h.token.Release()
					if !h.released {
						handles[h.shard]--
						h.released = true
					}
				}
			case 5:
				idx := int(op/6) % shards
				if direct[idx] > 0 {
					l.GetShard(idx).Release()
					direct[idx]--
				}
			}
			check(step)
		}
	})
}
