package ratelimit

import (
	"testing"
)

// The permit model checks capacity after adjustment; this independent integer
// oracle additionally checks exactly when and by how much the target changes.
func FuzzAdaptiveAdjustmentThresholds(f *testing.F) {
	f.Add(uint8(1), uint8(19), uint8(1), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	f.Add(uint8(4), uint8(25), uint8(3), []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	f.Add(uint8(0), uint8(0), uint8(0), []byte{})
	f.Add(uint8(2), uint8(70), uint8(2), []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 1})
	f.Fuzz(func(t *testing.T, minimumByte, extraByte, shardsByte uint8, records []byte) {
		minimum := 1 + int(minimumByte%8)
		maximum := minimum + int(extraByte%96)
		shards := 1 + int(shardsByte%4)
		if len(records) > 1024 {
			records = records[:1024]
		}
		single := NewAdaptiveRateLimiter(minimum, maximum)
		defer single.base.Close()
		sharded := NewShardedAdaptiveRateLimiter(shards, minimum, maximum)
		defer sharded.Close()
		type target struct{ min, max, current, successes, failures int }
		model := make([]target, shards+1)
		model[0] = target{min: minimum, max: maximum, current: minimum + (maximum-minimum)/2}
		for i := 0; i < shards; i++ {
			low := minimum / shards
			if i < minimum%shards {
				low++
			}
			low = max(low, 1)
			high := maximum / shards
			if i < maximum%shards {
				high++
			}
			high = max(high, low)
			model[i+1] = target{min: low, max: high, current: low + (high-low)/2}
		}
		for step, record := range records {
			failed := record&1 != 0
			if failed {
				single.RecordFailure()
				sharded.RecordFailure()
			} else {
				single.RecordOk()
				sharded.RecordOk()
			}
			for _, index := range []int{0, 1 + step%shards} {
				m := &model[index]
				if failed {
					m.failures++
				} else {
					m.successes++
				}
				n := m.successes + m.failures
				if n >= 10 {
					switch {
					case m.failures*5 > n:
						m.current = max(m.current/2, m.min)
					case m.failures*10 < n:
						m.current = min(m.current+max((m.max-m.current)/5, 1), m.max)
					}
					m.successes, m.failures = 0, 0
				}
			}
			for index, m := range model {
				actual := single
				if index > 0 {
					actual = sharded.GetShard(index - 1)
				}
				if actual.base.Size() != m.current || actual.base.Available() != m.current {
					t.Fatalf("step=%d index=%d bounds=[%d,%d] size=%d available=%d want=%d", step, index, m.min, m.max, actual.base.Size(), actual.base.Available(), m.current)
				}
			}
		}
	})
}
