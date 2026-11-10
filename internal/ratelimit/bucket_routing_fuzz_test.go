package ratelimit

import "testing"

// Integer quarter-token accounting is independent of float refill arithmetic.
// Rate zero removes elapsed-time dependence; operations remain sequential.
func FuzzShardedBucketRouting(f *testing.F) {
	f.Add(uint8(2), uint8(8), []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	f.Add(uint8(1), uint8(4), []byte{1, 1, 1, 0, 0, 0, 4, 8, 12, 16})
	f.Add(uint8(3), uint8(5), []byte{255, 254, 253, 252, 251, 250, 249, 248})
	f.Add(uint8(0), uint8(0), []byte{0, 1, 2, 3, 4})
	f.Fuzz(func(t *testing.T, shardByte, capacityByte uint8, ops []byte) {
		shards := 1 + int(shardByte%8)
		capacity := int(capacityByte % 33)
		bucket := NewShardedTokenBucket(shards, 0, float64(capacity)/4)
		if bucket.ShardCount() != shards || bucket.GetShard(-1) != nil || bucket.GetShard(shards) != nil {
			t.Fatal("shard access contract changed")
		}
		remaining := make([]int, shards)
		for i := range remaining {
			remaining[i] = capacity
		}
		next := 0
		if len(ops) > 64 {
			ops = ops[:64]
		}
		for step, op := range ops {
			index, request := next%shards, int(op%39)-3
			var got bool
			switch op % 4 {
			case 0:
				request = 4
				got = bucket.Allow()
				next++
			case 1, 2:
				got = bucket.AllowN(float64(request) / 4)
				next++
			case 3:
				index = int(op/4) % shards
				got = bucket.GetShard(index).AllowN(float64(request) / 4)
			}
			want := request <= 0 || request <= remaining[index]
			if got != want {
				t.Fatalf("step %d shard=%d quarter-request=%d remaining=%v allowed=%v want=%v", step, index, request, remaining, got, want)
			}
			if want && request > 0 {
				remaining[index] -= request
			}
			for i, quarters := range remaining {
				if got := bucket.GetShard(i).tokens; got != float64(quarters)/4 {
					t.Fatalf("step %d shard %d balance=%v want quarter-tokens=%d", step, i, got, quarters)
				}
			}
		}
	})
}
