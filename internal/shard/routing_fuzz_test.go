package shard

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// Independent integer routing and exact-once oracles check batch submission
// against both engines without depending on completion ordering. No cancellation,
// overflow, Reset, or unsupported concurrent configuration is modeled here.
func FuzzShardBatchRouting(f *testing.F) {
	f.Add([]byte{}, uint8(0), uint8(0))
	f.Add([]byte{0, 255, 1, 7, 128}, uint8(3), uint8(0))
	f.Add([]byte{7, 7, 7, 2}, uint8(255), uint8(1))
	f.Fuzz(func(t *testing.T, input []byte, size, mode uint8) {
		if len(input) > 48 {
			input = input[:48]
		}
		shards, workers := int(size%5)+1, int(mode%3)+1
		items := make([]int, len(input))
		for i := range items {
			items[i] = i
		}
		ctx := context.Background()
		wantErr := errors.New("model business failure")
		for engine := range 2 {
			visits := make([]int, len(items))
			var mu sync.Mutex
			fn := func(_ context.Context, i int) (int, error) {
				mu.Lock()
				visits[i]++
				mu.Unlock()
				if input[i]%7 == 0 {
					return i, wantErr
				}
				return i, nil
			}
			seen := make([]bool, len(items))
			check := func(shardIdx, value int, err error) {
				if value < 0 || value >= len(items) || seen[value] {
					t.Fatalf("engine %d invalid/repeated value %d", engine, value)
				}
				wantShard := value % shards
				if engine == 0 && mode%2 == 1 {
					wantShard = int(input[value]) % shards
				}
				if shardIdx != wantShard || errors.Is(err, wantErr) != (input[value]%7 == 0) {
					t.Fatalf("engine %d value %d shard %d want %d err %v", engine, value, shardIdx, wantShard, err)
				}
				seen[value] = true
			}
			if engine == 0 {
				dist := RoundRobin
				if mode%2 == 1 {
					dist = Hash
				}
				sp := NewShardedPool(ShardPoolConfig[int]{
					Shards: shards, SizePerShard: workers, Distribution: dist,
					KeyFn: func(i int) uint64 { return uint64(input[i]) },
				})
				defer sp.Close()
				submitted, err := sp.SubmitBatch(ctx, items, fn)
				if err != nil || len(submitted) != len(items) {
					t.Fatalf("pool batch: %v, count %d", err, len(submitted))
				}
				for i, result := range submitted {
					want := i % shards
					if dist == Hash {
						want = int(input[i]) % shards
					}
					if result.Index != i || result.ShardIdx != want || result.Err != nil {
						t.Fatalf("pool submit %d: %+v", i, result)
					}
				}
				for s := range shards {
					for _, result := range sp.GetShard(s).Wait() {
						if !result.Occupied {
							t.Fatal("pool result unoccupied")
						}
						check(s, result.Value, result.Err)
					}
				}
				sp.Close()
			} else {
				// Even Hash does not change this engine's GoBatch round-robin.
				sg := NewShardedGroup(ShardGroupConfig[int]{Shards: shards, ConcurrencyPerShard: workers, Distribution: Hash})
				defer sg.Close()
				submitted, err := sg.GoBatch(ctx, items, fn)
				if err != nil || len(submitted) != len(items) {
					t.Fatalf("group batch: %v, count %d", err, len(submitted))
				}
				for i, result := range submitted {
					if result.Index != i || result.ShardIdx != i%shards || result.Err != nil {
						t.Fatalf("group submit %d: %+v", i, result)
					}
				}
				for s := range shards {
					for _, result := range sg.GetShard(s).Wait() {
						if !result.Occupied {
							t.Fatal("group result unoccupied")
						}
						check(s, result.Value, result.Err)
					}
				}
				sg.Close()
			}
			for i := range items {
				if visits[i] != 1 || !seen[i] {
					t.Fatalf("engine %d item %d visits %d seen %v", engine, i, visits[i], seen[i])
				}
			}
		}
	})
}
