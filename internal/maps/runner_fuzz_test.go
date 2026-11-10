package maps

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/sliceops"
)

// Fixed key slots provide an independent oracle, with no reliance on map
// iteration order. Policies cross serial, parallel, shard scheduling and pools.
func FuzzMapRunnerStrategies(f *testing.F) {
	f.Add([]byte{}, uint8(0), uint8(0))
	f.Add([]byte{0, 1, 2, 255, 7, 9, 3}, uint8(1), uint8(3))
	f.Add([]byte{4, 4, 4, 1, 128, 2}, uint8(255), uint8(16))
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7}, uint8(2), uint8(1))
	f.Add([]byte{255, 0, 1, 128, 64}, uint8(1), uint8(2))
	f.Fuzz(func(t *testing.T, input []byte, failByte, config uint8) {
		if len(input) > 48 {
			input = input[:48]
		}
		data := make(map[int]int, len(input))
		for k, v := range input {
			data[k] = int(int8(v))
		}
		workers := int(config%5) + 1
		borrowed := pool.NewPool[int](workers).WithMaxResults(1).
			WithRingBuffer(1, []core.OverflowStrategy{core.OverflowDrop, core.OverflowError, core.OverflowBlock}[config%3])
		defer borrowed.Close()
		policies := []sliceops.Policy{
			sliceops.Seq(),
			sliceops.Par(workers),
			sliceops.Par(workers).Shard(int(config%9) + 1),
			sliceops.Par(workers).WithDefaultPool(),
			sliceops.Par(workers).WithPool(borrowed),
		}
		wantErr := errors.New("model business error")
		fails := func(k int) bool { return k%7 == int(failByte%7) }
		wantFailures := 0
		for k := range input {
			if fails(k) {
				wantFailures++
			}
		}
		for mode, policy := range policies {
			runner := NewMapRunner[int, int, int](context.Background(), data, policy)
			mapped := runner.Map(func(_ context.Context, k, v int) (int, error) {
				if fails(k) {
					return 999, wantErr
				}
				return v*3 + k, nil
			})
			if mapped.Len() != len(input)-wantFailures || (mapped.Error() != nil) != (wantFailures > 0) {
				t.Fatalf("mode %d mapping length/error: %d %v", mode, mapped.Len(), mapped.Error())
			}
			for k, b := range input {
				got, ok := mapped.Data()[k]
				if ok == fails(k) || (ok && got != int(int8(b))*3+k) {
					t.Fatalf("mode %d mapping key %d: %d %v", mode, k, got, ok)
				}
			}
			var mu sync.Mutex
			visits := make([]int, len(input))
			total, failures, err := runner.ForEach(func(_ context.Context, k, v int) error {
				mu.Lock()
				visits[k]++
				mu.Unlock()
				if v != int(int8(input[k])) {
					return errors.New("changed input")
				}
				if fails(k) {
					return wantErr
				}
				return nil
			})
			if total != int64(len(input)) || failures != int64(wantFailures) || (err != nil) != (wantFailures > 0) {
				t.Fatalf("mode %d foreach stats: %d %d %v", mode, total, failures, err)
			}
			for k, count := range visits {
				if count != 1 {
					t.Fatalf("mode %d foreach key %d called %d times", mode, k, count)
				}
			}
			seen := make(map[int]bool, len(input))
			for result := range runner.Stream(func(_ context.Context, k, v int) (int, error) {
				if fails(k) {
					return k, wantErr
				}
				return k, nil
			}, int(config%4)) {
				k := result.Value
				if k < 0 || k >= len(input) || seen[k] || errors.Is(result.Err, wantErr) != fails(k) {
					t.Fatalf("mode %d stream result: %+v", mode, result)
				}
				seen[k] = true
			}
			if len(seen) != len(input) {
				t.Fatalf("mode %d stream count %d want %d", mode, len(seen), len(input))
			}
			chunkSize := int(config%7) + 1
			batchRunner := NewMapRunner[int, int, int](context.Background(), data, policy.Chunk(chunkSize))
			batchVisits := make([]int, len(input))
			batchHeads := make(map[int]int)
			batchResult := batchRunner.MapBatch(func(_ context.Context, batch []MapEntry[int, int]) (int, error) {
				mu.Lock()
				defer mu.Unlock()
				if len(batch) == 0 || len(batch) > chunkSize {
					return 0, errors.New("invalid chunk length")
				}
				sum := 0
				for _, entry := range batch {
					batchVisits[entry.Key]++
					sum += entry.Val
				}
				batchHeads[batch[0].Key] = sum
				return sum, nil
			})
			wantBatches := (len(input) + chunkSize - 1) / chunkSize
			if batchResult.Error() != nil || batchResult.Len() != wantBatches || len(batchHeads) != wantBatches {
				t.Fatalf("mode %d batch count/error: %d %v", mode, batchResult.Len(), batchResult.Error())
			}
			for k, count := range batchVisits {
				if count != 1 {
					t.Fatalf("mode %d batch key %d called %d times", mode, k, count)
				}
			}
			for k, want := range batchHeads {
				if got, ok := batchResult.Data()[k]; !ok || got != want {
					t.Fatalf("mode %d batch head %d: %d %v want %d", mode, k, got, ok, want)
				}
			}
			total, failures, err = batchRunner.ForEachBatch(func(_ context.Context, batch []MapEntry[int, int]) error {
				return wantErr // Counts failed elements, not failed chunks.
			})
			if total != int64(len(input)) || failures != total || errors.Is(err, wantErr) != (len(input) > 0) {
				t.Fatalf("mode %d batch stats: %d %d %v", mode, total, failures, err)
			}
			// Partial batch failures use the actual head selected by this run.
			// A failed short tail must contribute its own size, not chunkSize.
			partialHeads := make(map[int]int)
			failedBatches := 0
			partial := batchRunner.MapBatch(func(_ context.Context, batch []MapEntry[int, int]) (int, error) {
				mu.Lock()
				defer mu.Unlock()
				if fails(batch[0].Key) {
					failedBatches++
					return 999, wantErr
				}
				sum := 0
				for _, entry := range batch {
					sum += entry.Val
				}
				partialHeads[batch[0].Key] = sum
				return sum, nil
			})
			if partial.Len() != wantBatches-failedBatches || errors.Is(partial.Error(), wantErr) != (failedBatches > 0) {
				t.Fatalf("mode %d partial map count/error: %d %v", mode, partial.Len(), partial.Error())
			}
			for head, sum := range partialHeads {
				if got, ok := partial.Data()[head]; !ok || got != sum {
					t.Fatalf("mode %d partial batch head %d: %d %v want %d", mode, head, got, ok, sum)
				}
			}
			var failedElements int64
			total, failures, err = batchRunner.ForEachBatch(func(_ context.Context, batch []MapEntry[int, int]) error {
				mu.Lock()
				defer mu.Unlock()
				if fails(batch[0].Key) {
					failedElements += int64(len(batch))
					return wantErr
				}
				return nil
			})
			if total != int64(len(input)) || failures != failedElements || errors.Is(err, wantErr) != (failedElements > 0) {
				t.Fatalf("mode %d partial batch stats: %d %d %v want failures %d", mode, total, failures, err, failedElements)
			}
		}
	})
}
