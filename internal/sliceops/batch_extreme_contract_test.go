package sliceops

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// Build the oracle one element at a time: do not reuse Chunk, ceil division,
// result collectors, or scheduler counts from the implementation under test.
func TestBatchExtremeSizesAndFailureProjection(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, length := range []int{0, 1, 7} {
		for _, size := range []int{-maxInt - 1, -1, 0, 1, 2, 3, maxInt - 1, maxInt} {
			for _, mode := range []string{"serial", "parallel", "timeout", "shard", "pool-limited", "pool-ring"} {
				for _, outcome := range []int{0, 1, 2} {
					t.Run(fmt.Sprintf("n=%d/size=%d/%s/outcome=%d", length, size, mode, outcome), func(t *testing.T) {
						items := make([]int, length)
						var batches [][]int
						for i := range items {
							items[i] = i
							if len(batches) == 0 || size <= 0 || len(batches[len(batches)-1]) == size {
								batches = append(batches, nil)
							}
							batches[len(batches)-1] = append(batches[len(batches)-1], i)
						}
						p := Par(2).Chunk(size)
						switch mode {
						case "serial":
							p = Seq().Chunk(size)
						case "timeout":
							p = p.TO(time.Hour)
						case "shard":
							p = p.Shard(3)
						case "pool-limited", "pool-ring":
							pl := pool.NewPool[int](2).WithMaxResults(1)
							if mode == "pool-ring" {
								pl.WithRingBuffer(1, core.OverflowDrop)
							}
							t.Cleanup(pl.Close)
							p = p.WithPool(pl)
						}
						b := NewSliceBuilder(context.Background(), items)
						b.policy = p
						failure := errors.New("batch failed")
						visits := make([]atomic.Int32, length)
						invoke := func(part []int) (int, error) {
							if len(part) == 0 || part[0] < 0 || part[0] >= length {
								t.Errorf("invalid batch: %v", part)
								return 0, failure
							}
							index := -1
							for j, want := range batches {
								if want[0] == part[0] {
									index = j
									if !slices.Equal(part, want) {
										t.Errorf("batch %d: %v want %v", j, part, want)
									}
									break
								}
							}
							if index < 0 {
								t.Errorf("unexpected batch start: %v", part)
								return 0, failure
							}
							visits[part[0]].Add(1)
							sum := 0
							for _, v := range part {
								sum += v + 1
							}
							switch (index + outcome) % 3 {
							case 1:
								return sum, failure
							case 2:
								panic(index)
							default:
								return sum, nil
							}
						}
						result := b.MapBatch(func(_ context.Context, part []int) (int, error) { return invoke(part) })
						if result.Len() != len(batches) {
							t.Fatalf("result slots=%d want %d", result.Len(), len(batches))
						}
						var wantValues []int
						var wantFailed []any
						var wantFailures int64
						for i, batch := range batches {
							got := result.Results()[i]
							sum := 0
							for _, v := range batch {
								sum += v + 1
								if (i+outcome)%3 != 0 {
									wantFailed = append(wantFailed, v)
								}
							}
							if (i+outcome)%3 != 0 {
								wantFailures++
							}
							switch (i + outcome) % 3 {
							case 0:
								wantValues = append(wantValues, sum)
								if got.Err != nil || got.Value != sum {
									t.Errorf("success batch %d: %+v", i, got)
								}
							case 1:
								if !errors.Is(got.Err, failure) || got.Value != sum {
									t.Errorf("error batch %d: %+v", i, got)
								}
							case 2:
								var pe *core.PanicError
								if !errors.As(got.Err, &pe) || pe.Value != i || got.Value != 0 {
									t.Errorf("panic batch %d: %+v", i, got)
								}
							}
						}
						if !slices.Equal(result.Values(), wantValues) || !reflect.DeepEqual(result.FailValues(), wantFailed) || len(result.Errors()) != int(wantFailures) || result.Err() != (wantFailures > 0) {
							t.Errorf("projection values=%v failed=%v errors=%v; want %v/%v/%d", result.Values(), result.FailValues(), result.Errors(), wantValues, wantFailed, wantFailures)
						}
						for i := range visits {
							want := int32(0)
							for _, batch := range batches {
								if batch[0] == i {
									want = 1
								}
							}
							if visits[i].Swap(0) != want {
								t.Errorf("MapBatch calls at %d want %d", i, want)
							}
						}
						each := b.ForEachBatch(func(_ context.Context, part []int) error { _, err := invoke(part); return err })
						if each.Total() != int64(len(batches)) || each.FailCount() != wantFailures || each.OkCount() != int64(len(batches))-wantFailures || each.Err() != (wantFailures > 0) {
							t.Errorf("batch stats total=%d failed=%d ok=%d err=%v", each.Total(), each.FailCount(), each.OkCount(), each.Error())
						}
						for _, batch := range batches {
							if visits[batch[0]].Load() != 1 {
								t.Errorf("ForEachBatch calls at %d: %d", batch[0], visits[batch[0]].Load())
							}
						}
						for i, value := range items {
							if value != i {
								t.Errorf("input changed at %d: %d", i, value)
							}
						}
					})
				}
			}
		}
	}
}
