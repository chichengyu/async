package maps

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/chichengyu/async/internal/sliceops"
)

// Values identify entries independently of key lookup, which cannot find NaNs.
func TestNonReflexiveMapRunnerEntryContracts(t *testing.T) {
	nan := math.Float64frombits(0x7ff8000000000042)
	data := map[any]int{1: 10}
	for i, key := range []any{nan, nan, float32(math.NaN()), complex(nan, 1), [1]any{nan}, struct{ X float64 }{nan}} {
		data[key] = 20 + i
	}
	signatures := make(map[int]string)
	var all []int
	for key, value := range data {
		signatures[value] = nonReflexiveKeySignature(key)
		all = append(all, value)
	}
	slices.Sort(all)
	for mode, policy := range []sliceops.Policy{sliceops.Seq(), sliceops.Par(3), sliceops.Par(3).Shard(2), sliceops.Par(3).WithDefaultPool()} {
		for _, method := range []string{"map", "each", "stream", "filter", "reduce", "map-batch", "each-batch"} {
			t.Run(fmt.Sprintf("%d/%s", mode, method), func(t *testing.T) {
				runner := NewMapRunner[any, int, int](context.Background(), data, policy.Chunk(3))
				var mu sync.Mutex
				var visits []int
				visit := func(key any, value int) {
					mu.Lock()
					defer mu.Unlock()
					if signature, ok := signatures[value]; !ok || signature != nonReflexiveKeySignature(key) {
						t.Errorf("changed key/value entry: %v=%d", key, value)
					}
					visits = append(visits, value)
				}
				checkMap := func(result map[any]int, offset int, want []int) {
					var values []int
					for key, value := range result {
						original := value - offset
						if signature, ok := signatures[original]; !ok || signature != nonReflexiveKeySignature(key) {
							t.Errorf("changed result entry: %v=%d", key, value)
						}
						values = append(values, original)
					}
					slices.Sort(values)
					if !slices.Equal(values, want) {
						t.Errorf("result values=%v want=%v", values, want)
					}
				}
				switch method {
				case "map":
					result := runner.Map(func(_ context.Context, key any, value int) (int, error) { visit(key, value); return value + 100, nil })
					if result.Error() != nil {
						t.Fatal(result.Error())
					}
					checkMap(result.Data(), 100, all)
				case "each":
					total, failed, err := runner.ForEach(func(_ context.Context, key any, value int) error { visit(key, value); return nil })
					if total != int64(len(all)) || failed != 0 || err != nil {
						t.Fatalf("stats=%d/%d/%v", total, failed, err)
					}
				case "stream":
					var values []int
					for result := range runner.Stream(func(_ context.Context, key any, value int) (int, error) { visit(key, value); return value, nil }, 1) {
						if result.Err != nil {
							t.Fatal(result.Err)
						}
						values = append(values, result.Value)
					}
					slices.Sort(values)
					if !slices.Equal(values, all) {
						t.Fatalf("stream=%v want=%v", values, all)
					}
				case "filter":
					result, err := runner.Filter(func(_ context.Context, key any, value int) bool { visit(key, value); return value%2 == 0 })
					if err != nil {
						t.Fatal(err)
					}
					var want []int
					for _, value := range all {
						if value%2 == 0 {
							want = append(want, value)
						}
					}
					checkMap(result.AsMap(), 0, want)
				case "reduce":
					result, err := runner.Reduce(5, func(_ context.Context, acc int, key any, value int) (int, error) {
						visit(key, value)
						return acc + value, nil
					})
					want := 5
					for _, value := range all {
						want += value
					}
					if err != nil || result != want {
						t.Fatalf("reduced=%d/%v want=%d", result, err, want)
					}
				case "map-batch", "each-batch":
					var heads []int
					batchVisit := func(entries []MapEntry[any, int]) {
						if len(entries) < 1 || len(entries) > 3 {
							t.Errorf("batch size=%d", len(entries))
						}
						for _, entry := range entries {
							visit(entry.Key, entry.Val)
						}
						mu.Lock()
						heads = append(heads, entries[0].Val)
						mu.Unlock()
					}
					if method == "map-batch" {
						result := runner.MapBatch(func(_ context.Context, entries []MapEntry[any, int]) (int, error) {
							batchVisit(entries)
							return entries[0].Val + 100, nil
						})
						if result.Error() != nil {
							t.Fatal(result.Error())
						}
						slices.Sort(heads)
						checkMap(result.Data(), 100, heads)
					} else {
						total, failed, err := runner.ForEachBatch(func(_ context.Context, entries []MapEntry[any, int]) error { batchVisit(entries); return nil })
						if total != int64(len(all)) || failed != 0 || err != nil {
							t.Fatalf("batch stats=%d/%d/%v", total, failed, err)
						}
					}
					if len(heads) != (len(all)+2)/3 {
						t.Errorf("batches=%d", len(heads))
					}
				}
				slices.Sort(visits)
				if !slices.Equal(visits, all) {
					t.Fatalf("visits=%v want=%v", visits, all)
				}
			})
		}
	}
}
