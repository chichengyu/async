package sliceops

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// The borrowed-pool postcondition differs for empty Map and Each/Stream. Use
// a completed unrelated sentinel to observe whether a terminal resets history.
func TestRunnerPoolOwnershipByTerminal(t *testing.T) {
	for _, terminal := range []string{"Map", "MapBatch", "Each", "EachBatch", "Stream", "Fold"} {
		for _, empty := range []bool{false, true} {
			for _, owned := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/empty=%v/owned=%v", terminal, empty, owned), func(t *testing.T) {
					ctx := context.Background()
					pl := pool.NewPool[int](2)
					t.Cleanup(pl.Close)
					finished := make(chan struct{})
					if err := pl.SubmitWithCompletion(ctx, func(context.Context) (int, error) { return -1, nil }, func(core.Result[int]) { close(finished) }); err != nil {
						t.Fatal(err)
					}
					<-finished
					input := []int{2, 3, 4}
					if empty {
						input = nil
					}
					p := Par(2).Chunk(2).WithPool(pl)
					if owned {
						p = p.WithOwnedPool(pl)
					}
					r := NewRunner[int, int](ctx, input, p)
					want := []int{4, 6, 8}
					if empty {
						want = nil
					}
					var values []int
					var err error
					switch terminal {
					case "Map":
						var results []core.Result[int]
						results, err = r.Map(func(_ context.Context, v int) (int, error) { return v * 2, nil })
						for _, v := range results {
							if v.Err != nil {
								t.Fatal(v.Err)
							}
							values = append(values, v.Value)
						}
					case "MapBatch":
						if !empty {
							want = []int{5, 4}
						}
						var results []core.Result[int]
						results, err = r.MapBatch(func(_ context.Context, part []int) (int, error) {
							sum := 0
							for _, v := range part {
								sum += v
							}
							return sum, nil
						})
						for _, v := range results {
							if v.Err != nil {
								t.Fatal(v.Err)
							}
							values = append(values, v.Value)
						}
					case "Each", "EachBatch":
						var total, failures int64
						var results []core.Result[struct{}]
						expected := int64(len(input))
						if terminal == "Each" {
							total, failures, err, results = r.Each(func(context.Context, int) error { return nil })
						} else {
							if !empty {
								expected = 2
							}
							total, failures, err, results = r.EachBatch(func(context.Context, []int) error { return nil })
						}
						if total != expected || failures != 0 || results != nil {
							t.Fatalf("each stats=%d/%d results=%v want total=%d", total, failures, results, expected)
						}
						want = nil
					case "Stream":
						for v := range r.Stream(func(_ context.Context, v int) (int, error) { return v * 2, nil }, 1) {
							if v.Err != nil {
								t.Fatal(v.Err)
							}
							values = append(values, v.Value)
						}
						slices.Sort(values)
					case "Fold":
						var sum int
						sum, err = r.Fold(10, func(_ context.Context, acc, v int) (int, error) { return acc + v, nil })
						want = []int{19}
						if empty {
							want = []int{10}
						}
						values = []int{sum}
					}
					if err != nil || !slices.Equal(values, want) {
						t.Fatalf("terminal values=%v want=%v err=%v", values, want, err)
					}
					if !owned {
						expected := int64(0)
						if terminal == "Fold" || empty && (terminal == "Map" || terminal == "MapBatch") {
							expected = 1
						}
						if count := pl.TotalCount(); count != expected {
							t.Fatalf("borrowed pool retained count=%d want=%d", count, expected)
						}
					}
					submitErr := pl.Submit(ctx, func(context.Context) (int, error) { return 42, nil })
					if owned {
						if !errors.Is(submitErr, core.ErrPoolClosed) {
							t.Fatalf("owned pool not closed: %v", submitErr)
						}
					} else if submitErr != nil {
						t.Fatalf("borrowed pool not reusable: %v", submitErr)
					}
				})
			}
		}
	}
}
