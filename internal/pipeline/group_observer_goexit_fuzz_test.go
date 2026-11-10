package pipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Independent ledgers compare entered business outcomes with observations;
// no assumption is made about which queued tasks survive FailFast.
func FuzzPipelineGroupObserverTerminalLedger(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 3}, uint8(0), uint8(2), false)
	f.Add([]byte{3, 1, 0}, uint8(1), uint8(0), true)
	f.Add([]byte{2, 3, 0}, uint8(2), uint8(3), false)
	f.Fuzz(func(t *testing.T, input []byte, mode, workers uint8, failFast bool) {
		if len(input) > 16 {
			input = input[:16]
		}
		items := make([]int, len(input))
		for i := range items {
			items[i] = i + 1
		}
		marker := errors.New("business")
		var mu sync.Mutex
		expected, observed := map[string]int{}, map[string]int{}
		key := func(kind int, value int) string { return fmt.Sprintf("%d/%d", kind, value) }
		fn := func(_ context.Context, _ string, v int) (int, error) {
			kind := int(input[v-1] % 4)
			value := v * 10
			if kind >= 2 {
				value = 0
			}
			mu.Lock()
			expected[key(kind, value)]++
			mu.Unlock()
			switch kind {
			case 1:
				return value, marker
			case 2:
				panic(marker)
			case 3:
				runtime.Goexit()
			}
			return value, nil
		}
		b := NewPipelineBuilder(items).Stage("only", int(workers%4)+1).OnResult(func(stage string, r core.Result[int]) {
			if stage != "only" || !r.Occupied {
				t.Errorf("stage=%s result=%+v", stage, r)
			}
			kind := 0
			switch {
			case r.Err == nil:
			case errors.Is(r.Err, marker):
				kind = 1
			case r.IsPanic():
				kind = 2
			case errors.Is(r.Err, core.ErrTaskGoexit):
				kind = 3
			default:
				t.Errorf("unexpected observed error: %v", r.Err)
			}
			mu.Lock()
			observed[key(kind, r.Value)]++
			mu.Unlock()
		})
		if failFast {
			b.FailFast()
		}
		var results []core.Result[int]
		switch mode % 3 {
		case 0:
			for r := range b.ExecutePipe(fn, 1) {
				results = append(results, r)
			}
		case 1:
			results, _ = b.Parallel().Shard(2).Run(fn)
		case 2:
			results, _ = b.Parallel().AutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 4, CheckInterval: time.Hour}).Run(fn)
		}
		if len(expected) != len(observed) {
			t.Fatalf("entered=%v observed=%v", expected, observed)
		}
		count := 0
		for k, want := range expected {
			count += want
			if observed[k] != want {
				t.Fatalf("entered=%v observed=%v", expected, observed)
			}
		}
		if !failFast && (count != len(items) || len(results) != len(items)) {
			t.Fatalf("calls=%d results=%d input=%d", count, len(results), len(items))
		}
	})
}
