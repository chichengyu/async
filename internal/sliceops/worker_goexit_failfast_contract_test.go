package sliceops

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

func TestSliceWorkerGoexitTriggersFailFast(t *testing.T) {
	for _, mode := range []string{"map-pool", "stream", "stream-pool", "each-stream"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			fn := func(_ context.Context, v int) (int, error) {
				calls.Add(1)
				if v == 1 {
					runtime.Goexit()
				}
				return v * 10, nil
			}
			p := Par(1).FF().Buf(1)
			if mode == "map-pool" || mode == "stream-pool" {
				pl := pool.NewPool[int](1)
				defer pl.Close()
				p = p.WithPool(pl)
			}
			items := []int{1, 2, 3, 4, 5, 6}
			goexits := 0
			switch mode {
			case "map-pool":
				rs, err := execMap(context.Background(), items, fn, p)
				if !errors.Is(err, core.ErrTaskGoexit) {
					t.Errorf("summary err=%v", err)
				}
				for _, r := range rs {
					if errors.Is(r.Err, core.ErrTaskGoexit) {
						goexits++
					}
				}
			case "each-stream":
				for r := range execEachStream(context.Background(), items, func(ctx context.Context, v int) error { _, err := fn(ctx, v); return err }, p) {
					if errors.Is(r.Err, core.ErrTaskGoexit) {
						goexits++
					}
				}
			default:
				for r := range execStream(context.Background(), items, fn, p) {
					if errors.Is(r.Err, core.ErrTaskGoexit) {
						goexits++
					}
				}
			}
			if calls.Load() != 1 || goexits != 1 {
				t.Fatalf("calls=%d goexits=%d", calls.Load(), goexits)
			}
		})
	}
}
