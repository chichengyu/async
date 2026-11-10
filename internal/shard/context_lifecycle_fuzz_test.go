package shard

import (
	"context"
	"errors"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// Start every task before controlled success/failure/cancellation, then compare
// both engines with a per-shard outcome oracle across sequential Reset rounds.
// No timing assumption, concurrent reconfiguration, or arbitrary callback is used.
func FuzzShardSharedContextLifecycle(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3}, uint8(0))
	f.Add([]byte{3, 2, 1, 0}, uint8(7))
	f.Add([]byte{1, 1, 2}, uint8(14))
	f.Fuzz(func(t *testing.T, data []byte, config uint8) {
		if len(data) > 4 {
			data = data[:4]
		}
		shards := int(config%3) + 2
		selected := int(config/3) % shards
		businessErr := errors.New("controlled shard failure")
		for engine := range 2 {
			sp := NewShardedPoolSimple[int](shards, 1)
			sg := NewShardedGroup(ShardGroupConfig[int]{Shards: shards, ConcurrencyPerShard: 1})
			defer sp.Close()
			defer sg.Close()
			for round, b := range data {
				if round > 0 {
					var err error
					if engine == 0 {
						err = sp.Reset()
					} else {
						err = sg.Reset()
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				type modelKey struct{}
				parent, cancel := context.WithCancel(context.WithValue(context.Background(), modelKey{}, round))
				var shared context.Context
				if engine == 0 {
					_, shared = sp.WithFailFast(parent)
				} else {
					_, shared = sg.WithFailFast(parent)
				}
				if shared.Err() != nil || shared.Value(modelKey{}) != round {
					cancel()
					t.Fatal("fresh shared context invalid")
				}
				started := make([]chan struct{}, shards)
				release := make([]chan struct{}, shards)
				for s := range shards {
					started[s] = make(chan struct{})
					release[s] = make(chan struct{})
					fn := func(ctx context.Context) (int, error) {
						close(started[s])
						<-release[s]
						if s == selected {
							switch b % 4 {
							case 2:
								return s + 1, businessErr
							case 3:
								panic(s)
							}
						}
						return s + 1, ctx.Err()
					}
					var err error
					if engine == 0 {
						err = sp.SubmitAt(s, 0, shared, fn)
					} else {
						err = sg.GoAt(s, 0, shared, fn)
					}
					if err != nil {
						for j := 0; j <= s; j++ {
							close(release[j])
						}
						cancel()
						t.Fatal(err)
					}
				}
				for _, ch := range started {
					<-ch
				}
				if b%4 == 1 {
					cancel()
				}
				close(release[selected])
				var first []core.Result[int]
				if engine == 0 {
					first = sp.GetShard(selected).Wait()
				} else {
					first = sg.GetShard(selected).Wait()
				}
				gotCanceled := shared.Err() != nil
				// Release all other tasks before reporting failures so cleanup cannot hang.
				for s := range shards {
					if s != selected {
						close(release[s])
					}
				}
				if gotCanceled != (b%4 != 0) {
					cancel()
					t.Fatalf("engine=%d round=%d mode=%d shared cancellation=%v", engine, round, b%4, gotCanceled)
				}
				var all []core.Result[int]
				if engine == 0 {
					all = append(first, sp.Wait()...)
				} else {
					all = sg.Wait()
				}
				if len(all) != shards {
					cancel()
					t.Fatalf("engine=%d outcomes=%v want %d", engine, all, shards)
				}
				// Group returns shard order; Pool places the already-consumed shard first.
				seen := make([]bool, shards)
				for _, r := range all {
					var pe *core.PanicError
					if errors.As(r.Err, &pe) {
						if b%4 != 3 || pe.Value != selected || r.Value != 0 || seen[selected] {
							cancel()
							t.Fatalf("unexpected panic=%+v", r)
						}
						seen[selected] = true
						continue
					}
					s := r.Value - 1
					if !r.Occupied || s < 0 || s >= shards || seen[s] {
						cancel()
						t.Fatalf("invalid outcome=%+v", r)
					}
					seen[s] = true
					var want error
					if b%4 != 0 {
						want = context.Canceled
					}
					if b%4 == 2 && s == selected {
						want = businessErr
					}
					if !errors.Is(r.Err, want) {
						cancel()
						t.Fatalf("engine=%d shard=%d err=%v want=%v", engine, s, r.Err, want)
					}
				}
				if shared.Err() != context.Canceled || (parent.Err() != nil) != (b%4 == 1) {
					cancel()
					t.Fatalf("engine=%d shared/parent lifetime invalid", engine)
				}
				cancel()
			}
		}
	})
}
