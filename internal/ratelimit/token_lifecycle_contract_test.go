package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Keep all other permits spent and observe each shard independently. Releasing
// the same Token after draining again must never restore another permit, even
// when its first release happened against a full, resized or closed bucket.
func TestTokenReleaseLifecycleAndShardIdentity(t *testing.T) {
	for _, shards := range []int{0, 1, 3} {
		for _, transition := range []string{"release", "grow", "shrink", "refill", "close"} {
			t.Run(fmt.Sprintf("shards=%d/%s", shards, transition), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var bases []*RateLimiter
					var getToken func(context.Context) (*Token, error)
					var closeAll func()
					var outerRelease func()
					if shards == 0 {
						rl := NewRateLimiter(2, time.Second).WithStrategy(Reject)
						bases = []*RateLimiter{rl}
						getToken = rl.Token
						closeAll = rl.Close
					} else {
						rl := NewShardedRateLimiter(shards, shards*2, time.Second).WithStrategy(Reject)
						bases = rl.limiters
						getToken = rl.Token
						closeAll = rl.Close
						outerRelease = rl.Release
					}
					defer closeAll()
					token, err := getToken(context.Background())
					if err != nil || token == nil {
						t.Fatalf("token=%v err=%v", token, err)
					}
					if token.limiter != bases[0] {
						t.Fatal("token not bound to selected shard")
					}
					for _, base := range bases {
						for base.Available() > 0 {
							if err := base.Acquire(context.Background()); err != nil {
								t.Fatal(err)
							}
						}
					}
					failed, err := getToken(context.Background())
					if err != core.ErrRateLimitExceeded || failed != nil {
						t.Fatalf("rejected token=%v err=%v", failed, err)
					}
					before := make([]int, len(bases))
					switch transition {
					case "grow":
						bases[0].Resize(4)
						before[0] = 2
					case "shrink":
						bases[0].Resize(1)
					case "refill":
						time.Sleep(time.Second)
						synctest.Wait() // Let the boundary tick finish before observing its balance.
						for i := range before {
							before[i] = 2
						}
					case "close":
						closeAll()
					}
					for i, base := range bases {
						if got := base.Available(); got != before[i] {
							t.Fatalf("transition shard=%d available=%d want=%d", i, got, before[i])
						}
					}
					releaseMany := func() {
						var wg sync.WaitGroup
						gate := make(chan struct{})
						for i := 0; i < 32; i++ {
							wg.Add(1)
							go func() { defer wg.Done(); <-gate; token.Release() }()
						}
						close(gate)
						wg.Wait()
					}
					releaseMany()
					expected := append([]int(nil), before...)
					if transition != "close" && transition != "refill" {
						expected[0]++
					}
					for i, base := range bases {
						if got := base.Available(); got != expected[i] {
							t.Fatalf("released shard=%d available=%d want=%d", i, got, expected[i])
						}
						for count := 0; count < expected[i]; count++ {
							if err := base.Acquire(context.Background()); err != nil {
								t.Fatal(err)
							}
						}
					}
					releaseMany()
					if outerRelease != nil {
						outerRelease()
					}
					for i, base := range bases {
						if got := base.Available(); got != 0 {
							t.Fatalf("duplicate release restored shard=%d available=%d", i, got)
						}
					}
					var want error = core.ErrRateLimitExceeded
					if transition == "close" {
						want = core.ErrRateLimiterStopped
					}
					failed, err = getToken(context.Background())
					if err != want || failed != nil {
						t.Fatalf("final token=%v err=%v want=%v", failed, err, want)
					}
				})
			})
		}
	}
}
