package retry

import (
	"context"
	"testing"

	"github.com/chichengyu/async/internal/ratelimit"
)

func TestRetryTokenBucketHonorsRateRegardlessOfConfigurationOrder(t *testing.T) {
	for _, modeFirst := range []bool{false, true} {
		chain := New[int](context.Background())
		if modeFirst {
			chain.TokenBucket().Rate(3)
		} else {
			chain.Rate(3).TokenBucket()
		}
		// With no explicit Capacity, the bucket starts with 2*Rate tokens.
		// A request above that capacity must fail even after time passes.
		bucket := chain.buildTokenBucket()
		if bucket.AllowN(7) {
			t.Fatalf("modeFirst=%v: Rate(3) allowed seven tokens, want capacity six", modeFirst)
		}
		if !bucket.AllowN(6) {
			t.Fatalf("modeFirst=%v: Rate(3) did not supply six initial tokens", modeFirst)
		}
	}
}

func TestRetryDefaultRateResetsTokenBucketRate(t *testing.T) {
	bucket := New[int](context.Background()).TokenBucket().Rate(3).DefaultRate().buildTokenBucket()
	if bucket.AllowN(21) || !bucket.AllowN(20) {
		t.Fatal("DefaultRate must restore ten tokens/second and default capacity twenty")
	}
}

func TestRetryShardedTokenBucketUsesConfiguredRatePerShard(t *testing.T) {
	chain := New[int](context.Background()).TokenBucket().Shards(2).Rate(3)
	bucket := ratelimit.NewShardedTokenBucket(chain.rlShards, chain.tbRateOrDefault(), chain.tbCapacityOrDefault())
	for i := 0; i < bucket.ShardCount(); i++ {
		shard := bucket.GetShard(i)
		if shard.AllowN(7) || !shard.AllowN(6) {
			t.Fatalf("shard %d did not use Rate(3) and default capacity six", i)
		}
	}
}
