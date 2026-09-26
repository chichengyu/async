package async

import (
	"context"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/mapreduce"
	"github.com/chichengyu/async/ratelimit"
	"github.com/chichengyu/async/retry"
)

// Chain 娉涘瀷閾惧紡璋冪敤鍣紝鏀寔瀵瑰垏鐗囧厓绱犺繘琛岄摼寮忓苟鍙戞搷浣溿€?
// 閫氳繃 ChainSlice 鍒涘缓锛屽彲閾惧紡璋冪敤 Filter銆丗orEach銆丆hunk 绛夋柟娉曘€?
// 瀵逛簬鏀瑰彉鍏冪礌绫诲瀷鐨勬搷浣滐紙Map銆丗latMap銆丷educe 绛夛級锛屼娇鐢ㄥ搴旂殑鐙珛鍑芥暟銆?
//
// Chain 鏄函澧為噺璁捐锛屼笉淇敼浠讳綍鐜版湁浠ｇ爜锛?00% 鍚戝悗鍏煎銆?
//
// 浣跨敤绀轰緥锛?
//
//	c := async.ChainSlice(ctx, []int{1, 2, 3, 4, 5}).
//	    WithConcurrency(4).
//	    Filter(func(n int) bool { return n > 2 })
//	c2 := async.ChainMap(c, 0, func(ctx context.Context, n int) (string, error) {
//	    return fmt.Sprintf("val-%d", n), nil
//	})
//	result := c2.ForEach(0, saveResult).Values()
//	// result = ["val-3", "val-4", "val-5"]
//
// chainRetryConfig defines retry parameters for chain operations.
type chainRetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Chain[T any] struct {
	ctx         context.Context
	items       []T
	concurrency int
	timeout     time.Duration
	failFast    bool
	shards      int
	firstErr    error
	rateLimiter *ratelimit.RateLimiter
	retryConfig *chainRetryConfig
}

// ChainSlice 浠庡垏鐗囧垱寤洪摼寮忚皟鐢ㄥ櫒锛屽紑濮嬩竴娈甸摼寮忔搷浣溿€?
// ctx 浼氳嚜鍔ㄦ敞鍏?trace_id銆?
func ChainSlice[T any](ctx context.Context, items []T) *Chain[T] {
	return &Chain[T]{
		ctx:         core.EnsureTraceID(ctx),
		items:       items,
		concurrency: defaultIO(),
	}
}

// WithConcurrency 璁剧疆閾剧殑榛樿骞跺彂搴︼紝鐢ㄤ簬鍚庣画鎵€鏈夋搷浣溿€?
// n <= 0 鏃朵繚鎸佸綋鍓嶅€间笉鍙樸€?
func (c *Chain[T]) WithConcurrency(n int) *Chain[T] {
	if n > 0 {
		c.concurrency = n
	}
	return c
}

// WithConcurrencyDefault 浣跨敤榛樿 IO 骞跺彂搴︺€?
func (c *Chain[T]) WithConcurrencyDefault() *Chain[T] {
	c.concurrency = defaultIO()
	return c
}

// WithTimeout 璁剧疆閾剧殑榛樿瓒呮椂鏃堕棿锛岀敤浜庡悗缁墍鏈夋搷浣溿€?
func (c *Chain[T]) WithTimeout(d time.Duration) *Chain[T] {
	c.timeout = d
	return c
}

// WithTimeoutDefault 浣跨敤榛樿瓒呮椂锛?0s锛夈€?
func (c *Chain[T]) WithTimeoutDefault() *Chain[T] {
	c.timeout = defaultTimeout
	return c
}

// WithFailFast 鍚敤 FailFast 妯″紡锛氫换涓€鎿嶄綔澶辫触鍚庯紝鍚庣画鎿嶄綔鐩存帴璺宠繃銆?
func (c *Chain[T]) WithFailFast() *Chain[T] {
	c.failFast = true
	return c
}

// WithContext 鏇挎崲閾剧殑涓婁笅鏂囷紝鑷姩娉ㄥ叆 trace_id銆?
func (c *Chain[T]) WithContext(ctx context.Context) *Chain[T] {
	c.ctx = core.EnsureTraceID(ctx)
	return c
}

// WithShards 璁剧疆姘村钩鍒嗙墖鏁帮紝鍚敤鏃?ForEachSharded / ChainMapSharded 浼氬皢浠诲姟鍒嗗彂鍒?N 涓?Group 瀹炰緥骞惰鎵ц銆?
// shards <= 0 鏃朵娇鐢ㄩ粯璁よ嚜鍔ㄥ垎鐗囨暟锛坮untime.GOMAXPROCS(0)锛屾渶灏?2锛夈€?
func (c *Chain[T]) WithShards(shards int) *Chain[T] {
	c.shards = shards
	return c
}

// WithShardsDefault 浣跨敤榛樿姘村钩鍒嗙墖鏁帮紙runtime.GOMAXPROCS(0)锛屾渶灏?2锛夈€?
// 鐢熶骇鐜涓垎鐗囨暟绛変簬鍙敤鐨?P 鏁伴噺锛岄€傚悎 IO 瀵嗛泦鍨嬮摼寮忓鐞嗭紙濡?ETL 绠￠亾锛夈€?
// 楂樺苟鍙戞帹鑽愭樉寮忚缃?WithShards(16) 鎴?WithShards(32) 鏉ュ尮閰嶅疄渚嬭鏍笺€?
func (c *Chain[T]) WithShardsDefault() *Chain[T] {
	c.shards = 0
	return c
}

// WithRateLimiter sets a rate limiter for all chain operations.
// When set, each fn invocation will wait for rate limiter permission before executing.
// Pass nil to disable rate limiting.
func (c *Chain[T]) WithRateLimiter(rl *ratelimit.RateLimiter) *Chain[T] {
	c.rateLimiter = rl
	return c
}

// WithRetry configures retry behavior for all chain operations.
// maxRetries: maximum retry attempts (0 = no retry).
// initialBackoff: initial backoff duration before first retry.
// maxBackoff: maximum backoff duration cap.
func (c *Chain[T]) WithRetry(maxRetries int, initialBackoff, maxBackoff time.Duration) *Chain[T] {
	c.retryConfig = &chainRetryConfig{
		MaxRetries:     maxRetries,
		InitialBackoff: initialBackoff,
		MaxBackoff:     maxBackoff,
	}
	return c
}

// Step 5: Add 4 chainWrap*Fn functions
// chainWrapForEachFn wraps a ForEach-style fn (func(ctx, T) error) with rate limiter and retry.
func chainWrapForEachFn[T any](c *Chain[T], fn func(context.Context, T) error) func(context.Context, T) error {
	if c.rateLimiter == nil && c.retryConfig == nil {
		return fn
	}
	return func(ctx context.Context, t T) error {
		if c.rateLimiter != nil {
			if err := c.rateLimiter.Wait(ctx); err != nil {
				return err
			}
		}
		if c.retryConfig != nil {
			return retry.RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
				return fn(ctx, t)
			}, c.retryConfig.MaxRetries, c.retryConfig.InitialBackoff, c.retryConfig.MaxBackoff)
		}
		return fn(ctx, t)
	}
}

// chainWrapMapFn wraps a Map-style fn (func(ctx, T) (R, error)) with rate limiter and retry.
func chainWrapMapFn[T any, R any](c *Chain[T], fn func(context.Context, T) (R, error)) func(context.Context, T) (R, error) {
	if c.rateLimiter == nil && c.retryConfig == nil {
		return fn
	}
	return func(ctx context.Context, t T) (R, error) {
		if c.rateLimiter != nil {
			if err := c.rateLimiter.Wait(ctx); err != nil {
				var zero R
				return zero, err
			}
		}
		if c.retryConfig != nil {
			return retry.RetryWithBackoff(ctx, func(ctx context.Context) (R, error) {
				return fn(ctx, t)
			}, c.retryConfig.MaxRetries, c.retryConfig.InitialBackoff, c.retryConfig.MaxBackoff)
		}
		return fn(ctx, t)
	}
}

// chainWrapChunkForEachFn wraps a chunked ForEach fn (func(ctx, []T) error) with rate limiter and retry.
func chainWrapChunkForEachFn[T any](c *Chain[T], fn func(context.Context, []T) error) func(context.Context, []T) error {
	if c.rateLimiter == nil && c.retryConfig == nil {
		return fn
	}
	return func(ctx context.Context, chunk []T) error {
		if c.rateLimiter != nil {
			if err := c.rateLimiter.Wait(ctx); err != nil {
				return err
			}
		}
		if c.retryConfig != nil {
			return retry.RetryWithBackoffVoid(ctx, func(ctx context.Context) error {
				return fn(ctx, chunk)
			}, c.retryConfig.MaxRetries, c.retryConfig.InitialBackoff, c.retryConfig.MaxBackoff)
		}
		return fn(ctx, chunk)
	}
}

// chainWrapChunkMapFn wraps a chunked Map fn (func(ctx, []T) (R, error)) with rate limiter and retry.
func chainWrapChunkMapFn[T any, R any](c *Chain[T], fn func(context.Context, []T) (R, error)) func(context.Context, []T) (R, error) {
	if c.rateLimiter == nil && c.retryConfig == nil {
		return fn
	}
	return func(ctx context.Context, chunk []T) (R, error) {
		if c.rateLimiter != nil {
			if err := c.rateLimiter.Wait(ctx); err != nil {
				var zero R
				return zero, err
			}
		}
		if c.retryConfig != nil {
			return retry.RetryWithBackoff(ctx, func(ctx context.Context) (R, error) {
				return fn(ctx, chunk)
			}, c.retryConfig.MaxRetries, c.retryConfig.InitialBackoff, c.retryConfig.MaxBackoff)
		}
		return fn(ctx, chunk)
	}
}

// ForEach 骞跺彂閬嶅巻姣忎釜鍏冪礌骞舵墽琛屽壇浣滅敤鎿嶄綔锛岃繑鍥炲綋鍓嶉摼锛堢被鍨嬪拰鍏冪礌涓嶅彉锛夈€?
// concurrency <= 0 鏃朵娇鐢ㄩ摼鐨勯粯璁ゅ苟鍙戝害銆?
// 鎿嶄綔瀹屾垚鍚庨樆濉炵瓑寰呮墍鏈変换鍔＄粨鏉熴€?
// 鏃犺鍐呴儴鏄惁澶辫触锛岄摼鐨勫厓绱犱繚鎸佷笉鍙橈紝鍙€氳繃 Error() 妫€鏌ラ敊璇€?
func (c *Chain[T]) ForEach(concurrency int, fn func(context.Context, T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	fn = chainWrapForEachFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	if c.failFast {
		nr, ffCtx := NewNoResult(conc).WithFFSubmitTO(c.ctx, 0)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for i := range c.items {
			idx := i
			nr.Go(ffCtx, func(ctx context.Context) error {
				return fn(ctx, c.items[idx])
			})
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	} else {
		nr := NewNoResult(conc)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for i := range c.items {
			idx := i
			nr.Go(c.ctx, func(ctx context.Context) error {
				return fn(ctx, c.items[idx])
			})
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	}
	return c
}

// DefaultForEach 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛?ForEach銆?
func (c *Chain[T]) DefaultForEach(fn func(context.Context, T) error) *Chain[T] {
	return c.ForEach(0, fn)
}

// Execute 骞跺彂澶勭悊姣忎釜鍏冪礌骞跺師鍦版浛鎹负鏂板€硷紙鍚岀被鍨嬪彉鎹級銆?
// 搴曞眰浣跨敤 Group 鎵ц锛屾敮鎸侀敊璇仛鍚堛€?
// 闈?FailFast 妯″紡涓嬶紝澶勭悊澶辫触鐨勫厓绱犱細琚涪寮冦€?
//
// 浣跨敤绀轰緥锛?
//
//	c := async.ChainSlice(ctx, items)
//	c.Execute(4, func(ctx context.Context, item MyType) (MyType, error) {
//	    return processItem(ctx, item)
//	})
func (c *Chain[T]) Execute(concurrency int, fn func(context.Context, T) (T, error)) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	fn = chainWrapMapFn(c, fn)

	results, _ := ExecuteWithGroup(c.ctx, c.items, fn, concurrency)
	values := mapreduce.ResultValues(results)
	c.items = values
	return c
}

// DefaultExecute 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛?Execute銆?
func (c *Chain[T]) DefaultExecute(fn func(context.Context, T) (T, error)) *Chain[T] {
	return c.Execute(0, fn)
}

// ForEachPool 浣跨敤鍗忕▼姹犲苟鍙戦亶鍘嗘瘡涓厓绱狅紙浠呭壇浣滅敤锛夈€?
// 搴曞眰浣跨敤 Pool 鑰岄潪 NoResult锛岄€傚悎瀵瑰崗绋嬬敓鍛藉懆鏈熸湁绮剧粏鎺у埗闇€姹傜殑鍦烘櫙銆?
func (c *Chain[T]) ForEachPool(concurrency int, fn func(context.Context, T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	fn = chainWrapForEachFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	ForEachPool(c.ctx, c.items, fn, conc)
	return c
}

// DefaultForEachPool 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛?ForEachPool銆?
func (c *Chain[T]) DefaultForEachPool(fn func(context.Context, T) error) *Chain[T] {
	return c.ForEachPool(0, fn)
}

// ForEachSerial 涓茶閬嶅巻姣忎釜鍏冪礌骞舵墽琛屽壇浣滅敤鎿嶄綔銆?
func (c *Chain[T]) ForEachSerial(fn func(context.Context, T) error) *Chain[T] {
	if len(c.items) == 0 {
		return c
	}

	fn = chainWrapForEachFn(c, fn)

	for i := range c.items {
		idx := i
		if err := fn(c.ctx, c.items[idx]); err != nil && c.firstErr == nil {
			c.firstErr = err
			if c.failFast {
				break
			}
		}
	}
	return c
}

// ForEachChunk 鍒嗗潡骞跺彂閬嶅巻锛宖n 鎺ユ敹鏁翠釜 chunk銆?
// concurrency <= 0 鏃朵娇鐢ㄩ摼鐨勯粯璁ゅ苟鍙戝害銆?
// 閫傚悎鎵归噺鎿嶄綔濡傛壒閲忓啓鍏ユ暟鎹簱銆?
//
// 浣跨敤绀轰緥锛?
//
//	c.ForEachChunk(4, 50, func(ctx context.Context, batch []T) error {
//	    return db.BatchInsert(ctx, batch)
//	})
func (c *Chain[T]) ForEachChunk(concurrency int, batchSize int, fn func(context.Context, []T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	fn = chainWrapChunkForEachFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	chunks := Chunk(c.items, batchSize)
	if c.failFast {
		nr, ffCtx := NewNoResult(conc).WithFFSubmitTO(c.ctx, 0)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for i := range chunks {
			idx := i
			nr.Go(ffCtx, func(ctx context.Context) error {
				return fn(ctx, chunks[idx])
			})
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	} else {
		nr := NewNoResult(conc)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for i := range chunks {
			idx := i
			nr.Go(c.ctx, func(ctx context.Context) error {
				return fn(ctx, chunks[idx])
			})
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	}
	return c
}

// ForEachChunked 鍒嗗潡骞跺彂 ForEach锛宖n 鎺ユ敹鍗曚釜鍏冪礌锛堝唴閮ㄨ嚜鍔ㄥ垎鍧楃鐞嗭級銆?
func (c *Chain[T]) ForEachChunked(concurrency int, batchSize int, fn func(context.Context, T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	fn = chainWrapForEachFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	if batchSize <= 0 {
		batchSize = len(c.items)
	}
	chunks := Chunk(c.items, batchSize)

	if c.failFast {
		nr, ffCtx := NewNoResult(conc).WithFFSubmitTO(c.ctx, 0)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for ci := range chunks {
			chunkIdx := ci
			for i := range chunks[chunkIdx] {
				ei := i
				nr.Go(ffCtx, func(ctx context.Context) error {
					return fn(ctx, chunks[chunkIdx][ei])
				})
			}
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	} else {
		nr := NewNoResult(conc)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for ci := range chunks {
			chunkIdx := ci
			for i := range chunks[chunkIdx] {
				ei := i
				nr.Go(c.ctx, func(ctx context.Context) error {
					return fn(ctx, chunks[chunkIdx][ei])
				})
			}
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	}
	return c
}

// DefaultForEachChunk 浣跨敤閾剧殑榛樿骞跺彂搴﹀垎鍧?ForEach锛坒n 鎺ユ敹 chunk锛夈€?
func (c *Chain[T]) DefaultForEachChunk(batchSize int, fn func(context.Context, []T) error) *Chain[T] {
	return c.ForEachChunk(0, batchSize, fn)
}

// DefaultForEachChunked 浣跨敤閾剧殑榛樿骞跺彂搴﹀垎鍧?ForEach锛坒n 鎺ユ敹鍗曚釜鍏冪礌锛夈€?
func (c *Chain[T]) DefaultForEachChunked(batchSize int, fn func(context.Context, T) error) *Chain[T] {
	return c.ForEachChunked(0, batchSize, fn)
}

// ForEachStream 娴佸紡骞跺彂閬嶅巻姣忎釜鍏冪礌锛岄€氳繃 channel 杈规墽琛岃竟杩斿洖閿欒缁撴灉銆?
// 閫傚悎鍗冧竾绾ф暟鎹殑瀹炴椂澶勭悊锛屽唴瀛樻晥鐜囬珮銆?
// bufSize <= 0 鏃惰嚜閫傚簲璁＄畻锛堝彇 len(items)/concurrency 鍜?256 鐨勮緝灏忓€硷紝鏈€灏?64锛夈€?
//
// concurrency <= 0 鏃朵娇鐢ㄩ摼鐨勯粯璁ゅ苟鍙戝害銆?
// 鎿嶄綔瀹屾垚鍚庨樆濉炵瓑寰呮墍鏈変换鍔＄粨鏉熴€?
// 鏃犺鍐呴儴鏄惁澶辫触锛岄摼鐨勫厓绱犱繚鎸佷笉鍙橈紝鍙€氳繃 Error() 妫€鏌ラ敊璇€?
//
// 绀轰緥锛?
//
//	c.ChainSlice(ctx, hugeItems).
//	    WithConcurrency(64).
//	    ForEachStream(0, 1024, func(ctx context.Context, item MyType) error {
//	        return saveToDB(ctx, item)
//	    })
func (c *Chain[T]) ForEachStream(concurrency int, bufSize int, fn func(context.Context, T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	fn = chainWrapForEachFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}

	var ch <-chan Result[struct{}]
	if c.failFast {
		ch = ForEachStreamWithFailFast(c.ctx, c.items, conc, fn, bufSize)
	} else {
		ch = ForEachStream(c.ctx, c.items, conc, fn, bufSize)
	}

	for r := range ch {
		if r.Err != nil && c.firstErr == nil {
			c.firstErr = r.Err
		}
	}
	return c
}

// DefaultForEachStream 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛屾祦寮?ForEach銆?
func (c *Chain[T]) DefaultForEachStream(bufSize int, fn func(context.Context, T) error) *Chain[T] {
	return c.ForEachStream(0, bufSize, fn)
}

// ForEachSharded 浣跨敤姘村钩鍒嗙墖鎵ц ForEach锛屽皢浠诲姟鍒嗗彂鍒?N 涓?Group 瀹炰緥骞惰鎵ц銆?
// concurrency <= 0 鏃朵娇鐢ㄩ摼鐨勯粯璁ゅ苟鍙戝害銆俿hards <= 0 鏃剁敱 DefaultShardCount 鑷姩鍐冲畾銆?
// 閫傚悎鏋侀檺楂樺苟鍙戝満鏅紙鐧句竾~鍗冧竾 QPS锛夛紝閫氳繃鍒嗙墖闄嶄綆閿佺珵浜夈€?
// 鏃犺鍐呴儴鏄惁澶辫触锛岄摼鐨勫厓绱犱繚鎸佷笉鍙橈紝鍙€氳繃 Error() 妫€鏌ラ敊璇€?
func (c *Chain[T]) ForEachSharded(concurrency int, shards int, fn func(context.Context, T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	fn = chainWrapForEachFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	s := shards
	if s <= 0 {
		s = c.shards
	}

	total, _, firstErr, _ := ForEachSharded(c.ctx, c.items, conc, fn, s)
	if firstErr != nil && c.firstErr == nil {
		c.firstErr = firstErr
	}
	_ = total
	return c
}

// DefaultForEachSharded 浣跨敤閾剧殑榛樿骞跺彂搴﹀拰鍒嗙墖鏁版墽琛?ForEachSharded銆?
func (c *Chain[T]) DefaultForEachSharded(fn func(context.Context, T) error) *Chain[T] {
	return c.ForEachSharded(0, 0, fn)
}

// Filter 鍚屾杩囨护鍏冪礌锛屼繚鐣欐弧瓒虫潯浠剁殑鍏冪礌銆?
// 姝ゆ搷浣滀笉娑夊強骞跺彂锛屾槸绾唴瀛樻搷浣溿€?
func (c *Chain[T]) Filter(fn func(T) bool) *Chain[T] {
	if len(c.items) == 0 {
		return c
	}
	filtered := make([]T, 0, len(c.items))
	for _, item := range c.items {
		if fn(item) {
			filtered = append(filtered, item)
		}
	}
	c.items = filtered
	return c
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€ 绫诲瀷鍖呰鐙珛鍑芥暟 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
//
// 鐢变簬 Go 缂栬瘧鍣ㄥ杩斿洖 Chain[[]T] 鐨勬柟娉曞瓨鍦ㄥ疄渚嬪寲寰幆闄愬埗锛?
// Chunk/ChunkN 涔熶互鐙珛鍑芥暟褰㈠紡鎻愪緵銆?

// ChainChunk 灏嗗厓绱犲垎鍓叉垚鍥哄畾澶у皬鐨勬壒娆★紝杩斿洖 *Chain[[]T]銆?
// 閫傚悎鍚庣画瀵规瘡涓壒娆″仛鎵归噺鎿嶄綔銆?
//
// 浣跨敤绀轰緥锛?
//
//	c := async.ChainSlice(ctx, items)
//	c2 := async.ChainChunk(c, 100)
//	result := async.ChainMap(c2, 4, batchProcessFn).Values()
func ChainChunk[T any](c *Chain[T], size int) *Chain[[]T] {
	chunks := mapreduce.Chunk(c.items, size)
	return &Chain[[]T]{
		ctx: c.ctx, items: chunks,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
		firstErr: c.firstErr,
	}
}

// ChainChunkN 灏嗗厓绱犲潎鍖€鍒嗗壊鎴?n 涓壒娆★紝杩斿洖 *Chain[[]T]銆?
func ChainChunkN[T any](c *Chain[T], n int) *Chain[[]T] {
	chunks := mapreduce.ChunkN(c.items, n)
	return &Chain[[]T]{
		ctx: c.ctx, items: chunks,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
		firstErr: c.firstErr,
	}
}

// ChainDefaultChunk 浣跨敤榛樿鍒嗗潡澶у皬锛?00锛夎繘琛屽垎鍧椼€?
func ChainDefaultChunk[T any](c *Chain[T]) *Chain[[]T] {
	return ChainChunk(c, defaultBatchSize)
}

// ChainDefaultChunkN 浣跨敤閾剧殑骞跺彂搴︿綔涓哄垎鍧楁暟杩涜鍧囧垎銆?
func ChainDefaultChunkN[T any](c *Chain[T]) *Chain[[]T] {
	return ChainChunkN(c, c.concurrency)
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€ 缁堢鎿嶄綔 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

// Split 鎸夎皳璇嶅皢鍏冪礌鎷嗗垎涓轰袱缁勶紝鏄粓绔搷浣溿€?
// 杩斿洖 (matched, unmatched)锛氭弧瓒虫潯浠剁殑鍏冪礌鍜屼笉婊¤冻鐨勩€?
//
// 浣跨敤绀轰緥锛?
//
//	matched, unmatched := c.Split(func(n int) bool { return n > 0 })
func (c *Chain[T]) Split(pred func(T) bool) (matched []T, unmatched []T) {
	return Split(c.items, pred)
}

// Values 杩斿洖閾句腑褰撳墠鐨勫厓绱犲垏鐗囷紝鏄粓绔搷浣溿€?
func (c *Chain[T]) Values() []T {
	return c.items
}

// Result 灏嗗綋鍓嶉摼鍏冪礌鍖呰涓?[]Result[T]锛屾墍鏈夌粨鏋滄爣璁颁负鎴愬姛銆?
func (c *Chain[T]) Result() []Result[T] {
	results := make([]Result[T], len(c.items))
	for i, item := range c.items {
		results[i] = Result[T]{Value: item}
	}
	return results
}

// First 杩斿洖绗竴涓厓绱犲拰鏄惁瀛樺湪銆?
func (c *Chain[T]) First() (T, bool) {
	if len(c.items) == 0 {
		var zero T
		return zero, false
	}
	return c.items[0], true
}

// Last 杩斿洖鏈€鍚庝竴涓厓绱犲拰鏄惁瀛樺湪銆?
func (c *Chain[T]) Last() (T, bool) {
	if len(c.items) == 0 {
		var zero T
		return zero, false
	}
	return c.items[len(c.items)-1], true
}

// Error 杩斿洖閾句腑閬囧埌鐨勭涓€涓敊璇€?
func (c *Chain[T]) Error() error {
	return c.firstErr
}

// Len 杩斿洖褰撳墠鍏冪礌涓暟銆?
func (c *Chain[T]) Len() int {
	return len(c.items)
}

// IsEmpty 杩斿洖閾炬槸鍚︿负绌恒€?
func (c *Chain[T]) IsEmpty() bool {
	return len(c.items) == 0
}

// Context 杩斿洖閾惧綋鍓嶇殑涓婁笅鏂囥€?
func (c *Chain[T]) Context() context.Context {
	return c.ctx
}

// Items 杩斿洖閾惧綋鍓嶅厓绱犵殑鍓湰锛堜笉褰卞搷鍘熼摼锛夈€?
func (c *Chain[T]) Items() []T {
	result := make([]T, len(c.items))
	copy(result, c.items)
	return result
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€ 绫诲瀷鍙樻崲鐙珛鍑芥暟 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
//
// 鐢变簬 Go 娉涘瀷闄愬埗锛屾柟娉曚笉鑳藉紩鍏ユ柊鐨勭被鍨嬪弬鏁帮紝鍥犳 Map銆丗latMap銆丷educe 绛?
// 鏀瑰彉鍏冪礌绫诲瀷鐨勬搷浣滃繀椤讳互鐙珛鍑芥暟褰㈠紡鎻愪緵銆?

// ChainMap 骞跺彂鏄犲皠姣忎釜鍏冪礌鍒版柊绫诲瀷锛岃繑鍥炴柊绫诲瀷鐨勯摼銆?
// concurrency <= 0 鏃朵娇鐢ㄩ摼鐨勯粯璁ゅ苟鍙戝害銆?
// 闈?FailFast 妯″紡涓嬶紝澶辫触鐨勫厓绱犱細琚涪寮冿紝鍙繚鐣欐垚鍔熷€笺€?
// FailFast 妯″紡涓嬶紝棣栦釜閿欒浼氬鑷村悗缁搷浣滃叏閮ㄨ烦杩囥€?
//
// 浣跨敤绀轰緥锛?
//
//	c := async.ChainSlice(ctx, []int{1, 2, 3})
//	c2 := async.ChainMap(c, 4, func(ctx context.Context, n int) (string, error) {
//	    return fmt.Sprintf("val-%d", n), nil
//	})
//	result := c2.Values() // ["val-1", "val-2", "val-3"]
func ChainMap[T any, R any](c *Chain[T], concurrency int, fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}

	fn = chainWrapMapFn(c, fn)

	fn = chainWrapMapFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	var results []core.Result[R]
	var ffErr error

	switch {
	case c.timeout > 0 && c.failFast:
		results, ffErr = mapreduce.MapWithFFTimeout(c.ctx, c.items, conc, c.timeout, fn)
	case c.timeout > 0:
		results = mapreduce.MapWithTimeout(c.ctx, c.items, conc, c.timeout, fn)
	case c.failFast:
		results, ffErr = mapreduce.MapWithFailFast(c.ctx, c.items, fn, conc)
	default:
		results, _ = mapreduce.Map(c.ctx, c.items, fn, conc)
	}

	values := mapreduce.ResultValues(results)
	nc := &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
	}
	if ffErr != nil {
		nc.firstErr = ffErr
	}
	return nc
}

// ChainDefaultMap 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛?Map銆?
func ChainDefaultMap[T any, R any](c *Chain[T], fn func(context.Context, T) (R, error)) *Chain[R] {
	return ChainMap(c, 0, fn)
}

// ChainMapSerial 涓茶鏄犲皠姣忎釜鍏冪礌鍒版柊绫诲瀷锛岃繑鍥炴柊绫诲瀷鐨勯摼銆?
// 涓嶄娇鐢ㄥ苟鍙戯紝鎸夐『搴忛€愪釜澶勭悊銆?
// FailFast 妯″紡涓嬶紝棣栦釜閿欒浼氱珛鍗崇粓姝€?
//
// 浣跨敤绀轰緥锛?
//
//	c := async.ChainSlice(ctx, []int{1, 2, 3})
//	c2 := async.ChainMapSerial(c, func(ctx context.Context, n int) (string, error) {
//	    return fmt.Sprintf("val-%d", n), nil
//	})
func ChainMapSerial[T any, R any](c *Chain[T], fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}

	fn = chainWrapMapFn(c, fn)

	var values []R
	var firstErr error

	for _, item := range c.items {
		v, err := fn(c.ctx, item)
		if err != nil {
			if c.failFast {
				firstErr = err
				break
			}
			continue
		}
		values = append(values, v)
	}

	return &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
		firstErr: firstErr,
	}
}

// ChainFlatMap 骞跺彂鏄犲皠姣忎釜鍏冪礌鍒板垏鐗囧苟灞曞钩銆?
// 绛変环浜?Map 杩斿洖 []R 鍚庡啀 flatten銆?
//
// 浣跨敤绀轰緥锛?
//
//	c := async.ChainSlice(ctx, []string{"a b", "c d"})
//	c2 := async.ChainFlatMap(c, 4, func(ctx context.Context, s string) ([]string, error) {
//	    return strings.Split(s, " "), nil
//	})
//	result := c2.Values() // ["a", "b", "c", "d"]
func ChainFlatMap[T any, R any](c *Chain[T], concurrency int, fn func(context.Context, T) ([]R, error)) *Chain[R] {
	mapped := ChainMap(c, concurrency, fn)
	var flat []R
	for _, slice := range mapped.items {
		flat = append(flat, slice...)
	}
	return &Chain[R]{
		ctx: c.ctx, items: flat,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
		firstErr: mapped.firstErr,
	}
}

// ChainDefaultFlatMap 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛?FlatMap銆?
func ChainDefaultFlatMap[T any, R any](c *Chain[T], fn func(context.Context, T) ([]R, error)) *Chain[R] {
	return ChainFlatMap(c, 0, fn)
}

// ChainReduce 瀵瑰綋鍓嶉摼鐨勫厓绱犺繘琛岃仛鍚堬紝鏄粓绔搷浣溿€?
//
// 浣跨敤绀轰緥锛?
//
//	sum := async.ChainReduce(
//	    async.ChainMap(async.ChainSlice(ctx, []int{1,2,3}), 4, doubleFn),
//	    0, func(acc, n int) int { return acc + n })
func ChainReduce[T any, R any](c *Chain[T], initial R, fn func(R, T) R) R {
	acc := initial
	for _, item := range c.items {
		acc = fn(acc, item)
	}
	return acc
}

// ChainMapReduce 鍏堝苟鍙?Map锛堣浆鎹㈠厓绱犵被鍨嬶級鍐嶈仛鍚堬紝鏄粓绔搷浣溿€?
// 杩斿洖鑱氬悎缁撴灉鍜屽彲鑳界殑閿欒銆?
//
// 浣跨敤绀轰緥锛?
//
//	total, err := async.ChainMapReduce(
//	    async.ChainSlice(ctx, []int{1,2,3,4,5}),
//	    4,
//	    func(ctx context.Context, n int) (int, error) { return n * n, nil },
//	    0,
//	    func(acc, n int) int { return acc + n },
//	)
func ChainMapReduce[T any, R any](c *Chain[T], concurrency int, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	if c.failFast && c.firstErr != nil {
		return initial, c.firstErr
	}
	if len(c.items) == 0 {
		return initial, nil
	}

	mapFn = chainWrapMapFn(c, mapFn)

	mapFn = chainWrapMapFn(c, mapFn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	switch {
	case c.timeout > 0 && c.failFast:
		return ReduceWithFFTimeout(c.ctx, c.items, conc, c.timeout, mapFn, initial, reduceFn)
	case c.timeout > 0:
		return ReduceWithTimeout(c.ctx, c.items, conc, c.timeout, mapFn, initial, reduceFn)
	case c.failFast:
		return ReduceWithFailFast(c.ctx, c.items, conc, mapFn, initial, reduceFn)
	default:
		return Reduce(c.ctx, c.items, conc, mapFn, initial, reduceFn)
	}
}

// ChainDefaultMapReduce 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛?MapReduce銆?
func ChainDefaultMapReduce[T any, R any](c *Chain[T], mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return ChainMapReduce(c, 0, mapFn, initial, reduceFn)
}

// ChainMapChunk 鍏堝垎鍧楀啀骞跺彂 Map锛屽鐞嗗嚱鏁版帴鏀舵暣涓?chunk銆?
// 閫傚悎鎵归噺鎿嶄綔濡傛暟鎹簱鎵归噺鎻掑叆銆?
//
// 浣跨敤绀轰緥锛?
//
//	c2 := async.ChainMapChunk(async.ChainSlice(ctx, records), 4, 100,
//	    func(ctx context.Context, batch []Record) (int, error) {
//	        return db.BatchInsert(ctx, batch)
//	    })
func ChainMapChunk[T any, R any](c *Chain[T], concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}

	fn = chainWrapChunkMapFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	var results []core.Result[R]
	var ffErr error

	switch {
	case c.timeout > 0 && c.failFast:
		results, ffErr = MapChunkWithFFTimeout(c.ctx, c.items, conc, batchSize, c.timeout, fn)
	case c.timeout > 0:
		results = MapChunkWithTimeout(c.ctx, c.items, conc, batchSize, c.timeout, fn)
	case c.failFast:
		results, ffErr = MapChunkWithFailFast(c.ctx, c.items, conc, batchSize, fn)
	default:
		results = MapChunk(c.ctx, c.items, conc, batchSize, fn)
	}

	values := mapreduce.ResultValues(results)
	nc := &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
	}
	if ffErr != nil {
		nc.firstErr = ffErr
	}
	return nc
}

// ChainMapChunked 鍒嗗潡鍚庡苟鍙?Map锛宖n 鎺ユ敹鍗曚釜鍏冪礌锛堝唴閮ㄨ嚜鍔ㄥ垎鍧楃鐞嗭級銆?
func ChainMapChunked[T any, R any](c *Chain[T], concurrency int, batchSize int, fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}

	fn = chainWrapMapFn(c, fn)
	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	var results []core.Result[R]
	var ffErr error

	switch {
	case c.timeout > 0 && c.failFast:
		results, ffErr = MapChunkedWithFFTimeout(c.ctx, c.items, conc, batchSize, c.timeout, fn)
	case c.timeout > 0:
		results = MapChunkedWithTimeout(c.ctx, c.items, conc, batchSize, c.timeout, fn)
	case c.failFast:
		results, ffErr = MapChunkedWithFailFast(c.ctx, c.items, conc, batchSize, fn)
	default:
		results = MapChunked(c.ctx, c.items, conc, batchSize, fn)
	}

	values := mapreduce.ResultValues(results)
	nc := &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
	}
	if ffErr != nil {
		nc.firstErr = ffErr
	}
	return nc
}

// ChainDefaultMapChunk 浣跨敤閾剧殑榛樿骞跺彂搴﹀垎鍧?Map锛坒n 鎺ユ敹 chunk锛夈€?
func ChainDefaultMapChunk[T any, R any](c *Chain[T], batchSize int, fn func(context.Context, []T) (R, error)) *Chain[R] {
	return ChainMapChunk(c, 0, batchSize, fn)
}

// ChainDefaultMapChunked 浣跨敤閾剧殑榛樿骞跺彂搴﹀垎鍧?Map锛坒n 鎺ユ敹鍗曚釜鍏冪礌锛夈€?
func ChainDefaultMapChunked[T any, R any](c *Chain[T], batchSize int, fn func(context.Context, T) (R, error)) *Chain[R] {
	return ChainMapChunked(c, 0, batchSize, fn)
}

// ChainMapPool 浣跨敤鍗忕▼姹犲苟鍙戞槧灏勬瘡涓厓绱犲埌鏂扮被鍨嬶紝杩斿洖鏂扮被鍨嬬殑閾俱€?
// 搴曞眰浣跨敤 Pool 鑰岄潪 Group锛岄€傚悎瀵瑰崗绋嬬敓鍛藉懆鏈熸湁绮剧粏鎺у埗闇€姹傜殑鍦烘櫙銆?
func ChainMapPool[T any, R any](c *Chain[T], concurrency int, fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}

	fn = chainWrapMapFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}

	ctx := c.ctx
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(c.ctx, c.timeout)
		defer cancel()
	}

	_, results, poolErr := MapPool(ctx, c.items, fn, conc)
	values := mapreduce.ResultValues(results)
	return &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
		firstErr: poolErr,
	}
}

// ChainDefaultMapPool 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛屽崗绋嬫睜 Map銆?
func ChainDefaultMapPool[T any, R any](c *Chain[T], fn func(context.Context, T) (R, error)) *Chain[R] {
	return ChainMapPool(c, 0, fn)
}

// ChainMapSharded 浣跨敤姘村钩鍒嗙墖骞跺彂鏄犲皠姣忎釜鍏冪礌鍒版柊绫诲瀷锛岃繑鍥炴柊绫诲瀷鐨勯摼銆?
// 閫傚悎鏋侀檺楂樺苟鍙戝満鏅紙鐧句竾~鍗冧竾 QPS锛夛紝閫氳繃鍒嗙墖闄嶄綆閿佺珵浜夈€?
// concurrency <= 0 鏃朵娇鐢ㄩ摼鐨勯粯璁ゅ苟鍙戝害銆俿hards <= 0 鏃朵娇鐢ㄩ摼鐨勯粯璁ゅ垎鐗囨暟銆?
// 闈?FailFast 妯″紡涓嬶紝澶辫触鐨勫厓绱犱細琚涪寮冿紝鍙繚鐣欐垚鍔熷€笺€?
// FailFast 妯″紡涓嬶紝棣栦釜閿欒浼氬鑷村悗缁搷浣滃叏閮ㄨ烦杩囥€?
//
// 浣跨敤绀轰緥锛?
//
//	c := async.ChainSlice(ctx, items).WithShards(16)
//	c2 := async.ChainMapSharded(c, 64, 0, processFn)
//	result := c2.Values()
func ChainMapSharded[T any, R any](c *Chain[T], concurrency int, shards int, fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}

	fn = chainWrapMapFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	s := shards
	if s <= 0 {
		s = c.shards
	}

	results, shardErr := MapSharded(c.ctx, c.items, conc, fn, s)
	values := mapreduce.ResultValues(results)
	return &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
		firstErr: shardErr,
	}
}

// ChainDefaultMapSharded 浣跨敤閾剧殑榛樿骞跺彂搴﹀拰鍒嗙墖鏁版墽琛?MapSharded銆?
func ChainDefaultMapSharded[T any, R any](c *Chain[T], fn func(context.Context, T) (R, error)) *Chain[R] {
	return ChainMapSharded(c, 0, 0, fn)
}

// ChainMapStream 娴佸紡骞跺彂鏄犲皠姣忎釜鍏冪礌鍒版柊绫诲瀷锛岄€氳繃 channel 杈规墽琛岃竟杩斿洖缁撴灉銆?
// 閫傚悎鍗冧竾绾ф暟鎹殑瀹炴椂澶勭悊锛屽唴瀛樻晥鐜囬珮銆?
// bufSize <= 0 鏃惰嚜閫傚簲璁＄畻锛堝彇 len(items)/concurrency 鍜?256 鐨勮緝灏忓€硷紝鏈€灏?64锛夈€?
//
// concurrency <= 0 鏃朵娇鐢ㄩ摼鐨勯粯璁ゅ苟鍙戝害銆?
// 闈?FailFast 妯″紡涓嬶紝澶辫触鐨勫厓绱犱細琚涪寮冿紝鍙繚鐣欐垚鍔熷€笺€?
// FailFast 妯″紡涓嬶紝棣栦釜閿欒浼氬鑷村悗缁搷浣滃叏閮ㄨ烦杩囥€?
//
// 浣跨敤绀轰緥锛?
//
//	c := async.ChainSlice(ctx, hugeItems)
//	c2 := async.ChainMapStream(c, 64, 1024, processFn)
//	result := c2.Values()
func ChainMapStream[T any, R any](c *Chain[T], concurrency int, bufSize int, fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards, rateLimiter: c.rateLimiter, retryConfig: c.retryConfig}
	}

	fn = chainWrapMapFn(c, fn)

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}

	var ch <-chan Result[R]
	if c.failFast {
		ch = MapStreamWithFailFast(c.ctx, c.items, conc, fn, bufSize)
	} else {
		ch = MapStream(c.ctx, c.items, conc, fn, bufSize)
	}

	var values []R
	var firstErr error
	for r := range ch {
		if r.Err != nil {
			if firstErr == nil {
				firstErr = r.Err
			}
			continue
		}
		values = append(values, r.Value)
	}

	return &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast, shards: c.shards,
		rateLimiter: c.rateLimiter, retryConfig: c.retryConfig,
		firstErr: firstErr,
	}
}

// ChainDefaultMapStream 浣跨敤閾剧殑榛樿骞跺彂搴︽墽琛屾祦寮?Map銆?
func ChainDefaultMapStream[T any, R any](c *Chain[T], bufSize int, fn func(context.Context, T) (R, error)) *Chain[R] {
	return ChainMapStream(c, 0, bufSize, fn)
}

// ChainResultValues 浠庨摼涓彁鍙栧€煎苟杩斿洖鏂扮被鍨嬪垏鐗囷紝鏄粓绔搷浣溿€?
// 绛変环浜?c.Values()锛屼絾鎻愪緵鏄惧紡鐨勭被鍨嬭浆鎹㈣涔夈€?
//
// 浣跨敤绀轰緥锛?
//
//	values := async.ChainResultValues(c) // []int
func ChainResultValues[T any](c *Chain[T]) []T {
	return c.items
}

// ChainPool creates a Pool for fn, auto-closes via defer.
// Corresponds to PoolBuilder.Run().
func (c *Chain[T]) ChainPool(concurrency int, fn func(p *Pool[T]) error) *Chain[T] {
	p := NewPool[T](concurrency)
	defer p.Close()
	if err := fn(p); err != nil && c.firstErr == nil {
		c.firstErr = err
	}
	return c
}

// ChainShardedPool creates a ShardedPool, auto-closes.
// Corresponds to ShardPoolBuilder.Run().
func (c *Chain[T]) ChainShardedPool(cfg ShardPoolConfig[T], fn func(sp *ShardedPool[T]) error) *Chain[T] {
	sp := NewShardedPool(cfg)
	defer sp.Close()
	if err := fn(sp); err != nil && c.firstErr == nil {
		c.firstErr = err
	}
	return c
}

// ChainMultiPool creates a MultiPool, auto-closes.
// Corresponds to MultiPoolBuilder.Run().
func (c *Chain[T]) ChainMultiPool(concurrency int, shards int, fn func(mp *MultiPool[T]) error) *Chain[T] {
	p := NewPool[T](concurrency)
	mp := ShardPool(p, shards)
	defer mp.Close()
	if err := fn(mp); err != nil && c.firstErr == nil {
		c.firstErr = err
	}
	return c
}

// ChainMultiGroup creates a MultiGroup, auto-closes.
// Corresponds to MultiGroupBuilder.Run().
func (c *Chain[T]) ChainMultiGroup(concurrency int, shards int, fn func(mg *MultiGroup[T]) error) *Chain[T] {
	g := NewGroup[T](concurrency)
	mg := ShardGroup(g, shards)
	defer mg.Close()
	if err := fn(mg); err != nil && c.firstErr == nil {
		c.firstErr = err
	}
	return c
}

// ChainNoResultPool creates a NoResultPool, auto-closes.
// Corresponds to NoResultPoolBuilder.Run().
func (c *Chain[T]) ChainNoResultPool(size int, fn func(np *NoResultPool) error) *Chain[T] {
	np := NewNoResultPool(size)
	defer np.Close()
	if err := fn(np); err != nil && c.firstErr == nil {
		c.firstErr = err
	}
	return c
}

// ChainRateLimiter creates a RateLimiter, auto-closes.
// Corresponds to RateLimitBuilder.Run().
func (c *Chain[T]) ChainRateLimiter(rate int, perDuration time.Duration, burst int, fn func(rl *ratelimit.RateLimiter) error) *Chain[T] {
	var rl *ratelimit.RateLimiter
	if burst > 0 {
		rl = ratelimit.NewRateLimiterWithBurst(rate, perDuration, burst)
	} else {
		rl = ratelimit.NewRateLimiter(rate, perDuration)
	}
	defer rl.Close()
	if err := fn(rl); err != nil && c.firstErr == nil {
		c.firstErr = err
	}
	return c
}

// ChainTask starts an async task, returns AsyncResult[T].
// Corresponds to TaskBuilder.Run().
func ChainTask[T any](ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T] {
	return GoResult(core.EnsureTraceID(ctx), fn)
}

// ──────────────────────────── From 流式构建器 (纯增量，零侵入) ────────────────────────────
//
// FromBuilder 提供 Map/Slice 双模式流式链式调用，内部完全委托给已有的成熟 API。
// 不修改原有任何代码，100% 向后兼容。
//
// Map 模式（同类型变换 + Sharded 分片）：
//
//	async.From(ctx, items).
//	    Map().Sharded(8).Run(fn).Values()
//
// Slice 模式（ForEach 副作用 + Sharded 分片）：
//
//	async.From(ctx, items).
//	    Slice().Sharded(8).ForEach(fn).Values()
//
// 带完整配置的用法：
//
//	async.From(ctx, items).
//	    WithConcurrency(64).WithFailFast().WithTimeout(30*time.Second).
//	    Map().Sharded(16).Run(fn).Values()
//
// 注意：跨类型变换（T→R）请使用已有的独立函数 ChainMap / ChainMapSharded。
// Run() 仅支持同类型变换 func(context.Context, T) (T, error)。

// fromBuilderMode 区分 Map / Slice 模式，纯语义标记。
type fromBuilderMode int

const (
	fromModeSlice fromBuilderMode = iota
	fromModeMap
)

// FromBuilder 流式链式调用构建器。
// 通过 From() 创建，内部包装 Chain[T]，全部委托给已有 API。
type FromBuilder[T any] struct {
	chain *Chain[T]
	mode  fromBuilderMode
}

// From 创建流式构建器入口，接收 ctx 和 items。
// ctx 自动注入 trace_id。
func From[T any](ctx context.Context, items []T) *FromBuilder[T] {
	return &FromBuilder[T]{
		chain: ChainSlice(ctx, items),
	}
}

// Map 进入 Map 模式，后续可调用 Run() 执行同类型并发变换。
func (fb *FromBuilder[T]) Map() *FromBuilder[T] {
	fb.mode = fromModeMap
	return fb
}

// Slice 进入 Slice 模式，后续可调用 ForEach() 执行并发副作用操作。
func (fb *FromBuilder[T]) Slice() *FromBuilder[T] {
	fb.mode = fromModeSlice
	return fb
}

// Sharded 设置水平分片数，用于 Map/Run 和 Slice/ForEach 的分片并发。
// n <= 0 时使用默认自动分片数。
func (fb *FromBuilder[T]) Sharded(n int) *FromBuilder[T] {
	fb.chain.WithShards(n)
	return fb
}

// DefaultSharded 使用默认水平分片数（自动），等价于 Sharded(0)。
func (fb *FromBuilder[T]) DefaultSharded() *FromBuilder[T] {
	fb.chain.WithShards(0)
	return fb
}

// WithConcurrency 设置后续操作的并发度。
func (fb *FromBuilder[T]) WithConcurrency(n int) *FromBuilder[T] {
	fb.chain.WithConcurrency(n)
	return fb
}

// WithConcurrencyDefault 恢复默认 IO 并发度。
func (fb *FromBuilder[T]) WithConcurrencyDefault() *FromBuilder[T] {
	fb.chain.WithConcurrencyDefault()
	return fb
}

// DefaultWithConcurrency 使用默认 IO 并发度，等价于 WithConcurrencyDefault()。
func (fb *FromBuilder[T]) DefaultWithConcurrency() *FromBuilder[T] {
	fb.chain.WithConcurrencyDefault()
	return fb
}

// WithFailFast 启用 FailFast 模式：任一操作失败立即停止后续。
func (fb *FromBuilder[T]) WithFailFast() *FromBuilder[T] {
	fb.chain.WithFailFast()
	return fb
}

// WithTimeout 设置后续操作的超时时间。
func (fb *FromBuilder[T]) WithTimeout(d time.Duration) *FromBuilder[T] {
	fb.chain.WithTimeout(d)
	return fb
}

// WithTimeoutDefault 恢复默认超时（30s）。
func (fb *FromBuilder[T]) WithTimeoutDefault() *FromBuilder[T] {
	fb.chain.WithTimeoutDefault()
	return fb
}

// DefaultWithTimeout 使用默认超时（30s），等价于 WithTimeoutDefault()。
func (fb *FromBuilder[T]) DefaultWithTimeout() *FromBuilder[T] {
	fb.chain.WithTimeoutDefault()
	return fb
}

// Run 并发执行同类型 Map 变换（使用水平分片）。
// fn 签名：func(context.Context, T) (T, error)
// 内部调用 MapSharded，失败的元素会被丢弃（非 FailFast 模式）或终止链（FailFast 模式）。
// 对于跨类型变换（T→R），请使用独立的 ChainMap / ChainMapSharded 函数。
//
// 示例：
//
//	async.From(ctx, users).
//	    Map().Sharded(16).Run(func(ctx context.Context, u User) (User, error) {
//	        u.Processed = true
//	        return u, processUser(ctx, u)
//	    }).Values()
func (fb *FromBuilder[T]) Run(fn func(context.Context, T) (T, error)) *FromBuilder[T] {
	if fb.chain.failFast && fb.chain.firstErr != nil {
		return fb
	}
	if len(fb.chain.items) == 0 {
		return fb
	}

	results, shardErr := MapSharded(fb.chain.ctx, fb.chain.items, fb.chain.concurrency, fn, fb.chain.shards)
	fb.chain.items = mapreduce.ResultValues(results)
	if shardErr != nil && fb.chain.firstErr == nil {
		fb.chain.firstErr = shardErr
	}
	return fb
}

// ForEach 并发执行副作用操作（使用水平分片）。
// fn 签名：func(context.Context, T) error
// 内部调用 Chain.ForEachSharded，元素保持不变。
// 无论内部是否失败，链元素维持原样，可通过 Error() 检查错误。
//
// 示例：
//
//	async.From(ctx, msgs).
//	    Slice().Sharded(16).ForEach(func(ctx context.Context, m Msg) error {
//	        return sendMsg(ctx, m)
//	    }).Values() // 返回原始 msgs
func (fb *FromBuilder[T]) ForEach(fn func(context.Context, T) error) *FromBuilder[T] {
	fb.chain.ForEachSharded(0, 0, fn)
	return fb
}

// Values 终端操作：返回当前链中的元素切片。
func (fb *FromBuilder[T]) Values() []T {
	return fb.chain.Values()
}

// Error 返回链中遇到的第一个错误。
func (fb *FromBuilder[T]) Error() error {
	return fb.chain.Error()
}

// Chain 获取底层的 Chain[T] 实例，用于需要跨类型变换等高级场景。
//
// 示例：
//
//	c := async.From(ctx, items).Map().Sharded(8)
//	result := async.ChainMap(c.Chain(), 0, func(ctx context.Context, t T) (Other, error) {
//	    return transform(t), nil
//	}).Values()
func (fb *FromBuilder[T]) Chain() *Chain[T] {
	return fb.chain
}

// Len 返回当前元素个数。
func (fb *FromBuilder[T]) Len() int {
	return fb.chain.Len()
}

// IsEmpty 返回链是否为空。
func (fb *FromBuilder[T]) IsEmpty() bool {
	return fb.chain.IsEmpty()
}

// Context 返回链的当前上下文。
func (fb *FromBuilder[T]) Context() context.Context {
	return fb.chain.Context()
}

// Items 返回链当前元素的副本。
func (fb *FromBuilder[T]) Items() []T {
	return fb.chain.Items()
}

// Result 将当前链元素包装为 []Result[T]，所有结果标记为成功。
func (fb *FromBuilder[T]) Result() []Result[T] {
	return fb.chain.Result()
}

// First 返回第一个元素和是否存在的标志。
func (fb *FromBuilder[T]) First() (T, bool) {
	return fb.chain.First()
}

// Last 返回最后一个元素和是否存在的标志。
func (fb *FromBuilder[T]) Last() (T, bool) {
	return fb.chain.Last()
}

// ──────────────────────────── slices 标准库操作方法 ────────────────────────────

// SortFunc 使用自定义比较函数对新链中的元素排序（不稳定排序）。
// cmp 签名：func(a, b T) int，负值表示 a < b，正值表示 a > b，0 表示相等。
// 底层调用 slices.SortFunc。
//
// 示例：
//
//	async.SliceOf(ctx, users).
//	    SortFunc(func(a, b User) int { return a.Age - b.Age }).
//	    Values()
func (fb *FromBuilder[T]) SortFunc(cmp func(a, b T) int) *FromBuilder[T] {
	slices.SortFunc(fb.chain.items, cmp)
	return fb
}

// StableSortFunc 使用自定义比较函数对链中元素进行稳定排序。
// 底层调用 slices.SortStableFunc。
func (fb *FromBuilder[T]) StableSortFunc(cmp func(a, b T) int) *FromBuilder[T] {
	slices.SortStableFunc(fb.chain.items, cmp)
	return fb
}

// IsSortedFunc 检查链是否已按给定比较函数有序。
// 底层调用 slices.IsSortedFunc。
func (fb *FromBuilder[T]) IsSortedFunc(cmp func(a, b T) int) bool {
	return slices.IsSortedFunc(fb.chain.items, cmp)
}

// Reverse 原地反转链中元素的顺序。
// 底层调用 slices.Reverse。
func (fb *FromBuilder[T]) Reverse() *FromBuilder[T] {
	slices.Reverse(fb.chain.items)
	return fb
}

// ContainsFunc 检查链中是否存在满足谓词的元素。
// 底层调用 slices.ContainsFunc。
func (fb *FromBuilder[T]) ContainsFunc(pred func(T) bool) bool {
	return slices.ContainsFunc(fb.chain.items, pred)
}

// IndexFunc 返回第一个满足谓词的元素下标，未找到返回 -1。
// 底层调用 slices.IndexFunc。
func (fb *FromBuilder[T]) IndexFunc(pred func(T) bool) int {
	return slices.IndexFunc(fb.chain.items, pred)
}

// CompactFunc 移除相邻的重复元素（保留第一个），使用自定义相等比较。
// 底层调用 slices.CompactFunc。
//
// 示例：
//
//	async.SliceOf(ctx, items).
//	    CompactFunc(func(a, b int) bool { return a == b }).
//	    Values()
func (fb *FromBuilder[T]) CompactFunc(eq func(a, b T) bool) *FromBuilder[T] {
	fb.chain.items = slices.CompactFunc(fb.chain.items, eq)
	return fb
}

// DedupFunc 移除链中所有重复元素（不限于相邻），使用自定义相等比较。
// 保留每个值第一次出现的位置。
func (fb *FromBuilder[T]) DedupFunc(eq func(a, b T) bool) *FromBuilder[T] {
	seen := make(map[any]struct{}, len(fb.chain.items))
	kept := fb.chain.items[:0]
	for _, v := range fb.chain.items {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			kept = append(kept, v)
		}
	}
	fb.chain.items = kept
	return fb
}

// MaxFunc 返回链中按自定义比较函数 cmp(a, b) int 比较后的最大元素。
// cmp 返回负值表示 a < b，正值表示 a > b，0 表示相等。
// 链为空时 found 返回 false。
// 底层调用 slices.MaxFunc。
func (fb *FromBuilder[T]) MaxFunc(cmp func(a, b T) int) (max T, found bool) {
	if len(fb.chain.items) == 0 {
		return max, false
	}
	return slices.MaxFunc(fb.chain.items, cmp), true
}

// MinFunc 返回链中按自定义比较函数 cmp(a, b) int 比较后的最小元素。
// 底层调用 slices.MinFunc。
func (fb *FromBuilder[T]) MinFunc(cmp func(a, b T) int) (min T, found bool) {
	if len(fb.chain.items) == 0 {
		return min, false
	}
	return slices.MinFunc(fb.chain.items, cmp), true
}

// BinarySearchFunc 在已排序的链中二分查找 target 的插入位置。
// cmp 签名与 SortFunc 一致。未找到时返回应插入的位置。
// 底层调用 slices.BinarySearchFunc。
func (fb *FromBuilder[T]) BinarySearchFunc(target T, cmp func(a, b T) int) (int, bool) {
	return slices.BinarySearchFunc(fb.chain.items, target, cmp)
}

// ──────────────────────────── 通用查询方法 ────────────────────────────

// All 检查链中所有元素是否都满足谓词。
// 空链返回 true。
func (fb *FromBuilder[T]) All(pred func(T) bool) bool {
	for _, v := range fb.chain.items {
		if !pred(v) {
			return false
		}
	}
	return true
}

// Any 检查链中是否存在至少一个元素满足谓词。
// 空链返回 false。
func (fb *FromBuilder[T]) Any(pred func(T) bool) bool {
	for _, v := range fb.chain.items {
		if pred(v) {
			return true
		}
	}
	return false
}

// Count 统计满足谓词的元素个数。
func (fb *FromBuilder[T]) Count(pred func(T) bool) int {
	n := 0
	for _, v := range fb.chain.items {
		if pred(v) {
			n++
		}
	}
	return n
}

// FindFunc 返回第一个满足谓词的元素。
// 未找到时 found 返回 false。
func (fb *FromBuilder[T]) FindFunc(pred func(T) bool) (val T, found bool) {
	idx := slices.IndexFunc(fb.chain.items, pred)
	if idx < 0 {
		return val, false
	}
	return fb.chain.items[idx], true
}

// FindLastFunc 返回最后一个满足谓词的元素。
func (fb *FromBuilder[T]) FindLastFunc(pred func(T) bool) (val T, found bool) {
	for i := len(fb.chain.items) - 1; i >= 0; i-- {
		if pred(fb.chain.items[i]) {
			return fb.chain.items[i], true
		}
	}
	return val, false
}

// ──────────────────────────── 元素增删改方法 ────────────────────────────

// Append 在链末尾追加元素。
func (fb *FromBuilder[T]) Append(items ...T) *FromBuilder[T] {
	fb.chain.items = append(fb.chain.items, items...)
	return fb
}

// Prepend 在链开头插入元素。
func (fb *FromBuilder[T]) Prepend(items ...T) *FromBuilder[T] {
	fb.chain.items = append(items, fb.chain.items...)
	return fb
}

// Insert 在指定下标 idx 处插入元素。idx 越界会 panic（与 slices.Insert 一致）。
// 底层调用 slices.Insert。
func (fb *FromBuilder[T]) Insert(idx int, items ...T) *FromBuilder[T] {
	fb.chain.items = slices.Insert(fb.chain.items, idx, items...)
	return fb
}

// Delete 删除指定下标的元素。idx 越界会 panic（与 slices.Delete 一致）。
// 底层调用 slices.Delete。
func (fb *FromBuilder[T]) Delete(idx int) *FromBuilder[T] {
	fb.chain.items = slices.Delete(fb.chain.items, idx, idx+1)
	return fb
}

// DeleteRange 删除 [i, j) 范围内的元素。
// 底层调用 slices.Delete。
func (fb *FromBuilder[T]) DeleteRange(i, j int) *FromBuilder[T] {
	fb.chain.items = slices.Delete(fb.chain.items, i, j)
	return fb
}

// Replace 用新元素替换 [i, j) 范围内的元素。
// 底层调用 slices.Replace。
func (fb *FromBuilder[T]) Replace(i, j int, items ...T) *FromBuilder[T] {
	fb.chain.items = slices.Replace(fb.chain.items, i, j, items...)
	return fb
}

// ──────────────────────────── 截取与容量方法 ────────────────────────────

// Take 保留前 n 个元素，丢弃后续。n 超出长度时保留全部。
func (fb *FromBuilder[T]) Take(n int) *FromBuilder[T] {
	if n <= 0 {
		fb.chain.items = fb.chain.items[:0]
		return fb
	}
	if n < len(fb.chain.items) {
		fb.chain.items = fb.chain.items[:n]
	}
	return fb
}

// Drop 丢弃前 n 个元素，保留后续。n 超出长度时清空。
func (fb *FromBuilder[T]) Drop(n int) *FromBuilder[T] {
	if n <= 0 {
		return fb
	}
	if n >= len(fb.chain.items) {
		fb.chain.items = fb.chain.items[:0]
		return fb
	}
	fb.chain.items = fb.chain.items[n:]
	return fb
}

// Slice 对内部元素进行切片截取 [i, j)，不越界检查（与 Go slice 语义一致）。
func (fb *FromBuilder[T]) SliceRange(i, j int) *FromBuilder[T] {
	fb.chain.items = fb.chain.items[i:j]
	return fb
}

// Clip 移除切片的未使用容量，使 cap == len。
// 底层调用 slices.Clip。
func (fb *FromBuilder[T]) Clip() *FromBuilder[T] {
	fb.chain.items = slices.Clip(fb.chain.items)
	return fb
}

// Grow 增加切片容量至少 n 个元素。n 为负数时不做操作。
// 底层调用 slices.Grow。
func (fb *FromBuilder[T]) Grow(n int) *FromBuilder[T] {
	if n > 0 {
		fb.chain.items = slices.Grow(fb.chain.items, n)
	}
	return fb
}

// ──────────────────────────── 随机化方法 ────────────────────────────

// Shuffle 使用默认随机源原地打乱元素顺序。
func (fb *FromBuilder[T]) Shuffle() *FromBuilder[T] {
	rand.Shuffle(len(fb.chain.items), func(i, j int) {
		fb.chain.items[i], fb.chain.items[j] = fb.chain.items[j], fb.chain.items[i]
	})
	return fb
}

// Clone 返回链当前元素的浅拷贝切片（等效于 Items()）。
// 底层调用 slices.Clone。
func (fb *FromBuilder[T]) Clone() []T {
	return slices.Clone(fb.chain.items)
}

// Chunk 将元素分割为固定大小的批次，返回 [][]T。
// chunkSize <= 0 或链为空时返回 nil。
// 底层调用 slices.Chunk（Go 1.23+ 返回 iterator，此处自动收集）。
func (fb *FromBuilder[T]) Chunk(chunkSize int) [][]T {
	if chunkSize <= 0 || len(fb.chain.items) == 0 {
		return nil
	}
	var result [][]T
	for chunk := range slices.Chunk(fb.chain.items, chunkSize) {
		result = append(result, chunk)
	}
	return result
}

// Repeat 原地重复链中元素 n 次。
// n <= 0 时清空链。n == 1 时不做操作。
// 底层调用 slices.Repeat。
func (fb *FromBuilder[T]) Repeat(n int) *FromBuilder[T] {
	if n <= 0 {
		fb.chain.items = fb.chain.items[:0]
		return fb
	}
	if n == 1 {
		return fb
	}
	fb.chain.items = slices.Repeat(fb.chain.items, n)
	return fb
}

// ──────────────────────────── 领域 Builder 类型（FromBuilder 包装器） ────────────────────────────
//
// ChainMapBuilder[T]   — Map 领域：侧重数据变换，核心方法 Run()
// ChainSliceBuilder[T] — Slice 领域：侧重副作用/遍历，核心方法 ForEach/ForEachChunk/Execute 等
// ChainArrayBuilder[T] — Array 领域：侧重索引原语，核心方法 At/Set/Swap
//
// 三种类型各有领域专属方法，共享的排序/配置/编辑方法通过包装器保持链式类型。
// 注意：区别于 builder.go 中的 MapBuilder[T,R]（跨类型变换终端构建器），
// 此处的 ChainMapBuilder 仅支持同类型链式变换。

// ChainMapBuilder Map 领域构建器，侧重同类型并发数据变换。
// 通过 MapOf() 创建。
type ChainMapBuilder[T any] struct {
	fb *FromBuilder[T]
}

// ChainSliceBuilder Slice 领域构建器，侧重并发副作用/遍历。
// 通过 SliceOf() 创建。
type ChainSliceBuilder[T any] struct {
	fb *FromBuilder[T]
}

// ChainArrayBuilder Array 领域构建器，侧重索引原语操作。
// 通过 ArrayOf() 创建。
type ChainArrayBuilder[T any] struct {
	fb *FromBuilder[T]
}

// ──────────────────────────── 入口函数 ────────────────────────────

// MapOf 创建 Map 领域构建器，用于同类型并发数据变换 + Sharded 分片。
//
// 示例：
//
//	async.MapOf(ctx, users).
//	    SortFunc(byAge).DefaultSharded().Run(transformFn).Values()
func MapOf[T any](ctx context.Context, items []T) *ChainMapBuilder[T] {
	return &ChainMapBuilder[T]{fb: From(ctx, items).Map()}
}

// SliceOf 创建 Slice 领域构建器，用于并发副作用操作 + Sharded 分片。
//
// 示例：
//
//	async.SliceOf(ctx, msgs).
//	    SortFunc(byPriority).DefaultSharded().ForEach(sendFn).Values()
func SliceOf[T any](ctx context.Context, items []T) *ChainSliceBuilder[T] {
	return &ChainSliceBuilder[T]{fb: From(ctx, items).Slice()}
}

// ArrayOf 从切片/数组创建 Array 领域构建器，侧重索引原语操作。
//
// 示例：
//
//	arr := [3]int{1, 2, 3}
//	async.ArrayOf(ctx, arr[:]).DefaultSharded().Swap(0, 2).Run(fn).Values()
func ArrayOf[T any](ctx context.Context, items []T) *ChainArrayBuilder[T] {
	return &ChainArrayBuilder[T]{fb: From(ctx, items)}
}

// ──────────────────────────── 领域专属方法 ────────────────────────────
//
// ChainMapBuilder 目前无专属方法；Run / ForEach / SortFunc 等全部在三类间共享。

// ──────────────────────────── ChainSliceBuilder 专属 ────────────────────────────

// Execute 并发执行同类型变换，返回新 slice（不使用 Sharded 分片，直接用并发度）。
// fn 签名：func(context.Context, T) (T, error)
func (sb *ChainSliceBuilder[T]) Execute(fn func(context.Context, T) (T, error)) *ChainSliceBuilder[T] {
	sb.fb.chain.Execute(0, fn)
	return sb
}

// ──────────────────────────── ChainArrayBuilder 专属 ────────────────────────────

// At 安全地返回第 idx 个元素。越界返回零值和 false。
func (ab *ChainArrayBuilder[T]) At(idx int) (T, bool) {
	if idx < 0 || idx >= len(ab.fb.chain.items) {
		var zero T
		return zero, false
	}
	return ab.fb.chain.items[idx], true
}

// Set 设置第 idx 个元素的值，返回自身用于链式调用。越界不做操作。
func (ab *ChainArrayBuilder[T]) Set(idx int, val T) *ChainArrayBuilder[T] {
	if idx >= 0 && idx < len(ab.fb.chain.items) {
		ab.fb.chain.items[idx] = val
	}
	return ab
}

// Swap 交换两个位置的元素。下标越界不做操作。
func (ab *ChainArrayBuilder[T]) Swap(i, j int) *ChainArrayBuilder[T] {
	n := len(ab.fb.chain.items)
	if i >= 0 && i < n && j >= 0 && j < n {
		ab.fb.chain.items[i], ab.fb.chain.items[j] = ab.fb.chain.items[j], ab.fb.chain.items[i]
	}
	return ab
}

// ──────────────────────────── 通用执行方法（Map / Slice / Array 共有） ────────────────────────────
//
// 以下 Run / ForEach 系列在三类 Builder 上均可用，内部统一委托到对应 Chain 方法，
// 分片/并发/超时/FailFast 配置由前面链式设置的 FromBuilder 状态统一管理。

// --- Run：同类型并发变换（使用 Sharded 分片） ---

// Run 并发执行同类型 Map 变换（使用水平分片）。
// fn 签名：func(context.Context, T) (T, error)
// 内部调用 MapSharded，失败的元素会被丢弃（非 FailFast 模式）或终止链（FailFast 模式）。
func (mb *ChainMapBuilder[T]) Run(fn func(context.Context, T) (T, error)) *ChainMapBuilder[T] {
	mb.fb.Run(fn)
	return mb
}

func (sb *ChainSliceBuilder[T]) Run(fn func(context.Context, T) (T, error)) *ChainSliceBuilder[T] {
	sb.fb.Run(fn)
	return sb
}

func (ab *ChainArrayBuilder[T]) Run(fn func(context.Context, T) (T, error)) *ChainArrayBuilder[T] {
	ab.fb.Run(fn)
	return ab
}

// --- ForEach：带分片的并发副作用遍历 ---

// ForEach 并发执行副作用操作（使用水平分片）。
// fn 签名：func(context.Context, T) error
func (mb *ChainMapBuilder[T]) ForEach(fn func(context.Context, T) error) *ChainMapBuilder[T] {
	mb.fb.ForEach(fn)
	return mb
}

func (sb *ChainSliceBuilder[T]) ForEach(fn func(context.Context, T) error) *ChainSliceBuilder[T] {
	sb.fb.ForEach(fn)
	return sb
}

func (ab *ChainArrayBuilder[T]) ForEach(fn func(context.Context, T) error) *ChainArrayBuilder[T] {
	ab.fb.ForEach(fn)
	return ab
}

// --- ForEachChunk：分批整体并发副作用 ---

// ForEachChunk 将元素分批，每批作为一个整体并发执行副作用。
// batchSize <= 0 时使用默认批大小。
func (mb *ChainMapBuilder[T]) ForEachChunk(batchSize int, fn func(context.Context, []T) error) *ChainMapBuilder[T] {
	mb.fb.chain.ForEachChunk(0, batchSize, fn)
	return mb
}

func (sb *ChainSliceBuilder[T]) ForEachChunk(batchSize int, fn func(context.Context, []T) error) *ChainSliceBuilder[T] {
	sb.fb.chain.ForEachChunk(0, batchSize, fn)
	return sb
}

func (ab *ChainArrayBuilder[T]) ForEachChunk(batchSize int, fn func(context.Context, []T) error) *ChainArrayBuilder[T] {
	ab.fb.chain.ForEachChunk(0, batchSize, fn)
	return ab
}

// --- ForEachChunked：分批单元素并发副作用 ---

// ForEachChunked 将元素分批，以单个元素粒度并发执行副作用。
// 与 ForEachChunk 的区别：fn 接收单个 T 而非 []T。
func (mb *ChainMapBuilder[T]) ForEachChunked(batchSize int, fn func(context.Context, T) error) *ChainMapBuilder[T] {
	mb.fb.chain.ForEachChunked(0, batchSize, fn)
	return mb
}

func (sb *ChainSliceBuilder[T]) ForEachChunked(batchSize int, fn func(context.Context, T) error) *ChainSliceBuilder[T] {
	sb.fb.chain.ForEachChunked(0, batchSize, fn)
	return sb
}

func (ab *ChainArrayBuilder[T]) ForEachChunked(batchSize int, fn func(context.Context, T) error) *ChainArrayBuilder[T] {
	ab.fb.chain.ForEachChunked(0, batchSize, fn)
	return ab
}

// --- ForEachPool：协程池并发副作用 ---

// ForEachPool 使用协程池并发执行副作用操作。
func (mb *ChainMapBuilder[T]) ForEachPool(fn func(context.Context, T) error) *ChainMapBuilder[T] {
	mb.fb.chain.ForEachPool(0, fn)
	return mb
}

func (sb *ChainSliceBuilder[T]) ForEachPool(fn func(context.Context, T) error) *ChainSliceBuilder[T] {
	sb.fb.chain.ForEachPool(0, fn)
	return sb
}

func (ab *ChainArrayBuilder[T]) ForEachPool(fn func(context.Context, T) error) *ChainArrayBuilder[T] {
	ab.fb.chain.ForEachPool(0, fn)
	return ab
}

// --- ForEachSerial：串行副作用 ---

// ForEachSerial 串行遍历每个元素并执行副作用操作。
func (mb *ChainMapBuilder[T]) ForEachSerial(fn func(context.Context, T) error) *ChainMapBuilder[T] {
	mb.fb.chain.ForEachSerial(fn)
	return mb
}

func (sb *ChainSliceBuilder[T]) ForEachSerial(fn func(context.Context, T) error) *ChainSliceBuilder[T] {
	sb.fb.chain.ForEachSerial(fn)
	return sb
}

func (ab *ChainArrayBuilder[T]) ForEachSerial(fn func(context.Context, T) error) *ChainArrayBuilder[T] {
	ab.fb.chain.ForEachSerial(fn)
	return ab
}

// ──────────────────────────── 共享配置方法包装器 ────────────────────────────

// --- Mode ---

func (mb *ChainMapBuilder[T]) Map() *ChainMapBuilder[T] { mb.fb.Map(); return mb }
func (mb *ChainMapBuilder[T]) Slice() *ChainSliceBuilder[T] {
	mb.fb.Slice()
	return &ChainSliceBuilder[T]{fb: mb.fb}
}
func (sb *ChainSliceBuilder[T]) Map() *ChainMapBuilder[T] {
	sb.fb.Map()
	return &ChainMapBuilder[T]{fb: sb.fb}
}
func (sb *ChainSliceBuilder[T]) Slice() *ChainSliceBuilder[T] { sb.fb.Slice(); return sb }
func (ab *ChainArrayBuilder[T]) Map() *ChainMapBuilder[T] {
	ab.fb.Map()
	return &ChainMapBuilder[T]{fb: ab.fb}
}
func (ab *ChainArrayBuilder[T]) Slice() *ChainSliceBuilder[T] {
	ab.fb.Slice()
	return &ChainSliceBuilder[T]{fb: ab.fb}
}

// --- Sharded ---

func (mb *ChainMapBuilder[T]) Sharded(n int) *ChainMapBuilder[T]     { mb.fb.Sharded(n); return mb }
func (mb *ChainMapBuilder[T]) DefaultSharded() *ChainMapBuilder[T]   { mb.fb.DefaultSharded(); return mb }
func (sb *ChainSliceBuilder[T]) Sharded(n int) *ChainSliceBuilder[T] { sb.fb.Sharded(n); return sb }
func (sb *ChainSliceBuilder[T]) DefaultSharded() *ChainSliceBuilder[T] {
	sb.fb.DefaultSharded()
	return sb
}
func (ab *ChainArrayBuilder[T]) Sharded(n int) *ChainArrayBuilder[T] { ab.fb.Sharded(n); return ab }
func (ab *ChainArrayBuilder[T]) DefaultSharded() *ChainArrayBuilder[T] {
	ab.fb.DefaultSharded()
	return ab
}

// --- Concurrency ---

func (mb *ChainMapBuilder[T]) WithConcurrency(n int) *ChainMapBuilder[T] {
	mb.fb.WithConcurrency(n)
	return mb
}
func (mb *ChainMapBuilder[T]) WithConcurrencyDefault() *ChainMapBuilder[T] {
	mb.fb.WithConcurrencyDefault()
	return mb
}
func (mb *ChainMapBuilder[T]) DefaultWithConcurrency() *ChainMapBuilder[T] {
	mb.fb.DefaultWithConcurrency()
	return mb
}
func (sb *ChainSliceBuilder[T]) WithConcurrency(n int) *ChainSliceBuilder[T] {
	sb.fb.WithConcurrency(n)
	return sb
}
func (sb *ChainSliceBuilder[T]) WithConcurrencyDefault() *ChainSliceBuilder[T] {
	sb.fb.WithConcurrencyDefault()
	return sb
}
func (sb *ChainSliceBuilder[T]) DefaultWithConcurrency() *ChainSliceBuilder[T] {
	sb.fb.DefaultWithConcurrency()
	return sb
}
func (ab *ChainArrayBuilder[T]) WithConcurrency(n int) *ChainArrayBuilder[T] {
	ab.fb.WithConcurrency(n)
	return ab
}
func (ab *ChainArrayBuilder[T]) WithConcurrencyDefault() *ChainArrayBuilder[T] {
	ab.fb.WithConcurrencyDefault()
	return ab
}
func (ab *ChainArrayBuilder[T]) DefaultWithConcurrency() *ChainArrayBuilder[T] {
	ab.fb.DefaultWithConcurrency()
	return ab
}

// --- FailFast ---

func (mb *ChainMapBuilder[T]) WithFailFast() *ChainMapBuilder[T]     { mb.fb.WithFailFast(); return mb }
func (sb *ChainSliceBuilder[T]) WithFailFast() *ChainSliceBuilder[T] { sb.fb.WithFailFast(); return sb }
func (ab *ChainArrayBuilder[T]) WithFailFast() *ChainArrayBuilder[T] { ab.fb.WithFailFast(); return ab }

// --- Timeout ---

func (mb *ChainMapBuilder[T]) WithTimeout(d time.Duration) *ChainMapBuilder[T] {
	mb.fb.WithTimeout(d)
	return mb
}
func (mb *ChainMapBuilder[T]) WithTimeoutDefault() *ChainMapBuilder[T] {
	mb.fb.WithTimeoutDefault()
	return mb
}
func (mb *ChainMapBuilder[T]) DefaultWithTimeout() *ChainMapBuilder[T] {
	mb.fb.DefaultWithTimeout()
	return mb
}
func (sb *ChainSliceBuilder[T]) WithTimeout(d time.Duration) *ChainSliceBuilder[T] {
	sb.fb.WithTimeout(d)
	return sb
}
func (sb *ChainSliceBuilder[T]) WithTimeoutDefault() *ChainSliceBuilder[T] {
	sb.fb.WithTimeoutDefault()
	return sb
}
func (sb *ChainSliceBuilder[T]) DefaultWithTimeout() *ChainSliceBuilder[T] {
	sb.fb.DefaultWithTimeout()
	return sb
}
func (ab *ChainArrayBuilder[T]) WithTimeout(d time.Duration) *ChainArrayBuilder[T] {
	ab.fb.WithTimeout(d)
	return ab
}
func (ab *ChainArrayBuilder[T]) WithTimeoutDefault() *ChainArrayBuilder[T] {
	ab.fb.WithTimeoutDefault()
	return ab
}
func (ab *ChainArrayBuilder[T]) DefaultWithTimeout() *ChainArrayBuilder[T] {
	ab.fb.DefaultWithTimeout()
	return ab
}

// ──────────────────────────── 共享排序/变换方法包装器 ────────────────────────────

// --- Sort ---

func (mb *ChainMapBuilder[T]) SortFunc(cmp func(a, b T) int) *ChainMapBuilder[T] {
	mb.fb.SortFunc(cmp)
	return mb
}
func (mb *ChainMapBuilder[T]) StableSortFunc(cmp func(a, b T) int) *ChainMapBuilder[T] {
	mb.fb.StableSortFunc(cmp)
	return mb
}
func (sb *ChainSliceBuilder[T]) SortFunc(cmp func(a, b T) int) *ChainSliceBuilder[T] {
	sb.fb.SortFunc(cmp)
	return sb
}
func (sb *ChainSliceBuilder[T]) StableSortFunc(cmp func(a, b T) int) *ChainSliceBuilder[T] {
	sb.fb.StableSortFunc(cmp)
	return sb
}
func (ab *ChainArrayBuilder[T]) SortFunc(cmp func(a, b T) int) *ChainArrayBuilder[T] {
	ab.fb.SortFunc(cmp)
	return ab
}
func (ab *ChainArrayBuilder[T]) StableSortFunc(cmp func(a, b T) int) *ChainArrayBuilder[T] {
	ab.fb.StableSortFunc(cmp)
	return ab
}

// --- Reverse / Shuffle ---

func (mb *ChainMapBuilder[T]) Reverse() *ChainMapBuilder[T]     { mb.fb.Reverse(); return mb }
func (mb *ChainMapBuilder[T]) Shuffle() *ChainMapBuilder[T]     { mb.fb.Shuffle(); return mb }
func (sb *ChainSliceBuilder[T]) Reverse() *ChainSliceBuilder[T] { sb.fb.Reverse(); return sb }
func (sb *ChainSliceBuilder[T]) Shuffle() *ChainSliceBuilder[T] { sb.fb.Shuffle(); return sb }
func (ab *ChainArrayBuilder[T]) Reverse() *ChainArrayBuilder[T] { ab.fb.Reverse(); return ab }
func (ab *ChainArrayBuilder[T]) Shuffle() *ChainArrayBuilder[T] { ab.fb.Shuffle(); return ab }

// --- Compact / Dedup ---

func (mb *ChainMapBuilder[T]) CompactFunc(eq func(a, b T) bool) *ChainMapBuilder[T] {
	mb.fb.CompactFunc(eq)
	return mb
}
func (mb *ChainMapBuilder[T]) DedupFunc(eq func(a, b T) bool) *ChainMapBuilder[T] {
	mb.fb.DedupFunc(eq)
	return mb
}
func (sb *ChainSliceBuilder[T]) CompactFunc(eq func(a, b T) bool) *ChainSliceBuilder[T] {
	sb.fb.CompactFunc(eq)
	return sb
}
func (sb *ChainSliceBuilder[T]) DedupFunc(eq func(a, b T) bool) *ChainSliceBuilder[T] {
	sb.fb.DedupFunc(eq)
	return sb
}
func (ab *ChainArrayBuilder[T]) CompactFunc(eq func(a, b T) bool) *ChainArrayBuilder[T] {
	ab.fb.CompactFunc(eq)
	return ab
}
func (ab *ChainArrayBuilder[T]) DedupFunc(eq func(a, b T) bool) *ChainArrayBuilder[T] {
	ab.fb.DedupFunc(eq)
	return ab
}

// ──────────────────────────── 共享编辑方法包装器 ────────────────────────────

// --- Append / Prepend ---

func (mb *ChainMapBuilder[T]) Append(items ...T) *ChainMapBuilder[T] {
	mb.fb.Append(items...)
	return mb
}
func (mb *ChainMapBuilder[T]) Prepend(items ...T) *ChainMapBuilder[T] {
	mb.fb.Prepend(items...)
	return mb
}
func (sb *ChainSliceBuilder[T]) Append(items ...T) *ChainSliceBuilder[T] {
	sb.fb.Append(items...)
	return sb
}
func (sb *ChainSliceBuilder[T]) Prepend(items ...T) *ChainSliceBuilder[T] {
	sb.fb.Prepend(items...)
	return sb
}
func (ab *ChainArrayBuilder[T]) Append(items ...T) *ChainArrayBuilder[T] {
	ab.fb.Append(items...)
	return ab
}
func (ab *ChainArrayBuilder[T]) Prepend(items ...T) *ChainArrayBuilder[T] {
	ab.fb.Prepend(items...)
	return ab
}

// --- Insert / Delete / Replace ---

func (mb *ChainMapBuilder[T]) Insert(idx int, items ...T) *ChainMapBuilder[T] {
	mb.fb.Insert(idx, items...)
	return mb
}
func (mb *ChainMapBuilder[T]) Delete(idx int) *ChainMapBuilder[T] { mb.fb.Delete(idx); return mb }
func (mb *ChainMapBuilder[T]) DeleteRange(i, j int) *ChainMapBuilder[T] {
	mb.fb.DeleteRange(i, j)
	return mb
}
func (mb *ChainMapBuilder[T]) Replace(i, j int, items ...T) *ChainMapBuilder[T] {
	mb.fb.Replace(i, j, items...)
	return mb
}
func (sb *ChainSliceBuilder[T]) Insert(idx int, items ...T) *ChainSliceBuilder[T] {
	sb.fb.Insert(idx, items...)
	return sb
}
func (sb *ChainSliceBuilder[T]) Delete(idx int) *ChainSliceBuilder[T] { sb.fb.Delete(idx); return sb }
func (sb *ChainSliceBuilder[T]) DeleteRange(i, j int) *ChainSliceBuilder[T] {
	sb.fb.DeleteRange(i, j)
	return sb
}
func (sb *ChainSliceBuilder[T]) Replace(i, j int, items ...T) *ChainSliceBuilder[T] {
	sb.fb.Replace(i, j, items...)
	return sb
}
func (ab *ChainArrayBuilder[T]) Insert(idx int, items ...T) *ChainArrayBuilder[T] {
	ab.fb.Insert(idx, items...)
	return ab
}
func (ab *ChainArrayBuilder[T]) Delete(idx int) *ChainArrayBuilder[T] { ab.fb.Delete(idx); return ab }
func (ab *ChainArrayBuilder[T]) DeleteRange(i, j int) *ChainArrayBuilder[T] {
	ab.fb.DeleteRange(i, j)
	return ab
}
func (ab *ChainArrayBuilder[T]) Replace(i, j int, items ...T) *ChainArrayBuilder[T] {
	ab.fb.Replace(i, j, items...)
	return ab
}

// --- Take / Drop / SliceRange ---

func (mb *ChainMapBuilder[T]) Take(n int) *ChainMapBuilder[T] { mb.fb.Take(n); return mb }
func (mb *ChainMapBuilder[T]) Drop(n int) *ChainMapBuilder[T] { mb.fb.Drop(n); return mb }
func (mb *ChainMapBuilder[T]) SliceRange(i, j int) *ChainMapBuilder[T] {
	mb.fb.SliceRange(i, j)
	return mb
}
func (sb *ChainSliceBuilder[T]) Take(n int) *ChainSliceBuilder[T] { sb.fb.Take(n); return sb }
func (sb *ChainSliceBuilder[T]) Drop(n int) *ChainSliceBuilder[T] { sb.fb.Drop(n); return sb }
func (sb *ChainSliceBuilder[T]) SliceRange(i, j int) *ChainSliceBuilder[T] {
	sb.fb.SliceRange(i, j)
	return sb
}
func (ab *ChainArrayBuilder[T]) Take(n int) *ChainArrayBuilder[T] { ab.fb.Take(n); return ab }
func (ab *ChainArrayBuilder[T]) Drop(n int) *ChainArrayBuilder[T] { ab.fb.Drop(n); return ab }
func (ab *ChainArrayBuilder[T]) SliceRange(i, j int) *ChainArrayBuilder[T] {
	ab.fb.SliceRange(i, j)
	return ab
}

// --- Clip / Grow / Repeat ---

func (mb *ChainMapBuilder[T]) Clip() *ChainMapBuilder[T]            { mb.fb.Clip(); return mb }
func (mb *ChainMapBuilder[T]) Grow(n int) *ChainMapBuilder[T]       { mb.fb.Grow(n); return mb }
func (mb *ChainMapBuilder[T]) Repeat(n int) *ChainMapBuilder[T]     { mb.fb.Repeat(n); return mb }
func (sb *ChainSliceBuilder[T]) Clip() *ChainSliceBuilder[T]        { sb.fb.Clip(); return sb }
func (sb *ChainSliceBuilder[T]) Grow(n int) *ChainSliceBuilder[T]   { sb.fb.Grow(n); return sb }
func (sb *ChainSliceBuilder[T]) Repeat(n int) *ChainSliceBuilder[T] { sb.fb.Repeat(n); return sb }
func (ab *ChainArrayBuilder[T]) Clip() *ChainArrayBuilder[T]        { ab.fb.Clip(); return ab }
func (ab *ChainArrayBuilder[T]) Grow(n int) *ChainArrayBuilder[T]   { ab.fb.Grow(n); return ab }
func (ab *ChainArrayBuilder[T]) Repeat(n int) *ChainArrayBuilder[T] { ab.fb.Repeat(n); return ab }

// ──────────────────────────── 终端 & 查询方法（直接委托 FromBuilder） ────────────────────────────

func (mb *ChainMapBuilder[T]) Values() []T                          { return mb.fb.Values() }
func (mb *ChainMapBuilder[T]) Error() error                         { return mb.fb.Error() }
func (mb *ChainMapBuilder[T]) Chain() *Chain[T]                     { return mb.fb.Chain() }
func (mb *ChainMapBuilder[T]) Len() int                             { return mb.fb.Len() }
func (mb *ChainMapBuilder[T]) IsEmpty() bool                        { return mb.fb.IsEmpty() }
func (mb *ChainMapBuilder[T]) Context() context.Context             { return mb.fb.Context() }
func (mb *ChainMapBuilder[T]) Items() []T                           { return mb.fb.Items() }
func (mb *ChainMapBuilder[T]) Result() []Result[T]                  { return mb.fb.Result() }
func (mb *ChainMapBuilder[T]) First() (T, bool)                     { return mb.fb.First() }
func (mb *ChainMapBuilder[T]) Last() (T, bool)                      { return mb.fb.Last() }
func (mb *ChainMapBuilder[T]) ContainsFunc(pred func(T) bool) bool  { return mb.fb.ContainsFunc(pred) }
func (mb *ChainMapBuilder[T]) IndexFunc(pred func(T) bool) int      { return mb.fb.IndexFunc(pred) }
func (mb *ChainMapBuilder[T]) All(pred func(T) bool) bool           { return mb.fb.All(pred) }
func (mb *ChainMapBuilder[T]) Any(pred func(T) bool) bool           { return mb.fb.Any(pred) }
func (mb *ChainMapBuilder[T]) Count(pred func(T) bool) int          { return mb.fb.Count(pred) }
func (mb *ChainMapBuilder[T]) FindFunc(pred func(T) bool) (T, bool) { return mb.fb.FindFunc(pred) }
func (mb *ChainMapBuilder[T]) FindLastFunc(pred func(T) bool) (T, bool) {
	return mb.fb.FindLastFunc(pred)
}
func (mb *ChainMapBuilder[T]) MaxFunc(cmp func(a, b T) int) (T, bool) { return mb.fb.MaxFunc(cmp) }
func (mb *ChainMapBuilder[T]) MinFunc(cmp func(a, b T) int) (T, bool) { return mb.fb.MinFunc(cmp) }
func (mb *ChainMapBuilder[T]) BinarySearchFunc(target T, cmp func(a, b T) int) (int, bool) {
	return mb.fb.BinarySearchFunc(target, cmp)
}
func (mb *ChainMapBuilder[T]) IsSortedFunc(cmp func(a, b T) int) bool { return mb.fb.IsSortedFunc(cmp) }
func (mb *ChainMapBuilder[T]) Clone() []T                             { return mb.fb.Clone() }
func (mb *ChainMapBuilder[T]) Chunk(chunkSize int) [][]T              { return mb.fb.Chunk(chunkSize) }

func (sb *ChainSliceBuilder[T]) Values() []T                          { return sb.fb.Values() }
func (sb *ChainSliceBuilder[T]) Error() error                         { return sb.fb.Error() }
func (sb *ChainSliceBuilder[T]) Chain() *Chain[T]                     { return sb.fb.Chain() }
func (sb *ChainSliceBuilder[T]) Len() int                             { return sb.fb.Len() }
func (sb *ChainSliceBuilder[T]) IsEmpty() bool                        { return sb.fb.IsEmpty() }
func (sb *ChainSliceBuilder[T]) Context() context.Context             { return sb.fb.Context() }
func (sb *ChainSliceBuilder[T]) Items() []T                           { return sb.fb.Items() }
func (sb *ChainSliceBuilder[T]) Result() []Result[T]                  { return sb.fb.Result() }
func (sb *ChainSliceBuilder[T]) First() (T, bool)                     { return sb.fb.First() }
func (sb *ChainSliceBuilder[T]) Last() (T, bool)                      { return sb.fb.Last() }
func (sb *ChainSliceBuilder[T]) ContainsFunc(pred func(T) bool) bool  { return sb.fb.ContainsFunc(pred) }
func (sb *ChainSliceBuilder[T]) IndexFunc(pred func(T) bool) int      { return sb.fb.IndexFunc(pred) }
func (sb *ChainSliceBuilder[T]) All(pred func(T) bool) bool           { return sb.fb.All(pred) }
func (sb *ChainSliceBuilder[T]) Any(pred func(T) bool) bool           { return sb.fb.Any(pred) }
func (sb *ChainSliceBuilder[T]) Count(pred func(T) bool) int          { return sb.fb.Count(pred) }
func (sb *ChainSliceBuilder[T]) FindFunc(pred func(T) bool) (T, bool) { return sb.fb.FindFunc(pred) }
func (sb *ChainSliceBuilder[T]) FindLastFunc(pred func(T) bool) (T, bool) {
	return sb.fb.FindLastFunc(pred)
}
func (sb *ChainSliceBuilder[T]) MaxFunc(cmp func(a, b T) int) (T, bool) { return sb.fb.MaxFunc(cmp) }
func (sb *ChainSliceBuilder[T]) MinFunc(cmp func(a, b T) int) (T, bool) { return sb.fb.MinFunc(cmp) }
func (sb *ChainSliceBuilder[T]) BinarySearchFunc(target T, cmp func(a, b T) int) (int, bool) {
	return sb.fb.BinarySearchFunc(target, cmp)
}
func (sb *ChainSliceBuilder[T]) IsSortedFunc(cmp func(a, b T) int) bool {
	return sb.fb.IsSortedFunc(cmp)
}
func (sb *ChainSliceBuilder[T]) Clone() []T                { return sb.fb.Clone() }
func (sb *ChainSliceBuilder[T]) Chunk(chunkSize int) [][]T { return sb.fb.Chunk(chunkSize) }

func (ab *ChainArrayBuilder[T]) Values() []T                          { return ab.fb.Values() }
func (ab *ChainArrayBuilder[T]) Error() error                         { return ab.fb.Error() }
func (ab *ChainArrayBuilder[T]) Chain() *Chain[T]                     { return ab.fb.Chain() }
func (ab *ChainArrayBuilder[T]) Len() int                             { return ab.fb.Len() }
func (ab *ChainArrayBuilder[T]) IsEmpty() bool                        { return ab.fb.IsEmpty() }
func (ab *ChainArrayBuilder[T]) Context() context.Context             { return ab.fb.Context() }
func (ab *ChainArrayBuilder[T]) Items() []T                           { return ab.fb.Items() }
func (ab *ChainArrayBuilder[T]) Result() []Result[T]                  { return ab.fb.Result() }
func (ab *ChainArrayBuilder[T]) First() (T, bool)                     { return ab.fb.First() }
func (ab *ChainArrayBuilder[T]) Last() (T, bool)                      { return ab.fb.Last() }
func (ab *ChainArrayBuilder[T]) ContainsFunc(pred func(T) bool) bool  { return ab.fb.ContainsFunc(pred) }
func (ab *ChainArrayBuilder[T]) IndexFunc(pred func(T) bool) int      { return ab.fb.IndexFunc(pred) }
func (ab *ChainArrayBuilder[T]) All(pred func(T) bool) bool           { return ab.fb.All(pred) }
func (ab *ChainArrayBuilder[T]) Any(pred func(T) bool) bool           { return ab.fb.Any(pred) }
func (ab *ChainArrayBuilder[T]) Count(pred func(T) bool) int          { return ab.fb.Count(pred) }
func (ab *ChainArrayBuilder[T]) FindFunc(pred func(T) bool) (T, bool) { return ab.fb.FindFunc(pred) }
func (ab *ChainArrayBuilder[T]) FindLastFunc(pred func(T) bool) (T, bool) {
	return ab.fb.FindLastFunc(pred)
}
func (ab *ChainArrayBuilder[T]) MaxFunc(cmp func(a, b T) int) (T, bool) { return ab.fb.MaxFunc(cmp) }
func (ab *ChainArrayBuilder[T]) MinFunc(cmp func(a, b T) int) (T, bool) { return ab.fb.MinFunc(cmp) }
func (ab *ChainArrayBuilder[T]) BinarySearchFunc(target T, cmp func(a, b T) int) (int, bool) {
	return ab.fb.BinarySearchFunc(target, cmp)
}
func (ab *ChainArrayBuilder[T]) IsSortedFunc(cmp func(a, b T) int) bool {
	return ab.fb.IsSortedFunc(cmp)
}
func (ab *ChainArrayBuilder[T]) Clone() []T                { return ab.fb.Clone() }
func (ab *ChainArrayBuilder[T]) Chunk(chunkSize int) [][]T { return ab.fb.Chunk(chunkSize) }
