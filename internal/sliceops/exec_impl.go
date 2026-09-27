// Package sliceops 统一执行器实现。
// 基于 Policy 策略配置，将原有的 50+ 函数变体统一为 Map/Each/Stream/Reduce 四个入口，
// 内部根据 Policy 的策略字段分发到对应的执行路径。
//
// 设计模式：Strategy Pattern —— Policy 决定执行算法，
// execMap/execEach/execStream 是 Template Method，内部根据策略分派到具体实现。
package sliceops

import (
	"context"
	"runtime"
	"sync"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
	"github.com/chichengyu/async/pool"
)

// ──────────────────────────── 统一 Map 执行器 ────────────────────────────

// execMap 根据 Policy 策略执行 Map 操作，是所有 Map 变体的统一入口。
// 策略优先级：Pool > Shard > Chunk > Timeout+FF > Timeout > FF > 并行 > 串行。
func execMap[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) ([]core.Result[R], error) {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if n == 0 {
		return nil, nil
	}

	if p.Pool() != nil || p.IsPoolOwned() {
		return execMapPool[T, R](ctx, items, fn, p)
	}

	if p.isZero() {
		p = DefPar()
	}

	switch {
	case p.shards > 0:
		return mapSharded[T, R](ctx, items, fn, p)
	case p.chunkSize > 0:
		return mapChunked[T, R](ctx, items, fn, p)
	case p.timeout > 0 && p.failFast:
		return mapTOFF[T, R](ctx, items, fn, p)
	case p.timeout > 0:
		return mapTO[T, R](ctx, items, fn, p)
	case p.failFast && p.isParallel():
		return mapImpl[T, R](ctx, items, fn, p.concurrency, true)
	case p.isParallel():
		return mapImpl[T, R](ctx, items, fn, p.concurrency, false)
	default:
		return mapSerialExec[T, R](ctx, items, fn, p.failFast)
	}
}

// execMapPool 使用外部协程池（或自动创建的默认池）执行 Map 操作。
// 用户池（非 Owned）使用后自动 Reset 以便复用。
func execMapPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) ([]core.Result[R], error) {
	var pl *pool.Pool[R]
	if pp := p.Pool(); pp != nil {
		pl, _ = pp.(*pool.Pool[R])
	}
	if pl == nil && p.IsPoolOwned() {
		pl = pool.DefaultPool[R]()
	}
	if pl == nil {
		return execMap(ctx, items, fn, Policy{concurrency: p.concurrency, timeout: p.timeout, failFast: p.failFast})
	}
	owned := p.IsPoolOwned()
	if owned {
		defer pl.Close()
	}

	var pCtx context.Context
	if p.failFast {
		pl, pCtx = pl.WithFailFast(ctx)
	} else {
		pCtx = ctx
	}
	if p.timeout > 0 {
		pl.WithTimeout(p.timeout)
	}
	for _, item := range items {
		it := item
		pl.Submit(pCtx, func(ctx context.Context) (R, error) {
			return fn(ctx, it)
		})
	}
	results := pl.Wait()
	if !owned {
		pl.ResetWait()
	}
	var firstErr error
	for _, r := range results {
		if r.Err != nil && firstErr == nil {
			firstErr = r.Err
		}
	}
	return results, firstErr
}

// execEachPool 使用外部协程池（或自动创建的默认池）执行 ForEach 操作。
// 用户池（非 Owned）使用后自动 Reset 以便复用。
func execEachPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error) {
	var pl *pool.Pool[R]
	if pp := p.Pool(); pp != nil {
		pl, _ = pp.(*pool.Pool[R])
	}
	if pl == nil && p.IsPoolOwned() {
		pl = pool.DefaultPool[R]()
	}
	if pl == nil {
		t, f, fe, _ := execEach(ctx, items, fn, Policy{concurrency: p.concurrency, timeout: p.timeout, failFast: p.failFast})
		return t, f, fe
	}
	owned := p.IsPoolOwned()
	if owned {
		defer pl.Close()
	}

	var pCtx context.Context
	if p.failFast {
		pl, pCtx = pl.WithFailFast(ctx)
	} else {
		pCtx = ctx
	}
	if p.timeout > 0 {
		pl.WithTimeout(p.timeout)
	}
	var zero R
	for _, item := range items {
		it := item
		pl.Submit(pCtx, func(ctx context.Context) (R, error) {
			return zero, fn(ctx, it)
		})
	}
	results := pl.Wait()
	if !owned {
		pl.ResetWait()
	}
	total = int64(len(results))
	for _, r := range results {
		if r.Err != nil {
			failCnt++
			if firstErr == nil {
				firstErr = r.Err
			}
		}
	}
	return
}

// ──────────────────────────── 统一 ForEach 执行器 ────────────────────────────

// execEach 根据 Policy 策略执行 ForEach 操作，是所有 ForEach 变体的统一入口。
func execEach[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	ctx = core.EnsureTraceID(ctx)
	n := len(items)
	if n == 0 {
		return 0, 0, nil, nil
	}
	if p.isZero() {
		p = DefPar()
	}

	switch {
	case p.shards > 0:
		return eachSharded[T](ctx, items, fn, p)
	case p.timeout > 0 && p.failFast:
		return eachTOFF[T](ctx, items, fn, p)
	case p.timeout > 0:
		return eachTO[T](ctx, items, fn, p)
	case p.failFast && p.isParallel():
		return eachParFF[T](ctx, items, fn, p)
	case p.chunkSize > 0 && p.isParallel():
		return eachChunked[T](ctx, items, fn, p)
	case p.isParallel():
		return eachPar[T](ctx, items, fn, p)
	default:
		return eachSerialImpl[T](ctx, items, fn, p.failFast)
	}
}

// ──────────────────────────── 统一 Stream 执行器 ────────────────────────────

// execStream 根据 Policy 策略执行流式 Map 操作。
func execStream[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) <-chan core.Result[R] {
	ctx = core.EnsureTraceID(ctx)
	if p.isZero() {
		p = DefPar()
	}

	if p.failFast {
		return streamFF[T, R](ctx, items, fn, p)
	}
	return stream[T, R](ctx, items, fn, p)
}

// ──────────────────────────── 内部实现：Map 路径 ────────────────────────────

// mapSharded 水平分片：将 items 均分到 shards 个 goroutine，每片独立执行。
func mapSharded[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) ([]core.Result[R], error) {
	n := len(items)
	shards := p.shards
	if shards <= 0 || shards > n {
		shards = n
	}
	if shards <= 0 {
		shards = 1
	}
	chunkSize := (n + shards - 1) / shards
	conc := p.concurrency
	if conc <= 0 {
		conc = shards
	}
	if conc > shards {
		conc = shards
	}

	var wg sync.WaitGroup
	results := make([]core.Result[R], n)
	var mu sync.Mutex
	var firstErr error
	var once sync.Once
	sem := make(chan struct{}, conc)

	if p.failFast {
		ffCtx, ffCancel := context.WithCancel(ctx)
		defer ffCancel()
		ctx = ffCtx
	}

	for shard := 0; shard < shards; shard++ {
		start := shard * chunkSize
		end := start + chunkSize
		if start >= n {
			break
		}
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					for j := start; j < end; j++ {
						results[j] = core.Result[R]{Err: core.NewPanicError(r)}
					}
					mu.Unlock()
				}
			}()
			for j := start; j < end; j++ {
				select {
				case <-ctx.Done():
					mu.Lock()
					results[j] = core.Result[R]{Err: ctx.Err()}
					mu.Unlock()
					continue
				default:
				}
				val, err := fn(ctx, items[j])
				mu.Lock()
				results[j] = core.Result[R]{Value: val, Err: err}
				mu.Unlock()
				if err != nil && p.failFast {
					once.Do(func() { firstErr = err })
				}
			}
		}(start, end)
	}
	wg.Wait()
	return results, firstErr
}

// mapChunked 分块执行：先 Chunk 再逐元素并发。
func mapChunked[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) ([]core.Result[R], error) {
	chunks := Chunk(items, p.chunkSize)
	g := group.NewGroup[R](p.concurrency)
	if p.timeout > 0 {
		g.WithTimeout(p.timeout)
	}
	if p.failFast {
		ffCtx, ffCancel := context.WithCancel(ctx)
		defer ffCancel()
		g.WithFailFast(ffCtx)
	}
	idx := 0
	for _, chunk := range chunks {
		for _, item := range chunk {
			item := item
			g.GoAt(idx, ctx, func(ctx context.Context) (R, error) {
				return fn(ctx, item)
			})
			idx++
		}
	}
	if p.failFast {
		return g.Wait(), g.FirstError()
	}
	return g.Wait(), nil
}

// mapTOFF 带超时 + FailFast 的并行 Map。
func mapTOFF[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) ([]core.Result[R], error) {
	n := len(items)
	conc := core.WithConfig(p.concurrency)
	if conc > n {
		conc = n
	}
	if conc <= 0 {
		conc = 1
	}

	g := group.NewGroup[R](conc)
	ffCtx, ffCancel := context.WithCancel(ctx)
	defer ffCancel()
	g.WithFailFast(ffCtx)
	g.WithTimeout(p.timeout)
	for idx := range items {
		g.GoAt(idx, ffCtx, func(ctx context.Context) (R, error) {
			return fn(ctx, items[idx])
		})
	}
	results := g.Wait()
	return results, g.FirstError()
}

// mapTO 带超时的并行 Map。
func mapTO[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) ([]core.Result[R], error) {
	n := len(items)
	conc := core.WithConfig(p.concurrency)
	if conc > n {
		conc = n
	}
	if conc <= 0 {
		conc = 1
	}

	g := group.NewGroup[R](conc)
	g.WithTimeout(p.timeout)
	for idx := range items {
		g.GoAt(idx, ctx, func(ctx context.Context) (R, error) {
			return fn(ctx, items[idx])
		})
	}
	return g.Wait(), nil
}

// ──────────────────────────── 内部实现：ForEach 路径 ────────────────────────────

func eachSharded[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	n := len(items)
	shards := p.shards
	if shards <= 0 || shards > n {
		shards = n
	}
	if shards <= 0 {
		shards = 1
	}
	chunkSize := (n + shards - 1) / shards
	conc := p.concurrency
	if conc <= 0 {
		conc = shards
	}
	if conc > shards {
		conc = shards
	}

	var wg sync.WaitGroup
	results = make([]core.Result[struct{}], n)
	var mu sync.Mutex
	sem := make(chan struct{}, conc)
	total = int64(n)

	if p.failFast {
		ffCtx, ffCancel := context.WithCancel(ctx)
		defer ffCancel()
		ctx = ffCtx
	}

	for shard := 0; shard < shards; shard++ {
		start := shard * chunkSize
		end := start + chunkSize
		if start >= n {
			break
		}
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					for j := start; j < end; j++ {
						results[j] = core.Result[struct{}]{Err: core.NewPanicError(r)}
						failCnt++
					}
					mu.Unlock()
				}
			}()
			for j := start; j < end; j++ {
				select {
				case <-ctx.Done():
					mu.Lock()
					results[j] = core.Result[struct{}]{Err: ctx.Err()}
					failCnt++
					mu.Unlock()
					continue
				default:
				}
				err := fn(ctx, items[j])
				mu.Lock()
				results[j] = core.Result[struct{}]{Err: err}
				if err != nil {
					failCnt++
					if firstErr == nil {
						firstErr = err
					}
				}
				mu.Unlock()
			}
		}(start, end)
	}
	wg.Wait()
	return
}

func eachTOFF[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	n := len(items)
	conc := core.WithConfig(p.concurrency)
	if conc > n {
		conc = n
	}

	ffCtx, ffCancel := context.WithCancel(ctx)
	defer ffCancel()

	g := group.NewNoResult(conc)
	g.WithFailFast(ffCtx)
	g.WithTimeout(p.timeout)
	for i := range items {
		g.Go(ffCtx, func(ctx context.Context) error {
			return fn(ctx, items[i])
		})
	}
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

func eachTO[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	n := len(items)
	conc := core.WithConfig(p.concurrency)
	if conc > n {
		conc = n
	}

	g := group.NewNoResult(conc)
	g.WithTimeout(p.timeout)
	for i := range items {
		g.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[i])
		})
	}
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

func eachParFF[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	n := len(items)
	conc := core.WithConfig(p.concurrency)
	if conc > n {
		conc = n
	}

	ffCtx, ffCancel := context.WithCancel(ctx)
	defer ffCancel()

	if conc <= 1 {
		return eachSerialImpl(ffCtx, items, fn, true)
	}

	g := group.NewNoResult(conc)
	for i := range items {
		g.Go(ffCtx, func(ctx context.Context) error {
			err := fn(ctx, items[i])
			if err != nil {
				ffCancel()
			}
			return err
		})
	}
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

func eachPar[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	n := len(items)
	conc := core.WithConfig(p.concurrency)
	if conc > n {
		conc = n
	}

	if conc <= 1 {
		return eachSerialImpl(ctx, items, fn, false)
	}

	g := group.NewNoResult(conc)
	for i := range items {
		g.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[i])
		})
	}
	g.Wait()
	return group.BuildAggregateNoResult(g)
}

func eachChunked[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	chunks := Chunk(items, p.chunkSize)
	total = int64(len(items))
	results = make([]core.Result[struct{}], 0, total)

	for ci, chunk := range chunks {
		var chunkTotal int64
		var chunkFail int64
		var chunkFirstErr error
		var chunkResults []core.Result[struct{}]

		chunkTotal, chunkFail, chunkFirstErr, chunkResults = execEach(ctx, chunk, fn, Par(p.concurrency))

		results = append(results, chunkResults...)
		failCnt += chunkFail
		if firstErr == nil {
			firstErr = chunkFirstErr
		}
		_ = ci
		_ = chunkTotal
	}
	return
}

// ──────────────────────────── 内部实现：Stream 路径 ────────────────────────────

func stream[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) <-chan core.Result[R] {
	n := len(items)
	conc := p.concurrency
	if conc <= 0 {
		conc = core.IO()
	}
	if conc > n && n > 0 {
		conc = n
	}
	if conc <= 0 {
		conc = 1
	}

	bufSize := p.bufSize
	if bufSize <= 0 {
		if n <= 16384 {
			bufSize = n
		} else {
			bufSize = 16384
		}
	}

	streamBuf := n
	const maxStreamBuf = 65536
	if streamBuf > maxStreamBuf {
		streamBuf = maxStreamBuf
	}
	if streamBuf < bufSize {
		streamBuf = bufSize
	}

	outCh := make(chan core.Result[R], bufSize)
	g := group.NewGroup[R](conc)
	g.WithStreaming(streamBuf)

	go func() {
		for r := range g.StreamResults() {
			outCh <- r
		}
		close(outCh)
	}()

	go func() {
		for i := range items {
			idx := i
			g.Go(ctx, func(ctx context.Context) (R, error) {
				return fn(ctx, items[idx])
			})
		}
		g.Wait()
	}()

	return outCh
}

func streamFF[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), p Policy) <-chan core.Result[R] {
	n := len(items)
	conc := p.concurrency
	if conc <= 0 {
		conc = core.IO()
	}
	if conc > n && n > 0 {
		conc = n
	}
	if conc <= 0 {
		conc = 1
	}

	bufSize := p.bufSize
	if bufSize <= 0 {
		if n <= 16384 {
			bufSize = n
		} else {
			bufSize = 16384
		}
	}

	ffCtx, ffCancel := context.WithCancel(ctx)
	streamBuf := n
	const maxStreamBuf = 65536
	if streamBuf > maxStreamBuf {
		streamBuf = maxStreamBuf
	}
	if streamBuf < bufSize {
		streamBuf = bufSize
	}

	outCh := make(chan core.Result[R], bufSize)
	g := group.NewGroup[R](conc)
	g.WithStreaming(streamBuf)

	go func() {
		for r := range g.StreamResults() {
			outCh <- r
		}
		close(outCh)
	}()

	go func() {
		for i := range items {
			idx := i
			g.Go(ffCtx, func(ctx context.Context) (R, error) {
				defer func() {
					if r := recover(); r != nil {
						ffCancel()
						panic(r)
					}
				}()
				val, err := fn(ctx, items[idx])
				if err != nil {
					ffCancel()
				}
				return val, err
			})
		}
		g.Wait()
		ffCancel()
	}()

	return outCh
}

// ──────────────────────────── 统一 ForEachStream 执行器 ────────────────────────────

// execEachStream 根据 Policy 策略执行流式 ForEach 操作。
func execEachStream[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) <-chan core.Result[struct{}] {
	ctx = core.EnsureTraceID(ctx)
	if p.isZero() {
		p = DefPar()
	}

	if p.failFast {
		return eachStreamFF[T](ctx, items, fn, p)
	}
	return eachStream[T](ctx, items, fn, p)
}

func eachStream[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) <-chan core.Result[struct{}] {
	n := len(items)
	conc := p.concurrency
	if conc <= 0 {
		conc = core.IO()
	}
	if conc > n && n > 0 {
		conc = n
	}
	if conc <= 0 {
		conc = 1
	}

	bufSize := p.bufSize
	if bufSize <= 0 {
		if n <= 16384 {
			bufSize = n
		} else {
			bufSize = 16384
		}
	}

	outCh := make(chan core.Result[struct{}], bufSize)
	nr := group.NewNoResult(conc)
	nr.WithResultCallback(func(r core.Result[struct{}]) {
		outCh <- r
	})

	go func() {
		for i := range items {
			idx := i
			nr.Go(ctx, func(ctx context.Context) error {
				return fn(ctx, items[idx])
			})
		}
		nr.Wait()
		close(outCh)
	}()

	return outCh
}

func eachStreamFF[T any](ctx context.Context, items []T, fn func(context.Context, T) error, p Policy) <-chan core.Result[struct{}] {
	n := len(items)
	conc := p.concurrency
	if conc <= 0 {
		conc = core.IO()
	}
	if conc > n && n > 0 {
		conc = n
	}
	if conc <= 0 {
		conc = 1
	}

	bufSize := p.bufSize
	if bufSize <= 0 {
		if n <= 16384 {
			bufSize = n
		} else {
			bufSize = 16384
		}
	}

	ffCtx, ffCancel := context.WithCancel(ctx)
	outCh := make(chan core.Result[struct{}], bufSize)
	nr := group.NewNoResult(conc)
	nr.WithResultCallback(func(r core.Result[struct{}]) {
		outCh <- r
	})

	go func() {
		for i := range items {
			idx := i
			nr.Go(ffCtx, func(ctx context.Context) error {
				err := fn(ctx, items[idx])
				if err != nil {
					ffCancel()
				}
				return err
			})
		}
		nr.Wait()
		ffCancel()
		close(outCh)
	}()

	return outCh
}

// ──────────────────────────── 统一 Reduce 执行器（始终串行） ────────────────────────────

// execReduce Reduce 是折叠/聚合操作，本质串行。
func execReduce[T any, R any](ctx context.Context, items []T, initial R, fn func(context.Context, R, T) (R, error)) (R, error) {
	ctx = core.EnsureTraceID(ctx)
	acc := initial
	for _, item := range items {
		var err error
		acc, err = fn(ctx, acc, item)
		if err != nil {
			return acc, err
		}
	}
	return acc, nil
}

// ──────────────────────────── Map 底层实现 ────────────────────────────

func mapImpl[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int, failFast bool) ([]core.Result[R], error) {
	n := len(items)
	if n == 0 {
		return nil, nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > n {
		concurrency = n
	}

	concurrency = core.WithConfig(concurrency)

	results := make([]core.Result[R], n)
	var wg sync.WaitGroup
	chunkSize := (n + concurrency - 1) / concurrency

	var cancel context.CancelFunc
	if failFast {
		ctx, cancel = context.WithCancel(ctx)
	}

	if failFast {
		mapParallelFF(ctx, cancel, items, fn, results, concurrency, chunkSize, &wg)
	} else {
		mapParallel(ctx, items, fn, results, concurrency, chunkSize, &wg)
	}

	wg.Wait()
	if cancel != nil {
		cancel()
	}

	if failFast {
		for _, r := range results {
			if r.Err != nil {
				return results, r.Err
			}
		}
	}
	return results, nil
}

func mapParallel[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), results []core.Result[R], concurrency int, chunkSize int, w *sync.WaitGroup) {
	for i := 0; i < concurrency; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(items) {
			break
		}
		if end > len(items) {
			end = len(items)
		}
		w.Add(1)
		go func(start, end int) {
			defer w.Done()
			defer func() {
				if r := recover(); r != nil {
					for j := start; j < end; j++ {
						results[j] = core.Result[R]{Err: core.NewPanicError(r)}
					}
				}
			}()
			for j := start; j < end; j++ {
				val, err := fn(ctx, items[j])
				results[j] = core.Result[R]{Value: val, Err: err}
			}
		}(start, end)
	}
}

func mapParallelFF[T any, R any](ctx context.Context, cancel context.CancelFunc, items []T, fn func(context.Context, T) (R, error), results []core.Result[R], concurrency int, chunkSize int, w *sync.WaitGroup) {
	for i := 0; i < concurrency; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(items) {
			break
		}
		if end > len(items) {
			end = len(items)
		}
		w.Add(1)
		go func(start, end int) {
			defer w.Done()
			for j := start; j < end; j++ {
				select {
				case <-ctx.Done():
					results[j] = core.Result[R]{Err: ctx.Err()}
					continue
				default:
				}
				val, err := fn(ctx, items[j])
				results[j] = core.Result[R]{Value: val, Err: err}
				if err != nil {
					cancel()
				}
			}
		}(start, end)
	}
}

// ──────────────────────────── Map 串行实现 ────────────────────────────

func mapSerialExec[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), failFast bool) ([]core.Result[R], error) {
	n := len(items)
	results := make([]core.Result[R], n)
	if failFast {
		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()
		for i, item := range items {
			val, err := core.SafeCall(ctx2, item, fn)
			results[i] = core.Result[R]{Value: val, Err: err}
			if err != nil {
				return results, err
			}
		}
		return results, nil
	}
	for i, item := range items {
		val, err := core.SafeCall(ctx, item, fn)
		results[i] = core.Result[R]{Value: val, Err: err}
	}
	return results, nil
}

// ──────────────────────────── ForEach 串行实现 ────────────────────────────

func eachSerialImpl[T any](ctx context.Context, items []T, fn func(context.Context, T) error, failFast bool) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	total = int64(len(items))
	results = make([]core.Result[struct{}], len(items))

	if failFast {
		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()
		for i, item := range items {
			if err := core.SafeCallVoid(ctx2, item, fn); err != nil {
				results[i] = core.Result[struct{}]{Err: err}
				failCnt++
				firstErr = err
				return
			}
			results[i] = core.Result[struct{}]{}
		}
		return
	}
	for i, item := range items {
		if err := core.SafeCallVoid(ctx, item, fn); err != nil {
			results[i] = core.Result[struct{}]{Err: err}
			failCnt++
			if firstErr == nil {
				firstErr = err
			}
		} else {
			results[i] = core.Result[struct{}]{}
		}
	}
	return
}

// ──────────────────────────── 工具函数 ────────────────────────────

// Chunk 将切片按大小均分。适用于将大数据集拆分为并发处理的小块。
func Chunk[T any](items []T, chunkSize int) [][]T {
	if chunkSize <= 0 || len(items) == 0 {
		return nil
	}
	chunks := make([][]T, 0, (len(items)+chunkSize-1)/chunkSize)
	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}
	return chunks
}

// Reduce 串行聚合：对每个元素调用 fn(acc, item)，累积结果。
func Reduce[T any, R any](ctx context.Context, items []T, initial R, fn func(context.Context, R, T) (R, error)) (R, error) {
	return execReduce(ctx, items, initial, fn)
}

// ChunkN 将切片按份数均分。
func ChunkN[T any](items []T, n int) [][]T {
	if n <= 0 || len(items) == 0 {
		return nil
	}
	chunkSize := (len(items) + n - 1) / n
	return Chunk(items, chunkSize)
}

// Partition 按条件将切片拆分为匹配和不匹配两个切片。
func Partition[T any](items []T, pred func(T) bool) (matched []T, unmatched []T) {
	for _, item := range items {
		if pred(item) {
			matched = append(matched, item)
		} else {
			unmatched = append(unmatched, item)
		}
	}
	return
}

// Must 错误时 panic。
func Must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

// Flat 从 Result 切片中提取所有值。
func Flat[T any](results []core.Result[T]) []T {
	out := make([]T, 0, len(results))
	for _, r := range results {
		out = append(out, r.Value)
	}
	return out
}

// OnlyErrors 从 Result 切片中提取所有非 nil 错误。
func OnlyErrors[T any](results []core.Result[T]) []error {
	out := make([]error, 0)
	for _, r := range results {
		if r.Err != nil {
			out = append(out, r.Err)
		}
	}
	return out
}

// SafeCallWithResult 带 panic 恢复的安全调用。
func SafeCallWithResult[T any, R any](ctx context.Context, item T, fn func(context.Context, T) (R, error)) (val R, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := core.NewPanicError(r)
			var pcs [64]uintptr
			n := runtime.Callers(0, pcs[:])
			frames := runtime.CallersFrames(pcs[:n])
			var fnName string
			frame, more := frames.Next()
			if more {
				fnName = frame.Function
			}
			core.LogError("async path panic recovered",
				core.Str("fn", fnName),
				core.Any("panic", r),
				core.Bytes("stack", pe.Stack))
			err = pe
		}
	}()
	return fn(ctx, item)
}

// ──────────────────────────── 旧 API 包级入口（向后兼容） ────────────────────────────

// Map 并发处理切片元素，返回 Result 切片。
// Deprecated: 推荐使用 execMap 结合 Policy。
func Map[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error) {
	return execMap(ctx, items, fn, Par(concurrency))
}

// MapWithFailFast 并发 Map，首个失败时取消其余任务。
// Deprecated: 推荐使用 execMap 结合 Policy.FF()。
func MapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error) {
	return execMap(ctx, items, fn, Par(concurrency).FF())
}

// ForEach 并发遍历切片。
// Deprecated: 推荐使用 execEach 结合 Policy。
func ForEach[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, items, fn, Par(concurrency))
}

// ForEachWithFailFast 并发 ForEach，首个失败时取消其余任务。
// Deprecated: 推荐使用 execEach 结合 Policy.FF()。
func ForEachWithFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, items, fn, Par(concurrency).FF())
}
