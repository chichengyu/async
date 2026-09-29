// Package sliceops Slice[T,R] MapReduce 方法层。
//
// 推荐使用基于 Policy 策略配置的新 API（Do/Each/Stream/EachStream/Reduce），
// 通过链式配置 Policy 消除函数组合爆炸。
//
// 旧 API（Map/MapWithFailFast/...）保留向后兼容。
package sliceops

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── 新 API：策略驱动 ────────────────────────────

// Do 根据 Policy 策略执行 Map 操作，统一串行/并行/分块/分片/超时/FailFast。
//
// 使用示例：
//
//	// 并行 + FailFast + 超时
//	results, err := s.Do(ctx, fn, sliceops.Par(8).FF().TO(5 * time.Second))
//
//	// 串行
//	results, err := s.Do(ctx, fn, sliceops.Seq())
//
//	// 串行 + FailFast
//	results, err := s.Do(ctx, fn, sliceops.Seq().FF())
//
//	// 分块并行
//	results, err := s.Do(ctx, fn, sliceops.Par(16).Chunk(100))
//
//	// 水平分片
//	results, err := s.Do(ctx, fn, sliceops.Par(8).Shard(4))
func (s *Slice[T, R]) Do(ctx context.Context, fn func(context.Context, T) (R, error), p Policy) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, p)
}

// Each 根据 Policy 策略执行 ForEach 操作。
func (s *Slice[T, R]) Each(ctx context.Context, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error) {
	t, f, fe, _ := execEach(ctx, s.items, fn, p)
	return t, f, fe
}

// EachFull 与 Each 相同，额外返回完整结果切片。
func (s *Slice[T, R]) EachFull(ctx context.Context, fn func(context.Context, T) error, p Policy) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, p)
}

// Stream 根据 Policy 策略执行流式 Map 操作。
func (s *Slice[T, R]) Stream(ctx context.Context, fn func(context.Context, T) (R, error), p Policy) <-chan core.Result[R] {
	return execStream(ctx, s.items, fn, p)
}

// EachStream 根据 Policy 策略执行流式 ForEach 操作。
func (s *Slice[T, R]) EachStream(ctx context.Context, fn func(context.Context, T) error, p Policy) <-chan core.Result[struct{}] {
	return execEachStream(ctx, s.items, fn, p)
}

// Reduce 串行聚合（始终串行，无需 Policy）。
func (s *Slice[T, R]) Reduce(ctx context.Context, initial R, fn func(context.Context, R, T) (R, error)) (R, error) {
	return execReduce(ctx, s.items, initial, fn)
}

// ──────────────────────────── 旧 API（向后兼容） ────────────────────────────

// Map 并发处理切片元素，返回 Result 切片。concurrency <= 0 时使用 core.IO()。
// Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency))。
func (s *Slice[T, R]) Map(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, Par(concurrency))
}

// MapWithFailFast 并发 Map，首个失败时取消其余任务。
// Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency).FF())。
func (s *Slice[T, R]) MapWithFailFast(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, Par(concurrency).FF())
}

// DefaultMap 使用默认并发度的 Map。
// Deprecated: 推荐使用 Do(ctx, fn, DefPar())，取 Values。
func (s *Slice[T, R]) DefaultMap(ctx context.Context, fn func(context.Context, T) (R, error)) []core.Result[R] {
	results, _ := execMap(ctx, s.items, fn, DefPar())
	return results
}

// DefaultMapWithFailFast 使用默认并发度的 FailFast Map。
// Deprecated: 推荐使用 Do(ctx, fn, DefPar().FF())。
func (s *Slice[T, R]) DefaultMapWithFailFast(ctx context.Context, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, DefPar().FF())
}

// MapSerial 串行处理切片元素，返回 Result 切片。
// Deprecated: 推荐使用 Do(ctx, fn, Seq())。
func (s *Slice[T, R]) MapSerial(ctx context.Context, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, Seq())
}

// MapSerialFailFast 串行带快速失败的 Map。
// Deprecated: 推荐使用 Do(ctx, fn, Seq().FF())。
func (s *Slice[T, R]) MapSerialFailFast(ctx context.Context, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, Seq().FF())
}

// MapStream 并发处理切片元素，通过 channel 流式返回结果。
// Deprecated: 推荐使用 Stream(ctx, fn, Par(concurrency).Buf(bufSize))。
func (s *Slice[T, R]) MapStream(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int, bufSize int) <-chan core.Result[R] {
	return execStream(ctx, s.items, fn, Par(concurrency).Buf(bufSize))
}

// MapStreamWithFailFast 带 FailFast 的流式 Map。
// Deprecated: 推荐使用 Stream(ctx, fn, Par(concurrency).FF().Buf(bufSize))。
func (s *Slice[T, R]) MapStreamWithFailFast(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int, bufSize int) <-chan core.Result[R] {
	return execStream(ctx, s.items, fn, Par(concurrency).FF().Buf(bufSize))
}

// MapWithTimeout 带单任务超时的并发 Map。
// Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency).TO(timeout))，取 Values。
func (s *Slice[T, R]) MapWithTimeout(ctx context.Context, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) []core.Result[R] {
	results, _ := execMap(ctx, s.items, fn, Par(concurrency).TO(timeout))
	return results
}

// DefaultMapWithTimeout 使用默认并发度的带超时 Map。
// Deprecated: 推荐使用 Do(ctx, fn, DefPar().TO(timeout))，取 Values。
func (s *Slice[T, R]) DefaultMapWithTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context, T) (R, error)) []core.Result[R] {
	results, _ := execMap(ctx, s.items, fn, DefPar().TO(timeout))
	return results
}

// MapWithFFTimeout 带 FailFast 和单任务超时的并发 Map。
// Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency).FF().TO(timeout))。
func (s *Slice[T, R]) MapWithFFTimeout(ctx context.Context, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, Par(concurrency).FF().TO(timeout))
}

// DefaultMapWithFFTimeout 使用默认并发度的 FailFast + 超时 Map。
// Deprecated: 推荐使用 Do(ctx, fn, DefPar().FF().TO(timeout))。
func (s *Slice[T, R]) DefaultMapWithFFTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, DefPar().FF().TO(timeout))
}

// MapSharded 水平分片并发 Map。
// Deprecated: 推荐使用 Do(ctx, fn, Par(concurrency).Shard(shards))。
func (s *Slice[T, R]) MapSharded(ctx context.Context, fn func(context.Context, T) (R, error), concurrency int, shards int) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, Par(concurrency).Shard(shards))
}

// ──────────────────────────── MapChunk / MapChunked 保留兼容 ────────────────────────────

// MapChunk 先分块再并发处理每个块，fn 接收整个 chunk。
func (s *Slice[T, R]) MapChunk(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) []core.Result[R] {
	return MapChunk(ctx, s.items, concurrency, batchSize, fn)
}
func (s *Slice[T, R]) DefaultMapChunk(ctx context.Context, batchSize int, fn func(context.Context, []T) (R, error)) []core.Result[R] {
	return DefaultMapChunk(ctx, s.items, batchSize, fn)
}
func (s *Slice[T, R]) MapChunkWithFailFast(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) ([]core.Result[R], error) {
	return MapChunkWithFailFast(ctx, s.items, concurrency, batchSize, fn)
}
func (s *Slice[T, R]) DefaultMapChunkWithFailFast(ctx context.Context, batchSize int, fn func(context.Context, []T) (R, error)) ([]core.Result[R], error) {
	return DefaultMapChunkWithFailFast(ctx, s.items, batchSize, fn)
}
func (s *Slice[T, R]) MapChunkWithTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []core.Result[R] {
	return MapChunkWithTimeout(ctx, s.items, concurrency, batchSize, timeout, fn)
}
func (s *Slice[T, R]) DefaultMapChunkWithTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []core.Result[R] {
	return DefaultMapChunkWithTimeout(ctx, s.items, batchSize, timeout, fn)
}
func (s *Slice[T, R]) MapChunkWithFFTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]core.Result[R], error) {
	return MapChunkWithFFTimeout(ctx, s.items, concurrency, batchSize, timeout, fn)
}
func (s *Slice[T, R]) DefaultMapChunkWithFFTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]core.Result[R], error) {
	return DefaultMapChunkWithFFTimeout(ctx, s.items, batchSize, timeout, fn)
}

// MapChunked 先分块再逐元素并发调用 fn。
func (s *Slice[T, R]) MapChunked(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) []core.Result[R] {
	results, _ := execMap(ctx, s.items, fn, Par(concurrency).Chunk(batchSize))
	return results
}
func (s *Slice[T, R]) DefaultMapChunked(ctx context.Context, batchSize int, fn func(context.Context, T) (R, error)) []core.Result[R] {
	results, _ := execMap(ctx, s.items, fn, DefPar().Chunk(batchSize))
	return results
}
func (s *Slice[T, R]) MapChunkedWithFailFast(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, Par(concurrency).Chunk(batchSize).FF())
}
func (s *Slice[T, R]) DefaultMapChunkedWithFailFast(ctx context.Context, batchSize int, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, DefPar().Chunk(batchSize).FF())
}
func (s *Slice[T, R]) MapChunkedWithTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []core.Result[R] {
	results, _ := execMap(ctx, s.items, fn, Par(concurrency).Chunk(batchSize).TO(timeout))
	return results
}
func (s *Slice[T, R]) DefaultMapChunkedWithTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []core.Result[R] {
	results, _ := execMap(ctx, s.items, fn, DefPar().Chunk(batchSize).TO(timeout))
	return results
}
func (s *Slice[T, R]) MapChunkedWithFFTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, Par(concurrency).Chunk(batchSize).FF().TO(timeout))
}
func (s *Slice[T, R]) DefaultMapChunkedWithFFTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(ctx, s.items, fn, DefPar().Chunk(batchSize).FF().TO(timeout))
}

// ──────────────────────────── ForEach 旧 API（向后兼容） ────────────────────────────

// ForEach 并发遍历切片。
// Deprecated: 推荐使用 Each(ctx, fn, Par(concurrency))。
func (s *Slice[T, R]) ForEach(ctx context.Context, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency))
}

// ForEachWithFailFast 并发 ForEach，首个失败时取消其余任务。
// Deprecated: 推荐使用 Each(ctx, fn, Par(concurrency).FF())。
func (s *Slice[T, R]) ForEachWithFailFast(ctx context.Context, fn func(context.Context, T) error, concurrency int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency).FF())
}

// DefaultForEach 使用默认并发度的 ForEach。
// Deprecated: 推荐使用 Each(ctx, fn, DefPar())。
func (s *Slice[T, R]) DefaultForEach(ctx context.Context, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, DefPar())
}

// DefaultForEachWithFailFast 使用默认并发度的 FailFast ForEach。
// Deprecated: 推荐使用 Each(ctx, fn, DefPar().FF())。
func (s *Slice[T, R]) DefaultForEachWithFailFast(ctx context.Context, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, DefPar().FF())
}

// ForEachSerial 串行遍历切片。
// Deprecated: 推荐使用 Each(ctx, fn, Seq())。
func (s *Slice[T, R]) ForEachSerial(ctx context.Context, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Seq())
}

// ForEachSerialFailFast 串行带快速失败的 ForEach。
// Deprecated: 推荐使用 Each(ctx, fn, Seq().FF())。
func (s *Slice[T, R]) ForEachSerialFailFast(ctx context.Context, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Seq().FF())
}

// ForEachStream 并发遍历，通过 channel 流式返回。
// Deprecated: 推荐使用 EachStream(ctx, fn, Par(concurrency).Buf(bufSize))。
func (s *Slice[T, R]) ForEachStream(ctx context.Context, fn func(context.Context, T) error, concurrency int, bufSize int) <-chan core.Result[struct{}] {
	return execEachStream(ctx, s.items, fn, Par(concurrency).Buf(bufSize))
}

// ForEachStreamWithFailFast 带 FailFast 的流式 ForEach。
// Deprecated: 推荐使用 EachStream(ctx, fn, Par(concurrency).FF().Buf(bufSize))。
func (s *Slice[T, R]) ForEachStreamWithFailFast(ctx context.Context, fn func(context.Context, T) error, concurrency int, bufSize int) <-chan core.Result[struct{}] {
	return execEachStream(ctx, s.items, fn, Par(concurrency).FF().Buf(bufSize))
}

// ForEachWithTimeout 带单任务超时的 ForEach。
// Deprecated: 推荐使用 Each(ctx, fn, Par(concurrency).TO(timeout))。
func (s *Slice[T, R]) ForEachWithTimeout(ctx context.Context, concurrency int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency).TO(timeout))
}

// DefaultForEachWithTimeout 使用默认并发度的带超时 ForEach。
func (s *Slice[T, R]) DefaultForEachWithTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, DefPar().TO(timeout))
}

// ForEachWithFFTimeout 带 FailFast 和单任务超时的 ForEach。
func (s *Slice[T, R]) ForEachWithFFTimeout(ctx context.Context, concurrency int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency).FF().TO(timeout))
}

// DefaultForEachWithFFTimeout 使用默认并发度的 FailFast + 超时 ForEach。
func (s *Slice[T, R]) DefaultForEachWithFFTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, DefPar().FF().TO(timeout))
}

// ForEachSharded 水平分片并发 ForEach。
// Deprecated: 推荐使用 Each(ctx, fn, Par(concurrency).Shard(shards))。
func (s *Slice[T, R]) ForEachSharded(ctx context.Context, fn func(context.Context, T) error, concurrency int, shards int) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency).Shard(shards))
}

// ──────────────────────────── ForEachChunk / ForEachChunked 保留兼容 ────────────────────────────

// ForEachChunk 先分块再并发处理每个块，fn 接收整个 chunk。
func (s *Slice[T, R]) ForEachChunk(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunk(ctx, s.items, concurrency, batchSize, fn)
}
func (s *Slice[T, R]) DefaultForEachChunk(ctx context.Context, batchSize int, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return DefaultForEachChunk(ctx, s.items, batchSize, fn)
}
func (s *Slice[T, R]) ForEachChunkWithFailFast(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithFailFast(ctx, s.items, concurrency, batchSize, fn)
}
func (s *Slice[T, R]) DefaultForEachChunkWithFailFast(ctx context.Context, batchSize int, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return DefaultForEachChunkWithFailFast(ctx, s.items, batchSize, fn)
}
func (s *Slice[T, R]) ForEachChunkWithTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithTimeout(ctx, s.items, concurrency, batchSize, timeout, fn)
}
func (s *Slice[T, R]) DefaultForEachChunkWithTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return DefaultForEachChunkWithTimeout(ctx, s.items, batchSize, timeout, fn)
}
func (s *Slice[T, R]) ForEachChunkWithFFTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return ForEachChunkWithFFTimeout(ctx, s.items, concurrency, batchSize, timeout, fn)
}
func (s *Slice[T, R]) DefaultForEachChunkWithFFTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return DefaultForEachChunkWithFFTimeout(ctx, s.items, batchSize, timeout, fn)
}

// ForEachChunked 先分块再逐元素并发执行。
func (s *Slice[T, R]) ForEachChunked(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency).Chunk(batchSize))
}
func (s *Slice[T, R]) DefaultForEachChunked(ctx context.Context, batchSize int, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, DefPar().Chunk(batchSize))
}
func (s *Slice[T, R]) ForEachChunkedWithFailFast(ctx context.Context, concurrency int, batchSize int, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency).Chunk(batchSize).FF())
}
func (s *Slice[T, R]) DefaultForEachChunkedWithFailFast(ctx context.Context, batchSize int, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, DefPar().Chunk(batchSize).FF())
}
func (s *Slice[T, R]) ForEachChunkedWithTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency).Chunk(batchSize).TO(timeout))
}
func (s *Slice[T, R]) DefaultForEachChunkedWithTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, DefPar().Chunk(batchSize).TO(timeout))
}
func (s *Slice[T, R]) ForEachChunkedWithFFTimeout(ctx context.Context, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, Par(concurrency).Chunk(batchSize).FF().TO(timeout))
}
func (s *Slice[T, R]) DefaultForEachChunkedWithFFTimeout(ctx context.Context, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	return execEach(ctx, s.items, fn, DefPar().Chunk(batchSize).FF().TO(timeout))
}
