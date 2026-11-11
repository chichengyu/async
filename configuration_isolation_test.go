package async

import (
	"context"
	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/task"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type isolationLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *isolationLogger) contains(msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.messages {
		if m == msg {
			return true
		}
	}
	return false
}

func TestFluentLoggerBuiltInstanceAndDefaultIsolation(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	global, a, b := &isolationLogger{}, &isolationLogger{}, &isolationLogger{}
	SetLogger(global)
	builder := Group[int]().Worker(1).Logger(a)
	first := builder.Build()
	defer first.Close()
	builder.Logger(b)
	second := builder.Build()
	defer second.Close()
	builder.DefaultLogger()
	inherited := builder.Build()
	defer inherited.Close()
	release := make(chan struct{})
	emit := func(label string) func(context.Context) (int, error) {
		return func(ctx context.Context) (int, error) { <-release; core.LogCtxInfo(ctx, label); return 1, nil }
	}
	if err := first.Go(context.Background(), emit("first")); err != nil {
		t.Fatal(err)
	}
	if err := second.Go(context.Background(), emit("second")); err != nil {
		t.Fatal(err)
	}
	if err := inherited.Go(context.Background(), emit("inherited")); err != nil {
		t.Fatal(err)
	}
	close(release)
	first.Wait()
	second.Wait()
	inherited.Wait()
	if !a.contains("first") || a.contains("second") || a.contains("inherited") ||
		!b.contains("second") || b.contains("first") || b.contains("inherited") ||
		!global.contains("inherited") || global.contains("first") || global.contains("second") {
		t.Fatal("built instances share logger configuration")
	}
	if GetLogger() != global {
		t.Fatal("DefaultLogger reset global logger")
	}
}

func TestFluentLoggerBorrowedPoolKeepsBothScopes(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	global, owner, local := &isolationLogger{}, &isolationLogger{}, &isolationLogger{}
	SetLogger(global)
	if err := Pool[int]().Worker(1).Logger(owner).Run(func(_ context.Context, p *PoolInstance[int]) error {
		g := Group[int]().Worker(1).Logger(local).Pool(p).Build()
		defer g.Close()
		if err := g.Go(context.Background(), func(ctx context.Context) (int, error) { core.LogCtxInfo(ctx, "group"); return 1, nil }); err != nil {
			return err
		}
		g.Wait()
		if err := p.Submit(context.Background(), func(ctx context.Context) (int, error) { core.LogCtxInfo(ctx, "pool"); return 1, nil }); err != nil {
			return err
		}
		p.Wait()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !local.contains("group") || local.contains("pool") || !owner.contains("pool") || owner.contains("group") || global.contains("group") || global.contains("pool") {
		t.Fatalf("borrowed pool changed scope: local=%v owner=%v global=%v", local.messages, owner.messages, global.messages)
	}
}

func TestFluentLoggerDefaultsRestoreOnlyCurrentScope(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	global, local := &isolationLogger{}, &isolationLogger{}
	SetLogger(global)
	ctx := context.Background()
	run := func(c context.Context) (int, error) { core.LogCtxInfo(c, "default-scope"); return 1, nil }
	_, err := Task[int]().Logger(local).DefaultLogger().Context(ctx).Go(run).Wait()
	if err != nil {
		t.Fatal(err)
	}
	builder := Pipeline([]int{1}).Stage("s", 1).Logger(local)
	branch := builder.Serial()
	builder.DefaultLogger()
	if _, err := builder.Execute(func(c context.Context, _ string, v int) (int, error) {
		core.LogCtxInfo(c, "default-pipeline")
		return v, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := branch.Execute(func(c context.Context, _ string, v int) (int, error) { core.LogCtxInfo(c, "branch"); return v, nil }); err != nil {
		t.Fatal(err)
	}
	if !global.contains("default-scope") || !global.contains("default-pipeline") || global.contains("branch") ||
		local.contains("default-scope") || local.contains("default-pipeline") || !local.contains("branch") {
		t.Fatal("DefaultLogger affected another scope")
	}
}

func TestFluentLoggerContextReplacementPreservesInstance(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	global, local := &isolationLogger{}, &isolationLogger{}
	SetLogger(global)
	g := Group[int]().Worker(1).Logger(local).Build()
	defer g.Close()
	g.WithContext(context.Background())
	if err := g.Go(context.Background(), func(c context.Context) (int, error) { core.LogCtxInfo(c, "rebound"); return 1, nil }); err != nil {
		t.Fatal(err)
	}
	g.Wait()
	if !local.contains("rebound") || global.contains("rebound") {
		t.Fatal("WithContext discarded instance logger")
	}
}

func TestFluentLoggerExecutionIsolation(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	ctx := context.Background()
	cases := []struct {
		name string
		run  func(*isolationLogger) error
	}{
		{"Pool", func(l *isolationLogger) error {
			return Pool[int]().Worker(1).Logger(l).Context(ctx).Run(func(_ context.Context, p *PoolInstance[int]) error {
				err := p.Submit(ctx, func(c context.Context) (int, error) { core.LogCtxInfo(c, "local"); return 1, nil })
				p.Wait()
				return err
			})
		}},
		{"PoolMulti", func(l *isolationLogger) error {
			return PoolMulti[int]().Worker(1).Shards(2).Logger(l).Run(func(_ context.Context, p *MultiPool[int]) error {
				err := p.GetShard(1).Submit(ctx, func(c context.Context) (int, error) { core.LogCtxInfo(c, "local"); return 1, nil })
				p.Wait()
				return err
			})
		}},
		{"PoolSharded", func(l *isolationLogger) error {
			return PoolSharded[int]().Worker(1).Shards(2).Logger(l).Run(func(_ context.Context, p *ShardedPool[int]) error {
				err := p.GetShard(1).Submit(ctx, func(c context.Context) (int, error) { core.LogCtxInfo(c, "local"); return 1, nil })
				p.Wait()
				return err
			})
		}},
		{"Group", func(l *isolationLogger) error {
			g := Group[int]().Worker(1).Logger(l).Context(ctx).Build()
			defer g.Close()
			err := g.Go(ctx, func(c context.Context) (int, error) { core.LogCtxInfo(c, "local"); return 1, nil })
			g.Wait()
			return err
		}},
		{"GroupMulti", func(l *isolationLogger) error {
			return GroupMulti[int]().Worker(1).Shards(2).Logger(l).Run(func(_ context.Context, g *MultiGroup[int]) error {
				err := g.GetShard(1).Go(ctx, func(c context.Context) (int, error) { core.LogCtxInfo(c, "local"); return 1, nil })
				g.Wait()
				return err
			})
		}},
		{"GroupSharded", func(l *isolationLogger) error {
			return GroupSharded[int]().Worker(1).Shards(2).Logger(l).Run(func(_ context.Context, g *ShardedGroup[int]) error {
				err := g.GetShard(1).Go(ctx, func(c context.Context) (int, error) { core.LogCtxInfo(c, "local"); return 1, nil })
				g.Wait()
				return err
			})
		}},
		{"Task", func(l *isolationLogger) error {
			_, err := Task[int]().Logger(l).Context(ctx).Go(func(c context.Context) (int, error) { core.LogCtxInfo(c, "local"); return 1, nil }).Wait()
			return err
		}},
		{"Slice", func(l *isolationLogger) error {
			return Slice(ctx, []int{1}).Logger(l).Context(ctx).Serial().Map(func(c context.Context, v int) (int, error) { core.LogCtxInfo(c, "local"); return v, nil }).Error()
		}},
		{"Map", func(l *isolationLogger) error {
			return NewMapChain(ctx, map[int]int{1: 1}).Logger(l).Context(ctx).Serial().Map(func(c context.Context, k, v int) (int, error) { core.LogCtxInfo(c, "local"); return v, nil }).Error()
		}},
		{"Pipeline", func(l *isolationLogger) error {
			_, err := Pipeline([]int{1}).Stage("s", 1).Logger(l).Context(ctx).Serial().Execute(func(c context.Context, _ string, v int) (int, error) { core.LogCtxInfo(c, "local"); return v, nil })
			return err
		}},
		{"Pipeline.Pipe", func(l *isolationLogger) error {
			p := Pipeline([]int{1}).Stage("s", 1).Pipe().Logger(l).Run(func(c context.Context, _ string, v int) (int, error) { core.LogCtxInfo(c, "local"); return v, nil })
			for range p.Receive() {
			}
			return nil
		}},
		{"Pipeline.Flow", func(l *isolationLogger) error {
			f := Pipeline([]int{1}).Stage("s", 1).Flow().Logger(l).Build(ctx, func(c context.Context, _ string, v int) (int, error) { core.LogCtxInfo(c, "local"); return v, nil })
			defer f.Close()
			if _, err := f.Submit(ctx, 1); err != nil {
				return err
			}
			f.Close()
			for range f.Results() {
			}
			return nil
		}},
		{"Retry", func(l *isolationLogger) error {
			_, err := Retry[int](ctx).Logger(l).Context(ctx).Execute(func(c context.Context) (int, error) { core.LogCtxInfo(c, "local"); return 1, nil })
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			global, local := &isolationLogger{}, &isolationLogger{}
			SetLogger(global)
			if err := tc.run(local); err != nil {
				t.Fatal(err)
			}
			if !local.contains("local") || global.contains("local") {
				t.Fatalf("execution logger not isolated: local=%v global=%v", local.messages, global.messages)
			}
			if GetLogger() != global {
				t.Fatal("execution changed global logger")
			}
		})
	}
}
func (l *isolationLogger) Log(_ context.Context, _ LogLevel, msg string, _ ...LogField) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, msg)
}
func (l *isolationLogger) With(...LogField) Logger                         { return l }
func (l *isolationLogger) WithContext(ctx context.Context) context.Context { return ctx }

// Every fluent logger entry must leave the application-wide logger unchanged,
// even before a terminal method is called.
func TestFluentLoggerDoesNotMutateGlobal(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	ctx := context.Background()
	builders := []struct {
		name  string
		value any
	}{
		{"Pool", Pool[int]()}, {"PoolMulti", PoolMulti[int]()},
		{"PoolSharded", PoolSharded[int]()}, {"Group", Group[int]()},
		{"GroupVoid", GroupVoid()}, {"GroupMulti", GroupMulti[int]()},
		{"GroupSharded", GroupSharded[int]()}, {"Task", Task[int]()},
		{"Bounded", NewBoundedRunnerBuilder()},
		{"Slice", Slice(ctx, []int{1})},
		{"Slice.Serial", Slice(ctx, []int{1}).Serial()},
		{"Slice.Parallel", Slice(ctx, []int{1}).Parallel()},
		{"Map", NewMapChain(ctx, map[int]int{1: 1})},
		{"Map.Serial", NewMapChain(ctx, map[int]int{1: 1}).Serial()},
		{"Map.Parallel", NewMapChain(ctx, map[int]int{1: 1}).Parallel()},
		{"Pipeline", Pipeline([]int{1})},
		{"Pipeline.Serial", Pipeline([]int{1}).Serial()},
		{"Pipeline.Parallel", Pipeline([]int{1}).Parallel()},
		{"Pipeline.Stream", Pipeline([]int{1}).Stream()},
		{"Pipeline.Flow", Pipeline([]int{1}).Stage("s", 1).Flow()},
		{"Pipeline.Pipe", Pipeline([]int{1}).Pipe()},
		{"Retry", Retry[int](ctx)}, {"Ratelimit", Ratelimit(ctx)},
		{"Ratelimit.RateLimiter", Ratelimit(ctx).RateLimiter()},
		{"Ratelimit.TokenBucket", Ratelimit(ctx).TokenBucket()},
		{"Ratelimit.SlidingWindow", Ratelimit(ctx).SlidingWindow()},
		{"Ratelimit.Adaptive", Ratelimit(ctx).Adaptive()},
		{"Ratelimit.Sharded", Ratelimit(ctx).Sharded()},
		{"Ratelimit.Sharded.RateLimiter", Ratelimit(ctx).Sharded().RateLimiter()},
		{"Ratelimit.Sharded.TokenBucket", Ratelimit(ctx).Sharded().TokenBucket()},
		{"Ratelimit.Sharded.SlidingWindow", Ratelimit(ctx).Sharded().SlidingWindow()},
		{"Ratelimit.Sharded.Adaptive", Ratelimit(ctx).Sharded().Adaptive()},
	}
	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			typ := reflect.TypeOf(b.value)
			names := []string{"Logger"}
			for i := 0; i < typ.NumMethod(); i++ {
				m := typ.Method(i)
				if strings.HasPrefix(m.Name, "Default") && m.Type.NumIn() == 1 {
					names = append(names, m.Name)
				}
			}
			for _, name := range names {
				m := reflect.ValueOf(b.value).MethodByName(name)
				if !m.IsValid() {
					continue
				}
				global := &isolationLogger{}
				SetLogger(global)
				before := configurationGlobals()
				var args []reflect.Value
				if name == "Logger" {
					args = []reflect.Value{reflect.ValueOf(&isolationLogger{})}
				}
				m.Call(args)
				if GetLogger() != global {
					t.Errorf("%s changed the application-wide logger", name)
				}
				if !reflect.DeepEqual(before, configurationGlobals()) {
					t.Errorf("%s changed application-wide defaults", name)
				}
			}
		})
	}
}

func configurationGlobals() []any {
	return []any{
		core.GetDefaultTimeout(), core.GetSubmitTimeoutValue(), core.GetMaxCleanupDuration(),
		core.GetDefaultMaxResults(), core.GetDefaultRingBufferCap(), core.GetDefaultOverflowStrategy(),
		core.GetTaskFailLogLevel(), core.GetTraceLogEnabled(), core.GetTraceIDKey(),
	}
}

func TestFluentLoggerBoundedAndVoidExecution(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	global, local := &isolationLogger{}, &isolationLogger{}
	SetLogger(global)
	ctx := context.Background()
	fn := func(c context.Context) (int, error) { core.LogCtxInfo(c, "bounded"); return 7, nil }
	b := NewBoundedRunnerBuilder().Max(1).Logger(local).Context(ctx)
	r := b.Build()
	b.DefaultLogger()
	if v, err := task.BoundedGo(r, ctx, fn).Wait(); v != 7 || err != nil {
		t.Fatalf("BoundedGo: %d, %v", v, err)
	}
	if v, err := task.BoundedGoResult(r, ctx, fn).Result(); v != 7 || err != nil {
		t.Fatalf("BoundedGoResult: %d, %v", v, err)
	}
	if _, err := task.BoundedGoAct(r, ctx, func(c context.Context) error { core.LogCtxInfo(c, "bounded"); return nil }).Wait(); err != nil {
		t.Fatal(err)
	}
	if err := GroupVoid().Worker(1).Logger(local).Run(func(_ context.Context, g *GroupNoResult) error {
		err := g.Go(ctx, func(c context.Context) error { core.LogCtxInfo(c, "void"); return nil })
		g.Wait()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !local.contains("bounded") || !local.contains("void") || global.contains("bounded") || global.contains("void") {
		t.Fatal("bounded or void execution discarded its local logger")
	}
}

func TestFluentLoggerResetPreservesOnlyInstanceScope(t *testing.T) {
	old := GetLogger()
	defer SetLogger(old)
	global, local := &isolationLogger{}, &isolationLogger{}
	SetLogger(global)
	g := Group[int]().Worker(1).Logger(local).Build()
	defer g.Close()
	g.Wait()
	if _, err := g.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := g.Go(context.Background(), func(c context.Context) (int, error) { core.LogCtxInfo(c, "group-reset"); return 1, nil }); err != nil {
		t.Fatal(err)
	}
	g.Wait()
	if err := Pool[int]().Worker(1).Logger(local).Run(func(_ context.Context, p *PoolInstance[int]) error {
		p.Wait()
		if _, err := p.Reset(); err != nil {
			return err
		}
		err := p.Submit(context.Background(), func(c context.Context) (int, error) { core.LogCtxInfo(c, "pool-reset"); return 1, nil })
		p.Wait()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !local.contains("group-reset") || !local.contains("pool-reset") || global.contains("group-reset") || global.contains("pool-reset") {
		t.Fatal("Reset discarded the instance logger")
	}
}
