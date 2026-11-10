"""Run verbatim limiter production/basic/README Markdown against business outcomes.

Python 3 and Go 1.25+ on PATH are required. Defaults to 25 race runs; optional
Go test flags follow the script name (e.g. -count=100 instead of the defaults).
Only imports, surrounding function signatures, application inputs and business
stubs are supplied. Markdown statements are never rewritten by this script.
The README Adaptive business suffix is also exercised with a shared limiter
to observe permit cleanup; it is not counted as an additional Markdown block.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/ratelimit.md').read_text(encoding='utf-8')
fence = chr(96) * 3

def block(heading):
    start = markdown.index(heading)
    start = markdown.index(fence + 'go', start) + len(fence + 'go')
    return markdown[start:markdown.index(fence, start)]

source = '''package auditratelimitproduction
import (
    "context"
    "errors"
    "fmt"
    "runtime"
    "testing"
    "testing/synctest"
    "time"
    "github.com/chichengyu/async"
)
type contextKey struct{}
var businessError=errors.New("downstream business failure")
var businessPanic=errors.New("downstream panic")
var mode string
var calls int
var seen context.Context
var cancelParent context.CancelFunc
func doRequest(ctx context.Context) error {
    calls++
    seen=ctx
    switch mode {
    case "error": return businessError
    case "cancel-error": cancelParent(); return businessError
    case "cancel-success": cancelParent(); return nil
    case "wait-cancel": <-ctx.Done(); return ctx.Err()
    case "panic": panic(businessPanic)
    case "goexit": runtime.Goexit()
    }
    return ctx.Err()
}
func production(ctx context.Context) error {
''' + block('### RateLimiter 生产推荐配置') + '\nreturn nil\n}\n'
source += 'func adaptive(ctx context.Context, al *async.AdaptiveRateLimiter) error {\n'
source += block('### RecordOk / RecordFailure') + '\nreturn nil\n}\n'
readme = (root / 'README.md').read_text(encoding='utf-8')
start = readme.index('### Adaptive\n')
start = readme.index(fence + 'go', start) + len(fence + 'go')
readme_body = readme[start:readme.index(fence, start)]
source += 'func readmeAdaptive(ctx context.Context) error {\n' + readme_body + '\nreturn nil\n}\n'
shared_start = readme_body.index('if err := al.Acquire(ctx)')
source += 'func readmeShared(ctx context.Context, al *async.AdaptiveRateLimiter) error {\n'
source += readme_body[shared_start:] + '\nreturn nil\n}\n'
for name, heading, sharded in [
    ('waitMode', '#### 模式一：', False),
    ('tokenMode', '#### 模式二：', False),
    ('rejectMode', '#### 模式三：', False),
    ('releaseMode', '### Release\n', False),
    ('traceMode', '### WithTraceID\n', False),
    ('shardedMode', '## 分片限流器\n', True),
]:
    # The first block under the Sharded heading builds four types; use the
    # later usage block, whose first statement uniquely identifies its scope.
    if sharded:
        marker = 'srl := async.Ratelimit(ctx).Sharded().RateLimiter().Shards(16).Rate(10000).Per(time.Second).Build()'
        start = markdown.index(marker)
        body = markdown[start:markdown.index(fence, start)]
    else:
        body = block(heading)
    source += 'func ' + name + '(ctx context.Context) error {\n'
    if name != 'waitMode' and not sharded:
        source += 'rl:=async.Ratelimit(ctx).RateLimiter().Rate(10).Per(time.Hour).Build(); defer rl.Close()\n'
    source += body + '\nreturn nil\n}\n'
source += '''
type outcome struct { err error; panicValue any; returned bool }
func invoke(fn func() error) <-chan outcome {
    done:=make(chan outcome,1)
    go func(){
        result:=outcome{}
        defer func(){ result.panicValue=recover(); done<-result }()
        result.err=fn(); result.returned=true
    }()
    return done
}
func TestOriginalLimiterBusiness(t *testing.T) {
    for _,kind:=range []string{"production","adaptive","readmeAdaptive","readmeShared","waitMode","tokenMode","rejectMode","releaseMode","traceMode","shardedMode"} {
        for _,scenario:=range []string{"success","error","pre-canceled","cancel-error","cancel-success","wait-cancel","deadline","panic","goexit"} {
            t.Run(kind+"/"+scenario,func(t *testing.T){
                synctest.Test(t,func(t *testing.T){
                    mode,calls,seen=scenario,0,nil
                    base:=context.WithValue(context.Background(),contextKey{},17)
                    ctx,cancel:=context.WithCancel(base)
                    cancelParent=cancel; defer cancel()
                    if scenario=="pre-canceled" { cancel() }
                    if scenario=="deadline" {
                        var stop context.CancelFunc
                        ctx,stop=context.WithTimeout(ctx,3*time.Second); defer stop()
                        mode="wait-cancel"
                    }
                    al:=async.Ratelimit(ctx).Adaptive().MinWorker(1).MaxWorker(9).Build()
                    fn:=func() error {
                        switch kind {
                        case "production": return production(ctx)
                        case "readmeAdaptive": return readmeAdaptive(ctx)
                        case "readmeShared": return readmeShared(ctx,al)
                        case "waitMode": return waitMode(ctx)
                        case "tokenMode": return tokenMode(ctx)
                        case "rejectMode": return rejectMode(ctx)
                        case "releaseMode": return releaseMode(ctx)
                        case "traceMode": return traceMode(ctx)
                        case "shardedMode": return shardedMode(ctx)
                        default: return adaptive(ctx,al)
                        }
                    }
                    start:=time.Now()
                    done:=invoke(fn)
                    if scenario=="wait-cancel" {
                        synctest.Wait()
                        if calls!=1 { t.Fatal("business never started") }
                        cancel()
                    }
                    got:=<-done
                    var want error
                    switch scenario {
                    case "error","cancel-error": want=businessError
                    case "pre-canceled","wait-cancel": want=context.Canceled
                    case "deadline": want=context.DeadlineExceeded
                    }
                    if scenario=="panic" {
                        if got.returned || got.panicValue!=businessPanic { t.Fatalf("panic outcome=%+v",got) }
                    } else if scenario=="goexit" {
                        if got.returned || got.panicValue!=nil { t.Fatalf("Goexit outcome=%+v",got) }
                    } else if !got.returned || got.panicValue!=nil || got.err!=want {
                        t.Fatalf("business outcome=%+v want=%v",got,want)
                    }
                    if calls!=1 || seen==nil || seen.Value(contextKey{})!=17 { t.Fatalf("calls=%d context lost",calls) }
                    if kind=="traceMode" {
                        if seen.Done()!=ctx.Done() || async.GetTraceID(seen)=="" { t.Fatal("trace or cancellation context lost") }
                    } else if seen!=ctx { t.Fatal("context identity lost") }
                    elapsed:=time.Duration(0); if scenario=="deadline" { elapsed=3*time.Second }
                    if time.Since(start)!=elapsed { t.Fatalf("elapsed=%v want=%v",time.Since(start),elapsed) }
                    if kind=="adaptive" || kind=="readmeShared" {
                        // No wall time/ticker can restore an Adaptive permit. Exhaust
                        // the fixed initial capacity, then require the next wait to fail.
                        canceled,stop:=context.WithCancel(context.Background()); stop()
                        for i:=0;i<5;i++ { if err:=al.Acquire(canceled);err!=nil { t.Fatalf("permit %d leaked: %v",i,err) } }
                        if err:=al.Acquire(canceled);!errors.Is(err,context.Canceled) { t.Fatalf("extra permit: %v",err) }
                        for i:=0;i<5;i++ { al.Release() }
                    }
                })
            })
        }
    }
}
func TestOriginalAdaptiveFeedback(t *testing.T) {
    for _,scenario:=range []string{"success","error","pre-canceled","cancel-success","cancel-error","panic","goexit"} {
        t.Run(scenario,func(t *testing.T){
            synctest.Test(t,func(t *testing.T){
                mode,calls,seen=scenario,0,nil
                ctx,cancel:=context.WithCancel(context.Background()); defer cancel()
                cancelParent=cancel
                if scenario=="pre-canceled" { cancel() }
                al:=async.Ratelimit(ctx).Adaptive().MinWorker(1).MaxWorker(9).Build()
                for i:=0;i<10;i++ {
                    result:=<-invoke(func() error { return adaptive(ctx,al) })
                    var want error
                    if scenario=="error" || scenario=="cancel-error" { want=businessError }
                    if scenario=="pre-canceled" { want=context.Canceled }
                    if scenario=="panic" {
                        if result.returned || result.panicValue!=businessPanic { t.Fatalf("panic outcome=%+v",result) }
                    } else if scenario=="goexit" {
                        if result.returned || result.panicValue!=nil { t.Fatalf("Goexit outcome=%+v",result) }
                    } else if !result.returned || result.err!=want { t.Fatalf("call %d: err=%v want=%v",i,result.err,want) }
                }
                capacity:=2
                if scenario=="success" || scenario=="cancel-success" { capacity=6 }
                if scenario=="panic" || scenario=="goexit" { capacity=5 }
                canceled,stop:=context.WithCancel(context.Background()); stop()
                for i:=0;i<capacity;i++ { if err:=al.Acquire(canceled);err!=nil { t.Fatalf("capacity %d: %v",i,err) } }
                if err:=al.Acquire(canceled);!errors.Is(err,context.Canceled) { t.Fatalf("feedback capacity mismatch: %v",err) }
                for i:=0;i<capacity;i++ { al.Release() }
                if calls!=10 { t.Fatal(fmt.Sprint("calls=",calls)) }
            })
        })
    }
}
'''
source += '''
func TestOriginalAdaptiveCanceledAdmission(t *testing.T) {
    synctest.Test(t,func(t *testing.T){
        mode,calls,seen="success",0,nil
        al:=async.Ratelimit(context.Background()).Adaptive().MinWorker(1).MaxWorker(9).Build()
        for i:=0;i<5;i++ { if err:=al.Acquire(context.Background());err!=nil { t.Fatal(err) } }
        ctx,cancel:=context.WithCancel(context.Background()); cancel()
        cancelParent=cancel
        if err:=adaptive(ctx,al);!errors.Is(err,context.Canceled) { t.Fatalf("admission err=%v",err) }
        if calls!=0 { t.Fatal("rejected admission invoked business") }
        // Rejected calls must not add feedback or release another call's permit.
        if err:=al.Acquire(ctx);!errors.Is(err,context.Canceled) { t.Fatalf("rejected call returned permit: %v",err) }
        for i:=0;i<5;i++ { al.Release() }
        mode="success"
        for i:=0;i<9;i++ { if err:=adaptive(context.Background(),al);err!=nil { t.Fatal(err) } }
        for i:=0;i<5;i++ { if err:=al.Acquire(ctx);err!=nil { t.Fatalf("early adjustment: %v",err) } }
        if err:=al.Acquire(ctx);!errors.Is(err,context.Canceled) { t.Fatalf("admission failure counted as feedback: %v",err) }
        for i:=0;i<5;i++ { al.Release() }
    })
}
'''
folder = root / '.gotmp/ratelimit_production_workflow'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
flags = sys.argv[1:] or ['-race', '-count=25', '-timeout=60s']
result = subprocess.run(['go', 'test', './.gotmp/ratelimit_production_workflow', *flags],
    cwd=root, capture_output=True, text=True, encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
