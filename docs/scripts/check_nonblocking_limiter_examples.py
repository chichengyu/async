"""Execute nonblocking limiter Markdown with typed business/error fixtures.

Python 3 and Go 1.25+ are required. Defaults to 25 race runs; optional
arguments override Go test flags. Statements are extracted without rewriting.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/ratelimit.md').read_text(encoding='utf-8')
fence = chr(96) * 3

def block(section, heading):
    start = markdown.index(section)
    start = markdown.index(heading, start)
    start = markdown.index(fence + 'go', start) + len(fence + 'go')
    return markdown[start:markdown.index(fence, start)]

cases = [
    ('bucketAllow', '## TokenBucket（', '### Allow\n', 'tb *async.TokenBucket', 1, 2),
    ('bucketBatch', '## TokenBucket（', '### AllowN\n', 'tb *async.TokenBucket', 10, 20),
    ('windowAllow', '## SlidingWindow（', '### Allow\n', 'sw *async.SlidingWindowRateLimiter', 1, 2),
    ('windowBatch', '## SlidingWindow（', '### AllowN\n', 'sw *async.SlidingWindowRateLimiter', 5, 10),
    ('shardedBatch', '## 分片限流器\n', 'fmt.Println(sw.ShardCount(), sw.TotalLimit())',
     'sw *async.ShardedSlidingWindowRateLimiter', 5, 25),
]
source = '''package auditnonblocking
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
var _=fmt.Println
var _=time.Second
type contextKey struct{}
var errRateLimited=errors.New("application admission denied")
var businessError=errors.New("application business failure")
var panicValue=errors.New("application panic")
var cancelParent context.CancelFunc
var mode string
var calls int
var seen context.Context
func business(ctx context.Context) error {
    calls++;seen=ctx
    switch mode {
    case "error": return businessError
    case "cancel-error": cancelParent(); return businessError
    case "cancel-success": cancelParent(); return nil
    case "wait-cancel": <-ctx.Done(); return ctx.Err()
    case "panic": panic(panicValue)
    case "goexit": runtime.Goexit()
    }
    return ctx.Err()
}
func doRequest(ctx context.Context) error { return business(ctx) }
func batchProcess(ctx context.Context) error { return business(ctx) }
func doBatchRequest(ctx context.Context) error { return business(ctx) }
'''
for name, section, heading, parameter, cost, capacity in cases:
    if name == 'shardedBatch':
        start = markdown.index(heading)
        body = markdown[start:markdown.index(fence, start)]
    else:
        body = block(section, heading)
    source += 'func ' + name + '(ctx context.Context, ' + parameter + ') error {\n'
    source += '{\n' + body + '\n}\nreturn nil\n}\n'
source += '''
type outcome struct { err error; panicValue any; returned bool }
func invoke(fn func() error) <-chan outcome {
    done:=make(chan outcome,1)
    go func(){
        result:=outcome{}
        defer func(){ result.panicValue=recover(); done<-result }()
        result.err=fn();result.returned=true
    }()
    return done
}
func TestOriginalNonblockingOutcome(t *testing.T) {
    for _,kind:=range []string{"bucketAllow","bucketBatch","windowAllow","windowBatch","shardedBatch"} {
        for _,scenario:=range []string{"success","error","pre-canceled","cancel-error","cancel-success","wait-cancel","deadline","panic","goexit","rejected","rejected-canceled"} {
            t.Run(kind+"/"+scenario,func(t *testing.T){
                synctest.Test(t,func(t *testing.T){
                    mode,calls,seen=scenario,0,nil
                    ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel()
                    cancelParent=cancel
                    if scenario=="pre-canceled" || scenario=="rejected-canceled" { cancel() }
                    if scenario=="deadline" {
                        var stop context.CancelFunc
                        ctx,stop=context.WithTimeout(ctx,3*time.Second);defer stop()
                        mode="wait-cancel"
                    }
                    tb:=async.Ratelimit(ctx).TokenBucket().Rate(1).Capacity(20).Build()
                    sw:=async.Ratelimit(ctx).SlidingWindow().Limit(10).Window(time.Hour).Build()
                    sh:=async.Ratelimit(ctx).Sharded().SlidingWindow().Shards(4).Limit(100).Window(time.Hour).Build()
                    rejected:=scenario=="rejected" || scenario=="rejected-canceled"
                    if rejected {
                        if !tb.AllowN(20) || !sw.AllowN(10) { t.Fatal("setup failed") }
                        for i:=0;i<4;i++ { if !sh.GetShard(i).AllowN(25) { t.Fatal("shard setup failed") } }
                    }
                    fn:=func() error {
                        switch kind {
                        case "bucketAllow":return bucketAllow(ctx,tb)
                        case "bucketBatch":return bucketBatch(ctx,tb)
                        case "windowAllow":return windowAllow(ctx,sw)
                        case "windowBatch":return windowBatch(ctx,sw)
                        default:return shardedBatch(ctx,sh)
                        }
                    }
                    begin:=time.Now()
                    done:=invoke(fn)
                    if scenario=="wait-cancel" { synctest.Wait();if calls!=1 { t.Fatal("business never started") };cancel() }
                    got:=<-done
                    var want error
                    wantCalls:=1
                    switch scenario {
                    case "error","cancel-error": want=businessError
                    case "pre-canceled","wait-cancel":want=context.Canceled
                    case "deadline":want=context.DeadlineExceeded
                    case "rejected","rejected-canceled":want=errRateLimited;wantCalls=0
                    }
                    if scenario=="panic" {
                        if got.returned || got.panicValue!=panicValue { t.Fatalf("panic outcome=%+v",got) }
                    } else if scenario=="goexit" {
                        if got.returned || got.panicValue!=nil { t.Fatalf("Goexit outcome=%+v",got) }
                    } else if !got.returned || got.panicValue!=nil || got.err!=want { t.Fatalf("outcome=%+v want=%v",got,want) }
                    if calls!=wantCalls { t.Fatalf("business calls=%d want=%d",calls,wantCalls) }
                    if calls>0 && (seen!=ctx || seen.Value(contextKey{})!=17) { t.Fatal("business context lost") }
                    elapsed:=time.Duration(0);if scenario=="deadline" { elapsed=3*time.Second }
                    if time.Since(begin)!=elapsed { t.Fatalf("elapsed=%v want=%v",time.Since(begin),elapsed) }
                })
            })
        }
    }
}
func TestOriginalNonblockingNoRefund(t *testing.T) {
    for _,kind:=range []string{"bucketAllow","bucketBatch","windowAllow","windowBatch","shardedBatch"} {
        for _,scenario:=range []string{"success","error","pre-canceled","cancel-error","cancel-success","panic","goexit"} {
            t.Run(kind+"/"+scenario,func(t *testing.T){
                synctest.Test(t,func(t *testing.T){
                    mode,calls,seen=scenario,0,nil
                    ctx,cancel:=context.WithCancel(context.Background());defer cancel();cancelParent=cancel
                    if scenario=="pre-canceled" { cancel() }
                    tb:=async.Ratelimit(ctx).TokenBucket().Rate(1).Capacity(20).Build()
                    sw:=async.Ratelimit(ctx).SlidingWindow().Limit(10).Window(time.Hour).Build()
                    sh:=async.Ratelimit(ctx).Sharded().SlidingWindow().Shards(4).Limit(100).Window(time.Hour).Build()
                    accepted:=0
                    switch kind {
                    case "bucketAllow":accepted=20
                    case "bucketBatch":accepted=2
                    case "windowAllow":accepted=10
                    case "windowBatch":accepted=2
                    case "shardedBatch":accepted=20
                    }
                    fn:=func() error {
                        switch kind {
                        case "bucketAllow":return bucketAllow(ctx,tb)
                        case "bucketBatch":return bucketBatch(ctx,tb)
                        case "windowAllow":return windowAllow(ctx,sw)
                        case "windowBatch":return windowBatch(ctx,sw)
                        default:return shardedBatch(ctx,sh)
                        }
                    }
                    for i:=0;i<accepted;i++ { <-invoke(fn) }
                    if calls!=accepted { t.Fatalf("accepted calls=%d want=%d",calls,accepted) }
                    mode="success"
                    got:=<-invoke(fn)
                    if got.err!=errRateLimited || calls!=accepted { t.Fatalf("spent quota refunded/reset: err=%v calls=%d want=%d",got.err,calls,accepted) }
                })
            })
        }
    }
}
func TestOriginalNonblockingRestoration(t *testing.T) {
    for _,kind:=range []string{"bucketAllow","bucketBatch","windowAllow","windowBatch","shardedBatch"} {
        t.Run(kind,func(t *testing.T){
            synctest.Test(t,func(t *testing.T){
                mode,calls,seen="success",0,nil
                ctx,cancel:=context.WithCancel(context.Background());defer cancel();cancelParent=cancel
                tb:=async.Ratelimit(ctx).TokenBucket().Rate(1).Capacity(20).Build()
                sw:=async.Ratelimit(ctx).SlidingWindow().Limit(10).Window(time.Hour).Build()
                sh:=async.Ratelimit(ctx).Sharded().SlidingWindow().Shards(4).Limit(100).Window(time.Hour).Build()
                fn:=func() error {
                    switch kind {
                    case "bucketAllow":return bucketAllow(ctx,tb)
                    case "bucketBatch":return bucketBatch(ctx,tb)
                    case "windowAllow":return windowAllow(ctx,sw)
                    case "windowBatch":return windowBatch(ctx,sw)
                    default:return shardedBatch(ctx,sh)
                    }
                }
                if !tb.AllowN(20) || !sw.AllowN(10) { t.Fatal("setup failed") }
                for i:=0;i<4;i++ { if !sh.GetShard(i).AllowN(25) { t.Fatal("setup failed") } }
                if err:=fn();err!=errRateLimited || calls!=0 { t.Fatalf("initial rejection err=%v calls=%d",err,calls) }
                interval:=time.Hour
                if kind=="bucketAllow" { interval=time.Second }
                if kind=="bucketBatch" { interval=10*time.Second }
                time.Sleep(interval-time.Nanosecond)
                if err:=fn();err!=errRateLimited || calls!=0 { t.Fatalf("restored too early: err=%v calls=%d",err,calls) }
                time.Sleep(time.Nanosecond)
                if err:=fn();err!=nil || calls!=1 { t.Fatalf("not restored at boundary: err=%v calls=%d",err,calls) }
            })
        })
    }
}
'''
folder = root / '.gotmp/nonblocking_limiter_workflow'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
flags = sys.argv[1:] or ['-race', '-count=25', '-timeout=60s']
result = subprocess.run(['go','test','./.gotmp/nonblocking_limiter_workflow',*flags],
    cwd=root,capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
