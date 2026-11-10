"""Execute the verbatim concurrent Wait, Acquire and strategy examples.

Requires Python 3 and Go 1.25+; optional Go test flags override the default
25 race runs. Only imports, function scopes and application stubs are added.
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

source = '''package auditratelimitwait
import (
    "context"
    "errors"
    "fmt"
    "log"
    "sync"
    "testing"
    "testing/synctest"
    "time"
    "github.com/chichengyu/async"
)
var _=log.Printf
var _=fmt.Errorf
type contextKey struct{}
var mu sync.Mutex
var calls int
var contexts []context.Context
var failures []error
var returned int
var mode string
var target int
var peak bool
var cancelParent context.CancelFunc
var gate chan struct{}
func isPeakHour() bool { return peak }
func doRequest(ctx context.Context) error {
    mu.Lock()
    calls++; index:=calls
    contexts=append(contexts,ctx)
    var failure error
    if mode=="error" && (target==0 || index==target) {
        failure=fmt.Errorf("unique business failure %d",index)
        failures=append(failures,failure)
    }
    mu.Unlock()
    switch mode {
    case "cancel-gated-error":
        <-gate
        failure=fmt.Errorf("accepted business failure %d",index)
        mu.Lock(); failures=append(failures,failure); mu.Unlock()
        failure=errors.Join(failure,ctx.Err())
    case "cancel-gated-success": <-gate
    case "cancel-error":
        if index==target { cancelParent(); failure=fmt.Errorf("canceling business failure %d",index); mu.Lock(); failures=append(failures,failure);mu.Unlock() }
    case "cancel-success": if index==target { cancelParent() }
    }
    mu.Lock(); returned++; mu.Unlock()
    if failure!=nil { return failure }
    if mode=="cancel-success" || mode=="cancel-gated-success" { return nil }
    return ctx.Err()
}
'''
for name, heading in [('waitBatch', '### Wait\n'), ('acquire', '### Acquire\n'),
                      ('strategy', '### WithStrategy\n')]:
    source += 'func ' + name + '(ctx context.Context) error {\n'
    source += block(heading) + '\nreturn nil\n}\n'
source += '''
func reset(ctx context.Context, cancel context.CancelFunc, scenario string, step int) {
    calls,returned,contexts,failures=0,0,nil,nil
    cancelParent,mode,target=cancel,scenario,step
    gate=make(chan struct{})
}
func TestOriginalWaitBatch(t *testing.T) {
    for _,scenario:=range []string{"success","error","pre-canceled","cancel-gated-error","cancel-gated-success"} {
        t.Run(scenario,func(t *testing.T){
            synctest.Test(t,func(t *testing.T){
                ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel()
                reset(ctx,cancel,scenario,0)
                if scenario=="pre-canceled" { cancel() }
                done:=make(chan error,1)
                go func(){ done<-waitBatch(ctx) }()
                if scenario=="cancel-gated-error" || scenario=="cancel-gated-success" {
                    synctest.Wait()
                    if calls!=10 || returned!=0 { t.Fatalf("before cancel calls=%d returned=%d",calls,returned) }
                    cancel();synctest.Wait()
                    select { case err:=<-done: t.Fatalf("returned before accepted business completed: %v",err); default: }
                    close(gate)
                }
                err:=<-done
                if calls==0 || calls!=returned { t.Fatalf("calls=%d returned=%d",calls,returned) }
                switch scenario {
                case "success": if err!=nil || calls!=100 { t.Fatalf("success err=%v calls=%d",err,calls) }
                case "error": if calls!=100 || len(failures)!=100 { t.Fatalf("calls=%d failures=%d",calls,len(failures)) }
                case "pre-canceled": if !errors.Is(err,context.Canceled) { t.Fatalf("cancellation lost: %v",err) }
                default:
                    if calls!=10 || !errors.Is(err,context.Canceled) { t.Fatalf("admission cancellation lost: err=%v calls=%d",err,calls) }
                }
                for _,failure:=range failures { if !errors.Is(err,failure) { t.Fatalf("accepted business error lost: %v in %v",failure,err) } }
                for _,seen:=range contexts { if seen!=ctx || seen.Value(contextKey{})!=17 { t.Fatal("business context lost") } }
            })
        })
    }
}
func TestOriginalSequentialPermits(t *testing.T) {
    for _,kind:=range []string{"acquire","strategy"} {
        for _,peakHour:=range []bool{false,true} {
            for _,scenario:=range []string{"success","error","pre-canceled","cancel-error","cancel-success"} {
                for step:=1;step<=2;step++ {
                    if kind=="strategy" && step==2 { continue }
                    if (scenario=="success" || scenario=="pre-canceled") && step!=1 { continue }
                    t.Run(fmt.Sprintf("%s/peak=%v/%s/%d",kind,peakHour,scenario,step),func(t *testing.T){
                        synctest.Test(t,func(t *testing.T){
                            ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel()
                            reset(ctx,cancel,scenario,step);peak=peakHour
                            if scenario=="pre-canceled" { cancel() }
                            var err error
                            if kind=="acquire" { err=acquire(ctx) } else { err=strategy(ctx) }
                            wantCalls:=2
                            if kind=="strategy" { wantCalls=1 }
                            if scenario=="error" || scenario=="cancel-error" { wantCalls=step }
                            if scenario=="pre-canceled" { wantCalls=1 }
                            if calls!=wantCalls || returned!=calls { t.Fatalf("calls=%d returned=%d want=%d",calls,returned,wantCalls) }
                            if scenario=="success" || scenario=="cancel-success" { if err!=nil { t.Fatalf("successful business rewritten: %v",err) } }
                            if scenario=="pre-canceled" && !errors.Is(err,context.Canceled) { t.Fatalf("pre-cancel lost: %v",err) }
                            for _,failure:=range failures { if !errors.Is(err,failure) { t.Fatalf("business error lost: %v in %v",failure,err) } }
                            if len(failures)==1 && err!=failures[0] { t.Fatal("business error identity changed") }
                            for _,seen:=range contexts { if seen!=ctx || seen.Value(contextKey{})!=17 { t.Fatal("business context lost") } }
                        })
                    })
                }
            }
        }
    }
}
'''
folder = root / '.gotmp/ratelimit_wait_workflow'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
flags = sys.argv[1:] or ['-race', '-count=25', '-timeout=60s']
result = subprocess.run(['go','test','./.gotmp/ratelimit_wait_workflow', *flags],
    cwd=root, capture_output=True, text=True, encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
