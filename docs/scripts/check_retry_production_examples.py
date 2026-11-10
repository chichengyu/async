"""Execute the verbatim Retry production workflow against controlled outcomes.

Requires Python 3 and a race-capable Go 1.25+ toolchain on PATH:
    python docs/scripts/check_retry_production_examples.py

Only imports, application inputs, typed service stubs and unused-value sinks
are supplied. The Markdown workflow is copied without modifying its statements.
"""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/retry.md').read_text(encoding='utf-8')
budget_rows = {
    '标准 RPC': ('4', '1.4s', '5.2s'),
    '本次执行 RateLimiter': ('11', '151.5s', '10.5s'),
    '本次执行 Adaptive': ('6', '3.1s', '3.1s'),
    '幂等写入': ('11', '303s', '31s'),
}
for name, expected in budget_rows.items():
    rows = [line for line in markdown.splitlines() if line.startswith('| ' + name + ' |')]
    assert len(rows) == 1, (name, rows)
    actual = tuple(cell.strip() for cell in rows[0].split('|')[2:-1])
    assert actual == expected, (name, actual, expected)
fence = chr(96) * 3
start = markdown.index('### 生产推荐配置')
start = markdown.index(fence + 'go', start) + len(fence + 'go')
block = markdown[start:markdown.index(fence, start)]
source = '''package auditretryproduction
import (
    "context"
    "errors"
    "slices"
    "testing"
    "testing/synctest"
    "time"
    "github.com/chichengyu/async"
)
type Data struct{ Stage int }
type service struct{ stage int }
type database struct{}
type contextKey struct{}
var rpcClient=service{0}
var rateLimitedAPI=service{1}
var unstableSvc=service{2}
var db database
var req=17
var key="idempotent-key"
var businessError=errors.New("production business failure")
var mode string
var target int
var cancelParent context.CancelFunc
var calls [4]int
var rpcResult *Data
var seenContexts []context.Context
var starts [4][]time.Duration
var begin time.Time
func invoke(ctx context.Context, stage int) (*Data,error) {
    if ctx.Value(contextKey{})!=req { panic("lost parent context value") }
    calls[stage]++
    starts[stage]=append(starts[stage],time.Since(begin))
    seenContexts=append(seenContexts,ctx)
    if stage==target {
        switch mode {
        case "failure": return &Data{stage},businessError
        case "recover": if calls[stage]==1 { return &Data{stage},businessError }
        case "per-call": if calls[stage]==1 { <-ctx.Done(); return &Data{stage},ctx.Err() }
        case "cancel-error": cancelParent(); return &Data{stage},businessError
        case "cancel-success": cancelParent()
        }
    }
    return &Data{stage},nil
}
func (s service) Query(ctx context.Context, request int) (*Data,error) {
    if request!=req { panic("wrong request") }
    data,err:=invoke(ctx,s.stage)
    if s.stage==0 && err==nil { rpcResult=data }
    return data,err
}
func (database) WriteWithIdempotentKey(ctx context.Context, gotKey string, data *Data) error {
    if gotKey!=key || data==nil || data!=rpcResult { panic("write lost RPC result or idempotent key") }
    _,err:=invoke(ctx,3)
    return err
}
func run(ctx context.Context) error {
''' + block + '\n_, _, _ = rpcData, limitedData, adaptiveData\nreturn nil\n}\n'
source += '''
func TestOriginalRetryProductionWorkflow(t *testing.T) {
    timeouts:=[]time.Duration{5*time.Second,10*time.Second,3*time.Second,30*time.Second}
    initial:=[]time.Duration{200*time.Millisecond,500*time.Millisecond,100*time.Millisecond,time.Second}
    attempts:=[]int{4,11,6,11}
    failureElapsed:=[]time.Duration{1400*time.Millisecond,151500*time.Millisecond,3100*time.Millisecond,303*time.Second}
    for _, scenario:=range []string{"success","failure","recover","per-call","cancel-error","cancel-success","pre-canceled"} {
        for stage:=range 4 {
            if (scenario=="success" || scenario=="pre-canceled") && stage!=0 { continue }
            t.Run(scenario+string(rune('0'+stage)),func(t *testing.T) {
                synctest.Test(t,func(t *testing.T) {
                    mode,target=scenario,stage
                    calls=[4]int{}; seenContexts=nil; starts=[4][]time.Duration{}; rpcResult=nil
                    parent:=context.WithValue(context.Background(),contextKey{},req)
                    ctx,cancel:=context.WithCancel(parent)
                    cancelParent=cancel
                    defer cancel()
                    if scenario=="pre-canceled" { cancel() }
                    begin=time.Now()
                    err:=run(ctx)
                    want:=[4]int{1,1,1,1}
                    var wantErr error
                    var elapsed time.Duration
                    switch scenario {
                    case "failure":
                        wantErr=businessError; want[stage]=attempts[stage]; elapsed=failureElapsed[stage]
                        for i:=stage+1;i<4;i++ { want[i]=0 }
                    case "recover": want[stage]=2; elapsed=initial[stage]
                    case "per-call": want[stage]=2; elapsed=timeouts[stage]+initial[stage]
                    case "cancel-error","cancel-success":
                        if scenario=="cancel-error" || stage<3 { wantErr=context.Canceled }
                        for i:=stage+1;i<4;i++ { want[i]=0 }
                    case "pre-canceled": want=[4]int{}; wantErr=context.Canceled
                    }
                    if !errors.Is(err,wantErr) || calls!=want || time.Since(begin)!=elapsed {
                        t.Fatalf("err=%v want=%v calls=%v want=%v elapsed=%v want=%v",err,wantErr,calls,want,time.Since(begin),elapsed)
                    }
                    for _, callCtx:=range seenContexts {
                        if callCtx.Err()!=context.Canceled && callCtx.Err()!=context.DeadlineExceeded { t.Fatal("per-call context retained") }
                    }
                    if scenario=="recover" || scenario=="per-call" {
                        firstGap:=initial[stage]
                        if scenario=="per-call" { firstGap+=timeouts[stage] }
                        if !slices.Equal(starts[stage],[]time.Duration{0,firstGap}) { t.Fatalf("starts=%v",starts[stage]) }
                    }
                    if scenario!="pre-canceled" && scenario!="cancel-error" && scenario!="cancel-success" && ctx.Err()!=nil { t.Fatal("execution canceled parent") }
                })
            })
        }
    }
}
'''
folder = root / '.gotmp/retry_production_workflow'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
result = subprocess.run(['go','test','-race','./.gotmp/retry_production_workflow',
    '-count=25','-timeout=60s'], cwd=root, capture_output=True, text=True, encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
