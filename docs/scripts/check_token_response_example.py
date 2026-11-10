"""Execute the verbatim Token/RPC/response Markdown under controlled outcomes.

Python 3 and Go 1.25+ are required; optional arguments override the default
25 race runs. Only function scope, imports, inputs and typed service stubs
are supplied. The extracted Markdown statements are never rewritten.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/ratelimit.md').read_text(encoding='utf-8')
fence = chr(96) * 3
start = markdown.index('### Token\n')
start = markdown.index(fence + 'go', start) + len(fence + 'go')
block = markdown[start:markdown.index(fence, start)]
source = '''package audittokenresponse
import (
    "context"
    "errors"
    "runtime"
    "testing"
    "testing/synctest"
    "time"
    "github.com/chichengyu/async"
)
type contextKey struct{}
type Response struct{ Request int }
var req=17
var rpcError=errors.New("RPC failure")
var responseError=errors.New("response processing failure")
var panicValue=errors.New("business panic")
var mode string
var target int
var calls [2]int
var seen [2]context.Context
var cancelParent context.CancelFunc
var rpcResponse *Response
var processed *Response
func perform(ctx context.Context, stage int) error {
    calls[stage]++;seen[stage]=ctx
    if stage==target {
        switch mode {
        case "error": if stage==0 { return rpcError }; return responseError
        case "cancel-error": cancelParent(); if stage==0 { return rpcError }; return responseError
        case "cancel-success":cancelParent();return nil
        case "wait-cancel":<-ctx.Done();return ctx.Err()
        case "panic":panic(panicValue)
        case "goexit":runtime.Goexit()
        }
    }
    return ctx.Err()
}
func callExternalAPI(ctx context.Context, request int) (*Response,error) {
    if request!=req { panic("request value lost") }
    response:=&Response{request}
    err:=perform(ctx,0)
    rpcResponse=response
    return response,err
}
func processResponse(ctx context.Context, response *Response) error {
    processed=response
    if response==nil || response!=rpcResponse { panic("response identity lost") }
    return perform(ctx,1)
}
func run(ctx context.Context) error {
''' + block + '\nreturn nil\n}\n'
source += '''
type outcome struct{ err error; panicValue any; returned bool }
func TestOriginalTokenResponse(t *testing.T) {
    for _,scenario:=range []string{"success","pre-canceled","error","cancel-error","cancel-success","wait-cancel","deadline","panic","goexit"} {
        for stage:=0;stage<2;stage++ {
            if (scenario=="success" || scenario=="pre-canceled") && stage>0 { continue }
            t.Run(scenario+string(rune('0'+stage)),func(t *testing.T){
                synctest.Test(t,func(t *testing.T){
                    mode,target,calls,seen=scenario,stage,[2]int{},[2]context.Context{}
                    rpcResponse,processed=nil,nil
                    ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},req));defer cancel()
                    cancelParent=cancel
                    if scenario=="pre-canceled" { cancel() }
                    if scenario=="deadline" {
                        var stop context.CancelFunc
                        ctx,stop=context.WithTimeout(ctx,3*time.Second);defer stop();mode="wait-cancel"
                    }
                    done:=make(chan outcome,1)
                    begin:=time.Now()
                    go func(){
                        result:=outcome{}
                        defer func(){ result.panicValue=recover();done<-result }()
                        result.err=run(ctx);result.returned=true
                    }()
                    if scenario=="wait-cancel" { synctest.Wait();if calls[stage]!=1 { t.Fatal("target did not start") };cancel() }
                    got:=<-done
                    wantCalls:=[2]int{1,1}
                    var want error
                    switch scenario {
                    case "pre-canceled":want=context.Canceled;wantCalls[1]=0
                    case "error","cancel-error":if stage==0 { want=rpcError } else { want=responseError }
                    case "cancel-success":if stage==0 { want=context.Canceled }
                    case "wait-cancel":want=context.Canceled
                    case "deadline":want=context.DeadlineExceeded
                    }
                    if stage==0 && scenario!="success" && scenario!="cancel-success" { wantCalls[1]=0 }
                    if scenario=="panic" {
                        if got.returned || got.panicValue!=panicValue { t.Fatalf("panic outcome=%+v",got) }
                    } else if scenario=="goexit" {
                        if got.returned || got.panicValue!=nil { t.Fatalf("Goexit outcome=%+v",got) }
                    } else if !got.returned || got.panicValue!=nil || got.err!=want { t.Fatalf("outcome=%+v want=%v",got,want) }
                    if calls!=wantCalls { t.Fatalf("calls=%v want=%v",calls,wantCalls) }
                    if calls[1]>0 && processed!=rpcResponse { t.Fatal("processing lost RPC response") }
                    for index,count:=range calls {
                        if count>0 && (seen[index]!=ctx || seen[index].Value(contextKey{})!=req) { t.Fatal("business context lost") }
                    }
                    elapsed:=time.Duration(0);if scenario=="deadline" { elapsed=3*time.Second }
                    if time.Since(begin)!=elapsed { t.Fatalf("elapsed=%v want=%v",time.Since(begin),elapsed) }
                })
            })
        }
    }
}
'''
folder = root / '.gotmp/token_response_workflow'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
flags = sys.argv[1:] or ['-race','-count=25','-timeout=60s']
result = subprocess.run(['go','test','./.gotmp/token_response_workflow',*flags],
    cwd=root,capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
