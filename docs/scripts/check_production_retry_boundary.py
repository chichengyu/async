"""Execute both independent calls from production.md's Retry boundary block.

The original retry count, one-second parent budget and backoff stay intact.
Only scopes, imports and business fixtures are supplied. Requires Go 1.25+.
"""
from pathlib import Path
import subprocess,sys
root=Path(__file__).resolve().parents[2]
md=(root/'docs/production.md').read_text(encoding='utf-8');fence=chr(96)*3
start=md.index('## Retry 关键边界');start=md.index(fence+'go',start)+len(fence+'go');body=md[start:md.index(fence,start)]
first,second=body.split('// 陷阱2：',1)
source='''package auditproductionretryboundary
import("context";"errors";"io";"log";"testing";"testing/synctest";"time";"github.com/chichengyu/async")
var _=errors.Join
var businessError=errors.New("business failed")
var panicValue=errors.New("business panic")
type contextKey struct{}
var mode string
var calls int
var seen context.Context
func fn()error{calls++;switch mode{case "error":return businessError;case "panic":panic(panicValue)};return nil}
func slowFn(ctx context.Context)error{
 calls++;seen=ctx
 if ctx.Value(contextKey{})!=17{panic("business context lost")}
 switch mode{case "error":return businessError;case "panic":panic(panicValue);case "wait","earlier-deadline":<-ctx.Done();return ctx.Err();case "late-success":<-ctx.Done();return nil}
 return nil
}
func run0(ctx context.Context)error{
'''+first+'\nreturn nil\n}\nfunc run1(parentCtx context.Context)error{\n// 陷阱2：'+second+'\nreturn nil\n}\n'
source+='''
func TestOriginalProductionRetryBoundary(t *testing.T){
 log.SetOutput(io.Discard)
 for index,run:=range []func(context.Context)error{run0,run1}{
  scenarios:=[]string{"success","error","panic"};if index==1{scenarios=append(scenarios,"pre-canceled","earlier-deadline","wait","late-success")}
  for _,scenario:=range scenarios{t.Run(string(rune('0'+index))+"/"+scenario,func(t *testing.T){synctest.Test(t,func(t *testing.T){
   mode,calls,seen=scenario,0,nil
   base:=context.WithValue(context.Background(),contextKey{},17);ctx,cancel:=context.WithCancel(base);defer cancel()
   if mode=="pre-canceled"{cancel()}
   budget:=time.Second;if mode=="earlier-deadline"{budget=250*time.Millisecond;var cleanup context.CancelFunc;ctx,cleanup=context.WithTimeout(base,budget);defer cleanup()}
   start:=time.Now();got:=run(ctx);synctest.Wait()
   if mode=="success"{if got!=nil||calls!=1{t.Fatalf("success error=%v calls=%d",got,calls)}}else if index==0{
    if calls!=1{t.Fatalf("zero retries calls=%d",calls)}
    if mode=="error"&&!errors.Is(got,businessError){t.Fatalf("business error lost: %v",got)}
    if mode=="panic"{var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=panicValue{t.Fatalf("panic lost: %v",got)}}
   }else{
    want:=error(context.DeadlineExceeded);expected:=1;if mode=="pre-canceled"{want=context.Canceled;expected=0}
    validCalls:=calls==expected;if (mode=="error"||mode=="panic")&&calls==2{validCalls=true}
    if !errors.Is(got,want)||!validCalls{t.Fatalf("budget error=%v calls=%d want=%v/%d",got,calls,want,expected)}
    if mode!="pre-canceled"&&time.Since(start)!=budget{t.Fatalf("budget elapsed=%v want=%v",time.Since(start),budget)}
   }
   if index==1&&seen!=nil{if seen.Err()==nil{t.Fatal("request context leaked")};deadline,ok:=seen.Deadline();if !ok||deadline.Sub(start)!=budget{t.Fatalf("business deadline=%v",deadline)}}
  })})}
 }
}
'''
folder=root/'.gotmp/production_retry_boundary';folder.mkdir(exist_ok=True);(folder/'documentation_test.go').write_text(source,encoding='utf-8')
result=subprocess.run(['go','test','./.gotmp/production_retry_boundary',*(sys.argv[1:] or ['-race','-count=25','-timeout=90s'])],capture_output=True,text=True,encoding='utf-8');print(result.stdout+result.stderr);raise SystemExit(result.returncode)
