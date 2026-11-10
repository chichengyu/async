"""Execute production.md's Pool workflow verbatim under cancellation/failure.

Uses its original million-item submission bound and pool budgets unchanged;
business fixtures cancel early, so the tests exercise accepted-work cleanup
and submission failures without replacing the documented workload constants.
Requires Go 1.25+, Python 3. Defaults to 25 race runs.
"""
from pathlib import Path
import subprocess
import sys
root=Path(__file__).resolve().parents[2]
markdown=(root/'docs/production.md').read_text(encoding='utf-8')
fence=chr(96)*3
start=markdown.index('## 生产推荐 Pool 完整配置')
start=markdown.index(fence+'go',start)+len(fence+'go')
body=markdown[start:markdown.index(fence,start)]
source="""package auditpoolproduction
import(
 "context"
 "errors"
 "runtime"
 "sync"
 "testing"
 "testing/synctest"
 "time"
 "github.com/chichengyu/async"
)
type contextKey struct{}
type MyType struct{ ID int }
var mode string
var cancelParent context.CancelFunc
var businessError=errors.New("business error")
var panicValue=errors.New("business panic")
var mu sync.Mutex
var calls,active,handled int
var contexts []context.Context
func fn(ctx context.Context)(MyType,error){
 mu.Lock();calls++;id:=calls;active++;contexts=append(contexts,ctx);mu.Unlock()
 defer func(){mu.Lock();active--;mu.Unlock()}()
 if id>1||mode=="cancel-success"{<-ctx.Done();return MyType{},ctx.Err()}
 if ctx.Value(contextKey{})!=17{panic("context value lost")}
 switch mode{
 case "late-error":time.Sleep(100*time.Millisecond);return MyType{17},businessError
 case "cancel-error":return MyType{17},businessError
 case "cancel-panic":panic(panicValue)
 case "cancel-goexit":runtime.Goexit()
 }
 return MyType{17},nil
}
func handleResults(results []async.Result[MyType])error{handled++;return nil}
func run(ctx context.Context)error{
"""+body+'\nreturn nil\n}\n'
source+="""
func TestOriginalPoolProduction(t *testing.T){
 for _,scenario:=range []string{"pre-canceled","cancel-success","cancel-error","cancel-panic","cancel-goexit","late-error"}{
  t.Run(scenario,func(t *testing.T){synctest.Test(t,func(t *testing.T){
   mode,calls,active,handled,contexts=scenario,0,0,0,nil
   ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel();cancelParent=cancel
   if mode=="pre-canceled"{cancel()}
   timer:=time.AfterFunc(time.Second,cancel);defer timer.Stop()
   got:=run(ctx);synctest.Wait()
   if !errors.Is(got,context.Canceled)&&!errors.Is(got,async.ErrQueueOverflow){t.Fatalf("submission failure lost: %v",got)}
   switch mode{
   case "cancel-error","late-error":if !errors.Is(got,businessError){t.Fatalf("accepted business error lost: %v",got)}
   case "cancel-panic":var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=panicValue{t.Fatalf("accepted panic lost: %v",got)}
   case "cancel-goexit":if !errors.Is(got,async.ErrTaskGoexit){t.Fatalf("accepted Goexit lost: %v",got)}
   }
   if active!=0||handled!=0{t.Fatalf("cleanup active=%d handled=%d",active,handled)}
   if mode=="pre-canceled"&&calls!=0||mode!="pre-canceled"&&calls==0{t.Fatalf("calls=%d",calls)}
   for _,businessCtx:=range contexts{if businessCtx.Value(contextKey{})!=17{t.Fatal("context value lost")};if businessCtx.Err()==nil{t.Fatal("child context not released")};if _,ok:=businessCtx.Deadline();!ok{t.Fatal("task timeout lost")}}
  })})
 }
}
"""
folder=root/'.gotmp/pool_production_workflow';folder.mkdir(parents=True,exist_ok=True)
(folder/'documentation_test.go').write_text(source,encoding='utf-8')
flags=sys.argv[1:] or ['-race','-count=25','-timeout=90s']
result=subprocess.run(['go','test','./.gotmp/pool_production_workflow',*flags],cwd=root,capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
