"""Execute the config.md aggregate/FillNoResultSkipped workflow verbatim.

Requires Python 3 and Go 1.25+. Optional flags replace 25 race runs.
Only imports, function scope, typed inputs and business fixtures are added.
"""
from pathlib import Path
import subprocess
import sys
root=Path(__file__).resolve().parents[2]
markdown=(root/"docs/config.md").read_text(encoding="utf-8")
start=markdown.index("nr := async.GroupVoid().Worker(4).Build()")
body=markdown[start:markdown.index(chr(96)*3,start)]
source="""package auditaggregateexample
import(
 "context"
 "errors"
 "fmt"
 "runtime"
 "sync"
 "testing"
 "testing/synctest"
 "github.com/chichengyu/async"
)
type contextKey struct{}
var mode string
var calls int
var mu sync.Mutex
var businessErrors []error
var gate chan struct{}
var started []int
func process(ctx context.Context,item int)error{
 mu.Lock();calls++;started=append(started,item);mu.Unlock()
 if ctx.Value(contextKey{})!=17 {panic("context value lost")}
 switch mode{
 case "error":return businessErrors[item]
 case "panic":if item==3 {panic(businessErrors[item])}
 case "goexit":if item==3 {runtime.Goexit()}
 case "cancel-running":<-gate;return businessErrors[item]
 }
 return ctx.Err()
}
func run(ctx context.Context,items []int)error{
"""+body+"\nreturn nil\n}\n"
source+="""
func TestOriginalAggregateExample(t *testing.T){
 for _,scenario:=range []string{"empty","success","error","pre-canceled","panic","goexit","cancel-running"}{
  t.Run(scenario,func(t *testing.T){synctest.Test(t,func(t *testing.T){
   mode,calls,started=scenario,0,nil
   gate=make(chan struct{})
   items:=[]int{0,1,2,3,4,5,6,7}
   if scenario=="empty"{items=nil}
   businessErrors=make([]error,len(items));for i:=range items{businessErrors[i]=fmt.Errorf("failure %d",i)}
   ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel()
   if scenario=="pre-canceled"{cancel()}
   var got error
   if scenario=="cancel-running"{
    done:=make(chan error,1)
    go func(){done<-run(ctx,items)}()
    synctest.Wait()
    mu.Lock();initialCalls:=calls;mu.Unlock()
    if initialCalls==0 {t.Fatal("no business started")}
    cancel();synctest.Wait()
    select {case <-done: t.Fatal("workflow returned before accepted business completed"); default:}
    close(gate);got=<-done
   } else {got=run(ctx,items)}
   mu.Lock();gotCalls:=calls;mu.Unlock()
   switch scenario{
   case "empty","success":if got!=nil {t.Fatalf("unexpected error=%v",got)}
   case "error":for _,marker:=range businessErrors{if !errors.Is(got,marker){t.Fatalf("business error lost: got=%v want=%v",got,marker)}}
   case "pre-canceled":if !errors.Is(got,context.Canceled){t.Fatalf("cancellation lost: %v",got)}
   case "panic":var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=businessErrors[3]{t.Fatalf("panic result lost: %v",got)}
   case "goexit":if !errors.Is(got,async.ErrTaskGoexit){t.Fatalf("Goexit result lost: %v",got)}
   case "cancel-running":for _,index:=range started{if !errors.Is(got,businessErrors[index]){t.Fatalf("accepted error lost: got=%v want=%v",got,businessErrors[index])}}
   }
   want:=len(items);if scenario=="pre-canceled"{want=0}
   if scenario!="cancel-running" && gotCalls!=want{t.Fatalf("calls=%d want=%d",gotCalls,want)}
  })})
 }
}
"""
folder=root/".gotmp/aggregate_example_workflow";folder.mkdir(parents=True,exist_ok=True)
(folder/"documentation_test.go").write_text(source,encoding="utf-8")
flags=sys.argv[1:] or ["-race","-count=25","-timeout=90s"]
result=subprocess.run(["go","test","./.gotmp/aggregate_example_workflow",*flags],cwd=root,capture_output=True,text=True,encoding="utf-8")
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
