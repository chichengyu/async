"""Execute the verbatim three-stage production Pipeline configuration.

Typed business fixtures verify stage values, errors, cancellation, deadline
and child-context cleanup. Go 1.25+; defaults to 25 race runs.
"""
from pathlib import Path
import subprocess
import sys
root=Path(__file__).resolve().parents[2]
markdown=(root/'docs/pipeline.md').read_text(encoding='utf-8')
fence=chr(96)*3
start=markdown.index('### 生产推荐配置')
start=markdown.index(fence+'go',start)+len(fence+'go')
body=markdown[start:markdown.index(fence,start)]
source="""package auditpipelineproduction
import(
 "context"
 "errors"
 "fmt"
 "runtime"
 "sync"
 "testing"
 "testing/synctest"
 "time"
 "github.com/chichengyu/async"
)
type contextKey struct{}
type Record struct{ID,Stage int}
var mode string
var target int
var cancelParent context.CancelFunc
var businessError=errors.New("business error")
var panicValue=errors.New("business panic")
var mu sync.Mutex
var calls [3]int
var contexts []context.Context
func perform(ctx context.Context,index int,r Record)(Record,error){
 mu.Lock();calls[index]++;if ctx!=nil{contexts=append(contexts,ctx)};mu.Unlock()
 if r.Stage!=index{panic("stage input lost")}
 if r.ID!=17{panic("record identity lost")}
 if ctx!=nil&&ctx.Value(contextKey{})!=17{panic("context value lost")}
 if mode=="shared-budget"{if index==0{time.Sleep(2*time.Minute)};if index==1{<-ctx.Done();return r,ctx.Err()}}
 if index==target{switch mode{
 case "error":return r,businessError
 case "panic":panic(panicValue)
 case "goexit":runtime.Goexit()
 case "cancel-error":cancelParent();return r,businessError
 case "cancel-success":cancelParent();return r,nil
 case "deadline","parent-deadline":<-ctx.Done();return r,ctx.Err()
 }}
 r.Stage++;return r,nil
}
func parseRecord(ctx context.Context,r Record)(Record,error){return perform(ctx,0,r)}
func enrichRecord(ctx context.Context,r Record)(Record,error){return perform(ctx,1,r)}
func validateRecord(r Record)(Record,error){return perform(nil,2,r)}
func run(ctx context.Context,records []Record)error{
"""+body+'\n_ =results;return err\n}\n'
source+="""
func TestOriginalPipelineProduction(t *testing.T){
 for _,scenario:=range []string{"success","empty","pre-canceled","error","panic","goexit","cancel-error","cancel-success","deadline","parent-deadline","shared-budget"}{
  for stage:=0;stage<3;stage++{
   if (scenario=="success"||scenario=="empty"||scenario=="pre-canceled"||scenario=="shared-budget")&&stage>0{continue}
   if (scenario=="deadline"||scenario=="parent-deadline")&&stage==2{continue}
   t.Run(fmt.Sprintf("%s/%d",scenario,stage),func(t *testing.T){synctest.Test(t,func(t *testing.T){
    mode,target,calls,contexts=scenario,stage,[3]int{},nil
    parent:=context.WithValue(context.Background(),contextKey{},17)
    if mode=="parent-deadline"{var stop context.CancelFunc;parent,stop=context.WithTimeout(parent,time.Second);defer stop()}
    ctx,cancel:=context.WithCancel(parent);defer cancel();cancelParent=cancel
    if mode=="pre-canceled"{cancel()}
    records:=[]Record{{ID:17}};if mode=="empty"{records=nil}
    begin:=time.Now();got:=run(ctx,records);synctest.Wait()
    var want error
    switch mode{
    case "error","cancel-error":want=businessError
    case "goexit":want=async.ErrTaskGoexit
    case "cancel-success","pre-canceled":want=context.Canceled
    case "deadline","parent-deadline","shared-budget":want=context.DeadlineExceeded
    }
    if mode=="panic"{var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=panicValue{t.Fatalf("panic lost: %v",got)}}else if !errors.Is(got,want){t.Fatalf("error=%v want=%v",got,want)}
    wanted:=[3]int{1,1,1};if mode=="empty"||mode=="pre-canceled"{wanted=[3]int{}}else if mode!="success"{for i:=stage+1;i<3;i++{wanted[i]=0}}
    if mode=="shared-budget"{wanted=[3]int{1,1,0}}
    if calls!=wanted{t.Fatalf("stage calls=%v want=%v",calls,wanted)}
    for _,businessCtx:=range contexts{
     duration:=5*time.Minute;if mode=="parent-deadline"{duration=time.Second}
     deadline,ok:=businessCtx.Deadline();if !ok||deadline.Sub(begin)!=duration{t.Fatalf("deadline=%v ok=%v",deadline.Sub(begin),ok)}
     if businessCtx.Err()==nil{t.Fatal("child context not released")}
    }
    elapsed:=time.Duration(0);if mode=="deadline"||mode=="shared-budget"{elapsed=5*time.Minute};if mode=="parent-deadline"{elapsed=time.Second}
    if time.Since(begin)!=elapsed{t.Fatalf("elapsed=%v want=%v",time.Since(begin),elapsed)}
   })})
  }
 }
}
"""
folder=root/'.gotmp/pipeline_production_workflow';folder.mkdir(parents=True,exist_ok=True)
(folder/'documentation_test.go').write_text(source,encoding='utf-8')
flags=sys.argv[1:] or ['-race','-count=25','-timeout=90s']
result=subprocess.run(['go','test','./.gotmp/pipeline_production_workflow',*flags],cwd=root,capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
