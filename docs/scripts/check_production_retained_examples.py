"""Execute production.md's five retained-result blocks as six independent steps.

Original million-item targets remain; cooperative fixture cancellation stops
submission early. This checks retained errors, not errors evicted by pool caps.
"""
from pathlib import Path
import subprocess,sys
root=Path(__file__).resolve().parents[2]
markdown=(root/'docs/production.md').read_text(encoding='utf-8');fence=chr(96)*3
markers=['// 生产建议','// Group 使用固定 worker','// Pool 用 Run 自动管理生命周期','// Pool 由 Run() 自动管理生命周期','// 8 分片 × 4 worker = 32 个 worker']
source='''package auditproductionretained
import("context";"errors";"fmt";"runtime";"sync";"testing";"testing/synctest";"time";"github.com/chichengyu/async")
type Result struct{ID int}
var businessError=errors.New("accepted error")
var panicValue=errors.New("accepted panic")
var mode string
var mu sync.Mutex
var calls,active int
var cancelParent context.CancelFunc
var contexts []context.Context
type contextKey struct{}
func task(ctx context.Context)(string,error){
 mu.Lock();calls++;id:=calls;active++;contexts=append(contexts,ctx);mu.Unlock()
 defer func(){mu.Lock();active--;mu.Unlock()}()
 if ctx.Value(contextKey{})!=17{panic("context identity lost")}
 if mode!="success"&&(id>1||mode=="cancel-success"){<-ctx.Done();if mode=="success"{return "ok",nil};return "",ctx.Err()}
 switch mode{case "error":return "",businessError;case "panic":panic(panicValue);case "goexit":runtime.Goexit();case "late-error":time.Sleep(100*time.Millisecond);return "",businessError}
 if mode=="success"{return "ok",nil};return "",ctx.Err()
}
func taskA(ctx context.Context)(string,error){return task(ctx)}
func taskB(ctx context.Context)(string,error){return task(ctx)}
func handleResults[T any](results []async.Result[T])error{var failures []error;for _,result:=range results{failures=append(failures,result.Err)};return errors.Join(failures...)}
var _=fmt.Sprint
var _=errors.Join
'''
for index,marker in enumerate(markers):
 at=markdown.index(marker);begin=markdown.rfind(fence+'go',0,at)+len(fence+'go');body=markdown[begin:markdown.index(fence,at)]
 if index==0:body=body[body.index(marker):]
 # The selection block has two standalone calls with distinct result types.
 if index==1:
  pieces=body.split('// 海量任务用 Pool');parts=[pieces[0],pieces[1]]
 else:parts=[body]
 for part_index,part in enumerate(parts):
  name=f'run{index}_{part_index}';prefix=''
  if index==0:prefix='fn:=taskA\n'
  if index==1:prefix='fn:=func(ctx context.Context)(int,error){_,err:=task(ctx);return 17,err}\n'
  if index==4:prefix='userID:="user-17";fn:=func(ctx context.Context)(*Result,error){_,err:=task(ctx);return &Result{17},err}\n'
  source+='\nfunc '+name+'(ctx context.Context)error{\n'+prefix+part+'\nreturn nil\n}\n'
source+='''
func TestOriginalRetainedErrorExamples(t *testing.T){
 runners:=[]func(context.Context)error{run0_0,run1_0,run1_1,run2_0,run3_0,run4_0}
 for index,run:=range runners{scenarios:=[]string{"pre-canceled","cancel-success","error","panic","goexit","late-error"};if index>=3{scenarios=append(scenarios,"success")};for _,scenario:=range scenarios{
  t.Run(fmt.Sprintf("%d/%s",index,scenario),func(t *testing.T){synctest.Test(t,func(t *testing.T){
   mode,calls,active,contexts=scenario,0,0,nil
   ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel();cancelParent=cancel
   if mode=="pre-canceled"{cancel()}
   timer:=time.AfterFunc(time.Second,cancel);defer timer.Stop()
   got:=run(ctx);synctest.Wait()
   // Two-item lifecycle examples can accept all work, so cancellation may
   // appear only through the second business result rather than submission.
   if mode=="success"{expectedCalls:=2;if index==5{expectedCalls=1};if got!=nil||calls!=expectedCalls{t.Fatalf("success error=%v calls=%d",got,calls)}}else if (index!=5||mode=="pre-canceled"||mode=="cancel-success")&&!errors.Is(got,context.Canceled){t.Fatalf("cancellation lost: %v",got)}
   switch mode{
   case "error","late-error":if !errors.Is(got,businessError){t.Fatalf("accepted error lost: %v",got)}
   case "panic":var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=panicValue{t.Fatalf("accepted panic lost: %v",got)}
   case "goexit":if !errors.Is(got,async.ErrTaskGoexit){t.Fatalf("accepted Goexit lost: %v",got)}
   }
   if active!=0{t.Fatalf("active=%d",active)}
   for _,ctx:=range contexts{if ctx.Err()==nil{t.Fatal("child context leaked")}}
  })})
 }}
}
'''
folder=root/'.gotmp/production_retained_workflow';folder.mkdir(exist_ok=True)
(folder/'documentation_test.go').write_text(source,encoding='utf-8')
flags=sys.argv[1:] or ['-race','-count=25','-timeout=90s']
result=subprocess.run(['go','test','./.gotmp/production_retained_workflow',*flags],capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr);raise SystemExit(result.returncode)
