"""Run the verbatim Task production workflow against typed business outcomes.

Requires Python 3 and Go 1.25+. Defaults to 25 race runs; Go flags may override.
Only imports, function scope, inputs, service stubs and unused-value sinks are
added. Service context/deadline, errors and accepted batch cleanup are checked.
"""
from pathlib import Path
import subprocess
import sys
root=Path(__file__).resolve().parents[2]
markdown=(root/'docs/task.md').read_text(encoding='utf-8')
fence=chr(96)*3
start=markdown.index('### 生产推荐配置')
start=markdown.index(fence+'go',start)+len(fence+'go')
body=markdown[start:markdown.index(fence,start)]
source="""package audittaskproduction
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
type Data struct{ ID int }
type Order struct{ ID int }
type payment struct{}
type reporter struct{}
var paymentService payment
var metrics reporter
var req,orderID,event=17,23,29
var scenario string
var target int
var businessError=errors.New("business error")
var panicValue=errors.New("business panic")
var calls [4]int
var seen [4][]context.Context
var mu sync.Mutex
var cancelParent context.CancelFunc
var handled []int
var active int
var reportDone chan struct{}
var delayedStarted chan struct{}
func perform(ctx context.Context,stage int)error{
 mu.Lock();calls[stage]++;seen[stage]=append(seen[stage],ctx);mu.Unlock()
 if ctx.Value(contextKey{})!=17 {panic("context value lost")}
 if stage==target {
  switch scenario{
  case "error":return businessError
  case "panic":panic(panicValue)
  case "goexit":runtime.Goexit()
  case "cancel-error":cancelParent();return businessError
  case "cancel-success":cancelParent();return nil
  case "deadline":<-ctx.Done();return ctx.Err()
  }
 }
 return ctx.Err()
}
func callExternalAPI(ctx context.Context,id int)(Data,error){if id!=req {panic("request lost")};return Data{id},perform(ctx,0)}
func (payment) Process(ctx context.Context,id int)(*Order,error){if id!=orderID {panic("order input lost")};return &Order{id},perform(ctx,1)}
func process(ctx context.Context,item int)(int,error){
 mu.Lock();active++;mu.Unlock()
 defer func(){mu.Lock();active--;mu.Unlock()}()
 if scenario=="batch-second-error" && item==256 {
  mu.Lock();calls[2]++;seen[2]=append(seen[2],ctx);mu.Unlock()
  return 0,businessError
 }
 if scenario=="batch-wait" {
  if item==0 {<-delayedStarted;mu.Lock();calls[2]++;seen[2]=append(seen[2],ctx);mu.Unlock();return 0,businessError}
  if item==2 {close(delayedStarted);time.Sleep(time.Second)}
 }
 return item*2,perform(ctx,2)
}
func handle(item int){handled=append(handled,item)}
func (reporter) Report(ctx context.Context,id int)error{defer close(reportDone);if id!=event {panic("event input lost")};return perform(ctx,3)}
func run(ctx context.Context,items []int)error{
"""+body+'\n_ = data;_ = order;return nil\n}\n'
source+="""
func TestOriginalTaskProduction(t *testing.T){
 for _,mode:=range []string{"success","error","panic","goexit","cancel-error","cancel-success","deadline","pre-canceled"}{
  for stage:=0;stage<4;stage++{
   if (mode=="success" || mode=="pre-canceled") && stage>0 {continue}
   if mode=="deadline" && stage==2 {continue} // Batch uses the parent's budget; no child timeout.
   t.Run(fmt.Sprintf("%s/%d",mode,stage),func(t *testing.T){synctest.Test(t,func(t *testing.T){
    scenario,target,calls,seen,handled,active=mode,stage,[4]int{},[4][]context.Context{},nil,0
    reportDone=make(chan struct{})
    ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel();cancelParent=cancel
    if mode=="pre-canceled"{cancel()}
    begin:=time.Now()
    got:=run(ctx,[]int{0,1,2})
    synctest.Wait()
    var want error
    stopped:=false
    switch mode{
    case "error","cancel-error":want=businessError;stopped=stage<3
    case "panic":stopped=stage<3
    case "goexit":want=async.ErrTaskGoexit;stopped=stage<3
    case "deadline":want=context.DeadlineExceeded;if stage==1{want=context.Canceled};stopped=stage<3
    case "cancel-success":if stage<2 {want=context.Canceled;stopped=true}
    case "pre-canceled":want=context.Canceled;stopped=true
    }
    if stage==3 && mode!="pre-canceled" {want=nil}
    ambiguous:=mode=="cancel-success" && stage==2
    if mode=="panic" && stage<3 {var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=panicValue{t.Fatalf("panic lost: %v",got)}} else if ambiguous {if got!=nil&&!errors.Is(got,context.Canceled){t.Fatalf("batch cancellation result=%v",got)}} else if !errors.Is(got,want) {t.Fatalf("error=%v want=%v",got,want)}
    if got==nil {<-reportDone} // Observe the deliberately asynchronous reporter only after checking the return contract.
    synctest.Wait()
    wantCalls:=[4]int{1,1,3,1}
    if mode=="pre-canceled"{wantCalls=[4]int{0,0,0,0}} else if stopped {for i:=stage+1;i<4;i++{wantCalls[i]=0}}
    // A free bounded permit races with cancellation; rejected submissions do
    // not invoke business, while admitted callbacks still observe cancellation.
    if mode=="cancel-success" && stage<2 {for i:=stage+1;i<4;i++{wantCalls[i]=0}}
    if mode=="pre-canceled" {if calls[0]>1||calls[1]!=0||calls[2]!=0||calls[3]!=0{t.Fatalf("canceled calls=%v",calls)}} else if mode=="cancel-success" && stage==0 {if calls[0]!=1||calls[1]>1||calls[2]!=0||calls[3]!=0{t.Fatalf("cancel-success calls=%v",calls)}} else if mode=="cancel-success" && stage==1 {if calls[0]!=1||calls[1]!=1||calls[2]>3||calls[3]!=0{t.Fatalf("cancel-success calls=%v",calls)}} else if mode=="cancel-error" && stage==2 {if calls[0]!=1||calls[1]!=1||calls[2]<1||calls[2]>3||calls[3]!=0{t.Fatalf("batch cancel-error calls=%v",calls)}} else if ambiguous {if calls[0]!=1||calls[1]!=1||calls[2]<1||calls[2]>3||calls[3]>1||(got!=nil&&calls[3]!=0)||(got==nil&&calls[3]!=1){t.Fatalf("batch calls=%v error=%v",calls,got)}} else if calls!=wantCalls {t.Fatalf("calls=%v want=%v",calls,wantCalls)}
    if active!=0 {t.Fatal("batch returned with active callbacks")}
    durations:=[4]time.Duration{30*time.Second,60*time.Second,0,5*time.Second}
    for index,contexts:=range seen{for _,businessCtx:=range contexts{
     if businessCtx.Value(contextKey{})!=17 {t.Fatal("context value lost")}
     deadline,ok:=businessCtx.Deadline()
     if durations[index]>0 && (!ok || deadline.Sub(begin)!=durations[index]) {t.Fatalf("stage=%d deadline=%v ok=%v",index,deadline.Sub(begin),ok)}
     if durations[index]==0 && ok {t.Fatal("batch acquired unexpected deadline")}
     if durations[index]>0 && businessCtx.Err()==nil {t.Fatal("callback child context not released")}
    }}
    elapsed:=time.Duration(0)
    if mode=="deadline" {switch stage{case 0:elapsed=30*time.Second;case 1:elapsed=10*time.Second;want=context.Canceled;case 3:elapsed=5*time.Second}}
    if mode=="deadline" && stage==1 && !errors.Is(got,context.Canceled){t.Fatalf("payment timer cancellation lost: %v",got)}
    if time.Since(begin)!=elapsed {t.Fatalf("elapsed=%v want=%v",time.Since(begin),elapsed)}
    if mode=="success" && !reflectEqual(handled,[]int{0,2,4}) {t.Fatalf("handled=%v",handled)}
   })})
  }
 }
}
func reflectEqual(a,b []int)bool{if len(a)!=len(b){return false};for i:=range a{if a[i]!=b[i]{return false}};return true}
func TestOriginalTaskBatchBoundaries(t *testing.T){
 for _,size:=range []int{0,1,100,256,257,513}{
  t.Run(fmt.Sprint(size),func(t *testing.T){synctest.Test(t,func(t *testing.T){
   scenario,target,calls,seen,handled,active="success",-1,[4]int{},[4][]context.Context{},nil,0
   reportDone=make(chan struct{})
   items:=make([]int,size);want:=make([]int,size);for i:=range items{items[i]=i;want[i]=i*2}
   ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel();cancelParent=cancel
   got:=run(ctx,items);if got!=nil{t.Fatal(got)};<-reportDone;synctest.Wait()
   if calls!=[4]int{1,1,size,1}||active!=0||!reflectEqual(handled,want){t.Fatalf("batch calls=%v active=%d handled=%v",calls,active,handled)}
  })})
 }
}
func TestOriginalTaskBatchFailure(t *testing.T){
 for _,mode:=range []string{"batch-second-error","batch-wait"}{
  t.Run(mode,func(t *testing.T){synctest.Test(t,func(t *testing.T){
   scenario,target,calls,seen,handled,active=mode,-1,[4]int{},[4][]context.Context{},nil,0
   reportDone=make(chan struct{});delayedStarted=make(chan struct{})
   size,accepted:=513,512;if mode=="batch-wait"{size,accepted=3,3}
   items:=make([]int,size);for i:=range items{items[i]=i}
   ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel();cancelParent=cancel
   begin:=time.Now();got:=run(ctx,items)
   mu.Lock();remaining:=active;mu.Unlock()
   if !errors.Is(got,businessError)||remaining!=0{t.Fatalf("accepted batch not drained: error=%v active=%d",got,remaining)}
   if calls!=[4]int{1,1,accepted,0}||len(handled)!=accepted-1{t.Fatalf("batch failure calls=%v handled=%d",calls,len(handled))}
   if mode=="batch-wait"&&time.Since(begin)!=time.Second{t.Fatalf("batch wait elapsed=%v",time.Since(begin))}
  })})
 }
}
"""
folder=root/'.gotmp/task_production_workflow';folder.mkdir(parents=True,exist_ok=True)
(folder/'documentation_test.go').write_text(source,encoding='utf-8')
flags=sys.argv[1:] or ['-race','-count=25','-timeout=90s']
result=subprocess.run(['go','test','./.gotmp/task_production_workflow',*flags],cwd=root,capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
