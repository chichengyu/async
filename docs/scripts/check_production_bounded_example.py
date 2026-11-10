"""Execute production.md's bounded batch fragment with typed business inputs.

Keep Bounded(1000) and batchSize=256. Check input order, batch drainage and
error/panic/Goexit at the application boundary. Already in the 88 fragments.
"""
from pathlib import Path
import subprocess,sys
root=Path(__file__).resolve().parents[2];md=(root/'docs/production.md').read_text(encoding='utf-8');fence=chr(96)*3
start=md.index('## BoundedRunner 注意事项');start=md.index(fence+'go',start)+len(fence+'go');body=md[start:md.index(fence,start)]
source='''package auditproductionbounded
import("context";"errors";"fmt";"runtime";"slices";"sync";"testing";"testing/synctest";"time";"github.com/chichengyu/async")
type contextKey struct{}
var mode string
var target int
var mu sync.Mutex
var calls,active,peak int
var handled []int
var sleeperStarted chan struct{}
var businessError=errors.New("business failed")
var panicValue=errors.New("business panic")
func processItem(ctx context.Context,item int)(int,error){
 mu.Lock();calls++;active++;if active>peak{peak=active};mu.Unlock()
 defer func(){mu.Lock();active--;mu.Unlock()}()
 if ctx.Value(contextKey{})!=17{panic("context lost")}
 if mode=="wait"{<-ctx.Done();return 0,ctx.Err()}
 if item==target{switch mode{case "error":return 0,businessError;case "panic":panic(panicValue);case "goexit":runtime.Goexit()}}
 if mode=="drain"&&item==target{<-sleeperStarted;return 0,businessError}
 if mode=="drain"&&item==target+1{close(sleeperStarted);time.Sleep(time.Second)}
 return item*2,nil
}
func handle(value int){handled=append(handled,value)}
func run(ctx context.Context,items []int)error{
'''+body+'\nreturn nil\n}\n'
source+='''
func TestOriginalProductionBoundedBatches(t *testing.T){
 for _,size:=range []int{0,1,255,256,257,512,513}{t.Run(fmt.Sprint(size),func(t *testing.T){synctest.Test(t,func(t *testing.T){
  mode,target,calls,active,peak,handled="success",-1,0,0,0,nil
  items:=make([]int,size);want:=make([]int,size);for i:=range items{items[i]=i;want[i]=i*2}
  got:=run(context.WithValue(context.Background(),contextKey{},17),items);synctest.Wait()
  if got!=nil||calls!=size||active!=0||!slices.Equal(handled,want)||peak>256{t.Fatalf("error=%v calls=%d active=%d peak=%d handled=%v",got,calls,active,peak,handled)}
 })})}
}
func TestOriginalProductionBoundedFailureDrainsAcceptedBatch(t *testing.T){
 for _,scenario:=range []string{"error","panic","goexit","drain","wait"}{for _,index:=range []int{0,256}{t.Run(fmt.Sprintf("%s/%d",scenario,index),func(t *testing.T){synctest.Test(t,func(t *testing.T){
  mode,target,calls,active,peak,handled=scenario,index,0,0,0,nil
  sleeperStarted=make(chan struct{})
  ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel()
  if mode=="wait"{timer:=time.AfterFunc(time.Second,cancel);defer timer.Stop()}
  items:=make([]int,513);for i:=range items{items[i]=i}
  started:=time.Now();got:=run(ctx,items);elapsed:=time.Since(started);mu.Lock();remaining:=active;mu.Unlock();synctest.Wait()
  if remaining!=0{t.Fatalf("returned before accepted batch drained: active=%d",remaining)}
  expectedCalls:=256+index
  want:=businessError;if mode=="goexit"{want=async.ErrTaskGoexit}
  if mode=="wait"{want=context.Canceled;expectedCalls=256}
  if mode=="panic"{var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=panicValue{t.Fatalf("panic lost: %v",got)}}else if !errors.Is(got,want){t.Fatalf("error=%v want=%v",got,want)}
  if calls!=expectedCalls||active!=0||peak>256{t.Fatalf("calls=%d want=%d active=%d peak=%d",calls,expectedCalls,active,peak)}
  if mode=="wait"{if len(handled)!=0{t.Fatalf("canceled handled=%v",handled)}}else{wantValues:=make([]int,0,expectedCalls-1);for i:=0;i<expectedCalls;i++{if i!=target{wantValues=append(wantValues,i*2)}};if !slices.Equal(handled,wantValues){t.Fatalf("handled=%v want=%v",handled,wantValues)}}
  if (mode=="drain"||mode=="wait")&&elapsed!=time.Second{t.Fatalf("drain elapsed=%v",elapsed)}
 })})}}
}
'''
folder=root/'.gotmp/production_bounded_workflow';folder.mkdir(exist_ok=True);(folder/'documentation_test.go').write_text(source,encoding='utf-8')
result=subprocess.run(['go','test','./.gotmp/production_bounded_workflow',*(sys.argv[1:] or ['-race','-count=25','-timeout=90s'])],capture_output=True,text=True,encoding='utf-8');print(result.stdout+result.stderr);raise SystemExit(result.returncode)
