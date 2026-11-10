"""Run production.md's literal pool streaming workflow; test received errors."""
from pathlib import Path
import subprocess,sys
root=Path(__file__).resolve().parents[2];md=(root/'docs/production.md').read_text(encoding='utf-8');fence=chr(96)*3
at=md.index('## 流式消费丢数据');start=md.index(fence+'go',at)+len(fence+'go');body=md[start:md.index(fence,start)]
source='''package auditproductionstream
import("context";"errors";"fmt";"io";"log";"runtime";"strings";"sync";"testing";"testing/synctest";"time";"github.com/chichengyu/async")
var _=errors.Join
var _=fmt.Sprint
var _ sync.Mutex
var businessError=errors.New("business error")
var consumerError=errors.New("consumer error")
var panicValue=errors.New("business panic")
var mode string
var calls,received int
var allTasks chan struct{}
var mu sync.Mutex
type contextKey struct{}
func process(ctx context.Context,value string)error{
 if ctx.Value(contextKey{})!=17{panic("consumer context lost")};received++;if mode=="dropped"{<-allTasks};if mode=="consumer-error"{return consumerError};return nil
}
func work(ctx context.Context)(string,error){
 mu.Lock();calls++;if mode=="dropped"&&calls==5000{close(allTasks)};mu.Unlock()
 if ctx.Value(contextKey{})!=17{panic("context identity lost")}
 switch mode{case "error":return "",businessError;case "panic":panic(panicValue);case "goexit":runtime.Goexit();case "wait":<-ctx.Done();return "",ctx.Err()}
 return "ok",nil
}
func run(ctx context.Context,tasks []func(context.Context)(string,error))error{
'''+body+'\nreturn nil\n}\n'
source+='''
func TestOriginalProductionStream(t *testing.T){
 log.SetOutput(io.Discard)
 for _,scenario:=range []string{"success","empty","pre-canceled","error","panic","goexit","consumer-error","wait","dropped"}{t.Run(scenario,func(t *testing.T){synctest.Test(t,func(t *testing.T){
  mode,calls,received=scenario,0,0;allTasks=make(chan struct{})
  ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),contextKey{},17));defer cancel()
  if mode=="pre-canceled"{cancel()}
  if mode=="wait"{timer:=time.AfterFunc(time.Second,cancel);defer timer.Stop()}
  tasks:=[]func(context.Context)(string,error){work};if mode=="empty"{tasks=nil};if mode=="dropped"{tasks=make([]func(context.Context)(string,error),5000);for i:=range tasks{tasks[i]=work}}
  got:=run(ctx,tasks);synctest.Wait()
  var want error;switch mode{case "pre-canceled","wait":want=context.Canceled;case "error":want=businessError;case "goexit":want=async.ErrTaskGoexit;case "consumer-error":want=consumerError}
  if mode=="dropped"{if got==nil||!strings.Contains(got.Error(),"stream dropped")||calls!=5000||received>=5000{t.Fatalf("drop report=%v calls=%d received=%d",got,calls,received)};return}
  if mode=="panic"{var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=panicValue{t.Fatalf("panic lost: %v",got)}}else if !errors.Is(got,want){t.Fatalf("error=%v want=%v",got,want)}
  if mode=="empty"||mode=="pre-canceled"{if calls!=0||received!=0{t.Fatalf("calls=%d received=%d",calls,received)}}else if calls!=1||((mode=="success"||mode=="consumer-error")&&received!=1){t.Fatalf("calls=%d received=%d",calls,received)}
 })})}
}
'''
folder=root/'.gotmp/production_stream_workflow';folder.mkdir(exist_ok=True);(folder/'documentation_test.go').write_text(source,encoding='utf-8')
result=subprocess.run(['go','test','./.gotmp/production_stream_workflow',*(sys.argv[1:] or ['-race','-count=25','-timeout=90s'])],capture_output=True,text=True,encoding='utf-8');print(result.stdout+result.stderr);raise SystemExit(result.returncode)
