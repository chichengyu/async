"""Run the literal resident-flow consumer against lifecycle/error fixtures."""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/pipeline.md').read_text(encoding='utf-8')
fence = chr(96) * 3
start = markdown.index('### Flow 常驻管道')
start = markdown.index(fence + 'go', start) + len(fence + 'go')
body = markdown[start:markdown.index(fence, start)]
source = '''package auditflowconsumer
import("context";"errors";"fmt";"log";"runtime";"sync";"sync/atomic";"testing";"testing/synctest";"github.com/chichengyu/async")
var _ = log.Printf
'''
if 'func consumeFlow(' in body:
    source += body + chr(10) + 'var run = consumeFlow' + chr(10)
else:
    source += '''func run(ctx context.Context,msgCh <-chan int,myHandler func(context.Context,string,int)(int,error),save func(context.Context,int)error)error{
saveResult:=func(v int)error{return save(ctx,v)}
''' + body + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
source += '''
func TestOriginalFlowConsumer(t *testing.T){
 for _,scenario:=range []string{"success","empty","large","close-residual","parse-error","enrich-error","parse-panic","enrich-panic","parse-goexit","enrich-goexit","storage-error","multiple-errors","idle-cancel","work-cancel"}{
 t.Run(scenario,func(t *testing.T){synctest.Test(t,func(t *testing.T){
  ctx,cancel:=context.WithCancel(context.WithValue(context.Background(),struct{}{},17));defer cancel()
  first,second,storageErr:=errors.New("first"),errors.New("second"),errors.New("storage")
  var mu sync.Mutex;var saved,invalid,downstream []int
  var inSave atomic.Int32
  count:=3;if scenario=="large"{count=1031};if scenario=="empty"||scenario=="idle-cancel"{count=0}
  inputs:=make(chan int,count);for i:=1;i<=count;i++{inputs<-i}
  if scenario!="idle-cancel"{close(inputs)}
  entered:=make(chan struct{},1)
  savingStarted,releaseSave,releaseWork:=make(chan struct{}),make(chan struct{}),make(chan struct{})
  handler:=func(businessCtx context.Context,stage string,v int)(int,error){
   if businessCtx.Value(struct{}{})!=17{panic("processing context lost")}
   id:=v%10000
   if scenario=="close-residual"&&stage=="enrich"&&id!=1{<-releaseWork}
   if stage=="enrich"{mu.Lock();downstream=append(downstream,id);mu.Unlock()}
   if scenario=="work-cancel"{select{case entered<-struct{}{}:default:};<-businessCtx.Done();return v,businessCtx.Err()}
   target:=stage=="parse"&&len(scenario)>=5&&scenario[:5]=="parse" || stage=="enrich"&&len(scenario)>=6&&scenario[:6]=="enrich"
   if id==2&&target{switch scenario{case "parse-error","enrich-error":return v,first;case "parse-panic","enrich-panic":panic(first);case "parse-goexit","enrich-goexit":runtime.Goexit()}}
   if scenario=="multiple-errors"&&stage=="parse"{if id==2{return v,first};if id==3{return v,second}}
   return v+10000,nil
  }
  save:=func(storageCtx context.Context,v int)error{
   if storageCtx!=ctx{t.Error("storage context lost")}
   if inSave.Add(1)!=1{t.Error("storage calls overlap")};defer inSave.Add(-1)
   runtime.Gosched()
   if scenario=="close-residual"&&v%10000==1{close(savingStarted);<-releaseSave}
   mu.Lock();defer mu.Unlock()
   if v/10000!=2{invalid=append(invalid,v)}
   if scenario=="storage-error"&&v%10000==2{return storageErr}
   saved=append(saved,v);return nil
  }
  finished:=make(chan error,1);go func(){finished<-run(ctx,inputs,handler,save)}()
  if scenario=="idle-cancel"{synctest.Wait();cancel()}
  if scenario=="work-cancel"{<-entered;cancel()}
  if scenario=="close-residual"{<-savingStarted;close(releaseWork);synctest.Wait();close(releaseSave)}
  got:=<-finished;synctest.Wait()
  if len(invalid)!=0{t.Fatalf("saved unfinished item: %v",invalid)}
  want:=error(nil);switch scenario{case "parse-error","enrich-error","multiple-errors":want=first;case "parse-goexit","enrich-goexit":want=async.ErrTaskGoexit;case "storage-error":want=storageErr;case "idle-cancel","work-cancel":want=context.Canceled}
  if scenario=="parse-panic"||scenario=="enrich-panic"{var pe *async.PanicError;if !errors.As(got,&pe)||pe.Value!=first{t.Fatalf("panic lost: %v",got)}}else if !errors.Is(got,want){t.Fatalf("error=%v want=%v",got,want)}
  if scenario=="multiple-errors"&&!errors.Is(got,second){t.Fatalf("second error lost: %v",got)}
  if len(scenario)>=5&&scenario[:5]=="parse" || scenario=="multiple-errors"{for _,id:=range downstream{if id==2||scenario=="multiple-errors"&&id==3{t.Fatal("failed parse entered enrich")}}}
  expected:=count;switch scenario{case "idle-cancel","work-cancel":expected=-1;case "multiple-errors":expected=1;case "success","empty","large","close-residual":default:expected--}
  if expected>=0&&len(saved)!=expected{t.Fatalf("saved=%v want count=%d, error=%v",saved,expected,got)}
  seen:=map[int]bool{};for _,v:=range saved{if seen[v]{t.Fatal("duplicate write")};seen[v]=true}
  _=fmt.Sprint(got)
 })})}
}
'''
folder = root / '.gotmp/flow_consumption_workflow'
folder.mkdir(exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
result = subprocess.run(['go', 'test', './.gotmp/flow_consumption_workflow', *(sys.argv[1:] or ['-race', '-count=25', '-timeout=90s'])], cwd=root, capture_output=True, text=True, encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
