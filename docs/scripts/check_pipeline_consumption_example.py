"""Execute pipeline.md's literal output consumer with business failure fixtures.

The legacy statement block is wrapped only to observe its missing error return.
The revised function is compiled verbatim. No library implementation is replaced.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/pipeline.md').read_text(encoding='utf-8')
fence = chr(96) * 3
start = markdown.index('### 管道输出消费')
start = markdown.index(fence + 'go', start) + len(fence + 'go')
body = markdown[start:markdown.index(fence, start)]
source = '''package auditpipelineconsumer
import("context";"errors";"fmt";"log";"runtime";"sync";"testing";"testing/synctest";"github.com/chichengyu/async")
type Data struct { ID, Stage int }
var _ = log.Printf
'''
if 'func consumePipeline(' in body:
    source += body + chr(10) + 'var run = consumePipeline' + chr(10)
else:
    source += '''func run(ctx context.Context, items []Data, fn func(context.Context,string,Data)(Data,error), save func(context.Context,Data)error)error{
saveToStorage:=func(v Data)error{return save(ctx,v)}
''' + body + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
source += '''
func TestOriginalPipelineConsumer(t *testing.T){
 for _,scenario:=range []string{"success","large","empty","parse-error","convert-error","parse-panic","convert-panic","parse-goexit","convert-goexit","cancel-goexit","pre-canceled","cancel-success","cancel-error","storage-error","multiple-errors"}{
 t.Run(scenario,func(t *testing.T){synctest.Test(t,func(t *testing.T){
  parent,cancel:=context.WithCancel(context.WithValue(context.Background(),struct{}{},17));defer cancel()
  businessErr,storageErr,secondErr:=errors.New("business"),errors.New("storage"),errors.New("second")
  var mu sync.Mutex;var saved []Data;var invalid []Data;var downstream []int
  var simultaneous sync.WaitGroup;if scenario=="multiple-errors"{simultaneous.Add(2)}
  items:=[]Data{{ID:1},{ID:2},{ID:3}}
  if scenario=="large"{items=make([]Data,1003);for i:=range items{items[i].ID=i+1}}
  if scenario=="empty"{items=nil};if scenario=="pre-canceled"{cancel()}
  fn:=func(ctx context.Context,stage string,v Data)(Data,error){
   if ctx.Value(struct{}{})!=17 {panic("context value lost")}
   if stage=="convert"{mu.Lock();downstream=append(downstream,v.ID);mu.Unlock()}
   target:=stage=="parse"&&len(scenario)>=5&&scenario[:5]=="parse" || stage=="convert"&&len(scenario)>=7&&scenario[:7]=="convert"
   if v.ID==2&&target{switch scenario{case "parse-error","convert-error":return v,businessErr;case "parse-panic","convert-panic":panic(businessErr);case "parse-goexit","convert-goexit":runtime.Goexit()}}
   if scenario=="multiple-errors"&&stage=="parse"&&v.ID>=2{simultaneous.Done();simultaneous.Wait();if v.ID==2{return v,businessErr};return v,secondErr}
   if scenario=="cancel-success"&&stage=="parse"{cancel()}
   if scenario=="cancel-error"&&stage=="parse"{cancel();return v,businessErr}
   if scenario=="cancel-goexit"&&stage=="convert"&&v.ID==2{cancel();runtime.Goexit()}
   if stage=="parse"{v.Stage=1}else{v.Stage++};return v,nil
  }
  save:=func(ctx context.Context,v Data)error{
   if ctx!=parent{t.Error("storage context replaced")}
   mu.Lock();defer mu.Unlock()
   if v.Stage!=2{invalid=append(invalid,v)}
   if scenario=="storage-error"&&v.ID==2{return storageErr}
   saved=append(saved,v);return nil
  }
  got:=run(parent,items,fn,save);synctest.Wait()
  if len(invalid)!=0{t.Fatalf("saved unfinished item: %v",invalid)}
  want:=error(nil)
  switch scenario{case "parse-error","convert-error","multiple-errors","cancel-error":want=businessErr;case "parse-goexit","convert-goexit","cancel-goexit":want=async.ErrTaskGoexit;case "pre-canceled","cancel-success":want=context.Canceled;case "storage-error":want=storageErr}
  if scenario=="parse-panic"||scenario=="convert-panic"{var pe *async.PanicError;if !errors.As(got,&pe)||pe.Value!=businessErr{t.Fatalf("panic lost: %v",got)}}else if !errors.Is(got,want){t.Fatalf("error=%v want=%v",got,want)}
  if scenario=="multiple-errors"&&!errors.Is(got,secondErr){t.Fatalf("second observed error lost: %v",got)}
  if (scenario=="cancel-error"||scenario=="cancel-goexit")&&!errors.Is(got,context.Canceled){t.Fatalf("parent error lost: %v",got)}
  if scenario=="parse-error"||scenario=="parse-panic"||scenario=="parse-goexit"||scenario=="multiple-errors"{for _,id:=range downstream{if id==2||scenario=="multiple-errors"&&id==3{t.Fatal("failed parse entered convert")}}}
  expected:=-1;switch scenario{case "empty","pre-canceled":expected=0;case "success":expected=3;case "large":expected=1003;case "storage-error":expected=2}
  if expected>=0&&len(saved)!=expected{t.Fatalf("saved=%v want count=%d, error=%v",saved,expected,got)}
  seen:=map[int]bool{};for _,v:=range saved{if seen[v.ID]{t.Fatal("duplicate write")};seen[v.ID]=true}
  _=fmt.Sprint(got)
 })})}
}
'''
folder = root / '.gotmp/pipeline_consumption_workflow'
folder.mkdir(exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
result = subprocess.run(['go', 'test', './.gotmp/pipeline_consumption_workflow', *(sys.argv[1:] or ['-race', '-count=25', '-timeout=90s'])], cwd=root, capture_output=True, text=True, encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
