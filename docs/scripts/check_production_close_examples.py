"""Execute production.md's four close alternatives as independent scopes.

Keep the literal 10-second timed close and its asynchronous cleanup.
Only imports, input pool fixtures and returned observations are added.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
md = (root / 'docs/production.md').read_text(encoding='utf-8')
fence = chr(96) * 3
start = md.index('// Close()：关闭提交入口')
body = md[start:md.index(fence, start)]
markers = ['// Close()：', '// CloseAndWait()：', '// CloseAndWaitTimeout()：', '// WaitAndClose()：']
parts = []
for index, marker in enumerate(markers):
    begin = body.index(marker)
    end = body.index(markers[index + 1]) if index + 1 < len(markers) else len(body)
    parts.append(body[begin:end])
source = '''package auditproductionclose
import("context";"errors";"fmt";"sync/atomic";"testing";"testing/synctest";"time";"github.com/chichengyu/async";"github.com/chichengyu/async/internal/pool")
'''
for index, part in enumerate(parts):
    returns = ''
    suffix = ''
    if index == 2:
        returns = '(bool, <-chan struct{})'
        suffix = 'return ok, workerDone'
    elif index == 3:
        returns = '[]async.Result[int]'
        suffix = 'return results'
    source += chr(10).join([f'func run{index}(p *pool.Pool[int], cleanup func()) {returns} {{', part, suffix, '}', ''])
source += '''
func TestOriginalIndependentCloseAlternatives(t *testing.T){
 marker:=errors.New("business failed")
 for _,kind:=range []int{0,1,3}{for _,fails:=range []bool{false,true}{t.Run(fmt.Sprintf("kind=%d/fails=%v",kind,fails),func(t *testing.T){synctest.Test(t,func(t *testing.T){
  p:=pool.NewPool[int](1).WithTimeout(0).WithMaxResults(0);defer p.Close()
  started,release,returned:=make(chan context.Context,1),make(chan struct{}),make(chan struct{})
  parent,cancel:=context.WithCancel(context.Background());defer cancel()
  fn:=func(ctx context.Context)(int,error){started<-ctx;<-release;if fails{return 17,marker};return 17,nil}
  if err:=p.Submit(parent,fn);err!=nil{t.Fatal(err)};businessCtx:=<-started
  var values,observed []async.Result[int]
  p.WithResultCallback(func(r async.Result[int]){observed=append(observed,r)})
  go func(){switch kind{case 0:run0(p,nil);case 1:run1(p,nil);case 3:values=run3(p,nil)};close(returned)}()
  synctest.Wait();select{case <-returned:t.Fatal("close returned before business");default:}
  if parent.Err()!=nil || businessCtx.Err()!=nil {t.Fatal("close canceled business")}
  close(release);<-returned
  if kind==0{values=p.Wait()}
  if kind==1{if again:=p.Wait();len(again)!=0{t.Fatal("CloseAndWait did not consume Wait")};values=observed}
  if len(values)!=1 || !values[0].Occupied || values[0].Value!=17 || (fails&&!errors.Is(values[0].Err,marker)) || (!fails&&values[0].Err!=nil){t.Fatalf("results=%v",values)}
  if err:=p.Submit(parent,fn);!errors.Is(err,async.ErrPoolClosed){t.Fatalf("closed submit=%v",err)}
 })})}}
}
func TestOriginalTimedCloseAndDeferredCleanup(t *testing.T){
 for _,scenario:=range []string{"empty","running","notification"}{t.Run(scenario,func(t *testing.T){synctest.Test(t,func(t *testing.T){
  p:=pool.NewPool[int](1).WithTimeout(0).WithMaxResults(0).WithStreaming(8)
  workStarted,releaseWork:=make(chan context.Context,1),make(chan struct{})
  noticeStarted,releaseNotice:=make(chan struct{}),make(chan struct{})
  defer func(){select{case <-releaseWork:default:close(releaseWork)};select{case <-releaseNotice:default:close(releaseNotice)};p.Close()}()
  parent,cancel:=context.WithCancel(context.Background());defer cancel()
  var cleaned atomic.Int32;cleanupDone:=make(chan struct{})
  cleanup:=func(){cleaned.Add(1);for range p.StreamResults(){};close(cleanupDone)}
  var submitDone chan error
  var workCtx context.Context
  if scenario!="empty"{
   if err:=p.Submit(parent,func(ctx context.Context)(int,error){workStarted<-ctx;<-releaseWork;return 17,nil});err!=nil{t.Fatal(err)};workCtx=<-workStarted
  }
  if scenario=="notification"{
   for i:=0;i<2;i++{if err:=p.Submit(parent,func(context.Context)(int,error){return 23,nil});err!=nil{t.Fatal(err)}}
   submitDone=make(chan error,1)
   go func(){submitDone<-p.SubmitWithCompletion(parent,func(context.Context)(int,error){return 99,nil},func(r async.Result[int]){if !errors.Is(r.Err,async.ErrPoolClosed){t.Errorf("notification=%v",r)};close(noticeStarted);<-releaseNotice})}()
   synctest.Wait()
  }
  start:=time.Now();ok,workerDone:=run2(p,cleanup);elapsed:=time.Since(start)
  if scenario=="empty"{if !ok||workerDone!=nil||cleaned.Load()!=0||elapsed!=0{t.Fatalf("empty ok=%v signal=%v cleanup=%d elapsed=%v",ok,workerDone,cleaned.Load(),elapsed)};return}
  if ok||workerDone==nil||cleaned.Load()!=0||elapsed!=10*time.Second{t.Fatalf("ok=%v signal=%v cleanup=%d elapsed=%v",ok,workerDone,cleaned.Load(),elapsed)}
  if parent.Err()!=nil||workCtx.Err()!=nil{t.Fatal("timed close canceled business")}
  if scenario=="notification"{<-noticeStarted}
  close(releaseWork);<-workerDone;synctest.Wait()
  if scenario=="notification"{
   if cleaned.Load()!=0{t.Fatal("cleanup bypassed outstanding notification")}
   select{case <-cleanupDone:t.Fatal("cleanup returned before full close");default:}
   close(releaseNotice);if err:=<-submitDone;!errors.Is(err,async.ErrPoolClosed){t.Fatal(err)}
  }
  <-cleanupDone
  if cleaned.Load()!=1||p.Active()!=0||p.Pending()!=0||p.Busy()!=0{t.Fatalf("cleanup=%d stats=%v",cleaned.Load(),p.Stats())}
 })})}
}
'''
folder = root / '.gotmp/production_close_workflow'
folder.mkdir(exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
result = subprocess.run(['go', 'test', './.gotmp/production_close_workflow', *(sys.argv[1:] or ['-race', '-count=25', '-timeout=90s'])], capture_output=True, text=True, encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
