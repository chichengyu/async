"""Execute production.md's global startup configuration verbatim.

The init declaration is scoped as configure so tests can save/restore globals.
Use Go 1.25+ and Python 3; optional Go test flags replace 25 race runs.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
md = (root / 'docs/production.md').read_text(encoding='utf-8')
fence = chr(96) * 3
start = md.index('## 生产推荐全局配置')
start = md.index(fence + 'go', start) + len(fence + 'go')
body = md[start:md.index(fence, start)]
assert body.count('func init()') == 1
body = body.replace('func init()', 'func configure()', 1)
source = '''package auditproductionglobal
import ("context";"errors";"sync";"testing";"testing/synctest";"time";"github.com/chichengyu/async";"github.com/chichengyu/async/internal/core";"github.com/chichengyu/async/internal/pool";"github.com/chichengyu/async/internal/group")
type record struct { level core.LogLevel; msg string; fields []core.LogField }
type recorder struct { mu sync.Mutex; records []record; with,attached int }
func (r *recorder) Log(_ context.Context,level core.LogLevel,msg string,fields ...core.LogField){r.mu.Lock();defer r.mu.Unlock();r.records=append(r.records,record{level,msg,append([]core.LogField(nil),fields...)})}
func (r *recorder) With(_ ...core.LogField) core.Logger { r.mu.Lock();defer r.mu.Unlock();r.with++;return r }
func (r *recorder) WithContext(ctx context.Context) context.Context {r.mu.Lock();defer r.mu.Unlock();r.attached++;return ctx}
var myProductionLogger *recorder
func prepare(t *testing.T){
 oldTimeout,oldSubmit:=core.GetDefaultTimeout(),core.GetSubmitTimeout()
 oldLevel,oldTrace,oldLogger:=core.GetTaskFailLogLevel(),core.GetTraceLogEnabled(),core.GetLogger()
 t.Cleanup(func(){core.SetDefaultTimeout(oldTimeout);core.SetSubmitTimeout(oldSubmit);core.SetTaskFailLogLevel(oldLevel);core.SetTraceLogEnabled(oldTrace);core.SetLogger(oldLogger)})
 core.SetDefaultTimeout(7*time.Second);core.SetSubmitTimeout(9*time.Second);core.SetTaskFailLogLevel(core.LogLevelError);core.SetTraceLogEnabled(true)
 myProductionLogger=&recorder{}
}
''' + body + '''
func TestOriginalStartupConfigurationSnapshots(t *testing.T){
 prepare(t)
 oldPool:=pool.NewPool[int](1);defer oldPool.Close()
 oldGroup:=group.NewGroup[int](1);defer oldGroup.Close()
 configure()
 if core.GetDefaultTimeout()!=30*time.Second || core.GetSubmitTimeout()!=5*time.Second || core.GetTaskFailLogLevel()!=core.LogLevelWarn || core.GetTraceLogEnabled() || core.GetLogger()!=myProductionLogger {t.Fatal("startup configuration not applied")}
 freshPool:=pool.NewPool[int](1);defer freshPool.Close()
 freshGroup:=group.NewGroup[int](1);defer freshGroup.Close()
 if oldPool.Stats().Timeout!=7*time.Second || oldGroup.Stats().Timeout!=7*time.Second || freshPool.Stats().Timeout!=30*time.Second || freshGroup.Stats().Timeout!=30*time.Second {t.Fatal("creation snapshot changed")}
 freshPool.WithTimeout(2*time.Second);freshGroup.WithTimeout(3*time.Second)
 core.SetDefaultTimeout(11*time.Second)
 if freshPool.Stats().Timeout!=2*time.Second || freshGroup.Stats().Timeout!=3*time.Second {t.Fatal("instance override changed")}
 configure() // The same startup assignment is repeatable.
 if core.GetDefaultTimeout()!=30*time.Second || oldPool.Stats().Timeout!=7*time.Second {t.Fatal("repeated configuration changed old instance")}
}
func TestOriginalStartupLoggingAndTrace(t *testing.T){
 prepare(t);configure()
 ctx:=core.EnsureTraceID(context.Background());trace:=core.GetTraceID(ctx)
 if len(trace)!=32 {t.Fatalf("trace=%q",trace)}
 if again:=core.EnsureTraceID(ctx);again!=ctx || core.GetTraceID(again)!=trace {t.Fatal("existing trace changed")}
 failure:=errors.New("business failed")
 core.LogTaskFail(ctx,failure,"expected-failure")
 core.LogCtxInfo(ctx,"expected-info")
 myProductionLogger.mu.Lock();defer myProductionLogger.mu.Unlock()
 r:=myProductionLogger
 if r.with!=0 || r.attached!=0 || len(r.records)!=2 {t.Fatalf("with=%d attached=%d records=%d",r.with,r.attached,len(r.records))}
 if r.records[0].level!=core.LevelWarn || r.records[1].level!=core.LevelInfo {t.Fatal("trace switch silenced logs or wrong failure level")}
 for _,entry:=range r.records {found:=false;for _,field:=range entry.fields {if field.Key=="trace_id" && field.Value==trace {found=true}};if !found {t.Fatal("explicit log lost trace field")}}
 found:=false;for _,field:=range r.records[0].fields {if field.Key=="error" && field.Value==failure {found=true}};if !found {t.Fatal("failure identity lost")}
}
func TestOriginalStartupTaskDeadline(t *testing.T){
 for _,kind:=range []string{"pool","group"}{for _,override:=range []bool{false,true}{name:=kind+"/default";if override{name=kind+"/explicit"};t.Run(name,func(t *testing.T){synctest.Test(t,func(t *testing.T){
  prepare(t);configure();want:=30*time.Second;if override{want=3*time.Second}
  start:=time.Now();var observed time.Duration
  fn:=func(ctx context.Context)(int,error){deadline,ok:=ctx.Deadline();if !ok{t.Error("task deadline missing")}else{observed=deadline.Sub(start)};<-ctx.Done();return 0,ctx.Err()}
  var results []core.Result[int]
  if kind=="pool"{p:=pool.NewPool[int](1);defer p.Close();if override{p.WithTimeout(want)};if err:=p.Submit(context.Background(),fn);err!=nil{t.Fatal(err)};results=p.Wait()}else{g:=group.NewGroup[int](1);defer g.Close();if override{g.WithTimeout(want)};if err:=g.Go(context.Background(),fn);err!=nil{t.Fatal(err)};results=g.Wait()}
  if len(results)!=1 || !errors.Is(results[0].Err,context.DeadlineExceeded) || observed!=want || time.Since(start)!=want {t.Fatalf("results=%v deadline=%v elapsed=%v want=%v",results,observed,time.Since(start),want)}
 })})}}
}
func TestOriginalStartupSubmitBudgetMustBeExplicit(t *testing.T){
 for _,kind:=range []string{"pool","group"}{for _,explicit:=range []bool{false,true}{name:=kind+"/global-only";if explicit{name=kind+"/explicit"};t.Run(name,func(t *testing.T){synctest.Test(t,func(t *testing.T){
  prepare(t);configure();release:=make(chan struct{});started:=make(chan struct{});accepted:=make(chan error,1)
  fn:=func(context.Context)(int,error){close(started);<-release;return 1,nil}
  next:=func(context.Context)(int,error){return 2,nil}
  start:=time.Now();var wait func()[]core.Result[int]
  if kind=="pool"{p:=pool.NewPool[int](1).WithMaxPending(1).WithOverflow(core.OverflowBlock);defer p.Close();if explicit{p.WithSubmitTimeout(2*time.Second)};if err:=p.Submit(context.Background(),fn);err!=nil{t.Fatal(err)};<-started;if err:=p.Submit(context.Background(),next);err!=nil{t.Fatal(err)};go func(){accepted<-p.Submit(context.Background(),next)}();wait=p.Wait}else{g:=group.NewGroup[int](1);defer g.Close();if explicit{g.WithSubmitTimeout(2*time.Second)};if err:=g.Go(context.Background(),fn);err!=nil{t.Fatal(err)};<-started;if err:=g.Go(context.Background(),next);err!=nil{t.Fatal(err)};go func(){accepted<-g.Go(context.Background(),next)}();wait=g.Wait}
  synctest.Wait()
  wantElapsed:=6*time.Second;if explicit{wantElapsed=2*time.Second}
  time.Sleep(wantElapsed);synctest.Wait()
  var got error
  if explicit {select{case got=<-accepted:default:t.Fatal("instance submit deadline not applied")};if !errors.Is(got,core.ErrSubmitTimeout){t.Fatalf("submit error=%v",got)}}else{select{case got=<-accepted:t.Fatalf("global submit setting unexpectedly applied: %v",got);default:}}
  if time.Since(start)!=wantElapsed {t.Fatalf("submit elapsed=%v want=%v",time.Since(start),wantElapsed)}
  close(release);if !explicit{if got=<-accepted;got!=nil{t.Fatal(got)}}
  results:=wait();found:=false;for _,r:=range results {if r.Err==nil && r.Value==1 {found=true}};if !found{t.Fatalf("accepted business lost: %v",results)}
 })})}}
}
'''
folder = root / '.gotmp/production_global_workflow'
folder.mkdir(exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
result = subprocess.run(['go', 'test', './.gotmp/production_global_workflow', *(sys.argv[1:] or ['-race', '-count=25', '-timeout=90s'])], capture_output=True, text=True, encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
