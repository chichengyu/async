"""Run ratelimit.md's Available monitor block, including deferred cleanup.

The original rate and five-second ticker remain intact. A surrounding workload
resizes the shared limiter to one token so the low-token log is deterministic.
The block is already one of the 88 compiled fragments; no new block is counted.
"""
from pathlib import Path
import subprocess,sys
root=Path(__file__).resolve().parents[2]
md=(root/'docs/ratelimit.md').read_text(encoding='utf-8');fence=chr(96)*3
at=md.index('monitorCtx, stopMonitor := context.WithCancel(ctx)');start=md.rfind(fence+'go',0,at)+len(fence+'go');body=md[start:md.index(fence,at)]
source='''package auditratemonitorbehavior
import("bytes";"context";"errors";"fmt";"log";"strings";"testing";"testing/synctest";"time";"github.com/chichengyu/async")
type workload func(context.Context,*async.RateLimiter)
func run(ctx context.Context,work workload){
'''+body+'\nwork(ctx,rl)\n}\n'
source+='''
func TestOriginalAvailableMonitorLifecycle(t *testing.T){
 previous:=log.Writer();defer log.SetOutput(previous)
 for _,scenario:=range []string{"return-now","two-ticks","parent-cancel","pre-canceled","deadline"}{t.Run(scenario,func(t *testing.T){synctest.Test(t,func(t *testing.T){
  var output bytes.Buffer;log.SetOutput(&output)
  var ctx context.Context;var cancel context.CancelFunc
  if scenario=="deadline"{ctx,cancel=context.WithTimeout(context.Background(),6*time.Second)}else{ctx,cancel=context.WithCancel(context.Background())};defer cancel()
  if scenario=="pre-canceled"{cancel()}
  var observed *async.RateLimiter
  run(ctx,func(ctx context.Context,rl *async.RateLimiter){
   observed=rl;synctest.Wait()
   if rl.Size()!=100{t.Fatalf("initial size=%d",rl.Size())}
   rl.Resize(1)
   if scenario=="two-ticks"{time.Sleep(10*time.Second);synctest.Wait()}
   if scenario=="parent-cancel"{time.Sleep(5*time.Second);synctest.Wait();cancel();synctest.Wait();before:=output.String();time.Sleep(5*time.Second);synctest.Wait();if output.String()!=before{t.Fatal("monitor logged after cancellation")}}
   if scenario=="deadline"{time.Sleep(6*time.Second);synctest.Wait()}
  })
  synctest.Wait()
  if !errors.Is(observed.Acquire(context.Background()),async.ErrRateLimiterStopped){t.Fatal("limiter left open")}
  expected:=0;if scenario=="two-ticks"{expected=2};if scenario=="parent-cancel"||scenario=="deadline"{expected=1}
  got:=output.String();if strings.Count(got,"限流器可用令牌过低")!=expected{t.Fatalf("monitor logs=%q want=%d",got,expected)}
  if expected>0&&!strings.Contains(got,"配置速率=1/窗口"){t.Fatalf("monitor rate=%q",got)}
  before:=output.String();time.Sleep(10*time.Second);synctest.Wait();if output.String()!=before{t.Fatal("monitor logged after return")}
  _=fmt.Sprint
 })})}
}
'''
folder=root/'.gotmp/ratelimit_monitor_workflow';folder.mkdir(exist_ok=True);(folder/'documentation_test.go').write_text(source,encoding='utf-8')
result=subprocess.run(['go','test','./.gotmp/ratelimit_monitor_workflow',*(sys.argv[1:] or ['-race','-count=25','-timeout=90s'])],capture_output=True,text=True,encoding='utf-8');print(result.stdout+result.stderr);raise SystemExit(result.returncode)
