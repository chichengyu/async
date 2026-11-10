"""Execute production.md's literal concurrent Resize/Acquire example.

Keep the 1000 resizes and 1000 requests. Go's virtual clock makes refill
and cancellation deterministic; business errors and context identity are
checked at the surrounding application boundary. Go 1.25+ is required.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/production.md').read_text(encoding='utf-8')
fence = chr(96) * 3
start = markdown.index('## RateLimiter 并发安全边界')
start = markdown.index(fence + 'go', start) + len(fence + 'go')
body = markdown[start:markdown.index(fence, start)]
source = '''package auditproductionresize
import ("context";"errors";"sync";"testing";"testing/synctest";"time";"github.com/chichengyu/async")
var _ = errors.Join
var businessError = errors.New("request failed")
var contextError = errors.New("request context lost")
type contextKey struct{}
var mode string
var calls int
var parent context.Context
var cancelParent context.CancelFunc
func doRequest(ctx context.Context) error {
 calls++
 if ctx != parent || ctx.Value(contextKey{}) != 17 { return contextError }
 switch mode {
 case "error": return businessError
 case "cancel-error": cancelParent(); return businessError
 case "cancel-success": cancelParent(); return nil
 case "wait", "deadline": <-ctx.Done(); return ctx.Err()
 case "late-error": <-ctx.Done(); return businessError
 }
 return nil
}
func run(ctx context.Context) error {
''' + body + '\nreturn nil\n}\n'
source += '''
func TestOriginalProductionResize(t *testing.T) {
 for _,scenario := range []string{"success","error","pre-canceled","cancel-error","cancel-success","wait","deadline","late-error"} {
  t.Run(scenario,func(t *testing.T) { synctest.Test(t,func(t *testing.T) {
   mode,calls = scenario,0
   base := context.WithValue(context.Background(),contextKey{},17)
   var ctx context.Context
   if mode == "deadline" { ctx,cancelParent = context.WithTimeout(base,time.Second) } else { ctx,cancelParent = context.WithCancel(base) }
   defer cancelParent(); parent = ctx
   if mode == "pre-canceled" { cancelParent() }
   if mode == "wait" || mode == "late-error" { timer:=time.AfterFunc(time.Second,cancelParent);defer timer.Stop() }
   started:=time.Now();got:=run(ctx);synctest.Wait()
   switch mode {
   case "success":
    if got!=nil || calls!=1000 { t.Fatalf("success error=%v calls=%d",got,calls) }
    if time.Since(started)<time.Second { t.Fatalf("tokens returned early: elapsed=%v",time.Since(started)) }
   case "error":
    if !errors.Is(got,businessError) || calls!=1 { t.Fatalf("business error=%v calls=%d",got,calls) }
   default:
    want:=error(context.Canceled);if mode=="deadline" {want=context.DeadlineExceeded}
    if !errors.Is(got,want) { t.Fatalf("cancel error=%v want=%v",got,want) }
    expected:=1;if mode=="pre-canceled" {expected=0}
    if calls!=expected {t.Fatalf("calls=%d want=%d",calls,expected)}
    if (mode=="cancel-error" || mode=="late-error") && !errors.Is(got,businessError) {t.Fatalf("business error lost: %v",got)}
   }
  }) })
 }
}
'''
folder = root / '.gotmp/production_resize_workflow'
folder.mkdir(exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
result = subprocess.run(['go','test','./.gotmp/production_resize_workflow',
                         *(sys.argv[1:] or ['-race','-count=25','-timeout=90s'])],
                        capture_output=True,text=True,encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
