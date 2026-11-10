"""Execute the verbatim SafeCall/error classification example in config.md.

Python 3 and Go 1.25+ are required. Defaults to 25 race runs. The fixture
supplies only imports, surrounding scope, typed inputs and a log sink.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
markdown = (root / "docs/config.md").read_text(encoding="utf-8")
fence = chr(96) * 3
start = markdown.index("_, err := async.SafeCall(ctx, item, fn)")
block = markdown[start:markdown.index(fence, start)]
source = """package auditpaniclogging
import (
 "context"
 "errors"
 "fmt"
 "strings"
 "testing"
 "github.com/chichengyu/async"
)
type logSink struct{ lines []string }
func (s *logSink) Printf(format string,args ...any){s.lines=append(s.lines,fmt.Sprintf(format,args...))}
func run(ctx context.Context, item int,fn func(context.Context,int)(int,error),log *logSink){
""" + block + "\n}\n"
source += """
func TestOriginalPanicLogging(t *testing.T){
 marker:=errors.New("business failure")
 typedNil:=(*async.PanicError)(nil)
 pe:=async.NewPanicError(marker)
 for _,scenario:=range []struct{name string; err error; panics bool; detail bool}{
  {"success",nil,false,false},
  {"business",marker,false,false},
  {"panic",nil,true,true},
  {"panic-error",pe,false,true},
  {"wrapped-panic",fmt.Errorf("wrapped: %w",pe),false,true},
  {"joined-panic",errors.Join(marker,pe),false,true},
  {"typed-nil",typedNil,false,false},
  {"wrapped-nil",fmt.Errorf("wrapped: %w",typedNil),false,false},
  {"joined-nil",errors.Join(marker,typedNil),false,false},
 }{
  t.Run(scenario.name,func(t *testing.T){
   ctx:=context.WithValue(context.Background(),struct{}{},17)
   sink:=&logSink{}
   calls:=0
   callback:=func(got context.Context,item int)(int,error){
    calls++
    if got!=ctx || item!=17 {t.Fatal("callback input lost")}
    if scenario.panics {panic(marker)}
    return 42,scenario.err
   }
   func(){
    defer func(){if recovered:=recover();recovered!=nil {t.Fatalf("classification panicked: %v",recovered)}}()
    run(ctx,17,callback,sink)
   }()
   if calls!=1 {t.Fatalf("calls=%d",calls)}
   if scenario.name=="success" {if len(sink.lines)!=0 {t.Fatal("success logged as failure")};return}
   if len(sink.lines)!=1 {t.Fatalf("log count=%d",len(sink.lines))}
   if scenario.detail {
    if !strings.HasPrefix(sink.lines[0],"panic: business failure, stack: ") && !strings.HasPrefix(sink.lines[0],"panic: business failure, stack:") {t.Fatalf("panic details lost: %s",sink.lines[0])}
   } else if !strings.HasPrefix(sink.lines[0],"business error: ") {t.Fatalf("business classification changed: %s",sink.lines[0])}
  })
 }
}
"""
folder = root / ".gotmp/panic_logging_workflow"
folder.mkdir(parents=True,exist_ok=True)
(folder / "documentation_test.go").write_text(source,encoding="utf-8")
flags = sys.argv[1:] or ["-race","-count=25","-timeout=60s"]
result = subprocess.run(["go","test","./.gotmp/panic_logging_workflow",*flags],cwd=root,
 capture_output=True,text=True,encoding="utf-8")
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
