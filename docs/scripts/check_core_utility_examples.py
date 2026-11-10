"""Execute config.md MergeCancel, WithConfig and Must examples verbatim.

Requires Python 3 and Go 1.25+. Optional Go test flags replace 25 race runs.
Only input fixtures, scopes, imports and unused-result observations are added.
"""
from pathlib import Path
import subprocess
import sys
root=Path(__file__).resolve().parents[2]
markdown=(root/"docs/config.md").read_text(encoding="utf-8")
fence=chr(96)*3
def block(marker):
 start=markdown.index(marker)
 return markdown[start:markdown.index(fence,start)]
source="""package auditcoreutilities
import (
 "context"
 "errors"
 "reflect"
 "runtime"
 "testing"
 "github.com/chichengyu/async"
)
func runMerge(cancelParent,cancelChild context.CancelFunc){
"""+block("cancelBoth := async.MergeCancel(cancelParent, cancelChild)")+"\n}\n"
source+="func runConfig(configuredWorkers int) int {\n"+block("workers := async.WithConfig(configuredWorkers)")+"\nreturn workers\n}\n"
source+="""type Config struct{ ID int }
var input *Config
var inputErr error
var calls int
func loadRequiredConfig()(*Config,error){calls++;return input,inputErr}
func runMust()*Config {
"""+block("required := async.Must(loadRequiredConfig())")+"\nreturn required\n}\n"
source+="""
func TestOriginalCoreUtilityExamples(t *testing.T){
 t.Run("merge",func(t *testing.T){
  var order []string
  parent,cancelParent:=context.WithCancel(context.Background());defer cancelParent()
  child,cancelChild:=context.WithCancel(parent);defer cancelChild()
  runMerge(func(){order=append(order,"parent");cancelParent()},func(){order=append(order,"child");cancelChild()})
  if !reflect.DeepEqual(order,[]string{"child","parent"}) {t.Fatalf("order=%v",order)}
  if parent.Err()!=context.Canceled || child.Err()!=context.Canceled {t.Fatal("contexts not canceled")}
 })
 for _,n:=range []int{-1,0,1,3}{
  t.Run("config",func(t *testing.T){want:=n;if n<=0 {want=runtime.NumCPU()*2};if got:=runConfig(n);got!=want {t.Fatalf("workers=%d want=%d",got,want)}})
 }
 typedNil:=(*async.PanicError)(nil)
 marker:=errors.New("config failed")
 for _,scenario:=range []struct{name string;value *Config;err error}{
  {"success",&Config{17},nil},{"nil-success",nil,nil},{"error",&Config{99},marker},{"typed-nil",nil,typedNil},
 }{
  t.Run("must/"+scenario.name,func(t *testing.T){
   input,inputErr,calls=scenario.value,scenario.err,0
   returned:=false
   var result *Config
   var recovered any
   func(){defer func(){recovered=recover()}();result=runMust();returned=true}()
   if calls!=1 {t.Fatalf("calls=%d",calls)}
   if scenario.err==nil {if !returned || recovered!=nil || result!=scenario.value {t.Fatal("success value lost")}} else if returned || recovered!=scenario.err {t.Fatalf("panic identity lost: %v",recovered)}
  })
 }
}
"""
folder=root/".gotmp/core_utility_workflow";folder.mkdir(parents=True,exist_ok=True)
(folder/"documentation_test.go").write_text(source,encoding="utf-8")
flags=sys.argv[1:] or ["-race","-count=25","-timeout=90s"]
result=subprocess.run(["go","test","./.gotmp/core_utility_workflow",*flags],cwd=root,capture_output=True,text=True,encoding="utf-8")
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
