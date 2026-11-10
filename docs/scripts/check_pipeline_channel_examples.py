"""Execute the two basic pipeline channel consumption blocks verbatim."""
from pathlib import Path
import subprocess
import sys
root=Path(__file__).resolve().parents[2]
markdown=(root/'docs/pipeline.md').read_text(encoding='utf-8')
fence=chr(96)*3
source='''package auditchannelconsumer
import("context";"errors";"fmt";"log";"runtime";"testing";"testing/synctest";"github.com/chichengyu/async")
'''
for i,heading in enumerate(['### ExecutePipe：通过 channel 流式输出','### Pipe()：从 SerialChain 切换到管道输出']):
 start=markdown.index(heading);start=markdown.index(fence+'go',start)+len(fence+'go')
 body=markdown[start:markdown.index(fence,start)]
 source+=f'func run{i}(ctx context.Context,items []int,fn func(context.Context,string,int)(int,error),handleResult func(int))'+chr(123)+chr(10)+body+chr(10)+chr(125)+chr(10)
source+='''
func TestOriginalChannelConsumerRejectsFailedValues(t *testing.T){
 for mode:=0;mode<2;mode++{for _,terminal:=range []string{"success","empty","error","panic","goexit","pre-canceled"}{t.Run(fmt.Sprintf("mode=%d/%s",mode,terminal),func(t *testing.T){synctest.Test(t,func(t *testing.T){
  parent,cancel:=context.WithCancel(context.Background());defer cancel()
  items:=[]int{1,2,3};if terminal=="empty"{items=nil};if terminal=="pre-canceled"{cancel()}
  var saved []int
  fn:=func(ctx context.Context,stage string,v int)(int,error){
   if stage=="multiply"{return v*2,nil}
   if v==4{switch terminal{case "error":return -99,errors.New("business");case "panic":panic("business");case "goexit":runtime.Goexit()}}
   return v+1,nil
  }
  consume:=func(v int){saved=append(saved,v)}
  if mode==0{run0(parent,items,fn,consume)}else{run1(parent,items,fn,consume)}
  synctest.Wait()
  seen:=map[int]bool{};for _,v:=range saved{if v!=3&&v!=5&&v!=7{t.Fatalf("consumed failed/unprocessed value: %v",saved)};if seen[v]{t.Fatalf("duplicate: %v",saved)};seen[v]=true}
  if terminal=="success"&&(len(saved)!=3||!seen[3]||!seen[5]||!seen[7]){t.Fatalf("success=%v",saved)}
  if terminal=="empty"&&len(saved)!=0{t.Fatal("empty input consumed")}
  if mode==1&&terminal=="goexit"{if len(saved)!=1||!seen[3]{t.Fatalf("serial Goexit output=%v",saved)}}else if terminal=="error"||terminal=="panic"||terminal=="goexit"{if len(saved)!=2||!seen[3]||!seen[7]{t.Fatalf("successful peers=%v",saved)}}
 })})}}
}
'''
folder=root/'.gotmp/pipeline_channel_workflow';folder.mkdir(exist_ok=True)
(folder/'documentation_test.go').write_text(source,encoding='utf-8')
result=subprocess.run(['go','test','./.gotmp/pipeline_channel_workflow',*(sys.argv[1:] or ['-race','-count=25','-timeout=90s'])],cwd=root,capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
