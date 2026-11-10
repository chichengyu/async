"""Execute six verbatim MapChain production sections with typed business fixtures."""
from pathlib import Path
import re,subprocess,sys
root=Path(__file__).resolve().parents[2]
markdown=(root/'docs/mapreduce.md').read_text(encoding='utf-8')
fence=chr(96)*3
start=markdown.index('### Map 容器 & MapChain 生产推荐配置')
start=markdown.index(fence+'go',start)+len(fence+'go')
body=markdown[start:markdown.index(fence,start)]
sections=re.split(r'(?=// ==========|// 或先并发转换再串行聚合)',body)[1:]
assert len(sections)==6
source='''package auditmapchainproduction
import("context";"errors";"fmt";"io";"log";"runtime";"sync";"testing";"testing/synctest";"time";"github.com/chichengyu/async")
type contextKey struct{}
type Record struct{ID int}
type Processed struct{Score float64}
type database struct{}
var db database
var businessError=errors.New("business error")
var panicValue=errors.New("business panic")
var mode string
var stop context.CancelFunc
var mu sync.Mutex
var calls,saves int
var seen map[int]int
var contexts []context.Context
var timeout time.Duration
func perform(ctx context.Context,d time.Duration)error{
 mu.Lock();calls++;contexts=append(contexts,ctx);mu.Unlock()
 if ctx.Value(contextKey{})!=17{panic("context identity lost")}
 if d>0{deadline,ok:=ctx.Deadline();if !ok||time.Until(deadline)!=d{panic("timeout lost")}}
 switch mode{
 case "error":return businessError
 case "panic":panic(panicValue)
 case "goexit":runtime.Goexit()
 case "cancel-error":stop();return businessError
 case "cancel-success":stop();return nil
 case "deadline","parent-deadline":<-ctx.Done();return ctx.Err()
 }
 return ctx.Err()
}
func processKV(ctx context.Context,k string,v int)(string,error){
 if k!=fmt.Sprint(v)&&!(len(k)==1&&int(k[0]-'a')+1==v){panic("map input identity lost")};err:=perform(ctx,timeout);return k,err
}
func saveResult(ctx context.Context,value string)error{if ctx.Value(contextKey{})!=17{panic("save context lost")};saves++;if mode=="save-error"{return businessError};return nil}
func(database)BatchUpsert(ctx context.Context,batch []async.MapEntry[string,Record])(int64,error){
 if len(batch)==0||len(batch)>200{panic("batch size lost")};mu.Lock();for _,entry:=range batch{if entry.Key!=fmt.Sprint(entry.Val.ID){panic("batch identity lost")};seen[entry.Val.ID]++};mu.Unlock()
 return int64(len(batch)),perform(ctx,time.Minute)
}
func heavyTransform(ctx context.Context,v int)(*Processed,error){err:=perform(ctx,0);return &Processed{Score:float64(v)},err}
'''
suffixes=[
 'if mode=="success"{if result.Len()!=4{panic("mapped values lost")};for k,v:=range result.Data(){if k!=v{panic("mapped value identity lost")}}}',
 '',
 'if mode=="success"{var n int64;for _,v:=range batchResult.Data(){n+=v};if n!=int64(len(recordData)){panic("batch result count lost")}}',
 'if mode=="success"&&sum!=6{panic("reduce result lost")}',
 'if mode=="success"&&reduced!=6{panic("transformed reduce result lost")}',
 'if m.Len()!=2{panic("container filter lost")};if v,ok:=m.Get("b");!ok||v!=20{panic("container map lost")};if v,ok:=m.Get("c");!ok||v!=30{panic("container map lost")}'
]
for index,section in enumerate(sections):
 data_name='inputData' if index==0 else 'data'
 record_name='recordData'
 source+=f'\nfunc run{index}(ctx context.Context,{data_name} map[string]int,{record_name} map[string]Record)error{{\n'+section+'\n'+suffixes[index]+'\nreturn nil\n}\n'
source+='''
func TestOriginalMapChainProduction(t *testing.T){
 log.SetOutput(io.Discard)
 runners:=[]func(context.Context,map[string]int,map[string]Record)error{run0,run1,run2,run3,run4,run5}
 for index,run:=range runners{
  scenarios:=[]string{"success","pre-canceled"}
  if index==0||index==1||index==2||index==4{scenarios=append(scenarios,"error","cancel-error","cancel-success")}
  if index==0||index==1||index==2{scenarios=append(scenarios,"panic","goexit")}
  if index==0||index==2{scenarios=append(scenarios,"deadline")}
  if index==1{scenarios=append(scenarios,"save-error")}
  if index==1||index==4{scenarios=append(scenarios,"parent-deadline")}
  if index==5{scenarios=[]string{"success"}}
  for _,scenario:=range scenarios{t.Run(fmt.Sprintf("%d/%s",index,scenario),func(t *testing.T){synctest.Test(t,func(t *testing.T){
   mode,calls,saves,seen,contexts=scenario,0,0,map[int]int{},nil
   timeout=0;if index==0{timeout=30*time.Second}
   parent:=context.WithValue(context.Background(),contextKey{},17)
   if mode=="parent-deadline"{var release context.CancelFunc;parent,release=context.WithTimeout(parent,time.Second);defer release()}
   ctx,cancel:=context.WithCancel(parent);defer cancel();stop=cancel
   if mode=="pre-canceled"{cancel()}
   records:=map[string]Record{};for i:=0;i<401;i++{records[fmt.Sprint(i)]=Record{ID:i}}
   got:=run(ctx,map[string]int{"0":0,"1":1,"2":2,"3":3},records);synctest.Wait()
   var want error
   switch mode{case "error","cancel-error","save-error":want=businessError;case "goexit":want=async.ErrTaskGoexit;case "pre-canceled","cancel-success":want=context.Canceled;case "deadline","parent-deadline":want=context.DeadlineExceeded}
   if index==1&&mode=="cancel-error"{want=context.Canceled}
   if mode=="panic"{var pe *async.PanicError;if !errors.As(got,&pe)||pe==nil||pe.Value!=panicValue{t.Fatalf("panic lost: %v",got)}}else if !errors.Is(got,want){t.Fatalf("error=%v want=%v",got,want)}
   if mode=="success"{
    if (index==0||index==1||index==4)&&calls!=4{t.Fatalf("calls=%d want=4",calls)}
    if index==1&&saves!=4{t.Fatalf("saved=%d want=4",saves)}
    if index==2{if calls!=3||len(seen)!=401{t.Fatalf("batch calls=%d entries=%d",calls,len(seen))};for _,count:=range seen{if count!=1{t.Fatal("entry repeated")}}}
   }
   if index==0||index==2{for _,ctx:=range contexts{if ctx.Err()==nil{t.Fatal("child context not released")}}}
  })})}
 }
}
'''
source+='''
func TestOriginalMapChainBatchBoundaries(t *testing.T){
 for _,size:=range []int{0,1,199,200,201,400,401}{t.Run(fmt.Sprint(size),func(t *testing.T){synctest.Test(t,func(t *testing.T){
  mode,calls,saves,seen,contexts="success",0,0,map[int]int{},nil
  records:=map[string]Record{};for i:=0;i<size;i++{records[fmt.Sprint(i)]=Record{ID:i}}
  if err:=run2(context.WithValue(context.Background(),contextKey{},17),nil,records);err!=nil{t.Fatal(err)}
  if calls!=(size+199)/200||len(seen)!=size{t.Fatalf("batch calls=%d seen=%d size=%d",calls,len(seen),size)}
  for _,count:=range seen{if count!=1{t.Fatal("entry repeated")}}
 })})}
}
'''
folder=root/'.gotmp/mapchain_production_workflow' ;folder.mkdir(parents=True,exist_ok=True)
(folder/'documentation_test.go').write_text(source,encoding='utf-8')
flags=sys.argv[1:] or ['-race','-count=25','-timeout=90s']
result=subprocess.run(['go','test','./.gotmp/mapchain_production_workflow',*flags],cwd=root,capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
