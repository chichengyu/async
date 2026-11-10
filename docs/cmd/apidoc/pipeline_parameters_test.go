package main

import (
	"strings"
	"testing"
)

func TestPipelineCallbackParametersMatchEntrypoint(t *testing.T) {
	for _, tc := range []struct {
		pkg, receiver, name string
		contains            []string
	}{
		{"pipeline", "*PipelinePipe[T]", "Drain", []string{"调用 Drain 的 goroutine", "无返回值", "panic 原样传播", "不会重放"}},
		{"pipeline", "*SerialChain[T]", "Execute", []string{"Serial 不读取 Stage.SkipOnError", "零值", "逐项同步"}},
		{"pipeline", "*SerialChain[T]", "ExecutePipe", []string{"后台逐项", "非最终阶段 Pipe 失败不会作为输出发送"}},
		{"pipeline", "*FlowBuilder[T]", "Build", []string{"保留该阶段输入值", "Flow 的 context", "可并发调用"}},
		{"pipeline", "*FlowBuilder[T]", "Run", []string{"保留该阶段输入值", "SkipOnError=true"}},
		{"pipeline", "", "NewFlow", []string{"保留该阶段输入值", "链式 Stage 未提供"}},
		{"async", "", "NewFlow", []string{"保留该阶段输入值", "链式 Stage 未提供"}},
		{"async", "", "ExecuteStream", []string{"有限输入路径", "值为零值", "Serial 也不读取"}},
		{"pipeline", "*PipelineBuilder[T]", "Execute", []string{"值为零值", "无设置入口", "可并发调用"}},
		{"pipeline", "", "ExecuteWithGroup", []string{"不接收阶段名", "顶层 error 不汇总"}},
	} {
		t.Run(tc.pkg+"/"+tc.receiver+"/"+tc.name, func(t *testing.T) {
			got := semanticParameter(entry{pkg: tc.pkg, receiver: tc.receiver, name: tc.name}, parameter{name: "fn", typ: "func(context.Context, string, T) (T, error)"})
			for _, want := range tc.contains {
				if !strings.Contains(got, want) {
					t.Errorf("parameter description %q missing %q", got, want)
				}
			}
		})
	}
}
