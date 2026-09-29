package core

import "errors"

// OverflowStrategy 队列溢出策略。
type OverflowStrategy int

const (
	OverflowBlock OverflowStrategy = iota
	OverflowDrop
	OverflowError
)

var ErrQueueOverflow = errors.New("async: queue overflow, task rejected")

// QueueDepth 返回当前 pending 任务数，用于背压检测。
// pending 超过阈值时调用方应自行降级（返回 429、写入溢出队列等）。
type QueueDepth = func() int
