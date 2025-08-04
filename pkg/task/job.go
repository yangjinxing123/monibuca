package task

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"m7s.live/v5/pkg/util"
)

var idG atomic.Uint32
var sourceFilePathPrefix string

func init() {
	if _, file, _, ok := runtime.Caller(0); ok {
		sourceFilePathPrefix = strings.TrimSuffix(file, "pkg/task/job.go")
	}
}

func GetNextTaskID() uint32 {
	return idG.Add(1)
}

// Job include tasks
type Job struct {
	Task
	children                    sync.Map
	addLock                     sync.Mutex
	descendantsDisposeListeners []func(ITask)
	descendantsStartListeners   []func(ITask)
	blocked                     ITask
	eventLoop                   EventLoop
	Size                        atomic.Int32
}

func (*Job) GetTaskType() TaskType {
	return TASK_TYPE_JOB
}

func (mt *Job) getJob() *Job {
	return mt
}

func (mt *Job) Blocked() ITask {
	return mt.blocked
}

func (mt *Job) waitChildrenDispose(stopReason error) {
	mt.addLock.Lock()
	mt.children.Range(func(key, value any) bool {
		value.(ITask).Stop(stopReason)
		return true
	})
	mt.eventLoop.exit()
	mt.addLock.Unlock()
}

func (mt *Job) OnDescendantsDispose(listener func(ITask)) {
	mt.descendantsDisposeListeners = append(mt.descendantsDisposeListeners, listener)
}

func (mt *Job) onDescendantsDispose(descendants ITask) {
	for _, listener := range mt.descendantsDisposeListeners {
		listener(descendants)
	}
	if mt.parent != nil {
		mt.parent.onDescendantsDispose(descendants)
	}
}

func (mt *Job) onChildDispose(child ITask) {
	mt.onDescendantsDispose(child)
	child.dispose()
}

func (mt *Job) removeChild(child ITask) {
	mt.children.Delete(child.getKey())
	mt.Size.Add(-1)
}

func (mt *Job) OnDescendantsStart(listener func(ITask)) {
	mt.descendantsStartListeners = append(mt.descendantsStartListeners, listener)
}

func (mt *Job) onDescendantsStart(descendants ITask) {
	for _, listener := range mt.descendantsStartListeners {
		listener(descendants)
	}
	if mt.parent != nil {
		mt.parent.onDescendantsStart(descendants)
	}
}

func (mt *Job) onChildStart(child ITask) {
	mt.onDescendantsStart(child)
}

func (mt *Job) RangeSubTask(callback func(task ITask) bool) {
	mt.children.Range(func(key, value any) bool {
		callback(value.(ITask))
		return true
	})
}

func (mt *Job) AddDependTask(t ITask, opt ...any) (task *Task) {
	t.Using(mt)
	opt = append(opt, 1)
	return mt.AddTask(t, opt...)
}

func (mt *Job) initContext(task *Task, opt ...any) {
	callDepth := 2
	for _, o := range opt {
		switch v := o.(type) {
		case context.Context:
			task.parentCtx = v
		case Description:
			task.SetDescriptions(v)
		case RetryConfig:
			task.retry = v
		case *slog.Logger:
			task.Logger = v
		case int:
			callDepth += v
		}
	}
	_, file, line, ok := runtime.Caller(callDepth)
	if ok {
		task.StartReason = fmt.Sprintf("%s:%d", strings.TrimPrefix(file, sourceFilePathPrefix), line)
	}
	task.parent = mt
	if task.parentCtx == nil {
		task.parentCtx = mt.Context
	}
	task.level = mt.level + 1
	if task.ID == 0 {
		task.ID = GetNextTaskID()
	}
	task.Context, task.CancelCauseFunc = context.WithCancelCause(task.parentCtx)
	task.startup = util.NewPromise(task.Context)
	task.shutdown = util.NewPromise(context.Background())
	if task.Logger == nil {
		task.Logger = mt.Logger
	}
}

func (mt *Job) AddTask(t ITask, opt ...any) (task *Task) {
	task = t.GetTask()
	task.handler = t
	mt.initContext(task, opt...)
	if mt.IsStopped() {
		task.startup.Reject(mt.StopReason())
		return
	}
	mt.addLock.Lock()
	defer mt.addLock.Unlock()
	mt.children.Store(t.getKey(), t)
	mt.Size.Add(1)
	if err := mt.eventLoop.add(mt, t); err != nil {
		task.startup.Reject(err)
		return
	}
	return
}

func (mt *Job) RunTask(t ITask, opt ...any) (err error) {
	task := t.GetTask()
	task.handler = t
	mt.initContext(task, opt...)
	if mt.IsStopped() {
		err = mt.StopReason()
		task.startup.Reject(err)
		return
	}
	started := task.start()
	<-task.Done()
	if started {
		task.dispose()
	}
	return task.StopReason()
}

func (mt *Job) Call(callback func()) {
	mt.addLock.Lock()
	if mt.eventLoop.childrenDisposed != nil {
		ctx, cancel := context.WithCancel(mt)
		_ = mt.eventLoop.add(mt, func() { callback(); cancel() })
		mt.addLock.Unlock()
		<-ctx.Done()
		return
	}
	mt.addLock.Unlock()
	callback()
}

func (mt *Job) Post(callback func()) {
	mt.addLock.Lock()
	if mt.eventLoop.childrenDisposed != nil {
		_ = mt.eventLoop.add(mt, callback)
		mt.addLock.Unlock()
		return
	}
	mt.addLock.Unlock()
	callback()
}
