package task

import (
	"errors"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
)

type EventLoop struct {
	cases                  []reflect.SelectCase
	children               []ITask
	addSub                 chan any
	childrenDisposed       chan struct{}
	activeOnce, addSubOnce sync.Once
}

func (e *EventLoop) active(mt *Job) {
	e.activeOnce.Do(func() {
		if mt.parent != nil {
			mt.parent.eventLoop.active(mt.parent)
		}
		e.childrenDisposed = make(chan struct{})
		go e.run(mt)
	})
}

func (e *EventLoop) add(mt *Job, sub any) (err error) {
	e.addSubOnce.Do(func() {
		e.addSub = make(chan any, 20)
	})
	select {
	case e.addSub <- sub:
		if e.childrenDisposed == nil {
			switch sub.(type) {
			case TaskStarter, TaskBlock, TaskGo:
				e.active(mt)
			}
		}
		return nil
	default:
		return ErrTooManyChildren
	}
}

func (e *EventLoop) exit() {
	e.activeOnce.Do(func() {})
	e.addSubOnce.Do(func() {})
	if e.addSub != nil {
		close(e.addSub)
	}
	if e.childrenDisposed == nil {
		return
	}
	<-e.childrenDisposed
	e.childrenDisposed = nil
}

func (e *EventLoop) run(mt *Job) {
	e.cases = []reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(e.addSub)}}
	defer func() {
		err := recover()
		if err != nil {
			mt.Error("job panic", "err", err, "stack", string(debug.Stack()))
			if !ThrowPanic {
				mt.Stop(errors.Join(err.(error), ErrPanic))
			} else {
				panic(err)
			}
		}
		close(e.childrenDisposed)
	}()
	for {
		mt.blocked = nil
		if chosen, rev, ok := reflect.Select(e.cases); chosen == 0 {
			if !ok {
				mt.Debug("job addSub channel closed, exiting", "taskId", mt.GetTaskID())
				mt.Stop(ErrAutoStop)
				return
			}
			switch v := rev.Interface().(type) {
			case func():
				v()
			case ITask:
				if len(e.cases) >= 65535 {
					mt.Warn("task children too many, may cause performance issue", "count", len(e.cases), "taskId", mt.GetTaskID(), "taskType", mt.GetTaskType(), "ownerType", mt.GetOwnerType())
					v.Stop(ErrTooManyChildren)
					continue
				}
				if mt.blocked = v; v.start() {
					e.cases = append(e.cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(v.GetSignal())})
					e.children = append(e.children, v)
					mt.onChildStart(v)
				} else {
					mt.removeChild(v)
				}
			}
		} else {
			taskIndex := chosen - 1
			child := e.children[taskIndex]
			mt.blocked = child
			switch tt := mt.blocked.(type) {
			case IChannelTask:
				if tt.IsStopped() {
					switch ttt := tt.(type) {
					case ITickTask:
						ttt.GetTicker().Stop()
					}
					mt.onChildDispose(child)
					mt.removeChild(child)
					e.children = slices.Delete(e.children, taskIndex, taskIndex+1)
					e.cases = slices.Delete(e.cases, chosen, chosen+1)
				} else {
					tt.Tick(rev.Interface())
				}
			default:
				if !ok {
					if mt.onChildDispose(child); child.checkRetry(child.StopReason()) {
						if child.reset(); child.start() {
							e.cases[chosen].Chan = reflect.ValueOf(child.GetSignal())
							mt.onChildStart(child)
							continue
						}
					}
					mt.removeChild(child)
					e.children = slices.Delete(e.children, taskIndex, taskIndex+1)
					e.cases = slices.Delete(e.cases, chosen, chosen+1)
				}
			}
		}
		if !mt.handler.keepalive() && len(e.children) == 0 {
			if mt.blocked != nil {
				mt.Stop(errors.Join(mt.blocked.StopReason(), ErrAutoStop))
			} else {
				mt.Stop(ErrAutoStop)
			}
		}
	}
}
