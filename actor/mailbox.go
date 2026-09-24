package actor

import (
	"game-server/framework/pkg/mpsc"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	idle int32 = iota
	running
)

type IMailbox interface {
	PushUser(msg interface{}) error
	PushSystem(msg interface{}) error
	RegisterHandlers(invoker IMessageInvoker, dispatcher IDispatcher)
	IsEmpty() bool
}

var _ IMailbox = &Mailbox{}

type Mailbox struct {
	invoker         IMessageInvoker
	queue           *mpsc.Mpsc
	dispatch        IDispatcher
	schedulerStatus atomic.Int32
	messagesMu      sync.RWMutex
	messages        atomic.Int32 // 待处理的消息
	capacity        int32
}

func newMailbox(capacity int32) *Mailbox {
	if capacity <= 0 {
		capacity = 1024
	}
	m := &Mailbox{
		queue:    mpsc.NewMpsc(),
		capacity: capacity,
	}
	return m
}

func (mb *Mailbox) RegisterHandlers(invoker IMessageInvoker, dispatcher IDispatcher) {
	mb.invoker = invoker
	mb.dispatch = dispatcher
}

func (mb *Mailbox) PushUser(msg interface{}) error {
	if msg == nil {
		return nil
	}
	if mb.messages.Add(1) > mb.capacity {
		mb.messages.Add(-1) // 回退
		return ErrMailboxFull
	}
	mb.queue.Push(msg)
	mb.schedule()
	return nil
}
func (mb *Mailbox) PushSystem(msg interface{}) error {
	if msg == nil {
		return nil
	}
	mb.messages.Add(1)
	mb.queue.Push(msg)
	mb.schedule()
	return nil
}

// schedule 调度消息处理
// 使用 CAS 操作确保同一时间只有一个 goroutine 在处理消息队列
// 如果已经有 goroutine 在处理，则直接返回
func (mb *Mailbox) schedule() {
	if mb.schedulerStatus.CompareAndSwap(idle, running) {
		mb.dispatch.Schedule(mb.processMessages)
	}
}
func (mb *Mailbox) processMessages() {
process:
	mb.run() // 执行实际的消息处理循环
	// 处理完成后，设置邮箱状态为空闲
	mb.schedulerStatus.Store(idle)

	// 如果还有未处理的消息，则重新尝试调度
	if mb.messages.Load() > 0 {
		if mb.schedulerStatus.CompareAndSwap(idle, running) {
			goto process
		}
	}
}

// run 执行消息处理循环
// 从队列中取出消息并处理，每处理一定数量后让出 CPU，避免长时间占用
func (mb *Mailbox) run() {
	throughput := mb.dispatch.Throughput()
	var processed int
	for {
		// 每处理一定数量的消息后让出 CPU，避免长时间占用导致其他 goroutine 饥饿
		// 这样可以提高系统的响应性和公平性
		if processed >= throughput {
			processed = 0
			runtime.Gosched()
			continue
		}
		processed++
		msg := mb.queue.Pop()
		if msg == nil {
			return
		}
		mb.messages.Add(-1)
		if mb.invoker != nil {
			if mb.invoker.InvokerMessage(msg) {
				return
			}
		}
	}
}

// IsEmpty 检查 mailbox 队列是否为空
func (mb *Mailbox) IsEmpty() bool {
	return mb.queue.Empty()
}
