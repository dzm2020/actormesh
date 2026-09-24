package actor

import (
	"fmt"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/timer"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type initEnvelopeMessage struct{}
type stopEnvelopeMessage struct{}

func newActorProcess(system *System, handler Actor, options SpawnOptions) *Process {
	pid := NewPID(system.nextId(), options.Name, system.GetNodeID())
	logger := glog.With(zap.Uint64("actor_id", pid.ActorID),
		zap.String("actor_name", pid.ActorName),
	)
	ctx := newActorContext(system, pid, handler, logger, options)
	proc := &Process{
		system:  system,
		pid:     pid,
		ctx:     ctx,
		actor:   handler,
		logger:  logger,
		mailbox: newMailbox(options.MailboxSize),
		current: Envelope{Sender: NoSender},
		timers:  make(map[int64]*timer.Timer),
	}
	proc.mailbox.RegisterHandlers(proc, NewDefaultDispatcher(300))
	ctx.process = proc
	return proc
}

type Process struct {
	system    *System
	pid       *PID
	logger    *zap.Logger
	ctx       *actorContext
	actor     Actor
	mailbox   *Mailbox
	current   Envelope
	responded bool
	timerId   atomic.Int64
	timers    map[int64]*timer.Timer
	closingMu sync.RWMutex
	closing   bool
}

func (p *Process) GetPID() *PID { return p.pid }

func (p *Process) GetName() string {
	if p.GetPID() == nil {
		return ""
	}
	return p.GetPID().ActorName
}

func (p *Process) Push(envelope Envelope) error {
	p.closingMu.RLock()
	defer p.closingMu.RUnlock()
	if p.closing {
		return ErrActorProcessStopped
	}
	return p.mailbox.PushUser(envelope)
}

func (p *Process) PushSystem(envelope Envelope) error {
	p.closingMu.RLock()
	defer p.closingMu.RUnlock()
	if p.closing {
		return ErrActorProcessStopped
	}
	return p.mailbox.PushSystem(envelope)
}

func (p *Process) after(d time.Duration, task Task) int64 {
	// 不返回timer是为了避免并发问题, 自己actor 产生的timer 只能由本actor stop
	timerId := p.timerId.Add(1)
	f := func(ctx Context) {
		defer p.stopTimer(timerId)
		task(ctx)
	}
	t := timer.After(d, func() {
		envelope := Envelope{
			Sender:  NoSender,
			Payload: Task(f),
		}
		_ = p.PushSystem(envelope)
	})
	p.timers[timerId] = t
	return timerId
}

func (p *Process) ticker(d time.Duration, task Task) int64 {
	t := timer.Ticker(d, func() {
		envelope := Envelope{
			Sender:  NoSender,
			Payload: task,
		}
		_ = p.PushSystem(envelope)
	})
	timerId := p.timerId.Add(1)
	p.timers[timerId] = t
	return timerId
}

func (p *Process) stopTimer(timerId int64) {
	t, ok := p.timers[timerId]
	delete(p.timers, timerId)
	if !ok {
		return
	}
	t.Stop()
}

func (p *Process) forward(target *PID, message any) error {
	if p.responded {
		return errors.Wrap(ErrAlreadyResponded, "")
	}
	if err := p.system.SendEnvelope(target, Envelope{
		Payload: message,
		Sender:  p.current.Sender,
		Meta:    p.current.Meta,
	}); err != nil {
		return err
	}
	p.responded = true
	return nil
}

func (p *Process) respond(message any) error {
	envelope := p.current
	if p.responded {
		return fmt.Errorf("respond:%w", ErrAlreadyResponded)
	}
	if err := p.system.respond(p.pid, envelope.Sender, envelope.Meta, message); err != nil {
		return err
	}
	p.responded = true
	return nil
}

func (p *Process) respondError(responseError error) error {
	envelope := p.current
	if p.responded {
		return fmt.Errorf("respond err:%w", ErrAlreadyResponded)
	}
	if responseError == nil {
		return ErrRespondErrorNil
	}
	if err := p.system.respondError(p.pid, envelope.Meta, responseError); err != nil {
		return fmt.Errorf("respond err:%w", err)
	}
	p.responded = true
	return nil
}

func (p *Process) InvokerMessage(msg interface{}) bool {
	defer func() {
		if recovered := recover(); recovered != nil {
			p.panic(recovered)
		}
	}()
	p.current = msg.(Envelope)
	p.responded = false
	envelope := p.current
	switch message := envelope.Payload.(type) {
	case *initEnvelopeMessage:
		p.actor.Init(p.ctx)
	case *stopEnvelopeMessage:
		p.handleDestroy()
		return true
	case Task:
		message(p.ctx)
	default:
		if envelope.Meta.Request != nil && envelope.Meta.Request.RequestID > 0 {
			p.handleAsk(envelope)
		} else {
			p.actor.HandleTell(p.ctx, envelope.Payload)
		}
	}
	return false
}

func (p *Process) handleAsk(envelope Envelope) {
	response, askErr := p.actor.HandleAsk(p.ctx, envelope.Payload)
	if p.responded {
		return
	}
	if askErr != nil {
		if err := p.respondError(askErr); err != nil {
			p.logger.Error("handle ask response err", zap.Error(err))
		}
	}
	if response != nil {
		if err := p.respond(response); err != nil {
			p.logger.Error("handle ask response data", zap.Error(err))
		}
	}
}

func (p *Process) handleDestroy() {
	defer p.system.mgr.remove(p.pid)
	p.clearTimers()
	p.actor.Destroy(p.ctx)
}

func (p *Process) panic(e any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			p.logger.Error("actor panic handler panicked", zap.Any("panic", recovered))
		}
	}()
	p.actor.Panic(p.ctx, e)
}

func (p *Process) clearTimers() {
	for _, t := range p.timers {
		t.Stop()
	}
	p.timers = make(map[int64]*timer.Timer)
}

func (p *Process) Stop(from *PID) {
	p.closingMu.Lock()
	defer p.closingMu.Unlock()
	if p.closing {
		return
	}
	p.closing = true
	_ = p.mailbox.PushSystem(Envelope{Payload: &stopEnvelopeMessage{}, Sender: from})
}
