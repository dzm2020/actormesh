package actor

import (
	"time"

	"go.uber.org/zap"
)

func newActorContext(system *System, pid *PID, handler Actor, logger *zap.Logger, options SpawnOptions) *actorContext {
	ctx := &actorContext{
		self:       pid,
		system:     system,
		initArgs:   options.InitArgs,
		actor:      handler,
		askTimeout: time.Second * 3,
		logger:     logger,
	}
	return ctx
}

type actorContext struct {
	system     *System
	process    *Process
	self       *PID
	initArgs   []any         // 初始化参数
	actor      Actor         // 回调句柄
	askTimeout time.Duration // 同步调用超时时间
	logger     *zap.Logger
}

func (c *actorContext) Self() *PID {
	return c.self
}

func (c *actorContext) Sender() *PID {
	if c.process == nil {
		return NoSender
	}
	return c.process.current.Sender
}

func (c *actorContext) InitArgs() []any {
	if len(c.initArgs) == 0 {
		return nil
	}
	return append([]any(nil), c.initArgs...)
}

func (c *actorContext) System() SystemAPI {
	return c.system
}

func (c *actorContext) SetAskTimeout(timeout time.Duration) {
	c.askTimeout = timeout
}

// Forward 转发消息并保留原始发送者。
func (c *actorContext) Forward(target *PID, message any) error {
	return c.process.forward(target, message)
}

// Tell 以当前 Actor 为发送者发送异步消息。
func (c *actorContext) Tell(target *PID, message any) error {
	return c.system.Tell(c.self, target, message)
}

// Ask 以当前 Actor 为发送者发送同步请求。
func (c *actorContext) Ask(target *PID, message any) (any, error) {
	return c.system.Ask(c.self, target, message, c.askTimeout)
}

// AskAsync 以当前 Actor 为发送者投递请求，并在当前 Actor 邮箱中执行完成回调。
func (c *actorContext) AskAsync(target *PID, message any, complete AskCompletion) error {
	return c.system.AskAsync(c.self, target, message, c.askTimeout, complete)
}

func (c *actorContext) Ticker(d time.Duration, task Task) int64 {
	return c.process.ticker(d, task)
}

func (c *actorContext) AfterFunc(d time.Duration, task Task) int64 {
	return c.process.after(d, task)
}

func (c *actorContext) StopTimer(timerId int64) {
	c.process.stopTimer(timerId)
}

func (c *actorContext) Message() any {
	return c.process.current.Payload
}
func (c *actorContext) MessageMeta() MessageMeta {
	if c.process == nil {
		return MessageMeta{}
	}
	return c.process.current.Meta
}

func (c *actorContext) Respond(message any) error {
	return c.process.respond(message)
}

func (c *actorContext) RespondError(responseError error) error {
	return c.process.respondError(responseError)
}

func (c *actorContext) Actor() Actor {
	return c.actor
}

func (c *actorContext) Logger() *zap.Logger {
	return c.logger
}

func (c *actorContext) Stop() {
	c.process.Stop(c.self)
}
