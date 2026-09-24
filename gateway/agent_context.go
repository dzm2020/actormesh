package gateway

import (
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/serialize/protocodec"
	"sync/atomic"

	"go.uber.org/zap"
)

var (
	ErrClientAlreadyResponded = errors.New("gateway client request already responded")
)

type ClientErrorCode = uint16

const (
	ClientErrorNone ClientErrorCode = iota
	ClientErrorInternal
)

const (
	requestStateIdle      uint32 = iota
	requestStatePending          // 等待回复
	requestStateResponded        // 已回复
)

func mapToClientCode(err error) ClientErrorCode {
	code, ok := actor.CodeOf(err)
	if !ok || code == actor.ErrorCodeUnknown || uint64(code) > uint64(^ClientErrorCode(0)) {
		return ClientErrorInternal
	}
	return ClientErrorCode(code)
}

var _ actor.Context = (*agentContext)(nil)

// agentContext
// @Description: 重写actor.context接口  使ResponseXX直接回复到客户端, Forward透传消息到其他actor,同时异步等待回复
type agentContext struct {
	actor.Context
	agent   ClientAgent
	message *protocol.MessageFrame
	request any // 已解析的包体
	state   atomic.Uint32
	logger  *zap.Logger
}

func newRequestContext(agent ClientAgent, ctx actor.Context, message *protocol.MessageFrame, request any) *agentContext {
	return &agentContext{
		Context: ctx,
		agent:   agent,
		message: message,
		request: request,
		logger: ctx.Logger().With(zap.Uint8("cmd", message.Cmd),
			zap.Uint8("act", message.Act), zap.Uint32("index", message.Index)),
	}
}

// Respond 向客户端回复成功消息。
func (c *agentContext) Respond(message any) error {
	return c.send(requestStateIdle, message)
}

// RespondError 向客户端回复错误码。
func (c *agentContext) RespondError(responseError error) error {
	return c.sendError(requestStateIdle, responseError)
}

// Forward 将客户端请求转发给业务 Actor，通过闭包存储客户端消息上下文。
func (c *agentContext) Forward(target *actor.PID, message any) error {
	if !c.state.CompareAndSwap(requestStateIdle, requestStatePending) {
		return ErrClientAlreadyResponded
	}

	c.logger.Debug("agent context  forward", zap.Any("targetPid", target))

	return c.Context.AskAsync(target, message, func(ctx actor.Context, value any, requestError error) {
		var completionError error
		if requestError != nil {
			completionError = c.sendError(requestStatePending, requestError)
		} else {
			completionError = c.send(requestStatePending, value)
		}
		c.logger.Debug("agent context  forward completion", zap.Any("completionError", completionError))
	})
}

func (c *agentContext) send(expect uint32, msg any) error {
	if !c.state.CompareAndSwap(expect, requestStateResponded) {
		return ErrClientAlreadyResponded
	}
	response := c.message.Clone()
	response.Flags = 0
	data, err := protocodec.Marshal(msg)
	if err != nil {
		c.logger.Error("agent context response data", zap.Error(err))
		response.Error = ClientErrorInternal
	} else {
		response.Body = data
	}
	if err = c.agent.SendMessage(response); err != nil {
		return fmt.Errorf("agent context response error: %w", err)
	}
	return nil
}

func (c *agentContext) sendError(expect uint32, err error) error {
	if !c.state.CompareAndSwap(expect, requestStateResponded) {
		return ErrClientAlreadyResponded
	}
	response := c.message.Clone()
	response.Flags = 0
	response.Error = mapToClientCode(err)
	if err = c.agent.SendMessage(response); err != nil {
		return fmt.Errorf("agent context response error: %w", err)
	}
	return nil
}
