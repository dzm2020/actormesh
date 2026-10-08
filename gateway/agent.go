package gateway

import (
	"errors"
	"fmt"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/gateway/route"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/network/protocol"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type ClientAgent interface {
	network.Connection
	actor.Actor
	Push(message proto.Message) error
}

func NewAgent(connection network.Connection, route route.ClientRoute) *Agent {
	return &Agent{
		route:      route,
		Connection: connection,
	}
}

type Agent struct {
	route route.ClientRoute
	actor.DefaultActor
	network.Connection
}

func (a *Agent) HandleTell(ctx actor.Context, message any) {
	ctx.Logger().Debug("agent handle tell begin", zap.Int64("connectionId", a.Connection.ID()), zap.Any("message", message))
	consumed := false
	var err error
	switch value := message.(type) {
	case *protocol.MessageFrame:
		consumed, err = a.handleInbound(ctx, value) // 处理客户端上行
	case proto.Message: // 已注册的 Protobuf 下行消息
		consumed, err = a.handleOutbound(value)
	}
	if err != nil {
		a.Log().Error("handle tell error", zap.Error(err))
		return
	}
	if consumed {
		return
	}
	a.DefaultActor.HandleTell(ctx, message)

	ctx.Logger().Debug("agent handle tell end", zap.Int64("connectionId", a.Connection.ID()), zap.Any("message", message))
}

func (a *Agent) handleInbound(ctx actor.Context, message *protocol.MessageFrame) (bool, error) {
	r := a.route
	if r == nil {
		return false, nil
	}
	if !r.InboundRouter().Registered(message) {
		return false, nil
	}

	reqCtx := newRequestContext(a, ctx, message)

	return true, r.InboundRouter().Dispatch(reqCtx, message)
}

func (a *Agent) handleOutbound(message proto.Message) (bool, error) {
	r := a.route
	if r == nil {
		return false, nil
	}
	_, _, ok := r.OutboundRouter().Resolve(message)
	if !ok {
		return false, nil
	}
	return true, a.Push(message)
}

func (a *Agent) Push(message proto.Message) error {
	r := a.route
	if r == nil {
		return errors.New("route is nil")
	}
	frame, err := r.OutboundRouter().Encode(message)
	if err != nil {
		return fmt.Errorf("agent marshal s2c err:%w", err)
	}
	if err = a.SendMessage(frame); err != nil {
		return fmt.Errorf("agent send s2c message err:%w", err)
	}
	return nil
}

func (a *Agent) Destroy(ctx actor.Context) {
	a.DefaultActor.Destroy(ctx)
	a.Connection.Close(nil)
}
