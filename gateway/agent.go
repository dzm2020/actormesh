package gateway

import (
	"fmt"
	"game-server/framework/actor"
	"game-server/framework/network"
	"game-server/framework/network/protocol"
	"game-server/framework/pkg/serialize/protocodec"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type ClientAgent interface {
	network.Connection
	actor.Actor
}

func NewAgent(connection network.Connection, route ClientRoute) *Agent {
	return &Agent{
		route:      route,
		Connection: connection,
	}
}

type Agent struct {
	route ClientRoute
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
	route := a.route
	if route == nil {
		return false, nil
	}

	if !route.IsRegisteredInbound(message) {
		return false, nil
	}

	return true, route.Handle(a, ctx, message)
}

func (a *Agent) handleOutbound(message proto.Message) (bool, error) {
	route := a.route
	if route == nil {
		return false, nil
	}
	cmd, act, ok := route.ResolveOutbound(message)
	if !ok {
		return false, nil
	}
	//  已注册s2c消息
	data, err := protocodec.Marshal(message)
	if err != nil {
		return true, fmt.Errorf("agent marshal s2c err:%w", err)
	}
	frame := protocol.NewMessageFrame(cmd, act, 0, data)

	if err = a.SendMessage(frame); err != nil {
		return true, fmt.Errorf("agent send s2c message err:%w", err)
	}
	return true, nil
}

func (a *Agent) Destroy(ctx actor.Context) {
	a.DefaultActor.Destroy(ctx)
	a.Connection.Close(nil)
}
