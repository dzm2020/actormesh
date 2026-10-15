package gateway

import (
	"fmt"

	"github.com/dzm2020/actormesh/actor"
	gatewaypb "github.com/dzm2020/actormesh/gateway/pb"
	"github.com/dzm2020/actormesh/gateway/route"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/serialize/protocodec"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Agent interface {
	Reply(message proto.Message) error
	ReplyError(err error) error
	Forward(ctx actor.Context, target *actor.PID, message any) error
	Actor() actor.Actor
	Push(message proto.Message) error
}

func newAgent(connection network.Connection, route route.ClientRoute) *clientAgent {
	return &clientAgent{
		route:      route,
		Connection: connection,
	}
}

var _ Agent = (*clientAgent)(nil)
var _ actor.Actor = (*clientAgent)(nil)

type clientAgent struct {
	network.Connection
	route          route.ClientRoute
	business       actor.Actor
	currentRequest *protocol.MessageFrame
}

func (a *clientAgent) Init(context actor.Context) {
	a.business.Init(context)
}

func (a *clientAgent) Reply(message proto.Message) (err error) {
	if a.currentRequest == nil {
		return ErrAgentRequestNil
	}
	return a.reply(a.currentRequest, message, nil)
}

func (a *clientAgent) ReplyError(cause error) (err error) {
	if a.currentRequest == nil {
		return ErrAgentRequestNil
	}
	if cause == nil {
		return actor.ErrRespondErrorNil
	}
	return a.reply(a.currentRequest, nil, cause)
}

func (a *clientAgent) reply(request *protocol.MessageFrame, msg any, err error) error {
	if request == nil {
		return ErrAgentRequestNil
	}
	response := request.Clone()
	response.Flags = 0

	if err != nil {
		response.Error = mapToClientCode(err)
	}

	if msg != nil {
		data, err := protocodec.Marshal(msg)
		if err != nil {
			return fmt.Errorf("encode client reply: %w", err)
		}
		response.Body = data
	}
	return a.Connection.SendMessage(response)
}

func (a *clientAgent) Forward(ctx actor.Context, target *actor.PID, message any) (err error) {
	request := a.currentRequest
	if request == nil {
		return ErrAgentRequestNil
	}
	err = ctx.AskAsync(target, message, func(ctx actor.Context, value any, e error) {
		if err = a.reply(request, value, e); err != nil {
			a.Log().Error("client agent forward finish", zap.Error(err))
		}
	})
	if err != nil {
		return fmt.Errorf("submit forwarded client request: %w", err)
	}
	return nil
}

func (a *clientAgent) HandleAsk(context actor.Context, v any) (any, error) {
	return a.business.HandleAsk(context, v)
}

func (a *clientAgent) Panic(context actor.Context, v any) {
	a.business.Panic(context, v)
}

func (a *clientAgent) Actor() actor.Actor {
	return a.business
}

func (a *clientAgent) HandleTell(ctx actor.Context, message any) {
	switch value := message.(type) {
	case *protocol.MessageFrame:
		if err := a.handleInbound(ctx, value); err != nil {
			a.Log().Error("handle client request",
				zap.Uint8("cmd", value.Cmd),
				zap.Uint8("act", value.Act),
				zap.Error(err),
			)
		}
		return
	case *gatewaypb.PushToClient: // 已注册的 Protobuf 下行消息
		if err := a.handlePushToClient(value); err != nil {
			a.Log().Error("handle client push",
				zap.String("typeURL", value.GetMessage().GetTypeUrl()),
				zap.Error(err),
			)
		}
		return
	default:
		a.business.HandleTell(ctx, message)
	}
}

func (a *clientAgent) handleInbound(ctx actor.Context, message *protocol.MessageFrame) error {
	defer func() {
		a.currentRequest = nil
	}()
	if message == nil {
		return ErrAgentRequestNil
	}
	if !a.route.InboundRouter().Registered(message) {
		return route.ErrInboundRouteNotFound
	}
	a.currentRequest = message
	return a.route.InboundRouter().Dispatch(ctx, message)
}

func (a *clientAgent) handlePushToClient(command *gatewaypb.PushToClient) error {
	if command == nil || command.Message == nil {
		return ErrClientPushMessageNil
	}
	message, err := command.Message.UnmarshalNew()
	if err != nil {
		return fmt.Errorf("decode client push message: %w", err)
	}
	return a.push(message)
}

func (a *clientAgent) Push(message proto.Message) (err error) {
	if err = a.push(message); err != nil {
		a.Log().Error("client agent push", zap.Error(err))
		return err
	}
	return
}

func (a *clientAgent) push(message proto.Message) error {
	_, _, exist := a.route.OutboundRouter().Resolve(message)
	if !exist {
		return fmt.Errorf("resolve client push %T: %w", message, route.ErrOutboundRouteNotFound)
	}
	frame, err := a.route.OutboundRouter().Encode(message)
	if err != nil {
		return fmt.Errorf("encode client push: %w", err)
	}
	if err = a.Connection.SendMessage(frame); err != nil {
		return fmt.Errorf("send client push cmd=%d act=%d: %w", frame.Cmd, frame.Act, err)
	}
	return nil
}

func (a *clientAgent) Destroy(ctx actor.Context) {
	a.business.Destroy(ctx)
	a.Connection.Close(nil)
}

// PushTo 通知目标 Agent 向客户端推送消息。
// 返回 nil 表示投递成功，不代表客户端已收到。
func PushTo(
	ctx actor.Context, agentPID *actor.PID, message proto.Message) (err error) {
	payload, err := anypb.New(message)
	if err != nil {
		ctx.Logger().Error("encode client push message", zap.Error(err))
		return err
	}
	if err = ctx.Tell(agentPID, &gatewaypb.PushToClient{
		Message: payload,
	}); err != nil {
		ctx.Logger().Error("client push message", zap.Error(err))
		return err
	}
	return nil
}
