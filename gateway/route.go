package gateway

import (
	"errors"
	"fmt"
	"game-server/framework/pkg/serialize/protocodec"
	"reflect"
	"sync"

	"game-server/framework/actor"
	"game-server/framework/network/protocol"
	"game-server/framework/pkg/glog"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	ErrRouteNotFound = errors.New("gateway client route not found")
)

type RouteHandler func(ctx actor.Context, request any) error

// ClientRoute 提供 Agent 处理上行包和识别下行消息所需的路由能力。
type ClientRoute interface {
	Register(cmd, act uint8, handler RouteHandler, c2s, s2c proto.Message)
	Handle(agent ClientAgent, ctx actor.Context, message *protocol.MessageFrame) error
	ResolveOutbound(message proto.Message) (uint8, uint8, bool)
	IsRegisteredInbound(message *protocol.MessageFrame) bool
}

var _ ClientRoute = (*Route)(nil)

type routeEntry struct {
	handler     RouteHandler
	requestType reflect.Type
}

// Route 同时保存客户端上行处理器和 Protobuf 下行类型映射。
type Route struct {
	mu       sync.RWMutex
	handlers map[uint16]routeEntry
	outbound map[string]uint16
}

// NewRoute 创建空路由表。
func NewRoute() *Route {
	return &Route{
		handlers: make(map[uint16]routeEntry),
		outbound: make(map[string]uint16),
	}
}

func (r *Route) Register(cmd, act uint8, handler RouteHandler, c2s, s2c proto.Message) {
	id := protocol.CmdAct(cmd, act)
	r.registerC2S(id, handler, c2s)
	r.registerS2C(id, s2c)
}

// registerC2S  注册客户端上行消息
func (r *Route) registerC2S(id uint16, handler RouteHandler, c2s proto.Message) {
	if handler == nil {
		return
	}
	if c2s == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[id]; exists {
		glog.Warn("c2s already registered", zap.Uint16("id", id))
		return
	}
	r.handlers[id] = routeEntry{
		handler:     handler,
		requestType: reflect.TypeOf(c2s),
	}
}

// registerS2C 注册服务器下行消息
func (r *Route) registerS2C(id uint16, s2c proto.Message) {
	if s2c == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s2cName := string(s2c.ProtoReflect().Descriptor().FullName())
	if existing, exists := r.outbound[s2cName]; exists {
		glog.Warn("s2c already registered", zap.Uint16("id", id), zap.Uint16("existing", existing))
		return
	}
	r.outbound[s2cName] = id
}

// Handle 解码并处理客户端上行包。处理器返回错误时自动回复客户端错误码。
func (r *Route) Handle(agent ClientAgent, ctx actor.Context, message *protocol.MessageFrame) error {
	r.mu.RLock()
	entry, exists := r.handlers[message.ID()]
	r.mu.RUnlock()
	if !exists {
		return ErrRouteNotFound
	}
	//  解析包体
	request := reflect.New(entry.requestType.Elem()).Interface()
	if err := protocodec.Unmarshal(message.Body, request); err != nil {
		return fmt.Errorf("agent route unmarshal body err:%w", err)
	}
	//  回调到业务
	requestCtx := newRequestContext(agent, ctx, message, request)
	if err := entry.handler(requestCtx, request); err != nil {
		return fmt.Errorf("agent route handle err:%w", err)
	}

	glog.Debug("agent route handle", zap.Uint8("cmd", message.Cmd), zap.Uint8("act", message.Act))
	return nil
}

func (r *Route) IsRegisteredInbound(message *protocol.MessageFrame) bool {
	r.mu.RLock()
	_, exists := r.handlers[message.ID()]
	r.mu.RUnlock()
	return exists
}

// ResolveOutbound 返回下行 Protobuf 消息类型对应的客户端 cmd/act。
func (r *Route) ResolveOutbound(message proto.Message) (uint8, uint8, bool) {
	name := message.ProtoReflect().Descriptor().FullName()
	r.mu.RLock()
	id, exists := r.outbound[string(name)]
	r.mu.RUnlock()
	if !exists {
		return 0, 0, false
	}
	return uint8(id >> 8), uint8(id), true
}
