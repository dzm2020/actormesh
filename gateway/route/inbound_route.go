package route

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/serialize/protocodec"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	ErrRouteNotFound        = errors.New("gateway client route not found")
	ErrInboundRouteNotFound = ErrRouteNotFound
)

type InboundRouter interface {
	Registered(message *protocol.MessageFrame) bool
	Dispatch(ctx actor.Context, message *protocol.MessageFrame) error
}

var _ InboundRouter = (*InboundRoute)(nil)

type inboundEntry struct {
	handler    Handler
	newRequest func() proto.Message
}

type InboundRoute struct {
	mu      sync.RWMutex
	entries map[uint16]inboundEntry
	logger  *glog.Logger
}

func NewInboundRoute(logger *glog.Logger) *InboundRoute {
	if logger == nil {
		logger = glog.Log()
	}
	return &InboundRoute{
		entries: make(map[uint16]inboundEntry),
		logger:  logger,
	}
}

func (r *InboundRoute) Register(cmd, act uint8, handler Handler, request proto.Message) {
	if handler == nil || request == nil {
		return
	}
	id := protocol.CmdAct(cmd, act)
	messageType := request.ProtoReflect().Type()

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[id]; exists {
		r.logger.Warn("c2s already registered", zap.Uint16("id", id))
		return
	}
	r.entries[id] = inboundEntry{
		handler: handler,
		newRequest: func() proto.Message {
			return messageType.New().Interface()
		},
	}
}

func (r *InboundRoute) Registered(message *protocol.MessageFrame) bool {
	if message == nil {
		return false
	}
	r.mu.RLock()
	_, exists := r.entries[message.ID()]
	r.mu.RUnlock()
	return exists
}

func (r *InboundRoute) Dispatch(ctx actor.Context, message *protocol.MessageFrame) error {
	if message == nil {
		return ErrInboundRouteNotFound
	}
	r.mu.RLock()
	entry, exists := r.entries[message.ID()]
	r.mu.RUnlock()
	if !exists {
		return ErrInboundRouteNotFound
	}

	request := entry.newRequest()
	if err := protocodec.Unmarshal(message.Body, request); err != nil {
		return fmt.Errorf("agent route unmarshal body err:%w", err)
	}

	if err := entry.handler(ctx, request); err != nil {
		return fmt.Errorf("agent route handle err:%w", err)
	}

	r.logger.Debug("agent route handle", zap.Uint8("cmd", message.Cmd), zap.Uint8("act", message.Act))
	return nil
}
