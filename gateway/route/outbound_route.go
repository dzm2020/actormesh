package route

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/serialize/protocodec"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var ErrOutboundRouteNotFound = errors.New("gateway outbound route not found")

type OutboundRouter interface {
	Resolve(message proto.Message) (uint8, uint8, bool)
	Encode(message proto.Message) (*protocol.MessageFrame, error)
}

var _ OutboundRouter = (*OutboundRoute)(nil)

type OutboundRoute struct {
	mu      sync.RWMutex
	entries map[protoreflect.FullName]uint16
	logger  *glog.Logger
}

func NewOutboundRoute(logger *glog.Logger) *OutboundRoute {
	if logger == nil {
		logger = glog.Log()
	}
	return &OutboundRoute{
		entries: make(map[protoreflect.FullName]uint16),
		logger:  logger,
	}
}

func (r *OutboundRoute) Register(cmd, act uint8, message proto.Message) {
	if message == nil {
		return
	}
	id := protocol.CmdAct(cmd, act)
	name := message.ProtoReflect().Descriptor().FullName()

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, exists := r.entries[name]; exists {
		r.logger.Warn("s2c already registered", zap.Uint16("id", id), zap.Uint16("existing", existing))
		return
	}
	r.entries[name] = id
}

func (r *OutboundRoute) Resolve(message proto.Message) (uint8, uint8, bool) {
	if message == nil {
		return 0, 0, false
	}
	name := message.ProtoReflect().Descriptor().FullName()
	r.mu.RLock()
	id, exists := r.entries[name]
	r.mu.RUnlock()
	if !exists {
		return 0, 0, false
	}
	return uint8(id >> 8), uint8(id), true
}

func (r *OutboundRoute) Encode(message proto.Message) (*protocol.MessageFrame, error) {
	cmd, act, exists := r.Resolve(message)
	if !exists {
		return nil, ErrOutboundRouteNotFound
	}
	body, err := protocodec.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("agent marshal s2c err:%w", err)
	}
	return protocol.NewMessageFrame(cmd, act, 0, body), nil
}
