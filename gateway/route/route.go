package route

import (
	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/pkg/glog"

	"google.golang.org/protobuf/proto"
)

type Handler func(ctx actor.Context, request any) error

type ClientRoute interface {
	Register(cmd, act uint8, handler Handler, c2s, s2c proto.Message)
	InboundRouter() InboundRouter
	OutboundRouter() OutboundRouter
}

var _ ClientRoute = (*Route)(nil)

type Route struct {
	*InboundRoute
	*OutboundRoute
}

func NewRoute(logger *glog.Logger) *Route {
	return &Route{
		InboundRoute:  NewInboundRoute(logger),
		OutboundRoute: NewOutboundRoute(logger),
	}
}

func (r *Route) Register(cmd, act uint8, handler Handler, c2s, s2c proto.Message) {
	r.InboundRoute.Register(cmd, act, handler, c2s)
	r.OutboundRoute.Register(cmd, act, s2c)
}

func (r *Route) InboundRouter() InboundRouter {
	return r.InboundRoute
}
func (r *Route) OutboundRouter() OutboundRouter {
	return r.OutboundRoute
}
