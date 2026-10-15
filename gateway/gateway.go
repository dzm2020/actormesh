package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/gateway/route"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/grs"

	"go.uber.org/zap"
)

var (
	ErrSystemNil       = errors.New("gateway actor system is nil")
	ErrAgentSpawnerNil = errors.New("gateway agent spawner is nil")
)

type ActorGateway interface {
	actor.EnvelopeSender
	actor.Supervisor
	actor.Spawner
}

type AgentSpawner func(agent Agent) (actor.Actor, actor.SpawnOptions)

type Options struct {
	Logger  *glog.Logger
	Server  network.Server
	System  ActorGateway
	Spawner AgentSpawner
	Route   route.ClientRoute
}

func (m *Options) logger() *glog.Logger {
	if m.Logger == nil {
		return glog.Log()
	}
	return m.Logger
}

func New(options Options) *Gateway {
	gateway := &Gateway{
		server: options.Server,
		bridge: NewGatewayBridge(options.System, options.Spawner, options.Route),
		logger: options.logger(),
	}
	gateway.ctx, gateway.cancel = context.WithCancel(context.Background())
	gateway.SetName("gateway")
	gateway.logger = gateway.logger.With(zap.String("component", gateway.GetName()))
	return gateway
}

type Gateway struct {
	component.BaseComponent
	ctx    context.Context
	cancel context.CancelFunc
	server network.Server
	bridge *Bridge
	logger *glog.Logger
}

func (g *Gateway) Init() error {
	return g.GuardInit(func() error {
		return g.bridge.Validate()
	})
}

func (g *Gateway) Start() error {
	return g.GuardStart(func() error {
		grs.SafeGo(func() {
			if err := g.server.Run(g.ctx, g.bridge); err != nil {
				g.logger.Error("gateway run server err", zap.Error(err))
			}
		})
		return nil
	})
}

func (g *Gateway) Stop() error {
	return g.GuardStop(func() error {
		g.bridge.rejectNewConnections()
		g.cancel()
		return g.bridge.stopAgents(10 * time.Second)
	})
}
