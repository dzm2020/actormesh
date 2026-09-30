package gateway

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/grs"

	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
)

var (
	ErrSystemNil       = errors.New("gateway actor system is nil")
	ErrAgentSpawnerNil = errors.New("gateway agent spawner is nil")
)

type ActorGateway interface {
	actor.EnvelopeSender
	actor.Supervisor
}

type AgentSpawner func(network.Connection) (*actor.PID, error)

type Options struct {
	Logger  *glog.Logger
	Server  network.Server
	System  ActorGateway
	spawner AgentSpawner
}

func (m *Options) logger() *glog.Logger {
	if m.Logger == nil {
		return glog.Log()
	}
	return m.Logger
}

func New(options Options) *Gateway {
	gateway := &Gateway{
		server:  options.Server,
		system:  options.System,
		spawner: options.spawner,
		pids:    maputil.NewConcurrentMap[int64, *actor.PID](10),
		logger:  options.logger(),
	}
	gateway.ctx, gateway.cancel = context.WithCancel(context.Background())
	gateway.SetName("gateway")
	gateway.logger = gateway.logger.With(zap.String("component", gateway.GetName()))
	return gateway
}

type Gateway struct {
	component.BaseComponent
	ctx      context.Context
	cancel   context.CancelFunc
	server   network.Server
	system   ActorGateway
	spawner  AgentSpawner
	logger   *glog.Logger
	pids     *maputil.ConcurrentMap[int64, *actor.PID]
	group    sync.WaitGroup
	acceptMu sync.RWMutex
	closing  bool
}

func (g *Gateway) Init() error {
	return g.GuardInit(func() error {
		switch {
		case g.system == nil:
			return ErrSystemNil
		case g.spawner == nil:
			return ErrAgentSpawnerNil
		}
		return nil
	})
}

func (g *Gateway) Start() error {

	return g.GuardStart(func() error {
		grs.SafeGo(func() {
			if err := g.server.Run(g.ctx, &connectionHandler{gateway: g}); err != nil {
				g.logger.Error("gateway run server err", zap.Error(err))
			}
		})
		return nil
	})
}

func (g *Gateway) Stop() error {
	return g.GuardStop(func() error {
		g.acceptMu.Lock()
		defer g.acceptMu.Unlock()
		g.closing = true
		//  关门监听
		g.cancel()
		//  停止所有agent  这里不能通过actor system停止触发,system 会不分顺序同时停止所有actor
		//  对于网关来说应该先拒绝连接,关闭agent 再关闭内部connection
		g.pids.Range(func(key int64, pid *actor.PID) bool {
			g.system.StopProcess(actor.NoSender, pid)
			return true
		})
		//  等待所有agent关闭完成
		return grs.WaitWithTimeout(&g.group, time.Second*10)
	})
}
