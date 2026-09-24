package gateway

import (
	"context"
	"errors"
	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/grs"
	"sync"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
)

var (
	ErrSystemNil       = errors.New("gateway actor system is nil")
	ErrAgentSpawnerNil = errors.New("gateway agent spawner is nil")
)

type AgentSpawner func(network.Connection) (*actor.PID, error)

func New(server network.Server, system actor.SystemAPI, spawner AgentSpawner) *Gateway {
	gateway := &Gateway{
		server:  server,
		system:  system,
		spawner: spawner,
		pids:    maputil.NewConcurrentMap[int64, *actor.PID](10),
	}
	gateway.ctx, gateway.cancel = context.WithCancel(context.Background())
	gateway.SetName("gateway")
	gateway.logger = glog.With(zap.String("component", gateway.GetName()))
	return gateway
}

type Gateway struct {
	component.BaseComponent
	ctx      context.Context
	cancel   context.CancelFunc
	server   network.Server
	system   actor.SystemAPI
	spawner  AgentSpawner
	logger   *zap.Logger
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
				glog.Error("gateway run server err", zap.Error(err))
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
