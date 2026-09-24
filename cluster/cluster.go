package cluster

import (
	"context"
	"errors"
	"game-server/framework/cluster/member"
	memberconsul "game-server/framework/cluster/member/consul"
	"game-server/framework/cluster/transport"
	transportnet "game-server/framework/cluster/transport/nettransport"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/grs"
	"game-server/framework/pkg/netutil"
	"sync"
	"time"

	"go.uber.org/zap"
)

var _ ClusterAPI = (*Cluster)(nil)

type Options struct {
	MemberManager member.MemberManager
	Transport     transport.Transport
}

func New(instance member.ServiceInstance, handler MessageHandler) *Cluster {
	return NewWithOptions(instance, handler, Options{})
}

func NewWithOptions(instance member.ServiceInstance, handler MessageHandler, options Options) *Cluster {
	c := &Cluster{
		handler:       handler,
		local:         instance,
		memberManager: options.MemberManager,
		transport:     options.Transport,
	}
	if c.memberManager == nil {
		c.memberManager = memberconsul.New()
	}
	if c.transport == nil {
		c.transport = transportnet.NewTransport(c.local.ID)
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.SetName("cluster")
	c.logger = glog.With(zap.String("component", c.GetName()))
	return c
}

type Cluster struct {
	component.BaseComponent
	local         member.ServiceInstance
	memberManager member.MemberManager // 集群发现器
	transport     transport.Transport
	handler       MessageHandler
	logger        *zap.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	leaveOnce     sync.Once
}

func (c *Cluster) Init() error {
	return c.GuardInit(func() error {
		if c.handler == nil {
			return errors.New("cluster handler is nil")
		}
		if err := c.local.Validate(); err != nil {
			return err
		}
		return nil
	})
}

func (c *Cluster) Start() error {
	return c.GuardStart(func() error {

		if err := c.memberManager.Run(c.ctx); err != nil {
			return err
		}

		grs.SafeGo(func() {
			address := netutil.EndpointAddress(c.local.Address, c.local.Port)
			if err := c.transport.ListenAndServe(address, transport.MessageHandler(c.handler)); err != nil {
				_ = c.Leave()
				glog.Error("cluster listen failed", zap.Error(err))
				return
			}
		})

		grs.SafeGo(func() {
			c.runConnector()
		})

		if err := c.Join(); err != nil {
			return err
		}
		return nil
	})
}

func (c *Cluster) runConnector() {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
			for _, instance := range c.memberManager.AllMembers() {
				c.connectMember(instance)
			}
		}
	}
}

func (c *Cluster) connectMember(instance member.ServiceInstance) {
	if c.local.ID >= instance.ID {
		return
	}

	state := c.transport.ConnectionState(instance.ID)
	if state == transport.PeerStateConnected || state == transport.PeerStateHandshaking {
		return
	}
	address := netutil.EndpointAddress(instance.Address, instance.Port)
	grs.SafeGo(func() {
		if err := c.transport.Connect(instance.ID, address, transport.MessageHandler(c.handler), time.Second*5); err != nil {
			c.logger.Warn("cluster connect member failed",
				zap.String("nodeId", instance.ID),
				zap.String("address", address), zap.Error(err))
		}
	})
}

func (c *Cluster) SendToNode(nodeID string, data []byte) error {
	return c.transport.Send(nodeID, data)
}

func (c *Cluster) Broadcast(data []byte) error {
	return c.transport.Broadcast(data)
}

func (c *Cluster) Join() error {
	return c.memberManager.Join(c.local)
}

func (c *Cluster) Leave() error {
	return c.memberManager.Leave(c.local.ID)
}

func (c *Cluster) Members(service string) map[string]member.ServiceInstance {
	return c.memberManager.Members(service)
}

func (c *Cluster) MemberById(serviceId string) (member.ServiceInstance, bool) {
	return c.memberManager.MemberById(serviceId)
}
func (c *Cluster) AllMembers() []member.ServiceInstance {
	return c.memberManager.AllMembers()
}

func (c *Cluster) Stop() error {
	return c.GuardStop(func() error {
		if c.cancel != nil {
			c.cancel()
		}
		if c.transport != nil {
			c.transport.Close()
		}
		return nil
	})
}
