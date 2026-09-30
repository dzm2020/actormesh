package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/cluster/transport"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/grs"

	"go.uber.org/zap"
)

var _ ClusterAPI = (*Cluster)(nil)

type Options struct {
	NodeInfo  member.NodeInfo
	Registry  member.RegistryAPI
	Handler   MessageHandler
	Transport transport.TransportAPI
	Logger    *glog.Logger
}

func NewWithOptions(options Options) *Cluster {
	c := &Cluster{
		node:      options.NodeInfo,
		registry:  options.Registry,
		transport: options.Transport,
		logger:    options.Logger,
		handler:   options.Handler,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.SetName("cluster")
	if c.logger == nil {
		c.logger = glog.Log().With(zap.String("component", c.GetName()))
	}
	return c
}

type Cluster struct {
	component.BaseComponent
	handler   MessageHandler
	node      member.NodeInfo
	registry  member.RegistryAPI // 集群发现器
	transport transport.TransportAPI
	logger    *glog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	leaveOnce sync.Once
}

func (c *Cluster) Init() error {
	return c.GuardInit(func() error {
		if err := c.node.Validate(); err != nil {
			return err
		}
		if c.registry == nil {
			return errors.New("cluster registry is nil")
		}
		if c.transport == nil {
			return errors.New("cluster transport is nil")
		}
		if c.handler == nil {
			return errors.New("cluster handler is nil")
		}
		return nil
	})
}

func (c *Cluster) Start() error {
	return c.GuardStart(func() error {
		if err := c.transport.Run(); err != nil {
			return err
		}
		if err := c.registry.Run(c.ctx); err != nil {
			return err
		}
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
			for _, instance := range c.registry.AllMembers() {
				c.connectMember(instance)
			}
		}
	}
}

func (c *Cluster) connectMember(node member.NodeInfo) {
	if c.node.ID >= node.ID {
		return
	}
	state := c.transport.ConnectionState(node.ID)
	if state == transport.PeerStateConnected || state == transport.PeerStateHandshaking {
		return
	}
	address := node.Address
	grs.SafeGo(func() {
		if err := c.transport.Connect(node.ID, address, time.Second*5); err != nil {
			c.logger.Warn("cluster connect member failed",
				zap.String("nodeId", node.ID),
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
	return c.registry.Join(c.node)
}

func (c *Cluster) Leave() error {
	return c.registry.Leave(c.node.ID)
}

func (c *Cluster) Members(nodeId string) map[string]member.NodeInfo {
	return c.registry.Members(nodeId)
}

func (c *Cluster) MemberById(nodeId string) (member.NodeInfo, bool) {
	return c.registry.MemberById(nodeId)
}
func (c *Cluster) AllMembers() []member.NodeInfo {
	return c.registry.AllMembers()
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
