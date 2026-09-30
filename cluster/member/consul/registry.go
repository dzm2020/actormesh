package consul

import (
	"context"
	"fmt"

	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/pkg/glog"

	"sync/atomic"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// ServiceInstance is kept as a compatibility alias for existing Consul users.
type ServiceInstance = member.NodeInfo

var _ member.RegistryAPI = (*Registry)(nil)

func New() *Registry {
	return NewWithOptions(Options{})
}

func NewWithOptions(options Options) *Registry {
	r := &Registry{
		options:      normalizeOptions(options),
		subscription: NewSubscriptionCenter(),
		logger:       options.logger(),
	}
	return r
}

type Registry struct {
	options      Options
	client       *api.Client
	state        atomic.Uint32
	ctx          context.Context
	cancel       context.CancelFunc
	watcher      *watcher
	reg          *registration
	subscription *SubscriptionCenter
	logger       *glog.Logger
}

func (r *Registry) Run(ctx context.Context) error {
	r.ctx, r.cancel = context.WithCancel(ctx)
	//  初始化客户端
	if err := r.initClient(); err != nil {
		return err
	}
	//  初始化注册中心
	r.reg = newRegistration(r)
	//  初始化watcher
	r.watcher = newWatcher(r, r.ctx)
	r.logger.Info("consul init", zap.String("address", r.options.Address))
	return nil
}

func (r *Registry) initClient() error {
	client, err := api.NewClient(toConsulConfig(r.options))
	if err != nil {
		return fmt.Errorf("consul init client err: %w", err)
	}

	if _, err = client.Agent().Self(); err != nil {
		return fmt.Errorf("consul init client err: %w", err)
	}
	r.client = client
	return nil
}

func (r *Registry) Join(node member.NodeInfo) error {
	return r.reg.join(node)
}

func (r *Registry) Leave(serviceId string) error {
	return r.reg.leave(serviceId)
}

func (r *Registry) Update(node member.NodeInfo) error {
	return r.reg.join(node)
}

func (r *Registry) Members(service string) map[string]member.NodeInfo {
	members, _ := r.watcher.caches.Get(service)
	//  clone 数据避免并发读写map
	ret := make(map[string]member.NodeInfo)
	for _, m := range members {
		ret[m.ID] = m.Clone()
	}
	return ret
}

func (r *Registry) MemberById(serviceId string) (member.NodeInfo, bool) {
	var result member.NodeInfo
	var ok bool
	r.watcher.caches.Range(func(key string, value map[string]ServiceInstance) bool {
		result, ok = value[serviceId]
		if !ok {
			return true
		}
		return false
	})
	//  clone 数据避免并发读写map
	return result.Clone(), ok
}

func (r *Registry) AllMembers() []member.NodeInfo {
	var result []member.NodeInfo
	r.watcher.caches.Range(func(key string, dict map[string]ServiceInstance) bool {
		for _, instance := range dict {
			result = append(result, instance)
		}
		return true
	})
	return result
}
