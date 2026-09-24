package consul

import (
	"context"
	"fmt"

	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/grs"
	"sync"

	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

type registration struct {
	registry *Registry

	keepersLock sync.RWMutex
	keepers     map[string]context.CancelFunc
}

func newRegistration(registry *Registry) *registration {
	return &registration{
		registry: registry,
		keepers:  make(map[string]context.CancelFunc),
	}
}

func (r *registration) join(instance ServiceInstance) error {
	options := r.registry.options
	client := r.registry.client
	serviceId := instance.ID
	//  无效实例
	if err := instance.Validate(); err != nil {
		return fmt.Errorf("consul service join err:%w", err)
	}

	instance = instance.Clone()
	serviceReg, err := instanceToRegistration(instance, options)
	if err != nil {
		return fmt.Errorf("consul service join err:%w", err)
	}
	//  注册服务
	if err = client.Agent().ServiceRegister(serviceReg); err != nil {
		return fmt.Errorf("consul service join err:%w", err)
	}
	r.keepersLock.Lock()
	if _, ok := r.keepers[serviceId]; ok {
		r.keepersLock.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(r.registry.ctx)
	r.keepers[serviceId] = cancel
	r.keepersLock.Unlock()

	//  启动保活
	grs.SafeGo(func() {
		r.runKeeper(ctx, serviceId)
	})

	glog.Info("consul service joined",
		zap.String("serviceId", serviceId),
		zap.String("serviceName", instance.Name))

	return nil
}

func (r *registration) runKeeper(ctx context.Context, serviceId string) {
	options := r.registry.options
	ticker := time.NewTicker(options.TTL / 2)
	defer ticker.Stop()
	r.keepAlive(ctx, serviceId)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.keepAlive(ctx, serviceId)
		}
	}
}

func (r *registration) keepAlive(ctx context.Context, serviceId string) {
	client := r.registry.client
	if serviceId == "" {
		glog.Error("consul service keepAlive", zap.String("serviceId", serviceId))
		return
	}
	checkID := serviceCheckID(serviceId)
	if ctx.Err() != nil {
		return
	}
	if err := client.Agent().UpdateTTL(checkID, "", api.HealthPassing); err != nil {
		glog.Error("consul service keepAlive", zap.String("serviceId", serviceId), zap.Error(err))
		return
	}
	return
}

func (r *registration) leave(serviceId string) error {
	if r == nil {
		return nil
	}
	if r.registry.client == nil {
		return nil
	}
	client := r.registry.client

	r.keepersLock.Lock()
	cancel, ok := r.keepers[serviceId]
	if !ok {
		r.keepersLock.Unlock()
		return nil
	}
	cancel()
	delete(r.keepers, serviceId)
	r.keepersLock.Unlock()

	if err := client.Agent().ServiceDeregister(serviceId); err != nil {
		return err
	}

	glog.Info("consul service leave", zap.String("serviceId", serviceId))
	return nil
}
