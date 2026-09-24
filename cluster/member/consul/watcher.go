package consul

import (
	"context"
	"fmt"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/grs"
	"sort"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

type watcher struct {
	registry *Registry
	caches   *maputil.ConcurrentMap[string, map[string]ServiceInstance]
	runGroup *grs.Group
}

func newWatcher(registry *Registry, ctx context.Context) *watcher {
	w := &watcher{
		caches:   maputil.NewConcurrentMap[string, map[string]ServiceInstance](1),
		registry: registry,
		runGroup: grs.NewGroup(ctx),
	}
	w.start()
	return w
}

func (w *watcher) start() {
	w.runGroup.Go(func(ctx context.Context) {
		w.watchCatalog(ctx)
	})
}

func (w *watcher) watchCatalog(ctx context.Context) {
	lastIndex := uint64(0)
	workers := make(map[string]context.CancelFunc)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		services, meta, err := w.services(lastIndex)
		if err != nil {
			glog.Warn("consul catalog watch failed, retrying", zap.Error(err))
			if !sleepContext(ctx, defaultWatchRetryDelay) {
				return
			}
			continue
		}
		if meta != nil {
			lastIndex = meta.LastIndex
		}
		w.reconcileServices(services, workers)
	}
}

// 拉去所有服务
func (w *watcher) services(lastIndex uint64) ([]string, *api.QueryMeta, error) {
	query := &api.QueryOptions{
		WaitIndex: lastIndex,
		WaitTime:  defaultWatchWaitTime,
	}

	servicesMap, meta, err := w.registry.client.Catalog().Services(query)
	if err != nil {
		return nil, nil, fmt.Errorf("catlog services err:%w", err)
	}
	services := make([]string, 0, len(servicesMap))
	for service := range servicesMap {
		if service != "" && service != "consul" {
			services = append(services, service)
		}
	}
	sort.Strings(services)
	return services, meta, nil
}

// 计算出新启动的服务 和停止的服务
func (w *watcher) reconcileServices(services []string, works map[string]context.CancelFunc) {
	current := make(map[string]struct{}, len(services))
	for _, service := range services {
		current[service] = struct{}{}
		if _, ok := works[service]; ok {
			continue
		} else {
			serviceCtx, cancel := context.WithCancel(w.runGroup.Context())
			works[service] = cancel

			w.runGroup.Go(func(ctx context.Context) {
				w.watchService(serviceCtx, service)
			})
		}
	}

	for service, cancel := range works {
		if _, exists := current[service]; !exists {
			if cancel != nil {
				cancel()
			}
			delete(works, service)
		}
	}
}

// 监控服务实例
func (w *watcher) watchService(ctx context.Context, service string) {
	defer func() {
		w.caches.Delete(service)
	}()
	var lastIndex uint64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		instances, meta, err := w.fetchService(service, lastIndex)
		if err != nil {
			glog.Warn("consul service watch failed, retrying",
				zap.String("service", service),
				zap.Error(err),
			)
			if !sleepContext(ctx, defaultWatchRetryDelay) {
				return
			}
			continue
		}
		if meta != nil {
			lastIndex = meta.LastIndex
		}
		w.caches.Set(service, instances)
	}
}

// 拉去服务实例
func (w *watcher) fetchService(service string, waitIndex uint64) (map[string]ServiceInstance, *api.QueryMeta, error) {
	query := &api.QueryOptions{
		WaitIndex: waitIndex,
		WaitTime:  defaultWatchWaitTime,
	}

	entries, meta, err := w.registry.client.Health().Service(service, "", true, query)
	if err != nil {
		return nil, nil, err
	}
	return entriesToInstances(entries), meta, nil
}

func (w *watcher) stop(ctx context.Context) error {
	w.runGroup.Cancel()
	return w.runGroup.Wait(ctx)
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
