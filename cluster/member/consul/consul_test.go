package consul

import (
	"context"
	"game-server/framework/pkg/glog"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func startTestRegistry(t *testing.T) *Registry {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	registry := New()
	if err := registry.Run(ctx); err != nil {
		cancel()
		t.Skipf("consul unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = registry.Leave("node-1")
		cancel()
	})
	return registry
}

func waitForMembers(t *testing.T, registry *Registry, service string, want int) map[string]ServiceInstance {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		members := registry.Members(service)
		if len(members) == want {
			return members
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d members of %q; got %d", want, service, len(registry.Members(service)))
	return nil
}

func TestServiceJoin(t *testing.T) {
	consul := startTestRegistry(t)
	if err := consul.Join(ServiceInstance{
		ID:   "node-1",
		Name: "node",
	}); err != nil {
		t.Fatal(err)
	}

	members := waitForMembers(t, consul, "node", 1)
	glog.Info("get members success", zap.Any("members", members))
}

func TestServiceConcurrencyJoin(t *testing.T) {
	consul := startTestRegistry(t)
	if err := consul.Join(ServiceInstance{
		ID:   "node-1",
		Name: "node",
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := consul.Join(ServiceInstance{
				ID:   "node-1",
				Name: "node",
			}); err != nil {
				//glog.Error("consul join node error", zap.Error(err))
			}
		}()
	}
	wg.Wait()
}

func TestServiceConcurrencyLeave(t *testing.T) {
	consul := startTestRegistry(t)
	if err := consul.Join(ServiceInstance{
		ID:   "node-1",
		Name: "node",
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := consul.Leave("node-1"); err != nil {

			}
		}()
	}
	wg.Wait()
}
