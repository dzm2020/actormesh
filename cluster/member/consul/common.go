package consul

import (
	"fmt"

	"github.com/hashicorp/consul/api"
)

func serviceCheckID(serviceID string) string {
	return fmt.Sprintf("service:%s", serviceID)
}

func entriesToInstances(entries []*api.ServiceEntry) map[string]ServiceInstance {
	instances := make(map[string]ServiceInstance, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.Service == nil {
			continue
		}
		instances[entry.Service.ID] = ServiceInstance{
			ID:      entry.Service.ID,
			Name:    entry.Service.Service,
			Address: entry.Service.Address,
			Port:    entry.Service.Port,
			Meta:    entry.Service.Meta,
		}
	}
	return instances
}

func instanceToRegistration(service ServiceInstance, options Options) (*api.AgentServiceRegistration, error) {
	check := &api.AgentServiceCheck{
		CheckID:                        serviceCheckID(service.ID),
		TTL:                            options.TTL.String(),
		DeregisterCriticalServiceAfter: options.DeregisterAfter.String(),
	}
	reg := &api.AgentServiceRegistration{
		ID:      service.ID,
		Name:    service.Name,
		Address: service.Address,
		Port:    service.Port,
		Meta:    service.Meta,
		Check:   check,
	}
	return reg, nil
}
