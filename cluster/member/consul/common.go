package consul

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/pkg/serialize/jsoncodec"
	"github.com/hashicorp/consul/api"
)

const nodeInfoMetaKey = "node_info"

func serviceCheckID(serviceID string) string {
	return fmt.Sprintf("service:%s", serviceID)
}

func entriesToInstances(entries []*api.ServiceEntry) map[string]member.NodeInfo {
	instances := make(map[string]member.NodeInfo, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.Service == nil {
			continue
		}
		address := entry.Service.Address
		if entry.Service.Port > 0 {
			address = net.JoinHostPort(address, fmt.Sprintf("%d", entry.Service.Port))
		}
		node := member.NodeInfo{
			ID:      entry.Service.ID,
			Name:    entry.Service.Service,
			Address: address,
			Meta:    cloneMeta(entry.Service.Meta),
		}
		if encoded := entry.Service.Meta[nodeInfoMetaKey]; encoded != "" {
			var stored member.NodeInfo
			if err := jsoncodec.Unmarshal([]byte(encoded), &stored); err == nil {
				node = stored
				if node.Address == "" {
					node.Address = address
				}
			}
			delete(node.Meta, nodeInfoMetaKey)
		}
		if status := serviceStatus(entry); status != "" {
			node.Status = status
		}
		instances[entry.Service.ID] = node
	}
	return instances
}

func instanceToRegistration(service member.NodeInfo, options Options) (*api.AgentServiceRegistration, error) {
	address, port, err := splitAddress(service.Address)
	if err != nil {
		return nil, err
	}
	meta := cloneMeta(service.Meta)
	delete(meta, nodeInfoMetaKey)
	stored := service.Clone()
	stored.Meta = cloneMeta(meta)
	payload, err := jsoncodec.Marshal(stored)
	if err != nil {
		return nil, fmt.Errorf("marshal node info: %w", err)
	}
	if meta == nil {
		meta = make(map[string]string, 1)
	}
	meta[nodeInfoMetaKey] = string(payload)

	check := &api.AgentServiceCheck{
		CheckID:                        serviceCheckID(service.ID),
		TTL:                            options.TTL.String(),
		DeregisterCriticalServiceAfter: options.DeregisterAfter.String(),
	}
	reg := &api.AgentServiceRegistration{
		ID:      service.ID,
		Name:    service.Name,
		Address: address,
		Port:    port,
		Meta:    meta,
		Check:   check,
	}
	return reg, nil
}

func cloneMeta(meta map[string]string) map[string]string {
	if meta == nil {
		return nil
	}
	clone := make(map[string]string, len(meta))
	for key, value := range meta {
		clone[key] = value
	}
	return clone
}

func serviceStatus(entry *api.ServiceEntry) string {
	status := ""
	for _, check := range entry.Checks {
		if check == nil {
			continue
		}
		switch check.Status {
		case api.HealthCritical:
			return api.HealthCritical
		case api.HealthWarning:
			status = api.HealthWarning
		case api.HealthPassing:
			if status == "" {
				status = api.HealthPassing
			}
		}
	}
	return status
}

func splitAddress(address string) (string, int, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", 0, nil
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("invalid node address %q: %w", address, err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return "", 0, fmt.Errorf("invalid node port %q", port)
	}
	return host, portNumber, nil
}
