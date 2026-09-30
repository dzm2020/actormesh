package member

import (
	"context"
	"errors"
)

type DiscoveryAPI interface {
	AllMembers() []NodeInfo
	Members(service string) map[string]NodeInfo
	MemberById(serviceID string) (NodeInfo, bool)
}

type RegistryAPI interface {
	DiscoveryAPI
	Run(ctx context.Context) error
	Join(node NodeInfo) error
	Update(node NodeInfo) error
	Leave(serviceId string) error
}

type NodeInfo struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Address   string            `json:"address"`              // 集群监听地址 "10.0.0.1:8000"
	Status    string            `json:"status,omitempty"`     // "passing" / "warning" / "critical"
	StartedAt int64             `json:"started_at,omitempty"` // 启动时间
	Version   string            `json:"version,omitempty"`    // 应用版本
	Meta      map[string]string `json:"metadata,omitempty"`   // 业务扩展
}

func (m *NodeInfo) Validate() error {
	switch {
	case m.ID == "":
		return errors.New("node id is required")
	case m.Name == "":
		return errors.New("node name is required")
	default:
		return nil
	}
}

func (m *NodeInfo) Clone() NodeInfo {
	clone := NodeInfo{
		ID:        m.ID,
		Name:      m.Name,
		Address:   m.Address,
		Status:    m.Status,
		StartedAt: m.StartedAt,
		Version:   m.Version,
	}
	if m.Meta != nil {
		clone.Meta = make(map[string]string, len(m.Meta))
		for key, value := range m.Meta {
			clone.Meta[key] = value
		}
	}
	return clone
}
