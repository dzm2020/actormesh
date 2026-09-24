package member

import (
	"context"
	"errors"
)

type Discovery interface {
	AllMembers() []ServiceInstance
	Members(service string) map[string]ServiceInstance
	MemberById(serviceID string) (ServiceInstance, bool)
}

type MemberManager interface {
	Discovery
	Run(ctx context.Context) error
	Join(instance ServiceInstance) error
	Update(instance ServiceInstance) error
	Leave(serviceId string) error
}

type ServiceInstance struct {
	ID      string
	Name    string
	Address string
	Port    int
	Meta    map[string]string
}

func (m *ServiceInstance) Validate() error {
	switch {
	case m.ID == "":
		return errors.New("service id is required")
	case m.Name == "":
		return errors.New("service name is required")
	default:
		return nil
	}
}

func (m *ServiceInstance) Clone() ServiceInstance {
	clone := ServiceInstance{
		ID:      m.ID,
		Name:    m.Name,
		Address: m.Address,
		Port:    m.Port,
	}
	if m.Meta != nil {
		clone.Meta = make(map[string]string, len(m.Meta))
		for key, value := range m.Meta {
			clone.Meta[key] = value
		}
	}
	return clone
}
