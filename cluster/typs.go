package cluster

import (
	"game-server/framework/cluster/member"
	"game-server/framework/pkg/component"
)

type MessageHandler func(nodeID string, data []byte) error

// ClusterAPI defines cluster lifecycle, membership, metadata, and messaging capabilities.
type ClusterAPI interface {
	component.IComponent
	Join() error
	Leave() error
	AllMembers() []member.ServiceInstance
	Members(service string) map[string]member.ServiceInstance
	MemberById(serviceId string) (member.ServiceInstance, bool)

	SendToNode(nodeID string, data []byte) error
	Broadcast(data []byte) error
}
