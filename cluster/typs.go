package cluster

import (
	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/pkg/component"
)

type MessageHandler func(nodeID string, data []byte) error

// ClusterAPI defines cluster lifecycle, membership, metadata, and messaging capabilities.
type ClusterAPI interface {
	component.IComponent
	Join() error
	Leave() error
	AllMembers() []member.NodeInfo
	Members(service string) map[string]member.NodeInfo
	MemberById(serviceId string) (member.NodeInfo, bool)

	SendToNode(nodeID string, data []byte) error
	Broadcast(data []byte) error
}
