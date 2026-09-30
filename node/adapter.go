package node

import (
	"fmt"

	"github.com/dzm2020/actormesh/cluster"

	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/logicalactor"
)

type MemberSource interface {
	Members(service string) (map[string]member.NodeInfo, error)
	MemberByID(serviceID string) (member.NodeInfo, bool)
}

type ActorNodeAdapter struct {
	cluster cluster.ClusterAPI
}

var _ logicalactor.Discovery = (*ActorNodeAdapter)(nil)

func NewActorNodeAdapter(cluster cluster.ClusterAPI) *ActorNodeAdapter {
	return &ActorNodeAdapter{cluster: cluster}
}
func (p *ActorNodeAdapter) Members(service string) map[string]logicalactor.NodeInfo {
	nodes := p.cluster.Members(service)
	result := make(map[string]logicalactor.NodeInfo, len(nodes))
	for s, instance := range nodes {
		result[s] = newLogicalActorNode(instance)
	}
	return result
}

func (p *ActorNodeAdapter) MemberById(serviceID string) (logicalactor.NodeInfo, bool) {
	node, ok := p.cluster.MemberById(serviceID)
	return newLogicalActorNode(node), ok
}

func newLogicalActorNode(node member.NodeInfo) logicalactor.NodeInfo {
	instanceId := fmt.Sprintf("%d", node.StartedAt)
	return logicalactor.NodeInfo{NodeId: node.ID, Kind: node.Name, InstanceId: instanceId}
}
