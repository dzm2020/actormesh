package node

import (
	"game-server/framework/cluster"
	"strings"

	"game-server/framework/cluster/member"
	"game-server/framework/logicalactor"
)

const NodeInstanceIDMetaKey = "node_instance_id"

type MemberSource interface {
	Members(service string) (map[string]member.ServiceInstance, error)
	MemberByID(serviceID string) (member.ServiceInstance, bool)
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

func newLogicalActorNode(instance member.ServiceInstance) logicalactor.NodeInfo {
	instanceId := instanceID(instance)
	return logicalactor.NodeInfo{NodeId: instance.ID, Kind: instance.Name, InstanceId: instanceId}
}

func instanceID(instance member.ServiceInstance) string {
	if instance.Meta == nil {
		return ""
	}
	return strings.TrimSpace(instance.Meta[NodeInstanceIDMetaKey])
}
