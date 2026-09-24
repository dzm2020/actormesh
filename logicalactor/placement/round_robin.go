package placement

import (
	"errors"

	"sort"
	"sync/atomic"

	"game-server/framework/logicalactor"
)

var _ logicalactor.PlacementStrategy = (*RoundRobinStrategy)(nil)

var (
	ErrNodeProviderNil  = errors.New("dynamic round robin requires node provider")
	ErrNoAvailableNodes = errors.New("dynamic round robin has no available nodes")
)

type NodeProvider interface {
	Members(service string) map[string]logicalactor.NodeInfo
}

type RoundRobinStrategy struct {
	provider  NodeProvider
	nextIndex atomic.Uint64
}

func NewDynamicRoundRobinStrategy(provider NodeProvider) (*RoundRobinStrategy, error) {
	return &RoundRobinStrategy{provider: provider}, nil
}

func (strategy *RoundRobinStrategy) PickNode(actorID logicalactor.ActorID) (logicalactor.NodeInfo, error) {
	if strategy.provider == nil {
		return logicalactor.NodeInfo{}, ErrNodeProviderNil
	}
	nodes, err := strategy.currentNodes(actorID.Kind)
	if err != nil {
		return logicalactor.NodeInfo{}, err
	}
	index := strategy.nextIndex.Add(1) - 1
	return nodes[index%uint64(len(nodes))], nil
}

func (strategy *RoundRobinStrategy) currentNodes(kind string) ([]logicalactor.NodeInfo, error) {
	nodes := strategy.provider.Members(kind)
	if len(nodes) == 0 {
		return nil, ErrNoAvailableNodes
	}
	nodeList := make([]logicalactor.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		nodeList = append(nodeList, node)
	}
	sort.Slice(nodeList, func(i, j int) bool {
		return nodeList[i].NodeId < nodeList[j].NodeId
	})
	return nodeList, nil
}
