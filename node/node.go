package node

import (
	"errors"
	"fmt"
	"time"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/cluster"
	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/logicalactor"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
)

var _ NodeAPI = (*Node)(nil)

type phase int

const (
	phaseNew phase = iota
	phaseRunning
	phaseStopped
)

var (
	ErrNodeInvalidPhase = errors.New("node is not in new phase")
)

func New(options Options) *Node {
	n := &Node{
		node:    options.NodeInfo,
		options: options,
		manager: component.NewComponentsMgr(),
	}
	n.node.StartedAt = time.Now().UnixMilli()
	return n
}

type Node struct {
	node               member.NodeInfo
	options            Options
	clusterAddress     string
	manager            component.IManager
	system             actor.SystemAPI
	cluster            cluster.ClusterAPI
	logicalActorRouter logicalactor.ActorRouterAPI
	logger             *glog.Logger
	phase              phase
}

func (n *Node) Info() member.NodeInfo {
	return n.node
}
func (n *Node) GetOptions() *Options {
	return &n.options
}
func (n *Node) GetSystem() actor.SystemAPI {
	return n.system
}

func (n *Node) GetCluster() cluster.ClusterAPI {
	return n.cluster
}

func (n *Node) GetActorRouter() logicalactor.ActorRouterAPI {
	return n.logicalActorRouter
}

func (n *Node) AddComponent(components ...component.IComponent) error {
	if n.phase != phaseNew {
		return fmt.Errorf("phase:%v :%w", n.phase, ErrNodeInvalidPhase)
	}
	return n.manager.Add(components...)
}
func (n *Node) GetComponent(name string) component.IComponent {
	return n.manager.Get(name)
}
