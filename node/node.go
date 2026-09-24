package node

import (
	"errors"
	"fmt"
	"game-server/framework/actor"
	"game-server/framework/cluster"
	"game-server/framework/logicalactor"
	"game-server/framework/pkg/component"
	"time"
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
		options:        options,
		id:             options.ID,
		kind:           options.Kind,
		clusterAddress: options.ClusterAddress,
		createdAt:      time.Now(),
		manager:        component.NewComponentsMgr(),
	}
	return n
}

type Node struct {
	options            Options
	id                 string
	kind               string
	clusterAddress     string
	createdAt          time.Time
	manager            component.IManager
	system             actor.SystemAPI
	cluster            cluster.ClusterAPI
	logicalActorRouter logicalactor.ActorRouter
	phase              phase
}

func (n *Node) GetID() string {
	return n.id
}
func (n *Node) GetClusterAddress() string {
	return n.clusterAddress
}
func (n *Node) GetKind() string {
	return n.kind
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

func (n *Node) GetInstanceID() string {
	return fmt.Sprintf("%d", n.createdAt.UnixNano())
}

func (n *Node) GetActorRouter() logicalactor.ActorRouter {
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
