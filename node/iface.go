package node

import (
	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/cluster"
	"github.com/dzm2020/actormesh/logicalactor"
	"github.com/dzm2020/actormesh/pkg/component"
)

type NodeAPI interface {
	Startup() error
	GetID() string
	GetKind() string
	GetInstanceID() string
	GetClusterAddress() string
	AddComponent(components ...component.IComponent) error
	GetComponent(name string) component.IComponent
	GetCluster() cluster.ClusterAPI
	GetSystem() actor.SystemAPI
	GetActorRouter() logicalactor.ActorRouter
}

type NodeBehavior interface {
	OnInit(node NodeAPI) error
	OnStart(node NodeAPI) error
	OnStop(node NodeAPI, reason error)
}

type DefaultNodeBehavior struct{}

var _ NodeBehavior = DefaultNodeBehavior{}

func (DefaultNodeBehavior) OnInit(NodeAPI) error  { return nil }
func (DefaultNodeBehavior) OnStart(NodeAPI) error { return nil }
func (DefaultNodeBehavior) OnStop(NodeAPI, error) { return }
