package logicalactor

import (
	"errors"
	"strings"
	"time"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/pkg/component"

	logicalactorpb "github.com/dzm2020/actormesh/logicalactor/pb"

	"google.golang.org/protobuf/proto"
)

type ActorFactory func(ActorID) (actor.Actor, actor.SpawnOptions)

type ActorRouter interface {
	component.IComponent
	SetDirectory(ownerDirectory OwnerDirectory)
	RegisterFactory(kind string, factory ActorFactory) error
	RegisterPlacement(kind string, strategy PlacementStrategy) error
	CloseActor(actorID ActorID) error
	Tell(ctx actor.Context, actorID ActorID, message proto.Message) error
	Ask(ctx actor.Context, actorID ActorID, message proto.Message) (any, error)
	Forward(ctx actor.Context, actorID ActorID, message proto.Message) error
}
type OwnerDirectory interface {
	GetOwner(actorID ActorID) (owner NodeInfo, found bool, err error)
	AcquireOwner(actorID ActorID, candidate NodeInfo) (owner NodeInfo, acquired bool, err error)
	RenewOwner(actorID ActorID, expectedOwner NodeInfo) (renewed bool, err error)
	DeleteOwner(actorID ActorID, expectedOwner NodeInfo) (deleted bool, err error)
	LeaseTTL() time.Duration
}

type PlacementStrategy interface {
	PickNode(actorID ActorID) (NodeInfo, error)
}

type Discovery interface {
	Members(service string) map[string]NodeInfo
	MemberById(serviceID string) (NodeInfo, bool)
}

// ActorID actor逻辑ID，如  {Kind:player,Key:1001}
type ActorID struct {
	Kind string `json:"kind"`
	Key  string `json:"key"`
}

func (actorID ActorID) Validate() error {
	switch {
	case actorID.Kind == "" || strings.Contains(actorID.Kind, ":"):
		return errors.New("actor id invalid  kind")
	case actorID.Key == "" || strings.Contains(actorID.Key, ":"):
		return errors.New("actor id invalid  key")
	default:
		return nil
	}
}

func (actorID ActorID) String() string {
	return actorID.Kind + ":" + actorID.Key
}

func (actorID ActorID) Name() string {
	return "$logical:" + actorID.String()
}

type NodeInfo struct {
	NodeId     string `json:"node_id"`
	Kind       string `json:"kind"`
	InstanceId string `json:"instance_id"`
}

func (nodeInfo NodeInfo) Validate() error {
	switch {
	case strings.TrimSpace(nodeInfo.NodeId) == "":
		return errors.New("node info id is empty")
	case strings.TrimSpace(nodeInfo.InstanceId) == "":
		return errors.New("node info instance id is empty")
	case strings.TrimSpace(nodeInfo.Kind) == "":
		return errors.New("node info kind is empty")
	default:
		return nil
	}
}

func actorIDToProto(actorID ActorID) *logicalactorpb.ActorID {
	return &logicalactorpb.ActorID{Kind: actorID.Kind, Key: actorID.Key}
}
func actorIDFromProto(protoActorID *logicalactorpb.ActorID) ActorID {
	if protoActorID == nil {
		return ActorID{}
	}
	return ActorID{Kind: protoActorID.Kind, Key: protoActorID.Key}
}

func ownerToProto(owner NodeInfo) *logicalactorpb.Owner {
	return &logicalactorpb.Owner{
		NodeId:     owner.NodeId,
		InstanceId: owner.InstanceId,
	}
}
func ownerFromProto(owner *logicalactorpb.Owner) NodeInfo {
	if owner == nil {
		return NodeInfo{}
	}
	return NodeInfo{
		NodeId:     owner.NodeId,
		InstanceId: owner.InstanceId,
	}
}
