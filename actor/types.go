package actor

import (
	actorpb "github.com/dzm2020/actormesh/actor/pb"
	"time"
)

const (
	defaultActorMailboxSize = 1024
	defaultActorAskTimeout  = 3 * time.Second
)

type (
	Task func(ctx Context)
	PID  struct {
		ActorID   uint64 `json:"actor_id"`
		ActorName string `json:"actor_name"`
		NodeID    string `json:"node_id"`
	}

	Envelope struct {
		Payload any
		Sender  *PID
		Meta    MessageMeta
	}

	RequestRef struct {
		NodeID    string
		RequestID uint64
	}

	MessageMeta struct {
		Request *RequestRef
	}

	SpawnOptions struct {
		Name        string
		InitArgs    []any
		MailboxSize int32
	}
)

var NoSender = &PID{}

func NewPID(actorID uint64, actorName, nodeID string) *PID {
	return &PID{ActorID: actorID, ActorName: actorName, NodeID: nodeID}
}

func pidToProto(pid *PID) *actorpb.PID {
	if pid == nil {
		return nil
	}
	return &actorpb.PID{
		ActorId:   pid.ActorID,
		ActorName: pid.ActorName,
		NodeId:    pid.NodeID,
	}
}
func pidFromProto(pid *actorpb.PID) *PID {
	if pid == nil {
		return nil
	}
	return NewPID(pid.ActorId, pid.ActorName, pid.NodeId)
}

func requestRefFromProto(request *actorpb.RequestRef) *RequestRef {
	if request == nil {
		return nil
	}
	return &RequestRef{NodeID: request.NodeId, RequestID: request.RequestId}
}

func requestRefToProto(request *RequestRef) *actorpb.RequestRef {
	if request == nil {
		return nil
	}
	return &actorpb.RequestRef{NodeId: request.NodeID, RequestId: request.RequestID}
}
