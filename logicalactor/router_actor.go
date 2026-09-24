package logicalactor

import (
	"errors"
	"fmt"

	"github.com/dzm2020/actormesh/actor"
	logicalactorpb "github.com/dzm2020/actormesh/logicalactor/pb"
	"github.com/dzm2020/actormesh/pkg/glog"

	"go.uber.org/zap"
)

const (
	RouterActorName  = "$logical-router"
	maxRouteForwards = uint32(3)
)

type closeActorRequest struct {
	actorID ActorID
}

type RouterActor struct {
	actor.DefaultActor
	router *Router
}

func (r *RouterActor) HandleTell(ctx actor.Context, message any) {
	if err := r.handleMessage(ctx, message); err != nil {
		ctx.Logger().Error("logical route actor handle tell message error", zap.Error(err))
	}
}

func (r *RouterActor) HandleAsk(ctx actor.Context, message any) (any, error) {
	return nil, r.handleMessage(ctx, message)
}

func (r *RouterActor) handleMessage(ctx actor.Context, message any) error {
	switch msg := message.(type) {
	//case closeActorRequest:
	//	if err := r.closeActor(ctx, msg.actorID); err != nil {
	//		return fmt.Errorf("close actor %s: %w", msg.actorID.String(), err)
	//	}
	case *logicalactorpb.RouteRequest:
		if err := r.routeMessage(ctx, msg); err != nil {
			return fmt.Errorf("route message: %w", err)
		}
	default:
		return fmt.Errorf("unknown msg type: %T", msg)
	}
	return nil
}

//func (r *RouterActor) closeActor(ctx actor.Context, actorId ActorID) error {
//	target := actor.NewPID(0, actorId.Name(), r.router.localNodeID())
//	owner, found, err := r.router.directory.GetOwner(actorId)
//	if err != nil {
//		return err
//	}
//	if found && !r.router.isLocal(owner) {
//		return ErrOwnerMismatch
//	}
//	r.router.actorSystem.StopProcess(ctx.Sender(), target)
//	return nil
//}

func (r *RouterActor) routeMessage(ctx actor.Context, request *logicalactorpb.RouteRequest) error {
	if request == nil {
		return errors.New("logical route request is nil")
	}
	actorId := actorIDFromProto(request.ActorId)
	if request.Message == nil {
		return errors.New("logical route message is nil")
	}
	payload, err := request.Message.UnmarshalNew()
	if err != nil {
		return fmt.Errorf("decode logical route message: %w", err)
	}
	if request.ForwardCount >= maxRouteForwards {
		return ErrRouteForwardLimit
	}

	//  检测本地是否已存在actor
	system := r.router.actorSystem
	target := actor.NewPID(0, actorId.Name(), system.GetNodeID())
	if system.Has(target) {
		// 并不保证一定成功  process 关闭中 队列满了
		if err = ctx.Forward(target, payload); err != nil {
			return fmt.Errorf("target:%v local forward: %w", target, err)
		}
		return nil
	}
	//  本地不存在 则抢占一次确保激活actor前 本机电获取到actor所有权
	candidate := r.router.local
	owner, acquired, err := r.router.directory.AcquireOwner(actorId, candidate)
	if err != nil {
		return fmt.Errorf("acquire owner: %w", err)
	}
	//  没有抢占到所有权 直接转发所有权节点
	if !acquired && !r.router.isLocal(owner) {
		return r.forwardToOwner(ctx, request, owner)
	}

	//  抢占到所有权 激活actor
	actorPID, err := r.activateActor(actorId, owner)
	if err != nil {
		if _, deleteErr := r.router.directory.DeleteOwner(actorId, owner); deleteErr != nil {
			glog.Error("logical actor owner cleanup failed", zap.String("actor_id", actorId.String()), zap.Error(deleteErr))
		}
		return fmt.Errorf("activate actor :%w", err)
	}
	if err = ctx.Forward(actorPID, payload); err != nil {
		return fmt.Errorf("forward activate actor%v :%w", actorPID, err)
	}
	return nil
}

func (r *RouterActor) forwardToOwner(ctx actor.Context, request *logicalactorpb.RouteRequest, owner NodeInfo) error {
	forwardRequest := &logicalactorpb.RouteRequest{
		ActorId:      request.ActorId,
		Owner:        ownerToProto(owner),
		Message:      request.Message,
		ForwardCount: request.ForwardCount + 1,
	}

	target := actor.NewPID(0, RouterActorName, owner.NodeId)
	if err := ctx.Forward(target, forwardRequest); err != nil {
		return fmt.Errorf("forward to remote owner %s: %w", owner.NodeId, err)
	}
	return nil
}

// activateActor 激活actor
func (r *RouterActor) activateActor(actorID ActorID, owner NodeInfo) (*actor.PID, error) {
	factory, ok := r.router.factories.Get(actorID.Kind)
	if !ok {
		return nil, ErrFactoryNotFound
	}
	handler, options := factory(actorID)
	if handler == nil {
		return nil, ErrActorFactoryReturnedNil
	}
	handler = &ownedActor{actorInstance: handler, actorId: actorID, owner: owner, routerRuntime: r.router}
	options.InitArgs = append(options.InitArgs, actorID)
	options.Name = actorID.Name()
	return r.router.actorSystem.SpawnActor(handler, options)
}

type ownedActor struct {
	actorInstance actor.Actor
	actorId       ActorID
	owner         NodeInfo
	routerRuntime *Router
}

func (owned *ownedActor) Init(ctx actor.Context) {
	owned.actorInstance.Init(ctx)
}

func (owned *ownedActor) HandleTell(ctx actor.Context, message any) {
	owned.actorInstance.HandleTell(ctx, message)
}

func (owned *ownedActor) HandleAsk(ctx actor.Context, message any) (any, error) {
	return owned.actorInstance.HandleAsk(ctx, message)
}

func (owned *ownedActor) Panic(ctx actor.Context, message any) {
	owned.actorInstance.Panic(ctx, message)
}

func (owned *ownedActor) Destroy(ctx actor.Context) {
	//  通知路由actor删除
	if _, err := owned.routerRuntime.directory.DeleteOwner(owned.actorId, owned.owner); err != nil {
		ctx.Logger().Error("delete owner failed", zap.Error(err))
	}
	owned.actorInstance.Destroy(ctx)
}
