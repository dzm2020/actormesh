package logicalactor

import (
	"errors"
	"fmt"
	"game-server/framework/actor"
	logicalactorpb "game-server/framework/logicalactor/pb"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/serialize/protocodec"
	"strings"
	"sync"

	"github.com/duke-git/lancet/v2/maputil"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	ErrDiscoveryNil = errors.New("discovery not initialized")
)

const maxOwnerResolveAttempts = 3

var ErrLogicalActorRouterNotStarted = errors.New("logical actor router is not started")

func New(local NodeInfo, actorSystem actor.SystemAPI, discovery Discovery) ActorRouter {
	router := &Router{
		local:       local,
		discovery:   discovery,
		actorSystem: actorSystem,
		placements:  maputil.NewConcurrentMap[string, PlacementStrategy](10),
		factories:   maputil.NewConcurrentMap[string, ActorFactory](10),
	}
	router.SetName("logicalactor")
	router.logger = glog.With(zap.String("component", router.GetName()))
	return router
}

var _ ActorRouter = (*Router)(nil)

type Router struct {
	component.BaseComponent
	local       NodeInfo
	actorSystem actor.SystemAPI
	discovery   Discovery
	directory   OwnerDirectory
	placements  *maputil.ConcurrentMap[string, PlacementStrategy]
	factories   *maputil.ConcurrentMap[string, ActorFactory]
	routerPIDMu sync.Mutex
	routerPID   *actor.PID
	logger      *zap.Logger
}

func (router *Router) localNodeID() string {
	return router.local.NodeId
}

func (router *Router) isLocal(owner NodeInfo) bool {
	return owner.NodeId == router.local.NodeId
}

func (router *Router) SetDirectory(ownerDirectory OwnerDirectory) {
	router.directory = ownerDirectory
}

func (router *Router) Start() error {
	return router.GuardStart(func() error {
		switch {
		case router.actorSystem == nil:
			return ErrActorSystemNil
		case router.directory == nil:
			return ErrOwnerDirectoryNil
		case router.discovery == nil:
			return ErrDiscoveryNil
		}
		_, err := router.startRouter()
		return err
	})
}

func (router *Router) startRouter() (*actor.PID, error) {
	router.routerPIDMu.Lock()
	defer router.routerPIDMu.Unlock()
	if router.routerPID != nil {
		return router.routerPID, nil
	}

	routerPID, err := router.actorSystem.SpawnActor(&RouterActor{router: router}, actor.SpawnOptions{
		Name: RouterActorName,
	})
	if err != nil {
		return nil, fmt.Errorf("logical actor router spawn err:%w", err)
	}
	router.routerPID = routerPID
	return routerPID, nil
}

func (router *Router) RegisterFactory(kind string, factory ActorFactory) error {
	if err := (ActorID{Kind: kind, Key: "validate"}).Validate(); err != nil {
		return err
	}
	if factory == nil {
		return ErrActorFactoryNil
	}
	if _, exists := router.factories.GetOrSet(kind, factory); exists {
		return fmt.Errorf("kind %s：%w", kind, ErrFactoryAlreadyExist)
	}
	return nil
}

func (router *Router) RegisterPlacement(kind string, strategy PlacementStrategy) error {
	kind = strings.TrimSpace(kind)
	if err := (ActorID{Kind: kind, Key: "validate"}).Validate(); err != nil {
		return err
	}
	if strategy == nil {
		return ErrPlacementStrategyNil
	}
	if _, exists := router.placements.GetOrSet(kind, strategy); exists {
		return fmt.Errorf("%w: %s", ErrPlacementStrategyRegistered, kind)
	}
	return nil
}

//func (router *Router) CloseActor(actorId ActorID) error {
//	if router == nil || router.Status() != component.LifecycleStateStarted {
//		return ErrLogicalActorRouterNotStarted
//	}
//	if err := actorId.Validate(); err != nil {
//		return err
//	}
//	if err := router.actorSystem.Tell(actor.NoSender, actor.NewPID(0, RouterActorName, router.localNodeID()), closeActorRequest{actorID: actorId}); err != nil {
//		return fmt.Errorf("logical actor close actor_id:%s err:%w", actorId.String(), err)
//	}
//	return nil
//}

func (router *Router) Tell(ctx actor.Context, actorId ActorID, message proto.Message) error {
	target, request, err := router.prepareRoute(ctx, actorId, message)
	if err != nil {
		return fmt.Errorf("logical tell :%w", err)
	}
	if err = ctx.Tell(target, request); err != nil {
		return fmt.Errorf("logical tell :%w", err)
	}
	return nil
}

func (router *Router) Ask(ctx actor.Context, actorId ActorID, message proto.Message) (any, error) {
	target, request, err := router.prepareRoute(ctx, actorId, message)
	if err != nil {
		return nil, fmt.Errorf("logical ask :%w", err)
	}
	value, err := ctx.Ask(target, request)
	if err != nil {
		return nil, fmt.Errorf("logical ask :%w", err)
	}
	return value, err
}
func (router *Router) Forward(ctx actor.Context, actorId ActorID, message proto.Message) error {
	target, request, err := router.prepareRoute(ctx, actorId, message)
	if err != nil {
		return fmt.Errorf("logical forward :%w", err)
	}
	if err = ctx.Forward(target, request); err != nil {
		return fmt.Errorf("logical forward :%w", err)
	}
	return nil
}

func (router *Router) prepareRoute(ctx actor.Context, actorId ActorID, message proto.Message) (target *actor.PID, request *logicalactorpb.RouteRequest, err error) {
	if router == nil || router.Status() != component.LifecycleStateStarted {
		return nil, nil, ErrLogicalActorRouterNotStarted
	}
	if ctx == nil {
		return nil, nil, ErrActorContextNil
	}
	owner, request, err := router.newRouteRequest(actorId, message)
	if err != nil {
		return nil, nil, err
	}
	return actor.NewPID(0, RouterActorName, owner.NodeId), request, nil
}

func (router *Router) newRouteRequest(actorId ActorID, message proto.Message) (NodeInfo, *logicalactorpb.RouteRequest, error) {
	if err := actorId.Validate(); err != nil {
		return NodeInfo{}, nil, err
	}
	//  获取actorId目标服务地址
	owner, err := router.resolveOwner(actorId)
	if err != nil {
		return NodeInfo{}, nil, err
	}
	//  转换消息
	payload, err := protocodec.MessageToAny(message)
	if err != nil {
		return NodeInfo{}, nil, err
	}
	return owner, &logicalactorpb.RouteRequest{
		ActorId: actorIDToProto(actorId),
		Owner:   ownerToProto(owner),
		Message: payload,
	}, nil
}

func (router *Router) resolveOwner(actorId ActorID) (NodeInfo, error) {
	var (
		owner NodeInfo
		err   error
		found bool
	)
	for range maxOwnerResolveAttempts {
		//  读取缓存
		owner, found, err = router.directory.GetOwner(actorId)
		if err != nil {
			err = fmt.Errorf("directory get owner :%w", err)
			continue
		}
		//  没有读取到重新负载均衡一个节点
		if !found {
			owner, err = router.placementFor(actorId)
			if err != nil {
				err = fmt.Errorf("placement for :%w", err)
				continue
			}
		}
		//  检测节点是否存活
		if alive := router.isNodeAlive(owner); !alive {
			//  删除僵尸节点owner(节点宕机/actor未正常退出)
			if _, err = router.directory.DeleteOwner(actorId, owner); err != nil {
				err = fmt.Errorf("directory delete owner :%w", err)
				continue
			}
			err = fmt.Errorf("owner %s is dead", owner)
			continue
		}
		return owner, nil
	}
	return owner, err
}

// placementFor 重新负载均衡一个节点
func (router *Router) placementFor(actorId ActorID) (NodeInfo, error) {
	//  负载均衡
	candidate, err := router.pickNode(actorId)
	if err != nil {
		return NodeInfo{}, err
	}
	//  原子抢占
	owner, _, acquireErr := router.directory.AcquireOwner(actorId, candidate)
	if acquireErr != nil {
		return NodeInfo{}, fmt.Errorf("directory acquire owner :%w", acquireErr)
	}
	return owner, nil
}

// pickNode 负载均衡一个节点
func (router *Router) pickNode(actorId ActorID) (NodeInfo, error) {
	//  负载均衡
	kind := actorId.Kind
	strategy, ok := router.placements.Get(kind)
	if !ok {
		return NodeInfo{}, fmt.Errorf("kind:%s %w", kind, ErrPlacementStrategyNotFound)
	}
	candidate, err := strategy.PickNode(actorId)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("strategy pick node:%w", err)
	}
	return candidate, err
}

func (router *Router) isNodeAlive(candidate NodeInfo) bool {
	owner, _ := router.discovery.MemberById(candidate.NodeId)
	return owner == candidate
}

func (router *Router) Stop() error {
	return router.GuardStop(func() error {
		router.routerPIDMu.Lock()
		defer router.routerPIDMu.Unlock()
		if router.routerPID != nil && router.actorSystem != nil {
			router.actorSystem.StopProcess(actor.NoSender, router.routerPID)
			router.routerPID = nil
		}
		return nil
	})
}
