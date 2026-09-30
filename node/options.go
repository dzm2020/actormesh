package node

import (
	"errors"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/cluster"
	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/cluster/transport"
	"github.com/dzm2020/actormesh/logicalactor"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"

	"go.uber.org/zap/zapcore"
)

type LoggerOptions = glog.Config

type Options struct {
	member.NodeInfo
	Logger         LoggerOptions
	Behavior       NodeBehavior
	PanicHook      zapcore.CheckWriteHook
	System         actor.SystemAPI
	Cluster        cluster.ClusterAPI
	MemberManager  member.RegistryAPI
	Transport      transport.TransportAPI
	ActorRouter    logicalactor.ActorRouter
	ActorDirectory logicalactor.OwnerDirectory
	Components     []component.IComponent
}

func (options Options) normalize() Options {
	if options.Behavior == nil {
		options.Behavior = DefaultNodeBehavior{}
	}
	return options
}
func (options Options) validate() error {
	if options.ID == "" {
		return errors.New("node id is empty")
	}
	if options.Name == "" {
		return errors.New("node name is empty")
	}
	if options.Behavior == nil {
		return errors.New("node behavior is nil")
	}
	return nil
}
