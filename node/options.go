package node

import (
	"errors"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/cluster"
	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/logicalactor"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"

	"go.uber.org/zap/zapcore"
)

type LoggerOptions = glog.Config

type Options struct {
	member.NodeInfo
	Logger         LoggerOptions               // 日志
	Behavior       NodeBehavior                // 节点回调
	PanicHook      zapcore.CheckWriteHook      // 节点panic回调
	System         actor.SystemAPI             // 本地actor系统
	Cluster        cluster.ClusterAPI          // 集群
	ActorRouter    logicalactor.ActorRouterAPI // 逻辑actor寻址
	ActorDirectory logicalactor.OwnerDirectory
	Components     []component.IComponent // 扩展组件
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
