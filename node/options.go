package node

import (
	"errors"
	"strings"

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
	ID                    string
	Kind                  string
	ClusterAddress        string
	Meta                  map[string]string
	Logger                LoggerOptions
	Behavior              NodeBehavior
	PanicHook             zapcore.CheckWriteHook
	System                actor.SystemAPI
	Cluster               cluster.ClusterAPI
	MemberManager         member.MemberManagerAPI
	Transport             transport.TransportAPI
	LogicalActorRouter    logicalactor.ActorRouter
	LogicalActorDirectory logicalactor.OwnerDirectory
	Components            []component.IComponent
}

func normalizeNodeOptions(options Options) Options {
	options.ID = strings.TrimSpace(options.ID)
	options.Kind = strings.TrimSpace(options.Kind)
	if options.Behavior == nil {
		options.Behavior = DefaultNodeBehavior{}
	}
	return options
}
func validateNodeOptions(options Options) error {
	if options.ID == "" {
		return errors.New("node id is empty")
	}
	if options.Kind == "" {
		return errors.New("node kind is empty")
	}
	if options.Behavior == nil {
		return errors.New("node behavior is nil")
	}
	return nil
}
