package node

import (
	"errors"
	"game-server/framework/actor"
	"game-server/framework/cluster"
	"game-server/framework/logicalactor"
	"strings"
	"time"

	"game-server/framework/pkg/glog"

	"go.uber.org/zap/zapcore"
)

const defaultShutdownTimeout = 30 * time.Second

type LoggerOptions = glog.Config

type Options struct {
	ID                    string
	Kind                  string
	ClusterAddress        string
	Logger                LoggerOptions
	Behavior              NodeBehavior
	PanicHook             zapcore.CheckWriteHook
	System                actor.SystemAPI
	Cluster               cluster.ClusterAPI
	LogicalActorRouter    logicalactor.ActorRouter
	LogicalActorDirectory logicalactor.OwnerDirectory
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
