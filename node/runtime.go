package node

import (
	"errors"
	"fmt"
	"os"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/cluster"
	"github.com/dzm2020/actormesh/cluster/bootstrap"
	"github.com/dzm2020/actormesh/logicalactor"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
	"go.uber.org/zap"
)

func (n *Node) bootstrapNode() error {
	n.options = n.options.normalize()
	if err := n.options.validate(); err != nil {
		return fmt.Errorf("node bootstrap %w", err)
	}
	logger, err := n.initializeLogger()
	if err != nil {
		return err
	}
	n.logger = logger

	n.cluster = n.initializeCluster()
	n.system = n.initializeSystem(n.cluster)
	n.logicalActorRouter = n.initializeActorRoute(n.cluster, n.system)
	return nil
}

func (n *Node) initializeLogger() (*glog.Logger, error) {
	options := []zap.Option{
		zap.Fields(
			zap.String("node_id", n.Info().ID),
			zap.String("node_kind", n.Info().Name),
		),
	}
	if n.options.PanicHook != nil {
		options = append(options, zap.WithPanicHook(n.options.PanicHook))
	}
	logger, err := glog.New(n.options.Logger, options...)
	if err != nil {
		return nil, fmt.Errorf("node logger: %w", err)
	}
	return logger, nil
}

func (n *Node) initializeSystem(cluster cluster.ClusterAPI) actor.SystemAPI {
	if n.options.System != nil {
		return n.options.System
	}
	return actor.NewSystemWithOptions(actor.SystemOptions{
		NodeID:       n.Info().ID,
		RemoteSender: cluster,
		Logger:       n.logger,
	})
}

func (n *Node) initializeCluster() cluster.ClusterAPI {
	if n.options.Cluster != nil {
		return n.options.Cluster
	}
	handler := func(nodeID string, data []byte) error {
		return n.system.OnMessage(nodeID, data)
	}
	return bootstrap.NewDefaultCluster(bootstrap.Options{
		NodeInfo: n.Info(),
		Logger:   n.logger,
		Handler:  handler,
	})
}

func (n *Node) initializeActorRoute(cluster cluster.ClusterAPI, system actor.SystemAPI) logicalactor.ActorRouterAPI {
	route := n.options.ActorRouter
	if route == nil {
		route = logicalactor.New(logicalactor.Options{
			Local:     newLogicalActorNode(n.Info()),
			System:    system,
			Discovery: NewActorNodeAdapter(cluster),
			Logger:    n.logger,
		})
	}
	return route
}

func (n *Node) registerCoreComponents() error {
	components := make([]component.IComponent, 0, len(n.options.Components)+3)
	components = append(components, n.options.Components...)
	components = append(components, n.system, n.cluster)
	if n.logicalActorRouter != nil {
		components = append(components, n.logicalActorRouter)
	}
	if err := n.manager.Add(components...); err != nil {
		return err
	}
	return nil
}

func (n *Node) Start() error {
	if err := n.bootstrapNode(); err != nil {
		return fmt.Errorf("node boot %w", err)
	}
	if err := n.registerCoreComponents(); err != nil {
		return fmt.Errorf("register components :%w", err)
	}
	if err := n.initializeComponents(); err != nil {
		return err
	}
	if err := n.startComponents(); err != nil {
		return err
	}
	n.phase = phaseRunning
	n.logger.Info("node started", zap.Int("process_id", os.Getpid()))
	return nil
}

func (n *Node) initializeComponents() error {
	var err error
	if err = n.options.Behavior.OnInit(n); err != nil {
		return fmt.Errorf("OnInit :%w", err)
	}
	n.manager.RangeInOrder(func(component component.IComponent) bool {
		if err = component.Init(); err != nil {
			err = fmt.Errorf("init component:%s :%w", component.GetName(), err)
			return false
		}
		n.logger.Info("component initialized", zap.String("component", component.GetName()))
		return true
	})
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) startComponents() error {
	var err error
	if err = n.options.Behavior.OnStart(n); err != nil {
		return fmt.Errorf("OnStart :%w", err)
	}
	n.manager.RangeInOrder(func(component component.IComponent) bool {
		if err = component.Start(); err != nil {
			err = fmt.Errorf("start component:%s :%w", component.GetName(), err)
			return false
		}
		n.logger.Info("component started", zap.String("component", component.GetName()))
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) stopComponents() error {
	var err error
	n.manager.RangeInReverseOrder(func(component component.IComponent) bool {
		if err = component.Stop(); err != nil {
			err = errors.Join(fmt.Errorf("stop component %q  %w", component.GetName(), err))
		} else {
			n.logger.Info("component stopped", zap.String("component", component.GetName()))
		}
		return true
	})
	return err
}

func (n *Node) Shutdown() error {
	var shutdownErr error
	if n.cluster != nil {
		shutdownErr = errors.Join(shutdownErr, n.cluster.Leave())
	}
	if err := n.stopComponents(); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
	}

	n.options.Behavior.OnStop(n, shutdownErr)
	n.logger.Info("node stopped", zap.Error(shutdownErr))
	n.phase = phaseStopped

	return shutdownErr
}
