package node

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/cluster"
	"github.com/dzm2020/actormesh/cluster/member"
	memberconsul "github.com/dzm2020/actormesh/cluster/member/consul"
	transportnet "github.com/dzm2020/actormesh/cluster/transport/nettransport"
	"github.com/dzm2020/actormesh/logicalactor"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/netutil"
	"go.uber.org/zap"
)

func (n *Node) bootstrapNode() error {
	options := normalizeNodeOptions(n.options)
	if err := validateNodeOptions(options); err != nil {
		return fmt.Errorf("node bootstrap %w", err)
	}

	n.options = options
	logger, err := n.initializeLogger()
	if err != nil {
		return err
	}
	n.logger = logger
	instance, err := n.buildServiceInstance()
	if err != nil {
		return err
	}

	n.cluster = n.initializeCluster(instance)
	n.system = n.initializeSystem(n.cluster)
	n.logicalActorRouter = n.initializeActorRoute(instance, n.cluster, n.system)
	return nil
}

func (n *Node) initializeLogger() (*glog.Logger, error) {
	options := []zap.Option{
		zap.Fields(
			zap.String("node_id", n.GetID()),
			zap.String("node_kind", n.GetKind()),
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
		NodeID:       n.GetID(),
		RemoteSender: cluster,
		Logger:       n.logger,
	})
}

func (n *Node) initializeCluster(instance member.ServiceInstance) cluster.ClusterAPI {
	if n.options.Cluster != nil {
		return n.options.Cluster
	}
	memberManager := n.options.MemberManager
	if memberManager == nil {
		memberManager = memberconsul.NewWithOptions(memberconsul.Options{Logger: n.logger})
	}

	transport := n.options.Transport
	if transport == nil {
		transport = transportnet.NewTransportWithOptions(transportnet.Options{
			LocalNodeID: instance.ID,
			Logger:      n.logger,
		})
	}
	handler := func(nodeID string, data []byte) error {
		return n.system.OnMessage(nodeID, data)
	}
	return cluster.NewWithOptions(instance, handler, cluster.Options{
		Logger:        n.logger,
		MemberManager: memberManager,
		Transport:     transport,
	})
}

func (n *Node) initializeActorRoute(instance member.ServiceInstance, cluster cluster.ClusterAPI, system actor.SystemAPI) logicalactor.ActorRouter {
	route := n.options.LogicalActorRouter
	if route == nil {
		route = logicalactor.New(logicalactor.Options{
			Local:     newLogicalActorNode(instance),
			System:    system,
			Discovery: NewActorNodeAdapter(cluster),
			Logger:    n.logger,
		})
	}
	route.SetDirectory(n.options.LogicalActorDirectory)
	return route
}

func (n *Node) buildServiceInstance() (member.ServiceInstance, error) {
	host, port, err := netutil.SplitHostPort(n.GetClusterAddress())
	if err != nil {
		return member.ServiceInstance{}, err
	}
	meta := make(map[string]string, len(n.options.Meta)+1)
	for key, value := range n.options.Meta {
		meta[key] = value
	}
	meta[NodeInstanceIDMetaKey] = n.GetInstanceID()
	return member.ServiceInstance{
		ID:      n.GetID(),
		Name:    n.GetKind(),
		Address: host,
		Port:    port,
		Meta:    meta,
	}, nil
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

func (n *Node) Startup() (err error) {
	defer func() {
		n.shutdown()
	}()
	if err = n.start(); err != nil {
		return err
	}
	n.logger.Info("node started",
		zap.String("instanceId", n.GetInstanceID()),
		zap.String("cluster_address", n.GetClusterAddress()),
		zap.Int("process_id", os.Getpid()),
	)
	n.wait()
	return
}

func (n *Node) wait() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)

	select {
	case receivedSignal := <-signals:
		n.logger.Info("node shutdown signal received", zap.String("signal", receivedSignal.String()))
	}
}

func (n *Node) start() error {
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

func (n *Node) shutdown() {
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

	return
}
