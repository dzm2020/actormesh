package node

import (
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/cluster"
	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/logicalactor"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/netutil"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

func (n *Node) bootstrapNode() error {
	options := normalizeNodeOptions(n.options)
	if err := validateNodeOptions(options); err != nil {
		return fmt.Errorf("node bootstrap %w", err)
	}
	instance, err := n.buildServiceInstance()
	if err != nil {
		return fmt.Errorf("node bootstrap %w", err)
	}
	n.options = options
	if err = n.initializeLogger(); err != nil {
		return fmt.Errorf("node bootstrap %w", err)
	}

	n.cluster = options.Cluster
	if n.cluster == nil {
		n.cluster = cluster.New(instance, func(nodeID string, data []byte) error {
			return n.system.OnMessage(nodeID, data)
		})
	}

	n.system = options.System
	if n.system == nil {
		n.system = actor.NewSystem(n.GetID(), n.cluster)
	}

	n.logicalActorRouter = options.LogicalActorRouter
	if n.logicalActorRouter == nil && options.LogicalActorDirectory != nil {
		n.logicalActorRouter = logicalactor.New(newLogicalActorNode(instance), n.system, NewActorNodeAdapter(n.cluster))
	}

	if n.logicalActorRouter != nil {
		n.logicalActorRouter.SetDirectory(n.options.LogicalActorDirectory)
	}
	return nil
}

func (n *Node) buildServiceInstance() (member.ServiceInstance, error) {
	host, port, err := netutil.SplitHostPort(n.GetClusterAddress())
	if err != nil {
		return member.ServiceInstance{}, err
	}
	return member.ServiceInstance{
		ID:      n.GetID(),
		Name:    n.GetKind(),
		Address: host,
		Port:    port,
		Meta: map[string]string{
			NodeInstanceIDMetaKey: n.GetInstanceID(),
		}}, nil
}

func (n *Node) initializeLogger() error {
	options := []zap.Option{
		zap.Fields(
			zap.String("node_id", n.GetID()),
			zap.String("node_kind", n.GetKind()),
		),
	}
	if n.options.PanicHook != nil {
		options = append(options, zap.WithPanicHook(n.options.PanicHook))
	}
	return glog.Init(n.options.Logger, options...)
}

func (n *Node) registerCoreComponents() error {
	components := []component.IComponent{
		n.system, n.cluster,
	}
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
	glog.Info("node started",
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
		glog.Info("node shutdown signal received", zap.String("signal", receivedSignal.String()))
	}
}

func (n *Node) start() error {
	if err := n.bootstrapNode(); err != nil {
		return err
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
		glog.Info("component initialized", zap.String("component", component.GetName()))
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
		glog.Info("component started", zap.String("component", component.GetName()))
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
			glog.Info("component stopped", zap.String("component", component.GetName()))
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
	glog.Info("node stopped", zap.Error(shutdownErr))
	n.phase = phaseStopped

	_ = glog.Stop()
	return
}
