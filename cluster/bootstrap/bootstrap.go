package bootstrap

import (
	"github.com/dzm2020/actormesh/cluster"
	"github.com/dzm2020/actormesh/cluster/member"
	memberconsul "github.com/dzm2020/actormesh/cluster/member/consul"
	"github.com/dzm2020/actormesh/cluster/transport"
	transportnet "github.com/dzm2020/actormesh/cluster/transport/nettransport"
	"github.com/dzm2020/actormesh/pkg/glog"
)

type Options struct {
	NodeInfo  member.NodeInfo
	Handler   cluster.MessageHandler
	Logger    *glog.Logger
	Registry  member.RegistryAPI
	Transport transport.TransportAPI
}

func NewDefaultTransportAPI(nodeId string, address string, handle transport.MessageHandler, logger *glog.Logger) transport.TransportAPI {
	return transportnet.NewTransportWithOptions(transportnet.Options{
		LocalNodeID: nodeId,
		Logger:      logger,
		Handler:     handle,
		ListenAddr:  address,
	})
}

func NewDefaultRegistry(logger *glog.Logger) member.RegistryAPI {
	return memberconsul.NewWithOptions(memberconsul.Options{Logger: logger})
}

func NewDefaultCluster(options Options) *cluster.Cluster {
	registry := options.Registry
	if registry == nil {
		registry = NewDefaultRegistry(options.Logger)
	}
	transportImpl := options.Transport
	if transportImpl == nil {
		transportImpl = NewDefaultTransportAPI(options.NodeInfo.ID, options.NodeInfo.Address, transport.MessageHandler(options.Handler), options.Logger)
	}
	return cluster.NewWithOptions(cluster.Options{
		NodeInfo:  options.NodeInfo,
		Registry:  registry,
		Handler:   options.Handler,
		Transport: transportImpl,
		Logger:    options.Logger,
	})
}
