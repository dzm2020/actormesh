package network

import (
	"context"
	"net"

	"github.com/dzm2020/actormesh/pkg/glog"
	"go.uber.org/zap"
)

type endpoint interface {
	Close() error
	Addr() net.Addr
}

type openFun func() (endpoint, error)
type runFun func(e endpoint)

func runEngine(ctx context.Context, open openFun, run runFun, logger *glog.Logger) error {
	ctx = normalizeContext(ctx)
	listener, err := open()
	if err != nil {
		return err
	}
	context.AfterFunc(ctx, func() {
		_ = listener.Close()
	})
	logger.Info("server running", zap.String("address", listener.Addr().String()))

	run(listener)

	logger.Info("server stopped", zap.String("address", listener.Addr().String()))
	return nil
}
