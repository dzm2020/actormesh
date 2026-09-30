package actormesh

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/dzm2020/actormesh/node"
)

func Run(ctx context.Context, nodes ...node.NodeAPI) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	//  启动节点
	for _, api := range nodes {
		if err := api.Start(); err != nil {
			return err
		}
	}
	//  等待退出
	select {
	case <-signals:
	case <-ctx.Done():
	}
	//  终止节点
	var shutdownErr error
	for _, api := range nodes {
		if err := api.Shutdown(); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}
	return shutdownErr
}
