package actormesh

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/dzm2020/actormesh/node"
)

func Run(ctx context.Context, options node.Options) (node.NodeAPI, error) {
	n, err := node.New(options)
	if err != nil {
		return nil, err
	}
	if err = n.Start(); err != nil {
		return n, err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	//  等待退出
	select {
	case <-signals:
	}
	return n, nil
}
