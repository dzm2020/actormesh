package network

import (
	"context"
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/pkg/glog"
	"net"

	"go.uber.org/zap"
)

func NewTCPServer(config *TCPConfig) *TCPServer {
	config.Normalize()
	return &TCPServer{
		config: config,
	}
}

type TCPServer struct {
	config *TCPConfig
}

func (s *TCPServer) Run(ctx context.Context, handler TransportHandler) error {
	return runEngine(ctx,
		func() (endpoint, error) {
			//  监听地址
			listener, err := net.Listen("tcp", s.config.Address)
			if err != nil {
				return nil, fmt.Errorf("tcp server  :%w", err)
			}
			glog.Info("tcp server listener created", zap.String("address", listener.Addr().String()))
			return listener, nil
		},
		func(e endpoint) {
			listener := e.(net.Listener)
			for {
				conn, err := listener.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						break
					}
					glog.Error("tcp server accept failed", zap.Error(err))
					continue
				}

				tcpCon, ok := conn.(*net.TCPConn)
				if !ok {
					_ = conn.Close()
					glog.Error("tcp server accepted unexpected connection type",
						zap.String("type", fmt.Sprintf("%T", conn)), zap.Error(ErrUnexpectedTCPConnType))
					continue
				}

				connection := newTCPConnection(handler, tcpCon, s.config, ConnectionRoleServer)
				_ = connection.runStart()
			}
		})
}
