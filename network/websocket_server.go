package network

import (
	"context"
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/pkg/glog"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

func NewWebSocketServer(config *WebSocketConfig) *WebSocketServer {
	config.Normalize()
	return &WebSocketServer{
		config: config,
	}
}

type WebSocketServer struct {
	config *WebSocketConfig
}

func (s *WebSocketServer) Run(ctx context.Context, handler TransportHandler) error {
	return runEngine(ctx,
		func() (endpoint, error) {
			//  监听地址
			listener, err := net.Listen("tcp", s.config.Address)
			if err != nil {
				return nil, fmt.Errorf("websocket server :%w", err)
			}
			glog.Info("websocket server listener created", zap.String("address", listener.Addr().String()))
			return listener, nil
		},
		func(e endpoint) {
			listener := e.(net.Listener)
			glog.Info("websocket server running", zap.String("address", listener.Addr().String()))
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
				conn, err := s.config.Upgrader.Upgrade(writer, request, nil)
				if err != nil {
					glog.Error("websocket upgrade failed", zap.Error(err))
					return
				}
				wsConn := newWebSocketConnection(handler, s.config, conn)
				if err = wsConn.runStart(); err != nil {
					glog.Error("websocket connection start failed", zap.Error(err))
				}
			})
			httpServer := &http.Server{
				Addr:              s.config.Address,
				Handler:           mux,
				ReadHeaderTimeout: time.Second * 5,
			}
			serveErr := httpServer.Serve(listener)
			if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && ctx.Err() == nil {
				glog.Error("websocket  server stopped unexpectedly", zap.Error(serveErr))
			}
		})

}
