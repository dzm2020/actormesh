package network

import (
	"context"
	"github.com/dzm2020/actormesh/pkg/grs"
	"net"
	"time"
)

func DialTCP(ctx context.Context, timeout time.Duration, handler TransportHandler, config *TCPConfig, userdata interface{}) (*TCPConnection, error) {
	ctx = grs.NormalizeContext(ctx)
	config.Normalize()
	dialer := net.Dialer{Timeout: timeout}
	con, err := dialer.DialContext(ctx, "tcp", config.Address)
	if err != nil {
		return nil, err
	}
	tcpCon, ok := con.(*net.TCPConn)
	if !ok {
		_ = con.Close()
		return nil, err
	}
	conn := newTCPConnection(handler, tcpCon, config, ConnectionRoleClient)
	conn.SetUserData(userdata)
	if err = conn.runStart(); err != nil {
		return nil, err
	}
	if err = waitReady(ctx, conn); err != nil {
		return nil, err
	}
	if conn.State() == ConnectionStateClosed {
		return nil, ErrConnectionClosed
	}
	return conn, nil
}

func waitReady(ctx context.Context, conn *TCPConnection) error {
	select {
	case <-conn.readyCh:
		return nil
	case <-ctx.Done():
		conn.Close(ctx.Err())
		return ctx.Err()
	case <-conn.ctx.Done():
		return conn.closeErr
	}
}
