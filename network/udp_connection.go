package network

import (
	"game-server/framework/network/protocol"
	"net"
	"sync/atomic"

	"go.uber.org/zap"
)

var _ Connection = (*UDPConnection)(nil)

func newUDPConnection(udpServer *UDPServer, baseConn *baseConn, remoteAddr *net.UDPAddr, sessionID uint64) *UDPConnection {
	udpConn := &UDPConnection{
		udpServer:  udpServer,
		baseConn:   baseConn,
		remoteAddr: remoteAddr,
		sessionID:  sessionID,
		receiveCh:  make(chan []byte, 2048),
	}
	udpConn.baseConn.bind(udpConn)
	return udpConn
}

type UDPConnection struct {
	*baseConn
	udpServer     *UDPServer
	remoteAddr    *net.UDPAddr
	conn          *net.UDPConn
	connectionKey string
	sessionID     uint64
	sendSequence  atomic.Uint64
	receiveCh     chan []byte
}

func (c *UDPConnection) LocalAddr() string {
	if c.conn == nil {
		return ""
	}
	return c.conn.LocalAddr().String()
}

func (c *UDPConnection) RemoteAddr() string {
	if c.remoteAddr == nil {
		return ""
	}
	return c.remoteAddr.String()
}

func (c *UDPConnection) Network() string {
	return "udp"
}

func (c *UDPConnection) recv(data []byte) {
	select {
	case c.receiveCh <- append([]byte(nil), data...):
	default:
		c.logger.Warn("udp conn  receive queue is full",
			zap.Int("queue_length", len(c.receiveCh)),
			zap.Int("queue_capacity", cap(c.receiveCh)),
		)
	}
}

func (c *UDPConnection) readLoop() {
	var err error
	defer func() {
		c.Close(err)
	}()
	for {
		select {
		case <-c.ctx.Done():
			err = c.ctx.Err()
			return
		case data, _ := <-c.receiveCh:
			if err = c.onProcess(data); err != nil {
				return
			}
		}
	}
}

func (c *UDPConnection) onProcess(data []byte) error {
	if len(data) < protocol.UDPHeaderLen {
		return protocol.ErrInvalidUDPHeader
	}
	// 帧大小上限由 baseConn.onProcess 按整帧长度（Body + 17）校验，与 TCP/WebSocket 一致。
	if _, err := c.baseConn.onProcess(c, data[protocol.UDPHeaderLen:]); err != nil {
		return err
	}
	return nil
}

func (c *UDPConnection) writeRaw(data []byte) error {
	header, err := protocol.EncodeUDPHeader(protocol.UDPHeader{
		SessionID: c.sessionID,
		Sequence:  c.sendSequence.Add(1),
	})
	if err != nil {
		c.Log().Error("send packet failed", zap.Error(err))
		return err
	}
	packet := &udpPacket{data: append(header, data...), remoteAddr: c.remoteAddr}
	if err = c.udpServer.trySend(packet); err != nil {
		c.Log().Error("send packet failed", zap.Error(err))
		return ErrNetworkChannelFull
	}
	return nil
}

func (c *UDPConnection) closeSocket() {
}

func (c *UDPConnection) Close(err error) {
	c.baseConn.Close(err)
	c.udpServer.removeConnection(c)
	return
}
