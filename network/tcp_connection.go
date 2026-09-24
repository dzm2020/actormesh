package network

import (
	"errors"
	"github.com/dzm2020/actormesh/pkg/buffer"
	"github.com/dzm2020/actormesh/pkg/netutil"
	"io"
	"net"
	"time"

	"go.uber.org/zap"
)

var _ Connection = (*TCPConnection)(nil)

func newTCPConnection(handler TransportHandler, conn *net.TCPConn, config *TCPConfig, role ConnectionRole) *TCPConnection {
	c := &TCPConnection{
		baseConn:   newBaseConnWithRole(handler, config.CommonConfig, role),
		conn:       conn,
		readChunk:  make([]byte, tcpReadChunkSize),
		readBuffer: buffer.New(config.ReadBufferCap, config.ReadBufferCap),
	}
	c.baseConn.bind(c)
	return c
}

type TCPConnection struct {
	*baseConn
	conn       *net.TCPConn
	readChunk  []byte
	readBuffer buffer.IBuffer
}

func (c *TCPConnection) LocalAddr() string {
	return c.conn.LocalAddr().String()
}

func (c *TCPConnection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *TCPConnection) Network() string {
	return "tcp"
}

func (c *TCPConnection) SetLinger(seconds int) error {
	return c.conn.SetLinger(seconds)
}

func (c *TCPConnection) SetNoDelay(noDelay bool) error {
	return c.conn.SetNoDelay(noDelay)
}

func (c *TCPConnection) SetSocketReadBuffer(bytes int) error {
	return c.conn.SetReadBuffer(bytes)
}
func (c *TCPConnection) SetSocketWriteBuffer(bytes int) error {
	return c.conn.SetWriteBuffer(bytes)
}

func (c *TCPConnection) readLoop() {
	var err error
	var n int
	defer func() {
		c.Close(err)
	}()
	for {
		n, err = c.conn.Read(c.readChunk)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || netutil.IsConnectionReset(err) {
				return
			}
			c.logger.Error("tcp conn readLoop read failed", zap.Error(err))
			return
		}
		if n == 0 {
			err = io.EOF
			return
		}
		// 写入缓冲区
		if _, err = c.readBuffer.Write(c.readChunk[:n]); err != nil {
			c.logger.Error("tcp conn readLoop buffer write failed", zap.Error(err))
			return
		}

		if err = c.processReadBuffer(c.readBuffer); err != nil {
			c.logger.Error("tcp conn readLoop processing failed", zap.Error(err))
			return
		}
	}
}

func (c *TCPConnection) writeRaw(data []byte) error {
	t := time.Now().Add(c.options.WriteTimeout)
	if err := c.conn.SetWriteDeadline(t); err != nil {
		return err
	}
	for len(data) > 0 {
		n, err := c.conn.Write(data)
		if err != nil {
			c.logger.Error("tcp write socket  failed", zap.Error(err))
			return err
		}
		data = data[n:]
	}
	return nil
}

func (c *TCPConnection) closeSocket() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}
