package network

import (
	"context"
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/grs"
	"github.com/dzm2020/actormesh/pkg/netutil"
	"net"
	"sync"

	"go.uber.org/zap"
)

var _ endpoint = (*udpEndpoint)(nil)

type udpEndpoint struct {
	net.PacketConn
}

func (u *udpEndpoint) Addr() net.Addr {
	return u.LocalAddr()
}

type udpPacket struct {
	data       []byte
	remoteAddr *net.UDPAddr
}

func NewUDPServer(config *UDPConfig) (*UDPServer, error) {
	config.Normalize()
	return &UDPServer{
		config:         config,
		remoteAddrDict: make(map[string]*UDPConnection),
		connections:    make(map[string]*UDPConnection),
		sendChannel:    make(chan *udpPacket, config.ServerSendChanSize),
	}, nil
}

type UDPServer struct {
	mu             sync.RWMutex
	connections    map[string]*UDPConnection
	remoteAddrDict map[string]*UDPConnection
	config         *UDPConfig
	sendChannel    chan *udpPacket
}

func (s *UDPServer) Run(ctx context.Context, handler TransportHandler) error {
	return runEngine(ctx, func() (endpoint, error) {
		//  监听地址
		listener, err := net.ListenPacket("udp", s.config.Address)
		if err != nil {
			return nil, fmt.Errorf("udp server :%w", err)
		}
		glog.Info("udp server listener created", zap.String("address", listener.LocalAddr().String()))
		return &udpEndpoint{PacketConn: listener}, nil
	},
		func(e endpoint) {
			packConn := e.(*udpEndpoint).PacketConn
			conn := packConn.(*net.UDPConn)
			//  启动发送协程
			grs.SafeGo(func() {
				s.writeLoop(ctx, conn)
			})
			//  阻塞读取数据
			buf := make([]byte, 65535)
			for {
				n, remoteAddr, err := conn.ReadFromUDP(buf)
				if err != nil {
					if !errors.Is(err, net.ErrClosed) {
						glog.Error("udp server read udp failed", zap.Error(err))
					}
					break
				}
				if n == 0 {
					continue
				}
				//  copy
				bytes := append([]byte(nil), buf[:n]...)
				s.transmit(handler, remoteAddr, bytes)
			}
		})
}

func (s *UDPServer) transmit(handler TransportHandler, remoteAddr *net.UDPAddr, bytes []byte) {
	var udpConn *UDPConnection
	header, err := protocol.DecodeUDPHeader(bytes)
	if err != nil {
		glog.Error("udp server decode  header failed", zap.Error(err))
		return
	}
	remoteAddr = netutil.CloneUDPAddr(remoteAddr)
	remoteKey := remoteAddr.String()
	connectionKey := udpConnectionKey(remoteAddr, header.SessionID)
	s.mu.RLock()
	conn, ok := s.connections[connectionKey]
	current, _ := s.remoteAddrDict[remoteKey]
	s.mu.RUnlock()

	if ok {
		udpConn = conn
	} else {
		//  新建连接前处理下，避免同源地址存在多个连接
		if current != nil {
			current.Close(errors.New("connection reset"))
		}
		s.mu.Lock()
		base := newBaseConnWithRole(handler, s.config.CommonConfig, ConnectionRoleServer)
		udpConn = newUDPConnection(s, base, remoteAddr, header.SessionID)
		s.connections[connectionKey] = udpConn
		s.remoteAddrDict[remoteKey] = udpConn
		s.mu.Unlock()
		_ = udpConn.runStart()
	}
	udpConn.recv(bytes)
}

func (s *UDPServer) writeLoop(ctx context.Context, conn *net.UDPConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case packet, _ := <-s.sendChannel:
			_, err := conn.WriteToUDP(packet.data, packet.remoteAddr)
			if err != nil && !errors.Is(err, net.ErrClosed) {
				glog.Error("udp server write failed", zap.Error(err))
			}
		}
	}
}

func (s *UDPServer) trySend(packet *udpPacket) error {
	select {
	case s.sendChannel <- packet:
	default:
		return errors.New("send channel is full")
	}
	return nil
}

func (s *UDPServer) removeConnection(c *UDPConnection) {
	remoteKey := c.remoteAddr.String()
	connectionKey := udpConnectionKey(c.remoteAddr, c.sessionID)
	s.mu.Lock()
	delete(s.connections, connectionKey)

	if conn := s.remoteAddrDict[remoteKey]; conn == c {
		delete(s.remoteAddrDict, remoteKey)
	}
	s.mu.Unlock()
}

func udpConnectionKey(remoteAddr *net.UDPAddr, sessionID uint64) string {
	return fmt.Sprintf("%s/%d", remoteAddr.String(), sessionID)
}
