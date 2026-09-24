package network

import (
	"context"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/glog"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
)

type udpTestHandler struct {
	connected chan Connection
}

func newUDPTestHandler() *udpTestHandler {
	return &udpTestHandler{
		connected: make(chan Connection, 16),
	}
}

func (h *udpTestHandler) OnConnected(connection Connection) error {
	return nil
}

func (h *udpTestHandler) OnMessage(connection Connection, msg *protocol.MessageFrame) error {
	glog.Info("OnMessage", zap.Any("msg", msg))
	if connection.Role() == ConnectionRoleServer {
		response := protocol.NewMessageFrame(1, 2, 0, []byte("response"))
		connection.SendMessage(response)
		connection.Close(nil)
	}
	return nil
}

func (h *udpTestHandler) OnClose(Connection, error) {}

func startUDPTestServer(t *testing.T, handler TransportHandler) {
	t.Helper()

	server, err := NewUDPServer(func() *UDPConfig {
		config := DefaultUDPConfig()
		config.Address = "127.0.0.1:8888"
		config.EncryptEnable = false
		//config.HeartbeatTimeout = time.Second * 3
		return config
	}())
	if err != nil {
		t.Fatal(err)
	}

	if err = server.Run(context.Background(), handler); err != nil {
		t.Fatal(err)
	}

}

func encodeUDPData(t *testing.T, sessionID, sequence uint64, message *protocol.MessageFrame) []byte {
	t.Helper()

	data, err := protocol.EncodeMessageFrame(message)
	if err != nil {
		t.Fatal(err)
	}
	header, err := protocol.EncodeUDPHeader(protocol.UDPHeader{SessionID: sessionID, Sequence: sequence})
	if err != nil {
		t.Fatal(err)
	}
	return append(header, data...)
}

func mustResolveUDPAddr(t *testing.T, address string) *net.UDPAddr {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestUDPServer(t *testing.T) {
	handler := newUDPTestHandler()
	startUDPTestServer(t, handler)
}

func TestUDPClient(t *testing.T) {
	conn, err := net.DialUDP("udp", nil, mustResolveUDPAddr(t, "127.0.0.1:8888"))
	if err != nil {
		t.Fatal(err)
	}
	var sessionID uint64 = 1
	for i := 0; i < 1; i++ {
		sessionID += uint64(1)
		wantData := []byte("udp payload")
		packet := encodeUDPData(t, sessionID, 1, protocol.NewMessageFrame(1, 1, 0, wantData))
		if _, err := conn.Write(packet); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
	}
	buf := make([]byte, 1024)
	conn.ReadFromUDP(buf)
	_ = buf
}
