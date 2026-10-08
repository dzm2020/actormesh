package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/glog"
)

func TestGatewayBridgeForwardsConnectionLifecycle(t *testing.T) {
	pid := actor.NewPID(7, "agent", "node")
	system := &bridgeTestSystem{}
	bridge := NewGatewayBridge(system, func(network.Connection) (*actor.PID, error) {
		return pid, nil
	})
	connection := &bridgeTestConnection{id: 42}

	if err := bridge.OnConnected(connection); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if connection.UserData() != pid {
		t.Fatalf("connection user data = %#v, want agent PID", connection.UserData())
	}

	frame := protocol.NewMessageFrame(1, 2, 0, []byte("request"))
	if err := bridge.OnMessage(connection, frame); err != nil {
		t.Fatalf("forward message: %v", err)
	}
	if system.target != pid || system.envelope.Sender != pid || system.envelope.Payload != frame {
		t.Fatalf("forwarded envelope = %#v, target = %#v", system.envelope, system.target)
	}

	bridge.OnClose(connection, nil)
	if system.stopped != pid {
		t.Fatalf("stopped PID = %#v, want %#v", system.stopped, pid)
	}
	if err := bridge.Close(time.Second); err != nil {
		t.Fatalf("close bridge: %v", err)
	}
}

func TestGatewayBridgeRejectsConnectionsAfterClose(t *testing.T) {
	bridge := NewGatewayBridge(&bridgeTestSystem{}, func(network.Connection) (*actor.PID, error) {
		return actor.NewPID(1, "agent", "node"), nil
	})
	if err := bridge.Close(time.Second); err != nil {
		t.Fatalf("close bridge: %v", err)
	}
	if err := bridge.OnConnected(&bridgeTestConnection{}); !errors.Is(err, ErrGatewayClosing) {
		t.Fatalf("connect after close error = %v, want %v", err, ErrGatewayClosing)
	}
}

type bridgeTestSystem struct {
	target   *actor.PID
	envelope actor.Envelope
	stopped  *actor.PID
}

func (s *bridgeTestSystem) SendEnvelope(target *actor.PID, envelope actor.Envelope) error {
	s.target = target
	s.envelope = envelope
	return nil
}

func (s *bridgeTestSystem) StopProcess(_ *actor.PID, target *actor.PID) {
	s.stopped = target
}

type bridgeTestConnection struct {
	id       int64
	userData any
}

func (c *bridgeTestConnection) ID() int64                              { return c.id }
func (*bridgeTestConnection) LocalAddr() string                        { return "local" }
func (*bridgeTestConnection) RemoteAddr() string                       { return "remote" }
func (*bridgeTestConnection) State() network.ConnectionState           { return network.ConnectionStateReady }
func (*bridgeTestConnection) Context() context.Context                 { return context.Background() }
func (c *bridgeTestConnection) UserData() any                          { return c.userData }
func (c *bridgeTestConnection) SetUserData(data any)                   { c.userData = data }
func (*bridgeTestConnection) Role() network.ConnectionRole             { return network.ConnectionRoleServer }
func (*bridgeTestConnection) Network() string                          { return "test" }
func (*bridgeTestConnection) Log() *glog.Logger                        { return glog.Log() }
func (*bridgeTestConnection) SendMessage(*protocol.MessageFrame) error { return nil }
func (*bridgeTestConnection) Close(error)                              {}
