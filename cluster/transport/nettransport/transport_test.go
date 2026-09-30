package nettransport

import (
	"errors"
	"github.com/dzm2020/actormesh/network"
	"net"
	"testing"
	"time"
)

func freeTransportAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func testTransportConfig() *network.TCPConfig {
	c := network.DefaultTCPConfig("")
	c.EncryptEnable = false
	c.HeartbeatTimeout = 30 * time.Second
	c.HandshakeTimeout = 2 * time.Second
	c.Normalize()
	return c
}

func waitTransportState(t *testing.T, tr *Transport, nodeID string, state PeerState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tr.ConnectionState(nodeID) == state {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s state %v; got %v", nodeID, state, tr.ConnectionState(nodeID))
}

func TestTransportConnectSendAndClose(t *testing.T) {
	serverAddress := freeTransportAddress(t)
	received := make(chan string, 1)
	server := NewTransportWithOptions(Options{
		LocalNodeID: "server",
		TcpConfig:   testTransportConfig(),
		ListenAddr:  serverAddress,
		Handler: func(nodeID string, data []byte) error {
			received <- nodeID + ":" + string(data)
			return nil
		},
	})
	client := NewTransportWithOptions(Options{
		LocalNodeID: "client",
		TcpConfig:   testTransportConfig(),
		Handler:     func(string, []byte) error { return nil },
	})
	defer server.Close()
	defer client.Close()

	if err := server.Run(); err != nil {
		t.Fatalf("run server: %v", err)
	}

	var connectErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		connectErr = client.Connect("server", serverAddress, 500*time.Millisecond)
		if connectErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if connectErr != nil {
		t.Fatalf("connect: %v", connectErr)
	}
	waitTransportState(t, client, "server", PeerStateConnected)
	waitTransportState(t, server, "client", PeerStateConnected)
	if got := client.ConnectionState("missing"); got != PeerStateIdle {
		t.Fatalf("unknown peer state = %v, want idle", got)
	}

	if err := client.Send("server", []byte("hello")); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case got := <-received:
		if got != "client:hello" {
			t.Fatalf("received %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	client.Close()
	waitTransportState(t, server, "client", PeerStateIdle)
}

func TestTransportConnectFailureCleansPeer(t *testing.T) {
	tr := NewTransportWithOptions(Options{
		LocalNodeID: "client",
		TcpConfig:   testTransportConfig(),
		Handler:     func(string, []byte) error { return nil },
	})
	defer tr.Close()
	address := freeTransportAddress(t)
	if err := tr.Connect("server", address, 100*time.Millisecond); err == nil {
		t.Fatal("expected connect failure")
	}
	if got := tr.ConnectionState("server"); got != PeerStateIdle {
		t.Fatalf("failed connect left state %v", got)
	}
	if err := tr.Send("server", []byte("x")); !errors.Is(err, ErrPeerNotConnected) {
		t.Fatalf("send after failed connect = %v", err)
	}
}

func TestTransportCloseCancelsConnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	config := testTransportConfig()
	config.EncryptEnable = true
	tr := NewTransportWithOptions(Options{
		LocalNodeID: "client",
		TcpConfig:   config,
		Handler:     func(string, []byte) error { return nil },
	})
	result := make(chan error, 1)
	go func() {
		result <- tr.Connect("server", listener.Addr().String(), 30*time.Second)
	}()
	var raw net.Conn
	select {
	case raw = <-accepted:
	case <-time.After(5 * time.Second):
		tr.Close()
		t.Fatal("server did not accept connection")
	}
	defer raw.Close()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	tr.Close()
	select {
	case <-result:
		if time.Since(start) > 5*time.Second {
			t.Fatal("handshake wait was not canceled promptly")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connect remained blocked while waiting for handshake")
	}
}
