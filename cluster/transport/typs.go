package transport

import (
	"time"
)

type MessageHandler func(nodeId string, data []byte) error

type PeerState uint8

const (
	PeerStateIdle PeerState = iota
	PeerStateHandshaking
	PeerStateConnected
)

type Transport interface {
	ListenAndServe(address string, handler MessageHandler) error
	Connect(nodeId string, address string, handler MessageHandler, timeout time.Duration) error
	ConnectionState(nodeId string) PeerState
	Disconnect(nodeId string) error
	Send(nodeId string, data []byte) error
	Broadcast(data []byte) error
	Close()
}
