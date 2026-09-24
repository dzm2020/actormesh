package nettransport

import (
	"errors"
	"github.com/dzm2020/actormesh/cluster/transport"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/network/protocol"
	"sync"
	"sync/atomic"
)

type PeerState = transport.PeerState

const (
	PeerStateIdle        = transport.PeerStateIdle
	PeerStateHandshaking = transport.PeerStateHandshaking
	PeerStateConnected   = transport.PeerStateConnected
)

type peer struct {
	mu       sync.RWMutex
	remoteId string
	expected string
	state    atomic.Uint64
	conn     network.Connection
}

func newPeer() *peer {
	h := &peer{}
	h.state.Store(uint64(PeerStateIdle))
	return h
}
func newPeerWithId(expected string) *peer {
	h := newPeer()
	h.expected = expected
	return h
}

func (p *peer) getState() PeerState {
	return PeerState(p.state.Load())
}

func (p *peer) swapState(expected, state PeerState) bool {
	return p.state.CompareAndSwap(uint64(expected), uint64(state))
}

func (p *peer) getRemoteId() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.remoteId == "" { // 握手还没完成的客户端
		return p.expected
	}
	return p.remoteId
}

func (p *peer) bindRemoteId(remoteId string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.expected != "" && remoteId != p.expected {
		return errors.New("remote peer id mismatch")
	}
	p.remoteId = remoteId
	return nil
}

func (p *peer) setConn(conn network.Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conn = conn
}

func (p *peer) getConn() network.Connection {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.conn
}

func (p *peer) send(frame *protocol.MessageFrame) error {
	con := p.getConn()
	if con == nil {
		return errors.New("peer conn nil ")
	}
	if p.getState() != PeerStateConnected {
		return errors.New("peer is not connected")
	}
	return con.SendMessage(frame)
}

func (p *peer) close() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.conn != nil {
		p.conn.Close(nil)
	}
}
