package nettransport

import (
	"sync"
)

func newPeerManager() *peerManager {
	return &peerManager{
		remoteToConn: make(map[string]*peer),
	}
}

type peerManager struct {
	sync.RWMutex
	remoteToConn map[string]*peer
}

func (r *peerManager) set(remoteId string, p *peer) {
	r.Lock()
	defer r.Unlock()
	r.remoteToConn[remoteId] = p
}

func (r *peerManager) get(remoteId string) (*peer, bool) {
	r.RLock()
	defer r.RUnlock()
	p, ok := r.remoteToConn[remoteId]
	return p, ok
}

func (r *peerManager) setLocked(remoteId string, p *peer) {
	r.remoteToConn[remoteId] = p
}

func (r *peerManager) getLocked(remoteId string) (*peer, bool) {
	p, ok := r.remoteToConn[remoteId]
	return p, ok
}

func (r *peerManager) remove(remoteId string, p *peer) {
	r.Lock()
	defer r.Unlock()
	old := r.remoteToConn[remoteId]
	if old != p {
		return
	}
	delete(r.remoteToConn, remoteId)
}

func (r *peerManager) snapshot() map[string]*peer {
	r.RLock()
	defer r.RUnlock()
	helpers := make(map[string]*peer, len(r.remoteToConn))
	for remoteID, h := range r.remoteToConn {
		helpers[remoteID] = h
	}
	return helpers
}
