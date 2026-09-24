package nettransport

import (
	"context"
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/glog"
	"time"

	"github.com/dzm2020/actormesh/cluster/transport"
	"github.com/dzm2020/actormesh/network"

	"go.uber.org/zap"
)

var (
	ErrMessageNil       = errors.New("rpc message is nil")
	ErrHandlerIsNil     = errors.New("rpc business handler is nil")
	ErrPeerNotConnected = errors.New("rpc peer is not connected")
)

var _ transport.Transport = (*Transport)(nil)

func NewTransport(localNodeID string) *Transport {
	return NewTransportWithOptions(localNodeID, network.DefaultTCPConfig(""))
}

func NewTransportWithOptions(localNodeID string, config *network.TCPConfig) *Transport {
	config.Normalize()
	t := &Transport{
		localNodeID: localNodeID,
		config:      config,
		peers:       newPeerManager(),
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	return t
}

type Transport struct {
	localNodeID string
	config      *network.TCPConfig
	peers       *peerManager // 连接管理器
	ctx         context.Context
	cancel      context.CancelFunc
}

func (t *Transport) newConfig(address string) *network.TCPConfig {
	config := *t.config
	config.Address = address
	return &config
}

func (t *Transport) newHandler(businessHandler transport.MessageHandler) *transportHandler {
	return &transportHandler{
		businessHandler:   businessHandler,
		peers:             t.peers,
		localId:           t.localNodeID,
		heartbeatInterval: t.config.HeartbeatTimeout / 2,
	}
}

func (t *Transport) ListenAndServe(address string, handler transport.MessageHandler) error {
	if handler == nil {
		return ErrHandlerIsNil
	}
	server := network.NewTCPServer(t.newConfig(address))
	if err := server.Run(t.ctx, t.newHandler(handler)); err != nil {
		return fmt.Errorf("rpc server run err:%w", err)
	}

	glog.Info("rpc listen", zap.String("address", address))
	return nil
}

func (t *Transport) ConnectionState(nodeId string) PeerState {
	p, ok := t.peers.get(nodeId)
	if !ok {
		return PeerStateIdle
	}
	return p.getState()
}

func (t *Transport) Connect(remoteId string, address string, handler transport.MessageHandler, timeout time.Duration) error {
	if t.localNodeID == remoteId {
		return fmt.Errorf("rpc connect connect local:%s", remoteId)
	}
	if handler == nil {
		return ErrHandlerIsNil
	}
	t.peers.Lock()
	if _, ok := t.peers.getLocked(remoteId); ok {
		t.peers.Unlock()
		return nil
	}
	h := newPeerWithId(remoteId)
	t.peers.setLocked(remoteId, h)
	t.peers.Unlock()

	conn, err := network.DialTCP(t.ctx, timeout, t.newHandler(handler), t.newConfig(address), h)
	if err != nil {
		t.peers.remove(remoteId, h)
		glog.Error("rpc connect", zap.String("remoteId", remoteId), zap.Error(err))
		return err
	}
	if err = t.ctx.Err(); err != nil {
		conn.Close(err)
		t.peers.remove(remoteId, h)
		return err
	}
	glog.Info("rpc connect success", zap.String("remoteId", remoteId), zap.String("address", address))
	return nil
}

func (t *Transport) Disconnect(nodeId string) error {
	h, ok := t.peers.get(nodeId)
	if !ok {
		return fmt.Errorf("rpc peer not exist:%s", nodeId)
	}
	h.close()
	glog.Info("rpc  disconnected", zap.String("remoteId", nodeId))
	return nil
}

func (t *Transport) Send(nodeId string, data []byte) error {
	p, ok := t.peers.get(nodeId)
	if !ok {
		return ErrPeerNotConnected
	}
	if p.getState() != PeerStateConnected {
		return ErrPeerNotConnected
	}
	return t.send(p, data)
}

func (t *Transport) Broadcast(data []byte) error {
	helpers := t.peers.snapshot()
	var sent int
	var lastErr error
	for _, h := range helpers {
		if err := t.send(h, data); err != nil {
			lastErr = err
			continue
		}
		sent++
	}
	if sent != len(helpers) {
		glog.Warn("rpc broadcast incomplete",
			zap.Int("sent_count", sent),
			zap.Int("attempted", len(helpers)), zap.Error(lastErr))
		return nil
	}
	return nil
}

func (t *Transport) send(p *peer, data []byte) error {
	message := protocol.NewMessageFrame(clusterMessageCommand, clusterMessageActData, 0, data)
	return p.send(message)
}

func (t *Transport) Close() {
	if t.cancel != nil {
		t.cancel()
	}
	helpers := t.peers.snapshot()
	for _, h := range helpers {
		h.close()
	}
}
