package nettransport

import (
	"errors"
	"fmt"
	"game-server/framework/cluster/transport"
	"game-server/framework/network"
	"game-server/framework/network/protocol"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/grs"
	"time"

	"go.uber.org/zap"
)

const (
	clusterMessageCommand = uint8(1)

	clusterMessageActHello = uint8(iota)
	clusterMessageActData
	clusterMessageActHeartbeat
)

var (
	ErrPeerState      = errors.New("remote peer state error")
	ErrConnHelperNil  = errors.New("conn peer is nil")
	ErrConnHelperType = errors.New("conn peer type error")
)

var _ network.TransportHandler = (*transportHandler)(nil)

type transportHandler struct {
	localId           string
	businessHandler   transport.MessageHandler
	peers             *peerManager
	heartbeatInterval time.Duration
}

func (c *transportHandler) OnConnected(connection network.Connection) error {
	var (
		p   *peer
		err error
	)
	if connection.Role() == network.ConnectionRoleServer {
		connection.SetUserData(newPeer())
	}

	if p, err = c.getPeer(connection); err != nil {
		return fmt.Errorf("rpc connected  %w", err)
	}
	p.setConn(connection)

	//  状态错误 直接关闭连接
	if !p.swapState(PeerStateIdle, PeerStateHandshaking) {
		return fmt.Errorf("rpc connected state:%v %w", p.getState(), ErrPeerState)
	}
	//  发送握手包
	nodeId := c.localId
	msg := protocol.NewMessageFrame(clusterMessageCommand, clusterMessageActHello, 0, []byte(nodeId))
	if err = connection.SendMessage(msg); err != nil {
		glog.Error("rpc send hello", zap.Int64("connId", connection.ID()), zap.String("nodeId", nodeId), zap.Error(err))
		return err
	}
	glog.Info("rpc send hello", zap.Int64("connId", connection.ID()), zap.String("nodeId", nodeId))
	return nil
}

func (c *transportHandler) OnMessage(connection network.Connection, frame *protocol.MessageFrame) error {
	glog.Debug("rpc message", zap.Int64("connId", connection.ID()),
		zap.Uint8("cmd", frame.Cmd),
		zap.Uint8("cmd", frame.Act),
		zap.Binary("body", frame.Body),
	)
	switch frame.ID() {
	case protocol.CmdAct(clusterMessageCommand, clusterMessageActHello):
		if err := c.handleHello(connection, frame); err != nil {
			return fmt.Errorf("handle hello err:%w", err)
		}
	case protocol.CmdAct(clusterMessageCommand, clusterMessageActData):
		if err := c.handleData(connection, frame); err != nil {
			return fmt.Errorf("handle data err:%w", err)
		}
	case protocol.CmdAct(clusterMessageCommand, clusterMessageActHeartbeat):
		if err := c.handleHeartbeat(connection); err != nil {
			return fmt.Errorf("handle heartbeat err:%w", err)
		}
	default:
		glog.Error("rpc invalid msgId", zap.Int64("connId", connection.ID()),
			zap.Uint8("cmd", frame.Cmd), zap.Uint8("act", frame.Act))
		return nil
	}
	return nil
}

func (c *transportHandler) handleHeartbeat(connection network.Connection) error {
	p, err := c.getPeer(connection)
	if err != nil {
		return err
	}
	if p.getState() != PeerStateConnected {
		return fmt.Errorf("state:%v %w", p.getState(), ErrPeerState)
	}
	return nil
}

func (c *transportHandler) handleHello(connection network.Connection, message *protocol.MessageFrame) error {
	remoteId := string(message.Body)
	if remoteId == "" {
		return errors.New("remote id can not be empty")
	}
	current, err := c.getPeer(connection)
	if err != nil {
		return err
	}

	//  绑定remoteId
	if err = current.bindRemoteId(remoteId); err != nil {
		return err
	}
	c.peers.Lock()
	//  状态错误 直接关闭连接
	if !current.swapState(PeerStateHandshaking, PeerStateConnected) {
		c.peers.Unlock()
		return fmt.Errorf("state:%v %w", current.getState(), ErrPeerState)
	}
	//  关闭旧连接
	if oldHelper, ok := c.peers.getLocked(remoteId); ok {
		if oldHelper != current {
			oldHelper.close()
		}
	}
	//  添加新连接
	c.peers.setLocked(remoteId, current)
	c.peers.Unlock()

	//  启动心跳
	c.runHeartbeatLoop(connection)

	return nil
}

func (c *transportHandler) handleData(conn network.Connection, message *protocol.MessageFrame) error {
	p, err := c.getPeer(conn)
	if err != nil {
		return err
	}
	if p.getState() != PeerStateConnected {
		return fmt.Errorf("state:%v %w", p.getState(), ErrPeerState)
	}
	if err = c.businessHandler(p.getRemoteId(), message.Body); err != nil {
		glog.Error("rpc handle data", zap.Int64("connId", conn.ID()), zap.String("remoteId", p.getRemoteId()), zap.Error(err))
		return nil
	}
	return nil
}

func (c *transportHandler) OnClose(conn network.Connection, cause error) {
	p, err := c.getPeer(conn)
	if err != nil {
		return
	}
	c.peers.remove(p.getRemoteId(), p)
	glog.Info("rpc close", zap.Int64("connId", conn.ID()), zap.Error(cause))
}

func (c *transportHandler) getPeer(connection network.Connection) (*peer, error) {
	data := connection.UserData()
	if data == nil {
		return nil, ErrConnHelperNil
	}
	p, ok := data.(*peer)
	if !ok {
		return nil, fmt.Errorf("type:%T :%w", p, ErrConnHelperType)
	}
	return p, nil
}

func (c *transportHandler) runHeartbeatLoop(conn network.Connection) {
	if c.heartbeatInterval <= 0 {
		return
	}
	grs.SafeGo(func() {
		ticker := time.NewTicker(c.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-conn.Context().Done():
				return
			case <-ticker.C:
				msg := protocol.NewMessageFrame(clusterMessageCommand, clusterMessageActHeartbeat, 0, nil)
				if err := conn.SendMessage(msg); err != nil {
					glog.Warn("rpc send heartbeat", zap.Int64("connId", conn.ID()), zap.Error(err))
				}
			}
		}
	})
}
