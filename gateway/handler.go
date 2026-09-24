package gateway

import (
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/network/protocol"
)

var ErrClientAgentNotFound = errors.New("gateway client agent not found")

var _ network.TransportHandler = (*connectionHandler)(nil)

type connectionHandler struct {
	gateway *Gateway
}

func (h *connectionHandler) OnConnected(connection network.Connection) error {
	//  确保退出时不再接受新链接
	h.gateway.acceptMu.RLock()
	defer h.gateway.acceptMu.RUnlock()
	if h.gateway.closing {
		return errors.New("gateway is closing")
	}
	pid, err := h.gateway.spawner(connection)
	if err != nil {
		return fmt.Errorf("agent handle connected spawn err:%w", err)
	}
	connection.SetUserData(pid)
	h.gateway.group.Add(1)
	h.gateway.pids.Set(connection.ID(), pid)
	return nil
}

func (h *connectionHandler) OnMessage(connection network.Connection, packet *protocol.MessageFrame) error {
	//  确保退出时不再接受新包
	h.gateway.acceptMu.RLock()
	defer h.gateway.acceptMu.RUnlock()
	if h.gateway.closing {
		return nil
	}
	agentPid, ok := connection.UserData().(*actor.PID)
	if !ok {
		return fmt.Errorf("agent handle message err:%w", ErrClientAgentNotFound)
	}
	return h.gateway.system.SendEnvelope(agentPid, actor.Envelope{
		Sender:  agentPid,
		Payload: packet,
	})
}

func (h *connectionHandler) OnClose(connection network.Connection, cause error) {
	agentPid, ok := connection.UserData().(*actor.PID)
	if !ok {
		return
	}
	h.gateway.system.StopProcess(actor.NoSender, agentPid)
	h.gateway.pids.Delete(connection.ID())
	h.gateway.group.Done()
	connection.Log().Info("gate connection close")
}
