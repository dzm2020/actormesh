package gateway

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/network"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/grs"

	"github.com/duke-git/lancet/v2/maputil"
)

var (
	ErrClientAgentNotFound = errors.New("gateway client agent not found")
	ErrGatewayClosing      = errors.New("gateway is closing")
)

var _ network.TransportHandler = (*Bridge)(nil)

type Bridge struct {
	system  ActorGateway
	spawner AgentSpawner
	pids    *maputil.ConcurrentMap[int64, *actor.PID]
	group   sync.WaitGroup
	mu      sync.RWMutex
	closing bool
}

func NewGatewayBridge(system ActorGateway, spawner AgentSpawner) *Bridge {
	return &Bridge{
		system:  system,
		spawner: spawner,
		pids:    maputil.NewConcurrentMap[int64, *actor.PID](10),
	}
}

func (b *Bridge) Validate() error {
	switch {
	case b.system == nil:
		return ErrSystemNil
	case b.spawner == nil:
		return ErrAgentSpawnerNil
	default:
		return nil
	}
}

func (b *Bridge) OnConnected(connection network.Connection) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closing {
		return ErrGatewayClosing
	}

	pid, err := b.spawner(connection)
	if err != nil {
		return fmt.Errorf("agent handle connected spawn err:%w", err)
	}
	connection.SetUserData(pid)
	b.group.Add(1)
	b.pids.Set(connection.ID(), pid)
	return nil
}

func (b *Bridge) OnMessage(connection network.Connection, packet *protocol.MessageFrame) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closing {
		return nil
	}

	agentPID, ok := connection.UserData().(*actor.PID)
	if !ok {
		return fmt.Errorf("agent handle message err:%w", ErrClientAgentNotFound)
	}
	return b.system.SendEnvelope(agentPID, actor.Envelope{
		Sender:  agentPID,
		Payload: packet,
	})
}

func (b *Bridge) OnClose(connection network.Connection, cause error) {
	agentPID, ok := connection.UserData().(*actor.PID)
	if !ok {
		return
	}
	if _, tracked := b.pids.GetAndDelete(connection.ID()); !tracked {
		return
	}
	b.system.StopProcess(actor.NoSender, agentPID)
	b.group.Done()
	connection.Log().Info("gate connection close")
}

func (b *Bridge) Close(timeout time.Duration) error {
	b.rejectNewConnections()
	return b.stopAgents(timeout)
}

func (b *Bridge) rejectNewConnections() {
	b.mu.Lock()
	b.closing = true
	b.mu.Unlock()
}

func (b *Bridge) stopAgents(timeout time.Duration) error {
	b.pids.Range(func(_ int64, pid *actor.PID) bool {
		b.system.StopProcess(actor.NoSender, pid)
		return true
	})
	return grs.WaitWithTimeout(&b.group, timeout)
}
