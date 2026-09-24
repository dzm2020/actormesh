package actor

import (
	"fmt"
	"github.com/dzm2020/actormesh/pkg/grs"
	"sync"
	"time"
)

func newManger(nodeId string) *manager {
	return &manager{
		nodeId:    nodeId,
		waitGroup: &sync.WaitGroup{},
		ids:       make(map[uint64]*Process),
		names:     make(map[string]*Process),
	}
}

type manager struct {
	sync.RWMutex
	waitGroup *sync.WaitGroup
	nodeId    string
	ids       map[uint64]*Process // actor ID -> process
	names     map[string]*Process // actor name -> process
}

func (m *manager) namedLocked(name string) bool {
	_, ok := m.names[name]
	return ok
}

func (m *manager) addLocked(pid *PID, p *Process) {
	if pid.ActorName != "" {
		m.names[pid.ActorName] = p
	}
	if pid.ActorID > 0 {
		m.ids[pid.ActorID] = p
		m.waitGroup.Add(1)
	}
}

func (m *manager) remove(pid *PID) {
	m.removeName(pid.ActorName)
	m.removeId(pid.ActorID)
}
func (m *manager) removeId(id uint64) {
	if id <= 0 {
		return
	}
	m.Lock()
	defer m.Unlock()
	_, ok := m.ids[id]
	if !ok {
		return
	}
	m.waitGroup.Add(-1)
	delete(m.ids, id)
}

func (m *manager) removeName(name string) {
	if name == "" {
		return
	}
	m.Lock()
	defer m.Unlock()
	delete(m.names, name)
}

func (m *manager) getByName(name string) *Process {
	if name == "" {
		return nil
	}
	m.RLock()
	defer m.RUnlock()
	p, _ := m.names[name]
	return p
}

func (m *manager) getById(id uint64) *Process {
	if id <= 0 {
		return nil
	}
	m.RLock()
	defer m.RUnlock()
	p, _ := m.ids[id]
	return p
}

func (m *manager) count() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.ids)
}

// getProcess 不应该对外导致 process只对外暴漏pid 通过pid操作process
func (m *manager) getProcess(pid *PID) (*Process, error) {
	if pid == nil {
		return nil, ErrPidNil
	}
	if pid.NodeID != m.nodeId {
		return nil, fmt.Errorf("nodeId:%s %w", pid.NodeID, ErrNotLocal)
	}
	if p := m.getById(pid.ActorID); p != nil {
		return p, nil
	}
	if p := m.getByName(pid.ActorName); p != nil {
		return p, nil
	}
	return nil, ErrNotFound
}

func (m *manager) stop() error {
	m.RLock()
	for _, process := range m.ids {
		process.Stop(NoSender)
	}
	m.RUnlock()
	return grs.WaitWithTimeout(m.waitGroup, time.Second*10)
}
