package actor

import (
	"fmt"
	"github.com/dzm2020/actormesh/pkg/component"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/snowflake"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

func NewSystem(nodeId string, sender RemoteSender) *System {
	s := &System{
		nodeID:       nodeId,
		remoteSender: sender,
		mgr:          newManger(nodeId),
		requests:     newRequestManager(defaultMaxPendingRequests),
	}
	s.SetName("actor")
	s.logger = glog.With(zap.String("component", s.GetName()))
	return s
}

var _ SystemAPI = (*System)(nil)
var _ component.IComponent = (*System)(nil)

type System struct {
	component.BaseComponent
	nodeID       string // 本节点 ID
	mgr          *manager
	remoteSender RemoteSender    // 远程发送端口
	requests     *requestManager // 同步调用管理器
	logger       *zap.Logger
	// 连接关闭相关
	spawnMu  sync.RWMutex
	stopping atomic.Bool
}

func (s *System) nextId() uint64 {
	return uint64(snowflake.GenId())
}

func (s *System) GetNodeID() string {
	return s.nodeID
}

func (s *System) SpawnActor(handler Actor, options SpawnOptions) (*PID, error) {
	// 读锁：允许多个 Spawn 并发，但会阻塞 Stop 的写锁
	s.spawnMu.RLock()
	defer s.spawnMu.RUnlock()
	if s.stopping.Load() {
		return nil, fmt.Errorf("actor is stopping")
	}
	if handler == nil {
		return NoSender, ErrHandlerNil
	}
	//  创建process
	process := newActorProcess(s, handler, options)

	//  已注册名字
	s.mgr.Lock()
	defer s.mgr.Unlock()
	if s.mgr.namedLocked(options.Name) {
		return nil, fmt.Errorf("%s:%w", options.Name, ErrNameExists)
	}
	//  发送初始化消息
	_ = process.mailbox.PushSystem(Envelope{Payload: &initEnvelopeMessage{}, Sender: NoSender})
	// 注册process
	s.mgr.addLocked(process.GetPID(), process)

	return process.GetPID(), nil
}

func (s *System) Has(pid *PID) bool {
	proc, err := s.mgr.getProcess(pid)
	if err != nil || proc == nil {
		return false
	}
	return true
}

func (s *System) StopProcess(from, target *PID) {
	proc, err := s.mgr.getProcess(target)
	if err != nil {
		return
	}
	proc.Stop(from)
}

func (s *System) Stop() error {
	return s.GuardStop(func() error {
		//  确保关闭期间不会产生新actor
		s.spawnMu.Lock()
		defer s.spawnMu.Unlock()
		s.stopping.Store(true)

		s.requests.stop()

		return s.mgr.stop()
	})
}
