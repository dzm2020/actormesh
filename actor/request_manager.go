package actor

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/dzm2020/actormesh/pkg/timer"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const defaultMaxPendingRequests = 65536

type askResult struct {
	payload any
	err     error
}
type requestCompletion func(uint64, any, error)

type pendingRequest struct {
	targetNodeId string
	complete     requestCompletion
	timer        *timer.Timer
}

type pendingCompletion struct {
	requestID uint64
	complete  requestCompletion
}

func newRequestManager(maxPending int) *requestManager {
	if maxPending <= 0 {
		maxPending = defaultMaxPendingRequests
	}
	manager := &requestManager{
		pending: make(map[uint64]*pendingRequest),
		max:     maxPending,
	}
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err == nil {
		manager.nextRequestId.Store(binary.LittleEndian.Uint64(seed[:]))
	} else {
		manager.nextRequestId.Store(uint64(time.Now().UnixNano()))
	}
	return manager
}

type requestManager struct {
	mu            sync.Mutex
	nextRequestId atomic.Uint64
	pending       map[uint64]*pendingRequest
	max           int
	closed        bool
}

func (m *requestManager) add(nodeId, targetNodeId string, timeout time.Duration, complete requestCompletion) (*RequestRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrRequestManagerClosed
	}
	if len(m.pending) >= m.max {
		return nil, errors.Wrapf(ErrRequestLimit, "count:%d max%d", len(m.pending), m.max)
	}

	requestID := m.nextRequestId.Add(1)

	pending := &pendingRequest{targetNodeId: targetNodeId, complete: complete}
	m.pending[requestID] = pending
	if timeout > 0 {
		pending.timer = timer.After(timeout, func() {
			_ = m.complete(requestID, nil, ErrAskTimeout)
		})
	}
	return &RequestRef{NodeID: nodeId, RequestID: requestID}, nil
}

func (m *requestManager) complete(requestId uint64, value any, err error) error {
	pending := m.remove(requestId)
	if pending == nil {
		return fmt.Errorf("request:%d  :%w", requestId, ErrRequestNotExist)
	}
	pending.complete(requestId, value, err)
	return nil
}

func (m *requestManager) remove(requestID uint64) *pendingRequest {
	m.mu.Lock()
	pending := m.pending[requestID]
	delete(m.pending, requestID)
	m.mu.Unlock()

	if pending != nil {
		if pending.timer != nil {
			pending.timer.Stop()
		}
	}
	return pending
}

func (m *requestManager) retarget(requestID uint64, targetNodeID string) {
	if targetNodeID == "" {
		return
	}
	m.mu.Lock()
	if pending := m.pending[requestID]; pending != nil {
		pending.targetNodeId = targetNodeID
	}
	m.mu.Unlock()
}

func (m *requestManager) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pending)
}
func (m *requestManager) stop() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	completions := make([]pendingCompletion, 0, len(m.pending))
	for id, request := range m.pending {
		if request.timer != nil {
			request.timer.Stop()
		}
		completions = append(completions, pendingCompletion{
			requestID: id,
			complete:  request.complete,
		})
	}
	m.pending = make(map[uint64]*pendingRequest)
	m.mu.Unlock()

	for _, completion := range completions {
		completion.complete(completion.requestID, nil, ErrRequestManagerClosed)
	}
}
