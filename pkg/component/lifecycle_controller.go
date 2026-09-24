package component

import (
	"errors"
	"fmt"
	"sync"
)

type State uint8

const (
	StateNew State = iota
	StateInited
	StateStarted
	StateStopped
)

type LifecycleState = State

const (
	LifecycleStateNew     LifecycleState = StateNew
	LifecycleStateInited  LifecycleState = StateInited
	LifecycleStateStarted LifecycleState = StateStarted
	LifecycleStateStopped LifecycleState = StateStopped
)

func (s State) String() string {
	switch s {
	case StateNew:
		return "new"
	case StateInited:
		return "inited"
	case StateStarted:
		return "started"
	case StateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

var (
	ErrInitAlreadyCalled  = errors.New("init already called")
	ErrStartAlreadyCalled = errors.New("start already called")
	ErrStopAlreadyCalled  = errors.New("stop already called")
	ErrInvalidOrder       = errors.New("invalid lifecycle order")
)

type Controller struct {
	mu sync.Mutex

	state State

	initCalled  bool
	startCalled bool
	stopCalled  bool
}

func (c *Controller) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *Controller) Init(run func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.initCalled {
		return ErrInitAlreadyCalled
	}
	if c.state != StateNew {
		return fmt.Errorf("%w: current=%s want=%s", ErrInvalidOrder, c.state, StateNew)
	}
	c.initCalled = true

	if run != nil {
		if err := run(); err != nil {
			return fmt.Errorf("init failed: %w", err)
		}
	}
	c.state = StateInited
	return nil
}

func (c *Controller) Start(run func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.startCalled {
		return ErrStartAlreadyCalled
	}
	if c.state != StateInited {
		return fmt.Errorf("%w: current=%s want=%s", ErrInvalidOrder, c.state, StateInited)
	}
	c.startCalled = true

	if run != nil {
		if err := run(); err != nil {
			return fmt.Errorf("start failed: %w", err)
		}
	}
	c.state = StateStarted
	return nil
}

func (c *Controller) Stop(run func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopCalled {
		return ErrStopAlreadyCalled
	}
	if !c.initCalled {
		return fmt.Errorf("%w: current=%s want=init attempted", ErrInvalidOrder, c.state)
	}
	c.stopCalled = true

	if run != nil {
		if err := run(); err != nil {
			return fmt.Errorf("stop failed: %w", err)
		}
	}
	c.state = StateStopped
	return nil
}
