package grs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dzm2020/actormesh/pkg/glog"

	"go.uber.org/zap"
)

func SafeGo(fn func()) {
	go func() {
		Try(fn, nil)
	}()
}

func Try(fn func(), recoverFunc func(e error)) {
	defer func() {
		if rec := recover(); rec != nil {
			err, ok := rec.(error)
			if !ok {
				err = fmt.Errorf("panic: %v", rec)
			}
			if recoverFunc != nil {
				recoverFunc(err)
				return
			}
			glog.Panic("goroutine panic recovered",
				zap.Any("panic", rec),
				zap.Stack("stack"),
			)
		}
	}()
	if fn != nil {
		fn()
	}
}

func NewGroup(ctx context.Context) *Group {
	g := &Group{}
	if ctx == nil {
		ctx = context.Background()
	}
	g.ctx, g.cancel = context.WithCancel(ctx)
	return g
}

type Group struct {
	runWG  sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func (g *Group) Context() context.Context {
	return g.ctx
}
func (g *Group) Go(fn func(ctx context.Context)) {
	g.runWG.Add(1)
	go func() {
		defer g.runWG.Done()
		Try(func() {
			fn(g.ctx)
		}, nil)
	}()
}

func (g *Group) Cancel() {
	if g.cancel != nil {
		g.cancel()
	}
}
func (g *Group) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		g.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *Group) WaitTimeout(timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		g.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return context.DeadlineExceeded
	}
}

func NormalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx
}

func WaitWithTimeout(group *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("wait group timeout")
	}
}
