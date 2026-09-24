package actor

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const actorConcurrencyTestTimeout = 3 * time.Second

type noOpMailboxDispatcher struct{}

func (noOpMailboxDispatcher) Schedule(func()) {}

func (noOpMailboxDispatcher) Throughput() int {
	return 1
}

func startConcurrencyTestSystem(t *testing.T) *System {
	t.Helper()

	system := NewSystem(fmt.Sprintf("actor-test-%d", time.Now().UnixNano()), nil)
	if err := system.Init(); err != nil {
		t.Fatalf("init actor system: %v", err)
	}
	if err := system.Start(); err != nil {
		t.Fatalf("start actor system: %v", err)
	}
	t.Cleanup(func() {
		_ = system.Stop()
	})
	return system
}

func waitConcurrencySignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(actorConcurrencyTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type blockingTestActor struct {
	initReady      chan struct{}
	blockStarted   chan struct{}
	release        chan struct{}
	destroyStarted chan struct{}
	destroyDone    chan struct{}

	initOnce    sync.Once
	blockOnce   sync.Once
	destroyOnce sync.Once
	destroyed   atomic.Int32
}

func (a *blockingTestActor) Init(Context) {
	a.initOnce.Do(func() {
		close(a.initReady)
	})
}

func (a *blockingTestActor) HandleTell(_ Context, message any) {
	if _, ok := message.(blockTestMessage); ok {
		a.blockOnce.Do(func() {
			close(a.blockStarted)
		})
		<-a.release
	}
}

func (*blockingTestActor) HandleAsk(Context, any) (any, error) {
	return nil, nil
}

func (a *blockingTestActor) Destroy(Context) {
	a.destroyed.Add(1)
	a.destroyOnce.Do(func() {
		close(a.destroyStarted)
		close(a.destroyDone)
	})
}

func (*blockingTestActor) Panic(Context, any) {}

type blockTestMessage struct{}

func TestMailboxUserMessagesRespectCapacityUnderConcurrentProducers(t *testing.T) {
	const (
		capacity  = int32(7)
		producers = 64
		perWorker = 32
	)

	mailbox := newMailbox(capacity)
	mailbox.RegisterHandlers(nil, noOpMailboxDispatcher{})

	var pushers sync.WaitGroup
	var accepted atomic.Int32
	var rejected atomic.Int32
	for worker := 0; worker < producers; worker++ {
		pushers.Add(1)
		go func(worker int) {
			defer pushers.Done()
			for message := 0; message < perWorker; message++ {
				err := mailbox.PushUser(struct {
					worker  int
					message int
				}{worker: worker, message: message})
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, ErrMailboxFull):
					rejected.Add(1)
				default:
					t.Errorf("unexpected mailbox push error: %v", err)
				}
			}
		}(worker)
	}
	pushers.Wait()

	if got := accepted.Load(); got != capacity {
		t.Fatalf("accepted messages = %d, want exactly mailbox capacity %d", got, capacity)
	}
	if got := mailbox.messages.Load(); got != capacity {
		t.Fatalf("mailbox message count = %d, want %d", got, capacity)
	}
	wantRejected := int32(producers*perWorker) - capacity
	if got := rejected.Load(); got != wantRejected {
		t.Fatalf("rejected messages = %d, want %d", got, wantRejected)
	}
}

func TestConcurrentPushAndStopRejectsMessagesAfterProcessStartsClosing(t *testing.T) {
	system := startConcurrencyTestSystem(t)
	actor := &blockingTestActor{
		initReady:      make(chan struct{}),
		blockStarted:   make(chan struct{}),
		release:        make(chan struct{}),
		destroyStarted: make(chan struct{}),
		destroyDone:    make(chan struct{}),
	}
	pid, err := system.SpawnActor(actor, SpawnOptions{
		Name:        "concurrent-stop",
		MailboxSize: 1,
	})
	if err != nil {
		t.Fatalf("spawn actor: %v", err)
	}
	waitConcurrencySignal(t, actor.initReady, "actor init")

	if err := system.Tell(NoSender, pid, blockTestMessage{}); err != nil {
		t.Fatalf("send blocking message: %v", err)
	}
	waitConcurrencySignal(t, actor.blockStarted, "actor blocker")
	if err := system.Tell(NoSender, pid, "queued"); err != nil {
		t.Fatalf("fill actor mailbox: %v", err)
	}

	stopDone := make(chan struct{})
	go func() {
		system.StopProcess(NoSender, pid)
		close(stopDone)
	}()

	waitConcurrencySignal(t, stopDone, "process stop request")

	const concurrentPushes = 128
	var pushers sync.WaitGroup
	var stopped atomic.Int32
	for i := 0; i < concurrentPushes; i++ {
		pushers.Add(1)
		go func() {
			defer pushers.Done()
			err := system.Tell(NoSender, pid, "after-stop")
			if errors.Is(err, ErrActorProcessStopped) {
				stopped.Add(1)
			}
		}()
	}
	pushers.Wait()
	if got := stopped.Load(); got != concurrentPushes {
		t.Fatalf("messages accepted after process began closing = %d, want 0", concurrentPushes-got)
	}

	close(actor.release)
	waitConcurrencySignal(t, actor.destroyDone, "actor destroy")
	if got := actor.destroyed.Load(); got != 1 {
		t.Fatalf("destroy count = %d, want exactly 1", got)
	}
}

func TestSystemStopSucceedsWhenUserMailboxIsFull(t *testing.T) {
	system := startConcurrencyTestSystem(t)
	actor := &blockingTestActor{
		initReady:      make(chan struct{}),
		blockStarted:   make(chan struct{}),
		release:        make(chan struct{}),
		destroyStarted: make(chan struct{}),
		destroyDone:    make(chan struct{}),
	}
	pid, err := system.SpawnActor(actor, SpawnOptions{
		Name:        "full-mailbox-stop",
		MailboxSize: 1,
	})
	if err != nil {
		t.Fatalf("spawn actor: %v", err)
	}
	waitConcurrencySignal(t, actor.initReady, "actor init")
	if err := system.Tell(NoSender, pid, blockTestMessage{}); err != nil {
		t.Fatalf("send blocking message: %v", err)
	}
	waitConcurrencySignal(t, actor.blockStarted, "actor blocker")
	if err := system.Tell(NoSender, pid, "queued"); err != nil {
		t.Fatalf("fill actor mailbox: %v", err)
	}
	process, err := system.mgr.getProcess(pid)
	if err != nil {
		t.Fatalf("find actor process: %v", err)
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- system.Stop()
	}()
	waitForProcessClosing(t, process)
	select {
	case <-actor.destroyDone:
		t.Fatal("actor destroy completed while message handler was blocked")
	default:
	}
	select {
	case err := <-stopDone:
		t.Fatalf("system stop returned while actor was blocked: %v", err)
	default:
	}

	close(actor.release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stop actor system: %v", err)
		}
	case <-time.After(actorConcurrencyTestTimeout):
		t.Fatal("timed out waiting for system stop")
	}
	if got := actor.destroyed.Load(); got != 1 {
		t.Fatalf("destroy count = %d, want exactly 1", got)
	}
}

type tickerBlockingTestActor struct {
	initReady    chan struct{}
	blockStarted chan struct{}
	release      chan struct{}
	ticked       chan struct{}
	destroyDone  chan struct{}
	initOnce     sync.Once
	blockOnce    sync.Once
	tickOnce     sync.Once
	destroyOnce  sync.Once
	tickCount    atomic.Int32
}

func (a *tickerBlockingTestActor) Init(ctx Context) {
	ctx.Ticker(10*time.Millisecond, func(Context) {
		a.tickCount.Add(1)
		a.tickOnce.Do(func() {
			close(a.ticked)
		})
	})
	a.initOnce.Do(func() {
		close(a.initReady)
	})
}

func (a *tickerBlockingTestActor) HandleTell(_ Context, message any) {
	if _, ok := message.(blockTestMessage); !ok {
		return
	}
	a.blockOnce.Do(func() {
		close(a.blockStarted)
	})
	<-a.release
}

func (*tickerBlockingTestActor) HandleAsk(Context, any) (any, error) {
	return nil, nil
}

func (a *tickerBlockingTestActor) Destroy(Context) {
	a.destroyOnce.Do(func() {
		close(a.destroyDone)
	})
}

func (*tickerBlockingTestActor) Panic(Context, any) {}

func waitForMailboxCount(t *testing.T, mailbox *Mailbox, want int32) {
	t.Helper()
	deadline := time.NewTimer(actorConcurrencyTestTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()

	for {
		if got := mailbox.messages.Load(); got >= want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for mailbox count %d, got %d", want, mailbox.messages.Load())
		case <-poll.C:
		}
	}
}

func waitForProcessClosing(t *testing.T, process *Process) {
	t.Helper()
	deadline := time.NewTimer(actorConcurrencyTestTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()

	for {
		process.closingMu.RLock()
		closing := process.closing
		process.closingMu.RUnlock()
		if closing {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for actor process to start closing")
		case <-poll.C:
		}
	}
}

func TestTickerIsDeliveredWhenUserMailboxIsFull(t *testing.T) {
	system := startConcurrencyTestSystem(t)
	actor := &tickerBlockingTestActor{
		initReady:    make(chan struct{}),
		blockStarted: make(chan struct{}),
		release:      make(chan struct{}),
		ticked:       make(chan struct{}),
		destroyDone:  make(chan struct{}),
	}
	pid, err := system.SpawnActor(actor, SpawnOptions{
		Name:        "ticker-full-mailbox",
		MailboxSize: 1,
	})
	if err != nil {
		t.Fatalf("spawn actor: %v", err)
	}
	waitConcurrencySignal(t, actor.initReady, "actor init")
	if err := system.Tell(NoSender, pid, blockTestMessage{}); err != nil {
		t.Fatalf("send blocking message: %v", err)
	}
	waitConcurrencySignal(t, actor.blockStarted, "actor blocker")
	if err := system.Tell(NoSender, pid, "queued"); err != nil {
		t.Fatalf("fill actor mailbox: %v", err)
	}

	process, err := system.mgr.getProcess(pid)
	if err != nil {
		t.Fatalf("find actor process: %v", err)
	}
	waitForMailboxCount(t, process.mailbox, 2)

	close(actor.release)
	waitConcurrencySignal(t, actor.ticked, "ticker task")

	if err := system.Stop(); err != nil {
		t.Fatalf("stop actor system: %v", err)
	}
	waitConcurrencySignal(t, actor.destroyDone, "actor destroy")
	if got := len(process.timers); got != 0 {
		t.Fatalf("registered timers after actor destroy = %d, want 0", got)
	}
	ticksAfterDestroy := actor.tickCount.Load()
	err = process.PushSystem(Envelope{
		Sender: NoSender,
		Payload: Task(func(Context) {
			actor.tickCount.Add(1)
		}),
	})
	if !errors.Is(err, ErrActorProcessStopped) {
		t.Fatalf("system task push after destroy error = %v, want %v", err, ErrActorProcessStopped)
	}
	if got := actor.tickCount.Load(); got != ticksAfterDestroy {
		t.Fatalf("ticker count changed after rejected post-destroy push: got %d, want %d", got, ticksAfterDestroy)
	}
}

type destroyBlockingTestActor struct {
	initReady      chan struct{}
	destroyStarted chan struct{}
	release        chan struct{}
	destroyDone    chan struct{}
	initOnce       sync.Once
	destroyOnce    sync.Once
}

func (a *destroyBlockingTestActor) Init(Context) {
	a.initOnce.Do(func() {
		close(a.initReady)
	})
}

func (*destroyBlockingTestActor) HandleTell(Context, any) {}

func (*destroyBlockingTestActor) HandleAsk(Context, any) (any, error) {
	return nil, nil
}

func (a *destroyBlockingTestActor) Destroy(Context) {
	a.destroyOnce.Do(func() {
		close(a.destroyStarted)
		<-a.release
		close(a.destroyDone)
	})
}

func (*destroyBlockingTestActor) Panic(Context, any) {}

func TestSystemStopWaitsForDestroyToComplete(t *testing.T) {
	system := startConcurrencyTestSystem(t)
	actor := &destroyBlockingTestActor{
		initReady:      make(chan struct{}),
		destroyStarted: make(chan struct{}),
		release:        make(chan struct{}),
		destroyDone:    make(chan struct{}),
	}
	if _, err := system.SpawnActor(actor, SpawnOptions{Name: "blocking-destroy"}); err != nil {
		t.Fatalf("spawn actor: %v", err)
	}
	waitConcurrencySignal(t, actor.initReady, "actor init")

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- system.Stop()
	}()
	waitConcurrencySignal(t, actor.destroyStarted, "actor destroy start")
	select {
	case err := <-stopDone:
		t.Fatalf("system stop returned before destroy completed: %v", err)
	default:
	}

	close(actor.release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stop actor system: %v", err)
		}
	case <-time.After(actorConcurrencyTestTimeout):
		t.Fatal("timed out waiting for system stop")
	}
	waitConcurrencySignal(t, actor.destroyDone, "actor destroy completion")
}

type spawnLifecycleTestActor struct {
	initCount    *atomic.Int32
	destroyCount *atomic.Int32
	initReady    chan struct{}
	destroyDone  chan struct{}
	initOnce     *sync.Once
	destroyOnce  *sync.Once
}

func (a *spawnLifecycleTestActor) Init(Context) {
	a.initCount.Add(1)
	a.initOnce.Do(func() {
		close(a.initReady)
	})
}

func (*spawnLifecycleTestActor) HandleTell(Context, any) {}

func (*spawnLifecycleTestActor) HandleAsk(Context, any) (any, error) {
	return nil, nil
}

func (a *spawnLifecycleTestActor) Destroy(Context) {
	a.destroyCount.Add(1)
	a.destroyOnce.Do(func() {
		close(a.destroyDone)
	})
}

func (*spawnLifecycleTestActor) Panic(Context, any) {}

func TestConcurrentSpawnWithSameNameCreatesOneActor(t *testing.T) {
	system := startConcurrencyTestSystem(t)

	const spawners = 64
	var initCount atomic.Int32
	var destroyCount atomic.Int32
	initReady := make(chan struct{})
	destroyDone := make(chan struct{})
	var initOnce sync.Once
	var destroyOnce sync.Once

	type spawnResult struct {
		pid *PID
		err error
	}
	results := make(chan spawnResult, spawners)
	var workers sync.WaitGroup
	for i := 0; i < spawners; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			handler := &spawnLifecycleTestActor{
				initCount:    &initCount,
				destroyCount: &destroyCount,
				initReady:    initReady,
				destroyDone:  destroyDone,
				initOnce:     &initOnce,
				destroyOnce:  &destroyOnce,
			}
			pid, err := system.SpawnActor(handler, SpawnOptions{Name: "same-name"})
			results <- spawnResult{pid: pid, err: err}
		}()
	}
	workers.Wait()
	close(results)

	var successes int
	for result := range results {
		if result.err == nil {
			successes++
			if result.pid == nil {
				t.Fatal("successful spawn returned nil PID")
			}
			continue
		}
		if !errors.Is(result.err, ErrNameExists) {
			t.Fatalf("unexpected duplicate spawn error: %v", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful duplicate spawns = %d, want 1", successes)
	}

	waitConcurrencySignal(t, initReady, "successful actor init")
	if got := initCount.Load(); got != 1 {
		t.Fatalf("init count = %d, want exactly 1", got)
	}

	if err := system.Stop(); err != nil {
		t.Fatalf("stop actor system: %v", err)
	}
	waitConcurrencySignal(t, destroyDone, "successful actor destroy")
	if got := destroyCount.Load(); got != 1 {
		t.Fatalf("destroy count = %d, want exactly 1", got)
	}
}

type askTestActor struct {
	initReady chan struct{}
	initOnce  sync.Once
}

func (a *askTestActor) Init(Context) {
	a.initOnce.Do(func() {
		close(a.initReady)
	})
}

func (*askTestActor) HandleTell(Context, any) {}

func (*askTestActor) HandleAsk(Context, any) (any, error) {
	return nil, nil
}

func (*askTestActor) Destroy(Context) {}

func (*askTestActor) Panic(Context, any) {}

func TestAskAsyncCompletesOnceWhenSystemStopsWithPendingRequest(t *testing.T) {
	system := startConcurrencyTestSystem(t)
	requester := &askTestActor{initReady: make(chan struct{})}
	target := &askTestActor{initReady: make(chan struct{})}
	requesterPID, err := system.SpawnActor(requester, SpawnOptions{Name: "ask-requester"})
	if err != nil {
		t.Fatalf("spawn requester: %v", err)
	}
	targetPID, err := system.SpawnActor(target, SpawnOptions{Name: "ask-target"})
	if err != nil {
		t.Fatalf("spawn target: %v", err)
	}
	waitConcurrencySignal(t, requester.initReady, "requester init")
	waitConcurrencySignal(t, target.initReady, "target init")

	completion := make(chan error, 1)
	var completions atomic.Int32
	if err := system.AskAsync(requesterPID, targetPID, "pending", time.Hour, func(_ Context, _ any, requestErr error) {
		completions.Add(1)
		completion <- requestErr
	}); err != nil {
		t.Fatalf("ask async: %v", err)
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- system.Stop()
	}()

	select {
	case requestErr := <-completion:
		if !errors.Is(requestErr, ErrRequestManagerClosed) {
			t.Fatalf("ask completion error = %v, want request manager closed", requestErr)
		}
	case <-time.After(actorConcurrencyTestTimeout):
		t.Fatal("timed out waiting for pending ask completion")
	}

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stop actor system: %v", err)
		}
	case <-time.After(actorConcurrencyTestTimeout):
		t.Fatal("timed out waiting for system stop")
	}
	if got := completions.Load(); got != 1 {
		t.Fatalf("ask completion count = %d, want exactly 1", got)
	}
}
