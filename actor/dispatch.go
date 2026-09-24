package actor

// 可优化为协程池

type IDispatcher interface {
	Schedule(f func())
	Throughput() int
}

// 协程调度器
type goroutineDispatcher int

func NewDefaultDispatcher(throughput int) IDispatcher {
	return goroutineDispatcher(throughput)
}
func (goroutineDispatcher) Schedule(fn func()) {
	go func() {
		fn()
	}()
}

func (d goroutineDispatcher) Throughput() int {
	return int(d)
}

// 同步调度器
type synchronizedDispatcher int

func (synchronizedDispatcher) Schedule(fn func()) {
	fn()
}

func (d synchronizedDispatcher) Throughput() int {
	return int(d)
}

func NewSynchronizedDispatcher(throughput int) IDispatcher {
	return synchronizedDispatcher(throughput)
}
