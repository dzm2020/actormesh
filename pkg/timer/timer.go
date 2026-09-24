package timer

import (
	"time"

	"github.com/RussellLuo/timingwheel"
)

var (
	tw = timingwheel.NewTimingWheel(10*time.Millisecond, 6000)
)

type EveryScheduler struct {
	Interval time.Duration
}

func (s *EveryScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.Interval)
}

type Timer struct {
	*timingwheel.Timer
}

func init() {
	tw.Start()
}

// After 执行一次
func After(duration time.Duration, f func()) *Timer {
	t := tw.AfterFunc(duration, func() {
		if f != nil {
			f()
		}
	})
	return &Timer{Timer: t}
}

// Ticker 周期执行
func Ticker(duration time.Duration, f func()) *Timer {
	t := tw.ScheduleFunc(&EveryScheduler{Interval: duration}, f)
	return &Timer{Timer: t}
}
