package actor

import (
	"fmt"
	"log"
	"testing"
	"time"
)

type EchoActor struct {
	t *testing.T
}

func (m *EchoActor) Init(Context) {
	m.t.Log("init")

}
func (m *EchoActor) HandleTell(ctx Context, message any) {
	m.t.Logf("tell:%v", message)
	switch command := message.(type) {
	case string:
		switch command {
		case "hello":
		case "panic":
			panic("test panic")
		case "stop":
			ctx.Stop()
		case "tick":
			ctx.Ticker(time.Second*1, func(ctx Context) {
				m.t.Log("tick success")
			})
		case "after":
			ctx.AfterFunc(time.Millisecond*100, func(ctx Context) {
				m.t.Log("after success")
			})
		}
	}
}

func (m *EchoActor) HandleAsk(_ Context, message any) (any, error) {
	text, ok := message.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", message)
	}
	return "echo: " + text, nil
}
func (m *EchoActor) Destroy(ctx Context) {
	m.t.Log("destroy ing")
	time.Sleep(time.Second * 5)
	m.t.Log("destroy ed")
}
func (m *EchoActor) Panic(ctx Context, e any) {
	m.t.Logf("panic:%v ", e)
}

func TestActor(t *testing.T) {
	system := NewSystem("node-1", nil)
	if err := system.Init(); err != nil {
		log.Fatal(err)
	}
	if err := system.Start(); err != nil {
		log.Fatal(err)
	}

	pid, err := system.SpawnActor(&EchoActor{t}, SpawnOptions{Name: "echo"})

	if err != nil {
		log.Fatal(err)
	}
	result, err := system.Ask(NoSender, pid, "hello", time.Second)
	if err != nil {
		log.Fatal(err)
	}

	if err = system.Tell(NoSender, pid, "hello"); err != nil {
		log.Fatal(err)
	}
	if err = system.Tell(NoSender, pid, "panic"); err != nil {
		log.Fatal(err)
	}
	if err = system.Tell(NoSender, pid, "hello 2"); err != nil {
		log.Fatal(err)
	}

	if err = system.Tell(NoSender, pid, "tick"); err != nil {
		log.Fatal(err)
	}
	if err = system.Tell(NoSender, pid, "after"); err != nil {
		log.Fatal(err)
	}

	t.Logf("ask result:%v", result)
	t.Log("system destroy ing")
	if err := system.Stop(); err != nil {
		log.Fatal(err)
	}
	t.Log("system destroy ed")
}
