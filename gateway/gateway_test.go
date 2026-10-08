package gateway_test

import (
	"testing"

	"github.com/dzm2020/actormesh/actor"
	"github.com/dzm2020/actormesh/gateway"
	"github.com/dzm2020/actormesh/network"
)

func TestOptionsAcceptsAgentSpawner(t *testing.T) {
	g := gateway.New(gateway.Options{
		System: actor.NewSystemWithOptions(actor.SystemOptions{NodeID: "gateway-test"}),
		Spawner: func(network.Connection) (*actor.PID, error) {
			return nil, nil
		},
	})
	if err := g.Init(); err != nil {
		t.Fatalf("initialize gateway with public spawner option: %v", err)
	}
}
