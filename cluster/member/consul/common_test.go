package consul

import (
	"testing"

	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/hashicorp/consul/api"
)

func TestNodeInfoConsulRoundTrip(t *testing.T) {
	want := member.NodeInfo{
		ID:        "node-1",
		Name:      "worker",
		Address:   "10.0.0.1:9000",
		Status:    api.HealthPassing,
		StartedAt: 123,
		Version:   "v1.2.3",
		Meta:      map[string]string{"zone": "west"},
	}

	registration, err := instanceToRegistration(want, DefaultOptions())
	if err != nil {
		t.Fatalf("instanceToRegistration: %v", err)
	}
	if registration.Address != "10.0.0.1" || registration.Port != 9000 {
		t.Fatalf("consul endpoint = %s:%d", registration.Address, registration.Port)
	}

	got, ok := entriesToInstances([]*api.ServiceEntry{{
		Service: &api.AgentService{
			ID:      registration.ID,
			Service: registration.Name,
			Address: registration.Address,
			Port:    registration.Port,
			Meta:    registration.Meta,
		},
		Checks: api.HealthChecks{{Status: api.HealthPassing}},
	}})[want.ID]
	if !ok {
		t.Fatal("round-trip node was not returned")
	}
	if got.ID != want.ID || got.Name != want.Name || got.Address != want.Address ||
		got.StartedAt != want.StartedAt || got.Version != want.Version || got.Status != want.Status {
		t.Fatalf("round-trip node = %+v, want %+v", got, want)
	}
	if got.Meta["zone"] != "west" {
		t.Fatalf("round-trip metadata = %+v", got.Meta)
	}
	if _, ok := got.Meta[nodeInfoMetaKey]; ok {
		t.Fatalf("internal metadata key leaked: %+v", got.Meta)
	}
}

func TestInstanceToRegistrationRejectsInvalidAddress(t *testing.T) {
	if _, err := instanceToRegistration(member.NodeInfo{ID: "node-1", Name: "worker", Address: "not-an-endpoint"}, DefaultOptions()); err == nil {
		t.Fatal("expected invalid address error")
	}
}
