package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dzm2020/actormesh/cluster/member"
	"github.com/dzm2020/actormesh/cluster/transport"
	"github.com/dzm2020/actormesh/pkg/glog"
)

type fakeRegistry struct {
	mu       sync.Mutex
	run      int
	joins    []member.NodeInfo
	leaves   []string
	members  []member.NodeInfo
	services map[string]map[string]member.NodeInfo
}

func (r *fakeRegistry) Run(context.Context) error {
	r.mu.Lock()
	r.run++
	r.mu.Unlock()
	return nil
}

func (r *fakeRegistry) Join(node member.NodeInfo) error {
	r.mu.Lock()
	r.joins = append(r.joins, node)
	r.mu.Unlock()
	return nil
}

func (r *fakeRegistry) Update(member.NodeInfo) error { return nil }

func (r *fakeRegistry) Leave(id string) error {
	r.mu.Lock()
	r.leaves = append(r.leaves, id)
	r.mu.Unlock()
	return nil
}

func (r *fakeRegistry) AllMembers() []member.NodeInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]member.NodeInfo(nil), r.members...)
}

func (r *fakeRegistry) Members(service string) map[string]member.NodeInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]member.NodeInfo)
	for id, node := range r.services[service] {
		result[id] = node
	}
	return result
}

func (r *fakeRegistry) MemberById(id string) (member.NodeInfo, bool) {
	for _, node := range r.AllMembers() {
		if node.ID == id {
			return node, true
		}
	}
	return member.NodeInfo{}, false
}

type fakeTransport struct {
	mu          sync.Mutex
	run         int
	closed      int
	connects    []connectCall
	connection  map[string]transport.PeerState
	sendNode    string
	sendData    []byte
	broadcast   []byte
	connectDone chan connectCall
}

type connectCall struct {
	nodeID  string
	address string
	timeout time.Duration
}

func (t *fakeTransport) Run() error {
	t.mu.Lock()
	t.run++
	t.mu.Unlock()
	return nil
}

func (t *fakeTransport) Connect(nodeID, address string, timeout time.Duration) error {
	call := connectCall{nodeID: nodeID, address: address, timeout: timeout}
	t.mu.Lock()
	t.connects = append(t.connects, call)
	t.mu.Unlock()
	if t.connectDone != nil {
		t.connectDone <- call
	}
	return nil
}

func (t *fakeTransport) ConnectionState(nodeID string) transport.PeerState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.connection[nodeID]
}

func (t *fakeTransport) Disconnect(string) error { return nil }

func (t *fakeTransport) Send(nodeID string, data []byte) error {
	t.mu.Lock()
	t.sendNode = nodeID
	t.sendData = append([]byte(nil), data...)
	t.mu.Unlock()
	return nil
}

func (t *fakeTransport) Broadcast(data []byte) error {
	t.mu.Lock()
	t.broadcast = append([]byte(nil), data...)
	t.mu.Unlock()
	return nil
}

func (t *fakeTransport) Close() {
	t.mu.Lock()
	t.closed++
	t.mu.Unlock()
}

func testCluster() (*Cluster, *fakeRegistry, *fakeTransport) {
	registry := &fakeRegistry{}
	transportImpl := &fakeTransport{
		connection:  make(map[string]transport.PeerState),
		connectDone: make(chan connectCall, 1),
	}
	c := NewWithOptions(Options{
		NodeInfo:  member.NodeInfo{ID: "node-a", Name: "worker", Address: "127.0.0.1:9001"},
		Registry:  registry,
		Transport: transportImpl,
		Logger:    glog.Log(),
		Handler:   func(string, []byte) error { return nil },
	})
	return c, registry, transportImpl
}

func TestClusterInitRejectsInvalidNode(t *testing.T) {
	_ = NewWithOptions(Options{
		Registry:  &fakeRegistry{},
		Transport: &fakeTransport{connection: make(map[string]transport.PeerState)},
	})
}

func TestClusterStartRunsDependenciesAndJoins(t *testing.T) {
	c, registry, transportImpl := testCluster()
	if err := c.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	registry.mu.Lock()
	runCount, joinCount := registry.run, len(registry.joins)
	registry.mu.Unlock()
	transportImpl.mu.Lock()
	runCountTransport, closeCount := transportImpl.run, transportImpl.closed
	transportImpl.mu.Unlock()
	if runCount != 1 || joinCount != 1 {
		t.Fatalf("registry lifecycle = run %d, joins %d", runCount, joinCount)
	}
	if runCountTransport != 1 || closeCount != 1 {
		t.Fatalf("transport lifecycle = run %d, close %d", runCountTransport, closeCount)
	}
}

func TestClusterConnectMemberUsesAddressAndDirection(t *testing.T) {
	c, _, transportImpl := testCluster()

	c.connectMember(member.NodeInfo{ID: "node-z", Name: "worker", Address: "10.0.0.2:9000"})
	select {
	case call := <-transportImpl.connectDone:
		if call.nodeID != "node-z" || call.address != "10.0.0.2:9000" || call.timeout != 5*time.Second {
			t.Fatalf("connect call = %+v", call)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connect")
	}

	c.connectMember(member.NodeInfo{ID: "node-0", Name: "worker", Address: "10.0.0.3:9000"})
	select {
	case call := <-transportImpl.connectDone:
		t.Fatalf("unexpected reverse connect: %+v", call)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestClusterConnectMemberSkipsConnectedPeer(t *testing.T) {
	c, _, transportImpl := testCluster()
	transportImpl.connection["node-z"] = transport.PeerStateConnected
	c.connectMember(member.NodeInfo{ID: "node-z", Name: "worker", Address: "10.0.0.2:9000"})
	select {
	case call := <-transportImpl.connectDone:
		t.Fatalf("unexpected connect: %+v", call)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestClusterDelegatesMessagingAndMembership(t *testing.T) {
	c, registry, transportImpl := testCluster()
	registry.members = []member.NodeInfo{{ID: "node-z", Name: "worker"}}
	registry.services = map[string]map[string]member.NodeInfo{"worker": {"node-z": registry.members[0]}}

	if err := c.SendToNode("node-z", []byte("hello")); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := c.Broadcast([]byte("all")); err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	if err := c.Join(); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := c.Leave(); err != nil {
		t.Fatalf("leave: %v", err)
	}

	transportImpl.mu.Lock()
	if transportImpl.sendNode != "node-z" || string(transportImpl.sendData) != "hello" || string(transportImpl.broadcast) != "all" {
		t.Fatalf("messaging delegation failed: node=%q send=%q broadcast=%q", transportImpl.sendNode, transportImpl.sendData, transportImpl.broadcast)
	}
	transportImpl.mu.Unlock()
	registry.mu.Lock()
	if len(registry.joins) != 1 || len(registry.leaves) != 1 || registry.leaves[0] != "node-a" {
		t.Fatalf("membership delegation failed: joins=%v leaves=%v", registry.joins, registry.leaves)
	}
	registry.mu.Unlock()

	if got := c.AllMembers(); len(got) != 1 || got[0].ID != "node-z" {
		t.Fatalf("all members = %+v", got)
	}
	if got := c.Members("worker"); got["node-z"].ID != "node-z" {
		t.Fatalf("members = %+v", got)
	}
	if got, ok := c.MemberById("node-z"); !ok || got.ID != "node-z" {
		t.Fatalf("member by id = %+v, %v", got, ok)
	}
}

func TestClusterStartPropagatesTransportError(t *testing.T) {
	expected := errors.New("transport failed")
	transportImpl := &errorTransport{err: expected}
	c := NewWithOptions(Options{
		NodeInfo:  member.NodeInfo{ID: "node-a", Name: "worker"},
		Registry:  &fakeRegistry{},
		Transport: transportImpl,
		Logger:    glog.Log(),
		Handler:   func(string, []byte) error { return nil },
	})
	if err := c.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := c.Start(); !errors.Is(err, expected) {
		t.Fatalf("start error = %v, want %v", err, expected)
	}
}

type errorTransport struct{ err error }

func (t *errorTransport) Run() error                                { return t.err }
func (*errorTransport) Connect(string, string, time.Duration) error { return nil }
func (*errorTransport) ConnectionState(string) transport.PeerState  { return transport.PeerStateIdle }
func (*errorTransport) Disconnect(string) error                     { return nil }
func (*errorTransport) Send(string, []byte) error                   { return nil }
func (*errorTransport) Broadcast([]byte) error                      { return nil }
func (*errorTransport) Close()                                      {}
