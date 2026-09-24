package cluster

import (
	"game-server/framework/cluster/member"
	"game-server/framework/pkg/glog"
	"testing"
	"time"

	"go.uber.org/zap"
)

func Handler(nodeID string, data []byte) error {
	glog.Info("cluster receive message", zap.String("nodeID", nodeID), zap.ByteString("data", data))
	return nil
}

func startCluster(instance member.ServiceInstance) (ClusterAPI, error) {
	cluster := New(instance, Handler)

	if err := cluster.Init(); err != nil {
		return cluster, err
	}
	if err := cluster.Start(); err != nil {
		return cluster, err
	}
	if err := cluster.Join(); err != nil {
		return cluster, err
	}

	return cluster, nil
}

func TestClusterInit(t *testing.T) {
	cluster1, err := startCluster(member.ServiceInstance{
		ID:      "node-1",
		Name:    "node",
		Address: "127.0.0.1",
		Port:    8888,
		Meta:    nil,
	})
	if err != nil {
		t.Errorf("start cluster error: %s", err.Error())
	}

	if err = cluster1.Join(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 3)
	if members := cluster1.Members("node"); members != nil {
		t.Log(members)
	}
	if err = cluster1.Leave(); err != nil {
		t.Fatal(err)
	}
	if members := cluster1.Members("node"); members != nil {
		t.Log(members)
	}

	time.Sleep(time.Second * 5)
}

func TestClusterSend(t *testing.T) {
	cluster1, err := startCluster(member.ServiceInstance{
		ID:      "node-1",
		Name:    "node",
		Address: "127.0.0.1",
		Port:    8888,
		Meta:    nil,
	})
	if err != nil {
		t.Errorf("start cluster error: %s", err.Error())
	}

	cluster2, err := startCluster(member.ServiceInstance{
		ID:      "node-2",
		Name:    "node",
		Address: "127.0.0.1",
		Port:    8889,
		Meta:    nil,
	})
	if err != nil {
		t.Errorf("start cluster error: %s", err.Error())
	}
	time.Sleep(time.Second * 1)
	if err := cluster1.SendToNode("node-2", []byte("hello world")); err != nil {
		t.Errorf("send to node error: %s", err.Error())
	}
	_ = cluster2
	time.Sleep(time.Second * 3)
}
