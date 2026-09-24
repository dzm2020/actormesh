package directory

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dzm2020/actormesh/logicalactor"

	"github.com/redis/go-redis/v9"
)

func redisAddress(t *testing.T) string {
	t.Helper()
	if address := strings.TrimSpace(os.Getenv("REDIS_ADDR")); address != "" {
		return address
	}
	return "127.0.0.1:6379"
}

func newIntegrationRedis(t *testing.T) (*redis.Client, string) {
	t.Helper()
	address := redisAddress(t)
	client := redis.NewClient(&redis.Options{Addr: address})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := client.Ping(ctx).Err()
	cancel()
	if err != nil {
		_ = client.Close()
		t.Skipf("Redis integration test requires Redis at %s: %v", address, err)
	}
	prefix := fmt.Sprintf("{directory-test-%d}.owner.", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if keys, err := client.Keys(ctx, prefix+"*").Result(); err == nil && len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
		cancel()
		_ = client.Close()
	})
	return client, prefix
}

func newDirectoryPair(t *testing.T, prefix string) (*RedisOwnerDirectory, *RedisOwnerDirectory, func()) {
	t.Helper()
	firstClient := redis.NewClient(&redis.Options{Addr: redisAddress(t)})
	secondClient := redis.NewClient(&redis.Options{Addr: redisAddress(t)})
	first, err := NewRedisDirectory(firstClient, RedisOwnerDirectoryOptions{KeyPrefix: prefix})
	if err != nil {
		t.Fatalf("create first directory: %v", err)
	}
	second, err := NewRedisDirectory(secondClient, RedisOwnerDirectoryOptions{KeyPrefix: prefix})
	if err != nil {
		_ = first.Close()
		t.Fatalf("create second directory: %v", err)
	}
	return first, second, func() {
		_ = first.Close()
		_ = second.Close()
		_ = firstClient.Close()
		_ = secondClient.Close()
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}

func TestRedisOwnerDirectoryLifecycle(t *testing.T) {
	client, prefix := newIntegrationRedis(t)
	first, second, closeDirectories := newDirectoryPair(t, prefix)
	defer closeDirectories()

	actorID := logicalactor.ActorID{Kind: "player", Key: fmt.Sprintf("%d", time.Now().UnixNano())}
	ownerA := logicalactor.NodeInfo{NodeId: "node-a", Kind: "player", InstanceId: "instance-a"}
	ownerB := logicalactor.NodeInfo{NodeId: "node-b", Kind: "player", InstanceId: "instance-b"}

	got, acquired, err := first.AcquireOwner(actorID, ownerA)
	if err != nil || !acquired || got != ownerA {
		t.Fatalf("initial acquire = owner=%+v acquired=%v err=%v", got, acquired, err)
	}
	got, acquired, err = second.AcquireOwner(actorID, ownerB)
	if err != nil || acquired || got != ownerA {
		t.Fatalf("competing acquire = owner=%+v acquired=%v err=%v", got, acquired, err)
	}
	got, found, err := second.GetOwner(actorID)
	if err != nil || !found || got != ownerA {
		t.Fatalf("cached get = owner=%+v found=%v err=%v", got, found, err)
	}
	deleted, err := first.DeleteOwner(actorID, ownerB)
	if err != nil || deleted {
		t.Fatalf("stale delete = deleted=%v err=%v", deleted, err)
	}
	deleted, err = first.DeleteOwner(actorID, ownerA)
	if err != nil || !deleted {
		t.Fatalf("current delete = deleted=%v err=%v", deleted, err)
	}
	waitFor(t, 2*time.Second, func() bool {
		_, found, getErr := second.GetOwner(actorID)
		return getErr == nil && !found
	})
	epoch, err := client.Get(context.Background(), genRedisKey(prefix, redisEpochKey, actorID.String())).Uint64()
	if err != nil || epoch != 2 {
		t.Fatalf("epoch after acquire/delete = %d err=%v, want 2", epoch, err)
	}
	got, acquired, err = second.AcquireOwner(actorID, ownerB)
	if err != nil || !acquired || got != ownerB {
		t.Fatalf("reacquire = owner=%+v acquired=%v err=%v", got, acquired, err)
	}
}

func TestRedisOwnerDirectoryRejectsInvalidStoredOwner(t *testing.T) {
	client, prefix := newIntegrationRedis(t)
	actorID := logicalactor.ActorID{Kind: "player", Key: fmt.Sprintf("invalid-%d", time.Now().UnixNano())}
	if err := client.Set(context.Background(), genRedisKey(prefix, redisOwnerKey, actorID.String()), `{"node_id":"","kind":"player","instance_id":""}`, 0).Err(); err != nil {
		t.Fatalf("seed invalid owner: %v", err)
	}
	if err := client.Set(context.Background(), genRedisKey(prefix, redisEpochKey, actorID.String()), 1, 0).Err(); err != nil {
		t.Fatalf("seed invalid epoch: %v", err)
	}
	directory, err := NewRedisDirectory(client, RedisOwnerDirectoryOptions{KeyPrefix: prefix})
	if err != nil {
		t.Fatalf("create directory: %v", err)
	}
	defer directory.Close()
	if _, found, err := directory.GetOwner(actorID); err == nil || found {
		t.Fatalf("invalid stored owner result = found=%v err=%v", found, err)
	}
}
