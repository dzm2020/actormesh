package directory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"game-server/framework/pkg/serialize/jsoncodec"
	"strings"
	"sync"

	"game-server/framework/logicalactor"
	"game-server/framework/pkg/glog"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	ownerUpdate = "ownerUpdate"
)

var _ logicalactor.OwnerDirectory = (*RedisOwnerDirectory)(nil)

var ErrRedisClientNil = errors.New("redis owner directory client is nil")

func NewRedisDirectory(client redis.UniversalClient, options RedisOwnerDirectoryOptions) (*RedisOwnerDirectory, error) {
	if client == nil {
		return nil, ErrRedisClientNil
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &RedisOwnerDirectory{
		client: client,
		prefix: strings.TrimSpace(options.KeyPrefix),
		cache:  make(map[string]Owner),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	m.pubsub = client.Subscribe(ctx, m.updateChannel())
	go m.consumeOwnerEvents(ctx)
	return m, nil
}

type RedisOwnerDirectory struct {
	client   redis.UniversalClient
	prefix   string
	cacheMu  sync.RWMutex
	cache    map[string]Owner
	pubsub   *redis.PubSub
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

func (m *RedisOwnerDirectory) updateChannel() string {
	return m.prefix + ownerUpdate
}

func (m *RedisOwnerDirectory) consumeOwnerEvents(ctx context.Context) {
	defer close(m.done)
	for {
		message, err := m.pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			glog.Warn("redis owner directory subscription stopped", zap.Error(err))
			continue
		}
		m.handleOwnerEvent(message)
	}
}

func (m *RedisOwnerDirectory) handleOwnerEvent(message *redis.Message) {
	if message == nil {
		return
	}
	var event ownerEvent
	if err := jsoncodec.Unmarshal([]byte(message.Payload), &event); err != nil {
		glog.Error("redis owner directory event decode failed", zap.Error(err))
		return
	}

	switch message.Channel {
	case m.updateChannel():
		m.tryDelCached(event.ActorId.String(), event.Epoch)
	}

}
func (m *RedisOwnerDirectory) tryDelCached(key string, epoch uint64) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	if current, found := m.cache[key]; found {
		if current.Epoch > epoch {
			return
		}
	}
	delete(m.cache, key)
}

func (m *RedisOwnerDirectory) getCached(key string) (Owner, bool) {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()
	owner, found := m.cache[key]
	return owner, found
}

func (m *RedisOwnerDirectory) trySetCached(key string, owner Owner) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	if current, found := m.cache[key]; found {
		if current.Epoch >= owner.Epoch {
			return
		}
	}
	m.cache[key] = owner
	return
}

func (m *RedisOwnerDirectory) AcquireOwner(actorId logicalactor.ActorID, candidate logicalactor.NodeInfo) (logicalactor.NodeInfo, bool, error) {
	if err := actorId.Validate(); err != nil {
		return logicalactor.NodeInfo{}, false, err
	}
	if err := candidate.Validate(); err != nil {
		return logicalactor.NodeInfo{}, false, err
	}
	owner, acquire, err := acquireOwnerFromRedis(m.client, m.prefix, actorId, candidate)
	if err != nil {
		return logicalactor.NodeInfo{}, false, err
	}
	m.trySetCached(actorId.String(), *owner)
	if acquire {
		m.notifyOwnerUpdate(actorId, owner.Epoch)
	}
	return owner.Node, acquire, nil
}

func (m *RedisOwnerDirectory) GetOwner(actorId logicalactor.ActorID) (logicalactor.NodeInfo, bool, error) {
	if err := actorId.Validate(); err != nil {
		return logicalactor.NodeInfo{}, false, err
	}
	owner, found := m.getCached(actorId.String())
	if found {
		return owner.Node, true, nil
	}
	ownerPtr, found, err := getOwnerFromRedis(m.client, m.prefix, actorId)
	if err != nil {
		return logicalactor.NodeInfo{}, false, fmt.Errorf("get owner %s: %w", actorId.String(), err)
	}
	if !found {
		return logicalactor.NodeInfo{}, false, nil
	}
	m.trySetCached(actorId.String(), *ownerPtr)
	return ownerPtr.Node, true, nil
}

func (m *RedisOwnerDirectory) DeleteOwner(actorId logicalactor.ActorID, expectedOwner logicalactor.NodeInfo) (bool, error) {
	if err := actorId.Validate(); err != nil {
		return false, err
	}
	if err := expectedOwner.Validate(); err != nil {
		return false, err
	}

	deleted, epoch, err := deleteOwnerFromRedis(m.client, m.prefix, actorId, expectedOwner)
	if err != nil {
		return false, fmt.Errorf("exec delete %s: %w", actorId.String(), err)
	}
	if deleted {
		m.tryDelCached(actorId.String(), epoch)
		m.notifyOwnerUpdate(actorId, epoch)
	}
	return deleted, nil
}

func (m *RedisOwnerDirectory) notifyOwnerUpdate(actorId logicalactor.ActorID, epoch uint64) {
	event := &ownerEvent{
		ActorId: actorId,
		Epoch:   epoch,
	}
	jsonBytes, err := json.Marshal(&event)
	if err != nil {
		glog.Error("redis owner directory event encode failed", zap.Error(err))
		return
	}
	if err = m.client.Publish(context.Background(), m.updateChannel(), jsonBytes).Err(); err != nil {
		glog.Error("redis owner update notify event", zap.Error(err))
		return
	}
}

func (m *RedisOwnerDirectory) Close() error {
	var err error
	m.stopOnce.Do(func() {
		if m.cancel == nil {
			return
		}
		m.cancel()
		closeErr := m.pubsub.Close()
		<-m.done
		if closeErr != nil {
			err = fmt.Errorf("close redis pubsub: %w", closeErr)
		}
	})
	return err
}
