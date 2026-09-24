package consul

import (
	"errors"
	"sync"
)

var (
	ErrSubscriptionCenterClosed = errors.New("consul subscription center is closed")
	ErrSubscriberNil            = errors.New("consul subscriber is nil")
	ErrInvalidChangeType        = errors.New("consul cluster change type is invalid")
)

type Subscriber interface {
	OnClusterChange(members []ServiceInstance)
}

type SubscriberFunc func(members []ServiceInstance) error

func (f SubscriberFunc) OnClusterChange(members []ServiceInstance) error {
	if f == nil {
		return ErrSubscriberNil
	}
	return f(members)
}

type SubscriptionID uint64

type subscriptionEntry struct {
	id         SubscriptionID
	service    string
	subscriber Subscriber
}

type SubscriptionCenter struct {
	mu          sync.RWMutex
	nextID      SubscriptionID
	closed      bool
	subscribers map[SubscriptionID]subscriptionEntry
}

func NewSubscriptionCenter() *SubscriptionCenter {
	return &SubscriptionCenter{subscribers: make(map[SubscriptionID]subscriptionEntry)}
}

func (c *SubscriptionCenter) Subscribe(service string, subscriber Subscriber) (SubscriptionID, error) {
	if subscriber == nil {
		return 0, ErrSubscriberNil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, ErrSubscriptionCenterClosed
	}
	if c.subscribers == nil {
		c.subscribers = make(map[SubscriptionID]subscriptionEntry)
	}
	c.nextID++
	id := c.nextID
	c.subscribers[id] = subscriptionEntry{id: id, service: service, subscriber: subscriber}
	return id, nil
}

func (c *SubscriptionCenter) Unsubscribe(id SubscriptionID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.subscribers[id]; !ok {
		return false
	}
	delete(c.subscribers, id)
	return true
}

func (c *SubscriptionCenter) Notify(members []ServiceInstance) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, entry := range c.subscribers {
		entry.subscriber.OnClusterChange(members)
	}
}

func (c *SubscriptionCenter) SubscriberCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.subscribers)
}

func (c *SubscriptionCenter) Close() {
	c.mu.Lock()
	c.closed = true
	clear(c.subscribers)
	c.mu.Unlock()
}
