package component

import (
	"fmt"
	"sync"

	"github.com/duke-git/lancet/v2/maputil"
)

var (
	ErrComponentCannotBeNil       = fmt.Errorf("组件不能为空")
	ErrComponentTypeCannotBeNil   = fmt.Errorf("组件类型不能为空")
	ErrComponentAlreadyRegistered = fmt.Errorf("组件已注册")
	ErrComponentNotRegistered     = fmt.Errorf("组件未注册")
)

type IManager interface {
	Count() int
	Get(name string) IComponent
	Add(component ...IComponent) error
	Remove(c IComponent) error
	RangeInOrder(fn func(component IComponent) bool)
	RangeInReverseOrder(fn func(component IComponent) bool)
	Range(fn func(component IComponent))
}

var _ IManager = (*Manager)(nil)

// NewComponentsMgr 创建新的生命周期管理器
func NewComponentsMgr() *Manager {
	return &Manager{
		components: maputil.NewConcurrentMap[string, IComponent](10),
		order:      make([]string, 0),
	}
}

type Manager struct {
	components *maputil.ConcurrentMap[string, IComponent]
	order      []string // 保存组件注册顺序
	orderMu    sync.RWMutex
}

func (cm *Manager) Count() int {
	cm.orderMu.RLock()
	defer cm.orderMu.RUnlock()
	return len(cm.order)
}

func (cm *Manager) Get(name string) IComponent {
	component, _ := cm.components.Get(name)
	return component
}

func (cm *Manager) Add(component ...IComponent) error {
	cm.orderMu.Lock()
	defer cm.orderMu.Unlock()

	for _, c := range component {
		if c == nil {
			return ErrComponentCannotBeNil
		}
		name := c.GetName()
		// 检查是否已注册同类型组件
		if _, exists := cm.components.Get(name); exists {
			return ErrComponentAlreadyRegistered
		}

		// 注册组件
		cm.components.Set(name, c)
		cm.order = append(cm.order, name)
	}
	return nil
}

func (cm *Manager) Remove(t IComponent) error {
	if t == nil {
		return ErrComponentTypeCannotBeNil
	}

	cm.orderMu.Lock()
	defer cm.orderMu.Unlock()

	if _, exists := cm.components.Get(t.GetName()); !exists {
		return ErrComponentNotRegistered
	}

	cm.components.Delete(t.GetName())
	for idx, name := range cm.order {
		if name == t.GetName() {
			cm.order = append(cm.order[:idx], cm.order[idx+1:]...)
			break
		}
	}
	return nil
}

func (cm *Manager) Range(fn func(component IComponent)) {
	cm.components.Range(func(key string, value IComponent) bool {
		fn(value)
		return true
	})
}

// RangeInOrder 按组件注册顺序遍历。
func (cm *Manager) RangeInOrder(fn func(component IComponent) bool) {
	if fn == nil {
		return
	}
	cm.orderMu.RLock()
	order := make([]string, len(cm.order))
	copy(order, cm.order)
	cm.orderMu.RUnlock()

	for _, name := range order {
		component, ok := cm.components.Get(name)
		if !ok {
			continue
		}
		if !fn(component) {
			return
		}
	}
}

// RangeInReverseOrder 按组件注册逆序遍历。
func (cm *Manager) RangeInReverseOrder(fn func(component IComponent) bool) {
	if fn == nil {
		return
	}
	cm.orderMu.RLock()
	order := make([]string, len(cm.order))
	copy(order, cm.order)
	cm.orderMu.RUnlock()

	for i := len(order) - 1; i >= 0; i-- {
		component, ok := cm.components.Get(order[i])
		if !ok {
			continue
		}
		if !fn(component) {
			return
		}
	}
}
