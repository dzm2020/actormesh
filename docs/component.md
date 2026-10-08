# Component 生命周期与管理

`pkg/component` 提供组件接口、生命周期状态机和按优先级及注册顺序管理组件的 Manager。

## 组件接口

```go
type IComponent interface {
	GetName() string
	SetName(string)
	Init() error
	Start() error
	Stop() error
	Status() LifecycleState
}
```

组件通常嵌入 `BaseComponent`，用 `GuardInit`、`GuardStart`、`GuardStop` 包住实际逻辑：

```go
type Cache struct { component.BaseComponent }

func NewCache() *Cache {
	c := &Cache{}
	c.SetName("cache")
	return c
}

func (c *Cache) Init() error { return c.GuardInit(func() error { return nil }) }
func (c *Cache) Start() error { return c.GuardStart(func() error { return nil }) }
func (c *Cache) Stop() error { return c.GuardStop(func() error { return nil }) }
```

## 状态和顺序

```text
new -> inited -> started -> stopped
```

- `Init` 只能从 `new` 调用一次。
- `Start` 只能从 `inited` 调用一次。
- `Stop` 只能调用一次，并要求至少尝试执行过 `Init`；允许从 `inited` 直接停止，也允许在 Init 或 Start 回调失败后执行清理。
- 重复调用返回 `ErrInitAlreadyCalled`、`ErrStartAlreadyCalled` 或 `ErrStopAlreadyCalled`。
- 顺序错误返回 `ErrInvalidOrder`。
- 回调失败时不会推进到下一状态，但会保留已调用标记，再次调用返回对应 AlreadyCalled 错误，不能重试。顺序校验失败则不会设置该标记。

`Controller` 串行执行生命周期回调。`LifecycleState` 是 `State` 的别名，状态常量同时提供 `StateNew` 等名称和 `LifecycleStateNew` 等兼容名称。

## Manager

```go
manager := component.NewComponentsMgr()
if err := manager.Add(first, second); err != nil { return err }
```

Manager 按名称索引组件，同时保存注册顺序。实现 `Priority() int` 的组件会按优先级排序，数值越小越早执行；相同优先级保持注册顺序。嵌入 `BaseComponent` 的组件默认优先级为 `0`，未实现该方法的组件也按 `0` 处理。`RangeInOrder` 和 `RangeInReverseOrder` 分别按优先级正序、逆序遍历；`Range` 不保证顺序。名称重复、nil 组件和删除未注册组件分别返回对应错误。

## 主要 API

Manager 只管理注册和遍历，不会自动调用生命周期。注册后应保持组件名称不变；Remove 按名称删除，Get 未找到时返回 nil。批量 Add 遇到错误立即返回，之前已添加的组件不会回滚。有序遍历回调返回 false 时提前停止。

| API | 说明 |
| --- | --- |
| `NewComponentsMgr()` | 创建 Manager |
| `(*Manager).Add(...)` | 注册组件 |
| `(*Manager).Remove(component)` | 删除组件 |
| `(*Manager).Get(name)` | 按名称查询 |
| `(*Manager).Count()` | 组件数量 |
| `(*Manager).Range(...)` | 无序遍历 |
| `(*Manager).RangeInOrder(...)` | 按优先级升序遍历，同优先级按注册顺序 |
| `(*Manager).RangeInReverseOrder(...)` | 将上述顺序反转后遍历 |
| `PriorityComponent` | 可选的组件排序能力，`Priority()` 越小越早执行 |
| `(*BaseComponent).GuardInit/GuardStart/GuardStop` | 执行生命周期状态保护 |
