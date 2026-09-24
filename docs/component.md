# Component 生命周期与管理

`framework/pkg/component` 提供组件接口、生命周期状态机和按注册顺序管理组件的 Manager。

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
- `Stop` 只能调用一次，并要求至少调用过 `Init`；当前实现允许从 `inited` 直接停止。
- 重复调用返回 `ErrInitAlreadyCalled`、`ErrStartAlreadyCalled` 或 `ErrStopAlreadyCalled`。
- 顺序错误返回 `ErrInvalidOrder`。
- 回调失败时不会推进到下一状态。

## Manager

```go
manager := component.NewComponentsMgr()
if err := manager.Add(first, second); err != nil { return err }
```

Manager 按名称索引组件，同时保存注册顺序。`RangeInOrder` 和 `RangeInReverseOrder` 分别按正序、逆序遍历；`Range` 不保证顺序。名称重复、nil 组件和删除未注册组件分别返回对应错误。

## 主要 API

| API | 说明 |
| --- | --- |
| `NewComponentsMgr()` | 创建 Manager |
| `(*Manager).Add(...)` | 注册组件 |
| `(*Manager).Remove(component)` | 删除组件 |
| `(*Manager).Get(name)` | 按名称查询 |
| `(*Manager).Count()` | 组件数量 |
| `(*Manager).Range(...)` | 无序遍历 |
| `(*Manager).RangeInOrder(...)` | 按注册顺序遍历 |
| `(*Manager).RangeInReverseOrder(...)` | 按逆序遍历 |
| `(*BaseComponent).GuardInit/GuardStart/GuardStop` | 执行生命周期状态保护 |
