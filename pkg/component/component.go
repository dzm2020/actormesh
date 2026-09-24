package component

// IComponent 组件提供的生命周期函数要保证幂等性和顺序执行
type IComponent interface {
	GetName() string
	SetName(string)
	Init() error
	Start() error
	Stop() error
	Status() LifecycleState
}

var _ IComponent = (*BaseComponent)(nil)

type BaseComponent struct {
	lifecycle Controller
	name      string
}

func (c *BaseComponent) GetName() string {
	return c.name
}

func (c *BaseComponent) SetName(s string) {
	c.name = s
}

func (c *BaseComponent) Init() error {
	return c.lifecycle.Init(nil)
}

func (c *BaseComponent) Start() error {
	return c.lifecycle.Start(nil)
}

func (c *BaseComponent) Stop() error {
	return c.lifecycle.Stop(nil)
}

func (c *BaseComponent) GuardInit(run func() error) error {
	return c.lifecycle.Init(run)
}

func (c *BaseComponent) GuardStart(run func() error) error {
	return c.lifecycle.Start(run)
}

func (c *BaseComponent) GuardStop(run func() error) error {
	return c.lifecycle.Stop(run)
}

func (c *BaseComponent) Status() LifecycleState {
	if c == nil {
		return LifecycleStateNew
	}
	return c.lifecycle.State()
}
