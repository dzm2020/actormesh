# Node 节点运行手册

`github.com/dzm2020/actormesh/node` 负责组装日志、Cluster、Actor System、逻辑 Actor Router 和业务 Behavior，并驱动启动与关闭。

## 启动与关闭

```text
node.New（设置 StartedAt）
  -> Node.Start
     -> bootstrapNode（校验配置、创建日志/Cluster/System/Router）
     -> 注册 Options.Components、System、Cluster、Router
     -> Behavior.OnInit
     -> 各组件 Init（优先级顺序）
     -> Behavior.OnStart
     -> 各组件 Start（优先级顺序）
     -> 返回，节点进入运行阶段
  -> Node.Shutdown
     -> Cluster.Leave
     -> 各组件 Stop（优先级逆序）
     -> Behavior.OnStop
```

`Start()` 启动成功后立即返回。根包的 `actormesh.Run(ctx, nodes...)` 按参数顺序启动节点，然后等待 Context 取消或 `SIGQUIT`、`SIGTERM`、`SIGINT`，再按参数顺序调用各节点的 `Shutdown()`。

`Start` 和 `Run` 遇到启动错误会直接返回，不会自动回滚已启动的组件或节点。业务需要自行处理已初始化资源的清理。`Shutdown` 先注销 Cluster，再尝试停止所有注册组件，最后把关闭错误传给 `OnStop`；当前组件停止循环仅保留最后一次遍历的错误结果，不能保证收集所有组件错误。启动和关闭应由调用方串行控制。

## Options

```go
options := node.Options{
	NodeInfo: member.NodeInfo{
		ID: "game-1", Name: "game", Address: "127.0.0.1:9100",
	},
	Behavior: Behavior{directory: ownerDirectory},
}
n := node.New(options)
err := actormesh.Run(ctx, n)
```

示例中的 `ctx` 和 `ownerDirectory` 由业务创建；`Behavior` 见下文。导入路径分别为 `github.com/dzm2020/actormesh`、`github.com/dzm2020/actormesh/node` 和 `github.com/dzm2020/actormesh/cluster/member`。

`Options` 嵌入 `member.NodeInfo`，要求 `ID`、`Name` 非空；`Address` 包含集群监听端口。`Behavior == nil` 时使用 `DefaultNodeBehavior`。可通过 `Logger`、`PanicHook` 配置日志，通过 `System`、`Cluster`、`ActorRouter` 注入实现，通过 `Components` 注册扩展组件。

未注入 `System`、`Cluster`、`ActorRouter` 时，Node 会分别创建默认实现。默认 Router 没有 Owner Directory，必须在 Router 启动前调用 `SetDirectory`，否则节点启动返回 `ErrOwnerDirectoryNil`。

## Behavior 和业务组件

```go
type NodeBehavior interface {
	OnInit(node NodeAPI) error
	OnStart(node NodeAPI) error
	OnStop(node NodeAPI, reason error)
}
```

`OnInit` 在组件初始化前调用，适合注册业务组件和注入 Directory；`OnStart` 在组件启动前调用，适合注册 Actor Factory/Placement；`OnStop` 在组件停止后调用。

```go
type Behavior struct {
	node.DefaultNodeBehavior
	directory logicalactor.OwnerDirectory
}

func (b Behavior) OnInit(n node.NodeAPI) error {
	n.GetActorRouter().SetDirectory(b.directory)
	return nil
}

func (b Behavior) OnStart(n node.NodeAPI) error {
	return n.GetActorRouter().RegisterFactory("chat", factory)
}
```

`factory` 是业务提供的 `logicalactor.ActorFactory`。业务组件通过 `AddComponent` 或 `Options.Components` 注册。`AddComponent` 只允许在节点的 new 阶段调用，成功启动或关闭后返回 `ErrNodeInvalidPhase`；建议在 `OnInit` 注册，以参与完整初始化流程。

组件按 `Priority() int` 从小到大初始化和启动，相同优先级保持注册顺序；停止时逆序执行。核心组件注册时，`Options.Components` 在前，随后是 System、Cluster、Router，最终执行顺序仍由优先级决定。

## NodeAPI

```go
type NodeAPI interface {
	Start() error
	Info() member.NodeInfo
	AddComponent(...component.IComponent) error
	GetComponent(name string) component.IComponent
	GetCluster() cluster.ClusterAPI
	GetSystem() actor.SystemAPI
	GetActorRouter() logicalactor.ActorRouterAPI
	Logger() *glog.Logger
	Shutdown() error
}
```

`Info()` 返回节点信息；`node.New` 将 `StartedAt` 设置为创建时的 Unix 毫秒时间。逻辑 Actor 的节点适配器使用 `ID` 作为 `NodeId`、`Name` 作为 Kind、`StartedAt` 的十进制字符串作为 `InstanceId`。当前没有独立的 `GetInstanceID` 或 `node_instance_id` 元数据注入。

具体 `*Node` 还提供 `GetOptions() *Options`；它不属于 `NodeAPI`，返回内部配置指针。
