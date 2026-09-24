# Node 节点运行手册

`framework/node` 负责组装日志、Cluster、Actor System、逻辑 Actor Router 和业务 Behavior，并统一驱动启动与关闭。

## 启动流程

```text
node.New
  -> bootstrapNode（校验配置、创建 Cluster/System/Router）
  -> 注册核心组件
  -> Behavior.OnInit
  -> 各组件 Init（注册顺序）
  -> Behavior.OnStart
  -> 各组件 Start（注册顺序）
  -> 等待退出信号
  -> Cluster.Leave
  -> 组件 Stop（注册逆序）
```

`Node.Startup()` 会阻塞等待 `SIGQUIT`、`SIGTERM` 或 `SIGINT`。启动失败也会进入关闭流程：已注册组件按逆序停止，Cluster 先执行 `Leave`，最后把聚合后的关闭错误传给 `Behavior.OnStop`。

## Options

```go
options := node.Options{
	ID:             "game-1",
	Kind:           "game",
	ClusterAddress: "127.0.0.1:9100",
	Behavior:       node.DefaultNodeBehavior{},
}
n := node.New(options)
```

`ID`、`Kind` 和 `Behavior` 必须有效。未注入 `System`、`Cluster` 时，Node 会创建默认实现；只有配置了 `LogicalActorDirectory` 时才会自动创建逻辑 Actor Router。

## Behavior 和业务组件

```go
type NodeBehavior interface {
	OnInit(node NodeAPI) error
	OnStart(node NodeAPI) error
	OnStop(node NodeAPI, reason error)
}
```

`OnInit` 在组件初始化前调用，适合注册业务组件；`OnStart` 在组件启动前调用，适合注册 Actor Factory/Placement；`OnStop` 在组件停止后由 Node 调用。

```go
func (b Behavior) OnStart(n node.NodeAPI) error {
	return n.GetActorRouter().RegisterFactory("chat", factory)
}
```

业务组件必须在 Node 进入运行阶段前通过 `AddComponent` 注册；进入运行阶段后再次添加会返回 `ErrNodeInvalidPhase`。Node 会按注册顺序初始化、启动，按逆序停止。

## NodeAPI

```go
type NodeAPI interface {
	Startup() error
	GetID() string
	GetKind() string
	GetInstanceID() string
	GetClusterAddress() string
	AddComponent(...component.IComponent) error
	GetComponent(name string) component.IComponent
	GetCluster() cluster.ClusterAPI
	GetSystem() actor.SystemAPI
	GetActorRouter() logicalactor.ActorRouter
}
```

`GetInstanceID` 在节点创建时生成，并写入 Cluster 服务实例的 `node_instance_id` 元数据。
