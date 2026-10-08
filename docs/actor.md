# Actor 运行时

`github.com/dzm2020/actormesh/actor` 提供基于邮箱的 Actor 运行时。每个 Actor 由一个 `Process` 承载，同一 Actor 的消息串行处理；不同 Actor 可以并发执行。

## 生命周期

```text
System.Init -> System.Start -> SpawnActor -> Actor.Init
                                             |
                              Tell -> HandleTell
                              Ask  -> HandleAsk
                                             |
                              Stop -> Actor.Destroy
```

组件生命周期由 `pkg/component` 管理，必须先 `Init`、再 `Start`。Actor 在 `SpawnActor` 时由系统投递初始化消息；方法返回不表示 `Actor.Init` 已完成。

## 实现 Actor

`Init`、`HandleTell`、`Destroy` 和 `Panic` 不返回错误，`HandleAsk` 返回 `(any, error)`；需要向 Ask 调用方返回错误时使用 `ctx.RespondError`，或在 `HandleAsk` 中返回错误。

```go
type EchoActor struct { actor.DefaultActor }

func (*EchoActor) HandleAsk(ctx actor.Context, message any) (any, error) {
	text, ok := message.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", message)
	}
	return "echo: " + text, nil
}
```

`HandleAsk` 返回 `(nil, nil)` 表示不自动回复；若始终没有响应，请求最终超时。成功调用 `Forward` 会标记当前消息已响应，之后再转发或回复会返回 `ErrAlreadyResponded`。同步 `Ask` 阻塞当前 Actor，等待链应避免依赖当前 Actor 继续处理消息。

`Init`、`Destroy`、`Panic` 等不需要实现的回调可以通过嵌入 `actor.DefaultActor` 获得空实现。

## 创建和发送消息

```go
system := actor.NewSystem("node-1", nil)
_ = system.Init()
_ = system.Start()

pid, err := system.SpawnActor(&EchoActor{}, actor.SpawnOptions{Name: "echo"})
if err != nil { return err }

result, err := system.Ask(actor.NoSender, pid, "hello", 0)
if err != nil { return err }
fmt.Println(result)
```

- `Tell` 异步投递消息，目标执行 `HandleTell`。
- `Ask` 等待 `HandleAsk` 的响应；超时参数小于等于 0 时使用默认 3 秒。
- Actor 内可通过 `ctx.Respond` 或 `ctx.RespondError` 主动响应，但同一请求只能响应一次。
- `Forward` 保留原始 Sender 和请求元数据，适合路由 Actor 转发请求。

`AskAsync` 不会阻塞当前 Actor；完成回调会再次投递到发起方 Actor 的邮箱，签名为 `func(ctx Context, value any, requestErr error)`。回调中应检查 `requestErr`，并注意发起方 Actor 停止或邮箱已满时结果可能无法继续处理。发起方应是可接收任务的本地 Actor。请求管理器默认最多保留 65536 个待完成请求，达到上限返回 `ErrRequestLimit`。

## Context 和初始化参数

`actor.Context` 提供当前 PID、Sender、消息、响应、定时任务、日志和发送能力。创建 Actor 时传入的 `SpawnOptions.InitArgs` 可通过 `ctx.InitArgs()` 读取；返回的是参数副本。

`Ticker` 和 `AfterFunc` 投递的任务也进入当前 Actor 邮箱，因此与普通消息共享串行顺序。两者返回 `int64` 定时器 ID，使用 `StopTimer` 取消任务；Actor 销毁时清理定时器。定时任务通过系统消息投递，不受用户消息容量限制。

常用 Context 方法：

| 方法 | 用途 |
| --- | --- |
| `Self` / `Sender` | 当前 Actor 与消息发送者 PID |
| `System` / `Actor` | 所属系统和当前 Actor 实例 |
| `Message` / `MessageMeta` | 当前消息及请求元数据 |
| `Tell` / `Forward` / `Ask` / `AskAsync` | 发送、转发和请求 |
| `Respond` / `RespondError` | 回复当前 Ask |
| `Ticker` / `AfterFunc` / `StopTimer` | 管理投递到邮箱的定时任务 |
| `SetAskTimeout` | 设置当前 Actor 后续 Ask 的默认超时 |
| `Logger` / `Stop` | 获取日志器或停止当前 Actor |

## PID、邮箱和停止

```go
type SpawnOptions struct {
	Name        string
	InitArgs    []any
	MailboxSize int32
}
```

- `MailboxSize <= 0` 使用默认容量 1024。
- 非空 Actor 名称在同一 Actor System 内必须唯一。
- 邮箱满时 `Tell` 或 `Push` 返回 `ErrMailboxFull`。
- `StopProcess` 将进程标记为关闭并投递停止控制消息，随后立即返回；关闭后的新投递返回 `ErrActorProcessStopped`；Actor 最终执行 `Destroy` 并从本地索引移除。
- `System.Stop` 停止接收新 Actor，关闭请求管理器并等待 Actor 退出，等待上限为 10 秒，超时返回错误。

`PID` 包含数值 `ActorID`、名称和 `NodeID`。`NewPID` 可构造本地或远程目标；`SystemAPI.Has` 只检查本地进程。本地查找先按 ActorID，再按 ActorName。`StopProcess` 对远程或不存在的 PID 静默返回，远程停止应由业务协议自行实现。

## 跨节点消息

系统通过 `RemoteSender` 发送目标 PID 不属于本节点的消息：

```go
type RemoteSender interface {
	SendToNode(nodeID string, data []byte) error
	Broadcast(data []byte) error
}
```

远程消息使用框架的 Protobuf 编码器携带类型名和二进制 Payload；Payload 必须实现 `proto.Message`，接收节点必须链接相应 Protobuf 类型，普通 Go 结构体不能作为跨节点 Payload。目标节点通过 `System.OnMessage(sourceNodeID, data)` 接收数据，Cluster 的消息回调通常直接调用该方法。远程 Ask 的请求引用包含请求 ID 和源节点，响应或错误会回到请求管理器。

## 主要 API

| API | 说明 |
| --- | --- |
| `NewSystem(nodeID string, sender RemoteSender) *System` | 创建 Actor System |
| `NewSystemWithOptions(options SystemOptions) *System` | 通过 NodeID、RemoteSender 和 Logger 配置系统 |
| `(*System).SpawnActor(handler, options)` | 创建 Actor |
| `(*System).Tell(from, target, message)` | 异步投递 |
| `(*System).Ask(from, target, message, timeout)` | 同步请求 |
| `(*System).AskAsync(...)` | 异步请求并回调 |
| `(*System).OnMessage(sourceNodeID, data)` | 接收并处理 Cluster 转发的远程 Actor 消息 |
| `(*System).StopProcess(from, target)` | 请求停止 Actor |
| `(*System).Stop()` | 停止整个 System |
| `NewPID(actorID, actorName, nodeID)` | 创建 PID |
| `DefaultActor` | 生命周期回调空实现 |

常见错误包括 `ErrHandlerNil`、`ErrNameExists`、`ErrNotLocal`、`ErrNotFound`、`ErrMailboxFull`、`ErrAskTimeout`、`ErrAlreadyResponded`、`ErrRequestManagerClosed` 和远程消息校验错误。需要稳定错误码时使用 `actor.NewError`，用 `actor.CodeOf` 读取错误码。
