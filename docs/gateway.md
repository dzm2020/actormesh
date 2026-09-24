# Gateway 业务接入手册

`framework/gateway` 将网络连接包装为 Actor，并把客户端协议帧路由到业务处理器。业务侧主要完成三件事：定义 Protobuf 消息、注册 C2S/S2C 路由、在 Handler 中响应或转发到业务 Actor。

## 请求链路

```text
network.Connection -> Agent Actor -> MessageFrame
                                      |
                              Route.Handle
                                      |
                              Protobuf request
                                      |
                         Respond / RespondError / Forward
```

## 注册 C2S/S2C 路由

Gateway 使用 `google.golang.org/protobuf/proto.Message`。应传入项目生成的 Protobuf 类型指针：

```go
route := gateway.NewRoute()
	route.Register(
	1, 1,
	func(ctx actor.Context, request any) error {
		return ctx.Respond(&pb.TestResponse{Content: []byte("ok")})
	},
	&pb.TestRequest{},
	&pb.TestResponse{},
)
```

`Register(cmd, act, handler, c2s, s2c)`：

- `cmd`、`act`：客户端协议帧中的命令和动作。
- `handler`：收到 C2S 消息后的业务处理器。
- `c2s`：用于反射创建并解码请求的 Protobuf 指针类型。
- `s2c`：该命令对应的下行 Protobuf 类型。

只注册 C2S 时可以将 `s2c` 传 `nil`；只注册 S2C 时 `handler` 和 `c2s` 可为 nil。重复注册同一个 C2S 命令或 S2C 类型时，后续注册会被忽略并记录警告。

## 创建 Agent 和 Gateway

```go
spawner := func(connection network.Connection) (*actor.PID, error) {
	agent := gateway.NewAgent(connection, route)
	return system.SpawnActor(agent, actor.SpawnOptions{})
}

server := network.NewTCPServer(network.DefaultTCPConfig("127.0.0.1:9000"))
gatewayComponent := gateway.New(server, system, spawner)

if err := gatewayComponent.Init(); err != nil { return err }
if err := gatewayComponent.Start(); err != nil { return err }
```

`gateway.New` 的 `server`、`system` 和 `spawner` 都应有效。`Gateway.Init` 会检查 Actor System 和 AgentSpawner；当前实现没有额外检查 `server` 是否为 nil，接入代码应自行保证。

`Agent` 同时实现 `network.Connection` 和 `actor.Actor`：

- 收到 `*protocol.MessageFrame` 时尝试执行 C2S 路由。
- 收到已注册的 Protobuf 消息时编码为 S2C 帧并发送。
- 未被 Route 消费的消息交给 `DefaultActor` 处理。
- `Agent.Destroy` 会关闭底层网络连接。

## 响应客户端

路由 Handler 的参数类型是 `actor.Context`，实际运行时是 Gateway 内部的请求上下文：

```go
	route.Register(1, 1,
	func(ctx actor.Context, request any) error {
		return ctx.Respond(&pb.TestResponse{Content: []byte("success")})
	},
	&pb.TestRequest{},
	&pb.TestResponse{},
)
```

`Respond` 会复用请求帧的 Cmd、Act 和 Index，清除 Flags，并将 Protobuf Body 写回客户端。

错误响应：

```go
	route.Register(1, 2,
	func(ctx actor.Context, request any) error {
		return ctx.RespondError(actor.NewError(
			actor.ErrorCodeActorNotFound, "player not found",
		))
	},
	&pb.TestRequest{}, nil,
)
```

包含 `actor.ErrorCode` 的错误会映射到协议帧的 `Error` 字段；无法映射时使用 `ClientErrorInternal`。同一请求只能响应一次，重复调用返回 `ErrClientAlreadyResponded`。

## 转发到业务 Actor

Handler 可以使用 `Forward` 把请求交给业务 Actor，并自动把结果写回客户端：

```go
	route.Register(2, 1,
	func(ctx actor.Context, request any) error {
		return ctx.Forward(playerPID, &pb.TestRequest{Content: []byte("load")})
	},
	&pb.TestRequest{}, &pb.TestResponse{},
)
```

处理过程：

1. 客户端请求进入 Pending 状态。
2. Gateway 使用 Actor `AskAsync` 调用目标 Actor。
3. 业务 Actor 成功返回时自动调用客户端 `Respond`。
4. 业务 Actor 返回错误或请求超时时自动调用客户端 `RespondError`。

业务 Actor 示例：

```go
type PlayerActor struct { actor.DefaultActor }

func (*PlayerActor) HandleAsk(_ actor.Context, message any) (any, error) {
	request, ok := message.(*pb.TestRequest)
	if !ok { return nil, fmt.Errorf("unexpected message %T", message) }
	return &pb.TestResponse{Content: request.Content}, nil
}
```

## S2C 主动推送

`Agent` Actor 收到已注册的 S2C Protobuf 消息后，会通过 `ResolveOutbound` 找到对应 Cmd/Act 并发送：

```go
err := system.Tell(businessPID, agentPID,
	&pb.TestResponse{Content: []byte("push")})
```

前提是该消息类型已经作为 `s2c` 参数注册。未注册的 Protobuf 消息不会自动发送。

## API 参考

| API | 说明 |
| --- | --- |
| `NewRoute() *Route` | 创建空路由表 |
| `(*Route).Register(cmd, act uint8, handler RouteHandler, c2s, s2c proto.Message)` | 注册 C2S Handler 和 S2C 类型 |
| `(*Route).Handle(agent ClientAgent, ctx actor.Context, message *protocol.MessageFrame) error` | 解码请求并执行 Handler |
| `(*Route).IsRegisteredInbound(message *protocol.MessageFrame) bool` | 判断 C2S 命令是否已注册 |
| `(*Route).ResolveOutbound(message proto.Message) (uint8, uint8, bool)` | 查找 S2C 类型对应的 Cmd/Act |
| `NewAgent(connection network.Connection, route ClientRoute) *Agent` | 创建客户端 Agent |
| `(*Agent).HandleTell(ctx actor.Context, message any)` | 处理上下行消息 |
| `New(server network.Server, system actor.SystemAPI, spawner AgentSpawner) *Gateway` | 创建 Gateway 组件 |
| `(*Gateway).Init() error` | 校验 Gateway 依赖 |
| `(*Gateway).Start() error` | 启动网络服务 |
| `ClientRoute` | C2S/S2C 路由能力接口 |
| `ClientAgent` | 同时具备 Connection 和 Actor 能力的客户端 Agent |
| `AgentSpawner` | 根据连接创建 Agent PID 的函数类型 |
| `RouteHandler` | C2S 业务处理函数类型 |
| `ClientErrorNone` | 客户端成功错误码 |
| `ClientErrorInternal` | 客户端内部错误码 |
| `ErrRouteNotFound` | C2S 路由不存在 |
| `ErrClientAgentNotFound` | 连接未关联 Agent |
| `ErrClientAlreadyResponded` | 客户端请求已响应 |
| `ErrSystemNil` | Gateway Actor System 为空 |
| `ErrAgentSpawnerNil` | AgentSpawner 为空 |

`Gateway.Stop` 会先停止接收新连接，再停止已登记的 Agent，并最多等待 10 秒；它不会替 Actor System 停止其他业务 Actor。`Gateway.Start` 在后台运行 `network.Server.Run`，因此 `Start` 返回只表示服务启动任务已提交。

## 业务接入检查清单

- [ ] C2S 和 S2C 类型使用生成的 `proto.Message` 指针。
- [ ] 业务命令不使用 `cmd=0`。
- [ ] 每个 C2S 路由都有非 nil Handler 和请求类型。
- [ ] 需要主动推送的 S2C 类型已注册。
- [ ] Agent PID 指向对应的 Gateway Agent。
- [ ] Handler 只响应一次；异步业务使用 `Forward`。
- [ ] 业务错误使用 `actor.NewError` 创建稳定错误码。

## 当前源码注意事项

路由 Handler 的 Context 接口没有导出请求对象访问方法；当前内部 `agentContext` 保存了解码后的请求，但未提供公开的 `Request()` API。接入层应沿用现有项目约定，不在业务文档中虚构该方法。
