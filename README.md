# ActorMesh

ActorMesh 是一个面向实时服务端的 Go 运行时：用 Actor 模型组织业务并发，用统一网络层承载客户端连接，用 Gateway 处理协议路由，再通过 Cluster 和 Logical Actor 把单机服务自然扩展到多节点。

它适合游戏服务器、实时互动、在线协作、社交服务以及其他需要大量长连接、状态化业务和水平扩展能力的场景。

## 为什么选择 ActorMesh

- **业务状态天然隔离**：一个 Actor 对应一个串行邮箱，同一 Actor 内的状态更新不需要到处加锁；不同 Actor 之间仍可并发执行。
- **从单机到集群平滑演进**：本地 `Tell`、`Ask`、`Forward` 与远程 Actor 消息使用同一套抽象，业务代码不必为“本地调用”和“跨节点调用”维护两套模型。
- **网络接入开箱即用**：统一的消息帧、编解码、心跳、握手、发送队列和连接生命周期，覆盖 TCP、UDP、WebSocket 三类常见接入方式。
- **协议与业务解耦**：Gateway 将网络连接、Protobuf 请求/响应和业务 Actor 连接起来，C2S 路由、S2C 响应以及主动推送都有清晰边界。
- **支持稳定的逻辑身份**：Logical Actor 让业务使用 `player:1001` 这类逻辑 ID 寻址，不需要关心实例当前在哪台节点；Owner Directory 和 Placement Strategy 负责归属与调度。
- **组件化生命周期**：Node、Actor System、Gateway、Cluster 等组件统一经过 `Init -> Start -> Stop` 管理，启动顺序和优雅关闭更容易控制。
- **可替换的基础设施**：Cluster 的成员管理、节点传输和 Logical Actor 的 Owner Directory 都通过接口接入，便于替换注册中心、传输层或存储实现。
- **面向生产的工程细节**：内置超时、邮箱容量限制、背压错误、连接状态、错误码、日志、定时任务和安全关闭路径，减少业务层重复造轮子。

## 核心能力

| 模块 | 能力 | 适合解决的问题 |
| --- | --- | --- |
| `actor` | Actor、邮箱、`Tell` / `Ask` / `Forward`、定时任务、远程消息 | 玩家、房间、订单、匹配等有状态业务 |
| `network` | TCP、UDP、WebSocket、统一消息帧、心跳、握手、可选 ECDH 加密 | 客户端长连接和实时通信 |
| `gateway` | C2S/S2C Protobuf 路由、请求响应、异步转发、主动推送 | 将网络协议接入业务 Actor |
| `cluster` | Consul 成员发现、节点间 TCP 传输、单播与广播 | 多节点发现和服务间通信 |
| `logicalactor` | 逻辑 ID、Owner 归属、跨节点路由、Factory 激活、Redis Directory | 不依赖物理节点的稳定业务实体 |
| `node` | 节点组装、Behavior、组件编排、统一生命周期 | 组织一个可运行的服务节点 |
| `pkg` | 组件管理、序列化、缓冲区、日志、定时器、Snowflake ID 等 | 构建运行时所需的通用能力 |

## 架构概览

```text
                         ┌──────────────────────┐
                         │      Client           │
                         └──────────┬───────────┘
                                    │ TCP / UDP / WebSocket
                         ┌──────────▼───────────┐
                         │       Network         │
                         │ frame / codec / ECDH  │
                         └──────────┬───────────┘
                                    │
                         ┌──────────▼───────────┐
                         │       Gateway         │
                         │ route / agent / push  │
                         └──────────┬───────────┘
                                    │
                 ┌──────────────────▼──────────────────┐
                 │            Actor System              │
                 │ mailbox / Ask / timer / lifecycle    │
                 └──────────────┬───────────────┬───────┘
                                │               │
                   ┌────────────▼──────┐  ┌────▼─────────────┐
                   │  Logical Actor    │  │    Cluster        │
                   │ owner / placement │  │ discovery / TCP   │
                   └────────────┬──────┘  └────┬─────────────┘
                                │               │
                         ┌──────▼───────────────▼──────┐
                         │       Other service nodes    │
                         └──────────────────────────────┘
```

## 一个最小的 Actor 示例

```go
package main

import (
	"fmt"

	"github.com/dzm2020/actormesh/actor"
)

type EchoActor struct{ actor.DefaultActor }

func (*EchoActor) HandleAsk(_ actor.Context, message any) (any, error) {
	return fmt.Sprintf("echo: %v", message), nil
}

func main() {
	system := actor.NewSystem("node-1", nil)
	_ = system.Init()
	_ = system.Start()
	defer system.Stop()

	pid, err := system.SpawnActor(&EchoActor{}, actor.SpawnOptions{Name: "echo"})
	if err != nil {
		panic(err)
	}

	result, err := system.Ask(actor.NoSender, pid, "hello", 0)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}
```

`Tell` 适合异步消息，`Ask` 适合请求/响应，`Forward` 适合保留原始发送者和请求元数据的路由场景。Actor 内的 `Ticker` 和 `AfterFunc` 也会回到当前邮箱执行，因此状态处理仍保持串行。

## 网络与 Gateway 的组合

网络层只负责连接和消息帧，Gateway 负责把 Protobuf 请求映射为业务路由；业务 Actor 不需要直接处理 TCP 粘包、WebSocket 消息边界或连接关闭细节。

```go
config := network.DefaultTCPConfig("127.0.0.1:9000")
config.EncryptEnable = true
server := network.NewTCPServer(config)

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

同一套路由既可以直接响应，也可以 `Forward` 到玩家、房间等业务 Actor；业务 Actor 的成功结果、错误和超时会自动回到客户端。

## 从单机扩展到集群

Cluster 默认使用 Consul 做成员发现、TCP 做节点间传输，并向 Actor System 提供远程发送能力：

```go
instance := member.ServiceInstance{
	ID: "game-1", Name: "game", Address: "127.0.0.1", Port: 9100,
}

clusterComponent := cluster.New(instance, func(nodeID string, data []byte) error {
	return actorSystem.OnMessage(nodeID, data)
})
```

当业务需要稳定的逻辑身份时，可以使用 Logical Actor：

```text
player:1001  ->  OwnerDirectory  ->  node-2  ->  PlayerActor
```

业务只关心 `ActorID{Kind: "player", Key: "1001"}`，Owner 失效时由发现、抢占和 Placement Strategy 协同完成重新路由或激活。跨节点 Actor 消息使用框架 Protobuf 编解码器，业务消息需要提供对应的 Protobuf 类型。

## 设计边界

ActorMesh 提供运行时和基础设施抽象，不替业务决定数据模型、业务协议或部署平台。当前默认集群实现使用 Consul；仓库提供 Redis Owner Directory 实现，也可以通过接口注入其他成员发现、传输层或目录实现。

## 安装与模块路径

`go.mod` 声明的模块路径为：

```text
github.com/dzm2020/actormesh
```

仓库中的部分源码仍保留旧的 `game-server/framework/...` 内部导入路径，当前版本属于模块路径迁移阶段。作为源码使用时，请先统一这些导入路径，再执行：

```bash
go mod download
go test ./...
```

## 文档导航

完整文档按“先理解运行时，再接入业务”的顺序组织：

- [文档总览](docs/README.md)
- [Actor 运行时](docs/actor.md)
- [Node 节点运行手册](docs/node.md)
- [网络与协议指南](docs/network.md)
- [Gateway 业务接入手册](docs/gateway.md)
- [Cluster 与服务发现](docs/cluster.md)
- [Logical Actor 手册](docs/logicalactor.md)
- [Component 生命周期与管理](docs/component.md)

## License

暂无单独 License 文件；在正式发布或对外分发前，请补充项目许可证和贡献指南。
