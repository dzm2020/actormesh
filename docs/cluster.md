# Cluster 与服务发现

`framework/cluster` 负责服务成员发现、节点间二进制消息传输和集群生命周期。默认实现由 Consul 成员管理器和 TCP `nettransport` 组成。

## 组件关系

```text
Cluster -> MemberManager(Consul Registry)
        -> Transport(nettransport.Transport)
                         -> network TCP
```

## 创建 Cluster

```go
instance := member.ServiceInstance{
	ID: "game-1", Name: "game", Address: "127.0.0.1", Port: 9100,
}
clusterComponent := cluster.New(instance, func(nodeID string, data []byte) error {
	return actorSystem.OnMessage(nodeID, data)
})
if err := clusterComponent.Init(); err != nil { return err }
if err := clusterComponent.Start(); err != nil { return err }
```

`Start` 启动成员管理器和传输监听；`Join` 由 `Start` 自动调用。

## 成员查询和消息

```go
members := clusterComponent.Members("game")
all := clusterComponent.AllMembers()
instance, ok := clusterComponent.MemberById("game-2")
err := clusterComponent.SendToNode("game-2", payload)
err = clusterComponent.Broadcast(payload)
```

Cluster 对外消息是 `[]byte`，Actor 远程消息可以把 Cluster 作为 `actor.RemoteSender` 使用。

## ServiceInstance

```go
type ServiceInstance struct {
	ID string
	Name string
	Address string
	Port int
	Meta map[string]string
}
```

`Validate` 当前只要求 `ID` 和 `Name` 非空；地址和端口由传输层在监听或连接时校验。`Clone` 会复制 Meta map。

## Consul 注册中心

```go
options := consul.DefaultOptions()
options.Address = "127.0.0.1:8500"
registry := consul.NewWithOptions(options)
if err := registry.Run(ctx); err != nil { return err }
if err := registry.Join(instance); err != nil { return err }
```

默认配置：

| 配置 | 默认值 |
| --- | --- |
| `Address` | `127.0.0.1:8500` |
| `TTL` | 5 秒 |
| `DeregisterAfter` | 30 秒 |

注册时创建 TTL 健康检查，并每 `TTL/2` 更新一次。`Leave` 停止保活并注销服务。Registry 没有导出的 `Stop` 方法；停止 watcher 的方式是取消传给 `Run` 的 Context。

## TCP 节点传输

```go
transport := nettransport.NewTransport("game-1")
err := transport.ListenAndServe("127.0.0.1:9100", handler)
err = transport.Connect("game-2", "127.0.0.1:9101", handler, 5*time.Second)
state := transport.ConnectionState("game-2")
err = transport.Send("game-2", payload)
```

`Connect` 在当前实现中同步执行 TCP 拨号，并等待 `DialTCP` 完成握手后返回；返回 `nil` 表示该连接已完成网络层 Ready（但后续仍可能断开）。可通过 `ConnectionState` 查询状态：不存在的节点为 `PeerStateIdle`，连接建立中为 `PeerStateHandshaking`，握手完成为 `PeerStateConnected`。Transport 使用 TCP 消息帧传输集群数据，并通过 Hello 帧绑定远端 Node ID。未建立连接时 `Send` 返回 `ErrPeerNotConnected`；空数据返回 `ErrMessageNil`。如果目标节点已有 peer，重复 `Connect` 会直接返回，不会创建第二条连接。

Cluster 连接器每秒检查成员列表。只有本地节点 ID 小于远端节点 ID 时主动发起连接，避免双向重复连接。

## API 参考

| API | 说明 |
| --- | --- |
| `cluster.New(instance, handler) *Cluster` | 创建默认 Cluster |
| `cluster.NewWithOptions(instance, handler, options) *Cluster` | 注入 MemberManager/Transport |
| `(*Cluster).Init/Start/Stop` | 生命周期 |
| `(*Cluster).Join/Leave` | 注册/注销本地实例 |
| `(*Cluster).AllMembers` | 查询所有成员 |
| `(*Cluster).Members(service)` | 查询服务成员 |
| `(*Cluster).MemberById(serviceID)` | 按 ID 查询成员 |
| `(*Cluster).SendToNode/Broadcast` | 节点单播/广播 |
| `consul.DefaultOptions` | 默认 Consul 配置 |
| `consul.New/NewWithOptions` | 创建 Registry |
| `(*Registry).Run/Join/Update/Leave` | Registry 生命周期和注册操作 |
| `(*Registry).Members/MemberById/AllMembers` | 查询健康成员 |
| `nettransport.NewTransport/NewTransportWithOptions` | 创建 TCP 集群传输 |
| `(*Transport).ListenAndServe/Connect/Disconnect` | 监听、连接和断开 |
| `(*Transport).ConnectionState` | 查询远端连接状态 |
| `(*Transport).Send/Broadcast` | 连接上的消息发送 |

## 接入注意事项

- 先 `Run/Init/Start`，再 发送消息。
- Consul 必须可访问，否则注册、查询会失败或重试。
- 成员查询返回快照副本，不应依赖其持续实时更新。
- `Cluster.Stop` 停止 Cluster 连接器；业务应同时取消 Registry `Run` 使用的 Context。

Consul Registry 还提供可选的 `SubscriptionCenter`：通过 `Subscribe(service, subscriber)` 注册成员变化订阅，使用返回的 `SubscriptionID` 调用 `Unsubscribe`；关闭 Registry 的运行 Context 后应主动 `Close` 订阅中心（如果业务直接持有它）。该中心是 Consul 实现的辅助 API，不属于 `cluster.ClusterAPI`。
