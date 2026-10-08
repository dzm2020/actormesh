# Cluster 与服务发现

`cluster` 负责服务发现、节点消息传输和集群生命周期。默认实现由 `cluster/bootstrap` 组装 Consul Registry 和 TCP Transport。

## 创建 Cluster

```go
node := member.NodeInfo{
    ID: "game-1", Name: "game", Address: "127.0.0.1:9100",
}
clusterComponent := bootstrap.NewDefaultCluster(bootstrap.Options{
    NodeInfo: node,
    Handler: func(nodeID string, data []byte) error {
        return actorSystem.OnMessage(nodeID, data)
    },
})
if err := clusterComponent.Init(); err != nil { return err }
if err := clusterComponent.Start(); err != nil { return err }
```

包路径为 `github.com/dzm2020/actormesh/cluster/member` 和 `github.com/dzm2020/actormesh/cluster/bootstrap`。直接调用 `cluster.NewWithOptions(cluster.Options{...})` 时必须提供 `NodeInfo`、`Registry`、`Transport` 和 `Handler`。bootstrap 在 Registry 或 Transport 为空时创建默认实现，并把 Handler 配置到默认 Transport。

`Init` 校验节点 ID、名称及 Registry、Transport、Handler 是否为空。`Start` 依次运行 Transport、Registry，启动连接器，再自动调用 `Join`。TCP 监听在后台运行，因此 Start 成功不保证监听已绑定成功；后台监听错误记录到日志。启动失败不会自动回滚已启动资源，可调用 Stop 清理。

## 成员和消息

```go
members := clusterComponent.Members("game")
all := clusterComponent.AllMembers()
node, ok := clusterComponent.MemberById("game-2")
err := clusterComponent.SendToNode("game-2", payload)
err = clusterComponent.Broadcast(payload)
```

Members 按服务名称查询，返回以节点 ID 为键的 map；AllMembers 返回切片。Cluster 消息为 `[]byte`，可作为 `actor.RemoteSender` 使用。

## NodeInfo

```go
type NodeInfo struct {
    ID        string
    Name      string
    Address   string
    Status    string
    StartedAt int64
    Version   string
    Meta      map[string]string
}
```

Address 包含端口，例如 `127.0.0.1:9100`，没有独立 Port 字段。Validate 只要求 ID 和 Name 非空；Consul 注册会解析非空地址并检查端口范围，传输层也会在监听或连接时处理地址。Clone 复制所有字段并深拷贝 Meta。`consul.ServiceInstance` 是 `member.NodeInfo` 的兼容别名。

## Consul 注册中心

```go
options := consul.DefaultOptions()
options.Address = "127.0.0.1:8500"
registry := consul.NewWithOptions(options)
if err := registry.Run(ctx); err != nil { return err }
if err := registry.Join(node); err != nil { return err }
```

| 配置 | 默认值 |
| --- | --- |
| Address | `127.0.0.1:8500` |
| TTL | 5 秒 |
| DeregisterAfter | 30 秒 |

Options 还支持 Scheme、Token、Datacenter 和 Logger。空 Address、TTL 不大于 2 纳秒、非正 DeregisterAfter 会回退到默认值。

Run 初始化客户端、注册管理器及异步 watcher，查询与注册应在 Run 成功后使用。Join 创建 TTL 健康检查，启动保活后立即更新一次，再每 TTL/2 更新。Update 使用与 Join 相同的注册逻辑。Leave 停止保活并注销服务。Registry 没有导出 Stop；取消 Run 的 Context 会停止 watcher 和保活，但不会立即注销服务。

成员来自异步更新的健康成员缓存。Members 和 MemberById 会 Clone 节点；AllMembers 返回新的切片，但节点 Meta map 仍与缓存共享，不应修改。Consul 使用保留元数据键 `node_info` 保存完整节点信息。

## TCP 节点传输

```go
transport := nettransport.NewTransportWithOptions(nettransport.Options{
    LocalNodeID: "game-1",
    ListenAddr:  "127.0.0.1:9100",
    Handler:     handler,
})
err := transport.Run()
err = transport.Connect("game-2", "127.0.0.1:9101", 5*time.Second)
state := transport.ConnectionState("game-2")
err = transport.Send("game-2", payload)
transport.Close()
```

Options 还支持 TcpConfig 和 Logger，为空时分别使用 `network.DefaultTCPConfig("")` 和默认 Logger。Handler 为空时 Run 和 Connect 返回 ErrHandlerIsNil。

Connect 同步拨号并等待网络层 Ready；集群 Hello 交换可能尚未完成，应通过 ConnectionState 确认 PeerStateConnected 后发送。不存在节点为 PeerStateIdle，Hello 交换期间为 PeerStateHandshaking。已有目标 peer 时重复 Connect 直接返回 nil，该返回值也不保证目标已连接。连接本地节点会返回错误。

Transport 使用 TCP 消息帧和 Hello 帧绑定远端节点 ID。未连接目标 Send 返回 ErrPeerNotConnected；Transport 自身不检查空 payload。Broadcast 向已有 peer 发送，部分失败会记录日志但仍返回 nil。业务 Handler 的错误仅记录日志。

Cluster 每秒检查成员列表，只有本地节点 ID 小于远端节点 ID 时主动连接，连接超时参数为 5 秒；已连接或握手中的节点会跳过。

## API 参考

| API | 说明 |
| --- | --- |
| `cluster.NewWithOptions(options)` | 注入节点、Registry、Transport 和 Handler |
| `bootstrap.NewDefaultCluster(options)` | 创建默认 Cluster，可覆盖 Registry/Transport |
| `bootstrap.NewDefaultRegistry(logger)` | 创建默认 Consul Registry |
| `bootstrap.NewDefaultTransportAPI(nodeID, address, handler, logger)` | 创建默认 TCP Transport |
| `(*Cluster).Init/Start/Stop` | 生命周期 |
| `(*Cluster).Join/Leave` | 注册/注销本地节点 |
| `(*Cluster).AllMembers/Members/MemberById` | 成员查询 |
| `(*Cluster).SendToNode/Broadcast` | 单播/广播 |
| `consul.DefaultOptions` | 默认配置 |
| `consul.New/NewWithOptions` | 创建 Registry |
| `(*Registry).Run/Join/Update/Leave` | 运行与注册操作 |
| `(*Registry).Members/MemberById/AllMembers` | 查询健康成员缓存 |
| `nettransport.NewTransportWithOptions` | 创建 TCP Transport |
| `(*Transport).Run/Connect/Disconnect/Close` | 监听、连接、断开、关闭 |
| `(*Transport).ConnectionState/Send/Broadcast` | 状态查询和消息发送 |

## 停止和订阅

Cluster.Stop 取消自身 Context（同时取消 Registry watcher 和保活），并调用 Transport.Close。它不会调用 Leave，需要立即注销时，应先 Leave 再 Stop。组件生命周期不支持重复启动。

Consul 包导出独立 SubscriptionCenter，支持 Subscribe、Unsubscribe、Notify 和 Close。当前 Registry 没有导出订阅入口，watcher 也没有调用 Notify；独立订阅中心不会自动收到 Registry 成员变化。Subscriber 要求 `OnClusterChange([]member.NodeInfo)` 无返回值，现有 SubscriberFunc 返回 error，不能直接实现该接口。Subscribe 的 service 参数当前不会被 Notify 用作筛选条件。
