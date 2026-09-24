# Framework 文档

`framework` 是当前仓库的通用游戏服务器运行时。文档按“先理解运行时，再接入业务”的顺序组织；示例以当前源码为准，发现示例与源码不一致时以 Go API 和测试为准。

## 阅读路径

### 1. 运行时基础

| 文档 | 适合了解 |
| --- | --- |
| [Component 生命周期与管理](component.md) | 组件状态、初始化顺序和 Manager |
| [Node 节点运行手册](node.md) | 节点组装、Behavior、启动和关闭 |
| [Actor 使用指南](actor.md) | Actor、邮箱、Tell/Ask、停止和远程消息 |

### 2. 网络与集群

| 文档 | 适合了解 |
| --- | --- |
| [网络与协议指南](network.md) | TCP、UDP、WebSocket、消息帧、编解码和加密 |
| [Cluster 与服务发现](cluster.md) | Consul 成员发现和节点间传输 |

### 3. 业务接入

| 文档 | 适合了解 |
| --- | --- |
| [Gateway 业务接入手册](gateway.md) | 客户端路由、Agent、响应和主动推送 |
| [Logical Actor 手册](logicalactor.md) | 逻辑身份、Owner、跨节点路由和 Redis Directory |

## 文档约定

- `framework/docs` 只保存框架使用文档，不保存生成提示词、临时笔记或外部模型输出。
- 每个文档只描述一个稳定模块；跨模块启动流程放在 [Node 节点运行手册](node.md)，不要在各模块文档重复维护。
- 代码示例必须使用当前导出的 API。接口签名变化时，先更新示例，再更新导航或概念说明。
- 框架包的测试命令：

  ```bash
  go test ./framework/...
  ```

## 模块地图

```text
framework/
├── actor/          Actor 并发运行时
├── cluster/        成员发现与节点传输
├── db/             MongoDB、Redis 适配
├── gateway/        客户端 Agent 与业务路由
├── logicalactor/   逻辑 Actor 路由与 Owner Directory
├── network/        TCP、UDP、WebSocket 与协议
├── node/           节点生命周期与组件编排
└── pkg/            component、日志、编解码等通用工具
```

