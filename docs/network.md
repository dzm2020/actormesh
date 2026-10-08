# 网络与协议指南

`network` 提供 TCP、UDP 和 WebSocket 服务端，以及 TCP 客户端连接。传输层统一向 `TransportHandler` 交付 `protocol.MessageFrame`，业务代码不需要直接处理 TCP 粘包或 WebSocket 消息边界。

## 连接模型

```text
Server.Run(ctx, handler)
        |
        v
Connection Created -> Handshaking -> Ready -> Closed
                              |
                   EncryptEnable=true 时执行 ECDH
```

所有服务端构造函数都只接收配置，处理器在 `Run` 时传入：

```go
type Server interface { Run(ctx context.Context, handler TransportHandler) error }
```

## TCP

```go
config := network.DefaultTCPConfig("127.0.0.1:9000")
config.EncryptEnable = true
config.HeartbeatTimeout = 30 * time.Second
server := network.NewTCPServer(config)
err := server.Run(ctx, &network.DefaultTransportHandler{})
```

当前源码签名是 `NewTCPServer(config *TCPConfig) *TCPServer`。历史测试中的 `NewTCPServer(handler, config)` 不作为当前 API。

客户端使用同一个 `TCPConfig`：

```go
config := network.DefaultTCPConfig("127.0.0.1:9000")
connection, err := network.DialTCP(
	ctx, 5*time.Second, &network.DefaultTransportHandler{}, config,
	nil, // userdata，可通过 Connection.UserData() 读取
)
if err != nil { return err }
defer connection.Close(nil)
```

第五个参数 `userdata` 会写入连接，可由 `Connection.UserData()` 读取；不需要时传 `nil`。当前源码只提供 `DefaultTCPConfig`，没有 `DefaultTCPClientConfig`。

## UDP

```go
config := network.DefaultUDPConfig()
config.Address = "127.0.0.1:9001"
server, err := network.NewUDPServer(config)
if err != nil { return err }
return server.Run(ctx, handler)
```

UDP 数据报应携带一个完整协议帧；当前接收实现只处理头后的首个帧，不跨数据报拼接半包，也不循环消费多帧。数据报需要带 16 字节头：8 字节 `SessionID` 和 8 字节 `Sequence`。`SessionID` 不能为 0。同一个源地址出现新的 SessionID 时，服务端会关闭旧 UDP 连接。`Sequence` 随服务端发送自增，接收端当前不做排序、去重或重传。

## WebSocket

```go
config := network.DefaultWebSocketConfig("127.0.0.1:9002")
config.EncryptEnable = false
server := network.NewWebSocketServer(config)
return server.Run(ctx, handler)
```

WebSocket 服务端使用 `/` 路径升级连接，文本帧和二进制帧都会尝试按统一消息帧解析，其他消息类型会被丢弃。

## TransportHandler

```go
type TransportHandler interface {
	OnConnected(connection Connection) error
	OnMessage(connection Connection, frame *protocol.MessageFrame) error
	OnClose(connection Connection, err error)
}
```

`OnConnected` 在 Ready 时调用，返回错误会关闭连接。写循环退出时异步调用 `OnClose`，握手失败或启动失败也可能触发该回调。`OnMessage` 返回错误会结束读循环并关闭连接。

## Connection

```go
type Connection interface {
	ID() int64
	LocalAddr() string
	RemoteAddr() string
	State() ConnectionState
	Context() context.Context
	UserData() any
	SetUserData(data any)
	Role() ConnectionRole
	Network() string
	Log() *glog.Logger
	SendMessage(message *protocol.MessageFrame) error
	Close(err error)
}
```

TCP 连接额外支持 `SetLinger`、`SetNoDelay`、`SetSocketReadBuffer` 和 `SetSocketWriteBuffer`。连接写入使用有容量限制的发送队列，队列满时返回 `ErrNetworkChannelFull`。

## 配置默认值

| 配置 | 默认值 |
| --- | --- |
| `MaxInboundSize` | 32 KiB（整帧长度：Body + 17 字节帧头） |
| `MaxOutboundSize` | 32 KiB（整帧长度：Body + 17 字节帧头） |
| TCP `ReadBufferCap` | 至少 `MaxInboundSize + 8 KiB` |
| `SendChanSize` | 所有连接 1024 |
| UDP `ServerSendChanSize` | 服务端共享队列 10240 |
| `HeartbeatTimeout` | 5 秒；正值小于 1 秒时提升到 1 秒 |
| `WriteTimeout` | 3 秒 |
| `HandshakeTimeout` | 5 秒 |
| `Upgrader` | 空配置的 `websocket.Upgrader` |

默认配置函数只创建配置对象，服务端构造函数和 `DialTCP` 调用 `Normalize()` 后应用默认值。`HeartbeatTimeout <= 0` 会恢复为 5 秒，不能用 0 禁用检查。每隔超时时长的一半检查一次；完整入站协议帧刷新活动时间，发送不刷新，握手期间跳过检查。

## MessageFrame

```go
type MessageFrame struct {
	Cmd uint8
	Act uint8
	Flags uint8
	Error uint16
	Index uint32
	Body []byte
}
```

CRC 是私有字段，编码时按 Body 计算，不能通过导出结构字段设置。统一帧头为 17 字节：`BodyLength(4) Cmd(1) Act(1) Flags(1) Error(2) Index(4) CRC(4) Body(n)`。

```go
frame := protocol.NewMessageFrame(1, 1, 0, []byte("hello"))
data, err := protocol.EncodeMessageFrame(frame)
decoded, consumed, err := protocol.DecodeMessageFrame(data)
```

`consumed == 0` 表示数据不足，需要等待更多数据；CRC 校验失败返回错误。`EncodeMessageFrameWithLimit` 和 `DecodeMessageFrameWithLimit` 按**整帧长度**（Body + 17 字节帧头）限制大小，`maxFrameSize <= 0` 表示不限制；超限返回 `protocol.ErrFrameTooLarge`（旧名 `ErrBodyTooLarge` 为兼容别名）。`MaxInboundSize` / `MaxOutboundSize` 同样是整帧口径，因此 TCP/WebSocket/UDP 对同一帧的接收判定一致。

WebSocket 当前没有调用 `SetReadLimit`。它使用 `NextReader` 按 8 KiB 分块读取，接收缓冲区容量为 `MaxInboundSize + 8 KiB`；一条 WS 消息可包含多个协议帧，一个协议帧也可跨多条 WS 消息。每个帧按整帧长度判定，默认配置下 32768 字节整帧（Body 32751）可收，32785 字节整帧（Body 32768）会被拒绝。超限本地关闭发送关闭码 1009，其他本地错误发送 1011，正常本地关闭发送 1000。

业务命令不能使用 `Cmd == 0`，该命令保留给内部握手和关闭控制。

## 编解码 Pipeline

```go
gzipCodec, err := codec.NewGzip(codec.GzipOptions{
	MinSize: 256, MaxDecodedSize: 64 * 1024,
})
pipeline, err := codec.New(gzipCodec)
encoded, flags, err := pipeline.Encode(payload)
decoded, err := pipeline.Decode(encoded, flags)
```

`Pipeline.Encode` 按注册顺序执行转换；`Decode` 按逆序执行。每个转换器必须返回恰好一个 flag bit，重复或未知 flag 会报错。Payload 小于 `MinSize` 时不压缩，解压结果超过 `MaxDecodedSize` 时返回 `ErrDecodedTooLarge`。

## 加密

连接配置 `EncryptEnable` 为 true 时，连接建立阶段执行 ECDH 公钥交换，业务 Body 在发送前加密、接收后解密。内部握手帧不经过业务加密流程。

底层 API：

```go
cipher, err := encrypt.NewECDHCipher()
publicKey := cipher.PublicKey()
err = cipher.GenerateSharedKey(peerPublicKey)
ciphertext, err := cipher.Encrypt(plaintext)
plaintext, err = cipher.Decrypt(ciphertext)
```

业务通常应通过 `CommonConfig.EncryptEnable` 使用连接层加密。当前实现通过 X25519 和 HKDF-SHA256 派生 32 字节密钥，再执行源码定义的逐字节可逆转换，输出与输入等长。配置 Codec 时，发送依次执行 Codec 编码、业务 Body 加密和帧编码；接收按逆序恢复。内部握手帧跳过加密，但仍经过配置的 Codec。

## 导出 API 参考

### `network`

| API | 说明 |
| --- | --- |
| `DefaultTCPConfig(address string) *TCPConfig` | 创建 TCP 配置 |
| `DefaultUDPConfig() *UDPConfig` | 创建 UDP 配置 |
| `DefaultWebSocketConfig(address string) *WebSocketConfig` | 创建 WebSocket 配置 |
| `NewTCPServer(config *TCPConfig) *TCPServer` | 创建 TCP 服务端 |
| `NewUDPServer(config *UDPConfig) (*UDPServer, error)` | 创建 UDP 服务端 |
| `NewWebSocketServer(config *WebSocketConfig) *WebSocketServer` | 创建 WebSocket 服务端 |
| `DialTCP(ctx, timeout, handler, config, userdata) (*TCPConnection, error)` | 建立 TCP 客户端连接，并等待连接进入 Ready |
| `(*TCPServer).Run(ctx, handler) error` | 运行 TCP 服务端 |
| `(*UDPServer).Run(ctx, handler) error` | 运行 UDP 服务端 |
| `(*WebSocketServer).Run(ctx, handler) error` | 运行 WebSocket 服务端 |
| `Connection` | 通用连接接口 |
| `TCPConn` | TCP 扩展连接接口 |
| `WebSocketConn` | WebSocket 连接接口 |
| `TransportHandler` | 连接、消息和关闭回调接口 |
| `DefaultTransportHandler` | 空回调实现 |
| `CommonConfig` | 通用连接配置 |
| `TCPConfig` | TCP 配置 |
| `UDPConfig` | UDP 配置 |
| `WebSocketConfig` | WebSocket 配置 |
| `ConnectionState.String() string` | 返回连接状态文本 |
| `TCPConnection` / `UDPConnection` / `WebSocketConnection` | 具体连接实现 |
| `Codec` | 网络层编解码接口 |

### `network/protocol`

| API | 说明 |
| --- | --- |
| `NewMessageFrame(cmd, act, flags, body) *MessageFrame` | 创建消息帧 |
| `(*MessageFrame).Clone() *MessageFrame` | 深拷贝消息帧 |
| `(*MessageFrame).ID() uint16` | 返回 Cmd/Act 组合 ID |
| `CmdAct(cmd, act uint8) uint16` | 组合命令 ID |
| `EncodeMessageFrame` / `DecodeMessageFrame` | 编解码统一消息帧 |
| `EncodeUDPHeader` / `DecodeUDPHeader` | 编解码 UDP 头 |

### `network/codec` 与 `network/encrypt`

| API | 说明 |
| --- | --- |
| `New(transforms ...Transform) (*Pipeline, error)` | 创建转换流水线 |
| `(*Pipeline).Encode` / `Decode` | 顺序编码、逆序解码 |
| `NewGzip(options GzipOptions) (*Gzip, error)` | 创建 Gzip 转换器 |
| `NewECDHCipher() (*ECDHCipher, error)` | 创建 ECDH 加密器 |

### 网络常量和错误

| API | 说明 |
| --- | --- |
| `ConnectionStateCreated` / `ConnectionStateHandshaking` / `ConnectionStateReady` / `ConnectionStateClosed` | 连接状态 |
| `ConnectionRoleServer` / `ConnectionRoleClient` | 连接角色 |
| `ErrConnectionClosed` | 连接已关闭 |
| `ErrNetworkChannelFull` | 发送队列已满 |
| `ErrHandshakeNotComplete` | 握手尚未完成 |
| `ErrHeartbeatTimeout` | 心跳超时 |
| `ErrInvalidFrameConsumeSize` | 消费字节数非法 |
| `ErrInvalidConnectionState` | 连接状态非法 |
| `ErrUnexpectedTCPConnType` | 非预期 TCP 连接类型错误常量 |

## 重要错误

`ErrConnectionClosed`、`ErrNetworkChannelFull`、`ErrHandshakeNotComplete`、`ErrHeartbeatTimeout`、`ErrInvalidFrameConsumeSize` 和协议包错误应由上层处理。接收解析、解码或 Handler 返回错误时读循环会自动关闭连接；`SendMessage` 返回错误由调用者处理。
