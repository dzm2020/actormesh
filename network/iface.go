package network

import (
	"context"
	"github.com/dzm2020/actormesh/network/protocol"
	"sync/atomic"

	"go.uber.org/zap"
)

type Server interface {
	Run(ctx context.Context, handler TransportHandler) error // 启动服务器
}

type TransportHandler interface {
	OnConnected(connection Connection) error
	OnMessage(connection Connection, frame *protocol.MessageFrame) error
	OnClose(connection Connection, err error)
}

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
	Log() *zap.Logger
	SendMessage(message *protocol.MessageFrame) error
	Close(err error)
}

type TCPConn interface {
	Connection                            // 包含通用连接能力
	SetLinger(seconds int) error          // 设置连接关闭时的延迟行为
	SetNoDelay(noDelay bool) error        // 设置是否禁用 Nagle 算法
	SetSocketReadBuffer(bytes int) error  // 设置套接字读取缓冲区大小
	SetSocketWriteBuffer(bytes int) error // 设置套接字写入缓冲区大小
}

type connCore interface {
	Connection
	readLoop() // 子类实现，但语义由 baseConn 调度
	writeRaw(data []byte) error
	closeSocket()
}

type ConnectionState uint32

const (
	ConnectionStateCreated ConnectionState = iota
	ConnectionStateHandshaking
	ConnectionStateReady
	ConnectionStateClosed
)

func (c ConnectionState) String() string {
	switch c {
	case ConnectionStateCreated:
		return "Created"
	case ConnectionStateHandshaking:
		return "Handshaking"
	case ConnectionStateReady:
		return "Ready"
	case ConnectionStateClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}

type ConnectionRole uint8

const (
	ConnectionRoleServer ConnectionRole = iota
	ConnectionRoleClient
)

type WebSocketConn interface {
	Connection // 包含通用连接能力
}

var _ TransportHandler = (*DefaultTransportHandler)(nil)

type DefaultTransportHandler struct{}

func (d *DefaultTransportHandler) OnConnected(connection Connection) error {
	return nil
}

func (d *DefaultTransportHandler) OnMessage(connection Connection, frame *protocol.MessageFrame) error {
	return nil
}

func (d *DefaultTransportHandler) OnClose(connection Connection, err error) {
	return
}

type Codec interface {
	Encode(data []byte) (encoded []byte, flags uint8, err error) // 编码出站数据并返回编码标记
	Decode(data []byte, flags uint8) ([]byte, error)             // 根据编码标记解码入站数据
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx
}

var connectionIDCounter atomic.Int64

func generateConnectionID() int64 {
	return connectionIDCounter.Add(1)
}
