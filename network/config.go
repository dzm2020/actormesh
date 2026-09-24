package network

import (
	"game-server/framework/pkg/netutil"
	"time"

	"github.com/gorilla/websocket"
)

type CommonConfig struct {
	Address          string
	EncryptEnable    bool
	HeartbeatTimeout time.Duration
	Codec            Codec
	MaxInboundSize   int // 入包最大整帧size（Body + 17 字节帧头）
	MaxOutboundSize  int // 出包最大整帧size（Body + 17 字节帧头）
	SendChanSize     int
	HandshakeTimeout time.Duration
	WriteTimeout     time.Duration
}

// tcpReadChunkSize 是 TCP readLoop 单次从 socket 读取的字节数。
// 接收缓冲区需要为「最大整帧 + 一个读块」预留空间，避免半包与下一批数据
// 同时驻留时触发缓冲区上限。
const tcpReadChunkSize = netutil.KB * 8

func (config *CommonConfig) Normalize() {
	if config.MaxInboundSize <= 0 {
		config.MaxInboundSize = 32 * netutil.KB
	}
	if config.MaxOutboundSize <= 0 {
		config.MaxOutboundSize = 32 * netutil.KB
	}
	if config.SendChanSize <= 0 {
		config.SendChanSize = 1024
	}
	if config.HeartbeatTimeout <= 0 {
		config.HeartbeatTimeout = time.Second * 5
	}
	config.HeartbeatTimeout = max(config.HeartbeatTimeout, time.Second*1)

	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = time.Second * 5
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = time.Second * 3
	}
}

func DefaultTCPConfig(address string) *TCPConfig {
	config := &TCPConfig{}
	config.Address = address
	return config
}

type TCPConfig struct {
	CommonConfig
	ReadBufferCap int
}

func (config *TCPConfig) Normalize() {
	config.CommonConfig.Normalize()

	// 接收缓冲区至少要能同时容纳「一个最大整帧 + 一个读块」：
	// 每次只消费完整帧，缓冲区里最多残留 MaxInboundSize-1 字节半包，
	// 此时再读入一个读块若超出容量就会触发 ErrBufferOverLimit，导致合法连接被断开。
	minReadBufferCap := config.MaxInboundSize + tcpReadChunkSize
	if config.ReadBufferCap < minReadBufferCap {
		config.ReadBufferCap = minReadBufferCap
	}
}

func DefaultUDPConfig() *UDPConfig { return &UDPConfig{} }

type UDPConfig struct {
	CommonConfig
	ServerSendChanSize int
}

func (config *UDPConfig) Normalize() {
	config.CommonConfig.Normalize()
	if config.ServerSendChanSize <= 0 {
		config.ServerSendChanSize = 10240
	}
}

func DefaultWebSocketConfig(address string) *WebSocketConfig {
	config := &WebSocketConfig{}
	config.Address = address
	return config
}

type WebSocketConfig struct {
	CommonConfig
	Upgrader *websocket.Upgrader
}

func (config *WebSocketConfig) Normalize() {
	config.CommonConfig.Normalize()
	if config.Upgrader == nil {
		config.Upgrader = &websocket.Upgrader{}
	}
}
