package network

import (
	"errors"
	"game-server/framework/network/protocol"
	"game-server/framework/pkg/buffer"
	"io"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var _ WebSocketConn = (*WebSocketConnection)(nil)

// websocketCloseGracePeriod 给对端留出读取并应答 Close 控制帧的时间。
// 尤其在 Windows 上，存在未读入站数据时立即关闭 socket 可能使 TCP RST
// 先于 Close 帧到达，客户端只能观察到 1006 / connection reset。
const websocketCloseGracePeriod = 100 * time.Millisecond

type WebSocketConnection struct {
	*baseConn
	conn       *websocket.Conn
	readChunk  []byte
	readBuffer buffer.IBuffer
}

func newWebSocketConnection(handler TransportHandler, config *WebSocketConfig, conn *websocket.Conn) *WebSocketConnection {
	readBufferCap := config.MaxInboundSize + tcpReadChunkSize
	wsConn := &WebSocketConnection{
		baseConn:   newBaseConnWithRole(handler, config.CommonConfig, ConnectionRoleServer),
		conn:       conn,
		readChunk:  make([]byte, tcpReadChunkSize),
		readBuffer: buffer.New(readBufferCap, readBufferCap),
	}
	wsConn.baseConn.bind(wsConn)
	return wsConn
}

func (c *WebSocketConnection) Network() string {
	return "ws"
}
func (c *WebSocketConnection) LocalAddr() string {
	return c.conn.LocalAddr().String()
}
func (c *WebSocketConnection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *WebSocketConnection) readLoop() {
	var err error
	defer func() {
		c.Close(err)
	}()
	for {
		messageType, reader, readErr := c.conn.NextReader()
		if readErr != nil {
			err = readErr
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			c.logger.Warn("websocket connection read drop msg", zap.Int("type", messageType))
			continue
		}

		// WebSocket 消息边界不等于业务协议帧边界。按固定大小分块读取并写入
		// 统一缓冲区，使一条 WS 消息可包含多帧，一帧也可跨多条 WS 消息。
		for {
			var n int
			n, readErr = reader.Read(c.readChunk)
			if n > 0 {
				if _, err = c.readBuffer.Write(c.readChunk[:n]); err != nil {
					c.logger.Error("websocket connection read buffer write failed", zap.Error(err))
					return
				}
				if err = c.processReadBuffer(c.readBuffer); err != nil {
					c.logger.Error("websocket connection read buffer processing failed", zap.Error(err))
					return
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				err = readErr
				return
			}
		}
	}
}

func (c *WebSocketConnection) writeRaw(data []byte) error {
	t := time.Now().Add(c.options.WriteTimeout)
	if err := c.conn.SetWriteDeadline(t); err != nil {
		return err
	}
	err := c.conn.WriteMessage(websocket.BinaryMessage, data)
	if err != nil {
		c.logger.Error("websocket connection write failed", zap.Error(err))
		return err
	}
	return nil
}

func (c *WebSocketConnection) closeSocket() {
	// 对端发来的 Close 已由 gorilla/websocket 自动应答，避免重复写关闭帧。
	var peerClose *websocket.CloseError
	if !errors.As(c.closeErr, &peerClose) {
		code := websocket.CloseNormalClosure
		reason := ""
		if c.closeErr != nil {
			code = websocket.CloseInternalServerErr
			reason = c.closeErr.Error()
			if errors.Is(c.closeErr, protocol.ErrFrameTooLarge) {
				code = websocket.CloseMessageTooBig
			}
		}
		// Close reason 最多 123 字节，否则控制帧会超过 RFC 6455 的 125 字节上限。
		if len(reason) > 123 {
			reason = reason[:123]
		}
		for !utf8.ValidString(reason) {
			reason = reason[:len(reason)-1]
		}
		deadline := time.Now().Add(c.options.WriteTimeout)
		_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline)
		time.Sleep(websocketCloseGracePeriod)
	}
	_ = c.conn.Close()
}
