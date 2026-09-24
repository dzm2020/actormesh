package network

// 本文件验证 WebSocket 与 TCP 使用相同的「协议字节流」语义：
//  1. WebSocket 消息边界不作为协议帧边界
//  2. 一条 WebSocket 消息可以承载多个完整协议帧
//  3. 一个协议帧可以跨多条 WebSocket 消息
//  4. 入站大小限制按单个完整协议帧（Body + 17 字节帧头）计算
//  5. 主动关闭发送符合 RFC 6455 的 Close 控制帧

import (
	"errors"
	"game-server/framework/network/protocol"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// newWebSocketTestConfig 返回关闭加密、拉长心跳的测试配置，避免非心跳测试被超时干扰。
func newWebSocketTestConfig() *WebSocketConfig {
	config := DefaultWebSocketConfig("")
	config.EncryptEnable = false
	config.HeartbeatTimeout = tcpTestHeartbeat
	config.Normalize()
	return config
}

// webSocketTestConn 启动一个使用真实 HTTP Upgrade 和 WebSocket socket 的临时服务端，
// 返回客户端及服务端连接。临时端口和连接都由测试自动清理。
func webSocketTestConn(t *testing.T, config *WebSocketConfig, handler *recvHandler) (*websocket.Conn, *WebSocketConnection) {
	t.Helper()
	serverConnCh := make(chan *WebSocketConnection, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := config.Upgrader.Upgrade(writer, request, nil)
		if err != nil {
			t.Errorf("WebSocket Upgrade 失败: %v", err)
			return
		}
		serverConn := newWebSocketConnection(handler, config, conn)
		if err = serverConn.runStart(); err != nil {
			t.Errorf("启动 WebSocket 连接失败: %v", err)
			return
		}
		serverConnCh <- serverConn
	}))
	t.Cleanup(server.Close)

	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("WebSocket 客户端拨号失败: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	var serverConn *WebSocketConnection
	select {
	case serverConn = <-serverConnCh:
	case <-time.After(tcpTestWait):
		t.Fatal("等待服务端 WebSocket 连接超时")
	}
	t.Cleanup(func() { serverConn.Close(nil) })
	return client, serverConn
}

// readWebSocketCloseCode 等待客户端收到服务端的 Close 帧，并返回关闭码。
func readWebSocketCloseCode(t *testing.T, client *websocket.Conn) int {
	t.Helper()
	if err := client.SetReadDeadline(time.Now().Add(tcpTestWait)); err != nil {
		t.Fatalf("设置 WebSocket 读取超时失败: %v", err)
	}
	_, _, err := client.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("读取错误 = %v, 期望 WebSocket CloseError", err)
	}
	return closeErr.Code
}

// TestWebSocketMultipleFramesInOneMessage 验证粘包语义：一条 WS BinaryMessage
// 中拼接 3 个协议帧，服务端必须按顺序投递全部帧，不能只处理第一帧或丢弃尾部。
func TestWebSocketMultipleFramesInOneMessage(t *testing.T) {
	handler := &recvHandler{}
	client, _ := webSocketTestConn(t, newWebSocketTestConfig(), handler)

	bodies := []string{"sticky-1", "sticky-2", "sticky-3"}
	var stream []byte
	for _, body := range bodies {
		stream = append(stream, mustEncodeFrame(t, tcpTestCmd, tcpTestAct, []byte(body))...)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, stream); err != nil {
		t.Fatalf("发送多帧 WebSocket 消息失败: %v", err)
	}

	frames := handler.waitReceivedFrames(t, len(bodies))
	for i, want := range bodies {
		if got := bodyOf(frames[i]); got != want {
			t.Fatalf("第 %d 帧 Body = %q, 期望 %q", i, got, want)
		}
	}
}

// TestWebSocketFrameAcrossMessages 验证半包语义：把同一协议帧拆到两条 WS 消息中，
// 第一条到达时不能投递，第二条到达并拼齐后必须恰好投递一个完整帧。
func TestWebSocketFrameAcrossMessages(t *testing.T) {
	handler := &recvHandler{}
	client, _ := webSocketTestConn(t, newWebSocketTestConfig(), handler)
	data := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, []byte("frame-across-messages"))

	const split = 8
	if err := client.WriteMessage(websocket.BinaryMessage, data[:split]); err != nil {
		t.Fatalf("发送协议帧第一段失败: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := handler.frameCount(); got != 0 {
		t.Fatalf("半包阶段不应投递消息, 实际收到 %d 个", got)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, data[split:]); err != nil {
		t.Fatalf("发送协议帧第二段失败: %v", err)
	}

	frames := handler.waitReceivedFrames(t, 1)
	if got := bodyOf(frames[0]); got != "frame-across-messages" {
		t.Fatalf("拼齐后 Body = %q, 期望 %q", got, "frame-across-messages")
	}
	time.Sleep(50 * time.Millisecond)
	if got := handler.frameCount(); got != 1 {
		t.Fatalf("拼齐后应只收到 1 帧, 实际收到 %d 个", got)
	}
}

// TestWebSocketFrameLimitMatchesTCP 验证 WS 与 TCP 的入站上限口径一致：
// 恰好等于 MaxInboundSize 的完整帧可接收；一条 WS 消息中的多个合法帧
// 即使总长超过上限也都能接收；单帧超限时业务层不收到该帧，并向客户端
// 发送 1009（Message Too Big）关闭码。
func TestWebSocketFrameLimitMatchesTCP(t *testing.T) {
	const maxFrame = 32 * 1024

	validHandler := &recvHandler{}
	validConfig := newWebSocketTestConfig()
	validConfig.MaxInboundSize = maxFrame
	validConfig.Normalize()
	validClient, _ := webSocketTestConn(t, validConfig, validHandler)
	validFrame := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, make([]byte, maxFrame-protocol.UnifiedFrameHeaderLen))
	if err := validClient.WriteMessage(websocket.BinaryMessage, validFrame); err != nil {
		t.Fatalf("发送最大合法帧失败: %v", err)
	}
	validFrames := validHandler.waitReceivedFrames(t, 1)
	if got := len(validFrames[0].Body); got != maxFrame-protocol.UnifiedFrameHeaderLen {
		t.Fatalf("合法帧 Body 长度 = %d, 期望 %d", got, maxFrame-protocol.UnifiedFrameHeaderLen)
	}

	// MaxInboundSize 约束单个协议帧，不约束承载它们的整条 WS 消息。
	combinedHandler := &recvHandler{}
	combinedConfig := newWebSocketTestConfig()
	combinedConfig.MaxInboundSize = maxFrame
	combinedConfig.Normalize()
	combinedClient, _ := webSocketTestConn(t, combinedConfig, combinedHandler)
	first := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, make([]byte, maxFrame/2))
	second := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, make([]byte, maxFrame/2))
	combined := append(append([]byte(nil), first...), second...)
	if len(combined) <= maxFrame {
		t.Fatalf("组合消息长度 = %d, 应大于单帧上限 %d", len(combined), maxFrame)
	}
	if err := combinedClient.WriteMessage(websocket.BinaryMessage, combined); err != nil {
		t.Fatalf("发送包含多个合法帧的大 WS 消息失败: %v", err)
	}
	if frames := combinedHandler.waitReceivedFrames(t, 2); len(frames) != 2 {
		t.Fatalf("大 WS 消息中的合法帧数量 = %d, 期望 2", len(frames))
	}

	overHandler := &recvHandler{}
	overConfig := newWebSocketTestConfig()
	overConfig.MaxInboundSize = maxFrame
	overConfig.Normalize()
	overClient, _ := webSocketTestConn(t, overConfig, overHandler)
	overFrame := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, make([]byte, maxFrame))
	// 服务端读到帧头即可判定超限并关闭；Windows 下客户端可能在 WriteMessage
	// 返回前先收到关闭，因此与 TCP 超限测试一致，不要求这次写操作成功。
	_ = overClient.WriteMessage(websocket.BinaryMessage, overFrame)
	overHandler.waitClosedError(t, protocol.ErrFrameTooLarge)
	if got := overHandler.frameCount(); got != 0 {
		t.Fatalf("超限帧不应投递给业务, 实际收到 %d 个", got)
	}
	if code := readWebSocketCloseCode(t, overClient); code != websocket.CloseMessageTooBig {
		t.Fatalf("超限帧关闭码 = %d, 期望 %d", code, websocket.CloseMessageTooBig)
	}
}

// TestWebSocketActiveCloseIsNormal 验证服务端主动正常关闭时会先发送 Close 控制帧，
// 客户端收到 1000（Normal Closure），而不是 1006（Abnormal Closure / unexpected EOF）。
func TestWebSocketActiveCloseIsNormal(t *testing.T) {
	handler := &recvHandler{}
	client, serverConn := webSocketTestConn(t, newWebSocketTestConfig(), handler)

	serverConn.Close(nil)
	if code := readWebSocketCloseCode(t, client); code != websocket.CloseNormalClosure {
		t.Fatalf("主动关闭码 = %d, 期望 %d", code, websocket.CloseNormalClosure)
	}
}
