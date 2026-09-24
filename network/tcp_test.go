package network

// 本文件是 TCP 传输层的测试清单，覆盖以下内容：
//  1. 明文链路的正常建连与收发往返
//  2. 粘包：一次写入多个完整帧
//  3. 拆包/半包：一帧分多次写入
//  4. 帧大小上限口径（整帧 = Body + 17 字节帧头）
//  5. TCP 接收缓冲区容量（半包 + 一个读块不能超限）
//  6. 出站帧上限
//  7. CRC 校验失败的处理
//  8. 开启加密时的 ECDH 握手
//  9. 心跳超时断连
// 10. 发送队列满与关闭后的发送行为
// 11. 拨号失败
// 12. 服务端 ctx 取消后的生命周期与端口释放
// 13. 多客户端并发收发互不串扰
// 14. 服务端停止不影响已建立连接（连接生命周期由上层决定）
//
// 所有测试都使用临时端口，避免固定端口被占用或测试之间互相干扰。

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"game-server/framework/network/protocol"
	"game-server/framework/pkg/buffer"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/netutil"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	glog.SetLogLevel(zapcore.InfoLevel)
}

const (
	// tcpTestWait 是等待异步事件（收包、连接关闭、服务端退出）的通用上限。
	tcpTestWait = 5 * time.Second
	// tcpTestHeartbeat 是测试用心跳超时。刻意拉长，避免非心跳类测试
	// 被默认 5 秒心跳误判为超时断连。
	tcpTestHeartbeat = 30 * time.Second
	// tcpTestCmd / tcpTestAct 是测试用业务命令。
	// Cmd 不能为 0，0 被协议保留给内部握手与关闭控制。
	tcpTestCmd uint8 = 1
	tcpTestAct uint8 = 1
)

// newTCPTestConfig 返回测试用 TCP 配置：关闭加密、拉长心跳，并完成 Normalize。
func newTCPTestConfig(address string) *TCPConfig {
	config := DefaultTCPConfig(address)
	config.EncryptEnable = false
	config.HeartbeatTimeout = tcpTestHeartbeat
	config.Normalize()
	return config
}

// freeTCPAddress 申请一个空闲的本地回环端口并立即释放，把地址交给服务端使用。
// 释放与监听之间存在极小竞态，但换来了测试之间不会互相占用固定端口。
func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("申请空闲端口失败: %v", err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatalf("释放临时监听端口失败: %v", err)
	}
	return address
}

// startTCPTestServer 在后台启动 TCP 服务端，返回等待其退出的函数。
func startTCPTestServer(t *testing.T, config *TCPConfig, handler TransportHandler) func() {
	t.Helper()
	server := NewTCPServer(config)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Run(ctx, handler)
	}()

	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("TCP 服务端退出错误: %v", err)
			}
		case <-time.After(tcpTestWait):
			t.Errorf("取消 ctx 后 TCP 服务端未在 %s 内退出", tcpTestWait)
		}
	}
}

// dialTCPWithRetry 反复尝试 DialTCP，直到服务端完成监听或超时；
// 返回最后一次错误，调用方自行决定是 Fatal 还是 Error，
// 因此这个函数可以安全地在测试协程之外调用。
func dialTCPWithRetry(handler TransportHandler, config *TCPConfig) (Connection, error) {
	deadline := time.Now().Add(tcpTestWait)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := DialTCP(context.Background(), time.Second, handler, config, nil)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return nil, fmt.Errorf("在 %s 内连接 %s 失败: %w", tcpTestWait, config.Address, lastErr)
}

// mustDialTCPWithRetry 是 dialTCPWithRetry 的断言封装，只能在主测试协程调用。
func mustDialTCPWithRetry(t *testing.T, handler TransportHandler, config *TCPConfig) Connection {
	t.Helper()
	conn, err := dialTCPWithRetry(handler, config)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// recvHandler 是测试用处理器：记录连接、收到的消息与关闭事件，
// 并可选地自动回复或原样回显，便于断言「收到 / 没收到 / 被关闭」。
type recvHandler struct {
	mu           sync.Mutex
	frames       []*protocol.MessageFrame
	connected    []Connection
	closedErrors []error
	replyBody    []byte // 非空时，收到业务帧后回该 Body
	echo         bool   // 为 true 时原样回显收到的 Body
}

func (h *recvHandler) OnConnected(connection Connection) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connected = append(h.connected, connection)
	return nil
}

func (h *recvHandler) OnMessage(connection Connection, frame *protocol.MessageFrame) error {
	h.mu.Lock()
	h.frames = append(h.frames, frame.Clone())
	reply := append([]byte(nil), h.replyBody...)
	echo := h.echo
	h.mu.Unlock()

	if echo {
		return connection.SendMessage(protocol.NewMessageFrame(tcpTestCmd, 2, 0, frame.Body))
	}
	if len(reply) > 0 {
		return connection.SendMessage(protocol.NewMessageFrame(tcpTestCmd, 2, 0, reply))
	}
	return nil
}

func (h *recvHandler) OnClose(_ Connection, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closedErrors = append(h.closedErrors, err)
}

// frameCount 返回已收到的帧数快照。
func (h *recvHandler) frameCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.frames)
}

// connectedCount 返回 OnConnected 调用次数快照。
func (h *recvHandler) connectedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.connected)
}

// receivedFrames 返回已收帧的副本快照，避免调用方与回调并发访问。
func (h *recvHandler) receivedFrames() []*protocol.MessageFrame {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*protocol.MessageFrame, 0, len(h.frames))
	for _, frame := range h.frames {
		out = append(out, frame.Clone())
	}
	return out
}

// hasClosedError 判断 OnClose 是否收到过满足 errors.Is 条件的错误；
// target 为 nil 时只要求发生过关闭。
func (h *recvHandler) hasClosedError(target error) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, err := range h.closedErrors {
		if target == nil || errors.Is(err, target) {
			return true
		}
	}
	return false
}

// waitReceivedFrames 轮询等待收到至少 want 个帧，超时则测试失败。
func (h *recvHandler) waitReceivedFrames(t *testing.T, want int) []*protocol.MessageFrame {
	t.Helper()
	deadline := time.Now().Add(tcpTestWait)
	for {
		frames := h.receivedFrames()
		if len(frames) >= want {
			return frames
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 %d 个消息帧超时，实际收到 %d 个", want, len(frames))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitClosed 轮询等待 OnClose 被调用，超时则测试失败。
func (h *recvHandler) waitClosed(t *testing.T) {
	t.Helper()
	h.waitClosedError(t, nil)
}

// waitClosedError 轮询等待 OnClose 收到满足 errors.Is 条件的错误，超时则测试失败。
func (h *recvHandler) waitClosedError(t *testing.T, target error) {
	t.Helper()
	deadline := time.Now().Add(tcpTestWait)
	for {
		if h.hasClosedError(target) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待连接关闭(错误 %v)超时", target)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// bodyOf 把帧 Body 转成字符串，便于失败信息可读。
func bodyOf(frame *protocol.MessageFrame) string {
	if frame == nil {
		return "<nil>"
	}
	return string(frame.Body)
}

// mustEncodeFrame 编码业务帧，失败即结束测试。
func mustEncodeFrame(t *testing.T, cmd, act uint8, body []byte) []byte {
	t.Helper()
	data, err := protocol.EncodeMessageFrame(protocol.NewMessageFrame(cmd, act, 0, body))
	if err != nil {
		t.Fatalf("编码消息帧失败: %v", err)
	}
	return data
}

// corruptCRC 篡改已编码帧的 CRC 字段（帧头偏移 13..17），用于构造校验失败的数据。
func corruptCRC(data []byte) []byte {
	corrupted := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(corrupted[13:17], binary.BigEndian.Uint32(corrupted[13:17])^0xFFFFFFFF)
	return corrupted
}

// tcpTestConn 用「自建 listener + newTCPConnection + runStart」启动服务端连接，
// 返回服务端连接对象和客户端裸 socket。
// 相比整包 TCPServer，它允许客户端按字节写原始数据（粘包 / 半包 / 篡改 CRC），
// 但仍然走真实的 TCP 读写循环与统一帧解析逻辑。
func tcpTestConn(t *testing.T, config *TCPConfig, handler TransportHandler, clientTimeout time.Duration) (*TCPConnection, net.Conn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("监听 TCP 失败: %v", err)
	}

	acceptCh := make(chan *net.TCPConn, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.AcceptTCP()
		if acceptErr != nil {
			acceptErrCh <- acceptErr
			return
		}
		acceptCh <- conn
	}()

	client, err := net.DialTimeout("tcp", listener.Addr().String(), 2*time.Second)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("客户端拨号失败: %v", err)
	}
	if err = client.SetDeadline(time.Now().Add(clientTimeout)); err != nil {
		_ = client.Close()
		_ = listener.Close()
		t.Fatalf("设置客户端超时失败: %v", err)
	}

	var accepted *net.TCPConn
	select {
	case accepted = <-acceptCh:
	case err = <-acceptErrCh:
		_ = client.Close()
		_ = listener.Close()
		t.Fatalf("服务端 Accept 失败: %v", err)
	case <-time.After(tcpTestWait):
		_ = client.Close()
		_ = listener.Close()
		t.Fatal("等待服务端 Accept 超时")
	}
	_ = listener.Close()

	conn := newTCPConnection(handler, accepted, config, ConnectionRoleServer)
	if err = conn.runStart(); err != nil {
		_ = client.Close()
		t.Fatalf("启动服务端连接失败: %v", err)
	}
	t.Cleanup(func() {
		conn.Close(nil)
		_ = client.Close()
	})
	return conn, client
}

// writeAll 把分片依次写入 TCP 流；失败即结束测试。
func writeAll(t *testing.T, conn net.Conn, chunks ...[]byte) {
	t.Helper()
	for _, chunk := range chunks {
		if _, err := conn.Write(chunk); err != nil {
			t.Fatalf("写入 %d 字节失败: %v", len(chunk), err)
		}
	}
}

// TestTCPClientServerSendReceive 验证明文 TCP 链路的完整往返：
// DialTCP 建连后客户端进入 Ready；客户端发出的 Body/Cmd/Act 在服务端一致到达；
// 服务端回包能被客户端收到；两端 OnConnected 各触发一次。
func TestTCPClientServerSendReceive(t *testing.T) {
	serverHandler := &recvHandler{replyBody: []byte("pong")}
	clientHandler := &recvHandler{}
	config := newTCPTestConfig(freeTCPAddress(t))
	stop := startTCPTestServer(t, config, serverHandler)
	defer stop()

	client := mustDialTCPWithRetry(t, clientHandler, config)
	t.Cleanup(func() { client.Close(nil) })

	if client.State() != ConnectionStateReady {
		t.Fatalf("客户端状态 = %s, 期望 Ready", client.State())
	}
	if err := client.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, []byte("ping"))); err != nil {
		t.Fatalf("客户端发送失败: %v", err)
	}

	serverFrames := serverHandler.waitReceivedFrames(t, 1)
	if got := bodyOf(serverFrames[0]); got != "ping" {
		t.Fatalf("服务端收到 Body = %q, 期望 %q", got, "ping")
	}
	if serverFrames[0].Cmd != tcpTestCmd || serverFrames[0].Act != tcpTestAct {
		t.Fatalf("服务端收到 Cmd/Act = %d/%d, 期望 %d/%d",
			serverFrames[0].Cmd, serverFrames[0].Act, tcpTestCmd, tcpTestAct)
	}

	clientFrames := clientHandler.waitReceivedFrames(t, 1)
	if got := bodyOf(clientFrames[0]); got != "pong" {
		t.Fatalf("客户端收到 Body = %q, 期望 %q", got, "pong")
	}

	if got := serverHandler.connectedCount(); got != 1 {
		t.Fatalf("服务端 OnConnected 调用 %d 次, 期望 1 次", got)
	}
	if got := clientHandler.connectedCount(); got != 1 {
		t.Fatalf("客户端 OnConnected 调用 %d 次, 期望 1 次", got)
	}
}

// TestTCPStickyPackets 验证粘包：把 3 个完整帧拼成一次 Write，
// 服务端必须按发送顺序拆出 3 个帧且 Body 各自正确（不能丢帧或错位）。
func TestTCPStickyPackets(t *testing.T) {
	handler := &recvHandler{}
	_, client := tcpTestConn(t, newTCPTestConfig(""), handler, tcpTestWait)

	bodies := []string{"sticky-1", "sticky-2", "sticky-3"}
	var stream []byte
	for _, body := range bodies {
		stream = append(stream, mustEncodeFrame(t, tcpTestCmd, tcpTestAct, []byte(body))...)
	}
	writeAll(t, client, stream)

	frames := handler.waitReceivedFrames(t, len(bodies))
	for i, want := range bodies {
		if got := bodyOf(frames[i]); got != want {
			t.Fatalf("第 %d 帧 Body = %q, 期望 %q", i, got, want)
		}
	}
}

// TestTCPPartialFrame 验证拆包/半包：把一帧拆成帧头中间断开的两段写入，
// 数据不完整时不能投递消息，拼齐后必须恰好收到 1 帧且 Body 完整。
func TestTCPPartialFrame(t *testing.T) {
	handler := &recvHandler{}
	_, client := tcpTestConn(t, newTCPTestConfig(""), handler, tcpTestWait)

	wantBody := "partial-frame-body"
	data := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, []byte(wantBody))

	split := 8 // 落在 17 字节帧头内部，保证第一次写入只是半包
	writeAll(t, client, data[:split])
	time.Sleep(50 * time.Millisecond)
	if got := handler.frameCount(); got != 0 {
		t.Fatalf("半包阶段不应投递消息, 实际收到 %d 个", got)
	}

	writeAll(t, client, data[split:])
	frames := handler.waitReceivedFrames(t, 1)
	if got := bodyOf(frames[0]); got != wantBody {
		t.Fatalf("拼齐后 Body = %q, 期望 %q", got, wantBody)
	}
	time.Sleep(50 * time.Millisecond)
	if got := handler.frameCount(); got != 1 {
		t.Fatalf("拼齐后应只收到 1 帧, 实际 %d 帧", got)
	}
}

// TestTCPFrameLimit 验证帧上限采用「整帧 = Body + 17 字节帧头」口径：
// MaxInboundSize=32768 时，32768 字节整帧（Body 32751）必须能正常收到；
// 32785 字节整帧（Body 32768）必须被拒绝、连接被关闭且该帧不会投递给业务；
// 编解码辅助函数对同一数据同样返回 ErrFrameTooLarge。
func TestTCPFrameLimit(t *testing.T) {
	const maxFrame = 32 * netutil.KB

	// 按整帧口径构造 Body：整帧 = 帧头 17 + Body。
	makeBody := func(bodyLen int) []byte {
		body := make([]byte, bodyLen)
		for i := range body {
			body[i] = byte('a' + i%26)
		}
		return body
	}

	if size := protocol.FrameSize(maxFrame - protocol.UnifiedFrameHeaderLen); size != maxFrame {
		t.Fatalf("FrameSize(%d) = %d, 期望 %d", maxFrame-protocol.UnifiedFrameHeaderLen, size, maxFrame)
	}

	validBody := makeBody(maxFrame - protocol.UnifiedFrameHeaderLen)
	validFrame, err := protocol.EncodeMessageFrameWithLimit(
		protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, validBody), maxFrame)
	if err != nil {
		t.Fatalf("32768 字节整帧编码失败: %v", err)
	}
	if len(validFrame) != maxFrame {
		t.Fatalf("合法整帧长度 = %d, 期望 %d", len(validFrame), maxFrame)
	}
	if _, _, err = protocol.DecodeMessageFrameWithLimit(validFrame, maxFrame); err != nil {
		t.Fatalf("32768 字节整帧应可解码, 实际错误: %v", err)
	}

	oversizeBody := makeBody(maxFrame)
	if _, err = protocol.EncodeMessageFrameWithLimit(
		protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, oversizeBody), maxFrame); !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Fatalf("32785 字节整帧编码错误 = %v, 期望 %v", err, protocol.ErrFrameTooLarge)
	}
	oversizeFrame := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, oversizeBody)
	if len(oversizeFrame) != maxFrame+protocol.UnifiedFrameHeaderLen {
		t.Fatalf("超限整帧长度 = %d, 期望 %d", len(oversizeFrame), maxFrame+protocol.UnifiedFrameHeaderLen)
	}
	if _, _, err = protocol.DecodeMessageFrameWithLimit(oversizeFrame, maxFrame); !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Fatalf("32785 字节整帧解码错误 = %v, 期望 %v", err, protocol.ErrFrameTooLarge)
	}

	// 传输层实测：合法的 32768 字节整帧能完整收到。
	validHandler := &recvHandler{}
	validConfig := newTCPTestConfig("")
	validConfig.MaxInboundSize = maxFrame
	validConfig.Normalize()
	_, validClient := tcpTestConn(t, validConfig, validHandler, tcpTestWait)
	writeAll(t, validClient, validFrame)
	validFrames := validHandler.waitReceivedFrames(t, 1)
	if got := len(validFrames[0].Body); got != len(validBody) {
		t.Fatalf("合法整帧 Body 长度 = %d, 期望 %d", got, len(validBody))
	}

	// 传输层实测：32785 字节整帧必须被拒绝并断开连接。
	overHandler := &recvHandler{}
	overConfig := newTCPTestConfig("")
	overConfig.MaxInboundSize = maxFrame
	overConfig.Normalize()
	_, overClient := tcpTestConn(t, overConfig, overHandler, tcpTestWait)
	// 服务端解析到超限帧后立即关闭连接，客户端写入可能拿到 RST，这里忽略写错误。
	_, _ = overClient.Write(oversizeFrame)
	overHandler.waitClosedError(t, protocol.ErrFrameTooLarge)
	if got := overHandler.frameCount(); got != 0 {
		t.Fatalf("超限帧不应投递给业务, 实际收到 %d 个", got)
	}
}

// TestTCPReadBufferCapLeavesRoom 验证接收缓冲区容量口径：
// Normalize 后 ReadBufferCap 至少为 MaxInboundSize + tcpReadChunkSize；
// 「最大合法帧的半包残留 + 一个完整读块」不会触发 ErrBufferOverLimit；
// 并且同一批数据里「最大合法整帧 + 后续小帧」能被完整解析、按序投递。
func TestTCPReadBufferCapLeavesRoom(t *testing.T) {
	config := newTCPTestConfig("")
	minCap := config.MaxInboundSize + tcpReadChunkSize
	if config.ReadBufferCap < minCap {
		t.Fatalf("ReadBufferCap = %d, 期望至少 %d (MaxInboundSize %d + 读块 %d)",
			config.ReadBufferCap, minCap, config.MaxInboundSize, tcpReadChunkSize)
	}

	// 极端场景：缓冲区里残留「最大整帧 - 1」字节半包，同一读块又读入 tcpReadChunkSize 字节。
	largeBody := make([]byte, config.MaxInboundSize-protocol.UnifiedFrameHeaderLen)
	for i := range largeBody {
		largeBody[i] = byte(i % 251)
	}
	largeFrame := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, largeBody)
	if len(largeFrame) != config.MaxInboundSize {
		t.Fatalf("大帧长度 = %d, 期望 %d", len(largeFrame), config.MaxInboundSize)
	}
	smallFrame := mustEncodeFrame(t, tcpTestCmd, tcpTestAct, []byte("tail"))

	readChunk := make([]byte, tcpReadChunkSize)
	readChunk[0] = largeFrame[len(largeFrame)-1]
	copy(readChunk[1:], smallFrame)

	raw := buffer.New(4096, config.ReadBufferCap)
	if _, err := raw.Write(largeFrame[:len(largeFrame)-1]); err != nil {
		t.Fatalf("写入半包失败: %v", err)
	}
	if _, err := raw.Write(readChunk); err != nil {
		if errors.Is(err, buffer.ErrBufferOverLimit) {
			t.Fatalf("合法数据触发缓冲区超限: ReadBufferCap %d, 半包 %d + 读块 %d",
				config.ReadBufferCap, len(largeFrame)-1, len(readChunk))
		}
		t.Fatalf("写入读块失败: %v", err)
	}
	if got, want := raw.Len(), config.MaxInboundSize-1+tcpReadChunkSize; got != want {
		t.Fatalf("缓冲区已用长度 = %d, 期望 %d", got, want)
	}

	// 端到端：最大合法整帧与后续小帧同时到达，必须都被按序解析。
	handler := &recvHandler{}
	e2eConfig := newTCPTestConfig("")
	e2eConfig.MaxInboundSize = config.MaxInboundSize
	e2eConfig.Normalize()
	_, client := tcpTestConn(t, e2eConfig, handler, tcpTestWait)
	writeAll(t, client, largeFrame, smallFrame)

	frames := handler.waitReceivedFrames(t, 2)
	if got := len(frames[0].Body); got != len(largeBody) {
		t.Fatalf("大帧 Body 长度 = %d, 期望 %d", got, len(largeBody))
	}
	if got := bodyOf(frames[1]); got != "tail" {
		t.Fatalf("后续小帧 Body = %q, 期望 %q", got, "tail")
	}
	if handler.hasClosedError(nil) {
		t.Fatalf("合法数据不应触发连接关闭")
	}
}

// TestTCPOutboundFrameLimit 验证出站上限同样按整帧口径：
// MaxOutboundSize 小于整帧长度时，SendMessage 直接返回 protocol.ErrFrameTooLarge；
// 恰好等于上限的整帧则允许发送。
func TestTCPOutboundFrameLimit(t *testing.T) {
	config := newTCPTestConfig("")
	config.MaxOutboundSize = 64
	config.Normalize()
	conn, _ := tcpTestConn(t, config, &recvHandler{}, tcpTestWait)

	// 整帧 = 64 + 17 = 81 字节，超过 64 上限。
	err := conn.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, make([]byte, 64)))
	if !errors.Is(err, protocol.ErrFrameTooLarge) {
		t.Fatalf("超限出站帧错误 = %v, 期望 %v", err, protocol.ErrFrameTooLarge)
	}

	// 恰好等于上限（Body 47 + 帧头 17 = 64）应当允许。
	if err = conn.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, make([]byte, 47))); err != nil {
		t.Fatalf("等于上限的出站帧发送失败: %v", err)
	}
}

// TestTCPInvalidCRC 验证 CRC 校验失败的处理：篡改 CRC 后服务端必须解包失败、
// 断开连接且不把该帧投递给业务。
func TestTCPInvalidCRC(t *testing.T) {
	handler := &recvHandler{}
	_, client := tcpTestConn(t, newTCPTestConfig(""), handler, tcpTestWait)

	writeAll(t, client, corruptCRC(mustEncodeFrame(t, tcpTestCmd, tcpTestAct, []byte("crc-broken"))))
	handler.waitClosed(t)
	if got := handler.frameCount(); got != 0 {
		t.Fatalf("CRC 校验失败的帧不应投递, 实际收到 %d 个", got)
	}
}

// TestTCPHandshakeWithEncryption 验证开启加密时的 ECDH 握手：
// 双方在握手完成后都进入 Ready，且加密链路仍能正常收发（对端收到的是解密后的明文）。
func TestTCPHandshakeWithEncryption(t *testing.T) {
	serverHandler := &recvHandler{replyBody: []byte("encrypted-pong")}
	clientHandler := &recvHandler{}
	config := newTCPTestConfig(freeTCPAddress(t))
	config.EncryptEnable = true
	stop := startTCPTestServer(t, config, serverHandler)
	defer stop()

	clientConfig := *config
	client := mustDialTCPWithRetry(t, clientHandler, &clientConfig)
	t.Cleanup(func() { client.Close(nil) })

	if client.State() != ConnectionStateReady {
		t.Fatalf("客户端状态 = %s, 期望 Ready", client.State())
	}
	if err := client.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, []byte("secret"))); err != nil {
		t.Fatalf("加密链路发送失败: %v", err)
	}
	serverFrames := serverHandler.waitReceivedFrames(t, 1)
	if got := bodyOf(serverFrames[0]); got != "secret" {
		t.Fatalf("服务端解密后 Body = %q, 期望 %q", got, "secret")
	}
	clientFrames := clientHandler.waitReceivedFrames(t, 1)
	if got := bodyOf(clientFrames[0]); got != "encrypted-pong" {
		t.Fatalf("客户端解密后 Body = %q, 期望 %q", got, "encrypted-pong")
	}
}

// TestTCPHeartbeatTimeout 验证心跳超时：客户端心跳设为 200ms 且保持空闲后，
// 客户端连接必须在心跳窗口之后被关闭，且 OnClose 稳定收到 ErrHeartbeatTimeout。
// 服务端心跳刻意保持较长，避免两端同时超时导致错误来源不确定。
//
// 这里刻意断言具体错误值：runWriteLoop 关闭时先 Close(err) 再 closeSocket()，
// 读循环不会再抢先以 socket 错误赢得状态切换。若将来关闭顺序被改回去，
// 该断言会失败，正是我们希望守住的回归点。
func TestTCPHeartbeatTimeout(t *testing.T) {
	serverConfig := newTCPTestConfig(freeTCPAddress(t))
	stop := startTCPTestServer(t, serverConfig, &recvHandler{})
	defer stop()

	clientHandler := &recvHandler{}
	clientConfig := *serverConfig
	clientConfig.HeartbeatTimeout = 200 * time.Millisecond
	clientConfig.Normalize()
	start := time.Now()
	client := mustDialTCPWithRetry(t, clientHandler, &clientConfig)
	t.Cleanup(func() { client.Close(nil) })

	clientHandler.waitClosedError(t, ErrHeartbeatTimeout)
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("连接在 %s 就关闭了, 早于心跳窗口 200ms", elapsed)
	}
	if client.State() != ConnectionStateClosed {
		t.Fatalf("心跳超时后客户端状态 = %s, 期望 Closed", client.State())
	}
}

// TestTCPSendChannelFull 验证发送队列背压与关闭语义：
// 写循环未启动时，队列写满后 SendMessage 返回 ErrNetworkChannelFull；
// 连接关闭后 SendMessage 返回 ErrConnectionClosed。
func TestTCPSendChannelFull(t *testing.T) {
	conn := newBaseConnWithRole(&recvHandler{}, CommonConfig{
		EncryptEnable:    false,
		HeartbeatTimeout: tcpTestHeartbeat,
		MaxInboundSize:   32 * netutil.KB,
		MaxOutboundSize:  32 * netutil.KB,
		SendChanSize:     1,
		WriteTimeout:     time.Second,
	}, ConnectionRoleServer)
	conn.logger = zap.NewNop()
	// 直接置为 Ready：本测试只关心队列与状态判断，不涉及握手。
	conn.state.Store(uint32(ConnectionStateReady))

	// 队列容量为 1：第一次入队成功，第二次必然返回队列满。
	if err := conn.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, []byte("first"))); err != nil {
		t.Fatalf("第一次发送失败: %v", err)
	}
	if err := conn.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, []byte("second"))); !errors.Is(err, ErrNetworkChannelFull) {
		t.Fatalf("队列满错误 = %v, 期望 %v", err, ErrNetworkChannelFull)
	}

	conn.Close(nil)
	if err := conn.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, []byte("after-close"))); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("关闭后发送错误 = %v, 期望 %v", err, ErrConnectionClosed)
	}
}

// TestTCPDialFailure 验证拨号失败路径：目标端口没有监听时 DialTCP 应返回错误而不是挂起。
func TestTCPDialFailure(t *testing.T) {
	address := freeTCPAddress(t) // 端口已释放，没有服务端监听
	config := newTCPTestConfig(address)

	start := time.Now()
	conn, err := DialTCP(context.Background(), 500*time.Millisecond, &recvHandler{}, config, nil)
	if err == nil {
		conn.Close(nil)
		t.Fatalf("拨号到未监听地址 %s 竟然成功", address)
	}
	if elapsed := time.Since(start); elapsed > tcpTestWait {
		t.Fatalf("拨号失败耗时 %s, 期望快速返回", elapsed)
	}
}

// TestTCPServerContextCancel 验证服务端生命周期：取消 ctx 后 Run 必须返回，
// 且监听端口被释放，可以立即被重新监听。
func TestTCPServerContextCancel(t *testing.T) {
	address := freeTCPAddress(t)
	config := newTCPTestConfig(address)
	stop := startTCPTestServer(t, config, &recvHandler{})

	// 先确认服务端已就绪，覆盖「运行中取消」的路径。
	client := mustDialTCPWithRetry(t, &recvHandler{}, config)
	client.Close(nil)

	stop()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("取消 ctx 后端口 %s 未释放: %v", address, err)
	}
	_ = listener.Close()
}

// TestTCPServerStopKeepsConnections 验证「服务端停止只关监听、不断已有连接」这一约定：
// 取消 ctx 让 Run 返回后，先前建立的连接必须仍然 Ready，且还能继续把消息投递给服务端 handler。
// 已有连接的关闭时机由上层业务决定，不由服务端 Run 的生命周期决定。
func TestTCPServerStopKeepsConnections(t *testing.T) {
	address := freeTCPAddress(t)
	config := newTCPTestConfig(address)
	serverHandler := &recvHandler{}
	stop := startTCPTestServer(t, config, serverHandler)

	clientHandler := &recvHandler{}
	clientConfig := *config
	client := mustDialTCPWithRetry(t, clientHandler, &clientConfig)
	t.Cleanup(func() { client.Close(nil) })

	// 服务端 Run 已返回，监听端口被关闭，但已建立的连接应保持存活。
	stop()
	if got := client.State(); got != ConnectionStateReady {
		t.Fatalf("服务端 Run 返回后客户端状态 = %s, 期望 Ready", got)
	}
	if err := client.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, []byte("after-stop"))); err != nil {
		t.Fatalf("服务端 Run 返回后连接应仍可发送, 实际错误: %v", err)
	}
	frames := serverHandler.waitReceivedFrames(t, 1)
	if got := bodyOf(frames[0]); got != "after-stop" {
		t.Fatalf("服务端 Run 返回后收到 Body = %q, 期望 %q", got, "after-stop")
	}
	if clientHandler.hasClosedError(nil) {
		t.Fatal("服务端 Run 返回不应关闭已建立的连接")
	}
}

// TestTCPHeartbeatIgnoresOutboundTraffic 验证心跳活跃度只由「入站数据」刷新：
// 客户端持续发送、服务端只收不回时，客户端因为没有收到任何数据，
// 仍会在心跳窗口到期后以 ErrHeartbeatTimeout 关闭自己（出站流量不算活跃）。
// 同时断言服务端确实收到了这些出站帧，证明连接本身是健康的。
func TestTCPHeartbeatIgnoresOutboundTraffic(t *testing.T) {
	const timeout = 400 * time.Millisecond

	config := newTCPTestConfig(freeTCPAddress(t))
	config.HeartbeatTimeout = timeout
	config.Normalize()
	serverHandler := &recvHandler{} // 不回复，客户端始终收不到入站数据
	stop := startTCPTestServer(t, config, serverHandler)
	defer stop()

	clientHandler := &recvHandler{}
	clientConfig := *config
	client := mustDialTCPWithRetry(t, clientHandler, &clientConfig)
	t.Cleanup(func() { client.Close(nil) })

	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(timeout / 2)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = client.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, nil))
			}
		}
	}()

	clientHandler.waitClosedError(t, ErrHeartbeatTimeout)
	if got := serverHandler.frameCount(); got == 0 {
		t.Fatal("服务端应收到客户端的出站帧，实际一帧未收到")
	}
}

// TestTCPMultipleClients 验证并发多客户端：10 个客户端各自发送不同 Body，
// 服务端原样回显，每个客户端只应收到自己发送的 Body，不能发生串包。
func TestTCPMultipleClients(t *testing.T) {
	const clientCount = 10

	serverHandler := &recvHandler{echo: true}
	config := newTCPTestConfig(freeTCPAddress(t))
	stop := startTCPTestServer(t, config, serverHandler)
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			clientHandler := &recvHandler{}
			clientConfig := *config
			conn, err := dialTCPWithRetry(clientHandler, &clientConfig)
			if err != nil {
				t.Errorf("客户端 %d 拨号失败: %v", index, err)
				return
			}
			defer conn.Close(nil)

			wantBody := fmt.Sprintf("client-%d", index)
			if err := conn.SendMessage(protocol.NewMessageFrame(tcpTestCmd, tcpTestAct, 0, []byte(wantBody))); err != nil {
				t.Errorf("客户端 %d 发送失败: %v", index, err)
				return
			}

			deadline := time.Now().Add(tcpTestWait)
			for {
				frames := clientHandler.receivedFrames()
				if len(frames) > 0 {
					if got := bodyOf(frames[0]); got != wantBody {
						t.Errorf("客户端 %d 收到 Body = %q, 期望 %q", index, got, wantBody)
					}
					return
				}
				if time.Now().After(deadline) {
					t.Errorf("客户端 %d 等待回显超时", index)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	if got := serverHandler.frameCount(); got != clientCount {
		t.Fatalf("服务端收到 %d 个消息, 期望 %d 个", got, clientCount)
	}
}
