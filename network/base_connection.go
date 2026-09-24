package network

import (
	"context"
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/network/encrypt"
	"github.com/dzm2020/actormesh/network/protocol"
	"github.com/dzm2020/actormesh/pkg/buffer"
	"github.com/dzm2020/actormesh/pkg/glog"
	"github.com/dzm2020/actormesh/pkg/grs"
	"github.com/dzm2020/actormesh/pkg/timer"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

func newBaseConnWithRole(handler TransportHandler, options CommonConfig, role ConnectionRole) *baseConn {
	id := generateConnectionID()
	connection := &baseConn{
		id:          id,
		handler:     handler,
		role:        role,
		readyCh:     make(chan bool, 1),
		sendChannel: make(chan []byte, options.SendChanSize),
		options:     options,
	}
	connection.ctx, connection.cancel = context.WithCancel(context.Background())
	connection.group = grs.NewGroup(connection.ctx)
	connection.state.Store(uint32(ConnectionStateCreated))
	connection.touch()
	return connection
}

type baseConn struct {
	id             int64
	role           ConnectionRole
	state          atomic.Uint32
	handler        TransportHandler
	userData       atomic.Pointer[any]
	sendChannel    chan []byte
	readyCh        chan bool
	cipher         *encrypt.ECDHCipher
	options        CommonConfig
	lastActive     atomic.Pointer[time.Time]
	ctx            context.Context
	cancel         context.CancelFunc
	group          *grs.Group
	logger         *zap.Logger
	conn           connCore // 子类接口实现
	closeErr       error
	handshakeTimer atomic.Pointer[timer.Timer]
}

func (b *baseConn) bind(conn connCore) {
	b.conn = conn
	b.logger = glog.With(zap.Int64("connId", conn.ID()),
		zap.Uint8("role", uint8(conn.Role())),
		zap.String("network", conn.Network()),
		zap.String("remoteAddr", conn.RemoteAddr()),
		zap.String("localAddr", conn.LocalAddr()),
	)
}

func (b *baseConn) ID() int64 {
	return b.id
}

func (b *baseConn) Context() context.Context {
	return b.ctx
}

func (b *baseConn) SetUserData(data any) {
	if data == nil {
		return
	}
	b.userData.Store(&data)
}

func (b *baseConn) UserData() any {
	ptr := b.userData.Load()
	if ptr == nil {
		return nil
	}
	return *ptr
}

func (b *baseConn) touch() {
	now := time.Now()
	b.lastActive.Store(&(now))
}

func (b *baseConn) Role() ConnectionRole {
	return b.role
}

func (b *baseConn) Log() *zap.Logger {
	return b.logger
}

func (b *baseConn) State() ConnectionState {
	return ConnectionState(b.state.Load())
}

func (b *baseConn) encrypt(plaintext []byte) ([]byte, error) {
	if b.State() != ConnectionStateReady {
		return nil, fmt.Errorf("encrypt %w", ErrHandshakeNotComplete)
	}
	if !b.options.EncryptEnable {
		return plaintext, nil
	}

	return b.cipher.Encrypt(plaintext)
}

func (b *baseConn) decrypt(ciphertext []byte) ([]byte, error) {
	if !b.options.EncryptEnable {
		return ciphertext, nil
	}
	if b.State() != ConnectionStateReady {
		return nil, fmt.Errorf("decrypt %w", ErrHandshakeNotComplete)
	}
	return b.cipher.Decrypt(ciphertext)
}

func (b *baseConn) checkHeartbeatTimeout() error {
	//  握手期间不进行心跳检查
	if b.State() == ConnectionStateHandshaking {
		return nil
	}
	lastActive := *(b.lastActive.Load())
	if time.Since(lastActive) >= b.options.HeartbeatTimeout {
		b.logger.Error("connection heartbeat timed out",
			zap.Duration("timeout", b.options.HeartbeatTimeout),
			zap.Error(ErrHeartbeatTimeout),
		)
		return ErrHeartbeatTimeout
	}
	return nil
}

// baseConn 提供
func (b *baseConn) runStart() error {
	b.group.Go(func(ctx context.Context) {
		b.runWriteLoop() // ← 子类不需要再实现
	})
	if err := b.start(b.conn); err != nil { // 握手
		b.conn.Close(err)
		return err
	}
	b.group.Go(func(ctx context.Context) {
		b.conn.(interface{ readLoop() }).readLoop() // ← 子类实现
	})
	return nil
}

// 通用写循环
func (b *baseConn) runWriteLoop() {
	var err error
	defer func() {
		b.drainMessages()   // 排水消息
		b.conn.Close(err)   // 执行关闭逻辑
		grs.SafeGo(func() { // 回调
			b.handler.OnClose(b.conn, b.closeErr)
		})
		b.conn.closeSocket() // 关闭原始socket连接
	}()
	ticker := time.NewTicker(b.options.HeartbeatTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			if err = b.checkHeartbeatTimeout(); err != nil {
				return
			}
		case message, ok := <-b.sendChannel:
			if !ok || message == nil {
				return
			}
			if err = b.conn.writeRaw(message); err != nil {
				return
			}
		}
	}
}

// 通用排空
func (b *baseConn) drainMessages() {
	for {
		select {
		case message, ok := <-b.sendChannel:
			if !ok || message == nil {
				return
			}
			if err := b.conn.writeRaw(message); err != nil {
				return
			}
		default:
			return
		}
	}
}

func (b *baseConn) start(conn Connection) error {
	if !b.state.CompareAndSwap(uint32(ConnectionStateCreated), uint32(ConnectionStateHandshaking)) {
		b.logger.Warn("connection not created", zap.String("state", b.State().String()))
		return ErrInvalidConnectionState
	}
	if !b.options.EncryptEnable {
		if !b.state.CompareAndSwap(uint32(ConnectionStateHandshaking), uint32(ConnectionStateReady)) {
			b.logger.Warn("connection not handshaking", zap.String("state", b.State().String()))
			return ErrInvalidConnectionState
		}
		return b.onReady(conn)
	} else {
		return b.startHandshake(conn)
	}
}

func (b *baseConn) startHandshake(conn Connection) error {
	var err error
	b.cipher, err = encrypt.NewECDHCipher()
	if err != nil {
		b.logger.Warn("connection start encryption failed", zap.Error(err))
		return err
	}

	if conn.Role() == ConnectionRoleClient {
		if err = b.sendExchangeKey(); err != nil {
			return err
		}
	}
	//  处理握手超时
	handshakeTimer := timer.After(b.options.HandshakeTimeout, func() {
		conn.Close(errors.New("handshake timeout"))
		return
	})
	b.handshakeTimer.Store(handshakeTimer)
	return nil
}

func (b *baseConn) onReady(conn Connection) error {
	b.logger.Info("connection ready  success")
	if t := b.handshakeTimer.Load(); t != nil {
		t.Stop()
	}
	if err := b.handler.OnConnected(conn); err != nil {
		b.logger.Error("connection OnConnected handler failed", zap.Error(err))
		return err
	}
	close(b.readyCh)
	return nil
}

func (b *baseConn) onProcess(conn Connection, data []byte) (int, error) {
	frame, consumed, err := protocol.DecodeMessageFrameWithLimit(data, b.options.MaxInboundSize)
	if err != nil {
		b.logger.Error("connection onProcess decode failed", zap.Int("bytes_consumed", consumed), zap.Error(err))
		return consumed, err
	}
	//  半包
	if consumed == 0 {
		return consumed, nil
	}

	b.logger.Debug("connection process", zap.Any("frame", frame))

	b.touch()
	if err = b.prepareMessageFrame(frame); err != nil {
		return consumed, err
	}
	if err = b.processFrame(conn, frame); err != nil {
		return consumed, err
	}
	return consumed, nil
}

// processReadBuffer 循环消费缓冲区中的完整帧（处理粘包），
// 遇到半包时保留剩余数据等待后续读取。
// 缓冲区上限需保证「最大整帧 + 一次读入量」都能容纳，否则合法帧也会被判定为超限。
func (b *baseConn) processReadBuffer(buf buffer.IBuffer) error {
	for {
		n, err := b.onProcess(b.conn, buf.Bytes())
		if err != nil {
			return err
		}
		if n < 0 || n > buf.Len() {
			b.logger.Error("connection process packet read buffer overflow", zap.Int("buffer", n))
			return ErrInvalidFrameConsumeSize
		}
		// 半包，等待更多数据
		if n == 0 {
			return nil
		}
		if err = buf.Skip(n); err != nil {
			b.logger.Error("connection process receive buffer advance failed", zap.Error(err))
			return err
		}
	}
}

func (b *baseConn) prepareMessageFrame(frame *protocol.MessageFrame) error {
	var err error
	if !protocol.IsInternalCommand(frame.Cmd) {
		frame.Body, err = b.decrypt(frame.Body)
		if err != nil {
			b.logger.Error("connection prepareMessageFrame encrypt failed", zap.Error(err))
			return err
		}
	}
	codec := b.options.Codec
	if codec != nil {
		frame.Body, err = codec.Decode(frame.Body, frame.Flags)
		if err != nil {
			b.logger.Error("connection prepareMessageFrame decode failed", zap.Error(err))
			return err
		}
	}
	return nil
}

func (b *baseConn) processFrame(conn Connection, frame *protocol.MessageFrame) error {
	if protocol.IsInternalCommand(frame.Cmd) {
		switch frame.Act {
		case protocol.InternalActClose:
			return ErrInvalidConnectionState
		case protocol.InternalKeyExchange:
			return b.handleKeyExchange(conn, frame)
		default:
			return protocol.ErrUnknownInternalAct
		}
	} else {
		if b.State() != ConnectionStateReady {
			b.logger.Error("connection process business frame before handshake")
			return ErrHandshakeNotComplete
		}
		if err := b.handler.OnMessage(conn, frame); err != nil {
			b.logger.Error("connection process business frame handler failed", zap.Error(err))
			return err
		}
	}
	return nil
}

func (b *baseConn) handleKeyExchange(conn Connection, frame *protocol.MessageFrame) error {
	if !b.state.CompareAndSwap(uint32(ConnectionStateHandshaking), uint32(ConnectionStateReady)) {
		b.logger.Warn("connection exchange key request state is not created")
		return nil
	}
	if conn.Role() == ConnectionRoleServer {
		if err := b.sendExchangeKey(); err != nil {
			return err
		}
	}
	if err := b.cipher.GenerateSharedKey(frame.Body); err != nil {
		b.logger.Error("connection exchange key  encrypt failed", zap.Error(err))
		return err
	}
	return b.onReady(conn)
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (b *baseConn) SendMessage(frame *protocol.MessageFrame) error {
	if b.State() == ConnectionStateClosed {
		return ErrConnectionClosed
	}
	return b.sendFrame(frame)
}

func (b *baseConn) sendFrame(frame *protocol.MessageFrame) error {
	if frame == nil {
		return errors.New("frame is nil")
	}
	//  避免同一个包多次
	copyFrame := *frame

	var err error
	codec := b.options.Codec
	if codec != nil {
		copyFrame.Body, copyFrame.Flags, err = codec.Encode(copyFrame.Body)
		if err != nil {
			return err
		}
	}
	if !protocol.IsInternalCommand(copyFrame.Cmd) {
		copyFrame.Body, err = b.encrypt(copyFrame.Body)
		if err != nil {
			return err
		}
	}
	bytes, err := protocol.EncodeMessageFrameWithLimit(&copyFrame, b.options.MaxOutboundSize)
	if err != nil {
		return err
	}

	b.logger.Debug("connection sendFrame", zap.Any("frame", copyFrame))

	return b.pushQueue(bytes)
}

func (b *baseConn) pushQueue(message []byte) error {
	select {
	case <-b.ctx.Done():
		b.logger.Warn("connection pushQueue ctx canceled")
		return ErrConnectionClosed
	case b.sendChannel <- message:
		return nil
	default:
		b.logger.Error("connection pushQueue send queue is full",
			zap.Int("queue_length", len(b.sendChannel)),
			zap.Int("queue_capacity", cap(b.sendChannel)),
		)
		return ErrNetworkChannelFull
	}
}

func (b *baseConn) sendExchangeKey() error {
	response := &protocol.MessageFrame{
		Cmd:  protocol.InternalCmd,
		Act:  protocol.InternalKeyExchange,
		Body: b.cipher.PublicKey(),
	}
	if err := b.sendFrame(response); err != nil {
		b.logger.Error("connection send key exchange  failed", zap.Error(err))
		return err
	}
	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (b *baseConn) Close(err error) {
	old := b.state.Swap(uint32(ConnectionStateClosed))
	if old == uint32(ConnectionStateClosed) {
		return
	}
	b.closeErr = err
	if b.cancel != nil {
		b.cancel()
	}
	if t := b.handshakeTimer.Load(); t != nil {
		(*t).Stop()
	}
	b.logger.Info("connection close", zap.Any("reason", err))
	return
}
