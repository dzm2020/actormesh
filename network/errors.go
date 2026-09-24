package network

import "errors"

var (
	ErrUnexpectedTCPConnType   = errors.New("unexpected tcp connection type")
	ErrInvalidFrameConsumeSize = errors.New("invalid frame consume size")
	ErrConnectionClosed        = errors.New("connection is closed")
	ErrNetworkChannelFull      = errors.New("connection  send channel full")
	ErrInvalidConnectionState  = errors.New("invalid connection state")
	ErrHandshakeNotComplete    = errors.New("connection handshake is not complete")
	ErrHeartbeatTimeout        = errors.New("heartbeat timeout")
)
