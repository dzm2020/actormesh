package protocol

import (
	"encoding/binary"
	"errors"
)

const UDPHeaderLen = 16 // session id(8) + sequence(8)

var ErrInvalidUDPHeader = errors.New("invalid UDP header")

type UDPHeader struct {
	SessionID uint64 // 用于区分同一ip 地址不同连接
	Sequence  uint64
}

func EncodeUDPHeader(header UDPHeader) ([]byte, error) {
	if header.SessionID == 0 {
		return nil, ErrInvalidUDPHeader
	}
	out := make([]byte, UDPHeaderLen)
	binary.BigEndian.PutUint64(out[:8], header.SessionID)
	binary.BigEndian.PutUint64(out[8:], header.Sequence)
	return out, nil
}

func DecodeUDPHeader(data []byte) (UDPHeader, error) {
	if len(data) < UDPHeaderLen {
		return UDPHeader{}, ErrInvalidUDPHeader
	}
	header := UDPHeader{
		SessionID: binary.BigEndian.Uint64(data[:8]),
		Sequence:  binary.BigEndian.Uint64(data[8:UDPHeaderLen]),
	}
	if header.SessionID == 0 {
		return UDPHeader{}, ErrInvalidUDPHeader
	}
	return header, nil
}
