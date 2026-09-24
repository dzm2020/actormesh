package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

const UnifiedFrameHeaderLen = 17

var (
	// ErrFrameTooLarge 表示整帧长度（Body + 帧头）超过配置上限。
	ErrFrameTooLarge = errors.New("protocol: frame exceeds configured limit")
	// ErrBodyTooLarge 为兼容保留的别名，语义与 ErrFrameTooLarge 一致。
	// Deprecated: 使用 ErrFrameTooLarge。
	ErrBodyTooLarge = ErrFrameTooLarge
)

// FrameSize 返回整帧长度：Body 长度 + 统一帧头长度。
// 网络层所有协议帧大小上限（MaxInboundSize / MaxOutboundSize）
// 都以整帧长度为口径。
func FrameSize(bodyLen int) int {
	return UnifiedFrameHeaderLen + bodyLen
}

func EncodeMessageFrame(frame *MessageFrame) ([]byte, error) {
	return EncodeMessageFrameWithLimit(frame, 0)
}

// EncodeMessageFrameWithLimit 按整帧长度校验上限，maxFrameSize <= 0 表示不限制。
func EncodeMessageFrameWithLimit(frame *MessageFrame, maxFrameSize int) ([]byte, error) {
	if IsInternalCommand(frame.Cmd) {
		if err := ValidateInternalAction(frame.Act); err != nil {
			return nil, err
		}
	} else if err := ValidateBusinessCommand(frame.Cmd, frame.Act); err != nil {
		return nil, err
	}
	frameSize := FrameSize(len(frame.Body))
	if frameSize > math.MaxInt32 {
		return nil, ErrFrameTooLarge
	}
	if maxFrameSize > 0 && frameSize > maxFrameSize {
		return nil, ErrFrameTooLarge
	}
	crc := crc32.ChecksumIEEE(frame.Body)
	out := make([]byte, frameSize)
	binary.BigEndian.PutUint32(out[:4], uint32(len(frame.Body)))
	out[4] = frame.Cmd
	out[5] = frame.Act
	out[6] = frame.Flags
	binary.BigEndian.PutUint16(out[7:9], frame.Error)
	binary.BigEndian.PutUint32(out[9:13], frame.Index)
	binary.BigEndian.PutUint32(out[13:17], crc)
	copy(out[UnifiedFrameHeaderLen:], frame.Body)
	return out, nil
}

func DecodeMessageFrame(data []byte) (*MessageFrame, int, error) {
	return DecodeMessageFrameWithLimit(data, 0)
}

// DecodeMessageFrameWithLimit 按整帧长度校验上限，maxFrameSize <= 0 表示不限制。
// consumed == 0 表示数据不足（半包），需要等待更多数据。
func DecodeMessageFrameWithLimit(data []byte, maxFrameSize int) (*MessageFrame, int, error) {
	if len(data) < UnifiedFrameHeaderLen {
		return nil, 0, nil
	}

	rawLength := binary.BigEndian.Uint32(data[:4])
	if rawLength > uint32(math.MaxInt32-UnifiedFrameHeaderLen) {
		return nil, UnifiedFrameHeaderLen, ErrFrameTooLarge
	}
	payloadLength := int(rawLength)
	frameSize := FrameSize(payloadLength)

	if maxFrameSize > 0 && frameSize > maxFrameSize {
		return nil, UnifiedFrameHeaderLen, ErrFrameTooLarge
	}

	total := frameSize
	if total > len(data) {
		return nil, 0, nil
	}

	frame := &MessageFrame{
		Cmd:   data[4],
		Act:   data[5],
		Flags: data[6],
		Error: binary.BigEndian.Uint16(data[7:9]),
		Index: binary.BigEndian.Uint32(data[9:13]),
		crc:   binary.BigEndian.Uint32(data[13:17]),
		Body:  append([]byte(nil), data[UnifiedFrameHeaderLen:total]...),
	}
	bodyCrc := crc32.ChecksumIEEE(frame.Body)
	if bodyCrc != frame.crc {
		return nil, total, fmt.Errorf("invalid crc  headCrc:%d bodyCrc:%d", frame.crc, bodyCrc)
	}

	if frame.Cmd == InternalCmd {
		if err := ValidateInternalAction(frame.Act); err != nil {
			return nil, total, err
		}
	} else {
		if err := ValidateBusinessCommand(frame.Cmd, frame.Act); err != nil {
			return nil, total, err
		}
	}
	return frame, total, nil
}
