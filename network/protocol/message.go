package protocol

import "bytes"

type MessageFrame struct {
	Cmd   uint8
	Act   uint8
	Flags uint8
	Error uint16
	Index uint32
	crc   uint32 // 非导出字段避免被外部误用
	Body  []byte
}

func NewMessageFrame(cmd, act, flags uint8, body []byte) *MessageFrame {
	return &MessageFrame{
		Cmd:   cmd,
		Act:   act,
		Flags: flags,
		Body:  bytes.Clone(body),
	}
}

func (m *MessageFrame) Clone() *MessageFrame {
	c := *m
	c.Body = bytes.Clone(m.Body)
	return &c
}

func (m *MessageFrame) ID() uint16 {
	return CmdAct(m.Cmd, m.Act)
}

func CmdAct(cmd, act uint8) uint16 {
	return (uint16(cmd) << 8) | uint16(act)
}
