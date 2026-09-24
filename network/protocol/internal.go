package protocol

import "errors"

const (
	// InternalCmd is reserved for messages consumed by the network layer.
	InternalCmd         uint8 = 0
	InternalKeyExchange uint8 = 1
	InternalActClose    uint8 = 2
)

var (
	ErrReservedCommand    = errors.New("protocol: command is reserved for network internals")
	ErrUnknownInternalAct = errors.New("protocol: unknown internal action")
)

func IsInternalCommand(cmd uint8) bool {
	return cmd == InternalCmd
}

func ValidateBusinessCommand(cmd, _ uint8) error {
	if IsInternalCommand(cmd) {
		return ErrReservedCommand
	}
	return nil
}

func ValidateInternalAction(act uint8) error {
	switch act {
	case InternalKeyExchange,
		InternalActClose:
		return nil
	default:
		return ErrUnknownInternalAct
	}
}
