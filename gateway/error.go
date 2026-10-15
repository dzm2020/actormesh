package gateway

import (
	"errors"

	"github.com/dzm2020/actormesh/actor"
)

var (
	ErrAgentRequestNil      = errors.New("gateway agent has no active client request")
	ErrAgentRouteNil        = errors.New("gateway agent route is nil")
	ErrClientPushMessageNil = errors.New("gateway client push message is nil")
)

type ClientErrorCode = uint16

const (
	ClientErrorNone ClientErrorCode = iota
	ClientErrorInternal
)

func mapToClientCode(err error) ClientErrorCode {
	code, ok := actor.CodeOf(err)
	if !ok || code == actor.ErrorCodeUnknown || uint64(code) > uint64(^ClientErrorCode(0)) {
		return ClientErrorInternal
	}
	return ClientErrorCode(code)
}
