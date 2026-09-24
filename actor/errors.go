package actor

import (
	"errors"
)

// ErrorCode 表示可跨节点保留的稳定错误标识。
type ErrorCode uint32

const (
	ErrorCodeUnknown             ErrorCode = 0
	ErrorCodeActorNotFound       ErrorCode = 1001
	ErrorCodeActorProcessStopped ErrorCode = 1002
	ErrorCodeActorMailboxFull    ErrorCode = 1003
)

// CodedError 提供独立于错误描述的稳定错误码。
type CodedError interface {
	error
	Code() ErrorCode
}

type codedError struct {
	code    ErrorCode
	message string
}

func NewError(code ErrorCode, message string) CodedError {
	return &codedError{code: code, message: message}
}

func (e *codedError) Error() string {
	return e.message
}

func (e *codedError) Code() ErrorCode {
	return e.code
}

func (e *codedError) Is(target error) bool {
	if e.code == ErrorCodeUnknown {
		return false
	}
	targetCode, ok := CodeOf(target)
	return ok && e.code == targetCode
}

// CodeOf 返回 err 包装链中首个非零错误码。
func CodeOf(err error) (ErrorCode, bool) {
	var coded CodedError
	if !errors.As(err, &coded) || coded.Code() == ErrorCodeUnknown {
		return ErrorCodeUnknown, false
	}
	return coded.Code(), true
}

// 需要跨节点传播的错误
var (
	ErrActorProcessStopped = NewError(ErrorCodeActorProcessStopped, "actor process is stopped")
	ErrNotFound            = NewError(ErrorCodeActorNotFound, "actor not found")
	ErrMailboxFull         = NewError(ErrorCodeActorMailboxFull, "mailbox is full")
)
var (
	ErrPidNil                      = errors.New("actor pid is nil")
	ErrHandlerNil                  = errors.New("actor handler is nil")
	ErrNameExists                  = errors.New("actor name already exists")
	ErrNotLocal                    = errors.New("actor not local")
	ErrRequestNil                  = errors.New("actor request nil")
	ErrRespondErrorNil             = errors.New("actor respondError requires a non-nil error")
	ErrAlreadyResponded            = errors.New("actor message already responded")
	ErrAskTimeout                  = errors.New("actor ask timeout")
	ErrAskCompletionNil            = errors.New("actor ask completion callback is nil")
	ErrRequestLimit                = errors.New("actor pending request limit reached")
	ErrRequestManagerClosed        = errors.New("actor request manager is closed")
	ErrActorTargetNil              = errors.New("actor message target pid is nil")
	ErrActorSenderNil              = errors.New("actor message sender pid is nil")
	ErrActorEnvelopeRequestInvalid = errors.New("actor envelope request is invalid")
	ErrRemoteMessageKindInvalid    = errors.New("remote actor message kind is invalid")
	ErrRemoteMessageBodyInvalid    = errors.New("remote actor message body is invalid")
	ErrRemoteCommonInvalid         = errors.New("remote actor common is invalid")
	ErrRemoteMessageNil            = errors.New("remote actor message is nil")
	ErrRemoteSourceNodeInvalid     = errors.New("remote actor source node is invalid")
	ErrRemoteSourcePIDInvalid      = errors.New("remote actor source PID is invalid")
	ErrRequestNotExist             = errors.New("remote actor request not exist")
	ErrRemoteSenderNil             = errors.New("remote actor sender is nil")
)
