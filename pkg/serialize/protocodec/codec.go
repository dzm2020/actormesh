package protocodec

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	ErrMessageType         = errors.New("protobuf message not available")
	ErrMessageNil          = errors.New("protobuf message is nil")
	ErrMessageNameInvalid  = errors.New("protobuf message name is invalid")
	ErrMessageTypeNotFound = errors.New("protobuf message type is not linked")
)

func Marshal(v any) ([]byte, error) {
	if v == nil {
		return nil, ErrMessageNil
	}
	message, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrMessageType, v)
	}
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, fmt.Errorf("%w: protobuf value is nil or invalid", ErrMessageNil)
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal protobuf %T: %w", message, err)
	}
	return data, nil
}

func Unmarshal(data []byte, v any) error {
	if v == nil {
		return fmt.Errorf("%w: value is nil", ErrMessageNil)
	}
	message, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("%w: %T", ErrMessageType, v)
	}
	if message == nil || !message.ProtoReflect().IsValid() {
		return fmt.Errorf("%w: protobuf value is nil or invalid", ErrMessageNil)
	}
	if err := proto.Unmarshal(data, message); err != nil {
		return fmt.Errorf("unmarshal protobuf %T: %w", message, err)
	}
	return nil
}

func MarshalWithName(v any) (string, []byte, error) {
	message, ok := v.(proto.Message)
	if !ok {
		return "", nil, fmt.Errorf("%w: %T", ErrMessageType, message)
	}
	if message == nil || !message.ProtoReflect().IsValid() {
		return "", nil, ErrMessageNil
	}
	name := message.ProtoReflect().Descriptor().FullName()
	if !name.IsValid() {
		return "", nil, fmt.Errorf("%w: %q", ErrMessageNameInvalid, name)
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return "", nil, fmt.Errorf("marshal protobuf %s: %w", name, err)
	}
	return string(name), payload, nil
}

func UnmarshalWithName(messageName string, payload []byte) (proto.Message, error) {
	name := protoreflect.FullName(messageName)
	if !name.IsValid() {
		return nil, fmt.Errorf("%w: %q", ErrMessageNameInvalid, messageName)
	}
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrMessageTypeNotFound, name)
	}
	message := messageType.New().Interface()
	if err = proto.Unmarshal(payload, message); err != nil {
		return nil, fmt.Errorf("unmarshal protobuf %s: %w", name, err)
	}
	return message, nil
}

func MessageToAny(message proto.Message) (*anypb.Any, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, ErrMessageNil
	}
	payload, err := anypb.New(message)
	if err != nil {
		return nil, err
	}
	return payload, nil
}
