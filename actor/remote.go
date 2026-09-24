package actor

import (
	"errors"
	"fmt"
	"github.com/dzm2020/actormesh/pkg/serialize/protocodec"

	actorpb "github.com/dzm2020/actormesh/actor/pb"

	"go.uber.org/zap"
)

func (s *System) sendRemoteEnvelope(target *PID, envelope Envelope) error {
	messageName, data, err := protocodec.MarshalWithName(envelope.Payload)
	if err != nil {
		return fmt.Errorf("marshal remote payload:%w", err)
	}
	source := envelope.Sender
	if request := envelope.Meta.Request; request != nil && request.NodeID == s.nodeID {
		s.requests.retarget(request.RequestID, target.NodeID)
	}
	return s.sendRemoteMessage(&actorpb.RemoteActorMessage{
		Common: &actorpb.RemoteActorCommon{
			SourcePid:    pidToProto(source),
			TargetNodeId: target.NodeID,
			TargetPid:    pidToProto(target),
		},
		Body: &actorpb.RemoteActorMessage_Request{
			Request: &actorpb.RemoteActorRequest{
				MessageName: messageName,
				Payload:     data,
				Request:     requestRefToProto(envelope.Meta.Request),
			},
		},
	})
}

func (s *System) sendRemoteAskResponse(from *PID, request *RequestRef, message any) error {
	messageName, payload, err := protocodec.MarshalWithName(message)
	if err != nil {
		return fmt.Errorf("marshal remote ask response:%w", err)
	}
	return s.sendRemoteMessage(&actorpb.RemoteActorMessage{
		Common: &actorpb.RemoteActorCommon{
			SourcePid:    pidToProto(from),
			TargetNodeId: request.NodeID,
		},
		Body: &actorpb.RemoteActorMessage_Response{
			Response: &actorpb.RemoteActorResponse{
				Kind:        actorpb.RemoteMessageKind_ASK_RESPONSE,
				Request:     requestRefToProto(request),
				MessageName: messageName,
				Payload:     payload,
			},
		},
	})
}

func (s *System) sendRemoteRequestError(from *PID, request *RequestRef, cause error) error {
	errorCode, _ := CodeOf(cause)
	return s.sendRemoteMessage(&actorpb.RemoteActorMessage{
		Common: &actorpb.RemoteActorCommon{
			SourcePid:    pidToProto(from),
			TargetNodeId: request.NodeID,
		},
		Body: &actorpb.RemoteActorMessage_Response{
			Response: &actorpb.RemoteActorResponse{
				Kind:      actorpb.RemoteMessageKind_ASK_ERROR,
				Request:   requestRefToProto(request),
				Error:     cause.Error(),
				ErrorCode: uint32(errorCode),
			},
		},
	})
}

func (s *System) sendRemoteMessage(message *actorpb.RemoteActorMessage) error {
	if s.remoteSender == nil {
		return ErrRemoteSenderNil
	}
	if message == nil {
		return ErrRemoteMessageNil
	}
	if message.GetCommon() == nil {
		return ErrRemoteCommonInvalid
	}
	data, err := protocodec.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal remote message:%w", err)
	}
	if err = s.remoteSender.SendToNode(message.GetCommon().TargetNodeId, data); err != nil {
		return fmt.Errorf("send remote message:%w", err)
	}
	return nil
}

func (s *System) OnMessage(sourceNodeID string, data []byte) error {
	if err := s.process(sourceNodeID, data); err != nil {
		s.logger.Error("OnMessage", zap.Error(err))
		return err
	}
	return nil
}

func (s *System) process(sourceNodeID string, data []byte) error {
	if sourceNodeID == "" {
		return ErrRemoteSourceNodeInvalid
	}
	message := &actorpb.RemoteActorMessage{}
	if err := protocodec.Unmarshal(data, message); err != nil {
		return fmt.Errorf("unmarshal remote message:%w", err)
	}
	common := message.GetCommon()
	if common == nil {
		return ErrRemoteCommonInvalid
	}
	//  广播消息
	if common.TargetNodeId == "" {
		common.TargetNodeId = s.nodeID
		if common.TargetPid != nil {
			common.TargetPid.NodeId = s.nodeID
		}
	}
	if common.SourcePid == nil {
		return ErrRemoteSenderNil
	}
	//  验证来源  跨节点 Forward 可能被来源校验拒绝
	//if common.SourcePid.NodeId != sourceNodeID {
	//	return ErrRemoteSourcePIDInvalid
	//}
	//  验证目标
	if common.TargetNodeId != s.nodeID {
		return fmt.Errorf("not local message nodeId:%s", common.TargetNodeId)
	}

	if err := s.onRemoteHandler(message); err != nil {
		return err
	}

	s.logger.Debug("actor remote message handled", zap.String("messageName", remoteMessageName(message)), zap.Any("message", message))
	return nil
}

// onRemoteHandler 投递已经解码并验证过的远程 Actor 消息。
func (s *System) onRemoteHandler(message *actorpb.RemoteActorMessage) error {
	switch body := message.GetBody().(type) {
	case *actorpb.RemoteActorMessage_Request: // 跨节点 Tell、Forward 或 Ask 请求到达目标节点时触发
		return s.onRemoteEnvelopeHandler(message, body.Request)
	case *actorpb.RemoteActorMessage_Response: // 同步消息回复数据或错误
		return s.onRemoteResponseHandler(body.Response)
	default:
		return ErrRemoteMessageBodyInvalid
	}
}

// deliverRemoteEnvelope 处理远程数据包
func (s *System) onRemoteEnvelopeHandler(message *actorpb.RemoteActorMessage, request *actorpb.RemoteActorRequest) error {
	if request == nil {
		return ErrRequestNil
	}
	//  非法消息 不回复
	payload, err := protocodec.UnmarshalWithName(request.MessageName, request.Payload)
	if err != nil {
		return fmt.Errorf("remote handler unmarshal body:%w", err)
	}
	err = s.SendEnvelope(pidFromProto(message.GetCommon().TargetPid), Envelope{
		Payload: payload,
		Sender:  pidFromProto(message.GetCommon().SourcePid),
		Meta:    MessageMeta{Request: requestRefFromProto(request.Request)},
	})
	if err != nil {
		s.rejectRemoteRequest(message, err)
		return fmt.Errorf("remote handler:%w", err)
	}
	return nil
}

// onRemoteResponseHandler 处理远程回复的数据或错误
func (s *System) onRemoteResponseHandler(response *actorpb.RemoteActorResponse) error {
	if response == nil {
		return errors.New("remote response nil response")
	}
	request := response.Request
	if request == nil || request.RequestId == 0 || request.NodeId == "" {
		return ErrRequestNil
	}
	switch response.Kind {
	case actorpb.RemoteMessageKind_ASK_RESPONSE:
		payload, err := protocodec.UnmarshalWithName(response.MessageName, response.Payload)
		if err != nil {
			return fmt.Errorf("remote response handler unmarshal body:%w", err)
		}
		return s.requests.complete(response.Request.RequestId, payload, nil)
	case actorpb.RemoteMessageKind_ASK_ERROR:
		err := &codedError{code: ErrorCode(response.ErrorCode), message: response.Error}
		return s.requests.complete(response.Request.RequestId, nil, err)
	default:
		return fmt.Errorf("remote response handler kind:%s:%w", response.Kind.String(), ErrRemoteMessageKindInvalid)
	}
}

// rejectRemoteRequest 拒绝远程请求 回复错误
func (s *System) rejectRemoteRequest(message *actorpb.RemoteActorMessage, cause error) {
	common := message.GetCommon()
	requestBody, ok := message.GetBody().(*actorpb.RemoteActorMessage_Request)
	if !ok || requestBody.Request == nil || requestBody.Request.Request == nil || common == nil || s.remoteSender == nil {
		s.logger.Warn("rejectRemoteRequest", zap.Error(cause))
		return
	}

	request := requestBody.Request.Request
	if request.RequestId == 0 || request.NodeId == "" {
		s.logger.Warn("rejectRemoteRequest", zap.Error(cause))
		return
	}

	if err := s.sendRemoteRequestError(pidFromProto(common.TargetPid), requestRefFromProto(request), cause); err != nil {
		s.logger.Error("rejectRemoteRequest", zap.Error(err))
		return
	}
}

func remoteMessageName(message *actorpb.RemoteActorMessage) string {
	switch body := message.GetBody().(type) {
	case *actorpb.RemoteActorMessage_Request:
		return body.Request.GetMessageName()
	case *actorpb.RemoteActorMessage_Response:
		return body.Response.GetMessageName()
	default:
		return ""
	}
}
