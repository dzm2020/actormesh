package actor

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

func (s *System) Tell(from, target *PID, message any) error {
	return s.SendEnvelope(target, Envelope{
		Payload: message,
		Sender:  from,
	})
}

func (s *System) Ask(from, target *PID, message any, timeout time.Duration) (any, error) {
	result := make(chan askResult, 1)
	complete := func(_ uint64, value any, err error) {
		result <- askResult{payload: value, err: err}
	}
	if err := s.sendAsk(from, target, message, timeout, complete); err != nil {
		return nil, err
	}
	response := <-result
	return response.payload, response.err
}

func (s *System) AskAsync(from, target *PID, message any, timeout time.Duration, complete AskCompletion) error {
	if from == nil {
		return ErrActorSenderNil
	}
	if target == nil {
		return ErrActorTargetNil
	}
	if complete == nil {
		return ErrAskCompletionNil
	}
	onComplete := func(requestID uint64, value any, requestErr error) {
		task := func(ctx Context) {
			complete(ctx, value, requestErr)
		}
		if err := s.doTask(target, from, task); err != nil {
			s.logger.Error("ask async deliver asynchronous ask result failed",
				zap.Uint64("actor_id", target.ActorID),
				zap.String("actor_name", target.ActorName),
				zap.String("node_id", target.NodeID))
		}
		return
	}
	return s.sendAsk(from, target, message, timeout, onComplete)
}

func (s *System) doTask(from, target *PID, task Task) error {
	return s.SendEnvelope(target, Envelope{
		Sender:  from,
		Payload: task,
	})
}

func (s *System) SendEnvelope(target *PID, envelope Envelope) error {
	//  目标非法
	if target == nil {
		return ErrActorTargetNil
	}
	if envelope.Sender == nil {
		return ErrActorSenderNil
	}
	//  验证元数据
	request := envelope.Meta.Request
	if request != nil && (request.NodeID == "" || request.RequestID == 0) {
		return fmt.Errorf("%v:%w", request, ErrActorEnvelopeRequestInvalid)
	}
	//  写入远程
	if target.NodeID != s.GetNodeID() {
		return s.sendRemoteEnvelope(target, envelope)
	}
	//  写入本地
	proc, err := s.mgr.getProcess(target)
	if err != nil {
		return err
	}
	return proc.Push(envelope)
}

func (s *System) sendAsk(from, target *PID, message any, timeout time.Duration, complete requestCompletion) error {
	if from == nil {
		return ErrActorSenderNil
	}
	if target == nil {
		return ErrActorTargetNil
	}
	if timeout <= 0 {
		timeout = defaultActorAskTimeout
	}
	ref, err := s.requests.add(s.nodeID, target.NodeID, timeout, complete)
	if err != nil {
		return err
	}
	err = s.SendEnvelope(target, Envelope{
		Payload: message,
		Sender:  from,
		Meta:    MessageMeta{Request: ref},
	})
	if err != nil {
		s.requests.remove(ref.RequestID)
		return fmt.Errorf("send ask:%w", err)
	}
	return nil
}

func (s *System) respond(from, _ *PID, meta MessageMeta, message any) error {
	if meta.Request == nil {
		return ErrRequestNil
	}
	if meta.Request.NodeID != s.nodeID {
		return s.sendRemoteAskResponse(from, meta.Request, message)

	}
	return s.requests.complete(meta.Request.RequestID, message, nil)
}

func (s *System) respondError(from *PID, meta MessageMeta, responseError error) error {
	if responseError == nil {
		return ErrRespondErrorNil
	}
	if meta.Request == nil {
		return ErrRequestNil
	}
	if meta.Request.NodeID != s.nodeID {
		return s.sendRemoteRequestError(from, meta.Request, responseError)
	}
	return s.requests.complete(meta.Request.RequestID, nil, responseError)
}
