package actorpb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestRemoteActorMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		message *RemoteActorMessage
		check   func(t *testing.T, message *RemoteActorMessage)
	}{
		{
			name: "request",
			message: &RemoteActorMessage{
				Common: &RemoteActorCommon{
					SourcePid:    &PID{ActorId: 1, NodeId: "node-a"},
					TargetPid:    &PID{ActorId: 2, NodeId: "node-b"},
					TargetNodeId: "node-b",
				},
				Body: &RemoteActorMessage_Request{Request: &RemoteActorRequest{
					MessageName: "actor.TestMessage",
					Payload:     []byte{1, 2, 3},
					Request:     &RequestRef{NodeId: "node-a", RequestId: 7},
				}},
			},
			check: func(t *testing.T, message *RemoteActorMessage) {
				request := message.GetRequest()
				if request == nil || request.GetRequest().GetRequestId() != 7 {
					t.Fatalf("request was not preserved: %v", request)
				}
				if string(request.GetPayload()) != string([]byte{1, 2, 3}) {
					t.Fatalf("payload was not preserved: %v", request.GetPayload())
				}
			},
		},
		{
			name: "response",
			message: &RemoteActorMessage{
				Common: &RemoteActorCommon{SourcePid: &PID{ActorId: 2, NodeId: "node-b"}, TargetNodeId: "node-a"},
				Body: &RemoteActorMessage_Response{Response: &RemoteActorResponse{
					Kind:        RemoteMessageKind_ASK_RESPONSE,
					Request:     &RequestRef{NodeId: "node-a", RequestId: 7},
					MessageName: "actor.TestResponse",
					Payload:     []byte{4, 5, 6},
				}},
			},
			check: func(t *testing.T, message *RemoteActorMessage) {
				response := message.GetResponse()
				if response == nil || response.GetKind() != RemoteMessageKind_ASK_RESPONSE {
					t.Fatalf("response was not preserved: %v", response)
				}
				if response.GetRequest().GetRequestId() != 7 {
					t.Fatalf("request id was not preserved: %v", response.GetRequest())
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := proto.Marshal(test.message)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			decoded := &RemoteActorMessage{}
			if err = proto.Unmarshal(data, decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			test.check(t, decoded)
		})
	}
}

func FuzzRemoteActorMessageDecode(f *testing.F) {
	seed, err := proto.Marshal(&RemoteActorMessage{
		Common: &RemoteActorCommon{TargetNodeId: "node-a"},
		Body: &RemoteActorMessage_Request{Request: &RemoteActorRequest{
			MessageName: "actor.TestMessage",
			Payload:     []byte{1, 2, 3},
		}},
	})
	if err != nil {
		f.Fatalf("marshal seed: %v", err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0x00, 0x80})

	f.Fuzz(func(_ *testing.T, data []byte) {
		message := &RemoteActorMessage{}
		_ = proto.Unmarshal(data, message)
	})
}
