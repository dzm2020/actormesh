package actor

import (
	"time"

	"github.com/dzm2020/actormesh/pkg/component"

	"go.uber.org/zap"
)

// RemoteSender 定义 Actor 系统向集群其他节点发送二进制消息的端口。
type RemoteSender interface {
	// SendToNode 将数据发送到指定节点。
	SendToNode(nodeID string, data []byte) error
	// Broadcast 将数据广播到集群中的其他节点。
	Broadcast(data []byte) error
}

type RemoteReceiver interface {
	// OnMessage 接收并处理来自指定节点的二进制 Actor 消息。
	OnMessage(nodeID string, data []byte) error
}

// SystemAPI defines actor system lifecycle, actor management, and messaging capabilities.
type SystemAPI interface {
	component.IComponent
	RemoteReceiver
	// GetNodeID 获取当前 Actor 系统所属的集群节点 ID。
	GetNodeID() string
	// Has  是否存在进程
	Has(pid *PID) bool
	// SpawnActor creates and starts an actor.
	SpawnActor(handler Actor, options SpawnOptions) (*PID, error)
	// Tell 投递一条不等待结果的独立异步消息。
	Tell(from, target *PID, message any) error
	// Ask 投递请求并等待 respond 返回结果或超时。
	Ask(from, target *PID, message any, timeout time.Duration) (any, error)
	// AskAsync 投递请求，并在发起请求的 Actor 邮箱中异步处理结果。
	AskAsync(from, target *PID, message any, timeout time.Duration, complete AskCompletion) error
	// SendEnvelope 将已构造的消息信封投递到指定 Actor。
	SendEnvelope(target *PID, envelope Envelope) error
	// StopProcess 停止指定 PID 对应的本地 Actor。
	StopProcess(from, target *PID)
}

// AskCompletion handles an asynchronous Ask result in the requesting actor mailbox.
type AskCompletion func(ctx Context, value any, requestErr error)

// Actor defines actor lifecycle and message callbacks.
type Actor interface {
	// Init 在 Actor 开始处理消息前执行初始化。
	Init(Context)
	// HandleTell 处理异步消息。
	HandleTell(Context, any)
	// HandleAsk 	处理同步消息  返回值都为nil 表示“不要回复”
	HandleAsk(Context, any) (any, error)
	// Destroy 在 Actor 停止时执行清理。
	Destroy(ctx Context)
	// Panic 处理 Actor 回调过程中的panic。
	Panic(Context, any)
}

// Context defines runtime capabilities available while processing an actor message.
type Context interface {
	// Self 返回当前 Actor 的 PID。
	Self() *PID
	// Sender 返回当前消息的发送者 PID。
	Sender() *PID
	// InitArgs 返回创建 Actor 时传入的初始化参数副本。
	InitArgs() []any
	// System 返回当前 Actor 所属的 Actor 系统。
	System() SystemAPI
	// Ticker 按固定间隔向当前 Actor 投递任务
	Ticker(d time.Duration, task Task) int64
	// AfterFunc 在指定延迟后向当前 Actor 投递一次任务，返回取消函数。
	AfterFunc(d time.Duration, task Task) int64
	// StopTimer 停止定时器
	StopTimer(timerId int64)
	// Tell 以当前 Actor 为发送者投递一条独立异步消息。
	Tell(target *PID, message any) error
	// Forward 保留当前消息的发送者和元数据，将其转发到指定 Actor。
	Forward(target *PID, message any) error
	// SetAskTimeout 设置当前 Actor 后续 Ask 调用的默认超时时间。
	SetAskTimeout(timeout time.Duration)
	// Ask 以当前 Actor 为发送者投递请求并等待结果。
	Ask(target *PID, message any) (any, error)
	// AskAsync 投递请求但不阻塞当前 Actor，结果将在当前 Actor 邮箱中处理。
	AskAsync(target *PID, message any, complete AskCompletion) error
	// Respond 主动回复消息
	Respond(message any) error
	// RespondError 主动回复错误
	RespondError(responseError error) error
	// Message 返回当前消息
	Message() any
	// MessageMeta 返回当前消息的元数据。
	MessageMeta() MessageMeta
	// Actor 返回当前上下文绑定的 Actor 实例。
	Actor() Actor
	// Logger 返回携带当前 Actor 字段的日志器。
	Logger() *zap.Logger
	// Stop 退出actor,stop之后写入的消息不会处理。
	Stop()
}

type IMessageInvoker interface {
	// InvokerMessage 返回error 则提前退出循环
	InvokerMessage(message interface{}) (exit bool)
}

type DefaultActor struct{}

func (*DefaultActor) Init(Context)                        {}
func (*DefaultActor) HandleTell(Context, any)             {}
func (*DefaultActor) HandleAsk(Context, any) (any, error) { return nil, nil }
func (*DefaultActor) Destroy(ctx Context)                 {}
func (*DefaultActor) Panic(Context, any)                  {}
