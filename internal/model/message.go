package model

import "time"

const (
	// MessageStatusSending 消息正在发送中（尚未确认成功或失败）
	MessageStatusSending = "sending"
	// MessageStatusSent 消息已成功送达
	MessageStatusSent = "sent"
	// MessageStatusFailed 消息发送失败
	MessageStatusFailed = "failed"
	// MessageStatusDeleted 消息已被删除
	MessageStatusDeleted = "deleted"

	// AttemptStatusRunning 尝试正在进行
	AttemptStatusRunning = "running"
	// AttemptStatusSuccess 尝试成功
	AttemptStatusSuccess = "success"
	// AttemptStatusFailed 尝试失败
	AttemptStatusFailed = "failed"

	// EventTypeMessageCreated 消息创建事件
	EventTypeMessageCreated = "message_created"
	// EventTypeMessageUpdated 消息更新事件（状态变更、内容变更等）
	EventTypeMessageUpdated = "message_updated"
	// EventTypeMessageDeleted 消息删除事件
	EventTypeMessageDeleted = "message_deleted"
)

// Message 表示一条聊天消息。
// 字段说明：
//
//	ID              - 消息唯一标识
//	SenderID        - 发送者用户ID
//	ReceiverID      - 接收者用户ID
//	DeviceID        - 发送设备标识
//	ConversationID - 所属会话ID
//	ClientMsgID    - 客户端生成的消息ID，用于幂等去重
//	Content         - 消息文本内容
//	Status          - 消息当前状态（sending/sent/failed/deleted）
//	ActiveAttemptID - 当前活跃的发送尝试ID
//	Version         - 乐观锁版本号（用于冲突检测，当前由仓储维护）
//	CreatedAt       - 创建时间
//	UpdatedAt       - 最后更新时间
//	DeletedAt       - 删除时间（非nil表示已删除）
type Message struct {
	ID               int64      `json:"id"`
	SenderID         int64      `json:"sender_id"`
	ReceiverID       int64      `json:"receiver_id"`
	DeviceID         string     `json:"device_id"`
	ConversationID   int64      `json:"conversation_id"`
	ClientMsgID      string     `json:"client_msg_id"`
	Content          string     `json:"content"`
	Status           string     `json:"status"`
	HasEverSucceeded bool       `json:"has_ever_succeeded"`
	ActiveAttemptID  int64      `json:"active_attempt_id"`
	Version          int64      `json:"version"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	DeletedAt        *time.Time `json:"deleted_at,omitempty"`
}

// DeliveryAttempt 表示消息的一次发送尝试。
// 每次尝试对应一个外部服务商的投递过程，尝试结束后状态为 success 或 failed。
type DeliveryAttempt struct {
	ID              int64      `json:"id"`
	MessageID       int64      `json:"message_id"`
	AttemptNo       int        `json:"attempt_no"`        // 第几次尝试
	ProviderTraceID string     `json:"provider_trace_id"` // 服务商返回的追踪ID
	Status          string     `json:"status"`            // running / success / failed
	ErrorCode       string     `json:"error_code"`        // 失败时的错误码
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
}

// SyncEvent 是多设备同步的最小事件单元。
// 每次消息状态变化都会产生一个事件，客户端通过游标增量拉取。
type SyncEvent struct {
	Seq            int64     `json:"seq"`       // 全局递增序号，用作游标
	UserID         int64     `json:"user_id"`   // 该事件所属用户
	DeviceID       string    `json:"device_id"` // 事件关联的设备
	ConversationID int64     `json:"conversation_id"`
	MessageID      int64     `json:"message_id"`
	EventType      string    `json:"event_type"`     // created / updated / deleted
	MessageStatus  string    `json:"message_status"` // 触发事件后的消息状态
	CreatedAt      time.Time `json:"created_at"`
}

// ConversationSummary 是聊天列表所需的聚合视图。
// 包含最后一条消息预览、更新时间和未读计数。
type ConversationSummary struct {
	UserID             int64     `json:"user_id"` // 所属用户
	ConversationID     int64     `json:"conversation_id"`
	LastMessageID      int64     `json:"last_message_id"`
	LastMessagePreview string    `json:"last_message_preview"` // 最后一条消息的前若干字
	UnreadCount        int       `json:"unread_count"`
	UpdatedAt          time.Time `json:"updated_at"`
}
