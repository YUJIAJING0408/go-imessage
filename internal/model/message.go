package model

import "time"

const (
	MessageStatusSending = "sending"
	MessageStatusSent    = "sent"
	MessageStatusFailed  = "failed"
	MessageStatusDeleted = "deleted"

	AttemptStatusRunning = "running"
	AttemptStatusSuccess = "success"
	AttemptStatusFailed  = "failed"

	EventTypeMessageCreated = "message_created"
	EventTypeMessageUpdated = "message_updated"
	EventTypeMessageDeleted = "message_deleted"
)

type Message struct {
	ID              int64      `json:"id"`
	SenderID        int64      `json:"sender_id"`
	ReceiverID      int64      `json:"receiver_id"`
	DeviceID        string     `json:"device_id"`
	ConversationID  int64      `json:"conversation_id"`
	ClientMsgID     string     `json:"client_msg_id"`
	Content         string     `json:"content"`
	Status          string     `json:"status"`
	ActiveAttemptID int64      `json:"active_attempt_id"`
	Version         int64      `json:"version"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	DeletedAt       *time.Time `json:"deleted_at,omitempty"`
}

type DeliveryAttempt struct {
	ID              int64      `json:"id"`
	MessageID       int64      `json:"message_id"`
	AttemptNo       int        `json:"attempt_no"`
	ProviderTraceID string     `json:"provider_trace_id"`
	Status          string     `json:"status"`
	ErrorCode       string     `json:"error_code"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
}

type SyncEvent struct {
	Seq            int64     `json:"seq"`
	UserID         int64     `json:"user_id"`
	DeviceID       string    `json:"device_id"`
	ConversationID int64     `json:"conversation_id"`
	MessageID      int64     `json:"message_id"`
	EventType      string    `json:"event_type"`
	MessageStatus  string    `json:"message_status"`
	CreatedAt      time.Time `json:"created_at"`
}

type ConversationSummary struct {
	UserID             int64     `json:"user_id"`
	ConversationID     int64     `json:"conversation_id"`
	LastMessageID      int64     `json:"last_message_id"`
	LastMessagePreview string    `json:"last_message_preview"`
	UnreadCount        int       `json:"unread_count"`
	UpdatedAt          time.Time `json:"updated_at"`
}
