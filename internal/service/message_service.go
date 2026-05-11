package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/model"
	"github.com/YUJIAJING0408/go-imessage/internal/repository"
)

// SendMessageRequest 发送消息的请求参数。
type SendMessageRequest struct {
	RequestID      string `json:"request_id"` // 可选的请求追踪 ID
	SenderID       int64  `json:"sender_id"`
	ReceiverID     int64  `json:"receiver_id"`
	DeviceID       string `json:"device_id"`
	ConversationID int64  `json:"conversation_id"`
	ClientMsgID    string `json:"client_msg_id"` // 客户端消息 ID，用于幂等
	Content        string `json:"content"`       // 消息文本
}

// CompleteAttemptRequest 完成发送尝试的请求。
type CompleteAttemptRequest struct {
	RequestID string `json:"request_id"`
	AttemptID int64  `json:"attempt_id"`
	Success   bool   `json:"success"`    // 是否成功
	ErrorCode string `json:"error_code"` // 失败时的错误码
}

// SyncRequest 同步事件的请求参数。
type SyncRequest struct {
	UserID   int64  `json:"user_id"`
	DeviceID string `json:"device_id"`
	Cursor   int64  `json:"cursor"` // 上次同步获得的游标，0 表示从最早开始
	Limit    int    `json:"limit"`  // 最多返回事件数，0 表示不限制
}

// MessageService 是消息业务逻辑的核心服务，依赖 MessageRepository 进行持久化。
type MessageService struct {
	repo repository.MessageRepository
}

// NewMessageService 创建新的 MessageService 实例。
func NewMessageService(repo repository.MessageRepository) *MessageService {
	return &MessageService{repo: repo}
}

// SendMessage 处理发送消息的完整逻辑。
// 流程：
//  1. 校验必填字段（SenderID、ReceiverID、ConversationID、Content）。
//  2. 如果提供了 ClientMsgID，则查询是否已存在该消息，实现幂等发送。
//  3. 如果未提供 ClientMsgID，检查短时间内是否有相同内容的重复消息（30秒窗口），作为旧客户端去重。
//  4. 创建消息并立即开始一次发送尝试。
//
// 返回创建/已有的消息、新的发送尝试（幂等情况下 attempt 为空）以及错误。
func (s *MessageService) SendMessage(ctx context.Context, req SendMessageRequest) (model.Message, model.DeliveryAttempt, error) {
	if req.SenderID <= 0 || req.ReceiverID <= 0 || req.ConversationID <= 0 || strings.TrimSpace(req.Content) == "" {
		slog.WarnContext(ctx, "send message rejected",
			"request_id", req.RequestID,
			"sender_id", req.SenderID,
			"receiver_id", req.ReceiverID)
		return model.Message{}, model.DeliveryAttempt{}, errors.New("invalid request")
	}

	// 1. 幂等：基于客户端消息 ID
	if strings.TrimSpace(req.ClientMsgID) != "" {
		existing, err := s.repo.FindByClientMsgID(req.SenderID, req.ClientMsgID)
		if err == nil {
			return existing, model.DeliveryAttempt{}, nil
		}
	}

	// 2. 无 clientMsgID 时的内容去重（30 秒内相同会话相同内容视为重复）
	if strings.TrimSpace(req.ClientMsgID) == "" {
		dup, err := s.repo.FindLikelyDuplicateMessage(req.SenderID, req.ConversationID, req.Content, 30*time.Second)
		if err == nil {
			return dup, model.DeliveryAttempt{}, nil
		}
	}

	// 创建消息
	msg := model.Message{
		SenderID:       req.SenderID,
		ReceiverID:     req.ReceiverID,
		DeviceID:       req.DeviceID,
		ConversationID: req.ConversationID,
		ClientMsgID:    req.ClientMsgID,
		Content:        req.Content,
		Status:         model.MessageStatusSending,
	}

	saved, err := s.repo.CreateMessage(msg)
	if err != nil {
		slog.ErrorContext(ctx, "create message failed",
			"request_id", req.RequestID,
			"sender_id", req.SenderID,
			"msg_id", saved.ID,
			"error", err,
		)
		return model.Message{}, model.DeliveryAttempt{}, err
	}

	// 启动发送尝试
	attempt, err := s.repo.StartAttempt(saved.ID, fmt.Sprintf("provider-%d", saved.ID))
	if err != nil {
		slog.ErrorContext(ctx, "start attempt failed",
			"request_id", req.RequestID,
			"sender_id", req.SenderID,
			"msg_id", saved.ID,
			"error", err,
		)
		return saved, model.DeliveryAttempt{}, err
	}
	return saved, attempt, nil
}

// CompleteAttempt 完成一次发送尝试。
// 直接委托给仓储层，仓储层会处理状态变更、事件广播和未读更新。
func (s *MessageService) CompleteAttempt(ctx context.Context, req CompleteAttemptRequest) (model.Message, error) {
	if req.AttemptID <= 0 {
		return model.Message{}, errors.New("attempt_id is required")
	}
	msg, err := s.repo.CompleteAttempt(req.AttemptID, req.Success, req.ErrorCode)
	if err != nil {
		slog.ErrorContext(ctx, "complete attempt failed",
			"request_id", req.RequestID,
			"attempt_id", req.AttemptID,
			"error", err,
		)
	}
	return msg, err
}

// RetryMessage 重新发送一条消息。
// 将消息状态重置为 sending，刷新发送方会话预览，并创建新的发送尝试。
func (s *MessageService) RetryMessage(ctx context.Context, messageID int64) (model.Message, model.DeliveryAttempt, error) {
	msg, err := s.repo.GetMessage(messageID)
	if err != nil {
		slog.ErrorContext(ctx, "retry load message failed",
			"msg_id", messageID,
			"error", err,
		)
		return model.Message{}, model.DeliveryAttempt{}, err
	}
	// 记录期望版本，防止并发覆盖
	expectedVersion := msg.Version
	msg.Status = model.MessageStatusSending
	// 传入期望版本保存
	msg, err = s.repo.SaveMessage(msg, expectedVersion)
	if err != nil {
		slog.ErrorContext(ctx, "retry save message failed",
			"msg_id", messageID,
			"error", err,
		)
		return model.Message{}, model.DeliveryAttempt{}, err
	}

	// 更新发送方预览，让聊天列表立即反映“发送中”
	s.repo.RefreshSenderPreview(msg.SenderID, msg.ConversationID, msg)

	attempt, err := s.repo.StartAttempt(msg.ID, fmt.Sprintf("retry-%d", msg.ID))
	if err != nil {
		slog.ErrorContext(ctx, "retry start attempt failed",
			"msg_id", messageID,
			"error", err,
		)
		return msg, model.DeliveryAttempt{}, err
	}
	return msg, attempt, nil
}

// ListConversationMessages 返回会话消息列表，支持分页。
// limit 默认 20，最大 100。
// 返回的消息中 Version 字段会累加尝试次数（用于客户端显示）。
func (s *MessageService) ListConversationMessages(conversationID int64, offset int, limit int) ([]model.Message, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	items, err := s.repo.ListConversationMessages(conversationID, offset, limit)
	if err != nil {
		return nil, err
	}
	for i := range items {
		count, _ := s.repo.CountAttempts(items[i].ID)
		if count > 0 {
			items[i].Version += int64(count)
		}
	}
	return items, nil
}

// GetMessage 获取单条消息详情。
func (s *MessageService) GetMessage(id int64) (model.Message, error) {
	return s.repo.GetMessage(id)
}

// GetConversationSummary 获取会话摘要。
func (s *MessageService) GetConversationSummary(userID int64, conversationID int64) (model.ConversationSummary, error) {
	return s.repo.GetConversationSummary(userID, conversationID)
}

// MarkConversationRead 将指定会话标记为已读（未读计数归零）。
func (s *MessageService) MarkConversationRead(userID int64, conversationID int64) error {
	return s.repo.MarkConversationRead(userID, conversationID)
}

// DeleteMessage 删除消息。
func (s *MessageService) DeleteMessage(messageID int64) (model.Message, error) {
	return s.repo.DeleteMessage(messageID)
}

// Sync 返回用户自某个游标之后的增量事件，并更新设备游标。
// 若请求游标为 0，则使用最近保存的设备游标作为起点。
func (s *MessageService) Sync(ctx context.Context, req SyncRequest) ([]model.SyncEvent, int64, error) {
	cursor := req.Cursor
	if cursor == 0 {
		cursor = s.repo.GetDeviceCursor(req.UserID, req.DeviceID)
	}
	events, err := s.repo.ListEventsAfter(req.UserID, cursor)
	if err != nil {
		return nil, cursor, err
	}
	if req.Limit > 0 && len(events) > req.Limit {
		events = events[:req.Limit]
	}
	nextCursor := cursor
	for _, ev := range events {
		if ev.Seq > nextCursor {
			nextCursor = ev.Seq
		}
	}
	s.repo.SaveDeviceCursor(req.UserID, req.DeviceID, nextCursor)
	return events, nextCursor, nil
}
