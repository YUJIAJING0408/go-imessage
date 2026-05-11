package service

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/model"
	"github.com/YUJIAJING0408/go-imessage/internal/repository"
)

type SendMessageRequest struct {
	RequestID      string `json:"request_id"`
	SenderID       int64  `json:"sender_id"`
	ReceiverID     int64  `json:"receiver_id"`
	DeviceID       string `json:"device_id"`
	ConversationID int64  `json:"conversation_id"`
	ClientMsgID    string `json:"client_msg_id"`
	Content        string `json:"content"`
}

type CompleteAttemptRequest struct {
	RequestID string `json:"request_id"`
	AttemptID int64  `json:"attempt_id"`
	Success   bool   `json:"success"`
	ErrorCode string `json:"error_code"`
}

type SyncRequest struct {
	UserID   int64  `json:"user_id"`
	DeviceID string `json:"device_id"`
	Cursor   int64  `json:"cursor"`
	Limit    int    `json:"limit"`
}

type MessageService struct {
	repo repository.MessageRepository
}

func NewMessageService(repo repository.MessageRepository) *MessageService {
	return &MessageService{repo: repo}
}

// SendMessage creates a message and starts provider delivery.
// It is idempotent when the client provides a valid ClientMsgID.
func (s *MessageService) SendMessage(req SendMessageRequest) (model.Message, model.DeliveryAttempt, error) {
	if req.SenderID <= 0 || req.ReceiverID <= 0 || req.ConversationID <= 0 || strings.TrimSpace(req.Content) == "" {
		log.Println("send message rejected: invalid request")
		return model.Message{}, model.DeliveryAttempt{}, errors.New("invalid request")
	}

	// 1. Idempotency: check by client message ID
	if strings.TrimSpace(req.ClientMsgID) != "" {
		existing, err := s.repo.FindByClientMsgID(req.SenderID, req.ClientMsgID)
		if err == nil {
			// Message already exists, return it (attempt info not needed for idempotent reply)
			return existing, model.DeliveryAttempt{}, nil
		}
	}

	// 2. Old‑client duplication guard (when clientMsgID is missing)
	if strings.TrimSpace(req.ClientMsgID) == "" {
		dup, err := s.repo.FindLikelyDuplicateMessage(req.SenderID, req.ConversationID, req.Content, 30*time.Second)
		if err == nil {
			return dup, model.DeliveryAttempt{}, nil
		}
	}

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
		log.Println("send message create failed:", err)
		return model.Message{}, model.DeliveryAttempt{}, err
	}

	attempt, err := s.repo.StartAttempt(saved.ID, fmt.Sprintf("provider-%d", saved.ID))
	if err != nil {
		log.Println("send attempt start failed:", err)
		return saved, model.DeliveryAttempt{}, err
	}
	return saved, attempt, nil
}

func (s *MessageService) CompleteAttempt(req CompleteAttemptRequest) (model.Message, error) {
	if req.AttemptID <= 0 {
		return model.Message{}, errors.New("attempt_id is required")
	}
	msg, err := s.repo.CompleteAttempt(req.AttemptID, req.Success, req.ErrorCode)
	if err != nil {
		log.Println("complete attempt failed:", err)
	}
	return msg, err
}

// RetryMessage marks a message as sending again and creates a new delivery attempt.
func (s *MessageService) RetryMessage(messageID int64) (model.Message, model.DeliveryAttempt, error) {
	msg, err := s.repo.GetMessage(messageID)
	if err != nil {
		log.Println("retry load message failed:", err)
		return model.Message{}, model.DeliveryAttempt{}, err
	}
	msg.Status = model.MessageStatusSending
	msg, err = s.repo.SaveMessage(msg)
	if err != nil {
		log.Println("retry save message failed:", err)
		return model.Message{}, model.DeliveryAttempt{}, err
	}

	// Update sender's conversation preview (the list should show "sending" state immediately)
	s.repo.RefreshSenderPreview(msg.SenderID, msg.ConversationID, msg)

	attempt, err := s.repo.StartAttempt(msg.ID, fmt.Sprintf("retry-%d", msg.ID))
	if err != nil {
		log.Println("retry start attempt failed:", err)
		return msg, model.DeliveryAttempt{}, err
	}
	return msg, attempt, nil
}

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

func (s *MessageService) GetMessage(id int64) (model.Message, error) {
	return s.repo.GetMessage(id)
}

func (s *MessageService) GetConversationSummary(userID int64, conversationID int64) (model.ConversationSummary, error) {
	return s.repo.GetConversationSummary(userID, conversationID)
}

func (s *MessageService) MarkConversationRead(userID int64, conversationID int64) error {
	return s.repo.MarkConversationRead(userID, conversationID)
}

func (s *MessageService) DeleteMessage(messageID int64) (model.Message, error) {
	return s.repo.DeleteMessage(messageID)
}

func (s *MessageService) Sync(req SyncRequest) ([]model.SyncEvent, int64, error) {
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
