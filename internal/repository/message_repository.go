package repository

import (
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/model"
)

var ErrNotFound = errors.New("not found")

// MessageRepository defines the persistence operations for messages.
type MessageRepository interface {
	CreateMessage(msg model.Message) (model.Message, error)
	FindByClientMsgID(senderID int64, clientMsgID string) (model.Message, error)
	GetMessage(id int64) (model.Message, error)
	SaveMessage(msg model.Message) (model.Message, error)
	StartAttempt(messageID int64, providerTraceID string) (model.DeliveryAttempt, error)
	CompleteAttempt(attemptID int64, success bool, errorCode string) (model.Message, error)
	DeleteMessage(messageID int64) (model.Message, error)
	ListConversationMessages(conversationID int64, offset int, limit int) ([]model.Message, error)
	CountAttempts(messageID int64) (int, error)
	GetConversationSummary(userID int64, conversationID int64) (model.ConversationSummary, error)
	MarkConversationRead(userID int64, conversationID int64) error
	ListEventsAfter(userID int64, cursor int64) ([]model.SyncEvent, error)
	GetDeviceCursor(userID int64, deviceID string) int64
	SaveDeviceCursor(userID int64, deviceID string, cursor int64)
	FindLikelyDuplicateMessage(senderID, conversationID int64, content string, within time.Duration) (model.Message, error)
	RefreshSenderPreview(userID, conversationID int64, msg model.Message)
}

// MemoryMessageRepository implements MessageRepository with in‑memory storage.
type MemoryMessageRepository struct {
	mu sync.RWMutex

	nextMessageID int64
	nextAttemptID int64
	nextEventSeq  int64

	messages      map[int64]model.Message
	attempts      map[int64]model.DeliveryAttempt
	events        map[int64][]model.SyncEvent
	summaries     map[string]model.ConversationSummary
	deviceCursors map[string]int64

	clientMsgIndex map[string]int64 // "senderID:clientMsgID" -> messageID
}

func NewMemoryMessageRepository() *MemoryMessageRepository {
	return &MemoryMessageRepository{
		nextMessageID:  1,
		nextAttemptID:  1,
		nextEventSeq:   1,
		messages:       make(map[int64]model.Message),
		attempts:       make(map[int64]model.DeliveryAttempt),
		events:         make(map[int64][]model.SyncEvent),
		summaries:      make(map[string]model.ConversationSummary),
		deviceCursors:  make(map[string]int64),
		clientMsgIndex: make(map[string]int64),
	}
}

// ---------- helpers ----------

func summaryKey(userID, conversationID int64) string {
	return strconv.FormatInt(userID, 10) + ":" + strconv.FormatInt(conversationID, 10)
}

func deviceKey(userID int64, deviceID string) string {
	return strconv.FormatInt(userID, 10) + ":" + deviceID
}

// ---------- public methods ----------

func (r *MemoryMessageRepository) CreateMessage(msg model.Message) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	msg.ID = r.nextMessageID
	r.nextMessageID++
	msg.CreatedAt = now
	msg.UpdatedAt = now
	if msg.Status == "" {
		msg.Status = model.MessageStatusSending
	}
	msg.Version = 1
	r.messages[msg.ID] = msg

	if msg.ClientMsgID != "" {
		idxKey := strconv.FormatInt(msg.SenderID, 10) + ":" + msg.ClientMsgID
		r.clientMsgIndex[idxKey] = msg.ID
	}

	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageCreated)
	r.updateSummaryLocked(msg.SenderID, msg, false)
	return msg, nil
}

func (r *MemoryMessageRepository) FindByClientMsgID(senderID int64, clientMsgID string) (model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	idxKey := strconv.FormatInt(senderID, 10) + ":" + clientMsgID
	if id, ok := r.clientMsgIndex[idxKey]; ok {
		if msg, exists := r.messages[id]; exists {
			return msg, nil
		}
	}
	return model.Message{}, ErrNotFound
}

func (r *MemoryMessageRepository) GetMessage(id int64) (model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	msg, ok := r.messages[id]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	return msg, nil
}

func (r *MemoryMessageRepository) SaveMessage(msg model.Message) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.messages[msg.ID]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	msg.Version++
	msg.UpdatedAt = time.Now()
	r.messages[msg.ID] = msg
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	r.appendEventLocked(msg.ReceiverID, msg, model.EventTypeMessageUpdated)
	return msg, nil
}

func (r *MemoryMessageRepository) StartAttempt(messageID int64, providerTraceID string) (model.DeliveryAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	msg, ok := r.messages[messageID]
	if !ok {
		return model.DeliveryAttempt{}, ErrNotFound
	}

	attemptNo := 1
	for _, a := range r.attempts {
		if a.MessageID == messageID && a.AttemptNo >= attemptNo {
			attemptNo = a.AttemptNo + 1
		}
	}

	att := model.DeliveryAttempt{
		ID:              r.nextAttemptID,
		MessageID:       messageID,
		AttemptNo:       attemptNo,
		ProviderTraceID: providerTraceID,
		Status:          model.AttemptStatusRunning,
		StartedAt:       time.Now(),
	}
	r.nextAttemptID++
	r.attempts[att.ID] = att

	msg.ActiveAttemptID = att.ID
	msg.Status = model.MessageStatusSending
	msg.Version++
	msg.UpdatedAt = time.Now()
	r.messages[msg.ID] = msg
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	return att, nil
}

// CompleteAttempt finishes a delivery attempt and updates the message status.
// It prevents unread count inflation: only the first successful attempt increments the receiver's unread.
func (r *MemoryMessageRepository) CompleteAttempt(attemptID int64, success bool, errorCode string) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	att, ok := r.attempts[attemptID]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	msg, ok := r.messages[att.MessageID]
	if !ok {
		return model.Message{}, ErrNotFound
	}

	now := time.Now()
	att.FinishedAt = &now
	att.ErrorCode = errorCode

	if success {
		att.Status = model.AttemptStatusSuccess
		msg.Status = model.MessageStatusSent
	} else {
		att.Status = model.AttemptStatusFailed
		// 检查历史上是否已有成功的 attempt，防止状态回退
		hadSuccess := false
		for _, a := range r.attempts {
			if a.MessageID == msg.ID && a.ID != attemptID && a.Status == model.AttemptStatusSuccess {
				hadSuccess = true
				break
			}
		}
		if hadSuccess {
			// 保持 sent，不回退为 failed
			msg.Status = model.MessageStatusSent
		} else {
			msg.Status = model.MessageStatusFailed
		}
	}

	msg.Version++
	msg.UpdatedAt = now

	r.attempts[attemptID] = att
	r.messages[msg.ID] = msg

	// Broadcast events to both participants
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	r.appendEventLocked(msg.ReceiverID, msg, model.EventTypeMessageUpdated)

	// Always update sender's conversation summary (preview)
	r.updateSummaryLocked(msg.SenderID, msg, false)

	// Increment receiver unread only if this is the FIRST successful attempt ever
	if success {
		alreadyHadSuccess := false
		for _, a := range r.attempts {
			if a.MessageID == msg.ID && a.ID != attemptID && a.Status == model.AttemptStatusSuccess {
				alreadyHadSuccess = true
				break
			}
		}
		if !alreadyHadSuccess {
			r.updateSummaryLocked(msg.ReceiverID, msg, true)
		}
	}

	return msg, nil
}

func (r *MemoryMessageRepository) DeleteMessage(messageID int64) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	msg, ok := r.messages[messageID]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	now := time.Now()
	msg.Status = model.MessageStatusDeleted
	msg.DeletedAt = &now
	msg.UpdatedAt = now
	msg.Version++
	r.messages[msg.ID] = msg

	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageDeleted)
	r.appendEventLocked(msg.ReceiverID, msg, model.EventTypeMessageDeleted)
	return msg, nil
}

func (r *MemoryMessageRepository) ListConversationMessages(conversationID int64, offset int, limit int) ([]model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	items := make([]model.Message, 0)
	for _, msg := range r.messages {
		if msg.ConversationID == conversationID && msg.Status != model.MessageStatusDeleted {
			items = append(items, msg)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	if offset >= len(items) {
		return []model.Message{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func (r *MemoryMessageRepository) CountAttempts(messageID int64) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, a := range r.attempts {
		if a.MessageID == messageID {
			count++
		}
	}
	return count, nil
}

func (r *MemoryMessageRepository) GetConversationSummary(userID int64, conversationID int64) (model.ConversationSummary, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, ok := r.summaries[summaryKey(userID, conversationID)]
	if !ok {
		return model.ConversationSummary{UserID: userID, ConversationID: conversationID}, nil
	}
	return s, nil
}

func (r *MemoryMessageRepository) MarkConversationRead(userID int64, conversationID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := summaryKey(userID, conversationID)
	s := r.summaries[key]
	s.UserID = userID
	s.ConversationID = conversationID
	s.UnreadCount = 0
	s.UpdatedAt = time.Now()
	r.summaries[key] = s
	return nil
}

func (r *MemoryMessageRepository) ListEventsAfter(userID int64, cursor int64) ([]model.SyncEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	userEvents := r.events[userID]
	result := make([]model.SyncEvent, 0)
	for _, ev := range userEvents {
		if ev.Seq > cursor {
			result = append(result, ev)
		}
	}
	return result, nil
}

func (r *MemoryMessageRepository) GetDeviceCursor(userID int64, deviceID string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.deviceCursors[deviceKey(userID, deviceID)]
}

func (r *MemoryMessageRepository) SaveDeviceCursor(userID int64, deviceID string, cursor int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deviceCursors[deviceKey(userID, deviceID)] = cursor
}

// FindLikelyDuplicateMessage searches for a message with the same sender, conversation, content, and within a time window.
func (r *MemoryMessageRepository) FindLikelyDuplicateMessage(senderID, conversationID int64, content string, within time.Duration) (model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := time.Now().Add(-within)
	for _, msg := range r.messages {
		if msg.SenderID == senderID &&
			msg.ConversationID == conversationID &&
			msg.Content == content &&
			msg.CreatedAt.After(cutoff) {
			return msg, nil
		}
	}
	return model.Message{}, ErrNotFound
}

// RefreshSenderPreview updates the sender's conversation summary with the latest message preview without changing the unread count.
func (r *MemoryMessageRepository) RefreshSenderPreview(userID, conversationID int64, msg model.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := summaryKey(userID, conversationID)
	s := r.summaries[key]
	s.UserID = userID
	s.ConversationID = conversationID
	if msg.ID >= s.LastMessageID {
		s.LastMessageID = msg.ID
		s.LastMessagePreview = msg.Content
	}
	s.UpdatedAt = time.Now()
	r.summaries[key] = s
}

// ---------- internal helpers (must be called with lock held) ----------

func (r *MemoryMessageRepository) appendEventLocked(userID int64, msg model.Message, eventType string) {
	if userID <= 0 {
		return
	}
	ev := model.SyncEvent{
		Seq:            r.nextEventSeq,
		UserID:         userID,
		DeviceID:       msg.DeviceID,
		ConversationID: msg.ConversationID,
		MessageID:      msg.ID,
		EventType:      eventType,
		MessageStatus:  msg.Status,
		CreatedAt:      time.Now(),
	}
	r.nextEventSeq++
	r.events[userID] = append(r.events[userID], ev)
}

func (r *MemoryMessageRepository) updateSummaryLocked(userID int64, msg model.Message, incrementUnread bool) {
	if userID <= 0 {
		return
	}
	key := summaryKey(userID, msg.ConversationID)
	s := r.summaries[key]
	s.UserID = userID
	s.ConversationID = msg.ConversationID
	s.LastMessageID = msg.ID
	s.LastMessagePreview = msg.Content
	if incrementUnread {
		s.UnreadCount++
	}
	s.UpdatedAt = time.Now()
	r.summaries[key] = s
}
