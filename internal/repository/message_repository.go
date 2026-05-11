package repository

import (
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/model"
)

// ErrNotFound 表示目标实体未找到。
var ErrNotFound = errors.New("not found")
var ErrVersionConflict = errors.New("version conflict: message has been modified concurrently")

// MessageRepository 定义消息持久化的操作集合。
// 所有操作都应是并发安全的。
type MessageRepository interface {
	CreateMessage(msg model.Message) (model.Message, error)
	FindByClientMsgID(senderID int64, clientMsgID string) (model.Message, error)
	GetMessage(id int64) (model.Message, error)
	SaveMessage(msg model.Message, expectedVersion int64) (model.Message, error)
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

// MemoryMessageRepository 是 MessageRepository 的内存实现。
// 使用 RWMutex 保证并发安全，主要数据结构：
//
//	messages      - 消息 ID -> Message
//	attempts      - 尝试 ID -> DeliveryAttempt
//	events        - 用户 ID -> 该用户的有序 SyncEvent 切片
//	summaries     - "userID:conversationID" -> ConversationSummary
//	deviceCursors - "userID:deviceID" -> 游标值
//	clientMsgIndex- "senderID:clientMsgID" -> 消息 ID（用于幂等）
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

	clientMsgIndex map[string]int64
}

// NewMemoryMessageRepository 创建一个新的内存仓储实例，初始化所有 map 和计数器。
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

// ---------- 内部辅助函数 ----------

// summaryKey 生成会话摘要的存储键。
// 格式："userID:conversationID"，使用 FormatInt 避免 int64 截断问题。
func summaryKey(userID, conversationID int64) string {
	return strconv.FormatInt(userID, 10) + ":" + strconv.FormatInt(conversationID, 10)
}

// deviceKey 生成设备游标的存储键。
// 格式："userID:deviceID"。
func deviceKey(userID int64, deviceID string) string {
	return strconv.FormatInt(userID, 10) + ":" + deviceID
}

// ---------- 公共方法 ----------

// CreateMessage 创建一条新消息。
// 自动分配 ID、设置创建时间、初始化状态为 sending（若未指定）。
// 同时维护 clientMsgIndex 索引，并产生创建事件和发送方会话摘要。
// 返回创建后的消息或错误。
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

// FindByClientMsgID 通过客户端的消息 ID 查找消息。
// 参数 senderID 用于隔离不同发送者的 ID 空间。
// 若不存在返回 ErrNotFound。
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

// GetMessage 根据 ID 获取消息，不存在时返回 ErrNotFound。
func (r *MemoryMessageRepository) GetMessage(id int64) (model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	msg, ok := r.messages[id]
	if !ok {
		return model.Message{}, ErrNotFound
	}
	return msg, nil
}

// SaveMessage 更新已有消息（使用乐观锁）。
// 参数：
//
//	msg             - 期望保存的消息快照。
//	expectedVersion - 调用方在读取时记录的版本号。
//
// 若 expectedVersion 与存储中的版本不匹配，表示并发冲突，返回 ErrVersionConflict。
// 若消息不存在，返回 ErrNotFound。
// 保存成功后返回更新后的消息（版本+1，UpdatedAt 刷新）。
func (r *MemoryMessageRepository) SaveMessage(msg model.Message, expectedVersion int64) (model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.messages[msg.ID]
	if !ok {
		return model.Message{}, ErrNotFound
	}

	// 乐观锁：期望版本与当前版本不一致，说明有并发修改
	if existing.Version != expectedVersion {
		return model.Message{}, ErrVersionConflict
	}

	msg.Version++
	msg.UpdatedAt = time.Now()
	r.messages[msg.ID] = msg
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	r.appendEventLocked(msg.ReceiverID, msg, model.EventTypeMessageUpdated)
	return msg, nil
}

// StartAttempt 开始一次新的发送尝试。
// 为消息追加一条 running 状态的 DeliveryAttempt，并更新消息的 ActiveAttemptID 和状态为 sending。
// 若消息不存在，返回 ErrNotFound。
func (r *MemoryMessageRepository) StartAttempt(messageID int64, providerTraceID string) (model.DeliveryAttempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	msg, ok := r.messages[messageID]
	if !ok {
		return model.DeliveryAttempt{}, ErrNotFound
	}

	// 防御性检查：已删除的消息不允许发起新的发送尝试
	if msg.Status == model.MessageStatusDeleted {
		return model.DeliveryAttempt{}, errors.New("cannot start attempt on a deleted message")
	}

	// 确定尝试序号
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

// CompleteAttempt 完成一次发送尝试。
// 根据成功/失败更新尝试状态和消息状态。
// 关键规则：
//   - 首次成功时会递增接收方的未读计数；若已有历史成功，则不再递增（防止未读膨胀）。
//   - 失败时若消息历史上已有成功尝试，则保持 sent 状态，防止状态回退。
//
// 同时广播 update 事件给双方，并更新发送方会话摘要。
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

	// 终态保护：已删除或已成功的消息不允许再变更状态
	if msg.Status == model.MessageStatusDeleted || msg.Status == model.MessageStatusSent {
		return model.Message{}, errors.New("message is already in a terminal state")
	}

	now := time.Now()
	att.FinishedAt = &now
	att.ErrorCode = errorCode

	if success {
		att.Status = model.AttemptStatusSuccess
		msg.Status = model.MessageStatusSent
	} else {
		att.Status = model.AttemptStatusFailed
		// 检查是否已经有过成功的尝试，若有则保持 sent 状态不变
		hadSuccess := false
		for _, a := range r.attempts {
			if a.MessageID == msg.ID && a.ID != attemptID && a.Status == model.AttemptStatusSuccess {
				hadSuccess = true
				break
			}
		}
		if hadSuccess {
			msg.Status = model.MessageStatusSent
		} else {
			msg.Status = model.MessageStatusFailed
		}
	}

	msg.Version++
	msg.UpdatedAt = now

	r.attempts[attemptID] = att
	r.messages[msg.ID] = msg

	// 向双方广播状态变更事件
	r.appendEventLocked(msg.SenderID, msg, model.EventTypeMessageUpdated)
	r.appendEventLocked(msg.ReceiverID, msg, model.EventTypeMessageUpdated)

	// 总是更新发送方会话摘要（预览）
	r.updateSummaryLocked(msg.SenderID, msg, false)

	// 仅在首次成功时增加接收方未读数
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

// DeleteMessage 将消息标记为已删除。
// 设置删除时间、状态为 deleted，并广播删除事件给发送方和接收方。
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

// ListConversationMessages 获取指定会话的消息列表，按创建时间降序。
// offset/limit 实现分页，已删除的消息会被过滤。
// 调用方可在 service 层补充 attempt 数量等信息。
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

// CountAttempts 统计某条消息的发送尝试次数。
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

// GetConversationSummary 获取用户与某会话的摘要视图。
// 若不存在返回空摘要（UserID、ConversationID 已赋值，其他字段为零值）。
func (r *MemoryMessageRepository) GetConversationSummary(userID int64, conversationID int64) (model.ConversationSummary, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, ok := r.summaries[summaryKey(userID, conversationID)]
	if !ok {
		return model.ConversationSummary{UserID: userID, ConversationID: conversationID}, nil
	}
	return s, nil
}

// MarkConversationRead 将指定会话的未读计数清零。
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

// ListEventsAfter 返回用户在给定游标之后的所有同步事件。
// 游标为 0 时从最早事件开始。结果按 seq 递增顺序（即插入顺序）。
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

// GetDeviceCursor 获取设备上一次同步的游标。
func (r *MemoryMessageRepository) GetDeviceCursor(userID int64, deviceID string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.deviceCursors[deviceKey(userID, deviceID)]
}

// SaveDeviceCursor 保存设备的最新同步游标。
func (r *MemoryMessageRepository) SaveDeviceCursor(userID int64, deviceID string, cursor int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deviceCursors[deviceKey(userID, deviceID)] = cursor
}

// FindLikelyDuplicateMessage 搜索在指定时间窗口内，同一会话中内容相同的消息。
// 用于老版本客户端（无 clientMsgID）的内容去重。
// 找到返回消息对象，否则返回 ErrNotFound。
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

// RefreshSenderPreview 更新发送方的会话摘要，主要刷新最后一条消息预览和更新时间，
// 但不改变未读计数（适用于重试等场景）。
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

// ---------- 内部辅助函数（调用前必须持有锁） ----------

// appendEventLocked 为目标用户追加一条同步事件。若 userID <= 0 则跳过。
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

// updateSummaryLocked 更新用户会话摘要。若 incrementUnread 为 true 则未读数加 1。
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
