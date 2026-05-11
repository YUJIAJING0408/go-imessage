package repository

import (
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/model"
)

/*
@Date:
@Auth: YUJIAJING
@Desp:
*/

// 本文件包含已废弃的函数实现，仅用于性能对比基准测试。
// 生产代码应使用 message_repository.go 中的新版本。

// Deprecated: 使用 CreateMessage 替代，旧版不维护会话索引和去重索引。
func (r *MemoryMessageRepository) CreateMessageOld(msg model.Message) (model.Message, error) {
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

// Deprecated: 使用 CompleteAttempt 替代，旧版每次完成尝试时遍历全部 attempts 判断历史成功。
func (r *MemoryMessageRepository) CompleteAttemptOld(attemptID int64, success bool, errorCode string) (model.Message, error) {
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

// Deprecated: 使用 ListConversationMessages 替代，旧版全表扫描 r.messages 并按时间排序。
func (r *MemoryMessageRepository) ListConversationMessagesOld(conversationID int64, offset int, limit int) ([]model.Message, error) {
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

// Deprecated: 使用 FindLikelyDuplicateMessage 替代，旧版全表扫描匹配内容。
func (r *MemoryMessageRepository) FindLikelyDuplicateMessageOld(senderID, conversationID int64, content string, within time.Duration) (model.Message, error) {
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

// Deprecated: 使用 ListEventsAfter 替代，返回用户在给定游标之后的所有同步事件。
// 游标为 0 时从最早事件开始。结果按 seq 递增顺序（即插入顺序）。
func (r *MemoryMessageRepository) ListEventsAfterOld(userID int64, cursor int64) ([]model.SyncEvent, error) {
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
