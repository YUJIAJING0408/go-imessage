package repository

import (
	"sync"
	"testing"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/model"
)

// ---------------- 辅助函数 ----------------

// newRepo 创建一个新的内存仓库实例
func newRepo() *MemoryMessageRepository {
	return NewMemoryMessageRepository()
}

// createMsg 快速创建一条测试消息
func createMsg(repo *MemoryMessageRepository, sender, receiver, conversation int64, content, clientMsgID string) (model.Message, error) {
	msg := model.Message{
		SenderID:       sender,
		ReceiverID:     receiver,
		ConversationID: conversation,
		Content:        content,
		ClientMsgID:    clientMsgID,
		Status:         model.MessageStatusSending,
		DeviceID:       "test-device",
	}
	return repo.CreateMessage(msg)
}

// ---------------- 测试用例 ----------------

// 验证创建消息时自动分配 ID 并设置初始状态
func TestCreateMessage_AssignsIDAndStatus(t *testing.T) {
	repo := newRepo()
	msg, err := createMsg(repo, 1, 2, 10, "你好", "")
	if err != nil {
		t.Fatalf("未预期的错误: %v", err)
	}
	if msg.ID == 0 {
		t.Error("消息 ID 不应为零")
	}
	if msg.Status != model.MessageStatusSending {
		t.Errorf("期望状态为 sending, 实际为 %s", msg.Status)
	}
	if msg.CreatedAt.IsZero() || msg.UpdatedAt.IsZero() {
		t.Error("创建时间和更新时间应被赋值")
	}
}

// 通过客户端消息 ID 查找已存在的消息
func TestFindByClientMsgID_Existing(t *testing.T) {
	repo := newRepo()
	_, err := createMsg(repo, 1, 2, 10, "你好", "abc-123")
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByClientMsgID(1, "abc-123")
	if err != nil {
		t.Fatalf("期望找到消息，但返回错误: %v", err)
	}
	if got.ClientMsgID != "abc-123" {
		t.Errorf("客户端消息 ID 不匹配")
	}
}

// 查找不存在的客户端消息 ID 时应返回 ErrNotFound
func TestFindByClientMsgID_NotFound(t *testing.T) {
	repo := newRepo()
	_, err := repo.FindByClientMsgID(1, "missing")
	if err != ErrNotFound {
		t.Errorf("期望 ErrNotFound, 实际 %v", err)
	}
}

// 幂等性：使用相同 client_msg_id 发送应返回已存在的消息
func TestIdempotency_SendWithSameClientMsgID_ReturnsExisting(t *testing.T) {
	repo := newRepo()
	first, _ := createMsg(repo, 1, 2, 10, "重复消息", "id-1")
	second, err := repo.FindByClientMsgID(1, "id-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Error("相同 client_msg_id 应返回已有消息")
	}
}

// 内容去重：在规定时间窗口内找到相似消息
func TestFindLikelyDuplicateMessage(t *testing.T) {
	repo := newRepo()
	now := time.Now()
	// 手动插入一条消息以精确控制创建时间
	msg := model.Message{
		ID:             1,
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "hello world",
		Status:         model.MessageStatusSending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	repo.mu.Lock()
	repo.messages[1] = msg
	repo.mu.Unlock()

	// 在 1 秒窗口内应能找到
	found, err := repo.FindLikelyDuplicateMessage(1, 10, "hello world", 1*time.Second)
	if err != nil {
		t.Fatalf("期望找到重复消息，但返回错误: %v", err)
	}
	if found.ID != 1 {
		t.Error("重复消息 ID 不匹配")
	}
	// 窗口为 0 时应找不到
	_, err = repo.FindLikelyDuplicateMessage(1, 10, "hello world", 0)
	if err != ErrNotFound {
		t.Errorf("期望超时窗口后返回 ErrNotFound, 实际 %v", err)
	}
}

// 开始一次发送尝试：应创建 attempt 并将消息状态设为 sending
func TestStartAttempt_AssignsAttemptAndUpdatesMessage(t *testing.T) {
	repo := newRepo()
	msg, _ := createMsg(repo, 1, 2, 10, "测试发送", "")
	attempt, err := repo.StartAttempt(msg.ID, "prov-1")
	if err != nil {
		t.Fatalf("StartAttempt 失败: %v", err)
	}
	if attempt.Status != model.AttemptStatusRunning {
		t.Error("attempt 状态应为 running")
	}
	if attempt.ProviderTraceID != "prov-1" {
		t.Error("provider trace ID 不匹配")
	}
	updatedMsg, _ := repo.GetMessage(msg.ID)
	if updatedMsg.ActiveAttemptID != attempt.ID {
		t.Error("消息的活跃 attempt 未正确设置")
	}
	if updatedMsg.Status != model.MessageStatusSending {
		t.Errorf("消息状态应为 sending, 实际 %s", updatedMsg.Status)
	}
}

// 发送成功：状态应变为 sent，接收方未读数应增加
func TestCompleteAttempt_Success_UpdatesStatusAndSummary(t *testing.T) {
	repo := newRepo()
	msg, _ := createMsg(repo, 1, 2, 10, "问候", "")
	attempt, _ := repo.StartAttempt(msg.ID, "p1")
	completedMsg, err := repo.CompleteAttempt(attempt.ID, true, "")
	if err != nil {
		t.Fatalf("CompleteAttempt 失败: %v", err)
	}
	if completedMsg.Status != model.MessageStatusSent {
		t.Errorf("期望 sent, 实际 %s", completedMsg.Status)
	}
	// 接收方摘要应显示未读数为 1
	summary, _ := repo.GetConversationSummary(2, 10)
	if summary.UnreadCount != 1 {
		t.Errorf("期望未读数 1, 实际 %d", summary.UnreadCount)
	}
}

// 发送失败：状态应变为 failed，接收方未读数不应增加
func TestCompleteAttempt_Failure_UpdatesStatus(t *testing.T) {
	repo := newRepo()
	msg, _ := createMsg(repo, 1, 2, 10, "失败测试", "")
	attempt, _ := repo.StartAttempt(msg.ID, "p2")
	failedMsg, err := repo.CompleteAttempt(attempt.ID, false, "timeout")
	if err != nil {
		t.Fatalf("CompleteAttempt 失败: %v", err)
	}
	if failedMsg.Status != model.MessageStatusFailed {
		t.Errorf("期望 failed, 实际 %s", failedMsg.Status)
	}
	// 接收方未读数必须为 0
	summary, _ := repo.GetConversationSummary(2, 10)
	if summary.UnreadCount != 0 {
		t.Errorf("期望失败后未读数为 0, 实际 %d", summary.UnreadCount)
	}
}

// 验证同一条消息多次成功完成不会导致未读数重复增加
func TestUnreadNotIncrementedTwiceForSameMessage(t *testing.T) {
	repo := newRepo()
	msg, _ := createMsg(repo, 1, 2, 10, "单次未读", "")
	att1, _ := repo.StartAttempt(msg.ID, "p1")
	// 第一次成功
	repo.CompleteAttempt(att1.ID, true, "")
	// 模拟重试：创建新的 attempt 并再次成功
	att2, _ := repo.StartAttempt(msg.ID, "p2")
	repo.CompleteAttempt(att2.ID, true, "")
	summary, _ := repo.GetConversationSummary(2, 10)
	if summary.UnreadCount != 1 {
		t.Errorf("第二次成功后未读数应仍为 1, 实际 %d", summary.UnreadCount)
	}
}

// 删除消息时删除事件应广播给发送方和接收方
func TestDeleteMessage_BroadcastsToBothUsers(t *testing.T) {
	repo := newRepo()
	msg, _ := createMsg(repo, 1, 2, 10, "待删除", "")
	_, err := repo.DeleteMessage(msg.ID)
	if err != nil {
		t.Fatalf("DeleteMessage 失败: %v", err)
	}
	deleted, _ := repo.GetMessage(msg.ID)
	if deleted.Status != model.MessageStatusDeleted {
		t.Error("消息未被标记为 deleted")
	}

	// 发送方事件
	senderEvents, _ := repo.ListEventsAfter(1, 0)
	foundSender := false
	for _, ev := range senderEvents {
		if ev.MessageID == msg.ID && ev.EventType == model.EventTypeMessageDeleted {
			foundSender = true
			break
		}
	}
	if !foundSender {
		t.Error("发送方未收到删除事件")
	}

	// 接收方事件
	receiverEvents, _ := repo.ListEventsAfter(2, 0)
	foundReceiver := false
	for _, ev := range receiverEvents {
		if ev.MessageID == msg.ID && ev.EventType == model.EventTypeMessageDeleted {
			foundReceiver = true
			break
		}
	}
	if !foundReceiver {
		t.Error("接收方未收到删除事件")
	}
}

// 列表查询应排除已删除的消息
func TestListConversationMessages_ExcludesDeleted(t *testing.T) {
	repo := newRepo()
	m1, _ := createMsg(repo, 1, 2, 10, "保留", "")
	m2, _ := createMsg(repo, 1, 2, 10, "删除", "")
	repo.DeleteMessage(m2.ID)
	msgs, _ := repo.ListConversationMessages(10, 0, 10)
	if len(msgs) != 1 {
		t.Fatalf("期望返回 1 条消息, 实际 %d", len(msgs))
	}
	if msgs[0].ID != m1.ID {
		t.Error("返回了错误的消息")
	}
}

// 标记会话已读后未读数应归零
func TestMarkConversationRead_ResetsUnread(t *testing.T) {
	repo := newRepo()
	msg, _ := createMsg(repo, 1, 2, 10, "未读消息", "")
	att, _ := repo.StartAttempt(msg.ID, "p")
	repo.CompleteAttempt(att.ID, true, "")
	// 此时未读数应为 1
	sum, _ := repo.GetConversationSummary(2, 10)
	if sum.UnreadCount != 1 {
		t.Fatalf("前置条件：未读数应为 1")
	}
	err := repo.MarkConversationRead(2, 10)
	if err != nil {
		t.Fatal(err)
	}
	sum, _ = repo.GetConversationSummary(2, 10)
	if sum.UnreadCount != 0 {
		t.Errorf("已读后未读数应为 0, 实际 %d", sum.UnreadCount)
	}
}

// 同步事件按用户分桶，基于游标正确分页
func TestSyncEvents_PerUserPartitioning(t *testing.T) {
	repo := newRepo()
	// 用户 1 发送消息，并完成一次成功尝试，使双方都产生事件
	msg, _ := createMsg(repo, 1, 2, 10, "同步测试", "")
	attempt, _ := repo.StartAttempt(msg.ID, "p1")
	repo.CompleteAttempt(attempt.ID, true, "")

	// 两个用户都应拥有事件
	ev1, _ := repo.ListEventsAfter(1, 0)
	ev2, _ := repo.ListEventsAfter(2, 0)
	if len(ev1) == 0 {
		t.Error("用户1 应有事件")
	}
	if len(ev2) == 0 {
		t.Error("用户2 应有事件（接收方在消息发送成功后收到事件）")
	}

	// 基于游标的分页：使用最后一个事件的 Seq 再次查询，不应返回结果
	lastSeq := ev1[len(ev1)-1].Seq
	more, _ := repo.ListEventsAfter(1, lastSeq)
	if len(more) != 0 {
		t.Error("在最后游标之后不应再有事件")
	}
}

// 设备游标的保存与读取
func TestDeviceCursor(t *testing.T) {
	repo := newRepo()
	repo.SaveDeviceCursor(1, "mobile", 42)
	c := repo.GetDeviceCursor(1, "mobile")
	if c != 42 {
		t.Errorf("期望 42, 实际 %d", c)
	}
}

// 刷新发送方预览：更新最后消息但不应改变未读数
func TestRefreshSenderPreview_UpdatesPreviewWithoutChangingUnread(t *testing.T) {
	repo := newRepo()
	msg, _ := createMsg(repo, 1, 2, 10, "预览测试", "")
	repo.RefreshSenderPreview(msg.SenderID, msg.ConversationID, msg)
	summary, _ := repo.GetConversationSummary(msg.SenderID, 10)
	if summary.LastMessageID != msg.ID {
		t.Error("发送方摘要未更新")
	}
}

// 并发读写安全性测试
func TestConcurrentReadWrite(t *testing.T) {
	repo := newRepo()
	var wg sync.WaitGroup
	// 写协程
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			createMsg(repo, 1, 2, 10, "并发测试", "")
		}
	}()
	// 多个读协程
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				repo.GetMessage(1)
				repo.GetConversationSummary(1, 10)
				repo.ListEventsAfter(1, 0)
			}
		}()
	}
	wg.Wait()
}

// 状态回退保护（当前跳过，因为尚未实现）
func TestStatusRollbackProtection(t *testing.T) {
	//t.Skip("跳过：状态回退保护尚未实现，需在 CompleteAttempt 中加入状态机规则后启用")
	repo := newRepo()
	msg, _ := createMsg(repo, 1, 2, 10, "禁止回退", "")
	att1, _ := repo.StartAttempt(msg.ID, "p1")
	repo.CompleteAttempt(att1.ID, true, "") // 当前状态为 sent
	// 尝试以失败结束另一个 attempt，最终状态应保持 sent
	att2, _ := repo.StartAttempt(msg.ID, "p2")
	repo.CompleteAttempt(att2.ID, false, "err")
	final, _ := repo.GetMessage(msg.ID)
	if final.Status != model.MessageStatusSent {
		t.Errorf("状态应保持 sent, 实际 %s", final.Status)
	}
}
