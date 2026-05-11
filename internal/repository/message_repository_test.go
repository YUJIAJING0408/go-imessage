package repository

import (
	"errors"
	"fmt"
	"math/rand"
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
	msg, err := createMsg(repo, 1, 2, 10, "hello world", "") // clientMsgID 为空，会建立去重索引
	if err != nil {
		t.Fatalf("创建消息失败: %v", err)
	}

	// 在 1 秒窗口内应能找到（刚刚创建）
	found, err := repo.FindLikelyDuplicateMessage(1, 10, "hello world", 10*time.Second)
	if err != nil {
		t.Fatalf("期望找到重复消息，但返回错误: %v", err)
	}
	if found.ID != msg.ID {
		t.Error("找到的消息 ID 与期望不一致")
	}

	// 窗口为 0 表示消息创建时间必须在 now 之后（不可能），应返回 ErrNotFound
	_, err = repo.FindLikelyDuplicateMessage(1, 10, "hello world", 0)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("期望窗口外返回 ErrNotFound，但得到: %v", err)
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

// TestStartAttempt_OnDeletedMessage_ShouldFail 验证对已删除的消息尝试 StartAttempt 应返回错误，
// 防止已删除的消息被意外恢复。
func TestStartAttempt_OnDeletedMessage_ShouldFail(t *testing.T) {
	repo := newRepo()
	// 1. 创建一条消息
	msg, err := createMsg(repo, 1, 2, 10, "待删除消息", "")
	if err != nil {
		t.Fatalf("创建消息失败: %v", err)
	}

	// 2. 删除该消息
	_, err = repo.DeleteMessage(msg.ID)
	if err != nil {
		t.Fatalf("删除消息失败: %v", err)
	}

	// 3. 尝试对已删除的消息发起 StartAttempt，预期应返回错误
	_, err = repo.StartAttempt(msg.ID, "ghost-provider")
	if err == nil {
		t.Error("期望 StartAttempt 对已删除消息返回错误，但未返回错误")
	}

	// 4. 确认消息状态未被篡改
	final, _ := repo.GetMessage(msg.ID)
	if final.Status != model.MessageStatusDeleted {
		t.Errorf("已删除消息的状态应保持 deleted, 实际为 %s", final.Status)
	}
}

// TestCompleteAttempt_OnDeletedMessage_ShouldNotRevive
// 验证对已删除消息的 CompleteAttempt 不会将状态重新设为 sent。
// 场景：消息正在发送时被用户删除，随后服务商成功回调到达，状态应保持 deleted。
func TestCompleteAttempt_OnDeletedMessage_ShouldNotRevive(t *testing.T) {
	repo := newRepo()
	msg, err := createMsg(repo, 1, 2, 10, "竞态删除", "")
	if err != nil {
		t.Fatalf("创建消息失败: %v", err)
	}
	att, err := repo.StartAttempt(msg.ID, "prov-1")
	if err != nil {
		t.Fatalf("启动尝试失败: %v", err)
	}

	// 模拟并发：先删除消息，后到达成功回调
	_, err = repo.DeleteMessage(msg.ID)
	if err != nil {
		t.Fatalf("删除消息失败: %v", err)
	}

	// 此时消息已是 deleted，CompleteAttempt 应立即拒绝
	_, err = repo.CompleteAttempt(att.ID, true, "")
	if err == nil {
		t.Error("期望对已删除消息的 CompleteAttempt 返回错误，但未返回错误")
	}

	// 状态必须是 deleted
	final, _ := repo.GetMessage(msg.ID)
	if final.Status != model.MessageStatusDeleted {
		t.Errorf("状态应保持 deleted, 实际 %s", final.Status)
	}
}

// TestCompleteAttempt_OnSentMessage_ShouldNotChange
// 验证对已成功消息的重复成功回调不会改变状态。
func TestCompleteAttempt_OnSentMessage_ShouldNotChange(t *testing.T) {
	repo := newRepo()
	msg, err := createMsg(repo, 1, 2, 10, "重复成功回调", "")
	if err != nil {
		t.Fatalf("创建消息失败: %v", err)
	}
	att, err := repo.StartAttempt(msg.ID, "prov-1")
	if err != nil {
		t.Fatalf("启动尝试失败: %v", err)
	}

	// 首次成功
	_, err = repo.CompleteAttempt(att.ID, true, "")
	if err != nil {
		t.Fatalf("首次成功回调失败: %v", err)
	}

	// 重复成功回调应被拒绝
	_, err = repo.CompleteAttempt(att.ID, true, "")
	if err == nil {
		t.Error("期望对已成功的消息的重试回调返回错误，但未返回错误")
	}

	final, _ := repo.GetMessage(msg.ID)
	if final.Status != model.MessageStatusSent {
		t.Errorf("状态应保持 sent, 实际 %s", final.Status)
	}
}

// TestSaveMessage_VersionConflict 验证乐观锁：当并发修改了消息后，用旧版本调用 SaveMessage
// 应返回 ErrVersionConflict，且消息数据保持并发修改后的结果，不被旧快照覆盖。
func TestSaveMessage_VersionConflict(t *testing.T) {
	repo := newRepo()
	msg, err := createMsg(repo, 1, 2, 10, "冲突测试", "")
	if err != nil {
		t.Fatalf("创建消息失败: %v", err)
	}

	// 记录读取时的版本和 ActiveAttemptID
	expectedVersion := msg.Version
	oldActiveAttemptID := msg.ActiveAttemptID // 初始为 0

	// 模拟并发：另一个操作抢先修改了消息（例如启动一次发送尝试）
	attempt, err := repo.StartAttempt(msg.ID, "other-trace")
	if err != nil {
		t.Fatalf("模拟并发修改失败: %v", err)
	}
	// 此时消息版本已变，ActiveAttemptID 已更新
	concurrentAttemptID := attempt.ID

	// 用旧快照准备保存
	conflictMsg := msg // 旧快照（Status 可能为 sending，ActiveAttemptID 为 old）
	conflictMsg.Status = model.MessageStatusSending

	// 调用 SaveMessage 应返回版本冲突错误
	_, err = repo.SaveMessage(conflictMsg, expectedVersion)
	if err == nil {
		t.Fatal("期望返回版本冲突错误，但保存成功")
	}
	if !errors.Is(err, ErrVersionConflict) {
		t.Errorf("期望 ErrVersionConflict, 实际 %v", err)
	}

	// 验证数据未被旧快照覆盖
	final, _ := repo.GetMessage(msg.ID)
	if final.Version != expectedVersion+1 {
		t.Errorf("版本应为 %d（仅并发修改增加），实际 %d；若为 %d 则说明乐观锁失效",
			expectedVersion+1, final.Version, expectedVersion+2)
	}
	if final.ActiveAttemptID != concurrentAttemptID {
		t.Errorf("ActiveAttemptID 应保持并发修改后的值 %d, 实际 %d (可能被旧快照覆盖为 %d)",
			concurrentAttemptID, final.ActiveAttemptID, oldActiveAttemptID)
	}
	if final.Status != model.MessageStatusSending {
		t.Errorf("消息状态应为并发修改后的 sending, 实际 %s", final.Status)
	}
}

// TestSaveMessage_SuccessWhenVersionMatch 正常案例：版本匹配时保存成功
func TestSaveMessage_SuccessWhenVersionMatch(t *testing.T) {
	repo := newRepo()
	msg, err := createMsg(repo, 1, 2, 10, "正常保存", "")
	if err != nil {
		t.Fatalf("创建消息失败: %v", err)
	}

	expectedVersion := msg.Version
	// 无并发修改，直接保存
	msg.Status = model.MessageStatusSending
	saved, err := repo.SaveMessage(msg, expectedVersion)
	if err != nil {
		t.Fatalf("版本匹配时应保存成功，但返回错误: %v", err)
	}
	if saved.Version != expectedVersion+1 {
		t.Errorf("期望版本递增为 %d，实际 %d", expectedVersion+1, saved.Version)
	}
}

// TestDeleteMessage_UpdatesSummaryToPrevious 测试删除多条消息中的最后一条后，
// 双方的会话摘要应回退到倒数第二条消息。
func TestDeleteMessage_UpdatesSummaryToPrevious(t *testing.T) {
	repo := newRepo()
	// 创建两条消息：msg1 先发，msg2 后发
	msg1, err := createMsg(repo, 1, 2, 10, "第一条消息", "cid-1")
	if err != nil {
		t.Fatalf("创建 msg1 失败: %v", err)
	}
	msg2, err := createMsg(repo, 1, 2, 10, "第二条消息", "cid-2")
	if err != nil {
		t.Fatalf("创建 msg2 失败: %v", err)
	}
	// 让一条消息成功发送（未读置位等不是重点）
	att1, err := repo.StartAttempt(msg1.ID, "p1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.CompleteAttempt(att1.ID, true, "")
	if err != nil {
		t.Fatal(err)
	}
	// 删除第二条消息
	_, err = repo.DeleteMessage(msg2.ID)
	if err != nil {
		t.Fatalf("删除 msg2 失败: %v", err)
	}

	// 检查发送方(1)和接收方(2)的摘要
	for _, uid := range []int64{1, 2} {
		sum, err := repo.GetConversationSummary(uid, 10)
		if err != nil {
			t.Fatalf("获取用户%d摘要失败: %v", uid, err)
		}
		// 最后一条消息应为 msg1
		if sum.LastMessageID != msg1.ID {
			t.Errorf("用户%d: LastMessageID 应为 %d, 实际 %d", uid, msg1.ID, sum.LastMessageID)
		}
		if sum.LastMessagePreview != msg1.Content {
			t.Errorf("用户%d: LastMessagePreview 应为 %q, 实际 %q", uid, msg1.Content, sum.LastMessagePreview)
		}
	}
}

// TestDeleteMessage_WhenOnlyOne_SummaryCleared 测试删除会话中唯一的一条消息后，
// 摘要应重置为空状态。
func TestDeleteMessage_WhenOnlyOne_SummaryCleared(t *testing.T) {
	repo := newRepo()
	msg, err := createMsg(repo, 1, 2, 10, "唯一消息", "cid-only")
	if err != nil {
		t.Fatalf("创建消息失败: %v", err)
	}
	// 完成发送使其进入 sent 状态
	att, _ := repo.StartAttempt(msg.ID, "p")
	repo.CompleteAttempt(att.ID, true, "")

	// 删除该消息
	_, err = repo.DeleteMessage(msg.ID)
	if err != nil {
		t.Fatalf("删除失败: %v", err)
	}

	// 验证两个用户的摘要均已清空
	for _, uid := range []int64{1, 2} {
		sum, _ := repo.GetConversationSummary(uid, 10)
		if sum.LastMessageID != 0 {
			t.Errorf("用户%d: LastMessageID 应为 0, 实际 %d", uid, sum.LastMessageID)
		}
		if sum.LastMessagePreview != "" {
			t.Errorf("用户%d: LastMessagePreview 应为空, 实际 %q", uid, sum.LastMessagePreview)
		}
	}
}

// TestDeleteMessage_WhenDeletingOlderMessage_SummaryUnchanged 验证删除较早的消息后，
// 会话摘要仍显示最新的消息。
func TestDeleteMessage_WhenDeletingOlderMessage_SummaryUnchanged(t *testing.T) {
	repo := newRepo()
	// 创建两条消息，msg1 先发，msg2 后发
	msg1, err := createMsg(repo, 1, 2, 10, "第一条消息", "cid-1")
	if err != nil {
		t.Fatalf("创建 msg1 失败: %v", err)
	}
	msg2, err := createMsg(repo, 1, 2, 10, "第二条消息", "cid-2")
	if err != nil {
		t.Fatalf("创建 msg2 失败: %v", err)
	}
	// 完成发送（确保状态正确）
	att1, _ := repo.StartAttempt(msg1.ID, "p1")
	repo.CompleteAttempt(att1.ID, true, "")
	att2, _ := repo.StartAttempt(msg2.ID, "p2")
	repo.CompleteAttempt(att2.ID, true, "")

	// 删除第一条（较早的）消息
	_, err = repo.DeleteMessage(msg1.ID)
	if err != nil {
		t.Fatalf("删除 msg1 失败: %v", err)
	}

	// 验证双方的摘要仍然指向 msg2
	for _, uid := range []int64{1, 2} {
		sum, err := repo.GetConversationSummary(uid, 10)
		if err != nil {
			t.Fatalf("获取用户%d摘要失败: %v", uid, err)
		}
		if sum.LastMessageID != msg2.ID {
			t.Errorf("用户%d: LastMessageID 应为 %d, 实际 %d", uid, msg2.ID, sum.LastMessageID)
		}
		if sum.LastMessagePreview != msg2.Content {
			t.Errorf("用户%d: LastMessagePreview 应为 %q, 实际 %q", uid, msg2.Content, sum.LastMessagePreview)
		}
	}
}

// ---------------- benchmark ----------------

// 辅助函数：填充仓库，返回仓库实例和所有创建消息的 ID 列表
func populateRepo(b *testing.B, newVersion bool, msgCount int) (*MemoryMessageRepository, []int64) {
	repo := NewMemoryMessageRepository()
	ids := make([]int64, 0, msgCount)
	create := repo.CreateMessage
	if !newVersion {
		create = repo.CreateMessageOld
	}
	for i := 0; i < msgCount; i++ {
		msg, err := create(model.Message{
			SenderID:       int64(rand.Intn(100) + 1),
			ReceiverID:     int64(rand.Intn(100) + 1),
			ConversationID: int64(rand.Intn(500) + 1),
			Content:        fmt.Sprintf("message-%d-%s", i, randomString(8)),
			ClientMsgID:    fmt.Sprintf("cid-%d", i),
		})
		if err != nil {
			b.Fatal(err)
		}
		ids = append(ids, msg.ID)
	}
	return repo, ids
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// BenchmarkCreateMessage 对比新旧 CreateMessage（大规模插入）。
// 注意：此处直接在 b.N 循环中插入新消息，每次迭代插入一条，不重置计时器。
// BenchmarkCreateMessage/New-24         	  990188	      3188 ns/op
// BenchmarkCreateMessage/Old-24         	 1000000	      1364 ns/op
func BenchmarkCreateMessage(b *testing.B) {
	b.Run("New", func(b *testing.B) {
		repo := NewMemoryMessageRepository()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = repo.CreateMessage(model.Message{
				SenderID:       int64(rand.Intn(100) + 1),
				ReceiverID:     int64(rand.Intn(100) + 1),
				ConversationID: int64(rand.Intn(500) + 1),
				Content:        fmt.Sprintf("bench-%d-%s", i, randomString(8)),
				ClientMsgID:    fmt.Sprintf("bench-cid-%d", i),
			})
		}
	})

	b.Run("Old", func(b *testing.B) {
		repo := NewMemoryMessageRepository()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = repo.CreateMessageOld(model.Message{
				SenderID:       int64(rand.Intn(100) + 1),
				ReceiverID:     int64(rand.Intn(100) + 1),
				ConversationID: int64(rand.Intn(500) + 1),
				Content:        fmt.Sprintf("bench-%d-%s", i, randomString(8)),
				ClientMsgID:    fmt.Sprintf("bench-cid-%d", i),
			})
		}
	})
}

// BenchmarkCompleteAttempt 对比新旧 CompleteAttempt。
// preload=1W，使用 1 万条消息和 1 万次尝试作为背景
// BenchmarkCompleteAttempt/New-24         	10503548	       100.1 ns/op
// BenchmarkCompleteAttempt/Old-24         	 3706884	       277.0 ns/op
// preload=5W，使用 5 万条消息和 5 万次尝试作为背景
// BenchmarkCompleteAttempt/New-24         	 7886774	       140.9 ns/op
// BenchmarkCompleteAttempt/Old-24         	    3824	    294801 ns/op
func BenchmarkCompleteAttempt(b *testing.B) {
	const preload = 10_000

	b.Run("New", func(b *testing.B) {
		repo, _ := populateRepo(b, true, preload)
		// 为每条消息创建一次 attempt，以便 CompleteAttempt 能查找
		for _, mid := range repo.messages {
			_, _ = repo.StartAttempt(mid.ID, "pre-trace")
		}
		// 随机选择一个已有 attempt 进行 Complete
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 随机挑选一个 attempt
			var attemptID int64
			for aid := range repo.attempts {
				attemptID = aid
				break
			}
			_, _ = repo.CompleteAttempt(attemptID, true, "")
		}
	})

	b.Run("Old", func(b *testing.B) {
		repo, _ := populateRepo(b, false, preload)
		for _, mid := range repo.messages {
			_, _ = repo.StartAttempt(mid.ID, "pre-trace")
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var attemptID int64
			for aid := range repo.attempts {
				attemptID = aid
				break
			}
			_, _ = repo.CompleteAttemptOld(attemptID, true, "")
		}
	})
}

// BenchmarkListConversationMessages 对比列表查询
// preload=1W，1 万条消息随机分布在 500 个会话。
// BenchmarkListConversationMessages/New-24         	  749229	      1773 ns/op
// BenchmarkListConversationMessages/Old-24         	   12202	     95182 ns/op
// preload=51W，5 万条消息随机分布在 500 个会话。
// BenchmarkListConversationMessages/New-24         	  191857	      6085 ns/op
// BenchmarkListConversationMessages/Old-24         	    2192	    650040 ns/op
func BenchmarkListConversationMessages(b *testing.B) {
	const preload = 10_000

	b.Run("New", func(b *testing.B) {
		repo, _ := populateRepo(b, true, preload)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			convID := int64(rand.Intn(500) + 1)
			_, _ = repo.ListConversationMessages(convID, 0, 20)
		}
	})

	b.Run("Old", func(b *testing.B) {
		repo, _ := populateRepo(b, false, preload)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			convID := int64(rand.Intn(500) + 1)
			_, _ = repo.ListConversationMessagesOld(convID, 0, 20)
		}
	})
}

// BenchmarkFindLikelyDuplicateMessage 对比去重查找，
// preload=1W，1 万条消息，随机选取存在的消息内容。
// BenchmarkFindLikelyDuplicateMessage/New-24         	12373200	       102.0 ns/op
// BenchmarkFindLikelyDuplicateMessage/Old-24         	   25911	     48649 ns/op
// preload=5W，5 万条消息，随机选取存在的消息内容。
// BenchmarkFindLikelyDuplicateMessage/New-24         	10252638	       114.8 ns/op
// BenchmarkFindLikelyDuplicateMessage/Old-24         	    4352	    250318 ns/op
func BenchmarkFindLikelyDuplicateMessage(b *testing.B) {
	const preload = 10_000

	b.Run("New", func(b *testing.B) {
		repo, ids := populateRepo(b, true, preload)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 随机选一条已有消息，获取其内容进行查找
			msgID := ids[rand.Intn(len(ids))]
			msg, _ := repo.GetMessage(msgID)
			_, _ = repo.FindLikelyDuplicateMessage(msg.SenderID, msg.ConversationID, msg.Content, 30*time.Second)
		}
	})

	b.Run("Old", func(b *testing.B) {
		repo, ids := populateRepo(b, false, preload)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			msgID := ids[rand.Intn(len(ids))]
			msg, _ := repo.GetMessage(msgID)
			_, _ = repo.FindLikelyDuplicateMessageOld(msg.SenderID, msg.ConversationID, msg.Content, 30*time.Second)
		}
	})
}

// BenchmarkListEventsAfter 对比 ListEventsAfter 新旧实现性能。
// 准备 10 万条事件，cursor 设置为 0（从最早开始，旧版需扫描所有事件）。
// BenchmarkListEventsAfter/New-24         	93161191	        13.66 ns/op
// BenchmarkListEventsAfter/Old-24         	     145	   7532661 ns/op
func BenchmarkListEventsAfter(b *testing.B) {
	const userID = 1
	const eventCount = 100_000

	setup := func(newVersion bool) *MemoryMessageRepository {
		repo := NewMemoryMessageRepository()
		repo.mu.Lock()
		for i := 0; i < eventCount; i++ {
			repo.nextEventSeq++
			seq := repo.nextEventSeq
			ev := model.SyncEvent{
				Seq:            seq,
				UserID:         userID,
				ConversationID: 10,
				MessageID:      int64(i + 1),
				EventType:      model.EventTypeMessageCreated,
				MessageStatus:  model.MessageStatusSent,
				CreatedAt:      time.Now(),
			}
			repo.events[userID] = append(repo.events[userID], ev)
		}
		repo.mu.Unlock()
		return repo
	}

	b.Run("New", func(b *testing.B) {
		repo := setup(true) // 无所谓版本，存储结构相同
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = repo.ListEventsAfter(userID, 0) // cursor=0 最坏情况
		}
	})

	b.Run("Old", func(b *testing.B) {
		repo := setup(false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = repo.ListEventsAfterOld(userID, 0)
		}
	})
}
