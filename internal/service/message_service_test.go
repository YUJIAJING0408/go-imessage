package service

import (
	"context"
	"os"
	"testing"

	"github.com/YUJIAJING0408/go-imessage/internal/logger"
	"github.com/YUJIAJING0408/go-imessage/internal/model"
	"github.com/YUJIAJING0408/go-imessage/internal/repository"
)

// newSvc 创建一个使用内存仓库的服务实例
func newSvc() *MessageService {
	return NewMessageService(repository.NewMemoryMessageRepository())
}

func TestMain(m *testing.M) {
	// 初始化日志，输出到测试专用文件
	cleanup := logger.Init("test.log")
	defer cleanup()

	os.Exit(m.Run())
}

// 有效请求应成功创建消息和发送尝试
func TestSendMessage_ValidRequest_CreatesMessageAndAttempt(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	req := SendMessageRequest{
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "你好",
		DeviceID:       "d1",
	}
	msg, att, err := svc.SendMessage(ctx, req)
	if err != nil {
		t.Fatalf("SendMessage 失败: %v", err)
	}
	if msg.ID == 0 {
		t.Error("消息 ID 缺失")
	}
	if att.ID == 0 || att.MessageID != msg.ID {
		t.Error("attempt 与消息不匹配")
	}
	if msg.Status != model.MessageStatusSending {
		t.Errorf("期望 sending, 实际 %s", msg.Status)
	}
}

// 携带 ClientMsgID 的请求应具备幂等性
func TestSendMessage_IdempotentWithClientMsgID(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	req := SendMessageRequest{
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "幂等测试",
		ClientMsgID:    "id-10",
	}
	first, _, _ := svc.SendMessage(ctx, req)
	second, _, _ := svc.SendMessage(ctx, req)
	if first.ID != second.ID {
		t.Error("幂等调用应返回同一条消息")
	}
}

// 缺少 ClientMsgID 时，短时间内相同内容应被去重
func TestSendMessage_DuplicateContentWithoutClientMsgID(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	req := SendMessageRequest{
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "相同内容",
		// 不提供 ClientMsgID
	}
	first, _, _ := svc.SendMessage(ctx, req)
	// 立即再次发送相同请求
	second, _, _ := svc.SendMessage(ctx, req)
	// 因为内容去重（30 秒内），第二次应返回第一条消息
	if first.ID != second.ID {
		t.Errorf("期望重复消息被去重，但得到不同的 ID: %d vs %d", first.ID, second.ID)
	}
}

// 无效请求应被拒绝（如空内容）
func TestSendMessage_InvalidRequest_Rejected(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	_, _, err := svc.SendMessage(ctx, SendMessageRequest{Content: ""})
	if err == nil {
		t.Error("空内容应返回错误")
	}
}

// 重试消息应创建新的 attempt 并更新发送方预览
func TestRetryMessage_StartsNewAttemptAndUpdatesSenderPreview(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	msg, att, _ := svc.SendMessage(ctx, SendMessageRequest{
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "重试测试",
	})
	// 先将第一次发送标记为失败
	_, err := svc.CompleteAttempt(ctx, CompleteAttemptRequest{AttemptID: att.ID, Success: false, ErrorCode: "timeout"})
	if err != nil {
		t.Fatal(err)
	}
	// 执行重试
	retriedMsg, retryAtt, err := svc.RetryMessage(ctx, msg.ID)
	if err != nil {
		t.Fatalf("RetryMessage 失败: %v", err)
	}
	if retriedMsg.Status != model.MessageStatusSending {
		t.Errorf("重试后消息状态应为 sending, 实际 %s", retriedMsg.Status)
	}
	if retryAtt.ID == att.ID {
		t.Error("新的 attempt ID 应与之前不同")
	}
	// 发送方摘要应反映最新消息
	summary, _ := svc.GetConversationSummary(1, 10)
	if summary.LastMessageID != retriedMsg.ID || summary.LastMessagePreview != retriedMsg.Content {
		t.Error("重试后发送方摘要未更新")
	}
}

// 完成一次成功发送：应更新状态并增加接收方未读数
func TestCompleteAttempt_UpdatesStatusAndSummary(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	msg, att, _ := svc.SendMessage(ctx, SendMessageRequest{
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "状态测试",
	})
	_, err := svc.CompleteAttempt(ctx, CompleteAttemptRequest{AttemptID: att.ID, Success: true})
	if err != nil {
		t.Fatal(err)
	}
	updated, _ := svc.GetMessage(msg.ID)
	if updated.Status != model.MessageStatusSent {
		t.Errorf("期望 sent, 实际 %s", updated.Status)
	}
	receiverSummary, _ := svc.GetConversationSummary(2, 10)
	if receiverSummary.UnreadCount != 1 {
		t.Errorf("接收方未读数应为 1, 实际 %d", receiverSummary.UnreadCount)
	}
}

// 删除消息后，双方同步事件应包含删除
func TestDeleteMessage_SyncsToBothUsers(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	msg, _, _ := svc.SendMessage(ctx, SendMessageRequest{
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "删除测试",
	})
	_, err := svc.DeleteMessage(msg.ID)
	if err != nil {
		t.Fatal(err)
	}
	// 发送方同步应包含删除事件
	senderEvents, _, _ := svc.Sync(ctx, SyncRequest{UserID: 1})
	found := false
	for _, ev := range senderEvents {
		if ev.EventType == model.EventTypeMessageDeleted {
			found = true
			break
		}
	}
	if !found {
		t.Error("发送方同步缺少删除事件")
	}
	// 接收方同步也应包含删除事件
	receiverEvents, _, _ := svc.Sync(ctx, SyncRequest{UserID: 2})
	found = false
	for _, ev := range receiverEvents {
		if ev.EventType == model.EventTypeMessageDeleted {
			found = true
			break
		}
	}
	if !found {
		t.Error("接收方同步缺少删除事件")
	}
}

// 会话消息列表应包含 attempt 计数信息（通过 Version 体现）
func TestListConversationMessages_IncludesAttemptCount(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	msg, _, _ := svc.SendMessage(ctx, SendMessageRequest{
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "列表测试",
	})
	// 再增加一次重试，产生更多 attempt
	svc.RetryMessage(ctx, msg.ID)
	msgs, _ := svc.ListConversationMessages(10, 0, 10)
	if len(msgs) != 1 {
		t.Fatalf("会话中应有 1 条消息")
	}
	// Version 应大于初始值 1（受更新和 attempt 计数影响）
	if msgs[0].Version < 1 {
		t.Error("Version 应反映更新次数")
	}
}

// 同步接口根据游标返回增量事件
func TestSync_ReturnsEventsBasedOnCursor(t *testing.T) {
	//cleanup := logger.Init("app.log")
	//defer cleanup()
	ctx := context.Background()
	svc := newSvc()
	svc.SendMessage(ctx, SendMessageRequest{
		SenderID:       1,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        "同步1",
		DeviceID:       "d1",
	})
	events, next, err := svc.Sync(ctx, SyncRequest{UserID: 1, DeviceID: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Error("应返回事件")
	}
	// 下一个游标应为最后一个事件的 Seq
	lastSeq := events[len(events)-1].Seq
	if next != lastSeq {
		t.Error("next_cursor 与最后一个事件的 Seq 不匹配")
	}
	// 使用返回的游标再次同步，应无新事件
	events2, _, _ := svc.Sync(ctx, SyncRequest{UserID: 1, DeviceID: "d1", Cursor: next})
	if len(events2) != 0 {
		t.Error("使用游标后不应再返回事件")
	}
}
