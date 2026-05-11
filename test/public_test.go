package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/model"
	"github.com/YUJIAJING0408/go-imessage/internal/repository"
	"github.com/YUJIAJING0408/go-imessage/internal/service"
)

func newService() *service.MessageService {
	repo := repository.NewMemoryMessageRepository()
	return service.NewMessageService(repo, time.Second*30)
}

func send(t *testing.T, svc *service.MessageService, clientID string) (model.Message, model.DeliveryAttempt) {
	var ctx = context.Background()
	t.Helper()
	msg, att, err := svc.SendMessage(ctx, service.SendMessageRequest{
		RequestID:      "req-" + clientID,
		SenderID:       1,
		ReceiverID:     2,
		DeviceID:       "device-a",
		ConversationID: 100,
		ClientMsgID:    clientID,
		Content:        "hello",
	})
	if err != nil {
		t.Fatalf("send message failed: %v", err)
	}
	return msg, att
}

func TestSendMessageCreatesAttempt(t *testing.T) {

	svc := newService()
	msg, attempt := send(t, svc, "local-001")
	if msg.ID <= 0 || attempt.ID <= 0 {
		t.Fatalf("expected ids, msg=%d attempt=%d", msg.ID, attempt.ID)
	}
	if msg.Status != model.MessageStatusSending {
		t.Fatalf("expected sending status, got %s", msg.Status)
	}
}

func TestCompleteAttemptSuccess(t *testing.T) {
	var ctx = context.Background()
	svc := newService()
	_, attempt := send(t, svc, "local-002")
	updated, err := svc.CompleteAttempt(ctx, service.CompleteAttemptRequest{RequestID: "callback-1", AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}
	if updated.Status != model.MessageStatusSent {
		t.Fatalf("expected sent status, got %s", updated.Status)
	}
}

func TestRetryMessageCreatesNewAttempt(t *testing.T) {
	var ctx = context.Background()
	svc := newService()
	msg, attempt := send(t, svc, "local-retry")
	_, err := svc.CompleteAttempt(ctx, service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: false, ErrorCode: "network_error"})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}
	retried, retryAttempt, err := svc.RetryMessage(ctx, msg.ID)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if retryAttempt.ID <= attempt.ID {
		t.Fatalf("expected new attempt")
	}
	if retried.Status != model.MessageStatusSending {
		t.Fatalf("expected sending after retry, got %s", retried.Status)
	}
}

func TestListConversationMessagesAndSummary(t *testing.T) {
	var ctx = context.Background()
	svc := newService()
	for i := 0; i < 3; i++ {
		msg, attempt := send(t, svc, fmt.Sprintf("local-list-%d", i))
		_, err := svc.CompleteAttempt(ctx, service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
		if err != nil {
			t.Fatalf("complete attempt failed for msg %d: %v", msg.ID, err)
		}
	}
	items, err := svc.ListConversationMessages(100, 0, 20)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(items))
	}
	summary, err := svc.GetConversationSummary(2, 100)
	if err != nil {
		t.Fatalf("summary failed: %v", err)
	}
	if summary.UnreadCount == 0 {
		t.Fatalf("expected receiver unread count")
	}
}

func TestSyncReturnsEvents(t *testing.T) {
	var ctx = context.Background()
	svc := newService()
	_, attempt := send(t, svc, "local-sync")
	_, err := svc.CompleteAttempt(ctx, service.CompleteAttemptRequest{AttemptID: attempt.ID, Success: true})
	if err != nil {
		t.Fatalf("complete attempt failed: %v", err)
	}
	events, cursor, err := svc.Sync(ctx, service.SyncRequest{UserID: 2, DeviceID: "device-b", Cursor: 0})
	if err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if len(events) == 0 || cursor == 0 {
		t.Fatalf("expected sync events and cursor")
	}
}
