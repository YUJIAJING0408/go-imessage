package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/repository"
	"github.com/YUJIAJING0408/go-imessage/internal/service"
)

// 辅助函数：创建测试 Server 和请求包装
func newTestServer() *Server {
	repo := repository.NewMemoryMessageRepository()
	svc := service.NewMessageService(repo)
	return NewServer(svc)
}

func sendRequest(s *Server, senderID int64, content string) *httptest.ResponseRecorder {
	body := service.SendMessageRequest{
		SenderID:       senderID,
		ReceiverID:     2,
		ConversationID: 10,
		Content:        content,
	}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/messages/send", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handleSendMessage(w, req)
	return w
}

// TestRateLimiter_AllowsUpToBurst 验证在突发容量内请求全部通过。
func TestRateLimiter_AllowsUpToBurst(t *testing.T) {
	s := newTestServer()
	senderID := int64(1001)

	for i := 0; i < burstSize; i++ {
		w := sendRequest(s, senderID, "hello")
		if w.Code != http.StatusOK {
			t.Fatalf("burst 请求 #%d 应成功，但得到状态码 %d", i+1, w.Code)
		}
	}
}

// TestRateLimiter_BlocksAfterBurst 验证超出突发容量后立即被拒绝。
func TestRateLimiter_BlocksAfterBurst(t *testing.T) {
	s := newTestServer()
	senderID := int64(1002)

	// 消耗所有令牌（包括突发）
	for i := 0; i < burstSize; i++ {
		w := sendRequest(s, senderID, "test")
		if w.Code != http.StatusOK {
			t.Fatalf("第 %d 个请求应成功，状态码 %d", i+1, w.Code)
		}
	}

	// 下一个请求应立即被拒绝
	w := sendRequest(s, senderID, "overflow")
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("超出突发后的请求应返回 429，实际 %d", w.Code)
	}
}

// TestRateLimiter_RefillsToken 验证耗尽令牌后立即被限流，等待后恢复成功。
func TestRateLimiter_RefillsToken(t *testing.T) {
	s := newTestServer()
	senderID := int64(1003)

	// 耗尽所有令牌
	for i := 0; i < burstSize; i++ {
		sendRequest(s, senderID, "use")
	}
	// 立即再发一次，应被限流
	w := sendRequest(s, senderID, "immediate after burst")
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("耗尽令牌后立即请求应返回 429，实际 %d", w.Code)
	}
	// 等待足够时间恢复一个令牌（速率 5/s，恢复一个令牌约需 200ms）
	time.Sleep(300 * time.Millisecond)

	w = sendRequest(s, senderID, "after refill")
	if w.Code != http.StatusOK {
		t.Errorf("等待令牌恢复后应成功，状态码 %d", w.Code)
	}
}

// TestRateLimiter_DifferentSendersIndependent 验证不同 senderID 的限流器互相独立。
func TestRateLimiter_DifferentSendersIndependent(t *testing.T) {
	s := newTestServer()
	senderA := int64(2001)
	senderB := int64(2002)

	// 耗尽 A
	for i := 0; i < burstSize; i++ {
		sendRequest(s, senderA, "a")
	}
	// A 应被限流
	w := sendRequest(s, senderA, "extra")
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("A 应被限流，状态码 %d", w.Code)
	}

	// B 应仍能正常发送
	w = sendRequest(s, senderB, "b")
	if w.Code != http.StatusOK {
		t.Errorf("B 应正常发送，状态码 %d", w.Code)
	}
}
