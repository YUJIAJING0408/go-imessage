package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/YUJIAJING0408/go-imessage/internal/service"
	"github.com/google/uuid"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// Server 是 HTTP API 的适配器，持有 MessageService 实例。
type Server struct {
	svc *service.MessageService
}

// NewServer 创建一个新的 Server 实例。
func NewServer(svc *service.MessageService) *Server {
	return &Server{svc: svc}
}

// Register 向 http.ServeMux 注册路由。
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("/messages/send", s.handleSendMessage)
	mux.HandleFunc("/sync", s.handleSync)
}

// injectRequestID 从 HTTP 请求中提取 X-Request-ID 头，若不存在则生成一个新的 UUID。
// 将 request_id 注入 context 并一并返回。
func injectRequestID(r *http.Request) (context.Context, string) {
	reqID := r.Header.Get("X-Request-ID")
	if reqID == "" {
		reqID = uuid.NewString()
	}
	ctx := context.WithValue(r.Context(), requestIDKey, reqID)
	return ctx, reqID
}

// handleSendMessage 处理 POST /messages/send 请求。
// 解析 JSON body 为 SendMessageRequest，调用服务层发送消息。
// 成功时返回消息和发送尝试信息。
func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, reqID := injectRequestID(r)
	var req service.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req.RequestID = reqID
	msg, attempt, err := s.svc.SendMessage(ctx, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"message": msg, "attempt": attempt})
}

// handleSync 处理 GET /sync?user_id=xxx&cursor=xxx&device_id=xxx 请求。
// 返回增量同步事件和下一个游标。
func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	userID, _ := strconv.ParseInt(r.URL.Query().Get("user_id"), 10, 64)
	cursor, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
	deviceID := r.URL.Query().Get("device_id")

	ctx, _ := injectRequestID(r)

	events, next, err := s.svc.Sync(ctx, service.SyncRequest{UserID: userID, DeviceID: deviceID, Cursor: cursor})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"events": events, "next_cursor": next})
}
