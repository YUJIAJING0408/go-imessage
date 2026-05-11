package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/YUJIAJING0408/go-imessage/internal/service"
)

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

// handleSendMessage 处理 POST /messages/send 请求。
// 解析 JSON body 为 SendMessageRequest，调用服务层发送消息。
// 成功时返回消息和发送尝试信息。
func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req service.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	msg, attempt, err := s.svc.SendMessage(req)
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
	events, next, err := s.svc.Sync(service.SyncRequest{UserID: userID, DeviceID: deviceID, Cursor: cursor})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"events": events, "next_cursor": next})
}
