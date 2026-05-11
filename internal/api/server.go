package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/YUJIAJING0408/go-imessage/internal/service"
)

type Server struct {
	svc *service.MessageService
}

func NewServer(svc *service.MessageService) *Server {
	return &Server{svc: svc}
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("/messages/send", s.handleSendMessage)
	mux.HandleFunc("/sync", s.handleSync)
}

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
