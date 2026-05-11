package main

import (
	"log"
	"net/http"

	"github.com/YUJIAJING0408/go-imessage/internal/api"
	"github.com/YUJIAJING0408/go-imessage/internal/logger"
	"github.com/YUJIAJING0408/go-imessage/internal/repository"
	"github.com/YUJIAJING0408/go-imessage/internal/service"
)

func main() {
	cleanup := logger.Init("app.log")
	defer cleanup()

	repo := repository.NewMemoryMessageRepository()
	svc := service.NewMessageService(repo)
	server := api.NewServer(svc)
	mux := http.NewServeMux()
	server.Register(mux)
	log.Println("demo server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
