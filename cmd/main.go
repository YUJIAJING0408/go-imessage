package main

import (
	"log"
	"net/http"

	"go-imessage/internal/api"
	"go-imessage/internal/repository"
	"go-imessage/internal/service"
)

func main() {
	repo := repository.NewMemoryMessageRepository()
	svc := service.NewMessageService(repo)
	server := api.NewServer(svc)
	mux := http.NewServeMux()
	server.Register(mux)
	log.Println("demo server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
