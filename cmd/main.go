package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/YUJIAJING0408/go-imessage/internal/api"
	"github.com/YUJIAJING0408/go-imessage/internal/logger"
	"github.com/YUJIAJING0408/go-imessage/internal/repository"
	"github.com/YUJIAJING0408/go-imessage/internal/service"
)

func main() {
	cleanup := logger.Init("app.log")
	defer cleanup()

	// 去重窗口命令行参数
	dedupeWindow := flag.Duration("dedupe-window", 30*time.Second, "消息去重时间窗口 (例如 30s, 1m)")
	flag.Parse()

	repo := repository.NewMemoryMessageRepository()
	svc := service.NewMessageService(repo, *dedupeWindow)
	server := api.NewServer(svc)
	mux := http.NewServeMux()
	server.Register(mux)
	log.Println("demo server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
