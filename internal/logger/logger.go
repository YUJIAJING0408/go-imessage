package logger

import (
	"io"
	"log/slog"
	"os"
)

// Init 初始化全局结构化日志，同时输出到文件和控制台。
func Init(logPath string) func() {
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("failed to open log file: " + err.Error())
	}
	multiWriter := io.MultiWriter(os.Stdout, file)
	handler := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return func() { file.Close() }
}

// Info 便捷方法
func Info(msg string, args ...any) {
	slog.Info(msg, args...)
}
