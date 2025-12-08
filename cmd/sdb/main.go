package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/MartinSimango/simple-db/pkg/db"
)

func main() {

	simpleDb, err := db.NewSimpleDb(":5050")
	if err != nil {
		slog.Error("Failed to start simple-db", "error", err)
		os.Exit(1)
	}
	slog.Info("Starting simple-db on :5050")
	go func() {
		if err := simpleDb.Start(); err != nil {
			slog.Error("Failed to start simple-db", "error", err)
			os.Exit(1)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	slog.Info("Shutting down simple-db")
	simpleDb.Stop(context.Background())

}
