package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MartinSimango/simple-db/pkg/db"
)

func main() {

	simpleDb, err := db.NewSimpleDb(":5050")
	if err != nil {
		slog.Error("Failed to start simple-db", "error", err)
		os.Exit(1)
	}
	slog.Info("Starting simple-db on :5050")
	errChan := make(chan error)
	go func() {
		errChan <- simpleDb.Start()
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-quit:
		slog.Info("Shutting down simple-db")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		simpleDb.Shutdown(ctx)
	case err := <-errChan:
		if err != nil {
			slog.Error("Failed to start simple-db", "error", err)
			os.Exit(1)
		}
	}

}
