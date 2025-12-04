package main

import (
	"log/slog"
	"os"

	"github.com/MartinSimango/simple-db/pkg/db"
)

func main() {

	simpleDb, err := db.NewSimpleDb(":5050")
	if err != nil {
		slog.Error("Failed to start simple-db", "error", err)
		os.Exit(1)
	}
	slog.Info("Starting simple-db on :5050")
	if err := simpleDb.Start(); err != nil {
		slog.Error("Failed to start simple-db", "error", err)
		os.Exit(1)
	}

}
