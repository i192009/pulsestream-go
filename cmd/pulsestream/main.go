package main

import (
	"github.com/pulsestream/pulsestream-go/internal/app"
	"log/slog"
	"os"
)

func main() {
	role := "ingestion-gateway"
	if len(os.Args) > 1 {
		role = os.Args[1]
	}
	if err := app.Run(role); err != nil {
		slog.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
