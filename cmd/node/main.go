package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/pkg/tdh"
	"go.uber.org/zap"
)

var Version = "dev" // Overridden by the release build script

func init() {
    zapConf := zap.Must(zap.NewProduction())
    if config.Get().LogZapMode == "development" {
        zapConf = zap.Must(zap.NewDevelopment())
    }
    zap.ReplaceGlobals(zapConf)
}

func main() {
    zap.L().Info("Starting 6529-Collections/6529node...", zap.String("Version", Version))

    // Open BadgerDB
    badgerDB, err := db.OpenBadger("./db")
    if err != nil {
        zap.L().Error("Failed to open BadgerDB", zap.Error(err))
        return
    }

    // Create context that we can cancel on shutdown
    ctx, cancel := context.WithCancel(context.Background())

    // Set up the contract listener
    contractListener, err := tdh.CreateTdhContractsListener(badgerDB)
    if err != nil {
        zap.L().Error("Failed to create Ethereum client", zap.Error(err))
        cancel()
        _ = badgerDB.Close()
        return
    }

    // Channel to catch OS signals
    stop := make(chan os.Signal, 1)
    signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

    // We'll run the listener in a separate goroutine,
    // so the main goroutine can wait for a signal.
    done := make(chan struct{})

    go func() {
        defer close(done)
        // If Listen() is blocking, it will exit when `ctx` is canceled (or on error).
        if err := contractListener.Listen(ctx); err != nil {
            zap.L().Error("Failed to watch contract events", zap.Error(err))
        }
    }()

    // Wait for OS signal
    <-stop
    zap.L().Info("Received shutdown signal, closing resources...")

    // 1) Cancel the context => tell Listen to exit
    cancel()

    // 2) Wait until the listener goroutine actually returns
    <-done

    // 3) Now we can safely close everything
    contractListener.Close()
    _ = badgerDB.Close()

    zap.L().Info("Shutdown complete")
}
