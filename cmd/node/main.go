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

var Version = "dev" // This is overridden by the release build script

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
	badger, err := db.OpenBadger("./db")
	if err != nil {
		zap.L().Error("Failed to open BadgerDB", zap.Error(err))
		return
	}
	defer func() {
		zap.L().Info("Closing BadgerDB...")
		badger.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	contractListener, err := tdh.CreateTdhContractsListener(badger)
	if err != nil {
		zap.L().Error("Failed to create Ethereum client", zap.Error(err))
		return
	}
	defer contractListener.Close()

	// Handle graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		zap.L().Info("Received shutdown signal, closing resources...")
		cancel()           // Cancel context
		badger.Close()     // Close BadgerDB
		contractListener.Close()
		os.Exit(0)
	}()

	// Start contract listener
	if err := contractListener.Listen(ctx); err != nil {
		zap.L().Error("Failed to watch contract events", zap.Error(err))
	}
}