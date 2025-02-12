package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/internal/db"
	network_creator "github.com/6529-Collections/6529node/internal/network/creator"
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
	defer badgerDB.Close() // Ensure it closes on exit

	// Create context that we can cancel on shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cleanup on exit

	// Set up OS signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Start P2P network transport
	bootstrapAddr := config.Get().P2PBootstrapAddr
	networkTransport, err := network_creator.NewNetworkTransport(bootstrapAddr, ctx)
	if err != nil {
		zap.L().Fatal("Failed to create network transport", zap.Error(err))
	}
	defer func() {
		if err := networkTransport.Close(); err != nil {
			zap.L().Warn("Error stopping transport", zap.Error(err))
		}
	}()

	// Start TDH contract listener async
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := tdh.BlockUntilOnTipAndKeepListeningAsync(badgerDB, ctx); err != nil {
			zap.L().Error("Failed to listen on TDH contracts", zap.Error(err))
		}
	}()

	// Wait for OS signal
	sig := <-sigCh
	zap.L().Info("Received shutdown signal, closing resources...", zap.String("signal", sig.String()))

	// 1) Cancel the context => tell all services to exit
	cancel()

	// 2) Wait for the listener goroutine to exit
	<-done

	zap.L().Info("Shutdown complete")
}
