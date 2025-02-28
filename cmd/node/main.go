package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/internal/db"
	network_creator "github.com/6529-Collections/6529node/internal/network/creator"
	"github.com/6529-Collections/6529node/internal/rpc"
	"github.com/6529-Collections/6529node/pkg/tdh"
	"go.uber.org/zap"
)

var Version = "dev" // Overridden by release build script

func init() {
	logger := zap.Must(zap.NewProduction())
	if config.Get().LogZapMode == "development" {
		logger = zap.Must(zap.NewDevelopment())
	}
	zap.ReplaceGlobals(logger)
}

func main() {
	zap.L().Info("Starting 6529-Collections/6529node...",
		zap.String("Version", Version))

	// Main context: canceled when we want to stop normal operation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Open DB
	sqlite, err := db.OpenSqlite("./db/sqlite/sqlite")
	if err != nil {
		zap.L().Fatal("Failed to open SQLite", zap.Error(err))
	}

	// Start RPC server (currently doesn't accept context for shutdown)
	closeRpcServer := rpc.StartRPCServer(config.Get().RPCPort, sqlite, ctx)

	// Create the P2P network transport
	bootstrapAddr := config.Get().P2PBootstrapAddr
	networkTransport, err := network_creator.NewNetworkTransport(bootstrapAddr, ctx)
	if err != nil {
		zap.L().Fatal("Failed to create network transport", zap.Error(err))
	}

	// Start TDH listening
	if err := tdh.BlockUntilOnTipAndKeepListeningAsync(sqlite, ctx); err != nil {
		zap.L().Error("Failed to listen on TDH contracts", zap.Error(err))
		cancel() // Cancel main context if critical startup failed
	}

	// Catch up to two signals: first for graceful, second to force
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	doneCh := make(chan struct{})

	go func() {
		<-sigCh
		zap.L().Info("Received shutdown signal, initiating graceful shutdown...")

		// 1. Stop new requests on RPC
		closeRpcServer() // If you update StartRPCServer to accept a context, pass shutdownCtx

		// 2. Cancel main context, telling background tasks to stop
		cancel()

		// 3. Close the network transport
		if err := networkTransport.Close(); err != nil {
			zap.L().Warn("Error closing network transport", zap.Error(err))
		}

		// 4. Close DB
		if err := sqlite.Close(); err != nil {
			zap.L().Warn("Error closing DB", zap.Error(err))
		}

		// 5. Signal that cleanup is done
		close(doneCh)

		// If a second signal arrives, force an immediate exit
		<-sigCh
		zap.L().Error("Received second signal, forcing shutdown")
		os.Exit(1)
	}()

	// Wait for either normal context cancellation or graceful shutdown completion
	select {
	case <-ctx.Done():
	case <-doneCh:
	}

	zap.L().Info("Shutdown complete")
	_ = zap.L().Sync()
}
