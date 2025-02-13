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

var Version = "dev" // this is overridden by the release build script

func init() {
	zapConf := zap.Must(zap.NewProduction())
	if config.Get().LogZapMode == "development" {
		zapConf = zap.Must(zap.NewDevelopment())
	}
	zap.ReplaceGlobals(zapConf)
}

func main() {
	zap.L().Info("Starting 6529-Collections/6529node...", zap.String("Version", Version))

	badger, err := db.OpenBadger("./db")
	if err != nil {
		zap.L().Error("Failed to open BadgerDB", zap.Error(err))
		return
	}
	defer badger.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		zap.L().Info("Received termination signal, initiating shutdown", zap.String("signal", sig.String()))
		cancel()
	}()

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

	if err := tdh.BlockUntilOnTipAndKeepListeningAsync(badger, ctx); err != nil {
		zap.L().Error("Failed to listen on TDH contracts", zap.Error(err))
		cancel()
	}
	<-ctx.Done()

	zap.L().Info("Shutdown complete")
}
