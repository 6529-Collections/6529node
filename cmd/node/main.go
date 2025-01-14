package main

import (
	"context"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/internal/db"
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
	ctx, cancel := context.WithCancel(context.Background())
	contractListener, err := tdh.CreateTdhContractsListener(badger)
	if err != nil {
		zap.L().Error("Failed to create Ethereum client", zap.Error(err))
		return
	}
	defer cancel()
	defer contractListener.Close()

	if contractListener.Listen(ctx) != nil {
		zap.L().Error("Failed to watch contract events", zap.Error(err))
	}
}
