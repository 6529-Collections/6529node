package main

import (
	"context"
	"time"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/internal/db"
	networkCreator"github.com/6529-Collections/6529node/internal/network/creator"
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
	defer cancel()

	contractListener, err := tdh.CreateTdhContractsListener(badger)
	if err != nil {
		zap.L().Error("Failed to create Ethereum client", zap.Error(err))
		return
	}
	defer contractListener.Close()

    bootstrapAddr := config.Get().P2PBootstrapAddr
	network, err := networkCreator.GetNetworkTransport(bootstrapAddr, ctx)
	if err != nil {
		zap.L().Error("Failed to initialise network transport", zap.Error(err))
		return
	}
	
	topic := "hello-world"
	subErr := network.Subscribe(topic, func(msg []byte) {
		zap.L().Info("Received hello-world message!", zap.ByteString("msg", msg))
	})
	if subErr != nil {
		zap.L().Warn("Could not subscribe to hello-world topic", zap.Error(err))
	} else {
		// Wait a moment so the mesh forms, then publish a greeting
		time.AfterFunc(2*time.Second, func() {
			if pubErr := network.Publish(topic, []byte("Test!")); pubErr != nil {
				zap.L().Error("Failed to publish hello-world message", zap.Error(pubErr))
			} else {
				zap.L().Info("Published hello-world message (after 2s delay)")
			}
		})
	}

	go func() {
		if err := contractListener.Listen(ctx); err != nil {
			zap.L().Error("Contract listener encountered error", zap.Error(err))
		}
	}()

	select {}
}
