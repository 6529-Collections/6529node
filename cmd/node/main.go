package main

import (
	"context"
	"time"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/internal/p2p"
	"github.com/6529-Collections/6529node/internal/p2p/libp2p"
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

	var network p2p.NetworkTransport
	if config.Get().EnableP2P {
		zap.L().Info("P2P is ENABLED. Initializing libp2p transport.")
		network = libp2p.NewLibp2pTransport()
	} else {
		zap.L().Info("P2P is DISABLED. Using NoopTransport.")
		network = &p2p.NoopTransport{}
	}

	if err := network.Start(); err != nil {
		zap.L().Error("Failed to start P2P transport", zap.Error(err))
		return
	}
	defer func() {
		if err := network.Stop(); err != nil {
			zap.L().Warn("Error stopping P2P transport", zap.Error(err))
		}
	}()

	if config.Get().EnableP2P {
		bootstrapAddr := config.Get().P2PBootstrapAddr
		// If a P2P_BOOTSTRAP_ADDR env var is provided, attempt to connect to that peer
		if bootstrapAddr != "" {
			// We only know how to connect if it's a Libp2pTransport
			if libp2pNet, ok := network.(*libp2p.Libp2pTransport); ok {
				if err := libp2pNet.Connect(bootstrapAddr); err != nil {
					zap.L().Error("Failed to connect to BOOTSTRAP peer", zap.Error(err), zap.String("addr", bootstrapAddr))
				} else {
					zap.L().Info("Connected to BOOTSTRAP peer", zap.String("addr", bootstrapAddr))
				}
			}
		}
	}

	if config.Get().EnableP2P {
		topic := "hello-world"
		err := network.Subscribe(topic, func(msg []byte) {
			zap.L().Info("Received hello-world message!", zap.ByteString("msg", msg))
		})
		if err != nil {
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
	}

	go func() {
		if err := contractListener.Listen(ctx); err != nil {
			zap.L().Error("Contract listener encountered error", zap.Error(err))
		}
	}()

	select {}
}
