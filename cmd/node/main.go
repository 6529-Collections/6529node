package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/6529-Collections/6529node/internal/node"
)

func main() {
	// CLI flags
	var cfg node.NodeConfig
	flag.IntVar(&cfg.P2PPort, "p2pPort", 4001, "P2P port")
	flag.BoolVar(&cfg.EnableTor, "enableTor", false, "Enable Tor integration")
	flag.StringVar(&cfg.DBPath, "dbPath", "./data", "Where to store DB files")
	flag.Parse()

	// Set up logger (production by default)
	logger, _ := zap.NewProduction()
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	zap.L().Info("6529 Node (Step 1) is initializing...",
		zap.Int("p2pPort", cfg.P2PPort),
		zap.Bool("enableTor", cfg.EnableTor),
		zap.String("dbPath", cfg.DBPath),
	)

	n := node.NewNode(cfg)
	if err := n.Start(); err != nil {
		zap.L().Fatal("Failed to start node", zap.Error(err))
	}

	// Set up channel to catch signals (SIGINT, SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for interrupt signal
	sig := <-sigChan
	zap.L().Info("Received shutdown signal", zap.String("signal", sig.String()))

	// Gracefully shutdown the node
	if err := n.Stop(); err != nil {
		zap.L().Error("Error stopping node", zap.Error(err))
	}
}
