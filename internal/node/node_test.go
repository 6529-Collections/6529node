// internal/node/node_test.go

package node_test

import (
	"os"
	"path/filepath"
	"testing"

	// Import the node package under test
	"github.com/6529-Collections/6529node/internal/node"

	"go.uber.org/zap"
)

func TestNodeStartStop(t *testing.T) {
	// Use a temporary directory for DBPath
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	// Create a simple test logger
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	// Create a test NodeConfig
	cfg := node.NodeConfig{
		P2PPort:   4001,
		EnableTor: false,
		DBPath:    dbPath,
	}

	// Instantiate a node
	n := node.NewNode(cfg)

	// Start the node
	if err := n.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	// Check that the data directory was created
	if _, err := os.Stat(dbPath); err != nil {
		t.Errorf("expected DB path to be created, but got error: %v", err)
	}

	// Stop the node
	if err := n.Stop(); err != nil {
		t.Fatalf("failed to stop node: %v", err)
	}
}

func TestNodeStartTwice(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	cfg := node.NodeConfig{
		P2PPort:   4001,
		EnableTor: false,
		DBPath:    t.TempDir(),
	}
	n := node.NewNode(cfg)

	// Start first time - should succeed
	if err := n.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	// Start second time - should fail
	if err := n.Start(); err == nil {
		t.Errorf("expected error when starting already running node, but got nil")
	}
}
