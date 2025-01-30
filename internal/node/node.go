// internal/node/node.go

package node

import (
    "fmt"
    "os"
    "sync"

    "go.uber.org/zap"
)

// NodeConfig holds user-supplied settings from flags or env
type NodeConfig struct {
    P2PPort   int
    EnableTor bool
    DBPath    string
}

// Node represents the running instance
type Node struct {
    mu      sync.Mutex
    cfg     NodeConfig
    running bool
}

// NewNode constructs a new Node from a given NodeConfig.
func NewNode(cfg NodeConfig) *Node {
    return &Node{cfg: cfg}
}

// Start begins operation of the Node.
func (n *Node) Start() error {
    n.mu.Lock()
    defer n.mu.Unlock()

    if n.running {
        return fmt.Errorf("node is already running")
    }

    // Step 1: Check p2pPort, DB path
    if n.cfg.P2PPort <= 0 {
        return fmt.Errorf("invalid p2p port %d", n.cfg.P2PPort)
    }
    if err := os.MkdirAll(n.cfg.DBPath, 0o755); err != nil {
        return fmt.Errorf("cannot create dbPath %s: %v", n.cfg.DBPath, err)
    }

    // In future steps: open the DB, start p2p, etc.
    n.running = true
    zap.L().Info("Node started successfully",
        zap.Int("p2pPort", n.cfg.P2PPort),
        zap.Bool("enableTor", n.cfg.EnableTor),
        zap.String("dbPath", n.cfg.DBPath),
    )
    return nil
}

// Stop gracefully stops the Node.
func (n *Node) Stop() error {
    n.mu.Lock()
    defer n.mu.Unlock()

    if !n.running {
        return fmt.Errorf("node not running")
    }
    // future: shut down submodules
    n.running = false
    zap.L().Info("Node stopped.")
    return nil
}
