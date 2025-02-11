package network_creator

import (
	"context"
	"fmt"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/6529-Collections/6529node/internal/network"
	"github.com/6529-Collections/6529node/internal/network/libp2p"

	multiaddr "github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

var (
	transportConcurrency = make(chan struct{}, config.Get().MaxTransportConcurrency)
	libp2pTransportFunc  = libp2p.NewLibp2pTransport
)

func init() {
	for i := 0; i < cap(transportConcurrency); i++ {
		transportConcurrency <- struct{}{}
	}
}

// inside /internal/network/creator/network_creator.go

func NewNetworkTransport(bootstrapAddr string, parentCtx context.Context) (network.NetworkTransport, error) {
    if parentCtx.Err() != nil {
        return nil, fmt.Errorf("context canceled or expired before transport creation: %w", parentCtx.Err())
    }

    select {
    case <-parentCtx.Done():
        return nil, fmt.Errorf("context done before acquiring concurrency token: %w", parentCtx.Err())
    case <-transportConcurrency:
    }
    defer func() {
        transportConcurrency <- struct{}{}
        zap.L().Info("Released concurrency token", zap.String("bootstrapAddr", bootstrapAddr))
    }()

    var ma multiaddr.Multiaddr
    var err error
    if bootstrapAddr != "" {
        ma, err = multiaddr.NewMultiaddr(bootstrapAddr)
        if err != nil {
            return nil, fmt.Errorf("invalid multiaddr %q: %w", bootstrapAddr, err)
        }
        zap.L().Info("Creating libp2p transport", zap.String("bootstrapAddr", bootstrapAddr))
    } else {
        zap.L().Info("No bootstrap address provided, creating libp2p transport without connecting to a remote peer")
    }

    lp2pTransport, err := libp2pTransportFunc(ma, parentCtx)
    if err != nil {
        return nil, fmt.Errorf("failed to create libp2p transport: %w", err)
    }

    zap.L().Info("Successfully created libp2p transport", zap.String("bootstrapAddr", bootstrapAddr))
    return lp2pTransport, nil
}
