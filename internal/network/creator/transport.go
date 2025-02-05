package creator

import (
	"context"

	"github.com/6529-Collections/6529node/internal/network"
	"github.com/6529-Collections/6529node/internal/network/libp2p"
	"github.com/6529-Collections/6529node/internal/network/noop"
)

func GetNetworkTransport(bootstrapAddr string, ctx context.Context) (network.NetworkTransport, error) {
	if bootstrapAddr == "" {
		noop := noop.NewNoopTransport()
		return noop, nil
	}
	lp2p, err := libp2p.NewLibp2pTransport(bootstrapAddr, ctx)
	return lp2p, err
}
