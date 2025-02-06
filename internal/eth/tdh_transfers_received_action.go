package eth

import (
	"context"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"go.uber.org/zap"
)

type TdhTransfersReceivedAction interface {
	Handle(ctx context.Context, transfers []tokens.TokenTransfer) error
}

type DefaultTdhTransfersReceivedAction struct {
}

func (a *DefaultTdhTransfersReceivedAction) Handle(ctx context.Context, transfers []tokens.TokenTransfer) error {
	// This does nothing meaningful, but it's a placeholder for future implementation
	for _, transfer := range transfers {
		zap.L().Info(
			"Token transfer",
			zap.Uint64("block", transfer.BlockNumber),
			zap.String("tx", transfer.TxHash),
			zap.String("from", transfer.From),
			zap.String("to", transfer.To),
			zap.String("tokenId", transfer.TokenID),
			zap.Int64("amount", transfer.Amount),
			zap.String("contract", transfer.Contract),
			zap.String("type", transfer.Type.String()),
		)
	}
	return nil
}
