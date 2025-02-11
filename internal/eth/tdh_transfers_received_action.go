package eth

import (
	"context"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"go.uber.org/zap"
)

type TdhTransfersReceivedAction interface {
	Handle(transfers tokens.TokenTransferBatch) error
}

type DefaultTdhTransfersReceivedAction struct {
	ctx             context.Context
	progressTracker TdhIdxTrackerDb
}

func NewDefaultTdhTransfersReceivedAction(ctx context.Context, progressTracker TdhIdxTrackerDb) *DefaultTdhTransfersReceivedAction {
	return &DefaultTdhTransfersReceivedAction{
		ctx:             ctx,
		progressTracker: progressTracker,
	}
}

func (a *DefaultTdhTransfersReceivedAction) Handle(transfers tokens.TokenTransferBatch) error {
	// This does nothing meaningful, but it's a placeholder for future implementation
	zap.L().Info("Received transfers", zap.Uint64("block", transfers.BlockNumber))
	for _, transfer := range transfers.Transfers {
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

	err := a.progressTracker.SetProgress(transfers.BlockNumber)
	if err != nil {
		return fmt.Errorf("error setting progress: %w", err)
	}
	return nil
}
