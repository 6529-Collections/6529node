package eth

import (
	"context"
	"errors"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type TdhTransfersReceivedAction interface {
	Handle(ctx context.Context, transfers []tokens.TokenTransfer) error
}

type DefaultTdhTransfersReceivedAction struct {
	db         *badger.DB
	transferDb TransferDb
	ownerDb    OwnerDb
	nftDb      NFTDb
}

func NewTdhTransfersReceivedActionImpl(db *badger.DB) *DefaultTdhTransfersReceivedAction {
	return &DefaultTdhTransfersReceivedAction{
		db:         db,
		transferDb: NewTransferDb(db),
		ownerDb:    NewOwnerDb(db),
		nftDb:      NewNFTDb(db),
	}
}

func (a *DefaultTdhTransfersReceivedAction) Handle(ctx context.Context, transfers []tokens.TokenTransfer) error {
	return a.db.Update(func(txn *badger.Txn) error {
		for _, transfer := range transfers {
			// Log the transfer attempt
			zap.L().Info("Processing transfer",
				zap.Uint64("block", transfer.BlockNumber),
				zap.String("tx", transfer.TxHash),
				zap.String("from", transfer.From),
				zap.String("to", transfer.To),
				zap.String("tokenId", transfer.TokenID),
				zap.Int64("amount", transfer.Amount),
				zap.String("contract", transfer.Contract),
			)

			// Step 1: Store the transfer in TransferDb
			if err := a.transferDb.StoreTransfer(transfer); err != nil {
				zap.L().Error("Failed to store transfer", zap.Error(err))
				return fmt.Errorf("failed to store transfer: %w", err)
			}

			// Step 2: Handle Minting (from null address)
			if transfer.From == constants.NULL_ADDRESS {
				// Increase NFT supply
				if err := a.nftDb.UpdateSupply(transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
					zap.L().Error("Failed to update NFT supply", zap.Error(err))
					return fmt.Errorf("failed to update NFT supply: %w", err)
				}
			} else {
				// Step 3: Ensure sender owns enough tokens
				balance, err := a.ownerDb.GetBalance(transfer.From, transfer.Contract, transfer.TokenID)
				if err != nil {
					zap.L().Error("Failed to get sender balance", zap.Error(err))
					return fmt.Errorf("failed to get sender balance: %w", err)
				}
				if balance < transfer.Amount {
					zap.L().Error("Sender does not have enough balance", zap.String("from", transfer.From))
					return errors.New("insufficient balance for transfer")
				}

				// Step 4: Transfer Ownership
				if err := a.ownerDb.UpdateOwnership(transfer.From, transfer.To, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
					zap.L().Error("Failed to update ownership", zap.Error(err))
					return fmt.Errorf("failed to update ownership: %w", err)
				}
			}
		}
		return nil
	})
}