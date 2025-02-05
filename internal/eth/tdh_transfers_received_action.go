package eth

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
		transferDb: NewTransferDb(),
		ownerDb:    NewOwnerDb(),
		nftDb:      NewNFTDb(),
	}
}

func (a *DefaultTdhTransfersReceivedAction) Handle(ctx context.Context, transfers []tokens.TokenTransfer) error {
	zap.L().Info("Processing transfers action", zap.Int("transfers", len(transfers)))
	return a.db.Update(func(txn *badger.Txn) error {
		for _, transfer := range transfers {
			// Step 0: Adjust transfer struct
			transfer.TxHash = strings.ToLower(transfer.TxHash)
			transfer.Contract = strings.ToLower(transfer.Contract)
			transfer.From = strings.ToLower(transfer.From)
			transfer.To = strings.ToLower(transfer.To)

			// Step 1: Store the transfer in TransferDb
			if err := a.transferDb.StoreTransfer(txn, transfer); err != nil {
				zap.L().Error("Failed to store transfer", zap.Error(err))
				return fmt.Errorf("failed to store transfer: %w", err)
			}

			// Step 2: Handle Minting (from null address) and Burn (to null or dead address)
			if transfer.From == constants.NULL_ADDRESS {
				// Increase NFT supply
				if err := a.nftDb.UpdateSupply(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
					zap.L().Error("Failed to update NFT supply", zap.Error(err))
					return fmt.Errorf("failed to update NFT supply: %w", err)
				}
			} else if transfer.To == constants.NULL_ADDRESS || transfer.To == constants.DEAD_ADDRESS {
				// Increase NFT burnt supply
				if err := a.nftDb.UpdateBurntSupply(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
					zap.L().Error("Failed to update NFT burnt supply", zap.Error(err))
					return fmt.Errorf("failed to update NFT burnt supply: %w", err)
				}
			}

			// Step 3: Ensure sender owns enough tokens
			if transfer.From != constants.NULL_ADDRESS {
				balance, err := a.ownerDb.GetBalance(txn, transfer.From, transfer.Contract, transfer.TokenID)
				if err != nil {
					zap.L().Error("Failed to get sender balance", zap.Error(err))
					return fmt.Errorf("failed to get sender balance: %w", err)
				}
				if balance < transfer.Amount {
					zap.L().Error("Sender does not have enough balance", zap.String("from", transfer.From))
					return errors.New("insufficient balance for transfer")
				}
			}

			// Step 4: Transfer Ownership
			if err := a.ownerDb.UpdateOwnership(txn, transfer.From, transfer.To, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
				zap.L().Error("Failed to update ownership", zap.Error(err))
				return fmt.Errorf("failed to update ownership: %w", err)
			}

			zap.L().Info("Transfer processed", zap.String("tx", transfer.TxHash), zap.Uint64("logIndex", transfer.LogIndex))
		}
		return nil
	})
}
