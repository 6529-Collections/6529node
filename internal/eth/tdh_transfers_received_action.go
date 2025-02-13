package eth

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type TdhTransfersReceivedAction interface {
	Handle(transfers []tokens.TokenTransfer) error
}

var ErrResetRequired = errors.New("reset required due to out-of-order checkpoint")

const actionsReceivedCheckpointKey = "tdh:actionsReceivedCheckpoint"

func checkpointValue(t tokens.TokenTransfer) string {
	return fmt.Sprintf("%d:%d:%d", t.BlockNumber, t.TransactionIndex, t.LogIndex)
}

func parseCheckpoint(value string) (uint64, uint64, uint64, error) {
	var block, txIndex, logIndex uint64
	_, err := fmt.Sscanf(value, "%d:%d:%d", &block, &txIndex, &logIndex)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid checkpoint format: %w", err)
	}
	return block, txIndex, logIndex, nil
}

type DefaultTdhTransfersReceivedAction struct {
	db         *badger.DB
	transferDb TransferDb
	ownerDb    OwnerDb
	nftDb      NFTDb
	ctx        context.Context
}

func NewTdhTransfersReceivedActionImpl(db *badger.DB, ctx context.Context) *DefaultTdhTransfersReceivedAction {

	transferDb := NewTransferDb()
	ownerDb := NewOwnerDb()
	nftDb := NewNFTDb()

	err := db.View(func(txn *badger.Txn) error {
		allTransfers, err := transferDb.GetAllTransfers(txn)
		if err != nil {
			return fmt.Errorf("failed to get all transfers: %w", err)
		}

		gradientTransfers, err := transferDb.GetTransfersByContract(txn, constants.GRADIENTS_CONTRACT)
		if err != nil {
			return fmt.Errorf("failed to get gradient transfers: %w", err)
		}

		memesTransfers, err := transferDb.GetTransfersByContract(txn, constants.MEMES_CONTRACT)
		if err != nil {
			return fmt.Errorf("failed to get memes transfers: %w", err)
		}

		nextgenTransfers, err := transferDb.GetTransfersByContract(txn, constants.NEXTGEN_CONTRACT)
		if err != nil {
			return fmt.Errorf("failed to get nextgen transfers: %w", err)
		}

		zap.L().Info("Transfers", zap.Int("total", len(allTransfers)), zap.Int("gradient", len(gradientTransfers)), zap.Int("memes", len(memesTransfers)), zap.Int("nextgen", len(nextgenTransfers)))

		sort.Slice(allTransfers, func(i, j int) bool {
			return allTransfers[i].BlockNumber < allTransfers[j].BlockNumber
		})
		if len(allTransfers) == 0 {
			zap.L().Info("No transfers found")
			return nil
		}
		latestTransfer := allTransfers[len(allTransfers)-1]
		zap.L().Info("Latest block", zap.Uint64("block", latestTransfer.BlockNumber), zap.Uint64("txIndex", latestTransfer.TransactionIndex), zap.Uint64("logIndex", latestTransfer.LogIndex))

		lastSavedCheckpointValue, err := getLastSavedCheckpoint(txn)
		if err != nil {
			return fmt.Errorf("failed to get last saved checkpoint: %w", err)
		}
		zap.L().Info("Last saved checkpoint", zap.String("checkpoint", string(lastSavedCheckpointValue)))

		return nil
	})

	if err != nil {
		zap.L().Error("Failed to view db", zap.Error(err))
		return nil
	}

	return &DefaultTdhTransfersReceivedAction{
		db:         db,
		transferDb: transferDb,
		ownerDb:    ownerDb,
		nftDb:      nftDb,
		ctx:        ctx,
	}
}

func (a *DefaultTdhTransfersReceivedAction) applyTransfer(
	txn *badger.Txn,
	transfer tokens.TokenTransfer,
	storeTransfer bool,
) error {

	tokens.Normalize(&transfer)

	// 1) Possibly store the transfer (usually only in live "Handle")
	if storeTransfer {
		if err := a.transferDb.StoreTransfer(txn, transfer); err != nil {
			zap.L().Error("Failed to store transfer", zap.Error(err))
			return fmt.Errorf("failed to store transfer: %w", err)
		}
	}

	// 2) Handle Minting / Burning for supply metrics
	if transfer.From == constants.NULL_ADDRESS {
		// Mint
		if err := a.nftDb.UpdateSupply(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
			zap.L().Error("Failed to update NFT supply", zap.Error(err))
			return fmt.Errorf("failed to update NFT supply: %w", err)
		}
	} else if transfer.To == constants.NULL_ADDRESS || transfer.To == constants.DEAD_ADDRESS {
		// Burn
		if err := a.nftDb.UpdateBurntSupply(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
			zap.L().Error("Failed to update NFT burnt supply", zap.Error(err))
			return fmt.Errorf("failed to update NFT burnt supply: %w", err)
		}
	}

	// 3) Check `from` balance if not minting
	if transfer.From != constants.NULL_ADDRESS {
		balance, err := a.ownerDb.GetBalance(txn, transfer.From, transfer.Contract, transfer.TokenID)
		if err != nil {
			zap.L().Error("Failed to get sender balance", zap.Error(err))
			return fmt.Errorf("failed to get sender balance: %w", err)
		}
		if balance < transfer.Amount {
			zap.L().Error("Sender does not have enough balance",
				zap.Int64("balance", balance),
				zap.Any("transfer", transfer),
			)
			return errors.New("insufficient balance for transfer")
		}
	}

	// 4) Perform the actual ownership update
	if err := a.ownerDb.UpdateOwnership(
		txn,
		transfer.From,
		transfer.To,
		transfer.Contract,
		transfer.TokenID,
		transfer.Amount,
	); err != nil {
		zap.L().Error("Failed to update ownership", zap.Error(err))
		return fmt.Errorf("failed to update ownership: %w", err)
	}

	return nil
}

func (a *DefaultTdhTransfersReceivedAction) reset(blockNumber uint64, txIndex uint64, logIndex uint64) error {
	zap.L().Info("Resetting transfers received DB", zap.Uint64("blockNumber", blockNumber), zap.Uint64("txIndex", txIndex), zap.Uint64("logIndex", logIndex))

	// Reset Owners
	ownerErr := a.ownerDb.ResetOwners(a.db)
	if ownerErr != nil {
		return fmt.Errorf("failed to reset owners: %w", ownerErr)
	}

	// Reset NFTs
	nftErr := a.nftDb.ResetNFTs(a.db)
	if nftErr != nil {
		return fmt.Errorf("failed to reset NFTs: %w", nftErr)
	}

	// Reset and fetch all transfers
	var remainingTransfers []tokens.TokenTransfer
	transferErr := a.db.Update(func(txn *badger.Txn) error {
		startingRransfers, err := a.transferDb.GetAllTransfers(txn)
		if err != nil {
			return fmt.Errorf("failed to get all transfers before reset: %w", err)
		}
		zap.L().Info("Starting transfers", zap.Int("count", len(startingRransfers)))

		resetTransfersErr := a.transferDb.ResetToCheckpoint(txn, blockNumber, txIndex, logIndex)
		if resetTransfersErr != nil {
			return fmt.Errorf("failed to reset transfers to checkpoint: %w", resetTransfersErr)
		}

		finalTransfers, err := a.transferDb.GetAllTransfers(txn)
		if err != nil {
			return fmt.Errorf("failed to get all transfers after reset: %w", err)
		}
		zap.L().Info("Final transfers", zap.Int("count", len(finalTransfers)))

		remainingTransfers = finalTransfers
		return nil
	})
	if transferErr != nil {
		return fmt.Errorf("failed to reset transfers: %w", transferErr)
	}

	zap.L().Info("Remaining transfers", zap.Int("count", len(remainingTransfers)))

	// Sort transfers
	sort.Slice(remainingTransfers, func(i, j int) bool {
		if remainingTransfers[i].BlockNumber != remainingTransfers[j].BlockNumber {
			return remainingTransfers[i].BlockNumber < remainingTransfers[j].BlockNumber
		}
		if remainingTransfers[i].TransactionIndex != remainingTransfers[j].TransactionIndex {
			return remainingTransfers[i].TransactionIndex < remainingTransfers[j].TransactionIndex
		}
		return remainingTransfers[i].LogIndex < remainingTransfers[j].LogIndex
	})

	// Replay Transfers in batches
	const batchSize = 100
	for i := 0; i < len(remainingTransfers); i += batchSize {
		end := i + batchSize
		if end > len(remainingTransfers) {
			end = len(remainingTransfers)
		}

		replayErr := a.db.Update(func(txn *badger.Txn) error {
			for _, t := range remainingTransfers[i:end] {
				if err := a.applyTransfer(txn, t, false); err != nil {
					return fmt.Errorf("failed to apply transfer: %w", err)
				}
			}
			return nil
		})
		if replayErr != nil {
			return fmt.Errorf("failed to apply transfers in batch: %w", replayErr)
		}
	}

	if len(remainingTransfers) > 0 {
		// Update checkpoint
		lastTransfer := remainingTransfers[len(remainingTransfers)-1]
		lastCheckpointValue := checkpointValue(lastTransfer)

		saveErr := a.db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(actionsReceivedCheckpointKey), []byte(lastCheckpointValue))
		})
		if saveErr != nil {
			return fmt.Errorf("failed to save checkpoint: %w", saveErr)
		}

		zap.L().Info("Checkpoint saved", zap.String(actionsReceivedCheckpointKey, lastCheckpointValue))
	} else {
		deleteErr := a.db.Update(func(txn *badger.Txn) error {
			return txn.Delete([]byte(actionsReceivedCheckpointKey))
		})
		if deleteErr != nil {
			return fmt.Errorf("failed to delete checkpoint: %w", deleteErr)
		}
	}

	zap.L().Info("Transfers received DB reset complete")
	return nil
}

func (a *DefaultTdhTransfersReceivedAction) Handle(transfers []tokens.TokenTransfer) error {
	zap.L().Info("Processing transfers received action", zap.Int("transfers", len(transfers)))

	const batchSize = 100
	numTransfers := len(transfers)
	if numTransfers == 0 {
		return nil
	}

	numBatches := (numTransfers + batchSize - 1) / batchSize // integer ceil

	for batchIndex := 0; batchIndex < numBatches; batchIndex++ {
		start := batchIndex * batchSize
		end := start + batchSize
		if end > numTransfers {
			end = numTransfers
		}
		chunk := transfers[start:end]

		firstTransfer := chunk[0]
		firstTransferValue := checkpointValue(firstTransfer)

		lastTransfer := chunk[len(chunk)-1]
		lastProcessedValue := checkpointValue(lastTransfer)

		zap.L().Info("Processing batch",
			zap.String("firstTransferValue", firstTransferValue),
			zap.String("lastProcessedValue", lastProcessedValue),
		)

		// Process each chunk in one transaction
		err := a.db.Update(func(txn *badger.Txn) error {
			lastSavedCheckpointValue, err := getLastSavedCheckpoint(txn)
			if err != nil {
				return fmt.Errorf("failed to get last saved checkpoint: %w", err)
			}

			lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex, err := parseCheckpoint(string(lastSavedCheckpointValue))
			if err != nil {
				return err // Parsing failed, likely corrupted data
			}

			firstBlock, firstTxIndex, firstLogIndex, err := parseCheckpoint(firstTransferValue)
			if err != nil {
				return err // Shouldn't happen, but good to handle it
			}

			// Proper numerical comparison to check if reset is needed
			if lastSavedBlock > firstBlock ||
				(lastSavedBlock == firstBlock && lastSavedTxIndex > firstTxIndex) ||
				(lastSavedBlock == firstBlock && lastSavedTxIndex == firstTxIndex && lastSavedLogIndex >= firstLogIndex) {
				zap.L().Warn("Checkpoint error: reset needed",
					zap.String("lastCheckpoint", string(lastSavedCheckpointValue)),
					zap.String("firstTransfer", firstTransferValue),
				)
				return ErrResetRequired
			}

			for _, t := range chunk {
				if err := a.applyTransfer(txn, t, true); err != nil {
					zap.L().Error("Failed to process transfer", zap.String("tx", t.TxHash), zap.Uint64("logIndex", t.LogIndex), zap.Error(err))
					return err
				}
			}

			if err := txn.Set([]byte(actionsReceivedCheckpointKey), []byte(lastProcessedValue)); err != nil {
				return fmt.Errorf("failed to save checkpoint: %w", err)
			}

			zap.L().Info("Checkpoint saved", zap.String(actionsReceivedCheckpointKey, lastProcessedValue))
			return nil
		})

		if err != nil {
			if errors.Is(err, ErrResetRequired) {
				zap.L().Warn("Resetting due to checkpoint mismatch")
				err = a.reset(firstTransfer.BlockNumber, firstTransfer.TransactionIndex, firstTransfer.LogIndex)
				if err != nil {
					zap.L().Error("Failed to reset", zap.Error(err))
					return err
				}
				return a.Handle(transfers) // Restart from scratch
			}
			return err
		}

		zap.L().Info("Batch processed",
			zap.String("batch", fmt.Sprintf("%d/%d", batchIndex+1, numBatches)),
			zap.Int("batchSize", len(chunk)),
			zap.String(actionsReceivedCheckpointKey, lastProcessedValue),
		)
	}

	return nil
}

func getLastSavedCheckpoint(txn *badger.Txn) ([]byte, error) {
	var lastSavedCheckpointValue []byte
	lastSavedCheckpoint, err := txn.Get([]byte(actionsReceivedCheckpointKey))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			zap.L().Info("No previous checkpoint found")
			lastSavedCheckpointValue = []byte("0:0:0")
		} else {
			return nil, fmt.Errorf("failed to retrieve last checkpoint: %w", err)
		}
	} else {
		lastSavedCheckpointValue, err = lastSavedCheckpoint.ValueCopy(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get last checkpoint value: %w", err)
		}
	}

	return lastSavedCheckpointValue, nil
}
