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
	Handle(transfers tokens.TokenTransferBatch) error
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
	ctx             context.Context
	progressTracker TdhIdxTrackerDb
	db              *badger.DB
	transferDb      TransferDb
	ownerDb         OwnerDb
	nftDb           NFTDb
}

func NewTdhTransfersReceivedActionImpl(ctx context.Context, progressTracker TdhIdxTrackerDb, db *badger.DB) *DefaultTdhTransfersReceivedAction {

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

		zap.L().Info("Transfers",
			zap.Int("total", len(allTransfers)),
			zap.Int("gradient", len(gradientTransfers)),
			zap.Int("memes", len(memesTransfers)),
			zap.Int("nextgen", len(nextgenTransfers)),
		)

		sort.Slice(allTransfers, func(i, j int) bool {
			return allTransfers[i].BlockNumber < allTransfers[j].BlockNumber
		})
		if len(allTransfers) == 0 {
			zap.L().Debug("No transfers found")
			return nil
		}
		latestTransfer := allTransfers[len(allTransfers)-1]
		zap.L().Debug("Latest block",
			zap.Uint64("block", latestTransfer.BlockNumber),
			zap.Uint64("txIndex", latestTransfer.TransactionIndex),
			zap.Uint64("logIndex", latestTransfer.LogIndex),
		)

		lastSavedCheckpointValue, err := getLastSavedCheckpoint(txn)
		if err != nil {
			return fmt.Errorf("failed to get last saved checkpoint: %w", err)
		}
		zap.L().Debug("Last saved checkpoint", zap.String("checkpoint", string(lastSavedCheckpointValue)))

		return nil
	})

	if err != nil {
		zap.L().Error("Failed to view db", zap.Error(err))
		return nil
	}

	return &DefaultTdhTransfersReceivedAction{
		ctx:             ctx,
		progressTracker: progressTracker,
		db:              db,
		transferDb:      transferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}
}

func (a *DefaultTdhTransfersReceivedAction) applyTransfer(
	txn *badger.Txn,
	transfer tokens.TokenTransfer,
) error {
	// 1) Store the transfer
	if err := a.transferDb.StoreTransfer(txn, transfer); err != nil {
		zap.L().Error("Failed to store transfer", zap.Error(err))
		return fmt.Errorf("failed to store transfer: %w", err)
	}

	// 2) Handle Minting / Burning
	if transfer.Type == tokens.MINT || transfer.Type == tokens.AIRDROP {
		if err := a.nftDb.UpdateSupply(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
			zap.L().Error("Failed to update NFT supply", zap.Error(err))
			return fmt.Errorf("failed to update NFT supply: %w", err)
		}
	} else if transfer.Type == tokens.BURN {
		if err := a.nftDb.UpdateBurntSupply(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
			zap.L().Error("Failed to update NFT burnt supply", zap.Error(err))
			return fmt.Errorf("failed to update NFT burnt supply: %w", err)
		}
	}

	// 3) Perform ownership update
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

func (a *DefaultTdhTransfersReceivedAction) applyTransferReverse(
	txn *badger.Txn,
	transfer tokens.TokenTransfer,
) error {
	// 1) If it was a burn forward, revert burnt supply
	if transfer.Type == tokens.BURN {
		if err := a.nftDb.UpdateBurntSupplyReverse(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
			zap.L().Error("Failed to revert burnt supply", zap.Error(err))
			return fmt.Errorf("failed to revert burnt supply: %w", err)
		}
	}

	// 2) If it was a mint forward, revert minted supply
	if transfer.Type == tokens.MINT {
		if err := a.nftDb.UpdateSupplyReverse(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
			zap.L().Error("Failed to revert minted supply", zap.Error(err))
			return fmt.Errorf("failed to revert minted supply: %w", err)
		}
	}

	// 3) Revert ownership
	if err := a.ownerDb.UpdateOwnershipReverse(
		txn,
		transfer.From,
		transfer.To,
		transfer.Contract,
		transfer.TokenID,
		transfer.Amount,
	); err != nil {
		zap.L().Error("Failed to revert ownership", zap.Error(err))
		return fmt.Errorf("failed to revert ownership: %w", err)
	}

	return nil
}

func (a *DefaultTdhTransfersReceivedAction) reset(
	blockNumber uint64,
	txIndex uint64,
	logIndex uint64,
) error {
	zap.L().Info("Resetting transfers received DB",
		zap.Uint64("blockNumber", blockNumber),
		zap.Uint64("txIndex", txIndex),
		zap.Uint64("logIndex", logIndex),
	)

	err := a.db.Update(func(txn *badger.Txn) error {
		// 1) Get all transfers at or after checkpoint
		transfers, err := a.transferDb.GetTransfersAfterCheckpoint(txn, blockNumber, txIndex, logIndex)
		if err != nil {
			return fmt.Errorf("failed to get transfers: %w", err)
		}

		// 2) Sort descending
		sort.Slice(transfers, func(i, j int) bool {
			if transfers[i].BlockNumber != transfers[j].BlockNumber {
				return transfers[i].BlockNumber > transfers[j].BlockNumber
			}
			if transfers[i].TransactionIndex != transfers[j].TransactionIndex {
				return transfers[i].TransactionIndex > transfers[j].TransactionIndex
			}
			return transfers[i].LogIndex > transfers[j].LogIndex
		})

		// 3) Reverse each transfer
		for _, tr := range transfers {
			if e := a.applyTransferReverse(txn, tr); e != nil {
				return e
			}
		}

		// 4) Delete them from TransferDb
		if err := a.transferDb.DeleteTransfersAfterCheckpoint(txn, blockNumber, txIndex, logIndex); err != nil {
			return fmt.Errorf("failed to delete transfers after checkpoint: %w", err)
		}

		// 5) Update the checkpoint
		latestTransfer, err := a.transferDb.GetLatestTransfer(txn)
		if err != nil {
			return fmt.Errorf("failed to get latest transfer: %w", err)
		}

		if latestTransfer == nil {
			deleteErr := a.db.Update(func(txn *badger.Txn) error {
				return txn.Delete([]byte(actionsReceivedCheckpointKey))
			})
			if deleteErr != nil {
				return fmt.Errorf("failed to delete checkpoint: %w", deleteErr)
			}
		} else {
			lastCheckpointValue := checkpointValue(*latestTransfer)

			saveErr := a.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(actionsReceivedCheckpointKey), []byte(lastCheckpointValue))
			})
			if saveErr != nil {
				return fmt.Errorf("failed to save checkpoint: %w", saveErr)
			}

			zap.L().Info("Checkpoint saved", zap.String(actionsReceivedCheckpointKey, lastCheckpointValue))
		}

		return nil
	})
	if err != nil {
		zap.L().Error("Reset error", zap.Error(err))
	}
	return err
}

func (a *DefaultTdhTransfersReceivedAction) Handle(transfersBatch tokens.TokenTransferBatch) error {
	if len(transfersBatch.Transfers) > 0 {
		zap.L().Debug("Processing transfers received action", zap.Int("transfers", len(transfersBatch.Transfers)))
	}

	const batchSize = 100
	numTransfers := len(transfersBatch.Transfers)
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
		chunk := transfersBatch.Transfers[start:end]

		firstTransfer := chunk[0]
		firstTransferValue := checkpointValue(firstTransfer)

		lastTransfer := chunk[len(chunk)-1]
		lastProcessedValue := checkpointValue(lastTransfer)

		zap.L().Debug("Processing batch",
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
				return err
			}

			firstBlock, firstTxIndex, firstLogIndex, err := parseCheckpoint(firstTransferValue)
			if err != nil {
				return err
			}

			// If new data is behind (or same logIndex) => reset needed
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
				if err := a.applyTransfer(txn, t); err != nil {
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
				// retry the entire batch (or entire set) after reset
				return a.Handle(transfersBatch)
			}
			return err
		}

		if len(transfersBatch.Transfers) > 0 {
			zap.L().Debug("Batch processed",
				zap.String("batch", fmt.Sprintf("%d/%d", batchIndex+1, numBatches)),
				zap.Int("batchSize", len(chunk)),
				zap.String(actionsReceivedCheckpointKey, lastProcessedValue),
			)
		} else {
			zap.L().Debug("Checkpoint saved",
				zap.String(actionsReceivedCheckpointKey, lastProcessedValue),
			)
		}
	}

	if err := a.progressTracker.SetProgress(transfersBatch.BlockNumber); err != nil {
		return fmt.Errorf("error setting progress: %w", err)
	}

	return nil
}

func getLastSavedCheckpoint(txn *badger.Txn) ([]byte, error) {
	var lastSavedCheckpointValue []byte
	lastSavedCheckpoint, err := txn.Get([]byte(actionsReceivedCheckpointKey))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			zap.L().Debug("No previous checkpoint found")
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
