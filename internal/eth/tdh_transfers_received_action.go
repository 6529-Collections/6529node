package eth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/6529-Collections/6529node/internal/eth/ethdb"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"go.uber.org/zap"
)

type TdhTransfersReceivedAction interface {
	Handle(transfers models.TokenTransferBatch) error
}

var ErrResetRequired = errors.New("reset required due to out-of-order checkpoint")

type DefaultTdhTransfersReceivedAction struct {
	ctx             context.Context
	db              *sql.DB
	progressTracker ethdb.TdhIdxTrackerDb
	transferDb      ethdb.TransferDb
	ownerDb         ethdb.OwnerDb
	nftDb           ethdb.NFTDb
}

func NewTdhTransfersReceivedActionImpl(ctx context.Context, db *sql.DB, progressTracker ethdb.TdhIdxTrackerDb) *DefaultTdhTransfersReceivedAction {
	transferDb := ethdb.NewTransferDb()
	ownerDb := ethdb.NewOwnerDb()
	nftDb := ethdb.NewNFTDb()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		zap.L().Error("Failed to begin transaction", zap.Error(err))
		return nil
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				zap.L().Error("Failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex, err := getLastSavedCheckpoint(tx)
	if err != nil {
		zap.L().Error("Failed to get last saved checkpoint", zap.Error(err))
		return nil
	}

	zap.L().Debug("Last saved checkpoint", zap.String("checkpoint", fmt.Sprintf("%d:%d:%d", lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex)))

	if err != nil {
		zap.L().Error("Failed to get last saved checkpoint", zap.Error(err))
		return nil
	}

	return &DefaultTdhTransfersReceivedAction{
		ctx:             ctx,
		db:              db,
		progressTracker: progressTracker,
		transferDb:      transferDb,
		ownerDb:         ownerDb,
		nftDb:           nftDb,
	}
}

func (a *DefaultTdhTransfersReceivedAction) applyTransfer(
	txn *sql.Tx,
	transfer models.TokenTransfer,
) error {
	// 1) Store the transfer
	if err := a.transferDb.StoreTransfer(txn, transfer); err != nil {
		zap.L().Error("Failed to store transfer", zap.Error(err))
		return fmt.Errorf("failed to store transfer: %w", err)
	}

	// 2) Handle Minting / Burning
	if transfer.Type == models.MINT || transfer.Type == models.AIRDROP {
		if err := a.nftDb.UpdateSupply(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
			zap.L().Error("Failed to update NFT supply", zap.Error(err))
			return fmt.Errorf("failed to update NFT supply: %w", err)
		}
	} else if transfer.Type == models.BURN {
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
	txn *sql.Tx,
	transfer models.TokenTransfer,
) error {
	// 1) If it was a burn forward, revert burnt supply
	if transfer.Type == models.BURN {
		if err := a.nftDb.UpdateBurntSupplyReverse(txn, transfer.Contract, transfer.TokenID, transfer.Amount); err != nil {
			zap.L().Error("Failed to revert burnt supply", zap.Error(err))
			return fmt.Errorf("failed to revert burnt supply: %w", err)
		}
	}

	// 2) If it was a mint forward, revert minted supply
	if transfer.Type == models.MINT || transfer.Type == models.AIRDROP {
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

	tx, err := a.db.BeginTx(a.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use a named return variable to ensure rollback logic works correctly
	var finalErr error
	defer func() {
		if finalErr != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				zap.L().Error("Failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	// 1) Gather all transfers at or after checkpoint in a *read* transaction.
	transfers, err := a.transferDb.GetTransfersAfterCheckpoint(tx, blockNumber, txIndex, logIndex)
	if err != nil {
		finalErr = fmt.Errorf("failed to get transfers: %w", err)
		return finalErr
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
		if e := a.applyTransferReverse(tx, tr); e != nil {
			finalErr = e
			return finalErr
		}
	}

	// 4) Delete from TransferDb
	err = a.transferDb.DeleteTransfersAfterCheckpoint(tx, blockNumber, txIndex, logIndex)
	if err != nil {
		finalErr = fmt.Errorf("failed to delete transfers after checkpoint: %w", err)
		return finalErr
	}

	// 5) Finally update the checkpoint in the same transaction
	latestTransfer, err := a.transferDb.GetLatestTransfer(tx)
	if err != nil {
		finalErr = fmt.Errorf("failed to get latest transfer: %w", err)
		return finalErr
	}

	if latestTransfer == nil {
		_, err = tx.Exec("DELETE FROM token_transfers_checkpoint")
	} else {
		err = updateCheckpoint(tx, latestTransfer.BlockNumber, latestTransfer.TransactionIndex, latestTransfer.LogIndex)
	}

	if err != nil {
		finalErr = err
		return finalErr
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	zap.L().Info("Reset complete")
	return nil
}

func (a *DefaultTdhTransfersReceivedAction) Handle(transfersBatch models.TokenTransferBatch) error {
	if len(transfersBatch.Transfers) > 0 {
		zap.L().Debug("Processing transfers received action", zap.Int("transfers", len(transfersBatch.Transfers)))
	}

	const batchSize = 100
	numTransfers := len(transfersBatch.Transfers)
	if numTransfers == 0 {
		return nil
	}

	tx, err := a.db.BeginTx(a.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				zap.L().Error("Failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	numBatches := (numTransfers + batchSize - 1) / batchSize // integer ceil

	for batchIndex := 0; batchIndex < numBatches; batchIndex++ {
		start := batchIndex * batchSize
		end := start + batchSize
		if end > numTransfers {
			end = numTransfers
		}
		chunk := transfersBatch.Transfers[start:end]

		firstTransfer := chunk[0]
		lastTransfer := chunk[len(chunk)-1]

		lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex, err := getLastSavedCheckpoint(tx)
		if err != nil {
			return fmt.Errorf("failed to get last saved checkpoint: %w", err)
		}

		// If new data is behind (or same logIndex) => reset needed
		if lastSavedBlock > firstTransfer.BlockNumber ||
			(lastSavedBlock == firstTransfer.BlockNumber && lastSavedTxIndex > firstTransfer.TransactionIndex) ||
			(lastSavedBlock == firstTransfer.BlockNumber && lastSavedTxIndex == firstTransfer.TransactionIndex && lastSavedLogIndex >= firstTransfer.LogIndex) {
			zap.L().Warn("Checkpoint error: reset needed",
				zap.String("lastCheckpoint", fmt.Sprintf("%d:%d:%d", lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex)),
				zap.String("firstTransfer", fmt.Sprintf("%d:%d:%d", firstTransfer.BlockNumber, firstTransfer.TransactionIndex, firstTransfer.LogIndex)),
			)
			return ErrResetRequired
		}

		for _, t := range chunk {
			if err := a.applyTransfer(tx, t); err != nil {
				zap.L().Error("Failed to process transfer", zap.String("tx", t.TxHash), zap.Uint64("logIndex", t.LogIndex), zap.Error(err))
				return err
			}
		}

		err = updateCheckpoint(tx, lastTransfer.BlockNumber, lastTransfer.TransactionIndex, lastTransfer.LogIndex)

		zap.L().Info("Checkpoint saved", zap.String("checkpoint", fmt.Sprintf("%d:%d:%d", lastTransfer.BlockNumber, lastTransfer.TransactionIndex, lastTransfer.LogIndex)))

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
				zap.String("checkpoint", fmt.Sprintf("%d:%d:%d", lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex)),
			)
		} else {
			zap.L().Debug("Checkpoint saved",
				zap.String("checkpoint", fmt.Sprintf("%d:%d:%d", lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex)),
			)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if err := a.progressTracker.SetProgress(transfersBatch.BlockNumber, a.ctx); err != nil {
		return fmt.Errorf("error setting progress: %w", err)
	}

	return nil
}

func getLastSavedCheckpoint(tx *sql.Tx) (uint64, uint64, uint64, error) {
	checkpoint := &models.TokenTransferCheckpoint{}
	err := tx.QueryRow("SELECT block_number, transaction_index, log_index FROM token_transfers_checkpoint").Scan(&checkpoint.BlockNumber, &checkpoint.TransactionIndex, &checkpoint.LogIndex)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, err
	}
	return checkpoint.BlockNumber, checkpoint.TransactionIndex, checkpoint.LogIndex, nil
}

func updateCheckpoint(tx *sql.Tx, blockNumber uint64, txIndex uint64, logIndex uint64) error {
	_, err := tx.Exec(`
		INSERT INTO token_transfers_checkpoint (id, block_number, transaction_index, log_index)
VALUES (1, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET 
    block_number = excluded.block_number,
    transaction_index = excluded.transaction_index,
    log_index = excluded.log_index;`,
		blockNumber, txIndex, logIndex)

	return err
}
