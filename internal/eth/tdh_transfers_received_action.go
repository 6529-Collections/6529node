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

type DefaultTdhTransfersReceivedAction struct {
	ctx             context.Context
	db              *sql.DB
	progressTracker ethdb.TdhIdxTrackerDb
	transferDb      ethdb.TransferDb
	ownerDb         ethdb.OwnerDb
	nftDb           ethdb.NFTDb
}

func NewTdhTransfersReceivedActionImpl(ctx context.Context, db *sql.DB, transferDb ethdb.TransferDb, ownerDb ethdb.OwnerDb, nftDb ethdb.NFTDb, progressTracker ethdb.TdhIdxTrackerDb) *DefaultTdhTransfersReceivedAction {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		zap.L().Error("Failed to begin transaction", zap.Error(err))
		return nil
	}

	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				zap.L().Error("Transaction rollback error", zap.Error(rbErr))
			}
		} else {
			if cmErr := tx.Commit(); cmErr != nil {
				zap.L().Error("Transaction commit error", zap.Error(cmErr))
			}
		}
	}()

	lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex, err := getLastSavedCheckpoint(tx)
	if err != nil {
		zap.L().Error("Failed to get last saved checkpoint", zap.Error(err))
		return nil
	}

	zap.L().Debug("Last saved checkpoint", zap.String("checkpoint", fmt.Sprintf("%d:%d:%d", lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex)))

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
	for i := int64(0); i < transfer.Amount; i++ {
		var tokenUniqueID uint64

		// 1) Update NFT supplies
		if transfer.Type == models.MINT || transfer.Type == models.AIRDROP {
			newSupply, updateSupplyErr := a.nftDb.UpdateSupply(txn, transfer.Contract, transfer.TokenID)
			if updateSupplyErr != nil {
				return fmt.Errorf("failed to update NFT supply: %w", updateSupplyErr)
			}
			tokenUniqueID = newSupply
		} else {
			if transfer.Type == models.BURN {
				if updateBurntSupplyErr := a.nftDb.UpdateBurntSupply(txn, transfer.Contract, transfer.TokenID); updateBurntSupplyErr != nil {
					return fmt.Errorf("failed to update NFT burnt supply: %w", updateBurntSupplyErr)
				}
			}
			uniqueID, getUniqueIDErr := a.ownerDb.GetUniqueID(txn, transfer.Contract, transfer.TokenID, transfer.From)
			if getUniqueIDErr != nil {
				return fmt.Errorf("failed to get NFT unique ID: %w", getUniqueIDErr)
			}
			tokenUniqueID = uniqueID
		}

		// 2) Store the transfer
		if storeTrfErr := a.transferDb.StoreTransfer(txn, transfer, tokenUniqueID); storeTrfErr != nil {
			return fmt.Errorf("failed to store transfer: %w", storeTrfErr)
		}

		// 3) Update ownership
		if updateOwnershipErr := a.ownerDb.UpdateOwnership(txn, transfer, tokenUniqueID); updateOwnershipErr != nil {
			return fmt.Errorf("failed to update ownership: %w", updateOwnershipErr)
		}
	}

	return nil
}

func (a *DefaultTdhTransfersReceivedAction) applyTransferReverse(
	txn *sql.Tx,
	transfer ethdb.NFTTransfer,
) error {
	// 0) Get the NFT unique ID
	tokenUniqueID, getUniqueIDErr := a.ownerDb.GetUniqueID(txn, transfer.Contract, transfer.TokenID, transfer.To)
	if getUniqueIDErr != nil {
		return fmt.Errorf("failed to get NFT unique ID: %w", getUniqueIDErr)
	}

	// 1) If it was a burn forward, revert burnt supply
	if transfer.Type == models.BURN {
		if updateBurntSupplyErr := a.nftDb.UpdateBurntSupplyReverse(txn, transfer.Contract, transfer.TokenID); updateBurntSupplyErr != nil {
			return fmt.Errorf("failed to revert burnt supply: %w", updateBurntSupplyErr)
		}
	}

	// 2) If it was a mint forward, revert minted supply
	if transfer.Type == models.MINT || transfer.Type == models.AIRDROP {
		if _, updateSupplyErr := a.nftDb.UpdateSupplyReverse(txn, transfer.Contract, transfer.TokenID); updateSupplyErr != nil {
			return fmt.Errorf("failed to revert minted supply: %w", updateSupplyErr)
		}
	}

	// 3) Revert ownership
	if updateOwnershipErr := a.ownerDb.UpdateOwnershipReverse(txn, transfer, tokenUniqueID); updateOwnershipErr != nil {
		return fmt.Errorf("failed to revert ownership: %w", updateOwnershipErr)
	}

	return nil
}

func (a *DefaultTdhTransfersReceivedAction) reset(
	tx *sql.Tx,
	blockNumber uint64,
	txIndex uint64,
	logIndex uint64,
) error {
	zap.L().Info("Resetting transfers received DB",
		zap.Uint64("blockNumber", blockNumber),
		zap.Uint64("txIndex", txIndex),
		zap.Uint64("logIndex", logIndex),
	)

	// 1) Gather all transfers at or after checkpoint
	transfers, err := a.transferDb.GetTransfersAfterCheckpoint(tx, blockNumber, txIndex, logIndex)
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
		if e := a.applyTransferReverse(tx, tr); e != nil {
			return e
		}
	}

	// 4) Delete from TransferDb
	err = a.transferDb.DeleteTransfersAfterCheckpoint(tx, blockNumber, txIndex, logIndex)
	if err != nil {
		return fmt.Errorf("failed to delete transfers after checkpoint: %w", err)
	}

	// 5) Finally update the checkpoint in the same transaction
	latestTransfer, err := a.transferDb.GetLatestTransfer(tx)
	if err != nil {
		return fmt.Errorf("failed to get latest transfer: %w", err)
	}

	if latestTransfer == nil {
		_, err = tx.Exec("DELETE FROM token_transfers_checkpoint")
	} else {
		err = updateCheckpoint(tx, latestTransfer.BlockNumber, latestTransfer.TransactionIndex, latestTransfer.LogIndex)
	}

	if err != nil {
		return fmt.Errorf("failed to update checkpoint: %w", err)
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
			_ = tx.Rollback()
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
			zap.L().Warn("Resetting due to checkpoint mismatch")
			err = a.reset(tx, firstTransfer.BlockNumber, firstTransfer.TransactionIndex, firstTransfer.LogIndex)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}

		for _, t := range chunk {
			if err := a.applyTransfer(tx, t); err != nil {
				_ = tx.Rollback()
				return err
			}
		}

		err = updateCheckpoint(tx, lastTransfer.BlockNumber, lastTransfer.TransactionIndex, lastTransfer.LogIndex)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		zap.L().Info("Checkpoint saved", zap.String("checkpoint", fmt.Sprintf("%d:%d:%d", lastTransfer.BlockNumber, lastTransfer.TransactionIndex, lastTransfer.LogIndex)))

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
	checkpoint := &ethdb.TokenTransferCheckpoint{}
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
