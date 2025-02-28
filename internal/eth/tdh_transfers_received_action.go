package eth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/6529-Collections/6529node/internal/db"
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
	transferDb      ethdb.NFTTransferDb
	ownerDb         ethdb.NFTOwnerDb
	nftDb           ethdb.NFTDb
}

func NewTdhTransfersReceivedActionImpl(ctx context.Context, db *sql.DB, transferDb ethdb.NFTTransferDb, ownerDb ethdb.NFTOwnerDb, nftDb ethdb.NFTDb, progressTracker ethdb.TdhIdxTrackerDb) *DefaultTdhTransfersReceivedAction {
	lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex, err := getLastSavedCheckpoint(db)
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

		// 1) if it's a burn, update the burnt supply
		if transfer.Type == models.BURN {
			if updateBurntSupplyErr := a.nftDb.UpdateBurntSupply(txn, transfer.Contract, transfer.TokenID); updateBurntSupplyErr != nil {
				return fmt.Errorf("failed to update NFT burnt supply: %w", updateBurntSupplyErr)
			}
		}

		// 2) If it's a mint or airdrop, update the supply and get the unique ID,
		// otherwise get the unique ID from the owner
		if transfer.Type == models.MINT || transfer.Type == models.AIRDROP {
			newSupply, updateSupplyErr := a.nftDb.UpdateSupply(txn, transfer.Contract, transfer.TokenID)
			if updateSupplyErr != nil {
				return fmt.Errorf("failed to update NFT supply: %w", updateSupplyErr)
			}
			tokenUniqueID = newSupply
		} else {
			uniqueID, getUniqueIDErr := a.ownerDb.GetUniqueID(txn, transfer.Contract, transfer.TokenID, transfer.From)
			if getUniqueIDErr != nil {
				return fmt.Errorf("failed to get NFT unique ID FROM owner %s for token %s:%s: %w", transfer.From, transfer.Contract, transfer.TokenID, getUniqueIDErr)
			}
			tokenUniqueID = uniqueID
		}

		// 3) Store the transfer
		if storeTrfErr := a.transferDb.StoreTransfer(txn, transfer, tokenUniqueID); storeTrfErr != nil {
			return fmt.Errorf("failed to store transfer: %w", storeTrfErr)
		}

		// 4) Update ownership
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
		return fmt.Errorf("failed to get NFT unique ID TO owner %s for token %s:%s: %w", transfer.To, transfer.Contract, transfer.TokenID, getUniqueIDErr)
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

	// 4) Delete the transfer
	if deleteTransferErr := a.transferDb.DeleteTransfer(txn, transfer, tokenUniqueID); deleteTransferErr != nil {
		return fmt.Errorf("failed to delete transfer: %w", deleteTransferErr)
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

	// 2) Sort descending by block number, tx index, and log index
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

	// 4) Finally update the checkpoint in the same transaction
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
	numTransfers := len(transfersBatch.Transfers)
	if numTransfers == 0 {
		return nil
	}

	// sort ascending by block number, tx index, and log index
	sort.Slice(transfersBatch.Transfers, func(i, j int) bool {
		if transfersBatch.Transfers[i].BlockNumber != transfersBatch.Transfers[j].BlockNumber {
			return transfersBatch.Transfers[i].BlockNumber < transfersBatch.Transfers[j].BlockNumber
		}
		if transfersBatch.Transfers[i].TransactionIndex != transfersBatch.Transfers[j].TransactionIndex {
			return transfersBatch.Transfers[i].TransactionIndex < transfersBatch.Transfers[j].TransactionIndex
		}
		return transfersBatch.Transfers[i].LogIndex < transfersBatch.Transfers[j].LogIndex
	})

	_, txErr := db.TxRunner(a.ctx, a.db, func(tx *sql.Tx) (struct{}, error) {
		zap.L().Debug("Processing transfers received action", zap.Int("transfers", numTransfers))

		firstTransfer := transfersBatch.Transfers[0]
		lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex, savedCheckpointErr := getLastSavedCheckpoint(tx)
		if savedCheckpointErr != nil {
			return struct{}{}, fmt.Errorf("failed to get last saved checkpoint: %w", savedCheckpointErr)
		}

		// If new data is behind (or same logIndex) => reset needed.
		if lastSavedBlock > firstTransfer.BlockNumber ||
			(lastSavedBlock == firstTransfer.BlockNumber && lastSavedTxIndex > firstTransfer.TransactionIndex) ||
			(lastSavedBlock == firstTransfer.BlockNumber && lastSavedTxIndex == firstTransfer.TransactionIndex && lastSavedLogIndex >= firstTransfer.LogIndex) {
			zap.L().Warn("Checkpoint error: reset needed",
				zap.String("lastCheckpoint", fmt.Sprintf("%d:%d:%d", lastSavedBlock, lastSavedTxIndex, lastSavedLogIndex)),
				zap.String("firstTransfer", fmt.Sprintf("%d:%d:%d", firstTransfer.BlockNumber, firstTransfer.TransactionIndex, firstTransfer.LogIndex)),
			)
			zap.L().Warn("Resetting due to checkpoint mismatch")

			if resetErr := a.reset(tx, firstTransfer.BlockNumber, firstTransfer.TransactionIndex, firstTransfer.LogIndex); resetErr != nil {
				return struct{}{}, resetErr
			}
		}

		const batchSize = 100

		numBatches := (numTransfers + batchSize - 1) / batchSize // integer ceil

		for batchIndex := 0; batchIndex < numBatches; batchIndex++ {
			start := batchIndex * batchSize
			end := start + batchSize
			if end > numTransfers {
				end = numTransfers
			}
			chunk := transfersBatch.Transfers[start:end]

			for _, t := range chunk {
				if err := a.applyTransfer(tx, t); err != nil {
					return struct{}{}, err
				}
			}

			lastChunkTransfer := chunk[len(chunk)-1]

			zap.L().Debug("Batch processed",
				zap.String("batch", fmt.Sprintf("%d/%d", batchIndex+1, numBatches)),
				zap.Int("batchSize", len(chunk)),
				zap.String("lastTransfer", fmt.Sprintf("%d:%d:%d", lastChunkTransfer.BlockNumber, lastChunkTransfer.TransactionIndex, lastChunkTransfer.LogIndex)),
			)
		}

		lastTransfer := transfersBatch.Transfers[numTransfers-1]

		if updateCheckpointErr := updateCheckpoint(tx, lastTransfer.BlockNumber, lastTransfer.TransactionIndex, lastTransfer.LogIndex); updateCheckpointErr != nil {
			return struct{}{}, updateCheckpointErr
		}

		zap.L().Info("Checkpoint saved", zap.String("checkpoint", fmt.Sprintf("%d:%d:%d", lastTransfer.BlockNumber, lastTransfer.TransactionIndex, lastTransfer.LogIndex)))

		return struct{}{}, nil
	})

	if txErr != nil {
		return txErr
	}

	if progressErr := a.progressTracker.SetProgress(transfersBatch.BlockNumber, a.ctx); progressErr != nil {
		return fmt.Errorf("error setting progress: %w", progressErr)
	}

	return nil
}

var getLastSavedCheckpoint = func(q db.QueryRunner) (uint64, uint64, uint64, error) {
	checkpoint := &ethdb.TokenTransferCheckpoint{}
	err := q.QueryRow("SELECT block_number, transaction_index, log_index FROM token_transfers_checkpoint").Scan(&checkpoint.BlockNumber, &checkpoint.TransactionIndex, &checkpoint.LogIndex)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, err
	}
	return checkpoint.BlockNumber, checkpoint.TransactionIndex, checkpoint.LogIndex, nil
}

var updateCheckpoint = func(tx *sql.Tx, blockNumber uint64, txIndex uint64, logIndex uint64) error {
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
