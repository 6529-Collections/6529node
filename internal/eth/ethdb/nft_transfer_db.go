package ethdb

import (
	"database/sql"

	"github.com/6529-Collections/6529node/pkg/tdh/models"
)

// TransferDb interface for managing token transfers.
type TransferDb interface {
	StoreTransfer(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error
	GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]NFTTransfer, error)
	DeleteTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) error
	GetLatestTransfer(tx *sql.Tx) (*NFTTransfer, error)
}

// NewTransferDb creates a new TransferDb instance.
func NewTransferDb() TransferDb {
	return &TransferDbImpl{}
}

// TransferDbImpl implements TransferDb.
type TransferDbImpl struct{}

// StoreTransfer inserts a new transfer into the database.
func (t *TransferDbImpl) StoreTransfer(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
	_, err := tx.Exec(`
		INSERT INTO nft_transfers (
			block_number, transaction_index, log_index, tx_hash, event_name, 
			from_address, to_address, contract, token_id, token_unique_id, block_time, transfer_type
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		transfer.BlockNumber, transfer.TransactionIndex, transfer.LogIndex, transfer.TxHash,
		transfer.EventName, transfer.From, transfer.To, transfer.Contract,
		transfer.TokenID, tokenUniqueID, transfer.BlockTime, transfer.Type)
	return err
}

// GetTransfersAfterCheckpoint retrieves all transfers after a specific checkpoint.
func (t *TransferDbImpl) GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]NFTTransfer, error) {
	rows, err := tx.Query(`
		SELECT block_number, transaction_index, log_index, tx_hash, event_name, 
			from_address, to_address, contract, token_id, token_unique_id, block_time, transfer_type
		FROM nft_transfers
		WHERE block_number > ? OR (block_number = ? AND transaction_index > ?) 
			OR (block_number = ? AND transaction_index = ? AND log_index >= ?)`,
		blockNumber, blockNumber, txIndex, blockNumber, txIndex, logIndex)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanTransfers(rows)
}

// DeleteTransfersAfterCheckpoint deletes all transfers after a specific checkpoint.
func (t *TransferDbImpl) DeleteTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) error {
	_, err := tx.Exec(`
		DELETE FROM nft_transfers
		WHERE block_number > ? OR (block_number = ? AND transaction_index > ?) 
			OR (block_number = ? AND transaction_index = ? AND log_index >= ?)`,
		blockNumber, blockNumber, txIndex, blockNumber, txIndex, logIndex)
	return err
}

// GetLatestTransfer retrieves the most recent transfer.
func (t *TransferDbImpl) GetLatestTransfer(tx *sql.Tx) (*NFTTransfer, error) {
	row := tx.QueryRow(`
		SELECT block_number, transaction_index, log_index, tx_hash, event_name, 
			from_address, to_address, contract, token_id, token_unique_id, block_time, transfer_type
		FROM nft_transfers ORDER BY block_number DESC, transaction_index DESC, log_index DESC LIMIT 1`)

	var transfer NFTTransfer
	err := row.Scan(
		&transfer.BlockNumber, &transfer.TransactionIndex, &transfer.LogIndex,
		&transfer.TxHash, &transfer.EventName, &transfer.From, &transfer.To,
		&transfer.Contract, &transfer.TokenID, &transfer.TokenUniqueID, &transfer.BlockTime, &transfer.Type,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &transfer, err
}

// scanTransfers is a helper to parse SQL rows into a slice of TokenTransfers.
func scanTransfers(rows *sql.Rows) ([]NFTTransfer, error) {
	var transfers []NFTTransfer
	for rows.Next() {
		var transfer NFTTransfer
		err := rows.Scan(
			&transfer.BlockNumber, &transfer.TransactionIndex, &transfer.LogIndex,
			&transfer.TxHash, &transfer.EventName, &transfer.From, &transfer.To,
			&transfer.Contract, &transfer.TokenID, &transfer.TokenUniqueID, &transfer.BlockTime, &transfer.Type,
		)
		if err != nil {
			return nil, err
		}
		transfers = append(transfers, transfer)
	}
	return transfers, nil
}
