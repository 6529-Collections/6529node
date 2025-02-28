package ethdb

import (
	"database/sql"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
)

// NFTTransferDb interface for managing token transfers.
type NFTTransferDb interface {
	StoreTransfer(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error
	GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]NFTTransfer, error)
	DeleteTransfer(tx *sql.Tx, transfer NFTTransfer, tokenUniqueID uint64) error
	GetLatestTransfer(tx *sql.Tx) (*NFTTransfer, error)

	GetAllTransfers(rq db.QueryRunner, pageSize int, page int) (total int, transfers []NFTTransfer, err error)
	GetTransfersForContract(rq db.QueryRunner, contract string, pageSize int, page int) (total int, transfers []NFTTransfer, err error)
	GetTransfersForContractToken(rq db.QueryRunner, contract string, tokenID string, pageSize int, page int) (total int, transfers []NFTTransfer, err error)
	GetTransfersForTxHash(rq db.QueryRunner, txHash string, pageSize int, page int) (total int, transfers []NFTTransfer, err error)
}

// NewTransferDb creates a new NFTTransferDb instance.
func NewTransferDb() NFTTransferDb {
	return &TransferDbImpl{}
}

// TransferDbImpl implements NFTTransferDb.
type TransferDbImpl struct{}

const allTransfersQuery = `
	SELECT block_number, transaction_index, log_index, tx_hash, event_name, 
		from_address, to_address, contract, token_id, token_unique_id, block_time, transfer_type
	FROM nft_transfers
`

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

func (t *TransferDbImpl) DeleteTransfer(tx *sql.Tx, transfer NFTTransfer, tokenUniqueID uint64) error {
	_, err := tx.Exec(`
		DELETE FROM nft_transfers
		WHERE tx_hash = ? AND log_index = ? AND from_address = ? AND to_address = ? AND contract = ? AND token_id = ? AND token_unique_id = ?`,
		transfer.TxHash, transfer.LogIndex, transfer.From, transfer.To, transfer.Contract, transfer.TokenID, tokenUniqueID)
	return err
}

// GetTransfersAfterCheckpoint retrieves all transfers after a specific checkpoint.
func (t *TransferDbImpl) GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]NFTTransfer, error) {
	rows, err := tx.Query(allTransfersQuery+`
		WHERE block_number > ? OR (block_number = ? AND transaction_index > ?) 
			OR (block_number = ? AND transaction_index = ? AND log_index >= ?)`,
		blockNumber, blockNumber, txIndex, blockNumber, txIndex, logIndex)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanTransfers(rows)
}

// GetLatestTransfer retrieves the most recent transfer.
func (t *TransferDbImpl) GetLatestTransfer(tx *sql.Tx) (*NFTTransfer, error) {
	row := tx.QueryRow(allTransfersQuery + `
		ORDER BY block_number DESC, transaction_index DESC, log_index DESC LIMIT 1`)

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

func (t *TransferDbImpl) GetAllTransfers(rq db.QueryRunner, pageSize int, page int) (total int, transfers []NFTTransfer, err error) {
	offset := (page - 1) * pageSize

	rows, err := rq.Query(allTransfersQuery+`
		ORDER BY block_number ASC, transaction_index ASC, log_index ASC
		LIMIT ? OFFSET ?
	`, pageSize, offset)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var transfer NFTTransfer
		err := rows.Scan(
			&transfer.BlockNumber, &transfer.TransactionIndex, &transfer.LogIndex,
			&transfer.TxHash, &transfer.EventName, &transfer.From, &transfer.To,
			&transfer.Contract, &transfer.TokenID, &transfer.TokenUniqueID, &transfer.BlockTime, &transfer.Type,
		)
		if err != nil {
			return 0, nil, err
		}
		transfers = append(transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	err = rq.QueryRow("SELECT COUNT(*) FROM nft_transfers").Scan(&total)
	if err != nil {
		return 0, nil, err
	}

	return total, transfers, nil
}

func (t *TransferDbImpl) GetTransfersForContract(rq db.QueryRunner, contract string, pageSize int, page int) (total int, transfers []NFTTransfer, err error) {
	offset := (page - 1) * pageSize

	rows, err := rq.Query(allTransfersQuery+`
		WHERE contract = ?
		ORDER BY block_number ASC, transaction_index ASC, log_index ASC
		LIMIT ? OFFSET ?
	`, contract, pageSize, offset)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var transfer NFTTransfer
		err := rows.Scan(
			&transfer.BlockNumber, &transfer.TransactionIndex, &transfer.LogIndex,
			&transfer.TxHash, &transfer.EventName, &transfer.From, &transfer.To,
			&transfer.Contract, &transfer.TokenID, &transfer.TokenUniqueID, &transfer.BlockTime, &transfer.Type,
		)
		if err != nil {
			return 0, nil, err
		}
		transfers = append(transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	err = rq.QueryRow("SELECT COUNT(*) FROM nft_transfers WHERE contract = ?", contract).Scan(&total)
	if err != nil {
		return 0, nil, err
	}

	return total, transfers, nil
}

func (t *TransferDbImpl) GetTransfersForContractToken(rq db.QueryRunner, contract string, tokenID string, pageSize int, page int) (total int, transfers []NFTTransfer, err error) {
	offset := (page - 1) * pageSize

	rows, err := rq.Query(allTransfersQuery+`
		WHERE contract = ? AND token_id = ?
		ORDER BY block_number ASC, transaction_index ASC, log_index ASC
		LIMIT ? OFFSET ?
	`, contract, tokenID, pageSize, offset)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var transfer NFTTransfer
		err := rows.Scan(
			&transfer.BlockNumber, &transfer.TransactionIndex, &transfer.LogIndex,
			&transfer.TxHash, &transfer.EventName, &transfer.From, &transfer.To,
			&transfer.Contract, &transfer.TokenID, &transfer.TokenUniqueID, &transfer.BlockTime, &transfer.Type,
		)
		if err != nil {
			return 0, nil, err
		}
		transfers = append(transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	err = rq.QueryRow("SELECT COUNT(*) FROM nft_transfers WHERE contract = ? AND token_id = ?", contract, tokenID).Scan(&total)
	if err != nil {
		return 0, nil, err
	}

	return total, transfers, nil
}

func (t *TransferDbImpl) GetTransfersForTxHash(rq db.QueryRunner, txHash string, pageSize int, page int) (total int, transfers []NFTTransfer, err error) {
	offset := (page - 1) * pageSize

	rows, err := rq.Query(allTransfersQuery+`
		WHERE tx_hash = ?
		ORDER BY block_number ASC, transaction_index ASC, log_index ASC
		LIMIT ? OFFSET ?`, txHash, pageSize, offset)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var transfer NFTTransfer
		err := rows.Scan(
			&transfer.BlockNumber, &transfer.TransactionIndex, &transfer.LogIndex,
			&transfer.TxHash, &transfer.EventName, &transfer.From, &transfer.To,
			&transfer.Contract, &transfer.TokenID, &transfer.TokenUniqueID, &transfer.BlockTime, &transfer.Type,
		)
		if err != nil {
			return 0, nil, err
		}
		transfers = append(transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	err = rq.QueryRow("SELECT COUNT(*) FROM nft_transfers WHERE tx_hash = ?", txHash).Scan(&total)
	if err != nil {
		return 0, nil, err
	}

	return total, transfers, nil
}
