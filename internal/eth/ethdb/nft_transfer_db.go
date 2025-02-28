package ethdb

import (
	"database/sql"
	"fmt"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"go.uber.org/zap"
)

// NFTTransferDb interface for managing token transfers.
type NFTTransferDb interface {
	StoreTransfer(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error
	GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]NFTTransfer, error)
	DeleteTransfer(tx *sql.Tx, transfer NFTTransfer, tokenUniqueID uint64) error
	GetLatestTransfer(tx *sql.Tx) (*NFTTransfer, error)

	GetPaginatedResponseForQuery(rq db.QueryRunner, queryOptions db.QueryOptions, queryParams []interface{}) (total int, transfers []NFTTransfer, err error)
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

	return scanTransfer(row)
}

func scanTransfer(scanner db.RowScanner) (*NFTTransfer, error) {
	var transfer NFTTransfer
	err := scanner.Scan(
		&transfer.BlockNumber, &transfer.TransactionIndex, &transfer.LogIndex,
		&transfer.TxHash, &transfer.EventName, &transfer.From, &transfer.To,
		&transfer.Contract, &transfer.TokenID, &transfer.TokenUniqueID, &transfer.BlockTime, &transfer.Type,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &transfer, err
}

var scanTransfers = func(rows *sql.Rows) ([]NFTTransfer, error) {
	var transfers []NFTTransfer
	for rows.Next() {
		transfer, err := scanTransfer(rows)
		if err != nil {
			return nil, err
		}
		transfers = append(transfers, *transfer)
	}
	return transfers, nil
}

func (t *TransferDbImpl) GetPaginatedResponseForQuery(rq db.QueryRunner, queryOptions db.QueryOptions, queryParams []interface{}) (total int, transfers []NFTTransfer, err error) {
	offset := (queryOptions.Page - 1) * queryOptions.PageSize

	order := fmt.Sprintf("block_number %s, transaction_index %s, log_index %s", queryOptions.Direction, queryOptions.Direction, queryOptions.Direction)

	where := ""
	if queryOptions.Where != "" {
		where = fmt.Sprintf("WHERE %s", queryOptions.Where)
	}

	query := fmt.Sprintf("%s %s ORDER BY %s LIMIT ? OFFSET ?", allTransfersQuery, where, order)
	params := append(queryParams, queryOptions.PageSize, offset)
	zap.L().Info("i am query", zap.String("query", query), zap.Any("params", params))

	rows, err := rq.Query(query, params...)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	transfers, err = scanTransfers(rows)
	if err != nil {
		return 0, nil, err
	}

	err = rows.Err()
	if err != nil {
		return 0, nil, err
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM nft_transfers %s", where)
	err = rq.QueryRow(countQuery, queryParams...).Scan(&total)
	if err != nil {
		return 0, nil, err
	}

	return total, transfers, nil
}
