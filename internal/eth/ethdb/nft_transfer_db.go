package ethdb

import (
	"database/sql"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
)

// NFTTransferDb interface for managing token transfers.
type NFTTransferDb interface {
	StoreTransfer(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error
	GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]*NFTTransfer, error)
	DeleteTransfer(tx *sql.Tx, transfer NFTTransfer, tokenUniqueID uint64) error
	GetLatestTransfer(tx *sql.Tx) (*NFTTransfer, error)
	db.PaginatedQuerier[NFTTransfer]
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
func (t *TransferDbImpl) GetTransfersAfterCheckpoint(tx *sql.Tx, blockNumber, txIndex, logIndex uint64) ([]*NFTTransfer, error) {
	rows, err := tx.Query(allTransfersQuery+`
		WHERE block_number > ? OR (block_number = ? AND transaction_index > ?) 
			OR (block_number = ? AND transaction_index = ? AND log_index >= ?)`,
		blockNumber, blockNumber, txIndex, blockNumber, txIndex, logIndex)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return db.ScanAll[*NFTTransfer](rows, newNFTTransfer)
}

func newNFTTransfer() *NFTTransfer {
	return &NFTTransfer{}
}

// GetLatestTransfer retrieves the most recent transfer.
func (t *TransferDbImpl) GetLatestTransfer(tx *sql.Tx) (*NFTTransfer, error) {
	_, data, err := t.GetPaginatedResponseForQuery(tx, db.QueryOptions{
		Direction: db.QueryDirectionDesc,
		Page:      1,
		PageSize:  1,
	}, []interface{}{})

	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	return data[0], nil
}

func (t *TransferDbImpl) GetPaginatedResponseForQuery(rq db.QueryRunner, queryOptions db.QueryOptions, queryParams []interface{}) (total int, data []*NFTTransfer, err error) {
	return db.GetPaginatedResponseForQuery[*NFTTransfer]("nft_transfers", rq, allTransfersQuery, queryOptions, []string{"block_number", "transaction_index", "log_index"}, queryParams, newNFTTransfer)
}
