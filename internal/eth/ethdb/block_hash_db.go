package ethdb

import (
	"database/sql"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

type BlockHashDb interface {
	GetHash(blockNumber uint64) (common.Hash, bool)
	SetHash(blockNumber uint64, hash common.Hash) error
	RevertFromBlock(fromBlock uint64) error
}

func NewBlockHashDb(db *sql.DB) BlockHashDb {
	return &BlockHashDbImpl{db: db}
}

type BlockHashDbImpl struct {
	mu sync.RWMutex
	db *sql.DB
}

func (b *BlockHashDbImpl) GetHash(blockNumber uint64) (common.Hash, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := b.db.QueryRow("select hash from block_hash where block_number = ?", blockNumber)
	var hash string
	err := result.Scan(&hash)
	if err != nil {
		return common.Hash{}, false
	}
	return common.HexToHash(hash), true

}

func (b *BlockHashDbImpl) SetHash(blockNumber uint64, hash common.Hash) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.db.Exec("insert into block_hash (block_number, hash) values (?, ?) on conflict(block_number) do update set hash = ?", blockNumber, hash.Hex(), hash.Hex())
	return err
}

func (b *BlockHashDbImpl) RevertFromBlock(fromBlock uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.db.Exec("delete from block_hash where block_number >= ?", fromBlock)
	return err
}
