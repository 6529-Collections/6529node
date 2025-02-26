package ethdb

import (
	"context"
	"database/sql"
	"sync"
)

type TdhIdxTrackerDb interface {
	GetProgress() (uint64, error)
	SetProgress(blockNumber uint64, ctx context.Context) error
}

func NewTdhIdxTrackerDb(db *sql.DB) TdhIdxTrackerDb {
	return &TdhIdxTrackerDbImpl{db: db}
}

type TdhIdxTrackerDbImpl struct {
	db *sql.DB
	mu sync.RWMutex
}

func (b *TdhIdxTrackerDbImpl) GetProgress() (uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var blockNumber uint64
	err := b.db.QueryRow("select block_number from tdh_scan_index").Scan(&blockNumber)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
	}
	return blockNumber, err
}

func (b *TdhIdxTrackerDbImpl) SetProgress(blockNumber uint64, ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	_, err = tx.Exec("delete from tdh_scan_index")
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	_, err = tx.Exec("insert into tdh_scan_index (block_number) values (?)", blockNumber)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return err
}
