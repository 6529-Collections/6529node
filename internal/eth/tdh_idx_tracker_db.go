package eth

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v3"
)

type TdhIdxTrackerDb interface {
	GetProgress() (uint64, error)
	SetProgress(blockNumber uint64) error
}

func NewTdhIdxTrackerDb(db *badger.DB) TdhIdxTrackerDb {
	return &TdhIdxTrackerDbImpl{db: db}
}

type TdhIdxTrackerDbImpl struct {
	db *badger.DB
}

const prefix = "indexer:transferWatcherProgress:"

func (b *TdhIdxTrackerDbImpl) GetProgress() (uint64, error) {
	var blockNumber uint64
	err := b.db.View(func(txn *badger.Txn) error {
		key := []byte(prefix)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			blockNumber = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return blockNumber, nil
	} else if err != nil {
		return blockNumber, err
	}
	return blockNumber, nil
}

func (b *TdhIdxTrackerDbImpl) SetProgress(blockNumber uint64) error {
	return b.db.Update(func(txn *badger.Txn) error {
		key := []byte(prefix)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, blockNumber)
		return txn.Set(key, buf)
	})
}
