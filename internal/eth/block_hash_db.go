package eth

import (
	"encoding/binary"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/ethereum/go-ethereum/common"
)

type BlockHashDb interface {
	GetHash(blockNumber uint64) (common.Hash, bool)
	SetHash(blockNumber uint64, hash common.Hash) error
	RevertFromBlock(fromBlock uint64) error
}

func NewBlockHashDb(db *badger.DB) BlockHashDb {
	return &BlockHashDbImpl{db: db}
}

type BlockHashDbImpl struct {
	mu sync.RWMutex
	db *badger.DB
}

const blockHashPrefix = "indexer:blockHash:"

func (b *BlockHashDbImpl) GetHash(blockNumber uint64) (common.Hash, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var blockHash common.Hash
	err := b.db.View(func(txn *badger.Txn) error {
		key := encodeKey(blockNumber)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		copy(blockHash[:], val)
		return nil
	})
	if err == badger.ErrKeyNotFound {
		return common.Hash{}, false
	}
	if err != nil {
		return common.Hash{}, false
	}
	return blockHash, true
}

func (b *BlockHashDbImpl) SetHash(blockNumber uint64, hash common.Hash) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.db.Update(func(txn *badger.Txn) error {
		key := encodeKey(blockNumber)
		val := hash[:]
		return txn.Set(key, val)
	})
}

func (b *BlockHashDbImpl) RevertFromBlock(fromBlock uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.db.Update(func(txn *badger.Txn) error {
		var keysToDelete [][]byte

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := encodeKey(fromBlock)
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			if len(k) < len(blockHashPrefix) || string(k[:len(blockHashPrefix)]) != blockHashPrefix {
				break
			}

			blockNum := decodeKey(k)
			if blockNum >= fromBlock {
				keyCopy := append([]byte(nil), k...)
				keysToDelete = append(keysToDelete, keyCopy)
			}
		}

		for _, k := range keysToDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})
}

func encodeKey(blockNum uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], blockNum)
	return append([]byte(blockHashPrefix), buf[:]...)
}

func decodeKey(key []byte) uint64 {
	numBytes := key[len(blockHashPrefix):]
	return binary.BigEndian.Uint64(numBytes)
}
