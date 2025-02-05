package eth

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
)

type TransferDb interface {
	StoreTransfer(transfer tokens.TokenTransfer) error
	GetTransfersByBlockNumber(blockNumber uint64) ([]tokens.TokenTransfer, error)
	GetTransfersByTxHash(txHash string) ([]tokens.TokenTransfer, error)
	RevertFromBlock(blockNumber uint64) error
}

func NewTransferDb(db *badger.DB) TransferDb {
	return &TransferDbImpl{db: db}
}

type TransferDbImpl struct {
	mu sync.RWMutex
	db *badger.DB
}

const transferPrefix = "tdh:transfer:"

func (t *TransferDbImpl) StoreTransfer(transfer tokens.TokenTransfer) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.db.Update(func(txn *badger.Txn) error {
		// Primary key (as you have)
		primaryKey := fmt.Sprintf("%s%d:%s:%d:%d", transferPrefix, transfer.BlockNumber, transfer.TxHash, transfer.TransactionIndex, transfer.LogIndex)
		value, err := json.Marshal(transfer)
		if err != nil {
			return err
		}
		if err := txn.Set([]byte(primaryKey), value); err != nil {
			return err
		}

		// Secondary index key (for txHash lookup)
		txHashKey := fmt.Sprintf("tdh:txhash:%s", transfer.TxHash)
		existingKeys := []string{}

		// Try to retrieve existing keys for this txHash
		item, err := txn.Get([]byte(txHashKey))
		if err == nil {
			err = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &existingKeys)
			})
			if err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}

		// Append new primary key reference
		existingKeys = append(existingKeys, primaryKey)

		// Store the updated key list
		updatedValue, err := json.Marshal(existingKeys)
		if err != nil {
			return err
		}
		return txn.Set([]byte(txHashKey), updatedValue)
	})
}

func (t *TransferDbImpl) GetTransfersByBlockNumber(blockNumber uint64) ([]tokens.TokenTransfer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var transfers []tokens.TokenTransfer

	err := t.db.View(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("%s%d:", transferPrefix, blockNumber)
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			var transfer tokens.TokenTransfer

			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &transfer)
			})
			if err != nil {
				return err
			}

			transfers = append(transfers, transfer)
		}

		return nil
	})

	return transfers, err
}

func (t *TransferDbImpl) RevertFromBlock(blockNumber uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.db.Update(func(txn *badger.Txn) error {
		var keysToDelete [][]byte

		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := fmt.Sprintf("%s%d:", transferPrefix, blockNumber)
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			key := item.Key()
			keysToDelete = append(keysToDelete, append([]byte(nil), key...))
		}

		for _, k := range keysToDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})
}

func (t *TransferDbImpl) GetTransfersByTxHash(txHash string) ([]tokens.TokenTransfer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var transferKeys []string

	// Get primary keys stored for this txHash
	err := t.db.View(func(txn *badger.Txn) error {
		txHashKey := fmt.Sprintf("tdh:txhash:%s", txHash)
		item, err := txn.Get([]byte(txHashKey))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &transferKeys)
		})
	})
	if err != nil {
		return nil, err
	}

	// Retrieve the actual transfer data
	var transfers []tokens.TokenTransfer
	err = t.db.View(func(txn *badger.Txn) error {
		for _, key := range transferKeys {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}

			var transfer tokens.TokenTransfer
			err = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &transfer)
			})
			if err != nil {
				return err
			}

			transfers = append(transfers, transfer)
		}
		return nil
	})

	return transfers, err
}