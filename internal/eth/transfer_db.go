package eth

import (
	"encoding/json"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
)

type TransferDb interface {
	StoreTransfer(txn *badger.Txn, transfer tokens.TokenTransfer) error
	GetTransfersByBlockNumber(txn *badger.Txn, blockNumber uint64) ([]tokens.TokenTransfer, error)
	GetTransfersByTxHash(txn *badger.Txn, txHash string) ([]tokens.TokenTransfer, error)
}

func NewTransferDb() TransferDb {
	return &TransferDbImpl{}
}

type TransferDbImpl struct {}

const transferPrefix = "tdh:transfer:"

func (t *TransferDbImpl) StoreTransfer(txn *badger.Txn, transfer tokens.TokenTransfer) error {
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
}

func (t *TransferDbImpl) GetTransfersByBlockNumber(txn *badger.Txn, blockNumber uint64) ([]tokens.TokenTransfer, error) {
	var transfers []tokens.TokenTransfer

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
			return nil, err
		}

		transfers = append(transfers, transfer)
	}

	return transfers, nil
}

func (t *TransferDbImpl) GetTransfersByTxHash(txn *badger.Txn, txHash string) ([]tokens.TokenTransfer, error) {
	var transferKeys []string

	// Get primary keys stored for this txHash
	txHashKey := fmt.Sprintf("tdh:txhash:%s", txHash)
	item, err := txn.Get([]byte(txHashKey))
	if err == badger.ErrKeyNotFound {
		return nil, nil // No transfers found for this txHash
	} else if err != nil {
		return nil, err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &transferKeys)
	})
	if err != nil {
		return nil, err
	}

	// Retrieve the actual transfer data
	var transfers []tokens.TokenTransfer
	for _, key := range transferKeys {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			continue // Skip missing keys instead of failing the whole retrieval
		} else if err != nil {
			return nil, err
		}

		var transfer tokens.TokenTransfer
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &transfer)
		})
		if err != nil {
			return nil, err
		}

		transfers = append(transfers, transfer)
	}

	return transfers, nil
}