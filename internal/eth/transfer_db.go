package eth

import (
	"encoding/json"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

/*
DB Indexes Created Here:

1. Primary transfer record:
   "tdh:transfer:{blockNumber}:{txIndex}:{logIndex}:{txHash}:{contract}:{tokenID}"
   => JSON(TokenTransfer)

   (We use zero-padding on numeric fields for lexical ordering: blockNumber => 10 digits,
    transactionIndex => 5 digits, logIndex => 5 digits.)

2. Secondary index for txHash -> list of primaryKeys:
   "tdh:txhash:{txHash}" => JSON([]string of primaryKeys)

3. NFT-based index to get all transfers by (contract, tokenID):
   "tdh:transferByNft:{contract}:{tokenID}:{blockNumber}:{txIndex}:{logIndex}:{txHash}" => primaryKey

4. Address-based index to get all transfers for a given address (from or to):
   "tdh:transferByAddress:{address}:{blockNumber}:{txIndex}:{logIndex}:{txHash}" => primaryKey
*/

type TransferDb interface {
	StoreTransfer(txn *badger.Txn, transfer tokens.TokenTransfer) error
	ResetToCheckpoint(txn *badger.Txn, blockNumber uint64, txIndex uint64, logIndex uint64) error
	GetAllTransfers(txn *badger.Txn) ([]tokens.TokenTransfer, error)
	GetTransfersByBlockNumber(txn *badger.Txn, blockNumber uint64) ([]tokens.TokenTransfer, error)
	GetTransfersByTxHash(txn *badger.Txn, txHash string) ([]tokens.TokenTransfer, error)
	GetTransfersByNft(txn *badger.Txn, contract, tokenID string) ([]tokens.TokenTransfer, error)
	GetTransfersByAddress(txn *badger.Txn, address string) ([]tokens.TokenTransfer, error)
}

func NewTransferDb() TransferDb {
	return &TransferDbImpl{}
}

type TransferDbImpl struct{}

const (
	transferPrefix       = "tdh:transfer:"
	txHashPrefix         = "tdh:txhash:"
	transferByNftPrefix  = "tdh:transferByNft:"
	transferByAddrPrefix = "tdh:transferByAddress:"
)

// StoreTransfer inserts a TokenTransfer into the DB with all relevant indexes.
func (t *TransferDbImpl) StoreTransfer(txn *badger.Txn, transfer tokens.TokenTransfer) error {
	//
	// 1) Primary Key
	//
	primaryKey := fmt.Sprintf("%s%010d:%05d:%05d:%s:%s:%s",
		transferPrefix,
		transfer.BlockNumber,
		transfer.TransactionIndex,
		transfer.LogIndex,
		transfer.TxHash,
		transfer.Contract,
		transfer.TokenID,
	)

	value, err := json.Marshal(transfer)
	if err != nil {
		return err
	}

	// Store the main record
	if err := txn.Set([]byte(primaryKey), value); err != nil {
		return err
	}

	//
	// 2) TX Hash -> list of primary keys
	//    "tdh:txhash:{txHash}" => JSON([]string of primaryKeys)
	//
	txHashKey := fmt.Sprintf("%s%s", txHashPrefix, transfer.TxHash)
	existingKeys := []string{}

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

	existingKeys = append(existingKeys, primaryKey)
	updatedValue, err := json.Marshal(existingKeys)
	if err != nil {
		return err
	}
	if err := txn.Set([]byte(txHashKey), updatedValue); err != nil {
		return err
	}

	//
	// 3) NFT-based index
	//    "tdh:transferByNft:{contract}:{tokenID}:{blockNumber}:{txIndex}:{logIndex}:{txHash}" => primaryKey
	//
	nftIndexKey := fmt.Sprintf("%s%s:%s:%d:%d:%d:%s",
		transferByNftPrefix,
		transfer.Contract,
		transfer.TokenID,
		transfer.BlockNumber,
		transfer.TransactionIndex,
		transfer.LogIndex,
		transfer.TxHash,
	)
	if err := txn.Set([]byte(nftIndexKey), []byte(primaryKey)); err != nil {
		return err
	}

	//
	// 4) Address-based index (for both 'from' and 'to')
	//    "tdh:transferByAddress:{address}:{blockNumber}:{txIndex}:{logIndex}:{txHash}" => primaryKey
	//
	fromAddr := transfer.From
	toAddr := transfer.To

	// Store index for `from`
	fromKey := fmt.Sprintf("%s%s:%d:%d:%d:%s",
		transferByAddrPrefix,
		fromAddr,
		transfer.BlockNumber,
		transfer.TransactionIndex,
		transfer.LogIndex,
		transfer.TxHash,
	)
	if err := txn.Set([]byte(fromKey), []byte(primaryKey)); err != nil {
		return err
	}

	// If from != to, also store index for `to`
	if fromAddr != toAddr {
		toKey := fmt.Sprintf("%s%s:%d:%d:%d:%s",
			transferByAddrPrefix,
			toAddr,
			transfer.BlockNumber,
			transfer.TransactionIndex,
			transfer.LogIndex,
			transfer.TxHash,
		)
		if err := txn.Set([]byte(toKey), []byte(primaryKey)); err != nil {
			return err
		}
	}

	return nil
}

// ResetToCheckpoint removes all transfers >= the given (blockNumber, txIndex, logIndex)
// along with their secondary index entries.
func (t *TransferDbImpl) ResetToCheckpoint(
	txn *badger.Txn,
	blockNumber uint64,
	txIndex uint64,
	logIndex uint64,
) error {
	// Construct a "start key" that represents the smallest key at or after the checkpoint.
	// Key format is now: "tdh:transfer:{blockNumber}:{txIndex}:{logIndex}:{txHash}:{contract}:{tokenID}"
	//
	// For lexical ordering, we'll zero out the fields after logIndex, leaving an empty suffix.
	//
	startKey := []byte(fmt.Sprintf("%s%010d:%05d:%05d:",
		transferPrefix,
		blockNumber,
		txIndex,
		logIndex,
	))
	

	zap.L().Info("Resetting to checkpoint",
		zap.Uint64("blockNumber", blockNumber),
		zap.Uint64("txIndex", txIndex),
		zap.Uint64("logIndex", logIndex),
		zap.String("startKey", string(startKey)),
	)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	// Iterate from 'startKey' to the end of the "tdh:transfer:" space.
	// For each key, parse the transfer, remove it from all indexes, and delete the primary record.
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()

		// If the key no longer starts with "tdh:transfer:", we've gone too far.
		if !hasPrefix(key, []byte(transferPrefix)) {
			break
		}

		// We'll parse the stored JSON to get the TokenTransfer fields
		var tr tokens.TokenTransfer
		err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &tr)
		})
		if err != nil {
			return fmt.Errorf("failed to unmarshal transfer: %w", err)
		}

		// Remove from txHash index
		txHashKey := fmt.Sprintf("%s%s", txHashPrefix, tr.TxHash)
		if err := removePrimaryKeyFromList(txn, txHashKey, string(key)); err != nil {
			return fmt.Errorf("failed to remove from txHash index: %w", err)
		}

		// Remove from NFT index
		nftIndexKey := fmt.Sprintf("%s%s:%s:%d:%d:%d:%s",
			transferByNftPrefix,
			tr.Contract,
			tr.TokenID,
			tr.BlockNumber,
			tr.TransactionIndex,
			tr.LogIndex,
			tr.TxHash,
		)
		if err := txn.Delete([]byte(nftIndexKey)); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete NFT index: %w", err)
		}

		// Remove from address-based index (from)
		fromKey := fmt.Sprintf("%s%s:%d:%d:%d:%s",
			transferByAddrPrefix,
			tr.From,
			tr.BlockNumber,
			tr.TransactionIndex,
			tr.LogIndex,
			tr.TxHash,
		)
		if err := txn.Delete([]byte(fromKey)); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete from address index: %w", err)
		}

		// Remove from address-based index (to) if different
		if tr.From != tr.To {
			toKey := fmt.Sprintf("%s%s:%d:%d:%d:%s",
				transferByAddrPrefix,
				tr.To,
				tr.BlockNumber,
				tr.TransactionIndex,
				tr.LogIndex,
				tr.TxHash,
			)
			if err := txn.Delete([]byte(toKey)); err != nil && err != badger.ErrKeyNotFound {
				return fmt.Errorf("failed to delete to address index: %w", err)
			}
		}

		// Delete the primary record
		if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete primary transfer record: %w", err)
		}

		it.Seek(key)
	}

	return nil
}

// removePrimaryKeyFromList removes `primaryKey` from the JSON array stored at txHashKey.
func removePrimaryKeyFromList(txn *badger.Txn, txHashKey, primaryKey string) error {
	item, err := txn.Get([]byte(txHashKey))
	if err == badger.ErrKeyNotFound {
		// nothing to remove
		return nil
	} else if err != nil {
		return err
	}

	var existingKeys []string
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &existingKeys)
	})
	if err != nil {
		return err
	}

	updated := make([]string, 0, len(existingKeys))
	for _, k := range existingKeys {
		if k != primaryKey {
			updated = append(updated, k)
		}
	}

	if len(updated) == 0 {
		// If no keys remain for this txHash, just delete the entire entry
		return txn.Delete([]byte(txHashKey))
	}

	updatedVal, err := json.Marshal(updated)
	if err != nil {
		return err
	}
	return txn.Set([]byte(txHashKey), updatedVal)
}

// hasPrefix is a simple helper to check if `s` starts with `prefix`.
func hasPrefix(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i := range prefix {
		if s[i] != prefix[i] {
			return false
		}
	}
	return true
}

// GetAllTransfers returns all transfers from the primary "tdh:transfer:" space.
func (t *TransferDbImpl) GetAllTransfers(txn *badger.Txn) ([]tokens.TokenTransfer, error) {
	var transfers []tokens.TokenTransfer

	prefix := []byte(transferPrefix) // "tdh:transfer:"
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
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

// GetTransfersByBlockNumber does a prefix-scan on "tdh:transfer:{blockNumber}:".
func (t *TransferDbImpl) GetTransfersByBlockNumber(txn *badger.Txn, blockNumber uint64) ([]tokens.TokenTransfer, error) {
	var transfers []tokens.TokenTransfer

	// Because the key format is now:
	//   "tdh:transfer:{blockNumber}:{txIndex}:{logIndex}:{txHash}:{contract}:{tokenID}"
	// We can still prefix-scan on "tdh:transfer:0000000010:" for blockNumber=10, for example.
	prefix := fmt.Sprintf("%s%010d:", transferPrefix, blockNumber)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
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

// GetTransfersByTxHash looks up all primary keys under "tdh:txhash:{txHash}" => JSON([]string of primary keys).
func (t *TransferDbImpl) GetTransfersByTxHash(txn *badger.Txn, txHash string) ([]tokens.TokenTransfer, error) {
	txHashKey := fmt.Sprintf("%s%s", txHashPrefix, txHash)

	item, err := txn.Get([]byte(txHashKey))
	if err == badger.ErrKeyNotFound {
		return nil, nil // no transfers for this txHash
	} else if err != nil {
		return nil, err
	}

	var primaryKeys []string
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &primaryKeys)
	})
	if err != nil {
		return nil, err
	}

	var transfers []tokens.TokenTransfer
	for _, key := range primaryKeys {
		pItem, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			// skip missing
			continue
		} else if err != nil {
			return nil, err
		}

		var transfer tokens.TokenTransfer
		err = pItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &transfer)
		})
		if err != nil {
			return nil, err
		}
		transfers = append(transfers, transfer)
	}

	return transfers, nil
}

// GetTransfersByNft scans "tdh:transferByNft:{contract}:{tokenID}:" => primaryKey => transfer object
func (t *TransferDbImpl) GetTransfersByNft(txn *badger.Txn, contract, tokenID string) ([]tokens.TokenTransfer, error) {
	var transfers []tokens.TokenTransfer

	prefix := fmt.Sprintf("%s%s:%s:", transferByNftPrefix, contract, tokenID)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()

		var primaryKey string
		err := item.Value(func(val []byte) error {
			primaryKey = string(val)
			return nil
		})
		if err != nil {
			return nil, err
		}

		pItem, err := txn.Get([]byte(primaryKey))
		if err == badger.ErrKeyNotFound {
			continue
		} else if err != nil {
			return nil, err
		}

		var transfer tokens.TokenTransfer
		err = pItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &transfer)
		})
		if err != nil {
			return nil, err
		}

		transfers = append(transfers, transfer)
	}

	return transfers, nil
}

// GetTransfersByAddress scans "tdh:transferByAddress:{address}:" => primaryKey => transfer object
func (t *TransferDbImpl) GetTransfersByAddress(txn *badger.Txn, address string) ([]tokens.TokenTransfer, error) {
	var transfers []tokens.TokenTransfer

	prefix := fmt.Sprintf("%s%s:", transferByAddrPrefix, address)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()

		var primaryKey string
		err := item.Value(func(val []byte) error {
			primaryKey = string(val)
			return nil
		})
		if err != nil {
			return nil, err
		}

		pItem, err := txn.Get([]byte(primaryKey))
		if err == badger.ErrKeyNotFound {
			continue
		} else if err != nil {
			return nil, err
		}

		var transfer tokens.TokenTransfer
		err = pItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &transfer)
		})
		if err != nil {
			return nil, err
		}

		transfers = append(transfers, transfer)
	}

	return transfers, nil
}
