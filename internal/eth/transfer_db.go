package eth

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
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

	GetTransfersAfterCheckpoint(txn *badger.Txn, blockNumber uint64, txIndex uint64, logIndex uint64) ([]tokens.TokenTransfer, error)
	DeleteTransfersAfterCheckpoint(txn *badger.Txn, blockNumber uint64, txIndex uint64, logIndex uint64) error

	GetAllTransfers(txn *badger.Txn) ([]tokens.TokenTransfer, error)
	GetTransfersByBlockNumber(txn *badger.Txn, blockNumber uint64) ([]tokens.TokenTransfer, error)
	GetTransfersByTxHash(txn *badger.Txn, txHash string) ([]tokens.TokenTransfer, error)
	GetTransfersByNft(txn *badger.Txn, contract, tokenID string) ([]tokens.TokenTransfer, error)
	GetTransfersByAddress(txn *badger.Txn, address string) ([]tokens.TokenTransfer, error)
	GetTransfersByContract(txn *badger.Txn, contract string) ([]tokens.TokenTransfer, error)
	GetTransfersByBlockMax(txn *badger.Txn, blockNumber uint64) ([]tokens.TokenTransfer, error)
	GetLatestTransfer(txn *badger.Txn) (*tokens.TokenTransfer, error)
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

func (t *TransferDbImpl) GetTransfersAfterCheckpoint(
	txn *badger.Txn,
	blockNumber uint64,
	txIndex uint64,
	logIndex uint64,
) ([]tokens.TokenTransfer, error) {
	// We'll create a startKey that includes the zero-padded blockNumber, txIndex, and logIndex,
	// followed by a colon, so we pick up all keys >= that point.
	// Key format: "tdh:transfer:0000000000:00000:00000:..."
	startKey := []byte(fmt.Sprintf("%s%010d:%05d:%05d:",
		transferPrefix,
		blockNumber,
		txIndex,
		logIndex,
	))

	var results []tokens.TokenTransfer

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Item().Key()

		// Stop if we've moved outside the "tdh:transfer:" space
		if !hasPrefix(key, []byte(transferPrefix)) {
			break
		}

		var tr tokens.TokenTransfer
		if err := it.Item().Value(func(val []byte) error {
			return json.Unmarshal(val, &tr)
		}); err != nil {
			return nil, fmt.Errorf("failed to unmarshal transfer: %w", err)
		}

		// This transfer is at or after the checkpoint in lexical ordering
		results = append(results, tr)
	}

	return results, nil
}

func (t *TransferDbImpl) DeleteTransfersAfterCheckpoint(
	txn *badger.Txn,
	blockNumber uint64,
	txIndex uint64,
	logIndex uint64,
) error {
	// same startKey approach
	startKey := []byte(fmt.Sprintf("%s%010d:%05d:%05d:",
		transferPrefix,
		blockNumber,
		txIndex,
		logIndex,
	))

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Item().Key()

		// If the key no longer starts with "tdh:transfer:", we've gone beyond the range
		if !hasPrefix(key, []byte(transferPrefix)) {
			break
		}

		// Parse the transfer data
		var tr tokens.TokenTransfer
		err := it.Item().Value(func(val []byte) error {
			return json.Unmarshal(val, &tr)
		})
		if err != nil {
			return fmt.Errorf("failed to unmarshal transfer: %w", err)
		}

		// 1) Remove from txHash -> [primaryKeys] index
		txHashKey := fmt.Sprintf("%s%s", txHashPrefix, tr.TxHash)
		if err := removePrimaryKeyFromList(txn, txHashKey, string(key)); err != nil {
			return fmt.Errorf("failed to remove from txHash index: %w", err)
		}

		// 2) Remove from the NFT-based index
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
			return fmt.Errorf("failed to delete NFT index key: %w", err)
		}

		// 3) Remove from address-based index (from)
		fromKey := fmt.Sprintf("%s%s:%d:%d:%d:%s",
			transferByAddrPrefix,
			tr.From,
			tr.BlockNumber,
			tr.TransactionIndex,
			tr.LogIndex,
			tr.TxHash,
		)
		if err := txn.Delete([]byte(fromKey)); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete 'from' address index: %w", err)
		}
		// If from != to, remove the 'to' entry as well
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
				return fmt.Errorf("failed to delete 'to' address index: %w", err)
			}
		}

		// 4) Finally remove the primary record
		if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete primary record: %w", err)
		}

		// Because we've just deleted the current item, we must reposition iteration to its next key:
		//   it.Seek(key)
		// This ensures the iterator won't skip anything. Alternatively, we could store `it.Item().KeyCopy()`
		// before deleting, but this approach is simpler if the data set isn't huge.
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

func (t *TransferDbImpl) GetTransfersByContract(txn *badger.Txn, contract string) ([]tokens.TokenTransfer, error) {
	var transfers []tokens.TokenTransfer

	// We already have an NFT-based index: "tdh:transferByNft:{contract}:{tokenID}:{blockNumber}:..."
	// To get everything for the entire contract, scan on just "tdh:transferByNft:{contract}:".
	prefix := fmt.Sprintf("%s%s:", transferByNftPrefix, contract) // "tdh:transferByNft:0xABC..."

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()

		// The value stored at "tdh:transferByNft:..." is the primaryKey of the main record.
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
			// Skip missing primary records
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

func (t *TransferDbImpl) GetTransfersByBlockMax(txn *badger.Txn, blockNumber uint64) ([]tokens.TokenTransfer, error) {
	var transfers []tokens.TokenTransfer

	// We'll iterate over the entire "tdh:transfer:" namespace, but stop once the blockNumber in the key
	// exceeds the requested blockNumber. Because we zero-pad blockNumber to 10 digits, lexical ordering
	// by key lines up with numerical ordering.
	prefix := []byte(transferPrefix) // "tdh:transfer:"

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		// key format: "tdh:transfer:0000000012:00000:00000:..." etc.
		// extract the 10-digit block number substring
		blockStr := string(key[len(prefix) : len(prefix)+10])
		bn, err := strconv.ParseUint(blockStr, 10, 64)
		if err != nil {
			// If for some reason we fail to parse, treat it as an error
			return nil, err
		}

		if bn > blockNumber {
			// Once the blockNumber in the key is greater than the requested max,
			// we can stop scanning altogether
			break
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

func (t *TransferDbImpl) GetLatestTransfer(txn *badger.Txn) (*tokens.TokenTransfer, error) {
	var transfer tokens.TokenTransfer
	prefix := []byte(transferPrefix)

	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.Reverse = true
	itr := txn.NewIterator(opts)
	defer itr.Close()

	itr.Rewind()
	if !itr.Valid() {
		return nil, nil
	}

	item := itr.Item()
	err := item.Value(func(val []byte) error {
		return json.Unmarshal(val, &transfer)
	})
	if err != nil {
		return nil, err
	}

	return &transfer, nil
}
