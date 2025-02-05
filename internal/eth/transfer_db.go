package eth

import (
	"encoding/json"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/tdh/tokens"
	"github.com/dgraph-io/badger/v4"
)

/*
DB Indexes Created Here:

1. Primary transfer record:
   "tdh:transfer:{blockNumber}:{txHash}:{txIndex}:{logIndex}" => JSON(TokenTransfer)

2. Secondary index for txHash -> list of primaryKeys:
   "tdh:txhash:{txHash}" => JSON([]string of primaryKeys)

3. NFT-based index to get all transfers by (contract,tokenID):
   "tdh:transferByNft:{contract}:{tokenID}:{blockNumber}:{txIndex}:{logIndex}" => primaryKey

4. NEW: Address-based index to get all transfers for a given address (from or to):
   "tdh:transferByAddress:{address}:{blockNumber}:{txHash}:{txIndex}:{logIndex}" => primaryKey
*/

type TransferDb interface {
    StoreTransfer(txn *badger.Txn, transfer tokens.TokenTransfer) error
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

func (t *TransferDbImpl) StoreTransfer(txn *badger.Txn, transfer tokens.TokenTransfer) error {
    //
    // 1) Primary Key
    //
    primaryKey := fmt.Sprintf("%s%d:%s:%d:%d",
        transferPrefix,
        transfer.BlockNumber,
        transfer.TxHash,
        transfer.TransactionIndex,
        transfer.LogIndex,
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
    //
    nftIndexKey := fmt.Sprintf("%s%s:%s:%d:%d:%d",
        transferByNftPrefix,
        transfer.Contract,
        transfer.TokenID,
        transfer.BlockNumber,
        transfer.TransactionIndex,
        transfer.LogIndex,
    )
    if err := txn.Set([]byte(nftIndexKey), []byte(primaryKey)); err != nil {
        return err
    }

    //
    // 4) Address-based index (for both 'from' and 'to')
    //
    // We'll create:
    //    "tdh:transferByAddress:{address}:{blockNumber}:{txHash}:{txIndex}:{logIndex}" => primaryKey
    // We store once for `from`, once for `to`, unless from == to (avoid duplicate).
    fromAddr := transfer.From
    toAddr := transfer.To

    // Store index for from
    fromKey := fmt.Sprintf("%s%s:%d:%s:%d:%d",
        transferByAddrPrefix,
        fromAddr,
        transfer.BlockNumber,
        transfer.TxHash,
        transfer.TransactionIndex,
        transfer.LogIndex,
    )
    if err := txn.Set([]byte(fromKey), []byte(primaryKey)); err != nil {
        return err
    }

    // If from != to, also store index for to
    if fromAddr != toAddr {
        toKey := fmt.Sprintf("%s%s:%d:%s:%d:%d",
            transferByAddrPrefix,
            toAddr,
            transfer.BlockNumber,
            transfer.TxHash,
            transfer.TransactionIndex,
            transfer.LogIndex,
        )
        if err := txn.Set([]byte(toKey), []byte(primaryKey)); err != nil {
            return err
        }
    }

    return nil
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

    txHashKey := fmt.Sprintf("%s%s", txHashPrefix, txHash)
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

    var transfers []tokens.TokenTransfer
    for _, key := range transferKeys {
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

func (t *TransferDbImpl) GetTransfersByNft(txn *badger.Txn, contract, tokenID string) ([]tokens.TokenTransfer, error) {
    var transfers []tokens.TokenTransfer

    prefix := fmt.Sprintf("%s%s:%s:", transferByNftPrefix, contract, tokenID)
    opts := badger.DefaultIteratorOptions
    it := txn.NewIterator(opts)
    defer it.Close()

    for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
        item := it.Item()

        // The value stored is the primary key (e.g. "tdh:transfer:123:0xTxHash:...")
        var primaryKey string
        err := item.Value(func(val []byte) error {
            primaryKey = string(val)
            return nil
        })
        if err != nil {
            return nil, err
        }

        // Now retrieve the actual transfer object from the primary key
        pItem, err := txn.Get([]byte(primaryKey))
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

// NEW: Retrieve all transfers for a given address (both from and to).
// This will do a prefix scan on "tdh:transferByAddress:{address}:" to find all relevant transfers.
func (t *TransferDbImpl) GetTransfersByAddress(txn *badger.Txn, address string) ([]tokens.TokenTransfer, error) {
    var transfers []tokens.TokenTransfer

    // Key prefix: "tdh:transferByAddress:{address}:"
    prefix := fmt.Sprintf("%s%s:", transferByAddrPrefix, address)

    opts := badger.DefaultIteratorOptions
    it := txn.NewIterator(opts)
    defer it.Close()

    for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
        item := it.Item()

        // The value is the primaryKey for the actual transfer
        var primaryKey string
        err := item.Value(func(val []byte) error {
            primaryKey = string(val)
            return nil
        })
        if err != nil {
            return nil, err
        }

        // Retrieve the actual TokenTransfer from the primary key
        pItem, err := txn.Get([]byte(primaryKey))
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
