package eth

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/dgraph-io/badger/v4"
)

/*
DB Indexes Created Here:

1. "tdh:owner:{owner}:{contract}:{tokenID}" => balance
   - Allows scanning all NFTs for a given owner with a prefix "tdh:owner:{owner}:"

2. "tdh:nftowners:{contract}:{tokenID}:{owner}" => balance
   - Allows scanning all owners for a given NFT with a prefix "tdh:nftowners:{contract}:{tokenID}:"
*/

type OwnerDb interface {
    UpdateOwnership(txn *badger.Txn, from, to, contract, tokenID string, amount int64) error
    GetBalance(txn *badger.Txn, owner, contract, tokenID string) (int64, error)
    GetOwnersByNft(txn *badger.Txn, contract, tokenID string) (map[string]int64, error)
}

func NewOwnerDb() OwnerDb {
    return &OwnerDbImpl{}
}

type OwnerDbImpl struct{}

const ownerPrefix = "tdh:owner:"
const nftOwnersPrefix = "tdh:nftowners:"

func (o *OwnerDbImpl) UpdateOwnership(txn *badger.Txn, from, to, contract, tokenID string, amount int64) error {
    // Deduct from sender
    if from != constants.NULL_ADDRESS {
        fromBalance, err := o.GetBalance(txn, from, contract, tokenID)
        if err != nil || fromBalance < amount {
            return errors.New("transfer error: insufficient balance")
        }

        newFromBalance := fromBalance - amount
        if newFromBalance == 0 {
            // Remove the key if balance is zero
            err = txn.Delete([]byte(fmt.Sprintf("%s%s:%s:%s", ownerPrefix, from, contract, tokenID)))
            if err != nil {
                return err
            }
            // Also remove from NFT->owners index
            err = txn.Delete([]byte(fmt.Sprintf("%s%s:%s:%s", nftOwnersPrefix, contract, tokenID, from)))
            if err != nil {
                return err
            }
        } else {
            // Update the balance
            err = o.setBalance(txn, from, contract, tokenID, newFromBalance)
            if err != nil {
                return err
            }
        }
    }

    // Add to receiver
    toBalance, err := o.GetBalance(txn, to, contract, tokenID)
    if err == badger.ErrKeyNotFound {
        toBalance = 0 // Default to 0 if not found
    } else if err != nil {
        return err
    }

    newToBalance := toBalance + amount
    err = o.setBalance(txn, to, contract, tokenID, newToBalance)
    if err != nil {
        return err
    }

    return nil
}

func (o *OwnerDbImpl) GetBalance(txn *badger.Txn, owner, contract, tokenID string) (int64, error) {
    // Generate key
    key := fmt.Sprintf("%s%s:%s:%s", ownerPrefix, owner, contract, tokenID)

    // Try retrieving the balance
    item, err := txn.Get([]byte(key))
    if err == badger.ErrKeyNotFound {
        return 0, nil // Default to 0 if key is not found
    } else if err != nil {
        return 0, err
    }

    var balance int64
    err = item.Value(func(val []byte) error {
        return json.Unmarshal(val, &balance)
    })
    if err != nil {
        return 0, err
    }

    return balance, nil
}

// setBalance updates both the 'owner -> NFT' key and the 'NFT -> owners' key.
func (o *OwnerDbImpl) setBalance(txn *badger.Txn, owner, contract, tokenID string, amount int64) error {
    // 1) Owner-based key
    ownerKey := fmt.Sprintf("%s%s:%s:%s", ownerPrefix, owner, contract, tokenID)

    // 2) NFT-based key
    nftOwnerKey := fmt.Sprintf("%s%s:%s:%s", nftOwnersPrefix, contract, tokenID, owner)

    value, err := json.Marshal(amount)
    if err != nil {
        return err
    }

    // Update "owner -> NFT" index
    if err := txn.Set([]byte(ownerKey), value); err != nil {
        return err
    }
    // Update "NFT -> owners" index
    if err := txn.Set([]byte(nftOwnerKey), value); err != nil {
        return err
    }

    return nil
}

// GetOwnersByNft scans the 'tdh:nftowners:{contract}:{tokenID}:' prefix
// to return all owners and their balances.
func (o *OwnerDbImpl) GetOwnersByNft(txn *badger.Txn, contract, tokenID string) (map[string]int64, error) {
    owners := make(map[string]int64)

    prefix := []byte(fmt.Sprintf("%s%s:%s:", nftOwnersPrefix, contract, tokenID))

    opts := badger.DefaultIteratorOptions
    it := txn.NewIterator(opts)
    defer it.Close()

    for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
        item := it.Item()
        keyBytes := item.Key()
        // key format: "tdh:nftowners:{contract}:{tokenID}:{owner}"

        var balance int64
        err := item.Value(func(val []byte) error {
            return json.Unmarshal(val, &balance)
        })
        if err != nil {
            return nil, err
        }

        // The part after prefix is the {owner}
        owner := string(keyBytes[len(prefix):])
        owners[owner] = balance
    }

    return owners, nil
}
