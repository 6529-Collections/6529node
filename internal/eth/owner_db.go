package eth

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

/*
DB Indexes Created Here:

1. "tdh:owner:{owner}:{contract}:{tokenID}" => balance
   - Allows scanning all NFTs for a given owner with a prefix "tdh:owner:{owner}:"

2. "tdh:nftowners:{contract}:{tokenID}:{owner}" => balance
   - Allows scanning all owners for a given NFT with a prefix "tdh:nftowners:{contract}:{tokenID}:"
*/

type OwnerDb interface {
	// Forward direction: from -> to
	UpdateOwnership(txn *badger.Txn, from, to, contract, tokenID string, amount int64) error

	// Reverse direction: undo a previous from->to by doing to->from
	UpdateOwnershipReverse(txn *badger.Txn, from, to, contract, tokenID string, amount int64) error

	GetBalance(txn *badger.Txn, owner, contract, tokenID string) (int64, error)
	GetOwnersByNft(txn *badger.Txn, contract, tokenID string) (map[string]int64, error)
	GetAllOwners(txn *badger.Txn) (map[string]int64, error)
}

func NewOwnerDb() OwnerDb {
	return &OwnerDbImpl{}
}

type OwnerDbImpl struct{}

const ownerPrefix = "tdh:owner:"
const nftOwnersPrefix = "tdh:nftowners:"

// UpdateOwnership handles the normal forward transfer: from -> to.
func (o *OwnerDbImpl) UpdateOwnership(txn *badger.Txn, from, to, contract, tokenID string, amount int64) error {
	if amount <= 0 {
		return errors.New("transfer error: amount must be positive")
	}

	padded := PaddedContractTokenID(contract, tokenID)

	// Deduct from sender
	if from != constants.NULL_ADDRESS {
		fromBalance, err := o.GetBalance(txn, from, contract, tokenID)
		if err != nil {
			return err
		}
		if fromBalance < amount {
			return errors.New("transfer error: insufficient balance")
		}

		newFromBalance := fromBalance - amount
		if newFromBalance == 0 {
			// Remove the key if balance is zero
			err = txn.Delete([]byte(fmt.Sprintf("%s%s:%s", ownerPrefix, from, padded)))
			if err != nil {
				return err
			}
			// Also remove from NFT->owners index
			err = txn.Delete([]byte(fmt.Sprintf("%s%s:%s", nftOwnersPrefix, padded, from)))
			if err != nil {
				return err
			}
		} else {
			// Update the balance
			err = o.setBalance(txn, from, padded, newFromBalance)
			if err != nil {
				return err
			}
		}
	}

	// Add to receiver
	if to != constants.NULL_ADDRESS {
		toBalance, err := o.GetBalance(txn, to, contract, tokenID)
		if err != nil {
			return err
		}
		newToBalance := toBalance + amount
		return o.setBalance(txn, to, padded, newToBalance)
	}

	// If `to` is the null address, we effectively "burn" them (no balance to maintain).
	return nil
}

// UpdateOwnershipReverse undoes a previous transfer from->to by doing to->from.
func (o *OwnerDbImpl) UpdateOwnershipReverse(txn *badger.Txn, from, to, contract, tokenID string, amount int64) error {
	if amount <= 0 {
		return errors.New("reverse transfer error: amount must be positive")
	}

	padded := PaddedContractTokenID(contract, tokenID)

	// In a forward scenario, 'from->to' was done.
	// So reversing means we do 'to->from' by removing tokens from 'to' and adding to 'from'.

	// Deduct from 'to'
	if to != constants.NULL_ADDRESS {
		toBalance, err := o.GetBalance(txn, to, contract, tokenID)
		if err != nil {
			return err
		}
		if toBalance < amount {
			zap.L().Error("hi i am reverse transfer error: insufficient balance at 'to' address", zap.String("to", to), zap.String("contract", contract), zap.String("tokenID", tokenID), zap.Int64("amount", amount), zap.Int64("toBalance", toBalance))
			return errors.New("reverse transfer error: insufficient balance at 'to' address")
		}

		newToBalance := toBalance - amount
		if newToBalance == 0 {
			// Remove the key if balance is zero
			err = txn.Delete([]byte(fmt.Sprintf("%s%s:%s", ownerPrefix, to, padded)))
			if err != nil {
				return err
			}
			// Also remove from NFT->owners index
			err = txn.Delete([]byte(fmt.Sprintf("%s%s:%s", nftOwnersPrefix, padded, to)))
			if err != nil {
				return err
			}
		} else {
			// Update the balance
			if err := o.setBalance(txn, to, padded, newToBalance); err != nil {
				return err
			}
		}
	}

	// Give back to 'from'
	if from != constants.NULL_ADDRESS {
		fromBalance, err := o.GetBalance(txn, from, contract, tokenID)
		if err != nil {
			return err
		}
		newFromBalance := fromBalance + amount
		if err := o.setBalance(txn, from, padded, newFromBalance); err != nil {
			return err
		}
	}

	// If 'from' is null, it effectively "un-mints" them (burn).
	return nil
}

// GetBalance returns the balance for owner->(contract,tokenID).
func (o *OwnerDbImpl) GetBalance(txn *badger.Txn, owner, contract, tokenID string) (int64, error) {
	padded := PaddedContractTokenID(contract, tokenID)
	key := fmt.Sprintf("%s%s:%s", ownerPrefix, owner, padded)
	item, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		return 0, nil
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

// setBalance updates both the 'owner->NFT' key and the 'NFT->owners' key.
func (o *OwnerDbImpl) setBalance(txn *badger.Txn, owner, padded string, amount int64) error {
	// 1) Owner-based key
	ownerKey := fmt.Sprintf("%s%s:%s", ownerPrefix, owner, padded)
	// 2) NFT-based key
	nftOwnerKey := fmt.Sprintf("%s%s:%s", nftOwnersPrefix, padded, owner)

	value, err := json.Marshal(amount)
	if err != nil {
		return err
	}

	if err := txn.Set([]byte(ownerKey), value); err != nil {
		return err
	}
	if err := txn.Set([]byte(nftOwnerKey), value); err != nil {
		return err
	}

	return nil
}

// GetOwnersByNft scans the 'tdh:nftowners:{contract}:{tokenID}:' prefix
// to find all owners of a given NFT.
func (o *OwnerDbImpl) GetOwnersByNft(txn *badger.Txn, contract, tokenID string) (map[string]int64, error) {
	padded := PaddedContractTokenID(contract, tokenID)
	prefix := []byte(fmt.Sprintf("%s%s:", nftOwnersPrefix, padded))

	owners := make(map[string]int64)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		keyBytes := item.Key()
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

// GetAllOwners enumerates all "tdh:owner:" records, returning a map of "owner:contract:tokenID" => balance
func (o *OwnerDbImpl) GetAllOwners(txn *badger.Txn) (map[string]int64, error) {
	owners := make(map[string]int64)
	prefix := []byte(ownerPrefix)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		keyBytes := item.Key()

		var balance int64
		err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &balance)
		})
		if err != nil {
			return nil, err
		}

		// Key portion after "tdh:owner:"
		suffix := string(keyBytes[len(prefix):]) // e.g. "0xAlice:contractX:tokenFoo"
		owners[suffix] = balance
	}

	return owners, nil
}
