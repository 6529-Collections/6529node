package eth

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/dgraph-io/badger/v4"
)

type OwnerDb interface {
	UpdateOwnership(from, to, contract, tokenID string, amount int64) error
	GetBalance(owner, contract, tokenID string) (int64, error)
}

func NewOwnerDb(db *badger.DB) OwnerDb {
	return &OwnerDbImpl{db: db}
}

type OwnerDbImpl struct {
	mu sync.RWMutex
	db *badger.DB
}

const ownerPrefix = "tdh:owner:"

func (o *OwnerDbImpl) UpdateOwnership(from, to, contract, tokenID string, amount int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.db.Update(func(txn *badger.Txn) error {
		// Deduct from sender
		if from != constants.NULL_ADDRESS {
			fromBalance, err := o.GetBalance(from, contract, tokenID)
			if err != nil || fromBalance < amount {
				return errors.New("transfer error: insufficient balance")
			}

			if fromBalance-amount == 0 {
				// Remove the key if balance is zero
				err = txn.Delete([]byte(fmt.Sprintf("%s%s:%s:%s", ownerPrefix, from, contract, tokenID)))
			} else {
				// Update the balance
				err = o.setBalance(txn, from, contract, tokenID, fromBalance-amount)
			}

			if err != nil {
				return err
			}
		}

		// Add to receiver
		toBalance, err := o.GetBalance(to, contract, tokenID)
		if err == badger.ErrKeyNotFound {
			toBalance = 0 // Default to 0 if not found
		} else if err != nil {
			return err
		}

		return o.setBalance(txn, to, contract, tokenID, toBalance+amount)
	})
}

func (o *OwnerDbImpl) GetBalance(owner, contract, tokenID string) (int64, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var balance int64
	err := o.db.View(func(txn *badger.Txn) error {
		key := fmt.Sprintf("%s%s:%s:%s", ownerPrefix, owner, contract, tokenID)
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &balance)
		})
	})

	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	return balance, err
}

func (o *OwnerDbImpl) setBalance(txn *badger.Txn, owner, contract, tokenID string, amount int64) error {
	key := fmt.Sprintf("%s%s:%s:%s", ownerPrefix, owner, contract, tokenID)
	value, err := json.Marshal(amount)
	if err != nil {
		return err
	}

	return txn.Set([]byte(key), value)
}