package eth

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/dgraph-io/badger/v4"
)

type OwnerDb interface {
	UpdateOwnership(txn *badger.Txn, from, to, contract, tokenID string, amount int64) error
	GetBalance(txn *badger.Txn, owner, contract, tokenID string) (int64, error)
}

func NewOwnerDb() OwnerDb {
	return &OwnerDbImpl{}
}

type OwnerDbImpl struct {
}

const ownerPrefix = "tdh:owner:"

func (o *OwnerDbImpl) UpdateOwnership(txn *badger.Txn, from, to, contract, tokenID string, amount int64) error {
	// Deduct from sender
	if from != constants.NULL_ADDRESS {
		fromBalance, err := o.GetBalance(txn, from, contract, tokenID)
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
	toBalance, err := o.GetBalance(txn, to, contract, tokenID)
	if err == badger.ErrKeyNotFound {
		toBalance = 0 // Default to 0 if not found
	} else if err != nil {
		return err
	}

	return o.setBalance(txn, to, contract, tokenID, toBalance+amount)
}

func (o *OwnerDbImpl) GetBalance(txn *badger.Txn, owner, contract, tokenID string) (int64, error) {
	// Generate key
	key := fmt.Sprintf("%s%s:%s:%s", ownerPrefix, owner, contract, tokenID)

	// Try retrieving the balance
	item, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		return 0, nil // Default to 0 balance if key is not found
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

func (o *OwnerDbImpl) setBalance(txn *badger.Txn, owner, contract, tokenID string, amount int64) error {
	key := fmt.Sprintf("%s%s:%s:%s", ownerPrefix, owner, contract, tokenID)
	value, err := json.Marshal(amount)
	if err != nil {
		return err
	}

	return txn.Set([]byte(key), value)
}