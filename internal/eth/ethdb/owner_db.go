package ethdb

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/constants"
)

type OwnerDb interface {
	// Forward direction: from -> to
	UpdateOwnership(txn *sql.Tx, from, to, contract, tokenID string, amount int64) error
	// Reverse direction: undo a previous from->to by doing to->from
	UpdateOwnershipReverse(txn *sql.Tx, from, to, contract, tokenID string, amount int64) error

	// GetBalance retrieves the balance of an owner for a specific NFT.
	GetBalance(txn *sql.Tx, owner string, contract string, tokenID string) (int64, error)
}

func NewOwnerDb() OwnerDb {
	return &OwnerDbImpl{}
}

type OwnerDbImpl struct{}

// UpdateOwnership processes a forward transfer (from -> to).
func (o *OwnerDbImpl) UpdateOwnership(tx *sql.Tx, from, to, contract, tokenID string, amount int64) error {
	if amount <= 0 {
		return errors.New("transfer error: amount must be positive")
	}

	// Deduct from sender
	if from != constants.NULL_ADDRESS {
		fromBalance, err := o.GetBalance(tx, from, contract, tokenID)
		if err != nil {
			return err
		}
		if fromBalance < amount {
			return fmt.Errorf("transfer error: insufficient balance, from=%s, contract=%s, tokenID=%s", from, contract, tokenID)
		}

		newFromBalance := fromBalance - amount
		if newFromBalance == 0 {
			_, err = tx.Exec("DELETE FROM nft_owners WHERE contract = ? AND token_id = ? AND owner = ?", contract, tokenID, from)
		} else {
			_, err = tx.Exec("UPDATE nft_owners SET balance = ? WHERE contract = ? AND token_id = ? AND owner = ?", newFromBalance, contract, tokenID, from)
		}
		if err != nil {
			return err
		}
	}

	// Add to receiver
	if to != constants.NULL_ADDRESS {
		toBalance, err := o.GetBalance(tx, to, contract, tokenID)
		if err != nil {
			return err
		}
		newToBalance := toBalance + amount
		_, err = tx.Exec("INSERT INTO nft_owners (contract, token_id, owner, balance) VALUES (?, ?, ?, ?) ON CONFLICT(contract, token_id, owner) DO UPDATE SET balance = balance + ?", contract, tokenID, to, newToBalance, amount)
		return err
	}

	return nil
}

// UpdateOwnershipReverse undoes a previous transfer (to -> from).
func (o *OwnerDbImpl) UpdateOwnershipReverse(tx *sql.Tx, from, to, contract, tokenID string, amount int64) error {
	if amount <= 0 {
		return errors.New("reverse transfer error: amount must be positive")
	}

	// Deduct from 'to'
	if to != constants.NULL_ADDRESS {
		toBalance, err := o.GetBalance(tx, to, contract, tokenID)
		if err != nil {
			return err
		}
		if toBalance < amount {
			return fmt.Errorf("reverse transfer error: insufficient balance at 'to' address, to=%s, contract=%s, tokenID=%s", to, contract, tokenID)
		}

		newToBalance := toBalance - amount
		if newToBalance == 0 {
			_, err = tx.Exec("DELETE FROM nft_owners WHERE contract = ? AND token_id = ? AND owner = ?", contract, tokenID, to)
		} else {
			_, err = tx.Exec("UPDATE nft_owners SET balance = ? WHERE contract = ? AND token_id = ? AND owner = ?", newToBalance, contract, tokenID, to)
		}
		if err != nil {
			return err
		}
	}

	// Give back to 'from'
	if from != constants.NULL_ADDRESS {
		fromBalance, err := o.GetBalance(tx, from, contract, tokenID)
		if err != nil {
			return err
		}
		newFromBalance := fromBalance + amount
		_, err = tx.Exec("INSERT INTO nft_owners (contract, token_id, owner, balance) VALUES (?, ?, ?, ?) ON CONFLICT(contract, token_id, owner) DO UPDATE SET balance = balance + ?", contract, tokenID, from, newFromBalance, amount)
		return err
	}

	return nil
}

// GetBalance retrieves the balance of an owner for a specific NFT.
func (o *OwnerDbImpl) GetBalance(tx *sql.Tx, owner, contract, tokenID string) (int64, error) {
	var balance int64
	err := tx.QueryRow("SELECT balance FROM nft_owners WHERE contract = ? AND token_id = ? AND owner = ?", contract, tokenID, owner).Scan(&balance)

	if err == sql.ErrNoRows {
		return 0, nil // Owner has no balance for this NFT
	}
	if err != nil {
		return 0, err
	}

	return balance, nil
}
