package ethdb

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"go.uber.org/zap"
)

type NFTDb interface {
	// Updates:
	UpdateSupply(txn *sql.Tx, contract string, tokenID string, delta int64) error
	UpdateBurntSupply(txn *sql.Tx, contract string, tokenID string, delta int64) error

	// Reverse updates:
	UpdateSupplyReverse(txn *sql.Tx, contract string, tokenID string, delta int64) error
	UpdateBurntSupplyReverse(txn *sql.Tx, contract string, tokenID string, delta int64) error

	// GetNft returns an NFT by contract and tokenID.
	GetNft(txn *sql.Tx, contract string, tokenID string) (*models.NFT, error)
}

func NewNFTDb() NFTDb {
	return &NFTDbImpl{}
}

type NFTDbImpl struct{}

// UpdateSupply increases the total supply when minting, creating the NFT if it doesn't exist.
func (n *NFTDbImpl) UpdateSupply(tx *sql.Tx, contract, tokenID string, delta int64) error {
	if delta < 0 {
		return errors.New("delta must be non-negative")
	}

	_, err := tx.Exec(`
		INSERT INTO nfts (contract, token_id, supply, burnt_supply) 
		VALUES (?, ?, ?, 0)
		ON CONFLICT(contract, token_id) 
		DO UPDATE SET supply = supply + ?`, contract, tokenID, delta, delta)

	if err != nil {
		zap.L().Error("Failed to update supply", zap.Error(err))
	}
	return err
}

// UpdateSupplyReverse decreases the supply but does NOT delete the NFT even if it reaches zero.
func (n *NFTDbImpl) UpdateSupplyReverse(tx *sql.Tx, contract, tokenID string, delta int64) error {
	if delta <= 0 {
		return errors.New("delta must be positive for reverse update")
	}

	// Ensure the NFT exists before updating
	var supply int64
	err := tx.QueryRow("SELECT supply FROM nfts WHERE contract = ? AND token_id = ?", contract, tokenID).Scan(&supply)
	if err == sql.ErrNoRows {
		return fmt.Errorf("cannot revert supply on nonexistent NFT: contract=%s tokenID=%s", contract, tokenID)
	} else if err != nil {
		return err
	}

	if supply < delta {
		return fmt.Errorf("cannot revert %d from supply %d (would go negative)", delta, supply)
	}

	_, err = tx.Exec("UPDATE nfts SET supply = supply - ? WHERE contract = ? AND token_id = ?", delta, contract, tokenID)
	return err
}

// UpdateBurntSupply increases the burnt supply; the NFT must exist.
func (n *NFTDbImpl) UpdateBurntSupply(tx *sql.Tx, contract, tokenID string, delta int64) error {
	if delta <= 0 {
		return errors.New("delta must be positive for burning supply")
	}

	// Ensure the NFT exists before updating
	var exists bool
	err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM nfts WHERE contract = ? AND token_id = ?)", contract, tokenID).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		zap.L().Error("Cannot burn NFT that does not exist", zap.String("contract", contract), zap.String("tokenId", tokenID))
		return errors.New("cannot burn NFT that does not exist")
	}

	_, err = tx.Exec("UPDATE nfts SET burnt_supply = burnt_supply + ? WHERE contract = ? AND token_id = ?", delta, contract, tokenID)
	return err
}

// UpdateBurntSupplyReverse decreases burnt supply.
func (n *NFTDbImpl) UpdateBurntSupplyReverse(tx *sql.Tx, contract, tokenID string, delta int64) error {
	if delta <= 0 {
		return errors.New("delta must be positive for reverse burnt supply update")
	}

	var burntSupply int64
	err := tx.QueryRow("SELECT burnt_supply FROM nfts WHERE contract = ? AND token_id = ?", contract, tokenID).Scan(&burntSupply)
	if err == sql.ErrNoRows {
		return fmt.Errorf("cannot revert burnt supply on nonexistent NFT contract=%s tokenID=%s", contract, tokenID)
	} else if err != nil {
		return err
	}

	if burntSupply < delta {
		return fmt.Errorf("cannot revert %d from burnt supply %d (would go negative)", delta, burntSupply)
	}

	_, err = tx.Exec("UPDATE nfts SET burnt_supply = burnt_supply - ? WHERE contract = ? AND token_id = ?", delta, contract, tokenID)
	return err
}

// GetNft returns an NFT by contract and tokenID.
func (n *NFTDbImpl) GetNft(tx *sql.Tx, contract string, tokenID string) (*models.NFT, error) {
	var nft models.NFT
	err := tx.QueryRow("SELECT supply, burnt_supply FROM nfts WHERE contract = ? AND token_id = ?", contract, tokenID).Scan(&nft.Supply, &nft.BurntSupply)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("NFT not found: contract=%s tokenID=%s", contract, tokenID)
	}

	return &nft, err
}
