package ethdb

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/6529-Collections/6529node/internal/db"
	"go.uber.org/zap"
)

type NFTDb interface {
	// Updates:
	UpdateSupply(txn *sql.Tx, contract string, tokenID string) (supply uint64, err error)
	UpdateBurntSupply(txn *sql.Tx, contract string, tokenID string) error

	// Reverse updates:
	UpdateSupplyReverse(txn *sql.Tx, contract string, tokenID string) (supply uint64, err error)
	UpdateBurntSupplyReverse(txn *sql.Tx, contract string, tokenID string) error

	GetNft(rq db.QueryRunner, contract string, tokenID string) (*NFT, error)
	db.PaginatedQuerier[NFT]
}

func NewNFTDb() NFTDb {
	return &NFTDbImpl{}
}

type NFTDbImpl struct{}

const allNftsQuery = `
	SELECT contract, token_id, supply, burnt_supply
	FROM nfts
`

// UpdateSupply increments the supply by 1. If the NFT doesn't exist, it is created with supply = 1.
func (n *NFTDbImpl) UpdateSupply(txn *sql.Tx, contract, tokenID string) (supply uint64, err error) {
	err = txn.QueryRow(`
		INSERT INTO nfts (contract, token_id, supply, burnt_supply)
		VALUES (?, ?, 1, 0)
		ON CONFLICT(contract, token_id)
		DO UPDATE SET supply = nfts.supply + 1
		RETURNING supply`, contract, tokenID).Scan(&supply)
	if err != nil {
		zap.L().Error("Failed to update supply", zap.Error(err))
		return 0, err
	}
	return supply, nil
}

// UpdateSupplyReverse decrements the supply by 1 (without deleting the NFT even if it reaches zero)
// and returns the new supply. It first ensures that the NFT exists and that supply >= 1.
func (n *NFTDbImpl) UpdateSupplyReverse(txn *sql.Tx, contract, tokenID string) (supply uint64, err error) {
	// Verify existence and fetch current supply.
	var current uint64
	err = txn.QueryRow("SELECT supply FROM nfts WHERE contract = ? AND token_id = ?", contract, tokenID).Scan(&current)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("cannot revert supply on nonexistent NFT: contract=%s tokenID=%s", contract, tokenID)
	} else if err != nil {
		return 0, err
	}

	if current < 1 {
		return 0, fmt.Errorf("cannot revert 1 from supply %d (would go negative)", current)
	}

	// Decrement supply by 1 and return the new value.
	err = txn.QueryRow(`
		UPDATE nfts
		SET supply = supply - 1
		WHERE contract = ? AND token_id = ?
		RETURNING supply`, contract, tokenID).Scan(&current)
	if err != nil {
		return 0, err
	}
	return uint64(current), nil
}

// UpdateBurntSupply increments the burnt_supply by 1. The NFT must exist.
func (n *NFTDbImpl) UpdateBurntSupply(txn *sql.Tx, contract, tokenID string) error {
	var exists bool
	err := txn.QueryRow("SELECT EXISTS(SELECT 1 FROM nfts WHERE contract = ? AND token_id = ?)", contract, tokenID).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		zap.L().Error("Cannot burn NFT that does not exist", zap.String("contract", contract), zap.String("tokenId", tokenID))
		return errors.New("cannot burn NFT that does not exist")
	}

	_, err = txn.Exec("UPDATE nfts SET burnt_supply = burnt_supply + 1 WHERE contract = ? AND token_id = ?", contract, tokenID)
	return err
}

// UpdateBurntSupplyReverse decrements the burnt_supply by 1. It first ensures that the NFT exists and that burnt_supply >= 1.
func (n *NFTDbImpl) UpdateBurntSupplyReverse(txn *sql.Tx, contract, tokenID string) error {
	var burntSupply uint64
	err := txn.QueryRow("SELECT burnt_supply FROM nfts WHERE contract = ? AND token_id = ?", contract, tokenID).Scan(&burntSupply)
	if err == sql.ErrNoRows {
		return fmt.Errorf("cannot revert burnt supply on nonexistent NFT: contract=%s tokenID=%s", contract, tokenID)
	} else if err != nil {
		return err
	}

	if burntSupply < 1 {
		return fmt.Errorf("cannot revert 1 from burnt supply %d (would go negative)", burntSupply)
	}

	_, err = txn.Exec("UPDATE nfts SET burnt_supply = burnt_supply - 1 WHERE contract = ? AND token_id = ?", contract, tokenID)
	return err
}

func newNFT() *NFT {
	return &NFT{}
}

func (n *NFTDbImpl) GetPaginatedResponseForQuery(rq db.QueryRunner, queryOptions db.QueryOptions, queryParams []interface{}) (total int, data []*NFT, err error) {
	return db.GetPaginatedResponseForQuery("nfts", rq, allNftsQuery, queryOptions, []string{"contract", "token_id"}, queryParams, newNFT)
}

func (n *NFTDbImpl) GetNft(rq db.QueryRunner, contract string, tokenID string) (*NFT, error) {
	_, data, err := n.GetPaginatedResponseForQuery(rq, db.QueryOptions{
		Where: "contract = ? AND token_id = ?",
		Page:  1,
		PageSize: 1,
	}, []interface{}{contract, tokenID})

	if err != nil {
		return nil, err
	}
	fmt.Println("i am data: ", data)
	if len(data) == 0 {
		return nil, fmt.Errorf("NFT not found: contract=%s tokenID=%s", contract, tokenID)
	}

	return data[0], nil
}