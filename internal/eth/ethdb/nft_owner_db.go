package ethdb

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
)

type NFTOwnerDb interface {
	GetUniqueID(txn *sql.Tx, contract, tokenID string, address string) (uint64, error)

	// Forward direction: from -> to
	UpdateOwnership(txn *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error

	// Reverse direction: undo a previous from->to by doing to->from
	UpdateOwnershipReverse(txn *sql.Tx, transfer NFTTransfer, tokenUniqueID uint64) error

	db.PaginatedQuerier[NFTOwner]
}

func NewOwnerDb() NFTOwnerDb {
	return &OwnerDbImpl{}
}

type OwnerDbImpl struct{}

const allOwnersQuery = `
	SELECT owner, contract, token_id, token_unique_id, timestamp
	FROM nft_owners
`

// GetUniqueID retrieves the unique ID of an NFT for a specific address.
func (o *OwnerDbImpl) GetUniqueID(txn *sql.Tx, contract, tokenID string, address string) (uint64, error) {
	var uniqueID uint64
	err := txn.QueryRow("SELECT token_unique_id FROM nft_owners WHERE contract = ? AND token_id = ? AND owner = ? ORDER BY timestamp DESC, token_unique_id DESC LIMIT 1", contract, tokenID, address).Scan(&uniqueID)
	return uniqueID, err
}

// UpdateOwnership processes a forward transfer (from -> to).
func (o *OwnerDbImpl) UpdateOwnership(tx *sql.Tx, transfer models.TokenTransfer, tokenUniqueID uint64) error {
	if transfer.Type != models.MINT && transfer.Type != models.AIRDROP {
		// Check current ownership (select nft_owners where contract, token_id, token_unique_id = ? and check owner = from)
		var currentOwner string
		currentOwnerErr := tx.QueryRow("SELECT owner FROM nft_owners WHERE contract = ? AND token_id = ? AND token_unique_id = ?", transfer.Contract, transfer.TokenID, tokenUniqueID).Scan(&currentOwner)
		if currentOwnerErr != nil {
			return currentOwnerErr
		}

		if currentOwner != transfer.From {
			errorText := fmt.Sprintf("current owner is not the sender: %s != %s for token %s:%s:%d", currentOwner, transfer.From, transfer.Contract, transfer.TokenID, tokenUniqueID)
			return errors.New(errorText)
		}

		// delete current ownership
		_, deleteErr := tx.Exec("DELETE FROM nft_owners WHERE owner = ? AND contract = ? AND token_id = ? AND token_unique_id = ?", transfer.From, transfer.Contract, transfer.TokenID, tokenUniqueID)
		if deleteErr != nil {
			return deleteErr
		}
	}

	// insert new ownership
	_, insertErr := tx.Exec("INSERT INTO nft_owners (owner, contract, token_id, token_unique_id, timestamp) VALUES (?, ?, ?, ?, ?)", transfer.To, transfer.Contract, transfer.TokenID, tokenUniqueID, transfer.BlockTime)
	if insertErr != nil {
		return insertErr
	}

	return nil
}

// UpdateOwnershipReverse undoes a previous transfer (to -> from).
func (o *OwnerDbImpl) UpdateOwnershipReverse(tx *sql.Tx, transfer NFTTransfer, tokenUniqueID uint64) error {
	// Check current ownership (select nft_owners where contract, token_id, token_unique_id = ? and check owner = from)
	var currentOwner string
	currentOwnerErr := tx.QueryRow("SELECT owner FROM nft_owners WHERE contract = ? AND token_id = ? AND token_unique_id = ?", transfer.Contract, transfer.TokenID, tokenUniqueID).Scan(&currentOwner)
	if currentOwnerErr != nil {
		return currentOwnerErr
	}

	if currentOwner != transfer.To {
		errorText := fmt.Sprintf("current owner is not the receiver: %s != %s for token %s:%s:%d", currentOwner, transfer.To, transfer.Contract, transfer.TokenID, tokenUniqueID)
		return errors.New(errorText)
	}

	// delete current ownership
	_, deleteErr := tx.Exec("DELETE FROM nft_owners WHERE owner = ? AND contract = ? AND token_id = ? AND token_unique_id = ?", transfer.To, transfer.Contract, transfer.TokenID, tokenUniqueID)
	if deleteErr != nil {
		return deleteErr
	}

	if transfer.Type != models.MINT && transfer.Type != models.AIRDROP {
		// insert new ownership
		_, insertErr := tx.Exec("INSERT INTO nft_owners (owner, contract, token_id, token_unique_id, timestamp) VALUES (?, ?, ?, ?, ?)", transfer.From, transfer.Contract, transfer.TokenID, tokenUniqueID, transfer.BlockTime)
		if insertErr != nil {
			return insertErr
		}
	}

	return nil
}

func newNFTOwner() *NFTOwner {
	return &NFTOwner{}
}

func (o *OwnerDbImpl) GetPaginatedResponseForQuery(rq db.QueryRunner, queryOptions db.QueryOptions, queryParams []interface{}) (total int, data []*NFTOwner, err error) {
	return db.GetPaginatedResponseForQuery("nft_owners", rq, allOwnersQuery, queryOptions, []string{"contract", "token_id", "token_unique_id"}, queryParams, newNFTOwner)
}
