package eth

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type NFTDb interface {
	RegisterNFT(contract, tokenID string, supply int64) error
	GetNFT(contract, tokenID string) (*NFT, error)
	UpdateSupply(contract, tokenID string, delta int64) error
}

func NewNFTDb(db *badger.DB) NFTDb {
	return &NFTDbImpl{db: db}
}

type NFT struct {
	Contract string `json:"contract"`
	TokenID  string `json:"token_id"`
	Supply   int64  `json:"supply"`
}

type NFTDbImpl struct {
	mu sync.RWMutex
	db *badger.DB
}

const nftPrefix = "tdh:nft:"

// RegisterNFT stores a new NFT record with its supply.
func (n *NFTDbImpl) RegisterNFT(contract, tokenID string, supply int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.db.Update(func(txn *badger.Txn) error {
		key := nftKey(contract, tokenID)

		// Check if NFT already exists
		existingNFT, err := n.GetNFT(contract, tokenID)
		if err != nil {
			return err
		}
		if existingNFT != nil {
			return errors.New("NFT already exists")
		}

		nft := NFT{Contract: contract, TokenID: tokenID, Supply: supply}
		value, err := json.Marshal(nft)
		if err != nil {
			return err
		}

		return txn.Set([]byte(key), value)
	})
}

// GetNFT retrieves the NFT metadata.
func (n *NFTDbImpl) GetNFT(contract, tokenID string) (*NFT, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var nft NFT
	err := n.db.View(func(txn *badger.Txn) error {
		key := nftKey(contract, tokenID)
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &nft)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return &nft, err
}

// UpdateSupply increases the supply of an NFT by a delta value (e.g., +1 for minting).
func (n *NFTDbImpl) UpdateSupply(contract, tokenID string, delta int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.db.Update(func(txn *badger.Txn) error {
		key := nftKey(contract, tokenID)

		// Retrieve the existing NFT record
		nft, err := n.GetNFT(contract, tokenID)
		if err != nil {
			return err
		}
		if nft == nil {
			return errors.New("NFT not found")
		}

		// Update supply (only increasing)
		nft.Supply += delta
		value, err := json.Marshal(nft)
		if err != nil {
			return err
		}

		return txn.Set([]byte(key), value)
	})
}

// Helper function to generate NFT key
func nftKey(contract, tokenID string) string {
	return fmt.Sprintf("%s%s:%s", nftPrefix, contract, tokenID)
}