package eth

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type NFTDb interface {
	GetNFT(txn *badger.Txn, contract, tokenID string) (*NFT, error)  
	UpdateSupply(txn *badger.Txn, contract, tokenID string, delta int64) error  
	UpdateBurntSupply(txn *badger.Txn, contract, tokenID string, delta int64) error 
}

func NewNFTDb() NFTDb {
	return &NFTDbImpl{}
}

type NFT struct {
	Contract   string `json:"contract"`
	TokenID    string `json:"token_id"`
	Supply     int64  `json:"supply"`      // Total minted supply
	BurntSupply int64 `json:"burnt_supply"` // Total burnt supply
}

type NFTDbImpl struct {}

const nftPrefix = "tdh:nft:"

// GetNFT retrieves the NFT metadata.
func (n *NFTDbImpl) GetNFT(txn *badger.Txn, contract, tokenID string) (*NFT, error) {
	key := nftKey(contract, tokenID)

	item, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		return nil, nil // Return nil if the NFT doesn't exist
	} else if err != nil {
		return nil, err
	}

	var nft NFT
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &nft)
	})
	if err != nil {
		return nil, err
	}

	return &nft, nil
}

// UpdateSupply increases the total supply when minting.
func (n *NFTDbImpl) UpdateSupply(txn *badger.Txn, contract, tokenID string, delta int64) error {
	if delta <= 0 {
		return errors.New("delta must be positive")
	}

	key := nftKey(contract, tokenID)

	var nft NFT
	item, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		// NFT doesn't exist, create a new one
		zap.L().Info("NFT not found, creating new NFT", zap.String("contract", contract), zap.String("tokenId", tokenID), zap.Int64("initialSupply", delta))
		nft = NFT{Contract: contract, TokenID: tokenID, Supply: delta, BurntSupply: 0}
	} else if err != nil {
		return err
	} else {
		// NFT exists, update its supply
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &nft)
		})
		if err != nil {
			return err
		}
		nft.Supply += delta
	}

	// Save NFT back to DB
	value, err := json.Marshal(nft)
	if err != nil {
		return err
	}

	return txn.Set([]byte(key), value)
}

// UpdateBurntSupply increases the burnt supply when burning tokens.
func (n *NFTDbImpl) UpdateBurntSupply(txn *badger.Txn, contract, tokenID string, delta int64) error {
	if delta <= 0 {
		return errors.New("delta must be positive")
	}

	key := nftKey(contract, tokenID)

	var nft NFT
	item, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		// If NFT doesn't exist, return an error (cannot burn what hasn't been minted)
		zap.L().Error("Cannot burn NFT that does not exist", zap.String("contract", contract), zap.String("tokenId", tokenID))
		return errors.New("cannot burn NFT that does not exist")
	} else if err != nil {
		return err
	}

	// NFT exists, update burnt supply
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &nft)
	})
	if err != nil {
		return err
	}

	nft.BurntSupply += delta // Burned tokens do not decrease total supply

	// Save NFT back to DB
	value, err := json.Marshal(nft)
	if err != nil {
		return err
	}

	return txn.Set([]byte(key), value)
}

// Helper function to generate NFT key
func nftKey(contract, tokenID string) string {
	return fmt.Sprintf("%s%s:%s", nftPrefix, contract, tokenID)
}