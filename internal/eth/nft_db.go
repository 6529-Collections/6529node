package eth

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

/*
DB Indexes Created Here:

1. Primary NFT storage:
   "tdh:nft:{contract}:{tokenID}" => JSON(NFT)

   tokenID padding for lexical ordering:
   - 3 digits for gradients
   - 5 digits for memes
   - no padding for nextgen
*/

type NFTDb interface {
	ResetNFTs(db *badger.DB) error
	GetNFT(txn *badger.Txn, contract, tokenID string) (*NFT, error)
	UpdateSupply(txn *badger.Txn, contract, tokenID string, delta int64) error
	UpdateBurntSupply(txn *badger.Txn, contract, tokenID string, delta int64) error
	GetNftsByOwnerAddress(txn *badger.Txn, owner string) ([]NFT, error)
}

func NewNFTDb() NFTDb {
	return &NFTDbImpl{}
}

type NFT struct {
	Contract    string `json:"contract"`
	TokenID     string `json:"token_id"`
	Supply      int64  `json:"supply"`       // Total minted supply
	BurntSupply int64  `json:"burnt_supply"` // Total burnt supply
}

type NFTDbImpl struct{}

const nftPrefix = "tdh:nft:"

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

func (n *NFTDbImpl) ResetNFTs(db *badger.DB) error {
	if err := db.DropPrefix([]byte(nftPrefix)); err != nil {
		return fmt.Errorf("failed to drop prefix %s: %w", nftPrefix, err)
	}
	return nil
}

// UpdateSupply increases the total supply when minting.
func (n *NFTDbImpl) UpdateSupply(txn *badger.Txn, contract, tokenID string, delta int64) error {
	if delta < 0 {
		return errors.New("delta must be positive")
	}

	key := nftKey(contract, tokenID)

	var nft NFT
	item, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		// NFT doesn't exist, create a new one
		zap.L().Info("NFT not found, creating new NFT",
			zap.String("contract", contract),
			zap.String("tokenId", tokenID),
			zap.Int64("initialSupply", delta),
		)
		nft = NFT{
			Contract:    contract,
			TokenID:     tokenID,
			Supply:      delta,
			BurntSupply: 0,
		}
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
		zap.L().Error("Cannot burn NFT that does not exist",
			zap.String("contract", contract),
			zap.String("tokenId", tokenID),
		)
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

// NEW: Get all NFTs (metadata) owned by a given address.
// This scans "tdh:owner:{owner}:" from OwnerDb to get (contract, tokenID) pairs.
func (n *NFTDbImpl) GetNftsByOwnerAddress(txn *badger.Txn, owner string) ([]NFT, error) {
	// We'll look for keys of the form: "tdh:owner:{owner}:{contract}:{tokenID}"
	ownerPrefix := fmt.Sprintf("tdh:owner:%s:", owner)
	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()

	// Use a map to avoid duplicates, in case of any repeated scanning
	// Key = contract:tokenID, value = NFT
	nftMap := make(map[string]NFT)

	for it.Seek([]byte(ownerPrefix)); it.ValidForPrefix([]byte(ownerPrefix)); it.Next() {
		keyBytes := it.Item().Key()
		// Key format: "tdh:owner:OWNER:CONTRACT:TOKENID"
		// Let's parse out contract & tokenID from that.

		// Convert key bytes to string
		fullKey := string(keyBytes)
		// The portion after "tdh:owner:{owner}:" is "contract:tokenID"
		suffix := fullKey[len(ownerPrefix):]

		// We'll split on the colon to get contract, tokenID
		// suffix => "contract:tokenID"
		var contract, tokenID string
		// We can do a simple parse if we expect exactly one colon
		// but if your contract might contain colons or partial strings, you'd do more robust parsing
		// For standard usage, let's do:
		//   "contract:tokenID" => [0]=contract, [1]=tokenID
		// or handle edge cases gracefully.
		parts := []rune{}
		for i, ch := range suffix {
			if ch == ':' {
				contract = string(parts)
				tokenID = suffix[i+1:]
				break
			}
			parts = append(parts, ch)
		}

		// Retrieve the NFT metadata
		nftObj, err := n.GetNFT(txn, contract, tokenID)
		if err != nil {
			return nil, err
		}
		// If the NFT record doesn't exist (nil), we can skip
		if nftObj == nil {
			continue
		}

		mapKey := contract + ":" + tokenID
		nftMap[mapKey] = *nftObj
	}

	// Convert map values to a slice
	nfts := make([]NFT, 0, len(nftMap))
	for _, nft := range nftMap {
		nfts = append(nfts, nft)
	}
	return nfts, nil
}

func nftKey(contract, tokenID string) string {
	switch contract {
	case constants.GRADIENTS_CONTRACT:
		tokenID = fmt.Sprintf("%03s", tokenID)
	case constants.MEMES_CONTRACT:
		tokenID = fmt.Sprintf("%05s", tokenID)
	}

	tokenID = strings.ReplaceAll(tokenID, " ", "0")

	return fmt.Sprintf("%s%s:%s", nftPrefix, contract, tokenID)
}
