package eth

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newInMemoryBadger(t *testing.T) *badger.DB {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	require.NoError(t, err, "failed to open badger in-memory DB")
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestNFTDb(t *testing.T) {
	zap.ReplaceGlobals(zap.NewNop()) // Mute logging if desired
	db := newInMemoryBadger(t)
	nftDb := NewNFTDb()

	t.Run("GetNFT - not found", func(t *testing.T) {
		err := db.View(func(txn *badger.Txn) error {
			nft, err := nftDb.GetNFT(txn, "0xNoSuchContract", "999")
			require.NoError(t, err)
			assert.Nil(t, nft, "expected a nil NFT when not found")
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("UpdateSupply - create new NFT", func(t *testing.T) {
		// Suppose we create a new NFT "0xCONTRACT:1" with supply=5
		err := db.Update(func(txn *badger.Txn) error {
			return nftDb.UpdateSupply(txn, "0xCONTRACT", "1", 5)
		})
		require.NoError(t, err)

		// Verify it was created
		err = db.View(func(txn *badger.Txn) error {
			nft, err := nftDb.GetNFT(txn, "0xCONTRACT", "1")
			require.NoError(t, err)
			require.NotNil(t, nft)
			assert.Equal(t, int64(5), nft.Supply)
			assert.Equal(t, int64(0), nft.BurntSupply)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("UpdateSupply - increment existing NFT", func(t *testing.T) {
		// Increase "0xCONTRACT:1" supply by 3 => total 8
		err := db.Update(func(txn *badger.Txn) error {
			return nftDb.UpdateSupply(txn, "0xCONTRACT", "1", 3)
		})
		require.NoError(t, err)

		// Verify supply=8
		err = db.View(func(txn *badger.Txn) error {
			nft, err := nftDb.GetNFT(txn, "0xCONTRACT", "1")
			require.NoError(t, err)
			require.NotNil(t, nft)
			assert.Equal(t, int64(8), nft.Supply)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("UpdateSupply - negative or zero delta fails", func(t *testing.T) {
		err := db.Update(func(txn *badger.Txn) error {
			return nftDb.UpdateSupply(txn, "0xCONTRACT", "2", 0)
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delta must be positive")
	})

	t.Run("UpdateBurntSupply - error if NFT does not exist", func(t *testing.T) {
		err := db.Update(func(txn *badger.Txn) error {
			return nftDb.UpdateBurntSupply(txn, "0xNOPE", "999", 1)
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot burn NFT that does not exist")
	})

	t.Run("UpdateBurntSupply - success for existing NFT", func(t *testing.T) {
		// We'll burn 2 from "0xCONTRACT:1" which now has supply=8
		err := db.Update(func(txn *badger.Txn) error {
			return nftDb.UpdateBurntSupply(txn, "0xCONTRACT", "1", 2)
		})
		require.NoError(t, err)

		// Should now have burntSupply=2
		err = db.View(func(txn *badger.Txn) error {
			nft, err := nftDb.GetNFT(txn, "0xCONTRACT", "1")
			require.NoError(t, err)
			require.NotNil(t, nft)
			assert.Equal(t, int64(8), nft.Supply)
			assert.Equal(t, int64(2), nft.BurntSupply)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("GetNftsByOwnerAddress", func(t *testing.T) {
		/*
		   This method scans "tdh:owner:{owner}:" to find "contract:tokenId"
		   suffixes. For example, "tdh:owner:0xAlice:0xCONTRACT:999" => someBalance
		   Then it calls GetNFT(...) to load the actual NFT struct.

		   So let's create some "owner" keys ourselves in the DB, plus
		   the NFT records, so that GetNftsByOwnerAddress returns them.
		*/

		// 1) Manually store an NFT "0xCONTRACT:999" with supply=10
		err := db.Update(func(txn *badger.Txn) error {
			return nftDb.UpdateSupply(txn, "0xCONTRACT", "999", 10)
		})
		require.NoError(t, err)

		// 2) Manually create a key "tdh:owner:0xAlice:0xCONTRACT:999" => JSON(123) as "balance"
		//    so that the scan sees "0xCONTRACT:999" suffix.
		err = db.Update(func(txn *badger.Txn) error {
			// Key format: "tdh:owner:{owner}:{contract}:{tokenID}"
			key := fmt.Sprintf("tdh:owner:%s:%s:%s", "0xAlice", "0xCONTRACT", "999")
			balance := int64(123) // any non-zero to confirm "ownership"
			val, _ := json.Marshal(balance)
			return txn.Set([]byte(key), val)
		})
		require.NoError(t, err)

		// 3) Also let's create "tdh:owner:0xAlice:0xCONTRACT:1" => JSON(5) to test the other NFT
		//    (We already have "0xCONTRACT:1" created above.)
		err = db.Update(func(txn *badger.Txn) error {
			key := "tdh:owner:0xAlice:0xCONTRACT:1"
			val, _ := json.Marshal(int64(5))
			return txn.Set([]byte(key), val)
		})
		require.NoError(t, err)

		// 4) Now call GetNftsByOwnerAddress("0xAlice") => should return [ (0xCONTRACT,1), (0xCONTRACT,999) ]
		err = db.View(func(txn *badger.Txn) error {
			nfts, err := nftDb.GetNftsByOwnerAddress(txn, "0xAlice")
			require.NoError(t, err)
			// Expect 2 items
			require.Len(t, nfts, 2, "expect 2 NFTs for 0xAlice")

			var found1, found999 bool
			for _, n := range nfts {
				if n.Contract == "0xCONTRACT" && n.TokenID == "1" {
					found1 = true
					// supply was 8 (Mint) & burnt 2 => supply=8, burnt=2
					assert.Equal(t, int64(8), n.Supply)
					assert.Equal(t, int64(2), n.BurntSupply)
				}
				if n.Contract == "0xCONTRACT" && n.TokenID == "999" {
					found999 = true
					// supply=10, burnt=0
					assert.Equal(t, int64(10), n.Supply)
					assert.Equal(t, int64(0), n.BurntSupply)
				}
			}
			assert.True(t, found1, "did not find NFT (0xCONTRACT,1)")
			assert.True(t, found999, "did not find NFT (0xCONTRACT,999)")
			return nil
		})
		require.NoError(t, err)
	})
}
