package eth

import (
	"strings"
	"testing"

	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResetNFTs(t *testing.T) {
	db := setupTestInMemoryDB(t)

	nftDb := NewNFTDb()

	// Insert a test NFT
	err := db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateSupply(txn, "contractA", "token1", 10)
	})
	require.NoError(t, err)

	// Confirm it exists
	err = db.View(func(txn *badger.Txn) error {
		nft, err := nftDb.GetNFT(txn, "contractA", "token1")
		require.NoError(t, err)
		assert.NotNil(t, nft)
		assert.Equal(t, int64(10), nft.Supply)
		return nil
	})
	require.NoError(t, err)

	// Now reset
	err = nftDb.ResetNFTs(db)
	require.NoError(t, err)

	// Confirm the NFT is gone
	err = db.View(func(txn *badger.Txn) error {
		nft, err := nftDb.GetNFT(txn, "contractA", "token1")
		require.NoError(t, err)
		assert.Nil(t, nft)
		return nil
	})
	require.NoError(t, err)
}

func TestGetNFT(t *testing.T) {
	db := setupTestInMemoryDB(t)
	nftDb := NewNFTDb()

	// NFT should not exist initially
	err := db.View(func(txn *badger.Txn) error {
		nft, err := nftDb.GetNFT(txn, "nonexistent", "tokenX")
		require.NoError(t, err)
		assert.Nil(t, nft)
		return nil
	})
	require.NoError(t, err)

	// Create one
	err = db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateSupply(txn, "contractB", "token2", 5)
	})
	require.NoError(t, err)

	// Retrieve it
	err = db.View(func(txn *badger.Txn) error {
		nft, err := nftDb.GetNFT(txn, "contractB", "token2")
		require.NoError(t, err)
		assert.NotNil(t, nft)
		assert.True(t, strings.EqualFold("contractb", nft.Contract))
		assert.Equal(t, "token2", nft.TokenID)
		assert.Equal(t, int64(5), nft.Supply)
		assert.Equal(t, int64(0), nft.BurntSupply)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateSupply(t *testing.T) {
	db := setupTestInMemoryDB(t)
	nftDb := NewNFTDb()

	// Negative or zero delta should fail
	err := db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateSupply(txn, "contractC", "token3", 0)
	})
	require.NoError(t, err)

	// Positive delta: create new NFT
	err = db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateSupply(txn, "contractC", "token3", 10)
	})
	require.NoError(t, err)

	// Confirm new NFT
	err = db.View(func(txn *badger.Txn) error {
		nft, err := nftDb.GetNFT(txn, "contractC", "token3")
		require.NoError(t, err)
		require.NotNil(t, nft)
		assert.Equal(t, int64(10), nft.Supply)
		assert.Equal(t, int64(0), nft.BurntSupply)
		return nil
	})
	require.NoError(t, err)

	// Additional supply
	err = db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateSupply(txn, "contractC", "token3", 5)
	})
	require.NoError(t, err)

	// Confirm updated supply
	err = db.View(func(txn *badger.Txn) error {
		nft, err := nftDb.GetNFT(txn, "contractC", "token3")
		require.NoError(t, err)
		require.NotNil(t, nft)
		assert.Equal(t, int64(15), nft.Supply)
		assert.Equal(t, int64(0), nft.BurntSupply)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateBurntSupply(t *testing.T) {
	db := setupTestInMemoryDB(t)
	nftDb := NewNFTDb()

	// Attempt to burn an NFT that doesn't exist
	err := db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateBurntSupply(txn, "contractD", "token4", 5)
	})
	assert.Error(t, err, "cannot burn NFT that does not exist")

	// Create NFT
	err = db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateSupply(txn, "contractD", "token4", 20)
	})
	require.NoError(t, err)

	// Burn some tokens
	err = db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateBurntSupply(txn, "contractD", "token4", 5)
	})
	require.NoError(t, err)

	// Validate burnt supply
	err = db.View(func(txn *badger.Txn) error {
		nft, err := nftDb.GetNFT(txn, "contractD", "token4")
		require.NoError(t, err)
		require.NotNil(t, nft)
		assert.Equal(t, int64(20), nft.Supply)     // total supply doesn't change
		assert.Equal(t, int64(5), nft.BurntSupply) // burnt supply increased
		return nil
	})
	require.NoError(t, err)
}

func TestGetNftsByOwnerAddress(t *testing.T) {
	db := setupTestInMemoryDB(t)
	nftDb := NewNFTDb()

	// We need to simulate ownership by writing keys:
	// "tdh:owner:{owner}:{contract}:{tokenID}"
	// Then verify `GetNftsByOwnerAddress` picks them up.

	// Let's create some NFTs in the DB first
	err := db.Update(func(txn *badger.Txn) error {
		// NFT1
		err := nftDb.UpdateSupply(txn, "contractX", "token100", 10)
		if err != nil {
			return err
		}
		// NFT2
		return nftDb.UpdateSupply(txn, "contractY", "token200", 5)
	})
	require.NoError(t, err)

	// Now store ownership for an address e.g. "0x123"
	err = db.Update(func(txn *badger.Txn) error {
		// Owner key for NFT1
		ownerKey1 := []byte("tdh:owner:0x123:contractx:token100")
		if err := txn.Set(ownerKey1, []byte("owned")); err != nil {
			return err
		}
		// Owner key for NFT2
		ownerKey2 := []byte("tdh:owner:0x123:contracty:token200")
		if err := txn.Set(ownerKey2, []byte("owned")); err != nil {
			return err
		}
		// We'll also add an NFT that the owner does NOT actually own
		// to verify that it won't appear in the result
		err := nftDb.UpdateSupply(txn, "contractz", "token999", 3)
		if err != nil {
			return err
		}
		// Notice we do NOT store the ownership key "tdh:owner:0x123:contractZ:token999"
		return nil
	})
	require.NoError(t, err)

	// Now get the NFTs for 0x123
	err = db.View(func(txn *badger.Txn) error {
		nfts, err := nftDb.GetNftsByOwnerAddress(txn, "0x123")
		require.NoError(t, err)

		// We expect to see exactly 2 NFTs: (contractX, token100) and (contractY, token200)
		assert.Len(t, nfts, 2)

		// We'll track which ones we found
		foundX100 := false
		foundY200 := false

		for _, nft := range nfts {
			switch {
			case nft.Contract == "contractx" && nft.TokenID == "token100":
				foundX100 = true
				assert.Equal(t, int64(10), nft.Supply)
			case nft.Contract == "contracty" && nft.TokenID == "token200":
				foundY200 = true
				assert.Equal(t, int64(5), nft.Supply)
			}
		}

		assert.True(t, foundX100, "expected contractx/token100 in results")
		assert.True(t, foundY200, "expected contracty/token200 in results")

		return nil
	})
	require.NoError(t, err)
}

// 1. Test UpdateSupply with negative delta
func TestUpdateSupply_NegativeDelta(t *testing.T) {
	db := setupTestInMemoryDB(t)
	nftDb := NewNFTDb()

	// Attempt a negative delta
	err := db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateSupply(txn, "contractC", "tokenNeg", -5)
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delta must be positive",
		"should return an error if delta < 0")
}

// 2. Test UpdateBurntSupply with zero or negative delta
func TestUpdateBurntSupply_NonPositiveDelta(t *testing.T) {
	db := setupTestInMemoryDB(t)
	nftDb := NewNFTDb()

	// First create an NFT so that it exists in DB
	err := db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateSupply(txn, "contractBurn", "tokenBurn", 10)
	})
	require.NoError(t, err)

	// Attempt to burn with delta=0
	err = db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateBurntSupply(txn, "contractBurn", "tokenBurn", 0)
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delta must be positive")

	// Attempt to burn with delta < 0
	err = db.Update(func(txn *badger.Txn) error {
		return nftDb.UpdateBurntSupply(txn, "contractBurn", "tokenBurn", -3)
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delta must be positive")

	// Confirm the NFT in DB is still supply=10, burnt=0
	err = db.View(func(txn *badger.Txn) error {
		nft, getErr := nftDb.GetNFT(txn, "contractBurn", "tokenBurn")
		require.NoError(t, getErr)
		require.NotNil(t, nft)
		assert.Equal(t, int64(10), nft.Supply)
		assert.Equal(t, int64(0), nft.BurntSupply)
		return nil
	})
	require.NoError(t, err)
}

// 3. Test coverage for nftKey switch (gradients, memes, others)
func TestNftKeyFormatting(t *testing.T) {
	// For reference:
	//   GRADIENTS_CONTRACT => %03s
	//   MEMES_CONTRACT     => %05s
	//   else => no padding
	// Also test that spaces are replaced with '0'.

	// If you have real constants, use them; otherwise define them here for the test.
	gradientsContract := constants.GRADIENTS_CONTRACT // e.g. "0xGradient..."
	memesContract := constants.MEMES_CONTRACT         // e.g. "0xMemes..."
	otherContract := "0xOther"

	// Gradients => 3 digit padding
	got := nftKey(gradientsContract, "7")
	assert.Contains(t, got, "007", "should pad token ID to 3 digits for gradients")

	// Memes => 5 digit padding
	got = nftKey(memesContract, "42")
	assert.Contains(t, got, "00042", "should pad token ID to 5 digits for memes")

	// "Other" => no special padding
	got = nftKey(otherContract, "123")
	assert.Contains(t, got, ":123", "should not pad for other contracts")

	// Spaces => replaced with '0'
	got = nftKey(otherContract, "12  3")
	assert.Contains(t, got, ":12003", "spaces should be replaced by zeroes")
}

// 4. Test retrieving an NFT with corrupted JSON data in DB
func TestGetNFT_InvalidJSON(t *testing.T) {
	db := setupTestInMemoryDB(t)
	nftDb := NewNFTDb()

	// Manually store invalid JSON for a key
	err := db.Update(func(txn *badger.Txn) error {
		key := []byte(nftKey("badjsoncontract", "badtoken"))
		val := []byte("not-valid-json")
		return txn.Set(key, val)
	})
	require.NoError(t, err)

	// Now attempt to retrieve it
	err = db.View(func(txn *badger.Txn) error {
		nft, getErr := nftDb.GetNFT(txn, "badjsoncontract", "badtoken")
		// We expect an Unmarshal error, so nft should be nil
		assert.Nil(t, nft)
		assert.Error(t, getErr, "should fail on invalid JSON")
		return nil
	})
	require.NoError(t, err)
}

// 5. Test GetNftsByOwnerAddress with no matching keys (empty result)
func TestGetNftsByOwnerAddress_NoOwnership(t *testing.T) {
	db := setupTestInMemoryDB(t)
	nftDb := NewNFTDb()

	// No ownership keys are set; we just do a read
	err := db.View(func(txn *badger.Txn) error {
		nfts, getErr := nftDb.GetNftsByOwnerAddress(txn, "0xNobody")
		require.NoError(t, getErr)
		// We expect an empty slice, not nil
		assert.Empty(t, nfts, "should return empty slice when no NFTs owned")
		return nil
	})
	require.NoError(t, err)
}
