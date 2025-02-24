package ethdb

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestNFTDb(t *testing.T) (*sql.DB, NFTDb, func()) {
	db, cleanup := testdb.SetupTestDB(t)

	nftDB := NewNFTDb()
	return db, nftDB, cleanup
}

func TestNFTDb_UpdateSupply_CreatesNewNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err, "Failed to begin transaction")

	supply, err := nftDB.UpdateSupply(tx, "0xContractA", "TokenA")
	require.NoError(t, err, "UpdateSupply should not fail on creating a new NFT")
	assert.Equal(t, uint64(1), supply, "Expected new NFT supply to be 1")

	err = tx.Commit()
	require.NoError(t, err, "Failed to commit transaction")

	// Verify in DB
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractA", "TokenA")
	require.NoError(t, err, "GetNft should find newly created NFT")
	assert.Equal(t, uint64(1), nft.Supply, "Expected supply=1 after creation")
	assert.Equal(t, uint64(0), nft.BurntSupply, "Expected burnt_supply=0 after creation")
	_ = txCheck.Rollback() // just checking, no need to commit
}

func TestNFTDb_UpdateSupply_ExistingNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create an NFT manually first
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractB", "TokenB")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Now update the same NFT again
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	supply, err := nftDB.UpdateSupply(tx, "0xContractB", "TokenB")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), supply, "Expected supply to increment to 2")

	err = tx.Commit()
	require.NoError(t, err)

	// Double check final state
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractB", "TokenB")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), nft.Supply, "Expected supply=2 after second update")
	assert.Equal(t, uint64(0), nft.BurntSupply)
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateBurntSupply_ExistingNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create an NFT first
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractC", "TokenC")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Now burn 1
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupply(tx, "0xContractC", "TokenC")
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Check final state
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractC", "TokenC")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nft.Supply, "Supply should remain 1")
	assert.Equal(t, uint64(1), nft.BurntSupply, "Burnt supply should be 1")
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateBurntSupply_NonexistentNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupply(tx, "0xContractD", "TokenD")
	assert.Error(t, err, "Expected error when burning nonexistent NFT")

	// Rolling back so we can test the final state (though it should not have changed anything)
	_ = tx.Rollback()

	// Double-check that there's no NFT in DB
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.GetNft(txCheck, "0xContractD", "TokenD")
	assert.Error(t, err, "Expected not found error after a failed burn attempt")
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateSupplyReverse_BasicRevert(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create NFT with supply=2
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractE", "TokenE")
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractE", "TokenE")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Reverse supply by 1
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	newSupply, err := nftDB.UpdateSupplyReverse(tx, "0xContractE", "TokenE")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), newSupply, "Supply should decrement to 1")
	require.NoError(t, tx.Commit())

	// Final check
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractE", "TokenE")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nft.Supply, "Expected supply=1 after revert")
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateSupplyReverse_NonexistentNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = nftDB.UpdateSupplyReverse(tx, "0xContractF", "TokenF")
	assert.Error(t, err, "Expected error when reverting supply on nonexistent NFT")
	_ = tx.Rollback()
}

func TestNFTDb_UpdateSupplyReverse_SupplyWouldGoNegative(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create NFT with supply=0 (we won't increment it, so supply remains 0).
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = nftDB.UpdateSupply(tx, "0xContractG", "TokenG")
	require.NoError(t, err)

	// Now set supply=1, then revert it to 0 so the final supply is 0
	_, err = nftDB.UpdateSupplyReverse(tx, "0xContractG", "TokenG")
	require.NoError(t, err)

	// Attempt another revert on supply=0 -> error
	_, err = nftDB.UpdateSupplyReverse(tx, "0xContractG", "TokenG")
	assert.Error(t, err, "Should fail if supply is already 0 and we revert again")

	_ = tx.Rollback()
}

func TestNFTDb_UpdateBurntSupplyReverse_BasicRevert(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create an NFT with supply=1, burnt_supply=1
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractH", "TokenH")
	require.NoError(t, err)
	err = nftDB.UpdateBurntSupply(tx, "0xContractH", "TokenH")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Revert the burn
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	err = nftDB.UpdateBurntSupplyReverse(tx, "0xContractH", "TokenH")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Final check: burnt_supply should be 0
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txCheck, "0xContractH", "TokenH")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), nft.BurntSupply, "Burnt supply should revert back to 0")
	_ = txCheck.Rollback()
}

func TestNFTDb_UpdateBurntSupplyReverse_NonexistentNft(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = nftDB.UpdateBurntSupplyReverse(tx, "0xContractI", "TokenI")
	assert.Error(t, err, "Expected error when reverting burnt supply on nonexistent NFT")

	_ = tx.Rollback()
}

func TestNFTDb_UpdateBurntSupplyReverse_WouldGoNegative(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Create an NFT but don't burn it => burnt_supply=0
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(tx, "0xContractJ", "TokenJ")
	require.NoError(t, err)

	// Attempting to revert burnt supply while burnt_supply=0
	err = nftDB.UpdateBurntSupplyReverse(tx, "0xContractJ", "TokenJ")
	assert.Error(t, err, "Cannot revert burnt supply from 0")

	_ = tx.Rollback()
}

func TestNFTDb_GetNft_Nonexistent(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	_, err = nftDB.GetNft(tx, "0xContractK", "TokenK")
	assert.Error(t, err, "Expected error on retrieving nonexistent NFT")

	_ = tx.Rollback()
}

func TestNFTDb_ClosedTxBehavior(t *testing.T) {
	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	// Begin and immediately commit
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Now use this closed tx for an NFT operation
	_, err = nftDB.UpdateSupply(tx, "0xContractZ", "TokenZ")
	assert.Error(t, err, "Expected an error if the transaction is already closed")
}

func TestNFTDb_ConcurrentAccess(t *testing.T) {
	// This test shows concurrency usage. Because each method expects a *sql.Tx,
	// we typically don't do concurrent writes in the same Tx. Instead we show
	// concurrent usage with separate transactions.

	db, nftDB, cleanup := setupTestNFTDb(t)
	defer cleanup()

	var wg sync.WaitGroup

	// We'll run concurrent supply updates on the same contract/token
	// to see if it behaves well in separate transactions.
	contract := "0xContractConcurrent"
	tokenID := "TokenConcurrency"

	// First, ensure the NFT is created
	txInit, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = nftDB.UpdateSupply(txInit, contract, tokenID)
	require.NoError(t, err)
	require.NoError(t, txInit.Commit())

	worker := func(start, end int) {
		defer wg.Done()
		for i := start; i < end; i++ {
			tx, err := db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			_, err = nftDB.UpdateSupply(tx, contract, tokenID)
			assert.NoError(t, err)
			err = tx.Commit()
			assert.NoError(t, err)
		}
	}

	wg.Add(2)
	go worker(0, 50)
	go worker(50, 100)
	wg.Wait()

	// Final check
	txFinal, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	nft, err := nftDB.GetNft(txFinal, contract, tokenID)
	require.NoError(t, err)
	// The initial call set supply=1, plus 100 increments from concurrency
	assert.Equal(t, uint64(1+100), nft.Supply, "Expected final supply to account for concurrency increments")
	_ = txFinal.Rollback()
}

func TestNFTDb_ErrorPropagation(t *testing.T) {
	// This test tries to force or check that an error inside a query is properly propagated.
	// The primary ways to do that are either by injecting a broken SQL statement,
	// or using a transaction on a closed DB, etc.

	db, nftDB, cleanup := setupTestNFTDb(t)
	cleanup()

	// The DB is now closed by cleanup, so any operation should fail
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		// might already fail here, but let's just check we can't do anything with nftDB
		assert.Error(t, err, "Expected an error when trying to create a transaction on a closed DB")
	} else {
		_, err := nftDB.UpdateSupply(tx, "0xContractErr", "TokenErr")
		assert.Error(t, err, "Expected error when using a closed DB transaction")

		err = tx.Commit()
		assert.Error(t, err, "Expected commit failure on closed DB")
	}
}
