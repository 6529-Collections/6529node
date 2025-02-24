package ethdb

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestOwnerDb spins up a test DB, returns:
//   - *sql.DB
//   - OwnerDb (system under test)
//   - NFTDb (to create references in nfts table when needed)
//   - cleanup function
//
// The test migrations are assumed to already create 'nfts' & 'nft_owners'.
func setupTestOwnerDb(t *testing.T) (*sql.DB, OwnerDb, NFTDb, func()) {
	db, cleanup := testdb.SetupTestDB(t)

	ownerDb := NewOwnerDb()
	nftDb := NewNFTDb()

	return db, ownerDb, nftDb, cleanup
}

// createNftInDB is a helper that ensures a record in 'nfts' table
// so we can insert into 'nft_owners' (which references it).
func createNftInDB(t *testing.T, db *sql.DB, nftDb NFTDb, contract, tokenID string) {
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	// Use UpdateSupply once to create an NFT with supply=1
	_, err = nftDb.UpdateSupply(tx, contract, tokenID)
	require.NoError(t, err, "failed to create NFT in nfts table")
	require.NoError(t, tx.Commit())
}

func TestOwnerDb_GetUniqueID_Nonexistent(t *testing.T) {
	db, ownerDb, _, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	uniqueID, err := ownerDb.GetUniqueID(tx, "0xTest", "TokenX", "0xOwnerDoesNotExist")
	assert.Error(t, err, "Expected an error if there's no record in nft_owners")
	assert.Equal(t, uint64(0), uniqueID, "UniqueID should be zero if not found or error occurred")

	_ = tx.Rollback()
}

func TestOwnerDb_UpdateOwnership_MintCreatesOwner(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xMintContract"
	tokenID := "MintToken1"
	createNftInDB(t, db, nftDb, contract, tokenID) // ensure NFT is in 'nfts'

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfer := models.TokenTransfer{
		From:      "", // for a mint, typically empty or some zero address
		To:        "0xNewOwner",
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 123456789,
		Type:      models.MINT,
	}

	// Typically, for a mint, tokenUniqueID might be something you track externally.
	// We'll pass 1 for testing:
	err = ownerDb.UpdateOwnership(tx, transfer, 1)
	require.NoError(t, err, "Mint update should succeed")

	require.NoError(t, tx.Commit())

	// Verify that the new ownership is present
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	uniqueID, err := ownerDb.GetUniqueID(txCheck, contract, tokenID, "0xNewOwner")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), uniqueID, "Expected uniqueID=1 after mint ownership")

	_ = txCheck.Rollback()
}

func TestOwnerDb_UpdateOwnership_TransferSuccess(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xTransferContract"
	tokenID := "TokenTransferA"
	fromAddr := "0xAlice"
	toAddr := "0xBob"

	// 1) Create NFT
	createNftInDB(t, db, nftDb, contract, tokenID)

	// 2) Insert initial ownership (simulate minted ownership to fromAddr)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mint := models.TokenTransfer{
		From:      "",
		To:        fromAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 100,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(tx, mint, 42)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	// 3) Do a forward transfer from => to
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfer := models.TokenTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 200,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnership(tx, transfer, 42)
	require.NoError(t, err, "Expected successful transfer")

	require.NoError(t, tx.Commit())

	// 4) Check that 'toAddr' is now the owner
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	uniqueID, err := ownerDb.GetUniqueID(txCheck, contract, tokenID, toAddr)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), uniqueID, "Owner should be Bob with the same tokenUniqueID")

	// 'fromAddr' should not have the record
	uidFrom, err := ownerDb.GetUniqueID(txCheck, contract, tokenID, fromAddr)
	assert.Error(t, err, "Alice's record should have been deleted")
	assert.Equal(t, uint64(0), uidFrom)

	_ = txCheck.Rollback()
}

func TestOwnerDb_UpdateOwnership_TransferWrongCurrentOwner(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xWrongOwnerContract"
	tokenID := "WrongOwnerToken"
	fromAddr := "0xAlice"
	toAddr := "0xBob"

	createNftInDB(t, db, nftDb, contract, tokenID)

	// Insert ownership to a different address (not fromAddr)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mint := models.TokenTransfer{
		From:      "",
		To:        "0xCarol", // Carol is the actual owner in DB
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 101,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(tx, mint, 999)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Attempt a transfer from Alice -> Bob, but Carol is the real owner
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfer := models.TokenTransfer{
		From:      fromAddr, // 0xAlice
		To:        toAddr,   // 0xBob
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 200,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnership(tx, transfer, 999)
	assert.Error(t, err, "Expected error because current owner is Carol, not Alice")

	_ = tx.Rollback()
}

func TestOwnerDb_UpdateOwnership_TransferNonExistentNftOwnerRecord(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xNonExistentOwner"
	tokenID := "TokenNEO"
	fromAddr := "0xNonOwner"
	toAddr := "0xBob"

	// We do create the NFT in nfts table, but there's no *ownership* record yet
	createNftInDB(t, db, nftDb, contract, tokenID)

	// Transfer tries to find the current owner row. Should fail with "no rows" on scanning currentOwner.
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfer := models.TokenTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 999999,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnership(tx, transfer, 777)
	assert.Error(t, err, "No existing ownership record => should error")

	_ = tx.Rollback()
}

func TestOwnerDb_UpdateOwnershipReverse_BasicRevert(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xReverseContract"
	tokenID := "ReverseToken1"
	fromAddr := "0xAlice"
	toAddr := "0xBob"
	tokenUniqueID := uint64(1234)

	// 1) Create NFT & initial ownership => fromAddr
	createNftInDB(t, db, nftDb, contract, tokenID)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mint := models.TokenTransfer{
		From:      "",
		To:        fromAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 10,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(tx, mint, tokenUniqueID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// 2) Transfer from => to
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	forward := models.TokenTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 20,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnership(tx, forward, tokenUniqueID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// 3) Reverse (undo) that transfer => to -> from
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// Suppose NFTTransfer is structurally the same but used for Reverse
	reverse := NFTTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 20,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnershipReverse(tx, reverse, tokenUniqueID)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	// 4) final check: fromAddr should own it again, toAddr should not.
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	uidFrom, err := ownerDb.GetUniqueID(txCheck, contract, tokenID, fromAddr)
	require.NoError(t, err, "Should exist again for fromAddr")
	assert.Equal(t, tokenUniqueID, uidFrom)

	uidTo, err := ownerDb.GetUniqueID(txCheck, contract, tokenID, toAddr)
	assert.Error(t, err, "Should no longer exist for toAddr")
	assert.Equal(t, uint64(0), uidTo)

	_ = txCheck.Rollback()
}

func TestOwnerDb_UpdateOwnershipReverse_WrongCurrentOwner(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xReverseWrongOwner"
	tokenID := "ReverseWrongToken"
	fromAddr := "0xAlice"
	toAddr := "0xBob"

	// NFT + fromAddr owns it
	createNftInDB(t, db, nftDb, contract, tokenID)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mint := models.TokenTransfer{
		From:      "",
		To:        fromAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 10,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(tx, mint, 1111)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Attempt a reverse as if the current owner is `toAddr`, but fromAddr is the actual owner
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	reverse := NFTTransfer{
		From:      fromAddr,
		To:        toAddr, // we say "to=Bob" but Bob isn't actually the current owner
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 10,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnershipReverse(tx, reverse, 1111)
	assert.Error(t, err, "Expected error because fromAddr is actually the owner, not Bob")

	_ = tx.Rollback()
}

func TestOwnerDb_UpdateOwnershipReverse_NoExistingRecord(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xReverseNoRecord"
	tokenID := "ReverseNoRecToken"
	fromAddr := "0xAlice"
	toAddr := "0xBob"
	tokenUniqueID := uint64(2020)

	// Create NFT but never create ownership in `nft_owners`
	createNftInDB(t, db, nftDb, contract, tokenID)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	reverse := NFTTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 50,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnershipReverse(tx, reverse, tokenUniqueID)
	assert.Error(t, err, "Expected error: no record found for this tokenUniqueID in nft_owners")

	_ = tx.Rollback()
}

func TestOwnerDb_GetBalance_ZeroIfNoRecord(t *testing.T) {
	db, ownerDb, _, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	// Just check that if there's no row for that owner, we get 0
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	balance, err := ownerDb.GetBalance(tx, "0xNobody", "0xContractZ", "TokenZ")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), balance, "If no row, balance=0")

	_ = tx.Rollback()
}

func TestOwnerDb_GetBalance_NonZero(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	// For demonstration, let's insert a row with balance=5.
	contract := "0xBalanceContract"
	tokenID := "BalanceToken"
	ownerAddr := "0xOwnerBalance"

	createNftInDB(t, db, nftDb, contract, tokenID)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err = tx.Exec(`
			INSERT INTO nft_owners (owner, contract, token_id, token_unique_id, timestamp)
			VALUES (?, ?, ?, ?, ?)`,
			ownerAddr, contract, tokenID, i, 9999,
		)
	}
	// If your table doesn't have 'balance', you'll need a different approach.
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	// Now test the retrieval
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	balance, err := ownerDb.GetBalance(txCheck, ownerAddr, contract, tokenID)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), balance, "Expected to read back the inserted balance=5")

	_ = txCheck.Rollback()
}

func TestOwnerDb_ClosedDbBehavior(t *testing.T) {
	db, ownerDb, _, cleanup := setupTestOwnerDb(t)
	cleanup() // close DB

	// Now any operation should fail
	tx, err := db.BeginTx(context.Background(), nil)
	if err == nil {
		// If we somehow got a transaction, let's test the usage
		mint := models.TokenTransfer{
			From:      "",
			To:        "0xClosedDB",
			Contract:  "0xSomeContract",
			TokenID:   "ClosedDBToken",
			BlockTime: 999,
			Type:      models.MINT,
		}
		// This should fail
		err = ownerDb.UpdateOwnership(tx, mint, 1)
		assert.Error(t, err, "Expected error with a closed DB/transaction")

		_ = tx.Rollback()
	} else {
		assert.Error(t, err, "Expected an error when attempting to beginTx on closed DB")
	}
}

func TestOwnerDb_ConcurrentAccess(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xConcOwner"
	tokenID := "ConcToken"
	tokenUniqueID := uint64(12345)

	// Create NFT record
	createNftInDB(t, db, nftDb, contract, tokenID)

	// We'll simulate multiple mints or transfers in parallel.
	// For simplicity, let's do multiple "mints" with different unique IDs or addresses.
	var wg sync.WaitGroup

	worker := func(owner string, uid uint64) {
		defer wg.Done()
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)

		mint := models.TokenTransfer{
			From:      "",
			To:        owner,
			Contract:  contract,
			TokenID:   tokenID,
			BlockTime: 111,
			Type:      models.MINT,
		}
		err = ownerDb.UpdateOwnership(tx, mint, uid)
		assert.NoError(t, err)

		err = tx.Commit()
		assert.NoError(t, err)
	}

	wg.Add(3)
	go worker("0xAlpha", tokenUniqueID+1)
	go worker("0xBeta", tokenUniqueID+2)
	go worker("0xGamma", tokenUniqueID+3)

	wg.Wait()

	// Check that all 3 owners appear in nft_owners
	txCheck, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	ids := []uint64{tokenUniqueID + 1, tokenUniqueID + 2, tokenUniqueID + 3}
	owners := []string{"0xAlpha", "0xBeta", "0xGamma"}

	for i, ownr := range owners {
		uid, err := ownerDb.GetUniqueID(txCheck, contract, tokenID, ownr)
		require.NoError(t, err)
		assert.Equal(t, ids[i], uid)
	}

	_ = txCheck.Rollback()
}

func TestOwnerDb_ErrorPropagation(t *testing.T) {
	db, ownerDb, _, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	// For demonstration, let's attempt some invalid SQL or do a forced error scenario.
	// Because your queries are pretty straightforward, we'll simulate by trying a reverse
	// update on an NFT that doesn't exist in nfts or nft_owners.

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfer := NFTTransfer{
		From:      "0xFrom",
		To:        "0xTo",
		Contract:  "0xDoesNotExist",
		TokenID:   "TokenErr",
		BlockTime: 777,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnershipReverse(tx, transfer, 999999)
	assert.Error(t, err, "Should fail because there's no such row in nft_owners")

	// Optionally we can do more forced errors if needed
	_ = tx.Rollback()
}
