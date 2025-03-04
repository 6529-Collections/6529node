package ethdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	internaldb "github.com/6529-Collections/6529node/internal/db"
	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/6529-Collections/6529node/pkg/tdh/models"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestOwnerDb spins up a test DB, returns:
//   - *sql.DB
//   - NFTOwnerDb (system under test)
//   - NFTDb (to create references in nfts table when needed)
//   - cleanup function
func setupTestOwnerDb(t *testing.T) (*sql.DB, NFTOwnerDb, NFTDb, func()) {
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
		From:      constants.NULL_ADDRESS,
		To:        "0xNewOwner",
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 123456789,
		Type:      models.MINT,
	}

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

	total, owners, err := ownerDb.GetPaginatedResponseForQuery(tx, internaldb.QueryOptions{
		Where: "owner = ? AND contract = ? AND token_id = ?",
	}, []interface{}{"0xNobody", "0xContractZ", "TokenZ"})

	require.NoError(t, err)
	assert.Equal(t, 0, len(owners), "If no row, balance=0")
	assert.Equal(t, 0, total, "If no row, balance=0")

	_ = tx.Rollback()
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

func TestOwnerDb_Error_InsertDuplicateNftOwner(t *testing.T) {
	// This test tries to force a duplicate key error on the INSERT portion of UpdateOwnership.
	// We do that by calling UpdateOwnership twice with the same (owner, contract, token_id, token_unique_id).

	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xDupContract"
	tokenID := "TokenDup"
	ownerAddr := "0xUser"

	// Create the NFT
	createNftInDB(t, db, nftDb, contract, tokenID)

	// Insert once (mint)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	mint := models.TokenTransfer{
		From:      "",
		To:        ownerAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 1234,
		Type:      models.MINT,
	}
	tokenUniqueID := uint64(777)
	err = ownerDb.UpdateOwnership(tx, mint, tokenUniqueID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert again the same exact row in a new Tx
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	// Doing another MINT with the same (owner, contract, tokenID, tokenUniqueID) triggers a PK violation
	err = ownerDb.UpdateOwnership(tx2, mint, tokenUniqueID)
	assert.Error(t, err, "Expected duplicate PK error on second insert of identical row")

	_ = tx2.Rollback()
}

func TestOwnerDb_Error_InsertWithoutNftFK(t *testing.T) {
	db, ownerDb, _, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xNoSuchNFT"
	tokenID := "MissingNFT"
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mint := models.TokenTransfer{
		From:      "",
		To:        "0xFKFail",
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 98765,
		Type:      models.MINT,
	}
	tokenUniqueID := uint64(9999)
	err = ownerDb.UpdateOwnership(tx, mint, tokenUniqueID)
	assert.Error(t, err, "Expected foreign key error because NFT doesn't exist in 'nfts' table")

	_ = tx.Rollback()
}

func TestOwnerDb_UpdateOwnershipReverse_InsertDuplicateOldOwner(t *testing.T) {
	// This test forces a duplicate key when reversing a transfer to an old owner
	// if that owner already has the same (contract, token_id, token_unique_id).
	// This scenario might be contrived, but let's see if your schema or logic might permit it.

	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xRevDup"
	tokenID := "RevDupToken"
	tokenUniqueID := uint64(1010)
	fromAddr := "0xOwner1"
	toAddr := "0xOwner2"

	// 1) Create NFT & ownership => fromAddr
	createNftInDB(t, db, nftDb, contract, tokenID)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	mint := models.TokenTransfer{
		From:      "",
		To:        fromAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 1000,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(tx, mint, tokenUniqueID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// 2) Transfer => fromAddr => toAddr
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	forward := models.TokenTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 1001,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnership(tx, forward, tokenUniqueID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// 3) Now do a partial "reverse," but first we artificially insert the old
	//    owner row again to force a duplicate PK.
	tx, err = db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// Insert the same (owner=fromAddr, token_unique_id=1010, contract+token=...) row
	_, dupeErr := tx.Exec(`
		INSERT INTO nft_owners (owner, contract, token_id, token_unique_id, timestamp)
		VALUES (?, ?, ?, ?, ?)`,
		fromAddr, contract, tokenID, tokenUniqueID, 99999,
	)
	require.NoError(t, dupeErr, "Insert old owner row manually => duplicates the PK")

	// Now reversing should try to insert the same row again => duplication
	reverse := NFTTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 1001,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnershipReverse(tx, reverse, tokenUniqueID)
	assert.Error(t, err, "Expected duplicate key error inserting old owner row again")

	_ = tx.Rollback()
}

func TestOwnerDb_GetUniqueID_ErrorPropagation(t *testing.T) {
	// If you want to test "GetUniqueID" returning an actual DB error (beyond no rows),
	// we can drop the 'nft_owners' table mid-transaction or use a mock.
	// For demonstration, let's rename the table to break the query.

	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	// create NFT + ownership
	createNftInDB(t, db, nftDb, "0xTest", "TokenX")
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	mint := models.TokenTransfer{From: "", To: "0xOwner", Contract: "0xTest", TokenID: "TokenX", BlockTime: 123, Type: models.MINT}
	err = ownerDb.UpdateOwnership(tx, mint, 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// rename table => break queries
	_, renameErr := db.Exec(`ALTER TABLE nft_owners RENAME TO nft_owners_broken;`)
	require.NoError(t, renameErr, "Renaming table should succeed")

	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, getErr := ownerDb.GetUniqueID(tx2, "0xTest", "TokenX", "0xOwner")
	assert.Error(t, getErr, "Expected error because 'nft_owners' table no longer exists")
	_ = tx2.Rollback()
}

func TestOwnerDb_UpdateOwnership_ErrorOnInsert(t *testing.T) {
	// Force an error specifically on the "insert new ownership" step of UpdateOwnership
	// e.g., by renaming or dropping nft_owners between checking currentOwner and the insert.

	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	// set up minted ownership for an NFT
	createNftInDB(t, db, nftDb, "0xInsertErr", "TIE")
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mint := models.TokenTransfer{
		From:      "",
		To:        "0xAlice",
		Contract:  "0xInsertErr",
		TokenID:   "TIE",
		BlockTime: 100,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(tx, mint, 55)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Now let's do a normal "transfer" => it calls "select owner" => success,
	// then tries to "DELETE" old row => success, but "INSERT" new row => we break it in mid-step
	tx2, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	transfer := models.TokenTransfer{
		From:      "0xAlice",
		To:        "0xBob",
		Contract:  "0xInsertErr",
		TokenID:   "TIE",
		BlockTime: 200,
		Type:      "transfer",
	}

	// Check current ownership => should pass
	// Then let's rename the table before the insert:
	_, renameErr := tx2.Exec(`ALTER TABLE nft_owners RENAME TO nft_owners_gone;`)
	require.NoError(t, renameErr)

	// Now calling UpdateOwnership -> The final insert fails
	err = ownerDb.UpdateOwnership(tx2, transfer, 55)
	assert.Error(t, err, "Expected an error on the final insert step")

	_ = tx2.Rollback()
}

func TestOwnerDb_UpdateOwnership_TransferDeleteError(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xDelError"
	tokenID := "TokenDelErr"
	fromAddr := "0xAlice"
	toAddr := "0xBob"
	tokenUniqueID := uint64(5000)

	// 1) Create the NFT and give ownership to `fromAddr` via a MINT
	createNftInDB(t, db, nftDb, contract, tokenID)

	txMint, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mint := models.TokenTransfer{
		From:      "",
		To:        fromAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 100,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(txMint, mint, tokenUniqueID)
	require.NoError(t, err)
	require.NoError(t, txMint.Commit())

	// 2) Create a trigger that *always* raises an error on DELETE
	_, triggerErr := db.Exec(`
        CREATE TRIGGER forbid_delete_nft_owners
        BEFORE DELETE ON nft_owners
        BEGIN
            SELECT RAISE(FAIL, 'No deletes allowed');
        END;
    `)
	require.NoError(t, triggerErr, "Failed to create SQLite trigger")

	// 3) Attempt a normal transfer (From->To)
	//    This calls SELECT -> DELETE -> INSERT in your UpdateOwnership code.
	txTransfer, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	forward := models.TokenTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 200,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnership(txTransfer, forward, tokenUniqueID)
	assert.Error(t, err, "Expected error because the DELETE should fail due to the trigger")

	_ = txTransfer.Rollback()

	// 4) Drop the trigger so it doesn't affect other tests
	_, dropErr := db.Exec(`DROP TRIGGER forbid_delete_nft_owners`)
	require.NoError(t, dropErr)
}

func TestOwnerDb_UpdateOwnership_TransferDeleteError_WithMock(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	// 1) Tell the mock: "We expect a transaction to begin now."
	mock.ExpectBegin()

	// 2) Actually begin the transaction
	tx, err := mockDB.Begin()
	require.NoError(t, err)

	// 3) We expect a SELECT to succeed (the code will do `tx.QueryRow("SELECT owner...")`)
	mock.ExpectQuery("SELECT owner FROM nft_owners").
		WithArgs("0xMockContract", "Token123", 42).
		WillReturnRows(sqlmock.NewRows([]string{"owner"}).AddRow("0xAlice"))

	// 4) We expect the DELETE to fail
	mock.ExpectExec("DELETE FROM nft_owners").
		WithArgs("0xAlice", "0xMockContract", "Token123", 42).
		WillReturnError(fmt.Errorf("forced delete error"))

	// Optionally, if your code never commits because an error occurs,
	// you might expect a rollback:
	mock.ExpectRollback()

	// 5) Now call the real method
	ownerDb := &OwnerDbImpl{}
	transfer := models.TokenTransfer{
		From:     "0xAlice",
		To:       "0xBob",
		Contract: "0xMockContract",
		TokenID:  "Token123",
		Type:     "transfer",
	}
	err = ownerDb.UpdateOwnership(tx, transfer, 42)
	assert.Error(t, err, "Should fail due to forced delete error")

	// 6) The test code does a rollback or commit, matching the expectation
	_ = tx.Rollback()

	// 7) Finally check all expectations
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestOwnerDb_UpdateOwnership_TransferDeleteError_Trigger(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xDelErrorRealDB"
	tokenID := "RealDBTokenDelErr"
	fromAddr := "0xAliceReal"
	toAddr := "0xBobReal"
	tokenUniqueID := uint64(5001)

	// 1) Create the NFT and give ownership to `fromAddr` via a MINT
	createNftInDB(t, db, nftDb, contract, tokenID)

	txMint, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	mint := models.TokenTransfer{
		From:      "",
		To:        fromAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 100,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(txMint, mint, tokenUniqueID)
	require.NoError(t, err)
	require.NoError(t, txMint.Commit())

	// 2) Create a trigger that *always* raises an error on DELETE
	_, triggerErr := db.Exec(`
        CREATE TRIGGER forbid_delete_nft_owners_realdb
        BEFORE DELETE ON nft_owners
        BEGIN
            SELECT RAISE(FAIL, 'No deletes allowed in real test');
        END;
    `)
	require.NoError(t, triggerErr, "Failed to create SQLite trigger in real DB")

	// 3) Attempt a normal transfer (From->To)
	txTransfer, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	forward := models.TokenTransfer{
		From:      fromAddr,
		To:        toAddr,
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 200,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnership(txTransfer, forward, tokenUniqueID)
	assert.Error(t, err, "Expected error because the DELETE should fail due to the trigger")

	_ = txTransfer.Rollback()

	// 4) Drop the trigger so it doesn't affect other tests
	_, dropErr := db.Exec(`DROP TRIGGER forbid_delete_nft_owners_realdb`)
	require.NoError(t, dropErr)
}

// TestOwnerDb_UpdateOwnership_ErrorOnInsert_RealDB
// Forces an INSERT error after the DELETE has succeeded by renaming the table mid-transaction.
func TestOwnerDb_UpdateOwnership_ErrorOnInsert_RealDB(t *testing.T) {
	db, ownerDb, nftDb, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	contract := "0xInsertErrRealDB"
	tokenID := "InsertErrToken"
	createNftInDB(t, db, nftDb, contract, tokenID)

	// 1) Mint so we have a valid ownership
	txMint, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	mint := models.TokenTransfer{
		From:      constants.NULL_ADDRESS,
		To:        "0xAliceInsert",
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 100,
		Type:      models.MINT,
	}
	err = ownerDb.UpdateOwnership(txMint, mint, 3333)
	require.NoError(t, err)
	require.NoError(t, txMint.Commit())

	// 2) Transfer => from Alice to Bob
	//    We'll rename the `nft_owners` table after the code does the "SELECT" and "DELETE",
	//    but before the final "INSERT".
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// Force the "SELECT owner" & "DELETE" to succeed
	var curOwner string
	_ = tx.QueryRow("SELECT owner FROM nft_owners WHERE contract=? AND token_id=? AND token_unique_id=?",
		contract, tokenID, 3333,
	).Scan(&curOwner)

	// Now rename the table -> any subsequent INSERT in this same Tx will fail
	_, renameErr := tx.Exec(`ALTER TABLE nft_owners RENAME TO nft_owners_gone_realdb`)
	require.NoError(t, renameErr)

	transfer := models.TokenTransfer{
		From:      "0xAliceInsert",
		To:        "0xBobInsert",
		Contract:  contract,
		TokenID:   tokenID,
		BlockTime: 200,
		Type:      "transfer",
	}
	err = ownerDb.UpdateOwnership(tx, transfer, 3333)
	assert.Error(t, err, "Expected insert error after table rename")

	_ = tx.Rollback()
}

func TestOwnerDb_GetBalance_Error_RealDB(t *testing.T) {
	db, ownerDb, _, cleanup := setupTestOwnerDb(t)
	defer cleanup()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	// rename the table
	_, renameErr := tx.Exec(`ALTER TABLE nft_owners RENAME TO nft_owners_broken_balance`)
	require.NoError(t, renameErr)

	// Now any SELECT from "nft_owners" will fail
	total, owners, err := ownerDb.GetPaginatedResponseForQuery(tx, internaldb.QueryOptions{
		Where: "owner = ? AND contract = ? AND token_id = ?",
	}, []interface{}{"0xSomeOwner", "0xAnyContract", "AnyToken"})
	assert.Error(t, err, "Expected an error because table doesn't exist now")
	assert.Equal(t, 0, total)
	assert.Equal(t, 0, len(owners))

	_ = tx.Rollback()
}

func TestUpdateOwnershipReverse_DeleteError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	// Expect transaction begin.
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("error beginning tx: %v", err)
	}

	transfer := NFTTransfer{
		Contract:  "some-contract",
		TokenID:   "some-token",
		To:        "receiver",
		From:      "sender",
		BlockTime: uint64(time.Now().Unix()),
		Type:      models.SEND,
	}
	tokenUniqueID := uint64(123)

	// Use regexp.QuoteMeta to escape the SQL string.
	selectSQL := regexp.QuoteMeta("SELECT owner FROM nft_owners WHERE contract = ? AND token_id = ? AND token_unique_id = ?")
	mock.ExpectQuery(selectSQL).
		WithArgs(transfer.Contract, transfer.TokenID, tokenUniqueID).
		WillReturnRows(sqlmock.NewRows([]string{"owner"}).AddRow(transfer.To))

	deleteSQL := regexp.QuoteMeta("DELETE FROM nft_owners WHERE owner = ? AND contract = ? AND token_id = ? AND token_unique_id = ?")
	mock.ExpectExec(deleteSQL).
		WithArgs(transfer.To, transfer.Contract, transfer.TokenID, tokenUniqueID).
		WillReturnError(errors.New("delete error"))

	ownerDbImpl := &OwnerDbImpl{}
	err = ownerDbImpl.UpdateOwnershipReverse(tx, transfer, tokenUniqueID)
	if err == nil || err.Error() != "delete error" {
		t.Errorf("expected delete error, got %v", err)
	}

	// Expect transaction rollback.
	mock.ExpectRollback()
	_ = tx.Rollback()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateOwnershipReverse_InsertError(t *testing.T) {
	// Create a new sqlmock DB.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	defer db.Close()

	// Expect a transaction begin.
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("error beginning tx: %v", err)
	}

	// Create a sample transfer that will trigger the INSERT branch.
	transfer := NFTTransfer{
		Contract:  "some-contract",
		TokenID:   "some-token",
		To:        "receiver",
		From:      "sender",
		BlockTime: 1234567890,
		Type:      "TRANSFER",
	}
	tokenUniqueID := uint64(123)

	// Expect the SELECT query for the current owner.
	selectSQL := regexp.QuoteMeta("SELECT owner FROM nft_owners WHERE contract = ? AND token_id = ? AND token_unique_id = ?")
	mock.ExpectQuery(selectSQL).
		WithArgs(transfer.Contract, transfer.TokenID, tokenUniqueID).
		WillReturnRows(sqlmock.NewRows([]string{"owner"}).AddRow(transfer.To))

	// Expect the DELETE query to succeed.
	deleteSQL := regexp.QuoteMeta("DELETE FROM nft_owners WHERE owner = ? AND contract = ? AND token_id = ? AND token_unique_id = ?")
	mock.ExpectExec(deleteSQL).
		WithArgs(transfer.To, transfer.Contract, transfer.TokenID, tokenUniqueID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect the INSERT query to fail.
	insertSQL := regexp.QuoteMeta("INSERT INTO nft_owners (owner, contract, token_id, token_unique_id, timestamp) VALUES (?, ?, ?, ?, ?)")
	mock.ExpectExec(insertSQL).
		WithArgs(transfer.From, transfer.Contract, transfer.TokenID, tokenUniqueID, transfer.BlockTime).
		WillReturnError(errors.New("insert error"))

	ownerDbImpl := &OwnerDbImpl{}
	err = ownerDbImpl.UpdateOwnershipReverse(tx, transfer, tokenUniqueID)
	if err == nil || err.Error() != "insert error" {
		t.Errorf("expected insert error, got %v", err)
	}

	// Expect a rollback.
	mock.ExpectRollback()
	_ = tx.Rollback()

	// Verify all expectations were met.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestNewNFTOwner(t *testing.T) {
	owner := newNFTOwner()
	require.NotNil(t, owner)
}
