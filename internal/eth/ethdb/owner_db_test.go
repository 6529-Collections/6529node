package ethdb

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/6529-Collections/6529node/internal/db/testdb"
	"github.com/6529-Collections/6529node/pkg/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupOwnerTestDB creates a fresh test DB, creates both nfts and nft_owners tables
// (because of the foreign key reference), and returns (OwnerDb, *sql.DB, cleanupFn).
func setupOwnerTestDB(t *testing.T) (OwnerDb, *sql.DB, func()) {
	db, cleanup := testdb.SetupTestDB(t)

	// Create the "nfts" table if it doesn't already exist
	createNftsTableSQL := `
	CREATE TABLE IF NOT EXISTS nfts (
		contract TEXT NOT NULL,
		token_id TEXT NOT NULL,
		supply INTEGER NOT NULL DEFAULT 0,
		burnt_supply INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (contract, token_id)
	);
	`
	_, err := db.Exec(createNftsTableSQL)
	require.NoError(t, err, "failed to create nfts table")

	// Create the "nft_owners" table with the foreign key reference
	createNftOwnersTableSQL := `
	CREATE TABLE IF NOT EXISTS nft_owners (
		owner    TEXT NOT NULL,
		contract TEXT NOT NULL,
		token_id TEXT NOT NULL,
		balance  INTEGER NOT NULL CHECK (balance >= 0),
		PRIMARY KEY (contract, token_id, owner),
		FOREIGN KEY (contract, token_id) REFERENCES nfts (contract, token_id) ON DELETE CASCADE
	);
	`
	_, err = db.Exec(createNftOwnersTableSQL)
	require.NoError(t, err, "failed to create nft_owners table")

	ownerDb := NewOwnerDb()

	return ownerDb, db, func() {
		// Clean up after tests
		_, _ = db.Exec("DROP TABLE IF EXISTS nft_owners")
		_, _ = db.Exec("DROP TABLE IF EXISTS nfts")
		cleanup()
	}
}

// insertNFT inserts a row into the nfts table with the given supply and burnt_supply.
// This ensures the foreign key constraint is satisfied for (contract, tokenID).
func insertNFT(t *testing.T, db *sql.DB, contract, tokenID string, supply, burntSupply int64) {
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(`
		INSERT INTO nfts (contract, token_id, supply, burnt_supply)
		VALUES (?, ?, ?, ?) 
		ON CONFLICT(contract, token_id) DO NOTHING
	`, contract, tokenID, supply, burntSupply)
	require.NoError(t, err, "failed to insert NFT for contract=%s tokenID=%s", contract, tokenID)

	require.NoError(t, tx.Commit())
}

// insertOwnerBalance inserts a row into the nft_owners table to set an owner's initial balance.
func insertOwnerBalance(t *testing.T, db *sql.DB, contract, tokenID, owner string, balance int64) {
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(`
		INSERT INTO nft_owners (contract, token_id, owner, balance)
		VALUES (?, ?, ?, ?)
	`, contract, tokenID, owner, balance)
	require.NoError(t, err, "failed to insert owner balance")

	require.NoError(t, tx.Commit())
}

// Helper to fetch an owner's balance directly from DB for test verification.
func getTestBalance(t *testing.T, db *sql.DB, contract, tokenID, owner string) int64 {
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	var balance int64
	err = tx.QueryRow(`
		SELECT balance 
		FROM nft_owners 
		WHERE contract = ? AND token_id = ? AND owner = ?
	`, contract, tokenID, owner).Scan(&balance)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	require.NoError(t, err)
	return balance
}

func TestOwnerDb_UpdateOwnership(t *testing.T) {
	ownerDb, db, cleanup := setupOwnerTestDB(t)
	defer cleanup()

	t.Run("error if amount <= 0", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = ownerDb.UpdateOwnership(tx, "someFrom", "someTo", "contractA", "tokenA", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "amount must be positive")
	})

	t.Run("error if from has insufficient balance", func(t *testing.T) {
		// Insert NFT row for foreign key
		insertNFT(t, db, "contractB", "tokenB", 100, 0)
		// fromB has only 5
		insertOwnerBalance(t, db, "contractB", "tokenB", "fromB", 5)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		// Transfer 10, from only has 5
		err = ownerDb.UpdateOwnership(tx, "fromB", "toB", "contractB", "tokenB", 10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient balance")
	})

	t.Run("deduct from fromAddr, row is deleted when balance becomes 0", func(t *testing.T) {
		insertNFT(t, db, "contractC", "tokenC", 100, 0)
		// fromC starts with 5
		insertOwnerBalance(t, db, "contractC", "tokenC", "fromC", 5)

		tx, err := db.Begin()
		require.NoError(t, err)

		err = ownerDb.UpdateOwnership(tx, "fromC", "toC", "contractC", "tokenC", 5)
		require.NoError(t, err)

		require.NoError(t, tx.Commit())

		// after commit, fromC => 0 => row should be deleted
		bal := getTestBalance(t, db, "contractC", "tokenC", "fromC")
		assert.Equal(t, int64(0), bal, "fromC row should be removed")

		// toC should have 5
		bal = getTestBalance(t, db, "contractC", "tokenC", "toC")
		assert.Equal(t, int64(5), bal, "toC should have gained 5")
	})

	t.Run("deduct from fromAddr, row is updated when balance remains > 0", func(t *testing.T) {
		insertNFT(t, db, "contractD", "tokenD", 100, 0)
		insertOwnerBalance(t, db, "contractD", "tokenD", "fromD", 10)

		tx, err := db.Begin()
		require.NoError(t, err)

		err = ownerDb.UpdateOwnership(tx, "fromD", "toD", "contractD", "tokenD", 3)
		require.NoError(t, err)

		require.NoError(t, tx.Commit())

		// fromD should now have 7
		bal := getTestBalance(t, db, "contractD", "tokenD", "fromD")
		assert.Equal(t, int64(7), bal)

		// toD should now have 3
		bal = getTestBalance(t, db, "contractD", "tokenD", "toD")
		assert.Equal(t, int64(3), bal)
	})

	t.Run("NULL_ADDRESS scenarios: skip deduction if from=NULL, skip addition if to=NULL", func(t *testing.T) {
		insertNFT(t, db, "contractX", "tokenX", 100, 0)
		tx, err := db.Begin()
		require.NoError(t, err)

		// from = NULL_ADDRESS, to = NULL_ADDRESS => effectively do nothing
		err = ownerDb.UpdateOwnership(tx, constants.NULL_ADDRESS, constants.NULL_ADDRESS, "contractX", "tokenX", 10)
		require.NoError(t, err)

		require.NoError(t, tx.Commit())

		bal := getTestBalance(t, db, "contractX", "tokenX", constants.NULL_ADDRESS)
		assert.Equal(t, int64(0), bal, "No row should exist or be updated for null address")
	})
}

func TestOwnerDb_UpdateOwnershipReverse(t *testing.T) {
	ownerDb, db, cleanup := setupOwnerTestDB(t)
	defer cleanup()

	t.Run("error if amount <= 0", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		err = ownerDb.UpdateOwnershipReverse(tx, "fromA", "toA", "contractA", "tokenA", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "amount must be positive")
	})

	t.Run("error if 'to' has insufficient balance for reverse", func(t *testing.T) {
		insertNFT(t, db, "contractB", "tokenB", 100, 0)
		// 'toB' in the reverse scenario has 5
		insertOwnerBalance(t, db, "contractB", "tokenB", "toB", 5)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		// Attempting to revert 10 from toB->fromB
		err = ownerDb.UpdateOwnershipReverse(tx, "fromB", "toB", "contractB", "tokenB", 10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient balance at 'to' address")
	})

	t.Run("'to' row removed if balance hits 0 after reverse", func(t *testing.T) {
		insertNFT(t, db, "contractC", "tokenC", 100, 0)
		insertOwnerBalance(t, db, "contractC", "tokenC", "toC", 5)

		tx, err := db.Begin()
		require.NoError(t, err)

		// Revert 5 from toC -> fromC
		err = ownerDb.UpdateOwnershipReverse(tx, "fromC", "toC", "contractC", "tokenC", 5)
		require.NoError(t, err)

		require.NoError(t, tx.Commit())

		// toC balance => 0 => row removed
		bal := getTestBalance(t, db, "contractC", "tokenC", "toC")
		assert.Equal(t, int64(0), bal)

		// fromC should have 5
		bal = getTestBalance(t, db, "contractC", "tokenC", "fromC")
		assert.Equal(t, int64(5), bal)
	})

	t.Run("'to' row updated if partial revert", func(t *testing.T) {
		insertNFT(t, db, "contractD", "tokenD", 100, 0)
		insertOwnerBalance(t, db, "contractD", "tokenD", "toD", 10)

		tx, err := db.Begin()
		require.NoError(t, err)

		err = ownerDb.UpdateOwnershipReverse(tx, "fromD", "toD", "contractD", "tokenD", 3)
		require.NoError(t, err)

		require.NoError(t, tx.Commit())

		bal := getTestBalance(t, db, "contractD", "tokenD", "toD")
		assert.Equal(t, int64(7), bal, "toD should have 7 left")

		bal = getTestBalance(t, db, "contractD", "tokenD", "fromD")
		assert.Equal(t, int64(3), bal, "fromD should have gained 3")
	})

	t.Run("NULL_ADDRESS scenarios: skip deduction if to=NULL, skip addition if from=NULL", func(t *testing.T) {
		insertNFT(t, db, "contractX", "tokenX", 100, 0)
		tx, err := db.Begin()
		require.NoError(t, err)

		err = ownerDb.UpdateOwnershipReverse(tx, constants.NULL_ADDRESS, constants.NULL_ADDRESS, "contractX", "tokenX", 10)
		require.NoError(t, err)

		require.NoError(t, tx.Commit())

		// No changes
		bal := getTestBalance(t, db, "contractX", "tokenX", constants.NULL_ADDRESS)
		assert.Equal(t, int64(0), bal)
	})
}

func TestOwnerDb_GetBalance(t *testing.T) {
	ownerDb, db, cleanup := setupOwnerTestDB(t)
	defer cleanup()

	t.Run("non-existent balance => returns 0, nil error", func(t *testing.T) {
		// Must still have an NFT row for the foreign key.
		insertNFT(t, db, "contractA", "tokenA", 100, 0)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		bal, err := ownerDb.GetBalance(tx, "unknownOwner", "contractA", "tokenA")
		require.NoError(t, err)
		assert.Equal(t, int64(0), bal)
	})

	t.Run("existing balance => correct value", func(t *testing.T) {
		insertNFT(t, db, "contractB", "tokenB", 100, 0)
		insertOwnerBalance(t, db, "contractB", "tokenB", "knownOwner", 50)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		bal, err := ownerDb.GetBalance(tx, "knownOwner", "contractB", "tokenB")
		require.NoError(t, err)
		assert.Equal(t, int64(50), bal)
	})
}

func TestOwnerDb_ErrorOnTransactionFailure(t *testing.T) {
	ownerDb, db, cleanup := setupOwnerTestDB(t)
	defer cleanup()

	t.Run("using a closed transaction fails queries", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback()) // immediately rollback

		err = ownerDb.UpdateOwnership(tx, "from", "to", "contractZ", "tokenZ", 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")

		_, err = ownerDb.GetBalance(tx, "owner", "contractZ", "tokenZ")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transaction has already been committed or rolled back")
	})
}

func TestOwnerDb_ManualQueryErrorInjection(t *testing.T) {
	ownerDb, db, cleanup := setupOwnerTestDB(t)
	defer cleanup()

	t.Run("force invalid SQL to test error path", func(t *testing.T) {
		// Insert an NFT so the foreign key constraint isn't the first error
		insertNFT(t, db, "brokenContract", "brokenToken", 100, 0)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		// Attempt to rename the table to break next queries
		_, renameErr := tx.Exec(`ALTER TABLE nft_owners RENAME TO nft_owners_tmp`)
		if renameErr != nil {
			// Some DB engines disallow table rename in a transaction; skip if not supported
			t.Skipf("Skipping rename test: %v", renameErr)
		}
		defer func() {
			// Try to rename back so we don't break further tests
			_, _ = tx.Exec(`ALTER TABLE nft_owners_tmp RENAME TO nft_owners`)
		}()

		// Now the code that tries to "INSERT INTO nft_owners" should fail
		err = ownerDb.UpdateOwnership(tx, "someFrom", "someTo", "brokenContract", "brokenToken", 5)
		assert.Error(t, err)
	})
}
